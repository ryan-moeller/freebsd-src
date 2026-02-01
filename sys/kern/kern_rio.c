/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include "opt_rio.h"

#define EXTERR_CATEGORY EXTERR_CAT_RIO
#include <sys/param.h>
#include <sys/systm.h>
#include <sys/bio.h>
#include <sys/bitset.h>
#include <sys/bitstring.h>
#include <sys/buf.h>
#include <sys/condvar.h>
#include <sys/conf.h>
#include <sys/counter.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/malloc.h>
#include <sys/mman.h>
#include <sys/module.h>
#include <sys/mutex.h>
#include <sys/pcpu.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/rio.h>
#include <sys/sched.h>
#include <sys/smp.h>
#include <sys/stat.h>
#include <sys/sx.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/taskqueue.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/umtxvar.h>
#include <sys/user.h>
#include <sys/vnode.h>

#include <geom/geom.h>

#include <rio/rio_internal.h>

#include <vm/pmap.h>
#include <vm/uma.h>
#include <vm/vm_extern.h>
#include <vm/vm_kern.h>
#include <vm/vm_map.h>
#include <vm/vm_page.h>
#include <vm/vnode_pager.h>

#include <ck_ec.h>
#include <ck_ring.h>

FEATURE(rio, "Ring I/O");

static SYSCTL_NODE(_kern, OID_AUTO, rio, CTLFLAG_RD | CTLFLAG_MPSAFE, NULL,
    "Ring IO configuration");

static MALLOC_DEFINE(M_RIO, "rio", "rio data structures");

typedef int rio_src_scheduler_f(struct rio_softc *);

/*
 * TODO: come up with a set of useful scheduling policies.
 *
 * Useful means different things, so policies are defined broadly by two
 * parameters: the behavior, and the configuration.
 *
 * Behavior encapsulates run-time decision-making.
 *
 * Configuration is split across two planes: user and system.  User policy
 * config is controlled by ioctls, system policy config is controlled by
 * sysctls.
 *
 * How should policy be defined?  An nvlist would be very extensible for sure,
 * or simply per-policy ioctls.
 *
 * As a starting point, all points of policy influence should be identified and
 * codified into a generic policy interface.  Then specific policies can be
 * implemented and exposed.
 *
 * Prior art: see domainset(9)
 *
 * Scheduling policy shall be implemented with mathematical rigor and precision.
 * System-level constraints, such as fairness, define a set of feasible choices.
 * The objective functions apply configured weights to observable metrics and
 * produce a score for each available choice, and the most favorable is chosen.
 *
 * A predefined set of policies are to be provided as predetermined weights
 * optimizing for typical concerns (e.g. locality, age, depth).
 *
 * For implementation utility and observability, see qmath(3) and stats(3).
 */
static rio_src_scheduler_f rio_src_scheduler_none;

static rio_src_scheduler_f *rio_src_policies[] = {
	[RIO_POLICY_NONE] = &rio_src_scheduler_none,
};

static SYSCTL_NODE(_kern_rio, OID_AUTO, flow, CTLFLAG_RD | CTLFLAG_MPSAFE, NULL,
    "RIO per-CPU flow configuration");

static u_int rio_flow_issuer_threads = 2;
SYSCTL_UINT(_kern_rio_flow, OID_AUTO, issuer_threads, CTLFLAG_RDTUN,
    &rio_flow_issuer_threads, 0, "Max number of issuer threads per CPU flow");

static u_int rio_flow_read_workers = 4;
SYSCTL_UINT(_kern_rio_flow, OID_AUTO, read_workers, CTLFLAG_RDTUN,
    &rio_flow_read_workers, 0, "Max number of read workers per CPU flow");

static u_int rio_flow_write_workers = 4;
SYSCTL_UINT(_kern_rio_flow, OID_AUTO, write_workers, CTLFLAG_RDTUN,
    &rio_flow_write_workers, 0, "Max number of write workers per CPU flow");

static u_int rio_flow_sync_workers = 4;
SYSCTL_UINT(_kern_rio_flow, OID_AUTO, sync_workers, CTLFLAG_RDTUN,
    &rio_flow_sync_workers, 0, "Max number of write workers per CPU flow");

/* TODO: other worker classes */

/* Deferred softc destruction. */
static struct taskqueue *rio_doom;

/* Shutdown handling. */
static u_int rio_open_count;
SYSCTL_UINT(_kern_rio, OID_AUTO, open_count, CTLFLAG_RD, &rio_open_count, 0,
    "Number of open RIO handles");

static struct mtx rio_shutdown_lock;
MTX_SYSINIT(rio_shutdown_lock, &rio_shutdown_lock, "rio shutdown lock",
    MTX_DEF);
static struct cv rio_shutdown_cond;
static boolean_t rio_shutdown_pending;
_Static_assert(sizeof(rio_shutdown_pending) == sizeof(int),
    "rio_shutdown_pending must be int-sized for atomic use");

static inline bool
rio_shuttingdown(void)
{
	return (atomic_load_acq_int(&rio_shutdown_pending));
}

/* size of riopriv bitset */
static u_int rio_max_workers = PAGE_SIZE * NBBY;
SYSCTL_UINT(_kern_rio, OID_AUTO, max_workers, CTLFLAG_RDTUN, &rio_max_workers,
    0, "Max worker processes (sizes worker affinity bitset)");

BITSET_DEFINE_VAR(rio_worker_affinity);

static inline void
rio_vmspace_init(struct vmspace *vm)
{
	struct rio_worker_affinity *waff;

	if (vm->vm_rio != NULL) {
		return;
	}
	waff = BITSET_ALLOC(rio_max_workers, M_RIO, M_WAITOK | M_ZERO);
	if (!atomic_cmpset_ptr((uintptr_t *)&vm->vm_rio, 0, (uintptr_t)waff)) {
		BITSET_FREE(waff, M_RIO);
	}
}

static inline void
rio_vmspace_switch(struct vmspace *vm, u_int id)
{
	struct vmspace *oldvm = curproc->p_vmspace;

	if (vm != oldvm) {
		if (oldvm->vm_rio != NULL) {
			BIT_CLR_ATOMIC(rio_max_workers, id, oldvm->vm_rio);
		}
		if (vm->vm_rio != NULL) {
			BIT_SET_ATOMIC(rio_max_workers, id, vm->vm_rio);
		}
		vmspace_switch_aio(vm);
	}
}

/* kernel-private IO control block */
struct rio_io {
	struct riocb	rio_cb;		/* validated and stable copy */
	struct file	*rio_fd_file;	/* ref'd file descriptor */
};

/* Get the basic command, stripped of flags. */
static inline u_int
rio_io_cmd(struct rio_io *io)
{
	return (io->rio_cb.rio_cmd & ~RIO_CMD_FLAGS);
}

/* Get the flag bits of the control block command. */
static inline u_int
rio_io_flags(struct rio_io *io)
{
	return (io->rio_cb.rio_cmd & RIO_CMD_FLAGS);
}

static inline bool
rio_io_vectored(struct rio_io *io)
{
	return ((rio_io_flags(io) & RIO_VECTORED) != 0);
}

static inline struct riocb *
rio_io_kiocb(struct rio_io *io)
{
	return (&io->rio_cb);
}

/* list of all handles for debugging, protected by shutdown lock */
static LIST_HEAD(, rio_softc) rio_handles;

enum rio_status {
	RIO_OPEN,	/* the handle is open */
	RIO_CLOSING,	/* the handle is closing */
	RIO_EXITING,	/* the process is closing */
};

struct rio_softc {
	struct rio	*sc_rio;	/* kernel address of SHM object */
	struct rio_slot *sc_submissions;/* submission queue slots in rio */
	struct rio_slot *sc_completions;/* completion queue slots in rio */
	struct rio_io	*sc_io;		/* kernel-private IO control blocks */
	struct ucred	*sc_cred;	/* user credentials */
	struct proc	*sc_proc;	/* user process */
	struct vmspace	*sc_vmspace;	/* user process vmspace */
	vm_offset_t	sc_urio;	/* user address of SHM object */
	/* TODO: policy metadata */
	struct task	sc_destroy_task;/* destruction task */
	enum rio_status	sc_status;	/* softc/process status */
	struct sx	sc_status_lock;	/* block status change while issuing */
	struct mtx	sc_issuer_lock;	/* for submission dequeue accounting */
	/* TODO: flags? counters? */
	struct rio_config	sc_config;
	/*
	 * XXX: As a workaround for not having context in wake32, we have to
	 * embed the CK event counter ops and mode in every softc so the ops
	 * can be used to derive a pointer to the softc.
	 */
	struct ck_ec_ops	sc_ec_umtx_ops;
	struct ck_ec_mode	sc_ec_umtx_mode;
	LIST_ENTRY(rio_softc)	sc_handles;
};

static inline int
rio_open(struct rio_softc **scp)
{
	struct rio_softc *sc;

	sc = malloc(sizeof(*sc), M_RIO, M_WAITOK | M_ZERO);
	mtx_lock(&rio_shutdown_lock);
	if (rio_shuttingdown()) {
		mtx_unlock(&rio_shutdown_lock);
		free(sc, M_RIO);
		return (ESHUTDOWN);
	}
	sx_init(&sc->sc_status_lock, "rio softc status lock");
	mtx_init(&sc->sc_issuer_lock, "rio softc issuer lock", NULL, MTX_DEF);
	LIST_INSERT_HEAD(&rio_handles, sc, sc_handles);
	rio_open_count++;
	mtx_unlock(&rio_shutdown_lock);
	*scp = sc;
	return (0);
}

static inline void
rio_close(struct rio_softc *sc)
{
	mtx_lock(&rio_shutdown_lock);
	LIST_REMOVE(sc, sc_handles);
	rio_open_count--;
	if (rio_open_count == 0 && rio_shuttingdown()) {
		cv_signal(&rio_shutdown_cond);
	}
	mtx_unlock(&rio_shutdown_lock);
	sx_destroy(&sc->sc_status_lock);
	mtx_destroy(&sc->sc_issuer_lock);
	free(sc, M_RIO);
}

static enum rio_status
rio_status(struct rio_softc *sc)
{
	return (atomic_load_acq_int(&sc->sc_status));
}

static int
rio_ec_gettime(const struct ck_ec_ops *ops __unused, struct timespec *out)
{
	nanouptime(out); /* CLOCK_MONOTONIC */
	return (0);
}

/* Translate a kernel address to a user address. */
static inline void *
rio_uaddr(struct rio_softc *sc, const uint32_t *address)
{
	vm_offset_t offset = (vm_offset_t)address - (vm_offset_t)sc->sc_rio;
	vm_offset_t uaddr = sc->sc_urio + offset;

	return ((void *)uaddr);
}

#if 0 /* keeping this code around for later use in polling */
static void
rio_ec_umtx_wait(const struct ck_ec_wait_state *state, const uint32_t *address,
    uint32_t expected, const struct timespec *deadline)
{
	struct umtx_abs_timeout uto, *utop;
	struct umtx_q *uq;
	struct rio_softc *sc = state->data;
	void *uaddr;
	uint32_t value;
	int error;

	/* This implementation is largely informed by kern_umtq.c:do_wait(). */
	uq = curthread->td_umtxq;
	uaddr = rio_uaddr(sc, address);
	if ((error = umtx_key_get_proc(uaddr, TYPE_SIMPLE_WAIT, AUTO_SHARE,
	    &uq->uq_key, sc->sc_proc)) != 0) {
		/* TODO: handle error somehow */
		printf("%s: umtx_key_get_proc: %d\n", __func__, error);
		return;
	}
	if (deadline == NULL) {
		utop = NULL;
	} else {
		umtx_abs_timeout_init(&uto, CLOCK_MONOTONIC, true, deadline);
		utop = &uto;
	}
	umtxq_lock(&uq->uq_key);
	umtxq_insert(uq);
	umtxq_unlock(&uq->uq_key);
	value = *address;
	umtxq_lock(&uq->uq_key);
	if (value == expected) {
		error = umtxq_sleep(uq, "riowait", utop);
	} else {
		error = 0;
	}
	if ((uq->uq_flags & UQF_UMTXQ) != 0) {
		umtxq_remove(uq);
	}
	umtxq_unlock(&uq->uq_key);
	umtx_key_release(&uq->uq_key);
	switch (error) {
	case 0:
	case ETIMEDOUT:
		break;
	default:
		/* TODO: handle error somehow */
		printf("%s: error %d\n", __func__, error);
		break;
	}
}
#endif

static inline void
rio_ec_umtx_wake(const struct ck_ec_ops *ops, const uint32_t *address)
{
	struct rio_softc *sc;
	void *uaddr;
	int error;

	sc = __containerof(ops, struct rio_softc, sc_ec_umtx_ops);
	uaddr = rio_uaddr(sc, address);
	/* Wake a single waiter. */
	if ((error = umtx_wake(sc->sc_proc, uaddr, 1, true)) != 0) {
		/* TODO: handle error somehow */
		printf("%s: umtx_wake: error=%d\n", __func__, error);
	}
}

static inline uint32_t
rio_inflight(struct rio_softc *sc)
{
	struct rio *rio = sc->sc_rio;
	uint32_t s = ck_ec_value(&rio->rio_submission.rr_dqc);
	uint32_t c = ck_ec_value(&rio->rio_completion.rr_dqc);

	return (s - c);
}

static inline struct riocb *
rio_submissions_dequeue_locked(struct rio_softc *sc, uint32_t *indexp)
{
	struct rio_slot slot;
	struct rio *rio = sc->sc_rio;

	/* Enforce inflight < cqlen so completion cannot block workers. */
	if (rio_inflight(sc) >= sc->sc_config.rio_cqlen - 1) {
		return (NULL);
	}
	if (CK_RING_DEQUEUE_MPSC(rio, &rio->rio_submission.rr_ring,
	    sc->sc_submissions, &slot)) {
		uint32_t index = slot.rs_index;

		ck_ec_inc(&rio->rio_submission.rr_dqc, &sc->sc_ec_umtx_mode);
		if (__predict_true(index < sc->sc_config.rio_ncb)) {
			*indexp = index;
			return (&rio->rio_control[index]);
		}
		/* TODO: how to handle invalid index? */
		printf("%s: invalid index %u\n", __func__, index);
	}
	return (NULL);
}

/*
 * TODO: Perhaps a variation to dequeue several in a batch if available?  Batch
 * size could be configured by policy.  The caller would supply a buffer to fill
 * with pointers.  The lock would remain held and only one event counter change
 * would be needed.
 */
static inline struct riocb *
rio_submissions_dequeue(struct rio_softc *sc, uint32_t *indexp)
{
	struct riocb *iocb;

	/* The lock prevents inflight counter racing. */
	mtx_lock(&sc->sc_issuer_lock);
	iocb = rio_submissions_dequeue_locked(sc, indexp);
	mtx_unlock(&sc->sc_issuer_lock);
	return (iocb);
}

static inline int
rio_completions_enqueue(struct rio_softc *sc, uint32_t index)
{
	struct rio_slot slot;
	struct rio *rio = sc->sc_rio;

	slot.rs_index = index;
	if (CK_RING_ENQUEUE_MPMC(rio, &rio->rio_completion.rr_ring,
	    sc->sc_completions, &slot)) {
		ck_ec_inc(&rio->rio_completion.rr_nqc, &sc->sc_ec_umtx_mode);
		return (0);
	}
	return (EINVAL);
}

static void
rio_destroy_task(void *arg, int pending __unused)
{
	struct rio_softc *sc = arg;
	vm_offset_t kva;
	size_t size;

	crfree(sc->sc_cred);
	vmspace_free(sc->sc_vmspace);
	/*
	 * The object can stay mapped even if the shmfd is closed or the user
	 * process exits.  Unmap the object directly instead of requiring the
	 * file to remain open for a call to shm_unmap().
	 */
	kva = (vm_offset_t)sc->sc_rio;
	/* We call shm_map() with an offset of 0, so kva is aligned. */
	size = round_page(rio_config_size(&sc->sc_config));
	vm_map_remove(kernel_map, kva, kva + size);
	free(sc->sc_io, M_RIO);
	rio_close(sc);
}

/* called by shm_drop on close */
void
rio_destroy(struct rio_softc *sc)
{
	sx_xlock(&sc->sc_status_lock);
	/* The sx ensures ordering and visibility to atomic readers. */
	if ((sc->sc_proc->p_flag & P_WEXIT) == 0) {
		sc->sc_status = RIO_CLOSING;
	} else {
		sc->sc_status = RIO_EXITING;
	}
	sx_xunlock(&sc->sc_status_lock);
	if (rio_inflight(sc) == 0) {
		taskqueue_enqueue(rio_doom, &sc->sc_destroy_task);
	}
	/* The final completion enqueues the destruction task. */
}

static inline void
rio_iocb_complete(struct rio_softc *sc, struct riocb *iocb, int cberror,
    ssize_t cbstatus)
{
	uint32_t index = iocb - sc->sc_rio->rio_control;
	int error;

	iocb->rio_error = cberror;
	iocb->rio_status = cbstatus;
	atomic_thread_fence_rel();
	error = rio_completions_enqueue(sc, index);
	if (__predict_false(error == EINVAL)) {
		/* The user is misbehaving. */
		rio_destroy(sc);
	}
	MPASS(error == 0);
	if (__predict_false(rio_status(sc) != RIO_OPEN) &&
	    __predict_false(rio_inflight(sc) == 0)) {
		taskqueue_enqueue(rio_doom, &sc->sc_destroy_task);
	}
}

static inline void
rio_ring_init(struct rio_ring *ring, u_int size)
{
	ck_ring_init(&ring->rr_ring, size);
	ck_ec_init(&ring->rr_nqc, 0);
	ck_ec_init(&ring->rr_dqc, 0);
}

/* Get the user address of the shmfd object in process p. */
static inline vm_offset_t
rio_shm_uaddr(struct rio_softc *sc, struct shmfd *shmfd)
{
	vm_map_t map = &sc->sc_vmspace->vm_map;
	vm_map_entry_t entry;
	vm_offset_t uaddr = 0;

	vm_map_lock_read(map);
	VM_MAP_ENTRY_FOREACH(entry, map) {
		if (entry->object.vm_object == shmfd->shm_object) {
			uaddr = entry->start;
			break;
		}
	}
	vm_map_unlock_read(map);
	return (uaddr);
}

static int
rio_configure(const struct rio_config *conf, struct file *fp, struct thread *td,
    struct ucred *active_cred)
{
	struct rio_softc *sc;
	struct rio *rio;
	struct shmfd *shmfd;
	size_t size;
	int error;

	MPASS(fp->f_type == DTYPE_SHM);
	shmfd = fp->f_data;
	if (shmfd->shm_path != NULL ||
	    conf->rio_policy_id >= nitems(rio_src_policies)) {
		return (EINVAL);
	}
	if ((error = rio_open(&sc)) != 0) {
		return (error);
	}
	memcpy(&sc->sc_config, conf, sizeof(*conf));
	size = rio_config_size(conf);
	/* TODO: Will mmap enforce size limits for us? */
	if ((error = shm_map(fp, size, 0, (void **)&sc->sc_rio)) != 0) {
		rio_close(sc);
		return (error);
	}
	sc->sc_cred = crhold(active_cred); /* XXX: for all IO on this ring */
	sc->sc_proc = td->td_proc;
	sc->sc_vmspace = vmspace_acquire_ref(sc->sc_proc);
	rio_vmspace_init(sc->sc_vmspace);
	rio = sc->sc_rio;
	rio_ring_init(&rio->rio_submission, conf->rio_sqlen);
	rio_ring_init(&rio->rio_completion, conf->rio_cqlen);
	memset(rio->rio_control, 0, size - sizeof(*rio));
	sc->sc_submissions = rio_submission_slots(rio, conf);
	sc->sc_completions = rio_completion_slots(rio, conf);
	sc->sc_io = mallocarray(conf->rio_ncb, sizeof(*sc->sc_io), M_RIO,
	    M_WAITOK | M_ZERO);
	sc->sc_urio = rio_shm_uaddr(sc, shmfd);
	TASK_INIT(&sc->sc_destroy_task, 0, rio_destroy_task, sc);
	sc->sc_ec_umtx_ops = (struct ck_ec_ops){
		.gettime = rio_ec_gettime,
#if 0 /* not yet */
		.wait32 = rio_ec_umtx_wait,
#endif
		.wake32 = rio_ec_umtx_wake,
		/* TODO: tune/override default options for ABI stability */
	};
	sc->sc_ec_umtx_mode = (struct ck_ec_mode){
		.ops = &sc->sc_ec_umtx_ops,
		.single_producer = false,
	};
	shmfd->shm_rio = sc;
	return (0);
}

static int
rio_submit(struct file *fp, struct thread *td)
{
	struct shmfd *shmfd;
	struct rio_softc *sc;
	rio_src_scheduler_f *schedule;

	MPASS(fp->f_type == DTYPE_SHM);
	shmfd = fp->f_data;
	if (__predict_false((sc = shmfd->shm_rio) == NULL)) {
		return (ENOTTY);
	}
	if (__predict_false(sc->sc_proc != td->td_proc)) {
		return (EDOOFUS);
	}
	schedule = rio_src_policies[sc->sc_config.rio_policy_id];
	return (schedule(sc));
}

int
rio_ioctl(struct file *fp, u_long com, void *data, struct ucred *active_cred,
    struct thread *td)
{
	switch (com) {
	case FIORIOCONFIGURE:
		return (rio_configure(data, fp, td, active_cred));
	case FIORIOSUBMIT:
		return (rio_submit(fp, td));
	default:
		return (ENOTTY);
	}
}

struct rio_src {
	struct rio_softc	*rs_sc;	/* io source context */
	STAILQ_ENTRY(rio_src)	rs_srcs;
};
STAILQ_HEAD(rio_srcs, rio_src);

static uma_zone_t rio_src_zone;

/*
 * A RIO issuer is a queue of IO request sources serviced by a collection of
 * kernel threads pinned to a CPU.  Each source is drained according to an IO
 * scheduling policy.  The requests are issued to an appropriate worker as
 * needed.
 */
struct rio_issuer {
	struct mtx		ri_lock;
	struct cv		ri_cond;
	struct rio_srcs		ri_srcs;
	u_int			ri_len;
	int			ri_phase;	/* for reducing bias */
	u_int			ri_cpu;
	u_int			ri_threads;
};

static inline void
rio_issuer_enqueue(struct rio_issuer *issuer, struct rio_src *src)
{
	bool wake;

	mtx_lock(&issuer->ri_lock);
	wake = STAILQ_EMPTY(&issuer->ri_srcs);
	STAILQ_INSERT_TAIL(&issuer->ri_srcs, src, rs_srcs);
	issuer->ri_len++;
	if (wake) {
		cv_signal(&issuer->ri_cond);
	}
	mtx_unlock(&issuer->ri_lock);
}

static inline int
rio_issuer_dequeue(struct rio_issuer *issuer, struct rio_src **srcp)
{
	struct rio_src *src;

	mtx_lock(&issuer->ri_lock);
	while (STAILQ_EMPTY(&issuer->ri_srcs)) {
		/* TODO: timeout for autoscaling */
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
		if (__predict_false(rio_shuttingdown())) {
			mtx_unlock(&issuer->ri_lock);
			return (ESHUTDOWN);
		}
	}
	src = STAILQ_FIRST(&issuer->ri_srcs);
	STAILQ_REMOVE_HEAD(&issuer->ri_srcs, rs_srcs);
	issuer->ri_len--;
	issuer->ri_phase++;
	mtx_unlock(&issuer->ri_lock);
	*srcp = src;
	return (0);
}

/*
 * Issuer outlets:
 *
 * a. Error - no dependencies
 * b. Blocking sync - vmspace only
 * c. Blocking non-vectored uio - vmspace, uio/iovec fits on stack
 * d. Blocking vectored uio - vmspace, uio/iovecs allocated/freed
 * e. Non-blocking non-vectored uio - vmspace, uio/iovec allocated/freed
 * f. Non-blocking vectored uio - vmspace, uio/iovecs allocated/freed
 * g. Callback-driven non-vectored bio strategy - bio/pages allocated/freed
 * h. Callback-driven vectored bio strategy - vmspace, bio/pages allocated/freed
 *
 * > Any vectored command (RIO_VECTORED) requires the rio_iov pointer to be
 *   validated before use.  This is most readily achieved by copyin, or more
 *   practically by copyiniov or most likely copyinuio.  Those also require
 *   use of the user vmspace, which implies it must occur in a worker.  They
 *   also imply malloc/free for the iovecs/uio.
 *
 * > Non-vectored commands do not need to validate a pointer to iovecs, but
 *   typically will need the process's vmspace for blocking I/O operations.
 *
 * > Except any socket operation that might have to wait has to be prepared to
 *   make incremental progress on a uio in multiple attempts, so the uio has to
 *   outlive any original stack, implying malloc/free for iovecs/uio, whether
 *   or not the command itself is vectored.
 *
 * > Devices with a bio_strategy handler don't even use a uio/iovecs for I/O,
 *   they use bios.  If we're not doing vectored I/O, we don't need a worker.
 *   We can issue non-blocking bio operations directly and let bio_done handle
 *   completion.  We just have to fault/hold/unhold the pages ourselves.  That
 *   might be better off happening in a worker, depending how slow faults are.
 *
 * So, there is a subset of commands that only needs to validate/fault user
 * buffers, which is performed by the requested operation, so a uio/iovec can be
 * on the stack.  Other commands will require a persistent uio from the heap for
 * repeated operations, but no iovec validation.  Some require iovecs validation
 * but do blocking I/O so the uio can be on the stack in a worker process.
 * Finally, there are the commands that must validate an iovec array and persist
 * the uio/iovecs on the heap.
 *
 * We use a small rio_srcio structure for blocking or callback-driven commands.
 * The larger rio_srcio_ext structure extends rio_srcio with additional fields
 * needed for non-blocking I/O (sockets).  The size of rio_srcio is small enough
 * to land in a smaller UMA bucket than rio_srcio_ext (<32B vs. <128B).  The
 * rio_uio structure provides storage and common setup of uio/iovecs on the
 * stack for blocking operations.
 */

struct rio_srcio {
	struct rio_softc	*rs_sc;		/* io source context */
	struct rio_io		*rs_io;		/* io request */
	struct uio		*rs_uio;	/* allocated or in ext */
	STAILQ_ENTRY(rio_srcio)	rs_srcios;
};
STAILQ_HEAD(rio_srcios, rio_srcio);

static uma_zone_t rio_srcio_zone;

struct rio_srcio_ext {
	struct rio_srcio	rse_srcio;
	struct uio		rse_uio;
	struct iovec		rse_iov;
	size_t			rse_completed;
	u_int			rse_cpu;
};

static uma_zone_t rio_srcio_ext_zone;

typedef void rio_srcio_handler_f(struct rio_srcio *, u_int);

static inline struct rio_srcio_ext *
rio_srcio_ext(struct rio_srcio *srcio)
{
	return (__containerof(srcio, struct rio_srcio_ext, rse_srcio));
}

static inline uma_zone_t
rio_file_srcio_zone(struct file *fp)
{
	if (fp != NULL && fp->f_type == DTYPE_SOCKET) {
		return (rio_srcio_ext_zone);
	}
	return (rio_srcio_zone);
}

static inline struct rio_srcio *
rio_srcio_new(struct rio_softc *sc, struct rio_io *io)
{
	struct rio_srcio *srcio;
	uma_zone_t zone = rio_file_srcio_zone(io->rio_fd_file);

	srcio = uma_zalloc(zone, M_WAITOK);
	srcio->rs_sc = sc;
	srcio->rs_io = io;
	srcio->rs_uio = NULL;
	if (zone == rio_srcio_ext_zone) {
		struct rio_srcio_ext *rse = rio_srcio_ext(srcio);

		rse->rse_completed = 0;
		rse->rse_cpu = curcpu;
	}
	return (srcio);
}

static inline void
rio_srcio_free(struct rio_srcio *srcio, uma_zone_t zone)
{
	if (zone == rio_srcio_ext_zone) {
		struct rio_srcio_ext *rse = rio_srcio_ext(srcio);

		if (srcio->rs_uio == &rse->rse_uio) {
			/* Don't try to free the ext uio. */
			srcio->rs_uio = NULL;
		}
	}
	if (srcio->rs_uio != NULL) {
		freeuio(srcio->rs_uio);
	}
	uma_zfree(zone, srcio);
}

static inline uint32_t
rio_srcio_index(struct rio_srcio *srcio)
{
	return (srcio->rs_io - srcio->rs_sc->sc_io);
}

static inline struct riocb *
rio_srcio_iocb(struct rio_srcio *srcio)
{
	struct rio *rio = srcio->rs_sc->sc_rio;
	uint32_t index = rio_srcio_index(srcio);

	return (rio->rio_control + index);
}

static inline void
rio_srcio_complete(struct rio_srcio *srcio)
{
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct riocb *iocb = rio_srcio_iocb(srcio);
	uma_zone_t zone = rio_file_srcio_zone(io->rio_fd_file);

	/* Must release file before io can be reused. */
	if (io->rio_fd_file != NULL) {
		fdrop(io->rio_fd_file, NULL);
	}
	rio_iocb_complete(sc, iocb, kiocb->rio_error, kiocb->rio_status);
	rio_srcio_free(srcio, zone);
}

static inline void
rio_srcio_error(struct rio_srcio *srcio, int error)
{
	struct riocb *kiocb = rio_io_kiocb(srcio->rs_io);

	kiocb->rio_error = error;
	kiocb->rio_status = -1;
	rio_srcio_complete(srcio);
}

static inline struct proc *
rio_srcio_proc(struct rio_srcio *srcio)
{
	return (srcio->rs_sc->sc_proc);
}

static inline struct vmspace *
rio_srcio_vmspace(struct rio_srcio *srcio)
{
	return (srcio->rs_sc->sc_vmspace);
}

static inline void
rio_srcio_vmspace_switch(struct rio_srcio *srcio, u_int id)
{
	rio_vmspace_switch(rio_srcio_vmspace(srcio), id);
}

static inline int
rio_bio_bufsetup(struct bio *bp, struct cdev *dev, vm_map_t map, void *buf,
    size_t len)
{
	struct buf *pbuf;
	vm_page_t *pages;
	vm_offset_t addr = (vm_offset_t)buf;
	vm_offset_t pgoff = addr & PAGE_MASK;
	vm_prot_t prot;
	int npages;

	bp->bio_dev = dev;
	bp->bio_length = len;
	bp->bio_bcount = len;
	if ((dev->si_flags & SI_UNMAPPED) != 0 && unmapped_buf_allowed) {
		pbuf = NULL;
		pages = mallocarray(atop(round_page(len)) + 1, sizeof(*pages),
		    M_TEMP, M_WAITOK | M_ZERO);
	} else {
		pbuf = uma_zalloc(pbuf_zone, M_WAITOK);
		BUF_KERNPROC(pbuf);
		pages = pbuf->b_pages;
	}
	prot = VM_PROT_READ | (bp->bio_cmd == BIO_READ ? VM_PROT_WRITE : 0);
	npages = vm_fault_quick_hold_pages(map, addr, len, prot, pages,
	    atop(maxphys) + 1);
	if (npages == -1) {
		if (pbuf == NULL) {
			free(pages, M_TEMP);
		} else {
			uma_zfree(pbuf_zone, pbuf);
		}
		return (EFAULT);
	}
	if (pbuf == NULL) {
		bp->bio_ma = pages;
		bp->bio_ma_n = npages;
		bp->bio_ma_offset = pgoff;
		bp->bio_data = unmapped_buf;
		bp->bio_flags = BIO_UNMAPPED;
		/* TODO: accounting a la aio num_unmapped_aio */
	} else {
		pmap_qenter((vm_offset_t)pbuf->b_data, pages, npages);
		bp->bio_data = pbuf->b_data + pgoff;
		bp->bio_caller2 = pbuf;
		pbuf->b_npages = npages;
		/* TODO: accounting a la aio num_buf_aio */
	}
	return (0);
}

static inline void
rio_bio_destroy(struct bio *bp)
{
	vm_page_t *pages = bp->bio_ma;
	struct buf *pbuf = bp->bio_caller2;

	if (pbuf != NULL) {
		int npages = pbuf->b_npages;

		MPASS(npages <= atop(maxphys) + 1);
		pmap_qremove((vm_offset_t)pbuf->b_data, npages);
		vm_page_unhold_pages(pbuf->b_pages, npages);
		uma_zfree(pbuf_zone, pbuf);
		/* TODO: accounting a la aio num_buf_aio */
	} else if (pages != NULL) {
		int npages = bp->bio_ma_n;

		MPASS(npages <= atop(maxphys) + 1);
		vm_page_unhold_pages(pages, npages);
		free(pages, M_TEMP);
		/* TODO: accounting a la aio num_unmapped_aio */
	}
	g_destroy_bio(bp);
}

static void
rio_bio_childdone(struct bio *bp)
{
	struct bio *pbp = bp->bio_parent;
	u_int inbed;

	/*
	 * First to error wins.  See sys/geom/notes.
	 */
	if (__predict_false(bp->bio_error != 0)) {
		atomic_cmpset_int(&pbp->bio_error, 0, bp->bio_error);
	}
	atomic_add_64(&pbp->bio_completed, bp->bio_completed);
	rio_bio_destroy(bp);
	inbed = atomic_fetchadd_int(&pbp->bio_inbed, 1) + 1;
	if (pbp->bio_children == inbed) {
		pbp->bio_done(pbp);
	}
}

static void
rio_bio_complete(struct bio *bp)
{
	struct rio_srcio *srcio = bp->bio_caller1;
	struct riocb *kiocb = rio_io_kiocb(srcio->rs_io);

	kiocb->rio_status = bp->bio_completed;
	kiocb->rio_error = bp->bio_error;
	rio_bio_destroy(bp);
	rio_srcio_complete(srcio);
}

static inline int
rio_srcio_bio_strategy(struct rio_srcio *srcio)
{
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	vm_map_t map = &srcio->rs_sc->sc_vmspace->vm_map;
	struct file *fp = io->rio_fd_file;
	struct vnode *vp = fp->f_vnode;
	struct cdevsw *csw;
	struct cdev *dev;
	struct bio *pbp;
	off_t offset = kiocb->rio_offset;
	size_t resid;
	u_int cmd = rio_io_cmd(io);
	int bsize, bio_cmd, error, ref = 0;

	switch (cmd) {
	case RIO_READ:
		bio_cmd = BIO_READ;
		break;
	case RIO_WRITE:
		bio_cmd = BIO_WRITE;
		break;
	/* TODO: BIO_DELETE? BIO_FLUSH? */
	default:
		return (EINVAL);
	}
	if (fp == NULL || fp->f_type != DTYPE_VNODE) {
		return (EINVAL);
	}
	if (vp->v_type != VCHR || (bsize = vp->v_bufobj.bo_bsize) == 0) {
		return (EINVAL);
	}
	/* TODO: limits a la max_buf_aio et cetera */
	if (rio_io_vectored(io)) {
		resid = 0;
		/* TODO: kiocb->rio_iov must be checked by copyin in a worker,
		 * move to a bio worker pool and using copyinuio for temporary
		 * kernel-copy of iovecs. */
		for (int i = 0; i < kiocb->rio_length; i++) {
			size_t len = kiocb->rio_iov[i].iov_len;

			if (len % bsize != 0 || len > maxphys) {
				return (EINVAL);
			}
			resid += len;
		}
	} else {
		resid = kiocb->rio_length;
		if (resid % bsize != 0 || resid > maxphys) {
			return (EINVAL);
		}
	}
	if ((csw = devvn_refthread(vp, &dev, &ref)) == NULL) {
		return (ENXIO);
	}
	if ((csw->d_flags & D_DISK) == 0) {
		error = EINVAL;
		goto unref;
	}
	if (resid > dev->si_iosize_max) {
		error = EINVAL;
		goto unref;
	}
	/* TODO: buffer count limits a la aio */
	pbp = g_alloc_bio();
	pbp->bio_cmd = bio_cmd;
	pbp->bio_offset = offset;
	pbp->bio_length = resid;
	pbp->bio_caller1 = srcio;
	pbp->bio_done = rio_bio_complete;
	if (rio_io_vectored(io)) {
		size_t nchildren = kiocb->rio_length;
		struct bio **children;

		children = mallocarray(nchildren, sizeof(*children), M_TEMP,
		    M_WAITOK | M_ZERO);
		for (int i = 0; i < nchildren; i++) {
			struct iovec *iov = kiocb->rio_iov + i;
			struct bio *bp = g_duplicate_bio(pbp);

			children[i] = bp;
			bp->bio_offset = offset;
			bp->bio_done = rio_bio_childdone;
			if ((error = rio_bio_bufsetup(bp, dev, map,
			    iov->iov_base, iov->iov_len)) != 0) {
				do {
					rio_bio_destroy(children[i]);
				} while (i-- > 0);
				free(children, M_TEMP);
				goto destroy;
			}
			offset += iov->iov_len;
		}
		for (int i = 0; i < nchildren; i++) {
			csw->d_strategy(children[i]);
		}
		free(children, M_TEMP);
	} else {
		if ((error = rio_bio_bufsetup(pbp, dev, map, kiocb->rio_buf,
		    kiocb->rio_length)) != 0) {
			goto destroy;
		}
		csw->d_strategy(pbp);
	}
	dev_relthread(dev, ref);
	return (0);
destroy:
	g_destroy_bio(pbp);
unref:
	dev_relthread(dev, ref);
	return (error);
}

static inline int
rio_io_foflag(struct rio_io *io)
{
	return ((rio_io_flags(io) & RIO_FOFFSET) == 0 ? FOF_OFFSET : 0);
}

/* Common context for blocking uio operations. */
struct rio_uio {
	struct uio	*ruio_uio;
	struct uio	ruio__uio;
	struct iovec	ruio__iov;
	size_t		ruio_len;
};

static inline int
rio_uio_setup(struct rio_uio *ruio, struct rio_srcio *srcio, struct thread *td)
{
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct uio *uio;
	struct iovec *iov;
	int error;

	if (rio_io_vectored(io)) {
		error = copyinuio(kiocb->rio_iov, kiocb->rio_length, &uio);
		if (error != 0) {
			return (error);
		}
		srcio->rs_uio = uio; /* free after completion */
	} else {
		iov = &ruio->ruio__iov;
		iov->iov_base = kiocb->rio_buf;
		iov->iov_len = kiocb->rio_length;
		uio = &ruio->ruio__uio;
		uio->uio_iov = iov;
		uio->uio_iovcnt = 1;
		uio->uio_resid = kiocb->rio_length;
		uio->uio_segflg = UIO_USERSPACE;
	}
	uio->uio_offset = kiocb->rio_offset;
	uio->uio_td = td;
	ruio->ruio_uio = uio;
	ruio->ruio_len = uio->uio_resid;
	return (0);
}

static inline ssize_t
rio_uio_completed(struct rio_uio *ruio)
{
	return (ruio->ruio_len - ruio->ruio_uio->uio_resid);
}

static void
rio_srcio_read(struct rio_srcio *srcio, u_int id)
{
	struct rio_uio ruio;
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct file *fp = io->rio_fd_file;
	int error, foflag;

	td->td_ucred = sc->sc_cred;
	rio_srcio_vmspace_switch(srcio, id);
	if ((error = rio_uio_setup(&ruio, srcio, td)) != 0) {
		rio_srcio_error(srcio, error);
		return;
	}
	ruio.ruio_uio->uio_rw = UIO_READ;
	foflag = rio_io_foflag(io);
	kiocb->rio_error = fo_read(fp, ruio.ruio_uio, sc->sc_cred, foflag, td);
	switch (kiocb->rio_error) {
	case 0:
	case ERESTART:
	case EINTR:
	case EWOULDBLOCK:
		kiocb->rio_status = rio_uio_completed(&ruio);
		break;
	default:
		kiocb->rio_status = -1;
		break;
	}
	td->td_ucred = saved_cred;
	rio_srcio_complete(srcio);
}

static void
rio_srcio_write(struct rio_srcio *srcio, u_int id)
{
	struct rio_uio ruio;
	struct proc *p = rio_srcio_proc(srcio);
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct file *fp = io->rio_fd_file;
	int error, foflag;

	td->td_ucred = sc->sc_cred;
	rio_srcio_vmspace_switch(srcio, id);
	if ((error = rio_uio_setup(&ruio, srcio, td)) != 0) {
		rio_srcio_error(srcio, error);
		return;
	}
	ruio.ruio_uio->uio_rw = UIO_WRITE;
	foflag = rio_io_foflag(io);
	if (fp->f_type == DTYPE_VNODE) {
		bwillwrite();
	}
	kiocb->rio_error = fo_write(fp, ruio.ruio_uio, sc->sc_cred, foflag, td);
	switch (kiocb->rio_error) {
	case EPIPE:
		PROC_LOCK(p);
		kern_psignal(p, SIGPIPE);
		PROC_UNLOCK(p);
		/* FALLTHROUGH */
	case 0:
	case ERESTART:
	case EINTR:
	case EWOULDBLOCK:
		kiocb->rio_status = rio_uio_completed(&ruio);
		break;
	default:
		kiocb->rio_status = -1;
		break;
	}
	td->td_ucred = saved_cred;
	rio_srcio_complete(srcio);
}

static void
rio_srcio_sync(struct rio_srcio *srcio, u_int id)
{
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct file *fp = io->rio_fd_file;
	struct vnode *vp;
	u_int cmd = rio_io_cmd(io);
	int error = 0;

	if (cmd == RIO_MLOCK) {
		rio_srcio_vmspace_switch(srcio, id);
		/*
		 * TODO: After the commands are fleshed out, see if it is
		 * possible to make ident an int and use the rio_data/rio_buf
		 * field for anything that is a pointer (like AIO).
		 */
		error = kern_mlock(rio_srcio_proc(srcio), sc->sc_cred,
		    kiocb->rio_ident, kiocb->rio_length);
	} else if ((vp = fp->f_vnode) != NULL) {
		struct mount *mp;

		while (error == ERELOOKUP) {
			if ((error = vn_start_write(vp, &mp, V_WAIT | V_PCATCH))
			    != 0) {
				break;
			}
			vn_lock(vp, LK_EXCLUSIVE | LK_RETRY);
			vnode_pager_clean_async(vp);
			switch (cmd) {
			case RIO_SYNC:
				error = VOP_FSYNC(vp, MNT_WAIT, td);
				break;
			case RIO_DSYNC:
				error = VOP_FDATASYNC(vp, td);
				break;
			default:
				__assert_unreachable();
			}
			VOP_UNLOCK(vp);
			vn_finished_write(mp);
		}
	}
	if ((kiocb->rio_error = error) == 0) {
		kiocb->rio_status = 0;
	} else {
		kiocb->rio_status = -1;
	}
	td->td_ucred = saved_cred;
	rio_srcio_complete(srcio);
}

/*
 * A RIO worker is a kernel process pinned to a CPU.  The worker performs
 * blocking IO operations on behalf of a user process.   A worker must be a
 * kernel process with a single thread because it potentially has to change
 * vmspace to perform copies to or from the user process.
 *
 * If the workers pulled directly from the src ring then they couldn't be
 * specialized for specific IO types (read, write, socket, etc).  Instead,
 * issuer threads dequeue work from the user and delegate it to workers.
 */
struct rio_worker {
	struct mtx		rw_lock;
	struct cv		rw_cond;
	struct rio_srcios	rw_srcios;
	rio_srcio_handler_f	*rw_handler;	/* specialized handler */
	u_int			rw_len;
	u_int			rw_cpu;
	u_int			rw_id;
	bool			rw_done;
};

static inline void
rio_worker_enqueue(struct rio_worker *worker, struct rio_srcio *srcio)
{
	bool wake;

	mtx_lock(&worker->rw_lock);
	wake = STAILQ_EMPTY(&worker->rw_srcios);
	STAILQ_INSERT_TAIL(&worker->rw_srcios, srcio, rs_srcios);
	worker->rw_len++;
	if (wake) {
		cv_signal(&worker->rw_cond);
	}
	mtx_unlock(&worker->rw_lock);
}

static inline int
rio_worker_dequeue(struct rio_worker *worker, struct rio_srcio **srciop)
{
	struct rio_srcio *srcio;

	mtx_lock(&worker->rw_lock);
	if (STAILQ_EMPTY(&worker->rw_srcios)) {
		/* TODO: timeout for autoscaling */
		cv_wait(&worker->rw_cond, &worker->rw_lock);
	}
	if (__predict_false(rio_shuttingdown())) {
		mtx_unlock(&worker->rw_lock);
		return (ESHUTDOWN);
	}
	srcio = STAILQ_FIRST(&worker->rw_srcios);
	if (__predict_false(srcio == NULL)) {
		/* Signaled to relinquish vmspace. */
		mtx_unlock(&worker->rw_lock);
		return (ESRCH);
	}
	STAILQ_REMOVE_HEAD(&worker->rw_srcios, rs_srcios);
	worker->rw_len--;
	mtx_unlock(&worker->rw_lock);
	*srciop = srcio;
	return (0);
}

/*
 * The workers are divided into operation classes such as read, write, or sync.
 * This helps keep latency and throughput consistent and prevents slow paths
 * from poisoning fast paths.
 */
struct rio_flow {
	struct rio_issuer rf_issuer;
	struct rio_worker *rf_read;
	struct rio_worker *rf_write;
	struct rio_worker *rf_sync;
	/* TODO: other worker classes */
};
DPCPU_DEFINE_STATIC(struct rio_flow, rio_flow);

static inline struct rio_worker *
rio_flow_classify(struct rio_flow *flow, struct rio_srcio *srcio, u_int *lenp)
{
	switch (rio_io_cmd(srcio->rs_io)) {
	case RIO_READ:
		*lenp = rio_flow_read_workers;
		return (flow->rf_read);
	case RIO_WRITE:
		*lenp = rio_flow_write_workers;
		return (flow->rf_write);
	case RIO_SYNC:
	case RIO_DSYNC:
	case RIO_MLOCK: /* ? */
		*lenp = rio_flow_sync_workers;
		return (flow->rf_sync);
	/* TODO: others */
	}
	__assert_unreachable();
}

static inline struct rio_worker *
rio_worker_lookup(u_int id)
{
	const size_t flow_stride = rio_flow_read_workers +
	    rio_flow_write_workers + rio_flow_sync_workers;
	const u_int class_strides[] = {
		rio_flow_read_workers,
		rio_flow_write_workers,
		rio_flow_sync_workers,
	};
	/* TODO: more classes? */
	struct rio_flow *flow = DPCPU_ID_PTR(id / flow_stride, rio_flow);
	struct rio_worker **classes = &flow->rf_read;
	u_int idx = id % flow_stride;

	for (u_int class = 0; class < nitems(class_strides); class++) {
		u_int class_stride = class_strides[class];

		if (idx < class_stride) {
			return (&classes[class][idx]);
		}
		idx -= class_stride;
	}
	__assert_unreachable();
}

void
rio_vmspace_exit(struct vmspace *vm)
{
	struct rio_worker_affinity *waff = vm->vm_rio;
	size_t id;

	if (waff == NULL) {
		return;
	}
	/* Signal any sleeping workers using this vmspace. */
	BIT_FOREACH_ISSET(rio_max_workers, id, waff) {
		struct rio_worker *worker = rio_worker_lookup(id);

		mtx_lock(&worker->rw_lock);
		if (worker->rw_len == 0) {
			cv_signal(&worker->rw_cond);
		}
		mtx_unlock(&worker->rw_lock);
	}
	BITSET_FREE(waff, M_RIO);
	vm->vm_rio = NULL;
}

/* TODO: src/flow schedulers deep dive */
/* XXX: this is a potentially confusing name */
static int
rio_src_scheduler_none(struct rio_softc *sc)
{
	struct rio_src *src;
	struct rio_flow *flow;
	struct rio_issuer *issuer;

	src = uma_zalloc(rio_src_zone, M_WAITOK);
	src->rs_sc = sc;
	/* Try the local flow first. */
	flow = DPCPU_PTR(rio_flow);
	issuer = &flow->rf_issuer;
	/* TODO: policy-based queue depth, spread */
	rio_issuer_enqueue(issuer, src);
	return (0);
}

/*
 * The selector picks a candidate given a criteria and a phase offset.
 * It is used to select a worker that should minimize request latency.
 */
struct rio_selector {
	bitstr_t	*rs_empty;
	bitstr_t	*rs_affine;
	bitstr_t	*rs_minimum;
	bitstr_t	*rs_candidates;
	u_int		*rs_indices;
	u_int		rs_len;
	u_int		rs_min;
	u_int		rs_n;
	struct rio_worker_affinity	*rs_vmspace_waff;
};

static inline void
rio_selector_init(struct rio_selector *sel, u_int len)
{
	sel->rs_empty = bit_alloc(len, M_RIO, M_WAITOK);
	sel->rs_affine = bit_alloc(len, M_RIO, M_WAITOK);
	sel->rs_minimum = bit_alloc(len, M_RIO, M_WAITOK);
	sel->rs_candidates = bit_alloc(len, M_RIO, M_WAITOK);
	sel->rs_indices = mallocarray(len, sizeof(u_int), M_RIO, M_WAITOK);
	sel->rs_len = len;
	sel->rs_min = UINT_MAX;
	sel->rs_n = 0;
	sel->rs_vmspace_waff = NULL;
}

static inline void
rio_selector_reset(struct rio_selector *sel, struct rio_srcio *srcio)
{
	u_int stop = sel->rs_len - 1;

	bit_nclear(sel->rs_empty, 0, stop);
	bit_nclear(sel->rs_affine, 0, stop);
	bit_nclear(sel->rs_minimum, 0, stop);
	sel->rs_min = UINT_MAX;
	sel->rs_n = 0;
	sel->rs_vmspace_waff = srcio->rs_sc->sc_vmspace->vm_rio;
	MPASS(sel->rs_vmspace_waff != NULL);
}

#if 0
static inline void
dump_bits(bitstr_t *bits, size_t len)
{
	ssize_t count;
	u_int i;

	bit_count(bits, 0, len, &count);
	printf("%s: len=%zu count=%zd\n", __func__, len, count);
	bit_foreach(bits, len, i) {
		printf("%s: bit %u set\n", __func__, i);
	}
}
#endif

static inline void
rio_selector_insert(struct rio_selector *sel, struct rio_worker *worker)
{
	u_int idx = sel->rs_n++;
	/* XXX: Unlocked, but it's probably good enough. */
	u_int qlen = worker->rw_len;

	if (qlen == 0) {
		bit_set(sel->rs_empty, idx);
	}
	if (BIT_ISSET(rio_max_workers, worker->rw_id, sel->rs_vmspace_waff)) {
		bit_set(sel->rs_affine, idx);
	}
	if (qlen == sel->rs_min) {
		bit_set(sel->rs_minimum, idx);
	} else if (qlen < sel->rs_min) {
		sel->rs_min = qlen;
		bit_nclear(sel->rs_minimum, 0, idx);
		bit_set(sel->rs_minimum, idx);
	}
}

static inline void
rio_selector_destroy(struct rio_selector *sel)
{
	free(sel->rs_empty, M_RIO);
	free(sel->rs_affine, M_RIO);
	free(sel->rs_minimum, M_RIO);
	free(sel->rs_candidates, M_RIO);
	free(sel->rs_indices, M_RIO);
}

#define UIMAX(...) ({ \
	u_int _vals[] = {__VA_ARGS__}; \
	u_int _max = 0; \
	for (u_int _i = 0; _i < nitems(_vals); _i++) { \
		if (_vals[_i] > _max) { \
		    _max = _vals[_i]; \
		} \
	} \
	_max; \
})

static inline void
bit_and(bitstr_t *a, bitstr_t *b, bitstr_t *r, size_t len)
{
	size_t n = bitstr_size(len) / sizeof(*a);

	for (size_t i = 0; i < n; i++) {
		r[i] = a[i] & b[i];
	}
}

/*
 * The tricky bit is that we want to avoid frequent vmspace changes, so
 * workers need to be fed in such a way as to balance the load while at
 * the same time being efficient about vmspace switches.
 *
 * How expensive is a vmspace switch?  The costly part is pmap_activate(), which
 * does TLB invalidation IPIs.
 *
 * To improve vmspace affinity, we save the user process pointer for the tail of
 * the worker's IO queue as a hint for worker affinity.  The scheduler first
 * checks the local workers for an idle affine worker.  Failing success in the
 * first pass, the scheduler then tries to select any idle local worker.  If the
 * second pass fails, the scheduler tries to select the least-busy affine local
 * worker.  If there are no local workers with a queue depth below a policy-
 * defined threshold, the scheduler will repeat the process for remote workers.
 *
 * TODO: Implement the remote work policies for controlled behavior under load.
 *
 * We want to stay on the same CPU as the user thread accessing the IO buffers,
 * for cache locality.  Scheduling should select the local CPU issuer until
 * local workers are all busy.  But, we also want to utilize idle CPU time to
 * minimize latency and maximize throughput.  Optimizing the balance of these
 * priorities is the role of the policy.
 *
 * TODO: Flow vnet affinity.
 */
struct rio_scheduler {
	struct rio_selector	rs_sel;
	int	*rs_phase;	/* XXX: stale reads should be good enough */
};

static inline void
rio_scheduler_init(struct rio_scheduler *sched, struct rio_issuer *issuer)
{
	sched->rs_phase = &issuer->ri_phase;
	rio_selector_init(&sched->rs_sel, UIMAX(rio_flow_read_workers,
	    rio_flow_write_workers, rio_flow_sync_workers));
	/* TODO: more worker classes? */
}

static inline void
rio_scheduler_reset(struct rio_scheduler *sched, struct rio_worker *workers,
    u_int len, struct rio_srcio *srcio)
{
	struct rio_selector *sel = &sched->rs_sel;

	rio_selector_reset(sel, srcio);
	MPASS(len > 0);
	for (u_int i = 0; i < len; i++) {
		rio_selector_insert(sel, workers + i);
	}
	MPASS(sel->rs_n == len);
	MPASS(sel->rs_min != UINT_MAX);
}

static inline struct rio_worker *
rio_scheduler_ideal(struct rio_scheduler *sched, struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rs_sel;
	u_int count, idx;

	bit_and(sel->rs_empty, sel->rs_affine, sel->rs_candidates, sel->rs_n);
	count = 0;
	bit_foreach(sel->rs_candidates, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[*sched->rs_phase % count]);
}

static inline struct rio_worker *
rio_scheduler_empty(struct rio_scheduler *sched, struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rs_sel;
	u_int count, idx;

	count = 0;
	bit_foreach(sel->rs_empty, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[*sched->rs_phase % count]);
}

static inline struct rio_worker *
rio_scheduler_affine(struct rio_scheduler *sched, struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rs_sel;
	u_int stop = sel->rs_n - 1;
	u_int min, count, idx;

	bit_nclear(sel->rs_candidates, 0, stop);
	min = UINT_MAX;
	bit_foreach(sel->rs_affine, sel->rs_n, idx) {
		u_int qlen = workers[idx].rw_len;

		if (qlen > min) {
			continue;
		}
		if (qlen < min) {
			min = qlen;
			bit_nclear(sel->rs_candidates, 0, stop);
		}
		bit_set(sel->rs_candidates, idx);
	}
	count = 0;
	bit_foreach(sel->rs_candidates, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[*sched->rs_phase % count]);
}

static inline struct rio_worker *
rio_scheduler_depth(struct rio_scheduler *sched, struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rs_sel;
	u_int count, idx;

	/* TODO: policy depth threshold to consider remote workers */
	count = 0;
	bit_foreach(sel->rs_minimum, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	MPASS(count > 0);
	return (workers + sel->rs_indices[*sched->rs_phase % count]);
}

/* Try selecting the least-busy affine worker. */
static inline struct rio_worker *
rio_scheduler_select_worker(struct rio_scheduler *sched,
    struct rio_worker *workers, u_int len, struct rio_srcio *srcio)
{
	struct rio_worker *worker;

	rio_scheduler_reset(sched, workers, len, srcio);
	if ((worker = rio_scheduler_ideal(sched, workers)) != NULL) {
		return (worker);
	}
	if ((worker = rio_scheduler_empty(sched, workers)) != NULL) {
		return (worker);
	}
	if ((worker = rio_scheduler_affine(sched, workers)) != NULL) {
		return (worker);
	}
	return (rio_scheduler_depth(sched, workers));
}

static inline void
rio_scheduler_schedule(struct rio_scheduler *sched, struct rio_srcio *srcio)
{
	struct rio_flow *flow;
	struct rio_worker *workers, *worker;
	u_int len;

	/* Try local flow first. */
	flow = DPCPU_PTR(rio_flow);
	workers = rio_flow_classify(flow, srcio, &len);
	worker = rio_scheduler_select_worker(sched, workers, len, srcio);
	rio_worker_enqueue(worker, srcio);
}

static inline void
rio_scheduler_destroy(struct rio_scheduler *sched)
{
	rio_selector_destroy(&sched->rs_sel);
}

static inline bool
riocb_canceled(struct riocb *iocb)
{
	return (atomic_load_int(&iocb->rio_error) == ECANCELED);
}

/* TODO: remote flow selection process */

/* TODO: tuning */
static u_int rio_attention_span = 1024;
SYSCTL_UINT(_kern_rio, OID_AUTO, attention_span, CTLFLAG_RW,
    &rio_attention_span, 0, "Single-source I/O batch size");

static void
rio_issuer_thread(void *arg)
{
	struct rio_scheduler sched;
	struct rio_issuer *self = arg;
	struct thread *td = curthread;

	thread_lock(td);
	sched_bind(td, self->ri_cpu);
	thread_unlock(td);
	rio_scheduler_init(&sched, self);

	for (;;) {
		struct rio_src *src;
		struct rio_softc *sc;
		struct thread *td;
		size_t issued;
next:
		/* TODO: Removal reduces concurrency!  Add an issuing list? */
		/* TODO: Work stealing! */
		if (__predict_false(rio_issuer_dequeue(self, &src) != 0)) {
			break;
		}
		sc = src->rs_sc;
		/*
		 * The shared lock prevents the SHM file from closing and the
		 * process exiting while we're using the process's file table.
		 */
		sx_slock(&sc->sc_status_lock);
		if (__predict_false(sc->sc_status != RIO_OPEN)) {
			sx_sunlock(&sc->sc_status_lock);
			uma_zfree(rio_src_zone, src);
			continue;
		}
		td = FIRST_THREAD_IN_PROC(sc->sc_proc);

		/* TODO: Policy-based attention span. */
		for (issued = 0; issued < rio_attention_span; issued++) {
			struct rio_srcio *srcio;
			struct rio_io *io;
			struct riocb *iocb, *kiocb;
			uint32_t index;
			int fd, error;

			if ((iocb = rio_submissions_dequeue(sc, &index))
			    == NULL) {
				sx_sunlock(&sc->sc_status_lock);
				uma_zfree(rio_src_zone, src);
				goto next;
			}
			/* Check if canceled while in queue. */
			if (__predict_false(riocb_canceled(iocb))) {
				rio_iocb_complete(sc, iocb, ECANCELED, -1);
				continue;
			}
			io = sc->sc_io + index;
			kiocb = rio_io_kiocb(io);
			memcpy(kiocb, iocb, sizeof(*iocb));
			fd = kiocb->rio_ident;
			switch (rio_io_cmd(io)) {
			case RIO_NOP:
			case RIO_MLOCK:
				error = 0;
				break;
			case RIO_WRITE:
				error = fget_write(td, fd, &cap_pwrite_rights,
				    &io->rio_fd_file);
				break;
			case RIO_READ:
				error = fget_read(td, fd, &cap_pread_rights,
				    &io->rio_fd_file);
				break;
			case RIO_SYNC:
			case RIO_DSYNC:
				error = fget(td, fd, &cap_fsync_rights,
				    &io->rio_fd_file);
				break;
			/* TODO: more commands */
			default:
				error = EINVAL;
				break;
			}
			if (__predict_false(error != 0)) {
				rio_iocb_complete(sc, iocb, error, -1);
				continue;
			}
			srcio = rio_srcio_new(sc, io);
			/* Try the async BIO strategy if available. */
			/* TODO: move to worker */
			switch (rio_srcio_bio_strategy(srcio)) {
			case 0:
				continue;
			case ENXIO:
				rio_srcio_error(srcio, ENXIO);
				continue;
			default:
				break;
			}
			rio_scheduler_schedule(&sched, srcio);
		}
		sx_sunlock(&sc->sc_status_lock);
		rio_issuer_enqueue(self, src);
	}
	rio_scheduler_destroy(&sched);
	mtx_lock(&self->ri_lock);
	self->ri_threads--;
	if (self->ri_threads == 0) {
		cv_broadcast(&self->ri_cond);
	}
	mtx_unlock(&self->ri_lock);
	kthread_exit();
}

static void
rio_worker_proc(void *arg)
{
	struct rio_worker *self = arg;
	struct vmspace *myvm = vmspace_acquire_ref(curproc);
	struct thread *td = curthread;
	u_int id = self->rw_id;

	thread_lock(td);
	sched_bind(td, self->rw_cpu);
	thread_unlock(td);
	for (;;) {
		struct rio_srcio *srcio;
		struct riocb *iocb;
		enum rio_status status;
		int error;

		/* TODO: Work stealing! */
		if ((error = rio_worker_dequeue(self, &srcio)) == ESHUTDOWN) {
			break;
		}
		if (error == ESRCH) {
			/* Relinquish vmspace on user process exit. */
			rio_vmspace_switch(myvm, id);
			continue;
		}
		/* Check for close/exit. */
		status = rio_status(srcio->rs_sc);
		if (__predict_false(status != RIO_OPEN)) {
			/*
			 * Check if the user process is exiting.  The above
			 * ESRCH check is for handling a wakeup when our queue
			 * was empty.  This check handles the final dequeue when
			 * the queue was not empty.
			 *
			 * There is no need to do this when there is more in the
			 * queue, because the next thing will switch vmspace for
			 * us anyway.
			 */
			if (STAILQ_EMPTY(&self->rw_srcios) &&
			    status == RIO_EXITING) {
				rio_vmspace_switch(myvm, id);
			}
			rio_srcio_error(srcio, ECANCELED);
			continue;
		}
		/* Check if canceled while in queue. */
		iocb = rio_srcio_iocb(srcio);
		if (__predict_false(riocb_canceled(iocb))) {
			rio_srcio_error(srcio, ECANCELED);
			continue;
		}
		atomic_store_int(&iocb->rio_error, EINPROGRESS);
		self->rw_handler(srcio, id);
	}
	rio_vmspace_switch(myvm, id);
	vmspace_free(myvm);
	mtx_lock(&self->rw_lock);
	self->rw_done = true;
	cv_broadcast(&self->rw_cond);
	mtx_unlock(&self->rw_lock);
	kproc_exit(0);
}

static struct proc *rio_issuers;

static inline int
rio_issuer_init(struct rio_issuer *issuer, u_int cpu)
{
	int error;

	issuer->ri_cpu = cpu;
	issuer->ri_threads = 0;
	mtx_init(&issuer->ri_lock, "rio issuer lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&issuer->ri_cond, "rio issuer cond");
	STAILQ_INIT(&issuer->ri_srcs);
	/* TODO: always initialize context, but add threads on demand */
	for (u_int i = 0; i < rio_flow_issuer_threads; i++) {
		/* Spawn issuer threads in the proc0 kernel process. */
		if ((error = kproc_kthread_add(rio_issuer_thread, issuer,
		    &rio_issuers, NULL, 0, 0, "rio", "issue %u.%u", cpu, i))
		    != 0) {
			/* TODO: error handling */
			printf("%s: kproc_kthread_add: %d\n", __func__, error);
			return (error);
		}
		issuer->ri_threads++;
	}
	return (0);
}

static inline int
rio_worker_init(struct rio_worker *worker, rio_srcio_handler_f *handler,
    const char *classname, u_int cpu, u_int i)
{
	static u_int nextid;
	int error;

	worker->rw_handler = handler;
	worker->rw_cpu = cpu;
	worker->rw_id = nextid++;
	worker->rw_done = false;
	mtx_init(&worker->rw_lock, "rio worker lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&worker->rw_cond, "rio worker cond");
	STAILQ_INIT(&worker->rw_srcios);
	/* Spawn each worker as its own kernel process. */
	/* TODO: always initialize context, but create processes on demand */
	if ((error = kproc_create(rio_worker_proc, worker, NULL, 0, 0,
	    "rio/%s %u.%u", classname, cpu, i)) != 0) {
		/* TODO: error handling */
		printf("%s: kproc_create: %d\n", __func__, error);
		return (error);
	}
	return (0);
}

static inline int
rio_workerclass_init_(struct rio_worker **workers, rio_srcio_handler_f *handler,
    const char *classname, u_int cpu, u_int n)
{
	int error;

	*workers = mallocarray(n, sizeof(**workers), M_RIO, M_WAITOK | M_ZERO);
	for (u_int i = 0; i < n; i++) {
		if ((error = rio_worker_init(*workers + i, handler, classname,
		    cpu, i)) != 0) {
			/* TODO: error handling */
			return (error);
		}
	}
	return (0);
}

#define rio_workerclass_init(flow, class, cpu) \
	rio_workerclass_init_(&(flow)->rf_##class, rio_srcio_##class, #class, \
	    cpu, rio_flow_##class##_workers)

static inline uma_zone_t
rio_zcreate(const char *name, size_t size)
{
	return (uma_zcreate(name, size, NULL, NULL, NULL, NULL, UMA_ALIGN_PTR,
	    0));
}

static int
rio_load(void)
{
	u_int cpu;
	int error;

	rio_doom = taskqueue_create("rio doom", M_WAITOK | M_ZERO,
	    taskqueue_thread_enqueue, &rio_doom);
	taskqueue_start_threads(&rio_doom, 1, PWAIT, "rio doom taskq");

	rio_src_zone = rio_zcreate("rio src", sizeof(struct rio_src));
	rio_srcio_zone = rio_zcreate("rio src+io", sizeof(struct rio_srcio));
	rio_srcio_ext_zone = rio_zcreate("rio src+io+extra",
	    sizeof(struct rio_srcio_ext));

	cv_init(&rio_shutdown_cond, "rio shutdown cond");

	CPU_FOREACH(cpu) {
		struct rio_flow *flow;

		flow = DPCPU_ID_PTR(cpu, rio_flow);
		if ((error = rio_issuer_init(&flow->rf_issuer, cpu)) != 0) {
			/* TODO: error handling */
			return (error);
		}
		if ((error = rio_workerclass_init(flow, read, cpu)) != 0) {
			/* TODO: error handling */
			return (error);
		}
		if ((error = rio_workerclass_init(flow, write, cpu)) != 0) {
			/* TODO: error handling */
			return (error);
		}
		if ((error = rio_workerclass_init(flow, sync, cpu)) != 0) {
			/* TODO: error handling */
			return (error);
		}
		/* TODO: more worker classes? */
	}
	return (0);
}

static inline void
rio_issuer_destroy(struct rio_issuer *issuer)
{
	mtx_lock(&issuer->ri_lock);
	cv_broadcast(&issuer->ri_cond);
	while (issuer->ri_threads > 0) {
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
	}
	MPASS(STAILQ_EMPTY(&issuer->ri_srcs));
	mtx_unlock(&issuer->ri_lock);
	mtx_destroy(&issuer->ri_lock);
	cv_destroy(&issuer->ri_cond);
}

static inline void
rio_worker_destroy(struct rio_worker *worker)
{
	mtx_lock(&worker->rw_lock);
	cv_signal(&worker->rw_cond);
	while (!worker->rw_done) {
		cv_wait(&worker->rw_cond, &worker->rw_lock);
	}
	MPASS(STAILQ_EMPTY(&worker->rw_srcios));
	mtx_unlock(&worker->rw_lock);
	mtx_destroy(&worker->rw_lock);
	cv_destroy(&worker->rw_cond);
}

static inline void
rio_workerclass_destroy_(struct rio_worker *workers, u_int n)
{
	for (u_int i = 0; i < n; i++) {
		rio_worker_destroy(workers + i);
	}
}

#define rio_workerclass_destroy(flow, class) \
	rio_workerclass_destroy_((flow)->rf_##class, \
	    rio_flow_##class##_workers)

static int
rio_shutdown(void)
{
	u_int cpu;

	mtx_lock(&rio_shutdown_lock);
	atomic_store_rel_int(&rio_shutdown_pending, true);
	while (rio_open_count > 0) {
		cv_wait(&rio_shutdown_cond, &rio_shutdown_lock);
	}
	mtx_unlock(&rio_shutdown_lock);
	CPU_FOREACH(cpu) {
		struct rio_flow *flow;

		flow = DPCPU_ID_PTR(cpu, rio_flow);
		rio_issuer_destroy(&flow->rf_issuer);
		rio_workerclass_destroy(flow, read);
		rio_workerclass_destroy(flow, write);
		rio_workerclass_destroy(flow, sync);
		/* TODO: more worker classes? */
	}
	taskqueue_quiesce(rio_doom);
	taskqueue_free(rio_doom);
	uma_zdestroy(rio_src_zone);
	uma_zdestroy(rio_srcio_zone);
	uma_zdestroy(rio_srcio_ext_zone);
	cv_destroy(&rio_shutdown_cond);
	return (0);
}

static int
rio_modload(struct module *module, int cmd, void *arg)
{
	int error = 0;

	switch (cmd) {
	case MOD_LOAD:
		error = rio_load();
		break;
	case MOD_SHUTDOWN:
		error = rio_shutdown();
		break;
	default:
		error = EOPNOTSUPP;
		break;
	}
	return (error);
}

static moduledata_t rio_mod = {
	"rio",
	&rio_modload,
	NULL
};

DECLARE_MODULE(rio, rio_mod, SI_SUB_VFS, SI_ORDER_ANY);
MODULE_VERSION(rio, 0);
