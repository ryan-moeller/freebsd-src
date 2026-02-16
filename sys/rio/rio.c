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
#include <sys/bitstring.h>
#include <sys/buf.h>
#include <sys/condvar.h>
#include <sys/conf.h>
#include <sys/counter.h>
#include <sys/exterrvar.h>
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
#include <sys/protosw.h>
#include <sys/ptrace.h>
#include <sys/queue.h>
#include <sys/rio.h>
#include <sys/sched.h>
#include <sys/smp.h>
#include <sys/sockbuf.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
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

#include <ck_ec.h>
#include <ck_ring.h>

#include <geom/geom.h>

#include <net/vnet.h>

#include <security/mac/mac_framework.h>

#include <vm/pmap.h>
#include <vm/uma.h>
#include <vm/vm_extern.h>
#include <vm/vm_kern.h>
#include <vm/vm_map.h>
#include <vm/vm_page.h>
#include <vm/vnode_pager.h>

#include "rio_internal.h"
#include "rio_io.h"
#include "rio_selector.h"

FEATURE(rio, "Ring I/O");

MALLOC_DEFINE(M_RIO, "rio", "rio data structures");

static SYSCTL_NODE(_kern, OID_AUTO, rio, CTLFLAG_RD | CTLFLAG_MPSAFE, NULL,
    "Ring IO configuration");

/*
 * We want to stay on the same CPU as the user thread accessing the IO buffers,
 * for cache locality.  Source (flow) scheduling should select the local CPU
 * issuer until local workers are all busy.  But, we also want to utilize idle
 * CPU time to minimize latency and maximize throughput.  Optimizing the balance
 * of these priorities is the role of the policy.  Different policies make
 * tradeoffs to optimize for a particular objective.
 *
 * XXX: Scheduling avoids locking every issuer so may see stale reads, but it's
 * good enough for the purpose of load balancing.
 */
struct rio_src_scheduler {
	struct rio_selector	rss_sel;
	struct mtx	rss_lock;
	u_int		*rss_affscore;	/* affinity scores */
	int		rss_phase;	/* round-robin index */
};

static void rio_src_scheduler_init(struct rio_src_scheduler *);
static void rio_src_scheduler_destroy(struct rio_src_scheduler *);

struct rio_src {
	struct rio_softc	*rs_sc;		/* io source context */
	u_int			rs_attention;	/* issuer credits */
	STAILQ_ENTRY(rio_src)	rs_srcs;
};
STAILQ_HEAD(rio_srcs, rio_src);

typedef u_int rio_src_scheduler_f(struct rio_src *);

/*
 * TODO: Improve the set of useful scheduling policies.
 *
 * Useful means different things, so policies are defined broadly by two
 * parameters: the behavior, and the configuration.
 *
 * Behavior encapsulates run-time decision-making.
 *
 * Configuration is split across two planes: user and system.  User policy
 * config is controlled by ioctls, system policy config is controlled by
 * sysctls and tunables.
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
static rio_src_scheduler_f rio_src_scheduler_soft_affinity;
static rio_src_scheduler_f rio_src_scheduler_least_loaded;
static rio_src_scheduler_f rio_src_scheduler_round_robin;
static rio_src_scheduler_f rio_src_scheduler_local_flow;

static rio_src_scheduler_f *rio_src_policies[] = {
	[RIO_POLICY_SOFT_AFFINITY] = rio_src_scheduler_soft_affinity,
	[RIO_POLICY_LEAST_LOADED] = rio_src_scheduler_least_loaded,
	[RIO_POLICY_ROUND_ROBIN] = rio_src_scheduler_round_robin,
	[RIO_POLICY_LOCAL_FLOW] = rio_src_scheduler_local_flow,
};

#if 0 /* TODO */
/* custom policy configuration */
#define RIO_POLICY_CUSTOM		UINT_MAX
#endif

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

static u_int rio_flow_socket_workers = 4;
SYSCTL_UINT(_kern_rio_flow, OID_AUTO, socket_workers, CTLFLAG_RDTUN,
    &rio_flow_socket_workers, 0, "Max number of socket workers per CPU flow");

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
static bool rio_shutdown_pending;

/* Dimensions for worker affinity. */
static u_int rio_max_flow_workers;
static u_int rio_max_workers;

static inline void
rio_vmspace_init(struct vmspace *vm)
{
	bitstr_t *waff; /* worker affinity */

	waff = bit_alloc(rio_max_workers, M_RIO, M_WAITOK | M_ZERO);
	if (!atomic_cmpset_ptr((uintptr_t *)&vm->vm_rio, 0, (uintptr_t)waff)) {
		free(waff, M_RIO);
	}
}

/* TODO: belongs in sys/bitstring.h */
static inline void
bit_set_atomic(bitstr_t *bitstr, size_t bit)
{
	atomic_set_long(&bitstr[_bit_idx(bit)], _bit_mask(bit));
}
static inline void
bit_clear_atomic(bitstr_t *bitstr, size_t bit)
{
	atomic_clear_long(&bitstr[_bit_idx(bit)], _bit_mask(bit));
}

static inline void
rio_vmspace_switch(struct vmspace *vm, u_int id)
{
	struct vmspace *oldvm = curproc->p_vmspace;

	if (vm != oldvm) {
		if (oldvm->vm_rio != NULL) {
			bit_clear_atomic(oldvm->vm_rio, id);
		}
		if (vm->vm_rio != NULL) {
			bit_set_atomic(vm->vm_rio, id);
		}
		vmspace_switch_aio(vm);
	}
}

VNET_DEFINE_STATIC(bitstr_t *, rio_vnet_flow_affinity);
#define V_rio_vnet_flow_affinity VNET(rio_vnet_flow_affinity)

static void
rio_vnet_init(const void *arg __unused)
{
	V_rio_vnet_flow_affinity = bit_alloc(mp_ncpus, M_RIO,
	    M_WAITOK | M_ZERO);
}
VNET_SYSINIT(rio_vnet_init, SI_SUB_PROTO_BEGIN, SI_ORDER_ANY, rio_vnet_init,
    NULL);

static void
rio_vnet_uninit(const void *arg __unused)
{
	free(V_rio_vnet_flow_affinity, M_RIO);
}
VNET_SYSUNINIT(rio_vnet_uninit, SI_SUB_PROTO_BEGIN, SI_ORDER_ANY,
    rio_vnet_uninit, NULL);

static inline void
rio_vnet_switch(struct vnet *vnet)
{
	if (vnet != curvnet) {
		if (curvnet != NULL) {
			bit_clear_atomic(V_rio_vnet_flow_affinity, curcpu);
		}
		if (vnet != NULL) {
			bit_set_atomic(VNET_VNET(vnet, rio_vnet_flow_affinity),
			    curcpu);
		}
		/* The network stack updates curvnet itself as needed. */
	}
}

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
	struct task	sc_destroy_task;/* destruction task */
	enum rio_status	sc_status;	/* softc/process status */
	struct sx	sc_status_lock;	/* block status change while issuing */
	struct mtx	sc_issuer_lock;	/* for submission dequeue accounting */
	struct rio_src_scheduler	sc_sched;	/* submit scheduler */
	/* TODO: flags? counters? */
	struct rio_config	sc_config;
	/* TODO: policy metadata */
	/*
	 * XXX: As a workaround for not having context in wake32, we have to
	 * embed the CK event counter ops and mode in every softc so the ops
	 * can be used to derive a pointer to the softc.
	 */
	struct ck_ec_ops	sc_ec_umtx_ops;
	struct ck_ec_mode	sc_ec_umtx_mode;
	LIST_ENTRY(rio_softc)	sc_handles;
};

/* list of all handles for debugging, protected by shutdown lock */
static LIST_HEAD(, rio_softc) rio_handles;

static inline int
rio_open(struct rio_softc **scp)
{
	struct rio_softc *sc;

	sc = malloc(sizeof(*sc), M_RIO, M_WAITOK | M_ZERO);
	rio_src_scheduler_init(&sc->sc_sched);
	mtx_lock(&rio_shutdown_lock);
	if (rio_shutdown_pending) {
		mtx_unlock(&rio_shutdown_lock);
		rio_src_scheduler_destroy(&sc->sc_sched);
		free(sc, M_RIO);
		return (EXTERROR(ESHUTDOWN, "system shutting down"));
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
	if (rio_open_count == 0 && rio_shutdown_pending) {
		cv_signal(&rio_shutdown_cond);
	}
	mtx_unlock(&rio_shutdown_lock);
	sx_destroy(&sc->sc_status_lock);
	mtx_destroy(&sc->sc_issuer_lock);
	rio_src_scheduler_destroy(&sc->sc_sched);
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
	u_int inflight = rio_inflight(sc);
	u_int limit = sc->sc_config.rio_cqlen - 1;

	/* Enforce inflight < cqlen so completion cannot block workers. */
	MPASS(inflight <= limit);
	if (inflight == limit) {
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
	    rio_inflight(sc) == 0) {
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
	if (shmfd->shm_path != NULL) {
		return (EXTERROR(EINVAL, "expected anonymous shm"));
	}
	if (conf->rio_policy_id >= nitems(rio_src_policies)) {
		/* TODO: custom policy */
		return (EXTERROR(EINVAL, "invalid policy id"));
	}
	if ((error = rio_open(&sc)) != 0) {
		return (error);
	}
	memcpy(&sc->sc_config, conf, sizeof(*conf));
	size = rio_config_size(conf);
	/* The size is validated by shm_map. */
	if ((error = shm_map(fp, size, 0, (void **)&sc->sc_rio)) != 0) {
		rio_close(sc);
		/* TODO: shm_map could set better error info */
		return (EXTERROR(error, "shm_map failed"));
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

static void rio_schedule(struct rio_softc *);

static int
rio_submit(struct file *fp, struct thread *td)
{
	struct shmfd *shmfd;
	struct rio_softc *sc;

	MPASS(fp->f_type == DTYPE_SHM);
	shmfd = fp->f_data;
	if (__predict_false((sc = shmfd->shm_rio) == NULL)) {
		return (EXTERROR(ENOTTY, "rio is not configured"));
	}
	if (__predict_false(sc->sc_proc != td->td_proc)) {
		return (EXTERROR(EDOOFUS, "rio is not transferrable"));
	}
	rio_schedule(sc);
	return (0);
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

static uma_zone_t rio_src_zone;

/* TODO: tuning, policy */
static u_int rio_attention_span = 1024;
SYSCTL_UINT(_kern_rio, OID_AUTO, attention_span, CTLFLAG_RW,
    &rio_attention_span, 0, "Single-source I/O batch size");

/* issuer/worker kickstart */
static struct taskqueue *rio_kick;

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
	bool			ri_shutdown;
	struct unrhdr		*ri_unr;
	struct rio_issuer_arg	*ri_tasks;	/* for adding threads */
};

struct rio_issuer_arg {
	struct rio_issuer	*ria_issuer;
	struct task		ria_task;
};

static inline u_int
rio_issuer_arg_idx(struct rio_issuer_arg *ria)
{
	return (ria - ria->ria_issuer->ri_tasks);
}

static inline int
rio_issuer_kick_check(struct rio_issuer *issuer)
{
	mtx_assert(&issuer->ri_lock, MA_OWNED);
	/* TODO: policy for threshold? */
	if (issuer->ri_threads < MIN(issuer->ri_len, rio_flow_issuer_threads)) {
		int idx = alloc_unrl(issuer->ri_unr);

		MPASS(idx != -1);
		issuer->ri_threads++;
		return (idx);
	}
	return (-1);
}

static inline void
rio_issuer_kick(struct rio_issuer *issuer, u_int idx)
{
	taskqueue_enqueue(rio_kick, &issuer->ri_tasks[idx].ria_task);
}

static inline void
rio_issuer_enqueue(struct rio_issuer *issuer, struct rio_src *src)
{
	int idx;

	/* Refill attention credits when enqueued to the back. */
	src->rs_attention = rio_attention_span;
	mtx_lock(&issuer->ri_lock);
	STAILQ_INSERT_TAIL(&issuer->ri_srcs, src, rs_srcs);
	issuer->ri_len++;
	idx = rio_issuer_kick_check(issuer);
	cv_signal_any(&issuer->ri_cond);
	mtx_unlock(&issuer->ri_lock);
	if (idx != -1) {
		rio_issuer_kick(issuer, idx);
	}
}

static inline void
rio_issuer_enqueue_front(struct rio_issuer *issuer, struct rio_src *src)
{
	int idx;

	/* Keep existing credits when enqueued to the front. */
	src->rs_attention = src->rs_attention;
	mtx_lock(&issuer->ri_lock);
	STAILQ_INSERT_HEAD(&issuer->ri_srcs, src, rs_srcs);
	issuer->ri_len++;
	idx = rio_issuer_kick_check(issuer);
	cv_signal_any(&issuer->ri_cond);
	mtx_unlock(&issuer->ri_lock);
	if (idx != -1) {
		rio_issuer_kick(issuer, idx);
	}
}

static sbintime_t rio_flow_issuer_idle = SBT_1S;
SYSCTL_SBINTIME_MSEC(_kern_rio_flow, OID_AUTO, issuer_idle_ms, CTLFLAG_RW,
    &rio_flow_issuer_idle, "Issuer idle timeout (ms)");

static inline int
rio_issuer_dequeue(struct rio_issuer *issuer, struct rio_src **srcp)
{
	struct rio_src *src;

	mtx_lock(&issuer->ri_lock);
	for (;;) {
		if (__predict_false(issuer->ri_shutdown)) {
			mtx_unlock(&issuer->ri_lock);
			return (ESHUTDOWN);
		}
		if ((src = STAILQ_FIRST(&issuer->ri_srcs)) != NULL) {
			break;
		}
		if (cv_timedwait_sbt(&issuer->ri_cond, &issuer->ri_lock,
		    rio_flow_issuer_idle, SBT_1MS, 0) == EWOULDBLOCK) {
			if (!STAILQ_EMPTY(&issuer->ri_srcs)) {
				continue;
			}
			mtx_unlock(&issuer->ri_lock);
			return (EWOULDBLOCK);
		}
	}
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
	bool			rse_charge;
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
		rse->rse_charge = false;
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
	inbed = atomic_fetchadd_int(&pbp->bio_inbed, 1) + 1;
	if (pbp->bio_children == inbed) {
		pbp->bio_done(pbp);
	}
	rio_bio_destroy(bp);
}

static void
rio_bio_complete(struct bio *bp)
{
	struct rio_srcio *srcio = bp->bio_caller1;
	struct riocb *kiocb = rio_io_kiocb(srcio->rs_io);

	kiocb->rio_status = bp->bio_completed;
	kiocb->rio_error = bp->bio_error;
	if (bp->bio_completed > 0) {
		struct proc *p = srcio->rs_sc->sc_proc;

		/*
		 * The resource usage can't be charged to a particular user
		 * thread.
		 */
		PROC_STATLOCK(p);
		switch (bp->bio_cmd) {
		case BIO_READ:
			p->p_ru.ru_inblock += btodb(bp->bio_completed);
			break;
		case BIO_WRITE:
			p->p_ru.ru_oublock += btodb(bp->bio_completed);
			break;
		}
		PROC_STATUNLOCK(p);
	}
	rio_srcio_complete(srcio);
	rio_bio_destroy(bp);
}

/* context for cdev bio ops */
struct rio_cdev {
	struct cdevsw	*rcd_csw;
	struct cdev	*rcd_dev;
	int		rcd_ref;
	int		rcd_cmd;
	int		rcd_bsize;
	int		rcd_maxio;
	size_t		rcd_iovcnt;
};

static inline void
rio_cdev_unref(struct rio_cdev *rcd)
{
	dev_relthread(rcd->rcd_dev, rcd->rcd_ref);
}

/* special return code for rio_cdev_setup */
#define RIO_CDEV_FALLBACK -1

static inline int
rio_cdev_setup(struct rio_cdev *rcd, struct rio_srcio *srcio)
{
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct file *fp = io->rio_fd_file;
	struct vnode *vp;
	struct cdevsw *csw;
	struct cdev *dev;
	int maxio;

	switch (rio_io_cmd(io)) {
	case RIO_READ:
		rcd->rcd_cmd = BIO_READ;
		break;
	case RIO_WRITE:
		rcd->rcd_cmd = BIO_WRITE;
		break;
	/* TODO: BIO_DELETE? BIO_FLUSH? */
	default:
		return (RIO_CDEV_FALLBACK);
	}
	if (fp == NULL || fp->f_type != DTYPE_VNODE) {
		return (RIO_CDEV_FALLBACK);
	}
	vp = fp->f_vnode;
	rcd->rcd_bsize = vp->v_bufobj.bo_bsize;
	if (vp->v_type != VCHR || rcd->rcd_bsize == 0) {
		return (RIO_CDEV_FALLBACK);
	}
	if ((csw = devvn_refthread(vp, &dev, &rcd->rcd_ref)) == NULL) {
		return (ENXIO);
	}
	rcd->rcd_csw = csw;
	rcd->rcd_dev = dev;
	if ((csw->d_flags & D_DISK) == 0) {
		rio_cdev_unref(rcd);
		return (RIO_CDEV_FALLBACK);
	}
	rcd->rcd_iovcnt = rio_io_vectored(io) ? kiocb->rio_length : 0;
	if ((dev->si_flags & SI_NOSPLIT) != 0 && rcd->rcd_iovcnt > 1) {
		rio_cdev_unref(rcd);
		return (RIO_CDEV_FALLBACK);
	}
	if (__predict_false((maxio = dev->si_iosize_max) < PAGE_SIZE)) {
		printf("WARNING: %s si_iosize_max=%d, using DFLTPHYS.\n",
		    devtoname(dev), maxio);
		maxio = DFLTPHYS;
	}
	rcd->rcd_maxio = MIN(maxio, maxphys);
	/* TODO: limits a la max_buf_aio et cetera */
	return (0);
}

static inline int
rio_cdev_bio_strategy(struct rio_cdev *rcd, struct rio_srcio *srcio)
{
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct proc *p = sc->sc_proc;
	vm_map_t map = &sc->sc_vmspace->vm_map;
	struct iovec *iov = NULL;
	struct bio *pbp;
	size_t resid, iovcnt = rcd->rcd_iovcnt;
	off_t offset = kiocb->rio_offset;
	int error;

	if (iovcnt > 0) {
		ssize_t iovsize, result;

		iov = mallocarray(iovcnt, sizeof(*iov), M_IOV, M_WAITOK);
		/*
		 * Avoid pushing this down to a worker for copyiniov vmspace.
		 * proc_readmem will validate the rio_iov pointer in the user's
		 * vmspace and do the vm song and dance to copy in the iovecs.
		 * Pushing this down to a worker would complicate fallback to
		 * regular file I/O in case of error.
		 *
		 * What kind of error might occur that would not also occur in
		 * the fallback path?  This fast path requires buffer lengths
		 * to be a multiple of the vnode buffer size and less than the
		 * device max iosize, whereas the fallback through physio will
		 * make adjustments to the I/O requests to fit the device's
		 * constraints.
		 */
		iovsize = iovcnt * sizeof(*iov);
		PHOLD(p);
		result = proc_readmem(curthread, p, (vm_offset_t)kiocb->rio_iov,
		    iov, iovsize);
		PRELE(p);
		if (__predict_false(result != iovsize)) {
			error = EFAULT;
			goto free;
		}
		resid = 0;
		for (int i = 0; i < iovcnt; i++) {
			size_t len = iov[i].iov_len;

			if (len % rcd->rcd_bsize != 0 || len > rcd->rcd_maxio) {
				error = RIO_CDEV_FALLBACK;
				goto free;
			}
			resid += len;
		}
	} else {
		resid = kiocb->rio_length;
		if (resid % rcd->rcd_bsize != 0 || resid > rcd->rcd_maxio) {
			error = RIO_CDEV_FALLBACK;
			goto free;
		}
	}
	/* TODO: buffer count limits a la aio */
	pbp = g_alloc_bio();
	pbp->bio_cmd = rcd->rcd_cmd;
	pbp->bio_offset = offset;
	pbp->bio_length = resid;
	pbp->bio_caller1 = srcio;
	pbp->bio_done = rio_bio_complete;
	if (iovcnt > 0) {
		struct bio **children;

		children = mallocarray(iovcnt, sizeof(*children), M_TEMP,
		    M_WAITOK | M_ZERO);
		for (int i = 0; i < iovcnt; i++) {
			struct bio *bp = g_duplicate_bio(pbp);

			children[i] = bp;
			bp->bio_offset = offset;
			bp->bio_done = rio_bio_childdone;
			if ((error = rio_bio_bufsetup(bp, rcd->rcd_dev, map,
			    iov[i].iov_base, iov[i].iov_len)) != 0) {
				do {
					rio_bio_destroy(children[i]);
				} while (i-- > 0);
				free(children, M_TEMP);
				goto destroy;
			}
			offset += iov[i].iov_len;
		}
		for (int i = 0; i < iovcnt; i++) {
			rcd->rcd_csw->d_strategy(children[i]);
		}
		free(children, M_TEMP);
	} else {
		if ((error = rio_bio_bufsetup(pbp, rcd->rcd_dev, map,
		    kiocb->rio_buf, kiocb->rio_length)) != 0) {
			goto destroy;
		}
		rcd->rcd_csw->d_strategy(pbp);
	}
	free(iov, M_IOV);
	rio_cdev_unref(rcd);
	return (0);
destroy:
	g_destroy_bio(pbp);
free:
	free(iov, M_IOV);
	rio_cdev_unref(rcd);
	return (error);
}

/* Common context for blocking uio operations. */
struct rio_uio {
	struct uio	*ruio_uio;
	size_t		ruio_len;
	/* internal storage */
	struct uio	ruio__uio;
	struct iovec	ruio__iov;
	/* resource usage checkpoint */
	long	ruio_msgsnd;
	long	ruio_msgrcv;
	long	ruio_oublock;
	long	ruio_inblock;
};

static inline int
rio_uio_setup(struct rio_uio *ruio, struct rio_srcio *srcio)
{
	struct thread *td = curthread;
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
	ruio->ruio_msgsnd = td->td_ru.ru_msgsnd;
	ruio->ruio_msgrcv = td->td_ru.ru_msgrcv;
	ruio->ruio_oublock = td->td_ru.ru_oublock;
	ruio->ruio_inblock = td->td_ru.ru_inblock;
	return (0);
}

static inline ssize_t
rio_uio_completed(struct rio_uio *ruio, struct proc *p)
{
	struct thread *td = curthread;

	MPASS(td == ruio->ruio_uio->uio_td);

	/* The resource usage can't be charged to a particular user thread. */
	PROC_STATLOCK(p);
	p->p_ru.ru_msgsnd += td->td_ru.ru_msgsnd - ruio->ruio_msgsnd;
	p->p_ru.ru_msgrcv += td->td_ru.ru_msgrcv - ruio->ruio_msgrcv;
	p->p_ru.ru_oublock += td->td_ru.ru_oublock - ruio->ruio_oublock;
	p->p_ru.ru_inblock += td->td_ru.ru_inblock - ruio->ruio_inblock;
	PROC_STATUNLOCK(p);
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
	if ((error = rio_uio_setup(&ruio, srcio)) != 0) {
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
		kiocb->rio_status = rio_uio_completed(&ruio, sc->sc_proc);
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
	if ((error = rio_uio_setup(&ruio, srcio)) != 0) {
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
		kiocb->rio_status = rio_uio_completed(&ruio, p);
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

static inline bool
rio_soready(struct socket *so, sb_which which)
{
	return (which == SO_SND ? sowriteable(so) : soreadable(so));
}

static inline bool
riocb_canceled(struct riocb *iocb)
{
	return (atomic_load_int(&iocb->rio_error) == ECANCELED);
}

static inline struct rio_srcio *
rio_sockbuf_takefirst(struct sockbuf *sb)
{
	struct rio_srcio *srcio;

	if ((srcio = STAILQ_FIRST(&sb->sb_riosrcios)) != NULL) {
		STAILQ_REMOVE_HEAD(&sb->sb_riosrcios, rs_srcios);
	}
	return (srcio);
}

#ifdef RIO_SOCK_BUF_DEBUG
#define RIO_SOCK_BUF_LOCK(sb, which) ({ \
	printf("%s:%u LOCK sb=%p which=%d\n", __func__, __LINE__, sb, which); \
	SOCK_BUF_LOCK(sb, which); \
})
#define RIO_SOCK_BUF_LOCK_ASSERT(sb, which) ({ \
	printf("%s:%u LOCK ASSERT sb=%p which=%d\n", __func__, __LINE__, sb, which);\
	SOCK_BUF_LOCK_ASSERT(sb, which); \
})
#define RIO_SOCK_BUF_UNLOCK_ASSERT(sb, which) ({ \
	printf("%s:%u UNLOCK ASSERT sb=%p which=%d\n", __func__, __LINE__, sb, which);\
	SOCK_BUF_UNLOCK_ASSERT(sb, which); \
})
#define RIO_SOCK_BUF_UNLOCK(sb, which) ({ \
	printf("%s:%u UNLOCK sb=%p which=%d\n", __func__, __LINE__, sb, which);\
	SOCK_BUF_UNLOCK(sb, which); \
})
#else
#define RIO_SOCK_BUF_LOCK		SOCK_BUF_LOCK
#define RIO_SOCK_BUF_LOCK_ASSERT	SOCK_BUF_LOCK_ASSERT
#define RIO_SOCK_BUF_UNLOCK_ASSERT	SOCK_BUF_UNLOCK_ASSERT
#define RIO_SOCK_BUF_UNLOCK		SOCK_BUF_UNLOCK
#endif

static inline struct rio_srcio_ext *
rio_srcio_ext_sockbuf(struct rio_srcio_ext *rse, sb_which which)
{
	struct rio_srcio *srcio = &rse->rse_srcio;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = rio_io_kiocb(io);
	struct uio *uio = srcio->rs_uio;
	struct file *fp = io->rio_fd_file;
	struct socket *so = fp->f_data;
	struct sockbuf *sb = sobuf(so, which);
	struct thread *td = curthread;
	struct proc *p = sc->sc_proc;
	size_t orig, completed = rse->rse_completed;
	long ru;
	int flags, error = 0;

	/* We won't race with another RIO worker thanks to SB_RIO_RUNNING. */
	RIO_SOCK_BUF_UNLOCK(so, which);
	if (uio == NULL) {
		if (rio_io_vectored(io)) {
			if ((error = copyinuio(kiocb->rio_iov,
			    kiocb->rio_length, &uio)) != 0) {
				goto complete;
			}
		} else {
			struct iovec *iov = &rse->rse_iov;

			iov->iov_base = kiocb->rio_buf;
			iov->iov_len = kiocb->rio_length;
			uio = &rse->rse_uio;
			uio->uio_iov = iov;
			uio->uio_iovcnt = 1;
			uio->uio_resid = kiocb->rio_length;
			uio->uio_segflg = UIO_USERSPACE;
		}
		uio->uio_offset = kiocb->rio_offset;
		uio->uio_rw = which == SO_RCV ? UIO_READ : UIO_WRITE;
		srcio->rs_uio = uio;
	}
	uio->uio_td = td;
	orig = uio->uio_resid;
	rio_vnet_switch(so->so_vnet);
	flags = MSG_NBIO;
	switch (which) {
	case SO_SND:
		ru = td->td_ru.ru_msgsnd;
		if (!STAILQ_EMPTY(&sb->sb_riosrcios)) {
			flags |= MSG_MORETOCOME;
		}
#ifdef MAC
		error = mac_socket_check_send(fp->f_cred, so);
#endif
		if (__predict_true(error == 0)) {
			error = sousrsend(so, NULL, uio, NULL, flags, p);
			if (td->td_ru.ru_msgsnd != ru) {
				rse->rse_charge = true;
			}
		}
		break;
	case SO_RCV:
		ru = td->td_ru.ru_msgrcv;
#ifdef MAC
		error = mac_socket_check_receive(fp->f_cred, so);
#endif
		if (__predict_true(error == 0)) {
			error = soreceive(so, NULL, uio, NULL, NULL, &flags);
			if (td->td_ru.ru_msgrcv != ru) {
				rse->rse_charge = true;
			}
		}
		break;
	}
	completed += orig - uio->uio_resid;
	rse->rse_completed = completed;
	if (__predict_false(error == EWOULDBLOCK) &&
	    (completed == 0 || (so->so_state & SS_NBIO) == 0)) {
		struct riocb *iocb;

		RIO_SOCK_BUF_LOCK(so, which);
		/* TODO: empty counter */
		printf("%s: empty\n", __func__);
		if (rio_soready(so, which)) {
			/* Readied up while waiting for lock. */
			/* TODO: retry counter */
			printf("%s: retry\n", __func__);
			return (rse);
		}
		iocb = rio_srcio_iocb(srcio);
		if (__predict_false(riocb_canceled(iocb))) {
			RIO_SOCK_BUF_UNLOCK(so, which);
			if (completed == 0) {
				error = ECANCELED;
			}
			goto complete;
		}
		/*
		 * The socket is blocked and we can't complete early.  Keep our
		 * place at the head of the queue and wait for the next wakeup.
		 */
		STAILQ_INSERT_HEAD(&sb->sb_riosrcios, srcio, rs_srcios);
		return (NULL);
	}
	if (__predict_true(completed > 0)) {
		switch (__builtin_expect(0, error)) {
		case EINTR:
		case ERESTART:
		case EWOULDBLOCK:
			/* Allow early completion if interrupted. */
			error = 0;
			break;
		default:
			break;
		}
	}
complete:
	RIO_SOCK_BUF_UNLOCK_ASSERT(so, which);
	if (rse->rse_charge) {
		/*
		 * The resource usage can't be charged to a particular user
		 * thread.
		 */
		PROC_STATLOCK(p);
		switch (which) {
		case SO_SND:
			p->p_ru.ru_msgsnd++;
			break;
		case SO_RCV:
			p->p_ru.ru_msgrcv++;
			break;
		}
		PROC_STATUNLOCK(p);
	}
	if (__predict_true(error == 0)) {
		kiocb->rio_status = completed;
		kiocb->rio_error = 0;
		rio_srcio_complete(srcio);
	} else {
		rio_srcio_error(srcio, error);
	}
	RIO_SOCK_BUF_LOCK(so, which);
	return (rio_srcio_ext(rio_sockbuf_takefirst(sb)));
}

static void
rio_srcio_socket(struct rio_srcio *srcio, u_int id)
{
	struct rio_srcio_ext *rse = rio_srcio_ext(srcio);
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct file *fp = io->rio_fd_file;
	struct socket *so = fp->f_data;
	struct sockbuf *sb;
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	sb_which which;

	td->td_ucred = sc->sc_cred;
	rio_srcio_vmspace_switch(srcio, id);
	switch (rio_io_cmd(io)) {
	case RIO_READ:
		which = SO_RCV;
		break;
	case RIO_WRITE:
		which = SO_SND;
		break;
	/* TODO: other socket commands */
	default:
		__assert_unreachable();
	}
	sb = sobuf(so, which);
	RIO_SOCK_BUF_LOCK(so, which);
	MPASS((sb->sb_flags & SB_RIO_RUNNING) != 0);
	/* TODO: accept command? */
	if (__predict_false(SOLISTENING(so))) {
		/* Any queued commands are invalid for a listening socket. */
		do {
			rio_srcio_error(srcio, EINVAL);
		} while ((srcio = rio_sockbuf_takefirst(sb)) != NULL);
	} else {
		while ((rse = rio_srcio_ext_sockbuf(rse, which)) != NULL) {
			continue;
		}
	}
	sb->sb_flags &= ~SB_RIO_RUNNING;
	RIO_SOCK_BUF_UNLOCK(so, which);
	td->td_ucred = saved_cred;
}

void
sowakeup_rio(struct socket *so, sb_which which)
{
	struct sockbuf *sb = sobuf(so, which);

	RIO_SOCK_BUF_LOCK_ASSERT(so, which);
	MPASS(!STAILQ_EMPTY(&sb->sb_riosrcios));

	if ((sb->sb_flags & SB_RIO_RUNNING) == 0) {
		sb->sb_flags |= SB_RIO_RUNNING;
		taskqueue_enqueue(rio_kick, &sb->sb_riotask);
	}
}

static inline bool
rio_srcio_socket_enqueue(struct rio_srcio *srcio)
{
	struct rio_io *io = srcio->rs_io;
	struct file *fp = io->rio_fd_file;
	struct socket *so;
	struct sockbuf *sb;
	sb_which which;

	/*
	 * At this point, adapting the fo_aio_queue interface to a more generic
	 * continuation-passing style could be a logical next step.
	 *
	 * Alternatively, skip a few steps and come up with a generic CPS UIO
	 * interface for the kernel?
	 *
	 * Basic needs:
	 *  - an embedded uio member
	 *  - a completion callback
	 *  - a cancellation point callback? what is that called...
	 *  - queue linkage
	 *  - arbitrary context (struct inheritance via nesting)
	 *
	 * The completion callback is responsible for releasing resources, so
	 * embedding in an arbitrary struct for context similar to queue linkage
	 * is an option.  Looks a lot like kaiocb with less baggage.  Heck,
	 * embed it in kaiocb as a retrofit.  The AIO syscalls could be made to
	 * invoke RIO instead of the AIO plumbing.  This does seem messier than
	 * using RIO to implement AIO in librt/libc though.  Are syscalls
	 * proxied through libc/libsys?
	 *
	 * "Core I/O" is catchy... or iocore
	 */
	if (fp == NULL || fp->f_type != DTYPE_SOCKET) {
		return (false);
	}
	so = fp->f_data;
	/* TODO: accept command? */
	if (SOLISTENING(so)) {
		rio_srcio_error(srcio, EINVAL);
		return (true);
	}
	switch (rio_io_cmd(io)) {
	case RIO_READ:
		which = SO_RCV;
		break;
	case RIO_WRITE:
		which = SO_SND;
		break;
	default:
		rio_srcio_error(srcio, EINVAL);
		return (true);
	}
	RIO_SOCK_BUF_LOCK(so, which);
	sb = sobuf(so, which);
	STAILQ_INSERT_TAIL(&sb->sb_riosrcios, srcio, rs_srcios);
	if (rio_soready(so, which)) {
		sowakeup_rio(so, which);
	}
	RIO_SOCK_BUF_UNLOCK(so, which);
	return (true);
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
	const char		*rw_classname;
	u_int			rw_len;
	u_int			rw_cpu;
	u_int			rw_idx;
	u_int			rw_id;		/* global worker id */
	bool			rw_running;
	bool			rw_shutdown;
	struct task		rw_task;	/* create kproc */
};

static inline void
rio_worker_enqueue(struct rio_worker *worker, struct rio_srcio *srcio)
{
	bool kick;

	mtx_lock(&worker->rw_lock);
	STAILQ_INSERT_TAIL(&worker->rw_srcios, srcio, rs_srcios);
	worker->rw_len++;
	kick = !worker->rw_running;
	worker->rw_running = true;
	cv_signal(&worker->rw_cond);
	mtx_unlock(&worker->rw_lock);
	if (kick) {
		taskqueue_enqueue(rio_kick, &worker->rw_task);
	}
}

static sbintime_t rio_flow_worker_idle = SBT_1S;
SYSCTL_SBINTIME_MSEC(_kern_rio_flow, OID_AUTO, worker_idle_ms, CTLFLAG_RW,
    &rio_flow_worker_idle, "Worker idle timeout (ms)");

static inline int
rio_worker_dequeue(struct rio_worker *worker, struct rio_srcio **srciop)
{
	struct rio_srcio *srcio;
	int error = 0;

	mtx_lock(&worker->rw_lock);
	if (__predict_false(worker->rw_shutdown)) {
		return (ESHUTDOWN);
	}
	if (STAILQ_EMPTY(&worker->rw_srcios)) {
		error = cv_timedwait_sbt(&worker->rw_cond, &worker->rw_lock,
		    rio_flow_worker_idle, SBT_1MS, 0);
	}
	srcio = STAILQ_FIRST(&worker->rw_srcios);
	if (__predict_false(srcio == NULL)) {
		if (error == 0) {
			/* The idle worker must switch vmspace. */
			*srciop = NULL;
		} else {
			/* The idle worker timed out and must exit. */
			MPASS(error == EWOULDBLOCK);
			worker->rw_running = false;
		}
		mtx_unlock(&worker->rw_lock);
		return (error);
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
	struct rio_worker *rf_socket;
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
	const u_int flow_stride = rio_max_flow_workers;
	const u_int class_strides[] = {
		rio_flow_read_workers,
		rio_flow_write_workers,
		rio_flow_sync_workers,
		rio_flow_socket_workers,
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
	bitstr_t *waff; /* worker affinity */
	size_t id;

	waff = (bitstr_t *)atomic_swap_ptr((uintptr_t *)&vm->vm_rio, 0);
	if (waff == NULL) {
		return;
	}
	/* Signal any sleeping workers using this vmspace. */
	bit_foreach(waff, rio_max_workers, id) {
		struct rio_worker *worker = rio_worker_lookup(id);

		mtx_lock(&worker->rw_lock);
		if (worker->rw_len == 0) {
			cv_signal(&worker->rw_cond);
		}
		mtx_unlock(&worker->rw_lock);
	}
	free(waff, M_RIO);
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

static inline ssize_t
rio_flow_score(u_int cpu, bitstr_t *waff)
{
	size_t start = cpu * rio_max_flow_workers;
	size_t end = start + rio_max_flow_workers;
	ssize_t score;

	bit_count(waff, start, end, &score);
	return (score);
}

static inline void
rio_src_scheduler_init(struct rio_src_scheduler *sched)
{
	rio_selector_init(&sched->rss_sel, mp_ncpus);
	sched->rss_affscore = mallocarray(mp_ncpus,
	    sizeof(*sched->rss_affscore), M_RIO, M_WAITOK);
	mtx_init(&sched->rss_lock, "rio src scheduler lock", NULL, MTX_DEF);
}

static inline bool
rio_src_scheduler_local_flow_preferable(void)
{
	struct rio_flow *flow = DPCPU_PTR(rio_flow);
	u_int level = flow->rf_issuer.ri_len;
	u_int watermark = 0;
	u_int cpu;

	if (level < rio_flow_issuer_threads) {
		return (true);
	}
	CPU_FOREACH(cpu) {
		struct rio_flow *flow = DPCPU_ID_PTR(cpu, rio_flow);

		watermark += flow->rf_issuer.ri_len;
	}
	return (level * mp_ncpus <= watermark);
}

/*
 * Source Scheduler: Soft Affinity
 *
 * The Soft Affinity scheduler prefers locality until worker activity reaches a
 * high water mark.  Then, the least busy remote flow in the local domain with
 * the most affine workers for the vmspace is selected.  Ties are resolved
 * round-robin.
 */
static u_int
rio_src_scheduler_soft_affinity(struct rio_src *src)
{
	/* Use the local flow if it's not too busy. */
	if (rio_src_scheduler_local_flow_preferable()) {
		return (curcpu);
	}
	/* Fall back to the least busy remote flow in the local domain. */
	return (rio_src_scheduler_least_loaded(src));
}

#define curdomain pcpu_find(curcpu)->pc_domain

/*
 * Source Scheduler: Least Loaded
 *
 * The Least Loaded scheduler selects the flow in the local domain with the
 * shallowest issuer queue.  Ties are resolved round-robin from the flows with
 * affine workers or from all domain-local flows if none are affine.
 *
 * TODO: Additional preference for flows with *running* issuers.
 */
static u_int
rio_src_scheduler_least_loaded(struct rio_src *src)
{
	struct rio_src_scheduler *sched = &src->rs_sc->sc_sched;
	struct rio_selector *sel = &sched->rss_sel;
	bitstr_t *waff = src->rs_sc->sc_vmspace->vm_rio; /* worker affinity */
	int local_domain = curdomain;
	u_int count, cpu, stop, max;

	mtx_lock(&sched->rss_lock);
	rio_selector_reset(sel);
	CPU_FOREACH(cpu) {
		struct rio_flow *flow = DPCPU_ID_PTR(cpu, rio_flow);
		struct rio_issuer *issuer = &flow->rf_issuer;
		ssize_t flow_score;

		if (pcpu_find(cpu)->pc_domain != local_domain) {
			continue;
		}
		flow_score = rio_flow_score(cpu, waff);
		rio_selector_insert(sel, issuer->ri_len, flow_score > 0);
		sched->rss_affscore[cpu] = flow_score;
	}
	MPASS(sel->rs_min != UINT_MAX);
	/* Look for the best candidates for round-robin. */
	count = 0;
	/* The ideal candidates are affine and idle. */
	bit_and(sel->rs_affine, sel->rs_empty, sel->rs_candidates, sel->rs_n);
	bit_foreach(sel->rs_candidates, sel->rs_n, cpu) {
		sel->rs_indices[count++] = cpu;
	}
	if (count > 0) {
		goto rr;
	}
	/* Next best are any idle flows. */
	bit_foreach(sel->rs_empty, sel->rs_n, cpu) {
		sel->rs_indices[count++] = cpu;
	}
	if (count > 0) {
		goto rr;
	}
	/*
	 * If no flows are idle, select from the eligible flows with minimum
	 * queue depth and maximum affinity.
	 */
	stop = sel->rs_n - 1;
	max = 0;
	bit_foreach(sel->rs_minimum, sel->rs_n, cpu) {
		u_int score = sched->rss_affscore[cpu];

		if (score < max) {
			continue;
		}
		if (score > max) {
			max = score;
			bit_nclear(sel->rs_candidates, 0, stop);
		}
		bit_set(sel->rs_candidates, cpu);
	}
	bit_foreach(sel->rs_candidates, sel->rs_n, cpu) {
		sel->rs_indices[count++] = cpu;
	}
rr:
	MPASS(count > 0);
	/* TODO: stride */
	cpu = sel->rs_indices[sched->rss_phase++ % count];
	mtx_unlock(&sched->rss_lock);
	return (cpu);
}

/*
 * Source Scheduler: Round Robin
 *
 * Flows are selected sequentially from the local domain.  Why use many rule
 * when one do trick?
 */
static u_int
rio_src_scheduler_round_robin(struct rio_src *src)
{
	struct rio_src_scheduler *sched = &src->rs_sc->sc_sched;
	struct rio_selector *sel = &sched->rss_sel;
	cpuset_t *domain = &cpuset_domain[curdomain];
	u_int cpu, count = 0;

	mtx_lock(&sched->rss_lock);
	CPU_FOREACH_ISSET(cpu, domain) {
		sel->rs_indices[count++] = cpu;
	}
	MPASS(count > 0);
	/* TODO: stride */
	cpu = sel->rs_indices[sched->rss_phase++ % count];
	mtx_unlock(&sched->rss_lock);
	return (cpu);
}

/*
 * Source Scheduler: Local Flow
 *
 * Only the local flow is selected.  There's no place like home.
 */
static u_int
rio_src_scheduler_local_flow(struct rio_src *src __unused)
{
	return (curcpu);
}

/* TODO: custom policy scheduler */

static inline void
rio_src_scheduler_destroy(struct rio_src_scheduler *sched)
{
	rio_selector_destroy(&sched->rss_sel);
	free(sched->rss_affscore, M_RIO);
	mtx_destroy(&sched->rss_lock);
}

static inline void
rio_schedule(struct rio_softc *sc)
{
	struct rio_src *src;
	struct rio_flow *flow;
	rio_src_scheduler_f *schedule;
	u_int cpu;

	src = uma_zalloc(rio_src_zone, M_WAITOK);
	src->rs_sc = sc;
	schedule = rio_src_policies[sc->sc_config.rio_policy_id];
	cpu = schedule(src);
	MPASS(cpu < mp_ncpus);
	flow = DPCPU_ID_PTR(cpu, rio_flow);
	rio_issuer_enqueue(&flow->rf_issuer, src);
}

/*
 * The tricky bit is that we want to avoid frequent vmspace changes, so
 * workers need to be fed in such a way as to balance the load while at
 * the same time being efficient about vmspace switches.
 *
 * How expensive is a vmspace switch?  The costly part is pmap_activate(), which
 * does TLB invalidation IPIs.
 *
 * To improve vmspace affinity, we keep a bitmap of affine workers in every
 * vmspace.  The srcio (worker) scheduler first checks the flow's workers for an
 * idle affine worker.  Failing success in the first pass, the scheduler then
 * tries to select any idle worker in the flow.  If the second pass fails, the
 * scheduler tries to select the least-busy affine local worker.
 *
 * XXX: Scheduling avoids locking every worker so may see stale reads, but it's
 * good enough for the purpose of load balancing.
 *
 * One relief is that when a CPU is busy running workers, the user thread will
 * tend to migrate to a less busy CPU so the next submission batch will utilize
 * a different flow.  In that way, a single-threaded application can still
 * utilize multiple cores for I/O execution in the kernel, as long as it keeps
 * up with completions.
 */
struct rio_srcio_scheduler {
	struct rio_selector	rss_sel;
	int	*rss_phase;	/* round-robin index */
};

static inline void
rio_srcio_scheduler_init(struct rio_srcio_scheduler *sched,
    struct rio_issuer *issuer)
{
	sched->rss_phase = &issuer->ri_phase;
	rio_selector_init(&sched->rss_sel, UIMAX(rio_flow_read_workers,
	    rio_flow_write_workers, rio_flow_sync_workers));
	/* TODO: more worker classes? */
}

static inline void
rio_srcio_scheduler_sockinit(struct rio_srcio_scheduler *sched, int *phasep)
{
	sched->rss_phase = phasep;
	rio_selector_init(&sched->rss_sel, rio_flow_socket_workers);
}

static inline void
rio_srcio_scheduler_reset(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers, u_int len, struct rio_srcio *srcio)
{
	struct rio_selector *sel = &sched->rss_sel;
	struct rio_softc *sc = srcio->rs_sc;
	bitstr_t *waff = sc->sc_vmspace->vm_rio; /* worker affinity */

	rio_selector_reset(sel);
	MPASS(len > 0);
	for (u_int i = 0; i < len; i++) {
		struct rio_worker *worker = workers + i;
		u_int qlen = worker->rw_len;
		bool affine = bit_test(waff, worker->rw_id);

		rio_selector_insert(sel, qlen, affine);
	}
	MPASS(sel->rs_n == len);
	MPASS(sel->rs_min != UINT_MAX);
}

static inline struct rio_worker *
rio_srcio_scheduler_ideal(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rss_sel;
	u_int count, idx;

	bit_and(sel->rs_empty, sel->rs_affine, sel->rs_candidates, sel->rs_n);
	count = 0;
	bit_foreach(sel->rs_candidates, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[*sched->rss_phase % count]);
}

static inline struct rio_worker *
rio_srcio_scheduler_empty(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rss_sel;
	u_int count, idx;

	count = 0;
	bit_foreach(sel->rs_empty, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[*sched->rss_phase % count]);
}

static inline struct rio_worker *
rio_srcio_scheduler_affine(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rss_sel;
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
	return (workers + sel->rs_indices[*sched->rss_phase % count]);
}

static inline struct rio_worker *
rio_srcio_scheduler_depth(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers)
{
	struct rio_selector *sel = &sched->rss_sel;
	u_int count, idx;

	count = 0;
	bit_foreach(sel->rs_minimum, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	MPASS(count > 0);
	return (workers + sel->rs_indices[*sched->rss_phase % count]);
}

/* Try selecting the least-busy affine worker. */
static inline struct rio_worker *
rio_srcio_scheduler_select_worker(struct rio_srcio_scheduler *sched,
    struct rio_worker *workers, u_int len, struct rio_srcio *srcio)
{
	struct rio_worker *worker;

	rio_srcio_scheduler_reset(sched, workers, len, srcio);
	if ((worker = rio_srcio_scheduler_ideal(sched, workers)) != NULL) {
		return (worker);
	}
	if ((worker = rio_srcio_scheduler_empty(sched, workers)) != NULL) {
		return (worker);
	}
	if ((worker = rio_srcio_scheduler_affine(sched, workers)) != NULL) {
		return (worker);
	}
	return (rio_srcio_scheduler_depth(sched, workers));
}

static inline void
rio_srcio_scheduler_schedule(struct rio_srcio_scheduler *sched,
    struct rio_srcio *srcio)
{
	struct rio_flow *flow;
	struct rio_worker *workers, *worker;
	u_int len;

	/* Try local flow first. */
	flow = DPCPU_PTR(rio_flow);
	workers = rio_flow_classify(flow, srcio, &len);
	worker = rio_srcio_scheduler_select_worker(sched, workers, len, srcio);
	rio_worker_enqueue(worker, srcio);
}

static inline void
rio_srcio_scheduler_destroy(struct rio_srcio_scheduler *sched)
{
	rio_selector_destroy(&sched->rss_sel);
}

struct rio_socket_issuer {
	struct rio_srcio_scheduler	rsi_sched;
	struct mtx	rsi_lock;
	int		rsi_phase;
};
DPCPU_DEFINE_STATIC(struct rio_socket_issuer, rio_socket_issuer);

static inline void
rio_socket_issuer_init(struct rio_socket_issuer *rsi)
{
	rio_srcio_scheduler_sockinit(&rsi->rsi_sched, &rsi->rsi_phase);
	mtx_init(&rsi->rsi_lock, "rio socket issuer lock", NULL, MTX_DEF);
}

static inline void
rio_socket_issuer_destroy(struct rio_socket_issuer *rsi)
{
	rio_srcio_scheduler_destroy(&rsi->rsi_sched);
	mtx_destroy(&rsi->rsi_lock);
}

static inline void
rio_socket_schedule(struct socket *so, sb_which which)
{
	struct rio_socket_issuer *rsi = DPCPU_PTR(rio_socket_issuer);
	struct rio_srcio *srcio;
	struct rio_srcio_ext *rse;
	struct rio_flow *flow;
	struct rio_worker *worker;
	struct sockbuf *sb = sobuf(so, which);

	/* TODO: optimizations for PF_UNIX? */
	RIO_SOCK_BUF_LOCK(so, which);
	MPASS((sb->sb_flags & SB_RIO_RUNNING) != 0);
	srcio = rio_sockbuf_takefirst(sb);
	RIO_SOCK_BUF_UNLOCK(so, which);
	MPASS(srcio != NULL);
	rse = rio_srcio_ext(srcio);
	/* TODO: Utilize policy/vnet affinity for overflow to remote workers? */
	flow = DPCPU_ID_PTR(rse->rse_cpu, rio_flow);
	mtx_lock(&rsi->rsi_lock);
	worker = rio_srcio_scheduler_select_worker(&rsi->rsi_sched,
	    flow->rf_socket, rio_flow_socket_workers, srcio);
	mtx_unlock(&rsi->rsi_lock);
	rio_worker_enqueue(worker, srcio);
}

void
sorio_snd(void *context, int pending __unused)
{
	rio_socket_schedule(context, SO_SND);
}

void
sorio_rcv(void *context, int pending __unused)
{
	rio_socket_schedule(context, SO_RCV);
}

static void
rio_issuer_thread(void *arg)
{
	struct rio_srcio_scheduler sched;
	struct rio_issuer_arg *ria = arg;
	struct rio_issuer *self = ria->ria_issuer;
	struct thread *td = curthread;

	thread_lock(td);
	sched_bind(td, self->ri_cpu);
	thread_unlock(td);
	rio_srcio_scheduler_init(&sched, self);

	for (;;) {
		struct rio_src *src;
		struct rio_softc *sc;
		struct thread *td;
next:
		/* TODO: Removal reduces concurrency!  Add an issuing list? */
		/* TODO: Maybe the solution is actually to rerun the policy?
		 * Then we have a tradeoff on the attention parameter.  It could
		 * be influenced by mp_ncpus/sqlen? */
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

		while (src->rs_attention-- > 0) {
			struct rio_cdev rcd;
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
			/* Handle sockets with non-blocking operations. */
			if (rio_srcio_socket_enqueue(srcio)) {
				continue;
			}
			/* Try the async BIO strategy if available. */
			switch ((error = rio_cdev_setup(&rcd, srcio))) {
			case RIO_CDEV_FALLBACK:
				goto schedule;
			case 0:
				if (rcd.rcd_iovcnt > 0) {
					/*
					 * Reading iovecs may sleep, so we
					 * requeue src to the front of the
					 * issuer queue for another thread to
					 * service.
					 */
					rio_issuer_enqueue_front(self, src);
				}
				break;
			default:
				rio_srcio_error(srcio, error);
				continue;
			}
			switch ((error = rio_cdev_bio_strategy(&rcd, srcio))) {
			case RIO_CDEV_FALLBACK:
				goto schedule;
			default:
				rio_srcio_error(srcio, error);
				/* FALLTHROUGH */
			case 0:
				if (rcd.rcd_iovcnt > 0) {
					/*
					 * The src was requeued on the issuer.
					 * Unlock it and go next.
					 */
					sx_sunlock(&sc->sc_status_lock);
					goto next;
				}
				continue;
			}
schedule:
			/* Schedule the io on a worker. */
			rio_srcio_scheduler_schedule(&sched, srcio);
		}
		sx_sunlock(&sc->sc_status_lock);
		/* TODO: Select issuer with src policy instead of directly. */
		rio_issuer_enqueue(self, src);
	}
	rio_srcio_scheduler_destroy(&sched);
	free_unr(self->ri_unr, rio_issuer_arg_idx(ria));
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

		/*
		 * Why not have one queue per worker class in each flow, and
		 * have workers take from there instead of each having its own
		 * queue?  Separate queues gives us control over which worker we
		 * schedule I/O on, which is how vmspace affinity is achieved.
		 */
		error = rio_worker_dequeue(self, &srcio);
		if (__predict_false(error != 0)) {
			MPASS(error == ESHUTDOWN || error == EWOULDBLOCK);
			break;
		}
		if (__predict_false(srcio == NULL)) {
			/* Relinquish vmspace on user process exit. */
			rio_vmspace_switch(myvm, id);
			continue;
		}
		/* Check for close/exit. */
		status = rio_status(srcio->rs_sc);
		if (__predict_false(status != RIO_OPEN)) {
			/*
			 * Check if the user process is exiting.  The above NULL
			 * check is for handling a wakeup when our queue was
			 * empty.  This check handles the final dequeue when the
			 * queue was not empty.
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
	cv_broadcast(&self->rw_cond);
	mtx_unlock(&self->rw_lock);
	kproc_exit(0);
}

static struct proc *rio_proc;

static void
rio_issuer_start(void *arg, int pending __unused)
{
	struct rio_issuer_arg *ria = arg;
	struct rio_issuer *issuer = ria->ria_issuer;
	int error;

	/* Spawn issuer threads in the main rio kernel process. */
	if ((error = kproc_kthread_add(rio_issuer_thread, ria, &rio_proc, NULL,
	    0, 0, NULL, "issue %u.%u", issuer->ri_cpu,
	    rio_issuer_arg_idx(ria))) != 0) {
		MPASS(error != ESRCH);
		/* TODO: error handling */
		printf("%s: kproc_kthread_add: %d\n", __func__, error);
	}
}

static void
rio_worker_start(void *arg, int pending __unused)
{
	struct rio_worker *worker = arg;
	int error;

	/* Spawn each worker as its own kernel process. */
	if ((error = kproc_create(rio_worker_proc, worker, NULL, 0, 0,
	    "rio/%s %u.%u", worker->rw_classname, worker->rw_cpu,
	    worker->rw_idx)) != 0) {
		/* TODO: error handling */
		printf("%s: kproc_create: %d\n", __func__, error);
	}
}

static inline void
rio_issuer_init(struct rio_issuer *issuer, u_int cpu)
{
	issuer->ri_cpu = cpu;
	issuer->ri_threads = 0;
	mtx_init(&issuer->ri_lock, "rio issuer lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&issuer->ri_cond, "rio issuer cond");
	STAILQ_INIT(&issuer->ri_srcs);
	issuer->ri_tasks = mallocarray(rio_flow_issuer_threads,
	    sizeof(*issuer->ri_tasks), M_RIO, M_WAITOK);
	for (int i = 0; i < rio_flow_issuer_threads; i++) {
		struct rio_issuer_arg *ria = &issuer->ri_tasks[i];

		ria->ria_issuer = issuer;
		TASK_INIT(&ria->ria_task, 0, rio_issuer_start, ria);
	}
	issuer->ri_unr = new_unrhdr(0, rio_flow_issuer_threads - 1, UNR_NO_MTX);
}

static inline void
rio_worker_init(struct rio_worker *worker, rio_srcio_handler_f *handler,
    const char *classname, u_int cpu, u_int idx)
{
	static u_int nextid;

	worker->rw_classname = classname;
	worker->rw_handler = handler;
	worker->rw_cpu = cpu;
	worker->rw_idx = idx;
	worker->rw_id = nextid++;
	mtx_init(&worker->rw_lock, "rio worker lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&worker->rw_cond, "rio worker cond");
	STAILQ_INIT(&worker->rw_srcios);
	TASK_INIT(&worker->rw_task, 0, rio_worker_start, worker);
}

static inline void
rio_workerclass_init_(struct rio_worker **workers, rio_srcio_handler_f *handler,
    const char *classname, u_int cpu, u_int n)
{
	*workers = mallocarray(n, sizeof(**workers), M_RIO, M_WAITOK | M_ZERO);
	for (u_int i = 0; i < n; i++) {
		rio_worker_init(*workers + i, handler, classname, cpu, i);
	}
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

#define RIO_TASKQUEUE_CREATE_THREAD(name) ({ \
	name = taskqueue_create(#name, M_WAITOK, taskqueue_thread_enqueue, \
	    &name); \
	taskqueue_start_threads_in_proc(&name, 1, PWAIT, curproc, "%s taskq", \
	    #name); \
})

static void
rio_bootstrap(void *arg __unused)
{
	u_int cpu;

	RIO_TASKQUEUE_CREATE_THREAD(rio_doom);
	RIO_TASKQUEUE_CREATE_THREAD(rio_kick);

	rio_max_flow_workers = rio_flow_read_workers + rio_flow_write_workers +
	    rio_flow_sync_workers + rio_flow_socket_workers;
	/* TODO: more worker classes? */
	rio_max_workers = mp_ncpus * rio_max_flow_workers;

	rio_src_zone = rio_zcreate("rio src", sizeof(struct rio_src));
	rio_srcio_zone = rio_zcreate("rio src+io", sizeof(struct rio_srcio));
	rio_srcio_ext_zone = rio_zcreate("rio src+io+extra",
	    sizeof(struct rio_srcio_ext));

	cv_init(&rio_shutdown_cond, "rio shutdown cond");

	CPU_FOREACH(cpu) {
		struct rio_flow *flow;

		flow = DPCPU_ID_PTR(cpu, rio_flow);
		rio_issuer_init(&flow->rf_issuer, cpu);
		rio_workerclass_init(flow, read, cpu);
		rio_workerclass_init(flow, write, cpu);
		rio_workerclass_init(flow, sync, cpu);
		rio_workerclass_init(flow, socket, cpu);
		/* TODO: more worker classes? */
		rio_socket_issuer_init(DPCPU_ID_PTR(cpu, rio_socket_issuer));
	}
	kthread_exit();
}

static int
rio_load(void)
{
	int error;

	/*
	 * Work around having no TASKQUEUE_DEFINE_PROC by creating a proc to
	 * start the taskqueues.  Let it do the rest of the initialization.
	 */
	error = kproc_create(rio_bootstrap, NULL, &rio_proc, 0, 0, "rio");
	if (error != 0) {
		printf("%s: kproc_create: %d\n", __func__, error);
	}
	return (error);
}

static inline void
rio_issuer_shutdown(struct rio_issuer *issuer)
{
	mtx_lock(&issuer->ri_lock);
	issuer->ri_shutdown = true;
	mtx_unlock(&issuer->ri_lock);
}

static inline void
rio_worker_shutdown(struct rio_worker *worker)
{
	mtx_lock(&worker->rw_lock);
	worker->rw_shutdown = true;
	mtx_unlock(&worker->rw_lock);
}

static inline void
rio_workerclass_shutdown_(struct rio_worker *workers, u_int n)
{
	for (u_int i = 0; i < n; i++) {
		rio_worker_shutdown(workers + i);
	}
}

#define rio_workerclass_shutdown(flow, class) \
	rio_workerclass_shutdown_((flow)->rf_##class, \
	    rio_flow_##class##_workers)

static inline void
rio_issuer_destroy(struct rio_issuer *issuer)
{
	mtx_lock(&issuer->ri_lock);
	issuer->ri_shutdown = true;
	cv_broadcast(&issuer->ri_cond);
	while (issuer->ri_threads > 0) {
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
	}
	MPASS(STAILQ_EMPTY(&issuer->ri_srcs));
	mtx_unlock(&issuer->ri_lock);
	mtx_destroy(&issuer->ri_lock);
	cv_destroy(&issuer->ri_cond);
	clean_unrhdr(issuer->ri_unr);
	delete_unrhdr(issuer->ri_unr);
	free(issuer->ri_tasks, M_RIO);
}

static inline void
rio_worker_destroy(struct rio_worker *worker)
{
	mtx_lock(&worker->rw_lock);
	cv_signal(&worker->rw_cond);
	while (worker->rw_running) {
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
	rio_shutdown_pending = true;
	CPU_FOREACH(cpu) {
		struct rio_flow *flow;

		flow = DPCPU_ID_PTR(cpu, rio_flow);
		rio_issuer_shutdown(&flow->rf_issuer);
		rio_workerclass_shutdown(flow, read);
		rio_workerclass_shutdown(flow, write);
		rio_workerclass_shutdown(flow, sync);
		rio_workerclass_shutdown(flow, socket);
	}
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
		rio_workerclass_destroy(flow, socket);
		/* TODO: more worker classes? */
		rio_socket_issuer_destroy(DPCPU_ID_PTR(cpu, rio_socket_issuer));
	}
	taskqueue_quiesce(rio_doom);
	taskqueue_free(rio_doom);
	taskqueue_quiesce(rio_kick);
	taskqueue_free(rio_kick);
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
