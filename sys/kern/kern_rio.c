/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include "opt_rio.h"

#define EXTERR_CATEGORY EXTERR_CAT_RIO
#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/buf.h>
#include <sys/condvar.h>
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
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/systm.h>
#include <sys/taskqueue.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/umtxvar.h>
#include <sys/user.h>
#include <sys/vnode.h>

#include <vm/pmap.h>
#include <vm/uma.h>
#include <vm/vm_extern.h>
#include <vm/vm_kern.h>
#include <vm/vm_map.h>
#include <vm/vnode_pager.h>

#include <ck_ec.h>
#include <ck_ring.h>

/* TODO: sysctls/tunables to control taskqueue properties, counters */

static MALLOC_DEFINE(M_RIO, "rio", "rio data structures");

static int
rio_ec_gettime(const struct ck_ec_ops *ops __unused, struct timespec *out)
{
	/* TODO: revisit which clock source to use, this is CLOCK_MONOTONIC */
	nanouptime(out);
	return (0);
}

static void
rio_ec_umtx_wait(const struct ck_ec_wait_state *state, const uint32_t *address,
    uint32_t expected, const struct timespec *deadline)
{
	struct umtx_abs_timeout uto, *utop;
	struct umtx_q *uq;
	uint32_t value;
	int error;

	/* This implementation is largely informed by kern_umtq.c:do_wait(). */
	uq = curthread->td_umtxq;
	if ((error = umtx_key_get(address, TYPE_SIMPLE_WAIT, AUTO_SHARE,
	    &uq->uq_key)) != 0) {
		/* TODO: handle error somehow */
		printf("%s: umtx_key_get: %d\n", __func__, error);
		return;
	}
	if (deadline == NULL) {
		utop = NULL;
	} else {
		/* TODO: revisit which clock source to use */
		umtx_abs_timeout_init(&uto, CLOCK_MONOTONIC, true, deadline);
		utop = &uto;
	}
	umtxq_lock(&uq->uq_key);
	umtxq_insert(uq);
	umtxq_unlock(&uq->uq_key);
	error = fueword32(address, &value);
	umtxq_lock(&uq->uq_key);
	if (error == 0 && value == expected) {
		error = umtxq_sleep(uq, "riowait", utop);
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

static void
rio_ec_umtx_wake(const struct ck_ec_ops *ops __unused, const uint32_t *address)
{
	int error;

	if ((error = kern_umtx_wake(curthread, __DECONST(uint32_t *, address),
	    INT_MAX, 0)) != 0) {
		/* TODO: handle error somehow */
		printf("%s: kern_umtx_wait: %d\n", __func__, error);
	}
}

static const struct ck_ec_ops rio_ec_umtx_ops = {
	.gettime = rio_ec_gettime,
	.wait32 = rio_ec_umtx_wait,
	.wake32 = rio_ec_umtx_wake,
	/* TODO: tune/override default options for ABI stability */
};

static const struct ck_ec_mode rio_ec_umtx_mode = {
	.ops = &rio_ec_umtx_ops,
	.single_producer = false,
};

typedef int rio_src_scheduler_f(struct rio_softc *);

/* TODO: come up with a set of useful scheduling policies */
static rio_src_scheduler_f rio_src_scheduler_none;

static rio_src_scheduler_f *rio_src_policies[] = {
	[RIO_POLICY_NONE] = &rio_src_scheduler_none,
};

/* TODO: tunables, tuning */
static const u_int rio_flow_issuer_threads = 2;
static const u_int rio_flow_read_workers = 4;
static const u_int rio_flow_write_workers = 4;
static const u_int rio_flow_sync_workers = 4;
/* TODO: other worker classes */

/* Deferred softc destruction. */
static struct taskqueue *rio_doom;

/* Shutdown handling. */
static u_int rio_hold_count;
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

static inline int
rio_tryhold(void)
{
	mtx_lock(&rio_shutdown_lock);
	if (rio_shuttingdown()) {
		mtx_unlock(&rio_shutdown_lock);
		return (ESHUTDOWN);
	}
	rio_hold_count++;
	mtx_unlock(&rio_shutdown_lock);
	return (0);
}

static inline void
rio_drop(void)
{
	mtx_lock(&rio_shutdown_lock);
	rio_hold_count--;
	if (rio_hold_count == 0 && rio_shuttingdown()) {
		cv_signal(&rio_shutdown_cond);
	}
	mtx_unlock(&rio_shutdown_lock);
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

struct rio_softc {
	struct rio	*sc_rio;	/* mapped address of SHM object */
	struct rio_slot *sc_submissions;/* submission queue slots in rio */
	struct rio_slot *sc_completions;/* completion queue slots in rio */
	struct rio_io	*sc_io;		/* kernel-private IO control blocks */
	struct ucred	*sc_cred;	/* user credentials */
	struct proc	*sc_proc;	/* user process */
	size_t		sc_size;	/* for vm_map_remove */
	u_int		sc_ncb;		/* number of control blocks */
	u_int		sc_policy_id;	/* scheduling policy */
	/* TODO: policy metadata */
	struct task	sc_destroy_task;/* destruction task */
	boolean_t	sc_doomed;	/* impending doom */
	counter_u64_t	sc_inflight;	/* #io issued and not yet completed */
	/* TODO: flags? more counters? */
};

static inline bool
rio_doomed(struct rio_softc *sc)
{
	return (atomic_load_acq_int(&sc->sc_doomed));
}

static inline struct riocb *
rio_submissions_trydequeue(struct rio_softc *sc, uint32_t *indexp)
{
	struct rio_slot slot;
	struct rio *rio;

	rio = sc->sc_rio;
	if (CK_RING_TRYDEQUEUE_MPMC(rio, &rio->rio_submission.rr_ring,
	    sc->sc_submissions, &slot)) {
		uint32_t index = slot.rs_index;

		ck_ec_inc(&rio->rio_submission.rr_dqc, &rio_ec_umtx_mode);
		if (__predict_true(index < sc->sc_ncb)) {
			*indexp = index;
			return (&rio->rio_control[index]);
		}
		/* TODO: how to handle invalid index? */
		printf("%s: invalid index %u\n", __func__, index);
	}
	return (NULL);
}

static int
rio_completions_enqueue_pred(const struct ck_ec_wait_state *state,
    struct timespec *deadline __unused)
{
	struct rio_softc *sc = state->data;

	if (__predict_false(rio_doomed(sc))) {
		return (ECANCELED);
	}
	if (__predict_false(rio_shuttingdown())) {
		return (ESHUTDOWN);
	}
	return (0);
}

/* TODO: generalization to batch several before touching event counter? */
static inline int
rio_completions_enqueue(struct rio_softc *sc, uint32_t index)
{
	struct rio_slot slot;
	struct rio *rio;

	slot.rs_index = index;
	rio = sc->sc_rio;
	for (;;) {
		uint32_t value;
		int error;

		value = ck_ec_value(&rio->rio_completion.rr_dqc);
		if (CK_RING_ENQUEUE_MPMC(rio, &rio->rio_completion.rr_ring,
		    sc->sc_completions, &slot)) {
			ck_ec_inc(&rio->rio_completion.rr_nqc,
			    &rio_ec_umtx_mode);
			return (0);
		}
		/* TODO: deadline from policy? that's a can of worms... */
		error = ck_ec_wait_pred(&rio->rio_completion.rr_dqc,
		    &rio_ec_umtx_mode, value, rio_completions_enqueue_pred, sc,
		    NULL);
		if (__predict_false(error != 0)) {
			return (error);
		}
	}
	__unreachable();
}

static void
rio_destroy_task(void *arg, int pending __unused)
{
	struct rio_softc *sc = arg;
	vm_offset_t kva;
	size_t size;

	crfree(sc->sc_cred);
	PRELE(sc->sc_proc);
	/*
	 * The object can stay mapped even if the shmfd is closed or the user
	 * process exits.  Unmap the object directly instead of requiring the
	 * file to remain open for a call to shm_unmap().
	 */
	kva = (vm_offset_t)sc->sc_rio;
	/* We call shm_map() with an offset of 0, so kva is aligned. */
	size = round_page(sc->sc_size);
	vm_map_remove(kernel_map, kva, kva + size);
	counter_u64_free(sc->sc_inflight);
	free(sc->sc_io, M_RIO);
	free(sc, M_RIO);
	rio_drop();
}

void
rio_destroy(struct rio_softc *sc)
{
	atomic_store_rel_int(&sc->sc_doomed, true);
	smp_rendezvous(NULL, NULL, NULL, NULL);
	if (counter_u64_fetch(sc->sc_inflight) == 0) {
		taskqueue_enqueue(rio_doom, &sc->sc_destroy_task);
	} else {
		/* TODO: this probably isn't enough to drain everything */
		ck_ec_inc(&sc->sc_rio->rio_completion.rr_dqc,
		    &rio_ec_umtx_mode);
	}
	/* The final completion enqueues the destruction task when doomed. */
}

static inline void
rio_ring_init(struct rio_ring *ring, u_int size)
{
	ck_ring_init(&ring->rr_ring, size);
	ck_ec_init(&ring->rr_nqc, 0);
	ck_ec_init(&ring->rr_dqc, 0);
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
	if (conf->rio_policy_id >= nitems(rio_src_policies)) {
		return (EINVAL);
	}
	if ((error = rio_tryhold()) != 0) {
		return (error);
	}
	sc = malloc(sizeof(*sc), M_RIO, M_WAITOK | M_ZERO);
	size = rio_config_size(conf);
	/* TODO: Will mmap enforce size limits for us? */
	if ((error = shm_map(fp, size, 0, (void **)&sc->sc_rio)) != 0) {
		rio_drop();
		free(sc, M_RIO);
		return (error);
	}
	sc->sc_cred = crhold(active_cred); /* XXX: for all IO on this ring */
	PHOLD((sc->sc_proc = td->td_proc));
	rio = sc->sc_rio;
	rio_ring_init(&rio->rio_submission, conf->rio_sqlen);
	rio_ring_init(&rio->rio_completion, conf->rio_cqlen);
	memset(rio->rio_control, 0, size - sizeof(*rio));
	sc->sc_submissions = rio_submission_slots(rio, conf);
	sc->sc_completions = rio_completion_slots(rio, conf);
	sc->sc_io = mallocarray(conf->rio_ncb, sizeof(*sc->sc_io), M_RIO,
	    M_WAITOK | M_ZERO);
	sc->sc_size = size;
	sc->sc_ncb = conf->rio_ncb;
	sc->sc_policy_id = conf->rio_policy_id;
	TASK_INIT(&sc->sc_destroy_task, 0, rio_destroy_task, sc);
	sc->sc_inflight = counter_u64_alloc(M_WAITOK);
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
	schedule = rio_src_policies[sc->sc_policy_id];
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
	int			ri_seq;	/* for reducing bias */
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
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
		if (__predict_false(rio_shuttingdown())) {
			mtx_unlock(&issuer->ri_lock);
			return (ESHUTDOWN);
		}
	}
	src = STAILQ_FIRST(&issuer->ri_srcs);
	STAILQ_REMOVE_HEAD(&issuer->ri_srcs, rs_srcs);
	issuer->ri_len--;
	issuer->ri_seq++;
	mtx_unlock(&issuer->ri_lock);
	*srcp = src;
	return (0);
}

struct rio_srcio {
	struct rio_softc	*rs_sc;	/* io source context */
	struct rio_io		*rs_io;	/* io request */
	STAILQ_ENTRY(rio_srcio)	rs_srcios;
};
STAILQ_HEAD(rio_srcios, rio_srcio);

static uma_zone_t rio_srcio_zone;

typedef void rio_srcio_handler_f(struct rio_srcio *);

static inline uint32_t
rio_srcio_index(struct rio_srcio *srcio)
{
	return (srcio->rs_io - srcio->rs_sc->sc_io);
}

static inline int
rio_srcio_canceled(struct rio_srcio *srcio)
{
	struct rio *rio = srcio->rs_sc->sc_rio;
	uint32_t index = rio_srcio_index(srcio);

	return (atomic_load_int(&rio->rio_control[index].rio_error)
	    == ECANCELED);
}

static inline int
rio_srcio_fdrop(struct rio_srcio *srcio)
{
	struct thread *td = FIRST_THREAD_IN_PROC(srcio->rs_sc->sc_proc);

	return (fdrop(srcio->rs_io->rio_fd_file, td));
}

static inline void
rio_srcio_complete(struct rio_srcio *srcio)
{
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb, *iocb;
	uint32_t index;
	int error;

	/* Must release resources before io can be reused. */
	if (io->rio_fd_file != NULL) {
		rio_srcio_fdrop(srcio);
	}
	if ((rio_io_flags(io) & RIO_VECTORED) != 0) {
		free(io->rio_cb.rio_iov, M_IOV);
	}
	index = rio_srcio_index(srcio);
	kiocb = &io->rio_cb;
	iocb = &sc->sc_rio->rio_control[index];
	iocb->rio_error = kiocb->rio_error;
	iocb->rio_status = kiocb->rio_status;
	atomic_thread_fence_rel();
	error = rio_completions_enqueue(sc, index);
	switch (__builtin_expect(0, error)) {
	case 0:
	case ECANCELED:
	case ESHUTDOWN:
		break;
	case -1: /* ETIMEDOUT */
		/* TODO: handle policy-based timeout somehow? */
	default:
		__assert_unreachable();
	}
	counter_u64_add(sc->sc_inflight, -1);
	if (__predict_false(error == ESHUTDOWN) &&
	    __predict_false(counter_u64_fetch(sc->sc_inflight) == 0)) {
		taskqueue_enqueue(rio_doom, &sc->sc_destroy_task);
	}
	uma_zfree(rio_srcio_zone, srcio);
}

static inline void
rio_srcio_error(struct rio_srcio *srcio, int error)
{
	srcio->rs_io->rio_cb.rio_error = error;
	srcio->rs_io->rio_cb.rio_status = -1;
	rio_srcio_complete(srcio);
}

static inline struct proc *
rio_srcio_proc(struct rio_srcio *srcio)
{
	return (srcio->rs_sc->sc_proc);
}

static void
rio_srcio_read(struct rio_srcio *srcio)
{
	struct proc *p = rio_srcio_proc(srcio);
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = &io->rio_cb;
	struct file *fp = io->rio_fd_file;
	struct iovec iov;
	struct uio uio;
	ssize_t len;
	u_int flags;

	/* TODO: special handling for devices/sockets */
	td->td_ucred = sc->sc_cred;
	/* TODO: surely this can be factored out and centralized */
	/* TODO: put this all in a kaiocb for socket fo_aio_queue */
	uio.uio_td = td;
	uio.uio_segflg = UIO_USERSPACE;
	uio.uio_rw = UIO_READ;
	uio.uio_offset = kiocb->rio_offset;
	flags = rio_io_flags(io);
	if ((flags & RIO_VECTORED) == 0) {
		iov.iov_base = kiocb->rio_buf;
		iov.iov_len = kiocb->rio_length;
		uio.uio_iov = &iov;
		uio.uio_iovcnt = 1;
	} else {
		uio.uio_iov = kiocb->rio_iov;
		uio.uio_iovcnt = kiocb->rio_length;
	}
	len = 0;
	for (int i = 0; i < uio.uio_iovcnt; i++) {
		len += uio.uio_iov[i].iov_len;
	}
	uio.uio_resid = len;
	/* We're not AIO, but close enough. */
	vmspace_switch_aio(p->p_vmspace);
	switch ((kiocb->rio_error = fo_read(fp, &uio, sc->sc_cred,
	    (flags & RIO_FOFFSET) == 0 ? 0 : FOF_OFFSET, td))) {
	case 0:
	case ERESTART:
	case EINTR:
	case EWOULDBLOCK:
		kiocb->rio_status = len - uio.uio_resid;
		break;
	default:
		kiocb->rio_status = -1;
		break;
	}
	td->td_ucred = saved_cred;
}

static void
rio_srcio_write(struct rio_srcio *srcio)
{
	struct proc *p = rio_srcio_proc(srcio);
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = &io->rio_cb;
	struct file *fp = io->rio_fd_file;
	struct iovec iov;
	struct uio uio;
	ssize_t len;
	u_int flags;

	/* TODO: special handling for devices/sockets */
	td->td_ucred = sc->sc_cred;
	/* TODO: surely this can be factored out and centralized */
	/* TODO: put this all in a kaiocb for socket fo_aio_queue */
	uio.uio_td = td;
	uio.uio_segflg = UIO_USERSPACE;
	uio.uio_rw = UIO_WRITE;
	uio.uio_offset = kiocb->rio_offset;
	flags = rio_io_flags(io);
	if ((flags & RIO_VECTORED) == 0) {
		iov.iov_base = kiocb->rio_buf;
		iov.iov_len = kiocb->rio_length;
		uio.uio_iov = &iov;
		uio.uio_iovcnt = 1;
	} else {
		uio.uio_iov = kiocb->rio_iov;
		uio.uio_iovcnt = kiocb->rio_length;
	}
	len = 0;
	for (int i = 0; i < uio.uio_iovcnt; i++) {
		len += uio.uio_iov[i].iov_len;
	}
	uio.uio_resid = len;
	if (fp->f_type == DTYPE_VNODE) {
		bwillwrite();
	}
	/* We're not AIO, but close enough. */
	vmspace_switch_aio(p->p_vmspace);
	switch ((kiocb->rio_error = fo_write(fp, &uio, sc->sc_cred,
	    (flags & RIO_FOFFSET) == 0 ? 0 : FOF_OFFSET, td))) {
	case EPIPE:
		PROC_LOCK(p);
		kern_psignal(p, SIGPIPE);
		PROC_UNLOCK(p);
		/* FALLTHROUGH */
	case 0:
	case ERESTART:
	case EINTR:
	case EWOULDBLOCK:
		kiocb->rio_status = len - uio.uio_resid;
		break;
	default:
		kiocb->rio_status = -1;
		break;
	}
	td->td_ucred = saved_cred;
}

static void
rio_srcio_sync(struct rio_srcio *srcio)
{
	struct proc *p = rio_srcio_proc(srcio);
	struct thread *td = curthread;
	struct ucred *saved_cred = td->td_ucred;
	struct rio_softc *sc = srcio->rs_sc;
	struct rio_io *io = srcio->rs_io;
	struct riocb *kiocb = &io->rio_cb;
	struct file *fp = io->rio_fd_file;
	struct vnode *vp;
	u_int cmd = rio_io_cmd(io);
	int error = 0;

	if (cmd == RIO_MLOCK) {
		/* We're not AIO, but close enough. */
		vmspace_switch_aio(p->p_vmspace);
		/*
		 * TODO: After the commands are fleshed out, see if it is
		 * possible to make ident an int and use the rio_data/rio_buf
		 * field for anything that is a pointer (like AIO).
		 */
		error = kern_mlock(p, sc->sc_cred, kiocb->rio_ident,
		    kiocb->rio_length);
	} else if ((vp = fp->f_vnode) != NULL) {
		struct mount *mp;

		while (error != ERELOOKUP) {
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
}

/*
 * A RIO worker is a kernel process pinned to a CPU.  The worker performs
 * blocking IO operations on behalf of a user process.   A worker must be a
 * kernel process with a single thread because it potentially has to change
 * vmspace to perform copies to or from the user process.
 *
 * If the workers pulled directly from the src ring then they couldn't be
 * specialized for specific IO types (read, write, socket, etc).  Instead, we
 * of separate issuers is that they can issue the requests to the appropriate
 * pool of workers.
 *
 * So we have an issuer that issues IO from sources to its set of workers.
 *
 * The workers are divided into operation classes such as read, write, or sync.
 * This helps keep latency and throughput consistent and prevents slow paths
 * from poisoning fast paths.
 *
 * The tricky bit is that we want to avoid frequent vmspace changes, so
 * workers need to be fed in such a way as to balance the load while at
 * the same time being efficient about vmspace switches.
 *
 * How expensive is a vmspace switch?  The costly part is pmap_activate(), which
 * does TLB invalidation IPIs.
 *
 * To improve vmspace affinity, we use check the user process pointer for the
 * tail of the worker's IO queue as a hint for worker affinity.  The issuer
 * first checks the local workers for the least-busy affine worker.  Failing
 * success in the first pass, the issuer then tries to select the least-busy
 * local worker.  If the second pass fails, the issuer consults the source's
 * policy for controlled behavior under load.
 *
 * TODO: The above algorithm was intended for use with bounded worker queues.
 * Revision to refer to policy for acceptable queue lengths is required.
 * TODO: Implement the mentioned policies for controlled behavior under load.
 *
 * We want to stay on the same CPU as the user thread accessing the IO buffers,
 * for cache locality.  Scheduling should select the local CPU issuer until
 * local workers are all busy.  But, we also want to utilize idle CPU time to
 * minimize latency and maximize throughput.  Optimizing the balance of these
 * priorities is the role of the policy.
 */
struct rio_worker {
	struct mtx		rw_lock;
	struct cv		rw_cond;
	struct rio_srcios	rw_srcios;
	rio_srcio_handler_f	*rw_handler;	/* specialized handler */
	struct proc		*rw_hint;	/* last enqueued proc */
	u_int			rw_len;
	u_int			rw_cpu;
	bool			rw_done;
};

static inline void
rio_worker_enqueue(struct rio_worker *worker, struct rio_srcio *srcio)
{
	struct rio_softc *sc = srcio->rs_sc;
	bool wake;

	counter_u64_add(sc->sc_inflight, 1);
	mtx_lock(&worker->rw_lock);
	wake = STAILQ_EMPTY(&worker->rw_srcios);
	STAILQ_INSERT_TAIL(&worker->rw_srcios, srcio, rs_srcios);
	if (wake) {
		cv_signal(&worker->rw_cond);
	}
	worker->rw_len++;
	mtx_unlock(&worker->rw_lock);
}

static inline int
rio_worker_dequeue(struct rio_worker *worker, struct rio_srcio **srciop)
{
	struct rio_srcio *srcio;

	mtx_lock(&worker->rw_lock);
	while (STAILQ_EMPTY(&worker->rw_srcios)) {
		cv_wait(&worker->rw_cond, &worker->rw_lock);
		if (__predict_false(rio_shuttingdown())) {
			mtx_unlock(&worker->rw_lock);
			return (ESHUTDOWN);
		}
	}
	srcio = STAILQ_FIRST(&worker->rw_srcios);
	STAILQ_REMOVE_HEAD(&worker->rw_srcios, rs_srcios);
	worker->rw_len--;
	mtx_unlock(&worker->rw_lock);
	*srciop = srcio;
	return (0);
}

/*
 * The selector picks a candidate given a criteria and a sequence number.
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
}

static inline void
rio_selector_reset(struct rio_selector *sel)
{
	u_int len = sel->rs_len;

	bit_nclear(sel->rs_empty, 0, len);
	bit_nclear(sel->rs_affine, 0, len);
	bit_nclear(sel->rs_minimum, 0, len);
	sel->rs_min = UINT_MAX;
	sel->rs_n = 0;
}

static inline void
rio_selector_insert(struct rio_selector *sel, struct rio_srcio *srcio,
    struct rio_worker *worker)
{
	u_int idx = sel->rs_n++;
	/* XXX: Unlocked, but it's probably good enough. */
	u_int len = worker->rw_len;

	if (len == 0) {
		bit_set(sel->rs_empty, idx);
	}
	if (worker->rw_hint == rio_srcio_proc(srcio)) {
		bit_set(sel->rs_affine, idx);
	}
	if (len == sel->rs_min) {
		bit_set(sel->rs_minimum, idx);
	} else if (len < sel->rs_min) {
		sel->rs_min = len;
		bit_nclear(sel->rs_minimum, 0, sel->rs_len);
		bit_set(sel->rs_minimum, idx);
	}
}

static inline void
bit_and(bitstr_t *a, bitstr_t *b, bitstr_t *r, size_t len)
{
	size_t n = bitstr_size(len);

	for (size_t i = 0; i < n; i++) {
		r[i] = a[i] & b[i];
	}
}

static inline struct rio_worker *
rio_selector_ideal(struct rio_selector *sel, struct rio_worker *workers, int x)
{
	u_int count, idx;

	bit_nclear(sel->rs_candidates, 0, sel->rs_n);
	bit_and(sel->rs_empty, sel->rs_affine, sel->rs_candidates, sel->rs_n);
	count = 0;
	bit_foreach(sel->rs_candidates, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[x % count]);
}

static inline struct rio_worker *
rio_selector_empty(struct rio_selector *sel, struct rio_worker *workers, int x)
{
	u_int count, idx;

	count = 0;
	bit_foreach(sel->rs_empty, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[x % count]);
}

static inline struct rio_worker *
rio_selector_affine(struct rio_selector *sel, struct rio_worker *workers, int x)
{
	u_int min, count, idx;

	bit_nclear(sel->rs_candidates, 0, sel->rs_n);
	min = UINT_MAX;
	bit_foreach(sel->rs_affine, sel->rs_n, idx) {
		u_int len = workers[idx].rw_len;

		if (len > min) {
			continue;
		}
		if (len < min) {
			min = len;
			bit_nclear(sel->rs_candidates, 0, sel->rs_n);
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
	return (workers + sel->rs_indices[x % count]);
}

static inline struct rio_worker *
rio_selector_depth(struct rio_selector *sel, struct rio_worker *workers, int x)
{
	u_int count, idx;

	count = 0;
	bit_foreach(sel->rs_minimum, sel->rs_n, idx) {
		sel->rs_indices[count++] = idx;
	}
	if (count == 0) {
		return (NULL);
	}
	return (workers + sel->rs_indices[x % count]);
}

/* TODO: remote flow selection process */

static inline void
rio_selector_free(struct rio_selector *sel)
{
	free(sel->rs_empty, M_RIO);
	free(sel->rs_affine, M_RIO);
	free(sel->rs_minimum, M_RIO);
	free(sel->rs_candidates, M_RIO);
	free(sel->rs_indices, M_RIO);
}

struct rio_flow {
	struct rio_issuer rf_issuer;
	struct rio_worker *rf_read;
	struct rio_worker *rf_write;
	struct rio_worker *rf_sync;
	/* TODO: other worker classes */
};
DPCPU_DEFINE_STATIC(struct rio_flow, rio_flow);

/* TODO: schedulers deep dive */
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

static inline struct rio_worker *
rio_srcio_class(struct rio_srcio *srcio, struct rio_flow *flow, u_int *lenp)
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

/* Try selecting the least-busy affine worker. */
static inline struct rio_worker *
rio_srcio_select_worker(struct rio_srcio *srcio, struct rio_selector *sel,
    struct rio_worker *workers, u_int len, int x)
{
	struct rio_worker *worker;

	rio_selector_reset(sel);
	for (u_int i = 0; i < len; i++) {
		rio_selector_insert(sel, srcio, workers + i);
	}
	if ((worker = rio_selector_ideal(sel, workers, x)) != NULL) {
		return (worker);
	}
	if ((worker = rio_selector_empty(sel, workers, x)) != NULL) {
		return (worker);
	}
	if ((worker = rio_selector_affine(sel, workers, x)) != NULL) {
		return (worker);
	}
	if ((worker = rio_selector_depth(sel, workers, x)) != NULL) {
		return (worker);
	}
	/* TODO: remote worker selection */
	return (NULL);
}

static inline int
rio_srcio_schedule(struct rio_srcio *srcio, struct rio_selector *sel, int seq)
{
	struct rio_flow *flow;
	struct rio_worker *workers, *worker;
	u_int len;

	/* Try local flow first. */
	flow = DPCPU_PTR(rio_flow);
	workers = rio_srcio_class(srcio, flow, &len);
	if ((worker = rio_srcio_select_worker(srcio, sel, workers, len, seq))
	    == NULL) {
		/* TODO: remote worker selection */
		return (ENOBUFS);
	}
	rio_worker_enqueue(worker, srcio);
	return (0);
}

/* TODO: tunable, tuning */
static u_int rio_attention_span = 1024; /* IO batching parameter */

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

static void
rio_issuer_thread(void *arg)
{
	struct rio_issuer *self = arg;
	struct rio_selector sel;
	struct thread *td = curthread;

	thread_lock(td);
	sched_bind(td, self->ri_cpu);
	thread_unlock(td);
	rio_selector_init(&sel, UIMAX(rio_flow_read_workers,
	    rio_flow_write_workers, rio_flow_sync_workers));
	/* TODO: more worker classes */

	for (;;) {
		struct rio_src *src;
		struct rio_softc *sc;
		struct thread *td;
		size_t issued;
		int error;
next:
		/* TODO: Removal prevents concurrency!  Add an issuing list? */
		/* TODO: Work stealing! */
		error = rio_issuer_dequeue(self, &src);
		if (__predict_false(error == ESHUTDOWN)) {
			break;
		}
		/* TODO: handle ETIMEDOUT */
		MPASS(error == 0);
		sc = src->rs_sc;
		if (__predict_false(rio_doomed(sc))) {
			uma_zfree(rio_src_zone, src);
			continue;
		}
		/* TODO: Check if PROC_LOCK() is required around this. */
		td = FIRST_THREAD_IN_PROC(sc->sc_proc);

		/*
		 * TODO: We want to avoid interleaving IO from different
		 * processes in the same worker, to minimize vmspace switches in
		 * the workers.  The CPU scheduler will take care of
		 * interleaving the workers on CPU.
		 */
		for (issued = 0; issued < rio_attention_span; issued++) {
			struct rio_srcio *srcio;
			struct rio_io *io;
			struct riocb *iocb;
			uint32_t index;
			int fd, error;

			if ((iocb = rio_submissions_trydequeue(sc, &index))
			    == NULL) {
				goto next;
			}
			srcio = uma_zalloc(rio_srcio_zone, M_WAITOK);
			srcio->rs_sc = sc;
			srcio->rs_io = io = sc->sc_io + index;
			memcpy(&io->rio_cb, iocb, sizeof(*iocb));
			fd = io->rio_cb.rio_ident;
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
				rio_srcio_error(srcio, error);
				break;
			}
			/* XXX: Shouldn't this use a zone allocator? */
			if ((rio_io_flags(io) & RIO_VECTORED) != 0 &&
			    __predict_false((error = copyiniov(
			    io->rio_cb.rio_iov, io->rio_cb.rio_length,
			    &io->rio_cb.rio_iov, EMSGSIZE)) != 0)) {
				rio_srcio_error(srcio, error);
				break;
			}
			if ((error = rio_srcio_schedule(srcio, &sel,
			    self->ri_seq)) != 0) {
				rio_srcio_error(srcio, error);
			}
		}
		rio_issuer_enqueue(self, src);
	}
	rio_selector_free(&sel);
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
	struct vmspace *myvm;
	struct thread *td = curthread;

	thread_lock(td);
	sched_bind(td, self->rw_cpu);
	thread_unlock(td);
	myvm = vmspace_acquire_ref(curproc);
	for (;;) {
		struct rio_srcio *srcio;

		/* TODO: idle timeouts for scaling down? */
		if (rio_worker_dequeue(self, &srcio) != 0) {
			/* ESHUTDOWN */
			break;
		}
		if (__predict_false(rio_doomed(srcio->rs_sc))) {
			rio_srcio_error(srcio, ECANCELED);
			continue;
		}
		/* Check for cancellation. */
		if (__predict_false(rio_srcio_canceled(srcio))) {
			rio_srcio_error(srcio, ECANCELED);
			continue;
		}
		self->rw_handler(srcio);
		rio_srcio_complete(srcio);
		/* TODO: Policy for switching back to myvm? */
	}
	vmspace_switch_aio(myvm);
	vmspace_free(myvm);
	mtx_lock(&self->rw_lock);
	self->rw_done = true;
	cv_broadcast(&self->rw_cond);
	mtx_unlock(&self->rw_lock);
	kproc_exit(0);
}

static inline int
rio_issuer_init(struct rio_issuer *issuer, u_int cpu)
{
	int error;

	issuer->ri_cpu = cpu;
	issuer->ri_threads = 0;
	mtx_init(&issuer->ri_lock, "rio issuer lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&issuer->ri_cond, "rio issuer cond");
	STAILQ_INIT(&issuer->ri_srcs);
	/* TODO: automatic startup/shutdown (kick taskqueue?) */
	for (u_int i = 0; i < rio_flow_issuer_threads; i++) {
		/* Spawn issuer threads in the proc0 kernel process. */
		if ((error = kthread_add(rio_issuer_thread, issuer, NULL, NULL,
		    0, 0, "rio/issue%u.%u", cpu, i)) != 0) {
			/* TODO: error handling */
			printf("%s: kthread_add: %d\n", __func__, error);
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
	int error;

	worker->rw_handler = handler;
	worker->rw_cpu = cpu;
	worker->rw_done = false;
	mtx_init(&worker->rw_lock, "rio worker lock", NULL, MTX_DEF | MTX_NEW);
	cv_init(&worker->rw_cond, "rio worker cond");
	STAILQ_INIT(&worker->rw_srcios);
	/* Spawn each worker as its own kernel process. */
	if ((error = kproc_create(rio_worker_proc, worker, NULL, 0, 0,
	    "rio/%s%u.%u", classname, cpu, i)) != 0) {
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
		/* TODO: more worker classes */
	}
	return (0);
}

static inline void
rio_issuer_destroy(struct rio_issuer *issuer)
{
	struct rio_src *src1, *src2;

	mtx_lock(&issuer->ri_lock);
	cv_broadcast(&issuer->ri_cond);
	while (issuer->ri_threads > 0) {
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
	}
	src1 = STAILQ_FIRST(&issuer->ri_srcs);
	while (src1 != NULL) {
		src2 = STAILQ_NEXT(src1, rs_srcs);
		uma_zfree(rio_src_zone, src1);
		src1 = src2;
	}
	mtx_unlock(&issuer->ri_lock);
	mtx_destroy(&issuer->ri_lock);
	cv_destroy(&issuer->ri_cond);
}

static inline void
rio_worker_destroy(struct rio_worker *worker)
{
	struct rio_srcio *srcio1, *srcio2;

	mtx_lock(&worker->rw_lock);
	cv_signal(&worker->rw_cond);
	while (!worker->rw_done) {
		cv_wait(&worker->rw_cond, &worker->rw_lock);
	}
	srcio1 = STAILQ_FIRST(&worker->rw_srcios);
	while (srcio1 != NULL) {
		srcio2 = STAILQ_NEXT(srcio1, rs_srcios);
		uma_zfree(rio_srcio_zone, srcio1);
		srcio1 = srcio2;
	}
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
	while (rio_hold_count > 0) {
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
		/* TODO: more worker classes */
	}
	taskqueue_quiesce(rio_doom);
	taskqueue_free(rio_doom);
	uma_zdestroy(rio_src_zone);
	uma_zdestroy(rio_srcio_zone);
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
