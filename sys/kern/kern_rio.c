/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#define EXTERR_CATEGORY EXTERR_CAT_RIO
#include <sys/param.h>
#include <sys/systm.h>
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
#include <sys/sysctl.h>
#include <sys/taskqueue.h>
#include <sys/uio.h>
#include <sys/user.h>

#include <vm/uma.h>
#include <vm/vm_param.h>
#include <vm/pmap.h>
#include <vm/vm_extern.h>
#include <vm/vm_kern.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>

#include <ck_ec.h>
#include <ck_ring.h>

/* TODO: sysctls/tunables to control taskqueue properties, counters */

static MALLOC_DEFINE(M_RIO, "rio", "rio data structures");

struct rio_issuer;

/* AKA src - a source of RIO requests */
struct rio_softc {
	struct rio	*sc_rio;	/* mapped address of SHM object */
	struct rio_slot *sc_submissions;/* submission queue slots in rio */
	struct rio_slot *sc_completions;/* completion queue slots in rio */
	struct ucred	*sc_cred;	/* user credentials */
	struct proc	*sc_proc;	/* user process */
	struct rio_issuer	*sc_issuer;	/* issuer queue */
	/* TODO: this could be per-CPU for work stealing */
	STAILQ_ENTRY(rio_softc) sc_srcs;	/* issuer queue linkage */
	vm_object_t	sc_object;	/* for vm_object_destroy */
	size_t		sc_size;	/* for vm_map_remove */
	u_int		sc_ncb;		/* number of control blocks */
	u_int		sc_policy_id;	/* scheduling policy */
	/* TODO: policy metadata */
	struct task	sc_destroy_task;/* destruction task */
	boolean_t	sc_doomed;	/* impending doom */
	counter_u64_t	sc_inflight;	/* #io issued and not yet completed */
	/* TODO: flags? more counters? */
};

static void
rio_destroy_task(void *arg, int pending __unused)
{
	struct rio_softc *sc = arg;
	vm_offset_t kva;
	size_t size;

	PRELE(sc->sc_proc); /* XXX: ideally this could happen sooner */
	/*
	 * The object stays mapped in the kernel even if the shmfd is closed or
	 * the user process exits.  Instead of keeping the file ref'd to call
	 * shm_unmap(), unmap and deallocate the object directly once safe.
	 */
	kva = (vm_offset_t)sc->sc_rio;
	/* We call shm_map() with an offset of 0, so kva is aligned. */
	size = round_page(sc->sc_size);
	vm_map_remove(kernel_map, kva, kva + size);
	vm_object_deallocate(sc->sc_object);
	crfree(sc->sc_cred);
	counter_u64_free(sc->sc_inflight);
	free(sc, M_RIO);
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
	sc = malloc(sizeof(*sc), M_RIO, M_WAITOK | M_ZERO);
	size = rio_config_size(conf);
	if ((error = shm_map(fp, size, 0, (void **)&sc->sc_rio)) != 0) {
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
	sc->sc_object = shmfd->shm_object;
	sc->sc_size = size;
	sc->sc_ncb = conf->rio_ncb;
	sc->sc_policy_id = conf->rio_policy_id;
	TASK_INIT(&sc->sc_destroy_task, 0, rio_destroy_task, sc);
	sc->sc_inflight = counter_u64_alloc(M_WAITOK);
	shmfd->shm_rio = sc;
	return (0);
}

typedef int rio_src_scheduler_f(struct rio_softc *);

/* TODO: come up with a set of useful scheduling policies */
static rio_src_scheduler_f rio_src_scheduler_none;

static rio_src_scheduler_f *rio_src_policies[] = {
	[RIO_POLICY_NONE] = &rio_src_scheduler_none,
};

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

static const struct ck_ec_mode rio_ec_kernel_mode = {
	/* TODO: implement the kernel mode */
};

static inline bool
rio_src_doomed(struct rio_softc *src)
{
	return (atomic_load_acq_int(&src->sc_doomed));
}

static void
rio_destroy_impl(struct rio_softc *sc)
{
	atomic_store_rel_int(&sc->sc_doomed, true);
	smp_rendezvous(NULL, NULL, NULL, NULL);
	ck_ec_inc(&sc->sc_rio->rio_completion.rr_dqc, &rio_ec_kernel_mode);
	/* The final completion enqueues the destruction task when doomed. */
}

static int
rio_ioctl_impl(struct file *fp, u_long com, void *data,
    struct ucred *active_cred, struct thread *td)
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

STAILQ_HEAD(rio_srcs, rio_softc);

/*
 * A RIO issuer is a queue of IO request sources serviced by a collection of
 * kernel threads pinned to a CPU.  Each source is drained according to an IO
 * scheduling policy.  The requests are issued to an appropriate worker as
 * needed.
 */
struct rio_issuer {
	struct mtx		ri_lock;
	struct cv		ri_cond;
	struct rio_srcs		ri_srcs; /* TODO: if this was a ck_ring... */
	u_int			ri_cpu;
};
DPCPU_DEFINE_STATIC(struct rio_issuer, rio_issuer);

static inline void
rio_issuer_enqueue(struct rio_issuer *issuer, struct rio_softc *src)
{
	bool wake;

	mtx_lock(&issuer->ri_lock);
	wake = STAILQ_EMPTY(&issuer->ri_srcs);
	STAILQ_INSERT_TAIL(&issuer->ri_srcs, src, sc_srcs);
	if (wake) {
		cv_signal(&issuer->ri_cond);
	}
	mtx_unlock(&issuer->ri_lock);
}

static inline struct rio_softc *
rio_issuer_dequeue(struct rio_issuer *issuer)
{
	struct rio_softc *src;

	mtx_lock(&issuer->ri_lock);
	/* TODO: handle shutdown */
	while (STAILQ_EMPTY(&issuer->ri_srcs)) {
		/* TODO: Work stealing to distribute load across all workers. */
		cv_wait(&issuer->ri_cond, &issuer->ri_lock);
	}
	src = STAILQ_FIRST(&issuer->ri_srcs);
	STAILQ_REMOVE_HEAD(&issuer->ri_srcs, sc_srcs);
	mtx_unlock(&issuer->ri_lock);
	return (src);
}

struct rio_io {
	struct riocb		rio_cb;		/* validated and stable copy */
	struct rio_softc	*rio_src;	/* context for completion */
	struct file		*rio_fd_file;	/* ref'd file descriptor */
	STAILQ_ENTRY(rio_io)	rio_io_queue;	/* queue linkage */
	uint32_t		rio_cb_index;	/* control block index */
};
STAILQ_HEAD(rio_io_queue, rio_io);

static uma_zone_t rio_io_zone;

/*
 * A RIO worker is a queue of IO requests serviced by a collection of kernel
 * processes pinned to a CPU.  The workers perform blocking IO operations on
 * behalf of an issuer.   A worker is a single-threaded kernel process because
 * it potentially has to change vmspace to perform copies to or from a userspace
 * process.
 */
struct rio_worker {
	struct mtx		rw_lock;
	struct cv		rw_cond;
	struct rio_io_queue	rw_io_queue;
	u_int			rw_cpu;
};
DPCPU_DEFINE_STATIC(struct rio_worker, rio_worker);

static inline void
rio_worker_enqueue(struct rio_worker *worker, struct rio_io *io)
{
	bool wake;

	mtx_lock(&worker->rw_lock);
	counter_u64_add(io->rio_src->sc_inflight, 1);
	wake = STAILQ_EMPTY(&worker->rw_io_queue);
	STAILQ_INSERT_TAIL(&worker->rw_io_queue, io, rio_io_queue);
	if (wake) {
		cv_signal(&worker->rw_cond);
	}
	mtx_unlock(&worker->rw_lock);
}

static inline struct rio_io *
rio_worker_dequeue(struct rio_worker *worker)
{
	struct rio_io *io;

	mtx_lock(&worker->rw_lock);
	/* TODO: handle shutdown */
	while (STAILQ_EMPTY(&worker->rw_io_queue)) {
		cv_wait(&worker->rw_cond, &worker->rw_lock);
	}
	io = STAILQ_FIRST(&worker->rw_io_queue);
	STAILQ_REMOVE_HEAD(&worker->rw_io_queue, rio_io_queue);
	mtx_unlock(&worker->rw_lock);
	return (io);
}

/*
 * Try to atomically assign the src to an issuer, failing if already assigned.
 */
static inline bool
rio_src_tryschedule(struct rio_softc *src, struct rio_issuer *issuer)
{
	return (atomic_cmpset_ptr((uintptr_t *)&src->sc_issuer, (uintptr_t)NULL,
	    (uintptr_t)issuer));
}

static inline void
rio_src_deschedule(struct rio_softc *src)
{
	return (atomic_store_rel_ptr((uintptr_t *)&src->sc_issuer,
	    (uintptr_t)NULL));
}

/*
 * Enqueue the softc as a source for the current CPU's issuer to handle, if not
 * already assigned to an issuer.
 */
static int
rio_src_scheduler_none(struct rio_softc *src)
{
	struct rio_issuer *issuer;

	issuer = &DPCPU_GET(rio_issuer);
	if (rio_src_tryschedule(src, issuer)) {
		rio_issuer_enqueue(issuer, src);
	}
	return (0);
}

/* TODO: tunable, tuning */
static size_t rio_attention_span = 1024; /* IO batching parameter */

static inline struct riocb *
rio_submissions_trydequeue(struct rio_softc *src, uint32_t *indexp)
{
	struct rio *rio;
	struct rio_slot slot;

	rio = src->sc_rio;
	if (CK_RING_TRYDEQUEUE_MPMC(rio, &rio->rio_submission.rr_ring,
	    src->sc_submissions, &slot)) {
		uint32_t index;

		ck_ec_inc(&rio->rio_submission.rr_dqc, &rio_ec_kernel_mode);
		if (__predict_true((index = slot.rs_index) < src->sc_ncb)) {
			*indexp = index;
			return (&rio->rio_control[index]);
		}
		/* TODO: invalid index error counter? */
	}
	return (NULL);
}

/* TODO: generalization to batch several before touching event counter */
static inline void
rio_completions_enqueue(struct rio_softc *src, uint32_t index)
{
	struct rio_slot slot;
	struct rio *rio;

	slot.rs_index = index;
	rio = src->sc_rio;
	while (__predict_true(!rio_src_doomed(src))) {
		uint32_t value;

		value = ck_ec_value(&rio->rio_completion.rr_dqc);
		if (CK_RING_ENQUEUE_MPMC(rio, &rio->rio_completion.rr_ring,
		    src->sc_completions, &slot)) {
			ck_ec_inc(&rio->rio_completion.rr_nqc,
			    &rio_ec_kernel_mode);
			break;
		}
		/* TODO: deadline or pred for shutdown reasons? */
		ck_ec_wait(&rio->rio_completion.rr_dqc,
		    &rio_ec_kernel_mode, value, NULL);
	}
}

static inline int
rio_io_fdrop(struct rio_io *io)
{
	struct thread *td;

	td = FIRST_THREAD_IN_PROC(io->rio_src->sc_proc);
	return (fdrop(io->rio_fd_file, td));
}

static struct taskqueue *rio_doom;

static inline void
rio_io_complete(struct rio_io *io)
{
	struct riocb *kiocb, *iocb;
	struct rio_softc *src;
	uint32_t index;

	if (io->rio_fd_file != NULL) {
		rio_io_fdrop(io);
	}
	src = io->rio_src;
	index = io->rio_cb_index;
	kiocb = &io->rio_cb;
	iocb = &src->sc_rio->rio_control[index];
	iocb->rio_error = kiocb->rio_error;
	iocb->rio_status = kiocb->rio_status;
	atomic_thread_fence_rel();
	counter_u64_add(src->sc_inflight, -1);
	if (__predict_false(rio_src_doomed(src))) {
		if (counter_u64_fetch(src->sc_inflight) == 0) {
			taskqueue_enqueue(rio_doom, &src->sc_destroy_task);
		}
	} else {
		rio_completions_enqueue(src, index);
	}
	if ((io->rio_cb.rio_cmd & RIO_VECTORED) != 0) {
		free(io->rio_cb.rio_iov, M_IOV);
	}
	uma_zfree(rio_io_zone, io);
}

static inline void
rio_io_error(struct rio_io *io, int error)
{
	io->rio_cb.rio_error = error;
	io->rio_cb.rio_status = -1;
	rio_io_complete(io);
}

static void
rio_issuer_thread(void *arg)
{
	struct rio_issuer *self = arg;

	sched_bind(curthread, self->ri_cpu);

	for (;;) {
		struct rio_softc *src;
		struct thread *td;
		size_t issued;
next:
		/* TODO: Removal prevents concurrency.  Add an issuing list? */
		src = rio_issuer_dequeue(self);
		if (__predict_false(rio_src_doomed(src))) {
			continue;
		}
		/* TODO: Check if PROC_LOCK() is required around this. */
		td = FIRST_THREAD_IN_PROC(src->sc_proc);

		/*
		 * TODO: This would fit better in the worker process.  We want
		 * to avoid interleaving IO from different processes in the same
		 * worker, to minimize vmspace switches in the workers.  The CPU
		 * scheduler will take care of interleaving the workers on CPU.
		 *
		 * That would also help avoid needing to allocate rio_io on the
		 * fly.  We could preallocate enough space in each src to have
		 * both rings full, and so there would be nothing to alloc or
		 * free per-IO.
		 *
		 * The catch is, multiple workers need to be able to work on the
		 * same src queue.  So, scheduling a src on workers will be more
		 * involved.
		 */
		for (issued = 0; issued < rio_attention_span; issued++) {
			struct rio_io *io;
			struct riocb *iocb;
			uint32_t index;
			int fd, error;
			u_int cmd;

			if ((iocb = rio_submissions_trydequeue(src, &index))
			    == NULL) {
				rio_src_deschedule(src);
				goto next;
			}
			io = uma_zalloc_arg(rio_io_zone, iocb,
			    M_WAITOK | M_ZERO);
			memcpy(&io->rio_cb, iocb, sizeof(*iocb));
			io->rio_src = src;
			io->rio_cb_index = index;
			fd = io->rio_cb.rio_ident;
			switch ((cmd = io->rio_cb.rio_cmd)) {
			case RIO_NOP:
			case RIO_MLOCK:
				error = 0;
				break;
			case RIO_WRITE:
			case RIO_WRITEV:
				error = fget_write(td, fd, &cap_pwrite_rights,
				    &io->rio_fd_file);
				break;
			case RIO_READ:
			case RIO_READV:
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
				rio_io_error(io, error);
				break;
			}
			/* XXX: Shouldn't this use a zone allocator? */
			if ((cmd & RIO_VECTORED) != 0 &&
			    __predict_false((error = copyiniov(
			    io->rio_cb.rio_iov, io->rio_cb.rio_length,
			    &io->rio_cb.rio_iov, EMSGSIZE)) != 0)) {
				rio_io_error(io, error);
				break;
			}
			/* TODO: Worker selection policy, e.g. IO classes. */
			rio_worker_enqueue(&DPCPU_GET(rio_worker), io);
		}
		rio_issuer_enqueue(self, src);
	}
	kthread_exit();
}

static inline int
rio_cb_error(struct rio *rio, struct rio_io *io)
{
	return (atomic_load_int(&rio->rio_control[io->rio_cb_index].rio_error));
}

static void
rio_worker_proc(void *arg)
{
	struct rio_worker *self = arg;
	struct proc *p = curproc;
	struct vmspace *myvm;

	sched_bind(curthread, self->rw_cpu);
	myvm = vmspace_acquire_ref(p);
	for (;;) {
		struct rio_softc *src;
		struct rio_io *io;
		struct vmspace *iovm;
		int error;

		/* TODO: make it actually return NULL when we need to exit */
		if ((io = rio_worker_dequeue(self)) == NULL) {
			break;
		}
		src = io->rio_src;
		if (__predict_false(rio_src_doomed(src))) {
			rio_io_error(io, ECANCELED);
		}
		/* Check for cancellation. */
		if (__predict_false((error = rio_cb_error(src->sc_rio, io))
		    != 0)) {
			rio_io_error(io, error);
			continue;
		}
		/* TODO: This may be optional depending on cmd? */
		if ((iovm = src->sc_proc->p_vmspace) != p->p_vmspace) {
			/* We're not AIO, but close enough. */
			vmspace_switch_aio(iovm);
		}
		/* TODO: perform IO (the tricky bit); see AIO for inspiration */
		rio_io_error(io, EIO);
		/*rio_io_complete(io)*/
		/* TODO: Policy for switching back to myvm here? */
	}
	vmspace_free(myvm);
	kproc_exit(0);
}

/* TODO: tunables, tuning */
static u_int rio_issuer_pcpu_threads = 2;
static u_int rio_worker_pcpu_threads = 4;

static int
rio_load(void)
{
	u_int cpu;
	int error;

	rio_doom = taskqueue_create("rio doom", M_WAITOK | M_ZERO,
	    taskqueue_thread_enqueue, &rio_doom);
	taskqueue_start_threads(&rio_doom, 1, PWAIT, "rio doom taskq");
	rio_destroy = rio_destroy_impl;
	rio_ioctl = rio_ioctl_impl;
	rio_io_zone = uma_zcreate("rio io", sizeof(struct rio_io), NULL, NULL,
	    NULL, NULL, UMA_ALIGN_PTR, 0);
	/* TODO: register process_* event handlers */
	CPU_FOREACH(cpu) {
		struct rio_issuer *issuer;
		struct rio_worker *worker;

		issuer = &DPCPU_ID_GET(cpu, rio_issuer);
		issuer->ri_cpu = cpu;
		mtx_init(&issuer->ri_lock, "rio issuer lock", NULL,
		    MTX_DEF | MTX_NEW);
		cv_init(&issuer->ri_cond, "rio issuer cond");
		STAILQ_INIT(&issuer->ri_srcs);
		/* TODO: automatic startup/shutdown (kick taskqueue?) */
		for (u_int i = 0; i < rio_issuer_pcpu_threads; i++) {
			/* Spawn issuer threads in the proc0 kernel process. */
			if ((error = kthread_add(rio_issuer_thread, issuer,
			    NULL, NULL, 0, 0, "rio issuer %u.%u", cpu,
			    i)) != 0) {
				/* TODO: error handling */
				return (error);
			}
		}

		worker = &DPCPU_ID_GET(cpu, rio_worker);
		worker->rw_cpu = cpu;
		mtx_init(&worker->rw_lock, "rio worker lock", NULL,
		    MTX_DEF | MTX_NEW);
		cv_init(&worker->rw_cond, "rio worker cond");
		STAILQ_INIT(&worker->rw_io_queue);
		for (u_int i = 0; i < rio_worker_pcpu_threads; i++) {
			/* Spawn each worker as its own kernel process. */
			if ((error = kproc_create(rio_worker_proc, worker,
			    NULL, 0, 0, "rio worker %u.%u", cpu, i)) != 0) {
				/* TODO: error handling */
				return (error);
			}
		}
	}
	return (0);
}

static int
rio_shutdown(void)
{
	u_int cpu;

	rio_destroy = NULL;
	rio_ioctl = NULL;
	CPU_FOREACH(cpu) {
		struct rio_issuer *issuer;
		struct rio_worker *worker;

		issuer = &DPCPU_ID_GET(cpu, rio_issuer);
		/* TODO: drain queue */
		mtx_destroy(&issuer->ri_lock);
		cv_destroy(&issuer->ri_cond);

		worker = &DPCPU_ID_GET(cpu, rio_worker);
		/* TODO: drain queue */
		mtx_destroy(&worker->rw_lock);
		cv_destroy(&worker->rw_cond);
	}
	/* TODO: drain taskqueue */
	taskqueue_free(rio_doom);
	/* TODO: destroy everything else (event handlers?) */
	uma_zdestroy(rio_io_zone);
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
