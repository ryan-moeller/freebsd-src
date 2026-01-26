/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <sys/param.h>
#include <sys/mman.h>
#include <sys/umtx.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <ck_ec.h>
#include <ck_ring.h>

#include <librio.h>
#include <rio/rio_internal.h>

struct _rio {
	struct rio	*rio_mapped;		/* mapped memory region */
	ck_ring_t	rio_freelist;		/* control blocks freelist */
	ck_ec32_t	rio_freelist_nqc;	/* freelist enqueue events */
	struct rio_slot *rio_freelist_slots;	/* freelist slots */
	struct rio_slot *rio_submission_slots;	/* pointer into mapped */
	struct rio_slot *rio_completion_slots;	/* pointer into mapped */
	size_t		rio_size;		/* mapped region size */
	int		rio_fd;			/* SHM file descriptor */
};

static int
gettime(const struct ck_ec_ops *op __unused, struct timespec *out)
{
	return (clock_gettime(CLOCK_MONOTONIC, out));
}

static void
wait32(const struct ck_ec_wait_state *state __unused, const uint32_t *address,
    uint32_t expected, const struct timespec *deadline)
{
	if (_umtx_op(__DECONST(uint32_t *, address), UMTX_OP_WAIT_UINT_PRIVATE,
	    expected, (void *)(uintptr_t)sizeof(*deadline),
	    __DECONST(struct timespec *, deadline)) == -1) {
		int error = errno;

		/* TODO: How to handle errors? EINTR? */
		(void)error;
	}
}

static void
wake32(const struct ck_ec_ops *op __unused, const uint32_t *address)
{
	_umtx_op(__DECONST(uint32_t *, address), UMTX_OP_WAKE_PRIVATE, 1, NULL,
	    NULL);
}

static const struct ck_ec_ops rio_ec_umtx_ops = {
	.gettime = gettime,
	.wait32 = wait32,
	.wake32 = wake32,
	/* TODO: ABI-stable tuning? */
};

static const struct ck_ec_mode rio_ec_umtx_mode = {
	.ops = &rio_ec_umtx_ops,
	.single_producer = false,
};

/* Take a control block from the freelist. */
static inline struct riocb *
rio_reserve(rio_t rio, const struct timespec *deadline)
{
	struct rio_slot slot;
	struct riocb *iocb;
	uint32_t value;

	/* XXX: could give the user more options for freelist usage pattern */
	for (;;) {
		value = ck_ec_value(&rio->rio_freelist_nqc);
		if (CK_RING_DEQUEUE_MPMC(rio, &rio->rio_freelist,
		    rio->rio_freelist_slots, &slot)) {
			iocb = rio->rio_mapped->rio_control + slot.rs_index;
			memset(iocb, 0, sizeof(*iocb));
			return (iocb);
		}
		/* TODO: variation with predicate? How to handle EINTR? */
		if (ck_ec_wait(&rio->rio_freelist_nqc, &rio_ec_umtx_mode, value,
		    deadline) == -1) {
			return (NULL);
		}
	}
	__unreachable();
}

/* Enqueue a submission. */
static inline int
rio_enqueue(rio_t rio, struct riocb *iocb, const struct timespec *deadline)
{
	struct rio_slot slot;
	struct rio *shm = rio->rio_mapped;
	uint32_t value;

	slot.rs_index = iocb - shm->rio_control;
	for (;;) {
		value = ck_ec_value(&shm->rio_submission.rr_dqc);
		if (CK_RING_ENQUEUE_MPSC(rio, &shm->rio_submission.rr_ring,
		    rio->rio_submission_slots, &slot)) {
			ck_ec_inc(&shm->rio_submission.rr_nqc,
			    &rio_ec_umtx_mode);
			return (0);
		}
		/* TODO: predicates? how to handle EINTR? */
		if (ck_ec_wait(&shm->rio_submission.rr_dqc, &rio_ec_umtx_mode,
		    value, deadline) == -1) {
			return (-1);
		}
	}
	__unreachable();
}

/* Return a control block to the freelist. */
static inline void
rio_return(rio_t rio, struct riocb *iocb)
{
	struct rio_slot slot;

	slot.rs_index = iocb - rio->rio_mapped->rio_control;
	CK_RING_ENQUEUE_MPMC(rio, &rio->rio_freelist, rio->rio_freelist_slots,
	    &slot);
	ck_ec_inc(&rio->rio_freelist_nqc, &rio_ec_umtx_mode);
}

static inline int
rio_start(rio_t rio, const struct riocb *iocb, u_int cmd,
    const struct timespec *deadline)
{
	struct riocb *riocb;

	if ((riocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	memcpy(riocb, iocb, sizeof(*iocb));
	riocb->rio_cmd = cmd;
	if (rio_enqueue(rio, riocb, deadline) == -1) {
		rio_return(rio, riocb);
		errno = ETIMEDOUT;
		return (-1);
	}
	return (0);
}

/* Set up a handle. */
int
rio_create(rio_t *riop, u_int sqlen, u_int cqlen, u_int ncb, u_int policyid)
{
	struct rio_config conf = {
		.rio_sqlen = sqlen,
		.rio_cqlen = cqlen,
		.rio_ncb = ncb,
		.rio_policy_id = policyid,
	};
	struct rio_slot *slots;
	rio_t rio;
	void *p;
	size_t size;
	u_int freelist_size;
	int fd, error;

	if (riop == NULL ||
	    !(powerof2(sqlen) && powerof2(cqlen) && powerof2(ncb))) {
		errno = EINVAL;
		return (-1);
	}
	if ((rio = malloc(sizeof(*rio))) == NULL) {
		return (-1);
	}
	/*
	 * Rings must be a power of 2 size and always waste one slot.  Double
	 * the size of the freelist ring buffer to ensure we can supply the
	 * requested number of control blocks.  The rest of the ring sizes are
	 * still off by one, but those are rings the user is expected to be
	 * aware of.  The freelist is a hidden implementation detail.
	 */
	freelist_size = ncb * 2;
	if ((slots = calloc(freelist_size, sizeof(*slots))) == NULL) {
		error = errno;
		goto error_free;
	}
	if ((fd = shm_open(SHM_ANON, O_RDWR | O_CLOEXEC | O_CLOFORK | O_CREAT,
	    0)) == -1) {
		error = errno;
		goto error_free;
	}
	size = rio_config_size(&conf);
	if (ftruncate(fd, size) == -1) {
		error = errno;
		goto error_close;
	}
	if ((p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0))
	    == NULL) {
		error = errno;
		goto error_close;
	}
	if (minherit(p, size, INHERIT_NONE) == -1) {
		error = errno;
		goto error_munmap;
	}
	if (ioctl(fd, FIORIOCONFIGURE, &conf) == -1) {
		error = errno;
		goto error_munmap;
	}
	rio->rio_mapped = p;
	ck_ring_init(&rio->rio_freelist, freelist_size);
	/* Only populate ncb slots in the freelist. */
	for (struct rio_slot slot = {0}; slot.rs_index < ncb; slot.rs_index++) {
		CK_RING_ENQUEUE_MPMC(rio, &rio->rio_freelist, slots, &slot);
	}
	ck_ec_init(&rio->rio_freelist_nqc, ncb);
	/* The freelist doesn't need a dequeue event counter. */
	rio->rio_freelist_slots = slots;
	rio->rio_submission_slots = rio_submission_slots(p, &conf);
	rio->rio_completion_slots = rio_completion_slots(p, &conf);
	rio->rio_size = size;
	rio->rio_fd = fd;
	*riop = rio;
	return (0);
error_munmap:
	munmap(p, size);
error_close:
	close(fd);
error_free:
	free(slots);
	free(rio);
	errno = error;
	return (-1);
}

int
rio_read(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_READ, deadline));
}

int
rio_write(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_WRITE, deadline));
}

int
rio_readv(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_READV, deadline));
}

int
rio_writev(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_WRITEV, deadline));
}

int
rio_fsync(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_SYNC, deadline));
}

int
rio_fdatasync(rio_t rio, const struct riocb *iocb,
    const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_DSYNC, deadline));
}

int
rio_mlock(rio_t rio, const struct riocb *iocb, const struct timespec *deadline)
{
	return (rio_start(rio, iocb, RIO_MLOCK, deadline));
}

/* TODO: the rest of the ops */

/* Submit the queue to the kernel for scheduling. */
int
rio_submit(rio_t rio)
{
	return (ioctl(rio->rio_fd, FIORIOSUBMIT));
}

/* Pull a completed control block off the completion queue. */
int
rio_poll(rio_t rio, struct riocb *iocb, const struct timespec *deadline)
{
	struct rio_slot slot;
	struct rio *shm = rio->rio_mapped;
	struct riocb *riocb;
	uint32_t value;

	for (;;) {
		value = ck_ec_value(&shm->rio_completion.rr_nqc);
		if (CK_RING_DEQUEUE_MPMC(rio, &shm->rio_completion.rr_ring,
		    rio->rio_completion_slots, &slot)) {
			ck_ec_inc(&shm->rio_completion.rr_dqc,
			    &rio_ec_umtx_mode);
			riocb = shm->rio_control + slot.rs_index;
			if (iocb != NULL) {
				memcpy(iocb, riocb, sizeof(*iocb));
			}
			rio_return(rio, riocb);
			return (0);
		}
		/* TODO: predicate? */
		if (ck_ec_wait(&shm->rio_completion.rr_nqc,
		    &rio_ec_umtx_mode, value, deadline) == -1) {
			errno = ETIMEDOUT;
			return (-1);
		}
	}
	__unreachable();
}

/* Release handle resources. */
void
rio_destroy(rio_t rio)
{
	munmap(rio->rio_mapped, rio->rio_size);
	close(rio->rio_fd);
	free(rio->rio_freelist_slots);
	free(rio);
}
