/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/umtx.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <ck_ec.h>
#include <ck_ring.h>

#include <librio.h>

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
	/* TODO: reevaluate clock selection */
	return (clock_gettime(CLOCK_MONOTONIC, out));
}

static void
wait32(const struct ck_ec_wait_state *state __unused, const uint32_t *address,
    uint32_t expected, const struct timespec *deadline)
{
	/* TODO: EINTR? */
	_umtx_op(__DECONST(uint32_t *, address), UMTX_OP_WAIT_UINT, expected,
	    (void *)(uintptr_t)sizeof(*deadline),
	    __DECONST(struct timespec *, deadline));
}

static void
wake32(const struct ck_ec_ops *op __unused, const uint32_t *address)
{
	_umtx_op(__DECONST(uint32_t *, address), UMTX_OP_WAKE, INT_MAX, NULL,
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
		/* TODO: variation with predicate? */
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
		if (CK_RING_ENQUEUE_MPMC(rio, &shm->rio_submission.rr_ring,
		    rio->rio_submission_slots, &slot)) {
			ck_ec_inc(&shm->rio_submission.rr_nqc,
			    &rio_ec_umtx_mode);
			return (0);
		}
		/* TODO: predicates? */
		if (ck_ec_wait(&shm->rio_submission.rr_dqc,
		    &rio_ec_umtx_mode, value, deadline) == -1) {
			return (-1);
		}
	}
	__unreachable();
}

/* Enqueue a submission or return it to the freelist on timeout. */
static inline int
rio_start(rio_t rio, struct riocb *iocb, const struct timespec *deadline)
{
	if (rio_enqueue(rio, iocb, deadline) == -1) {
		rio_return(rio, iocb);
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
	int fd, error;

	if (riop == NULL) {
		errno = EINVAL;
		return (-1);
	}
	if ((rio = malloc(sizeof(*rio))) == NULL) {
		return (-1);
	}
	if ((slots = calloc(ncb, sizeof(*slots))) == NULL) {
		error = errno;
		goto error_free;
	}
	if ((fd = shm_open(SHM_ANON, O_CLOEXEC | O_CLOFORK, 0)) == -1) {
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
	ck_ring_init(&rio->rio_freelist, size);
	ck_ec_init(&rio->rio_freelist_nqc, 0);
	/* The freelist doesn't need the dequeue event counter. */
	for (struct rio_slot slot = {0}; slot.rs_index < ncb; slot.rs_index++) {
		CK_RING_ENQUEUE_SPSC(rio, &rio->rio_freelist, slots, &slot);
	}
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
rio_read(rio_t rio, int fd, void *buf, size_t len,
    const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_READ;
	iocb->rio_ident = fd;
	iocb->rio_buf = buf;
	iocb->rio_length = len;
	return (rio_start(rio, iocb, deadline));
}

int
rio_write(rio_t rio, int fd, const void *buf, size_t len,
    const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_WRITE;
	iocb->rio_ident = fd;
	iocb->rio_buf = __DECONST(void *, buf);
	iocb->rio_length = len;
	return (rio_start(rio, iocb, deadline));
}

int
rio_readv(rio_t rio, int fd, const struct iovec *iov, int niov,
    const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_READV;
	iocb->rio_ident = fd;
	iocb->rio_iov = __DECONST(struct iovec *, iov);
	iocb->rio_length = niov;
	return (rio_start(rio, iocb, deadline));
}

int
rio_writev(rio_t rio, int fd, const struct iovec *iov, int niov,
    const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_WRITEV;
	iocb->rio_ident = fd;
	iocb->rio_iov = __DECONST(struct iovec *, iov);
	iocb->rio_length = niov;
	return (rio_start(rio, iocb, deadline));
}

int
rio_fsync(rio_t rio, int fd, const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_SYNC;
	iocb->rio_ident = fd;
	return (rio_start(rio, iocb, deadline));
}

int
rio_fdatasync(rio_t rio, int fd, const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_DSYNC;
	iocb->rio_ident = fd;
	return (rio_start(rio, iocb, deadline));
}

int
rio_mlock(rio_t rio, void *addr, size_t len, const struct timespec *deadline)
{
	struct riocb *iocb;

	if ((iocb = rio_reserve(rio, deadline)) == NULL) {
		errno = ETIMEDOUT;
		return (-1);
	}
	iocb->rio_cmd = RIO_MLOCK;
	iocb->rio_ident = (uintptr_t)addr;
	iocb->rio_length = len;
	return (rio_start(rio, iocb, deadline));
}

/* TODO: the rest of the ops */

/* Submit the queue to the kernel for scheduling. */
int
rio_submit(rio_t rio)
{
	return (ioctl(rio->rio_fd, FIORIOSUBMIT));
}

/* Pull a completed control block off the completion queue. */
struct riocb *
rio_poll(rio_t rio, const struct timespec *deadline)
{
	struct rio_slot slot;
	struct rio *shm = rio->rio_mapped;
	uint32_t value;

	for (;;) {
		value = ck_ec_value(&shm->rio_completion.rr_nqc);
		if (CK_RING_DEQUEUE_MPMC(rio, &shm->rio_completion.rr_ring,
		    rio->rio_completion_slots, &slot)) {
			ck_ec_inc(&shm->rio_completion.rr_dqc,
			    &rio_ec_umtx_mode);
			return (shm->rio_control + slot.rs_index);
		}
		/* TODO: predicate? */
		if (ck_ec_wait(&shm->rio_completion.rr_nqc,
		    &rio_ec_umtx_mode, value, deadline) == -1) {
			return (NULL);
		}
	}
	__unreachable();
}

/* Return a control block to the freelist. */
void
rio_return(rio_t rio, struct riocb *iocb)
{
	struct rio_slot slot;

	slot.rs_index = iocb - rio->rio_mapped->rio_control;
	CK_RING_ENQUEUE_MPMC(rio, &rio->rio_freelist, rio->rio_freelist_slots,
	    &slot);
	ck_ec_inc(&rio->rio_freelist_nqc, &rio_ec_umtx_mode);
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
