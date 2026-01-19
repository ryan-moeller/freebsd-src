/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#ifndef _SYS_RIO_H_
#define _SYS_RIO_H_

#include <sys/_types.h>
#include <sys/ioccom.h>
#include <ck_ec.h>
#include <ck_ring.h>

/* scheduling policies */
#define RIO_POLICY_NONE	0
/* TBD */

/* RIO ioctls */
struct rio_config {
	u_int	rio_sqlen;	/* submission queue length */
	u_int	rio_cqlen;	/* completion queue length */
	u_int	rio_ncb;	/* control block count */
	u_int	rio_policy_id;	/* scheduling policy */
	/* TODO: policy options */
};
#define	FIORIOCONFIGURE	_IOW('R', 0, struct rio_config)
#define	FIORIOSUBMIT	_IO('R', 1)
/* TODO: ioctls to change configuration? */

/* RIO commands */
#define RIO_NOP		0x0
#define	RIO_WRITE	0x1
#define	RIO_READ	0x2
#define	RIO_VECTORED	0x4
#define	RIO_WRITEV	(RIO_WRITE | RIO_VECTORED)
#define	RIO_READV	(RIO_READ | RIO_VECTORED)
#define	RIO_SYNC	0x8
#define	RIO_DSYNC	(0x10 | RIO_SYNC)
#define	RIO_MLOCK	0x20
#define RIO_FOFFSET	0x40
/* TODO: extend beyond lio */

/* TODO: flags? spare fields? sequence/generation number? sigevent? */
struct riocb {
	uintptr_t	rio_ident;	/* fd or whatever */
	u_int		rio_cmd;
	int		rio_error;	/* set ECANCELED to cancel */
	ssize_t		rio_status;
	off_t		rio_offset;
	size_t		rio_length;
	union {
		uintptr_t	rio_data;
		void		*rio_buf;
		struct iovec	*rio_iov;
	};
};
/* TODO: support a pre-mapped buffer pool */

struct rio_slot {
	uint32_t	rs_index;	/* index of a riocb in rio_control */
};
CK_RING_PROTOTYPE(rio, rio_slot)

/*
 * TODO: replace ck_ring_t and ck_ec32_t with an ABI-stable version combining
 * ring and wait/wake words (head and tail become the event counters)?
 */
struct rio_ring {
	ck_ring_t	rr_ring;	/* queue ring */
	ck_ec32_t	rr_nqc;		/* enqueue counter */
	ck_ec32_t	rr_dqc;		/* dequeue counter */
};

struct rio {
	struct rio_ring	rio_submission;	/* submission queue */
	struct rio_ring	rio_completion;	/* completion queue */
	struct riocb	rio_control[];	/* control blocks */
};
/*
 * IDEA: array of kernel-allocated buffers following the control blocks?
 * Basically like netmap.  The benefit would be zero-copy pre-mapped buffers?
 * But you don't get zero-copy in reality for rio, it's not like these buffers
 * are actually getting handed to hardware.  So, may be not very useful.
 */

static inline struct rio_slot *
rio_submission_slots(struct rio *rio, const struct rio_config *conf)
{
	return ((struct rio_slot *)&rio->rio_control[conf->rio_ncb]);
}

static inline struct rio_slot *
rio_completion_slots(struct rio *rio, const struct rio_config *conf)
{
	return ((struct rio_slot *)&rio->rio_control[conf->rio_ncb] +
	    conf->rio_sqlen);
}

static inline size_t
rio_config_size(const struct rio_config *conf)
{
	return (sizeof(struct rio) + conf->rio_ncb * sizeof(struct riocb) +
	    (conf->rio_sqlen + conf->rio_cqlen) * sizeof(struct rio_slot));
}

#ifdef _KERNEL
#include <sys/file.h>

struct rio_softc;

#ifdef RIO
fo_ioctl_t rio_ioctl;
void rio_destroy(struct rio_softc *);
#endif
#else
#include <sys/cdefs.h>

/* TODO: this part goes in librio */
/*
 * With SHM the user must do the following:
 *
 * size = rio_config_size(config)
 * fd = shm_open(SHM_ANON, O_CLOFORK | O_CLOEXEC, 0)
 * ftruncate(fd, size)
 * rio = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
 * minherit(rio, size, INHERIT_NONE)
 * ioctl(fd, FIORIOCONFIGURE, config)
 * <enqueue control blocks to submission ring>
 * ioctl(fd, FIORIOSUBMIT)
 * <dequeue control blocks from completion ring>
 * munmap(rio, size)
 * close(fd)
 *
 * librio handles those details and provides a simpler API.
 */
struct _rio {
	struct rio	*rio_mapped;		/* mapped memory region */
	struct rio_ring	rio_freelist;		/* control blocks freelist */
	struct rio_slot *rio_freelist_slots;	/* freelist slots */
	struct rio_slot *rio_submission_slots;	/* pointer into mapped */
	struct rio_slot *rio_completion_slots;	/* pointer into mapped */
	int		rio_fd;			/* SHM file descriptor */
};

typedef struct _rio *rio_t;

__BEGIN_DECLS
int rio_create(rio_t *rio, u_int sqlen, u_int cqlen, u_int ncb, u_int policyid);
/* TODO: queue manipulation ops in all their variety */
int rio_submit(rio_t *rio, const struct rio_policy *policy);
int rio_destroy(rio_t *rio);
__END_DECLS
#endif /* !_KERNEL */

#endif /* !_SYS_RIO_H_ */
