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
/* TBD (perhaps a policy that creates a dedicated polling kernel thread?) */

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
#ifdef RIO
#include <sys/file.h>

struct rio_softc;

fo_ioctl_t rio_ioctl;
void rio_destroy(struct rio_softc *);
#endif /* RIO */
#endif /* _KERNEL */

#endif /* !_SYS_RIO_H_ */
