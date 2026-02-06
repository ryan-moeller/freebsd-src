/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#ifndef _SYS_RIO_H_
#define _SYS_RIO_H_

#include <sys/types.h>
#include <sys/ioccom.h>

/* scheduling policy presets */
#define	RIO_POLICY_SOFT_AFFINITY	0
#define	RIO_POLICY_LEAST_LOADED		1
#define	RIO_POLICY_ROUND_ROBIN		2
#define	RIO_POLICY_LOCAL_FLOW		3

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
/* TODO: ioctls to change policy(/configuration?) */

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
#define RIO_CMD_FLAGS	(RIO_VECTORED | RIO_FOFFSET)

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

#ifdef _KERNEL
#ifdef RIO
#include <sys/file.h>

fo_ioctl_t rio_ioctl;
void rio_destroy(struct rio_softc *);
void rio_vmspace_exit(struct vmspace *);
#endif /* RIO */
#endif /* _KERNEL */

#endif /* !_SYS_RIO_H_ */
