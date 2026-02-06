/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#pragma once

#include <sys/types.h>
#include <sys/file.h>
#include <sys/rio.h>

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

static inline int
rio_io_foflag(struct rio_io *io)
{
	return ((rio_io_flags(io) & RIO_FOFFSET) == 0 ? FOF_OFFSET : 0);
}
