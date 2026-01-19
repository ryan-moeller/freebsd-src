/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#ifndef _LIBRIO_H_
#define _LIBRIO_H_

#include <sys/cdefs.h>
#include <sys/rio.h>
#include <sys/uio.h>

typedef struct _rio *rio_t;

__BEGIN_DECLS
int rio_create(rio_t *, u_int sqlen, u_int cqlen, u_int ncb, u_int policyid);
int rio_read(rio_t, int, void *, size_t, const struct timespec *);
int rio_write(rio_t, int, const void *, size_t, const struct timespec *);
int rio_readv(rio_t, int, const struct iovec *, int, const struct timespec *);
int rio_writev(rio_t, int, const struct iovec *, int, const struct timespec *);
int rio_fsync(rio_t, int, const struct timespec *);
int rio_fdatasync(rio_t, int, const struct timespec *);
int rio_mlock(rio_t, void *, size_t, const struct timespec *);
/* TODO: The rest of the IO ops go here. */
int rio_submit(rio_t);
struct riocb *rio_poll(rio_t, const struct timespec *);
void rio_return(rio_t, struct riocb *);
void rio_destroy(rio_t);
__END_DECLS

#endif /* _LIBRIO_H_ */
