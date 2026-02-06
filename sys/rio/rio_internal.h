/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#pragma once

#include <sys/rio.h>

#include <ck_ec.h>
#include <ck_ring.h>

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
#include <sys/malloc.h>

MALLOC_DECLARE(M_RIO);
#endif /* !_KERNEL */
