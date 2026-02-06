/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#pragma once

#include <sys/param.h>
#include <sys/limits.h>
#include <sys/bitstring.h>
#include <sys/malloc.h>

#include "rio_internal.h"

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
	u_int stop = sel->rs_len - 1;

	bit_nclear(sel->rs_empty, 0, stop);
	bit_nclear(sel->rs_affine, 0, stop);
	bit_nclear(sel->rs_minimum, 0, stop);
	sel->rs_min = UINT_MAX;
	sel->rs_n = 0;
}

#if 0
static inline void
dump_bits(bitstr_t *bits, size_t len)
{
	ssize_t count;
	u_int i;

	bit_count(bits, 0, len, &count);
	printf("%s: len=%zu count=%zd\n", __func__, len, count);
	bit_foreach(bits, len, i) {
		printf("%s: bit %u set\n", __func__, i);
	}
}
#endif

static inline void
rio_selector_insert(struct rio_selector *sel, u_int qlen, bool affine)
{
	u_int idx = sel->rs_n++;

	if (qlen == 0) {
		bit_set(sel->rs_empty, idx);
	}
	if (affine) {
		bit_set(sel->rs_affine, idx);
	}
	if (qlen == sel->rs_min) {
		bit_set(sel->rs_minimum, idx);
	} else if (qlen < sel->rs_min) {
		sel->rs_min = qlen;
		bit_nclear(sel->rs_minimum, 0, idx);
		bit_set(sel->rs_minimum, idx);
	}
}

static inline void
rio_selector_destroy(struct rio_selector *sel)
{
	free(sel->rs_empty, M_RIO);
	free(sel->rs_affine, M_RIO);
	free(sel->rs_minimum, M_RIO);
	free(sel->rs_candidates, M_RIO);
	free(sel->rs_indices, M_RIO);
}
