/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file include/ut0byte.h
Utilities for byte operations

Created 1/20/1994 Heikki Tuuri
***********************************************************************/

#pragma once

#include "innodb0types.h"

[[nodiscard]] inline uint64_t ut_uint64_align_down(uint64_t n, ulint align_no) {
  ut_ad(align_no > 0);
  ut_ad(ut_is_2pow(align_no));

  return n & ~((uint64_t)align_no - 1);
}

[[nodiscard]] inline uint64_t ut_uint64_align_up(uint64_t n, ulint align_no) {
  const uint64_t align_1 = (uint64_t)align_no - 1;

  ut_ad(align_no > 0);
  ut_ad(ut_is_2pow(align_no));

  return (n + align_1) & ~align_1;
}

[[nodiscard]] inline void *ut_align(const void *ptr, ulint align_no) {
  ut_ad(align_no > 0);
  ut_ad(((align_no - 1) & align_no) == 0);
  ut_ad(ptr);

  ut_ad(sizeof(void *) == sizeof(ulint));

  return (void *)((((ulint)ptr) + align_no - 1) & ~(align_no - 1));
}

[[nodiscard]] inline void *ut_align_down(const void *ptr, ulint align_no) {
  ut_ad(align_no > 0);
  ut_ad(((align_no - 1) & align_no) == 0);
  ut_ad(ptr);

  ut_ad(sizeof(void *) == sizeof(ulint));

  return (void *)((((ulint)ptr)) & ~(align_no - 1));
}

[[nodiscard]] inline ulint ut_align_offset(const void *ptr, ulint align_no) {
  ut_ad(align_no > 0);
  ut_ad(((align_no - 1) & align_no) == 0);
  ut_ad(ptr);

  ut_ad(sizeof(void *) == sizeof(ulint));

  return (ulint(ptr)) & (align_no - 1);
}

[[nodiscard]] inline bool ut_bit_get_nth(ulint a, ulint n) {
  ut_ad(n < 8 * sizeof(ulint));
  return (1 & (a >> n)) != 0;
}

[[nodiscard]] inline ulint ut_bit_set_nth(ulint a, ulint n, bool val) {
  ut_ad(n < 8 * sizeof(ulint));

  if (val) {
    return (ulint(1) << n) | a;
  } else {
    return ~(ulint(1) << n) & a;
  }
}
