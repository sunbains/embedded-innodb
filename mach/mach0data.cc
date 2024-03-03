/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file mach/mach0data.c
Utilities for converting data from the database file
to the machine format.

Created 11/28/1995 Heikki Tuuri
***********************************************************************/

#include "mach0data.h"

byte *mach_parse_compressed(byte *ptr, byte *end_ptr, ulint *v) {
  if (ptr >= end_ptr) {

    return nullptr;
  }

  auto flag = mach_read_from_1(ptr);

  if (flag < 0x80UL) {

    *v = flag;

    return (ptr + 1);

  } else if (flag < 0xC0UL) {

    if (end_ptr < ptr + 2) {

      return nullptr;
    }

    *v = mach_read_from_2(ptr) & 0x7FFFUL;

    return (ptr + 2);

  } else if (flag < 0xE0UL) {

    if (end_ptr < ptr + 3) {

      return nullptr;
    }

    *v = mach_read_from_3(ptr) & 0x3FFFFFUL;

    return (ptr + 3);

  } else if (flag < 0xF0UL) {

    if (end_ptr < ptr + 4) {

      return nullptr;
    }

    *v = mach_read_from_4(ptr) & 0x1FFFFFFFUL;

    return (ptr + 4);

  } else {

    ut_ad(flag == 0xF0UL);

    if (end_ptr < ptr + 5) {

      return nullptr;
    }

    *v = mach_read_from_4(ptr + 1);

    return ptr + 5;
  }
}

byte *mach_uint64_parse_compressed(byte *ptr, byte *end_ptr, uint64_t *v) {
  if (end_ptr < ptr + 5) {

    return nullptr;
  }

  auto high = mach_read_compressed(ptr);
  auto size = mach_get_compressed_size(high);

  ptr += size;

  if (end_ptr < ptr + 4) {

    return nullptr;
  }

  auto low = mach_read_from_4(ptr);

  *v = (uint64_t(high) << 32) | low;

  return ptr + 4;
}
