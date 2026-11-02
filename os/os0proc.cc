/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file os/os0proc.c
The interface to the operating system
process control primitives

Created 9/30/1995 Heikki Tuuri
*******************************************************/

#include "innodb0types.h"

#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNIST_H */

#include "ut0byte.h"
#include "ut0mem.h"

#define OS_MAP_ANON MAP_ANONYMOUS

ulint os_proc_get_number() {
  return (ulint)getpid();
}

void *os_mem_alloc_large(ulint *n) {
  /* Align block size to system page size */
  const ulint page_size = getpagesize();

  ut_ad(ut_is_2pow(page_size));

  const auto size = *n = ut_2pow_round(*n + (page_size - 1), page_size);
  auto ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | OS_MAP_ANON, -1, 0);

  if (ptr == MAP_FAILED) {
    log_err(std::format("mmap({} bytes) failed; errno {}", size, errno));
    return nullptr;
  }

  UNIV_MEM_ALLOC(ptr, size);

  return ptr;
}

void os_mem_free_large(void *ptr, ulint size) {
  if (munmap(ptr, size) != 0) {
    log_err(std::format("munmap({}, {}) failed; errno {}: {}", ptr, size, errno, strerror(errno)));
  } else {
    UNIV_MEM_FREE(ptr, size);
  }
}
