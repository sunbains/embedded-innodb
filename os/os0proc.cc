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

bool os_use_large_pages;

/* Large page size. This may be a boot-time option on some platforms */
ulint os_large_page_size;

/** Reset the variables. */

void os_proc_var_init() {
  os_use_large_pages = 0;
  os_large_page_size = 0;
}

ulint os_proc_get_number() {
  return (ulint)getpid();
}

void *os_mem_alloc_large(ulint *n) {
  void *ptr;
  ulint size;

#if defined HAVE_LARGE_PAGES && defined UNIV_LINUX
  int shmid;
  struct shmid_ds buf;

  if (!os_use_large_pages || !os_large_page_size) {
    goto skip;
  }

  /* Align block size to os_large_page_size */
  ut_ad(ut_is_2pow(os_large_page_size));
  size = ut_2pow_round(*n + (os_large_page_size - 1), os_large_page_size);

  shmid = shmget(IPC_PRIVATE, (size_t)size, SHM_HUGETLB | SHM_R | SHM_W);
  if (shmid < 0) {
    ib_logger(
      ib_stream,
      "HugeTLB: Warning: Failed to allocate"
      " %lu bytes. errno %d\n",
      size,
      errno
    );
    ptr = NULL;
  } else {);

    ut_a(block->m_magic_n == UT_MEM_MAGIC_N);
    ut_a(ut_total_allocated_memory >= block->m_size);
    ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1) {
      ib_logger(
        ib_stream,
        "HugeTLB: Warning: Failed to"
        " attach shared memory segment, errno %d\n",
        errno
      );
      ptr = NULL;
    }

    /* Remove the shared memory segment so that it will be
    automatically freed after memory is detached or
    process exits */
    shmctl(shmid, IPC_RMID, &buf);
  }

  if (ptr) {
    *n = size;
    os_fast_mutex_lock(&ut_list_mutex);
    ut_total_allocated_memory += size;
    os_fast_mutex_unlock(&ut_list_mutex);
#ifdef UNIV_SET_MEM_TO_ZERO
    memset(ptr, '\0', size);
#endif
    UNIV_MEM_ALLOC(ptr, size);
    return (ptr);
  }

  ib_logger(ib_stream, "InnoDB HugeTLB: Warning: Using conventional memory pool\n");
skip:
#endif /* HAVE_LARGE_PAGES && UNIV_LINUX */

#ifdef HAVE_GETPAGESIZE
  size = getpagesize();
#else
  size = UNIV_PAGE_SIZE;
#endif /* HAVE_GETPAGESiZE */

  /* Align block size to system page size */
  ut_ad(ut_is_2pow(size));
  size = *n = ut_2pow_round(*n + (size - 1), size);
  ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | OS_MAP_ANON, -1, 0);
  if (ptr == (void *)-1) {
    ib_logger(ib_stream, "mmap(%lu bytes) failed;errno %lu\n", (ulong)size, (ulong)errno);
    ptr = nullptr;
  } else {
    ut_allocated_memory(size);
    UNIV_MEM_ALLOC(ptr, size);
  }
  return (ptr);
}

void os_mem_free_large(void *ptr, ulint size) {
  ut_a(ut_total_allocated_memory() >= size);

#if defined HAVE_LARGE_PAGES && defined UNIV_LINUX
  if (os_use_large_pages && os_large_page_size && !shmdt(ptr)) {
    os_fast_mutex_lock(&ut_list_mutex);
    ut_a(ut_total_allocated_memory >= size);
    ut_total_allocated_memory -= size;
    os_fast_mutex_unlock(&ut_list_mutex);
    UNIV_MEM_FREE(ptr, size);
    return;
  }
#endif /* HAVE_LARGE_PAGES && UNIV_LINUX */
  if (munmap(ptr, size) != 0) {
    log_err(std::format("munmap({}, {}) failed; errno {}: {}", ptr, size, errno, strerror(errno)));
  } else {
    ut_deallocated_memory(size);
    UNIV_MEM_FREE(ptr, size);
  }
}
