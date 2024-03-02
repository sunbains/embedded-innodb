/**
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

/** @file os/os0proc.c
The interface to the operating system
process control primitives

Created 9/30/1995 Heikki Tuuri
*******************************************************/

#include "innodb0types.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/mman.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "os0proc.h"
#ifdef UNIV_NONINL
#include "os0proc.ic"
#endif

#include "ut0byte.h"
#include "ut0mem.h"

/* FreeBSD for example has only MAP_ANON, Linux has MAP_ANONYMOUS and
MAP_ANON but MAP_ANON is marked as deprecated */
#if defined(MAP_ANONYMOUS)
#define OS_MAP_ANON MAP_ANONYMOUS
#elif defined(MAP_ANON)
#define OS_MAP_ANON MAP_ANON
#endif

bool os_use_large_pages;
/* Large page size. This may be a boot-time option on some platforms */
ulint os_large_page_size;

/** Reset the variables. */

void os_proc_var_init(void) {
  os_use_large_pages = 0;
  os_large_page_size = 0;
}

/** Converts the current process id to a number. It is not guaranteed that the
number is unique. In Linux returns the 'process number' of the current
thread. That number is the same as one sees in 'top', for example. In Linux
the thread id is not the same as one sees in 'top'.
@return	process id as a number */

ulint os_proc_get_number(void) {
  return (ulint)getpid();
}

/** Allocates large pages memory.
@return	allocated memory */

void *os_mem_alloc_large(ulint *n) /*!< in/out: number of bytes */
{
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
    ib_logger(ib_stream,
              "InnoDB: HugeTLB: Warning: Failed to allocate"
              " %lu bytes. errno %d\n",
              size, errno);
    ptr = NULL;
  } else {
    ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1) {
      ib_logger(ib_stream,
                "InnoDB: HugeTLB: Warning: Failed to"
                " attach shared memory segment, errno %d\n",
                errno);
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

  ib_logger(ib_stream, "InnoDB HugeTLB: Warning: Using conventional"
                       " memory pool\n");
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
  ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | OS_MAP_ANON, -1,
             0);
  if (unlikely(ptr == (void *)-1)) {
    ib_logger(ib_stream,
              "InnoDB: mmap(%lu bytes) failed;"
              " errno %lu\n",
              (ulong)size, (ulong)errno);
    ptr = NULL;
  } else {
    os_fast_mutex_lock(&ut_list_mutex);
    ut_total_allocated_memory += size;
    os_fast_mutex_unlock(&ut_list_mutex);
    UNIV_MEM_ALLOC(ptr, size);
  }
  return (ptr);
}

void os_mem_free_large(void *ptr, ulint size) {
  os_fast_mutex_lock(&ut_list_mutex);
  ut_a(ut_total_allocated_memory >= size);
  os_fast_mutex_unlock(&ut_list_mutex);

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
  if (munmap(ptr, size)) {
    ib_logger(ib_stream,
              "InnoDB: munmap(%p, %lu) failed;"
              " errno %lu\n",
              ptr, (ulong)size, (ulong)errno);
  } else {
    os_fast_mutex_lock(&ut_list_mutex);
    ut_a(ut_total_allocated_memory >= size);
    ut_total_allocated_memory -= size;
    os_fast_mutex_unlock(&ut_list_mutex);
    UNIV_MEM_FREE(ptr, size);
  }
}
