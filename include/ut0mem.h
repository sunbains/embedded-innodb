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

/** @file include/ut0mem.h
Memory primitives

Created 5/30/1994 Heikki Tuuri
************************************************************************/

#pragma once

#include "innodb0types.h"

/** Initializes the mem block list at database startup. */
void ut_mem_init();

#define ut_new(n) ut_new_func(n, Source_location(std::source_location::current()))    
#define ut_delete(ptr) ut_delete_func(ptr, Source_location(std::source_location::current()))
#define ut_realloc(ptr, n) ut_realloc_func(ptr, n, Source_location(std::source_location::current()))

/**
 * Allocates memory. Sets it also to zero if UNIV_SET_MEM_TO_ZERO is defined.
 *
 * @param n Number of bytes to allocate
 *
 * @return Allocated memory
 */
void *ut_new_func(ulint n, Source_location location);

/**
 * Frees a memory block allocated with ut_new_func().
 *
 * @param ptr Memory block to free
 */
void ut_delete_func(void *ptr, Source_location location);

/**
 * Implements realloc. This is needed by /pars/lexyy.c. Otherwise, you should
 * not use this function because the allocation functions in mem0mem.h are the
 * recommended ones in InnoDB.
 *
 * man realloc in Linux, 2004:
 *
 *       realloc()  changes the size of the memory block pointed to
 *       by ptr to size bytes.  The contents will be  unchanged  to
 *       the minimum of the old and new sizes; newly allocated mem
 *       ory will be uninitialized.  If ptr is nullptr,  the	 call  is
 *       equivalent  to malloc(size); if size is equal to zero, the
 *       call is equivalent to free(ptr).	 Unless ptr is	nullptr,  it
 *       must  have  been	 returned by an earlier call to malloc(),
 *       calloc() or realloc.
 *
 * RETURN VALUE
 *       realloc() returns a pointer to the newly allocated memory,
 *       which is suitably aligned for any kind of variable and may
 *       be different from ptr, or nullptr if the  request  fails.  If
 *       size  was equal to 0, either nullptr or a pointer suitable to
 *       be passed to free() is returned.	 If realloc()  fails  the
 *       original	 block	is  left  untouched  - it is not freed or
 *       moved.
 *
 * @param ptr Pointer to old block or nullptr
 * @param size Desired size
 *
 * @return Pointer to new mem block or nullptr
 */
void *ut_realloc_func(void *ptr, ulint size, Source_location location);

/** Frees in shutdown all allocated memory not freed yet. */
void ut_delete_all_mem();

/** Reset the variables. */
void ut_mem_var_init();

/** The total amount of memory currently allocated from the operating
system with os_mem_alloc_large() or malloc().  Does not count malloc()
if srv_use_sys_malloc is set.  Protected by ut_list_mutex.
@return the total memory allocated. */
ulint ut_total_allocated_memory();

/** Memory was allocated externally, @see os0proc.cc */
void ut_allocated_memory(ulint size);

/** Memory was deallocated externally, @see os0proc.cc */
void ut_deallocated_memory(ulint size);

/**
 * Copies up to size - 1 characters from the NUL-terminated string src to
 * dst, NUL-terminating the result. Returns strlen(src), so truncation
 * occurred if the return value >= size.
 *
 * @param dst Destination buffer
 * @param src Source buffer
 * @param size Size of destination buffer
 *
 * @return strlen(src)
 */
ulint ut_strlcpy(char *dst, const char *src, ulint size);

/**
 * Like ut_strlcpy, but if src doesn't fit in dst completely, copies the last
 * (size - 1) bytes of src, not the first.
 *
 * @param dst Destination buffer
 * @param src Source buffer
 * @param size Size of destination buffer
 *
 * @return strlen(src)
 */
ulint ut_strlcpy_rev(char *dst, const char *src, ulint size);
