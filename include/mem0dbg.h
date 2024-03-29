/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/** @file include/mem0dbg.h
The memory management: the debug code. This is not a compilation module,
but is included in mem0mem.* !

Created 6/9/1994 Heikki Tuuri
*******************************************************/

#pragma once

/* In the debug version each allocated field is surrounded with
check fields whose sizes are given below */

/* Space needed when allocating for a user a field of
length N. The space is allocated only in multiples of
UNIV_MEM_ALIGNMENT. In the debug version there are also
check fields at the both ends of the field. */
#define MEM_SPACE_NEEDED(N) ut_calc_align((N), UNIV_MEM_ALIGNMENT)

#if defined UNIV_DEBUG

/** Checks a memory heap for consistency and prints the contents if requested.
Outputs the sum of sizes of buffers given to the user (only in
the debug version), the physical size of the heap and the number of
blocks in the heap. In case of error returns 0 as sizes and number
of blocks. */
void mem_heap_validate_or_print(
  mem_heap_t *heap, /*!< in: memory heap */
  byte *top,        /*!< in: calculate and validate only until
                      this top pointer in the heap is reached,
                      if this pointer is NULL, ignored */
  bool print,       /*!< in: if true, prints the contents
                      of the heap; works only in
                      the debug version */
  bool *error,      /*!< out: true if error */
  ulint *us_size,   /*!< out: allocated memory
                      (for the user) in the heap,
                      if a NULL pointer is passed as this
                      argument, it is ignored; in the
                      non-debug version this is always -1 */
  ulint *ph_size,   /*!< out: physical size of the heap,
                      if a NULL pointer is passed as this
                      argument, it is ignored */
  ulint *n_blocks
); /*!< out: number of blocks in the heap,
                   if a NULL pointer is passed as this
                   argument, it is ignored */

/** Validates the contents of a memory heap.
@return	true if ok */
bool mem_heap_validate(mem_heap_t *heap); /*!< in: memory heap */

/** Checks that an object is a memory heap (or a block of it)
@return	true if ok */
bool mem_heap_check(mem_heap_t *heap); /*!< in: memory heap */

/** Validates the dynamic memory
@return	true if ok */
bool mem_validate();

#endif /* UNIV_DEBUG */
