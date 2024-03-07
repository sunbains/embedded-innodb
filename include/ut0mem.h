/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

#ifndef ut0mem_h
#define ut0mem_h

#include "innodb0types.h"

#include <string.h>
#include "os0sync.h"

/** The total amount of memory currently allocated from the operating
system with os_mem_alloc_large() or malloc().  Does not count malloc()
if srv_use_sys_malloc is set.  Protected by ut_list_mutex. */
extern ulint ut_total_allocated_memory;

/** Mutex protecting ut_total_allocated_memory and ut_mem_block_list */
extern os_fast_mutex_t ut_list_mutex;

/** Initializes the mem block list at database startup. */

void ut_mem_init(void);

/** Allocates memory. Sets it also to zero if UNIV_SET_MEM_TO_ZERO is
defined and set_to_zero is true.
@return	own: allocated memory */

void *ut_malloc_low(
  ulint n,          /*!< in: number of bytes to allocate */
  bool set_to_zero, /*!< in: true if allocated memory
                                       should be set to zero if
                                       UNIV_SET_MEM_TO_ZERO is defined */
  bool assert_on_error
); /*!< in: if true, we crash the
                                            engine if the memory cannot be
                                            allocated */
/** Allocates memory. Sets it also to zero if UNIV_SET_MEM_TO_ZERO is
defined.
@return	own: allocated memory */

void *ut_malloc(ulint n); /*!< in: number of bytes to allocate */
/** Tests if malloc of n bytes would succeed. ut_malloc() asserts if memory runs
out. It cannot be used if we want to return an error message. Prints to
stderr a message if fails.
@return	true if succeeded */

bool ut_test_malloc(ulint n); /*!< in: try to allocate this many bytes */
/** Frees a memory block allocated with ut_malloc. */

void ut_free(void *ptr); /*!< in, own: memory block */
/** Implements realloc. This is needed by /pars/lexyy.c. Otherwise, you should
not use this function because the allocation functions in mem0mem.h are the
recommended ones in InnoDB.

man realloc in Linux, 2004:

       realloc()  changes the size of the memory block pointed to
       by ptr to size bytes.  The contents will be  unchanged  to
       the minimum of the old and new sizes; newly allocated mem­
       ory will be uninitialized.  If ptr is NULL,  the	 call  is
       equivalent  to malloc(size); if size is equal to zero, the
       call is equivalent to free(ptr).	 Unless ptr is	NULL,  it
       must  have  been	 returned by an earlier call to malloc(),
       calloc() or realloc().

RETURN VALUE
       realloc() returns a pointer to the newly allocated memory,
       which is suitably aligned for any kind of variable and may
       be different from ptr, or NULL if the  request  fails.  If
       size  was equal to 0, either NULL or a pointer suitable to
       be passed to free() is returned.	 If realloc()  fails  the
       original	 block	is  left  untouched  - it is not freed or
       moved.
@return	own: pointer to new mem block or NULL */

void *ut_realloc(
  void *ptr, /*!< in: pointer to old block or NULL */
  ulint size
); /*!< in: desired size */
/** Frees in shutdown all allocated memory not freed yet. */

void ut_free_all_mem(void);

/** Copies up to size - 1 characters from the NUL-terminated string src to
dst, NUL-terminating the result. Returns strlen(src), so truncation
occurred if the return value >= size.
@return	strlen(src) */

ulint ut_strlcpy(
  char *dst,       /*!< in: destination buffer */
  const char *src, /*!< in: source buffer */
  ulint size
); /*!< in: size of destination buffer */

/** Like ut_strlcpy, but if src doesn't fit in dst completely, copies the last
(size - 1) bytes of src, not the first.
@return	strlen(src) */

ulint ut_strlcpy_rev(
  char *dst,       /*!< in: destination buffer */
  const char *src, /*!< in: source buffer */
  ulint size
); /*!< in: size of destination buffer */

/** Compute strlen(strcpyq(str, q)).
@return	length of the string when quoted */
inline ulint strlenq(
  const char *str, /*!< in: null-terminated string */
  char q
); /*!< in: the quote character */

/** Make a quoted copy of a NUL-terminated string.	Leading and trailing
quotes will not be included; only embedded quotes will be escaped.
See also strlenq() and memcpyq().
@return	pointer to end of dest */

char *strcpyq(
  char *dest, /*!< in: output buffer */
  char q,     /*!< in: the quote character */
  const char *src
); /*!< in: null-terminated string */

/** Make a quoted copy of a fixed-length string.  Leading and trailing
quotes will not be included; only embedded quotes will be escaped.
See also strlenq() and strcpyq().
@return	pointer to end of dest */

char *memcpyq(
  char *dest,      /*!< in: output buffer */
  char q,          /*!< in: the quote character */
  const char *src, /*!< in: string to be quoted */
  ulint len
); /*!< in: length of src */

/** Return the number of times s2 occurs in s1. Overlapping instances of s2
are only counted once.
@return	the number of times s2 occurs in s1 */

ulint ut_strcount(
  const char *s1, /*!< in: string to search in */
  const char *s2
); /*!< in: string to search for */

/** Replace every occurrence of s1 in str with s2. Overlapping instances of s1
are only replaced once.
@return	own: modified string, must be freed with mem_free() */

char *ut_strreplace(
  const char *str, /*!< in: string to operate on */
  const char *s1,  /*!< in: string to replace */
  const char *s2
); /*!< in: string to replace s1 with */

/** Converts a raw binary data to a NUL-terminated hex string. The output is
truncated if there is not enough space in "hex", make sure "hex_size" is at
least (2 * raw_size + 1) if you do not want this to happen. Returns the
actual number of characters written to "hex" (including the NUL).
@return	number of chars written */
inline ulint ut_raw_to_hex(
  const void *raw, /*!< in: raw data */
  ulint raw_size,  /*!< in: "raw" length in bytes */
  char *hex,       /*!< out: hex string */
  ulint hex_size
); /*!< in: "hex" size in bytes */

/** Adds single quotes to the start and end of string and escapes any quotes
by doubling them. Returns the number of bytes that were written to "buf"
(including the terminating NUL). If buf_size is too small then the
trailing bytes from "str" are discarded.
@return	number of bytes that were written */
inline ulint ut_str_sql_format(
  const char *str, /*!< in: string */
  ulint str_len,   /*!< in: string length in bytes */
  char *buf,       /*!< out: output buffer */
  ulint buf_size
); /*!< in: output buffer size
                                                in bytes */
/** Reset the variables. */

void ut_mem_var_init(void);

#ifndef UNIV_NONINL
#include "ut0mem.ic"
#endif

#endif
