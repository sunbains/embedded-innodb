/**
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

/** @file include/mem0mem.h
The memory management

Created 6/9/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mach0data.h"
#include "sync0sync.h"
#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "ut0rnd.h"

/* -------------------- MEMORY HEAPS ----------------------------- */

/* The info structure stored at the beginning of a heap block */
typedef struct mem_block_info_struct mem_block_info_t;

/* A block of a memory heap consists of the info structure
followed by an area of memory */
typedef mem_block_info_t mem_block_t;

/* A memory heap is a nonempty linear list of memory blocks */
typedef mem_block_t mem_heap_t;

/* Types of allocation for memory heaps: DYNAMIC means allocation from the
dynamic memory pool of the C compiler, BUFFER means allocation from the
buffer pool; the latter method is used for very big heaps */

#define MEM_HEAP_DYNAMIC 0 /* the most common type */
#define MEM_HEAP_BUFFER 1
#define MEM_HEAP_BTR_SEARCH \
  2 /* this flag can optionally be                                             \
    ORed to MEM_HEAP_BUFFER, in which                                          \
    case heap->free_block is used in                                           \
    some cases for memory allocations,                                         \
    and if it's NULL, the memory                                               \
    allocation functions can return                                            \
    NULL. */

/* The following start size is used for the first block in the memory heap if
the size is not specified, i.e., 0 is given as the parameter in the call of
create. The standard size is the maximum (payload) size of the blocks used for
allocations of small buffers. */

#define MEM_BLOCK_START_SIZE 64
#define MEM_BLOCK_STANDARD_SIZE (UNIV_PAGE_SIZE >= 16384 ? 8000 : MEM_MAX_ALLOC_IN_BUF)

/* If a memory heap is allowed to grow into the buffer pool, the following
is the maximum size for a single allocated buffer: */
#define MEM_MAX_ALLOC_IN_BUF (UNIV_PAGE_SIZE - 200)

/** Initializes the memory system. */

void mem_init(ulint size); /*!< in: common pool size in bytes */
/** Closes the memory system. */

void mem_close(void);

/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */

#define mem_heap_create(N) mem_heap_create_func((N), MEM_HEAP_DYNAMIC, __FILE__, __LINE__)
/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */

#define mem_heap_create_in_buffer(N) mem_heap_create_func((N), MEM_HEAP_BUFFER, __FILE__, __LINE__)
/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */

#define mem_heap_create_in_btr_search(N) mem_heap_create_func((N), MEM_HEAP_BTR_SEARCH | MEM_HEAP_BUFFER, __FILE__, __LINE__)

/** Use this macro instead of the corresponding function! Macro for memory
heap freeing. */

#define mem_heap_free(heap) mem_heap_free_func((heap), __FILE__, __LINE__)

/** Use this macro instead of the corresponding function!
Macro for memory buffer allocation */

#define mem_zalloc(N) memset(mem_alloc(N), 0, (N))

#define mem_alloc(N) mem_alloc_func((N), NULL, __FILE__, __LINE__)
#define mem_alloc2(N, S) mem_alloc_func((N), (S), __FILE__, __LINE__)

/** Duplicates a NUL-terminated string, allocated from a memory heap.
@return	own: a copy of the string */
char *mem_heap_strdup(
  mem_heap_t *heap, /*!< in: memory heap where string is allocated */
  const char *str
); /*!< in: string to be copied */

/** Concatenate two strings and return the result, using a memory heap.
@return	own: the result */
char *mem_heap_strcat(
  mem_heap_t *heap, /*!< in: memory heap where string is allocated */
  const char *s1,   /*!< in: string 1 */
  const char *s2
); /*!< in: string 2 */

/** Duplicate a block of data, allocated from a memory heap.
@return	own: a copy of the data */
void *mem_heap_dup(
  mem_heap_t *heap, /*!< in: memory heap where copy is allocated */
  const void *data, /*!< in: data to be copied */
  ulint len
); /*!< in: length of data, in bytes */

/** A simple (s)printf replacement that dynamically allocates the space for the
formatted string from the given heap. This supports a very limited set of
the printf syntax: types 's' and 'u' and length modifier 'l' (which is
required for the 'u' type).
@return	heap-allocated formatted string */
char *mem_heap_printf(
  mem_heap_t *heap,   /*!< in: memory heap */
  const char *format, /*!< in: format string */
  ...
) __attribute__((format(printf, 2, 3)));

#ifdef UNIV_DEBUG
/** Goes through the list of all allocated mem blocks, checks their magic
numbers, and reports possible corruption. */

void mem_heap_verify(const mem_heap_t *heap); /*!< in: heap to verify */
#endif

/*#######################################################################*/

/* The info header of a block in a memory heap */

struct mem_block_info_struct {
  /* magic number for debugging */
  ulint magic_n;

  /* file name where the mem heap was created */
  char file_name[8];

  /*!< line number where the mem heap was created */
  ulint line;

  /** In the first block in the list this is the base
  node of the list of blocks; in subsequent blocks this is undefined */
  UT_LIST_BASE_NODE_T_EXTERN(mem_block_t, list) base;

  /* This contains pointers to next and prev in the list. The first
  block allocated to the heap is also the first block in this list,
  though it also contains the base node of the list. */
  UT_LIST_NODE_T(mem_block_t) list;

  /*!< physical length of this block in bytes */
  ulint len;

  /* physical length in bytes of all blocks in the heap. This is
  defined only in the base node and is set to ULINT_UNDEFINED in others. */
  ulint total_size;

  /*!< type of heap: MEM_HEAP_DYNAMIC, or MEM_HEAP_BUF possibly ORed
  to MEM_HEAP_BTR_SEARCH */
  ulint type;

  /*!< offset in bytes of the first free position for user data in the block */
  ulint free;

  /*!< the value of the struct field 'free' at the creation of the block */
  ulint start;

  /* if the MEM_HEAP_BTR_SEARCH bit is set in type, and this is the heap
  root, this can contain an allocated buffer frame, which can be appended
  as a free block to the heap, if we need more space; otherwise, this is NULL */
  void *free_block;

  /** if this block has been allocated from the buffer pool, this contains
  the buf_block_t handle; otherwise, this is NULL */
  void *buf_block;

#ifdef UNIV_DEBUG
  /* List of all mem blocks allocated; protected by the mem_comm_pool mutex */
  UT_LIST_NODE_T(mem_block_t) mem_block_list;
#endif /* UNIV_DEBUG */
};

UT_LIST_NODE_GETTER_DEFINITION(mem_block_t, list);

#define MEM_BLOCK_MAGIC_N 764741555
#define MEM_FREED_BLOCK_MAGIC_N 547711122

/* Header size for a memory heap block */
#define MEM_BLOCK_HEADER_SIZE ut_calc_align(sizeof(mem_block_info_t), UNIV_MEM_ALIGNMENT)
#include "mem0dbg.h"

/** Creates a memory heap block where data can be allocated.
@return own: memory heap block, NULL if did not succeed (only possible
for MEM_HEAP_BTR_SEARCH type heaps) */
mem_block_t *mem_heap_create_block(
  mem_heap_t *heap,      /*!< in: memory heap or NULL if first
                                        block should be created */
  ulint n,               /*!< in: number of bytes needed for user data */
  ulint type,            /*!< in: type of heap: MEM_HEAP_DYNAMIC or
                                  MEM_HEAP_BUFFER */
  const char *file_name, /*!< in: file name where created */
  ulint line
); /*!< in: line where created */

/** Frees a block from a memory heap. */
void mem_heap_block_free(
  mem_heap_t *heap, /*!< in: heap */
  mem_block_t *block
); /*!< in: block to free */

/** Frees the free_block field from a memory heap. */
void mem_heap_free_block_free(mem_heap_t *heap); /*!< in: heap */

/** Adds a new block to a memory heap.
@return created block, NULL if did not succeed (only possible for
MEM_HEAP_BTR_SEARCH type heaps) */
mem_block_t *mem_heap_add_block(
  mem_heap_t *heap, /*!< in: memory heap */
  ulint n
); /*!< in: number of bytes user needs */

inline void mem_block_set_len(mem_block_t *block, ulint len) {
  ut_ad(len > 0);

  block->len = len;
}

inline ulint mem_block_get_len(mem_block_t *block) {
  return (block->len);
}

inline void mem_block_set_type(mem_block_t *block, ulint type) {
  ut_ad((type == MEM_HEAP_DYNAMIC) || (type == MEM_HEAP_BUFFER) || (type == MEM_HEAP_BUFFER + MEM_HEAP_BTR_SEARCH));

  block->type = type;
}

inline ulint mem_block_get_type(mem_block_t *block) {
  return (block->type);
}

inline void mem_block_set_free(mem_block_t *block, ulint free_block) {
  ut_ad(free_block > 0);
  ut_ad(free_block <= mem_block_get_len(block));

  block->free = free_block;
}

inline ulint mem_block_get_free(mem_block_t *block) {
  return (block->free);
}

inline void mem_block_set_start(mem_block_t *block, ulint start) {
  ut_ad(start > 0);

  block->start = start;
}

inline ulint mem_block_get_start(mem_block_t *block) {
  return (block->start);
}

/** Allocates n bytes of memory from a memory heap.
@return allocated storage, NULL if did not succeed (only possible for
MEM_HEAP_BTR_SEARCH type heaps)
@param[in,out] heap             Memory heap.
@param[in] n                    Number of bytes; if the heap is allowed
                                to grow into the buffer pool, this must be
                                <= MEM_MAX_ALLOC_IN_BUF */
inline byte *mem_heap_alloc(mem_heap_t *heap, ulint n) {
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  ut_ad(!(block->type & MEM_HEAP_BUFFER) || (n <= MEM_MAX_ALLOC_IN_BUF));

  /* Check if there is enough space in block. If not, create a new
  block to the heap */

  if (mem_block_get_len(block) < mem_block_get_free(block) + MEM_SPACE_NEEDED(n)) {

    block = mem_heap_add_block(heap, n);

    if (block == nullptr) {
      return nullptr;
    }
  }

  auto free_sz = mem_block_get_free(block);

  byte *ptr = (byte *)block + free_sz;

  mem_block_set_free(block, free_sz + MEM_SPACE_NEEDED(n));

#ifdef UNIV_SET_MEM_TO_ZERO
  UNIV_MEM_ALLOC(ptr, n);
  memset(ptr, '\0', n);
#endif /* UNIV_SET_MEM_TO_ZERO */

  UNIV_MEM_ALLOC(ptr, n);

  return ptr;
}

/** Allocates and zero-fills n bytes of memory from a memory heap.
@param[in,out] heap             Memory heap.
@param[in] n                    Number of bytes; if the heap is allowed
                                to grow into the buffer pool, this must be
                                <= MEM_MAX_ALLOC_IN_BUF
@return	allocated, zero-filled storage */
inline byte *mem_heap_zalloc(mem_heap_t *heap, ulint n) {
  ut_ad(heap != nullptr);
  ut_ad(!(heap->type & MEM_HEAP_BTR_SEARCH));

  return (byte *)memset(mem_heap_alloc(heap, n), 0, n);
}

/** Frees the space in a memory heap exceeding the pointer given. The
pointer must have been acquired from mem_heap_get_heap_top. The first
memory block of the heap is not freed. */
inline void mem_heap_free_heap_top(
  mem_heap_t *heap, /*!< in: heap from which to free */
  byte *old_top
) /*!< in: pointer to old top of heap */
{
  mem_block_t *prev_block;
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  while (block != NULL) {
    if (((byte *)block + mem_block_get_free(block) >= old_top) && ((byte *)block <= old_top)) {
      /* Found the right block */

      break;
    }

    /* Store prev_block value before freeing the current block
    (the current block will be erased in freeing) */

    prev_block = UT_LIST_GET_PREV(list, block);

    mem_heap_block_free(heap, block);

    block = prev_block;
  }

  ut_ad(block);

  /* Set the free field of block */
  mem_block_set_free(block, old_top - (byte *)block);

  UNIV_MEM_ASSERT_W(old_top, (byte *)block + block->len - old_top);
  UNIV_MEM_ALLOC(old_top, (byte *)block + block->len - old_top);

  /* If free == start, we may free the block if it is not the first
  one */

  if ((heap != block) && (mem_block_get_free(block) == mem_block_get_start(block))) {
    mem_heap_block_free(heap, block);
  }
}

/** Empties a memory heap. The first memory block of the heap is not freed. */
inline void mem_heap_empty(mem_heap_t *heap) /*!< in: heap to empty */
{
  mem_heap_free_heap_top(heap, (byte *)heap + mem_block_get_start(heap));
  if (heap->free_block) {
    mem_heap_free_block_free(heap);
  }
}

/** Returns a pointer to the topmost element in a memory heap. The size of the
element must be given.
@return	pointer to the topmost element */
inline void *mem_heap_get_top(
  mem_heap_t *heap, /*!< in: memory heap */
  ulint n
) /*!< in: size of the topmost element */
{

  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);
  auto buf = (byte *)block + mem_block_get_free(block) - MEM_SPACE_NEEDED(n);

  return buf;
}

/** Frees the topmost element in a memory heap. The size of the element must be
given. */
inline void mem_heap_free_top(
  mem_heap_t *heap, /*!< in: memory heap */
  ulint n
) /*!< in: size of the topmost element */
{
  mem_block_t *block;

  ut_ad(mem_heap_check(heap));

  block = UT_LIST_GET_LAST(heap->base);

  /* Subtract the free field of block */
  mem_block_set_free(block, mem_block_get_free(block) - MEM_SPACE_NEEDED(n));
  UNIV_MEM_ASSERT_W((byte *)block + mem_block_get_free(block), n);

  /* If free == start, we may free the block if it is not the first one */

  if ((heap != block) && (mem_block_get_free(block) == mem_block_get_start(block))) {
    mem_heap_block_free(heap, block);
  } else {
    /* Avoid a bogus UNIV_MEM_ASSERT_W() warning in a
    subsequent invocation of mem_heap_free_top().
    Originally, this was UNIV_MEM_FREE(), to catch writes
    to freed memory. */
    UNIV_MEM_ALLOC((byte *)block + mem_block_get_free(block), n);
  }
}

/** NOTE: Use the corresponding macros instead of this function. Creates a
memory heap. For debugging purposes, takes also the file name and line as
argument.
@return own: memory heap, NULL if did not succeed (only possible for
MEM_HEAP_BTR_SEARCH type heaps) */
inline mem_heap_t *mem_heap_create_func(
  ulint n,               /*!< in: desired start block size,
                                            this means that a single user buffer
                                            of size n will fit in the block,
                                            0 creates a default size block */
  ulint type,            /*!< in: heap type */
  const char *file_name, /*!< in: file name where created */
  ulint line
) /*!< in: line where created */
{
  mem_block_t *block;

  if (!n) {
    n = MEM_BLOCK_START_SIZE;
  }

  block = mem_heap_create_block(NULL, n, type, file_name, line);

  if (block == NULL) {

    return (NULL);
  }

  UT_LIST_INIT(block->base);

  /* Add the created block itself as the first block in the list */
  UT_LIST_ADD_FIRST(block->base, block);

  return block;
}

/** NOTE: Use the corresponding macro instead of this function. Frees the space
occupied by a memory heap. In the debug version erases the heap memory
blocks. */
inline void mem_heap_free_func(
  mem_heap_t *heap, /*!< in, own: heap to be freed */
  const char *file_name __attribute__((unused)),
  /*!< in: file name where freed */
  ulint line __attribute__((unused))
) {
  mem_block_t *block;
  mem_block_t *prev_block;

  ut_ad(mem_heap_check(heap));

  block = UT_LIST_GET_LAST(heap->base);

  if (heap->free_block) {
    mem_heap_free_block_free(heap);
  }

  while (block != NULL) {
    /* Store the contents of info before freeing current block
    (it is erased in freeing) */

    prev_block = UT_LIST_GET_PREV(list, block);

    mem_heap_block_free(heap, block);

    block = prev_block;
  }
}

/** NOTE: Use the corresponding macro instead of this function.
Allocates a single buffer of memory from the dynamic memory of
the C compiler. Is like malloc of C. The buffer must be freed
with mem_free.
@return	own: free storage */
inline void *mem_alloc_func(
  ulint n,               /*!< in: desired number of bytes */
  ulint *size,           /*!< out: allocated size in bytes,
                                      or NULL */
  const char *file_name, /*!< in: file name where created */
  ulint line
) /*!< in: line where created */
{
  mem_heap_t *heap;
  void *buf;

  heap = mem_heap_create_func(n, MEM_HEAP_DYNAMIC, file_name, line);

  /* Note that as we created the first block in the heap big enough
  for the buffer requested by the caller, the buffer will be in the
  first block and thus we can calculate the pointer to the heap from
  the pointer to the buffer when we free the memory buffer. */

  if (likely_null(size)) {
    /* Adjust the allocation to the actual size of the
    memory block. */
    ulint m = mem_block_get_len(heap) - mem_block_get_free(heap);
    ut_ad(m >= n);
    *size = n = m;
  }

  buf = mem_heap_alloc(heap, n);

  ut_a((byte *)heap == (byte *)buf - MEM_BLOCK_HEADER_SIZE);
  return buf;
}

/** NOTE: Use the corresponding macro instead of this function. Frees a single
buffer of storage from the dynamic memory of the C compiler. Similar to the
free of C. */
inline void mem_free_func(
  void *ptr,             /*!< in, own: buffer to be freed */
  const char *file_name, /*!< in: file name where created */
  ulint line
) /*!< in: line where created */
{
  auto heap = (mem_heap_t *)((byte *)ptr - MEM_BLOCK_HEADER_SIZE);

  mem_heap_free_func(heap, file_name, line);
}

/** Returns the space in bytes occupied by a memory heap. */
inline ulint mem_heap_get_size(mem_heap_t *heap) /*!< in: heap */
{
  ulint size = 0;

  ut_ad(mem_heap_check(heap));

  size = heap->total_size;

  if (heap->free_block) {
    size += UNIV_PAGE_SIZE;
  }

  return (size);
}

/** Duplicates a NUL-terminated string.
@return	own: a copy of the string, must be deallocated with mem_free */
inline char *mem_strdup(const char *str) /*!< in: string to be copied */
{
  ulint len = strlen(str) + 1;
  return ((char *)memcpy(mem_alloc(len), str, len));
}

/** Makes a NUL-terminated copy of a nonterminated string.
@return	own: a copy of the string, must be deallocated with mem_free */
inline char *mem_strdupl(
  const char *str, /*!< in: string to be copied */
  ulint len
) /*!< in: length of str, in bytes */
{
  char *s = (char *)mem_alloc(len + 1);
  s[len] = 0;
  return ((char *)memcpy(s, str, len));
}

/** Makes a NUL-terminated copy of a nonterminated string,
allocated from a memory heap.
@return	own: a copy of the string */
inline char *mem_heap_strdupl(
  mem_heap_t *heap, /*!< in: memory heap where string is allocated */
  const char *str,  /*!< in: string to be copied */
  ulint len
) /*!< in: length of str, in bytes */
{
  char *s = (char *)mem_heap_alloc(heap, len + 1);
  s[len] = 0;
  return ((char *)memcpy(s, str, len));
}

/** Use this macro instead of the corresponding function!
Macro for memory buffer freeing */

#define mem_free(PTR) mem_free_func((PTR), __FILE__, __LINE__)
