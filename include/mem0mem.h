/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/mem0mem.h
The memory management

Created 6/9/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0dbg.h"
#include "mem0types.h"
#include "ut0lst.h"
#include "ut0mem.h"
// #include "ut0rnd.h"

/** Initializes the memory system. */
void mem_init(ulint size); /*!< in: common pool size in bytes */

/** Closes the memory system. */
void mem_close();

/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */
#define mem_heap_create(N) mem_heap_create_func((N), MEM_HEAP_DYNAMIC, Current_location())

/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */
#define mem_heap_create_in_buffer(N) mem_heap_create_func((N), MEM_HEAP_BUFFER, Current_location())

/** Use this macro instead of the corresponding function! Macro for memory
heap creation. */
#define mem_heap_create_in_btr_search(N) mem_heap_create_func((N), MEM_HEAP_BTR_SEARCH | MEM_HEAP_BUFFER, Current_location())

/** Use this macro instead of the corresponding function! Macro for memory
heap freeing. */
#define mem_heap_free(heap) mem_heap_free_func((heap), Current_location())

/** Use this macro instead of the corresponding function!
Macro for memory buffer allocation */

#define mem_zalloc(N) memset(mem_alloc(N), 0, (N))

#define mem_alloc(N) mem_alloc_func((N), NULL, Current_location())

#define mem_alloc2(N, S) mem_alloc_func((N), (S), Current_location())

/** Duplicates a NUL-terminated string, allocated from a memory heap.
 * @param heap memory heap where string is allocated
 * @param str string to be copied
 * @return own: a copy of the string */
[[nodiscard]] char *mem_heap_strdup(mem_heap_t *heap, const char *str);

/** Concatenate two strings and return the result, using a memory heap.
 * @param heap memory heap where string is allocated
 * @param s1 string 1
 * @param s2 string 2
 * @return own: the result */
[[nodiscard]] char *mem_heap_strcat(mem_heap_t *heap, const char *s1, const char *s2);

/** Duplicate a block of data, allocated from a memory heap.
 * @param heap memory heap where copy is allocated
 * @param data data to be copied
 * @param len length of data, in bytes
 * @return own: a copy of the data */
[[nodiscard]] void *mem_heap_dup(mem_heap_t *heap, const void *data, ulint len);

/** A simple (s)printf replacement that dynamically allocates the space for the
 * formatted string from the given heap. This supports a very limited set of
 * the printf syntax: types 's' and 'u' and length modifier 'l' (which is
 * required for the 'u' type).
 * @param heap memory heap
 * @param format format string
 * @return heap-allocated formatted string */
[[nodiscard]] char *mem_heap_printf(mem_heap_t *heap, const char *format, ...)
  __attribute__((format(printf, 2, 3)));

/**
 * Goes through the list of all allocated mem blocks, checks their magic
 * numbers, and reports possible corruption.
 *
 * @param heap heap to verify
 */
IF_DEBUG(void mem_heap_verify(const mem_heap_t *heap);)

/* The info header of a block in a memory heap */
struct mem_block_info_t {
  /** Magic number for debugging */
  ulint magic_n;

  /** Location where the heap was created */
  Source_location m_loc;

  /** In the first block in the list this is the base
  node of the list of blocks; in subsequent blocks this is undefined */
  UT_LIST_BASE_NODE_T_EXTERN(mem_block_t, list) base;

  /** This contains pointers to next and prev in the list. The first
  block allocated to the heap is also the first block in this list,
  though it also contains the base node of the list. */
  UT_LIST_NODE_T(mem_block_t) list;

  /** physical length of this block in bytes */
  ulint len;

  /** Physical length in bytes of all blocks in the heap. This is
  defined only in the base node and is set to ULINT_UNDEFINED in others. */
  ulint total_size;

  /** type of heap: MEM_HEAP_DYNAMIC, or MEM_HEAP_BUF possibly ORed
  to MEM_HEAP_BTR_SEARCH */
  ulint type;

  /** Offset in bytes of the first free position for user data in the block */
  ulint free;

  /** the value of the struct field 'free' at the creation of the block */
  ulint start;

  /** If the MEM_HEAP_BTR_SEARCH bit is set in type, and this is the heap
  root, this can contain an allocated buffer frame, which can be appended
  as a free block to the heap, if we need more space; otherwise, this is NULL */
  void *free_block;

  /** if this block has been allocated from the buffer pool, this contains
  the Buf_block handle; otherwise, this is NULL */
  void *buf_block;

  /* List of all mem blocks allocated; protected by the mem_comm_pool mutex */
  IF_DEBUG(UT_LIST_NODE_T(mem_block_t) mem_block_list;)
};

UT_LIST_NODE_GETTER_DEFINITION(mem_block_t, list);

constexpr ulint MEM_BLOCK_MAGIC_N = 764741555;
constexpr ulint MEM_FREED_BLOCK_MAGIC_N = 547711122;

/**
 * @brief Calculates the header size for a memory heap block.
 * 
 * @return The header size.
 */
[[nodiscard]] inline ulint mem_block_header_size() {
  return ut_calc_align(sizeof(mem_block_info_t), UNIV_MEM_ALIGNMENT);
}

/**
 * Creates a memory heap block where data can be allocated.
 *
 * @param[in] heap memory heap or NULL if first block should be created
 * @param[in] n number of bytes needed for user data
 * @param[in] type type of heap: MEM_HEAP_DYNAMIC or MEM_HEAP_BUFFER
 * @param[in] loc location where created
 * 
 * @return own: memory heap block, NULL if did not succeed (only possible for MEM_HEAP_BTR_SEARCH type heaps)
 */
[[nodiscard]] mem_block_t *mem_heap_create_block(mem_heap_t *heap, ulint n, ulint type, Source_location loc);

/**
 * Frees a block from a memory heap.
 *
 * @param[in] heap heap
 * @param[in] block block to free
 */
void mem_heap_block_free(mem_heap_t *heap, mem_block_t *block);

/**
 * Frees the free_block field from a memory heap.
 *
 * @param[in] heap heap
 */
void mem_heap_free_block_free(mem_heap_t *heap);

/**
 * Adds a new block to a memory heap.
 *
 * @param[in] heap memory heap
 * @param[in] n number of bytes user needs
 * 
 * @return created block, NULL if did not succeed (only possible for MEM_HEAP_BTR_SEARCH type heaps)
 */
[[nodiscard]] mem_block_t *mem_heap_add_block(mem_heap_t *heap, ulint n);

inline void mem_block_set_len(mem_block_t *block, ulint len) {
  ut_ad(len > 0);

  block->len = len;
}

[[nodiscard]] inline ulint mem_block_get_len(mem_block_t *block) {
  return block->len;
}

inline void mem_block_set_type(mem_block_t *block, ulint type) {
  ut_ad((type == MEM_HEAP_DYNAMIC) || (type == MEM_HEAP_BUFFER) || (type == MEM_HEAP_BUFFER + MEM_HEAP_BTR_SEARCH));

  block->type = type;
}

[[nodiscard]] inline ulint mem_block_get_type(mem_block_t *block) {
  return block->type;
}

inline void mem_block_set_free(mem_block_t *block, ulint free_block) {
  ut_ad(free_block > 0);
  ut_ad(free_block <= mem_block_get_len(block));

  block->free = free_block;
}

[[nodiscard]] inline ulint mem_block_get_free(mem_block_t *block) {
  return (block->free);
}

inline void mem_block_set_start(mem_block_t *block, ulint start) {
  ut_ad(start > 0);

  block->start = start;
}

[[nodiscard]] inline ulint mem_block_get_start(mem_block_t *block) {
  return block->start;
}

/**
 * Allocates n bytes of memory from a memory heap.
 * 
 * @param[in,out] heap          Memory heap.
 * @param[in] n                 Number of bytes; if the heap is allowed
 *                              to grow into the buffer pool, this must be
 *                              <= MEM_MAX_ALLOC_IN_BUF
 * 
 * @return allocated storage, NULL if did not succeed (only possible for MEM_HEAP_BTR_SEARCH type heaps)
 */
[[nodiscard]] inline byte *mem_heap_alloc(mem_heap_t *heap, ulint n) {
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  ut_ad(!(block->type & MEM_HEAP_BUFFER) || n <= MEM_MAX_ALLOC_IN_BUF);

  /* Check if there is enough space in block. If not, create a new block to the heap */

  if (mem_block_get_len(block) < mem_block_get_free(block) + MEM_SPACE_NEEDED(n)) {

    block = mem_heap_add_block(heap, n);

    if (block == nullptr) {
      return nullptr;
    }
  }

  auto free_sz = mem_block_get_free(block);
  auto ptr = reinterpret_cast<byte *>(block) + free_sz;

  mem_block_set_free(block, free_sz + MEM_SPACE_NEEDED(n));

#ifdef UNIV_SET_MEM_TO_ZERO
  UNIV_MEM_ALLOC(ptr, n);
  memset(ptr, '\0', n);
#endif /* UNIV_SET_MEM_TO_ZERO */

  UNIV_MEM_ALLOC(ptr, n);

  return ptr;
}

/**
 * Allocates and zero-fills n bytes of memory from a memory heap.
 * 
 * @param[in,out] heap Memory heap.
 * @param[in] n Number of bytes; if the heap is allowed to grow into the buffer pool, this must be <= MEM_MAX_ALLOC_IN_BUF
 * 
 * @return	allocated, zero-filled storage
*/
[[nodiscard]] inline byte *mem_heap_zalloc(mem_heap_t *heap, ulint n) {
  ut_ad(heap != nullptr);
  ut_ad(!(heap->type & MEM_HEAP_BTR_SEARCH));

  return reinterpret_cast<byte *>(memset(mem_heap_alloc(heap, n), 0, n));
}

/**
 * @brief Frees the space in a memory heap exceeding the pointer given.
 * 
 * The pointer must have been acquired from mem_heap_get_heap_top.
 * The first memory block of the heap is not freed.
 * 
 * @param[in] heap The heap from which to free.
 * @param[in] old_top Pointer to the old top of the heap.
 */
inline void mem_heap_free_heap_top(mem_heap_t *heap, byte *old_top) {
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  while (block != nullptr) {
    if (reinterpret_cast<byte *>(block) + mem_block_get_free(block) >= old_top &&
         reinterpret_cast<byte *>(block) <= old_top) {
      /* Found the right block */

      break;
    }

    /* Store prev_block value before freeing the current block
    (the current block will be erased in freeing) */

    auto prev_block = UT_LIST_GET_PREV(list, block);

    mem_heap_block_free(heap, block);

    block = prev_block;
  }

  ut_ad(block);

  /* Set the free field of block */
  mem_block_set_free(block, old_top - reinterpret_cast<byte *>(block));

  UNIV_MEM_ASSERT_W(old_top, reinterpret_cast<byte *>(block) + block->len - old_top);
  UNIV_MEM_ALLOC(old_top, reinterpret_cast<byte *>(block) + block->len - old_top);

  /* If free == start, we may free the block if it is not the first
  one */

  if ((heap != block) && (mem_block_get_free(block) == mem_block_get_start(block))) {
    mem_heap_block_free(heap, block);
  }
}

/**
 * Empties a memory heap. The first memory block of the heap is not freed.
 */
inline void mem_heap_empty(mem_heap_t *heap) {
  mem_heap_free_heap_top(heap, reinterpret_cast<byte *>(heap) + mem_block_get_start(heap));

  if (heap->free_block) {
    mem_heap_free_block_free(heap);
  }
}

/**
 * @brief Returns a pointer to the topmost element in a memory heap. The size of the
 * element must be given.
 * 
 * @param[in] heap Memory heap.
 * @param[in] n Size of the topmost element.
 * 
 * @return Pointer to the topmost element.
 */
[[nodiscard]] inline void *mem_heap_get_top(mem_heap_t *heap, ulint n) {
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);
  auto buf = reinterpret_cast<byte *>(block) + mem_block_get_free(block) - MEM_SPACE_NEEDED(n);

  return buf;
}

/**
 * Frees the topmost element in a memory heap. The size of the element must be given.
 * 
 * @param[in] heap Memory heap.
 * @param[in] n Size of the topmost element.
*/
inline void mem_heap_free_top( mem_heap_t *heap, ulint n)
{
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  /* Subtract the free field of block */
  mem_block_set_free(block, mem_block_get_free(block) - MEM_SPACE_NEEDED(n));
  UNIV_MEM_ASSERT_W(reinterpret_cast<byte *>(block) + mem_block_get_free(block), n);

  /* If free == start, we may free the block if it is not the first one */

  if ((heap != block) && (mem_block_get_free(block) == mem_block_get_start(block))) {
    mem_heap_block_free(heap, block);
  } else {
    /* Avoid a bogus UNIV_MEM_ASSERT_W() warning in a
    subsequent invocation of mem_heap_free_top().
    Originally, this was UNIV_MEM_FREE(), to catch writes
    to freed memory. */
    UNIV_MEM_ALLOC(reinterpret_cast<byte *>(block) + mem_block_get_free(block), n);
  }
}

/**
 * NOTE: Use the corresponding macros instead of this function.
 * @brief Creates a memory heap.
 * 
 * For debugging purposes, takes also the file name and line as argument.
 * 
 * @param[in] n desired start block size, this means that a single user buffer of size
 *  n will fit in the block, 0 creates a default size block
 * @param[in] type heap type
 * @param[in] loc file name and line where created
 * 
 * @return own: memory heap, NULL if did not succeed (only possible for MEM_HEAP_BTR_SEARCH type heaps)
 */
[[nodiscard]] inline mem_heap_t *mem_heap_create_func(ulint n, ulint type, Source_location &&loc) {
  if (n == 0) {
    n = MEM_BLOCK_START_SIZE;
  }

  auto block = mem_heap_create_block(nullptr, n, type, loc);

  if (block == nullptr) {
    return nullptr;
  }

  UT_LIST_INIT(block->base);

  /* Add the created block itself as the first block in the list */
  UT_LIST_ADD_FIRST(block->base, block);

  return block;
}

/** NOTE: Use the corresponding macro instead of this function. Frees the space
occupied by a memory heap. In the debug version erases the heap memory
blocks. */
inline void mem_heap_free_func(mem_heap_t *heap, Source_location loc) {
  ut_ad(mem_heap_check(heap));

  auto block = UT_LIST_GET_LAST(heap->base);

  if (heap->free_block) {
    mem_heap_free_block_free(heap);
  }

  while (block != nullptr) {
    auto prev_block = UT_LIST_GET_PREV(list, block);

    mem_heap_block_free(heap, block);
    block = prev_block;
  }
}

/**
 * NOTE: Use the corresponding macro instead of this function.
 * Allocates a single buffer of memory from the dynamic memory of the compiler.
 * Is like malloc of C. The buffer must be freed with mem_free.
 * 
 * @param[in] n                 Desired number of bytes
 * @param[out] size             Allocated size in bytes, or nullptr
 * @param[in] loc               File name and line where created
 * 
 * @return	own: free storage
 */
[[nodiscard]] inline void *mem_alloc_func(ulint n, ulint *size, Source_location &&loc) {
  auto heap = mem_heap_create_func(n, MEM_HEAP_DYNAMIC, std::move(loc));

  /* Note that as we created the first block in the heap big enough
  for the buffer requested by the caller, the buffer will be in the
  first block and thus we can calculate the pointer to the heap from
  the pointer to the buffer when we free the memory buffer. */

  if (likely_null(size)) {
    /* Adjust the allocation to the actual size of the memory block. */
    ulint m = mem_block_get_len(heap) - mem_block_get_free(heap);
    ut_ad(m >= n);
    *size = n = m;
  }

  auto buf = mem_heap_alloc(heap, n);

  ut_a(reinterpret_cast<byte *>(heap) == reinterpret_cast<byte *>(buf - mem_block_header_size()));

  return buf;
}

/**
 * @brief NOTE: Use the corresponding macro instead of this function. Frees a single
 * buffer of storage from the dynamic memory of the C compiler. Similar to the
 * free of C.
 * 
 * @param[in] ptr buffer to be freed
 * @param[in] loc file name and line where created
 */
inline void mem_free_func(void *ptr, Source_location loc) {
  auto heap = reinterpret_cast<mem_heap_t *>(reinterpret_cast<byte *>(ptr) - mem_block_header_size());

  mem_heap_free_func(heap, loc);
}

/**
 * Returns the space in bytes occupied by a memory heap.
 */
[[nodiscard]] inline ulint mem_heap_get_size(mem_heap_t *heap) {
  ut_ad(mem_heap_check(heap));

  auto size = heap->total_size;

  if (heap->free_block) {
    size += UNIV_PAGE_SIZE;
  }

  return size;
}

/**
 * Duplicates a NUL-terminated string.
 * 
 * @param[in] str string to be copied
 * 
 * @return own: a copy of the string, must be deallocated with mem_free
 */
[[nodiscard]] inline char *mem_strdup(const char *str) {
  auto len = strlen(str) + 1;
  return reinterpret_cast<char *>(memcpy(mem_alloc(len), str, len));
}

/**
 * @brief Makes a NUL-terminated copy of a nonterminated string.
 * 
 * @param[in] str string to be copied
 * @param[in] len length of str, in bytes
 * 
 * @return own: a copy of the string, must be deallocated with mem_free
 */
[[nodiscard]] inline char *mem_strdupl(const char *str, ulint len) {
  auto s = reinterpret_cast<char *>(mem_alloc(len + 1));
  s[len] = 0;
  return reinterpret_cast<char *>(memcpy(s, str, len));
}

/**
 * @brief Makes a NUL-terminated copy of a nonterminated string, allocated from a memory heap.
 * 
 * @param[in] heap memory heap where string is allocated
 * @param[in] str string to be copied
 * @param[in] len length of str, in bytes
 * 
 * @return own: a copy of the string
 */
[[nodiscard]] inline char *mem_heap_strdupl(mem_heap_t *heap, const char *str, ulint len) {
  auto s = reinterpret_cast<char *>(mem_heap_alloc(heap, len + 1));
  s[len] = 0;
  return reinterpret_cast<char *>(memcpy(s, str, len));
}

/**
 * Use this macro instead of the corresponding function!
 * Macro for memory buffer freeing
 */
#define mem_free(PTR) mem_free_func((PTR), Current_location())
