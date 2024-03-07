/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/dyn0dyn.h
The dynamically allocated array

Created 2/5/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0mem.h"
#include "ut0lst.h"

/** This is the initial 'payload' size of a dynamic array;
this must be > MLOG_BUF_MARGIN + 30! */
constexpr ulint DYN_ARRAY_DATA_SIZE = 512;

/** Value of dyn_block_struct::magic_n */
constexpr ulint DYN_BLOCK_MAGIC_N = 375767;

/** Flag for dyn_block_struct::used that indicates a full block */
constexpr ulint DYN_BLOCK_FULL_FLAG = 0x1000000UL;

/** @brief A block in a dynamically allocated array.
NOTE! Do not access the fields of the struct directly: the definition
appears here only for the compiler to know its size! */
struct dyn_block_t {
  /** in the first block this is != nullptr if dynamic
  allocation has been needed */
  mem_heap_t *heap;

  /** Number of data bytes used in this block; DYN_BLOCK_FULL_FLAG
  is set when the block becomes full */
  ulint used;

  /** Storage for array elements */
  byte data[DYN_ARRAY_DATA_SIZE];

  /** Linear list of dyn blocks: this node is used only in the first block */
  UT_LIST_BASE_NODE_T(dyn_block_t) base;

  /** linear list node: used in all blocks */
  UT_LIST_NODE_T(dyn_block_t) list;

#ifdef UNIV_DEBUG
  /** Only in the debug version: if dyn array is opened, this
  is the buffer end offset, else this is 0 */
  ulint buf_end;

  /** magic number (DYN_BLOCK_MAGIC_N) */
  ulint magic_n;
#endif /* UNIV_DEBUG */
};

using dyn_array_t = dyn_block_t;

/** Adds a new block to a dyn array.
@param[in,out] arr              Dynamic array.
@return	created block */
dyn_block_t *dyn_array_add_block(dyn_array_t *arr);

/** Gets the first block in a dyn array.
@param[in,out] arr              Dynamic array.
@return first block in the instance. */
inline dyn_block_t * dyn_array_get_first_block(dyn_array_t *arr) {
  return arr;
}

/** Gets the last block in a dyn array. 
@param[in,out] arr              Dynamic array.
@return last block in the instance. */
inline dyn_block_t *dyn_array_get_last_block(dyn_array_t *arr) {
  if (arr->heap == nullptr) {

    return arr;
  } else {
    return UT_LIST_GET_LAST(arr->base);
  }
}

/** Gets the next block in a dyn array.
@param[in,out] arr             Dynamic array
@param[in,out] block           Block in the dynamic array
@return	pointer to next, nullptr if end of list */
inline dyn_block_t * dyn_array_get_next_block(dyn_array_t *arr, dyn_block_t *block) {
  if (arr->heap == nullptr) {

    ut_ad(arr == block);
    return nullptr;

  } else {
    return UT_LIST_GET_NEXT(list, block);
  }
}

/** Gets the number of used bytes in a dyn array block.
@param[in,out] block           Block to check
@return	number of bytes used */
inline ulint dyn_block_get_used(dyn_block_t *block) {
  return block->used & ~DYN_BLOCK_FULL_FLAG;
}

/** Gets pointer to the start of data in a dyn array block.
@param[in,out] block           Block to get the data from.
@return	pointer to data */
inline byte *dyn_block_get_data(dyn_block_t *block) {
  return block->data;
}

// FIXME: Replace with placement new
/** Initializes a dynamic array.
@param[in,out] arr              pointer to a memory buffer of size
                                sizeof(dyn_array_t)
@return	initialized dyn array */
inline dyn_array_t * dyn_array_create(dyn_array_t *arr) {
  static_assert(DYN_ARRAY_DATA_SIZE < DYN_BLOCK_FULL_FLAG, "error DYN_ARRAY_DATA_SIZE >= DYN_BLOCK_FULL_FLAG");

  arr->heap = nullptr;
  arr->used = 0;

#ifdef UNIV_DEBUG
  arr->buf_end = 0;
  arr->magic_n = DYN_BLOCK_MAGIC_N;
#endif /* UNIV_DEBUG */

  return arr;
}

/** Frees a dynamic array.
@param[in,out] arr              Dynamic array. */
inline void dyn_array_free(dyn_array_t *arr) {
  if (arr->heap != nullptr) {
    mem_heap_free(arr->heap);
  }

#ifdef UNIV_DEBUG
  arr->magic_n = 0;
#endif /* UNIV_DEBUG */
}

/** Makes room on top of a dyn array and returns a pointer to the added element.
The caller must copy the element to the pointer returned.
@param[in,out] arr              Dynamic array.
@param[in] size                 Size of the element to push.
@return	pointer to the element */
inline void *dyn_array_push(dyn_array_t *arr, ulint size) {
  ut_ad(size > 0);
  ut_ad(size <= DYN_ARRAY_DATA_SIZE);
  ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

  auto block = arr;
  auto used = block->used;

  if (used + size > DYN_ARRAY_DATA_SIZE) {
    /* Get the last array block */

    block = dyn_array_get_last_block(arr);
    used = block->used;

    if (used + size > DYN_ARRAY_DATA_SIZE) {
      block = dyn_array_add_block(arr);
      used = block->used;
    }
  }

  block->used = used + size;
  ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

  return (block->data) + used;
}

/** Makes room on top of a dyn array and returns a pointer to a buffer in it.
After copying the elements, the caller must close the buffer using
dyn_array_close.
@param[in,out] arr              Dynamic array.
@param[in] size                 Size in bytes of the buffer; MUST be smaller
                                than DYN_ARRAY_DATA_SIZE!
@return	pointer to the buffer */
inline byte * dyn_array_open(dyn_array_t *arr, ulint size) {
  ut_ad(size);
  ut_ad(size <= DYN_ARRAY_DATA_SIZE);
  ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

  auto block = arr;
  auto used = block->used;

  if (used + size > DYN_ARRAY_DATA_SIZE) {
    /* Get the last array block */

    block = dyn_array_get_last_block(arr);
    used = block->used;

    if (used + size > DYN_ARRAY_DATA_SIZE) {
      block = dyn_array_add_block(arr);
      used = block->used;
      ut_a(size <= DYN_ARRAY_DATA_SIZE);
    }
  }

  ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

#ifdef UNIV_DEBUG
  ut_ad(arr->buf_end == 0);
  arr->buf_end = used + size;
#endif /* UNIV_DEBU */

  return (block->data) + used;
}

/** Closes the buffer returned by dyn_array_open.
@param[in,out] arr              Dynamic array.
@param[in] ptr                  Buffer space from ptr up was not used. */
inline void dyn_array_close(dyn_array_t *arr, byte *ptr) {
  ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

  auto block = dyn_array_get_last_block(arr);

  ut_ad(arr->buf_end + block->data >= ptr);

  block->used = ptr - block->data;

  ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

#ifdef UNIV_DEBUG
  arr->buf_end = 0;
#endif /* UNIV_DEBUG */
}

/** Returns pointer to an element in dyn array.
@param[in,out] arr              Dynamic array.
@param[in] pos                  Position of element as bytes from array start
@return	pointer to element */
inline void *dyn_array_get_element(dyn_array_t *arr, ulint pos) {
  ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

  /* Get the first array block */
  auto block = dyn_array_get_first_block(arr);

  if (arr->heap != nullptr) {
    auto used = dyn_block_get_used(block);

    while (pos >= used) {
      pos -= used;

      block = UT_LIST_GET_NEXT(list, block);

      ut_ad(block);

      used = dyn_block_get_used(block);
    }
  }

  ut_ad(block != nullptr);
  ut_ad(dyn_block_get_used(block) >= pos);

  return block->data + pos;
}

/** Returns the size of stored data in a dyn array.
@param[in,out] arr              Dynamic array.
@return	data size in bytes */
inline ulint dyn_array_get_data_size(dyn_array_t *arr) {
  ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

  if (arr->heap == nullptr) {

    return arr->used;
  }

  ulint sum{};
  auto block = dyn_array_get_first_block(arr);

  while (block != nullptr) {
    sum += dyn_block_get_used(block);
    block = dyn_array_get_next_block(arr, block);
  }

  return sum;
}

/** Pushes n bytes to a dyn array.
@param[in,out] arr              Dynamic array.
@param[in] str                  String to write
@param[in] len                  String length. */
inline void dyn_push_string(dyn_array_t *arr, const byte *str, ulint len) {

  while (len > 0) {
    ulint n_copied{len};

    if (unlikely(len > DYN_ARRAY_DATA_SIZE)) {
      n_copied = DYN_ARRAY_DATA_SIZE;
    }

    memcpy(dyn_array_push(arr, n_copied), str, n_copied);

    str += n_copied;
    len -= n_copied;
  }
}
