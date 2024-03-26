#pragma once

#include "innodb0types.h"

/** The info structure stored at the beginning of a heap block */
struct mem_block_info_t;

/** A block of a memory heap consists of the info structure
followed by an area of memory */
using mem_block_t = mem_block_info_t;

/* A memory heap is a nonempty linear list of memory blocks */
using mem_heap_t = mem_block_t;

/* Types of allocation for memory heaps: DYNAMIC means allocation from the
dynamic memory pool of the C compiler, BUFFER means allocation from the
buffer pool; the latter method is used for very big heaps */

/** The most common type, use malloc/free */
constexpr ulint MEM_HEAP_DYNAMIC = 0;

constexpr ulint MEM_HEAP_BUFFER = 1;

/** this flag can optionally be ORed to MEM_HEAP_BUFFER, in which case
 * heap->free_block is used in some cases for memory allocations, and if
 * it's nullptr, the memory allocation functions can return nullptr. */
constexpr ulint MEM_HEAP_BTR_SEARCH = 2;

/** The following start size is used for the first block in the memory heap if
the size is not specified, i.e., 0 is given as the parameter in the call of
create. The standard size is the maximum (payload) size of the blocks used for
allocations of small buffers. */
constexpr ulint MEM_BLOCK_START_SIZE = 64;

/** If a memory heap is allowed to grow into the buffer pool, the following
is the maximum size for a single allocated buffer: */
constexpr auto MEM_MAX_ALLOC_IN_BUF  = UNIV_PAGE_SIZE - 200;

[[nodiscard]] inline constexpr ulint mem_block_standard_size() {
 return UNIV_PAGE_SIZE >= 16384 ? 8000 : MEM_MAX_ALLOC_IN_BUF;
}

#define MEM_BLOCK_STANDARD_SIZE mem_block_standard_size()