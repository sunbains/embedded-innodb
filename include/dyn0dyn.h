/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/dyn0dyn.h
The dynamically allocated array

Created 2/5/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0mem.h"
#include "ut0slst.h"

#include <array>

/** This is the initial 'payload' size of a dynamic array;
this must be > MLOG_BUF_MARGIN + 30! */
constexpr uint32_t DYN_ARRAY_DATA_SIZE = 512;

/** Value of dyn_block_struct::magic_n */
constexpr uint32_t DYN_BLOCK_MAGIC_N = 375767;

/** Flag for dyn_block_struct::used that indicates a full block */
constexpr uint32_t DYN_BLOCK_FULL_FLAG = 0x1000000UL;

/** @brief A dynamically allocated array that grows as needed.
 * This class provides functionality for managing a dynamic array that can grow
 * as needed. It maintains a list of blocks where data is stored.
 */
template <size_t N = DYN_ARRAY_DATA_SIZE>
struct Dynamic_array {
  /** @brief A block in a dynamically allocated array. */
  struct Block {
    Block() : m_used(0), m_magic_n(DYN_BLOCK_MAGIC_N) {}

    ~Block() noexcept { ut_ad(m_magic_n == DYN_BLOCK_MAGIC_N); }

    /** Gets the number of used bytes in a dyn array block.
     * @param[in] block Block to check
     * @return number of bytes used */
    uint32_t get_used() const noexcept {
      ut_ad(m_magic_n == DYN_BLOCK_MAGIC_N);

      return m_used & ~DYN_BLOCK_FULL_FLAG;
    }

    /** Sets the full flag for a block.
     * @param[in] block Block to set the full flag for. */
    void set_full() noexcept {
      ut_ad(m_used <= N);
      ut_ad(m_magic_n == DYN_BLOCK_MAGIC_N);

      m_used |= DYN_BLOCK_FULL_FLAG;
    }

    byte *get_data() noexcept { return m_buffer.data(); }

    const byte *get_data() const noexcept { return m_buffer.data(); }

    /** Number of data bytes used in this block; DYN_BLOCK_FULL_FLAG
    is set when the block becomes full */
    uint32_t m_used;

    std::array<byte, N> m_buffer;

    UT_SLIST_NODE_T(Block) m_node;

#ifdef UNIV_DEBUG
    /** Magic number (DYN_BLOCK_MAGIC_N) */
    uint32_t m_magic_n = DYN_BLOCK_MAGIC_N;
#endif /* UNIV_DEBUG */
  };

  UT_SLIST_NODE_GETTER_DEF(Block, m_node)

  /** Constructor */
  Dynamic_array() : m_block() { m_blocks.push_back(&m_block); }

  /** Destructor */
  ~Dynamic_array() {
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);
    for (auto block : m_blocks) {
      call_destructor(block);
    }
    if (m_heap != nullptr) {
      mem_heap_free(m_heap);
      m_heap = nullptr;
    }
  };

  /** Gets the first block in a dyn array.
   * @return first block in the instance. */
  const Block *get_first_block() const noexcept {
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    return m_blocks.front();
  }

  /** Gets the last block in a dyn array.
   * @return last block in the instance. */
  Block *get_last_block() noexcept { return m_blocks.back(); }

  /** Gets the next block in a dyn array.
   * @param[in] block Block in the dynamic array
   * @return pointer to next, nullptr if end of list */
  const Block *get_next_block(const Block *block) const noexcept {
    ut_ad(block->m_magic_n == DYN_BLOCK_MAGIC_N);

    return block->m_node.m_next;
  }

  /** Gets the number of used bytes in a dyn array block.
   * @param[in] block Block to check
   * @return number of bytes used */
  auto get_used(const Block *block) const noexcept { return block->get_used(); }

  /** Gets pointer to the start of data in a dyn array block.
   * @param[in] block Block to get the data from.
   * @return const pointer to data */
  auto get_data(const Block *block) const noexcept {
    ut_ad(block->m_magic_n == DYN_BLOCK_MAGIC_N);

    return block->get_data();
  }

  /** Gets pointer to the start of data in a dyn array block.
   * @param[in] block Block to get the data from.
   * @return pointer to data */
  auto get_data(Block *block) noexcept { return block->get_data(); }

  /** Gets pointer to the start of data in the first block of the dyn array.
   * @return pointer to data */
  auto get_data() noexcept { return m_block.get_data(); }

  /** Makes room on top of a dyn array and returns a pointer to a buffer in it.
   * After copying the elements, the caller must close the buffer using close().
   * @param[in] size Size in bytes of the buffer; MUST be smaller than N!
   * @return pointer to the buffer */
  byte *open(ulint size) noexcept {
    ut_ad(!m_is_open);
    ut_ad(size > 0);
    ut_ad(size <= N);
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    auto block = &m_block;
    auto used = block->m_used;

    if (used + size > N) {
      /* Get the last array block */
      block = get_last_block();
      used = block->m_used;

      if (used + size > N) {
        block->set_full();
        block = add_block();
        used = block->m_used;
        ut_a(size <= N);
      }
    }

    ut_ad(block->m_used <= N);
    ut_ad(m_buf_end == 0);
    ut_d(m_buf_end = used + size);

    ut_d(m_is_open = true);

    return block->get_data() + used;
  }

  /** Closes the buffer returned by open().
   * @param[in] ptr Buffer space from ptr up was not used. */
  void close(byte *ptr) noexcept {
    ut_ad(m_is_open);
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    auto block = get_last_block();

    ut_ad(m_buf_end + block->get_data() >= ptr);

    block->m_used = ptr - block->get_data();

    ut_ad(block->m_used <= N);

    ut_d(m_buf_end = 0);
    ut_d(m_is_open = false);
  }

  /** Returns pointer to an element in dyn array.
   * @param[in] pos Position of element as bytes from array start
   * @return pointer to element */
  void *get_element(ulint pos) const noexcept {
    ut_a(m_heap != nullptr || m_blocks.size() == 1);
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    /* Get the first array block */
    auto block = get_first_block();
    auto used = block->get_used();

    while (pos >= used) {
      pos -= used;
      block = get_next_block(block);
      used = block->get_used();
    }

    ut_ad(block != nullptr);
    ut_ad(block->get_used() >= pos);

    return (void *)(block->get_data() + pos);
  }

  /** Returns the size of stored data in a dyn array.
   * @return data size in bytes */
  ulint get_data_size() const noexcept {
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    if (m_heap == nullptr) [[likely]] {
      return m_block.m_used;
    }

    ulint sum{};
    auto block = get_first_block();

    while (block != nullptr) {
      sum += block->get_used();
      block = get_next_block(block);
    }

    return sum;
  }

  /** Makes room on top of a dyn array and returns a pointer to the added element.
   * The caller must copy the element to the pointer returned.
   * @param[in] size Size of the element to push.
   * @return pointer to the element, and space available in the current block */
  void *push(ulint size) noexcept {
    ut_ad(size > 0);
    ut_ad(size <= N);
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    auto block = get_last_block();
    auto used = block->m_used;

    ut_ad(!(used & DYN_BLOCK_FULL_FLAG));

    if (used + size <= N) [[likely]] {
      block->m_used += size;
      return block->get_data() + used;
    } else {
      block->set_full();
      block = add_block();
      block->m_used = size;
      return block->get_data();
    }
  }

  /** Pushes n bytes to a dyn array.
   * Note: we don't pack the block because we try and avoid double
   * copying in the caller. This means if the len is greater than N,
   * we can loop forever.
   * @param[in] str String to write
   * @param[in] len String length. */
  void push_string(const byte *str, ulint len) noexcept {
    ut_a(len > 0);

    while (len > 0) {
      ulint n_copied{len};

      if (len > N) [[unlikely]] {
        n_copied = N;
      }

      auto ptr = push(n_copied);

      memcpy(ptr, str, n_copied);

      str += n_copied;
      len -= n_copied;
    }
  }

  bool single_block() const noexcept { return m_heap == nullptr; }

#ifdef UNIV_DEBUG
  void eq(ulint pos, const void *ptr, ulint size) const noexcept {
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);
    ut_ad(m_heap != nullptr || m_blocks.size() == 1);

    /* Get the first array block */
    auto block = get_first_block();
    auto used = block->get_used();

    ut_ad(used > 0);

    while (pos >= used) {
      pos -= used;
      block = get_next_block(block);
      used = block->get_used();
    }

    ut_ad(block != nullptr);
    ut_ad(block->get_used() >= pos);

    auto len{size};
    auto p{reinterpret_cast<const byte *>(ptr)};
    const byte *start = block->get_data() + pos;

    while (len > 0) {
      ut_a(*start == *p);

      ++p;
      ++start;
      --len;

      if (len > 0 && start == block->get_data() + block->get_used()) {
        block = get_next_block(block);
        start = block->get_data();
      }
    }
  }
#endif /* UNIV_DEBUG */

 private:
  /** Adds a new block to a dyn array.
   * @return created block */
  Block *add_block() noexcept {
    ut_ad(m_block.m_magic_n == DYN_BLOCK_MAGIC_N);

    if (m_heap == nullptr) {
      create_heap();
    }

    auto block = get_last_block();
    ut_a(block->m_used & DYN_BLOCK_FULL_FLAG);

    block = new (mem_heap_alloc(m_heap, sizeof(Block))) Block();

    m_blocks.push_back(block);

    return block;
  }

  void create_heap() noexcept {
    ut_ad(m_heap == nullptr);
    ut_ad(m_blocks.size() == 1);

    m_heap = mem_heap_create(sizeof(Block));
  }

 private:
#ifdef UNIV_DEBUG
  /** CHeck whether the open/close sequence is correct. */
  bool m_is_open{false};

  /** if opened, this is the buffer end offset, else this is 0 */
  uint32_t m_buf_end{};
#endif /* UNIV_DEBUG */

  /** The first block of the dynamic array */
  Block m_block;

  /** In the first block this is != nullptr if dynamic
  allocation has been needed */
  mem_heap_t *m_heap{};

  using Blocks = UT_SLIST_BASE_NODE_T(Block, m_node);

  /** List of all blocks */
  Blocks m_blocks;
};

using Dyn_array = Dynamic_array<DYN_ARRAY_DATA_SIZE>;
