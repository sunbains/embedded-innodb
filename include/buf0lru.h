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

/*** @file include/buf0lru.h
The database buffer pool LRU replacement algorithm

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "ut0byte.h"

struct Buf_LRU {

  /** Number of intervals for which we keep the history of these stats.
  Each interval is 1 second, defined by the rate at which
  srv_error_monitor_thread() calls Buf_LRU::stat_update(). */
  static constexpr ulint STAT_N_INTERVAL = 50;

  /** @name Heuristics for detecting index scan @{ */

  /** The denominator of buf_LRU_old_ratio. */
  static constexpr ulint OLD_RATIO_DIV = 1024;

  /** Maximum value of buf_LRU_old_ratio.
  @see m_old_adjust_len
  @see m_old_ratio_update */
  static constexpr ulint OLD_RATIO_MAX = OLD_RATIO_DIV;

  /** Minimum value of buf_LRU_old_ratio.
  @see buf_LRU_old_adjust_len
  @see buf_LRU_old_ratio_update
  The minimum must exceed (OLD_TOLERANCE + 5) * OLD_RATIO_DIV / OLD_MIN_LEN. */
  static constexpr ulint OLD_RATIO_MIN = 51;

  /** Minimum LRU list length for which the LRU_old pointer is defined.  8 megabytes of 16k pages */
  static constexpr ulint OLD_MIN_LEN = 512;

  /** The return type of buf_LRU_free_block() */
  enum Block_status {
    /** Freed */
    FREED = 0,

    /** Not freed because the caller asked to remove the
    uncompressed frame but the control block cannot be
    relocated */
    CANNOT_RELOCATE,

    /** Not freed because of some other reason */
    NOT_FREED
  };

  /** @brief Statistics for selecting the LRU list for eviction.
  
  These statistics are not 'of' LRU but 'for' LRU.  We keep count of I/O
  operations.  Based on the statistics we decide if we want to evict
  from buf_pool->LRU. */
  struct Stat {
    /** Counter of buffer pool I/O operations. */
    ulint m_io{};
  };

  /** Constructor
  @param[in] old_threshold_ms   Move the blocks to the "new" list after this threshold. */
  explicit Buf_LRU(Buf_pool *buf_pool) : m_buf_pool(buf_pool), m_old_ratio(s_old_ratio), m_old_threshold_ms(s_old_threshold_ms) {}

  /**
   * Tries to remove LRU flushed blocks from the end of the LRU list and put
   * them to the free list. This is beneficial for the efficiency of the insert
   * buffer operation, as flushed pages from non-unique non-clustered indexes are
   * here taken out of the buffer pool, and their inserts redirected to the insert
   * buffer. Otherwise, the flushed blocks could get modified again before read
   * operations need new buffer blocks, and the i/o work done in flushing would be
   * wasted.
   */
  void try_free_flushed_blocks();

  /**
   * Returns true if less than 25 % of the buffer pool is available. This can be
   * used in heuristics to prevent huge transactions eating up the whole buffer
   * pool for their locks.
   * @return true if less than 25 % of buffer pool left
   */
  bool buf_pool_running_out();

  /** Invalidates all pages belonging to a given tablespace when we are deleting
  the data file(s) of tHat tablespace. A PROBLEM: if readahead is being started,
  what guarantees that it will not try to read in pages after this operation has
  completed? */
  void invalidate_tablespace(space_id_t space_id); /*!< in: space id */

  /**
   * Try to free a block. If bpage is a descriptor of a compressed-only page, the
   *  descriptor object will be freed as well.
   *
   * NOTE: If this function returns FREED, it will not temporarily release
   * buf_pool_mutex. Furthermore, the page frame will no longer be accessible via bpage.
   *
   * The caller must hold buf_pool_mutex and buf_page_get_mutex(bpage) and release
   * these two mutexes after the call. No other buf_page_get_mutex() may be held
   * when calling this function.
   *
   * @param bpage The block to be freed.
   * @param buf_pool_mutex_released Pointer to a variable that will be assigned true
   *             if buf_pool_mutex was temporarily released, or nullptr.
   * 
   * @return FREED if freed, CANNOT_RELOCATE or NOT_FREED otherwise.
   */
  Block_status free_block(Buf_page *bpage, bool *buf_pool_mutex_released);

  /**
   * Try to free a replaceable block.
   *
   * @param n_iterations How many times this has been called repeatedly without result:
   *                     a high value means that we should search farther;
   *                     if n_iterations < 10, then we search
   *                     n_iterations / 10 * buf_pool->curr_size pages from the end of the LRU list
   * @return true if found and freed
   */
  bool search_and_free_block(ulint n_iterations);

  /**
   * Returns a free block from the buf_pool. The block is taken off the
   * free list. If it is empty, returns nullptr.
   *
   * @return A free control block, or nullptr if the buf_block->free list is empty.
   */
  Buf_block *get_free_only();

  /**
   * Returns a free block from the buf_pool. The block is taken off the
   * free list. If it is empty, blocks are moved from the end of the
   * LRU list to the free list.
   *
   * @return The free control block, in state BUF_BLOCK_READY_FOR_USE.
   */
  Buf_block *get_free_block();

  /**
   * Puts a block back to the free list.
   * 
   * @param block The block to be freed. Must not contain a file page.
   */
  void block_free_non_file_page(Buf_block *block);

  /**
   * Adds a block to the LRU list.
   * 
   * @param bpage The control block to be added.
   * @param old True if the block should be put to the old blocks in the LRU list, 
   *            else put to the start. If the LRU list is very short, added to the 
   *            start regardless of this parameter.
   */
  void add_block(Buf_page *bpage, bool old);

  /**
   * Moves a block to the start of the LRU list.
   * 
   * @param bpage The control block to be moved.
   */
  void make_block_young(Buf_page *bpage);

  /**
   * Moves a block to the end of the LRU list.
   * 
   * @param bpage The control block to be moved.
   */
  void make_block(Buf_page *bpage);

  /**
   * Update the historical stats that we are collecting for LRU eviction policy 
   * at the end of each interval.
   */
  void stat_update();

  /**
   * Reset buffer LRU variables.
   */
  void var_init();
#if defined UNIV_DEBUG

  /*** Validates the LRU list.
  @return	true */
  bool validate();

  /*** Prints the LRU list. */
  void print();

#endif /* UNIV_DEBUG */

  /** Maximum LRU list search length in buf_pool->m_flusher->LRU_recommendation() */
  static ulint get_free_search_len() { return 5 + 2 * srv_buf_pool->get_read_ahead_area(); }

  /** Increments the I/O counter in buf_LRU_stat_cur. */
  void stat_inc_io() { ++m_stat_cur.m_io; }

  /** @return the old theshold in ms at which we can move the block to new area of the list. */
  auto get_old_threshold_ms() const { return m_old_threshold_ms; }

  /** @return the old theshold in ms at which we can move the block to new area of the list. */
  auto get_old_threshold_ms_ptr() { return &m_old_threshold_ms; }

  auto get_old_ratio() const { return m_old_ratio; }

  /**
   * Updates buf_LRU_old_ratio.
   * 
   * @param old_pct Reserve this percentage of the buffer pool for "old" blocks.
   * @param adjust True to adjust the LRU list, false to just assign buf_LRU_old_ratio 
   *               during the initialization of InnoDB.
   * @return The updated old_pct.
   */
  ulint old_ratio_update(ulint old_pct, bool adjust);

 private:
  /** Takes a block out of the LRU list and page hash table.

  @param[in,out] bpage          block, must contain a file page and
                                be in a state where it can be freed; there
                                may or may not be a hash index to the page
  @return the new state of the block BUF_BLOCK_REMOVE_HASH otherwise */
  /** Puts a file page whose has no hash index to the free list. */
  void block_free_hashed_page(Buf_block *block);

  /** Takes a block out of the LRU list and page hash table.
  
  @param[in,out] bpage          block, must contain a file page and
                                be in a state where it can be freed; there
                                may or may not be a hash index to the page
  @return the new state of the block BUF_BLOCK_REMOVE_HASH otherwise */
  Buf_page_state block_remove_hashed_page(Buf_page *bpage);

  /** Try to free a clean page from the common LRU list.

  If n_iterations < 10, then we search:
     n_iterations / 10 * buf_pool->curr_size pages

  from the end of the LRU list.A high value means that we should search farther.

  @param[in] n_iterations       How many times this has been called repeatedly
                                without result.

  @return	true if freed */
  bool free_from_common_LRU_list(ulint n_iterations);

  /** Moves the LRU_old pointer so that the length of the old blocks list
  is inside the allowed limits. */
  void old_adjust_len();

  /** Initializes the old blocks pointer in the LRU list. This function should be
  called when the LRU list grows to OLD_MIN_LEN length. */
  void old_init();

  /** Removes a block from the LRU list.
  @param[in] bpage                Buffer lock to remove */
  void remove_block(Buf_page *bpage);

  /** Adds a block to the LRU list end.
  @param[in] bpage                Buffer lock to add */
  void add_block_to_end_low(Buf_page *bpage);

  /** Adds a block to the LRU list.
  @param[in] bpage              Buffer lock to add
  @param[in] old                true if should be put to the old blocks in the LRU list,
                                else put to the start; if the LRU list is very short, the
				block is added to the start, regardless of this parameter */
  void add_block_low(Buf_page *bpage, bool old);

 private:
  static_assert(OLD_RATIO_MIN < OLD_RATIO_MAX, "error OLD_RATIO_MIN >= OLD_RATIO_MAX");

  static_assert(OLD_RATIO_MAX <= OLD_RATIO_DIV, "error OLD_RATIO_MAX > OLD_RATIO_DIV");

  /* @} */

  /** @name Heuristics for detecting index scan @{ */

  /** The buffer pool. */
  Buf_pool *m_buf_pool{};

  /** Reserve this much/OLD_RATIO_DIV of the buffer pool for "old" blocks.
  Protected by buf_pool_mutex. */
  ulint m_old_ratio{};

  /** Move blocks to "new" LRU list only if the first access was at least this
  many milliseconds ago.  Not protected by any mutex or latch. */
  ulint m_old_threshold_ms{};

  /** Sampled values buf_LRU_stat_cur. Protected by buf_pool_mutex. Updated by stat_update(). */
  std::array<Stat, STAT_N_INTERVAL> m_stats{};

  /** Cursor to buf_LRU_stat_arr[] that is updated in a round-robin fashion. */
  ulint m_stat_ind{};

  /** If we switch on the monitor thread because there are too few available
  frames in the buffer pool, we set this to true */
  bool m_switched_on_monitor{};

 public:
  /** Current operation counters. Not protected by any mutex. Cleared by stat_update(). */
  Stat m_stat_cur{};

  /** Running sum of past values of buf_LRU_stat_cur. Updated by stat_update().
  Protected by buf_pool_mutex. */
  Stat m_stat_sum{};

  /** Configure on startup. */
  static ulint s_old_threshold_ms;

  /** Configuration for startup. */
  static ulint s_old_ratio;
};
