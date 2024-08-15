/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
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

/*** @file include/buf0flu.h
The database buffer pool flush algorithm

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "buf0buf.h"
#include "mtr0mtr.h"
#include "ut0byte.h"

#include <array>

struct buf_page_t;
struct buf_block_t;

struct Buf_flush {
  /** Number of intervals for which we keep the history of these stats.
  Each interval is 1 second, defined by the rate at which
  srv_error_monitor_thread() calls buf_pool->m_flusher->stat_update(). */
  static constexpr ulint STAT_N_INTERVAL = 20;

  /**
  * Remove a block from the flush list of modified blocks.
  *
  * @param bpage Pointer to the block in question.
  */
  void remove(buf_page_t *bpage);

  /**
  * Relocates a buffer control block on the flush_list.
  * Note that it is assumed that the contents of bpage has already been copied to dpage.
  *
  * @param bpage Pointer to the control block being moved.
  * @param dpage Pointer to the destination block.
  */
  void relocate_on_flush_list(buf_page_t *bpage, buf_page_t *dpage);

  /**
  * Updates the flush system data structures when a write is completed.
  *
  * @param bpage Pointer to the block in question.
  */
  void write_complete(buf_page_t *bpage);

  /**
   * Flushes pages from the end of the LRU list if there is too small
   * a margin of replaceable pages there.
   */
  void free_margin();

  /**
   * Initializes a page for writing to the tablespace.
   *
   * @param page The page to initialize.
   * @param newest_lsn The newest modification LSN to the page.
   */
  void init_for_writing(byte *page, uint64_t newest_lsn);

  /**
   * This utility flushes dirty blocks from the end of the LRU list or flush_list.
   *
   * NOTE 1: in the case of an LRU flush the calling thread may own latches to pages:
   *         to avoid deadlocks, this function must be written so that it cannot end
   *         up waiting for these latches!
   *
   * NOTE 2: in the case of a flush list flush, the calling thread is not allowed to
   *         own any latches on pages!
   *
   * @param flush_type          BUF_FLUSH_LRU or BUF_FLUSH_LIST; if BUF_FLUSH_LIST,
   *                            then the caller must not own any latches on pages
   *
   * @param min_n               wished minimum number of blocks flushed (it is not guaranteed that
   *                            the actual number is that big, though)
   *
   * @param lsn_limit           In the case BUF_FLUSH_LIST all blocks whose oldest_modification
   *                            is smaller than this should be flushed (if their number
   *                            does not exceed min_n), otherwise ignored
   *
   * @return number of blocks for which the write request was queued; ULINT_UNDEFINED if
   *         there was a flush of the same type already running
   */
  ulint batch(buf_flush flush_type, ulint min_n, uint64_t lsn_limit);

  /**
   * Waits until a flush batch of the given type ends.
   *
   * @param type The type of flush batch to wait for (BUF_FLUSH_LRU or BUF_FLUSH_LIST).
   */
  void wait_batch_end(buf_flush type);

  /**
   * @brief Returns true if the file page block is immediately suitable for replacement,
   * i.e., transition FILE_PAGE => NOT_USED allowed.
   *
   * @param bpage The buffer control block, must be bpage->in_file() and in the LRU list.
   * @return True if the block can be replaced immediately.
   */
  bool ready_for_replace(buf_page_t *bpage);

  /**
   * @brief Update the historical stats that we are collecting for flush rate
   * heuristics at the end of each interval.
   */
  void stat_update();

  /**
   * @brief Determines the fraction of dirty pages that need to be flushed based
   * on the speed at which we generate redo log. Note that if redo log
   * is generated at significant rate without a corresponding increase
   * in the number of dirty pages (for example, an in-memory workload)
   * it can cause IO bursts of flushing. This function implements heuristics
   * to avoid this burstiness.
   * @return Number of dirty pages to be flushed per second.
   */
  ulint get_desired_flush_rate();

#if defined UNIV_DEBUG
  /** Validates the flush list.
  @return	true if ok */
  bool validate();
#endif /* UNIV_DEBUG */

  /**
   * @brief Initialize the red-black tree to speed up insertions into the flush_list
   * during recovery process. Should be called at the start of recovery
   * process before any page has been read/written.
   */
  void init_flush_rbt();

  /**
   * @brief Frees up the red-black tree.
   */
  void free_flush_rbt();

  /**
   * @brief Inserts a modified block into the flush list.
   *
   * @param block The block which is modified.
   */
  void insert_into_flush_list(buf_block_t *block);

  /**
   * @brief Inserts a modified block into the flush list in the right sorted position.
   * This function is used by recovery, because there the modifications do not
   * necessarily come in the order of lsn's.
   *
   * @param block The block which is modified.
   */
  void insert_sorted_into_flush_list(buf_block_t *block);

  /**
   * @brief This function should be called at a mini-transaction commit, if a page was
   * modified in it. Puts the block to the list of modified blocks, if it is not
   * already in it.
   *
   * @param block The block which is modified.
   * @param mtr The mini-transaction.
   */
  void note_modification( buf_block_t *block, mtr_t *mtr) {
    ut_ad(block);
    ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);
    ut_ad(block->m_page.m_buf_fix_count > 0);
  #ifdef UNIV_SYNC_DEBUG
    ut_ad(rw_lock_own(&(block->lock), RW_LOCK_EX));
  #endif /* UNIV_SYNC_DEBUG */
    ut_ad(buf_pool_mutex_own());
  
    ut_ad(mtr->start_lsn != 0);
    ut_ad(mtr->modifications);
    ut_ad(block->m_page.m_newest_modification <= mtr->end_lsn);
  
    block->m_page.m_newest_modification = mtr->end_lsn;
  
    if (!block->m_page.m_oldest_modification) {
  
      block->m_page.m_oldest_modification = mtr->start_lsn;
      ut_ad(block->m_page.m_oldest_modification != 0);
  
      insert_into_flush_list(block);
    } else {
      ut_ad(block->m_page.m_oldest_modification <= mtr->start_lsn);
    }
  
    ++srv_buf_pool_write_requests;
  }
  
  /**
   * @brief This function should be called when recovery has modified a buffer page.
   *
   * @param block The block which is modified.
   * @param start_lsn The start LSN of the first MTR in a set of MTRs.
   * @param end_lsn The end LSN of the last MTR in the set of MTRs.
   */
  void recv_note_modification( buf_block_t *block, uint64_t start_lsn, uint64_t end_lsn) {
    ut_ad(block);
    ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);
    ut_ad(block->m_page.m_buf_fix_count > 0);
  
  #ifdef UNIV_SYNC_DEBUG
    ut_ad(rw_lock_own(&(block->lock), RW_LOCK_EX));
  #endif /* UNIV_SYNC_DEBUG */
  
    buf_pool_mutex_enter();
  
    ut_ad(block->m_page.m_newest_modification <= end_lsn);
  
    block->m_page.m_newest_modification = end_lsn;
  
    if (!block->m_page.m_oldest_modification) {
  
      block->m_page.m_oldest_modification = start_lsn;
  
      ut_ad(block->m_page.m_oldest_modification != 0);
  
      insert_sorted_into_flush_list(block);
    } else {
      ut_ad(block->m_page.m_oldest_modification <= start_lsn);
    }
  
    buf_pool_mutex_exit();
  }

  /** When free_margin is called, it tries to make this many blocks
  available to replacement in the free list and at the end of the LRU list (to
  make sure that a read-ahead batch can be read efficiently in a single sweep). */
  auto get_free_block_margin() const {
    return 5 + srv_buf_pool->get_read_ahead_area();
  }

  /** Extra margin to apply above the free block margin */
  auto get_extra_margin() const {
    return get_free_block_margin() / 4 + 100;
  }

#if defined UNIV_DEBUG
  /** Validates the flush list.
  @return	true if ok */
  bool validate_low();
#endif /* UNIV_DEBUG */

  /** @brief Statistics for selecting flush rate based on redo log
  generation speed.

  These statistics are generated for heuristics used in estimating the
  rate at which we should flush the dirty blocks to avoid bursty IO
  activity. Note that the rate of flushing not only depends on how many
  dirty pages we have in the buffer pool but it is also a fucntion of
  how much redo the workload is generating and at what rate. */
  struct Stat {
    /** Amount of redo generated. */
    uint64_t m_redo{};
  
    /** Number of pages flushed. */
    ulint m_n_flushed{};
  };

private:
  /**
   * @brief Insert a block in the m_recovery_flush_list and returns a pointer to its predecessor or nullptr if no predecessor.
   * The ordering is maintained on the basis of the <oldest_modification, space, offset> key.
   * @param bpage The bpage to be inserted.
   * @return Pointer to the predecessor or nullptr if no predecessor.
   */
  buf_page_t *insert_in_flush_rbt(buf_page_t *bpage);

  /**
   * @brief Delete a bpage from the m_recovery_flush_list.
   * @param bpage The bpage to be removed.
   */
  void delete_from_flush_rbt(buf_page_t *bpage);

  /**
   * @brief Compare two modified blocks in the buffer pool. The key for comparison is: key = <oldest_modification, space, offset>
   *
   * This comparison is used to maintain ordering of blocks in the buf_pool->m_recovery_flush_list.
   * Note that for the purpose of m_recovery_flush_list, we only need to order blocks on the oldest_modification.
   * The other two fields are used to uniquely identify the blocks.
   *
   * @param[in] b1              Block
   * @param[in] b2              Block
   *
   * @return < 0 if b2 < b1, 0 if b2 == b1, > 0 if b2 > b1
   */
  static bool block_cmp(const buf_page_t *b1, const buf_page_t *b2);

  /**
   * @brief Flush a batch of writes to the datafiles that have already been written by the OS.
   */
  void sync_datafiles();

  /**
   * @brief Flushes possible buffered writes from the doublewrite memory buffer to disk,
   * and also wakes up the aio thread if simulated aio is used.
   * It is very important to call this function after a batch of writes has been posted,
   * and also when we may have to wait for a page latch! Otherwise a deadlock of threads can occur.
   */
  void buffered_writes();

  /**
   * @brief Posts a buffer page for writing. If the doublewrite memory buffer is full,
   * calls buf_pool->m_flusher->buffered_writes and waits for for free space to appear.
   *
   * @param bpage The buffer block to write.
   */
  void post_to_doublewrite_buf(buf_page_t *bpage);

  /**
   * @brief Does an asynchronous write of a buffer page.
   * NOTE: in simulated aio and also when the doublewrite buffer is used, we must call buf_pool->m_flusher->buffered_writes after we have posted a batch of writes!
   * @param bpage The buffer block to write.
   */
  void write_block_low(buf_page_t *bpage);

  /**
   * @brief Writes a flushable page asynchronously from the buffer pool to a file.
   * NOTE: in simulated aio we must call os_aio_simulated_wake_handler_threads after we have posted a batch of writes!
   * NOTE: buf_pool_mutex and buf_page_get_mutex(bpage) must be held upon entering this function, and they will be released by this function.
   * @param bpage The buffer control block.
   * @param flush_type The flush type.
   */
  void page(buf_page_t *bpage, buf_flush flush_type);

  /**
   * @brief Flushes to disk all flushable pages within the flush area.
   * @param space The space id.
   * @param offset The page offset.
   * @param flush_type The flush type (BUF_FLUSH_LRU or BUF_FLUSH_LIST).
   * @return Number of pages flushed.
   */
  ulint try_neighbors(ulint space, ulint offset, buf_flush flush_type);

  /**
   * @brief Gives a recommendation of how many blocks should be flushed to establish
   * a big enough margin of replaceable blocks near the end of the LRU list and in the free list.
   *
   * @return Number of blocks which should be flushed from the end of the LRU list.
   */
  ulint LRU_recommendation();

  /**
   * @brief Returns true if the block is modified and ready for flushing.
   * @param bpage Buffer control block, must be bpage->in_file()
   * @param flush_type Flush type (BUF_FLUSH_LRU or BUF_FLUSH_LIST)
   * @return True if can flush immediately
   */
  bool ready_for_flush(buf_page_t *bpage, buf_flush flush_type);


private:
  /** Sampled values buf_pool->m_flusher->stat_cur.
  Not protected by any mutex.  Updated by buf_pool->m_flusher->stat_update(). */
  std::array<Stat, STAT_N_INTERVAL> m_stats;

  /** Cursor to m_stats[]. Updated in a round-robin fashion. */
  ulint m_stat_ind;

  /** Values at start of the current interval. Reset by stat_update(). */
  Stat m_stat_cur;

  /** Running sum of past values of buf_pool->m_flusher->stat_cur.
  Updated by buf_pool->m_flusher->stat_update(). Not protected by any mutex. */
  Stat m_stat_sum;

  /** Number of pages flushed through non m_flush_list flushes. */
  ulint m_LRU_flush_page_count{};

/* @} */

};
