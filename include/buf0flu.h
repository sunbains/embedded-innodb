/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.

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

/**
 * Remove a block from the flush list of modified blocks.
 *
 * @param bpage Pointer to the block in question.
 */
void buf_flush_remove(buf_page_t *bpage);

/**
 * Relocates a buffer control block on the flush_list.
 * Note that it is assumed that the contents of bpage has already been copied to dpage.
 *
 * @param bpage Pointer to the control block being moved.
 * @param dpage Pointer to the destination block.
 */
void buf_flush_relocate_on_flush_list(buf_page_t *bpage, buf_page_t *dpage);

/**
 * Updates the flush system data structures when a write is completed.
 *
 * @param bpage Pointer to the block in question.
 */
void buf_flush_write_complete(buf_page_t *bpage);

/**
 * Flushes pages from the end of the LRU list if there is too small
 * a margin of replaceable pages there.
 */
void buf_flush_free_margin();

/**
 * Initializes a page for writing to the tablespace.
 *
 * @param page The page to initialize.
 * @param newest_lsn The newest modification LSN to the page.
 */
void buf_flush_init_for_writing( byte *page, uint64_t newest_lsn);

/**
 * This utility flushes dirty blocks from the end of the LRU list or flush_list.
 * NOTE 1: in the case of an LRU flush the calling thread may own latches to pages:
 * to avoid deadlocks, this function must be written so that it cannot end up waiting for these latches!
 * NOTE 2: in the case of a flush list flush, the calling thread is not allowed to own any latches on pages!
 *
 * @param flush_type BUF_FLUSH_LRU or BUF_FLUSH_LIST; if BUF_FLUSH_LIST, then the caller must not own any latches on pages
 * @param min_n wished minimum number of blocks flushed (it is not guaranteed that the actual number is that big, though)
 * @param lsn_limit in the case BUF_FLUSH_LIST all blocks whose oldest_modification is smaller than this should be flushed (if their number does not exceed min_n), otherwise ignored
 * @return number of blocks for which the write request was queued; ULINT_UNDEFINED if there was a flush of the same type already running
 */
ulint buf_flush_batch(buf_flush flush_type, ulint min_n, uint64_t lsn_limit);

/**
 * Waits until a flush batch of the given type ends.
 *
 * @param type The type of flush batch to wait for (BUF_FLUSH_LRU or BUF_FLUSH_LIST).
 */
void buf_flush_wait_batch_end(buf_flush type);

/**
 * Returns true if the file page block is immediately suitable for replacement,
 * i.e., transition FILE_PAGE => NOT_USED allowed.
 *
 * @param bpage The buffer control block, must be buf_page_in_file(bpage) and in the LRU list.
 * @return True if the block can be replaced immediately.
 */
bool buf_flush_ready_for_replace(buf_page_t *bpage);

/**
 * @brief Returns true if the file page block is immediately suitable for replacement,
 * i.e., transition FILE_PAGE => NOT_USED allowed.
 *
 * @param bpage The buffer control block, must be buf_page_in_file(bpage) and in the LRU list.
 * @return True if the block can be replaced immediately.
 */
bool buf_flush_ready_for_replace(buf_page_t *bpage);

/** @brief Statistics for selecting flush rate based on redo log
generation speed.

These statistics are generated for heuristics used in estimating the
rate at which we should flush the dirty blocks to avoid bursty IO
activity. Note that the rate of flushing not only depends on how many
dirty pages we have in the buffer pool but it is also a fucntion of
how much redo the workload is generating and at what rate. */
struct buf_flush_stat_struct {
  /** amount of redo generated. */
  uint64_t redo;

  /** number of pages flushed. */
  ulint n_flushed;
};

/** Statistics for selecting flush rate of dirty pages. */
typedef struct buf_flush_stat_struct buf_flush_stat_t;

/**
 * @brief Update the historical stats that we are collecting for flush rate
 * heuristics at the end of each interval.
 */
void buf_flush_stat_update();

/**
 * @brief Determines the fraction of dirty pages that need to be flushed based
 * on the speed at which we generate redo log. Note that if redo log
 * is generated at significant rate without a corresponding increase
 * in the number of dirty pages (for example, an in-memory workload)
 * it can cause IO bursts of flushing. This function implements heuristics
 * to avoid this burstiness.
 * @return Number of dirty pages to be flushed per second.
 */
ulint buf_flush_get_desired_flush_rate();

#if defined UNIV_DEBUG
/** Validates the flush list.
@return	true if ok */
bool buf_flush_validate();
#endif /* UNIV_DEBUG */

/**
 * @brief Initialize the red-black tree to speed up insertions into the flush_list
 * during recovery process. Should be called at the start of recovery
 * process before any page has been read/written.
 */
void buf_flush_init_flush_rbt();

/**
 * @brief Frees up the red-black tree.
 */
void buf_flush_free_flush_rbt();

/** When buf_flush_free_margin is called, it tries to make this many blocks
available to replacement in the free list and at the end of the LRU list (to
make sure that a read-ahead batch can be read efficiently in a single
sweep). */
#define BUF_FLUSH_FREE_BLOCK_MARGIN (5 + BUF_READ_AHEAD_AREA)

/** Extra margin to apply above BUF_FLUSH_FREE_BLOCK_MARGIN */
#define BUF_FLUSH_EXTRA_MARGIN (BUF_FLUSH_FREE_BLOCK_MARGIN / 4 + 100)

/**
 * @brief Inserts a modified block into the flush list.
 *
 * @param block The block which is modified.
 */
void buf_flush_insert_into_flush_list(buf_block_t *block);

/**
 * @brief Inserts a modified block into the flush list in the right sorted position.
 * This function is used by recovery, because there the modifications do not
 * necessarily come in the order of lsn's.
 *
 * @param block The block which is modified.
 */
void buf_flush_insert_sorted_into_flush_list(buf_block_t *block);

/**
 * @brief This function should be called at a mini-transaction commit, if a page was
 * modified in it. Puts the block to the list of modified blocks, if it is not
 * already in it.
 *
 * @param block The block which is modified.
 * @param mtr The mini-transaction.
 */
inline void buf_flush_note_modification( buf_block_t *block, mtr_t *mtr) {
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

    buf_flush_insert_into_flush_list(block);
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
inline void buf_flush_recv_note_modification( buf_block_t *block, uint64_t start_lsn, uint64_t end_lsn) {
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

    buf_flush_insert_sorted_into_flush_list(block);
  } else {
    ut_ad(block->m_page.m_oldest_modification <= start_lsn);
  }

  buf_pool_mutex_exit();
}
