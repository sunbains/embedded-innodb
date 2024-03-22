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

/** @file buf/buf0lru.c
The database buffer replacement algorithm

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#include "buf0lru.h"

#ifdef UNIV_NONINL
#include "buf0lru.ic"
#endif

#include "btr0btr.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "fil0fil.h"
#include "hash0hash.h"
#include "log0recv.h"
#include "os0file.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0rnd.h"

/** The number of blocks from the LRU_old pointer onward, including
the block pointed to, must be buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV
of the whole LRU list length, except that the tolerance defined below
is allowed. Note that the tolerance must be small enough such that for
even the BUF_LRU_OLD_MIN_LEN long LRU list, the LRU_old pointer is not
allowed to point to either end of the LRU list. */
constexpr ulint BUF_LRU_OLD_TOLERANCE = 20;

/** The minimum amount of non-old blocks when the LRU_old list exists
(that is, when there are more than BUF_LRU_OLD_MIN_LEN blocks).
@see buf_LRU_old_adjust_len */
constexpr ulint BUF_LRU_NON_OLD_MIN_LEN = 5;

static_assert(BUF_LRU_NON_OLD_MIN_LEN < BUF_LRU_OLD_MIN_LEN, "error BUF_LRU_NON_OLD_MIN_LEN >= BUF_LRU_OLD_MIN_LEN");

/** If we switch on the InnoDB monitor because there are too few available
frames in the buffer pool, we set this to true */
static bool buf_lru_switched_on_innodb_mon = false;

/* @{ */

/** Number of intervals for which we keep the history of these stats.
Each interval is 1 second, defined by the rate at which
srv_error_monitor_thread() calls buf_LRU_stat_update(). */
constexpr ulint BUF_LRU_STAT_N_INTERVAL = 50;

/** Sampled values buf_LRU_stat_cur.
Protected by buf_pool_mutex.  Updated by buf_LRU_stat_update(). */
static buf_LRU_stat_t buf_LRU_stat_arr[BUF_LRU_STAT_N_INTERVAL];

/** Cursor to buf_LRU_stat_arr[] that is updated in a round-robin fashion. */
static ulint buf_LRU_stat_arr_ind;

/** Current operation counters.  Not protected by any mutex.  Cleared
by buf_LRU_stat_update(). */
buf_LRU_stat_t buf_LRU_stat_cur;

/** Running sum of past values of buf_LRU_stat_cur.
Updated by buf_LRU_stat_update().  Protected by buf_pool_mutex. */
buf_LRU_stat_t buf_LRU_stat_sum;

/* @} */

/** @name Heuristics for detecting index scan @{ */
/** Reserve this much/BUF_LRU_OLD_RATIO_DIV of the buffer pool for
"old" blocks.  Protected by buf_pool_mutex. */
ulint buf_LRU_old_ratio;

/** Move blocks to "new" LRU list only if the first access was at
least this many milliseconds ago.  Not protected by any mutex or latch. */
ulint buf_LRU_old_threshold_ms;

/* @} */

/** Takes a block out of the LRU list and page hash table.

@param[in,out] bpage            block, must contain a file page and
                                be in a state where it can be freed; there
                                may or may not be a hash index to the page
@return the new state of the block BUF_BLOCK_REMOVE_HASH otherwise */
/** Puts a file page whose has no hash index to the free list. */
static void buf_LRU_block_free_hashed_page(buf_block_t *block);

/** Takes a block out of the LRU list and page hash table.

@param[in,out] bpage            block, must contain a file page and
                                be in a state where it can be freed; there
                                may or may not be a hash index to the page
@return the new state of the block BUF_BLOCK_REMOVE_HASH otherwise */
static buf_page_state buf_LRU_block_remove_hashed_page(buf_page_t *bpage);

void buf_LRU_var_init() {
  buf_lru_switched_on_innodb_mon = false;

  memset(buf_LRU_stat_arr, 0x0, sizeof(buf_LRU_stat_arr));
  buf_LRU_stat_arr_ind = 0;
  memset(&buf_LRU_stat_cur, 0x0, sizeof(buf_LRU_stat_cur));
  memset(&buf_LRU_stat_sum, 0x0, sizeof(buf_LRU_stat_sum));
}

void buf_LRU_invalidate_tablespace(ulint id) {
  bool all_freed{};

  while (!all_freed) {
    buf_pool_mutex_enter();

    all_freed = true;

    auto bpage = UT_LIST_GET_LAST(buf_pool->m_lru_list);

    while (bpage != nullptr) {
      ut_a(buf_page_in_file(bpage));

      auto prev_bpage = UT_LIST_GET_PREV(m_lru_list, bpage);

      /* bpage->m_space and bpage->m_io_fix are protected by
      buf_pool_mutex and block_mutex.  It is safe to check
      them while holding buf_pool_mutex only. */

      if (bpage->get_space() != id) {

        /* Skip this block, as it does not belong to the space that is being invalidated. */

      } else if (buf_page_get_io_fix(bpage) != BUF_IO_NONE) {

        /* We cannot remove this page during this scan yet; maybe the
        system is currently reading it in, or flushing the modifications
        to the file */

        all_freed = false;

      } else {
        auto block_mutex = buf_page_get_mutex(bpage);

        mutex_enter(block_mutex);

        if (bpage->m_buf_fix_count > 0) {

          /* We cannot remove this page during this scan yet; maybe the system is
          currently reading it in, or flushing the modifications to the file */

          all_freed = false;
	  break;

        } else {

          if (bpage->m_oldest_modification != 0) {
            buf_flush_remove(bpage);
          }

          /* Remove from the LRU list. */
          buf_LRU_block_remove_hashed_page(bpage);

	  {
            /* We can't cast it using buf_page_get_block() because of the checks there. */
	    auto block{reinterpret_cast<buf_block_t*>(bpage)};
            buf_LRU_block_free_hashed_page(block);
           }
        }

        mutex_exit(block_mutex);
      }

      bpage = prev_bpage;
    }

    buf_pool_mutex_exit();

    if (!all_freed) {
      os_thread_sleep(20000);
    }
  }
}

/** Try to free a clean page from the common LRU list.

If n_iterations < 10, then we search:
   n_iterations / 10 * buf_pool->curr_size pages

from the end of the LRU list.A high value means that we should search farther.

@param[in] n_iterations         How many times this has been called repeatedly
                                without result.

@return	true if freed */
static bool buf_LRU_free_from_common_LRU_list(ulint n_iterations)
       
{

  ut_ad(buf_pool_mutex_own());

  auto distance = 100 + (n_iterations * buf_pool->m_curr_size) / 10;

  for (auto bpage = UT_LIST_GET_LAST(buf_pool->m_lru_list);
       likely(bpage != nullptr) && likely(distance > 0);
       bpage = UT_LIST_GET_PREV(m_lru_list, bpage), distance--) {

    enum buf_lru_free_block_status freed;
    unsigned accessed;
    mutex_t *block_mutex = buf_page_get_mutex(bpage);

    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->m_in_lru_list);

    mutex_enter(block_mutex);
    accessed = buf_page_is_accessed(bpage);
    freed = buf_LRU_free_block(bpage, nullptr);
    mutex_exit(block_mutex);

    switch (freed) {
      case BUF_LRU_FREED:
        /* Keep track of pages that are evicted without
      ever being accessed. This gives us a measure of
      the effectiveness of readahead */
        if (!accessed) {
          ++buf_pool->m_stat.n_ra_pages_evicted;
        }
        return (true);

      case BUF_LRU_NOT_FREED:
        /* The block was dirty, buffer-fixed, or I/O-fixed.
      Keep looking. */
        continue;

      case BUF_LRU_CANNOT_RELOCATE:
        /* This should never occur, because we
      want to discard the compressed page too. */
        break;
    }

    /* inappropriate return value from
    buf_LRU_free_block() */
    ut_error;
  }

  return (false);
}

bool buf_LRU_search_and_free_block(ulint n_iterations) {
  buf_pool_mutex_enter();

  auto freed = buf_LRU_free_from_common_LRU_list(n_iterations);

  if (!freed) {
    buf_pool->m_lru_flush_ended = 0;
  } else if (buf_pool->m_lru_flush_ended > 0) {
    buf_pool->m_lru_flush_ended--;
  }

  buf_pool_mutex_exit();

  return freed;
}

void buf_LRU_try_free_flushed_blocks() {
  buf_pool_mutex_enter();

  while (buf_pool->m_lru_flush_ended > 0) {

    buf_pool_mutex_exit();

    buf_LRU_search_and_free_block(1);

    buf_pool_mutex_enter();
  }

  buf_pool_mutex_exit();
}

bool buf_LRU_buf_pool_running_out() {
  buf_pool_mutex_enter();

  auto ret = !recv_recovery_on &&
	     UT_LIST_GET_LEN(buf_pool->m_free_list) + UT_LIST_GET_LEN(buf_pool->m_lru_list) <
	     buf_pool->m_curr_size / 4;

  buf_pool_mutex_exit();

  return ret;
}

buf_block_t *buf_LRU_get_free_only() {
  buf_block_t *block;

  ut_ad(buf_pool_mutex_own());

  block = (buf_block_t *)UT_LIST_GET_FIRST(buf_pool->m_free_list);

  if (block) {
    ut_ad(block->m_page.m_in_free_list);
    ut_d(block->m_page.m_in_free_list = false);
    ut_ad(!block->m_page.m_in_flush_list);
    ut_ad(!block->m_page.m_in_lru_list);
    ut_a(!buf_page_in_file(&block->m_page));
    UT_LIST_REMOVE(buf_pool->m_free_list, (&block->m_page));

    mutex_enter(&block->m_mutex);

    buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);
    UNIV_MEM_ALLOC(block->m_frame, UNIV_PAGE_SIZE);

    mutex_exit(&block->m_mutex);
  }

  return (block);
}

buf_block_t *buf_LRU_get_free_block() {
  bool freed;
  ulint n_iterations = 1;
  buf_block_t *block = nullptr;
  bool mon_value_was = false;
  bool started_monitor = false;

loop:
  buf_pool_mutex_enter();

  if (!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->m_free_list) + UT_LIST_GET_LEN(buf_pool->m_lru_list) < buf_pool->m_curr_size / 20) {

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  ERROR: over 95 percent of the buffer pool is occupied by lock heaps!"
      " Check that your transactions do not set too many row locks."
      " Your buffer pool size is %lu MB. Maybe you should make the buffer pool bigger?"
      " We intentionally generate a seg fault to print a stack trace on Linux!\n",
      (ulong)(buf_pool->m_curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
    );

    ut_error;

  } else if (!recv_recovery_on && (UT_LIST_GET_LEN(buf_pool->m_free_list) + UT_LIST_GET_LEN(buf_pool->m_lru_list)) < buf_pool->m_curr_size / 3) {

    if (!buf_lru_switched_on_innodb_mon) {

      /* Over 67 % of the buffer pool is occupied by lock
      heaps. This may be a memory leak! */

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  WARNING: over 67 percent of the buffer pool is occupied by"
        " lock heaps! Check that your transactions do not set too many"
        " row locks. Your buffer pool size is %lu MB.Maybe you should"
        " make the buffer pool bigger? Starting the InnoDB Monitor to"
        " print diagnostics, including lock heap and hash index sizes",
        (ulong)(buf_pool->m_curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
      );

      buf_lru_switched_on_innodb_mon = true;
      srv_print_innodb_monitor = true;
      os_event_set(srv_lock_timeout_thread_event);
    }

  } else if (buf_lru_switched_on_innodb_mon) {

    /* Switch off the InnoDB Monitor; this is a simple way
    to stop the monitor if the situation becomes less urgent,
    but may also surprise users if the user also switched on the
    monitor! */

    buf_lru_switched_on_innodb_mon = false;
    srv_print_innodb_monitor = false;
  }

  /* If there is a block in the free list, take it */
  block = buf_LRU_get_free_only();

  if (block != nullptr) {
    buf_pool_mutex_exit();

    if (started_monitor) {
      srv_print_innodb_monitor = mon_value_was;
    }

    return block;
  }

  /* If no block was in the free list, search from the end of the LRU
  list and try to free a block there */

  buf_pool_mutex_exit();

  freed = buf_LRU_search_and_free_block(n_iterations);

  if (freed > 0) {
    goto loop;
  }

  if (n_iterations > 30) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Warning: difficult to find free blocks in\n"
      "the buffer pool (%lu search iterations)!"
      " Consider\n"
      "increasing the buffer pool size.\n"
      "It is also possible that"
      " in your Unix version\n"
      "fsync is very slow, or"
      " completely frozen inside\n"
      "the OS kernel. Then upgrading to"
      " a newer version\n"
      "of your operating system may help."
      " Look at the\n"
      "number of fsyncs in diagnostic info below.\n"
      "Pending flushes (fsync) log: %lu;"
      " buffer pool: %lu\n"
      "%lu OS file reads, %lu OS file writes,"
      " %lu OS fsyncs\n"
      "Starting InnoDB Monitor to print further\n"
      "diagnostics to the standard output.\n",
      (ulong)n_iterations,
      (ulong)fil_n_pending_log_flushes,
      (ulong)fil_n_pending_tablespace_flushes,
      (ulong)os_n_file_reads,
      (ulong)os_n_file_writes,
      (ulong)os_n_fsyncs
    );

    mon_value_was = srv_print_innodb_monitor;
    started_monitor = true;
    srv_print_innodb_monitor = true;
    os_event_set(srv_lock_timeout_thread_event);
  }

  /* No free block was found: try to flush the LRU list */

  buf_flush_free_margin();
  ++srv_buf_pool_wait_free;

  os_aio_simulated_wake_handler_threads();

  buf_pool_mutex_enter();

  if (buf_pool->m_lru_flush_ended > 0) {
    /* We have written pages in an LRU flush. To make the insert
    buffer more efficient, we try to move these pages to the free
    list. */

    buf_pool_mutex_exit();

    buf_LRU_try_free_flushed_blocks();
  } else {
    buf_pool_mutex_exit();
  }

  if (n_iterations > 10) {

    os_thread_sleep(500000);
  }

  n_iterations++;

  goto loop;
}

/** Moves the LRU_old pointer so that the length of the old blocks list
is inside the allowed limits. */
static void buf_LRU_old_adjust_len() {
  ut_a(buf_pool->m_lru_old);
  ut_ad(buf_pool_mutex_own());
  ut_ad(buf_LRU_old_ratio >= BUF_LRU_OLD_RATIO_MIN);
  ut_ad(buf_LRU_old_ratio <= BUF_LRU_OLD_RATIO_MAX);

  static_assert(
    BUF_LRU_OLD_RATIO_MIN * BUF_LRU_OLD_MIN_LEN > BUF_LRU_OLD_RATIO_DIV * (BUF_LRU_OLD_TOLERANCE + 5),
    "BUF_LRU_OLD_RATIO_MIN * BUF_LRU_OLD_MIN_LEN <= "
    "BUF_LRU_OLD_RATIO_DIV * (BUF_LRU_OLD_TOLERANCE + 5)"
  );

  auto old_len = buf_pool->m_lru_old_len;

  auto new_len = std::min(
    UT_LIST_GET_LEN(buf_pool->m_lru_list) * buf_LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
    UT_LIST_GET_LEN(buf_pool->m_lru_list) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN)
  );

  for (;;) {
    auto lru_old = buf_pool->m_lru_old;

    ut_a(lru_old->m_old);
    ut_ad(lru_old->m_in_lru_list);

    /* Update the LRU_old pointer if necessary */

    if (old_len + BUF_LRU_OLD_TOLERANCE < new_len) {

      buf_pool->m_lru_old = lru_old = UT_LIST_GET_PREV(m_lru_list, lru_old);

      old_len = ++buf_pool->m_lru_old_len;

      buf_page_set_old(lru_old, true);

    } else if (old_len > new_len + BUF_LRU_OLD_TOLERANCE) {

      buf_pool->m_lru_old = UT_LIST_GET_NEXT(m_lru_list, lru_old);

      --buf_pool->m_lru_old_len;

      old_len = buf_pool->m_lru_old_len;

      buf_page_set_old(lru_old, false);

    } else {
      return;
    }
  }
}

/** Initializes the old blocks pointer in the LRU list. This function should be
called when the LRU list grows to BUF_LRU_OLD_MIN_LEN length. */
static void buf_LRU_old_init() {
  ut_ad(buf_pool_mutex_own());
  ut_a(UT_LIST_GET_LEN(buf_pool->m_lru_list) == BUF_LRU_OLD_MIN_LEN);

  /* We first initialize all blocks in the LRU list as old and then use
  the adjust function to move the LRU_old pointer to the right
  position */

  for (auto bpage = UT_LIST_GET_LAST(buf_pool->m_lru_list);
       bpage != nullptr;
       bpage = UT_LIST_GET_PREV(m_lru_list, bpage)) {

    ut_ad(bpage->m_in_lru_list);
    ut_ad(buf_page_in_file(bpage));
    /* This loop temporarily violates the
    assertions of buf_page_set_old(). */
    bpage->m_old = true;
  }

  buf_pool->m_lru_old = UT_LIST_GET_FIRST(buf_pool->m_lru_list);
  buf_pool->m_lru_old_len = UT_LIST_GET_LEN(buf_pool->m_lru_list);

  buf_LRU_old_adjust_len();
}

/** Removes a block from the LRU list.
@param[in] bpage                Buffer lock to remove */
static void buf_LRU_remove_block(buf_page_t *bpage) {
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));

  ut_ad(bpage->m_in_lru_list);

  /* If the LRU_old pointer is defined and points to just this block,
  move it backward one step */

  if (unlikely(bpage == buf_pool->m_lru_old)) {

    /* Below: the previous block is guaranteed to exist,
    because the LRU_old pointer is only allowed to differ
    by BUF_LRU_OLD_TOLERANCE from strict
    buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV of the LRU
    list length. */
    buf_page_t *prev_bpage = UT_LIST_GET_PREV(m_lru_list, bpage);

    ut_a(prev_bpage);
    buf_pool->m_lru_old = prev_bpage;
    buf_page_set_old(prev_bpage, true);

    buf_pool->m_lru_old_len++;
  }

  /* Remove the block from the LRU list */
  UT_LIST_REMOVE(buf_pool->m_lru_list, bpage);
  ut_d(bpage->m_in_lru_list = false);

  /* If the LRU list is so short that LRU_old is not defined,
  clear the "old" flags and return */
  if (UT_LIST_GET_LEN(buf_pool->m_lru_list) < BUF_LRU_OLD_MIN_LEN) {

    for (bpage = UT_LIST_GET_FIRST(buf_pool->m_lru_list); bpage != nullptr; bpage = UT_LIST_GET_NEXT(m_lru_list, bpage)) {
      /* This loop temporarily violates the
      assertions of buf_page_set_old(). */
      bpage->m_old = false;
    }

    buf_pool->m_lru_old = nullptr;
    buf_pool->m_lru_old_len = 0;

    return;
  }

  ut_ad(buf_pool->m_lru_old);

  /* Update the LRU_old_len field if necessary */
  if (buf_page_is_old(bpage)) {

    buf_pool->m_lru_old_len--;
  }

  /* Adjust the length of the old block list if necessary */
  buf_LRU_old_adjust_len();
}

/** Adds a block to the LRU list end.
@param[in] bpage                Buffer lock to add */
static void buf_LRU_add_block_to_end_low(buf_page_t *bpage) {
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));

  ut_ad(!bpage->m_in_lru_list);
  UT_LIST_ADD_LAST(buf_pool->m_lru_list, bpage);
  ut_d(bpage->m_in_lru_list = true);

  if (UT_LIST_GET_LEN(buf_pool->m_lru_list) > BUF_LRU_OLD_MIN_LEN) {

    ut_ad(buf_pool->m_lru_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set_old(bpage, true);
    buf_pool->m_lru_old_len++;
    buf_LRU_old_adjust_len();

  } else if (UT_LIST_GET_LEN(buf_pool->m_lru_list) == BUF_LRU_OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    buf_LRU_old_init();
  } else {
    buf_page_set_old(bpage, buf_pool->m_lru_old != nullptr);
  }
}

/** Adds a block to the LRU list.
@param[in] bpage                Buffer lock to add
@param[in] old                  true if should be put to the old blocks in the LRU list,
                                else put to the start; if the LRU list is very short, the
				block is added to the start, regardless of this parameter */
static void buf_LRU_add_block_low(buf_page_t *bpage, bool old) {
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));
  ut_ad(!bpage->m_in_lru_list);

  if (!old || (UT_LIST_GET_LEN(buf_pool->m_lru_list) < BUF_LRU_OLD_MIN_LEN)) {

    UT_LIST_ADD_FIRST(buf_pool->m_lru_list, bpage);

    bpage->m_freed_page_clock = buf_pool->m_freed_page_clock;
  } else {
    UT_LIST_INSERT_AFTER(buf_pool->m_lru_list, buf_pool->m_lru_old, bpage);
    buf_pool->m_lru_old_len++;
  }

  ut_d(bpage->m_in_lru_list = true);

  if (UT_LIST_GET_LEN(buf_pool->m_lru_list) > BUF_LRU_OLD_MIN_LEN) {

    ut_ad(buf_pool->m_lru_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set_old(bpage, old);
    buf_LRU_old_adjust_len();

  } else if (UT_LIST_GET_LEN(buf_pool->m_lru_list) == BUF_LRU_OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    buf_LRU_old_init();
  } else {
    buf_page_set_old(bpage, buf_pool->m_lru_old != nullptr);
  }
}

void buf_LRU_add_block(buf_page_t *bpage, bool old) {
  buf_LRU_add_block_low(bpage, old);
}

void buf_LRU_make_block_young(buf_page_t *bpage) {
  ut_ad(buf_pool_mutex_own());

  if (bpage->m_old) {
    buf_pool->m_stat.n_pages_made_young++;
  }

  buf_LRU_remove_block(bpage);
  buf_LRU_add_block_low(bpage, false);
}

void buf_LRU_make_block_old(buf_page_t *bpage) {
  buf_LRU_remove_block(bpage);
  buf_LRU_add_block_to_end_low(bpage);
}

buf_lru_free_block_status buf_LRU_free_block(buf_page_t *bpage, bool *buf_pool_mutex_released) {
  auto block_mutex = buf_page_get_mutex(bpage);

  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(block_mutex));
  ut_ad(buf_page_in_file(bpage));
  ut_ad(bpage->m_in_lru_list);
  ut_ad(!bpage->m_in_flush_list == !bpage->m_oldest_modification);

  UNIV_MEM_ASSERT_RW(bpage, sizeof(*bpage));

  if (!buf_page_can_relocate(bpage)) {

    /* Do not free buffer-fixed or I/O-fixed blocks. */
    return BUF_LRU_NOT_FREED;
  }

  if (bpage->m_oldest_modification > 0) {
    return BUF_LRU_NOT_FREED;
  } else if (buf_LRU_block_remove_hashed_page(bpage) == BUF_BLOCK_REMOVE_HASH) {
    ut_a(bpage->m_buf_fix_count == 0);

    if (buf_pool_mutex_released) {
      *buf_pool_mutex_released = true;
    }

    buf_pool_mutex_exit();
    mutex_exit(block_mutex);

    /* The page was declared uninitialized by
    buf_LRU_block_remove_hashed_page().  We need to flag
    the contents of the page valid (which it still is) in
    order to avoid bogus Valgrind warnings.*/

    UNIV_MEM_VALID(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

    UNIV_MEM_INVALID(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

    buf_pool_mutex_enter();

    mutex_enter(block_mutex);

    buf_LRU_block_free_hashed_page((buf_block_t *)bpage);
  }

  return BUF_LRU_FREED;
}

void buf_LRU_block_free_non_file_page(buf_block_t *block) {
  ut_ad(block != nullptr);
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&block->m_mutex));

  switch (block->get_state()) {
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_READY_FOR_USE:
      break;
    default:
      ut_error;
  }

  ut_ad(!block->m_page.m_in_free_list);
  ut_ad(!block->m_page.m_in_flush_list);
  ut_ad(!block->m_page.m_in_lru_list);

  buf_block_set_state(block, BUF_BLOCK_NOT_USED);

  UNIV_MEM_ALLOC(block->m_frame, UNIV_PAGE_SIZE);

  auto frame{block->m_frame};

#ifdef UNIV_DEBUG
  /* Wipe contents of page to reveal possible stale pointers to it */
  memset(frame, 0x0, UNIV_PAGE_SIZE);
#else
  /* Wipe page_no and space_id */
  memset(frame + FIL_PAGE_OFFSET, 0xcafe, 4);
  memset(frame + FIL_PAGE_SPACE_ID, 0xcafe, 4);
#endif /* UNIV_DEBUG */

  UT_LIST_ADD_FIRST(buf_pool->m_free_list, &block->m_page);

  ut_d(block->m_page.m_in_free_list = true);

  UNIV_MEM_ASSERT_AND_FREE(block, UNIV_PAGE_SIZE);
}

static buf_page_state buf_LRU_block_remove_hashed_page(buf_page_t *bpage) {
  ut_ad(bpage != nullptr);
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  ut_a(buf_page_get_io_fix(bpage) == BUF_IO_NONE);
  ut_a(bpage->m_buf_fix_count == 0);

  UNIV_MEM_ASSERT_RW(bpage, sizeof *bpage);

  buf_LRU_remove_block(bpage);

  buf_pool->m_freed_page_clock += 1;

  switch (bpage->get_state()) {
    case BUF_BLOCK_FILE_PAGE:
      UNIV_MEM_ASSERT_W(bpage, sizeof(buf_block_t));
      UNIV_MEM_ASSERT_W(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

      buf_block_modify_clock_inc((buf_block_t *)bpage);
      break;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  auto hashed_bpage = buf_page_hash_get(bpage->m_space, bpage->m_page_no);

  if (unlikely(bpage != hashed_bpage)) {
    ib_logger(
      ib_stream,
      "Error: page %lu %lu not found"
      " in the hash table\n",
      (ulong)bpage->m_space,
      (ulong)bpage->m_page_no
    );
    if (hashed_bpage) {
      ib_logger(
        ib_stream,
        "In hash table we find block"
        " %p of %lu %lu which is not %p\n",
        (const void *)hashed_bpage,
        (ulong)hashed_bpage->m_space,
        (ulong)hashed_bpage->m_page_no,
        (const void *)bpage
      );
    }

#if defined UNIV_DEBUG
    mutex_exit(buf_page_get_mutex(bpage));
    buf_pool_mutex_exit();
    buf_print();
    buf_LRU_print();
    buf_validate();
    buf_LRU_validate();
#endif /* UNIV_DEBUG */
    ut_error;
  }

  ut_ad(bpage->m_in_page_hash);
  ut_d(bpage->m_in_page_hash = false);

  HASH_DELETE(buf_page_t, m_hash, buf_pool->m_page_hash, buf_page_address_fold(bpage->m_space, bpage->m_page_no), bpage);

  switch (bpage->get_state()) {
    case BUF_BLOCK_FILE_PAGE:
      memset(((buf_block_t *)bpage)->m_frame + FIL_PAGE_OFFSET, 0xff, 4);
      memset(((buf_block_t *)bpage)->m_frame + FIL_PAGE_SPACE_ID, 0xff, 4);

      UNIV_MEM_INVALID(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

      buf_page_set_state(bpage, BUF_BLOCK_REMOVE_HASH);

      return BUF_BLOCK_REMOVE_HASH;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  return BUF_BLOCK_REMOVE_HASH;
}

static void buf_LRU_block_free_hashed_page(buf_block_t *block) {
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&block->m_mutex));

  buf_block_set_state(block, BUF_BLOCK_MEMORY);

  buf_LRU_block_free_non_file_page(block);
}

ulint buf_LRU_old_ratio_update(ulint old_pct, bool adjust) {
  ulint ratio;

  ratio = old_pct * BUF_LRU_OLD_RATIO_DIV / 100;
  if (ratio < BUF_LRU_OLD_RATIO_MIN) {
    ratio = BUF_LRU_OLD_RATIO_MIN;
  } else if (ratio > BUF_LRU_OLD_RATIO_MAX) {
    ratio = BUF_LRU_OLD_RATIO_MAX;
  }

  if (adjust) {
    buf_pool_mutex_enter();

    if (ratio != buf_LRU_old_ratio) {
      buf_LRU_old_ratio = ratio;

      if (UT_LIST_GET_LEN(buf_pool->m_lru_list) >= BUF_LRU_OLD_MIN_LEN) {
        buf_LRU_old_adjust_len();
      }
    }

    buf_pool_mutex_exit();
  } else {
    buf_LRU_old_ratio = ratio;
  }

  /* the reverse of
  ratio = old_pct * BUF_LRU_OLD_RATIO_DIV / 100 */
  return ((ulint)(ratio * 100 / (double)BUF_LRU_OLD_RATIO_DIV + 0.5));
}

void buf_LRU_stat_update() {
  /* If we haven't started eviction yet then don't update stats. */
  if (buf_pool->m_freed_page_clock != 0) {
    buf_pool_mutex_enter();

    /* Update the index. */
    auto item = &buf_LRU_stat_arr[buf_LRU_stat_arr_ind];

    ++buf_LRU_stat_arr_ind;

    buf_LRU_stat_arr_ind %= BUF_LRU_STAT_N_INTERVAL;

    /* Add the current value and subtract the obsolete entry. */
    buf_LRU_stat_sum.io += buf_LRU_stat_cur.io - item->io;

    /* Put current entry in the array. */
    memcpy(item, &buf_LRU_stat_cur, sizeof *item);

    buf_pool_mutex_exit();
  }

  /* Clear the current entry. */
  memset(&buf_LRU_stat_cur, 0x0, sizeof buf_LRU_stat_cur);
}

#if defined UNIV_DEBUG
bool buf_LRU_validate() {
  buf_page_t *bpage;
  ulint old_len;
  ulint new_len;

  ut_ad(buf_pool);
  buf_pool_mutex_enter();

  if (UT_LIST_GET_LEN(buf_pool->m_lru_list) >= BUF_LRU_OLD_MIN_LEN) {

    ut_a(buf_pool->m_lru_old);
    old_len = buf_pool->m_lru_old_len;
    new_len = ut_min(
      UT_LIST_GET_LEN(buf_pool->m_lru_list) * buf_LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
      UT_LIST_GET_LEN(buf_pool->m_lru_list) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN)
    );
    ut_a(old_len >= new_len - BUF_LRU_OLD_TOLERANCE);
    ut_a(old_len <= new_len + BUF_LRU_OLD_TOLERANCE);
  }

  UT_LIST_CHECK(buf_pool->m_lru_list);

  bpage = UT_LIST_GET_FIRST(buf_pool->m_lru_list);

  old_len = 0;

  while (bpage != nullptr) {

    switch (bpage->get_state()) {
      default:
      case BUF_BLOCK_NOT_USED:
      case BUF_BLOCK_READY_FOR_USE:
      case BUF_BLOCK_MEMORY:
      case BUF_BLOCK_REMOVE_HASH:
        ut_error;
        break;
      case BUF_BLOCK_FILE_PAGE:
        break;
    }

    if (buf_page_is_old(bpage)) {
      const buf_page_t *prev = UT_LIST_GET_PREV(m_lru_list, bpage);
      const buf_page_t *next = UT_LIST_GET_NEXT(m_lru_list, bpage);

      if (!old_len++) {
        ut_a(buf_pool->m_lru_old == bpage);
      } else {
        ut_a(!prev || buf_page_is_old(prev));
      }

      ut_a(!next || buf_page_is_old(next));
    }

    bpage = UT_LIST_GET_NEXT(m_lru_list, bpage);
  }

  ut_a(buf_pool->m_lru_old_len == old_len);

  auto check = [](const buf_page_t *page) { ut_ad(page->m_in_free_list); };
  ut_list_validate(buf_pool->m_free_list, check);

  for (bpage = UT_LIST_GET_FIRST(buf_pool->m_free_list);
       bpage != nullptr;
       bpage = UT_LIST_GET_NEXT(m_list, bpage)) {

    ut_a(bpage->get_state() == BUF_BLOCK_NOT_USED);
  }

  buf_pool_mutex_exit();

  return (true);
}

void buf_LRU_print() {
  const buf_page_t *bpage;

  ut_ad(buf_pool);
  buf_pool_mutex_enter();

  bpage = UT_LIST_GET_FIRST(buf_pool->m_lru_list);

  while (bpage != nullptr) {

    ib_logger(ib_stream, "BLOCK space %lu page %lu ", (ulong)bpage->get_space(), (ulong)buf_page_get_page_no(bpage));

    if (buf_page_is_old(bpage)) {
      ib_logger(ib_stream, "old ");
    }

    if (bpage->m_buf_fix_count > 0) {
      ib_logger(ib_stream, "buffix count %lu ", (ulong)bpage->m_buf_fix_count);
    }

    if (buf_page_get_io_fix(bpage)) {
      ib_logger(ib_stream, "io_fix %lu ", (ulong)buf_page_get_io_fix(bpage));
    }

    if (bpage->m_oldest_modification > 0) {
      ib_logger(ib_stream, "modif. ");
    }

    switch (bpage->get_state()) {
      const byte *frame;
      case BUF_BLOCK_FILE_PAGE:
        frame = buf_block_get_frame((buf_block_t *)bpage);
        ib_logger(
          ib_stream,
          "\ntype %lu"
          " index id %lu\n",
          (ulong)fil_page_get_type(frame),
          (ulong)btr_page_get_index_id(frame)
        );
        break;

      default:
        ib_logger(ib_stream, "\n!state %lu!\n", (ulong)bpage->get_state());
        break;
    }

    bpage = UT_LIST_GET_NEXT(m_lru_list, bpage);
  }

  buf_pool_mutex_exit();
}
#endif /* UNIV_DEBUG */
