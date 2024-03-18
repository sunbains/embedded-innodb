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

/** When dropping the search hash index entries before deleting an ibd
file, we build a local array of pages belonging to that tablespace
in the buffer pool. Following is the size of that array. */
constexpr ulint BUF_LRU_DROP_SEARCH_HASH_SIZE = 1024;

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

/** When doing a DROP TABLE/DISCARD TABLESPACE we have to drop all page
hash index entries belonging to that table. This function tries to
do that in batch. Note that this is a 'best effort' attempt and does
not guarantee that ALL hash entries will be removed. */
static void buf_LRU_drop_page_hash_for_tablespace(ulint id) /*!< in: space id */
{
  buf_page_t *bpage;
  ulint *page_arr;
  ulint num_entries;

  page_arr = (ulint *)ut_malloc(sizeof(ulint) * BUF_LRU_DROP_SEARCH_HASH_SIZE);
  buf_pool_mutex_enter();

scan_again:
  num_entries = 0;
  bpage = UT_LIST_GET_LAST(buf_pool->LRU);

  while (bpage != nullptr) {
    mutex_t *block_mutex = buf_page_get_mutex(bpage);
    buf_page_t *prev_bpage;

    mutex_enter(block_mutex);
    prev_bpage = UT_LIST_GET_PREV(LRU, bpage);

    ut_a(buf_page_in_file(bpage));

    if (buf_page_get_state(bpage) != BUF_BLOCK_FILE_PAGE || bpage->space != id || bpage->buf_fix_count > 0 || bpage->io_fix != BUF_IO_NONE) {
      /* We leave the fixed pages as is in this scan.
      To be dealt with later in the final scan. */
      mutex_exit(block_mutex);
      goto next_page;
    }

    if (((buf_block_t *)bpage)->is_hashed) {

      /* Store the offset(i.e.: page_no) in the array
      so that we can drop hash index in a batch
      later. */
      page_arr[num_entries] = bpage->offset;
      mutex_exit(block_mutex);
      ut_a(num_entries < BUF_LRU_DROP_SEARCH_HASH_SIZE);
      ++num_entries;

      if (num_entries < BUF_LRU_DROP_SEARCH_HASH_SIZE) {
        goto next_page;
      }
      /* Array full. We release the buf_pool_mutex to
      obey the latching order. */
      buf_pool_mutex_exit();

      num_entries = 0;
      buf_pool_mutex_enter();
    } else {
      mutex_exit(block_mutex);
    }

  next_page:
    /* Note that we may have released the buf_pool mutex
    above after reading the prev_bpage during processing
    of a page_hash_batch (i.e.: when the array was full).
    This means that prev_bpage can change in LRU list.
    This is OK because this function is a 'best effort'
    to drop as many search hash entries as possible and
    it does not guarantee that ALL such entries will be
    dropped. */
    bpage = prev_bpage;

    /* If, however, bpage has been removed from LRU list
    to the free list then we should restart the scan.
    bpage->state is protected by buf_pool mutex. */
    if (bpage && !buf_page_in_file(bpage)) {
      ut_a(num_entries == 0);
      goto scan_again;
    }
  }

  buf_pool_mutex_exit();

  /* Drop any remaining batch of search hashed pages. */
  ut_free(page_arr);
}

/** Invalidates all pages belonging to a given tablespace when we are deleting
the data file(s) of that tablespace. */

void buf_LRU_invalidate_tablespace(ulint id) /*!< in: space id */
{
  buf_page_t *bpage;
  bool all_freed;

  /* Before we attempt to drop pages one by one we first
  attempt to drop page hash index entries in batches to make
  it more efficient. The batching attempt is a best effort
  attempt and does not guarantee that all pages hash entries
  will be dropped. We get rid of remaining page hash entries
  one by one below. */
  buf_LRU_drop_page_hash_for_tablespace(id);

scan_again:
  buf_pool_mutex_enter();

  all_freed = true;

  bpage = UT_LIST_GET_LAST(buf_pool->LRU);

  while (bpage != nullptr) {
    ut_a(buf_page_in_file(bpage));

    auto prev_bpage = UT_LIST_GET_PREV(LRU, bpage);

    /* bpage->space and bpage->io_fix are protected by
    buf_pool_mutex and block_mutex.  It is safe to check
    them while holding buf_pool_mutex only. */

    if (buf_page_get_space(bpage) != id) {
      /* Skip this block, as it does not belong to
      the space that is being invalidated. */
    } else if (buf_page_get_io_fix(bpage) != BUF_IO_NONE) {
      /* We cannot remove this page during this scan
      yet; maybe the system is currently reading it
      in, or flushing the modifications to the file */

      all_freed = false;
    } else {
      mutex_t *block_mutex = buf_page_get_mutex(bpage);
      mutex_enter(block_mutex);

      if (bpage->buf_fix_count > 0) {

        /* We cannot remove this page during
        this scan yet; maybe the system is
        currently reading it in, or flushing
        the modifications to the file */

        all_freed = false;

        goto next_page;
      }

#ifdef UNIV_DEBUG
      if (buf_debug_prints) {
        ib_logger(ib_stream, "Dropping space %lu page %lu\n", (ulong)buf_page_get_space(bpage), (ulong)buf_page_get_page_no(bpage));
      }
#endif
      if (buf_page_get_state(bpage) != BUF_BLOCK_FILE_PAGE) {
        /* This is a compressed-only block
        descriptor.  Ensure that prev_bpage
        cannot be relocated when bpage is freed. */
        if (likely(prev_bpage != nullptr)) {
          switch (buf_page_get_state(prev_bpage)) {
            case BUF_BLOCK_FILE_PAGE:
              /* Descriptors of uncompressed
            blocks will not be relocated,
            because we are holding the
            buf_pool_mutex. */
              break;
            default:
              ut_error;
          }
        }
      } else if (((buf_block_t *)bpage)->is_hashed) {

        buf_pool_mutex_exit();

        mutex_exit(block_mutex);

        /* Note that the following call will acquire
        an S-latch on the page */

        goto scan_again;
      }

      if (bpage->oldest_modification != 0) {

        buf_flush_remove(bpage);
      }

      /* Remove from the LRU list. */
      buf_LRU_block_remove_hashed_page(bpage);
      buf_LRU_block_free_hashed_page((buf_block_t *)bpage);

    next_page:
      mutex_exit(block_mutex);
    }

    bpage = prev_bpage;
  }

  buf_pool_mutex_exit();

  if (!all_freed) {
    os_thread_sleep(20000);

    goto scan_again;
  }
}

/** Try to free a clean page from the common LRU list.
@return	true if freed */
static bool buf_LRU_free_from_common_LRU_list(ulint n_iterations) /*!< in: how many times this has been called
                        repeatedly without result: a high value means
                        that we should search farther; if
                        n_iterations < 10, then we search
                        n_iterations / 10 * buf_pool->curr_size
                        pages from the end of the LRU list */
{
  buf_page_t *bpage;
  ulint distance;

  ut_ad(buf_pool_mutex_own());

  distance = 100 + (n_iterations * buf_pool->curr_size) / 10;

  for (bpage = UT_LIST_GET_LAST(buf_pool->LRU); likely(bpage != nullptr) && likely(distance > 0);
       bpage = UT_LIST_GET_PREV(LRU, bpage), distance--) {

    enum buf_lru_free_block_status freed;
    unsigned accessed;
    mutex_t *block_mutex = buf_page_get_mutex(bpage);

    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

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
          ++buf_pool->stat.n_ra_pages_evicted;
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
    buf_pool->LRU_flush_ended = 0;
  } else if (buf_pool->LRU_flush_ended > 0) {
    buf_pool->LRU_flush_ended--;
  }

  buf_pool_mutex_exit();

  return freed;
}

/** Tries to remove LRU flushed blocks from the end of the LRU list and put them
to the free list. This is beneficial for the efficiency of the insert buffer
operation, as flushed pages from non-unique non-clustered indexes are here
taken out of the buffer pool, and their inserts redirected to the insert
buffer. Otherwise, the flushed blocks could get modified again before read
operations need new buffer blocks, and the i/o work done in flushing would be
wasted. */

void buf_LRU_try_free_flushed_blocks(void) {
  buf_pool_mutex_enter();

  while (buf_pool->LRU_flush_ended > 0) {

    buf_pool_mutex_exit();

    buf_LRU_search_and_free_block(1);

    buf_pool_mutex_enter();
  }

  buf_pool_mutex_exit();
}

/** Returns true if less than 25 % of the buffer pool is available. This can be
used in heuristics to prevent huge transactions eating up the whole buffer
pool for their locks.
@return	true if less than 25 % of buffer pool left */

bool buf_LRU_buf_pool_running_out(void) {
  bool ret = false;

  buf_pool_mutex_enter();

  if (!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU) < buf_pool->curr_size / 4) {

    ret = true;
  }

  buf_pool_mutex_exit();

  return (ret);
}

/** Returns a free block from the buf_pool.  The block is taken off the
free list.  If it is empty, returns nullptr.
@return	a free control block, or nullptr if the buf_block->free list is empty */

buf_block_t *buf_LRU_get_free_only(void) {
  buf_block_t *block;

  ut_ad(buf_pool_mutex_own());

  block = (buf_block_t *)UT_LIST_GET_FIRST(buf_pool->free);

  if (block) {
    ut_ad(block->page.in_free_list);
    ut_d(block->page.in_free_list = false);
    ut_ad(!block->page.in_flush_list);
    ut_ad(!block->page.in_LRU_list);
    ut_a(!buf_page_in_file(&block->page));
    UT_LIST_REMOVE(buf_pool->free, (&block->page));

    mutex_enter(&block->mutex);

    buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);
    UNIV_MEM_ALLOC(block->frame, UNIV_PAGE_SIZE);

    mutex_exit(&block->mutex);
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

  if (!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU) < buf_pool->curr_size / 20) {

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  ERROR: over 95 percent of the buffer pool is occupied by lock heaps!"
      " Check that your transactions do not set too many row locks."
      " Your buffer pool size is %lu MB. Maybe you should make the buffer pool bigger?"
      " We intentionally generate a seg fault to print a stack trace on Linux!\n",
      (ulong)(buf_pool->curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
    );

    ut_error;

  } else if (!recv_recovery_on && (UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU)) < buf_pool->curr_size / 3) {

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
        (ulong)(buf_pool->curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
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

  if (buf_pool->LRU_flush_ended > 0) {
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
static void buf_LRU_old_adjust_len(void) {
  ulint old_len;
  ulint new_len;

  ut_a(buf_pool->LRU_old);
  ut_ad(buf_pool_mutex_own());
  ut_ad(buf_LRU_old_ratio >= BUF_LRU_OLD_RATIO_MIN);
  ut_ad(buf_LRU_old_ratio <= BUF_LRU_OLD_RATIO_MAX);

  static_assert(
    BUF_LRU_OLD_RATIO_MIN * BUF_LRU_OLD_MIN_LEN > BUF_LRU_OLD_RATIO_DIV * (BUF_LRU_OLD_TOLERANCE + 5),
    "BUF_LRU_OLD_RATIO_MIN * BUF_LRU_OLD_MIN_LEN <= "
    "BUF_LRU_OLD_RATIO_DIV * (BUF_LRU_OLD_TOLERANCE + 5)"
  );

#ifdef UNIV_LRU_DEBUG
  /* buf_pool->LRU_old must be the first item in the LRU list
  whose "old" flag is set. */
  ut_a(buf_pool->LRU_old->old);
  ut_a(!UT_LIST_GET_PREV(LRU, buf_pool->LRU_old) || !UT_LIST_GET_PREV(LRU, buf_pool->LRU_old)->old);
  ut_a(!UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old) || UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old)->old);
#endif /* UNIV_LRU_DEBUG */

  old_len = buf_pool->LRU_old_len;
  new_len = ut_min(
    UT_LIST_GET_LEN(buf_pool->LRU) * buf_LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
    UT_LIST_GET_LEN(buf_pool->LRU) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN)
  );

  for (;;) {
    buf_page_t *LRU_old = buf_pool->LRU_old;

    ut_a(LRU_old);
    ut_ad(LRU_old->in_LRU_list);
#ifdef UNIV_LRU_DEBUG
    ut_a(LRU_old->old);
#endif /* UNIV_LRU_DEBUG */

    /* Update the LRU_old pointer if necessary */

    if (old_len + BUF_LRU_OLD_TOLERANCE < new_len) {

      buf_pool->LRU_old = LRU_old = UT_LIST_GET_PREV(LRU, LRU_old);
#ifdef UNIV_LRU_DEBUG
      ut_a(!LRU_old->old);
#endif /* UNIV_LRU_DEBUG */
      old_len = ++buf_pool->LRU_old_len;
      buf_page_set_old(LRU_old, true);

    } else if (old_len > new_len + BUF_LRU_OLD_TOLERANCE) {

      buf_pool->LRU_old = UT_LIST_GET_NEXT(LRU, LRU_old);
      old_len = --buf_pool->LRU_old_len;
      buf_page_set_old(LRU_old, false);
    } else {
      return;
    }
  }
}

/** Initializes the old blocks pointer in the LRU list. This function should be
called when the LRU list grows to BUF_LRU_OLD_MIN_LEN length. */
static void buf_LRU_old_init(void) {
  buf_page_t *bpage;

  ut_ad(buf_pool_mutex_own());
  ut_a(UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN);

  /* We first initialize all blocks in the LRU list as old and then use
  the adjust function to move the LRU_old pointer to the right
  position */

  for (bpage = UT_LIST_GET_LAST(buf_pool->LRU); bpage != nullptr; bpage = UT_LIST_GET_PREV(LRU, bpage)) {
    ut_ad(bpage->in_LRU_list);
    ut_ad(buf_page_in_file(bpage));
    /* This loop temporarily violates the
    assertions of buf_page_set_old(). */
    bpage->old = true;
  }

  buf_pool->LRU_old = UT_LIST_GET_FIRST(buf_pool->LRU);
  buf_pool->LRU_old_len = UT_LIST_GET_LEN(buf_pool->LRU);

  buf_LRU_old_adjust_len();
}

/** Removes a block from the LRU list. */
static void buf_LRU_remove_block(buf_page_t *bpage) /*!< in: control block */
{
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));

  ut_ad(bpage->in_LRU_list);

  /* If the LRU_old pointer is defined and points to just this block,
  move it backward one step */

  if (unlikely(bpage == buf_pool->LRU_old)) {

    /* Below: the previous block is guaranteed to exist,
    because the LRU_old pointer is only allowed to differ
    by BUF_LRU_OLD_TOLERANCE from strict
    buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV of the LRU
    list length. */
    buf_page_t *prev_bpage = UT_LIST_GET_PREV(LRU, bpage);

    ut_a(prev_bpage);
#ifdef UNIV_LRU_DEBUG
    ut_a(!prev_bpage->old);
#endif /* UNIV_LRU_DEBUG */
    buf_pool->LRU_old = prev_bpage;
    buf_page_set_old(prev_bpage, true);

    buf_pool->LRU_old_len++;
  }

  /* Remove the block from the LRU list */
  UT_LIST_REMOVE(buf_pool->LRU, bpage);
  ut_d(bpage->in_LRU_list = false);

  /* If the LRU list is so short that LRU_old is not defined,
  clear the "old" flags and return */
  if (UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN) {

    for (bpage = UT_LIST_GET_FIRST(buf_pool->LRU); bpage != nullptr; bpage = UT_LIST_GET_NEXT(LRU, bpage)) {
      /* This loop temporarily violates the
      assertions of buf_page_set_old(). */
      bpage->old = false;
    }

    buf_pool->LRU_old = nullptr;
    buf_pool->LRU_old_len = 0;

    return;
  }

  ut_ad(buf_pool->LRU_old);

  /* Update the LRU_old_len field if necessary */
  if (buf_page_is_old(bpage)) {

    buf_pool->LRU_old_len--;
  }

  /* Adjust the length of the old block list if necessary */
  buf_LRU_old_adjust_len();
}

/** Adds a block to the LRU list end. */
static void buf_LRU_add_block_to_end_low(buf_page_t *bpage) /*!< in: control block */
{
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));

  ut_ad(!bpage->in_LRU_list);
  UT_LIST_ADD_LAST(buf_pool->LRU, bpage);
  ut_d(bpage->in_LRU_list = true);

  if (UT_LIST_GET_LEN(buf_pool->LRU) > BUF_LRU_OLD_MIN_LEN) {

    ut_ad(buf_pool->LRU_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set_old(bpage, true);
    buf_pool->LRU_old_len++;
    buf_LRU_old_adjust_len();

  } else if (UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    buf_LRU_old_init();
  } else {
    buf_page_set_old(bpage, buf_pool->LRU_old != nullptr);
  }
}

/** Adds a block to the LRU list. */
static void buf_LRU_add_block_low(
  buf_page_t *bpage, /*!< in: control block */
  bool old
) /*!< in: true if should be put to the old blocks
                                 in the LRU list, else put to the start; if the
                                 LRU list is very short, the block is added to
                                 the start, regardless of this parameter */
{
  ut_ad(buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(buf_page_in_file(bpage));
  ut_ad(!bpage->in_LRU_list);

  if (!old || (UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN)) {

    UT_LIST_ADD_FIRST(buf_pool->LRU, bpage);

    bpage->freed_page_clock = buf_pool->freed_page_clock;
  } else {
#ifdef UNIV_LRU_DEBUG
    /* buf_pool->LRU_old must be the first item in the LRU list
    whose "old" flag is set. */
    ut_a(buf_pool->LRU_old->old);
    ut_a(!UT_LIST_GET_PREV(LRU, buf_pool->LRU_old) || !UT_LIST_GET_PREV(LRU, buf_pool->LRU_old)->old);
    ut_a(!UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old) || UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old)->old);
#endif /* UNIV_LRU_DEBUG */
    UT_LIST_INSERT_AFTER(buf_pool->LRU, buf_pool->LRU_old, bpage);
    buf_pool->LRU_old_len++;
  }

  ut_d(bpage->in_LRU_list = true);

  if (UT_LIST_GET_LEN(buf_pool->LRU) > BUF_LRU_OLD_MIN_LEN) {

    ut_ad(buf_pool->LRU_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set_old(bpage, old);
    buf_LRU_old_adjust_len();

  } else if (UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    buf_LRU_old_init();
  } else {
    buf_page_set_old(bpage, buf_pool->LRU_old != nullptr);
  }
}

void buf_LRU_add_block(buf_page_t *bpage, bool old) {
  buf_LRU_add_block_low(bpage, old);
}

void buf_LRU_make_block_young(buf_page_t *bpage) {
  ut_ad(buf_pool_mutex_own());

  if (bpage->old) {
    buf_pool->stat.n_pages_made_young++;
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
  ut_ad(bpage->in_LRU_list);
  ut_ad(!bpage->in_flush_list == !bpage->oldest_modification);

  UNIV_MEM_ASSERT_RW(bpage, sizeof(*bpage));

  if (!buf_page_can_relocate(bpage)) {

    /* Do not free buffer-fixed or I/O-fixed blocks. */
    return BUF_LRU_NOT_FREED;
  }

  if (bpage->oldest_modification > 0) {
    return BUF_LRU_NOT_FREED;
  } else if (buf_LRU_block_remove_hashed_page(bpage) == BUF_BLOCK_REMOVE_HASH) {
    ut_a(bpage->buf_fix_count == 0);

    if (buf_pool_mutex_released) {
      *buf_pool_mutex_released = true;
    }

    buf_pool_mutex_exit();
    mutex_exit(block_mutex);

    /* The page was declared uninitialized by
    buf_LRU_block_remove_hashed_page().  We need to flag
    the contents of the page valid (which it still is) in
    order to avoid bogus Valgrind warnings.*/

    UNIV_MEM_VALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

    UNIV_MEM_INVALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

    buf_pool_mutex_enter();

    mutex_enter(block_mutex);

    buf_LRU_block_free_hashed_page((buf_block_t *)bpage);
  }

  return BUF_LRU_FREED;
}

void buf_LRU_block_free_non_file_page(buf_block_t *block) {
  ut_ad(block != nullptr);
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&block->mutex));

  switch (buf_block_get_state(block)) {
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_READY_FOR_USE:
      break;
    default:
      ut_error;
  }

  ut_ad(!block->page.in_free_list);
  ut_ad(!block->page.in_flush_list);
  ut_ad(!block->page.in_LRU_list);

  buf_block_set_state(block, BUF_BLOCK_NOT_USED);

  UNIV_MEM_ALLOC(block->frame, UNIV_PAGE_SIZE);

#ifdef UNIV_DEBUG
  /* Wipe contents of page to reveal possible stale pointers to it */
  memset(block->frame, '\0', UNIV_PAGE_SIZE);
#else
  /* Wipe page_no and space_id */
  memset(block->frame + FIL_PAGE_OFFSET, 0xfe, 4);
  memset(block->frame + FIL_PAGE_PACE_ID, 0xfe, 4);
#endif /* UNIV_DEBUG */

  UT_LIST_ADD_FIRST(buf_pool->free, (&block->page));

  ut_d(block->page.in_free_list = true);

  UNIV_MEM_ASSERT_AND_FREE(block->frame, UNIV_PAGE_SIZE);
}

static buf_page_state buf_LRU_block_remove_hashed_page(buf_page_t *bpage) {
  ut_ad(bpage != nullptr);
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  ut_a(buf_page_get_io_fix(bpage) == BUF_IO_NONE);
  ut_a(bpage->buf_fix_count == 0);

  UNIV_MEM_ASSERT_RW(bpage, sizeof *bpage);

  buf_LRU_remove_block(bpage);

  buf_pool->freed_page_clock += 1;

  switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_FILE_PAGE:
      UNIV_MEM_ASSERT_W(bpage, sizeof(buf_block_t));
      UNIV_MEM_ASSERT_W(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

      buf_block_modify_clock_inc((buf_block_t *)bpage);
      break;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  auto hashed_bpage = buf_page_hash_get(bpage->space, bpage->offset);

  if (unlikely(bpage != hashed_bpage)) {
    ib_logger(
      ib_stream,
      "Error: page %lu %lu not found"
      " in the hash table\n",
      (ulong)bpage->space,
      (ulong)bpage->offset
    );
    if (hashed_bpage) {
      ib_logger(
        ib_stream,
        "In hash table we find block"
        " %p of %lu %lu which is not %p\n",
        (const void *)hashed_bpage,
        (ulong)hashed_bpage->space,
        (ulong)hashed_bpage->offset,
        (const void *)bpage
      );
    }

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
    mutex_exit(buf_page_get_mutex(bpage));
    buf_pool_mutex_exit();
    buf_print();
    buf_LRU_print();
    buf_validate();
    buf_LRU_validate();
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
    ut_error;
  }

  ut_ad(bpage->in_page_hash);
  ut_d(bpage->in_page_hash = false);

  HASH_DELETE(buf_page_t, hash, buf_pool->page_hash, buf_page_address_fold(bpage->space, bpage->offset), bpage);

  switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_FILE_PAGE:
      memset(((buf_block_t *)bpage)->frame + FIL_PAGE_OFFSET, 0xff, 4);
      memset(((buf_block_t *)bpage)->frame + FIL_PAGE_SPACE_ID, 0xff, 4);

      UNIV_MEM_INVALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

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
  ut_ad(mutex_own(&block->mutex));

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

      if (UT_LIST_GET_LEN(buf_pool->LRU) >= BUF_LRU_OLD_MIN_LEN) {
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
  if (buf_pool->freed_page_clock != 0) {
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

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
bool buf_LRU_validate() {
  buf_page_t *bpage;
  ulint old_len;
  ulint new_len;

  ut_ad(buf_pool);
  buf_pool_mutex_enter();

  if (UT_LIST_GET_LEN(buf_pool->LRU) >= BUF_LRU_OLD_MIN_LEN) {

    ut_a(buf_pool->LRU_old);
    old_len = buf_pool->LRU_old_len;
    new_len = ut_min(
      UT_LIST_GET_LEN(buf_pool->LRU) * buf_LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
      UT_LIST_GET_LEN(buf_pool->LRU) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN)
    );
    ut_a(old_len >= new_len - BUF_LRU_OLD_TOLERANCE);
    ut_a(old_len <= new_len + BUF_LRU_OLD_TOLERANCE);
  }

  UT_LIST_CHECK(buf_pool->LRU);

  bpage = UT_LIST_GET_FIRST(buf_pool->LRU);

  old_len = 0;

  while (bpage != nullptr) {

    switch (buf_page_get_state(bpage)) {
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
      const buf_page_t *prev = UT_LIST_GET_PREV(LRU, bpage);
      const buf_page_t *next = UT_LIST_GET_NEXT(LRU, bpage);

      if (!old_len++) {
        ut_a(buf_pool->LRU_old == bpage);
      } else {
        ut_a(!prev || buf_page_is_old(prev));
      }

      ut_a(!next || buf_page_is_old(next));
    }

    bpage = UT_LIST_GET_NEXT(LRU, bpage);
  }

  ut_a(buf_pool->LRU_old_len == old_len);

  auto check = [](const buf_page_t *page) {
    ut_ad(page->in_free_list);
  };
  ut_list_validate(buf_pool->free, check);

  for (bpage = UT_LIST_GET_FIRST(buf_pool->free); bpage != nullptr; bpage = UT_LIST_GET_NEXT(list, bpage)) {

    ut_a(buf_page_get_state(bpage) == BUF_BLOCK_NOT_USED);
  }

  buf_pool_mutex_exit();

  return (true);
}
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#if defined UNIV_DEBUG_PRINT || defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** Prints the LRU list. */

void buf_LRU_print(void) {
  const buf_page_t *bpage;

  ut_ad(buf_pool);
  buf_pool_mutex_enter();

  bpage = UT_LIST_GET_FIRST(buf_pool->LRU);

  while (bpage != nullptr) {

    ib_logger(ib_stream, "BLOCK space %lu page %lu ", (ulong)buf_page_get_space(bpage), (ulong)buf_page_get_page_no(bpage));

    if (buf_page_is_old(bpage)) {
      ib_logger(ib_stream, "old ");
    }

    if (bpage->buf_fix_count) {
      ib_logger(ib_stream, "buffix count %lu ", (ulong)bpage->buf_fix_count);
    }

    if (buf_page_get_io_fix(bpage)) {
      ib_logger(ib_stream, "io_fix %lu ", (ulong)buf_page_get_io_fix(bpage));
    }

    if (bpage->oldest_modification) {
      ib_logger(ib_stream, "modif. ");
    }

    switch (buf_page_get_state(bpage)) {
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
        ib_logger(ib_stream, "\n!state %lu!\n", (ulong)buf_page_get_state(bpage));
        break;
    }

    bpage = UT_LIST_GET_NEXT(LRU, bpage);
  }

  buf_pool_mutex_exit();
}
#endif /* UNIV_DEBUG_PRINT || UNIV_DEBUG || UNIV_BUF_DEBUG */
