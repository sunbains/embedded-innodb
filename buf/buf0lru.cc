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
#include "fil0fil.h"
#include "log0recv.h"
#include "os0file.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "ut0lst.h"

/** The number of blocks from the LRU_old pointer onward, including
the block pointed to, must be Buf_LRU::old_ratio/OLD_RATIO_DIV
of the whole LRU list length, except that the tolerance defined below
is allowed. Note that the tolerance must be small enough such that for
even the OLD_MIN_LEN long LRU list, the LRU_old pointer is not
allowed to point to either end of the LRU list. */
constexpr ulint OLD_TOLERANCE = 20;

/** The minimum amount of non-old blocks when the LRU_old list exists
(that is, when there are more than LD_MIN_LEN blocks).
@see Buf_LRU::old_adjust_len */
constexpr ulint NON_MIN_LEN = 5;

static_assert(NON_MIN_LEN < Buf_LRU::OLD_MIN_LEN, "error Buf_LRU::NON_MIN_LEN >= Buf_LRU::OLD_MIN_LEN");

ulint Buf_LRU::s_old_ratio{};
ulint Buf_LRU::s_old_threshold_ms{};

void Buf_LRU::invalidate_tablespace(space_id_t id) {
  bool all_freed{};

  while (!all_freed) {
    buf_pool_mutex_enter();

    all_freed = true;

    auto bpage = UT_LIST_GET_LAST(srv_buf_pool->m_LRU_list);

    while (bpage != nullptr) {
      ut_a(bpage->in_file());

      auto prev_bpage = UT_LIST_GET_PREV(m_LRU_list, bpage);

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
            srv_buf_pool->m_flusher->remove(bpage);
          }

          /* Remove from the LRU list. */
          block_remove_hashed_page(bpage);

          {
            /* We can't cast it using buf_page_get_block() because of the checks there. */
            auto block{reinterpret_cast<buf_block_t *>(bpage)};
            block_free_hashed_page(block);
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

bool Buf_LRU::free_from_common_LRU_list(ulint n_iterations) {
  ut_ad(buf_pool_mutex_own());

  auto distance = 100 + (n_iterations * srv_buf_pool->m_curr_size) / 10;

  for (auto bpage = UT_LIST_GET_LAST(srv_buf_pool->m_LRU_list); likely(bpage != nullptr) && likely(distance > 0);
       bpage = UT_LIST_GET_PREV(m_LRU_list, bpage), distance--) {

    auto block_mutex = buf_page_get_mutex(bpage);

    ut_ad(bpage->m_in_LRU_list);
    ut_ad(bpage->in_file());

    mutex_enter(block_mutex);

    auto accessed = buf_page_is_accessed(bpage);
    auto block_status = free_block(bpage, nullptr);

    mutex_exit(block_mutex);

    switch (block_status) {
      case Block_status::FREED:
        /* Keep track of pages that are evicted without ever being accessed.
	his gives us a measure of the effectiveness of readahead */
        if (!accessed) {
          ++srv_buf_pool->m_stat.n_ra_pages_evicted;
        }
        return true;

      case Block_status::NOT_FREED:
        /* The block was dirty, buffer-fixed, or I/O-fixed.  Keep looking. */
        continue;

      case Block_status::CANNOT_RELOCATE:
        /* This should never occur, because we want to discard the compressed page too. */
        break;
    }

    ut_error;
  }

  return false;
}

bool Buf_LRU::search_and_free_block(ulint n_iterations) {
  buf_pool_mutex_enter();

  auto freed = free_from_common_LRU_list(n_iterations);

  if (!freed) {
    srv_buf_pool->m_LRU_flush_ended = 0;
  } else if (srv_buf_pool->m_LRU_flush_ended > 0) {
    --srv_buf_pool->m_LRU_flush_ended;
  }

  buf_pool_mutex_exit();

  return freed;
}

void Buf_LRU::try_free_flushed_blocks() {
  buf_pool_mutex_enter();

  while (srv_buf_pool->m_LRU_flush_ended > 0) {

    buf_pool_mutex_exit();

    search_and_free_block(1);

    buf_pool_mutex_enter();
  }

  buf_pool_mutex_exit();
}

bool Buf_LRU::buf_pool_running_out() {
  buf_pool_mutex_enter();

  auto ret = !recv_recovery_on &&
             UT_LIST_GET_LEN(srv_buf_pool->m_free_list) + UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) < srv_buf_pool->m_curr_size / 4;

  buf_pool_mutex_exit();

  return ret;
}

buf_block_t *Buf_LRU::get_free_only() {
  ut_ad(buf_pool_mutex_own());

  auto block = (buf_block_t *)UT_LIST_GET_FIRST(srv_buf_pool->m_free_list);

  if (block != nullptr) {
    ut_ad(block->m_page.m_in_free_list);
    ut_d(block->m_page.m_in_free_list = false);
    ut_ad(!block->m_page.m_in_flush_list);
    ut_ad(!block->m_page.m_in_LRU_list);
    ut_a(!block->m_page.in_file());

    UT_LIST_REMOVE(srv_buf_pool->m_free_list, (&block->m_page));

    mutex_enter(&block->m_mutex);

    buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);

    UNIV_MEM_ALLOC(block->m_frame, UNIV_PAGE_SIZE);

    mutex_exit(&block->m_mutex);
  }

  return block;
}

buf_block_t *Buf_LRU::get_free_block() {
  bool freed;
  ulint n_iterations = 1;
  buf_block_t *block = nullptr;
  bool mon_value_was = false;
  bool started_monitor = false;

loop:
  buf_pool_mutex_enter();

  if (!recv_recovery_on &&
      UT_LIST_GET_LEN(srv_buf_pool->m_free_list) + UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) < srv_buf_pool->m_curr_size / 20) {

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  ERROR: over 95 percent of the buffer pool is occupied by lock heaps!"
      " Check that your transactions do not set too many row locks."
      " Your buffer pool size is %lu MB. Maybe you should make the buffer pool bigger?"
      " We intentionally generate a seg fault to print a stack trace on Linux!\n",
      (ulong)(srv_buf_pool->m_curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
    );

    ut_error;

  } else if (!recv_recovery_on && (UT_LIST_GET_LEN(srv_buf_pool->m_free_list) + UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list)) <
                                    srv_buf_pool->m_curr_size / 3) {

    if (!m_switched_on_monitor) {

      /* Over 67 % of the buffer pool is occupied by lock heaps. This may be a memory leak! */

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  WARNING: over 67 percent of the buffer pool is occupied by"
        " lock heaps! Check that your transactions do not set too many"
        " row locks. Your buffer pool size is %lu MB.Maybe you should"
        " make the buffer pool bigger? Starting the InnoDB Monitor to"
        " print diagnostics, including lock heap and hash index sizes",
        (ulong)(srv_buf_pool->m_curr_size / (1024 * 1024 / UNIV_PAGE_SIZE))
      );

      m_switched_on_monitor = true;

      srv_print_innodb_monitor = true;

      os_event_set(srv_lock_timeout_thread_event);
    }

  } else if (m_switched_on_monitor) {

    /* Switch off the InnoDB Monitor; this is a simple way
    to stop the monitor if the situation becomes less urgent,
    but may also surprise users if the user also switched on the
    monitor! */

    m_switched_on_monitor = false;
    srv_print_innodb_monitor = false;
  }

  /* If there is a block in the free list, take it */
  block = Buf_LRU::get_free_only();

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

  freed = Buf_LRU::search_and_free_block(n_iterations);

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
      (ulong)srv_fil->get_pending_log_flushes(),
      (ulong)srv_fil->get_pending_tablespace_flushes(),
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

  srv_buf_pool->m_flusher->free_margin();
  ++srv_buf_pool_wait_free;

  buf_pool_mutex_enter();

  if (srv_buf_pool->m_LRU_flush_ended > 0) {
    /* We have written pages in an LRU flush. To make the insert
    buffer more efficient, we try to move these pages to the free
    list. */

    buf_pool_mutex_exit();

    try_free_flushed_blocks();
  } else {
    buf_pool_mutex_exit();
  }

  if (n_iterations > 10) {

    os_thread_sleep(500000);
  }

  n_iterations++;

  goto loop;
}

void Buf_LRU::old_adjust_len() {
  ut_a(srv_buf_pool->m_LRU_old);
  ut_ad(buf_pool_mutex_own());
  ut_ad(m_old_ratio >= OLD_RATIO_MIN);
  ut_ad(m_old_ratio <= OLD_RATIO_MAX);

  static_assert(
    OLD_RATIO_MIN * OLD_MIN_LEN > OLD_RATIO_DIV * (OLD_TOLERANCE + 5),
    "OLD_RATIO_MIN * OLD_MIN_LEN <= OLD_RATIO_DIV * (OLD_TOLERANCE + 5)"
  );

  auto old_len = srv_buf_pool->m_LRU_old_len;

  auto new_len = std::min(
    UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) * m_old_ratio / OLD_RATIO_DIV,
    UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) - (OLD_TOLERANCE + NON_MIN_LEN)
  );

  for (;;) {
    auto lru_old = srv_buf_pool->m_LRU_old;

    ut_a(lru_old->m_old);
    ut_ad(lru_old->m_in_LRU_list);

    /* Update the LRU_old pointer if necessary */

    if (old_len + OLD_TOLERANCE < new_len) {

      srv_buf_pool->m_LRU_old = lru_old = UT_LIST_GET_PREV(m_LRU_list, lru_old);

      old_len = ++srv_buf_pool->m_LRU_old_len;

      buf_page_set(lru_old, true);

    } else if (old_len > new_len + OLD_TOLERANCE) {

      srv_buf_pool->m_LRU_old = UT_LIST_GET_NEXT(m_LRU_list, lru_old);

      --srv_buf_pool->m_LRU_old_len;

      old_len = srv_buf_pool->m_LRU_old_len;

      buf_page_set(lru_old, false);

    } else {
      return;
    }
  }
}

void Buf_LRU::old_init() {
  ut_ad(buf_pool_mutex_own());
  ut_a(UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) == OLD_MIN_LEN);

  /* We first initialize all blocks in the LRU list as old and then use
  the adjust function to move the LRU_old pointer to the right
  position */

  for (auto bpage = UT_LIST_GET_LAST(srv_buf_pool->m_LRU_list); bpage != nullptr; bpage = UT_LIST_GET_PREV(m_LRU_list, bpage)) {

    ut_ad(bpage->m_in_LRU_list);
    ut_ad(bpage->in_file());
    /* This loop temporarily violates the
    assertions of buf_page_set(). */
    bpage->m_old = true;
  }

  srv_buf_pool->m_LRU_old = UT_LIST_GET_FIRST(srv_buf_pool->m_LRU_list);
  srv_buf_pool->m_LRU_old_len = UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list);

  old_adjust_len();
}

void Buf_LRU::remove_block(buf_page_t *bpage) {
  ut_ad(srv_buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(bpage->in_file());

  ut_ad(bpage->m_in_LRU_list);

  /* If the LRU_old pointer is defined and points to just this block,
  move it backward one step */

  if (unlikely(bpage == srv_buf_pool->m_LRU_old)) {

    /* Below: the previous block is guaranteed to exist, because the LRU_old pointer is
    only allowed to differ by OLD_TOLERANCE from strict Buf_LRU::old_ratio/OLD_RATIO_DIV
    of the LRU list length. */
    auto prev_bpage = UT_LIST_GET_PREV(m_LRU_list, bpage);

    ut_a(prev_bpage);
    srv_buf_pool->m_LRU_old = prev_bpage;
    buf_page_set(prev_bpage, true);

    ++srv_buf_pool->m_LRU_old_len;
  }

  /* Remove the block from the LRU list */
  UT_LIST_REMOVE(srv_buf_pool->m_LRU_list, bpage);
  ut_d(bpage->m_in_LRU_list = false);

  /* If the LRU list is so short that LRU_old is not defined,
  clear the "old" flags and return */
  if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) < OLD_MIN_LEN) {

    for (bpage = UT_LIST_GET_FIRST(srv_buf_pool->m_LRU_list); bpage != nullptr; bpage = UT_LIST_GET_NEXT(m_LRU_list, bpage)) {
      /* This loop temporarily violates the assertions of buf_page_set(). */
      bpage->m_old = false;
    }

    srv_buf_pool->m_LRU_old = nullptr;
    srv_buf_pool->m_LRU_old_len = 0;

    return;
  }

  ut_ad(srv_buf_pool->m_LRU_old);

  /* Update the LRU_old_len field if necessary */
  if (buf_page_is_old(bpage)) {

    srv_buf_pool->m_LRU_old_len--;
  }

  /* Adjust the length of the old block list if necessary */
  old_adjust_len();
}

void Buf_LRU::add_block_to_end_low(buf_page_t *bpage) {
  ut_ad(srv_buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(bpage->in_file());

  ut_ad(!bpage->m_in_LRU_list);
  UT_LIST_ADD_LAST(srv_buf_pool->m_LRU_list, bpage);
  ut_d(bpage->m_in_LRU_list = true);

  if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) > OLD_MIN_LEN) {

    ut_ad(srv_buf_pool->m_LRU_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set(bpage, true);
    srv_buf_pool->m_LRU_old_len++;
    old_adjust_len();

  } else if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) == OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    old_init();
  } else {
    buf_page_set(bpage, srv_buf_pool->m_LRU_old != nullptr);
  }
}

void Buf_LRU::add_block_low(buf_page_t *bpage, bool old) {
  ut_ad(srv_buf_pool);
  ut_ad(bpage);
  ut_ad(buf_pool_mutex_own());

  ut_a(bpage->in_file());
  ut_ad(!bpage->m_in_LRU_list);

  if (!old || (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) < OLD_MIN_LEN)) {

    UT_LIST_ADD_FIRST(srv_buf_pool->m_LRU_list, bpage);

    bpage->m_freed_page_clock = srv_buf_pool->m_freed_page_clock;
  } else {
    UT_LIST_INSERT_AFTER(srv_buf_pool->m_LRU_list, srv_buf_pool->m_LRU_old, bpage);
    srv_buf_pool->m_LRU_old_len++;
  }

  ut_d(bpage->m_in_LRU_list = true);

  if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) > OLD_MIN_LEN) {

    ut_ad(srv_buf_pool->m_LRU_old);

    /* Adjust the length of the old block list if necessary */

    buf_page_set(bpage, old);
    old_adjust_len();

  } else if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) == OLD_MIN_LEN) {

    /* The LRU list is now long enough for LRU_old to become
    defined: init it */

    old_init();
  } else {
    buf_page_set(bpage, srv_buf_pool->m_LRU_old != nullptr);
  }
}

void Buf_LRU::add_block(buf_page_t *bpage, bool old) {
  add_block_low(bpage, old);
}

void Buf_LRU::make_block_young(buf_page_t *bpage) {
  ut_ad(buf_pool_mutex_own());

  if (bpage->m_old) {
    ++srv_buf_pool->m_stat.n_pages_made_young;
  }

  remove_block(bpage);
  add_block_low(bpage, false);
}

void Buf_LRU::make_block(buf_page_t *bpage) {
  remove_block(bpage);
  add_block_to_end_low(bpage);
}

Buf_LRU::Block_status Buf_LRU::free_block(buf_page_t *bpage, bool *buf_pool_mutex_released) {
  auto block_mutex = buf_page_get_mutex(bpage);

  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(block_mutex));
  ut_ad(bpage->in_file());
  ut_ad(bpage->m_in_LRU_list);
  ut_ad(!bpage->m_in_flush_list == !bpage->m_oldest_modification);

  UNIV_MEM_ASSERT_RW(bpage, sizeof(*bpage));

  if (!buf_page_can_relocate(bpage)) {

    /* Do not free buffer-fixed or I/O-fixed blocks. */
    return Block_status::NOT_FREED;
  }

  if (bpage->m_oldest_modification > 0) {

    return Block_status::NOT_FREED;

  } else if (block_remove_hashed_page(bpage) == BUF_BLOCK_REMOVE_HASH) {
    ut_a(bpage->m_buf_fix_count == 0);

    if (buf_pool_mutex_released) {
      *buf_pool_mutex_released = true;
    }

    buf_pool_mutex_exit();
    mutex_exit(block_mutex);

    /* The page was declared uninitialized by Buf_LRU::block_remove_hashed_page(). We need to flag
    the contents of the page valid (which it still is) in order to avoid bogus Valgrind warnings.*/

    UNIV_MEM_VALID(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

    UNIV_MEM_INVALID(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

    buf_pool_mutex_enter();

    mutex_enter(block_mutex);

    block_free_hashed_page(reinterpret_cast<buf_block_t *>(bpage));
  }

  return Block_status::FREED;
}

void Buf_LRU::block_free_non_file_page(buf_block_t *block) {
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
  ut_ad(!block->m_page.m_in_LRU_list);

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

  UT_LIST_ADD_FIRST(srv_buf_pool->m_free_list, &block->m_page);

  ut_d(block->m_page.m_in_free_list = true);

  UNIV_MEM_ASSERT_AND_FREE(block, UNIV_PAGE_SIZE);
}

buf_page_state Buf_LRU::block_remove_hashed_page(buf_page_t *bpage) {
  ut_ad(bpage != nullptr);
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  ut_a(buf_page_get_io_fix(bpage) == BUF_IO_NONE);
  ut_a(bpage->m_buf_fix_count == 0);

  UNIV_MEM_ASSERT_RW(bpage, sizeof *bpage);

  remove_block(bpage);

  srv_buf_pool->m_freed_page_clock += 1;

  switch (bpage->get_state()) {
    case BUF_BLOCK_FILE_PAGE:
      UNIV_MEM_ASSERT_W(bpage, sizeof(buf_block_t));
      UNIV_MEM_ASSERT_W(((buf_block_t *)bpage)->m_frame, UNIV_PAGE_SIZE);

      buf_block_modify_clock_inc(reinterpret_cast<buf_block_t *>(bpage));
      break;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  auto hashed_bpage = srv_buf_pool->hash_get_page(bpage->m_space, bpage->m_page_no);

  if (unlikely(bpage != hashed_bpage)) {
    ib_logger(ib_stream, "Error: page %lu %lu not found in the hash table ", (ulong)bpage->m_space, (ulong)bpage->m_page_no);
    if (hashed_bpage) {
      ib_logger(
        ib_stream,
        "In hash table we find block %p of %lu %lu which is not %p",
        (const void *)hashed_bpage,
        (ulong)hashed_bpage->m_space,
        (ulong)hashed_bpage->m_page_no,
        (const void *)bpage
      );
    }

#if defined UNIV_DEBUG
    mutex_exit(buf_page_get_mutex(bpage));

    buf_pool_mutex_exit();

    srv_buf_pool->print();

    print();

    srv_buf_pool->validate();

    validate();
#endif /* UNIV_DEBUG */
    ut_error;
  }

  ut_ad(bpage->m_in_page_hash);
  ut_d(bpage->m_in_page_hash = false);

  srv_buf_pool->m_page_hash->erase(Page_id(bpage->m_space, bpage->m_page_no));

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

void Buf_LRU::block_free_hashed_page(buf_block_t *block) {
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&block->m_mutex));

  buf_block_set_state(block, BUF_BLOCK_MEMORY);

  block_free_non_file_page(block);
}

ulint Buf_LRU::old_ratio_update(ulint old_pct, bool adjust) {
  auto ratio = old_pct * OLD_RATIO_DIV / 100;

  if (ratio < OLD_RATIO_MIN) {

    ratio = OLD_RATIO_MIN;

  } else if (ratio > OLD_RATIO_MAX) {

    ratio = OLD_RATIO_MAX;
  }

  if (adjust) {
    buf_pool_mutex_enter();

    auto buf_LRU = srv_buf_pool->m_LRU.get();

    if (ratio != buf_LRU->m_old_ratio) {

      buf_LRU->m_old_ratio = ratio;

      if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) >= OLD_MIN_LEN) {

        buf_LRU->old_adjust_len();
      }
    }

    buf_pool_mutex_exit();
  } else {

    s_old_ratio = ratio;
  }

  /* the reverse of ratio = old_pct * OLD_RATIO_DIV / 100 */
  return (ulint)(ratio * 100 / (double)OLD_RATIO_DIV + 0.5);
}

void Buf_LRU::stat_update() {
  /* If we haven't started eviction yet then don't update stats. */
  if (srv_buf_pool->m_freed_page_clock != 0) {
    buf_pool_mutex_enter();

    /* Update the index. */
    auto item = &m_stats[m_stat_ind];

    ++m_stat_ind;

    m_stat_ind %= m_stats.size();

    /* Add the current value and subtract the obsolete entry. */
    m_stat_sum.m_io += m_stat_cur.m_io - item->m_io;

    *item = m_stat_cur;

    buf_pool_mutex_exit();
  }

  m_stat_cur = Stat{};
}

#if defined UNIV_DEBUG
bool Buf_LRU::validate() {
  buf_pool_mutex_enter();

  if (UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) >= OLD_MIN_LEN) {

    ut_a(srv_buf_pool->m_LRU_old);

    const auto old_len = srv_buf_pool->m_LRU_old_len;

    const auto new_len = std::min(
      UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) * m_old_ratio / OLD_RATIO_DIV,
      UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) - (OLD_TOLERANCE + NON_MIN_LEN)
    );

    ut_a(old_len >= new_len - OLD_TOLERANCE);
    ut_a(old_len <= new_len + OLD_TOLERANCE);
  }

  UT_LIST_CHECK(srv_buf_pool->m_LRU_list);

  ulint old_len{};

  for (auto bpage = UT_LIST_GET_FIRST(srv_buf_pool->m_LRU_list); bpage != nullptr; bpage = UT_LIST_GET_NEXT(m_LRU_list, bpage)) {

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
      const auto prev = UT_LIST_GET_PREV(m_LRU_list, bpage);
      const auto next = UT_LIST_GET_NEXT(m_LRU_list, bpage);

      ++old_len;

      if (old_len >= 1) {
        ut_a(srv_buf_pool->m_LRU_old == bpage);
      } else {
        ut_a(prev == nullptr || buf_page_is_old(prev));
      }

      ut_a(next == nullptr || buf_page_is_old(next));
    }
  }

  ut_a(srv_buf_pool->m_LRU_old_len == old_len);

  auto check = [](const buf_page_t *page) {
    ut_ad(page->m_in_free_list);
  };
  ut_list_validate(srv_buf_pool->m_free_list, check);

  for (auto bpage = UT_LIST_GET_FIRST(srv_buf_pool->m_free_list); bpage != nullptr; bpage = UT_LIST_GET_NEXT(m_list, bpage)) {

    ut_a(bpage->get_state() == BUF_BLOCK_NOT_USED);
  }

  buf_pool_mutex_exit();

  return (true);
}

void Buf_LRU::print() {
  buf_pool_mutex_enter();

  for (auto bpage = UT_LIST_GET_FIRST(srv_buf_pool->m_LRU_list); bpage != nullptr; bpage = UT_LIST_GET_NEXT(m_LRU_list, bpage)) {

    ib_logger(ib_stream, "BLOCK space %lu page %lu ", (ulong)bpage->get_space(), (ulong)bpage->get_page_no());

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
        frame = reinterpret_cast<buf_block_t *>(bpage)->get_frame();

        ib_logger(
          ib_stream, "\ntype %lu index id %lu\n", (ulong)srv_fil->page_get_type(frame), (ulong)btr_page_get_index_id(frame)
        );
        break;

      default:
        ib_logger(ib_stream, "\n!state %lu!\n", (ulong)bpage->get_state());
        break;
    }
  }

  buf_pool_mutex_exit();
}
#endif /* UNIV_DEBUG */
