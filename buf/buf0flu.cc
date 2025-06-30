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

/** @file buf/buf0flu.c
The database buffer buf pool flush algorithm

Created 11/11/1995 Heikki Tuuri
*******************************************************/

#include "buf0flu.h"
#include <algorithm>
#include "buf0buf.h"
#include "buf0dblwr.h"
#include "buf0lru.h"
#include "buf0rea.h"
#include "fil0fil.h"
#include "log0log.h"
#include "os0aio.h"
#include "os0file.h"
#include "page0page.h"
#include "srv0srv.h"
#include "trx0sys.h"
#include "ut0lst.h"

Buf_page *Buf_flush::insert_in_flush_rbt(Buf_page *bpage) {
  Buf_page *prev = nullptr;

  ut_ad(m_buf_pool->mutex_is_owned());

  /* Insert this buffer into the rbt. */
  auto insert_result = flush_set_insert(m_buf_pool->m_recovery_flush_list, bpage);
  ut_a(insert_result.second);

  auto c_node = insert_result.first;
  /* Get the predecessor. */
  auto p_node = flush_set_prev(m_buf_pool->m_recovery_flush_list, c_node);

  if (p_node.has_value()) {
    prev = *p_node.value();
    ut_a(prev != nullptr);
  }

  return prev;
}

void Buf_flush::delete_from_flush_rbt(Buf_page *bpage) {
  ut_ad(m_buf_pool->mutex_is_owned());

  auto ret = flush_set_erase(m_buf_pool->m_recovery_flush_list, bpage);
  ut_a(ret);
}

bool Buf_flush::block_cmp(const Buf_page *b1, const Buf_page *b2) {
  ut_ad(b1 != nullptr);
  ut_ad(b2 != nullptr);

  ut_ad(b1->m_in_flush_list);
  ut_ad(b2->m_in_flush_list);

  if (b2->m_oldest_modification > b1->m_oldest_modification) {
    return false;
  }

  if (b2->m_oldest_modification < b1->m_oldest_modification) {
    return true;
  }

  /* If oldest_modification is same then decide on the space. */
  if (b2->m_space != b1->m_space) {
    return b2->m_space < b1->m_space;
  }

  /* Or else decide ordering on the offset field. */
  return b2->m_page_no < b1->m_page_no;
}

void Buf_flush::init_flush_list() {
  ut_ad(m_buf_pool->mutex_is_owned());

  /* Create red black tree for speedy insertions in flush list. */
  m_buf_pool->m_recovery_flush_list = flush_set_create(block_cmp);
}

void Buf_flush::free_flush_list() {
  mutex_enter(&m_buf_pool->m_mutex);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(validate_low());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  flush_set_destroy(m_buf_pool->m_recovery_flush_list);
  m_buf_pool->m_recovery_flush_list = nullptr;

  mutex_exit(&m_buf_pool->m_mutex);
}

void Buf_flush::insert_into_flush_list(Buf_block *block) {
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(
    (UT_LIST_GET_FIRST(m_buf_pool->m_flush_list) == nullptr) ||
    (UT_LIST_GET_FIRST(m_buf_pool->m_flush_list)->m_oldest_modification <= block->m_page.m_oldest_modification)
  );

  /* If we are in the recovery then we need to update the flush
  red-black tree as well. */
  if (m_buf_pool->m_recovery_flush_list != nullptr) {
    insert_sorted_into_flush_list(block);
    return;
  }

  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);
  ut_ad(block->m_page.m_in_LRU_list);
  ut_ad(block->m_page.m_in_page_hash);
  ut_ad(!block->m_page.m_in_flush_list);
  ut_d(block->m_page.m_in_flush_list = true);
  UT_LIST_ADD_FIRST(m_buf_pool->m_flush_list, &block->m_page);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(validate_low());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
}

void Buf_flush::insert_sorted_into_flush_list(Buf_block *block) {
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);

  ut_ad(block->m_page.m_in_LRU_list);
  ut_ad(block->m_page.m_in_page_hash);
  ut_ad(!block->m_page.m_in_flush_list);
  ut_d(block->m_page.m_in_flush_list = true);

  Buf_page *prev_b{};

  /* For the most part when this function is called the m_recovery_flush_list
  should not be nullptr. In a very rare boundary case it is possible that the
  m_recovery_flush_list has already been freed by the recovery thread before
  the last page was hooked up in the m_flush_list by the io-handler thread.
  In that case we'll  just do a simple linear search in the else block. */
  if (m_buf_pool->m_recovery_flush_list != nullptr) {

    prev_b = insert_in_flush_rbt(&block->m_page);

  } else {

    auto b = UT_LIST_GET_FIRST(m_buf_pool->m_flush_list);

    while (b != nullptr && b->m_oldest_modification > block->m_page.m_oldest_modification) {
      ut_ad(b->m_in_flush_list);
      prev_b = b;
      b = UT_LIST_GET_NEXT(m_list, b);
    }
  }

  if (prev_b == nullptr) {
    UT_LIST_ADD_FIRST(m_buf_pool->m_flush_list, &block->m_page);
  } else {
    UT_LIST_INSERT_AFTER(m_buf_pool->m_flush_list, prev_b, &block->m_page);
  }

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(validate_low());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
}

bool Buf_flush::ready_for_replace(Buf_page *bpage) {
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));
  ut_ad(bpage->m_in_LRU_list);

  if (likely(bpage->in_file())) {

    return bpage->m_oldest_modification == 0 && buf_page_get_io_fix(bpage) == BUF_IO_NONE && bpage->m_buf_fix_count == 0;
  }

  log_err("Buffer block state ", (ulong)bpage->get_state(), " in the LRU list!");
  log_warn_buf(bpage, sizeof(Buf_page));

  return false;
}

bool Buf_flush::ready_for_flush(Buf_page *bpage, buf_flush flush_type) {
  ut_a(bpage->in_file());
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));
  ut_ad(flush_type == to_int(BUF_FLUSH_LRU) || flush_type == BUF_FLUSH_LIST);

  if (bpage->m_oldest_modification != 0 && buf_page_get_io_fix(bpage) == BUF_IO_NONE) {
    ut_ad(bpage->m_in_flush_list);

    if (flush_type != BUF_FLUSH_LRU) {

      return true;

    } else if (bpage->m_buf_fix_count == 0) {

      /* If we are flushing the LRU list, to avoid deadlocks
      we require the block not to be bufferfixed, and hence
      not latched. */

      return true;
    }
  }

  return false;
}

void Buf_flush::remove(Buf_page *bpage) {
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));
  ut_ad(bpage->m_in_flush_list);

  switch (bpage->get_state()) {
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
    case BUF_BLOCK_FILE_PAGE:
      UT_LIST_REMOVE(m_buf_pool->m_flush_list, bpage);
      break;
  }

  /* If the m_recovery_flush_list is active then delete from it as well. */
  if (likely_null(m_buf_pool->m_recovery_flush_list)) {
    delete_from_flush_rbt(bpage);
  }

  /* Must be done after we have removed it from the m_recovery_flush_list
  because we assert on in_flush_list in comparison function. */
  ut_d(bpage->m_in_flush_list = false);

  bpage->m_oldest_modification = 0;

  auto check = [](const Buf_page *ptr) {
    ut_ad(ptr->m_in_flush_list);
  };
  ut_list_validate(m_buf_pool->m_flush_list, check);
}

void Buf_flush::relocate_on_flush_list(Buf_page *bpage, Buf_page *dpage) {
  Buf_page *prev_b{};

  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  ut_ad(bpage->m_in_flush_list);
  ut_ad(dpage->m_in_flush_list);

  /* If recovery is active we must swap the control blocks in
  the m_recovery_flush_list  as well. */
  if (likely_null(m_buf_pool->m_recovery_flush_list)) {
    delete_from_flush_rbt(bpage);
    prev_b = insert_in_flush_rbt(dpage);
  }

  /* Must be done after we have removed it from the m_recoevry_flush_list
  because we assert on in_flush_list in comparison function. */
  ut_d(bpage->m_in_flush_list = false);

  auto prev = UT_LIST_GET_PREV(m_list, bpage);
  UT_LIST_REMOVE(m_buf_pool->m_flush_list, bpage);

  if (prev != nullptr) {
    ut_ad(prev->m_in_flush_list);
    UT_LIST_INSERT_AFTER(m_buf_pool->m_flush_list, prev, dpage);
  } else {
    UT_LIST_ADD_FIRST(m_buf_pool->m_flush_list, dpage);
  }

  /* Just an extra check. Previous in m_flush_list
  should be the same control block as in m_recovery_flush_list. */
  ut_a(!m_buf_pool->m_recovery_flush_list || prev_b == prev);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(validate_low());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
}

void Buf_flush::write_complete(Buf_page *bpage) {
  remove(bpage);

  auto flush_type = buf_page_get_flush_type(bpage);

  m_buf_pool->m_n_flush[flush_type]--;

  if (flush_type == BUF_FLUSH_LRU) {
    /* Put the block to the end of the LRU list to wait to be
    moved to the free list */

    m_buf_pool->m_LRU->make_block(bpage);

    m_buf_pool->m_LRU_flush_ended++;
  }

  /* ib_logger(ib_stream, "n pending flush %lu\n",
  m_buf_pool->n_flush[flush_type]); */

  if ((m_buf_pool->m_n_flush[flush_type] == 0) && (m_buf_pool->m_init_flush[flush_type] == false)) {

    /* The running flush batch has ended */

    os_event_set(m_buf_pool->m_no_flush[flush_type]);
  }
}

void Buf_flush::sync_datafiles() {
  /* Wait until all pending async writes are completed */
  srv_aio->wait_for_pending_ops(aio::WRITE);

  /* Now we flush the data to disk (for example, with fsync) */
  srv_fil->flush_file_spaces(FIL_TABLESPACE);

  return;
}

void Buf_flush::buffered_writes(DBLWR *dblwr) {
  ulint len;
  ulint len2;
  byte *write_buf;

  if (!srv_config.m_use_doublewrite_buf || dblwr == nullptr) {
    /* Sync the writes to the disk. */
    sync_datafiles();
    return;
  }

  mutex_enter(&dblwr->m_mutex);

  /* Write first to doublewrite buffer blocks. We use synchronous
  aio and thus know that file write has been completed when the
  control returns. */

  if (dblwr->m_first_free == 0) {

    mutex_exit(&dblwr->m_mutex);

    return;
  }

  for (ulint i{}; i < dblwr->m_first_free; ++i) {

    auto block = reinterpret_cast<const Buf_block *>(dblwr->m_bpages[i]);

    if (block->get_state() != BUF_BLOCK_FILE_PAGE) {
      /* No simple validate for compressed pages exists. */
      continue;
    }

    if (unlikely(memcmp(block->m_frame + (FIL_PAGE_LSN + 4), block->m_frame + (UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4), 4))) {
      log_err(
        "The page to be written seems corrupt! The lsn fields do not match! Noticed in the buffer pool"
        " before posting to the doublewrite buffer."
      );
    }

    if (block->m_check_index_page_at_flush && unlikely(!page_simple_validate(block->m_frame))) {
      buf_page_print(block->m_frame, 0);

      log_fatal(std::format(
        "Apparent corruption of an index page n:o {} in space {}"
        " to be written to data file. We intentionally crash server"
        " to prevent corrupt data from ending up in data files.",
        block->get_page_no(),
        block->get_space()
      ));
    }
  }

  /* Increment the doublewrite flushed pages counter */
  srv_dblwr_pages_written += dblwr->m_first_free;
  ++srv_dblwr_writes;

  len = std::min<ulint>(SYS_DOUBLEWRITE_BLOCK_SIZE, dblwr->m_first_free) * UNIV_PAGE_SIZE;

  write_buf = dblwr->m_write_buf;

  ulint i{};

  srv_fil->io(IO_request::Sync_write, false, TRX_SYS_SPACE, dblwr->m_block1, 0, len, write_buf, nullptr);

  for (len2 = 0; len2 + UNIV_PAGE_SIZE <= len; len2 += UNIV_PAGE_SIZE, i++) {
    const Buf_block *block = reinterpret_cast<Buf_block *>(dblwr->m_bpages[i]);

    if (likely(block->get_state() == BUF_BLOCK_FILE_PAGE) &&
        unlikely(memcmp(write_buf + len2 + (FIL_PAGE_LSN + 4), write_buf + len2 + (UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4), 4)
        )) {

      log_err(
        "The page to be written seems corrupt! The lsn fields do not match!"
        " Noticed in the doublewrite block1."
      );
    }
  }

  if (dblwr->m_first_free <= SYS_DOUBLEWRITE_BLOCK_SIZE) {
    goto flush;
  }

  len = (dblwr->m_first_free - SYS_DOUBLEWRITE_BLOCK_SIZE) * UNIV_PAGE_SIZE;

  write_buf = dblwr->m_write_buf + SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE;
  ut_ad(i == SYS_DOUBLEWRITE_BLOCK_SIZE);

  srv_fil->io(IO_request::Sync_write, false, TRX_SYS_SPACE, dblwr->m_block2, 0, len, (void *)write_buf, nullptr);

  for (len2 = 0; len2 + UNIV_PAGE_SIZE <= len; len2 += UNIV_PAGE_SIZE, i++) {
    const Buf_block *block = reinterpret_cast<Buf_block *>(dblwr->m_bpages[i]);

    if (likely(block->get_state() == BUF_BLOCK_FILE_PAGE) &&
        unlikely(memcmp(write_buf + len2 + (FIL_PAGE_LSN + 4), write_buf + len2 + (UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4), 4)
        )) {
      log_err(
        "The page to be written seems corrupt! The lsn fields do not match!"
        " Noticed in the doublewrite block2."
      );
    }
  }

flush:
  /* Now flush the doublewrite buffer data to disk */

  srv_fil->flush(TRX_SYS_SPACE);

  /* We know that the writes have been flushed to disk now
  and in recovery we will find them in the doublewrite buffer
  blocks. Next do the writes to the intended positions. */

  for (i = 0; i < dblwr->m_first_free; ++i) {
    const Buf_block *block = reinterpret_cast<Buf_block *>(dblwr->m_bpages[i]);

    ut_a(block->m_page.in_file());

    ut_a(block->get_state() == BUF_BLOCK_FILE_PAGE);

    if (unlikely(memcmp(block->m_frame + (FIL_PAGE_LSN + 4), block->m_frame + (UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4), 4))) {
      log_err(std::format(
        "The page to be written seems corrupt! The lsn fields do not match! Noticed in the buffer pool"
        " after posting and flushing the doublewrite buffer. Page buf fix count {}, io fix {}, state {}",
        int(block->m_page.m_buf_fix_count),
        int(buf_block_get_io_fix(block)),
        int(block->get_state())
      ));
    }

    srv_fil->io(
      IO_request::Async_write,
      true,
      block->get_space(),
      block->get_page_no(),
      0,
      UNIV_PAGE_SIZE,
      (void *)block->m_frame,
      (void *)block
    );

    /* Increment the counter of I/O operations used
    for selecting LRU policy. */
    m_buf_pool->m_LRU->stat_inc_io();
  }

  /* Sync the writes to the disk. */
  sync_datafiles();

  /* We can now reuse the doublewrite memory buffer: */
  dblwr->m_first_free = 0;

  mutex_exit(&dblwr->m_mutex);
}

void Buf_flush::post_to_doublewrite_buf(DBLWR *dblwr, Buf_page *bpage) {
  for (;;) {
    mutex_enter(&dblwr->m_mutex);

    ut_a(bpage->in_file());

    if (dblwr->m_first_free < 2 * SYS_DOUBLEWRITE_BLOCK_SIZE) {
      break;
    }

    mutex_exit(&dblwr->m_mutex);

    buffered_writes(dblwr);
  }

  ut_a(bpage->get_state() == BUF_BLOCK_FILE_PAGE);

  memcpy(dblwr->m_write_buf + UNIV_PAGE_SIZE * dblwr->m_first_free, reinterpret_cast<Buf_block *>(bpage)->m_frame, UNIV_PAGE_SIZE);

  dblwr->m_bpages[dblwr->m_first_free] = bpage;

  ++dblwr->m_first_free;

  if (dblwr->m_first_free >= 2 * SYS_DOUBLEWRITE_BLOCK_SIZE) {
    mutex_exit(&dblwr->m_mutex);
    buffered_writes(dblwr);
  } else {
    mutex_exit(&dblwr->m_mutex);
  }
}

void Buf_flush::init_for_writing(byte *page, lsn_t newest_lsn) {
  /* Write the newest modification lsn to the page header and trailer */
  mach_write_to_8(page + FIL_PAGE_LSN, newest_lsn);

  mach_write_to_8(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM, newest_lsn);

  /* Store the new formula checksum */

  mach_write_to_4(page + FIL_PAGE_SPACE_OR_CHKSUM, buf_page_data_calc_checksum(page));

  /* We overwrite the first 4 bytes of the end lsn field to store
  the old formula checksum. Since it depends also on the field
  FIL_PAGE_SPACE_OR_CHKSUM, it has to be calculated after storing the
  new formula checksum. */

  mach_write_to_4(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM, BUF_NO_CHECKSUM_MAGIC);
}

void Buf_flush::write_block_low(DBLWR *dblwr, Buf_page *bpage) {
  page_t *frame = nullptr;

  ut_ad(bpage->in_file());

  /* We are not holding buf_pool_mutex or block_mutex here.
  Nevertheless, it is safe to access bpage, because it is
  io_fixed and m_oldest_modification != 0.  Thus, it cannot be
  relocated in the buffer pool or removed from m_flush_list or
  LRU_list. */
  ut_ad(!m_buf_pool->mutex_is_owned());
  ut_ad(!mutex_own(buf_page_get_mutex(bpage)));
  ut_ad(buf_page_get_io_fix(bpage) == BUF_IO_WRITE);
  ut_ad(bpage->m_oldest_modification != 0);
  ut_ad(bpage->m_newest_modification != 0);

  /* Force the log to the disk before writing the modified block */
  log_sys->write_up_to(bpage->m_newest_modification, LOG_WAIT_ALL_GROUPS, true);

  switch (bpage->get_state()) {
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
    case BUF_BLOCK_FILE_PAGE:
      frame = ((Buf_block *)bpage)->m_frame;
      init_for_writing(((Buf_block *)bpage)->m_frame, bpage->m_newest_modification);
      break;
  }

  if (!srv_config.m_use_doublewrite_buf || dblwr == nullptr) {
    srv_fil->io(IO_request::Async_write, true, bpage->get_space(), bpage->get_page_no(), 0, UNIV_PAGE_SIZE, frame, bpage);
  } else {
    post_to_doublewrite_buf(dblwr, bpage);
  }
}

inline static bool free_page_if_truncated(Buf_pool *buf_pool, Buf_page *bpage) {
  ut_a(bpage != nullptr);

  ut_ad(buf_pool->mutex_is_owned());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  auto space = bpage->get_space();
  bool table_truncated = srv_fil->tablespace_deleted_or_being_deleted_in_mem(space, -1);

  bool page_from_flush_list = bpage->m_oldest_modification != 0;

  if (table_truncated) {
    log_err(std::format("freeing a page from truncated table, tablespace_id: {}, page_no: {}", space, bpage->m_page_no));
    // free up the page.
    buf_pool->m_LRU->free_block(bpage, nullptr);

    if (page_from_flush_list) {
      buf_pool->m_flusher->remove(bpage);
    }

    return true;
  }

  return false;
}

void Buf_flush::page(DBLWR *dblwr, Buf_page *bpage, buf_flush flush_type) {
  ut_ad(m_buf_pool->mutex_is_owned());
  ut_ad(flush_type == BUF_FLUSH_LRU || flush_type == BUF_FLUSH_LIST);
  ut_ad(bpage->in_file());

  auto block_mutex = buf_page_get_mutex(bpage);
  ut_ad(mutex_own(block_mutex));

  ut_ad(ready_for_flush(bpage, flush_type));

  buf_page_set_io_fix(bpage, BUF_IO_WRITE);

  buf_page_set_flush_type(bpage, flush_type);

  if (m_buf_pool->m_n_flush[flush_type] == 0) {

    os_event_reset(m_buf_pool->m_no_flush[flush_type]);
  }

  m_buf_pool->m_n_flush[flush_type]++;

  switch (flush_type) {
    bool is_s_latched;
    case BUF_FLUSH_LIST:
      /* If the simulated aio thread is not running, we must
      not wait for any latch, as we may end up in a deadlock:
      if buf_fix_count == 0, then we know we need not wait */

      is_s_latched = (bpage->m_buf_fix_count == 0);
      if (is_s_latched) {
        rw_lock_s_lock_gen(&((Buf_block *)bpage)->m_rw_lock, BUF_IO_WRITE);
      }

      mutex_exit(block_mutex);
      m_buf_pool->mutex_release();

      /* Even though bpage is not protected by any mutex at
      this point, it is safe to access bpage, because it is
      io_fixed and m_oldest_modification != 0.  Thus, it
      cannot be relocated in the buffer pool or removed from
      m_flush_list or LRU_list. */

      if (!is_s_latched) {
        buffered_writes(dblwr);

        rw_lock_s_lock_gen(&((Buf_block *)bpage)->m_rw_lock, BUF_IO_WRITE);
      }

      break;

    case BUF_FLUSH_LRU:
      /* VERY IMPORTANT:
      Because any thread may call the LRU flush, even when owning
      locks on pages, to avoid deadlocks, we must make sure that the
      s-lock is acquired on the page without waiting: this is
      accomplished because ready_for_flush() must hold,
      and that requires the page not to be bufferfixed. */

      rw_lock_s_lock_gen(&((Buf_block *)bpage)->m_rw_lock, BUF_IO_WRITE);

      /* Note that the s-latch is acquired before releasing the
      m_buf_pool mutex: this ensures that the latch is acquired
      immediately. */

      mutex_exit(block_mutex);
      m_buf_pool->mutex_release();
      break;

    default:
      ut_error;
  }

  /* Even though bpage is not protected by any mutex at this
  point, it is safe to access bpage, because it is io_fixed and
  m_oldest_modification != 0.  Thus, it cannot be relocated in the
  buffer pool or removed from m_flush_list or LRU_list. */

  write_block_low(dblwr, bpage);
}

ulint Buf_flush::try_neighbors(DBLWR *dblwr, space_id_t space, page_no_t page_no, buf_flush flush_type) {
  ulint count{};
  page_no_t low;
  page_no_t high;

  ut_ad(flush_type == BUF_FLUSH_LRU || flush_type == BUF_FLUSH_LIST);

  if (UT_LIST_GET_LEN(m_buf_pool->m_LRU_list) < Buf_LRU::OLD_MIN_LEN) {
    /* If there is little space, it is better not to flush any
    block except from the end of the LRU list */

    low = page_no;
    high = page_no + 1;
  } else {
    /* When flushed, dirty blocks are searched in neighborhoods of
    this size, and flushed along with the original page. */

    ulint area = std::min<ulint>(m_buf_pool->get_read_ahead_area(), m_buf_pool->m_curr_size / 16);

    low = (page_no / area) * area;
    high = (page_no / area + 1) * area;
  }

  /* ib_logger(ib_stream, "Flush area: low %lu high %lu\n", low, high); */

  if (high > srv_fil->space_get_size(space)) {
    high = srv_fil->space_get_size(space);
  }

  m_buf_pool->mutex_acquire();

  for (auto i = low; i < high; ++i) {

    auto bpage = m_buf_pool->hash_get_page(space, i);

    if (bpage == nullptr) {

      continue;
    }

    {
      auto block_mutex = buf_page_get_mutex(bpage);

      mutex_enter(block_mutex);

      if (free_page_if_truncated(m_buf_pool, bpage)) {
        mutex_exit(block_mutex);
        continue;
      }

      mutex_exit(block_mutex);
    }

    ut_a(bpage->in_file());

    /* We avoid flushing 'non-old' blocks in an LRU flush,
    because the flushed blocks are soon freed */

    if (flush_type != BUF_FLUSH_LRU || i == page_no || buf_page_is_old(bpage)) {
      auto block_mutex = buf_page_get_mutex(bpage);

      mutex_enter(block_mutex);

      if (ready_for_flush(bpage, flush_type) && (i == page_no || bpage->m_buf_fix_count == 0)) {
        /* We only try to flush those neighbors != page_no where the buf fix count is
        zero, as we then know that we probably can latch the page without a semaphore wait.
        Semaphore waits are expensive because we must flush the doublewrite buffer before
        we start waiting. */

        page(dblwr, bpage, flush_type);
        ut_ad(!mutex_own(block_mutex));
        ++count;

        m_buf_pool->mutex_acquire();
      } else {
        mutex_exit(block_mutex);
      }
    }
  }

  m_buf_pool->mutex_release();

  return count;
}

ulint Buf_flush::batch(DBLWR *dblwr, buf_flush flush_type, ulint min_n, uint64_t lsn_limit) {
  Buf_page *bpage;
  ulint page_count = 0;
  space_id_t space;
  page_no_t page_no;

  ut_ad(flush_type == BUF_FLUSH_LRU || flush_type == BUF_FLUSH_LIST);
  IF_SYNC_DEBUG(ut_ad(flush_type != BUF_FLUSH_LIST || sync_thread_levels_empty_gen(true));)

  m_buf_pool->mutex_acquire();

  if (m_buf_pool->m_n_flush[flush_type] > 0 || m_buf_pool->m_init_flush[flush_type] == true) {

    /* There is already a flush batch of the same type running */

    m_buf_pool->mutex_release();

    return ULINT_UNDEFINED;
  }

  m_buf_pool->m_init_flush[flush_type] = true;

  for (;;) {
  flush_next:
    /* If we have flushed enough, leave the loop */
    if (page_count >= min_n) {

      break;
    }

    /* Start from the end of the list looking for a suitable
    block to be flushed. */

    if (flush_type == BUF_FLUSH_LRU) {
      bpage = UT_LIST_GET_LAST(m_buf_pool->m_LRU_list);
    } else {
      ut_ad(flush_type == BUF_FLUSH_LIST);

      bpage = UT_LIST_GET_LAST(m_buf_pool->m_flush_list);
      if (!bpage || bpage->m_oldest_modification >= lsn_limit) {
        /* We have flushed enough */

        break;
      }
      ut_ad(bpage->m_in_flush_list);
    }

    /* Note that after finding a single flushable page, we try to
    flush also all its neighbors, and after that start from the
    END of the LRU list or flush list again: the list may change
    during the flushing and we cannot safely preserve within this
    function a pointer to a block in the list! */

    do {
      mutex_t *block_mutex = buf_page_get_mutex(bpage);
      bool ready{false};

      ut_a(bpage->in_file());

      mutex_enter(block_mutex);
      if (!free_page_if_truncated(m_buf_pool, bpage)) {
        ready = ready_for_flush(bpage, flush_type);
      }
      mutex_exit(block_mutex);

      if (ready) {
        space = bpage->get_space();
        page_no = bpage->get_page_no();

        m_buf_pool->mutex_release();

        /* Try to flush also all the neighbors */
        page_count += try_neighbors(dblwr, space, page_no, flush_type);

        m_buf_pool->mutex_acquire();
        goto flush_next;

      } else if (flush_type == BUF_FLUSH_LRU) {
        bpage = UT_LIST_GET_PREV(m_LRU_list, bpage);
      } else {
        ut_ad(flush_type == BUF_FLUSH_LIST);

        bpage = UT_LIST_GET_PREV(m_list, bpage);
        ut_ad(!bpage || bpage->m_in_flush_list);
      }
    } while (bpage != nullptr);

    /* If we could not find anything to flush, leave the loop */

    break;
  }

  m_buf_pool->m_init_flush[flush_type] = false;

  if (m_buf_pool->m_n_flush[flush_type] == 0) {

    /* The running flush batch has ended */

    os_event_set(m_buf_pool->m_no_flush[flush_type]);
  }

  m_buf_pool->mutex_release();

  buffered_writes(dblwr);

  srv_buf_pool_flushed += page_count;

  /* We keep track of all flushes happening as part of LRU
  flush. When estimating the desired rate at which m_flush_list
  should be flushed we factor in this value. */
  if (flush_type == BUF_FLUSH_LRU) {
    m_LRU_flush_page_count += page_count;
  }

  return page_count;
}

void Buf_flush::wait_batch_end(buf_flush type) {
  ut_ad(type == BUF_FLUSH_LRU || type == BUF_FLUSH_LIST);

  os_event_wait(m_buf_pool->m_no_flush[type]);
}

ulint Buf_flush::LRU_recommendation() {
  Buf_page *bpage;
  ulint n_replaceable;
  ulint distance = 0;

  m_buf_pool->mutex_acquire();

  n_replaceable = UT_LIST_GET_LEN(m_buf_pool->m_free_list);

  bpage = UT_LIST_GET_LAST(m_buf_pool->m_LRU_list);

  while (bpage != nullptr && n_replaceable < get_free_block_margin() + get_extra_margin() &&
         (distance < Buf_LRU::get_free_search_len())) {

    auto block_mutex = buf_page_get_mutex(bpage);

    mutex_enter(block_mutex);

    if (ready_for_replace(bpage)) {
      n_replaceable++;
    }

    mutex_exit(block_mutex);

    ++distance;

    bpage = UT_LIST_GET_PREV(m_LRU_list, bpage);
  }

  m_buf_pool->mutex_release();

  if (n_replaceable >= get_free_block_margin()) {

    return 0;

  } else {

    return get_free_block_margin() + get_extra_margin() - n_replaceable;
  }
}

void Buf_flush::free_margin(DBLWR *dblwr) {
  auto n_to_flush = LRU_recommendation();

  if (n_to_flush > 0) {

    const auto n_flushed = batch(dblwr, BUF_FLUSH_LRU, n_to_flush, 0);

    if (n_flushed == ULINT_UNDEFINED) {
      /* There was an LRU type flush batch already running;
      let us wait for it to end */

      wait_batch_end(BUF_FLUSH_LRU);
    }
  }
}

void Buf_flush::stat_update() {
  auto lsn = log_sys->get_lsn();

  if (m_stat_cur.m_redo == 0) {
    /* First time around. Just update the current LSN and return. */
    m_stat_cur.m_redo = lsn;
    return;
  }

  auto item = &m_stats[m_stat_ind];

  /* values for this interval */
  auto lsn_diff = lsn - m_stat_cur.m_redo;
  auto n_flushed = m_LRU_flush_page_count - m_stat_cur.m_n_flushed;

  /* add the current value and subtract the obsolete entry. */
  m_stat_sum.m_redo += lsn_diff - item->m_redo;
  m_stat_sum.m_n_flushed += n_flushed - item->m_n_flushed;

  /* put current entry in the array. */
  item->m_redo = lsn_diff;
  item->m_n_flushed = n_flushed;

  /* update the index */
  ++m_stat_ind;
  m_stat_ind %= m_stats.size();

  /* reset the current entry. */
  m_stat_cur.m_redo = lsn;
  m_stat_cur.m_n_flushed = m_LRU_flush_page_count;
}

ulint Buf_flush::get_desired_flush_rate() {
  auto lsn = log_sys->get_lsn();
  auto log_capacity = log_sys->get_capacity();

  /* log_capacity should never be zero after the initialization of log subsystem. */
  ut_ad(log_capacity != 0);

  /* Get total number of dirty pages. It is OK to access m_flush_list without holding
  any mtex as we are using this only for heuristics. */
  auto n_dirty = UT_LIST_GET_LEN(m_buf_pool->m_flush_list);

  /* An overflow can happen if we generate more than 2^32 bytes of redo in this
  interval i.e.: 4G of redo in 1 second. We can safely consider this as infinity
  because if we ever come close to 4G we'll start a synchronous flush of dirty pages.

  Redo_avg below is average at which redo is generated in past STAT_N_INTERVAL
  redo generated in the current interval. */
  auto redo_avg = ulint(m_stat_sum.m_redo / STAT_N_INTERVAL + (lsn - m_stat_cur.m_redo));

  /* An overflow can happen possibly if we flush more than 2^32 pages in STAT_N_INTERVAL.
  This is a very very unlikely scenario. Even when this happens it means that our flush
  rate will be off the mark. It won't affect correctness of any subsystem.

  lru_flush_avg below is rate at which pages are flushed as part of LRU flush in
  past STAT_N_INTERVAL the number of pages flushed in the current interval. */

  auto lru_flush_avg = m_stat_sum.m_n_flushed / STAT_N_INTERVAL + (m_LRU_flush_page_count - m_stat_cur.m_n_flushed);

  auto n_flush_req = (n_dirty * redo_avg) / log_capacity;

  /* The number of pages that we want to flush from the flush
  list is the difference between the required rate and the
  number of pages that we are historically flushing from the
  LRU list */
  lint rate = n_flush_req - lru_flush_avg;

  return rate > 0 ? ulint(rate) : 0;
}

#if defined UNIV_DEBUG
bool Buf_flush::validate_low() {
  std::optional<Buf_page_set_itr> rnode{};

  UT_LIST_CHECK(m_buf_pool->m_flush_list);

  auto bpage = UT_LIST_GET_FIRST(m_buf_pool->m_flush_list);

  /* If we are in recovery mode i.e.: m_recovery_flush_list != nullptr
  then each block in the m_flush_list must also be present
  in the m_recovery_flush_list. */
  if (m_buf_pool->m_recovery_flush_list != nullptr) {
    rnode = flush_set_first(m_buf_pool->m_recovery_flush_list);
  }

  while (bpage != nullptr) {
    const uint64_t om = bpage->m_oldest_modification;
    ut_ad(bpage->m_in_flush_list);
    ut_a(bpage->in_file());
    ut_a(om > 0);

    if (m_buf_pool->m_recovery_flush_list != nullptr) {
      ut_a(rnode.has_value());

      auto rpage = *rnode.value();
      ut_a(rpage != nullptr);
      ut_a(rpage == bpage);

      rnode = flush_set_next(m_buf_pool->m_recovery_flush_list, rnode.value());
    }

    bpage = UT_LIST_GET_NEXT(m_list, bpage);

    ut_a(!bpage || om >= bpage->m_oldest_modification);
  }

  /* By this time we must have exhausted the traversal of
  m_recovery_flush_list (if active) as well. */
  ut_a(!rnode.has_value());

  return true;
}

bool Buf_flush::validate() {
  m_buf_pool->mutex_acquire();

  auto ret = validate_low();

  m_buf_pool->mutex_release();

  return ret;
}
#endif /* UNIV_DEBUG */
