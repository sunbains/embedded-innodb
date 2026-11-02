/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file log/recv_recovery.cc
Recovery operations - page recovery and application

*******************************************************/

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "log0recv.h"
#include "srv0srv.h"

constexpr ulint RECV_READ_AHEAD_AREA = 32;
constexpr ulint RECV_DATA_BLOCK_SIZE = MEM_MAX_ALLOC_IN_BUF - sizeof(LogRecordData);

static PageLogRecords *recv_get_log_record(System &sys, space_id_t space, page_no_t page_no) noexcept {
  auto space_it = sys.m_recv->m_log_records.find(space);

  if (space_it == sys.m_recv->m_log_records.end()) {
    return nullptr;
  }

  auto &page_map = space_it->second;
  auto page_it = page_map.find(page_no);

  return page_it == page_map.end() ? nullptr : page_it->second;
}

static void recv_data_copy_to_buf(byte *buf, LogRecord *recv) noexcept {
  auto len = recv->m_len;
  auto recv_data = recv->m_data;

  while (len > 0) {
    const auto part_len = (len > RECV_DATA_BLOCK_SIZE) ? RECV_DATA_BLOCK_SIZE : len;
    memcpy(buf, reinterpret_cast<byte *>(recv_data) + sizeof(LogRecordData), part_len);
    buf += part_len;
    len -= part_len;
    recv_data = recv_data->m_next;
  }
}

void recv_recover_page(System &sys, bool just_read_in, BufBlock *block) noexcept {
  Mtr mtr;

  mutex_enter(&sys.m_recv->m_mutex);

  if (!sys.m_recv->m_apply_log_recs) {
    mutex_exit(&sys.m_recv->m_mutex);
    return;
  }

  auto log_record = recv_get_log_record(block->get_space(), block->get_page_no());

  if (log_record == nullptr || log_record->m_state == RECV_BEING_PROCESSED || log_record->m_state == RECV_PROCESSED) {
    mutex_exit(&sys.m_recv->m_mutex);
    return;
  }

  log_record->m_state = RECV_BEING_PROCESSED;

  mutex_exit(&sys.m_recv->m_mutex);

  mtr.start();

  {
    auto old_mode = mtr.set_log_mode(MTR_LOG_NONE);
    ut_a(old_mode == MTR_LOG_ALL);
  }

  auto page = block->m_frame;

  if (just_read_in) {
    rw_lock_x_lock_move_ownership(&block->m_rw_lock);
  }

  BufPool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_guess = block, .m_mode = BUF_KEEP_OLD, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
  };

  auto success = sys.m_buf_pool->try_get_known_nowait(req);
  ut_a(success);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(lock, SYNC_NO_ORDER_CHECK));

  lsn_t page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);
  auto page_newest_lsn = buf_page_get_newest_modification(&block->m_page);

  if (page_newest_lsn) {
    page_lsn = page_newest_lsn;
  }

  lsn_t end_lsn{};
  lsn_t start_lsn{};
  bool modification_to_page{};

  ut_a(log_record->m_rec_head != nullptr);

  for (auto recv{log_record->m_rec_head}; recv != nullptr; recv = recv->m_next) {
    byte *buf;
    byte *ptr{};

    end_lsn = recv->m_end_lsn;

    if (recv->m_len > RECV_DATA_BLOCK_SIZE) {
      ptr = buf = static_cast<byte *>(mem_alloc(recv->m_len));
      recv_data_copy_to_buf(buf, recv);
    } else {
      buf = reinterpret_cast<byte *>(recv->m_data) + sizeof(LogRecordData);
    }

    if (recv->m_type == MLOG_INIT_FILE_PAGE) {
      page_lsn = page_newest_lsn;

      memset(FIL_PAGE_LSN + page, 0, 8);
      memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + page, 0, 8);
    }

    if (recv->m_start_lsn >= page_lsn) {

      if (!modification_to_page) {
        modification_to_page = true;
        start_lsn = recv->m_start_lsn;
      }

      recv_parse_or_apply_log_rec_body(recv->m_type, buf, buf + recv->m_len, block, &mtr);

      end_lsn = recv->m_start_lsn + recv->m_len;

      mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
      mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + page, end_lsn);
    }

    if (ptr != nullptr) {
      mem_free(ptr);
    }
  }

  mutex_enter(&sys.m_recv->m_mutex);

  ut_a(sys.m_recv->m_n_log_records > 0);
  --sys.m_recv->m_n_log_records;

  if (recv_max_page_lsn < page_lsn) {
    recv_max_page_lsn = page_lsn;
  }

  log_record->m_state = RECV_PROCESSED;

  mutex_exit(&sys.m_recv->m_mutex);

  if (modification_to_page) {
    ut_a(block != nullptr);
    sys.m_buf_pool->m_flusher->recv_note_modification(block, start_lsn, end_lsn);
  }

  mtr.m_modifications = false;
  mtr.commit(sys);
}

static ulint recv_read_in_area(System &sys, space_id_t space, page_no_t start_page_no) noexcept {
  std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;
  const auto low_limit = start_page_no - (start_page_no % RECV_READ_AHEAD_AREA);

  ulint n = 0;

  for (auto page_no = low_limit; page_no < low_limit + RECV_READ_AHEAD_AREA; ++page_no) {
    auto log_record = recv_get_log_record(space, page_no);

    if (log_record != nullptr && !sys.m_buf_pool->peek(PageId(space, page_no))) {

      mutex_enter(&sys.m_recv->m_mutex);

      if (log_record->m_state == RECV_NOT_PROCESSED) {
        log_record->m_state = RECV_BEING_READ;

        page_nos[n] = page_no;
        n++;
      }

      mutex_exit(&sys.m_recv->m_mutex);
    }
  }

  buf_read_recv_pages(false, PageId(space, 0), page_nos.data(), n);

  return n;
}

void recv_apply_log_recs(System &sys, Dblwr *dblwr, bool flush_and_free_pages) noexcept {
  for (;;) {
    mutex_enter(&sys.m_recv->m_mutex);

    if (!sys.m_recv->m_apply_batch_on) {
      break;
    }

    mutex_exit(&sys.m_recv->m_mutex);

    os_thread_sleep(100000);
  }

  sys.m_recv->m_apply_log_recs = true;
  sys.m_recv->m_apply_batch_on = true;

  const ulint n_total{sys.m_recv->m_n_log_records};

  ulint n_recs{};
  bool printed_header{};

  for (const auto &[space_id, log_records_map] : sys.m_recv->m_log_records) {

    for (const auto &[page_no, log_record] : log_records_map) {

      if (log_record->m_state == RECV_NOT_PROCESSED) {

        if (!printed_header) {
          log_info("Starting an apply batch of log records to the database");
          log_info_hdr("Progress in percents: ");
          printed_header = true;
        }

        mutex_exit(&sys.m_recv->m_mutex);

        if (sys.m_buf_pool->peek(PageId(space_id, page_no))) {

          Mtr mtr;

          mtr.start();

          BufPool::Request req{
            .m_rw_latch = RW_X_LATCH,
            .m_page_id = {space_id, page_no},
            .m_mode = BUF_GET,
            .m_file = __FILE__,
            .m_line = __LINE__,
            .m_mtr = &mtr
          };

          auto block = sys.m_buf_pool->get(req, nullptr);
          buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

          recv_recover_page(false, block);

          mtr.commit(sys);
        } else {
          recv_read_in_area(space_id, page_no);
        }

        mutex_enter(&sys.m_recv->m_mutex);
      }

      const auto pct{(n_recs * 100) / n_total};

      if (printed_header && pct != ((n_recs + 1) * 100) / n_total) {
        log_info_msg(std::format("{} ", pct));
      }

      ++n_recs;
    }
  }

  while (sys.m_recv->m_n_log_records != 0) {

    mutex_exit(&sys.m_recv->m_mutex);

    os_thread_sleep(100000);

    mutex_enter(&sys.m_recv->m_mutex);
  }

  if (printed_header) {
    log_info_msg("\n");
  }

  if (flush_and_free_pages) {
    mutex_exit(&sys.m_recv->m_mutex);
    sys.m_log->release();

    auto n_pages = sys.m_buf_pool->m_flusher->batch(dblwr, BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX);
    ut_a(n_pages != ULINT_UNDEFINED);

    sys.m_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);
    sys.m_buf_pool->invalidate();

    sys.m_log->acquire();
    mutex_enter(&sys.m_recv->m_mutex);
  }

  sys.m_recv->m_apply_log_recs = false;
  sys.m_recv->m_apply_batch_on = false;

  sys.m_recv->reset();

  if (printed_header) {
    log_info("Apply batch completed");
  }

  mutex_exit(&sys.m_recv->m_mutex);
}
