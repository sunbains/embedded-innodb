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

#include "buf0buf.h"
#include "log0log.h"
#include "mem0mem.h"
#include "srv0srv.h"

constexpr ulint LOG_BUF_WRITE_MARGIN = 4 * IB_FILE_BLOCK_SIZE;
constexpr ulint LOG_BUF_FLUSH_RATIO = 2;
constexpr ulint LOG_UNLOCK_FLUSH_LOCK = 2;
constexpr ulint LOG_BUF_FLUSH_MARGIN = LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE;

lsn_t Log::reserve_and_open(ulint len) noexcept {
  IF_DEBUG(ulint count = 0;)

  ut_a(len < m_buf_size / 2);

  for (;;) {
    acquire();

    auto len_upper_limit = LOG_BUF_WRITE_MARGIN + (5 * len) / 4;

    if (m_buf_free + len_upper_limit > m_buf_size) {
      release();

      buffer_flush_to_disk();

      srv_log_waits++;

      ut_ad(++count < 50);
    } else {
      break;
    }
  }

  return m_lsn;
}

void Log::write_low(const byte *str, ulint str_len) noexcept {
  ut_ad(mutex_own(&m_mutex));

  while (str_len > 0) {
    auto data_len = (m_buf_free % sizeof(Log_phy_block)) + str_len;

    ulint len;

    if (data_len <= IB_FILE_BLOCK_SIZE - Log_phy_block::TRAILER_SIZE) {
      len = str_len;
    } else {
      data_len = Log_phy_block::DATA_SIZE;
      len = sizeof(Log_phy_block) - (m_buf_free % sizeof(Log_phy_block)) - Log_phy_block::TRAILER_SIZE;
    }

    auto log_block = static_cast<Log_phy_block *>(ut_align_down(m_buf + m_buf_free, sizeof(Log_phy_block)));

    log_block->copy(m_buf_free, str, len);

    str_len -= len;
    str += len;

    log_block->set_data_len(data_len);

    if (data_len == IB_FILE_BLOCK_SIZE - Log_phy_block::TRAILER_SIZE) {
      log_block->set_data_len(Log_phy_block::DATA_SIZE);
      log_block->set_checkpoint_no(m_checkpoint.m_next_no);
      len += Log_phy_block::HEADER_SIZE + Log_phy_block::TRAILER_SIZE;

      m_lsn += len;

      log_block->set_block_no(Log_vfile::convert_lsn_to_block_no(m_lsn));
      log_block->set_first_rec_group(0);
      log_block->set_data_len(Log_phy_block::HEADER_SIZE);
    } else {
      m_lsn += len;
    }

    m_buf_free += len;

    ut_ad(m_buf_free <= m_buf_size);
  }

  ++srv_log_write_requests;
}

void Log::buffer_flush_to_disk() noexcept {
  acquire();

  auto lsn = m_lsn;

  release();

  write_up_to(lsn, false, true);
}

void Log::buffer_sync_in_background(bool flush) noexcept {
  mutex_enter(&m_mutex);

  auto lsn = m_lsn;

  mutex_exit(&m_mutex);

  write_up_to(lsn, true, flush);
}

void Log::flush_margin() noexcept {
  lsn_t lsn;

  acquire();

  if (m_buf_free > m_max_buf_free) {

    if (m_n_pending_writes > 0) {
      // A flush is running: hope that it will provide enough free space
    } else {
      lsn = m_lsn;
    }
  }

  release();

  if (lsn != 0) {
    write_up_to(lsn, true, false);
  }
}

ulint Log::sys_check_flush_completion() noexcept {
  ulint move_start;
  ulint move_end;

  ut_ad(mutex_own(&m_mutex));

  if (m_n_pending_writes == 0) {

    m_written_to_all_lsn = m_write_lsn;
    m_buf_next_to_write = m_write_end_offset;

    if (m_write_end_offset > m_max_buf_free / 2) {
      move_start = ut_calc_align_down(m_write_end_offset, IB_FILE_BLOCK_SIZE);
      move_end = ut_calc_align(m_buf_free, IB_FILE_BLOCK_SIZE);

      memmove(m_buf, m_buf + move_start, move_end - move_start);
      m_buf_free -= move_start;

      m_buf_next_to_write -= move_start;
    }

    return LOG_UNLOCK_FLUSH_LOCK;
  }

  return 0;
}
