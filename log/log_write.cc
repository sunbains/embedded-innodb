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

/** @file log/log_write.cc
Log write operations

*******************************************************/

#include "fil0fil.h"
#include "log0log.h"
#include "srv0srv.h"

constexpr ulint LOG_UNLOCK_NONE_FLUSHED_LOCK = 1;
constexpr ulint LOG_UNLOCK_FLUSH_LOCK = 2;

void Log::write_up_to(lsn_t lsn, bool async, bool flush_to_disk) noexcept {
  ulint start_offset;
  ulint end_offset;
  ulint area_start;
  ulint area_end;
  ulint unlock;

  IF_DEBUG(ulint loop_count = 0;)

  auto do_waits = [this](bool async) {
    if (!async) {
      os_event_wait(m_no_flush_event);
    }
  };

  for (;;) {
    ut_ad(++loop_count < 5);

    acquire();

    if (flush_to_disk && m_flushed_to_disk_lsn >= lsn) {
      release();
      return;
    }

    if (!flush_to_disk && (m_written_to_all_lsn >= lsn || (m_written_to_some_lsn >= lsn && async))) {
      release();
      return;
    }

    if (m_n_pending_writes > 0) {
      if (flush_to_disk && m_current_flush_lsn >= lsn) {
        release();
        do_waits(async);
        break;
      }

      if (!flush_to_disk && m_write_lsn >= lsn) {
        release();
        do_waits(async);
        break;
      }

      release();
      os_event_wait(m_no_flush_event);
      continue;
    }

    if (!flush_to_disk && m_buf_free == m_buf_next_to_write) {
      release();
      return;
    }

    ++m_n_pending_writes;

    ++m_vfile.m_n_pending_writes;

    os_event_reset(m_no_flush_event);
    os_event_reset(m_one_flushed_event);

    start_offset = m_buf_next_to_write;
    end_offset = m_buf_free;

    area_start = ut_calc_align_down(start_offset, IB_FILE_BLOCK_SIZE);
    area_end = ut_calc_align(end_offset, IB_FILE_BLOCK_SIZE);

    ut_ad(area_end - area_start > 0);

    m_write_lsn = m_lsn;

    if (flush_to_disk) {
      m_current_flush_lsn = m_lsn;
    }

    m_one_flushed = false;

    auto log_block = static_cast<Log_phy_block *>(ut_align_down(m_buf + area_start, sizeof(Log_phy_block)));

    log_block->set_flush_bit(true);
    log_block->set_checkpoint_no(m_checkpoint.m_next_no);

    memcpy(m_buf + area_end, m_buf + area_end - IB_FILE_BLOCK_SIZE, IB_FILE_BLOCK_SIZE);

    m_buf_free += IB_FILE_BLOCK_SIZE;
    m_write_end_offset = m_buf_free;

    ut_a(m_vfile.write_segment(
      m_buf + area_start, area_end - area_start, ut_uint64_align_down(m_written_to_all_lsn, sizeof(Log_phy_block))
    ));

    m_vfile.set_fields(m_write_lsn);

    release();

    if (srv_config.m_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
      m_flushed_to_disk_lsn = m_write_lsn;
    } else if (flush_to_disk) {
      // FIXME: Sync the log group to disk
      m_flushed_to_disk_lsn = m_write_lsn;
    }

    acquire();

    ut_a(m_vfile.m_n_pending_writes == 1);
    ut_a(m_n_pending_writes == 1);

    --m_vfile.m_n_pending_writes;
    --m_n_pending_writes;

    unlock = m_vfile.check_flush_completion();
    unlock |= sys_check_flush_completion();

    flush_do_unlocks(unlock);

    release();

    return;
  }
}

void Log::flush_do_unlocks(ulint code) noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (code & LOG_UNLOCK_NONE_FLUSHED_LOCK) {
    os_event_set(m_one_flushed_event);
  }

  if (code & LOG_UNLOCK_FLUSH_LOCK) {
    os_event_set(m_no_flush_event);
  }
}
