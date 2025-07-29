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

void Log::write_up_to(lsn_t lsn, ulint wait, bool flush_to_disk) noexcept {
  log_group_t *group;
  ulint start_offset;
  ulint end_offset;
  ulint area_start;
  ulint area_end;
  ulint unlock;

  IF_DEBUG(ulint loop_count = 0;)

  auto do_waits = [this](ulint wait) {
    switch (wait) {
      case LOG_WAIT_ONE_GROUP:
        os_event_wait(m_one_flushed_event);
        break;
      case LOG_WAIT_ALL_GROUPS:
        os_event_wait(m_no_flush_event);
        break;
#ifdef UNIV_DEBUG
      case LOG_NO_WAIT:
        break;
      default:
        ut_error;
#endif
    }
  };

  for (;;) {
    ut_ad(++loop_count < 5);

    acquire();

    if (flush_to_disk && m_flushed_to_disk_lsn >= lsn) {
      release();
      return;
    }

    if (!flush_to_disk && (m_written_to_all_lsn >= lsn || (m_written_to_some_lsn >= lsn && wait != LOG_WAIT_ALL_GROUPS))) {
      release();
      return;
    }

    if (m_n_pending_writes > 0) {
      if (flush_to_disk && m_current_flush_lsn >= lsn) {
        release();
        do_waits(wait);
        break;
      }

      if (!flush_to_disk && m_write_lsn >= lsn) {
        release();
        do_waits(wait);
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

    group = UT_LIST_GET_FIRST(m_log_groups);
    ++group->n_pending_writes;

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

    block_set_flush_bit(m_buf + area_start, true);
    block_set_checkpoint_no(m_buf + area_end - IB_FILE_BLOCK_SIZE, m_next_checkpoint_no);

    memcpy(m_buf + area_end, m_buf + area_end - IB_FILE_BLOCK_SIZE, IB_FILE_BLOCK_SIZE);

    m_buf_free += IB_FILE_BLOCK_SIZE;
    m_write_end_offset = m_buf_free;

    for (auto group : m_log_groups) {
      group_write_buf(
        group,
        m_buf + area_start,
        area_end - area_start,
        ut_uint64_align_down(m_written_to_all_lsn, IB_FILE_BLOCK_SIZE),
        start_offset - area_start
      );

      group_set_fields(group, m_write_lsn);
    }

    release();

    if (srv_config.m_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
      m_flushed_to_disk_lsn = m_write_lsn;
    } else if (flush_to_disk) {
      group = UT_LIST_GET_FIRST(m_log_groups);
      srv_fil->flush(group->space_id);
      m_flushed_to_disk_lsn = m_write_lsn;
    }

    acquire();

    group = UT_LIST_GET_FIRST(m_log_groups);

    ut_a(group->n_pending_writes == 1);
    ut_a(m_n_pending_writes == 1);

    --group->n_pending_writes;
    --m_n_pending_writes;

    unlock = group_check_flush_completion(group);
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
