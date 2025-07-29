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

/** @file log/log_checkpoint.cc
Log checkpoint management

WAL checkpoint code.
*******************************************************/

#include "buf0flu.h"
#include "fil0fil.h"
#include "log0log.h"
#include "log0recv.h"
#include "os0aio.h"
#include "srv0srv.h"

extern ulint log_fsp_current_free_limit;

constexpr ulint LOG_CHECKPOINT_FREE_PER_THREAD = 4 * UNIV_PAGE_SIZE;
constexpr ulint LOG_CHECKPOINT_EXTRA_FREE = 8 * UNIV_PAGE_SIZE;
constexpr ulint LOG_POOL_CHECKPOINT_RATIO_ASYNC = 32;

void Log::checkpoint_set_nth_group_info(byte *buf, ulint n, ulint file_no, ulint offset) noexcept {
  ut_ad(n < LOG_MAX_N_GROUPS);

  mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_FILE_NO, file_no);
  mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_OFFSET, offset);
}

void Log::checkpoint_get_nth_group_info(const byte *buf, ulint n, ulint *file_no, ulint *offset) noexcept {
  ut_ad(n < LOG_MAX_N_GROUPS);

  *file_no = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_FILE_NO);

  *offset = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_OFFSET);
}

void Log::group_checkpoint(log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  static_assert(LOG_CHECKPOINT_SIZE <= IB_FILE_BLOCK_SIZE, "error LOG_CHECKPOINT_SIZE > IB_FILE_BLOCK_SIZE");

  auto buf = group->checkpoint_buf;

  mach_write_to_8(buf + LOG_CHECKPOINT_NO, m_next_checkpoint_no);
  mach_write_to_8(buf + LOG_CHECKPOINT_LSN, m_next_checkpoint_lsn);
  mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, group_calc_lsn_offset(m_next_checkpoint_lsn, group));
  mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, m_buf_size);
  mach_write_to_8(buf + LOG_CHECKPOINT_UNUSED_LSN, IB_UINT64_T_MAX);

  for (ulint i{}; i < LOG_MAX_N_GROUPS; ++i) {
    checkpoint_set_nth_group_info(buf, i, 0, 0);
  }

  auto group2 = UT_LIST_GET_FIRST(m_log_groups);

  while (group2 != nullptr) {
    checkpoint_set_nth_group_info(buf, group2->id, 0, 0);
    group2 = UT_LIST_GET_NEXT(log_groups, group2);
  }

  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_FREE_LIMIT, log_fsp_current_free_limit);
  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_MAGIC_N, LOG_CHECKPOINT_FSP_MAGIC_N_VAL);

  ulint write_offset;

  if ((m_next_checkpoint_no & 1) == 0) {
    write_offset = LOG_CHECKPOINT_1;
  } else {
    write_offset = LOG_CHECKPOINT_2;
  }

  if (log_do_write) {
    if (m_n_pending_checkpoint_writes == 0) {
      rw_lock_x_lock_gen(&m_checkpoint_lock, LOG_CHECKPOINT);
    }

    m_n_pending_checkpoint_writes++;
    ++m_n_log_ios;

    log_io(
      IO_request::Async_log_write,
      false,
      Page_id(group->space_id, write_offset / UNIV_PAGE_SIZE),
      write_offset % UNIV_PAGE_SIZE,
      IB_FILE_BLOCK_SIZE,
      buf,
      ((byte *)group + 1)
    );

    ut_ad(((ulint)group & 0x1UL) == 0);
  }
}

void Log::groups_write_checkpoint_info(void) noexcept {
  ut_ad(mutex_own(&m_mutex));

  for (auto group : m_log_groups) {
    group_checkpoint(group);
  }
}

bool Log::checkpoint(bool sync, bool write_always) noexcept {
  if (recv_recovery_on) {
    recv_apply_log_recs(srv_dblwr, false);
  }

  if (srv_config.m_unix_file_flush_method != SRV_UNIX_NOSYNC) {
    srv_fil->flush_file_spaces(FIL_TABLESPACE);
  }

  acquire();

  const auto oldest_lsn = buf_pool_get_oldest_modification();

  release();

  write_up_to(oldest_lsn, LOG_WAIT_ALL_GROUPS, true);

  acquire();

  if (!write_always && m_last_checkpoint_lsn >= oldest_lsn) {
    release();
    return true;
  }

  ut_ad(m_flushed_to_disk_lsn >= oldest_lsn);

  if (m_n_pending_checkpoint_writes > 0) {
    release();

    if (sync) {
      rw_lock_s_lock(&m_checkpoint_lock);
      rw_lock_s_unlock(&m_checkpoint_lock);
    }

    return false;
  }

  m_next_checkpoint_lsn = oldest_lsn;

  groups_write_checkpoint_info();

  release();

  if (sync) {
    rw_lock_s_lock(&m_checkpoint_lock);
    rw_lock_s_unlock(&m_checkpoint_lock);
  }

  return true;
}

void Log::make_checkpoint_at(lsn_t lsn, bool write_always) noexcept {
  while (!preflush_pool_modified_pages(lsn, true)) {
    ;
  }

  while (!checkpoint(true, write_always)) {
    ;
  }
}

void Log::checkpoint_margin() noexcept {
  bool success;
  uint64_t advance;

  for (;;) {
    auto sync = false;
    auto checkpoint_sync = false;
    auto do_checkpoint = false;

    acquire();

    if (!m_check_flush_or_checkpoint) {
      release();
      return;
    }

    auto oldest_lsn = buf_pool_get_oldest_modification();
    auto age = m_lsn - oldest_lsn;

    if (age > m_max_modified_age_sync) {
      sync = true;
      advance = 2 * (age - m_max_modified_age_sync);
    } else if (age > m_max_modified_age_async) {
      advance = age - m_max_modified_age_async;
    } else {
      advance = 0;
    }

    auto checkpoint_age = m_lsn - m_last_checkpoint_lsn;

    if (checkpoint_age > m_max_checkpoint_age) {
      checkpoint_sync = true;
      do_checkpoint = true;
    } else if (checkpoint_age > m_max_checkpoint_age_async) {
      do_checkpoint = true;
      m_check_flush_or_checkpoint = false;
    } else {
      m_check_flush_or_checkpoint = false;
    }

    release();

    if (advance) {
      auto new_oldest = oldest_lsn + advance;

      success = preflush_pool_modified_pages(new_oldest, sync);

      if (sync && !success) {
        continue;
      }
    }

    if (do_checkpoint) {
      auto success = checkpoint(checkpoint_sync, false);

      if (!success) {
        log_info("Checkpoint was already running, waiting for it to complete");
      }

      if (checkpoint_sync) {
        continue;
      }
    }

    break;
  }
}

void Log::complete_checkpoint() noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(m_n_pending_checkpoint_writes == 0);

  m_next_checkpoint_no++;
  m_last_checkpoint_lsn = m_next_checkpoint_lsn;

  rw_lock_x_unlock_gen(&m_checkpoint_lock, LOG_CHECKPOINT);
}

void Log::io_complete_checkpoint() noexcept {
  acquire();

  ut_ad(m_n_pending_checkpoint_writes > 0);

  --m_n_pending_checkpoint_writes;

  if (m_n_pending_checkpoint_writes == 0) {
    complete_checkpoint();
  }

  release();
}
