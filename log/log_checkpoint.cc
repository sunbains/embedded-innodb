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

#include "buf0flu.h"
#include "fil0fil.h"
#include "log0log.h"
#include "log0recv.h"
#include "os0aio.h"
#include "srv0srv.h"

constexpr ulint LOG_CHECKPOINT_FREE_PER_THREAD = 4 * UNIV_PAGE_SIZE;
constexpr ulint LOG_CHECKPOINT_EXTRA_FREE = 8 * UNIV_PAGE_SIZE;
constexpr ulint LOG_POOL_CHECKPOINT_RATIO_ASYNC = 32;

Log_checkpoint::Log_checkpoint() noexcept {
  rw_lock_create(&m_lock, SYNC_NO_ORDER_CHECK);
}

Log_checkpoint::~Log_checkpoint() noexcept {
  rw_lock_free(&m_lock);
}

std::string Log_checkpoint::to_string() const noexcept {
  return std::format(
    "Checkpoint: next_no: {}, next_lsn: {}, last_lsn: {}, next_lsn: {},"
    " max_modified_age_async: {}, max_modified_age_sync: {}, interval: {}, max_age_async: {},"
    " max_age: {}, n_pending_writes: {}",
    m_next_no,
    m_next_lsn,
    m_last_lsn,
    m_next_lsn,
    m_max_modified_age_async,
    m_max_modified_age_sync,
    m_adm_interval,
    m_max_age_async,
    m_max_age,
    m_n_pending_writes
  );
}

ulint Log_checkpoint::prepare(Log &log) noexcept {
  ut_ad(mutex_own(&log.m_mutex));

  m_phy_checkpoint.set_no(m_next_no);
  m_phy_checkpoint.set_lsn(m_next_lsn);
  m_phy_checkpoint.set_offset(log.m_vfile.calc_lsn_offset(m_next_lsn));

  const auto data_len = sizeof(&m_phy_checkpoint - sizeof(m_phy_checkpoint.m_checksum));
  m_phy_checkpoint.set_checksum(log_calc_checksum(reinterpret_cast<const byte *>(&m_phy_checkpoint), data_len));

  return (m_next_no & 1) == 0 ? LOG_CHECKPOINT_1 : LOG_CHECKPOINT_2;
}

bool Log_checkpoint::persist(Log_vfile &log_group, off_t write_offset) noexcept {
  ut_ad(write_offset == LOG_CHECKPOINT_1 || write_offset == LOG_CHECKPOINT_2);

  ++m_n_pending_writes;

  return log_group.write_checkpoint(&m_phy_checkpoint, write_offset);
}

bool Log_checkpoint::read(off_t offset) noexcept {
  ut_ad(offset == LOG_CHECKPOINT_1 || offset == LOG_CHECKPOINT_2);

  // FIXME: Implement this
  return false;
}

void Log::checkpoint() noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto write_offset = m_checkpoint.prepare(*this);

  if (!m_disable_writes) {
    if (m_checkpoint.m_n_pending_writes == 0) {
      rw_lock_x_lock_gen(&m_checkpoint.m_lock, LOG_CHECKPOINT);
    }

    ++m_n_log_ios;

    m_checkpoint.persist(m_vfile, write_offset);
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

  write_up_to(oldest_lsn, false, true);

  acquire();

  if (!write_always && m_checkpoint.m_last_lsn >= oldest_lsn) {
    release();
    return true;
  }

  ut_ad(m_flushed_to_disk_lsn >= oldest_lsn);

  if (m_checkpoint.m_n_pending_writes > 0) {
    release();

    if (sync) {
      rw_lock_s_lock(&m_checkpoint.m_lock);
      rw_lock_s_unlock(&m_checkpoint.m_lock);
    }

    return false;
  }

  m_checkpoint.m_next_lsn = oldest_lsn;

  checkpoint();

  release();

  if (sync) {
    rw_lock_s_lock(&m_checkpoint.m_lock);
    rw_lock_s_unlock(&m_checkpoint.m_lock);
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

    if (age > m_checkpoint.m_max_modified_age_sync) {
      sync = true;
      advance = 2 * (age - m_checkpoint.m_max_modified_age_sync);
    } else if (age > m_checkpoint.m_max_modified_age_async) {
      advance = age - m_checkpoint.m_max_modified_age_async;
    } else {
      advance = 0;
    }

    auto checkpoint_age = m_lsn - m_checkpoint.m_last_lsn;

    if (checkpoint_age > m_checkpoint.m_max_age) {
      checkpoint_sync = true;
      do_checkpoint = true;
    } else if (checkpoint_age > m_checkpoint.m_max_age_async) {
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

void Log_checkpoint::complete() noexcept {
  ut_ad(m_n_pending_writes == 0);

  ++m_next_no;
  m_last_lsn = m_next_lsn;

  rw_lock_x_unlock_gen(&m_lock, LOG_CHECKPOINT);
}

void Log_checkpoint::io_complete() noexcept {

  ut_ad(m_n_pending_writes > 0);

  --m_n_pending_writes;

  if (m_n_pending_writes == 0) {
    complete();
  }
}

void Log::io_complete() noexcept {
  acquire();

  m_checkpoint.io_complete();

  release();
}
