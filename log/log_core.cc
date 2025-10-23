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

/** @file log/log_core.cc
WAL - LSN calculations, block operations, ages

*******************************************************/

#include "buf0flu.h"
#include "log0log.h"
#include "log0recv.h"
#include "mem0mem.h"
#include "srv0srv.h"

constexpr ulint LOG_POOL_PREFLUSH_RATIO_SYNC = 32;
constexpr ulint LOG_POOL_PREFLUSH_RATIO_ASYNC = 6;
constexpr ulint LOG_CHECKPOINT_FREE_PER_THREAD = 4 * UNIV_PAGE_SIZE;
constexpr ulint LOG_CHECKPOINT_EXTRA_FREE = 8 * UNIV_PAGE_SIZE;
constexpr ulint LOG_POOL_CHECKPOINT_RATIO_ASYNC = 32;

extern bool log_has_printed_chkp_warning;
extern time_t log_last_warning_time;

bool Log::calc_max_ages() noexcept {
  acquire();

  auto smallest_capacity = ULINT_MAX;

  auto capacity = m_vfile.get_capacity();

  capacity = capacity - smallest_capacity / 10;

  auto free = LOG_CHECKPOINT_FREE_PER_THREAD * 10 + LOG_CHECKPOINT_EXTRA_FREE;

  ulint margin{};
  bool success = true;

  if (free >= smallest_capacity / 2) {
    success = false;
  } else {
    margin = capacity - free;
  }

  margin = std::min<ulint>(margin, m_checkpoint.m_adm_interval);
  margin = margin - margin / 10;

  m_checkpoint.m_max_modified_age_async = margin - margin / LOG_POOL_PREFLUSH_RATIO_ASYNC;
  m_checkpoint.m_max_modified_age_sync = margin - margin / LOG_POOL_PREFLUSH_RATIO_SYNC;
  m_checkpoint.m_max_age_async = margin - margin / LOG_POOL_CHECKPOINT_RATIO_ASYNC;
  m_checkpoint.m_max_age = margin;

  release();

  if (!success) {
    log_fatal(
      "ib_logfiles are too small for thread_concurrency setting. The combined size of ib_logfiles"
      " should be bigger than 200 kB. To get the server to start up, set thread_concurrency variable"
      " to a lower value, for example, to 8. After an ERROR-FREE shutdown of the server you can"
      " adjust the size of ib_logfiles, as explained on he InnoDB website. Cannot continue operation."
      " Forcing shutdown."
    );
  }

  return success;
}

lsn_t Log::close(ib_recovery_t recovery) noexcept {
  auto log_block = static_cast<Log_phy_block *>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE));
  auto first_rec_group = log_block->get_first_rec_group();
  auto lsn = m_lsn;
  auto checkpoint_age = lsn - m_checkpoint.m_last_lsn;
  auto oldest_lsn = srv_buf_pool->get_oldest_modification();

  ut_ad(mutex_own(&m_mutex));

  if (first_rec_group == 0) {
    log_block->set_first_rec_group(log_block->get_data_len());
  }

  if (m_buf_free > m_max_buf_free) {
    m_check_flush_or_checkpoint = true;
  }

  if (checkpoint_age >= m_vfile.get_capacity()) {
    if (!log_has_printed_chkp_warning || difftime(time(nullptr), log_last_warning_time) > 15) {
      log_has_printed_chkp_warning = true;
      log_last_warning_time = time(nullptr);

      log_err(std::format(
        "The age of the last checkpoint is {}, which exceeds the log group"
        " capacity {}. If you are using big BLOB or TEXT rows, you must set the"
        " combined size of log files at least 10 times bigger than the largest"
        " such row.",
        checkpoint_age,
        m_vfile.get_capacity()
      ));
    }
  }

  if (checkpoint_age > m_checkpoint.m_max_modified_age_async ||
      (oldest_lsn > 0 && lsn - oldest_lsn > m_checkpoint.m_max_modified_age_async) ||
      checkpoint_age > m_checkpoint.m_max_age_async) {
    m_check_flush_or_checkpoint = true;
  }

  return lsn;
}

lsn_t Log::buf_pool_get_oldest_modification() noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto lsn = srv_buf_pool->get_oldest_modification();

  if (lsn == 0) {
    lsn = m_lsn;
  }

  return lsn;
}

bool Log::preflush_pool_modified_pages(lsn_t new_oldest, bool sync) noexcept {
  if (recv_recovery_on) {
    recv_apply_log_recs(srv_dblwr, false);
  }

  auto n_pages = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, ULINT_MAX, new_oldest);

  if (sync) {
    srv_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);
  }

  return n_pages != ULINT_UNDEFINED;
}

void Log::check_margins() noexcept {
  for (;;) {
    flush_margin();
    checkpoint_margin();

    acquire();

    if (!m_check_flush_or_checkpoint) {
      break;
    }

    release();
  }

  release();
}

void Log::free_check() noexcept {
  if (m_check_flush_or_checkpoint) {
    check_margins();
  }
}

void Log::block_store_checksum(Log_phy_block *phy_block) noexcept {
  phy_block->set_checksum(log_calc_checksum(phy_block->data(), phy_block->get_data_len()));
}

bool Log::peek_lsn(lsn_t *lsn) noexcept {
  if (mutex_enter_nowait(&m_mutex) == 0) {
    *lsn = m_lsn;
    release();
    return true;
  } else {
    return false;
  }
}

void Log::print() noexcept {
  acquire();

  log_info(std::format(
    "Log sequence number {}\n"
    "Log flushed up to   {}\n"
    "Last checkpoint at  {}\n",
    m_lsn,
    m_flushed_to_disk_lsn,
    m_checkpoint.m_last_lsn
  ));

  auto current_time = time(nullptr);
  auto time_elapsed = 0.001 + difftime(current_time, m_last_printout_time);

  log_info(std::format(
    "{} pending log writes, {} pending chkp writes"
    " {} log i/o's done, {:2} log i/o's/second",
    m_n_pending_writes,
    m_checkpoint.m_n_pending_writes,
    m_n_log_ios,
    double((m_n_log_ios - m_n_log_ios_old) / time_elapsed)
  ));

  m_n_log_ios_old = m_n_log_ios;
  m_last_printout_time = current_time;

  release();
}

void Log::refresh_stats() noexcept {
  m_n_log_ios_old = m_n_log_ios;
  m_last_printout_time = time(nullptr);
}
