/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** @file log/log0log.c
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#include "log0log.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "dict0store.h"
#include "fil0fil.h"
#include "log0recv.h"
#include "mem0mem.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "trx0sys.h"

/*
General philosophy of InnoDB redo-logs:

1) Every change to a contents of a data page must be done
through mtr, which in mtr_commit() writes log records
to the InnoDB redo log.

2) Normally these changes are performed using a mlog_write_ulint()
or similar function.

3) In some page level operations only a code number of a
c-function and its parameters are written to the log to
reduce the size of the log.

  3a) You should not add parameters to these kind of functions
  (e.g. trx_undo_header_create(), trx_undo_insert_header_reuse())

  3b) You should not add such functionality which either change
  working when compared with the old or are dependent on data
  outside of the page. These kind of functions should implement
  self-contained page transformation and it should be unchanged
  if you don't have very essential reasons to change log
  semantics or format.

*/

IF_DEBUG(bool log_debug_writes = false;)

/* Global log system variable */
Log *log_sys = nullptr;

IF_DEBUG(bool log_do_write = true;)

/* These control how often we print warnings if the last checkpoint is too old */
bool log_has_printed_chkp_warning = false;

time_t log_last_warning_time;

/** A margin for free space in the log buffer before a log entry is catenated */
constexpr ulint LOG_BUF_WRITE_MARGIN = 4 * IB_FILE_BLOCK_SIZE;

/** Margins for free space in the log buffer after a log entry is catenated */
constexpr ulint LOG_BUF_FLUSH_RATIO = 2;
constexpr ulint LOG_BUF_FLUSH_MARGIN = LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE;

/** Margin for the free space in the smallest log group, before a new query
step which modifies the database, is started */

constexpr ulint LOG_CHECKPOINT_FREE_PER_THREAD = 4 * UNIV_PAGE_SIZE;
constexpr ulint LOG_CHECKPOINT_EXTRA_FREE = 8 * UNIV_PAGE_SIZE;

/** This parameter controls asynchronous making of a new checkpoint; the value
should be bigger than LOG_POOL_PREFLUSH_RATIO_SYNC */

constexpr ulint LOG_POOL_CHECKPOINT_RATIO_ASYNC = 32;

/** This parameter controls synchronous preflushing of modified buffer pages */
constexpr ulint LOG_POOL_PREFLUSH_RATIO_SYNC = 32;

/** The same ratio for asynchronous preflushing; this value should be less than
the previous */
constexpr ulint LOG_POOL_PREFLUSH_RATIO_ASYNC = 6;

/* Extra margin, in addition to one log file, used in archiving */

/* Codes used in unlocking flush latches */
constexpr ulint LOG_UNLOCK_NONE_FLUSHED_LOCK = 1;
constexpr ulint LOG_UNLOCK_FLUSH_LOCK = 2;

Log::Log(size_t size) noexcept : m_buffer(size) {
  mutex_create(&m_mutex, IF_DEBUG("log_sys_mutex", ) IF_SYNC_DEBUG(SYNC_LOG, ) Current_location());

  /* Start the LSN from one log block from zero: this way every
  log record has a start lsn != zero, a fact which we will use */

  m_lsn = LOG_START_LSN;

  ut_a(LOG_BUFFER_SIZE >= 16 * sizeof(Log_phy_block));

  m_check_flush_or_checkpoint = true;

  m_n_log_ios_old = m_n_log_ios;
  m_last_printout_time = time(nullptr);

  m_written_to_all_lsn = m_lsn;
  m_written_to_some_lsn = m_lsn;

  m_no_flush_event = os_event_create(nullptr);

  os_event_set(m_no_flush_event);

  m_one_flushed_event = os_event_create(nullptr);

  os_event_set(m_one_flushed_event);

  m_checkpoint.m_last_lsn = m_lsn;

  // const auto block_no = Log_vfile::convert_lsn_to_block_no(m_lsn);

  // log_block.prepare(block_no);

  m_lsn = LOG_START_LSN + Log_phy_block::HEADER_SIZE;
}

Log::~Log() noexcept {
  mutex_free(&m_mutex);
  os_event_free(m_no_flush_event);
  os_event_free(m_one_flushed_event);
}

void Log::do_checkpoint() noexcept {
  acquire();

  release();

  /* Try to make a synchronous checkpoint */

  while (!checkpoint(true, true)) {
    /* No op */
  }
}

lsn_t Log::buf_pool_get_oldest_modification() noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto lsn = srv_buf_pool->get_oldest_modification();

  if (lsn == 0) {
    lsn = m_lsn;
  }

  return lsn;
}

lsn_t Log::reserve_and_open(ulint len) noexcept {
  IF_DEBUG(ulint count = 0;)

  ut_a(len < m_buffer.size() / 2);

  for (;;) {
    acquire();

    /* Calculate an upper limit for the space the string may take in the log buffer */

    auto len_upper_limit = LOG_BUF_WRITE_MARGIN + (5 * len) / 4;

    if (m_buffer.size() + len_upper_limit > m_buffer.size()) {
      release();

      /* Not enough free space, do a synchronous flush of the log buffer */

      flush_buffer_to_disk();

      ++srv_log_waits;

      ut_ad(++count < 50);
    } else {
      break;
    }
  }

  return m_lsn;
}

void Log::append(const byte *ptr, ulint len) noexcept {
  ut_ad(mutex_own(&m_mutex));

  while (len > 0) {
    auto written = m_buffer.append(ptr, len);

    if (written == -1) {
      // FIXME: Write the log buffer to disk and resume writing data to it.
      log_fatal("TBD: Log buffer is full");
      return;
    }

    ptr += written;
    len -= written;
  }

  ++m_log_write_requests;
}

lsn_t Log::close(ib_recovery_t recovery) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto lsn = m_lsn;
  auto checkpoint_age = lsn - m_checkpoint.m_last_lsn;

  auto log_block = m_buffer.back();

  auto first_rec_group = log_block.get_first_rec_group();
  auto oldest_lsn = srv_buf_pool->get_oldest_modification();

  if (first_rec_group == 0) {
    /* We initialized a new log block which was not written
    full by the current mtr: the next mtr log record group
    will start within this block at the offset data_len */

    log_block.set_first_rec_group(log_block.get_data_len());
  }

  m_check_flush_or_checkpoint = true;

  if (checkpoint_age >= static_cast<ulint>(m_vfile.get_capacity())) {
    /* TODO: split btr_store_big_rec_extern_fields() into small
    steps so that we can release all latches in the middle, and
    call log_free_check() to ensure we never write over log written
    after the latest checkpoint. In principle, we should split all
    big_rec operations, but other operations are smaller. */

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

bool Log::calc_max_ages() noexcept {
  acquire();

  auto capacity = m_vfile.get_capacity();

  /* Add extra safety */
  capacity = capacity - capacity / 10;

  ulint margin{};

  /* For each OS thread we must reserve so much free space in the
  smallest log group that it can accommodate the log entries produced
  by single query steps: running out of free log space is a serious
  system error which requires rebooting the database. */
  auto free = LOG_CHECKPOINT_FREE_PER_THREAD * 10 + LOG_CHECKPOINT_EXTRA_FREE;

  if (free >= static_cast<ulint>(capacity / 2)) {
    log_fatal(
      "Redo log files are too small for thread_concurrency setting. The combined size of the redo log files"
      " should be bigger than 200 kB. To get the server to start up, set thread_concurrency variable"
      " to a lower value, for example, to 8. After an ERROR-FREE shutdown of the server you can"
      " adjust the size of the redo log files. Cannot continue operation. Forcing shutdown."
    );
  } else {
    margin = capacity - free;
  }

  margin = std::min<ulint>(margin, m_checkpoint.m_adm_interval);

  margin = margin - margin / 10; /* Add still some extra safety */

  m_checkpoint.m_max_modified_age_async = margin - margin / LOG_POOL_PREFLUSH_RATIO_ASYNC;

  m_checkpoint.m_max_modified_age_sync = margin - margin / LOG_POOL_PREFLUSH_RATIO_SYNC;

  m_checkpoint.m_max_age_async = margin - margin / LOG_POOL_CHECKPOINT_RATIO_ASYNC;

  m_checkpoint.m_max_age = margin;

  release();

  return true;
}

Log *Log::create(size_t log_buffer_size) noexcept {
  ut_a(log_sys == nullptr);

  log_sys = new (static_cast<log_t *>(mem_zalloc(sizeof(Log)))) Log(log_buffer_size);

  return log_sys;
}

bool Log::open(ulint n_files, off_t file_size) noexcept {
  // FIXME: Implement this.
  // m_vfile = Log_vfile(0, "redo", n_files, file_size);
  // calc_max_ages();
  return false;
}

void Log::complete_checkpoint() noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(m_checkpoint.m_n_pending_writes == 0);

  ++m_checkpoint.m_next_no;

  m_checkpoint.m_last_lsn = m_checkpoint.m_next_lsn;

  rw_lock_x_unlock_gen(&m_checkpoint.m_lock, LOG_CHECKPOINT);
}

void Log::io_complete_checkpoint() noexcept {
  acquire();

  ut_ad(m_checkpoint.m_n_pending_writes > 0);

  --m_checkpoint.m_n_pending_writes;

  if (m_checkpoint.m_n_pending_writes == 0) {
    complete_checkpoint();
  }

  release();
}

ulint Log::sys_check_flush_completion() noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (m_n_pending_writes == 0) {
    m_buffer.reset();
    return LOG_UNLOCK_FLUSH_LOCK;
  }

  return 0;
}

void Log::write_up_to(lsn_t lsn, bool async, bool flush_to_disk) noexcept {
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
      /* A write (+ possibly flush to disk) is running */

      if (flush_to_disk && m_current_flush_lsn >= lsn) {
        /* The write + flush will write enough: wait for it to complete  */
        release();
        do_waits(async);
        break;
      }

      if (!flush_to_disk && m_write_lsn >= lsn) {
        /* The write will write enough: wait for it to complete  */
        release();
        do_waits(async);
        break;
      }

      release();

      /* Wait for the write to complete and try to start a new write */
      os_event_wait(m_no_flush_event);
      continue;
    }

    if (!flush_to_disk && m_buffer.empty()) {
      /* Nothing to write and no flush to disk requested */
      release();
      return;
    }

    ++m_n_pending_writes;

    /* We assume here that we have only one log group! */
    ++m_vfile.m_n_pending_writes;

    os_event_reset(m_no_flush_event);
    os_event_reset(m_one_flushed_event);

    auto log_block = m_buffer.back();

    log_block.set_flush_bit(true);
    log_block.set_checkpoint_no(m_checkpoint.m_next_no);

    m_write_lsn = m_lsn;

    if (flush_to_disk) {
      m_current_flush_lsn = m_lsn;
    }

    m_vfile.write(m_buffer);

    release();

    if (srv_config.m_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
      /* O_DSYNC means the OS did not buffer the log file at all:
      so we have also flushed to disk what we have written */
      m_flushed_to_disk_lsn = m_write_lsn;
    } else if (flush_to_disk) {
      m_vfile.sync();
      m_flushed_to_disk_lsn = m_write_lsn;
    }

    {
      acquire();

      ut_a(m_n_pending_writes == 1);
      ut_a(m_vfile.m_n_pending_writes == 1);

      --m_vfile.m_n_pending_writes;
      --m_n_pending_writes;

      auto unlock = check_flush_completion();
      unlock |= sys_check_flush_completion();

      flush_do_unlocks(unlock);

      release();
    }

    return;
  }
}

void Log::flush_buffer_to_disk() noexcept {
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
      /* A flush is running: hope that it will provide enough free space */
    } else {
      lsn = m_lsn;
    }
  }

  release();

  if (lsn != 0) {
    write_up_to(lsn, true, false);
  }
}

bool Log::preflush_pool_modified_pages(lsn_t new_oldest, bool sync) noexcept {
  if (recv_recovery_on) {
    /* If the recovery is running, we must first apply all
    log records to their respective file pages to get the
    right modify lsn values to these pages: otherwise, there
    might be pages on disk which are not yet recovered to the
    current lsn, and even after calling this function, we could
    not know how up-to-date the disk version of the database is,
    and we could not make a new checkpoint on the basis of the
    info on the buffer pool only. */

    recv_apply_log_recs(srv_dblwr, false);
  }

  auto n_pages = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, ULINT_MAX, new_oldest);

  if (sync) {
    srv_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);
  }

  return n_pages != ULINT_UNDEFINED;
}

void Log::checkpoint() noexcept {
  ut_ad(mutex_own(&m_mutex));

  m_vfile.prepare_checkpoint(m_checkpoint.m_next_no, m_checkpoint.m_next_lsn, m_buf_size);

  if (!m_disable_writes) {
    if (m_checkpoint.m_n_pending_writes == 0) {
      rw_lock_x_lock_gen(&m_checkpoint.m_lock, LOG_CHECKPOINT);
    }

    ++m_n_log_ios;
    ++m_checkpoint.m_n_pending_writes;

    m_vfile.write_checkpoint(&m_checkpoint.m_phy_checkpoint, m_checkpoint.prepare(*this));
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

  /* Because log also contains headers and dummy log records,
  if the buffer pool contains no dirty buffers, oldest_lsn
  gets the value m_lsn from the previous function,
  and we must make sure that the log is flushed up to that
  lsn. If there are dirty buffers in the buffer pool, then our
  write-ahead-logging algorithm ensures that the log has been flushed
  up to oldest_lsn. */

  write_up_to(oldest_lsn, false, true);

  acquire();

  if (!write_always && m_checkpoint.m_last_lsn >= oldest_lsn) {

    release();

    return true;
  }

  ut_ad(m_flushed_to_disk_lsn >= oldest_lsn);

  if (m_checkpoint.m_n_pending_writes > 0) {
    /* A checkpoint write is running */

    release();

    if (sync) {
      /* Wait for the checkpoint write to complete */
      rw_lock_s_lock(&m_checkpoint.m_lock);
      rw_lock_s_unlock(&m_checkpoint.m_lock);
    }

    return false;
  }

  m_checkpoint.m_next_lsn = oldest_lsn;

  checkpoint();

  release();

  if (sync) {
    /* Wait for the checkpoint write to complete */
    rw_lock_s_lock(&m_checkpoint.m_lock);
    rw_lock_s_unlock(&m_checkpoint.m_lock);
  }

  return true;
}

void Log::make_checkpoint_at(lsn_t lsn, bool write_always) noexcept {
  /* Preflush pages synchronously */

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
    auto age = m_checkpoint.m_next_lsn - oldest_lsn;

    if (age > m_checkpoint.m_max_modified_age_sync) {
      sync = true;
      advance = 2 * (age - m_checkpoint.m_max_modified_age_sync);
    } else if (age > m_checkpoint.m_max_modified_age_async) {
      advance = age - m_checkpoint.m_max_modified_age_async;
    } else {
      advance = 0;
    }

    auto checkpoint_age = m_checkpoint.m_next_lsn - m_checkpoint.m_last_lsn;

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

/**
 * Checks if the given buffer containing checkpoint information is consistent.
 *
 * @param buf The buffer containing the checkpoint info.
 *
 * @return true if the checkpoint is consistent, false otherwise.
 */
static bool is_checkpoint_consistent(const byte *buf) noexcept {
  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
    return false;
  }

  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
    return false;
  } else {
    return true;
  }
}

bool Log::read_checkpoint(lsn_t &lsn, uint64_t &no) noexcept {
  if (!m_vfile.read_checkpoint(lsn, no)) {
    return false;
  }

  uint64_t max_no{};

  m_vfile.m_state = Log_vfile::State::CORRUPTED;

  static constexpr std::array<off_t, 2> checkpoint_offsets = {LOG_CHECKPOINT_1, LOG_CHECKPOINT_2};

  for (auto offset : checkpoint_offsets) {

    if (m_vfile.read_checkpoint(m_checkpoint, offset)) {
      if (m_checkpoint.is_consistent()) {
        m_vfile.m_state = Log_vfile::State::OK;
        m_vfile.m_lsn = m_checkpoint.m_phy_checkpoint.get_lsn();
        m_vfile.m_lsn_offset = m_checkpoint.m_phy_checkpoint.get_offset();
        no = m_checkpoint.m_phy_checkpoint.get_no();
        max_no = no;
        return true;
      }
    }

    if (is_checkpoint_consistent(buf)) {

      m_vfile.m_state = Log_vfile::State::OK;
      m_vfile.m_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
      m_vfile.m_lsn_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET);

      auto checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

      if (checkpoint_no >= max_no) {
        max_field = i;
        max_no = checkpoint_no;
      }
    }
  }

  return max_no != 0;
}

void Log::shutdown() noexcept {
  /* This can happen if we have to abort during startup. */
  if (log_sys == nullptr) {
    return;
  }

  m_vfile.close();

  mem_free(m_buf_ptr);
  m_buf_ptr = nullptr;

  os_event_free(m_no_flush_event);
  os_event_free(m_one_flushed_event);
}

void Log::destroy(Log *&log_sys) noexcept {
  if (log_sys != nullptr) {
    call_destructor(log_sys);
    mem_free(log_sys);
    log_sys = nullptr;
  }
}

void Log::free_check() noexcept {
  if (m_check_flush_or_checkpoint) {
    check_margins();
  }
}
