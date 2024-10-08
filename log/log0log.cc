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
#include "dict0boot.h"
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

/* Current free limit of space 0; protected by the log sys mutex; 0 means
uninitialized */
ulint log_fsp_current_free_limit = 0;

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

Log::Log() noexcept {
  mutex_create(&m_mutex, IF_DEBUG("log_sys_mutex",) IF_SYNC_DEBUG(SYNC_LOG,) Source_location{});

  acquire();

  /* Start the lsn from one log block from zero: this way every
  log record has a start lsn != zero, a fact which we will use */

  m_lsn = LOG_START_LSN;

  ut_a(LOG_BUFFER_SIZE >= 16 * IB_FILE_BLOCK_SIZE);
  ut_a(LOG_BUFFER_SIZE >= 4 * UNIV_PAGE_SIZE);

  m_buf_ptr = static_cast<byte *>(mem_alloc(LOG_BUFFER_SIZE + IB_FILE_BLOCK_SIZE));

  m_buf = static_cast<byte *>(ut_align(m_buf_ptr, IB_FILE_BLOCK_SIZE));

  m_buf_size = LOG_BUFFER_SIZE;

  memset(m_buf, '\0', LOG_BUFFER_SIZE);

  m_max_buf_free = m_buf_size / LOG_BUF_FLUSH_RATIO - LOG_BUF_FLUSH_MARGIN;
  m_check_flush_or_checkpoint = true;
  UT_LIST_INIT(m_log_groups);

  m_n_log_ios = 0;

  m_n_log_ios_old = m_n_log_ios;
  m_last_printout_time = time(nullptr);
  /*----------------------------*/

  m_buf_next_to_write = 0;

  m_write_lsn = 0;
  m_current_flush_lsn = 0;
  m_flushed_to_disk_lsn = 0;

  m_written_to_some_lsn = m_lsn;
  m_written_to_all_lsn = m_lsn;

  m_n_pending_writes = 0;

  m_no_flush_event = os_event_create(nullptr);

  os_event_set(m_no_flush_event);

  m_one_flushed_event = os_event_create(nullptr);

  os_event_set(m_one_flushed_event);

  /*----------------------------*/
  m_adm_checkpoint_interval = ULINT_MAX;

  m_next_checkpoint_no = 0;
  m_last_checkpoint_lsn = m_lsn;
  m_n_pending_checkpoint_writes = 0;

  rw_lock_create(&m_checkpoint_lock, SYNC_NO_ORDER_CHECK);

  m_checkpoint_buf_ptr = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));

  m_checkpoint_buf = static_cast<byte *>(ut_align(m_checkpoint_buf_ptr, IB_FILE_BLOCK_SIZE));

  memset(m_checkpoint_buf, '\0', IB_FILE_BLOCK_SIZE);
  /*----------------------------*/

  block_init(m_buf, m_lsn);
  block_set_first_rec_group(m_buf, LOG_BLOCK_HDR_SIZE);

  m_buf_free = LOG_BLOCK_HDR_SIZE;
  m_lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;

  release();
}

Log::~Log() noexcept {
  // FIXME:
}

void Log::var_init() noexcept {
  ut_a(log_sys == nullptr);
  log_last_warning_time = 0;
  log_fsp_current_free_limit = 0;
  log_has_printed_chkp_warning = false;
}

void Log::fsp_current_free_limit_set_and_checkpoint(ulint limit) noexcept {
  acquire();

  log_fsp_current_free_limit = limit;

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

  ut_a(len < m_buf_size / 2);

  for (;;) {
    acquire();

    /* Calculate an upper limit for the space the string may take in the
    log buffer */

    auto len_upper_limit = LOG_BUF_WRITE_MARGIN + (5 * len) / 4;

    if (m_buf_free + len_upper_limit > m_buf_size) {
      release();

      /* Not enough free space, do a synchronous flush of the log buffer */

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
    /* Calculate a part length */
    auto data_len = (m_buf_free % IB_FILE_BLOCK_SIZE) + str_len;

    ulint len;

    if (data_len <= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      /* The string fits within the current log block */
      len = str_len;
    } else {
      data_len = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
      len = IB_FILE_BLOCK_SIZE - (m_buf_free % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
    }

    memcpy(m_buf + m_buf_free, str, len);

    str_len -= len;
    str += len;

    auto log_block = static_cast<byte *>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE));

    block_set_data_len(log_block, data_len);

    if (data_len == IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      /* This block is full. */
      block_set_data_len(log_block, IB_FILE_BLOCK_SIZE);
      block_set_checkpoint_no(log_block, m_next_checkpoint_no);
      len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;

      m_lsn += len;

      // Initialize the next block header
      block_init(log_block + IB_FILE_BLOCK_SIZE, m_lsn);
    } else {
      m_lsn += len;
    }

    m_buf_free += len;

    ut_ad(m_buf_free <= m_buf_size);
  }

  ++srv_log_write_requests;
}

lsn_t Log::close(ib_recovery_t recovery) noexcept {
  auto log_block = static_cast<byte *>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE));
  auto first_rec_group = block_get_first_rec_group(log_block);
  auto lsn = m_lsn;
  auto checkpoint_age = lsn - m_last_checkpoint_lsn;
  auto oldest_lsn = srv_buf_pool->get_oldest_modification();

  ut_ad(mutex_own(&m_mutex));

  if (first_rec_group == 0) {
    /* We initialized a new log block which was not written
    full by the current mtr: the next mtr log record group
    will start within this block at the offset data_len */

    block_set_first_rec_group(log_block, block_get_data_len(log_block));
  }

  if (m_buf_free > m_max_buf_free) {
    m_check_flush_or_checkpoint = true;
  }

  if (checkpoint_age >= m_log_group_capacity) {
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
        m_log_group_capacity
      ));
    }
  }

  if (checkpoint_age > m_max_modified_age_async ||
      (oldest_lsn > 0 && lsn - oldest_lsn > m_max_modified_age_async) ||
      checkpoint_age > m_max_checkpoint_age_async) {

    m_check_flush_or_checkpoint = true;
  }

  return lsn;
}

ulint Log::group_get_capacity(const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return (group->file_size - LOG_FILE_HDR_SIZE) * group->n_files;
}

ulint Log::group_calc_size_offset(ulint offset, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return offset - LOG_FILE_HDR_SIZE * (1 + offset / group->file_size);
}

uint64_t Log::group_calc_real_offset(ulint offset, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE));
}

uint64_t Log::group_calc_lsn_offset(lsn_t lsn, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  /* If total log file size is > 2 GB we can easily get overflows with
  32-bit integers. Use 64-bit integers instead. */

  auto gr_lsn = group->lsn;
  auto gr_lsn_size_offset = (int64_t)group_calc_size_offset(group->lsn_offset, group);
  auto group_size = (int64_t)group_get_capacity(group);

  int64_t difference;

  if (lsn >= gr_lsn) {
    difference = (int64_t)(lsn - gr_lsn);
  } else {
    difference = (int64_t)(gr_lsn - lsn);
    difference = difference % group_size;
    difference = group_size - difference;
  }

  auto offset = (gr_lsn_size_offset + difference) % group_size;

  ut_a(offset < (((int64_t)1) << 32));  // offset must be < 4 GB

  return group_calc_real_offset((ulint)offset, group);
}

IF_DEBUG(bool log_debug_writes = false;)

ulint Log::calc_where_lsn_is(off_t *log_file_offset, lsn_t first_header_lsn, lsn_t lsn, ulint n_log_files, off_t log_file_size) noexcept {
  const int64_t capacity = log_file_size - LOG_FILE_HDR_SIZE;

  if (lsn < first_header_lsn) {
    auto add_this_many = 1 + (first_header_lsn - lsn) / (capacity * (int64_t)n_log_files);
    lsn += add_this_many * capacity * (int64_t)n_log_files;
  }

  ut_a(lsn >= first_header_lsn);

  auto file_no = ((ulint)((lsn - first_header_lsn) / capacity)) % n_log_files;
  *log_file_offset = (lsn - first_header_lsn) % capacity;

  *log_file_offset = *log_file_offset + LOG_FILE_HDR_SIZE;

  return file_no;
}

void Log::group_set_fields(log_group_t *group, lsn_t lsn) noexcept {
  group->lsn_offset = group_calc_lsn_offset(lsn, group);
  group->lsn = lsn;
}

bool Log::calc_max_ages() noexcept {
  acquire();

  auto smallest_capacity = ULINT_MAX;

  ut_a(UT_LIST_GET_FIRST(m_log_groups) != nullptr);


  for (auto group : m_log_groups) {
    auto capacity = group_get_capacity(group);

    smallest_capacity = std::min(smallest_capacity, capacity);
  }

  /* Add extra safety */
  smallest_capacity = smallest_capacity - smallest_capacity / 10;

  /* For each OS thread we must reserve so much free space in the
  smallest log group that it can accommodate the log entries produced
  by single query steps: running out of free log space is a serious
  system error which requires rebooting the database. */

  auto free = LOG_CHECKPOINT_FREE_PER_THREAD * 10 + LOG_CHECKPOINT_EXTRA_FREE;

  ulint margin{};
  bool success = true;

  if (free >= smallest_capacity / 2) {
    success = false;
  } else {
    margin = smallest_capacity - free;
  }

  margin = std::min(margin, m_adm_checkpoint_interval);

  margin = margin - margin / 10; /* Add still some extra safety */

  m_log_group_capacity = smallest_capacity;

  m_max_modified_age_async = margin - margin / LOG_POOL_PREFLUSH_RATIO_ASYNC;

  m_max_modified_age_sync = margin - margin / LOG_POOL_PREFLUSH_RATIO_SYNC;

  m_max_checkpoint_age_async = margin - margin / LOG_POOL_CHECKPOINT_RATIO_ASYNC;

  m_max_checkpoint_age = margin;

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

Log* Log::create() noexcept {
  ut_a(log_sys == nullptr);

  log_sys = static_cast<log_t *>(mem_alloc(sizeof(Log)));
  new (log_sys) Log();

  return log_sys;
}

void Log::group_init(ulint id, ulint n_files, off_t file_size, space_id_t space_id) noexcept {
  auto group = static_cast<log_group_t *>(mem_alloc(sizeof(log_group_t)));

  new (group) log_group_t();

  group->id = id;
  group->n_files = n_files;
  group->file_size = file_size;
  group->space_id = space_id;
  group->state = LOG_GROUP_OK;
  group->lsn = LOG_START_LSN;
  group->lsn_offset = LOG_FILE_HDR_SIZE;
  group->n_pending_writes = 0;

  group->file_header_bufs_ptr = static_cast<byte **>(mem_alloc(sizeof(byte *) * n_files));

  group->file_header_bufs = static_cast<byte **>(mem_alloc(sizeof(byte *) * n_files));

  for (ulint i = 0; i < n_files; i++) {
    group->file_header_bufs_ptr[i] = static_cast<byte *>(mem_alloc(LOG_FILE_HDR_SIZE + IB_FILE_BLOCK_SIZE));

    group->file_header_bufs[i] = static_cast<byte *>(ut_align(group->file_header_bufs_ptr[i], IB_FILE_BLOCK_SIZE));

    memset(*(group->file_header_bufs + i), '\0', LOG_FILE_HDR_SIZE);
  }

  group->checkpoint_buf_ptr = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));

  group->checkpoint_buf = static_cast<byte *>(ut_align(group->checkpoint_buf_ptr, IB_FILE_BLOCK_SIZE));

  memset(group->checkpoint_buf, '\0', IB_FILE_BLOCK_SIZE);

  UT_LIST_ADD_LAST(m_log_groups, group);

  ut_a(calc_max_ages());
}

void Log::flush_do_unlocks(ulint code) noexcept {
  ut_ad(mutex_own(&m_mutex));

  /* NOTE that we must own the log mutex when doing the setting of the
     events: this is because transactions will wait for these events to
     be set, and at that moment the log flush they were waiting for must
     have ended. If the log mutex were not reserved here, the i/o-thread
     calling this function might be preempted for a while, and when it
     resumed execution, it might be that a new flush had been started, and
     this function would erroneously signal the NEW flush as completed.
     Thus, the changes in the state of these events are performed
     atomically in conjunction with the changes in the state of
     m_n_pending_writes etc. */

  if (code & LOG_UNLOCK_NONE_FLUSHED_LOCK) {
    os_event_set(m_one_flushed_event);
  }

  if (code & LOG_UNLOCK_FLUSH_LOCK) {
    os_event_set(m_no_flush_event);
  }
}

ulint Log::group_check_flush_completion(log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (!m_one_flushed && group->n_pending_writes == 0) {
    m_written_to_some_lsn = m_write_lsn;
    m_one_flushed = true;

    return LOG_UNLOCK_NONE_FLUSHED_LOCK;
  }

  return 0;
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

ulint Log::sys_check_flush_completion() noexcept {
  ulint move_start;
  ulint move_end;

  ut_ad(mutex_own(&m_mutex));

  if (m_n_pending_writes == 0) {

    m_written_to_all_lsn = m_write_lsn;
    m_buf_next_to_write = m_write_end_offset;

    if (m_write_end_offset > m_max_buf_free / 2) {
      /* Move the log buffer content to the start of the buffer */

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

void Log::io_complete(log_group_t *group) noexcept {
  if (uintptr_t(group) & 0x1UL) {
    /* It was a checkpoint write */
    group = reinterpret_cast<log_group_t *>(uintptr_t(group) - 1);

    if (srv_config.m_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_config.m_unix_file_flush_method != SRV_UNIX_NOSYNC) {

      srv_fil->flush(group->space_id);
    }

    io_complete_checkpoint();

    return;
  }

  /* We currently use synchronous writing of the logs and cannot end up here! */
  ut_error;

  if (srv_config.m_unix_file_flush_method != SRV_UNIX_O_DSYNC &&
      srv_config.m_unix_file_flush_method != SRV_UNIX_NOSYNC &&
      srv_config.m_flush_log_at_trx_commit != 2) {

    srv_fil->flush(group->space_id);
  }

  acquire();

  ut_a(group->n_pending_writes > 0);
  ut_a(m_n_pending_writes > 0);

  --group->n_pending_writes;
  --m_n_pending_writes;

  auto unlock = group_check_flush_completion(group);
  unlock |= sys_check_flush_completion();

  flush_do_unlocks(unlock);

  release();
}

void Log::group_file_header_flush(log_group_t *group, ulint nth_file, lsn_t start_lsn) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_a(nth_file < group->n_files);

  auto buf = group->file_header_bufs[nth_file];

  mach_write_to_4(buf + LOG_GROUP_ID, group->id);
  mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);

  /* Wipe over possible label of ibbackup --restore */
  memcpy(buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);

  auto dest_offset = nth_file * group->file_size;

  if (log_do_write) {
    ++m_n_log_ios;

    ++srv_os_log_pending_writes;

    srv_fil->io(
      IO_request::Sync_log_write,
      false,
      group->space_id,
      dest_offset / UNIV_PAGE_SIZE,
      dest_offset % UNIV_PAGE_SIZE,
      IB_FILE_BLOCK_SIZE,
      buf,
      nullptr
    );

    --srv_os_log_pending_writes;
  }
}

void Log::block_store_checksum(byte *block) noexcept {
  block_set_checksum(block, block_calc_checksum(block));
}

void Log::group_write_buf(log_group_t *group, byte *buf, ulint len, lsn_t start_lsn, ulint new_data_offset) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_a(len % IB_FILE_BLOCK_SIZE == 0);
  ut_a(((ulint)start_lsn) % IB_FILE_BLOCK_SIZE == 0);

  auto write_header = new_data_offset == 0;

  while (len > 0) {
    auto next_offset = group_calc_lsn_offset(start_lsn, group);

    if ((next_offset % group->file_size == LOG_FILE_HDR_SIZE) && write_header) {
      /* We start to write a new log file instance in the group */
      group_file_header_flush(group, next_offset / group->file_size, start_lsn);
      srv_os_log_written += IB_FILE_BLOCK_SIZE;
      ++srv_log_writes;
    }

    ulint write_len;

    if ((next_offset % group->file_size) + len > group->file_size) {
      write_len = group->file_size - (next_offset % group->file_size);
    } else {
      write_len = len;
    }

    // Calculate the checksums for each log block and write them
    // to the trailer fields of the log blocks
    for (ulint i = 0; i < write_len / IB_FILE_BLOCK_SIZE; i++) {
      block_store_checksum(buf + i * IB_FILE_BLOCK_SIZE);
    }

    if (log_do_write) {
      ++m_n_log_ios;
      ++srv_os_log_pending_writes;

      srv_fil->io(
        IO_request::Sync_log_write,
        false,
        group->space_id,
        next_offset / UNIV_PAGE_SIZE,
        next_offset % UNIV_PAGE_SIZE,
        write_len,
        buf,
        nullptr
      );

      --srv_os_log_pending_writes;

      srv_os_log_written += write_len;

      ++srv_log_writes;
    }

    if (write_len < len) {
      start_lsn += write_len;
      len -= write_len;
      buf += write_len;
      write_header = true;
    } else {
      break;
    }
  }
}

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
#endif /* UNIV_DEBUG */
    }
  };

  for (;;) {
    ut_ad(++loop_count < 5);

    acquire();

    if (flush_to_disk && m_flushed_to_disk_lsn >= lsn) {
      release();
      return;
    }

    if (!flush_to_disk &&
        (m_written_to_all_lsn >= lsn ||
         (m_written_to_some_lsn >= lsn && wait != LOG_WAIT_ALL_GROUPS))) {
      release();
      return;
    }

    if (m_n_pending_writes > 0) {
      /* A write (+ possibly flush to disk) is running */

      if (flush_to_disk && m_current_flush_lsn >= lsn) {
        /* The write + flush will write enough: wait for it to complete  */
        release();
        do_waits(wait);
        break;
      }

      if (!flush_to_disk && m_write_lsn >= lsn) {
        /* The write will write enough: wait for it to complete  */
        release();
        do_waits(wait);
        break;
      }

      release();

      /* Wait for the write to complete and try to start a new write */
      os_event_wait(m_no_flush_event);
      continue;
    }

    if (!flush_to_disk && m_buf_free == m_buf_next_to_write) {
      /* Nothing to write and no flush to disk requested */
      release();
      return;
    }

    ++m_n_pending_writes;

    group = UT_LIST_GET_FIRST(m_log_groups);

    /* We assume here that we have only one log group! */
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

    /* Copy the last, incompletely written, log block a log block length
    up, so that when the flush operation writes from the log buffer, the
    segment to write will not be changed by writers to the log */
    memcpy(m_buf + area_end, m_buf + area_end - IB_FILE_BLOCK_SIZE, IB_FILE_BLOCK_SIZE);

    m_buf_free += IB_FILE_BLOCK_SIZE;
    m_write_end_offset = m_buf_free;

    /* Do the write to the log files */
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
      /* O_DSYNC means the OS did not buffer the log file at all:
      so we have also flushed to disk what we have written */
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

void Log::buffer_flush_to_disk() noexcept {
  acquire();

  auto lsn = m_lsn;

  release();

  write_up_to(lsn, LOG_WAIT_ALL_GROUPS, true);
}

void Log::buffer_sync_in_background(bool flush) noexcept {
  mutex_enter(&m_mutex);

  auto lsn = m_lsn;

  mutex_exit(&m_mutex);

  write_up_to(lsn, LOG_NO_WAIT, flush);
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
    write_up_to(lsn, LOG_NO_WAIT, false);
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

  /* Write the checkpoint number */
  mach_write_to_8(buf + LOG_CHECKPOINT_NO, m_next_checkpoint_no);

  /* Write the checkpoint LSN */
  mach_write_to_8(buf + LOG_CHECKPOINT_LSN, m_next_checkpoint_lsn);

  /* Write the checkpoint offset */
  mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, group_calc_lsn_offset(m_next_checkpoint_lsn, group));

  /* Write the log buffer size */
  mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, m_buf_size);

  /* Write the unused LSN */
  mach_write_to_8(buf + LOG_CHECKPOINT_UNUSED_LSN, IB_UINT64_T_MAX);

  /* Initialize group info to 0 */
  for (ulint i{}; i < LOG_MAX_N_GROUPS; ++i) {
    checkpoint_set_nth_group_info(buf, i, 0, 0);
  }

  /* Write group info for each log group */
  auto group2 = UT_LIST_GET_FIRST(m_log_groups);

  while (group2 != nullptr) {
    checkpoint_set_nth_group_info(buf, group2->id, 0, 0);

    group2 = UT_LIST_GET_NEXT(log_groups, group2);
  }

  /* Calculate and write the first checksum */
  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

  /* Calculate and write the second checksum */
  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

  /* Write the free limit of the tablespace */
  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_FREE_LIMIT, log_fsp_current_free_limit);

  /* Write the magic number for the tablespace */
  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_MAGIC_N, LOG_CHECKPOINT_FSP_MAGIC_N_VAL);

  ulint write_offset;

  /* Alternate the physical place of the checkpoint info in the first log file */
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

    /* Send the log file write request */
    srv_fil->io(
      IO_request::Async_log_write,
      false,
      group->space_id,
      write_offset / UNIV_PAGE_SIZE,
      write_offset % UNIV_PAGE_SIZE,
      IB_FILE_BLOCK_SIZE,
      buf,
      ((byte *)group + 1)
    );

    ut_ad(((ulint)group & 0x1UL) == 0);
  }
}

void Log::group_read_checkpoint_info(log_group_t *group, ulint field) noexcept {
  ut_ad(mutex_own(&m_mutex));

  ++m_n_log_ios;

  srv_fil->io(
    IO_request::Sync_log_read,
    false,
    group->space_id,
    field / UNIV_PAGE_SIZE,
    field % UNIV_PAGE_SIZE,
    IB_FILE_BLOCK_SIZE,
    m_checkpoint_buf,
    nullptr
  );
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

  /* Because log also contains headers and dummy log records,
  if the buffer pool contains no dirty buffers, oldest_lsn
  gets the value m_lsn from the previous function,
  and we must make sure that the log is flushed up to that
  lsn. If there are dirty buffers in the buffer pool, then our
  write-ahead-logging algorithm ensures that the log has been flushed
  up to oldest_lsn. */

  write_up_to(oldest_lsn, LOG_WAIT_ALL_GROUPS, true);

  acquire();

  if (!write_always && m_last_checkpoint_lsn >= oldest_lsn) {

    release();

    return true;
  }

  ut_ad(m_flushed_to_disk_lsn >= oldest_lsn);

  if (m_n_pending_checkpoint_writes > 0) {
    /* A checkpoint write is running */

    release();

    if (sync) {
      /* Wait for the checkpoint write to complete */
      rw_lock_s_lock(&m_checkpoint_lock);
      rw_lock_s_unlock(&m_checkpoint_lock);
    }

    return false;
  }

  m_next_checkpoint_lsn = oldest_lsn;

  groups_write_checkpoint_info();

  release();

  if (sync) {
    /* Wait for the checkpoint write to complete */
    rw_lock_s_lock(&m_checkpoint_lock);
    rw_lock_s_unlock(&m_checkpoint_lock);
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

void Log::group_read_log_seg(ulint type, byte *buf, log_group_t *group, lsn_t start_lsn, lsn_t end_lsn) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto source_offset = group_calc_lsn_offset(start_lsn, group);
  auto len = (ulint)(end_lsn - start_lsn);

  ut_ad(len != 0);

  while (len > 0) {
    if ((source_offset % group->file_size) + len > group->file_size) {
      len = group->file_size - (source_offset % group->file_size);
    }

    ++m_n_log_ios;

    srv_fil->io(
      type == LOG_RECOVER ? IO_request::Sync_log_read : IO_request::Async_log_read,
      false,
      group->space_id,
      source_offset / UNIV_PAGE_SIZE,
      source_offset % UNIV_PAGE_SIZE,
      len,
      buf,
      nullptr
    );

    start_lsn += len;
    buf += len;
    len = (ulint)(end_lsn - start_lsn);
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
    m_last_checkpoint_lsn
   ));

  auto current_time = time(nullptr);
  auto time_elapsed = 0.001 + difftime(current_time, m_last_printout_time);

  log_info(std::format(
    "{} pending log writes, {} pending chkp writes"
    " {} log i/o's done, {:2} log i/o's/second",
    m_n_pending_writes,
    m_n_pending_checkpoint_writes,
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

void Log::group_close(log_group_t *group) noexcept {
  for (ulint i = 0; i < group->n_files; ++i) {
    mem_free(group->file_header_bufs_ptr[i]);
  }

  mem_free(group->file_header_bufs);
  mem_free(group->file_header_bufs_ptr);

  mem_free(group->checkpoint_buf_ptr);

  mem_free(group);
}

void Log::shutdown() noexcept {
  /* This can happen if we have to abort during startup. */
  if (log_sys == nullptr || UT_LIST_GET_LEN(m_log_groups) == 0) {
    return;
  }

  auto group = UT_LIST_GET_FIRST(m_log_groups);

  while (UT_LIST_GET_LEN(m_log_groups) > 0) {
    auto prev_group = group;

    group = UT_LIST_GET_NEXT(log_groups, group);
    UT_LIST_REMOVE(m_log_groups, prev_group);

    group_close(prev_group);
  }

  mem_free(m_buf_ptr);
  m_buf_ptr = nullptr;
  mem_free(m_checkpoint_buf_ptr);
  m_checkpoint_buf_ptr = nullptr;

  os_event_free(m_no_flush_event);
  os_event_free(m_one_flushed_event);

  rw_lock_free(&m_checkpoint_lock);
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
