/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Google Inc.

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
#include "srv0start.h"
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
log_t *log_sys = nullptr;

#ifdef UNIV_DEBUG
bool log_do_write = true;
#endif /* UNIV_DEBUG */

/* These control how often we print warnings if the last checkpoint is too old */
bool log_has_printed_chkp_warning = false;
time_t log_last_warning_time;

/* A margin for free space in the log buffer before a log entry is catenated */
constexpr ulint LOG_BUF_WRITE_MARGIN = 4 * IB_FILE_BLOCK_SIZE;

/* Margins for free space in the log buffer after a log entry is catenated */
constexpr ulint LOG_BUF_FLUSH_RATIO = 2;
constexpr ulint LOG_BUF_FLUSH_MARGIN = LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE;

/* Margin for the free space in the smallest log group, before a new query
step which modifies the database, is started */

constexpr ulint LOG_CHECKPOINT_FREE_PER_THREAD = 4 * UNIV_PAGE_SIZE;
constexpr ulint LOG_CHECKPOINT_EXTRA_FREE = 8 * UNIV_PAGE_SIZE;

/* This parameter controls asynchronous making of a new checkpoint; the value
should be bigger than LOG_POOL_PREFLUSH_RATIO_SYNC */

constexpr ulint LOG_POOL_CHECKPOINT_RATIO_ASYNC = 32;

/* This parameter controls synchronous preflushing of modified buffer pages */
constexpr ulint LOG_POOL_PREFLUSH_RATIO_SYNC = 32;

/* The same ratio for asynchronous preflushing; this value should be less than
the previous */
constexpr ulint LOG_POOL_PREFLUSH_RATIO_ASYNC = 6;

/* Extra margin, in addition to one log file, used in archiving */

/* Codes used in unlocking flush latches */
constexpr ulint LOG_UNLOCK_NONE_FLUSHED_LOCK = 1;
constexpr ulint LOG_UNLOCK_FLUSH_LOCK = 2;

void log_var_init() {
  log_sys = nullptr;
  log_last_warning_time = 0;
  log_fsp_current_free_limit = 0;
  log_has_printed_chkp_warning = false;
}

void log_fsp_current_free_limit_set_and_checkpoint(ulint limit) {
  log_acquire();

  log_fsp_current_free_limit = limit;

  log_release();

  /* Try to make a synchronous checkpoint */

  while (!log_checkpoint(true, true)) {
    /* No op */
  }
}

/**
 * Returns the oldest modified block LSN in the pool, or log_sys->lsn if none exists.
 * 
 * @return LSN of oldest modification
 */
static lsn_t log_buf_pool_get_oldest_modification() {
  ut_ad(mutex_own(&(log_sys->mutex)));

  auto lsn = buf_pool->get_oldest_modification();

  if (lsn == 0) {
    lsn = log_sys->lsn;
  }

  return lsn;
}

uint64_t log_reserve_and_open(ulint len) {
  log_t *log = log_sys;
#ifdef UNIV_DEBUG
  ulint count = 0;
#endif /* UNIV_DEBUG */

  ut_a(len < log->buf_size / 2);

  for (;;) {
    log_acquire();

    /* Calculate an upper limit for the space the string may take in the
    log buffer */

    auto len_upper_limit = LOG_BUF_WRITE_MARGIN + (5 * len) / 4;

    if (log->buf_free + len_upper_limit > log->buf_size) {
      log_release();

      /* Not enough free space, do a synchronous flush of the log buffer */

      log_buffer_flush_to_disk();

      srv_log_waits++;

      ut_ad(++count < 50);
    } else {
      break;
    }
  }

#ifdef UNIV_LOG_DEBUG
  log->old_buf_free = log->buf_free;
  log->old_lsn = log->lsn;
#endif /* UNIV_LOG_DEBUG */

  return log->lsn;
}

void log_write_low(byte *str, ulint str_len) {
  auto log = log_sys;

  ut_ad(mutex_own(&(log->mutex)));

  while (str_len > 0) {
    // Calculate a part length
    auto data_len = (log->buf_free % IB_FILE_BLOCK_SIZE) + str_len;

    ulint len;

    if (data_len <= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      // The string fits within the current log block
      len = str_len;
    } else {
      data_len = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
      len = IB_FILE_BLOCK_SIZE - (log->buf_free % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
    }

    memcpy(log->buf + log->buf_free, str, len);

    str_len -= len;
    str += len;

    auto log_block = static_cast<byte *>(ut_align_down(log->buf + log->buf_free, IB_FILE_BLOCK_SIZE));

    log_block_set_data_len(log_block, data_len);

    if (data_len == IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      // This block became full
      log_block_set_data_len(log_block, IB_FILE_BLOCK_SIZE);
      log_block_set_checkpoint_no(log_block, log_sys->next_checkpoint_no);
      len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;

      log->lsn += len;

      // Initialize the next block header
      log_block_init(log_block + IB_FILE_BLOCK_SIZE, log->lsn);
    } else {
      log->lsn += len;
    }

    log->buf_free += len;

    ut_ad(log->buf_free <= log->buf_size);
  }

  srv_log_write_requests++;
}

lsn_t log_close(ib_recovery_t recovery) {
  auto log = log_sys;
  auto log_block = static_cast<byte *>(ut_align_down(log->buf + log->buf_free, IB_FILE_BLOCK_SIZE));
  auto first_rec_group = log_block_get_first_rec_group(log_block);
  auto lsn = log->lsn;
  auto checkpoint_age = lsn - log->last_checkpoint_lsn;
  auto oldest_lsn = buf_pool->get_oldest_modification();

  ut_ad(mutex_own(&(log->mutex)));

  if (first_rec_group == 0) {
    /* We initialized a new log block which was not written
    full by the current mtr: the next mtr log record group
    will start within this block at the offset data_len */

    log_block_set_first_rec_group(log_block, log_block_get_data_len(log_block));
  }

  if (log->buf_free > log->max_buf_free) {
    log->check_flush_or_checkpoint = true;
  }

  if (checkpoint_age >= log->log_group_capacity) {
    /* TODO: split btr_store_big_rec_extern_fields() into small
    steps so that we can release all latches in the middle, and
    call log_free_check() to ensure we never write over log written
    after the latest checkpoint. In principle, we should split all
    big_rec operations, but other operations are smaller. */

    if (!log_has_printed_chkp_warning || difftime(time(nullptr), log_last_warning_time) > 15) {

      log_has_printed_chkp_warning = true;
      log_last_warning_time = time(nullptr);

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  ERROR: the age of the last"
        " checkpoint is %lu,\n"
        "which exceeds the log group"
        " capacity %lu.\n"
        "If you are using big"
        " BLOB or TEXT rows, you must set the\n"
        "combined size of log files"
        " at least 10 times bigger than the\n"
        "largest such row.\n",
        (ulong)checkpoint_age,
        (ulong)log->log_group_capacity
      );
    }
  }

  if (checkpoint_age > log->max_modified_age_async || (oldest_lsn && lsn - oldest_lsn > log->max_modified_age_async) ||
      checkpoint_age > log->max_checkpoint_age_async) {
    log->check_flush_or_checkpoint = true;
  }

#ifdef UNIV_LOG_DEBUG
  log_check_log_recs(recovery, log->buf + log->old_buf_free, log->buf_free - log->old_buf_free, log->old_lsn);
#endif

  return lsn;
}

ulint log_group_get_capacity(const log_group_t *group) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  return (group->file_size - LOG_FILE_HDR_SIZE) * group->n_files;
}

/**
 * Calculates the offset within a log group, when the log file headers are not included.
 *
 * @param offset Real offset within the log group
 * @param group Log group
 * @return Size offset (<= offset)
 */
inline ulint log_group_calc_size_offset(ulint offset, const log_group_t *group) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  return offset - LOG_FILE_HDR_SIZE * (1 + offset / group->file_size);
}

/**
 * Calculates the offset within a log group, when the log file headers are included.
 * 
 * @param offset Size offset within the log group
 * @param group Log group
 * @return Real offset (>= offset)
 */
inline ulint log_group_calc_real_offset(ulint offset, const log_group_t *group) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  return (offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE)));
}

/**
 * Calculates the offset of an lsn within a log group.
 *
 * @param lsn   LSN, must be within 4 GB of group->lsn
 * @param group Log group
 * @return      Offset within the log group
 */
static ulint log_group_calc_lsn_offset(uint64_t lsn, const log_group_t *group) {
  lsn_t gr_lsn;
  int64_t gr_lsn_size_offset;
  int64_t difference;
  int64_t group_size;
  int64_t offset;

  ut_ad(mutex_own(&(log_sys->mutex)));

  // If total log file size is > 2 GB we can easily get overflows with 32-bit integers. Use 64-bit integers instead.

  gr_lsn = group->lsn;

  gr_lsn_size_offset = (int64_t)log_group_calc_size_offset(group->lsn_offset, group);

  group_size = (int64_t)log_group_get_capacity(group);

  if (lsn >= gr_lsn) {
    difference = (int64_t)(lsn - gr_lsn);
  } else {
    difference = (int64_t)(gr_lsn - lsn);
    difference = difference % group_size;
    difference = group_size - difference;
  }

  offset = (gr_lsn_size_offset + difference) % group_size;

  ut_a(offset < (((int64_t)1) << 32));  // offset must be < 4 GB

  return log_group_calc_real_offset((ulint)offset, group);
}

#ifdef UNIV_DEBUG
bool log_debug_writes = false;
#endif /* UNIV_DEBUG */

ulint log_calc_where_lsn_is(off_t *log_file_offset, lsn_t first_header_lsn, lsn_t lsn, ulint n_log_files, off_t log_file_size) {
  ulint file_no;
  int64_t add_this_many;
  int64_t capacity = log_file_size - LOG_FILE_HDR_SIZE;

  if (lsn < first_header_lsn) {
    add_this_many = 1 + (first_header_lsn - lsn) / (capacity * (int64_t)n_log_files);
    lsn += add_this_many * capacity * (int64_t)n_log_files;
  }

  ut_a(lsn >= first_header_lsn);

  file_no = ((ulint)((lsn - first_header_lsn) / capacity)) % n_log_files;
  *log_file_offset = (lsn - first_header_lsn) % capacity;

  *log_file_offset = *log_file_offset + LOG_FILE_HDR_SIZE;

  return file_no;
}

void log_group_set_fields(log_group_t *group, lsn_t lsn) {
  group->lsn_offset = log_group_calc_lsn_offset(lsn, group);
  group->lsn = lsn;
}

/** Calculates the recommended highest values for lsn - last_checkpoint_lsn,
lsn - buf_get_oldest_modification(), and lsn - max_archive_lsn_age.
@return error value false if the smallest log group is too small to
accommodate the number of OS threads in the database server */
static bool log_calc_max_ages() {
  log_acquire();

  auto group = UT_LIST_GET_FIRST(log_sys->log_groups);
  ut_a(group != nullptr);

  auto smallest_capacity = ULINT_MAX;

  while (group != nullptr) {
    auto capacity = log_group_get_capacity(group);

    smallest_capacity = std::min(smallest_capacity, capacity);
    group = UT_LIST_GET_NEXT(log_groups, group);
  }

  /* Add extra safety */
  smallest_capacity = smallest_capacity - smallest_capacity / 10;

  /* For each OS thread we must reserve so much free space in the
  smallest log group that it can accommodate the log entries produced
  by single query steps: running out of free log space is a serious
  system error which requires rebooting the database. */

  auto free = LOG_CHECKPOINT_FREE_PER_THREAD * 10 + LOG_CHECKPOINT_EXTRA_FREE;

  ulint margin;
  bool success = true;

  if (free >= smallest_capacity / 2) {
    success = false;
  } else {
    margin = smallest_capacity - free;
  }

  margin = std::min(margin, log_sys->adm_checkpoint_interval);

  margin = margin - margin / 10; /* Add still some extra safety */

  log_sys->log_group_capacity = smallest_capacity;

  log_sys->max_modified_age_async = margin - margin / LOG_POOL_PREFLUSH_RATIO_ASYNC;

  log_sys->max_modified_age_sync = margin - margin / LOG_POOL_PREFLUSH_RATIO_SYNC;

  log_sys->max_checkpoint_age_async = margin - margin / LOG_POOL_CHECKPOINT_RATIO_ASYNC;

  log_sys->max_checkpoint_age = margin;

  log_release();

  if (!success) {
    log_fatal(
      "Error: ib_logfiles are too small"
      " for thread_concurrency setting.\n"
      "The combined size of ib_logfiles"
      " should be bigger than\n"
      "200 kB.\n"
      "To get the server to start up, set"
      " thread_concurrency variable\n"
      "to a lower value, for example, to 8."
      " After an ERROR-FREE shutdown\n"
      "of the server you can adjust the size of"
      " ib_logfiles, as explained on\n"
      "the InnoDB website."
      "Cannot continue operation."
      " Forcing shutdown.\n"
    );
  }

  return success;
}

void innobase_log_init() {
  ut_a(log_sys == nullptr);

  log_sys = static_cast<log_t *>(mem_alloc(sizeof(log_t)));

  mutex_create(&log_sys->mutex, IF_DEBUG("log_sys_mutex",) IF_SYNC_DEBUG(SYNC_LOG,) Source_location{});

  log_acquire();

  /* Start the lsn from one log block from zero: this way every
  log record has a start lsn != zero, a fact which we will use */

  log_sys->lsn = LOG_START_LSN;

  ut_a(LOG_BUFFER_SIZE >= 16 * IB_FILE_BLOCK_SIZE);
  ut_a(LOG_BUFFER_SIZE >= 4 * UNIV_PAGE_SIZE);

  log_sys->buf_ptr = static_cast<byte *>(mem_alloc(LOG_BUFFER_SIZE + IB_FILE_BLOCK_SIZE));

  log_sys->buf = static_cast<byte *>(ut_align(log_sys->buf_ptr, IB_FILE_BLOCK_SIZE));

  log_sys->buf_size = LOG_BUFFER_SIZE;

  memset(log_sys->buf, '\0', LOG_BUFFER_SIZE);

  log_sys->max_buf_free = log_sys->buf_size / LOG_BUF_FLUSH_RATIO - LOG_BUF_FLUSH_MARGIN;
  log_sys->check_flush_or_checkpoint = true;
  UT_LIST_INIT(log_sys->log_groups);

  log_sys->n_log_ios = 0;

  log_sys->n_log_ios_old = log_sys->n_log_ios;
  log_sys->last_printout_time = time(nullptr);
  /*----------------------------*/

  log_sys->buf_next_to_write = 0;

  log_sys->write_lsn = 0;
  log_sys->current_flush_lsn = 0;
  log_sys->flushed_to_disk_lsn = 0;

  log_sys->written_to_some_lsn = log_sys->lsn;
  log_sys->written_to_all_lsn = log_sys->lsn;

  log_sys->n_pending_writes = 0;

  log_sys->no_flush_event = os_event_create(nullptr);

  os_event_set(log_sys->no_flush_event);

  log_sys->one_flushed_event = os_event_create(nullptr);

  os_event_set(log_sys->one_flushed_event);

  /*----------------------------*/
  log_sys->adm_checkpoint_interval = ULINT_MAX;

  log_sys->next_checkpoint_no = 0;
  log_sys->last_checkpoint_lsn = log_sys->lsn;
  log_sys->n_pending_checkpoint_writes = 0;

  rw_lock_create(&log_sys->checkpoint_lock, SYNC_NO_ORDER_CHECK);

  log_sys->checkpoint_buf_ptr = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));

  log_sys->checkpoint_buf = static_cast<byte *>(ut_align(log_sys->checkpoint_buf_ptr, IB_FILE_BLOCK_SIZE));

  memset(log_sys->checkpoint_buf, '\0', IB_FILE_BLOCK_SIZE);
  /*----------------------------*/

  log_block_init(log_sys->buf, log_sys->lsn);
  log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

  log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
  log_sys->lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;

  log_release();

#ifdef UNIV_LOG_DEBUG
  recv_sys_create();
  recv_sys_init(buf_pool->get_curr_size());

  recv_sys->parse_start_lsn = log_sys->lsn;
  recv_sys->scanned_lsn = log_sys->lsn;
  recv_sys->scanned_checkpoint_no = 0;
  recv_sys->recovered_lsn = log_sys->lsn;
  recv_sys->limit_lsn = IB_UINT64_T_MAX;
#endif /* UNIV_DEBUG */
}

void log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id) {
  auto group = static_cast<log_group_t *>(mem_alloc(sizeof(log_group_t)));

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

  UT_LIST_ADD_LAST(log_sys->log_groups, group);

  ut_a(log_calc_max_ages());
}

/**
 * Does the unlockings needed in flush I/O completion.
 *
 * @param code Any ORed combination of LOG_UNLOCK_FLUSH_LOCK and LOG_UNLOCK_NONE_FLUSHED_LOCK
 */
static void log_flush_do_unlocks(ulint code) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  /* NOTE that we must own the log mutex when doing the setting of the
     events: this is because transactions will wait for these events to
     be set, and at that moment the log flush they were waiting for must
     have ended. If the log mutex were not reserved here, the i/o-thread
     calling this function might be preempted for a while, and when it
     resumed execution, it might be that a new flush had been started, and
     this function would erroneously signal the NEW flush as completed.
     Thus, the changes in the state of these events are performed
     atomically in conjunction with the changes in the state of
     log_sys->n_pending_writes etc. */

  if (code & LOG_UNLOCK_NONE_FLUSHED_LOCK) {
    os_event_set(log_sys->one_flushed_event);
  }

  if (code & LOG_UNLOCK_FLUSH_LOCK) {
    os_event_set(log_sys->no_flush_event);
  }
}

/**
 * Checks if a flush is completed for a log group and performs the completion routine if yes.
 * 
 * @param group Log group
 * @return LOG_UNLOCK_NONE_FLUSHED_LOCK if flush is completed, 0 otherwise
 */
inline ulint log_group_check_flush_completion(log_group_t *group) /*!< [in] Log group */
{
  ut_ad(mutex_own(&(log_sys->mutex)));

  if (!log_sys->one_flushed && group->n_pending_writes == 0) {
#ifdef UNIV_DEBUG
    if (log_debug_writes) {
      ib_logger(ib_stream, "Log flushed first to group %lu\n", (ulong)group->id);
    }
#endif /* UNIV_DEBUG */
    log_sys->written_to_some_lsn = log_sys->write_lsn;
    log_sys->one_flushed = true;

    return LOG_UNLOCK_NONE_FLUSHED_LOCK;
  }

#ifdef UNIV_DEBUG
  if (log_debug_writes && (group->n_pending_writes == 0)) {
    ib_logger(ib_stream, "Log flushed to group %lu\n", (ulong)group->id);
  }
#endif /* UNIV_DEBUG */
  return 0;
}

/** Completes a checkpoint. */
static void log_complete_checkpoint() {
  ut_ad(mutex_own(&(log_sys->mutex)));
  ut_ad(log_sys->n_pending_checkpoint_writes == 0);

  log_sys->next_checkpoint_no++;

  log_sys->last_checkpoint_lsn = log_sys->next_checkpoint_lsn;

  rw_lock_x_unlock_gen(&(log_sys->checkpoint_lock), LOG_CHECKPOINT);
}

/** Completes an asynchronous checkpoint info write i/o to a log file. */
static void log_io_complete_checkpoint() {
  log_acquire();

  ut_ad(log_sys->n_pending_checkpoint_writes > 0);

  log_sys->n_pending_checkpoint_writes--;

  if (log_sys->n_pending_checkpoint_writes == 0) {
    log_complete_checkpoint();
  }

  log_release();
}

/** Checks if a flush is completed and does the completion routine if yes.
@return	LOG_UNLOCK_FLUSH_LOCK or 0 */
static ulint log_sys_check_flush_completion() {
  ulint move_start;
  ulint move_end;

  ut_ad(mutex_own(&(log_sys->mutex)));

  if (log_sys->n_pending_writes == 0) {

    log_sys->written_to_all_lsn = log_sys->write_lsn;
    log_sys->buf_next_to_write = log_sys->write_end_offset;

    if (log_sys->write_end_offset > log_sys->max_buf_free / 2) {
      /* Move the log buffer content to the start of the buffer */

      move_start = ut_calc_align_down(log_sys->write_end_offset, IB_FILE_BLOCK_SIZE);
      move_end = ut_calc_align(log_sys->buf_free, IB_FILE_BLOCK_SIZE);

      memmove(log_sys->buf, log_sys->buf + move_start, move_end - move_start);
      log_sys->buf_free -= move_start;

      log_sys->buf_next_to_write -= move_start;
    }

    return LOG_UNLOCK_FLUSH_LOCK;
  }

  return 0;
}

void log_io_complete(log_group_t *group) {
  ulint unlock;

  if ((ulint)group & 0x1UL) {
    /* It was a checkpoint write */
    group = (log_group_t *)((ulint)group - 1);

    if (srv_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC) {

      fil_flush(group->space_id);
    }

#ifdef UNIV_DEBUG
    if (log_debug_writes) {
      ib_logger(ib_stream, "Checkpoint info written to group %lu\n", group->id);
    }
#endif /* UNIV_DEBUG */
    log_io_complete_checkpoint();

    return;
  }

  /* We currently use synchronous writing of the logs and cannot end up here! */
  ut_error;

  if (srv_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC && srv_flush_log_at_trx_commit != 2) {

    fil_flush(group->space_id);
  }

  log_acquire();

  ut_a(group->n_pending_writes > 0);
  ut_a(log_sys->n_pending_writes > 0);

  group->n_pending_writes--;
  log_sys->n_pending_writes--;

  unlock = log_group_check_flush_completion(group);
  unlock = unlock | log_sys_check_flush_completion();

  log_flush_do_unlocks(unlock);

  log_release();
}

/**
 * Writes a log file header to a log file space.
 *
 * @param group The log group
 * @param nth_file The header to the nth file in the log file space
 * @param start_lsn The log file data starts at this LSN
 */
static void log_group_file_header_flush(log_group_t *group, ulint nth_file, uint64_t start_lsn) {
  ut_ad(mutex_own(&(log_sys->mutex)));
  ut_a(nth_file < group->n_files);

  auto buf = group->file_header_bufs[nth_file];

  mach_write_to_4(buf + LOG_GROUP_ID, group->id);
  mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);

  /* Wipe over possible label of ibbackup --restore */
  memcpy(buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);

  auto dest_offset = nth_file * group->file_size;

#ifdef UNIV_DEBUG
  if (log_debug_writes) {
    ib_logger(ib_stream, "Writing log file header to group %lu file %lu\n", (ulong)group->id, (ulong)nth_file);
  }
#endif /* UNIV_DEBUG */

  if (log_do_write) {
    log_sys->n_log_ios++;

    srv_os_log_pending_writes++;

    fil_io(
      OS_FILE_WRITE | OS_FILE_LOG,
      true,
      group->space_id,
      dest_offset / UNIV_PAGE_SIZE,
      dest_offset % UNIV_PAGE_SIZE,
      IB_FILE_BLOCK_SIZE,
      buf,
      group
    );

    srv_os_log_pending_writes--;
  }
}

/**
 * Stores a 4-byte checksum to the trailer checksum field of a log block
 * before writing it to a log file. This checksum is used in recovery to
 * check the consistency of a log block.
 *
 * @param block Pointer to a log block
 */
static void log_block_store_checksum(byte *block) {
  log_block_set_checksum(block, log_block_calc_checksum(block));
}

void log_group_write_buf(log_group_t *group, byte *buf, ulint len, uint64_t start_lsn, ulint new_data_offset) {
  ut_ad(mutex_own(&(log_sys->mutex)));
  ut_a(len % IB_FILE_BLOCK_SIZE == 0);
  ut_a(((ulint)start_lsn) % IB_FILE_BLOCK_SIZE == 0);

  ulint write_len;
  ulint next_offset;
  auto write_header = new_data_offset == 0;

  while (len > 0) {
    next_offset = log_group_calc_lsn_offset(start_lsn, group);

    if ((next_offset % group->file_size == LOG_FILE_HDR_SIZE) && write_header) {
      /* We start to write a new log file instance in the group */
      log_group_file_header_flush(group, next_offset / group->file_size, start_lsn);
      srv_os_log_written += IB_FILE_BLOCK_SIZE;
      srv_log_writes++;
    }

    if ((next_offset % group->file_size) + len > group->file_size) {
      write_len = group->file_size - (next_offset % group->file_size);
    } else {
      write_len = len;
    }

#ifdef UNIV_DEBUG
    if (log_debug_writes) {
      ib_logger(
        ib_stream,
        "Writing log file segment to group %lu"
        " offset %lu len %lu\n"
        "start lsn %lu\n"
        "First block n:o %lu last block n:o %lu\n",
        (ulong)group->id,
        (ulong)next_offset,
        (ulong)write_len,
        start_lsn,
        (ulong)log_block_get_hdr_no(buf),
        (ulong)log_block_get_hdr_no(buf + write_len - IB_FILE_BLOCK_SIZE)
      );

      ut_a(log_block_get_hdr_no(buf) == log_block_convert_lsn_to_no(start_lsn));

      for (ulint i = 0; i < write_len / IB_FILE_BLOCK_SIZE; i++) {
        ut_a(log_block_get_hdr_no(buf) + i == log_block_get_hdr_no(buf + i * IB_FILE_BLOCK_SIZE));
      }
    }
#endif /* UNIV_DEBUG */

    // Calculate the checksums for each log block and write them
    // to the trailer fields of the log blocks
    for (ulint i = 0; i < write_len / IB_FILE_BLOCK_SIZE; i++) {
      log_block_store_checksum(buf + i * IB_FILE_BLOCK_SIZE);
    }

    if (log_do_write) {
      ++log_sys->n_log_ios;
      ++srv_os_log_pending_writes;

      fil_io(
        OS_FILE_WRITE | OS_FILE_LOG,
        true,
        group->space_id,
        next_offset / UNIV_PAGE_SIZE,
        next_offset % UNIV_PAGE_SIZE,
        write_len,
        buf,
        group
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

void log_write_up_to(uint64_t lsn, ulint wait, bool flush_to_disk) {
  log_group_t *group;
  ulint start_offset;
  ulint end_offset;
  ulint area_start;
  ulint area_end;
  ulint unlock;

#ifdef UNIV_DEBUG
  ulint loop_count = 0;
#endif /* UNIV_DEBUG */

  auto do_waits = [](ulint wait) {
    switch (wait) {
      case LOG_WAIT_ONE_GROUP:
        os_event_wait(log_sys->one_flushed_event);
        break;
      case LOG_WAIT_ALL_GROUPS:
        os_event_wait(log_sys->no_flush_event);
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
#ifdef UNIV_DEBUG
    ++loop_count;
    ut_ad(loop_count < 5);
#endif /* UNIV_DEBUG */

    log_acquire();

    if (flush_to_disk && log_sys->flushed_to_disk_lsn >= lsn) {
      log_release();
      return;
    }

    if (!flush_to_disk && (log_sys->written_to_all_lsn >= lsn || (log_sys->written_to_some_lsn >= lsn && wait != LOG_WAIT_ALL_GROUPS))) {
      log_release();
      return;
    }

    if (log_sys->n_pending_writes > 0) {
      /* A write (+ possibly flush to disk) is running */

      if (flush_to_disk && log_sys->current_flush_lsn >= lsn) {
        /* The write + flush will write enough: wait for it to complete  */
        log_release();
        do_waits(wait);
        break;
      }

      if (!flush_to_disk && log_sys->write_lsn >= lsn) {
        /* The write will write enough: wait for it to complete  */
        log_release();
        do_waits(wait);
        break;
      }

      log_release();

      /* Wait for the write to complete and try to start a new write */
      os_event_wait(log_sys->no_flush_event);
      continue;
    }

    if (!flush_to_disk && log_sys->buf_free == log_sys->buf_next_to_write) {
      /* Nothing to write and no flush to disk requested */
      log_release();
      return;
    }

#ifdef UNIV_DEBUG
    if (log_debug_writes) {
      ib_logger(ib_stream, "Writing log from %lu up to lsn %lu\n", log_sys->written_to_all_lsn, log_sys->lsn);
    }
#endif /* UNIV_DEBUG */
    log_sys->n_pending_writes++;

    group = UT_LIST_GET_FIRST(log_sys->log_groups);
    group->n_pending_writes++; /*!< We assume here that we have only one log group! */

    os_event_reset(log_sys->no_flush_event);
    os_event_reset(log_sys->one_flushed_event);

    start_offset = log_sys->buf_next_to_write;
    end_offset = log_sys->buf_free;

    area_start = ut_calc_align_down(start_offset, IB_FILE_BLOCK_SIZE);
    area_end = ut_calc_align(end_offset, IB_FILE_BLOCK_SIZE);

    ut_ad(area_end - area_start > 0);

    log_sys->write_lsn = log_sys->lsn;

    if (flush_to_disk) {
      log_sys->current_flush_lsn = log_sys->lsn;
    }

    log_sys->one_flushed = false;

    log_block_set_flush_bit(log_sys->buf + area_start, true);

    log_block_set_checkpoint_no(log_sys->buf + area_end - IB_FILE_BLOCK_SIZE, log_sys->next_checkpoint_no);

    /* Copy the last, incompletely written, log block a log block length
    up, so that when the flush operation writes from the log buffer, the
    segment to write will not be changed by writers to the log */
    memcpy(log_sys->buf + area_end, log_sys->buf + area_end - IB_FILE_BLOCK_SIZE, IB_FILE_BLOCK_SIZE);

    log_sys->buf_free += IB_FILE_BLOCK_SIZE;
    log_sys->write_end_offset = log_sys->buf_free;

    group = UT_LIST_GET_FIRST(log_sys->log_groups);

    /* Do the write to the log files */
    while (group != nullptr) {
      log_group_write_buf(
        group,
        log_sys->buf + area_start,
        area_end - area_start,
        ut_uint64_align_down(log_sys->written_to_all_lsn, IB_FILE_BLOCK_SIZE),
        start_offset - area_start
      );

      log_group_set_fields(group, log_sys->write_lsn);

      group = UT_LIST_GET_NEXT(log_groups, group);
    }

    log_release();

    if (srv_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
      /* O_DSYNC means the OS did not buffer the log file at all:
      so we have also flushed to disk what we have written */
      log_sys->flushed_to_disk_lsn = log_sys->write_lsn;
    } else if (flush_to_disk) {
      group = UT_LIST_GET_FIRST(log_sys->log_groups);
      fil_flush(group->space_id);
      log_sys->flushed_to_disk_lsn = log_sys->write_lsn;
    }

    log_acquire();

    group = UT_LIST_GET_FIRST(log_sys->log_groups);

    ut_a(group->n_pending_writes == 1);
    ut_a(log_sys->n_pending_writes == 1);

    group->n_pending_writes--;
    log_sys->n_pending_writes--;

    unlock = log_group_check_flush_completion(group);
    unlock = unlock | log_sys_check_flush_completion();

    log_flush_do_unlocks(unlock);

    log_release();

    return;
  }
}

void log_buffer_flush_to_disk() {
  log_acquire();

  auto lsn = log_sys->lsn;

  log_release();

  log_write_up_to(lsn, LOG_WAIT_ALL_GROUPS, true);
}

void log_buffer_sync_in_background(bool flush) {
  mutex_enter(&(log_sys->mutex));

  auto lsn = log_sys->lsn;

  mutex_exit(&(log_sys->mutex));

  log_write_up_to(lsn, LOG_NO_WAIT, flush);
}

/** Tries to establish a big enough margin of free space in
the log buffer, such that a new log entry can be catenated
without an immediate need for a flush. */
static void log_flush_margin() {
  auto log = log_sys;

  lsn_t lsn = 0;

  log_acquire();

  if (log->buf_free > log->max_buf_free) {

    if (log->n_pending_writes > 0) {
      /* A flush is running: hope that it will provide enough
      free space */
    } else {
      lsn = log->lsn;
    }
  }

  log_release();

  if (lsn != 0) {
    log_write_up_to(lsn, LOG_NO_WAIT, false);
  }
}

bool log_preflush_pool_modified_pages(lsn_t new_oldest, bool sync) {
  if (recv_recovery_on) {
    /* If the recovery is running, we must first apply all
    log records to their respective file pages to get the
    right modify lsn values to these pages: otherwise, there
    might be pages on disk which are not yet recovered to the
    current lsn, and even after calling this function, we could
    not know how up-to-date the disk version of the database is,
    and we could not make a new checkpoint on the basis of the
    info on the buffer pool only. */

    recv_apply_hashed_log_recs(false);
  }

  auto n_pages = buf_pool->m_flusher->batch(BUF_FLUSH_LIST, ULINT_MAX, new_oldest);

  if (sync) {
    buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);
  }

  return n_pages != ULINT_UNDEFINED;
}

/**
 * Writes info to a checkpoint about a log group.
 *
 * @param buf Buffer for checkpoint info
 * @param n Nth slot
 * @param file_no Archived file number
 * @param offset Archived file offset
 */
static void log_checkpoint_set_nth_group_info(byte *buf, ulint n, ulint file_no, ulint offset) {
  ut_ad(n < LOG_MAX_N_GROUPS);

  mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_FILE_NO, file_no);
  mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_OFFSET, offset);
}

void log_checkpoint_get_nth_group_info(const byte *buf, ulint n, ulint *file_no, ulint *offset) {
  ut_ad(n < LOG_MAX_N_GROUPS);

  *file_no = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_FILE_NO);

  *offset = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_UNUSED_OFFSET);
}

/**
 * Writes the checkpoint info to a log group header.
 *
 * @param[in,out] group Log group.
 */
static void log_group_checkpoint(log_group_t *group) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  static_assert(LOG_CHECKPOINT_SIZE <= IB_FILE_BLOCK_SIZE, "error LOG_CHECKPOINT_SIZE > IB_FILE_BLOCK_SIZE");

  auto buf = group->checkpoint_buf;

  /** Write the checkpoint number */
  mach_write_to_8(buf + LOG_CHECKPOINT_NO, log_sys->next_checkpoint_no);

  /** Write the checkpoint LSN */
  mach_write_to_8(buf + LOG_CHECKPOINT_LSN, log_sys->next_checkpoint_lsn);

  /** Write the checkpoint offset */
  mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, log_group_calc_lsn_offset(log_sys->next_checkpoint_lsn, group));

  /** Write the log buffer size */
  mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, log_sys->buf_size);

  /** Write the unused LSN */
  mach_write_to_8(buf + LOG_CHECKPOINT_UNUSED_LSN, IB_UINT64_T_MAX);

  /** Initialize group info to 0 */
  for (ulint i = 0; i < LOG_MAX_N_GROUPS; i++) {
    log_checkpoint_set_nth_group_info(buf, i, 0, 0);
  }

  /** Write group info for each log group */
  auto group2 = UT_LIST_GET_FIRST(log_sys->log_groups);

  while (group2 != nullptr) {
    log_checkpoint_set_nth_group_info(buf, group2->id, 0, 0);

    group2 = UT_LIST_GET_NEXT(log_groups, group2);
  }

  /** Calculate and write the first checksum */
  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

  /** Calculate and write the second checksum */
  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

  /** Write the free limit of the tablespace */
  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_FREE_LIMIT, log_fsp_current_free_limit);

  /** Write the magic number for the tablespace */
  mach_write_to_4(buf + LOG_CHECKPOINT_FSP_MAGIC_N, LOG_CHECKPOINT_FSP_MAGIC_N_VAL);

  ulint write_offset;

  /** Alternate the physical place of the checkpoint info in the first log file */
  if ((log_sys->next_checkpoint_no & 1) == 0) {
    write_offset = LOG_CHECKPOINT_1;
  } else {
    write_offset = LOG_CHECKPOINT_2;
  }

  if (log_do_write) {
    if (log_sys->n_pending_checkpoint_writes == 0) {
      rw_lock_x_lock_gen(&(log_sys->checkpoint_lock), LOG_CHECKPOINT);
    }

    log_sys->n_pending_checkpoint_writes++;

    log_sys->n_log_ios++;

    /** Send the log file write request */
    fil_io(
      OS_FILE_WRITE | OS_FILE_LOG,
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

void log_reset_first_header_and_checkpoint(byte *hdr_buf, lsn_t start) {
  mach_write_to_4(hdr_buf + LOG_GROUP_ID, 0);
  mach_write_to_8(hdr_buf + LOG_FILE_START_LSN, start);

  auto lsn = start + LOG_BLOCK_HDR_SIZE;

  /* Write the label of ibbackup --restore */
  strcpy((char *)hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "ei");

  ut_sprintf_timestamp((char *)hdr_buf + (LOG_FILE_WAS_CREATED_BY_HOT_BACKUP + (sizeof "ei") - 1));

  auto buf = hdr_buf + LOG_CHECKPOINT_1;

  mach_write_to_8(buf + LOG_CHECKPOINT_NO, 0);
  mach_write_to_8(buf + LOG_CHECKPOINT_LSN, lsn);

  mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, LOG_FILE_HDR_SIZE + LOG_BLOCK_HDR_SIZE);

  mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, 2 * 1024 * 1024);

  mach_write_to_8(buf + LOG_CHECKPOINT_UNUSED_LSN, IB_UINT64_T_MAX);

  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);

  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
  mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

  /* Starting from InnoDB-3.23.50, we should also write info on
  allocated size in the tablespace, but unfortunately we do not
  know it here */
}

void log_group_read_checkpoint_info(log_group_t *group, ulint field) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  log_sys->n_log_ios++;

  fil_io(
    OS_FILE_READ | OS_FILE_LOG,
    true,
    group->space_id,
    field / UNIV_PAGE_SIZE,
    field % UNIV_PAGE_SIZE,
    IB_FILE_BLOCK_SIZE,
    log_sys->checkpoint_buf,
    nullptr
  );
}

void log_groups_write_checkpoint_info(void) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  auto group = UT_LIST_GET_FIRST(log_sys->log_groups);

  while (group != nullptr) {
    log_group_checkpoint(group);

    group = UT_LIST_GET_NEXT(log_groups, group);
  }
}

bool log_checkpoint(bool sync, bool write_always) {
  if (recv_recovery_on) {
    recv_apply_hashed_log_recs(false);
  }

  if (srv_unix_file_flush_method != SRV_UNIX_NOSYNC) {
    fil_flush_file_spaces(FIL_TABLESPACE);
  }

  log_acquire();

  auto oldest_lsn = log_buf_pool_get_oldest_modification();

  log_release();

  /* Because log also contains headers and dummy log records,
  if the buffer pool contains no dirty buffers, oldest_lsn
  gets the value log_sys->lsn from the previous function,
  and we must make sure that the log is flushed up to that
  lsn. If there are dirty buffers in the buffer pool, then our
  write-ahead-logging algorithm ensures that the log has been flushed
  up to oldest_lsn. */

  log_write_up_to(oldest_lsn, LOG_WAIT_ALL_GROUPS, true);

  log_acquire();

  if (!write_always && log_sys->last_checkpoint_lsn >= oldest_lsn) {

    log_release();

    return true;
  }

  ut_ad(log_sys->flushed_to_disk_lsn >= oldest_lsn);

  if (log_sys->n_pending_checkpoint_writes > 0) {
    /* A checkpoint write is running */

    log_release();

    if (sync) {
      /* Wait for the checkpoint write to complete */
      rw_lock_s_lock(&(log_sys->checkpoint_lock));
      rw_lock_s_unlock(&(log_sys->checkpoint_lock));
    }

    return false;
  }

  log_sys->next_checkpoint_lsn = oldest_lsn;

#ifdef UNIV_DEBUG
  if (log_debug_writes) {
    ib_logger(ib_stream, "Making checkpoint no %lu at lsn %lu\n", (ulong)log_sys->next_checkpoint_no, oldest_lsn);
  }
#endif /* UNIV_DEBUG */

  log_groups_write_checkpoint_info();

  log_release();

  if (sync) {
    /* Wait for the checkpoint write to complete */
    rw_lock_s_lock(&(log_sys->checkpoint_lock));
    rw_lock_s_unlock(&(log_sys->checkpoint_lock));
  }

  return true;
}

void log_make_checkpoint_at(lsn_t lsn, bool write_always) {
  /* Preflush pages synchronously */

  while (!log_preflush_pool_modified_pages(lsn, true)) {
    ;
  }

  while (!log_checkpoint(true, write_always)) {
    ;
  }
}

/**
 * Tries to establish a big enough margin of free space in the log groups, such
 * that a new log entry can be catenated without an immediate need for a checkpoint.
 * NOTE: this function may only be called if the calling thread owns no synchronization objects!
 */
static void log_checkpoint_margin() {
  log_t *log = log_sys;
  uint64_t age;
  uint64_t checkpoint_age;
  uint64_t advance;
  uint64_t oldest_lsn;
  bool sync;
  bool checkpoint_sync;
  bool do_checkpoint;
  bool success;

  for (;;) {
    sync = false;
    checkpoint_sync = false;
    do_checkpoint = false;

    log_acquire();

    if (!log->check_flush_or_checkpoint) {
      log_release();
      return;
    }

    oldest_lsn = log_buf_pool_get_oldest_modification();

    age = log->lsn - oldest_lsn;

    if (age > log->max_modified_age_sync) {
      sync = true;
      advance = 2 * (age - log->max_modified_age_sync);
    } else if (age > log->max_modified_age_async) {
      advance = age - log->max_modified_age_async;
    } else {
      advance = 0;
    }

    checkpoint_age = log->lsn - log->last_checkpoint_lsn;

    if (checkpoint_age > log->max_checkpoint_age) {
      checkpoint_sync = true;
      do_checkpoint = true;
    } else if (checkpoint_age > log->max_checkpoint_age_async) {
      do_checkpoint = true;
      log->check_flush_or_checkpoint = false;
    } else {
      log->check_flush_or_checkpoint = false;
    }

    log_release();

    if (advance) {
      auto new_oldest = oldest_lsn + advance;

      success = log_preflush_pool_modified_pages(new_oldest, sync);

      if (sync && !success) {
        continue;
      }
    }

    if (do_checkpoint) {
      log_checkpoint(checkpoint_sync, false);

      if (checkpoint_sync) {
        continue;
      }
    }

    break;
  }
}

void log_group_read_log_seg(ulint type, byte *buf, log_group_t *group, uint64_t start_lsn, uint64_t end_lsn) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  auto sync = (type == LOG_RECOVER);
  auto source_offset = log_group_calc_lsn_offset(start_lsn, group);
  auto len = (ulint)(end_lsn - start_lsn);

  ut_ad(len != 0);

  while (len > 0) {
    if ((source_offset % group->file_size) + len > group->file_size) {
      len = group->file_size - (source_offset % group->file_size);
    }

    log_sys->n_log_ios++;

    fil_io(
      OS_FILE_READ | OS_FILE_LOG,
      sync,
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

void log_check_margins() {
  for (;;) {
    log_flush_margin();

    log_checkpoint_margin();

    log_acquire();

    if (!log_sys->check_flush_or_checkpoint) {
      break;
    }

    log_release();
  }

  log_release();
}

void logs_empty_and_mark_files_at_shutdown(ib_recovery_t recovery, ib_shutdown_t shutdown) {
  lsn_t lsn;

  /* If we have to abort during the startup phase then it's possible
  that the log sub-system hasn't as yet been initialized. We simply
  attempt to close all open files and return. */
  if (log_sys == nullptr || UT_LIST_GET_LEN(log_sys->log_groups) == 0) {

    fil_close_all_files();

    return;
  }

  if (srv_print_verbose_log) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  Starting shutdown...\n");
  }
  /* Wait until the master thread and all other operations are idle: our
  algorithm only works if the server is idle at shutdown */

  srv_shutdown_state = SRV_SHUTDOWN_CLEANUP;

  for (;;) {
    os_thread_sleep(100000);

    mutex_enter(&kernel_mutex);

    /* We need the monitor threads to stop before we proceed with a
    normal shutdown. In case of very fast shutdown, however, we can
    proceed without waiting for monitor threads. */

    if (shutdown != IB_SHUTDOWN_NO_BUFPOOL_FLUSH && (srv_error_monitor_active || srv_lock_timeout_active || srv_monitor_active)) {

      mutex_exit(&kernel_mutex);
      continue;
    }

    /* Check that there are no longer transactions. We need this wait even
    for the 'very fast' shutdown, because the InnoDB layer may have
    committed or prepared transactions and we don't want to lose them. */

    if (trx_n_transactions > 0 || (trx_sys != nullptr && UT_LIST_GET_LEN(trx_sys->trx_list) > 0)) {

      mutex_exit(&kernel_mutex);

      continue;
    }

    if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
      /* In this fastest shutdown we do not flush the buffer pool:
      it is essentially a 'crash' of the InnoDB server. Make sure
      that the log is all flushed to disk, so that we can recover
      all committed transactions in a crash recovery. We must not
      write the lsn stamps to the data files, since at a startup
      InnoDB deduces from the stamps if the previous shutdown was
      clean. */

      log_buffer_flush_to_disk();

      mutex_exit(&kernel_mutex);

      return; /* We SKIP ALL THE REST !! */
    }

    /* Check that the master thread is suspended */
    if (srv_n_threads_active[SRV_MASTER] != 0) {

      mutex_exit(&kernel_mutex);

      continue;
    }

    mutex_exit(&kernel_mutex);

    log_acquire();

    if (log_sys->n_pending_checkpoint_writes || log_sys->n_pending_writes) {

      log_release();

      continue;
    }

    log_release();

    if (buf_pool->is_io_pending()) {

      continue;
    }

    log_make_checkpoint_at(IB_UINT64_T_MAX, true);

    log_acquire();

    lsn = log_sys->lsn;

    if (lsn != log_sys->last_checkpoint_lsn) {

      log_release();

      continue;
    }

    log_release();

    mutex_enter(&kernel_mutex);

    /* Check that the master thread has stayed suspended */
    if (srv_n_threads_active[SRV_MASTER] != 0) {
      ib_logger(
        ib_stream,
        "Warning: the master thread woke up"
        " during shutdown\n"
      );

      mutex_exit(&kernel_mutex);

      continue;
    }

    mutex_exit(&kernel_mutex);

    fil_flush_file_spaces(FIL_TABLESPACE);
    fil_flush_file_spaces(FIL_LOG);

    /* The call fil_write_flushed_lsn_to_data_files() will pass the buffer
    pool: therefore it is essential that the buffer pool has been
    completely flushed to disk! (We do not call fil_write... if the
    'very fast' shutdown is enabled.) */

    if (buf_pool->all_freed()) {
      break;
    }
  }

  srv_shutdown_state = SRV_SHUTDOWN_LAST_PHASE;

  /* Make some checks that the server really is quiet */
  ut_a(srv_n_threads_active[SRV_MASTER] == 0);
  ut_a(buf_pool->all_freed());
  ut_a(lsn == log_sys->lsn);

  if (lsn < srv_start_lsn) {
    ib_logger(
      ib_stream,
      "Error: log sequence number"
      " at shutdown %lu\n"
      "is lower than at startup %lu!\n",
      lsn,
      srv_start_lsn
    );
  }

  srv_shutdown_lsn = lsn;

  fil_write_flushed_lsn_to_data_files(lsn);

  fil_flush_file_spaces(FIL_TABLESPACE);

  fil_close_all_files();

  /* Make some checks that the server really is quiet */
  ut_a(srv_n_threads_active[SRV_MASTER] == 0);
  ut_a(buf_pool->all_freed());
  ut_a(lsn == log_sys->lsn);
}

#ifdef UNIV_LOG_DEBUG
bool log_check_log_recs(const byte *buf, ulint len, lsn_t buf_start_lsn) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  if (len == 0) {

    return true;
  }

  auto start = static_cast<byte *>(ut_align_down(buf, IB_FILE_BLOCK_SIZE));
  auto end = static_cast<byte *>(ut_align(buf + len, IB_FILE_BLOCK_SIZE));
  auto buf1 = static_cast<byte *>(mem_alloc((end - start) + IB_FILE_BLOCK_SIZE));
  auto scan_buf = static_cast<byte *>(ut_align(buf1, IB_FILE_BLOCK_SIZE));

  memcpy(scan_buf, start, end - start);

  lsn_t scanned_lsn;
  lsn_t contiguous_lsn;

  recv_scan_log_recs(
    recovery,
    (buf_pool->curr_size - recv_n_pool_free_frames) * UNIV_PAGE_SIZE,
    false,
    scan_buf,
    end - start,
    ut_uint64_align_down(buf_start_lsn, IB_FILE_BLOCK_SIZE),
    &contiguous_lsn,
    &scanned_lsn
  );

  ut_a(scanned_lsn == buf_start_lsn + len);
  ut_a(recv_sys->recovered_lsn == scanned_lsn);

  mem_free(buf1);

  return true;
}
#endif /* UNIV_LOG_DEBUG */

bool log_peek_lsn(lsn_t *lsn) {
  if (mutex_enter_nowait(&(log_sys->mutex)) == 0) {
    *lsn = log_sys->lsn;

    log_release();

    return true;
  } else {
    return false;
  }
}

void log_print(ib_stream_t ib_stream) {
  log_acquire();

  ib_logger(
    ib_stream,
    "Log sequence number %lu\n"
    "Log flushed up to   %lu\n"
    "Last checkpoint at  %lu\n",
    log_sys->lsn,
    log_sys->flushed_to_disk_lsn,
    log_sys->last_checkpoint_lsn
  );

  auto current_time = time(nullptr);
  auto time_elapsed = 0.001 + difftime(current_time, log_sys->last_printout_time);

  ib_logger(
    ib_stream,
    "%lu pending log writes, %lu pending chkp writes\n"
    "%lu log i/o's done, %.2f log i/o's/second\n",
    (ulong)log_sys->n_pending_writes,
    (ulong)log_sys->n_pending_checkpoint_writes,
    (ulong)log_sys->n_log_ios,
    ((log_sys->n_log_ios - log_sys->n_log_ios_old) / time_elapsed)
  );

  log_sys->n_log_ios_old = log_sys->n_log_ios;
  log_sys->last_printout_time = current_time;

  log_release();
}

void log_refresh_stats() {
  log_sys->n_log_ios_old = log_sys->n_log_ios;
  log_sys->last_printout_time = time(nullptr);
}

/** Closes a log group.
@param[in,own] group            Log group to close. */
static void log_group_close(log_group_t *group) {
  for (ulint i = 0; i < group->n_files; ++i) {
    mem_free(group->file_header_bufs_ptr[i]);
  }

  mem_free(group->file_header_bufs);
  mem_free(group->file_header_bufs_ptr);

  mem_free(group->checkpoint_buf_ptr);

  mem_free(group);
}

void log_shutdown() {
  log_group_t *group;

  /* This can happen if we have to abort during startup. */
  if (log_sys == nullptr || UT_LIST_GET_LEN(log_sys->log_groups) == 0) {
    return;
  }

  group = UT_LIST_GET_FIRST(log_sys->log_groups);

  while (UT_LIST_GET_LEN(log_sys->log_groups) > 0) {
    log_group_t *prev_group = group;

    group = UT_LIST_GET_NEXT(log_groups, group);
    UT_LIST_REMOVE(log_sys->log_groups, prev_group);

    log_group_close(prev_group);
  }

  mem_free(log_sys->buf_ptr);
  log_sys->buf_ptr = nullptr;
  mem_free(log_sys->checkpoint_buf_ptr);
  log_sys->checkpoint_buf_ptr = nullptr;

  os_event_free(log_sys->no_flush_event);
  os_event_free(log_sys->one_flushed_event);

  rw_lock_free(&log_sys->checkpoint_lock);

#ifdef UNIV_LOG_DEBUG
  recv_sys_debug_free();
#endif /* UNIV_LOG_DEBUG */

  recv_sys_close();
}

void log_mem_free() {
  if (log_sys != nullptr) {
    recv_sys_mem_free();
    mem_free(log_sys);

    log_sys = nullptr;
  }
}

void log_free_check() {
  if (log_sys->check_flush_or_checkpoint) {
    log_check_margins();
  }
}
