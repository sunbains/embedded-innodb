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

/** @file include/log0log.h
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "log0types.h"
#include "api0api.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "ut0lst.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "os0file.h"

extern log_t *log_sys;

/** Acquire the log mutex. */
inline void log_acquire() {
  ut_ad(!mutex_own(&log_sys->mutex));
  mutex_enter(&log_sys->mutex);
}

/** Releases the log mutex. */
inline void log_release() {
  ut_ad(mutex_own(&log_sys->mutex));
  mutex_exit(&log_sys->mutex);
}

#ifdef UNIV_LOG_DEBUG
/**
 * Checks by parsing that the concatenated log segment for a single mtr is consistent.
 *
 * @param buf The pointer to the start of the log segment in the log_sys->buf log buffer.
 * @param len The segment length in bytes.
 * @param buf_start_lsn The buffer start lsn.
 * @return True if the log segment is consistent, false otherwise.
 */
bool log_check_log_recs(const byte *buf, ulint len, lsn_t buf_start_lsn);
#endif /* UNIV_LOG_DEBUG */


/**
 * Gets the flush bit of a log block.
 *
 * @param log_block The log block.
 * @return True if this block was the first to be written in a log flush, false otherwise.
 */
inline bool log_block_get_flush_bit(const byte *log_block) {
  return (LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO)) != 0;
}

/**
 * Sets the flush bit of a log block.
 *
 * @param log_block The log block.
 * @param val The value to set.
 */
inline void log_block_set_flush_bit(byte *log_block, bool val) {
  ulint field = mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);

  if (val) {
    field |= LOG_BLOCK_FLUSH_BIT_MASK;
  } else {
    field &= ~LOG_BLOCK_FLUSH_BIT_MASK;
  }

  mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, field);
}

/**
 * Gets the log block number stored in the header.
 *
 * @param log_block The log block.
 * @return The log block number stored in the block header.
 */
inline ulint log_block_get_hdr_no(const byte *log_block) {
  return (~LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO));
}

/**
 * Sets the log block number stored in the header.
 * NOTE: This must be set before the flush bit!
 *
 * @param log_block The log block.
 * @param n The log block number: must be > 0 and < LOG_BLOCK_FLUSH_BIT_MASK.
 */
inline void log_block_set_hdr_no(byte *log_block, ulint n) {
  ut_ad(n > 0);
  ut_ad(n < LOG_BLOCK_FLUSH_BIT_MASK);

  mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, n);
}

/**
 * Gets the data length of a log block.
 *
 * @param log_block The log block.
 * @return The log block data length measured as a byte offset from the block start.
 */
inline ulint log_block_get_data_len(const byte *log_block) {
  return mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN);
}

/**
 * Sets the data length of a log block.
 *
 * @param log_block The log block.
 * @param len The data length.
 */
inline void log_block_set_data_len(byte *log_block, ulint len) {
  mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
}

/**
 * Gets the offset of the first mtr log record group in a log block.
 *
 * @param log_block The log block.
 * @return The first mtr log record group byte offset from the block start, 0 if none.
 */
inline ulint log_block_get_first_rec_group(const byte *log_block) {
  return mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP);
}

/**
 * Sets the offset of the first mtr log record group in a log block.
 *
 * @param log_block The log block.
 * @param offset The offset, 0 if none.
 */
inline void log_block_set_first_rec_group(byte *log_block, ulint offset) {
  mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
}

/**
 * Gets the checkpoint number field of a log block.
 *
 * @param log_block The log block.
 * @return The checkpoint number (4 lowest bytes).
 */
inline ulint log_block_get_checkpoint_no(const byte *log_block) {
  return mach_read_from_4(log_block + LOG_BLOCK_CHECKPOINT_NO);
}

/**
 * Sets the checkpoint number field of a log block.
 *
 * @param log_block The log block.
 * @param no The checkpoint number.
 */
inline void log_block_set_checkpoint_no(byte *log_block, uint64_t no) {
  mach_write_to_4(log_block + LOG_BLOCK_CHECKPOINT_NO, (ulint)no);
}

/**
 * Converts a lsn to a log block number.
 *
 * @param lsn The lsn of a byte within the block.
 * @return The log block number, it is > 0 and <= 1G.
 */
inline ulint log_block_convert_lsn_to_no(lsn_t lsn) {
  return ((ulint)(lsn / IB_FILE_BLOCK_SIZE) & 0x3FFFFFFFUL) + 1;
}

/**
 * Calculates the checksum for a log block.
 *
 * @param block The log block.
 * @return The checksum.
 */
inline ulint log_block_calc_checksum(const byte *block) {
  auto sh = 0; // Shift value.
  auto sum = 1; // Checksum value.

  for (ulint i = 0; i < IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE; i++) {
    ulint b = ulint(block[i]);

    sum &= 0x7FFFFFFFUL;
    sum += b;
    sum += b << sh;
    sh++;

    if (sh > 24) {
      sh = 0;
    }
  }

  return sum;
}

/**
 * Gets a log block checksum field value.
 *
 * @param log_block The log block.
 * @return The checksum.
 */
inline ulint log_block_get_checksum(const byte *log_block) {
  return mach_read_from_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM);
}

/**
 * Sets a log block checksum field value.
 *
 * @param log_block The log block.
 * @param checksum The checksum.
 */
inline void log_block_set_checksum(byte *log_block, ulint checksum) {
  mach_write_to_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, checksum);
}

/**
 * Initializes a log block in the log buffer.
 *
 * @param log_block Pointer to the log buffer.
 * @param lsn LSN within the log block.
 */
inline void log_block_init(byte *log_block, lsn_t lsn) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  auto no = log_block_convert_lsn_to_no(lsn);

  log_block_set_hdr_no(log_block, no);

  log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
  log_block_set_first_rec_group(log_block, 0);
}

/**
 * Initializes a log block in the log buffer in the old format, where there was no checksum yet.
 *
 * @param log_block Pointer to the log buffer.
 * @param lsn LSN within the log block.
 */
inline void log_block_init_in_old_format(byte *log_block, lsn_t lsn) {
  ut_ad(mutex_own(&(log_sys->mutex)));

  auto no = log_block_convert_lsn_to_no(lsn);

  log_block_set_hdr_no(log_block, no);
  mach_write_to_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, no);
  log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
  log_block_set_first_rec_group(log_block, 0);
}

/**
 * Reserves space in the log buffer and writes a string to the log.
 * The log must be released with log_release().
 *
 * @param str The string to write to the log.
 * @param len The length of the string.
 * @param start_lsn[out] The start lsn of the log record.
 * @return The end lsn of the log record, or zero if it did not succeed.
 */
inline lsn_t log_reserve_and_write_fast(const void *str, ulint len, lsn_t *start_lsn) {
#ifdef UNIV_LOG_LSN_DEBUG
  // Length of the LSN pseudo-record
  ulint lsn_len;
#endif /* UNIV_LOG_LSN_DEBUG */

#ifdef UNIV_LOG_LSN_DEBUG
  lsn_len = 1 + mach_get_compressed_size(log_sys->lsn >> 32) +
            mach_get_compressed_size(log_sys->lsn & 0xFFFFFFFFUL);
#endif /* UNIV_LOG_LSN_DEBUG */

  auto data_len = len

#ifdef UNIV_LOG_LSN_DEBUG
             + lsn_len
#endif /* UNIV_LOG_LSN_DEBUG */
             + log_sys->buf_free % IB_FILE_BLOCK_SIZE;

  if (data_len >= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    // The string does not fit within the current log block or the log block would become full
    return 0;
  }

  *start_lsn = log_sys->lsn;

#ifdef UNIV_LOG_LSN_DEBUG
  {
    // Write the LSN pseudo-record
    byte *b = &log_sys->buf[log_sys->buf_free];
    *b++ = MLOG_LSN | (MLOG_SINGLE_REC_FLAG & *(const byte *)str);
    // Write the LSN in two parts, as a pseudo page number and space id
    b += mach_write_compressed(b, log_sys->lsn >> 32);
    b += mach_write_compressed(b, log_sys->lsn & 0xFFFFFFFFUL);
    ut_a(b - lsn_len == &log_sys->buf[log_sys->buf_free]);

    memcpy(b, str, len);
    len += lsn_len;
  }
#else  /* UNIV_LOG_LSN_DEBUG */
  memcpy(log_sys->buf + log_sys->buf_free, str, len);
#endif /* UNIV_LOG_LSN_DEBUG */

  log_block_set_data_len((byte *)ut_align_down(log_sys->buf + log_sys->buf_free, IB_FILE_BLOCK_SIZE), data_len);

#ifdef UNIV_LOG_DEBUG
  log_sys->old_buf_free = log_sys->buf_free;
  log_sys->old_lsn = log_sys->lsn;
#endif /* UNIV_LOG_DEBUG */

  log_sys->buf_free += len;

  ut_ad(log_sys->buf_free <= log_sys->buf_size);

  log_sys->lsn += len;

#ifdef UNIV_LOG_DEBUG
  log_check_log_recs(log_sys->buf + log_sys->old_buf_free, log_sys->buf_free - log_sys->old_buf_free, log_sys->old_lsn);
#endif /* UNIV_LOG_DEBUG */

  return log_sys->lsn;
}

/** Gets the current lsn.
@return	current lsn */
inline lsn_t log_get_lsn() {
  log_acquire();

  auto lsn = log_sys->lsn;

  log_release();

  return (lsn);
}

/**
 * Gets the log group capacity.
 * It is OK to read the value without holding log_sys->mutex because it is constant.
 *
 * @return The log group capacity.
 */
inline ulint log_get_capacity() { return (log_sys->log_group_capacity); }

/**
 * Checks if there is a need for a log buffer flush or a new checkpoint, and does this if yes.
 * Any database operation should call this when it has modified more than about 4 pages.
 * NOTE that this function may only be called when the OS thread owns no synchronization objects except the dictionary mutex.
 */
void log_free_check();

/**
 * @brief Inits a log group to the log system.
 *
 * @param id The group id.
 * @param n_files The number of log files.
 * @param file_size The log file size in bytes.
 * @param space_id The space id of the file space which contains the log files of this group.
 */
void log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id);

/**
 * @brief Completes an i/o to a log file.
 *
 * @param group The log group.
 */
void log_io_complete(log_group_t *group);

/**
 * @brief Checks that the log has been written to the log file up to the last log entry written by the transaction.
 * If there is a flush running, it waits and checks if the flush flushed enough.
 * If not, starts a new flush.
 *
 * @param lsn The log sequence number up to which the log should be checked.
 */
void log_check_flush(lsn_t lsn);

/**
 * @brief Sets the global variable log_fsp_current_free_limit.
 * Also makes a checkpoint, so that we know that the limit has been written to a log checkpoint field on disk.
 *
 * @param limit The limit to set.
 */
void log_fsp_current_free_limit_set_and_checkpoint(ulint limit);

/**
 * @brief Calculates where in log files we find a specified lsn.
 *
 * @param log_file_offset[out] Offset in that file (including the header).
 * @param first_header_lsn First log file start lsn.
 * @param lsn Lsn whose position to determine.
 * @param n_log_files Total number of log files.
 * @param log_file_size Log file size (including the header).
 * @return Log file number.
 */
ulint log_calc_where_lsn_is(off_t *log_file_offset, lsn_t first_header_lsn, lsn_t lsn, ulint n_log_files, off_t log_file_size);

/**
 * @brief Opens the log for log_write_low.
 * The log must be closed with log_close and released with log_release.
 *
 * @param len Length of data to be catenated.
 * @return Start lsn of the log record.
 */
lsn_t log_reserve_and_open(ulint len);

/**
 * @brief Writes to the log the string given.
 * It is assumed that the caller holds the log mutex.
 *
 * @param str String to write.
 * @param str_len Length of the string.
 */
void log_write_low(byte *str, ulint str_len);

/**
 * @brief Closes the log.
 *
 * @param recovery Recovery flag.
 * @return Lsn.
 */
lsn_t log_close(ib_recovery_t recovery);

/**
 * @brief Gets the current lsn.
 *
 * @return Current lsn.
 */
inline lsn_t log_get_lsn();

/**
 * @brief Initializes the log.
 */
void innobase_log_init();

/**
 * @brief Inits a log group to the log system.
 *
 * @param id The group id.
 * @param n_files The number of log files.
 * @param file_size The log file size in bytes.
 * @param space_id The space id of the file space which contains the log files of this group.
 */
void log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id);

/**
 * @brief Completes an i/o to a log file.
 *
 * @param group The log group.
 */
void log_io_complete(log_group_t *group);

/**
 * @brief Checks that the log has been written to the log
 * file up to the last log entry written by the transaction. If there is
 * a flush running, it waits and checks if the flush flushed enough.
 * If not, starts a new flush.
 *
 * @param lsn The log sequence number up to which the log should be
 * written. Use IB_UINT64_T_MAX if not specified.
 * 
 * @param wait The wait option: LOG_NO_WAIT, LOG_WAIT_ONE_GROUP,
 * or LOG_WAIT_ALL_GROUPS.
 * 
 * @param flush_to_disk True if we want the written log also to be flushed to disk.
 */
void log_write_up_to(lsn_t lsn, ulint wait, bool flush_to_disk);

/**
 * @brief Does a synchronous flush of the log buffer to disk.
 */
void log_buffer_flush_to_disk(void);

/**
 * @brief Writes the log buffer to the log file and if 'flush' is set
 * it forces a flush of the log file as well. This is meant to be
 * called from background master thread only as it does not wait
 * for the write (+ possible flush) to finish.
 *
 * @param flush True if the logs should be flushed to disk.
 */
void log_buffer_sync_in_background(bool flush);

/**
 * @brief Advances the smallest lsn for which there are unflushed
 * dirty blocks in the buffer pool and also may make a new checkpoint.
 * Note: This function may only be called if the calling thread
 * owns no synchronization objects!
 *
 * @param new_oldest Try to advance oldest_modified_lsn at least to this lsn.
 * @param sync True if synchronous operation is desired.
 * @return False if there was a flush batch of the same type running,
 * which means that we could not start this flush batch.
 */
bool log_preflush_pool_modified_pages(lsn_t new_oldest, bool sync);

/**
 * @brief Makes a checkpoint.
 * Note: This function does not flush dirty blocks from the buffer pool,
 * it only checks the lsn of the oldest modification in the pool and
 * writes information about the lsn in log files.
 *
 * @param sync True if synchronous operation is desired.
 * @param write_always The function normally checks if the new
 * checkpoint would have a greater lsn than the previous one:
 * if not, then no physical write is done. By setting this parameter
 * true, a physical write will always be made to log files.
 * @return True if success, false if a checkpoint write was already running.
 */
bool log_checkpoint(bool sync, bool write_always);

/**
 * @brief Makes a checkpoint at a given lsn or later.
 *
 * @param lsn Make a checkpoint at this or a later lsn. Use IB_UINT64_T_MAX
 * to make a checkpoint at the latest lsn.
 * @param write_always The function normally checks if the new checkpoint would
 * have a greater lsn than the previous one: if not, then no physical write is done.
 * By setting this parameter true, a physical write will always be made to log files.
 */
void log_make_checkpoint_at(lsn_t lsn, bool write_always);

/**
 * @brief Empties the logs and marks files at shutdown.
 *
 * @param recovery The recovery flag.
 * @param shutdown The shutdown flag.
 */
void logs_empty_and_mark_files_at_shutdown(ib_recovery_t recovery, ib_shutdown_t shutdown);

/**
 * @brief Reads a checkpoint info from a log group header to log_sys->checkpoint_buf.
 *
 * @param group The log group.
 * @param field The field indicating the checkpoint (LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2).
 */
void log_group_read_checkpoint_info(log_group_t *group, ulint field);

/**
 * @brief Gets information from a checkpoint about a log group.
 *
 * @param buf The buffer containing checkpoint info.
 * @param n The nth slot.
 * @param file_no The archived file number (output parameter).
 * @param offset The archived file offset (output parameter).
 */
void log_checkpoint_get_nth_group_info(const byte *buf, ulint n, ulint *file_no, ulint *offset);

/** Writes checkpoint info to groups. */
void log_groups_write_checkpoint_info(void);

/** Checks that there is enough free space in the log to start a new query step.
Flushes the log buffer or makes a new checkpoint if necessary. NOTE: this
function may only be called if the calling thread owns no synchronization
objects! */
void log_check_margins();

/**
 * Reads a specified log segment to a buffer.
 *
 * @param type The type of log segment to read.
 * @param buf The buffer where the log segment will be read into.
 * @param group The log group from which to read the log segment.
 * @param start_lsn The start lsn of the log segment to read.
 * @param end_lsn The end lsn of the log segment to read.
 */
void log_group_read_log_seg(ulint type, byte *buf, log_group_t *group, lsn_t start_lsn, lsn_t end_lsn);

/**
 * Writes a buffer to a log file group.
 *
 * @param group The log group to write to.
 * @param buf The buffer containing the data to be written.
 * @param len The length of the buffer. Must be divisible by IB_FILE_BLOCK_SIZE.
 * @param start_lsn The start lsn of the buffer. Must be divisible by IB_FILE_BLOCK_SIZE.
 * @param new_data_offset The start offset of new data in the buffer. This
 * parameter is used to decide if we have to write a new log file header.
 */
void log_group_write_buf(log_group_t *group, byte *buf, ulint len, lsn_t start_lsn, ulint new_data_offset);

/**
 * Sets the field values in a log group to correspond to a given lsn.
 * For this function to work, the values must already be correctly initialized
 * to correspond to some lsn, for instance, a checkpoint lsn.
 *
 * @param group The log group to set the field values for.
 * @param lsn The lsn for which the values should be set.
 */
void log_group_set_fields(log_group_t *group, lsn_t lsn);

/** Calculates the data capacity of a log group, when the log file headers are
not included.
@param group  Log group
@return	capacity in bytes */
ulint log_group_get_capacity(const log_group_t *group);

/**
 * Sets the global variable log_fsp_current_free_limit.
 * Also makes a checkpoint, so that we know that the limit has been written
 * to a log checkpoint field on disk.
 *
 * @param limit The limit to set.
 */
void log_fsp_current_free_limit_set_and_checkpoint(ulint limit);

/**
 * @brief Prints information of the log.
 *
 * @param ib_stream The stream where to print.
 */
void log_print(ib_stream_t ib_stream);

/**
 * @brief Peeks the current lsn.
 *
 * @param lsn The output parameter where the current lsn will be stored if the function returns true.
 * @return True if success, false if could not get the log system mutex.
 */
bool log_peek_lsn(lsn_t *lsn);

/** Refreshes the statistics used to print per-second averages. */
void log_refresh_stats();

/** Shutdown log system but doesn't release all the memory. */
void log_shutdown();

/** Reset the variables. */
void log_var_init();

/** Free the log system data structures. */
void log_mem_free();

