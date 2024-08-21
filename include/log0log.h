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

/** @file include/log0log.h
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "api0api.h"
#include "log0types.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "os0file.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "ut0lst.h"

struct Log;
struct log_group_t;

extern Log *log_sys;

struct Log {

  /**
   * Constructor */
  Log() noexcept;

  /**
   * Destructor */
  ~Log() noexcept;

  /**
   * Acquire the log mutex.
   */
  void acquire() const noexcept {
    ut_ad(!mutex_own(&m_mutex));
    mutex_enter(&m_mutex);
  }

  /**
   * Releases the log mutex.
   */
  void release() const noexcept {
    ut_ad(mutex_own(&m_mutex));
    mutex_exit(&m_mutex);
  }

  /**
   * Gets the flush bit of a log block.
   *
   * @param log_block The log block.
   * @return True if this block was the first to be written in a log flush, false otherwise.
   */
  [[nodiscard]] static bool block_get_flush_bit(const byte *log_block) noexcept {
    return (LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO)) != 0;
  }

  /**
   * Sets the flush bit of a log block.
   *
   * @param log_block The log block.
   * @param val The value to set.
   */
  static void block_set_flush_bit(byte *log_block, bool val) noexcept {
    auto field = mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);

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
  [[nodiscard]] static ulint block_get_hdr_no(const byte *log_block) noexcept {
    return ~LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);
  }

  /**
  * Sets the log block number stored in the header.
  * NOTE: This must be set before the flush bit!
  *
  * @param log_block The log block.
  * @param n The log block number: must be > 0 and < LOG_BLOCK_FLUSH_BIT_MASK.
  */
 static void block_set_hdr_no(byte *log_block, ulint n) noexcept{
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
 [[nodiscard]] static ulint block_get_data_len(const byte *log_block) noexcept {
   return mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN);
 }
 
 /**
  * Sets the data length of a log block.
  *
  * @param log_block The log block.
  * @param len The data length.
  */
 static void block_set_data_len(byte *log_block, ulint len) noexcept {
   mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
 }
 
 /**
  * Gets the offset of the first mtr log record group in a log block.
  *
  * @param log_block The log block.
  * 
  * @return The first mtr log record group byte offset from the block start, 0 if none.
  */
 [[nodiscard]] static ulint block_get_first_rec_group(const byte *log_block) noexcept {
   return mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP);
 }
 
 /**
  * Sets the offset of the first mtr log record group in a log block.
  *
  * @param log_block The log block.
  * @param offset The offset, 0 if none.
  */
 static void block_set_first_rec_group(byte *log_block, ulint offset) noexcept {
   mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
 }
 
 /**
  * Gets the checkpoint number field of a log block.
  *
  * @param log_block The log block.
  * @return The checkpoint number (4 lowest bytes).
  */
 [[nodiscard]] static ulint block_get_checkpoint_no(const byte *log_block) noexcept {
   return mach_read_from_4(log_block + LOG_BLOCK_CHECKPOINT_NO);
 }
 
 /**
  * Sets the checkpoint number field of a log block.
  *
  * @param log_block The log block.
  * @param no The checkpoint number.
  */
 static void block_set_checkpoint_no(byte *log_block, uint64_t no) noexcept {
   mach_write_to_4(log_block + LOG_BLOCK_CHECKPOINT_NO, (ulint)no);
 }
 
 /**
  * Converts a lsn to a log block number.
  *
  * @param lsn The lsn of a byte within the block.
  * @return The log block number, it is > 0 and <= 1G.
  */
 [[nodiscard]] static ulint block_convert_lsn_to_no(lsn_t lsn) noexcept {
   return ((ulint)(lsn / IB_FILE_BLOCK_SIZE) & 0x3FFFFFFFUL) + 1;
 }
 
 /**
  * Calculates the checksum for a log block.
  *
  * @param block The log block.
  * @return The checksum.
  */
 [[nodiscard]] static uint32_t block_calc_checksum(const byte *block) noexcept {
   uint32_t sh = 0;   // Shift value.
   uint32_t sum = 1;  // Checksum value.
 
   for (ulint i = 0; i < IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE; i++) {
     auto b = uint32_t(block[i]);
 
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
 [[nodiscard]] static uint32_t block_get_checksum(const byte *log_block) noexcept {
   return mach_read_from_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM);
 }
 
 /**
  * Sets a log block checksum field value.
  *
  * @param log_block The log block.
  * @param checksum The checksum.
  */
 static void block_set_checksum(byte *log_block, ulint checksum) noexcept {
   mach_write_to_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, checksum);
 }
 
 /**
  * Initializes a log block in the log buffer.
  *
  * @param log_block Pointer to the log buffer.
  * @param lsn LSN within the log block.
  */
 void block_init(byte *log_block, lsn_t lsn) const noexcept {
   ut_ad(mutex_own(&m_mutex));
 
   const auto no = block_convert_lsn_to_no(lsn);
 
   block_set_hdr_no(log_block, no);
 
   block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
   block_set_first_rec_group(log_block, 0);
 }
 
 /**
  * Initializes a log block in the log buffer in the old format, where there was no checksum yet.
  *
  * @param log_block Pointer to the log buffer.
  * @param lsn LSN within the log block.
  */
 void block_init_in_old_format(byte *log_block, lsn_t lsn) const noexcept {
   ut_ad(mutex_own(&m_mutex));
 
   const auto no = block_convert_lsn_to_no(lsn);
 
   block_set_hdr_no(log_block, no);
   mach_write_to_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, no);
   block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
   block_set_first_rec_group(log_block, 0);
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
 [[nodiscard]] lsn_t reserve_and_write_fast(const void *str, ulint len, lsn_t *start_lsn) noexcept {
   auto data_len = len + m_buf_free % IB_FILE_BLOCK_SIZE;
 
   if (data_len >= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
     /* The string does not fit within the current log block
     or the log block would become full */
     return 0;
   }
 
   *start_lsn = m_lsn;
 
   memcpy(m_buf + m_buf_free, str, len);
 
   block_set_data_len(static_cast<byte *>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE)), data_len);
 
   m_buf_free += len;
 
   ut_ad(m_buf_free <= m_buf_size);
 
   m_lsn += len;
 
   return m_lsn;
 }
 
 /** Gets the current lsn.
  * 
  * @return	current lsn
  */
 [[nodiscard]] lsn_t get_lsn() const noexcept {
   acquire();
 
   auto lsn = m_lsn;
 
   release();
 
   return lsn;
 }
 
 /**
  * Gets the log group capacity.
  * It is OK to read the value without holding mutex because it is constant.
  *
  * @return The log group capacity.
  */
 [[nodiscard]] ulint get_capacity() const noexcept{
   return m_log_group_capacity;
 }
 
 /**
  * Checks if there is a need for a log buffer flush or a new checkpoint, and does this if yes.
  * Any database operation should call this when it has modified more than about 4 pages.
  * NOTE that this function may only be called when the OS thread owns no synchronization objects except the dictionary mutex.
  */
 void free_check() noexcept;
 
 /**
  * @brief Completes an i/o to a log file.
  *
  * @param group The log group.
  */
 void io_complete(log_group_t *group) noexcept;
 
 /**
  * @brief Checks that the log has been written to the log file up to the last log entry written by the transaction.
  * If there is a flush running, it waits and checks if the flush flushed enough.
  * If not, starts a new flush.
  *
  * @param lsn The log sequence number up to which the log should be checked.
  */
 void check_flush(lsn_t lsn) noexcept;
 
 /**
  * @brief Sets the global variable log_fsp_current_free_limit.
  * Also makes a checkpoint, so that we know that the limit has been written to a log checkpoint field on disk.
  *
  * @param limit The limit to set.
  */
 void fsp_current_free_limit_set_and_checkpoint(ulint limit) noexcept;
 
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
 [[nodiscard]] ulint calc_where_lsn_is(
    off_t *log_file_offset,
    lsn_t first_header_lsn,
    lsn_t lsn,
    ulint n_log_files,
    off_t log_file_size) noexcept;
 
 /**
  * @brief Opens the log for log_write_low.
  * The log must be closed with log_close and released with log_release.
  *
  * @param len Length of data to be catenated.
  * @return Start lsn of the log record.
  */
 [[nodiscard]] lsn_t reserve_and_open(ulint len) noexcept;
 
 /**
  * @brief Writes to the log the string given.
  * It is assumed that the caller holds the log mutex.
  *
  * @param str String to write.
  * @param str_len Length of the string.
  */
 void write_low(byte *str, ulint str_len) noexcept;
 
 /**
  * @brief Closes the log.
  *
  * @param recovery Recovery flag.
  * @return Lsn.
  */
 [[nodiscard]] lsn_t close(ib_recovery_t recovery) noexcept;
 
 /**
  * @brief Initializes the log.
  */
 void innobase_log_init() noexcept;
 
 /**
  * @brief Inits a log group to the log system.
  *
  * @param id The group id.
  * @param n_files The number of log files.
  * @param file_size The log file size in bytes.
  * @param space_id The space id of the file space which contains the log files of this group.
  */
 void group_init(ulint id, ulint n_files, off_t file_size, space_id_t space_id) noexcept;
 
 /**
  * @brief Checks that the log has been written to the log
  * file up to the last log entry written by the transaction. If there is
  * a flush running, it waits and checks if the flush flushed enough.
  * If not, starts a new flush.
  *
  * @param lsn The log sequence number up to which the log should be
  *   written. Use IB_UINT64_T_MAX if not specified.
  * 
  * @param wait The wait option: LOG_NO_WAIT, LOG_WAIT_ONE_GROUP,
  *   or LOG_WAIT_ALL_GROUPS.
  * 
  * @param flush_to_disk True if we want the written log also to be flushed to disk.
  */
 void write_up_to(lsn_t lsn, ulint wait, bool flush_to_disk) noexcept;
 
 /**
  * @brief Does a synchronous flush of the log buffer to disk.
  */
 void buffer_flush_to_disk() noexcept;
 
 /**
  * @brief Writes the log buffer to the log file and if 'flush' is set
  * it forces a flush of the log file as well. This is meant to be
  * called from background master thread only as it does not wait
  * for the write (+ possible flush) to finish.
  *
  * @param flush True if the logs should be flushed to disk.
  */
 void buffer_sync_in_background(bool flush) noexcept;
 
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
 [[nodiscard]] bool preflush_pool_modified_pages(lsn_t new_oldest, bool sync) noexcept;
 
 /**
  * @brief Makes a checkpoint.
  * Note: This function does not flush dirty blocks from the buffer pool,
  * it only checks the lsn of the oldest modification in the pool and
  * writes information about the lsn in log files.
  *
  * @param sync True if synchronous operation is desired.
  * @param write_always The function normally checks if the new
  *   checkpoint would have a greater lsn than the previous one:
  *   if not, then no physical write is done. By setting this parameter
  *   true, a physical write will always be made to log files.
  * 
  * @return True if success, false if a checkpoint write was already running.
  */
 [[nodiscard]] bool checkpoint(bool sync, bool write_always) noexcept;
 
 /**
  * @brief Makes a checkpoint at a given lsn or later.
  *
  * @param lsn Make a checkpoint at this or a later lsn. Use IB_UINT64_T_MAX
  * to make a checkpoint at the latest lsn.
  * @param write_always The function normally checks if the new checkpoint would
  * have a greater lsn than the previous one: if not, then no physical write is done.
  * By setting this parameter true, a physical write will always be made to log files.
  */
 void make_checkpoint_at(lsn_t lsn, bool write_always) noexcept;
 
 /**
  * @brief Empties the logs and marks files at shutdown.
  *
  * @param recovery The recovery flag.
  * @param shutdown The shutdown flag.
  */
 static void empty_and_mark_files_at_shutdown(ib_recovery_t recovery, ib_shutdown_t shutdown) noexcept;
 
 /**
  * @brief Reads a checkpoint info from a log group header to checkpoint_buf.
  *
  * @param group The log group.
  * @param field The field indicating the checkpoint (LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2).
  */
 void group_read_checkpoint_info(log_group_t *group, ulint field) noexcept;
 
 /**
  * @brief Gets information from a checkpoint about a log group.
  *
  * @param buf The buffer containing checkpoint info.
  * @param n The nth slot.
  * @param file_no The archived file number (output parameter).
  * @param offset The archived file offset (output parameter).
  */
 void checkpoint_get_nth_group_info(const byte *buf, ulint n, ulint *file_no, ulint *offset) noexcept;
 
 /**
  * Writes checkpoint info to groups.
  */
 void groups_write_checkpoint_info() noexcept;
 
 /**
  * Checks that there is enough free space in the log to start a new query step.
  * Flushes the log buffer or makes a new checkpoint if necessary. NOTE: this
  * function may only be called if the calling thread owns no synchronization
  * objects! */
 void check_margins() noexcept;
 
 /**
  * Reads a specified log segment to a buffer.
  *
  * @param type The type of log segment to read.
  * @param buf The buffer where the log segment will be read into.
  * @param group The log group from which to read the log segment.
  * @param start_lsn The start lsn of the log segment to read.
  * @param end_lsn The end lsn of the log segment to read.
  */
 void group_read_log_seg(ulint type, byte *buf, log_group_t *group, lsn_t start_lsn, lsn_t end_lsn) noexcept;
 
 /**
  * Writes a buffer to a log file group.
  *
  * @param group The log group to write to.
  * @param buf The buffer containing the data to be written.
  * @param len The length of the buffer. Must be divisible by IB_FILE_BLOCK_SIZE.
  * @param start_lsn The start lsn of the buffer. Must be divisible by IB_FILE_BLOCK_SIZE.
  * @param new_data_offset The start offset of new data in the buffer. This
  *   parameter is used to decide if we have to write a new log file header.
  */
 void group_write_buf(log_group_t *group, byte *buf, ulint len, lsn_t start_lsn, ulint new_data_offset) noexcept;
 
 /**
  * Sets the field values in a log group to correspond to a given lsn.
  * For this function to work, the values must already be correctly initialized
  * to correspond to some lsn, for instance, a checkpoint lsn.
  *
  * @param group The log group to set the field values for.
  * @param lsn The lsn for which the values should be set.
  */
 void group_set_fields(log_group_t *group, lsn_t lsn) noexcept;
 
 /**
  * Calculates the data capacity of a log group, when the log file headers are
  * not included.
  * 
  * @param group  Log group
  * 
  * @return	capacity in bytes
  */
 [[nodiscard]] ulint group_get_capacity(const log_group_t *group) noexcept;
 
 /**
  * @brief Prints information of the log.
  */
 void print() noexcept;
 
 /**
  * @brief Peeks the current lsn.
  *
  * @param lsn The output parameter where the current lsn will be stored if the function returns true.
  * @return True if success, false if could not get the log system mutex.
  */
 [[nodiscard]] bool peek_lsn(lsn_t *lsn) noexcept;
 
 /**
  * Refreshes the statistics used to print per-second averages.
  */
 void refresh_stats() noexcept;
 
 /**
  * Shutdown log system but doesn't release all the memory.
  */
 void shutdown() noexcept;
 
 /**
  * Reset the variables.
  */
 static void var_init() noexcept;
 
 /**
  * Create the log instance.
  * 
  * @return The log instance.
  */
 [[nodiscard]] static Log* create() noexcept;

 /**
  *  Free the log system data structures.
  * 
  * @param[in,own] log Log instance to destroy.
  */
 static void destroy(Log *&log) noexcept;

private:
  /**
   * Returns the oldest modified block LSN in the pool, or m_lsn if none exists.
   * 
   * @return LSN of oldest modification
   */
  [[nodiscard]] lsn_t buf_pool_get_oldest_modification() noexcept;

  /**
   * Calculates the offset within a log group, when the log file headers are not included.
   *
   * @param offset Real offset within the log group
   * @param group Log group
   * 
   * @return Size offset (<= offset)
   */
  [[nodiscard]] ulint group_calc_size_offset(ulint offset, const log_group_t *group) noexcept;

  /**
   * Calculates the offset within a log group, when the log file headers are included.
   * 
   * @param offset Size offset within the log group
   * @param group Log group
   * @return Real offset (>= offset)
   */
  [[nodiscard]] uint64_t group_calc_real_offset(ulint offset, const log_group_t *group) noexcept;

  /**
   * Calculates the offset of an lsn within a log group.
   *
   * @param lsn   LSN, must be within 4 GB of group->lsn
   * @param group Log group
   * @return      Offset within the log group
   */
  [[nodiscard]] uint64_t group_calc_lsn_offset(lsn_t lsn, const log_group_t *group) noexcept;

   /**
    * Calculates the recommended highest values for lsn - last_checkpoint_lsn,
    * lsn - buf_get_oldest_modification(), and lsn - max_archive_lsn_age.
    * 
    * @return error value false if the smallest log group is too small to
    *  accommodate the number of OS threads in the database server */
   [[nodiscard]] bool calc_max_ages() noexcept;

   /**
    * Does the unlockings needed in flush I/O completion.
    *
    * @param code Any ORed combination of LOG_UNLOCK_FLUSH_LOCK and LOG_UNLOCK_NONE_FLUSHED_LOCK
    */
  void flush_do_unlocks(ulint code) noexcept;

  /**
   * Checks if a flush is completed for a log group and performs the completion routine if yes.
   * 
   * @param group Log group
   * @return LOG_UNLOCK_NONE_FLUSHED_LOCK if flush is completed, 0 otherwise
   */
  [[nodiscard]] ulint group_check_flush_completion(log_group_t *group) noexcept;

  /**
   * Completes a checkpoint.
   */
  void complete_checkpoint() noexcept;

  /**
   * Completes an asynchronous checkpoint info write i/o to a log file.
   */
  void io_complete_checkpoint() noexcept;

  /**
   * Checks if a flush is completed and does the completion routine if yes.
   * 
   * @return	LOG_UNLOCK_FLUSH_LOCK or 0
   */
  [[nodiscard]] ulint sys_check_flush_completion() noexcept;

  /**
   * Writes a log file header to a log file space.
   *
   * @param group The log group
   * @param nth_file The header to the nth file in the log file space
   * @param start_lsn The log file data starts at this LSN
   */
  void group_file_header_flush(log_group_t *group, ulint nth_file, lsn_t start_lsn) noexcept;

  /**
   * Stores a 4-byte checksum to the trailer checksum field of a log block
   * before writing it to a log file. This checksum is used in recovery to
   * check the consistency of a log block.
   *
   * @param block Pointer to a log block
   */
  void block_store_checksum(byte *block) noexcept; 

  /**
   * Tries to establish a big enough margin of free space in
   * the log buffer, such that a new log entry can be catenated
   * without an immediate need for a flush.
   */
  void flush_margin() noexcept;

  /**
   * Writes info to a checkpoint about a log group.
   *
   * @param buf Buffer for checkpoint info
   * @param n Nth slot
   * @param file_no Archived file number
   * @param offset Archived file offset
   */
  void checkpoint_set_nth_group_info(byte *buf, ulint n, ulint file_no, ulint offset) noexcept;

  /**
   * Writes the checkpoint info to a log group header.
   *
   * @param[in,out] group Log group.
   */
  void group_checkpoint(log_group_t *group) noexcept; 

  /**
   * Tries to establish a big enough margin of free space in the log groups, such
   * that a new log entry can be catenated without an immediate need for a checkpoint.
   * NOTE: this function may only be called if the calling thread owns no synchronization objects!
   */
  void checkpoint_margin() noexcept;

   /**
    * Closes a log group.
    * 
    * @param[in,own] group            Log group to close.
    */
   void group_close(log_group_t *group) noexcept;

  
public:
  /**
   * Padding to prevent other memory update hotspots from residing on
   * the same memory cache line
   */
  byte m_pad[64];

  /** Log sequence number */
  lsn_t m_lsn{};

  /** First free offset within the log buffer */
  ulint m_buf_free{};

  /** Mutex protecting the log */
  mutable mutex_t m_mutex{};

  /* Unaligned log buffer */
  byte *m_buf_ptr{};

  /** Log buffer */
  byte *m_buf{};

  /** Log buffer size in bytes */
  ulint m_buf_size{};

  /* recommended maximum value of buf_free, after which the buffer is flushed */
  ulint m_max_buf_free{};

#ifdef UNIV_DEBUG

  /** value of buf free when log was last time opened; only in the debug version */
  ulint m_old_buf_free{};

  /** value of lsn when log was last time opened; only in the debug version */
  lsn_t m_old_lsn{};
#endif /* UNIV_DEBUG */

  /** This is set to true when there may be need to flush the log buffer,
  or preflush buffer pool pages, or make a checkpoint; this MUST be true
  when lsn - last_checkpoint_lsn > max_checkpoint_age; this flag is
  peeked at by log_free_check(), which does not reserve the log mutex */
  bool m_check_flush_or_checkpoint{};

  /** Log groups */
  UT_LIST_BASE_NODE_T_EXTERN(log_group_t, log_groups) m_log_groups{};

  /** The fields involved in the log buffer flush @{ */

  /** First offset in the log buffer where the byte content may not exist
  written to file, e.g., the start offset of a log record catenated later;
  this is advanced when a flush operation is completed to all the log groups */
  ulint m_buf_next_to_write{};

  /** First log sequence number not yet written to any log group; for this
  to be advanced, it is enough that the write i/o has been completed for
  any one log group */
  lsn_t m_written_to_some_lsn{};

  /** First log sequence number not yet written to some log group; for this
  to be advanced, it is enough that the write i/o has been completed for all
  log groups.  Note that since InnoDB currently has only one log group therefore
  this value is redundant. Also it is possible that this value falls behind
  the flushed_to_disk_lsn transiently.  It is appropriate to use either
  flushed_to_disk_lsn or write_lsn which are always up-to-date and accurate. */
  lsn_t m_written_to_all_lsn{};

  /** End lsn for the current running write */
  lsn_t m_write_lsn{};

  /** THe data in buffer has been written up to this offset when the current
  write ends: this field will then be copied to buf_next_to_write */
  ulint m_write_end_offset{};

  /** End lsn for the current running write + flush operation */
  lsn_t m_current_flush_lsn{};

  /** How far we have written the log AND flushed to disk */
  lsn_t m_flushed_to_disk_lsn{};

  /** Number of currently pending flushes or writes */
  ulint m_n_pending_writes{};

  /* We separate the write of the log file and the actual fsync()
  or other method to flush it to disk. The names below shhould really
  be 'flush_or_write'! */

  /** This event is in the reset state when a flush or a write is
  running; a thread should wait for this without owning the log
  mutex, but NOTE that to set or reset this event, the thread MUST
  own the log mutex! */
  Cond_var* m_no_flush_event{};

  /** During a flush, this is first false and becomes true when one
  log group has been written or flushed */
  bool m_one_flushed{};

  /** This event is reset when the flush or write has not yet completed
  for any log group; e.g., this means that a transaction has been
  committed when this is set; a thread should wait for this without owning
  the log mutex, but NOTE that to set or reset this event, the thread
  MUST own the log mutex! */
  Cond_var* m_one_flushed_event{};

  /** Number of log i/os initiated thus far */
  ulint m_n_log_ios{};

  /** Number of log i/o's at the previous printout */
  ulint m_n_log_ios_old{};

  /** When log_print was last time called */
  time_t m_last_printout_time{};
  /* @} */

  /** Fields involved in checkpoints @{ */

  /** Capacity of the log group; if the checkpoint age exceeds this,
  it is a serious error because it is possible we will then overwrite
  log and spoil crash recovery */
  ulint m_log_group_capacity{};

  /** When this recommended value for lsn - srv_buf_pool->get_oldest_modification()
  s exceeded, we start an asynchronous preflush of pool pages */
  ulint m_max_modified_age_async{};

  /** When this recommended value for lsn - srv_buf_pool->get_oldest_modification()
  is exceeded, we start a synchronous preflush of pool pages */
  ulint m_max_modified_age_sync{};

  /** Administrator-specified checkpoint interval in terms of log growth in
  bytes; the interval actually used by the database can be smaller */
  ulint m_adm_checkpoint_interval{};

  /** When this checkpoint age is exceeded we start an asynchronous writing
  of a new checkpoint */
  ulint m_max_checkpoint_age_async{};

  /** This is the maximum allowed value for lsn - last_checkpoint_lsn when a new query step is started */
  ulint m_max_checkpoint_age{};

  /** Next checkpoint number */
  lsn_t m_next_checkpoint_no{};

  /** Latest checkpoint lsn */
  lsn_t m_last_checkpoint_lsn{};

  /** Next checkpoint lsn */
  lsn_t m_next_checkpoint_lsn{};

  /** Number of currently pending checkpoint writes */
  ulint m_n_pending_checkpoint_writes{};

  /** This latch is x-locked when a checkpoint write is running; a thread should wait for this without owning the log mutex */
  rw_lock_t m_checkpoint_lock{};

  /** Unaligned checkpoint header */
  byte *m_checkpoint_buf_ptr{};

  /** Checkpoint header is read to this buffer */
  byte *m_checkpoint_buf{};
};
   
using log_t = Log;