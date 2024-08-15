/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file include/mtr0log.h
Mini-transaction logging routines

Created 12/7/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "mtr0mtr.h"

/* Insert, update, and maybe other functions may use this value to define an
extra mlog buffer size for variable size data */
constexpr ulint MLOG_BUF_MARGIN = 256;

/**
 * @brief Writes 1 - 4 bytes to a file page buffered in the buffer pool.
 *        Writes the corresponding log record to the mini-transaction log.
 * 
 * @param ptr Pointer where to write.
 * @param val Value to write.
 * @param type MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES.
 * @param mtr Mini-transaction handle.
 */
void mlog_write_ulint(byte *ptr, ulint val, mlog_type_t type, mtr_t *mtr);

/**
 * @brief Writes 8 bytes to a file page buffered in the buffer pool.
 *        Writes the corresponding log record to the mini-transaction log.
 * 
 * @param ptr Pointer where to write.
 * @param val Value to write.
 * @param mtr Mini-transaction handle.
 */
void mlog_write_uint64(byte *ptr, uint64_t val, mtr_t *mtr);

/**
 * @brief Writes a string to a file page buffered in the buffer pool.
 *        Writes the corresponding log record to the mini-transaction log.
 * 
 * @param ptr Pointer where to write.
 * @param str String to write.
 * @param len String length.
 * @param mtr Mini-transaction handle.
 */
void mlog_write_string(byte *ptr, const byte *str, ulint len, mtr_t *mtr);

/**
 * @brief Logs a write of a string to a file page buffered in the buffer pool.
 *        Writes the corresponding log record to the mini-transaction log.
 * 
 * @param ptr Pointer written to.
 * @param len String length.
 * @param mtr Mini-transaction handle.
 */
void mlog_log_string(byte *ptr, ulint len, mtr_t *mtr);

/**
 * @brief Writes initial part of a log record consisting of one-byte item type
 *        and four-byte space and page numbers.
 * 
 * @param ptr Pointer to (inside) a buffer frame holding the file page where modification is made.
 * @param type Log item type: MLOG_1BYTE, ...
 * @param mtr Mini-transaction handle.
 */
void mlog_write_initial_log_record(const byte *ptr, mlog_type_t type, mtr_t *mtr);

/**
 * @brief Catenates n bytes to the mtr log.
 * 
 * @param mtr Mtr.
 * @param str String to write.
 * @param len String length.
 */
void mlog_catenate_string(mtr_t *mtr, const byte *str, ulint len);

/**
 * @brief Writes the initial part of a log record (3..11 bytes).
 *        If the implementation of this function is changed, all
 *        size parameters to mlog_open() should be adjusted accordingly!
 * 
 * @param ptr Pointer to (inside) a buffer frame holding the file page where modification is made.
 * @param type Log item type: MLOG_1BYTE, ...
 * @param log_ptr Pointer to mtr log which has been opened.
 * @param mtr Mini-transaction handle.
 * @return New value of log_ptr.
 */
byte *mlog_write_initial_log_record_fast(const byte *ptr, mlog_type_t type, byte *log_ptr, mtr_t *mtr);

/**
 * @brief Parses an initial log record written by mlog_write_initial_log_record.
 * 
 * @param ptr Buffer.
 * @param end_ptr Buffer end.
 * @param type Log record type: MLOG_1BYTE, ...
 * @param space Space id.
 * @param page_no Page number.
 * @return Parsed record end, nullptr if not a complete record.
 */
byte *mlog_parse_initial_log_record(byte *ptr, byte *end_ptr, mlog_type_t *type, ulint *space, ulint *page_no);

/**
 * @brief Parses a log record written by mlog_write_ulint or mlog_write_uint64.
 * 
 * @param type Log record type: MLOG_1BYTE, ...
 * @param ptr Buffer.
 * @param end_ptr Buffer end.
 * @param page Page where to apply the log record, or nullptr.
 * @return Parsed record end, nullptr if not a complete record.
 */
byte *mlog_parse_nbytes(mlog_type_t type, byte *ptr, byte *end_ptr, byte *page);

/**
 * @brief Parses a log record written by mlog_write_string.
 * 
 * @param ptr Buffer.
 * @param end_ptr Buffer end.
 * @param page Page where to apply the log record, or nullptr.
 * @return Parsed record end, nullptr if not a complete record.
 */
byte *mlog_parse_string(byte *ptr, byte *end_ptr, byte *page);

/**
 *  @brief Opens a buffer for mlog, writes the initial log record
 *    Reserves space for further log entries. The log entry must be
 *    closed with mtr_close().
 * 
 * @param mtr Mtr.
 * @param rec Index record or page.
 * @param index Record descriptor.
 * @param type Log type.
 * @param size Requested buffer size in bytes (if 0, calls mlog_close() and returns nullptr).
 * @return Buffer, nullptr if log mode MTR_LOG_NONE.
 */
byte *mlog_open_and_write_index(mtr_t *mtr, const byte *rec, dict_index_t *index, mlog_type_t type, ulint size);

/**
 * @brief Parses a log record written by mlog_open_and_write_index.
 * 
 * @param ptr Buffer.
 * @param end_ptr Buffer end.
 * @param index Dummy index.
 * @return Parsed record end, nullptr if not a complete record.
 */
byte *mlog_parse_index(byte *ptr, const byte *end_ptr, dict_index_t **index);

/**
 * @brief Opens a buffer to mlog. It must be closed with mlog_close.
 * 
 * @param mtr Mtr.
 * @param size Buffer size in bytes; MUST be smaller than DYN_ARRAY_DATA_SIZE!
 * @return Buffer, nullptr if log mode MTR_LOG_NONE.
 */
inline byte *mlog_open(mtr_t *mtr, ulint size) {
  /**
   * Set modifications flag to true.
   */
  mtr->modifications = true;

  if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
    return nullptr;
  }

  auto mlog = &mtr->log;

  return dyn_array_open(mlog, size);
}

/**
 * @brief Closes a buffer opened to mlog.
 * 
 * @param mtr Mtr.
 * @param ptr Buffer space from ptr up was not used.
 */
inline void mlog_close(mtr_t *mtr, byte *ptr) {
  ut_ad(mtr_get_log_mode(mtr) != MTR_LOG_NONE);

  auto mlog = &mtr->log;

  dyn_array_close(mlog, ptr);
}

/**
 * @brief Catenates 1 - 4 bytes to the mtr log. The value is not compressed.
 * 
 * @param mtr Mtr.
 * @param val Value to write.
 * @param type MLOG_1BYTE, MLOG_2BYTES, or MLOG_4BYTES.
 */
inline void mlog_catenate_ulint(mtr_t *mtr, ulint val, mlog_type_t type) {
  if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
    return;
  }

  auto mlog = &mtr->log;

  /**
   * Assert that the type values are correct.
   */
  static_assert(MLOG_1BYTE == 1, "error MLOG_1BYTE != 1");
  static_assert(MLOG_2BYTES == 2, "error MLOG_2BYTES != 2");
  static_assert(MLOG_4BYTES == 4, "error MLOG_4BYTES != 4");
  static_assert(MLOG_8BYTES == 8, "error MLOG_8BYTES != 8");

  /**
   * Push the type to the mlog.
   */
  auto ptr = reinterpret_cast<byte *>(dyn_array_push(mlog, type));

  /**
   * Write the value to the mlog based on the type.
   */
  if (type == MLOG_4BYTES) {
    mach_write_to_4(ptr, val);
  } else if (type == MLOG_2BYTES) {
    mach_write_to_2(ptr, val);
  } else {
    ut_ad(type == MLOG_1BYTE);
    mach_write_to_1(ptr, val);
  }
}

/**
 * @brief Catenates a compressed ulint to mlog.
 * 
 * @param mtr Mtr.
 * @param val Value to write.
 */
inline void mlog_catenate_ulint_compressed(mtr_t *mtr, ulint val) {
  auto log_ptr = mlog_open(mtr, 10);

  /**
   * If no logging is requested, we may return now.
   */
  if (log_ptr == nullptr) {
    return;
  }

  log_ptr += mach_write_compressed(log_ptr, val);

  mlog_close(mtr, log_ptr);
}

/**
 * @brief Catenates a compressed uint64_t to mlog.
 * 
 * @param mtr Mtr.
 * @param val Value to write.
 */
inline void mlog_catenate_uint64_compressed(mtr_t *mtr, uint64_t val) {
  auto log_ptr = mlog_open(mtr, 15);

  /**
   * If no logging is requested, we may return now.
   */
  if (log_ptr == nullptr) {
    return;
  }

  log_ptr += mach_uint64_write_compressed(log_ptr, val);

  mlog_close(mtr, log_ptr);
}

/**
 * @brief Writes a log record about an .ibd file create/delete/rename.
 * 
 * @param type Type of the log record (MLOG_FILE_CREATE, MLOG_FILE_DELETE, or MLOG_FILE_RENAME).
 * @param space_id Space ID, if applicable.
 * @param page_no Page number (not relevant currently).
 * @param log_ptr Pointer to the mtr log which has been opened.
 * @param mtr Mtr.
 * @return New value of log_ptr.
 */
inline byte *mlog_write_initial_log_record_for_file_op(
  mlog_type_t type,
  space_id_t space_id,
  page_no_t page_no,
  byte *log_ptr,
  mtr_t *mtr)
{
  mach_write_to_1(log_ptr, type);

  ++log_ptr;

  /* We write dummy space id and page number */
  log_ptr += mach_write_compressed(log_ptr, space_id);
  log_ptr += mach_write_compressed(log_ptr, page_no);

  mtr->n_log_recs++;

  return log_ptr;
}
