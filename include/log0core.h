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

/** @file include/log0core.h
Database log - core functionality with minimal dependencies

Created for testing and minimal coupling
*******************************************************/

#pragma once

#include "log0types.h"

/** Log_core - Core logging functionality with minimal dependencies
 * This class provides the essential log buffer management and block operations
 * without requiring external subsystems like buffer pool, file system, etc.
 */
struct Log_core {
  /**
   * Constructor - initializes log buffer and basic structures
   */
  Log_core() noexcept;

  /**
   * Destructor - frees allocated memory
   */
  ~Log_core() noexcept;

  /**
   * Fast write to log buffer if there's space within current block
   * @param str String to write
   * @param len Length of string
   * @param start_lsn Output parameter for start LSN
   * @return End LSN if successful, 0 if not enough space
   */
  lsn_t reserve_and_write_fast(const void* str, ulint len, lsn_t* start_lsn) noexcept;

  /**
   * Write to log buffer, handling block boundaries
   * @param str String to write
   * @param str_len Length of string
   */
  void write_low(const byte* str, ulint str_len) noexcept;

  /**
   * Get current LSN
   * @return Current log sequence number
   */
  lsn_t get_lsn() const noexcept;

  /**
   * Get current buffer free position
   * @return Free position in buffer
   */
  ulint get_buf_free() const noexcept;

  /**
   * Get log buffer pointer
   * @return Pointer to log buffer
   */
  const byte* get_buf() const noexcept;

  /**
   * Get log buffer size
   * @return Buffer size in bytes
   */
  ulint get_buf_size() const noexcept;

  /**
   * Check if buffer has space for given length
   * @param len Required length
   * @return True if space available
   */
  bool is_buf_available(ulint len) const noexcept;

  /**
   * Reset log to initial state
   */
  void reset() noexcept;

  // Static block manipulation functions - can be used independently

  /**
   * Initialize a log block
   * @param log_block Block to initialize
   * @param lsn LSN for the block
   */
  static void block_init(byte* log_block, lsn_t lsn) noexcept;

  /**
   * Convert LSN to block number
   * @param lsn Log sequence number
   * @return Block number
   */
  static ulint block_convert_lsn_to_no(lsn_t lsn) noexcept;

  /**
   * Set block header number
   * @param log_block Block to modify
   * @param n Header number
   */
  static void block_set_hdr_no(byte* log_block, ulint n) noexcept;

  /**
   * Get block header number
   * @param log_block Block to read from
   * @return Header number
   */
  static ulint block_get_hdr_no(const byte* log_block) noexcept;

  /**
   * Set block data length
   * @param log_block Block to modify
   * @param len Data length
   */
  static void block_set_data_len(byte* log_block, ulint len) noexcept;

  /**
   * Get block data length
   * @param log_block Block to read from
   * @return Data length
   */
  static ulint block_get_data_len(const byte* log_block) noexcept;

  /**
   * Set first record group offset
   * @param log_block Block to modify
   * @param offset Offset to set
   */
  static void block_set_first_rec_group(byte* log_block, ulint offset) noexcept;

  /**
   * Get first record group offset
   * @param log_block Block to read from
   * @return First record group offset
   */
  static ulint block_get_first_rec_group(const byte* log_block) noexcept;

  /**
   * Calculate block checksum
   * @param block Block to calculate checksum for
   * @return Calculated checksum
   */
  static uint32_t block_calc_checksum(const byte* block) noexcept;

  /**
   * Set block checksum
   * @param log_block Block to modify
   * @param checksum Checksum to set
   */
  static void block_set_checksum(byte* log_block, ulint checksum) noexcept;

  /**
   * Get block checksum
   * @param log_block Block to read from
   * @return Block checksum
   */
  static uint32_t block_get_checksum(const byte* log_block) noexcept;

  /**
   * Store calculated checksum in block
   * @param block Block to update
   */
  void block_store_checksum(byte* block) noexcept;

private:
  /** Log sequence number */
  lsn_t m_lsn{};

  /** First free offset within the log buffer */
  ulint m_buf_free{};

  /** Unaligned log buffer pointer */
  byte* m_buf_ptr{};

  /** Log buffer (aligned) */
  byte* m_buf{};

  /** Log buffer size in bytes */
  ulint m_buf_size{};

  /** Maximum recommended free space before flush */
  ulint m_max_buf_free{};
};
