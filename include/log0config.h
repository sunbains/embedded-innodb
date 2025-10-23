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

/** @file include/log0config.h
Log configuration constants - shared between main code and tests

This file contains all log-related constants to avoid duplication
between main implementation and test code.
*******************************************************/

#pragma once

#include <cstddef>
#include "innodb0types.h"
#include "mach0data.h"

/* Values used as flags */
constexpr ulint LOG_FLUSH = 7652559;
constexpr ulint LOG_RECOVER = 98887331;
constexpr ulint LOG_CHECKPOINT = 78656949;

/** Mask used to get the highest bit in the block number. */
constexpr ulint LOG_BLOCK_FLUSH_BIT_MASK = 0x80000000UL;

/** Log data is measured and written in this page size. */
constexpr ulint LOG_DATA_PAGE_SIZE = IB_FILE_BLOCK_SIZE;

/** Assume we are running on an SSD with a 4KB page size. */
constexpr ulint LOG_METADATA_PAGE_SIZE = 4096;

/* Block and file header constants - will be defined after struct definitions */

/* In a Log_vfile.  First file layout.

    LOG_START_LSN ---------------------------+
                                             |
+--------------------------------------------v-------------------------------+
| File header  | Checkpoint 1 | Checkpoint 2 |  data  | data  | data  | data |
+----------------------------------------------------------------------------+

 Second file and onwards layout.
+----------------------------------------------------------------------------+
| File header  |  data     | data   | data   | data   | data  | data  | data |
+----------------------------------------------------------------------------+
*/

/**
* Calculates the checksum for a log block.
*
* @param block The log block.
* @return The checksum.
*/
[[nodiscard]] inline uint32_t log_calc_checksum(const byte *ptr, ulint len) noexcept {
  uint32_t sh = 0;   // Shift value.
  uint32_t sum = 1;  // Checksum value.

  for (ulint i{}; i < len; ++i) {
    auto b = uint32_t(ptr[i]);

    sum &= 0x7FFFFFFFUL;
    sum += b;
    sum += b << sh;
    ++sh;

    if (sh > 24) {
      sh = 0;
    }
  }

  return sum;
}

#pragma pack(push, 1)

struct Log_phy_file_header {
  constexpr static ulint MAGIC = 1441231243;

  void set_ver(uint64_t ver) noexcept { mach_write_to_8(reinterpret_cast<byte *>(&m_ver), ver); }

  void set_lsn(uint64_t lsn) noexcept { mach_write_to_8(reinterpret_cast<byte *>(&m_lsn), lsn); }

  void set_checksum(uint32_t checksum) noexcept { mach_write_to_4(reinterpret_cast<byte *>(&m_checksum), checksum); }

  [[nodiscard]] uint64_t get_ver() noexcept { return mach_read_from_8(reinterpret_cast<byte *>(&m_ver)); }

  [[nodiscard]] ulint get_lsn() noexcept { return mach_read_from_8(reinterpret_cast<byte *>(&m_lsn)); }

  [[nodiscard]] ulint get_checksum() noexcept { return mach_read_from_4(reinterpret_cast<byte *>(&m_checksum)); }

  [[nodiscard]] std::string to_string() const noexcept {
    return std::format("Log_phy_file_header: magic: {}, ver: {}, lsn: {}, checksum: {}", m_magic, m_ver, m_lsn, m_checksum);
  }

  uint32_t m_magic{MAGIC};

  /** Version number */
  uint64_t m_ver{0};

  /** Checkpoint LSN on shutdown. */
  uint64_t m_lsn{0};

  constexpr static ulint PAGE_SIZE = LOG_METADATA_PAGE_SIZE;
  constexpr static ulint HEADER_SIZE = sizeof(m_magic) + sizeof(m_ver) + sizeof(m_lsn);

  /** sizeof (m_checksum) */
  constexpr static ulint TRAILER_SIZE = sizeof(uint32_t);

  byte m_unused[PAGE_SIZE - HEADER_SIZE - TRAILER_SIZE]{0};

  /** Checksum of the entire block. */
  uint32_t m_checksum{0};
};

struct Log_phy_checkpoint {
  constexpr static ulint MAGIC = 1441271243;

  void set_no(uint32_t no) noexcept { mach_write_to_4(reinterpret_cast<byte *>(&m_no), no); }

  void set_lsn(uint64_t lsn) noexcept { mach_write_to_8(reinterpret_cast<byte *>(&m_lsn), lsn); }

  void set_offset(int64_t offset) noexcept { mach_write_to_8(reinterpret_cast<byte *>(&m_offset), offset); }

  void set_checksum(uint32_t checksum) noexcept { mach_write_to_4(reinterpret_cast<byte *>(&m_checksum), checksum); }

  [[nodiscard]] ulint get_no() noexcept { return mach_read_from_4(reinterpret_cast<byte *>(&m_no)); }

  [[nodiscard]] lsn_t get_lsn() noexcept { return mach_read_from_8(reinterpret_cast<byte *>(&m_lsn)); }

  [[nodiscard]] off_t get_offset() noexcept { return mach_read_from_8(reinterpret_cast<byte *>(&m_offset)); }

  [[nodiscard]] ulint get_checksum() noexcept { return mach_read_from_4(reinterpret_cast<byte *>(&m_checksum)); }

  [[nodiscard]] std::string to_string() const noexcept {
    return std::format(
      "Log_phy_checkpoint: magic: {}, no: {}, lsn: {}, offset: {}, checksum: {}", m_magic, m_no, m_lsn, m_offset, m_checksum
    );
  }

  uint32_t m_magic{MAGIC};

  /** Checkpoint number */
  uint32_t m_no{0};

  /** Checkpoint LSN */
  uint64_t m_lsn{0};

  /** Checkpoint offset */
  int64_t m_offset{0};

  constexpr static ulint PAGE_SIZE = LOG_METADATA_PAGE_SIZE;

  constexpr static ulint HEADER_SIZE = sizeof(m_magic) + sizeof(m_no) + sizeof(m_lsn) + sizeof(m_offset);

  /** sizeof (m_checksum) */
  constexpr static ulint TRAILER_SIZE = sizeof(uint32_t);

  /** Unused */
  byte m_unused[PAGE_SIZE - HEADER_SIZE - TRAILER_SIZE]{0};

  /** Checksum of the entire block. */
  uint32_t m_checksum{0};
};

struct Log_phy_block {

  Log_phy_block() = default;

  /** Prepares the log block for writing. */
  void prepare(ulint block_no) noexcept {
    set_block_no(block_no);
    set_first_rec_group(0);
    set_data_len(Log_phy_block::HEADER_SIZE);
  }

  /**
  * Sets the log block number stored in the header.
  * NOTE: This must be set before the flush bit!
  *
  * @param block_no The log block number: must be > 0 and < LOG_BLOCK_FLUSH_BIT_MASK.
  */
  void set_block_no(ulint block_no) noexcept { mach_write_to_4(reinterpret_cast<byte *>(&m_block_no), block_no); }

  void set_data_len(ulint data_len) noexcept { mach_write_to_2(reinterpret_cast<byte *>(&m_data_len), data_len); }

  void set_first_rec_group(ulint first_rec_group) noexcept {
    mach_write_to_2(reinterpret_cast<byte *>(&m_first_rec_group), first_rec_group);
  }

  void set_checkpoint_no(ulint checkpoint_no) noexcept {
    mach_write_to_4(reinterpret_cast<byte *>(&m_checkpoint_no), checkpoint_no);
  }

  [[nodiscard]] uint32_t calc_checksum() const noexcept { return log_calc_checksum(m_data, m_data_len); }

  void set_checksum(uint32_t checksum) noexcept { mach_write_to_4(reinterpret_cast<byte *>(&m_checksum), checksum); }

  void copy(ulint offset, const byte *data, ulint len) noexcept {
    ut_ad(offset + len <= sizeof(m_data));

    memcpy(m_data + offset, data, len);
  }

  [[nodiscard]] byte *data() noexcept { return m_data; }

  [[nodiscard]] const byte *data() const noexcept { return m_data; }

  void clear_data() noexcept { memset(m_data, 0, sizeof(m_data)); }

  /** This doesn't strip off the flush bit.
   *
   * @return The raw block number.
   */
  [[nodiscard]] ulint get_raw_block_no() const noexcept { return mach_read_from_4(reinterpret_cast<const byte *>(&m_block_no)); }

  /**
   * Gets the log block number stored in the header (without the flush bit).
   *
   * @return The log block number stored in the block header.
   */
  [[nodiscard]] ulint get_block_no() const noexcept { return ~LOG_BLOCK_FLUSH_BIT_MASK & get_raw_block_no(); }

  [[nodiscard]] ulint get_data_len() const noexcept { return mach_read_from_2(reinterpret_cast<const byte *>(&m_data_len)); }

  [[nodiscard]] ulint get_first_rec_group() const noexcept {
    return mach_read_from_2(reinterpret_cast<const byte *>(&m_first_rec_group));
  }

  [[nodiscard]] ulint get_checkpoint_no() const noexcept {
    return mach_read_from_4(reinterpret_cast<const byte *>(&m_checkpoint_no));
  }

  [[nodiscard]] ulint get_checksum() const noexcept { return mach_read_from_4(reinterpret_cast<const byte *>(&m_checksum)); }

  /**
   * Gets the flush bit of a log block.
   *
   * @return True if this block was the first to be written in a log flush, false otherwise.
   */
  [[nodiscard]] bool get_flush_bit() const noexcept { return (LOG_BLOCK_FLUSH_BIT_MASK & get_raw_block_no()) != 0; }

  /**
   * Sets the flush bit of a log block.
   *
   * @param val The value to set.
   */
  void set_flush_bit(bool val) noexcept {
    auto block_no = get_raw_block_no();

    if (val) {
      block_no |= LOG_BLOCK_FLUSH_BIT_MASK;
    } else {
      block_no &= ~LOG_BLOCK_FLUSH_BIT_MASK;
    }

    set_block_no(block_no);
  }

  /**
   * Returns a string representation of the log block.
   *
   * @return A string representation of the log block.
   */
  [[nodiscard]] std::string to_string() const noexcept {
    return std::format(
      "Log_phy_block: block_no: {}, data_len: {}, first_rec_group: {}, checkpoint_no: {}, checksum: {}",
      m_block_no,
      m_data_len,
      m_first_rec_group,
      m_checkpoint_no,
      m_checksum
    );
  }

  /** Block number, the left most bit is the flush bit. */
  uint32_t m_block_no{0};

  /** Valid data length inside m_data[]. */
  uint16_t m_data_len{0};

  /** Offset of the first mtr log record group in m_data[]. */
  uint16_t m_first_rec_group{0};

  /** Checkpoint number */
  uint32_t m_checkpoint_no{0};

  constexpr static ulint PAGE_SIZE = LOG_DATA_PAGE_SIZE;

  constexpr static ulint HEADER_SIZE =
    sizeof(m_block_no) + sizeof(m_data_len) + sizeof(m_first_rec_group) + sizeof(m_checkpoint_no);

  /** sizeof (m_checksum) */
  constexpr static ulint TRAILER_SIZE = sizeof(uint32_t);

  constexpr static ulint DATA_SIZE = PAGE_SIZE - HEADER_SIZE - TRAILER_SIZE;

  /** Actual data */
  byte m_data[DATA_SIZE]{0};

  /** Checksum of the entire block. */
  uint32_t m_checksum{0};
};

#pragma pack(pop)

static_assert(std::is_standard_layout<Log_phy_file_header>::value, "Log_phy_file_header must have a standard layout");
static_assert(std::is_standard_layout<Log_phy_checkpoint>::value, "Log_phy_checkpoint must have a standard layout");
static_assert(std::is_standard_layout<Log_phy_block>::value, "Log_phy_block must have a standard layout");

/** The data pages start after the block header and the two checkpoint fields. */
static_assert(sizeof(Log_phy_file_header) == LOG_METADATA_PAGE_SIZE, "Log_phy_file_header size != LOG_METADATA_PAGE_SIZE");
static_assert(sizeof(Log_phy_checkpoint) == LOG_METADATA_PAGE_SIZE, "Log_phy_checkpoint size != LOG_METADATA_PAGE_SIZE");
static_assert(sizeof(Log_phy_block) == LOG_DATA_PAGE_SIZE, "Log_phy_blocksize != LOG_DATA_PAGE_SIZE");

constexpr off_t LOG_DATA_PAGE_START = sizeof(Log_phy_file_header) + sizeof(Log_phy_checkpoint) * 2;

/* Checkpoint area constants for first file only */
constexpr off_t LOG_CHECKPOINT_AREA_SIZE = sizeof(Log_phy_checkpoint) * 2;
constexpr off_t LOG_FIRST_FILE_DATA_START = LOG_DATA_PAGE_START;

/* Checkpoint constants */
constexpr ulint LOG_CHECKPOINT_1 = sizeof(Log_phy_file_header);
constexpr ulint LOG_CHECKPOINT_2 = LOG_CHECKPOINT_1 + sizeof(Log_phy_checkpoint);

/* The counting of LSN starts from this value: this must be non-zero */
constexpr auto LOG_START_LSN = LOG_CHECKPOINT_2 + sizeof(Log_phy_checkpoint);
