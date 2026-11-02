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

/** @file log/log_vfile.cc
Log virtual file management and LSN calculations

This file implements the LogVfile class which manages multiple log files
and provides LSN (Log Sequence Number) to physical file/offset mapping.

LSN CALCULATION ALGORITHMS
==========================

The log system uses a circular log architecture where LSNs map to physical
locations across multiple log files. The key calculations are:

1. CAPACITY CALCULATION
   ---------------------
   Total log capacity = (file_size - header_size - checkpoint_area_size) + (file_size * (num_files - 1))

   Where:
   - file_size: Maximum size of each log file (e.g., 1MB)
   - header_size: Size of LogPhyFileHeader
   - checkpoint_area_size: Size of checkpoint area in first file (2 * checkpoint_size)
   - num_files: Number of log files in the group

   File 0: header + checkpoints + data
   Files 1+: data only

2. FILE NUMBER CALCULATION
   ------------------------
   Given an LSN, determine which log file it maps to:

   For checkpoint LSNs (first 2 blocks after header):
   file_no = 0  // Always in first file

   For data LSNs:
   file_no = ((lsn - first_data_lsn) / file_capacity) % num_files

   Where:
   - first_data_lsn: First LSN after header and checkpoints
   - file_capacity: file_size for data files, (file_size - header - checkpoints) for file 0

3. OFFSET CALCULATION
   -------------------
   Given an LSN, determine the offset within the target file:

   For checkpoint LSNs:
   offset = header_size + (lsn - first_header_lsn) * checkpoint_size

   For data LSNs in file 0:
   offset = header_size + checkpoint_area_size + ((lsn - first_data_lsn) % file_capacity)

   For data LSNs in other files:
   offset = ((lsn - first_data_lsn) % file_capacity)

4. SIZE OFFSET CONVERSION
   -----------------------
   Convert between logical offsets (excluding headers) and physical offsets:

   size_offset = offset - header_size * (1 + offset / file_size)
   real_offset = offset + header_size * (1 + offset / (file_size - header_size))

   These handle the header overhead when calculating usable space.

5. LSN OFFSET CALCULATION
   -----------------------
   Calculate the physical offset for an LSN relative to the current log position:

   - If lsn >= current_lsn: difference = lsn - current_lsn
   - If lsn < current_lsn: difference = group_size - ((current_lsn - lsn) % group_size)

   final_offset = (current_size_offset + difference) % group_size
   physical_offset = calc_real_offset(final_offset)

   This handles wrap-around in the circular log.

WRAP-AROUND HANDLING
====================

The log system handles wrap-around scenarios:

1. LSNs beyond capacity wrap to the beginning
2. Negative LSNs (before first_header_lsn) are adjusted by adding multiples
   of the total capacity
3. File numbers cycle through the available files using modulo arithmetic

EXAMPLE CALCULATION
===================

Given:
- 3 files of 1MB each (1,048,576 bytes)
- Header size: 512 bytes
- Checkpoint size: 512 bytes each (2 checkpoints = 1024 bytes)
- First header LSN: 0x1000
- First data LSN: 0x1000 + 512 + 1024 = 0x1400
- File 0 data capacity: 1,048,576 - 512 - 1024 = 1,047,040 bytes
- Other files capacity: 1,048,576 bytes each
- Total capacity: 1,047,040 + (1,048,576 * 2) = 3,144,192 bytes

For checkpoint LSN 0x1000:
- file_no = 0 (first file)
- offset = 512 + (0x1000 - 0x1000) = 512

For data LSN 0x50000:
- file_no = ((0x50000 - 0x1400) / 1,048,576) % 3 = 0 (first file)
- offset = 512 + 1024 + ((0x50000 - 0x1400) % 1,047,040) = 1536 + 0x4EC00

For data LSN 0x200000 (beyond first file):
- file_no = ((0x200000 - 0x1400) / 1,048,576) % 3 = 1 (second file)
- offset = ((0x200000 - 0x1400) % 1,048,576)

THREAD SAFETY
=============

The LogVfile class is designed for single-threaded access during normal
operation. The LSN calculation functions are const and thread-safe for
read operations, but file I/O operations require proper synchronization.

*******************************************************/

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <system_error>

#include "log0types.h"
#include "log_buffer.h"
#include "mem0mem.h"
#include "ut0logger.h"

constexpr ulint LOG_UNLOCK_NONE_FLUSHED_LOCK = 1;

LogFile::LogFile(const std::string &path, off_t max_size) noexcept : m_fd(-1), m_max_size(max_size), m_path(path) {}

LogFile::LogFile(LogFile &&rhs) noexcept
    : m_fd(rhs.m_fd), m_max_size(rhs.m_max_size), m_path(std::move(rhs.m_path)), m_pending_ops(rhs.m_pending_ops.load()) {
  rhs.m_fd = -1;
  rhs.m_pending_ops = 0;
}

LogFile &LogFile::operator=(LogFile &&rhs) noexcept {
  if (this != &rhs) {
    if (m_fd != -1) {
      auto ret = ::close(m_fd);
      ut_a(ret != -1);
    }
    m_fd = rhs.m_fd;
    m_max_size = rhs.m_max_size;
    m_path = std::move(rhs.m_path);
    m_pending_ops = rhs.m_pending_ops.load();
    rhs.m_fd = -1;
    rhs.m_pending_ops = 0;
  }
  return *this;
}

LogFile::~LogFile() noexcept {
  if (m_fd != -1) {
    auto ret = ::close(m_fd);
    ut_a(ret != -1);
    m_fd = -1;
  }
}

std::string LogFile::to_string() const noexcept {
  return std::format("Log file: path: {}, max_size: {}, fd: {}, pending_ops: {}", m_path, m_max_size, m_fd, m_pending_ops.load());
}

LogVfile::LogVfile(std::string directory, size_t n_files, off_t max_file_size)
    : m_directory(directory), m_max_file_size(max_file_size) {

  if (n_files < 2) {
    log_err("Minimum number of files is 2");
    throw std::invalid_argument("Minimum number of files is 2");
  }

  for (size_t i = 0; i < n_files; ++i) {
    m_files.emplace_back(directory + "/redo_log_" + std::to_string(i), max_file_size);
  }
}

LogVfile::~LogVfile() noexcept {
  m_files.clear();
}

std::string LogVfile::to_string() const noexcept {
  std::string str{};

  for (auto &file : m_files) {
    str += file.to_string();
    str += ", ";
  }

  const char *state = (m_state == State::OK ? "OK" : "CORRUPTED");

  return std::format(
    "Log vfile: directory: {}, max_file_size: {}, files: {{ {} }}, state: {}, lsn: {}, lsn_offset: {},"
    " n_pending_writes: {}, scanned_lsn: {}",
    m_directory,
    m_max_file_size,
    str,
    state,
    m_lsn,
    m_lsn_offset,
    m_n_pending_writes,
    m_recovered_upto_lsn
  );
}

void LogVfile::set_fields(lsn_t lsn) noexcept {
  m_lsn = lsn;
  m_lsn_offset = calc_lsn_offset(lsn);
}

off_t LogVfile::get_capacity() const noexcept {
  ut_ad(m_files.size() > 0);
  ut_ad(m_max_file_size > LOG_FIRST_FILE_DATA_START);

  /* First file: header + checkpoints + data, other files: data only */
  auto first_file_data_capacity = m_max_file_size - LOG_FIRST_FILE_DATA_START;
  auto other_files_capacity = m_max_file_size * (m_files.size() - 1);
  auto capacity = first_file_data_capacity + other_files_capacity;

  ut_ad(capacity > 0);
  ut_ad(capacity > LOG_CHECKPOINT_AREA_SIZE);

  return capacity;
}

off_t LogVfile::calc_size_offset(off_t offset) const noexcept {
  /* For first file: subtract header and checkpoint area */
  if (offset < m_max_file_size) {
    return offset - LOG_FIRST_FILE_DATA_START;
  }
  /* For other files: no header overhead */
  return offset;
}

off_t LogVfile::calc_real_offset(off_t offset) noexcept {
  /* For first file: add header and checkpoint area */
  if (offset < m_max_file_size) {
    return offset + LOG_FIRST_FILE_DATA_START;
  }
  /* For other files: no header overhead */
  return offset;
}

off_t LogVfile::calc_lsn_offset(lsn_t lsn) noexcept {
  auto group_size = get_capacity();
  auto lsn_size_offset = calc_size_offset(m_lsn_offset);

  off_t difference;

  if (lsn >= m_lsn) {
    difference = off_t(lsn - m_lsn);
  } else {
    difference = off_t(m_lsn - lsn);
    difference %= group_size;
    difference = group_size - difference;
  }

  auto offset = (lsn_size_offset + difference) % group_size;

  ut_a(offset < (off_t(1) << 32));

  return calc_real_offset(offset);
}

std::pair<const LogFile *, off_t> LogVfile::get_io_coordinates(lsn_t first_header_lsn, lsn_t lsn) const noexcept {
  const int64_t first_file_data_capacity = m_max_file_size - LOG_FIRST_FILE_DATA_START;
  const int64_t other_files_capacity = m_max_file_size;
  const int64_t total_capacity = first_file_data_capacity + other_files_capacity * (m_files.size() - 1);

  if (lsn < first_header_lsn) {
    auto add_this_many = 1 + (first_header_lsn - lsn) / total_capacity;
    lsn += add_this_many * total_capacity;
  }

  ut_a(lsn >= first_header_lsn);

  auto lsn_offset = lsn - first_header_lsn;

  if (static_cast<uint64_t>(lsn_offset) < static_cast<uint64_t>(first_file_data_capacity)) {
    /* Data in first file */
    return std::make_pair(&m_files[0], LOG_FIRST_FILE_DATA_START + lsn_offset);
  } else {
    /* Data in other files */
    auto remaining_offset = lsn_offset - first_file_data_capacity;
    auto file_no = 1 + (remaining_offset / other_files_capacity);
    auto offset = remaining_offset % other_files_capacity;
    return std::make_pair(&m_files[file_no % m_files.size()], offset);
  }
}

bool LogVfile::open() noexcept {
  try {
    std::filesystem::path dir_path{m_directory};

    if (!std::filesystem::exists(dir_path)) {
      std::error_code ec;

      if (!std::filesystem::create_directories(dir_path, ec)) {
        log_err("Failed to create log group directory: ", m_directory);
        return false;
      }
      log_info("Created log group directory: ", m_directory);
      m_created_directory = true;
    } else if (!std::filesystem::is_directory(dir_path)) {
      log_err("Log group directory is not a directory: ", m_directory);
      return false;
    }
  } catch (const std::filesystem::filesystem_error &) {
    log_err("Failed to open log group: ", m_directory);
    return false;
  }

  for (auto &file : m_files) {
    if (!std::filesystem::exists(file.m_path)) {
      log_err("Log file does not exist: ", file.m_path, " creating it...");

      file.m_fd = ::open(file.m_path.c_str(), O_RDWR | O_CREAT, 0666);

      if (file.m_fd == -1) {
        log_err("Failed to create log file: ", file.m_path, ": ", strerror(errno));
        return false;
      }

      log_info("Created log file: ", file.m_path);

    } else {

      file.m_fd = ::open(file.m_path.c_str(), O_RDWR | O_CREAT, 0666);

      if (file.m_fd == -1) {
        log_err("Failed to open log file: ", file.m_path, ": ", strerror(errno));
        return false;
      }

      log_info("Opened log file: ", file.m_path);
    }

    if (file.m_fd != -1) {
      struct stat file_stat;
      auto ret = ::fstat(file.m_fd, &file_stat);

      if (ret == -1) {
        log_err("Failed to stat log file: ", file.m_path, ": ", strerror(errno));
        return false;
      }

      if (file_stat.st_size < m_max_file_size) {
        auto ret = ::ftruncate(file.m_fd, m_max_file_size);

        if (ret == -1) {
          log_err("Failed to truncate log file: ", file.m_path, ": ", strerror(errno));
          return false;
        }

        log_info("Expanded log file: ", file.m_path, " to ", m_max_file_size, " bytes");
      }
    }
  }

  return true;
}

bool LogVfile::close() noexcept {
  for (auto &file : m_files) {
    if (file.m_fd != -1) {
      auto ret = ::close(file.m_fd);
      ut_a(ret != -1);
      file.m_fd = -1;
    }
  }
  return true;
}

bool LogVfile::sync_read(void *buf, size_t len, off_t off) const noexcept {
  return true;
}

bool LogVfile::sync_write(const void *buf, size_t len, off_t off) noexcept {
  return true;
}

bool LogVfile::async_write(const void *buf, size_t len, off_t offset) noexcept {
  return true;
}

bool LogVfile::read(LogPhyCheckpoint &phy_checkpoint, off_t off) const noexcept {
  return sync_read(&phy_checkpoint, sizeof(phy_checkpoint), off);
}

bool LogVfile::flush_header(byte *header, off_t offset, lsn_t start_lsn) noexcept {
  // auto io_coordinates = get_write_coordinates(LOG_START_LSN, start_lsn);

  // mach_write_to_4(m_header + LOG_GROUP_ID, m_id);
  // mach_write_to_8(m_header + LOG_FILE_START_LSN, start_lsn);

  // memset(header + LOG_FILE_WAS_CREATED_BY_BACKUP, ' ', 4);

  // FIXME: This needs to be part of the log_io system
  ++m_n_pending_writes;

  // sync_write(header, IB_FILE_BLOCK_SIZE, offset);

  // FIXME: This needs to be part of the log_io system
  --m_n_pending_writes;

  return false;
}

bool LogVfile::read(LogBuffer &buffer, lsn_t start_lsn, lsn_t end_lsn) noexcept {
  // FIXME: Implement this.
  return false;
}

bool LogVfile::write(LogBuffer &buffer, lsn_t write_lsn) noexcept {
  ut_a(buffer.size() % sizeof(LogPhyBlock) == 0);
  ut_a(write_lsn % lsn_t(sizeof(LogPhyBlock)) == 0);

  // FIXME: Calculate the length of the buffer to write.
  ulint len = {};

  while (len > 0) {
    // auto offset = calc_lsn_offset(write_lsn);

    //if ((offset % m_max_file_size == sizeof(LogPhyFileHeader)) && write_header) {
    // flush_header(offset / m_max_file_size, start_lsn);

    // FIXME: This needs to be part of the log_io system
    // m_written_to_all_lsn += sizeof(LogPhyBlock);
    // ++m_n_writes;
    //}

    // ++m_n_pending_writes;

    // sync_write(buf, write_len, offset);

    // --m_n_pending_writes;

    // if (write_len < len) {
    //   start_lsn += write_len;
    //   len -= write_len;
    //   buf += write_len;
    //   write_header = true;
    // } else {
    //   break;
    // }
  }
  return true;
}

bool LogVfile::sync() noexcept {
  // FIXME: Determine which file to sync
  auto fd = m_files[0].m_fd;
  auto ret = ::fsync(fd);

  if (ret == -1) {
    // log_fatal("Failed to sync log file: ", file.m_path, ": ", strerror(errno));
  }

  return true;
}

void LogVfile::checkpoint_set(ulint file_no, off_t offset) noexcept {
  // auto buf = m_checkpoint_buf;

  //mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * m_id + LOG_CHECKPOINT_UNUSED_FILE_NO, file_no);
  //mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * m_id + LOG_CHECKPOINT_UNUSED_OFFSET, offset);
}

std::pair<ulint, ulint> LogVfile::checkpoint_get() const noexcept {
  std::pair<ulint, ulint> result;

  return result;
}

void LogVfile::prepare_checkpoint(ulint checkpoint_no, lsn_t checkpoint_lsn) noexcept {
  // static_assert(LOG_CHECKPOINT_SIZE <= IB_FILE_BLOCK_SIZE, "error LOG_CHECKPOINT_SIZE > IB_FILE_BLOCK_SIZE");

  // auto buf = m_checkpoint_buf;

  /* Write the checkpoint number */
  // mach_write_to_8(buf + LOG_CHECKPOINT_NO, checkpoint_no);

  /* Write the checkpoint LSN */
  // mach_write_to_8(buf + LOG_CHECKPOINT_LSN, checkpoint_lsn);

  /* Write the checkpoint offset */
  // mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, calc_lsn_offset(checkpoint_lsn));

  // checkpoint_set(0, 0);

  /* Calculate and write the first checksum */
  // auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
  // mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

  /* Calculate and write the second checksum */
  // fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
  // mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

  // /* Write the magic number for the tablespace */
  // mach_write_to_4(buf + LOG_CHECKPOINT_FSP_MAGIC_N, LOG_CHECKPOINT_FSP_MAGIC_N_VAL);
}

bool LogVfile::write_checkpoint([[maybe_unused]] LogPhyCheckpoint *checkpoint, [[maybe_unused]] off_t off) noexcept {
  return true;
}
