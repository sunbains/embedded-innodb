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

#include "log0wal.h"
#include "ut0ut.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>

#include <util/logger.h>

/** Global logger for WAL-CPP library */
util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Info);

Log_wal_backend::Log_wal_backend(const Config &config, lsn_t start_lsn) noexcept
    : m_config(config), m_lsn(start_lsn), m_flushed_lsn(start_lsn), m_synced_lsn(start_lsn) {

  // Create WAL configuration
  wal::Buffer::Config buffer_config(m_config.m_n_blocks, m_config.m_block_size);

  wal::Pool::Config pool_config;
  pool_config.m_pool_size = m_config.m_pool_size;

  wal::Log::Config log_config{
    .m_pool_config = pool_config,
    .m_buffer_config = buffer_config
  };

  // Create thread pool for I/O
  m_thread_pool = std::make_unique<util::Thread_pool>(m_config.m_n_io_threads);

  // Create WAL instance
  m_wal = std::make_unique<wal::Log>(start_lsn, log_config);
}

Log_wal_backend::~Log_wal_backend() noexcept {
  stop_io();
  (void) close();
}

bool Log_wal_backend::open() noexcept {
  // Ensure directory exists
  std::filesystem::create_directories(m_config.m_log_dir);

  // Build file path
  auto file_path = std::filesystem::path(m_config.m_log_dir) / m_config.m_log_file;

  // Open or create the log file
  m_fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);

  if (m_fd < 0) {
    // Try without O_DIRECT
    m_fd = ::open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
  }

  if (m_fd < 0) {
    return false;
  }

  // Get current file size to determine write offset
  struct stat st;
  if (::fstat(m_fd, &st) == 0) {
    m_write_offset = st.st_size;
  }

  return true;
}

bool Log_wal_backend::close() noexcept {
  if (m_fd >= 0) {
    ::close(m_fd);
    m_fd = -1;
  }
  return true;
}

lsn_t Log_wal_backend::append(const byte *data, ulint len) noexcept {
  if (!m_wal || len == 0) {
    return 0;
  }

  auto result = m_wal->append(
    std::span<const std::byte>(reinterpret_cast<const std::byte *>(data), len),
    m_thread_pool.get()
  );

  if (result.has_value()) {
    auto slot = result.value();
    lsn_t new_lsn = slot.m_lsn + slot.m_len;
    m_lsn.store(new_lsn);
    return new_lsn;
  }

  return 0;
}

bool Log_wal_backend::flush(lsn_t lsn, bool sync_to_disk) noexcept {
  if (!m_wal || m_fd < 0) {
    return false;
  }

  // Create write callback
  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return this->write_callback(iovecs);
  };

  // Flush to storage
  auto result = m_wal->write_to_store(write_cb);

  if (!result.has_value()) {
    return false;
  }

  // Sync to disk if requested
  if (sync_to_disk) {
    auto sync_cb = [this]() -> wal::Result<bool> {
      return this->sync_callback();
    };

    m_wal->request_sync(wal::Sync_type::Fdatasync, &sync_cb);
  }

  return true;
}

void Log_wal_backend::start_io() noexcept {
  if (m_io_running.exchange(true)) {
    return;  // Already running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return this->write_callback(iovecs);
  };

  m_wal->start_io(write_cb, m_thread_pool.get());
}

void Log_wal_backend::stop_io() noexcept {
  if (!m_io_running.exchange(false)) {
    return;  // Not running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return this->write_callback(iovecs);
  };

  auto sync_cb = [this]() -> wal::Result<bool> {
    return this->sync_callback();
  };

  (void) m_wal->shutdown(write_cb, &sync_cb);
}

void Log_wal_backend::wait_for_io_idle() noexcept {
  if (m_wal) {
    m_wal->wait_for_io_idle();
  }
}

wal::Result<lsn_t> Log_wal_backend::write_callback(std::span<struct iovec> iovecs) noexcept {
  if (m_fd < 0 || iovecs.empty()) {
    return std::unexpected(wal::Status::IO_error);
  }

  // Calculate total bytes to write
  size_t total_bytes = 0;
  for (const auto &iov : iovecs) {
    total_bytes += iov.iov_len;
  }

  // Write using pwritev
  ssize_t written = ::pwritev(m_fd, iovecs.data(), static_cast<int>(iovecs.size()), m_write_offset);

  if (written < 0 || static_cast<size_t>(written) != total_bytes) {
    return std::unexpected(wal::Status::IO_error);
  }

  m_write_offset += written;

  // Update flushed LSN
  lsn_t new_flushed = m_flushed_lsn.load() + static_cast<lsn_t>(written);
  m_flushed_lsn.store(new_flushed);

  return new_flushed;
}

wal::Result<bool> Log_wal_backend::sync_callback() noexcept {
  if (m_fd < 0) {
    return std::unexpected(wal::Status::IO_error);
  }

  if (::fdatasync(m_fd) != 0) {
    return std::unexpected(wal::Status::IO_error);
  }

  // Update synced LSN to match flushed
  m_synced_lsn.store(m_flushed_lsn.load());

  return true;
}
