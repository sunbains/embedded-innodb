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

#include <wal/io.h>
#include <wal/wal.h>
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

struct LogWalBackend::Impl {
  /**
   * Write blocks to the WAL file. Log_file::write() places each block at its
   * own offset and returns the number of payload bytes written, which is what
   * WAL-CPP expects.
   */
  wal::Result<lsn_t> write(LogWalBackend &backend, std::span<struct iovec> iovecs) noexcept {
    if (m_file == nullptr || iovecs.empty()) {
      return std::unexpected(wal::Status::IO_error);
    }

    auto result = m_file->write(iovecs);

    if (result.has_value()) {
      backend.m_flushed_lsn.fetch_add(result.value());
    }

    return result;
  }

  /** Make the written blocks durable. */
  wal::Result<bool> sync(LogWalBackend &backend) noexcept {
    if (m_file == nullptr) {
      return std::unexpected(wal::Status::IO_error);
    }

    if (::fdatasync(m_file->m_fd) != 0) {
      return std::unexpected(wal::Status::IO_error);
    }

    backend.m_synced_lsn.store(backend.m_flushed_lsn.load());

    return true;
  }

  /** The WAL file: blocks are written at block_no * block_size, wrapping at the configured size. */
  std::unique_ptr<wal::Log_file> m_file;

  /** WAL log instance */
  std::unique_ptr<wal::Log> m_wal;
};

LogWalBackend::LogWalBackend(const Config &config, lsn_t start_lsn) noexcept
    : m_config(config), m_lsn(start_lsn), m_flushed_lsn(start_lsn), m_synced_lsn(start_lsn), m_impl(std::make_unique<Impl>()) {

  // Create WAL configuration
  wal::Buffer::Config buffer_config(m_config.m_n_blocks, m_config.m_block_size);

  wal::Pool::Config pool_config;
  pool_config.m_pool_size = m_config.m_pool_size;

  wal::Log::Config log_config{
    .m_pool_config = pool_config,
    .m_buffer_config = buffer_config
  };

  // Create WAL instance
  m_impl->m_wal = std::make_unique<wal::Log>(start_lsn, log_config);
}

lsn_t LogWalBackend::align_start_lsn(const Config &config, lsn_t lsn) noexcept {
  const wal::Buffer::Config buffer_config(config.m_n_blocks, config.m_block_size);
  const auto data_size = static_cast<lsn_t>(buffer_config.get_data_size_in_block());

  return ((lsn + data_size - 1) / data_size) * data_size;
}

LogWalBackend::~LogWalBackend() noexcept {
  stop_io();
  (void) close();
}

bool LogWalBackend::open() noexcept {
  std::filesystem::create_directories(m_config.m_log_dir);

  const auto path = std::filesystem::path(m_config.m_log_dir) / m_config.m_log_file;

  try {
    m_impl->m_file = std::make_unique<wal::Log_file>(path.string(), m_config.m_max_file_size, m_config.m_block_size);
  } catch (const std::exception &e) {
    log_err(std::format("Cannot open the WAL file {}: {}", path.string(), e.what()));
    return false;
  }

  return true;
}

bool LogWalBackend::close() noexcept {
  m_impl->m_file.reset();
  return true;
}

lsn_t LogWalBackend::append(const byte *data, ulint len) noexcept {
  if (m_impl->m_wal == nullptr || len == 0) {
    return 0;
  }

  /* Synchronous mode: when the active buffer fills up it is written inline
  through the callback registered by start_io(). */
  auto result = m_impl->m_wal->append(std::span<const std::byte>(reinterpret_cast<const std::byte *>(data), len));

  if (result.has_value()) {
    auto slot = result.value();
    lsn_t new_lsn = slot.m_lsn + slot.m_len;
    m_lsn.store(new_lsn);
    return new_lsn;
  }

  return 0;
}

bool LogWalBackend::flush(lsn_t lsn, bool sync_to_disk) noexcept {
  if (m_impl->m_wal == nullptr || m_impl->m_file == nullptr) {
    return false;
  }

  // Create write callback
  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(*this, iovecs);
  };

  // Flush to storage
  auto result = m_impl->m_wal->write_to_store(write_cb);

  if (!result.has_value()) {
    return false;
  }

  // Sync to disk if requested
  if (sync_to_disk) {
    auto sync_cb = [this]() -> wal::Result<bool> {
      return m_impl->sync(*this);
    };

    m_impl->m_wal->request_sync(wal::Sync_type::Fdatasync, &sync_cb);
  }

  return true;
}

void LogWalBackend::start_io() noexcept {
  if (m_io_running.exchange(true)) {
    return;  // Already running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(*this, iovecs);
  };

  m_impl->m_wal->start_io(write_cb, nullptr);
}

void LogWalBackend::stop_io() noexcept {
  if (!m_io_running.exchange(false)) {
    return;  // Not running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(*this, iovecs);
  };

  auto sync_cb = [this]() -> wal::Result<bool> {
    return m_impl->sync(*this);
  };

  (void) m_impl->m_wal->shutdown(write_cb, &sync_cb);
}

void LogWalBackend::wait_for_io_idle() noexcept {
  if (m_impl->m_wal != nullptr) {
    m_impl->m_wal->wait_for_io_idle();
  }
}

