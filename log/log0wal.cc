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
#include "mach0data.h"
#include "ut0ut.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <mutex>

#include <util/checksum.h>
#include <util/logger.h>

/** The logger wal-cpp declares extern; the library requires this definition. */
util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Info);

struct LogWalBackend::Impl {
  /**
   * Write blocks to the WAL file. Log_file::write() places each block at its
   * own offset. flush() tracks how far the log is written and synced: the
   * byte count returned here does not tell.
   */
  wal::Result<lsn_t> write(std::span<struct iovec> iovecs) noexcept {
    if (m_file == nullptr || iovecs.empty()) {
      return std::unexpected(wal::Status::IO_error);
    }

    return m_file->write(iovecs);
  }

  /** Make the written blocks durable. Out of space, wait until space is made available. */
  wal::Result<bool> sync() noexcept {
    if (m_file == nullptr) {
      return std::unexpected(wal::Status::IO_error);
    }

    while (::fdatasync(m_file->m_fd) != 0) {
      if (errno == ENOSPC || errno == EDQUOT) {
        wal::Log_file::wait_for_space("fdatasync");
      } else if (errno != EINTR) {
        return std::unexpected(wal::Status::IO_error);
      }
    }

    return true;
  }

  /** Serializes flush(), which writes all buffered log and moves the flushed and synced LSNs. */
  std::mutex m_flush_mutex;

  /** Signalled when a flush ends: the waiters it covered return. */
  std::condition_variable m_flush_cv;

  /** A thread is writing and syncing, protected by m_flush_mutex. */
  bool m_flushing{};

  /** Serializes appends with the write of the active buffer. Order: m_flush_mutex, then this. */
  std::mutex m_append_mutex;

  /** The WAL file: blocks are written at block_no * block_size, wrapping at the configured size. */
  std::unique_ptr<wal::Log_file> m_file;

  /** WAL log instance */
  std::unique_ptr<wal::Log> m_wal;
};

LogWalBackend::LogWalBackend(const Config &config, lsn_t start_lsn) noexcept
    : m_config(config), m_lsn(start_lsn), m_flushed_lsn(start_lsn), m_synced_lsn(start_lsn), m_impl(std::make_unique<Impl>()) {

  // Create WAL configuration
  wal::Buffer::Config buffer_config(m_config.m_n_blocks, m_config.m_block_size);

  /* A block's data starts on 8 bytes in the writer's array, whatever block it is. */
  ut_a(buffer_config.get_data_size_in_block() % 8 == 0);

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

  /* The log continues at m_lsn, a block boundary. What the file holds from
  there on is log of an earlier run that recovery did not use: an incomplete
  record group, or blocks after a torn one. Drop it, or a later recovery
  would read it as the continuation of this run's log. */
  if (m_config.m_max_file_size == 0) {
    const wal::Buffer::Config buffer_config(m_config.m_n_blocks, m_config.m_block_size);
    const auto off = off_t(m_lsn.load() / buffer_config.get_data_size_in_block() * m_config.m_block_size);
    struct stat st {};

    if (::fstat(m_impl->m_file->m_fd, &st) == 0 && st.st_size > off &&
        (::ftruncate(m_impl->m_file->m_fd, off) != 0 || ::fsync(m_impl->m_file->m_fd) != 0)) {
      log_err(std::format("Cannot truncate the WAL file {} at {}: {}", path.string(), off, strerror(errno)));
      return false;
    }
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
  through the callback registered by start_io(). The caller holds the log
  mutex, so the pieces of a big record group stay contiguous. */
  lsn_t new_lsn{};

  std::lock_guard<std::mutex> guard(m_impl->m_append_mutex);

  m_stats.m_n_bytes += len;

  while (len > 0) {
    const auto n = std::min(len, MAX_APPEND);
    auto result = m_impl->m_wal->append(std::span<const std::byte>(reinterpret_cast<const std::byte *>(data), n));

    if (!result.has_value()) {
      return 0;
    }

    const auto slot = result.value();

    new_lsn = slot.m_lsn + slot.m_len;
    data += n;
    len -= n;
  }

  m_lsn.store(new_lsn);

  ++m_stats.m_n_appends;

  return new_lsn;
}

bool LogWalBackend::flush(lsn_t lsn, bool sync_to_disk) noexcept {
  if (m_impl->m_wal == nullptr || m_impl->m_file == nullptr) {
    return false;
  }

  auto done = [&] { return (sync_to_disk ? m_synced_lsn.load() : m_flushed_lsn.load()) >= std::min(lsn, m_lsn.load()); };

  std::unique_lock<std::mutex> guard(m_impl->m_flush_mutex);

  /* Wait for the flush in progress: it may cover this one (group commit). */
  m_impl->m_flush_cv.wait(guard, [&] { return done() || !m_impl->m_flushing; });

  if (done()) {
    return true;
  }

  m_impl->m_flushing = true;
  guard.unlock();

  /* Let the next flusher in, and the waiters this flush covered out. */
  struct End {
    Impl &m_impl;
    ~End() {
      {
        std::lock_guard<std::mutex> lock(m_impl.m_flush_mutex);
        m_impl.m_flushing = false;
      }
      m_impl.m_flush_cv.notify_all();
    }
  } end{*m_impl};

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(iovecs);
  };

  lsn_t target{};

  {
    /* Write the full buffers waiting for I/O, then the active buffer: that
    is all the log appended up to target. */
    std::lock_guard<std::mutex> append_guard(m_impl->m_append_mutex);

    target = m_lsn.load();

    if (!m_impl->m_wal->write_to_store(write_cb).has_value() || !m_impl->m_wal->write_active(write_cb).has_value()) {
      log_err("Writing the WAL buffers failed");
      return false;
    }
  }

  m_flushed_lsn.store(std::max(m_flushed_lsn.load(), target));

  ++m_stats.m_n_writes;

  /* wal::Log::request_sync() syncs only through a thread pool, which the
  engine does not give it: sync here. */
  if (sync_to_disk) {
    if (!m_impl->sync().has_value()) {
      log_err("fdatasync() of the WAL file failed");
      return false;
    }

    m_synced_lsn.store(std::max(m_synced_lsn.load(), target));

    ++m_stats.m_n_syncs;
  }

  return true;
}

void LogWalBackend::discard_up_to(lsn_t lsn) noexcept {
  if (m_config.m_max_file_size != 0 || m_impl->m_file == nullptr) {
    return;
  }

  const wal::Buffer::Config buffer_config(m_config.m_n_blocks, m_config.m_block_size);
  const auto block_no = lsn / buffer_config.get_data_size_in_block();
  const auto off = off_t(block_no * m_config.m_block_size);

  /* The block that holds lsn stays: recovery reads from it. */
  if (off <= m_discarded_off) {
    return;
  }

  if (::fallocate(m_impl->m_file->m_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, m_discarded_off, off - m_discarded_off) == 0) {
    m_discarded_off = off;
  } else if (errno != EOPNOTSUPP) {
    log_warn(std::format("Cannot give back the space of the WAL before LSN {}: {}", lsn, strerror(errno)));
  }
}

lsn_t LogWalBackend::read(const Config &config, lsn_t start_lsn, const Consumer &consume) noexcept {
  const auto path = std::filesystem::path(config.m_log_dir) / config.m_log_file;
  const auto fd = ::open(path.c_str(), O_RDONLY);

  if (fd == -1) {
    return start_lsn;
  }

  /* On disk a block is its header, its data area and the CRC32C of the data
  area. Block n is at n * block_size, modulo the file size if the log wraps.
  The header is in network byte order: block number (with the flush bit),
  checkpoint number, first record group, data length. The CRC is in host
  byte order. */
  const wal::Buffer::Config buffer_config(config.m_n_blocks, config.m_block_size);
  const auto data_size = lsn_t(buffer_config.get_data_size_in_block());
  const auto wraps = config.m_max_file_size != 0;
  const auto n_file_blocks = wraps ? static_cast<lsn_t>(config.m_max_file_size) / config.m_block_size : LSN_MAX;
  constexpr auto BLOCK_NO_MASK = ~wal::Block_header::FLUSH_BIT_MASK;

  /* A block in the file is a header, data and a checksum. They're read to where they belong, as the
  writer has them: the data on memory allocated for it, aligned for the checksum, that reads 8 bytes at
  a time from its start. A block's data is a multiple of 8, so it's the same for the writer's array. */
  constexpr size_t DATA_ALIGN = 64;

  ut_a(data_size % 8 == 0);

  struct Free {
    void operator()(byte *ptr) const noexcept { std::free(ptr); }
  };

  const std::unique_ptr<byte, Free> data_buf(static_cast<byte *>(std::aligned_alloc(DATA_ALIGN, ut_calc_align(size_t(data_size), DATA_ALIGN))));

  ut_a(data_buf != nullptr);

  const auto data = data_buf.get();

  std::array<byte, sizeof(wal::Block_header::Data)> header;
  wal::crc32_t crc;

  std::array<iovec, 3> iov{{{header.data(), header.size()}, {data, size_t(data_size)}, {&crc, sizeof(crc)}}};

  util::Checksum checksum(util::ChecksumAlgorithm::CRC32C);

  auto lsn = start_lsn;
  auto block_no = lsn / data_size;
  auto offset = lsn % data_size;

  for (lsn_t n{}; n < n_file_blocks; ++n, ++block_no, offset = 0) {
    const auto pos = block_no * config.m_block_size;
    const auto off = off_t(wraps ? pos % lsn_t(config.m_max_file_size) : pos);

    if (::preadv(fd, iov.data(), int(iov.size()), off) != ssize_t(config.m_block_size)) {
      break;
    }

    const auto header_block_no = wal::block_no_t(mach_read_from_4(header.data()));

    /* A block of an earlier lap through the file, or one never written. */
    if ((header_block_no & BLOCK_NO_MASK) != (wal::block_no_t(block_no) & BLOCK_NO_MASK)) {
      break;
    }

    const auto len = lsn_t(mach_read_from_2(header.data() + offsetof(wal::Block_header::Data, m_len)));

    if (len > data_size || len < offset) {
      break;
    }

    checksum.reset();
    checksum.update(data, data_size);

    if (checksum.value() != crc) {
      break;
    }

    if (len > offset) {
      if (!consume(data + offset, ulint(len - offset))) {
        break;
      }

      lsn += len - offset;
    }

    if (len < data_size) {
      break;
    }
  }

  ::close(fd);

  return lsn;
}

namespace {

/** A checkpoint slot: number, LSN, then the CRC32C of both, big endian. */
constexpr ulint CHECKPOINT_SLOT_SIZE = 512;
constexpr ulint CHECKPOINT_NO = 0;
constexpr ulint CHECKPOINT_LSN = 8;
constexpr ulint CHECKPOINT_CRC = 16;

uint32_t checkpoint_crc(const byte *slot) noexcept {
  util::Checksum checksum(util::ChecksumAlgorithm::CRC32C);

  checksum.update(slot, CHECKPOINT_CRC);

  return checksum.value();
}

std::filesystem::path checkpoint_path(const LogWalBackend::Config &config) noexcept {
  return std::filesystem::path(config.m_log_dir) / LogWalBackend::CHECKPOINT_FILE;
}

}  // namespace

bool LogWalBackend::write_checkpoint(const Config &config, uint64_t no, lsn_t lsn) noexcept {
  const auto path = checkpoint_path(config);
  const auto fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);

  if (fd == -1) {
    log_err(std::format("Cannot open the checkpoint file {}", path.string()));
    return false;
  }

  std::array<byte, CHECKPOINT_SLOT_SIZE> slot{};

  mach_write_to_8(slot.data() + CHECKPOINT_NO, no);
  mach_write_to_8(slot.data() + CHECKPOINT_LSN, lsn);
  mach_write_to_4(slot.data() + CHECKPOINT_CRC, checkpoint_crc(slot.data()));

  const auto off = off_t((no % 2) * CHECKPOINT_SLOT_SIZE);
  const auto ok = ::pwrite(fd, slot.data(), slot.size(), off) == ssize_t(slot.size()) && ::fdatasync(fd) == 0;

  ::close(fd);

  if (!ok) {
    log_err(std::format("Cannot write the checkpoint file {}", path.string()));
  }

  return ok;
}

bool LogWalBackend::read_checkpoint(const Config &config, uint64_t &no, lsn_t &lsn) noexcept {
  const auto path = checkpoint_path(config);
  const auto fd = ::open(path.c_str(), O_RDONLY);

  if (fd == -1) {
    return false;
  }

  bool found{};

  for (ulint i{}; i < 2; ++i) {
    std::array<byte, CHECKPOINT_SLOT_SIZE> slot{};

    if (::pread(fd, slot.data(), slot.size(), off_t(i * CHECKPOINT_SLOT_SIZE)) != ssize_t(slot.size())) {
      continue;
    }

    if (mach_read_from_4(slot.data() + CHECKPOINT_CRC) != checkpoint_crc(slot.data())) {
      continue;
    }

    const auto slot_no = mach_read_from_8(slot.data() + CHECKPOINT_NO);

    if (!found || slot_no > no) {
      no = slot_no;
      lsn = mach_read_from_8(slot.data() + CHECKPOINT_LSN);
      found = true;
    }
  }

  ::close(fd);

  return found;
}

void LogWalBackend::start_io() noexcept {
  if (m_io_running.exchange(true)) {
    return;  // Already running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(iovecs);
  };

  m_impl->m_wal->start_io(write_cb, nullptr);
}

void LogWalBackend::stop_io() noexcept {
  if (!m_io_running.exchange(false)) {
    return;  // Not running
  }

  auto write_cb = [this](std::span<struct iovec> iovecs) -> wal::Result<lsn_t> {
    return m_impl->write(iovecs);
  };

  auto sync_cb = [this]() -> wal::Result<bool> {
    return m_impl->sync();
  };

  (void) m_impl->m_wal->shutdown(write_cb, &sync_cb);
}

ulint LogWalBackend::get_n_buffer_waits() const noexcept {
  return m_impl->m_wal == nullptr ? 0 : m_impl->m_wal->get_sync_write_count();
}

void LogWalBackend::wait_for_io_idle() noexcept {
  if (m_impl->m_wal != nullptr) {
    m_impl->m_wal->wait_for_io_idle();
  }
}
