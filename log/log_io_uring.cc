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

/** @file log/log_io_uring.cc
WAL I/O operations using io_uring with scatter-gather support

Implementation of simplified I/O system specifically for WAL operations.
*******************************************************/

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <chrono>
#include <cstring>

#include "log_io_uring.h"
#include "mem0mem.h"
#include "ut0logger.h"

static const int CREATE_FILE_MASK = 0644;

// FIXME: This file_index and context map is a bad idea. We can do without both.

// FIXME: Global WAL I/O system instance
WalIoSystem *g_wal_io_system = nullptr;

/* Custom deleter function for io_uring that ensures proper cleanup */
void log_aio_deleter(io_uring *ring) noexcept {
  if (ring == nullptr) {
    return;
  }

  log_dbg("Cleaning up io_uring instance");

  // Ensure io_uring is properly shut down before deletion
  io_uring_queue_exit(ring);
  delete ring;
}

/* Custom allocator function for io_uring that provides logging and error handling */
io_uring *log_aio_allocator() noexcept {
  auto *ring = new (std::nothrow) io_uring();

  if (ring == nullptr) {
    // Log allocation failure
    log_fatal("Failed to allocate io_uring instance");
    return nullptr;
  }
  return ring;
}

WalIoSystem::WalIoSystem(size_t n_io_ctxs, size_t n_files) noexcept
    : m_ring(log_aio_allocator(), log_aio_deleter),
      m_files(n_files),
      m_free_files(n_files),
      m_io_contexts(n_io_ctxs),
      m_free_contexts(n_io_ctxs) {

  for (auto &io_ctx : m_io_contexts) {
    auto success = m_free_contexts.enqueue(&io_ctx);
    ut_a(success);
  }

  for (auto &file : m_files) {
    file.m_fd = -1;
    auto success = m_free_files.enqueue(&file);
    ut_a(success);
  }
}

WalIoSystem::~WalIoSystem() noexcept {
  if (m_initialized.load()) {
    shutdown();
  }
}

bool WalIoSystem::initialize(uint32_t queue_depth) noexcept {
  ut_a(!m_shutdown.load());
  ut_a(!m_initialized.load());

  int ret = io_uring_queue_init(queue_depth, m_ring.get(), 0);

  if (ret != 0) {
    log_err("Failed to initialize io_uring: ", strerror(-ret));
    m_ring.reset();
    return false;
  }

  m_shutdown.store(false);
  m_initialized.store(true);

  log_info("WAL I/O system initialized with io_uring queue depth ", queue_depth);

  return true;
}

void WalIoSystem::shutdown() noexcept {
  ut_a(m_initialized.load());
  ut_a(!m_shutdown.load());

  m_shutdown.store(true);

  for (auto &file : m_files) {
    if (file.m_fd >= 0) {
      log_dbg("Closing WAL file ", file.m_path);

      const auto ret = ::close(file.m_fd);

      if (ret != 0) {
        log_err("Failed to close WAL file ", file.m_path, ": ", strerror(errno));
      } else {

        file.m_fd = -1;
        file.m_path.clear();

        while (!m_free_files.enqueue(&file)) {
          usleep(1000);
        }
      }
    }
  }

  IF_DEBUG(
    {
      /* Check that there are no free contexts left */
      WalIoCtx *io_ctx = nullptr;
      size_t free_contexts_count = 0;

      while (m_free_contexts.dequeue(io_ctx)) {
        ++free_contexts_count;
      }

      const auto active_contexts_count = m_io_contexts.size() - free_contexts_count;

      if (active_contexts_count != 0) {
        log_err("There are still active contexts left: ", active_contexts_count);
      }
    }

    {
      /* Check that there are no free files left */
      WalFile *file = nullptr;
      size_t free_files_count = 0;

      while (m_free_files.dequeue(file)) {
        ++free_files_count;
      }

      const auto open_files_count = m_files.size() - free_files_count;
      if (open_files_count != 0) {
        log_err("There are still open files left: ", open_files_count);
      }
    }
  )

  m_files.clear();
  m_io_contexts.clear();

  /* Cleanup io_uring (handled automatically by custom deleter) */
  m_ring.reset();

  m_initialized.store(false);

  log_info("WAL I/O system shutdown complete");
}

WalFile *WalIoSystem::allocate_file() noexcept {
  WalFile *file = nullptr;

  while (!m_free_files.dequeue(file)) {
    usleep(1000);
  }

  return file;
}

void WalIoSystem::release_file(WalFile *&file) noexcept {
  ut_a(file != nullptr);

  while (!m_free_files.enqueue(file)) {
    usleep(1000);
  }

  file = nullptr;
}

uint64_t WalIoSystem::get_timestamp() const noexcept {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

bool WalIoSystem::file_exists(std::string_view file_path) noexcept {
  struct stat st;

  return ::stat(file_path.data(), &st) == 0;
}

bool WalIoSystem::is_file_rw_compatible(std::string_view file_path) noexcept {
  // Check if a file exists and can be read and written to
  struct stat st;

  if (::stat(file_path.data(), &st) != 0) {
    return false;
  }

  if (S_ISREG(st.st_mode) == 0) {
    return true;
  } else {
    return false;
  }

  return false;
}

off_t WalIoSystem::get_file_size(std::string_view file_path) noexcept {
  struct stat st;

  if (::stat(file_path.data(), &st) != 0) {
    return -1;
  }

  return st.st_size;
}

WalFile *WalIoSystem::create_wal_file(const std::string &file_path, off_t file_size) noexcept {
  ut_a(!file_path.empty());
  ut_a(file_size >= 0 && (file_size % sizeof(LogPhyBlock) == 0));

  ut_a(m_initialized.load());
  ut_a(!m_shutdown.load());

  ut_a(!file_exists(file_path));

  bool success{true};

  int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT, CREATE_FILE_MASK);

  if (fd < 0) {
    log_err("Failed to create WAL file ", file_path, ": ", strerror(errno));
    return nullptr;
  }

  if (::ftruncate(fd, file_size) != 0) {
    log_err("Failed to resize WAL file ", file_path, " to ", file_size, " bytes: ", strerror(errno));
    success = false;
  } else {
    log_dbg("Resized WAL file ", file_path, " to ", file_size, " bytes");
  }

  /* Get file size to verify it was resized */
  auto stat_file_size = get_file_size(file_path);

  if (file_size != stat_file_size) {
    log_err("File size mismatch for WAL file", file_path, ": ", file_size, "specified != stat ", stat_file_size, "reported");
    success = false;
  }

  if (::fsync(fd) != 0) {
    log_err("Failed to fsync WAL file ", file_path, ": ", strerror(errno));
    success = false;
  }

  const auto ret = ::close(fd);

  if (ret != 0) {
    log_err("Failed to close WAL file ", file_path, " after create : ", strerror(errno));
    success = false;
  }

  return success ? open_wal_file(file_path, file_size) : nullptr;
}

WalFile *WalIoSystem::open_wal_file(const std::string &file_path, off_t file_size) noexcept {
  ut_a(!file_path.empty());
  ut_a(file_size >= 0 && (file_size % sizeof(LogPhyBlock) == 0));

  ut_a(m_initialized.load());
  ut_a(!m_shutdown.load());

  auto file = allocate_file();

  if (file == nullptr) {
    log_err("No free file available for ", file_path);
    return nullptr;
  }

  int fd = ::open(file_path.c_str(), O_RDWR, 0);

  if (fd < 0) {
    release_file(file);
    log_err("Failed to open WAL file ", file_path, ": ", strerror(errno));
    return nullptr;
  }

  struct stat st;

  if (::fstat(fd, &st) != 0) {
    const auto ret = ::close(fd);

    if (ret != 0) {
      log_err("Failed to close WAL file ", file_path, " after failed stat: ", strerror(errno));
    }

    release_file(file);

    log_err("Failed to stat WAL file ", file_path, ": ", strerror(errno));

    return nullptr;
  }

  if (file_size != st.st_size) {
    const auto ret = ::close(fd);

    if (ret != 0) {
      log_err("Failed to close WAL file ", file_path, " after failed file size check: ", strerror(errno));
    }

    release_file(file);

    log_err("File size mismatch for WAL file", file_path, ": ", file_size, "specified != stat ", st.st_size, "reported");

    return nullptr;
  }

  file->m_fd = fd;
  file->m_path = file_path;
  file->m_size = file_size;
  file->m_pending_ops.store(0);
  file->m_mtime = st.st_mtime;

  log_dbg("Opened WAL file ", file_path, " with fd ", file->m_fd, " size ", file->m_size);

  return file;
}

bool WalIoSystem::close_wal_file(WalFile *file) noexcept {
  /* Wait for pending operations to complete */
  while (file->m_pending_ops.load() > 0) {
    /* Sleep for 10ms */
    usleep(10000);
  }

  if (file->m_fd >= 0) {
    const auto ret = ::close(file->m_fd);

    if (ret != 0) {
      log_err("Failed to close WAL file ", file->m_path, ": ", strerror(errno));
    }

    file->m_fd = -1;
    file->m_size = 0;
    file->m_mtime = 0;
    file->m_path.clear();

    log_dbg("Closed WAL file ", file->m_path);

    release_file(file);
  }

  return true;
}

ssize_t WalIoSystem::sync_read(WalFile *file, void *buffer, size_t length, off_t offset) noexcept {
  ut_a(length > 0);
  ut_a(offset >= 0);
  ut_a(buffer != nullptr);
  ut_a(off_t(length) <= file->m_size - offset);

  file->m_pending_ops.fetch_add(1);

  auto ptr = static_cast<std::byte *>(buffer);
  ssize_t total_read{};

  do {
    ut_a(length > 0);

    ssize_t ret = ::pread(file->m_fd, ptr, length, offset);

    if (ret < 0) {
      switch (errno) {
        case EINTR:
        case EAGAIN:
          continue;
        default:
          log_fatal("Sync read failed for file ", file->m_path, ": ", strerror(errno));
          break;
      }
    }

    ut_a(ret >= 0);
    total_read += ret;
    length -= ret;
    offset += ret;
    ptr += ret;

    stats.m_total_sync_ops.fetch_add(1);

  } while (length > 0);

  file->m_pending_ops.fetch_sub(1);

  if (total_read > 0) {
    stats.m_total_reads.fetch_add(1);
    stats.m_total_bytes_read.fetch_add(total_read);
  } else {
    stats.m_total_errors.fetch_add(1);
    log_err("Sync read failed for file ", file->m_path, ": ", strerror(errno));
  }

  return total_read;
}

ssize_t WalIoSystem::sync_write(WalFile *file, const void *buffer, size_t length, off_t offset) noexcept {
  ut_a(length > 0);
  ut_a(offset >= 0);
  ut_a(buffer != nullptr);

  ut_a(off_t(length) <= file->m_size - offset);

  file->m_pending_ops.fetch_add(1);

  auto ptr = static_cast<const std::byte *>(buffer);
  ssize_t total_written{};

  do {
    ut_a(length > 0);

    ssize_t ret = ::pwrite(file->m_fd, ptr, length, offset);

    if (ret < 0) {
      switch (errno) {
        case EINTR:
        case EAGAIN:
          continue;
        default:
          log_fatal("Sync write failed for file ", file->m_path, ": ", strerror(errno));
          break;
      }
    }

    ut_a(ret >= 0);
    total_written += ret;
    length -= ret;
    offset += ret;
    ptr += ret;

    stats.m_total_sync_ops.fetch_add(1);

  } while (length > 0);

  file->m_pending_ops.fetch_sub(1);

  if (total_written > 0) {
    stats.m_total_writes.fetch_add(1);
    stats.m_total_bytes_written.fetch_add(total_written);
  } else {
    stats.m_total_errors.fetch_add(1);
    log_err("Sync write failed for file ", file->m_path, ": ", strerror(errno));
  }

  return total_written;
}

WalIoCtx *WalIoSystem::create_io_context(
  IoRequest req_type, WalFile *file, std::function<void(WalIoCtx *)> callback
) noexcept {
  WalIoCtx *ctx;

  while (!m_free_contexts.dequeue(ctx)) {
    usleep(1000);
  }

  ctx->m_req_type = req_type;
  ctx->m_callback = callback;
  ctx->m_file = file;
  ctx->m_start_time = get_timestamp();
  ctx->m_bytes_requested = 0;
  ctx->m_result.store(0);
  ctx->m_completed.store(false);

  return ctx;
}

void WalIoSystem::release_io_context(WalIoCtx *&ctx) noexcept {
  while (!m_free_contexts.enqueue(ctx)) {
    usleep(1000);
  }

  ctx = nullptr;
}

void WalIoSystem::complete_io_context(WalIoCtx *&ctx, int result) noexcept {
  ctx->m_result.store(result);
  ctx->m_completed.store(true);

  if (ctx->m_callback) {
    ctx->m_callback(ctx);
  }

  auto req_type = ctx->m_req_type;

  release_io_context(ctx);

  if (result >= 0) {
    switch (req_type) {
      case IoRequest::Async_log_read:
        stats.m_total_reads.fetch_add(1);
        if (result > 0) {
          stats.m_total_bytes_read.fetch_add(result);
        }
        break;
      case IoRequest::Async_log_write:
        stats.m_total_writes.fetch_add(1);
        if (result > 0) {
          stats.m_total_bytes_written.fetch_add(result);
        }
        break;
      case IoRequest::Async_log_flush:
        // Flush operations return 0 on success, no bytes transferred
        break;
      default:
        break;
    }
    stats.m_total_async_ops.fetch_add(1);
  } else {
    stats.m_total_errors.fetch_add(1);
  }
}

WalIoCtx *WalIoSystem::async_read(
  WalFile *file, void *buffer, size_t length, off_t offset, std::function<void(WalIoCtx *)> callback
) noexcept {

  auto ctx = create_io_context(IoRequest::Async_log_read, file, callback);

  if (ctx == nullptr) {
    return nullptr;
  }

  ctx->m_bytes_requested = length;

  auto sqe = io_uring_get_sqe(m_ring.get());

  if (sqe == nullptr) {
    log_err("Failed to get io_uring SQE for async read");
    release_io_context(ctx);
    return nullptr;
  }

  file->m_pending_ops.fetch_add(1);

  io_uring_prep_read(sqe, file->m_fd, buffer, length, offset);
  io_uring_sqe_set_data(sqe, ctx);

  const int ret = io_uring_submit(m_ring.get());

  if (ret < 0) {
    file->m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async read for file ", file->m_path, ": ", strerror(-ret));
    release_io_context(ctx);
    return nullptr;
  }

  return ctx;
}

WalIoCtx *WalIoSystem::async_write(
  WalFile *file, const void *buffer, size_t length, off_t offset, std::function<void(WalIoCtx *)> callback
) noexcept {
  auto ctx = create_io_context(IoRequest::Async_log_write, file, callback);

  if (ctx == nullptr) {
    return nullptr;
  }

  ctx->m_bytes_requested = length;

  auto sqe = io_uring_get_sqe(m_ring.get());

  if (sqe == nullptr) {
    log_err("Failed to get io_uring SQE for async write");
    release_io_context(ctx);
    return nullptr;
  }

  file->m_pending_ops.fetch_add(1);

  io_uring_prep_write(sqe, file->m_fd, buffer, length, offset);
  io_uring_sqe_set_data(sqe, ctx);

  const int ret = io_uring_submit(m_ring.get());

  if (ret < 0) {
    file->m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async write for file ", file->m_path, ": ", strerror(-ret));
    release_io_context(ctx);
    return nullptr;
  }

  return ctx;
}

WalIoCtx *WalIoSystem::async_scatter_read(
  WalFile *file, const WAL_io_vecs &iovecs, off_t offset, std::function<void(WalIoCtx *)> callback
) noexcept {
  auto ctx = create_io_context(IoRequest::Async_log_read, file, callback);

  if (ctx == nullptr) {
    return nullptr;
  }

  {
    /* Calculate total bytes for statistics */
    size_t total_bytes{};
    for (const auto &iovec : iovecs) {
      total_bytes += iovec.iov_len;
    }
    ctx->m_bytes_requested = total_bytes;
  }

  auto sqe = io_uring_get_sqe(m_ring.get());

  if (sqe == nullptr) {
    log_err("Failed to get io_uring SQE for scatter read");
    release_io_context(ctx);
    return nullptr;
  }

  file->m_pending_ops.fetch_add(1);

  io_uring_prep_readv(sqe, file->m_fd, iovecs.data(), iovecs.size(), offset);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(m_ring.get());
  if (ret < 0) {
    file->m_pending_ops.fetch_sub(1);
    log_err("Failed to submit scatter read: ", strerror(-ret));
    release_io_context(ctx);
    return nullptr;
  }

  return ctx;
}

WalIoCtx *WalIoSystem::async_gather_write(
  WalFile *file, const WAL_io_vecs &iovecs, off_t offset, std::function<void(WalIoCtx *)> callback
) noexcept {

  auto ctx = create_io_context(IoRequest::Async_log_read, file, callback);

  if (ctx == nullptr) {
    return nullptr;
  }

  {
    /* Calculate total bytes for statistics */
    size_t total_bytes{};
    for (const auto &iovec : iovecs) {
      total_bytes += iovec.iov_len;
    }
    ctx->m_bytes_requested = total_bytes;
  }

  auto sqe = io_uring_get_sqe(m_ring.get());

  if (sqe == nullptr) {
    log_err("Failed to get io_uring SQE for gather write");
    release_io_context(ctx);
    return nullptr;
  }

  file->m_pending_ops.fetch_add(1);

  io_uring_prep_writev(sqe, file->m_fd, iovecs.data(), iovecs.size(), offset);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(m_ring.get());
  if (ret < 0) {
    file->m_pending_ops.fetch_sub(1);
    log_err("Failed to submit gather write: ", strerror(-ret));
    release_io_context(ctx);
    return nullptr;
  }

  return ctx;
}

bool WalIoSystem::flush_file(WalFile *file) noexcept {
  file->m_pending_ops.fetch_add(1);

  auto ret = ::fdatasync(file->m_fd);

  file->m_pending_ops.fetch_sub(1);

  if (ret != 0) {
    log_fatal("Failed to flush file ", file->m_path, ": ", strerror(errno));
    stats.m_total_errors.fetch_add(1);
    return false;
  }

  stats.m_total_sync_ops.fetch_add(1);
  return true;
}

WalIoCtx *WalIoSystem::async_flush(WalFile *file, std::function<void(WalIoCtx *)> callback) noexcept {
  auto ctx = create_io_context(IoRequest::Async_log_flush, file, callback);

  if (ctx == nullptr) {
    return nullptr;
  }

  /* Flush doesn't transfer bytes */
  ctx->m_bytes_requested = 0;

  auto sqe = io_uring_get_sqe(m_ring.get());

  if (sqe == nullptr) {
    log_err("Failed to get io_uring SQE for async flush");
    release_io_context(ctx);
    return nullptr;
  }

  file->m_pending_ops.fetch_add(1);

  io_uring_prep_fsync(sqe, file->m_fd, IORING_FSYNC_DATASYNC);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(m_ring.get());
  if (ret < 0) {
    file->m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async flush: ", strerror(-ret));
    release_io_context(ctx);
    return nullptr;
  }

  return ctx;
}

int WalIoSystem::process_completions(WalIoCtx *&in_ctx, uint32_t max_events, uint32_t timeout_ms) noexcept {
  ut_a(m_initialized.load());

  int processed{};
  io_uring_cqe *cqe{};
  struct __kernel_timespec ts;

  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000;

  for (uint32_t i{}; i < max_events; ++i) {
    int ret;

    if (timeout_ms > 0) {
      ret = io_uring_wait_cqe_timeout(m_ring.get(), &cqe, &ts);
    } else {
      ret = io_uring_peek_cqe(m_ring.get(), &cqe);
    }

    if (ret < 0) {
      if (ret == -EAGAIN || ret == -ETIME) {
        /* No more events or timeout */
        break;
      }
      log_err("Error waiting for completion: ", strerror(-ret));
      return -1;
    }

    ut_a(cqe != nullptr);

    auto ctx = static_cast<WalIoCtx *>(io_uring_cqe_get_data(cqe));

    if (ctx != nullptr) {
      ut_a(ctx->m_file != nullptr);
      ctx->m_file->m_pending_ops.fetch_sub(1);

      auto set_null = ctx == in_ctx;
      complete_io_context(ctx, cqe->res);

      if (set_null) {
        in_ctx = nullptr;
      }
    }

    io_uring_cqe_seen(m_ring.get(), cqe);
    ++processed;
  }

  return processed;
}

bool WalIoSystem::wait_for_completion(WalIoCtx *&ctx, uint32_t timeout_ms) noexcept {
  ut_a(ctx != nullptr);

  auto start_time = get_timestamp();
  auto timeout_us = timeout_ms * 1000ULL;

  while ((get_timestamp() - start_time) < timeout_us && ctx != nullptr) {

    if (ctx->m_completed.load()) {
      release_io_context(ctx);
      return true;
    }

    auto processed = process_completions(ctx, 10, 1);
    ut_a(processed >= 0);
  }

  /* Timeout */
  return ctx != nullptr ? false : true;
}

bool wal_io_system_init() noexcept {
  ut_a(g_wal_io_system == nullptr);

  g_wal_io_system = new (std::nothrow) WalIoSystem(32, 32);

  if (g_wal_io_system == nullptr) {
    return false;
  }

  // FIXME: IO uring queue depth is hardcoded for now
  if (!g_wal_io_system->initialize(16)) {
    delete g_wal_io_system;
    g_wal_io_system = nullptr;
    return false;
  }

  return true;
}

void wal_io_system_shutdown() noexcept {
  if (g_wal_io_system != nullptr) {
    delete g_wal_io_system;
    g_wal_io_system = nullptr;
  }
}
