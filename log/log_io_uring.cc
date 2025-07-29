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

#include "log_io_uring.h"
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include "mem0mem.h"
#include "ut0logger.h"

// Global WAL I/O system instance
WAL_IO_System *g_wal_io_system = nullptr;

WAL_IO_System::WAL_IO_System() {
  mutex_create(&m_mutex, IF_DEBUG("WAL_IO_System::mutex", ) IF_SYNC_DEBUG(SYNC_ANY_LATCH, ) Current_location());
}

WAL_IO_System::~WAL_IO_System() {
  if (m_initialized.load()) {
    shutdown();
  }
  mutex_free(&m_mutex);
}

bool WAL_IO_System::initialize(uint32_t queue_depth, uint32_t max_files) {
  if (m_initialized.load()) {
    return false;
  }

  // Initialize io_uring
  int ret = io_uring_queue_init(queue_depth, &m_ring, 0);
  if (ret != 0) {
    log_err("Failed to initialize io_uring: ", strerror(-ret));
    return false;
  }

  mutex_enter(&m_mutex);

  // Initialize file array
  m_files.resize(max_files);
  m_free_indices.reserve(max_files);

  for (uint32_t i = 0; i < max_files; ++i) {
    m_files[i].m_fd = -1;
    m_files[i].m_path = nullptr;
    m_files[i].m_size.store(0);
    m_files[i].m_pending_ops.store(0);
    m_files[i].m_is_open.store(false);
    m_files[i].m_mtime = 0;
    m_free_indices.push_back(i);
  }

  m_initialized.store(true);
  m_shutdown.store(false);

  mutex_exit(&m_mutex);

  log_info("WAL I/O system initialized with queue depth ", queue_depth, " and max files ", max_files);
  return true;
}

void WAL_IO_System::shutdown() {
  if (!m_initialized.load()) {
    return;
  }

  m_shutdown.store(true);

  mutex_enter(&m_mutex);

  // Close all open files
  for (auto &file : m_files) {
    if (file.m_is_open.load()) {
      if (file.m_fd >= 0) {
        close(file.m_fd);
        file.m_fd = -1;
      }
      if (file.m_path) {
        mem_free(file.m_path);
        file.m_path = nullptr;
      }
      file.m_is_open.store(false);
    }
  }

  // Clear containers
  m_files.clear();
  m_free_indices.clear();
  m_io_contexts.clear();

  mutex_exit(&m_mutex);

  // Cleanup io_uring
  io_uring_queue_exit(&m_ring);

  m_initialized.store(false);

  log_info("WAL I/O system shutdown complete");
}

int WAL_IO_System::allocate_file_index() {
  if (m_free_indices.empty()) {
    return -1;
  }

  int index = m_free_indices.back();
  m_free_indices.pop_back();
  return index;
}

void WAL_IO_System::release_file_index(int index) {
  if (index >= 0 && index < static_cast<int>(m_files.size())) {
    m_free_indices.push_back(index);
  }
}

bool WAL_IO_System::validate_file_index(int index) const {
  return index >= 0 && index < static_cast<int>(m_files.size()) && m_files[index].m_is_open.load();
}

uint64_t WAL_IO_System::get_timestamp() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

int WAL_IO_System::open_wal_file(const char *file_path, uint64_t file_size) {
  if (!m_initialized.load() || m_shutdown.load()) {
    return -1;
  }

  mutex_enter(&m_mutex);

  int index = allocate_file_index();
  if (index < 0) {
    mutex_exit(&m_mutex);
    log_err("No free file slots available for ", file_path);
    return -1;
  }

  int flags = O_RDWR | O_CREAT;
  int fd = open(file_path, flags, 0644);
  if (fd < 0) {
    release_file_index(index);
    mutex_exit(&m_mutex);
    log_err("Failed to open WAL file ", file_path, ": ", strerror(errno));
    return -1;
  }

  // Get file statistics
  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    release_file_index(index);
    mutex_exit(&m_mutex);
    log_err("Failed to stat WAL file ", file_path, ": ", strerror(errno));
    return -1;
  }

  // Initialize file structure
  auto &file = m_files[index];
  file.m_fd = fd;
  file.m_path = mem_strdup(file_path);
  file.m_size.store(file_size > 0 ? file_size : st.st_size);
  file.m_pending_ops.store(0);
  file.m_mtime = st.st_mtime;
  file.m_is_open.store(true);

  mutex_exit(&m_mutex);

  log_info("Opened WAL file ", file_path, " with index ", index, " size ", file.m_size.load());
  return index;
}

bool WAL_IO_System::close_wal_file(int file_index) {
  if (!validate_file_index(file_index)) {
    return false;
  }

  mutex_enter(&m_mutex);

  auto &file = m_files[file_index];

  // Wait for pending operations to complete
  while (file.m_pending_ops.load() > 0) {
    mutex_exit(&m_mutex);
    usleep(1000);  // 1ms
    mutex_enter(&m_mutex);
  }

  if (file.m_fd >= 0) {
    close(file.m_fd);
    file.m_fd = -1;
  }

  if (file.m_path) {
    mem_free(file.m_path);
    file.m_path = nullptr;
  }

  file.m_size.store(0);
  file.m_mtime = 0;
  file.m_is_open.store(false);

  release_file_index(file_index);

  mutex_exit(&m_mutex);

  log_info("Closed WAL file with index ", file_index);
  return true;
}

ssize_t WAL_IO_System::sync_read(int file_index, void *buffer, size_t length, off_t offset) {
  if (!validate_file_index(file_index)) {
    return -1;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  ssize_t result = pread(file.m_fd, buffer, length, offset);

  file.m_pending_ops.fetch_sub(1);

  if (result > 0) {
    stats.total_reads.fetch_add(1);
    stats.total_bytes_read.fetch_add(result);
    stats.total_sync_ops.fetch_add(1);
  } else {
    stats.total_errors.fetch_add(1);
    log_err("Sync read failed for file index ", file_index, ": ", strerror(errno));
  }

  return result;
}

ssize_t WAL_IO_System::sync_write(int file_index, const void *buffer, size_t length, off_t offset) {
  if (!validate_file_index(file_index)) {
    return -1;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  ssize_t result = pwrite(file.m_fd, buffer, length, offset);

  file.m_pending_ops.fetch_sub(1);

  if (result > 0) {
    stats.total_writes.fetch_add(1);
    stats.total_bytes_written.fetch_add(result);
    stats.total_sync_ops.fetch_add(1);

    // Update file size if we wrote beyond current size
    uint64_t new_size = offset + result;
    uint64_t current_size = file.m_size.load();
    while (new_size > current_size && !file.m_size.compare_exchange_weak(current_size, new_size)) {
      // Retry if size was updated by another thread
    }
  } else {
    stats.total_errors.fetch_add(1);
    log_err("Sync write failed for file index ", file_index, ": ", strerror(errno));
  }

  return result;
}

WAL_io_ctx_t *WAL_IO_System::create_io_context(IO_request req_type, uint32_t file_index, void *callback_data) {
  uint64_t op_id = m_next_op_id.fetch_add(1);

  WAL_io_ctx_t ctx;
  ctx.req_type = req_type;
  ctx.callback_data = callback_data;
  ctx.file_index = file_index;
  ctx.start_time = get_timestamp();
  ctx.bytes_requested = 0;
  ctx.result.store(0);
  ctx.completed.store(false);

  mutex_enter(&m_mutex);
  m_io_contexts[op_id] = ctx;
  mutex_exit(&m_mutex);

  return &m_io_contexts[op_id];
}

void WAL_IO_System::complete_io_context(uint64_t op_id, int result) {
  mutex_enter(&m_mutex);

  auto it = m_io_contexts.find(op_id);
  if (it != m_io_contexts.end()) {
    it->second.result.store(result);
    it->second.completed.store(true);

    if (result >= 0) {
      switch (it->second.req_type) {
        case IO_request::Async_log_read:
          stats.total_reads.fetch_add(1);
          if (result > 0) {
            stats.total_bytes_read.fetch_add(result);
          }
          break;
        case IO_request::Async_log_write:
          stats.total_writes.fetch_add(1);
          if (result > 0) {
            stats.total_bytes_written.fetch_add(result);
          }
          break;
        case IO_request::Async_log_flush:
          // Flush operations return 0 on success, no bytes transferred
          break;
        default:
          break;
      }
      stats.total_async_ops.fetch_add(1);
    } else {
      stats.total_errors.fetch_add(1);
    }
  }

  mutex_exit(&m_mutex);
}

int WAL_IO_System::async_read(int file_index, void *buffer, size_t length, off_t offset, void *callback_data) {
  if (!validate_file_index(file_index)) {
    return -1;
  }

  auto ctx = create_io_context(IO_request::Async_log_read, file_index, callback_data);
  if (!ctx) {
    return -1;
  }

  ctx->bytes_requested = length;

  struct io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
  if (!sqe) {
    log_err("Failed to get io_uring SQE for async read");
    return -1;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  io_uring_prep_read(sqe, file.m_fd, buffer, length, offset);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(&m_ring);
  if (ret < 0) {
    file.m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async read: ", strerror(-ret));
    return -1;
  }

  return m_next_op_id.load() - 1;  // Return the op_id that was just used
}

int WAL_IO_System::async_write(int file_index, const void *buffer, size_t length, off_t offset, void *callback_data) {
  if (!validate_file_index(file_index)) {
    return -1;
  }

  auto ctx = create_io_context(IO_request::Async_log_write, file_index, callback_data);
  if (!ctx) {
    return -1;
  }

  ctx->bytes_requested = length;

  struct io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
  if (!sqe) {
    log_err("Failed to get io_uring SQE for async write");
    return -1;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  io_uring_prep_write(sqe, file.m_fd, buffer, length, offset);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(&m_ring);
  if (ret < 0) {
    file.m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async write: ", strerror(-ret));
    return -1;
  }

  return m_next_op_id.load() - 1;  // Return the op_id that was just used
}

int WAL_IO_System::scatter_read(int file_index, const WAL_iovec_t *iovec, size_t iovec_count, void *callback_data) {
  if (!validate_file_index(file_index) || !iovec || iovec_count == 0) {
    return -1;
  }

  // Check if we can use vectored I/O (all buffers must be contiguous)
  bool can_use_vectored = (iovec_count > 1);
  off_t expected_offset = iovec[0].offset;

  for (size_t i = 0; i < iovec_count && can_use_vectored; ++i) {
    if (iovec[i].offset != expected_offset) {
      can_use_vectored = false;
      break;
    }
    expected_offset += iovec[i].length;
  }

  if (can_use_vectored && iovec_count <= IOV_MAX) {
    // Use true vectored I/O for contiguous scatter read
    auto ctx = create_io_context(IO_request::Async_log_read, file_index, callback_data);
    if (!ctx) {
      return -1;
    }

    // Calculate total bytes for statistics
    size_t total_bytes = 0;
    for (size_t i = 0; i < iovec_count; ++i) {
      total_bytes += iovec[i].length;
    }
    ctx->bytes_requested = total_bytes;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (!sqe) {
      log_err("Failed to get io_uring SQE for scatter read");
      return -1;
    }

    auto &file = m_files[file_index];
    file.m_pending_ops.fetch_add(1);

    // Convert WAL_iovec_t to struct iovec
    std::vector<struct iovec> iovecs(iovec_count);
    for (size_t i = 0; i < iovec_count; ++i) {
      iovecs[i].iov_base = iovec[i].buffer;
      iovecs[i].iov_len = iovec[i].length;
    }

    io_uring_prep_readv(sqe, file.m_fd, iovecs.data(), iovec_count, iovec[0].offset);
    io_uring_sqe_set_data(sqe, ctx);

    int ret = io_uring_submit(&m_ring);
    if (ret < 0) {
      file.m_pending_ops.fetch_sub(1);
      log_err("Failed to submit scatter read: ", strerror(-ret));
      return -1;
    }

    return m_next_op_id.load() - 1;
  } else {
    // Fall back to multiple individual operations for non-contiguous I/O
    std::vector<int> op_ids;
    op_ids.reserve(iovec_count);

    // Reserve space for all SQEs first
    std::vector<struct io_uring_sqe *> sqes(iovec_count);
    std::vector<WAL_io_ctx_t *> contexts(iovec_count);

    for (size_t i = 0; i < iovec_count; ++i) {
      contexts[i] = create_io_context(IO_request::Async_log_read, file_index, callback_data);
      if (!contexts[i]) {
        return -1;
      }
      contexts[i]->bytes_requested = iovec[i].length;

      sqes[i] = io_uring_get_sqe(&m_ring);
      if (!sqes[i]) {
        log_err("Failed to get io_uring SQE for scatter read operation ", i);
        return -1;
      }
    }

    auto &file = m_files[file_index];
    file.m_pending_ops.fetch_add(iovec_count);

    // Prepare all operations
    for (size_t i = 0; i < iovec_count; ++i) {
      io_uring_prep_read(sqes[i], file.m_fd, iovec[i].buffer, iovec[i].length, iovec[i].offset);
      io_uring_sqe_set_data(sqes[i], contexts[i]);
      op_ids.push_back(m_next_op_id.load() - (iovec_count - i));
    }

    // Submit all operations in a single batch
    int ret = io_uring_submit(&m_ring);
    if (ret < 0) {
      file.m_pending_ops.fetch_sub(iovec_count);
      log_err("Failed to submit scatter read batch: ", strerror(-ret));
      return -1;
    }

    return op_ids.empty() ? -1 : op_ids[0];
  }
}

int WAL_IO_System::gather_write(int file_index, const WAL_iovec_t *iovec, size_t iovec_count, void *callback_data) {
  if (!validate_file_index(file_index) || !iovec || iovec_count == 0) {
    return -1;
  }

  // Check if we can use vectored I/O (all buffers must be contiguous)
  bool can_use_vectored = (iovec_count > 1);
  off_t expected_offset = iovec[0].offset;

  for (size_t i = 0; i < iovec_count && can_use_vectored; ++i) {
    if (iovec[i].offset != expected_offset) {
      can_use_vectored = false;
      break;
    }
    expected_offset += iovec[i].length;
  }

  if (can_use_vectored && iovec_count <= IOV_MAX) {
    // Use true vectored I/O for contiguous gather write
    auto ctx = create_io_context(IO_request::Async_log_write, file_index, callback_data);
    if (!ctx) {
      return -1;
    }

    // Calculate total bytes for statistics
    size_t total_bytes = 0;
    for (size_t i = 0; i < iovec_count; ++i) {
      total_bytes += iovec[i].length;
    }
    ctx->bytes_requested = total_bytes;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (!sqe) {
      log_err("Failed to get io_uring SQE for gather write");
      return -1;
    }

    auto &file = m_files[file_index];
    file.m_pending_ops.fetch_add(1);

    // Convert WAL_iovec_t to struct iovec
    std::vector<struct iovec> iovecs(iovec_count);
    for (size_t i = 0; i < iovec_count; ++i) {
      iovecs[i].iov_base = iovec[i].buffer;
      iovecs[i].iov_len = iovec[i].length;
    }

    io_uring_prep_writev(sqe, file.m_fd, iovecs.data(), iovec_count, iovec[0].offset);
    io_uring_sqe_set_data(sqe, ctx);

    int ret = io_uring_submit(&m_ring);
    if (ret < 0) {
      file.m_pending_ops.fetch_sub(1);
      log_err("Failed to submit gather write: ", strerror(-ret));
      return -1;
    }

    return m_next_op_id.load() - 1;
  } else {
    // Fall back to multiple individual operations for non-contiguous I/O
    std::vector<int> op_ids;
    op_ids.reserve(iovec_count);

    // Reserve space for all SQEs first
    std::vector<struct io_uring_sqe *> sqes(iovec_count);
    std::vector<WAL_io_ctx_t *> contexts(iovec_count);

    for (size_t i = 0; i < iovec_count; ++i) {
      contexts[i] = create_io_context(IO_request::Async_log_write, file_index, callback_data);
      if (!contexts[i]) {
        return -1;
      }
      contexts[i]->bytes_requested = iovec[i].length;

      sqes[i] = io_uring_get_sqe(&m_ring);
      if (!sqes[i]) {
        log_err("Failed to get io_uring SQE for gather write operation ", i);
        return -1;
      }
    }

    auto &file = m_files[file_index];
    file.m_pending_ops.fetch_add(iovec_count);

    // Prepare all operations
    for (size_t i = 0; i < iovec_count; ++i) {
      io_uring_prep_write(sqes[i], file.m_fd, iovec[i].buffer, iovec[i].length, iovec[i].offset);
      io_uring_sqe_set_data(sqes[i], contexts[i]);
      op_ids.push_back(m_next_op_id.load() - (iovec_count - i));
    }

    // Submit all operations in a single batch
    int ret = io_uring_submit(&m_ring);
    if (ret < 0) {
      file.m_pending_ops.fetch_sub(iovec_count);
      log_err("Failed to submit gather write batch: ", strerror(-ret));
      return -1;
    }

    return op_ids.empty() ? -1 : op_ids[0];
  }
}

bool WAL_IO_System::flush_file(int file_index, bool sync_metadata) {
  if (!validate_file_index(file_index)) {
    return false;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  int ret;
  if (sync_metadata) {
    ret = fsync(file.m_fd);
  } else {
    ret = fdatasync(file.m_fd);
  }

  file.m_pending_ops.fetch_sub(1);

  if (ret != 0) {
    log_err("Failed to flush file index ", file_index, ": ", strerror(errno));
    stats.total_errors.fetch_add(1);
    return false;
  }

  stats.total_sync_ops.fetch_add(1);
  return true;
}

int WAL_IO_System::async_flush(int file_index, bool sync_metadata, void *callback_data) {
  if (!validate_file_index(file_index)) {
    return -1;
  }

  auto ctx = create_io_context(IO_request::Async_log_flush, file_index, callback_data);
  if (!ctx) {
    return -1;
  }

  ctx->bytes_requested = 0;  // Flush doesn't transfer bytes

  struct io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
  if (!sqe) {
    log_err("Failed to get io_uring SQE for async flush");
    return -1;
  }

  auto &file = m_files[file_index];
  file.m_pending_ops.fetch_add(1);

  // Prepare fsync operation
  unsigned fsync_flags = sync_metadata ? 0 : IORING_FSYNC_DATASYNC;
  io_uring_prep_fsync(sqe, file.m_fd, fsync_flags);
  io_uring_sqe_set_data(sqe, ctx);

  int ret = io_uring_submit(&m_ring);
  if (ret < 0) {
    file.m_pending_ops.fetch_sub(1);
    log_err("Failed to submit async flush: ", strerror(-ret));
    return -1;
  }

  return m_next_op_id.load() - 1;  // Return the op_id that was just used
}

int WAL_IO_System::process_completions(uint32_t max_events, uint32_t timeout_ms) {
  if (!m_initialized.load() || m_shutdown.load()) {
    return -1;
  }

  struct io_uring_cqe *cqe;
  int processed = 0;

  struct __kernel_timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000;

  for (uint32_t i = 0; i < max_events; ++i) {
    int ret;
    if (timeout_ms > 0) {
      ret = io_uring_wait_cqe_timeout(&m_ring, &cqe, &ts);
    } else {
      ret = io_uring_peek_cqe(&m_ring, &cqe);
    }

    if (ret < 0) {
      if (ret == -EAGAIN || ret == -ETIME) {
        break;  // No more events or timeout
      }
      log_err("Error waiting for completion: ", strerror(-ret));
      return -1;
    }

    if (cqe) {
      WAL_io_ctx_t *ctx = static_cast<WAL_io_ctx_t *>(io_uring_cqe_get_data(cqe));
      if (ctx) {
        // Update file pending ops counter
        if (ctx->file_index < m_files.size()) {
          m_files[ctx->file_index].m_pending_ops.fetch_sub(1);
        }

        // Complete the context
        complete_io_context(reinterpret_cast<uint64_t>(ctx) % m_next_op_id.load(), cqe->res);
      }

      io_uring_cqe_seen(&m_ring, cqe);
      processed++;
    }
  }

  return processed;
}

bool WAL_IO_System::wait_for_completion(int op_id, uint32_t timeout_ms) {
  if (op_id < 0) {
    return false;
  }

  uint64_t start_time = get_timestamp();
  uint64_t timeout_us = timeout_ms * 1000ULL;

  while ((get_timestamp() - start_time) < timeout_us) {
    mutex_enter(&m_mutex);
    auto it = m_io_contexts.find(op_id);
    if (it != m_io_contexts.end() && it->second.completed.load()) {
      bool success = it->second.result.load() >= 0;
      m_io_contexts.erase(it);
      mutex_exit(&m_mutex);
      return success;
    }
    mutex_exit(&m_mutex);

    // Process some completions
    process_completions(10, 1);
  }

  return false;  // Timeout
}

uint64_t WAL_IO_System::get_file_size(int file_index) const {
  if (!validate_file_index(file_index)) {
    return 0;
  }

  return m_files[file_index].m_size.load();
}

bool WAL_IO_System::is_file_open(int file_index) const {
  if (file_index < 0 || file_index >= static_cast<int>(m_files.size())) {
    return false;
  }

  return m_files[file_index].m_is_open.load();
}

// Global functions
bool wal_io_system_init() {
  if (g_wal_io_system) {
    return false;  // Already initialized
  }

  g_wal_io_system = new WAL_IO_System();
  if (!g_wal_io_system->initialize()) {
    delete g_wal_io_system;
    g_wal_io_system = nullptr;
    return false;
  }

  return true;
}

void wal_io_system_shutdown() {
  if (g_wal_io_system) {
    delete g_wal_io_system;
    g_wal_io_system = nullptr;
  }
}
