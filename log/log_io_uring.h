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

/** @file log/log_io_uring.h
WAL I/O operations using io_uring with scatter-gather support

This header defines a simplified I/O system specifically for WAL operations
that uses io_uring for high-performance asynchronous I/O with scatter-gather
capabilities, removing dependencies on the general file system (fil0fil.cc).
*****************************************************************************/

#pragma once

#include <liburing.h>
#include <atomic>
#include <unordered_map>
#include <vector>
#include "innodb0types.h"
#include "log0types.h"
#include "os0aio.h"
#include "sync0sync.h"

// Forward declarations
struct log_group_t;

/** WAL file descriptor structure */
struct WAL_file {
  /** File descriptor */
  int m_fd{-1};

  /** File path */
  char *m_path{nullptr};

  /** File size in bytes */
  std::atomic<uint64_t> m_size{0};

  /** Number of pending I/O operations */
  std::atomic<uint32_t> m_pending_ops{0};

  /** File is open flag */
  std::atomic<bool> m_is_open{false};

  /** File creation/modification time for validation */
  time_t m_mtime{0};

  /** Default constructor */
  WAL_file() = default;

  /** Copy constructor */
  WAL_file(const WAL_file &other) : m_fd(other.m_fd), m_path(other.m_path), m_mtime(other.m_mtime) {
    m_size.store(other.m_size.load());
    m_pending_ops.store(other.m_pending_ops.load());
    m_is_open.store(other.m_is_open.load());
  }

  /** Move constructor */
  WAL_file(WAL_file &&other) noexcept : m_fd(other.m_fd), m_path(other.m_path), m_mtime(other.m_mtime) {
    m_size.store(other.m_size.load());
    m_pending_ops.store(other.m_pending_ops.load());
    m_is_open.store(other.m_is_open.load());
    other.m_fd = -1;
    other.m_path = nullptr;
    other.m_mtime = 0;
  }

  /** Copy assignment operator */
  WAL_file &operator=(const WAL_file &other) {
    if (this != &other) {
      m_fd = other.m_fd;
      m_path = other.m_path;
      m_mtime = other.m_mtime;
      m_size.store(other.m_size.load());
      m_pending_ops.store(other.m_pending_ops.load());
      m_is_open.store(other.m_is_open.load());
    }
    return *this;
  }

  /** Move assignment operator */
  WAL_file &operator=(WAL_file &&other) noexcept {
    if (this != &other) {
      m_fd = other.m_fd;
      m_path = other.m_path;
      m_mtime = other.m_mtime;
      m_size.store(other.m_size.load());
      m_pending_ops.store(other.m_pending_ops.load());
      m_is_open.store(other.m_is_open.load());
      other.m_fd = -1;
      other.m_path = nullptr;
      other.m_mtime = 0;
    }
    return *this;
  }
};

/** Scatter-gather I/O vector */
struct WAL_iovec_t {
  /** Buffer pointer */
  void *buffer;

  /** Buffer length */
  size_t length;

  /** File offset */
  off_t offset;
};

/** WAL I/O completion context */
struct WAL_io_ctx_t {
  /** I/O request type */
  IO_request req_type;

  /** Completion callback data */
  void *callback_data;

  /** File descriptor index */
  uint32_t file_index;

  /** Operation start time for latency tracking */
  uint64_t start_time;

  /** Number of bytes requested */
  size_t bytes_requested;

  /** Result code */
  std::atomic<int> result{0};

  /** Completion flag */
  std::atomic<bool> completed{false};

  /** Default constructor */
  WAL_io_ctx_t() = default;

  /** Copy constructor */
  WAL_io_ctx_t(const WAL_io_ctx_t &other)
      : req_type(other.req_type),
        callback_data(other.callback_data),
        file_index(other.file_index),
        start_time(other.start_time),
        bytes_requested(other.bytes_requested) {
    result.store(other.result.load());
    completed.store(other.completed.load());
  }

  /** Move constructor */
  WAL_io_ctx_t(WAL_io_ctx_t &&other) noexcept
      : req_type(other.req_type),
        callback_data(other.callback_data),
        file_index(other.file_index),
        start_time(other.start_time),
        bytes_requested(other.bytes_requested) {
    result.store(other.result.load());
    completed.store(other.completed.load());
    other.callback_data = nullptr;
  }

  /** Copy assignment operator */
  WAL_io_ctx_t &operator=(const WAL_io_ctx_t &other) {
    if (this != &other) {
      req_type = other.req_type;
      callback_data = other.callback_data;
      file_index = other.file_index;
      start_time = other.start_time;
      bytes_requested = other.bytes_requested;
      result.store(other.result.load());
      completed.store(other.completed.load());
    }
    return *this;
  }

  /** Move assignment operator */
  WAL_io_ctx_t &operator=(WAL_io_ctx_t &&other) noexcept {
    if (this != &other) {
      req_type = other.req_type;
      callback_data = other.callback_data;
      file_index = other.file_index;
      start_time = other.start_time;
      bytes_requested = other.bytes_requested;
      result.store(other.result.load());
      completed.store(other.completed.load());
      other.callback_data = nullptr;
    }
    return *this;
  }
};

/** Simplified WAL I/O system using io_uring */
struct WAL_IO_System {
  /** Constructor */
  WAL_IO_System();

  /** Destructor */
  ~WAL_IO_System();

  /** Initialize the WAL I/O system
   * @param queue_depth io_uring queue depth
   * @param max_files maximum number of WAL files
   * @return true on success
   */
  bool initialize(uint32_t queue_depth = 256, uint32_t max_files = 16);

  /** Shutdown the WAL I/O system */
  void shutdown();

  /** Open a WAL file
   * @param file_path path to the WAL file
   * @param file_size expected file size (0 for auto-detect)
   * @return file index on success, -1 on error
   */
  int open_wal_file(const char *file_path, uint64_t file_size = 0);

  /** Close a WAL file
   * @param file_index file index returned by open_wal_file
   * @return true on success
   */
  bool close_wal_file(int file_index);

  /** Submit synchronous read operation
   * @param file_index file index
   * @param buffer read buffer
   * @param length number of bytes to read
   * @param offset file offset
   * @return number of bytes read, -1 on error
   */
  ssize_t sync_read(int file_index, void *buffer, size_t length, off_t offset);

  /** Submit synchronous write operation
   * @param file_index file index
   * @param buffer write buffer
   * @param length number of bytes to write
   * @param offset file offset
   * @return number of bytes written, -1 on error
   */
  ssize_t sync_write(int file_index, const void *buffer, size_t length, off_t offset);

  /** Submit asynchronous read operation
   * @param file_index file index
   * @param buffer read buffer
   * @param length number of bytes to read
   * @param offset file offset
   * @param callback_data data passed to completion handler
   * @return operation ID on success, -1 on error
   */
  int async_read(int file_index, void *buffer, size_t length, off_t offset, void *callback_data = nullptr);

  /** Submit asynchronous write operation
   * @param file_index file index
   * @param buffer write buffer
   * @param length number of bytes to write
   * @param offset file offset
   * @param callback_data data passed to completion handler
   * @return operation ID on success, -1 on error
   */
  int async_write(int file_index, const void *buffer, size_t length, off_t offset, void *callback_data = nullptr);

  /** Submit scatter-gather read operation
   * @param file_index file index
   * @param iovec array of I/O vectors
   * @param iovec_count number of I/O vectors
   * @param callback_data data passed to completion handler
   * @return operation ID on success, -1 on error
   */
  int scatter_read(int file_index, const WAL_iovec_t *iovec, size_t iovec_count, void *callback_data = nullptr);

  /** Submit scatter-gather write operation
   * @param file_index file index
   * @param iovec array of I/O vectors
   * @param iovec_count number of I/O vectors
   * @param callback_data data passed to completion handler
   * @return operation ID on success, -1 on error
   */
  int gather_write(int file_index, const WAL_iovec_t *iovec, size_t iovec_count, void *callback_data = nullptr);

  /** Flush/sync a WAL file to disk (synchronous)
   * @param file_index file index
   * @param sync_metadata whether to sync metadata as well
   * @return true on success
   */
  bool flush_file(int file_index, bool sync_metadata = true);

  /** Submit asynchronous flush/sync operation
   * @param file_index file index
   * @param sync_metadata whether to sync metadata as well
   * @param callback_data data passed to completion handler
   * @return operation ID on success, -1 on error
   */
  int async_flush(int file_index, bool sync_metadata = true, void *callback_data = nullptr);

  /** Process completion events
   * @param max_events maximum number of events to process
   * @param timeout_ms timeout in milliseconds (0 for non-blocking)
   * @return number of events processed, -1 on error
   */
  int process_completions(uint32_t max_events = 32, uint32_t timeout_ms = 0);

  /** Wait for a specific operation to complete
   * @param op_id operation ID returned by async operations
   * @param timeout_ms timeout in milliseconds
   * @return true if operation completed successfully
   */
  bool wait_for_completion(int op_id, uint32_t timeout_ms = 5000);

  /** Get file size
   * @param file_index file index
   * @return file size in bytes, 0 on error
   */
  uint64_t get_file_size(int file_index) const;

  /** Check if file is open
   * @param file_index file index
   * @return true if file is open
   */
  bool is_file_open(int file_index) const;

  /** Get I/O statistics */
  struct {
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> total_bytes_read{0};
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> total_sync_ops{0};
    std::atomic<uint64_t> total_async_ops{0};
    std::atomic<uint64_t> total_errors{0};
  } stats;

 private:
  /** io_uring instance */
  struct io_uring m_ring;

  /** Initialization flag */
  std::atomic<bool> m_initialized{false};

  /** Shutdown flag */
  std::atomic<bool> m_shutdown{false};

  /** Mutex for thread safety */
  mutable mutex_t m_mutex;

  /** WAL files array */
  std::vector<WAL_file> m_files;

  /** Free file indices */
  std::vector<uint32_t> m_free_indices;

  /** I/O context map */
  std::unordered_map<uint64_t, WAL_io_ctx_t> m_io_contexts;

  /** Next operation ID */
  std::atomic<uint64_t> m_next_op_id{1};

  /** Helper functions */
  int allocate_file_index();
  void release_file_index(int index);
  bool validate_file_index(int index) const;
  uint64_t get_timestamp() const;
  WAL_io_ctx_t *create_io_context(IO_request req_type, uint32_t file_index, void *callback_data);
  void complete_io_context(uint64_t op_id, int result);
};

/** Global WAL I/O system instance */
extern WAL_IO_System *g_wal_io_system;

/** Initialize global WAL I/O system */
bool wal_io_system_init();

/** Shutdown global WAL I/O system */
void wal_io_system_shutdown();
