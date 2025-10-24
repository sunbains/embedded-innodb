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
capabilities.
*****************************************************************************/

#pragma once

#include <sys/uio.h>

#include <atomic>
#include <vector>

#include "log0types.h"
#include "os0aio.h"
#include "ut0mpmcbq.h"

// Forward declarations
struct io_uring;

/* Function pointer type for io_uring deleter */
using Log_aio_deleter_func = void (*)(io_uring *) noexcept;

/* Alias for io_uring using unique_ptr with function pointer deleter */
using Log_aio = std::unique_ptr<io_uring, Log_aio_deleter_func>;

/** WAL file descriptor structure */
struct WAL_file {
  /** Default constructor */
  WAL_file() = default;

  /** Copy constructor */
  WAL_file(const WAL_file &rhs) noexcept : m_fd(rhs.m_fd), m_path(rhs.m_path), m_size(rhs.m_size), m_mtime(rhs.m_mtime) {
    m_pending_ops.store(rhs.m_pending_ops.load());
  }

  /** Move constructor */
  WAL_file(WAL_file &&rhs) noexcept : m_fd(rhs.m_fd), m_path(rhs.m_path), m_size(rhs.m_size), m_mtime(rhs.m_mtime) {
    m_pending_ops.store(rhs.m_pending_ops.load());
    rhs.m_fd = -1;
    rhs.m_size = 0;
    rhs.m_path.clear();
    rhs.m_mtime = 0;
    rhs.m_pending_ops.store(0);
  }

  /** Copy assignment operator */
  WAL_file &operator=(const WAL_file &rhs) noexcept {
    if (this != &rhs) {
      m_fd = rhs.m_fd;
      m_path = rhs.m_path;
      m_mtime = rhs.m_mtime;
      m_size = rhs.m_size;
      m_pending_ops.store(rhs.m_pending_ops.load());
    }
    return *this;
  }

  /** Move assignment operator */
  WAL_file &operator=(WAL_file &&rhs) noexcept {
    if (this != &rhs) {
      m_fd = rhs.m_fd;
      m_path = rhs.m_path;
      m_mtime = rhs.m_mtime;
      m_size = rhs.m_size;
      m_pending_ops.store(rhs.m_pending_ops.load());
      rhs.m_fd = -1;
      rhs.m_path.clear();
      rhs.m_mtime = 0;
      rhs.m_size = 0;
      rhs.m_pending_ops.store(0);
    }
    return *this;
  }

  /** File descriptor */
  int m_fd{-1};

  /** File path */
  std::string m_path{};

  /** Current file size in bytes */
  off_t m_size{0};

  /** File should not be larger than this size. Ideally m_size should always be equal to m_max_size. */
  off_t m_max_size{0};

  /** Number of pending I/O operations */
  std::atomic<uint32_t> m_pending_ops{0};

  /** File creation/modification time for validation */
  time_t m_mtime{0};
};

/** Scatter-gather I/O vector */
using WAL_iovec_t = struct iovec;

/** WAL I/O completion context */
struct WAL_io_ctx_t {
  /** Default constructor */
  WAL_io_ctx_t() = default;

  /** Copy constructor */
  WAL_io_ctx_t(const WAL_io_ctx_t &rhs) noexcept
      : m_req_type(rhs.m_req_type),
        m_callback(rhs.m_callback),
        m_file(rhs.m_file),
        m_bytes_requested(rhs.m_bytes_requested),
        m_start_time(rhs.m_start_time) {
    m_result.store(rhs.m_result.load());
    m_completed.store(rhs.m_completed.load());
  }

  /** Move constructor */
  WAL_io_ctx_t(WAL_io_ctx_t &&rhs) noexcept
      : m_req_type(rhs.m_req_type),
        m_callback(rhs.m_callback),
        m_file(rhs.m_file),
        m_bytes_requested(rhs.m_bytes_requested),
        m_start_time(rhs.m_start_time) {
    m_result.store(rhs.m_result.load());
    m_completed.store(rhs.m_completed.load());
    rhs.m_callback = nullptr;
  }

  /** Copy assignment operator */
  WAL_io_ctx_t &operator=(const WAL_io_ctx_t &rhs) noexcept {
    if (this != &rhs) {
      m_req_type = rhs.m_req_type;
      m_callback = rhs.m_callback;
      m_file = rhs.m_file;
      m_start_time = rhs.m_start_time;
      m_bytes_requested = rhs.m_bytes_requested;
      m_result.store(rhs.m_result.load());
      m_completed.store(rhs.m_completed.load());
    }
    return *this;
  }

  /** Move assignment operator */
  WAL_io_ctx_t &operator=(WAL_io_ctx_t &&rhs) noexcept {
    if (this != &rhs) {
      m_req_type = rhs.m_req_type;
      m_callback = rhs.m_callback;
      m_file = rhs.m_file;
      m_start_time = rhs.m_start_time;
      m_bytes_requested = rhs.m_bytes_requested;
      m_result.store(rhs.m_result.load());
      m_completed.store(rhs.m_completed.load());
      rhs.m_callback = nullptr;
    }
    return *this;
  }

  /** I/O request type */
  IO_request m_req_type{IO_request::None};

  /** Completion callback, the argument is the result code */
  std::function<void(WAL_io_ctx_t *)> m_callback;

  /** File */
  WAL_file *m_file{nullptr};

  /** Number of bytes requested */
  uint32_t m_bytes_requested{0};

  /** Operation start time for latency tracking */
  uint64_t m_start_time{0};

  /** Result code */
  std::atomic<int> m_result{0};

  /** Completion flag */
  std::atomic<bool> m_completed{false};
};

/** Simplified WAL I/O system using io_uring */
struct WAL_IO_System {
  using File_pool = Bounded_channel<WAL_file *>;
  using Context_pool = Bounded_channel<WAL_io_ctx_t *>;
  using WAL_io_vecs = std::vector<WAL_iovec_t>;

  /** Constructor
   *
   * @note Both parameters should be a power of 2, there is an assertion that checks this.
   *
   * @param[in] n_files number of WAL files to use
   * @param[in] n_io_ctxs number of I/O contexts to use
   */
  explicit WAL_IO_System(size_t n_files, size_t n_io_ctxs) noexcept;

  /** Destructor */
  ~WAL_IO_System() noexcept;

  /** Initialize the WAL I/O system
   * @param[in] queue_depth io_uring queue depth
   * @return true on success
   */
  [[nodiscard]] bool initialize(uint32_t queue_depth) noexcept;

  /** Shutdown the WAL I/O system */
  void shutdown() noexcept;

  /** Open a WAL file
   * @param[in] file_path path to the WAL file
   * @param[in] file_size expected file size (0 for auto-detect)
   * @return file on success, nullptr on error
   */
  [[nodiscard]] WAL_file *open_wal_file(const std::string &file_path, off_t file_size) noexcept;

  /** Create a new WAL file
   * @param[in] file_path path to the WAL file
   * @param[in] file_size expected file size (0 for auto-detect)
   * @return file on success, nullptr on error
   */
  [[nodiscard]] WAL_file *create_wal_file(const std::string &file_path, off_t file_size) noexcept;

  /** Close a WAL file
   * @param[in] file file returned by open_wal_file
   * @return true on success
   */
  [[nodiscard]] bool close_wal_file(WAL_file *file) noexcept;

  /** Submit synchronous read operation
   * @param[in] file file returned by open_wal_file
   * @param[out] buffer read buffer
   * @param[in] length number of bytes to read
   * @param[in] offset file offset
   * @return number of bytes read, -1 on error
   */
  [[nodiscard]] ssize_t sync_read(WAL_file *file, void *buffer, size_t length, off_t offset) noexcept;

  /** Submit synchronous write operation
   * @param[in] file file returned by open_wal_file
   * @param[in] buffer write buffer
   * @param[in] length number of bytes to write
   * @param[in] offset file offset
   * @return number of bytes written, -1 on error
   */
  [[nodiscard]] ssize_t sync_write(WAL_file *file, const void *buffer, size_t length, off_t offset) noexcept;

  /** Submit asynchronous read operation
   * @param[in] file file returned by open_wal_file
   * @param[out] buffer read buffer
   * @param[in] length number of bytes to read
   * @param[in] offset file offset
   * @param[in] callback_data data passed to completion handler
   *
   * @return I/O context on success, nullptr on error
   */
  [[nodiscard]] WAL_io_ctx_t *async_read(
    WAL_file *file, void *buffer, size_t length, off_t offset, std::function<void(WAL_io_ctx_t *)> callback
  ) noexcept;

  /** Submit asynchronous write operation
   * @param[in] file file returned by open_wal_file
   * @param[in] buffer write buffer
   * @param[in] length number of bytes to write
   * @param[in] offset file offset
   * @param[in] callback_data data passed to completion handler
   *
   * @return I/O context on success, nullptr on error
   */
  [[nodiscard]] WAL_io_ctx_t *async_write(
    WAL_file *file, const void *buffer, size_t length, off_t offset, std::function<void(WAL_io_ctx_t *)> callback
  ) noexcept;

  /** Submit scatter-gather read operation
   * @param[in] file file returned by open_wal_file
   * @param[in] iovec array of I/O vectors
   * @param[in] offset start reading from this file offset
   * @param[in] callback_data data passed to completion handler
   *
   * @return I/O context on success, nullptr on error
   */
  [[nodiscard]] WAL_io_ctx_t *async_scatter_read(
    WAL_file *file, const WAL_io_vecs &iovecs, off_t offset, std::function<void(WAL_io_ctx_t *)> callback
  ) noexcept;

  /** Submit scatter-gather write operation
   * @param[in] file file returned by open_wal_file
   * @param[in] iovecs array of I/O vectors
   * @param[in] offset start writing to this file offset
   * @param[in] callback_data data passed to completion handler
   *
   * @return I/O context on success, nullptr on error
   */
  [[nodiscard]] WAL_io_ctx_t *async_gather_write(
    WAL_file *file, const WAL_io_vecs &iovecs, off_t offset, std::function<void(WAL_io_ctx_t *)> callback
  ) noexcept;

  /** Flush/sync a WAL file to disk (synchronous)
   * @param[in] file file returned by open_wal_file
   *
   * @return true on success
   */
  [[nodiscard]] bool flush_file(WAL_file *file) noexcept;

  /** Submit asynchronous flush/sync operation
   * @param[in] file file returned by open_wal_file
   * @param[in] callback_data data passed to completion handler
   *
   * @return I/O context on success, nullptr on error
   */
  [[nodiscard]] WAL_io_ctx_t *async_flush(WAL_file *file, std::function<void(WAL_io_ctx_t *)> callback) noexcept;

  /** Process completion events
   * @param[in, out] ctx I/O context that was passed to wait_for_completion, you can pass nullptr.
   * @param[in] max_events maximum number of events to process
   * @param[in] timeout_ms timeout in milliseconds (0 for non-blocking)
   *
   * @return number of events processed, -1 on error
   */
  [[nodiscard]] int process_completions(WAL_io_ctx_t *&ctx, uint32_t max_events = 32, uint32_t timeout_ms = 0) noexcept;

  /** Wait for a specific operation to complete
   *
   * @note The I/O context is released after completion and the pointer is set to nullptr.
   *
   * @param[in] ctx I/O context returned by async operations
   * @param[in] timeout_ms timeout in milliseconds
   *
   * @return true if operation completed successfully
   */
  [[nodiscard]] bool wait_for_completion(WAL_io_ctx_t *&ctx, uint32_t timeout_ms = 5000) noexcept;

  /** Get the current timestamp */
  [[nodiscard]] uint64_t get_timestamp() const noexcept;

  /** I/O statistics */
  struct {
    std::atomic<uint64_t> m_total_reads{0};
    std::atomic<uint64_t> m_total_writes{0};
    std::atomic<uint64_t> m_total_bytes_read{0};
    std::atomic<uint64_t> m_total_bytes_written{0};
    std::atomic<uint64_t> m_total_sync_ops{0};
    std::atomic<uint64_t> m_total_async_ops{0};
    std::atomic<uint64_t> m_total_errors{0};
  } stats;

  /**
   * Check if a file exists
   * @param[in] file_path path to the file
   * @return true if the file exists
   */
  static bool file_exists(std::string_view file_path) noexcept;

  /**
   * Check if a file is read and write compatible
   * @param[in] file_path path to the file
   * @return true if the file is read and write compatible
   */
  static bool is_file_rw_compatible(std::string_view file_path) noexcept;

  /** Get the size of a file
   * @param[in] file_path path to the file
   * @return file size in bytes, -1 on error
   */
  static off_t get_file_size(std::string_view file_path) noexcept;

 private:
  /** Allocate a file
   *
   * @return allocated file, nullptr if no free files are available
  */
  [[nodiscard]] WAL_file *allocate_file() noexcept;

  /** Release a file
   *
   * @param[in, out] file file to release
   * @note The file is set to nullptr after release
  */
  void release_file(WAL_file *&file) noexcept;

  /** Create an I/O context
   *
   * @param[in] req_type I/O request type
   * @param[in] file file returned by open_wal_file
   * @param[in] callback completion callback
   *
   * @return I/O context on success, nullptr on error
  */
  [[nodiscard]] WAL_io_ctx_t *create_io_context(
    IO_request req_type, WAL_file *file, std::function<void(WAL_io_ctx_t *)> callback
  ) noexcept;

  /** Complete an I/O context
   *
   * @note The I/O context is released after completion and the pointer is set to nullptr.
   *
   * @param[in, out] ctx I/O context that was passed to wait_for_completion, you can pass nullptr.
   * @param[in] result result code
   */
  void complete_io_context(WAL_io_ctx_t *&ctx, int result) noexcept;

  /** Release an I/O context
   *
   * @param[in, out] ctx I/O context
   * @note The I/O context is set to nullptr after release
   */
  void release_io_context(WAL_io_ctx_t *&ctx) noexcept;

 private:
  /** io_uring instance (managed by unique_ptr) */
  Log_aio m_ring;

  /** Initialization flag */
  std::atomic<bool> m_initialized{false};

  /** Shutdown flag */
  std::atomic<bool> m_shutdown{false};

  /** WAL files array */
  std::vector<WAL_file> m_files;

  /** Free file pool, the files are from m_files. */
  File_pool m_free_files;

  /** I/O contexts to use. */
  std::vector<WAL_io_ctx_t> m_io_contexts;

  /** Free I/O context pool, the contexts are from m_io_contexts. */
  Context_pool m_free_contexts;

  /** Next operation ID */
  std::atomic<uint64_t> m_next_op_id{1};
};

/** Global WAL I/O system instance */
extern WAL_IO_System *g_wal_io_system;

/** Initialize global WAL I/O system */
bool wal_io_system_init() noexcept;

/** Shutdown global WAL I/O system */
void wal_io_system_shutdown() noexcept;
