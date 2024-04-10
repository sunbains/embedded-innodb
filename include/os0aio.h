/***********************************************************************
Copyright 2024 Sunny Bains

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or Implied.
See the License for the specific language governing permissions and
limitations under the License.

***********************************************************************/

#pragma once

#include "innodb0types.h"

struct fil_node_t;

namespace aio {

constexpr ulint LOG = 0;
constexpr ulint READ = 1;
constexpr ulint WRITE = 2;

using Queue_id = ulint;

} // namespace aio

/** Types for aio operations @{ */
enum class IO_request {
  None,
  Async_read,
  Async_write,
  Async_log_read,
  Async_log_write,
  Sync_read,
  Sync_write,
  Sync_log_read,
  Sync_log_write,
};

struct IO_ctx {
  void validate() const noexcept;

  bool is_sync_request() const noexcept {
    switch (m_io_request) {
      case IO_request::Sync_read:
      case IO_request::Sync_write:
      case IO_request::Sync_log_read:
      case IO_request::Sync_log_write:
        return true;
      default:
        return false;
    }
  }

  bool is_log_request() const noexcept {
    switch (m_io_request) {
      case IO_request::Async_log_read:
      case IO_request::Async_log_write:
      case IO_request::Sync_log_read:
      case IO_request::Sync_log_write:
        return true;
      default:
        return false;
    }
  }

  bool is_read_request() const noexcept {
    switch (m_io_request) {
      case IO_request::Async_read:
      case IO_request::Async_log_read:
      case IO_request::Sync_read:
      case IO_request::Sync_log_read:
        return true;
      default:
        return false;
    }
  }

  bool is_shutdown() const noexcept {
    return m_io_request == IO_request::None && m_fil_node == nullptr && m_ret == 0;
  }

  void shutdown() noexcept {
    m_ret = 0;
    m_batch = false;
    m_fil_node = nullptr;
    m_msg = nullptr;
    m_io_request = IO_request::None;
  }

  /** IO file operation result. */
  int m_ret{-1}; 

  /**  Batch mode, we need to wake up the IO handler threads after posting the batch. */
  bool m_batch{};

  /** File meta data. */
  fil_node_t *m_fil_node{};

  /** User defined message. */
  void *m_msg{};

  /** Request type. */
  IO_request m_io_request{};
};

struct AIO {
  /**
  * @brief Initializes the asynchronous io system.
  * Note: The log uses a single thread for IO.
  *
  * @param[in] n_slots          Total number of slots per handler
  * @param[in] n_read_queues    Number of queues for read operations
  * @param[in] n_write_queues   Number of queues for write operations
  * 
  * @retval AIO* Pointer to the created instance. Call destroy() below to delete it.
  */
  static AIO* create(ulint n_slots, ulint n_read_queues, ulint n_write_queues) noexcept;

  /** Destroy an instance that was created using AIO::create()
   * @param[own] aio The instance to destroy.
   */
  static void destroy(AIO *&aio) noexcept;

  /** Destructor */
  virtual ~AIO() = default;

  /**
  * @brief Submit a request
  *
  * @param[in] io_ctx           Context of the i/o operation.
  * @param[in] buf              Buffer where to read or from which to write.
  * @param[in] n                Number of bytes to read or write.
  * @param[in] off              Least significant 32 bits of file offset where
  *                             to read or write.
  * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] virtual db_err submit(IO_ctx&& io_ctx, void *buf, ulint n, off_t off) noexcept = 0;

  /**
  * @brief Reaps requests that have completed. It's a blocking function.
  *
  * @param[in] queue_id         ID of the queue to reap from.
  * @param[out] io_ctx          Context of the i/o operation.
  * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] virtual db_err reap(aio::Queue_id queue_id, IO_ctx &io_ctx) noexcept = 0;

  /**
  * @brief Waits until there are no pending operations
  * 
  * @param[in] handler_id       ID of the handler to wait for.
  */
  virtual void wait_for_pending_ops(ulint handler_id) noexcept = 0;

  /**
  * @brief Closes/shuts down the IO sub-system and frees all the memory.
  */
  virtual void shutdown() noexcept = 0;

  /**
   * @return A string representation of the instance.
  */
  virtual std::string to_string() noexcept = 0;
};

inline const char* to_string(IO_request request) noexcept {
  switch(request) {
    case IO_request::None:
      return "None";
      break;
    case IO_request::Async_read:
      return "Async_read";
      break;
    case IO_request::Async_write:
      return "Async_write";
      break;
    case IO_request::Async_log_read:
      return "Async_log_read";
      break;
    case IO_request::Async_log_write:
      return "Async_log_write";
      break;
    case IO_request::Sync_read:
      return "Sync_read";
      break;
    case IO_request::Sync_write:
      return "Sync_write";
      break;
    case IO_request::Sync_log_read:
      return "Sync_log_read";
      break;
    case IO_request::Sync_log_write:
      return "Sync_log_write";
      break;
  }
  ut_error;
  return "Unknown IO request type";
}
