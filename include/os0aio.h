#pragma once

#include "innodb0types.h"

struct fil_node_t;

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
  * @param max_pending Maximum number of pending aio operations allowed per thread.
  * @param read_threads Number of reader threads.
  * @param writer_threads Number of writer threads.
  * @param sync_slots Number of slots in the sync AIO_array.
  * 
  * @retval AIO* Pointer to the created instance. Call destroy() below to delete it.
  */
  static AIO* create(ulint max_pending, ulint read_threads, ulint write_threads) noexcept;

  /** Destroy an instance that was created using AIO::create()
   * @param[own] aio The instance to destroy.
   */
  static void destroy(AIO *&aio) noexcept;

  /** Destructor */
  virtual ~AIO() = default;

  /**
  * @brief Submit a request
  *
  * @param io_ctx Context of the i/o operation.
  * @param buf Buffer where to read or from which to write.
  * @param n Number of bytes to read or write.
  * @param offset Least significant 32 bits of file offset where to read or write.
  * @return true if the request was queued successfully, false if failed.
  */
  [[nodiscard]] virtual db_err submit(IO_ctx&& io_ctx, void *buf, ulint n, off_t off) noexcept = 0;

  /**
  * @brief Does simulated aio. This function should be called by an i/o-handler thread.
  *
  * @param[in] segment The number of the segment in the aio arrays to wait for.
  * @param[out] io_ctx Context of the i/o operation.
  * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] virtual db_err reap(ulint segment, IO_ctx &io_ctx) noexcept = 0;

  /**
  * @brief Waits until there are no pending async writes, There can be other,
  * synchronous, pending writes.
  */
  virtual void wait_for_async_writes() noexcept = 0;

  /**
  * @brief Closes/shuts down the IO sub-system and frees all the memory.
  */
  virtual void shutdown() noexcept = 0;

  /**
  * @brief Prints info of the aio arrays.
  *
  * @param ib_stream Stream where to print.
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
