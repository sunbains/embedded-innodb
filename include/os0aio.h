#pragma once

#include "innodb0types.h"

struct fil_node_t;

/** This can be ORed to mode in the call of os_aio(...), if the caller wants
to post several i/o requests in a batch, and only after that wake the
i/o-handler thread; this has effect only in simulated aio */
constexpr ulint OS_AIO_SIMULATED_WAKE_LATER = 512;

/** Note: This value is from Windows NT, should be updated */
constexpr ulint OS_AIO_N_PENDING_IOS_PER_THREAD = 32;

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
  void validate() const {
    ut_a(m_file != -1);
    ut_a(m_name != nullptr);
  }

  bool is_sync_request() const {
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

  bool is_log_request() const {
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

  bool is_read_request() const {
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

  /** File name. */
  const char *m_name{};

  /** File handle. */
  os_file_t m_file{-1};

  /** Request type. */
  IO_request m_io_request{};

  /**  Batch mode, we need to wake up the IO handler threads after posting the batch. */
  bool m_batch{};

  /** File meta data. */
  fil_node_t *m_fil_node{};

  /** User defined message. */
  void *m_msg{};
};

/**
 * @brief Initializes the asynchronous io system.
 *
 * @param max_pending Maximum number of pending aio operations allowed per segment.
 * @param read_threads Number of reader threads.
 * @param writer_threads Number of writer threads.
 * @param sync_slots Number of slots in the sync AIO_array.
 */
void os_aio_init(ulint max_pending, ulint read_threads, ulint write_threads, ulint sync_slots);

/**
 * @brief Frees the asynchronous io system.
 */
void os_aio_free();

/**
 * @brief Requests an asynchronous i/o operation.
 *
 * @param io_ctx Context of the i/o operation.
 * @param buf Buffer where to read or from which to write.
 * @param n Number of bytes to read or write.
 * @param offset Least significant 32 bits of file offset where to read or write.
 * @return true if the request was queued successfully, false if failed.
 */
bool os_aio(IO_ctx&& io_ctx, void *buf, ulint n, off_t off);

/**
 * @brief Wakes up all async i/o threads so that they know to exit themselves in shutdown.
 */
void os_aio_wake_all_threads_at_shutdown();

/**
 * @brief Waits until there are no pending writes in os_aio_write_array.
 *        There can be other, synchronous, pending writes.
 */
void os_aio_wait_until_no_pending_writes();

/**
 * @brief Wakes up simulated aio i/o-handler threads if they have something to do.
 */
void os_aio_simulated_wake_handler_threads();

/**
 * @brief Posts a batch of reads and prefers an i/o-handler thread to handle them all at once later.
 *        You must call os_aio_simulated_wake_handler_threads later to ensure the threads are not left sleeping!
 */
void os_aio_simulated_put_read_threads_to_sleep();

/**
 * @brief Does simulated aio. This function should be called by an i/o-handler thread.
 *
 * @param[in] segment The number of the segment in the aio arrays to wait for.
 * @param[out] io_ctx Context of the i/o operation.
 * @return true if the aio operation succeeded.
 */
bool os_aio_simulated_handle(ulint segment, IO_ctx &io_ctx);

/**
 * @brief Validates the consistency of the aio system.
 *
 * @return true if the aio system is consistent, false otherwise.
 */
bool os_aio_validate(void);

/**
 * @brief Prints info of the aio arrays.
 *
 * @param ib_stream Stream where to print.
 */
void os_aio_print(ib_stream_t ib_stream);

/**
 * @brief Refreshes the statistics used to print per-second averages.
 */
void os_file_refresh_stats();

#ifdef UNIV_DEBUG
/**
 * @brief Checks that all slots in the system have been freed, that is, there are no pending io operations.
 *
 * @return true if all slots are free, false otherwise.
 */
bool os_aio_all_slots_free();
#endif /* UNIV_DEBUG */

/**
 * @brief Resets the variables.
 */
void os_aio_var_init();

/**
 * @brief Closes/shuts down the IO sub-system and frees all the memory.
 */
void os_aio_close();

inline const char* to_string(IO_request request) {
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
