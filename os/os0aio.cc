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

#include <errno.h>

#include <array>
#include <vector>

#include <liburing.h>

#include "os0aio.h"
#include "fil0types.h"
#include "os0file.h"
#include "ut0mem.h"
#include "os0sync.h"
#include "ut0byte.h"
#include "ut0logger.h"
#include "ut0mpmcbq.h"

AIO *srv_aio{};

namespace aio {

struct Stats {
  std::string to_string() const {
    return std::format(
      "sqes: {}, cqes: {}, total: {}, partial = {{ reqs: {}, data: {} }}, retries: {{ sqe: {}, cqe: {} }}",
      m_n_sqes.load(), m_n_cqes.load(),
      m_total.load(),
      m_partial_ops.load(), m_partial_data.load(),
      m_sqe_eintrs.load(), m_cqe_eintrs.load());
  }

  /** Total number of SQEs submitted (including partial). */
  std::atomic<uint64_t> m_n_sqes{};

  /** Total number of CQEs reaple (including partial), */
  std::atomic<uint64_t> m_n_cqes{};

  /** Total number of bytes read/written. */
  std::atomic<uint64_t> m_total{};

  /** Total number of partial SQEs submitted, */
  std::atomic<uint64_t> m_partial_ops{};

  /** Tootal number of partial bytes read/written. */
  std::atomic<uint64_t> m_partial_data{};

  /** Total number of SQE EINTR errors. */
  std::atomic<uint64_t> m_sqe_eintrs{};

  /** Total number of CQE EINTR errors. */
  std::atomic<uint64_t> m_cqe_eintrs{};
};


/** The request buffer and lengthh. */
struct Buffer {
  byte *m_ptr{};
  uint32_t m_len{};
};

/** The request context for an asynchronous i/o operation.
 */
struct Slot {
  /** Buffer pointer and length subbmitted to the kernel. */
  Buffer m_request{};

  /** Total number of bytes read or written so far. */
  uint32_t m_len{};

  /** File offset in bytes */
  off_t m_off{};

  /** true if this slot is reserved */
  IF_DEBUG(bool m_reserved{};)

  /** The IO context */
  IO_ctx m_io_ctx{};
};

using Slots_pool = Bounded_channel<Slot*>;

/** There are three different types of requests:
 * 1. Log requests
 * 2. Read requests
 * 3. Write requests
 * 
 * Each type is managed by handler. Log requests
 * are currently configured to use a single queue.
 * 
 * Both read and write requests can have multiple queues.
 * 
 * Each handler manages a pool of slots that is shared by the
 * queues. For each request, a slot is reserved from the free
 * pool and submitted to the io_uring infrastructure. When the
 * request is completed, the slot is returned to the free pool.
 * 
 * The handler has two events: m_not_full and m_is_empty. The
 * m_not_full event is set when there are free slots in the
 * handler. The m_is_empty event is set when there are no free
 * slots in the free pool.
 * 
 * The io_uring completion queue is polled by the queue reap threads.
 * These queue reap threads are created by the callers of AIO::reap().
 * So the caller must ensure that reap is called with the correct
 * queue id.
 * 
 * The queues are suspended when there are no available
 * completion entries in the io_uring queue.
 * 
 * The queue id argument to AIO::reap() is the ordinal number
 * of the queue in the "global" queue array. The "global" queue
 * array is:   LOG + READ + WRITE
 * 
 * e.g., if you have 1 log queue, 3 read queues and 2 write queues
 * then the queue id of the 1st write queue will be:
 *  
 *                 = 1 + 3 + 0 = 4
 * 
 * FIXME: In the next refactor iteration remove the requirement to pass
 * the queue ID to AIO::reap().
 */
struct Handler {

  struct Queue;

  /** Constructor
   * @param[in] id Id of the handler
   * @param[in] n_slots Number of slots in the handler
   * @param[in] n_queues Number of queues per handler
   */
  explicit Handler(ulint id, size_t n_slots, size_t n_queues) noexcept;

  /* Destructor */
  ~Handler() noexcept;

  /* @return true if it's a log handler. */
  [[nodiscard]] bool is_log() const noexcept {
    return m_id == LOG;
  }

  /* @return true if it's a read handler. */
  [[nodiscard]] bool is_read() const noexcept {
    return m_id == READ;
  }

  /* @return true if it's a write handler. */
  [[nodiscard]] bool is_write() const noexcept {
    return m_id == WRITE;
  }

  /**
  * Validates the consistency of this handler.
  *
  * @return true if ok
  */
  [[nodiscard]] bool validate() const noexcept;

  /** Get the queue for submitting a request
   * 
   * @return the queue for the AIO
  */
  [[nodiscard]] Queue *get_queue_for_submit() noexcept;

  /** Free the slot.
  * @param[in] slot Slot to free.
  */
  void mark_as_free(Slot* slot) noexcept;

  /** Wake up the queue queues, we are shutting down. */
  void shutdown() noexcept;

  /** @return the handlers state as a string. */
  [[nodiscard]] std::string to_string() const;

  /**
  * Creates a handler with the given number of slots and queues.
  *
  * @param[in] id               Id of the handler
  * @param n_slots              Number of slots.
  * @param n_queues            Number of queues in the handler 
  * @return own: handler instance.
  */
  [[nodiscard]] static Handler *create(ulint id, ulint n_slots, ulint n_queues) noexcept;

  /** Destoy a Handler instance.
   * @param[in,own] Handler instance to destroy.
   */
  static void destroy(Handler *handler) noexcept;

  /** Type of the handler. */
  ulint m_id{ULINT_UNDEFINED};

  /** The event which is set to the signaled state when there are
   * slots available in this handler. */
  Cond_var* m_not_full{};

  /** The event which is set to the signaled state when there are
   * no free slots in this handler. */
  Cond_var* m_is_empty{};

  /** Number of  reserved slots in the handler */
  std::atomic<ulint> m_n_reserved{};

  /** Slots to use for submitting/reapling requwests. */
  std::vector<Slot> m_slots{};

  /** The free slot pool */
  Slots_pool *m_free_pool{};

  /** io_uring queues of this handler. */
  std::vector<Queue *> m_queues{};
};

/** Manages a single io_uring, for submitting and reaping requests. */
struct Handler::Queue {
  /** Constructor 
  * @param[in] handler The owning handler of this queue
  * @param[in] id Id of the queue
  * @param[in] queue_size Size of the io_uring queue
  */
  Queue(Handler *handler, ulint id, ulint queue_size) noexcept
    : m_handler(handler),
      m_id(id) {

    if (auto ret = io_uring_queue_init(queue_size, &m_iouring, 0); ret < 0) {
      log_fatal("Initializing io_uring queue failed: " + std::to_string(ret));
    }
  }

  Queue(Queue &&) = delete;
  Queue(const Queue &) = delete;
  Queue&operator=(Queue &&) = delete;
  Queue&operator=(const Queue &) = delete;

  ~Queue() noexcept {
    io_uring_queue_exit(&m_iouring);
  }

  /** Submit an asynchronous IO request.
   * 
   * @param[in,out] slot Slot to submit
   * 
   * @return DB_SUCCESS or error code.
  */
  db_err submit(Slot *slot) noexcept ;

  /** Wait for completed requests and return the IO context.
   * 
   * @param[out] io_ctx IO context
   * 
   *  @return DB_SUCCESS or error code. */
  db_err reap(IO_ctx &io_ctx) noexcept;

  /**
  * Reserve a slot from the free pool
  * @param[in] io_ctx IO context.
  * @param[in] ptr Pointer to the buffer for IO
  * @param[in] len Length of the buffer (to read/write)
  * @param[in] off Offset in the file.
  * @return an IO slot instance. */
  [[nodiscard]] Slot *reserve_slot(const IO_ctx &io_ctx, void *ptr, uint32_t len, off_t off) noexcept;

  /** Shutdown the queue. */
  void shutdown() {
    ut_a(!m_shutdown.load());
    ut_a(m_pending_slots.load() == 0);
    ut_a(m_handler->m_free_pool->full());
    ut_a(m_handler->m_n_reserved.load() == 0);

    m_shutdown.store(true);

    io_uring_sqe *sqe = io_uring_get_sqe(&m_iouring);
    ut_a(sqe != nullptr);

    io_uring_prep_cancel(sqe, this, 0);

    for (;;) {
      const auto ret = io_uring_submit(&m_iouring);

      switch(ret) {
        case 1:
          m_stats.m_n_sqes.fetch_add(1, std::memory_order_relaxed);
          return;
        case -EINTR:
        case -EAGAIN:
          m_stats.m_sqe_eintrs.fetch_add(1, std::memory_order_relaxed);
          continue;
        default:
          log_fatal("io_uring_submit failed: " + std::to_string(ret));
      }
    }
  }

  /** @return the queue's state as a string. */
  [[nodiscard]] std::string to_string() const {
    return "stats: { " + m_stats.to_string() + " }";
  }

  /** We are shutting down. */
  std::atomic<bool> m_shutdown{};

  /* For collecting operational statistics. */
  Stats m_stats{};

  /** Number of pending AIO slots. */
  std::atomic<ulint> m_pending_slots{};

  /** Parent handler. */
  Handler *m_handler{};

  /** Local queue id. */
  ulint m_id{ULINT_UNDEFINED};

  /** io_uring instance  use for AIO. */
  io_uring m_iouring{};
};

struct Impl : public AIO {
  /** Constructor.
   * @param[in] n_slots Total number of slots for all queues
   * @param[in] read_queues Number of reader queues.
   * @param[in] writer_queues Number of writer queues.
   */ 
  Impl(ulint n_slots, ulint read_queues, ulint write_queues) noexcept
    : m_n_queues(read_queues + write_queues + 1) {
    m_handlers[LOG] = Handler::create(LOG, n_slots, 1);
    m_handlers[READ] = Handler::create(READ, n_slots, read_queues);
    m_handlers[WRITE] = Handler::create(WRITE, n_slots, write_queues);
  }

  ~Impl() noexcept {
    Handler::destroy(m_handlers[LOG]);
    Handler::destroy(m_handlers[READ]);
    Handler::destroy(m_handlers[WRITE]);
  }

  /**
  * @brief Submit a request
  *
  * @param io_ctx Context of the i/o operation.
  * @param buf Buffer where to read or from which to write.
  * @param n Number of bytes to read or write.
  * @param offset Least significant 32 bits of file offset where to read or write.
  * @return true if the request was queued successfully, false if failed.
  */
  [[nodiscard]] virtual db_err submit(IO_ctx&& io_ctx, void *buf, ulint n, off_t off) noexcept;

  /**
  * @brief Reap the completed request from io_uring.
  *
  * @param[in] queue_id The ID of the queue that is calling this function.
  * @param[out] io_ctx Context of the i/o operation.
  * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] virtual db_err reap(ulint queue_id, IO_ctx &io_ctx) noexcept;

  /**
  * @brief Waits until there are no pending async operations.
  */
  virtual void wait_for_pending_ops(ulint handler_idd) noexcept;

  /**
  * @brief Closes/shuts down the AIO sub-system and frees all the memory.
  */
  virtual void shutdown() noexcept;

  /**
  * @brief Prints info of the aio arrays.
  *
  * @param ib_stream Stream where to print.
  */
  virtual std::string to_string() noexcept;

  /** Get the AIO type from the IO context.
   * 
   * @param[in] io_ctx  IO context
   * 
   * @return one of LOG, READ or WRITE
  */
  [[nodiscard]] ulint get_type(const IO_ctx& io_ctx) noexcept;

  /** 
   * Get the handler for the given queue_id
   * 
   * @param[in] queue_id The queue ordinal number across all handlers
   * 
   * @return the handler for the given queue id
  */
  [[nodiscard]] Handler *get_handler(ulint queue_id) noexcept;

  /** 
   * Get the queue for the given queue id
   * 
   * @param[in] queue_id The queue ordinal number across all handlers
   * 
   * @return the queue for the given queue id
  */
  [[nodiscard]] Handler::Queue *get_queue(ulint queue_id) noexcept;

  /** 
   * Get the total number of queues.
   * 
   * @return the total number of queues
  */
  [[nodiscard]] ulint get_total_queues() const  noexcept{
    return m_n_queues;
  }

  Impl(Impl&&) = delete;
  Impl(const Impl&) = delete;
  Impl& operator=(Impl&&) = delete;
  Impl& operator=(const Impl&) = delete;

  using Array = std::array<Handler *, WRITE + 1>;

  /** true if shutdown has been called. */
  bool m_shutdown{};

  /** The handler instances. */
  Array m_handlers;

  /** Total number of queues/queues. */
  std::size_t m_n_queues;
};

Handler::Handler(ulint id, size_t n_slots, size_t n_queues) noexcept
  : m_id(id) {
  m_not_full = Cond_var::create(nullptr);
  m_is_empty = Cond_var::create(nullptr);

  m_slots.resize(n_slots);

  for (size_t i = 0; i < n_queues; i++) {
    auto queue = new (ut_new(sizeof(Queue))) Queue(this, i, n_slots);
    ut_a(queue != nullptr);
    m_queues.push_back(queue);
  }

  m_is_empty->set();

  m_n_reserved.store(0, std::memory_order_release);

  m_free_pool = new (ut_new(sizeof(Slots_pool))) Slots_pool(n_slots);

  for (auto &slot : m_slots) {
    auto success = m_free_pool->enqueue(&slot);
    ut_a(success);
  }
}

Handler::~Handler() noexcept {
  for (auto queue : m_queues) {
    call_destructor(queue);
    ut_delete(queue);
  }
  m_queues.clear();

  Cond_var::destroy(m_not_full);
  Cond_var::destroy(m_is_empty);
}

std::string Handler::to_string() const {
  std::ostringstream os{};

  switch (m_id) {
    case LOG:
      os << "log:";
      break;
    case READ:
      os << "read:";
      break;
    case WRITE:
      os << "write:";
      break;
    default:
      ut_error;
  }

  os << " slots: " << m_slots.size() << ", queues: [ ";

  std::for_each(m_queues.begin(), m_queues.end() - 1,
    [&os](const Queue *queue) {
      os << queue->to_string() << ", ";
  });

  os << m_queues.back()->to_string() << " ]";

  return os.str();
}

Handler::Queue *Handler::get_queue_for_submit() noexcept {
  Queue *submit_queue{};

  for (auto queue : m_queues) {
    if (!submit_queue) {
      submit_queue = queue;
    } else if (queue->m_pending_slots.load(std::memory_order_relaxed) <
               submit_queue->m_pending_slots.load(std::memory_order_relaxed)) {
      submit_queue = queue;
    }
  }

  return submit_queue;
}

void Handler::shutdown() noexcept {
  for (auto &queue : m_queues) {
    if (!queue->m_shutdown.load(std::memory_order_acquire)){
      queue->shutdown();
    }
  }
}

bool Handler::validate() const noexcept {
  return true;
}

void Handler::destroy(Handler *handler) noexcept {
  call_destructor(handler);
  ut_delete(handler);
}

Handler *Handler::create(ulint id, ulint n_slots, ulint n_queues) noexcept {
  ut_a(n_slots > 0);
  ut_a(n_queues > 0);

  return new (ut_new(sizeof(Handler))) Handler(id, n_slots, n_queues);
}

void Handler::mark_as_free(Slot *slot) noexcept {
  ut_ad(slot->m_reserved);
  ut_d(slot->m_reserved = false);

  auto success = m_free_pool->enqueue(slot);
  ut_a(success);

  m_n_reserved.fetch_sub(1, std::memory_order_relaxed);

  if (m_n_reserved.load() == m_slots.capacity() - 1) {
    m_not_full->set();
  }

  if (m_n_reserved.load() == 0) {
    m_is_empty->set();
  }
}

Slot *Handler::Queue::reserve_slot(const IO_ctx &io_ctx, void *ptr, uint32_t len, off_t off) noexcept {
  for (;;) {
    if (m_handler->m_n_reserved.load(std::memory_order_relaxed) == m_handler->m_slots.capacity()) {

      /* If the handler queues are suspended, wake them
      so that we get more slots */

      m_handler->m_not_full->wait(0);

    } else {
      Slot *slot = nullptr;

      while (!m_handler->m_free_pool->dequeue(slot)) {
        std::this_thread::yield();
      }

      ut_ad(!slot->m_reserved);

      auto io_ctx_copy = io_ctx;

      m_handler->m_n_reserved.fetch_add(1, std::memory_order_relaxed);

      if (m_handler->m_n_reserved.load() == 1) {
        m_handler->m_is_empty->reset();
      }

      if (m_handler->m_n_reserved.load() == m_handler->m_slots.capacity()) {
        m_handler->m_not_full->reset();
      }

      slot->m_len = 0;
      slot->m_off = off;
      ut_ad(slot->m_reserved = true);
      slot->m_io_ctx = std::move(io_ctx_copy);
      slot->m_request = {static_cast<byte*>(ptr), len};

      return slot;
    }
  }

  ut_error;
  return nullptr;
}

db_err Handler::Queue::submit(Slot *slot) noexcept {
  ut_a(!m_shutdown.load(std::memory_order_acquire));
  ut_ad(m_handler->m_n_reserved.load(std::memory_order_acquire) > 0);

  auto sqe = io_uring_get_sqe(&m_iouring);

  if (sqe == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto &buffer = slot->m_request;
  auto fh{slot->m_io_ctx.m_fil_node->m_fh};

  /* Do the i/o with ordinary, synchronous i/o functions: */
  if (slot->m_io_ctx.is_read_request()) {
    io_uring_prep_read(sqe, fh, buffer.m_ptr, buffer.m_len, slot->m_off);
  } else {
    io_uring_prep_write(sqe, fh, buffer.m_ptr, buffer.m_len, slot->m_off);
  }

  io_uring_sqe_set_data64(sqe, uintptr_t(slot));

  m_pending_slots.fetch_add(1, std::memory_order_relaxed);

  m_stats.m_n_sqes.fetch_add(1, std::memory_order_relaxed);

  for (;;) {
    const auto ret = io_uring_submit(&m_iouring);

    switch(ret) {
      case 1:
        return DB_SUCCESS;
      case -EINTR:
      case -EAGAIN:
        m_stats.m_sqe_eintrs.fetch_add(1);
        continue;
      default:
        log_fatal("io_uring_submit failed: " + std::to_string(ret));
    }
    return DB_ERROR;
  }
} 

db_err Handler::Queue::reap(IO_ctx &io_ctx) noexcept {
  ut_ad(m_handler->validate());

  io_uring_cqe *cqe;

  for (;;) {
    do {
      const int ret = io_uring_wait_cqe(&m_iouring, &cqe);

      switch (ret) {
        case 0:
          m_stats.m_n_cqes.fetch_add(1, std::memory_order_relaxed);
          break;
        case -EINTR:
        case -EAGAIN:
          m_stats.m_cqe_eintrs.fetch_add(1, std::memory_order_relaxed);
          continue;
        default:
          log_fatal("io_uring_wait_cqe failed: " + std::to_string(ret));
      }
    } while (cqe == nullptr);

    if (m_shutdown.load()) {
      io_uring_cqe_seen(&m_iouring, cqe);
      io_ctx.shutdown();
      return DB_SUCCESS;
    }

    auto slot = reinterpret_cast<Slot*>(io_uring_cqe_get_data64(cqe));

    ut_a(cqe->res != -1);
    ut_ad(slot->m_reserved);
    ut_a(decltype(cqe->res)(slot->m_request.m_len) >= cqe->res);

    slot->m_off += cqe->res;
    slot->m_len += cqe->res;
    slot->m_request.m_len -= cqe->res;
    slot->m_request.m_ptr += cqe->res;

    m_stats.m_total.fetch_add(cqe->res, std::memory_order_relaxed);

    if (slot->m_request.m_len > 0) {
      m_stats.m_partial_ops.fetch_add(1, std::memory_order_relaxed);
      m_stats.m_partial_data.fetch_add(slot->m_request.m_len, std::memory_order_relaxed);
      /* It was a partial read/write, try and read the remaining bytes. */
      submit(slot);
    } else {
      slot->m_io_ctx.m_ret = cqe->res;

      auto n = m_pending_slots.fetch_sub(1, std::memory_order_relaxed);
      ut_a(n > 0);

      io_uring_cqe_seen(&m_iouring, cqe);

      io_ctx = slot->m_io_ctx;


      m_handler->mark_as_free(slot);


      return DB_SUCCESS;
    }
  }
}

ulint Impl::get_type(const IO_ctx& io_ctx) noexcept {
  if (io_ctx.is_log_request()) {
    return LOG;
  } else if (io_ctx.is_read_request()) {
    return READ;
  } else {
    return WRITE;
  } 
  ut_error;
  return ULINT_UNDEFINED;
}

Handler *Impl::get_handler(ulint queue_id) noexcept {
  ut_a(queue_id < get_total_queues());

  if (queue_id == 0) {
    return m_handlers[LOG];
  } else if (queue_id < m_handlers[READ]->m_queues.size() + 1) {
    return m_handlers[READ];
  } else {
    return m_handlers[WRITE];
  }
}

Handler::Queue *Impl::get_queue(ulint queue_id) noexcept {
  ut_a(queue_id < get_total_queues());
  auto handler = get_handler(queue_id);

  if (queue_id == 0) {
    return handler->m_queues[0];
  } else if (queue_id < m_handlers[READ]->m_queues.size() + 1) {
    return handler->m_queues[queue_id - 1];
  } else {
    return handler->m_queues[queue_id - m_handlers[READ]->m_queues.size() - 1];
  }
}

db_err Impl::submit(IO_ctx&& io_ctx, void *ptr, ulint n, off_t off) noexcept {
  io_ctx.validate();

  ut_ad(n > 0);
  ut_ad(ptr != nullptr);
  ut_ad(n % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(off % IB_FILE_BLOCK_SIZE == 0);

  if (io_ctx.is_sync_request()) {
    auto fh{io_ctx.m_fil_node->m_fh};

    if (io_ctx.is_read_request()) {
      return os_file_read(fh, ptr, n, off) ? DB_SUCCESS : DB_ERROR;
    } else {
      auto name{io_ctx.m_fil_node->m_file_name};

      return os_file_write(name, fh, ptr, n, off) ? DB_SUCCESS : DB_ERROR;
    }
  }
  auto handler = m_handlers[get_type(io_ctx)];
  auto queue = handler->get_queue_for_submit();
  auto slot = queue->reserve_slot(io_ctx, ptr, n, off);

  queue->submit(slot);

  return DB_SUCCESS;
}

std::string Impl::to_string() noexcept {
  std::ostringstream os{};

  os << "handler = [";

  for (auto handler : m_handlers) {
    os << handler->to_string();

    if (handler->m_id < WRITE) {
      os << ", ";
    }
  }

  os << "]";

  return os.str();
}

db_err Impl::reap(ulint handler_id, IO_ctx &io_ctx) noexcept {
  return get_queue(handler_id)->reap(io_ctx);
}

void Impl::wait_for_pending_ops(ulint handler_id) noexcept {
  m_handlers[handler_id]->m_is_empty->wait(0);
}

void Impl::shutdown() noexcept {
  if (!m_shutdown) {
    for (auto handler : m_handlers) {
      handler->shutdown();
    }

    log_info(to_string())
    m_shutdown = true;
  }
}

} // namespace aio

void IO_ctx::validate() const noexcept {
  ut_a(m_fil_node->m_fh != -1);
  ut_a(m_fil_node->m_file_name != nullptr);
}

AIO* AIO::create(ulint max_slots, ulint read_queues, ulint write_queues) noexcept {
  ut_a(read_queues > 0);
  ut_a(write_queues > 0);

  return new (ut_new(sizeof(aio::Impl))) aio::Impl(max_slots, read_queues, write_queues);
}

void AIO::destroy(AIO *&aio) noexcept {
  call_destructor(aio);
  ut_delete(aio);
  aio = nullptr;
}
