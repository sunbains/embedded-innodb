#include <errno.h>

#include <array>
#include <vector>

#include <liburing.h>
#include <BS_thread_pool.hpp>

#include "os0aio.h"
#include "fil0types.h"
#include "os0file.h"
#include "ut0mem.h"
#include "os0sync.h"
#include "ut0byte.h"
#include "ut0logger.h"
#include "ut0mpmcbq.h"

constexpr ulint AIO_LOG = 0;
constexpr ulint AIO_READ = 1;
constexpr ulint AIO_WRITE = 2;

AIO *srv_aio{};

struct Buffer {
  byte *m_ptr{};
  size_t m_len{};
};

/** The asynchronous i/o array slot structure */
struct AIO_slot {
  /** Buffer pointer and length subbmitted to the kernel. */
  Buffer m_requested{};

  /** Total number of bytes read or written so far. */
  uint32_t m_len{};

  /** File offset in bytes */
  off_t m_off{};

  /** true if this slot is reserved */
  bool m_reserved{};

  /** The IO context */
  IO_ctx m_io_ctx{};
};

using Slots_pool = Bounded_channel<AIO_slot*>;

/** The asynchronous i/o array structure */
struct Segment {

  struct Handler;

  /** Constructor
   * @param[in] id Id of the segment
   * @param[in] n_slots Number of slots in the aio array
   * @param[in] n_threads Number of threads per segment
   */
  explicit Segment(ulint id, size_t n_slots, size_t n_threads) noexcept;

  /* Destructor */
  ~Segment() noexcept;

  /* @return true if it's a log segment. */
  [[nodiscard]] bool is_log() const noexcept {
    return m_id == AIO_LOG;
  }

  /* @return true if it's a read segment. */
  [[nodiscard]] bool is_read() const noexcept {
    return m_id == AIO_READ;
  }

  /* @return true if it's a write segment. */
  [[nodiscard]] bool is_write() const noexcept {
    return m_id == AIO_WRITE;
  }

  void lock() const noexcept {
    os_mutex_enter(m_mutex);
  }

  void unlock() const noexcept {
    os_mutex_exit(m_mutex);
  }

  /**
  * Validates the consistency of an aio array.
  *
  * @return true if ok
  */
  [[nodiscard]] bool validate() const noexcept;

  /** Get the handler for the AIO.
   * 
   * @return the handler for the AIO
  */
  [[nodiscard]] Handler *get_handler_for_aio() noexcept;

  /** Free the slot.
  * @param[in] slot Slot to free.
  */
  void mark_as_free(AIO_slot* slot) noexcept;

  /** Wake up the reaper threads, we are shutting down. */
  void shutdown() noexcept;

  /**
  * Creates an aio wait array.
  *
  * @param[in] id               Id of the segment
  * @param n_slots              Number of slots.
  * @param n_threads            Number of segments in the aio array
  * @return own: aio array
  */
  [[nodiscard]] static Segment *create(ulint id, ulint n_slots, ulint n_threads) noexcept;

  /** Destoy a Segment instance.
   * @param[in,own] segment Segment instance to destroy.
   */
  static void destroy(Segment *segment) noexcept;

  /** Type of the AIO array. */
  ulint m_id{ULINT_UNDEFINED};

  /* Mutex protecting the fields below. */
  mutable OS_mutex *m_mutex{};

  /** The event which is set to the signaled state when there are
   * slots available in this segment. */
  Cond_var* m_not_full{};

  /** The event which is set to the signaled state when there are
   * no free slots in this segment. */
  Cond_var* m_is_empty{};

  /** Number of  Treserved slots in the aio array segment */
  std::atomic<ulint> m_n_reserved{};

  /** Pointer to the to the slots in the array, the slots are partitioned
   * into local segments.
   */
  std::vector<AIO_slot> m_slots{};

  /** The free slot pool */
  Slots_pool *m_free_pool{};

  /** Handler for this segment. */
  std::vector<Handler *> m_handlers{};
};

/** AIO handler. */
struct Segment::Handler {
  /** Constructor 
  * @param[in] segment Parent segment
  * @param[in] id Id of the AIO array
  * @param[in] queue_size Size of the io_uring queue
  */
  Handler(Segment *segment, ulint id, ulint queue_size) noexcept
    : m_segment(segment),
      m_id(id) {

    if (auto ret = io_uring_queue_init(queue_size, &m_iouring, 0); ret < 0) {
      log_fatal("Initializing io_uring queue failed: " + std::to_string(ret));
    }
  }

  Handler(Handler &&) = delete;
  Handler(const Handler &) = delete;
  Handler&operator=(Handler &&) = delete;
  Handler&operator=(const Handler &) = delete;

  ~Handler() noexcept {
    io_uring_queue_exit(&m_iouring);
  }

  void lock() const noexcept {
    m_segment->lock();
  }

  void unlock() const noexcept {
    m_segment->unlock();
  }

  /** Submit an asynchronous IO request.
   * 
   * @param[in,out] slot Slot to submit
   * 
   * @return DB_SUCCESS or error code.
  */
  db_err submit(AIO_slot *slot) noexcept ;

  /** Wait for completed requests and return the IO context.
   * 
   * @param[out] io_ctx IO context
   * 
   *  @return DB_SUCCESS or error code. */
  db_err reap(IO_ctx &io_ctx) noexcept;

  /**
  * Reserve a slot from the array.
  * @param[in] io_ctx IO context.
  * @param[in] ptr Pointer to the buffer for IO
  * @param[in] len Length of the buffer (to read/write)
  * @param[in] off Offset in the file.
  * @return an IO slot from the array. */
  [[nodiscard]] AIO_slot *reserve_slot(const IO_ctx &io_ctx, void *ptr, size_t len, off_t off) noexcept;

  /** Shutdown the handler. */
  void shutdown() {
    ut_a(!m_shutdown.load());
    ut_a(m_pending_slots.load() == 0);
    ut_a(m_segment->m_free_pool->full());
    ut_a(m_segment->m_n_reserved.load() == 0);

    m_shutdown.store(true);

    io_uring_sqe *sqe = io_uring_get_sqe(&m_iouring);
    ut_a(sqe != nullptr);

    io_uring_prep_cancel(sqe, this, 0);

    for (;;) {
      const auto ret = io_uring_submit(&m_iouring);

      switch(ret) {
        case 1:
          return;
        case -EINTR:
        case -EAGAIN:
          continue;
        default:
          log_fatal("io_uring_submit failed: " + std::to_string(ret));
      }
    }
  }

  /** We are shutting down. */
  std::atomic<bool> m_shutdown{};

  /** Number of pending AIO slots. */
  std::atomic<ulint> m_pending_slots{};

  /** Parent segment. */
  Segment *m_segment{};

  /** Local segment number. */
  ulint m_id{ULINT_UNDEFINED};

  /** io_uring instance  use for AIO. */
  io_uring m_iouring{};
};

struct AIO_impl : public AIO {
  /** Constructor.
   * @param[in] max_pending Maximum number of pending aio operations allowed per thread.
   * @param[in] read_threads Number of reader threads.
   * @param[in] writer_threads Number of writer threads.
   * @param[in] sync_slots Number of slots in the sync AIO_array.
   */ 
  AIO_impl(ulint max_pending, ulint read_threads, ulint write_threads) noexcept
    : m_n_threads(read_threads + write_threads + 1) {
    m_segments[AIO_LOG] = Segment::create(AIO_LOG, max_pending, 1);
    m_segments[AIO_READ] = Segment::create(AIO_READ, max_pending, read_threads);
    m_segments[AIO_WRITE] = Segment::create(AIO_WRITE, max_pending, write_threads);
  }

  ~AIO_impl() noexcept {
    Segment::destroy(m_segments[AIO_LOG]);
    Segment::destroy(m_segments[AIO_READ]);
    Segment::destroy(m_segments[AIO_WRITE]);
  };

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
  * @brief Does simulated aio. This function should be called by an i/o-handler thread.
  *
  * @param[in] segment The number of the segment in the aio arrays to wait for.
  * @param[out] io_ctx Context of the i/o operation.
  * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] virtual db_err reap(ulint segment, IO_ctx &io_ctx) noexcept;

  /**
  * @brief Waits until there are no pending async writes, There can be other,
  * synchronous, pending writes.
  */
  virtual void wait_for_async_writes() noexcept;

  /**
  * @brief Closes/shuts down the IO sub-system and frees all the memory.
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
   * @return the AIO type
  */
  [[nodiscard]] ulint get_aio_type(const IO_ctx& io_ctx) noexcept;

  /** 
   * Get the segment for the given global segment.
   * 
   * @param[in] global_segment Global segment number
   * 
   * @return the segment for the given global segment
  */
  [[nodiscard]] Segment *get_segment(ulint global_segment) noexcept;

  /** 
   * Get the handler for the given global segment.
   * 
   * @param[in] global_segment Global segment number
   * 
   * @return the handler for the given global segment
  */
  [[nodiscard]] Segment::Handler *get_handler(ulint global_segment) noexcept;

  /** 
   * Get the total number of threads.
   * 
   * @return the total number of threads
  */
  [[nodiscard]] ulint get_total_threads() const  noexcept{
    return m_n_threads;
  }

  AIO_impl(AIO_impl&&) = delete;
  AIO_impl(const AIO_impl&) = delete;
  AIO_impl& operator=(AIO_impl&&) = delete;
  AIO_impl& operator=(const AIO_impl&) = delete;

  using Array = std::array<Segment *, AIO_WRITE + 1>;

  /** The segment instances. */
  Array m_segments;

  /** Total number of threads. */
  std::size_t m_n_threads;
};

Segment::Segment(ulint id, size_t n_slots, size_t n_threads) noexcept
  : m_id(id) {
  m_mutex = os_mutex_create(nullptr);
  m_not_full = os_event_create(nullptr);
  m_is_empty = os_event_create(nullptr);

  m_slots.resize(n_slots);

  for (size_t i = 0; i < n_threads; i++) {
    auto handler = new (ut_new(sizeof(Handler))) Handler(this, i, n_slots);
    ut_a(handler != nullptr);
    m_handlers.push_back(handler);
  }

  os_event_set(m_is_empty);

  m_n_reserved.store(0, std::memory_order_release);

  m_free_pool = new (ut_new(sizeof(Slots_pool))) Slots_pool(n_slots);

  for (auto &slot : m_slots) {
    auto success = m_free_pool->enqueue(&slot);
    ut_a(success);
  }
}

Segment::~Segment() noexcept {
  os_mutex_destroy(m_mutex);

  for (auto handler : m_handlers) {
    call_destructor(handler);
    ut_delete(handler);
  }
  m_handlers.clear();

  os_event_free(m_not_full);
  os_event_free(m_is_empty);
}

Segment::Handler *Segment::get_handler_for_aio() noexcept {
  Handler *aio_handler;

  for (auto handler : m_handlers) {
    if (!aio_handler) {
      aio_handler = handler;
    } else if (handler->m_pending_slots.load(std::memory_order_relaxed) <
               aio_handler->m_pending_slots.load(std::memory_order_relaxed)) {
      aio_handler = handler;
    }
  }

  return aio_handler;
}

void Segment::shutdown() noexcept {
  for (auto &handler : m_handlers) {
    if (!handler->m_shutdown.load(std::memory_order_acquire)){
      handler->shutdown();
    }
  }
}

bool Segment::validate() const noexcept {
  return true;
}

void Segment::destroy(Segment *array) noexcept {
  call_destructor(array);
  ut_delete(array);
}

Segment *Segment::create(ulint id, ulint n_slots, ulint n_threads) noexcept {
  ut_a(n_slots > 0);
  ut_a(n_threads > 0);

  return new (ut_new(sizeof(Segment))) Segment(id, n_slots, n_threads);
}

void Segment::mark_as_free(AIO_slot *slot) noexcept {
  ut_ad(slot->m_reserved);

  slot->m_reserved = false;

  auto success = m_free_pool->enqueue(slot);
  ut_a(success);

  m_n_reserved.fetch_sub(1, std::memory_order_relaxed);

  if (m_n_reserved.load() == m_slots.capacity() - 1) {
    os_event_set(m_not_full);
  }

  if (m_n_reserved.load() == 0) {
    os_event_set(m_is_empty);
  }
}

AIO_slot *Segment::Handler::reserve_slot(const IO_ctx &io_ctx, void *ptr, size_t len, off_t off) noexcept {
  for (;;) {
    if (m_segment->m_n_reserved.load(std::memory_order_relaxed) == m_segment->m_slots.capacity()) {

      /* If the handler threads are suspended, wake them
      so that we get more slots */

      os_event_wait(m_segment->m_not_full);

    } else {
      AIO_slot *slot = nullptr;

      while (!m_segment->m_free_pool->dequeue(slot)) {
        std::this_thread::yield();
      }

      ut_a(!slot->m_reserved);

      auto io_ctx_copy = io_ctx;

      m_segment->m_n_reserved.fetch_add(1, std::memory_order_relaxed);

      if (m_segment->m_n_reserved.load() == 1) {
        os_event_reset(m_segment->m_is_empty);
      }

      if (m_segment->m_n_reserved.load() == m_segment->m_slots.capacity()) {
        os_event_reset(m_segment->m_not_full);
      }

      slot->m_len = 0;
      slot->m_off = off;
      slot->m_reserved = true;
      slot->m_io_ctx = std::move(io_ctx_copy);
      slot->m_requested = {static_cast<byte*>(ptr), len};

      return slot;
    }
  }

  ut_error;
  return nullptr;
}

db_err Segment::Handler::submit(AIO_slot *slot) noexcept {
  ut_a(!m_shutdown.load(std::memory_order_acquire));
  ut_ad(m_segment->m_n_reserved.load(std::memory_order_acquire) > 0);

  auto sqe = io_uring_get_sqe(&m_iouring);

  if (sqe == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto &buffer = slot->m_requested;
  auto fh{slot->m_io_ctx.m_fil_node->m_fh};

  /* Do the i/o with ordinary, synchronous i/o functions: */
  if (slot->m_io_ctx.is_read_request()) {
    io_uring_prep_read(sqe, fh, buffer.m_ptr, buffer.m_len, slot->m_off);
  } else {
    io_uring_prep_write(sqe, fh, buffer.m_ptr, buffer.m_len, slot->m_off);
  }

  io_uring_sqe_set_data64(sqe, uintptr_t(slot));

  m_pending_slots.fetch_add(1, std::memory_order_relaxed);

  for (;;) {
    const auto ret = io_uring_submit(&m_iouring);

    switch(ret) {
      case 1:
        return DB_SUCCESS;
      case -EINTR:
      case -EAGAIN:
        continue;
      default:
        log_fatal("io_uring_submit failed: " + std::to_string(ret));
    }
    return DB_ERROR;
  }
} 

db_err Segment::Handler::reap(IO_ctx &io_ctx) noexcept {
  ut_ad(m_segment->validate());

  io_uring_cqe *cqe;

  for (;;) {
    do {
      const int ret = io_uring_wait_cqe(&m_iouring, &cqe);

      switch (ret) {
        case 0:
          break;
        case -EINTR:
        case -EAGAIN:
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

    auto slot = reinterpret_cast<AIO_slot*>(io_uring_cqe_get_data64(cqe));

    ut_a(cqe->res != -1);
    ut_a(slot->m_reserved);
    ut_a(decltype(cqe->res)(slot->m_requested.m_len) >= cqe->res);

    slot->m_off += cqe->res;
    slot->m_len += cqe->res;
    slot->m_requested.m_len -= cqe->res;
    slot->m_requested.m_ptr += cqe->res;

    if (slot->m_requested.m_len > 0) {
      /* It was a partial read/write, try and read the remaining bytes. */
      submit(slot);
    } else {
      slot->m_io_ctx.m_ret = cqe->res;

      auto n = m_pending_slots.fetch_sub(1, std::memory_order_relaxed);
      ut_a(n > 0);

      io_uring_cqe_seen(&m_iouring, cqe);

      io_ctx = slot->m_io_ctx;

      m_segment->mark_as_free(slot);

      return DB_SUCCESS;
    }
  }
}

void IO_ctx::validate() const noexcept {
  ut_a(m_fil_node->m_fh != -1);
  ut_a(m_fil_node->m_file_name != nullptr);
}

AIO* AIO::create(ulint max_slots, ulint read_threads, ulint write_threads) noexcept {
  ut_a(read_threads > 0);
  ut_a(write_threads > 0);

  return new (ut_new(sizeof(AIO_impl))) AIO_impl(max_slots, read_threads, write_threads);
}

void AIO::destroy(AIO *&aio) noexcept {
  call_destructor(aio);
  ut_delete(aio);
  aio = nullptr;
}

ulint AIO_impl::get_aio_type(const IO_ctx& io_ctx) noexcept {
  if (io_ctx.is_log_request()) {
    return AIO_LOG;
  } else if (io_ctx.is_read_request()) {
    return AIO_READ;
  } else {
    return AIO_WRITE;
  } 
  ut_error;
  return ULINT_UNDEFINED;
}

Segment *AIO_impl::get_segment(ulint global_segment) noexcept {
  ut_a(global_segment < get_total_threads());

  if (global_segment == AIO_LOG) {
    return m_segments[AIO_LOG];
  } else if (global_segment < m_segments[AIO_READ]->m_handlers.size() + 1) {
    return m_segments[AIO_READ];
  } else {
    return m_segments[AIO_WRITE];
  }
}

Segment::Handler *AIO_impl::get_handler(ulint global_segment) noexcept {
  ut_a(global_segment < get_total_threads());
  auto segment = get_segment(global_segment);

  if (global_segment == AIO_LOG) {
    return segment->m_handlers[0];
  } else if (global_segment < m_segments[AIO_READ]->m_handlers.size() + 1) {
    return segment->m_handlers[global_segment - 1];
  } else {
    return segment->m_handlers[global_segment - m_segments[AIO_READ]->m_handlers.size() - 1];
  }
}

db_err AIO_impl::submit(IO_ctx&& io_ctx, void *ptr, ulint n, off_t off) noexcept {
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
  auto segment = m_segments[get_aio_type(io_ctx)];
  auto handler = segment->get_handler_for_aio();
  auto slot = handler->reserve_slot(io_ctx, ptr, n, off);

  handler->submit(slot);

  return DB_SUCCESS;
}

std::string AIO_impl::to_string() noexcept {
  std::ostringstream os{};

  for (auto &segment : m_segments) {
    ut_ad(segment->validate());

    os << "Pending normal aio reads:";

    switch (segment->m_id) {
      case AIO_READ:
        os << ", aio reads:";
        break;
      case AIO_WRITE:
        os << ", aio writes:";
        break;
      case AIO_LOG: 
        os << ", aio log :";
        break;
      default:
        ut_error;
    }

    os_mutex_enter(segment->m_mutex);
 
    ut_a(!segment->m_slots.empty());
    ut_a(!segment->m_handlers.empty());

    ulint n_reserved{};

    for (auto &slot : segment->m_slots) {
      if (slot.m_reserved) {
        ++n_reserved;
        ut_a(slot.m_len > 0);
      }
    }

    os << std::format(" {}", n_reserved);

    os_mutex_exit(segment->m_mutex);
  }

  os << "\n";

  return os.str();
}

db_err AIO_impl::reap(ulint global_segment, IO_ctx &io_ctx) noexcept {
  return get_handler(global_segment)->reap(io_ctx);
}

void AIO_impl::wait_for_async_writes() noexcept {
  os_event_wait(m_segments[AIO_WRITE]->m_is_empty);
}

void AIO_impl::shutdown() noexcept {
  for (auto &segment : m_segments) {
    segment->shutdown();
  }
}
