#include <errno.h>

#include <array>
#include <vector>

#include "os0aio.h"
#include "os0file.h"
#include "ut0mem.h"
#include "os0sync.h"
#include "ut0byte.h"

/* In simulated aio, merge at most this many consecutive i/os */
constexpr ulint OS_AIO_MERGE_N_CONSECUTIVE = 64;

/** If this flag is true, then we will use the native aio of the
OS (provided we compiled Innobase with it in), otherwise we will
use simulated aio we build below with threads */

/** The asynchronous i/o array slot structure */
struct AIO_slot {
  /** Time when reserved */
  time_t m_reservation_time{};

  /** Index of the slot in the aio array */
  uint32_t m_pos{};

  /** Length of the block to read or write */
  uint32_t m_len{};

  /** Buffer used in i/o */
  byte *m_buf{};

  /** File offset in bytes */
  off_t m_off{};

  /** Used only in simulated aio: true if the physical i/o already
   * made and only the slot message needs to be passed to the caller
   * of os_aio_simulated_handle */
  bool m_io_already_done{};
  
  /** true if this slot is reserved */
  bool m_reserved{};

  /** The IO context */
  IO_ctx m_io_ctx;
};

constexpr ulint AIO_LOG = 0;
constexpr ulint AIO_READ = 1;
constexpr ulint AIO_WRITE = 2;
constexpr ulint AIO_SYNC = 3;

/** The asynchronous i/o array structure */
struct Segment {
  /** Constructor
   * @param[in] n_segments Number of segments in the aio array
   * @param[in] n_slots Number of slots in the aio array
   */
 explicit Segment(ulint id, size_t n_segments, size_t n_slots);

  /* Destructor */
  ~Segment() noexcept;

  [[nodiscard]] bool is_log() const {
    return m_id == AIO_LOG;
  }

  [[nodiscard]] bool is_read() const {
    return m_id == AIO_READ;
  }

  [[nodiscard]] bool is_write() const {
    return m_id == AIO_WRITE;
  }

  /**
  * Validates the consistency of an aio array.
  *
  * @return true if ok
  */
  [[nodiscard]] bool validate() const;

  /* Wake up any thread that's waiting for IO completion. */
  void notify_all() {
    for (auto e : m_completion) {
      os_event_set(e);
    }
  }

  /**
  * Creates an aio wait array.
  *
  * @param[in] id               Id of the aio array  
  * @param n                    Number of slots.
  * @param n_segments           Number of segments in the aio array
  * @return own: aio array
  */
  [[nodiscard]] static std::shared_ptr<Segment> create(ulint id, ulint n, ulint n_segments);

  /** Destoy a Segment instance.
   * @param[in,own] array Segment instance to destroy.
   */
  static void destroy(Segment *array);
  
  /** Create the infrastructure */
  [[nodiscard]] static bool open();

  /** Type of the AIO array. */
  ulint m_id{ULINT_UNDEFINED};

  /* Mutex protecting the fields below. */
  mutable OS_mutex *m_mutex{};

  /** There is one event per local segment (or thread). */
  std::vector<Cond_var *>m_completion{};

  /** The event which is set to the signaled state when there are
   * slots available in this segment. */
  Cond_var* m_not_full{};

  /** The event which is set to the signaled state when there are
   * no free slots in this segment. */
  Cond_var* m_is_empty{};

  /** Number of  Treserved slots in the aio array segment */
  ulint m_n_reserved{};

  /** Pointer to the to the slots in the array, the slots are partitioned
   * into local segments.
   */
  std::vector<AIO_slot> m_slots{};

  /** Number of asynchronous I/O segments.  Set by os_aio_init(). */
  static ulint s_n_segments;
};

ulint Segment::s_n_segments = ULINT_UNDEFINED;

struct Local_segment {
  ulint get_slot_count() const {
    return m_segment->m_slots.capacity() / m_segment->m_completion.size();
  }

  void lock() const {
    os_mutex_enter(m_segment->m_mutex);
  }

  void unlock() const {
    os_mutex_exit(m_segment->m_mutex);
  }

  /* Wait on a completion event. */
  void wait_for_completion_event() {
    os_event_wait(m_segment->m_completion[m_no]);
  }

  /* Reset the completion event. */
  void reset_completion_event() {
    os_event_reset(m_segment->m_completion[m_no]);
  }

  /**
   * Reserve a slot from the array.
   * @param[in] io_ctx IO context.
   * @param[in] ptr Pointer to the buffer for IO
   * @param[in] len Length of the buffer (to read/write)
   * @param[in] off Offset in the file.
   * @return an IO slot from the array. */
  [[nodiscard]] AIO_slot *reserve_slot(const IO_ctx &io_ctx, void *ptr, ulint len, off_t off);

  /** Free the slot.
   * @param[in] slot Slot to free.
  */
  void free_slot(AIO_slot* slot);

  /** Create an instnce without the local segment number from the
  * IO mode and request type. *
  * @param[in] io_ctx IO context.
  * 
  * @return an AIO_segment instance. */
  [[nodiscard]] static Local_segment create(const IO_ctx &io_ctx);

  /**
  * Calculates local segment number and aio array from global segment number.
  *
  * @param global_segment    in: global segment number
  *
  * @return the AIO_segment with the local segment number and the aio array.
  */
  [[nodiscard]] static Local_segment create(ulint global_segment);
  
  /** Get a local segment number from the AIO array an slot ordinal value.
   * @param[in] slot AIO slot.
   * 
   * @return the local segment number.
  */
  [[nodiscard]] ulint get_local_segment_no(const AIO_slot *slot) const;

  std::shared_ptr<Segment> m_segment{};

  /* Local segment number. */
  ulint m_no{std::numeric_limits<ulint>::max()};
};

static std::array<std::shared_ptr<Segment>, AIO_SYNC + 1> segments;

/** If the following is true, read i/o handler threads try to
wait until a batch of new read requests have been posted */
static bool os_aio_recommend_sleep_for_read_threads = false;

void os_aio_var_init() {
  for (auto &segment: segments) {
    ut_a(!segment);
  }

  Segment::s_n_segments = ULINT_UNDEFINED;

  os_aio_recommend_sleep_for_read_threads = false;
}

Segment::Segment(ulint id, size_t n_segments, size_t n)
  : m_id(id) {
  m_mutex = os_mutex_create(nullptr);
  m_not_full = os_event_create(nullptr);
  m_is_empty = os_event_create(nullptr);

  for (size_t i = 0; i < n_segments; i++) {
    m_completion.push_back(os_event_create(nullptr));
  }

  os_event_set(m_is_empty);

  m_n_reserved = 0;

  m_slots.resize(n);

  ulint i{};
  for (auto &slot : m_slots) {
    slot.m_pos = i++;
  }
}

Segment::~Segment() {
  os_mutex_destroy(m_mutex);

  for (auto e : m_completion) {
    os_event_free(e);
  }

  m_completion.clear();

  os_event_free(m_not_full);
  os_event_free(m_is_empty);
}

bool Segment::validate() const {
  os_mutex_enter(m_mutex);

  ut_a(!m_slots.empty());
  ut_a(!m_completion.empty());

  ulint n_reserved{};

  for (auto &slot : m_slots) {
    if (slot.m_reserved) {
      ++n_reserved;
      ut_a(slot.m_len > 0);
    }
  }

  ut_a(m_n_reserved == n_reserved);

  os_mutex_exit(m_mutex);

  return true;
}

void Segment::destroy(Segment *array) {
  call_destructor(array);
  ut_delete(array);
}

std::shared_ptr<Segment> Segment::create(ulint id, ulint n, ulint n_segments) {
  ut_a(n > 0);
  ut_a(n_segments > 0);

  return std::shared_ptr<Segment>(
      new (ut_new(sizeof(Segment))) Segment(id, n_segments, n),
      [](Segment* segment) { destroy(segment); });
}

Local_segment Local_segment::create(const IO_ctx& io_ctx) {
  Local_segment segment;

  if (io_ctx.is_log_request()) {
    segment.m_segment = segments[AIO_LOG];
  } else if (io_ctx.is_sync_request()) {
    segment.m_segment = segments[AIO_SYNC];
  } else if (io_ctx.is_read_request()) {
    segment.m_segment = segments[AIO_READ];
  } else {
    segment.m_segment = segments[AIO_READ];
  } 

  return segment;
}

void Local_segment::free_slot(AIO_slot *slot) {
  lock();

  ut_ad(slot->m_reserved);

  slot->m_reserved = false;

  --m_segment->m_n_reserved;

  if (m_segment->m_n_reserved == m_segment->m_slots.capacity() - 1) {
    os_event_set(m_segment->m_not_full);
  }

  if (m_segment->m_n_reserved == 0) {
    os_event_set(m_segment->m_is_empty);
  }

  unlock();
}

ulint Local_segment::get_local_segment_no(const AIO_slot *slot) const {
  if (m_segment->is_log()) {
    return 0;

  } else if (m_segment->is_read()) {

    return 1 + slot->m_pos / get_slot_count();

  } else {
    const auto local_segments = m_segment->m_completion.size();
    ut_a(m_segment->is_write());

    return 1 + local_segments + slot->m_pos / get_slot_count();
  }
}

/**
 * Calculates local segment number and aio array from global segment number.
 *
 * @param global_segment    in: global segment number
 *
 * @return the AIO_segment with the local segment number and the aio array.
 */
Local_segment Local_segment::create(ulint global_segment) {
  Local_segment local_segment{};

  ut_a(global_segment < Segment::s_n_segments);

  if (global_segment == AIO_LOG) {

    local_segment.m_no = 0;
    local_segment.m_segment = segments[AIO_LOG];

  } else if (global_segment < segments[AIO_READ]->m_completion.size() + 1) {

    local_segment.m_no = global_segment - 1;
    local_segment.m_segment = segments[AIO_READ];

  } else {

    local_segment.m_segment = segments[AIO_WRITE];
    local_segment.m_no = global_segment - (segments[AIO_READ]->m_completion.size() + 1);
  }

  return local_segment;
}

AIO_slot *Local_segment::reserve_slot(const IO_ctx &io_ctx, void *ptr, ulint len, off_t off) {
  /* No need of a mutex. Only reading constant fields */
  /* We attempt to keep adjacent blocks in the same local
  segment. This can help in merging IO requests when we are
  doing simulated AIO */
  auto slots_per_seg = get_slot_count();
  auto local_seg = (off >> (UNIV_PAGE_SIZE_SHIFT + 6)) % m_segment->m_completion.size();
  auto slot_prepare = [&](AIO_slot &slot) {
    auto io_ctx_copy = io_ctx;
    ut_a(slot.m_reserved == false);

    ++m_segment->m_n_reserved;

    if (m_segment->m_n_reserved == 1) {
      os_event_reset(m_segment->m_is_empty);
    }

    if (m_segment->m_n_reserved == m_segment->m_slots.capacity()) {
      os_event_reset(m_segment->m_not_full);
    }

    slot.m_reserved = true;
    slot.m_reservation_time = time(nullptr);
    slot.m_io_ctx = std::move(io_ctx_copy);
    slot.m_len = len;
    slot.m_buf = static_cast<byte *>(ptr);
    slot.m_off = off;
    slot.m_io_already_done = false;
  };

  for (;;) {
    lock();

    if (m_segment->m_n_reserved == m_segment->m_slots.capacity()) {
      unlock();

      /* If the handler threads are suspended, wake them
      so that we get more slots */

      os_aio_simulated_wake_handler_threads();

      os_event_wait(m_segment->m_not_full);

    } else {
      /* First try to find a slot in the preferred local segment */
      for (ulint i = local_seg * slots_per_seg; i < m_segment->m_slots.capacity(); i++) {
        auto &slot = m_segment->m_slots[i];

        if (!slot.m_reserved) {
          slot_prepare(slot); 
          unlock();
          return &slot;
        }
      }

      /* Fall back to a full scan. We are guaranteed to find a slot */
      for (auto &slot : m_segment->m_slots) { 
        if (!slot.m_reserved) {
          slot_prepare(slot);
          unlock();
          return &slot;
        }
      }
    }
  }

  ut_error;
  return nullptr;
}
void os_aio_init(ulint max_slots, ulint read_threads, ulint write_threads, ulint sync_slots) {
  ulint n_segments = 1 + read_threads + write_threads;

  ut_ad(n_segments >= 3);

  segments[AIO_LOG] = Segment::create(AIO_LOG, max_slots, 1);

  segments[AIO_READ] = Segment::create(AIO_READ, read_threads * max_slots, read_threads);

  segments[AIO_WRITE] = Segment::create(AIO_WRITE, write_threads * max_slots, write_threads);

  segments[AIO_SYNC] = Segment::create(AIO_SYNC, sync_slots, 1);

  Segment::s_n_segments = n_segments;

  os_aio_validate();
}

void os_aio_close() {
  for (auto &segment : segments) {
    if (segment) {
      segment = nullptr;
    }
  }
}

void os_aio_wake_all_threads_at_shutdown() {
  /* This loop wakes up all simulated ai/o threads */
  for (auto &segment : segments) {
    if (segment) {
      segment->notify_all();
    }
  }
}

void os_aio_wait_until_no_pending_writes() {
  os_event_wait(segments[AIO_WRITE]->m_is_empty);
}

/**
 * Wakes up a simulated aio i/o-handler thread if it has something to do.
 *
 * @param global_segment - the number of the segment in the aio arrays
 */
static void os_aio_simulated_wake_handler_thread(ulint global_segment) {
  auto segment = Local_segment::create(global_segment);
  auto n = segment.get_slot_count();

  /* Look through n slots after the segment * n'th slot */

  segment.lock();

  bool found_empty_slot = false;

  for (ulint i = 0; i < n; i++) {
    auto slot = &segment.m_segment->m_slots[i + segment.m_no * n];

    if (slot->m_reserved) {
      found_empty_slot = true;
      break;
    }
  }

  segment.unlock();

  if (found_empty_slot) {
    segment.m_segment->notify_all();
  }
}

void os_aio_simulated_wake_handler_threads() {
  os_aio_recommend_sleep_for_read_threads = false;

  for (auto &segment : segments) {
    segment->notify_all();
  }
}

void os_aio_simulated_put_read_threads_to_sleep() {}

bool os_aio(IO_ctx&& io_ctx, void *ptr, ulint n, off_t off) {
  io_ctx.validate();

  ut_ad(n > 0);
  ut_ad(ptr != nullptr);
  ut_ad(n % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(off % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(os_aio_validate());

  if (io_ctx.is_sync_request()) {
    if (io_ctx.is_read_request()) {
      return os_file_read(io_ctx.m_file, ptr, n, off);
    } else {
      return os_file_write(io_ctx.m_name, io_ctx.m_file, ptr, n, off);
    }
  }
  auto local_segment = Local_segment::create(io_ctx);
  auto slot = local_segment.reserve_slot(io_ctx, ptr, n, off);

  if (io_ctx.is_read_request()) {
    if (!io_ctx.m_batch) {
      os_aio_simulated_wake_handler_thread(local_segment.get_local_segment_no(slot));
    }
  } else {
    if (!io_ctx.m_batch) {
      os_aio_simulated_wake_handler_thread(local_segment.get_local_segment_no(slot));
    }
  }

  return true;
}

bool os_aio_simulated_handle(ulint global_segment, IO_ctx &out_io_ctx) {
  std::array<AIO_slot *, OS_AIO_MERGE_N_CONSECUTIVE> consecutive_ios;
  auto local_segment = Local_segment::create(global_segment);

  for (;;) {
    /* NOTE! We only access constant fields in os_aio_array. Therefore
    we do not have to acquire the protecting mutex yet */

    ut_ad(os_aio_validate());
    ut_ad(local_segment.m_no < local_segment.m_segment->m_completion.size());

    auto n = local_segment.get_slot_count();

    /* Look through n slots after the segment * n'th slot */

    if (local_segment.m_segment->is_read() && os_aio_recommend_sleep_for_read_threads) {

      /* Give other threads chance to add several i/os to the array at once. */

      local_segment.wait_for_completion_event();

      continue;
    }

    local_segment.lock();

    /* Check if there is a slot for which the i/o has already been done */

    for (ulint i = 0; i < n; i++) {
      auto slot = &local_segment.m_segment->m_slots[i + local_segment.m_no * n];

      if (slot->m_reserved && slot->m_io_already_done) {

        ut_a(slot->m_reserved);

        out_io_ctx = slot->m_io_ctx;

        local_segment.unlock();

        local_segment.free_slot(slot);

        return true;
      }
    }

    ulint age{};
    ulint n_consecutive = 0;
    ulint biggest_age = 0;
    off_t lowest_offset = std::numeric_limits<off_t>::max();

    /* If there are at least 2 seconds old requests, then pick the oldest
    one to prevent starvation. If several requests have the same age,
    then pick the one at the lowest offset. */

    for (ulint i = 0; i < n; i++) {
      auto slot = &local_segment.m_segment->m_slots[i + local_segment.m_no * n];

      if (slot->m_reserved) {
        age = (ulint)difftime(time(nullptr), slot->m_reservation_time);

        if ((age >= 2 && age > biggest_age) ||
            (age >= 2 && age == biggest_age && slot->m_off < lowest_offset)) {

          /* Found an i/o request */
          consecutive_ios[0] = slot;

          n_consecutive = 1;

          biggest_age = age;
          lowest_offset = slot->m_off;
        }
      }
    }

    if (n_consecutive == 0) {
      /* There were no old requests. Look for an i/o request at the
      lowest offset in the array (we ignore the high 32 bits of the
      offset in these heuristics) */

      off_t lowest_offset = std::numeric_limits<off_t>::max();

      for (ulint i = 0; i < n; i++) {
        auto slot = &local_segment.m_segment->m_slots[i + local_segment.m_no * n];

        if (slot->m_reserved && slot->m_off < lowest_offset) {
          /* Found an i/o request */
          consecutive_ios[0] = slot;

          n_consecutive = 1;

          lowest_offset = slot->m_off;
        }
      }
    }

    if (n_consecutive == 0) {
      /* No i/o requested at the moment */

      /* We wait here until there again can be i/os in the segment of this thread */

      local_segment.reset_completion_event();

      local_segment.unlock();

      /* Give other threads chance to add several i/os to the array at once. */

      local_segment.wait_for_completion_event();

      continue;
    }

    auto slot = consecutive_ios[0];
    ut_a(slot != nullptr);

    /* Check if there are several consecutive blocks to read or write */

    ulint i;

    do {
      for (i = 0; i < n && n_consecutive < OS_AIO_MERGE_N_CONSECUTIVE; i++) {
        auto slot2 = &local_segment.m_segment->m_slots[i + local_segment.m_no * n];

        if (slot2->m_reserved &&
            slot2 != slot &&
            slot2->m_io_ctx.m_file  == slot->m_io_ctx.m_file &&
            slot2->m_off == slot->m_off + off_t(slot->m_len) &&
            slot2->m_io_ctx.m_io_request == slot->m_io_ctx.m_io_request) {

          /* Found a consecutive i/o request */
          consecutive_ios[n_consecutive] = slot2;

          ++n_consecutive;

          slot = slot2;

          /* Scan from the beginning comparing the current match. */
          break;
        }
      }
    } while (n_consecutive < OS_AIO_MERGE_N_CONSECUTIVE && i < n);

    /* We have now collected n_consecutive i/o requests in the array;
    allocate a single buffer which can hold all data, and perform the
    i/o */

    uint total_len = 0;
    slot = consecutive_ios[0];

    for (ulint i = 0; i < n_consecutive; i++) {
      total_len += consecutive_ios[i]->m_len;
    }

    byte *combined_buf;
    byte *combined_buf_ptr;

    if (n_consecutive == 1) {
      /* We can use the buffer of the i/o request */
      combined_buf = slot->m_buf;
      combined_buf_ptr = nullptr;
    } else {
      combined_buf_ptr = static_cast<byte *>(ut_new(total_len + UNIV_PAGE_SIZE));

      ut_a(combined_buf_ptr);

      combined_buf = static_cast<byte *>(ut_align(combined_buf_ptr, UNIV_PAGE_SIZE));
    }

    /* We release the array mutex for the time of the i/o: NOTE that
    this assumes that there is just one i/o-handler thread serving
    a single segment of slots! */

    local_segment.unlock();

    ulint offs{};

    if (!slot->m_io_ctx.is_read_request() && n_consecutive > 1) {
      /* Copy the buffers to the combined buffer */
      offs = 0;

      for (ulint i = 0; i < n_consecutive; i++) {
        memcpy(combined_buf + offs, consecutive_ios[i]->m_buf, consecutive_ios[i]->m_len);
        offs += consecutive_ios[i]->m_len;
      }
    }

    bool ret;

    /* Do the i/o with ordinary, synchronous i/o functions: */
    if (slot->m_io_ctx.is_read_request()) {
      const auto &io_ctx = slot->m_io_ctx;

      ret = os_file_read(io_ctx.m_file, combined_buf, total_len, slot->m_off);
    } else {
      const auto &io_ctx = slot->m_io_ctx;

      ret = os_file_write(io_ctx.m_name, io_ctx.m_file, combined_buf, total_len, slot->m_off);
    }

    ut_a(ret);

    if (slot->m_io_ctx.is_read_request() && n_consecutive > 1) {
      /* Copy the combined buffer to individual buffers */
      offs = 0;

      for (ulint i = 0; i < n_consecutive; i++) {
        memcpy(consecutive_ios[i]->m_buf, combined_buf + offs, consecutive_ios[i]->m_len);
        offs += consecutive_ios[i]->m_len;
      }
    }

    if (combined_buf_ptr) {
      ut_delete(combined_buf_ptr);
    }

    local_segment.lock();
    
    /* Mark the i/os done in slots */

    for (ulint i = 0; i < n_consecutive; i++) {
      consecutive_ios[i]->m_io_already_done = true;
    }

    ut_a(slot->m_reserved);

    out_io_ctx = slot->m_io_ctx;

    local_segment.unlock();
    local_segment.free_slot(slot);

    return ret;
  }
}

bool os_aio_validate() {
  for (auto &segment : segments) {
    if (!segment->validate()) {
      return false;
    }
  }

  return true;
}

void os_aio_print(ib_stream_t ib_stream) {
  for (auto &segment : segments) {
    ib_logger(ib_stream, "Pending normal aio reads:");

    const char* name{};
    switch (segment->m_id) {
      case AIO_READ:
        name = ", aio reads:";
        break;
      case AIO_WRITE:
        name = ", aio writes:";
        break;
      case AIO_LOG:
        name = ", aio log :";
        break;
      case AIO_SYNC:
        name = ", sync i/o's:";
        break;
      default:
        ut_error;
    }

    ib_logger(ib_stream, name);

    for (auto e : segment->m_completion) {
      if (e->m_is_set) {
        ib_logger(ib_stream, " [ev %s]", e->m_is_set ? "set" : "reset");
      }
    }

    ib_logger(ib_stream, ":");

    os_mutex_enter(segment->m_mutex);

    ut_a(!segment->m_slots.empty());
    ut_a(!segment->m_completion.empty());

    ulint n_reserved{};

    for (auto &slot : segment->m_slots) {
      if (slot.m_reserved) {
        ++n_reserved;
        ut_a(slot.m_len > 0);
      }
    }

    ut_a(segment->m_n_reserved == n_reserved);

    ib_logger(ib_stream, " %lu", (ulong)n_reserved);

    os_mutex_exit(segment->m_mutex);
  }

  ib_logger(ib_stream, "\n");
}

#ifdef UNIV_DEBUG
bool os_aio_all_slots_free() {
  for (auto &segment: segments) {
    os_mutex_enter(segment->m_mutex);

    auto reserved = segment->m_n_reserved;

    os_mutex_exit(segment->m_mutex);

    if (reserved != 0) {
      return false;
    }
  }

  return true;
}
#endif /* UNIV_DEBUG */