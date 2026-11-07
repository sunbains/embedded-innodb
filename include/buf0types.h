/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/**************************************************/ /**
 @file include/buf0types.h
 The database buffer pool global types for the directory

 Created 11/17/1995 Heikki Tuuri
 *******************************************************/

#pragma once

#include "srv0sys.h"

#include "innodb0types.h"

#include <atomic>
#include <functional>
#include <optional>
#include <set>
#include <sstream>
#include <unordered_map>

#include "sync0mutex.h"
#include "sync0rw.h"
#include "ut0mem.h"
#include "ut0stat.h"

/** Transaction system. */
struct TrxSys;

/** Mini-transaction. */
struct Mtr;

/** The buffer pool page flusher. */
struct BufFlush;

/** Buffer pool LRU implementation. */
struct BufLru;

/** Buffer page */
struct BufPage;

/** Buffer block, with its page frame */
struct BufBlock;

/** Buffer pool chunk comprising buf_block_t */
struct BufChunk;

/** Buffer pool comprising BufChunk */
struct BufPool;

/** Buffer pool statistics struct */
struct BufPoolStat;

/** A buffer frame. @see page_t */
using buf_frame_t = byte;

/** Flags for flush types */
enum BufFlushType {

  /** Flush via the LRU list */
  BUF_FLUSH_LRU = 0,

  /** Flush a single page */
  BUF_FLUSH_SINGLE_PAGE,

  /** Flush via the flush list of dirty blocks */
  BUF_FLUSH_LIST,

  /** Index of last element + 1  */
  BUF_FLUSH_N_TYPES
};

/** Flags for io_fix types */
enum BufIoFix {
  /** No pending I/O */
  BUF_IO_NONE = 0,

  /** Read pending */
  BUF_IO_READ,

  /** Write pending */
  BUF_IO_WRITE
};

struct FilAddr;

/** @name Modes for BufPool::get */
/* @{ */

/** Get always */
constexpr ulint BUF_GET = 10;

/** Get if in pool */
constexpr ulint BUF_GET_IF_IN_POOL = 11;

/** Get and bufferfix, but set no latch; we have separated
this case, because it is error-prone programming not to set
a latch, and it should be used with care */
constexpr ulint BUF_GET_NO_LATCH = 14;

/* @} */
/** @name Modes for buf_page_get_known_nowait */
/* @{ */
/** Move the block to the start of the LRU list if there
is a danger that the block would drift out of the buffer pool */
constexpr ulint BUF_MAKE_YOUNG = 51;

/** Preserve the current LRU position of the block. */
constexpr ulint BUF_KEEP_OLD = 52;
/* @} */


#ifdef UNIV_DEBUG
/*! If this is set true, the program prints info whenever read or flush occurs */
extern bool buf_debug_prints;
#endif /* UNIV_DEBUG */

/** These statistics are generated for heuristics used in estimating the
rate at which we should flush the dirty blocks to avoid bursty IO
activity. Note that the rate of flushing not only depends on how many
dirty pages we have in the buffer pool but it is also a fucntion of
how much redo the workload is generating and at what rate. */
/* @{ */

/** Magic value to use instead of checksums when they are disabled */
constexpr ulint BUF_NO_CHECKSUM_MAGIC = 0xDEADBEEFUL;

/** Simple allocator using ut_new and ut_free */
template <typename T>
struct UtAllocator {
  using value_type = T;

  UtAllocator() noexcept = default;

  template <class U>
  explicit UtAllocator(const UtAllocator<U> &) noexcept {}

  T *allocate(std::size_t n) { return new (ut_new(sizeof(T) * n)) T[n]; }

  void deallocate(T *p, std::size_t n) {
    call_destructor(p, n);
    ut_delete(p);
  }
};

template <class T, class U>
constexpr bool operator==(const UtAllocator<T> &, const UtAllocator<U> &) noexcept {
  return true;
}

template <class T, class U>
constexpr bool operator!=(const UtAllocator<T> &, const UtAllocator<U> &) noexcept {
  return false;
}

/* set typedefs for buffer_page_t */
using Buf_page_set_cmp_t = std::function<bool(BufPage *, BufPage *)>;

using Buf_page_set = std::set<BufPage *, Buf_page_set_cmp_t, UtAllocator<BufPage *>>;

using Buf_page_set_itr = typename Buf_page_set::iterator;

/** @brief States of a control block
@see BufPage

The enumeration values must be 0..7. */
enum BufPageState {
  /** Is in the free list */
  BUF_BLOCK_NOT_USED,

  /** When m_LRU->get_free_block returns a block, it is in this state */
  BUF_BLOCK_READY_FOR_USE,

  /** Contains a buffered file page */
  BUF_BLOCK_FILE_PAGE,

  /** Contains some main memory object */
  BUF_BLOCK_MEMORY,

  /** Hash index should be removed before putting to the free list */
  BUF_BLOCK_REMOVE_HASH
};

/** The buffer control block structure */
struct BufPage {

  /**
   * Reads the freed_page_clock of the buffer page.
   *
   * @return The freed_page_clock.
   */
  [[nodiscard]] ulint get_freed_page_clock() const {
    /* This is sometimes read without holding buf_pool_mutex: a relaxed
    load, the writers hold the buf_pool mutex. */
    return ut::stat_load(m_freed_page_clock);
  }

  /** @return the buffer fix count. Relaxed load: callers that don't hold
  the block mutex (asserts, get_frame()) only need a value that was
  current at some point. Decisions on it (eviction, relocation, flush)
  are made under the block mutex. */
  [[nodiscard]] uint32_t get_fix_count() const { return ut::stat_load(m_buf_fix_count); }

  /**
  * Gets the space id of a page.
  *
  * @return Space id.
  */
  [[nodiscard]] space_id_t get_space() const {
    ut_a(in_file());

    return m_space;
  }

  /**
   * Gets the page number of a block.
   *
   * @return Page number.
   */
  [[nodiscard]] page_no_t get_page_no() const {
    ut_a(in_file());

    return m_page_no;
  }

  /**
   * Gets the page id of a page.
   *
   * @return Page id.
   */
  [[nodiscard]] PageId get_page_id() const noexcept { return {get_space(), get_page_no()}; }

  /**
  * Gets the state of a block.
  *
  * @param bpage Pointer to the control block.
  * @return The state of the block.
  */
  [[nodiscard]] BufPageState get_state() const;

  /** @return cast the instance to a BufPage pointer. */
  [[nodiscard]] BufBlock *get_block() { return reinterpret_cast<BufBlock *>(this); }

  /** @return cast the instance to a BufPage pointer. */
  const BufBlock *get_block() const { return reinterpret_cast<const BufBlock *>(this); }

  /**
   * Determines if a block is mapped to a tablespace.
   *
   * @param bpage Pointer to the control block.
   * @return True if the block is mapped to a tablespace, false otherwise.
   */
  bool in_file() const {
    switch (get_state()) {
      case BUF_BLOCK_FILE_PAGE:
        return true;
      case BUF_BLOCK_NOT_USED:
      case BUF_BLOCK_READY_FOR_USE:
      case BUF_BLOCK_MEMORY:
      case BUF_BLOCK_REMOVE_HASH:
        break;
    }

    return false;
  }

  /** @name General fields
  None of these bit-fields must be modified without holding
  buf_page_get_mutex() buf_block_t::mutex since they can be
  stored in the same machine word.  Some of these fields are
  additionally protected by buf_pool_mutex. */
  /* @{ */

  /** tablespace id; also protected by buf_pool_mutex. */
  space_id_t m_space;

  /** page number; also protected by buf_pool_mutex. */
  page_no_t m_page_no;

  /** state of the control block; also protected by buf_pool_mutex.
  State transitions from BUF_BLOCK_READY_FOR_USE to BUF_BLOCK_MEMORY
  need not be protected by buf_page_get_mutex().
  @see enum BufPageState */
  uint8_t m_state;

  /** if this block is currently being flushed to disk, this tells the
   * flush_type.  @see enum BufFlushType */
  uint8_t m_flush_type;

  /** type of pending I/O operation; also protected by buf_pool_mutex
  @see enum * BufIoFix */
  uint8_t m_io_fix;

  /** count of how manyfold this block is currently bufferfixed. Written
  only under the block mutex, with relaxed atomic stores (see
  BufBlock::fix_inc()) because asserts read it without that mutex. */
  uint32_t m_buf_fix_count;
  /* @} */

  bool m_old;

  /** the value of BufPool::freed_page_clock (its low 31 bits) when this
  block was the last time put to the head of the LRU list; a thread is
  allowed to read this for heuristic purposes without holding any mutex or
  latch. Was a 31-bit bit-field: a plain word now so that the unlatched
  reads can be relaxed atomic loads (ut::stat_load), the writers use
  ut::stat_store under the buf_pool mutex. */
  uint32_t m_freed_page_clock;

  /** time of first access, or 0 if the block was never accessed in the
  buffer pool. Written under the buf_pool mutex, read without it as a
  heuristic (buf_page_is_accessed()): relaxed atomic accesses. */
  uint32_t m_access_time{};

  /** @name Page flushing fields
  All these are protected by buf_pool_mutex. */
  /* @{ */
  UT_LIST_NODE_T(BufPage) m_list;

  /** based on state, this is a list node, protected only by buf_pool_mutex, in
  one of the following lists in buf_pool:

  - BUF_BLOCK_NOT_USED: free

  The contents of the list node is undefined if !in_flush_list && state ==
  BUF_BLOCK_FILE_PAGE, or if state is one of BUF_BLOCK_MEMORY,
  BUF_BLOCK_REMOVE_HASH or
  BUF_BLOCK_READY_IN_USE. */

  /** log sequence number of the youngest modification to this block, zero if
  not modified */
  lsn_t m_newest_modification;

  /** log sequence number of the START of the log entry written of the oldest
  modification to this block which has not yet been flushed on disk; zero if
  all modifications are on disk */
  lsn_t m_oldest_modification;

  /* @} */

  /** @name LRU replacement algorithm fields
  These fields are protected by buf_pool_mutex only
  not the buf_block_t::mutex). */
  /* @{ */

  /** node of the LRU list */
  UT_LIST_NODE_T(BufPage) m_LRU_list;

  /* @} */

#ifdef UNIV_DEBUG
  /** true if in BufPool::page_hash */
  bool m_in_page_hash;

  /** true if in BufPool::flush_list; when buf_pool_mutex is free, the
  following should hold: in_flush_list == (state == BUF_BLOCK_FILE_PAGE) */
  bool m_in_flush_list;

  /** true if in BufPool::free; when buf_pool_mutex is free, the following
  should hold: in_free_list == (state == BUF_BLOCK_NOT_USED) */
  bool m_in_free_list;

  /** true if the page is in the LRU list; used in debugging */
  bool m_in_LRU_list;

  /** this is set to true when fsp
  frees a page in buffer pool */
  bool m_file_page_was_freed;
#endif /* UNIV_DEBUG */
};

/** We need this to alias a buf_block_t from a BufPage. */
static_assert(std::is_standard_layout<BufPage>::value, "BufPage must have a standard layout");

/** The buffer control block structure */
struct BufBlock {
  /**
   * Gets the freed_page_clock of the buffer block.
   *
   * @return The freed_page_clock.
   */
  [[nodiscard]] ulint get_freed_page_clock() const { return m_page.get_freed_page_clock(); }

  /**
  * Gets the space id of a block.
  *
  * @return Space id.
  */
  [[nodiscard]] space_id_t get_space() const {
    ut_a(get_state() == BUF_BLOCK_FILE_PAGE);

    return m_page.get_space();
  }

  /**
  * Gets the page number of a block.
  *
  * @return Page number
  */
  [[nodiscard]] page_no_t get_page_no() const { return m_page.get_page_no(); }

  /**
   * Gets the page id of a block.
   *
   * @return The page id.
   */
  [[nodiscard]] PageId get_page_id() const noexcept { return {get_space(), get_page_no()}; }

  /**
  * Gets the state of a block.
  *
  * @return The state of the block.
  */
  [[nodiscard]] BufPageState get_state() const;

  /**
   * @brief Decrements the buffer fix count.
   *
   * This function decrements the bufferfix count of a block,
   * indicating that the block is no longer fixed in the buffer pool.
   *
   * @param block Pointer to the buffer block.
   */
  void fix_dec();

  /**
   * Gets a pointer to the memory frame of a block.
   *
   * @return Pointer to the frame.
   */
  buf_frame_t *get_frame() const;

  /** Acquire block mutex. */
  void acquire_mutex() const { mutex_enter(&m_mutex); }

  /** Release the block mutex. */
  void release_mutex() const { mutex_exit(&m_mutex); }

  /** @name General fields */
  /* @{ */

  /** page information; this must be the first field, so that
  BufPool::page_hash can point to BufPage or buf_block_t */
  BufPage m_page;

  /** mutex protecting this block: state (also protected by the buffer
  pool mutex), io_fix, buf_fix_count, and accessed. */
  mutable Mutex m_mutex;

  /** RW lock protoecting the buffer frame state changes. */
  mutable RwLock m_rw_lock;

  /* What every read and every change of the page looks at, on a line of its own: away from the
  block mutex and the fix count above, that the buffer pool writes on every get of the page. The rest
  of the line is what's written only with the page owned, or never. */

  /** Who owns the regions of the page, in memory only: it isn't on the page and doesn't survive a
  restart. A page has regions, the header and the groups of records under its directory slots, and a
  set bit is an exclusive hold on one. A compare and swap takes any set of them at once, so there's no
  order to take them in. A group has a bit from its owner record's offset: BufBlock::group_bit(). Two
  groups can have the same bit, which is safe, they just wait for each other.

  A change to the bytes of one record that are no key (Intent::Mark) has the bit of its group. Anything
  else has ALL_BITS: the structure of the page, and what any search reads, changes only then. HEADER_BIT
  is also how one that wants ALL_BITS gets it: it stops new holders of group bits, and waits for the
  ones there are to leave. */
  alignas(64) mutable std::atomic<uint64_t> m_bits;

  /** What a read with no ownership validates against: odd while ALL_BITS is held, so it changes around
  everything that changes the structure of the frame. A read reads the frame and sees the same even
  value before and after. Page I/O doesn't count: nobody reads a frame that is being read in, a buffer
  fix waits. */
  std::atomic<uint64_t> m_version;

  /** Around the writes of one record's bytes by a holder of a group bit: began and ended. A read of a
  record has no use for the bit: the holder can be busy elsewhere for a long time, with undo or the log,
  with the record as it was. It only has to not overlap the writes: both the same before, and the first
  the same after. A write anywhere on the page makes it go again, one record's copy. */
  std::atomic<uint32_t> m_rec_writes_begun;
  std::atomic<uint32_t> m_rec_writes_ended;

  /** Work for whoever owns the page next: for the index implementation, a stack of its own kind of
  operation, each in the frame of a thread that waits for it. */
  std::atomic<void *> m_queued_ops;

  /** pointer to buffer frame which is of size UNIV_PAGE_SIZE, and
  aligned to an address divisible by UNIV_PAGE_SIZE */
  byte *m_frame;

  /** this clock is incremented every time a pointer to a record on the
  page may become obsolete; this is used in the optimistic cursor
  positioning: if the modify clock has not changed, we know that the pointer
  is still valid; this field may be changed if the thread (1) owns the
  pool mutex and the page is not bufferfixed, or (2) the thread has an
  x-latch on the block */
  uint64_t m_modify_clock;

  /** true if we know that this is an index page, and want the database
  to check its consistency before flush; note that there may be pages in the
  buffer pool which are index pages, but this flag is not set because
  we do not keep track of all pages; NOT protected by any mutex */
  bool m_check_index_page_at_flush;

  static constexpr uint64_t HEADER_BIT = uint64_t{1} << 63;

  /** Somebody sleeps on m_bits: not a region. A notify on it is a write to a line that all the blocks
  share, in the standard library, even with nobody to wake: so only when this says so. */
  static constexpr uint64_t WAITERS_BIT = uint64_t{1} << 62;

  static constexpr uint64_t ALL_BITS = ~WAITERS_BIT;

  /** The number of bits for groups. */
  static constexpr ulint N_GROUP_BITS = 62;

  /** @return the bit of a group, from the offset of its owner record. */
  [[nodiscard]] static ulint group_bit(ulint owner) noexcept { return ((owner * 2654435761UL) >> 16) % N_GROUP_BITS; }

#ifdef UNIV_DEBUG
  /** Who owns it: a thread that waits for itself is a bug to report, not to hang on. */
  std::atomic<uint64_t> m_bits_owner;
#endif /* UNIV_DEBUG */

  /** Own the page, the caller has it buffer fixed. Waits for an owner, and for a write of the page
  to the file to finish: that reads the frame with no ownership of it.
  @param[in] rw_x  The caller has the X latch: the write of the page can't start or be running, the
                   flusher has the page's S latch for that. It can be marked already, the flusher
                   waiting for this thread's latch, so it isn't to be waited for. */
  void bits_lock(bool rw_x = false) noexcept;

  /** @return true if the page is owned now. Doesn't wait.
  @param[in] rw_x  See bits_lock(). */
  [[nodiscard]] bool bits_try_lock(bool rw_x = false) noexcept;

  /** Give up the ownership. */
  void bits_unlock() noexcept;

  /** Own a group of records, the caller has the page buffer fixed. Waits as bits_lock() does. */
  void group_lock(ulint bit) noexcept;

  /** Give up a group of records. */
  void group_unlock(ulint bit) noexcept;

  /** Sleep until the bits change, setting WAITERS_BIT for it to be noticed. For one that saw them held. */
  void bits_wait(uint64_t bits) const noexcept;

  /** Start a read of a record, under no bit.
  @param[out] begun             What to pass to rec_read_is_valid().
  @return false if a record is being written: there's no point. */
  [[nodiscard]] bool rec_read_begin(uint32_t &begun) const noexcept {
    begun = m_rec_writes_begun.load(std::memory_order_acquire);

    return m_rec_writes_ended.load(std::memory_order_acquire) == begun;
  }

  /** @return true if no record was written during the read since rec_read_begin(). */
  [[nodiscard]] bool rec_read_is_valid(uint32_t begun) const noexcept {
    std::atomic_thread_fence(std::memory_order_acquire);

    return m_rec_writes_begun.load(std::memory_order_relaxed) == begun;
  }

  /** Around the writes of a record's bytes: all of what a read must see together. */
  struct RecWrite {
    explicit RecWrite(BufBlock *block) noexcept : m_block(block) {
      m_block->m_rec_writes_begun.fetch_add(1, std::memory_order_relaxed);

      /* It's seen as begun before any of the writes. */
      std::atomic_thread_fence(std::memory_order_release);
    }

    ~RecWrite() noexcept { m_block->m_rec_writes_ended.fetch_add(1, std::memory_order_release); }

    BufBlock *m_block;
  };

  /** For the flusher, with the page marked as being written: on return no thread owns the page, and
  any that tries sees the mark and gives it up without having changed anything. */
  void bits_wait_free() noexcept;

  /** To be called with the X latch just taken: an RW latch on the page owns it too, so a read sees
  that change. Only the outermost hold of a recursive one counts. */
  void x_locked() noexcept {
    if (rw_lock_get_x_lock_count(&m_rw_lock) == 1) {
      bits_lock(true);
    }
  }

  /** To be called with the X latch about to be released. */
  void x_unlocking() noexcept {
    if (rw_lock_get_x_lock_count(&m_rw_lock) == 1) {
      bits_unlock();
    }
  }

  /** Start a read of the frame with no latch, by a thread that has it buffer fixed.
  @return what to pass to read_is_valid(). Odd if there is a writer: don't bother. */
  [[nodiscard]] uint64_t read_begin() const noexcept { return m_version.load(std::memory_order_acquire); }

  /** @return true if what was read since read_begin() is what the page had at one time. Before
  this it can be anything, half of one write and half of another. */
  [[nodiscard]] bool read_is_valid(uint64_t version) const noexcept {
    std::atomic_thread_fence(std::memory_order_acquire);

    return (version & 1) == 0 && m_version.load(std::memory_order_relaxed) == version;
  }

  /** Sleep for now, for a read that saw an owner of all of it. */
  void read_wait() const noexcept {
    if (const auto bits = m_bits.load(std::memory_order_relaxed); (bits & HEADER_BIT) != 0) {
      bits_wait(bits);
    }
  }

  /* @} */

  /** Note that this is an index page, to be checked before it's flushed.
  Callers hold only an S latch (or none), so several threads set it at
  once and the flusher reads it: relaxed atomic accesses. Test first so
  the common case (already set) doesn't dirty the block's cache line on
  every B-tree descent. */
  void set_check_index_page_at_flush() {
    if (!ut::stat_load(m_check_index_page_at_flush)) {
      ut::stat_store(m_check_index_page_at_flush, true);
    }
  }

  /** Clear m_check_index_page_at_flush, see set_check_index_page_at_flush(). */
  void clear_check_index_page_at_flush() { ut::stat_store(m_check_index_page_at_flush, false); }

  /** @return m_check_index_page_at_flush (relaxed load: a hint). */
  [[nodiscard]] bool get_check_index_page_at_flush() const { return ut::stat_load(m_check_index_page_at_flush); }

  /** @name Optimistic search field */
  /* @{ */


  /* @} */
  /** @name Hash search fields (unprotected)
  NOTE that these fields are NOT protected by any semaphore! */
  /* @{ */

  /* @} */

};

/** Check if a buf_block_t object is in a valid state
@param block buffer block
@return true if valid */
#define buf_block_state_valid(block) \
  (buf_block_get_state(block) >= BUF_BLOCK_NOT_USED && (buf_block_get_state(block) <= BUF_BLOCK_REMOVE_HASH))

/** @brief The buffer pool statistics structure. */
struct BufPoolStat {
  /* All fields are ut::Relaxed: n_page_gets is bumped without any latch,
  the others under the buf_pool mutex, and they are all read without it
  (master thread I/O heuristics, monitor output, export_innodb_status).
  Relaxed keeps those reads race free and the struct copyable. */

  /** number of page gets performed; this field is NOT protected
  by the buffer pool mutex */
  ut::Relaxed<ulint> n_page_gets{};

  /** number read operations */
  ut::Relaxed<ulint> n_pages_read{};

  /** number write operations */
  ut::Relaxed<ulint> n_pages_written{};

  /** number of pages created in the pool with no read */
  ut::Relaxed<ulint> n_pages_created{};

  /** number of pages read in as part of read ahead */
  ut::Relaxed<ulint> n_ra_pages_read{};

  /** number of read ahead pages that are evicted without
  being accessed */
  ut::Relaxed<ulint> n_ra_pages_evicted{};

  /** number of pages made young, in calls to m_LRU->make_block_young() */
  ut::Relaxed<ulint> n_pages_made_young{};

  /** number of pages not made young because the first access
  was not long enough ago, in buf_page_peek_if_too_old() */
  ut::Relaxed<ulint> n_pages_not_made_young{};
};

/** @brief The buffer pool structure.

NOTE! The definition appears here only for other modules of this
directory (buf) to see it. Do not use from outside! */
struct BufPool {
  /** The instance this sub-system belongs to. */
  System &m_sys;

  using page_hash_t = Page_id_hash<BufPage *>;

  struct Request {
    /** RW_S_LATCH or RW_X_LATCH */
    ulint m_rw_latch{};

    union {
      PageId m_page_id{};

      /** Known or guessed block. */
      BufBlock *m_guess;
    };

    union {
      /** Modify clock value if node is ..._GUESS_ON_CLOCK */
      uint64_t m_modify_clock{};

      /** BUF_GET, BUF_GET_IF_IN_POOL, BUF_GET_NO_LATCH. */
      ulint m_mode;
    };

    /** File from where called. */
    const char *m_file{};

    /** Line in file from where called. */
    ulint m_line{};

    /** Mini-transaction to track the latches. */
    Mtr *m_mtr{};
  };

  static_assert(std::is_standard_layout<Request>::value, "Request must have a standard layout");

  /** Default constructor. */
  explicit BufPool(System &sys) noexcept;

  /** Destructor. */
  ~BufPool() noexcept;

  [[nodiscard]] bool open(uint64_t pool_size);


  /** Returns the number of pending buf pool ios.
  @return number of pending I/O operations */
  [[nodiscard]] ulint get_n_pending_ios();

  /** Prints info of the buffer i/o.
  @para,[in,out] stderr      File write to write. */
  void print_io(FILE *out);

  /** Returns the ratio in percents of modified pages in the buffer pool /
  database pages in the buffer pool.
  @return modified page percentage ratio */
  [[nodiscard]] ulint buf_get_modified_ratio_pct();

  /** Refreshes the statistics used to print per-second averages. */
  void refresh_io_stats();

  /** Asserts that all file pages in the buffer are in a replaceable state.
  @return true */
  [[nodiscard]] bool all_freed();

  /** Checks that there currently are no pending i/o-operations for the buffer pool.
  @return true if there is pending I/O */
  [[nodiscard]] bool is_io_pending();

  /** Invalidates the file pages in the buffer pool when a batch of crash
  recovery is applied and written. All the file pages buffered must be in a replaceable state when
  this function is called: not latched and not modified. */
  void invalidate();

  /** Returns the ratio in percents of modified pages in the buffer pool /
  database pages in the buffer pool.
  @return	modified page percentage ratio */
  ulint get_modified_ratio_pct();


  /** Prepares the buffer pool for shutdown. */
  void close();

  /**
   * Get optimistic access to a database page.
   * @param[in,out]       Request.
   */
  bool try_get(Request &req);

  /**
   * This is used to get access to a known database page, when no waiting can be done.
   *
   * @param[in]       Get request
   * @return          true if success
   */
  bool try_get_known_nowait(Request &request);

  /*** Given a tablespace id and page number tries to get that page. If the page is not in
  the buffer pool it is not loaded and nullptr is returned. Suitable for using when holding
  the kernel mutex.
  @param[in,out] req       Request
  @return page or nullptr */
  const BufBlock *try_get_by_page_id(Request &req);

  /**
   * This is the general function used to get access to a database page.
   *
   * @param[in,out] req    Request
   * @param[in] guess      Hint
   *
   * @return          pointer to the block or nullptr
   */
  BufBlock *get(Request &req, BufBlock *guess);

  /**
   * @brief Initializes a page to the buffer buf_pool. The page is usually not read
   * from a file even if it cannot be found in the buffer buf_pool. This is
   * one of the functions which perform to a block a state transition
   * NOT_USED => FILE_PAGE (the other is BufPool::get).
   *
   * @param page_id   in: page ID containing space and page number
   * @param mtr       in: mini-transaction handle
   * @return          pointer to the block, page bufferfixed
   */
  [[nodiscard]] BufBlock *create(const PageId &page_id, Mtr *mtr);

  /**
   * @brief Inits a page to the buffer buf_pool.
   *
   * @param page_id in: page ID containing space and page number
   * @param block in: block to init
   */
  void page_init(const PageId &page_id, BufBlock *block);

  /**
   * Moves a page to the start of the buffer pool LRU list. This high-level
   * function can be used to prevent an important page from slipping out of
   * the buffer pool.
   *
   * @param bpage     in: buffer block of a file page
   */
  void make_young(BufPage *bpage);

  /**
   * Resets the check_index_page_at_flush field of a page if found in the buffer pool.
   *
   * @param page_id   in: page ID containing space and page number
   */
  void check_index_page_at_flush(const PageId &page_id);

  /**
   * Gets the current size of the buffer pool in bytes.
   *
   * @return The size of the buffer pool in bytes.
   */
  [[nodiscard]] uint64_t get_curr_size() { return m_curr_size * UNIV_PAGE_SIZE; }

  /**
   * Gets the smallest oldest_modification lsn for any page in the pool.
   * Returns zero if all modified pages have been flushed to disk.
   *
   * @return The oldest modification in the pool, zero if none.
  */
  [[nodiscard]] uint64_t get_oldest_modification() const;

  /**
  * @brief The size in pages of the area which the read-ahead algorithms read if invoked
  */
  [[nodiscard]] ulint get_read_ahead_area() const { return std::min(ulint(64), ut_2_power_up(m_curr_size / 32)); }

  /** Allocates a buffer block.
  @return own: the allocated block, in state BUF_BLOCK_MEMORY */
  [[nodiscard]] BufBlock *block_alloc();

  /**
   * @brief Frees a buffer block which does not contain a file page.
   *
   * @param block Pointer to the buffer block to be freed.
   */
  void block_free(BufBlock *block);

  /**
   * @brief Decrements the bufferfix count of a buffer control block and releases a latch, if specified.
   *
   * @param block The buffer block.
   * @param rw_latch The type of latch (RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH).
   * @param mtr The mtr.
   */
  void release(BufBlock *block, ulint rw_latch, Mtr *mtr);

  /**
   * Checks if a page is corrupt.
   *
   * @param read_buf  in: a database page
   * @return          true if corrupted
   */
  [[nodiscard]] bool is_corrupted(const byte *read_buf);

  /**
   * @brief Returns the control block of a file page, nullptr if not found.
   *
   * This function retrieves the control block of a file page based on
   * the page ID.
   *
   * @param page_id The page ID containing space and page number.
   * @return The control block of the file page, nullptr if not found.
  */
  [[nodiscard]] BufPage *hash_get_page(const PageId &page_id);

  /**
   * @brief Returns the control block of a file page.
   *
   * This function retrieves the control block of a file page based on the page ID.
   *
   * @param page_id The page ID containing space and page number.
   * @return The control block of the file page, nullptr if not found.
   */
  [[nodiscard]] BufBlock *hash_get_block(const PageId &page_id);

  /**
   * @brief Checks if the page can be found in the buffer pool hash table.
   *
   * Note that it is possible that the page is not yet read from disk.
   *
   * @param page_id The page ID containing space and page number.
   * @return true if found in the page hash table, false otherwise.
   */
  [[nodiscard]] bool peek(const PageId &page_id);

  /** Gets the current length of the free list of buffer blocks.
  @return	length of the free list */
  [[nodiscard]] ulint get_free_list_len();

  /** Gets the block to whose frame the pointer is pointing to.
  @param[in] ptr                 Pointer to a frame.
  @return pointer to block, never nullptr */
  [[nodiscard]] BufBlock *block_align(const byte *ptr);

  /** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
  the buf_block_t itself or a member of it
  @param[in] ptr                  Pointer not dereferenced
  @return true if ptr belongs to a buf_block_t struct */
  [[nodiscard]] bool pointer_is_block_field(const void *ptr);

  /**
   * Initializes a page for reading into the buffer pool. If the page is
   * already in the buffer pool, or if we specify to read only ibuf pages
   * and the page is not an ibuf page, or if the space is deleted or being
   * deleted, then this function does nothing. Sets the io_fix flag to
   * BUF_IO_READ and sets a non-recursive exclusive lock on the buffer frame.
   * The io-handler must take care that the flag is cleared and the lock released later.
   *
   * @param[out] err - Pointer to the error code (DB_SUCCESS or DB_TABLESPACE_DELETED).
   * @param page_id - The page ID containing space and page number.
   * @param[in] tablespace_version - Prevents reading from a wrong version of the
   *  tablespace in case we have done DISCARD + IMPORT.
   * @return Pointer to the block or nullptr.
   */
  BufPage *init_for_read(DbErr *err, const PageId &page_id, int64_t tablespace_version);

  /**
   * @brief Completes an asynchronous read or write request of a file page to or from the buffer pool.
   *
   * @param bpage Pointer to the block in question.
   */
  void io_complete(BufPage *bpage);

  /**
   * @brief Acquires the buffer pool mutex.
   */
  void mutex_acquire() noexcept { mutex_enter(&m_mutex); }

#ifdef UNIV_DEBUG
  /** Prints info of the buffer pool data structure. */
  void print();

  /**
   * Sets file_page_was_freed true if the page is found in the buffer pool.
   * This function should be called when we free a file page and want the
   * debug version to check that it is not accessed any more unless reallocated.
   *
   * @param page_id   in: page ID containing space and page number
   * @return          control block if found in page hash table, otherwise nullptr
   */
  BufPage *set_file_page_was_freed(const PageId &page_id);

  /** Returns the number of latched pages in the buffer pool.
  @return        number of latched pages */
  ulint get_latched_pages_number();

  /** Check the state of the buffer pool instance.
  @return true if it is consistent. */
  bool validate();

  /**
   * @brief Checks if the buffer pool mutex is owned.
   *
   * @return True if the buffer pool mutex is owned, false otherwise.
   */
  [[nodiscard]] bool mutex_is_owned() const noexcept { return mutex_own(&m_mutex); }

  /** Forbid the release of the buffer pool mutex. */
  inline void mutex_exit_forbid() noexcept {
    ut_ad(mutex_own(&m_mutex));
    ++m_mutex_exit_forbidden;
  }

  /** Allow the release of the buffer pool mutex. */
  inline void mutex_exit_allow() noexcept {
    ut_ad(mutex_own(&m_mutex));
    ut_a(m_mutex_exit_forbidden);
    --m_mutex_exit_forbidden;
  }

  /** Release the buffer pool mutex. */
  inline void mutex_release() noexcept {
    ut_a(m_mutex_exit_forbidden == 0);
    mutex_exit(&m_mutex);
  }

#else /* UNIV_DEBUG */
/** Forbid the release of the buffer pool mutex. */
#define mutex_exit_forbid() ((void)0)

/** Allow the release of the buffer pool mutex. */
#define mutex_exit_allow() ((void)0)

  /** Release the buffer pool mutex. */
  inline void mutex_release() noexcept { mutex_exit(&m_mutex); }

#endif /* UNIV_DEBUG */
       /* @} */
 private:
  /**
   * @brief Sets the time of the first access of a page and moves a page to the
   * start of the buffer pool LRU list if it is too old.
   *
   * This high-level function can be used to prevent an important page from slipping
   * out of the buffer pool.
   *
   * @param bpage Buffer block of a file page (in/out)
   * @param access_time Access time of the page (in: bpage->m_access_time read under mutex protection, or 0 if unknown)
   */
  void set_accessed_make_young(BufPage *bpage, unsigned access_time);

  /**
   * @brief Inits a page to the buffer buf_pool.
   *
   * @param space in: space id
   * @param page_no in: Page number within space
   * @param block in: block to init
   */
  void page_init(space_id_t space, page_no_t page_no, BufBlock *block);

  /** Recommends a move of a block to the start of the LRU list if there is danger
   * of dropping from the buffer pool. NOTE: does not reserve the buffer pool mutex.
   * @param[in,out] bpage            Block to make younger.
   * @return true if should be made younger */
  bool peek_if_too_old(const BufPage *bpage);

  /**
   * @brief Allocates a chunk of buffer frames.
   *
   * This function initializes a buffer chunk and allocates a memory block for buffer frames.
   *
   * @param[out] chunk Pointer to the buffer chunk structure.
   * @param[in] mem_size Requested size of the memory block in bytes.
   * @return Pointer to the initialized buffer chunk, or nullptr on failure.
   */
  BufChunk *chunk_init(BufChunk *chunk, ulint mem_size);

  /**
   * @brief Checks that all file pages in the buffer chunk are in a replaceable state.
   *
   * @param[in] chunk Chunk being checked.
   * @return Address of a non-free block, or nullptr if all freed.
   */
  const BufBlock *chunk_not_freed(BufChunk *chunk);

  /**
   * @brief Initializes a buffer control block when the buf_pool is created.
   *
   * @param block Pointer to the control block.
   * @param frame Pointer to the buffer frame.
   */
  void block_init(BufBlock *block, byte *frame);

  /**
   * @brief Initialize some fields of a control block.
   *
   * @param bpage The block to initialize.
   */
  void page_init_low(BufPage *bpage);

 public:

  /** mutex protecting the buffer pool struct and control blocks, except the
  read-write lock in them */
  mutable Mutex m_mutex{};

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  /** This is used to insert validation operations in excution in the debug version.
  Bumped by every page get without any latch: relaxed, a lost tick only moves
  the next validate() a little. */
  mutable ut::Relaxed<ulint> m_dbg_counter{};

  /** Flag to forbid the release of the buffer pool mutex.
  Protected by buf_pool_mutex. */
  mutable ulint m_mutex_exit_forbidden{};

#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  /** @name General fields */
  /* @{ */

  /** number of buffer pool chunks */
  ulint m_n_chunks{};

  /** buffer pool chunks */
  BufChunk *m_chunks{};

  /** current pool size in pages */
  ulint m_curr_size{};

  /** hash table of BufPage or buf_block_t file pages,
  BufPage::in_file() == true, indexed by (m_nspace_id, m_page_no) */
  page_hash_t *m_page_hash{};

  /** Number of pending read operations. Written under the buf_pool mutex,
  read without it for heuristics and monitoring (get_n_pending_ios(),
  recovery read throttling): relaxed. */
  ut::Relaxed<ulint> m_n_pend_reads{};

  /** When buf_print_io was last time called */
  time_t m_last_printout_time{};

  /** current statistics */
  BufPoolStat m_stat{};

  /** old statistics */
  BufPoolStat m_old_stat{};

  /* @} */

  /** @name Page flushing algorithm fields */

  /* @{ */

  /** base node of the modified block list */
  UT_LIST_BASE_NODE_T(BufPage, m_list) m_flush_list;

  /** this is true when a flush of the given type is being initialized */
  bool m_init_flush[BUF_FLUSH_N_TYPES]{};

  /** This is the number of pending writes in the given flush type. Written
  under the buf_pool mutex, read without it in get_n_pending_ios() and
  is_io_pending() (heuristics): relaxed. */
  ut::Relaxed<ulint> m_n_flush[BUF_FLUSH_N_TYPES]{};

  /** this is in the set state when there is no flush batch of the given type
   * running */
  CondVar *m_no_flush[BUF_FLUSH_N_TYPES]{};

  /** A red-black tree is used exclusively during recovery to speed up
  insertions in the flush_list. This tree contains blocks in order of
  oldest_modification LSN and is kept in sync with the flush_list.
  Each member of the tree MUST also be on the flush_list. This tree
  is relevant only in recovery and is set to nullptr once the recovery is over. */
  Buf_page_set *m_recovery_flush_list{};

  /** a sequence number used to count the number of buffer blocks removed
  from the end of the LRU list; NOTE that this counter may wrap around
  at 4 billion! A thread is allowed to read this for heuristic purposes
  without holding any mutex or latch: relaxed. Written under the mutex. */
  ut::Relaxed<ulint> m_freed_page_clock{};

  /** when an LRU flush ends for a page, this is incremented by one;
  this is set to zero when a buffer block is allocated */
  ulint m_LRU_flush_ended{};

  /* @} */
  /** @name LRU replacement algorithm fields */
  /* @{ */

  /** base node of the free block list */
  UT_LIST_BASE_NODE_T(BufPage, m_list) m_free_list {};

  /** base node of the LRU list */
  UT_LIST_BASE_NODE_T(BufPage, m_LRU_list) m_LRU_list {};

  /** pointer to the about m_LRU->old_ratio/BUF_LRU_RATIO_DIV oldest
  blocks in the LRU list; nullptr if LRU length less than BUF_LRU_MIN_LEN;
  NOTE: when LRU_old != nullptr, its length should always equal LRU_old_len */
  BufPage *m_LRU_old{};

  /** length of the LRU list from the block to which LRU_old points onward,
  including that block; see buf0lru.c for the restrictions on this value;
  0 if LRU_old == nullptr; NOTE: LRU_old_len must be adjusted whenever LRU_old
  shrinks or grows! */
  ulint m_LRU_old_len{};

  /** Number of write requests issued (a statistic) */
  ut::Relaxed<ulint> m_write_requests{};

  /** LRU replacement algorithm */
  std::unique_ptr<BufLru> m_LRU{};

  /** Page flushing algorithm */
  std::unique_ptr<BufFlush> m_flusher{};
};

/** We need this to alias a buf_block_t from a BufPage. */
static_assert(std::is_standard_layout<BufBlock>::value, "buf_block_t must have a standard layout");

/** Create an instance of set for buffer_page_t */
inline Buf_page_set *flush_set_create(Buf_page_set_cmp_t cmp) {
  return new Buf_page_set(std::move(cmp), UtAllocator<BufPage *>{});
}

/** Free te instance of set for buffer_page_t */
inline void flush_set_destroy(Buf_page_set *&tree) {
  delete tree;
  tree = nullptr;
}

/** Insert buf_page in the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline std::pair<Buf_page_set_itr, bool> flush_set_insert(Buf_page_set *tree, BufPage *t) {
  return tree->emplace(t);
}

/** Delete buf_page from the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline bool flush_set_erase(Buf_page_set *tree, BufPage *t) {
  auto delete_count = tree->erase(t);
  return delete_count > 0;
}

/** Get the first element of the set.
 * @return iterator to the first element of the set if it exists. */
inline std::optional<Buf_page_set_itr> flush_set_first(Buf_page_set *tree) {
  if (tree->empty()) {
    return {};
  } else {
    return tree->begin();
  }
}

/** Get the previous node of the current node.
@return iterator to the prev element if it exists. */
inline std::optional<Buf_page_set_itr> flush_set_prev(Buf_page_set *tree, Buf_page_set_itr itr) {
  if (itr == tree->begin()) {
    return {};
  } else {
    --itr;
    return itr;
  }
}

/** Get the next node of the current node.
@return iterator to the next element if it exists. */
inline std::optional<Buf_page_set_itr> flush_set_next(Buf_page_set *tree, Buf_page_set_itr itr) {
  if (itr == tree->end() || (++itr) == tree->end()) {
    return {};
  } else {
    return itr;
  }
}
