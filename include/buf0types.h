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

#include "innodb0types.h"

#include <functional>
#include <optional>
#include <set>
#include <sstream>
#include <unordered_map>

#include "sync0mutex.h"
#include "sync0rw.h"
#include "ut0mem.h"

/** Mini-transaction. */
struct mtr_t;

/** The buffer pool page flusher. */
struct Buf_flush;

/** Buffer pool LRU implementation. */
struct Buf_LRU;

/** Buffer page (uncompressed or compressed) */
struct Buf_page;

/** Buffer block for which an uncompressed page exists */
struct Buf_block;

/** Buffer pool chunk comprising buf_block_t */
struct buf_chunk_t;

/** Buffer pool comprising buf_chunk_t */
struct Buf_pool;

/** Buffer pool statistics struct */
struct buf_pool_stat_t;

/** A buffer frame. @see page_t */
using buf_frame_t = byte;

/** Flags for flush types */
enum buf_flush {

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
enum buf_io_fix {
  /** No pending I/O */
  BUF_IO_NONE = 0,

  /** Read pending */
  BUF_IO_READ,

  /** Write pending */
  BUF_IO_WRITE
};

struct Fil_addr;

/** @name Modes for Buf_pool::get */
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

/** The buffer pool of the database */
extern Buf_pool *srv_buf_pool;

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
struct ut_allocator {
  using value_type = T;

  ut_allocator() noexcept = default;

  template <class U>
  explicit ut_allocator(const ut_allocator<U> &) noexcept {}

  T *allocate(std::size_t n) { return new (ut_new(sizeof(T) * n)) T[n]; }

  void deallocate(T *p, std::size_t n) {
    call_destructor(p, n);
    ut_delete(p);
  }
};

template <class T, class U>
constexpr bool operator==(const ut_allocator<T> &, const ut_allocator<U> &) noexcept {
  return true;
}

template <class T, class U>
constexpr bool operator!=(const ut_allocator<T> &, const ut_allocator<U> &) noexcept {
  return false;
}

/* set typedefs for buffer_page_t */
using Buf_page_set_cmp_t = std::function<bool(Buf_page *, Buf_page *)>;

using Buf_page_set = std::set<Buf_page *, Buf_page_set_cmp_t, ut_allocator<Buf_page *>>;

using Buf_page_set_itr = typename Buf_page_set::iterator;

/** @brief States of a control block
@see Buf_page

The enumeration values must be 0..7. */
enum Buf_page_state {
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

/** The common buffer control block structure
for compressed and uncompressed frames */
struct Buf_page {

  /**
   * Reads the freed_page_clock of the buffer page.
   *
   * @return The freed_page_clock.
   */
  [[nodiscard]] ulint get_freed_page_clock() const {
    /* This is sometimes read without holding buf_pool_mutex. */
    return m_freed_page_clock;
  }

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
  [[nodiscard]] Page_id get_page_id() const noexcept { return {get_space(), get_page_no()}; }

  /**
  * Gets the state of a block.
  *
  * @param bpage Pointer to the control block.
  * @return The state of the block.
  */
  [[nodiscard]] Buf_page_state get_state() const;

  /** @return cast the instance to a Buf_page pointer. */
  [[nodiscard]] Buf_block *get_block() { return reinterpret_cast<Buf_block *>(this); }

  /** @return cast the instance to a Buf_page pointer. */
  const Buf_block *get_block() const { return reinterpret_cast<const Buf_block *>(this); }

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
  @see enum Buf_page_state */
  unsigned m_state : 3;

  /** if this block is currently being flushed to disk, this tells the
   * flush_type.  @see enum buf_flush */
  unsigned m_flush_type : 2;

  /** type of pending I/O operation; also protected by buf_pool_mutex
  @see enum * buf_io_fix */
  unsigned m_io_fix : 2;

  /** count of how manyfold this block is currently bufferfixed */
  unsigned m_buf_fix_count : 25;
  /* @} */

  bool m_old;

  /** the value of Buf_pool::freed_page_clock when this block was the last time
  put to the head of the LRU list; a thread is allowed to read this for
  heuristic purposes without holding any mutex or latch */
  unsigned m_freed_page_clock : 31;

  /** time of first access, or 0 if the block was never accessed in the
  buffer pool */
  uint32_t m_access_time{};

  /** @name Page flushing fields
  All these are protected by buf_pool_mutex. */
  /* @{ */
  UT_LIST_NODE_T(Buf_page) m_list;

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
  UT_LIST_NODE_T(Buf_page) m_LRU_list;

  /* @} */

#ifdef UNIV_DEBUG
  /** true if in Buf_pool::page_hash */
  bool m_in_page_hash;

  /** true if in Buf_pool::flush_list; when buf_pool_mutex is free, the
  following should hold: in_flush_list == (state == BUF_BLOCK_FILE_PAGE) */
  bool m_in_flush_list;

  /** true if in Buf_pool::free; when buf_pool_mutex is free, the following
  should hold: in_free_list == (state == BUF_BLOCK_NOT_USED) */
  bool m_in_free_list;

  /** true if the page is in the LRU list; used in debugging */
  bool m_in_LRU_list;

  /** this is set to true when fsp
  frees a page in buffer pool */
  bool m_file_page_was_freed;
#endif /* UNIV_DEBUG */
};

/** We need this to alias a buf_block_t from a Buf_page. */
static_assert(std::is_standard_layout<Buf_page>::value, "Buf_page must have a standard layout");

/** The buffer control block structure */
struct Buf_block {
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
  [[nodiscard]] Page_id get_page_id() const noexcept { return {get_space(), get_page_no()}; }

  /**
  * Gets the state of a block.
  *
  * @return The state of the block.
  */
  [[nodiscard]] Buf_page_state get_state() const;

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
  Buf_pool::page_hash can point to Buf_page or buf_block_t */
  Buf_page m_page;

  /** pointer to buffer frame which is of size UNIV_PAGE_SIZE, and
  aligned to an address divisible by UNIV_PAGE_SIZE */
  byte *m_frame;

  /** mutex protecting this block: state (also protected by the buffer
  pool mutex), io_fix, buf_fix_count, and accessed. */
  mutable mutex_t m_mutex;

  /** RW lock protoecting the buffer frame state changes. */
  mutable rw_lock_t m_rw_lock;

  /** true if we know that this is an index page, and want the database
  to check its consistency before flush; note that there may be pages in the
  buffer pool which are index pages, but this flag is not set because
  we do not keep track of all pages; NOT protected by any mutex */
  bool m_check_index_page_at_flush;
  /* @} */

  /** @name Optimistic search field */
  /* @{ */

  /** this clock is incremented every time a pointer to a record on the
  page may become obsolete; this is used in the optimistic cursor
  positioning: if the modify clock has not changed, we know that the pointer
  is still valid; this field may be changed if the thread (1) owns the
  pool mutex and the page is not bufferfixed, or (2) the thread has an
  x-latch on the block */
  uint64_t m_modify_clock;

  /* @} */
  /** @name Hash search fields (unprotected)
  NOTE that these fields are NOT protected by any semaphore! */
  /* @{ */

  /* @} */

#ifdef UNIV_SYNC_DEBUG
  /** @name Debug fields */
  /* @{ */
  /** in the debug version, each thread which bufferfixes the block
  acquires an s-latch here; so we can use the debug utilities in sync0rw */
  rw_lock_t debug_latch;
  /* @} */
#endif /* UNIV_SYNC_DEBUG */
};

/** Check if a buf_block_t object is in a valid state
@param block buffer block
@return true if valid */
#define buf_block_state_valid(block) \
  (buf_block_get_state(block) >= BUF_BLOCK_NOT_USED && (buf_block_get_state(block) <= BUF_BLOCK_REMOVE_HASH))

/** @brief The buffer pool statistics structure. */
struct buf_pool_stat_t {
  /** number of page gets performed; this field is NOT protected
  by the buffer pool mutex */
  ulint n_page_gets{};

  /** number read operations */
  ulint n_pages_read{};

  /** number write operations */
  ulint n_pages_written{};

  /** number of pages created in the pool with no read */
  ulint n_pages_created{};

  /** number of pages read in as part of read ahead */
  ulint n_ra_pages_read{};

  /** number of read ahead pages that are evicted without
  being accessed */
  ulint n_ra_pages_evicted{};

  /** number of pages made young, in calls to m_LRU->make_block_young() */
  ulint n_pages_made_young{};

  /** number of pages not made young because the first access
  was not long enough ago, in buf_page_peek_if_too_old() */
  ulint n_pages_not_made_young{};
};

/** @brief The buffer pool structure.

NOTE! The definition appears here only for other modules of this
directory (buf) to see it. Do not use from outside! */
struct Buf_pool {
  using page_hash_t = Page_id_hash<Buf_page *>;

  struct Request {
    /** RW_S_LATCH or RW_X_LATCH */
    ulint m_rw_latch{};

    union {
      Page_id m_page_id{};

      /** Known or guessed block. */
      Buf_block *m_guess;
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
    mtr_t *m_mtr{};
  };

  static_assert(std::is_standard_layout<Request>::value, "Request must have a standard layout");

  /** Default constructor. */
  Buf_pool();

  /** Destructor. */
  ~Buf_pool() noexcept;

  [[nodiscard]] bool open(uint64_t pool_size);

  /** Returns the number of pending buf pool ios.
  @return number of pending I/O operations */
  [[nodiscard]] ulint get_n_pending_ios();

  /** Prints info of the buffer i/o.
  @para,[in,out] ib_stream      File write to write. */
  void print_io(ib_stream_t ib_stream);

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

  /** Invalidates the file pages in the buffer pool when an archive recovery is
  completed. All the file pages buffered must be in a replaceable state when
  this function is called: not latched and not modified. */
  void invalidate();

  /** Returns the ratio in percents of modified pages in the buffer pool /
  database pages in the buffer pool.
  @return	modified page percentage ratio */
  ulint get_modified_ratio_pct();

  /** Reset the buffer variables. */
  void init();

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
  const Buf_block *try_get_by_page_id(Request &req);

  /**
   * This is the general function used to get access to a database page.
   *
   * @param[in,out] req    Request
   * @param[in] guess      Hint
   *
   * @return          pointer to the block or nullptr
   */
  Buf_block *get(Request &req, Buf_block *guess);

  /**
   * @brief Initializes a page to the buffer buf_pool. The page is usually not read
   * from a file even if it cannot be found in the buffer buf_pool. This is
   * one of the functions which perform to a block a state transition
   * NOT_USED => FILE_PAGE (the other is Buf_pool::get).
   *
   * @param page_id   in: page ID containing space and page number
   * @param mtr       in: mini-transaction handle
   * @return          pointer to the block, page bufferfixed
   */
  [[nodiscard]] Buf_block *create(const Page_id &page_id, mtr_t *mtr);

  /**
   * @brief Inits a page to the buffer buf_pool.
   *
   * @param page_id in: page ID containing space and page number
   * @param block in: block to init
   */
  void page_init(const Page_id &page_id, Buf_block *block);

  /**
   * Moves a page to the start of the buffer pool LRU list. This high-level
   * function can be used to prevent an important page from slipping out of
   * the buffer pool.
   *
   * @param bpage     in: buffer block of a file page
   */
  void make_young(Buf_page *bpage);

  /**
   * Resets the check_index_page_at_flush field of a page if found in the buffer pool.
   *
   * @param page_id   in: page ID containing space and page number
   */
  void check_index_page_at_flush(const Page_id &page_id);

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
  [[nodiscard]] Buf_block *block_alloc();

  /**
   * @brief Frees a buffer block which does not contain a file page.
   *
   * @param block Pointer to the buffer block to be freed.
   */
  void block_free(Buf_block *block);

  /**
   * @brief Decrements the bufferfix count of a buffer control block and releases a latch, if specified.
   *
   * @param block The buffer block.
   * @param rw_latch The type of latch (RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH).
   * @param mtr The mtr.
   */
  void release(Buf_block *block, ulint rw_latch, mtr_t *mtr);

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
  [[nodiscard]] Buf_page *hash_get_page(const Page_id &page_id);

  /**
   * @brief Returns the control block of a file page.
   *
   * This function retrieves the control block of a file page based on the page ID.
   *
   * @param page_id The page ID containing space and page number.
   * @return The control block of the file page, nullptr if not found.
   */
  [[nodiscard]] Buf_block *hash_get_block(const Page_id &page_id);

  /**
   * @brief Checks if the page can be found in the buffer pool hash table.
   *
   * Note that it is possible that the page is not yet read from disk.
   *
   * @param page_id The page ID containing space and page number.
   * @return true if found in the page hash table, false otherwise.
   */
  [[nodiscard]] bool peek(const Page_id &page_id);

  /** Gets the current length of the free list of buffer blocks.
  @return	length of the free list */
  [[nodiscard]] ulint get_free_list_len();

  /** Gets the block to whose frame the pointer is pointing to.
  @param[in] ptr                 Pointer to a frame.
  @return pointer to block, never nullptr */
  [[nodiscard]] Buf_block *block_align(const byte *ptr);

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
  Buf_page *init_for_read(db_err *err, const Page_id &page_id, int64_t tablespace_version);

  /**
   * @brief Completes an asynchronous read or write request of a file page to or from the buffer pool.
   *
   * @param bpage Pointer to the block in question.
   */
  void io_complete(Buf_page *bpage);

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
  Buf_page *set_file_page_was_freed(const Page_id &page_id);

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
  void set_accessed_make_young(Buf_page *bpage, unsigned access_time);

  /**
   * @brief Inits a page to the buffer buf_pool.
   *
   * @param space in: space id
   * @param page_no in: Page number within space
   * @param block in: block to init
   */
  void page_init(space_id_t space, page_no_t page_no, Buf_block *block);

  /** Recommends a move of a block to the start of the LRU list if there is danger
   * of dropping from the buffer pool. NOTE: does not reserve the buffer pool mutex.
   * @param[in,out] bpage            Block to make younger.
   * @return true if should be made younger */
  bool peek_if_too_old(const Buf_page *bpage);

  /**
   * @brief Allocates a chunk of buffer frames.
   *
   * This function initializes a buffer chunk and allocates a memory block for buffer frames.
   *
   * @param[out] chunk Pointer to the buffer chunk structure.
   * @param[in] mem_size Requested size of the memory block in bytes.
   * @return Pointer to the initialized buffer chunk, or nullptr on failure.
   */
  buf_chunk_t *chunk_init(buf_chunk_t *chunk, ulint mem_size);

  /**
   * @brief Checks that all file pages in the buffer chunk are in a replaceable state.
   *
   * @param[in] chunk Chunk being checked.
   * @return Address of a non-free block, or nullptr if all freed.
   */
  const Buf_block *chunk_not_freed(buf_chunk_t *chunk);

  /**
   * @brief Initializes a buffer control block when the buf_pool is created.
   *
   * @param block Pointer to the control block.
   * @param frame Pointer to the buffer frame.
   */
  void block_init(Buf_block *block, byte *frame);

  /**
   * @brief Initialize some fields of a control block.
   *
   * @param bpage The block to initialize.
   */
  void page_init_low(Buf_page *bpage);

 public:
  /** mutex protecting the buffer pool struct and control blocks, except the
  read-write lock in them */
  mutable mutex_t m_mutex{};

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  /** This is used to insert validation operations in excution in the debug version */
  mutable ulint m_dbg_counter{};

  /** Flag to forbid the release of the buffer pool mutex.
  Protected by buf_pool_mutex. */
  mutable ulint m_mutex_exit_forbidden{};

#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  /** @name General fields */
  /* @{ */

  /** number of buffer pool chunks */
  ulint m_n_chunks{};

  /** buffer pool chunks */
  buf_chunk_t *m_chunks{};

  /** current pool size in pages */
  ulint m_curr_size{};

  /** hash table of Buf_page or buf_block_t file pages,
  Buf_page::in_file() == true, indexed by (m_nspace_id, m_page_no) */
  page_hash_t *m_page_hash{};

  /** Number of pending read operations */
  ulint m_n_pend_reads{};

  /** When buf_print_io was last time called */
  time_t m_last_printout_time;

  /** current statistics */
  buf_pool_stat_t m_stat;

  /** old statistics */
  buf_pool_stat_t m_old_stat;

  /* @} */

  /** @name Page flushing algorithm fields */

  /* @{ */

  /** base node of the modified block list */
  UT_LIST_BASE_NODE_T(Buf_page, m_list) m_flush_list;

  /** this is true when a flush of the given type is being initialized */
  bool m_init_flush[BUF_FLUSH_N_TYPES];

  /** This is the number of pending writes in the given flush type */
  ulint m_n_flush[BUF_FLUSH_N_TYPES];

  /** this is in the set state when there is no flush batch of the given type
   * running */
  Cond_var *m_no_flush[BUF_FLUSH_N_TYPES];

  /** A red-black tree is used exclusively during recovery to speed up
  insertions in the flush_list. This tree contains blocks in order of
  oldest_modification LSN and is kept in sync with the flush_list.
  Each member of the tree MUST also be on the flush_list. This tree
  is relevant only in recovery and is set to nullptr once the recovery is over. */
  Buf_page_set *m_recovery_flush_list{};

  /** a sequence number used to count the number of buffer blocks removed
  from the end of the LRU list; NOTE that this counter may wrap around
  at 4 billion! A thread is allowed to read this for heuristic purposes
  without holding any mutex or latch */
  ulint m_freed_page_clock{};

  /** when an LRU flush ends for a page, this is incremented by one;
  this is set to zero when a buffer block is allocated */
  ulint m_LRU_flush_ended{};

  /* @} */
  /** @name LRU replacement algorithm fields */
  /* @{ */

  /** base node of the free block list */
  UT_LIST_BASE_NODE_T(Buf_page, m_list) m_free_list {};

  /** base node of the LRU list */
  UT_LIST_BASE_NODE_T(Buf_page, m_LRU_list) m_LRU_list {};

  /** pointer to the about m_LRU->old_ratio/BUF_LRU_RATIO_DIV oldest
  blocks in the LRU list; nullptr if LRU length less than BUF_LRU_MIN_LEN;
  NOTE: when LRU_old != nullptr, its length should always equal LRU_old_len */
  Buf_page *m_LRU_old{};

  /** length of the LRU list from the block to which LRU_old points onward,
  including that block; see buf0lru.c for the restrictions on this value;
  0 if LRU_old == nullptr; NOTE: LRU_old_len must be adjusted whenever LRU_old
  shrinks or grows! */
  ulint m_LRU_old_len{};

  /** Number of write requests issued */
  ulint m_write_requests{};

  /** LRU replacement algorithm */
  std::unique_ptr<Buf_LRU> m_LRU{};

  /** Page flushing algorithm */
  std::unique_ptr<Buf_flush> m_flusher{};
};

/** We need this to alias a buf_block_t from a Buf_page. */
static_assert(std::is_standard_layout<Buf_block>::value, "buf_block_t must have a standard layout");

/** Create an instance of set for buffer_page_t */
inline Buf_page_set *flush_set_create(Buf_page_set_cmp_t cmp) {
  return new Buf_page_set(std::move(cmp), ut_allocator<Buf_page *>{});
}

/** Free te instance of set for buffer_page_t */
inline void flush_set_destroy(Buf_page_set *&tree) {
  delete tree;
  tree = nullptr;
}

/** Insert buf_page in the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline std::pair<Buf_page_set_itr, bool> flush_set_insert(Buf_page_set *tree, Buf_page *t) {
  return tree->emplace(t);
}

/** Delete buf_page from the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline bool flush_set_erase(Buf_page_set *tree, Buf_page *t) {
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
