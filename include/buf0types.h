/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

#include "hash0hash.h"
#include "sync0mutex.h"
#include "sync0rw.h"
#include "ut0mem.h"

/** The buffer pool page flusher. */
struct Buf_flush;

/** Buffer page (uncompressed or compressed) */
struct buf_page_t;

/** Buffer block for which an uncompressed page exists */
struct buf_block_t;

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

struct fil_addr_t;

/** @name Modes for buf_page_get_gen */
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
extern Buf_pool *buf_pool;

#ifdef UNIV_DEBUG
/*! If this is set true, the program prints info whenever read or flush occurs */
extern bool buf_debug_prints;
#endif /* UNIV_DEBUG */

/** variable to count write request issued */
extern ulint srv_buf_pool_write_requests;

/** Magic value to use instead of checksums when they are disabled */
constexpr ulint BUF_NO_CHECKSUM_MAGIC = 0xDEADBEEFUL;

/*** Simple allocator using ut_malloc and ur_free */
template <typename T>
struct ut_allocator {
  using value_type = T;
  ut_allocator() noexcept = default;

  template <class U>
  explicit ut_allocator(const ut_allocator<U> &) noexcept {}

  T *allocate(std::size_t n) { return (T *)ut_malloc(sizeof(T) * n); }

  void deallocate(T *p, std::size_t n) { ut_free(p); }
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
using buf_page_rbt_cmp_t = std::function<bool(buf_page_t *, buf_page_t *)>;

using buf_page_rbt_t = std::set<buf_page_t *, buf_page_rbt_cmp_t, ut_allocator<buf_page_t *>>;

using buf_page_rbt_itr_t = typename buf_page_rbt_t::iterator;
/** @brief States of a control block
@see buf_page_t

The enumeration values must be 0..7. */
enum buf_page_state {
  /** Is in the free list */
  BUF_BLOCK_NOT_USED,

  /** When buf_LRU_get_free_block returns a block, it is in this state */
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
struct buf_page_t {

   /**
   * Reads the freed_page_clock of the buffer page.
   *
   * @return The freed_page_clock.
   */
  ulint get_freed_page_clock() const {
    /* This is sometimes read without holding buf_pool_mutex. */
    return m_freed_page_clock;
  }

  /**
  * Gets the space id of a block.
  *
  * @param bpage Pointer to the control block.
  * @return Space id.
  */
  space_id_t get_space() const;

  /**
  * Gets the state of a block.
  *
  * @param bpage Pointer to the control block.
  * @return The state of the block.
  */
  buf_page_state get_state() const;

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
  @see enum buf_page_state */
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

  /** the value of buf_pool->freed_page_clock when this block was the last time
  put to the head of the LRU list; a thread is allowed to read this for
  heuristic purposes without holding any mutex or latch */
  unsigned m_freed_page_clock : 31;

  /** time of first access, or 0 if the block was never accessed in the
  buffer pool */
  uint32_t m_access_time{};

  /** node used in chaining to buf_pool->page_hash */
  buf_page_t *m_hash;

  /** @name Page flushing fields
  All these are protected by buf_pool_mutex. */
  /* @{ */
  UT_LIST_NODE_T(buf_page_t) m_list;

  /** based on state, this is a list node, protected only by buf_pool_mutex, in
  one of the following lists in buf_pool:

  - BUF_BLOCK_NOT_USED:	free

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
  UT_LIST_NODE_T(buf_page_t) m_lru_list;

  /* @} */

#ifdef UNIV_DEBUG
  /** true if in buf_pool->page_hash */
  bool m_in_page_hash;

  /** true if in buf_pool->flush_list; when buf_pool_mutex is free, the
  following should hold: in_flush_list == (state == BUF_BLOCK_FILE_PAGE) */
  bool m_in_flush_list;

  /** true if in buf_pool->free; when buf_pool_mutex is free, the following
  should hold: in_free_list == (state == BUF_BLOCK_NOT_USED) */
  bool m_in_free_list;

  /** true if the page is in the LRU list; used in debugging */
  bool m_in_lru_list;

  /** this is set to true when fsp
  frees a page in buffer pool */
  bool m_file_page_was_freed;
#endif /* UNIV_DEBUG */
};

/** We need this to alias a buf_block_t from a buf_page_t. */
static_assert(std::is_standard_layout<buf_page_t>::value, "buf_page_t mustt have a standard layout");

/** The buffer control block structure */
struct buf_block_t {
  /**
   * Gets the freed_page_clock of the buffer block.
   *
   * @return The freed_page_clock.
   */
  ulint get_freed_page_clock() const {
   return m_page.get_freed_page_clock();
  }

  /**
  * Gets the space id of a block.
  *
  * @return Space id.
  */
  space_id_t get_space() const;

  /**
  * Gets the state of a block.
  *
  * @return The state of the block.
  */
  buf_page_state  get_state() const;

  /** @name General fields */
  /* @{ */

  /** page information; this must be the first field, so that
  buf_pool->page_hash can point to buf_page_t or buf_block_t */
  buf_page_t m_page;

  /** pointer to buffer frame which is of size UNIV_PAGE_SIZE, and
  aligned to an address divisible by UNIV_PAGE_SIZE */
  byte *m_frame;

  /** mutex protecting this block: state (also protected by the buffer
  pool mutex), io_fix, buf_fix_count, and accessed. */
  mutex_t m_mutex;

  /** RW lock protoecting the buffer frame state changes. */
  rw_lock_t m_rw_lock;

  /** hashed value of the page address in the record lock hash table;
  protected by buf_block_t::lock (or buf_block_t::mutex, buf_pool_mutex
  in buf_page_get_gen(), buf_page_init_for_read() and buf_page_create()) */
  uint32_t m_lock_hash_val;

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
#endif
};

/** Check if a buf_block_t object is in a valid state
@param block	buffer block
@return		true if valid */
#define buf_block_state_valid(block) \
  (buf_block_get_state(block) >= BUF_BLOCK_NOT_USED && (buf_block_get_state(block) <= BUF_BLOCK_REMOVE_HASH))

/** @brief The buffer pool statistics structure. */
struct buf_pool_stat_t {
  /** number of page gets performed; this field is NOT protected
  by the buffer pool mutex */
  ulint n_page_gets;

  /** number read operations */
  ulint n_pages_read;

  /** number write operations */
  ulint n_pages_written;

  /** number of pages created in the pool with no read */
  ulint n_pages_created;

  /** number of pages read in as part of read ahead */
  ulint n_ra_pages_read;

  /** number of read ahead pages that are evicted without
  being accessed */
  ulint n_ra_pages_evicted;

  /** number of pages made young, in calls to buf_LRU_make_block_young() */
  ulint n_pages_made_young;

  /** number of pages not made young because the first access
  was not long enough ago, in buf_page_peek_if_too_old() */
  ulint n_pages_not_made_young;
};

/** @brief The buffer pool structure.

NOTE! The definition appears here only for other modules of this
directory (buf) to see it. Do not use from outside! */
struct Buf_pool {
  /**
   * Gets the current size of the buffer pool in bytes.
   *
   * @return The size of the buffer pool in bytes.
   */
  uint64_t get_curr_size() {
    return m_curr_size * UNIV_PAGE_SIZE;
  }

  /**
   * Gets the smallest oldest_modification lsn for any page in the pool.
   * Returns zero if all modified pages have been flushed to disk.
   *
   * @return The oldest modification in the pool, zero if none.
  */
  uint64_t get_oldest_modification() const;


  /** Allocates a buffer block.
  @return	own: the allocated block, in state BUF_BLOCK_MEMORY */
  static buf_block_t *m_block_alloc();

  /** @name General fields */
  /* @{ */

  /** number of buffer pool chunks */
  ulint m_n_chunks;

  /** buffer pool chunks */
  buf_chunk_t *m_chunks;

  /** current pool size in pages */
  ulint m_curr_size;

  /** hash table of buf_page_t or buf_block_t file pages,
   buf_page_in_file() == true, indexed by (m_nspace_id, m_page_no) */
  hash_table_t *m_page_hash;

  ulint m_n_pend_reads; /** number of pending read operations */

  /** when buf_print_io was last time called */
  time_t m_last_printout_time;

  /** current statistics */
  buf_pool_stat_t m_stat;

  /** old statistics */
  buf_pool_stat_t m_old_stat;

  /* @} */

  /** @name Page flushing algorithm fields */

  /* @{ */

  /** base node of the modified block list */
  UT_LIST_BASE_NODE_T(buf_page_t, m_list) m_flush_list;

  /** this is true when a flush of the given type is being initialized */
  bool m_init_flush[BUF_FLUSH_N_TYPES];

  /** This is the number of pending writes in the given flush type */
  ulint m_n_flush[BUF_FLUSH_N_TYPES];

  /** this is in the set state when there is no flush batch of the given type
   * running */
  OS_cond* m_no_flush[BUF_FLUSH_N_TYPES];

  /** A red-black tree is used exclusively during recovery to speed up
  insertions in the flush_list. This tree contains blocks in order of
  oldest_modification LSN and is kept in sync with the flush_list.
  Each member of the tree MUST also be on the flush_list. This tree
  is relevant only in recovery and is set to nullptr once the recovery is over. */
  buf_page_rbt_t *m_recovery_flush_list;

  /** a sequence number used to count the number of buffer blocks removed
  from the end of the LRU list; NOTE that this counter may wrap around
  at 4 billion! A thread is allowed to read this for heuristic purposes
  without holding any mutex or latch */
  ulint m_freed_page_clock;

  /** when an LRU flush ends for a page, this is incremented by one;
  this is set to zero when a buffer block is allocated */
  ulint m_lru_flush_ended;

  /* @} */
  /** @name LRU replacement algorithm fields */
  /* @{ */

  /** base node of the free block list */
  UT_LIST_BASE_NODE_T(buf_page_t, m_list) m_free_list;

  /** base node of the LRU list */
  UT_LIST_BASE_NODE_T(buf_page_t, m_lru_list) m_lru_list;

  /** pointer to the about buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV oldest
  blocks in the LRU list; nullptr if LRU length less than BUF_LRU_OLD_MIN_LEN;
  NOTE: when LRU_old != nullptr, its length should always equal LRU_old_len */
  buf_page_t *m_lru_old;

  /** length of the LRU list from the block to which LRU_old points onward,
  including that block; see buf0lru.c for the restrictions on this value;
  0 if LRU_old == nullptr; NOTE: LRU_old_len must be adjusted whenever LRU_old
  shrinks or grows! */
  ulint m_lru_old_len;

  // FIXME: Convert to a std::unique_ptr
  Buf_flush* m_flusher;

  /* @} */
};

/** We need this to alias a buf_block_t from a buf_page_t. */
static_assert(std::is_standard_layout<buf_block_t>::value, "buf_block_t must have a standard layout");

/** Create an instance of set for buffer_page_t */
inline buf_page_rbt_t *rbt_create(buf_page_rbt_cmp_t cmp) {
  return new buf_page_rbt_t(std::move(cmp), ut_allocator<buf_page_t *>{});
}

/** Free te instance of set for buffer_page_t */
inline void rbt_free(buf_page_rbt_t *tree) {
  delete tree;
}

/** Insert buf_page in the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline std::pair<buf_page_rbt_itr_t, bool> rbt_insert(buf_page_rbt_t *tree, buf_page_t *t) {
  return tree->emplace(t);
}

/** Delete buf_page from the set.
@return pair of inserted node if successful. success is determined by second element of pair. */
inline bool rbt_delete(buf_page_rbt_t *tree, buf_page_t *t) {
  auto delete_count = tree->erase(t);
  return delete_count > 0;
}

/** Get the first element of the set.
 * @return iterator to the first element of the set if it exists. */
inline std::optional<buf_page_rbt_itr_t> rbt_first(buf_page_rbt_t *tree) {
  if (tree->empty()) {
    return {};
  } else {
   return tree->begin();
  }
}

/** Get the previous node of the current node.
@return iterator to the prev element if it exists. */
inline std::optional<buf_page_rbt_itr_t> rbt_prev(buf_page_rbt_t *tree, buf_page_rbt_itr_t itr) {
  if (itr == tree->begin()) {
    return {};
  } else {
    --itr;
    return itr;
  }
}

/** Get the next node of the current node.
@return iterator to the next element if it exists. */
inline std::optional<buf_page_rbt_itr_t> rbt_next(buf_page_rbt_t *tree, buf_page_rbt_itr_t itr) {
  if (itr == tree->end() || (++itr) == tree->end()) {
    return {};
  } else {
    return itr;
  }
}
