/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.

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

/*** @file include/buf0buf.h
The database buffer pool high-level routines

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "fil0types.h"
#include "hash0hash.h"
#include "mtr0types.h"
#include "page0types.h"
#include "sync0rw.h"

struct fil_addr_t;
typedef struct ib_rbt_struct ib_rbt_t;

/*** Returns a free block from the buf_pool. The block is taken off the
free list. If it is empty, blocks are moved from the end of the
LRU list to the free list.
@return the free control block, in state BUF_BLOCK_READY_FOR_USE */
buf_block_t *buf_LRU_get_free_block();

/** This function should be called at a mini-transaction commit, if a page was
modified in it. Puts the block to the list of modified blocks, if it is not
already in it.
@param[in,out] block              BLock which is modified
@param[in,out] mtr                Mini-transaction that modified it. */
void buf_flush_note_modification(buf_block_t *block, mtr_t *mtr);

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
extern buf_pool_t *buf_pool;

#ifdef UNIV_DEBUG
/*! If this is set true, the program prints info whenever read or flush occurs */
extern bool buf_debug_prints;
#endif /* UNIV_DEBUG */

/** variable to count write request issued */
extern ulint srv_buf_pool_write_requests;

/** Magic value to use instead of checksums when they are disabled */
#define BUF_NO_CHECKSUM_MAGIC 0xDEADBEEFUL

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

/*** Creates the buffer pool.
@return	own: buf_pool object, nullptr if not enough memory or error */
buf_pool_t *buf_pool_init();

/*** Prepares the buffer pool for shutdown. */
void buf_close();

/*** Frees the buffer pool at shutdown.  This must not be invoked after
freeing all mutexes. */
void buf_mem_free();

/*** Resizes the buffer pool. */
void buf_pool_resize();

/*** NOTE! The following macros should be used instead of buf_page_get_gen,
to improve debugging. Only values RW_S_LATCH and RW_X_LATCH are allowed
in LA! */
#define buf_page_get(SP, UNUSED, PG, LA, MTR) buf_page_get_gen(SP, PG, LA, nullptr, BUF_GET, __FILE__, __LINE__, MTR)

/*** Use these macros to bufferfix a page with no latching. Remember not to
read the contents of the page unless you know it is safe. Do not modify
the contents of the page! We have separated this case, because it is
error-prone programming not to set a latch, and it should be used
with care. */
#define buf_page_get_with_no_latch(SP, UNUSED, PG, MTR) \
  buf_page_get_gen(SP, PG, RW_NO_LATCH, nullptr, BUF_GET_NO_LATCH, __FILE__, __LINE__, MTR)

/**
 * This is the general function used to get optimistic access to a database page.
 * @param rw_latch      in: RW_S_LATCH, RW_X_LATCH
 * @param block         in: guessed block
 * @param modify_clock  in: modify clock value if mode is ..._GUESS_ON_CLOCK
 * @param file          in: file name
 * @param line          in: line where called
 * @param mtr           in: mini-transaction
 * @return              true if success
 */
bool buf_page_optimistic_get(ulint rw_latch, buf_block_t *block, uint64_t modify_clock, const char *file, ulint line, mtr_t *mtr);

/**
 * This is used to get access to a known database page, when no waiting can be done.
 *
 * @param rw_latch  in: RW_S_LATCH, RW_X_LATCH
 * @param block     in: the known page
 * @param mode      in: BUF_MAKE_YOUNG or BUF_KEEP_OLD
 * @param file      in: file name
 * @param line      in: line where called
 * @param mtr       in: mini-transaction
 * @return          true if success
 */
bool buf_page_get_known_nowait(ulint rw_latch, buf_block_t *block, ulint mode, const char *file, ulint line, mtr_t *mtr);

/*** Given a tablespace id and page number tries to get that page. If the
page is not in the buffer pool it is not loaded and nullptr is returned.
Suitable for using when holding the kernel mutex.
@param[in] space_id       Tablespace id
@param[in] page_no        Page number
@param[in] file           File name
@param[in] line           Line where called
@param[in,out] mtr        Mini-transaction  */
const buf_block_t *buf_page_try_get_func(space_id_t space_id, page_no_t page_no, const char *file, ulint line, mtr_t *mtr);

/** Tries to get a page. If the page is not in the buffer pool it is
not loaded.  Suitable for using when holding the kernel mutex.
@param space_id	in: tablespace id
@param page_no	in: page number
@param mtr	in: mini-transaction
@return		the page if in buffer pool, nullptr if not */
#define buf_page_try_get(space_id, page_no, mtr) buf_page_try_get_func(space_id, page_no, __FILE__, __LINE__, mtr);

/**
 * This is the general function used to get access to a database page.
 *
 * @param space     in: space id
 * @param offset    in: page number
 * @param rw_latch  in: RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH
 * @param guess     in: guessed block or nullptr
 * @param mode      in: BUF_GET, BUF_GET_IF_IN_POOL, BUF_GET_NO_LATCH
 * @param file      in: file name
 * @param line      in: line where called
 * @param mtr       in: mini-transaction
 * 
 * @return          pointer to the block or nullptr
 */
buf_block_t *buf_page_get_gen(
  space_id_t space, page_no_t offset, ulint rw_latch, buf_block_t *guess, ulint mode, const char *file, ulint line, mtr_t *mtr
);

/**
 * Initializes a page to the buffer buf_pool. The page is usually not read
 * from a file even if it cannot be found in the buffer buf_pool. This is
 * one of the functions which perform to a block a state transition
 * NOT_USED => FILE_PAGE (the other is buf_page_get_gen).
 *
 * @param space     in: space id
 * @param offset    in: offset of the page within space in units of a page
 * @param mtr       in: mini-transaction handle
 * @return          pointer to the block, page bufferfixed
 */
buf_block_t *buf_page_create(space_id_t space, page_no_t offset, mtr_t *mtr);

/**
 * Inits a page to the buffer buf_pool, for use in ibbackup --restore.
 *
 * @param space  in: space id
 * @param offset in: offset of the page within space in units of a page
 * @param block  in: block to init
 */
void buf_page_init_for_backup_restore(space_id_t space, page_no_t offset, ulint, buf_block_t *block);

/**
 * Moves a page to the start of the buffer pool LRU list. This high-level
 * function can be used to prevent an important page from slipping out of
 * the buffer pool.
 *
 * @param bpage     in: buffer block of a file page
 */
void buf_page_make_young(buf_page_t *bpage);

/**
 * Resets the check_index_page_at_flush field of a page if found in the buffer pool.
 *
 * @param space     in: space id
 * @param offset    in: page number
 */
void buf_reset_check_index_page_at_flush(space_id_t space, page_no_t offset);

#ifdef UNIV_DEBUG
/**
 * Sets file_page_was_freed true if the page is found in the buffer pool.
 * This function should be called when we free a file page and want the
 * debug version to check that it is not accessed any more unless reallocated.
 *
 * @param space     in: space id
 * @param offset    in: page number
 * @return          control block if found in page hash table, otherwise nullptr
 */
buf_page_t *buf_page_set_file_page_was_freed(space_id_t space, page_no_t offset);

/**
 * Sets file_page_was_freed false if the page is found in the buffer
 * pool. This function should be called when we free a file page and
 * want the debug version to check that it is not accessed any more unless
 * reallocated.
 *
 * @param space     in: space id
 * @param offset    in: page number
 * @return          control block if found in page hash table, otherwise nullptr
 */
buf_page_t *buf_page_reset_file_page_was_freed(space_id_t space, page_no_t offset);
#endif /* UNIV_DEBUG */

/**
 * Returns the current state of is_hashed of a page. false if the page
 * is not in the pool.
 * NOTE that this operation does not fix the page in the pool if it is found there.
 *
 * @param space     in: space id
 * @param offset    in: page number
 * @return          true if page hash index is built in search system
 */
bool buf_page_peek_if_search_hashed(space_id_t space, page_no_t offset);

/**
 * Calculates a page checksum which is stored to the page when it is
 * written to a file. Note that we must be careful to calculate the
 * same value on 32-bit and 64-bit architectures.
 *
 * @param page      in: buffer page
 * @return          checksum
 */
ulint buf_calc_page_new_checksum(const byte *page);

/**
 * In versions < 4.0.14 and < 4.1.1 there was a bug that the
 * checksum only looked at the first few bytes of the page.
 * This calculates that old checksum.
 * NOTE: we must first store the new formula checksum to
 * FIL_PAGE_SPACE_OR_CHKSUM before calculating and storing this old checksum
 * because this takes that field as an input!
 *
 * @param page      in: buffer page
 * @return          checksum
 */
ulint buf_calc_page_old_checksum(const byte *page);

/**
 * Checks if a page is corrupt.
 *
 * @param read_buf  in: a database page
 * @return          true if corrupted
 */
bool buf_page_is_corrupted(const byte *read_buf);

/**
 * @brief Decrements the bufferfix count of a buffer control block and releases a latch, if specified.
 *
 * @param block The buffer block.
 * @param rw_latch The type of latch (RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH).
 * @param mtr The mtr.
 */
void buf_page_release(buf_block_t *block, ulint rw_latch, mtr_t *mtr);

/**
 * Gets the space id, page offset, and byte offset within page of a
 * pointer pointing to a buffer frame containing a file page.
 *
 * @param ptr       in: pointer to a buffer frame
 * @param space     out: space id
 * @param addr      out: page offset and byte offset
 */
inline void buf_ptr_get_fsp_addr(const void *ptr, ulint *space, fil_addr_t *addr);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/*** Validates the buffer pool data structure.
@return	true */
bool buf_validate();
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#if defined UNIV_DEBUG_PRINT || defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/*** Prints info of the buffer pool data structure. */
void buf_print(void);
#endif /* UNIV_DEBUG_PRINT || UNIV_DEBUG || UNIV_BUF_DEBUG */

/*** Prints a page to stderr. */
void buf_page_print(
  const byte *read_buf, /** in: a database page */
  ulint
);

#ifdef UNIV_DEBUG

/*** Returns the number of latched pages in the buffer pool.
@return	number of latched pages */
ulint buf_get_latched_pages_number(void);
#endif /* UNIV_DEBUG */

/*** Returns the number of pending buf pool ios.
@return	number of pending I/O operations */
ulint buf_get_n_pending_ios();

/*** Prints info of the buffer i/o. */
void buf_print_io(ib_stream_t ib_stream); /** in: file where to print */

/*** Returns the ratio in percents of modified pages in the buffer pool /
database pages in the buffer pool.
@return	modified page percentage ratio */
ulint buf_get_modified_ratio_pct();

/*** Refreshes the statistics used to print per-second averages. */
void buf_refresh_io_stats();

/*** Asserts that all file pages in the buffer are in a replaceable state.
@return	true */
bool buf_all_freed();

/*** Checks that there currently are no pending i/o-operations for the buffer
pool.
@return	true if there is no pending i/o */
bool buf_pool_check_no_pending_io();

/*** Invalidates the file pages in the buffer pool when an archive recovery is
completed. All the file pages buffered must be in a replaceable state when
this function is called: not latched and not modified. */
void buf_pool_invalidate();

/*** Reset the buffer variables. */
void buf_var_init();

/* --------------------------- LOWER LEVEL ROUTINES ------------------------- */

#ifdef UNIV_SYNC_DEBUG
/*** Adds latch level info for the rw-lock protecting the buffer frame. This
should be called in the debug version after a successful latching of a
page if we know the latching order level of the acquired latch. */
inline void buf_block_dbg_add_level(
  buf_block_t *block, /** in: buffer page
                                              where we have acquired latch */
  ulint level
);                                            /** in: latching order level */
#else                                         /* UNIV_SYNC_DEBUG */
#define buf_block_dbg_add_level(block, level) /* nothing */
#endif                                        /* UNIV_SYNC_DEBUG */

#ifdef UNIV_DEBUG
/*** Gets a pointer to the memory frame of a block.
@return	pointer to the frame */
inline buf_frame_t *buf_block_get_frame(const buf_block_t *block) /** in: pointer to the control block */
  __attribute__((pure));
#else /* UNIV_DEBUG */
#define buf_block_get_frame(block) (block)->frame
#endif /* UNIV_DEBUG */

/*** Gets the block to whose frame the pointer is pointing to.
@return	pointer to block, never nullptr */
buf_block_t *buf_block_align(const byte *ptr); /** in: pointer to a frame */

/*** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
the buf_block_t itself or a member of it
@return	true if ptr belongs to a buf_block_t struct */
bool buf_pointer_is_block_field(const void *ptr); /** in: pointer not
                                                   dereferenced */

/** Find out if a pointer corresponds to a buf_block_t::mutex.
@param m	in: mutex candidate
@return		true if m is a buf_block_t::mutex */
#define buf_pool_is_block_mutex(m) buf_pointer_is_block_field((const void *)(m))

/** Find out if a pointer corresponds to a buf_block_t::lock.
@param l	in: rw-lock candidate
@return		true if l is a buf_block_t::lock */
#define buf_pool_is_block_lock(l) buf_pointer_is_block_field((const void *)(l))

/**
 * Initializes a page for reading into the buffer pool. If the page is
 * already in the buffer pool, or if we specify to read only ibuf pages
 * and the page is not an ibuf page, or if the space is deleted or being
 * deleted, then this function does nothing. Sets the io_fix flag to
 * BUF_IO_READ and sets a non-recursive exclusive lock on the buffer frame.
 * The io-handler must take care that the flag is cleared and the lock released later.
 *
 * @param[out] err - Pointer to the error code (DB_SUCCESS or DB_TABLESPACE_DELETED).
 * @param mode - The mode of reading (BUF_READ_ANY_PAGE, ...).
 * @param space - The space id.
 * @param[in] tablespace_version - Prevents reading from a wrong version of the tablespace in case we have done DISCARD + IMPORT.
 * @param offset - The page number.
 * @return Pointer to the block or nullptr.
 */
buf_page_t *buf_page_init_for_read(db_err *err, ulint mode, space_id_t space, int64_t tablespace_version, page_no_t offset);

/**
 * @brief Completes an asynchronous read or write request of a file page to or from the buffer pool.
 * 
 * @param bpage Pointer to the block in question.
 */
void buf_page_io_complete(buf_page_t *bpage);

/*** Gets the current length of the free list of buffer blocks.
@return	length of the free list */
ulint buf_get_free_list_len();

/**
 * @brief Frees a buffer block which does not contain a file page.
 *
 * @param block Pointer to the buffer block to be freed.
 */
void buf_block_free(buf_block_t *block);

/** The common buffer control block structure
for compressed and uncompressed frames */

struct buf_page_t {
  /** @name General fields
  None of these bit-fields must be modified without holding
  buf_page_get_mutex() buf_block_t::mutex since they can be
  stored in the same machine word.  Some of these fields are
  additionally protected by buf_pool_mutex. */
  /* @{ */

  /** tablespace id; also protected by buf_pool_mutex. */
  uint32_t space;

  /** page number; also protected by buf_pool_mutex. */
  uint32_t offset;

  /** state of the control block; also protected by buf_pool_mutex.
  State transitions from BUF_BLOCK_READY_FOR_USE to BUF_BLOCK_MEMORY
  need not be protected by buf_page_get_mutex().
  @see enum buf_page_state */
  unsigned state : 3;

  /** if this block is currently being flushed to disk, this tells the
   * flush_type.  @see enum buf_flush */
  unsigned flush_type : 2;

  /** type of pending I/O operation; also protected by buf_pool_mutex
  @see enum * buf_io_fix */
  unsigned io_fix : 2;

  /** count of how manyfold this block is currently bufferfixed */
  unsigned buf_fix_count : 25;
  /* @} */

  bool old;

  /** the value of buf_pool->freed_page_clock when this block was the last time
  put to the head of the LRU list; a thread is allowed to read this for
  heuristic purposes without holding any mutex or latch */
  unsigned freed_page_clock : 31;

  /** time of first access, or 0 if the block was never accessed in the
  buffer pool */
  uint32_t access_time{};

  /** node used in chaining to buf_pool->page_hash */
  buf_page_t *hash;

  /** @name Page flushing fields
  All these are protected by buf_pool_mutex. */
  /* @{ */
  UT_LIST_NODE_T(buf_page_t) list;

  /** based on state, this is a list node, protected only by buf_pool_mutex, in
  one of the following lists in buf_pool:

  - BUF_BLOCK_NOT_USED:	free

  The contents of the list node is undefined if !in_flush_list && state ==
  BUF_BLOCK_FILE_PAGE, or if state is one of BUF_BLOCK_MEMORY,
  BUF_BLOCK_REMOVE_HASH or
  BUF_BLOCK_READY_IN_USE. */

  /** log sequence number of the youngest modification to this block, zero if
  not modified */
  lsn_t newest_modification;

  /** log sequence number of the START of the log entry written of the oldest
  modification to this block which has not yet been flushed on disk; zero if
  all modifications are on disk */
  lsn_t oldest_modification;

  /* @} */

  /** @name LRU replacement algorithm fields
  These fields are protected by buf_pool_mutex only
  not the buf_block_t::mutex). */
  /* @{ */

  /** node of the LRU list */
  UT_LIST_NODE_T(buf_page_t) LRU;

  /* @} */

#ifdef UNIV_DEBUG
  /** true if in buf_pool->page_hash */
  bool in_page_hash;

  /** true if in buf_pool->flush_list; when buf_pool_mutex is free, the
  following should hold: in_flush_list == (state == BUF_BLOCK_FILE_PAGE) */
  bool in_flush_list;

  /** true if in buf_pool->free; when buf_pool_mutex is free, the following
  should hold: in_free_list == (state == BUF_BLOCK_NOT_USED) */
  bool in_free_list;

  /** true if the page is in the LRU list; used in debugging */
  bool in_LRU_list;

  /** this is set to true when fsp
  frees a page in buffer pool */
  bool file_page_was_freed;
#endif /* UNIV_DEBUG */
};

/** The buffer control block structure */
struct buf_block_t {

  /** @name General fields */
  /* @{ */

  /** page information; this must be the first field, so that
  buf_pool->page_hash can point to buf_page_t or buf_block_t */
  buf_page_t page;

  /** pointer to buffer frame which is of size UNIV_PAGE_SIZE, and
  aligned to an address divisible by UNIV_PAGE_SIZE */
  byte *frame;

  /** mutex protecting this block: state (also protected by the buffer
  pool mutex), io_fix, buf_fix_count, and accessed; we introduce this new
  mutex in InnoDB-5.1 to relieve contention on the buffer pool mutex */
  mutex_t mutex;

  rw_lock_t lock;

  /** hashed value of the page address in the record lock hash table;
  protected by buf_block_t::lock (or buf_block_t::mutex, buf_pool_mutex
  in buf_page_get_gen(), buf_page_init_for_read() and buf_page_create()) */
  uint32_t lock_hash_val;

  /** true if we know that this is an index page, and want the database
  to check its consistency before flush; note that there may be pages in the
  buffer pool which are index pages, but this flag is not set because
  we do not keep track of all pages; NOT protected by any mutex */
  bool check_index_page_at_flush;
  /* @} */

  /** @name Optimistic search field */
  /* @{ */

  /** this clock is incremented every time a pointer to a record on the
  page may become obsolete; this is used in the optimistic cursor
  positioning: if the modify clock has not changed, we know that the pointer
  is still valid; this field may be changed if the thread (1) owns the
  pool mutex and the page is not bufferfixed, or (2) the thread has an
  x-latch on the block */
  uint64_t modify_clock;

  /* @} */
  /** @name Hash search fields (unprotected)
  NOTE that these fields are NOT protected by any semaphore! */
  /* @{ */

  /** Counter which controls building of a new hash index for the page */
  uint32_t n_hash_helps;

  /** Recommended prefix length for hash search: number of full fields */
  uint16_t n_fields;

  /** Recommended prefix: number of bytes in an incomplete field */
  uint16_t n_bytes;

  /** true or false, depending on whether the leftmost
  record of several records with the same prefix should
  be indexed in the hash index */
  bool left_side;
  /* @} */

  /** @name Hash search fields
  These 6 fields may only be modified when we have
  an x-latch on btr_search_latch AND
  - we are holding an s-latch or x-latch on buf_block_t::lock or
  - we know that buf_block_t::buf_fix_count == 0.

  An exception to this is when we init or create a page
  in the buffer pool in buf0buf.c. */

  /* @{ */

  /** true if hash index has already been built on this page;
  note that it does not guarantee that the index is complete,
  though: there may have been hash collisions, record deletions, etc. */
  bool is_hashed;

  /** prefix length for hash indexing: number of full fields */
  uint16_t curr_n_fields;

  /** number of bytes in hash indexing */
  uint16_t curr_n_bytes;

  /** true or false in hash indexing */
  bool curr_left_side;

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
struct buf_pool_t {

  /** @name General fields */
  /* @{ */

  /** number of buffer pool chunks */
  ulint n_chunks;

  /** buffer pool chunks */
  buf_chunk_t *chunks;

  /** current pool size in pages */
  ulint curr_size;

  /** hash table of buf_page_t or buf_block_t file pages,
   buf_page_in_file() == true, indexed by (space_id, offset) */
  hash_table_t *page_hash;

  ulint n_pend_reads; /** number of pending read operations */

  /** when buf_print_io was last time called */
  time_t last_printout_time;

  /** current statistics */
  buf_pool_stat_t stat;

  /** old statistics */
  buf_pool_stat_t old_stat;

  /* @} */

  /** @name Page flushing algorithm fields */

  /* @{ */

  /** base node of the modified block list */
  UT_LIST_BASE_NODE_T(buf_page_t, list)
  flush_list;

  /** this is true when a flush of the given type is being initialized */
  bool init_flush[BUF_FLUSH_N_TYPES];

  /** This is the number of pending writes in the given flush type */
  ulint n_flush[BUF_FLUSH_N_TYPES];

  /** this is in the set state when there is no flush batch of the given type
   * running */
  os_event_t no_flush[BUF_FLUSH_N_TYPES];

  /** a red-black tree is used exclusively during recovery to speed up
  insertions in the flush_list. This tree contains blocks in order of
  oldest_modification LSN and is kept in sync with the flush_list.
  Each member of the tree MUST also be on the flush_list. This tree
  is relevant only in recovery and is set to nullptr once the recovery is over. */
  ib_rbt_t *flush_rbt;

  /** a sequence number used to count the number of buffer blocks removed
  from the end of the LRU list; NOTE that this counter may wrap around
  at 4 billion! A thread is allowed to read this for heuristic purposes
  without holding any mutex or latch */
  ulint freed_page_clock;

  /** when an LRU flush ends for a page, this is incremented by one;
  this is set to zero when a buffer block is allocated */
  ulint LRU_flush_ended;

  /* @} */
  /** @name LRU replacement algorithm fields */
  /* @{ */

  /** base node of the free block list */
  UT_LIST_BASE_NODE_T(buf_page_t, list) free;

  /** base node of the LRU list */
  UT_LIST_BASE_NODE_T(buf_page_t, LRU) LRU;

  /** pointer to the about buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV oldest
  blocks in the LRU list; nullptr if LRU length less than BUF_LRU_OLD_MIN_LEN;
  NOTE: when LRU_old != nullptr, its length should always equal LRU_old_len */
  buf_page_t *LRU_old;

  /** length of the LRU list from the block to which LRU_old points onward,
  including that block; see buf0lru.c for the restrictions on this value;
  0 if LRU_old == nullptr; NOTE: LRU_old_len must be adjusted whenever LRU_old
  shrinks or grows! */
  ulint LRU_old_len;

  /* @} */
};

/** mutex protecting the buffer pool struct and control blocks, except the
read-write lock in them */
extern mutex_t buf_pool_mutex;

/** @name Accessors for buf_pool_mutex.
Use these instead of accessing buf_pool_mutex directly. */
/* @{ */

/** Test if buf_pool_mutex is owned. */
#define buf_pool_mutex_own() mutex_own(&buf_pool_mutex)
/** Acquire the buffer pool mutex. */
#define buf_pool_mutex_enter()    \
  do {                            \
    mutex_enter(&buf_pool_mutex); \
  } while (0)

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** Flag to forbid the release of the buffer pool mutex.
Protected by buf_pool_mutex. */
extern ulint buf_pool_mutex_exit_forbidden;

/** Forbid the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_forbid() \
  do {                               \
    ut_ad(buf_pool_mutex_own());     \
    buf_pool_mutex_exit_forbidden++; \
  } while (0)

/** Allow the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_allow()      \
  do {                                   \
    ut_ad(buf_pool_mutex_own());         \
    ut_a(buf_pool_mutex_exit_forbidden); \
    buf_pool_mutex_exit_forbidden--;     \
  } while (0)

/** Release the buffer pool mutex. */
#define buf_pool_mutex_exit()             \
  do {                                    \
    ut_a(!buf_pool_mutex_exit_forbidden); \
    mutex_exit(&buf_pool_mutex);          \
  } while (0)
#else
/** Forbid the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_forbid() ((void)0)

/** Allow the release of the buffer pool mutex. */

#define buf_pool_mutex_exit_allow() ((void)0)
/** Release the buffer pool mutex. */
#define buf_pool_mutex_exit() mutex_exit(&buf_pool_mutex)

#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
/* @} */

/*** Let us list the consistency conditions for different control block states.

NOT_USED:
  is in free list,
  not in LRU list,
  not in flush list,
  nor page hash table

READY_FOR_USE:
  is not in free list
  is not in LRU list
  is not in flush list
  is not in the page hash table

MEMORY:
	is not in free list
  is not in LRU list
  is not in flush list
  is not in the page hash table

FILE_PAGE:	space and offset are defined, is in page hash table
  if io_fix == BUF_IO_WRITE,
    pool: no_flush[flush_type] is in reset state,
    pool: n_flush[flush_type] > 0

  (1) if buf_fix_count == 0, then
    is in LRU list, not in free list
    is in flush list,
      if and only if oldest_modification > 0
    is x-locked,
      if and only if io_fix == BUF_IO_READ
    is s-locked,
      if and only if io_fix == BUF_IO_WRITE

  (2) if buf_fix_count > 0, then
    is not in LRU list, not in free list
    is in flush list,
      if and only if oldest_modification > 0
    if io_fix == BUF_IO_READ,
      is x-locked
    if io_fix == BUF_IO_WRITE,
      is s-locked

State transitions:

NOT_USED => READY_FOR_USE
READY_FOR_USE => MEMORY
READY_FOR_USE => FILE_PAGE
MEMORY => NOT_USED

FILE_PAGE => NOT_USED	NOTE: This transition is allowed if and only if
  (1) buf_fix_count == 0,
  (2) oldest_modification == 0, and
  (3) io_fix == 0.
*/

/**
 * Reads the freed_page_clock of a buffer block.
 *
 * @param bpage The buffer block.
 * @return The freed_page_clock.
 */
inline ulint buf_page_get_freed_page_clock(const buf_page_t *bpage) {
  /* This is sometimes read without holding buf_pool_mutex. */
  return bpage->freed_page_clock;
}

/**
 * Reads the freed_page_clock of a buffer block.
 *
 * @param block The buffer block.
 * @return The freed_page_clock.
 */
inline ulint buf_block_get_freed_page_clock(const buf_block_t *block) {
  return buf_page_get_freed_page_clock(&block->page);
}

/**
 * Gets the current size of the buffer pool in bytes.
 *
 * @return The size of the buffer pool in bytes.
 */
inline ulint buf_pool_get_curr_size() {
  return buf_pool->curr_size * UNIV_PAGE_SIZE;
}

/**
 * Gets the smallest oldest_modification lsn for any page in the pool.
 * Returns zero if all modified pages have been flushed to disk.
 *
 * @return The oldest modification in the pool, zero if none.
 */
inline uint64_t buf_pool_get_oldest_modification() {
  buf_pool_mutex_enter();

  uint64_t lsn;
  auto bpage = UT_LIST_GET_LAST(buf_pool->flush_list);

  if (bpage == nullptr) {
    lsn = 0;
  } else {
    ut_ad(bpage->in_flush_list);
    lsn = bpage->oldest_modification;
  }

  buf_pool_mutex_exit();

  /* The returned answer may be out of date: the flush_list can
  change after the mutex has been released. */

  return lsn;
}

/**
 * Gets the state of a block.
 *
 * @param bpage Pointer to the control block.
 * @return The state of the block.
 */
inline auto buf_page_get_state(const buf_page_t *bpage) {
  auto state = static_cast<buf_page_state>(bpage->state);

#ifdef UNIV_DEBUG
  switch (state) {
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_FILE_PAGE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      break;
    default:
      ut_error;
  }
#endif /* UNIV_DEBUG */

  return state;
}

/** Gets the state of a block.
@param[in] block     Pointer to control block.
@return	state */
inline buf_page_state buf_block_get_state(const buf_block_t *block) {
  return buf_page_get_state(&block->page);
}

/**
 * Sets the state of a block.
 *
 * @param bpage Pointer to the control block.
 * @param state The state to set.
 */
inline void buf_page_set_state(buf_page_t *bpage, buf_page_state state) {
#ifdef UNIV_DEBUG
  buf_page_state old_state = buf_page_get_state(bpage);

  switch (old_state) {
    case BUF_BLOCK_NOT_USED:
      ut_a(state == BUF_BLOCK_READY_FOR_USE);
      break;
    case BUF_BLOCK_READY_FOR_USE:
      ut_a(state == BUF_BLOCK_MEMORY || state == BUF_BLOCK_FILE_PAGE || state == BUF_BLOCK_NOT_USED);
      break;
    case BUF_BLOCK_MEMORY:
      ut_a(state == BUF_BLOCK_NOT_USED);
      break;
    case BUF_BLOCK_FILE_PAGE:
      ut_a(state == BUF_BLOCK_NOT_USED || state == BUF_BLOCK_REMOVE_HASH);
      break;
    case BUF_BLOCK_REMOVE_HASH:
      ut_a(state == BUF_BLOCK_MEMORY);
      break;
  }
#endif /* UNIV_DEBUG */

  bpage->state = state;
  ut_ad(buf_page_get_state(bpage) == state);
}

/**
 * Sets the state of a block.
 *
 * @param block Pointer to the control block.
 * @param state The state to set.
 */
inline void buf_block_set_state(buf_block_t *block, buf_page_state state) {
  buf_page_set_state(&block->page, state);
}

/**
 * Determines if a block is mapped to a tablespace.
 *
 * @param bpage Pointer to the control block.
 * @return True if the block is mapped to a tablespace, false otherwise.
 */
inline bool buf_page_in_file(const buf_page_t *bpage) {
  switch (buf_page_get_state(bpage)) {
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

/**
 * Gets the mutex of a block.
 *
 * @param bpage Pointer to the control block.
 * @return Pointer to the mutex protecting the block.
 */
inline mutex_t *buf_page_get_mutex(const buf_page_t *bpage) {
  return &((buf_block_t *)bpage)->mutex;
}

/**
 * Get the flush type of a page.
 *
 * @param bpage Pointer to the buffer page.
 * @return The flush type of the page.
 */
inline auto buf_page_get_flush_type(const buf_page_t *bpage) {
  auto flush_type = static_cast<buf_flush>(bpage->flush_type);

#ifdef UNIV_DEBUG
  switch (flush_type) {
    case BUF_FLUSH_LRU:
    case BUF_FLUSH_SINGLE_PAGE:
    case BUF_FLUSH_LIST:
      return flush_type;
    case BUF_FLUSH_N_TYPES:
      break;
  }
  ut_error;
#endif /* UNIV_DEBUG */

  return flush_type;
}

/**
 * Set the flush type of a page.
 *
 * @param bpage Pointer to the buffer page.
 * @param flush_type The flush type to set.
 */
inline void buf_page_set_flush_type(buf_page_t *bpage, buf_flush flush_type) {
  bpage->flush_type = flush_type;
  ut_ad(buf_page_get_flush_type(bpage) == flush_type);
}

/**
 * Map a block to a file page.
 *
 * @param block Pointer to the control block.
 * @param space Tablespace ID.
 * @param page_no Page number.
 */
inline void buf_block_set_file_page(buf_block_t *block, space_id_t space, page_no_t page_no) {
  buf_block_set_state(block, BUF_BLOCK_FILE_PAGE);
  block->page.space = space;
  block->page.offset = page_no;
}

/**
 * Gets the io_fix state of a block.
 *
 * @param bpage Pointer to the control block.
 * @return The io_fix state of the block.
 */
inline auto buf_page_get_io_fix(const buf_page_t *bpage) {
  auto io_fix = static_cast<buf_io_fix>(bpage->io_fix);

#ifdef UNIV_DEBUG
  switch (io_fix) {
    case BUF_IO_NONE:
    case BUF_IO_READ:
    case BUF_IO_WRITE:
      return io_fix;
  }
  ut_error;
#endif /* UNIV_DEBUG */

  return io_fix;
}

/**
 * Gets the io_fix state of a block.
 *
 * @param block Pointer to the control block.
 * @return The io_fix state of the block.
 */
inline buf_io_fix buf_block_get_io_fix(const buf_block_t *block) {
  return buf_page_get_io_fix(&block->page);
}

/**
 * Sets the io_fix state of a block.
 *
 * @param bpage Pointer to the control block.
 * @param io_fix The io_fix state to set.
 */
inline void buf_page_set_io_fix(buf_page_t *bpage, buf_io_fix io_fix) {
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  bpage->io_fix = io_fix;
  ut_ad(buf_page_get_io_fix(bpage) == io_fix);
}

/**
 * Sets the io_fix state of a block.
 *
 * @param block Pointer to the control block.
 * @param io_fix The io_fix state to set.
 */
inline void buf_block_set_io_fix(buf_block_t *block, buf_io_fix io_fix) {
  buf_page_set_io_fix(&block->page, io_fix);
}

/**
 * Determine if a buffer block can be relocated in memory.
 * The block can be dirty, but it must not be I/O-fixed or bufferfixed.
 *
 * @param bpage Pointer to the control block being relocated.
 * @return True if the block can be relocated, false otherwise.
 */
inline bool buf_page_can_relocate(const buf_page_t *bpage) {
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));
  ut_ad(buf_page_in_file(bpage));
  ut_ad(bpage->in_LRU_list);

  return buf_page_get_io_fix(bpage) == BUF_IO_NONE && bpage->buf_fix_count == 0;
}

/**
 * Determine if a block has been flagged as old.
 *
 * @param bpage Pointer to the control block.
 * @return True if the block is flagged as old, false otherwise.
 */
inline bool buf_page_is_old(const buf_page_t *bpage) {
  ut_ad(buf_page_in_file(bpage));
  ut_ad(buf_pool_mutex_own());

  return bpage->old;
}

/**
 * Flag a block as old.
 *
 * @param bpage Pointer to the control block.
 * @param old Flag indicating if the block is old.
 */
inline void buf_page_set_old(buf_page_t *bpage, bool old) {
  ut_a(buf_page_in_file(bpage));
  ut_ad(buf_pool_mutex_own());
  ut_ad(bpage->in_LRU_list);

#ifdef UNIV_LRU_DEBUG
  ut_a((buf_pool->LRU_old_len == 0) == (buf_pool->LRU_old == nullptr));
  /* If a block is flagged "old", the LRU_old list must exist. */
  ut_a(!old || buf_pool->LRU_old);

  if (UT_LIST_GET_PREV(LRU, bpage) && UT_LIST_GET_NEXT(LRU, bpage)) {
    const buf_page_t *prev = UT_LIST_GET_PREV(LRU, bpage);
    const buf_page_t *next = UT_LIST_GET_NEXT(LRU, bpage);
    if (prev->old == next->old) {
      ut_a(prev->old == old);
    } else {
      ut_a(!prev->old);
      ut_a(buf_pool->LRU_old == (old ? bpage : next));
    }
  }
#endif /* UNIV_LRU_DEBUG */

  bpage->old = old;
}

/**
 * Determine the time of first access of a block in the buffer pool.
 *
 * @param bpage Pointer to the control block.
 * @return The time of first access (ut_time_ms()) if the block has been accessed, 0 otherwise.
 */
inline unsigned buf_page_is_accessed(const buf_page_t *bpage) {
  ut_ad(buf_page_in_file(bpage));

  return bpage->access_time;
}

/**
 * Flag a block as accessed.
 *
 * @param bpage Pointer to the control block.
 * @param time_ms The current time in milliseconds.
 */
inline void buf_page_set_accessed(buf_page_t *bpage, ulint time_ms) {
  ut_a(buf_page_in_file(bpage));
  ut_ad(buf_pool_mutex_own());

  if (!bpage->access_time) {
    /* Make this the time of the first access. */
    bpage->access_time = time_ms;
  }
}

/**
 * Get the buf_block_t handle of a buffered file block if an uncompressed page frame exists, or nullptr.
 *
 * @param bpage Pointer to the control block.
 * @return The control block if an uncompressed page frame exists, or nullptr.
 */
inline buf_block_t *buf_page_get_block(buf_page_t *bpage) {
  if (likely(bpage != nullptr)) {
    ut_ad(buf_page_in_file(bpage));

    if (buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE) {
      return reinterpret_cast<buf_block_t *>(bpage);
    }
  }

  return nullptr;
}

#ifdef UNIV_DEBUG
/**
 * Gets a pointer to the memory frame of a block.
 *
 * @param block Pointer to the control block.
 * @return Pointer to the frame.
 */
inline buf_frame_t *buf_block_get_frame(const buf_block_t *block) {
  switch (buf_block_get_state(block)) {
    case BUF_BLOCK_NOT_USED:
      ut_error;
      break;
    case BUF_BLOCK_FILE_PAGE:
      ut_a(block->page.buf_fix_count > 0);
      /* fall through */
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      goto ok;
  }
  ut_error;
ok:
  return reinterpret_cast<buf_frame_t *>(block->frame);
}
#endif /* UNIV_DEBUG */

/**
 * Gets the space id of a block.
 *
 * @param bpage Pointer to the control block.
 * @return Space id.
 */
inline ulint buf_page_get_space(const buf_page_t *bpage) {
  ut_ad(bpage);
  ut_a(buf_page_in_file(bpage));

  return bpage->space;
}

/**
 * Gets the space id of a block.
 *
 * @param block Pointer to the control block.
 * @return Space id.
 */
inline ulint buf_block_get_space(const buf_block_t *block) {
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);

  return block->page.space;
}

/**
 * Gets the page number of a block.
 *
 * @param bpage Pointer to the control block.
 * @return Page number.
 */
inline ulint buf_page_get_page_no(const buf_page_t *bpage) {
  ut_ad(bpage);
  ut_a(buf_page_in_file(bpage));

  return bpage->offset;
}

/**
 * Gets the page number of a block.
 *
 * @param block Pointer to the control block.
 * @return Page number.
 */
inline ulint buf_block_get_page_no(const buf_block_t *block) {
  ut_ad(block != nullptr);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);

  return block->page.offset;
}

/**
 * @brief Gets the space id, page offset, and byte offset within page of a pointer pointing to a buffer frame containing a file page.
 *
 * @param ptr Pointer to a buffer frame.
 * @param space Pointer to store the space id.
 * @param addr Pointer to store the page offset and byte offset.
 */
inline void buf_ptr_get_fsp_addr(const void *ptr, ulint *space, fil_addr_t *addr) {
  const page_t *page = (const page_t *)ut_align_down(ptr, UNIV_PAGE_SIZE);

  *space = mach_read_from_4(page + FIL_PAGE_SPACE_ID);
  addr->page = mach_read_from_4(page + FIL_PAGE_OFFSET);
  addr->boffset = ut_align_offset(ptr, UNIV_PAGE_SIZE);
}

/**
 * Gets the hash value of the page the pointer is pointing to. This can be used in searches in the lock hash table.
 *
 * @param block Pointer to the buffer block.
 * @return Lock hash value.
 */
inline ulint buf_block_get_lock_hash_val(const buf_block_t *block) {
  ut_ad(buf_page_in_file(&block->page));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(
    rw_lock_own(&(((buf_block_t *)block)->lock), RW_LOCK_EXCLUSIVE) || rw_lock_own(&(((buf_block_t *)block)->lock), RW_LOCK_SHARED)
  );
#endif /* UNIV_SYNC_DEBUG */

  return block->lock_hash_val;
}

/** Allocates a buffer block.
@return	own: the allocated block, in state BUF_BLOCK_MEMORY */
inline buf_block_t *buf_block_alloc() {
  auto block = buf_LRU_get_free_block();

  buf_block_set_state(block, BUF_BLOCK_MEMORY);

  return block;
}

/**
 * @brief Copies contents of a buffer frame to a given buffer.
 *
 * @param buf Pointer to the buffer to copy to.
 * @param frame Pointer to the buffer frame.
 * @return Pointer to the copied buffer.
 */
inline byte *buf_frame_copy(byte *buf, const buf_frame_t *frame) {
  memcpy(buf, frame, UNIV_PAGE_SIZE);

  return buf;
}

/**
 * @brief Calculates a folded value of a file page address to use in the page hash table.
 *
 * @param space Space id.
 * @param offset Offset of the page within space.
 * @return The folded value.
 */
inline ulint buf_page_address_fold(space_id_t space, ulint offset) {
  return (space << 20) + space + offset;
}

/**
 * @brief Gets the youngest modification log sequence number for a frame.
 * Returns zero if not a file page or no modification occurred yet.
 *
 * @param bpage Pointer to the block containing the page frame.
 * @return The newest modification to the page.
 */
inline lsn_t buf_page_get_newest_modification(const buf_page_t *bpage) {
  auto block_mutex = buf_page_get_mutex(bpage);

  mutex_enter(block_mutex);

  lsn_t lsn;

  if (buf_page_in_file(bpage)) {
    lsn = bpage->newest_modification;
  } else {
    lsn = 0;
  }

  mutex_exit(block_mutex);

  return lsn;
}

/**
 * @brief Increments the modify clock of a frame by 1.
 * The caller must (1) own the buf_pool mutex and block bufferfix count has to be zero,
 * (2) or own an x-lock on the block.
 *
 * @param block Pointer to the buffer block.
 */
inline void buf_block_modify_clock_inc(buf_block_t *block) {
#ifdef UNIV_SYNC_DEBUG
  ut_ad((buf_pool_mutex_own() && (block->page.buf_fix_count == 0)) || rw_lock_own(&(block->lock), RW_LOCK_EXCLUSIVE));
#endif /* UNIV_SYNC_DEBUG */

  block->modify_clock++;
}

/**
 * @brief Returns the value of the modify clock.
 * The caller must have an s-lock or x-lock on the block.
 *
 * @param block Pointer to the buffer block.
 * @return The value of the modify clock.
 */
inline uint64_t buf_block_get_modify_clock(buf_block_t *block) {
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&(block->lock), RW_LOCK_SHARED) || rw_lock_own(&(block->lock), RW_LOCK_EXCLUSIVE));
#endif /* UNIV_SYNC_DEBUG */

  return block->modify_clock;
}

#ifdef UNIV_SYNC_DEBUG
/**
 * @brief Increments the bufferfix count.
 *
 * @param file File name.
 * @param line Line number.
 * @param block Pointer to the buffer block.
 */
inline void buf_block_buf_fix_inc_func(const char *file, ulint line, buf_block_t *block)
#else  /* UNIV_SYNC_DEBUG */
/**
 * @brief Increments the bufferfix count.
 *
 * @param block Pointer to the buffer block.
 */
inline void buf_block_buf_fix_inc_func(buf_block_t *block)
#endif /* UNIV_SYNC_DEBUG */
{
#ifdef UNIV_SYNC_DEBUG
  bool ret;

  ret = rw_lock_s_lock_nowait(&(block->debug_latch), file, line);
  ut_a(ret);
#endif /* UNIV_SYNC_DEBUG */
  ut_ad(mutex_own(&block->mutex));

  block->page.buf_fix_count++;
}

#ifdef UNIV_SYNC_DEBUG
/** Increments the bufferfix count.
@param b	in/out: block to bufferfix
@param f	in: file name where requested
@param l	in: line number where requested */
#define buf_block_buf_fix_inc(b, f, l) buf_block_buf_fix_inc_func(f, l, b)
#else /* UNIV_SYNC_DEBUG */
/** Increments the bufferfix count.
@param b	in/out: block to bufferfix
@param f	in: file name where requested
@param l	in: line number where requested */
#define buf_block_buf_fix_inc(b, f, l) buf_block_buf_fix_inc_func(b)
#endif /* UNIV_SYNC_DEBUG */

/**
 * @brief Decrements the bufferfix count.
 *
 * This function decrements the bufferfix count of a block, indicating that the block is no longer fixed in the buffer pool.
 *
 * @param block Pointer to the buffer block.
 */
inline void buf_block_buf_fix_dec(buf_block_t *block) {
  ut_ad(mutex_own(&block->mutex));

  block->page.buf_fix_count--;

#ifdef UNIV_SYNC_DEBUG
  rw_lock_s_unlock(&block->debug_latch);
#endif /* UNIV_SYNC_DEBUG */
}

/**
 * @brief Returns the control block of a file page, nullptr if not found.
 *
 * This function retrieves the control block of a file page based on the space id and offset.
 *
 * @param space The space id of the page.
 * @param offset The offset of the page within the space.
 * @return The control block of the file page, nullptr if not found.
 */
inline buf_page_t *buf_page_hash_get(space_id_t space, ulint offset) {
  ut_ad(buf_pool_mutex_own());

  // Look for the page in the hash table
  auto fold = buf_page_address_fold(space, offset);

  buf_page_t *bpage;

  HASH_SEARCH(
    hash,
    buf_pool->page_hash,
    fold,
    buf_page_t *,
    bpage,
    ut_ad(bpage->in_page_hash && buf_page_in_file(bpage)),
    bpage->space == space && bpage->offset == offset
  );

  if (bpage != nullptr) {
    ut_a(buf_page_in_file(bpage));
    ut_ad(bpage->in_page_hash);
    UNIV_MEM_ASSERT_RW(bpage, sizeof *bpage);
  }

  return bpage;
}

/**
 * @brief Returns the control block of a file page.
 *
 * This function retrieves the control block of a file page based on the space id and offset.
 *
 * @param space The space id of the page.
 * @param offset The offset of the page within the space.
 * @return The control block of the file page, nullptr if not found.
 */
inline buf_block_t *buf_block_hash_get(space_id_t space, ulint offset) {
  return buf_page_get_block(buf_page_hash_get(space, offset));
}

/**
 * @brief Checks if the page can be found in the buffer pool hash table.
 *
 * Note that it is possible that the page is not yet read from disk.
 *
 * @param space The space id of the page.
 * @param offset The offset of the page within the space.
 * @return True if found in the page hash table, false otherwise.
 */
inline bool buf_page_peek(space_id_t space, ulint offset) {
  buf_pool_mutex_enter();

  auto bpage = buf_page_hash_get(space, offset);

  buf_pool_mutex_exit();

  return bpage != nullptr;
}

#ifdef UNIV_SYNC_DEBUG
/**
 * @brief Adds latch level info for the rw-lock protecting the buffer frame.
 *
 * This should be called in the debug version after a successful latching of a page
 * if we know the latching order level of the acquired latch.
 *
 * @param block The buffer page where we have acquired latch.
 * @param level The latching order level.
 */
inline void buf_block_dbg_add_level(buf_block_t *block, ulint level) {
  sync_thread_add_level(&block->lock, level);
}
#endif /* UNIV_SYNC_DEBUG */
