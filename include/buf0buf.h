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


/** mutex protecting the buffer pool struct and control blocks, except the
read-write lock in them */
extern mutex_t buf_pool_mutex;

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

#ifdef UNIV_SYNC_DEBUG
/**
 * @brief Adds latch level info for the rw-lock protecting the buffer frame.
 *        This should be called in the debug version after a successful latching
 *        of a page if we know the latching order level of the acquired latch.
 *
 * @param block Pointer to the buffer page where we have acquired latch.
 * @param level Latching order level.
 */
inline void buf_block_dbg_add_level(buf_block_t *block, ulint level);
#else /* UNIV_SYNC_DEBUG */
#define buf_block_dbg_add_level(block, level) /* nothing */
#endif /* UNIV_SYNC_DEBUG */

#ifdef UNIV_DEBUG
/**
 * @brief Gets a pointer to the memory frame of a block.
 *
 * @param block Pointer to the control block.
 * @return Pointer to the frame.
 */
inline buf_frame_t *buf_block_get_frame(const buf_block_t *block) __attribute__((pure));
#else /* UNIV_DEBUG */
#define buf_block_get_frame(block) (block)->frame
#endif /* UNIV_DEBUG */

/*** Gets the block to whose frame the pointer is pointing to.
 @param[in] ptr                 Pointer to a frame.
@return	pointer to block, never nullptr */
buf_block_t *buf_block_align(const byte *ptr);

/*** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
the buf_block_t itself or a member of it
@param[in] ptr                  Pointer not dereferenced
@return	true if ptr belongs to a buf_block_t struct */
bool buf_pointer_is_block_field(const void *ptr);

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
void buf_print();
#endif /* UNIV_DEBUG_PRINT || UNIV_DEBUG || UNIV_BUF_DEBUG */

/**
 * @brief Prints a page to stderr.
 *
 * @param read_buf  in: a database page
 * @param ulint
 */
void buf_page_print(const byte *read_buf, ulint);

#ifdef UNIV_DEBUG
/** Returns the number of latched pages in the buffer pool.
@return	number of latched pages */
ulint buf_get_latched_pages_number();
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
/**
 * Adds latch level info for the rw-lock protecting the buffer frame. This
 * should be called in the debug version after a successful latching of a
 * page if we know the latching order level of the acquired latch.
 *
 * @param block The buffer page where we have acquired latch.
 * @param level The latching order level.
 */
inline void buf_block_dbg_add_level(buf_block_t *block, ulint level);
#else /* UNIV_SYNC_DEBUG */
#define buf_block_dbg_add_level(block, level) /* nothing */
#endif /* UNIV_SYNC_DEBUG */

#ifdef UNIV_DEBUG
/**
 * Gets a pointer to the memory frame of a block.
 *
 * @param block Pointer to the control block.
 * @return Pointer to the frame.
 */
inline buf_frame_t *buf_block_get_frame(const buf_block_t *block) __attribute__((pure));
#else /* UNIV_DEBUG */
#define buf_block_get_frame(block) (block)->frame
#endif /* UNIV_DEBUG */

/** Gets the block to whose frame the pointer is pointing to.
@return	pointer to block, never nullptr */
buf_block_t *buf_block_align(const byte *ptr); /** in: pointer to a frame */

/** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
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
  ut_ad(bpage != nullptr);
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

  return buf_page_get_space(&block->page);
}

/**
 * Gets the page number of a block.
 *
 * @param bpage Pointer to the control block.
 * @return Page number.
 */
inline ulint buf_page_get_page_no(const buf_page_t *bpage) {
  ut_ad(bpage != nullptr);
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

  return buf_page_get_page_no(&block->page);
}

/**
 * @brief Gets the space id, page offset, and byte offset within page of a pointer pointing to a buffer frame containing a file page.
 *
 * @param ptr Pointer to a buffer frame.
 * @param space Pointer to store the space id.
 * @param addr Pointer to store the page offset and byte offset.
 */
inline void buf_ptr_get_fsp_addr(const void *ptr, ulint *space, fil_addr_t *addr) {
  auto page = reinterpret_cast<const page_t *>(ut_align_down(ptr, UNIV_PAGE_SIZE));

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
  ut_ad((buf_pool_mutex_own() && block->page.buf_fix_count == 0) || rw_lock_own(&block->lock, RW_LOCK_EXCLUSIVE));
#endif /* UNIV_SYNC_DEBUG */

  ++block->modify_clock;
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
  auto ret = rw_lock_s_lock_nowait(&(block->debug_latch), file, line);
  ut_a(ret);
#endif /* UNIV_SYNC_DEBUG */
  ut_ad(mutex_own(&block->mutex));

  ++block->page.buf_fix_count;
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

  --block->page.buf_fix_count;

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
