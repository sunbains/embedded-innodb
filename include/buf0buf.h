/***
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

#ifndef buf0buf_h
#define buf0buf_h

#include "innodb0types.h"

#include "buf0types.h"
#include "fil0fil.h"
#include "hash0hash.h"
#include "mtr0types.h"
#include "os0proc.h"
#include "page0types.h"
#include "ut0byte.h"
#include "ut0rbt.h"

/** @name Modes for buf_page_get_gen */
/* @{ */
#define BUF_GET 10            /*!< get always */
#define BUF_GET_IF_IN_POOL 11 /*!< get if in pool */
#define BUF_GET_NO_LATCH                                                       \
  14 /*!< get and bufferfix, but                                               \
     set no latch; we have                                                     \
     separated this case, because                                              \
     it is error-prone programming                                             \
     not to set a latch, and it                                                \
     should be used with care */
/* @} */
/** @name Modes for buf_page_get_known_nowait */
/* @{ */
#define BUF_MAKE_YOUNG                                                         \
  51 /*!< Move the block to the                                                \
     start of the LRU list if there                                            \
     is a danger that the block                                                \
     would drift out of the buffer                                             \
     pool*/
#define BUF_KEEP_OLD                                                           \
  52 /*!< Preserve the current LRU                                             \
     position of the block. */
/* @} */

extern buf_pool_t *buf_pool; /*!< The buffer pool of the database */
#ifdef UNIV_DEBUG
extern bool buf_debug_prints;             /*!< If this is set true, the program
                                          prints info whenever read or flush
                                          occurs */
#endif                                    /* UNIV_DEBUG */
extern ulint srv_buf_pool_write_requests; /*!< variable to count write request
                                          issued */

/** Magic value to use instead of checksums when they are disabled */
#define BUF_NO_CHECKSUM_MAGIC 0xDEADBEEFUL

/** @brief States of a control block
@see buf_page_struct

The enumeration values must be 0..7. */
enum buf_page_state {
  BUF_BLOCK_NOT_USED,      /*!< is in the free list;
                           must be after the BUF_BLOCK_ZIP_
                           constants for compressed-only pages
                           @see buf_block_state_valid() */
  BUF_BLOCK_READY_FOR_USE, /*!< when buf_LRU_get_free_block
                           returns a block, it is in this state */
  BUF_BLOCK_FILE_PAGE,     /*!< contains a buffered file page */
  BUF_BLOCK_MEMORY,        /*!< contains some main memory
                           object */
  BUF_BLOCK_REMOVE_HASH    /*!< hash index should be removed
                           before putting to the free list */
};

/*** Creates the buffer pool.
@return	own: buf_pool object, NULL if not enough memory or error */

buf_pool_t *buf_pool_init(void);
/*** Prepares the buffer pool for shutdown. */

void buf_close(void);
/*** Frees the buffer pool at shutdown.  This must not be invoked after
freeing all mutexes. */

void buf_mem_free(void);

/*** Drops the adaptive hash index.  To prevent a livelock, this function
is only to be called while holding btr_search_latch and while
btr_search_enabled == false. */

void buf_pool_drop_hash_index(void);

/*** Relocate a buffer control block.  Relocates the block on the LRU list
and in buf_pool->page_hash.  Does not relocate bpage->list.
The caller must take care of relocating bpage->list. */

void buf_relocate(
    buf_page_t *bpage, /*!< in/out: control block being relocated;
                       buf_page_get_state(bpage) must be
                       BUF_BLOCK_ZIP_DIRTY or BUF_BLOCK_ZIP_PAGE */
    buf_page_t *dpage) /*!< in/out: destination control block */
    __attribute__((nonnull));
/*** Resizes the buffer pool. */

void buf_pool_resize(void);
/*** Gets the current size of buffer buf_pool in bytes.
@return	size in bytes */
inline ulint buf_pool_get_curr_size(void);
/*** Gets the smallest oldest_modification lsn for any page in the pool. Returns
zero if all modified pages have been flushed to disk.
@return	oldest modification in pool, zero if none */
inline uint64_t buf_pool_get_oldest_modification(void);

/*** Allocates a buffer block.
@return	own: the allocated block, in state BUF_BLOCK_MEMORY */
inline buf_block_t *buf_block_alloc();

/*** Frees a buffer block which does not contain a file page. */
inline void
buf_block_free(buf_block_t *block); /*!< in, own: block to be freed */
/*** Copies contents of a buffer frame to a given buffer.
@return	buf */
inline byte *buf_frame_copy(byte *buf, /*!< in: buffer to copy to */
                            const buf_frame_t *frame); /*!< in: buffer frame */
/*** NOTE! The following macros should be used instead of buf_page_get_gen,
to improve debugging. Only values RW_S_LATCH and RW_X_LATCH are allowed
in LA! */
#define buf_page_get(SP, ZS, OF, LA, MTR)                                      \
  buf_page_get_gen(SP, ZS, OF, LA, NULL, BUF_GET, __FILE__, __LINE__, MTR)

/*** Use these macros to bufferfix a page with no latching. Remember not to
read the contents of the page unless you know it is safe. Do not modify
the contents of the page! We have separated this case, because it is
error-prone programming not to set a latch, and it should be used
with care. */
#define buf_page_get_with_no_latch(SP, ZS, OF, MTR)                            \
  buf_page_get_gen(SP, ZS, OF, RW_NO_LATCH, NULL, BUF_GET_NO_LATCH, __FILE__,  \
                   __LINE__, MTR)

/*** This is the general function used to get optimistic access to a database
page.
@return	true if success */

bool buf_page_optimistic_get(
    ulint rw_latch,        /*!< in: RW_S_LATCH, RW_X_LATCH */
    buf_block_t *block,    /*!< in: guessed block */
    uint64_t modify_clock, /*!< in: modify clock value if mode is
                         ..._GUESS_ON_CLOCK */
    const char *file,      /*!< in: file name */
    ulint line,            /*!< in: line where called */
    mtr_t *mtr);           /*!< in: mini-transaction */
/*** This is used to get access to a known database page, when no waiting can be
done.
@return	true if success */

bool buf_page_get_known_nowait(
    ulint rw_latch,     /*!< in: RW_S_LATCH, RW_X_LATCH */
    buf_block_t *block, /*!< in: the known page */
    ulint mode,         /*!< in: BUF_MAKE_YOUNG or BUF_KEEP_OLD */
    const char *file,   /*!< in: file name */
    ulint line,         /*!< in: line where called */
    mtr_t *mtr);        /*!< in: mini-transaction */

/*** Given a tablespace id and page number tries to get that page. If the
page is not in the buffer pool it is not loaded and NULL is returned.
Suitable for using when holding the kernel mutex. */

const buf_block_t *
buf_page_try_get_func(ulint space_id,   /*!< in: tablespace id */
                      ulint page_no,    /*!< in: page number */
                      const char *file, /*!< in: file name */
                      ulint line,       /*!< in: line where called */
                      mtr_t *mtr);      /*!< in: mini-transaction */

/** Tries to get a page. If the page is not in the buffer pool it is
not loaded.  Suitable for using when holding the kernel mutex.
@param space_id	in: tablespace id
@param page_no	in: page number
@param mtr	in: mini-transaction
@return		the page if in buffer pool, NULL if not */
#define buf_page_try_get(space_id, page_no, mtr)                               \
  buf_page_try_get_func(space_id, page_no, __FILE__, __LINE__, mtr);

/*** This is the general function used to get access to a database page.
@return	pointer to the block or NULL */
buf_block_t *
buf_page_get_gen(ulint space,         /*!< in: space id */
                 ulint, ulint offset, /*!< in: page number */
                 ulint rw_latch, /*!< in: RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH */
                 buf_block_t *guess, /*!< in: guessed block or NULL */
                 ulint mode,         /*!< in: BUF_GET, BUF_GET_IF_IN_POOL,
                                     BUF_GET_NO_LATCH */
                 const char *file,   /*!< in: file name */
                 ulint line,         /*!< in: line where called */
                 mtr_t *mtr);        /*!< in: mini-transaction */

/*** Initializes a page to the buffer buf_pool. The page is usually not read
from a file even if it cannot be found in the buffer buf_pool. This is one
of the functions which perform to a block a state transition NOT_USED =>
FILE_PAGE (the other is buf_page_get_gen).
@return	pointer to the block, page bufferfixed */
buf_block_t *buf_page_create(ulint space,  /*!< in: space id */
                             ulint offset, /*!< in: offset of the page within
                                           space in units of a page */
                             ulint,
                             mtr_t *mtr); /*!< in: mini-transaction handle */

/*** Inits a page to the buffer buf_pool, for use in ibbackup --restore. */
void buf_page_init_for_backup_restore(
    ulint space,                /*!< in: space id */
    ulint offset,               /*!< in: offset of the page within space
                                in units of a page */
    ulint, buf_block_t *block); /*!< in: block to init */

/*** Decrements the bufferfix count of a buffer control block and releases
a latch, if specified. */
inline void buf_page_release(buf_block_t *block, /*!< in: buffer block */
                             ulint rw_latch, /*!< in: RW_S_LATCH, RW_X_LATCH,
                                             RW_NO_LATCH */
                             mtr_t *mtr);    /*!< in: mtr */
/*** Moves a page to the start of the buffer pool LRU list. This high-level
function can be used to prevent an important page from slipping out of
the buffer pool. */

void buf_page_make_young(
    buf_page_t *bpage); /*!< in: buffer block of a file page */
/*** Returns true if the page can be found in the buffer pool hash table.

NOTE that it is possible that the page is not yet read from disk,
though.

@return	true if found in the page hash table */
inline bool buf_page_peek(ulint space,   /*!< in: space id */
                          ulint offset); /*!< in: page number */
/*** Resets the check_index_page_at_flush field of a page if found in the buffer
pool. */

void buf_reset_check_index_page_at_flush(ulint space,   /*!< in: space id */
                                         ulint offset); /*!< in: page number */
#ifdef UNIV_DEBUG_FILE_ACCESSES
/*** Sets file_page_was_freed true if the page is found in the buffer pool.
This function should be called when we free a file page and want the
debug version to check that it is not accessed any more unless
reallocated.
@return	control block if found in page hash table, otherwise NULL */

buf_page_t *
buf_page_set_file_page_was_freed(ulint space,   /*!< in: space id */
                                 ulint offset); /*!< in: page number */
/*** Sets file_page_was_freed false if the page is found in the buffer pool.
This function should be called when we free a file page and want the
debug version to check that it is not accessed any more unless
reallocated.
@return	control block if found in page hash table, otherwise NULL */

buf_page_t *
buf_page_reset_file_page_was_freed(ulint space,   /*!< in: space id */
                                   ulint offset); /*!< in: page number */
#endif                                            /* UNIV_DEBUG_FILE_ACCESSES */
/*** Reads the freed_page_clock of a buffer block.
@return	freed_page_clock */
inline ulint
buf_page_get_freed_page_clock(const buf_page_t *bpage) /*!< in: block */
    __attribute__((pure));
/*** Reads the freed_page_clock of a buffer block.
@return	freed_page_clock */
inline ulint
buf_block_get_freed_page_clock(const buf_block_t *block) /*!< in: block */
    __attribute__((pure));

/*** Recommends a move of a block to the start of the LRU list if there is
danger of dropping from the buffer pool. NOTE: does not reserve the buffer pool
mutex.
@return	true if should be made younger */
inline bool buf_page_peek_if_too_old(
    const buf_page_t *bpage); /*!< in: block to make younger */
/*** Returns the current state of is_hashed of a page. false if the page is
not in the pool. NOTE that this operation does not fix the page in the
pool if it is found there.
@return	true if page hash index is built in search system */

bool buf_page_peek_if_search_hashed(ulint space,   /*!< in: space id */
                                    ulint offset); /*!< in: page number */
/*** Gets the youngest modification log sequence number for a frame.
Returns zero if not file page or no modification occurred yet.
@return	newest modification to page */
inline uint64_t buf_page_get_newest_modification(
    const buf_page_t *bpage); /*!< in: block containing the
                              page frame */
/*** Increments the modify clock of a frame by 1. The caller must (1) own the
buf_pool mutex and block bufferfix count has to be zero, (2) or own an x-lock
on the block. */
inline void buf_block_modify_clock_inc(buf_block_t *block); /*!< in: block */
/*** Returns the value of the modify clock. The caller must have an s-lock
or x-lock on the block.
@return	value */
inline uint64_t
buf_block_get_modify_clock(buf_block_t *block); /*!< in: block */
/*** Calculates a page checksum which is stored to the page when it is written
to a file. Note that we must be careful to calculate the same value
on 32-bit and 64-bit architectures.
@return	checksum */

ulint buf_calc_page_new_checksum(const byte *page); /*!< in: buffer page */
/*** In versions < 4.0.14 and < 4.1.1 there was a bug that the checksum only
looked at the first few bytes of the page. This calculates that old
checksum.
NOTE: we must first store the new formula checksum to
FIL_PAGE_SPACE_OR_CHKSUM before calculating and storing this old checksum
because this takes that field as an input!
@return	checksum */

ulint buf_calc_page_old_checksum(const byte *page); /*!< in: buffer page */

/*** Checks if a page is corrupt.
@return	true if corrupted */
bool buf_page_is_corrupted(const byte *read_buf, /*!< in: a database page */
                           ulint);

/*** Gets the space id, page offset, and byte offset within page of a
pointer pointing to a buffer frame containing a file page. */
inline void
buf_ptr_get_fsp_addr(const void *ptr,   /*!< in: pointer to a buffer frame */
                     ulint *space,      /*!< out: space id */
                     fil_addr_t *addr); /*!< out: page offset and byte offset */
/*** Gets the hash value of a block. This can be used in searches in the
lock hash table.
@return	lock hash value */
inline ulint
buf_block_get_lock_hash_val(const buf_block_t *block) /*!< in: block */
    __attribute__((pure));

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

void buf_page_print(const byte *read_buf, /*!< in: a database page */
                    ulint);

#ifdef UNIV_DEBUG
/*** Returns the number of latched pages in the buffer pool.
@return	number of latched pages */

ulint buf_get_latched_pages_number(void);
#endif /* UNIV_DEBUG */
/*** Returns the number of pending buf pool ios.
@return	number of pending I/O operations */

ulint buf_get_n_pending_ios(void);
/*** Prints info of the buffer i/o. */

void buf_print_io(ib_stream_t ib_stream); /*!< in: file where to print */
/*** Returns the ratio in percents of modified pages in the buffer pool /
database pages in the buffer pool.
@return	modified page percentage ratio */

ulint buf_get_modified_ratio_pct(void);
/*** Refreshes the statistics used to print per-second averages. */

void buf_refresh_io_stats(void);
/*** Asserts that all file pages in the buffer are in a replaceable state.
@return	true */

bool buf_all_freed(void);
/*** Checks that there currently are no pending i/o-operations for the buffer
pool.
@return	true if there is no pending i/o */

bool buf_pool_check_no_pending_io(void);
/*** Invalidates the file pages in the buffer pool when an archive recovery is
completed. All the file pages buffered must be in a replaceable state when
this function is called: not latched and not modified. */

void buf_pool_invalidate(void);
/*** Reset the buffer variables. */

void buf_var_init(void);

/* --------------------------- LOWER LEVEL ROUTINES ------------------------- */

#ifdef UNIV_SYNC_DEBUG
/*** Adds latch level info for the rw-lock protecting the buffer frame. This
should be called in the debug version after a successful latching of a
page if we know the latching order level of the acquired latch. */
inline void
buf_block_dbg_add_level(buf_block_t *block,   /*!< in: buffer page
                                              where we have acquired latch */
                        ulint level);         /*!< in: latching order level */
#else                                         /* UNIV_SYNC_DEBUG */
#define buf_block_dbg_add_level(block, level) /* nothing */
#endif                                        /* UNIV_SYNC_DEBUG */
/*** Gets the state of a block.
@return	state */
inline enum buf_page_state buf_page_get_state(
    const buf_page_t *bpage); /*!< in: pointer to the control block */
/*** Gets the state of a block.
@return	state */
inline enum buf_page_state buf_block_get_state(
    const buf_block_t *block) /*!< in: pointer to the control block */
    __attribute__((pure));
/*** Sets the state of a block. */
inline void
buf_page_set_state(buf_page_t *bpage, /*!< in/out: pointer to control block */
                   enum buf_page_state state); /*!< in: state */
/*** Sets the state of a block. */
inline void
buf_block_set_state(buf_block_t *block, /*!< in/out: pointer to control block */
                    enum buf_page_state state); /*!< in: state */
/*** Determines if a block is mapped to a tablespace.
@return	true if mapped */
inline bool
buf_page_in_file(const buf_page_t *bpage) /*!< in: pointer to control block */
    __attribute__((pure));

/*** Gets the mutex of a block.
@return	pointer to mutex protecting bpage */
inline mutex_t *
buf_page_get_mutex(const buf_page_t *bpage) /*!< in: pointer to control block */
    __attribute__((pure));

/*** Get the flush type of a page.
@return	flush type */
inline enum buf_flush
buf_page_get_flush_type(const buf_page_t *bpage) /*!< in: buffer page */
    __attribute__((pure));
/*** Set the flush type of a page. */
inline void
buf_page_set_flush_type(buf_page_t *bpage,          /*!< in: buffer page */
                        enum buf_flush flush_type); /*!< in: flush type */
/*** Map a block to a file page. */
inline void buf_block_set_file_page(
    buf_block_t *block, /*!< in/out: pointer to control block */
    ulint space,        /*!< in: tablespace id */
    ulint page_no);     /*!< in: page number */
/*** Gets the io_fix state of a block.
@return	io_fix state */
inline enum buf_io_fix buf_page_get_io_fix(
    const buf_page_t *bpage) /*!< in: pointer to the control block */
    __attribute__((pure));
/*** Gets the io_fix state of a block.
@return	io_fix state */
inline enum buf_io_fix buf_block_get_io_fix(
    const buf_block_t *block) /*!< in: pointer to the control block */
    __attribute__((pure));
/*** Sets the io_fix state of a block. */
inline void
buf_page_set_io_fix(buf_page_t *bpage,       /*!< in/out: control block */
                    enum buf_io_fix io_fix); /*!< in: io_fix state */
/*** Sets the io_fix state of a block. */
inline void
buf_block_set_io_fix(buf_block_t *block,      /*!< in/out: control block */
                     enum buf_io_fix io_fix); /*!< in: io_fix state */

/*** Determine if a buffer block can be relocated in memory.  The block
can be dirty, but it must not be I/O-fixed or bufferfixed. */
inline bool buf_page_can_relocate(
    const buf_page_t *bpage) /*!< control block being relocated */
    __attribute__((pure));

/*** Determine if a block has been flagged old.
@return	true if old */
inline bool buf_page_is_old(const buf_page_t *bpage) /*!< in: control block */
    __attribute__((pure));
/*** Flag a block old. */
inline void buf_page_set_old(buf_page_t *bpage, /*!< in/out: control block */
                             bool old);         /*!< in: old */
/*** Determine the time of first access of a block in the buffer pool.
@return	ut_time_ms() at the time of first access, 0 if not accessed */
inline unsigned
buf_page_is_accessed(const buf_page_t *bpage) /*!< in: control block */
    __attribute__((nonnull, pure));
/*** Flag a block accessed. */
inline void
buf_page_set_accessed(buf_page_t *bpage, /*!< in/out: control block */
                      ulint time_ms)     /*!< in: ut_time_ms() */
    __attribute__((nonnull));
/*** Gets the buf_block_t handle of a buffered file block if an uncompressed
page frame exists, or NULL.
@return	control block, or NULL */
inline buf_block_t *
buf_page_get_block(buf_page_t *bpage) /*!< in: control block, or NULL */
    __attribute__((pure));
#ifdef UNIV_DEBUG
/*** Gets a pointer to the memory frame of a block.
@return	pointer to the frame */
inline buf_frame_t *buf_block_get_frame(
    const buf_block_t *block) /*!< in: pointer to the control block */
    __attribute__((pure));
#else /* UNIV_DEBUG */
#define buf_block_get_frame(block) (block)->frame
#endif /* UNIV_DEBUG */
/*** Gets the space id of a block.
@return	space id */
inline ulint buf_page_get_space(
    const buf_page_t *bpage) /*!< in: pointer to the control block */
    __attribute__((pure));
/*** Gets the space id of a block.
@return	space id */
inline ulint buf_block_get_space(
    const buf_block_t *block) /*!< in: pointer to the control block */
    __attribute__((pure));
/*** Gets the page number of a block.
@return	page number */
inline ulint buf_page_get_page_no(
    const buf_page_t *bpage) /*!< in: pointer to the control block */
    __attribute__((pure));

/*** Gets the page number of a block.
@return	page number */
inline ulint buf_block_get_page_no(
    const buf_block_t *block) /*!< in: pointer to the control block */
    __attribute__((pure));

/*** Gets the block to whose frame the pointer is pointing to.
@return	pointer to block, never NULL */
buf_block_t *buf_block_align(const byte *ptr); /*!< in: pointer to a frame */

/*** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
the buf_block_t itself or a member of it
@return	true if ptr belongs to a buf_block_t struct */

bool buf_pointer_is_block_field(const void *ptr); /*!< in: pointer not
                                                   dereferenced */
/** Find out if a pointer corresponds to a buf_block_t::mutex.
@param m	in: mutex candidate
@return		true if m is a buf_block_t::mutex */
#define buf_pool_is_block_mutex(m) buf_pointer_is_block_field((const void *)(m))
/** Find out if a pointer corresponds to a buf_block_t::lock.
@param l	in: rw-lock candidate
@return		true if l is a buf_block_t::lock */
#define buf_pool_is_block_lock(l) buf_pointer_is_block_field((const void *)(l))

/*** Function which inits a page for read to the buffer buf_pool. If the page is
(1) already in buf_pool, or
(2) if we specify to read only ibuf pages and the page is not an ibuf page, or
(3) if the space is deleted or being deleted,
then this function does nothing.
Sets the io_fix flag to BUF_IO_READ and sets a non-recursive exclusive lock
on the buffer frame. The io-handler must take care that the flag is cleared
and the lock released later.
@return	pointer to the block or NULL */
buf_page_t *buf_page_init_for_read(
    db_err *err, /*!< out: DB_SUCCESS or DB_TABLESPACE_DELETED */
    ulint mode,  /*!< in: BUF_READ_IBUF_PAGES_ONLY, ... */
    ulint space, /*!< in: space id */
    ulint, bool, int64_t tablespace_version, /*!< in: prevents reading from a
                                     wrong version of the tablespace in case we
                                     have done DISCARD + IMPORT */
    ulint offset);                           /*!< in: page number */

/*** Completes an asynchronous read or write request of a file page to or from
the buffer pool. */
void buf_page_io_complete(
    buf_page_t *bpage); /*!< in: pointer to the block in question */

/*** Calculates a folded value of a file page address to use in the page hash
table.
@return	the folded value */
inline ulint
buf_page_address_fold(ulint space,  /*!< in: space id */
                      ulint offset) /*!< in: offset of the page within space */
    __attribute__((const));

/*** Returns the control block of a file page, NULL if not found.
@return	block, NULL if not found */
inline buf_page_t *
buf_page_hash_get(ulint space,   /*!< in: space id */
                  ulint offset); /*!< in: offset of the page within space */

/*** Returns the control block of a file page, NULL if not found
or an uncompressed page frame does not exist.
@return	block, NULL if not found */
inline buf_block_t *
buf_block_hash_get(ulint space,   /*!< in: space id */
                   ulint offset); /*!< in: offset of the page within space */

/*** Gets the current length of the free list of buffer blocks.
@return	length of the free list */
ulint buf_get_free_list_len(void);

/** The common buffer control block structure
for compressed and uncompressed frames */

struct buf_page_struct {
  /** @name General fields
  None of these bit-fields must be modified without holding
  buf_page_get_mutex() buf_block_struct::mutex since they can be
  stored in the same machine word.  Some of these fields are
  additionally protected by buf_pool_mutex. */
  /* @{ */

  /*!< tablespace id; also protected by buf_pool_mutex. */
  unsigned space : 32;

  /*!< page number; also protected by buf_pool_mutex. */
  unsigned offset : 32;

  /*!< state of the control block; also protected by buf_pool_mutex.
  State transitions from BUF_BLOCK_READY_FOR_USE to BUF_BLOCK_MEMORY
  need not be protected by buf_page_get_mutex().  @see enum buf_page_state */
  unsigned state : 3;

  /*!< if this block is currently being flushed to disk, this tells the
   * flush_type.  @see enum buf_flush */
  unsigned flush_type : 2;

  /*!< type of pending I/O operation; also protected by buf_pool_mutex @see enum
   * buf_io_fix */
  unsigned io_fix : 2;

  /*!< count of how manyfold this block is currently bufferfixed */
  unsigned buf_fix_count : 25;
  /* @} */

  /*!< node used in chaining to buf_pool->page_hash */
  buf_page_t *hash;

#ifdef UNIV_DEBUG
  /*!< true if in buf_pool->page_hash */
  bool in_page_hash;
#endif /* UNIV_DEBUG */

  /** @name Page flushing fields
  All these are protected by buf_pool_mutex. */
  /* @{ */

  UT_LIST_NODE_T(buf_page_t) list;

  /*!< based on state, this is a list node, protected only by buf_pool_mutex, in
  one of the following lists in buf_pool:

  - BUF_BLOCK_NOT_USED:	free

  The contents of the list node is undefined if !in_flush_list && state ==
  BUF_BLOCK_FILE_PAGE, or if state is one of BUF_BLOCK_MEMORY,
  BUF_BLOCK_REMOVE_HASH or
  BUF_BLOCK_READY_IN_USE. */

#ifdef UNIV_DEBUG
  /*!< true if in buf_pool->flush_list; when buf_pool_mutex is free, the
   * following should hold: in_flush_list == (state == BUF_BLOCK_FILE_PAGE ||
   * state == BUF_BLOCK_ZIP_DIRTY) */
  bool in_flush_list;

  /*!< true if in buf_pool->free; when buf_pool_mutex is free, the following
   * should hold: in_free_list == (state == BUF_BLOCK_NOT_USED) */
  bool in_free_list;
#endif /* UNIV_DEBUG */

  /*!< log sequence number of the youngest modification to this block, zero if
   * not modified */
  uint64_t newest_modification;

  /*!< log sequence number of the START of the log entry written of the oldest
   * modification to this block which has not yet been flushed on disk; zero if
   * all modifications are on disk */
  uint64_t oldest_modification;

  /* @} */

  /** @name LRU replacement algorithm fields
  These fields are protected by buf_pool_mutex only
  not the buf_block_struct::mutex). */
  /* @{ */

  /*!< node of the LRU list */
  UT_LIST_NODE_T(buf_page_t) LRU;
#ifdef UNIV_DEBUG
  /*!< true if the page is in the LRU list; used in debugging */
  bool in_LRU_list;
#endif /* UNIV_DEBUG */

  /*!< true if the block is in the old blocks in buf_pool->LRU_old */
  unsigned old : 1;

  /*!< the value of buf_pool->freed_page_clock when this block was the last time
   * put to the head of the LRU list; a thread is allowed to read this for
   * heuristic purposes without holding any mutex or latch */
  unsigned freed_page_clock : 31;

  /*!< time of first access, or 0 if the block was never accessed in the buffer
   * pool */
  unsigned access_time : 32;
  /* @} */
#ifdef UNIV_DEBUG_FILE_ACCESSES
  bool file_page_was_freed;
  /*!< this is set to true when fsp
  frees a page in buffer pool */
#endif /* UNIV_DEBUG_FILE_ACCESSES */
};

/** The buffer control block structure */

struct buf_block_struct {

  /** @name General fields */
  /* @{ */

  /*!< page information; this must be the first field, so that
  buf_pool->page_hash can point to buf_page_t or buf_block_t */
  buf_page_t page;

  /*!< pointer to buffer frame which is of size UNIV_PAGE_SIZE, and
  aligned to an address divisible by UNIV_PAGE_SIZE */
  byte *frame;

  /*!< mutex protecting this block: state (also protected by the buffer
  pool mutex), io_fix, buf_fix_count, and accessed; we introduce this new
  mutex in InnoDB-5.1 to relieve contention on the buffer pool mutex */
  mutex_t mutex;

  rw_lock_t lock;

  /*!< hashed value of the page address in the record lock hash table;
  protected by buf_block_t::lock (or buf_block_t::mutex, buf_pool_mutex
  in buf_page_get_gen(), buf_page_init_for_read() and buf_page_create()) */
  unsigned lock_hash_val : 32;

  /*!< true if we know that this is an index page, and want the database
  to check its consistency before flush; note that there may be pages in the
  buffer pool which are index pages, but this flag is not set because
  we do not keep track of all pages; NOT protected by any mutex */
  bool check_index_page_at_flush;
  /* @} */
  /** @name Optimistic search field */
  /* @{ */

  /*!< this clock is incremented every time a pointer to a record on the
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

  /*!< counter which controls building of a new hash index for the page */
  ulint n_hash_helps;

  /*!< recommended prefix length for hash search: number of full fields */
  ulint n_fields;

  /*!< recommended prefix: number of bytes in an incomplete field */
  ulint n_bytes;

  /*!< true or false, depending on whether the leftmost
  record of several records with the same prefix should
  be indexed in the hash index */
  bool left_side;
  /* @} */

  /** @name Hash search fields
  These 6 fields may only be modified when we have
  an x-latch on btr_search_latch AND
  - we are holding an s-latch or x-latch on buf_block_struct::lock or
  - we know that buf_block_struct::buf_fix_count == 0.

  An exception to this is when we init or create a page
  in the buffer pool in buf0buf.c. */

  /* @{ */

#if defined UNIV_AHI_DEBUG || defined UNIV_DEBUG
  /*!< used in debugging: the number of pointers in the
  adaptive hash index pointing to this frame */
  ulint n_pointers;
#endif /* UNIV_AHI_DEBUG || UNIV_DEBUG */

  /*!< true if hash index has already been built on this page;
  note that it does not guarantee that the index is complete,
though: there may have been hash collisions, record deletions, etc. */
  unsigned is_hashed : 1;

  /*!< prefix length for hash indexing: number of full fields */
  unsigned curr_n_fields : 10;

  /*!< number of bytes in hash indexing */
  unsigned curr_n_bytes : 15;

  /*!< true or false in hash indexing */
  unsigned curr_left_side : 1;

  /*!< Index for which the adaptive hash index has been created. */
  dict_index_t *index;
  /* @} */
#ifdef UNIV_SYNC_DEBUG
  /** @name Debug fields */
  /* @{ */
  /*!< in the debug version, each thread which bufferfixes the block
  acquires an s-latch here; so we can use the debug utilities in sync0rw */
  rw_lock_t debug_latch;
  /* @} */
#endif
};

/** Check if a buf_block_t object is in a valid state
@param block	buffer block
@return		true if valid */
#define buf_block_state_valid(block)                                           \
  (buf_block_get_state(block) >= BUF_BLOCK_NOT_USED &&                         \
   (buf_block_get_state(block) <= BUF_BLOCK_REMOVE_HASH))

/** @brief The buffer pool statistics structure. */
struct buf_pool_stat_struct {
  /*!< number of page gets performed; also successful searches
  through the adaptive hash index are counted as page gets;
  this field is NOT protected by the buffer pool mutex */
  ulint n_page_gets;

  /*!< number read operations */
  ulint n_pages_read;

  /*!< number write operations */
  ulint n_pages_written;

  /*!< number of pages created in the pool with no read */
  ulint n_pages_created;

  /*!< number of pages read in as part of read ahead */
  ulint n_ra_pages_read;

  /*!< number of read ahead pages that are evicted without
  being accessed */
  ulint n_ra_pages_evicted;

  /*!< number of pages made young, in calls to buf_LRU_make_block_young() */
  ulint n_pages_made_young;

  /*!< number of pages not made young because the first access
  was not long enough ago, in buf_page_peek_if_too_old() */
  ulint n_pages_not_made_young;
};

/** @brief The buffer pool structure.

NOTE! The definition appears here only for other modules of this
directory (buf) to see it. Do not use from outside! */

struct buf_pool_struct {

  /** @name General fields */
  /* @{ */

  /*!< number of buffer pool chunks */
  ulint n_chunks;

  /*!< buffer pool chunks */
  buf_chunk_t *chunks;

  /*!< current pool size in pages */
  ulint curr_size;

  /*!< hash table of buf_page_t or buf_block_t file pages,
   buf_page_in_file() == true, indexed by (space_id, offset) */
  hash_table_t *page_hash;

  ulint n_pend_reads; /*!< number of pending read operations */

  /*!< when buf_print_io was last time called */
  time_t last_printout_time;

  /*!< current statistics */
  buf_pool_stat_t stat;

  /*!< old statistics */
  buf_pool_stat_t old_stat;

  /* @} */

  /** @name Page flushing algorithm fields */

  /* @{ */

  /** base node of the modified block list */
  UT_LIST_BASE_NODE_T(buf_page_t) flush_list;

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
  is relevant only in recovery and is set to NULL once the recovery is over. */
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

  /*!< base node of the free block list */
  UT_LIST_BASE_NODE_T(buf_page_t) free;

  /*!< base node of the LRU list */
  UT_LIST_BASE_NODE_T(buf_page_t) LRU;

  /*!< pointer to the about buf_LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV oldest
  blocks in the LRU list; NULL if LRU length less than BUF_LRU_OLD_MIN_LEN;
  NOTE: when LRU_old != NULL, its length should always equal LRU_old_len */
  buf_page_t *LRU_old;

  /*!< length of the LRU list from the block to which LRU_old points onward,
  including that block; see buf0lru.c for the restrictions on this value;
  0 if LRU_old == NULL; NOTE: LRU_old_len must be adjusted whenever LRU_old
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
#define buf_pool_mutex_enter()                                                 \
  do {                                                                         \
    mutex_enter(&buf_pool_mutex);                                              \
  } while (0)

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** Flag to forbid the release of the buffer pool mutex.
Protected by buf_pool_mutex. */
extern ulint buf_pool_mutex_exit_forbidden;

/** Forbid the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_forbid()                                           \
  do {                                                                         \
    ut_ad(buf_pool_mutex_own());                                               \
    buf_pool_mutex_exit_forbidden++;                                           \
  } while (0)

/** Allow the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_allow()                                            \
  do {                                                                         \
    ut_ad(buf_pool_mutex_own());                                               \
    ut_a(buf_pool_mutex_exit_forbidden);                                       \
    buf_pool_mutex_exit_forbidden--;                                           \
  } while (0)

/** Release the buffer pool mutex. */
#define buf_pool_mutex_exit()                                                  \
  do {                                                                         \
    ut_a(!buf_pool_mutex_exit_forbidden);                                      \
    mutex_exit(&buf_pool_mutex);                                               \
  } while (0)
#else
/** Forbid the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_forbid() ((void)0)
/** Allow the release of the buffer pool mutex. */
#define buf_pool_mutex_exit_allow() ((void)0)
/** Release the buffer pool mutex. */
#define buf_pool_mutex_exit() mutex_exit(&buf_pool_mutex)
#endif
/* @} */

/*** Let us list the consistency conditions for different control block states.

NOT_USED:	is in free list, not in LRU list, not in flush list, nor
                page hash table
READY_FOR_USE:	is not in free list, LRU list, or flush list, nor page
                hash table
MEMORY:		is not in free list, LRU list, or flush list, nor page
                hash table
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

#ifndef UNIV_NONINL
#include "buf0buf.ic"
#endif

#endif
