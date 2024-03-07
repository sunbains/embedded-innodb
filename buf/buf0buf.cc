/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** @file buf/buf0buf.c
The database buffer buf_pool

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#include "buf0buf.h"

#include "btr0btr.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "buf0rea.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "lock0lock.h"
#include "log0log.h"
#include "log0recv.h"
#include "mem0mem.h"
#include "os0proc.h"
#include "srv0srv.h"
#include "trx0undo.h"

/*
                IMPLEMENTATION OF THE BUFFER POOL
                =================================

Performance improvement:
------------------------
Thread scheduling in NT may be so slow that the OS wait mechanism should
not be used even in waiting for disk reads to complete.
Rather, we should put waiting query threads to the queue of
waiting jobs, and let the OS thread do something useful while the i/o
is processed. In this way we could remove most OS thread switches in
an i/o-intensive benchmark like TPC-C.

A possibility is to put a user space thread library between the database
and NT. User space thread libraries might be very fast.

SQL Server 7.0 can be configured to use 'fibers' which are lightweight
threads in NT. These should be studied.

                Buffer frames and blocks
                ------------------------
Following the terminology of Gray and Reuter, we call the memory
blocks where file pages are loaded buffer frames. For each buffer
frame there is a control block, or shortly, a block, in the buffer
control array. The control info which does not need to be stored
in the file along with the file page, resides in the control block.

                Buffer pool struct
                ------------------
The buffer buf_pool contains a single mutex which protects all the
control data structures of the buf_pool. The content of a buffer frame is
protected by a separate read-write lock in its control block, though.
These locks can be locked and unlocked without owning the buf_pool mutex.
The OS events in the buf_pool struct can be waited for without owning the
buf_pool mutex.

The buf_pool mutex is a hot-spot in main memory, causing a lot of
memory bus traffic on multiprocessor systems when processors
alternately access the mutex. On our Pentium, the mutex is accessed
maybe every 10 microseconds. We gave up the solution to have mutexes
for each control block, for instance, because it seemed to be
complicated.

A solution to reduce mutex contention of the buf_pool mutex is to
create a separate mutex for the page hash table. On Pentium,
accessing the hash table takes 2 microseconds, about half
of the total buf_pool mutex hold time.

                Control blocks
                --------------

The control block contains, for instance, the bufferfix count
which is incremented when a thread wants a file page to be fixed
in a buffer frame. The bufferfix operation does not lock the
contents of the frame, however. For this purpose, the control
block contains a read-write lock.

The buffer frames have to be aligned so that the start memory
address of a frame is divisible by the universal page size, which
is a power of two.

We intend to make the buffer buf_pool size on-line reconfigurable,
that is, the buf_pool size can be changed without closing the database.
Then the database administarator may adjust it to be bigger
at night, for example. The control block array must
contain enough control blocks for the maximum buffer buf_pool size
which is used in the particular database.
If the buf_pool size is cut, we exploit the virtual memory mechanism of
the OS, and just refrain from using frames at high addresses. Then the OS
can swap them to disk.

The control blocks containing file pages are put to a hash table
according to the file address of the page.
We could speed up the access to an individual page by using
"pointer swizzling": we could replace the page references on
non-leaf index pages by direct pointers to the page, if it exists
in the buf_pool. We could make a separate hash table where we could
chain all the page references in non-leaf pages residing in the buf_pool,
using the page reference as the hash key,
and at the time of reading of a page update the pointers accordingly.
Drawbacks of this solution are added complexity and,
possibly, extra space required on non-leaf pages for memory pointers.
A simpler solution is just to speed up the hash table mechanism
in the database, using tables whose size is a power of 2.

                Lists of blocks
                ---------------

There are several lists of control blocks.

The free list (buf_pool->free) contains blocks which are currently not
used.

The common LRU list contains all the blocks holding a file page
except those for which the bufferfix count is non-zero.
The pages are in the LRU list roughly in the order of the last
access to the page, so that the oldest pages are at the end of the
list. We also keep a pointer to near the end of the LRU list,
which we can use when we want to artificially age a page in the
buf_pool. This is used if we know that some page is not needed
again for some time: we insert the block right after the pointer,
causing it to be replaced sooner than would noramlly be the case.
Currently this aging mechanism is used for read-ahead mechanism
of pages, and it can also be used when there is a scan of a full
table which cannot fit in the memory. Putting the pages near the
of the LRU list, we make sure that most of the buf_pool stays in the
main memory, undisturbed.

The chain of modified blocks (buf_pool->flush_list) contains the blocks
holding file pages that have been modified in the memory
but not written to disk yet. The block with the oldest modification
which has not yet been written to disk is at the end of the chain.

                Loading a file page
                -------------------

First, a victim block for replacement has to be found in the
buf_pool. It is taken from the free list or searched for from the
end of the LRU-list. An exclusive lock is reserved for the frame,
the io_fix field is set in the block fixing the block in buf_pool,
and the io-operation for loading the page is queued. The io-handler thread
releases the X-lock on the frame and resets the io_fix field
when the io operation completes.

A thread may request the above operation using the function
buf_page_get(). It may then continue to request a lock on the frame.
The lock is granted when the io-handler releases the x-lock.

                Read-ahead
                ----------

The read-ahead mechanism is intended to be intelligent and
isolated from the semantically higher levels of the database
index management. From the higher level we only need the
information if a file page has a natural successor or
predecessor page. On the leaf level of a B-tree index,
these are the next and previous pages in the natural
order of the pages.

Let us first explain the read-ahead mechanism when the leafs
of a B-tree are scanned in an ascending or descending order.
When a read page is the first time referenced in the buf_pool,
the buffer manager checks if it is at the border of a so-called
linear read-ahead area. The tablespace is divided into these
areas of size 64 blocks, for example. So if the page is at the
border of such an area, the read-ahead mechanism checks if
all the other blocks in the area have been accessed in an
ascending or descending order. If this is the case, the system
looks at the natural successor or predecessor of the page,
checks if that is at the border of another area, and in this case
issues read-requests for all the pages in that area. Maybe
we could relax the condition that all the pages in the area
have to be accessed: if data is deleted from a table, there may
appear holes of unused pages in the area.

A different read-ahead mechanism is used when there appears
to be a random access pattern to a file.
If a new page is referenced in the buf_pool, and several pages
of its random access area (for instance, 32 consecutive pages
in a tablespace) have recently been referenced, we may predict
that the whole area may be needed in the near future, and issue
the read requests for the whole area.
*/

/** Value in microseconds */
static const int WAIT_FOR_READ = 5000;
/** Number of attemtps made to read in a page in the buffer pool */
static const ulint BUF_PAGE_READ_MAX_RETRIES = 100;

/** The buffer buf_pool of the database */
buf_pool_t *buf_pool = nullptr;

/** mutex protecting the buffer pool struct and control blocks, except the
read-write lock in them */
mutex_t buf_pool_mutex;

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
static ulint buf_dbg_counter = 0; /*!< This is used to insert validation
                                     operations in excution in the
                                     debug version */
/** Flag to forbid the release of the buffer pool mutex.
Protected by buf_pool_mutex. */
ulint buf_pool_mutex_exit_forbidden = 0;
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
/** If this is set true, the program prints info whenever
read-ahead or flush occurs */
bool buf_debug_prints = false;
#endif /* UNIV_DEBUG */

/** A chunk of buffers.  The buffer pool is allocated in chunks. */
struct buf_chunk_t {
  ulint mem_size;      /*!< allocated size of the chunk */
  ulint size;          /*!< size of frames[] and blocks[] */
  void *mem;           /*!< pointer to the memory area which
                       was allocated for the frames */
  buf_block_t *blocks; /*!< array of buffer control blocks */
};

/** Recommends a move of a block to the start of the LRU list if there is danger
of dropping from the buffer pool. NOTE: does not reserve the buffer pool
mutex.
@param[in,out] bpage            Block to make younger.
@return        true if should be made younger */
inline bool buf_page_peek_if_too_old(const buf_page_t *bpage) {
  if (unlikely(buf_pool->freed_page_clock == 0)) {
    /* If eviction has not started yet, do not update the
    statistics or move blocks in the LRU list.  This is
    either the warm-up phase or an in-memory workload. */
    return (false);
  } else if (buf_LRU_old_threshold_ms && bpage->old) {
    unsigned access_time = buf_page_is_accessed(bpage);

    if (access_time > 0 && ((uint32_t)(ut_time_ms() - access_time)) >= buf_LRU_old_threshold_ms) {
      return (true);
    }

    buf_pool->stat.n_pages_not_made_young++;
    return (false);
  } else {
    /* FIXME: bpage->freed_page_clock is 31 bits */
    return (buf_pool->freed_page_clock & ((1UL << 31) - 1)) >
           ((ulint)bpage->freed_page_clock +
            (buf_pool->curr_size * (BUF_LRU_OLD_RATIO_DIV - buf_LRU_old_ratio) / (BUF_LRU_OLD_RATIO_DIV * 4)));
  }
}

void buf_block_free(buf_block_t *block) {
  buf_pool_mutex_enter();

  mutex_enter(&block->mutex);

  ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);

  buf_LRU_block_free_non_file_page(block);

  mutex_exit(&block->mutex);

  buf_pool_mutex_exit();
}

void buf_page_release(buf_block_t *block, ulint rw_latch, mtr_t *mtr) {
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
  ut_a(block->page.buf_fix_count > 0);

  if (rw_latch == RW_X_LATCH && mtr->modifications) {
    buf_pool_mutex_enter();
    buf_flush_note_modification(block, mtr);
    buf_pool_mutex_exit();
  }

  mutex_enter(&block->mutex);

#ifdef UNIV_SYNC_DEBUG
  rw_lock_s_unlock(&(block->debug_latch));
#endif /* UNIV_SYNC_DEBUG */

  --block->page.buf_fix_count;

  mutex_exit(&block->mutex);

  if (rw_latch == RW_S_LATCH) {
    rw_lock_s_unlock(&(block->lock));
  } else if (rw_latch == RW_X_LATCH) {
    rw_lock_x_unlock(&(block->lock));
  }
}

void buf_var_init() {
  buf_pool = nullptr;
  memset(&buf_pool_mutex, 0x0, sizeof(buf_pool_mutex));

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  buf_dbg_counter = 0;
  buf_pool_mutex_exit_forbidden = 0;
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
  buf_debug_prints = false;
#endif /* UNIV_DEBUG */
}

/** Calculates a page checksum which is stored to the page when it is written
to a file. Note that we must be careful to calculate the same value on
32-bit and 64-bit architectures.
@return  checksum */

ulint buf_calc_page_new_checksum(const byte *page) /*!< in: buffer page */
{
  ulint checksum;

  /* Since the field FIL_PAGE_FILE_FLUSH_LSN, and in versions <= 4.1.x
  ...ARCH_LOG_NO, are written outside the buffer pool to the first
  pages of data files, we have to skip them in the page checksum
  calculation.
  We must also skip the field FIL_PAGE_SPACE_OR_CHKSUM where the
  checksum is stored, and also the last 8 bytes of page because
  there we store the old formula checksum. */

  checksum = ut_fold_binary(page + FIL_PAGE_OFFSET, FIL_PAGE_FILE_FLUSH_LSN - FIL_PAGE_OFFSET) +
             ut_fold_binary(page + FIL_PAGE_DATA, UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_END_LSN_OLD_CHKSUM);
  checksum = checksum & 0xFFFFFFFFUL;

  return (checksum);
}

/** In versions < 4.0.14 and < 4.1.1 there was a bug that the checksum only
looked at the first few bytes of the page. This calculates that old
checksum.
NOTE: we must first store the new formula checksum to
FIL_PAGE_SPACE_OR_CHKSUM before calculating and storing this old checksum
because this takes that field as an input!
@return  checksum */

ulint buf_calc_page_old_checksum(const byte *page) /*!< in: buffer page */
{
  ulint checksum;

  checksum = ut_fold_binary(page, FIL_PAGE_FILE_FLUSH_LSN);

  checksum = checksum & 0xFFFFFFFFUL;

  return (checksum);
}

bool buf_page_is_corrupted(const byte *read_buf) {
  ulint checksum_field;
  ulint old_checksum_field;

  if (memcmp(read_buf + FIL_PAGE_LSN + 4, read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + 4, 4)) {

    /* Stored log sequence numbers at the start and the end
    of page do not match */

    return (true);
  }

  if (recv_lsn_checks_on) {
    uint64_t current_lsn;

    if (log_peek_lsn(&current_lsn) && current_lsn < mach_read_from_8(read_buf + FIL_PAGE_LSN)) {
      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "  Error: page %lu log sequence number"
        " %llu\n"
        "is in the future! Current system "
        "log sequence number %llu.\n"
        "Your database may be corrupt or "
        "you may have copied the InnoDB\n"
        "tablespace but not the InnoDB "
        "log files. See\n"
        "the InnoDB website for details\n"
        "for more information.\n",
        (ulong)mach_read_from_4(read_buf + FIL_PAGE_OFFSET),
        (long long unsigned int)mach_read_from_8(read_buf + FIL_PAGE_LSN),
        (long long unsigned int)current_lsn
      );
    }
  }

  /* If we use checksums validation, make additional check before
  returning true to ensure that the checksum is not equal to
  BUF_NO_CHECKSUM_MAGIC which might be stored by InnoDB with checksums
  disabled. Otherwise, skip checksum calculation and return false */

  if (likely(srv_use_checksums)) {
    checksum_field = mach_read_from_4(read_buf + FIL_PAGE_SPACE_OR_CHKSUM);

    old_checksum_field = mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM);

    /* There are 2 valid formulas for old_checksum_field:

    1. Very old versions of InnoDB only stored 8 byte lsn to the
    start and the end of the page.

    2. Newer InnoDB versions store the old formula checksum
    there. */

    if (old_checksum_field != mach_read_from_4(read_buf + FIL_PAGE_LSN) && old_checksum_field != BUF_NO_CHECKSUM_MAGIC && old_checksum_field != buf_calc_page_old_checksum(read_buf)) {

      return (true);
    }

    /* InnoDB versions < 4.0.14 and < 4.1.1 stored the space id
    (always equal to 0), to FIL_PAGE_SPACE_OR_CHKSUM */

    if (checksum_field != 0 && checksum_field != BUF_NO_CHECKSUM_MAGIC && checksum_field != buf_calc_page_new_checksum(read_buf)) {

      return (true);
    }
  }

  return (false);
}

void buf_page_print(const byte *read_buf, ulint) {
  dict_index_t *index;
  auto size = UNIV_PAGE_SIZE;

  ut_print_timestamp(ib_stream);
  ib_logger(ib_stream, "  Page dump in ascii and hex (%lu bytes):\n", (ulong)size);
  ut_print_buf(ib_stream, read_buf, size);
  ib_logger(ib_stream, "\nEnd of page dump\n");

  auto checksum = srv_use_checksums ? buf_calc_page_new_checksum(read_buf) : BUF_NO_CHECKSUM_MAGIC;
  auto old_checksum = srv_use_checksums ? buf_calc_page_old_checksum(read_buf) : BUF_NO_CHECKSUM_MAGIC;

  ut_print_timestamp(ib_stream);
  ib_logger(
    ib_stream,
    "  Page checksum %lu, prior-to-4.0.14-form"
    " checksum %lu\n"
    "stored checksum %lu, prior-to-4.0.14-form"
    " stored checksum %lu\n"
    "Page lsn %lu %lu, low 4 bytes of lsn"
    " at page end %lu\n"
    "Page number (if stored to page already) %lu,\n"
    "space id (if created with >= v4.1.1"
    " and stored already) %lu\n",
    (ulong)checksum,
    (ulong)old_checksum,
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_SPACE_OR_CHKSUM),
    (ulong)mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_LSN),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_LSN + 4),
    (ulong)mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + 4),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_OFFSET),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_SPACE_ID)
  );

  if (mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT) {
    ib_logger(ib_stream, "Page may be an insert undo log page\n");
  } else if (mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE) {
    ib_logger(ib_stream, "Page may be an update undo log page\n");
  }

  switch (fil_page_get_type(read_buf)) {
    case FIL_PAGE_INDEX:
      ib_logger(
        ib_stream,
        "Page may be an index page where"
        " index id is  %lu\n",
        (uint64_t)btr_page_get_index_id(read_buf)
      );
      index = dict_index_find_on_id_low(btr_page_get_index_id(read_buf));
      if (index) {
        ib_logger(ib_stream, "(");
        dict_index_name_print(ib_stream, nullptr, index);
        ib_logger(ib_stream, ")\n");
      }
      break;
    case FIL_PAGE_INODE:
      ib_logger(ib_stream, "Page may be an 'inode' page\n");
      break;
    case FIL_PAGE_TYPE_ALLOCATED:
      ib_logger(ib_stream, "Page may be a freshly allocated page\n");
      break;
    case FIL_PAGE_TYPE_SYS:
      ib_logger(ib_stream, "Page may be a system page\n");
      break;
    case FIL_PAGE_TYPE_TRX_SYS:
      ib_logger(ib_stream, "Page may be a transaction system page\n");
      break;
    case FIL_PAGE_TYPE_FSP_HDR:
      ib_logger(ib_stream, "Page may be a file space header page\n");
      break;
    case FIL_PAGE_TYPE_XDES:
      ib_logger(ib_stream, "Page may be an extent descriptor page\n");
      break;
    case FIL_PAGE_TYPE_BLOB:
      ib_logger(ib_stream, "Page may be a BLOB page\n");
      break;
    case FIL_PAGE_TYPE_ZBLOB:
    case FIL_PAGE_TYPE_ZBLOB2:
      ib_logger(ib_stream, "Page may be a compressed BLOB page\n");
      break;
  }
}

/** Initializes a buffer control block when the buf_pool is created. */
static void buf_block_init(
  buf_block_t *block, /*!< in: pointer to control block */
  byte *frame
) /*!< in: pointer to buffer frame */
{
  UNIV_MEM_DESC(frame, UNIV_PAGE_SIZE, block);

  block->frame = frame;

  block->page.state = BUF_BLOCK_NOT_USED;
  block->page.buf_fix_count = 0;
  block->page.io_fix = BUF_IO_NONE;

  block->modify_clock = 0;

#ifdef UNIV_DEBUG
  block->page.file_page_was_freed = false;
#endif /* UNIV_DEBUG */

  block->check_index_page_at_flush = false;

#ifdef UNIV_DEBUG
  block->page.in_page_hash = false;
  block->page.in_flush_list = false;
  block->page.in_free_list = false;
  block->page.in_LRU_list = false;
#endif /* UNIV_DEBUG */

  mutex_create(&block->mutex, SYNC_BUF_BLOCK);

  rw_lock_create(&block->lock, SYNC_LEVEL_VARYING);
  ut_ad(rw_lock_validate(&(block->lock)));

#ifdef UNIV_SYNC_DEBUG
  rw_lock_create(&block->debug_latch, SYNC_NO_ORDER_CHECK);
#endif /* UNIV_SYNC_DEBUG */
}

/** Allocates a chunk of buffer frames.
@return  chunk, or nullptr on failure */
static buf_chunk_t *buf_chunk_init(
  buf_chunk_t *chunk, /*!< out: chunk of buffers */
  ulint mem_size
) /*!< in: requested size in bytes */
{
  buf_block_t *block;
  byte *frame;
  ulint i;

  /* Round down to a multiple of page size,
  although it already should be. */
  mem_size = ut_2pow_round(mem_size, UNIV_PAGE_SIZE);
  /* Reserve space for the block descriptors. */
  mem_size += ut_2pow_round((mem_size / UNIV_PAGE_SIZE) * (sizeof *block) + (UNIV_PAGE_SIZE - 1), UNIV_PAGE_SIZE);

  chunk->mem_size = mem_size;
  chunk->mem = os_mem_alloc_large(&chunk->mem_size);

  if (unlikely(chunk->mem == nullptr)) {

    return (nullptr);
  }

  /* Allocate the block descriptors from
  the start of the memory block. */
  chunk->blocks = (buf_block_t *)chunk->mem;

  /* Align a pointer to the first frame.  Note that when
  os_large_page_size is smaller than UNIV_PAGE_SIZE,
  we may allocate one fewer block than requested.  When
  it is bigger, we may allocate more blocks than requested. */

  frame = (byte *)ut_align((byte *)chunk->mem, UNIV_PAGE_SIZE);
  chunk->size = chunk->mem_size / UNIV_PAGE_SIZE - (frame != chunk->mem);

  /* Subtract the space needed for block descriptors. */
  {
    ulint size = chunk->size;

    while (frame < (byte *)(chunk->blocks + size)) {
      frame += UNIV_PAGE_SIZE;
      size--;
    }

    chunk->size = size;
  }

  /* Init block structs and assign frames for them. Then we
  assign the frames to the first blocks (we already mapped the
  memory above). */

  block = chunk->blocks;

  for (i = chunk->size; i--;) {

    buf_block_init(block, frame);

#ifdef HAVE_purify
    /* Wipe contents of frame to eliminate a Purify warning */
    memset(block->frame, '\0', UNIV_PAGE_SIZE);
#endif
    /* Add the block to the free list */
    UT_LIST_ADD_LAST(list, buf_pool->free, (&block->page));
    ut_d(block->page.in_free_list = true);

    block++;
    frame += UNIV_PAGE_SIZE;
  }

  return (chunk);
}

/** Checks that all file pages in the buffer chunk are in a replaceable state.
@return  address of a non-free block, or nullptr if all freed */
static const buf_block_t *buf_chunk_not_freed(buf_chunk_t *chunk) /*!< in: chunk being checked */
{
  buf_block_t *block;
  ulint i;

  ut_ad(buf_pool);
  ut_ad(buf_pool_mutex_own());

  block = chunk->blocks;

  for (i = chunk->size; i--; block++) {
    bool ready;

    switch (buf_block_get_state(block)) {
      case BUF_BLOCK_NOT_USED:
      case BUF_BLOCK_READY_FOR_USE:
      case BUF_BLOCK_MEMORY:
      case BUF_BLOCK_REMOVE_HASH:
        /* Skip blocks that are not being used for
      file pages. */
        break;
      case BUF_BLOCK_FILE_PAGE:
        mutex_enter(&block->mutex);
        ready = buf_flush_ready_for_replace(&block->page);
        mutex_exit(&block->mutex);

        if (!ready) {

          return (block);
        }

        break;
    }
  }

  return (nullptr);
}

buf_pool_t *buf_pool_init() {
  buf_chunk_t *chunk;
  ulint i;

  buf_pool = (buf_pool_t *)mem_zalloc(sizeof(buf_pool_t));

  /* 1. Initialize general fields
  ------------------------------- */
  mutex_create(&buf_pool_mutex, SYNC_BUF_POOL);

  buf_pool_mutex_enter();

  buf_pool->n_chunks = 1;
  buf_pool->chunks = chunk = (buf_chunk_t *)mem_alloc(sizeof *chunk);

  UT_LIST_INIT(buf_pool->free);

  if (!buf_chunk_init(chunk, srv_buf_pool_size)) {
    mem_free(chunk);
    mem_free(buf_pool);
    buf_pool = nullptr;
    buf_pool_mutex_exit();
    return (nullptr);
  }

  srv_buf_pool_old_size = srv_buf_pool_size;
  buf_pool->curr_size = chunk->size;
  srv_buf_pool_curr_size = buf_pool->curr_size * UNIV_PAGE_SIZE;

  buf_pool->page_hash = hash_create(2 * buf_pool->curr_size);

  buf_pool->last_printout_time = ut_time();

  /* 2. Initialize flushing fields */

  for (i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    buf_pool->no_flush[i] = os_event_create(nullptr);
  }

  /* 3. Initialize LRU fields */
  /* All fields are initialized by mem_zalloc(). */

  /* Initialize red-black tree for fast insertions into the
  flush_list during recovery process.
  As this initialization is done while holding the buffer pool
  mutex we perform it before acquiring recv_sys->mutex. */
  buf_flush_init_flush_rbt();

  buf_pool_mutex_exit();

  /* 4. Initialize the buddy allocator fields */
  /* All fields are initialized by mem_zalloc(). */

  return buf_pool;
}

void buf_close() {
  /* This can happen if we abort during the startup phase. */
  if (buf_pool == nullptr) {
    return;
  }

  hash_table_free(buf_pool->page_hash);
  buf_pool->page_hash = nullptr;

  for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    os_event_free(buf_pool->no_flush[i]);
    buf_pool->no_flush[i] = nullptr;
  }
}

void buf_mem_free() {
  if (buf_pool != nullptr) {
    buf_chunk_t *chunk;
    buf_chunk_t *chunks;

    chunks = buf_pool->chunks;
    chunk = chunks + buf_pool->n_chunks;

    while (--chunk >= chunks) {
      /* Bypass the checks of buf_chunk_free(), since they
      would fail at shutdown. */
      os_mem_free_large(chunk->mem, chunk->mem_size);
    }

    buf_pool->n_chunks = 0;

    mem_free(buf_pool->chunks);
    mem_free(buf_pool);
    buf_pool = nullptr;
  }
}

void buf_pool_drop_hash_index() {
  ut_error;
}

void buf_page_make_young(buf_page_t *bpage) {
  buf_pool_mutex_enter();

  ut_a(buf_page_in_file(bpage));

  buf_LRU_make_block_young(bpage);

  buf_pool_mutex_exit();
}

/** Sets the time of the first access of a page and moves a page to the
start of the buffer pool LRU list if it is too old.  This high-level
function can be used to prevent an important page from slipping
out of the buffer pool. */
static void buf_page_set_accessed_make_young(
  buf_page_t *bpage, /*!< in/out: buffer block of a
                          file page */
  unsigned access_time
) /*!< in: bpage->access_time
                          read under mutex protection,
                          or 0 if unknown */
{
  ut_ad(!buf_pool_mutex_own());
  ut_a(buf_page_in_file(bpage));

  if (buf_page_peek_if_too_old(bpage)) {
    buf_pool_mutex_enter();
    buf_LRU_make_block_young(bpage);
    buf_pool_mutex_exit();
  } else if (!access_time) {
    ulint time_ms = ut_time_ms();
    buf_pool_mutex_enter();
    buf_page_set_accessed(bpage, time_ms);
    buf_pool_mutex_exit();
  }
}

void buf_reset_check_index_page_at_flush(space_id_t space, ulint offset) {
  buf_pool_mutex_enter();

  auto block = (buf_block_t *)buf_page_hash_get(space, offset);

  if (block && buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE) {
    block->check_index_page_at_flush = false;
  }

  buf_pool_mutex_exit();
}

bool buf_page_peek_if_search_hashed(space_id_t space, ulint offset) {
  buf_pool_mutex_enter();

  auto block = (buf_block_t *)buf_page_hash_get(space, offset);

  bool is_hashed;

  if (!block || buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE) {
    is_hashed = false;
  } else {
    is_hashed = block->is_hashed;
  }

  buf_pool_mutex_exit();

  return is_hashed;
}

#ifdef UNIV_DEBUG
buf_page_t *buf_page_set_file_page_was_freed(space_id_t space, page_no_t offset) {
  buf_pool_mutex_enter();

  auto bpage = buf_page_hash_get(space, offset);

  if (bpage != nullptr) {
    bpage->file_page_was_freed = true;
  }

  buf_pool_mutex_exit();

  return bpage;
}

/** Sets file_page_was_freed false if the page is found in the buffer pool.
This function should be called when we free a file page and want the
debug version to check that it is not accessed any more unless
reallocated.
@return  control block if found in page hash table, otherwise nullptr */

buf_page_t *buf_page_reset_file_page_was_freed(
  space_id_t space, /*!< in: space id */
  ulint offset
) /*!< in: page number */
{
  buf_page_t *bpage;

  buf_pool_mutex_enter();

  bpage = buf_page_hash_get(space, offset);

  if (bpage) {
    bpage->file_page_was_freed = false;
  }

  buf_pool_mutex_exit();

  return (bpage);
}
#endif /* UNIV_DEBUG */

/** Initialize some fields of a control block. */
inline void buf_block_init_low(buf_block_t *block) /*!< in: block to init */
{
  block->check_index_page_at_flush = false;
  block->n_hash_helps = 0;
  block->is_hashed = false;
  block->n_fields = 1;
  block->n_bytes = 0;
  block->left_side = true;
}

buf_block_t *buf_block_align(const byte *ptr) {
  buf_chunk_t *chunk;
  ulint i;

  /* TODO: protect buf_pool->chunks with a mutex (it will
  currently remain constant after buf_pool_init()) */
  for (chunk = buf_pool->chunks, i = buf_pool->n_chunks; i--; chunk++) {
    lint offs = ptr - chunk->blocks->frame;

    if (unlikely(offs < 0)) {

      continue;
    }

    offs >>= UNIV_PAGE_SIZE_SHIFT;

    if (likely((ulint)offs < chunk->size)) {
      buf_block_t *block = &chunk->blocks[offs];

      /* The function buf_chunk_init() invokes
      buf_block_init() so that block[n].frame ==
      block->frame + n * UNIV_PAGE_SIZE.  Check it. */
      ut_ad(block->frame == page_align(ptr));
#ifdef UNIV_DEBUG
      /* A thread that updates these fields must
      hold buf_pool_mutex and block->mutex.  Acquire
      only the latter. */
      mutex_enter(&block->mutex);

      switch (buf_block_get_state(block)) {
        case BUF_BLOCK_NOT_USED:
        case BUF_BLOCK_READY_FOR_USE:
        case BUF_BLOCK_MEMORY:
          /* Some data structures contain
        "guess" pointers to file pages.  The
        file pages may have been freed and
        reused.  Do not complain. */
          break;
        case BUF_BLOCK_REMOVE_HASH:
          /* buf_LRU_block_remove_hashed_page()
        will overwrite the FIL_PAGE_OFFSET and
        FIL_PAGE_SPACE_ID with
        0xff and set the state to
        BUF_BLOCK_REMOVE_HASH. */
          ut_ad(page_get_space_id(page_align(ptr)) == 0xffffffff);
          ut_ad(page_get_page_no(page_align(ptr)) == 0xffffffff);
          break;
        case BUF_BLOCK_FILE_PAGE:
          ut_ad(block->page.space == page_get_space_id(page_align(ptr)));
          ut_ad(block->page.offset == page_get_page_no(page_align(ptr)));
          break;
      }

      mutex_exit(&block->mutex);
#endif /* UNIV_DEBUG */

      return (block);
    }
  }

  /* The block should always be found. */
  ut_error;
  return (nullptr);
}

/** Find out if a pointer belongs to a buf_block_t. It can be a pointer to
the buf_block_t itself or a member of it
@return  true if ptr belongs to a buf_block_t struct */

bool buf_pointer_is_block_field(const void *ptr) /*!< in: pointer not
                                                  dereferenced */
{
  const buf_chunk_t *chunk = buf_pool->chunks;
  const buf_chunk_t *const echunk = chunk + buf_pool->n_chunks;

  /* TODO: protect buf_pool->chunks with a mutex (it will
  currently remain constant after buf_pool_init()) */
  while (chunk < echunk) {
    if (ptr >= (void *)chunk->blocks && ptr < (void *)(chunk->blocks + chunk->size)) {

      return (true);
    }

    chunk++;
  }

  return (false);
}

/** Find out if a buffer block was created by buf_chunk_init().
@return  true if "block" has been added to buf_pool->free by buf_chunk_init() */
static bool buf_block_is_uncompressed(const buf_block_t *block) /*!< in: pointer to block,
                                                    not dereferenced */
{
  ut_ad(buf_pool_mutex_own());

  if (unlikely((((ulint)block) % sizeof *block) != 0)) {
    /* The pointer should be aligned. */
    return (false);
  }

  return (buf_pointer_is_block_field((void *)block));
}

buf_block_t *buf_page_get_gen(
  space_id_t space, ulint offset, ulint rw_latch, buf_block_t *guess, ulint mode, const char *file, ulint line, mtr_t *mtr
) {
  buf_block_t *block;
  unsigned access_time;
  ulint fix_type;
  bool must_read;
  ulint retries = 0;

  ut_ad(mtr);
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH) || (rw_latch == RW_NO_LATCH));
  ut_ad((mode != BUF_GET_NO_LATCH) || (rw_latch == RW_NO_LATCH));
  ut_ad((mode == BUF_GET) || (mode == BUF_GET_IF_IN_POOL) || (mode == BUF_GET_NO_LATCH));

  ++buf_pool->stat.n_page_gets;

loop:
  buf_pool_mutex_enter();

  block = guess;

  if (block != nullptr) {
    /* If the guess is a compressed page descriptor that
    has been allocated by buf_buddy_alloc(), it may have
    been invalidated by buf_buddy_relocate().  In that
    case, block could point to something that happens to
    contain the expected bits in block->page.  Similarly,
    the guess may be pointing to a buffer pool chunk that
    has been released when resizing the buffer pool. */

    if (!buf_block_is_uncompressed(block) || offset != block->page.offset || space != block->page.space || buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE) {

      block = guess = nullptr;
    } else {
      ut_ad(block->page.in_page_hash);
    }
  }

  if (block == nullptr) {
    block = (buf_block_t *)buf_page_hash_get(space, offset);
  }

  if (block == nullptr) {
    /* Page not in buf_pool: needs to be read from file */

    buf_pool_mutex_exit();

    if (mode == BUF_GET_IF_IN_POOL) {

      return (nullptr);
    }

    if (buf_read_page(space, offset)) {
      retries = 0;
    } else if (retries < BUF_PAGE_READ_MAX_RETRIES) {
      ++retries;
    } else {
      ib_logger(
        ib_stream,
        "Error: Unable"
        " to read tablespace %lu page no"
        " %lu into the buffer pool after"
        " %lu attempts\n"
        "The most probable cause"
        " of this error may be that the"
        " table has been corrupted.\n"
        "You can try to fix this"
        " problem by using"
        " innodb_force_recovery.\n"
        "Please see reference manual"
        " for more details.\n"
        "Aborting...\n",
        space,
        offset,
        BUF_PAGE_READ_MAX_RETRIES
      );

      ut_error;
    }

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
    ut_a(++buf_dbg_counter % 37 || buf_validate());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
    goto loop;
  }

  must_read = buf_block_get_io_fix(block) == BUF_IO_READ;

  if (must_read && mode == BUF_GET_IF_IN_POOL) {
    /* The page is only being read to buffer */
    buf_pool_mutex_exit();

    return (nullptr);
  }

  switch (buf_block_get_state(block)) {
    case BUF_BLOCK_FILE_PAGE:
      break;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);

  mutex_enter(&block->mutex);
  UNIV_MEM_ASSERT_RW(&block->page, sizeof block->page);

  buf_block_buf_fix_inc(block, file, line);

  mutex_exit(&block->mutex);

  /* Check if this is the first access to the page */

  access_time = buf_page_is_accessed(&block->page);

  buf_pool_mutex_exit();

  buf_page_set_accessed_make_young(&block->page, access_time);

#ifdef UNIV_DEBUG
  ut_a(!block->page.file_page_was_freed);
#endif

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(++buf_dbg_counter % 5771 || buf_validate());
  ut_a(block->page.buf_fix_count > 0);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  switch (rw_latch) {
    case RW_NO_LATCH:
      if (must_read) {
        /* Let us wait until the read operation
      completes */

        for (;;) {
          enum buf_io_fix io_fix;

          mutex_enter(&block->mutex);
          io_fix = buf_block_get_io_fix(block);
          mutex_exit(&block->mutex);

          if (io_fix == BUF_IO_READ) {

            os_thread_sleep(WAIT_FOR_READ);
          } else {
            break;
          }
        }
      }

      fix_type = MTR_MEMO_BUF_FIX;
      break;

    case RW_S_LATCH:
      rw_lock_s_lock_func(&(block->lock), 0, file, line);

      fix_type = MTR_MEMO_PAGE_S_FIX;
      break;

    default:
      ut_ad(rw_latch == RW_X_LATCH);
      rw_lock_x_lock_func(&(block->lock), 0, file, line);

      fix_type = MTR_MEMO_PAGE_X_FIX;
      break;
  }

  mtr_memo_push(mtr, block, fix_type);

  if (!access_time) {
    /* In the case of a first access, try to apply linear read-ahead */

    buf_read_ahead_linear(space, offset);
  }

  return block;
}

/** This is the general function used to get optimistic access to a database
page.
@return  true if success */

bool buf_page_optimistic_get(
  ulint rw_latch,        /*!< in: RW_S_LATCH, RW_X_LATCH */
  buf_block_t *block,    /*!< in: guessed buffer block */
  uint64_t modify_clock, /*!< in: modify clock value if mode is
                         ..._GUESS_ON_CLOCK */
  const char *file,      /*!< in: file name */
  ulint line,            /*!< in: line where called */
  mtr_t *mtr
) /*!< in: mini-transaction */
{
  unsigned access_time;
  bool success;
  ulint fix_type;

  ut_ad(block);
  ut_ad(mtr);
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

  mutex_enter(&block->mutex);

  if (unlikely(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE)) {

    mutex_exit(&block->mutex);

    return (false);
  }

  buf_block_buf_fix_inc(block, file, line);

  mutex_exit(&block->mutex);

  /* Check if this is the first access to the page.
  We do a dirty read on purpose, to avoid mutex contention.
  This field is only used for heuristic purposes; it does not
  affect correctness. */

  access_time = buf_page_is_accessed(&block->page);
  buf_page_set_accessed_make_young(&block->page, access_time);

  if (rw_latch == RW_S_LATCH) {
    success = rw_lock_s_lock_nowait(&(block->lock), file, line);
    fix_type = MTR_MEMO_PAGE_S_FIX;
  } else {
    success = rw_lock_x_lock_func_nowait(&(block->lock), file, line);
    fix_type = MTR_MEMO_PAGE_X_FIX;
  }

  if (unlikely(!success)) {
    mutex_enter(&block->mutex);
    buf_block_buf_fix_dec(block);
    mutex_exit(&block->mutex);

    return (false);
  }

  if (unlikely(modify_clock != block->modify_clock)) {
    buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

    if (rw_latch == RW_S_LATCH) {
      rw_lock_s_unlock(&(block->lock));
    } else {
      rw_lock_x_unlock(&(block->lock));
    }

    mutex_enter(&block->mutex);
    buf_block_buf_fix_dec(block);
    mutex_exit(&block->mutex);

    return (false);
  }

  mtr_memo_push(mtr, block, fix_type);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(++buf_dbg_counter % 5771 || buf_validate());
  ut_a(block->page.buf_fix_count > 0);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
  ut_a(block->page.file_page_was_freed == false);
#endif
  if (unlikely(!access_time)) {
    /* In the case of a first access, try to apply linear
    read-ahead */

    buf_read_ahead_linear(buf_block_get_space(block), buf_block_get_page_no(block));
  }

  ++buf_pool->stat.n_page_gets;

  return true;
}

/** This is used to get access to a known database page, when no waiting can be
done. For example, if a search in an adaptive hash index leads us to this
frame.
@return  true if success */

bool buf_page_get_known_nowait(
  ulint rw_latch,     /*!< in: RW_S_LATCH, RW_X_LATCH */
  buf_block_t *block, /*!< in: the known page */
  ulint mode,         /*!< in: BUF_MAKE_YOUNG or BUF_KEEP_OLD */
  const char *file,   /*!< in: file name */
  ulint line,         /*!< in: line where called */
  mtr_t *mtr
) /*!< in: mini-transaction */
{
  bool success;
  ulint fix_type;

  ut_ad(mtr);
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

  mutex_enter(&block->mutex);

  if (buf_block_get_state(block) == BUF_BLOCK_REMOVE_HASH) {
    /* Another thread is just freeing the block from the LRU list
    of the buffer pool: do not try to access this page; this
    attempt to access the page can only come through the hash
    index because when the buffer block state is ..._REMOVE_HASH,
    we have already removed it from the page address hash table
    of the buffer pool. */

    mutex_exit(&block->mutex);

    return (false);
  }

  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);

  buf_block_buf_fix_inc(block, file, line);

  mutex_exit(&block->mutex);

  if (mode == BUF_MAKE_YOUNG && buf_page_peek_if_too_old(&block->page)) {
    buf_pool_mutex_enter();
    buf_LRU_make_block_young(&block->page);
    buf_pool_mutex_exit();
  } else if (!buf_page_is_accessed(&block->page)) {
    /* Above, we do a dirty read on purpose, to avoid
    mutex contention.  The field buf_page_t::access_time
    is only used for heuristic purposes.  Writes to the
    field must be protected by mutex, however. */
    ulint time_ms = ut_time_ms();

    buf_pool_mutex_enter();
    buf_page_set_accessed(&block->page, time_ms);
    buf_pool_mutex_exit();
  }

  if (rw_latch == RW_S_LATCH) {
    success = rw_lock_s_lock_nowait(&(block->lock), file, line);
    fix_type = MTR_MEMO_PAGE_S_FIX;
  } else {
    success = rw_lock_x_lock_func_nowait(&(block->lock), file, line);
    fix_type = MTR_MEMO_PAGE_X_FIX;
  }

  if (!success) {
    mutex_enter(&block->mutex);
    buf_block_buf_fix_dec(block);
    mutex_exit(&block->mutex);

    return (false);
  }

  mtr_memo_push(mtr, block, fix_type);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(++buf_dbg_counter % 5771 || buf_validate());
  ut_a(block->page.buf_fix_count > 0);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
#ifdef UNIV_DEBUG
  ut_a(block->page.file_page_was_freed == false);
#endif

  ++buf_pool->stat.n_page_gets;

  return true;
}

/** Given a tablespace id and page number tries to get that page. If the
page is not in the buffer pool it is not loaded and nullptr is returned.
Suitable for using when holding the kernel mutex.
@return  pointer to a page or nullptr */

const buf_block_t *buf_page_try_get_func(
  ulint space_id,   /*!< in: tablespace id */
  ulint page_no,    /*!< in: page number */
  const char *file, /*!< in: file name */
  ulint line,       /*!< in: line where called */
  mtr_t *mtr
) /*!< in: mini-transaction */
{
  buf_block_t *block;
  bool success;
  ulint fix_type;

  ut_ad(mtr);
  ut_ad(mtr->state == MTR_ACTIVE);

  buf_pool_mutex_enter();
  block = buf_block_hash_get(space_id, page_no);

  if (!block) {
    buf_pool_mutex_exit();
    return (nullptr);
  }

  mutex_enter(&block->mutex);
  buf_pool_mutex_exit();

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
  ut_a(buf_block_get_space(block) == space_id);
  ut_a(buf_block_get_page_no(block) == page_no);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  buf_block_buf_fix_inc(block, file, line);
  mutex_exit(&block->mutex);

  fix_type = MTR_MEMO_PAGE_S_FIX;
  success = rw_lock_s_lock_nowait(&block->lock, file, line);

  if (!success) {
    /* Let us try to get an X-latch. If the current thread
    is holding an X-latch on the page, we cannot get an
    S-latch. */

    fix_type = MTR_MEMO_PAGE_X_FIX;
    success = rw_lock_x_lock_func_nowait(&block->lock, file, line);
  }

  if (!success) {
    mutex_enter(&block->mutex);
    buf_block_buf_fix_dec(block);
    mutex_exit(&block->mutex);

    return (nullptr);
  }

  mtr_memo_push(mtr, block, fix_type);
#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(++buf_dbg_counter % 5771 || buf_validate());
  ut_a(block->page.buf_fix_count > 0);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
#ifdef UNIV_DEBUG
  ut_a(block->page.file_page_was_freed == false);
#endif /* UNIV_DEBUG */
  buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

  buf_pool->stat.n_page_gets++;

  return block;
}

/** Initialize some fields of a control block. */
inline void buf_page_init_low(buf_page_t *bpage) /*!< in: block to init */
{
  bpage->flush_type = BUF_FLUSH_LRU;
  bpage->io_fix = BUF_IO_NONE;
  bpage->buf_fix_count = 0;
  bpage->freed_page_clock = 0;
  bpage->access_time = 0;
  bpage->newest_modification = 0;
  bpage->oldest_modification = 0;
  HASH_INVALIDATE(bpage, hash);
#ifdef UNIV_DEBUG
  bpage->file_page_was_freed = false;
#endif /* UNIV_DEBUG */
}

/** Inits a page to the buffer buf_pool. */
static void buf_page_init(
  space_id_t space, /*!< in: space id */
  ulint offset,     /*!< in: offset of the page within space
                                        in units of a page */
  buf_block_t *block
) /*!< in: block to init */
{
  buf_page_t *hash_page;

  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&(block->mutex)));
  ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);

  /* Set the state of the block */
  buf_block_set_file_page(block, space, offset);

#ifdef UNIV_DEBUG_VALGRIND
  if (!space) {
    /* Silence valid Valgrind warnings about uninitialized
    data being written to data files.  There are some unused
    bytes on some pages that InnoDB does not initialize. */
    UNIV_MEM_VALID(block->frame, UNIV_PAGE_SIZE);
  }
#endif /* UNIV_DEBUG_VALGRIND */

  buf_block_init_low(block);

  block->lock_hash_val = lock_rec_hash(space, offset);

  /* Insert into the hash table of file pages */

  hash_page = buf_page_hash_get(space, offset);

  if (likely_null(hash_page)) {
    ib_logger(
      ib_stream,
      "Error: page %lu %lu already found"
      " in the hash table: %p, %p\n",
      (ulong)space,
      (ulong)offset,
      (const void *)hash_page,
      (const void *)block
    );
#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
    mutex_exit(&block->mutex);
    buf_pool_mutex_exit();
    buf_print();
    buf_LRU_print();
    buf_validate();
    buf_LRU_validate();
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
    ut_error;
  }

  buf_page_init_low(&block->page);

  ut_ad(!block->page.in_page_hash);
  ut_d(block->page.in_page_hash = true);
  HASH_INSERT(buf_page_t, hash, buf_pool->page_hash, buf_page_address_fold(space, offset), &block->page);
}

buf_page_t *buf_page_init_for_read(db_err *err, ulint mode, space_id_t space, int64_t tablespace_version, ulint offset) {
  buf_block_t *block;
  buf_page_t *bpage = nullptr;

  ut_ad(buf_pool);

  *err = DB_SUCCESS;

  ut_ad(mode == BUF_READ_ANY_PAGE);

  block = buf_LRU_get_free_block();
  ut_ad(block != nullptr);

  buf_pool_mutex_enter();

  if (buf_page_hash_get(space, offset)) {
    /* The page is already in the buffer pool. */
  err_exit:
    if (block) {
      mutex_enter(&block->mutex);
      buf_LRU_block_free_non_file_page(block);
      mutex_exit(&block->mutex);
    }

    bpage = nullptr;
    goto func_exit;
  }

  if (fil_tablespace_deleted_or_being_deleted_in_mem(space, tablespace_version)) {
    /* The page belongs to a space which has been
    deleted or is being deleted. */
    *err = DB_TABLESPACE_DELETED;

    goto err_exit;
  }

  if (block != nullptr) {
    bpage = &block->page;

    mutex_enter(&block->mutex);

    buf_page_init(space, offset, block);

    /* The block must be put to the LRU list, to the old blocks */
    buf_LRU_add_block(bpage, true /* to old blocks */);

    /* We set a pass-type x-lock on the frame because then
    the same thread which called for the read operation
    (and is running now at this point of code) can wait
    for the read to complete by waiting for the x-lock on
    the frame; if the x-lock were recursive, the same
    thread would illegally get the x-lock before the page
    read is completed.  The x-lock is cleared by the
    io-handler thread. */

    rw_lock_x_lock_gen(&block->lock, BUF_IO_READ);
    buf_page_set_io_fix(bpage, BUF_IO_READ);

    mutex_exit(&block->mutex);
  }

  buf_pool->n_pend_reads++;

func_exit:
  buf_pool_mutex_exit();

  ut_ad(bpage == nullptr || buf_page_in_file(bpage));
  return bpage;
}

buf_block_t *buf_page_create(space_id_t space, ulint offset, mtr_t *mtr) {
  buf_frame_t *frame;
  buf_block_t *block;
  buf_block_t *free_block = nullptr;
  ulint time_ms = ut_time_ms();

  ut_ad(mtr);
  ut_ad(mtr->state == MTR_ACTIVE);

  free_block = buf_LRU_get_free_block();

  buf_pool_mutex_enter();

  block = (buf_block_t *)buf_page_hash_get(space, offset);

  if (block && buf_page_in_file(&block->page)) {
#ifdef UNIV_DEBUG
    block->page.file_page_was_freed = false;
#endif /* UNIV_DEBUG */

    /* Page can be found in buf_pool */
    buf_pool_mutex_exit();

    buf_block_free(free_block);

    return (buf_page_get_with_no_latch(space, 0, offset, mtr));
  }

  /* If we get here, the page was not in buf_pool: init it there */

#ifdef UNIV_DEBUG
  if (buf_debug_prints) {
    ib_logger(ib_stream, "Creating space %lu page %lu to buffer\n", (ulong)space, (ulong)offset);
  }
#endif /* UNIV_DEBUG */

  block = free_block;

  mutex_enter(&block->mutex);

  buf_page_init(space, offset, block);

  /* The block must be put to the LRU list */
  buf_LRU_add_block(&block->page, false);

  buf_block_buf_fix_inc(block, __FILE__, __LINE__);
  buf_pool->stat.n_pages_created++;

  buf_page_set_accessed(&block->page, time_ms);

  buf_pool_mutex_exit();

  mtr_memo_push(mtr, block, MTR_MEMO_BUF_FIX);

  mutex_exit(&block->mutex);

  /* Delete possible entries for the page from the insert buffer:
  such can exist if the page belonged to an index which was dropped */

  /* Flush pages from the end of the LRU list if necessary */
  buf_flush_free_margin();

  frame = block->frame;

  memset(frame + FIL_PAGE_PREV, 0xff, 4);
  memset(frame + FIL_PAGE_NEXT, 0xff, 4);
  mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_ALLOCATED);

  /* Reset to zero the file flush lsn field in the page; if the first
  page of an ibdata file is 'created' in this function into the buffer
  pool then we lose the original contents of the file flush lsn stamp.
  Then InnoDB could in a crash recovery print a big, false, corruption
  warning if the stamp contains an lsn bigger than the ib_logfile lsn. */

  memset(frame + FIL_PAGE_FILE_FLUSH_LSN, 0, 8);

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
  ut_a(++buf_dbg_counter % 357 || buf_validate());
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

  return block;
}

void buf_page_io_complete(buf_page_t *bpage) {
  ut_a(buf_page_in_file(bpage));

  /* We do not need protect io_fix here by mutex to read
  it because this is the only function where we can change the value
  from BUF_IO_READ or BUF_IO_WRITE to some other value, and our code
  ensures that this is the only thread that handles the i/o for this
  block. */

  auto io_type = buf_page_get_io_fix(bpage);
  ut_ad(io_type == BUF_IO_READ || io_type == BUF_IO_WRITE);

  if (io_type == BUF_IO_READ) {

    auto frame = ((buf_block_t *)bpage)->frame;

    /* If this page is not uninitialized and not in the
    doublewrite buffer, then the page number and space id
    should be the same as in block. */
    auto read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
    auto read_space_id = mach_read_from_4(frame + FIL_PAGE_SPACE_ID);

    if (bpage->space == TRX_SYS_SPACE && trx_doublewrite_page_inside(bpage->offset)) {

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: reading page %lu\n"
        "which is in the"
        " doublewrite buffer!\n",
        (ulong)bpage->offset
      );
    } else if (!read_space_id && !read_page_no) {
      /* This is likely an uninitialized page. */
    } else if ((bpage->space && bpage->space != read_space_id) || bpage->offset != read_page_no) {
      /* We did not compare space_id to read_space_id
      if bpage->space == 0, because the field on the
      page may contain garbage in version < 4.1.1,
      which only supported bpage->space == 0. */

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: space id and page n:o"
        " stored in the page\n"
        "read in are %lu:%lu,"
        " should be %lu:%lu!\n",
        (ulong)read_space_id,
        (ulong)read_page_no,
        (ulong)bpage->space,
        (ulong)bpage->offset
      );
    }

    /* From version 3.23.38 up we store the page checksum
    to the 4 first bytes of the page end lsn field */

    if (buf_page_is_corrupted(frame)) {
      ib_logger(
        ib_stream,
        "Database page corruption on disk"
        " or a failed\n"
        "file read of page %lu.\n"
        "You may have to recover"
        " from a backup.\n",
        (ulong)bpage->offset
      );
      buf_page_print(frame, 0);
      ib_logger(
        ib_stream,
        "Database page corruption on disk"
        " or a failed\n"
        "file read of page %lu.\n"
        "You may have to recover"
        " from a backup.\n",
        (ulong)bpage->offset
      );
      ib_logger(
        ib_stream,
        "It is also possible that"
        " your operating\n"
        "system has corrupted its"
        " own file cache\n"
        "and rebooting your computer"
        " removes the\n"
        "error.\n"
        "If the corrupt page is an index page\n"
        "you can also try to"
        " fix the corruption\n"
        "by dumping, dropping,"
        " and reimporting\n"
        "the corrupt table."
        " You can use CHECK\n"
        "TABLE to scan your"
        " table for corruption.\n"
        "See also"
        " the InnoDB website for details\n"
        "about forcing recovery.\n"
      );

      if (srv_force_recovery < IB_RECOVERY_IGNORE_CORRUPT) {
        log_fatal("Ending processing because of a corrupt database page.");
      }
    }

    if (recv_recovery_on) {
      /* Pages must be uncompressed for crash recovery. */
      recv_recover_page(true, (buf_block_t *)bpage);
    }
  }

  buf_pool_mutex_enter();
  mutex_enter(buf_page_get_mutex(bpage));

  /* Because this thread which does the unlocking is not the same that
  did the locking, we use a pass value != 0 in unlock, which simply
  removes the newest lock debug record, without checking the thread
  id. */

  buf_page_set_io_fix(bpage, BUF_IO_NONE);

  switch (io_type) {
    case BUF_IO_READ:
      ut_ad(buf_pool->n_pend_reads > 0);

      --buf_pool->n_pend_reads;
      ++buf_pool->stat.n_pages_read;

      rw_lock_x_unlock_gen(&((buf_block_t *)bpage)->lock, BUF_IO_READ);

      break;

    case BUF_IO_WRITE:
      /* Write means a flush operation: call the completion
    routine in the flush system */

      buf_flush_write_complete(bpage);

      rw_lock_s_unlock_gen(&((buf_block_t *)bpage)->lock, BUF_IO_WRITE);

      ++buf_pool->stat.n_pages_written;

      break;

    default:
      ut_error;
  }

#ifdef UNIV_DEBUG
  if (buf_debug_prints) {
    ib_logger(
      ib_stream,
      "Has %s page space %lu page no %lu\n",
      io_type == BUF_IO_READ ? "read" : "written",
      (ulong)buf_page_get_space(bpage),
      (ulong)buf_page_get_page_no(bpage)
    );
  }
#endif /* UNIV_DEBUG */

  mutex_exit(buf_page_get_mutex(bpage));
  buf_pool_mutex_exit();
}

void buf_pool_invalidate() {
  buf_pool_mutex_enter();

  for (auto i = (ulint)BUF_FLUSH_LRU; i < (ulint)BUF_FLUSH_N_TYPES; i++) {

    /* As this function is called during startup and
    during redo application phase during recovery, InnoDB
    is single threaded (apart from IO helper threads) at
    this stage. No new write batch can be in intialization
    stage at this point. */
    ut_ad(buf_pool->init_flush[i] == false);

    /* However, it is possible that a write batch that has
    been posted earlier is still not complete. For buffer
    pool invalidation to proceed we must ensure there is NO
    write activity happening. */
    if (buf_pool->n_flush[i] > 0) {
      buf_pool_mutex_exit();
      buf_flush_wait_batch_end((buf_flush)i);
      buf_pool_mutex_enter();
    }
  }

  buf_pool_mutex_exit();

  ut_ad(buf_all_freed());

  while (buf_LRU_search_and_free_block(100)) {
    ;
  }

  buf_pool_mutex_enter();

  ut_ad(UT_LIST_GET_LEN(buf_pool->LRU) == 0);

  buf_pool->freed_page_clock = 0;
  buf_pool->LRU_old = nullptr;
  buf_pool->LRU_old_len = 0;
  buf_pool->LRU_flush_ended = 0;

  memset(&buf_pool->stat, 0x00, sizeof(buf_pool->stat));
  buf_refresh_io_stats();

  buf_pool_mutex_exit();
}

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
bool buf_validate() {
  buf_chunk_t *chunk;
  ulint n_single_flush = 0;
  ulint n_lru_flush = 0;
  ulint n_list_flush = 0;
  ulint n_lru = 0;
  ulint n_flush = 0;
  ulint n_free = 0;

  ut_ad(buf_pool);

  buf_pool_mutex_enter();

  chunk = buf_pool->chunks;

  /* Check the uncompressed blocks. */

  for (ulint i = buf_pool->n_chunks; i--; chunk++) {
    ulint j;
    buf_block_t *block = chunk->blocks;

    for (j = chunk->size; j--; block++) {

      mutex_enter(&block->mutex);

      switch (buf_block_get_state(block)) {
        case BUF_BLOCK_FILE_PAGE:
          ut_a(buf_page_hash_get(buf_block_get_space(block), buf_block_get_page_no(block)) == &block->page);

          switch (buf_page_get_io_fix(&block->page)) {
            case BUF_IO_NONE:
              break;

            case BUF_IO_WRITE:
              switch (buf_page_get_flush_type(&block->page)) {
                case BUF_FLUSH_LRU:
                  n_lru_flush++;
                  ut_a(rw_lock_is_locked(&block->lock, RW_LOCK_SHARED));
                  break;
                case BUF_FLUSH_LIST:
                  n_list_flush++;
                  break;
                case BUF_FLUSH_SINGLE_PAGE:
                  n_single_flush++;
                  break;
                default:
                  ut_error;
              }

              break;

            case BUF_IO_READ:

              ut_a(rw_lock_is_locked(&block->lock, RW_LOCK_EX));
              break;
          }

          n_lru++;

          if (block->page.oldest_modification > 0) {
            n_flush++;
          }

          break;

        case BUF_BLOCK_NOT_USED:
          n_free++;
          break;

        case BUF_BLOCK_READY_FOR_USE:
        case BUF_BLOCK_MEMORY:
        case BUF_BLOCK_REMOVE_HASH:
          /* do nothing */
          break;
      }

      mutex_exit(&block->mutex);
    }
  }

  if (n_lru + n_free > buf_pool->curr_size) {
    ib_logger(ib_stream, "n LRU %lu, n free %lu, pool %lu\n", (ulong)n_lru, (ulong)n_free, (ulong)buf_pool->curr_size);
    ut_error;
  }

  ut_a(UT_LIST_GET_LEN(buf_pool->LRU) == n_lru);

  if (UT_LIST_GET_LEN(buf_pool->free) != n_free) {

    ib_logger(ib_stream, "Free list len %lu, free blocks %lu\n", (ulong)UT_LIST_GET_LEN(buf_pool->free), (ulong)n_free);

    ut_error;
  }

  ut_a(UT_LIST_GET_LEN(buf_pool->flush_list) == n_flush);

  ut_a(buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE] == n_single_flush);
  ut_a(buf_pool->n_flush[BUF_FLUSH_LIST] == n_list_flush);
  ut_a(buf_pool->n_flush[BUF_FLUSH_LRU] == n_lru_flush);

  buf_pool_mutex_exit();

  ut_a(buf_LRU_validate());
  ut_a(buf_flush_validate());

  return (true);
}
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#if defined UNIV_DEBUG_PRINT || defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** Prints info of the buffer buf_pool data structure. */

void buf_print(void) {
  ulint size;
  ulint i;
  ulint j;
  uint64_t id;
  ulint n_found;
  buf_chunk_t *chunk;
  dict_index_t *index;

  ut_ad(buf_pool);

  size = buf_pool->curr_size;

  auto index_ids = static_cast<uint64_t *>(mem_alloc(sizeof(uint64_t) * size));
  auto counts = static_cast<ulint *>(mem_alloc(sizeof(ulint) * size));

  buf_pool_mutex_enter();

  ib_logger(
    ib_stream,
    "buf_pool size %lu\n"
    "database pages %lu\n"
    "free pages %lu\n"
    "modified database pages %lu\n"
    "n pending reads %lu\n"
    "n pending flush LRU %lu list %lu single page %lu\n"
    "pages made young %lu, not young %lu\n"
    "pages read %lu, created %lu, written %lu\n",
    (ulong)size,
    (ulong)UT_LIST_GET_LEN(buf_pool->LRU),
    (ulong)UT_LIST_GET_LEN(buf_pool->free),
    (ulong)UT_LIST_GET_LEN(buf_pool->flush_list),
    (ulong)buf_pool->n_pend_reads,
    (ulong)buf_pool->n_flush[BUF_FLUSH_LRU],
    (ulong)buf_pool->n_flush[BUF_FLUSH_LIST],
    (ulong)buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE],
    (ulong)buf_pool->stat.n_pages_made_young,
    (ulong)buf_pool->stat.n_pages_not_made_young,
    (ulong)buf_pool->stat.n_pages_read,
    (ulong)buf_pool->stat.n_pages_created,
    (ulong)buf_pool->stat.n_pages_written
  );

  /* Count the number of blocks belonging to each index in the buffer */

  n_found = 0;

  chunk = buf_pool->chunks;

  for (i = buf_pool->n_chunks; i--; chunk++) {
    buf_block_t *block = chunk->blocks;
    ulint n_blocks = chunk->size;

    for (; n_blocks--; block++) {
      const buf_frame_t *frame = block->frame;

      if (fil_page_get_type(frame) == FIL_PAGE_INDEX) {

        id = btr_page_get_index_id(frame);

        /* Look for the id in the index_ids array */
        j = 0;

        while (j < n_found) {

          if (index_ids[j] == id) {
            counts[j]++;

            break;
          }
          j++;
        }

        if (j == n_found) {
          n_found++;
          index_ids[j] = id;
          counts[j] = 1;
        }
      }
    }
  }

  buf_pool_mutex_exit();

  for (i = 0; i < n_found; i++) {
    index = dict_index_get_if_in_cache(index_ids[i]);

    ib_logger(ib_stream, "Block count for index %lu in buffer is about %lu", (ulong)index_ids[i], (ulong)counts[i]);

    if (index) {
      ib_logger(ib_stream, " ");
      dict_index_name_print(ib_stream, nullptr, index);
    }

    ib_logger(ib_stream, "\n");
  }

  mem_free(index_ids);
  mem_free(counts);

  ut_a(buf_validate());
}
#endif /* UNIV_DEBUG_PRINT || UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
ulint buf_get_latched_pages_number() {
  ulint fixed_pages_number{};

  buf_pool_mutex_enter();

  auto chunk = buf_pool->chunks;

  for (ulint i = buf_pool->n_chunks; i--; chunk++) {

    auto block = chunk->blocks;

    for (ulint j = chunk->size; j--; block++) {
      if (buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE) {

        continue;
      }

      mutex_enter(&block->mutex);

      if (block->page.buf_fix_count != 0 || buf_page_get_io_fix(&block->page) != BUF_IO_NONE) {
        ++fixed_pages_number;
      }

      mutex_exit(&block->mutex);
    }
  }

  buf_pool_mutex_exit();

  return fixed_pages_number;
}
#endif /* UNIV_DEBUG */

ulint buf_get_n_pending_ios(void) {
  return (
    buf_pool->n_pend_reads + buf_pool->n_flush[BUF_FLUSH_LRU] + buf_pool->n_flush[BUF_FLUSH_LIST] +
    buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]
  );
}

ulint buf_get_modified_ratio_pct() {
  buf_pool_mutex_enter();

  auto ratio =
    (100 * UT_LIST_GET_LEN(buf_pool->flush_list)) / (1 + UT_LIST_GET_LEN(buf_pool->LRU) + UT_LIST_GET_LEN(buf_pool->free));

  /* 1 + is there to avoid division by zero */

  buf_pool_mutex_exit();

  return (ratio);
}

void buf_print_io(ib_stream_t ib_stream) {
  time_t current_time;
  double time_elapsed;
  ulint n_gets_diff;

  ut_ad(buf_pool);

  buf_pool_mutex_enter();

  log_info(
    "Buffer pool size     ",
    (ulong)buf_pool->curr_size,
    "\n",
    "Free buffers        ",
    (ulong)UT_LIST_GET_LEN(buf_pool->free),
    "\n",
    "Database pages      ",
    (ulong)buf_pool->LRU_old_len,
    "\n",
    "Old database pages  ",
    (ulong)UT_LIST_GET_LEN(buf_pool->LRU),
    "\n"
    "Modified db pages   ",
    (ulong)UT_LIST_GET_LEN(buf_pool->flush_list),
    "\n"
    "Pending reads       ",
    (ulong)buf_pool->n_pend_reads,
    "\n"
    "Pending writes: LRU ",
    (ulong)buf_pool->n_flush[BUF_FLUSH_LRU] + buf_pool->init_flush[BUF_FLUSH_LRU],
    ", flush list ",
    (ulong)buf_pool->n_flush[BUF_FLUSH_LIST] + buf_pool->init_flush[BUF_FLUSH_LIST],
    ", single page ",
    (ulong)buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]
  );

  current_time = time(nullptr);
  time_elapsed = 0.001 + difftime(current_time, buf_pool->last_printout_time);

  log_info(
    ib_stream,
    "Pages made young ",
    (ulong)buf_pool->stat.n_pages_made_young,
    ", not young ",
    (ulong)buf_pool->stat.n_pages_not_made_young,
    "\n",
    (buf_pool->stat.n_pages_made_young - buf_pool->old_stat.n_pages_made_young) / time_elapsed,
    " youngs/s, ",
    (buf_pool->stat.n_pages_not_made_young - buf_pool->old_stat.n_pages_not_made_young) / time_elapsed,
    " non-youngs/s\n",
    "Pages read ",
    (ulong)buf_pool->stat.n_pages_read,
    ",",
    " created",
    (ulong)buf_pool->stat.n_pages_created,
    ","
    " written ",
    (ulong)buf_pool->stat.n_pages_written,
    "\n",
    (buf_pool->stat.n_pages_read - buf_pool->old_stat.n_pages_read) / time_elapsed,
    " reads/s, ",
    (buf_pool->stat.n_pages_created - buf_pool->old_stat.n_pages_created) / time_elapsed,
    " creates/s, ",
    (buf_pool->stat.n_pages_written - buf_pool->old_stat.n_pages_written) / time_elapsed,
    " writes/s"
  );

  n_gets_diff = buf_pool->stat.n_page_gets - buf_pool->old_stat.n_page_gets;

  if (n_gets_diff) {
    log_info(
      "Buffer pool hit rate ",
      (ulong)(1000 - ((1000 * (buf_pool->stat.n_pages_read - buf_pool->old_stat.n_pages_read)) /
                      (buf_pool->stat.n_page_gets - buf_pool->old_stat.n_page_gets))),
      "/ 1000,",
      " young-making rate ",
      (ulong)(1000 * (buf_pool->stat.n_pages_made_young - buf_pool->old_stat.n_pages_made_young) / n_gets_diff),
      "/ 1000",
      " not ",
      (ulong)(1000 * (buf_pool->stat.n_pages_not_made_young - buf_pool->old_stat.n_pages_not_made_young) / n_gets_diff),
      "/ 1000"
    );
  } else {
    log_info("No buffer pool page gets since the last printout");
  }

  /* Statistics about read ahead algorithm */
  log_info(
    "Pages read ahead ",
    (buf_pool->stat.n_ra_pages_read - buf_pool->old_stat.n_ra_pages_read) / time_elapsed,
    "/s",
    " evicted without access ",
    (buf_pool->stat.n_ra_pages_evicted - buf_pool->old_stat.n_ra_pages_evicted) / time_elapsed,
    "/s"
  );

  /* Print some values to help us with visualizing what is
  happening with LRU eviction. */
  log_info("LRU len: ", UT_LIST_GET_LEN(buf_pool->LRU), "I/O sum[", buf_LRU_stat_sum.io, "], ", "cur[", buf_LRU_stat_cur.io, "]");

  buf_refresh_io_stats();
  buf_pool_mutex_exit();
}

void buf_refresh_io_stats() {
  buf_pool->last_printout_time = time(nullptr);
  buf_pool->old_stat = buf_pool->stat;
}

bool buf_all_freed() {
  buf_pool_mutex_enter();

  auto chunk = buf_pool->chunks;

  for (ulint i = buf_pool->n_chunks; i--; chunk++) {

    const auto block = buf_chunk_not_freed(chunk);

    if (block != nullptr) {
      ib_logger(ib_stream, "Page %lu %lu still fixed or dirty\n", (ulong)block->page.space, (ulong)block->page.offset);
      ut_error;
    }
  }

  buf_pool_mutex_exit();

  return true;
}

bool buf_pool_check_no_pending_io() {
  bool ret;

  buf_pool_mutex_enter();

  if (buf_pool->n_pend_reads + buf_pool->n_flush[BUF_FLUSH_LRU] + buf_pool->n_flush[BUF_FLUSH_LIST] + buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]) {
    ret = false;
  } else {
    ret = true;
  }

  buf_pool_mutex_exit();

  return ret;
}

ulint buf_get_free_list_len() {
  buf_pool_mutex_enter();

  auto len = UT_LIST_GET_LEN(buf_pool->free);

  buf_pool_mutex_exit();

  return len;
}
