/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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
The database buffer srv_buf_pool

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
The buffer srv_buf_pool contains a single mutex which protects all the
control data structures of the srv_buf_pool. The content of a buffer frame is
protected by a separate read-write lock in its control block, though.
These locks can be locked and unlocked without owning the srv_buf_pool mutex.
The OS events in the srv_buf_pool struct can be waited for without owning the
srv_buf_pool mutex.

The srv_buf_pool mutex is a hot-spot in main memory, causing a lot of
memory bus traffic on multiprocessor systems when processors
alternately access the mutex. On our Pentium, the mutex is accessed
maybe every 10 microseconds. We gave up the solution to have mutexes
for each control block, for instance, because it seemed to be
complicated.

A solution to reduce mutex contention of the srv_buf_pool mutex is to
create a separate mutex for the page hash table. On Pentium,
accessing the hash table takes 2 microseconds, about half
of the total srv_buf_pool mutex hold time.

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

We intend to make the buffer srv_buf_pool size on-line reconfigurable,
that is, the srv_buf_pool size can be changed without closing the database.
Then the database administarator may adjust it to be bigger
at night, for example. The control block array must
contain enough control blocks for the maximum buffer srv_buf_pool size
which is used in the particular database.
If the srv_buf_pool size is cut, we exploit the virtual memory mechanism of
the OS, and just refrain from using frames at high addresses. Then the OS
can swap them to disk.

The control blocks containing file pages are put to a hash table
according to the file address of the page.
We could speed up the access to an individual page by using
"pointer swizzling": we could replace the page references on
non-leaf index pages by direct pointers to the page, if it exists
in the srv_buf_pool. We could make a separate hash table where we could
chain all the page references in non-leaf pages residing in the srv_buf_pool,
using the page reference as the hash key,
and at the time of reading of a page update the pointers accordingly.
Drawbacks of this solution are added complexity and,
possibly, extra space required on non-leaf pages for memory pointers.
A simpler solution is just to speed up the hash table mechanism
in the database, using tables whose size is a power of 2.

                Lists of blocks
                ---------------

There are several lists of control blocks.

The free list (Buf_pool::m_free_list) contains blocks which are currently not
used.

The common LRU list contains all the blocks holding a file page
except those for which the bufferfix count is non-zero.
The pages are in the LRU list roughly in the order of the last
access to the page, so that the oldest pages are at the end of the
list. We also keep a pointer to near the end of the LRU list,
which we can use when we want to artificially age a page in the
srv_buf_pool. This is used if we know that some page is not needed
again for some time: we insert the block right after the pointer,
causing it to be replaced sooner than would noramlly be the case.
Currently this aging mechanism is used for read-ahead mechanism
of pages, and it can also be used when there is a scan of a full
table which cannot fit in the memory. Putting the pages near the
of the LRU list, we make sure that most of the srv_buf_pool stays in the
main memory, undisturbed.

The chain of modified blocks (Buf_pool::m_flush_list) contains the blocks
holding file pages that have been modified in the memory
but not written to disk yet. The block with the oldest modification
which has not yet been written to disk is at the end of the chain.

                Loading a file page
                -------------------

First, a victim block for replacement has to be found in the
srv_buf_pool. It is taken from the free list or searched for from the
end of the LRU-list. An exclusive lock is reserved for the frame,
the io_fix field is set in the block fixing the block in srv_buf_pool,
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
When a read page is the first time referenced in the srv_buf_pool,
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
If a new page is referenced in the srv_buf_pool, and several pages
of its random access area (for instance, 32 consecutive pages
in a tablespace) have recently been referenced, we may predict
that the whole area may be needed in the near future, and issue
the read requests for the whole area.
*/

/** Value in microseconds */
static constexpr int WAIT_FOR_READ = 5000;

/** Number of attemtps made to read in a page in the buffer pool */
static constexpr ulint BUF_PAGE_READ_MAX_RETRIES = 100;

/** The buffer srv_buf_pool of the database */
Buf_pool *srv_buf_pool = nullptr;

/** Checksum function. */
crc32::Checksum crc32::checksum = {};

/** mutex protecting the buffer pool struct and control blocks, except the
read-write lock in them */
mutex_t buf_pool_mutex{};

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** This is used to insert validation operations in excution in the debug version */
static ulint buf_dbg_counter = 0;

/** Flag to forbid the release of the buffer pool mutex.
Protected by buf_pool_mutex. */
ulint buf_pool_mutex_exit_forbidden = 0;

#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

/** A chunk of buffers.  The buffer pool is allocated in chunks. */
struct buf_chunk_t {
  /** Allocated size of the chunk */
  ulint mem_size{};

  /** Size of frames[] and blocks[] */
  ulint size{};

  /** Pointer to the memory area which was allocated for the frames */
  void *mem{};

  /** Array of buffer control blocks */
  buf_block_t *blocks{};
};

bool Buf_pool::peek_if_too_old(const buf_page_t *bpage) {
  if (unlikely(m_freed_page_clock == 0)) {
    /* If eviction has not started yet, do not update the statistics or move blocks
    in the LRU list.  This is either the warm-up phase or an in-memory workload. */
    return false;
  } else if (m_LRU->get_old_threshold_ms() > 0 && bpage->m_old) {
    auto access_time = buf_page_is_accessed(bpage);

    if (access_time > 0 && ((uint32_t)(ut_time_ms() - access_time)) >= m_LRU->get_old_threshold_ms()) {
      return true;
    }

    m_stat.n_pages_not_made_young++;

    return false;

  } else {
    /* FIXME: bpage->m_freed_page_clock is 31 bits */
    return (m_freed_page_clock & ((1UL << 31) - 1)) >
           ((ulint)bpage->m_freed_page_clock +
            (m_curr_size * (Buf_LRU::OLD_RATIO_DIV - m_LRU->get_old_ratio()) / (Buf_LRU::OLD_RATIO_DIV * 4)));
  }
}

buf_block_t *Buf_pool::block_alloc() {
  auto block = m_LRU->get_free_block();

  buf_block_set_state(block, BUF_BLOCK_MEMORY);

  return block;
}

void Buf_pool::block_free(buf_block_t *block) {
  buf_pool_mutex_enter();

  mutex_enter(&block->m_mutex);

  ut_a(block->get_state() != BUF_BLOCK_FILE_PAGE);

  m_LRU->block_free_non_file_page(block);

  mutex_exit(&block->m_mutex);

  buf_pool_mutex_exit();
}

void Buf_pool::release(buf_block_t *block, ulint rw_latch, mtr_t *mtr) {
  ut_a(block->get_state() == BUF_BLOCK_FILE_PAGE);
  ut_a(block->m_page.m_buf_fix_count > 0);

  if (rw_latch == RW_X_LATCH && mtr->m_modifications) {
    buf_pool_mutex_enter();

    m_flusher->note_modification(block, mtr);

    buf_pool_mutex_exit();
  }

  mutex_enter(&block->m_mutex);

  IF_SYNC_DEBUG(rw_lock_s_unlock(&(block->m_debug_latch));)

  --block->m_page.m_buf_fix_count;

  mutex_exit(&block->m_mutex);

  if (rw_latch == RW_S_LATCH) {
    rw_lock_s_unlock(&(block->m_rw_lock));
  } else if (rw_latch == RW_X_LATCH) {
    rw_lock_x_unlock(&(block->m_rw_lock));
  }
}

void Buf_pool::init() {
  ut_d(buf_dbg_counter = 0);
  ut_d(buf_pool_mutex_exit_forbidden = 0);
}

bool Buf_pool::is_corrupted(const byte *read_buf) {
  if (memcmp(read_buf + FIL_PAGE_LSN + 4, read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4, 4)) {

    /* Stored log sequence numbers at the start and the end
    of page do not match */

    return true;
  }

  if (recv_lsn_checks_on) {
    lsn_t current_lsn;

    if (log_sys->peek_lsn(&current_lsn) && current_lsn < mach_read_from_8(read_buf + FIL_PAGE_LSN)) {
      log_err(std::format(
        "Page {}::{} log sequence number {} is in the future! Current system"
        " log sequence number is {}. Your database may be corrupt or you may have copied"
        " the InnoDB tablespace but not the InnoDB log files.",
        mach_read_from_4(read_buf + FIL_PAGE_SPACE_ID),
        mach_read_from_4(read_buf + FIL_PAGE_OFFSET),
        mach_read_from_8(read_buf + FIL_PAGE_LSN),
        current_lsn
      ));
    }
  }

  /* If we use checksums validation, make additional check before
  returning true to ensure that the checksum is not equal to
  BUF_NO_CHECKSUM_MAGIC which might be stored by InnoDB with checksums
  disabled. Otherwise, skip checksum calculation and return false */

  if (likely(srv_use_checksums)) {
    auto checksum = mach_read_from_4(read_buf + FIL_PAGE_SPACE_OR_CHKSUM);

    if (checksum != 0 &&
        checksum != BUF_NO_CHECKSUM_MAGIC &&
	      checksum != buf_page_data_calc_checksum(read_buf)) {

      return true;
    }
  }

  return false;
}

void buf_page_print(const byte *read_buf, ulint) {
  dict_index_t *index;
  auto size = UNIV_PAGE_SIZE;

  ib_logger(ib_stream, "  Page dump in ascii and hex (%lu bytes):\n", (ulong)size);
  ib_logger(ib_stream, "\nEnd of page dump\n");

  auto checksum =  buf_page_data_calc_checksum(read_buf);

  ib_logger(
    ib_stream,
    " Checksum stored on page %lu, lsn %lu, %lu space id %lu, page number %lu",
    (ulong)checksum,
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_SPACE_OR_CHKSUM),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_LSN) | (ulong)mach_read_from_4(read_buf + FIL_PAGE_LSN + 4),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_SPACE_ID),
    (ulong)mach_read_from_4(read_buf + FIL_PAGE_OFFSET)
  );

  if (mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT) {
    ib_logger(ib_stream, "Page may be an insert undo log page\n");
  } else if (mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE) {
    ib_logger(ib_stream, "Page may be an update undo log page\n");
  }

  switch (srv_fil->page_get_type(read_buf)) {
    case FIL_PAGE_TYPE_INDEX:
      log_warn(std::format(
        "Page may be an index page where index id is {}",
        btr_page_get_index_id(read_buf)
      ));

      index = dict_index_find_on_id_low(btr_page_get_index_id(read_buf));

      if (index != nullptr) {
        ib_logger(ib_stream, "(");
        dict_index_name_print(ib_stream, nullptr, index);
        ib_logger(ib_stream, ")\n");
      }
      break;
    case FIL_PAGE_TYPE_INODE:
      log_warn("Page may be an 'inode' page");
      break;
    case FIL_PAGE_TYPE_ALLOCATED:
      log_warn("Page may be a freshly allocated page");
      break;
    case FIL_PAGE_TYPE_SYS:
      log_warn("Page may be a system page");
      break;
    case FIL_PAGE_TYPE_TRX_SYS:
      log_warn("Page may be a transaction system page");
      break;
    case FIL_PAGE_TYPE_FSP_HDR:
      log_warn("Page may be a file space header page");
      break;
    case FIL_PAGE_TYPE_XDES:
      log_warn("Page may be an extent descriptor page");
      break;
    case FIL_PAGE_TYPE_BLOB:
      log_warn("Page may be a BLOB page");
      break;
    case FIL_PAGE_TYPE_UNDO_LOG:
      log_warn("Page may be an undo log page");
      break;
  }
}

void Buf_pool::block_init(buf_block_t *block, byte *frame) {
  UNIV_MEM_DESC(frame, UNIV_PAGE_SIZE, block);

  block->m_frame = frame;

  block->m_page.m_state = BUF_BLOCK_NOT_USED;
  block->m_page.m_buf_fix_count = 0;
  block->m_page.m_io_fix = BUF_IO_NONE;

  block->m_modify_clock = 0;

  ut_d(block->m_page.m_file_page_was_freed = false);

  block->m_check_index_page_at_flush = false;

  ut_d(block->m_page.m_in_page_hash = false);
  ut_d(block->m_page.m_in_flush_list = false);
  ut_d(block->m_page.m_in_free_list = false);
  ut_d(block->m_page.m_in_LRU_list = false);

  mutex_create(&block->m_mutex, IF_DEBUG("block_mutex",) IF_SYNC_DEBUG(SYNC_BUF_BLOCK,) Source_location{});

  rw_lock_create(&block->m_rw_lock, SYNC_LEVEL_VARYING);
  ut_ad(rw_lock_validate(&(block->m_rw_lock)));

#ifdef UNIV_SYNC_DEBUG
  rw_lock_create(&block->m_debug_latch, SYNC_NO_ORDER_CHECK);
#endif /* UNIV_SYNC_DEBUG */
}

buf_chunk_t *Buf_pool::chunk_init(buf_chunk_t *chunk, ulint mem_size) {

  /* Round down to a multiple of page size, although it already should be. */
  mem_size = ut_2pow_round(mem_size, UNIV_PAGE_SIZE);

  /* Reserve space for the block descriptors. */
  mem_size += ut_2pow_round((mem_size / UNIV_PAGE_SIZE) * sizeof(buf_block_t) + (UNIV_PAGE_SIZE - 1), UNIV_PAGE_SIZE);

  chunk->mem_size = mem_size;
  chunk->mem = os_mem_alloc_large(&chunk->mem_size);

  if (unlikely(chunk->mem == nullptr)) {

    return nullptr;
  }

  /* Allocate the block descriptors from the start of the memory block. */
  chunk->blocks = (buf_block_t *)chunk->mem;

  /* Align a pointer to the first frame.  Note that when os_large_page_size is
  smaller than UNIV_PAGE_SIZE, we may allocate one fewer block than requested.
  When it is bigger, we may allocate more blocks than requested. */

  auto frame = (byte *)ut_align((byte *)chunk->mem, UNIV_PAGE_SIZE);

  chunk->size = chunk->mem_size / UNIV_PAGE_SIZE - (frame != chunk->mem);

  /* Subtract the space needed for block descriptors. */
  {
    auto size = chunk->size;

    while (frame < (byte *)(chunk->blocks + size)) {
      frame += UNIV_PAGE_SIZE;
      size--;
    }

    chunk->size = size;
  }

  /* Init block structs and assign frames for them. Then we assign the frames
  to the first blocks (we already mapped the memory above). */

  auto block = chunk->blocks;

  for (ulint i = chunk->size; i--;) {

    block_init(block, frame);

    /* Add the block to the free list */
    UT_LIST_ADD_LAST(m_free_list, &block->m_page);
    ut_d(block->m_page.m_in_free_list = true);

    ++block;

    frame += UNIV_PAGE_SIZE;
  }

  return chunk;
}

const buf_block_t *Buf_pool::chunk_not_freed(buf_chunk_t *chunk) {
  ut_ad(buf_pool_mutex_own());

  auto block = chunk->blocks;

  for (ulint i = chunk->size; i--; block++) {
    switch (block->get_state()) {
      case BUF_BLOCK_NOT_USED:
      case BUF_BLOCK_READY_FOR_USE:
      case BUF_BLOCK_MEMORY:
      case BUF_BLOCK_REMOVE_HASH:
        /* Skip blocks that are not being used for file pages. */
        break;
      case BUF_BLOCK_FILE_PAGE: {
        mutex_enter(&block->m_mutex);
        auto ready = m_flusher->ready_for_replace(&block->m_page);
        mutex_exit(&block->m_mutex);

        if (!ready) {

          return block;
        }
      }

      break;
    }
  }

  return nullptr;
}

Buf_pool::Buf_pool()
  : m_LRU(new (std::nothrow) Buf_LRU()),
    m_flusher(new (std::nothrow) Buf_flush()) {}

bool Buf_pool::open(uint64_t pool_size) {

  if (m_LRU == nullptr || m_flusher == nullptr) {
    return false;
  }

  /* 1. Initialize general fields
  ------------------------------- */
  mutex_create(&buf_pool_mutex, IF_DEBUG("buffer_pool",) IF_SYNC_DEBUG(SYNC_BUF_POOL,) Source_location{});

  buf_pool_mutex_enter();

  auto chunk = reinterpret_cast<buf_chunk_t *>(mem_alloc(sizeof(buf_chunk_t)));

  m_n_chunks = 1;
  m_chunks = chunk;

  UT_LIST_INIT(m_LRU_list);
  UT_LIST_INIT(m_free_list);
  UT_LIST_INIT(m_flush_list);

  if (!chunk_init(chunk, pool_size)) {
    mem_free(chunk);
    buf_pool_mutex_exit();
    return false;
  }

  srv_buf_pool_old_size = pool_size;

  m_curr_size = chunk->size;

  srv_buf_pool_curr_size = m_curr_size * UNIV_PAGE_SIZE;

  m_page_hash = new page_hash_t{};

  m_last_printout_time = ut_time();

  /* 2. Initialize flushing fields */

  for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    m_no_flush[i] = os_event_create(nullptr);
  }

  /* 3. Initialize LRU fields */
  /* All fields are initialized by mem_zalloc(). */

  /* Initialize red-black tree for fast insertions into the flush_list
  during recovery process.  As this initialization is done while holding
  the buffer pool mutex we perform it before acquiring recv_sys->mutex. */

  m_flusher->init_flush_rbt();

  buf_pool_mutex_exit();

  /* 4. Initialize the buddy allocator fields */
  /* All fields are initialized by mem_zalloc(). */

  crc32::checksum = crc32::init();

  return true;
}

void Buf_pool::close() {
  delete m_page_hash;

  for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    os_event_free(m_no_flush[i]);
    m_no_flush[i] = nullptr;
  }
}

Buf_pool::~Buf_pool() {
  auto chunks = m_chunks;
  auto chunk = chunks + m_n_chunks;

  while (--chunk >= chunks) {
    /* Bypass the checks of buf_chunk_free(), since they fail at shutdown. */
    os_mem_free_large(chunk->mem, chunk->mem_size);
  }

  m_n_chunks = 0;

  mem_free(m_chunks);
}


void Buf_pool::make_young(buf_page_t *bpage) {
  buf_pool_mutex_enter();

  ut_a(bpage->in_file());

  m_LRU->make_block_young(bpage);

  buf_pool_mutex_exit();
}

void Buf_pool::set_accessed_make_young(buf_page_t *bpage, unsigned access_time) {
  ut_ad(!buf_pool_mutex_own());
  ut_a(bpage->in_file());

  if (peek_if_too_old(bpage)) {
    buf_pool_mutex_enter();

    m_LRU->make_block_young(bpage);

    buf_pool_mutex_exit();
  } else if (access_time == 0) {

    const ulint time_ms = ut_time_ms();

    buf_pool_mutex_enter();

    buf_page_set_accessed(bpage, time_ms);

    buf_pool_mutex_exit();
  }
}

void Buf_pool::check_index_page_at_flush(space_id_t space, page_no_t page_no) {
  buf_pool_mutex_enter();

  auto block = hash_get_block(space, page_no);

  if (block != nullptr && block->get_state() == BUF_BLOCK_FILE_PAGE) {
    block->m_check_index_page_at_flush = false;
  }

  buf_pool_mutex_exit();
}

buf_block_t *Buf_pool::block_align(const byte *ptr) {
  ulint i = m_n_chunks;;

  /* TODO: protect Buf_pool::m_chunks with a mutex (it will
  currently remain constant after Buf_pool::open()) */
  for (auto chunk = m_chunks; i--; ++chunk) {
    lint offs = ptr - chunk->blocks->m_frame;

    if (unlikely(offs < 0)) {

      continue;
    }

    offs >>= UNIV_PAGE_SIZE_SHIFT;

    if (likely((ulint)offs < chunk->size)) {
      auto block = &chunk->blocks[offs];

      /* The function buf_chunk_init() invokes block_init() so that
      block[n].frame == block->frame + n * UNIV_PAGE_SIZE.  Check it. */
      ut_ad(block->m_frame == page_align(ptr));

#ifdef UNIV_DEBUG
      /* A thread that updates these fields must hold buf_pool_mutex and
      block->mutex.  Acquire only the latter. */
      mutex_enter(&block->m_mutex);

      switch (block->get_state()) {
        case BUF_BLOCK_NOT_USED:
        case BUF_BLOCK_READY_FOR_USE:
        case BUF_BLOCK_MEMORY:
          /* Some data structures contain "guess" pointers to file pages.  The
        file pages may have been freed and reused.  Do not complain. */
          break;
        case BUF_BLOCK_REMOVE_HASH:
          /* Buf_pool::m_LRU->block_remove_hashed_page() will overwrite the FIL_PAGE_OFFSET and
          FIL_PAGE_SPACE_ID with 0xff and set the state to BUF_BLOCK_REMOVE_HASH. */
          ut_ad(page_get_space_id(page_align(ptr)) == 0xffffffff);
          ut_ad(page_get_page_no(page_align(ptr)) == 0xffffffff);
          break;
        case BUF_BLOCK_FILE_PAGE:
          ut_ad(block->get_space() == page_get_space_id(page_align(ptr)));
          ut_ad(block->get_page_no() == page_get_page_no(page_align(ptr)));
          break;
      }

      mutex_exit(&block->m_mutex);
#endif /* UNIV_DEBUG */

      return block;
    }
  }

  /* The block should always be found. */
  ut_error;
  return nullptr;
}

bool Buf_pool::pointer_is_block_field(const void *ptr) {
  auto chunk = m_chunks;
  const auto chunk_end = chunk + m_n_chunks;

  /* TODO: protect Buf_pool::m_chunks with a mutex (it will
  currently remain constant after Buf_pool::open()) */
  while (chunk < chunk_end) {
    if (ptr >= (void *)chunk->blocks && ptr < (void *)(chunk->blocks + chunk->size)) {

      return true;
    }

    ++chunk;
  }

  return false;
}

buf_block_t *Buf_pool::get(Request &req, buf_block_t *guess) {
  ulint n_retries{};
  buf_block_t *block{};
  const auto &page_id{req.m_page_id};

  ut_ad(req.m_mtr != nullptr);
  ut_ad(req.m_mtr->m_state == MTR_ACTIVE);
  ut_ad(req.m_rw_latch == RW_S_LATCH || req.m_rw_latch == RW_X_LATCH || req.m_rw_latch == RW_NO_LATCH);
  ut_ad(req.m_mode != BUF_GET_NO_LATCH || req.m_rw_latch == RW_NO_LATCH);
  ut_ad(req.m_mode == BUF_GET || req.m_mode == BUF_GET_IF_IN_POOL || req.m_mode == BUF_GET_NO_LATCH);

  ++m_stat.n_page_gets;

  mtr_memo_type_t fix_type;

  for (;;) {
    buf_pool_mutex_enter();

    block = guess;

    if (block != nullptr) {
      if (page_id.m_page_no != block->m_page.m_page_no ||
	        page_id.m_space_id != block->m_page.m_space ||
	        block->get_state() != BUF_BLOCK_FILE_PAGE) {

        block = guess = nullptr;

      } else {
        ut_ad(block->m_page.m_in_page_hash);
      }
    }

    if (block == nullptr) {
      block = hash_get_block(page_id.m_space_id, page_id.m_page_no);
    }

    if (block == nullptr) {
      buf_pool_mutex_exit();

      if (req.m_mode == BUF_GET_IF_IN_POOL) {
        return nullptr;
      }

      if (buf_read_page(page_id.m_space_id, page_id.m_page_no)) {

        n_retries = 0;

      } else if (n_retries < BUF_PAGE_READ_MAX_RETRIES) {

        ++n_retries;

      } else {

        ib_logger(
          ib_stream,
          "Error: Unable to read tablespace %lu page no %lu into the buffer pool after"
          " %lu attempts. The most probable cause of this error may be that the table"
          " has been corrupted. You can try to fix this problem by using innodb_force_recovery."
          " Please see reference manual for more details. Aborting...",
          page_id.m_space_id, page_id.m_page_no, BUF_PAGE_READ_MAX_RETRIES
        );

        ut_error;
      }

      ut_ad(++buf_dbg_counter % 37 || validate());

    } else {
      break;
    }
  }

  auto must_read = buf_block_get_io_fix(block) == BUF_IO_READ;

  if (must_read && req.m_mode == BUF_GET_IF_IN_POOL) {
    /* The page is only being read to buffer */
    buf_pool_mutex_exit();

    return nullptr;
  }

  switch (block->get_state()) {
    case BUF_BLOCK_FILE_PAGE:
      break;

    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
      ut_error;
      break;
  }

  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);

  mutex_enter(&block->m_mutex);

  UNIV_MEM_ASSERT_RW(&block->m_page, sizeof(block->m_page));

  buf_block_buf_fix_inc(block, req.m_file, req.m_line);

  mutex_exit(&block->m_mutex);

  /* Check if this is the first access to the page */
  auto access_time = buf_page_is_accessed(&block->m_page);

  buf_pool_mutex_exit();

  set_accessed_make_young(&block->m_page, access_time);

  ut_ad(!block->m_page.m_file_page_was_freed);

  ut_ad(++buf_dbg_counter % 5771 || validate());
  ut_ad(block->m_page.m_buf_fix_count > 0);
  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);

  switch (req.m_rw_latch) {
    case RW_NO_LATCH:
      if (must_read) {
        // TODO: Use coroutines here. No need to waste time spinning.
        /* Let us wait until the read operation completes */

        for (;;) {
          mutex_enter(&block->m_mutex);
          auto io_fix = buf_block_get_io_fix(block);

          mutex_exit(&block->m_mutex);

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
      rw_lock_s_lock_func(&block->m_rw_lock, 0, req.m_file, req.m_line);

      fix_type = MTR_MEMO_PAGE_S_FIX;
      break;

    default:
      ut_ad(req.m_rw_latch == RW_X_LATCH);
      rw_lock_x_lock_func(&block->m_rw_lock, 0, req.m_file, req.m_line);

      fix_type = MTR_MEMO_PAGE_X_FIX;
      break;
  }

  req.m_mtr->memo_push(block, fix_type);

  if (access_time == 0) {
    /* In the case of a first access, try to apply linear read-ahead */

    buf_read_ahead_linear(page_id.m_space_id, page_id.m_page_no);
  }

  return block;
}

bool Buf_pool::try_get(Request& req) {
  ut_ad(req.m_guess != nullptr);
  ut_ad(req.m_mtr != nullptr);
  ut_ad(req.m_mtr->m_state == MTR_ACTIVE);
  ut_ad(req.m_rw_latch == RW_S_LATCH || req.m_rw_latch == RW_X_LATCH);

  mutex_enter(&req.m_guess->m_mutex);

  if (unlikely(req.m_guess->get_state() != BUF_BLOCK_FILE_PAGE)) {

    mutex_exit(&req.m_guess->m_mutex);

    return false;
  }

  buf_block_buf_fix_inc(req.m_guess, req.m_file, req.m_line);

  mutex_exit(&req.m_guess->m_mutex);

  /* Check if this is the first access to the page. We do a dirty read on
  purpose, to avoid mutex contention. This field is only used for heuristic
  purposes; it does not affect correctness. */

  const auto access_time = buf_page_is_accessed(&req.m_guess->m_page);

  set_accessed_make_young(&req.m_guess->m_page, access_time);

  bool success;
  mtr_memo_type_t fix_type;

  /* The "try" part. */
  if (req.m_rw_latch == RW_S_LATCH) {

    success = rw_lock_s_lock_nowait(&req.m_guess->m_rw_lock, req.m_file, req.m_line);
    fix_type = MTR_MEMO_PAGE_S_FIX;

  } else {

    ut_a(req.m_rw_latch == RW_X_LATCH);

    success = rw_lock_x_lock_func_nowait(&req.m_guess->m_rw_lock, req.m_file, req.m_line);
    fix_type = MTR_MEMO_PAGE_X_FIX;
  }

  if (unlikely(!success)) {

    mutex_enter(&req.m_guess->m_mutex);

    req.m_guess->fix_dec();

    mutex_exit(&req.m_guess->m_mutex);

    return false;

  } else  if (unlikely(req.m_modify_clock != req.m_guess->m_modify_clock)) {

    buf_block_dbg_add_level(IF_SYNC_DEBUG(lock, SYNC_NO_ORDER_CHECK));

    if (req.m_rw_latch == RW_S_LATCH) {

      rw_lock_s_unlock(&req.m_guess->m_rw_lock);

    } else {

      rw_lock_x_unlock(&req.m_guess->m_rw_lock);

    }

    mutex_enter(&req.m_guess->m_mutex);

    req.m_guess->fix_dec();

    mutex_exit(&req.m_guess->m_mutex);

    return false;

  } else {

    req.m_mtr->memo_push(req.m_guess, fix_type);

    ut_ad(++buf_dbg_counter % 5771 || validate());
    ut_ad(req.m_guess->m_page.m_buf_fix_count > 0);
    ut_ad(req.m_guess->get_state() == BUF_BLOCK_FILE_PAGE);
    ut_ad(!req.m_guess->m_page.m_file_page_was_freed);

    if (unlikely(!access_time)) {

      /* In the case of a first access, try to apply linear read-ahead */
      buf_read_ahead_linear(req.m_guess->get_space(), req.m_guess->get_page_no());
    }

    ++m_stat.n_page_gets;

    return true;
  }
}

bool Buf_pool::try_get_known_nowait(Request& req) {
  ut_ad(req.m_mtr != nullptr);
  ut_ad(req.m_mtr->m_state == MTR_ACTIVE);
  ut_ad(req.m_rw_latch == RW_S_LATCH || req.m_rw_latch == RW_X_LATCH);

  mutex_enter(&req.m_guess->m_mutex);

  if (req.m_guess->get_state() == BUF_BLOCK_REMOVE_HASH) {
    /* Another thread is just freeing the block from the LRU list of the buffer
    pool: do not try to access this page; this attempt to access the page can
    only come through the hash index because when the buffer block state is
    ..._REMOVE_HASH, we have already removed it from the page address hash table
    of the buffer pool. */

    mutex_exit(&req.m_guess->m_mutex);

    return false;
  }

  ut_a(req.m_guess->get_state() == BUF_BLOCK_FILE_PAGE);

  buf_block_buf_fix_inc(req.m_guess, req.m_file, req.m_line);

  mutex_exit(&req.m_guess->m_mutex);

  if (req.m_mode == BUF_MAKE_YOUNG && peek_if_too_old(&req.m_guess->m_page)) {

    buf_pool_mutex_enter();

    m_LRU->make_block_young(&req.m_guess->m_page);

    buf_pool_mutex_exit();

  } else if (!buf_page_is_accessed(&req.m_guess->m_page)) {

    /* Above, we do a dirty read on purpose, to avoid mutex contention.
    The field buf_page_t::access_time is only used for heuristic purposes.
    Writes to the field must be protected by mutex, however. */

    const auto time_ms = ut_time_ms();

    buf_pool_mutex_enter();

    buf_page_set_accessed(&req.m_guess->m_page, time_ms);

    buf_pool_mutex_exit();
  }

  bool success;
  mtr_memo_type_t fix_type;

  /* This is the "nowait" part. */
  if (req.m_rw_latch == RW_S_LATCH) {
    success = rw_lock_s_lock_nowait(&req.m_guess->m_rw_lock, req.m_file, req.m_line);
    fix_type = MTR_MEMO_PAGE_S_FIX;
  } else {
    success = rw_lock_x_lock_func_nowait(&req.m_guess->m_rw_lock, req.m_file, req.m_line);
    fix_type = MTR_MEMO_PAGE_X_FIX;
  }

  if (!success) {

    /** Failed to acquire the latch. */
    mutex_enter(&req.m_guess->m_mutex);

    req.m_guess->fix_dec();

    mutex_exit(&req.m_guess->m_mutex);

    return false;

  } else {

    req.m_mtr->memo_push(req.m_guess, fix_type);

    ut_ad(++buf_dbg_counter % 5771 || validate());
    ut_ad(req.m_guess->m_page.m_buf_fix_count > 0);
    ut_ad(req.m_guess->get_state() == BUF_BLOCK_FILE_PAGE);
    ut_ad(req.m_guess->m_page.m_file_page_was_freed == false);

    ++m_stat.n_page_gets;

    return true;
  }
}

const buf_block_t *Buf_pool::try_get_by_page_id(Request& req) {
  ut_ad(req.m_mtr != nullptr);
  ut_ad(req.m_mtr->m_state == MTR_ACTIVE);

  const auto &page_id{req.m_page_id};

  buf_pool_mutex_enter();

  auto block = hash_get_block(page_id.m_space_id, page_id.m_page_no);

  if (block == nullptr) {

    buf_pool_mutex_exit();

    return nullptr;
  }

  mutex_enter(&block->m_mutex);

  buf_pool_mutex_exit();

  ut_ad(block->get_space() == page_id.m_space_id);
  ut_ad(block->get_page_no() == page_id.m_page_no);
  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);

  buf_block_buf_fix_inc(block, req.m_file, req.m_line);

  mutex_exit(&block->m_mutex);

  auto fix_type = MTR_MEMO_PAGE_S_FIX;
  auto success = rw_lock_s_lock_nowait(&block->m_rw_lock, req.m_file, req.m_line);

  if (!success) {

    /* Let us try to get an X-latch. If the current thread is holding an X-latch
    on the page, we cannot get an S-latch. */

    fix_type = MTR_MEMO_PAGE_X_FIX;
    success = rw_lock_x_lock_func_nowait(&block->m_rw_lock, req.m_file, req.m_line);

  }

  if (!success) {

    mutex_enter(&block->m_mutex);

    block->fix_dec();

    mutex_exit(&block->m_mutex);

    return nullptr;
  }

  req.m_mtr->memo_push(block, fix_type);

  ut_ad(++buf_dbg_counter % 5771 || validate());
  ut_ad(block->m_page.m_buf_fix_count > 0);
  ut_ad(block->get_state() == BUF_BLOCK_FILE_PAGE);
  ut_ad(block->m_page.m_file_page_was_freed == false);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  ++m_stat.n_page_gets;

  return block;
}

void Buf_pool::page_init_low(buf_page_t *bpage) {
  bpage->m_flush_type = BUF_FLUSH_LRU;
  bpage->m_io_fix = BUF_IO_NONE;
  bpage->m_buf_fix_count = 0;
  bpage->m_freed_page_clock = 0;
  bpage->m_access_time = 0;
  bpage->m_newest_modification = 0;
  bpage->m_oldest_modification = 0;
  HASH_INVALIDATE(bpage, hash);

  ut_d(bpage->m_file_page_was_freed = false);
}

void Buf_pool::page_init(space_id_t space, page_no_t page_no, buf_block_t *block) {
  ut_ad(buf_pool_mutex_own());
  ut_ad(mutex_own(&(block->m_mutex)));
  ut_a(block->get_state() != BUF_BLOCK_FILE_PAGE);

  /* Set the state of the block */
  buf_block_set_file_page(block, space, page_no);

#ifdef UNIV_DEBUG_VALGRIND
  if (space == 0) {
    /* Silence valid Valgrind warnings about uninitialized
    data being written to data files.  There are some unused
    bytes on some pages that InnoDB does not initialize. */
    UNIV_MEM_VALID(block->frame, UNIV_PAGE_SIZE);
  }
#endif /* UNIV_DEBUG_VALGRIND */

  block->m_check_index_page_at_flush = false;

  block->m_lock_hash_val = lock_rec_hash(space, page_no);

  /* Insert into the hash table of file pages */

  auto hash_page = hash_get_page(space, page_no);

  if (likely_null(hash_page)) {
    ib_logger(
      ib_stream,
      "Error: page %lu %lu already found in the hash table: %p, %p",
      (ulong)space,
      (ulong)page_no,
      (const void *)hash_page,
      (const void *)block
    );

    ut_d(mutex_exit(&block->m_mutex));
    ut_d(buf_pool_mutex_exit());
    ut_d(print());
    ut_d(m_LRU->print());
    ut_d(validate());
    ut_d(m_LRU->validate());

    ut_error;
  }

  page_init_low(&block->m_page);

  ut_ad(!block->m_page.m_in_page_hash);
  ut_d(block->m_page.m_in_page_hash = true);

  auto result = m_page_hash->emplace(Page_id(space, page_no), &block->m_page);
  ut_a(!result.second);
}

buf_page_t *Buf_pool::init_for_read(db_err *err, space_id_t space, page_no_t page_no, int64_t tablespace_version) {
  buf_page_t *bpage{};
  auto block = m_LRU->get_free_block();

  *err = DB_SUCCESS;

  buf_pool_mutex_enter();

  if (hash_get_page(space, page_no) != nullptr) {
    /* The page is already in the buffer pool. */
    if (block != nullptr) {

      mutex_enter(&block->m_mutex);

      m_LRU->block_free_non_file_page(block);

      mutex_exit(&block->m_mutex);
    }

  } else if (srv_fil->tablespace_deleted_or_being_deleted_in_mem(space, tablespace_version)) {

    /* The page belongs to a space which has been deleted or is being deleted. */
    *err = DB_TABLESPACE_DELETED;

    if (block != nullptr) {

      mutex_enter(&block->m_mutex);

      m_LRU->block_free_non_file_page(block);

      mutex_exit(&block->m_mutex);
    }

  } else if (block != nullptr) {

    bpage = &block->m_page;

    mutex_enter(&block->m_mutex);

    page_init(space, page_no, block);

    m_LRU->add_block(bpage, true /* to old blocks */);

    rw_lock_x_lock_gen(&block->m_rw_lock, BUF_IO_READ);

    buf_page_set_io_fix(bpage, BUF_IO_READ);

    mutex_exit(&block->m_mutex);

    ++m_n_pend_reads;
  }

  buf_pool_mutex_exit();

  ut_ad(bpage == nullptr || bpage->in_file());

  return bpage;
}

buf_block_t *Buf_pool::create(space_id_t space, page_no_t page_no, mtr_t *mtr) {
  auto time_ms = ut_time_ms();

  ut_ad(mtr != nullptr);
  ut_ad(mtr->m_state == MTR_ACTIVE);

  auto free_block = m_LRU->get_free_block();

  buf_pool_mutex_enter();

  auto block = hash_get_block(space, page_no);

  if (block != nullptr && block->m_page.in_file()) {
    ut_d(block->m_page.m_file_page_was_freed = false);

    /* Page can be found in srv_buf_pool */
    buf_pool_mutex_exit();

    block_free(free_block);

    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { space, page_no },
      .m_mode = BUF_GET_NO_LATCH,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    return get(req, nullptr);
  }

  block = free_block;

  mutex_enter(&block->m_mutex);

  page_init(space, page_no, block);

  /* The block must be put to the LRU list */
  m_LRU->add_block(&block->m_page, false);

  buf_block_buf_fix_inc(block, __FILE__, __LINE__);

  ++m_stat.n_pages_created;

  buf_page_set_accessed(&block->m_page, time_ms);

  buf_pool_mutex_exit();

  mtr->memo_push(block, MTR_MEMO_BUF_FIX);

  mutex_exit(&block->m_mutex);

  /* Delete possible entries for the page from the insert buffer:
  such can exist if the page belonged to an index which was dropped */

  /* Flush pages from the end of the LRU list if necessary */
  m_flusher->free_margin();

  auto frame = block->m_frame;

  memset(frame + FIL_PAGE_PREV, 0xff, 4);
  memset(frame + FIL_PAGE_NEXT, 0xff, 4);
  mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_ALLOCATED);

  /* Reset to zero the file flush lsn field in the page; if the first
  page of an ibdata file is 'created' in this function into the buffer
  pool then we lose the original contents of the file flush lsn stamp.
  Then InnoDB could in a crash recovery print a big, false, corruption
  warning if the stamp contains an lsn bigger than the ib_logfile lsn. */

  memset(frame + FIL_PAGE_FILE_FLUSH_LSN, 0, 8);

  ut_ad(++buf_dbg_counter % 357 || validate());

  return block;
}

void Buf_pool::io_complete(buf_page_t *bpage) {
  ut_a(bpage->in_file());

  /* We do not need protect io_fix here by mutex to read
  it because this is the only function where we can change the value
  from BUF_IO_READ or BUF_IO_WRITE to some other value, and our code
  ensures that this is the only thread that handles the i/o for this
  block. */

  const auto io_type = buf_page_get_io_fix(bpage);
  ut_ad(io_type == BUF_IO_READ || io_type == BUF_IO_WRITE);

  if (io_type == BUF_IO_READ) {

    auto frame = reinterpret_cast<buf_block_t *>(bpage)->m_frame;

    /* If this page is not uninitialized and not in the
    doublewrite buffer, then the page number and space id
    should be the same as in block. */
    auto read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
    auto read_space_id = mach_read_from_4(frame + FIL_PAGE_SPACE_ID);

    if (bpage->m_space == TRX_SYS_SPACE && trx_doublewrite_page_inside(bpage->m_page_no)) {

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        " Error: reading page %lu which is in the doublewrite buffer!",
        (ulong)bpage->m_page_no
      );
    } else if (read_space_id == 0 && read_page_no == 0) {
      /* This is likely an uninitialized page. */
    } else if ((bpage->m_space != 0 && bpage->m_space != read_space_id) || bpage->m_page_no != read_page_no) {
      /* We did not compare space_id to read_space_id if bpage-m_>space == 0, because the field on the
      page may contain garbage in version < 4.1.1, which only supported bpage->m_space == 0. */

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        " Error: space id and page n:o stored in the page read in are %lu:%lu, should be %lu:%lu!",
        (ulong)read_space_id,
        (ulong)read_page_no,
        (ulong)bpage->m_space,
        (ulong)bpage->m_page_no
      );
    }

    /* From version 3.23.38 up we store the page checksum
    to the 4 first bytes of the page end lsn field */

    if (is_corrupted(frame)) {
      ib_logger(
        ib_stream,
        "Database page corruption on disk or a failed file read of page %lu."
        " You may have to recover from a backup.",
        (ulong)bpage->m_page_no
      );

      buf_page_print(frame, 0);

      ib_logger(
        ib_stream,
        "Database page corruption on disk or a failed file read of page %lu."
        " You may have to recoverfrom a backup.",
        (ulong)bpage->m_page_no
      );
      ib_logger(
        ib_stream,
        "It is also possible that your operating system has corrupted its own file cache"
        " and rebooting your computer removes the error. If the corrupt page is an index page"
        " you can also try to fix the corruption by dumping, dropping, and reimporting"
        " the corrupt table. You can use CHECK TABLE to scan your table for corruption."
        " You can also use the force recovery flags."
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
      ut_ad(m_n_pend_reads > 0);

      --m_n_pend_reads;
      ++m_stat.n_pages_read;

      rw_lock_x_unlock_gen(&((buf_block_t *)bpage)->m_rw_lock, BUF_IO_READ);

      break;

    case BUF_IO_WRITE:
      /* Write means a flush operation: call the completion
    routine in the flush system */

      m_flusher->write_complete(bpage);

      rw_lock_s_unlock_gen(&((buf_block_t *)bpage)->m_rw_lock, BUF_IO_WRITE);

      ++m_stat.n_pages_written;

      break;

    default:
      ut_error;
  }

  mutex_exit(buf_page_get_mutex(bpage));
  buf_pool_mutex_exit();
}

void Buf_pool::invalidate() {
  buf_pool_mutex_enter();

  for (auto i = ulint(BUF_FLUSH_LRU); i < ulint(BUF_FLUSH_N_TYPES); ++i) {

    /* As this function is called during startup and
    during redo application phase during recovery, InnoDB
    is single threaded (apart from IO helper threads) at
    this stage. No new write batch can be in intialization
    stage at this point. */
    ut_ad(m_init_flush[i] == false);

    /* However, it is possible that a write batch that has
    been posted earlier is still not complete. For buffer
    pool invalidation to proceed we must ensure there is NO
    write activity happening. */
    if (m_n_flush[i] > 0) {
      buf_pool_mutex_exit();
      m_flusher->wait_batch_end((buf_flush)i);
      buf_pool_mutex_enter();
    }
  }

  buf_pool_mutex_exit();

  ut_ad(all_freed());

  while (m_LRU->search_and_free_block(100)) {
    ;
  }

  buf_pool_mutex_enter();

  ut_ad(UT_LIST_GET_LEN(m_LRU_list) == 0);

  m_freed_page_clock = 0;
  m_LRU_old = nullptr;
  m_LRU_old_len = 0;
  m_LRU_flush_ended = 0;

  m_stat = buf_pool_stat_t{};

  refresh_io_stats();

  buf_pool_mutex_exit();
}

#if defined UNIV_DEBUG
bool Buf_pool::validate() {
  ulint n_single_flush = 0;
  ulint n_LRU_flush = 0;
  ulint n_list_flush = 0;
  ulint n_lru = 0;
  ulint n_flush = 0;
  ulint n_free = 0;

  ut_ad(srv_buf_pool);

  buf_pool_mutex_enter();

  auto chunk = m_chunks;

  /* Check the uncompressed blocks. */

  for (ulint i = m_n_chunks; i--; chunk++) {
    ulint j;
    buf_block_t *block = chunk->blocks;

    for (j = chunk->size; j--; block++) {

      mutex_enter(&block->m_mutex);

      switch (block->get_state()) {
        case BUF_BLOCK_FILE_PAGE:
          ut_a(hash_get_page(block->get_space(), block->get_page_no()) == &block->m_page);

          switch (buf_page_get_io_fix(&block->m_page)) {
            case BUF_IO_NONE:
              break;

            case BUF_IO_WRITE:
              switch (buf_page_get_flush_type(&block->m_page)) {
                case BUF_FLUSH_LRU:
                  n_LRU_flush++;
                  ut_a(rw_lock_is_locked(&block->m_rw_lock, RW_LOCK_SHARED));
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

              ut_a(rw_lock_is_locked(&block->m_rw_lock, RW_LOCK_EX));
              break;
          }

          n_lru++;

          if (block->m_page.m_oldest_modification > 0) {
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

      mutex_exit(&block->m_mutex);
    }
  }

  if (n_lru + n_free > m_curr_size) {
	  ib_logger(ib_stream, "n LRU %lu, n free %lu, pool %lu\n", (ulong)n_lru, (ulong)n_free, (ulong)m_curr_size);
    ut_error;
  }

  ut_a(UT_LIST_GET_LEN(m_LRU_list) == n_lru);

  if (UT_LIST_GET_LEN(m_free_list) != n_free) {

    ib_logger(ib_stream, "Free list len %lu, free blocks %lu\n", (ulong)UT_LIST_GET_LEN(m_free_list), (ulong)n_free);

    ut_error;
  }

  ut_a(UT_LIST_GET_LEN(m_flush_list) == n_flush);

  ut_a(m_n_flush[BUF_FLUSH_SINGLE_PAGE] == n_single_flush);
  ut_a(m_n_flush[BUF_FLUSH_LIST] == n_list_flush);
  ut_a(m_n_flush[BUF_FLUSH_LRU] == n_LRU_flush);

  buf_pool_mutex_exit();

  ut_a(m_LRU->validate());
  ut_a(m_flusher->validate());

  return true;
}

void Buf_pool::print() {
  uint64_t id;
  dict_index_t *index;

  ut_ad(srv_buf_pool);

  auto size = m_curr_size;

  auto index_ids = static_cast<uint64_t *>(mem_alloc(sizeof(uint64_t) * size));
  auto counts = static_cast<ulint *>(mem_alloc(sizeof(ulint) * size));

  buf_pool_mutex_enter();

  ib_logger(
    ib_stream,
    "srv_buf_pool size %lu\n"
    "database pages %lu\n"
    "free pages %lu\n"
    "modified database pages %lu\n"
    "n pending reads %lu\n"
    "n pending flush LRU %lu list %lu single page %lu\n"
    "pages made young %lu, not young %lu\n"
    "pages read %lu, created %lu, written %lu\n",
    (ulong)size,
    (ulong)UT_LIST_GET_LEN(m_LRU_list),
    (ulong)UT_LIST_GET_LEN(m_free_list),
    (ulong)UT_LIST_GET_LEN(m_flush_list),
    (ulong)m_n_pend_reads,
    (ulong)m_n_flush[BUF_FLUSH_LRU],
    (ulong)m_n_flush[BUF_FLUSH_LIST],
    (ulong)m_n_flush[BUF_FLUSH_SINGLE_PAGE],
    (ulong)m_stat.n_pages_made_young,
    (ulong)m_stat.n_pages_not_made_young,
    (ulong)m_stat.n_pages_read,
    (ulong)m_stat.n_pages_created,
    (ulong)m_stat.n_pages_written
  );

  /* Count the number of blocks belonging to each index in the buffer */

  ulint n_found = 0;

  auto chunk = m_chunks;

  for (ulint i = m_n_chunks; i--; chunk++) {
    buf_block_t *block = chunk->blocks;
    ulint n_blocks = chunk->size;

    for (; n_blocks--; block++) {
      const buf_frame_t *frame = block->m_frame;

      if (srv_fil->page_get_type(frame) == FIL_PAGE_TYPE_INDEX) {

        id = btr_page_get_index_id(frame);

        /* Look for the id in the index_ids array */
        ulint j = 0;

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

  for (ulint i = 0; i < n_found; i++) {
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

  ut_a(validate());
}

ulint Buf_pool::get_latched_pages_number() {
  ulint fixed_pages_number{};

  buf_pool_mutex_enter();

  auto chunk = m_chunks;

  for (ulint i = m_n_chunks; i--; chunk++) {

    auto block = chunk->blocks;

    for (ulint j = chunk->size; j--; block++) {
      if (block->get_state() != BUF_BLOCK_FILE_PAGE) {

        continue;
      }

      mutex_enter(&block->m_mutex);

      if (block->m_page.m_buf_fix_count != 0 || buf_page_get_io_fix(&block->m_page) != BUF_IO_NONE) {
        ++fixed_pages_number;
      }

      mutex_exit(&block->m_mutex);
    }
  }

  buf_pool_mutex_exit();

  return fixed_pages_number;
}

#endif /* UNIV_DEBUG */

ulint Buf_pool::get_n_pending_ios() {
  return
    m_n_pend_reads + m_n_flush[BUF_FLUSH_LRU] + m_n_flush[BUF_FLUSH_LIST] +
    m_n_flush[BUF_FLUSH_SINGLE_PAGE];
}

ulint Buf_pool::get_modified_ratio_pct() {
  buf_pool_mutex_enter();

  auto ratio = (100 * UT_LIST_GET_LEN(m_flush_list)) / (1 + UT_LIST_GET_LEN(m_LRU_list) + UT_LIST_GET_LEN(m_free_list));

  /* 1 + is there to avoid division by zero */

  buf_pool_mutex_exit();

  return ratio;
}

void Buf_pool::print_io(ib_stream_t ib_stream) {
  time_t current_time;
  double time_elapsed;
  ulint n_gets_diff;

  ut_ad(srv_buf_pool);

  buf_pool_mutex_enter();

  log_info(
    "Buffer pool size     ", (ulong)m_curr_size, "\n",
    "Free buffers        ", (ulong)UT_LIST_GET_LEN(m_free_list),
    "\n",
    "Database pages      ", (ulong)m_LRU_old_len,
    "\n",
    "Old database pages  ", (ulong)UT_LIST_GET_LEN(m_LRU_list),
    "\n"
    "Modified db pages   ", (ulong)UT_LIST_GET_LEN(m_flush_list),
    "\n"
    "Pending reads       ", (ulong)m_n_pend_reads,
    "\n"
    "Pending writes: LRU ", (ulong)m_n_flush[BUF_FLUSH_LRU] + m_init_flush[BUF_FLUSH_LRU],
    ", flush list ", (ulong)m_n_flush[BUF_FLUSH_LIST] + m_init_flush[BUF_FLUSH_LIST],
    ", single page ", (ulong)m_n_flush[BUF_FLUSH_SINGLE_PAGE]
  );

  current_time = time(nullptr);
  time_elapsed = 0.001 + difftime(current_time, m_last_printout_time);

  log_info(
    ib_stream,
    "Pages made young ", (ulong)m_stat.n_pages_made_young,
    ", not young ", (ulong)m_stat.n_pages_not_made_young,
    "\n",
    (m_stat.n_pages_made_young - m_old_stat.n_pages_made_young) / time_elapsed,
    " youngs/s, ",
    (m_stat.n_pages_not_made_young - m_old_stat.n_pages_not_made_young) / time_elapsed,
    " non-youngs/s\n",
    "Pages read ", (ulong)m_stat.n_pages_read,
    ",",
    " created", (ulong)m_stat.n_pages_created,
    ","
    " written ", (ulong)m_stat.n_pages_written,
    "\n",
    (m_stat.n_pages_read - m_old_stat.n_pages_read) / time_elapsed,
    " reads/s, ",
    (m_stat.n_pages_created - m_old_stat.n_pages_created) / time_elapsed,
    " creates/s, ",
    (m_stat.n_pages_written - m_old_stat.n_pages_written) / time_elapsed,
    " writes/s"
  );

  n_gets_diff = m_stat.n_page_gets - m_old_stat.n_page_gets;

  if (n_gets_diff) {
    log_info(
      "Buffer pool hit rate ",
      (ulong)(1000 - ((1000 * (m_stat.n_pages_read - m_old_stat.n_pages_read)) /
                      (m_stat.n_page_gets - m_old_stat.n_page_gets))),
      "/ 1000,",
      " young-making rate ",
      (ulong)(1000 * (m_stat.n_pages_made_young - m_old_stat.n_pages_made_young) / n_gets_diff),
      "/ 1000",
      " not ",
      (ulong)(1000 * (m_stat.n_pages_not_made_young - m_old_stat.n_pages_not_made_young) / n_gets_diff),
      "/ 1000"
    );
  } else {
    log_info("No buffer pool page gets since the last printout");
  }

  /* Statistics about read ahead algorithm */
  log_info(
    "Pages read ahead ",
    (m_stat.n_ra_pages_read - m_old_stat.n_ra_pages_read) / time_elapsed,
    "/s",
    " evicted without access ",
    (m_stat.n_ra_pages_evicted - m_old_stat.n_ra_pages_evicted) / time_elapsed,
    "/s"
  );

  /* Print some values to help us with visualizing what is happening with LRU eviction. */
  log_info("LRU len: ", UT_LIST_GET_LEN(m_LRU_list),
	    "I/O sum[", m_LRU->m_stat_sum.m_io, "], ",
	    "cur[", m_LRU->m_stat_cur.m_io, "]");

  refresh_io_stats();
  buf_pool_mutex_exit();
}

void Buf_pool::refresh_io_stats() {
  m_last_printout_time = time(nullptr);
  m_old_stat = m_stat;
}

bool Buf_pool::all_freed() {
  buf_pool_mutex_enter();

  auto chunk = m_chunks;

  for (ulint i = m_n_chunks; i--; chunk++) {

    const auto block = chunk_not_freed(chunk);

    if (block != nullptr) {
      ib_logger(ib_stream, "Page %lu %lu still fixed or dirty\n", (ulong)block->m_page.m_space, (ulong)block->m_page.m_page_no);
      ut_error;
    }
  }

  buf_pool_mutex_exit();

  return true;
}

bool Buf_pool::is_io_pending() {
  buf_pool_mutex_enter();

  auto ret = m_n_pend_reads + m_n_flush[BUF_FLUSH_LRU] + m_n_flush[BUF_FLUSH_LIST] + m_n_flush[BUF_FLUSH_SINGLE_PAGE] > 0;

  buf_pool_mutex_exit();

  return ret;
}

ulint Buf_pool::get_free_list_len() {
  buf_pool_mutex_enter();

  const auto len = UT_LIST_GET_LEN(m_free_list);

  buf_pool_mutex_exit();

  return len;
}

#ifdef UNIV_DEBUG
buf_page_t *Buf_pool::set_file_page_was_freed(space_id_t space, page_no_t page_no) {
  buf_pool_mutex_enter();

  auto bpage = hash_get_page(space, page_no);

  if (bpage != nullptr) {
    bpage->m_file_page_was_freed = true;
  }

  buf_pool_mutex_exit();

  return bpage;
}
#endif /* UNIV_DEBUG */

