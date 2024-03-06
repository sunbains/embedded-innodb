/**
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

/** @file buf/buf0rea.c
The database buffer read

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#include "buf0rea.h"

#include "fil0fil.h"
#include "mtr0mtr.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"

#include "log0recv.h"
#include "os0file.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "trx0sys.h"

/** The linear read-ahead area size */
#define BUF_READ_AHEAD_LINEAR_AREA BUF_READ_AHEAD_AREA

/** If there are buf_pool->curr_size per the number below pending reads, then
read-ahead is not done: this is to prevent flooding the buffer pool with
i/o-fixed buffer blocks */
#define BUF_READ_AHEAD_PEND_LIMIT 2

/** Low-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there, in which case does nothing.
Sets the io_fix flag and sets an exclusive lock on the buffer frame. The
flag is cleared and the x-lock released by an i/o-handler thread.
@return 1 if a read request was queued, 0 if the page already resided
in buf_pool, or if the page is in the doublewrite buffer blocks in
which case it is never read into the pool, or if the tablespace does
not exist or is being dropped
@return 1 if read request is issued. 0 if it is not */
static ulint buf_read_page_low(
    db_err *err, /*!< out: DB_SUCCESS or DB_TABLESPACE_DELETED if we are
                trying to read from a non-existent tablespace, or a
                tablespace which is just now being dropped */
    bool sync,   /*!< in: true if synchronous aio is desired */
    ulint mode,  /*!< in: ..., ORed to OS_AIO_SIMULATED_WAKE_LATER (see below
                 at read-ahead functions) */
    ulint space, /*!< in: space id */
    ulint, bool,
    int64_t tablespace_version, /*!< in: if the space memory object has
                    this timestamp different from what we are giving here,
                    treat the tablespace as dropped; this is a timestamp we
                    use to stop dangling page reads from a tablespace
                    which we have DISCARDed + IMPORTed back */
    ulint offset)               /*!< in: page number */
{
  buf_page_t *bpage;
  ulint wake_later;

  *err = DB_SUCCESS;

  wake_later = mode & OS_AIO_SIMULATED_WAKE_LATER;
  mode = mode & ~OS_AIO_SIMULATED_WAKE_LATER;

  if (trx_doublewrite && space == TRX_SYS_SPACE &&
      ((offset >= trx_doublewrite->block1 &&
        offset < trx_doublewrite->block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) ||
       (offset >= trx_doublewrite->block2 &&
        offset < trx_doublewrite->block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE))) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream,
              "  Warning: trying to read"
              " doublewrite buffer page %lu\n",
              (ulong)offset);

    return (0);
  }

  if (trx_sys_hdr_page(space, offset)) {

    /* Trx sys header is so low in the latching order that we play
    safe and do not leave the i/o-completion to an asynchronous
    i/o-thread. */

    sync = true;
  }

  /* The following call will also check if the tablespace does not exist
  or is being dropped; if we succeed in initing the page in the buffer
  pool for read, then DISCARD cannot proceed until the read has
  completed */
  bpage = buf_page_init_for_read(err, mode, space, tablespace_version, offset);
  if (bpage == nullptr) {

    return (0);
  }

#ifdef UNIV_DEBUG
  if (buf_debug_prints) {
    ib_logger(ib_stream, "Posting read request for page %lu, sync %lu\n",
              (ulong)offset, (ulong)sync);
  }
#endif /* UNIV_DEBUG */

  ut_ad(buf_page_in_file(bpage));

  ut_a(buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE);

  *err = fil_io(OS_FILE_READ | wake_later, sync, space, offset, 0,
                UNIV_PAGE_SIZE, ((buf_block_t *)bpage)->frame, bpage);
  ut_a(*err == DB_SUCCESS);

  if (sync) {
    /* The i/o is already completed when we arrive from
    fil_read */
    buf_page_io_complete(bpage);
  }

  return (1);
}

bool buf_read_page(ulint space, ulint offset) {
  db_err err;

  auto tablespace_version = fil_space_get_version(space);

  /* We do the i/o in the synchronous aio mode to save thread switches: hence
   * true */
  auto count = buf_read_page_low(&err, true, BUF_READ_ANY_PAGE, space, 0, false,
                                 tablespace_version, offset);

  srv_buf_pool_reads += count;

  if (err == DB_TABLESPACE_DELETED) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream,
              "  Error: trying to access"
              " tablespace %lu page no. %lu,\n"
              "but the tablespace does not exist"
              " or is just being dropped.\n",
              (ulong)space, (ulong)offset);
  }

  /* Flush pages from the end of the LRU list if necessary */
  buf_flush_free_margin();

  /* Increment number of I/O operations used for LRU policy. */
  buf_LRU_stat_inc_io();

  return (count > 0);
}

ulint buf_read_ahead_linear(ulint space, ulint offset) {
  buf_page_t *bpage;
  buf_frame_t *frame;
  buf_page_t *pred_bpage = nullptr;
  ulint pred_offset;
  ulint succ_offset;
  ulint count;
  int asc_or_desc;
  ulint new_offset;
  ulint fail_count;
  db_err err;
  ulint i;
  const ulint buf_read_ahead_linear_area = BUF_READ_AHEAD_LINEAR_AREA;
  ulint threshold;

  if (unlikely(srv_startup_is_before_trx_rollback_phase)) {
    /* No read-ahead to avoid thread deadlocks */
    return (0);
  }

  auto low = (offset / buf_read_ahead_linear_area) * buf_read_ahead_linear_area;
  auto high = (offset / buf_read_ahead_linear_area + 1) * buf_read_ahead_linear_area;

  if ((offset != low) && (offset != high - 1)) {
    /* This is not a border page of the area: return */

    return 0;
  }

  /* Remember the tablespace version before we ask te tablespace size
  below: if DISCARD + IMPORT changes the actual .ibd file meanwhile, we
  do not try to read outside the bounds of the tablespace! */

  auto tablespace_version = fil_space_get_version(space);

  buf_pool_mutex_enter();

  if (high > fil_space_get_size(space)) {
    buf_pool_mutex_exit();
    /* The area is not whole, return */

    return (0);
  }

  if (buf_pool->n_pend_reads >
      buf_pool->curr_size / BUF_READ_AHEAD_PEND_LIMIT) {
    buf_pool_mutex_exit();

    return (0);
  }

  /* Check that almost all pages in the area have been accessed; if
  offset == low, the accesses must be in a descending order, otherwise,
  in an ascending order. */

  asc_or_desc = 1;

  if (offset == low) {
    asc_or_desc = -1;
  }

  /* How many out of order accessed pages can we ignore
  when working out the access pattern for linear readahead */
  threshold = ut_min((64 - srv_read_ahead_threshold), BUF_READ_AHEAD_AREA);

  fail_count = 0;

  for (i = low; i < high; i++) {
    bpage = buf_page_hash_get(space, i);

    if ((bpage == nullptr) || !buf_page_is_accessed(bpage)) {
      /* Not accessed */
      fail_count++;

    } else if (pred_bpage) {
      /* Note that buf_page_is_accessed() returns
      the time of the first access.  If some blocks
      of the extent existed in the buffer pool at
      the time of a linear access pattern, the first
      access times may be nonmonotonic, even though
      the latest access times were linear.  The
      threshold (srv_read_ahead_factor) should help
      a little against this. */
      int res = ut_ulint_cmp(buf_page_is_accessed(bpage),
                             buf_page_is_accessed(pred_bpage));
      /* Accesses not in the right order */
      if (res != 0 && res != asc_or_desc) {
        fail_count++;
      }
    }

    if (fail_count > threshold) {
      /* Too many failures: return */
      buf_pool_mutex_exit();
      return (0);
    }

    if (bpage && buf_page_is_accessed(bpage)) {
      pred_bpage = bpage;
    }
  }

  /* If we got this far, we know that enough pages in the area have
  been accessed in the right order: linear read-ahead can be sensible */

  bpage = buf_page_hash_get(space, offset);

  if (bpage == nullptr) {
    buf_pool_mutex_exit();

    return (0);
  }

  switch (buf_page_get_state(bpage)) {
  case BUF_BLOCK_FILE_PAGE:
    frame = ((buf_block_t *)bpage)->frame;
    break;
  default:
    ut_error;
    break;
  }

  /* Read the natural predecessor and successor page addresses from
  the page; NOTE that because the calling thread may have an x-latch
  on the page, we do not acquire an s-latch on the page, this is to
  prevent deadlocks. Even if we read values which are nonsense, the
  algorithm will work. */

  pred_offset = fil_page_get_prev(frame);
  succ_offset = fil_page_get_next(frame);

  buf_pool_mutex_exit();

  if ((offset == low) && (succ_offset == offset + 1)) {

    /* This is ok, we can continue */
    new_offset = pred_offset;

  } else if ((offset == high - 1) && (pred_offset == offset - 1)) {

    /* This is ok, we can continue */
    new_offset = succ_offset;
  } else {
    /* Successor or predecessor not in the right order */

    return 0;
  }

  low = (new_offset / buf_read_ahead_linear_area) * buf_read_ahead_linear_area;
  high = (new_offset / buf_read_ahead_linear_area + 1) *
         buf_read_ahead_linear_area;

  if ((new_offset != low) && (new_offset != high - 1)) {
    /* This is not a border page of the area: return */

    return 0;
  }

  if (high > fil_space_get_size(space)) {
    /* The area is not whole, return */

    return 0;
  }

  count = 0;

  for (i = low; i < high; i++) {
    /* It is only sensible to do read-ahead in the non-sync
    aio mode: hence false as the first parameter */

    count += buf_read_page_low(&err, false,
      OS_AIO_SIMULATED_WAKE_LATER, space, 0, false, tablespace_version, i);

    if (err == DB_TABLESPACE_DELETED) {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream,
                "Lnear readahead trying to access tablespace %lu page %lu,but the tablespace does not"
                " exist or is just being dropped.",
                  (ulong)space, (ulong)i);
    }
  }

  /* In simulated aio we wake the aio handler threads only after
  queuing all aio requests, in native aio the following call does
  nothing: */

  os_aio_simulated_wake_handler_threads();

  /* Flush pages from the end of the LRU list if necessary */
  buf_flush_free_margin();

#ifdef UNIV_DEBUG
  if (buf_debug_prints && (count > 0)) {
    ib_logger(ib_stream, "LINEAR read-ahead space %lu offset %lu pages %lu\n",
              (ulong)space, (ulong)offset, (ulong)count);
  }
#endif /* UNIV_DEBUG */

  /* Read ahead is considered one I/O operation for the purpose of
  LRU policy decision. */
  buf_LRU_stat_inc_io();

  buf_pool->stat.n_ra_pages_read += count;
  return (count);
}

void buf_read_recv_pages(bool sync, ulint space, const ulint *page_nos, ulint n_stored) {
  if (fil_space_get_size(space) == ULINT_UNDEFINED) {
    /* It is a single table tablespace and the .ibd file is
    missing: do nothing */
    return;
  }

  db_err err;
  ulint count;
  auto tablespace_version = fil_space_get_version(space);

  for (ulint i = 0; i < n_stored; i++) {

    count = 0;

    os_aio_print_debug = false;

    while (buf_pool->n_pend_reads >= recv_n_pool_free_frames / 2) {

      os_aio_simulated_wake_handler_threads();
      os_thread_sleep(10000);

      count++;

      if (count > 1000) {
        ib_logger(ib_stream,
                  "Error: InnoDB has waited for"
                  " 10 seconds for pending\n"
                  "reads to the buffer pool to"
                  " be finished.\n"
                  "Number of pending reads %lu,"
                  " pending pread calls %lu\n",
                  (ulong)buf_pool->n_pend_reads,
                  (ulong)os_file_n_pending_preads);

        os_aio_print_debug = true;
      }
    }

    os_aio_print_debug = false;

    if ((i + 1 == n_stored) && sync) {
      buf_read_page_low(&err, true, BUF_READ_ANY_PAGE, space, 0, true,
                        tablespace_version, page_nos[i]);
    } else {
      buf_read_page_low(&err, false,
                        BUF_READ_ANY_PAGE | OS_AIO_SIMULATED_WAKE_LATER, space,
                        0, true, tablespace_version, page_nos[i]);
    }
  }

  os_aio_simulated_wake_handler_threads();

  /* Flush pages from the end of the LRU list if necessary */
  buf_flush_free_margin();

#ifdef UNIV_DEBUG
  if (buf_debug_prints) {
    ib_logger(ib_stream, "Recovery applies read-ahead pages %lu\n",
              (ulong)n_stored);
  }
#endif /* UNIV_DEBUG */
}
