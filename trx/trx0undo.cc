/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file trx/trx0undo.c
Transaction undo log

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0undo.h"

#include "fsp0fsp.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0rseg.h"
#include "trx0trx.h"

/* How should the old versions in the history list be managed?
   ----------------------------------------------------------
If each transaction is given a whole page for its update undo log, file
space consumption can be 10 times higher than necessary. Therefore,
partly filled update undo log pages should be reusable. But then there
is no way individual pages can be ordered so that the ordering agrees
with the serialization numbers of the transactions on the pages. Thus,
the history list must be formed of undo logs, not their header pages as
it was in the old implementation.
        However, on a single header page the transactions are placed in
the order of their serialization numbers. As old versions are purged, we
may free the page when the last transaction on the page has been purged.
        A problem is that the purge has to go through the transactions
in the serialization order. This means that we have to look through all
rollback segments for the one that has the smallest transaction number
in its history list.
        When should we do a purge? A purge is necessary when space is
running out in any of the rollback segments. Then we may have to purge
also old version which might be needed by some consistent read. How do
we trigger the start of a purge? When a transaction writes to an undo log,
it may notice that the space is running out. When a read view is closed,
it may make some history superfluous. The server can have an utility which
periodically checks if it can purge some history.
        In a parallellized purge we have the problem that a query thread
can remove a delete marked clustered index record before another query
thread has processed an earlier version of the record, which cannot then
be done because the row cannot be constructed from the clustered index
record. To avoid this problem, we will store in the update and delete mark
undo record also the columns necessary to construct the secondary index
entries which are modified.
        We can latch the stack of versions of a single clustered index record
by taking a latch on the clustered index page. As long as the latch is held,
no new versions can be added and no versions removed by undo. But, a purge
can still remove old versions from the bottom of the stack. */

/* How to protect rollback segments, undo logs, and history lists with
   -------------------------------------------------------------------
latches?
-------
The contention of the kernel mutex should be minimized. When a transaction
does its first insert or modify in an index, an undo log is assigned for it.
Then we must have an x-latch to the rollback segment header.
        When the transaction does more modifys or rolls back, the undo log is
protected with undo_mutex in the transaction.
        When the transaction commits, its insert undo log is either reset and
cached for a fast reuse, or freed. In these cases we must have an x-latch on
the rollback segment page. The update undo log is put to the history list. If
it is not suitable for reuse, its slot in the rollback segment is reset. In
both cases, an x-latch must be acquired on the rollback segment.
        The purge operation steps through the history list without modifying
it until a truncate operation occurs, which can remove undo logs from the end
of the list and release undo log segments. In stepping through the list,
s-latches on the undo log pages are enough, but in a truncate, x-latches must
be obtained on the rollback segment and individual pages. */

/**
 * Initializes the fields in an undo log segment page.
 *
 * @param undo_page The undo log segment page to be initialized.
 * @param type The type of the undo log segment.
 * @param mtr The mini-transaction handle.
 */
static void trx_undo_page_init(page_t *undo_page, ulint type, mtr_t *mtr) noexcept;

/**
 * Creates a new undo log in the rollback segment memory object.
 *
 * @param rseg The rollback segment memory object.
 * @param id The slot index within the rollback segment.
 * @param type The type of the log: TRX_UNDO_INSERT or TRX_UNDO_UPDATE.
 * @param trx_id The ID of the transaction for which the undo log is created.
 * @param xid The X/Open XA transaction identification.
 * @param page_no The undo log header page number.
 * @param offset The undo log header byte offset on page.
 * 
 * @return A pointer to the newly created trx_undo_t object.
 */
static trx_undo_t *trx_undo_mem_create(
  trx_rseg_t *rseg,
  ulint id,
  ulint type,
  trx_id_t trx_id,
  const XID *xid,
  page_no_t page_no,
  ulint offset
);

/**
 * Initializes a cached insert undo log header page for new use. NOTE that this
 * function has its own log record type MLOG_UNDO_HDR_REUSE. You must NOT change
 * the operation of this function!
 *
 * @param undo_page The insert undo log segment header page, x-latched.
 * @param trx_id The transaction id.
 * @param mtr The mtr.
 * 
 * @return	undo log header byte offset on page
 */
static ulint trx_undo_insert_header_reuse(page_t *undo_page, trx_id_t trx_id, mtr_t *mtr);

/**
 * If an update undo log can be discarded immediately, this function frees the
 * space, resetting the page to the proper state for caching.
 * 
 * @param undo_page The undo log page to be reset.
 * @param mtr The mini-transaction handle.
 */
static void trx_undo_discard_latest_update_undo(page_t *undo_page, mtr_t *mtr);

/**
 * Gets the previous record in an undo log from the previous page.
 * 
 * @param[in] rec The undo record.
 * @param[in] page_no The undo log header page number.
 * @param[in] offset The undo log header offset on page.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	undo log record, the page s-latched, nullptr if none
 */
static trx_undo_rec_t *trx_undo_get_prev_rec_from_prev_page(
  trx_undo_rec_t *rec,
  page_no_t page_no,
  ulint offset,
  mtr_t *mtr)
{
  auto undo_page = page_align(rec);
  auto prev_page_no = flst_get_prev_addr(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr).m_page_no;

  if (prev_page_no == FIL_NULL) {

    return nullptr;
  }

  auto space = page_get_space_id(undo_page);
  auto prev_page = trx_undo_page_get_s_latched(space, prev_page_no, mtr);

  return trx_undo_page_get_last_rec(prev_page, page_no, offset);
}

trx_undo_rec_t *trx_undo_get_prev_rec(trx_undo_rec_t *rec, page_no_t page_no, ulint offset, mtr_t *mtr) {
  auto prev_rec = trx_undo_page_get_prev_rec(rec, page_no, offset);

  if (prev_rec != nullptr) {

    return prev_rec;
  }

  /* We have to go to the previous undo log page to look for the
  previous record */

  return trx_undo_get_prev_rec_from_prev_page(rec, page_no, offset, mtr);
}

/**
 * Gets the next record in an undo log from the next page.
 * 
 * @param[in] space The undo log header space.
 * @param[in] undo_page The undo log page.
 * @param[in] page_no The undo log header page number.
 * @param[in] offset The undo log header offset on page.
 * @param[in] mode The latch mode: RW_S_LATCH or RW_X_LATCH.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	undo log record, the page latched, nullptr if none
 */
static trx_undo_rec_t *trx_undo_get_next_rec_from_next_page(
  space_id_t space,
  page_t *undo_page,
  page_no_t page_no,
  ulint offset,
  ulint mode,
  mtr_t *mtr)
{
  if (page_no == page_get_page_no(undo_page)) {

    auto log_hdr = undo_page + offset;
    auto next = mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG);

    if (next != 0) {

      return nullptr;
    }
  }

  auto next_page_no = flst_get_next_addr(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr).m_page_no;

  if (next_page_no == FIL_NULL) {

    return nullptr;
  }

  page_t *next_page;

  if (mode == RW_S_LATCH) {
    next_page = trx_undo_page_get_s_latched(space, next_page_no, mtr);
  } else {
    ut_ad(mode == RW_X_LATCH);
    next_page = trx_undo_page_get(space, next_page_no, mtr);
  }

  return trx_undo_page_get_first_rec(next_page, page_no, offset);
}

trx_undo_rec_t *trx_undo_get_next_rec(trx_undo_rec_t *rec, page_no_t page_no, ulint offset, mtr_t *mtr) {
  auto next_rec = trx_undo_page_get_next_rec(rec, page_no, offset);

  if (next_rec != nullptr) {
    return next_rec;
  }

  auto space = page_get_space_id(page_align(rec));

  return trx_undo_get_next_rec_from_next_page(space, page_align(rec), page_no, offset, RW_S_LATCH, mtr);
}

trx_undo_rec_t *trx_undo_get_first_rec(ulint space, ulint page_no, ulint offset, ulint mode, mtr_t *mtr) {
  page_t *undo_page;

  if (mode == RW_S_LATCH) {
    undo_page = trx_undo_page_get_s_latched(space, page_no, mtr);
  } else {
    undo_page = trx_undo_page_get(space, page_no, mtr);
  }

  auto rec = trx_undo_page_get_first_rec(undo_page, page_no, offset);

  if (rec != nullptr) {
    return rec;
  }

  return trx_undo_get_next_rec_from_next_page(space, undo_page, page_no, offset, mode, mtr);
}

/**
 * Writes the mtr log entry of an undo log page initialization.
 *
 * @param undo_page The undo log page to be initialized.
 * @param type The type of the undo log.
 * @param mtr The mini-transaction handle.
 */
inline void trx_undo_page_init_log(page_t *undo_page, ulint type, mtr_t *mtr) {
  mlog_write_initial_log_record(undo_page, MLOG_UNDO_INIT, mtr);

  mlog_catenate_ulint_compressed(mtr, type);
}

byte *trx_undo_parse_page_init(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) {
  auto type = mach_parse_compressed(ptr, end_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (page != nullptr) {
    trx_undo_page_init(page, type, mtr);
  }

  return ptr;
}

static void trx_undo_page_init(page_t *undo_page, ulint type, mtr_t *mtr) noexcept {
  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_TYPE, type);

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE);
  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE);

  srv_fil->page_set_type(undo_page, FIL_PAGE_TYPE_UNDO_LOG);

  trx_undo_page_init_log(undo_page, type, mtr);
}

/**
 * Creates a new undo segment within a rollback segment.
 *
 * @param rseg The rollback segment.
 * @param rseg_hdr The rollback segment header page (x-latched).
 * @param type The type of the segment: TRX_UNDO_INSERT or TRX_UNDO_UPDATE.
 * @param id Output parameter for the slot index within the rseg header.
 * @param undo_page Output parameter for the segment header page (x-latched). Set to nullptr if there was an error.
 * @param mtr The mtr (mini-transaction) object.
 * 
 * @return A db_err indicating the success or failure of the operation.
 *  (DB_TOO_MANY_CONCURRENT_TRXS DB_OUT_OF_FILE_SPACE)
 */
static db_err trx_undo_seg_create(
  trx_rseg_t *rseg __attribute__((unused)),
  trx_rsegf_t *rseg_hdr,
  ulint type,
  ulint *id,
  page_t **undo_page,
  mtr_t *mtr) noexcept
{
  ut_ad(mtr && id && rseg_hdr);
  ut_ad(mutex_own(&(rseg->mutex)));

  auto slot_no = trx_rsegf_undo_find_free(rseg_hdr, mtr);

  if (slot_no == ULINT_UNDEFINED) {
    log_warn(
      "Cannot find a free slot for an undo log. Do you have too"
      " many active transactions running concurrently?"
    );

    return DB_TOO_MANY_CONCURRENT_TRXS;
  }

  ulint n_reserved;
  auto space = page_get_space_id(page_align(rseg_hdr));
  auto success = fsp_reserve_free_extents(&n_reserved, space, 2, FSP_UNDO, mtr);

  if (!success) {

    return DB_OUT_OF_FILE_SPACE;
  }

  /* Allocate a new file segment for the undo log */
  auto block = fseg_create_general(space, 0, TRX_UNDO_SEG_HDR + TRX_UNDO_FSEG_HEADER, true, mtr);

  srv_fil->space_release_free_extents(space, n_reserved);

  if (block == nullptr) {
    /* No space left */

    return DB_OUT_OF_FILE_SPACE;
  }

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_UNDO_PAGE));

  *undo_page = block->get_frame();

  auto page_hdr = *undo_page + TRX_UNDO_PAGE_HDR;
  auto seg_hdr = *undo_page + TRX_UNDO_SEG_HDR;

  trx_undo_page_init(*undo_page, type, mtr);

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_FREE, TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE, MLOG_2BYTES, mtr);

  mlog_write_ulint(seg_hdr + TRX_UNDO_LAST_LOG, 0, MLOG_2BYTES, mtr);

  flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, mtr);

  flst_add_last(seg_hdr + TRX_UNDO_PAGE_LIST, page_hdr + TRX_UNDO_PAGE_NODE, mtr);

  trx_rsegf_set_nth_undo(rseg_hdr, slot_no, page_get_page_no(*undo_page), mtr);
  *id = slot_no;

  return DB_SUCCESS;
}

/**
 * Writes the mtr log entry of an undo log header initialization.
 * 
 * @param undo_page The undo log header page.
 * @param trx_id The transaction id.
 * @param mtr The mtr.
 */
inline void trx_undo_header_create_log(const page_t *undo_page, trx_id_t trx_id, mtr_t *mtr) {
  mlog_write_initial_log_record(undo_page, MLOG_UNDO_HDR_CREATE, mtr);

  mlog_catenate_uint64_compressed(mtr, trx_id);
}

/**
 * Creates a new undo log header in file. NOTE that this function has its own
 * log record type MLOG_UNDO_HDR_CREATE. You must NOT change the operation of
 * this function!
 * 
 * @param[in,out] undo_page The undo log segment header page, x-latched. It
 *  is assumed that there is TRX_UNDO_LOG_XA_HDR_SIZE bytes free space on it.
 * @param[in] trx_id The transaction id.
 * @param[in] mtr The mtr.
 * 
 * @return	header byte offset on page
 */
static ulint trx_undo_header_create(page_t *undo_page, trx_id_t trx_id, mtr_t *mtr) {
  ut_ad(mtr && undo_page);

  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  auto free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);
  auto log_hdr = undo_page + free;
  auto new_free = free + TRX_UNDO_LOG_HDR_SIZE;

#ifdef WITH_XOPEN
  ut_a(free + TRX_UNDO_LOG_XA_HDR_SIZE < UNIV_PAGE_SIZE - 100);
#endif /* WITH_XOPEN */

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, new_free);

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, new_free);

  mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_ACTIVE);

  auto prev_log = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);

  if (prev_log != 0) {
    auto prev_log_hdr = undo_page + prev_log;

    mach_write_to_2(prev_log_hdr + TRX_UNDO_NEXT_LOG, free);
  }

  mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, free);

  log_hdr = undo_page + free;

  mach_write_to_2(log_hdr + TRX_UNDO_DEL_MARKS, true);

  mach_write_to_8(log_hdr + TRX_UNDO_TRX_ID, trx_id);
  mach_write_to_2(log_hdr + TRX_UNDO_LOG_START, new_free);

  mach_write_to_1(log_hdr + TRX_UNDO_XID_EXISTS, false);
  mach_write_to_1(log_hdr + TRX_UNDO_DICT_TRANS, false);

  mach_write_to_2(log_hdr + TRX_UNDO_NEXT_LOG, 0);
  mach_write_to_2(log_hdr + TRX_UNDO_PREV_LOG, prev_log);

  /* Write the log record about the header creation */
  trx_undo_header_create_log(undo_page, trx_id, mtr);

  return free;
}

/**
 * Write X/Open XA Transaction Identification (XID) to undo log header
 * 
 * @param[in] log_hdr The undo log header.
 * @param[in] xid The X/Open XA Transaction Identification.
 * @param[in] mtr The mtr.
 */
static void trx_undo_write_xid(
  trx_ulogf_t *log_hdr,
#ifdef WITH_XOPEN
  const XID *xid,
#endif /* WITH_XOPEN */
  mtr_t *mtr)
{
#ifdef WITH_XOPEN
  mlog_write_ulint(log_hdr + TRX_UNDO_XA_FORMAT, (ulint)xid->formatID, MLOG_4BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_XA_TRID_LEN, (ulint)xid->gtrid_length, MLOG_4BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_XA_BQUAL_LEN, (ulint)xid->bqual_length, MLOG_4BYTES, mtr);

  mlog_write_string(log_hdr + TRX_UNDO_XA_XID, (const byte *)xid->data, XIDDATASIZE, mtr);
#else
  static byte buf[XIDDATASIZE];

  mlog_write_ulint(log_hdr + TRX_UNDO_XA_FORMAT, (ulint)-1, MLOG_4BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_XA_TRID_LEN, (ulint)0, MLOG_4BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_XA_BQUAL_LEN, (ulint)0, MLOG_4BYTES, mtr);

  mlog_write_string(log_hdr + TRX_UNDO_XA_XID, (const byte *)buf, XIDDATASIZE, mtr);
#endif /* WITH_XOPEN */
}

#ifdef WITH_XOPEN
/**
 * Read X/Open XA Transaction Identification (XID) from undo log header
 * 
 * @param[in] log_hdr The undo log header.
 * @param[out] xid The X/Open XA Transaction Identification.
 */
static void trx_undo_read_xid(trx_ulogf_t *log_hdr, XID *xid) {
  xid->formatID = (long)mach_read_from_4(log_hdr + TRX_UNDO_XA_FORMAT);

  xid->gtrid_length = (long)mach_read_from_4(log_hdr + TRX_UNDO_XA_TRID_LEN);
  xid->bqual_length = (long)mach_read_from_4(log_hdr + TRX_UNDO_XA_BQUAL_LEN);

  memcpy(xid->data, log_hdr + TRX_UNDO_XA_XID, XIDDATASIZE);
}

/**
 * Adds space for the XA XID after an undo log old-style header.
 * 
 * @param[in] undo_page The undo log segment header page.
 * @param[in] log_hdr The undo log header.
 * @param[in] mtr The mtr.
 */
static void trx_undo_header_add_space_for_xid(page_t *undo_page, trx_ulogf_t *log_hdr, mtr_t *mtr) {
  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  auto free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);

  /* free is now the end offset of the old style undo log header */

  ut_a(free == (ulint)(log_hdr - undo_page) + TRX_UNDO_LOG_HDR_SIZE);

  auto new_free = free + (TRX_UNDO_LOG_XA_HDR_SIZE - TRX_UNDO_LOG_HDR_SIZE);

  /* Add space for a XID after the header, update the free offset
  fields on the undo log page and in the undo log header */

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_START, new_free, MLOG_2BYTES, mtr);

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_FREE, new_free, MLOG_2BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_LOG_START, new_free, MLOG_2BYTES, mtr);
}
#endif /* WITH_XOPEN */

/**
 * Writes the mtr log entry of an undo log header reuse.
 * 
 * @param[in] undo_page The undo log header page.
 * @param[in] trx_id The transaction id.
 * @param[in] mtr The mtr.
 */
inline void trx_undo_insert_header_reuse_log(const page_t *undo_page, trx_id_t trx_id, mtr_t *mtr) {
  mlog_write_initial_log_record(undo_page, MLOG_UNDO_HDR_REUSE, mtr);

  mlog_catenate_uint64_compressed(mtr, trx_id);
}

byte *trx_undo_parse_page_header(ulint type, byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) {
  trx_id_t trx_id;

  ptr = mach_uint64_parse_compressed(ptr, end_ptr, &trx_id);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (page != nullptr) {
    if (type == MLOG_UNDO_HDR_CREATE) {
      trx_undo_header_create(page, trx_id, mtr);
    } else {
      ut_ad(type == MLOG_UNDO_HDR_REUSE);
      trx_undo_insert_header_reuse(page, trx_id, mtr);
    }
  }

  return ptr;
}

/**
 * Initializes a cached insert undo log header page for new use. NOTE that this
 * function has its own log record type MLOG_UNDO_HDR_REUSE. You must NOT change
 * the operation of this function!
 * 
 * @param[in,out] undo_page The insert undo log segment header page, x-latched.
 * @param[in] trx_id The transaction id.
 * @param[in] mtr The mtr.
 * 
 * @return	undo log header byte offset on page
 */
static ulint trx_undo_insert_header_reuse(page_t *undo_page, trx_id_t trx_id, mtr_t *mtr) {
  ut_ad(mtr && undo_page);

  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  auto free = TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE;

  ut_a(free + TRX_UNDO_LOG_XA_HDR_SIZE < UNIV_PAGE_SIZE - 100);

  auto log_hdr = undo_page + free;
  auto new_free = free + TRX_UNDO_LOG_HDR_SIZE;

  /* Insert undo data is not needed after commit: we may free all
  the space on the page */

  ut_a(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT);

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, new_free);

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, new_free);

  mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_ACTIVE);

  log_hdr = undo_page + free;

  mach_write_to_8(log_hdr + TRX_UNDO_TRX_ID, trx_id);
  mach_write_to_2(log_hdr + TRX_UNDO_LOG_START, new_free);

  mach_write_to_1(log_hdr + TRX_UNDO_XID_EXISTS, false);
  mach_write_to_1(log_hdr + TRX_UNDO_DICT_TRANS, false);

  /* Write the log record MLOG_UNDO_HDR_REUSE */
  trx_undo_insert_header_reuse_log(undo_page, trx_id, mtr);

  return free;
}

/**
 * Writes the redo log entry of an update undo log header discard.
 * 
 * @param[in] undo_page The undo log header page.
 * @param[in] mtr The mtr.
 */
inline void trx_undo_discard_latest_log(page_t *undo_page, mtr_t *mtr) {
  mlog_write_initial_log_record(undo_page, MLOG_UNDO_HDR_DISCARD, mtr);
}

byte *trx_undo_parse_discard_latest(
  byte *ptr,
  byte *end_ptr __attribute__((unused)),
  page_t *page,
  mtr_t *mtr)
{
  ut_ad(end_ptr);

  if (page != nullptr) {
    trx_undo_discard_latest_update_undo(page, mtr);
  }

  return ptr;
}

/**
 * If an update undo log can be discarded immediately, this function frees the
 * space, resetting the page to the proper state for caching.
 * 
 * @param[in] undo_page Header page of an undo log of size 1.
 * @param[in] mtr The mini-transaction handle.
 */
static void trx_undo_discard_latest_update_undo(page_t *undo_page, mtr_t *mtr) {
  auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  auto free = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
  auto log_hdr = undo_page + free;
  auto prev_hdr_offset = mach_read_from_2(log_hdr + TRX_UNDO_PREV_LOG);

  trx_ulogf_t *prev_log_hdr;

  if (prev_hdr_offset != 0) {
    prev_log_hdr = undo_page + prev_hdr_offset;

    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, mach_read_from_2(prev_log_hdr + TRX_UNDO_LOG_START));
    mach_write_to_2(prev_log_hdr + TRX_UNDO_NEXT_LOG, 0);
  }

  mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, free);

  mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_CACHED);
  mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, prev_hdr_offset);

  trx_undo_discard_latest_log(undo_page, mtr);
}

ulint trx_undo_add_page(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) {
  ut_ad(mutex_own(&trx->undo_mutex));
  ut_ad(!mutex_own(&kernel_mutex));
  ut_ad(mutex_own(&trx->rseg->mutex));

  auto rseg = trx->rseg;

  if (rseg->curr_size == rseg->max_size) {

    return FIL_NULL;
  }

  ulint n_reserved;
  auto header_page = trx_undo_page_get(undo->space, undo->hdr_page_no, mtr);
  auto success = fsp_reserve_free_extents(&n_reserved, undo->space, 1, FSP_UNDO, mtr);

  if (!success) {

    return FIL_NULL;
  }

  auto page_no = fseg_alloc_free_page_general(
    header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_FSEG_HEADER, undo->top_page_no + 1, FSP_UP, true, mtr);

  srv_fil->space_release_free_extents(undo->space, n_reserved);

  if (page_no == FIL_NULL) {

    /* No space left */

    return FIL_NULL;
  }

  undo->last_page_no = page_no;

  auto new_page = trx_undo_page_get(undo->space, page_no, mtr);

  trx_undo_page_init(new_page, undo->type, mtr);

  flst_add_last(header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST, new_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);

  ++undo->size;
  ++rseg->curr_size;

  return page_no;
}

/** Frees an undo log page that is not the header page.
@return	last page number in remaining log */
static ulint trx_undo_free_page(
  trx_rseg_t *rseg,  /*!< in: rollback segment */
  bool in_history,   /*!< in: true if the undo log is in the
                                       history  list */
  ulint space,       /*!< in: space */
  ulint hdr_page_no, /*!< in: header page number */
  ulint page_no,     /*!< in: page number to free: must not be the
                                  header page */
  mtr_t *mtr
) /*!< in: mtr which does not have a latch to any
                               undo log page; the caller must have reserved
                               the rollback segment mutex */
{
  page_t *header_page;
  page_t *undo_page;
  fil_addr_t last_addr;
  trx_rsegf_t *rseg_header;
  ulint hist_size;

  ut_a(hdr_page_no != page_no);
  ut_ad(!mutex_own(&kernel_mutex));
  ut_ad(mutex_own(&(rseg->mutex)));

  undo_page = trx_undo_page_get(space, page_no, mtr);

  header_page = trx_undo_page_get(space, hdr_page_no, mtr);

  flst_remove(header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST, undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);

  fseg_free_page(header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_FSEG_HEADER, space, page_no, mtr);

  last_addr = flst_get_last(header_page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST, mtr);
  rseg->curr_size--;

  if (in_history) {
    rseg_header = trx_rsegf_get(space, rseg->page_no, mtr);

    hist_size = mtr_read_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, mtr);
    ut_ad(hist_size > 0);
    mlog_write_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, hist_size - 1, MLOG_4BYTES, mtr);
  }

  return last_addr.m_page_no;
}

/** Frees an undo log page when there is also the memory object for the undo
log. */
static void trx_undo_free_page_in_rollback(
  trx_t *trx __attribute__((unused)), /*!< in: transaction */
  trx_undo_t *undo,                   /*!< in: undo log memory copy */
  ulint page_no,                      /*!< in: page number to free: must not be the
                   header page */
  mtr_t *mtr
) /*!< in: mtr which does not have a latch to any
                   undo log page; the caller must have reserved
                   the rollback segment mutex */
{
  ulint last_page_no;

  ut_ad(undo->hdr_page_no != page_no);
  ut_ad(mutex_own(&(trx->undo_mutex)));

  last_page_no = trx_undo_free_page(undo->rseg, false, undo->space, undo->hdr_page_no, page_no, mtr);

  undo->last_page_no = last_page_no;
  undo->size--;
}

/**
 * Empties an undo log header page of undo records for that undo log. Other
 * undo logs may still have records on that page, if it is an update undo log.
 * 
 * @param[in] space The undo log header space.
 * @praam[in] hdr_page_no The undo log header page number.
 * @param[in] hdr_offset The undo log header offset on page.
 * @param[in] mtr The mini-transaction handle.
 */
static void trx_undo_empty_header_page(space_id_t space, page_no_t hdr_page_no, ulint hdr_offset, mtr_t *mtr) {
  auto header_page = trx_undo_page_get(space, hdr_page_no, mtr);
  auto log_hdr = header_page + hdr_offset;
  auto end = trx_undo_page_get_end(header_page, hdr_page_no, hdr_offset);

  mlog_write_ulint(log_hdr + TRX_UNDO_LOG_START, end, MLOG_2BYTES, mtr);
}

void trx_undo_truncate_end(trx_t *trx, trx_undo_t *undo, undo_no_t limit) {
  mtr_t mtr;

  ut_ad(mutex_own(&trx->undo_mutex));
  ut_ad(mutex_own(&trx->rseg->mutex));

  page_t *undo_page;
  trx_undo_rec_t *trunc_here{};

  for (;;) {
    mtr_start(&mtr);

    auto last_page_no = undo->last_page_no;

    undo_page = trx_undo_page_get(undo->space, last_page_no, &mtr);

    auto rec = trx_undo_page_get_last_rec(undo_page, undo->hdr_page_no, undo->hdr_offset);

    for (;;) {
      if (rec == nullptr) {
        if (last_page_no == undo->hdr_page_no) {

          break;
        }

        trx_undo_free_page_in_rollback(trx, undo, last_page_no, &mtr);
        break;
      }

      if (trx_undo_rec_get_undo_no(rec) >= limit) {
        /* Truncate at least this record off, maybe more */
        trunc_here = rec;
      } else {
        break;
      }

      rec = trx_undo_page_get_prev_rec(rec, undo->hdr_page_no, undo->hdr_offset);
    }

    mtr_commit(&mtr);
  }

  if (trunc_here != nullptr) {
    mlog_write_ulint(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, trunc_here - undo_page, MLOG_2BYTES, &mtr);
  }

  mtr_commit(&mtr);
}

void trx_undo_truncate_start(trx_rseg_t *rseg, ulint space, ulint hdr_page_no, ulint hdr_offset, undo_no_t limit) {
  page_t *undo_page;
  trx_undo_rec_t *rec;
  trx_undo_rec_t *last_rec;
  page_no_t page_no;
  mtr_t mtr;

  ut_ad(mutex_own(&rseg->mutex));

  if (limit == 0) {

    return;
  }

loop:
  mtr_start(&mtr);

  rec = trx_undo_get_first_rec(space, hdr_page_no, hdr_offset, RW_X_LATCH, &mtr);

  if (rec == nullptr) {
    /* Already empty */

    mtr_commit(&mtr);

    return;
  }

  undo_page = page_align(rec);

  last_rec = trx_undo_page_get_last_rec(undo_page, hdr_page_no, hdr_offset);
  if (trx_undo_rec_get_undo_no(last_rec) >= limit) {

    mtr_commit(&mtr);

    return;
  }

  page_no = page_get_page_no(undo_page);

  if (page_no == hdr_page_no) {
    trx_undo_empty_header_page(space, hdr_page_no, hdr_offset, &mtr);
  } else {
    trx_undo_free_page(rseg, true, space, hdr_page_no, page_no, &mtr);
  }

  mtr_commit(&mtr);

  goto loop;
}

/**
 * Frees an undo log segment which is not in the history list.
 * 
 * @param[in] undo The undo log memory object.
 */
static void trx_undo_seg_free(trx_undo_t *undo) {
  mtr_t mtr;

  bool finished{};
  auto rseg = undo->rseg;

  do {
    mtr_start(&mtr);

    ut_ad(!mutex_own(&kernel_mutex));

    mutex_enter(&rseg->mutex);

    auto seg_header = trx_undo_page_get(undo->space, undo->hdr_page_no, &mtr) + TRX_UNDO_SEG_HDR;
    auto file_seg = seg_header + TRX_UNDO_FSEG_HEADER;

    finished = fseg_free_step(file_seg, &mtr);

    if (finished) {
      /* Update the rseg header */
      auto rseg_header = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

      trx_rsegf_set_nth_undo(rseg_header, undo->id, FIL_NULL, &mtr);
    }

    mutex_exit(&rseg->mutex);

    mtr_commit(&mtr);

  } while (!finished);
}

/**
 * Creates and initializes an undo log memory object according to the values
 * in the header in file, when the database is started. The memory object is
 * inserted in the appropriate list of rseg.
 * 
 * @param[in] rseg The rollback segment.
 * @param[in] id The slot index within rseg.
 * @param[in] page_no The undo log segment page number.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	own: the undo log memory object
 */
static trx_undo_t *trx_undo_mem_create_at_db_start(trx_rseg_t *rseg, ulint id, page_no_t page_no, mtr_t *mtr) {
  page_t *last_page;
  fil_addr_t last_addr;

#ifdef WITH_XOPEN
  XID xid;
  bool xid_exists = false;
#endif /* WITH_XOPEN */

  if (id >= TRX_RSEG_N_SLOTS) {
    log_fatal("undo->id is ", ulong(id));
  }

  auto undo_page = trx_undo_page_get(rseg->space, page_no, mtr);
  auto page_header = undo_page + TRX_UNDO_PAGE_HDR;
  auto type = mtr_read_ulint(page_header + TRX_UNDO_PAGE_TYPE, MLOG_2BYTES, mtr);
  auto seg_header = undo_page + TRX_UNDO_SEG_HDR;
  auto state = mach_read_from_2(seg_header + TRX_UNDO_STATE);
  auto offset = mach_read_from_2(seg_header + TRX_UNDO_LAST_LOG);
  auto undo_header = undo_page + offset;
  auto trx_id = mtr_read_uint64(undo_header + TRX_UNDO_TRX_ID, mtr);

#ifdef WITH_XOPEN
  xid_exists = mtr_read_ulint(undo_header + TRX_UNDO_XID_EXISTS, MLOG_1BYTE, mtr);

  /* Read X/Open XA transaction identification if it exists, or
  set it to nullptr. */

  memset(&xid, 0, sizeof(xid));
  xid.formatID = -1;

  if (xid_exists == true) {
    trx_undo_read_xid(undo_header, &xid);
  }
#endif /* WITH_XOPEN */

  mutex_enter(&rseg->mutex);

#ifdef WITH_XOPEN
  auto undo = trx_undo_mem_create(rseg, id, type, trx_id, &xid, page_no, offset);
#else
  auto undo = trx_undo_mem_create(rseg, id, type, trx_id, nullptr, page_no, offset);
#endif /* WITH_XOPEN */

  mutex_exit(&rseg->mutex);

  undo->dict_operation = mtr_read_ulint(undo_header + TRX_UNDO_DICT_TRANS, MLOG_1BYTE, mtr);

  undo->table_id = mtr_read_uint64(undo_header + TRX_UNDO_TABLE_ID, mtr);
  undo->state = state;
  undo->size = flst_get_len(seg_header + TRX_UNDO_PAGE_LIST, mtr);

  rec_t *rec;

  /* If the log segment is being freed, the page list is inconsistent! */
  if (state == TRX_UNDO_TO_FREE) {

    goto add_to_list;
  }

  last_addr = flst_get_last(seg_header + TRX_UNDO_PAGE_LIST, mtr);

  undo->last_page_no = last_addr.m_page_no;
  undo->top_page_no = last_addr.m_page_no;

  last_page = trx_undo_page_get(rseg->space, undo->last_page_no, mtr);

  rec = trx_undo_page_get_last_rec(last_page, page_no, offset);

  if (rec == nullptr) {
    undo->empty = true;
  } else {
    undo->empty = false;
    undo->top_offset = rec - last_page;
    undo->top_undo_no = trx_undo_rec_get_undo_no(rec);
  }

add_to_list:
  if (type == TRX_UNDO_INSERT) {
    if (state != TRX_UNDO_CACHED) {
      UT_LIST_ADD_LAST(rseg->insert_undo_list, undo);
    } else {
      UT_LIST_ADD_LAST(rseg->insert_undo_cached, undo);
    }
  } else {
    ut_ad(type == TRX_UNDO_UPDATE);
    if (state != TRX_UNDO_CACHED) {
      UT_LIST_ADD_LAST(rseg->update_undo_list, undo);
    } else {
      UT_LIST_ADD_LAST(rseg->update_undo_cached, undo);
    }
  }

  return undo;
}

ulint trx_undo_lists_init(ib_recovery_t recovery, trx_rseg_t *rseg) {
  UT_LIST_INIT(rseg->update_undo_list);
  UT_LIST_INIT(rseg->update_undo_cached);
  UT_LIST_INIT(rseg->insert_undo_list);
  UT_LIST_INIT(rseg->insert_undo_cached);

  mtr_t mtr;

  mtr_start(&mtr);

  auto rseg_header = trx_rsegf_get_new(rseg->space, rseg->page_no, &mtr);

  ulint size = 0;

  for (ulint i = 0; i < TRX_RSEG_N_SLOTS; i++) {
    auto page_no = trx_rsegf_get_nth_undo(rseg_header, i, &mtr);

    /* In forced recovery: try to avoid operations which look
    at database pages; undo logs are rapidly changing data, and
    the probability that they are in an inconsistent state is
    high */

    if (page_no != FIL_NULL && recovery < IB_RECOVERY_NO_UNDO_LOG_SCAN) {

      auto undo = trx_undo_mem_create_at_db_start(rseg, i, page_no, &mtr);
      size += undo->size;

      mtr_commit(&mtr);

      mtr_start(&mtr);

      rseg_header = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);
    }
  }

  mtr_commit(&mtr);

  return size;
}

/**
 * Creates and initializes an undo log memory object.
 * 
 * @param[in] rseg The rollback segment.
 * @param[in] id The slot index within rseg.
 * @param[in] type The type of the log: TRX_UNDO_INSERT or TRX_UNDO_UPDATE.
 * @param[in] trx_id The transaction id for which the undo log is created.
 * @param[in] xid The X/Open transaction identification.
 * @param[in] page_no The undo log header page number.
 * @param[in] offset The undo log header byte offset on page.
 * 
 * @return	own: the undo log memory object
 */
static trx_undo_t *trx_undo_mem_create(
  trx_rseg_t *rseg,
  ulint id,
  ulint type,
  trx_id_t trx_id,
  const XID *xid,
  page_no_t page_no,
  ulint offset)
{
  ut_ad(mutex_own(&rseg->mutex));

  if (id >= TRX_RSEG_N_SLOTS) {
    log_fatal("undo->id is ", id);
  }

  auto undo = static_cast<trx_undo_t *>(mem_alloc(sizeof(trx_undo_t)));

  if (undo == nullptr) {

    return nullptr;
  }

  undo->id = id;
  undo->type = type;
  undo->state = TRX_UNDO_ACTIVE;
  undo->del_marks = false;
  undo->trx_id = trx_id;
#ifdef WITH_XOPEN
  undo->xid = *xid;
#endif /* WITH_XOPEN */

  undo->dict_operation = false;

  undo->rseg = rseg;

  undo->space = rseg->space;
  undo->hdr_page_no = page_no;
  undo->hdr_offset = offset;
  undo->last_page_no = page_no;
  undo->size = 1;

  undo->empty = true;
  undo->top_page_no = page_no;
  undo->guess_block = nullptr;

  return undo;
}

/**
 * Initializes a cached undo log object for new use.
 * 
 * @param[in] undo The undo log memory object.
 * @param[in] trx_id The transaction id for which the undo log is created.
 * @param[in] xid The X/Open transaction identification.
 * @param[in] offset The undo log header byte offset on page.
 */
static void trx_undo_mem_init_for_reuse(
  trx_undo_t *undo,
  trx_id_t trx_id,
#ifdef WITH_XOPEN
  const XID *xid,
#endif /* WITH_XOPEN */
  ulint offset)
{
  ut_ad(mutex_own(&(undo->rseg->mutex)));

  if (unlikely(undo->id >= TRX_RSEG_N_SLOTS)) {
    log_fatal("undo->id is ", undo->id);
  }

  undo->state = TRX_UNDO_ACTIVE;
  undo->del_marks = false;
  undo->trx_id = trx_id;
#ifdef WITH_XOPEN
  undo->xid = *xid;
#endif /* WITH_XOPEN */

  undo->dict_operation = false;

  undo->hdr_offset = offset;
  undo->empty = true;
}

void trx_undo_mem_free(trx_undo_t *undo) {
  if (undo->id >= TRX_RSEG_N_SLOTS) {
    log_fatal("undo->id is ", undo->id);
  }

  mem_free(undo);
}

/**
 * Creates a new undo log.
 * 
 * @param[in] trx The transaction.
 * @param[in] rseg The rollback segment memory copy.
 * @param[in] type The type of the log: TRX_UNDO_INSERT or TRX_UNDO_UPDATE.
 * @param[in] trx_id The transaction id for which the undo log is created.
 * @param[in] xid The X/Open transaction identification.
 * @param[out] undo The new undo log object, undefined if did not succeed.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return DB_SUCCESS or DB_TOO_MANY_CONCURRENT_TRXS, DB_OUT_OF_FILE_SPACE
 *  or DB_OUT_OF_MEMORY
 */
static db_err trx_undo_create(
  trx_t *trx,
  trx_rseg_t *rseg,
  ulint type,
  trx_id_t trx_id,
  const XID *xid,
  trx_undo_t **undo,
  mtr_t *mtr)
{
  ut_ad(mutex_own(&rseg->mutex));

  if (rseg->curr_size == rseg->max_size) {

    return DB_OUT_OF_FILE_SPACE;
  }

  ++rseg->curr_size;

  ulint id;
  page_t *undo_page;
  auto rseg_header = trx_rsegf_get(rseg->space, rseg->page_no, mtr);
  auto err = trx_undo_seg_create(rseg, rseg_header, type, &id, &undo_page, mtr);

  if (err != DB_SUCCESS) {
    /* Did not succeed */

    --rseg->curr_size;

    return err;
  }

  auto page_no = page_get_page_no(undo_page);
  auto offset = trx_undo_header_create(undo_page, trx_id, mtr);

#ifdef WITH_XOPEN
  if (trx->m_support_xa) {
    trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, mtr);
  }
#endif /* WITH_XOPEN */

  *undo = trx_undo_mem_create(rseg, id, type, trx_id, xid, page_no, offset);

  if (*undo == nullptr) {

    err = DB_OUT_OF_MEMORY;
  }

  return err;
}

/**
 * Reuses a cached undo log.
 * 
 * @param[in] trx The transaction.
 * @param[in] rseg The rollback segment memory copy.
 * @param[in] type The type of the log: TRX_UNDO_INSERT or TRX_UNDO_UPDATE.
 * @param[in] trx_id The transaction id for which the undo log is used.
 * @param[in] xid The X/Open transaction identification.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	the undo log memory object, nullptr if none cached
 */
static trx_undo_t *trx_undo_reuse_cached(
  trx_t *trx,
  trx_rseg_t *rseg,
  ulint type,
  trx_id_t trx_id,
#ifdef WITH_XOPEN
  const XID *xid,
#endif /* WITH_XOPEN */
  mtr_t *mtr)
{
  ulint offset;
  trx_undo_t *undo;
  page_t *undo_page;

  ut_ad(mutex_own(&(rseg->mutex)));

  if (type == TRX_UNDO_INSERT) {

    undo = UT_LIST_GET_FIRST(rseg->insert_undo_cached);

    if (undo == nullptr) {

      return nullptr;
    }

    UT_LIST_REMOVE(rseg->insert_undo_cached, undo);
  } else {
    ut_ad(type == TRX_UNDO_UPDATE);

    undo = UT_LIST_GET_FIRST(rseg->update_undo_cached);

    if (undo == nullptr) {

      return nullptr;
    }

    UT_LIST_REMOVE(rseg->update_undo_cached, undo);
  }

  ut_ad(undo->size == 1);

  if (undo->id >= TRX_RSEG_N_SLOTS) {
    log_fatal("undo->id is ", undo->id);
  }

  undo_page = trx_undo_page_get(undo->space, undo->hdr_page_no, mtr);

  if (type == TRX_UNDO_INSERT) {
    offset = trx_undo_insert_header_reuse(undo_page, trx_id, mtr);

#ifdef WITH_XOPEN
    if (trx->m_support_xa) {
      trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, mtr);
    }
#endif /* WITH_XOPEN */
  } else {
    ut_a(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE);

    offset = trx_undo_header_create(undo_page, trx_id, mtr);

#ifdef WITH_XOPEN
    if (trx->m_support_xa) {
      trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, mtr);
    }
#endif /* WITH_XOPEN */
  }

#ifdef WITH_XOPEN
  trx_undo_mem_init_for_reuse(undo, trx_id, xid, offset);
#else
  trx_undo_mem_init_for_reuse(undo, trx_id, offset);
#endif /* WITH_XOPEN */

  return undo;
}

/**
 * Marks an undo log header as a header of a data dictionary operation
 * transaction.
 * @param[in] trx The transaction.
 * @param[in] undo The undo log memory object.
 * @param[in] mtr The mini-transaction handle.
 */
static void trx_undo_mark_as_dict_operation(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) {
  auto hdr_page = trx_undo_page_get(undo->space, undo->hdr_page_no, mtr);

  switch (trx_get_dict_operation(trx)) {
    case TRX_DICT_OP_NONE:
      ut_error;
    case TRX_DICT_OP_INDEX:
      /* Do not discard the table on recovery. */
      undo->table_id = 0;
      break;
    case TRX_DICT_OP_TABLE:
      undo->table_id = trx->table_id;
      break;
  }

  mlog_write_ulint(hdr_page + undo->hdr_offset + TRX_UNDO_DICT_TRANS, true, MLOG_1BYTE, mtr);

  mlog_write_uint64(hdr_page + undo->hdr_offset + TRX_UNDO_TABLE_ID, undo->table_id, mtr);

  undo->dict_operation = true;
}

db_err trx_undo_assign_undo(trx_t *trx, ulint type) {
  ut_ad(trx->rseg);

  auto rseg = trx->rseg;

  ut_ad(mutex_own(&(trx->undo_mutex)));

  mtr_t mtr;

  mtr_start(&mtr);

  ut_ad(!mutex_own(&kernel_mutex));

  mutex_enter(&rseg->mutex);

#ifdef WITH_XOPEN
  auto undo = trx_undo_reuse_cached(trx, rseg, type, trx->m_id, &trx->m_xid, &mtr);
#else
  auto undo = trx_undo_reuse_cached(trx, rseg, type, trx->id, &mtr);
#endif /* WITH_XOPEN */

  if (undo == nullptr) {
#ifdef WITH_XOPEN
    auto err = trx_undo_create(trx, rseg, type, trx->m_id, &trx->m_xid, &undo, &mtr);
#else
    auto err = trx_undo_create(trx, rseg, type, trx->id, nullptr, &undo, &mtr);
#endif /* WITH_OPEN */

    if (err != DB_SUCCESS) {

      mutex_exit(&rseg->mutex);
      mtr_commit(&mtr);

      return err;
    }
  }

  if (type == TRX_UNDO_INSERT) {
    UT_LIST_ADD_FIRST(rseg->insert_undo_list, undo);
    ut_ad(trx->insert_undo == nullptr);
    trx->insert_undo = undo;
  } else {
    UT_LIST_ADD_FIRST(rseg->update_undo_list, undo);
    ut_ad(trx->update_undo == nullptr);
    trx->update_undo = undo;
  }

  if (trx_get_dict_operation(trx) != TRX_DICT_OP_NONE) {
    trx_undo_mark_as_dict_operation(trx, undo, &mtr);
  }

  mutex_exit(&rseg->mutex);

  mtr_commit(&mtr);

  return DB_SUCCESS;
}

page_t *trx_undo_set_state_at_finish(trx_rseg_t *rseg, trx_t *trx __attribute__((unused)), trx_undo_t *undo, mtr_t *mtr) {
  ulint state;

  ut_ad(trx);
  ut_ad(undo);
  ut_ad(mtr);
  ut_ad(mutex_own(&rseg->mutex));

  if (undo->id >= TRX_RSEG_N_SLOTS) {
    ib_logger(ib_stream, "Error: undo->id is %lu\n", (ulong)undo->id);
    ut_error;
  }

  auto undo_page = trx_undo_page_get(undo->space, undo->hdr_page_no, mtr);

  auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  auto page_hdr = undo_page + TRX_UNDO_PAGE_HDR;

  if (undo->size == 1 && mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE) < TRX_UNDO_PAGE_REUSE_LIMIT) {

    /* This is a heuristic to avoid the problem of all UNDO
    slots ending up in one of the UNDO lists. Previously if
    the server crashed with all the slots in one of the lists,
    transactions that required the slots of a different type
    would fail for lack of slots. */

    if (UT_LIST_GET_LEN(rseg->update_undo_list) < 500 && UT_LIST_GET_LEN(rseg->insert_undo_list) < 500) {

      state = TRX_UNDO_CACHED;
    } else {
      state = TRX_UNDO_TO_FREE;
    }

  } else if (undo->type == TRX_UNDO_INSERT) {

    state = TRX_UNDO_TO_FREE;
  } else {
    state = TRX_UNDO_TO_PURGE;
  }

  undo->state = state;

  mlog_write_ulint(seg_hdr + TRX_UNDO_STATE, state, MLOG_2BYTES, mtr);

  return undo_page;
}

page_t *trx_undo_set_state_at_prepare(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) {
  if (undo->id >= TRX_RSEG_N_SLOTS) {
    ib_logger(ib_stream, "Error: undo->id is %lu\n", (ulong)undo->id);
    ut_error;
  }

  auto undo_page = trx_undo_page_get(undo->space, undo->hdr_page_no, mtr);
  auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  /*------------------------------*/
  undo->state = TRX_UNDO_PREPARED;
#ifdef WITH_XOPEN
  undo->xid = trx->m_xid;
#endif /* WITH_XOPEN */
  /*------------------------------*/

  mlog_write_ulint(seg_hdr + TRX_UNDO_STATE, undo->state, MLOG_2BYTES, mtr);

  auto offset = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
  auto undo_header = undo_page + offset;

  mlog_write_ulint(undo_header + TRX_UNDO_XID_EXISTS, true, MLOG_1BYTE, mtr);

#ifdef WITH_XOPEN
  trx_undo_write_xid(undo_header, &undo->xid, mtr);
#else
  trx_undo_write_xid(undo_header, mtr);
#endif /* WITH_XOPEN */

  return undo_page;
}

void trx_undo_update_cleanup(trx_t *trx, page_t *undo_page, mtr_t *mtr) {
  auto undo = trx->update_undo;
  auto rseg = trx->rseg;

  ut_ad(mutex_own(&rseg->mutex));

  trx_purge_add_update_undo_to_history(trx, undo_page, mtr);

  UT_LIST_REMOVE(rseg->update_undo_list, undo);

  trx->update_undo = nullptr;

  if (undo->state == TRX_UNDO_CACHED) {

    UT_LIST_ADD_FIRST(rseg->update_undo_cached, undo);
  } else {
    ut_ad(undo->state == TRX_UNDO_TO_PURGE);

    trx_undo_mem_free(undo);
  }
}

void trx_undo_insert_cleanup(trx_t *trx) {
  auto undo = trx->insert_undo;
  ut_ad(undo != nullptr);

  auto rseg = trx->rseg;

  mutex_enter(&rseg->mutex);

  UT_LIST_REMOVE(rseg->insert_undo_list, undo);

  trx->insert_undo = nullptr;

  if (undo->state == TRX_UNDO_CACHED) {

    UT_LIST_ADD_FIRST(rseg->insert_undo_cached, undo);
  } else {
    ut_ad(undo->state == TRX_UNDO_TO_FREE);

    /* Delete first the undo log segment in the file */

    mutex_exit(&rseg->mutex);

    trx_undo_seg_free(undo);

    mutex_enter(&rseg->mutex);

    ut_ad(rseg->curr_size > undo->size);

    rseg->curr_size -= undo->size;

    trx_undo_mem_free(undo);
  }

  mutex_exit(&rseg->mutex);
}
