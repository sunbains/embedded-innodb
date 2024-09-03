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

/** @file include/trx0undo.h
Transaction undo log

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "mtr0mtr.h"
#include "page0types.h"
#include "trx0sys.h"
#include "trx0types.h"
#include "trx0xa.h"
#include "data0type.h"
#include "page0page.h"


/* Types of an undo log segment */

/** Contains undo entries for inserts */
constexpr ulint TRX_UNDO_INSERT = 1;

/** Contains undo entries for updates and delete markings: in short,
 modifys (the name 'UPDATE' is a historical relic) */
constexpr ulint TRX_UNDO_UPDATE = 2;

/* States of an undo log segment */
/* contains an undo log of an active transaction */
constexpr ulint TRX_UNDO_ACTIVE = 1;

/* Cached for quick reuse */
constexpr ulint TRX_UNDO_CACHED = 2;

/* Insert undo segment can be freed */
constexpr ulint TRX_UNDO_TO_FREE = 3;

/** Update undo segment will not be reused: it can be freed in
 * purge when all undo data in it is removed */
constexpr ulint TRX_UNDO_TO_PURGE = 4;

/* Contains an undo log of an prepared transaction */
constexpr ulint TRX_UNDO_PREPARED = 5;

/** The offset of the undo log page header on pages of the undo log */
constexpr auto TRX_UNDO_PAGE_HDR = FSEG_PAGE_DATA;

/*-------------------------------------------------------------*/
/** Transaction undo log page header offsets */
/* @{ */

/** TRX_UNDO_INSERT or TRX_UNDO_UPDATE */
constexpr ulint TRX_UNDO_PAGE_TYPE = 0;

/** Byte offset where the undo log records for the LATEST transaction start
on this page (remember that in an update undo log, the first page can contain
several undo logs) */
constexpr ulint TRX_UNDO_PAGE_START = 2;

/** On each page of the undo log this field contains the byte offset of the
first free byte on the page */
constexpr ulint TRX_UNDO_PAGE_FREE = 4;

/** The file list node in the chain of undo log pages */
constexpr ulint TRX_UNDO_PAGE_NODE = 6;

/*-------------------------------------------------------------*/
/** Size of the transaction undo log page header, in bytes */
constexpr ulint TRX_UNDO_PAGE_HDR_SIZE = 6 + FLST_NODE_SIZE;

/* @} */

/** An update undo segment with just one page can be reused if it has
at most this many bytes used; we must leave space at least for one new undo
log header on the page */
constexpr auto TRX_UNDO_PAGE_REUSE_LIMIT  = 3 * UNIV_PAGE_SIZE / 4;

/* An update undo log segment may contain several undo logs on its first page
if the undo logs took so little space that the segment could be cached and
reused. All the undo log headers are then on the first page, and the last one
owns the undo log records on subsequent pages if the segment is bigger than
one page. If an undo log is stored in a segment, then on the first page it is
allowed to have zero undo records, but if the segment extends to several
pages, then all the rest of the pages must contain at least one undo log
record. */

/** The offset of the undo log segment header on the first page of the undo
log segment */
constexpr auto TRX_UNDO_SEG_HDR = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;

/** Undo log segment header */
/* @{ */
/*-------------------------------------------------------------*/

/** TRX_UNDO_ACTIVE, ... */
constexpr ulint TRX_UNDO_STATE = 0;

/** Offset of the last undo log header on the segment header page, 0 if none */
constexpr ulint TRX_UNDO_LAST_LOG = 2;

/** Header for the file segment which the undo log segment occupies */
constexpr ulint TRX_UNDO_FSEG_HEADER = 4;

/** Base node for the list of pages in the undo log segment; defined only
 * on the undo log segment's first page */
constexpr ulint TRX_UNDO_PAGE_LIST = 4 + FSEG_HEADER_SIZE;

/*-------------------------------------------------------------*/
/** Size of the undo log segment header */
constexpr ulint TRX_UNDO_SEG_HDR_SIZE = 4 + FSEG_HEADER_SIZE + FLST_BASE_NODE_SIZE;
/* @} */

/** The undo log header. There can be several undo log headers on the first
page of an update undo log segment. */
/* @{ */
/*-------------------------------------------------------------*/

/** Transaction id */
constexpr ulint TRX_UNDO_TRX_ID = 0;

/** Transaction number of the transaction; defined only if the log is
 * in a history list */
constexpr ulint TRX_UNDO_TRX_NO = 8;

 /** Defined only in an update undo log: true if the transaction may have
  * done delete markings of records, and thus purge is necessary */
constexpr ulint TRX_UNDO_DEL_MARKS = 16;

/** Offset of the first undo log record of this log on the header page;
purge may remove undo log record from the log start, and therefore this
is not necessarily the same as this log header end offset */
constexpr ulint TRX_UNDO_LOG_START = 18;

/** true if undo log header includes X/Open XA transaction identification XID */
constexpr ulint TRX_UNDO_XID_EXISTS = 20;

/** true if the transaction is a table create, index create, or drop transaction:
 * in recovery the transaction cannot be rolled back in the usual way: a 'rollback'
 * rather means dropping the created or dropped table, if it still exists */
constexpr ulint TRX_UNDO_DICT_TRANS = 21;

/** Id of the table if the preceding field is true */
constexpr ulint TRX_UNDO_TABLE_ID = 22;

/** Offset of the next undo log header on this page, 0 if none */
constexpr ulint TRX_UNDO_NEXT_LOG = 30;

/** Offset of the previous undo log header on this page, 0 if none */
constexpr ulint TRX_UNDO_PREV_LOG = 32;

/** If the log is put to the history list, the file list node is here */
constexpr ulint TRX_UNDO_HISTORY_NODE = 34;

/*-------------------------------------------------------------*/
/** Size of the undo log header without XID information */
constexpr auto TRX_UNDO_LOG_HDR_SIZE = FLST_NODE_SIZE + 34;

/* Note: the writing of the undo log old header is coded by a log record
MLOG_UNDO_HDR_CREATE or MLOG_UNDO_HDR_REUSE. The appending of an XID to the
header is logged separately. In this sense, the XID is not really a member
of the undo log header. TODO: do not append the XID to the log header if XA
is not needed by the user. The XID wastes about 150 bytes of space in every
undo log. In the history list we may have millions of undo logs, which means
quite a large overhead. */

/** X/Open XA Transaction Identification (XID) */
/* @{ */
/** xid_t::formatID */
constexpr auto TRX_UNDO_XA_FORMAT = TRX_UNDO_LOG_HDR_SIZE;

/** xid_t::gtrid_length */
constexpr auto TRX_UNDO_XA_TRID_LEN = TRX_UNDO_XA_FORMAT + 4;

/** xid_t::bqual_length */
constexpr auto TRX_UNDO_XA_BQUAL_LEN = TRX_UNDO_XA_TRID_LEN + 4;

/** Distributed transaction identifier data */
constexpr auto TRX_UNDO_XA_XID = TRX_UNDO_XA_BQUAL_LEN + 4;

/*--------------------------------------------------------------*/
/*!< Total size of the undo log header with the XA XID */
constexpr auto TRX_UNDO_LOG_XA_HDR_SIZE = TRX_UNDO_XA_XID + XIDDATASIZE;




/** Transaction undo log memory object; this is protected by the undo_mutex
in the corresponding transaction object */
struct trx_undo_t {
  /** Undo log slot number within the rollback segment */
  ulint m_id;

  /** TRX_UNDO_INSERT or TRX_UNDO_UPDATE */
  ulint m_type;

  /** State of the corresponding undo log segment */
  ulint m_state;

  /** Relevant only in an update undo log: this is true if the transaction
   * may have delete marked records, because of a delete of a row or an
   * update of an indexed field; purge is then necessary; also true if
   * the transaction has updated an externally stored field */
  bool m_del_marks;

  /** Id of the trx assigned to the undo log */
  trx_id_t m_trx_id;

  /** X/Open XA transaction identification */
  IF_XA(XID m_xid;)

  /** true if a dict operation trx */
  bool m_dict_operation;

  /** If a dict operation, then the table id */
  uint64_t m_table_id;

  /** rseg where the undo log belongs */
  trx_rseg_t *m_rseg;

  /** Space id where the undo log placed */
  space_id_t m_space;

  /** Page number of the header page in the undo log */
  page_no_t m_hdr_page_no;

  /** Header offset of the undo log on the page */
  ulint m_hdr_offset;

  /** Page number of the last page in the undo log; this may
   * differ from top_page_no during a rollback */
  page_no_t m_last_page_no;

  /** Current size in pages */
  ulint m_size;

  /** true if the stack of undo log records is currently empty */
  ulint m_empty;

  /** Page number where the latest undo log record was catenated;
   * during rollback the page from which the latest undo record
   * was chosen */
  page_no_t m_top_page_no;

  /** offset of the latest undo record, i.e., the topmost element
   * in the undo log if we think of it as a stack */
  ulint m_top_offset;

  /** Undo number of the latest record */
  undo_no_t m_top_undo_no;

  /** Guess for the buffer block where the top page might reside */
  buf_block_t *m_guess_block;

  /** Undo log instances in the rollback segment are chained into lists */
  UT_LIST_NODE_T(trx_undo_t) m_undo_list;
};

UT_LIST_NODE_GETTER_DEFINITION(trx_undo_t, m_undo_list);

struct Undo {
  /** Constructor
   * 
   * @param[in] fsp Use this for file space management.
   */
  explicit Undo(FSP *fsp) : m_fsp(fsp) {}

  /**
   * Create an undo instance.
   * 
   * @param[in] fsp File space management.
   */
  static Undo* create(FSP *fsp) noexcept;

  /** 
   * Destroy the undo instance.
   * 
   * @param[in,owm] undo Undo instance.
   */
  static void destroy(Undo *&undo) noexcept;

 /**
   * Gets an undo log page and x-latches it.
   * 
   * @param[in] space_id Tablspace ID.
   * @param[in] page_no Page number
   * @param[in,out] mtr Mini-transaction covering the page get.
   * 
   * @return	pointer to page x-latched
   */
  inline page_t *page_get(space_id_t space_id, page_no_t page_no, mtr_t *mtr) noexcept {
    Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { space_id, page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto block = m_fsp->m_buf_pool->get(req, nullptr);
    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_UNDO_PAGE));

    return block->get_frame();
  }

  /**
   * Gets an undo log page and s-latches it.
   * 
   * @param[in] space_id Tablspace ID.
   * @param[in] page_no Page number
   * @param[in,out] mtr Mini-transaction covering the page get.
   * 
   * @return	pointer to page s-latched
   */
  inline page_t *page_get_s_latched(space_id_t  space_id, page_no_t page_no, mtr_t *mtr) noexcept {
    Buf_pool::Request req {
      .m_rw_latch = RW_S_LATCH,
      .m_page_id = { space_id, page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto block = m_fsp->m_buf_pool->get(req, nullptr);
    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_UNDO_PAGE));

    return block->get_frame();
  }

  /**
   * Gets the next record in an undo log.
   * 
   * @param[in] rec undo record
   * @param[in] page_no undo log header page number
   * @param[in] offset undo log header offset on page
   * @param[in] mtr mini-transaction handle
   * 
   * @return	undo log record, the page s-latched, NULL if none
   */
  trx_undo_rec_t *get_next_rec(trx_undo_rec_t *rec, page_no_t page_no, ulint offset, mtr_t *mtr) noexcept;

  /**
   * Gets the previous record in an undo log.
   * 
   * @param[in] rec Undo record
   * @param[in] page_no Undo log header page number
   * @param[in] offset Undo log header offset on page
   * @param[in] mtr Mini-transaction handle
   * 
   * @return	undo log record, the page s-latched, NULL if none
   */
  trx_undo_rec_t *get_prev_rec(trx_undo_rec_t *rec, page_no_t page_no, ulint offset, mtr_t *mtr) noexcept;

  /**
   * Gets the first record in an undo log.
   * 
   * @param[in] space Undo log header space
   * @param[in] page_no Undo log header page number
   * @param[in] offset Undo log header offset on page
   * @param[in] mode Latching mode: RW_S_LATCH or RW_X_LATCH
   * 
   * @return	undo log record, the page latched, NULL if none
   */
  trx_undo_rec_t *get_first_rec(space_id_t space, page_no_t page_no, ulint offset, ulint mode, mtr_t *mtr) noexcept;

  /**
   * Tries to add a page to the undo log segment where the undo log is placed.
   * 
   * @param[in] trx Transaction
   * @param[in] undo Undo log memory object
   * @param[in] mtr MTR which does not have a latch to any undo log page; the caller
   *  must have reserved the rollback segment mutex
   * 
   * @return	page number if success, else FIL_NULL
   */
  ulint add_page(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) noexcept;

  /**
   * Truncates an undo log from the start. This function is used during a purge
   * operation.
   * 
   * @param[in] rseg Rollback segment
   * @param[in] space Space id of the log
   * @param[in] hdr_page_no Header page number
   * @param[in] hdr_offset Header offset on the page
   * @param[in] limit All undo pages with undo numbers < this value should be truncated; 
   *  NOTE that the function only frees whole pages; the header page is not freed, but
   *  emptied, if all the records there are < limit
   */
  void truncate_start(trx_rseg_t *rseg, space_id_t space, page_no_t hdr_page_no, ulint hdr_offset, undo_no_t limit) noexcept;

  /**
   * Truncates an undo log from the end. This function is used during a rollback
   * to free space from an undo log.
   * 
   * @param[in] trx Transaction whose undo log it is
   * @param[in] undo Undo log
   * @param[in] limit All undo records with undo number >= this value should be truncated
   */
  void truncate_end(trx_t *trx, trx_undo_t *undo, undo_no_t limit) noexcept;

  /**
   * Initializes the undo log lists for a rollback segment memory copy.
   * This function is only called when the database is started or a new
   * rollback segment created.
   * 
   * @param[in] recovery Recovery flag
   * @param[in] rseg Rollback segment memory object
   * 
   * @return	the combined size of undo log segments in pages
   */
  ulint lists_init(ib_recovery_t recovery, trx_rseg_t *rseg) noexcept;

  /**
   * Assigns an undo log for a transaction. A new undo log is created or a cached
   * undo log reused.
   * 
   * @param[in] trx Transaction
   * @param[in] type TRX_UNDO_INSERT or TRX_UNDO_UPDATE
   * 
   * @return DB_SUCCESS if undo log assign successful, possible error codes
   *  are: DB_TOO_MANY_CONCURRENT_TRXS DB_OUT_OF_FILE_SPACE DB_OUT_OF_MEMORY
   */
  db_err assign_undo(trx_t *trx, ulint type) noexcept;

  /**
   * Sets the state of the undo log segment at a transaction finish.
   * 
   * @param[in] rseg Rollback segment memory object
   * @param[in] trx Transaction
   * @param[in] undo Undo log memory copy
   * @param[in] mtr Mini-transaction handle
   * 
   * @return	undo log segment header page, x-latched
   */
  page_t *set_state_at_finish(trx_rseg_t *rseg, trx_t *trx, trx_undo_t *undo, mtr_t *mtr) noexcept;

  /**
   * Sets the state of the undo log segment at a transaction prepare.
   * 
   * @param[in] trx Transaction
   * @param[in] undo Undo log memory copy
   * @param[in] mtr Mini-transaction handle
   * 
   * @return	undo log segment header page, x-latched
   */
  page_t *set_state_at_prepare(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) noexcept;

  /**
   * Adds the update undo log header as the first in the history list, and
   * frees the memory object, or puts it to the list of cached update undo log
   * segments.
   * 
   * @param[in] trx Transaction
   * @param[in] undo_page Update undo log header page, x-latched
   * @param[in] mtr Mini-transaction handle
   */
  void update_cleanup(trx_t *trx, page_t *undo_page, mtr_t *mtr) noexcept;

  /**
   * Frees or caches an insert undo log after a transaction commit or rollback.
   * Knowledge of inserts is not needed after a commit or rollback, therefore
   * the data can be discarded.
   * 
   * @param[in] trx Transaction
   */
  void insert_cleanup(trx_t *trx) noexcept;

  /**
   * Parses the redo log entry of an undo log page initialization.
   * 
   * @param[in] ptr Buffer
   * @param[in] end_ptr Buffer end
   * @param[in] page Page or NULL
   * @param[in] mtr Mini-transaction handle, cane be nullptr
   * 
   * @return	end of log record or NULL
   */
  static byte *parse_page_init(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) noexcept;

  /**
   * Parses the redo log entry of an undo log page header create or reuse.
   * 
   * @param[in] type MLOG_UNDO_HDR_CREATE or MLOG_UNDO_HDR_REUSE
   * @param[in] ptr Buffer
   * @param[in] end_ptr Buffer end
   * @param[in] page Page or NULL
   * @param[in] mtr Mini-transaction handle, can be nullptr
   * 
   * @return	end of log record or NULL
   */
  static byte *parse_page_header(ulint type, byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) noexcept;

  /**
   * Parses the redo log entry of an undo log page header discard.
   * 
   * @param[in] ptr Buffer
   * @param[in] end_ptr Buffer end
   * @param[in] page Page or NULL
   * @param[in] mtr Mini-transaction handle, can be nullptr
   * 
   * @return	end of log record or NULL
   */
  static byte *parse_discard_latest(byte *ptr, IF_DEBUG(byte *end_ptr,) page_t *page, mtr_t *mtr) noexcept;

  /**
   * Frees an undo log memory copy.
   * 
   * @param[in,own] undo Undo log memory object to be freed
   */
  static void delete_undo(trx_undo_t *&undo) noexcept;

private:

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
  trx_undo_rec_t *get_prev_rec_from_prev_page(trx_undo_rec_t *rec, page_no_t page_no, ulint offset, mtr_t *mtr) noexcept;

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
  trx_undo_rec_t *get_next_rec_from_next_page(space_id_t space, page_t *undo_page, page_no_t page_no, ulint offset, ulint mode, mtr_t *mtr) noexcept;

  /**
   * Frees an undo log page that is not the header page.
   * 
   * @param[in] rseg The rollback segment.
   * @param[in] in_history True if the undo log is in the history list.
   * @param[in] space The undo log header space.
   * @param[in] hdr_page_no The undo log header page number.
   * @param[in] page_no The page number to free: must not be the header page.
   * @param[in] mtr The mini-transaction handle. The caller must have reserved the rollback segment mutex.
   * 
   * @return	last page number in remaining log
   */
  ulint free_page(trx_rseg_t *rseg, bool in_history, space_id_t space, page_no_t hdr_page_no, page_no_t page_no, mtr_t *mtr) noexcept;

  /**
   * Frees an undo log page when there is also the memory object for the undo log.
   * 
   * @param[in] trx The transaction.
   * @param[in] undo The undo log memory copy.
   * @param[in] page_no The page number to free: must not be the header page.
   * @param[in] mtr The mini-transaction handle. The caller must have reserved the rollback segment mutex.
   */
  void free_page_in_rollback(trx_t *trx __attribute__((unused)), trx_undo_t *undo, page_no_t page_no, mtr_t *mtr) noexcept;

  /**
   * Empties an undo log header page of undo records for that undo log. Other
   * undo logs may still have records on that page, if it is an update undo log.
   * 
   * @param[in] space The undo log header space.
   * @praam[in] hdr_page_no The undo log header page number.
   * @param[in] hdr_offset The undo log header offset on page.
   * @param[in] mtr The mini-transaction handle.
   */
  void empty_header_page(space_id_t space, page_no_t hdr_page_no, ulint hdr_offset, mtr_t *mtr) noexcept;

  /**
   * Frees an undo log segment which is not in the history list.
   * 
   * @param[in] undo The undo log memory object.
   */
  void seg_free(trx_undo_t *undo) noexcept;

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
  trx_undo_t *mem_create_at_db_start(trx_rseg_t *rseg, ulint id, page_no_t page_no, mtr_t *mtr) noexcept;

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
  trx_undo_t *reuse_cached(trx_t *trx, trx_rseg_t *rseg, ulint type, trx_id_t trx_id, IF_XA(const XID *xid,) mtr_t *mtr) noexcept;

  /**
   * Marks an undo log header as a header of a data dictionary operation
   * transaction.
   * @param[in] trx The transaction.
   * @param[in] undo The undo log memory object.
   * @param[in] mtr The mini-transaction handle.
   */
  void mark_as_dict_operation(trx_t *trx, trx_undo_t *undo, mtr_t *mtr) noexcept;

public:
  FSP* m_fsp{};
};

/* @} */

static_assert(DATA_ROLL_PTR_LEN == 7, "error DATA_ROLL_PTR_LEN != 7");

/**
 * Builds a roll pointer.
 * 
 * @param[in] is_insert true if insert undo log
 * @param[in] rseg_id Rollback segment id
 * @param[in] page_no Page number
 * 
 * 
 * @return	roll pointer
 */
inline roll_ptr_t trx_undo_build_roll_ptr(bool is_insert, ulint rseg_id, space_id_t page_no, ulint offset) {
  ut_ad(rseg_id < 128);

  return static_cast<roll_ptr_t>(is_insert) << 55 |
         static_cast<roll_ptr_t>(rseg_id) << 48 |
         static_cast<roll_ptr_t>(page_no) << 16 |
         offset;
}

/**
 * Decodes a roll pointer.
 * 
 * @param[in] roll_ptr Roll pointer
 * @param[out] is_insert true if insert undo log
 * @param[out] rseg_id Rollback segment id
 * @param[out] page_no Page number
 * @param[out] offset Offset of the undo entry within page
 */
inline void trx_undo_decode_roll_ptr(roll_ptr_t roll_ptr, bool *is_insert, ulint *rseg_id, page_no_t *page_no, ulint *offset) {

  ut_ad(roll_ptr < (1ULL << 56));

  *offset = ulint(roll_ptr) & 0xFFFF;
  roll_ptr >>= 16;
  *page_no = ulint(roll_ptr) & 0xFFFFFFFF;
  roll_ptr >>= 32;
  *rseg_id = ulint(roll_ptr) & 0x7F;
  roll_ptr >>= 7;
  *is_insert = roll_ptr == 1;
}

/**
 * Returns true if the roll pointer is of the insert type.
 * 
 * @param[in] roll_ptr Roll pointer
 * 
 * @return	true if insert undo log
 */
inline bool trx_undo_roll_ptr_is_insert(roll_ptr_t roll_ptr) {
  static_assert(DATA_ROLL_PTR_LEN == 7, "error DATA_ROLL_PTR_LEN != 7");

  auto high = roll_ptr;

  return high / (256 * 256 * 128);
}

/**
 * Writes a roll ptr to an index page. In case that the size changes in
 * some future version, this function should be used instead of
 * mach_write_...
 * 
 * @param[in] ptr Pointer to memory where written
 * @param[in] roll_ptr Roll ptr
 */
inline void trx_write_roll_ptr(byte *ptr, roll_ptr_t roll_ptr) {
  static_assert(DATA_ROLL_PTR_LEN == 7, "error DATA_ROLL_PTR_LEN != 7");

  mach_write_to_7(ptr, roll_ptr);
}

/**
 * Reads a roll ptr from an index page. In case that the roll ptr size
 * changes in some future version, this function should be used instead of
 * mach_read_...
 * 
 * @param[in] ptr Pointer to memory from where to read
 * 
 * @return	roll ptr
 */
inline roll_ptr_t trx_read_roll_ptr(const byte *ptr) {
  static_assert(DATA_ROLL_PTR_LEN == 7, "error DATA_ROLL_PTR_LEN != 7");

  return mach_read_from_7(ptr);
}

/**
 * Returns the start offset of the undo log records of the specified undo
 * log on the page.
 * 
 * @param[in] undo_page Undo log page
 * @param[in] page_no Undo log header page number
 * @param[in] offset Undo log header offset on page
 * 
 * @return	start offset
 */
inline ulint trx_undo_page_get_start(page_t *undo_page, page_no_t page_no, ulint offset) {
  if (page_no == page_get_page_no(undo_page)) {
    return mach_read_from_2(offset + undo_page + TRX_UNDO_LOG_START);
  } else {
    return TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;
  }
}

/**
 * Returns the end offset of the undo log records of the specified undo
 * log on the page.
 * 
 * @param[in] undo_page Undo log page
 * @param[in] page_no Undo log header page number
 * @param[in] offset Undo log header offset on page
 * 
 * @return	end offset
 */
inline ulint trx_undo_page_get_end(page_t *undo_page, page_no_t page_no, ulint offset) {
  ulint end;

  if (page_no == page_get_page_no(undo_page)) {

    auto log_hdr = undo_page + offset;

    end = mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG);

    if (end == 0) {
      end = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
    }
  } else {
    end = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  }

  return (end);
}

/** Returns the previous undo record on the page in the specified log, or
NULL if none exists.
@return	pointer to record, NULL if none */
inline trx_undo_rec_t *trx_undo_page_get_prev_rec(
  trx_undo_rec_t *rec, /*!< in: undo log record */
  ulint page_no,       /*!< in: undo log header page number */
  ulint offset
) /*!< in: undo log header offset on page */
{
  page_t *undo_page;
  ulint start;

  undo_page = (page_t *)ut_align_down(rec, UNIV_PAGE_SIZE);

  start = trx_undo_page_get_start(undo_page, page_no, offset);

  if (start + undo_page == rec) {

    return (nullptr);
  }

  return (undo_page + mach_read_from_2(rec - 2));
}

/** Returns the next undo log record on the page in the specified log, or
NULL if none exists.
@return	pointer to record, NULL if none */
inline trx_undo_rec_t *trx_undo_page_get_next_rec(
  trx_undo_rec_t *rec, /*!< in: undo log record */
  ulint page_no,       /*!< in: undo log header page number */
  ulint offset
) /*!< in: undo log header offset on page */
{
  auto undo_page = (page_t *)ut_align_down(rec, UNIV_PAGE_SIZE);
  auto end = trx_undo_page_get_end(undo_page, page_no, offset);
  auto next = mach_read_from_2(rec);

  return next == end ? nullptr : undo_page + next;
}

/** Returns the last undo record on the page in the specified undo log, or
NULL if none exists.
@return	pointer to record, NULL if none */
inline trx_undo_rec_t *trx_undo_page_get_last_rec(
  page_t *undo_page, /*!< in: undo log page */
  ulint page_no,     /*!< in: undo log header page number */
  ulint offset
) /*!< in: undo log header offset on page */
{
  auto start = trx_undo_page_get_start(undo_page, page_no, offset);
  auto end = trx_undo_page_get_end(undo_page, page_no, offset);

  return start == end ? nullptr : undo_page + mach_read_from_2(undo_page + end - 2);
}

/** Returns the first undo record on the page in the specified undo log, or
NULL if none exists.
@return	pointer to record, NULL if none */
inline trx_undo_rec_t *trx_undo_page_get_first_rec(
  page_t *undo_page, /*!< in: undo log page */
  ulint page_no,     /*!< in: undo log header page number */
  ulint offset
) /*!< in: undo log header offset on page */
{
  auto start = trx_undo_page_get_start(undo_page, page_no, offset);
  auto end = trx_undo_page_get_end(undo_page, page_no, offset);

  return start == end ? nullptr : undo_page + start;
}
