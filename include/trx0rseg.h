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

/** @file include/trx0rseg.h
Rollback segment

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "trx0sys.h"
#include "trx0types.h"
#include "mtr0log.h"
#include "srv0srv.h"

/** Looks for a rollback segment, based on the rollback segment id.
@return	rollback segment */
trx_rseg_t *trx_rseg_get_on_id(ulint id); /*!< in: rollback segment id */

/** Creates a rollback segment header. This function is called only when
a new rollback segment is created in the database.
@return	page number of the created segment, FIL_NULL if fail */
ulint trx_rseg_header_create(
  ulint space,    /*!< in: space id */
  ulint max_size, /*!< in: max size in pages */
  ulint *slot_no, /*!< out: rseg id == slot number in trx sys */
  mtr_t *mtr
); /*!< in: mtr */

/** Creates the memory copies for rollback segments and initializes the
rseg list and array in trx_sys at a database startup. */
void trx_rseg_list_and_array_init(
  ib_recovery_t recovery, /*!< in: recovery flag */
  trx_sysf_t *sys_header, /*!< in: trx system header */
  mtr_t *mtr
); /*!< in: mtr */

/** Free's an instance of the rollback segment in memory. */
void trx_rseg_mem_free(trx_rseg_t *rseg); /*!< in, own: instance to free */

/* Number of undo log slots in a rollback segment file copy */
constexpr auto TRX_RSEG_N_SLOTS = UNIV_PAGE_SIZE / 16;

/* Maximum number of transactions supported by a single rollback segment */
constexpr auto TRX_RSEG_MAX_N_TRXS = TRX_RSEG_N_SLOTS / 2;

/* The rollback segment memory object */
struct trx_rseg_t {
  /*--------------------------------------------------------*/
  ulint id;        /*!< rollback segment id == the index of
                   its slot in the trx system file copy */
  mutex_t mutex;   /*!< mutex protecting the fields in this
                   struct except id; NOTE that the latching
                   order must always be kernel mutex ->
                   rseg mutex */
  ulint space;     /*!< space where the rollback segment is
                   header is placed */
  ulint page_no;   /* page number of the rollback segment
                   header */
  ulint max_size;  /* maximum allowed size in pages */
  ulint curr_size; /* current size in pages */
  /*--------------------------------------------------------*/
  /* Fields for update undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(trx_undo_t, undo_list) update_undo_list;
  /* List of update undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(trx_undo_t, undo_list) update_undo_cached;
  /* List of update undo log segments
  cached for fast reuse */
  /*--------------------------------------------------------*/
  /* Fields for insert undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(trx_undo_t, undo_list) insert_undo_list;
  /* List of insert undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(trx_undo_t, undo_list) insert_undo_cached;
  /* List of insert undo log segments
  cached for fast reuse */
  /*--------------------------------------------------------*/
  ulint last_page_no;   /*!< Page number of the last not yet
                        purged log header in the history list;
                        FIL_NULL if all list purged */
  ulint last_offset;    /*!< Byte offset of the last not yet
                        purged log header */
  trx_id_t last_trx_no; /*!< Transaction number of the last not
                        yet purged log */
  bool last_del_marks;  /*!< true if the last not yet purged log
                         needs purging */
  /*--------------------------------------------------------*/
  UT_LIST_NODE_T(trx_rseg_t) rseg_list;
  /* the list of the rollback segment
  memory objects */
};

UT_LIST_NODE_GETTER_DEFINITION(trx_rseg_t, rseg_list);

/* Undo log segment slot in a rollback segment header */
/*-------------------------------------------------------------*/
/* Page number of the header page of an undo log segment */
constexpr page_no_t TRX_RSEG_SLOT_PAGE_NO = 0;

/*-------------------------------------------------------------*/
/* Slot size */
constexpr ulint TRX_RSEG_SLOT_SIZE = 4;

/** The offset of the rollback segment header on its page */
constexpr auto TRX_RSEG = FSEG_PAGE_DATA;

/* Transaction rollback segment header */
/*-------------------------------------------------------------*/

/** Maximum allowed size for rollback segment in pages */
constexpr ulint TRX_RSEG_MAX_SIZE = 0;

/** Number of file pages occupied by the logs in the history list */
constexpr ulint TRX_RSEG_HISTORY_SIZE = 4;

/** The update undo logs for committed transactions */
constexpr ulint TRX_RSEG_HISTORY = 8;

/** Header for the file segment where this page is placed */
constexpr ulint TRX_RSEG_FSEG_HEADER = 8 + FLST_BASE_NODE_SIZE;

/** Undo log segment slots */
constexpr ulint TRX_RSEG_UNDO_SLOTS = 8 + FLST_BASE_NODE_SIZE + FSEG_HEADER_SIZE;

/*-------------------------------------------------------------*/

/** Gets a rollback segment header.
@return	rollback segment header, page x-latched */
inline trx_rsegf_t *trx_rsegf_get(
  space_id_t space_id,   /*!< in: space where placed */
  page_no_t page_no, /*!< in: page number of the header */
  mtr_t *mtr
) /*!< in: mtr */
{
  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { space_id, page_no },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_RSEG_HEADER));

  auto header = TRX_RSEG + block->get_frame();

  return header;
}

/** Gets a newly created rollback segment header.
@return	rollback segment header, page x-latched */
inline trx_rsegf_t *trx_rsegf_get_new(
  space_id_t space_id,   /*!< in: space where placed */
  page_no_t page_no, /*!< in: page number of the header */
  mtr_t *mtr
) /*!< in: mtr */
{
  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { space_id, page_no },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_RSEG_HEADER_NEW));

  auto header = TRX_RSEG + block->get_frame();

  return header;
}

/** Gets the file page number of the nth undo log slot.
@return	page number of the undo log segment */
inline ulint trx_rsegf_get_nth_undo(
  trx_rsegf_t *rsegf, /*!< in: rollback segment header */
  ulint n,            /*!< in: index of slot */
  mtr_t *mtr
) /*!< in: mtr */
{
  if (unlikely(n >= TRX_RSEG_N_SLOTS)) {
    ib_logger(ib_stream, "Error: trying to get slot %lu of rseg\n", (ulong)n);
    ut_error;
  }

  return (mtr_read_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, MLOG_4BYTES, mtr));
}

/** Sets the file page number of the nth undo log slot. */
inline void trx_rsegf_set_nth_undo(
  trx_rsegf_t *rsegf, /*!< in: rollback segment header */
  ulint n,            /*!< in: index of slot */
  ulint page_no,      /*!< in: page number of the undo log segment */
  mtr_t *mtr
) /*!< in: mtr */
{
  if (unlikely(n >= TRX_RSEG_N_SLOTS)) {
    ib_logger(ib_stream, "Error: trying to set slot %lu of rseg\n", (ulong)n);
    ut_error;
  }

  mlog_write_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

/** Looks for a free slot for an undo log segment.
@return	slot index or ULINT_UNDEFINED if not found */
inline ulint trx_rsegf_undo_find_free(
  trx_rsegf_t *rsegf, /*!< in: rollback segment header */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint i;
  ulint page_no;

  for (i = 0; i < TRX_RSEG_N_SLOTS; i++) {

    page_no = trx_rsegf_get_nth_undo(rsegf, i, mtr);

    if (page_no == FIL_NULL) {

      return (i);
    }
  }

  return (ULINT_UNDEFINED);
}
