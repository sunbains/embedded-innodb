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
#include "fut0lst.h"
#include "mtr0log.h"
#include "srv0srv.h"
#include "trx0types.h"

/** Number of undo log slots in a rollback segment file copy */
constexpr auto TRX_RSEG_N_SLOTS = UNIV_PAGE_SIZE / 16;

/** Maximum number of transactions supported by a single rollback segment */
constexpr auto TRX_RSEG_MAX_N_TRXS = TRX_RSEG_N_SLOTS / 2;

struct TrxRseg {
  /**
   * Looks for a rollback segment, based on the rollback segment id.
   * 
   * @param[in] id The rollback segment id.
   * 
   * @return	rollback segment
   */
  static TrxRseg *get_on_id(System &sys, ulint id);

  /**
   * Creates a rollback segment header. This function is called only when
   * a new rollback segment is created in the database.
   * 
   * @param[in] space The space id.
   * @param[in] max_size The maximum size in pages.
   * @param[out] slot_no The rollback segment id == slot number in trx sys.
   * @param[in] mtr The mini-transaction handle.
   * 
   * @return	page number of the created segment, FIL_NULL if fail
   */
  static page_no_t header_create(System &sys, space_id_t space, ulint max_size, ulint *slot_no, Mtr *mtr);

  /**
   * Creates the memory copies for rollback segments and initializes the
   * rseg list and array in srv_trx_sys at a database startup.
   * 
   * @param[in] recovery Recovery flag.
   * @param[in] sys_header The trx system header.
   * @param[in] mtr The mini-transaction handle.
   */
  static void list_and_array_init(System &sys, IbRecovery recovery, trx_sysf_t *sys_header, Mtr *mtr);

  /**
   * Free's an instance of the rollback segment in memory.
   */
  void memory_free(System &sys);

private:
  /**
   * Creates and initializes a rollback segment object.
   */
  static TrxRseg *mem_create(IbRecovery recovery, ulint id, ulint space, ulint page_no, Mtr *mtr);

public:
  /** Rollback segment id == the index of its slot in the trx
   * system file copy */
  ulint m_id;

  /** mutex protecting the fields in this struct except id;
   * NOTE that the latching order must always be TrxSys mutex -> rseg mutex */
  Mutex m_mutex;

  /** Space where the rollback segment is header is placed */
  space_id_t m_space;

  /** Page number of the rollback segment header */
  page_no_t m_page_no;

  /**Maximum allowed size in pages */
  ulint m_max_size;

  /** Current size in pages */
  ulint m_curr_size;

  /** List of update undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(TrxUndo, m_undo_list) m_update_undo_list;

  /* List of update undo log segments cached for fast reuse */
  UT_LIST_BASE_NODE_T_EXTERN(TrxUndo, m_undo_list) m_update_undo_cached;

  /** List of insert undo logs */
  UT_LIST_BASE_NODE_T_EXTERN(TrxUndo, m_undo_list) m_insert_undo_list;

  /** List of insert undo log segments cached for fast reuse */
  UT_LIST_BASE_NODE_T_EXTERN(TrxUndo, m_undo_list) m_insert_undo_cached;

  /* Fields for insert undo logs */

  /** Page number of the last not yet purged log header i
   * the history list; FIL_NULL if all list purged */
  page_no_t m_last_page_no;

  /** Byte offset of the last not yet purged log header */
  ulint m_last_offset;

  /** Transaction number of the last not yet purged log */
  trx_id_t m_last_trx_no;

  /** true if the last not yet purged log needs purging */
  bool m_last_del_marks;

  /* the list of the rollback segment memory objects */
  UT_LIST_NODE_T(TrxRseg) m_rseg_list;
};

UT_LIST_NODE_GETTER_DEFINITION(TrxRseg, m_rseg_list);

/** Page number of the header page of an undo log segment */
constexpr page_no_t TRX_RSEG_SLOT_PAGE_NO = 0;

/** Slot size */
constexpr ulint TRX_RSEG_SLOT_SIZE = 4;

/** The offset of the rollback segment header on its page */
constexpr auto TRX_RSEG = FSEG_PAGE_DATA;

/* Transaction rollback segment header */

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

/**
 * Gets a rollback segment header.
 * 
 * @param[in] space_id The space where placed.
 * @param[in] page_no The page number of the header.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	rollback segment header, page x-latched
 */
inline trx_rsegf_t *trx_rsegf_get(System &sys, space_id_t space_id, page_no_t page_no, Mtr *mtr) noexcept {
  BufPool::Request req{
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = {space_id, page_no},
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = sys.m_buf_pool->get(req, nullptr);
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_RSEG_HEADER));

  auto header = TRX_RSEG + block->get_frame();

  return header;
}

/**
 * Gets a newly created rollback segment header.
 * 
 * @param[in] space_id The space where placed.
 * @param[in] page_no The page number of the header.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	rollback segment header, page x-latched
 */
inline trx_rsegf_t *trx_rsegf_get_new(System &sys, space_id_t space_id, page_no_t page_no, Mtr *mtr) noexcept {
  BufPool::Request req{
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = {space_id, page_no},
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = sys.m_buf_pool->get(req, nullptr);
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_RSEG_HEADER_NEW));

  auto header = TRX_RSEG + block->get_frame();

  return header;
}

/**
 * Gets the file page number of the nth undo log slot.
 * 
 * @param[in] rsegf The rollback segment header.
 * @param[in] n The index of the slot.
 * @param[in] mtr The mini-transaction handle.
 * 
 * @return	page number of the undo log segment
 */
inline ulint trx_rsegf_get_nth_undo(trx_rsegf_t *rsegf, ulint n, Mtr *mtr) noexcept {
  if (unlikely(n >= TRX_RSEG_N_SLOTS)) {
    log_fatal(std::format("Trying to get slot {} of rseg", n));
  }

  return mtr->read_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, MLOG_4BYTES);
}

/**
 * Sets the file page number of the nth undo log slot.
 * 
 * @param[in] rsegf The rollback segment header.
 * @param[in] n The index of the slot.
 * @param[in] page_no The page number of the undo log segment.
 * @param[in] mtr The mini-transaction handle.
 */
inline void trx_rsegf_set_nth_undo(trx_rsegf_t *rsegf, ulint n, page_no_t page_no, Mtr *mtr) noexcept {
  if (unlikely(n >= TRX_RSEG_N_SLOTS)) {
    log_fatal(std::format("Trying to set slot {} of rseg", n));
  }

  mlog_write_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

/**
 * Looks for a free slot for an undo log segment.
 * 
 * @param[in] rsegf The rollback segment header.
 * @param[in] mtr The mini-transaction handle.
 *
 * @return	slot index or ULINT_UNDEFINED if not found
 */
inline ulint trx_rsegf_undo_find_free(trx_rsegf_t *rsegf, Mtr *mtr) noexcept {
  for (ulint i{}; i < TRX_RSEG_N_SLOTS; ++i) {

    const auto page_no = trx_rsegf_get_nth_undo(rsegf, i, mtr);

    if (page_no == FIL_NULL) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}
