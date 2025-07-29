/*****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/trx0purge.h
Purge old versions

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "fil0fil.h"
#include "innodb0types.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "que0types.h"
#include "trx0sys.h"
#include "trx0types.h"
#include "trx0undo.h"
#include "usr0sess.h"

/**
 * A dummy undo record used as a return value when we have a whole undo log
 * which needs no purge
 */
extern trx_undo_rec_t trx_purge_dummy_rec;

enum Purge_state {
  /** Unknown state. */
  PURGE_STATE_UNKNOWN = 0,

  /** Purge operation is running */
  PURGE_STATE_ON = 1,

  /* Purge operation is stopped, or it should be stopped */
  PURGE_STATE_OFF = 2
};

/**
 * Calculates the file address of an undo log header when we have the file
 * address of its history list node.
 * 
 * @param[in,out] node_addr  File address of the history list node of the log.
 * 
 * @return	file address of the log */
inline Fil_addr trx_purge_get_log_from_hist(Fil_addr node_addr) noexcept {
  node_addr.m_boffset -= TRX_UNDO_HISTORY_NODE;

  return node_addr;
}

/**
 * The control structure used in the purge operation
 */
struct Purge_sys {

  /**
   * Constructor.
   * 
   * @param trx Transaction instance permanently assigned for the purge
   */
  explicit Purge_sys(Trx *trx) noexcept;

  /**
   * Destructor.
   */
  ~Purge_sys() noexcept;

  /**
   * Checks if trx_id is >= purge_view: then it is guaranteed that its update
   * undo log still exists in the system.
   * 
   * @param[in] trx_id            Transaction ID
   * 
   * @return true if is sure that it is preserved, also if the function
   *  returns false, it is possible that the undo log still exists in the
   *  system */
  bool update_undo_must_exist(trx_id_t trx_id) noexcept;

  /**
   * Adds the update undo log as the first log in the history list. Removes the
   * update undo log segment from the rseg slot if it is too big for reuse.
   * 
   * @param[in] trx               Transaction
   * @param[in] undo_page         Update undo log header page, x-latched.
   * @param[in] mtr               Mini-transaction.
   */
  static void add_update_undo_to_history(Trx *trx, page_t *undo_page, mtr_t *mtr) noexcept;

  /**
   * Fetches the next undo log record from the history list to purge. It must be
   * released with the corresponding release function.
   * 
   * @param[out] roll_ptr         Roll pointer to undo record
   * @param[out] cell             Storage cell for the record in the purge array
   * @param[in,out] heap          Memory heap where copied.
   * 
   * @return copy of an undo log record or pointer to trx_purge_dummy_rec,
   *  if the whole undo log can skipped in purge; nullptr if none left
   */
  trx_undo_rec_t *fetch_next_rec(roll_ptr_t *roll_ptr, trx_undo_inf_t **cell, mem_heap_t *heap) noexcept;

  /**
   * Releases a reserved purge undo record.
   * 
   * @praam[in,out] cell          Storage cell
   */
  void rec_release(trx_undo_inf_t *cell) noexcept;

  /**
   * This function runs a purge batch.
   * 
   * @return	number of undo log pages handled in the batch
   */
  ulint run() noexcept;

  /**
   * @return string representation of the purge sub-system.
   */
  std::string to_string() noexcept;

 private:
  /**
   * Stores info of an undo log record during a purge.
   * 
   * @param[in] trx_no            Transaction number.
   * @param[in] undo_no           Undo number.
   * 
   * @return	pointer to the storage cell */
  trx_undo_inf_t *arr_store_info(trx_id_t trx_no, undo_no_t undo_no) noexcept;

  /**
   * Removes info of an undo log record during a purge.
   * 
   * @param[in] cell              Pointer to storage cell.
   */
  void arr_remove_info(trx_undo_inf_t *cell) noexcept;

  /**
   * Builds a purge 'query' graph. The actual purge is performed by executing
   * this query graph.
   * 
   * @return	own: the query graph
   */
  que_t *graph_build() noexcept;

  /**
   * Frees an undo log segment which is in the history list. Cuts the end of the
   * history list at the youngest undo log in this segment.
   * 
   * @param[in] rseg              Rollback segment
   * @param[in] hdr_addr          File address of the log header
   * @param[in] n_removed_logs    Count of how many undo logs we will cut off from the
   *                              end of the history list.
   */
  void free_segment(trx_rseg_t *rseg, Fil_addr hdr_addr, ulint n_removed_logs) noexcept;

  /**
   * Removes unnecessary history data from a rollback segment.
   * 
   * @param[in] rseg              Rollback segment
   * @param[in] limit_trx_no      Remove update undo logs whose
   *                              trx number is < limit_trx_no
   * @param[in] limit_undo_no     If transaction mnumber is equal to
   *                              limit_trx_no, truncate undo records
   *                              with undo mumber < limit_undo_no
   */
  void truncate_rseg_history(trx_rseg_t *rseg, trx_id_t limit_trx_no, undo_no_t limit_undo_no) noexcept;

  /**
   * Removes unnecessary history data from rollback segments. NOTE that when this
   * function is called, the caller must not have any latches on undo log pages!
   */
  void truncate_history() noexcept;

  /**
   * Does a truncate if the purge array is empty. NOTE that when this function is
   * called, the caller must not have any latches on undo log pages!
   * 
   * @return	true if array empty
   */
  bool truncate_if_arr_empty() noexcept;

  /**
   * Updates the last not yet purged history log info in rseg when we have purged
   * a whole undo log. Advances also purge_sys->purge_trx_no past the purged log.
   * 
   * @param[in] rseg              Rollback segment.
   * 
   */
  void rseg_get_next_history_log(trx_rseg_t *rseg) noexcept;

  /**
   * Chooses the next undo log to purge and updates the info in purge_sys. This
   * function is used to initialize purge_sys when the next record to purge is
   * not known, and also to update the purge system info on the next record when
   * purge has handled the whole undo log for a transaction.
   */
  void choose_next_log() noexcept;

  /**
   * Gets the next record to purge and updates the info in the purge system.
   * 
   * @param[in] heap              Memory heap where copied.
   * 
   * @return	copy of an undo log record or pointer to the dummy undo log record
   */
  trx_undo_rec_t *get_next_rec(mem_heap_t *heap) noexcept;

 public:
  /** Purge system state */
  Purge_state m_state{PURGE_STATE_UNKNOWN};

  /** System session running the purge query */
  Session *m_sess{};

  /** System transaction running the purge query:
   * this trx is not in the trx list of the trx system and it never ends */
  Trx *m_trx{};

  /** The query graph which will do the parallelized purge operation */
  que_t *m_query{};

  /** The latch protecting the purge view. A purge operation must acquire
   * an x-latch here for the instant at which it changes the purge view:
   * an undo log operation can prevent this by obtaining an s-latch here. */
  mutable rw_lock_t m_latch;

  /** The purge will not remove undo logs which are >= this view (purge view) */
  read_view_t *m_view{};

  /** Mutex protecting the fields below */
  mutable mutex_t m_mutex{};

  /** Approximate number of undo log pages processed in purge */
  ulint m_n_pages_handled{};

  /** Target of how many pages to get processed in the current purge */
  ulint m_handle_limit{};

  /* The following two fields form the 'purge pointer' which advances
  during a purge, and which is used in history list truncation */

  /** Purge has advanced past all transactions whose number is less than this */
  trx_id_t m_purge_trx_no{};

  /** Purge has advanced past all records whose undo number is less than this */
  undo_no_t m_purge_undo_no{};

  /** true if the info of the next record to purge is stored below:
   * if yes, then the transaction number and the undo number of the
   * record are stored in purge_trx_no and purge_undo_no above */
  bool m_next_stored{};

  /** Rollback segment for the next undo record to purge */
  trx_rseg_t *m_rseg{};

  /** Page number for the next undo record to purge, page number
   * of the log header, if dummy record */
  page_no_t m_page_no{};

  /** Page offset for the next undo record to purge, 0 if the dummy record */
  ulint m_offset{};

  /** Header page of the undo log where the next record to purge belongs */
  page_no_t m_hdr_page_no{};

  /** Header byte offset on the page */
  ulint m_hdr_offset{};

  /** Array of transaction numbers and undo numbers of the undo records
   * currently under processing in purge */
  trx_undo_arr_t *m_arr{};

  /** Temporary storage used during a purge: can be emptied after
   * purge completes */
  mem_heap_t *m_heap{};
};
