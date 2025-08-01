/****************************************************************************
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

/** @file include/trx0roll.h
Transaction rollback

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "mtr0mtr.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0types.h"
#include "trx0undo_rec.h"

/** A cell of trx_undo_arr_t; used during a rollback and a purge */
struct trx_undo_inf_t {
  trx_id_t trx_no;   /*!< transaction number: not defined during
                     a rollback */
  undo_no_t undo_no; /*!< undo number of an undo record */
  bool in_use;       /*!< true if the cell is in use */
};

/** During a rollback and a purge, undo numbers of undo records currently being
processed are stored in this array */
struct trx_undo_arr_t {
  ulint n_cells;         /*!< number of cells in the array */
  ulint n_used;          /*!< number of cells currently in use */
  trx_undo_inf_t *infos; /*!< the array of undo infos */
  mem_heap_t *heap;      /*!< memory heap from which allocated */
};

/** Rollback node states */
enum roll_node_state {
  ROLL_NODE_SEND = 1, /*!< about to send a rollback signal to
                      the transaction */
  ROLL_NODE_WAIT      /*!< rollback signal sent to the transaction,
                      waiting for completion */
};

/**
 * @brief Transaction rollback operations class
 * 
 * This class encapsulates all transaction rollback functionality,
 * including savepoint management, undo log processing, and rollback execution.
 */
struct Trx_rollback {
  /**
   * @brief Constructs a new Trx_rollback object
   * @param[in] heap Memory heap where object is allocated
   */
  explicit Trx_rollback(mem_heap_t *heap);

  /**
   * @brief Determines if this transaction is rolling back an incomplete transaction
   * in crash recovery.
   * @param[in] trx Transaction to check
   * @return true if trx is an incomplete transaction that is being rolled
   * back in crash recovery
   */
  static bool is_recv_trx(const Trx *trx);

  /**
   * @brief Returns a transaction savepoint taken at this point in time.
   * @param[in] trx Transaction
   * @return savepoint
   */
  static trx_savept_t savept_take(Trx *trx);

  /**
   * @brief Creates an undo number array.
   * @return Newly created undo number array
   */
  static trx_undo_arr_t *undo_arr_create();

  /**
   * @brief Frees an undo number array.
   * @param[in] arr Undo number array to free
   */
  static void undo_arr_free(trx_undo_arr_t *arr);

  /**
   * @brief Tries to truncate the undo logs.
   * @param[in,out] trx Transaction
   */
  static void try_truncate(Trx *trx);

  /**
   * @brief Pops the topmost record when the two undo logs of a transaction are seen
   * as a single stack of records ordered by their undo numbers.
   * @param[in] trx Transaction
   * @param[in] limit Least undo number we need
   * @param[out] roll_ptr Roll pointer to undo record
   * @param[in] heap Memory heap where copied
   * @return undo log record copied to heap, nullptr if none left, or if the
   * undo number of the top record would be less than the limit
   */
  static Trx_undo_record pop_top_rec_of_trx(Trx *trx, undo_no_t limit, 
                                           roll_ptr_t *roll_ptr, mem_heap_t *heap);

  /**
   * @brief Reserves an undo log record for a query thread to undo.
   * @param[in,out] trx Transaction
   * @param[in] undo_no Undo number of the record
   * @return true if succeeded
   */
  static bool undo_rec_reserve(Trx *trx, undo_no_t undo_no);

  /**
   * @brief Releases a reserved undo record.
   * @param[in,out] trx Transaction
   * @param[in] undo_no Undo number
   */
  static void undo_rec_release(Trx *trx, undo_no_t undo_no);

  /**
   * @brief Starts a rollback operation.
   * @param[in] trx Transaction
   * @param[in] sig Signal starting the rollback
   * @param[in,out] next_thr Next query thread to run; if the value
   * which is passed in is a pointer to a nullptr pointer, then
   * the calling function can start running a new query thread
   */
  static void rollback(Trx *trx, trx_sig_t *sig, que_thr_t **next_thr);

  /**
   * @brief Rollback or clean up any incomplete transactions which were
   * encountered in crash recovery.
   * @param[in] all false=roll back dictionary transactions;
   * true=roll back all non-PREPARED transactions
   */
  static void rollback_or_clean_recovered(bool all);

  /**
   * @brief Rollback or clean up any incomplete transactions which were
   * encountered in crash recovery. (Background thread version)
   * @return dummy parameter
   */
  static void *rollback_or_clean_all_recovered(void *);

  /**
   * @brief Finishes a transaction rollback.
   * @param[in] graph Undo graph which can now be freed
   * @param[in] trx Transaction
   * @param[in,out] next_thr Next query thread to run; if the value which is passed
   * in is a pointer to a nullptr pointer, then the calling function can
   * start running a new query thread; if this parameter is nullptr, it is ignored
   */
  static void finish_rollback_off_kernel(que_t *graph, Trx *trx, que_thr_t **next_thr);

  /**
   * @brief Builds an undo 'query' graph for a transaction.
   * @param[in] trx Transaction handle
   * @return own: the query graph
   */
  static que_t *roll_graph_build(Trx *trx);

  /**
   * @brief Rollback a user transaction.
   * @param[in] trx Transaction handle
   * @param[in] partial true if partial rollback requested
   * @param[in] savept Pointer to savepoint undo number, if partial rollback requested
   * @return error code or DB_SUCCESS
   */
  static db_err general_rollback(Trx *trx, bool partial, trx_savept_t *savept);

  /**
   * @brief Frees savepoint structs starting from savep.
   * @param[in] trx Transaction handle
   * @param[in] savep Free all savepoints > this one; if this is nullptr, free all savepoints of trx
   */
  static void savepoints_free(Trx *trx, trx_named_savept_t *savep);

  /**
   * @brief Performs an execution step for a rollback command node in a query graph.
   * @param[in] thr Query thread
   * @return query thread to run next, or nullptr
   */
  que_thr_t *rollback_step(que_thr_t *thr);

  // Data members (merged from Trx_rollback)
  que_common_t m_common;      /*!< node type: QUE_NODE_ROLLBACK */
  enum roll_node_state m_state; /*!< node execution state */
  bool m_partial;             /*!< true if we want a partial rollback */
  trx_savept_t m_savept;      /*!< savepoint to which to roll back, in the case of a partial rollback */

private:
  /**
   * @brief Roll back an active transaction.
   * @param[in] recovery Recovery flag
   * @param[in,out] trx Transaction
   */
  static void rollback_active(ib_recovery_t recovery, Trx *trx);

  /**
   * @brief Finishes error processing after the necessary partial rollback has been done.
   * @param[in] trx Transaction
   */
  static void finish_error_processing(Trx *trx);

  /**
   * @brief Finishes a partial rollback operation.
   * @param[in] trx Transaction
   * @param[in,out] next_thr Next query thread to run
   */
  static void finish_partial_rollback_off_kernel(Trx *trx, que_thr_t **next_thr) noexcept;
};

/** A savepoint set with SQL's "SAVEPOINT savepoint_id" command */
struct trx_named_savept_t {
  void *name;          /*!< savepoint name, it can be any
                       arbitrary set of characters. Note:
                       When allocating memory for trx_named
                       structure allocate an additional
                       name_len bytes and set the value of
                       name to point to the first byte past
                       an instance of this structure */
  ulint name_len;      /*!< length of name in bytes */
  trx_savept_t savept; /*!< the undo number corresponding to
                       the savepoint */
  UT_LIST_NODE_T(trx_named_savept_t)
  trx_savepoints; /*!< the list of savepoints of a
                  transaction */
};

UT_LIST_NODE_GETTER_DEFINITION(trx_named_savept_t, trx_savepoints);

/** Returns pointer to nth element in an undo number array.
@param[in] arr                  Undo number array
@param[in] n                    Position
@return	pointer to the nth element */
inline trx_undo_inf_t *trx_undo_arr_get_nth_info(trx_undo_arr_t *arr, ulint n) {
  ut_ad(n < arr->n_cells);

  return arr->infos + n;
}

// Legacy function wrappers for backward compatibility
#define trx_roll_free_all_savepoints(s) Trx_rollback::savepoints_free((s), nullptr)

inline bool trx_is_recv(const Trx *trx) { return Trx_rollback::is_recv_trx(trx); }
inline trx_savept_t trx_savept_take(Trx *trx) { return Trx_rollback::savept_take(trx); }
inline trx_undo_arr_t *trx_undo_arr_create(void) { return Trx_rollback::undo_arr_create(); }
inline void trx_undo_arr_free(trx_undo_arr_t *arr) { Trx_rollback::undo_arr_free(arr); }
inline void trx_roll_try_truncate(Trx *trx) { Trx_rollback::try_truncate(trx); }

inline Trx_undo_record trx_roll_pop_top_rec_of_trx(Trx *trx, undo_no_t limit, roll_ptr_t *roll_ptr, mem_heap_t *heap) {
  return Trx_rollback::pop_top_rec_of_trx(trx, limit, roll_ptr, heap);
}

inline bool trx_undo_rec_reserve(Trx *trx, undo_no_t undo_no) { return Trx_rollback::undo_rec_reserve(trx, undo_no); }
inline void trx_undo_rec_release(Trx *trx, undo_no_t undo_no) { Trx_rollback::undo_rec_release(trx, undo_no); }
inline void trx_rollback(Trx *trx, trx_sig_t *sig, que_thr_t **next_thr) { Trx_rollback::rollback(trx, sig, next_thr); }
inline void trx_rollback_or_clean_recovered(bool all) { Trx_rollback::rollback_or_clean_recovered(all); }
inline void *trx_rollback_or_clean_all_recovered(void *arg) { return Trx_rollback::rollback_or_clean_all_recovered(arg); }

inline void trx_finish_rollback_off_kernel(que_t *graph, Trx *trx, que_thr_t **next_thr) {
  Trx_rollback::finish_rollback_off_kernel(graph, trx, next_thr);
}

inline que_t *trx_roll_graph_build(Trx *trx) { return Trx_rollback::roll_graph_build(trx); }
inline Trx_rollback *roll_node_create(mem_heap_t *heap) { return new (mem_heap_alloc(heap, sizeof(Trx_rollback))) Trx_rollback(heap); }
inline que_thr_t *trx_rollback_step(que_thr_t *thr) { 

  return static_cast<Trx_rollback*>(thr->run_node)->rollback_step(thr); 
}

inline db_err trx_general_rollback(Trx *trx, bool partial, trx_savept_t *savept) {
  return Trx_rollback::general_rollback(trx, partial, savept);
}

inline void trx_roll_savepoints_free(Trx *trx, trx_named_savept_t *savep) { Trx_rollback::savepoints_free(trx, savep); }
