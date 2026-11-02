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

/** A cell of TrxUndoArr; used during a rollback and a purge */
struct TrxUndoInf {
  trx_id_t trx_no;   /*!< transaction number: not defined during
                     a rollback */
  undo_no_t undo_no; /*!< undo number of an undo record */
  bool in_use;       /*!< true if the cell is in use */
};

/** During a rollback and a purge, undo numbers of undo records currently being
processed are stored in this array */
struct TrxUndoArr {
  ulint n_cells;         /*!< number of cells in the array */
  ulint n_used;          /*!< number of cells currently in use */
  TrxUndoInf *infos; /*!< the array of undo infos */
  MemHeap *heap;      /*!< memory heap from which allocated */
};

/** Rollback node states */
enum RollNodeState {
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
struct TrxRollback {
  /**
   * @brief Constructs a new TrxRollback object
   * @param[in] heap Memory heap where object is allocated
   */
  explicit TrxRollback(MemHeap *heap);

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
  static TrxSavept savept_take(Trx *trx);

  /**
   * @brief Creates an undo number array.
   * @return Newly created undo number array
   */
  static TrxUndoArr *undo_arr_create();

  /**
   * @brief Frees an undo number array.
   * @param[in] arr Undo number array to free
   */
  static void undo_arr_free(TrxUndoArr *arr);

  /**
   * @brief Tries to truncate the undo logs.
   * @param[in,out] trx Transaction
   */
  static void try_truncate(System &sys, Trx *trx);

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
  static TrxUndoRecord pop_top_rec_of_trx(System &sys, Trx *trx, undo_no_t limit, 
                                           roll_ptr_t *roll_ptr, MemHeap *heap);

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
  static void rollback(System &sys, Trx *trx, TrxSig *sig, QueThr **next_thr);

  /**
   * @brief Rollback or clean up any incomplete transactions which were
   * encountered in crash recovery.
   * @param[in] all false=roll back dictionary transactions;
   * true=roll back all non-PREPARED transactions
   */
  static void rollback_or_clean_recovered(System &sys, bool all);

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
  static void finish_rollback_off_kernel(System &sys, QueFork *graph, Trx *trx, QueThr **next_thr);

  /**
   * @brief Builds an undo 'query' graph for a transaction.
   * @param[in] trx Transaction handle
   * @return own: the query graph
   */
  static QueFork *roll_graph_build(System &sys, Trx *trx);

  /**
   * @brief Rollback a user transaction.
   * @param[in] trx Transaction handle
   * @param[in] partial true if partial rollback requested
   * @param[in] savept Pointer to savepoint undo number, if partial rollback requested
   * @return error code or DB_SUCCESS
   */
  static DbErr general_rollback(System &sys, Trx *trx, bool partial, TrxSavept *savept);

  /**
   * @brief Frees savepoint structs starting from savep.
   * @param[in] trx Transaction handle
   * @param[in] savep Free all savepoints > this one; if this is nullptr, free all savepoints of trx
   */
  static void savepoints_free(Trx *trx, TrxNamedSavept *savep);

  /**
   * @brief Performs an execution step for a rollback command node in a query graph.
   * @param[in] thr Query thread
   * @return query thread to run next, or nullptr
   */
  QueThr *rollback_step(System &sys, QueThr *thr);

  // Data members (merged from TrxRollback)
  QueCommon m_common;      /*!< node type: QUE_NODE_ROLLBACK */
  enum RollNodeState m_state; /*!< node execution state */
  bool m_partial;             /*!< true if we want a partial rollback */
  TrxSavept m_savept;      /*!< savepoint to which to roll back, in the case of a partial rollback */

private:
  /**
   * @brief Roll back an active transaction.
   * @param[in] recovery Recovery flag
   * @param[in,out] trx Transaction
   */
  static void rollback_active(IbRecovery recovery, Trx *trx);

  /**
   * @brief Finishes error processing after the necessary partial rollback has been done.
   * @param[in] trx Transaction
   */
  static void finish_error_processing(System &sys, Trx *trx);

  /**
   * @brief Finishes a partial rollback operation.
   * @param[in] trx Transaction
   * @param[in,out] next_thr Next query thread to run
   */
  static void finish_partial_rollback_off_kernel(System &sys, Trx *trx, QueThr **next_thr) noexcept;
};

/** A savepoint set with SQL's "SAVEPOINT savepoint_id" command */
struct TrxNamedSavept {
  void *name;          /*!< savepoint name, it can be any
                       arbitrary set of characters. Note:
                       When allocating memory for trx_named
                       structure allocate an additional
                       name_len bytes and set the value of
                       name to point to the first byte past
                       an instance of this structure */
  ulint name_len;      /*!< length of name in bytes */
  TrxSavept savept; /*!< the undo number corresponding to
                       the savepoint */
  UT_LIST_NODE_T(TrxNamedSavept)
  trx_savepoints; /*!< the list of savepoints of a
                  transaction */
};

UT_LIST_NODE_GETTER_DEFINITION(TrxNamedSavept, trx_savepoints);

/** Returns pointer to nth element in an undo number array.
@param[in] arr                  Undo number array
@param[in] n                    Position
@return	pointer to the nth element */
inline TrxUndoInf *trx_undo_arr_get_nth_info(TrxUndoArr *arr, ulint n) {
  ut_ad(n < arr->n_cells);

  return arr->infos + n;
}

// Legacy function wrappers for backward compatibility
#define trx_roll_free_all_savepoints(s) TrxRollback::savepoints_free((s), nullptr)

inline bool trx_is_recv(const Trx *trx) { return TrxRollback::is_recv_trx(trx); }
inline TrxSavept trx_savept_take(Trx *trx) { return TrxRollback::savept_take(trx); }
inline TrxUndoArr *trx_undo_arr_create(void) { return TrxRollback::undo_arr_create(); }
inline void trx_undo_arr_free(TrxUndoArr *arr) { TrxRollback::undo_arr_free(arr); }
inline void trx_roll_try_truncate(System &sys, Trx *trx) { TrxRollback::try_truncate(sys, trx); }

inline TrxUndoRecord trx_roll_pop_top_rec_of_trx(System &sys, Trx *trx, undo_no_t limit, roll_ptr_t *roll_ptr, MemHeap *heap) {
  return TrxRollback::pop_top_rec_of_trx(sys, trx, limit, roll_ptr, heap);
}

inline bool trx_undo_rec_reserve(Trx *trx, undo_no_t undo_no) { return TrxRollback::undo_rec_reserve(trx, undo_no); }
inline void trx_undo_rec_release(Trx *trx, undo_no_t undo_no) { TrxRollback::undo_rec_release(trx, undo_no); }
inline void trx_rollback(System &sys, Trx *trx, TrxSig *sig, QueThr **next_thr) { TrxRollback::rollback(sys, trx, sig, next_thr); }
inline void trx_rollback_or_clean_recovered(System &sys, bool all) { TrxRollback::rollback_or_clean_recovered(sys, all); }
inline void *trx_rollback_or_clean_all_recovered(void *arg) { return TrxRollback::rollback_or_clean_all_recovered(arg); }

inline void trx_finish_rollback_off_kernel(System &sys, QueFork *graph, Trx *trx, QueThr **next_thr) {
  TrxRollback::finish_rollback_off_kernel(sys, graph, trx, next_thr);
}

inline QueFork *trx_roll_graph_build(System &sys, Trx *trx) { return TrxRollback::roll_graph_build(sys, trx); }
inline TrxRollback *roll_node_create(MemHeap *heap) { return new (mem_heap_alloc(heap, sizeof(TrxRollback))) TrxRollback(heap); }
inline QueThr *trx_rollback_step(System &sys, QueThr *thr) { 

  return static_cast<TrxRollback*>(thr->run_node)->rollback_step(sys, thr); 
}

inline DbErr trx_general_rollback(System &sys, Trx *trx, bool partial, TrxSavept *savept) {
  return TrxRollback::general_rollback(sys, trx, partial, savept);
}

inline void trx_roll_savepoints_free(Trx *trx, TrxNamedSavept *savep) { TrxRollback::savepoints_free(trx, savep); }
