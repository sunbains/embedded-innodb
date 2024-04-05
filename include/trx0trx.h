/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/** @file include/trx0trx.h
The transaction

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "dict0types.h"
#include "mem0mem.h"
#include "que0types.h"
#include "read0types.h"
#include "srv0srv.h"
#include "trx0types.h"
#include "usr0types.h"
#include "ut0vec.h"

struct read_view_t;

/** Dummy session used currently in MySQL interface */
extern sess_t *trx_dummy_sess;

/** Number of transactions currently allocated for MySQL: protected by
the kernel mutex */
extern ulint trx_n_transactions;

/**
 * Set detailed error message for the transaction.
 *
 * @param trx The transaction struct.
 * @param msg The detailed error message.
 */
void trx_set_detailed_error(trx_t *trx, const char *msg);

/**
 * Creates and initializes a transaction object.
 *
 * @param[in] sess The session.
 *
 * @return The transaction object.
 */
[[nodiscard]] trx_t *trx_create(sess_t *sess);

#ifdef UNIT_TESTING
/** Initialize all the fields in a transaction instance.
@param[in] sess                 Client session (nullptr for now).
@return transaction instance. */
[[nodiscard]] trx_t *trx_create_low(trx_t *trx, sess_t* sess);
#endif /* UNIT_TESTING */

/**
 * Creates a transaction object.
 *
 * @return own: transaction object
 */
[[nodiscard]] trx_t *trx_allocate();

/**
 * Creates a transaction object for background operations by the master thread.
 *
 * @return own: transaction object
 */
[[nodiscard]] trx_t *trx_allocate_for_background();

/**
 * Frees a transaction object.
 *
 * @param[in] trx The transaction object to be freed.
 */
void trx_free_(trx_t *trx);

/**
 * Frees a transaction object of a background operation of the master thread.
 *
 * @param[in] trx The transaction object to be freed.
 */
void trx_free_for_background(trx_t *trx);

/**
 * Creates trx objects for transactions and initializes the trx list of trx_sys at database start.
 * Rollback segment and undo log lists must already exist when this function is called,
 * because the lists of transactions to be rolled back or cleaned up are built based on the undo log lists.
 *
 * @param[in] recovery The recovery flag.
 */
void trx_lists_init_at_db_start(ib_recovery_t recovery);

/**
 * Starts a new transaction.
 *
 * @param[in] trx The transaction.
 * @param[in] rseg_id The rollback segment id; if ULINT_UNDEFINED is passed,
 *                    the system chooses the rollback segment automatically in a round-robin fashion.
 * @return true if success, false if the rollback segment could not support this many transactions.
 */
[[nodiscard]] bool trx_start(trx_t *trx, ulint rseg_id);

/**
 * Starts a new transaction.
 *
 * @param[in] trx The transaction.
 * @param[in] rseg_id The rollback segment id; if ULINT_UNDEFINED is passed,
 *                    the system chooses the rollback segment automatically in a round-robin fashion.
 *
 * @return true if success, false if the rollback segment could not support this many transactions.
 */
[[nodiscard]] bool trx_start_low(trx_t *trx, ulint rseg_id);

/**
 * Commits a transaction.
 *
 * @param[in] trx The transaction.
 */
void trx_commit_off_kernel(trx_t *trx);

/**
 * Cleans up a transaction at database startup. The cleanup is needed if
 * the transaction already got to the middle of a commit when the database
 * crashed, and we cannot roll it back.
 *
 * @param[in] trx The transaction.
 */
void trx_cleanup_at_db_startup(trx_t *trx);

/**
 * Does the transaction prepare.
 *
 * @param[in] trx The transaction.
 *
 * @return 0 or error number.
 */
[[nodiscard]] ulint trx_prepare(trx_t *trx);

/**
 * This function is used to find number of prepared transactions and
 * their transaction objects for a recovery. This function is used to
 * recover any X/Open XA distributed transactions.
 *
 * @param[in,out] xid_list Prepared transactions.
 * @param[in] len Number of slots in xid_list.
 *
 * @return Number of prepared transactions.
 */
[[nodiscard]] int trx_recover(XID *xid_list, ulint len);

#ifdef WITH_XOPEN
/**
 * This function is used to find one X/Open XA distributed transaction
 * which is in the prepared state
 *
 * @param[in] xid X/Open XA transaction identification
 *
 * @return trx or nullptr
 */
[[nodiscard]] trx_t *trx_get_trx_by_xid(XID *xid);
#endif /* WITH_XOPEN */

/**
 * If required, flushes the log to disk if we called trx_commit_()
 * with trx->m_flush_log_later == true.
 *
 * @param[in] trx The transaction handle.
 *
 * @return 0 or error number.
 */
[[nodiscard]] ulint trx_commit_complete_(trx_t *trx);

/**
 * Marks the latest SQL statement ended.
 *
 * @param[in] trx The transaction handle.
 */
void trx_mark_sql_stat_end(trx_t *trx); /*!< in: trx handle */

/**
 * Assigns a read view for a consistent read query. All the consistent reads
 * within the same transaction will get the same read view, which is created
 * when this function is first called for a new started transaction.
 *
 * @param[in] trx The active transaction.
 *
 * @return Consistent read view.
 */
[[nodiscard]] read_view_t *trx_assign_read_view(trx_t *trx);

/**
 * The transaction must be in the TRX_QUE_LOCK_WAIT state. Puts it to
 * the TRX_QUE_RUNNING state and releases query threads which were
 * waiting for a lock in the wait_thrs list.
 *
 * @param[in] trx The transaction.
 */
void trx_end_lock_wait(trx_t *trx);

/**
 * Sends a signal to a trx object.
 *
 * @param[in] trx The transaction handle.
 * @param[in] type The signal type.
 * @param[in] sender The sender type (TRX_SIG_SELF or TRX_SIG_OTHER_SESS).
 * @param[in] receiver_thr The query thread which wants the reply, or nullptr;
 *   if type is TRX_SIG_END_WAIT, this must be nullptr.
 * @param[in] savept Possible rollback savepoint, or nullptr.
 * @param[in,out] next_thr Next query thread to run; if the value which is
 *   passed in is a pointer to a nullptr pointer, then the calling function can
 *   start running a new query thread; if the parameter is nullptr, it is ignored.
 */
void trx_sig_send(trx_t *trx, ulint type, ulint sender, que_thr_t *receiver_thr, trx_savept_t *savept, que_thr_t **next_thr);

/**
 * Send the reply message when a signal in the queue of the trx has
 * been handled.
 *
 * @param[in] sig Signal object.
 * @param[in,out] next_thr Next query thread to run; if the value which is passed in is
 *   a pointer to a nullptr pointer, then the calling function can start running a new query thread.
 */
void trx_sig_reply(trx_sig_t *sig, que_thr_t **next_thr);

/**
 * Removes the signal object from a trx signal queue.
 *
 * @param[in] trx Trx handle.
 * @param[in] sig Signal object.
 */
void trx_sig_remove(trx_t *trx, trx_sig_t *sig);

/**
 * Starts handling of a trx signal.
 *
 * @param[in] trx Trx handle.
 * @param[in,out] next_thr Next query thread to run; if the value which is passed in is
 *   a pointer to a nullptr pointer, then the calling function can start running a new query thread.
 */
void trx_sig_start_handle(trx_t *trx, que_thr_t **next_thr);

/**
 * Ends signal handling. If the session is in the error state, and
 * trx->graph_before_signal_handling != nullptr, returns control to the error
 * handling routine of the graph (currently only returns the control to the
 * graph root which then sends an error message to the client).
 *
 * @param[in] trx Trx handle.
 */
void trx_end_signal_handling(trx_t *trx);

/**
 * Creates a commit command node struct.
 *
 * @param[in] heap Mem heap where created.
 * @return Commit node struct.
 */
[[nodiscard]] commit_node_t *commit_node_create(mem_heap_t *heap);

/**
 * Performs an execution step for a commit type node in a query graph.
 *
 * @param[in] thr Query thread.
 * @return Query thread to run next, or nullptr.
 */
[[nodiscard]] que_thr_t *trx_commit_step(que_thr_t *thr);

/**
 * Prints info about a transaction to the given file. The caller must own the
 * kernel mutex.
 *
 * @param[in] ib_stream Output stream.
 * @param[in] trx Transaction.
 * @param[in] max_query_len Max query length to print, or 0 to use the default max length.
 */
void trx_print(ib_stream_t ib_stream, trx_t *trx, ulint max_query_len);

/**
 * Determines if the currently running transaction has been interrupted.
 *
 * @param[in] trx Transaction.
 * @return True if interrupted.
 */
[[nodiscard]] bool trx_is_interrupted(const trx_t *trx);

/**
 * Compares the "weight" (or size) of two transactions. Transactions that
 * have edited non-transactional tables are considered heavier than ones
 * that have not.
 *
 * @param[in] a The first transaction to be compared.
 * @param[in] b The second transaction to be compared.
 * @return <0, 0 or >0; similar to strcmp(3).
 */
[[nodiscard]] int trx_weight_cmp(const trx_t *a, const trx_t *b);

/**
 * Creates a transaction object for client.
 *
 * @param[in] arg Pointer to client data.
 * @return Transaction object.
 */
[[nodiscard]] trx_t *trx_allocate_for_client(void *arg); /*!< in: pointer to client data */

/**
 * Does the transaction commit for client.
 *
 * @param[in] trx Trx handle.
 * @return DB_SUCCESS or error number.
 */
[[nodiscard]] db_err trx_commit(trx_t *trx); /*!< in: trx handle */

/**
 * Frees a transaction object for client.
 *
 * @param[in,out] trx Trx object.
 */
void trx_free_for_client(trx_t *&trx); /*!< in, own: trx object */

/** Reset global variables. */
void trx_var_init();

/**
 * Starts the transaction if it is not yet started.
 *
 * @param[in] trx Transaction.
 */
inline bool trx_start_if_not_started(trx_t *trx) {
  ut_ad(trx->m_conc_state != TRX_COMMITTED_IN_MEMORY);

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    return trx_start(trx, ULINT_UNDEFINED);
  } else {
    return true;
  }
}

/**
 * Retrieves the error_info field from a trx.
 *
 * @param[in] trx Trx object.
 * @return The error info.
 */
[[nodiscard]] inline const dict_index_t *trx_get_error_info(const trx_t *trx) {
  return trx->error_info;
}

/**
 * Retrieves transacion's id, represented as unsigned long long.
 *
 * @param[in] trx Transaction.
 * @return Transaction's id.
 */
[[nodiscard]] inline uint64_t trx_get_id(const trx_t *trx) {
  return trx->m_id;
}

/**
 * Retrieves transaction's que state in a human readable string.
 * The string should not be free()'d or modified.
 *
 * @param[in] trx Transaction.
 * @return String in the data segment.
 */
[[nodiscard]] inline const char *trx_get_que_state_str(const trx_t *trx) {
  /* be sure to adjust TRX_QUE_STATE_STR_MAX_LEN if you change this */
  switch (trx->m_que_state) {
    case TRX_QUE_RUNNING:
      return ("RUNNING");
    case TRX_QUE_LOCK_WAIT:
      return ("LOCK WAIT");
    case TRX_QUE_ROLLING_BACK:
      return ("ROLLING BACK");
    case TRX_QUE_COMMITTING:
      return ("COMMITTING");
    default:
      return ("UNKNOWN");
  }
}

/**
 * Determine if a transaction is a dictionary operation.
 *
 * @param[in] trx Transaction.
 * @return Dictionary operation mode.
 */
[[nodiscard]] inline trx_dict_op_t trx_get_dict_operation(const trx_t *trx) {
  auto op = static_cast<trx_dict_op_t>(trx->m_dict_operation);

#ifdef UNIV_DEBUG
  switch (op) {
    case TRX_DICT_OP_NONE:
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      return op;
  }
  ut_error;
#endif /* UNIV_DEBUG */
  return static_cast<trx_dict_op_t>(expect(op, TRX_DICT_OP_NONE));
}

/**
 * Flag a transaction a dictionary operation.
 *
 * @param[in/out] trx Transaction.
 * @param[in] op Operation, not TRX_DICT_OP_NONE.
 */
inline void trx_set_dict_operation( trx_t *trx, trx_dict_op_t op) {
#ifdef UNIV_DEBUG
  const auto old_op = trx_get_dict_operation(trx);

  switch (op) {
    case TRX_DICT_OP_NONE:
      ut_error;
      break;
    case TRX_DICT_OP_TABLE:
      switch (old_op) {
        case TRX_DICT_OP_NONE:
        case TRX_DICT_OP_INDEX:
        case TRX_DICT_OP_TABLE:
          trx->m_dict_operation = op;
	  return;
      }
      ut_error;
      break;
    case TRX_DICT_OP_INDEX:
      ut_ad(old_op == TRX_DICT_OP_NONE);
      break;
  }
#endif /* UNIV_DEBUG */

  trx->m_dict_operation = op;
}

/**
 * Calculates the "weight" of a transaction. The weight of one transaction
 * is estimated as the number of altered rows + the number of locked rows.
 *
 * @param[in] t Transaction.
 * @return Transaction weight.
 */
[[nodiscard]] inline ulint trx_weight(const trx_t* trx) {
  return trx->undo_no + UT_LIST_GET_LEN(trx->trx_locks);
}
