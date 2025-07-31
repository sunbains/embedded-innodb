/****************************************************************************
nCopyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/trx0trx.h
The transaction

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "dict0types.h"
#include "mem0mem.h"
#include "que0types.h"
#include "read0types.h"
#include "srv0srv.h"
#include "trx0types.h"
#include "usr0types.h"

struct Lock;
struct Read_view;

/*
 * The transaction handle; every session has a trx object which is freed only
 * when the session is freed; in addition there may be session-less transactions
 * rolling back after a database recovery
 */
struct Trx {
  /**
   * Constructs a transaction object.
   *
   * @param[in] trx_sys The owning transaction system.
   * @param[in] sess The session.
   * @param[in] arg Any context that needs to be passed to the trx.
   */
  explicit Trx(Trx_sys *trx_sys, Session *sess, void *arg) noexcept;

  /**
   * Destructs a transaction object.
   */
  ~Trx() noexcept;

  /**
   * Set detailed error message for the transaction.
   *
   * @param[in] trx The transaction struct.
   * @param[in] msg The detailed error message.
   */
  void set_detailed_error(const char *msg) noexcept;

  /**
   * Creates and initializes a transaction object.
   *
   * @param[in] trx_sys The owning transaction system.
   * @param[in] sess The session.
   * @param[in] arg Any context that needs to be passed to the trx.
   *
   * @return The transaction object.
   */
  [[nodiscard]] static Trx *create(Trx_sys *trx_sys, Session *sess, void *arg) noexcept;

  /**
   * Destroys a transaction instance..
   *
   * @param[in,own] trx              Transaction instance to free.
   */
  static void destroy(Trx *&trx) noexcept;

  /**
   * Creates a transaction object.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *allocate() noexcept;

  /**
   * Starts a new transaction.
   *
   * @param[in] rseg_id The rollback segment id; if ULINT_UNDEFINED is passed,
   *  the system chooses the rollback segment automatically in a round-robin fashion.
   *
   * @return true if success, false if the rollback segment could not support this many transactions.
   */
  [[nodiscard]] bool start_low(ulint rseg_id) noexcept;

  /**
   * Starts a new transaction.
   *
   * @param[in] rseg_id The rollback segment id; if ULINT_UNDEFINED is passed,
   *  the system chooses the rollback segment automatically in a round-robin fashion.
   * @return true if success, false if the rollback segment could not support this many transactions.
   */
  [[nodiscard]] bool start(ulint rseg_id) noexcept;

  /**
   * Commits a transaction.
   */
  void commit_off_kernel() noexcept;

  /**
   * Cleans up a transaction at database startup. The cleanup is needed if
   * the transaction already got to the middle of a commit when the database
   * crashed, and we cannot roll it back.
   *
   * @param[in] trx The transaction.
   */
  void cleanup_at_db_startup() noexcept;

  /**
   * Does the transaction prepare.
   *
   * @return 0 or error number.
   */
  [[nodiscard]] ulint prepare() noexcept;

  /**
   * Calculates the "weight" of a transaction. The weight of one transaction
   * is estimated as the number of altered rows + the number of locked rows.
   *
   * @param[in] t Transaction.
   *
   * @return Transaction weight.
   */
  [[nodiscard]] inline ulint weight() const noexcept { return m_undo_no + m_trx_locks.size(); }

  /**
   * Starts the transaction if it is not yet started.
   *
   * @return True if the transaction was started, false if it was already started.
   */
  [[nodiscard]] inline bool start_if_not_started() noexcept {
    ut_ad(m_conc_state != TRX_COMMITTED_IN_MEMORY);

    if (m_conc_state == TRX_NOT_STARTED) {
      return start(ULINT_UNDEFINED);
    } else {
      return true;
    }
  }

  /**
   * Retrieves the error_info field from a trx.
   *
   * @return The error info.
   */
  [[nodiscard]] inline const Index *get_error_info() const noexcept { return m_error_info; }

  /**
   * Retrieves transacion's id, represented as unsigned long long.
   *
   * @param[in] trx Transaction.
   *
   * @return Transaction's id.
   */
  [[nodiscard]] inline trx_id_t get_id() const noexcept { return m_id; }

  /**
   * Retrieves transaction's que state in a human readable string.
   * The string should not be free()'d or modified.
   *
   * @return String in the data segment.
   */
  [[nodiscard]] inline const char *get_que_state_str() const noexcept {
    /* be sure to adjust TRX_QUE_STATE_STR_MAX_LEN if you change this */
    switch (m_que_state) {
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
   * @return Dictionary operation mode.
   */
  [[nodiscard]] inline trx_dict_op_t get_dict_operation() const noexcept {
    const auto op = static_cast<trx_dict_op_t>(m_dict_operation);

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
   * @param[in] op Operation, not TRX_DICT_OP_NONE.
   */
  inline void set_dict_operation(trx_dict_op_t op) noexcept {
#ifdef UNIV_DEBUG
    const auto old_op = get_dict_operation();

    switch (op) {
      case TRX_DICT_OP_NONE:
        ut_error;
        break;
      case TRX_DICT_OP_TABLE:
        switch (old_op) {
          case TRX_DICT_OP_NONE:
          case TRX_DICT_OP_INDEX:
          case TRX_DICT_OP_TABLE:
            m_dict_operation = op;
            return;
        }
        ut_error;
        break;
      case TRX_DICT_OP_INDEX:
        ut_ad(old_op == TRX_DICT_OP_NONE);
        break;
    }
#endif /* UNIV_DEBUG */

    m_dict_operation = op;
  }

#ifdef WITH_XOPEN
  /**
   * If required, flushes the log to disk if we called trx_commit_()
   * with trx->m_flush_log_later == true.
   *
   * @return 0 or error number.
   */
  [[nodiscard]] ulint commit_flush_log() noexcept;
#endif /* WITH_XOPEN */

  /**
   * If required, flushes the log to disk if we called trx_commit_()
   * with trx->m_flush_log_later == true.
   *
   * @return 0 or error number.
   */
  [[nodiscard]] ulint commit_complete() noexcept;

  /**
   * Marks the latest SQL statement ended.
   */
  void mark_sql_stat_end() noexcept;

  /**
   * Assigns a read view for a consistent read query. All the consistent reads
   * within the same transaction will get the same read view, which is created
   * when this function is first called for a new started transaction.
   *
   * @return Consistent read view.
   */
  [[nodiscard]] Read_view *assign_read_view() noexcept;

  /**
   * The transaction must be in the TRX_QUE_LOCK_WAIT state. Puts it to
   * the TRX_QUE_RUNNING state and releases query threads which were
   * waiting for a lock in the wait_thrs list.
   *
   */
  void end_lock_wait() noexcept;

  /**
   * Sends a signal to a trx object.
   *
   * @param[in] type The signal type.
   * @param[in] sender The sender type (TRX_SIG_SELF or TRX_SIG_OTHER_SESS).
   * @param[in] receiver_thr The query thread which wants the reply, or nullptr;
   *   if type is TRX_SIG_END_WAIT, this must be nullptr.
   * @param[in] savept Possible rollback savepoint, or nullptr.
   * @param[in,out] next_thr Next query thread to run; if the value which is
   *   passed in is a pointer to a nullptr pointer, then the calling function can
   *   start running a new query thread; if the parameter is nullptr, it is ignored.
   */
  void sig_send(ulint type, ulint sender, que_thr_t *receiver_thr, trx_savept_t *savept, que_thr_t *&next_thr) noexcept;

  /**
   * Send the reply message when a signal in the queue of the trx has
   * been handled.
   *
   * @param[in] sig Signal object.
   * @param[in,out] next_thr Next query thread to run; if the value which is passed in is
   *   a pointer to a nullptr pointer, then the calling function can start running a new query thread.
   */
  static void sig_reply(trx_sig_t *sig, que_thr_t *&next_thr) noexcept;

  /**
   * Removes the signal object from a trx signal queue.
   *
   * @param[in] sig Signal object.
   */
  void sig_remove(trx_sig_t *&sig) noexcept;

  /**
   * Starts handling of a trx signal.
   *
   * @param[in,out] next_thr Next query thread to run; if the value which is passed in is
   *   a pointer to a nullptr pointer, then the calling function can start running a new query thread.
   */
  void sig_start_handling(que_thr_t *&next_thr) noexcept;

  /**
   * Ends signal handling. If the session is in the error state, and
   * trx->graph_before_signal_handling != nullptr, returns control to the error
   * handling routine of the graph (currently only returns the control to the
   * graph root which then sends an error message to the client).
   *
   */
  void end_signal_handling() noexcept;

  /**
   * Creates a commit command node struct.
   *
   * @param[in] heap Mem heap where created.
   *
   * @return Commit node struct.
   */
  [[nodiscard]] static Commit_node *commit_node_create(mem_heap_t *heap) noexcept;

  /**
   * Performs an execution step for a commit type node in a query graph.
   *
   * @param[in] thr Query thread.
   *
   * @return Query thread to run next, or nullptr.
   */
  [[nodiscard]] static que_thr_t *commit_step(que_thr_t *thr) noexcept;

  /**
   * Prints info about a transaction to the given file. The caller must own the
   * kernel mutex.
   *
   * @param[in] max_query_len Max query length to print, or 0 to use the default max length.
   *
   * @return String representation of the transaction.
   */
  [[nodiscard]] std::string to_string(ulint max_query_len) const noexcept;

  /**
   * Determines if the currently running transaction has been interrupted.
   *
   * @return True if interrupted.
   */
  [[nodiscard]] bool is_interrupted() const noexcept;

  /**
   * Compares the "weight" (or size) of two transactions. Transactions that
   * have edited non-transactional tables are considered heavier than ones
   * that have not.
   *
   * @param[in] lhs The first transaction to be compared.
   * @param[in] rh The second transaction to be compared.
   *
   * @return <0, 0 or >0; similar to strcmp(3).
   */
  [[nodiscard]] static int weight_cmp(const Trx *lhs, const Trx *rhs) noexcept;

  /**
   * Does the transaction commit for client.
   *
   * @return DB_SUCCESS or error number.
   */
  [[nodiscard]] db_err commit() noexcept;

  /**
   * Frees a transaction object for client.
   *
   */
  static void free_for_client(Trx *&trx) noexcept;

#ifdef UNIV_TEST
 private:
#endif /* UNIV_TEST */
  /**
   *  @brief Inserts the trx handle in the trx system trx list in the right position.
   *
   * The list is sorted on the trx id so that the biggest id is at the list start.
   * This function is used at the database startup to insert incomplete transactions to the list.
   *
   * @param[in]  trx The transaction handle to insert.
   */
  void t_insert_ordered(Trx *trx) noexcept;

  /**
   * @brief Commits a transaction.
   *
   * @note The kernel mutex is temporarily released.
   *
   * @param[in,out] next_thr Next query thread to run. If the value passed in is a pointer to
   *  a nullptr pointer, then the calling function can start running a new query thread.
   */
  void handle_commit_sig_off_kernel(que_thr_t *&next_thr) noexcept;

  /**
   * @brief checks the compatibility of a new signal with the other signals in the queue.
   *
   * @param[in] type the signal type.
   * @param[in] sender trx_sig_self or trx_sig_other_sess.
   *
   * @return true if the signal can be queued, false otherwise.
   */
  bool sig_is_compatible(ulint type, ulint sender) noexcept;

  /**
   * @brief Moves the query threads in the lock wait list to the SUSPENDED state.
   *
   * This function changes the state of all query threads in the lock wait list
   * of the given transaction to QUE_THR_SUSPENDED and updates the transaction
   * state to TRX_QUE_RUNNING.
   *
   * @param[in] trx The transaction in the TRX_QUE_LOCK_WAIT state.
   */
  void lock_wait_to_suspended() noexcept;

  /**
   * @brief Moves the query threads in the sig reply wait list of trx to the SUSPENDED state.
   *
   * @param[in] trx The transaction.
   */
  void sig_reply_wait_to_suspended() noexcept;

  /**
   * @brief Returns the approximate number of record locks for a transaction.
   *
   * This function calculates the approximate number of record locks (bits set in the bitmap)
   * for the specified transaction. Note that since delete-marked records may be removed,
   * the record count will not be precise.
   *
   * @return The approximate number of record locks for the transaction.
   */
  [[nodiscard]] ulint number_of_rows_locked() const noexcept;

  /**
   * @brief Prepares a transaction for commit.
   */
  void prepare_for_commit() noexcept;

 public:
  IF_DEBUG(ulint m_magic_n{TRX_MAGIC_N};)

  /** Transaction start id. See m_no below too. */
  trx_id_t m_id{};

  /* These fields are not protected by any mutex. */

  /** English text describing the current operation, or an empty string */
  const char *m_op_info{};

  /** State of the trx from the point of view of concurrency control:
   * TRX_ACTIVE, TRX_COMMITTED_IN_MEMORY, ... */
  Trx_status m_conc_state{TRX_NOT_STARTED};

  /** TRX_ISO_REPEATABLE_READ, ... */
  Trx_isolation m_isolation_level{TRX_ISO_REPEATABLE_READ};

  /** Normally true, but if the user wants to suppress foreign key
  checks, (in table imports, for example) we set this false */
  ulint m_check_foreigns{true};

#ifdef WITH_XOPEN
  /** X/Open XA transaction identification to identify a transaction branch */
  XID m_xid{};

  /** Normally we do the XA two-phase commit steps, but by setting this to
  false, one can save CPU time and about 150 bytes in the undo log size
  as then we skip XA steps */
  ulint m_support_xa{};

  /** In 2PC, we hold the prepare_commit mutex across both phases. In that
  case, we defer flush of the logs to disk until after we release the mutex. */
  ulint m_flush_log_later{};

  /* This flag is set to true in trx_commit_off_kernel() if flush_log_later
  was true, and there were modifications by the transaction; in that case
  we must flush the log in trx_commit() */
  ulint m_must_flush_log_later{};
#endif /* WITH_XOPEN */

  /** TRX_DUP_IGNORE | TRX_DUP_REPLACE */
  ulint m_duplicates{};

  /* A mark field used in deadlock checking algorithm.  */
  bool m_deadlock_mark{};

  /** @see trx_dict_op */
  trx_dict_op_t m_dict_operation{TRX_DICT_OP_NONE};

  /* Fields protected by the srv_conc_mutex. */

  /* This is true if we have declared this transaction in
  srv_conc_enter_innodb to be inside the InnoDB engine */
  bool m_declared_to_be_inside_innodb{};

  /* Fields protected by dict_operation_lock. The very latch it is used to track. */

  /** 0, RW_S_LATCH, or RW_X_LATCH: the latch mode trx currently holds on dict_operation_lock */
  ulint m_dict_operation_lock_mode{};

  /* All the next fields are protected by the kernel mutex, except the
  undo logs which are protected by undo_mutex */

  /** false=user transaction, true=purge */
  bool m_is_purge{};

  /** false=normal transaction, true=recovered, must be rolled back */
  bool m_is_recovered{};

  /** Valid when conc_state == TRX_ACTIVE: TRX_QUE_RUNNING, TRX_QUE_LOCK_WAIT, ... */
  ulint m_que_state{TRX_QUE_RUNNING};

  /** This is true as long as the trx is handling signals */
  ulint m_handling_signals{};

  /** Time the trx object was created or the state last time became TRX_ACTIVE */
  time_t m_start_time{};

  /** Transaction serialization number == max trx id when the transaction is
  moved to COMMITTED_IN_MEMORY state. You can also think of this as the commit ID. */
  trx_id_t m_no{LSN_MAX};

  /** LSN at the time of the commit */
  lsn_t m_commit_lsn{};

  /** Table to drop iff dict_operation is true, or 0. */
  trx_id_t m_table_id{};

  /** Client thread handle corresponding to this trx, or nullptr */
  void *m_client_ctx{};

  /** Pointer to the SQL query string */
  char **m_client_query_str{};

  /** Number of Innobase tables used in the processing of the current SQL statement. */
  ulint m_n_client_tables_in_use{};

  /** how many tables the current SQL statement uses, except those in consistent read */
  ulint m_client_n_tables_locked{};

  /** List of transactions */
  UT_LIST_NODE_T(Trx) m_trx_list;

  /** List of transactions created for client */
  UT_LIST_NODE_T(Trx) m_client_trx_list;

  /*!< 0 if no error, otherwise error number; NOTE That ONLY the
  thread doing the transaction is allowed to set this field: this
  is NOT protected by the kernel mutex */
  db_err m_error_state{DB_SUCCESS};

  /** If the error number indicates a duplicate key error, a
   pointer to the problematic index is stored here */
  const Index *m_error_info{};

  // FIXME: Remove this, it's MySQL specific.
  /** if the index creation fails to a duplicate key error,
  a key number of that index is stored here */
  ulint m_error_key_num{};

  /** Session of the trx, nullptr if none */
  Session *m_sess{};

  /** Query currently run in the session, or nullptr if none; NOTE that the query
  belongs to the session, and it can survive over a transaction commit, if
  it is a stored procedure with a COMMIT WORK statement, for instance */
  que_t *m_graph{};

  /** Number of active query threads */
  ulint m_n_active_thrs{};

  /** Value of graph when signal handling for this trx started: this is used to
  return control to the original query graph for error processing */
  que_t *m_graph_before_signal_handling{};

  /** One signal object can be allocated in this space, avoiding mem_alloc */
  trx_sig_t m_sig{};

  /** Queue of processed or pending signals to the trx */
  UT_LIST_BASE_NODE_T(trx_sig_t, signals) m_signals;

  /** List of signals sent by the query threads of this trx
  for which a thread is waiting for a reply; if this trx is killed,
  the reply requests in the list must be canceled */
  UT_LIST_BASE_NODE_T(trx_sig_t, reply_signals) m_reply_signals;

  /** If trx execution state is TRX_QUE_LOCK_WAIT, this points to the
  lock request, otherwise this is nullptr */
  Lock *m_wait_lock{};

  /** When the transaction decides to wait for a lock, it sets this to false;
  if another transaction chooses this transaction as a victim in deadlock
  resolution, it sets this to true */
  bool m_was_chosen_as_deadlock_victim{};

  /** Lock wait started at this time */
  time_t m_wait_started{};

  /** Query threads belonging to this trx that are in the QUE_THR_LOCK_WAIT state */
  UT_LIST_BASE_NODE_T_EXTERN(que_thr_t, trx_thrs) m_wait_thrs;

  /** Memory heap for the locks of the transaction */
  mem_heap_t *m_lock_heap{};

  /** Locks reserved by the transaction */
  UT_LIST_BASE_NODE_T_EXTERN(Lock, m_trx_locks) m_trx_locks;

  /** Memory heap for the global read view */
  mem_heap_t *m_global_read_view_heap;

  /** Consistent read view associated to a transaction or nullptr */
  Read_view *m_global_read_view{};

  /** Consistent read view used in the transaction or nullptr, this
  read view if defined can be normal read view associated to a transaction
  (i.e.  same as global_read_view) or read view associated to a cursor */
  Read_view *m_read_view{};

  /** Savepoints set with SAVEPOINT ..., oldest first */
  UT_LIST_BASE_NODE_T_EXTERN(trx_named_savept_t, trx_savepoints) m_trx_savepoints;

  /** Mutex protecting the fields in this section (down to undo_no_arr),
  EXCEPT last_sql_stat_start, which can be accessed only when we know
  that there cannot be any activity in the undo logs! */
  mutable mutex_t m_undo_mutex{};

  /** Next undo log record number to assign; since the undo log is private
  for a transaction, this is a simple ascending sequence with no gaps;
  thus it represents the number of modified/inserted rows in a transaction */
  undo_no_t m_undo_no{};

  /** Undo_no when the last sql statement was started: in case of an error,
  trx is rolled back down to this undo number; see note at undo_mutex! */
  trx_savept_t m_last_sql_stat_start{};

  /*!< rollback segment assigned to the transaction, or nullptr if not assigned yet */
  trx_rseg_t *m_rseg{};

  /** Pointer to the insert undo log, or nullptr if no inserts performed yet */
  trx_undo_t *m_insert_undo{};

  /** Pointer to the update undo log, or nullptr if no update performed yet */
  trx_undo_t *m_update_undo{};

  /** Least undo number to undo during a rollback */
  undo_no_t m_roll_limit{};

  /** Number of undo log pages undone since the last undo log truncation */
  uint32_t m_pages_undone{};

  /** Array of undo numbers of undo log records which are currently processed
  by a rollback operation */
  trx_undo_arr_t *m_undo_no_arr{};

  /** Detailed error message for last error, or empty. */
  std::array<char, 256> m_detailed_error{};

  /** Pointer to the parent trx system */
  Trx_sys *m_trx_sys{};
};

UT_LIST_NODE_GETTER_DEFINITION(Trx, m_trx_list);
UT_LIST_NODE_GETTER_DEFINITION(Trx, m_client_trx_list);
