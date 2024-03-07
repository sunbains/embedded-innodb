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

#include "dict0types.h"
#include "innodb0types.h"
#include "lock0types.h"
#include "mem0mem.h"
#include "que0types.h"
#include "read0types.h"
#include "srv0srv.h"
#include "trx0types.h"
#include "trx0xa.h"
#include "usr0types.h"
#include "ut0vec.h"

/** Dummy session used currently in MySQL interface */
extern sess_t *trx_dummy_sess;

/** Number of transactions currently allocated for MySQL: protected by
the kernel mutex */
extern ulint trx_n_transactions;

/** Set detailed error message for the transaction. */

void trx_set_detailed_error(
  trx_t *trx, /*!< in: transaction struct */
  const char *msg
); /*!< in: detailed error message */
/** Retrieves the error_info field from a trx.
@return	the error info */
inline const dict_index_t *trx_get_error_info(const trx_t *trx); /*!< in: trx object */
/** Creates and initializes a transaction object.
@return	own: the transaction */

trx_t *trx_create(sess_t *sess) /*!< in: session */
  __attribute__((nonnull));
/** Creates a transaction object.
@return	own: transaction object */

trx_t *trx_allocate_(void);
/** Creates a transaction object for background operations by the master thread.
@return	own: transaction object */

trx_t *trx_allocate_for_background(void);
/** Frees a transaction object. */

void trx_free_(trx_t *trx); /*!< in, own: trx object */
/** Frees a transaction object of a background operation of the master thread.
 */

void trx_free_for_background(trx_t *trx); /*!< in, own: trx object */
/** Creates trx objects for transactions and initializes the trx list of
trx_sys at database start. Rollback segment and undo log lists must
already exist when this function is called, because the lists of
transactions to be rolled back or cleaned up are built based on the
undo log lists. */

void trx_lists_init_at_db_start(ib_recovery_t recovery); /*!< in: recovery flag */
/** Starts a new transaction.
@return true if success, false if the rollback segment could not
support this many transactions */

bool trx_start(
  trx_t *trx, /*!< in: transaction */
  ulint rseg_id
); /*!< in: rollback segment id; if ULINT_UNDEFINED
                              is passed, the system chooses the rollback
                              segment automatically in a round-robin fashion */
/** Starts a new transaction.
@return	true */

bool trx_start_low(
  trx_t *trx, /*!< in: transaction */
  ulint rseg_id
); /*!< in: rollback segment id; if ULINT_UNDEFINED
                   is passed, the system chooses the rollback segment
                   automatically in a round-robin fashion */
/** Starts the transaction if it is not yet started. */
inline void trx_start_if_not_started(trx_t *trx); /*!< in: transaction */
/** Commits a transaction. */

void trx_commit_off_kernel(trx_t *trx); /*!< in: transaction */
/** Cleans up a transaction at database startup. The cleanup is needed if
the transaction already got to the middle of a commit when the database
crashed, and we cannot roll it back. */

void trx_cleanup_at_db_startup(trx_t *trx); /*!< in: transaction */
/** Does the transaction prepare.
@return	0 or error number */

ulint trx_prepare(trx_t *trx); /*!< in: trx handle */
/** This function is used to find number of prepared transactions and
their transaction objects for a recovery. This function is used to
recover any X/Open XA distributed transactions
@return	number of prepared transactions */

int trx_recover(
  XID *xid_list, /*!< in/out: prepared transactions */
  ulint len
); /*!< in: number of slots in xid_list */
#ifdef WITH_XOPEN
/** This function is used to find one X/Open XA distributed transaction
which is in the prepared state
@return	trx or NULL */

trx_t *trx_get_trx_by_xid(XID *xid); /*!< in: X/Open XA transaction identification */
#endif                               /* WITH_XOPEN */
/** If required, flushes the log to disk if we called trx_commit_()
with trx->flush_log_later == true.
@return	0 or error number */

ulint trx_commit_complete_(trx_t *trx); /*!< in: trx handle */
/** Marks the latest SQL statement ended. */

void trx_mark_sql_stat_end(trx_t *trx); /*!< in: trx handle */
/** Assigns a read view for a consistent read query. All the consistent reads
within the same transaction will get the same read view, which is created
when this function is first called for a new started transaction.
@return	consistent read view */

read_view_t *trx_assign_read_view(trx_t *trx); /*!< in: active transaction */
/** The transaction must be in the TRX_QUE_LOCK_WAIT state. Puts it to
the TRX_QUE_RUNNING state and releases query threads which were
waiting for a lock in the wait_thrs list. */

void trx_end_lock_wait(trx_t *trx); /*!< in: transaction */
/** Sends a signal to a trx object. */

void trx_sig_send(
  trx_t *trx,              /*!< in: trx handle */
  ulint type,              /*!< in: signal type */
  ulint sender,            /*!< in: TRX_SIG_SELF or
                             TRX_SIG_OTHER_SESS */
  que_thr_t *receiver_thr, /*!< in: query thread which wants the
                             reply, or NULL; if type is
                             TRX_SIG_END_WAIT, this must be NULL */
  trx_savept_t *savept,    /*!< in: possible rollback savepoint, or
                             NULL */
  que_thr_t **next_thr
); /*!< in/out: next query thread to run;
                             if the value which is passed in is
                             a pointer to a NULL pointer, then the
                             calling function can start running
                             a new query thread; if the parameter
                             is NULL, it is ignored */
/** Send the reply message when a signal in the queue of the trx has
been handled. */

void trx_sig_reply(
  trx_sig_t *sig, /*!< in: signal */
  que_thr_t **next_thr
); /*!< in/out: next query thread to run;
                                          if the value which is passed in is
                                          a pointer to a NULL pointer, then the
                                          calling function can start running
                                          a new query thread */
/** Removes the signal object from a trx signal queue. */

void trx_sig_remove(
  trx_t *trx, /*!< in: trx handle */
  trx_sig_t *sig
); /*!< in, own: signal */
/** Starts handling of a trx signal. */

void trx_sig_start_handle(
  trx_t *trx, /*!< in: trx handle */
  que_thr_t **next_thr
); /*!< in/out: next query thread to run;
                           if the value which is passed in is
                           a pointer to a NULL pointer, then the
                           calling function can start running
                           a new query thread */
/** Ends signal handling. If the session is in the error state, and
trx->graph_before_signal_handling != NULL, returns control to the error
handling routine of the graph (currently only returns the control to the
graph root which then sends an error message to the client). */

void trx_end_signal_handling(trx_t *trx); /*!< in: trx */
/** Creates a commit command node struct.
@return	own: commit node struct */

commit_node_t *commit_node_create(mem_heap_t *heap); /*!< in: mem heap where created */
/** Performs an execution step for a commit type node in a query graph.
@return	query thread to run next, or NULL */

que_thr_t *trx_commit_step(que_thr_t *thr); /*!< in: query thread */

/** Prints info about a transaction to the given file. The caller must own the
kernel mutex. */

void trx_print(
  ib_stream_t ib_stream, /*!< in: output stream */
  trx_t *trx,            /*!< in: transaction */
  ulint max_query_len
); /*!< in: max query length to print, or
                                      0 to use the default max length */

/** Type of data dictionary operation */
typedef enum trx_dict_op {
  /** The transaction is not modifying the data dictionary. */
  TRX_DICT_OP_NONE = 0,
  /** The transaction is creating a table or an index, or
  dropping a table.  The table must be dropped in crash
  recovery.  This and TRX_DICT_OP_NONE are the only possible
  operation modes in crash recovery. */
  TRX_DICT_OP_TABLE = 1,
  /** The transaction is creating or dropping an index in an
  existing table.  In crash recovery, the data dictionary
  must be locked, but the table must not be dropped. */
  TRX_DICT_OP_INDEX = 2
} trx_dict_op_t;

/** Determine if a transaction is a dictionary operation.
@return	dictionary operation mode */
inline enum trx_dict_op trx_get_dict_operation(const trx_t *trx) /*!< in: transaction */
  __attribute__((pure));
/** Flag a transaction a dictionary operation. */
inline void trx_set_dict_operation(
  trx_t *trx, /*!< in/out: transaction */
  enum trx_dict_op op
); /*!< in: operation, not
                                                         TRX_DICT_OP_NONE */

/** Determines if the currently running transaction has been interrupted.
@return	true if interrupted */

bool trx_is_interrupted(const trx_t *trx); /*!< in: transaction */

/** Calculates the "weight" of a transaction. The weight of one transaction
is estimated as the number of altered rows + the number of locked rows.
@param t	transaction
@return		transaction weight */
#define TRX_WEIGHT(t) ((t)->undo_no + UT_LIST_GET_LEN((t)->trx_locks))

/** Compares the "weight" (or size) of two transactions. Transactions that
have edited non-transactional tables are considered heavier than ones
that have not.
@return	<0, 0 or >0; similar to strcmp(3) */

int trx_weight_cmp(
  const trx_t *a, /*!< in: the first transaction to be compared */
  const trx_t *b
); /*!< in: the second transaction to be compared */

/** Retrieves transacion's id, represented as unsigned long long.
@return	transaction's id */
inline uint64_t trx_get_id(const trx_t *trx); /*!< in: transaction */

/** Creates a transaction object for client. */

trx_t *trx_allocate_for_client(void *arg); /*!< in: pointer to client data */

/** Does the transaction commit for client.
@return	DB_SUCCESS or error number */
db_err trx_commit(trx_t *trx); /*!< in: trx handle */

/** Frees a transaction object for client. */
void trx_free_for_client(trx_t *trx); /*!< in, own: trx object */

/* Maximum length of a string that can be returned by
trx_get_que_state_str(). */
#define TRX_QUE_STATE_STR_MAX_LEN 12 /* "ROLLING BACK" */

/** Retrieves transaction's que state in a human readable string. The string
should not be free()'d or modified.
@return	string in the data segment */
inline const char *trx_get_que_state_str(const trx_t *trx); /*!< in: transaction */

/** Reset global variables. */

void trx_var_init(void);

/* Signal to a transaction */
struct trx_sig_struct {
  unsigned type : 3;   /*!< signal type */
  unsigned sender : 1; /*!< TRX_SIG_SELF or
                       TRX_SIG_OTHER_SESS */
  que_thr_t *receiver; /*!< non-NULL if the sender of the signal
                       wants reply after the operation induced
                       by the signal is completed */
  trx_savept_t savept; /*!< possible rollback savepoint */
  UT_LIST_NODE_T(trx_sig_t)
  signals; /*!< queue of pending signals to the
           transaction */
  UT_LIST_NODE_T(trx_sig_t)
  reply_signals; /*!< list of signals for which the sender
                 transaction is waiting a reply */
};

#define TRX_MAGIC_N 91118598

/* The transaction handle; every session has a trx object which is freed only
when the session is freed; in addition there may be session-less transactions
rolling back after a database recovery */

struct trx_struct {
  ulint magic_n;

  /* These fields are not protected by any mutex. */
  const char *op_info;   /*!< English text describing the
                         current operation, or an empty
                         string */
  ulint conc_state;      /*!< state of the trx from the point
                         of view of concurrency control:
                         TRX_ACTIVE, TRX_COMMITTED_IN_MEMORY,
                         ... */
  ulint isolation_level; /* TRX_ISO_REPEATABLE_READ, ... */
  ulint check_foreigns;  /* normally true, but if the user
                         wants to suppress foreign key checks,
                         (in table imports, for example) we
                         set this false */
  ulint check_unique_secondary;
  /*!< normally true, but if the user
  wants to speed up inserts by
  suppressing unique key checks
  for secondary indexes when we decide
  if we can use the insert buffer for
  them, we set this false */
#ifdef WITH_XOPEN
  ulint support_xa;             /*!< normally we do the XA two-phase
                              commit steps, but by setting this to
                              false, one can save CPU time and about
                              150 bytes in the undo log size as then
                              we skip XA steps */
  ulint flush_log_later;        /* In 2PC, we hold the
                              prepare_commit mutex across
                              both phases. In that case, we
                              defer flush of the logs to disk
                              until after we release the
                              mutex. */
  ulint must_flush_log_later;   /* this flag is set to true in
                         trx_commit_off_kernel() if
                         flush_log_later was true, and there
                         were modifications by the transaction;
                         in that case we must flush the log
                         in trx_commit() */
#endif                          /* WITH_XOPEN */
  ulint duplicates;             /*!< TRX_DUP_IGNORE | TRX_DUP_REPLACE */
  ulint deadlock_mark;          /*!< a mark field used in deadlock
                                checking algorithm.  */
  trx_dict_op_t dict_operation; /**< @see enum trx_dict_op */

  /* Fields protected by the srv_conc_mutex. */
  ulint declared_to_be_inside_innodb;
  /* this is true if we have declared
  this transaction in
  srv_conc_enter_innodb to be inside the
  InnoDB engine */

  /* Fields protected by dict_operation_lock. The very latch
  it is used to track. */
  ulint dict_operation_lock_mode;
  /*!< 0, RW_S_LATCH, or RW_X_LATCH:
  the latch mode trx currently holds
  on dict_operation_lock */

  /* All the next fields are protected by the kernel mutex, except the
  undo logs which are protected by undo_mutex */
  ulint is_purge;         /*!< 0=user transaction, 1=purge */
  ulint is_recovered;     /*!< 0=normal transaction,
                          1=recovered, must be rolled back */
  ulint que_state;        /*!< valid when conc_state
                          == TRX_ACTIVE: TRX_QUE_RUNNING,
                          TRX_QUE_LOCK_WAIT, ... */
  ulint handling_signals; /* this is true as long as the trx
                         is handling signals */
  time_t start_time;      /*!< time the trx object was created
                          or the state last time became
                          TRX_ACTIVE */
  trx_id_t id;            /*!< transaction id */
#ifdef WITH_XOPEN
  XID xid;             /*!< X/Open XA transaction
                       identification to identify a
                       transaction branch */
#endif                 /* WITH_XOPEN */
  trx_id_t no;         /*!< transaction serialization number ==
                       max trx id when the transaction is
                       moved to COMMITTED_IN_MEMORY state */
  uint64_t commit_lsn; /*!< lsn at the time of the commit */
  trx_id_t table_id;   /*!< Table to drop iff dict_operation
                       is true, or 0. */
  /*------------------------------*/
  void *client_thd;                /*!< Client thread handle corresponding
                                   to this trx, or NULL */
  char **client_query_str;         /*!< pointer to the SQL query string */
  os_thread_id_t client_thread_id; /*!< id of the user thread associated
                                  with this transaction object */
  ulint client_process_no;         /*!< since in Linux, 'top' reports
                                 process id's and not thread id's, we
                                 store the process number too */
  /*------------------------------*/
  ulint n_client_tables_in_use; /*!< number of Innobase tables
                        used in the processing of the current
                        SQL statement. */
  ulint client_n_tables_locked;
  /*!< how many tables the current SQL
  statement uses, except those
  in consistent read */
  /*------------------------------*/
  UT_LIST_NODE_T(trx_t)
  trx_list; /*!< list of transactions */
  UT_LIST_NODE_T(trx_t)
  client_trx_list; /*!< list of transactions created for
                   client */
  /*------------------------------*/
  enum db_err error_state;        /*!< 0 if no error, otherwise error
                                  number; NOTE That ONLY the thread
                                  doing the transaction is allowed to
                                  set this field: this is NOT protected
                                  by the kernel mutex */
  const dict_index_t *error_info; /*!< if the error number indicates a
                                  duplicate key error, a pointer to
                                  the problematic index is stored here */
  ulint error_key_num;            /*!< if the index creation fails to a
                                  duplicate key error, a key number of
                                  that index is stored here */
  sess_t *sess;                   /*!< session of the trx, NULL if none */
  que_t *graph;                   /*!< query currently run in the session,
                                  or NULL if none; NOTE that the query
                                  belongs to the session, and it can
                                  survive over a transaction commit, if
                                  it is a stored procedure with a COMMIT
                                  WORK statement, for instance */
  ulint n_active_thrs;            /*!< number of active query threads */
  que_t *graph_before_signal_handling;
  /*!< value of graph when signal handling
  for this trx started: this is used to
  return control to the original query
  graph for error processing */
  trx_sig_t sig; /*!< one signal object can be allocated
                 in this space, avoiding mem_alloc */
  UT_LIST_BASE_NODE_T(trx_sig_t)
  signals; /*!< queue of processed or pending
           signals to the trx */
  UT_LIST_BASE_NODE_T(trx_sig_t)
  reply_signals; /*!< list of signals sent by the query
                 threads of this trx for which a thread
                 is waiting for a reply; if this trx is
                 killed, the reply requests in the list
                 must be canceled */
  /*------------------------------*/
  lock_t *wait_lock; /*!< if trx execution state is
                     TRX_QUE_LOCK_WAIT, this points to
                     the lock request, otherwise this is
                     NULL */
  bool was_chosen_as_deadlock_victim;
  /*!< when the transaction decides to
  wait for a lock, it sets this to false;
  if another transaction chooses this
  transaction as a victim in deadlock
  resolution, it sets this to true */
  time_t wait_started; /*!< lock wait started at this time */
  UT_LIST_BASE_NODE_T(que_thr_t)
  wait_thrs; /*!< query threads belonging to this
             trx that are in the QUE_THR_LOCK_WAIT
             state */
  /*------------------------------*/
  mem_heap_t *lock_heap; /*!< memory heap for the locks of the
                         transaction */
  UT_LIST_BASE_NODE_T(lock_t)
  trx_locks; /*!< locks reserved by the
             transaction */
  /*------------------------------*/
  mem_heap_t *global_read_view_heap;
  /*!< memory heap for the global read
  view */
  read_view_t *global_read_view;
  /*!< consistent read view associated
  to a transaction or NULL */
  read_view_t *read_view; /*!< consistent read view used in the
                          transaction or NULL, this read view
                          if defined can be normal read view
                          associated to a transaction (i.e.
                          same as global_read_view) or read view
                          associated to a cursor */
  /*------------------------------*/
  UT_LIST_BASE_NODE_T(trx_named_savept_t)
  trx_savepoints; /*!< savepoints set with SAVEPOINT ...,
                  oldest first */
  /*------------------------------*/
  mutex_t undo_mutex; /*!< mutex protecting the fields in this
                      section (down to undo_no_arr), EXCEPT
                      last_sql_stat_start, which can be
                      accessed only when we know that there
                      cannot be any activity in the undo
                      logs! */
  undo_no_t undo_no;  /*!< next undo log record number to
                      assign; since the undo log is
                      private for a transaction, this
                      is a simple ascending sequence
                      with no gaps; thus it represents
                      the number of modified/inserted
                      rows in a transaction */
  trx_savept_t last_sql_stat_start;
  /*!< undo_no when the last sql statement
  was started: in case of an error, trx
  is rolled back down to this undo
  number; see note at undo_mutex! */
  trx_rseg_t *rseg;            /*!< rollback segment assigned to the
                               transaction, or NULL if not assigned
                               yet */
  trx_undo_t *insert_undo;     /*!< pointer to the insert undo log, or
                               NULL if no inserts performed yet */
  trx_undo_t *update_undo;     /*!< pointer to the update undo log, or
                               NULL if no update performed yet */
  undo_no_t roll_limit;        /*!< least undo number to undo during
                               a rollback */
  ulint pages_undone;          /*!< number of undo log pages undone
                               since the last undo log truncation */
  trx_undo_arr_t *undo_no_arr; /*!< array of undo numbers of undo log
                               records which are currently processed
                               by a rollback operation */
  /*------------------------------*/
  char detailed_error[256]; /*!< detailed error message for last
                            error, or empty. */
};

#define TRX_MAX_N_THREADS \
  32 /*!< maximum number of                                                    \
     concurrent threads running a                                              \
     single operation of a                                                     \
     transaction, e.g., a parallel                                             \
     query */
/* Transaction concurrency states (trx->conc_state) */
#define TRX_NOT_STARTED 0
#define TRX_ACTIVE 1
#define TRX_COMMITTED_IN_MEMORY 2
#define TRX_PREPARED 3 /*!< Support for 2PC/XA */

/* Transaction execution states when trx->conc_state == TRX_ACTIVE */
#define TRX_QUE_RUNNING 0 /*!< transaction is running */
#define TRX_QUE_LOCK_WAIT \
  1                            /*!< transaction is waiting                     \
                               for a lock */
#define TRX_QUE_ROLLING_BACK 2 /*!< transaction is rolling back */
#define TRX_QUE_COMMITTING 3   /*!< transaction is committing */

/* Transaction isolation levels (trx->isolation_level) */
#define TRX_ISO_READ_UNCOMMITTED \
  0 /*!< dirty read: non-locking                                               \
    SELECTs are performed so that                                              \
    we do not look at a possible                                               \
    earlier version of a record;                                               \
    thus they are not 'consistent'                                             \
    reads under this isolation                                                 \
    level; otherwise like level                                                \
    2 */

#define TRX_ISO_READ_COMMITTED \
  1 /*!< somewhat Oracle-like                                                  \
    isolation, except that in                                                  \
    range UPDATE and DELETE we                                                 \
    must block phantom rows                                                    \
    with next-key locks;                                                       \
    SELECT ... FOR UPDATE and ...                                              \
    LOCK IN SHARE MODE only lock                                               \
    the index records, NOT the                                                 \
    gaps before them, and thus                                                 \
    allow free inserting;                                                      \
    each consistent read reads its                                             \
    own snapshot */

#define TRX_ISO_REPEATABLE_READ \
  2 /*!< this is the default;                                                  \
    all consistent reads in the                                                \
    same trx read the same                                                     \
    snapshot;                                                                  \
    full next-key locking used                                                 \
    in locking reads to block                                                  \
    insertions into gaps */

#define TRX_ISO_SERIALIZABLE \
  3 /*!< all plain SELECTs are                                                 \
    converted to LOCK IN SHARE                                                 \
    MODE reads */

/* Treatment of duplicate values (trx->duplicates; for example, in inserts).
Multiple flags can be combined with bitwise OR. */
#define TRX_DUP_IGNORE 1  /*!< duplicate rows are to be updated */
#define TRX_DUP_REPLACE 2 /*!< duplicate rows are to be replaced */

/* Types of a trx signal */
#define TRX_SIG_NO_SIGNAL 0
#define TRX_SIG_TOTAL_ROLLBACK 1
#define TRX_SIG_ROLLBACK_TO_SAVEPT 2
#define TRX_SIG_COMMIT 3
#define TRX_SIG_ERROR_OCCURRED 4
#define TRX_SIG_BREAK_EXECUTION 5

/* Sender types of a signal */
#define TRX_SIG_SELF \
  0 /*!< sent by the session itself, or                                        \
    by an error occurring within this                                          \
    session */
#define TRX_SIG_OTHER_SESS \
  1 /*!< sent by another session (which                                        \
    must hold rights to this) */

/** Commit node states */
enum commit_node_state {
  COMMIT_NODE_SEND = 1, /*!< about to send a commit signal to
                        the transaction */
  COMMIT_NODE_WAIT      /*!< commit signal sent to the transaction,
                        waiting for completion */
};

/** Commit command node in a query graph */
struct commit_node_struct {
  que_common_t common;          /*!< node type: QUE_NODE_COMMIT */
  enum commit_node_state state; /*!< node execution state */
};

#ifndef UNIV_NONINL
#include "trx0trx.ic"
#endif
