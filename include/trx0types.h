/*****************************************************************************
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

/**************************************************/ /**
 @file include/trx0types.h
 Transaction system global type definitions

 Created 3/26/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "ut0byte.h"
#include "que0types.h"
#include "sync0mutex.h"
#include "trx0xa.h"

/** Prepare trx_t::id for being printed via printf(3) */
#define TRX_ID_PREP_PRINTF(id) id

/** printf(3) format used for printing TRX_ID_PRINTF_PREP() */
#define TRX_ID_FMT "%lX"

/** maximum length that a formatted trx_t::id could take, not including
the terminating NUL character. */
constexpr ulint TRX_ID_MAX_LEN  = 17;

/** Transaction */
struct trx_t;

/** Transaction system */
struct Trx_sys;

/** Doublewrite information */
struct DBLWR;

/** Signal */
struct trx_sig_t;

/** Rollback segment */
struct trx_rseg_t;

/** Transaction undo log */
struct trx_undo_t;

/** Array of undo numbers of undo records being rolled back or purged */
struct trx_undo_arr_t;

/** A cell of trx_undo_arr_t */
struct trx_undo_inf_t;

/** The control structure used in the purge operation */
struct Purge_sys;

/** Rollback command node in a query graph */
struct roll_node_t;

/** Commit command node in a query graph */
struct commit_node_t;

/** SAVEPOINT command node in a query graph */
struct trx_named_savept_t;

/** Transaction savepoint */
struct trx_savept_t;

/** Lock instance. */
struct Lock;

struct sess_t;
struct dict_index_t;
struct read_view_t;

/** Rollback contexts */
enum trx_rb_ctx {
   /** No rollback */
  RB_NONE = 0,

  /** Normal rollback */
  RB_NORMAL,

  /** Rolling back an incomplete transaction, in crash recovery, rolling back
  an INSERT that was performed by updating a delete-marked record; if the
  delete-marked record no longer exists in an active read view, it will
  be purged. */
  RB_RECOVERY_PURGE_REC,

 /** Rolling back an incomplete transaction, in crash recovery */
  RB_RECOVERY
};

/** Type of data dictionary operation */
enum trx_dict_op_t {
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
};

/** Rollback pointer (DB_ROLL_PTR, DATA_ROLL_PTR) */
using roll_ptr_t = uint64_t;

/** Undo number */
using undo_no_t = uint64_t;

/** Transaction savepoint */
struct trx_savept_t {
   /** Least undo number to undo */
  undo_no_t least_undo_no;
};

/** Transaction system header */
using trx_sysf_t = byte;

/** Rollback segment header */
using trx_rsegf_t = byte;

/** Undo segment header */
using trx_usegf_t = byte;

/** Undo log header */
using trx_ulogf_t = byte;

/** Undo log page header */
using trx_upagef_t = byte;

/** Undo log record */
using trx_undo_rec_t = byte;

/** Maximum number of concurrent threads running a single operation of a
transaction, e.g., a parallel query */
constexpr ulint TRX_MAX_N_THREADS = 32;

/* Transaction concurrency states (trx->conc_state) */
enum Trx_status {
  TRX_NOT_STARTED,

  TRX_ACTIVE,

  TRX_COMMITTED_IN_MEMORY,

  /** Support for 2PC/XA */
  TRX_PREPARED = 3
};

/* Transaction execution states when trx->conc_state == TRX_ACTIVE */

/** transaction is running */
constexpr ulint TRX_QUE_RUNNING = 0;

/** transaction is waiting for a lock */
constexpr ulint TRX_QUE_LOCK_WAIT = 1 ;

/** transaction is rolling back */
constexpr ulint TRX_QUE_ROLLING_BACK = 2;

/** transaction is committing */
constexpr ulint TRX_QUE_COMMITTING = 3;

/** Transaction isolation levels (trx->isolation_level) */
enum Trx_isolation {
  /** dirty read: non-locking SELECTs are performed so that we
  do not look at a possible earlier version of a record; thus they
  are not 'consistent' reads under this isolation level; otherwise like level 2 */
  TRX_ISO_READ_UNCOMMITTED,

  /** somewhat Oracle-like isolation, except that in range UPDATE and DELETE
  we must block phantom rows with next-key locks;
    SELECT ... FOR UPDATE and ...  LOCK IN SHARE MODE only lock the index records,
    NOT the gaps before them, and thus allow free inserting;
    each consistent read reads its own snapshot */
  TRX_ISO_READ_COMMITTED,
  
  /** this is the default; all consistent reads in the same trx read the same
  snapshot; full next-key locking used in locking reads to block insertions into gaps */
  TRX_ISO_REPEATABLE_READ,
  
  /** all plain SELECTs are converted to LOCK IN SHARE MODE reads */
  TRX_ISO_SERIALIZABLE,
};

/* Treatment of duplicate values (trx->m_duplicates; for example, in inserts).
Multiple flags can be combined with bitwise OR. */

/** Duplicate rows are to be updated */
constexpr ulint TRX_DUP_IGNORE = 1;

/** Duplicate rows are to be replaced */
constexpr ulint TRX_DUP_REPLACE = 2;

// FIXME: Get rid of this magic number
/* Maximum length of a string that can be returned by trx_get_que_state_str(). */
/* "ROLLING BACK" */
// constexpr ulint TRX_QUE_STATE_STR_MAX_LEN = 12;

/* Types of a trx signal */
enum Trx_signal {
  TRX_SIG_NO_SIGNAL,
  TRX_SIG_TOTAL_ROLLBACK,
  TRX_SIG_ROLLBACK_TO_SAVEPT,
  TRX_SIG_COMMIT,
  TRX_SIG_ERROR_OCCURRED,
  TRX_SIG_BREAK_EXECUTION
};

/* Sender types of a signal */
enum Trx_signal_origin {
  /** sent by the session itself, or by an error occurring within this session */
  TRX_SIG_SELF = 0,

  /** sent by another session (which must hold rights to this) */
  TRX_SIG_OTHER_SESS = 1
};

/** Commit node states */
enum commit_node_state {
   /** about to send a commit signal to the transaction */
  COMMIT_NODE_SEND = 1,

  /** Commit signal sent to the transaction, waiting for completion */
  COMMIT_NODE_WAIT
};

/** Commit command node in a query graph */
struct commit_node_t {

  /** Node type: QUE_NODE_COMMIT */
  que_common_t common;

  /** Node execution state */
  commit_node_state state;
};

/* Signal to a transaction */
struct trx_sig_t {
  /* Signal type */
  unsigned type : 3;

  /** TRX_SIG_SELF or TRX_SIG_OTHER_SESS */
  unsigned sender : 1;

  /** non-nullptr if the sender of the signal wants reply after the operation induced by the signal is completed */
  que_thr_t *receiver;

  /** Possible rollback savepoint */
  trx_savept_t savept;

  /** queue of pending signals to the transaction */
  UT_LIST_NODE_T(trx_sig_t) signals;

  /** List of signals for which the sender transaction is waiting a reply */
  UT_LIST_NODE_T(trx_sig_t) reply_signals;
};

constexpr ulint TRX_MAGIC_N = 91118598;

/* The transaction handle; every session has a trx object which is freed only
when the session is freed; in addition there may be session-less transactions
rolling back after a database recovery */
struct trx_t {
  ulint m_magic_n;

  /** Transaction start id. See m_no below too. */
  trx_id_t m_id;

  /* These fields are not protected by any mutex. */

  /** English text describing the current operation, or an empty string */
  const char *m_op_info;

  /** State of the trx from the point of view of concurrency control:
   * TRX_ACTIVE, TRX_COMMITTED_IN_MEMORY, ... */
  Trx_status m_conc_state;

   /** TRX_ISO_REPEATABLE_READ, ... */
  Trx_isolation m_isolation_level;

  /** Normally true, but if the user wants to suppress foreign key
  checks, (in table imports, for example) we set this false */
  ulint m_check_foreigns;

#ifdef WITH_XOPEN
  /** X/Open XA transaction identification to identify a transaction branch */
  XID m_xid;

 /** Normally we do the XA two-phase commit steps, but by setting this to
  false, one can save CPU time and about 150 bytes in the undo log size
  as then we skip XA steps */
  ulint m_support_xa;

  /** In 2PC, we hold the prepare_commit mutex across both phases. In that
  case, we defer flush of the logs to disk until after we release the mutex. */
  ulint m_flush_log_later;

  /* This flag is set to true in trx_commit_off_kernel() if flush_log_later
  was true, and there were modifications by the transaction; in that case
  we must flush the log in trx_commit() */
  ulint m_must_flush_log_later;
#endif /* WITH_XOPEN */

  /** TRX_DUP_IGNORE | TRX_DUP_REPLACE */
  ulint m_duplicates;

  /* A mark field used in deadlock checking algorithm.  */
  bool m_deadlock_mark;

  /** @see trx_dict_op */
  trx_dict_op_t m_dict_operation;

  /* Fields protected by the srv_conc_mutex. */

  /* This is true if we have declared this transaction in
  srv_conc_enter_innodb to be inside the InnoDB engine */
  bool m_declared_to_be_inside_innodb;

  /* Fields protected by dict_operation_lock. The very latch it is used to track. */

  /** 0, RW_S_LATCH, or RW_X_LATCH: the latch mode trx currently holds on dict_operation_lock */
  ulint m_dict_operation_lock_mode;

  /* All the next fields are protected by the kernel mutex, except the
  undo logs which are protected by undo_mutex */

  /** false=user transaction, true=purge */
  bool m_is_purge;

  /** false=normal transaction, true=recovered, must be rolled back */
  bool m_is_recovered;

  /** Valid when conc_state == TRX_ACTIVE: TRX_QUE_RUNNING, TRX_QUE_LOCK_WAIT, ... */
  ulint m_que_state;

  /** This is true as long as the trx is handling signals */
  ulint m_handling_signals;

  /** Time the trx object was created or the state last time became TRX_ACTIVE */
  time_t m_start_time;

  /** Transaction serialization number == max trx id when the transaction is
  moved to COMMITTED_IN_MEMORY state. You can also think of this as the commit ID. */
  trx_id_t m_no;

  /** LSN at the time of the commit */
  lsn_t commit_lsn;

  /** Table to drop iff dict_operation is true, or 0. */
  trx_id_t table_id;

  /** Client thread handle corresponding to this trx, or nullptr */
  void *m_client_ctx;

  /** Pointer to the SQL query string */
  char **client_query_str;

  /** Number of Innobase tables used in the processing of the current SQL statement. */
  ulint n_client_tables_in_use;

  /** how many tables the current SQL statement uses, except those in consistent read */
  ulint client_n_tables_locked;

  /** List of transactions */
  UT_LIST_NODE_T(trx_t) trx_list;

  /** List of transactions created for client */
  UT_LIST_NODE_T(trx_t) client_trx_list;

  /*!< 0 if no error, otherwise error number; NOTE That ONLY the
  thread doing the transaction is allowed to set this field: this
  is NOT protected by the kernel mutex */
  db_err error_state;

   /** If the error number indicates a duplicate key error, a
   pointer to the problematic index is stored here */

  const dict_index_t *error_info;

  // FIXME: Remove this, it's MySQL specific.
  /** if the index creation fails to a duplicate key error,
  a key number of that index is stored here */
  ulint error_key_num;

  /** Session of the trx, nullptr if none */
  sess_t *sess;

  /** Query currently run in the session, or nullptr if none; NOTE that the query
  belongs to the session, and it can survive over a transaction commit, if
  it is a stored procedure with a COMMIT WORK statement, for instance */
  que_t *graph;

  /** Number of active query threads */
  ulint n_active_thrs;

  /** Value of graph when signal handling for this trx started: this is used to
  return control to the original query graph for error processing */
  que_t *graph_before_signal_handling;

  /** One signal object can be allocated in this space, avoiding mem_alloc */
  trx_sig_t sig;

  /** Queue of processed or pending signals to the trx */
  UT_LIST_BASE_NODE_T(trx_sig_t, signals) signals;

  /** List of signals sent by the query threads of this trx
  for which a thread is waiting for a reply; if this trx is killed,
  the reply requests in the list must be canceled */
  UT_LIST_BASE_NODE_T(trx_sig_t, reply_signals) reply_signals;

  /** If trx execution state is TRX_QUE_LOCK_WAIT, this points to the
  lock request, otherwise this is nullptr */
  Lock *wait_lock;

  /** When the transaction decides to wait for a lock, it sets this to false;
  if another transaction chooses this transaction as a victim in deadlock
  resolution, it sets this to true */
  bool was_chosen_as_deadlock_victim;

  /** Lock wait started at this time */
  time_t wait_started;

  /** Query threads belonging to this trx that are in the QUE_THR_LOCK_WAIT state */
  UT_LIST_BASE_NODE_T_EXTERN(que_thr_t, trx_thrs) wait_thrs;

  /** Memory heap for the locks of the transaction */
  mem_heap_t *lock_heap;

  /** Locks reserved by the transaction */
  UT_LIST_BASE_NODE_T_EXTERN(Lock, m_trx_locks) trx_locks;

  /** Memory heap for the global read view */
  mem_heap_t *global_read_view_heap;

  /** Consistent read view associated to a transaction or nullptr */
  read_view_t *global_read_view;

  /** Consistent read view used in the transaction or nullptr, this
  read view if defined can be normal read view associated to a transaction
  (i.e.  same as global_read_view) or read view associated to a cursor */
  read_view_t *read_view;

   /** Savepoints set with SAVEPOINT ..., oldest first */
  UT_LIST_BASE_NODE_T_EXTERN(trx_named_savept_t, trx_savepoints) trx_savepoints;

  /** Mutex protecting the fields in this section (down to undo_no_arr),
  EXCEPT last_sql_stat_start, which can be accessed only when we know
  that there cannot be any activity in the undo logs! */
  mutex_t undo_mutex;

  /** Next undo log record number to assign; since the undo log is private
  for a transaction, this is a simple ascending sequence with no gaps;
  thus it represents the number of modified/inserted rows in a transaction */
  undo_no_t undo_no;

  /** Undo_no when the last sql statement was started: in case of an error,
  trx is rolled back down to this undo number; see note at undo_mutex! */
  trx_savept_t last_sql_stat_start;

  /*!< rollback segment assigned to the transaction, or nullptr if not assigned yet */
  trx_rseg_t *rseg;

  /** Pointer to the insert undo log, or nullptr if no inserts performed yet */
  trx_undo_t *insert_undo;

  /** Pointer to the update undo log, or nullptr if no update performed yet */
  trx_undo_t *update_undo;

  /** Least undo number to undo during a rollback */
  undo_no_t roll_limit;

  /** Number of undo log pages undone since the last undo log truncation */
  ulint pages_undone;

  /** Array of undo numbers of undo log records which are currently processed
  by a rollback operation */
  trx_undo_arr_t *undo_no_arr;

  /** Detailed error message for last error, or empty. */
  char detailed_error[256];
};

UT_LIST_NODE_GETTER_DEFINITION(trx_t, trx_list);
UT_LIST_NODE_GETTER_DEFINITION(trx_t, client_trx_list);

