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

#include "que0types.h"
#include "sync0mutex.h"
#include "trx0xa.h"
#include "ut0byte.h"

/** Prepare trx_t::id for being printed via printf(3) */
#define TRX_ID_PREP_PRINTF(id) id

/** printf(3) format used for printing TRX_ID_PRINTF_PREP() */
#define TRX_ID_FMT "%lX"

/** maximum length that a formatted trx_t::id could take, not including
the terminating NUL character. */
constexpr ulint TRX_ID_MAX_LEN = 17;

/** Transaction */
struct Trx;

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
struct Commit_node;

/** SAVEPOINT command node in a query graph */
struct trx_named_savept_t;

/** Transaction savepoint */
struct trx_savept_t;

/** Lock instance. */
struct Lock;

struct Session;
struct Index;
struct Read_view;

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
constexpr ulint TRX_QUE_LOCK_WAIT = 1;

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
struct Commit_node {

  /** Node type: QUE_NODE_COMMIT */
  que_common_t common;

  /** Node execution state */
  commit_node_state state;
};

/* Signal to a transaction */
struct trx_sig_t {

  /** Next signal in the queue */
  trx_sig_t *next() noexcept { return signals.m_next; }

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
