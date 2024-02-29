/**
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.

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

#ifndef trx0roll_h
#define trx0roll_h

#include "innodb0types.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0types.h"

#define trx_roll_free_all_savepoints(s) trx_roll_savepoints_free((s), NULL)

/** Determines if this transaction is rolling back an incomplete transaction
in crash recovery.
@return true if trx is an incomplete transaction that is being rolled
back in crash recovery */

bool trx_is_recv(const trx_t *trx); /*!< in: transaction */
/** Returns a transaction savepoint taken at this point in time.
@return	savepoint */

trx_savept_t trx_savept_take(trx_t *trx); /*!< in: transaction */
/** Creates an undo number array. */

trx_undo_arr_t *trx_undo_arr_create(void);
/** Frees an undo number array. */

void trx_undo_arr_free(trx_undo_arr_t *arr); /*!< in: undo number array */
/** Returns pointer to nth element in an undo number array.
@return	pointer to the nth element */
inline trx_undo_inf_t *
trx_undo_arr_get_nth_info(trx_undo_arr_t *arr, /*!< in: undo number array */
                          ulint n);            /*!< in: position */
/** Tries truncate the undo logs. */

void trx_roll_try_truncate(trx_t *trx); /*!< in/out: transaction */
/** Pops the topmost record when the two undo logs of a transaction are seen
as a single stack of records ordered by their undo numbers. Inserts the
undo number of the popped undo record to the array of currently processed
undo numbers in the transaction. When the query thread finishes processing
of this undo record, it must be released with trx_undo_rec_release.
@return undo log record copied to heap, NULL if none left, or if the
undo number of the top record would be less than the limit */

trx_undo_rec_t *trx_roll_pop_top_rec_of_trx(
    trx_t *trx,           /*!< in: transaction */
    undo_no_t limit,      /*!< in: least undo number we need */
    roll_ptr_t *roll_ptr, /*!< out: roll pointer to undo record */
    mem_heap_t *heap);    /*!< in: memory heap where copied */
/** Reserves an undo log record for a query thread to undo. This should be
called if the query thread gets the undo log record not using the pop
function above.
@return	true if succeeded */

bool trx_undo_rec_reserve(
    trx_t *trx,         /*!< in/out: transaction */
    undo_no_t undo_no); /*!< in: undo number of the record */
/** Releases a reserved undo record. */

void trx_undo_rec_release(trx_t *trx,         /*!< in/out: transaction */
                          undo_no_t undo_no); /*!< in: undo number */
/** Starts a rollback operation. */

void trx_rollback(trx_t *trx,     /*!< in: transaction */
                  trx_sig_t *sig, /*!< in: signal starting the rollback */
                  que_thr_t **next_thr); /*!< in/out: next query thread to run;
                                       if the value which is passed in is
                                       a pointer to a NULL pointer, then the
                                       calling function can start running
                                       a new query thread */
/** Rollback or clean up any incomplete transactions which were
encountered in crash recovery.  If the transaction already was
committed, then we clean up a possible insert undo log. If the
transaction was not yet committed, then we roll it back. */

void trx_rollback_or_clean_recovered(
    bool all); /*!< in: false=roll back dictionary transactions;
                true=roll back all non-PREPARED transactions */
/** Rollback or clean up any incomplete transactions which were
encountered in crash recovery.  If the transaction already was
committed, then we clean up a possible insert undo log. If the
transaction was not yet committed, then we roll it back.
Note: this is done in a background thread.
@return	a dummy parameter */
void *trx_rollback_or_clean_all_recovered(void *arg __attribute__((unused)));

/*!< in: a dummy parameter required by
os_thread_create */
/** Finishes a transaction rollback. */

void trx_finish_rollback_off_kernel(
    que_t *graph,          /*!< in: undo graph which can now be freed */
    trx_t *trx,            /*!< in: transaction */
    que_thr_t **next_thr); /*!< in/out: next query thread to run;
                         if the value which is passed in is
                         a pointer to a NULL pointer, then the
                         calling function can start running
                         a new query thread; if this parameter is
                         NULL, it is ignored */
/** Builds an undo 'query' graph for a transaction. The actual rollback is
performed by executing this query graph like a query subprocedure call.
The reply about the completion of the rollback will be sent by this
graph.
@return	own: the query graph */

que_t *trx_roll_graph_build(trx_t *trx); /*!< in: trx handle */

/** Creates a rollback command node struct.
@return	own: rollback node struct */
roll_node_t *
roll_node_create(mem_heap_t *heap); /*!< in: mem heap where created */

/** Performs an execution step for a rollback command node in a query graph.
@return	query thread to run next, or NULL */

que_thr_t *trx_rollback_step(que_thr_t *thr); /*!< in: query thread */
/** Rollback a user transaction.
@return	error code or DB_SUCCESS */

db_err trx_general_rollback(
    trx_t *trx,            /*!< in: transaction handle */
    bool partial,          /*!< in: true if partial rollback requested */
    trx_savept_t *savept); /*!< in: pointer to savepoint undo number, if
                           partial rollback requested */

/** Frees savepoint structs starting from savep, if savep == NULL then
free all savepoints. */

void trx_roll_savepoints_free(
    trx_t *trx,                 /*!< in: transaction handle */
    trx_named_savept_t *savep); /*!< in: free all savepoints > this one;
                                if this is NULL, free all savepoints
                                of trx */

/** A cell of trx_undo_arr_struct; used during a rollback and a purge */
struct trx_undo_inf_struct {
  trx_id_t trx_no;   /*!< transaction number: not defined during
                     a rollback */
  undo_no_t undo_no; /*!< undo number of an undo record */
  bool in_use;       /*!< true if the cell is in use */
};

/** During a rollback and a purge, undo numbers of undo records currently being
processed are stored in this array */

struct trx_undo_arr_struct {
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

/** Rollback command node in a query graph */
struct roll_node_struct {
  que_common_t common;        /*!< node type: QUE_NODE_ROLLBACK */
  enum roll_node_state state; /*!< node execution state */
  bool partial;               /*!< true if we want a partial
                               rollback */
  trx_savept_t savept;        /*!< savepoint to which to
                              roll back, in the case of a
                              partial rollback */
};

/** A savepoint set with SQL's "SAVEPOINT savepoint_id" command */
struct trx_named_savept_struct {
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

#ifndef UNIV_NONINL
#include "trx0roll.ic"
#endif

#endif
