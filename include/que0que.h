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

/** @file include/que0que.h
Query graph

Created 5/27/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "pars0types.h"
#include "que0types.h"
#include "row0types.h"
#include "srv0srv.h"
#include "trx0roll.h"
#include "trx0trx.h"
#include "usr0sess.h"
#include "usr0types.h"

/* If the following flag is set true, the module will print trace info
of SQL execution in the UNIV_SQL_DEBUG version */
extern bool que_trace_on;

/** Adds a query graph to the session's list of graphs. */

void que_graph_publish(
  que_t *graph, /*!< in: graph */
  Session *sess
); /*!< in: session */
/** Creates a query graph fork node.
@return	own: fork node */

que_fork_t *que_fork_create(
  que_t *graph,       /*!< in: graph, if NULL then this
                                    fork node is assumed to be the
                                    graph root */
  que_node_t *parent, /*!< in: parent node */
  ulint fork_type,    /*!< in: fork type */
  mem_heap_t *heap
); /*!< in: memory heap where created */

/** Creates a query graph thread node.
@return	own: query thread node */
que_thr_t *que_thr_create(
  que_fork_t *parent, /*!< in: parent node, i.e., a fork node */
  mem_heap_t *heap
); /*!< in: memory heap where created */

/** Frees a query graph, but not the heap where it was created. Does not free
explicit cursor declarations, they are freed in que_graph_free. */
void que_graph_free_recursive(que_node_t *node); /*!< in: query graph node */

/** Frees a query graph. */
void que_graph_free(que_t *graph); /*!< in: query graph; we assume that the
                                   memory heap where this graph was created is
                                   private to this graph: if not, then use
                                   que_graph_free_recursive and free the heap
                                   afterwards! */

/** Stops a query thread if graph or trx is in a state requiring it. The
conditions are tested in the order (1) graph, (2) trx. The kernel mutex has
to be reserved.
@return	true if stopped */
bool que_thr_stop(que_thr_t *thr); /*!< in: query thread */

/** Moves a thread from another state to the QUE_THR_RUNNING state. Increments
the n_active_thrs counters of the query graph and transaction if thr was
not active. */
void que_thr_move_to_run_state_for_client(
  que_thr_t *thr, /*!< in: an query thread */
  Trx *trx
); /*!< in: transaction */

/** The query thread is stopped and made inactive, except in the case where
it was put to the lock wait state in lock0lock.c, but the lock has already
been granted or the transaction chosen as a victim in deadlock resolution. */
void que_thr_stop_client(que_thr_t *thr); /*!< in: query thread */

/** Run a query thread. Handles lock waits. */
void que_run_threads(que_thr_t *thr); /*!< in: query thread */

/** After signal handling is finished, returns control to a query graph error
handling routine. (Currently, just returns the control to the root of the
graph so that the graph can communicate an error message to the client.) */
void que_fork_error_handle(
  Trx *trx, /*!< in: trx */
  que_t *fork
); /*!< in: query graph which was run before signal
                  handling started, NULL not allowed */

/** Moves a suspended query thread to the QUE_THR_RUNNING state and releases
a single worker thread to execute it. This function should be used to end
the wait state of a query thread waiting for a lock or a stored procedure
completion. */
void que_thr_end_wait(
  que_thr_t *thr, /*!< in: query thread in the
                                             QUE_THR_LOCK_WAIT,
                                             or QUE_THR_PROCEDURE_WAIT, or
                                             QUE_THR_SIG_REPLY_WAIT state */
  que_thr_t **next_thr
); /*!< in/out: next query thread to
                                             run; if the value which is passed
                                             in is a pointer to a NULL pointer,
                                             then the calling function can start
                                             running a new query thread */

/** Same as que_thr_end_wait, but no parameter next_thr available. */
void que_thr_end_wait_no_next_thr(que_thr_t *thr); /*!< in: query thread in the
                     QUE_THR_LOCK_WAIT,
                     or QUE_THR_PROCEDURE_WAIT, or
                     QUE_THR_SIG_REPLY_WAIT state */

/** Starts execution of a command in a query fork. Picks a query thread which
is not in the QUE_THR_RUNNING state and moves it to that state. If none
can be chosen, a situation which may arise in parallelized fetches, NULL
is returned.
@return a query thread of the graph moved to QUE_THR_RUNNING state, or
NULL; the query thread should be executed by que_run_threads by the
caller */
que_thr_t *que_fork_start_command(que_fork_t *fork); /*!< in: a query fork */

/** Get the first containing loop node (e.g. while_node_t or for_node_t) for the
given node, or NULL if the node is not within a loop.
@return	containing loop node, or NULL. */
que_node_t *que_node_get_containing_loop_node(que_node_t *node); /*!< in: node */

/** Prints info of an SQL query graph node. */
void que_node_print_info(que_node_t *node); /*!< in: query graph node */

/** Evaluate the given SQL
@return	error code or DB_SUCCESS */
db_err que_eval_sql(
  pars_info_t *info, /*!< in: info struct, or NULL */
  const char *sql,   /*!< in: SQL string */
  bool reserve_dict_mutex,
  /*!< in: if true, acquire/release
                    dict_sys->mutex around call to pars_sql. */
  Trx *trx
); /*!< in: trx */

/** Moves a thread from another state to the QUE_THR_RUNNING state. Increments
the n_active_thrs counters of the query graph and transaction if thr was
not active.
***NOTE***: This is the only functions in which such a transition is
allowed to happen! */
void que_thr_move_to_run_state(que_thr_t *thr); /*!< in: an query thread */

/** A patch for a client used to 'stop' a dummy query thread used in client
select, when there is no error or lock wait.
TODO: Currently only called from row0merge, needs to be removed. */
void que_thr_stop_for_client_no_error(
  que_thr_t *thr, /*!< in: query thread */
  Trx *trx
); /*!< in: transaction */

/** Reset the variables. */
void que_var_init(void);

/** Gets the trx of a query thread. */
inline Trx *thr_get_trx(que_thr_t *thr) /*!< in: query thread */
{
  ut_ad(thr);

  return (thr->graph->trx);
}

/** Determines if this thread is rolling back an incomplete transaction
in crash recovery.
@return true if thr is rolling back an incomplete transaction in crash
recovery */
inline bool thr_is_recv(const que_thr_t *thr) /*!< in: query thread */
{
  return (trx_is_recv(thr->graph->trx));
}

/** Gets the first thr in a fork. */
inline que_thr_t *que_fork_get_first_thr(que_fork_t *query_fork) /*!< in: query fork */
{
  return (UT_LIST_GET_FIRST(query_fork->thrs));
}

/** Gets the child node of the first thr in a fork. */
inline que_node_t *que_fork_get_child(que_fork_t *query_fork) /*!< in: query fork */
{
  que_thr_t *thr;

  thr = UT_LIST_GET_FIRST(query_fork->thrs);

  return (thr->child);
}

/** Gets the type of a graph node. */
inline ulint que_node_get_type(que_node_t *node) /*!< in: graph node */
{
  ut_ad(node);

  return (((que_common_t *)node)->type);
}

/** Gets pointer to the value dfield of a graph node. */
inline dfield_t *que_node_get_val(que_node_t *node) /*!< in: graph node */
{
  ut_ad(node);

  return (&(((que_common_t *)node)->val));
}

/** Gets the value buffer size of a graph node.
@return	val buffer size, not defined if val.data == NULL in node */
inline ulint que_node_get_val_buf_size(que_node_t *node) /*!< in: graph node */
{
  ut_ad(node);

  return (((que_common_t *)node)->val_buf_size);
}

/** Sets the value buffer size of a graph node. */
inline void que_node_set_val_buf_size(
  que_node_t *node, /*!< in: graph node */
  ulint size
) /*!< in: size */
{
  ut_ad(node);

  ((que_common_t *)node)->val_buf_size = size;
}

/** Sets the parent of a graph node. */
inline void que_node_set_parent(
  que_node_t *node, /*!< in: graph node */
  que_node_t *parent
) /*!< in: parent */
{
  ut_ad(node);

  ((que_common_t *)node)->parent = parent;
}

/** Gets pointer to the value data type field of a graph node. */
inline dtype_t *que_node_get_data_type(que_node_t *node) /*!< in: graph node */
{
  ut_ad(node);

  return (dfield_get_type(&((que_common_t *)node)->val));
}

/** Catenates a query graph node to a list of them, possible empty list.
@return	one-way list of nodes */
inline que_node_t *que_node_list_add_last(
  que_node_t *node_list, /*!< in: node list, or NULL */
  que_node_t *node
) /*!< in: node */
{
  que_common_t *cnode;
  que_common_t *cnode2;

  cnode = (que_common_t *)node;

  cnode->brother = nullptr;

  if (node_list == nullptr) {

    return (node);
  }

  cnode2 = (que_common_t *)node_list;

  while (cnode2->brother != nullptr) {
    cnode2 = (que_common_t *)cnode2->brother;
  }

  cnode2->brother = node;

  return (node_list);
}

/** Gets the next list node in a list of query graph nodes.
@return	next node in a list of nodes */
inline que_node_t *que_node_get_next(que_node_t *node) /*!< in: node in a list */
{
  return (((que_common_t *)node)->brother);
}

/** Gets a query graph node list length.
@return	length, for NULL list 0 */
inline ulint que_node_list_get_len(que_node_t *node_list) /*!< in: node list, or NULL */
{
  const que_common_t *cnode;
  ulint len;

  cnode = (const que_common_t *)node_list;
  len = 0;

  while (cnode != nullptr) {
    len++;
    cnode = (const que_common_t *)cnode->brother;
  }

  return (len);
}

/**
 * @brief Gets the parent node of a query graph node.
 * 
 * @param[in] node The query graph node.
 * 
 * @return The parent node or NULL.
 */
inline que_node_t *que_node_get_parent(que_node_t *node) {
  return (((que_common_t *)node)->parent);
}

/**
 * @brief Checks if graph, trx, or session is in a state where the query thread should be stopped.
 * 
 * @param[in] thr The query thread.
 * 
 * @return true if the query thread should be stopped.
 * 
 * @note If the peek is made without reserving the kernel mutex, then another peek with the
 * mutex reserved is necessary before deciding the actual stopping.
 */
inline bool que_thr_peek_stop(que_thr_t *thr) {
  auto graph = thr->graph;
  auto trx = graph->trx;

  if (graph->state != QUE_FORK_ACTIVE || trx->m_que_state == TRX_QUE_LOCK_WAIT || (!trx->m_signals.empty() && trx->m_que_state == TRX_QUE_RUNNING)) {

    return true;
  }

  return false;
}

/**
 * @brief Returns true if the query graph is for a SELECT statement.
 * 
 * @param[in] graph The query graph.
 * 
 * @return true if the query graph is for a SELECT statement.
 */
inline bool que_graph_is_select(que_t *graph) {
  return graph->fork_type == QUE_FORK_SELECT_SCROLL || graph->fork_type == QUE_FORK_SELECT_NON_SCROLL;
}
