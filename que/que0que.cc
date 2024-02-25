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

/** @file que/que0que.c
Query graph

Created 5/27/1996 Heikki Tuuri
*******************************************************/

#include "que0que.h"

#ifdef UNIV_NONINL
#include "que0que.ic"
#endif

#include "dict0crea.h"
#include "eval0eval.h"
#include "eval0proc.h"
#include "log0log.h"
#include "pars0types.h"
#include "row0ins.h"
#include "row0purge.h"
#include "row0sel.h"
#include "row0undo.h"
#include "row0upd.h"
#include "srv0que.h"
#include "trx0roll.h"
#include "trx0trx.h"
#include "usr0sess.h"

#define QUE_PARALLELIZE_LIMIT (64 * 256 * 256 * 256)
#define QUE_ROUND_ROBIN_LIMIT (64 * 256 * 256 * 256)
#define QUE_MAX_LOOPS_WITHOUT_CHECK 16

#ifdef UNIV_DEBUG
/* If the following flag is set TRUE, the module will print trace info
of SQL execution in the UNIV_SQL_DEBUG version */
ibool que_trace_on = FALSE;
#endif /* UNIV_DEBUG */

/* Short introduction to query graphs
   ==================================

A query graph consists of nodes linked to each other in various ways. The
execution starts at que_run_threads() which takes a que_thr_t parameter.
que_thr_t contains two fields that control query graph execution: run_node
and prev_node. run_node is the next node to execute and prev_node is the
last node executed.

Each node has a pointer to a 'next' statement, i.e., its brother, and a
pointer to its parent node. The next pointer is nullptr in the last statement
of a block.

Loop nodes contain a link to the first statement of the enclosed statement
list. While the loop runs, que_thr_step() checks if execution to the loop
node came from its parent or from one of the statement nodes in the loop. If
it came from the parent of the loop node it starts executing the first
statement node in the loop. If it came from one of the statement nodes in
the loop, then it checks if the statement node has another statement node
following it, and runs it if so.

To signify loop ending, the loop statements (see e.g. while_step()) set
que_thr_t->run_node to the loop node's parent node. This is noticed on the
next call of que_thr_step() and execution proceeds to the node pointed to by
the loop node's 'next' pointer.

For example, the code:

X := 1;
WHILE X < 5 LOOP
 X := X + 1;
 X := X + 1;
X := 5

will result in the following node hierarchy, with the X-axis indicating
'next' links and the Y-axis indicating parent/child links:

A - W - A
    |
    |
    A - A

A = assign_node_t, W = while_node_t. */

/* How a stored procedure containing COMMIT or ROLLBACK commands
is executed?

The commit or rollback can be seen as a subprocedure call.
The problem is that if there are several query threads
currently running within the transaction, their action could
mess the commit or rollback operation. Or, at the least, the
operation would be difficult to visualize and keep in control.

Therefore the query thread requesting a commit or a rollback
sends to the transaction a signal, which moves the transaction
to TRX_QUE_SIGNALED state. All running query threads of the
transaction will eventually notice that the transaction is now in
this state and voluntarily suspend themselves. Only the last
query thread which suspends itself will trigger handling of
the signal.

When the transaction starts to handle a rollback or commit
signal, it builds a query graph which, when executed, will
roll back or commit the incomplete transaction. The transaction
is moved to the TRX_QUE_ROLLING_BACK or TRX_QUE_COMMITTING state.
If specified, the SQL cursors opened by the transaction are closed.
When the execution of the graph completes, it is like returning
from a subprocedure: the query thread which requested the operation
starts running again. */

void que_var_init() {
#ifdef UNIV_DEBUG
  que_trace_on = FALSE;
#endif /* UNIV_DEBUG */
}

void que_graph_publish(que_t *graph, sess_t *sess) {
  ut_ad(mutex_own(&kernel_mutex));

  UT_LIST_ADD_LAST(graphs, sess->graphs, graph);
}

que_fork_t * que_fork_create(que_t *graph, que_node_t *parent, ulint fork_type, mem_heap_t *heap) {
  ut_ad(heap);

  auto fork = reinterpret_cast<que_fork_t*>(mem_heap_alloc(heap, sizeof(que_fork_t)));

  fork->common.type = QUE_NODE_FORK;
  fork->n_active_thrs = 0;

  fork->state = QUE_FORK_COMMAND_WAIT;

  if (graph != nullptr) {
    fork->graph = graph;
  } else {
    fork->graph = fork;
  }

  fork->common.parent = parent;
  fork->fork_type = fork_type;

  fork->caller = nullptr;

  UT_LIST_INIT(fork->thrs);

  fork->sym_tab = nullptr;
  fork->info = nullptr;

  fork->heap = heap;

  return fork;
}

que_thr_t *que_thr_create(que_fork_t *parent, mem_heap_t *heap) {
  ut_ad(parent && heap);

  auto thr = reinterpret_cast<que_thr_t*>(mem_heap_alloc(heap, sizeof(que_thr_t)));

  thr->common.type = QUE_NODE_THR;
  thr->common.parent = parent;

  thr->magic_n = QUE_THR_MAGIC_N;

  thr->graph = parent->graph;

  thr->state = QUE_THR_COMMAND_WAIT;

  thr->is_active = FALSE;

  thr->run_node = nullptr;
  thr->resource = 0;
  thr->lock_state = QUE_THR_LOCK_NOLOCK;

  UT_LIST_ADD_LAST(thrs, parent->thrs, thr);

  return thr;
}

void que_thr_end_wait(que_thr_t *thr, que_thr_t **next_thr) {
  ibool was_active;

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(thr);
  ut_ad((thr->state == QUE_THR_LOCK_WAIT) ||
        (thr->state == QUE_THR_PROCEDURE_WAIT) ||
        (thr->state == QUE_THR_SIG_REPLY_WAIT));
  ut_ad(thr->run_node);

  thr->prev_node = thr->run_node;

  was_active = thr->is_active;

  que_thr_move_to_run_state(thr);

  if (was_active) {

    return;
  }

  if (next_thr && *next_thr == nullptr) {
    *next_thr = thr;
  } else {
    ut_a(0);
    srv_que_task_enqueue_low(thr);
  }
}

void que_thr_end_wait_no_next_thr(que_thr_t *thr) {
  ibool was_active;

  ut_a(thr->state == QUE_THR_LOCK_WAIT);
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(thr);
  ut_ad((thr->state == QUE_THR_LOCK_WAIT) ||
        (thr->state == QUE_THR_PROCEDURE_WAIT) ||
        (thr->state == QUE_THR_SIG_REPLY_WAIT));

  was_active = thr->is_active;

  que_thr_move_to_run_state(thr);

  if (was_active) {

    return;
  }

  /* We let the OS thread (not just the query thread) to wait
  for the lock to be released: */

  srv_release_user_thread_if_suspended(thr);

  /* srv_que_task_enqueue_low(thr); */
}

/** Inits a query thread for a command.
@param[in,out] thr              Query thread. */
inline
void que_thr_init_command(que_thr_t *thr) {
  thr->run_node = thr;
  thr->prev_node = thr->common.parent;

  que_thr_move_to_run_state(thr);
}

que_thr_t *que_fork_start_command(que_fork_t *fork) {
  que_thr_t *thr;
  que_thr_t *suspended_thr = nullptr;
  que_thr_t *completed_thr = nullptr;

  fork->state = QUE_FORK_ACTIVE;

  fork->last_sel_node = nullptr;

  suspended_thr = nullptr;
  completed_thr = nullptr;

  /* Choose the query thread to run: usually there is just one thread,
  but in a parallelized select, which necessarily is non-scrollable,
  there may be several to choose from */

  /* First we try to find a query thread in the QUE_THR_COMMAND_WAIT
  state. Then we try to find a query thread in the QUE_THR_SUSPENDED
  state, finally we try to find a query thread in the QUE_THR_COMPLETED
  state */

  thr = UT_LIST_GET_FIRST(fork->thrs);

  /* We make a single pass over the thr list within which we note which
  threads are ready to run. */
  while (thr != nullptr) {
    switch (thr->state) {
    case QUE_THR_COMMAND_WAIT:

      /* We have to send the initial message to query thread
      to start it */

      que_thr_init_command(thr);

      return thr;

    case QUE_THR_SUSPENDED:
      /* In this case the execution of the thread was
      suspended: no initial message is needed because
      execution can continue from where it was left */
      if (!suspended_thr) {
        suspended_thr = thr;
      }

      break;

    case QUE_THR_COMPLETED:
      if (!completed_thr) {
        completed_thr = thr;
      }

      break;

    case QUE_THR_LOCK_WAIT:
      ut_error;
    }

    thr = UT_LIST_GET_NEXT(thrs, thr);
  }

  if (suspended_thr) {

    thr = suspended_thr;
    que_thr_move_to_run_state(thr);

  } else if (completed_thr) {

    thr = completed_thr;
    que_thr_init_command(thr);
  }

  return thr;
}

void que_fork_error_handle( trx_t *trx __attribute__((unused)), que_t *fork) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(trx->sess->state == SESS_ERROR);
  ut_ad(UT_LIST_GET_LEN(trx->reply_signals) == 0);
  ut_ad(UT_LIST_GET_LEN(trx->wait_thrs) == 0);

  auto thr = UT_LIST_GET_FIRST(fork->thrs);

  while (thr != nullptr) {
    ut_ad(!thr->is_active);
    ut_ad(thr->state != QUE_THR_SIG_REPLY_WAIT);
    ut_ad(thr->state != QUE_THR_LOCK_WAIT);

    thr->run_node = thr;
    thr->prev_node = thr->child;
    thr->state = QUE_THR_COMPLETED;

    thr = UT_LIST_GET_NEXT(thrs, thr);
  }

  thr = UT_LIST_GET_FIRST(fork->thrs);

  que_thr_move_to_run_state(thr);

  ut_a(0);
  srv_que_task_enqueue_low(thr);
}

/** Tests if all the query threads in the same fork have a given state.
@return TRUE if all the query threads in the same fork were in the
given state.
@param[in,out] fork             Query fork.
@param[in] state                The state to check for. */
inline ibool que_fork_all_thrs_in_state(que_fork_t *fork, ulint state) {
  auto thr_node = UT_LIST_GET_FIRST(fork->thrs);

  while (thr_node != nullptr) {
    if (thr_node->state != state) {

      return FALSE;
    }

    thr_node = UT_LIST_GET_NEXT(thrs, thr_node);
  }

  return TRUE;
}

/** Calls que_graph_free_recursive for statements in a statement list.
@param[in,out] node             First query graph node in the list. */
static void que_graph_free_stat_list(que_node_t *node) {
  while (node) {
    que_graph_free_recursive(node);

    node = que_node_get_next(node);
  }
}

void que_graph_free_recursive(que_node_t *node) {
  que_fork_t *fork;
  que_thr_t *thr;
  undo_node_t *undo;
  sel_node_t *sel;
  ins_node_t *ins;
  upd_node_t *upd;
  tab_node_t *cre_tab;
  ind_node_t *cre_ind;
  purge_node_t *purge;

  if (node == nullptr) {

    return;
  }

  switch (que_node_get_type(node)) {

  case QUE_NODE_FORK:
    fork = static_cast<que_fork_t*>(node);

    thr = UT_LIST_GET_FIRST(fork->thrs);

    while (thr) {
      que_graph_free_recursive(thr);

      thr = UT_LIST_GET_NEXT(thrs, thr);
    }

    break;
  case QUE_NODE_THR:

    thr = static_cast<que_thr_t*>(node);

    if (thr->magic_n != QUE_THR_MAGIC_N) {
      ib_logger(ib_stream,
                "que_thr struct appears corrupt;"
                " magic n %lu\n",
                (unsigned long)thr->magic_n);
      ut_error;
    }

    thr->magic_n = QUE_THR_MAGIC_FREED;

    que_graph_free_recursive(thr->child);

    break;
  case QUE_NODE_UNDO:

    undo = static_cast<undo_node_t*>(node);

    mem_heap_free(undo->heap);

    break;
  case QUE_NODE_SELECT:

    sel = static_cast<sel_node_t*>(node);

    sel_node_free_private(sel);

    break;
  case QUE_NODE_INSERT:

    ins = static_cast<ins_node_t*>(node);

    que_graph_free_recursive(ins->select);

    mem_heap_free(ins->entry_sys_heap);

    break;
  case QUE_NODE_PURGE:
    purge = static_cast<purge_node_t*>(node);

    mem_heap_free(purge->heap);

    break;

  case QUE_NODE_UPDATE:

    upd = static_cast<upd_node_t*>(node);

    if (upd->in_client_interface) {

      btr_pcur_free(upd->pcur);
    }

    que_graph_free_recursive(upd->cascade_node);

    if (upd->cascade_heap) {
      mem_heap_free(upd->cascade_heap);
    }

    que_graph_free_recursive(upd->select);

    mem_heap_free(upd->heap);

    break;
  case QUE_NODE_CREATE_TABLE:
    cre_tab = static_cast<tab_node_t*>(node);

    que_graph_free_recursive(cre_tab->tab_def);
    que_graph_free_recursive(cre_tab->col_def);
    que_graph_free_recursive(cre_tab->commit_node);

    mem_heap_free(cre_tab->heap);

    break;
  case QUE_NODE_CREATE_INDEX:
    cre_ind = static_cast<ind_node_t*>(node);

    que_graph_free_recursive(cre_ind->ind_def);
    que_graph_free_recursive(cre_ind->field_def);
    que_graph_free_recursive(cre_ind->commit_node);

    mem_heap_free(cre_ind->heap);

    break;
  case QUE_NODE_PROC:
    que_graph_free_stat_list(((proc_node_t *)node)->stat_list);

    break;
  case QUE_NODE_IF:
    que_graph_free_stat_list(((if_node_t *)node)->stat_list);
    que_graph_free_stat_list(((if_node_t *)node)->else_part);
    que_graph_free_stat_list(((if_node_t *)node)->elsif_list);

    break;
  case QUE_NODE_ELSIF:
    que_graph_free_stat_list(((elsif_node_t *)node)->stat_list);

    break;
  case QUE_NODE_WHILE:
    que_graph_free_stat_list(((while_node_t *)node)->stat_list);

    break;
  case QUE_NODE_FOR:
    que_graph_free_stat_list(((for_node_t *)node)->stat_list);

    break;

  case QUE_NODE_ASSIGNMENT:
  case QUE_NODE_EXIT:
  case QUE_NODE_RETURN:
  case QUE_NODE_COMMIT:
  case QUE_NODE_ROLLBACK:
  case QUE_NODE_LOCK:
  case QUE_NODE_FUNC:
  case QUE_NODE_ORDER:
  case QUE_NODE_ROW_PRINTF:
  case QUE_NODE_OPEN:
  case QUE_NODE_FETCH:
    /* No need to do anything */

    break;
  default:
    ib_logger(ib_stream, "que_node struct appears corrupt; type %lu\n",
              (unsigned long)que_node_get_type(node));
    ut_error;
  }
}

void que_graph_free(que_t *graph) {
  ut_ad(graph);

  if (graph->sym_tab) {
    /* The following call frees dynamic memory allocated
    for variables etc. during execution. Frees also explicit
    cursor definitions. */

    sym_tab_free_private(graph->sym_tab);
  }

  if (graph->info && graph->info->graph_owns_us) {
    pars_info_free(graph->info);
  }

  que_graph_free_recursive(graph);

  mem_heap_free(graph->heap);
}

/** Performs an execution step on a thr node.
@return	query thread to run next, or nullptr if none
@param[in,out]                  Query thread where run_node must
                                be the thread node itself */
static que_thr_t *
que_thr_node_step(que_thr_t *thr) {
  ut_ad(thr->run_node == thr);

  if (thr->prev_node == thr->common.parent) {
    /* If control to the node came from above, it is just passed on */

    thr->run_node = thr->child;

    return thr;
  }

  mutex_enter(&kernel_mutex);

  if (que_thr_peek_stop(thr)) {

    mutex_exit(&kernel_mutex);

    return thr;
  }

  /* Thread execution completed */

  thr->state = QUE_THR_COMPLETED;

  mutex_exit(&kernel_mutex);

  return nullptr;
}

void que_thr_move_to_run_state(que_thr_t *thr) {
  ut_ad(thr->state != QUE_THR_RUNNING);

  auto trx = thr_get_trx(thr);

  if (!thr->is_active) {

    ++thr->graph->n_active_thrs;

    ++trx->n_active_thrs;

    thr->is_active = TRUE;

    ut_ad((thr->graph)->n_active_thrs == 1);
    ut_ad(trx->n_active_thrs == 1);
  }

  thr->state = QUE_THR_RUNNING;
}

/** Decrements the query thread reference counts in the query graph and the
transaction. May start signal handling, e.g., a rollback.

*** NOTE ***:
This and que_thr_stop_client are the only functions where the reference
count can be decremented and this function may only be called from inside
que_run_threads or que_thr_check_if_switch! These restrictions exist to make
the rollback code easier to maintain. */
static void que_thr_dec_refer_count(
    que_thr_t *thr,       /*!< in: query thread */
    que_thr_t **next_thr) /*!< in/out: next query thread to run;
                          if the value which is passed in is
                          a pointer to a nullptr pointer, then the
                          calling function can start running
                          a new query thread */
{
  auto fork = static_cast<que_fork_t*>(thr->common.parent);
  auto trx = thr_get_trx(thr);

  mutex_enter(&kernel_mutex);

  ut_a(thr->is_active);

  if (thr->state == QUE_THR_RUNNING) {

    auto stopped = que_thr_stop(thr);

    if (!stopped) {
      /* The reason for the thr suspension or wait was
      already canceled before we came here: continue
      running the thread */

      /* ib_logger(ib_stream,
              "!!!!!!!! Wait already ended: continue thr\n");
      */

      if (next_thr && *next_thr == nullptr) {
        /* Normally srv_suspend_user_thread resets
        the state to DB_SUCCESS before waiting, but
        in this case we have to do it here,
        otherwise nobody does it. */
        trx->error_state = DB_SUCCESS;

        *next_thr = thr;
      } else {
        ut_error;
        srv_que_task_enqueue_low(thr);
      }

      mutex_exit(&kernel_mutex);

      return;
    }
  }

  ut_ad(fork->n_active_thrs == 1);
  ut_ad(trx->n_active_thrs == 1);

  fork->n_active_thrs--;
  trx->n_active_thrs--;

  thr->is_active = FALSE;

  if (trx->n_active_thrs > 0) {

    mutex_exit(&kernel_mutex);

    return;
  }

  auto fork_type = fork->fork_type;

  /* Check if all query threads in the same fork are completed */

  if (que_fork_all_thrs_in_state(fork, QUE_THR_COMPLETED)) {

    switch (fork_type) {
    case QUE_FORK_ROLLBACK:
      /* This is really the undo graph used in rollback,
      no roll_node in this graph */

      ut_ad(UT_LIST_GET_LEN(trx->signals) > 0);
      ut_ad(trx->handling_signals == TRUE);

      trx_finish_rollback_off_kernel(fork, trx, next_thr);
      break;

    case QUE_FORK_PURGE:
    case QUE_FORK_RECOVERY:
    case QUE_FORK_USER_INTERFACE:

      /* Do nothing */
      break;

    default:
      ut_error; /* not used */
    }
  }

  if (UT_LIST_GET_LEN(trx->signals) > 0 && trx->n_active_thrs == 0) {

    /* If the trx is signaled and its query thread count drops to
    zero, then we start processing a signal; from it we may get
    a new query thread to run */

    trx_sig_start_handle(trx, next_thr);
  }

  if (trx->handling_signals && UT_LIST_GET_LEN(trx->signals) == 0) {

    trx_end_signal_handling(trx);
  }

  mutex_exit(&kernel_mutex);
}

ibool que_thr_stop(que_thr_t *thr) {
  ibool ret = TRUE;

  ut_ad(mutex_own(&kernel_mutex));

  auto graph = thr->graph;
  auto trx = graph->trx;

  if (graph->state == QUE_FORK_COMMAND_WAIT) {
    thr->state = QUE_THR_SUSPENDED;

  } else if (trx->que_state == TRX_QUE_LOCK_WAIT) {

    UT_LIST_ADD_FIRST(trx_thrs, trx->wait_thrs, thr);
    thr->state = QUE_THR_LOCK_WAIT;

  } else if (trx->error_state != DB_SUCCESS &&
             trx->error_state != DB_LOCK_WAIT) {

    thr->state = QUE_THR_COMPLETED;

  } else if (UT_LIST_GET_LEN(trx->signals) > 0 &&
             graph->fork_type != QUE_FORK_ROLLBACK) {

    thr->state = QUE_THR_SUSPENDED;
  } else {
    ut_ad(graph->state == QUE_FORK_ACTIVE);

    ret = FALSE;
  }

  return ret;
}

void que_thr_stop_for_client_no_error(que_thr_t *thr, trx_t *trx) {
  ut_ad(thr->state == QUE_THR_RUNNING);
  ut_ad(thr->is_active == TRUE);
  ut_ad(trx->n_active_thrs == 1);
  ut_ad(thr->graph->n_active_thrs == 1);

  if (thr->magic_n != QUE_THR_MAGIC_N) {
    ib_logger(ib_stream, "que_thr struct appears corrupt; magic n %lu\n",
              (unsigned long)thr->magic_n);

    ut_error;
  }

  thr->state = QUE_THR_COMPLETED;

  thr->is_active = FALSE;
  (thr->graph)->n_active_thrs--;

  trx->n_active_thrs--;
}

void que_thr_move_to_run_state_for_client(que_thr_t *thr, trx_t *trx) {
  if (thr->magic_n != QUE_THR_MAGIC_N) {
    ib_logger(ib_stream, "que_thr struct appears corrupt; magic n %lu\n",
              (unsigned long)thr->magic_n);

    ut_error;
  } else if (!thr->is_active) {

    thr->graph->n_active_thrs++;

    trx->n_active_thrs++;

    thr->is_active = TRUE;
  }

  thr->state = QUE_THR_RUNNING;
}

void que_thr_stop_client(que_thr_t *thr) {
  auto trx = thr_get_trx(thr);

  mutex_enter(&kernel_mutex);

  if (thr->state == QUE_THR_RUNNING) {

    if (trx->error_state != DB_SUCCESS && trx->error_state != DB_LOCK_WAIT) {

      thr->state = QUE_THR_COMPLETED;
    } else {
      /* It must have been a lock wait but the lock was
      already released, or this transaction was chosen
      as a victim in selective deadlock resolution */

      mutex_exit(&kernel_mutex);

      return;
    }
  }

  ut_ad(thr->is_active == TRUE);
  ut_ad(trx->n_active_thrs == 1);
  ut_ad(thr->graph->n_active_thrs == 1);

  thr->is_active = FALSE;
  (thr->graph)->n_active_thrs--;

  trx->n_active_thrs--;

  mutex_exit(&kernel_mutex);
}

que_node_t *que_node_get_containing_loop_node(que_node_t *node) {
  ut_ad(node);

  for (;;) {
    ulint type;

    node = que_node_get_parent(node);

    if (!node) {
      break;
    }

    type = que_node_get_type(node);

    if ((type == QUE_NODE_FOR) || (type == QUE_NODE_WHILE)) {
      break;
    }
  }

  return node;
}

void que_node_print_info(que_node_t *node) {
  ulint type;
  const char *str;

  type = que_node_get_type(node);

  if (type == QUE_NODE_SELECT) {
    str = "SELECT";
  } else if (type == QUE_NODE_INSERT) {
    str = "INSERT";
  } else if (type == QUE_NODE_UPDATE) {
    str = "UPDATE";
  } else if (type == QUE_NODE_WHILE) {
    str = "WHILE";
  } else if (type == QUE_NODE_ASSIGNMENT) {
    str = "ASSIGNMENT";
  } else if (type == QUE_NODE_IF) {
    str = "IF";
  } else if (type == QUE_NODE_FETCH) {
    str = "FETCH";
  } else if (type == QUE_NODE_OPEN) {
    str = "OPEN";
  } else if (type == QUE_NODE_PROC) {
    str = "STORED PROCEDURE";
  } else if (type == QUE_NODE_FUNC) {
    str = "FUNCTION";
  } else if (type == QUE_NODE_LOCK) {
    str = "LOCK";
  } else if (type == QUE_NODE_THR) {
    str = "QUERY THREAD";
  } else if (type == QUE_NODE_COMMIT) {
    str = "COMMIT";
  } else if (type == QUE_NODE_UNDO) {
    str = "UNDO ROW";
  } else if (type == QUE_NODE_PURGE) {
    str = "PURGE ROW";
  } else if (type == QUE_NODE_ROLLBACK) {
    str = "ROLLBACK";
  } else if (type == QUE_NODE_CREATE_TABLE) {
    str = "CREATE TABLE";
  } else if (type == QUE_NODE_CREATE_INDEX) {
    str = "CREATE INDEX";
  } else if (type == QUE_NODE_FOR) {
    str = "FOR LOOP";
  } else if (type == QUE_NODE_RETURN) {
    str = "RETURN";
  } else if (type == QUE_NODE_EXIT) {
    str = "EXIT";
  } else {
    str = "UNKNOWN NODE TYPE";
  }

  ib_logger(ib_stream, "Node type %lu: %s, address %p\n", (ulong)type, str,
            (void *)node);
}

/** Performs an execution step on a query thread.
@return query thread to run next: it may differ from the input
parameter if, e.g., a subprocedure call is made
@param[in,out] thr              Query thread. */
inline que_thr_t *que_thr_step(que_thr_t *thr) {
  auto trx = thr_get_trx(thr);

  ut_ad(thr->state == QUE_THR_RUNNING);
  ut_a(trx->error_state == DB_SUCCESS);

  thr->resource++;

  auto node = thr->run_node;
  auto type = que_node_get_type(node);

  auto old_thr = thr;

#ifdef UNIV_DEBUG
  if (que_trace_on) {
    ib_logger(ib_stream, "To execute: ");
    que_node_print_info(node);
  }
#endif /* UNIV_DEBUG */
  if (type & QUE_NODE_CONTROL_STAT) {
    if (thr->prev_node != que_node_get_parent(node) &&
        que_node_get_next(thr->prev_node)) {

      /* The control statements, like WHILE, always pass the
      control to the next child statement if there is any
      child left */

      thr->run_node = que_node_get_next(thr->prev_node);

    } else if (type == QUE_NODE_IF) {
      if_step(thr);
    } else if (type == QUE_NODE_FOR) {
      for_step(thr);
    } else if (type == QUE_NODE_PROC) {

      /* We can access trx->undo_no without reserving
      trx->undo_mutex, because there cannot be active query
      threads doing updating or inserting at the moment! */

      if (thr->prev_node == que_node_get_parent(node)) {
        trx->last_sql_stat_start.least_undo_no = trx->undo_no;
      }

      proc_step(thr);
    } else if (type == QUE_NODE_WHILE) {
      while_step(thr);
    } else {
      ut_error;
    }
  } else if (type == QUE_NODE_ASSIGNMENT) {
    assign_step(thr);
  } else if (type == QUE_NODE_SELECT) {
    thr = row_sel_step(thr);
  } else if (type == QUE_NODE_INSERT) {
    thr = row_ins_step(thr);
  } else if (type == QUE_NODE_UPDATE) {
    thr = row_upd_step(thr);
  } else if (type == QUE_NODE_FETCH) {
    thr = fetch_step(thr);
  } else if (type == QUE_NODE_OPEN) {
    thr = open_step(thr);
  } else if (type == QUE_NODE_FUNC) {
    proc_eval_step(thr);

  } else if (type == QUE_NODE_LOCK) {

    ut_error;
    /*
    thr = que_lock_step(thr);
    */
  } else if (type == QUE_NODE_THR) {
    thr = que_thr_node_step(thr);
  } else if (type == QUE_NODE_COMMIT) {
    thr = trx_commit_step(thr);
  } else if (type == QUE_NODE_UNDO) {
    thr = row_undo_step(thr);
  } else if (type == QUE_NODE_PURGE) {
    thr = row_purge_step(thr);
  } else if (type == QUE_NODE_RETURN) {
    thr = return_step(thr);
  } else if (type == QUE_NODE_EXIT) {
    thr = exit_step(thr);
  } else if (type == QUE_NODE_ROLLBACK) {
    thr = trx_rollback_step(thr);
  } else if (type == QUE_NODE_CREATE_TABLE) {
    thr = dict_create_table_step(thr);
  } else if (type == QUE_NODE_CREATE_INDEX) {
    thr = dict_create_index_step(thr);
  } else if (type == QUE_NODE_ROW_PRINTF) {
    thr = row_printf_step(thr);
  } else {
    ut_error;
  }

  if (type == QUE_NODE_EXIT) {
    old_thr->prev_node = que_node_get_containing_loop_node(node);
  } else {
    old_thr->prev_node = node;
  }

  if (thr) {
    ut_a(thr_get_trx(thr)->error_state == DB_SUCCESS);
  }

  return thr;
}

/** Run a query thread until it finishes or encounters e.g. a lock wait.
@param[in,out thr               Query thread. */
static void que_run_threads_low(que_thr_t *thr) {
  que_thr_t *next_thr;

  ut_ad(thr->state == QUE_THR_RUNNING);
  ut_a(thr_get_trx(thr)->error_state == DB_SUCCESS);
  ut_ad(!mutex_own(&kernel_mutex));

loop:
  /* Check that there is enough space in the log to accommodate
  possible log entries by this query step; if the operation can touch
  more than about 4 pages, checks must be made also within the query
  step! */

  log_free_check();

  /* Perform the actual query step: note that the query thread
  may change if, e.g., a subprocedure call is made */

  /*-------------------------*/
  next_thr = que_thr_step(thr);
  /*-------------------------*/

  ut_a(!next_thr || (thr_get_trx(next_thr)->error_state == DB_SUCCESS));

  if (next_thr != thr) {
    ut_a(next_thr == nullptr);

    /* This can change next_thr to a non-nullptr value if there was
    a lock wait that already completed. */
    que_thr_dec_refer_count(thr, &next_thr);

    if (next_thr == nullptr) {

      return;
    }

    thr = next_thr;
  }

  goto loop;
}

void que_run_threads(que_thr_t *thr) {
loop:
  ut_a(thr_get_trx(thr)->error_state == DB_SUCCESS);
  que_run_threads_low(thr);

  mutex_enter(&kernel_mutex);

  switch (thr->state) {

  case QUE_THR_RUNNING:
    /* There probably was a lock wait, but it already ended
    before we came here: continue running thr */

    mutex_exit(&kernel_mutex);

    goto loop;

  case QUE_THR_LOCK_WAIT:
    mutex_exit(&kernel_mutex);

    /* The ..._user_... function works also for InnoDB's
    internal threads. Let us wait that the lock wait ends. */

    srv_suspend_user_thread(thr);

    if (thr_get_trx(thr)->error_state != DB_SUCCESS) {
      /* thr was chosen as a deadlock victim or there was
      a lock wait timeout */

      que_thr_dec_refer_count(thr, nullptr);

      return;
    }

    goto loop;

  case QUE_THR_COMPLETED:
  case QUE_THR_COMMAND_WAIT:
    /* Do nothing */
    break;

  default:
    ut_error;
  }

  mutex_exit(&kernel_mutex);
}

db_err que_eval_sql(pars_info_t *info, const char *sql, ibool reserve_dict_mutex, trx_t *trx) {
  que_thr_t *thr;
  que_t *graph;

  ut_a(trx->error_state == DB_SUCCESS);

  if (reserve_dict_mutex) {
    mutex_enter(&dict_sys->mutex);
  }

  graph = pars_sql(info, sql);

  if (reserve_dict_mutex) {
    mutex_exit(&dict_sys->mutex);
  }

  ut_a(graph);

  graph->trx = trx;
  trx->graph = nullptr;

  graph->fork_type = QUE_FORK_USER_INTERFACE;

  thr = que_fork_start_command(graph);

  ut_a(thr != nullptr);

  que_run_threads(thr);

  que_graph_free(graph);

  return trx->error_state;
}
