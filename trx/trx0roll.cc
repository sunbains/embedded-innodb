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

/** @file trx/trx0roll.c
Transaction rollback

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0roll.h"

#include "ddl0ddl.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "mach0data.h"
#include "os0proc.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0undo.h"
#include "trx0rec.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "usr0sess.h"

/** This many pages must be undone before a truncate is tried within
rollback */
static constexpr ulint TRX_ROLL_TRUNC_THRESHOLD = 1;

/** In crash recovery, the current trx to be rolled back */
static trx_t *trx_roll_crash_recv_trx = nullptr;

/** In crash recovery we set this to the undo n:o of the current trx to be
rolled back. Then we can print how many % the rollback has progressed. */
static int64_t trx_roll_max_undo_no;

/** Auxiliary variable which tells the previous progress % we printed */
static ulint trx_roll_progress_printed_pct;

db_err trx_general_rollback(trx_t *trx, bool partial, trx_savept_t *savept) {
  mem_heap_t *heap;
  que_thr_t *thr;
  roll_node_t *roll_node;

  /* Tell Innobase server that there might be work for
  utility threads: */

  InnoDB::active_wake_master_thread();

  heap = mem_heap_create(512);

  roll_node = roll_node_create(heap);

  roll_node->partial = partial;

  if (partial) {
    roll_node->savept = *savept;
  }

  trx->error_state = DB_SUCCESS;

  thr = pars_complete_graph_for_exec(roll_node, trx, heap);

  ut_a(thr == que_fork_start_command(static_cast<que_fork_t *>(que_node_get_parent(thr))));

  que_run_threads(thr);

  mutex_enter(&kernel_mutex);

  while (trx->m_que_state != TRX_QUE_RUNNING) {

    mutex_exit(&kernel_mutex);

    os_thread_sleep(100000);

    mutex_enter(&kernel_mutex);
  }

  mutex_exit(&kernel_mutex);

  mem_heap_free(heap);

  ut_a(trx->error_state == DB_SUCCESS);

  /* Tell Innobase server that there might be work for
  utility threads: */

  InnoDB::active_wake_master_thread();

  return trx->error_state;
}

/** Frees savepoint structs starting from savep, if savep == nullptr then
free all savepoints. */

void trx_roll_savepoints_free(
  trx_t *trx, /*!< in: transaction handle */
  trx_named_savept_t *savep
) /*!< in: free all savepoints > this one;
                               if this is nullptr, free all savepoints
                               of trx */
{
  trx_named_savept_t *next_savep;

  if (savep == nullptr) {
    savep = UT_LIST_GET_FIRST(trx->trx_savepoints);
  } else {
    savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
  }

  while (savep != nullptr) {
    next_savep = UT_LIST_GET_NEXT(trx_savepoints, savep);

    UT_LIST_REMOVE(trx->trx_savepoints, savep);
    mem_free(savep);

    savep = next_savep;
  }
}

/** Determines if this transaction is rolling back an incomplete transaction
in crash recovery.
@return true if trx is an incomplete transaction that is being rolled
back in crash recovery */

bool trx_is_recv(const trx_t *trx) /*!< in: transaction */
{
  return trx == trx_roll_crash_recv_trx;
}

/** Returns a transaction savepoint taken at this point in time.
@return	savepoint */

trx_savept_t trx_savept_take(trx_t *trx) /*!< in: transaction */
{
  trx_savept_t savept;

  savept.least_undo_no = trx->undo_no;

  return savept;
}

/** Roll back an active transaction. */
static void trx_rollback_active(
  ib_recovery_t recovery, /*!< in: recovery flag */
  trx_t *trx
) /*!< in/out: transaction */
{
  mem_heap_t *heap;
  que_fork_t *fork;
  que_thr_t *thr;
  roll_node_t *roll_node;
  dict_table_t *table;
  int64_t rows_to_undo;
  const char *unit = "";
  bool dictionary_locked = false;

  heap = mem_heap_create(512);

  fork = que_fork_create(nullptr, nullptr, QUE_FORK_RECOVERY, heap);
  fork->trx = trx;

  thr = que_thr_create(fork, heap);

  roll_node = roll_node_create(heap);

  thr->child = roll_node;
  roll_node->common.parent = thr;

  mutex_enter(&kernel_mutex);

  trx->graph = fork;

  ut_a(thr == que_fork_start_command(fork));

  trx_roll_crash_recv_trx = trx;
  trx_roll_max_undo_no = trx->undo_no;
  trx_roll_progress_printed_pct = 0;
  rows_to_undo = trx_roll_max_undo_no;

  if (rows_to_undo > 1000000000) {
    rows_to_undo = rows_to_undo / 1000000;
    unit = "M";
  }

  log_info(std::format(
    "Rolling back trx with id {}, {} rows to undo",
    TRX_ID_PREP_PRINTF(trx->m_id),
    rows_to_undo,
    unit
  ));

  mutex_exit(&kernel_mutex);

  if (trx_get_dict_operation(trx) != TRX_DICT_OP_NONE) {
    dict_lock_data_dictionary(trx);
    dictionary_locked = true;
  }

  que_run_threads(thr);

  mutex_enter(&kernel_mutex);

  while (trx->m_que_state != TRX_QUE_RUNNING) {

    mutex_exit(&kernel_mutex);

    log_info(std::format("Waiting for rollback of trx id {} to end", trx->m_id));
    os_thread_sleep(100000);

    mutex_enter(&kernel_mutex);
  }

  mutex_exit(&kernel_mutex);

  if (trx_get_dict_operation(trx) != TRX_DICT_OP_NONE && trx->table_id != 0) {

    /* If the transaction was for a dictionary operation, we
    drop the relevant table, if it still exists */

    log_info(std::format("Dropping table with id {} {} in recovery if it exists", trx->table_id, trx->table_id));

    table = dict_table_get_on_id_low(recovery, trx->table_id);

    if (table != nullptr) {
      log_info(std::format("Table found: dropping table {} in recovery", table->name));

      auto err = ddl_drop_table(table->name, trx, true);
      auto err_commit = trx_commit(trx);
      ut_a(err_commit == DB_SUCCESS);

      ut_a(err == (int)DB_SUCCESS);
    }
  }

  if (dictionary_locked) {
    dict_unlock_data_dictionary(trx);
  }

  log_info(std::format("Rolling back of trx id {} completed", TRX_ID_PREP_PRINTF(trx->m_id)));

  mem_heap_free(heap);

  trx_roll_crash_recv_trx = nullptr;
}

/** Rollback or clean up any incomplete transactions which were
encountered in crash recovery.  If the transaction already was
committed, then we clean up a possible insert undo log. If the
transaction was not yet committed, then we roll it back. */

void trx_rollback_or_clean_recovered(bool all) /*!< in: false=roll back dictionary transactions;
               true=roll back all non-PREPARED transactions */
{
  trx_t *trx;

  mutex_enter(&kernel_mutex);

  if (!UT_LIST_GET_FIRST(trx_sys->trx_list)) {
    goto leave_function;
  }

  if (all) {
    log_info("Starting in background the rollback of uncommitted transactions");
  }

  mutex_exit(&kernel_mutex);

loop:
  mutex_enter(&kernel_mutex);

  for (trx = UT_LIST_GET_FIRST(trx_sys->trx_list); trx; trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    if (!trx->m_is_recovered) {
      continue;
    }

    switch (trx->m_conc_state) {
      case TRX_NOT_STARTED:
      case TRX_PREPARED:
        continue;

      case TRX_COMMITTED_IN_MEMORY:
        mutex_exit(&kernel_mutex);
        log_info("Cleaning up trx with id ", TRX_ID_PREP_PRINTF(trx->m_id));
        trx_cleanup_at_db_startup(trx);
        goto loop;

      case TRX_ACTIVE:
        if (all || trx_get_dict_operation(trx) != TRX_DICT_OP_NONE) {
          mutex_exit(&kernel_mutex);
          // FIXME: Need to get rid of this global access
          trx_rollback_active(srv_force_recovery, trx);
          goto loop;
        }
    }
  }

  if (all) {
    log_info("Rollback of non-prepared transactions completed");
  }

leave_function:
  mutex_exit(&kernel_mutex);
}

void *trx_rollback_or_clean_all_recovered(void *) {
  trx_rollback_or_clean_recovered(true);

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit. */

  os_thread_exit();

  return nullptr;
}

trx_undo_arr_t *trx_undo_arr_create() {
  auto heap = mem_heap_create(1024);
  auto arr = reinterpret_cast<trx_undo_arr_t *>(mem_heap_alloc(heap, sizeof(trx_undo_arr_t)));

  arr->infos = reinterpret_cast<trx_undo_inf_t *>(mem_heap_alloc(heap, sizeof(trx_undo_inf_t) * UNIV_MAX_PARALLELISM));

  arr->n_cells = UNIV_MAX_PARALLELISM;
  arr->n_used = 0;

  arr->heap = heap;

  for (ulint i = 0; i < UNIV_MAX_PARALLELISM; i++) {
    trx_undo_arr_get_nth_info(arr, i)->in_use = false;
  }

  return arr;
}

void trx_undo_arr_free(trx_undo_arr_t *arr) {
  ut_ad(arr->n_used == 0);

  mem_heap_free(arr->heap);
}

/** Stores info of an undo log record to the array if it is not stored yet.
@return	false if the record already existed in the array */
static bool trx_undo_arr_store_info(
  trx_t *trx, /*!< in: transaction */
  undo_no_t undo_no
) /*!< in: undo number */
{
  trx_undo_inf_t *cell;
  trx_undo_inf_t *stored_here;
  trx_undo_arr_t *arr;
  ulint n_used;
  ulint n;
  ulint i;

  n = 0;
  arr = trx->undo_no_arr;
  n_used = arr->n_used;
  stored_here = nullptr;

  for (i = 0;; i++) {
    cell = trx_undo_arr_get_nth_info(arr, i);

    if (!cell->in_use) {
      if (!stored_here) {
        /* Not in use, we may store here */
        cell->undo_no = undo_no;
        cell->in_use = true;

        arr->n_used++;

        stored_here = cell;
      }
    } else {
      n++;

      if (cell->undo_no == undo_no) {

        if (stored_here) {
          stored_here->in_use = false;
          ut_ad(arr->n_used > 0);
          arr->n_used--;
        }

        ut_ad(arr->n_used == n_used);

        return false;
      }
    }

    if (n == n_used && stored_here) {

      ut_ad(arr->n_used == 1 + n_used);

      return true;
    }
  }
}

/** Removes an undo number from the array. */
static void trx_undo_arr_remove_info(
  trx_undo_arr_t *arr, /*!< in: undo number array */
  undo_no_t undo_no
) /*!< in: undo number */
{
  for (ulint i = 0;; i++) {
    auto cell = trx_undo_arr_get_nth_info(arr, i);

    if (cell->in_use && cell->undo_no == undo_no) {

      cell->in_use = false;

      ut_a(arr->n_used > 0);

      arr->n_used--;

      return;
    }
  }
}

/** Gets the biggest undo number in an array.
@return	biggest value, 0 if the array is empty */
static undo_no_t trx_undo_arr_get_biggest(trx_undo_arr_t *arr) /*!< in: undo number array */
{
  trx_undo_inf_t *cell;
  ulint n_used;
  undo_no_t biggest;
  ulint n;
  ulint i;

  n = 0;
  n_used = arr->n_used;
  biggest = 0;

  for (i = 0;; i++) {
    cell = trx_undo_arr_get_nth_info(arr, i);

    if (cell->in_use) {
      n++;
      if (cell->undo_no > biggest) {

        biggest = cell->undo_no;
      }
    }

    if (n == n_used) {
      return biggest;
    }
  }
}

void trx_roll_try_truncate(trx_t *trx) {
  trx_undo_arr_t *arr;
  undo_no_t limit;

  ut_ad(mutex_own(&(trx->undo_mutex)));
  ut_ad(mutex_own(&((trx->rseg)->mutex)));

  trx->pages_undone = 0;

  arr = trx->undo_no_arr;

  limit = trx->undo_no;

  if (arr->n_used > 0) {
    auto biggest = trx_undo_arr_get_biggest(arr);

    if (biggest >= limit) {

      limit = biggest + 1;
    }
  }

  if (trx->insert_undo) {
    srv_undo->truncate_end(trx, trx->insert_undo, limit);
  }

  if (trx->update_undo) {
    srv_undo->truncate_end(trx, trx->update_undo, limit);
  }
}

/** Pops the topmost undo log record in a single undo log and updates the info
about the topmost record in the undo log memory struct.
@return	undo log record, the page s-latched */
static trx_undo_rec_t *trx_roll_pop_top_rec(
  trx_t *trx,       /*!< in: transaction */
  trx_undo_t *undo, /*!< in: undo log */
  mtr_t *mtr
) /*!< in: mtr */
{
  ut_ad(mutex_own(&trx->undo_mutex));

  auto undo_page = srv_undo->page_get_s_latched(undo->m_space, undo->m_top_page_no, mtr);
  auto offset = undo->m_top_offset;
  auto prev_rec = srv_undo->get_prev_rec(undo_page + offset, undo->m_hdr_page_no, undo->m_hdr_offset, mtr);

  if (prev_rec == nullptr) {

    undo->m_empty = true;
  } else {
    auto prev_rec_page = page_align(prev_rec);

    if (prev_rec_page != undo_page) {

      ++trx->pages_undone;
    }

    undo->m_top_page_no = page_get_page_no(prev_rec_page);
    undo->m_top_offset = prev_rec - prev_rec_page;
    undo->m_top_undo_no = trx_undo_rec_get_undo_no(prev_rec);
  }

  return undo_page + offset;
}

trx_undo_rec_t *trx_roll_pop_top_rec_of_trx(trx_t *trx, undo_no_t limit, roll_ptr_t *roll_ptr, mem_heap_t *heap) {
  trx_undo_t *undo;
  trx_undo_t *ins_undo;
  trx_undo_t *upd_undo;
  trx_undo_rec_t *undo_rec;
  trx_undo_rec_t *undo_rec_copy;
  undo_no_t undo_no;
  bool is_insert;
  ulint progress_pct;
  mtr_t mtr;

  auto rseg = trx->rseg;

try_again:
  mutex_enter(&trx->undo_mutex);

  if (trx->pages_undone >= TRX_ROLL_TRUNC_THRESHOLD) {
    mutex_enter(&rseg->mutex);

    trx_roll_try_truncate(trx);

    mutex_exit(&rseg->mutex);
  }

  ins_undo = trx->insert_undo;
  upd_undo = trx->update_undo;

  if (!ins_undo || ins_undo->m_empty) {
    undo = upd_undo;
  } else if (!upd_undo || upd_undo->m_empty) {
    undo = ins_undo;
  } else if (upd_undo->m_top_undo_no > ins_undo->m_top_undo_no) {
    undo = upd_undo;
  } else {
    undo = ins_undo;
  }

  if (undo == nullptr || undo->m_empty || limit > undo->m_top_undo_no) {

    if ((trx->undo_no_arr)->n_used == 0) {
      /* Rollback is ending */

      mutex_enter(&rseg->mutex);

      trx_roll_try_truncate(trx);

      mutex_exit(&rseg->mutex);
    }

    mutex_exit(&trx->undo_mutex);

    return nullptr;
  }

  if (undo == ins_undo) {
    is_insert = true;
  } else {
    is_insert = false;
  }

  *roll_ptr = trx_undo_build_roll_ptr(is_insert, undo->m_rseg->id, undo->m_top_page_no, undo->m_top_offset);

  mtr.start();

  undo_rec = trx_roll_pop_top_rec(trx, undo, &mtr);

  undo_no = trx_undo_rec_get_undo_no(undo_rec);

  ut_ad(undo_no + 1 == trx->undo_no);

  /* We print rollback progress info if we are in a crash recovery
  and the transaction has at least 1000 row operations to undo. */

  if (trx == trx_roll_crash_recv_trx && trx_roll_max_undo_no > 1000) {

    progress_pct = 100 - (ulint)((undo_no * 100) / trx_roll_max_undo_no);

    if (progress_pct != trx_roll_progress_printed_pct) {
      if (trx_roll_progress_printed_pct == 0) {
        ib_logger(
          ib_stream,
          "\nProgress in percents:"
          " %lu",
          (ulong)progress_pct
        );
      } else {
        ib_logger(ib_stream, " %lu", (ulong)progress_pct);
      }
      trx_roll_progress_printed_pct = progress_pct;
    }
  }

  trx->undo_no = undo_no;

  if (!trx_undo_arr_store_info(trx, undo_no)) {
    /* A query thread is already processing this undo log record */

    mutex_exit(&(trx->undo_mutex));

    mtr.commit();

    goto try_again;
  }

  undo_rec_copy = trx_undo_rec_copy(undo_rec, heap);

  mutex_exit(&(trx->undo_mutex));

  mtr.commit();

  return undo_rec_copy;
}

bool trx_undo_rec_reserve(trx_t *trx, undo_no_t undo_no) {
  bool ret;

  mutex_enter(&(trx->undo_mutex));

  ret = trx_undo_arr_store_info(trx, undo_no);

  mutex_exit(&(trx->undo_mutex));

  return ret;
}

void trx_undo_rec_release(trx_t *trx, undo_no_t undo_no) {
  trx_undo_arr_t *arr;

  mutex_enter(&(trx->undo_mutex));

  arr = trx->undo_no_arr;

  trx_undo_arr_remove_info(arr, undo_no);

  mutex_exit(&(trx->undo_mutex));
}

void trx_rollback(trx_t *trx, trx_sig_t *sig, que_thr_t **next_thr) {
  /*	que_thr_t*	thr2; */

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((trx->undo_no_arr == nullptr) || ((trx->undo_no_arr)->n_used == 0));

  /* Initialize the rollback field in the transaction */

  if (sig->type == TRX_SIG_TOTAL_ROLLBACK) {

    trx->roll_limit = 0;

  } else if (sig->type == TRX_SIG_ROLLBACK_TO_SAVEPT) {

    trx->roll_limit = (sig->savept).least_undo_no;

  } else if (sig->type == TRX_SIG_ERROR_OCCURRED) {

    trx->roll_limit = trx->last_sql_stat_start.least_undo_no;
  } else {
    ut_error;
  }

  ut_a(trx->roll_limit <= trx->undo_no);

  trx->pages_undone = 0;

  if (trx->undo_no_arr == nullptr) {
    trx->undo_no_arr = trx_undo_arr_create();
  }

  /* Build a 'query' graph which will perform the undo operations */

  auto roll_graph = trx_roll_graph_build(trx);

  trx->graph = roll_graph;
  trx->m_que_state = TRX_QUE_ROLLING_BACK;

  auto thr = que_fork_start_command(roll_graph);

  ut_ad(thr);

  /*	thr2 = que_fork_start_command(roll_graph);

  ut_ad(thr2); */

  if (next_thr && (*next_thr == nullptr)) {
    *next_thr = thr;
    /*		InnoDB::que_task_enqueue_low(thr2); */
  } else {
	  InnoDB::que_task_enqueue_low(thr);
    /*		InnoDB::que_task_enqueue_low(thr2); */
  }
}

que_t *trx_roll_graph_build(trx_t *trx) {
  /*	que_thr_t*	thr2; */

  ut_ad(mutex_own(&kernel_mutex));

  auto heap = mem_heap_create(512);
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_ROLLBACK, heap);

  fork->trx = trx;

  auto thr = que_thr_create(fork, heap);
  /*	thr2 = que_thr_create(fork, heap); */

  thr->child = row_undo_node_create(trx, thr, heap);
  /*	thr2->child = row_undo_node_create(trx, thr2, heap); */

  return fork;
}

/** Finishes error processing after the necessary partial rollback has been
done. */
static void trx_finish_error_processing(trx_t *trx) /*!< in: transaction */
{
  ut_ad(mutex_own(&kernel_mutex));

  auto sig = UT_LIST_GET_FIRST(trx->signals);

  while (sig != nullptr) {
    auto next_sig = UT_LIST_GET_NEXT(signals, sig);

    if (sig->type == TRX_SIG_ERROR_OCCURRED) {

      trx_sig_remove(trx, sig);
    }

    sig = next_sig;
  }

  trx->m_que_state = TRX_QUE_RUNNING;
}

/** Finishes a partial rollback operation. */
static void trx_finish_partial_rollback_off_kernel(
  trx_t *trx, /*!< in: transaction */
  que_thr_t **next_thr
) /*!< in/out: next query thread to run;
                         if the value which is passed in is a pointer
                         to a nullptr pointer, then the calling function
                         can start running a new query thread; if this
                         parameter is nullptr, it is ignored */
{
  ut_ad(mutex_own(&kernel_mutex));

  auto sig = UT_LIST_GET_FIRST(trx->signals);

  /* Remove the signal from the signal queue and send reply message to it */

  trx_sig_reply(sig, next_thr);
  trx_sig_remove(trx, sig);

  trx->m_que_state = TRX_QUE_RUNNING;
}

void trx_finish_rollback_off_kernel(que_t *graph, trx_t *trx, que_thr_t **next_thr) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(trx->undo_no_arr == nullptr || trx->undo_no_arr->n_used == 0);

  /* Free the memory reserved by the undo graph */
  que_graph_free(graph);

  auto sig = UT_LIST_GET_FIRST(trx->signals);

  if (sig->type == TRX_SIG_ROLLBACK_TO_SAVEPT) {

    trx_finish_partial_rollback_off_kernel(trx, next_thr);

    return;

  } else if (sig->type == TRX_SIG_ERROR_OCCURRED) {

    trx_finish_error_processing(trx);

    return;
  }

#ifdef UNIV_DEBUG
  if (lock_print_waits) {
    ib_logger(ib_stream, "Trx %lu rollback finished\n", (ulong)trx->m_id);
  }
#endif /* UNIV_DEBUG */

  trx_commit_off_kernel(trx);

  /* Remove all TRX_SIG_TOTAL_ROLLBACK signals from the signal queue and
  send reply messages to them */

  trx->m_que_state = TRX_QUE_RUNNING;

  while (sig != nullptr) {
    auto next_sig = UT_LIST_GET_NEXT(signals, sig);

    if (sig->type == TRX_SIG_TOTAL_ROLLBACK) {

      trx_sig_reply(sig, next_thr);

      trx_sig_remove(trx, sig);
    }

    sig = next_sig;
  }
}

roll_node_t *roll_node_create(mem_heap_t *heap) {
  auto node = reinterpret_cast<roll_node_t *>(mem_heap_alloc(heap, sizeof(roll_node_t)));

  node->common.type = QUE_NODE_ROLLBACK;
  node->state = ROLL_NODE_SEND;

  node->partial = false;

  return node;
}

que_thr_t *trx_rollback_step(que_thr_t *thr) {
  trx_savept_t *savept;

  auto node = static_cast<roll_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_ROLLBACK);

  if (thr->prev_node == que_node_get_parent(node)) {
    node->state = ROLL_NODE_SEND;
  }

  if (node->state == ROLL_NODE_SEND) {
    ulint sig_no;

    mutex_enter(&kernel_mutex);

    node->state = ROLL_NODE_WAIT;

    if (node->partial) {
      sig_no = TRX_SIG_ROLLBACK_TO_SAVEPT;
      savept = &(node->savept);
    } else {
      sig_no = TRX_SIG_TOTAL_ROLLBACK;
      savept = nullptr;
    }

    /* Send a rollback signal to the transaction */

    trx_sig_send(thr_get_trx(thr), sig_no, TRX_SIG_SELF, thr, savept, nullptr);

    thr->state = QUE_THR_SIG_REPLY_WAIT;

    mutex_exit(&kernel_mutex);

    return nullptr;
  }

  ut_ad(node->state == ROLL_NODE_WAIT);

  thr->run_node = que_node_get_parent(node);

  return thr;
}
