/****************************************************************************
Copyright (c) 1998, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/eval0proc.h
Executes SQL stored procedures and their control structures

Created 1/20/1998 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "pars0pars.h"
#include "pars0sym.h"
#include "que0types.h"
#include "eval0eval.h"
#include "que0que.h"

/** Performs an execution step of an if-statement node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *if_step(que_thr_t *thr);

/** Performs an execution step of a while-statement node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *while_step(que_thr_t *thr);

/** Performs an execution step of a for-loop node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *for_step(que_thr_t *thr);

/** Performs an execution step of an assignment statement node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *assign_step(que_thr_t *thr);

/** Performs an execution step of an exit statement node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *exit_step(que_thr_t *thr);

/** Performs an execution step of a return-statement node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
que_thr_t *return_step(que_thr_t *thr);

/** Performs an execution step of a procedure node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
inline que_thr_t *proc_step(que_thr_t *thr) {
  auto node = reinterpret_cast<proc_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_PROC);

  if (thr->prev_node == que_node_get_parent(node)) {
    /* Start execution from the first statement in the statement list */

    thr->run_node = node->stat_list;
  } else {
    /* Move to the next statement */
    ut_ad(que_node_get_next(thr->prev_node) == nullptr);

    thr->run_node = nullptr;
  }

  if (thr->run_node == nullptr) {
    thr->run_node = que_node_get_parent(node);
  }

  return thr;
}

/** Performs an execution step of a procedure call node.
@param[in,out] thr              Query thread.
@return        query thread to run next or nullptr */
inline que_thr_t *proc_eval_step(que_thr_t *thr) /*!< in: query thread */
{
  auto node = reinterpret_cast<func_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_FUNC);

  /* Evaluate the procedure */

  eval_exp(node);

  thr->run_node = que_node_get_parent(node);

  return thr;
}
