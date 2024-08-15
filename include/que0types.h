/*****************************************************************************
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

/**************************************************/ /**
 @file include/que0types.h
 Query graph global types

 Created 5/27/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"

#include "data0types.h"
#include "ut0lst.h"

/* Query fork (or graph) types */
constexpr ulint QUE_FORK_SELECT_NON_SCROLL = 1 /* forward-only cursor */;
constexpr ulint QUE_FORK_SELECT_SCROLL = 2     /* scrollable cursor */;
constexpr ulint QUE_FORK_INSERT = 3;
constexpr ulint QUE_FORK_UPDATE = 4;
constexpr ulint QUE_FORK_ROLLBACK = 5;

/* This is really the undo graph used in rollback,
no signal-sending roll_node in this graph */
constexpr ulint QUE_FORK_PURGE = 6;
constexpr ulint QUE_FORK_EXECUTE = 7;
constexpr ulint QUE_FORK_PROCEDURE = 8;
constexpr ulint QUE_FORK_PROCEDURE_CALL = 9;
constexpr ulint QUE_FORK_USER_INTERFACE = 10;
constexpr ulint QUE_FORK_RECOVERY = 11;

/* Query fork (or graph) states */
constexpr ulint QUE_FORK_ACTIVE = 1;
constexpr ulint QUE_FORK_COMMAND_WAIT = 2;
constexpr ulint QUE_FORK_INVALID = 3;
constexpr ulint QUE_FORK_BEING_FREED = 4;

/* Flag which is ORed to control structure statement node types */
constexpr ulint QUE_NODE_CONTROL_STAT = 1024;

/* Query graph node types */
constexpr ulint QUE_NODE_LOCK = 1;
constexpr ulint QUE_NODE_INSERT = 2;
constexpr ulint QUE_NODE_UPDATE = 4;
constexpr ulint QUE_NODE_CURSOR = 5;
constexpr ulint QUE_NODE_SELECT = 6;
constexpr ulint QUE_NODE_AGGREGATE = 7;
constexpr ulint QUE_NODE_FORK = 8;
constexpr ulint QUE_NODE_THR = 9;
constexpr ulint QUE_NODE_UNDO = 10;
constexpr ulint QUE_NODE_COMMIT = 11;
constexpr ulint QUE_NODE_ROLLBACK = 12;
constexpr ulint QUE_NODE_PURGE = 13;
constexpr ulint QUE_NODE_CREATE_TABLE = 14;
constexpr ulint QUE_NODE_CREATE_INDEX = 15;
constexpr ulint QUE_NODE_SYMBOL = 16;
constexpr ulint QUE_NODE_RES_WORD = 17;
constexpr ulint QUE_NODE_FUNC = 18;
constexpr ulint QUE_NODE_ORDER = 19;
constexpr ulint QUE_NODE_PROC = (20 + QUE_NODE_CONTROL_STAT);
constexpr ulint QUE_NODE_IF = (21 + QUE_NODE_CONTROL_STAT);
constexpr ulint QUE_NODE_WHILE = (22 + QUE_NODE_CONTROL_STAT);
constexpr ulint QUE_NODE_ASSIGNMENT = 23;
constexpr ulint QUE_NODE_FETCH = 24;
constexpr ulint QUE_NODE_OPEN = 25;
constexpr ulint QUE_NODE_COL_ASSIGNMENT = 26;
constexpr ulint QUE_NODE_FOR = (27 + QUE_NODE_CONTROL_STAT);
constexpr ulint QUE_NODE_RETURN = 28;
constexpr ulint QUE_NODE_ROW_PRINTF = 29;
constexpr ulint QUE_NODE_ELSIF = 30;
constexpr ulint QUE_NODE_CALL = 31;
constexpr ulint QUE_NODE_EXIT = 32;

/* Query thread states */
constexpr ulint QUE_THR_RUNNING = 1;
constexpr ulint QUE_THR_PROCEDURE_WAIT = 2;

/** In selects this means that the thread is at the end of its result set 
or start, in case of a scroll cursor); in other statements, this means
the thread has done its task */
constexpr ulint QUE_THR_COMPLETED = 3;

constexpr ulint QUE_THR_COMMAND_WAIT = 4;
constexpr ulint QUE_THR_LOCK_WAIT = 5;
constexpr ulint QUE_THR_SIG_REPLY_WAIT = 6;
constexpr ulint QUE_THR_SUSPENDED = 7;
constexpr ulint QUE_THR_ERROR = 8;

/* Query thread lock states */
constexpr ulint QUE_THR_LOCK_NOLOCK = 0;
constexpr ulint QUE_THR_LOCK_ROW = 1;
constexpr ulint QUE_THR_LOCK_TABLE = 2;

/* From where the cursor position is counted */
constexpr ulint QUE_CUR_NOT_DEFINED = 1;
constexpr ulint QUE_CUR_START = 2;
constexpr ulint QUE_CUR_END = 3;
constexpr ulint QUE_THR_MAGIC_N = 8476583;
constexpr ulint QUE_THR_MAGIC_FREED = 123461526;

struct trx_t;
struct sym_tab_t;
struct pars_info_t;
struct sel_node_t;

/* Pseudotype for all graph nodes */
using que_node_t = void;

struct que_fork_t;

/** Query graph root is a fork node */
using que_t = que_fork_t;

/** Common struct at the beginning of each query graph node; the name of this
substruct must be 'common' */
struct que_common_t {
  /** Query node type */
  ulint type;

  /** Back pointer to parent node, or nullptr */
  que_node_t *parent;

  /** Pointer to a possible brother node */
  que_node_t *brother;

  /** Evaluated value for an expression */
  dfield_t val;

  /** Buffer size for the evaluated value data, if the buffer has been
  allocated dynamically: if this field is != 0, and the node is a symbol
  node or a function node, then we have to free the data field in val explicitly */
  ulint val_buf_size;
};

/* Query graph query thread node: the fields are protected by the kernel
mutex with the exceptions named below */
struct que_thr_t {
  /**type: QUE_NODE_THR */
  que_common_t common;

  /** Magic number to catch memory corruption */
  ulint magic_n;

  /** Graph child node */
  que_node_t *child;

  /** Graph where this node belongs */
  que_t *graph;

  /** true if the thread has been set to the run state in
  que_thr_move_to_run_state, but not deactivated in
  que_thr_dec_reference_count */
  bool is_active;

  /** State of the query thread */
  ulint state;

  /** List of thread nodes of the fork node */
  UT_LIST_NODE_T(que_thr_t) thrs;

  /** Lists of threads in wait list of the trx */
  UT_LIST_NODE_T(que_thr_t) trx_thrs;

  /** List of runnable thread nodes in the server task queue */
  UT_LIST_NODE_T(que_thr_t) queue;

  /* The following fields are private to the OS thread executing the
  query thread, and are not protected by the kernel mutex: */

  /** Pointer to the node where the subgraph down from this
  node is currently executed */
  que_node_t *run_node;

  /** Pointer to the node from which the control came */
  que_node_t *prev_node;

  /** Resource usage of the query thread thus far */
  ulint resource;

  /** Lock state of thread (table or row) */
  ulint lock_state;
};

UT_LIST_NODE_GETTER_DEFINITION(que_thr_t, thrs);
UT_LIST_NODE_GETTER_DEFINITION(que_thr_t, trx_thrs);
UT_LIST_NODE_GETTER_DEFINITION(que_thr_t, queue);

/* Query graph fork node: its fields are protected by the kernel mutex */
struct que_fork_t {
  /** type: QUE_NODE_FORK */
  que_common_t common;

  /** Query graph of this node */
  que_t *graph;

  /** Fork type */
  ulint fork_type;

  /** If this is the root of a graph, the number query threads that have
  been started in que_thr_move_to_run_state but for which que_thr_dec_refer_count
  has not yet been called */
  ulint n_active_thrs;

  /** transaction: this is set only in the root node */
  trx_t *trx;

  /** State of the fork node */
  ulint state;

  /** Pointer to a possible calling query thread */
  que_thr_t *caller;

  /** List of query threads */
  UT_LIST_BASE_NODE_T(que_thr_t, thrs) thrs;

  /* The fields in this section are defined only in the root node */

  /** Symbol table of the query, generated by the parser, or nullptr
  if the graph was created 'by hand' */
  sym_tab_t *sym_tab;

  /* info struct, or nullptr */
  pars_info_t *info;

  /* The following cur_... fields are relevant only in a select graph */

  /** QUE_CUR_NOT_DEFINED, QUE_CUR_START, QUE_CUR_END */
  ulint cur_end;

  /** If there are n rows in the result set, values 0 and n + 1 mean
  before first row, or after last row, depending on cur_end;
  values 1...n mean a row index */
  ulint cur_pos;

  /** true if cursor is on a row, i.e., it is not before the first row or after the last row */
  bool cur_on_row;

  /** Number of rows inserted */
  uint64_t n_inserts;

  /** Number of rows updated */
  uint64_t n_updates;

  /** Number of rows deleted */
  uint64_t n_deletes;

  /** Last executed select node, or NULL if none */
  sel_node_t *last_sel_node;

  /** List of query graphs of a session or a stored procedure */
  UT_LIST_NODE_T(que_fork_t) graphs;

  /** Memory heap where the fork was created */
  mem_heap_t *heap;
};

UT_LIST_NODE_GETTER_DEFINITION(que_t, graphs);

