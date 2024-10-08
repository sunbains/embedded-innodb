/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/row0sel.h
Select

Created 12/19/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "api0api.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "pars0sym.h"
#include "que0que.h"
#include "read0read.h"
#include "row0types.h"
#include "trx0types.h"

/**
 * @brief Creates a select node struct.
 *
 * This function allocates and initializes a select node struct using the provided memory heap.
 *
 * @param[in] heap The memory heap where the select node struct will be created.
 * @return A pointer to the newly created select node struct.
 */
sel_node_t *sel_node_create(mem_heap_t *heap);

/**
 * @brief Frees the memory private to a select node when a query graph is freed.
 *
 * This function does not free the heap where the node was originally created.
 *
 * @param[in] node The select node struct whose private memory is to be freed.
 */
void sel_node_free_private(sel_node_t *node);

/**
 * @brief Frees a prefetch buffer for a column.
 *
 * This function frees a prefetch buffer for a column, including the dynamically allocated
 * memory for data stored there.
 *
 * @param[in, own] prefetch_buf The prefetch buffer to be freed.
 */
void sel_col_prefetch_buf_free(sel_buf_t *prefetch_buf);

/**
 * @brief Gets the plan node for the nth table in a join.
 *
 * This function retrieves the plan node for the specified table index in a join.
 *
 * @param[in] node The select node.
 * @param[in] i The index of the table in the join.
 * @return A pointer to the plan node for the nth table.
 */
inline Plan *sel_node_get_nth_plan(
  sel_node_t *node, /*!< in: select node */
  ulint i
); /*!< in: get ith plan node */

/**
 * @brief Performs a select step.
 *
 * This is a high-level function used in SQL execution graphs.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
que_thr_t *row_sel_step(que_thr_t *thr); /*!< in: query thread */

/**
 * @brief Performs an execution step of an open or close cursor statement node.
 *
 * This function executes a step in the process of opening or closing a cursor
 * statement node within a query execution graph.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
inline que_thr_t *open_step(que_thr_t *thr); /*!< in: query thread */

/**
 * @brief Performs a fetch for a cursor.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
que_thr_t *fetch_step(que_thr_t *thr); /*!< in: query thread */

/**
 * @brief Sample callback function for fetch that prints each row.
 *
 * This function is a sample callback used during a fetch operation. It prints each row.
 *
 * @param[in] row The row to be printed (of type sel_node_t*).
 * @param[in] user_arg Not used.
 * @return Always returns a non-nullptr.
 */
void *row_fetch_print(
  void *row, /*!< in:  sel_node_t* */
  void *user_arg
); /*!< in:  not used */

/**
 * @brief Callback function for fetch that stores an unsigned 4-byte integer to the location pointed.
 *
 * This function is used as a callback during a fetch operation. It stores an unsigned 4-byte integer
 * to the location pointed by the user argument. The column's type must be DATA_INT, DATA_UNSIGNED, 
 * with a length of 4.
 *
 * @param[in] row The row to be processed (of type sel_node_t*).
 * @param[in] user_arg Pointer to the location where the data will be stored.
 * @return Always returns nullptr.
 */
void *row_fetch_store_uint4(void *row, void *user_arg);

/**
 * @brief Prints a row in a select result.
 *
 * This function prints a row in the result of a select query.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
que_thr_t *row_printf_step(que_thr_t *thr); /*!< in: query thread */

/**
 * @brief Builds a dummy query graph used in selects.
 *
 * This function constructs a dummy query graph for use in select operations.
 *
 * @param[in] prebuilt The prebuilt handle.
 */
void row_sel_prebuild_graph(row_prebuilt_t *prebuilt); /*!< in: prebuilt handle */

/**
 * @brief This function performs multiple operations including:
 * 
 * 1. Move to/Search for a record
 * 2. Fetch the next record
 * 3. Fetch the previous record
 * 4. Set appropriate locks (gap and table locks)
 * 5. Handle rollback
 * 
 * This function opens a cursor and also implements fetch next and fetch
 * previous operations. Note that if a search is performed with a full key value
 * from a unique index (ROW_SEL_EXACT), the cursor position will not be stored,
 * and fetch next or fetch previous operations must not be attempted on the cursor.
 * 
 * @param[in] recovery Recovery flag.
 * @param[in] mode Search mode.
 * @param[in] prebuilt Prebuilt struct for the table handle; this contains the info
 *                     of search_tuple, index; if search tuple contains 0 fields then
 *                     the cursor is positioned at the start or the end of the index,
 *                     depending on 'mode'.
 * @param[in] match_mode Mode for matching the key.
 * @param[in] direction Cursor operation. Note: if this is not ROW_SEL_MOVETO, then
 *                      prebuilt must have a pcur with stored position! In opening
 *                      of a cursor 'direction' should be ROW_SEL_MOVETO.
 * 
 * @return DB_SUCCESS, DB_RECORD_NOT_FOUND, DB_END_OF_INDEX, DB_DEADLOCK,
 *         DB_LOCK_TABLE_FULL, DB_CORRUPTION, or DB_TOO_BIG_RECORD.
 */
db_err row_search_for_client(ib_recovery_t recovery, ib_srch_mode_t mode, row_prebuilt_t *prebuilt, ib_match_t match_mode, ib_cur_op_t direction);

/**
 * @brief Reads the current row from the fetch cache.
 *
 * This function retrieves the current row from the fetch cache.
 *
 * @param[in] prebuilt The prebuilt struct for the table handle.
 * @return The current row from the row cache.
 */
const rec_t *row_sel_row_cache_get(row_prebuilt_t *prebuilt);

/**
 * @brief Pops a cached row from the fetch cache.
 *
 * This function retrieves the next row from the fetch cache and removes it from the cache.
 *
 * @param[in] prebuilt The prebuilt struct for the table handle.
 * @return DB_SUCCESS if all OK else error code.
 */
void row_sel_row_cache_next(row_prebuilt_t *prebuilt); /*!< in: prebuilt struct */

/**
 * @brief Checks if there are any rows in the cache.
 *
 * This function checks whether the row cache is empty.
 *
 * @param[in] prebuilt The prebuilt struct for the table handle.
 * @return true if the row cache is empty, false otherwise.
 */
bool row_sel_row_cache_is_empty(row_prebuilt_t *prebuilt); /*!< in: prebuilt struct */

/** A structure for caching column values for prefetched rows */
struct sel_buf_struct {
  /** data, or nullptr; if not nullptr, this field has allocated memory
   * which must be explicitly freed; can be != nullptr even when len is UNIV_SQL_NULL */
  byte *data;

  /** data length or UNIV_SQL_NULL */
  ulint len; 

  /** size of memory buffer allocated for data: this can be more than len;
   * this is defined when data != nullptr */
  ulint val_buf_size;
};

/** Query plan */
struct Plan {
  Plan(FSP *fsp, Btree *btree, Lock_sys *lock_sys) noexcept
  : pcur{fsp, btree, lock_sys},
    clust_pcur{fsp, btree, lock_sys} { }

  /** Table struct in the dictionary cache */
  dict_table_t *table;

  /** table index used in the search */
  dict_index_t *index;

  /** persistent cursor used to search the index */
  Btree_pcursor pcur{srv_fsp, srv_btree_sys, srv_lock_sys};

  /** true if cursor traveling upwards */
  bool asc{};

  /** true if pcur has been positioned and we can try to fetch new rows */
  bool pcur_is_open{};

  /** true if the cursor is open but we know that there are no more qualifying
   * rows left to retrieve from the index tree; NOTE though, that there may still
   * be unprocessed rows in the prefetch stack; always false when pcur_is_open is false */
  bool cursor_at_end{};

  /** true if the pcur position has been stored and the record it is positioned
   * on has already been processed */
  bool stored_cursor_rec_processed{};

  /** array of expressions which are used to calculate the field values in the
   * search tuple: there is one expression for each field in the search tuple */
  que_node_t **tuple_exps{};

  /** search tuple */
  dtuple_t *tuple{};

  /** search mode: PAGE_CUR_G, ... */
  ib_srch_mode_t mode{};

  /** number of first fields in the search tuple which must be exactly matched */
  ulint n_exact_match{};

  /** true if we are searching an index record with a unique key */
  bool unique_search{};

  /** number of rows fetched using pcur after it was opened */
  ulint n_rows_fetched{};

  /** number of prefetched rows cached for fetch: fetching several rows in the same mtr saves CPU time */
  ulint n_rows_prefetched;

  /** index of the first cached row in select buffer arrays for each column */
  ulint first_prefetched{};

  /** no prefetch for this table */
  bool no_prefetch{};

  /** symbol table nodes for the columns to retrieve from the table */
  sym_node_list_t columns{};

  /** conditions which determine the fetch limit of the index segment we have to look at:
   * when one of these fails, the result set has been exhausted for the cursor in this index;
   * these conditions are normalized so that in a comparison the column for this table is the
   * first argument */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, cond_list) end_conds{};

  /** the rest of search conditions we can test at this table in a join */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, func_node_list) other_conds{};

  /** true if index is a non-clustered index and we must also fetch the clustered index record;
   * this is the case if the non-clustered record does not contain all the needed columns,
   * or if this is a single-table explicit cursor, or a searched update or delete */
  bool must_get_clust{};

  /** map telling how clust_ref is built from the fields of a non-clustered record */
  ulint *clust_map{};

  /** the reference to the clustered index entry is built here if index is a non-clustered index */
  dtuple_t *clust_ref{};

  /** if index is non-clustered, we use this pcur to search the clustered index */
  Btree_pcursor clust_pcur{srv_fsp, srv_btree_sys, srv_lock_sys};

  /** memory heap used in building an old version of a row, or nullptr */
  mem_heap_t *old_vers_heap{};
};

/** Select node states */
enum sel_node_state {
  /** it is a declared cursor which is not currently open */
  SEL_NODE_CLOSED,     

  /** intention locks not yet set on tables */
  SEL_NODE_OPEN,       

  /** intention locks have been set */
  SEL_NODE_FETCH,      

  /** cursor has reached the result set end */
  SEL_NODE_NO_MORE_ROWS
};

/** Select statement node */
struct sel_node_t {
  /** node type: QUE_NODE_SELECT */
  que_common_t common;

  /** node state */
  sel_node_state state;

  /** select list */
  que_node_t *select_list;  

  /** variables list or nullptr */
  sym_node_t *into_list;    

  /** table list */
  sym_node_t *table_list;   

  /** true if the rows should be fetched in an ascending order */
  bool asc;                 

  /** true if the cursor is for update or delete, which means that a row x-lock should be placed on the cursor row */
  bool set_x_locks;         

  /** LOCK_X or LOCK_S */
  Lock_mode row_lock_mode;  

  /** number of tables */
  ulint n_tables;           

  /** number of the next table to access in the join */
  ulint fetch_table;        

  /** array of n_tables many plan nodes containing the search plan and the search data structures */
  Plan *m_plans;            

  /** search condition */
  que_node_t *search_cond;  

  /** if the query is a non-locking consistent read, its read view is placed here, otherwise nullptr */
  read_view_t *read_view;   

  /** true if the select is a consistent, non-locking read */
  bool consistent_read;     

  /** order by column definition, or nullptr */
  order_node_t *order_by;   

  /** true if the select list consists of aggregate functions */
  bool is_aggregate;        

  /** true if the aggregate row has already been fetched for the current cursor */
  bool aggregate_already_fetched;

  /** this is true if the select is in a single-table explicit cursor which can get updated within the stored
   * procedure, or in a searched update or delete; NOTE that to determine of an explicit cursor if it can
   * get updated, the parser checks from a stored procedure if it contains positioned update or delete statements */
  bool can_get_updated;       

  /** not nullptr if an explicit cursor */
  sym_node_t *explicit_cursor;

  /** variables whose values we have to copy when an explicit cursor is opened, so that they do not
   * change between fetches */
  UT_LIST_BASE_NODE_T(sym_node_t, sym_list) copy_variables;
};

/** Fetch statement node */
struct fetch_node_struct {
  /** type: QUE_NODE_FETCH */
  que_common_t common;   

  /** cursor definition */
  sel_node_t *cursor_def;

  /** variables to set */
  sym_node_t *into_list; 

  /** User callback function or nullptr.  The first argument to the function is a sel_node_t*, containing the
   * results of the SELECT operation for one row. If the function returns nullptr, it is not interested in
   * further rows and the cursor is modified so (cursor % NOTFOUND) is true. If it returns not-nullptr,
   * continue normally. See row_fetch_print() for an example (and a useful debugging tool). */
  pars_user_func_t *func;
};

/** Open or close cursor operation type */
enum open_node_op {
  /** open cursor */
  ROW_SEL_OPEN_CURSOR,

  /** close cursor */
  ROW_SEL_CLOSE_CURSOR
};

/** Open or close cursor statement node */
struct open_node_struct {
  /** type: QUE_NODE_OPEN */
  que_common_t common;      

  /** operation type: open or close cursor */
  open_node_op op_type;

  /** cursor definition */
  sel_node_t *cursor_def;   
};

/** Row printf statement node */
struct row_printf_node_struct {
  /** type: QUE_NODE_ROW_PRINTF */
  que_common_t common; 

  /** select */
  sel_node_t *sel_node;
};

/**
 * @brief Gets the plan node for the nth table in a join.
 *
 * This function retrieves the plan node for the specified table index in a join.
 *
 * @param[in] node The select node.
 * @param[in] i The index of the table in the join.
 * @return A pointer to the plan node for the nth table.
 */
inline Plan *sel_node_get_nth_plan( sel_node_t *node, ulint i) {
  ut_ad(i < node->n_tables);

  return node->m_plans + i;
}

/**
 * @brief Resets the cursor defined by sel_node to the SEL_NODE_OPEN state.
 *
 * This function resets the cursor defined by sel_node to the SEL_NODE_OPEN state,
 * which means that it will start fetching from the start of the result set again,
 * regardless of where it was before, and it will set intention locks on the tables.
 *
 * @param[in] node The select node.
 */
inline void sel_node_reset_cursor(sel_node_t *node) {
  node->state = SEL_NODE_OPEN;
}

/**
 * @brief Performs an execution step of an open or close cursor statement node.
 *
 * This function executes a step in the process of opening or closing a cursor
 * statement node within a query execution graph.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
inline que_thr_t *open_step(que_thr_t *thr) {
  auto node = static_cast<open_node_t *>(thr->run_node);
  ut_ad(que_node_get_type(node) == QUE_NODE_OPEN);

  db_err err = DB_SUCCESS;
  auto sel_node = node->cursor_def;

  if (node->op_type == ROW_SEL_OPEN_CURSOR) {

    /*		if (sel_node->state == SEL_NODE_CLOSED) { */

    sel_node_reset_cursor(sel_node);
    /*		} else {
    err = DB_ERROR;
    } */
  } else {
    if (sel_node->state != SEL_NODE_CLOSED) {

      sel_node->state = SEL_NODE_CLOSED;
    } else {
      err = DB_ERROR;
    }
  }

  if (expect(err, DB_SUCCESS) != DB_SUCCESS) {
    /* SQL error detected */
    log_fatal("SQL error ", (ulong)err);

    ut_error;
  }

  thr->run_node = que_node_get_parent(node);

  return thr;
}
