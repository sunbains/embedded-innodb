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

/** Creates a select node struct.
@return	own: select node struct */

sel_node_t *sel_node_create(mem_heap_t *heap); /*!< in: memory heap where created */
/** Frees the memory private to a select node when a query graph is freed,
does not free the heap where the node was originally created. */

void sel_node_free_private(sel_node_t *node); /*!< in: select node struct */
/** Frees a prefetch buffer for a column, including the dynamically allocated
memory for data stored there. */

void sel_col_prefetch_buf_free(sel_buf_t *prefetch_buf); /*!< in, own: prefetch buffer */
/** Gets the plan node for the nth table in a join.
@return	plan node */
inline plan_t *sel_node_get_nth_plan(
  sel_node_t *node, /*!< in: select node */
  ulint i
); /*!< in: get ith plan node */
/** Performs a select step. This is a high-level function used in SQL execution
graphs.
@return	query thread to run next or nullptr */

que_thr_t *row_sel_step(que_thr_t *thr); /*!< in: query thread */
/** Performs an execution step of an open or close cursor statement node.
@return	query thread to run next or nullptr */
inline que_thr_t *open_step(que_thr_t *thr); /*!< in: query thread */
/** Performs a fetch for a cursor.
@return	query thread to run next or nullptr */

que_thr_t *fetch_step(que_thr_t *thr); /*!< in: query thread */
/** Sample callback function for fetch that prints each row.
@return	always returns non-nullptr */

void *row_fetch_print(
  void *row, /*!< in:  sel_node_t* */
  void *user_arg
); /*!< in:  not used */
/** Callback function for fetch that stores an unsigned 4 byte integer to the
location pointed. The column's type must be DATA_INT, DATA_UNSIGNED, length
= 4.
@return	always returns nullptr */

void *row_fetch_store_uint4(
  void *row, /*!< in:  sel_node_t* */
  void *user_arg
); /*!< in:  data pointer */
/** Prints a row in a select result.
@return	query thread to run next or nullptr */

que_thr_t *row_printf_step(que_thr_t *thr); /*!< in: query thread */
/** Builds a dummy query graph used in selects. */

void row_sel_prebuild_graph(row_prebuilt_t *prebuilt); /*!< in: prebuilt handle */

/** This function does several things, in fact too many things:

 1. Moveto/Search for a record
 2. Next record
 3. Prev record
 4. Set appropriate locks (gap and table locks).
 5. Handle rollback

This function opens a cursor, and also implements fetch next and fetch
prev. NOTE that if we do a search with a full key value from a unique
index (ROW_SEL_EXACT), then we will not store the cursor position and
fetch next or fetch prev must not be tried to the cursor!
@return	DB_SUCCESS, DB_RECORD_NOT_FOUND, DB_END_OF_INDEX, DB_DEADLOCK,
DB_LOCK_TABLE_FULL, DB_CORRUPTION, or DB_TOO_BIG_RECORD */

enum db_err row_search_for_client(
  ib_recovery_t recovery,   /*!< in: recovery flag */
  ib_srch_mode_t mode,      /*!< in: search mode */
  row_prebuilt_t *prebuilt, /*!< in: prebuilt struct for the
                              table handle; this contains the info
                              of search_tuple, index; if search
                              tuple contains 0 fields then we
                              position the cursor at the start or
                              the end of the index, depending on
                              'mode' */
  ib_match_t match_mode,    /*!< in: mode for matching the key */
  ib_cur_op_t direction
); /*!< in: cursor operation, NOTE: if this
                              is != ROW_SEL_MOVETO, then prebuilt
                              must have a pcur with stored position!
                              In opening of a cursor 'direction'
                              should be ROW_SEL_MOVETO */

/** Reads the current row from the fetch cache.
@return current row from the row cache. */

const rec_t *row_sel_row_cache_get(row_prebuilt_t *prebuilt); /*!< in: prebuilt struct */

/** Pops a cached row from the fetch cache.
@return	DB_SUCCESS if all OK else error code */

void row_sel_row_cache_next(row_prebuilt_t *prebuilt); /*!< in: prebuilt struct */

/** Check if there are any rows in the cache.
@return true if row cache is empty. */

bool row_sel_row_cache_is_empty(row_prebuilt_t *prebuilt); /*!< in: prebuilt struct */

/** A structure for caching column values for prefetched rows */
struct sel_buf_struct {
  byte *data; /*!< data, or nullptr; if not nullptr, this field
              has allocated memory which must be explicitly
              freed; can be != nullptr even when len is
              UNIV_SQL_NULL */
  ulint len;  /*!< data length or UNIV_SQL_NULL */
  ulint val_buf_size;
  /*!< size of memory buffer allocated for data:
  this can be more than len; this is defined
  when data != nullptr */
};

/** Query plan */
struct plan_struct {
  dict_table_t *table; /*!< table struct in the dictionary
                       cache */
  dict_index_t *index; /*!< table index used in the search */
  btr_pcur_t pcur;     /*!< persistent cursor used to search
                       the index */
  bool asc;            /*!< true if cursor traveling upwards */
  bool pcur_is_open;   /*!< true if pcur has been positioned
                        and we can try to fetch new rows */
  bool cursor_at_end;  /*!< true if the cursor is open but
                        we know that there are no more
                        qualifying rows left to retrieve from
                        the index tree; NOTE though, that
                        there may still be unprocessed rows in
                        the prefetch stack; always false when
                        pcur_is_open is false */
  bool stored_cursor_rec_processed;
  /*!< true if the pcur position has been
  stored and the record it is positioned
  on has already been processed */
  que_node_t **tuple_exps; /*!< array of expressions
                           which are used to calculate
                           the field values in the search
                           tuple: there is one expression
                           for each field in the search
                           tuple */
  dtuple_t *tuple;         /*!< search tuple */
  ib_srch_mode_t mode;     /*!< search mode: PAGE_CUR_G, ... */
  ulint n_exact_match;     /*!< number of first fields in
                           the search tuple which must be
                           exactly matched */
  bool unique_search;      /*!< true if we are searching an
                            index record with a unique key */
  ulint n_rows_fetched;    /*!< number of rows fetched using pcur
                           after it was opened */
  ulint n_rows_prefetched; /*!< number of prefetched rows cached
                         for fetch: fetching several rows in
                         the same mtr saves CPU time */
  ulint first_prefetched;  /*!< index of the first cached row in
                          select buffer arrays for each column */
  bool no_prefetch;        /*!< no prefetch for this table */
  sym_node_list_t columns; /*!< symbol table nodes for the columns
                           to retrieve from the table */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, cond_list)
  end_conds; /*!< conditions which determine the
             fetch limit of the index segment we
             have to look at: when one of these
             fails, the result set has been
             exhausted for the cursor in this
             index; these conditions are normalized
             so that in a comparison the column
             for this table is the first argument */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, func_node_list)
  other_conds;               /*!< the rest of search conditions we
                             can test at this table in a join */
  bool must_get_clust;       /*!< true if index is a non-clustered
                              index and we must also fetch the
                              clustered index record; this is the
                              case if the non-clustered record does
                              not contain all the needed columns, or
                              if this is a single-table explicit
                              cursor, or a searched update or
                              delete */
  ulint *clust_map;          /*!< map telling how clust_ref is built
                             from the fields of a non-clustered
                             record */
  dtuple_t *clust_ref;       /*!< the reference to the clustered
                             index entry is built here if index is
                             a non-clustered index */
  btr_pcur_t clust_pcur;     /*!< if index is non-clustered, we use
                             this pcur to search the clustered
                             index */
  mem_heap_t *old_vers_heap; /*!< memory heap used in building an old
                             version of a row, or nullptr */
};

/** Select node states */
enum sel_node_state {
  SEL_NODE_CLOSED,      /*!< it is a declared cursor which is not
                        currently open */
  SEL_NODE_OPEN,        /*!< intention locks not yet set on tables */
  SEL_NODE_FETCH,       /*!< intention locks have been set */
  SEL_NODE_NO_MORE_ROWS /*!< cursor has reached the result set end */
};

/** Select statement node */
struct sel_node_t {
  que_common_t common;       /*!< node type: QUE_NODE_SELECT */
  enum sel_node_state state; /*!< node state */
  que_node_t *select_list;   /*!< select list */
  sym_node_t *into_list;     /*!< variables list or nullptr */
  sym_node_t *table_list;    /*!< table list */
  bool asc;                  /*!< true if the rows should be fetched
                              in an ascending order */
  bool set_x_locks;          /*!< true if the cursor is for update or
                              delete, which means that a row x-lock
                              should be placed on the cursor row */
  Lock_mode row_lock_mode;   /*!< LOCK_X or LOCK_S */
  ulint n_tables;            /*!< number of tables */
  ulint fetch_table;         /*!< number of the next table to access
                             in the join */
  plan_t *plans;             /*!< array of n_tables many plan nodes
                             containing the search plan and the
                             search data structures */
  que_node_t *search_cond;   /*!< search condition */
  read_view_t *read_view;    /*!< if the query is a non-locking
                             consistent read, its read view is
                             placed here, otherwise nullptr */
  bool consistent_read;      /*!< true if the select is a consistent,
                              non-locking read */
  order_node_t *order_by;    /*!< order by column definition, or
                             nullptr */
  bool is_aggregate;         /*!< true if the select list consists of
                              aggregate functions */
  bool aggregate_already_fetched;
  /*!< true if the aggregate row has
  already been fetched for the current
  cursor */
  bool can_get_updated;        /*!< this is true if the select
                                is in a single-table explicit
                                cursor which can get updated
                                within the stored procedure,
                                or in a searched update or
                                delete; NOTE that to determine
                                of an explicit cursor if it
                                can get updated, the parser
                                checks from a stored procedure
                                if it contains positioned
                                update or delete statements */
  sym_node_t *explicit_cursor; /*!< not nullptr if an explicit cursor */
  UT_LIST_BASE_NODE_T(sym_node_t, sym_list)
  copy_variables; /*!< variables whose values we have to
                  copy when an explicit cursor is opened,
                  so that they do not change between
                  fetches */
};

/** Fetch statement node */
struct fetch_node_struct {
  que_common_t common;    /*!< type: QUE_NODE_FETCH */
  sel_node_t *cursor_def; /*!< cursor definition */
  sym_node_t *into_list;  /*!< variables to set */

  pars_user_func_t *func; /*!< User callback function or nullptr.
                          The first argument to the function
                          is a sel_node_t*, containing the
                          results of the SELECT operation for
                          one row. If the function returns
                          nullptr, it is not interested in
                          further rows and the cursor is
                          modified so (cursor % NOTFOUND) is
                          true. If it returns not-nullptr,
                          continue normally. See
                          row_fetch_print() for an example
                          (and a useful debugging tool). */
};

/** Open or close cursor operation type */
enum open_node_op {
  ROW_SEL_OPEN_CURSOR, /*!< open cursor */
  ROW_SEL_CLOSE_CURSOR /*!< close cursor */
};

/** Open or close cursor statement node */
struct open_node_struct {
  que_common_t common;       /*!< type: QUE_NODE_OPEN */
  enum open_node_op op_type; /*!< operation type: open or
                             close cursor */
  sel_node_t *cursor_def;    /*!< cursor definition */
};

/** Row printf statement node */
struct row_printf_node_struct {
  que_common_t common;  /*!< type: QUE_NODE_ROW_PRINTF */
  sel_node_t *sel_node; /*!< select */
};

/** Gets the plan node for the nth table in a join.
@return	plan node */
inline plan_t *sel_node_get_nth_plan(
  sel_node_t *node, /*!< in: select node */
  ulint i
) /*!< in: get ith plan node */
{
  ut_ad(i < node->n_tables);

  return (node->plans + i);
}

/** Resets the cursor defined by sel_node to the SEL_NODE_OPEN state, which
means that it will start fetching from the start of the result set again,
regardless of where it was before, and it will set intention locks on the
tables. */
inline void sel_node_reset_cursor(sel_node_t *node) /*!< in: select node */
{
  node->state = SEL_NODE_OPEN;
}

/** Performs an execution step of an open or close cursor statement node.
@return	query thread to run next or nullptr */
inline que_thr_t *open_step(que_thr_t *thr) /*!< in: query thread */
{
  sel_node_t *sel_node;
  open_node_t *node;
  ulint err;

  ut_ad(thr);

  node = (open_node_t *)thr->run_node;
  ut_ad(que_node_get_type(node) == QUE_NODE_OPEN);

  sel_node = node->cursor_def;

  err = DB_SUCCESS;

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
    ib_logger(ib_stream, "SQL error %lu\n", (ulong)err);

    ut_error;
  }

  thr->run_node = que_node_get_parent(node);

  return (thr);
}
