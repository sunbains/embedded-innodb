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

/** @file include/dict0crea.h
Database object creation

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0pcur.h"
#include "dict0dict.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "row0types.h"

/**
 * Creates a table create graph.
 *
 * This function is responsible for creating a table create graph, which is
 * used to represent the structure of a table in memory. It is typically used
 * during the process of table creation in the database.
 *
 * @param[in] table Table to create, built as a memory data structure.
 * @param[in] heap Heap where the table create graph is created.
 * @param[in] commit If true, commit the transaction.
 *
 * @return Own: table create node.
 */
tab_node_t *tab_create_graph_create(dict_table_t *table, mem_heap_t *heap, bool commit);

/**
 * Creates an index create graph.
 *
 * This function is responsible for creating an index create graph, which is
 * used to represent the structure of an index in memory. It is typically used
 * during the process of index creation in the database.
 *
 * @param[in] index Index to create, built as a memory data structure.
 * @param[in] heap Heap where the index create graph is created.
 * @param[in] commit True if the transaction should be committed.
 *
 * @return Own: index create node.
 */
ind_node_t *ind_create_graph_create(dict_index_t *index, mem_heap_t *heap, bool commit);

/**
 * Creates a table. This is a high-level function used in SQL execution graphs.
 *
 * This function is responsible for creating a table as part of the SQL execution
 * process. It is typically used within execution graphs to handle the creation
 * of tables in the database.
 *
 * @param[in] thr Query thread that is executing the table creation.
 * 
 * @return The next query thread to run, or nullptr if there is no next thread.
 */
que_thr_t *dict_create_table_step(que_thr_t *thr);

/**
 * Creates an index. This is a high-level function used in SQL execution graphs.
 *
 * This function is responsible for creating an index as part of the SQL execution
 * process. It is typically used within execution graphs to handle the creation
 * of indexes on tables.
 *
 * @param[in] thr Query thread that is executing the index creation.
 * 
 * @return The next query thread to run, or nullptr if there is no next thread.
 */
que_thr_t *dict_create_index_step(que_thr_t *thr);

/**
 * Truncates the index tree associated with a row in the SYS_INDEXES table.
 *
 * @param[in] table The table the index belongs to.
 * @param[in] space 0 to truncate, nonzero to create the index tree in the given tablespace.
 * @param[in,out] pcur Persistent cursor pointing to the record in the clustered index of the SYS_INDEXES table. The cursor may be repositioned in this call.
 * @param[in] mtr Mini-transaction having the latch on the record page. The mtr may be committed and restarted in this call.
 * 
 * @return New root page number, or FIL_NULL on failure.
 */
page_no_t dict_truncate_index_tree(dict_table_t *table, space_id_t space, btr_pcur_t *pcur, mtr_t *mtr);

/**
 * Drops the index tree associated with a row in the SYS_INDEXES table.
 *
 * This function frees all the pages of the index tree except the root page
 * first, which may span several mini-transactions. Then it frees the root
 * page in the same mini-transaction where it writes FIL_NULL to the appropriate
 * field in the SYS_INDEXES record, marking the B-tree as totally freed.
 *
 * @param[in,out] rec Record in the clustered index of SYS_INDEXES table.
 * @param[in] mtr Mini-transaction having the latch on the record page.
 */
void dict_drop_index_tree(rec_t *rec, mtr_t *mtr);

/**
 * Creates the foreign key constraints system tables inside InnoDB
 * at database creation or database start if they are not found or are
 * not of the right form.
 *
 * This function ensures that the necessary system tables for foreign key
 * constraints are present in the InnoDB database. If the tables are missing
 * or not in the correct format, they will be created or corrected.
 *
 * @return DB_SUCCESS on success, or an error code on failure.
 */
db_err dict_create_or_check_foreign_constraint_tables();

/**
 * Adds foreign key definitions to data dictionary tables in the database.
 * We look at table->foreign_list, and also generate names to constraints
 * that were not named by the user. A generated constraint has a name of
 * the format databasename/tablename_ibfk_NUMBER, where the numbers start
 * from 1, and are given locally for this table, that is, the number is
 * not global, as in the old format constraints < 4.0.18 it used to be.
 *
 * @param[in] start_id If we are actually doing ALTER TABLE ADD CONSTRAINT,
 *                     we want to generate constraint numbers which are
 *                     bigger than in the table so far; we number the
 *                     constraints from start_id + 1 up; start_id should be
 *                     set to 0 if we are creating a new table, or if the
 *                     table so far has no constraints for which the name
 *                     was generated here.
 * @param[in] table    Table to which foreign key definitions are added.
 * @param[in] trx      Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err dict_create_add_foreigns_to_dictionary(ulint start_id, dict_table_t *table, trx_t *trx);

/** Table create node structure */
struct tab_node_t {
  /** Node type: QUE_NODE_TABLE_CREATE */
  que_common_t common;

   /** Table to create, built as a memory data structure
    * with dict_mem_... functions */
  dict_table_t *table;

  /** Child node which does the insert of the table definition;
   * the row to be inserted is built by the parent node  */
  ins_node_t *tab_def;

  /** Child node which does the inserts of the column definitions;
   * the row to be inserted is built by the parent node  */
  ins_node_t *col_def;

  /** Child node which performs a commit after a successful table creation */
  commit_node_t *commit_node;

  /*----------------------*/
  /* Local storage for this graph node */

  /** Node execution state */
  ulint state;

  /** Next column definition to insert */
  ulint col_no;

  /** Memory heap used as auxiliary storage */
  mem_heap_t *heap;
};

/* Table create node states */
constexpr ulint TABLE_BUILD_TABLE_DEF = 1;
constexpr ulint TABLE_BUILD_COL_DEF = 2;
constexpr ulint TABLE_COMMIT_WORK = 3;
constexpr ulint TABLE_ADD_TO_CACHE = 4;
constexpr ulint TABLE_COMPLETED = 5;

/* Index create node struct */

struct ind_node_t {
  /** Node type: QUE_NODE_INDEX_CREATE */
  que_common_t common;

  /** Index to create, built as a memory data structure with dict_mem_... functions */
  dict_index_t *index;

  /* Child node which does the insert of the index definition;
   * the row to be inserted is built by the parent node  */
  ins_node_t *ind_def;

  /** Child node which does the inserts of the field definitions;
   * the row to be inserted is built by the parent node  */
  ins_node_t *field_def;

  /** Child node which performs a commit after a successful index creation */
  commit_node_t *commit_node;


  /*----------------------*/
  /* Local storage for this graph node */

  /** Node execution state */
  ulint state;

  /** Root page number of the index */
  page_no_t page_no;

   /** Table which owns the index */
  dict_table_t *table;

  /** Index definition row built */
  dtuple_t *ind_row;

  /** Next field definition to insert */
  ulint field_no;

  /** Memory heap used as auxiliary storage */
  mem_heap_t *heap;
};

/* Index create node states */
constexpr ulint INDEX_BUILD_INDEX_DEF = 1;
constexpr ulint INDEX_BUILD_FIELD_DEF = 2;
constexpr ulint INDEX_CREATE_INDEX_TREE = 3;
constexpr ulint INDEX_COMMIT_WORK = 4;
constexpr ulint INDEX_ADD_TO_CACHE = 5;
