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

/** @file include/row0ins.h
Insert into a table

Created 4/20/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "data0data.h"
#include "dict0types.h"
#include "que0types.h"
#include "row0types.h"
#include "trx0types.h"

/**
 * @brief Checks if foreign key constraint fails for an index entry.
 * 
 * Sets shared locks which lock either the success or the failure of the constraint.
 * NOTE that the caller must have a shared latch on dict_foreign_key_check_lock.
 * 
 * @param[in] check_ref True if we want to check that the referenced table is ok, false if we want to check the foreign key table.
 * @param[in] foreign Foreign constraint; NOTE that the tables mentioned in it must be in the dictionary cache if they exist at all.
 * @param[in] table If check_ref is true, then the foreign table, else the referenced table.
 * @param[in] entry Index entry for index.
 * @param[in] thr Query thread.
 * 
 * @return DB_SUCCESS, DB_LOCK_WAIT, DB_NO_REFERENCED_ROW, or DB_ROW_IS_REFERENCED.
 */
db_err row_ins_check_foreign_constraint(bool check_ref, const Foreign *foreign, const Table *table, DTuple *entry, que_thr_t *thr);

/**
 * @brief Creates an insert node struct.
 * 
 * @param[in] ins_type The type of insert operation (e.g., INS_VALUES, INS_SEARCHED, INS_DIRECT).
 * @param[in] table The table where the row will be inserted.
 * @param[in] heap The memory heap where the insert node struct will be created.
 * 
 * @return A pointer to the newly created insert node struct.
 */
ins_node_t *row_ins_node_create(ib_ins_mode_t ins_type, Table *table, mem_heap_t *heap);

/**
 * @brief Sets a new row to insert for an INS_DIRECT node.
 * 
 * This function is only used if we have constructed the row separately, 
 * which is a rare case; this function is quite slow.
 * 
 * @param[in] node Insert node.
 * @param[in] row New row (or first row) for the node.
 */
void row_ins_node_set_new_row(ins_node_t *node, DTuple *row);

/**
 * @brief Inserts an index entry to index.
 * 
 * Tries first optimistic, then pessimistic descent down the tree. If the entry matches enough to a delete marked record,
 * performs the insert by updating or delete unmarking the delete marked record.
 * 
 * @param[in] index Index to insert the entry into.
 * @param[in] entry Index entry to insert.
 * @param[in] n_ext Number of externally stored columns.
 * @param[in] foreign True if foreign key constraints should be checked.
 * @param[in] thr Query thread.
 * 
 * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DUPLICATE_KEY, or some other error code.
 */
db_err row_ins_index_entry(const Index *index, DTuple *entry, ulint n_ext, bool foreign, que_thr_t *thr);

/**
 * @brief Inserts a row into a table.
 * 
 * This is a high-level function used in SQL execution graphs.
 * 
 * @param[in] thr Query thread.
 * 
 * @return Query thread to run next or nullptr.
 */
que_thr_t *row_ins_step(que_thr_t *thr);

/**
 * @brief Creates an entry template for each index of a table.
 * 
 * This function initializes an entry template for each index associated with the given insert node.
 * 
 * @param[in] node The insert node for which to create entry templates.
 */
void row_ins_node_create_entry_list(ins_node_t *node);

/**
 * @brief Insert node structure.
 */
struct Insert_node {
  /** Node type: QUE_NODE_INSERT */
  que_common_t common;

  /** Insert type: INS_VALUES, INS_SEARCHED, or INS_DIRECT */
  ib_ins_mode_t ins_type;

  /** Row to insert */
  DTuple *row;

  /** Table where to insert */
  Table *table;

  /** Select in searched insert */
  sel_node_t *select;

  /** List of expressions to evaluate and insert in an INS_VALUES insert */
  que_node_t *values_list;

  /** Node execution state */
  ulint state;

  /** nullptr, or the next index where the index entry should be inserted */
  Index *index;

  /** nullptr, or entry to insert in the index; after a successful insert
   * of the entry, this should be reset to nullptr */
  DTuple *entry;

  /** List of entries, one for each index */
  UT_LIST_BASE_NODE_T(DTuple, tuple_list) entry_list;

  /** Buffer for the row id sys field in row */
  byte *row_id_buf;

  /** Transaction id or the last transaction which executed the node */
  trx_id_t trx_id;

  /** Buffer for the transaction id sys field in row */
  byte *trx_id_buf;

  /** Memory heap used as auxiliary storage; entry_list and sys fields are
   * stored here; if this is nullptr, entry list should be created and
   * buffers for sys fields in row allocated */
  mem_heap_t *entry_sys_heap;

  /** Magic number for validation */
  ulint magic_n;
};

#define INS_NODE_MAGIC_N 15849075
