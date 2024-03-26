/****************************************************************************
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

/** @file include/row0ins.h
Insert into a table

Created 4/20/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "que0types.h"
#include "row0types.h"
#include "trx0types.h"

/** Checks if foreign key constraint fails for an index entry. Sets shared locks
which lock either the success or the failure of the constraint. NOTE that
the caller must have a shared latch on dict_foreign_key_check_lock.
@return DB_SUCCESS, DB_LOCK_WAIT, DB_NO_REFERENCED_ROW, or
DB_ROW_IS_REFERENCED */
db_err row_ins_check_foreign_constraint(
  bool check_ref,          /*!< in: true If we want to check that
                            the referenced table is ok, false if we
                            want to check the foreign key table */
  dict_foreign_t *foreign, /*!< in: foreign constraint; NOTE that the
                             tables mentioned in it must be in the
                             dictionary cache if they exist at all */
  dict_table_t *table,     /*!< in: if check_ref is true, then the foreign
                             table, else the referenced table */
  dtuple_t *entry,         /*!< in: index entry for index */
  que_thr_t *thr
); /*!< in: query thread */

/** Creates an insert node struct.
@return	own: insert node struct */
ins_node_t *row_ins_node_create(
  ib_ins_mode_t ins_type, /*!< in: INS_VALUES, ... */
  dict_table_t *table,    /*!< in: table where to insert */
  mem_heap_t *heap
); /*!< in: mem heap where created */

/** Sets a new row to insert for an INS_DIRECT node. This function is only used
if we have constructed the row separately, which is a rare case; this
function is quite slow. */
void row_ins_node_set_new_row(
  ins_node_t *node, /*!< in: insert node */
  dtuple_t *row
); /*!< in: new row (or first row) for the node */

/** Inserts an index entry to index. Tries first optimistic, then pessimistic
descent down the tree. If the entry matches enough to a delete marked record,
performs the insert by updating or delete unmarking the delete marked
record.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DUPLICATE_KEY, or some other error code */
db_err row_ins_index_entry(
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in: index entry to insert */
  ulint n_ext,         /*!< in: number of externally stored columns */
  bool foreign,        /*!< in: true=check foreign key constraints */
  que_thr_t *thr
); /*!< in: query thread */

/** Inserts a row to a table. This is a high-level function used in
SQL execution graphs.
@return	query thread to run next or nullptr */
que_thr_t *row_ins_step(que_thr_t *thr); /*!< in: query thread */

/** Creates an entry template for each index of a table. */
void row_ins_node_create_entry_list(ins_node_t *node); /*!< in: row insert node */

/* Insert node structure */
struct ins_node_struct {
  que_common_t common;     /*!< node type: QUE_NODE_INSERT */
  ib_ins_mode_t ins_type;  /* INS_VALUES, INS_SEARCHED, or INS_DIRECT */
  dtuple_t *row;           /*!< row to insert */
  dict_table_t *table;     /*!< table where to insert */
  sel_node_t *select;      /*!< select in searched insert */
  que_node_t *values_list; /* list of expressions to evaluate and
                       insert in an INS_VALUES insert */
  ulint state;             /*!< node execution state */
  dict_index_t *index;     /*!< nullptr, or the next index where the index
                           entry should be inserted */
  dtuple_t *entry;         /*!< nullptr, or entry to insert in the index;
                           after a successful insert of the entry,
                           this should be reset to nullptr */
  UT_LIST_BASE_NODE_T(dtuple_t, tuple_list)
      entry_list;       /* list of entries, one for each index */
  byte *row_id_buf; /* buffer for the row id sys field in row */
  trx_id_t trx_id;  /*!< trx id or the last trx which executed the
                    node */
  byte *trx_id_buf; /* buffer for the trx id sys field in row */
  mem_heap_t *entry_sys_heap;
  /* memory heap used as auxiliary storage;
  entry_list and sys fields are stored here;
  if this is nullptr, entry list should be created
  and buffers for sys fields in row allocated */
  ulint magic_n;
};

#define INS_NODE_MAGIC_N 15849075
