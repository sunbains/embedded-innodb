/****************************************************************************
Copyright (c) 2005, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/row0merge.h
Index build routines using a merge sort

Created 13/06/2005 Jan Lindstrom
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "lock0types.h"
#include "row0types.h"
#include "srv0srv.h"

struct Trx;
struct Table;

/** Index field definition */
struct merge_index_field_t {
  /** Column prefix length, or 0 if indexing the whole column */
  ulint prefix_len;

  /** Field name */
  const char *field_name;
};

/** Definition of an index being created */
struct merge_index_def_t {
  /** Index name */
  const char *name;

  /** *  0, DICT_UNIQUE, or DICT_CLUSTERED */
  ulint ind_type;

  /** Number of fields in index */
  ulint n_fields;

  /** field definitions */
  merge_index_field_t *fields;
};

/**
 * Sets an exclusive lock on a table, for the duration of creating indexes.
 *
 * @param trx   in/out: transaction
 * @param table in: table to lock
 * @param mode  in: LOCK_X or LOCK_S
 *
 * @return error code or DB_SUCCESS
 */
db_err row_merge_lock_table(Trx *trx, Table *table, Lock_mode mode);

/**
 * Drop an index from the InnoDB system tables. The data dictionary must
 * have been locked exclusively by the caller, because the transaction
 * will not be committed.
 *
 * @param index in: index to be removed
 * @param table in: table
 * @param trx   in: transaction handle
 */
void row_merge_drop_index(Index *index, Table *table, Trx *trx);

/**
 * Drop those indexes which were created before an error occurred when
 * building an index. The data dictionary must have been locked
 * exclusively by the caller, because the transaction will not be
 * committed.
 *
 * @param trx         in: transaction
 * @param table       in: table containing the indexes
 * @param index       in: indexes to drop
 * @param num_created in: number of elements in index[]
 */
void row_merge_drop_indexes(Trx *trx, Table *table, Index **index, ulint num_created);

/**
 * Drop all partially created indexes during crash recovery.
 *
 * @param recovery in: recovery level setting
 */
void row_merge_drop_temp_indexes(ib_recovery_t recovery);

/**
 * Rename the tables in the data dictionary. The data dictionary must
 * have been locked exclusively by the caller, because the transaction
 * will not be committed.
 *
 * @param old_table in/out: old table, renamed to tmp_name
 * @param new_table in/out: new table, renamed to old_table->name
 * @param tmp_name  in: new name for old_table
 * @param trx       in: transaction handle
 *
 * @return error code or DB_SUCCESS
 */
db_err row_merge_rename_tables(
  Table *old_table,
  Table *new_table,
  const char *tmp_name,
  Trx *trx
);

/**
 * Create a temporary table for creating a primary key, using the definition
 * of an existing table.
 *
 * @param table_name in: new table name
 * @param index_def  in: the index definition of the primary key
 * @param table      in: old table definition
 * @param trx        in/out: transaction (sets error_state)
 *
 * @return table, or nullptr on error
 */
Table *row_merge_create_temporary_table(
  const char *table_name,
  const merge_index_def_t *index_def,
  const Table *table,
  Trx *trx
);

/**
 * Rename the temporary indexes in the dictionary to permanent ones. The
 * data dictionary must have been locked exclusively by the caller,
 * because the transaction will not be committed.
 *
 * @param trx   in/out: transaction
 * @param table in/out: table with new indexes
 *
 * @return DB_SUCCESS if all OK
 */
db_err row_merge_rename_indexes( Trx *trx, Table *table);

/**
 * Create the index and load in to the dictionary.
 *
 * @param trx       in/out: trx (sets error_state)
 * @param table     in: the index is on this table
 * @param index_def in: the index definition
 *
 * @return index, or nullptr on error
 */
Index *row_merge_create_index(
  Trx *trx,
  Table *table,
  const merge_index_def_t *index_def
);

/**
 * Check if a transaction can use an index.
 *
 * @param trx   in: transaction
 * @param index in: index to check
 *
 * @return true if index can be used by the transaction else false
 */
bool row_merge_is_index_usable(const Trx *trx, const Index *index);

/**
 * If there are views that refer to the old table name then we "attach" to
 * the new instance of the table else we drop it immediately.
 *
 * @param trx   in: transaction
 * @param table in: table instance to drop
 *
 * @return DB_SUCCESS or error code
 */
db_err row_merge_drop_table(Trx *trx, Table *table);

/**
 * Build indexes on a table by reading a clustered index,
 * creating a temporary file containing index entries, merge sorting
 * these index entries and inserting sorted index entries to indexes.
 *
 * @param trx        in: transaction
 * @param old_table  in: table where rows are read from
 * @param new_table  in: table where indexes are created; identical to old_table
 *                      unless creating a PRIMARY KEY
 * @param indexes    in: indexes to be created
 * @param n_indexes  in: size of indexes[]
 * @param table      in/out: table, for reporting erroneous key value if applicable
 *
 * @return DB_SUCCESS or error code
 */
db_err row_merge_build_indexes(
  Trx *trx,
  Table *old_table,
  Table *new_table,
  Index **indexes,
  ulint n_indexes,
  table_handle_t table
);
