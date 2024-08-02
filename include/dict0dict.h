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

/** @file include/dict0dict.h
Data dictionary system

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "data0data.h"
#include "data0type.h"
#include "dict0load.h"
#include "dict0mem.h"
#include "dict0types.h"
#include "hash0hash.h"
#include "mem0mem.h"
#include "rem0types.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "trx0types.h"
#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "ut0rnd.h"

/** dummy index for ROW_FORMAT=REDUNDANT supremum and infimum records */
extern dict_index_t *dict_ind_redundant;

/** dummy index for ROW_FORMAT=COMPACT supremum and infimum records */
extern dict_index_t *dict_ind_compact;

/* Buffers for storing detailed information about the latest foreign key
and unique key errors */
extern ib_stream_t dict_foreign_err_file;

extern mutex_t dict_foreign_err_mutex; /* mutex protecting the buffers */

/** the dictionary system */
extern dict_sys_t *dict_sys;

/** the data dictionary rw-latch protecting dict_sys */
extern rw_lock_t dict_operation_lock;

/** Inits dict_ind_redundant and dict_ind_compact. */
void dict_ind_init();

/** Closes the data dictionary module. */
void dict_close();

/** Makes all characters in a NUL-terminated UTF-8 string lower case.
@param a string to put in lower case */
void dict_casedn_str(char *a);

/** Get the database name length in a table name.
@param name table name in the form dbname '/' tablename
@return database name length */
ulint dict_get_db_name_len(const char *name);

/** Return the end of table name where we have removed dbname and '/'.
@param name table name in the form dbname '/' tablename
@return table name */
const char *dict_remove_db_name(const char *name);

/** Returns a table object based on table id.
@param recovery recovery flag
@param table_id table id
@param trx transaction handle
@return table, NULL if does not exist */
dict_table_t *dict_table_get_on_id(ib_recovery_t recovery, uint64_t table_id, trx_t *trx);

/** Decrements the count of open handles to a table.
@param table table
@param dict_locked true if data dictionary locked */
void dict_table_decrement_handle_count(dict_table_t *table, bool dict_locked);

/** Increments the count of open client handles to a table.
@param table table
@param dict_locked true if data dictionary locked */
void dict_table_increment_handle_count(dict_table_t *table, bool dict_locked);

/** Inits the data dictionary module. */
void dict_init();

/**
 * @brief Returns a table object and optionally increments its open handle count.
 * 
 * @param table_name The table name.
 * @param inc_count Whether to increment the open handle count on the table.
 * @return The table object, or NULL if it does not exist.
 */
dict_table_t *dict_table_get(const char *table_name, bool inc_count);

/**
 * @brief Returns a table instance based on table id.
 * 
 * @param recovery The recovery flag.
 * @param table_id The table id.
 * @param ref_count Whether to increment open handle count if true.
 * @return The table object, or NULL if it does not exist.
 */
dict_table_t *dict_table_get_using_id(ib_recovery_t recovery, uint64_t table_id, bool ref_count);

/**
 * @brief Returns an index object based on table and index id, and memoryfixes it.
 * 
 * @param table The table.
 * @param index_id The index id.
 * @return The index object, or NULL if it does not exist.
 */
dict_index_t *dict_index_get_on_id_low(dict_table_t *table, uint64_t index_id);

/**
 * @brief Checks if the given column name is reserved for InnoDB system columns.
 * 
 * @param name The column name.
 * @return true if the name is reserved, false otherwise.
 */
bool dict_col_name_is_reserved(const char *name);

/**
 * @brief Adds system columns to a table object.
 * 
 * @param table The table object.
 * @param heap The temporary heap.
 */
void dict_table_add_system_columns(dict_table_t *table, mem_heap_t *heap);

/**
 * @brief Adds a table object to the dictionary cache.
 * 
 * @param table The table object.
 * @param heap The temporary heap.
 */
void dict_table_add_to_cache(dict_table_t *table, mem_heap_t *heap, bool skip_add_to_cache = false);

/**
 * @brief Removes a table object from the dictionary cache.
 * 
 * @param table The table object.
 */
void dict_table_remove_from_cache(dict_table_t *table);

/**
 * @brief Renames a table object.
 * 
 * @param table The table object.
 * @param new_name The new name.
 * @param rename_also_foreigns Whether to preserve the original table name in constraints which reference it.
 * @return true if success, false otherwise.
 */
bool dict_table_rename_in_cache(dict_table_t *table, const char *new_name, bool rename_also_foreigns);

/**
 * @brief Removes an index from the dictionary cache.
 * 
 * @param table The table object.
 * @param index The index object.
 */
void dict_index_remove_from_cache(dict_table_t *table, dict_index_t *index);

/**
 * @brief Changes the id of a table object in the dictionary cache.
 * 
 * @param table The table object.
 * @param new_id The new id to set.
 */
void dict_table_change_id_in_cache(dict_table_t *table, uint64_t new_id);

/**
 * @brief Adds a foreign key constraint object to the dictionary cache.
 * 
 * @param foreign The foreign key constraint object.
 * @param check_charsets Whether to check charset compatibility.
 * @return DB_SUCCESS or error code.
 */
db_err dict_foreign_add_to_cache(dict_foreign_t *foreign, bool check_charsets);

/**
 * @brief Checks if the index is referenced by a foreign key.
 * 
 * @param table The InnoDB table.
 * @param index The InnoDB index.
 * @return Pointer to foreign key struct if index is defined for foreign key, otherwise NULL.
 */
dict_foreign_t *dict_table_get_referenced_constraint(dict_table_t *table, dict_index_t *index);

/**
 * @brief Checks if a table is referenced by foreign keys.
 * 
 * @param table The InnoDB table.
 * @return true if table is referenced by a foreign key, false otherwise.
 */
bool dict_table_is_referenced_by_foreign_key(const dict_table_t *table);

/**
 * @brief Replaces the index in the foreign key list that matches this index's definition with an equivalent index.
 * 
 * @param table The table object.
 * @param index The index to be replaced.
 */
void dict_table_replace_index_in_foreign_list(dict_table_t *table, dict_index_t *index);

/**
 * @brief Checks if an index is defined for a foreign key constraint.
 * 
 * @param table The InnoDB table.
 * @param index The InnoDB index.
 * @return Pointer to foreign key struct if index is defined for foreign key, otherwise NULL.
 */
dict_foreign_t *dict_table_get_foreign_constraint(dict_table_t *table, dict_index_t *index);

/**
 * @brief Scans a table create SQL string and adds the foreign key constraints declared in the string to the data dictionary.
 * 
 * @param trx The transaction.
 * @param sql_string The table create statement where foreign keys are declared.
 * @param name The table full name in the normalized form.
 * @param reject_fks Whether to fail with error code DB_CANNOT_ADD_CONSTRAINT if any foreign keys are found.
 * @return Error code or DB_SUCCESS.
 */
db_err dict_create_foreign_constraints(trx_t *trx, const char *sql_string, const char *name, bool reject_fks);

/**
 * @brief Parses the CONSTRAINT id's to be dropped in an ALTER TABLE statement.
 * 
 * @param heap The heap from which memory can be allocated.
 * @param trx The transaction.
 * @param table The table.
 * @param n The number of constraints to drop.
 * @param constraints_to_drop The id's of the constraints to drop.
 * @return DB_SUCCESS or DB_CANNOT_DROP_CONSTRAINT if syntax error or the constraint id does not match.
 */
db_err dict_foreign_parse_drop_constraints(
  mem_heap_t *heap, trx_t *trx, dict_table_t *table, ulint *n, const char ***constraints_to_drop
);

/**
 * @brief Finds an index that is equivalent to the one passed in and is not marked for deletion.
 * 
 * @param foreign The foreign key.
 * @return The index equivalent to foreign->foreign_index, or NULL.
 */
dict_index_t *dict_foreign_find_equiv_index(dict_foreign_t *foreign);

/**
 * @brief Returns an index object by matching on the name and column names, and if more than one index matches, returns the index with the max id.
 * 
 * @param table The table.
 * @param name The index name to find.
 * @param columns The array of column names.
 * @param n_cols The number of columns.
 * @return The matching index, or NULL if not found.
 */
dict_index_t *dict_table_get_index_by_max_id(dict_table_t *table, const char *name, const char **columns, ulint n_cols);

/**
 * @brief Returns a column's name.
 * 
 * @param table The table.
 * @param col_nr The column number.
 * @return The column name. Note: not guaranteed to stay valid if table is modified in any way.
 */
const char *dict_table_get_col_name(const dict_table_t *table, ulint col_nr);

/**
 * @brief Returns a column's ordinal value.
 * 
 * @param table The table.
 * @param name The column name.
 * @return The column pos. -1 if not found. Note: not guaranteed to stay valid if table is modified in any way.
 */
int dict_table_get_col_no(const dict_table_t *table, const char *name);

/**
 * @brief Prints a table definition.
 * 
 * @param table The table.
 */
void dict_table_print(dict_table_t *table);

/**
 * @brief Prints a table data.
 * 
 * @param table The table.
 */
void dict_table_print_low(dict_table_t *table);

/**
 * @brief Prints a table data when we know the table name.
 * 
 * @param name The table name.
 */
void dict_table_print_by_name(const char *name);

/**
 * @brief Outputs info on foreign keys of a table.
 * 
 * @param create_table_format Whether to print in a format suitable to be inserted into a CREATE TABLE.
 * @param ib_stream The stream where to print.
 * @param trx The transaction.
 * @param table The table.
 */
void dict_print_info_on_foreign_keys(bool create_table_format, ib_stream_t ib_stream, trx_t *trx, dict_table_t *table);

/**
 * @brief Outputs info on a foreign key of a table in a format suitable for CREATE TABLE.
 * 
 * @param stream The file where to print.
 * @param trx The transaction.
 * @param foreign The foreign key constraint.
 * @param add_newline Whether to add a newline.
 */
void dict_print_info_on_foreign_key_in_create_format(ib_stream_t stream, trx_t *trx, dict_foreign_t *foreign, bool add_newline);

/**
 * @brief Displays the names of the index and the table.
 * 
 * @param stream The output stream.
 * @param trx The transaction.
 * @param index The index to print.
 */
void dict_index_name_print(ib_stream_t stream, trx_t *trx, const dict_index_t *index);

/**
 * Checks if a column is in the ordering columns of the clustered index of a table.
 * Column prefixes are treated like whole columns.
 * @param table The table.
 * @param n The column number.
 * @return True if the column, or its prefix, is in the clustered key.
 */
bool dict_table_col_in_clustered_key(const dict_table_t *table, ulint n);

/**
 * Copies types of columns contained in table to tuple and sets all fields of the
 * tuple to the SQL NULL value. This function should be called right after dtuple_create().
 * @param tuple The data tuple.
 * @param table The table.
 */
void dict_table_copy_types(dtuple_t *tuple, const dict_table_t *table);

/**
 * Looks for an index with the given id. NOTE that we do not reserve the
 * dictionary mutex: this function is for emergency purposes like printing
 * info of a corrupt database page!
 * @param id The index id.
 * @return The index or NULL if not found from cache.
 */
dict_index_t *dict_index_find_on_id_low(uint64_t id);

/**
 * Adds an index to the dictionary cache.
 * @param table The table on which the index is.
 * @param index The index.
 * @param page_no The root page number of the index.
 * @param strict True to refuse to create the index if records could be too big to fit in a B-tree page.
 * @return DB_SUCCESS, DB_TOO_BIG_RECORD, or DB_CORRUPTION.
 */
db_err dict_index_add_to_cache(dict_table_t *table, dict_index_t *index, ulint page_no, bool strict);

/**
 * Looks for column n in an index.
 * @param index index
 * @param n column number
 * @return position in internal representation of the index; ULINT_UNDEFINED if not contained
 */
ulint dict_index_get_nth_col_pos(const dict_index_t *index, ulint n);

/**
 * Returns true if the index contains a column or a prefix of that column.
 * @param index index
 * @param n column number
 * @return true if contains the column or its prefix
 */
bool dict_index_contains_col_or_prefix(const dict_index_t *index, ulint n);

/**
 * Looks for a matching field in an index. The column has to be the same.
 * The column in index must be complete, or must contain a prefix longer
 * than the column in index2. That is, we must be able to construct the
 * prefix in index2 from the prefix in index.
 * @param index index from which to search
 * @param index2 index
 * @param n field number in index2
 * @return position in internal representation of the index; ULINT_UNDEFINED if not contained
 */
ulint dict_index_get_nth_field_pos(const dict_index_t *index, const dict_index_t *index2, ulint n);

/**
 * Looks for column n position in the clustered index.
 * @param table table
 * @param n column number
 * @return position in internal representation of the clustered index
 */
ulint dict_table_get_nth_col_pos(const dict_table_t *table, ulint n);

/**
 * Adds a column to index.
 * @param index index
 * @param table table
 * @param col column
 * @param prefix_len column prefix length
 */
void dict_index_add_col(dict_index_t *index, const dict_table_t *table, dict_col_t *col, ulint prefix_len);

/**
 * Copies types of fields contained in index to tuple.
 * @param tuple data tuple
 * @param index index
 * @param n_fields number of field types to copy
 */
void dict_index_copy_types(dtuple_t *tuple, const dict_index_t *index, ulint n_fields);

/**
 * Returns an index object if it is found in the dictionary cache.
 * Assumes that dict_sys->mutex is already being held.
 * @param index_id index id
 * @return index, NULL if not found
 */
dict_index_t *dict_index_get_if_in_cache_low(uint64_t index_id);

/**
 * Returns an index object if it is found in the dictionary cache.
 * @param index_id index id
 * @return index, NULL if not found
 */
dict_index_t *dict_index_get_if_in_cache(uint64_t index_id);

/**
 * Checks that a tuple has n_fields_cmp value in a sensible range,
 * so that no comparison can occur with the page number field in a node pointer.
 * @param index index tree
 * @param tuple tuple used in a search
 * @return true if ok
 */
bool dict_index_check_search_tuple(const dict_index_t *index, const dtuple_t *tuple);

/**
 * Builds a node pointer out of a physical record and a page number.
 * @param index index
 * @param rec record for which to build node pointer
 * @param page_no page number to put in node pointer
 * @param heap memory heap where pointer created
 * @param level level of rec in tree: 0 means leaf level
 * @return own: node pointer
 */
dtuple_t *dict_index_build_node_ptr(const dict_index_t *index, const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level);

/**
 * Copies an initial segment of a physical record, long enough to specify an index entry uniquely.
 * @param index index
 * @param rec record for which to copy prefix
 * @param n_fields number of fields copied
 * @param buf memory buffer for the copied prefix, or NULL.
 * @param buf_size buffer size
 * @return pointer to the prefix record
 */
rec_t *dict_index_copy_rec_order_prefix(const dict_index_t *index, const rec_t *rec, ulint *n_fields, byte *&buf, ulint &buf_size);

/**
 * Builds a typed data tuple out of a physical record.
 * @param index index
 * @param rec record for which to build data tuple
 * @param n_fields number of data fields
 * @param heap memory heap where tuple created
 * @return own: data tuple
 */
dtuple_t *dict_index_build_data_tuple(dict_index_t *index, rec_t *rec, ulint n_fields, mem_heap_t *heap);

/**
 * Calculates the minimum record length in an index.
 * @param index index
 * @return minimum record length
 */
ulint dict_index_calc_min_rec_len(const dict_index_t *index);

/**
 * Calculates new estimates for table and index statistics. The statistics are used in query optimization.
 * @param table table
 * @param has_dict_mutex true if the caller has the dictionary mutex
 */
void dict_update_statistics_low(dict_table_t *table, bool has_dict_mutex);

/**
 * Calculates new estimates for table and index statistics. The statistics are used in query optimization.
 * @param table table
 */
void dict_update_statistics(dict_table_t *table);

/**
 * Reserves the dictionary system mutex.
 */
void dict_mutex_enter();

/**
 * Releases the dictionary system mutex.
 */
void dict_mutex_exit();

/**
 * Lock the appropriate mutex to protect index->stat_n_diff_key_vals[].
 * index->id is used to pick the right mutex and it should not change before dict_index_stat_mutex_exit() is called on this index.
 * @param index index
 */
void dict_index_stat_mutex_enter(const dict_index_t *index);

/**
 * Unlock the appropriate mutex that protects index->stat_n_diff_key_vals[].
 * @param index index
 */
void dict_index_stat_mutex_exit(const dict_index_t *index);

/**
 * Checks if the database name in two table names is the same.
 * @param name1 table name in the form dbname '/' tablename
 * @param name2 table name in the form dbname '/' tablename
 * @return true if same db name
 */
bool dict_tables_have_same_db(const char *name1, const char *name2);

/**
 * Get index by name
 * @param table table
 * @param name name of the index to find
 * @return index, NULL if does not exist
 */
dict_index_t *dict_table_get_index_on_name(dict_table_t *table, const char *name);

/**
 * In case there is more than one index with the same name return the index with the min(id).
 * @param table table
 * @param name name of the index to find
 * @return index, NULL if does not exist
 */
dict_index_t *dict_table_get_index_on_name_and_min_id(dict_table_t *table, const char *name);

/**
 * Locks the data dictionary exclusively for performing a table create or other data dictionary modification operation.
 * @param trx transaction
 */
void dict_lock_data_dictionary(trx_t *trx);

/**
 * Unlocks the data dictionary exclusive lock.
 * @param trx transaction
 */
void dict_unlock_data_dictionary(trx_t *trx);

/**
 * Locks the data dictionary in shared mode from modifications,
 * for performing foreign key check, rollback, or other operation
 * invisible to users.
 * @param trx transaction
 */
void dict_freeze_data_dictionary(trx_t *trx);

/**
 * Unlocks the data dictionary shared lock.
 * @param trx transaction
 */
void dict_unfreeze_data_dictionary(trx_t *trx);

/** Reset dict variables. */
void dict_var_init();

/**
 * Gets the column data type.
 * @param col column
 * @param type data type
 */
inline void dict_col_copy_type(const dict_col_t *col, dtype_t *type) {
  ut_ad(col && type);

  type->mtype = col->mtype;
  type->prtype = col->prtype;
  type->len = col->len;
  type->mbminlen = col->mbminlen;
  type->mbmaxlen = col->mbmaxlen;
}

#ifdef UNIV_DEBUG
/**
 * Assert that a column and a data type match.
 * @param col column
 * @param type data type
 * @return true
 */
inline bool dict_col_type_assert_equal(const dict_col_t *col, const dtype_t *type) {
  ut_ad(col->mtype == type->mtype);
  ut_ad(col->prtype == type->prtype);
  ut_ad(col->len == type->len);
  ut_ad(col->mbminlen == type->mbminlen);
  ut_ad(col->mbmaxlen == type->mbmaxlen);

  return true;
}
#endif /* UNIV_DEBUG */

/**
 * Returns the minimum size of the column.
 * @param col column
 * @return minimum size
 */
inline ulint dict_col_get_min_size(const dict_col_t *col) {
  return dtype_get_min_size_low(col->mtype, col->prtype, col->len, col->mbminlen, col->mbmaxlen);
}

/**
 * Returns the maximum size of the column.
 * @param col column
 * @return maximum size
 */
inline ulint dict_col_get_max_size(const dict_col_t *col) {
  return dtype_get_max_size_low(col->mtype, col->len);
}

/**
 * Returns the size of a fixed size column, 0 if not a fixed size column.
 * @param col column
 * @param comp nonzero=ROW_FORMAT=COMPACT
 * @return fixed size, or 0
 */
inline ulint dict_col_get_fixed_size(const dict_col_t *col, ulint comp) {
  return dtype_get_fixed_size_low(col->mtype, col->prtype, col->len, col->mbminlen, col->mbmaxlen, comp);
}

/**
 * Returns the ROW_FORMAT=REDUNDANT stored SQL NULL size of a column.
 * For fixed length types it is the fixed length of the type, otherwise 0.
 * @param col column
 * @param comp nonzero=ROW_FORMAT=COMPACT
 * @return SQL null storage size in ROW_FORMAT=REDUNDANT
 */
inline ulint dict_col_get_sql_null_size(const dict_col_t *col, ulint comp) {
  return dict_col_get_fixed_size(col, comp);
}

/**
 * Gets the column number.
 * @param col column
 * @return col->ind, table column position (starting from 0)
 */
inline ulint dict_col_get_no(const dict_col_t *col) {
  return col->ind;
}

/**
 * Check whether the index is the clustered index.
 * @return nonzero for clustered index, zero for other indexes
 */
inline bool dict_index_is_clust(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return unlikely(dict_index->type & DICT_CLUSTERED) > 0;
}

/**
 * Gets the column position in the clustered index.
 *
 * @param col table column
 * @param clust_index clustered index
 * @return column position in the clustered index
 */
inline ulint dict_col_get_clust_pos(const dict_col_t *col, const dict_index_t *clust_index) {
  ut_ad(dict_index_is_clust(clust_index));

  for (ulint i = 0; i < clust_index->n_def; i++) {
    const auto field = &clust_index->fields[i];

    if (!field->prefix_len && field->col == col) {
      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * Check whether the index is unique.
 * @return nonzero for unique index, zero for other indexes
 */
inline ulint dict_index_is_unique(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return unlikely(dict_index->type & DICT_UNIQUE);
}

/**
 * Check whether the index is a secondary index tree.
 * @return nonzero zero for other indexes
 */
inline bool dict_index_is_sec(const dict_index_t *dict_index) {
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return likely(!(dict_index->type & DICT_CLUSTERED));
}

/**
 * Gets the number of user-defined columns in a table in the dictionary cache.
 * @return number of user-defined (e.g., not ROW_ID) columns of a table
 */
inline ulint dict_table_get_n_user_cols(const dict_table_t *table) {
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return table->n_cols - DATA_N_SYS_COLS;
}

/**
 * Gets the number of system columns in a table in the dictionary cache.
 * @param table table
 * @return number of system (e.g., ROW_ID) columns of a table
 */
inline ulint dict_table_get_n_sys_cols(const dict_table_t *IF_DEBUG(table)) {
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(table->cached);

  return DATA_N_SYS_COLS;
}

/**
 * Gets the number of all columns (also system) in a table in the dictionary cache.
 * @param table table
 * @return number of columns of a table
 */
inline ulint dict_table_get_n_cols(const dict_table_t *table) {
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return table->n_cols;
}

/** Gets the nth column of a table.
 * @param table in: table
 * @param pos in: position of column
 * @return pointer to column object */
inline dict_col_t *dict_table_get_nth_col(const dict_table_t *table, ulint pos) {
  ut_ad(pos < table->n_def);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return const_cast<dict_col_t *>(table->cols) + pos;
}

/** Gets the given system column of a table.
 * @param table in: table
 * @param sys in: DATA_ROW_ID, ...
 * @return pointer to column object */
inline dict_col_t *dict_table_get_sys_col(const dict_table_t *table, ulint sys) {
  ut_ad(sys < DATA_N_SYS_COLS);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  auto col = dict_table_get_nth_col(table, table->n_cols - DATA_N_SYS_COLS + sys);
  ut_ad(col->mtype == DATA_SYS);
  ut_ad(col->prtype == (sys | DATA_NOT_NULL));

  return col;
}

/** Gets the given system column number of a table.
 * @param table in: table
 * @param sys in: DATA_ROW_ID, ...
 * @return column number */
inline ulint dict_table_get_sys_col_no(const dict_table_t *table, ulint sys) {
  ut_ad(sys < DATA_N_SYS_COLS);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return table->n_cols - DATA_N_SYS_COLS + sys;
}

/** Check whether the table uses the compact page format.
 * @param table in: table
 * @return true if table uses the compact page format */
inline bool dict_table_is_comp(const dict_table_t *table) {
  ut_a(DICT_TF_COMPACT);

  return likely(table->flags & DICT_TF_COMPACT);
}

/** Determine the file format of a table.
 * @param table in: table
 * @return file format version */
inline ulint dict_table_get_format(const dict_table_t *table) {
  return (table->flags & DICT_TF_FORMAT_MASK) >> DICT_TF_FORMAT_SHIFT;
}

/** Determine the file format of a table.
 * @param table in/out: table
 * @param format in: file format version */
inline void dict_table_set_format(dict_table_t *table, ulint format) {
  table->flags = (table->flags & ~DICT_TF_FORMAT_MASK) | (format << DICT_TF_FORMAT_SHIFT);
}

/** Gets the number of fields in the internal representation of an index,
 * including fields added by the dictionary system.
 * @param dict_index in: an internal representation of index (in the dictionary cache)
 * @return number of fields */
inline ulint dict_index_get_n_fields(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return dict_index->n_fields;
}

/** Gets the number of fields in the internal representation of an index
 * that uniquely determine the position of an index entry in the index, if
 * we do not take multiversioning into account: in the B-tree use the value
 * returned by dict_index_get_n_unique_in_tree.
 * @param dict_index in: an internal representation of index (in the dictionary cache)
 * @return number of fields */
inline ulint dict_index_get_n_unique(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(dict_index->cached == 1);

  return dict_index->n_uniq;
}

/** Gets the number of fields in the internal representation of an index
 * which uniquely determine the position of an index entry in the index, if
 * we also take multiversioning into account.
 * @param dict_index in: an internal representation of index (in the dictionary cache)
 * @return number of fields */
inline ulint dict_index_get_n_unique_in_tree(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(dict_index->cached);

  if (dict_index_is_clust(dict_index)) {
    return dict_index_get_n_unique(dict_index);
  } else {
    return dict_index_get_n_fields(dict_index);
  }
}

/** Gets the number of user-defined ordering fields in the index. In the
 * internal representation of clustered indexes we add the row id to the ordering
 * fields to make a clustered index unique, but this function returns the number of
 * fields the user defined in the index as ordering fields.
 * @param dict_index in: an internal representation of index (in the dictionary cache)
 * @return number of fields */
inline ulint dict_index_get_n_ordering_defined_by_user(const dict_index_t *dict_index) {
  return dict_index->n_user_defined_cols;
}

/** Gets the nth field of an index.
 * @param index in: index
 * @param pos in: position of field
 * @return pointer to field object */
inline dict_field_t *dict_index_get_nth_field(const dict_index_t *index, ulint pos) {
  ut_ad(pos < index->n_def);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  return const_cast<dict_field_t *>(&index->fields[pos]);
}

/** Returns the position of a system column in an index.
 * @param dict_index in: index
 * @param type in: DATA_ROW_ID, ...
 * @return position, ULINT_UNDEFINED if not contained */
inline ulint dict_index_get_sys_col_pos(const dict_index_t *dict_index, ulint type) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  if (dict_index_is_clust(dict_index)) {
    return dict_col_get_clust_pos(dict_table_get_sys_col(dict_index->table, type), dict_index);
  } else {
    return dict_index_get_nth_col_pos(dict_index, dict_table_get_sys_col_no(dict_index->table, type));
  }
}

/** Gets the field column.
 * @param field in: index field
 * @return field->col, pointer to the table column */
inline const dict_col_t *dict_field_get_col(const dict_field_t *field) {
  return field->col;
}

/** Gets pointer to the nth column in an index.
 * @param dict_index in: index
 * @param pos in: position of the field
 * @return column */
inline const dict_col_t *dict_index_get_nth_col(const dict_index_t *dict_index, ulint pos) {
  return dict_field_get_col(dict_index_get_nth_field(dict_index, pos));
}

/** Gets the column number the nth field in an index.
 * @param dict_index in: index
 * @param pos in: position of the field
 * @return column number */
inline ulint dict_index_get_nth_col_no(const dict_index_t *dict_index, ulint pos) {
  return dict_col_get_no(dict_index_get_nth_col(dict_index, pos));
}

/** Returns the minimum data size of an index record.
 * @param dict_index in: dict_index
 * @return minimum data size in bytes */
inline ulint dict_index_get_min_size(const dict_index_t *dict_index) {
  ulint size = 0;
  ulint n = dict_index_get_n_fields(dict_index);

  while (n--) {
    size += dict_col_get_min_size(dict_index_get_nth_col(dict_index, n));
  }

  return size;
}

/** Gets the space id of the root of the index tree.
 * @param dict_index in: dict_index
 * @return space id */
inline ulint dict_index_get_space(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (dict_index->space);
}

/** Sets the space id of the root of the index tree.
 * @param dict_index in/out: dict_index
 * @param space in: space id */
inline void dict_index_set_space(dict_index_t *dict_index, ulint space) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  dict_index->space = space;
}

/** Gets the page number of the root of the index tree.
 * @param dict_index in: dict_index
 * @return page number */
inline ulint dict_index_get_page(const dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return dict_index->page;
}

/** Sets the page number of the root of index tree.
 * @param dict_index in/out: dict_index
 * @param page in: page number */
inline void dict_index_set_page(dict_index_t *dict_index, ulint page) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  dict_index->page = page;
}

/** Gets the read-write lock of the index tree.
 * @param dict_index in: dict_index
 * @return read-write lock */
inline rw_lock_t *dict_index_get_lock(dict_index_t *dict_index) {
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return &dict_index->lock;
}

/** Returns free space reserved for future updates of records. This is
relevant only in the case of many consecutive inserts, as updates
which make the records bigger might fragment the index.
@return	number of free bytes on page, reserved for updates */
inline ulint dict_index_get_space_reserve() {
  return UNIV_PAGE_SIZE / 16;
}

/**
 * @brief Checks if a table is in the dictionary cache.
 * @param table_name in: table name
 * @return table, NULL if not found
 */
inline dict_table_t *dict_table_check_if_in_cache_low(const char *table_name) {
  ut_ad(table_name);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  /* Look for the table name in the hash table */
  dict_table_t *table{dict_sys->table_lookup->get(table_name)};

  return table;
}

/**
 * @brief Gets a table; loads it to the dictionary cache if necessary. A low-level function.
 * @param table_name in: table name
 * @return table, NULL if not found
 */
inline dict_table_t *dict_table_get_low(const char *table_name) {
  ut_ad(table_name != nullptr);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  auto table = dict_table_check_if_in_cache_low(table_name);

  if (table == nullptr) {
    // FIXME: srv_force_recovery should be passed in as an arg
    table = dict_load_table(static_cast<ib_recovery_t>(srv_force_recovery), table_name);
  }

  ut_ad(table == nullptr || table->cached);

  return table;
}

/**
 * @brief Returns a table object based on table id.
 * @param recovery in: recovery flag
 * @param table_id in: table id
 * @return table, NULL if does not exist
 */
inline dict_table_t *dict_table_get_on_id_low(ib_recovery_t recovery, uint64_t table_id) {
  ut_ad(mutex_own(&dict_sys->mutex));

  /* Look for the table name in the hash table */
  dict_table_t *table{dict_sys->table_lookup->get(table_id)};

  if (table == nullptr) {
    table = dict_load_table_on_id(recovery, table_id);
  }

  ut_ad(table == nullptr || table->cached);

  return table;
}

/** Gets the first index on the table (the clustered index).
 * @param[in] table  Get the first index for this table
@return        index, nullptr if none exists */
inline dict_index_t *dict_table_get_first_index(const dict_table_t *table) {
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return UT_LIST_GET_FIRST(((dict_table_t *)table)->indexes);
}

/** Gets the next index on the table.
 * @param[in] table  Get index after this one
@return        index, nullptr if none left */
inline dict_index_t *dict_table_get_next_index(const dict_index_t *index) {
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  return UT_LIST_GET_NEXT(indexes, (dict_index_t *)index);
}
