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

/** Makes all characters in a NUL-terminated UTF-8 string lower case. */

void dict_casedn_str(char *a); /*!< in/out: string to put in lower case */
/** Get the database name length in a table name.
@return	database name length */
ulint dict_get_db_name_len(const char *name); /*!< in: table name in the form
                                              dbname '/' tablename */
/** Return the end of table name where we have removed dbname and '/'.
@return	table name */

const char *dict_remove_db_name(const char *name); /*!< in: table name in the
                                                   form dbname '/' tablename */
/** Returns a table object based on table id.
@return	table, NULL if does not exist */

dict_table_t *dict_table_get_on_id(
  ib_recovery_t recovery, /*!< in: recovery flag */
  uint64_t table_id,      /*!< in: table id */
  trx_t *trx
); /*!< in: transaction handle */
/** Decrements the count of open handles to a table. */

void dict_table_decrement_handle_count(
  dict_table_t *table, /*!< in/out: table */
  bool dict_locked
); /*!< in: true=data dictionary locked */
/** Increments the count of open client handles to a table. */

void dict_table_increment_handle_count(
  dict_table_t *table, /*!< in/out: table */
  bool dict_locked
); /*!< in: true=data dictionary locked */
/** Inits the data dictionary module. */

void dict_init(void);
/** Closes the data dictionary module. */

void dict_close(void);
/** Gets the column data type. */
inline void dict_col_copy_type(
  const dict_col_t *col, /*!< in: column */
  dtype_t *type
); /*!< out: data type */

#ifdef UNIV_DEBUG
/** Assert that a column and a data type match.
@return	true */
inline bool dict_col_type_assert_equal(
  const dict_col_t *col, /*!< in: column */
  const dtype_t *type
);     /*!< in: data type */
#endif /* UNIV_DEBUG */

/** Returns the minimum size of the column.
@return	minimum size */
inline ulint dict_col_get_min_size(const dict_col_t *col); /*!< in: column */

/** Returns the maximum size of the column.
@return	maximum size */
inline ulint dict_col_get_max_size(const dict_col_t *col); /*!< in: column */

/** Returns the size of a fixed size column, 0 if not a fixed size column.
@return	fixed size, or 0 */
inline ulint dict_col_get_fixed_size(
  const dict_col_t *col, /*!< in: column */
  ulint comp
); /*!< in: nonzero=ROW_FORMAT=COMPACT  */

/** Returns the ROW_FORMAT=REDUNDANT stored SQL NULL size of a column.
For fixed length types it is the fixed length of the type, otherwise 0.
@return	SQL null storage size in ROW_FORMAT=REDUNDANT */
inline ulint dict_col_get_sql_null_size(
  const dict_col_t *col, /*!< in: column */
  ulint comp
); /*!< in: nonzero=ROW_FORMAT=COMPACT  */

/** Gets the column number.
@return	col->ind, table column position (starting from 0) */
inline ulint dict_col_get_no(const dict_col_t *col); /*!< in: column */

/** Gets the column position in the clustered index. */
inline ulint dict_col_get_clust_pos(
  const dict_col_t *col, /*!< in: table column */
  const dict_index_t *clust_index
); /*!< in: clustered index */
/** If the given column name is reserved for InnoDB system columns, return
true.
@return	true if name is reserved */

bool dict_col_name_is_reserved(const char *name); /*!< in: column name */

/** Adds system columns to a table object. */
void dict_table_add_system_columns(
  dict_table_t *table, /*!< in/out: table */
  mem_heap_t *heap
); /*!< in: temporary heap */

/** Adds a table object to the dictionary cache. */
void dict_table_add_to_cache(
  dict_table_t *table, /*!< in: table */
  mem_heap_t *heap
); /*!< in: temporary heap */

/** Removes a table object from the dictionary cache. */
void dict_table_remove_from_cache(dict_table_t *table); /*!< in, own: table */

/** Renames a table object.
@return	true if success */
bool dict_table_rename_in_cache(
  dict_table_t *table,  /*!< in/out: table */
  const char *new_name, /*!< in: new name */
  bool rename_also_foreigns
); /*!< in: in ALTER TABLE we want
                           to preserve the original table name
                           in constraints which reference it */

/** Removes an index from the dictionary cache. */
void dict_index_remove_from_cache(
  dict_table_t *table, /*!< in/out: table */
  dict_index_t *index
); /*!< in, own: index */

/** Change the id of a table object in the dictionary cache. This is used in
DISCARD TABLESPACE. */

void dict_table_change_id_in_cache(
  dict_table_t *table, /*!< in/out: table object already in cache */
  uint64_t new_id
); /*!< in: new id to set */

/** Adds a foreign key constraint object to the dictionary cache. May free
the object if there already is an object with the same identifier in.
At least one of foreign table or referenced table must already be in
the dictionary cache!
@return	DB_SUCCESS or error code */
db_err dict_foreign_add_to_cache(
  dict_foreign_t *foreign, /*!< in, own: foreign key constraint */
  bool check_charsets
); /*!< in: true=check charset
                              compatibility */

/** Check if the index is referenced by a foreign key, if true return the
matching instance NULL otherwise.
@return pointer to foreign key struct if index is defined for foreign
key, otherwise NULL */
dict_foreign_t *dict_table_get_referenced_constraint(
  dict_table_t *table, /*!< in: InnoDB table */
  dict_index_t *index
); /*!< in: InnoDB index */

/** Checks if a table is referenced by foreign keys.
@return	true if table is referenced by a foreign key */
bool dict_table_is_referenced_by_foreign_key(const dict_table_t *table); /*!< in: InnoDB table */

/** Replace the index in the foreign key list that matches this index's
definition with an equivalent index. */
void dict_table_replace_index_in_foreign_list(
  dict_table_t *table, /*!< in/out: table */
  dict_index_t *index
); /*!< in: index to be replaced */

/** Checks if a index is defined for a foreign key constraint. Index is a part
of a foreign key constraint if the index is referenced by foreign key
or index is a foreign key index
@return pointer to foreign key struct if index is defined for foreign
key, otherwise NULL */
dict_foreign_t *dict_table_get_foreign_constraint(
  dict_table_t *table, /*!< in: InnoDB table */
  dict_index_t *index
); /*!< in: InnoDB index */

/** Scans a table create SQL string and adds to the data dictionary
the foreign key constraints declared in the string. This function
should be called after the indexes for a table have been created.
Each foreign key constraint must be accompanied with indexes in
bot participating tables. The indexes are allowed to contain more
fields than mentioned in the constraint.
@return	error code or DB_SUCCESS */
db_err dict_create_foreign_constraints(
  trx_t *trx,             /*!< in: transaction */
  const char *sql_string, /*!< in: table create statement where
                            foreign keys are declared like:
                            FOREIGN KEY (a, b) REFERENCES
                            table2(c, d), table2 can be written
                            also with the database
                            name before it: test.table2; the
                            default database id the database of
                            parameter name */
  const char *name,       /*!< in: table full name in the
                            normalized form
                            database_name/table_name */
  bool reject_fks
); /*!< in: if true, fail with error
                             code DB_CANNOT_ADD_CONSTRAINT if
                             any foreign keys are found. */

/** Parses the CONSTRAINT id's to be dropped in an ALTER TABLE statement.
@return DB_SUCCESS or DB_CANNOT_DROP_CONSTRAINT if syntax error or the
constraint id does not match */
db_err dict_foreign_parse_drop_constraints(
  mem_heap_t *heap,    /*!< in: heap from which we can
                                        allocate memory */
  trx_t *trx,          /*!< in: transaction */
  dict_table_t *table, /*!< in: table */
  ulint *n,            /*!< out: number of constraints
                                        to drop */
  const char ***constraints_to_drop
); /*!< out: id's of the
                                        constraints to drop */

/** Returns a table object and optionally increment its open handle count.
NOTE! This is a high-level function to be used mainly from outside the
'dict' directory. Inside this directory dict_table_get_low is usually the
appropriate function.
@return	table, NULL if does not exist */
dict_table_t *dict_table_get(
  const char *table_name, /*!< in: table name */
  bool inc_count
); /*!< in: whether to increment the
                                               open handle count on the table */

/** Returns a table instance based on table id.
@return	table, NULL if does not exist */
dict_table_t *dict_table_get_using_id(
  ib_recovery_t recovery, /*!< in: recovery flag */
  uint64_t table_id,      /*!< in: table id */
  bool ref_count
); /*!< in: increment open handle count if true */

/** Returns a index object, based on table and index id, and memoryfixes it.
@return	index, NULL if does not exist */
dict_index_t *dict_index_get_on_id_low(
  dict_table_t *table, /*!< in: table */
  uint64_t index_id
); /*!< in: index id */

/** Checks if a table is in the dictionary cache.
@return	table, NULL if not found */
inline dict_table_t *dict_table_check_if_in_cache_low(const char *table_name); /*!< in: table name */

/** Gets a table; loads it to the dictionary cache if necessary. A low-level
function.
@return	table, NULL if not found */
inline dict_table_t *dict_table_get_low(const char *table_name); /*!< in: table name */

/** Returns a table object based on table id.
@return	table, NULL if does not exist */
inline dict_table_t *dict_table_get_on_id_low(
  ib_recovery_t recovery, /*!< in: recovery flag */
  uint64_t table_id
); /*!< in: table id */

/** Find an index that is equivalent to the one passed in and is not marked
for deletion.
@return	index equivalent to foreign->foreign_index, or NULL */
dict_index_t *dict_foreign_find_equiv_index(dict_foreign_t *foreign); /*!< in: foreign key */

/** Returns an index object by matching on the name and column names and
if more than one index matches return the index with the max id
@return	matching index, NULL if not found */
dict_index_t *dict_table_get_index_by_max_id(
  dict_table_t *table,  /*!< in: table */
  const char *name,     /*!< in: the index name to find */
  const char **columns, /*!< in: array of column names */
  ulint n_cols
); /*!< in: number of columns */

/** Returns a column's name.
@return column name. NOTE: not guaranteed to stay valid if table is
modified in any way (columns added, etc.). */
const char *dict_table_get_col_name(
  const dict_table_t *table, /*!< in: table */
  ulint col_nr
); /*!< in: column number */

/** Returns a column's ordinal value.
@return	column pos. -1 if not found. NOTE: not guaranteed to stay valid
if table is modified in any way (columns added, etc.). */
int dict_table_get_col_no(
  const dict_table_t *table, /*!< in: table */
  const char *name
); /*!< in: column name */

/** Prints a table definition. */
void dict_table_print(dict_table_t *table); /*!< in: table */

/** Prints a table data. */
void dict_table_print_low(dict_table_t *table); /*!< in: table */

/** Prints a table data when we know the table name. */
void dict_table_print_by_name(const char *name); /*!< in: table name */

/** Outputs info on foreign keys of a table. */
void dict_print_info_on_foreign_keys(
  bool create_table_format,
  /*!< in: if true then print in
    a format suitable to be inserted into
    a CREATE TABLE, otherwise in the format
    of SHOW TABLE STATUS */
  ib_stream_t ib_stream, /*!< in: stream where to print */
  trx_t *trx,            /*!< in: transaction */
  dict_table_t *table
); /*!< in: table */

/** Outputs info on a foreign key of a table in a format suitable for
CREATE TABLE. */
void dict_print_info_on_foreign_key_in_create_format(
  ib_stream_t stream,      /*!< in: file where to print */
  trx_t *trx,              /*!< in: transaction */
  dict_foreign_t *foreign, /*!< in: foreign key constraint */
  bool add_newline
); /*!< in: whether to add a newline */

/** Displays the names of the index and the table. */
void dict_index_name_print(
  ib_stream_t stream, /*!< in: output stream */
  trx_t *trx,         /*!< in: transaction */
  const dict_index_t *index
); /*!< in: index to print */

#ifdef UNIV_DEBUG
/** Gets the first index on the table (the clustered index).
@return	index, NULL if none exists */
inline dict_index_t *dict_table_get_first_index(const dict_table_t *table); /*!< in: table */

/** Gets the next index on the table.
@return	index, NULL if none left */
inline dict_index_t *dict_table_get_next_index(const dict_index_t *index); /*!< in: index */
#else                                                                      /* UNIV_DEBUG */
#define dict_table_get_first_index(table) UT_LIST_GET_FIRST((table)->indexes)
#define dict_table_get_next_index(index) UT_LIST_GET_NEXT(indexes, index)
#endif /* UNIV_DEBUG */

/** Check whether the index is the clustered index.
@return	nonzero for clustered index, zero for other indexes */
inline ulint dict_index_is_clust(const dict_index_t *index) /*!< in: index */
  __attribute__((pure));

/** Check whether the index is unique.
@return	nonzero for unique index, zero for other indexes */
inline ulint dict_index_is_unique(const dict_index_t *index) /*!< in: index */
  __attribute__((pure));

/** Check whether the index is a secondary index tree.
@return	true if it's a secondary index. */
inline bool dict_index_is_sec(const dict_index_t *index) /*!< in: index */
  __attribute__((pure));

/** Gets the number of user-defined columns in a table in the dictionary
cache.
@return	number of user-defined (e.g., not ROW_ID) columns of a table */
inline ulint dict_table_get_n_user_cols(const dict_table_t *table); /*!< in: table */

/** Gets the number of system columns in a table in the dictionary cache.
@return	number of system (e.g., ROW_ID) columns of a table */
inline ulint dict_table_get_n_sys_cols(const dict_table_t *table); /*!< in: table */

/** Gets the number of all columns (also system) in a table in the dictionary
cache.
@return	number of columns of a table */
inline ulint dict_table_get_n_cols(const dict_table_t *table); /*!< in: table */

#ifdef UNIV_DEBUG
/** Gets the nth column of a table.
@return	pointer to column object */
inline dict_col_t *dict_table_get_nth_col(
  const dict_table_t *table, /*!< in: table */
  ulint pos
); /*!< in: position of column */

/** Gets the given system column of a table.
@return	pointer to column object */
inline dict_col_t *dict_table_get_sys_col(
  const dict_table_t *table, /*!< in: table */
  ulint sys
);    /*!< in: DATA_ROW_ID, ... */
#else /* UNIV_DEBUG */
#define dict_table_get_nth_col(table, pos) ((table)->cols + (pos))
#define dict_table_get_sys_col(table, sys) ((table)->cols + (table)->n_cols + (sys)-DATA_N_SYS_COLS)
#endif /* UNIV_DEBUG */

/** Gets the given system column number of a table.
@return	column number */
inline ulint dict_table_get_sys_col_no(
  const dict_table_t *table, /*!< in: table */
  ulint sys
); /*!< in: DATA_ROW_ID, ... */

/** Returns the minimum data size of an index record.
@return	minimum data size in bytes */
inline ulint dict_index_get_min_size(const dict_index_t *index); /*!< in: index */

/** Check whether the table uses the compact page format.
@return	true if table uses the compact page format */
inline bool dict_table_is_comp(const dict_table_t *table); /*!< in: table */

/** Determine the file format of a table.
@return	file format version */
inline ulint dict_table_get_format(const dict_table_t *table); /*!< in: table */

/** Set the file format of a table. */
inline void dict_table_set_format(
  dict_table_t *table, /*!< in/out: table */
  ulint format
); /*!< in: file format version */

/** Checks if a column is in the ordering columns of the clustered index of a
table. Column prefixes are treated like whole columns.
@return	true if the column, or its prefix, is in the clustered key */
bool dict_table_col_in_clustered_key(
  const dict_table_t *table, /*!< in: table */
  ulint n
); /*!< in: column number */

/** Copies types of columns contained in table to tuple and sets all
fields of the tuple to the SQL NULL value.  This function should
be called right after dtuple_create(). */

void dict_table_copy_types(
  dtuple_t *tuple, /*!< in/out: data tuple */
  const dict_table_t *table
); /*!< in: table */

/** Looks for an index with the given id. NOTE that we do not reserve
the dictionary mutex: this function is for emergency purposes like
printing info of a corrupt database page!
@return	index or NULL if not found from cache */
dict_index_t *dict_index_find_on_id_low(uint64_t id); /*!< in: index id */

/** Adds an index to the dictionary cache.
@return	DB_SUCCESS, DB_TOO_BIG_RECORD, or DB_CORRUPTION */
db_err dict_index_add_to_cache(
  dict_table_t *table, /*!< in: table on which the index is */
  dict_index_t *index, /*!< in, own: index; NOTE! The index memory
                         object is freed in this function! */
  ulint page_no,       /*!< in: root page number of the index */
  bool strict
); /*!< in: true=refuse to create the index
                          if records could be too big to fit in
                          an B-tree page */

/** Gets the number of fields in the internal representation of an index,
including fields added by the dictionary system.
@return	number of fields */
inline ulint dict_index_get_n_fields(const dict_index_t *index); /*!< in: an internal
                                                    representation of index (in
                                                    the dictionary cache) */

/** Gets the number of fields in the internal representation of an index
that uniquely determine the position of an index entry in the index, if
we do not take multiversioning into account: in the B-tree use the value
returned by dict_index_get_n_unique_in_tree.
@return	number of fields */
inline ulint dict_index_get_n_unique(const dict_index_t *index); /*!< in: an internal representation
                                of index (in the dictionary cache) */

/** Gets the number of fields in the internal representation of an index
which uniquely determine the position of an index entry in the index, if
we also take multiversioning into account.
@return	number of fields */
inline ulint dict_index_get_n_unique_in_tree(const dict_index_t *index); /*!< in: an internal representation
                                of index (in the dictionary cache) */

/** Gets the number of user-defined ordering fields in the index. In the
internal representation we add the row id to the ordering fields to make all
indexes unique, but this function returns the number of fields the user defined
in the index as ordering fields.
@return	number of fields */
inline ulint dict_index_get_n_ordering_defined_by_user(const dict_index_t *index); /*!< in: an internal representation
                                of index (in the dictionary cache) */

#ifdef UNIV_DEBUG
/** Gets the nth field of an index.
@return	pointer to field object */
inline dict_field_t *dict_index_get_nth_field(
  const dict_index_t *index, /*!< in: index */
  ulint pos
);    /*!< in: position of field */
#else /* UNIV_DEBUG */
#define dict_index_get_nth_field(index, pos) ((index)->fields + (pos))
#endif /* UNIV_DEBUG */

/** Gets pointer to the nth column in an index.
@return	column */
inline const dict_col_t *dict_index_get_nth_col(
  const dict_index_t *index, /*!< in: index */
  ulint pos
); /*!< in: position of the field */

/** Gets the column number of the nth field in an index.
@return	column number */
inline ulint dict_index_get_nth_col_no(
  const dict_index_t *index, /*!< in: index */
  ulint pos
); /*!< in: position of the field */

/** Looks for column n in an index.
@return position in internal representation of the index;
ULINT_UNDEFINED if not contained */
ulint dict_index_get_nth_col_pos(
  const dict_index_t *index, /*!< in: index */
  ulint n
); /*!< in: column number */

/** Returns true if the index contains a column or a prefix of that column.
@return	true if contains the column or its prefix */
bool dict_index_contains_col_or_prefix(
  const dict_index_t *index, /*!< in: index */
  ulint n
); /*!< in: column number */

/** Looks for a matching field in an index. The column has to be the same. The
column in index must be complete, or must contain a prefix longer than the
column in index2. That is, we must be able to construct the prefix in index2
from the prefix in index.
@return position in internal representation of the index;
ULINT_UNDEFINED if not contained */
ulint dict_index_get_nth_field_pos(
  const dict_index_t *index,  /*!< in: index from which to search */
  const dict_index_t *index2, /*!< in: index */
  ulint n
); /*!< in: field number in index2 */

/** Looks for column n position in the clustered index.
@return	position in internal representation of the clustered index */
ulint dict_table_get_nth_col_pos(
  const dict_table_t *table, /*!< in: table */
  ulint n
); /*!< in: column number */

/** Returns the position of a system column in an index.
@return	position, ULINT_UNDEFINED if not contained */
inline ulint dict_index_get_sys_col_pos(
  const dict_index_t *index, /*!< in: index */
  ulint type
); /*!< in: DATA_ROW_ID, ... */

/** Adds a column to index. */
void dict_index_add_col(
  dict_index_t *index,       /*!< in/out: index */
  const dict_table_t *table, /*!< in: table */
  dict_col_t *col,           /*!< in: column */
  ulint prefix_len
); /*!< in: column prefix length */

/** Copies types of fields contained in index to tuple. */
void dict_index_copy_types(
  dtuple_t *tuple,           /*!< in/out: data tuple */
  const dict_index_t *index, /*!< in: index */
  ulint n_fields
); /*!< in: number of
                                                      field types to copy */

/** Gets the field column.
@return	field->col, pointer to the table column */
inline const dict_col_t *dict_field_get_col(const dict_field_t *field); /*!< in: index field */

/** Returns an index object if it is found in the dictionary cache.
Assumes that dict_sys->mutex is already being held.
@return	index, NULL if not found */
dict_index_t *dict_index_get_if_in_cache_low(uint64_t index_id); /*!< in: index id */

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
/** Returns an index object if it is found in the dictionary cache.
@return	index, NULL if not found */
dict_index_t *dict_index_get_if_in_cache(uint64_t index_id); /*!< in: index id */
#endif                                                       /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
/** Checks that a tuple has n_fields_cmp value in a sensible range, so that
no comparison can occur with the page number field in a node pointer.
@return	true if ok */
bool dict_index_check_search_tuple(
  const dict_index_t *index, /*!< in: index tree */
  const dtuple_t *tuple
); /*!< in: tuple used in a search */

#endif /* UNIV_DEBUG */

/** Builds a node pointer out of a physical record and a page number.
@return	own: node pointer */
dtuple_t *dict_index_build_node_ptr(
  const dict_index_t *index, /*!< in: index */
  const rec_t *rec,          /*!< in: record for which
                                                      to build node pointer */
  ulint page_no,             /*!< in: page number to put
                                                   in node pointer */
  mem_heap_t *heap,          /*!< in: memory heap where
                                                      pointer created */
  ulint level
); /*!< in: level of rec in tree:
                                                  0 means leaf level */

/** Copies an initial segment of a physical record, long enough to specify an
index entry uniquely.
@return	pointer to the prefix record */
rec_t *dict_index_copy_rec_order_prefix(
  const dict_index_t *index, /*!< in: index */
  const rec_t *rec,          /*!< in: record for which to
                               copy prefix */
  ulint *n_fields,           /*!< out: number of fields copied */
  byte **buf,                /*!< in/out: memory buffer for the
                               copied prefix, or NULL */
  ulint *buf_size
); /*!< in/out: buffer size */

/** Builds a typed data tuple out of a physical record.
@return	own: data tuple */
dtuple_t *dict_index_build_data_tuple(
  dict_index_t *index, /*!< in: index */
  rec_t *rec,          /*!< in: record for which to build data tuple */
  ulint n_fields,      /*!< in: number of data fields */
  mem_heap_t *heap
); /*!< in: memory heap where tuple created */

/** Gets the space id of the root of the index tree.
@return	space id */
inline ulint dict_index_get_space(const dict_index_t *index); /*!< in: index */

/** Sets the space id of the root of the index tree. */
inline void dict_index_set_space(
  dict_index_t *index, /*!< in/out: index */
  ulint space
); /*!< in: space id */

/** Gets the page number of the root of the index tree.
@return	page number */
inline ulint dict_index_get_page(const dict_index_t *tree); /*!< in: index */

/** Sets the page number of the root of index tree. */
inline void dict_index_set_page(
  dict_index_t *index, /*!< in/out: index */
  ulint page
); /*!< in: page number */

/** Gets the read-write lock of the index tree.
@return	read-write lock */
inline rw_lock_t *dict_index_get_lock(dict_index_t *index); /*!< in: index */

/** Returns free space reserved for future updates of records. This is
relevant only in the case of many consecutive inserts, as updates
which make the records bigger might fragment the index.
@return	number of free bytes on page, reserved for updates */
inline ulint dict_index_get_space_reserve();

/** Calculates the minimum record length in an index. */
ulint dict_index_calc_min_rec_len(const dict_index_t *index); /*!< in: index */

/** Calculates new estimates for table and index statistics. The statistics
are used in query optimization. */
void dict_update_statistics_low(
  dict_table_t *table, /*!< in/out: table */
  bool has_dict_mutex
); /*!< in: true if the caller has the
                           dictionary mutex */

/** Calculates new estimates for table and index statistics. The statistics
are used in query optimization. */
void dict_update_statistics(dict_table_t *table); /*!< in/out: table */

/** Reserves the dictionary system mutex. */
void dict_mutex_enter(void);

/** Releases the dictionary system mutex. */
void dict_mutex_exit(void);

/** Lock the appropriate mutex to protect index->stat_n_diff_key_vals[].
index->id is used to pick the right mutex and it should not change
before dict_index_stat_mutex_exit() is called on this index. */
void dict_index_stat_mutex_enter(const dict_index_t *index); /*!< in: index */

/** Unlock the appropriate mutex that protects index->stat_n_diff_key_vals[]. */
void dict_index_stat_mutex_exit(const dict_index_t *index); /*!< in: index */

/** Checks if the database name in two table names is the same.
@return	true if same db name */
bool dict_tables_have_same_db(
  const char *name1, /*!< in: table name in the
                                                   form  dbname '/' tablename */
  const char *name2
); /*!< in: table name in the
                                                  form dbname '/' tablename */

/** Get index by name
@return	index, NULL if does not exist */
dict_index_t *dict_table_get_index_on_name(
  dict_table_t *table, /*!< in: table */
  const char *name
); /*!< in: name of the index to find */

/** In case there is more than one index with the same name return the index
with the min(id).
@return	index, NULL if does not exist */
dict_index_t *dict_table_get_index_on_name_and_min_id(
  dict_table_t *table, /*!< in: table */
  const char *name
); /*!< in: name of the index to find */

/** Locks the data dictionary exclusively for performing a table create or other
data dictionary modification operation. */
void dict_lock_data_dictionary(trx_t *trx); /*!< in: transaction */

/** Unlocks the data dictionary exclusive lock. */
void dict_unlock_data_dictionary(trx_t *trx); /*!< in: transaction */

/** Locks the data dictionary in shared mode from modifications, for performing
foreign key check, rollback, or other operation invisible to users. */
void dict_freeze_data_dictionary(trx_t *trx); /*!< in: transaction */

/** Unlocks the data dictionary shared lock. */
void dict_unfreeze_data_dictionary(trx_t *trx); /*!< in: transaction */

/** Reset dict variables. */
void dict_var_init(void);

/* Buffers for storing detailed information about the latest foreign key
and unique key errors */
extern ib_stream_t dict_foreign_err_file;

extern mutex_t dict_foreign_err_mutex; /* mutex protecting the buffers */

/** the dictionary system */
extern dict_sys_t *dict_sys;

/** the data dictionary rw-latch protecting dict_sys */
extern rw_lock_t dict_operation_lock;

/* Dictionary system struct */
struct dict_sys_struct {
  mutex_t mutex;               /*!< mutex protecting the data
                               dictionary; protects also the
                               disk-based dictionary system tables;
                               this mutex serializes CREATE TABLE
                               and DROP TABLE, as well as reading
                               the dictionary data for a table from
                               system tables */
  uint64_t row_id;             /*!< the next row id to assign;
                               NOTE that at a checkpoint this
                               must be written to the dict system
                               header and flushed to a file; in
                               recovery this must be derived from
                               the log records */
  hash_table_t *table_hash;    /*!< hash table of the tables, based
                               on name */
  hash_table_t *table_id_hash; /*!< hash table of the tables, based
                               on id */
  UT_LIST_BASE_NODE_T(dict_table_t, table_LRU)
  table_LRU;                 /*!< LRU list of tables */
  ulint size;                /*!< varying space in bytes occupied
                             by the data dictionary table and
                             index objects */
  dict_table_t *sys_tables;  /*!< SYS_TABLES table */
  dict_table_t *sys_columns; /*!< SYS_COLUMNS table */
  dict_table_t *sys_indexes; /*!< SYS_INDEXES table */
  dict_table_t *sys_fields;  /*!< SYS_FIELDS table */
};

/** dummy index for ROW_FORMAT=REDUNDANT supremum and infimum records */
extern dict_index_t *dict_ind_redundant;

/** dummy index for ROW_FORMAT=COMPACT supremum and infimum records */
extern dict_index_t *dict_ind_compact;

/** Inits dict_ind_redundant and dict_ind_compact. */
void dict_ind_init();

/** Closes the data dictionary module. */
void dict_close(void);

#include "data0type.h"
#include "dict0load.h"
#include "rem0types.h"

/** Gets the column data type. */
inline void dict_col_copy_type(
  const dict_col_t *col, /*!< in: column */
  dtype_t *type
) /*!< out: data type */
{
  ut_ad(col && type);

  type->mtype = col->mtype;
  type->prtype = col->prtype;
  type->len = col->len;
  type->mbminlen = col->mbminlen;
  type->mbmaxlen = col->mbmaxlen;
}

#ifdef UNIV_DEBUG
/** Assert that a column and a data type match.
@return	true */
inline bool dict_col_type_assert_equal(
  const dict_col_t *col, /*!< in: column */
  const dtype_t *type
) /*!< in: data type */
{
  ut_ad(col);
  ut_ad(type);

  ut_ad(col->mtype == type->mtype);
  ut_ad(col->prtype == type->prtype);
  ut_ad(col->len == type->len);
  ut_ad(col->mbminlen == type->mbminlen);
  ut_ad(col->mbmaxlen == type->mbmaxlen);

  return (true);
}
#endif /* UNIV_DEBUG */

/** Returns the minimum size of the column.
@return	minimum size */
inline ulint dict_col_get_min_size(const dict_col_t *col) /*!< in: column */
{
  return (dtype_get_min_size_low(col->mtype, col->prtype, col->len, col->mbminlen, col->mbmaxlen));
}

/** Returns the maximum size of the column.
@return	maximum size */
inline ulint dict_col_get_max_size(const dict_col_t *col) /*!< in: column */
{
  return (dtype_get_max_size_low(col->mtype, col->len));
}

/** Returns the size of a fixed size column, 0 if not a fixed size column.
@return	fixed size, or 0 */
inline ulint dict_col_get_fixed_size(
  const dict_col_t *col, /*!< in: column */
  ulint comp
) /*!< in: nonzero=ROW_FORMAT=COMPACT */
{
  return (dtype_get_fixed_size_low(col->mtype, col->prtype, col->len, col->mbminlen, col->mbmaxlen, comp));
}

/** Returns the ROW_FORMAT=REDUNDANT stored SQL NULL size of a column.
For fixed length types it is the fixed length of the type, otherwise 0.
@return	SQL null storage size in ROW_FORMAT=REDUNDANT */
inline ulint dict_col_get_sql_null_size(
  const dict_col_t *col, /*!< in: column */
  ulint comp
) /*!< in: nonzero=ROW_FORMAT=COMPACT  */
{
  return (dict_col_get_fixed_size(col, comp));
}

/** Gets the column number.
@return	col->ind, table column position (starting from 0) */
inline ulint dict_col_get_no(const dict_col_t *col) /*!< in: column */
{
  ut_ad(col);

  return (col->ind);
}

/** Gets the column position in the clustered index. */
inline ulint dict_col_get_clust_pos(
  const dict_col_t *col, /*!< in: table column */
  const dict_index_t *clust_index
) /*!< in: clustered index */
{
  ulint i;

  ut_ad(col);
  ut_ad(clust_index);
  ut_ad(dict_index_is_clust(clust_index));

  for (i = 0; i < clust_index->n_def; i++) {
    const dict_field_t *field = &clust_index->fields[i];

    if (!field->prefix_len && field->col == col) {
      return (i);
    }
  }

  return (ULINT_UNDEFINED);
}

#ifdef UNIV_DEBUG
/** Gets the first index on the table (the clustered index).
@return	index, NULL if none exists */
inline dict_index_t *dict_table_get_first_index(const dict_table_t *table) /*!< in: table */
{
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return (UT_LIST_GET_FIRST(((dict_table_t *)table)->indexes));
}

/** Gets the next index on the table.
@return	index, NULL if none left */
inline dict_index_t *dict_table_get_next_index(const dict_index_t *index) /*!< in: index */
{
  ut_ad(index);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  return (UT_LIST_GET_NEXT(indexes, (dict_index_t *)index));
}
#endif /* UNIV_DEBUG */

/** Check whether the index is the clustered index.
@return	nonzero for clustered index, zero for other indexes */
inline ulint dict_index_is_clust(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (unlikely(dict_index->type & DICT_CLUSTERED));
}

/** Check whether the index is unique.
@return	nonzero for unique index, zero for other indexes */
inline ulint dict_index_is_unique(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (unlikely(dict_index->type & DICT_UNIQUE));
}

/** Check whether the index is a secondary index tree.
@return	nonzero zero for other indexes */
inline bool dict_index_is_sec(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return likely(!(dict_index->type & DICT_CLUSTERED));
}

/** Gets the number of user-defined columns in a table in the dictionary
cache.
@return	number of user-defined (e.g., not ROW_ID) columns of a table */
inline ulint dict_table_get_n_user_cols(const dict_table_t *table) /*!< in: table */
{
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return (table->n_cols - DATA_N_SYS_COLS);
}

/** Gets the number of system columns in a table in the dictionary cache.
@return	number of system (e.g., ROW_ID) columns of a table */
inline ulint dict_table_get_n_sys_cols(const dict_table_t *table __attribute__((unused))) /*!< in: table */
{
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(table->cached);

  return (DATA_N_SYS_COLS);
}

/** Gets the number of all columns (also system) in a table in the dictionary
cache.
@return	number of columns of a table */
inline ulint dict_table_get_n_cols(const dict_table_t *table) /*!< in: table */
{
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return (table->n_cols);
}

#ifdef UNIV_DEBUG
/** Gets the nth column of a table.
@return	pointer to column object */
inline dict_col_t *dict_table_get_nth_col(
  const dict_table_t *table, /*!< in: table */
  ulint pos
) /*!< in: position of column */
{
  ut_ad(table);
  ut_ad(pos < table->n_def);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return ((dict_col_t *)(table->cols) + pos);
}

/** Gets the given system column of a table.
@return	pointer to column object */
inline dict_col_t *dict_table_get_sys_col(
  const dict_table_t *table, /*!< in: table */
  ulint sys
) /*!< in: DATA_ROW_ID, ... */
{
  dict_col_t *col;

  ut_ad(table);
  ut_ad(sys < DATA_N_SYS_COLS);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  col = dict_table_get_nth_col(table, table->n_cols - DATA_N_SYS_COLS + sys);
  ut_ad(col->mtype == DATA_SYS);
  ut_ad(col->prtype == (sys | DATA_NOT_NULL));

  return (col);
}
#endif /* UNIV_DEBUG */

/** Gets the given system column number of a table.
@return	column number */
inline ulint dict_table_get_sys_col_no(
  const dict_table_t *table, /*!< in: table */
  ulint sys
) /*!< in: DATA_ROW_ID, ... */
{
  ut_ad(table);
  ut_ad(sys < DATA_N_SYS_COLS);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  return (table->n_cols - DATA_N_SYS_COLS + sys);
}

/** Check whether the table uses the compact page format.
@return	true if table uses the compact page format */
inline bool dict_table_is_comp(const dict_table_t *table) /*!< in: table */
{
  ut_ad(table != nullptr);
  ut_a(DICT_TF_COMPACT);

  return likely(table->flags & DICT_TF_COMPACT);
}

/** Determine the file format of a table.
@return	file format version */
inline ulint dict_table_get_format(const dict_table_t *table) /*!< in: table */
{
  ut_ad(table);

  return ((table->flags & DICT_TF_FORMAT_MASK) >> DICT_TF_FORMAT_SHIFT);
}

/** Determine the file format of a table. */
inline void dict_table_set_format(
  dict_table_t *table, /*!< in/out: table */
  ulint format
) /*!< in: file format version */
{
  ut_ad(table);

  table->flags = (table->flags & ~DICT_TF_FORMAT_MASK) | (format << DICT_TF_FORMAT_SHIFT);
}

/** Gets the number of fields in the internal representation of an index,
including fields added by the dictionary system.
@return	number of fields */
inline ulint dict_index_get_n_fields(const dict_index_t *dict_index) /*!< in: an internal
                                      representation of index (in
                                      the dictionary cache) */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (dict_index->n_fields);
}

/** Gets the number of fields in the internal representation of an index
that uniquely determine the position of an index entry in the index, if
we do not take multiversioning into account: in the B-tree use the value
returned by dict_index_get_n_unique_in_tree.
@return	number of fields */
inline ulint dict_index_get_n_unique(const dict_index_t *dict_index) /*!< in: an internal
                                                          representation
                                                          of index (in the
                                                          dictionary cache) */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(dict_index->cached == 1);

  return (dict_index->n_uniq);
}

/** Gets the number of fields in the internal representation of an index
which uniquely determine the position of an index entry in the index, if
we also take multiversioning into account.
@return	number of fields */
inline ulint dict_index_get_n_unique_in_tree(const dict_index_t *dict_index) /*!< in: an internal
                                      representation
                                      of index (in the dictionary
                                      cache) */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(dict_index->cached);

  if (dict_index_is_clust(dict_index)) {

    return (dict_index_get_n_unique(dict_index));
  }

  return (dict_index_get_n_fields(dict_index));
}

/** Gets the number of user-defined ordering fields in the index. In the
internal representation of clustered indexes we add the row id to the ordering
fields to make a clustered index unique, but this function returns the number of
fields the user defined in the index as ordering fields.
@return	number of fields */
inline ulint dict_index_get_n_ordering_defined_by_user(const dict_index_t *dict_index) /*!< in: an internal
                                      representation of index
                                      (in the dictionary cache) */
{
  return (dict_index->n_user_defined_cols);
}

#ifdef UNIV_DEBUG
/** Gets the nth field of an index.
@return	pointer to field object */
inline dict_field_t *dict_index_get_nth_field(
  const dict_index_t *index, /*!< in: index */
  ulint pos
) /*!< in: position of field */
{
  ut_ad(index);
  ut_ad(pos < index->n_def);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  return ((dict_field_t *)(index->fields) + pos);
}
#endif /* UNIV_DEBUG */

/** Returns the position of a system column in an index.
@return	position, ULINT_UNDEFINED if not contained */
inline ulint dict_index_get_sys_col_pos(
  const dict_index_t *dict_index, /*!< in: index */
  ulint type
) /*!< in: DATA_ROW_ID, ... */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  if (dict_index_is_clust(dict_index)) {

    return dict_col_get_clust_pos(dict_table_get_sys_col(dict_index->table, type), dict_index);

  } else {

    return dict_index_get_nth_col_pos(dict_index, dict_table_get_sys_col_no(dict_index->table, type));
  }
}

/** Gets the field column.
@return	field->col, pointer to the table column */
inline const dict_col_t *dict_field_get_col(const dict_field_t *field) /*!< in: index field */
{
  ut_ad(field);

  return (field->col);
}

/** Gets pointer to the nth column in an index.
@return	column */
inline const dict_col_t *dict_index_get_nth_col(
  const dict_index_t *dict_index, /*!< in: index */
  ulint pos
) /*!< in: position of the field */
{
  return (dict_field_get_col(dict_index_get_nth_field(dict_index, pos)));
}

/** Gets the column number the nth field in an index.
@return	column number */
inline ulint dict_index_get_nth_col_no(
  const dict_index_t *dict_index, /*!< in: index */
  ulint pos
) /*!< in: position of the field */
{
  return (dict_col_get_no(dict_index_get_nth_col(dict_index, pos)));
}

/** Returns the minimum data size of an index record.
@return	minimum data size in bytes */
inline ulint dict_index_get_min_size(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ulint n = dict_index_get_n_fields(dict_index);
  ulint size = 0;

  while (n--) {
    size += dict_col_get_min_size(dict_index_get_nth_col(dict_index, n));
  }

  return (size);
}

/** Gets the space id of the root of the index tree.
@return	space id */
inline ulint dict_index_get_space(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (dict_index->space);
}

/** Sets the space id of the root of the index tree. */
inline void dict_index_set_space(
  dict_index_t *dict_index, /*!< in/out: dict_index */
  ulint space
) /*!< in: space id */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  dict_index->space = space;
}

/** Gets the page number of the root of the index tree.
@return	page number */
inline ulint dict_index_get_page(const dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (dict_index->page);
}

/** Sets the page number of the root of index tree. */
inline void dict_index_set_page(
  dict_index_t *dict_index, /*!< in/out: dict_index */
  ulint page
) /*!< in: page number */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  dict_index->page = page;
}

/** Gets the read-write lock of the index tree.
@return	read-write lock */
inline rw_lock_t *dict_index_get_lock(dict_index_t *dict_index) /*!< in: dict_index */
{
  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  return (&(dict_index->lock));
}

/** Returns free space reserved for future updates of records. This is
relevant only in the case of many consecutive inserts, as updates
which make the records bigger might fragment the index.
@return	number of free bytes on page, reserved for updates */
inline ulint dict_index_get_space_reserve(void) {
  return (UNIV_PAGE_SIZE / 16);
}

/** Checks if a table is in the dictionary cache.
@return	table, NULL if not found */
inline dict_table_t *dict_table_check_if_in_cache_low(const char *table_name) /*!< in: table name */
{
  ut_ad(table_name);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  /* Look for the table name in the hash table */
  auto table_fold = ut_fold_string(table_name);

  dict_table_t *table;

  HASH_SEARCH(
    name_hash, dict_sys->table_hash, table_fold, dict_table_t *, table, ut_ad(table->cached), !strcmp(table->name, table_name)
  );

  return table;
}

/** Gets a table; loads it to the dictionary cache if necessary. A low-level
function.
@return	table, NULL if not found */
inline dict_table_t *dict_table_get_low(const char *table_name) /*!< in: table name */
{
  dict_table_t *table;

  ut_ad(table_name);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  table = dict_table_check_if_in_cache_low(table_name);

  if (table == NULL) {
    // FIXME: srv_force_recovery should be passed in as an arg
    table = dict_load_table(static_cast<ib_recovery_t>(srv_force_recovery), table_name);
  }

  ut_ad(!table || table->cached);

  return (table);
}

/** Returns a table object based on table id.
@return	table, NULL if does not exist */
inline dict_table_t *dict_table_get_on_id_low(
  ib_recovery_t recovery, /*!< in: recovery flag */
  uint64_t table_id
) /*!< in: table id */
{
  ut_ad(mutex_own(&(dict_sys->mutex)));

  /* Look for the table name in the hash table */
  auto fold = ut_uint64_fold(table_id);

  dict_table_t *table;

  HASH_SEARCH(id_hash, dict_sys->table_id_hash, fold, dict_table_t *, table, ut_ad(table->cached), table->id == table_id);

  if (table == NULL) {
    table = dict_load_table_on_id(recovery, table_id);
  }

  ut_ad(!table || table->cached);

  return (table);
}
