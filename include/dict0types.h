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
 @file include/dict0types.h
 Data dictionary global types

 Created 1/8/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"

#include "data0types.h"
#include "fsp0types.h"
#include "hash0hash.h"
#include "lock0types.h"
#include "mem0types.h"
#include "rem0types.h"
#include "sync0rw.h"

struct dict_table_t;

/* A cluster is a table with the type field set to DICT_CLUSTERED */
using dict_cluster_t = dict_table_t;

struct dict_sys_t;
struct dict_col_t;
struct dict_field_t;
struct dict_index_t;
struct dict_foreign_t;

struct ind_node_t;
struct tab_node_t;

/** Space id and page no where the dictionary header resides */
constexpr ulint DICT_HDR_SPACE  = 0;

 /** The page number of the dictionary header */
constexpr ulint DICT_HDR_PAGE_NO  = FSP_DICT_HDR_PAGE_NO;

/** The flags for ON_UPDATE and ON_DELETE can be ORed; the default is that
a foreign key constraint is enforced, therefore RESTRICT just means no flag */
/* @{ */
/** On delete cascade */
constexpr ulint DICT_FOREIGN_ON_DELETE_CASCADE = 1;

/** On update set null */
constexpr ulint DICT_FOREIGN_ON_DELETE_SET_NULL = 2;

/** On delete cascade */
constexpr ulint DICT_FOREIGN_ON_UPDATE_CASCADE = 4;

/** On update set null */
constexpr ulint DICT_FOREIGN_ON_UPDATE_SET_NULL = 8;

/** On delete no action */
constexpr ulint DICT_FOREIGN_ON_DELETE_NO_ACTION = 16;

/** On update no action */
constexpr ulint DICT_FOREIGN_ON_UPDATE_NO_ACTION = 32;
/* @} */

IF_DEBUG(constexpr ulint DICT_TABLE_MAGIC_N = 76333786;)

/** Number of flag bits */
constexpr ulint DICT_TF_BITS = 6;

/** Type flags of an index: OR'ing of the flags is allowed to define a
combination of types */
/* @{ */

/** Clustered index */
constexpr ulint DICT_CLUSTERED = 1;

/** Unique index */
constexpr ulint DICT_UNIQUE = 2;

/* @} */

/** Types for a table object */

/** ordinary table */
constexpr ulint DICT_TABLE_ORDINARY = 1;

/** Table flags.  All unused bits must be 0. */
/* @{ */

/* @} */

/** File format */
/* @{ */

/** File format */
constexpr ulint DICT_TF_FORMAT_SHIFT = 5;

constexpr ulint DICT_TF_FORMAT_MASK = ((~(~0UL << (DICT_TF_BITS - DICT_TF_FORMAT_SHIFT))) << DICT_TF_FORMAT_SHIFT);

/** InnoDBL up to 0.1 */
constexpr ulint DICT_TF_FORMAT_51 = 0;

/** InnoDB 0.1: V1 row format flag. */
constexpr ulint DICT_TF_FORMAT_V1 = DICT_TF_FORMAT_51;

/** Maximum supported file format */
constexpr ulint DICT_TF_FORMAT_MAX = DICT_TF_FORMAT_V1;

/* @} */

static_assert(
  (1 << (DICT_TF_BITS - DICT_TF_FORMAT_SHIFT)) > DICT_TF_FORMAT_MAX, "error DICT_TF_BITS is insufficient for DICT_TF_FORMAT_MAX"
);

/* @} */

/** @brief Additional table flags.

These flags will be stored in SYS_TABLES.MIX_LEN.  All unused flags
will be written as 0.  The column may contain garbage for tables
created with old versions of InnoDB that only implemented ROW_FORMAT=REDUNDANT.
*/

/* @{ */
constexpr ulint DICT_TF2_SHIFT = DICT_TF_BITS;

/** Shift value for table->flags. */
/** true for tables from CREATE TEMPORARY TABLE. */
constexpr ulint DICT_TF2_TEMPORARY = 1;

/** Total number of bits in table->flags. */
constexpr ulint DICT_TF2_BITS = DICT_TF2_SHIFT + 1;

/** TBD: This needs to be implemented to fix a bug.
 *  The status of online index creation */
enum class DDL_index_status {
  /** The index is complete and ready for access */
  READY = 0,

  /** The index is being created, online
  (allowing concurrent modifications) */
  IN_PROGRESS,

  /** Secondary index creation was aborted and the index
  should be dropped as soon as index->table->n_ref_count reaches 0,
  or online table rebuild was aborted and the clustered index
  of the original table should soon be restored to ONLINE_INDEX_COMPLETE */
  FAILED,

  /** The online index creation was aborted, the index was
  dropped from the data dictionary and the tablespace, and it
  should be dropped from the data dictionary cache as soon as
  index->table->n_ref_count reaches 0. */
  DROPPED
};

/* @} */
/** Data structure for a column in a table */
struct dict_col_t {
  /* @{ */
  DTYPE_FIELDS
  /* @} */

  /** Table column position (starting from 0) */
  unsigned ind : 10;

  /** nonzero if this column appears in the ordering fields of an index */
  unsigned ord_part : 1;
};

/** @brief DICT_MAX_INDEX_COL_LEN is measured in bytes and is the maximum
indexed column length (or indexed prefix length).

It is set to 3*256, so that one can create a column prefix index on
256 characters of a TEXT or VARCHAR column also in the UTF-8
charset. In that charset, a character may take at most 3 bytes.  This
constant MUST NOT BE CHANGED, or the compatibility of InnoDB data
files would be at risk! */
constexpr auto DICT_MAX_INDEX_COL_LEN = REC_MAX_INDEX_COL_LEN;

/** Value of dict_index_struct::magic_n */
IF_DEBUG(const ulint DICT_INDEX_MAGIC_N = 76789786;)

/** Data structure for a field in an index */
struct dict_field_t {
  /** Pointer to the table column */
  dict_col_t *col;

  /** Name of the column */
  const char *name;

  /** 0 or the length of the column prefix in bytes e.g., for
  INDEX (textcol(25)); must be smaller than
  DICT_MAX_INDEX_COL_LEN; NOTE that in the UTF-8 charset,
  MySQL sets this to 3 * the prefix len in UTF-8 chars */
  unsigned prefix_len : 10;

  /** 0 or the fixed length of the column if smaller than
  DICT_MAX_INDEX_COL_LEN */
  unsigned fixed_len : 10;
};

/** Data structure for an index.  Most fields will be
initialized to 0, nullptr or false in dict_mem_index_create(). */
struct dict_index_t {
  [[nodiscard]] bool is_clustered() const {
    ut_ad(magic_n == DICT_INDEX_MAGIC_N);

    return type & DICT_CLUSTERED;
  }

  /**
  * @brief Gets the status of online index creation.
  * Without the index->lock protection, the online status can change from
  * ONLINE_INDEX_CREATION to ONLINE_INDEX_COMPLETE (or ONLINE_INDEX_ABORTED) in
  * row_log_apply() once log application is done. So to make sure the status
  * is ONLINE_INDEX_CREATION or ONLINE_INDEX_COMPLETE you should always do
  * the recheck after acquiring index->lock 
  *
  * @return The status.
  */
  [[nodiscard]] DDL_index_status get_online_ddl_status() const;

  /** Determines if a secondary index is being created online, or if the
  * table is being rebuilt online, allowing concurrent modifications
  * to the table.
  * @retval true if the index is being or has been built online, or
  * if this is a clustered index and the table is being or has been rebuilt online
  * @retval false if the index has been created or the table has been
  * rebuilt completely */
  [[nodiscard]] bool is_online_ddl_in_progress() const;

  /** Id of the index */
  uint64_t id;

  /** Memory heap */
  mem_heap_t *heap;

  /** Index name */
  const char *name;

  /** Table name */
  const char *table_name;

  /** Back pointer to table */
  dict_table_t *table;

  /** Space where the index tree is placed */
  space_id_t space;

  /** Index tree root page number */
  page_no_t page;

  /** Array of field descriptions */
  dict_field_t *fields;

  /* List of indexes of the table */
  UT_LIST_NODE_T(dict_index_t) indexes;

  /** Statistics for query optimization */

  /* @{ */

  /* Approximate number of different key values for this index, for
  each n-column prefix where n <= dict_get_n_unique(index); we
  periodically calculate new estimates */
  int64_t *stat_n_diff_key_vals;

  /** Approximate index size in database pages */
  page_no_t stat_index_size;

  /** Approximate number of leaf pages in the index tree */
  page_no_t stat_n_leaf_pages;

  /** read-write lock protecting the upper levels of the index tree */
  rw_lock_t lock;

  /** Client compare context. For use defined column types and BLOBs
  the client is responsible for comparing the column values. This field
  is the argument for the callback compare function. */
  void *cmp_ctx;

  /** Id of the transaction that created this index, or 0 if the index existed
  when InnoDB was started up */
  uint64_t trx_id;

  /* @} */
  /** Index type (DICT_CLUSTERED, DICT_UNIQUE, DICT_UNIVERSAL) */
  unsigned type : 4;

  /* Position of the trx id column in a clustered index
  record, if the fields before it are known to be of a
  fixed size, 0 otherwise */
  unsigned trx_id_offset : 10;

  /** Number of columns the user defined to be in the
  index: in the internal representation we add more columns */
  unsigned n_user_defined_cols : 10;

  /** Number of fields from the beginning which are enough
  to determine an index entry uniquely */
  unsigned n_uniq : 10;

  /** Number of fields defined so far */
  unsigned n_def : 10;

  /** Number of fields in the index */
  unsigned n_fields : 10;

  /** Number of nullable fields */
  unsigned n_nullable : 10;

  /** True if the index object is in the dictionary cache */
  unsigned cached : 1;

  /** DDL_index_status. Transitions from IN_PROGRESS to SUCCESS
  are protected by dict_operation_lock and dict_sys->mutex. Other
  changes are protected only by the index->lock. */
  unsigned m_ddl_status : 2;

  /** Flag that is set for secondary indexes that have not been
  committed to the data dictionary yet */
  unsigned uncommitted : 1;

  /** True if this index is marked to be dropped in
  ha_innobase::prepare_drop_index(), otherwise false */
  unsigned to_be_dropped : 1;

  /** Magic number */
  IF_DEBUG(ulint magic_n;)
};

/** Data structure for a foreign key constraint; an example:
FOREIGN KEY (A, B) REFERENCES TABLE2 (C, D).  Most fields will be
initialized to 0, nullptr or false in dict_mem_foreign_create(). */
struct dict_foreign_t {
  /** This object is allocated from this memory heap */
  mem_heap_t *heap;

  /** id of the constraint as a null-terminated string */
  char *id;

  /** Number of indexes' first fields for which the foreign
  key constraint is defined: we allow the indexes to contain
  more fields than mentioned in the constraint, as long as
  the first fields are as mentioned */
  uint16_t n_fields;

  /** 0 or DICT_FOREIGN_ON_DELETE_CASCADE or DICT_FOREIGN_ON_DELETE_SET_NULL */
  uint8_t type;

  /** foreign table name */
  char *foreign_table_name;

  /** Table where the foreign key is */
  dict_table_t *foreign_table;

  /** Names of the columns in the foreign key */
  const char **foreign_col_names;

  /** Referenced table name */
  char *referenced_table_name;

  /** Table where the referenced key is */
  dict_table_t *referenced_table;

  /** Names of the referenced columns in the referenced table */
  const char **referenced_col_names;

  /** Foreign index; we require that both tables contain explicitly
  defined indexes for the constraint: InnoDB does not generate new
  indexes implicitly */
  dict_index_t *foreign_index;

  /** Referenced index */
  dict_index_t *referenced_index;

  /** List node for foreign keys of the table */
  UT_LIST_NODE_T(dict_foreign_t) foreign_list;

  /** List node for referenced keys of the table */
  UT_LIST_NODE_T(dict_foreign_t) referenced_list;
};

/** List of locks that different transactions have acquired on a table. This
list has a list node that is embedded in a nested union/structure. We have to
generate a specific template for it. See lock0lock.cc for the implementation. */
struct Table_lock_get_node;
using Table_locks = ut_list_base<Lock, Table_lock_get_node>;

/** Data structure for a database table.  Most fields will be
initialized to 0, nullptr or false in dict_mem_table_create(). */
struct dict_table_t {
  /** Id of the table */
  uint64_t id;

  /** Memory heap */
  mem_heap_t *heap;

  /** Table name */
  const char *name;

  /** nullptr or the directory path where a TEMPORARY table that was explicitly
  created by a user should be placed if innodb_file_per_table is defined; in
  Unix this is usually /tmp/... */
  const char *dir_path_of_temp_table;

  /*!< space where the clustered index of the
  table is placed */
  space_id_t space;

  unsigned flags : DICT_TF2_BITS; /*!< DICT_TF_COMPACT, ... */

  /** true if this is in a single-table tablespace and the .ibd file is
  missing; then we must return an error if the user tries
  to query such an orphaned table */
  unsigned ibd_file_missing : 1;

  /** This flag is set true when the user calls DISCARD TABLESPACE on this
  table, and reset to false in IMPORT TABLESPACE */
  unsigned tablespace_discarded : 1;

  /** true if the table object has been added to the dictionary cache */
  unsigned cached : 1;

  /** Number of columns defined so far */
  unsigned n_def : 10;

  /** Number of columns */
  unsigned n_cols : 10;

  /** Array of column descriptions */
  dict_col_t *cols;

  /** Column names packed in a character string "name1\0name2\0...nameN\0".
  Until the string contains n_cols, it will be allocated from a temporary heap.
  The final string will be allocated from table->heap. */
  const char *col_names;

  /** Hash chain node */
  hash_node_t name_hash;

  /** Hash chain node */
  hash_node_t id_hash;

  /** List of indexes of the table */
  UT_LIST_BASE_NODE_T(dict_index_t, indexes) indexes;

  /** List of foreign key constraints in the table; these refer to columns
  in other tables */
  UT_LIST_BASE_NODE_T(dict_foreign_t, foreign_list) foreign_list;

  /** List of foreign key constraints which refer to this table */
  UT_LIST_BASE_NODE_T(dict_foreign_t, referenced_list) referenced_list;

  /** Node of the LRU list of tables */
  UT_LIST_NODE_T(dict_table_t) table_LRU;

  /** Count of how many handles the user has opened to this table; dropping
  of the table is NOT allowed until this count gets to zero */
  ulint n_handles_opened;

  /** Count of how many foreign key check operations are currently being performed
  on the table: we cannot drop the table while there are foreign key checks running
  on it! */
  ulint n_foreign_key_checks_running;

  /** List of locks on the table */
  Table_locks locks;

  /** This field is used to specify in simulations tables which are so big
  that disk should be accessed: disk access is simulated by putting the
  thread to sleep for a while; NOTE that this flag is not stored to the data
  dictionary on disk, and the database will forget about value true if it has
  to reload the table definition from disk */
  IF_DEBUG(bool does_not_fit_in_memory;)

  /** flag: true if the maximum length of a single row exceeds BIG_ROW_SIZE;
  initialized in dict_table_add_to_cache() */
  unsigned big_rows : 1;

  /** Statistics for query optimization */
  /* @{ */
  /** true if statistics have been calculated the first time after database
  startup or table creation */
  unsigned stat_initialized : 1;

  /** Approximate number of rows in the table; we periodically calculate
  new estimates */
  int64_t stat_n_rows;

  /** Approximate clustered index size in database pages */
  ulint stat_clustered_index_size;

  /** Other indexes in database pages */
  ulint stat_sum_of_other_index_sizes;

  /** When a row is inserted, updated, or deleted, we add 1 to this
  number; we calculate new estimates for the stat_...  values for
  the table and the indexes at an interval of 2 GB or when about 1 / 16
  of table has been modified; also when an estimate operation is called
  for; the counter is reset to zero at statistics calculation; this
  counter is not protected by any latch, because this is only used
  for heuristics */
  ulint stat_modified_counter;
  /* @} */

  /** Value of dict_table_struct::magic_n */
  IF_DEBUG(ulint magic_n;)
};

/** Data dictionary */
struct dict_sys_t {
  /** mutex protecting the data dictionary; protects also the disk-based
   * dictionary system tables; this mutex serializes CREATE TABLE and
   * DROP TABLE, as well as reading the dictionary data for a table from 
   * system tables.
   */
  mutex_t mutex;

  /** The next row id to assign; NOTE that at a checkpoint this must
   * be written to the dict system header and flushed to a file; in
   * recovery this must be derived from the log records */
  uint64_t row_id;

  /** Hash table of the tables, based on name */
  hash_table_t *table_hash;

  /** Hash table of the tables, based on id */
  hash_table_t *table_id_hash;

  /** LRU list of tables */
  UT_LIST_BASE_NODE_T(dict_table_t, table_LRU) table_LRU;

  /** Varying space in bytes occupied by the data dictionary table
   * and index objects */
  ulint size;

  /*!< SYS_TABLES table */
  dict_table_t *sys_tables;

  /** SYS_COLUMNS table */
  dict_table_t *sys_columns;

  /** SYS_INDEXES table */
  dict_table_t *sys_indexes;

  /** SYS_FIELDS table */
  dict_table_t *sys_fields;
};
