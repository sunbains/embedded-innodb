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

#include "data0type.h"
#include "fsp0types.h"
#include "lock0types.h"
#include "mem0types.h"
#include "que0types.h"
#include "rem0types.h"
#include "sync0rw.h"

#include <string>

struct Table;

/* A cluster is a table with the type field set to DICT_CLUSTERED */
using dict_cluster_t = Table;

struct Dict;
struct Field;
struct Column;
struct Index;
struct Foreign;
struct Insert_node;
struct Commit_node;
struct Index_node;
struct Table_node;

/** Space id and page no where the dictionary header resides */
constexpr space_id_t DICT_HDR_SPACE = SYS_TABLESPACE;

/** The page number of the dictionary header */
constexpr page_no_t DICT_HDR_PAGE_NO = FSP_DICT_HDR_PAGE_NO;

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

/** Array of mutexes protecting Index::stat_n_diff_key_vals[] */
constexpr ulint DICT_INDEX_STAT_MUTEX_SIZE = 32;

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

using Dict_id = uint64_t;


/** The ids for the basic system tables and their indexes */
constexpr Dict_id DICT_TABLES_ID = 1;
constexpr Dict_id DICT_COLUMNS_ID = 2;
constexpr Dict_id DICT_INDEXES_ID = 3;
constexpr Dict_id DICT_FIELDS_ID = 4;
/** @} */

/** The following is a secondary index on SYS_TABLES */
constexpr Dict_id DICT_TABLE_IDS_ID = 5;

/** The ids for tables etc. start from this number, except for basic system tables and their above defined indexes. */
constexpr Dict_id DICT_HDR_FIRST_ID = 10;

/* System table IDs start from this. */
constexpr Dict_id DICT_SYS_ID_MIN = (0xFFFFFFFFUL << 32) | SYS_TABLESPACE;

/* The offset of the dictionary header on the page */
constexpr auto DICT_HDR = FSEG_PAGE_DATA;

/*-------------------------------------------------------------*/
/* Dictionary header offsets */
/** The latest assigned row id */
constexpr Dict_id DICT_HDR_ROW_ID = 0;

enum class Dict_id_type {
  /** The latest assigned table id */
  TABLE_ID = 8,

  /** The latest assigned index id */
  INDEX_ID = 16,
};

/** Obsolete, always 0. */
constexpr Dict_id DICT_HDR_MIX_ID = 24;

/** Root of the table index tree */
constexpr Dict_id DICT_HDR_TABLES = 32;

/** Root of the table index tree */
constexpr Dict_id DICT_HDR_TABLE_IDS = 36;

/** Root of the column index tree */
constexpr Dict_id DICT_HDR_COLUMNS = 40;

/** Root of the index index tree */
constexpr Dict_id DICT_HDR_INDEXES = 44;

/** Root of the index field index tree */
constexpr Dict_id DICT_HDR_FIELDS = 48;

/** Segment header for the tablespace segment into which the dictionary header is created */
constexpr Dict_id DICT_HDR_FSEG_HEADER = 56;

/*-------------------------------------------------------------*/

/** The field number of the page number field in the sys_indexes table clustered index */

constexpr ulint DICT_SYS_INDEXES_PAGE_NO_FIELD = 8;
constexpr ulint DICT_SYS_INDEXES_SPACE_NO_FIELD = 7;
constexpr ulint DICT_SYS_INDEXES_TYPE_FIELD = 6;
constexpr ulint DICT_SYS_INDEXES_NAME_FIELD = 4;

/** When a row id which is zero modulo this number (which must be a power of
two) is assigned, the field DICT_HDR_ROW_ID on the dictionary header page is
updated */
constexpr ulint DICT_HDR_ROW_ID_WRITE_MARGIN = 256;

/** @brief Additional table flags.

These flags will be stored in SYS_TABLES.MIX_LEN.  All unused flags
will be written as 0. */

/* @{ */
constexpr ulint DICT_TF2_SHIFT = DICT_TF_BITS;

/** Shift value for table->flags. */
/** true for tables from CREATE TEMPORARY TABLE. */
constexpr ulint DICT_TF2_TEMPORARY = 1;

/** Total number of bits in Tableflags. */
constexpr ulint DICT_TF2_BITS = DICT_TF2_SHIFT + 1;

/** initial memory heap size when creating a table or index object */
constexpr ulint DICT_HEAP_SIZE = 120;

/** Identifies generated InnoDB foreign key names */
constexpr char dict_ibfk[] = "_ibfk_";

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
struct Column {

  /**
   * @brief Gets the fixed size of the column.
   * 
   * @return The fixed size of the column.
   */
  [[nodiscard]] inline ulint get_fixed_size() const noexcept {
    return dtype_get_fixed_size_low(mtype, prtype, len, mbminlen, mbmaxlen);
  }

  /**
   * @brief Gets the column number/position.
   * 
   * @return The column number.
   */
  [[nodiscard]] inline ulint get_no() const noexcept {
    return m_ind;
  }

  /**
   * Returns the ROW_FORMAT=REDUNDANT stored SQL NULL size of a column.
   * For fixed length types it is the fixed length of the type, otherwise 0.
   * 
   * @return SQL null storage size in ROW_FORMAT=REDUNDANT
   */
  [[nodiscard]] inline ulint get_sql_null_size() const noexcept {
    return get_fixed_size();
  }
  #ifdef UNIV_DEBUG

  /**
   * Gets the column data type.
   * 
   * @param[in,out] type data type
   */
  inline void copy_type(dtype_t *type) const noexcept {
    type->mtype = mtype;
    type->prtype = prtype;
    type->len = len;
    type->mbminlen = mbminlen;
    type->mbmaxlen = mbmaxlen;
  }

  /**
   * Assert that a column and a data type match.
   * 
   * @param[in] col column
   * @param[in] type data type
   * 
   * @return true
   */
  [[nodiscard]] inline bool assert_equal(const dtype_t *type) const noexcept {
    ut_ad(mtype == type->mtype);
    ut_ad(prtype == type->prtype);
    ut_ad(len == type->len);
    ut_ad(mbminlen == type->mbminlen);
    ut_ad(mbmaxlen == type->mbmaxlen);

    return true;
  }

  /**
   * @brief Gets the minimum size of the column.
   * 
   * @return The minimum size of the column.
   */
  [[nodiscard]] inline ulint get_min_size() const noexcept {
    return dtype_get_min_size_low(mtype, prtype, len, mbminlen, mbmaxlen);
  }

  /**
   * Returns the maximum size of the column.
   * 
   * @return maximum size
   */
  [[nodiscard]] inline ulint get_max_size() const noexcept {
    return dtype_get_max_size_low(mtype, len);
  }

  #endif /* UNIV_DEBUG */
  /* @{ */
  DTYPE_FIELDS
  /* @} */

  /** Column name */
  const char *m_name{};

  /** Table column position (starting from 0) */
  unsigned m_ind : 10;

  /** Nonzero if this column appears in the ordering fields of an index */
  unsigned m_ord_part : 1;

};

/** @brief DICT_MAX_INDEX_COL_LEN is measured in bytes and is the maximum
indexed column length (or indexed prefix length).

It is set to 3*256, so that one can create a column prefix index on
256 characters of a TEXT or VARCHAR column also in the UTF-8
charset. In that charset, a character may take at most 3 bytes.  This
constant MUST NOT BE CHANGED, or the compatibility of InnoDB data
files would be at risk! */
constexpr auto DICT_MAX_INDEX_COL_LEN = REC_MAX_INDEX_COL_LEN;

/** Value of Index::m_magic_n */
IF_DEBUG(const ulint DICT_INDEX_MAGIC_N = 76789786;)

/** Data structure for a field in an index */
struct Field {
  /**
   * Gets the field column.
   * 
   * @param[in] field index field
   * 
   * @return field->col, pointer to the table column
   */
  [[nodiscard]] inline const Column *get_col() const noexcept {
    return m_col;
  }

  /**
   * Gets the field column.
   * 
   * @param[in] field index field
   * 
   * @return field->col, pointer to the table column
   */
  [[nodiscard]] inline Column *get_col() noexcept {
    return const_cast<Column *>(static_cast<const Field *>(this)->get_col());
  }

  /** Pointer to the table column */
  Column *m_col{};

  /** Name of the column */
  const char *m_name{};

  /** 0 or the length of the column prefix in bytes e.g., for
  INDEX (textcol(25)); must be smaller than
  DICT_MAX_INDEX_COL_LEN; NOTE that in the UTF-8 charset,
  MySQL sets this to 3 * the prefix len in UTF-8 chars */
  uint32_t m_prefix_len{};

  /** 0 or the fixed length of the column if smaller than
  DICT_MAX_INDEX_COL_LEN */
  uint32_t m_fixed_len{};
};

/** Data structure for an index. */ 
struct Index {

  /**
   * @brief Constructor for an index instance.
   * 
   * @param[in] heap The memory heap.
   * @param[in] table The parent table.
   * @param[in] index_name The index name.
   * @param[in] page_id The page id where the index tree is placed, ignored if the index is of the clustered type.
   * @param[in] type The index type (DICT_UNIQUE, DICT_CLUSTERED, ... ORed).
   * @param[in] n_fields The number of fields.
   */
  Index(mem_heap_t *heap, Table *table, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept;

  /**
   * @brief Creates an index memory object.
   * 
   * @param[in] table Parent table.
   * @param[in] index_name The index name.
   * @param[in] page_id The page id where the index tree is placed, ignored if the index is of the clustered type.
   * @param[in] type The index type (DICT_UNIQUE, DICT_CLUSTERED, ... ORed).
   * @param[in] n_fields The number of fields.
   * 
   * @return The created index object.
   */
  [[nodiscard]] static Index *create(Table *table, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept;

/**
   * @brief Creates an instance of an index without a parent table. Used in the SQL parser.
   * 
   * @param[in] table_name Parent table name
   * @param[in] index_name The index name.
   * @param[in] page_id The page id where the index tree is placed, ignored if the index is of the clustered type.
   * @param[in] type The index type (DICT_UNIQUE, DICT_CLUSTERED, ... ORed).
   * @param[in] n_fields The number of fields.
   * 
   * @return The created index object.
   */
  [[nodiscard]] static Index *create(const char* table_name, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept;

  /**
   * @brief Destroys an index instance.
   * 
   * @param[in,out] index The index instance to destroy.
   */
  static void destroy(Index *&index, Source_location loc) noexcept {
    auto *heap = index->m_heap;
    call_destructor<Index>(index);
    mem_heap_free_func(heap, loc);
    index = nullptr;
  }

  /**
   * @brief Destructor for an index instance.
   */
  ~Index() = default; 

  [[nodiscard]] bool is_clustered() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return m_type & DICT_CLUSTERED;
  }

  /**
   * @brief Gets the name of the table.
   * 
   * @return The name of the table.
   */
  [[nodiscard]] inline const char *get_table_name() const noexcept;

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

  /**
   * Gets the page number of the root of the index tree.
   * 
   * @return page number
   */
  [[nodiscard]] inline space_id_t get_space_id() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return m_page_id.space_id();
  }

  /**
   * Gets the page number of the root of the index tree.
   * 
   * @return page number
   */
  [[nodiscard]] inline page_no_t get_page_no() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return m_page_id.page_no();
  }

  /**
   * Gets the number of fields in the internal representation of an index,
   * including fields added by the dictionary system.
   * 
   * @return number of fields
   */
  [[nodiscard]] inline ulint get_n_fields() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return m_n_fields;
  }

  /**
   * Gets the number of fields in the internal representation of an index
   * that uniquely determine the position of an index entry in the index, if
   * we do not take multiversioning into account: in the B-tree use the value
   * returned by get_n_unique_in_tree.
   * 
   * @return number of fields
   */
  [[nodiscard]] inline ulint get_n_unique() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);
    ut_ad(m_cached == 1);

    return m_n_uniq;
  }
  /**
   * Gets the number of fields in the internal representation of an index
   * which uniquely determine the position of an index entry in the index, if
   * we also take multiversioning into account.
   * 
   * @return number of fields
   */
  [[nodiscard]] inline ulint get_n_unique_in_tree() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);
    ut_ad(m_cached == 1);

    return is_clustered() ? get_n_unique() : get_n_fields();
  }

  /**
   * Gets the number of user-defined ordering fields in the index. In the
   * internal representation of clustered indexes we add the row id to the ordering
   * fields to make a clustered index unique, but this function returns the number of
   * fields the user defined in the index as ordering fields.
   * 
   * @return number of fields
   */
  [[nodiscard]] inline ulint get_n_ordering_defined_by_user() const noexcept {
    return m_n_user_defined_cols;
  }

  /**
   * Gets the nth field of an index.
   * 
   * @param[in] pos position of field
   * 
  * @return pointer to field object
   */
  [[nodiscard]] inline const Field *get_nth_field(ulint pos) const noexcept {
    ut_ad(pos < m_n_defined);
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return &m_fields[pos];
  }

  /**
   * Gets the nth field of an index.
   * 
   * @param[in] pos position of field
   * 
  * @return pointer to field object
   */
  [[nodiscard]] inline Field *get_nth_field(ulint pos) noexcept {
    return const_cast<Field *>(static_cast<const Index *>(this)->get_nth_field(pos));
  }

  /**
   * Looks for column n in an index and returns its position in the internal
   * representation of the index ie. ordinal value in m_fields[].
   * 
   * @param[in] n field number
   * 
   * @return position in internal representation of the index; ULINT_UNDEFINED if not found.
   */
  [[nodiscard]] ulint get_nth_field_pos(ulint n) const noexcept;

  /**
   * Returns true if the index contains a column or a prefix of that column.
   * 
   * @param[in] n column number
   * 
   * @return true if contains the column or its prefix
   */
  [[nodiscard]] bool contains_col_or_prefix(ulint n) const noexcept;

  /**
   * Looks for a matching field in an index. The column has to be the same.
   * The column in index must be complete, or must contain a prefix longer
   * than the column in index. That is, we must be able to construct the
   * prefix in given index from the prefix in this checked index.
   * 
   * @param[in] index index to match
   * @param[in] n field number in inde2
   * 
   * @return position in internal representation of the index; ULINT_UNDEFINED if not contained
   */
  [[nodiscard]] ulint get_nth_field_pos(const Index *index, ulint n) const noexcept;

  /**
   * Gets the next index on the table.
   * 
   * @return index, nullptr if none left
   */
  [[nodiscard]] inline const Index *get_next() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return UT_LIST_GET_NEXT(m_indexes, this);
  }

  /**
   * Gets the next index on the table.
   * 
   * @return index, nullptr if none left
   */
  [[nodiscard]] inline Index *get_next() noexcept {
    return const_cast<Index *>(static_cast<const Index *>(this)->get_next());
  }
  
  /**
   * Returns the minimum size of the column.
   * 
   * @param[in] col column
   * 
   * @return minimum size
   */
  [[nodiscard]] static inline ulint col_get_min_size(const Column *col) noexcept {
    return dtype_get_min_size_low(col->mtype, col->prtype, col->len, col->mbminlen, col->mbmaxlen);
  }

  /**
   * Returns the minimum data size of an index record.
   * 
   * @param[in] col column
   * 
   * @return minimum data size in bytes
   */
  [[nodiscard]] inline ulint get_min_size() const noexcept;

/**
   * Gets the column position in the clustered index.
   *
   * @param[in] col table column
   * 
   * @return column position in the clustered index
   */
  [[nodiscard]] inline ulint get_clustered_field_pos(const Column *col) const noexcept {
    ut_ad(is_clustered());
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    for (ulint i{}; i < m_n_defined; ++i) {
      const auto field = &m_fields[i];

      if (!field->m_prefix_len && field->m_col == col) {
        return i;
      }
    }

    return ULINT_UNDEFINED;
  }

  /**
   * Gets the clustered index of the table.
   * 
   * @return the clustered index
   */
  [[nodiscard]] inline const Index *get_clustered_index() const noexcept;

  /**
   * Gets the clustered index of the table.
   * 
   * @return the clustered index
   */
  [[nodiscard]] inline Index *get_clustered_index() noexcept;

  /**
   * Returns the position of a system column in an index.
   * 
   * @param[in] type DATA_ROW_ID, ...
   * 
   * @return position, ULINT_UNDEFINED if not contained
   */
  [[nodiscard]] inline ulint get_sys_col_field_pos(ulint type) const noexcept;

  /**
   * Check whether the index is unique.
   * 
   * @return nonzero for unique index, zero for other indexes
   */
  [[nodiscard]] inline bool is_unique() const noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    return (m_type & DICT_UNIQUE) != 0;
  }

/**
   * Gets the column number in the owning table using the nth field in an index.
   * 
   * @param[in] pos position of the field
   * 
   * @return column number
   */
  [[nodiscard]] inline ulint get_nth_table_col_no(ulint pos) const noexcept {
    return m_fields[pos].m_col->m_ind;
  }

  /**
   * @brief Converts an index to a string representation.
   * 
   * @return The string.
   */
  [[nodiscard]] std::string to_string() const noexcept;

  /**
  * @brief Adds a field definition to an index.
  * 
  * @param[in] name The column name.
  * @param[in] prefix_len The column prefix length in a column prefix index like INDEX (textcol(25)).
  * 
  * @return The field.
  */
  [[nodiscard]] Field *add_field(const char *name, ulint prefix_len) noexcept {
    ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

    auto field = get_nth_field(m_n_defined++);

    field->m_name = name;
    field->m_prefix_len = prefix_len;

    return field;
  }

  /** Gets the nth column of a table.
   * 
   * @param[in] pos position of column
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline const Column *get_nth_col(ulint pos) const noexcept;

  /** Gets the nth column of a table.
   * 
   * @param[in] pos position of column
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline Column *get_nth_col(ulint pos) noexcept;

  /**
   * @brief Copies fields contained in index2 to index1.
   *
   * @param[in] index Index to copy from
   */
  void copy_from(Index *index) noexcept;

  /**
   * @brief Copies the types of the fields in the index to a tuple.
   * 
   * @param[in] tuple The tuple.
   * @param[in] n_fields The number of fields.
   */
  void copy_types(DTuple *tuple, ulint n_fields) const noexcept;

  /**
   * @brief Adds a column to the index.
   * 
   * @param[in] table The table.
   * @param[in] col The column.
   * @param[in] prefix_len The prefix length.
   */
  void add_col(const Table *table, Column *col, ulint prefix_len) noexcept;

  /**
   * @brief Adds an index to the cache.
   * 
   * @param[in] table The table.
   * @param[in] page_no The page number.
   * @param[in] strict The strict flag.
   * 
   * @return The error code.
   */
   [[nodiscard]] db_err add_to_cache(Table *table, page_no_t page_no, bool strict) noexcept;

  /**
   * Calculates the minimum record length in an index.
   * 
   * @return minimum record length
   */
  [[nodiscard]] ulint calc_min_rec_len() const noexcept;

  /**
   * @brief Gets the read-write lock of the index.
   * 
   * @return The read-write lock.
   */
  [[nodiscard]] inline rw_lock_t *get_lock() const noexcept {
    return &m_lock;
  }

  /**
   * Builds a typed data tuple out of a physical record.
   * 
   * @param[in] rec record for which to build data tuple
   * @param[in] n_fields number of data fields
   * @param[in] heap memory heap where tuple created
   * 
   * @return own: data tuple
   */
  [[nodiscard]] DTuple *build_data_tuple(rec_t *rec, ulint n_fields, mem_heap_t *heap) const noexcept;

/**
   * Copies an initial segment of a physical record, long enough to specify an index entry uniquely.
   * 
   * @param[in] rec record for which to copy prefix
   * @param[in,out] n_fields number of fields copied
   * @param[in,out] buf memory buffer for the copied prefix, or NULL.
   * @param[in,out] buf_size buffer size
   * 
   * @return pointer to the prefix record
   */
  [[nodiscard]] rec_t *copy_rec_order_prefix(const rec_t *rec, ulint *n_fields, byte *&buf, ulint &buf_size) const noexcept;

  /**
   * Builds a node pointer out of a physical record and a page number.
   * 
   * @param[in] rec record for which to build node pointer
   * @param[in] page_no page number to put in node pointer
   * @param[in] heap memory heap where pointer created
   * @param[in] level level of rec in tree: 0 means leaf level
   * 
   * @return own: node pointer
   */
  [[nodiscard]] DTuple *build_node_ptr(const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level) const noexcept;

#ifdef UNIV_DEBUG
/**
   * Checks that a tuple has n_fields_cmp value in a sensible range,
   * so that no comparison can occur with the page number field in a node pointer.
   * 
   * @param[in] tuple tuple used in a search
   * 
   * @return true if ok
   */
  [[nodiscard]] bool index_check_search_tuple(const Index *index, const DTuple *tuple) noexcept;

  bool check_search_tuple(const DTuple *tuple) const noexcept {
    ut_ad(tuple->n_fields_cmp <= get_n_unique_in_tree());
    return true;
  }
#endif /* UNIV_DEBUG */

  // Disable copying and moving
  Index(Index &&) = delete;
  Index(const Index &) = delete;
  Index &operator=(Index &&) = delete;
  Index &operator=(const Index &) = delete;

  /** Data structure for index statistics */
  struct Stats {
    /** Approximate index size in database pages */
    page_no_t m_index_size{};

    /** Approximate number of leaf pages in the index tree */
    page_no_t m_n_leaf_pages{};

    /** Approximate number of different key values for this index, for
    each n-column prefix where n <= get_n_unique(); we
    periodically calculate new estimates */
    int64_t *m_n_diff_key_vals{};
  };

  /** Index type (DICT_CLUSTERED, DICT_UNIQUE, DICT_UNIVERSAL) */
  unsigned m_type : 4;

  /** Position of the trx id column in a clustered index record, if the fields
   * before it are known to be of a fixed size, 0 otherwise */
  unsigned m_trx_id_offset : 10;

  /** Number of columns the user defined to be in the index: in the internal
   * representation we add more columns */
  unsigned m_n_user_defined_cols : 10;

  /** Number of fields from the beginning which are enough to determine an
   * index entry uniquely */
  unsigned m_n_uniq : 10;

  /** Number of fields defined so far */
  unsigned m_n_defined : 10;

  /** Number of fields in the index */
  unsigned m_n_fields : 10;

  /** Number of nullable fields */
  unsigned m_n_nullable : 10;

  /** True if the index object is in the dictionary cache */
  unsigned m_cached : 1;

  /** DDL_index_status. Transitions from IN_PROGRESS to SUCCESS are protected by
   * dict_operation_lock and dict_sys->mutex. Other changes are protected only by
   * the index->m_lock. */
  unsigned m_ddl_status : 2;

  /** Flag that is set for secondary indexes that have not been committed to the data dictionary yet */
  unsigned m_uncommitted : 1;

  /** True if this index is marked to be dropped in ha_innobase::prepare_drop_index(), otherwise false */
  unsigned m_to_be_dropped : 1;

  /** Id of the index */
  Dict_id m_id;

  /** Memory heap */
  mem_heap_t *m_heap;

  /** Index name */
  const char *m_name;

  /** Back pointer to table */
  Table *m_table;

  /** Space and root page number where the index tree is placed */
  Page_id m_page_id{};

  /** Array of field descriptions */
  Field *m_fields;

  /* List of indexes of the table */
  UT_LIST_NODE_T(Index) m_indexes;

  /** Statistics for query optimization */
  Stats m_stats;

  /** read-write lock protecting the upper levels of the index tree */
  mutable rw_lock_t m_lock;

  /** Client compare context. For use defined column types and BLOBs
  the client is responsible for comparing the column values. This field
  is the argument for the callback compare function. */
  void *m_cmp_ctx;

  /** Id of the transaction that created this index, or 0 if the index existed
  when InnoDB was started up */
  trx_id_t m_trx_id;

  /* @} */

  /** Magic number */
  IF_DEBUG(ulint m_magic_n;)
};

/** Data structure for a foreign key constraint; an example:
FOREIGN KEY (A, B) REFERENCES TABLE2 (C, D).  Most fields will be
initialized to 0, nullptr or false in dict_mem_foreign_create(). */
struct Foreign {
  /**
   * @brief Creates a foreign key constraint memory object.
   * 
   * @param[in] heap The memory heap.
   */
  Foreign(mem_heap_t *heap) noexcept : m_heap(heap) { }

  /**
   * Destructor
   */
  ~Foreign() = default;

  /**
   * @brief Creates a foreign key constraint memory object.
   * 
   * @return The created foreign key constraint object.
   */
  [[nodiscard]] static Foreign* create() noexcept;

  /**
   * @brief Destroys a foreign key constraint memory instance.
   * 
   * @param[in, out] foreign The foreign key constraint object to be destroyed.
   */
  static void destroy(Foreign *&foreign) noexcept;

  /**
  * @brief Drops the foreign key constraint from the referenced and foreign tables.
  */
  void drop_constraint() noexcept;

  /**
   * @brief Finds an equivalent index in the foreign table.
   * 
   * @return The equivalent index.
   */
  [[nodiscard]] Index *find_equiv_index() const noexcept;

  /**
   * @brief Converts a foreign key constraint to a string representation.
   * 
   * @return The string.
   */
  [[nodiscard]] std::string to_string() const noexcept;

  // Disable copying and moving
  Foreign(Foreign &&) = delete;
  Foreign(const Foreign &) = delete;
  Foreign &operator=(Foreign &&) = delete;
  Foreign &operator=(const Foreign &) = delete;

  /** This object is allocated from this memory heap */
  mem_heap_t *m_heap{};

  /** id of the constraint as a null-terminated string */
  char *m_id{};

  /** Number of indexes' first fields for which the foreign
  key constraint is defined: we allow the indexes to contain
  more fields than mentioned in the constraint, as long as
  the first fields are as mentioned */
  uint16_t m_n_fields{};

  /** 0 or DICT_FOREIGN_ON_DELETE_CASCADE or DICT_FOREIGN_ON_DELETE_SET_NULL */
  uint8_t m_type{};

  /** foreign table name */
  char *m_foreign_table_name{};

  /** Table where the foreign key is */
  Table *m_foreign_table{};

  /** Names of the columns in the foreign key */
  const char **m_foreign_col_names{};

  /** Referenced table name */
  char *m_referenced_table_name{};

  /** Table where the referenced key is */
  Table *m_referenced_table{};

  /** Names of the referenced columns in the referenced table */
  const char **m_referenced_col_names{};

  /** Foreign index; we require that both tables contain explicitly
  defined indexes for the constraint: InnoDB does not generate new
  indexes implicitly */
  Index *m_foreign_index{};

  /** Referenced index */
  Index *m_referenced_index{};

  /** List node for foreign keys of the table */
  UT_LIST_NODE_T(Foreign) m_foreign_list{};

  /** List node for referenced keys of the table */
  UT_LIST_NODE_T(Foreign) m_referenced_list{};
};

/** List of locks that different transactions have acquired on a table. This
list has a list node that is embedded in a nested union/structure. We have to
generate a specific template for it. See lock0lock.cc for the implementation. */
struct Table_lock_get_node;
using Table_locks = ut_list_base<Lock, Table_lock_get_node>;

/** Data structure for a database table.  Most fields will be
initialized to 0, nullptr or false in dict_mem_table_create(). */
struct Table {

  /**
   * @brief Creates a table memory object.
   * 
   * @param[in] heap The memory heap.
   * @param[in] name The table name.
   * @param[in] space The space where the clustered index of the table is placed; this parameter is ignored if the table is made a member of a cluster.
   * @param[in] n_cols The number of columns.
   * @param[in] flags The table flags.
   * @param[in] ibd_file_missing True if the ibd file is missing, false otherwise.
   * 
    * @return The created table object.
   */
  Table(mem_heap_t *heap, const char *name, space_id_t space, ulint n_cols, ulint flags, bool ibd_file_missing) noexcept;

  /**
   * @brief Destructor.
   */
  ~Table() noexcept;

  /**
   * @brief Creates a table memory object.
   * 
   * @param[in] name The table name.
   * @param[in] space The space where the clustered index of the table is placed; this parameter is ignored if the table is made a member of a cluster.
   * @param[in] n_cols The number of columns.
   * @param[in] flags The table flags.
   * @param[in] ibd_file_missing True if the ibd file is missing, false otherwise.
   * 
    * @return The created table object.
   */
  [[nodiscard]] static Table *create(const char *name, space_id_t space, ulint n_cols, ulint flags, bool ibd_file_missing, Source_location loc) noexcept;

  /**
   * @brief Destroys a table memory instance.
   * 
   * @param[in] table The table object to be destroyed.
   */
  static void destroy(Table *&table, Source_location loc) noexcept;

  /**
   * Gets the name of a column.
   * 
   * @param[in] col_nr The column number.
   * 
   * @return The column name.
   */
  [[nodiscard]] inline const char *get_col_name(ulint col_no) const noexcept {
    ut_ad(col_no < m_n_fields);
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
    return m_cols[col_no].m_name;
  }

  /**
   * Gets the number of a column.
   * 
   * @param[in] name The column name.
   * 
   * @return The column number.
   */
  [[nodiscard]] inline int get_col_no(const char *name) const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    for (ulint i{}; i < m_n_fields; ++i) {
      if (strcmp(m_cols[i].m_name, name) == 0) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Gets an index by its id.
   * 
   * @param[in] id The index id.
   * 
   * @return The index.
   */
  [[nodiscard]] inline Index *index_get_on_id(Dict_id id) noexcept {
    for (auto index : m_indexes) {
      if (id == index->m_id) {
        return index;
      }
    }

    return nullptr;
  }

  /**
   * Gets the first index on the table (the clustered index).
   * 
   * @return index, nullptr if none exists
   */
  [[nodiscard]] inline const Index *get_first_index() const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    /* A table must have at least one index: the clustered index */
    ut_a(!m_indexes.empty());

    return UT_LIST_GET_FIRST(m_indexes);
  }

  /**
   * Gets the first index on the table (the clustered index).
   * 
   * @return index, nullptr if none exists
   */
  [[nodiscard]] inline Index *get_first_index() noexcept {
    return const_cast<Index *>(static_cast<const Table *>(this)->get_first_index());
  }

  /**
   * Gets the clustered index on the table.
   * 
   * @return clustered index, nullptr if none exists
   */
  [[nodiscard]] inline const Index *get_clustered_index() const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
    return get_first_index();
  }

  /**
   * Gets the first index on the table (the clustered index).
   * 
   * @return index, nullptr if none exists
   */
  [[nodiscard]] inline Index *get_clustered_index() noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
    return get_first_index();
  }

  /**
   * Gets the first secondary index on the table.
   * 
   * @return secondary index, nullptr if none exists
   */
  [[nodiscard]] inline const Index *get_secondary_index() const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    return get_first_index()->get_next();
  }

  /**
   * Gets the first secondary index on the table.
   * 
   * @return secondary index, nullptr if none exists
   */
  [[nodiscard]] inline Index *get_secondary_index() noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
    return get_first_index()->get_next();
  }

  /**
   * Looks for column n position in the clustered index.
   * 
   * @param[in] n column number
   * 
   * @return position in internal representation of the clustered index
   */
  [[nodiscard]] ulint get_nth_col_pos(ulint n) const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
    ut_ad(!m_indexes.empty());

    return get_clustered_index()->get_nth_field_pos(n);
  }

  /**
   * @brief Adds system columns to a table object.
   */
  void add_system_columns() noexcept;

  /**
   * @brief Converts a table to a string representation.
   * 
   * @param[in] dict The dictionary object.
   * 
   * @return The string.
   */
  [[nodiscard]] std::string to_string(Dict *dict) noexcept;

  /**
   * Gets the number of user-defined columns in a table in the dictionary cache.
   * 
   * @return number of user-defined (e.g., not ROW_ID) columns of a table
   */
  [[nodiscard]] inline ulint get_n_user_cols() const noexcept {
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    return m_n_cols - DATA_N_SYS_COLS;
  }

  /**
   * Gets the number of system columns in a table in the dictionary cache.
   * 
   * @return number of system (e.g., ROW_ID) columns of a table
   */
  [[nodiscard]] constexpr inline ulint get_n_sys_cols() const noexcept {
    return DATA_N_SYS_COLS;
  }

  /** Gets the nth column of a table.
   * 
   * @param[in] pos position of column
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline const Column *get_nth_col(ulint pos) const noexcept {
    ut_ad(pos < m_n_fields);
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    return &m_cols[pos];
  }

  /** Gets the nth column of a table.
   * 
   * @param[in] pos position of column
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline Column *get_nth_col(ulint pos) noexcept {
    return const_cast<Column *>(static_cast<const Table *>(this)->get_nth_col(pos));
  }

  /** Gets the given system column number of a table.
   * 
   * @param[in] sys DATA_ROW_ID, ...
   * 
   * @return column number
   */
  [[nodiscard]] inline ulint get_sys_col_no(ulint sys_col_no) const noexcept {
    ut_ad(sys_col_no < DATA_N_SYS_COLS);
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    return m_n_cols - DATA_N_SYS_COLS + sys_col_no;
  }

  /** Gets the given system column of a table.
   * 
   * @param[in] sys DATA_ROW_ID, ...
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline const Column *get_sys_col(ulint sys_col_no) const noexcept {
    ut_ad(sys_col_no < DATA_N_SYS_COLS);
    ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

    auto col = get_nth_col(get_sys_col_no(sys_col_no));

    ut_ad(col->mtype == DATA_SYS);
    ut_ad(col->prtype == (sys_col_no | DATA_NOT_NULL));

    return col;
  }

  /** Gets the given system column of a table.
   * 
   * @param[in] sys DATA_ROW_ID, ...
   * 
   * @return pointer to column object
   */
  [[nodiscard]] inline Column *get_sys_col(ulint sys_col_no) noexcept {
    return const_cast<Column *>(static_cast<const Table *>(this)->get_sys_col(sys_col_no));
  }

  /**
   * Determine the file format of a table.
   * 
   * @return file format version
   */
  [[nodiscard]] inline ulint get_format() const noexcept {
    return (m_flags & DICT_TF_FORMAT_MASK) >> DICT_TF_FORMAT_SHIFT;
  }

  /**
   * Sets the file format of a table.
   * 
   * @param[in] format file format version
   */
  inline void set_format(ulint format) noexcept {
    m_flags = (m_flags & ~DICT_TF_FORMAT_MASK) | (format << DICT_TF_FORMAT_SHIFT);
  }

  /**
   * @brief Adds a column definition to a table.
   * 
   * @param[in] name The column name, or nullptr.
   * @param[in] mtype The main datatype.
   * @param[in] prtype The precise type.
   * @param[in] len The precision.
   */
  void add_col(const char *name, ulint mtype, ulint prtype, ulint len) noexcept;

  /**
   * @brief Checks if a table is referenced by a foreign key.
   * 
   * @return True if the table is referenced by a foreign key, false otherwise.
   */
  [[nodiscard]] inline bool is_referenced_by_foreign_key() const noexcept {
    return !m_referenced_list.empty();
  }

  /**
   * @brief Gets the referenced constraint for an index.
   * 
   * @param[in] index The index.
   * 
   * @return The referenced constraint, or nullptr if not found.
   */
  [[nodiscard]] inline Foreign *find_referenced_constraint(Index *index) noexcept {
    for (auto foreign : m_referenced_list) {

      if (foreign->m_referenced_index == index) {

        return foreign;
      }
    }

    return nullptr;
  }

  /**
   * @brief Gets the foreign constraint for an index.
   * 
   * @param[in] index The index.
   * 
   * @return The foreign constraint, or nullptr if not found.
   */
  [[nodiscard]] inline Foreign *find_foreign_constraint(Index *index) noexcept {
    for (auto foreign : m_foreign_list) {

      if (foreign->m_foreign_index == index || foreign->m_referenced_index == index) {

        return foreign;
      }
    }

    return nullptr;
  }

  /**
   * @brief Looks for the foreign constraint from the foreign and referenced lists of a table.
   * 
   * @param[in] id The foreign constraint id.
   * 
   * @return The foreign constraint.
   */
  [[nodiscard]] inline Foreign *find_foreign_constraint(const char *id) noexcept {
    for (auto foreign : m_foreign_list) {
      if (strcmp(id, foreign->m_id) == 0) {

        return foreign;
      }
    }

    for (auto foreign : m_referenced_list) {
      if (strcmp(id, foreign->m_id) == 0) {

        return foreign;
      }
    }

    return nullptr;
  }

  /**
   * @brief Returns an index object by matching on the name and column names, and if more than
   * one index matches, returns the index with the max id.
   * 
   * @param[in] name The index name to find.
   * @param[in] columns The array of column names.
   * @param[in] n_cols The number of columns.
   * 
   * @return The matching index, or NULL if not found.
   */
  [[nodiscard]] Index *get_index_by_max_id(const char *name, const char **columns, ulint n_cols) noexcept;
  
  /**
   * @brief Tries to find an index whose first fields are the columns in the array,
   * in the same order and is not marked for deletion and is not the same
   * as types_idx.
   * 
   * @param[in] columns The array of column names.
   * @param[in] n_cols The number of columns.
   * @param[in] types_idx The index to whose types the column types must match, or nullptr.
   * @param[in] check_charsets Whether to check charsets. Only has an effect if types_idx != nullptr.
   * @param[in] check_null Nonzero if none of the columns must be declared NOT nullptr.
   * 
   * @return The matching index, or nullptr if not found.
   */
  [[nodiscard]] Index *foreign_find_index(const char **columns, ulint n_cols, Index *types_idx, bool check_charsets, bool check_null) noexcept;

  /**
   * @brief Gets an index by its name.
   * 
   * @param[in] name The index name.
   * 
   * @return The index, or nullptr if not found.
   */
  [[nodiscard]] inline Index *get_index_on_name(const char *name) noexcept {
    for (auto index : m_indexes) {
      if (strcmp(index->m_name, name) == 0) {
        return index;
      }
    }

    return nullptr;
  }

  /**
   * @brief Replaces an index in the foreign list.
   * 
   * @param[in] index The index to replace.
   */
  void replace_index_in_foreign_list(Index *index) noexcept {
    for (auto foreign : m_foreign_list) {
      if (foreign->m_foreign_index == index) {
        auto new_index = foreign->find_equiv_index();
        ut_a(new_index != nullptr);

        foreign->m_foreign_index = new_index;
      }
    }
  }

  /**
   * @brief Gets the foreign constraint for an index.
   * 
   * @param[in] index The index.
   * 
   * @return The foreign constraint, or nullptr if not found.
   */
  [[nodiscard]] inline Foreign *get_foreign_constraint(Index *index) noexcept {
    for (auto foreign : m_foreign_list) {

      if (foreign->m_foreign_index == index || foreign->m_referenced_index == index) {

        return foreign;
      }
    }

    return nullptr;
  }

  /**
   * @brief Gets the index with the matching name and the minimum id.
   * 
   * @param[in] name The index name.
   * 
   * @return The index, or nullptr if not found.
   */
  [[nodiscard]] Index *get_index_on_name_and_min_id(const char *name) noexcept {
    /* Index with matching name and min(id) */
    Index *min_index{};

    for (auto index : m_indexes) {
      if (strcmp(index->m_name, name) == 0) {
        if (min_index == nullptr || index->m_id < min_index->m_id) {
          min_index = index;
        }
      }
    }

    return min_index;
  }

  /**
   * @brief Gets the number of columns in a table.
   * 
   * @return The number of columns.
   */
  [[nodiscard]] inline ulint get_n_cols() const noexcept {
    return m_n_cols;
  }

  /**
   * Finds all the columns of the index in the table and links the
   * 
   *  Index::Field[N]:Column  = Table::Column.
   *
   * @param[in] index Index
   *
   * @return true if the column names were found
   */
  [[nodiscard]] bool link_cols(Index *index) noexcept;

  /**
   * Copies types of columns contained in table to tuple and sets all fields of the
   * tuple to the SQL NULL value. This function should be called right after dtuple_create().
   * 
   * @param[in] tuple The data tuple.
   */
  void copy_types(DTuple *tuple) const noexcept;

  // Disable copying and moving
  Table(Table &&) = delete;
  Table(const Table&) = delete;
  Table &operator=(Table &&) = delete;
  Table &operator=(const Table&) = delete;

  /**
   * @brief Data structure for table statistics.
   */
  struct Stats {
    /** Approximate number of rows in the table; we periodically calculate
    new estimates */
    int64_t m_n_rows{};

    /** Approximate clustered index size in database pages */
    page_no_t m_clustered_index_size{};

    /** Other indexes in database pages */
    page_no_t m_sum_of_secondary_index_sizes{};

    /** When a row is inserted, updated, or deleted, we add 1 to this
    number; we calculate new estimates for the stat_...  values for
    the table and the indexes at an interval of 2 GB or when about 1 / 16
    of table has been modified; also when an estimate operation is called
    for; the counter is reset to zero at statistics calculation; this
    counter is not protected by any latch, because this is only used
    for heuristics */
    ulint m_modified_counter{};

    /** true if statistics have been calculated the first time after database
    startup or table creation */
    bool m_initialized{};
  };

  /** DICT_TF_COMPACT, ... */
  unsigned m_flags : DICT_TF2_BITS;

  /** True if this is in a single-table tablespace and the .ibd file is
  missing; then we must return an error if the user tries
  to query such an orphaned table */
  unsigned m_ibd_file_missing : 1;

  /** This flag is set true when the user calls DISCARD TABLESPACE on this
  table, and reset to false in IMPORT TABLESPACE */
  unsigned m_tablespace_discarded : 1;

  /** true if the table object has been added to the dictionary cache */
  unsigned m_cached : 1;

  /** flag: true if the maximum length of a single row exceeds BIG_ROW_SIZE;
  initialized in dict_table_add_to_cache() */
  unsigned m_big_rows : 1;

  /** Number of columns defined so far */
  unsigned m_n_fields : 10;

  /** Number of columns */
  unsigned m_n_cols : 10;

  /** Statistics for query optimization */
  /* @{ */

  /** Id of the table */
  Dict_id m_id;

  /** Memory heap */
  mem_heap_t *m_heap;

  /** Table name */
  const char *m_name;

  /** nullptr or the directory path where a TEMPORARY table that was explicitly
  created by a user should be placed if innodb_file_per_table is defined; in
  Unix this is usually /tmp/... */
  const char *m_dir_path_of_temp_table;

  /** Tablepace where the clustered index of the table is placed */
  space_id_t m_space_id;

  /** Array of column descriptions */
  const Column *m_cols;

  /** List of indexes of the table */
  UT_LIST_BASE_NODE_T(Index, m_indexes) m_indexes;

  /** List of foreign key constraints in the table; these refer to columns
  in other tables */
  UT_LIST_BASE_NODE_T(Foreign, m_foreign_list) m_foreign_list;

  /** List of foreign key constraints which refer to this table */
  UT_LIST_BASE_NODE_T(Foreign, m_referenced_list) m_referenced_list;

  /** Node of the LRU list of tables */
  UT_LIST_NODE_T(Table) m_table_LRU;

  /** Count of how many handles the user has opened to this table; dropping
  of the table is NOT allowed until this count gets to zero */
  uint32_t m_n_handles_opened;

  /** Count of how many foreign key check operations are currently being performed
  on the table: we cannot drop the table while there are foreign key checks running
  on it! */
  uint32_t m_n_foreign_key_checks_running;

  /** List of locks on the table */
  Table_locks m_locks;

  /** This field is used to specify in simulations tables which are so big
  that disk should be accessed: disk access is simulated by putting the
  thread to sleep for a while; NOTE that this flag is not stored to the data
  dictionary on disk, and the database will forget about value true if it has
  to reload the table definition from disk */
  IF_DEBUG(bool m_does_not_fit_in_memory;)

  /** Table statistics */
  Stats m_stats;
  
  /* @} */

  /** Value of dict_table_struct::magic_n */
  IF_DEBUG(ulint m_magic_n;)
};

inline void Foreign::drop_constraint() noexcept {
  ut_ad(m_referenced_table != nullptr);

  UT_LIST_REMOVE(m_referenced_table->m_referenced_list, this);

  if (m_foreign_table != nullptr) {
    UT_LIST_REMOVE(m_foreign_table->m_foreign_list, this);
  }
}

[[nodiscard]] inline const char *Index::get_table_name() const noexcept {
  return m_table->m_name;
}

[[nodiscard]] inline ulint Index::get_min_size() const noexcept {
  ulint size{};
  ulint n{get_n_fields()};

  while (n--) {
    size += col_get_min_size(m_table->get_nth_col(n));
  }

  return size;
}

[[nodiscard]] inline ulint Index::get_sys_col_field_pos(ulint type) const noexcept {
  ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

  if (is_clustered()) {
    return get_clustered_field_pos(m_table->get_sys_col(type));
  } else {
    return get_nth_field_pos(m_table->get_sys_col_no(type));
  }
}

[[nodiscard]] inline Index *Foreign::find_equiv_index() const noexcept {
  /* first fields and in the right order, and the types are the
  same as in foreign->foreign_index */

  return m_foreign_table->foreign_find_index(
    m_foreign_col_names,
    m_n_fields,
    m_foreign_index,
    true, /* check types */
    false /* allow columns to be nullptr */
  );
}

[[nodiscard]] inline const Column *Index::get_nth_col(ulint pos) const noexcept {
  ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);
  ut_a(pos < m_n_defined);

  return m_fields[pos].m_col;
}

[[nodiscard]] inline Column *Index::get_nth_col(ulint pos) noexcept {
  return const_cast<Column *>(static_cast<const Index *>(this)->get_nth_col(pos));
}

[[nodiscard]] inline const Index *Index::get_clustered_index() const noexcept {
  return m_table->get_clustered_index();
}

[[nodiscard]] inline Index *Index::get_clustered_index() noexcept {
  return const_cast<Index *>(static_cast<const Index *>(this)->get_clustered_index());
}
/** Table create node structure */
struct Table_node {
  /**
   * Creates a new table node.
   * 
   * @param[in] dict The dictionary.
   * @param[in] table The table.
   * @param[in] heap The memory heap.
   * @param[in] commit Whether to commit the work.
   * 
   * @return The table node.
   */
  [[nodiscard]] static Table_node *create(Dict *dict, Table *table, mem_heap_t *heap, bool commit) noexcept;

  /** Node type: QUE_NODE_TABLE_CREATE */
  que_common_t m_common;

   /** Table to create, built as a memory data structure
    * with dict_mem_... functions */
  Table *m_table{};

  /** Child node which does the insert of the table definition;
   * the row to be inserted is built by the parent node  */
  Insert_node *m_tab_def{};

  /** Child node which does the inserts of the column definitions;
   * the row to be inserted is built by the parent node  */
  Insert_node *m_col_def{};

  /** Child node which performs a commit after a successful table creation */
  Commit_node *m_commit_node{};

  /*----------------------*/
  /* Local storage for this graph node */

  /** Node execution state */
  ulint m_state{};

  /** Next column definition to insert */
  ulint m_col_no{};

  /** Memory heap used as auxiliary storage */
  mem_heap_t *m_heap{};
};

/* Table create node states */
constexpr ulint TABLE_BUILD_TABLE_DEF = 1;
constexpr ulint TABLE_BUILD_COL_DEF = 2;
constexpr ulint TABLE_COMMIT_WORK = 3;
constexpr ulint TABLE_ADD_TO_CACHE = 4;
constexpr ulint TABLE_COMPLETED = 5;

/* Index create node struct */
struct Index_node {
  /** 
   * Creates a new index node.
   * 
   * @param[in] dict The dictionary.
   * @param[in] index The index.
   * @param[in] heap The memory heap.
   * @param[in] commit Whether to commit the work.
   * 
   * @return The index node.
   */
  [[nodiscard]] static Index_node *create(Dict *dict, Index *index, mem_heap_t *heap, bool commit) noexcept;

  /** Node type: QUE_NODE_INDEX_CREATE */
  que_common_t m_common;

  /** Index to create, built as a memory data structure with dict_mem_... functions */
  Index *m_index{};

  /* Child node which does the insert of the index definition;
   * the row to be inserted is built by the parent node  */
  Insert_node *m_ind_def{};

  /** Child node which does the inserts of the field definitions;
   * the row to be inserted is built by the parent node  */
  Insert_node *m_field_def{};

  /** Child node which performs a commit after a successful index creation */
  Commit_node *m_commit_node{};

  /*----------------------*/
  /* Local storage for this graph node */

  /** Node execution state */
  ulint m_state{};

  /** Root page number of the index */
  page_no_t m_page_no{NULL_PAGE_NO};

   /** Table which owns the index */
  Table *m_table{};

  /** Index definition row built */
  DTuple *m_ind_row{};

  /** Next field definition to insert */
  ulint m_field_no{};

  /** Memory heap used as auxiliary storage */
  mem_heap_t *m_heap{};
};

/* Index create node states */
constexpr ulint INDEX_BUILD_INDEX_DEF = 1;
constexpr ulint INDEX_BUILD_FIELD_DEF = 2;
constexpr ulint INDEX_CREATE_INDEX_TREE = 3;
constexpr ulint INDEX_COMMIT_WORK = 4;
constexpr ulint INDEX_ADD_TO_CACHE = 5;