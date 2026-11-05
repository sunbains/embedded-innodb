/****************************************************************************
Copyright (c) 2008, 2009 Innobase Oy. All rights reserved.
Copyright (c) 2008, 2009 Oracle. All rights reserved.
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file include/api0api.h
The C++ client interface: cursors, tuples, schemas and transactions on top of
a System. Per-object classes reach the engine through a System& parameter. */

#pragma once

#include "api0types.h"
#include "innodb0types.h"
#include "ut0hlc.h"

#include <functional>
#include <vector>

class System;
struct DTuple;
struct Index;
struct InsertNode;
struct MemHeap;
struct Prebuilt;
struct QueFork;
struct SelNode;
struct Table;
struct Trx;
struct UpdNode;

namespace api {

/** Length of a column value that is SQL NULL. */
constexpr uint32_t IB_SQL_NULL = 0xFFFFFFFF;

/** The number of system columns in a row. */
constexpr ulint IB_N_SYS_COLS = 3;

/** The maximum length of a text column. */
constexpr ulint MAX_TEXT_LEN = 4096;

/** Maximum length of a column name, in bytes. */
constexpr ulint IB_MAX_COL_NAME_LEN = 64 * 3;

/** Maximum length of a table name, in bytes. */
constexpr ulint IB_MAX_TABLE_NAME_LEN = 64 * 3;

/** Tuple types. */
enum TupleType {
  /** Data row tuple */
  TPL_ROW,

  /** Index key tuple */
  TPL_KEY
};

/** Query types supported. */
enum QueryType {
  /** None/Sentinel */
  QRY_NON,

  /** Insert operation */
  QRY_INS,

  /** Update operation */
  QRY_UPD,

  /** Select operation */
  QRY_SEL
};

/** Query graphs of a cursor. */
struct QueryGraph {
  /** SQL query graph used in inserts */
  QueFork *m_ins{};

  /** SQL query graph used in updates or deletes */
  QueFork *m_upd{};

  /** Dummy query graph used in selects */
  QueFork *m_sel{};
};

/** Query nodes of a cursor. */
struct QueryNode {
  /** SQL insert node used to perform inserts to the table */
  InsertNode *m_ins{};

  /** SQL update node used to perform updates and deletes */
  UpdNode *m_upd{};

  /** SQL select node used to perform selects on the table */
  SelNode *m_sel{};
};

/** Query processing state of a cursor. */
struct QueryProc {
  QueryNode m_node{};
  QueryGraph m_grph{};
};

/** Table columns used during table and index schema creation. */
struct Col {
  /** Name of column */
  const char *m_name{};

  /** Main type of the column */
  ColType m_col_type{};

  /** Length of the column */
  ulint m_len{};

  /** Column attributes */
  ColAttr m_col_attr{};
};

/** Index columns used during index and index schema creation. */
struct KeyCol {
  /** Name of column */
  const char *m_name{};

  /** Column index prefix len or 0 */
  ulint m_prefix_len{};
};

using SchemaVisitorTableAll = std::function<int(void *arg, const char *name, int name_len)>;

struct SchemaVisitor {
  /** Currently, this is also the number of callback functions in the struct. */
  enum Version { TABLE = 1, TABLE_COL = 2, TABLE_AND_INDEX = 3, TABLE_AND_INDEX_COL = 4 };

  /** Table visitor */
  /**
  * @brief Function pointer type for visiting schema tables.
  *
  * This function pointer type is used for visiting schema tables in the embedded InnoDB library.
  * The visitor function should return 0 on success or a nonzero value on failure to abort the traversal.
  *
  * @param arg User callback argument.
  * @param name The name of the table being visited.
  * @param tbl_fmt The type of the table.
  * @param page_size The page size of the table.
  * @param n_cols The number of columns defined in the table.
  * @param n_indexes The number of indexes defined in the table.
  * @return 0 on success, nonzero on failure.
  */
  using table_t = std::function<int(void *arg, const char *name, TableFormat tbl_fmt, ulint page_size, int n_cols, int n_indexes)>;

  /** Table column visitor */
  /**
  * @brief Function pointer type for visiting table columns in a schema.
  *
  * This function pointer type is used for traversing and visiting table columns in a schema.
  * The visitor function should return 0 on success and a nonzero value on failure to abort the traversal.
  *
  * @param arg User callback argument.
  * @param name The name of the column.
  * @param col_type The type of the column.
  * @param len The length of the column.
  * @param attr The attributes of the column.
  * @return 0 on success, nonzero on failure.
  */
  using table_col_t = std::function<int(void *arg, const char *name, ColType col_type, ulint len, ColAttr attr)>;

  /** Index visitor */
  /**
  * @brief Function pointer type for visiting indexes in the schema.
  *
  * This function pointer type is used for visiting indexes in the schema.
  * The visitor function should return 0 on success and nonzero on failure
  * to abort the traversal.
  *
  * @param arg       User callback argument.
  * @param name      The name of the index.
  * @param clustered True if the index is clustered.
  * @param unique    True if the index is unique.
  * @param n_cols    The number of columns defined in the index.
  *
  * @return 0 on success, nonzero on failure.
  */
  using index_t = std::function<int(void *arg, const char *name, bool clustered, bool unique, int n_cols)>;

  /** Index column visitor */
  /**
  * @brief Function pointer type for visiting index columns in the InnoDB schema.
  *
  * This function pointer type is used to define a callback function for visiting index columns in the InnoDB schema.
  * The callback function should return 0 on success and a nonzero value on failure to abort the traversal.
  *
  * @param arg The user callback argument.
  * @param name The name of the column being visited.
  * @param prefix_len The length of the prefix.
  *
  * @return 0 on success, nonzero on failure.
  */
  using index_col_t = std::function<int(void *arg, const char *name, ulint prefix_len)>;

  /** Visitor version */
  Version version;

  /** For travesing table info */
  table_t table;

  /** For travesing table column info */
  table_col_t table_col;

  /** For travesing index info */
  index_t index;

  /** For travesing index column info */
  index_col_t index_col;
};

struct TableStats {
  /** Approximate number of rows in the table; we periodically calculate new estimates */
  int64_t m_stat_n_rows;

  /** Approximate clustered index size in bytes. */
  uint64_t m_stat_clustered_index_size;

  /** Other indexes in bytes */
  uint64_t m_stat_sum_of_other_index_sizes;

  /** When a row is inserted, updated, or deleted, we add 1 to this number; we calculate
   * new estimates for the stat_...  values for the table and the indexes at an interval
   * of 2 GB or when about 1 / 16 of table has been modified; also when an estimate operation
   * is called for; the counter is reset to zero at statistics calculation; this counter is
   * not protected by any latch, because this is only used for heuristics */
  uint64_t m_stat_modified_counter;
};

/** A row or a key of an index. */
struct Tuple {
  /** Set a column of the tuple. Make a copy using the tuple's heap.
   *
   * @ingroup dml
   * @param col_no is the column index in the tuple
   * @param src is the data value to set
   * @param len is the data value (src) length in bytes
   * @return  DB_SUCCESS or error code */
  [[nodiscard]] DbErr set_value(ulint col_no, const void *src, ulint len) noexcept;

  /** Get the size of the data available in the column the tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @return  bytes avail or IB_SQL_NULL */
  [[nodiscard]] ulint get_len(ulint i) const noexcept;

  /** Copy a column value from the tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] dst is where the data will be copied
   * @param len is the maximum number of bytes that can be copied to dst
   * @return  bytes copied or IB_SQL_NULL */
  [[nodiscard]] ulint copy_value(ulint i, void *dst, ulint len) const noexcept;

  /** Get a column value pointer from the tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @return  nullptr or pointer to buffer */
  [[nodiscard]] const void *get_value(ulint i) const noexcept;

  /** Get a column type, length and attributes from the tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] col_meta the column meta data
   * @return  len of column data */
  ulint get_meta(ulint i, ColMeta *col_meta) const noexcept;

  /** "Clear" or reset an InnoDB tuple. We free the heap and recreate the tuple.
   *
   * @ingroup tuple
   * @return  new tuple, or nullptr */
  [[nodiscard]] Tuple *clear() noexcept;

  /** Copy the contents of  source tuple to destination tuple. The tuples
   * must be of the same type and belong to the same table/index.
   *
   * @ingroup tuple
   * @param src_tpl is the source tuple
   * @return  DB_SUCCESS or error code */
  [[nodiscard]] DbErr copy(const Tuple *src_tpl) noexcept;

  /** Return the number of user columns in the tuple definition.
   *
   * @ingroup tuple
   * @return  number of user columns */
  [[nodiscard]] ulint get_n_user_cols() const noexcept;

  /** Return the number of columns in the tuple definition.
   *
   * @ingroup tuple
   * @return  number of columns */
  [[nodiscard]] ulint get_n_cols() const noexcept;

  /** Read a signed int 8 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_i8(ulint i, int8_t *ival) const noexcept;

  /** Read an unsigned int 8 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_u8(ulint i, uint8_t *ival) const noexcept;

  /** Read a signed int 16 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_i16(ulint i, int16_t *ival) const noexcept;

  /** Read an unsigned int 16 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_u16(ulint i, uint16_t *ival) const noexcept;

  /** Read a signed int 32 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_i32(ulint i, int32_t *ival) const noexcept;

  /** Read an unsigned int 32 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_u32(ulint i, uint32_t *ival) const noexcept;

  /** Read a signed int 64 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_i64(ulint i, int64_t *ival) const noexcept;

  /** Read an unsigned int 64 bit column from an InnoDB tuple.
   *
   * @ingroup dml
   * @param i is the index (ordinal position) of the column within the tuple
   * @param[out] ival is the integer value
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_u64(ulint i, uint64_t *ival) const noexcept;

  /** Read a double column value from an InnoDB tuple.
   *
   * @ingroup dml
   * @param col_no is the column number to read
   * @param[out] dval is where the value is copied
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_double(ulint col_no, double *dval) const noexcept;

  /** Read a float value from an InnoDB tuple.
   *
   * @ingroup dml
   * @param col_no is the column number to read
   * @param[out] fval is where the value is copied
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr read_float(ulint col_no, float *fval) const noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_i8(int col_no, int8_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_u8(int col_no, uint8_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_i16(int col_no, int16_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_u16(int col_no, uint16_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_i32(int col_no, int32_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_u32(int col_no, uint32_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_i64(int col_no, int64_t val) noexcept;

  /** Write an integer value to a column. Integers are stored in big-endian
   * format and will need to be converted from the host format.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCESS or error */
  [[nodiscard]] DbErr write_u64(int col_no, uint64_t val) noexcept;

  /** Write a double value to a column.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr write_double(int col_no, double val) noexcept;

  /** Write a float value to a column.
   *
   * @ingroup dml
   * @param col_no is the column number to update
   * @param val is the value to write
   * @return  DB_SUCCESS or error */
  [[nodiscard]] DbErr write_float(int col_no, float val) noexcept;

  /** Destroy an InnoDB tuple.
   *
   * @ingroup tuple
   * @param tpl is the tuple instance to delete */
  static void destroy(Tuple *tpl) noexcept;

  /** Heap used to build this and for copying the column values. */
  MemHeap *m_heap{};

  /** Tuple discriminator. */
  TupleType m_type{};

  /** Index for tuple can be either secondary or cluster index. */
  const Index *m_index{};

  /** The internal tuple instance */
  DTuple *m_ptr{};
};

/** A cursor for traversing tables and indexes. */
struct Cursor {
  /** Set to true if it's a simple select.
   *
   * @ingroup sql
   */
  void set_simple_select() noexcept;

  /** Reset the cursor.
   *
   * @ingroup cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr reset(System &sys) noexcept;

  /** Insert a row to a table.
   *
   * @ingroup dml
   * @param tpl is the tuple to insert
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr insert_row(System &sys, const Tuple *tpl) noexcept;

  /** Update a row in a table.
   *
   * @ingroup dml
   * @param old_tpl is the old tuple in the table
   * @param new_tpl is the new tuple with the updated values
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr update_row(System &sys, const Tuple *old_tpl, const Tuple *new_tpl) noexcept;

  /** Delete a row in a table.
   *
   * @ingroup dml
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr delete_row(System &sys) noexcept;

  /** Read current row.
   *
   * @ingroup dml
   * @param[out] tpl is the tuple to read the column values
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr read_row(System &sys, Tuple *tpl) noexcept;

  /** Move cursor to the prev user record in the table.
   *
   * @ingroup cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr prev(System &sys) noexcept;

  /** Move cursor to the next user record in the table.
   *
   * @ingroup cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr next(System &sys) noexcept;

  /** Move cursor to the first record in the table.
   *
   * @ingroup cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr first(System &sys) noexcept;

  /** Move cursor to the last record in the table.
   *
   * @ingroup cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr last(System &sys) noexcept;

  /** Search for key.
   *
   * @ingroup cursor
   * @param tpl is a key to search for
   * @param srch_mode is the search mode
   * @param[out] result is -1, 0 or 1 depending on tuple eq or gt than the current row
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr moveto(System &sys, Tuple *tpl, SearchMode srch_mode, int *result) noexcept;

  /** Attach the cursor to the transaction. The cursor must not already be
   * attached to another transaction.
   *
   * @ingroup cursor
   * @param trx is the transaction to attach to the cursor */
  void attach_trx(System &sys, Trx *trx) noexcept;

  /** Set the match mode for cursor_move().
   *
   * @ingroup cursor
   * @param match_mode is the match mode to set */
  void set_match_mode(MatchMode match_mode) noexcept;

  /** Check if cursor is positioned.
   *
   * @ingroup cursor
   * @return  true if positioned */
  [[nodiscard]] bool is_positioned() const noexcept;

  /** Lock an InnoDB cursor/table.
   *
   * @ingroup trx
   * @param lck_mode is the lock mode
   * @return  DB_SUCCESS or error code */
  [[nodiscard]] DbErr lock(System &sys, CursorLockMode lck_mode) noexcept;

  /** Set the Lock mode of the cursor.
   *
   * @ingroup trx
   * @param  lck_mode is the lock mode
   * @return  DB_SUCCESS or error code */
  [[nodiscard]] DbErr set_lock_mode(System &sys, CursorLockMode lck_mode) noexcept;

  /** Set need to access clustered index record flag.
   *
   * @ingroup dml
   */
  void set_cluster_access() noexcept;

  /** Inform the cursor that it's the start of an SQL statement.
   *
   * @ingroup cursor
   */
  void stmt_begin() noexcept;

  /** Open an InnoDB secondary index cursor and return a cursor handle to it.
   *
   * @ingroup cursor
   * @param index_name is the name of the index
   * @param[out] crsr is the new cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr open_index(System &sys, const char *index_name, Cursor **crsr) noexcept;

  /** Create an InnoDB tuple used for index/table search.
   *
   * @ingroup tuple
   * @return tuple for current index */
  [[nodiscard]] Tuple *sec_search_tuple_create() noexcept;

  /** Create an InnoDB tuple used for index/table search.
   *
   * @ingroup tuple
   * @return  tuple for current index */
  [[nodiscard]] Tuple *sec_read_tuple_create() noexcept;

  /** Create an InnoDB tuple used for table key operations.
   *
   * @ingroup tuple
   * @return  tuple for current table */
  [[nodiscard]] Tuple *clust_search_tuple_create() noexcept;

  /** Create an InnoDB tuple for table row operations.
   *
   * @ingroup tuple
   * @return  tTuple for current table */
  [[nodiscard]] Tuple *clust_read_tuple_create() noexcept;

  /** Create a new cluster key search tuple and copy the contents of  the
   * secondary index key tuple columns that refer to the cluster index record
   * to the cluster key. It does a deep copy of the column data.
   *
   * @ingroup tuple
   * @param[out] dst_tpl is the tuple where the key data will be copied
   * @param src_tpl is the source secondary index tuple to copy from
   * @return  DB_SUCCESS or error code */
  [[nodiscard]] DbErr get_cluster_key(Tuple **dst_tpl, const Tuple *src_tpl) noexcept;

  /** Get table statistics.
   *
   * This function will fill out the provided \ref TableStats with
   * statistics about the table on the currently opened \ref Cursor *
   *
   * @ingroup misc
   * @param table_stats a \ref TableStats to be filled out by InnoDB
   * @param sizeof_ib_table_stats_t sizeof(TableStats). This allows for ABI compatible changes to the size of TableStats.
   * @returns \ref DB_SUCCESS or error */
  [[nodiscard]] DbErr get_table_statistics(System &sys, TableStats *table_stats) noexcept;

  /** Get statistics on number of different key values per index part
   *
   * This function returns the approximate different key
   * values for this index. They are periodically recalculated.
   *
   * @ingroup misc
   * @param index_name name of the index
   * @param ncols returns the number of elements in n_diff
   * @param n_diff An array allocated with malloc() (user needs to free()) containing the statistics
   * @returns \ref DB_SUCCESS or error. \ref DB_NOT_FOUND if index is not found */
  [[nodiscard]] DbErr get_index_stat_n_diff_key_vals(
    System &sys, const char *index_name, uint64_t *ncols, int64_t **n_diff
  ) noexcept;

  /** Force an update of table and index statistics
   *
   * This function forces an update to the table and index statistics for the table crsr is opened on.
   *
   * @ingroup misc
   * @returns \ref DB_SUCCESS or error.  */
  [[nodiscard]] DbErr update_table_statistics(System &sys) noexcept;

  /** Close an InnoDB table and free the cursor.
   *
   * @ingroup cursor
   * @param crsr is an open cursor
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] static DbErr close(System &sys, Cursor *crsr) noexcept;

  /** Instance heap */
  MemHeap *m_heap{};

  /** Heap to use for query graphs */
  MemHeap *m_query_heap{};

  /** Query processing info */
  QueryProc m_q_proc{};

  /** moveto() match mode */
  MatchMode m_match_mode{};

  /** For reading rows */
  Prebuilt *m_prebuilt{};
};

struct TableSchema;

/** Index schema used during index creation. */
struct IndexSchema {
  /** Add columns to an index schema definition.
   *
   * @ingroup ddl
   * @param name is the name of the column to add to the index schema
   * @param prefix_len is the prefix length of the index or 0 if no prefix
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr add_col(const char *name, ulint prefix_len) noexcept;

  /** Set index as clustered index. Implies UNIQUE.
   *
   * @ingroup ddl
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr set_clustered() noexcept;

  /** Set index as a unique index.
   *
   * @ingroup ddl
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr set_unique() noexcept;

  /** Create a secondary index. The index id encodes the table id in the high
   * 4 bytes and the index id in the lower 4 bytes.
   *
   * @ingroup ddl
   * @param[out] index_id is the new index id that was created
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr create_index(System &sys, uint64_t *index_id) noexcept;

  /** Destroy an index schema.
   *
   * @ingroup ddl
   * @param idx_sch is the index schema to delete */
  static void destroy(IndexSchema *idx_sch) noexcept;

  /** Create an index schema instance.
   *
   * @ingroup ddl
   * @param usr_trx is the current user transaction
   * @param name is the name of the index to create
   * @param table_name is the name of the table the index belongs to
   * @param[out] idx_sch is the newly created index schema instance
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] static DbErr create(
    System &sys, Trx *usr_trx, const char *name, const char *table_name, IndexSchema **idx_sch
  ) noexcept;

  /** Heap used to build this and all its columns in the list */
  MemHeap *m_heap{};

  /** Index name */
  const char *m_name{};

  /** Parent table */
  Table *m_table{};

  /** Parent table schema that owns this instance */
  TableSchema *m_schema{};

  /** True if clustered index */
  bool m_clustered{};

  /** True if unique index */
  bool m_unique{};

  /** Vector of columns */
  std::vector<KeyCol *> *m_cols{};

  /** User transaction covering the DDL operations */
  Trx *m_usr_trx{};
};

/** Table schema used during table creation. */
struct TableSchema {
  /** Add columns to a table schema. Tables are created in InnoDB by first
  * creating a table schema which is identified by a handle. Then you
  * add the column definitions to the table schema.
  *
  * @ingroup ddl
  * @param name is the name of the column to add
  * @param col_type is the type of the column
  * @param col_attr are the attributes of the column, including constraints
  * @param client_type is any 16 bit number relevant only to the client
  * @param len is the maximum length of the column
  * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr add_col(const char *name, ColType col_type, ColAttr col_attr, uint16_t client_type, ulint len) noexcept;

  /** Create and add an index key definition to a table schema. The index
  * schema is owned by the table schema instance and will be freed when
  * the table schema instance is freed.
  *
  * @ingroup ddl
  * @param name name of the key definition to create
  * @param[out] idx_sch is the key definition schema instance
  * @return  DB_SUCCESS or err code */
  [[nodiscard]] DbErr add_index(const char *name, IndexSchema **idx_sch) noexcept;

  /** Destroy a schema. The handle is freed by this function.
  *
  * @ingroup ddl
  * @param tbl_sch is the table schema to delte*/
  static void destroy(System &sys, TableSchema *tbl_sch) noexcept;

  /** Create a table schema.
   *
   * @ingroup ddl
   * @param name is the table name for which to create the schema
   * @param[out] tbl_sch is the schema instance that is created
   * @param tbl_fmt is the format of the table to be created
   * @param page_size is the page size for the table or 0 for default
   *
   * @return  DB_SUCCESS or err code */
  [[nodiscard]] static DbErr create(const char *name, TableSchema **tbl_sch, TableFormat tbl_fmt, ulint page_size) noexcept;

  /** Heap used to build this and all its columns in the list */
  MemHeap *m_heap{};

  /** Table name */
  const char *m_name{};

  /** Row format */
  TableFormat m_tbl_fmt{};

  /** Page size */
  ulint m_page_size{};

  /** Vector of columns */
  std::vector<Col *> *m_cols{};

  /** Vector of indexes */
  std::vector<IndexSchema *> *m_indexes{};

  /** Table read from or nullptr */
  Table *m_table{};
};

/** Start a transaction that's been rolled back. This special function
 * exists for the case when InnoDB's deadlock detector has rolledack
 * a transaction. While the transaction has been rolled back the handle
 * is still valid and can be reused by calling this function. If you
 * don't want to reuse the transaction handle then you can free the handle
 * by calling trx_release().
 *
 * @ingroup trx
 * @param trx is the transaction to restart
 * @param trx_level is the transaction isolation level
 * @return  innobase txn handle */
[[nodiscard]] DbErr trx_start(System &sys, Trx *trx, TrxLevel trx_level) noexcept;

/** Begin a transaction. This will allocate a new transaction handle and
 * put the transaction in the active state.
 *
 * @ingroup trx
 * @param trx_level is the transaction isolation level
 * @return  innobase txn handle */
[[nodiscard]] Trx *trx_begin(System &sys, TrxLevel trx_level) noexcept;

/**
 * Interrupt a transaction from any thread: a lock wait it is in, or starts
 * later, ends with DB_INTERRUPTED and the statement is rolled back. The
 * transaction itself stays active, the client commits or rolls it back.
 *
 * @ingroup trx
 * @param[in,out] trx           The transaction to interrupt.
 */
void trx_interrupt(Trx *trx) noexcept;

/** Set client data for a transaction. This is passed back to the client
 * in callbacks. InnoDB will only ever pass this around, it will never
 * dereference it.
 *
 * @ingroup trx
 * @param trx is the transaction to set the client data for
 * @param client_data is client program's data about this transaction */
void trx_set_client_data(Trx *trx, void *client_data) noexcept;

/** Query the transaction's state. This function can be used to check for
* the state of the transaction in case it has been rolled back by the
* InnoDB deadlock detector. Note that when a transaction is selected as
* a victim for rollback, InnoDB will always return an appropriate error
* code indicating this. @see DB_DEADLOCK, @see DB_LOCK_TABLE_FULL and
* @see DB_LOCK_WAIT_TIMEOUT
*
* @ingroup trx
* @param trx is the transaction handle
* @return  transaction state */
[[nodiscard]] TrxState trx_state(Trx *trx) noexcept;

/** @return a new timestamp of the clock of the instance: an Accord timestamp. */
[[nodiscard]] ut::Timestamp clock_next(System &sys) noexcept;

/** Pass in a timestamp received from another node: the clock moves past it.
@return DB_INVALID_INPUT if its hlc is past the last transaction id. */
[[nodiscard]] DbErr clock_update(System &sys, const ut::Timestamp &received) noexcept;

/** @return the timestamp of the id of a started transaction. */
[[nodiscard]] ut::Timestamp trx_timestamp(const Trx *trx) noexcept;

/** @return the commit timestamp of the last commit of the transaction. */
[[nodiscard]] ut::Timestamp trx_commit_timestamp(System &sys, const Trx *trx) noexcept;

/** @return the read timestamp of the read view of the transaction, zero if none. */
[[nodiscard]] ut::Timestamp trx_view_timestamp(System &sys, const Trx *trx) noexcept;

/** Release the resources of the transaction. If the transaction was
* selected as a victim by InnoDB and rolled back then use this function
* to free the transaction handle.
*
* @ingroup trx
* @param trx is the transaction handle
* @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr trx_release(System &sys, Trx *trx) noexcept;

/** Commit a transaction. This function will release the schema latches too.
* It will also free the transaction handle.
*
* @ingroup trx
* @param trx is thr transaction handle
* @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr trx_commit(System &sys, Trx *trx) noexcept;

/** Rollback a transaction. This function will release the schema latches too.
* It will also free the transaction handle.
*
* @ingroup trx
* @param trx is the transaction handle
* @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr trx_rollback(System &sys, Trx *trx) noexcept;

/** Create a table in the InnoDB data dictionary using the schema definition.
* If the table exists in the database then this function will return
* DB_TABLE_IS_BEING_USED and id will contain that table's id.
*
* @ingroup ddl
* @param[in,out] trx the current user transaction
* @param tbl_sch the the schema for the table to create
* @param[out] id table id that was created
* @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr table_create(System &sys, Trx *trx, const TableSchema *tbl_sch, uint64_t *id) noexcept;

/** Rename a table. Ensure that you have acquired the schema lock in
 * exclusive mode.
 *
 * @ingroup ddl
 * @param[in,out] trx is the current user transaction
 * @param old_name the current name of the table
 * @param new_name the new name for the table
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr table_rename(System &sys, Trx *trx, const char *old_name, const char *new_name) noexcept;

/** Drop a table. Ensure that you have acquired the schema lock in
 * exclusive mode.
 *
 * @ingroup ddl
 * @param trx is the covering transaction.
 * @param name is the name of the table to drop
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr table_drop(System &sys, Trx *trx, const char *name) noexcept;

/** Drop a secondary index. Ensure that you have acquired the schema lock in
 * exclusive mode.
 *
 * @ingroup ddl
 * @param trx is the covering transaction.
 * @param index_id is the id of the index to drop
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr index_drop(System &sys, Trx *trx, uint64_t index_id) noexcept;

/** Open an InnoDB table and return a cursor handle to it.
 *
 * @ingroup cursor
 * @param table_id is the id of the table to open
 * @param trx is the current transaction handle, can be nullptr
 * @param[out] crsr is the new cursor
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr cursor_open_table_using_id(System &sys, uint64_t table_id, Trx *trx, Cursor **crsr) noexcept;

/** Open an InnoDB index and return a cursor handle to it.
 *
 * @ingroup cursor
 * @param index_id is the id of the index to open
 * @param trx is the current transaction handlem can be nullptr
 * @param[out] crsr is the new cursor
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr cursor_open_index_using_id(System &sys, uint64_t index_id, Trx *trx, Cursor **crsr) noexcept;

/** Open an InnoDB table by name and return a cursor handle to it.
 *
 * @ingroup cursor
 * @param name is the table name to open
 * @param trx is the current transactionm, can be nullptr
 * @param crsr is the new cursor
 * @return  DB_SUCCESS or err code */
[[nodiscard]] DbErr cursor_open_table(System &sys, const char *name, Trx *trx, Cursor **crsr) noexcept;

/** Truncate a table. The cursor handle will be closed and set to nullptr
 * on success.
 *
 * @ingroup ddl
 * @param[out] crsr is the cursor for table to truncate
 * @param[out] table_id is the new table id
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr cursor_truncate(System &sys, Cursor **crsr, uint64_t *table_id) noexcept;

/** Truncate a table.
 *
 * @ingroup ddl
 * @param table_name is the name of the table to truncate
 * @param[out] table_id is the new table id
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr table_truncate(System &sys, const char *table_name, uint64_t *table_id) noexcept;

/** Get a table id.
 *
 * @ingroup ddl
 * @param table_name is the name of the table to lookup
 * @param[out] table_id is the new table id if found
 * @return  DB_SUCCESS if found */
[[nodiscard]] DbErr table_get_id(System &sys, const char *table_name, uint64_t *table_id) noexcept;

/** Get an index id.
 *
 * @ingroup ddl
 * @param table_name is the name of the table that contains the index
 * @param index_name is the name of the index to lookup
 * @param[out] index_id contains the index id if found
 * @return  DB_SUCCESS if found */
[[nodiscard]] DbErr index_get_id(System &sys, const char *table_name, const char *index_name, uint64_t *index_id) noexcept;

/** Create a database if it doesn't exist.
 *
 * @ingroup ddl
 * @param dbname is the name of the database to create
 * @return  true on success */
[[nodiscard]] bool database_create(System &sys, const char *dbname) noexcept;

/** Drop a database if it exists. This function will also drop all tables
 * within the database.
 *
 * @ingroup ddl
 * @param dbname is the name of the database to drop
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr database_drop(System &sys, const char *dbname) noexcept;

/** Latches the data dictionary in shared mode.
 *
 * @ingroup ddl
 * @param trx is the transaction instance
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr schema_lock_shared(System &sys, Trx *trx) noexcept;

/** Latches the data dictionary in exclusive mode.
 *
 * @ingroup ddl
 * @param trx is the transaction instance
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr schema_lock_exclusive(System &sys, Trx *trx) noexcept;

/** Checks if the data dictionary is latched in exclusive mode by a
 * user transaction.
 *
 * @ingroup ddl
 * @param trx is a transaction instance
 * @return  true if exclusive latch */
[[nodiscard]] bool schema_lock_is_exclusive(const Trx *trx) noexcept;

/** Checks if the data dictionary is latched in shared mode.
 * @param trx is a transaction instance
 * @return  true if shared latch */
[[nodiscard]] bool schema_lock_is_shared(const Trx *trx) noexcept;

/** Unlocks the data dictionary.
 *
 * @ingroup ddl
 * @param trx is a transaction instance
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr schema_unlock(System &sys, Trx *trx) noexcept;

/** Set the Lock an InnoDB table using the table id.
 *
 * @ingroup trx
 * @param trx is a transaction instance
 * @param table_id is the table to lock
 * @param lck_mode is the lock mode
 * @return  DB_SUCCESS or error code */
[[nodiscard]] DbErr table_lock(System &sys, Trx *trx, uint64_t table_id, CursorLockMode lck_mode) noexcept;

/** Read a table's schema using the visitor pattern. It will make the
 * following sequence of calls:
 *
 * visitor->table()
 * visitor->table_col() for each user column
 * visitor->index() for each user index
 * visitor->index_col() for each column in user index
 *
 * It will stop if any of the above functions returns a non-zero value.
 * The caller must have an exclusive lock on the InnoDB data dictionary
 *
 * @ingroup ddl
 * @param trx transaction that owns the schema lock
 * @param name is the table name to read
 * @param visitor visitor functions to invoke on each definition
 * @param arg is the argument passed to the visitor functions.
 * @return  DB_SUCCESS or DB_ERROR */
[[nodiscard]] DbErr table_schema_visit(System &sys, Trx *trx, const char *name, const SchemaVisitor *visitor, void *arg) noexcept;

/** List all the tables in the InnoDB's data dictionary. It will abort
 * if visitor returns a non-zero value.
 *
 * It will call the function:
 *   visitor.tables(arg, const char* name, int name_len);
 *
 * The function will abort if visitor.tables() returns non-zero.
 *
 * @ingroup ddl
 * @param trx is the transaction that owns the schema lock
 * @param visitor is the visitor function
 * @param arg argument passed to the visitor function
 * @return DB_SUCCESS if successful */
[[nodiscard]] DbErr schema_tables_iterate(System &sys, Trx *trx, SchemaVisitorTableAll visitor, void *arg) noexcept;

/** Creates a named savepoint. The transaction must be started. If there is
 * already a savepoint of the same name, this call erases that old savepoint
 * and replaces it with a new. Savepoints are deleted in a transaction
 * commit or rollback.
 *
 * @ingroup trx
 * @param trx is the transaction instance
 * @param name is the name of the savepoint
 * @param name_len is the length of name in bytes */
void savepoint_take(Trx *trx, const void *name, ulint name_len) noexcept;

/** Releases only the named savepoint. Savepoints which were set after this
 * savepoint are left as is.
 *
 * @ingroup trx
 * @param trx is the active transaction
 * @param name is the name of the savepoint
 * @param name_len is the length of name in bytes
 * @return  if no savepoint of the name found then DB_NO_SAVEPOINT,
 * otherwise DB_SUCCESS */
[[nodiscard]] DbErr savepoint_release(Trx *trx, const void *name, ulint name_len) noexcept;

/** Rolls back a transaction back to a named savepoint. Modifications after the
 * savepoint are undone but InnoDB does NOT release the corresponding locks
 * which are stored in memory. If a lock is 'implicit', that is, a new inserted
 * row holds a lock where the lock information is carried by the trx id stored in
 * the row, these locks are naturally released in the rollback. Savepoints which
 * were set after this savepoint are deleted. If name equals nullptr then all the
 * savepoints are rolled back.
 *
 * @ingroup trx
 * @param trx is the active transaction
 * @param name is the savepoint name  can be nullptr
 * @param name_len is the length of name in bytes
 * @return  if no savepoint of the name found then DB_NO_SAVEPOINT,
 *         otherwise DB_SUCCESS */
[[nodiscard]] DbErr savepoint_rollback(System &sys, Trx *trx, const void *name, ulint name_len) noexcept;

/** Get which key caused a duplicate key error.
 *
 * In the event of \ref DB_DUPLICATE_KEY error, you can call this function
 * immediately after to get the name of the table and index that caused
 * the error.
 *
 * @ingroup trx
 * @param trx Transaction where error occured
 * @param table_name pointer to be set to table_name. Valid until the next API call
 * call. If you would like to keep it, make a copy.
 * @param index_name pointer to be set to the index name. Valid until the next API call
 * all. If you would like to keep it, make a copy.  */
[[nodiscard]] DbErr get_duplicate_key(Trx *trx, const char **table_name, const char **index_name) noexcept;

/** Do a parallel select count(*) from T;
 * @param[in] trx InnoDB transaction
 * @param[in] crsrs InnoDB cursors
 * @param[in] n_threads Number of threads to use
 * @param[out] n_rows Number of rows in the table */
[[nodiscard]] DbErr parallel_select_count_star(System &sys, Trx *trx, std::vector<Cursor *> &crsrs, size_t n_threads, uint64_t &n_rows) noexcept;

/**
 * Checks the table for errors using the given transaction and cursor.
 *
 * @param trx The InnoDB transaction to use for checking the table.
 * @param crsr The InnoDB cursor to use for checking the table.
 * @param n_threads The number of threads to use for checking the table.
 * @return The error code indicating the result of the table check.
 */
[[nodiscard]] DbErr check_table(System &sys, Trx *trx, Cursor *crsr, size_t n_threads) noexcept;

/** Return the API version number, the version number format is:
| 16 bits future use | 16 bits current | 16 bits revision | 16 bits age |

- If the library source code has changed at all since the last release,
  then revision will be incremented (`c:r:a' becomes `c:r+1:a').
- If any interfaces have been added, removed, or changed since the last
  update, current will be incremented, and revision will be set to 0.
- If any interfaces have been added (but not changed or removed) since
  the last release, then age will be incremented.
- If any interfaces have been changed or removed since the last release,
  then age will be set to 0.

@ingroup misc
@return  API version number */
[[nodiscard]] uint64_t api_version() noexcept;

/**
 * Get the value of a status variable.
 *
 * @ingroup misc
 * @param[in] sys       The instance.
 * @param[in] name      The status variable name, case insensitive.
 * @param[out] dst      Where the value is copied if name is found.
 *
 * @return DB_SUCCESS if found, DB_NOT_FOUND otherwise.
 */
[[nodiscard]] DbErr status_get_i64(System &sys, const char *name, int64_t *dst) noexcept;

/**
 * @ingroup misc
 * @return the names of all the status variables.
 */
[[nodiscard]] std::vector<const char *> status_get_all() noexcept;

}  // namespace api
