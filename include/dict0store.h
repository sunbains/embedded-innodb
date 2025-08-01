/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/dict0store.h
Data dictionary creation and booting

Created 4/18/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "buf0buf.h"
#include "dict0types.h"
#include "fsp0fsp.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "ut0byte.h"

struct que_thr_t;
struct Table_node;
struct Btree_pcursor;

/** Responsible for data dictionary persistence. */
struct Dict_store {
  using hdr_t = byte;

  /** Constructor.
   * 
   * @param[in] dict The dictionary object.
   * @param[in] btree The b-tree object.
   */
  explicit Dict_store(Dict *dict, Btree *btree) noexcept;

  /** Destructor. */
  ~Dict_store() = default;

  /**
   * @brief Creates and initializes the data dictionary at the database creation.
   * 
   * @return true if successful, false otherwise.
   */
  [[nodiscard]] db_err create_instance() noexcept;

  /**
   * @brief Destroys the data dictionary.
   */
  static void destroy(Dict_store *store) noexcept;

  /**
   * @brief Gets a pointer to the dictionary header and x-latches its page.
   *
   * @param[in] mtr The mini-transaction object.
   * 
   * @return Pointer to the dictionary header, page x-latched.
   */
  [[nodiscard]] hdr_t *hdr_get(mtr_t *mtr) noexcept;

  /**
   * @brief Returns a new row, table, index, or tree id.
   *
   * @param[in] type The type of id to generate (DICT_HDR_ROW_ID, etc.)
   * 
   * @return The new id.
   */
  [[nodiscard]] Dict_id hdr_get_new_id(Dict_id_type type) noexcept;

  /**
   * @brief Writes the current value of the row id counter to the dictionary header file page.
   */
  void hdr_flush_row_id() noexcept;

  /**
   * @brief Returns a new row id.
   * 
   * This function generates a new row id and increments the row id counter.
   * If the new row id is divisible by DICT_HDR_ROW_ID_WRITE_MARGIN, the current
   * row id counter value is written to the dictionary header file page.
   * 
   * @return The new row id.
   */
  [[nodiscard]] Dict_id sys_get_new_row_id() noexcept;

  /**
   * @brief Reads a row id from a record or other 6-byte stored form.
   * 
   * @param[in] field The record field.
   * 
   * @return The row id.
   */
  [[nodiscard]] inline Dict_id sys_read_row_id(byte *field) noexcept {
    static_assert(DATA_ROW_ID_LEN == 6, "error DATA_ROW_ID_LEN != 6");

    return mach_read_from_6(field);
  }

  /**
   * @brief Writes a row id to a record or other 6-byte stored form.
   * 
   * @param[in] field The record field.
   * @param[in] row_id The row id.
   */
  inline void sys_write_row_id(byte *field, Dict_id row_id) noexcept {
    static_assert(DATA_ROW_ID_LEN == 6, "error DATA_ROW_ID_LEN != 6");

    mach_write_to_6(field, row_id);
  }

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
  que_thr_t *create_table_step(que_thr_t *thr) noexcept;

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
  que_thr_t *create_index_step(que_thr_t *thr) noexcept;

  /**
   * @brief Based on a table object, this function builds the entry to be inserted in the SYS_TABLES system table.
   * 
   * @param[in] table The table object.
   * @param[in] heap The memory heap from which the memory for the built tuple is allocated.
   * 
   * @return The tuple which should be inserted.
   */
  [[nodiscard]] DTuple *create_sys_tables_tuple(const Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Based on a table object, this function builds the entry to be inserted in the SYS_COLUMNS system table.
   * 
   * @param[in] table The table object.
   * @param[in] i The column number.
   * @param[in] heap The memory heap from which the memory for the built tuple is allocated.
   * 
   * @return The tuple which should be inserted.
   */
  [[nodiscard]] DTuple *create_sys_columns_tuple(const Table *table, ulint i, mem_heap_t *heap) noexcept;

  /**
 * @brief Add a single foreign key definition to the data dictionary tables in the database.
 * We also generate names to constraints that were not named by the user.
 * A generated constraint has a name of the format databasename/tablename_ibfk_NUMBER,
 * where the numbers start from 1, and are given locally for this table, that is,
 * the number is not global, as in the old format constraints < 4.0.18 it used to be.
 * 
 * @param[in] id_nr in/out: number to use in id generation; incremented if used
 * @param[in] table in: table
 * @param[in] foreign in: foreign
 * @param[in] trx in: transaction
 * 
 * @return error code or DB_SUCCESS
 */
  [[nodiscard]] db_err add_foreign_to_dictionary(ulint *id_nr, Table *table, Foreign *foreign, Trx *trx) noexcept;

  /**
   * @brief Creates the foreign key constraints system tables inside InnoDB
   * at database creation or database start if they are not found or are
   * not of the right form.
   *
   * This function ensures that the necessary system tables for foreign key
   * constraints are present in the InnoDB database. If the tables are missing
   * or not in the correct format, they will be created or corrected.
   *
   * @return DB_SUCCESS on success, or an error code on failure.
   */
  [[nodiscard]] db_err create_or_check_foreign_constraint_tables() noexcept;

  /**
   * @brief Creates the dictionary header and system tables.
   * 
   * @param[in] mtr The mini-transaction.
   * 
   * @return true if successful, false otherwise.
   */
  [[nodiscard]] bool bootstrap_system_tables(mtr_t *mtr) noexcept;

  /**
   * @brief Builds a column definition to insert.
   * 
   * @param[in] node The table create node.
   * 
   * @return DB_SUCCESS
   */
  [[nodiscard]] db_err build_col_def_step(Table_node *node) noexcept;

  /**
   * @brief Based on an index object, this function builds the entry to be inserted in the SYS_INDEXES system table.
   * 
   * @param[in] index The index.
   * @param[in] heap The memory heap from which the memory for the built tuple is allocated.
   * 
   * @return The tuple which should be inserted.
   */
  [[nodiscard]] DTuple *create_sys_indexes_tuple(const Index *index, mem_heap_t *heap) noexcept;

  /**
   * @brief Based on an index object, this function builds the entry to be inserted in the SYS_FIELDS system table.
   * 
   * @param[in] index The index.
   * @param[in] i The field number.
   * @param[in] heap The memory heap from which the memory for the built tuple is allocated.
   * 
   * @return The tuple which should be inserted.
   */
  [[nodiscard]] DTuple *create_sys_fields_tuple(const Index *index, ulint i, mem_heap_t *heap) noexcept;

  /**
   * @brief Creates the tuple with which the index entry is searched for writing the index tree root page number, if such a tree is created.
   * 
   * @param[in] tuple The tuple inserted in the SYS_INDEXES table.
   * @param[in] heap The memory heap from which the memory for the built tuple is allocated.
   * 
   * @return The tuple for search.
   */
  [[nodiscard]] DTuple *create_search_tuple(const DTuple *tuple, mem_heap_t *heap) noexcept;

  /**
   * @brief Builds a table definition to insert.
   * 
   * @param[in] thr The query thread.
   * @param[in] node The table create node.
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err build_table_def_step(que_thr_t *thr, Table_node *node) noexcept;

  /**
   * @brief Builds an index definition row to insert.
   * 
   * @param[in] thr The query thread.
   * @param[in] node The index create node.
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err build_index_def_step(que_thr_t *thr, Index_node *node) noexcept;

  /**
   * @brief Builds a field definition row to insert.
   * 
   * @param[in] node The index create node.
   * 
   * @return DB_SUCCESS
   */
  [[nodiscard]] db_err build_field_def_step(Index_node *node) noexcept;

  /**
   * @brief Creates an index tree for the index if it is not a member of a cluster.
   * 
   * @param[in] node The index create node.
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err create_index_tree_step(Index_node *node) noexcept;

  /**
   * @brief Evaluate the given foreign key SQL statement.
   * 
   * @param[in] info info struct, or nullptr
   * @param[in] sql SQL string to evaluate
   * @param[in] table table
   * @param[in] foreign foreign
   * @param[in] trx transaction
   * 
   * @return error code or DB_SUCCESS
   */
  [[nodiscard]] db_err foreign_eval_sql(pars_info_t *info, const char *sql, Table *table, Foreign *foreign, Trx *trx) noexcept;

  /**
 * @brief Add a single foreign key field definition to the data dictionary tables in the database.
 * 
   * @param field_nr foreign field number
   * @param table table
   * @param foreign foreign
   * @param trx transaction
   * 
   * @return error code or DB_SUCCESS
   */
  [[nodiscard]] db_err add_foreign_field_to_dictionary(ulint field_nr, Table *table, Foreign *foreign, Trx *trx) noexcept;

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
  [[nodiscard]] db_err add_foreigns_to_dictionary(ulint start_id, Table *table, Trx *trx) noexcept;

  /**
   * Truncates the index tree associated with a row in the SYS_INDEXES table.
   *
   * @param[in] table The table the index belongs to.
   * @param[in] space_id  0 to truncate, nonzero to create the index tree in the given tablespace.
   * @param[in,out] pcur Persistent cursor pointing to the record in the clustered index of the SYS_INDEXES table. The cursor may be repositioned in this call.
   * @param[in] mtr Mini-transaction having the latch on the record page. The mtr may be committed and restarted in this call.
   * 
   * @return New root page number, or FIL_NULL on failure.
   */
  [[nodiscard]] page_no_t truncate_index_tree(Table *table, space_id_t space_id, Btree_pcursor *pcur, mtr_t *mtr) noexcept;

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
  void drop_index_tree(Rec rec, mtr_t *mtr) noexcept;

  /** The dictionary . */
  Dict *m_dict{};

  /**
   * The next row id to assign; NOTE that at a checkpoint this must
   * be written to the dict system header and flushed to a file; in
   * recovery this must be derived from the log records.
   */
  Dict_id m_row_id{};

  /** File space manager.*/
  FSP *m_fsp{};

  /** B-tree system.*/
  Btree *m_btree{};
};
