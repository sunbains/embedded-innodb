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

const ulint INS_NODE_MAGIC_N = 15849075;

struct Row_update;

struct Row_insert {

  /**
   * @brief Constructor for Row_insert.
   * 
   * @param[in] dict Data dictionary.
   * @param[in] lock_sys Lock system.
   * @param[in] row_upd Row update system.
   */
  Row_insert(Dict *dict, Lock_sys *lock_sys, Row_update *row_upd) noexcept;

  /**
   * @brief Destructor for Row_insert.
   */
  ~Row_insert() = default;

  /**
   * @brief Creates a new Row_insert object.
   * 
   * @param[in] dict Data dictionary.
   * @param[in] lock_sys Lock system.
   * @param[in] row_upd Row update system.
   * 
   * @return Pointer to the newly created Row_insert object.
   */
  [[nodiscard]] static Row_insert *create(Dict *dict, Lock_sys *lock_sys, Row_update *row_upd) noexcept;

  /**
   * @brief Destroys a Row_insert object.
   * 
   * @param[in] row_insert Pointer to the Row_insert object to destroy. 
   */
  static void destroy(Row_insert *&row_insert) noexcept;

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
  [[nodiscard]] db_err check_foreign_constraint(bool check_ref, const Foreign *foreign, const Table *table, DTuple *entry, que_thr_t *thr) noexcept;

  /**
   * @brief Creates an insert node struct.
   * 
   * @param[in] ins_type The type of insert operation (e.g., INS_VALUES, INS_SEARCHED, INS_DIRECT).
   * @param[in] table The table where the row will be inserted.
   * @param[in] heap The memory heap where the insert node struct will be created.
   * 
   * @return A pointer to the newly created insert node struct.
   */
  [[nodiscard]] ins_node_t *node_create(ib_ins_mode_t ins_type, Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Sets a new row to insert for an INS_DIRECT node.
   * 
   * This function is only used if we have constructed the row separately, 
   * which is a rare case; this function is quite slow.
   * 
   * @param[in] node Insert node.
   * @param[in] row New row (or first row) for the node.
   */
  void set_new_row(ins_node_t *node, DTuple *row) noexcept;

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
  [[nodiscard]] db_err index_entry(const Index *index, DTuple *entry, ulint n_ext, bool foreign, que_thr_t *thr) noexcept;

  /**
   * @brief Inserts a row into a table.
   * 
   * This is a high-level function used in SQL execution graphs.
   * 
   * @param[in] thr Query thread.
   * 
   * @return Query thread to run next or nullptr.
   */
  [[nodiscard]] que_thr_t *step(que_thr_t *thr) noexcept;

  /**
   * @brief Creates an entry template for each index of a table.
   * 
   * This function initializes an entry template for each index associated with the given insert node.
   * 
   * @param[in] node The insert node for which to create entry templates.
   */
  void create_entry_list(ins_node_t *node) noexcept;

  #ifdef UNIT_TEST
  private:
  #endif /* UNIT_TEST */
  /**
   * @brief Does an insert operation by updating a delete-marked existing record
   * in the index. This situation can occur if the delete-marked record is
   * kept in the index for consistent reads.
   *
   * @param[in] mode              BTR_MODIFY_LEAF or BTR_MODIFY_TREE, depending on whether mtr holds just a leaf
   *                              latch or also a tree latch
   * @param[in] btr_cur           B-tree cursor
   * @param[in] entry             Index entry to insert
   * @param[in] thr               Query thread
   * @param[in] mtr               Must be committed before latching any further pages
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err sec_index_entry_by_modify(ulint mode, Btree_cursor *btr_cur, const DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * @brief Does an insert operation by delete unmarking and updating a delete marked
   * existing record in the index. This situation can occur if the delete marked
   * record is kept in the index for consistent reads.
   * 
   * @param[in] mode BTR_MODIFY_LEAF or BTR_MODIFY_TREE, depending on whether mtr holds just a leaf latch or also a tree latch
   * @param[in] btr_cur B-tree cursor
   * @param[in,out] heap Pointer to memory heap, or NULL
   * @param[out] big_rec Possible big rec vector of fields which have to be stored externally by the caller
   * @param[in] entry Index entry to insert
   * @param[in] thr Query thread
   * @param[in] mtr Mini-transaction; must be committed before latching any further pages
   * 
   * @return DB_SUCCESS, DB_FAIL, or error code
   */
  [[nodiscard]] db_err clust_index_entry_by_modify(ulint mode, Btree_cursor *btr_cur, mem_heap_t **heap, big_rec_t **big_rec, const DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * @brief Checks if an ancestor node of the given node updates the table in a cascaded update/delete.
   *
   * This function traverses the query graph upwards from the given node and checks if any ancestor node
   * performs an UPDATE (not DELETE) operation on the specified table.
   *
   * @param[in] node The node in a query graph.
   * @param[in] table The table to check for updates.
   * 
   * @return true if an ancestor updates the table, false otherwise.
   */
  [[nodiscard]] bool cascade_ancestor_updates_table(que_node_t *node, Table *table) noexcept;

  /**
   * @brief Returns the number of ancestor UPDATE or DELETE nodes of a cascaded update/delete node.
   * 
   * @param[in] node Node in a query graph.
   * 
   * @return Number of ancestors.
   */
  [[nodiscard]] ulint cascade_n_ancestors(que_node_t *node) noexcept;

  /**
   * @brief Calculates the update vector node->cascade->update for a child
   * table in a cascaded update.
   * 
   * @param[in] node Update node of the parent table.
   * @param[in] foreign Foreign key constraint whose type is != 0.
   * @param[in] heap Memory heap to use as temporary storage.
   * 
   * @return Number of fields in the calculated update vector. The value
   *  can also be 0 if no foreign key fields changed. The returned value
   *  is ULINT_UNDEFINED if the column type in the child table is too short
   *  to fit the new value in the parent table, indicating the update fails.
   */
  [[nodiscard]] ulint cascade_calc_update_vec(upd_node_t *node, const Foreign *foreign, mem_heap_t *heap) noexcept;

  /**
   * @brief Set detailed error message associated with foreign key errors for the given transaction.
   * 
   * @param trx Transaction.
   * @param foreign Foreign key constraint.
   */
  void set_detailed(Trx *trx, const Foreign *foreign) noexcept;

  /**
   * Reports a foreign key error associated with an update or a delete of a
   * parent table index entry.
   *
   * @param[in] errstr Error string from the viewpoint of the parent table.
   * @param[in] thr Query thread whose run_node is an update node.
   * @param[in] foreign Foreign key constraint.
   * @param[in] rec A matching index record in the child table.
   * @param[in] entry Index entry in the parent table.
   */
  void foreign_report_err(const char *errstr, que_thr_t *thr, const Foreign *foreign, const rec_t *rec, const DTuple *entry) noexcept;

  /**
   * @brief Reports a foreign key error to ib_stream when trying to add an index entry to a child table.
   * 
   * This function is called when there is a foreign key constraint violation while adding an index entry to a child table.
   * The addition may be the result of an update operation.
   * 
   * @param[in] trx Transaction.
   * @param[in] foreign Foreign key constraint.
   * @param[in] rec A record in the parent table that does not match the entry due to the error.
   * @param[in] entry Index entry to insert in the child table.
   */
  void foreign_report_add_err(Trx *trx, const Foreign *foreign, const rec_t *rec, const DTuple *entry) noexcept;

  /**
   * @brief Does a cascaded delete or set null in a foreign key operation.
   * 
   * @param thr Query thread.
   * @param node Update node used in the cascade or set null operation.
   * @param table Table where the operation is performed.
   * 
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] db_err update_cascade_client(que_thr_t *thr, upd_node_t *node, Table *table) noexcept;

  /**
   * Perform referential actions or checks when a parent row is
   * deleted or updated and the constraint had an ON DELETE or
   * ON UPDATE condition which was not RESTRICT.
   *
   * @param[in] thr     in: query thread whose run_node is an update node
   * @param[in] foreign in: foreign key constraint whose type is != 0
   * @param[in] pcur    in: btr_cur placed on a matching index record in the child table
   * @param[in] entry   in: index entry in the parent table
   * @param[in] mtr     in: mtr holding the latch of pcur page
   * 
   * @return DB_SUCCESS, DB_LOCK_WAIT, or error code
   */
  [[nodiscard]] db_err foreign_check_on_constraint(que_thr_t *thr, const Foreign *foreign, Btree_pcursor *pcur, DTuple *entry, mtr_t *mtr) noexcept;

  /**
   * @brief Sets a shared lock on a record.
   * 
   * This function is used in locking possible duplicate key records and also in checking foreign key constraints.
   * 
   * @param[in] type     LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP type lock.
   * @param[in] block    Buffer block of the record.
   * @param[in] rec      Record to lock.
   * @param[in] index    Index of the record.
   * @param[in] offsets  Column offsets in the record.
   * @param[in] thr      Query thread.
   * 
   * @return DB_SUCCESS if successful, or an error code.
   */
  [[nodiscard]] db_err set_shared_rec_lock(ulint type, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr) noexcept;

  /**
   * @brief Sets an exclusive lock on a record.
   * 
   * This function is used in locking possible duplicate key records.
   * 
   * @param[in] type     LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP type lock.
   * @param[in] block    Buffer block of the record.
   * @param[in] rec      Record to lock.
   * @param[in] index    Index of the record.
   * @param[in] offsets  Column offsets in the record.
   * @param[in] thr      Query thread.
   * 
   * @return DB_SUCCESS if successful, or an error code.
   */
  [[nodiscard]] db_err set_exclusive_rec_lock(ulint type, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr) noexcept;

  /**
   * @brief Checks if foreign key constraints fail for an index entry.
   * 
   * If the index is not mentioned in any constraint, this function does nothing.
   * Otherwise, it searches the indexes of referenced tables and sets shared locks
   * which lock either the success or the failure of a constraint.
   * 
   * @param[in] table Table
   * @param[in] index Index
   * @param[in] entry Index entry for index
   * @param[in] thr Query thread
   * 
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err check_foreign_constraints(const Table *table, const Index *index, DTuple *entry, que_thr_t *thr) noexcept;

  /**
  * @brief Checks if a unique key violation would occur at the index entry insert.
   * 
   * @param[in] rec User record. Assumes that the caller already has a record lock on the record.
   * @param[in] entry Entry to insert.
   * @param[in] index Index.
   * @param[in] offsets Phy_rec::get_col_offsets(index, rec).
   * 
   * @return true if a unique key violation occurs, false otherwise.
   */
  [[nodiscard]] bool dupl_error_with_rec(const rec_t *rec, const DTuple *entry, const Index *index, const ulint *offsets) noexcept;

  /**
   * @brief Scans a unique non-clustered index at a given index entry to determine
   * whether a uniqueness violation has occurred for the key value of the entry.
   * 
   * @param[in] index Non-clustered unique index.
   * @param[in] entry Index entry.
   * @param[in] thr Query thread.
    * 
   * @return DB_SUCCESS, DB_DUPLICATE_KEY, or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err scan_sec_index_for_duplicate(const Index *index, DTuple *entry, que_thr_t *thr) noexcept;

  /**
   * @brief Checks if a unique key violation error would occur at an index entry insert.
   * 
   * This function sets shared locks on possible duplicate records. It works only for a clustered index.
   * 
   * @param[in] btr_cur B-tree cursor.
   * @param[in] entry Entry to insert.
   * @param[in] thr Query thread.
   * @param[in] mtr Mini-transaction.
   * 
   * @return DB_SUCCESS if no error, DB_DUPLICATE_KEY if error, DB_LOCK_WAIT if we have to wait for a lock on a possible duplicate record.
   */
  [[nodiscard]] db_err duplicate_error_in_clust(Btree_cursor *btr_cur, DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * @brief Checks if an index entry has a long enough common prefix with an existing record.
   * 
   * This function determines if the intended insert of the entry must be changed to a modify 
   * of the existing record. In the case of a clustered index, the prefix must be n_unique 
   * fields long, and in the case of a secondary index, all fields must be equal.
   * 
   * @param[in] btr_cur B-tree cursor.
   * 
   * @return 0 if no update is needed, ROW_INS_PREV if the previous record should be updated.
   */
  [[nodiscard]] ulint must_modify(Btree_cursor *btr_cur) noexcept;

  /**
   * @brief Tries to insert an index entry to an index.
   * 
   * If the index is clustered and a record with the same unique key is found, 
   * the other record is necessarily marked deleted by a committed transaction, 
   * or a unique key violation error occurs. The delete marked record is then 
   * updated to an existing record, and we must write an undo log record on the 
   * delete marked record. If the index is secondary, and a record with exactly 
   * the same fields is found, the other record is necessarily marked deleted. 
   * It is then unmarked. Otherwise, the entry is just inserted to the index.
   * 
   * @param[in] mode BTR_MODIFY_LEAF or BTR_MODIFY_TREE, depending on whether 
   *                 we wish optimistic or pessimistic descent down the index tree.
   * @param[in] index Index to insert the entry into.
   * @param[in] entry Index entry to insert.
   * @param[in] n_ext Number of externally stored columns.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_FAIL if pessimistic retry needed, or error code.
   */
  [[nodiscard]] db_err index_entry_low(ulint mode, const Index *index, DTuple *entry, ulint n_ext, que_thr_t *thr) noexcept;

  /**
   * @brief Sets the values of the dtuple fields in entry from the values of appropriate columns in row.
   * 
   * @param[in] index Index to use for setting values.
   * @param[in,out] entry Index entry to populate with values.
   * @param[in] row Row containing the values to set in the entry.
   */
  void index_entry_set_vals(Index *index, DTuple *entry, const DTuple *row) noexcept;

  /**
   * @brief Inserts a single index entry to the table.
   * 
   * @param[in] node Row insert node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err index_entry_step(ins_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Allocates a row id for row and inits the node->index field.
   * 
   * @param[in] node Row insert node.
   */
  void alloc_row_id_step(ins_node_t *node) noexcept;

  /**
   * @brief Gets a row to insert from the values list.
   * 
   * @param[in] node Row insert node.
   */
  void get_row_from_values(ins_node_t *node) noexcept;

  /**
   * @brief Gets a row to insert from the select list.
   * 
   * @param[in] node Row insert node.
   */
  void get_row_from_select(ins_node_t *node) noexcept;

  /**
   * @brief Inserts a row into a table.
   * 
   * This function handles the insertion of a row into a specified table. It allocates
   * a row ID, retrieves the row data from either a select list or values list, and
   * inserts the entries into the appropriate indexes.
   * 
   * @param[in] node Row insert node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if the operation is successfully completed, otherwise an error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err insert(ins_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Adds system field buffers to a row.
   * 
   * @param[in,out] node Insert node to initialize.
   */
  void alloc_sys_fields(ins_node_t *node) noexcept;

public:
  /** Data dictionary */
  Dict *m_dict{};

  /** Lock system */
  Lock_sys *m_lock_sys{};

  /** Row update system */
  Row_update *m_row_upd{};
};

/**
 * @brief Insert node structure.
 */
struct Insert_node {
  /** Node type: QUE_NODE_INSERT */
  que_common_t m_common;

  /** Insert type: INS_VALUES, INS_SEARCHED, or INS_DIRECT */
  ib_ins_mode_t m_ins_type;

  /** Row to insert */
  DTuple *m_row{};

  /** Table where to insert */
  Table *m_table{};

  /** Select in searched insert */
  sel_node_t *m_select{};

  /** List of expressions to evaluate and insert in an INS_VALUES insert */
  que_node_t *m_values_list{};

  /** Node execution state */
  ulint m_state{};

  /** nullptr, or the next index where the index entry should be inserted */
  Index *m_index{};

  /** nullptr, or entry to insert in the index; after a successful insert
   * of the entry, this should be reset to nullptr */
  DTuple *m_entry{};

  /** List of entries, one for each index */
  UT_LIST_BASE_NODE_T(DTuple, tuple_list) m_entry_list{};

  /** Buffer for the row id sys field in row */
  byte *m_row_id_buf{};

  /** Transaction id or the last transaction which executed the node */
  trx_id_t m_trx_id{};

  /** Buffer for the transaction id sys field in row */
  byte *m_trx_id_buf{};

  /** Memory heap used as auxiliary storage; entry_list and sys fields are
   * stored here; if this is nullptr, entry list should be created and
   * buffers for sys fields in row allocated */
  mem_heap_t *m_entry_sys_heap{};

  /** Magic number for validation */
  IF_DEBUG(ulint m_magic_n{INS_NODE_MAGIC_N};)
};
