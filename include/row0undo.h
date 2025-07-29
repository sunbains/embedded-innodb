/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/row0undo.h
Row undo

Created 1/8/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0pcur.h"
#include "dict0types.h"
// #include "row0types.h"

struct Trx;
struct FSP;
struct Dict;
struct Table;
struct Index;
struct Btree;
struct que_thr_t;
struct Row_update;

struct Row_undo {
  /**
   * @brief Constructs a row undo.
   *
   * @param[in] dict The data dictionary.
   */
  explicit Row_undo(Dict *dict) noexcept : m_dict(dict) {}

  /**
   * @brief Creates a row undo.
   *
   * @param[in] dict The data dictionary.
   * 
   * @return A pointer to the newly created row undo.
   */
  [[nodiscard]] static Row_undo *create(Dict *dict) noexcept;

  /**
   * @brief Destroys a row undo.
   *
   * @param[in,out] row_undo The row undo to destroy.
   */
  static void destroy(Row_undo *&row_undo) noexcept;

  /**
   * @brief Creates a row undo node to a query graph.
   *
   * @param[in] trx The transaction.
   * @param[in] parent The parent node, i.e., a thr node.
   * @param[in] heap The memory heap where the node is created.
   * 
   * @return A pointer to the newly created undo node.
   */
  [[nodiscard]] Undo_node *create_undo_node(Trx *trx, que_thr_t *parent, mem_heap_t *heap) noexcept;

  /**
   * @brief Undoes a row operation in a table.
   *
   * This is a high-level function used in SQL execution graphs.
   *
   * @param[in,out] thr Query thread.
   * 
   * @return Query thread to run next or nullptr.
   */
  [[nodiscard]] que_thr_t *step(que_thr_t *thr) noexcept;

  /** Data dictionary */
  Dict *m_dict{};
};

/* A single query thread will try to perform the undo for all successive
versions of a clustered index record, if the transaction has modified it
several times during the execution which is rolled back. It may happen
that the task is transferred to another query thread, if the other thread
is assigned to handle an undo log record in the chain of different versions
of the record, and the other thread happens to get the x-latch to the
clustered index record at the right time.

If a query thread notices that the clustered index record it is looking
for is missing, or the roll ptr field in the record doed not point to the
undo log record the thread was assigned to handle, then it gives up the undo
task for that undo log record, and fetches the next. This situation can occur
just in the case where the transaction modified the same record several times
and another thread is currently doing the undo for successive versions of
that index record. */

/** Execution state of an undo node */
enum undo_exec {
  UNDO_NODE_UNDEFINED = 0,

  /** we should fetch the next undo log record */
  UNDO_NODE_FETCH_NEXT = 1,

  /** the roll ptr to previous version of a row is
  stored in node, and undo should be done based on it */
  UNDO_NODE_PREV_VERS,

  /** undo a fresh insert of a row to a table */
  UNDO_NODE_INSERT,

  /** undo a modify operation (DELETE or UPDATE) on a row of a table */
  UNDO_NODE_MODIFY
};

/** Undo node structure */
struct Undo_node {

  Undo_node() = delete;

  /**
  * @brief Constructs a row undo node.
  *
  * @param[in] dict The data dictionary.
  * @param[in] log The redo log (for the free_ckeck()). FIXME: Get rid of it.
  * @param[in] trx The transaction.
  * @param[in] parent The parent node, i.e., a thr node.
  */
  Undo_node(Dict *dict, Trx *trx, que_thr_t *parent) noexcept;

  /**
   * @brief Creates a row undo node to a query graph.
   *
   * @param[in] dict The data dictionary.
   * @param[in] trx The transaction.
   * @param[in] parent The parent node, i.e., a thr node.
   * @param[in] heap The memory heap where the node is created.
   * 
   * @return A pointer to the newly created undo node.
   */
  [[nodiscard]] static Undo_node *create(Dict *dict, Trx *trx, que_thr_t *parent, mem_heap_t *heap) noexcept;

  /**
   * @brief Looks for the clustered index record when node has the row reference.
   *
   * The pcur in node is used in the search. If found, stores the row to node,
   * and stores the position of pcur, and detaches it. The pcur must be closed
   * by the caller in any case.
   *
   * @return true if found; NOTE the node->pcur must be closed by the
   * caller, regardless of the return value.
   */
  [[nodiscard]] bool search_cluster_index_and_persist_position() noexcept;

  /**
   * @brief Fetches an undo log record and performs the undo for the recorded operation.
   *
   * If no undo log record is left, or a partial rollback is completed, control is returned
   * to the parent node, which is always a query thread node.
   *
   * @param[in,out] thr Query thread.
   * 
   * @return DB_SUCCESS if the operation is successfully completed, otherwise an error code.
   */
  [[nodiscard]] db_err fetch_undo_log_and_undo(que_thr_t *thr) noexcept;

  /**
   * @brief Removes a clustered index record.
   *
   * The persistent cursor (pcur) in the node was positioned on the record, now it is detached.
   *
   * @param[in] dict The data dictionary.
   * 
   * @return DB_SUCCESS if the operation is successful, otherwise DB_OUT_OF_FILE_SPACE.
   */
  [[nodiscard]] db_err remove_cluster_rec() noexcept;

  /**
   * @brief Parses the row reference and other info in a fresh insert undo record.
   *
   * @param[in] recovery Recovery flag.
   */
  void parse_insert_undo_rec(ib_recovery_t recovery) noexcept;

  /**
    * @brief Parses the row reference and other info in a modify undo log record.
   *
   * @param[in] recovery             Recovery flag.
   * @param[in,out] thr              Query thread.
   */
  void parse_update_undo_rec(ib_recovery_t recovery, que_thr_t *thr) noexcept;

  /**
   * @brief Undoes a fresh insert of a row to a table.
   *
   * @return DB_SUCCESS if the operation is successful, otherwise an error code.
   */
  [[nodiscard]] db_err undo_insert() noexcept;

  /**
   * @brief Checks if also the previous version of the clustered index record was
   * modified or inserted by the same transaction, and its undo number is such
   * that it should be undone in the same rollback.
   *
   * @param[out] undo_no The undo number.
   * 
   * @return true if also previous modify or insert of this row should be undone
   */
  [[nodiscard]] inline bool undo_previous_version(undo_no_t *undo_no) noexcept;

  /**
 * @brief Undoes a modify in a clustered index record.
 *
 * @param[in,out] thr           Query thread.
 * @param[in,out] mtr           Must be committed before latching any further pages.
 * @param[in] mode              BTR_MODIFY_LEAF or BTR_MODIFY_TREE.
 * 
 * @return DB_SUCCESS, DB_FAIL, or error code: we may run out of file space.
   */
  [[nodiscard]] db_err undo_clustered_index_modification(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept;

  /**
   * @brief Removes a clustered index record after undo if possible.
   * 
   * This is attempted when the record was inserted by updating a
   * delete-marked record and there no longer exist transactions
   * that would see the delete-marked record. In other words, we
   * roll back the insert by purging the record.
   * 
   * @param[in,out] thr         Query thread.
   * @param[in,out] mtr         Must be committed before latching any further pages.
   * @param[in] mode            BTR_MODIFY_LEAF or BTR_MODIFY_TREE.
   * 
   * @return DB_SUCCESS, DB_FAIL, or error code: we may run out of file space.
   */
  [[nodiscard]] db_err remove_clustered_index_record(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept;

  /**
   * @brief Removes a clustered index record after undo if possible.
   * 
   * This is attempted when the record was inserted by updating a
   * delete-marked record and there no longer exist transactions
   * that would see the delete-marked record. In other words, we
   * roll back the insert by purging the record.
   * 
   * @param[in,out] node Row undo node.
   * @param[in,out] thr Query thread.
   * @param[in,out] mtr Must be committed before latching any further pages.
   * @param[in] mode BTR_MODIFY_LEAF or BTR_MODIFY_TREE.
   * 
   * @return DB_SUCCESS, DB_FAIL, or error code: we may run out of file space.
   */
  [[nodiscard]] db_err remove_clustered_index_record_low(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept;

  /**
   * @brief Delete marks or removes a secondary index entry if found.
   *
   * @param[in,out] thr         Query thread
   * @param[in,out] index       Index
   * @param[in] entry           Index entry
   * @param[in] mode            Latch mode BTR_MODIFY_LEAF or BTR_MODIFY_TREE
   * 
   * @return DB_SUCCESS, DB_FAIL, or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err delete_mark_or_remove_secondary_index_entry(
    que_thr_t *thr, Index *index, DTuple *entry, ulint mode
  ) noexcept;

  /**
   * @brief Delete marks or removes a secondary index entry if found.
   *
   * If the fields of a delete-marked secondary index record are updated such that 
   * they remain alphabetically the same (e.g., 'abc' -> 'aBc'), the original values 
   * cannot be restored because they are unknown. This should not cause issues 
   * because in row0sel.c, queries always retrieve the clustered index record or 
   * an earlier version of it if the secondary index record used for the search is 
   * delete-marked.
   *
   * @param[in,out] thr         Query thread
   * @param[in,out] index       Index
   * @param[in] entry           Index entry
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
     */
  [[nodiscard]] db_err delete_mark_or_remove_secondary_index_entry(que_thr_t *thr, Index *index, DTuple *entry) noexcept;

  /**
 * Delete unmarks a secondary index entry which must be found. It might not be
 * delete-marked at the moment, but it does not harm to unmark it anyway. We also
 * need to update the fields of the secondary index record if we updated its
 * fields but alphabetically they stayed the same, e.g., 'abc' -> 'aBc'.
 * 
 * @param[in] mode              Search mode: BTR_MODIFY_LEAF or BTR_MODIFY_TREE
 * @param[in,out] thr           Query thread
 * @param[in,out] index         Secondary index in which to unmark the entry
 * @param[in] entry             Index entry to unmark.
 * 
 * @return DB_FAIL or DB_SUCCESS or DB_OUT_OF_FILE_SPACE
 */
  [[nodiscard]] db_err del_unmark_secondary_index_and_undo_update(
    ulint mode, que_thr_t *thr, Index *index, const DTuple *entry
  ) noexcept;

  /**
   * @brief Undoes a modify in a clustered index record.
   * 
   * Sets also the node state for the next round of undo.
   * 
   * @param[in,out] thr         Query thread.
   * 
   * @return DB_SUCCESS or error code: we may run out of file space.
   */
  [[nodiscard]] db_err undo_clustered_index_modification(que_thr_t *thr) noexcept;

  /**
   * Undoes a modify in secondary indexes when undo record type is UPD_DEL.
   * 
   * @param[in,out] thr         Query thread
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err undo_delete_in_secondary_indexes(que_thr_t *thr) noexcept;

  /**
   * @brief Undoes a modify in secondary indexes when undo record type is DEL_MARK.
   * 
   * @param[in,out] thr         Query thread
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err undo_del_mark_secondary_indexes(que_thr_t *thr) noexcept;

  /**
   * Undoes a modify in secondary indexes when undo record type is UPD_EXIST.
   * 
   * @param[in,out] thr              Query thread
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err undo_update_of_secondary_indexes(que_thr_t *thr) noexcept;

  /**
   * @brief Undoes a modify operation on a row of a table.
   * 
   * @param[in,out] thr Query thread.
   * 
   * @return	DB_SUCCESS or error code */
  [[nodiscard]] db_err undo_update(que_thr_t *thr) noexcept;

  /**
   * Removes a secondary index entry if found.
   * 
   * @param[in,out] mode          BTR_MODIFY_LEAF or BTR_MODIFY_TREE,
   *                              depending on whether we wish optimistic or
   *                              pessimistic descent down the index tree
   * @param[in] index             Remove entry from this index
   * @param[in] entry             Index entry to remove
   * 
   * @return	DB_SUCCESS, DB_FAIL, or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err remove_secondary_index_entry(ulint mode, Index *index, DTuple *entry) noexcept;

  /**
   * @brief Removes a secondary index entry from the index if found.
   *
   * Tries first optimistic, then pessimistic descent down the tree.
   *
   * @param[in,out] index Remove entry from this secondary index.
   * @param[in] entry Entry to remove.
   * 
   * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
   */
  [[nodiscard]] db_err remove_secondary_index_entry(Index *index, DTuple *entry) noexcept;

  /** node type: QUE_NODE_UNDO */
  que_common_t m_common{};

  /** node execution state */
  undo_exec m_state{UNDO_NODE_UNDEFINED};

  /** trx for which undo is done */
  Trx *m_trx{};

  /** roll pointer to undo log record */
  roll_ptr_t m_roll_ptr{};

  /** undo log record */
  trx_undo_rec_t *m_undo_rec{};

  /** undo number of the record */
  undo_no_t m_undo_no{};

  /** undo log record type: TRX_UNDO_INSERT_REC, ... */
  ulint m_rec_type{};

  /** roll ptr to restore to clustered index record */
  roll_ptr_t m_new_roll_ptr{};

  /** trx id to restore to clustered index record */
  trx_id_t m_new_trx_id{};

  /** persistent cursor used in searching the clustered index record */
  Btree_pcursor m_pcur;

  /** table where undo is done */
  Table *m_table{};

  /** compiler analysis of an update */
  ulint m_cmpl_info{};

  /** update vector for a clustered index record */
  upd_t *m_update{};

  /** row reference to the next row to handle */
  DTuple *m_ref{};

  /** a copy (also fields copied to heap) of the row to handle */
  DTuple *m_row{};

  /** nullptr, or prefixes of the externally stored columns of the row */
  row_ext_t *m_ext{};

  /** nullptr, or the row after undo */
  DTuple *m_undo_row{};

  /** nullptr, or prefixes of the externally stored columns of undo_row */
  row_ext_t *m_undo_ext{};

  /** the next index whose record should be handled */
  Index *m_index{};

  /** memory heap used as auxiliary storage for row; this must be emptied
  after undo is tried on a row */
  mem_heap_t *m_heap{};

  /** data dictionary */
  Dict *m_dict{};
};
