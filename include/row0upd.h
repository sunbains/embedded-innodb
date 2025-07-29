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

/** @file include/row0upd.h
Update of a row

Created 12/27/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0pcur.h"
#include "btr0types.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0log.h"
#include "pars0types.h"
#include "que0types.h"
#include "row0row.h"
#include "row0types.h"
#include "trx0trx.h"
#include "trx0types.h"
#include "trx0undo.h"

struct Lock_sys;

#ifndef UNIV_DEBUG
#define upd_get_nth_field(update, n) (update->fields + (n))
#endif /* !UNIV_DEBUG */

/** Magic number for debug checks */
constexpr ulint UPD_NODE_MAGIC_N = 1579975;

/* Node execution states */

/** Execution came to the node from a node above and if the field has_clust_rec_x_lock
is false, we should set an intention x-lock on the table */
constexpr ulint UPD_NODE_SET_IX_LOCK = 1;

/** Clustered index record should be updated */
constexpr ulint UPD_NODE_UPDATE_CLUSTERED = 2;

/** Clustered index record should be inserted, old record is already delete marked */
constexpr ulint UPD_NODE_INSERT_CLUSTERED = 3;

/** An ordering field of the clustered index record was changed, or this is a delete
operation: should update all the secondary index records */
constexpr ulint UPD_NODE_UPDATE_ALL_SEC = 4;

/** Secondary index entries should be looked at and updated if an ordering field changed */
constexpr ulint UPD_NODE_UPDATE_SOME_SEC = 5;

/** Compilation info flags: these must fit within 3 bits; see trx0rec.h */

/** No secondary index record will be changed in the update and no ordering field
of the clustered index */
constexpr ulint UPD_NODE_NO_ORD_CHANGE = 1;

/** No record field size will be changed in the update */
constexpr ulint UPD_NODE_NO_SIZE_CHANGE = 2;

/* Update vector field */
struct upd_field_t {
  /**
   * @brief Sets an index field number to be updated by an update vector field.
   *
   * @param[in] field_no        Field number in a clustered index.
   * @param[in] index      Dictionary index.
   * @param[in] trx             Transaction.
   */
  inline void set_field_no(ulint field_no, const Index *index, Trx *trx) noexcept {
    m_orig_len = 0;
    m_field_no = field_no;

    if (unlikely(m_field_no >= index->get_n_fields())) {
      log_err(
        std::format("Trying to access field {} in but index only has %lu fields", m_field_no, index->m_name, index->get_n_fields())
      );
    }

    index->get_nth_col(m_field_no)->copy_type(dfield_get_type(&m_new_val));
  }

  /** Field number in an index, usually the clustered index, but in updating
   * a secondary index record in btr0cur.c this is the position in the secondary
   * index */
  uint16_t m_field_no{};

  /** Original length of the locally stored part of an externally stored
   * column, or 0 */
  uint16_t m_orig_len{};

  /** Expression for calculating a new value: it refers to column values and
   * constants in the symbol table of the query graph */
  que_node_t *m_exp{};

  /** New value for the column */
  dfield_t m_new_val;
};

/* Update vector structure */
struct upd_t {

  /**
   * @brief Returns a field of an update vector by field number.
   *
   * @param[in] no      Field number in the update vector.
   * 
   * @return Update vector field, or nullptr if not found.
   */
  inline const upd_field_t *get_field(ulint no) const noexcept {
    for (ulint i{}; i < m_n_fields; ++i) {
      auto uf = &m_fields[i];

      if (uf->m_field_no == no) {

        return uf;
      }
    }

    return nullptr;
  }

  /**
   * @brief Returns the nth field of an update vector.
   *
   * @param[in] n       Field position in update vector.
   * 
   * @return Update vector field.
   */
  inline const upd_field_t *get_nth_field(ulint n) const noexcept {
    ut_ad(n < m_n_fields);

    return &m_fields[n];
  }

  /**
   * @brief Returns the nth field of an update vector.
   *
   * @param[in] n       Field position in update vector.
   * 
   * @return Update vector field.
   */
  inline upd_field_t *get_nth_field(ulint n) noexcept {
    return const_cast<upd_field_t *>(static_cast<const upd_t *>(this)->get_nth_field(n));
  }

  /** New value of info bits to record; default is 0 */
  uint16_t m_info_bits{};

  /** Number of update fields */
  uint16_t m_n_fields{};

  /** Array of update fields */
  upd_field_t *m_fields{};
};

/* Update node structure which also implements the delete operation
of a row */
struct upd_node_t {
  /** Node type: QUE_NODE_UPDATE */
  que_common_t m_common;

  /** True if delete, false if update */
  bool m_is_delete{};

  /** True if searched update, false if positioned */
  bool m_searched_update{};

  /** True if the update node was created for the client interface */
  bool m_in_client_interface{};

  /** NULL or pointer to a foreign key constraint if this update node
   * is used in doing an ON DELETE or ON UPDATE operation */
  const Foreign *m_foreign{};

  /** NULL or an update node template which is used to implement
   * ON DELETE/UPDATE CASCADE or ... SET NULL for foreign keys */
  upd_node_t *m_cascade_node{};

  /** NULL or a mem heap where the cascade node is created */
  mem_heap_t *m_cascade_heap{};

  /** Query graph subtree implementing a base table cursor:
   * the rows returned will be updated */
  sel_node_t *m_select{};

  /** Persistent cursor placed on the clustered index record
   * which should be updated or deleted; the cursor is stored
   * in the graph of 'select' field above, except in the case
   * of the client interface */
  Btree_pcursor *m_pcur{};

  /** Table where updated */
  Table *m_table{};

  /** Update vector for the row */
  upd_t *m_update{};

  /** When this struct is used to implement a cascade operation
   * for foreign keys, we store here the size of the buffer
   * allocated for use as the update vector */
  uint16_t m_update_n_fields{};

  /** Symbol table nodes for the columns to retrieve from the table */
  sym_node_list_t m_columns;

  /** True if the select which retrieves the records to update
   * already sets an x-lock on the clustered record; note that it
   * must always set at least an s-lock */
  bool m_has_clust_rec_x_lock{};

  /** Information extracted during query compilation; speeds up
   * execution: UPD_NODE_NO_ORD_CHANGE and UPD_NODE_NO_SIZE_CHANGE,
   * ORed */
  ulint m_cmpl_info{};

  /** Node execution state */
  uint16_t m_state{};

  /** NULL, or the next index whose record should be updated */
  Index *m_index{};

  /** NULL, or a copy (also fields copied to heap) of the row
   * to update; this must be reset to NULL after a successful
   * update */
  DTuple *m_row{};

  /** NULL, or prefixes of the externally stored columns in
   * the old row */
  row_ext_t *m_ext{};

  /** NULL, or a copy of the updated row */
  DTuple *m_upd_row{};

  /** NULL, or prefixes of the externally stored columns in upd_row */
  row_ext_t *m_upd_ext{};

  /** Memory heap used as auxiliary storage; this must be emptied after a successful update */
  mem_heap_t *m_heap{};

  /** Table node in symbol table */
  sym_node_t *m_table_sym{};

  /** Column assignment list */
  que_node_t *m_col_assign_list{};

  /** Magic number */
  IF_DEBUG(ulint m_magic_n{UPD_NODE_MAGIC_N};)
};

struct Row_update {

  /**
   * @brief Constructor.
   * 
   * @param[in] dict_sys Dictionary system.
   */
  Row_update(Dict *dict, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Destructor.
   */
  ~Row_update() noexcept;

  /**
   * @brief Creates a row update object.
   * 
   * @param[in] dict_sys Dictionary system.
   * 
   * @return Pointer to the created row update object.
   */
  [[nodiscard]] static Row_update *create(Dict *dict, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Destroys a row update object.
   * 
   * @param[in,out] row_upd Pointer to the row update object to destroy.
   */
  static void destroy(Row_update *&row_upd) noexcept;

  /**
   * @brief Writes into the redo log the values of trx id and roll ptr and enough info
   * to determine their positions within a clustered index record.
   * 
   * @param[in] index     Clustered index.
   * @param[in] trx       Transaction.
   * @param[in] roll_ptr  Roll ptr of the undo log record.
   * @param[in] log_ptr   Pointer to a buffer of size > 20 opened in mlog.
   * @param[in] mtr       Mini-transaction.
   * 
   * @return New pointer to mlog.
   */
  [[nodiscard]] byte *write_sys_vals_to_log(const Index *index, Trx *trx, roll_ptr_t roll_ptr, byte *log_ptr, mtr_t *mtr) noexcept;

  /**
   * @brief Sets the trx id or roll ptr field of a clustered index entry.
   * 
   * This function sets the transaction ID or roll pointer field of a clustered index entry.
   * The memory buffers for the system fields in the index entry should already be allocated.
   * 
   * @param[in] entry  Index entry where the memory buffers for system fields are already allocated.
   * @param[in] index  Clustered index.
   * @param[in] type   Type of field to set (DATA_TRX_ID or DATA_ROLL_PTR).
   * @param[in] val    Value to write to the field.
   */
  void index_entry_sys_field(const DTuple *entry, const Index *index, ulint type, uint64_t val) noexcept;

  /**
   * @brief Creates an update node for a query graph.
   * 
   * @param[in] heap  Memory heap where the update node is created.
   * 
   * @return Pointer to the created update node.
   */
  [[nodiscard]] upd_node_t *node_create(mem_heap_t *heap) noexcept;

  /**
   * @brief Writes to the redo log the new values of the fields occurring in the index.
   * 
   * @param[in] update  Update vector.
   * @param[in] log_ptr Pointer to mlog buffer. Must contain at least MLOG_BUF_MARGIN bytes of free space.
   *                    The buffer is closed within this function.
   * @param[in] mtr     Mini-transaction into whose log to write.
   */
  void index_write_log(const upd_t *update, byte *log_ptr, mtr_t *mtr) noexcept;

  /**
   * @brief Checks if a row update changes the size of any field in the index or if any field to be updated is stored externally.
   * 
   * @param[in] index    Index.
   * @param[in] offsets  Column offsets in the record.
   * @param[in] update   Update vector.
   * 
   * @return true if the update changes the size of any field in the index or if the field is external in the record or update.
   */
  [[nodiscard]] bool changes_field_size_or_external(const Index *index, const ulint *offsets, const upd_t *update) noexcept;

  /**
   * @brief Replaces the new column values stored in the update vector to the given record. No field size changes are allowed.
   * 
   * @param[in,out] rec      Record where values are replaced.
   * @param[in] index        The index the record belongs to.
   * @param[in] offsets      Array returned by Phy_rec::get_col_offsets().
   * @param[in] update       Update vector.
   */
  void rec_in_place(rec_t *rec, const Index *index, const ulint *offsets, const upd_t *update) noexcept;

  /**
   * @brief Builds an update vector from those fields which in a secondary index entry
   * differ from a record that has the equal ordering fields. NOTE: we compare
   * the fields as binary strings!
   * 
   * @param[in] index  Index.
   * @param[in] entry  Entry to insert.
   * @param[in] rec    Secondary index record.
   * @param[in] trx    Transaction.
   * @param[in] heap   Memory heap from which allocated.
   * 
   * @return Update vector of differing fields.
   */
  [[nodiscard]] static upd_t *build_sec_rec_difference_binary(
    const Index *index, const DTuple *entry, const rec_t *rec, Trx *trx, mem_heap_t *heap
  ) noexcept;

  /**
   * @brief Builds an update vector from those fields, excluding the roll ptr and
   * trx id fields, which in an index entry differ from a record that has
   * the equal ordering fields. NOTE: we compare the fields as binary strings!
   * 
   * @param[in] index  Clustered index.
   * @param[in] entry  Entry to insert.
   * @param[in] rec    Clustered index record.
   * @param[in] trx    Transaction.
   * @param[in] heap   Memory heap from which allocated.
   * 
   * @return Update vector of differing fields, excluding roll ptr and trx id.
   */
  [[nodiscard]] upd_t *build_difference_binary(
    const Index *index, const DTuple *entry, const rec_t *rec, Trx *trx, mem_heap_t *heap
  ) noexcept;

  /**
   * @brief Replaces the new column values stored in the update vector to the index
   * entry given.
   * 
   * @param[in,out] entry  Index entry where replaced; the clustered index record must be
   *                       covered by a lock or a page latch to prevent deletion (rollback or purge).
   * @param[in] index      Index; NOTE that this may also be a non-clustered index.
   * @param[in] update     An update vector built for the index so that the field number in an upd_field is the index position.
   * @param[in] order_only If true, limit the replacement to ordering fields of index; note that this does not work for non-clustered indexes.
   * @param[in] heap       Memory heap for allocating and copying the new values.
   */
  void index_replace_new_col_vals_index_pos(
    DTuple *entry, const Index *index, const upd_t *update, bool order_only, mem_heap_t *heap
  ) noexcept;

  /**
   * @brief Replaces the new column values stored in the update vector to the index
   * entry given.
   * 
   * @param[in,out] entry  Index entry where replaced; the clustered index record must be
   *                       covered by a lock or a page latch to prevent deletion (rollback or purge).
   * @param[in] index      Index; NOTE that this may also be a non-clustered index.
   * @param[in] update     An update vector built for the CLUSTERED index so that the field number in an upd_field is the clustered index position.
   * @param[in] heap       Memory heap for allocating and copying the new values.
   */
  void index_replace_new_col_vals(DTuple *entry, Index *index, const upd_t *update, mem_heap_t *heap) noexcept;

  /**
   * @brief Replaces the new column values stored in the update vector.
   * 
   * @param[in,out] row    Row where replaced, indexed by col_no; the clustered index record must be
   *                       covered by a lock or a page latch to prevent deletion (rollback or purge).
   * @param[out] ext       NULL, or externally stored column prefixes.
   * @param[in] index      Clustered index.
   * @param[in] update     An update vector built for the clustered index.
   * @param[in] heap       Memory heap.
   */
  static void replace(DTuple *row, row_ext_t **ext, const Index *index, const upd_t *update, mem_heap_t *heap) noexcept;

  /**
   * @brief Checks if an update vector changes an ordering field of an index record.
   * This function is fast if the update vector is short or the number of ordering
   * fields in the index is small. Otherwise, this can be quadratic.
   * NOTE: we compare the fields as binary strings!
   * 
   * @param[in] row    Old value of row, or NULL if the row and the data values in update are not
   *                   known when this function is called, e.g., at compile time.
   * @param[in] index  Index of the record.
   * @param[in] update Update vector for the row; NOTE: the field numbers in this MUST be clustered index positions!
   * 
   * @return true if update vector changes an ordering field in the index record.
   */
  [[nodiscard]] static bool changes_ord_field_binary(const DTuple *row, Index *index, const upd_t *update) noexcept;

  /**
   * @brief Checks if an update vector changes an ordering field of an index record.
   * This function is fast if the update vector is short or the number of ordering
   * fields in the index is small. Otherwise, this can be quadratic.
   * NOTE: we compare the fields as binary strings!
   * 
   * @param[in] table  Table.
   * @param[in] update Update vector for the row.
   * 
   * @return true if update vector may change an ordering field in an index record.
   */
  [[nodiscard]] bool changes_some_index_ord_field_binary(const Table *table, const upd_t *update) noexcept;

  /**
   * @brief Updates a row in a table. This is a high-level function used
   * in SQL execution graphs.
   * 
   * @param[in] thr  Query thread.
   * 
   * @return Query thread to run next or NULL.
   */
  [[nodiscard]] que_thr_t *step(que_thr_t *thr) noexcept;

  /**
   * @brief Parses the log data of system field values.
   * 
   * @param[in] ptr       Buffer.
   * @param[in] end_ptr   Buffer end.
   * @param[out] pos      TRX_ID position in record.
   * @param[out] trx_id   Transaction id.
   * @param[out] roll_ptr Roll ptr.
   * 
   * @return Log data end or NULL.
   */
  [[nodiscard]] byte *parse_sys_vals(byte *ptr, byte *end_ptr, ulint *pos, trx_id_t *trx_id, roll_ptr_t *roll_ptr) noexcept;

  /**
   * @brief Updates the trx id and roll ptr field in a clustered index record in
   * database recovery.
   * 
   * @param[in,out] rec      Record.
   * @param[in] offsets      Array returned by Phy_rec::get_col_offsets().
   * @param[in] pos          TRX_ID position in rec.
   * @param[in] trx_id       Transaction id.
   * @param[in] roll_ptr     Roll ptr of the undo log record.
   */
  void rec_sys_fields_in_recovery(rec_t *rec, const ulint *offsets, ulint pos, trx_id_t trx_id, roll_ptr_t roll_ptr) noexcept;

  /**
   * @brief Parses the log data written by row_upd_index_write_log.
   * 
   * @param[in] ptr        Buffer.
   * @param[in] end_ptr    Buffer end.
   * @param[in] heap       Memory heap where update vector is built.
   * @param[out] update_out Update vector.
   * 
   * @return Log data end or NULL.
   */
  [[nodiscard]] byte *index_parse(byte *ptr, byte *end_ptr, mem_heap_t *heap, upd_t **update_out) noexcept;

  /**
   * @brief Creates a query graph node of 'update' type to be used in the engine
   * interface.
   * 
   * @param[in] table  Table to update.
   * @param[in] heap   Memory heap from which allocated.
   * 
   * @return Update node.
   */
  [[nodiscard]] upd_node_t *create_update_node(Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Creates an update vector object.
   * 
   * This function creates an update vector object with the specified number of fields.
   * Memory for the update vector object is allocated from the provided heap.
   * 
   * @param[in] n     Number of fields.
   * @param[in] heap  Heap from which memory is allocated.
   * 
   * @return          Pointer to the created update vector object.
   */
  inline static upd_t *upd_create(ulint n, mem_heap_t *heap) noexcept {
    auto ptr = mem_heap_alloc(heap, sizeof(upd_t));
    auto update = new (ptr) upd_t;

    update->m_n_fields = n;
    update->m_fields = reinterpret_cast<upd_field_t *>(mem_heap_alloc(heap, sizeof(upd_field_t) * n));

    return update;
  }

  /**
   * @brief Returns the number of fields in the update vector.
   * 
   * This function returns the number of fields in the update vector, which is equal to the number of columns
   * to be updated by the update vector.
   * 
   * @param[in] update  Update vector.
   * 
   * @return Number of fields.
   */
  inline static ulint upd_get_n_fields(const upd_t *update) noexcept { return update->m_n_fields; }

  /**
   * @brief Updates the trx id and roll ptr field in a clustered index record when
   * a row is updated or marked deleted.
   *
   * @param[in,out] rec       Record to be updated.
   * @param[in] index    Clustered index.
   * @param[in] offsets       Column offsets in the record.
   * @param[in] trx           Transaction.
   * @param[in] roll_ptr      Roll pointer of the undo log record.
   */
  inline static void rec_sys_fields(rec_t *rec, const Index *index, const ulint *offsets, Trx *trx, roll_ptr_t roll_ptr) {
    ulint offset = index->m_trx_id_offset;

    if (offset == 0) {
      offset = row_get_trx_id_offset(rec, index, offsets);
    }

    static_assert(DATA_TRX_ID + 1 == DATA_ROLL_PTR, "error DATA_TRX_ID + 1 != DATA_ROLL_PTR");

    srv_trx_sys->write_trx_id(rec + offset, trx->m_id);

    trx_write_roll_ptr(rec + offset + DATA_TRX_ID_LEN, roll_ptr);
  }

#ifdef UNIT_TEST
 private:
#endif /* UNIT_TEST */

  /**
   * @brief Checks if an update vector changes some of the first ordering fields of an index record.
   * 
   * This is only used in foreign key checks and we can assume that index does not contain column prefixes.
   * 
   * @param[in] entry Old value of index entry.
   * @param[in] index Index of entry.
   * @param[in] update Update vector for the row.
   * @param[in] n How many first fields to check.
   * 
   * @return true if changes.
   */
  [[nodiscard]] bool changes_first_fields_binary(DTuple *entry, const Index *index, const upd_t *update, ulint n) noexcept;

  /**
   * @brief Checks if the index is currently mentioned as a referenced
   * index in a foreign key constraint.
   *
   * @note Since we do not hold dict_operation_lock when leaving the
   * function, it may be that the referencing table has been dropped
   * when we leave this function. This function is only for heuristic use.
   *
   * @param[in] index The index to check.
   * @param[in] trx The transaction.
   * 
   * @return true if the index is referenced, false otherwise.
   */
  [[nodiscard]] bool index_is_referenced(const Index *index, Trx *trx) noexcept;

  /**
   * @brief Checks if possible foreign key constraints hold after a delete of the record under pcur.
   *
   * @note This function will temporarily commit mtr and lose the pcur position.
   *
   * @param[in] node Row update node.
   * @param[in] pcur Cursor positioned on a record. The cursor position is lost in this function.
   * @param[in] table Table in question.
   * @param[in] index Index of the cursor.
   * @param[in,out] offsets Phy_rec::get_col_offsets(index, pcur.rec).
   * @param[in] thr Query thread.
   * @param[in] mtr Mini-transaction.
   * 
   * @return DB_SUCCESS or an error code.
   */
  [[nodiscard]] db_err check_references_constraints(
    upd_node_t *node, Btree_pcursor *pcur, Table *table, const Index *index, ulint *offsets, que_thr_t *thr, mtr_t *mtr
  ) noexcept;

  /**
  * @brief Fetch a prefix of an externally stored column.
  * 
  * This is similar to row_ext_lookup(), but the row_ext_t holds the old values
  * of the column and must not be poisoned with the new values.
  * 
  * @param[in] data 'Internally' stored part of the field containing also the reference to the external part.
  * @param[in] local_len Length of data, in bytes.
  * @param[in,out] len Length of prefix to fetch; fetched length of the prefix.
  * @param[in] heap Heap where to allocate.
  * 
  * @return BLOB prefix.
  */
  [[nodiscard]] byte *ext_fetch(const byte *data, ulint local_len, ulint *len, mem_heap_t *heap) noexcept;

  /**
   * @brief Replaces the new column value stored in the update vector in the given index entry field.
   * 
   * @param[in,out] dfield Data field of the index entry.
   * @param[in] field Index field.
   * @param[in] col Field's column.
   * @param[in] uf Update field.
   * @param[in] heap Memory heap for allocating and copying the new value.
   */
  void index_replace_new_col_val(
    dfield_t *dfield, const Field *field, const Column *col, const upd_field_t *uf, mem_heap_t *heap
  ) noexcept;

  /**
   * Copies the column values from a record. 
   *
   * @param[in] rec The record in a clustered index.
   * @param[in] offsets The array returned by Phy_rec::get_col_offsets().
   * @param[in] column The first column in a column list, or nullptr.
   */
  inline void copy_columns(rec_t *rec, const ulint *offsets, sym_node_t *column) noexcept;

  /**
   * Calculates the new values for fields to update. Note that
   * copy_columns must have been called first.
   * 
   * @param[in,out] update Update vector.
   */
  inline void eval_new_vals(upd_t *update) noexcept;

  /**
   * @brief Stores to the heap the row on which the node->pcur is positioned.
   * 
   * @param[in] node Row update node.
   */
  void store_row(upd_node_t *node) noexcept;

  /**
   * @brief Updates a secondary index entry of a row.
   * 
   * @param[in] node Row update node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err sec_index_entry(upd_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Updates the secondary index record if it is changed in the row update or deletes it if this is a delete.
   * 
   * @param[in] node Row update node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] inline db_err sec_step(upd_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Marks the clustered index record deleted and inserts the updated version
   * of the record to the index. This function should be used when the ordering
   * fields of the clustered index record change. This should be quite rare in
   * database applications.
   * 
   * @param[in] node Row update node.
   * @param[in] index Clustered index of the record.
   * @param[in] thr Query thread.
   * @param[in] check_ref True if index may be referenced in a foreign key constraint.
   * @param[in] mtr Mini-transaction; gets committed here.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err clust_rec_by_insert(
    upd_node_t *node, const Index *index, que_thr_t *thr, bool check_ref, mtr_t *mtr
  ) noexcept;

  /**
   * @brief Updates a clustered index record of a row when the ordering fields do not change.
   * 
   * @param[in] node Row update node.
   * @param[in] index Clustered index.
   * @param[in] thr Query thread.
   * @param[in] mtr Mini-transaction; gets committed here.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err clust_rec(upd_node_t *node, const Index *index, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * @brief Delete marks a clustered index record.
   * 
   * @param[in] node Row update node.
   * @param[in] index Clustered index.
   * @param[in,out] offsets Phy_rec::get_col_offsets() for the record under the cursor.
   * @param[in] thr Query thread.
   * @param[in] check_ref True if index may be referenced in a foreign key constraint.
   * @param[in] mtr Mini-transaction; gets committed here.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code.
   */
  [[nodiscard]] db_err del_mark_clust_rec(
    upd_node_t *node, const Index *index, ulint *offsets, que_thr_t *thr, bool check_ref, mtr_t *mtr
  ) noexcept;

  /**
   * @brief Updates the clustered index record.
   * 
   * @param[in] node Row update node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if operation successfully completed, DB_LOCK_WAIT in case of a lock wait, else error code.
   */
  [[nodiscard]] db_err clust_step(upd_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Updates the affected index records of a row.
   * 
   * When the control is transferred to this node, we assume that we have a 
   * persistent cursor which was on a record, and the position of the cursor 
   * is stored in the cursor.
   * 
   * @param[in] node Row update node.
   * @param[in] thr Query thread.
   * 
   * @return DB_SUCCESS if operation successfully completed, else error code or DB_LOCK_WAIT.
   */
  [[nodiscard]] db_err update(upd_node_t *node, que_thr_t *thr) noexcept;

  /**
   * @brief Dictionary.
   */
  Dict *m_dict{};

  /**
   * @brief Lock system.
   */
  Lock_sys *m_lock_sys{};
};
