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

/** @file row/row0ins.c
Insert into a table

Created 4/20/1996 Heikki Tuuri
*******************************************************/

#include "row0ins.h"

#include "api0misc.h"
#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0lru.h"
#include "data0data.h"
#include "dict0boot.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "log0log.h"
#include "mach0data.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0upd.h"
#include "trx0undo.h"
#include "usr0sess.h"

constexpr ulint ROW_INS_PREV = 1;
constexpr ulint ROW_INS_NEXT = 2;

ins_node_t *row_ins_node_create(ib_ins_mode_t ins_type, dict_table_t *table, mem_heap_t *heap) {
  auto node = reinterpret_cast<ins_node_t *>(mem_heap_alloc(heap, sizeof(ins_node_t)));

  node->common.type = QUE_NODE_INSERT;

  node->ins_type = ins_type;

  node->state = INS_NODE_SET_IX_LOCK;
  node->table = table;
  node->index = nullptr;
  node->entry = nullptr;

  node->select = nullptr;

  node->trx_id = 0;

  node->entry_sys_heap = mem_heap_create(128);

  node->magic_n = INS_NODE_MAGIC_N;

  return node;
}

void row_ins_node_create_entry_list(ins_node_t *node) {
  ut_ad(node->entry_sys_heap);

  UT_LIST_INIT(node->entry_list);

  auto index = dict_table_get_first_index(node->table);

  while (index != nullptr) {
    auto entry = row_build_index_entry(node->row, nullptr, index, node->entry_sys_heap);

    UT_LIST_ADD_LAST(node->entry_list, entry);

    index = dict_table_get_next_index(index);
  }
}

/** Adds system field buffers to a row.
@param[in,out] node             Insert node to initialize. */
static void row_ins_alloc_sys_fields(ins_node_t *node) {
  dtuple_t *row;
  dict_table_t *table;
  mem_heap_t *heap;
  const dict_col_t *col;
  dfield_t *dfield;
  byte *ptr;

  row = node->row;
  table = node->table;
  heap = node->entry_sys_heap;

  ut_ad(row && table && heap);
  ut_ad(dtuple_get_n_fields(row) == dict_table_get_n_cols(table));

  /* 1. Allocate buffer for row id */

  col = dict_table_get_sys_col(table, DATA_ROW_ID);

  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));

  ptr = mem_heap_zalloc(heap, DATA_ROW_ID_LEN);

  dfield_set_data(dfield, ptr, DATA_ROW_ID_LEN);

  node->row_id_buf = ptr;

  /* 3. Allocate buffer for trx id */

  col = dict_table_get_sys_col(table, DATA_TRX_ID);

  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
  ptr = mem_heap_zalloc(heap, DATA_TRX_ID_LEN);

  dfield_set_data(dfield, ptr, DATA_TRX_ID_LEN);

  node->trx_id_buf = ptr;

  /* 4. Allocate buffer for roll ptr */

  col = dict_table_get_sys_col(table, DATA_ROLL_PTR);

  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
  ptr = mem_heap_zalloc(heap, DATA_ROLL_PTR_LEN);

  dfield_set_data(dfield, ptr, DATA_ROLL_PTR_LEN);
}

void row_ins_node_set_new_row(ins_node_t *node, dtuple_t *row) {
  node->state = INS_NODE_SET_IX_LOCK;
  node->index = nullptr;
  node->entry = nullptr;

  node->row = row;

  mem_heap_empty(node->entry_sys_heap);

  /* Create templates for index entries */

  row_ins_node_create_entry_list(node);

  /* Allocate from entry_sys_heap buffers for sys fields */

  row_ins_alloc_sys_fields(node);

  /* As we allocated a new trx id buf, the trx id should be written
  there again: */

  node->trx_id = 0;
}

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
static db_err row_ins_sec_index_entry_by_modify(ulint mode, Btree_cursor *btr_cur, const dtuple_t *entry, que_thr_t *thr, mtr_t *mtr) noexcept {
  db_err err;
  big_rec_t *dummy_big_rec;

  auto rec = btr_cur->get_rec();

  ut_ad(!dict_index_is_clust(btr_cur->m_index));
  ut_ad(rec_get_deleted_flag(rec));

  /* We know that in the alphabetical ordering, entry and rec are
  identified. But in their binary form there may be differences if
  there are char fields in them. Therefore we have to calculate the
  difference. */

  auto heap = mem_heap_create(1024);
  auto update = row_upd_build_sec_rec_difference_binary(btr_cur->m_index, entry, rec, thr_get_trx(thr), heap);

  if (mode == BTR_MODIFY_LEAF) {
    /* Try an optimistic updating of the record, keeping changes
    within the page */

    err = btr_cur->optimistic_update(BTR_KEEP_SYS_FLAG, update, 0, thr, mtr);
    switch (err) {
      case DB_OVERFLOW:
      case DB_UNDERFLOW:
        err = DB_FAIL;
        break;
      default:
        break;
    }
  } else {
    ut_a(mode == BTR_MODIFY_TREE);

    if (srv_buf_pool->m_LRU->buf_pool_running_out()) {

      err = DB_LOCK_TABLE_FULL;

      goto func_exit;
    }

    err = btr_cur->pessimistic_update(BTR_KEEP_SYS_FLAG, &heap, &dummy_big_rec, update, 0, thr, mtr);
    ut_ad(!dummy_big_rec);
  }

func_exit:
  mem_heap_free(heap);

  return err;
}

/** Does an insert operation by delete unmarking and updating a delete marked
existing record in the index. This situation can occur if the delete marked
record is kept in the index for consistent reads.
@return	DB_SUCCESS, DB_FAIL, or error code */
static db_err row_ins_clust_index_entry_by_modify(
  ulint mode,            /*!< in: BTR_MODIFY_LEAF or BTR_MODIFY_TREE,
                           depending on whether mtr holds just a leaf
                           latch or also a tree latch */
  Btree_cursor *btr_cur,     /*!< in: B-tree btr_cur */
  mem_heap_t **heap,     /*!< in/out: pointer to memory heap, or NULL */
  big_rec_t **big_rec,   /*!< out: possible big rec vector of fields
                           which have to be stored externally by the
                           caller */
  const dtuple_t *entry, /*!< in: index entry to insert */
  que_thr_t *thr,        /*!< in: query thread */
  mtr_t *mtr
) /*!< in: mtr; must be committed before
                           latching any further pages */
{
  rec_t *rec;
  upd_t *update;
  db_err err;

  ut_ad(dict_index_is_clust(btr_cur->m_index));

  *big_rec = nullptr;

  rec = btr_cur->get_rec();

  ut_ad(rec_get_deleted_flag(rec));

  if (!*heap) {
    *heap = mem_heap_create(1024);
  }

  /* Build an update vector containing all the fields to be modified;
  NOTE that this vector may NOT contain system columns trx_id or
  roll_ptr */

  update = row_upd_build_difference_binary(btr_cur->m_index, entry, rec, thr_get_trx(thr), *heap);

  if (mode == BTR_MODIFY_LEAF) {
    /* Try optimistic updating of the record, keeping changes
    within the page */

    err = btr_cur->optimistic_update(0, update, 0, thr, mtr);

    switch (err) {
      case DB_OVERFLOW:
      case DB_UNDERFLOW:
        err = DB_FAIL;
        break;
      default:
        break;
    }
  } else {
    ut_a(mode == BTR_MODIFY_TREE);
    if (srv_buf_pool->m_LRU->buf_pool_running_out()) {

      return DB_LOCK_TABLE_FULL;
    }
    err = btr_cur->pessimistic_update(0, heap, big_rec, update, 0, thr, mtr);
  }

  return err;
}

/** Returns true if in a cascaded update/delete an ancestor node of node
updates (not DELETE, but UPDATE) table.
@return	true if an ancestor updates table */
static bool row_ins_cascade_ancestor_updates_table(
  que_node_t *node, /*!< in: node in a query graph */
  dict_table_t *table
) /*!< in: table */
{
  que_node_t *parent;
  upd_node_t *upd_node;

  parent = que_node_get_parent(node);

  while (que_node_get_type(parent) == QUE_NODE_UPDATE) {

    upd_node = static_cast<upd_node_t *>(parent);

    if (upd_node->table == table && upd_node->is_delete == false) {

      return true;
    }

    parent = que_node_get_parent(parent);

    ut_a(parent);
  }

  return false;
}

/** Returns the number of ancestor UPDATE or DELETE nodes of a
cascaded update/delete node.
@return	number of ancestors */
static ulint row_ins_cascade_n_ancestors(que_node_t *node) /*!< in: node in a query graph */
{
  que_node_t *parent;
  ulint n_ancestors = 0;

  parent = que_node_get_parent(node);

  while (que_node_get_type(parent) == QUE_NODE_UPDATE) {
    n_ancestors++;

    parent = que_node_get_parent(parent);

    ut_a(parent);
  }

  return n_ancestors;
}

/** Calculates the update vector node->cascade->update for a child table in
a cascaded update.
@return number of fields in the calculated update vector; the value
can also be 0 if no foreign key fields changed; the returned value is
ULINT_UNDEFINED if the column type in the child table is too short to
fit the new value in the parent table: that means the update fails */
static ulint row_ins_cascade_calc_update_vec(
  upd_node_t *node,        /*!< in: update node of the parent
                             table */
  dict_foreign_t *foreign, /*!< in: foreign key constraint whose
                             type is != 0 */
  mem_heap_t *heap
) /*!< in: memory heap to use as
                             temporary storage */
{
  upd_node_t *cascade = node->cascade_node;
  dict_table_t *table = foreign->foreign_table;
  dict_index_t *index = foreign->foreign_index;
  upd_t *update;
  upd_field_t *ufield;
  dict_table_t *parent_table;
  dict_index_t *parent_index;
  upd_t *parent_update;
  upd_field_t *parent_ufield;
  ulint n_fields_updated;
  ulint parent_field_no;
  ulint i;
  ulint j;

  ut_a(node);
  ut_a(foreign);
  ut_a(cascade);
  ut_a(table);
  ut_a(index);

  /* Calculate the appropriate update vector which will set the fields
  in the child index record to the same value (possibly padded with
  spaces if the column is a fixed length CHAR or FIXBINARY column) as
  the referenced index record will get in the update. */

  parent_table = node->table;
  ut_a(parent_table == foreign->referenced_table);
  parent_index = foreign->referenced_index;
  parent_update = node->update;

  update = cascade->update;

  update->info_bits = 0;
  update->n_fields = foreign->n_fields;

  n_fields_updated = 0;

  for (i = 0; i < foreign->n_fields; i++) {

    parent_field_no = dict_table_get_nth_col_pos(parent_table, dict_index_get_nth_col_no(parent_index, i));

    for (j = 0; j < parent_update->n_fields; j++) {
      parent_ufield = parent_update->fields + j;

      if (parent_ufield->field_no == parent_field_no) {

        ulint min_size;
        const dict_col_t *col;
        ulint ufield_len;

        col = dict_index_get_nth_col(index, i);

        /* A field in the parent index record is
        updated. Let us make the update vector
        field for the child table. */

        ufield = update->fields + n_fields_updated;

        ufield->field_no = dict_table_get_nth_col_pos(table, dict_col_get_no(col));
        ufield->exp = nullptr;

        ufield->new_val = parent_ufield->new_val;
        ufield_len = dfield_get_len(&ufield->new_val);

        /* Clear the "external storage" flag */
        dfield_set_len(&ufield->new_val, ufield_len);

        /* Do not allow a NOT NULL column to be
        updated as NULL */

        if (dfield_is_null(&ufield->new_val) && (col->prtype & DATA_NOT_NULL)) {

          return ULINT_UNDEFINED;
        }

        /* If the new value would not fit in the
        column, do not allow the update */

        if (!dfield_is_null(&ufield->new_val) &&
            dtype_get_at_most_n_mbchars(
                col->prtype, col->mbminlen, col->mbmaxlen, col->len, ufield_len, (const char*)dfield_get_data(&ufield->new_val)
            ) < ufield_len) {

          return ULINT_UNDEFINED;
        }

        /* If the parent column type has a different
        length than the child column type, we may
        need to pad with spaces the new value of the
        child column */

        min_size = dict_col_get_min_size(col);

        /* Because UNIV_SQL_NULL (the marker
        of SQL NULL values) exceeds all possible
        values of min_size, the test below will
        not hold for SQL NULL columns. */

        if (min_size > ufield_len) {

          char *pad_start;
          const char *pad_end;
          auto padded_data = reinterpret_cast<char *>(mem_heap_alloc(heap, min_size));

          pad_start = padded_data + ufield_len;
          pad_end = padded_data + min_size;

          memcpy(padded_data, dfield_get_data(&ufield->new_val), dfield_get_len(&ufield->new_val));

          switch (expect(col->mbminlen, 1)) {
            default:
              ut_error;
              return ULINT_UNDEFINED;
            case 1:
              if (unlikely(dtype_get_charset_coll(col->prtype) == DATA_CLIENT_BINARY_CHARSET_COLL)) {
                /* Do not pad BINARY
              columns. */
                return ULINT_UNDEFINED;
              }

              /* space=0x20 */
              memset(pad_start, 0x20, pad_end - pad_start);
              break;
            case 2:
              /* space=0x0020 */
              ut_a(!(ufield_len % 2));
              ut_a(!(min_size % 2));
              do {
                *pad_start++ = 0x00;
                *pad_start++ = 0x20;
              } while (pad_start < pad_end);
              break;
          }

          dfield_set_data(&ufield->new_val, padded_data, min_size);
        }

        n_fields_updated++;
      }
    }
  }

  update->n_fields = n_fields_updated;

  return n_fields_updated;
}

/** Set detailed error message associated with foreign key errors for
the given transaction. */
static void row_ins_set_detailed(
  trx_t *trx, /*!< in: transaction */
  dict_foreign_t *foreign
) /*!< in: foreign key constraint */
{
  ut_print_name(foreign->foreign_table_name);
  dict_print_info_on_foreign_key_in_create_format(ib_stream, trx, foreign, false);
  trx_set_detailed_error(trx, "foreign key error");
}

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
static void row_ins_foreign_report_err(
  const char *errstr,
  que_thr_t *thr,
  dict_foreign_t *foreign,
  const rec_t *rec,
  const dtuple_t *entry
) {
  auto trx = thr_get_trx(thr);

  row_ins_set_detailed(trx, foreign);

  mutex_enter(&dict_foreign_err_mutex);

  log_info(" Transaction:");
  log_info(trx_to_string(trx, 600));
  log_info("Foreign key constraint fails for table ", foreign->foreign_table_name, ": ");
  dict_print_info_on_foreign_key_in_create_format(ib_stream, trx, foreign, true);
  log_info(errstr);
  log_info(" in parent table, in index ", foreign->referenced_index->name);

  if (entry != nullptr) {
    log_err(" tuple:");
    dtuple_print(ib_stream, entry);
  }

  log_err("\nBut in child table ", foreign->foreign_table_name, ", in index ", foreign->foreign_index->name);

  if (rec != nullptr) {
    log_err(", there is a record:");
    log_err(rec_to_string(rec));
  } else {
    log_err(", the record is not available");
  }

  mutex_exit(&dict_foreign_err_mutex);
}


/** Reports a foreign key error to ib_stream when we are trying to add an index
entry to a child table. Note that the adding may be the result of an update,
too. */
static void row_ins_foreign_report_add_err(
  trx_t *trx,              /*!< in: transaction */
  dict_foreign_t *foreign, /*!< in: foreign key constraint */
  const rec_t *rec,        /*!< in: a record in the parent table:
                             it does not match entry because we
                             have an error! */
  const dtuple_t *entry
) /*!< in: index entry to insert in the
                             child table */
{
  row_ins_set_detailed(trx, foreign);

  mutex_enter(&dict_foreign_err_mutex);

  log_err(" Transaction:");
  log_err(trx_to_string(trx, 600));
  log_err("Foreign key constraint fails for table ", foreign->foreign_table_name, ":");
  dict_print_info_on_foreign_key_in_create_format(ib_stream, trx, foreign, true);
  log_err("Trying to add in child table, in index ", foreign->foreign_index->name);

  if (entry != nullptr) {
    log_err(" tuple:");
    /* TODO: DB_TRX_ID and DB_ROLL_PTR may be uninitialized.
    It would be better to only display the user columns. */
    dtuple_print(ib_stream, entry);
  }

  log_err("But in parent table ", foreign->referenced_table_name, ", in index ", foreign->referenced_index->name);
  log_err(", the closest match we can find is record:");

  if (rec != nullptr && page_rec_is_supremum(rec)) {
    /* If the btr_cur ended on a supremum record, it is better
    to report the previous record in the error message, so that
    the user gets a more descriptive error message. */
    rec = page_rec_get_prev_const(rec);
  }

  if (rec != nullptr) {
    log_err(rec_to_string(rec));
  }
  log_err("\n");

  mutex_exit(&dict_foreign_err_mutex);
}

/** Does a cascaded delete or set null in a foreign key operation.
@return	error code or DB_SUCCESS */
static db_err row_update_cascade_client(
  que_thr_t *thr,   /*!< in: query thread */
  upd_node_t *node, /*!< in: update node used in the cascade
                         or set null operation */
  dict_table_t *table
) /*!< in: table where we do the operation */
{
  auto trx = thr_get_trx(thr);

run_again:
  thr->run_node = node;
  thr->prev_node = node;

  row_upd_step(thr);

  auto err = trx->error_state;

  /* Note that the cascade node is a subnode of another InnoDB
  query graph node. We do a normal lock wait in this node, but
  all errors are handled by the parent node. */

  if (err == DB_LOCK_WAIT) {
    /* Handle lock wait here */

    que_thr_stop_client(thr);

    InnoDB::suspend_user_thread(thr);

    /* Note that a lock wait may also end in a lock wait timeout,
    or this transaction is picked as a victim in selective
    deadlock resolution */

    if (trx->error_state != DB_SUCCESS) {

      return trx->error_state;
    }

    /* Retry operation after a normal lock wait */

    goto run_again;
  }

  if (err != DB_SUCCESS) {

    return err;
  }

  if (node->is_delete) {
    if (table->stat_n_rows > 0) {
      table->stat_n_rows--;
    }

    srv_n_rows_deleted++;
  } else {
    srv_n_rows_updated++;
  }

  ib_update_statistics_if_needed(table);

  return err;
}

/**
 * Perform referential actions or checks when a parent row is
 * deleted or updated and the constraint had an ON DELETE or
 * ON UPDATE condition which was not RESTRICT.
 *
 * @param thr     in: query thread whose run_node is an update node
 * @param foreign in: foreign key constraint whose type is != 0
 * @param pcur    in: btr_cur placed on a matching index record in the child table
 * @param entry   in: index entry in the parent table
 * @param mtr     in: mtr holding the latch of pcur page
 * @return        DB_SUCCESS, DB_LOCK_WAIT, or error code
 */
static db_err row_ins_foreign_check_on_constraint(
  que_thr_t *thr,
  dict_foreign_t *foreign,
  Btree_pcursor *pcur,
  dtuple_t *entry,
  mtr_t *mtr
)
{
  upd_node_t *node;
  upd_node_t *cascade;
  dict_table_t *table = foreign->foreign_table;
  dict_index_t *index;
  dict_index_t *clust_index;
  dtuple_t *ref;
  mem_heap_t *upd_vec_heap = nullptr;
  const rec_t *rec;
  const rec_t *clust_rec;
  const Buf_block *clust_block;
  upd_t *update;
  ulint n_to_update;
  db_err err;
  ulint i;
  trx_t *trx;
  mem_heap_t *tmp_heap = nullptr;

  ut_a(thr);
  ut_a(foreign);
  ut_a(pcur);
  ut_a(mtr);

  trx = thr_get_trx(thr);

  node = static_cast<upd_node_t *>(thr->run_node);

  if (node->is_delete && 0 == (foreign->type & (DICT_FOREIGN_ON_DELETE_CASCADE | DICT_FOREIGN_ON_DELETE_SET_NULL))) {

    row_ins_foreign_report_err("Trying to delete", thr, foreign, pcur->get_rec(), entry);

    return DB_ROW_IS_REFERENCED;
  }

  if (!node->is_delete && 0 == (foreign->type & (DICT_FOREIGN_ON_UPDATE_CASCADE | DICT_FOREIGN_ON_UPDATE_SET_NULL))) {
    /* This is an UPDATE */

    row_ins_foreign_report_err("Trying to update", thr, foreign, pcur->get_rec(), entry);

    return DB_ROW_IS_REFERENCED;
  }

  if (node->cascade_node == nullptr) {
    /* Extend our query graph by creating a child to current
    update node. The child is used in the cascade or set null
    operation. */

    node->cascade_heap = mem_heap_create(128);
    node->cascade_node = row_create_update_node(table, node->cascade_heap);
    que_node_set_parent(node->cascade_node, node);
  }

  /* Initialize cascade_node to do the operation we want. Note that we
  use the SAME cascade node to do all foreign key operations of the
  SQL DELETE: the table of the cascade node may change if there are
  several child tables to the table where the delete is done! */

  cascade = node->cascade_node;

  cascade->table = table;

  cascade->foreign = foreign;

  if (node->is_delete && (foreign->type & DICT_FOREIGN_ON_DELETE_CASCADE)) {
    cascade->is_delete = true;
  } else {
    cascade->is_delete = false;

    if (foreign->n_fields > cascade->update_n_fields) {
      /* We have to make the update vector longer */

      cascade->update = upd_create(foreign->n_fields, node->cascade_heap);
      cascade->update_n_fields = foreign->n_fields;
    }
  }

  /* We do not allow cyclic cascaded updating (DELETE is allowed,
  but not UPDATE) of the same table, as this can lead to an infinite
  cycle. Check that we are not updating the same table which is
  already being modified in this cascade chain. We have to check
  this also because the modification of the indexes of a 'parent'
  table may still be incomplete, and we must avoid seeing the indexes
  of the parent table in an inconsistent state! */

  if (!cascade->is_delete && row_ins_cascade_ancestor_updates_table(cascade, table)) {

    /* We do not know if this would break foreign key
    constraints, but play safe and return an error */

    err = DB_ROW_IS_REFERENCED;

    row_ins_foreign_report_err(
      "Trying an update, possibly causing a cyclic"
      " cascaded update\n"
      "in the child table,",
      thr,
      foreign,
      pcur->get_rec(),
      entry
    );

    goto nonstandard_exit_func;
  }

  if (row_ins_cascade_n_ancestors(cascade) >= 15) {
    err = DB_ROW_IS_REFERENCED;

    row_ins_foreign_report_err("Trying a too deep cascaded delete or update\n", thr, foreign, pcur->get_rec(), entry);

    goto nonstandard_exit_func;
  }

  index = pcur->get_index();

  ut_a(index == foreign->foreign_index);

  rec = pcur->get_rec();

  if (dict_index_is_clust(index)) {
    /* pcur is already positioned in the clustered index of
    the child table */

    clust_index = index;
    clust_rec = rec;
    clust_block = pcur->get_block();
  } else {
    /* We have to look for the record in the clustered index
    in the child table */

    clust_index = dict_table_get_first_index(table);

    tmp_heap = mem_heap_create(256);

    ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, tmp_heap);

    cascade->pcur->open_with_no_init(clust_index, ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, false, mtr, Source_location{});

    clust_rec = cascade->pcur->get_rec();
    clust_block = cascade->pcur->get_block();

    if (!page_rec_is_user_rec(clust_rec) || cascade->pcur->get_low_match() < dict_index_get_n_unique(clust_index)) {

      log_err("error in cascade of a foreign key op");
      dict_index_name_print(ib_stream, trx, index);

      log_err("record ");
      log_err(rec_to_string(rec));
      log_err("clustered record ");
      log_err(rec_to_string(clust_rec));
      log_err("Submit a detailed bug report, check the Embedded InnoDB website for details");

      err = DB_SUCCESS;

      goto nonstandard_exit_func;
    }
  }

  /* Set an X-lock on the row to delete or update in the child table */

  err = srv_lock_sys->lock_table(0, table, LOCK_IX, thr);

  if (err == DB_SUCCESS) {
    /* Here it suffices to use a LOCK_REC_NOT_GAP type lock;
    we already have a normal shared lock on the appropriate
    gap if the search criterion was not unique */

    err = srv_lock_sys->clust_rec_read_check_and_lock_alt(0, clust_block, clust_rec, clust_index, LOCK_X, LOCK_REC_NOT_GAP, thr);
  }

  if (err != DB_SUCCESS) {

    goto nonstandard_exit_func;
  }

  if (rec_get_deleted_flag(clust_rec)) {
    /* This can happen if there is a circular reference of
    rows such that cascading delete comes to delete a row
    already in the process of being delete marked */
    err = DB_SUCCESS;

    goto nonstandard_exit_func;
  }

  if ((node->is_delete && (foreign->type & DICT_FOREIGN_ON_DELETE_SET_NULL)) || (!node->is_delete && (foreign->type & DICT_FOREIGN_ON_UPDATE_SET_NULL))) {

    /* Build the appropriate update vector which sets
    foreign->n_fields first fields in rec to SQL NULL */

    update = cascade->update;

    update->info_bits = 0;
    update->n_fields = foreign->n_fields;

    for (i = 0; i < foreign->n_fields; i++) {
      auto ufield = &update->fields[i];

      ufield->field_no = dict_table_get_nth_col_pos(table, dict_index_get_nth_col_no(index, i));
      ufield->orig_len = 0;
      ufield->exp = nullptr;
      dfield_set_null(&ufield->new_val);
    }
  }

  if (!node->is_delete && (foreign->type & DICT_FOREIGN_ON_UPDATE_CASCADE)) {

    /* Build the appropriate update vector which sets changing
    foreign->n_fields first fields in rec to new values */

    upd_vec_heap = mem_heap_create(256);

    n_to_update = row_ins_cascade_calc_update_vec(node, foreign, upd_vec_heap);

    if (n_to_update == ULINT_UNDEFINED) {
      err = DB_ROW_IS_REFERENCED;

      row_ins_foreign_report_err(
        "Trying a cascaded update where the"
        " updated value in the child\n"
        "table would not fit in the length"
        " of the column, or the value would\n"
        "be NULL and the column is"
        " declared as not NULL in the child table,",
        thr,
        foreign,
        pcur->get_rec(),
        entry
      );

      goto nonstandard_exit_func;
    }

    if (cascade->update->n_fields == 0) {

      /* The update does not change any columns referred
      to in this foreign key constraint: no need to do
      anything */

      err = DB_SUCCESS;

      goto nonstandard_exit_func;
    }
  }

  /* Store pcur position and initialize or store the cascade node
  pcur stored position */

  pcur->store_position(mtr);

  if (index == clust_index) {
    cascade->pcur->copy_stored_position(pcur);
  } else {
    cascade->pcur->store_position(mtr);
  }

  mtr->commit();

  ut_a(cascade->pcur->get_rel_pos() == Btree_cursor_pos::ON);

  cascade->state = UPD_NODE_UPDATE_CLUSTERED;

  err = row_update_cascade_client(thr, cascade, foreign->foreign_table);

  if (foreign->foreign_table->n_foreign_key_checks_running == 0) {
    ib_logger(
      ib_stream,
      "error: table %s has the counter 0"
      " though there is\n"
      "a FOREIGN KEY check running on it.\n",
      foreign->foreign_table->name
    );
  }

  /* Release the data dictionary latch for a while, so that we do not
  starve other threads from doing CREATE TABLE etc. if we have a huge
  cascaded operation running. The counter n_foreign_key_checks_running
  will prevent other users from dropping or ALTERing the table when we
  release the latch. */

  dict_unfreeze_data_dictionary(thr_get_trx(thr));
  dict_freeze_data_dictionary(thr_get_trx(thr));

  mtr->start();

  /* Restore pcur position */

  (void) pcur->restore_position(BTR_SEARCH_LEAF, mtr, Source_location{});

  if (tmp_heap != nullptr) {
    mem_heap_free(tmp_heap);
  }

  if (upd_vec_heap != nullptr) {
    mem_heap_free(upd_vec_heap);
  }

  return err;

nonstandard_exit_func:
  if (tmp_heap) {
    mem_heap_free(tmp_heap);
  }

  if (upd_vec_heap) {
    mem_heap_free(upd_vec_heap);
  }

  pcur->store_position(mtr);

  mtr->commit();

  mtr->start();

  (void) pcur->restore_position(BTR_SEARCH_LEAF, mtr, Source_location{});

  return err;
}

/** Sets a shared lock on a record. Used in locking possible duplicate key
records and also in checking foreign key constraints.
@return	DB_SUCCESS or error code */
static db_err row_ins_set_shared_rec_lock(
  ulint type,               /*!< in: LOCK_ORDINARY, LOCK_GAP, or
                              LOCK_REC_NOT_GAP type lock */
  const Buf_block *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: record */
  dict_index_t *index,      /*!< in: index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(index, rec) */
  que_thr_t *thr
) /*!< in: query thread */
{
  db_err err;

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (dict_index_is_clust(index)) {
    err = srv_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_S, type, thr);
  } else {
    err = srv_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_S, type, thr);
  }

  return err;
}

/** Sets a exclusive lock on a record. Used in locking possible duplicate key
records
@return	DB_SUCCESS or error code */
static db_err row_ins_set_exclusive_rec_lock(
  ulint type,               /*!< in: LOCK_ORDINARY, LOCK_GAP, or
                              LOCK_REC_NOT_GAP type lock */
  const Buf_block *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: record */
  dict_index_t *index,      /*!< in: index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(index, rec) */
  que_thr_t *thr
) /*!< in: query thread */
{
  db_err err;

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (dict_index_is_clust(index)) {
    err = srv_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_X, type, thr);
  } else {
    err = srv_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_X, type, thr);
  }

  return err;
}

db_err row_ins_check_foreign_constraint(
  bool check_ref, dict_foreign_t *foreign, dict_table_t *table, dtuple_t *entry, que_thr_t *thr
) {
  upd_node_t *upd_node;
  dict_table_t *check_table;
  dict_index_t *check_index;
  ulint n_fields_cmp;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  bool moved;
  int cmp;
  db_err err;
  ulint i;
  mtr_t mtr;
  trx_t *trx = thr_get_trx(thr);
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

run_again:
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  err = DB_SUCCESS;

  if (trx->m_check_foreigns == false) {
    /* The user has suppressed foreign key checks currently for
    this session */
    goto exit_func;
  }

  /* If any of the foreign key fields in entry is SQL NULL, we
  suppress the foreign key check: this is compatible with Oracle,
  for example */

  for (i = 0; i < foreign->n_fields; i++) {
    if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

      goto exit_func;
    }
  }

  if (que_node_get_type(thr->run_node) == QUE_NODE_UPDATE) {
    upd_node = static_cast<upd_node_t *>(thr->run_node);

    if (!upd_node->is_delete && upd_node->foreign == foreign) {
      /* If a cascaded update is done as defined by a
      foreign key constraint, do not check that
      constraint for the child row. In ON UPDATE CASCADE
      the update of the parent row is only half done when
      we come here: if we would check the constraint here
      for the child row it would fail.

      A QUESTION remains: if in the child table there are
      several constraints which refer to the same parent
      table, we should merge all updates to the child as
      one update? And the updates can be contradictory!
      Currently we just perform the update associated
      with each foreign key constraint, one after
      another, and the user has problems predicting in
      which order they are performed. */

      goto exit_func;
    }
  }

  if (check_ref) {
    check_table = foreign->referenced_table;
    check_index = foreign->referenced_index;
  } else {
    check_table = foreign->foreign_table;
    check_index = foreign->foreign_index;
  }

  if (check_table == nullptr || check_table->ibd_file_missing) {
    if (check_ref) {
      row_ins_set_detailed(trx, foreign);

      mutex_enter(&dict_foreign_err_mutex);
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, " Transaction:\n");
      log_info(trx_to_string(trx, 600));
      ib_logger(ib_stream, "Foreign key constraint fails for table ");
      ut_print_name(foreign->foreign_table_name);
      ib_logger(ib_stream, ":\n");
      dict_print_info_on_foreign_key_in_create_format(ib_stream, trx, foreign, true);
      ib_logger(ib_stream, "\nTrying to add to index ");
      ut_print_name(foreign->foreign_index->name);
      ib_logger(ib_stream, " tuple:\n");
      dtuple_print(ib_stream, entry);
      ib_logger(ib_stream, "\nBut the parent table ");
      ut_print_name(foreign->referenced_table_name);
      ib_logger(
        ib_stream,
        "\nor its .ibd file does"
        " not currently exist!\n"
      );
      mutex_exit(&dict_foreign_err_mutex);

      err = DB_NO_REFERENCED_ROW;
    }

    goto exit_func;
  }

  ut_a(check_table);
  ut_a(check_index);

  if (check_table != table) {
    /* We already have a LOCK_IX on table, but not necessarily
    on check_table */

    err = srv_lock_sys->lock_table(0, check_table, LOCK_IS, thr);

    if (err != DB_SUCCESS) {

      goto do_possible_lock_wait;
    }
  }

  mtr.start();

  /* Store old value on n_fields_cmp */

  n_fields_cmp = dtuple_get_n_fields_cmp(entry);

  dtuple_set_n_fields_cmp(entry, foreign->n_fields);

  pcur.open(check_index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  /* Scan index records and check if there is a matching record */

  for (;;) {
    const rec_t *rec = pcur.get_rec();
    const Buf_block *block = pcur.get_block();

    if (page_rec_is_infimum(rec)) {

      goto next_rec;
    }

    {
      Phy_rec record{check_index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    if (page_rec_is_supremum(rec)) {

      err = row_ins_set_shared_rec_lock(LOCK_ORDINARY, block, rec, check_index, offsets, thr);

      if (err != DB_SUCCESS) {

        break;
      }

      goto next_rec;
    }

    cmp = cmp_dtuple_rec(check_index->cmp_ctx, entry, rec, offsets);

    if (cmp == 0) {
      if (rec_get_deleted_flag(rec)) {
        err = row_ins_set_shared_rec_lock(LOCK_ORDINARY, block, rec, check_index, offsets, thr);
        if (err != DB_SUCCESS) {

          break;
        }
      } else {
        /* Found a matching record. Lock only
        a record because we can allow inserts
        into gaps */

        err = row_ins_set_shared_rec_lock(LOCK_REC_NOT_GAP, block, rec, check_index, offsets, thr);

        if (err != DB_SUCCESS) {

          break;
        }

        if (check_ref) {
          err = DB_SUCCESS;

          break;
        } else if (foreign->type != 0) {
          /* There is an ON UPDATE or ON DELETE
          condition: check them in a separate
          function */

          err = row_ins_foreign_check_on_constraint(thr, foreign, &pcur, entry, &mtr);
          if (err != DB_SUCCESS) {
            /* Since reporting a plain
            "duplicate key" error
            message to the user in
            cases where a long CASCADE
            operation would lead to a
            duplicate key in some
            other table is very
            confusing, map duplicate
            key errors resulting from
            FK constraints to a
            separate error code. */

            if (err == DB_DUPLICATE_KEY) {
              err = DB_FOREIGN_DUPLICATE_KEY;
            }

            break;
          }

          /* row_ins_foreign_check_on_constraint
          may have repositioned pcur on a
          different block */
          block = pcur.get_block();
        } else {
          row_ins_foreign_report_err("Trying to delete or update", thr, foreign, rec, entry);

          err = DB_ROW_IS_REFERENCED;
          break;
        }
      }
    }

    if (cmp < 0) {
      err = row_ins_set_shared_rec_lock(LOCK_GAP, block, rec, check_index, offsets, thr);
      if (err != DB_SUCCESS) {

        break;
      }

      if (check_ref) {
        err = DB_NO_REFERENCED_ROW;
        row_ins_foreign_report_add_err(trx, foreign, rec, entry);
      } else {
        err = DB_SUCCESS;
      }

      break;
    }

    ut_a(cmp == 0);
  next_rec:
    moved = pcur.move_to_next(&mtr);

    if (!moved) {
      if (check_ref) {
        rec = pcur.get_rec();
        row_ins_foreign_report_add_err(trx, foreign, rec, entry);
        err = DB_NO_REFERENCED_ROW;
      } else {
        err = DB_SUCCESS;
      }

      break;
    }
  }

  pcur.close();

  mtr.commit();

  /* Restore old value */
  dtuple_set_n_fields_cmp(entry, n_fields_cmp);

do_possible_lock_wait:
  if (err == DB_LOCK_WAIT) {
    trx->error_state = static_cast<db_err>(err);

    que_thr_stop_client(thr);

    InnoDB::suspend_user_thread(thr);

    if (trx->error_state == DB_SUCCESS) {

      goto run_again;
    }

    err = trx->error_state;
  }

exit_func:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

/** Checks if foreign key constraints fail for an index entry. If index
is not mentioned in any constraint, this function does nothing,
Otherwise does searches to the indexes of referenced tables and
sets shared locks which lock either the success or the failure of
a constraint.
@return	DB_SUCCESS or error code */
static db_err row_ins_check_foreign_constraints(
  dict_table_t *table, /*!< in: table */
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in: index entry for index */
  que_thr_t *thr
) /*!< in: query thread */
{
  dict_foreign_t *foreign;
  db_err err;
  trx_t *trx;
  bool got_s_lock = false;

  trx = thr_get_trx(thr);

  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign) {
    if (foreign->foreign_index == index) {

      if (foreign->referenced_table == nullptr) {
        dict_table_get(foreign->referenced_table_name, false);
      }

      if (0 == trx->m_dict_operation_lock_mode) {
        got_s_lock = true;

        dict_freeze_data_dictionary(trx);
      }

      if (foreign->referenced_table) {
        mutex_enter(&(dict_sys->mutex));

        (foreign->referenced_table->n_foreign_key_checks_running)++;

        mutex_exit(&(dict_sys->mutex));
      }

      /* NOTE that if the thread ends up waiting for a lock
      we will release dict_operation_lock temporarily!
      But the counter on the table protects the referenced
      table from being dropped while the check is running. */

      err = row_ins_check_foreign_constraint(true, foreign, table, entry, thr);

      if (foreign->referenced_table) {
        mutex_enter(&(dict_sys->mutex));

        ut_a(foreign->referenced_table->n_foreign_key_checks_running > 0);
        (foreign->referenced_table->n_foreign_key_checks_running)--;

        mutex_exit(&(dict_sys->mutex));
      }

      if (got_s_lock) {
        dict_unfreeze_data_dictionary(trx);
      }

      if (err != DB_SUCCESS) {
        return err;
      }
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  return DB_SUCCESS;
}

/** Checks if a unique key violation to rec would occur at the index entry
insert.
@return	true if error */
static bool row_ins_dupl_error_with_rec(
  const rec_t *rec,      /*!< in: user record; NOTE that we assume
                           that the caller already has a record lock on
                           the record! */
  const dtuple_t *entry, /*!< in: entry to insert */
  dict_index_t *index,   /*!< in: index */
  const ulint *offsets
) /*!< in: Phy_rec::get_col_offsets(index, rec) */
{
  ulint matched_fields;
  ulint matched_bytes;
  ulint n_unique;
  ulint i;

  ut_ad(rec_offs_validate(rec, index, offsets));

  n_unique = dict_index_get_n_unique(index);

  matched_fields = 0;
  matched_bytes = 0;

  cmp_dtuple_rec_with_match(index->cmp_ctx, entry, rec, offsets, &matched_fields, &matched_bytes);

  if (matched_fields < n_unique) {

    return false;
  }

  /* In a unique secondary index we allow equal key values if they
  contain SQL NULLs */

  if (!dict_index_is_clust(index)) {

    for (i = 0; i < n_unique; i++) {
      if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

        return false;
      }
    }
  }

  return !rec_get_deleted_flag(rec);
}

/** Scans a unique non-clustered index at a given index entry to determine
whether a uniqueness violation has occurred for the key value of the entry.
Set shared locks on possible duplicate records.
@return	DB_SUCCESS, DB_DUPLICATE_KEY, or DB_LOCK_WAIT */
static db_err row_ins_scan_sec_index_for_duplicate(
  dict_index_t *index, /*!< in: non-clustered unique index */
  dtuple_t *entry,     /*!< in: index entry */
  que_thr_t *thr
) /*!< in: query thread */
{
  ulint n_unique;
  ulint i;
  int cmp;
  ulint n_fields_cmp;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  db_err err = DB_SUCCESS;
  unsigned allow_duplicates;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  n_unique = dict_index_get_n_unique(index);

  /* If the secondary index is unique, but one of the fields in the
  n_unique first fields is NULL, a unique key violation cannot occur,
  since we define NULL != NULL in this case */

  for (i = 0; i < n_unique; i++) {
    if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

      return DB_SUCCESS;
    }
  }

  mtr.start();

  /* Store old value on n_fields_cmp */

  n_fields_cmp = dtuple_get_n_fields_cmp(entry);

  dtuple_set_n_fields_cmp(entry, dict_index_get_n_unique(index));

  pcur.open(index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  allow_duplicates = thr_get_trx(thr)->m_duplicates & TRX_DUP_IGNORE;

  /* Scan index records and check if there is a duplicate */

  do {
    const rec_t *rec = pcur.get_rec();
    const Buf_block *block = pcur.get_block();

    if (page_rec_is_infimum(rec)) {

      continue;
    }

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    if (allow_duplicates) {

      /* If the SQL-query will update or replace
      duplicate key we will take X-lock for
      duplicates ( REPLACE, LOAD DATAFILE REPLACE,
      INSERT ON DUPLICATE KEY UPDATE). */

      err = row_ins_set_exclusive_rec_lock(LOCK_ORDINARY, block, rec, index, offsets, thr);
    } else {

      err = row_ins_set_shared_rec_lock(LOCK_ORDINARY, block, rec, index, offsets, thr);
    }

    if (err != DB_SUCCESS) {

      break;
    }

    if (page_rec_is_supremum(rec)) {

      continue;
    }

    cmp = cmp_dtuple_rec(index->cmp_ctx, entry, rec, offsets);

    if (cmp == 0) {
      if (row_ins_dupl_error_with_rec(rec, entry, index, offsets)) {
        err = DB_DUPLICATE_KEY;

        thr_get_trx(thr)->error_info = index;

        break;
      }
    }

    if (cmp < 0) {
      break;
    }

    ut_a(cmp == 0);
  } while (pcur.move_to_next(&mtr));

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  mtr.commit();

  /* Restore old value */
  dtuple_set_n_fields_cmp(entry, n_fields_cmp);

  return err;
}

/** Checks if a unique key violation error would occur at an index entry
insert. Sets shared locks on possible duplicate records. Works only
for a clustered index!
@return DB_SUCCESS if no error, DB_DUPLICATE_KEY if error,
DB_LOCK_WAIT if we have to wait for a lock on a possible duplicate
record */
static db_err row_ins_duplicate_error_in_clust(
  Btree_cursor *btr_cur, /*!< in: B-tree btr_cur */
  dtuple_t *entry,   /*!< in: entry to insert */
  que_thr_t *thr,    /*!< in: query thread */
  mtr_t *mtr
) /*!< in: mtr */
{
  db_err err;
  rec_t *rec;
  ulint n_unique;
  trx_t *trx = thr_get_trx(thr);
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  UT_NOT_USED(mtr);

  ut_a(dict_index_is_clust(btr_cur->m_index));
  ut_ad(dict_index_is_unique(btr_cur->m_index));

  /* NOTE: For unique non-clustered indexes there may be any number
  of delete marked records with the same value for the non-clustered
  index key (remember multiversioning), and which differ only in
  the row refererence part of the index record, containing the
  clustered index key fields. For such a secondary index record,
  to avoid race condition, we must FIRST do the insertion and after
  that check that the uniqueness condition is not breached! */

  /* NOTE: A problem is that in the B-tree node pointers on an
  upper level may match more to the entry than the actual existing
  user records on the leaf level. So, even if low_match would suggest
  that a duplicate key violation may occur, this may not be the case. */

  n_unique = dict_index_get_n_unique(btr_cur->m_index);

  if (btr_cur->m_low_match >= n_unique) {

    rec = btr_cur->get_rec();

    if (!page_rec_is_infimum(rec)) {
      {
        Phy_rec record{btr_cur->m_index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
      }

      /* We set a lock on the possible duplicate: this
      is needed in logical logging, we need to make
      sure that in roll-forward we get the same duplicate
      errors as in original execution */

      if (trx->m_duplicates & TRX_DUP_IGNORE) {

        /* If the SQL-query will update or replace
        duplicate key we will take X-lock for
        duplicates ( REPLACE, LOAD DATAFILE REPLACE,
        INSERT ON DUPLICATE KEY UPDATE). */

        err = row_ins_set_exclusive_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      } else {

        err = row_ins_set_shared_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      }

      if (err != DB_SUCCESS) {
        goto func_exit;
      }

      if (row_ins_dupl_error_with_rec(rec, entry, btr_cur->m_index, offsets)) {
        trx->error_info = btr_cur->m_index;
        err = DB_DUPLICATE_KEY;
        goto func_exit;
      }
    }
  }

  if (btr_cur->m_up_match >= n_unique) {

    rec = page_rec_get_next(btr_cur->get_rec());

    if (!page_rec_is_supremum(rec)) {
      {
        Phy_rec record{btr_cur->m_index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
      }

      if (trx->m_duplicates & TRX_DUP_IGNORE) {

        /* If the SQL-query will update or replace
        duplicate key we will take X-lock for
        duplicates ( REPLACE, LOAD DATAFILE REPLACE,
        INSERT ON DUPLICATE KEY UPDATE). */

        err = row_ins_set_exclusive_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      } else {

        err = row_ins_set_shared_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      }

      if (err != DB_SUCCESS) {
        goto func_exit;
      }

      if (row_ins_dupl_error_with_rec(rec, entry, btr_cur->m_index, offsets)) {
        trx->error_info = btr_cur->m_index;
        err = DB_DUPLICATE_KEY;
        goto func_exit;
      }
    }

    ut_a(!dict_index_is_clust(btr_cur->m_index));
    /* This should never happen */
  }

  err = DB_SUCCESS;
func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

/** Checks if an index entry has long enough common prefix with an existing
record so that the intended insert of the entry must be changed to a modify of
the existing record. In the case of a clustered index, the prefix must be
n_unique fields long, and in the case of a secondary index, all fields must be
equal.
@return 0 if no update, ROW_INS_PREV if previous should be updated;
currently we do the search so that only the low_match record can match
enough to the search tuple, not the next record */
inline ulint row_ins_must_modify(Btree_cursor *btr_cur) /*!< in: B-tree btr_cur */
{
  ulint enough_match;
  rec_t *rec;

  /* NOTE: (compare to the note in row_ins_duplicate_error) Because node
  pointers on upper levels of the B-tree may match more to entry than
  to actual user records on the leaf level, we have to check if the
  candidate record is actually a user record. In a clustered index
  node pointers contain index->n_unique first fields, and in the case
  of a secondary index, all fields of the index. */

  enough_match = dict_index_get_n_unique_in_tree(btr_cur->m_index);

  if (btr_cur->m_low_match >= enough_match) {

    rec = btr_cur->get_rec();

    if (!page_rec_is_infimum(rec)) {

      return ROW_INS_PREV;
    }
  }

  return 0;
}

/** Tries to insert an index entry to an index. If the index is clustered
and a record with the same unique key is found, the other record is
necessarily marked deleted by a committed transaction, or a unique key
violation error occurs. The delete marked record is then updated to an
existing record, and we must write an undo log record on the delete
marked record. If the index is secondary, and a record with exactly the
same fields is found, the other record is necessarily marked deleted.
It is then unmarked. Otherwise, the entry is just inserted to the index.
@return DB_SUCCESS, DB_LOCK_WAIT, DB_FAIL if pessimistic retry needed,
or error code */
static db_err row_ins_index_entry_low(
  ulint mode,          /*!< in: BTR_MODIFY_LEAF or BTR_MODIFY_TREE,
                         depending on whether we wish optimistic or
                         pessimistic descent down the index tree */
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in: index entry to insert */
  ulint n_ext,         /*!< in: number of externally stored columns */
  que_thr_t *thr
) /*!< in: query thread */
{
  ulint modify = 0; /* remove warning */
  rec_t *insert_rec;
  rec_t *rec;
  db_err err;
  ulint n_unique;
  big_rec_t *big_rec = nullptr;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  Btree_cursor btr_cur(srv_fsp, srv_btree_sys, srv_lock_sys);

  log_sys->free_check();

  mtr.start();

  btr_cur.m_thr = thr;

  btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, mode | BTR_INSERT, &mtr, Source_location{});

#ifdef UNIV_DEBUG
  {
    page_t *page = btr_cur.get_page();
    rec_t *first_rec = page_rec_get_next(page_get_infimum_rec(page));

    ut_ad(page_rec_is_supremum(first_rec) || rec_get_n_fields(first_rec, index) == dtuple_get_n_fields(entry));
  }
#endif /* UNIV_DEBUG */

  n_unique = dict_index_get_n_unique(index);

  if (dict_index_is_unique(index) && (btr_cur.m_up_match >= n_unique ||btr_cur.m_low_match >= n_unique)) {

    if (dict_index_is_clust(index)) {
      /* Note that the following may return also
      DB_LOCK_WAIT */

      err = row_ins_duplicate_error_in_clust(&btr_cur, entry, thr, &mtr);

      if (err != DB_SUCCESS) {

        goto function_exit;
      }
    } else {
      mtr.commit();
      err = row_ins_scan_sec_index_for_duplicate(index, entry, thr);
      mtr.start();

      if (err != DB_SUCCESS) {

        goto function_exit;
      }

      /* We did not find a duplicate and we have now
      locked with s-locks the necessary records to
      prevent any insertion of a duplicate by another
      transaction. Let us now reposition the cursor and
      continue the insertion. */

      btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, mode | BTR_INSERT, &mtr, Source_location{});
    }
  }

  modify = row_ins_must_modify(&btr_cur);

  if (modify != 0) {
    /* There is already an index entry with a long enough common
    prefix, we must convert the insert into a modify of an
    existing record */

    if (modify == ROW_INS_NEXT) {
      rec = page_rec_get_next(btr_cur.get_rec());

      btr_cur.position(index, rec, btr_cur.get_block());
    }

    if (dict_index_is_clust(index)) {
      err = row_ins_clust_index_entry_by_modify(mode, &btr_cur, &heap, &big_rec, entry, thr, &mtr);
    } else {
      ut_ad(!n_ext);
      err = row_ins_sec_index_entry_by_modify(mode, &btr_cur, entry, thr, &mtr);
    }
  } else {
    if (mode == BTR_MODIFY_LEAF) {
      err = btr_cur.optimistic_insert(0, entry, &insert_rec, &big_rec, n_ext, thr, &mtr);
    } else {
      ut_a(mode == BTR_MODIFY_TREE);
      if (srv_buf_pool->m_LRU->buf_pool_running_out()) {

        err = DB_LOCK_TABLE_FULL;

        goto function_exit;
      }
      err = btr_cur.pessimistic_insert(0, entry, &insert_rec, &big_rec, n_ext, thr, &mtr);
    }
  }

function_exit:
  mtr.commit();

  if (likely_null(big_rec)) {
    mtr.start();

    btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, BTR_MODIFY_TREE, &mtr, Source_location{});

    ulint *offsets;
    auto rec = btr_cur.get_rec();

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Source_location{});
    }

    Blob blob(srv_fsp, srv_btree_sys);

    err = blob.store_big_rec_extern_fields(index, btr_cur.get_block(), rec, offsets, big_rec, &mtr);

    if (modify) {
      dtuple_big_rec_free(big_rec);
    } else {
      dtuple_convert_back_big_rec(index, entry, big_rec);
    }

    mtr.commit();
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

db_err row_ins_index_entry(dict_index_t *index, dtuple_t *entry, ulint n_ext, bool foreign, que_thr_t *thr) {
  db_err err;

  if (foreign && UT_LIST_GET_FIRST(index->table->foreign_list)) {

    err = row_ins_check_foreign_constraints(index->table, index, entry, thr);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  /* Try first optimistic descent to the B-tree */

  err = row_ins_index_entry_low(BTR_MODIFY_LEAF, index, entry, n_ext, thr);

  if (err != DB_FAIL) {

    return err;
  }

  /* Try then pessimistic descent to the B-tree */

  err = row_ins_index_entry_low(BTR_MODIFY_TREE, index, entry, n_ext, thr);

  return err;
}

/** Sets the values of the dtuple fields in entry from the values of appropriate
columns in row. */
static void row_ins_index_entry_set_vals(
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in: index entry to make */
  const dtuple_t *row
) /*!< in: row */
{
  ulint n_fields;
  ulint i;

  ut_ad(entry && row);

  n_fields = dtuple_get_n_fields(entry);

  for (i = 0; i < n_fields; i++) {
    dict_field_t *ind_field;
    dfield_t *field;
    const dfield_t *row_field;
    ulint len;

    field = dtuple_get_nth_field(entry, i);
    ind_field = dict_index_get_nth_field(index, i);
    row_field = dtuple_get_nth_field(row, ind_field->col->ind);
    len = dfield_get_len(row_field);

    /* Check column prefix indexes */
    if (ind_field->prefix_len > 0 && dfield_get_len(row_field) != UNIV_SQL_NULL) {

      const dict_col_t *col;

      col = dict_field_get_col(ind_field);

      len = dtype_get_at_most_n_mbchars(
        col->prtype, col->mbminlen, col->mbmaxlen, ind_field->prefix_len, len, (char *)dfield_get_data(row_field)
      );

      ut_ad(!dfield_is_ext(row_field));
    }

    dfield_set_data(field, dfield_get_data(row_field), len);

    if (dfield_is_ext(row_field)) {
      ut_ad(dict_index_is_clust(index));
      dfield_set_ext(field);
    }
  }
}

/** Inserts a single index entry to the table.
@return DB_SUCCESS if operation successfully completed, else error
code or DB_LOCK_WAIT */
static ulint row_ins_index_entry_step(
  ins_node_t *node, /*!< in: row insert node */
  que_thr_t *thr
) /*!< in: query thread */
{
  ulint err;

  ut_ad(dtuple_check_typed(node->row));

  row_ins_index_entry_set_vals(node->index, node->entry, node->row);

  ut_ad(dtuple_check_typed(node->entry));

  err = row_ins_index_entry(node->index, node->entry, 0, true, thr);

  return err;
}

/** Allocates a row id for row and inits the node->index field. */
inline void row_ins_alloc_row_id_step(ins_node_t *node) /*!< in: row insert node */
{
  uint64_t row_id;

  ut_ad(node->state == INS_NODE_ALLOC_ROW_ID);

  if (dict_index_is_unique(dict_table_get_first_index(node->table))) {

    /* No row id is stored if the clustered index is unique */

    return;
  }

  /* Fill in row id value to row */

  row_id = dict_sys_get_new_row_id();

  dict_sys_write_row_id(node->row_id_buf, row_id);
}

/** Gets a row to insert from the values list. */
inline void row_ins_get_row_from_values(ins_node_t *node) /*!< in: row insert node */
{
  ulint i;
  dtuple_t *row;
  que_node_t *list_node;

  /* The field values are copied in the buffers of the select node and
  it is safe to use them until we fetch from select again: therefore
  we can just copy the pointers */

  row = node->row;

  i = 0;
  list_node = node->values_list;

  while (list_node) {
    dfield_t *dfield;

    eval_exp(list_node);

    dfield = dtuple_get_nth_field(row, i);
    dfield_copy_data(dfield, que_node_get_val(list_node));

    i++;
    list_node = que_node_get_next(list_node);
  }
}

/** Gets a row to insert from the select list. */
inline void row_ins_get_row_from_select(ins_node_t *node) /*!< in: row insert node */
{
  ulint i;
  dtuple_t *row;
  que_node_t *list_node;

  /* The field values are copied in the buffers of the select node and
  it is safe to use them until we fetch from select again: therefore
  we can just copy the pointers */

  row = node->row;

  i = 0;
  list_node = node->select->select_list;

  while (list_node) {
    dfield_t *dfield;

    dfield = dtuple_get_nth_field(row, i);
    dfield_copy_data(dfield, que_node_get_val(list_node));

    i++;
    list_node = que_node_get_next(list_node);
  }
}

/** Inserts a row to a table.
@return DB_SUCCESS if operation successfully completed, else error
code or DB_LOCK_WAIT */
static ulint row_ins(
  ins_node_t *node, /*!< in: row insert node */
  que_thr_t *thr
) /*!< in: query thread */
{
  ulint err;

  ut_ad(node && thr);

  if (node->state == INS_NODE_ALLOC_ROW_ID) {

    row_ins_alloc_row_id_step(node);

    node->index = dict_table_get_first_index(node->table);
    node->entry = UT_LIST_GET_FIRST(node->entry_list);

    if (node->ins_type == INS_SEARCHED) {

      row_ins_get_row_from_select(node);

    } else if (node->ins_type == INS_VALUES) {

      row_ins_get_row_from_values(node);
    }

    node->state = INS_NODE_INSERT_ENTRIES;
  }

  ut_ad(node->state == INS_NODE_INSERT_ENTRIES);

  while (node->index != nullptr) {
    err = row_ins_index_entry_step(node, thr);

    if (err != DB_SUCCESS) {

      return err;
    }

    node->index = dict_table_get_next_index(node->index);
    node->entry = UT_LIST_GET_NEXT(tuple_list, node->entry);
  }

  ut_ad(node->entry == nullptr);

  node->state = INS_NODE_ALLOC_ROW_ID;

  return DB_SUCCESS;
}

que_thr_t *row_ins_step(que_thr_t *thr) {
  que_node_t *parent;
  sel_node_t *sel_node;
  trx_t *trx;
  ulint err;

  ut_ad(thr);

  trx = thr_get_trx(thr);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = static_cast<ins_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_INSERT);

  parent = que_node_get_parent(node);
  sel_node = node->select;

  if (thr->prev_node == parent) {
    node->state = INS_NODE_SET_IX_LOCK;
  }

  /* If this is the first time this node is executed (or when
  execution resumes after wait for the table IX lock), set an
  IX lock on the table and reset the possible select node. If the
  client code calls an insert within the same SQL statement AFTER
  it has used this table handle to do a search. In that case, we
  have already set the IX lock on the table during the search
  operation, and there is no need to set it again here. But we
  must write trx->id to node->trx_id. */

  srv_trx_sys->write_trx_id(node->trx_id_buf, trx->m_id);

  if (node->state == INS_NODE_SET_IX_LOCK) {

    /* It may be that the current session has not yet started
    its transaction, or it has been committed: */

    if (trx->m_id == node->trx_id) {
      /* No need to do IX-locking */

      goto same_trx;
    }

    err = srv_lock_sys->lock_table(0, node->table, LOCK_IX, thr);

    if (err != DB_SUCCESS) {

      goto error_handling;
    }

    node->trx_id = trx->m_id;
  same_trx:
    node->state = INS_NODE_ALLOC_ROW_ID;

    if (node->ins_type == INS_SEARCHED) {
      /* Reset the cursor */
      sel_node->state = SEL_NODE_OPEN;

      /* Fetch a row to insert */
      thr->run_node = sel_node;

      return thr;
    }
  }

  if ((node->ins_type == INS_SEARCHED) && (sel_node->state != SEL_NODE_FETCH)) {

    ut_ad(sel_node->state == SEL_NODE_NO_MORE_ROWS);

    /* No more rows to insert */
    thr->run_node = parent;

    return thr;
  }

  /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */

  err = row_ins(node, thr);

error_handling:
  trx->error_state = static_cast<db_err>(err);

  if (err != DB_SUCCESS) {
    /* err == DB_LOCK_WAIT or SQL error detected */
    return nullptr;
  }

  /* DO THE TRIGGER ACTIONS HERE */

  if (node->ins_type == INS_SEARCHED) {
    /* Fetch a row to insert */

    thr->run_node = sel_node;
  } else {
    thr->run_node = que_node_get_parent(node);
  }

  return thr;
}
