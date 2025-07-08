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

#include "api0misc.h"
#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0lru.h"
#include "data0data.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "log0log.h"
#include "mach0data.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "row0ins.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0upd.h"
#include "trx0undo.h"
#include "usr0sess.h"

constexpr ulint ROW_INS_PREV = 1;
constexpr ulint ROW_INS_NEXT = 2;

Row_insert *srv_row_ins;

Row_insert::Row_insert(Dict *dict, Lock_sys *lock_sys, Row_update *row_upd) noexcept
  : m_dict(dict), m_lock_sys(lock_sys), m_row_upd(row_upd) {
}

Row_insert* Row_insert::create(Dict *dict, Lock_sys *lock_sys, Row_update *row_upd) noexcept {
  auto ptr = ut_new(sizeof(Row_insert));

  return new (ptr) Row_insert(dict, lock_sys, row_upd);
}

void Row_insert::destroy(Row_insert *&ptr) noexcept {
  call_destructor(ptr);
  ut_delete(ptr);
  ptr = nullptr;
}

ins_node_t *Row_insert::node_create(ib_ins_mode_t ins_type, Table *table, mem_heap_t *heap) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(ins_node_t));
  auto node = new (ptr) ins_node_t();

  node->m_common.type = QUE_NODE_INSERT;

  node->m_ins_type = ins_type;

  node->m_state = INS_NODE_SET_IX_LOCK;
  node->m_table = table;
  node->m_index = nullptr;
  node->m_entry = nullptr;

  node->m_select = nullptr;

  node->m_trx_id = 0;

  node->m_entry_sys_heap = mem_heap_create(128);

  node->m_magic_n = INS_NODE_MAGIC_N;

  return node;
}

void Row_insert::create_entry_list(ins_node_t *node) noexcept {
  ut_ad(node->m_entry_sys_heap);

  UT_LIST_INIT(node->m_entry_list);

  for (auto index : node->m_table->m_indexes) {
    auto entry = row_build_index_entry(node->m_row, nullptr, index, node->m_entry_sys_heap);

    node->m_entry_list.push_back(entry);
  }
}

/**
 * @brief Adds system field buffers to a row.
 *
 * @param[in,out] node Insert node to initialize.
 */
void Row_insert::alloc_sys_fields(ins_node_t *node) noexcept {
  auto row = node->m_row;
  auto table = node->m_table;
  auto heap = node->m_entry_sys_heap;

  ut_ad(dtuple_get_n_fields(row) == table->get_n_cols());

  /* 1. Allocate buffer for row id */

  auto col = table->get_sys_col(DATA_ROW_ID);
  auto dfield = dtuple_get_nth_field(row, col->get_no());
  auto ptr = mem_heap_zalloc(heap, DATA_ROW_ID_LEN);

  dfield_set_data(dfield, ptr, DATA_ROW_ID_LEN);

  node->m_row_id_buf = ptr;

  /* 2. Allocate buffer for trx id */

  col = table->get_sys_col(DATA_TRX_ID);

  dfield = dtuple_get_nth_field(row, col->get_no());
  ptr = mem_heap_zalloc(heap, DATA_TRX_ID_LEN);

  dfield_set_data(dfield, ptr, DATA_TRX_ID_LEN);

  node->m_trx_id_buf = ptr;

  /* 3. Allocate buffer for roll ptr */

  col = table->get_sys_col(DATA_ROLL_PTR);

  dfield = dtuple_get_nth_field(row, col->get_no());
  ptr = mem_heap_zalloc(heap, DATA_ROLL_PTR_LEN);

  dfield_set_data(dfield, ptr, DATA_ROLL_PTR_LEN);
}

void Row_insert::set_new_row(ins_node_t *node, DTuple *row) noexcept {
  node->m_state = INS_NODE_SET_IX_LOCK;
  node->m_index = nullptr;
  node->m_entry = nullptr;

  node->m_row = row;

  mem_heap_empty(node->m_entry_sys_heap);

  /* Create templates for index entries */

  create_entry_list(node);

  /* Allocate from entry_sys_heap buffers for sys fields */

  alloc_sys_fields(node);

  /* As we allocated a new trx id buf, the trx id should be written
  there again: */

  node->m_trx_id = 0;
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
db_err Row_insert::sec_index_entry_by_modify(ulint mode, Btree_cursor *btr_cur, const DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept {
  db_err err;
  auto rec = btr_cur->get_rec();

  ut_ad(!btr_cur->m_index->is_clustered());
  ut_ad(rec_get_deleted_flag(rec));

  /* We know that in the alphabetical ordering, entry and rec are
  identified. But in their binary form there may be differences if
  there are char fields in them. Therefore we have to calculate the
  difference. */

  auto heap = mem_heap_create(1024);
  auto update = m_row_upd->build_sec_rec_difference_binary(btr_cur->m_index, entry, rec, thr_get_trx(thr), heap);

  if (likely(mode == BTR_MODIFY_LEAF)) {
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

    auto buf_pool = m_dict->m_store.m_fsp->m_buf_pool;

    if (unlikely(buf_pool->m_LRU->buf_pool_running_out())) {
      err = DB_LOCK_TABLE_FULL;
    } else {
      big_rec_t *dummy_big_rec;

      err = btr_cur->pessimistic_update(BTR_KEEP_SYS_FLAG, &heap, &dummy_big_rec, update, 0, thr, mtr);
      ut_ad(!dummy_big_rec);
    }
  }

  mem_heap_free(heap);

  return err;
}

db_err Row_insert::clust_index_entry_by_modify(ulint mode, Btree_cursor *btr_cur, mem_heap_t **heap, big_rec_t **big_rec, const DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept {
  ut_ad(btr_cur->m_index->is_clustered());

  *big_rec = nullptr;

  auto rec = btr_cur->get_rec();

  ut_ad(rec_get_deleted_flag(rec));

  if (*heap == nullptr) {
    *heap = mem_heap_create(1024);
  }

  /* Build an update vector containing all the fields to be modified;
  NOTE that this vector may NOT contain system columns trx_id or
  roll_ptr */

  auto update = m_row_upd->build_difference_binary(btr_cur->m_index, entry, rec, thr_get_trx(thr), *heap);

  if (mode == BTR_MODIFY_LEAF) {
    /* Try optimistic updating of the record, keeping changes
    within the page */

    const auto err = btr_cur->optimistic_update(0, update, 0, thr, mtr);

    switch (err) {
      case DB_OVERFLOW:
      case DB_UNDERFLOW:
        return DB_FAIL;
      default:
        return err;
    }

  } else {

    ut_a(mode == BTR_MODIFY_TREE);

    auto buf_pool = m_dict->m_store.m_fsp->m_buf_pool;

    if (unlikely(buf_pool->m_LRU->buf_pool_running_out())) {

      return DB_LOCK_TABLE_FULL;

    } else {

      return btr_cur->pessimistic_update(0, heap, big_rec, update, 0, thr, mtr);
    }
  }
}

bool Row_insert::cascade_ancestor_updates_table(que_node_t *node, Table *table) noexcept {
  auto parent = que_node_get_parent(node);

  while (que_node_get_type(parent) == QUE_NODE_UPDATE) {

    auto upd_node = static_cast<upd_node_t *>(parent);

    if (upd_node->m_table == table && !upd_node->m_is_delete) {

      return true;
    }

    parent = que_node_get_parent(parent);

    ut_a(parent);
  }

  return false;
}

ulint Row_insert::cascade_n_ancestors(que_node_t *node) noexcept {
  ulint n_ancestors{};

  auto parent = que_node_get_parent(node);

  while (que_node_get_type(parent) == QUE_NODE_UPDATE) {
    ++n_ancestors;

    parent = que_node_get_parent(parent);

    ut_a(parent);
  }

  return n_ancestors;
}

ulint Row_insert::cascade_calc_update_vec(upd_node_t *node, const Foreign *foreign, mem_heap_t *heap) noexcept {
  Table *table = foreign->m_foreign_table;
  Index *index = foreign->m_foreign_index;
  upd_node_t *cascade = node->m_cascade_node;

  /* Calculate the appropriate update vector which will set the fields
  in the child index record to the same value (possibly padded with
  spaces if the column is a fixed length CHAR or FIXBINARY column) as
  the referenced index record will get in the update. */

  auto parent_table = node->m_table;
  ut_a(parent_table == foreign->m_foreign_table);

  auto parent_index = foreign->m_foreign_index;
  auto parent_update = node->m_update;

  auto update = cascade->m_update;

  update->m_info_bits = 0;
  update->m_n_fields = foreign->m_n_fields;

  ulint n_fields_updated{};

  for (ulint i{}; i < foreign->m_n_fields; ++i) {

    auto parent_field_no = parent_table->get_nth_col_pos(parent_index->get_nth_table_col_no(i));

    for (ulint j{}; j < parent_update->m_n_fields; ++j) {
      auto parent_ufield = parent_update->m_fields + j;

      if (parent_ufield->m_field_no == parent_field_no) {

        const auto col = index->get_nth_col(i);

        /* A field in the parent index record is
        updated. Let us make the update vector
        field for the child table. */

        auto ufield = update->m_fields + n_fields_updated;

        ufield->m_field_no = table->get_nth_col_pos(col->get_no());
        ufield->m_exp = nullptr;

        ufield->m_new_val = parent_ufield->m_new_val;
        auto ufield_len = dfield_get_len(&ufield->m_new_val);

        /* Clear the "external storage" flag */
        dfield_set_len(&ufield->m_new_val, ufield_len);

        /* Do not allow a NOT NULL column to be
        updated as NULL */

        if (dfield_is_null(&ufield->m_new_val) && (col->prtype & DATA_NOT_NULL)) {

          return ULINT_UNDEFINED;
        }

        /* If the new value would not fit in the
        column, do not allow the update */

        if (!dfield_is_null(&ufield->m_new_val) &&
            dtype_get_at_most_n_mbchars(
                col->prtype, col->mbminlen, col->mbmaxlen, col->len, ufield_len,
                reinterpret_cast<const char *>(dfield_get_data(&ufield->m_new_val))
            ) < ufield_len) {

          return ULINT_UNDEFINED;
        }

        /* If the parent column type has a different
        length than the child column type, we may
        need to pad with spaces the new value of the
        child column */

        auto min_size = col->get_min_size();

        /* Because UNIV_SQL_NULL (the marker
        of SQL NULL values) exceeds all possible
        values of min_size, the test below will
        not hold for SQL NULL columns. */

        if (min_size > ufield_len) {

          auto padded_data = reinterpret_cast<char *>(mem_heap_alloc(heap, min_size));
          auto pad_start = padded_data + ufield_len;
          const auto pad_end = padded_data + min_size;

          memcpy(padded_data, dfield_get_data(&ufield->m_new_val), dfield_get_len(&ufield->m_new_val));

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

          dfield_set_data(&ufield->m_new_val, padded_data, min_size);
        }

        ++n_fields_updated;
      }
    }
  }

  update->m_n_fields = n_fields_updated;

  return n_fields_updated;
}

void Row_insert::set_detailed(Trx *trx, const Foreign *foreign) noexcept {
  ut_print_name(foreign->m_foreign_table_name);

  m_dict->print_info_on_foreign_key_in_create_format(trx, foreign, false);

  trx->set_detailed_error("foreign key error");
}

void Row_insert::foreign_report_err(const char *errstr, que_thr_t *thr, const Foreign *foreign, const rec_t *rec, const DTuple *entry) noexcept {
  auto trx = thr_get_trx(thr);

  set_detailed(trx, foreign);

  mutex_enter(&m_dict->m_foreign_err_mutex);

  log_info(" Transaction:");
  log_info(trx->to_string(600));
  log_info("Foreign key constraint fails for table ", foreign->m_foreign_table_name, ": ");
  m_dict->print_info_on_foreign_key_in_create_format(trx, foreign, true);
  log_info(errstr);
  log_info(" in parent table, in index ", foreign->m_referenced_index->m_name);

  if (entry != nullptr) {
    log_err(" tuple:");
    dtuple_print(ib_stream, entry);
  }

  log_err("\nBut in child table ", foreign->m_foreign_table_name, ", in index ", foreign->m_foreign_index->m_name);

  if (rec != nullptr) {
    log_err(", there is a record:");
    log_err(rec_to_string(rec));
  } else {
    log_err(", the record is not available");
  }

  mutex_exit(&m_dict->m_foreign_err_mutex);
}

void Row_insert::foreign_report_add_err(Trx *trx, const Foreign *foreign, const rec_t *rec, const DTuple *entry) noexcept {
  set_detailed(trx, foreign);

  mutex_enter(&m_dict->m_foreign_err_mutex);

  log_err(" Transaction:");
  log_err(trx->to_string(600));
  log_err("Foreign key constraint fails for table ", foreign->m_foreign_table_name, ":");
  m_dict->print_info_on_foreign_key_in_create_format(trx, foreign, true);
  log_err("Trying to add in child table, in index ", foreign->m_foreign_index->m_name);

  if (entry != nullptr) {
    log_err(" tuple:");
    /* TODO: DB_TRX_ID and DB_ROLL_PTR may be uninitialized.
    It would be better to only display the user columns. */
    dtuple_print(ib_stream, entry);
  }

  log_err("But in parent table ", foreign->m_referenced_table_name, ", in index ", foreign->m_referenced_index->m_name);
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

  mutex_exit(&m_dict->m_foreign_err_mutex);
}

db_err Row_insert::update_cascade_client(que_thr_t *thr, upd_node_t *node, Table *table) noexcept {
  auto trx = thr_get_trx(thr);

  for (;;) {
    thr->run_node = node;
    thr->prev_node = node;

    (void) m_row_upd->step(thr);

    auto err = trx->m_error_state;

    /* Note that the cascade node is a subnode of another InnoDB
    query graph node. We do a normal lock wait in this node, but
    all errors are handled by the parent node. */

    if (err != DB_LOCK_WAIT) {
      if (err != DB_SUCCESS) {
        return err;
      }
      break;
    }

    /* Handle lock wait here */

    que_thr_stop_client(thr);

    InnoDB::suspend_user_thread(thr);

    /* Note that a lock wait may also end in a lock wait timeout,
    or this transaction is picked as a victim in selective
    deadlock resolution */

    if (trx->m_error_state != DB_SUCCESS) {

      return trx->m_error_state;
    }

    /* Retry operation after a normal lock wait */
  }

  if (node->m_is_delete) {
    if (table->m_stats.m_n_rows > 0) {
      --table->m_stats.m_n_rows;
    }

    ++srv_n_rows_deleted;
  } else {
    ++srv_n_rows_updated;
  }

  ib_update_statistics_if_needed(table);

  return DB_SUCCESS;
}

db_err Row_insert::foreign_check_on_constraint(que_thr_t *thr, const Foreign *foreign, Btree_pcursor *pcur, DTuple *entry, mtr_t *mtr) noexcept {
  Table *table = foreign->m_foreign_table;
  const Index *index;
  const Index *clust_index;
  DTuple *ref;
  mem_heap_t *upd_vec_heap = nullptr;
  const rec_t *rec;
  const rec_t *clust_rec;
  const Buf_block *clust_block;
  upd_t *update;
  ulint n_to_update;
  db_err err;
  mem_heap_t *tmp_heap{};

  ut_a(thr);
  ut_a(foreign);
  ut_a(pcur);
  ut_a(mtr);

  auto trx = thr_get_trx(thr);
  auto node = static_cast<upd_node_t *>(thr->run_node);

  if (node->m_is_delete && 0 == (foreign->m_type & (DICT_FOREIGN_ON_DELETE_CASCADE | DICT_FOREIGN_ON_DELETE_SET_NULL))) {

    foreign_report_err("Trying to delete", thr, foreign, pcur->get_rec(), entry);

    return DB_ROW_IS_REFERENCED;
  }

  if (!node->m_is_delete && 0 == (foreign->m_type & (DICT_FOREIGN_ON_UPDATE_CASCADE | DICT_FOREIGN_ON_UPDATE_SET_NULL))) {
    /* This is an UPDATE */

    foreign_report_err("Trying to update", thr, foreign, pcur->get_rec(), entry);

    return DB_ROW_IS_REFERENCED;
  }

  if (node->m_cascade_node == nullptr) {
    /* Extend our query graph by creating a child to current
    update node. The child is used in the cascade or set null
    operation. */

    node->m_cascade_heap = mem_heap_create(128);
    node->m_cascade_node = m_row_upd->create_update_node(table, node->m_cascade_heap);
    que_node_set_parent(node->m_cascade_node, node);
  }

  /* Initialize cascade_node to do the operation we want. Note that we
  use the SAME cascade node to do all foreign key operations of the
  SQL DELETE: the table of the cascade node may change if there are
  several child tables to the table where the delete is done! */

  auto cascade = node->m_cascade_node;

  cascade->m_table = table;
  cascade->m_foreign = foreign;

  if (node->m_is_delete && (foreign->m_type & DICT_FOREIGN_ON_DELETE_CASCADE)) {
    cascade->m_is_delete = true;
  } else {
    cascade->m_is_delete = false;

    if (foreign->m_n_fields > cascade->m_update_n_fields) {
      /* We have to make the update vector longer */

      cascade->m_update = Row_update::upd_create(foreign->m_n_fields, node->m_cascade_heap);
      cascade->m_update_n_fields = foreign->m_n_fields;
    }
  }

  /* We do not allow cyclic cascaded updating (DELETE is allowed,
  but not UPDATE) of the same table, as this can lead to an infinite
  cycle. Check that we are not updating the same table which is
  already being modified in this cascade chain. We have to check
  this also because the modification of the indexes of a 'parent'
  table may still be incomplete, and we must avoid seeing the indexes
  of the parent table in an inconsistent state! */

  if (!cascade->m_is_delete && cascade_ancestor_updates_table(cascade, table)) {

    /* We do not know if this would break foreign key
    constraints, but play safe and return an error */

    err = DB_ROW_IS_REFERENCED;

    foreign_report_err(
      "Trying an update, possibly causing a cyclic cascaded update in the child table,",
      thr,
      foreign,
      pcur->get_rec(),
      entry
    );

    goto nonstandard_exit_func;
  }

  if (cascade_n_ancestors(cascade) >= 15) {
    err = DB_ROW_IS_REFERENCED;

    foreign_report_err("Trying a too deep cascaded delete or update\n", thr, foreign, pcur->get_rec(), entry);

    goto nonstandard_exit_func;
  }

  index = pcur->get_index();

  ut_a(index == foreign->m_foreign_index);

  rec = pcur->get_rec();

  if (index->is_clustered()) {
    /* pcur is already positioned in the clustered index of
    the child table */

    clust_index = index;
    clust_rec = rec;
    clust_block = pcur->get_block();
  } else {
    /* We have to look for the record in the clustered index
    in the child table */

    clust_index = table->get_clustered_index();

    tmp_heap = mem_heap_create(256);

    ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, tmp_heap);

    cascade->m_pcur->open_with_no_init(clust_index, ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, false, mtr, Current_location());

    clust_rec = cascade->m_pcur->get_rec();
    clust_block = cascade->m_pcur->get_block();

    if (!page_rec_is_user_rec(clust_rec) || cascade->m_pcur->get_low_match() < clust_index->get_n_unique()) {

      log_err("error in cascade of a foreign key op");
      m_dict->index_name_print(trx, index);

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

  err = m_lock_sys->lock_table(0, table, LOCK_IX, thr);

  if (err == DB_SUCCESS) {
    /* Here it suffices to use a LOCK_REC_NOT_GAP type lock;
    we already have a normal shared lock on the appropriate
    gap if the search criterion was not unique */

    err = m_lock_sys->clust_rec_read_check_and_lock_alt(0, clust_block, clust_rec, clust_index, LOCK_X, LOCK_REC_NOT_GAP, thr);
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

  if ((node->m_is_delete && (foreign->m_type & DICT_FOREIGN_ON_DELETE_SET_NULL)) || (!node->m_is_delete && (foreign->m_type & DICT_FOREIGN_ON_UPDATE_SET_NULL))) {

    /* Build the appropriate update vector which sets
    foreign->n_fields first fields in rec to SQL NULL */

    update = cascade->m_update;

    update->m_info_bits = 0;
    update->m_n_fields = foreign->m_n_fields;

    for (ulint i{}; i < foreign->m_n_fields; ++i) {
      auto ufield = &update->m_fields[i];

      ufield->m_field_no = table->get_nth_col_pos(index->get_nth_table_col_no(i));
      ufield->m_orig_len = 0;
      ufield->m_exp = nullptr;
      dfield_set_null(&ufield->m_new_val);
    }
  }

  if (!node->m_is_delete && (foreign->m_type & DICT_FOREIGN_ON_UPDATE_CASCADE)) {

    /* Build the appropriate update vector which sets changing
    foreign->n_fields first fields in rec to new values */

    upd_vec_heap = mem_heap_create(256);

    n_to_update = cascade_calc_update_vec(node, foreign, upd_vec_heap);

    if (n_to_update == ULINT_UNDEFINED) {
      err = DB_ROW_IS_REFERENCED;

      foreign_report_err(
        "Trying a cascaded update where the updated value in the child table would not fit in the length"
        " of the column, or the value would be NULL and the column is declared as not NULL in the child table.",
        thr,
        foreign,
        pcur->get_rec(),
        entry
      );

      goto nonstandard_exit_func;
    }

    if (cascade->m_update->m_n_fields == 0) {

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
    cascade->m_pcur->copy_stored_position(pcur);
  } else {
    cascade->m_pcur->store_position(mtr);
  }

  mtr->commit();

  ut_a(cascade->m_pcur->get_rel_pos() == Btree_cursor_pos::ON);

  cascade->m_state = UPD_NODE_UPDATE_CLUSTERED;

  err = update_cascade_client(thr, cascade, foreign->m_foreign_table);

  if (foreign->m_foreign_table->m_n_foreign_key_checks_running == 0) {
    log_err(std::format("Table {} has the counter 0 though there is a FOREIGN KEY check running on it.", foreign->m_foreign_table->m_name));
  }

  /* Release the data dictionary latch for a while, so that we do not
  starve other threads from doing CREATE TABLE etc. if we have a huge
  cascaded operation running. The counter n_foreign_key_checks_running
  will prevent other users from dropping or ALTERing the table when we
  release the latch. */

  m_dict->unfreeze_data_dictionary(thr_get_trx(thr));
  m_dict->freeze_data_dictionary(thr_get_trx(thr));

  mtr->start();

  /* Restore pcur position */

  (void) pcur->restore_position(BTR_SEARCH_LEAF, mtr, Current_location());

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

  (void) pcur->restore_position(BTR_SEARCH_LEAF, mtr, Current_location());

  return err;
}

db_err Row_insert::set_shared_rec_lock(ulint type, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (index->is_clustered()) {
    return m_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_S, type, thr);
  } else {
    return m_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_S, type, thr);
  }
}

db_err Row_insert::set_exclusive_rec_lock(ulint type, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (index->is_clustered()) {
    return m_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_X, type, thr);
  } else {
    return m_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, LOCK_X, type, thr);
  }
}

db_err Row_insert::check_foreign_constraint(bool check_ref, const Foreign *foreign, const Table *table, DTuple *entry, que_thr_t *thr) noexcept {
  upd_node_t *upd_node;
  Table *check_table;
  Index *check_index;
  ulint n_fields_cmp;
  bool moved;
  int cmp;
  db_err err;
  ulint i;
  mtr_t mtr;
  Trx *trx = thr_get_trx(thr);
  mem_heap_t *heap = nullptr;
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  rec_offs_init(offsets_);

run_again:

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&m_dict->m_lock, RW_LOCK_SHARED));
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

  for (i = 0; i < foreign->m_n_fields; i++) {
    if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

      goto exit_func;
    }
  }

  if (que_node_get_type(thr->run_node) == QUE_NODE_UPDATE) {
    upd_node = static_cast<upd_node_t *>(thr->run_node);

    if (!upd_node->m_is_delete && upd_node->m_foreign == foreign) {
      /* If a cascaded update is done as defined by a foreign key constraint, do not check that
      constraint for the child row. In ON UPDATE CASCADE the update of the parent row is only half done when
      we come here: if we would check the constraint here for the child row it would fail.

      A QUESTION remains: if in the child table there are several constraints which refer to the same parent
      table, we should merge all updates to the child as one update? And the updates can be contradictory!
      Currently we just perform the update associated with each foreign key constraint, one after
      another, and the user has problems predicting in which order they are performed. */

      goto exit_func;
    }
  }

  if (check_ref) {
    check_table = foreign->m_referenced_table;
    check_index = foreign->m_referenced_index;
  } else {
    check_table = foreign->m_foreign_table;
    check_index = foreign->m_foreign_index;
  }

  if (check_table == nullptr || check_table->m_ibd_file_missing) {
    if (check_ref) {
      set_detailed(trx, foreign);

      mutex_enter(&m_dict->m_foreign_err_mutex);
      log_err(std::format(" Transaction: {} foreign key constraint fails for table {}: ", trx->to_string(600), foreign->m_foreign_table_name));
      m_dict->print_info_on_foreign_key_in_create_format(trx, foreign, true);
      log_err("Trying to add to index {}", foreign->m_foreign_index->m_name, " tuple: ");
      dtuple_print(ib_stream, entry);
      log_err("\nBut the parent table {}", foreign->m_referenced_table_name, " does not currently exist! Or, it's .ibd file is missing.");
      mutex_exit(&m_dict->m_foreign_err_mutex);

      err = DB_NO_REFERENCED_ROW;
    }

    goto exit_func;
  }

  ut_a(check_table != nullptr);
  ut_a(check_index != nullptr);

  if (check_table != table) {
    /* We already have a LOCK_IX on table, but not necessarily
    on check_table */

    err = m_lock_sys->lock_table(0, check_table, LOCK_IS, thr);

    if (err != DB_SUCCESS) {

      goto do_possible_lock_wait;
    }
  }

  mtr.start();

  /* Store old value on n_fields_cmp */

  n_fields_cmp = dtuple_get_n_fields_cmp(entry);

  dtuple_set_n_fields_cmp(entry, foreign->m_n_fields);

  pcur.open(check_index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  /* Scan index records and check if there is a matching record */

  for (;;) {
    const rec_t *rec = pcur.get_rec();
    const Buf_block *block = pcur.get_block();

    if (page_rec_is_infimum(rec)) {

      goto next_rec;
    }

    {
      Phy_rec record{check_index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    if (page_rec_is_supremum(rec)) {

      err = set_shared_rec_lock(LOCK_ORDINARY, block, rec, check_index, offsets, thr);

      if (err != DB_SUCCESS) {

        break;
      }

      goto next_rec;
    }

    cmp = cmp_dtuple_rec(check_index->m_cmp_ctx, entry, rec, offsets);

    if (cmp == 0) {
      if (rec_get_deleted_flag(rec)) {
        err = set_shared_rec_lock(LOCK_ORDINARY, block, rec, check_index, offsets, thr);
        if (err != DB_SUCCESS) {

          break;
        }
      } else {
        /* Found a matching record. Lock only
        a record because we can allow inserts
        into gaps */

        err = set_shared_rec_lock(LOCK_REC_NOT_GAP, block, rec, check_index, offsets, thr);

        if (err != DB_SUCCESS) {

          break;
        }

        if (check_ref) {
          err = DB_SUCCESS;

          break;
        } else if (foreign->m_type != 0) {
          /* There is an ON UPDATE or ON DELETE condition: check them in a separate function */

          err = foreign_check_on_constraint(thr, foreign, &pcur, entry, &mtr);
          if (err != DB_SUCCESS) {
            /* Since reporting a plain "duplicate key" error message to the user in cases where a long CASCADE
            operation would lead to a duplicate key in some other table is very confusing, map duplicate
            key errors resulting from FK constraints to a separate error code. */

            if (err == DB_DUPLICATE_KEY) {
              err = DB_FOREIGN_DUPLICATE_KEY;
            }

            break;
          }

          /* foreign_check_on_constraint may have repositioned pcur on a different block */
          block = pcur.get_block();
        } else {
          foreign_report_err("Trying to delete or update", thr, foreign, rec, entry);

          err = DB_ROW_IS_REFERENCED;
          break;
        }
      }
    }

    if (cmp < 0) {
      err = set_shared_rec_lock(LOCK_GAP, block, rec, check_index, offsets, thr);
      if (err != DB_SUCCESS) {

        break;
      }

      if (check_ref) {
        err = DB_NO_REFERENCED_ROW;
        foreign_report_add_err(trx, foreign, rec, entry);
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
        foreign_report_add_err(trx, foreign, rec, entry);
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
    trx->m_error_state = static_cast<db_err>(err);

    que_thr_stop_client(thr);

    InnoDB::suspend_user_thread(thr);

    if (trx->m_error_state == DB_SUCCESS) {

      goto run_again;
    }

    err = trx->m_error_state;
  }

exit_func:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

db_err Row_insert::check_foreign_constraints(const Table *table, const Index *index, DTuple *entry, que_thr_t *thr) noexcept {
  db_err err;
  bool got_s_lock = false;

  auto trx = thr_get_trx(thr);

  for (auto foreign : table->m_foreign_list) {
    if (foreign->m_foreign_index == index) {

      if (foreign->m_referenced_table == nullptr) {
        (void) m_dict->table_get(foreign->m_referenced_table_name, false);
      }

      if (0 == trx->m_dict_operation_lock_mode) {
        got_s_lock = true;

        m_dict->freeze_data_dictionary(trx);
      }

      if (foreign->m_referenced_table) {
        m_dict->mutex_acquire();

        ++foreign->m_referenced_table->m_n_foreign_key_checks_running;

        m_dict->mutex_release();
      }

      /* NOTE that if the thread ends up waiting for a lock we will release dict_operation_lock temporarily!
      But the counter on the table protects the referenced table from being dropped while the check is running. */

      err = check_foreign_constraint(true, foreign, table, entry, thr);

      if (foreign->m_referenced_table) {
        m_dict->mutex_acquire();

        ut_a(foreign->m_referenced_table->m_n_foreign_key_checks_running > 0);
        --foreign->m_referenced_table->m_n_foreign_key_checks_running;

        m_dict->mutex_release();
      }

      if (got_s_lock) {
        m_dict->unfreeze_data_dictionary(trx);
      }

      if (err != DB_SUCCESS) {
        return err;
      }
    }
  }

  return DB_SUCCESS;
}

bool Row_insert::dupl_error_with_rec(const rec_t *rec, const DTuple *entry, const Index *index, const ulint *offsets) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));

  const auto n_unique = index->get_n_unique();

  ulint matched_bytes{};
  ulint matched_fields{};

  cmp_dtuple_rec_with_match(index->m_cmp_ctx, entry, rec, offsets, &matched_fields, &matched_bytes);

  if (matched_fields < n_unique) {

    return false;
  }

  /* In a unique secondary index we allow equal key values if they
  contain SQL NULLs */

  if (!index->is_clustered()) {

    for (ulint i = 0; i < n_unique; ++i) {
      if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

        return false;
      }
    }
  }

  return !rec_get_deleted_flag(rec);
}

db_err Row_insert::scan_sec_index_for_duplicate(const Index *index, DTuple *entry, que_thr_t *thr) noexcept {
  int cmp;
  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);
  db_err err = DB_SUCCESS;
  unsigned allow_duplicates;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  const auto n_unique = index->get_n_unique();

  rec_offs_init(offsets_);

  /* If the secondary index is unique, but one of the fields in the
  n_unique first fields is NULL, a unique key violation cannot occur,
  since we define NULL != NULL in this case */

  for (ulint i = 0; i < n_unique; ++i) {
    if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i))) {

      return DB_SUCCESS;
    }
  }

  mtr.start();

  /* Store old value on n_fields_cmp */

  const auto n_fields_cmp = dtuple_get_n_fields_cmp(entry);

  dtuple_set_n_fields_cmp(entry, index->get_n_unique());

  pcur.open(index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

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

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    if (allow_duplicates) {

      /* If the SQL-query will update or replace
      duplicate key we will take X-lock for
      duplicates ( REPLACE, LOAD DATAFILE REPLACE,
      INSERT ON DUPLICATE KEY UPDATE). */

      err = set_exclusive_rec_lock(LOCK_ORDINARY, block, rec, index, offsets, thr);
    } else {

      err = set_shared_rec_lock(LOCK_ORDINARY, block, rec, index, offsets, thr);
    }

    if (err != DB_SUCCESS) {

      break;
    }

    if (page_rec_is_supremum(rec)) {

      continue;
    }

    cmp = cmp_dtuple_rec(index->m_cmp_ctx, entry, rec, offsets);

    if (cmp == 0) {
      if (dupl_error_with_rec(rec, entry, index, offsets)) {
        err = DB_DUPLICATE_KEY;

        thr_get_trx(thr)->m_error_info = index;

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

db_err Row_insert::duplicate_error_in_clust(Btree_cursor *btr_cur, DTuple *entry, que_thr_t *thr, mtr_t *mtr) noexcept {
  db_err err;
  rec_t *rec;
  Trx *trx = thr_get_trx(thr);
  mem_heap_t *heap = nullptr;
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();

  rec_offs_init(offsets_);

  UT_NOT_USED(mtr);

  ut_a(btr_cur->m_index->is_clustered());
  ut_ad(btr_cur->m_index->is_unique());

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

  const auto n_unique = btr_cur->m_index->get_n_unique();

  if (btr_cur->m_low_match >= n_unique) {

    rec = btr_cur->get_rec();

    if (!page_rec_is_infimum(rec)) {
      {
        Phy_rec record{btr_cur->m_index, rec};

        offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
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

        err = set_exclusive_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      } else {

        err = set_shared_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      }

      if (err != DB_SUCCESS) {
        goto func_exit;
      }

      if (dupl_error_with_rec(rec, entry, btr_cur->m_index, offsets)) {
        trx->m_error_info = btr_cur->m_index;
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

        offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
      }

      if (trx->m_duplicates & TRX_DUP_IGNORE) {

        /* If the SQL-query will update or replace duplicate key we will take X-lock for
        duplicates ( REPLACE, LOAD DATAFILE REPLACE, INSERT ON DUPLICATE KEY UPDATE). */

        err = set_exclusive_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      } else {

        err = set_shared_rec_lock(LOCK_REC_NOT_GAP, btr_cur->get_block(), rec, btr_cur->m_index, offsets, thr);
      }

      if (err != DB_SUCCESS) {
        goto func_exit;
      }

      if (dupl_error_with_rec(rec, entry, btr_cur->m_index, offsets)) {
        trx->m_error_info = btr_cur->m_index;
        err = DB_DUPLICATE_KEY;
        goto func_exit;
      }
    }

    ut_a(!btr_cur->m_index->is_clustered());
    /* This should never happen */
  }

  err = DB_SUCCESS;

func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

inline ulint Row_insert::must_modify(Btree_cursor *btr_cur) noexcept {
  /* NOTE: (compare to the note in duplicate_error) Because node
  pointers on upper levels of the B-tree may match more to entry than
  to actual user records on the leaf level, we have to check if the
  candidate record is actually a user record. In a clustered index
  node pointers contain index->n_unique first fields, and in the case
  of a secondary index, all fields of the index. */

  const auto enough_match = btr_cur->m_index->get_n_unique_in_tree();

  if (btr_cur->m_low_match >= enough_match) {

    const auto rec = btr_cur->get_rec();

    if (!page_rec_is_infimum(rec)) {
      return ROW_INS_PREV;
    }
  }

  return 0;
}

db_err Row_insert::index_entry_low(ulint mode, const Index *index, DTuple *entry, ulint n_ext, que_thr_t *thr) noexcept {
  ulint modify{};
  rec_t *insert_rec;
  rec_t *rec;
  db_err err;
  mem_heap_t *heap = nullptr;
  big_rec_t *big_rec = nullptr;
  Btree_cursor btr_cur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  log_sys->free_check();

  mtr_t mtr;

  mtr.start();

  btr_cur.m_thr = thr;

  btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, mode | BTR_INSERT, &mtr, Current_location());

#ifdef UNIV_DEBUG
  {
    auto page = btr_cur.get_page_no();
    auto first_rec = page_rec_get_next(page_get_infimum_rec(page));

    ut_ad(page_rec_is_supremum(first_rec) || rec_get_n_fields(first_rec, index) == dtuple_get_n_fields(entry));
  }
#endif /* UNIV_DEBUG */

  const auto n_unique = index->get_n_unique();

  if (index->is_unique() && (btr_cur.m_up_match >= n_unique ||btr_cur.m_low_match >= n_unique)) {

    if (index->is_clustered()) {
      /* Note that the following may return also
      DB_LOCK_WAIT */

      err = duplicate_error_in_clust(&btr_cur, entry, thr, &mtr);

      if (err != DB_SUCCESS) {

        goto function_exit;
      }
    } else {
      mtr.commit();
      err = scan_sec_index_for_duplicate(index, entry, thr);
      mtr.start();

      if (err != DB_SUCCESS) {

        goto function_exit;
      }

      /* We did not find a duplicate and we have now
      locked with s-locks the necessary records to
      prevent any insertion of a duplicate by another
      transaction. Let us now reposition the cursor and
      continue the insertion. */

      btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, mode | BTR_INSERT, &mtr, Current_location());
    }
  }

  modify = must_modify(&btr_cur);

  if (modify != 0) {
    /* There is already an index entry with a long enough common
    prefix, we must convert the insert into a modify of an
    existing record */

    if (modify == ROW_INS_NEXT) {
      rec = page_rec_get_next(btr_cur.get_rec());

      btr_cur.position(index, rec, btr_cur.get_block());
    }

    if (index->is_clustered()) {
      err = clust_index_entry_by_modify(mode, &btr_cur, &heap, &big_rec, entry, thr, &mtr);
    } else {
      ut_ad(!n_ext);
      err = sec_index_entry_by_modify(mode, &btr_cur, entry, thr, &mtr);
    }
  } else {

    if (mode == BTR_MODIFY_LEAF) {

      err = btr_cur.optimistic_insert(0, entry, &insert_rec, &big_rec, n_ext, thr, &mtr);

    } else {
      ut_a(mode == BTR_MODIFY_TREE);

      auto buf_pool = m_dict->m_store.m_fsp->m_buf_pool;

      if (unlikely(buf_pool->m_LRU->buf_pool_running_out())) {

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

    btr_cur.search_to_nth_level(nullptr, index, 0, entry, PAGE_CUR_LE, BTR_MODIFY_TREE, &mtr, Current_location());

    ulint *offsets;
    auto rec = btr_cur.get_rec();

    {
      Phy_rec record{index, rec};

      offsets = record.get_all_col_offsets(nullptr, &heap, Current_location());
    }

    Blob blob(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

    err = blob.store_big_rec_extern_fields(index, btr_cur.get_block(), rec, offsets, big_rec, &mtr);

    if (modify) {
      dtuple_big_rec_free(big_rec);
    } else {
      dtuple_convert_back_big_rec(entry, big_rec);
    }

    mtr.commit();
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

db_err Row_insert::index_entry(const Index *index, DTuple *entry, ulint n_ext, bool foreign, que_thr_t *thr) noexcept {
  if (foreign && !index->m_table->m_foreign_list.empty()) {
    const auto err = check_foreign_constraints(index->m_table, index, entry, thr);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  /* Try first optimistic descent to the B-tree */
  {
    const auto err = index_entry_low(BTR_MODIFY_LEAF, index, entry, n_ext, thr);

    if (err != DB_FAIL) {

      return err;
    }
  }

  /* Try then pessimistic descent to the B-tree */
  return index_entry_low(BTR_MODIFY_TREE, index, entry, n_ext, thr);
}

void Row_insert::index_entry_set_vals(Index *index, DTuple *entry, const DTuple *row) noexcept {
  ut_ad(entry && row);

  const auto n_fields = dtuple_get_n_fields(entry);

  for (ulint i{}; i < n_fields; ++i) {
    auto field = dtuple_get_nth_field(entry, i);
    auto ind_field = index->get_nth_field(i);
    auto row_field = dtuple_get_nth_field(row, ind_field->m_col->m_ind);
    auto len = dfield_get_len(row_field);

    /* Check column prefix indexes */
    if (ind_field->m_prefix_len > 0 && dfield_get_len(row_field) != UNIV_SQL_NULL) {

      const auto col = ind_field->get_col();

      len = dtype_get_at_most_n_mbchars(
        col->prtype, col->mbminlen, col->mbmaxlen, ind_field->m_prefix_len, len, (char *)dfield_get_data(row_field)
      );

      ut_ad(!dfield_is_ext(row_field));
    }

    dfield_set_data(field, dfield_get_data(row_field), len);

    if (dfield_is_ext(row_field)) {
      ut_ad(index->is_clustered());
      dfield_set_ext(field);
    }
  }
}

db_err Row_insert::index_entry_step(ins_node_t *node, que_thr_t *thr) noexcept {
  ut_ad(dtuple_check_typed(node->m_row));

  index_entry_set_vals(node->m_index, node->m_entry, node->m_row);

  ut_ad(dtuple_check_typed(node->m_entry));

  return index_entry(node->m_index, node->m_entry, 0, true, thr);
}

inline void Row_insert::alloc_row_id_step(ins_node_t *node) noexcept {
  ut_ad(node->m_state == INS_NODE_ALLOC_ROW_ID);

  /* No row id is stored if the clustered index is unique */
  if (!node->m_table->get_clustered_index()->is_unique()) {
    auto row_id = m_dict->m_store.sys_get_new_row_id();

    m_dict->m_store.sys_write_row_id(node->m_row_id_buf, row_id);
  }
}

void Row_insert::get_row_from_values(ins_node_t *node) noexcept {
  /* The field values are copied in the buffers of the select node and
  it is safe to use them until we fetch from select again: therefore
  we can just copy the pointers */

  auto row = node->m_row;

  ulint i{};
  auto list_node = node->m_values_list;

  while (list_node != nullptr) {
    eval_exp(list_node);

    auto dfield = dtuple_get_nth_field(row, i);
    dfield_copy_data(dfield, que_node_get_val(list_node));

    ++i;

    list_node = que_node_get_next(list_node);
  }
}

inline void Row_insert::get_row_from_select(ins_node_t *node) noexcept {
  /* The field values are copied in the buffers of the select node and
  it is safe to use them until we fetch from select again: therefore
  we can just copy the pointers */

  auto row = node->m_row;

  ulint i{};
  auto list_node = node->m_select->m_select_list;

  while (list_node != nullptr) {
    auto dfield = dtuple_get_nth_field(row, i);

    dfield_copy_data(dfield, que_node_get_val(list_node));

    ++i;

    list_node = que_node_get_next(list_node);
  }
}

db_err Row_insert::insert(ins_node_t *node, que_thr_t *thr) noexcept {
  db_err err;

  if (node->m_state == INS_NODE_ALLOC_ROW_ID) {

    alloc_row_id_step(node);

    node->m_index = node->m_table->get_clustered_index();
    node->m_entry = UT_LIST_GET_FIRST(node->m_entry_list);

    if (node->m_ins_type == INS_SEARCHED) {

      get_row_from_select(node);

    } else if (node->m_ins_type == INS_VALUES) {

      get_row_from_values(node);
    }

    node->m_state = INS_NODE_INSERT_ENTRIES;
  }

  ut_ad(node->m_state == INS_NODE_INSERT_ENTRIES);

  while (node->m_index != nullptr) {
    err = index_entry_step(node, thr);

    if (err != DB_SUCCESS) {

      return err;
    }

    node->m_index = node->m_index->get_next();
    node->m_entry = UT_LIST_GET_NEXT(tuple_list, node->m_entry);
  }

  ut_ad(node->m_entry == nullptr);

  node->m_state = INS_NODE_ALLOC_ROW_ID;

  return DB_SUCCESS;
}

que_thr_t *Row_insert::step(que_thr_t *thr) noexcept {
  db_err err;

  auto trx = thr_get_trx(thr);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = static_cast<ins_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_INSERT);

  auto parent = que_node_get_parent(node);
  auto sel_node = node->m_select;

  if (thr->prev_node == parent) {
    node->m_state = INS_NODE_SET_IX_LOCK;
  }

  /* If this is the first time this node is executed (or when
  execution resumes after wait for the table IX lock), set an
  IX lock on the table and reset the possible select node. If the
  client code calls an insert within the same SQL statement AFTER
  it has used this table handle to do a search. In that case, we
  have already set the IX lock on the table during the search
  operation, and there is no need to set it again here. But we
  must write trx->m_id to node->trx_id. */

  Trx_sys::write_trx_id(node->m_trx_id_buf, trx->m_id);

  auto insert_row = [&](ins_node_t *node, que_thr_t *thr) noexcept -> auto {
    node->m_state = INS_NODE_ALLOC_ROW_ID;

    if (node->m_ins_type == INS_SEARCHED) {
      /* Reset the cursor */
      sel_node->m_state = SEL_NODE_OPEN;

      /* Fetch a row to insert */
      thr->run_node = sel_node;

      return true;
    } else {
      return false;
    }
  };

  auto error = [thr, sel_node](ins_node_t *node, Trx *trx, db_err err) noexcept -> que_thr_t * {
    trx->m_error_state = static_cast<db_err>(err);

    if (err != DB_SUCCESS) {
      /* err == DB_LOCK_WAIT or SQL error detected */
      return nullptr;
    }

    /* DO THE TRIGGER ACTIONS HERE */

    if (node->m_ins_type == INS_SEARCHED) {
      /* Fetch a row to insert */
      thr->run_node = sel_node;
    } else {
      thr->run_node = que_node_get_parent(node);
    }

    return thr;
  };

  if (node->m_state == INS_NODE_SET_IX_LOCK) {
    /* It may be that the current session has not yet started
    its transaction, or it has been committed: */
    if (trx->m_id == node->m_trx_id) {
      /* No need to do IX-locking */
      if (insert_row(node, thr)) {
        return thr;
      }

    } else {

      err = m_lock_sys->lock_table(0, node->m_table, LOCK_IX, thr);

      if (err != DB_SUCCESS) {

        return error(node, trx, err);
      }

      node->m_trx_id = trx->m_id;

      if (insert_row(node, thr)) {
        return thr;
      }
    }
  }

  if (node->m_ins_type == INS_SEARCHED && sel_node->m_state != SEL_NODE_FETCH) {

    ut_ad(sel_node->m_state == SEL_NODE_NO_MORE_ROWS);

    /* No more rows to insert */
    thr->run_node = parent;

    return thr;

  } else {
    /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */
    return error(node, trx, insert(node, thr));
  }
}
