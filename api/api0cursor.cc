/***********************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

************************************************************************/

/* Cursors, row insert/update/delete/read, tuples. */

#include "api0int.h"

namespace api {

/**
 * Create an internal cursor instance.
 *
 * @param[out] crsr out: InnoDB cursor
 * @param[in] table in: table instance
 * @param[in] index_id in: index id or 0
 * @param[in] trx in: transaction
 *
 * @return DB_SUCCESS or err code
 */
DbErr create_cursor(System &sys, Cursor **crsr, Table *table, uint64_t index_id, Trx *trx) noexcept {
  DbErr err = DB_SUCCESS;
  const Dict_id id = index_id;

  auto heap = mem_heap_create(sizeof(Cursor) * 2);

  if (heap == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto cursor = reinterpret_cast<Cursor *>(mem_heap_zalloc(heap, sizeof(Cursor)));

  cursor->m_heap = heap;

  cursor->m_query_heap = mem_heap_create(64);

  if (cursor->m_query_heap == nullptr) {
    mem_heap_free(heap);

    return DB_OUT_OF_MEMORY;
  }

  cursor->m_prebuilt = Prebuilt::create(table);

  auto prebuilt = cursor->m_prebuilt;

  prebuilt->m_trx = trx;
  prebuilt->m_table = table;
  prebuilt->m_select_lock_type = LOCK_NONE;

  if (index_id > 0) {
    prebuilt->m_index = table->index_get_on_id(id);
  } else {
    prebuilt->m_index = table->get_first_index();
  }

  ut_a(prebuilt->m_index != nullptr);

  if (prebuilt->m_trx != nullptr) {
    ++prebuilt->m_trx->m_n_client_tables_in_use;

    prebuilt->m_index_usable = row_merge_is_index_usable(prebuilt->m_trx, prebuilt->m_index);

    /* Assign a read view if the transaction does not have it yet */

    auto rv = prebuilt->m_trx->assign_read_view(sys);
    ut_a(rv != nullptr);
  }

  *crsr = cursor;

  return err;
}

DbErr cursor_open_table_using_id(System &sys, uint64_t table_id, Trx *trx, Cursor **crsr) noexcept {
  Table *table;

  if (trx == nullptr || !schema_lock_is_exclusive(trx)) {
    table = open_table_by_id(sys, table_id, false);
  } else {
    table = open_table_by_id(sys, table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  return create_cursor(sys, crsr, table, 0, trx);
}

DbErr cursor_open_index_using_id(System &sys, uint64_t index_id, Trx *trx, Cursor **crsr) noexcept {
  Table *table;
  ulint table_id = (ulint)(index_id >> 32);

  if (trx == nullptr || !schema_lock_is_exclusive(trx)) {
    table = open_table_by_id(sys, table_id, false);
  } else {
    table = open_table_by_id(sys, table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  /* We only return the lower 32 bits of the uint64_t. */
  auto err = create_cursor(sys, crsr, table, index_id & 0xFFFFFFFFULL, trx);

  if (crsr != nullptr) {
    auto cursor = *(Cursor **)crsr;

    if (cursor->m_prebuilt->m_index == nullptr) {
      auto crsr_err = Cursor::close(sys, *crsr);
      ut_a(crsr_err == DB_SUCCESS);

      *crsr = nullptr;
    }
  }

  return err;
}

DbErr Cursor::open_index(System &sys, const char *index_name, Cursor **crsr) noexcept {
  auto cursor = this;
  Table *table;
  uint64_t index_id = 0;
  DbErr err = DB_TABLE_NOT_FOUND;
  auto trx = cursor->m_prebuilt->m_trx;

  if (trx != nullptr && !schema_lock_is_exclusive(trx)) {
    sys.m_dict->mutex_acquire();
  }

  /* We want to increment the ref count, so we do a redundant search. */
  table = sys.m_dict->table_get_using_id(sys.m_config.m_recv.m_force_recovery, cursor->m_prebuilt->m_table->m_id, true);
  ut_a(table != nullptr);

  if (trx != nullptr && !schema_lock_is_exclusive(trx)) {
    sys.m_dict->mutex_release();
  }

  /* The first index is always the cluster index. */
  for (auto index : table->m_indexes) {
    if (strcmp(index->m_name, index_name) == 0) {
      index_id = index->m_id;
    }
  }

  *crsr = nullptr;

  if (index_id > 0) {
    err = create_cursor(sys, crsr, table, index_id, cursor->m_prebuilt->m_trx);
  }

  if (*crsr != nullptr) {
    const Cursor *cursor;

    cursor = *(Cursor **)crsr;

    if (cursor->m_prebuilt->m_index == nullptr) {
      err = Cursor::close(sys, *crsr);
      ut_a(err == DB_SUCCESS);
      *crsr = nullptr;
    }
  } else {
    /* The dictionary mutex is held here only under the exclusive schema lock. */
    sys.m_dict->table_decrement_handle_count(table, trx != nullptr && schema_lock_is_exclusive(trx));
  }

  return err;
}

DbErr cursor_open_table(System &sys, const char *name, Trx *trx, Cursor **crsr) noexcept {
  DbErr err;
  Table *table;

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  normalize_table_name(normalized_name, name);

  if (trx != nullptr) {
    if (!schema_lock_is_exclusive(trx)) {
      table = open_table_by_name(sys, normalized_name);
    } else {
      table = lookup_table_by_name(sys, normalized_name);

      if (table != nullptr) {
        sys.m_dict->table_increment_handle_count(table, true);
      }
    }
  } else {
    table = open_table_by_name(sys, normalized_name);
  }

  mem_free(normalized_name);
  normalized_name = nullptr;

  /* It can happen that another thread has created the table but
  not the cluster index or it's a broken table definition. Refuse to
  open if that's the case. */
  if (table != nullptr && table->get_first_index() == nullptr) {
    sys.m_dict->table_decrement_handle_count(table, false);
    table = nullptr;
  }

  if (table != nullptr) {
    err = create_cursor(sys, crsr, table, 0, trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  return err;
}

/**
 * Free a context struct for a table handle.
 *
 * @param[in] q_proc in, own: qproc struct
 */
static void qry_proc_free(QueryProc *q_proc) {
  que_graph_free_recursive(q_proc->m_grph.m_ins);
  que_graph_free_recursive(q_proc->m_grph.m_upd);
  que_graph_free_recursive(q_proc->m_grph.m_sel);

  *q_proc = QueryProc{};
}

DbErr Cursor::reset(System &sys) noexcept {
  auto cursor = this;
  Prebuilt *prebuilt = cursor->m_prebuilt;

  if (prebuilt->m_trx != nullptr && prebuilt->m_trx->m_n_client_tables_in_use > 0) {

    --prebuilt->m_trx->m_n_client_tables_in_use;
  }

  /* The fields in this data structure are allocated from
  the query heap and so need to be reset too. */
  qry_proc_free(&cursor->m_q_proc);

  mem_heap_empty(cursor->m_query_heap);

  prebuilt->clear();

  return DB_SUCCESS;
}

DbErr Cursor::close(System &sys, Cursor *cursor) noexcept {
  Prebuilt *prebuilt = cursor->m_prebuilt;
  Trx *trx = prebuilt->m_trx;

  qry_proc_free(&cursor->m_q_proc);

  /* The transaction could have been detached from the cursor. */
  if (trx != nullptr && trx->m_n_client_tables_in_use > 0) {
    --trx->m_n_client_tables_in_use;
  }

  if (trx != nullptr && schema_lock_is_exclusive(trx)) {
    Prebuilt::destroy(sys, cursor->m_prebuilt, true);
  } else {
    Prebuilt::destroy(sys, cursor->m_prebuilt, false);
  }

  mem_heap_free(cursor->m_query_heap);
  mem_heap_free(cursor->m_heap);

  return DB_SUCCESS;
}

/**
 * Run the insert query and do error handling.
 *
 * @param[in] thr in: insert query graph
 * @param[in] node in: insert node for the query
 * @param[in] savept in: savepoint to rollback to in case of an error
 *
 * @return DB_SUCCESS or error code
 */
static DbErr insert_row_with_lock_retry(System &sys, QueThr *thr, InsertNode *node, TrxSavept *savept) noexcept {
  DbErr err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    (void)sys.m_row_ins->step(thr);

    err = trx->m_error_state;

    if (err != DB_SUCCESS) {
      que_thr_stop_client(sys, thr);

      thr->lock_state = QUE_THR_LOCK_ROW;
      lock_wait = row_handle_errors(sys, &err, trx, thr, savept);
      thr->lock_state = QUE_THR_LOCK_NOLOCK;
    } else {
      lock_wait = false;
    }
  } while (lock_wait);

  return err;
}

/**
 * Write a row.
 *
 * @param[in] table in: table where to insert
 * @param[in] ins_graph in: query graph
 * @param[in] node in: insert node
 *
 * @return DB_SUCCESS or err code
 */
static DbErr execute_insert_query_graph(System &sys, Table *table, QueFork *ins_graph, InsertNode *node) noexcept {
  /* This is a short term solution to fix the purge lag. */
  delay_dml_if_needed(sys);

  auto trx = ins_graph->trx;
  auto savept = trx_savept_take(trx);
  auto thr = que_fork_get_first_thr(ins_graph);

  que_thr_move_to_run_state(thr);

  auto err = insert_row_with_lock_retry(sys, thr, node, &savept);

  if (err == DB_SUCCESS) {
    que_thr_stop_for_client_no_error(thr, trx);

    ++table->m_stats.m_n_rows;

    sys.m_srv->m_stats.m_n_rows_inserted++;

    row_update_statistics_if_needed(sys, table);

    wake_master_thread(sys);
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create an insert query graph node.
 *
 * @param[in] cursor in: Cursor instance
 */
static void insert_query_graph_create(System &sys, Cursor *cursor) noexcept {
  auto q_proc = &cursor->m_q_proc;
  auto node = &q_proc->m_node;
  auto trx = cursor->m_prebuilt->m_trx;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->m_ins == nullptr) {
    auto grph = &q_proc->m_grph;
    auto heap = cursor->m_query_heap;
    auto table = cursor->m_prebuilt->m_table;

    node->m_ins = sys.m_row_ins->node_create(INS_DIRECT, table, heap);

    node->m_ins->m_select = nullptr;
    node->m_ins->m_values_list = nullptr;

    auto row = dtuple_create(heap, table->get_n_cols());
    table->copy_types(row);

    sys.m_row_ins->set_new_row(node->m_ins, row);

    grph->m_ins = static_cast<QueFork *>(que_node_get_parent(pars_complete_graph_for_exec(node->m_ins, trx, heap)));

    grph->m_ins->state = QUE_FORK_ACTIVE;
  }
}

DbErr Cursor::insert_row(System &sys, const Tuple *src_tuple) noexcept {
  auto cursor = this;
  ulint n_fields;
  QueryNode *node;
  QueryProc *q_proc;
  DTuple *dst_dtuple;
  DbErr err = DB_SUCCESS;

  insert_query_graph_create(sys, cursor);

  ut_ad(src_tuple->m_type == TPL_ROW);

  q_proc = &cursor->m_q_proc;
  node = &q_proc->m_node;

  node->m_ins->m_state = INS_NODE_ALLOC_ROW_ID;
  dst_dtuple = node->m_ins->m_row;

  n_fields = dtuple_get_n_fields(src_tuple->m_ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_dtuple));

  /* Do a shallow copy of the data fields and check for nullptr
  constraints on columns. */
  for (ulint i = 0; i < n_fields; ++i) {
    auto src_field = dtuple_get_nth_field(src_tuple->m_ptr, i);
    auto mtype = dtype_get_mtype(dfield_get_type(src_field));

    /* Don't touch the system columns. */
    if (mtype != DATA_SYS) {
      auto prtype = dtype_get_prtype(dfield_get_type(src_field));

      if ((prtype & DATA_NOT_NULL) && dfield_is_null(src_field)) {

        err = DB_DATA_MISMATCH;
        break;
      }

      auto dst_field = dtuple_get_nth_field(dst_dtuple, i);
      ut_ad(mtype == dtype_get_mtype(dfield_get_type(dst_field)));

      /* Do a shallow copy. */
      dfield_set_data(dst_field, src_field->data, src_field->len);

      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);
      UNIV_MEM_ASSERT_RW(dst_field->data, dst_field->len);
    }
  }

  if (err == DB_SUCCESS) {
    err = execute_insert_query_graph(sys, src_tuple->m_index->m_table, q_proc->m_grph.m_ins, node->m_ins);
  }

  return err;
}

/**
 * Gets pointer to a prebuilt update vector used in updates.
 *
 * @param[in] cursor in: current cursor
 *
 * @return update vector
 */
static Upd *update_vector_create(System &sys, Cursor *cursor) {
  auto trx = cursor->m_prebuilt->m_trx;
  auto heap = cursor->m_query_heap;
  auto table = cursor->m_prebuilt->m_table;
  auto q_proc = &cursor->m_q_proc;
  auto grph = &q_proc->m_grph;
  auto node = &q_proc->m_node;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->m_upd == nullptr) {
    node->m_upd = sys.m_row_upd->create_update_node(table, heap);
  }

  grph->m_upd = static_cast<QueFork *>(que_node_get_parent(pars_complete_graph_for_exec(node->m_upd, trx, heap)));

  grph->m_upd->state = QUE_FORK_ACTIVE;

  return node->m_upd->m_update;
}

/**
 * Note that a column has changed.
 *
 * @param[in] cursor in: current cursor
 * @param[in/out] upd_field in/out: update field
 * @param[in] col_no in: column number
 * @param[in] dfield in: updated dfield
 */
static void update_col(Cursor *cursor, UpdField *upd_field, ulint col_no, Dfield *dfield) {
  auto table = cursor->m_prebuilt->m_table;
  auto index = table->get_first_index();

  auto data_len = dfield_get_len(dfield);

  if (data_len == UNIV_SQL_NULL) {
    dfield_set_null(&upd_field->m_new_val);
  } else {
    dfield_copy_data(&upd_field->m_new_val, dfield);
  }

  upd_field->m_exp = nullptr;

  upd_field->m_orig_len = 0;

  upd_field->m_field_no = index->get_clustered_field_pos(&table->m_cols[col_no]);
}

/**
 * Checks which fields have changed in a row and stores the new data
 * to an update vector.
 *
 * @param[in] cursor in: current cursor
 * @param[in/out] upd in/out: update vector
 * @param[in] old_tuple in: Old tuple in table
 * @param[in] new_tuple in: New tuple to update
 *
 * @return DB_SUCCESS or err code
 */
static DbErr calc_diff(Cursor *cursor, Upd *upd, const Tuple *old_tuple, const Tuple *new_tuple) noexcept {
  ulint n_changed = 0;
  DbErr err = DB_SUCCESS;
  const auto n_fields = dtuple_get_n_fields(new_tuple->m_ptr);

  ut_a(old_tuple->m_type == TPL_ROW);
  ut_a(new_tuple->m_type == TPL_ROW);
  ut_a(old_tuple->m_index->m_table == new_tuple->m_index->m_table);

  for (ulint i = 0; i < n_fields; ++i) {
    auto new_dfield = dtuple_get_nth_field(new_tuple->m_ptr, i);
    auto old_dfield = dtuple_get_nth_field(old_tuple->m_ptr, i);

    auto mtype = dtype_get_mtype(dfield_get_type(old_dfield));
    auto prtype = dtype_get_prtype(dfield_get_type(old_dfield));

    /* Skip the system columns */
    if (mtype == DATA_SYS) {
      continue;

    } else if ((prtype & DATA_NOT_NULL) && dfield_is_null(new_dfield)) {

      err = DB_DATA_MISMATCH;
      break;
    }

    if (dfield_get_len(new_dfield) != dfield_get_len(old_dfield) ||
        (!dfield_is_null(old_dfield) &&
         memcmp(dfield_get_data(new_dfield), dfield_get_data(old_dfield), dfield_get_len(old_dfield)) != 0)) {

      auto upd_field = &upd->m_fields[n_changed];

      update_col(cursor, upd_field, i, new_dfield);

      ++n_changed;
    }
  }

  if (err == DB_SUCCESS) {
    upd->m_info_bits = 0;
    upd->m_n_fields = n_changed;
  }

  return err;
}

/**
 * Run the update query and do error handling.
 *
 * @param[in] thr in: Update query graph
 * @param[in] node in: Update node for the query
 * @param[in] savept in: savepoint to rollback to in case of an error
 *
 * @return DB_SUCCESS or error code
 */
static DbErr update_row_with_lock_retry(System &sys, QueThr *thr, UpdNode *node, TrxSavept *savept) noexcept {
  DbErr err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    (void)sys.m_row_upd->step(thr);

    err = trx->m_error_state;

    if (err != DB_SUCCESS) {
      que_thr_stop_client(sys, thr);

      if (err != DB_RECORD_NOT_FOUND) {
        thr->lock_state = QUE_THR_LOCK_ROW;

        lock_wait = row_handle_errors(sys, &err, trx, thr, savept);

        thr->lock_state = QUE_THR_LOCK_NOLOCK;
      } else {
        lock_wait = false;
      }
    } else {
      lock_wait = false;
    }
  } while (lock_wait);

  return err;
}

/**
 * Does an update or delete of a row.
 *
 * @param[in] cursor in: Cursor instance
 * @param[in] pcur in: Btree persistent cursor
 *
 * @return DB_SUCCESS or err code
 */
static DbErr execute_update_query_graph(System &sys, Cursor *cursor, BtreePcursor *pcur) noexcept {
  auto trx = cursor->m_prebuilt->m_trx;
  auto table = cursor->m_prebuilt->m_table;
  auto q_proc = &cursor->m_q_proc;

  /* The transaction must be running. */
  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = q_proc->m_node.m_upd;

  /* This is a short term solution to fix the purge lag. */
  delay_dml_if_needed(sys);

  ut_a(pcur->get_index()->is_clustered());
  node->m_pcur->copy_stored_position(pcur);

  ut_a(node->m_pcur->get_rel_pos() == BtreeCursorPos::ON);

  auto savept = trx_savept_take(trx);

  auto thr = que_fork_get_first_thr(q_proc->m_grph.m_upd);

  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  que_thr_move_to_run_state(thr);

  auto err = update_row_with_lock_retry(sys, thr, node, &savept);

  if (err == DB_SUCCESS) {

    que_thr_stop_for_client_no_error(thr, trx);

    if (node->m_is_delete) {

      if (table->m_stats.m_n_rows > 0) {
        --table->m_stats.m_n_rows;
      }

      ++sys.m_srv->m_stats.m_n_rows_deleted;
    } else {
      ++sys.m_srv->m_stats.m_n_rows_updated;
    }

    row_update_statistics_if_needed(sys, table);

  } else if (err == DB_RECORD_NOT_FOUND) {
    trx->m_error_state = DB_SUCCESS;
  }

  wake_master_thread(sys);

  trx->m_op_info = "";

  return err;
}

DbErr Cursor::update_row(System &sys, const Tuple *old_tuple, const Tuple *new_tuple) noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  BtreePcursor *pcur;

  if (prebuilt->m_index->is_clustered()) {
    pcur = prebuilt->m_pcur;
  } else if (prebuilt->m_need_to_access_clustered && prebuilt->m_clust_pcur != nullptr) {
    pcur = prebuilt->m_clust_pcur;
  } else {
    return DB_ERROR;
  }

  ut_a(old_tuple->m_type == TPL_ROW);
  ut_a(new_tuple->m_type == TPL_ROW);

  auto upd = update_vector_create(sys, cursor);
  auto err = calc_diff(cursor, upd, old_tuple, new_tuple);

  if (err == DB_SUCCESS) {
    /* Note that this is not a delete. */
    cursor->m_q_proc.m_node.m_upd->m_is_delete = false;

    err = execute_update_query_graph(sys, cursor, pcur);
  }

  return err;
}

/**
 * Build the update query graph to delete a row from an index.
 *
 * @param[in] cursor in: current cursor
 * @param[in] pcur in: Btree persistent cursor
 * @param[in] rec in: record to delete
 *
 * @return DB_SUCCESS or err code
 */
static DbErr delete_row_low(System &sys, Cursor *cursor, BtreePcursor *pcur, const Rec &rec) {
  auto table = cursor->m_prebuilt->m_table;
  auto index = table->get_first_index();

  auto n_cols = index->get_n_ordering_defined_by_user();
  auto tpl = key_tuple_new(index, n_cols);

  if (tpl == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto tuple = tpl;
  auto upd = update_vector_create(sys, cursor);

  read_tuple(sys, rec, tuple);

  upd->m_n_fields = tpl->get_n_cols();

  for (ulint i{}; i < upd->m_n_fields; ++i) {
    auto upd_field = &upd->m_fields[i];
    auto dfield = dtuple_get_nth_field(tuple->m_ptr, i);

    dfield_copy_data(&upd_field->m_new_val, dfield);

    upd_field->m_exp = nullptr;

    upd_field->m_orig_len = 0;

    upd->m_info_bits = 0;

    upd_field->m_field_no = index->get_clustered_field_pos(&table->m_cols[i]);
  }

  /* Note that this is a delete. */
  cursor->m_q_proc.m_node.m_upd->m_is_delete = true;

  auto err = execute_update_query_graph(sys, cursor, pcur);

  Tuple::destroy(tpl);

  return err;
}

DbErr Cursor::delete_row(System &sys) noexcept {
  auto cursor = this;
  DbErr err;
  BtreePcursor *pcur;
  auto prebuilt = cursor->m_prebuilt;

  auto index = prebuilt->m_index->m_table->get_first_index();

  /* Check whether this is a secondary index cursor */
  if (index != prebuilt->m_index) {
    if (prebuilt->m_need_to_access_clustered) {
      pcur = prebuilt->m_clust_pcur;
    } else {
      return DB_ERROR;
    }
  } else {
    pcur = prebuilt->m_pcur;
  }

  if (btr_cursor_is_positioned(pcur)) {
    Rec rec;

    if (!prebuilt->m_row_cache.is_cache_empty()) {
      rec = prebuilt->m_row_cache.cache_get_row();
      ut_a(!rec.is_null());
    } else {
      Mtr mtr;

      mtr.start();

      if (pcur->restore_position(sys, BTR_SEARCH_LEAF, &mtr, Current_location())) {

        rec = pcur->get_rec();
      } else {
        rec = Rec();
      }

      mtr.commit(sys);
    }

    if (rec.is_valid() && !rec.get_deleted_flag()) {
      err = delete_row_low(sys, cursor, pcur, Rec(rec));

      if (err == DB_SUCCESS) {
        /* The cached copy of the row was taken before the delete. */
        prebuilt->m_row_cache.clear();
      }
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    err = DB_RECORD_NOT_FOUND;
  }

  return err;
}

DbErr Cursor::read_row(System &sys, Tuple *tpl) noexcept {
  auto cursor = this;
  DbErr err;
  auto tuple = tpl;

  ut_a(cursor->m_prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  /* When searching with IB_EXACT_MATCH set, row_search_mvcc()
  will not position the persistent cursor but will copy the record
  found into the row cache. It should be the only entry. */
  if (!cursor->is_positioned() && cursor->m_prebuilt->m_row_cache.is_cache_empty()) {
    err = DB_RECORD_NOT_FOUND;
  } else if (!cursor->m_prebuilt->m_row_cache.is_cache_empty()) {
    auto rec = cursor->m_prebuilt->m_row_cache.cache_get_row();
    ut_a(!rec.is_null());

    /* The search put it here because the view sees it: a secondary record delete
    marked by a transaction the view doesn't see is visible, as of its older state. */
    read_tuple(sys, Rec(rec), tuple);
    err = DB_SUCCESS;
  } else {
    Mtr mtr;
    BtreePcursor *pcur;
    auto prebuilt = cursor->m_prebuilt;

    if (prebuilt->m_need_to_access_clustered && tuple->m_type == TPL_ROW) {
      pcur = prebuilt->m_clust_pcur;
    } else {
      pcur = prebuilt->m_pcur;
    }

    if (pcur == nullptr) {
      return DB_ERROR;
    }

    mtr.start();

    if (pcur->restore_position(sys, BTR_SEARCH_LEAF, &mtr, Current_location())) {
      auto rec = pcur->get_rec();

      if (!rec.get_deleted_flag()) {
        read_tuple(sys, Rec(rec), tuple);
        err = DB_SUCCESS;
      } else {
        err = DB_RECORD_NOT_FOUND;
      }
    } else {
      err = DB_RECORD_NOT_FOUND;
    }

    mtr.commit(sys);
  }

  return err;
}

DbErr Cursor::prev(System &sys) noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  prebuilt->m_row_cache.cache_next();

  return sys.m_row_sel->mvcc_fetch(sys.m_config.m_recv.m_force_recovery, IB_CUR_L, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_PREV);
}

DbErr Cursor::next(System &sys) noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  prebuilt->m_row_cache.cache_next();

  return sys.m_row_sel->mvcc_fetch(sys.m_config.m_recv.m_force_recovery, IB_CUR_G, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_NEXT);
}

/**
 * Move cursor to the first record in the table.
 *
 * @param[in] cursor in: InnoDB cursor instance
 * @param[in] mode in: Search mode
 *
 * @return DB_SUCCESS or err code
 */
static DbErr cursor_position(System &sys, Cursor *cursor, SearchMode mode) {
  auto prebuilt = cursor->m_prebuilt;

  /* We want to position at one of the ends, row_search_mvcc()
  uses the search_tuple fields to work out what to do. */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  return sys.m_row_sel->mvcc_fetch(sys.m_config.m_recv.m_force_recovery, mode, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_MOVETO);
}

DbErr Cursor::first(System &sys) noexcept {
  auto cursor = this;

  return cursor_position(sys, cursor, IB_CUR_G);
}

DbErr Cursor::last(System &sys) noexcept {
  auto cursor = this;

  return cursor_position(sys, cursor, IB_CUR_L);
}

DbErr Cursor::moveto(System &sys, Tuple *tpl, SearchMode srch_mode, int *result) noexcept {
  auto cursor = this;
  auto tuple = tpl;
  auto prebuilt = cursor->m_prebuilt;
  auto search_tuple = prebuilt->m_search_tuple;

  ut_a(tuple->m_type == TPL_KEY);

  auto n_fields = prebuilt->m_index->get_n_ordering_defined_by_user();

  dtuple_set_n_fields(search_tuple, n_fields);
  dtuple_set_n_fields_cmp(search_tuple, n_fields);

  /* Do a shallow copy */
  for (ulint i{}; i < n_fields; ++i) {
    dfield_copy(dtuple_get_nth_field(search_tuple, i), dtuple_get_nth_field(tuple->m_ptr, i));
  }

  ut_a(prebuilt->m_select_lock_type <= LOCK_NUM);

  auto err = sys.m_row_sel->mvcc_fetch(
    sys.m_config.m_recv.m_force_recovery, srch_mode, prebuilt, (IbMatch)cursor->m_match_mode, ROW_SEL_MOVETO
  );

  *result = prebuilt->m_result;

  return err;
}

void Cursor::attach_trx(System &sys, Trx *trx) noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  ut_a(trx != nullptr);
  ut_a(prebuilt->m_trx == nullptr);

  prebuilt->clear();
  prebuilt->update_trx(trx);

  /* Assign a read view if the transaction does not have it yet */
  auto rv = prebuilt->m_trx->assign_read_view(sys);
  ut_a(rv != nullptr);

  ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  ++prebuilt->m_trx->m_n_client_tables_in_use;
}

void Cursor::set_match_mode(MatchMode match_mode) noexcept {
  auto cursor = this;

  cursor->m_match_mode = match_mode;
}

/**
 * @brief Get the dfield instance for the column in the tuple.
 *
 * @param[in] tuple Tuple instance.
 * @param[in] col_no Column number in the tuple.
 *
 * @return Dfield* dfield instance in the tuple.
 */
const Dfield *col_get_dfield(const Tuple *tuple, ulint col_no) noexcept {
  return dtuple_get_nth_field(tuple->m_ptr, col_no);
}

/** @copydoc col_get_dfield(const Tuple *, ulint) */
Dfield *col_get_dfield(Tuple *tuple, ulint col_no) noexcept {
  return dtuple_get_nth_field(tuple->m_ptr, col_no);
}

/**
 * @brief Predicate to check whether a column type contains variable length data.
 *
 * @param[in] dtype Column type.
 *
 * @return true if the column type contains variable length data, false otherwise.
 */
static bool col_is_capped(const Dtype *dtype) noexcept {
  return dtype_get_len(dtype) > 0 && (dtype_get_mtype(dtype) == DATA_VARCHAR || dtype_get_mtype(dtype) == DATA_CHAR ||
                                      dtype_get_mtype(dtype) == DATA_CLIENT || dtype_get_mtype(dtype) == DATA_VARCLIENT ||
                                      dtype_get_mtype(dtype) == DATA_FIXBINARY || dtype_get_mtype(dtype) == DATA_BINARY);
}

DbErr Tuple::set_value(ulint col_no, const void *s, ulint len) noexcept {
  auto tpl = this;
  auto src = static_cast<const byte *>(s);
  auto tuple = tpl;

  ut_d(mem_heap_verify(tuple->m_heap));

  auto dfield = col_get_dfield(tuple, col_no);

  /* User wants to set the column to nullptr. */
  if (len == IB_SQL_NULL) {
    dfield_set_null(dfield);
    return DB_SUCCESS;
  }

  const auto dtype = dfield_get_type(dfield);

  /* Not allowed to update system columns. */
  if (dtype_get_mtype(dtype) == DATA_SYS) {
    return DB_DATA_MISMATCH;
  }

  auto dst = static_cast<byte *>(dfield_get_data(dfield));

  /* Since TEXT/CLOB also map to DATA_VARCHAR we need to make an
  exception. Perhaps we need to set the precise type and check
  for that. */
  if (col_is_capped(dtype)) {

    len = std::min<ulint>(len, dtype_get_len(dtype));

    if (dst == nullptr) {
      dst = mem_heap_alloc(tuple->m_heap, dtype_get_len(dtype));
      ut_a(dst != nullptr);
    }
  } else if (dst == nullptr || len > dfield_get_len(dfield)) {
    dst = mem_heap_alloc(tuple->m_heap, len);
  }

  if (dst == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  switch (dtype_get_mtype(dtype)) {
    case DATA_INT: {

      if (dtype_get_len(dtype) == len) {
        bool usign;

        usign = dtype_get_prtype(dtype) & DATA_UNSIGNED;
        mach_write_int_type(dst, src, len, usign);
      } else {
        return DB_DATA_MISMATCH;
      }
      break;
    }

    case DATA_FLOAT:
      if (len == sizeof(float)) {
        mach_float_ptr_write(dst, src);
      } else {
        return DB_DATA_MISMATCH;
      }
      break;

    case DATA_DOUBLE:
      if (len == sizeof(double)) {
        mach_double_ptr_write(dst, src);
      } else {
        return DB_DATA_MISMATCH;
      }
      break;

    case DATA_SYS:
      ut_error;
      break;

    case DATA_CHAR: {
      ulint pad_char = ULINT_UNDEFINED;

      pad_char = dtype_get_pad_char(dtype_get_mtype(dtype), dtype_get_prtype(dtype));

      ut_a(pad_char != ULINT_UNDEFINED);

      memcpy(dst, src, len);
      memset((byte *)dst + len, pad_char, dtype_get_len(dtype) - len);

      len = dtype_get_len(dtype);
      break;
    }
    case DATA_BLOB:
    case DATA_BINARY:
    case DATA_CLIENT:
    case DATA_DECIMAL:
    case DATA_VARCHAR:
    case DATA_VARCLIENT:
    case DATA_FIXBINARY:
      memcpy(dst, src, len);
      break;

    default:
      ut_error;
  }

  if (dst != dfield_get_data(dfield)) {
    dfield_set_data(dfield, dst, len);
  } else {
    dfield_set_len(dfield, len);
  }

#ifdef UNIV_DEBUG
  mem_heap_verify(tuple->m_heap);
#endif

  return DB_SUCCESS;
}

ulint Tuple::get_len(ulint i) const noexcept {
  auto tpl = this;
  auto tuple = tpl;

  auto dfield = col_get_dfield(tuple, i);
  auto data_len = dfield_get_len(dfield);

  return data_len == UNIV_SQL_NULL ? IB_SQL_NULL : data_len;
}

/**
 * Copy a column value from the tuple.
 *
 * @param[in] tpl Tuple instance.
 * @param[in] i Column index in tuple.
 * @param[out] dst Copied data value.
 * @param[in] len Max data value len to copy.
 *
 * @return bytes copied or IB_SQL_NULL
 */
ulint col_copy_value_low(const Tuple *tpl, ulint i, void *dst, ulint len) noexcept {
  auto tuple = tpl;

  auto dfield = col_get_dfield(tuple, i);
  auto data = static_cast<const byte *>(dfield_get_data(dfield));
  auto data_len = dfield_get_len(dfield);

  if (data_len != UNIV_SQL_NULL) {

    const auto dtype = dfield_get_type(dfield);

    switch (dtype_get_mtype(dfield_get_type(dfield))) {
      case DATA_INT: {
        bool usign;

        ut_a(data_len == len);

        usign = dtype_get_prtype(dtype) & DATA_UNSIGNED;
        mach_read_int_type(dst, data, data_len, usign);
        break;
      }
      case DATA_FLOAT:
        if (len == data_len) {
          float f;

          ut_a(data_len == sizeof(f));
          f = mach_float_read(data);
          memcpy(dst, &f, sizeof(f));
        } else {
          data_len = 0;
        }
        break;
      case DATA_DOUBLE:
        if (len == data_len) {
          double d;

          ut_a(data_len == sizeof(d));
          d = mach_double_read(data);
          memcpy(dst, &d, sizeof(d));
        } else {
          data_len = 0;
        }
        break;
      default:
        data_len = std::min<ulint>(data_len, len);
        memcpy(dst, data, data_len);
    }
  } else {
    data_len = IB_SQL_NULL;
  }

  return data_len;
}

ulint Tuple::copy_value(ulint i, void *dst, ulint len) const noexcept {
  auto tpl = this;
  return col_copy_value_low(tpl, i, dst, len);
}

/**
 * Get the InnoDB column attribute from the internal column precise type.
 *
 * @param[in] prtype Column definition.
 *
 * @return Precise type in API format.
 */
ColAttr col_get_attr(ulint prtype) {
  ColAttr col_attr = IB_COL_NONE;
  auto attr = to_int(col_attr);

  if (prtype & DATA_UNSIGNED) {
    attr |= IB_COL_UNSIGNED;
  }

  if (prtype & DATA_NOT_NULL) {
    attr |= IB_COL_NOT_NULL;
  }

  if (prtype & DATA_CUSTOM_TYPE) {
    attr |= IB_COL_CUSTOM1;
  }

  if (prtype & (DATA_CUSTOM_TYPE << 1)) {
    attr |= IB_COL_CUSTOM2;
  }

  if (prtype & (DATA_CUSTOM_TYPE << 2)) {
    attr |= IB_COL_CUSTOM3;
  }

  return static_cast<ColAttr>(attr);
}

/**
 * Get a column type, length and attributes from the tuple.
 *
 * @param[in] tpl Tuple instance.
 * @param[in] i Column index in tuple.
 * @param[out] col_meta Column meta data.
 *
 * @return Length of column data.
 */
ulint col_get_meta_low(const Tuple *tpl, ulint i, ColMeta *col_meta) noexcept {
  auto tuple = tpl;

  auto dfield = col_get_dfield(tuple, i);
  auto data_len = dfield_get_len(dfield);

  /* We assume 1-1 mapping between the ENUM and internal type codes. */
  col_meta->type = static_cast<ColType>(dtype_get_mtype(dfield_get_type(dfield)));

  col_meta->type_len = dtype_get_len(dfield_get_type(dfield));

  auto prtype = (uint16_t)dtype_get_prtype(dfield_get_type(dfield));

  col_meta->attr = col_get_attr(prtype);
  col_meta->client_type = prtype & DATA_CLIENT_TYPE_MASK;

  return data_len;
}

/**
 * Read a signed int 8 bit column from an InnoDB tuple.
 *
 * @param[in] tpl Tuple instance.
 * @param[in] i Column number.
 * @param[in] usign True if unsigned.
 * @param[in] size Size of integer.
 *
 * @return DB_SUCCESS or error.
 */
static DbErr tuple_check_int(const Tuple *tpl, ulint i, bool usign, ulint size) noexcept {
  ColMeta col_meta;

  col_get_meta_low(tpl, i, &col_meta);

  if (col_meta.type != IB_INT) {
    return DB_DATA_MISMATCH;
  } else if (col_meta.type_len == IB_SQL_NULL) {
    return DB_UNDERFLOW;
  } else if (col_meta.type_len != size) {
    return DB_DATA_MISMATCH;
  } else if ((col_meta.attr & IB_COL_UNSIGNED) && !usign) {
    return DB_DATA_MISMATCH;
  }

  return DB_SUCCESS;
}

DbErr Tuple::read_i8(ulint i, int8_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u8(ulint i, uint8_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i16(ulint i, int16_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u16(ulint i, uint16_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i32(ulint i, int32_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u32(ulint i, uint32_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i64(ulint i, int64_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u64(ulint i, uint64_t *ival) const noexcept {
  auto tpl = this;

  const auto err = tuple_check_int(tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    col_copy_value_low(tpl, i, ival, sizeof(*ival));
  }

  return err;
}

const void *Tuple::get_value(ulint i) const noexcept {
  auto tpl = this;
  auto tuple = tpl;
  auto dfield = col_get_dfield(tuple, i);
  auto data = dfield_get_data(dfield);
  auto data_len = dfield_get_len(dfield);

  return data_len != UNIV_SQL_NULL ? data : nullptr;
}

ulint Tuple::get_meta(ulint i, ColMeta *col_meta) const noexcept {
  auto tpl = this;
  return col_get_meta_low(tpl, i, col_meta);
}

Tuple *Tuple::clear() noexcept {
  auto tpl = this;
  auto tuple = tpl;
  const auto type = tuple->m_type;
  auto heap = tuple->m_heap;

  const auto index = tuple->m_index;
  const auto n_cols = dtuple_get_n_fields(tuple->m_ptr);

  mem_heap_empty(heap);

  if (type == TPL_ROW) {
    return row_tuple_new_low(index, n_cols, heap);
  } else {
    return key_tuple_new_low(index, n_cols, heap);
  }
}

DbErr Cursor::get_cluster_key(Tuple **dst_tpl, const Tuple *src_tpl) noexcept {
  auto cursor = this;
  auto src_tuple = src_tpl;

  auto clust_index = cursor->m_prebuilt->m_table->get_clustered_index();

  /* We need to ensure that the src tuple belongs to the same table
  as the open cursor and that it's not a tuple for a cluster index. */
  if (src_tuple->m_type != TPL_KEY) {
    return DB_ERROR;
  } else if (src_tuple->m_index->m_table != cursor->m_prebuilt->m_table) {
    return DB_DATA_MISMATCH;
  } else if (src_tuple->m_index == clust_index) {
    return DB_ERROR;
  }

  /* Create the cluster index key search tuple. */
  *dst_tpl = cursor->clust_search_tuple_create();

  if (!*dst_tpl) {
    return DB_OUT_OF_MEMORY;
  }

  auto dst_tuple = *dst_tpl;
  ut_a(dst_tuple->m_index == clust_index);

  const auto n_fields = dst_tuple->m_index->get_n_unique();

  /* Do a deep copy of the data fields. */
  for (ulint i{}; i < n_fields; ++i) {
    auto pos = src_tuple->m_index->get_nth_field_pos(dst_tuple->m_index, i);

    ut_a(pos != ULINT_UNDEFINED);

    auto src_field = dtuple_get_nth_field(src_tuple->m_ptr, pos);
    auto dst_field = dtuple_get_nth_field(dst_tuple->m_ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data = mem_heap_dup(dst_tuple->m_heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return DB_SUCCESS;
}

DbErr Tuple::copy(const Tuple *src_tuple) noexcept {
  auto dst_tuple = this;

  /* Make sure src and dst are not the same. */
  ut_a(src_tuple != dst_tuple);

  /* Make sure they are the same type and refer to the same index. */
  if (src_tuple->m_type != dst_tuple->m_type || src_tuple->m_index != dst_tuple->m_index) {

    return DB_DATA_MISMATCH;
  }

  auto n_fields = dtuple_get_n_fields(src_tuple->m_ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_tuple->m_ptr));

  /* Do a deep copy of the data fields. */
  for (ulint i = 0; i < n_fields; ++i) {
    auto src_field = dtuple_get_nth_field(src_tuple->m_ptr, i);
    auto dst_field = dtuple_get_nth_field(dst_tuple->m_ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data = mem_heap_dup(dst_tuple->m_heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return DB_SUCCESS;
}

Tuple *Cursor::sec_search_tuple_create() noexcept {
  auto cursor = this;
  auto index = cursor->m_prebuilt->m_index;

  const auto n_cols = index->get_n_unique_in_tree();
  return key_tuple_new(index, n_cols);
}

Tuple *Cursor::sec_read_tuple_create() noexcept {
  auto cursor = this;
  auto index = cursor->m_prebuilt->m_index;
  /* Not the DB_TRX_ID: it is not a column of the index to the user. */
  const auto n_cols = index->get_n_fields() - 1;

  return row_tuple_new(index, n_cols);
}

Tuple *Cursor::clust_search_tuple_create() noexcept {
  auto cursor = this;
  auto index = cursor->m_prebuilt->m_table->get_clustered_index();
  const auto n_cols = index->get_n_ordering_defined_by_user();

  return key_tuple_new(index, n_cols);
}

Tuple *Cursor::clust_read_tuple_create() noexcept {
  auto cursor = this;

  auto index = cursor->m_prebuilt->m_table->get_clustered_index();

  const auto n_cols = cursor->m_prebuilt->m_table->get_n_cols();
  return row_tuple_new(index, n_cols);
}

ulint Tuple::get_n_user_cols() const noexcept {
  auto tpl = this;
  const auto tuple = tpl;

  if (tuple->m_type == TPL_ROW) {
    return tuple->m_index->m_table->get_n_user_cols();
  } else {
    return tuple->m_index->get_n_ordering_defined_by_user();
  }
}

ulint Tuple::get_n_cols() const noexcept {
  auto tpl = this;
  const auto tuple = tpl;

  return dtuple_get_n_fields(tuple->m_ptr);
}

void Tuple::destroy(Tuple *tuple) noexcept {

  mem_heap_free(tuple->m_heap);
}

}  // namespace api
