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

/* Truncate, ids, schema locks, savepoints, schema visitors, parallel reads, status. */

#include "api0int.h"

namespace api {

DbErr cursor_truncate(System &sys, Cursor **crsr, uint64_t *table_id) noexcept {
  auto cursor = *reinterpret_cast<Cursor **>(crsr);
  auto prebuilt = cursor->m_prebuilt;

  ut_a(schema_lock_is_exclusive(prebuilt->m_trx));

  *table_id = 0;

  auto err = (*crsr)->lock(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    Trx *trx;
    Table *table = prebuilt->m_table;

    /* We are going to free the cursor and the prebuilt. Store
    the transaction handle locally. */
    trx = prebuilt->m_trx;
    err = Cursor::close(sys, *crsr);
    ut_a(err == DB_SUCCESS);

    *crsr = nullptr;

    /* This function currently commits the transaction
    on success. */
    err = sys.m_dict->m_ddl.truncate_table(table, trx);

    if (err == DB_SUCCESS) {
      *table_id = table->m_id;
    }
  }

  return err;
}

DbErr table_truncate(System &sys, const char *table_name, uint64_t *table_id) noexcept {

  auto trx = trx_begin(sys, IB_TRX_SERIALIZABLE);

  sys.m_dict->mutex_acquire();

  auto table = sys.m_dict->table_get(table_name);

  DbErr err;
  Cursor *crsr{};

  if (table != nullptr && table->get_first_index()) {
    sys.m_dict->table_increment_handle_count(table, true);
    err = create_cursor(sys, &crsr, table, 0, trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  sys.m_dict->mutex_release();

  if (err == DB_SUCCESS) {
    err = schema_lock_exclusive(sys, trx);
  }

  DbErr trunc_err;

  if (err == DB_SUCCESS) {
    trunc_err = cursor_truncate(sys, &crsr, table_id);
    ut_a(err == DB_SUCCESS);
  } else {
    trunc_err = err;
  }

  if (crsr != nullptr) {
    err = Cursor::close(sys, crsr);
    ut_a(err == DB_SUCCESS);
  }

  if (trunc_err == DB_SUCCESS) {
    ut_a(trx_state(trx) == IB_TRX_NOT_STARTED);

    err = schema_unlock(sys, trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

    err = trx_release(sys, trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = trx_rollback(sys, trx);
    ut_a(err == DB_SUCCESS);
  }

  return trunc_err;
}

DbErr table_get_id(System &sys, const char *table_name, uint64_t *table_id) noexcept {

  sys.m_dict->mutex_acquire();

  auto err = table_get_id_low(sys, table_name, table_id);

  sys.m_dict->mutex_release();

  return err;
}

DbErr index_get_id(System &sys, const char *table_name, const char *index_name, uint64_t *index_id) noexcept {
  DbErr err = DB_TABLE_NOT_FOUND;

  *index_id = 0;

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  normalize_table_name(normalized_name, table_name);

  auto table = lookup_table_by_name(sys, normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table != nullptr) {
    auto index = table->get_index_on_name(index_name);

    if (index != nullptr) {
      /* We only support 32 bit table and index ids. Because
      we need to pack the table id into the index id. */
      ut_a((table->m_id >> 32) == 0);
      ut_a((index->m_id >> 32) == 0);

      *index_id = table->m_id;
      *index_id <<= 32;
      *index_id |= index->m_id;

      err = DB_SUCCESS;
    }
  }

  return err;
}

bool database_create(System &sys, const char *dbname) noexcept {
  const char *ptr;

  for (ptr = dbname; *ptr; ++ptr) {
    if (*ptr == SRV_PATH_SEPARATOR) {
      return false;
    }
  }

  /* Only necessary if file per table is set. */
  if (sys.m_config.m_fil.m_file_per_table) {
    return sys.m_fil->mkdir(dbname);
  }

  return true;
}

DbErr database_drop(System &sys, const char *dbname) noexcept {
  Trx *trx;
  ulint len = strlen(dbname);

  if (len == 0) {
    return DB_INVALID_INPUT;
  }

  auto ptr = reinterpret_cast<char *>(mem_alloc(len + 2));

  memset(ptr, 0x0, len + 2);
  strcpy(ptr, dbname);

  trx = trx_begin(sys, IB_TRX_SERIALIZABLE);

  /* Drop all the tables in the database first. */
  /* ddl_drop_database() expects a string that ends in '/'. */
  if (ptr[len - 1] != '/') {
    ptr[len] = '/';
  }

  auto err = sys.m_dict->m_ddl.drop_database(ptr, trx);

  /* Only necessary if file per table is set. */
  if (err == DB_SUCCESS && sys.m_config.m_fil.m_file_per_table) {
    sys.m_fil->rmdir(ptr);
  }

  mem_free(ptr);

  if (err == DB_SUCCESS) {
    auto trx_err = trx_commit(sys, trx);
    ut_a(trx_err == DB_SUCCESS);
  } else {
    auto trx_err = trx_rollback(sys, trx);
    ut_a(trx_err == DB_SUCCESS);
  }

  return err;
}

bool Cursor::is_positioned() const noexcept {
  auto cursor = this;
  const Prebuilt *prebuilt = cursor->m_prebuilt;

  return prebuilt->m_pcur->has_saved();
}

DbErr schema_lock_shared(System &sys, Trx *trx) noexcept {
  DbErr err = DB_SUCCESS;

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_S_LATCH) {

    sys.m_dict->freeze_data_dictionary(trx);
  }

  return err;
}

DbErr schema_lock_exclusive(System &sys, Trx *trx) noexcept {
  DbErr err = DB_SUCCESS;

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH) {

    sys.m_dict->lock_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

bool schema_lock_is_exclusive(const Trx *trx) noexcept {

  return trx->m_dict_operation_lock_mode == RW_X_LATCH;
}

bool schema_lock_is_shared(const Trx *trx) noexcept {

  return trx->m_dict_operation_lock_mode == RW_S_LATCH;
}

DbErr schema_unlock(System &sys, Trx *trx) noexcept {
  DbErr err = DB_SUCCESS;

  if (trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    sys.m_dict->unlock_data_dictionary(trx);
  } else if (trx->m_dict_operation_lock_mode == RW_S_LATCH) {
    sys.m_dict->unfreeze_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

DbErr Cursor::lock(System &sys, CursorLockMode lck_mode) noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;
  auto trx = prebuilt->m_trx;
  auto table = prebuilt->m_table;

  return row_lock_table_with_retry(sys, trx, table, LockMode(lck_mode));
}

DbErr table_lock(System &sys, Trx *trx, uint64_t table_id, CursorLockMode lck_mode) noexcept {

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto table = open_table_by_id(sys, table_id, false);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  ut_a(lck_mode <= (CursorLockMode)LOCK_NUM);

  auto heap = mem_heap_create(128);

  QueryProc q_proc;

  q_proc.m_node.m_sel = SelNode::create(heap);

  auto thr = pars_complete_graph_for_exec(q_proc.m_node.m_sel, trx, heap);

  q_proc.m_grph.m_sel = static_cast<QueFork *>(que_node_get_parent(thr));
  q_proc.m_grph.m_sel->state = QUE_FORK_ACTIVE;

  trx->m_op_info = "setting table lock";

  ut_a(lck_mode == IB_LOCK_IS || lck_mode == IB_LOCK_IX);

  auto err = sys.m_lock_sys->lock_table(0, table, (enum LockMode)lck_mode, thr);

  trx->m_error_state = err;

  sys.m_dict->table_decrement_handle_count(table, false);

  mem_heap_free(heap);

  return err;
}

DbErr Cursor::set_lock_mode(System &sys, CursorLockMode lck_mode) noexcept {
  auto cursor = this;
  DbErr err = DB_SUCCESS;
  auto prebuilt = cursor->m_prebuilt;

  ut_a(lck_mode <= (CursorLockMode)LOCK_NUM);

  if (lck_mode == IB_LOCK_X) {
    err = cursor->lock(sys, IB_LOCK_IX);
  } else {
    err = cursor->lock(sys, IB_LOCK_IS);
  }

  if (err == DB_SUCCESS) {
    prebuilt->m_select_lock_type = (enum LockMode)lck_mode;
    ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);
  }

  return err;
}

void Cursor::set_cluster_access() noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  prebuilt->m_need_to_access_clustered = true;
}

void Cursor::set_simple_select() noexcept {
  auto cursor = this;
  auto prebuilt = cursor->m_prebuilt;

  prebuilt->m_simple_select = true;
}

void savepoint_take(Trx *trx, const void *name, ulint name_len) noexcept {

  ut_a(trx);
  ut_a(name != nullptr);
  ut_a(name_len > 0);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto it = std::find_if(trx->m_trx_savepoints.begin(), trx->m_trx_savepoints.end(), [name, name_len](const auto &savep) {
    return name_len == savep->name_len && 0 == memcmp(savep->name, name, name_len);
  });

  if (it != trx->m_trx_savepoints.end()) {
    auto savep = *it;
    /* There is a savepoint with the same name: free that */
    trx->m_trx_savepoints.remove(savep);
    mem_free(savep);
  }

  /* Create a new savepoint and add it as the last in the list */
  auto savep = static_cast<TrxNamedSavept *>(mem_alloc(sizeof(TrxNamedSavept) + name_len));

  savep->name = savep + 1;
  savep->savept = trx_savept_take(trx);

  savep->name_len = name_len;
  memcpy(savep->name, name, name_len);

  trx->m_trx_savepoints.push_back(savep);
}

DbErr savepoint_release(Trx *trx, const void *name, ulint name_len) noexcept {

  auto it = std::find_if(trx->m_trx_savepoints.begin(), trx->m_trx_savepoints.end(), [name, name_len](const auto &savep) {
    return name_len == savep->name_len && memcmp(savep->name, name, name_len) == 0;
  });

  if (it != trx->m_trx_savepoints.end()) {
    auto savep = *it;
    trx->m_trx_savepoints.remove(savep);
    mem_free(savep);
    return DB_SUCCESS;
  } else {
    return DB_NO_SAVEPOINT;
  }
}

DbErr savepoint_rollback(System &sys, Trx *trx, const void *name, ulint name_len) noexcept {

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    ut_print_timestamp(stderr);
    ib_logger(
      stderr,
      "  Error: transaction trying to rollback a  "
      "savepoint "
    );
    ut_print_name(name != nullptr ? (char *)name : "(null)");
    ib_logger(stderr, " though it is not started\n");

    return DB_ERROR;
  }

  /* Without a name, the first savepoint; else the one with that name. */
  auto it = std::find_if(trx->m_trx_savepoints.begin(), trx->m_trx_savepoints.end(), [&](const TrxNamedSavept *savep) {
    return name == nullptr || name_len == 0 || (savep->name_len == name_len && memcmp(savep->name, name, name_len) == 0);
  });

  if (it == trx->m_trx_savepoints.end()) {
    return DB_NO_SAVEPOINT;
  }

  auto savep = *it;

  /* We can now free all savepoints strictly later than this one; it stays,
  a later rollback can return to it again. */
  trx_roll_savepoints_free(trx, savep);

  trx->m_op_info = "rollback to a savepoint";

  auto err = trx_general_rollback(sys, trx, true, &savep->savept);

  /* Store the current undo_no of the transaction so that we know where
  to roll back if we have to roll back the next SQL statement: */
  trx->mark_sql_stat_end();

  trx->m_op_info = "";

  return err;
}

/**
 * @brief Convert from internal format to the the table definition table attributes
 *
 * @param[in] table - in: table definition
 * @param[out] tbl_fmt - out: table format
 */
static void table_get_format(const Table *table, TableFormat *tbl_fmt) noexcept {
  if (table->m_flags == DICT_TF_FORMAT_V1) {
    *tbl_fmt = IB_TBL_V1;
  } else {
    *tbl_fmt = IB_TBL_UNKNOWN;
  }
}

/**
 * @brief Call the visitor for each column in a table.
 *
 * @param[in] table - in: table to visit
 * @param[in] table_col - in: table column visitor
 * @param[in] arg - in: argument to visitor
 *
 * @return return value from index_col
 */
static int table_schema_visit_table_columns(const Table *table, SchemaVisitor::table_col_t table_col, void *arg) {
  for (ulint i{}; i < table->get_n_cols(); ++i) {
    auto col = table->get_nth_col(i);
    auto col_no = col->get_no();
    auto name = table->get_col_name(col_no);
    auto attr = col_get_attr(col->prtype);
    auto user_err = table_col(arg, name, (ColType)col->mtype, col->len, attr);

    if (user_err) {
      return user_err;
    }
  }

  return 0;
}

/**
 * @brief Call the visitor for each column in an index.
 *
 * @param[in] index - in: index to visit
 * @param[in] index_col - in: index column visitor
 * @param[in] arg - in: argument to visitor
 *
 * @return return value from index_col
 */
static int table_schema_visit_index_columns(const Index *index, SchemaVisitor::index_col_t index_col, void *arg) noexcept {
  auto n_index_cols = index->m_n_user_defined_cols;

  for (ulint i{}; i < n_index_cols; ++i) {
    auto dfield = &index->m_fields[i];
    auto user_err = index_col(arg, dfield->m_name, dfield->m_prefix_len);

    if (user_err) {
      return user_err;
    }
  }

  return 0;
}

DbErr table_schema_visit(System &sys, Trx *trx, const char *name, const SchemaVisitor *visitor, void *arg) noexcept {
  int user_err{};

  if (!schema_lock_is_exclusive(trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  normalize_table_name(normalized_name, name);

  auto table = lookup_table_by_name(sys, normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table != nullptr) {
    sys.m_dict->table_increment_handle_count(table, true);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  TableFormat tbl_fmt;

  table_get_format(table, &tbl_fmt);

  /* We need the count of user defined indexes only. */
  auto n_indexes = table->m_indexes.size();

  /* The first index is always the cluster index. */
  auto index = table->get_first_index();

  /* Only the clustered index can be auto generated. */
  if (index->m_n_user_defined_cols == 0) {
    --n_indexes;
  }

  if (visitor->version < SchemaVisitor::Version::TABLE) {

    goto func_exit;

  } else if (visitor->table) {
    user_err = visitor->table(arg, table->m_name, tbl_fmt, table->m_n_cols, n_indexes);

    if (user_err > 0) {
      goto func_exit;
    }
  }

  if (visitor->version < SchemaVisitor::Version::TABLE_COL) {

    goto func_exit;

  } else if (visitor->table_col) {
    user_err = table_schema_visit_table_columns(table, visitor->table_col, arg);

    if (user_err > 0) {
      goto func_exit;
    }
  }

  if (!visitor->index) {
    goto func_exit;
  } else if (visitor->version < SchemaVisitor::Version::TABLE_AND_INDEX) {
    goto func_exit;
  }

  /* Traverse the user defined indexes. */
  do {
    auto n_index_cols = index->m_n_user_defined_cols;

    /* Ignore system generated indexes. */
    if (n_index_cols > 0) {
      user_err = visitor->index(arg, index->m_name, index->is_clustered(), index->is_unique(), n_index_cols);
      if (user_err > 0) {
        goto func_exit;
      }

      if (visitor->version >= SchemaVisitor::Version::TABLE_AND_INDEX_COL && visitor->index_col) {
        user_err = table_schema_visit_index_columns(index, visitor->index_col, arg);

        if (user_err > 0) {
          break;
        }
      }
    }

    index = index->get_next();

  } while (index != nullptr);

func_exit:
  ut_a(schema_lock_is_exclusive(trx));
  sys.m_dict->table_decrement_handle_count(table, true);

  return user_err != 0 ? DB_ERROR : DB_SUCCESS;
}

DbErr schema_tables_iterate(System &sys, Trx *trx, SchemaVisitorTableAll visitor, void *arg) noexcept {
  DbErr err;
  Cursor *crsr;
  DbErr crsr_err;
  Tuple *tpl{};

  if (!schema_lock_is_exclusive(trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = lookup_table_by_name(sys, "SYS_TABLES");

  if (table != nullptr) {
    sys.m_dict->table_increment_handle_count(table, true);
    err = create_cursor(sys, &crsr, table, 0, trx);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  if (err == DB_SUCCESS) {
    err = crsr->first(sys);
  }

  tpl = crsr->clust_read_tuple_create();

  while (err == DB_SUCCESS) {
    const void *ptr;
    ColMeta col_meta;

    err = crsr->read_row(sys, tpl);

    if (err == DB_SUCCESS) {
      ulint len;

      ptr = tpl->get_value(0);
      /* Can't have nullptr columns. */
      ut_a(ptr != nullptr);

      len = col_get_meta_low(tpl, 0, &col_meta);
      ut_a(len != UNIV_SQL_NULL);

      if (visitor(arg, (const char *)ptr, len)) {
        break;
      }

      err = crsr->next(sys);
    }
  }

  Tuple::destroy(tpl);

  crsr_err = Cursor::close(sys, crsr);
  ut_a(crsr_err == DB_SUCCESS);

  if (err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }

  return err;
}

DbErr Tuple::write_i8(int col_no, int8_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_i16(int col_no, int16_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_i32(int col_no, int32_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_i64(int col_no, int64_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_u8(int col_no, uint8_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_u16(int col_no, uint16_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_u32(int col_no, uint32_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

DbErr Tuple::write_u64(int col_no, uint64_t val) noexcept {
  return set_value(col_no, &val, sizeof(val));
}

void Cursor::stmt_begin() noexcept {
  auto cursor = this;

  cursor->m_prebuilt->m_sql_stat_start = true;
}

DbErr Tuple::write_double(int col_no, double val) noexcept {
  auto tpl = this;
  auto tuple = tpl;

  const auto dfield = col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    return set_value(col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

DbErr Tuple::read_double(ulint col_no, double *dval) const noexcept {
  auto tpl = this;
  DbErr err;
  auto tuple = tpl;
  const auto dfield = col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    col_copy_value_low(tpl, col_no, dval, sizeof(*dval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

DbErr Tuple::write_float(int col_no, float val) noexcept {
  auto tpl = this;
  auto tuple = tpl;
  const auto dfield = col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    return set_value(col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

DbErr Tuple::read_float(ulint col_no, float *fval) const noexcept {
  auto tpl = this;
  DbErr err;
  auto tuple = tpl;

  const auto dfield = col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    col_copy_value_low(tpl, col_no, fval, sizeof(*fval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

DbErr get_duplicate_key(Trx *trx, const char **table_name, const char **index_name) noexcept {

  if (trx->m_error_info == nullptr) {
    return DB_ERROR;
  }

  *table_name = trx->m_error_info->get_table_name();
  *index_name = trx->m_error_info->m_name;

  return DB_SUCCESS;
}

DbErr Cursor::get_table_statistics(System &sys, TableStats *table_stats) noexcept {
  auto cursor = this;
  auto table = cursor->m_prebuilt->m_table;

  /* The cursor may hold no table lock, serialize with TRUNCATE. */
  sys.m_dict->mutex_acquire();

  if (!table->m_stats.m_initialized) {
    sys.m_dict->update_statistics(table);
  }

  sys.m_dict->mutex_release();

  table_stats->m_stat_n_rows = table->m_stats.m_n_rows;
  table_stats->m_stat_clustered_index_size = table->m_stats.m_clustered_index_size * UNIV_PAGE_SIZE;
  table_stats->m_stat_sum_of_other_index_sizes = table->m_stats.m_sum_of_secondary_index_sizes * UNIV_PAGE_SIZE;
  table_stats->m_stat_modified_counter = table->m_stats.m_modified_counter;

  return DB_SUCCESS;
}

DbErr Cursor::get_index_stat_n_diff_key_vals(System &sys, const char *index_name, uint64_t *ncols, int64_t **n_diff) noexcept {
  auto cursor = this;
  auto table = cursor->m_prebuilt->m_table;

  /* The cursor may hold no table lock, serialize with TRUNCATE. */
  sys.m_dict->mutex_acquire();

  if (!table->m_stats.m_initialized) {
    sys.m_dict->update_statistics(table);
  }

  sys.m_dict->mutex_release();

  auto index = table->get_index_on_name(index_name);

  if (index == nullptr) {
    return DB_NOT_FOUND;
  }

  *ncols = index->m_n_uniq;

  *n_diff = (int64_t *)malloc(sizeof(int64_t) * index->m_n_uniq);

  sys.m_dict->index_stat_mutex_enter(index);

  /* Indexed 1 .. n_uniq: [i] is the number of distinct prefixes of i fields. */
  memcpy(*n_diff, index->m_stats.m_n_diff_key_vals + 1, sizeof(int64_t) * index->m_n_uniq);

  sys.m_dict->index_stat_mutex_exit(index);

  return DB_SUCCESS;
}

DbErr Cursor::update_table_statistics(System &sys) noexcept {
  auto cursor = this;
  auto table = cursor->m_prebuilt->m_table;

  /* The cursor may hold no table lock, serialize with TRUNCATE. */
  sys.m_dict->mutex_acquire();

  sys.m_dict->update_statistics(table);

  sys.m_dict->mutex_release();

  return DB_SUCCESS;
}

DbErr parallel_select_count_star(
  System &sys, Trx *trx, std::vector<Cursor *> &crsrs, size_t n_threads, uint64_t &n_rows
) noexcept {
  ut_a(n_threads > 1);
  ut_a(!crsrs.empty());

  ut::Sharded_counter<ParallelReader::MAX_THREADS> n_recs{};

  n_recs.clear();

  const ParallelReader::ScanRange full_scan;

  DbErr err{DB_SUCCESS};

  /* The reader gives back max_threads() threads: it must be built for the
  threads reserved. */
  n_threads = ParallelReader::available_threads(sys, n_threads, false);

  ParallelReader reader(n_threads);
  std::vector<Table *> tables{};

  for (auto cursor : crsrs) {
    ParallelReader::Config config(full_scan, cursor->m_prebuilt->m_index);

    err = reader.add_scan(sys, trx, config, [&](const ParallelReader::Ctx *ctx) {
      n_recs.inc(1, ctx->thread_id());
      return DB_SUCCESS;
    });

    if (err != DB_SUCCESS) {
      break;
    }
  }

  if (err == DB_SUCCESS) {
    err = reader.run(sys, n_threads);
  }

  if (err == DB_OUT_OF_RESOURCES) {
    ut_a(n_threads > 0);

    log_warn(
      "Resource not available to create threads for parallel scan."
      " Falling back to single thread mode."
    );

    err = reader.run(sys, 0);
  }

  if (err == DB_SUCCESS) {
    n_rows = n_recs.value();
  }

  return err;
}

static DbErr check_table(System &sys, Trx *trx, Index *index, size_t n_threads) {
  ut_a(n_threads > 1);

  using Shards = ut::Sharded_counter<ParallelReader::MAX_THREADS>;

  Shards n_recs{};
  Shards n_dups{};
  Shards n_corrupt{};

  n_dups.clear();
  n_recs.clear();
  n_corrupt.clear();

  using Tuples = std::vector<DTuple *>;
  using Heaps = std::vector<MemHeap *>;

  Heaps heaps;
  Tuples prev_tuples;

  for (size_t i = 0; i < n_threads; ++i) {
    heaps.push_back(mem_heap_create(4096));
  }

  ParallelReader reader(n_threads);
  ParallelReader::ScanRange full_scan;
  ParallelReader::Config config(full_scan, index);

  auto err = reader.add_scan(sys, trx, config, [&](const ParallelReader::Ctx *ctx) {
    const auto rec = ctx->m_rec;
    const auto id = ctx->thread_id();

    n_recs.inc(1, id);

    auto heap = heaps[id];

    if (ctx->m_start) {
      /* Starting scan of a new range. We need to reset the previous tuple
      because we don't know what the value of the previous last tuple was. */
      prev_tuples[id] = nullptr;
    }

    auto prev_tuple = prev_tuples[id];
    ulint *offsets{};

    {
      PhyRec record{index, rec};

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
    }

    if (prev_tuple != nullptr) {
      ulint matched_fields = 0;

      auto cmp = prev_tuple->compare(rec, index, offsets, &matched_fields);

      /* In a unique secondary index we allow equal key values if
      they contain SQL NULLs */

      bool contains_null = false;
      const auto n_ordering = index->get_n_ordering_defined_by_user();

      for (size_t i{}; i < n_ordering; ++i) {
        const auto nth_field = dtuple_get_nth_field(prev_tuple, i);

        if (UNIV_SQL_NULL == dfield_get_len(nth_field)) {
          contains_null = true;
          break;
        }
      }

      if (cmp > 0) {
        std::ostringstream rec_os{};
        std::ostringstream dtuple_os{};

        n_corrupt.inc(1, id);
        prev_tuple->print(dtuple_os);
        rec_os << rec.to_string();

        log_err(
          std::format(
            "Index records in a wrong order in index {} of table {}: {}, {}",
            index->m_name,
            index->m_table->m_name,
            dtuple_os.str(),
            rec_os.str()
          )
        );

        /* Continue reading */
      } else if (index->is_unique() && !contains_null && matched_fields >= index->get_n_ordering_defined_by_user()) {

        std::ostringstream rec_os{};
        std::ostringstream dtuple_os{};

        n_dups.inc(1, id);
        rec_os << rec.to_string();
        prev_tuple->print(dtuple_os);

        log_err(
          std::format(
            "Duplicate key in {} of table {}: {}, {}", index->m_name, index->m_table->m_name, dtuple_os.str(), rec_os.str()
          )
        );
      }
    }

    /* The previous tuple has been compared: its memory is for this one. */
    mem_heap_empty(heap);

    {
      PhyRec record{index, rec};

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
    }

    ulint n_ext{};

    prev_tuples[id] = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

    return DB_SUCCESS;
  });

  if (err == DB_SUCCESS) {
    prev_tuples.resize(n_threads);

    err = reader.run(sys, n_threads);
  }

  if (err == DB_OUT_OF_RESOURCES) {
    ut_a(n_threads > 0);
    log_warn("Resource not available to create threads for parallel scan. Trying single threaded mode.");

    err = reader.run(sys, 0);
  }

  for (auto heap : heaps) {
    mem_heap_free(heap);
  }

  if (n_dups.value() > 0) {
    log_err("Found ", n_dups.value(), " duplicate rows in ", index->m_name);
    err = DB_DUPLICATE_KEY;
  }

  if (n_corrupt.value() > 0) {
    log_err("Found ", n_corrupt.value(), " rows in the wrong order in ", index->m_name);
    err = DB_INDEX_CORRUPT;
  }

  return err;
}

DbErr check_table(System &sys, Trx *trx, Cursor *crsr, size_t n_threads) noexcept {
  auto cursor = crsr;
  auto prebuilt = cursor->m_prebuilt;
  auto index = prebuilt->m_index;

  ut_a(trx != nullptr);

  if (index->is_clustered()) {
    /* The clustered index of a table is always available. During online ALTER TABLE
    that rebuilds the table, the clustered index in the old table will have
    index->online_log pointing to the new table. All indexes of the old table
    will remain valid and the new table will be unaccessible until the completion
    of the ALTER TABLE. */
  } else if (index->is_online_ddl_in_progress()) {
    /* Skip secondary indexes that are being created online. */
    return DB_DDL_IN_PROGRESS;
  }

  if (trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && prebuilt->m_select_lock_type == LOCK_NONE &&
      trx->m_client_n_tables_locked == 0) {
    n_threads = ParallelReader::available_threads(sys, n_threads, false);

    auto rv = trx->assign_read_view(sys);
    ut_a(rv != nullptr);

    ut_a(prebuilt->m_table == index->m_table);

    std::vector<Index *> indexes;

    indexes.push_back(index);

    return check_table(sys, trx, index, n_threads);
  } else {
    log_err("Invalid transaction state for check table");
    return DB_ERROR;
  }
}

/** A status variable a user can query. */
struct StatusVar {
  /** Name of the variable. */
  const char *m_name;

  /** Reads the value from the export variables. */
  int64_t (*m_get)(const ExportVars &);
};

/** @return the value of the export variable M. */
template <auto M>
static int64_t export_var(const ExportVars &vars) noexcept {
  return int64_t(vars.*M);
}

/** All status variables that a user can query. */
static constexpr StatusVar status_vars[] = {
  {"read_req_pending", &export_var<&ExportVars::m_data_pending_reads>},
  {"write_req_pending", &export_var<&ExportVars::m_data_pending_writes>},
  {"fsync_req_pending", &export_var<&ExportVars::m_data_pending_fsyncs>},
  {"write_req_done", &export_var<&ExportVars::m_data_writes>},
  {"read_req_done", &export_var<&ExportVars::m_data_reads>},
  {"fsync_req_done", &export_var<&ExportVars::m_data_fsyncs>},
  {"bytes_total_written", &export_var<&ExportVars::m_data_written>},
  {"bytes_total_read", &export_var<&ExportVars::m_data_read>},
  {"buffer_pool_current_size", &export_var<&ExportVars::m_buffer_pool_pages_total>},
  {"buffer_pool_data_pages", &export_var<&ExportVars::m_buffer_pool_pages_data>},
  {"buffer_pool_dirty_pages", &export_var<&ExportVars::m_buffer_pool_pages_dirty>},
  {"buffer_pool_misc_pages", &export_var<&ExportVars::m_buffer_pool_pages_misc>},
  {"buffer_pool_free_pages", &export_var<&ExportVars::m_buffer_pool_pages_free>},
  {"buffer_pool_read_reqs", &export_var<&ExportVars::m_buffer_pool_read_requests>},
  {"buffer_pool_reads", &export_var<&ExportVars::m_buffer_pool_reads>},
  {"buffer_pool_waited_for_free", &export_var<&ExportVars::m_buffer_pool_wait_free>},
  {"buffer_pool_pages_flushed", &export_var<&ExportVars::m_buffer_pool_pages_flushed>},
  {"buffer_pool_write_reqs", &export_var<&ExportVars::m_buffer_pool_write_requests>},
  {"buffer_pool_total_pages", &export_var<&ExportVars::m_pages_created>},
  {"buffer_pool_pages_read", &export_var<&ExportVars::m_pages_read>},
  {"buffer_pool_pages_written", &export_var<&ExportVars::m_pages_written>},
  {"double_write_pages_written", &export_var<&ExportVars::m_dblwr_pages_written>},
  {"double_write_invoked", &export_var<&ExportVars::m_dblwr_writes>},
  {"log_buffer_slot_waits", &export_var<&ExportVars::m_log_waits>},
  {"log_write_reqs", &export_var<&ExportVars::m_log_write_requests>},
  {"log_write_flush_count", &export_var<&ExportVars::m_log_writes>},
  {"log_bytes_written", &export_var<&ExportVars::m_os_log_written>},
  {"log_fsync_req_done", &export_var<&ExportVars::m_os_log_fsyncs>},
  {"log_write_req_pending", &export_var<&ExportVars::m_os_log_pending_writes>},
  {"log_fsync_req_pending", &export_var<&ExportVars::m_os_log_pending_fsyncs>},
  {"lock_row_waits", &export_var<&ExportVars::m_row_lock_waits>},
  {"lock_row_waiting", &export_var<&ExportVars::m_row_lock_current_waits>},
  {"lock_total_wait_time_in_secs", &export_var<&ExportVars::m_row_lock_time>},
  {"lock_wait_time_avg_in_secs", &export_var<&ExportVars::m_row_lock_time_avg>},
  {"lock_max_wait_time_in_secs", &export_var<&ExportVars::m_row_lock_time_max>},
  {"row_total_read", &export_var<&ExportVars::m_rows_read>},
  {"row_total_inserted", &export_var<&ExportVars::m_rows_inserted>},
  {"row_total_updated", &export_var<&ExportVars::m_rows_updated>},
  {"row_total_deleted", &export_var<&ExportVars::m_rows_deleted>},
  {"page_size", &export_var<&ExportVars::m_page_size>},
};

DbErr status_get_i64(System &sys, const char *name, int64_t *dst) noexcept {
  for (const auto &var : status_vars) {
    if (ib_utf8_strcasecmp(name, var.m_name) == 0) {
      /* Read the latest values into the export variables. */
      sys.m_srv->export_innodb_status();

      *dst = var.m_get(sys.m_srv->m_export_vars);

      return DB_SUCCESS;
    }
  }

  return DB_NOT_FOUND;
}

std::vector<const char *> status_get_all() noexcept {
  std::vector<const char *> names;

  for (const auto &var : status_vars) {
    names.push_back(var.m_name);
  }

  return names;
}

}  // namespace api
