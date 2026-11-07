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

/* Transactions, table and index schemas, DDL. */

#include "api0int.h"

namespace api {

/** Delays an INSERT, DELETE or UPDATE operation if the purge is lagging. */
void delay_dml_if_needed(System &sys) {
  /* Read once (relaxed, the purge thread updates it): checking one value
  and sleeping on another could sleep on a value that just went up. */
  if (const ulint delay = sys.m_srv->m_dml_needed_delay; delay > 0) {
    os_thread_sleep(delay);
  }
}

/**
 * Open a table using the table id, if found then increment table ref count.
 *
 * @param tid - table id to lookup
 * @param locked - true if own dict mutex
 * @return table instance if found
 */
Table *open_table_by_id(System &sys, uint64_t tid, bool locked) noexcept {
  /* We only return the lower 32 bits of the uint64_t. */
  ut_a(tid < 0xFFFFFFFFULL);
  auto table_id = tid;

  if (!locked) {
    sys.m_dict->mutex_acquire();
  }

  auto table = sys.m_dict->table_get_using_id(sys.m_config.m_recv.m_force_recovery, Dict_id{table_id}, true);

  if (table != nullptr && table->m_ibd_file_missing) {

    ib_logger(stderr, "The .ibd file for table %s is missing.", table->m_name);

    sys.m_dict->table_decrement_handle_count(table, true);

    table = nullptr;
  }

  if (!locked) {
    sys.m_dict->mutex_release();
  }

  return table;
}

/**
 * Open a table using the table name, if found then increment table ref count.
 *
 * @param[in] name  Table name
 *
 * @return	table instance if found */
Table *open_table_by_name(System &sys, const char *name) {
  auto table = sys.m_dict->table_get(name, true);

  if (table != nullptr && table->m_ibd_file_missing) {

    log_warn(std::format("The .ibd file for table {} is missing.", name));

    sys.m_dict->table_decrement_handle_count(table, false);

    table = nullptr;
  }

  return table;
}

/**
 * Find table using table name.
 *
 * @param[in] name - table name to lookup
 *
 * @return table instance if found
 */
Table *lookup_table_by_name(System &sys, const char *name) {
  ut_ad(mutex_own(&sys.m_dict->m_mutex));

  auto table = sys.m_dict->table_get(name);

  if (table != nullptr && table->m_ibd_file_missing) {

    log_warn(std::format("The .ibd file for table {} is missing.", name));

    table = nullptr;
  }

  return table;
}

/**
 * Increments innobase_active_counter and every INNOBASE_WAKE_INTERVALth
 * time calls srv_active_wake_master_thread. This function should be used
 * when a single database operation may introduce a small need for
 * server utility activity, like checkpointing.
 */
void wake_master_thread(System &sys) noexcept {
  if ((++sys.m_srv->m_n_api_ops % INNOBASE_WAKE_INTERVAL) == 0) {
    sys.m_srv->active_wake_master_thread();
  }
}

#if 0

/** Calculate the length of the column data less trailing space. */
static
/**
 * Calculate the max row size of the columns in a cluster index.
 *
 * @return	max row length
 */
static ulint get_max_row_len(Index *index) {
  ut_a(index->is_clustered());

  ulint max_len{};
  const ulint n_fields = index->get_n_fields();

  /* Add the size of the ordering columns in the
  clustered index. */
  for (ulint i{}; i < n_fields; ++i) {
    auto col = index->get_nth_col(i);

    /* Use the maximum output size of
    varint::encode(), although the encoded
    length should always fit in 2 bytes. */
    max_len += col_get_max_size(col);
  }

  return max_len;
}
#endif

/**
 * Read the columns from a record into a tuple.
 *
 * @param[in] rec           Record to read
 * @param[in] tuple         Tuple to read into
 */
void read_tuple(System &sys, const Rec &rec, Tuple *tuple) noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  RecOffs offsets(offsets_.data());
  DTuple *dtuple = tuple->m_ptr;
  const Index *dindex = tuple->m_index;

  rec_offs_init(offsets_);

  {
    PhyRec record{dindex, rec};

    offsets = record.get_all_col_offsets(offsets, &tuple->m_heap, Current_location());
  }

  auto rec_meta_data = rec.get_info_bits();
  dtuple_set_info_bits(dtuple, rec_meta_data);

  /* Make a copy of the rec. */
  auto ptr = reinterpret_cast<void *>(mem_heap_alloc(tuple->m_heap, offsets.size()));
  auto copy = rec.copy(ptr, offsets);

  /* Avoid a debug assertion in rec.offs_validate(). */
  ut_d(rec.offs_make_valid(dindex, offsets));

  auto n_index_fields = std::min<ulint>(offsets.n_fields(), dtuple_get_n_fields(dtuple));

  for (ulint i{}; i < n_index_fields; ++i) {
    Dfield *dfield;

    if (tuple->m_type == TPL_ROW) {
      auto index_field = dindex->get_nth_field(i);
      auto col = index_field->get_col();
      auto col_no = col->get_no();

      dfield = dtuple_get_nth_field(dtuple, col_no);

    } else {
      dfield = dtuple_get_nth_field(dtuple, i);
    }

    ulint len;
    auto data = copy.get_nth_field(offsets, i, &len);

    /* Fetch and copy any externally stored column. */
    if (rec_offs_nth_extern(offsets, i)) {

      Blob blob;

      data = blob.copy_externally_stored_field(sys, copy, offsets, i, &len, tuple->m_heap);

      ut_a(len != UNIV_SQL_NULL);
    }

    dfield_set_data(dfield, data.get(), len);
  }
}

/**
 * Create an InnoDB key tuple.
 *
 * @param index The index for which the tuple is required.
 * @param n_cols The number of user-defined columns.
 * @param heap The memory heap.
 * @return Tuple instance created, or nullptr.
 */
Tuple *key_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap) {

  auto tuple = reinterpret_cast<Tuple *>(mem_heap_alloc(heap, sizeof(Tuple)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->m_heap = heap;
  tuple->m_index = index;
  tuple->m_type = TPL_KEY;

  /* Is it a generated clustered index ? */
  if (n_cols == 0) {
    ++n_cols;
  }

  tuple->m_ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  index->copy_types(tuple->m_ptr, n_cols);

  for (ulint i = 0; i < n_cols; i++) {

    Dfield *dfield;

    dfield = dtuple_get_nth_field(tuple->m_ptr, i);
    dfield_set_null(dfield);
  }

  auto n_cmp_cols = index->get_n_ordering_defined_by_user();

  dtuple_set_n_fields_cmp(tuple->m_ptr, n_cmp_cols);

  return tuple;
}

/**
 * Create an InnoDB key tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of user-defined columns.
 *
 * @return Tuple instance created, or nullptr.
 */
Tuple *key_tuple_new(const Index *index, ulint n_cols) noexcept {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return key_tuple_new_low(index, n_cols, heap);
}

/**
 * Create an InnoDB row tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of columns in the tuple.
 * @param[in] heap The memory heap.
 *
 * @return Tuple instance created, or nullptr.
 */
Tuple *row_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap) noexcept {
  auto tuple = reinterpret_cast<Tuple *>(mem_heap_alloc(heap, sizeof(Tuple)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->m_heap = heap;
  tuple->m_index = index;
  tuple->m_type = TPL_ROW;

  tuple->m_ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  index->m_table->copy_types(tuple->m_ptr);

  return tuple;
}

/**
 * Create an InnoDB row tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of columns in the tuple.
 *
 * @return Tuple instance created, or nullptr.
 */
Tuple *row_tuple_new(const Index *index, ulint n_cols) noexcept {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return row_tuple_new_low(index, n_cols, heap);
}

uint64_t api_version() noexcept {
  return ((uint64_t)IB_API_VERSION_CURRENT << 32 | (IB_API_VERSION_REVISION << 16) | IB_API_VERSION_AGE);
}

DbErr trx_start(System &sys, Trx *trx, TrxLevel trx_level) noexcept {
  DbErr err = DB_SUCCESS;

  ut_a(trx_level >= IB_TRX_READ_UNCOMMITTED);
  ut_a(trx_level <= IB_TRX_SERIALIZABLE);

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    auto started = trx->start(sys, ULINT_UNDEFINED);
    ut_a(started);

    trx->m_isolation_level = static_cast<TrxIsolation>(trx_level);
  } else {
    err = DB_ERROR;
  }

  trx->m_client_ctx = nullptr;

  return err;
}

void trx_interrupt(Trx *trx) noexcept {
  trx->interrupt();
}

void trx_set_client_data(Trx *trx, void *client_data) noexcept {

  trx->m_client_ctx = client_data;
}

Trx *trx_begin(System &sys, TrxLevel trx_level) noexcept {
  auto trx = sys.m_trx_sys->create_user_trx(nullptr);
  auto started = trx_start(sys, trx, trx_level);

  ut_a(started);

  return trx;
}

ut::Timestamp clock_next(System &sys) noexcept { return sys.m_trx_sys->clock_next(); }

DbErr clock_update(System &sys, const ut::Timestamp &received) noexcept {
  return sys.m_trx_sys->clock_update(received) ? DB_SUCCESS : DB_INVALID_INPUT;
}

ut::Timestamp trx_timestamp(const Trx *trx) noexcept { return ut::id_to_timestamp(trx->m_id); }

ut::Timestamp trx_commit_timestamp(System &sys, const Trx *trx) noexcept { return sys.m_trx_sys->timestamp_of_word(trx->m_commit_word); }

ut::Timestamp trx_view_timestamp(System &sys, const Trx *trx) noexcept {
  return trx->m_read_view == nullptr ? ut::Timestamp{} : sys.m_trx_sys->timestamp_of_word(trx->m_read_view->m_snap_word);
}

TrxState trx_state(Trx *trx) noexcept {

  return static_cast<TrxState>(trx->m_conc_state.load());
}

DbErr trx_release(System &sys, Trx *trx) noexcept {

  ut_ad(trx != nullptr);
  sys.m_trx_sys->destroy_user_trx(trx);

  return DB_SUCCESS;
}

DbErr trx_commit(System &sys, Trx *trx) noexcept {

  auto err = trx->commit(sys);
  ut_a(err == DB_SUCCESS);

  err = schema_unlock(sys, trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = trx_release(sys, trx);
  ut_a(err == DB_SUCCESS);

  wake_master_thread(sys);

  return DB_SUCCESS;
}

DbErr trx_rollback(System &sys, Trx *trx) noexcept {

  auto err = trx_general_rollback(sys, trx, false, nullptr);

  /* It should always succeed */
  ut_a(err == DB_SUCCESS);

  err = schema_unlock(sys, trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = trx_release(sys, trx);
  ut_a(err == DB_SUCCESS);

  wake_master_thread(sys);

  return err;
}

/**
 * Check that the combination of values and name makes sense.
 *
 * @param[in] name The column name.
 * @param[in] col_type The column type.
 * @param[in] col_attr The column attribute.
 * @param[in] len The length of the column.
 *
 * @return True if the combination is valid, false otherwise.
 */
static bool check_col_is_ok(const char *name, ColType col_type, ColAttr col_attr, ulint len) {
  if (strlen(name) > IB_MAX_COL_NAME_LEN) {
    return false;
  } else if ((col_type == IB_VARCHAR || col_type == IB_CHAR || col_type == IB_BINARY) && len == 0) {
    return false;
  } else if (col_type == IB_INT) {
    switch (len) {
      case 1:
      case 2:
      case 4:
      case 8:
        break;
      default:
        return false;
    }
  } else if (col_type == IB_FLOAT && len != 4) {
    return false;
  } else if (col_type == IB_DOUBLE && len != 8) {
    return false;
  }

  return true;
}

/**
 * Find an index definition from the index vector using index name.
 *
 * @param[in] indexes Vector of indexes.
 * @param[in] name Index name.
 *
 * @return Index definition if found, else nullptr.
 */
static const IndexSchema *table_find_index(const std::vector<IndexSchema *> &indexes, const char *name) {
  for (const auto index_def : indexes) {
    if (ib_utf8_strcasecmp(name, index_def->m_name) == 0) {
      return index_def;
    }
  }

  return nullptr;
}

/**
 * Get the InnoDB internal precise type from the schema column definition.
 *
 * @param[in] schema_col The column definition.
 *
 * @return The precise type in API format.
 */
static ulint col_get_prtype(const Col *col) {
  ulint prtype = 0;

  if (col->m_col_attr & IB_COL_UNSIGNED) {
    prtype |= DATA_UNSIGNED;

    ut_a(col->m_col_type == IB_INT);
  }

  if (col->m_col_attr & IB_COL_NOT_NULL) {
    prtype |= DATA_NOT_NULL;
  }

  if (col->m_col_attr & IB_COL_CUSTOM1) {
    prtype |= DATA_CUSTOM_TYPE;
  }

  if (col->m_col_attr & IB_COL_CUSTOM2) {
    prtype |= (DATA_CUSTOM_TYPE << 1);
  }

  if (col->m_col_attr & IB_COL_CUSTOM3) {
    prtype |= (DATA_CUSTOM_TYPE << 2);
  }

  return prtype;
}

/**
 * Get the InnoDB internal main type from the schema column definition.
 *
 * @param[in] schema_col The column list head
 *
 * @return The main type.
 */
static ulint col_get_mtype(const Col *col) noexcept {
  /* Note: The api0api.h types should map directly to the internal numeric codes. */
  return col->m_col_type;
}

/**
 * Find a column in the the column vector with the same name.
 *
 * @param[in] cols                 Column list.
 * @param[in] name                 Column name to file.
 *
 * @return The column definition if found, else nullptr.
 * @return	col. def. if found else nullptr
 */
static const Col *table_find_col(const std::vector<Col *> &cols, const char *name) noexcept {
  for (const auto col : cols) {

    if (ib_utf8_strcasecmp(col->m_name, name) == 0) {
      return col;
    }
  }

  return nullptr;
}

/**
 * Find a column in the the column list with the same name.
 *
 * @param cols The column list head.
 * @param name The column name to find.
 * @return Column definition if found, else nullptr.
 */
static const KeyCol *index_find_col(const std::vector<KeyCol *> &cols, const char *name) noexcept {
  for (auto col : cols) {

    if (ib_utf8_strcasecmp(col->m_name, name) == 0) {
      return col;
    }
  }

  return nullptr;
}

DbErr TableSchema::add_col(const char *name, ColType col_type, ColAttr col_attr, uint16_t client_type, ulint len) noexcept {
  auto table_def = this;
  DbErr err = DB_SUCCESS;

  if (table_def->m_table != nullptr) {
    err = DB_ERROR;
  } else if (table_find_col(*table_def->m_cols, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else if (!check_col_is_ok(name, col_type, col_attr, len)) {
    err = DB_ERROR;
  } else {
    auto heap = table_def->m_heap;

    auto col = reinterpret_cast<Col *>(mem_heap_zalloc(heap, sizeof(Col)));

    if (col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      col->m_name = mem_heap_strdup(heap, name);
      col->m_col_type = col_type;
      col->m_col_attr = col_attr;

      col->m_len = len;

      table_def->m_cols->emplace_back(col);
    }
  }

  return err;
}

DbErr TableSchema::add_index(const char *name, IndexSchema **idx_sch) noexcept {
  auto table_def = this;
  DbErr err = DB_SUCCESS;

  if (table_def->m_table != nullptr) {
    err = DB_ERROR;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  }
  if (table_find_index(*table_def->m_indexes, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else {
    auto heap = table_def->m_heap;
    auto index_def = reinterpret_cast<IndexSchema *>(mem_heap_zalloc(heap, sizeof(IndexSchema)));

    if (index_def == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {

      index_def->m_heap = heap;

      index_def->m_schema = table_def;

      index_def->m_name = mem_heap_strdup(heap, name);

      index_def->m_cols = new std::vector<KeyCol *>();

      table_def->m_indexes->emplace_back(index_def);

      *idx_sch = index_def;
    }
  }

  return err;
}

void TableSchema::destroy(System &sys, TableSchema *table_def) noexcept {

  /* Check that all indexes are owned by the table schema. */
  for (auto index_def : *table_def->m_indexes) {
    ut_a(index_def->m_schema != nullptr);
  }

  if (table_def->m_table != nullptr) {
    sys.m_dict->table_decrement_handle_count(table_def->m_table, false);
  }

  for (auto index_def : *table_def->m_indexes) {
    delete index_def->m_cols;
  }

  delete table_def->m_cols;
  delete table_def->m_indexes;

  mem_heap_free(table_def->m_heap);
}

/**
 * Normalizes a table name string. A normalized name consists of the
 * database name catenated to '/' and table name. An example:
 * test/mytable. On Windows normalization puts both the database name and the
 * table name always to lower case. This function can be called for system
 * tables and they don't have a database component. For tables that don't have
 * a database component, we don't normalize them to lower case on Windows.
 * The assumption is that they are system tables that reside in the system
 * table space.
 *
 * @param[out] norm_name Normalized name as a null-terminated string.
 * @param[in] name Table name string.
 */
void normalize_table_name(char *norm_name, const char *name) {
  const char *ptr = name;

  /* Scan name from the end */
  ptr += strlen(name) - 1;

  /* Find the start of the table name. */
  while (ptr >= name && *ptr != '\\' && *ptr != '/' && ptr > name) {
    --ptr;
  }

  /* For system tables there is no '/' or dbname. */
  ut_a(ptr >= name);

  if (ptr > name) {
    auto table_name = ptr + 1;

    --ptr;

    while (ptr >= name && *ptr != '\\' && *ptr != '/') {
      ptr--;
    }

    auto db_name = ptr + 1;

    memcpy(norm_name, db_name, strlen(name) + 1 - (db_name - name));

    norm_name[table_name - db_name - 1] = '/';
  } else {
    strcpy(norm_name, name);
  }
}

/**
 * Check whether the table name conforms to our requirements. Currently
 * we only do a simple check for the presence of a '/'.
 *
 * @param[in] name The table name to check.
 *
 * @return DB_SUCCESS or err code.
 */
static DbErr table_name_check(const char *name) noexcept {
  const char *slash = nullptr;
  ulint len = strlen(name);

  if (len < 2 || *name == '/' || name[len - 1] == '/' || (name[0] == '.' && name[1] == '/') ||
      (name[0] == '.' && name[1] == '.' && name[2] == '/')) {

    return DB_DATA_MISMATCH;
  }

  for (; *name; ++name) {
    if (*name == '/') {
      if (slash) {
        return DB_DATA_MISMATCH;
      }
      slash = name;
    }
  }

  return slash ? DB_SUCCESS : DB_DATA_MISMATCH;
}

DbErr TableSchema::create(const char *name, TableSchema **tbl_sch, TableFormat tbl_fmt) noexcept {

  auto err = table_name_check(name);

  if (err != DB_SUCCESS) {
    return err;
  }

  MemHeap *heap = mem_heap_create(1024);

  if (heap == nullptr) {
    err = DB_OUT_OF_MEMORY;
  } else {
    auto table_def = reinterpret_cast<TableSchema *>(mem_heap_zalloc(heap, sizeof(TableSchema)));

    if (table_def == nullptr) {
      err = DB_OUT_OF_MEMORY;

      mem_heap_free(heap);
    } else {
      table_def->m_heap = heap;

      auto normalized_name = mem_heap_strdup(heap, name);

      normalize_table_name(normalized_name, name);

      table_def->m_name = normalized_name;

      table_def->m_tbl_fmt = tbl_fmt;

      table_def->m_cols = new std::vector<Col *>();

      table_def->m_indexes = new std::vector<IndexSchema *>();

      *tbl_sch = table_def;
    }
  }

  return err;
}

/**
 * Get the column number within the index definition.
 *
 * @param[in] index_schema The index definition.
 * @param[in] name The column name to search.
 *
 * @return -1 or column number.
 */
static int index_get_col_no(const IndexSchema *index_def, const char *name) {
  int col_no;

  /* Is this column definition for an existing table ? */
  if (index_def->m_table != nullptr) {
    col_no = index_def->m_table->get_col_no(name);
  } else {
    const auto col = table_find_col(*index_def->m_schema->m_cols, name);

    if (col != nullptr) {
      /* We simply note that we've found the column. */
      col_no = 0;
    } else {
      col_no = -1;
    }
  }

  return col_no;
}

/**
 * Check whether a prefix length index is allowed on the column.
 *
 * @param[in] index_schema The index definition.
 * @param[in] name The column name to check.
 *
 * @return True if allowed.
 */
static int index_is_prefix_allowed(const IndexSchema *index_def, const char *name) noexcept {
  bool allowed = true;
  ulint mtype = ULINT_UNDEFINED;

  /* Is this column definition for an existing table ? */
  if (index_def->m_table != nullptr) {
    auto table = index_def->m_table;
    auto col_no = table->get_col_no(name);
    ut_a(col_no != -1);

    const auto col = table->get_nth_col(col_no);
    ut_a(col != nullptr);

    mtype = col->mtype;
  } else {
    const Col *schema_col;

    schema_col = table_find_col(*index_def->m_schema->m_cols, name);

    ut_a(schema_col != nullptr);

    mtype = (ulint)schema_col->m_col_type;
  }

  /* The following column types can't have prefix column indexes. */
  switch (mtype) {
    case DATA_INT:
    case DATA_FLOAT:
    case DATA_DOUBLE:
    case DATA_DECIMAL:
      allowed = false;
      break;

    case ULINT_UNDEFINED:
      ut_error;
  }

  return allowed;
}

DbErr IndexSchema::add_col(const char *name, ulint prefix_len) noexcept {
  auto index_def = this;
  DbErr err = DB_SUCCESS;

  /* Check for duplicates. */
  if (index_find_col(*index_def->m_cols, name) != nullptr) {
    err = DB_COL_APPEARS_TWICE_IN_INDEX;
    /* Check if the column exists in the table definition. */
  } else if (index_get_col_no(index_def, name) == -1) {
    err = DB_NOT_FOUND;
    /* Some column types can't have prefix length indexes. */
  } else if (prefix_len > 0 && !index_is_prefix_allowed(index_def, name)) {
    err = DB_SCHEMA_ERROR;
  } else if (prefix_len > DICT_MAX_INDEX_COL_LEN) {
    err = DB_SCHEMA_ERROR;
  } else {
    auto heap = index_def->m_heap;
    auto col = reinterpret_cast<KeyCol *>(mem_heap_zalloc(heap, sizeof(KeyCol)));

    if (col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      col->m_name = mem_heap_strdup(heap, name);
      col->m_prefix_len = prefix_len;

      index_def->m_cols->emplace_back(col);
    }
  }

  return err;
}

DbErr IndexSchema::create(
  System &sys, Trx *usr_trx, const char *name, const char *table_name, IndexSchema **idx_sch
) noexcept {
  DbErr err = DB_SUCCESS;

  if (!schema_lock_is_exclusive(usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  normalize_table_name(normalized_name, table_name);

  auto table = lookup_table_by_name(sys, normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table == nullptr) {
    err = DB_TABLE_NOT_FOUND;
  } else if (table->get_index_on_name(name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else {
    auto heap = mem_heap_create(1024);

    if (heap == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      auto index_def = reinterpret_cast<IndexSchema *>(mem_heap_zalloc(heap, sizeof(IndexSchema)));

      if (index_def == nullptr) {
        err = DB_OUT_OF_MEMORY;

        mem_heap_free(heap);
      } else {
        index_def->m_heap = heap;

        index_def->m_table = table;

        index_def->m_name = mem_heap_strdup(heap, name);

        index_def->m_cols = new std::vector<KeyCol *>();

        index_def->m_usr_trx = usr_trx;

        *idx_sch = index_def;
      }
    }
  }

  return err;
}

/**
 * Get an index definition that is tagged as a clustered index.
 *
 * @param[in] indexes The index defs. to search.
 *
 * @return Cluster index schema.
 */
static IndexSchema *find_clustered_index(const std::vector<IndexSchema *> &indexes) {
  for (const auto index_def : indexes) {
    if (index_def->m_clustered) {
      return index_def;
    }
  }

  return nullptr;
}

DbErr IndexSchema::set_clustered() noexcept {
  auto index_def = this;
  DbErr err = DB_SUCCESS;

  /* If this index schema is part of a table schema then we need
  to check the state of the other indexes. */

  if (index_def->m_schema != nullptr) {
    auto clust_index_def = find_clustered_index(*index_def->m_schema->m_indexes);

    if (clust_index_def != nullptr) {
      ut_a(clust_index_def->m_clustered);
      clust_index_def->m_clustered = false;
    }
  }

  index_def->m_unique = true;
  index_def->m_clustered = true;

  return err;
}

DbErr IndexSchema::set_unique() noexcept {
  auto index_def = this;
  DbErr err = DB_SUCCESS;
  index_def->m_unique = true;

  return err;
}

void IndexSchema::destroy(IndexSchema *index_def) noexcept {

  ut_a(index_def->m_schema == nullptr);

  delete index_def->m_cols;
  mem_heap_free(index_def->m_heap);
}

/**
 * Convert the table definition table attributes to the internal format.
 *
 * @param[in] table_def The table definition.
 *
 * @return Flags.
 */
static ulint table_def_get_flags(const TableSchema *table_def) {
  ulint flags = 0;

  switch (table_def->m_tbl_fmt) {
    case IB_TBL_V1:
      break;
    default:
      ut_error;
  }

  return flags;
}

/**
 * Copy the index definition to row0merge.c format.
 *
 * @param[in] index_schema Key definition for index.
 * @param[in] clustered True if clustered index.
 *
 * @return Converted index definition.
 */
static const index_def_t *copy_index_definition(IndexSchema *index_schema, bool clustered) {
  auto index_def = reinterpret_cast<index_def_t *>(mem_heap_zalloc(index_schema->m_heap, sizeof(index_def_t)));
  auto name_len = strlen(index_schema->m_name);
  auto index_name = reinterpret_cast<char *>(mem_heap_zalloc(index_schema->m_heap, name_len + 2));

  /* The TEMP_INDEX_PREFIX is only needed if we are rebuilding an
  index or creating a new index on a table that has records. If
  the definition is owned by a table schema then we can be sure that
  this index definition is part of a CREATE TABLE. */
  if (index_schema->m_schema == nullptr) {
    *index_name = TEMP_INDEX_PREFIX;
    memcpy(index_name + 1, index_schema->m_name, name_len + 1);
  } else {
    memcpy(index_name, index_schema->m_name, name_len);
  }

  index_def->name = index_name;
  index_def->n_fields = index_schema->m_cols->size();

  if (index_schema->m_unique) {
    index_def->ind_type = DICT_UNIQUE;
  } else {
    index_def->ind_type = 0;
  }

  if (clustered) {
    index_def->ind_type |= DICT_CLUSTERED;
  }

  index_def->fields = (index_field_t *)mem_heap_zalloc(index_schema->m_heap, sizeof(index_field_t) * index_def->n_fields);

  for (ulint i = 0; i < index_schema->m_cols->size(); ++i) {
    auto col = index_schema->m_cols->at(i);

    index_def->fields[i].field_name = col->m_name;
    index_def->fields[i].prefix_len = col->m_prefix_len;
  }

  return index_def;
}

/**
 * (Re)Create a secondary index.
 *
 * @param[in] usr_trx The transaction.
 * @param[in] table The parent table of the index.
 * @param[in] index_schema The index definition.
 * @param[in] create True if part of table create.
 * @param[out] index The index created.
 *
 * @return DB_SUCCESS or err code.
 */
static DbErr build_secondary_index(
  System &sys, Trx *usr_trx, Table *table, IndexSchema *index_schema, bool create, Index **index
) noexcept {
  DbErr err;
  Trx *ddl_trx;

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (!create) {
    ddl_trx = sys.m_trx_sys->create_user_trx(nullptr);
    const auto started = ddl_trx->start(sys, ULINT_UNDEFINED);
    ut_a(started);
  } else {
    ddl_trx = usr_trx;
  }

  /* Set the CLUSTERED flag to false. */
  const auto index_def = copy_index_definition(index_schema, false);

  ut_a(!(index_def->ind_type & DICT_CLUSTERED));

  ddl_trx->m_op_info = "creating secondary index";

  auto trx = usr_trx;

  if (!(create || schema_lock_is_exclusive(trx))) {
    err = schema_lock_exclusive(sys, trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  if (!create) {
    /* Flag this transaction as a dictionary operation, so that
    the data dictionary will be locked in crash recovery. */
    ddl_trx->set_dict_operation(TRX_DICT_OP_INDEX);
  }

  *index = row_merge_create_index(sys, ddl_trx, table, index_def);

  if (!create) {
    /* Even if the user locked the schema, we release it here and
    build the index without holding the dictionary lock. */
    err = schema_unlock(sys, usr_trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);
  }

  err = ddl_trx->m_error_state;

  if (!create) {
    /* Commit the data dictionary transaction in order to release
    the table locks on the system tables. */
    auto err = ddl_trx->commit(sys);
    ut_a(err == DB_SUCCESS);

    sys.m_trx_sys->destroy_user_trx(ddl_trx);
  }

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (*index != nullptr) {
    ut_a(err == DB_SUCCESS);

    (*index)->m_cmp_ctx = nullptr;

    /* Read the clustered index records and build the index. */
    err = row_merge_build_indexes(sys, usr_trx, table, table, index, 1, nullptr);
  }

  return err;
}

/**
 * Create a temporary tablename using table name and id.
 *
 * @param[in] heap Memory heap.
 * @param[in] id Identifier [0-9a-zA-Z].
 * @param[in] table_name Table name.
 *
 * @return Temporary tablename.
 */
static char *table_create_temp_name(MemHeap *heap, char id, const char *table_name) noexcept {
  static const char suffix[] = "# ";

  const auto len = strlen(table_name);
  auto name = reinterpret_cast<char *>(mem_heap_zalloc(heap, len + sizeof(suffix)));

  memcpy(name, table_name, len);
  memcpy(name + len, suffix, sizeof(suffix));
  name[len + (sizeof(suffix) - 2)] = id;

  return name;
}

/**
 * Create an index definition from the index.
 *
 * @param[in] index The index definition to copy.
 * @param[out] index_def The index definition.
 * @param[in] heap The heap where allocated.
 */
static void index_create_def(const Index *index, index_def_t *index_def, MemHeap *heap) noexcept {
  const auto n_fields = index->m_n_user_defined_cols;

  index_def->fields = (MergeIndexField *)mem_heap_zalloc(heap, n_fields * sizeof(*index_def->fields));

  index_def->name = index->m_name;
  index_def->n_fields = n_fields;
  index_def->ind_type = index->m_type & ~DICT_CLUSTERED;

  auto dfield = index->m_fields;

  for (ulint i{}; i < n_fields; ++i, ++dfield) {
    auto def_field = &index_def->fields[i];

    def_field->field_name = dfield->m_name;
    def_field->prefix_len = dfield->m_prefix_len;
  }
}

/**
 * Create and return an array of index definitions on a table. Skip the
 * old clustered index if it's a generated clustered index. If there is a
 * user defined clustered index on the table its CLUSTERED flag will be unset.
 *
 * @param[in] trx in: transaction
 * @param[in] table in: table definition
 * @param[in] heap in: heap where space for index definitions are allocated
 * @param[out] n_indexes out: number of indexes returned
 *
 * @return index definitions or nullptr
 */
static index_def_t *table_create_index_defs(System &sys, Trx *trx, const Table *table, MemHeap *heap, ulint *n_indexes) noexcept {
  auto sz = sizeof(index_def_t) * UT_LIST_GET_LEN(table->m_indexes);
  auto index_defs = reinterpret_cast<index_def_t *>(mem_heap_zalloc(heap, sz));
  auto err = schema_lock_exclusive(sys, trx);

  ut_a(err == DB_SUCCESS);

  auto index = table->get_first_index();

  /* Skip a generated cluster index. */
  if (ib_utf8_strcasecmp(index->m_name, GEN_CLUST_INDEX) == 0) {
    ut_a(index->get_nth_col(0)->mtype == DATA_SYS);
    index = index->get_next();
  }

  while (index != nullptr) {
    index_create_def(index, index_defs++, heap);
    index = index->get_next();
  }

  err = schema_unlock(sys, trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  return index_defs;
}

/**
 * Create a cluster index specified by the user. The cluster index
 * shouldn't already exist.
 *
 * @param[in] trx in: transaction
 * @param[in] table in: parent table of index
 * @param[in] index_schema in: index definition
 * @param[out] index out: index created
 *
 * @return DB_SUCCESS or err code
 */
static DbErr create_cluster_index(System &sys, Trx *trx, Table *table, IndexSchema *index_schema, Index **index) noexcept {

  ut_a(!index_schema->m_cols->empty());

  /* Set the CLUSTERED flag to true. */
  auto index_def = copy_index_definition(index_schema, true);

  trx->m_op_info = "creating clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  auto err = row_lock_table_with_retry(sys, trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *index = row_merge_create_index(sys, trx, table, index_def);

    err = trx->m_error_state;
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create the secondary indexes on new table using the index definitions
 * from the src table. The assumption is that a cluster index on the
 * new table already exists. All the indexes in the source table will
 * be copied with the exception of any generated clustered indexes.
 *
 * @param[in] trx in: transaction
 * @param[in] src_table in: table to clone from
 * @param[in] new_table in: table to clone to
 * @param[in] heap in: memory heap to use
 *
 * @return DB_SUCCESS or error code
 */
static DbErr table_clone_indexes(System &sys, Trx *trx, Table *src_table, Table *new_table, MemHeap *heap) noexcept {
  ulint n_index_defs = 0;
  auto index_defs = table_create_index_defs(sys, trx, src_table, heap, &n_index_defs);

  ut_a(index_defs != nullptr);

  for (ulint i{}; i < n_index_defs; ++i) {
    ut_a(!(index_defs[i].ind_type & DICT_CLUSTERED));
    auto index = row_merge_create_index(sys, trx, new_table, &index_defs[i]);

    if (index == nullptr) {
      return trx->m_error_state;
    }
  }

  return DB_SUCCESS;
}

/**
 * Clone the indexes definitions from src_table to dst_table. The cluster
 * index is not cloned. If it was generated then it's dropped else it's
 * demoted to a secondary index. A new cluster index is created for the
 * new table.
 *
 * @param[in] trx in: transaction
 * @param[in] src_table in: table to clone from
 * @param[out] new_table out: cloned table
 * @param[in] index_schema in: new cluster index definition
 * @param[in] heap in: memory heap to use
 *
 * @return DB_SUCCESS or error code
 */
static DbErr table_clone(
  System &sys, Trx *trx, Table *src_table, Table **new_table, IndexSchema *index_schema, MemHeap *heap
) noexcept {
  auto new_table_name = table_create_temp_name(heap, '1', src_table->m_name);
  auto err = schema_lock_exclusive(sys, trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  /* Set the CLUSTERED flag to true. */
  auto index_def = copy_index_definition(index_schema, true);

  /* Create the new table and the cluster index. */
  *new_table = row_merge_create_temporary_table(sys, new_table_name, index_def, src_table, trx);

  if (unlikely(new_table == nullptr)) {
    err = trx->m_error_state;
  } else {
    trx->m_table_id = (*new_table)->m_id;

    err = table_clone_indexes(sys, trx, src_table, *new_table, heap);
  }

  err = schema_unlock(sys, trx);
  ut_a(err == DB_SUCCESS);

  return err;
}

/**
 * Copy the data from the source table to dst table.
 *
 * @param[in] trx in: transaction
 * @param[in] src_table in: table to copy from
 * @param[in] dst_table in: table to copy to
 * @param[in] heap in: heap to use
 *
 * @return DB_SUCCESS or error code
 */
static DbErr table_copy(System &sys, Trx *trx, Table *src_table, Table *dst_table, MemHeap *heap) noexcept {
  auto err = schema_lock_exclusive(sys, trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  auto n_indexes = dst_table->m_indexes.size();
  auto indexes = reinterpret_cast<Index **>(mem_heap_zalloc(heap, n_indexes * sizeof(Index *)));
  auto index = dst_table->get_first_index();

  n_indexes = 0;

  /* Copy the indexes to an array. */
  while (index != nullptr) {
    indexes[n_indexes++] = index;
    index = index->get_next();
  }
  ut_a(n_indexes == dst_table->m_indexes.size());

  err = schema_unlock(sys, trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  /* Build the actual indexes. */
  return row_merge_build_indexes(sys, trx, src_table, dst_table, indexes, n_indexes, nullptr);
}

/**
 * Create a default cluster index, this usually means the user didn't
 * create a table with a primary key.
 *
 * @param[in] trx in: transaction
 * @param[in] table in: parent table of index
 * @param[out] index out: index created
 *
 * @return DB_SUCCESS or err code
 */
static DbErr create_default_cluster_index(System &sys, Trx *trx, Table *table, Index **index) noexcept {
  index_def_t index_def;

  index_def.n_fields = 0;
  index_def.fields = nullptr;
  index_def.name = GEN_CLUST_INDEX;
  index_def.ind_type = DICT_CLUSTERED;

  trx->m_op_info = "creating default clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(schema_lock_is_exclusive(trx));

  auto err = row_lock_table_with_retry(sys, trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *index = row_merge_create_index(sys, trx, table, &index_def);

    err = trx->m_error_state;
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create the indexes for the table. Each index is created in a separate transaction.
 * The caller is responsible for dropping any indexes that exist if there is a failure.
 *
 * @param[in] ddl_trx in: transaction
 * @param[in] table in: table where index created
 * @param[in] indexes in: index defs. to create
 *
 * @return DB_SUCCESS or err code
 */
static DbErr create_indexes(System &sys, Trx *ddl_trx, Table *table, const std::vector<IndexSchema *> &indexes) noexcept {
  DbErr err = DB_ERROR;
  Index *index = nullptr;
  IndexSchema *clust_index_def = nullptr;

  auto n_indexes = indexes.size();

  if (n_indexes > 0) {
    clust_index_def = find_clustered_index(indexes);

    if (clust_index_def != nullptr) {
      ut_a(clust_index_def->m_clustered);

      err = create_cluster_index(sys, ddl_trx, table, clust_index_def, &index);
    }
  }

  if (clust_index_def == nullptr) {
    err = create_default_cluster_index(sys, ddl_trx, table, &index);
  }

  for (ulint i{}; err == DB_SUCCESS && i < n_indexes; ++i) {
    auto index_def = indexes.at(i);

    ut_a(!index_def->m_cols->empty());

    if (!index_def->m_clustered) {
      /* Since this is part of CREATE TABLE, set the create flag to true. */
      err = build_secondary_index(sys, ddl_trx, table, index_def, true, &index);
    } else {
      /* There can be at most one cluster definition. */
      ut_a(clust_index_def == index_def);
    }
  }

  return err;
}

/**
 * Get a table id. The caller must have acquired the dictionary mutex.
 *
 * @param[in] table_name in: table to find
 * @param[out] table_id out: table id if found
 *
 * @return DB_SUCCESS if found
 */
DbErr table_get_id_low(System &sys, const char *table_name, uint64_t *table_id) noexcept {
  DbErr err = DB_TABLE_NOT_FOUND;

  *table_id = 0;

  auto table = lookup_table_by_name(sys, table_name);

  if (table != nullptr) {
    *table_id = table->m_id;

    err = DB_SUCCESS;
  }

  return err;
}

DbErr table_create(System &sys, Trx *ddl_trx, const TableSchema *table_def, uint64_t *id) noexcept {

  /* Another thread may have created the table already when we get
  here. We need to search the data dictionary before we attempt to
  create the table. */

  if (!schema_lock_is_exclusive(ddl_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto err = table_get_id_low(sys, table_def->m_name, id);

  if (err == DB_SUCCESS) {
    return DB_TABLE_EXISTS;
  }

  *id = 0;

  auto n_cols = table_def->m_cols->size();

  if (n_cols == 0) {
    return DB_SCHEMA_ERROR;
  }

  ulint n_cluster{};

  /* Check that all index definitions are valid. */
  for (auto index_schema : *table_def->m_indexes) {

    /* Check that the index definition has at least one column. */
    if (index_schema->m_cols->empty()) {
      return DB_SCHEMA_ERROR;
    }

    /* Check for duplicate cluster definitions. */
    if (index_schema->m_clustered) {
      ++n_cluster;

      if (n_cluster > 1) {
        return DB_SCHEMA_ERROR;
      }
    }
  }

  /* Create the table prototype. */
  auto table =
    Table::create(table_def->m_name, SYS_TABLESPACE, n_cols, table_def_get_flags(table_def), false, Current_location());

  /* Create the columns defined by the user. */
  for (ulint i{}; i < n_cols; ++i) {
    auto col = table_def->m_cols->at(i);

    table->add_col(col->m_name, col_get_mtype(col), col_get_prtype(col), col->m_len);
  }

  /* Create the table using the prototype in the data dictionary. */
  err = sys.m_dict->m_ddl.create_table(table, ddl_trx);

  table = nullptr;

  if (err == DB_SUCCESS) {

    table = lookup_table_by_name(sys, table_def->m_name);
    ut_a(table != nullptr);

    /* Bump up the reference count, so that another transaction
    doesn't delete it behind our back. */
    sys.m_dict->table_increment_handle_count(table, true);

    err = create_indexes(sys, ddl_trx, table, *table_def->m_indexes);
  }

  /* FIXME: If create_indexes() fails, it's unclear what state
  the data dictionary is in. */

  if (err == DB_SUCCESS) {
    *id = table->m_id;
  }

  if (table != nullptr) {
    sys.m_dict->table_decrement_handle_count(table, true);
  }

  return err;
}

DbErr table_rename(System &sys, Trx *trx, const char *old_name, const char *new_name) noexcept {
  DbErr err;

  if (!schema_lock_is_exclusive(trx)) {
    err = schema_lock_exclusive(sys, trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  auto normalized_old_name = static_cast<char *>(mem_alloc(strlen(old_name) + 1));
  normalize_table_name(normalized_old_name, old_name);

  auto normalized_new_name = static_cast<char *>(mem_alloc(strlen(new_name) + 1));
  normalize_table_name(normalized_new_name, new_name);

  err = sys.m_dict->m_ddl.rename_table(normalized_old_name, normalized_new_name, trx);

  mem_free(normalized_old_name);
  mem_free(normalized_new_name);

  return err;
}

/**
 * @brief Create a primary index.
 *
 * The index id encodes the table id in the high 4 bytes and the index id in the lower 4 bytes.
 *
 * @param[in] idx_sch in: key definition for index
 * @param[out] index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static DbErr create_primary_index(System &sys, IndexSchema *index_def, uint64_t *index_id) {
  auto usr_trx = index_def->m_usr_trx;
  auto table = index_def->m_table;

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(index_def->m_schema == nullptr);
  ut_a(index_def->m_clustered);

  /* Recreate the cluster index and all the secondary indexes
  on a table. If there was a user defined cluster index on the
  table, it will be recreated a secondary index. The InnoDB
  generated cluster index if one exists will be dropped. */

  usr_trx->m_op_info = "recreating clustered index";

  auto heap = mem_heap_create(1024);

  /* This transaction should be the only one operating on the table. */
  ut_a(table->m_n_handles_opened == 1);

  usr_trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(!index_def->m_cols->empty());

  /* Set the CLUSTERED flag to true. */
  copy_index_definition(index_def, true);

  Table *new_table{};
  auto err = row_lock_table_with_retry(sys, usr_trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    err = table_clone(sys, usr_trx, table, &new_table, index_def, heap);
  }

  if (err == DB_SUCCESS) {
    err = row_lock_table_with_retry(sys, usr_trx, new_table, LOCK_X);
  }

  if (err == DB_SUCCESS) {
    err = table_copy(sys, usr_trx, table, new_table, heap);
  }

  if (err == DB_SUCCESS) {
    /* Swap the cloned table with the original table. On success drop
    the original table. */
    auto old_name = table->m_name;
    auto tmp_name = table_create_temp_name(heap, '2', old_name);

    err = row_merge_rename_tables(sys, table, new_table, tmp_name, usr_trx);

    if (err != DB_SUCCESS) {
      row_merge_drop_table(sys, usr_trx, new_table);
    }
  }

  mem_heap_free(heap);

  usr_trx->m_op_info = "";

  return err;
}

/**
 * Create a secondary index.
 *
 * @param[in] idx_sch in: key definition for index
 * @param[out] index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static DbErr create_secondary_index(System &sys, IndexSchema *index_def, uint64_t *index_id) {
  Trx *ddl_trx{};
  Index *index{};
  auto usr_trx = index_def->m_usr_trx;
  auto table = index_def->m_table;

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(index_def->m_schema == nullptr);
  ut_a(!index_def->m_clustered);

  auto err = row_lock_table_with_retry(sys, usr_trx, table, LOCK_S);

  if (err == DB_SUCCESS) {

    /* Since this is part of ALTER TABLE set the create flag to false. */
    err = build_secondary_index(sys, usr_trx, table, index_def, false, &index);

    const auto lock_err = schema_lock_exclusive(sys, usr_trx);
    ut_a(lock_err == DB_SUCCESS);

    if (index != nullptr && err != DB_SUCCESS) {
      row_merge_drop_indexes(sys, usr_trx, table, &index, 1);
      index = nullptr;
    } else {
      bool started;

      ddl_trx = sys.m_trx_sys->create_user_trx(nullptr);
      started = ddl_trx->start(sys, ULINT_UNDEFINED);
      ut_a(started);
    }
  }

  ut_a(ddl_trx != nullptr || err != DB_SUCCESS);

  /* Rename from the TEMP new index to the actual name. */
  if (index != nullptr && err == DB_SUCCESS) {
    err = row_merge_rename_indexes(sys, usr_trx, table);

    if (err != DB_SUCCESS) {
      row_merge_drop_indexes(sys, usr_trx, table, &index, 1);
      index = nullptr;
    }
  }

  if (index != nullptr && err == DB_SUCCESS) {
    /* We only support 32 bit table and index ids. Because
    we need to pack the table id into the index id. */
    ut_a((table->m_id & 0xFFFFFFFF00000000) == 0);
    ut_a((index->m_id & 0xFFFFFFFF00000000) == 0);

    *index_id = table->m_id;
    *index_id <<= 32;
    *index_id |= index->m_id;

    auto err = ddl_trx->commit(sys);
    ut_a(err == DB_SUCCESS);
  } else if (ddl_trx != nullptr) {
    trx_general_rollback(sys, ddl_trx, false, nullptr);
  }

  if (ddl_trx != nullptr) {
    ddl_trx->m_op_info = "";
    sys.m_trx_sys->destroy_user_trx(ddl_trx);
  }

  return err;
}

DbErr IndexSchema::create_index(System &sys, uint64_t *index_id) noexcept {
  auto index_def = this;
  DbErr err;

  if (!schema_lock_is_exclusive(index_def->m_usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (index_def->m_clustered) {
    err = create_primary_index(sys, index_def, index_id);
  } else {
    err = create_secondary_index(sys, index_def, index_id);
  }

  return err;
}

DbErr table_drop(System &sys, Trx *trx, const char *name) noexcept {

  if (!schema_lock_is_exclusive(trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  normalize_table_name(normalized_name, name);

  DbErr err;

  if ((err = sys.m_dict->m_ddl.drop_table(normalized_name, trx, false)) != DB_SUCCESS) {
    log_err("DROP table failed with error ", err, " while dropping table ", normalized_name);
  }

  mem_free(normalized_name);

  return err;
}

DbErr index_drop(System &sys, Trx *trx, uint64_t index_id) noexcept {
  ulint table_id = (ulint)(index_id >> 32);

  if (!schema_lock_is_exclusive(trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = open_table_by_id(sys, table_id, true);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  /* We use only the lower 32 bits of the uint64_t. */
  index_id &= 0xFFFFFFFFULL;

  auto index = table->index_get_on_id(index_id);

  DbErr err;

  if (index != nullptr) {
    err = sys.m_dict->m_ddl.drop_index(table, index, trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  sys.m_dict->table_decrement_handle_count(table, true);

  /* (The caller holds the dictionary mutex, through the schema lock.) */

  return err;
}

}  // namespace api
