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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <ranges>
#include <vector>

#include "api0api.h"
#include "btr0blob.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0dict.h"
#include "innodb0types.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "pars0pars.h"
#include "rem0cmp.h"
#include "row0ins.h"
#include "row0merge.h"
#include "row0misc.h"
#include "row0pread.h"
#include "row0prebuilt.h"
#include "row0sel.h"
#include "row0upd.h"
#include "row0vers.h"
#include "srv0srv.h"
#include "trx0roll.h"
#include "ut0counter.h"
#include "ut0dbg.h"
#include "ut0logger.h"
#include "ut0ucode.h"

namespace api {

static const char *GEN_CLUST_INDEX = "GEN_CLUST_INDEX";

using index_def_t = MergeIndexDef;
using index_field_t = MergeIndexField;

/* The following counter is used to convey information to InnoDB
about server activity: in selects it is not sensible to call
srv_active_wake_master_thread after each fetch or search, we only do
it every INNOBASE_WAKE_INTERVAL'th step. */
constexpr ulint INNOBASE_WAKE_INTERVAL = 32;

/** Check if the Innodb persistent cursor is positioned.
 *
 * @param[in] pcur                 InnoDB persistent cursor
 *
 * @return	true if positioned
 */
static bool ib_btr_cursor_is_positioned(const BtreePcursor *pcur) noexcept {
  return pcur->m_old_stored &&
         (pcur->m_pos_state == BtrPcurPositioned::IS_POSITIONED || pcur->m_pos_state == BtrPcurPositioned::WAS_POSITIONED);
}

/** Delays an INSERT, DELETE or UPDATE operation if the purge is lagging. */
static void ib_delay_dml_if_needed(System &sys) {
  if (sys.m_srv->m_dml_needed_delay) {
    os_thread_sleep(sys.m_srv->m_dml_needed_delay);
  }
}

/**
 * Open a table using the table id, if found then increment table ref count.
 *
 * @param tid - table id to lookup
 * @param locked - true if own dict mutex
 * @return table instance if found
 */
static Table *ib_open_table_by_id(System &sys, uint64_t tid, bool locked) noexcept {
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
static Table *ib_open_table_by_name(System &sys, const char *name) {
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
static Table *ib_lookup_table_by_name(System &sys, const char *name) {
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
static void ib_wake_master_thread(System &sys) noexcept {
  if ((++sys.m_srv->m_n_api_ops % INNOBASE_WAKE_INTERVAL) == 0) {
    sys.m_srv->active_wake_master_thread();
  }
}

#if 0

/** Calculate the length of the column data less trailing space. */
static
ulint ib_varchar_len(const Dtype*	dtype, byte*	ptr, ulint len) {
	/* Handle UCS2 strings differently. */
	ulint		mbminlen = dtype_get_mbminlen(dtype);

	if (mbminlen == 2) {
		/* SPACE = 0x0020. */
		/* Trim "half-chars", just in case. */
		len &= ~1;

		while (len >= 2 && ptr[len - 2] == 0x00 && ptr[len - 1] == 0x20) {

			len -= 2;
		}

	} else {
		ut_a(mbminlen == 1);

		/* SPACE = 0x20. */
		while (len > 0 && ptr[len - 1] == 0x20) {

			--len;
		}
	}

	return len;
}

/**
 * Calculate the max row size of the columns in a cluster index.
 *
 * @return	max row length
 */
static ulint ib_get_max_row_len(Index *index) {
  ut_a(index->is_clustered());

  ulint max_len{};
  const ulint n_fields = index->get_n_fields();

  /* Add the size of the ordering columns in the
  clustered index. */
  for (ulint i{}; i < n_fields; ++i) {
    auto col = index->get_nth_col(i);

    /* Use the maximum output size of
    mach_write_compressed(), although the encoded
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
static void ib_read_tuple(System &sys, const Rec &rec, Tuple *tuple) noexcept {
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
static Tuple *ib_key_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap) {

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

  return (Tuple *)tuple;
}

/**
 * Create an InnoDB key tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of user-defined columns.
 *
 * @return Tuple instance created, or nullptr.
 */
static Tuple *ib_key_tuple_new(const Index *index, ulint n_cols) noexcept {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return ib_key_tuple_new_low(index, n_cols, heap);
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
static Tuple *ib_row_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap) noexcept {
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

  return (Tuple *)tuple;
}

/**
 * Create an InnoDB row tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of columns in the tuple.
 *
 * @return Tuple instance created, or nullptr.
 */
static Tuple *ib_row_tuple_new(const Index *index, ulint n_cols) noexcept {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return ib_row_tuple_new_low(index, n_cols, heap);
}

uint64_t api_version() noexcept {
  return ((uint64_t)IB_API_VERSION_CURRENT << 32 | (IB_API_VERSION_REVISION << 16) | IB_API_VERSION_AGE);
}

DbErr trx_start(System &sys, Trx *ib_trx, TrxLevel ib_trx_level) noexcept {
  DbErr err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut_a(ib_trx_level >= IB_TRX_READ_UNCOMMITTED);
  ut_a(ib_trx_level <= IB_TRX_SERIALIZABLE);

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    auto started = trx->start(sys, ULINT_UNDEFINED);
    ut_a(started);

    trx->m_isolation_level = static_cast<TrxIsolation>(ib_trx_level);
  } else {
    err = DB_ERROR;
  }

  trx->m_client_ctx = nullptr;

  return err;
}

void trx_set_client_data(Trx *ib_trx, void *client_data) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  trx->m_client_ctx = client_data;
}

Trx *trx_begin(System &sys, TrxLevel ib_trx_level) noexcept {
  auto trx = sys.m_trx_sys->create_user_trx(nullptr);
  auto started = trx_start(sys, (Trx *)trx, ib_trx_level);

  ut_a(started);

  return reinterpret_cast<Trx *>(trx);
}

TrxState trx_state(Trx *ib_trx) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  return static_cast<TrxState>(trx->m_conc_state);
}

DbErr trx_release(System &sys, Trx *ib_trx) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut_ad(trx != nullptr);
  sys.m_trx_sys->destroy_user_trx(trx);

  return DB_SUCCESS;
}

DbErr trx_commit(System &sys, Trx *ib_trx) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  auto err = trx->commit(sys);
  ut_a(err == DB_SUCCESS);

  err = schema_unlock(sys, ib_trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = trx_release(sys, ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread(sys);

  return DB_SUCCESS;
}

DbErr trx_rollback(System &sys, Trx *ib_trx) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  auto err = trx_general_rollback(sys, trx, false, nullptr);

  /* It should always succeed */
  ut_a(err == DB_SUCCESS);

  err = schema_unlock(sys, ib_trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = trx_release(sys, ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread(sys);

  return err;
}

/**
 * Check that the combination of values and name makes sense.
 *
 * @param[in] name The column name.
 * @param[in] ib_col_type The column type.
 * @param[in] ib_col_attr The column attribute.
 * @param[in] len The length of the column.
 *
 * @return True if the combination is valid, false otherwise.
 */
static bool ib_check_col_is_ok(const char *name, ColType ib_col_type, ColAttr ib_col_attr, ulint len) {
  if (strlen(name) > IB_MAX_COL_NAME_LEN) {
    return false;
  } else if ((ib_col_type == IB_VARCHAR || ib_col_type == IB_CHAR || ib_col_type == IB_BINARY) && len == 0) {
    return false;
  } else if (ib_col_type == IB_INT) {
    switch (len) {
      case 1:
      case 2:
      case 4:
      case 8:
        break;
      default:
        return false;
    }
  } else if (ib_col_type == IB_FLOAT && len != 4) {
    return false;
  } else if (ib_col_type == IB_DOUBLE && len != 8) {
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
static const IndexSchema *ib_table_find_index(const std::vector<IndexSchema *> &indexes, const char *name) {
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
 * @param[in] ib_col The column definition.
 *
 * @return The precise type in API format.
 */
static ulint ib_col_get_prtype(const Col *ib_col) {
  ulint prtype = 0;

  if (ib_col->m_col_attr & IB_COL_UNSIGNED) {
    prtype |= DATA_UNSIGNED;

    ut_a(ib_col->m_col_type == IB_INT);
  }

  if (ib_col->m_col_attr & IB_COL_NOT_NULL) {
    prtype |= DATA_NOT_NULL;
  }

  if (ib_col->m_col_attr & IB_COL_CUSTOM1) {
    prtype |= DATA_CUSTOM_TYPE;
  }

  if (ib_col->m_col_attr & IB_COL_CUSTOM2) {
    prtype |= (DATA_CUSTOM_TYPE << 1);
  }

  if (ib_col->m_col_attr & IB_COL_CUSTOM3) {
    prtype |= (DATA_CUSTOM_TYPE << 2);
  }

  return prtype;
}

/**
 * Get the InnoDB internal main type from the schema column definition.
 *
 * @param[in] ib_col The column list head
 *
 * @return The main type.
 */
static ulint ib_col_get_mtype(const Col *ib_col) noexcept {
  /* Note: The api0api.h types should map directly to the internal numeric codes. */
  return ib_col->m_col_type;
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
static const Col *ib_table_find_col(const std::vector<Col *> &cols, const char *name) noexcept {
  for (const auto ib_col : cols) {

    if (ib_utf8_strcasecmp(ib_col->m_name, name) == 0) {
      return ib_col;
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
static const KeyCol *ib_index_find_col(const std::vector<KeyCol *> &cols, const char *name) noexcept {
  for (auto ib_col : cols) {

    if (ib_utf8_strcasecmp(ib_col->m_name, name) == 0) {
      return ib_col;
    }
  }

  return nullptr;
}

DbErr TableSchema::add_col(const char *name, ColType ib_col_type, ColAttr ib_col_attr, uint16_t client_type, ulint len) noexcept {
  auto ib_tbl_sch = this;
  DbErr err = DB_SUCCESS;
  TableSchema *table_def = (TableSchema *)ib_tbl_sch;

  if (table_def->m_table != nullptr) {
    err = DB_ERROR;
  } else if (ib_table_find_col(*table_def->m_cols, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else if (!ib_check_col_is_ok(name, ib_col_type, ib_col_attr, len)) {
    err = DB_ERROR;
  } else {
    auto heap = table_def->m_heap;

    auto ib_col = reinterpret_cast<Col *>(mem_heap_zalloc(heap, sizeof(Col)));

    if (ib_col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->m_name = mem_heap_strdup(heap, name);
      ib_col->m_col_type = ib_col_type;
      ib_col->m_col_attr = ib_col_attr;

      ib_col->m_len = len;

      table_def->m_cols->emplace_back(ib_col);
    }
  }

  return err;
}

DbErr TableSchema::add_index(const char *name, IndexSchema **ib_idx_sch) noexcept {
  auto ib_tbl_sch = this;
  DbErr err = DB_SUCCESS;
  auto table_def = reinterpret_cast<TableSchema *>(ib_tbl_sch);

  if (table_def->m_table != nullptr) {
    err = DB_ERROR;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  }
  if (ib_table_find_index(*table_def->m_indexes, name) != nullptr) {
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

      *ib_idx_sch = (IndexSchema *)index_def;
    }
  }

  return err;
}

void TableSchema::destroy(System &sys, TableSchema *ib_tbl_sch) noexcept {
  TableSchema *table_def = (TableSchema *)ib_tbl_sch;

  /* Check that all indexes are owned by the table schema. */
  for (auto index_def : *table_def->m_indexes) {
    ut_a(index_def->m_schema != nullptr);
  }

  if (table_def->m_table != nullptr) {
    sys.m_dict->table_decrement_handle_count(table_def->m_table, false);
  }

  delete table_def->m_cols;
  delete table_def->m_indexes;

  mem_heap_free(table_def->m_heap);
}

static DbErr ib_table_schema_check(TableFormat ib_tbl_fmt, ulint *page_size) {
  DbErr err = DB_SUCCESS;

  *page_size = 0;

  switch (*page_size) {
    case 0:
      /* The page size value will be ignored for uncompressed tables. */
      /* Set to the system default of 8K page size.  Better to be conservative
     * here. */
      *page_size = 8;
      break;
    case 4:
    case 8:
    case 16:
    default:
      /* Unknown page size. */
      err = DB_UNSUPPORTED;
      break;
  }

  return err;
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
static void ib_normalize_table_name(char *norm_name, const char *name) {
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
static DbErr ib_table_name_check(const char *name) noexcept {
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

DbErr TableSchema::create(const char *name, TableSchema **ib_tbl_sch, TableFormat ib_tbl_fmt, ulint page_size) noexcept {

  auto err = ib_table_name_check(name);

  if (err != DB_SUCCESS) {
    return err;
  }

  err = ib_table_schema_check(ib_tbl_fmt, &page_size);

  if (err != DB_SUCCESS) {
    return err;
  }

  MemHeap *heap = mem_heap_create(1024);

  if (heap == nullptr) {
    err = DB_OUT_OF_MEMORY;
  } else {
    auto table_def = (TableSchema *)mem_heap_zalloc(heap, sizeof(TableSchema));

    if (table_def == nullptr) {
      err = DB_OUT_OF_MEMORY;

      mem_heap_free(heap);
    } else {
      table_def->m_heap = heap;

      auto normalized_name = mem_heap_strdup(heap, name);

      ib_normalize_table_name(normalized_name, name);

      table_def->m_name = normalized_name;

      table_def->m_page_size = page_size;

      table_def->m_tbl_fmt = ib_tbl_fmt;

      table_def->m_cols = new std::vector<Col *>();

      table_def->m_indexes = new std::vector<IndexSchema *>();

      *ib_tbl_sch = (TableSchema *)table_def;
    }
  }

  return err;
}

/**
 * Get the column number within the index definition.
 *
 * @param[in] ib_index_def The index definition.
 * @param[in] name The column name to search.
 *
 * @return -1 or column number.
 */
static int ib_index_get_col_no(const IndexSchema *ib_index_def, const char *name) {
  int col_no;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->m_table != nullptr) {
    col_no = ib_index_def->m_table->get_col_no(name);
  } else {
    const auto ib_col = ib_table_find_col(*ib_index_def->m_schema->m_cols, name);

    if (ib_col != nullptr) {
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
 * @param[in] ib_index_def The index definition.
 * @param[in] name The column name to check.
 *
 * @return True if allowed.
 */
static int ib_index_is_prefix_allowed(const IndexSchema *ib_index_def, const char *name) noexcept {
  bool allowed = true;
  ulint mtype = ULINT_UNDEFINED;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->m_table != nullptr) {
    auto table = ib_index_def->m_table;
    auto col_no = table->get_col_no(name);
    ut_a(col_no != -1);

    const auto col = table->get_nth_col(col_no);
    ut_a(col != nullptr);

    mtype = col->mtype;
  } else {
    const Col *ib_col;

    ib_col = ib_table_find_col(*ib_index_def->m_schema->m_cols, name);

    ut_a(ib_col != nullptr);

    mtype = (ulint)ib_col->m_col_type;
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
  auto ib_idx_sch = this;
  DbErr err = DB_SUCCESS;
  IndexSchema *index_def = (IndexSchema *)ib_idx_sch;

  /* Check for duplicates. */
  if (ib_index_find_col(*index_def->m_cols, name) != nullptr) {
    err = DB_COL_APPEARS_TWICE_IN_INDEX;
    /* Check if the column exists in the table definition. */
  } else if (ib_index_get_col_no(index_def, name) == -1) {
    err = DB_NOT_FOUND;
    /* Some column types can't have prefix length indexes. */
  } else if (prefix_len > 0 && !ib_index_is_prefix_allowed(index_def, name)) {
    err = DB_SCHEMA_ERROR;
  } else {
    auto heap = index_def->m_heap;
    auto ib_col = reinterpret_cast<KeyCol *>(mem_heap_zalloc(heap, sizeof(KeyCol)));

    if (ib_col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->m_name = mem_heap_strdup(heap, name);
      ib_col->m_prefix_len = prefix_len;

      index_def->m_cols->emplace_back(ib_col);
    }
  }

  return err;
}

DbErr IndexSchema::create(
  System &sys, Trx *ib_usr_trx, const char *name, const char *table_name, IndexSchema **ib_idx_sch
) noexcept {
  DbErr err = DB_SUCCESS;

  if (!schema_lock_is_exclusive(ib_usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  ib_normalize_table_name(normalized_name, table_name);

  auto table = ib_lookup_table_by_name(sys, normalized_name);

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

        index_def->m_usr_trx = reinterpret_cast<Trx *>(ib_usr_trx);

        *ib_idx_sch = reinterpret_cast<IndexSchema *>(index_def);
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
static IndexSchema *ib_find_clustered_index(const std::vector<IndexSchema *> &indexes) {
  for (const auto ib_index_def : indexes) {
    if (ib_index_def->m_clustered) {
      return ib_index_def;
    }
  }

  return nullptr;
}

DbErr IndexSchema::set_clustered() noexcept {
  auto ib_idx_sch = this;
  DbErr err = DB_SUCCESS;

  auto index_def = reinterpret_cast<IndexSchema *>(ib_idx_sch);

  /* If this index schema is part of a table schema then we need
  to check the state of the other indexes. */

  if (index_def->m_schema != nullptr) {
    auto ib_clust_index_def = ib_find_clustered_index(*index_def->m_schema->m_indexes);

    if (ib_clust_index_def != nullptr) {
      ut_a(ib_clust_index_def->m_clustered);
      ib_clust_index_def->m_clustered = false;
    }
  }

  index_def->m_unique = true;
  index_def->m_clustered = true;

  return err;
}

DbErr IndexSchema::set_unique() noexcept {
  auto ib_idx_sch = this;
  DbErr err = DB_SUCCESS;

  auto index_def = (IndexSchema *)ib_idx_sch;
  index_def->m_unique = true;

  return err;
}

void IndexSchema::destroy(IndexSchema *ib_idx_sch) noexcept {
  auto index_def = (IndexSchema *)ib_idx_sch;

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
static ulint ib_table_def_get_flags(const TableSchema *table_def) {
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
 * @param[in] ib_index_def Key definition for index.
 * @param[in] clustered True if clustered index.
 *
 * @return Converted index definition.
 */
static const index_def_t *ib_copy_index_definition(IndexSchema *ib_index_def, bool clustered) {
  auto index_def = reinterpret_cast<index_def_t *>(mem_heap_zalloc(ib_index_def->m_heap, sizeof(index_def_t)));
  auto name_len = strlen(ib_index_def->m_name);
  auto index_name = reinterpret_cast<char *>(mem_heap_zalloc(ib_index_def->m_heap, name_len + 2));

  /* The TEMP_INDEX_PREFIX is only needed if we are rebuilding an
  index or creating a new index on a table that has records. If
  the definition is owned by a table schema then we can be sure that
  this index definition is part of a CREATE TABLE. */
  if (ib_index_def->m_schema == nullptr) {
    *index_name = TEMP_INDEX_PREFIX;
    memcpy(index_name + 1, ib_index_def->m_name, name_len + 1);
  } else {
    memcpy(index_name, ib_index_def->m_name, name_len);
  }

  index_def->name = index_name;
  index_def->n_fields = ib_index_def->m_cols->size();

  if (ib_index_def->m_unique) {
    index_def->ind_type = DICT_UNIQUE;
  } else {
    index_def->ind_type = 0;
  }

  if (clustered) {
    index_def->ind_type |= DICT_CLUSTERED;
  }

  index_def->fields = (index_field_t *)mem_heap_zalloc(ib_index_def->m_heap, sizeof(index_field_t) * index_def->n_fields);

  for (ulint i = 0; i < ib_index_def->m_cols->size(); ++i) {
    auto ib_col = ib_index_def->m_cols->at(i);

    index_def->fields[i].field_name = ib_col->m_name;
    index_def->fields[i].prefix_len = ib_col->m_prefix_len;
  }

  return index_def;
}

/**
 * (Re)Create a secondary index.
 *
 * @param[in] usr_trx The transaction.
 * @param[in] table The parent table of the index.
 * @param[in] ib_index_def The index definition.
 * @param[in] create True if part of table create.
 * @param[out] index The index created.
 *
 * @return DB_SUCCESS or err code.
 */
static DbErr ib_build_secondary_index(
  System &sys, Trx *usr_trx, Table *table, IndexSchema *ib_index_def, bool create, Index **index
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
  const auto index_def = ib_copy_index_definition(ib_index_def, false);

  ut_a(!(index_def->ind_type & DICT_CLUSTERED));

  ddl_trx->m_op_info = "creating secondary index";

  auto trx{reinterpret_cast<Trx *>(usr_trx)};

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
    err = schema_unlock(sys, (Trx *)usr_trx);
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
static char *ib_table_create_temp_name(MemHeap *heap, char id, const char *table_name) noexcept {
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
static void ib_index_create_def(const Index *index, index_def_t *index_def, MemHeap *heap) noexcept {
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
static index_def_t *ib_table_create_index_defs(System &sys, Trx *trx, const Table *table, MemHeap *heap, ulint *n_indexes) noexcept {
  auto sz = sizeof(index_def_t) * UT_LIST_GET_LEN(table->m_indexes);
  auto index_defs = reinterpret_cast<index_def_t *>(mem_heap_zalloc(heap, sz));
  auto err = schema_lock_exclusive(sys, (Trx *)trx);

  ut_a(err == DB_SUCCESS);

  auto index = table->get_first_index();

  /* Skip a generated cluster index. */
  if (ib_utf8_strcasecmp(index->m_name, GEN_CLUST_INDEX) == 0) {
    ut_a(index->get_nth_col(0)->mtype == DATA_SYS);
    index = index->get_next();
  }

  while (index != nullptr) {
    ib_index_create_def(index, index_defs++, heap);
    index = index->get_next();
  }

  err = schema_unlock(sys, (Trx *)trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  return index_defs;
}

/**
 * Create a cluster index specified by the user. The cluster index
 * shouldn't already exist.
 *
 * @param[in] trx in: transaction
 * @param[in] table in: parent table of index
 * @param[in] ib_index_def in: index definition
 * @param[out] index out: index created
 *
 * @return DB_SUCCESS or err code
 */
static DbErr ib_create_cluster_index(System &sys, Trx *trx, Table *table, IndexSchema *ib_index_def, Index **index) noexcept {

  ut_a(!ib_index_def->m_cols->empty());

  /* Set the CLUSTERED flag to true. */
  auto index_def = ib_copy_index_definition(ib_index_def, true);

  trx->m_op_info = "creating clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  auto err = ib_trx_lock_table_with_retry(sys, trx, table, LOCK_X);

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
static DbErr ib_table_clone_indexes(System &sys, Trx *trx, Table *src_table, Table *new_table, MemHeap *heap) noexcept {
  ulint n_index_defs = 0;
  auto index_defs = ib_table_create_index_defs(sys, trx, src_table, heap, &n_index_defs);

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
 * @param[in] ib_index_def in: new cluster index definition
 * @param[in] heap in: memory heap to use
 *
 * @return DB_SUCCESS or error code
 */
static DbErr ib_table_clone(
  System &sys, Trx *trx, Table *src_table, Table **new_table, IndexSchema *ib_index_def, MemHeap *heap
) noexcept {
  auto new_table_name = ib_table_create_temp_name(heap, '1', src_table->m_name);
  auto err = schema_lock_exclusive(sys, (Trx *)trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  /* Set the CLUSTERED flag to true. */
  auto index_def = ib_copy_index_definition(ib_index_def, true);

  /* Create the new table and the cluster index. */
  *new_table = row_merge_create_temporary_table(sys, new_table_name, index_def, src_table, trx);

  if (unlikely(new_table == nullptr)) {
    err = trx->m_error_state;
  } else {
    trx->m_table_id = (*new_table)->m_id;

    err = ib_table_clone_indexes(sys, trx, src_table, *new_table, heap);
  }

  err = schema_unlock(sys, (Trx *)trx);
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
static DbErr ib_table_copy(System &sys, Trx *trx, Table *src_table, Table *dst_table, MemHeap *heap) noexcept {
  auto err = schema_lock_exclusive(sys, (Trx *)trx);

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

  err = schema_unlock(sys, (Trx *)trx);
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
static DbErr ib_create_default_cluster_index(System &sys, Trx *trx, Table *table, Index **index) noexcept {
  index_def_t index_def;

  index_def.n_fields = 0;
  index_def.fields = nullptr;
  index_def.name = GEN_CLUST_INDEX;
  index_def.ind_type = DICT_CLUSTERED;

  trx->m_op_info = "creating default clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(schema_lock_is_exclusive((Trx *)trx));

  auto err = ib_trx_lock_table_with_retry(sys, trx, table, LOCK_X);

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
static DbErr ib_create_indexes(System &sys, Trx *ddl_trx, Table *table, const std::vector<IndexSchema *> &indexes) noexcept {
  DbErr err = DB_ERROR;
  Index *index = nullptr;
  IndexSchema *ib_clust_index_def = nullptr;

  auto n_indexes = indexes.size();

  if (n_indexes > 0) {
    ib_clust_index_def = ib_find_clustered_index(indexes);

    if (ib_clust_index_def != nullptr) {
      ut_a(ib_clust_index_def->m_clustered);

      err = ib_create_cluster_index(sys, ddl_trx, table, ib_clust_index_def, &index);
    }
  }

  if (ib_clust_index_def == nullptr) {
    err = ib_create_default_cluster_index(sys, ddl_trx, table, &index);
  }

  for (ulint i{}; err == DB_SUCCESS && i < n_indexes; ++i) {
    auto ib_index_def = indexes.at(i);

    ut_a(!ib_index_def->m_cols->empty());

    if (!ib_index_def->m_clustered) {
      /* Since this is part of CREATE TABLE, set the create flag to true. */
      err = ib_build_secondary_index(sys, ddl_trx, table, ib_index_def, true, &index);
    } else {
      /* There can be at most one cluster definition. */
      ut_a(ib_clust_index_def == ib_index_def);
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
static DbErr ib_table_get_id_low(System &sys, const char *table_name, uint64_t *table_id) noexcept {
  DbErr err = DB_TABLE_NOT_FOUND;

  *table_id = 0;

  auto table = ib_lookup_table_by_name(sys, table_name);

  if (table != nullptr) {
    *table_id = table->m_id;

    err = DB_SUCCESS;
  }

  return err;
}

DbErr table_create(System &sys, Trx *ib_trx, const TableSchema *ib_tbl_sch, uint64_t *id) noexcept {
  auto ddl_trx = (Trx *)ib_trx;
  const auto table_def = (const TableSchema *)ib_tbl_sch;

  /* Another thread may have created the table already when we get
  here. We need to search the data dictionary before we attempt to
  create the table. */

  if (!schema_lock_is_exclusive((Trx *)ddl_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto err = ib_table_get_id_low(sys, table_def->m_name, id);

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
  for (auto ib_index_def : *table_def->m_indexes) {

    /* Check that the index definition has at least one column. */
    if (ib_index_def->m_cols->empty()) {
      return DB_SCHEMA_ERROR;
    }

    /* Check for duplicate cluster definitions. */
    if (ib_index_def->m_clustered) {
      ++n_cluster;

      if (n_cluster > 1) {
        return DB_SCHEMA_ERROR;
      }
    }
  }

  /* Create the table prototype. */
  auto table =
    Table::create(table_def->m_name, SYS_TABLESPACE, n_cols, ib_table_def_get_flags(table_def), false, Current_location());

  /* Create the columns defined by the user. */
  for (ulint i{}; i < n_cols; ++i) {
    auto ib_col = table_def->m_cols->at(i);

    table->add_col(ib_col->m_name, ib_col_get_mtype(ib_col), ib_col_get_prtype(ib_col), ib_col->m_len);
  }

  /* Create the table using the prototype in the data dictionary. */
  err = sys.m_dict->m_ddl.create_table(table, ddl_trx);

  table = nullptr;

  if (err == DB_SUCCESS) {

    table = ib_lookup_table_by_name(sys, table_def->m_name);
    ut_a(table != nullptr);

    /* Bump up the reference count, so that another transaction
    doesn't delete it behind our back. */
    sys.m_dict->table_increment_handle_count(table, true);

    err = ib_create_indexes(sys, ddl_trx, table, *table_def->m_indexes);
  }

  /* FIXME: If ib_create_indexes() fails, it's unclear what state
  the data dictionary is in. */

  if (err == DB_SUCCESS) {
    *id = table->m_id;
  }

  if (table != nullptr) {
    sys.m_dict->table_decrement_handle_count(table, true);
  }

  return err;
}

DbErr table_rename(System &sys, Trx *ib_trx, const char *old_name, const char *new_name) noexcept {
  DbErr err;
  Trx *trx = (Trx *)ib_trx;

  if (!schema_lock_is_exclusive(ib_trx)) {
    err = schema_lock_exclusive(sys, ib_trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  auto normalized_old_name = static_cast<char *>(mem_alloc(strlen(old_name) + 1));
  ib_normalize_table_name(normalized_old_name, old_name);

  auto normalized_new_name = static_cast<char *>(mem_alloc(strlen(new_name) + 1));
  ib_normalize_table_name(normalized_new_name, new_name);

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
 * @param[in] ib_idx_sch in: key definition for index
 * @param[out] index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static DbErr ib_create_primary_index(System &sys, IndexSchema *ib_idx_sch, uint64_t *index_id) {
  auto ib_index_def = (IndexSchema *)ib_idx_sch;
  auto usr_trx = ib_index_def->m_usr_trx;
  auto table = ib_index_def->m_table;

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->m_schema == nullptr);
  ut_a(ib_index_def->m_clustered);

  /* Recreate the cluster index and all the secondary indexes
  on a table. If there was a user defined cluster index on the
  table, it will be recreated a secondary index. The InnoDB
  generated cluster index if one exists will be dropped. */

  usr_trx->m_op_info = "recreating clustered index";

  auto heap = mem_heap_create(1024);

  /* This transaction should be the only one operating on the table. */
  ut_a(table->m_n_handles_opened == 1);

  usr_trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(!ib_index_def->m_cols->empty());

  /* Set the CLUSTERED flag to true. */
  ib_copy_index_definition(ib_index_def, true);

  Table *new_table{};
  auto err = ib_trx_lock_table_with_retry(sys, usr_trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    err = ib_table_clone(sys, usr_trx, table, &new_table, ib_index_def, heap);
  }

  if (err == DB_SUCCESS) {
    err = ib_trx_lock_table_with_retry(sys, usr_trx, new_table, LOCK_X);
  }

  if (err == DB_SUCCESS) {
    err = ib_table_copy(sys, usr_trx, table, new_table, heap);
  }

  if (err == DB_SUCCESS) {
    /* Swap the cloned table with the original table. On success drop
    the original table. */
    auto old_name = table->m_name;
    auto tmp_name = ib_table_create_temp_name(heap, '2', old_name);

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
 * @param[in] ib_idx_sch in: key definition for index
 * @param[out] index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static DbErr ib_create_secondary_index(System &sys, IndexSchema *ib_idx_sch, uint64_t *index_id) {
  Trx *ddl_trx{};
  Index *index{};
  auto ib_index_def = reinterpret_cast<IndexSchema *>(ib_idx_sch);
  auto usr_trx = ib_index_def->m_usr_trx;
  auto table = ib_index_def->m_table;

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->m_schema == nullptr);
  ut_a(!ib_index_def->m_clustered);

  auto err = ib_trx_lock_table_with_retry(sys, usr_trx, table, LOCK_S);

  if (err == DB_SUCCESS) {

    /* Since this is part of ALTER TABLE set the create flag to false. */
    err = ib_build_secondary_index(sys, usr_trx, table, ib_index_def, false, &index);

    err = schema_lock_exclusive(sys, (Trx *)usr_trx);
    ut_a(err == DB_SUCCESS);

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
  auto ib_idx_sch = this;
  DbErr err;
  IndexSchema *ib_index_def = (IndexSchema *)ib_idx_sch;

  if (!schema_lock_is_exclusive((Trx *)ib_index_def->m_usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (ib_index_def->m_clustered) {
    err = ib_create_primary_index(sys, ib_idx_sch, index_id);
  } else {
    err = ib_create_secondary_index(sys, ib_idx_sch, index_id);
  }

  return err;
}

DbErr table_drop(System &sys, Trx *ib_trx, const char *name) noexcept {

  if (!schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  auto trx = reinterpret_cast<Trx *>(ib_trx);

  DbErr err;

  if ((err = sys.m_dict->m_ddl.drop_table(normalized_name, trx, false)) != DB_SUCCESS) {
    log_err("DROP table failed with error ", err, " while dropping table ", normalized_name);
  }

  mem_free(normalized_name);

  return err;
}

DbErr index_drop(System &sys, Trx *ib_trx, uint64_t index_id) noexcept {
  ulint table_id = (ulint)(index_id >> 32);

  if (!schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = ib_open_table_by_id(sys, table_id, true);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  /* We use only the lower 32 bits of the uint64_t. */
  index_id &= 0xFFFFFFFFULL;

  auto index = table->index_get_on_id(index_id);

  DbErr err;

  if (index != nullptr) {
    err = sys.m_dict->m_ddl.drop_index(table, index, (Trx *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  sys.m_dict->table_decrement_handle_count(table, false);

  return err;
}

/**
 * Create an internal cursor instance.
 *
 * @param[out] ib_crsr out: InnoDB cursor
 * @param[in] table in: table instance
 * @param[in] index_id in: index id or 0
 * @param[in] trx in: transaction
 *
 * @return DB_SUCCESS or err code
 */
static DbErr ib_create_cursor(System &sys, Cursor **ib_crsr, Table *table, uint64_t index_id, Trx *trx) noexcept {
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

  *ib_crsr = (Cursor *)cursor;

  return err;
}

DbErr cursor_open_table_using_id(System &sys, uint64_t table_id, Trx *ib_trx, Cursor **ib_crsr) noexcept {
  Table *table;

  if (ib_trx == nullptr || !schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(sys, table_id, false);
  } else {
    table = ib_open_table_by_id(sys, table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  return ib_create_cursor(sys, ib_crsr, table, 0, (Trx *)ib_trx);
}

DbErr cursor_open_index_using_id(System &sys, uint64_t index_id, Trx *ib_trx, Cursor **ib_crsr) noexcept {
  Table *table;
  ulint table_id = (ulint)(index_id >> 32);

  if (ib_trx == nullptr || !schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(sys, table_id, false);
  } else {
    table = ib_open_table_by_id(sys, table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  /* We only return the lower 32 bits of the uint64_t. */
  auto err = ib_create_cursor(sys, ib_crsr, table, index_id & 0xFFFFFFFFULL, (Trx *)ib_trx);

  if (ib_crsr != nullptr) {
    auto cursor = *(Cursor **)ib_crsr;

    if (cursor->m_prebuilt->m_index == nullptr) {
      auto crsr_err = Cursor::close(sys, *ib_crsr);
      ut_a(crsr_err == DB_SUCCESS);

      *ib_crsr = nullptr;
    }
  }

  return err;
}

DbErr Cursor::open_index(System &sys, const char *index_name, Cursor **ib_crsr) noexcept {
  auto ib_open_crsr = this;
  Table *table;
  uint64_t index_id = 0;
  DbErr err = DB_TABLE_NOT_FOUND;
  auto cursor = reinterpret_cast<Cursor *>(ib_open_crsr);
  auto trx = cursor->m_prebuilt->m_trx;

  if (trx != nullptr && !schema_lock_is_exclusive((Trx *)trx)) {
    sys.m_dict->mutex_acquire();
  }

  /* We want to increment the ref count, so we do a redundant search. */
  table = sys.m_dict->table_get_using_id(sys.m_config.m_recv.m_force_recovery, cursor->m_prebuilt->m_table->m_id, true);
  ut_a(table != nullptr);

  if (trx != nullptr && !schema_lock_is_exclusive((Trx *)trx)) {
    sys.m_dict->mutex_release();
  }

  /* The first index is always the cluster index. */
  for (auto index : table->m_indexes) {
    if (strcmp(index->m_name, index_name) == 0) {
      index_id = index->m_id;
    }
  }

  *ib_crsr = nullptr;

  if (index_id > 0) {
    err = ib_create_cursor(sys, ib_crsr, table, index_id, cursor->m_prebuilt->m_trx);
  }

  if (*ib_crsr != nullptr) {
    const Cursor *cursor;

    cursor = *(Cursor **)ib_crsr;

    if (cursor->m_prebuilt->m_index == nullptr) {
      err = Cursor::close(sys, *ib_crsr);
      ut_a(err == DB_SUCCESS);
      *ib_crsr = nullptr;
    }
  } else {
    /* The dictionary mutex is held here only under the exclusive schema lock. */
    sys.m_dict->table_decrement_handle_count(table, trx != nullptr && schema_lock_is_exclusive(trx));
  }

  return err;
}

DbErr cursor_open_table(System &sys, const char *name, Trx *ib_trx, Cursor **ib_crsr) noexcept {
  DbErr err;
  Table *table;

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  if (ib_trx != nullptr) {
    if (!schema_lock_is_exclusive(ib_trx)) {
      table = ib_open_table_by_name(sys, normalized_name);
    } else {
      table = ib_lookup_table_by_name(sys, normalized_name);

      if (table != nullptr) {
        sys.m_dict->table_increment_handle_count(table, true);
      }
    }
  } else {
    table = ib_open_table_by_name(sys, normalized_name);
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
    err = ib_create_cursor(sys, ib_crsr, table, 0, (Trx *)ib_trx);
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
static void ib_qry_proc_free(QueryProc *q_proc) {
  que_graph_free_recursive(q_proc->m_grph.m_ins);
  que_graph_free_recursive(q_proc->m_grph.m_upd);
  que_graph_free_recursive(q_proc->m_grph.m_sel);

  *q_proc = QueryProc{};
}

DbErr Cursor::reset(System &sys) noexcept {
  auto ib_crsr = this;
  Cursor *cursor = (Cursor *)ib_crsr;
  Prebuilt *prebuilt = cursor->m_prebuilt;

  if (prebuilt->m_trx != nullptr && prebuilt->m_trx->m_n_client_tables_in_use > 0) {

    --prebuilt->m_trx->m_n_client_tables_in_use;
  }

  /* The fields in this data structure are allocated from
  the query heap and so need to be reset too. */
  ib_qry_proc_free(&cursor->m_q_proc);

  mem_heap_empty(cursor->m_query_heap);

  prebuilt->clear();

  return DB_SUCCESS;
}

DbErr Cursor::close(System &sys, Cursor *ib_crsr) noexcept {
  Cursor *cursor = (Cursor *)ib_crsr;
  Prebuilt *prebuilt = cursor->m_prebuilt;
  Trx *trx = prebuilt->m_trx;

  ib_qry_proc_free(&cursor->m_q_proc);

  /* The transaction could have been detached from the cursor. */
  if (trx != nullptr && trx->m_n_client_tables_in_use > 0) {
    --trx->m_n_client_tables_in_use;
  }

  if (trx != nullptr && schema_lock_is_exclusive((Trx *)trx)) {
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
static DbErr ib_insert_row_with_lock_retry(System &sys, QueThr *thr, InsertNode *node, TrxSavept *savept) noexcept {
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
      lock_wait = ib_handle_errors(sys, &err, trx, thr, savept);
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
static DbErr ib_execute_insert_query_graph(System &sys, Table *table, QueFork *ins_graph, InsertNode *node) noexcept {
  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed(sys);

  auto trx = ins_graph->trx;
  auto savept = trx_savept_take(trx);
  auto thr = que_fork_get_first_thr(ins_graph);

  que_thr_move_to_run_state(thr);

  auto err = ib_insert_row_with_lock_retry(sys, thr, node, &savept);

  if (err == DB_SUCCESS) {
    que_thr_stop_for_client_no_error(thr, trx);

    ++table->m_stats.m_n_rows;

    sys.m_srv->m_stats.m_n_rows_inserted++;

    ib_update_statistics_if_needed(sys, table);

    ib_wake_master_thread(sys);
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create an insert query graph node.
 *
 * @param[in] cursor in: Cursor instance
 */
static void ib_insert_query_graph_create(System &sys, Cursor *cursor) noexcept {
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

DbErr Cursor::insert_row(System &sys, const Tuple *ib_tpl) noexcept {
  auto ib_crsr = this;
  ulint n_fields;
  QueryNode *node;
  QueryProc *q_proc;
  DTuple *dst_dtuple;
  DbErr err = DB_SUCCESS;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  const auto src_tuple = reinterpret_cast<const Tuple *>(ib_tpl);

  ib_insert_query_graph_create(sys, cursor);

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
    err = ib_execute_insert_query_graph(sys, src_tuple->m_index->m_table, q_proc->m_grph.m_ins, node->m_ins);
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
static Upd *ib_update_vector_create(System &sys, Cursor *cursor) {
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
static void ib_update_col(Cursor *cursor, UpdField *upd_field, ulint col_no, Dfield *dfield) {
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
static DbErr ib_calc_diff(Cursor *cursor, Upd *upd, const Tuple *old_tuple, const Tuple *new_tuple) noexcept {
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

      ib_update_col(cursor, upd_field, i, new_dfield);

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
static DbErr ib_update_row_with_lock_retry(System &sys, QueThr *thr, UpdNode *node, TrxSavept *savept) noexcept {
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

        lock_wait = ib_handle_errors(sys, &err, trx, thr, savept);

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
static DbErr ib_execute_update_query_graph(System &sys, Cursor *cursor, BtreePcursor *pcur) noexcept {
  auto trx = cursor->m_prebuilt->m_trx;
  auto table = cursor->m_prebuilt->m_table;
  auto q_proc = &cursor->m_q_proc;

  /* The transaction must be running. */
  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = q_proc->m_node.m_upd;

  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed(sys);

  ut_a(pcur->get_index()->is_clustered());
  node->m_pcur->copy_stored_position(pcur);

  ut_a(node->m_pcur->get_rel_pos() == BtreeCursorPos::ON);

  auto savept = trx_savept_take(trx);

  auto thr = que_fork_get_first_thr(q_proc->m_grph.m_upd);

  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  que_thr_move_to_run_state(thr);

  auto err = ib_update_row_with_lock_retry(sys, thr, node, &savept);

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

    ib_update_statistics_if_needed(sys, table);

  } else if (err == DB_RECORD_NOT_FOUND) {
    trx->m_error_state = DB_SUCCESS;
  }

  ib_wake_master_thread(sys);

  trx->m_op_info = "";

  return err;
}

DbErr Cursor::update_row(System &sys, const Tuple *ib_old_tpl, const Tuple *ib_new_tpl) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;
  const auto old_tuple = reinterpret_cast<const Tuple *>(ib_old_tpl);
  const auto new_tuple = reinterpret_cast<const Tuple *>(ib_new_tpl);

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

  auto upd = ib_update_vector_create(sys, cursor);
  auto err = ib_calc_diff(cursor, upd, old_tuple, new_tuple);

  if (err == DB_SUCCESS) {
    /* Note that this is not a delete. */
    cursor->m_q_proc.m_node.m_upd->m_is_delete = false;

    err = ib_execute_update_query_graph(sys, cursor, pcur);
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
static DbErr ib_delete_row(System &sys, Cursor *cursor, BtreePcursor *pcur, const Rec &rec) {
  auto table = cursor->m_prebuilt->m_table;
  auto index = table->get_first_index();

  auto n_cols = index->get_n_ordering_defined_by_user();
  auto ib_tpl = ib_key_tuple_new(index, n_cols);

  if (ib_tpl == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto tuple = ib_tpl;
  auto upd = ib_update_vector_create(sys, cursor);

  ib_read_tuple(sys, rec, tuple);

  upd->m_n_fields = ib_tpl->get_n_cols();

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

  auto err = ib_execute_update_query_graph(sys, cursor, pcur);

  return err;
}

DbErr Cursor::delete_row(System &sys) noexcept {
  auto ib_crsr = this;
  DbErr err;
  BtreePcursor *pcur;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
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

  if (ib_btr_cursor_is_positioned(pcur)) {
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
      err = ib_delete_row(sys, cursor, pcur, Rec(rec));
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    err = DB_RECORD_NOT_FOUND;
  }

  return err;
}

DbErr Cursor::read_row(System &sys, Tuple *ib_tpl) noexcept {
  auto ib_crsr = this;
  DbErr err;
  auto tuple = ib_tpl;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

  ut_a(cursor->m_prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  /* When searching with IB_EXACT_MATCH set, row_search_mvcc()
  will not position the persistent cursor but will copy the record
  found into the row cache. It should be the only entry. */
  if (!ib_crsr->is_positioned() && cursor->m_prebuilt->m_row_cache.is_cache_empty()) {
    err = DB_RECORD_NOT_FOUND;
  } else if (!cursor->m_prebuilt->m_row_cache.is_cache_empty()) {
    auto rec = cursor->m_prebuilt->m_row_cache.cache_get_row();
    ut_a(!rec.is_null());

    if (!rec.get_deleted_flag()) {
      ib_read_tuple(sys, Rec(rec), tuple);
      err = DB_SUCCESS;
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
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
        ib_read_tuple(sys, Rec(rec), tuple);
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
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  prebuilt->m_row_cache.cache_next();

  return sys.m_row_sel->mvcc_fetch(sys.m_config.m_recv.m_force_recovery, IB_CUR_L, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_PREV);
}

DbErr Cursor::next(System &sys) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
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
static DbErr ib_cursor_position(System &sys, Cursor *cursor, SearchMode mode) {
  auto prebuilt = cursor->m_prebuilt;

  /* We want to position at one of the ends, row_search_mvcc()
  uses the search_tuple fields to work out what to do. */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  return sys.m_row_sel->mvcc_fetch(sys.m_config.m_recv.m_force_recovery, mode, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_MOVETO);
}

DbErr Cursor::first(System &sys) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

  return ib_cursor_position(sys, cursor, IB_CUR_G);
}

DbErr Cursor::last(System &sys) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

  return ib_cursor_position(sys, cursor, IB_CUR_L);
}

DbErr Cursor::moveto(System &sys, Tuple *ib_tpl, SearchMode ib_srch_mode, int *result) noexcept {
  auto ib_crsr = this;
  auto tuple = ib_tpl;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
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
    sys.m_config.m_recv.m_force_recovery, ib_srch_mode, prebuilt, (IbMatch)cursor->m_match_mode, ROW_SEL_MOVETO
  );

  *result = prebuilt->m_result;

  return err;
}

void Cursor::attach_trx(System &sys, Trx *ib_trx) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  ut_a(ib_trx != nullptr);
  ut_a(prebuilt->m_trx == nullptr);

  prebuilt->clear();
  prebuilt->update_trx((Trx *)ib_trx);

  /* Assign a read view if the transaction does not have it yet */
  auto rv = prebuilt->m_trx->assign_read_view(sys);
  ut_a(rv != nullptr);

  ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  ++prebuilt->m_trx->m_n_client_tables_in_use;
}

void Cursor::set_match_mode(MatchMode match_mode) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

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
static const Dfield *ib_col_get_dfield(const Tuple *tuple, ulint col_no) noexcept {
  return dtuple_get_nth_field(tuple->m_ptr, col_no);
}

/** @copydoc ib_col_get_dfield(const Tuple *, ulint) */
static Dfield *ib_col_get_dfield(Tuple *tuple, ulint col_no) noexcept {
  return dtuple_get_nth_field(tuple->m_ptr, col_no);
}

/**
 * @brief Predicate to check whether a column type contains variable length data.
 *
 * @param[in] dtype Column type.
 *
 * @return true if the column type contains variable length data, false otherwise.
 */
static bool ib_col_is_capped(const Dtype *dtype) noexcept {
  return dtype_get_len(dtype) > 0 && (dtype_get_mtype(dtype) == DATA_VARCHAR || dtype_get_mtype(dtype) == DATA_CHAR ||
                                      dtype_get_mtype(dtype) == DATA_CLIENT || dtype_get_mtype(dtype) == DATA_VARCLIENT ||
                                      dtype_get_mtype(dtype) == DATA_FIXBINARY || dtype_get_mtype(dtype) == DATA_BINARY);
}

DbErr Tuple::set_value(ulint col_no, const void *s, ulint len) noexcept {
  auto ib_tpl = this;
  auto src = static_cast<const byte *>(s);
  auto tuple = ib_tpl;

  ut_d(mem_heap_verify(tuple->m_heap));

  auto dfield = ib_col_get_dfield(tuple, col_no);

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
  if (ib_col_is_capped(dtype)) {

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

      memset((byte *)dst + len, pad_char, dtype_get_len(dtype) - len);

      len = dtype_get_len(dtype);
      /* Fall through */
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
  auto ib_tpl = this;
  auto tuple = ib_tpl;

  auto dfield = ib_col_get_dfield(tuple, i);
  auto data_len = dfield_get_len(dfield);

  return data_len == UNIV_SQL_NULL ? IB_SQL_NULL : data_len;
}

/**
 * Copy a column value from the tuple.
 *
 * @param[in] ib_tpl Tuple instance.
 * @param[in] i Column index in tuple.
 * @param[out] dst Copied data value.
 * @param[in] len Max data value len to copy.
 *
 * @return bytes copied or IB_SQL_NULL
 */
static ulint ib_col_copy_value_low(const Tuple *ib_tpl, ulint i, void *dst, ulint len) noexcept {
  auto tuple = ib_tpl;

  auto dfield = ib_col_get_dfield(tuple, i);
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
  auto ib_tpl = this;
  return ib_col_copy_value_low(ib_tpl, i, dst, len);
}

/**
 * Get the InnoDB column attribute from the internal column precise type.
 *
 * @param[in] prtype Column definition.
 *
 * @return Precise type in API format.
 */
static ColAttr ib_col_get_attr(ulint prtype) {
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
 * @param[in] ib_tpl Tuple instance.
 * @param[in] i Column index in tuple.
 * @param[out] ib_col_meta Column meta data.
 *
 * @return Length of column data.
 */
static ulint ib_col_get_meta_low(const Tuple *ib_tpl, ulint i, ColMeta *ib_col_meta) noexcept {
  auto tuple = ib_tpl;

  auto dfield = ib_col_get_dfield(tuple, i);
  auto data_len = dfield_get_len(dfield);

  /* We assume 1-1 mapping between the ENUM and internal type codes. */
  ib_col_meta->type = static_cast<ColType>(dtype_get_mtype(dfield_get_type(dfield)));

  ib_col_meta->type_len = dtype_get_len(dfield_get_type(dfield));

  auto prtype = (uint16_t)dtype_get_prtype(dfield_get_type(dfield));

  ib_col_meta->attr = ib_col_get_attr(prtype);
  ib_col_meta->client_type = prtype & DATA_CLIENT_TYPE_MASK;

  return data_len;
}

/**
 * Read a signed int 8 bit column from an InnoDB tuple.
 *
 * @param[in] ib_tpl Tuple instance.
 * @param[in] i Column number.
 * @param[in] usign True if unsigned.
 * @param[in] size Size of integer.
 *
 * @return DB_SUCCESS or error.
 */
static DbErr ib_tuple_check_int(const Tuple *ib_tpl, ulint i, bool usign, ulint size) noexcept {
  ColMeta ib_col_meta;

  ib_col_get_meta_low(ib_tpl, i, &ib_col_meta);

  if (ib_col_meta.type != IB_INT) {
    return DB_DATA_MISMATCH;
  } else if (ib_col_meta.type_len == IB_SQL_NULL) {
    return DB_UNDERFLOW;
  } else if (ib_col_meta.type_len != size) {
    return DB_DATA_MISMATCH;
  } else if ((ib_col_meta.attr & IB_COL_UNSIGNED) && !usign) {
    return DB_DATA_MISMATCH;
  }

  return DB_SUCCESS;
}

DbErr Tuple::read_i8(ulint i, int8_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u8(ulint i, uint8_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i16(ulint i, int16_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u16(ulint i, uint16_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i32(ulint i, int32_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u32(ulint i, uint32_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_i64(ulint i, int64_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

DbErr Tuple::read_u64(ulint i, uint64_t *ival) const noexcept {
  auto ib_tpl = this;

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

const void *Tuple::get_value(ulint i) const noexcept {
  auto ib_tpl = this;
  auto tuple = ib_tpl;
  auto dfield = ib_col_get_dfield(tuple, i);
  auto data = dfield_get_data(dfield);
  auto data_len = dfield_get_len(dfield);

  return data_len != UNIV_SQL_NULL ? data : nullptr;
}

ulint Tuple::get_meta(ulint i, ColMeta *ib_col_meta) const noexcept {
  auto ib_tpl = this;
  return ib_col_get_meta_low(ib_tpl, i, ib_col_meta);
}

Tuple *Tuple::clear() noexcept {
  auto ib_tpl = this;
  auto tuple = ib_tpl;
  const auto type = tuple->m_type;
  auto heap = tuple->m_heap;

  const auto index = tuple->m_index;
  const auto n_cols = dtuple_get_n_fields(tuple->m_ptr);

  mem_heap_empty(heap);

  if (type == TPL_ROW) {
    return ib_row_tuple_new_low(index, n_cols, heap);
  } else {
    return ib_key_tuple_new_low(index, n_cols, heap);
  }
}

DbErr Cursor::get_cluster_key(Tuple **ib_dst_tpl, const Tuple *ib_src_tpl) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto src_tuple = ib_src_tpl;

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
  *ib_dst_tpl = ib_crsr->clust_search_tuple_create();

  if (!*ib_dst_tpl) {
    return DB_OUT_OF_MEMORY;
  }

  auto dst_tuple = (Tuple *)*ib_dst_tpl;
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

DbErr Tuple::copy(const Tuple *ib_src_tpl) noexcept {
  auto ib_dst_tpl = this;
  const Tuple *src_tuple = (const Tuple *)ib_src_tpl;
  Tuple *dst_tuple = (Tuple *)ib_dst_tpl;

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
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto index = cursor->m_prebuilt->m_index;

  const auto n_cols = index->get_n_unique_in_tree();
  return ib_key_tuple_new(index, n_cols);
}

Tuple *Cursor::sec_read_tuple_create() noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto index = cursor->m_prebuilt->m_index;
  const auto n_cols = index->get_n_fields();

  return ib_row_tuple_new(index, n_cols);
}

Tuple *Cursor::clust_search_tuple_create() noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto index = cursor->m_prebuilt->m_table->get_clustered_index();
  const auto n_cols = index->get_n_ordering_defined_by_user();

  return ib_key_tuple_new(index, n_cols);
}

Tuple *Cursor::clust_read_tuple_create() noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

  auto index = cursor->m_prebuilt->m_table->get_clustered_index();

  const auto n_cols = cursor->m_prebuilt->m_table->get_n_cols();
  return ib_row_tuple_new(index, n_cols);
}

ulint Tuple::get_n_user_cols() const noexcept {
  auto ib_tpl = this;
  const auto tuple = ib_tpl;

  if (tuple->m_type == TPL_ROW) {
    return tuple->m_index->m_table->get_n_user_cols();
  } else {
    return tuple->m_index->get_n_ordering_defined_by_user();
  }
}

ulint Tuple::get_n_cols() const noexcept {
  auto ib_tpl = this;
  const auto tuple = ib_tpl;

  return dtuple_get_n_fields(tuple->m_ptr);
}

void Tuple::destroy(Tuple *ib_tpl) noexcept {
  Tuple *tuple = (Tuple *)ib_tpl;

  mem_heap_free(tuple->m_heap);
}

DbErr cursor_truncate(System &sys, Cursor **ib_crsr, uint64_t *table_id) noexcept {
  auto cursor = *reinterpret_cast<Cursor **>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  ut_a(schema_lock_is_exclusive((Trx *)prebuilt->m_trx));

  *table_id = 0;

  auto err = (*ib_crsr)->lock(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    Trx *trx;
    Table *table = prebuilt->m_table;

    /* We are going to free the cursor and the prebuilt. Store
    the transaction handle locally. */
    trx = prebuilt->m_trx;
    err = Cursor::close(sys, *ib_crsr);
    ut_a(err == DB_SUCCESS);

    *ib_crsr = nullptr;

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

  auto ib_trx = trx_begin(sys, IB_TRX_SERIALIZABLE);

  sys.m_dict->mutex_acquire();

  auto table = sys.m_dict->table_get(table_name);

  DbErr err;
  Cursor *ib_crsr{};

  if (table != nullptr && table->get_first_index()) {
    sys.m_dict->table_increment_handle_count(table, true);
    err = ib_create_cursor(sys, &ib_crsr, table, 0, reinterpret_cast<Trx *>(ib_trx));
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  sys.m_dict->mutex_release();

  if (err == DB_SUCCESS) {
    err = schema_lock_exclusive(sys, ib_trx);
  }

  DbErr trunc_err;

  if (err == DB_SUCCESS) {
    trunc_err = cursor_truncate(sys, &ib_crsr, table_id);
    ut_a(err == DB_SUCCESS);
  } else {
    trunc_err = err;
  }

  if (ib_crsr != nullptr) {
    err = Cursor::close(sys, ib_crsr);
    ut_a(err == DB_SUCCESS);
  }

  if (trunc_err == DB_SUCCESS) {
    ut_a(trx_state(ib_trx) == IB_TRX_NOT_STARTED);

    err = schema_unlock(sys, ib_trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

    err = trx_release(sys, ib_trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = trx_rollback(sys, ib_trx);
    ut_a(err == DB_SUCCESS);
  }

  return trunc_err;
}

DbErr table_get_id(System &sys, const char *table_name, uint64_t *table_id) noexcept {

  sys.m_dict->mutex_acquire();

  auto err = ib_table_get_id_low(sys, table_name, table_id);

  sys.m_dict->mutex_release();

  return err;
}

DbErr index_get_id(System &sys, const char *table_name, const char *index_name, uint64_t *index_id) noexcept {
  DbErr err = DB_TABLE_NOT_FOUND;

  *index_id = 0;

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  ib_normalize_table_name(normalized_name, table_name);

  auto table = ib_lookup_table_by_name(sys, normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table != nullptr) {
    auto index = table->get_index_on_name(index_name);

    if (index != nullptr) {
      /* We only support 32 bit table and index ids. Because
      we need to pack the table id into the index id. */
      ut_a(table->m_id == 0);
      ut_a(index->m_id == 0);

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
  Trx *ib_trx;
  ulint len = strlen(dbname);

  if (len == 0) {
    return DB_INVALID_INPUT;
  }

  auto ptr = reinterpret_cast<char *>(mem_alloc(len + 2));

  memset(ptr, 0x0, len + 2);
  strcpy(ptr, dbname);

  ib_trx = trx_begin(sys, IB_TRX_SERIALIZABLE);

  /* Drop all the tables in the database first. */
  /* ddl_drop_database() expects a string that ends in '/'. */
  if (ptr[len - 1] != '/') {
    ptr[len] = '/';
  }

  auto err = sys.m_dict->m_ddl.drop_database(ptr, (Trx *)ib_trx);

  /* Only necessary if file per table is set. */
  if (err == DB_SUCCESS && sys.m_config.m_fil.m_file_per_table) {
    sys.m_fil->rmdir(ptr);
  }

  mem_free(ptr);

  if (err == DB_SUCCESS) {
    auto trx_err = trx_commit(sys, ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  } else {
    auto trx_err = trx_rollback(sys, ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  }

  return err;
}

bool Cursor::is_positioned() const noexcept {
  auto ib_crsr = this;
  const Cursor *cursor = (const Cursor *)ib_crsr;
  const Prebuilt *prebuilt = cursor->m_prebuilt;

  return ib_btr_cursor_is_positioned(prebuilt->m_pcur);
}

DbErr schema_lock_shared(System &sys, Trx *ib_trx) noexcept {
  DbErr err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_S_LATCH) {

    sys.m_dict->freeze_data_dictionary(trx);
  }

  return err;
}

DbErr schema_lock_exclusive(System &sys, Trx *ib_trx) noexcept {
  DbErr err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH) {

    sys.m_dict->lock_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

bool schema_lock_is_exclusive(const Trx *ib_trx) noexcept {
  const auto trx = reinterpret_cast<const Trx *>(ib_trx);

  return trx->m_dict_operation_lock_mode == RW_X_LATCH;
}

bool schema_lock_is_shared(const Trx *ib_trx) noexcept {
  const auto trx = reinterpret_cast<const Trx *>(ib_trx);

  return trx->m_dict_operation_lock_mode == RW_S_LATCH;
}

DbErr schema_unlock(System &sys, Trx *ib_trx) noexcept {
  DbErr err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  if (trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    sys.m_dict->unlock_data_dictionary(trx);
  } else if (trx->m_dict_operation_lock_mode == RW_S_LATCH) {
    sys.m_dict->unfreeze_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

DbErr Cursor::lock(System &sys, CursorLockMode ib_lck_mode) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;
  auto trx = prebuilt->m_trx;
  auto table = prebuilt->m_table;

  return ib_trx_lock_table_with_retry(sys, trx, table, LockMode(ib_lck_mode));
}

DbErr table_lock(System &sys, Trx *ib_trx, uint64_t table_id, CursorLockMode ib_lck_mode) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto table = ib_open_table_by_id(sys, table_id, false);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  ut_a(ib_lck_mode <= (CursorLockMode)LOCK_NUM);

  auto heap = mem_heap_create(128);

  QueryProc q_proc;

  q_proc.m_node.m_sel = SelNode::create(heap);

  auto thr = pars_complete_graph_for_exec(q_proc.m_node.m_sel, trx, heap);

  q_proc.m_grph.m_sel = static_cast<QueFork *>(que_node_get_parent(thr));
  q_proc.m_grph.m_sel->state = QUE_FORK_ACTIVE;

  trx->m_op_info = "setting table lock";

  ut_a(ib_lck_mode == IB_LOCK_IS || ib_lck_mode == IB_LOCK_IX);

  auto err = sys.m_lock_sys->lock_table(0, table, (enum LockMode)ib_lck_mode, thr);

  trx->m_error_state = err;

  sys.m_dict->table_decrement_handle_count(table, false);

  mem_heap_free(heap);

  return err;
}

DbErr ib_cursor_unlock(Cursor *ib_crsr) {
  DbErr err;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  if (prebuilt->m_trx->m_client_n_tables_locked > 0) {
    --prebuilt->m_trx->m_client_n_tables_locked;
    err = DB_SUCCESS;
  } else {
    err = DB_ERROR;
  }

  return err;
}

DbErr Cursor::set_lock_mode(System &sys, CursorLockMode ib_lck_mode) noexcept {
  auto ib_crsr = this;
  DbErr err = DB_SUCCESS;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  ut_a(ib_lck_mode <= (CursorLockMode)LOCK_NUM);

  if (ib_lck_mode == IB_LOCK_X) {
    err = ib_crsr->lock(sys, IB_LOCK_IX);
  } else {
    err = ib_crsr->lock(sys, IB_LOCK_IS);
  }

  if (err == DB_SUCCESS) {
    prebuilt->m_select_lock_type = (enum LockMode)ib_lck_mode;
    ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);
  }

  return err;
}

void Cursor::set_cluster_access() noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  prebuilt->m_need_to_access_clustered = true;
}

void Cursor::set_simple_select() noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;

  prebuilt->m_simple_select = true;
}

void savepoint_take(Trx *ib_trx, const void *name, ulint name_len) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

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

DbErr savepoint_release(Trx *ib_trx, const void *name, ulint name_len) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

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

DbErr savepoint_rollback(System &sys, Trx *ib_trx, const void *name, ulint name_len) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

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

  TrxNamedSavept *savep;

  if (name == nullptr || name_len == 0) {
    auto it = std::find_if(trx->m_trx_savepoints.begin(), trx->m_trx_savepoints.end(), [](const auto &savep) { return true; });

    if (it == trx->m_trx_savepoints.end()) {
      return DB_NO_SAVEPOINT;
    }

    savep = *it;

  } else {
    savep = trx->m_trx_savepoints.front();
  }

  trx->m_trx_savepoints.remove(savep);
  mem_free(savep);

  /* We can now free all savepoints strictly later than this one */
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
 * @param[out] page_size - out: page size
 */
static void ib_table_get_format(const Table *table, TableFormat *tbl_fmt, ulint *page_size) noexcept {
  *page_size = 0;

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
static int ib_table_schema_visit_table_columns(const Table *table, SchemaVisitor::table_col_t table_col, void *arg) {
  for (ulint i{}; i < table->get_n_cols(); ++i) {
    auto col = table->get_nth_col(i);
    auto col_no = col->get_no();
    auto name = table->get_col_name(col_no);
    auto attr = ib_col_get_attr(col->prtype);
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
static int ib_table_schema_visit_index_columns(const Index *index, SchemaVisitor::index_col_t index_col, void *arg) noexcept {
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

DbErr table_schema_visit(System &sys, Trx *ib_trx, const char *name, const SchemaVisitor *visitor, void *arg) noexcept {
  int user_err{};

  if (!schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  auto table = ib_lookup_table_by_name(sys, normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table != nullptr) {
    sys.m_dict->table_increment_handle_count(table, true);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  ulint page_size;
  TableFormat tbl_fmt;

  ib_table_get_format(table, &tbl_fmt, &page_size);

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
    user_err = visitor->table(arg, table->m_name, tbl_fmt, page_size, table->m_n_cols, n_indexes);

    if (user_err > 0) {
      goto func_exit;
    }
  }

  if (visitor->version < SchemaVisitor::Version::TABLE_COL) {

    goto func_exit;

  } else if (visitor->table_col) {
    user_err = ib_table_schema_visit_table_columns(table, visitor->table_col, arg);

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
      user_err = visitor->index(arg, index->m_name, index->is_unique(), index->is_clustered(), n_index_cols);
      if (user_err > 0) {
        goto func_exit;
      }

      if (visitor->version >= SchemaVisitor::Version::TABLE_AND_INDEX_COL && visitor->index_col) {
        user_err = ib_table_schema_visit_index_columns(index, visitor->index_col, arg);

        if (user_err > 0) {
          break;
        }
      }
    }

    index = index->get_next();

  } while (index != nullptr);

func_exit:
  ut_a(schema_lock_is_exclusive(ib_trx));
  sys.m_dict->table_decrement_handle_count(table, true);

  return user_err != 0 ? DB_ERROR : DB_SUCCESS;
}

DbErr schema_tables_iterate(System &sys, Trx *ib_trx, SchemaVisitorTableAll visitor, void *arg) noexcept {
  DbErr err;
  Cursor *ib_crsr;
  DbErr crsr_err;
  Tuple *ib_tpl{};

  if (!schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = ib_lookup_table_by_name(sys, "SYS_TABLES");

  if (table != nullptr) {
    sys.m_dict->table_increment_handle_count(table, true);
    err = ib_create_cursor(sys, &ib_crsr, table, 0, (Trx *)ib_trx);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  if (err == DB_SUCCESS) {
    err = ib_crsr->first(sys);
  }

  ib_tpl = ib_crsr->clust_read_tuple_create();

  while (err == DB_SUCCESS) {
    const void *ptr;
    ColMeta ib_col_meta;

    err = ib_crsr->read_row(sys, ib_tpl);

    if (err == DB_SUCCESS) {
      ulint len;

      ptr = ib_tpl->get_value(0);
      /* Can't have nullptr columns. */
      ut_a(ptr != nullptr);

      len = ib_col_get_meta_low(ib_tpl, 0, &ib_col_meta);
      ut_a(len != UNIV_SQL_NULL);

      if (visitor(arg, (const char *)ptr, len)) {
        break;
      }

      err = ib_crsr->next(sys);
    }
  }

  crsr_err = Cursor::close(sys, ib_crsr);
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
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);

  cursor->m_prebuilt->m_sql_stat_start = true;
}

DbErr Tuple::write_double(int col_no, double val) noexcept {
  auto ib_tpl = this;
  auto tuple = ib_tpl;

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    return set_value(col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

DbErr Tuple::read_double(ulint col_no, double *dval) const noexcept {
  auto ib_tpl = this;
  DbErr err;
  auto tuple = ib_tpl;
  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    ib_col_copy_value_low(ib_tpl, col_no, dval, sizeof(*dval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

DbErr Tuple::write_float(int col_no, float val) noexcept {
  auto ib_tpl = this;
  auto tuple = ib_tpl;
  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    return set_value(col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

DbErr Tuple::read_float(ulint col_no, float *fval) const noexcept {
  auto ib_tpl = this;
  DbErr err;
  auto tuple = ib_tpl;

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    ib_col_copy_value_low(ib_tpl, col_no, fval, sizeof(*fval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

DbErr get_duplicate_key(Trx *ib_trx, const char **table_name, const char **index_name) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  if (trx->m_error_info == nullptr) {
    return DB_ERROR;
  }

  *table_name = trx->m_error_info->get_table_name();
  *index_name = trx->m_error_info->m_name;

  return DB_SUCCESS;
}

DbErr Cursor::get_table_statistics(System &sys, TableStats *table_stats) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto table = cursor->m_prebuilt->m_table;

  if (!table->m_stats.m_initialized) {
    sys.m_dict->update_statistics(table);
  }

  table_stats->m_stat_n_rows = table->m_stats.m_n_rows;
  table_stats->m_stat_clustered_index_size = table->m_stats.m_clustered_index_size * UNIV_PAGE_SIZE;
  table_stats->m_stat_sum_of_other_index_sizes = table->m_stats.m_sum_of_secondary_index_sizes * UNIV_PAGE_SIZE;
  table_stats->m_stat_modified_counter = table->m_stats.m_modified_counter;

  return DB_SUCCESS;
}

DbErr Cursor::get_index_stat_n_diff_key_vals(System &sys, const char *index_name, uint64_t *ncols, int64_t **n_diff) noexcept {
  auto ib_crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto table = cursor->m_prebuilt->m_table;

  if (!table->m_stats.m_initialized) {
    sys.m_dict->update_statistics(table);
  }

  auto index = table->get_index_on_name(index_name);

  if (index == nullptr) {
    return DB_NOT_FOUND;
  }

  *ncols = index->m_n_uniq;

  *n_diff = (int64_t *)malloc(sizeof(int64_t) * index->m_n_uniq);

  sys.m_dict->index_stat_mutex_enter(index);

  memcpy(*n_diff, index->m_stats.m_n_diff_key_vals, sizeof(int64_t) * index->m_n_uniq);

  sys.m_dict->index_stat_mutex_exit(index);

  return DB_SUCCESS;
}

DbErr Cursor::update_table_statistics(System &sys) noexcept {
  auto crsr = this;
  auto cursor = reinterpret_cast<Cursor *>(crsr);
  auto table = cursor->m_prebuilt->m_table;

  sys.m_dict->update_statistics(table);

  return DB_SUCCESS;
}

DbErr parallel_select_count_star(
  System &sys, Trx *ib_trx, std::vector<Cursor *> &ib_crsrs, size_t n_threads, uint64_t &n_rows
) noexcept {
  ut_a(n_threads > 1);
  ut_a(!ib_crsrs.empty());

  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut::Sharded_counter<ParallelReader::MAX_THREADS> n_recs{};

  n_recs.clear();

  const ParallelReader::ScanRange full_scan;

  DbErr err{DB_SUCCESS};
  ParallelReader reader(n_threads);
  std::vector<Table *> tables{};

  for (auto &ib_crsr : ib_crsrs) {
    auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
    ParallelReader::Config config(full_scan, cursor->m_prebuilt->m_index);

    err = reader.add_scan(sys, trx, config, [&](const ParallelReader::Ctx *ctx) {
      n_recs.inc(1, ctx->thread_id());
      return DB_SUCCESS;
    });

    if (err != DB_SUCCESS) {
      break;
    }
  }

  n_threads = ParallelReader::available_threads(sys, n_threads, false);

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
  using Blocks = std::vector<const BufBlock *>;

  Heaps heaps;
  Tuples prev_tuples;
  Blocks prev_blocks;

  for (size_t i = 0; i < n_threads; ++i) {
    heaps.push_back(mem_heap_create(4096));
  }

  ParallelReader reader(n_threads);
  ParallelReader::ScanRange full_scan;
  ParallelReader::Config config(full_scan, index);

  auto err = reader.add_scan(sys, trx, config, [&](const ParallelReader::Ctx *ctx) {
    const auto rec = ctx->m_rec;
    const auto block = ctx->m_block;
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

    if (prev_blocks[id] != block || prev_blocks[id] == nullptr) {
      mem_heap_empty(heap);

      PhyRec record{index, rec};

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());

      prev_blocks[id] = block;
    }

    ulint n_ext{};

    prev_tuples[id] = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

    return DB_SUCCESS;
  });

  if (err == DB_SUCCESS) {
    prev_tuples.resize(n_threads);
    prev_blocks.resize(n_threads);

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

DbErr check_table(System &sys, Trx **ib_trx, Cursor *ib_crsr, size_t n_threads) noexcept {
  auto trx = reinterpret_cast<Trx *>(ib_trx);
  auto cursor = reinterpret_cast<Cursor *>(ib_crsr);
  auto prebuilt = cursor->m_prebuilt;
  auto index = prebuilt->m_index;

  ut_a(prebuilt->m_trx->m_conc_state == TRX_NOT_STARTED);

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

  if (trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && prebuilt->m_select_lock_type == LOCK_NONE && index->is_clustered() &&
      trx->m_client_n_tables_locked == 0) {
    n_threads = ParallelReader::available_threads(sys, n_threads, false);

    auto rv = trx->assign_read_view(sys);
    ut_a(rv != nullptr);

    auto trx = prebuilt->m_trx;

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
