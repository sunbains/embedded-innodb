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
#include "api0misc.h"
#include "api0ucode.h"
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

extern ib_panic_handler_t ib_panic;

static const char *GEN_CLUST_INDEX = "GEN_CLUST_INDEX";

/* Protected by the schema lock */
struct ib_db_format_t {
  /** Numeric representation of database format */
  ulint id;

  /** Text representation of name, allocated using
  ut_new() and should be automatically freed at
  InnoDB shutdown */
  const char *name;
};

/** This value is read at database startup. */
static ib_db_format_t db_format;

/**
 * Does a simple memcmp(3).
 *
 * @param col_meta   in: column meta data
 * @param p1         in: packed key
 * @param p1_len     in: packed key length
 * @param p2         in: packed key
 * @param p2_len     in: packed key length
 * @return           1, 0, -1, if a is greater, equal, less than b, respectively
 */
static int ib_default_compare(const ib_col_meta_t *col_meta, const ib_byte_t *p1, ulint p1_len, const ib_byte_t *p2, ulint p2_len);

ib_client_cmp_t ib_client_compare = ib_default_compare;

/* InnoDB tuple types. */
enum ib_tuple_type_t {
  /** Data row tuple */
  TPL_ROW,

  /** Index key tuple */
  TPL_KEY
};

/* Query types supported. */
enum ib_qry_type_t {
  /** None/Sentinel */
  QRY_NON,

  /** Insert operation */
  QRY_INS,

  /** Update operation */
  QRY_UPD,

  /** Select operation */
  QRY_SEL
};

/* Query graph types. */
struct ib_qry_grph_t {
  /** Innobase SQL query graph used in inserts */
  que_fork_t *ins;

  /** SQL query graph used in updates or deletes */
  que_fork_t *upd;

  /* Dummy query graph used in selects */
  que_fork_t *sel;
};

/* Query node types. */
struct ib_qry_node_t {
  /** SQL insert node used to perform inserts to the table */
  ins_node_t *ins;

  /** SQL update node used to perform updates and deletes */
  upd_node_t *upd;

  /** SQL select node used to perform selects on the table */
  sel_node_t *sel;
};

/* Query processing fields. */
struct ib_qry_proc_t {
  /** Query node */
  ib_qry_node_t node;

  /** Query graph */
  ib_qry_grph_t grph;
};

/* Cursor instance for traversing tables/indexes. This will eventually
become Prebuilt. */
struct ib_cursor_t {
  /** Instance heap */
  mem_heap_t *heap;

  /** Heap to use for query graphs */
  mem_heap_t *query_heap;

  /** Query processing info */
  ib_qry_proc_t q_proc;

  /** ib_cursor_moveto match mode */
  ib_match_mode_t match_mode;

  /** For reading rows */
  Prebuilt *prebuilt;
};

/* InnoDB table columns used during table and index schema creation. */
struct ib_col_t {
  /** Name of column */
  const char *name;

  /** Main type of the column */
  ib_col_type_t ib_col_type;

  /** Length of the column */
  ulint len;

  /** Column attributes */
  ib_col_attr_t ib_col_attr;
};

/* InnoDB index columns used during index and index schema creation. */
struct ib_key_col_t {
  /** Name of column */
  const char *name;

  /** Column index prefix len or 0 */
  ulint prefix_len;
};

struct ib_table_def_t;

/* InnoDB index schema used during index creation */
struct ib_index_def_t {
  /** Heap used to build this and all its columns in the list */
  mem_heap_t *heap;

  /** Index name */
  const char *name;

  /** Parent InnoDB table */
  Table *table;

  /** Parent table schema that owns this instance */
  ib_table_def_t *schema;

  /** True if clustered index */
  bool clustered;

  /** True if unique index */
  bool unique;

  /** Vector of columns */
  std::vector<ib_key_col_t *> *cols;

  /* User transacton covering the DDL operations */
  Trx *usr_trx;
};

/* InnoDB table schema used during table creation */
struct ib_table_def_t {
  /** Heap used to build this and all its columns in the list */
  mem_heap_t *heap;

  /** Table name */
  const char *name;

  /** Row format */
  ib_tbl_fmt_t ib_tbl_fmt;

  /** Page size */
  ulint page_size;

  /** Vector of columns */
  std::vector<ib_col_t *> *cols;

  /** Vector of indexes */
  std::vector<ib_index_def_t *> *indexes;

  /** Table read from or nullptr */
  Table *table;
};

/* InnoDB tuple used for key operations. */
struct ib_tuple_t {
  /** Heap used to build this and for copying the column values. */
  mem_heap_t *heap;

  /** Tuple discriminitor. */
  ib_tuple_type_t type;

  /** Index for tuple can be either secondary or cluster index. */
  const Index *index;

  /** The internal tuple instance */
  DTuple *ptr;
};

using index_def_t = merge_index_def_t;
using index_field_t = merge_index_field_t;

/* The following counter is used to convey information to InnoDB
about server activity: in selects it is not sensible to call
srv_active_wake_master_thread after each fetch or search, we only do
it every INNOBASE_WAKE_INTERVAL'th step. */
constexpr ulint INNOBASE_WAKE_INTERVAL = 32;

/**
 * Does a simple memcmp(3).
 *
 * @param[in] ib_col_meta Column meta data
 * @param[in] p1 Packed key
 * @param[in] p1_len Packed key length
 * @param[in] p2 Packed key
 * @param[in] p2_len Packed key length
 *
 * @return 1 if a is greater, 0 if equal, -1 if less than b
 */
static int ib_default_compare(
  const ib_col_meta_t *ib_col_meta, const ib_byte_t *p1, ulint p1_len, const ib_byte_t *p2, ulint p2_len
) {
  (void)ib_col_meta;

  auto ret = memcmp(p1, p2, std::min<ulint>(p1_len, p2_len));

  if (ret == 0) {
    ret = p1_len - p2_len;
  }

  return ret < 0 ? -1 : ((ret > 0) ? 1 : 0);
}

/** Check if the Innodb persistent cursor is positioned.
 *
 * @param[in] pcur                 InnoDB persistent cursor
 *
 * @return	true if positioned
 */
static bool ib_btr_cursor_is_positioned(const Btree_pcursor *pcur) noexcept {
  return pcur->m_old_stored &&
         (pcur->m_pos_state == Btr_pcur_positioned::IS_POSITIONED || pcur->m_pos_state == Btr_pcur_positioned::WAS_POSITIONED);
}

/** Delays an INSERT, DELETE or UPDATE operation if the purge is lagging. */
static void ib_delay_dml_if_needed() {
  if (srv_dml_needed_delay) {
    os_thread_sleep(srv_dml_needed_delay);
  }
}

/**
 * Open a table using the table id, if found then increment table ref count.
 *
 * @param tid - table id to lookup
 * @param locked - true if own dict mutex
 * @return table instance if found
 */
static Table *ib_open_table_by_id(ib_id_t tid, bool locked) noexcept {
  /* We only return the lower 32 bits of the uint64_t. */
  ut_a(tid < 0xFFFFFFFFULL);
  auto table_id = tid;

  if (!locked) {
    srv_dict_sys->mutex_acquire();
  }

  auto table = srv_dict_sys->table_get_using_id(srv_config.m_force_recovery, Dict_id{table_id}, true);

  if (table != nullptr && table->m_ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.", table->m_name);

    srv_dict_sys->table_decrement_handle_count(table, true);

    table = nullptr;
  }

  if (!locked) {
    srv_dict_sys->mutex_release();
  }

  return table;
}

/**
 * Open a table using the table name, if found then increment table ref count.
 *
 * @param[in] name  Table name
 *
 * @return	table instance if found */
static Table *ib_open_table_by_name(const char *name) {
  auto table = srv_dict_sys->table_get(name, true);

  if (table != nullptr && table->m_ibd_file_missing) {

    log_warn(std::format("The .ibd file for table {} is missing.", name));

    srv_dict_sys->table_decrement_handle_count(table, false);

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
static Table *ib_lookup_table_by_name(const char *name) {
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

  auto table = srv_dict_sys->table_get(name);

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
static void ib_wake_master_thread() noexcept {
  static ulint ib_signal_counter = 0;

  ++ib_signal_counter;

  if ((ib_signal_counter % INNOBASE_WAKE_INTERVAL) == 0) {
    InnoDB::active_wake_master_thread();
  }
}

#if 0

/** Calculate the length of the column data less trailing space. */
static
ulint ib_varchar_len(const dtype_t*	dtype, ib_byte_t*	ptr, ulint len) {
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
static void ib_read_tuple(const Rec &rec, ib_tuple_t *tuple) noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  DTuple *dtuple = tuple->ptr;
  const Index *dindex = tuple->index;

  rec_offs_init(offsets_);

  {
    Phy_rec record{dindex, rec};

    offsets = record.get_all_col_offsets(offsets, &tuple->heap, Current_location());
  }

  auto rec_meta_data = rec.get_info_bits();
  dtuple_set_info_bits(dtuple, rec_meta_data);

  /* Make a copy of the rec. */
  auto ptr = reinterpret_cast<void *>(mem_heap_alloc(tuple->heap, rec_offs_size(offsets)));
  auto copy = rec.copy(ptr, offsets);

  /* Avoid a debug assertion in rec.offs_validate(). */
  ut_d(rec.offs_make_valid(dindex, (ulint *)offsets));

  auto n_index_fields = std::min<ulint>(rec_offs_n_fields(offsets), dtuple_get_n_fields(dtuple));

  for (ulint i{}; i < n_index_fields; ++i) {
    dfield_t *dfield;

    if (tuple->type == TPL_ROW) {
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

      Blob blob(srv_fsp, srv_btree_sys);

      data = blob.copy_externally_stored_field(copy, offsets, i, &len, tuple->heap);

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
static ib_tpl_t ib_key_tuple_new_low(const Index *index, ulint n_cols, mem_heap_t *heap) {

  auto tuple = reinterpret_cast<ib_tuple_t *>(mem_heap_alloc(heap, sizeof(ib_tuple_t)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->heap = heap;
  tuple->index = index;
  tuple->type = TPL_KEY;

  /* Is it a generated clustered index ? */
  if (n_cols == 0) {
    ++n_cols;
  }

  tuple->ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  index->copy_types(tuple->ptr, n_cols);

  for (ulint i = 0; i < n_cols; i++) {

    dfield_t *dfield;

    dfield = dtuple_get_nth_field(tuple->ptr, i);
    dfield_set_null(dfield);
  }

  auto n_cmp_cols = index->get_n_ordering_defined_by_user();

  dtuple_set_n_fields_cmp(tuple->ptr, n_cmp_cols);

  return (ib_tpl_t)tuple;
}

/**
 * Create an InnoDB key tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of user-defined columns.
 *
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_key_tuple_new(const Index *index, ulint n_cols) noexcept {
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
static ib_tpl_t ib_row_tuple_new_low(const Index *index, ulint n_cols, mem_heap_t *heap) noexcept {
  auto tuple = reinterpret_cast<ib_tuple_t *>(mem_heap_alloc(heap, sizeof(ib_tuple_t)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->heap = heap;
  tuple->index = index;
  tuple->type = TPL_ROW;

  tuple->ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  index->m_table->copy_types(tuple->ptr);

  return (ib_tpl_t)tuple;
}

/**
 * Create an InnoDB row tuple.
 *
 * @param[in] index The index of the tuple.
 * @param[in] n_cols The number of columns in the tuple.
 *
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_row_tuple_new(const Index *index, ulint n_cols) noexcept {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return ib_row_tuple_new_low(index, n_cols, heap);
}

uint64_t ib_api_version() {
  return ((uint64_t)IB_API_VERSION_CURRENT << 32 | (IB_API_VERSION_REVISION << 16) | IB_API_VERSION_AGE);
}

ib_err_t ib_init() {
  ib_err_t ib_err;

  IB_CHECK_PANIC();

  ut_mem_init();

  ib_stream = stderr;

  ib_err = ib_cfg_init();

  return ib_err;
}

ib_err_t ib_startup(const char *) {

  db_format.id = 0;
  db_format.name = nullptr;

  return InnoDB::start();
}

ib_err_t ib_shutdown(ib_shutdown_t flag) {
  IB_CHECK_PANIC();

  auto err = ib_cfg_shutdown();

  if (err != DB_SUCCESS) {
    ib_logger(ib_stream, "ib_cfg_shutdown(): %s; continuing shutdown anyway\n", ib_strerror(err));
  }

  db_format.id = 0;
  db_format.name = nullptr;

  return InnoDB::shutdown(flag);
}

ib_err_t ib_trx_start(ib_trx_t ib_trx, ib_trx_level_t ib_trx_level) {
  ib_err_t err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut_a(ib_trx_level >= IB_TRX_READ_UNCOMMITTED);
  ut_a(ib_trx_level <= IB_TRX_SERIALIZABLE);

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    auto started = trx->start(ULINT_UNDEFINED);
    ut_a(started);

    trx->m_isolation_level = static_cast<Trx_isolation>(ib_trx_level);
  } else {
    err = DB_ERROR;
  }

  trx->m_client_ctx = nullptr;

  return err;
}

void ib_trx_set_client_data(ib_trx_t ib_trx, void *client_data) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  trx->m_client_ctx = client_data;
}

ib_trx_t ib_trx_begin(ib_trx_level_t ib_trx_level) {
  auto trx = srv_trx_sys->create_user_trx(nullptr);
  auto started = ib_trx_start((ib_trx_t)trx, ib_trx_level);

  ut_a(started);

  return reinterpret_cast<ib_trx_t>(trx);
}

ib_trx_state_t ib_trx_state(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  return static_cast<ib_trx_state_t>(trx->m_conc_state);
}

ib_err_t ib_trx_release(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  ut_ad(trx != nullptr);
  srv_trx_sys->destroy_user_trx(trx);

  return DB_SUCCESS;
}

ib_err_t ib_trx_commit(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  auto err = trx->commit();
  ut_a(err == DB_SUCCESS);

  err = ib_schema_unlock(ib_trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = ib_trx_release(ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread();

  return DB_SUCCESS;
}

ib_err_t ib_trx_rollback(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  auto err = trx_general_rollback(trx, false, nullptr);

  /* It should always succeed */
  ut_a(err == DB_SUCCESS);

  err = ib_schema_unlock(ib_trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = ib_trx_release(ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread();

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
static bool ib_check_col_is_ok(const char *name, ib_col_type_t ib_col_type, ib_col_attr_t ib_col_attr, ulint len) {
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
static const ib_index_def_t *ib_table_find_index(const std::vector<ib_index_def_t *> &indexes, const char *name) {
  for (const auto index_def : indexes) {
    if (ib_utf8_strcasecmp(name, index_def->name) == 0) {
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
static ulint ib_col_get_prtype(const ib_col_t *ib_col) {
  ulint prtype = 0;

  if (ib_col->ib_col_attr & IB_COL_UNSIGNED) {
    prtype |= DATA_UNSIGNED;

    ut_a(ib_col->ib_col_type == IB_INT);
  }

  if (ib_col->ib_col_attr & IB_COL_NOT_NULL) {
    prtype |= DATA_NOT_NULL;
  }

  if (ib_col->ib_col_attr & IB_COL_CUSTOM1) {
    prtype |= DATA_CUSTOM_TYPE;
  }

  if (ib_col->ib_col_attr & IB_COL_CUSTOM2) {
    prtype |= (DATA_CUSTOM_TYPE << 1);
  }

  if (ib_col->ib_col_attr & IB_COL_CUSTOM3) {
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
static ulint ib_col_get_mtype(const ib_col_t *ib_col) noexcept {
  /* Note: The api0api.h types should map directly to the internal numeric codes. */
  return ib_col->ib_col_type;
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
static const ib_col_t *ib_table_find_col(const std::vector<ib_col_t *> &cols, const char *name) noexcept {
  for (const auto ib_col : cols) {

    if (ib_utf8_strcasecmp(ib_col->name, name) == 0) {
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
static const ib_key_col_t *ib_index_find_col(const std::vector<ib_key_col_t *> &cols, const char *name) noexcept {
  for (auto ib_col : cols) {

    if (ib_utf8_strcasecmp(ib_col->name, name) == 0) {
      return ib_col;
    }
  }

  return nullptr;
}

ib_err_t ib_table_schema_add_col(
  ib_tbl_sch_t ib_tbl_sch, const char *name, ib_col_type_t ib_col_type, ib_col_attr_t ib_col_attr, uint16_t client_type, ulint len
) {
  ib_err_t err = DB_SUCCESS;
  ib_table_def_t *table_def = (ib_table_def_t *)ib_tbl_sch;

  IB_CHECK_PANIC();

  if (table_def->table != nullptr) {
    err = DB_ERROR;
  } else if (ib_table_find_col(*table_def->cols, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else if (!ib_check_col_is_ok(name, ib_col_type, ib_col_attr, len)) {
    err = DB_ERROR;
  } else {
    auto heap = table_def->heap;

    auto ib_col = reinterpret_cast<ib_col_t *>(mem_heap_zalloc(heap, sizeof(ib_col_t)));

    if (ib_col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->name = mem_heap_strdup(heap, name);
      ib_col->ib_col_type = ib_col_type;
      ib_col->ib_col_attr = ib_col_attr;

      ib_col->len = len;

      table_def->cols->emplace_back(ib_col);
    }
  }

  return err;
}

ib_err_t ib_table_schema_add_index(ib_tbl_sch_t ib_tbl_sch, const char *name, ib_idx_sch_t *ib_idx_sch) {
  ib_err_t err = DB_SUCCESS;
  auto table_def = reinterpret_cast<ib_table_def_t *>(ib_tbl_sch);

  IB_CHECK_PANIC();

  if (table_def->table != nullptr) {
    err = DB_ERROR;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  }
  if (ib_table_find_index(*table_def->indexes, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else {
    auto heap = table_def->heap;
    auto index_def = reinterpret_cast<ib_index_def_t *>(mem_heap_zalloc(heap, sizeof(ib_index_def_t)));

    if (index_def == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {

      index_def->heap = heap;

      index_def->schema = table_def;

      index_def->name = mem_heap_strdup(heap, name);

      index_def->cols = new std::vector<ib_key_col_t *>();

      table_def->indexes->emplace_back(index_def);

      *ib_idx_sch = (ib_idx_sch_t)index_def;
    }
  }

  return err;
}

void ib_table_schema_delete(ib_tbl_sch_t ib_tbl_sch) {
  ib_table_def_t *table_def = (ib_table_def_t *)ib_tbl_sch;

  /* Check that all indexes are owned by the table schema. */
  for (auto index_def : *table_def->indexes) {
    ut_a(index_def->schema != nullptr);
  }

  if (table_def->table != nullptr) {
    srv_dict_sys->table_decrement_handle_count(table_def->table, false);
  }

  delete table_def->cols;
  delete table_def->indexes;

  mem_heap_free(table_def->heap);
}

static ib_err_t ib_table_schema_check(ib_tbl_fmt_t ib_tbl_fmt, ulint *page_size) {
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

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
static ib_err_t ib_table_name_check(const char *name) noexcept {
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

ib_err_t ib_table_schema_create(const char *name, ib_tbl_sch_t *ib_tbl_sch, ib_tbl_fmt_t ib_tbl_fmt, ulint page_size) {
  IB_CHECK_PANIC();

  auto err = ib_table_name_check(name);

  if (err != DB_SUCCESS) {
    return err;
  }

  err = ib_table_schema_check(ib_tbl_fmt, &page_size);

  if (err != DB_SUCCESS) {
    return err;
  }

  mem_heap_t *heap = mem_heap_create(1024);

  if (heap == nullptr) {
    err = DB_OUT_OF_MEMORY;
  } else {
    auto table_def = (ib_table_def_t *)mem_heap_zalloc(heap, sizeof(ib_table_def_t));

    if (table_def == nullptr) {
      err = DB_OUT_OF_MEMORY;

      mem_heap_free(heap);
    } else {
      table_def->heap = heap;

      auto normalized_name = mem_heap_strdup(heap, name);

      ib_normalize_table_name(normalized_name, name);

      table_def->name = normalized_name;

      table_def->page_size = page_size;

      table_def->ib_tbl_fmt = ib_tbl_fmt;

      table_def->cols = new std::vector<ib_col_t *>();

      table_def->indexes = new std::vector<ib_index_def_t *>();

      *ib_tbl_sch = (ib_tbl_sch_t)table_def;
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
static int ib_index_get_col_no(const ib_index_def_t *ib_index_def, const char *name) {
  int col_no;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != nullptr) {
    col_no = ib_index_def->table->get_col_no(name);
  } else {
    const auto ib_col = ib_table_find_col(*ib_index_def->schema->cols, name);

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
static int ib_index_is_prefix_allowed(const ib_index_def_t *ib_index_def, const char *name) noexcept {
  bool allowed = true;
  ulint mtype = ULINT_UNDEFINED;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != nullptr) {
    auto table = ib_index_def->table;
    auto col_no = table->get_col_no(name);
    ut_a(col_no != -1);

    const auto col = table->get_nth_col(col_no);
    ut_a(col != nullptr);

    mtype = col->mtype;
  } else {
    const ib_col_t *ib_col;

    ib_col = ib_table_find_col(*ib_index_def->schema->cols, name);

    ut_a(ib_col != nullptr);

    mtype = (ulint)ib_col->ib_col_type;
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

ib_err_t ib_index_schema_add_col(ib_idx_sch_t ib_idx_sch, const char *name, ulint prefix_len) {
  ib_err_t err = DB_SUCCESS;
  ib_index_def_t *index_def = (ib_index_def_t *)ib_idx_sch;

  IB_CHECK_PANIC();

  /* Check for duplicates. */
  if (ib_index_find_col(*index_def->cols, name) != nullptr) {
    err = DB_COL_APPEARS_TWICE_IN_INDEX;
    /* Check if the column exists in the table definition. */
  } else if (ib_index_get_col_no(index_def, name) == -1) {
    err = DB_NOT_FOUND;
    /* Some column types can't have prefix length indexes. */
  } else if (prefix_len > 0 && !ib_index_is_prefix_allowed(index_def, name)) {
    err = DB_SCHEMA_ERROR;
  } else {
    auto heap = index_def->heap;
    auto ib_col = reinterpret_cast<ib_key_col_t *>(mem_heap_zalloc(heap, sizeof(ib_key_col_t)));

    if (ib_col == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->name = mem_heap_strdup(heap, name);
      ib_col->prefix_len = prefix_len;

      index_def->cols->emplace_back(ib_col);
    }
  }

  return err;
}

ib_err_t ib_index_schema_create(ib_trx_t ib_usr_trx, const char *name, const char *table_name, ib_idx_sch_t *ib_idx_sch) {
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  ib_normalize_table_name(normalized_name, table_name);

  auto table = ib_lookup_table_by_name(normalized_name);

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
      auto index_def = reinterpret_cast<ib_index_def_t *>(mem_heap_zalloc(heap, sizeof(ib_index_def_t)));

      if (index_def == nullptr) {
        err = DB_OUT_OF_MEMORY;

        mem_heap_free(heap);
      } else {
        index_def->heap = heap;

        index_def->table = table;

        index_def->name = mem_heap_strdup(heap, name);

        index_def->cols = new std::vector<ib_key_col_t *>();

        index_def->usr_trx = reinterpret_cast<Trx *>(ib_usr_trx);

        *ib_idx_sch = reinterpret_cast<ib_idx_sch_t>(index_def);
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
static ib_index_def_t *ib_find_clustered_index(const std::vector<ib_index_def_t *> &indexes) {
  for (const auto ib_index_def : indexes) {
    if (ib_index_def->clustered) {
      return ib_index_def;
    }
  }

  return nullptr;
}

ib_err_t ib_index_schema_set_clustered(ib_idx_sch_t ib_idx_sch) {
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  auto index_def = reinterpret_cast<ib_index_def_t *>(ib_idx_sch);

  /* If this index schema is part of a table schema then we need
  to check the state of the other indexes. */

  if (index_def->schema != nullptr) {
    auto ib_clust_index_def = ib_find_clustered_index(*index_def->schema->indexes);

    if (ib_clust_index_def != nullptr) {
      ut_a(ib_clust_index_def->clustered);
      ib_clust_index_def->clustered = false;
    }
  }

  index_def->unique = true;
  index_def->clustered = true;

  return err;
}

ib_err_t ib_index_schema_set_unique(ib_idx_sch_t ib_idx_sch) {
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  auto index_def = (ib_index_def_t *)ib_idx_sch;
  index_def->unique = true;

  return err;
}

void ib_index_schema_delete(ib_idx_sch_t ib_idx_sch) {
  auto index_def = (ib_index_def_t *)ib_idx_sch;

  ut_a(index_def->schema == nullptr);

  delete index_def->cols;
  mem_heap_free(index_def->heap);
}

/**
 * Convert the table definition table attributes to the internal format.
 *
 * @param[in] table_def The table definition.
 *
 * @return Flags.
 */
static ulint ib_table_def_get_flags(const ib_table_def_t *table_def) {
  ulint flags = 0;

  switch (table_def->ib_tbl_fmt) {
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
static const index_def_t *ib_copy_index_definition(ib_index_def_t *ib_index_def, bool clustered) {
  auto index_def = reinterpret_cast<index_def_t *>(mem_heap_zalloc(ib_index_def->heap, sizeof(index_def_t)));
  auto name_len = strlen(ib_index_def->name);
  auto index_name = reinterpret_cast<char *>(mem_heap_zalloc(ib_index_def->heap, name_len + 2));

  /* The TEMP_INDEX_PREFIX is only needed if we are rebuilding an
  index or creating a new index on a table that has records. If
  the definition is owned by a table schema then we can be sure that
  this index definition is part of a CREATE TABLE. */
  if (ib_index_def->schema == nullptr) {
    *index_name = TEMP_INDEX_PREFIX;
    memcpy(index_name + 1, ib_index_def->name, name_len + 1);
  } else {
    memcpy(index_name, ib_index_def->name, name_len);
  }

  index_def->name = index_name;
  index_def->n_fields = ib_index_def->cols->size();

  if (ib_index_def->unique) {
    index_def->ind_type = DICT_UNIQUE;
  } else {
    index_def->ind_type = 0;
  }

  if (clustered) {
    index_def->ind_type |= DICT_CLUSTERED;
  }

  index_def->fields = (index_field_t *)mem_heap_zalloc(ib_index_def->heap, sizeof(index_field_t) * index_def->n_fields);

  for (ulint i = 0; i < ib_index_def->cols->size(); ++i) {
    auto ib_col = ib_index_def->cols->at(i);

    index_def->fields[i].field_name = ib_col->name;
    index_def->fields[i].prefix_len = ib_col->prefix_len;
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
static ib_err_t ib_build_secondary_index(
  Trx *usr_trx, Table *table, ib_index_def_t *ib_index_def, bool create, Index **index
) noexcept {
  ib_err_t err;
  Trx *ddl_trx;

  IB_CHECK_PANIC();

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (!create) {
    ddl_trx = srv_trx_sys->create_user_trx(nullptr);
    const auto started = ddl_trx->start(ULINT_UNDEFINED);
    ut_a(started);
  } else {
    ddl_trx = usr_trx;
  }

  /* Set the CLUSTERED flag to false. */
  const auto index_def = ib_copy_index_definition(ib_index_def, false);

  ut_a(!(index_def->ind_type & DICT_CLUSTERED));

  ddl_trx->m_op_info = "creating secondary index";

  auto trx{reinterpret_cast<ib_trx_t>(usr_trx)};

  if (!(create || ib_schema_lock_is_exclusive(trx))) {
    err = ib_schema_lock_exclusive(trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  if (!create) {
    /* Flag this transaction as a dictionary operation, so that
    the data dictionary will be locked in crash recovery. */
    ddl_trx->set_dict_operation(TRX_DICT_OP_INDEX);
  }

  *index = row_merge_create_index(ddl_trx, table, index_def);

  if (!create) {
    /* Even if the user locked the schema, we release it here and
    build the index without holding the dictionary lock. */
    err = ib_schema_unlock((ib_trx_t)usr_trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);
  }

  err = ddl_trx->m_error_state;

  if (!create) {
    /* Commit the data dictionary transaction in order to release
    the table locks on the system tables. */
    auto err = ddl_trx->commit();
    ut_a(err == DB_SUCCESS);

    srv_trx_sys->destroy_user_trx(ddl_trx);
  }

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (*index != nullptr) {
    ut_a(err == DB_SUCCESS);

    (*index)->m_cmp_ctx = nullptr;

    /* Read the clustered index records and build the index. */
    err = row_merge_build_indexes(usr_trx, table, table, index, 1, nullptr);
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
static char *ib_table_create_temp_name(mem_heap_t *heap, char id, const char *table_name) noexcept {
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
static void ib_index_create_def(const Index *index, index_def_t *index_def, mem_heap_t *heap) noexcept {
  const auto n_fields = index->m_n_user_defined_cols;

  index_def->fields = (merge_index_field_t *)mem_heap_zalloc(heap, n_fields * sizeof(*index_def->fields));

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
static index_def_t *ib_table_create_index_defs(Trx *trx, const Table *table, mem_heap_t *heap, ulint *n_indexes) noexcept {
  auto sz = sizeof(index_def_t) * UT_LIST_GET_LEN(table->m_indexes);
  auto index_defs = reinterpret_cast<index_def_t *>(mem_heap_zalloc(heap, sz));
  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);

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

  err = ib_schema_unlock((ib_trx_t)trx);
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
static ib_err_t ib_create_cluster_index(Trx *trx, Table *table, ib_index_def_t *ib_index_def, Index **index) noexcept {
  IB_CHECK_PANIC();

  ut_a(!ib_index_def->cols->empty());

  /* Set the CLUSTERED flag to true. */
  auto index_def = ib_copy_index_definition(ib_index_def, true);

  trx->m_op_info = "creating clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  auto err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *index = row_merge_create_index(trx, table, index_def);

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
static ib_err_t ib_table_clone_indexes(Trx *trx, Table *src_table, Table *new_table, mem_heap_t *heap) noexcept {
  ulint n_index_defs = 0;
  auto index_defs = ib_table_create_index_defs(trx, src_table, heap, &n_index_defs);

  ut_a(index_defs != nullptr);

  for (ulint i{}; i < n_index_defs; ++i) {
    ut_a(!(index_defs[i].ind_type & DICT_CLUSTERED));
    auto index = row_merge_create_index(trx, new_table, &index_defs[i]);

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
static ib_err_t ib_table_clone(
  Trx *trx, Table *src_table, Table **new_table, ib_index_def_t *ib_index_def, mem_heap_t *heap
) noexcept {
  auto new_table_name = ib_table_create_temp_name(heap, '1', src_table->m_name);
  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  /* Set the CLUSTERED flag to true. */
  auto index_def = ib_copy_index_definition(ib_index_def, true);

  /* Create the new table and the cluster index. */
  *new_table = row_merge_create_temporary_table(new_table_name, index_def, src_table, trx);

  if (unlikely(new_table == nullptr)) {
    err = trx->m_error_state;
  } else {
    trx->m_table_id = (*new_table)->m_id;

    err = ib_table_clone_indexes(trx, src_table, *new_table, heap);
  }

  err = ib_schema_unlock((ib_trx_t)trx);
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
static ib_err_t ib_table_copy(Trx *trx, Table *src_table, Table *dst_table, mem_heap_t *heap) noexcept {
  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);

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

  err = ib_schema_unlock((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  /* Build the actual indexes. */
  return row_merge_build_indexes(trx, src_table, dst_table, indexes, n_indexes, nullptr);
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
static ib_err_t ib_create_default_cluster_index(Trx *trx, Table *table, Index **index) noexcept {
  index_def_t index_def;

  index_def.n_fields = 0;
  index_def.fields = nullptr;
  index_def.name = GEN_CLUST_INDEX;
  index_def.ind_type = DICT_CLUSTERED;

  trx->m_op_info = "creating default clustered index";

  trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(ib_schema_lock_is_exclusive((ib_trx_t)trx));

  auto err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *index = row_merge_create_index(trx, table, &index_def);

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
static ib_err_t ib_create_indexes(Trx *ddl_trx, Table *table, const std::vector<ib_index_def_t *> &indexes) noexcept {
  ib_err_t err = DB_ERROR;
  Index *index = nullptr;
  ib_index_def_t *ib_clust_index_def = nullptr;

  auto n_indexes = indexes.size();

  if (n_indexes > 0) {
    ib_clust_index_def = ib_find_clustered_index(indexes);

    if (ib_clust_index_def != nullptr) {
      ut_a(ib_clust_index_def->clustered);

      err = ib_create_cluster_index(ddl_trx, table, ib_clust_index_def, &index);
    }
  }

  if (ib_clust_index_def == nullptr) {
    err = ib_create_default_cluster_index(ddl_trx, table, &index);
  }

  for (ulint i{}; err == DB_SUCCESS && i < n_indexes; ++i) {
    auto ib_index_def = indexes.at(i);

    ut_a(!ib_index_def->cols->empty());

    if (!ib_index_def->clustered) {
      /* Since this is part of CREATE TABLE, set the create flag to true. */
      err = ib_build_secondary_index(ddl_trx, table, ib_index_def, true, &index);
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
static ib_err_t ib_table_get_id_low(const char *table_name, ib_id_t *table_id) noexcept {
  ib_err_t err = DB_TABLE_NOT_FOUND;

  *table_id = 0;

  auto table = ib_lookup_table_by_name(table_name);

  if (table != nullptr) {
    *table_id = table->m_id;

    err = DB_SUCCESS;
  }

  return err;
}

ib_err_t ib_table_create(ib_trx_t ib_trx, const ib_tbl_sch_t ib_tbl_sch, ib_id_t *id) {
  auto ddl_trx = (Trx *)ib_trx;
  const auto table_def = (const ib_table_def_t *)ib_tbl_sch;

  IB_CHECK_PANIC();

  /* Another thread may have created the table already when we get
  here. We need to search the data dictionary before we attempt to
  create the table. */

  if (!ib_schema_lock_is_exclusive((ib_trx_t)ddl_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto err = ib_table_get_id_low(table_def->name, id);

  if (err == DB_SUCCESS) {
    return DB_TABLE_EXISTS;
  }

  *id = 0;

  auto n_cols = table_def->cols->size();

  if (n_cols == 0) {
    return DB_SCHEMA_ERROR;
  }

  ulint n_cluster{};

  /* Check that all index definitions are valid. */
  for (auto ib_index_def : *table_def->indexes) {

    /* Check that the index definition has at least one column. */
    if (ib_index_def->cols->empty()) {
      return DB_SCHEMA_ERROR;
    }

    /* Check for duplicate cluster definitions. */
    if (ib_index_def->clustered) {
      ++n_cluster;

      if (n_cluster > 1) {
        return DB_SCHEMA_ERROR;
      }
    }
  }

  /* Create the table prototype. */
  auto table = Table::create(table_def->name, SYS_TABLESPACE, n_cols, ib_table_def_get_flags(table_def), false, Current_location());

  /* Create the columns defined by the user. */
  for (ulint i{}; i < n_cols; ++i) {
    auto ib_col = table_def->cols->at(i);

    table->add_col(ib_col->name, ib_col_get_mtype(ib_col), ib_col_get_prtype(ib_col), ib_col->len);
  }

  /* Create the table using the prototype in the data dictionary. */
  err = srv_dict_sys->m_ddl.create_table(table, ddl_trx);

  table = nullptr;

  if (err == DB_SUCCESS) {

    table = ib_lookup_table_by_name(table_def->name);
    ut_a(table != nullptr);

    /* Bump up the reference count, so that another transaction
    doesn't delete it behind our back. */
    srv_dict_sys->table_increment_handle_count(table, true);

    err = ib_create_indexes(ddl_trx, table, *table_def->indexes);
  }

  /* FIXME: If ib_create_indexes() fails, it's unclear what state
  the data dictionary is in. */

  if (err == DB_SUCCESS) {
    *id = table->m_id;
  }

  if (table != nullptr) {
    srv_dict_sys->table_decrement_handle_count(table, true);
  }

  return err;
}

ib_err_t ib_table_rename(ib_trx_t ib_trx, const char *old_name, const char *new_name) {
  ib_err_t err;
  Trx *trx = (Trx *)ib_trx;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    err = ib_schema_lock_exclusive(ib_trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  auto normalized_old_name = static_cast<char *>(mem_alloc(strlen(old_name) + 1));
  ib_normalize_table_name(normalized_old_name, old_name);

  auto normalized_new_name = static_cast<char *>(mem_alloc(strlen(new_name) + 1));
  ib_normalize_table_name(normalized_new_name, new_name);

  err = srv_dict_sys->m_ddl.rename_table(normalized_old_name, normalized_new_name, trx);

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
static ib_err_t ib_create_primary_index(ib_idx_sch_t ib_idx_sch, ib_id_t *index_id) {
  auto ib_index_def = (ib_index_def_t *)ib_idx_sch;
  auto usr_trx = ib_index_def->usr_trx;
  auto table = ib_index_def->table;

  IB_CHECK_PANIC();

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->schema == nullptr);
  ut_a(ib_index_def->clustered);

  /* Recreate the cluster index and all the secondary indexes
  on a table. If there was a user defined cluster index on the
  table, it will be recreated a secondary index. The InnoDB
  generated cluster index if one exists will be dropped. */

  usr_trx->m_op_info = "recreating clustered index";

  auto heap = mem_heap_create(1024);

  /* This transaction should be the only one operating on the table. */
  ut_a(table->m_n_handles_opened == 1);

  usr_trx->set_dict_operation(TRX_DICT_OP_TABLE);

  ut_a(!ib_index_def->cols->empty());

  /* Set the CLUSTERED flag to true. */
  ib_copy_index_definition(ib_index_def, true);

  Table *new_table{};
  auto err = ib_trx_lock_table_with_retry(usr_trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    err = ib_table_clone(usr_trx, table, &new_table, ib_index_def, heap);
  }

  if (err == DB_SUCCESS) {
    err = ib_trx_lock_table_with_retry(usr_trx, new_table, LOCK_X);
  }

  if (err == DB_SUCCESS) {
    err = ib_table_copy(usr_trx, table, new_table, heap);
  }

  if (err == DB_SUCCESS) {
    /* Swap the cloned table with the original table. On success drop
    the original table. */
    auto old_name = table->m_name;
    auto tmp_name = ib_table_create_temp_name(heap, '2', old_name);

    err = row_merge_rename_tables(table, new_table, tmp_name, usr_trx);

    if (err != DB_SUCCESS) {
      row_merge_drop_table(usr_trx, new_table);
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
static ib_err_t ib_create_secondary_index(ib_idx_sch_t ib_idx_sch, ib_id_t *index_id) {
  Trx *ddl_trx{};
  Index *index{};
  auto ib_index_def = reinterpret_cast<ib_index_def_t *>(ib_idx_sch);
  auto usr_trx = ib_index_def->usr_trx;
  auto table = ib_index_def->table;

  IB_CHECK_PANIC();

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->schema == nullptr);
  ut_a(!ib_index_def->clustered);

  auto err = ib_trx_lock_table_with_retry(usr_trx, table, LOCK_S);

  if (err == DB_SUCCESS) {

    /* Since this is part of ALTER TABLE set the create flag to false. */
    err = ib_build_secondary_index(usr_trx, table, ib_index_def, false, &index);

    err = ib_schema_lock_exclusive((ib_trx_t)usr_trx);
    ut_a(err == DB_SUCCESS);

    if (index != nullptr && err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &index, 1);
      index = nullptr;
    } else {
      bool started;

      ddl_trx = srv_trx_sys->create_user_trx(nullptr);
      started = ddl_trx->start(ULINT_UNDEFINED);
      ut_a(started);
    }
  }

  ut_a(ddl_trx != nullptr || err != DB_SUCCESS);

  /* Rename from the TEMP new index to the actual name. */
  if (index != nullptr && err == DB_SUCCESS) {
    err = row_merge_rename_indexes(usr_trx, table);

    if (err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &index, 1);
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

    auto err = ddl_trx->commit();
    ut_a(err == DB_SUCCESS);
  } else if (ddl_trx != nullptr) {
    trx_general_rollback(ddl_trx, false, nullptr);
  }

  if (ddl_trx != nullptr) {
    ddl_trx->m_op_info = "";
    srv_trx_sys->destroy_user_trx(ddl_trx);
  }

  return err;
}

ib_err_t ib_index_create(ib_idx_sch_t ib_idx_sch, ib_id_t *index_id) {
  ib_err_t err;
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive((ib_trx_t)ib_index_def->usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (ib_index_def->clustered) {
    err = ib_create_primary_index(ib_idx_sch, index_id);
  } else {
    err = ib_create_secondary_index(ib_idx_sch, index_id);
  }

  return err;
}

ib_err_t ib_table_drop(ib_trx_t ib_trx, const char *name) {
  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  auto trx = reinterpret_cast<Trx *>(ib_trx);

  db_err err;

  if ((err = srv_dict_sys->m_ddl.drop_table(normalized_name, trx, false)) != DB_SUCCESS) {
    log_err("DROP table failed with error ", err, " while dropping table ", normalized_name);
  }

  mem_free(normalized_name);

  return err;
}

ib_err_t ib_index_drop(ib_trx_t ib_trx, ib_id_t index_id) {
  ulint table_id = (ulint)(index_id >> 32);

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = ib_open_table_by_id(table_id, true);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  /* We use only the lower 32 bits of the uint64_t. */
  index_id &= 0xFFFFFFFFULL;

  auto index = table->index_get_on_id(index_id);

  ib_err_t err;

  if (index != nullptr) {
    err = srv_dict_sys->m_ddl.drop_index(table, index, (Trx *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  srv_dict_sys->table_decrement_handle_count(table, false);

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
static ib_err_t ib_create_cursor(ib_crsr_t *ib_crsr, Table *table, ib_id_t index_id, Trx *trx) noexcept {
  ib_err_t err = DB_SUCCESS;
  const Dict_id id = index_id;

  IB_CHECK_PANIC();

  auto heap = mem_heap_create(sizeof(ib_cursor_t) * 2);

  if (heap == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto cursor = reinterpret_cast<ib_cursor_t *>(mem_heap_zalloc(heap, sizeof(ib_cursor_t)));

  cursor->heap = heap;

  cursor->query_heap = mem_heap_create(64);

  if (cursor->query_heap == nullptr) {
    mem_heap_free(heap);

    return DB_OUT_OF_MEMORY;
  }

  cursor->prebuilt = Prebuilt::create(srv_fsp, srv_btree_sys, table);

  auto prebuilt = cursor->prebuilt;

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

    auto rv = prebuilt->m_trx->assign_read_view();
    ut_a(rv != nullptr);
  }

  *ib_crsr = (ib_crsr_t)cursor;

  return err;
}

ib_err_t ib_cursor_open_table_using_id(ib_id_t table_id, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  Table *table;

  IB_CHECK_PANIC();

  if (ib_trx == nullptr || !ib_schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(table_id, false);
  } else {
    table = ib_open_table_by_id(table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  return ib_create_cursor(ib_crsr, table, 0, (Trx *)ib_trx);
}

ib_err_t ib_cursor_open_index_using_id(ib_id_t index_id, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  Table *table;
  ulint table_id = (ulint)(index_id >> 32);

  IB_CHECK_PANIC();

  if (ib_trx == nullptr || !ib_schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(table_id, false);
  } else {
    table = ib_open_table_by_id(table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  /* We only return the lower 32 bits of the uint64_t. */
  auto err = ib_create_cursor(ib_crsr, table, index_id & 0xFFFFFFFFULL, (Trx *)ib_trx);

  if (ib_crsr != nullptr) {
    auto cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->m_index == nullptr) {
      auto crsr_err = ib_cursor_close(*ib_crsr);
      ut_a(crsr_err == DB_SUCCESS);

      *ib_crsr = nullptr;
    }
  }

  return err;
}

ib_err_t ib_cursor_open_index_using_name(ib_crsr_t ib_open_crsr, const char *index_name, ib_crsr_t *ib_crsr) {
  Table *table;
  ib_id_t index_id = 0;
  ib_err_t err = DB_TABLE_NOT_FOUND;
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_open_crsr);
  auto trx = cursor->prebuilt->m_trx;

  IB_CHECK_PANIC();

  if (trx != nullptr && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    srv_dict_sys->mutex_acquire();
  }

  /* We want to increment the ref count, so we do a redundant search. */
  table = srv_dict_sys->table_get_using_id(srv_config.m_force_recovery, cursor->prebuilt->m_table->m_id, true);
  ut_a(table != nullptr);

  if (trx != nullptr && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    srv_dict_sys->mutex_release();
  }

  /* The first index is always the cluster index. */
  for (auto index : table->m_indexes) {
    if (strcmp(index->m_name, index_name) == 0) {
      index_id = index->m_id;
    }
  }

  *ib_crsr = nullptr;

  if (index_id > 0) {
    err = ib_create_cursor(ib_crsr, table, index_id, cursor->prebuilt->m_trx);
  }

  if (*ib_crsr != nullptr) {
    const ib_cursor_t *cursor;

    cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->m_index == nullptr) {
      err = ib_cursor_close(*ib_crsr);
      ut_a(err == DB_SUCCESS);
      *ib_crsr = nullptr;
    }
  } else {
    srv_dict_sys->table_decrement_handle_count(table, true);
  }

  return err;
}

ib_err_t ib_cursor_open_table(const char *name, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  ib_err_t err;
  Table *table;

  IB_CHECK_PANIC();

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  if (ib_trx != nullptr) {
    if (!ib_schema_lock_is_exclusive(ib_trx)) {
      table = ib_open_table_by_name(normalized_name);
    } else {
      table = ib_lookup_table_by_name(normalized_name);

      if (table != nullptr) {
        srv_dict_sys->table_increment_handle_count(table, true);
      }
    }
  } else {
    table = ib_open_table_by_name(normalized_name);
  }

  mem_free(normalized_name);
  normalized_name = nullptr;

  /* It can happen that another thread has created the table but
  not the cluster index or it's a broken table definition. Refuse to
  open if that's the case. */
  if (table != nullptr && table->get_first_index() == nullptr) {
    srv_dict_sys->table_decrement_handle_count(table, false);
    table = nullptr;
  }

  if (table != nullptr) {
    err = ib_create_cursor(ib_crsr, table, 0, (Trx *)ib_trx);
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
static void ib_qry_proc_free(ib_qry_proc_t *q_proc) {
  que_graph_free_recursive(q_proc->grph.ins);
  que_graph_free_recursive(q_proc->grph.upd);
  que_graph_free_recursive(q_proc->grph.sel);

  memset(q_proc, 0x0, sizeof(*q_proc));
}

ib_err_t ib_cursor_reset(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  Prebuilt *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  if (prebuilt->m_trx != nullptr && prebuilt->m_trx->m_n_client_tables_in_use > 0) {

    --prebuilt->m_trx->m_n_client_tables_in_use;
  }

  /* The fields in this data structure are allocated from
  the query heap and so need to be reset too. */
  ib_qry_proc_free(&cursor->q_proc);

  mem_heap_empty(cursor->query_heap);

  prebuilt->clear();

  return DB_SUCCESS;
}

ib_err_t ib_cursor_close(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  Prebuilt *prebuilt = cursor->prebuilt;
  Trx *trx = prebuilt->m_trx;

  IB_CHECK_PANIC();

  ib_qry_proc_free(&cursor->q_proc);

  /* The transaction could have been detached from the cursor. */
  if (trx != nullptr && trx->m_n_client_tables_in_use > 0) {
    --trx->m_n_client_tables_in_use;
  }

  if (trx != nullptr && ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    Prebuilt::destroy(srv_dict_sys, cursor->prebuilt, true);
  } else {
    Prebuilt::destroy(srv_dict_sys, cursor->prebuilt, false);
  }

  mem_heap_free(cursor->query_heap);
  mem_heap_free(cursor->heap);

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
static ib_err_t ib_insert_row_with_lock_retry(que_thr_t *thr, ins_node_t *node, trx_savept_t *savept) noexcept {
  ib_err_t err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    (void)srv_row_ins->step(thr);

    err = trx->m_error_state;

    if (err != DB_SUCCESS) {
      que_thr_stop_client(thr);

      thr->lock_state = QUE_THR_LOCK_ROW;
      lock_wait = ib_handle_errors(&err, trx, thr, savept);
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
static ib_err_t ib_execute_insert_query_graph(Table *table, que_fork_t *ins_graph, ins_node_t *node) noexcept {
  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  auto trx = ins_graph->trx;
  auto savept = trx_savept_take(trx);
  auto thr = que_fork_get_first_thr(ins_graph);

  que_thr_move_to_run_state(thr);

  auto err = ib_insert_row_with_lock_retry(thr, node, &savept);

  if (err == DB_SUCCESS) {
    que_thr_stop_for_client_no_error(thr, trx);

    ++table->m_stats.m_n_rows;

    srv_n_rows_inserted++;

    ib_update_statistics_if_needed(table);

    ib_wake_master_thread();
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create an insert query graph node.
 *
 * @param[in] cursor in: Cursor instance
 */
static void ib_insert_query_graph_create(ib_cursor_t *cursor) noexcept {
  auto q_proc = &cursor->q_proc;
  auto node = &q_proc->node;
  auto trx = cursor->prebuilt->m_trx;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->ins == nullptr) {
    auto grph = &q_proc->grph;
    auto heap = cursor->query_heap;
    auto table = cursor->prebuilt->m_table;

    node->ins = srv_row_ins->node_create(INS_DIRECT, table, heap);

    node->ins->m_select = nullptr;
    node->ins->m_values_list = nullptr;

    auto row = dtuple_create(heap, table->get_n_cols());
    table->copy_types(row);

    srv_row_ins->set_new_row(node->ins, row);

    grph->ins = static_cast<que_fork_t *>(que_node_get_parent(pars_complete_graph_for_exec(node->ins, trx, heap)));

    grph->ins->state = QUE_FORK_ACTIVE;
  }
}

ib_err_t ib_cursor_insert_row(ib_crsr_t ib_crsr, const ib_tpl_t ib_tpl) {
  ulint n_fields;
  ib_qry_node_t *node;
  ib_qry_proc_t *q_proc;
  DTuple *dst_dtuple;
  ib_err_t err = DB_SUCCESS;
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  const auto src_tuple = reinterpret_cast<const ib_tuple_t *>(ib_tpl);

  IB_CHECK_PANIC();

  ib_insert_query_graph_create(cursor);

  ut_ad(src_tuple->type == TPL_ROW);

  q_proc = &cursor->q_proc;
  node = &q_proc->node;

  node->ins->m_state = INS_NODE_ALLOC_ROW_ID;
  dst_dtuple = node->ins->m_row;

  n_fields = dtuple_get_n_fields(src_tuple->ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_dtuple));

  /* Do a shallow copy of the data fields and check for nullptr
  constraints on columns. */
  for (ulint i = 0; i < n_fields; ++i) {
    auto src_field = dtuple_get_nth_field(src_tuple->ptr, i);
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
    err = ib_execute_insert_query_graph(src_tuple->index->m_table, q_proc->grph.ins, node->ins);
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
static upd_t *ib_update_vector_create(ib_cursor_t *cursor) {
  auto trx = cursor->prebuilt->m_trx;
  auto heap = cursor->query_heap;
  auto table = cursor->prebuilt->m_table;
  auto q_proc = &cursor->q_proc;
  auto grph = &q_proc->grph;
  auto node = &q_proc->node;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->upd == nullptr) {
    node->upd = srv_row_upd->create_update_node(table, heap);
  }

  grph->upd = static_cast<que_fork_t *>(que_node_get_parent(pars_complete_graph_for_exec(node->upd, trx, heap)));

  grph->upd->state = QUE_FORK_ACTIVE;

  return node->upd->m_update;
}

/**
 * Note that a column has changed.
 *
 * @param[in] cursor in: current cursor
 * @param[in/out] upd_field in/out: update field
 * @param[in] col_no in: column number
 * @param[in] dfield in: updated dfield
 */
static void ib_update_col(ib_cursor_t *cursor, upd_field_t *upd_field, ulint col_no, dfield_t *dfield) {
  auto table = cursor->prebuilt->m_table;
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
static ib_err_t ib_calc_diff(ib_cursor_t *cursor, upd_t *upd, const ib_tuple_t *old_tuple, const ib_tuple_t *new_tuple) noexcept {
  ulint n_changed = 0;
  ib_err_t err = DB_SUCCESS;
  const auto n_fields = dtuple_get_n_fields(new_tuple->ptr);

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);
  ut_a(old_tuple->index->m_table == new_tuple->index->m_table);

  for (ulint i = 0; i < n_fields; ++i) {
    auto new_dfield = dtuple_get_nth_field(new_tuple->ptr, i);
    auto old_dfield = dtuple_get_nth_field(old_tuple->ptr, i);

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
static ib_err_t ib_update_row_with_lock_retry(que_thr_t *thr, upd_node_t *node, trx_savept_t *savept) noexcept {
  ib_err_t err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    (void)srv_row_upd->step(thr);

    err = trx->m_error_state;

    if (err != DB_SUCCESS) {
      que_thr_stop_client(thr);

      if (err != DB_RECORD_NOT_FOUND) {
        thr->lock_state = QUE_THR_LOCK_ROW;

        lock_wait = ib_handle_errors(&err, trx, thr, savept);

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
static ib_err_t ib_execute_update_query_graph(ib_cursor_t *cursor, Btree_pcursor *pcur) noexcept {
  auto trx = cursor->prebuilt->m_trx;
  auto table = cursor->prebuilt->m_table;
  auto q_proc = &cursor->q_proc;

  /* The transaction must be running. */
  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = q_proc->node.upd;

  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  ut_a(pcur->get_index()->is_clustered());
  node->m_pcur->copy_stored_position(pcur);

  ut_a(node->m_pcur->get_rel_pos() == Btree_cursor_pos::ON);

  auto savept = trx_savept_take(trx);

  auto thr = que_fork_get_first_thr(q_proc->grph.upd);

  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  que_thr_move_to_run_state(thr);

  auto err = ib_update_row_with_lock_retry(thr, node, &savept);

  if (err == DB_SUCCESS) {

    que_thr_stop_for_client_no_error(thr, trx);

    if (node->m_is_delete) {

      if (table->m_stats.m_n_rows > 0) {
        --table->m_stats.m_n_rows;
      }

      ++srv_n_rows_deleted;
    } else {
      ++srv_n_rows_updated;
    }

    ib_update_statistics_if_needed(table);

  } else if (err == DB_RECORD_NOT_FOUND) {
    trx->m_error_state = DB_SUCCESS;
  }

  ib_wake_master_thread();

  trx->m_op_info = "";

  return err;
}

ib_err_t ib_cursor_update_row(ib_crsr_t ib_crsr, const ib_tpl_t ib_old_tpl, const ib_tpl_t ib_new_tpl) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;
  const auto old_tuple = reinterpret_cast<const ib_tuple_t *>(ib_old_tpl);
  const auto new_tuple = reinterpret_cast<const ib_tuple_t *>(ib_new_tpl);

  IB_CHECK_PANIC();

  Btree_pcursor *pcur;

  if (prebuilt->m_index->is_clustered()) {
    pcur = prebuilt->m_pcur;
  } else if (prebuilt->m_need_to_access_clustered && prebuilt->m_clust_pcur != nullptr) {
    pcur = prebuilt->m_clust_pcur;
  } else {
    return DB_ERROR;
  }

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);

  auto upd = ib_update_vector_create(cursor);
  auto err = ib_calc_diff(cursor, upd, old_tuple, new_tuple);

  if (err == DB_SUCCESS) {
    /* Note that this is not a delete. */
    cursor->q_proc.node.upd->m_is_delete = false;

    err = ib_execute_update_query_graph(cursor, pcur);
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
static ib_err_t ib_delete_row(ib_cursor_t *cursor, Btree_pcursor *pcur, const Rec &rec) {
  auto table = cursor->prebuilt->m_table;
  auto index = table->get_first_index();

  IB_CHECK_PANIC();

  auto n_cols = index->get_n_ordering_defined_by_user();
  auto ib_tpl = ib_key_tuple_new(index, n_cols);

  if (ib_tpl == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  auto upd = ib_update_vector_create(cursor);

  ib_read_tuple(rec, tuple);

  upd->m_n_fields = ib_tuple_get_n_cols(ib_tpl);

  for (ulint i{}; i < upd->m_n_fields; ++i) {
    auto upd_field = &upd->m_fields[i];
    auto dfield = dtuple_get_nth_field(tuple->ptr, i);

    dfield_copy_data(&upd_field->m_new_val, dfield);

    upd_field->m_exp = nullptr;

    upd_field->m_orig_len = 0;

    upd->m_info_bits = 0;

    upd_field->m_field_no = index->get_clustered_field_pos(&table->m_cols[i]);
  }

  /* Note that this is a delete. */
  cursor->q_proc.node.upd->m_is_delete = true;

  auto err = ib_execute_update_query_graph(cursor, pcur);

  ib_tuple_delete(ib_tpl);

  return err;
}

ib_err_t ib_cursor_delete_row(ib_crsr_t ib_crsr) {
  ib_err_t err;
  Btree_pcursor *pcur;
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

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
      mtr_t mtr;

      mtr.start();

      if (pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location())) {

        rec = pcur->get_rec();
      } else {
        rec = Rec();
      }

      mtr.commit();
    }

    if (rec.is_valid() && !rec.get_deleted_flag()) {
      err = ib_delete_row(cursor, pcur, Rec(rec));
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    err = DB_RECORD_NOT_FOUND;
  }

  return err;
}

ib_err_t ib_cursor_read_row(ib_crsr_t ib_crsr, ib_tpl_t ib_tpl) {
  ib_err_t err;
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  IB_CHECK_PANIC();

  ut_a(cursor->prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  /* When searching with IB_EXACT_MATCH set, row_search_mvcc()
  will not position the persistent cursor but will copy the record
  found into the row cache. It should be the only entry. */
  if (!ib_cursor_is_positioned(ib_crsr) && cursor->prebuilt->m_row_cache.is_cache_empty()) {
    err = DB_RECORD_NOT_FOUND;
  } else if (!cursor->prebuilt->m_row_cache.is_cache_empty()) {
    auto rec = cursor->prebuilt->m_row_cache.cache_get_row();
    ut_a(!rec.is_null());

    if (!rec.get_deleted_flag()) {
      ib_read_tuple(Rec(rec), tuple);
      err = DB_SUCCESS;
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    mtr_t mtr;
    Btree_pcursor *pcur;
    auto prebuilt = cursor->prebuilt;

    if (prebuilt->m_need_to_access_clustered && tuple->type == TPL_ROW) {
      pcur = prebuilt->m_clust_pcur;
    } else {
      pcur = prebuilt->m_pcur;
    }

    if (pcur == nullptr) {
      return DB_ERROR;
    }

    mtr.start();

    if (pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location())) {
      auto rec = pcur->get_rec();

      if (!rec.get_deleted_flag()) {
        ib_read_tuple(Rec(rec), tuple);
        err = DB_SUCCESS;
      } else {
        err = DB_RECORD_NOT_FOUND;
      }
    } else {
      err = DB_RECORD_NOT_FOUND;
    }

    mtr.commit();
  }

  return err;
}

ib_err_t ib_cursor_prev(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  prebuilt->m_row_cache.cache_next();

  return srv_row_sel->mvcc_fetch(srv_config.m_force_recovery, IB_CUR_L, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_PREV);
}

ib_err_t ib_cursor_next(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  prebuilt->m_row_cache.cache_next();

  return srv_row_sel->mvcc_fetch(srv_config.m_force_recovery, IB_CUR_G, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_NEXT);
}

/**
 * Move cursor to the first record in the table.
 *
 * @param[in] cursor in: InnoDB cursor instance
 * @param[in] mode in: Search mode
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_cursor_position(ib_cursor_t *cursor, ib_srch_mode_t mode) {
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to position at one of the ends, row_search_mvcc()
  uses the search_tuple fields to work out what to do. */
  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);

  return srv_row_sel->mvcc_fetch(srv_config.m_force_recovery, mode, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_MOVETO);
}

ib_err_t ib_cursor_first(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  IB_CHECK_PANIC();

  return ib_cursor_position(cursor, IB_CUR_G);
}

ib_err_t ib_cursor_last(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  IB_CHECK_PANIC();

  return ib_cursor_position(cursor, IB_CUR_L);
}

ib_err_t ib_cursor_moveto(ib_crsr_t ib_crsr, ib_tpl_t ib_tpl, ib_srch_mode_t ib_srch_mode, int *result) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;
  auto search_tuple = prebuilt->m_search_tuple;

  IB_CHECK_PANIC();

  ut_a(tuple->type == TPL_KEY);

  auto n_fields = prebuilt->m_index->get_n_ordering_defined_by_user();

  dtuple_set_n_fields(search_tuple, n_fields);
  dtuple_set_n_fields_cmp(search_tuple, n_fields);

  /* Do a shallow copy */
  for (ulint i{}; i < n_fields; ++i) {
    dfield_copy(dtuple_get_nth_field(search_tuple, i), dtuple_get_nth_field(tuple->ptr, i));
  }

  ut_a(prebuilt->m_select_lock_type <= LOCK_NUM);

  auto err =
    srv_row_sel->mvcc_fetch(srv_config.m_force_recovery, ib_srch_mode, prebuilt, (ib_match_t)cursor->match_mode, ROW_SEL_MOVETO);

  *result = prebuilt->m_result;

  return err;
}

void ib_cursor_attach_trx(ib_crsr_t ib_crsr, ib_trx_t ib_trx) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  ut_a(ib_trx != nullptr);
  ut_a(prebuilt->m_trx == nullptr);

  prebuilt->clear();
  prebuilt->update_trx((Trx *)ib_trx);

  /* Assign a read view if the transaction does not have it yet */
  auto rv = prebuilt->m_trx->assign_read_view();
  ut_a(rv != nullptr);

  ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);

  ++prebuilt->m_trx->m_n_client_tables_in_use;
}

void ib_set_client_compare(ib_client_cmp_t client_cmp_func) {
  ib_client_compare = client_cmp_func;
}

void ib_cursor_set_match_mode(ib_crsr_t ib_crsr, ib_match_mode_t match_mode) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  cursor->match_mode = match_mode;
}

/**
 * @brief Get the dfield instance for the column in the tuple.
 *
 * @param[in] tuple Tuple instance.
 * @param[in] col_no Column number in the tuple.
 *
 * @return dfield_t* dfield instance in the tuple.
 */
static dfield_t *ib_col_get_dfield(ib_tuple_t *tuple, ulint col_no) noexcept {
  return dtuple_get_nth_field(tuple->ptr, col_no);
}

/**
 * @brief Predicate to check whether a column type contains variable length data.
 *
 * @param[in] dtype Column type.
 *
 * @return true if the column type contains variable length data, false otherwise.
 */
static bool ib_col_is_capped(const dtype_t *dtype) noexcept {
  return dtype_get_len(dtype) > 0 && (dtype_get_mtype(dtype) == DATA_VARCHAR || dtype_get_mtype(dtype) == DATA_CHAR ||
                                      dtype_get_mtype(dtype) == DATA_CLIENT || dtype_get_mtype(dtype) == DATA_VARCLIENT ||
                                      dtype_get_mtype(dtype) == DATA_FIXBINARY || dtype_get_mtype(dtype) == DATA_BINARY);
}

ib_err_t ib_col_set_value(ib_tpl_t ib_tpl, ulint col_no, const void *s, ulint len) {
  auto src = static_cast<const byte *>(s);
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  IB_CHECK_PANIC();

  ut_d(mem_heap_verify(tuple->heap));

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
      dst = mem_heap_alloc(tuple->heap, dtype_get_len(dtype));
      ut_a(dst != nullptr);
    }
  } else if (dst == nullptr || len > dfield_get_len(dfield)) {
    dst = mem_heap_alloc(tuple->heap, len);
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
  mem_heap_verify(tuple->heap);
#endif

  return DB_SUCCESS;
}

ulint ib_col_get_len(ib_tpl_t ib_tpl, ulint i) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

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
static ulint ib_col_copy_value_low(ib_tpl_t ib_tpl, ulint i, void *dst, ulint len) noexcept {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

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

ulint ib_col_copy_value(ib_tpl_t ib_tpl, ulint i, void *dst, ulint len) {
  return ib_col_copy_value_low(ib_tpl, i, dst, len);
}

/**
 * Get the InnoDB column attribute from the internal column precise type.
 *
 * @param[in] prtype Column definition.
 *
 * @return Precise type in API format.
 */
static ib_col_attr_t ib_col_get_attr(ulint prtype) {
  ib_col_attr_t col_attr = IB_COL_NONE;
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

  return static_cast<ib_col_attr_t>(attr);
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
static ulint ib_col_get_meta_low(ib_tpl_t ib_tpl, ulint i, ib_col_meta_t *ib_col_meta) noexcept {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  auto dfield = ib_col_get_dfield(tuple, i);
  auto data_len = dfield_get_len(dfield);

  /* We assume 1-1 mapping between the ENUM and internal type codes. */
  ib_col_meta->type = static_cast<ib_col_type_t>(dtype_get_mtype(dfield_get_type(dfield)));

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
static ib_err_t ib_tuple_check_int(ib_tpl_t ib_tpl, ulint i, bool usign, ulint size) noexcept {
  ib_col_meta_t ib_col_meta;

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

ib_err_t ib_tuple_read_i8(ib_tpl_t ib_tpl, ulint i, int8_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u8(ib_tpl_t ib_tpl, ulint i, uint8_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i16(ib_tpl_t ib_tpl, ulint i, int16_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u16(ib_tpl_t ib_tpl, ulint i, uint16_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i32(ib_tpl_t ib_tpl, ulint i, int32_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u32(ib_tpl_t ib_tpl, ulint i, uint32_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i64(ib_tpl_t ib_tpl, ulint i, int64_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u64(ib_tpl_t ib_tpl, ulint i, uint64_t *ival) {
  IB_CHECK_PANIC();

  const auto err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

const void *ib_col_get_value(ib_tpl_t ib_tpl, ulint i) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  auto dfield = ib_col_get_dfield(tuple, i);
  auto data = dfield_get_data(dfield);
  auto data_len = dfield_get_len(dfield);

  return data_len != UNIV_SQL_NULL ? data : nullptr;
}

ulint ib_col_get_meta(ib_tpl_t ib_tpl, ulint i, ib_col_meta_t *ib_col_meta) {
  return ib_col_get_meta_low(ib_tpl, i, ib_col_meta);
}

ib_tpl_t ib_tuple_clear(ib_tpl_t ib_tpl) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  const auto type = tuple->type;
  auto heap = tuple->heap;

  const auto index = tuple->index;
  const auto n_cols = dtuple_get_n_fields(tuple->ptr);

  mem_heap_empty(heap);

  if (type == TPL_ROW) {
    return ib_row_tuple_new_low(index, n_cols, heap);
  } else {
    return ib_key_tuple_new_low(index, n_cols, heap);
  }
}

ib_err_t ib_tuple_get_cluster_key(ib_crsr_t ib_crsr, ib_tpl_t *ib_dst_tpl, const ib_tpl_t ib_src_tpl) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto src_tuple = reinterpret_cast<ib_tuple_t *>(ib_src_tpl);

  IB_CHECK_PANIC();

  auto clust_index = cursor->prebuilt->m_table->get_clustered_index();

  /* We need to ensure that the src tuple belongs to the same table
  as the open cursor and that it's not a tuple for a cluster index. */
  if (src_tuple->type != TPL_KEY) {
    return DB_ERROR;
  } else if (src_tuple->index->m_table != cursor->prebuilt->m_table) {
    return DB_DATA_MISMATCH;
  } else if (src_tuple->index == clust_index) {
    return DB_ERROR;
  }

  /* Create the cluster index key search tuple. */
  *ib_dst_tpl = ib_clust_search_tuple_create(ib_crsr);

  if (!*ib_dst_tpl) {
    return DB_OUT_OF_MEMORY;
  }

  auto dst_tuple = (ib_tuple_t *)*ib_dst_tpl;
  ut_a(dst_tuple->index == clust_index);

  const auto n_fields = dst_tuple->index->get_n_unique();

  /* Do a deep copy of the data fields. */
  for (ulint i{}; i < n_fields; ++i) {
    auto pos = src_tuple->index->get_nth_field_pos(dst_tuple->index, i);

    ut_a(pos != ULINT_UNDEFINED);

    auto src_field = dtuple_get_nth_field(src_tuple->ptr, pos);
    auto dst_field = dtuple_get_nth_field(dst_tuple->ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data = mem_heap_dup(dst_tuple->heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return DB_SUCCESS;
}

ib_err_t ib_tuple_copy(ib_tpl_t ib_dst_tpl, const ib_tpl_t ib_src_tpl) {
  const ib_tuple_t *src_tuple = (const ib_tuple_t *)ib_src_tpl;
  ib_tuple_t *dst_tuple = (ib_tuple_t *)ib_dst_tpl;

  IB_CHECK_PANIC();

  /* Make sure src and dst are not the same. */
  ut_a(src_tuple != dst_tuple);

  /* Make sure they are the same type and refer to the same index. */
  if (src_tuple->type != dst_tuple->type || src_tuple->index != dst_tuple->index) {

    return DB_DATA_MISMATCH;
  }

  auto n_fields = dtuple_get_n_fields(src_tuple->ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_tuple->ptr));

  /* Do a deep copy of the data fields. */
  for (ulint i = 0; i < n_fields; ++i) {
    auto src_field = dtuple_get_nth_field(src_tuple->ptr, i);
    auto dst_field = dtuple_get_nth_field(dst_tuple->ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data = mem_heap_dup(dst_tuple->heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return DB_SUCCESS;
}

ib_tpl_t ib_sec_search_tuple_create(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto index = cursor->prebuilt->m_index;

  const auto n_cols = index->get_n_unique_in_tree();
  return ib_key_tuple_new(index, n_cols);
}

ib_tpl_t ib_sec_read_tuple_create(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto index = cursor->prebuilt->m_index;
  const auto n_cols = index->get_n_fields();

  return ib_row_tuple_new(index, n_cols);
}

ib_tpl_t ib_clust_search_tuple_create(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto index = cursor->prebuilt->m_table->get_clustered_index();
  const auto n_cols = index->get_n_ordering_defined_by_user();

  return ib_key_tuple_new(index, n_cols);
}

ib_tpl_t ib_clust_read_tuple_create(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  auto index = cursor->prebuilt->m_table->get_clustered_index();

  const auto n_cols = cursor->prebuilt->m_table->get_n_cols();
  return ib_row_tuple_new(index, n_cols);
}

ulint ib_tuple_get_n_user_cols(const ib_tpl_t ib_tpl) {
  const auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  if (tuple->type == TPL_ROW) {
    return tuple->index->m_table->get_n_user_cols();
  } else {
    return tuple->index->get_n_ordering_defined_by_user();
  }
}

ulint ib_tuple_get_n_cols(const ib_tpl_t ib_tpl) {
  const auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  return dtuple_get_n_fields(tuple->ptr);
}

void ib_tuple_delete(ib_tpl_t ib_tpl) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  mem_heap_free(tuple->heap);
}

ib_err_t ib_cursor_truncate(ib_crsr_t *ib_crsr, ib_id_t *table_id) {
  auto cursor = *reinterpret_cast<ib_cursor_t **>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  ut_a(ib_schema_lock_is_exclusive((ib_trx_t)prebuilt->m_trx));

  *table_id = 0;

  auto err = ib_cursor_lock(*ib_crsr, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    Trx *trx;
    Table *table = prebuilt->m_table;

    /* We are going to free the cursor and the prebuilt. Store
    the transaction handle locally. */
    trx = prebuilt->m_trx;
    err = ib_cursor_close(*ib_crsr);
    ut_a(err == DB_SUCCESS);

    *ib_crsr = nullptr;

    /* This function currently commits the transaction
    on success. */
    err = srv_dict_sys->m_ddl.truncate_table(table, trx);

    if (err == DB_SUCCESS) {
      *table_id = table->m_id;
    }
  }

  return err;
}

ib_err_t ib_table_truncate(const char *table_name, ib_id_t *table_id) {
  IB_CHECK_PANIC();

  auto ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  srv_dict_sys->mutex_acquire();

  auto table = srv_dict_sys->table_get(table_name);

  ib_err_t err;
  ib_crsr_t ib_crsr{};

  if (table != nullptr && table->get_first_index()) {
    srv_dict_sys->table_increment_handle_count(table, true);
    err = ib_create_cursor(&ib_crsr, table, 0, reinterpret_cast<Trx *>(ib_trx));
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  srv_dict_sys->mutex_release();

  if (err == DB_SUCCESS) {
    err = ib_schema_lock_exclusive(ib_trx);
  }

  ib_err_t trunc_err;

  if (err == DB_SUCCESS) {
    trunc_err = ib_cursor_truncate(&ib_crsr, table_id);
    ut_a(err == DB_SUCCESS);
  } else {
    trunc_err = err;
  }

  if (ib_crsr != nullptr) {
    err = ib_cursor_close(ib_crsr);
    ut_a(err == DB_SUCCESS);
  }

  if (trunc_err == DB_SUCCESS) {
    ut_a(ib_trx_state(ib_trx) == IB_TRX_NOT_STARTED);

    err = ib_schema_unlock(ib_trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

    err = ib_trx_release(ib_trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = ib_trx_rollback(ib_trx);
    ut_a(err == DB_SUCCESS);
  }

  return trunc_err;
}

ib_err_t ib_table_get_id(const char *table_name, ib_id_t *table_id) {
  IB_CHECK_PANIC();

  srv_dict_sys->mutex_acquire();

  auto err = ib_table_get_id_low(table_name, table_id);

  srv_dict_sys->mutex_release();

  return err;
}

ib_err_t ib_index_get_id(const char *table_name, const char *index_name, ib_id_t *index_id) {
  ib_err_t err = DB_TABLE_NOT_FOUND;

  IB_CHECK_PANIC();

  *index_id = 0;

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  ib_normalize_table_name(normalized_name, table_name);

  auto table = ib_lookup_table_by_name(normalized_name);

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

bool ib_database_create(const char *dbname) {
  const char *ptr;

  for (ptr = dbname; *ptr; ++ptr) {
    if (*ptr == SRV_PATH_SEPARATOR) {
      return false;
    }
  }

  /* Only necessary if file per table is set. */
  if (srv_config.m_file_per_table) {
    return srv_fil->mkdir(dbname);
  }

  return true;
}

ib_err_t ib_database_drop(const char *dbname) {
  ib_trx_t ib_trx;
  ulint len = strlen(dbname);

  IB_CHECK_PANIC();

  if (len == 0) {
    return DB_INVALID_INPUT;
  }

  auto ptr = reinterpret_cast<char *>(mem_alloc(len + 2));

  memset(ptr, 0x0, len + 2);
  strcpy(ptr, dbname);

  ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  /* Drop all the tables in the database first. */
  /* ddl_drop_database() expects a string that ends in '/'. */
  if (ptr[len - 1] != '/') {
    ptr[len] = '/';
  }

  auto err = srv_dict_sys->m_ddl.drop_database(ptr, (Trx *)ib_trx);

  /* Only necessary if file per table is set. */
  if (err == DB_SUCCESS && srv_config.m_file_per_table) {
    srv_fil->rmdir(ptr);
  }

  mem_free(ptr);

  if (err == DB_SUCCESS) {
    auto trx_err = ib_trx_commit(ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  } else {
    auto trx_err = ib_trx_rollback(ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  }

  return err;
}

bool ib_cursor_is_positioned(const ib_crsr_t ib_crsr) {
  const ib_cursor_t *cursor = (const ib_cursor_t *)ib_crsr;
  const Prebuilt *prebuilt = cursor->prebuilt;

  return ib_btr_cursor_is_positioned(prebuilt->m_pcur);
}

ib_err_t ib_schema_lock_shared(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_S_LATCH) {

    srv_dict_sys->freeze_data_dictionary(trx);
  }

  return err;
}

ib_err_t ib_schema_lock_exclusive(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH) {

    srv_dict_sys->lock_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

bool ib_schema_lock_is_exclusive(const ib_trx_t ib_trx) {
  const auto trx = reinterpret_cast<const Trx *>(ib_trx);

  return trx->m_dict_operation_lock_mode == RW_X_LATCH;
}

bool ib_schema_lock_is_shared(const ib_trx_t ib_trx) {
  const auto trx = reinterpret_cast<const Trx *>(ib_trx);

  return trx->m_dict_operation_lock_mode == RW_S_LATCH;
}

ib_err_t ib_schema_unlock(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    srv_dict_sys->unlock_data_dictionary(trx);
  } else if (trx->m_dict_operation_lock_mode == RW_S_LATCH) {
    srv_dict_sys->unfreeze_data_dictionary(trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

ib_err_t ib_cursor_lock(ib_crsr_t ib_crsr, ib_lck_mode_t ib_lck_mode) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;
  auto trx = prebuilt->m_trx;
  auto table = prebuilt->m_table;

  IB_CHECK_PANIC();

  return ib_trx_lock_table_with_retry(trx, table, Lock_mode(ib_lck_mode));
}

ib_err_t ib_table_lock(ib_trx_t ib_trx, ib_id_t table_id, ib_lck_mode_t ib_lck_mode) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto table = ib_open_table_by_id(table_id, false);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  auto heap = mem_heap_create(128);

  ib_qry_proc_t q_proc;

  q_proc.node.sel = sel_node_t::create(heap);

  auto thr = pars_complete_graph_for_exec(q_proc.node.sel, trx, heap);

  q_proc.grph.sel = static_cast<que_fork_t *>(que_node_get_parent(thr));
  q_proc.grph.sel->state = QUE_FORK_ACTIVE;

  trx->m_op_info = "setting table lock";

  ut_a(ib_lck_mode == IB_LOCK_IS || ib_lck_mode == IB_LOCK_IX);

  auto err = srv_lock_sys->lock_table(0, table, (enum Lock_mode)ib_lck_mode, thr);

  trx->m_error_state = err;

  srv_dict_sys->table_decrement_handle_count(table, false);

  mem_heap_free(heap);

  return err;
}

ib_err_t ib_cursor_unlock(ib_crsr_t ib_crsr) {
  ib_err_t err;
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  if (prebuilt->m_trx->m_client_n_tables_locked > 0) {
    --prebuilt->m_trx->m_client_n_tables_locked;
    err = DB_SUCCESS;
  } else {
    err = DB_ERROR;
  }

  return err;
}

ib_err_t ib_cursor_set_lock_mode(ib_crsr_t ib_crsr, ib_lck_mode_t ib_lck_mode) {
  ib_err_t err = DB_SUCCESS;
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  if (ib_lck_mode == IB_LOCK_X) {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IX);
  } else {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IS);
  }

  if (err == DB_SUCCESS) {
    prebuilt->m_select_lock_type = (enum Lock_mode)ib_lck_mode;
    ut_a(prebuilt->m_trx->m_conc_state != TRX_NOT_STARTED);
  }

  return err;
}

void ib_cursor_set_cluster_access(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  prebuilt->m_need_to_access_clustered = true;
}

void ib_cursor_set_simple_select(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;

  prebuilt->m_simple_select = true;
}

void ib_savepoint_take(ib_trx_t ib_trx, const void *name, ulint name_len) {
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
  auto savep = static_cast<trx_named_savept_t *>(mem_alloc(sizeof(trx_named_savept_t) + name_len));

  savep->name = savep + 1;
  savep->savept = trx_savept_take(trx);

  savep->name_len = name_len;
  memcpy(savep->name, name, name_len);

  trx->m_trx_savepoints.push_back(savep);
}

ib_err_t ib_savepoint_release(ib_trx_t ib_trx, const void *name, ulint name_len) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

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

ib_err_t ib_savepoint_rollback(ib_trx_t ib_trx, const void *name, ulint name_len) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  IB_CHECK_PANIC();

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: transaction trying to rollback a  "
      "savepoint "
    );
    ut_print_name(name != nullptr ? (char *)name : "(null)");
    ib_logger(ib_stream, " though it is not started\n");

    return DB_ERROR;
  }

  trx_named_savept_t *savep;

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

  auto err = trx_general_rollback(trx, true, &savep->savept);

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
static void ib_table_get_format(const Table *table, ib_tbl_fmt_t *tbl_fmt, ulint *page_size) noexcept {
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
static int ib_table_schema_visit_table_columns(const Table *table, ib_schema_visitor_t::table_col_t table_col, void *arg) {
  for (ulint i{}; i < table->get_n_cols(); ++i) {
    auto col = table->get_nth_col(i);
    auto col_no = col->get_no();
    auto name = table->get_col_name(col_no);
    auto attr = ib_col_get_attr(col->prtype);
    auto user_err = table_col(arg, name, (ib_col_type_t)col->mtype, col->len, attr);

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
static int ib_table_schema_visit_index_columns(const Index *index, ib_schema_visitor_t::index_col_t index_col, void *arg) noexcept {
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

ib_err_t ib_table_schema_visit(ib_trx_t ib_trx, const char *name, const ib_schema_visitor_t *visitor, void *arg) {
  int user_err{};

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  auto table = ib_lookup_table_by_name(normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table != nullptr) {
    srv_dict_sys->table_increment_handle_count(table, true);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  ulint page_size;
  ib_tbl_fmt_t tbl_fmt;

  ib_table_get_format(table, &tbl_fmt, &page_size);

  /* We need the count of user defined indexes only. */
  auto n_indexes = table->m_indexes.size();

  /* The first index is always the cluster index. */
  auto index = table->get_first_index();

  /* Only the clustered index can be auto generated. */
  if (index->m_n_user_defined_cols == 0) {
    --n_indexes;
  }

  if (visitor->version < ib_schema_visitor_t::Version::TABLE) {

    goto func_exit;

  } else if (visitor->table) {
    user_err = visitor->table(arg, table->m_name, tbl_fmt, page_size, table->m_n_cols, n_indexes);

    if (user_err > 0) {
      goto func_exit;
    }
  }

  if (visitor->version < ib_schema_visitor_t::Version::TABLE_COL) {

    goto func_exit;

  } else if (visitor->table_col) {
    user_err = ib_table_schema_visit_table_columns(table, visitor->table_col, arg);

    if (user_err > 0) {
      goto func_exit;
    }
  }

  if (!visitor->index) {
    goto func_exit;
  } else if (visitor->version < ib_schema_visitor_t::Version::TABLE_AND_INDEX) {
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

      if (visitor->version >= ib_schema_visitor_t::Version::TABLE_AND_INDEX_COL && visitor->index_col) {
        user_err = ib_table_schema_visit_index_columns(index, visitor->index_col, arg);

        if (user_err > 0) {
          break;
        }
      }
    }

    index = index->get_next();

  } while (index != nullptr);

func_exit:
  ut_a(ib_schema_lock_is_exclusive(ib_trx));
  srv_dict_sys->table_decrement_handle_count(table, true);

  return user_err != 0 ? DB_ERROR : DB_SUCCESS;
}

ib_err_t ib_schema_tables_iterate(ib_trx_t ib_trx, ib_schema_visitor_table_all_t visitor, void *arg) {
  ib_err_t err;
  ib_crsr_t ib_crsr;
  ib_err_t crsr_err;
  ib_tpl_t ib_tpl{};

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  auto table = ib_lookup_table_by_name("SYS_TABLES");

  if (table != nullptr) {
    srv_dict_sys->table_increment_handle_count(table, true);
    err = ib_create_cursor(&ib_crsr, table, 0, (Trx *)ib_trx);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  if (err == DB_SUCCESS) {
    err = ib_cursor_first(ib_crsr);
  }

  ib_tpl = ib_clust_read_tuple_create(ib_crsr);

  while (err == DB_SUCCESS) {
    const void *ptr;
    ib_col_meta_t ib_col_meta;

    err = ib_cursor_read_row(ib_crsr, ib_tpl);

    if (err == DB_SUCCESS) {
      ulint len;

      ptr = ib_col_get_value(ib_tpl, 0);
      /* Can't have nullptr columns. */
      ut_a(ptr != nullptr);

      len = ib_col_get_meta_low(ib_tpl, 0, &ib_col_meta);
      ut_a(len != UNIV_SQL_NULL);

      if (visitor(arg, (const char *)ptr, len)) {
        break;
      }

      err = ib_cursor_next(ib_crsr);
    }
  }

  ib_tuple_delete(ib_tpl);

  crsr_err = ib_cursor_close(ib_crsr);
  ut_a(crsr_err == DB_SUCCESS);

  if (err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }

  return err;
}

#if 0
/**
 * @brief Convert and write an INT column value to an InnoDB tuple.
 *
 * @param[in] ib_tpl - in: tuple to write to
 * @param[in] col_no - in: column number
 * @param[in] value - in: integer value
 * @param[in] value_len - in: sizeof value type
 *
 * @return DB_SUCCESS or error
 */
static ib_err_t ib_tuple_write_int(ib_tpl_t ib_tpl, ulint col_no, const void *value, ulint value_len) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  ut_a(col_no < ib_tuple_get_n_cols(ib_tpl));

  auto dfield = ib_col_get_dfield(tuple, col_no);
  auto data_len = dfield_get_len(dfield);
  auto type_len = dtype_get_len(dfield_get_type(dfield));

  if (dtype_get_mtype(dfield_get_type(dfield)) != DATA_INT || value_len != data_len) {
    return DB_DATA_MISMATCH;
  } else {
    return ib_col_set_value(ib_tpl, col_no, value, type_len);
  }
}
#endif

ib_err_t ib_tuple_write_i8(ib_tpl_t ib_tpl, int col_no, int8_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_i16(ib_tpl_t ib_tpl, int col_no, int16_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_i32(ib_tpl_t ib_tpl, int col_no, int32_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_i64(ib_tpl_t ib_tpl, int col_no, int64_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_u8(ib_tpl_t ib_tpl, int col_no, uint8_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_u16(ib_tpl_t ib_tpl, int col_no, uint16_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_u32(ib_tpl_t ib_tpl, int col_no, uint32_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

ib_err_t ib_tuple_write_u64(ib_tpl_t ib_tpl, int col_no, uint64_t val) {
  return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
}

void ib_cursor_stmt_begin(ib_crsr_t ib_crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);

  cursor->prebuilt->m_sql_stat_start = true;
}

ib_err_t ib_tuple_write_double(ib_tpl_t ib_tpl, int col_no, double val) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

ib_err_t ib_tuple_read_double(ib_tpl_t ib_tpl, ulint col_no, double *dval) {
  ib_err_t err;
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    ib_col_copy_value_low(ib_tpl, col_no, dval, sizeof(*dval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

ib_err_t ib_tuple_write_float(ib_tpl_t ib_tpl, int col_no, float val) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);
  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

ib_err_t ib_tuple_read_float(ib_tpl_t ib_tpl, ulint col_no, float *fval) {
  ib_err_t err;
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    ib_col_copy_value_low(ib_tpl, col_no, fval, sizeof(*fval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return err;
}

void ib_logger_set(ib_msg_log_t, ib_msg_stream_t ib_msg_stream) {
  log_info("Setting log output stream.");
  ib_stream = (ib_stream_t)ib_msg_stream;
}

void ib_set_panic_handler(ib_panic_handler_t new_panic_handler) {
  ib_panic = std::move(new_panic_handler);
}

extern ib_trx_is_interrupted_handler_t ib_trx_is_interrupted;

void ib_set_trx_is_interrupted_handler(ib_trx_is_interrupted_handler_t handler) {
  ib_trx_is_interrupted = handler;
}

ib_err_t ib_get_duplicate_key(ib_trx_t ib_trx, const char **table_name, const char **index_name) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);

  if (trx->m_error_info == nullptr) {
    return DB_ERROR;
  }

  *table_name = trx->m_error_info->get_table_name();
  *index_name = trx->m_error_info->m_name;

  return DB_SUCCESS;
}

ib_err_t ib_get_table_statistics(ib_crsr_t ib_crsr, ib_table_stats_t *table_stats, size_t sizeof_ib_table_stats_t) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto table = cursor->prebuilt->m_table;

  if (!table->m_stats.m_initialized) {
    srv_dict_sys->update_statistics(table);
  }

  table_stats->stat_n_rows = table->m_stats.m_n_rows;
  table_stats->stat_clustered_index_size = table->m_stats.m_clustered_index_size * UNIV_PAGE_SIZE;
  table_stats->stat_sum_of_other_index_sizes = table->m_stats.m_sum_of_secondary_index_sizes * UNIV_PAGE_SIZE;
  table_stats->stat_modified_counter = table->m_stats.m_modified_counter;

  return DB_SUCCESS;
}

ib_err_t ib_get_index_stat_n_diff_key_vals(ib_crsr_t ib_crsr, const char *index_name, uint64_t *ncols, int64_t **n_diff) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto table = cursor->prebuilt->m_table;

  if (!table->m_stats.m_initialized) {
    srv_dict_sys->update_statistics(table);
  }

  auto index = table->get_index_on_name(index_name);

  if (index == nullptr) {
    return DB_NOT_FOUND;
  }

  *ncols = index->m_n_uniq;

  *n_diff = (int64_t *)malloc(sizeof(int64_t) * index->m_n_uniq);

  srv_dict_sys->index_stat_mutex_enter(index);

  memcpy(*n_diff, index->m_stats.m_n_diff_key_vals, sizeof(int64_t) * index->m_n_uniq);

  srv_dict_sys->index_stat_mutex_exit(index);

  return DB_SUCCESS;
}

ib_err_t ib_update_table_statistics(ib_crsr_t crsr) {
  auto cursor = reinterpret_cast<ib_cursor_t *>(crsr);
  auto table = cursor->prebuilt->m_table;

  srv_dict_sys->update_statistics(table);

  return DB_SUCCESS;
}

ib_err_t ib_error_inject(int error_to_inject) {
  if (error_to_inject == 1) {
    log_fatal("test panic message");
    return DB_SUCCESS;
  } else {
    return DB_ERROR;
  }
}

ib_err_t ib_parallel_select_count_star(ib_trx_t ib_trx, std::vector<ib_crsr_t> &ib_crsrs, size_t n_threads, uint64_t &n_rows) {
  ut_a(n_threads > 1);
  ut_a(!ib_crsrs.empty());

  auto trx = reinterpret_cast<Trx *>(ib_trx);

  ut::Sharded_counter<Parallel_reader::MAX_THREADS> n_recs{};

  n_recs.clear();

  const Parallel_reader::Scan_range full_scan;

  dberr_t err{DB_SUCCESS};
  Parallel_reader reader(n_threads);
  std::vector<Table *> tables{};

  for (auto &ib_crsr : ib_crsrs) {
    auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
    Parallel_reader::Config config(full_scan, cursor->prebuilt->m_index);

    err = reader.add_scan(trx, config, [&](const Parallel_reader::Ctx *ctx) {
      n_recs.inc(1, ctx->thread_id());
      return DB_SUCCESS;
    });

    if (err != DB_SUCCESS) {
      break;
    }
  }

  n_threads = Parallel_reader::available_threads(n_threads, false);

  if (err == DB_SUCCESS) {
    err = reader.run(n_threads);
  }

  if (err == DB_OUT_OF_RESOURCES) {
    ut_a(n_threads > 0);

    log_warn(
      "Resource not available to create threads for parallel scan."
      " Falling back to single thread mode."
    );

    err = reader.run(0);
  }

  if (err == DB_SUCCESS) {
    n_rows = n_recs.value();
  }

  return err;
}

static dberr_t check_table(Trx *trx, Index *index, size_t n_threads) {
  ut_a(n_threads > 1);

  using Shards = ut::Sharded_counter<Parallel_reader::MAX_THREADS>;

  Shards n_recs{};
  Shards n_dups{};
  Shards n_corrupt{};

  n_dups.clear();
  n_recs.clear();
  n_corrupt.clear();

  using Tuples = std::vector<DTuple *>;
  using Heaps = std::vector<mem_heap_t *>;
  using Blocks = std::vector<const Buf_block *>;

  Heaps heaps;
  Tuples prev_tuples;
  Blocks prev_blocks;

  for (size_t i = 0; i < n_threads; ++i) {
    heaps.push_back(mem_heap_create(4096));
  }

  Parallel_reader reader(n_threads);
  Parallel_reader::Scan_range full_scan;
  Parallel_reader::Config config(full_scan, index);

  auto err = reader.add_scan(trx, config, [&](const Parallel_reader::Ctx *ctx) {
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
      Phy_rec record{index, rec};

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

        log_err(std::format(
          "Index records in a wrong order in index {} of table {}: {}, {}",
          index->m_name,
          index->m_table->m_name,
          dtuple_os.str(),
          rec_os.str()
        ));

        /* Continue reading */
      } else if (index->is_unique() && !contains_null && matched_fields >= index->get_n_ordering_defined_by_user()) {

        std::ostringstream rec_os{};
        std::ostringstream dtuple_os{};

        n_dups.inc(1, id);
        rec_os << rec.to_string();
        prev_tuple->print(dtuple_os);

        log_err(std::format(
          "Duplicate key in {} of table {}: {}, {}", index->m_name, index->m_table->m_name, dtuple_os.str(), rec_os.str()
        ));
      }
    }

    if (prev_blocks[id] != block || prev_blocks[id] == nullptr) {
      mem_heap_empty(heap);

      Phy_rec record{index, rec};

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

    err = reader.run(n_threads);
  }

  if (err == DB_OUT_OF_RESOURCES) {
    ut_a(n_threads > 0);
    log_warn("Resource not available to create threads for parallel scan. Trying single threaded mode.");

    err = reader.run(0);
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

ib_err_t ib_check_table(ib_trx_t *ib_trx, ib_crsr_t ib_crsr, size_t n_threads) {
  auto trx = reinterpret_cast<Trx *>(ib_trx);
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;
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
    n_threads = Parallel_reader::available_threads(n_threads, false);

    auto rv = trx->assign_read_view();
    ut_a(rv != nullptr);

    auto trx = prebuilt->m_trx;

    ut_a(prebuilt->m_table == index->m_table);

    std::vector<Index *> indexes;

    indexes.push_back(index);

    return check_table(trx, index, n_threads);
  } else {
    log_err("Invalid transaction state for check table");
    return DB_ERROR;
  }
}
