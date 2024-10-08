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
#include <vector>

#include "api0api.h"
#include "api0misc.h"
#include "api0ucode.h"
#include "btr0blob.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0crea.h"
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
become row_prebuilt_t. */
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
  row_prebuilt_t *prebuilt;
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
  dict_table_t *table;

  /** Parent table schema that owns this instance */
  ib_table_def_t *schema;

  /** True if clustered index */
  bool clustered;

  /** True if unique index */
  bool unique;

  /** Vector of columns */
  std::vector<ib_key_col_t *> *cols;

  /* User transacton covering the DDL operations */
  trx_t *usr_trx;
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
  dict_table_t *table;
};

/* InnoDB tuple used for key operations. */
struct ib_tuple_t {
  /** Heap used to build this and for copying the column values. */
  mem_heap_t *heap;

  /** Tuple discriminitor. */
  ib_tuple_type_t type;

  /** Index for tuple can be either secondary or cluster index. */
  const dict_index_t *index;

  /** The internal tuple instance */
  dtuple_t *ptr;
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
 * @return 1 if a is greater, 0 if equal, -1 if less than b
 */
static int ib_default_compare(
  const ib_col_meta_t *ib_col_meta, const ib_byte_t *p1, ulint p1_len, const ib_byte_t *p2, ulint p2_len
) {
  (void)ib_col_meta;

  auto ret = memcmp(p1, p2, ut_min(p1_len, p2_len));

  if (ret == 0) {
    ret = p1_len - p2_len;
  }

  return ret < 0 ? -1 : ((ret > 0) ? 1 : 0);
}

/** Check if the Innodb persistent cursor is positioned.
@param[in] pcur                 InnoDB persistent cursor
@return	true if positioned */
static bool ib_btr_cursor_is_positioned(const Btree_pcursor *pcur) {
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
 * @param tid - table id to lookup
 * @param locked - true if own dict mutex
 * @return table instance if found
 */
static dict_table_t *ib_open_table_by_id(ib_id_t tid, bool locked) {
  /* We only return the lower 32 bits of the uint64_t. */
  ut_a(tid < 0xFFFFFFFFULL);
  auto table_id = tid;

  if (!locked) {
    dict_mutex_enter();
  }

  auto table = dict_table_get_using_id(srv_config.m_force_recovery, table_id, true);

  if (table != nullptr && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n", table->name);

    dict_table_decrement_handle_count(table, true);

    table = nullptr;
  }

  if (!locked) {
    dict_mutex_exit();
  }

  return table;
}

/** Open a table using the table name, if found then increment table ref count.
 * @param[in] name  Table name
@return	table instance if found */
static dict_table_t *ib_open_table_by_name(const char *name) {
  auto table = dict_table_get(name, true);

  if (table != nullptr && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n", name);

    dict_table_decrement_handle_count(table, false);

    table = nullptr;
  }

  return table;
}

/**
 * Find table using table name.
 * @param name - table name to lookup
 * @return table instance if found
 */
static dict_table_t *ib_lookup_table_by_name(const char *name) {
  auto table = dict_table_get_low(name);

  if (table != nullptr && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n", name);

    table = nullptr;
  }

  return table;
}

/** Increments innobase_active_counter and every INNOBASE_WAKE_INTERVALth
time calls srv_active_wake_master_thread. This function should be used
when a single database operation may introduce a small need for
server utility activity, like checkpointing. */
static void ib_wake_master_thread() {
  static ulint ib_signal_counter = 0;

  ++ib_signal_counter;

  if ((ib_signal_counter % INNOBASE_WAKE_INTERVAL) == 0) {
    InnoDB::active_wake_master_thread();
  }
}

#if 0

/** Calculate the length of the column data less trailing space. */
static
ulint ib_varchar_len( const dtype_t*	dtype, ib_byte_t*	ptr, ulint		len) {
	/* Handle UCS2 strings differently. */
	ulint		mbminlen = dtype_get_mbminlen(dtype);

	if (mbminlen == 2) {
		/* SPACE = 0x0020. */
		/* Trim "half-chars", just in case. */
		len &= ~1;

		while (len >= 2
		       && ptr[len - 2] == 0x00
		       && ptr[len - 1] == 0x20) {

			len -= 2;
		}

	} else {
		ut_a(mbminlen == 1);

		/* SPACE = 0x20. */
		while (len > 0 && ptr[len - 1] == 0x20) {

			--len;
		}
	}

	return(len);
}

/** Calculate the max row size of the columns in a cluster index.
@return	max row length */
static ulint ib_get_max_row_len(dict_index_t *cluster) {
  ulint i;
  ulint max_len = 0;
  ulint n_fields = cluster->n_fields;

  /* Add the size of the ordering columns in the
  clustered index. */
  for (i = 0; i < n_fields; ++i) {
    const dict_col_t *col;

    col = dict_index_get_nth_col(cluster, i);

    /* Use the maximum output size of
    mach_write_compressed(), although the encoded
    length should always fit in 2 bytes. */
    max_len += dict_col_get_max_size(col);
  }

  return max_len;
}
#endif

/**
 * Read the columns from a record into a tuple.
 *
 * @param rec           Record to read
 * @param tuple         Tuple to read into
 */
static void ib_read_tuple(const rec_t *rec, ib_tuple_t *tuple) {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  dtuple_t *dtuple = tuple->ptr;
  const dict_index_t *dindex = tuple->index;

  rec_offs_init(offsets_);

  {
    Phy_rec record{dindex, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &tuple->heap, Source_location{});
  }

  auto rec_meta_data = rec_get_info_bits(rec);
  dtuple_set_info_bits(dtuple, rec_meta_data);

  /* Make a copy of the rec. */
  auto ptr = reinterpret_cast<void *>(mem_heap_alloc(tuple->heap, rec_offs_size(offsets)));
  auto copy = rec_copy(ptr, rec, offsets);

  /* Avoid a debug assertion in rec_offs_validate(). */
  ut_d(rec_offs_make_valid(rec, dindex, (ulint *)offsets));

  auto n_index_fields = ut_min(rec_offs_n_fields(offsets), dtuple_get_n_fields(dtuple));

  for (ulint i = 0; i < n_index_fields; ++i) {
    dfield_t *dfield;

    if (tuple->type == TPL_ROW) {
      auto index_field = dict_index_get_nth_field(dindex, i);
      auto col = dict_field_get_col(index_field);
      auto col_no = dict_col_get_no(col);

      dfield = dtuple_get_nth_field(dtuple, col_no);

    } else {
      dfield = dtuple_get_nth_field(dtuple, i);
    }

    ulint len;
    auto data = rec_get_nth_field(copy, offsets, i, &len);

    /* Fetch and copy any externally stored column. */
    if (rec_offs_nth_extern(offsets, i)) {

      Blob blob(srv_fsp, srv_btree_sys);

      data = blob.copy_externally_stored_field(copy, offsets, i, &len, tuple->heap);

      ut_a(len != UNIV_SQL_NULL);
    }

    dfield_set_data(dfield, data, len);
  }
}

/**
 * Create an InnoDB key tuple.
 *
 * @param dict_index The index for which the tuple is required.
 * @param n_cols The number of user-defined columns.
 * @param heap The memory heap.
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_key_tuple_new_low(const dict_index_t *dict_index, ulint n_cols, mem_heap_t *heap) {

  auto tuple = reinterpret_cast<ib_tuple_t *>(mem_heap_alloc(heap, sizeof(ib_tuple_t)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->heap = heap;
  tuple->index = dict_index;
  tuple->type = TPL_KEY;

  /* Is it a generated clustered index ? */
  if (n_cols == 0) {
    ++n_cols;
  }

  tuple->ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  dict_index_copy_types(tuple->ptr, dict_index, n_cols);

  for (ulint i = 0; i < n_cols; i++) {

    dfield_t *dfield;

    dfield = dtuple_get_nth_field(tuple->ptr, i);
    dfield_set_null(dfield);
  }

  auto n_cmp_cols = dict_index_get_n_ordering_defined_by_user(dict_index);

  dtuple_set_n_fields_cmp(tuple->ptr, n_cmp_cols);

  return (ib_tpl_t)tuple;
}

/**
 * Create an InnoDB key tuple.
 *
 * @param dict_index The index of the tuple.
 * @param n_cols The number of user-defined columns.
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_key_tuple_new(const dict_index_t *dict_index, ulint n_cols) {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return ib_key_tuple_new_low(dict_index, n_cols, heap);
}

/**
 * Create an InnoDB row tuple.
 *
 * @param dict_index The index of the tuple.
 * @param n_cols The number of columns in the tuple.
 * @param heap The memory heap.
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_row_tuple_new_low(const dict_index_t *dict_index, ulint n_cols, mem_heap_t *heap) {
  auto tuple = reinterpret_cast<ib_tuple_t *>(mem_heap_alloc(heap, sizeof(ib_tuple_t)));

  if (tuple == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  tuple->heap = heap;
  tuple->index = dict_index;
  tuple->type = TPL_ROW;

  tuple->ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  dict_table_copy_types(tuple->ptr, dict_index->table);

  return (ib_tpl_t)tuple;
}

/**
 * Create an InnoDB row tuple.
 *
 * @param dict_index The index of the tuple.
 * @param n_cols The number of columns in the tuple.
 * @return Tuple instance created, or nullptr.
 */
static ib_tpl_t ib_row_tuple_new(const dict_index_t *dict_index, ulint n_cols) {
  auto heap = mem_heap_create(64);

  if (heap == nullptr) {
    return nullptr;
  }

  return ib_row_tuple_new_low(dict_index, n_cols, heap);
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
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  ut_a(ib_trx_level >= IB_TRX_READ_UNCOMMITTED);
  ut_a(ib_trx_level <= IB_TRX_SERIALIZABLE);

  if (trx->m_conc_state == TRX_NOT_STARTED) {
    auto started = trx_start(trx, ULINT_UNDEFINED);
    ut_a(started);

    trx->m_isolation_level = static_cast<Trx_isolation>(ib_trx_level);
  } else {
    err = DB_ERROR;
  }

  trx->m_client_ctx = nullptr;

  return err;
}

void ib_trx_set_client_data(ib_trx_t ib_trx, void *client_data) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  trx->m_client_ctx = client_data;
}

ib_trx_t ib_trx_begin(ib_trx_level_t ib_trx_level) {
  auto trx = trx_allocate_for_client(nullptr);
  auto started = ib_trx_start((ib_trx_t)trx, ib_trx_level);

  ut_a(started);

  return reinterpret_cast<ib_trx_t>(trx);
}

ib_trx_state_t ib_trx_state(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  return static_cast<ib_trx_state_t>(trx->m_conc_state);
}

ib_err_t ib_trx_release(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  IB_CHECK_PANIC();

  ut_ad(trx != nullptr);
  trx_free_for_client(trx);

  return DB_SUCCESS;
}

ib_err_t ib_trx_commit(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  IB_CHECK_PANIC();

  auto err = trx_commit(trx);
  ut_a(err == DB_SUCCESS);

  err = ib_schema_unlock(ib_trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  err = ib_trx_release(ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread();

  return DB_SUCCESS;
}

ib_err_t ib_trx_rollback(ib_trx_t ib_trx) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);

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
 * @param name The column name.
 * @param ib_col_type The column type.
 * @param ib_col_attr The column attribute.
 * @param len The length of the column.
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
 * @param indexes Vector of indexes.
 * @param name Index name.
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
 * @param ib_col The column definition.
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
 * @param ib_col The column list head
 * @return The main type.
 */
static ulint ib_col_get_mtype(const ib_col_t *ib_col) {
  /* Note: The api0api.h types should map directly to
  the internal numeric codes. */
  return ib_col->ib_col_type;
}

/** Find a column in the the column vector with the same name.
@param[in] cols                 Column list.
@param[in] name                 Column name to file.
@return	col. def. if found else nullptr */
static const ib_col_t *ib_table_find_col(const std::vector<ib_col_t *> &cols, const char *name) {
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
static const ib_key_col_t *ib_index_find_col(const std::vector<ib_key_col_t *> &cols, const char *name) {
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
  ib_col_t *ib_col;
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
    mem_heap_t *heap;

    heap = table_def->heap;

    ib_col = reinterpret_cast<ib_col_t *>(mem_heap_zalloc(heap, sizeof(*ib_col)));

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
  ib_table_def_t *table_def = reinterpret_cast<ib_table_def_t *>(ib_tbl_sch);

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
    ib_index_def_t *index_def;
    mem_heap_t *heap = table_def->heap;

    index_def = (ib_index_def_t *)mem_heap_zalloc(heap, sizeof(*index_def));

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
    dict_table_decrement_handle_count(table_def->table, false);
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
 * @param norm_name[out] Normalized name as a null-terminated string.
 * @param name[in] Table name string.
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
    const char *db_name;
    const char *table_name;

    table_name = ptr + 1;

    --ptr;

    while (ptr >= name && *ptr != '\\' && *ptr != '/') {
      ptr--;
    }

    db_name = ptr + 1;

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
 * @param name The table name to check.
 * @return DB_SUCCESS or err code.
 */
static ib_err_t ib_table_name_check(const char *name) {
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
    ib_table_def_t *table_def;

    table_def = (ib_table_def_t *)mem_heap_zalloc(heap, sizeof(*table_def));

    if (table_def == nullptr) {
      err = DB_OUT_OF_MEMORY;

      mem_heap_free(heap);
    } else {
      char *normalized_name;

      table_def->heap = heap;

      normalized_name = mem_heap_strdup(heap, name);

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
 * @return -1 or column number.
 */
static int ib_index_get_col_no(const ib_index_def_t *ib_index_def, const char *name) {
  int col_no;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != nullptr) {
    col_no = dict_table_get_col_no(ib_index_def->table, name);
  } else {
    const ib_col_t *ib_col;
    ib_col = ib_table_find_col(*ib_index_def->schema->cols, name);

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
 * @param ib_index_def The index definition.
 * @param name The column name to check.
 * @return True if allowed.
 */
static int ib_index_is_prefix_allowed(const ib_index_def_t *ib_index_def, const char *name) {
  bool allowed = true;
  ulint mtype = ULINT_UNDEFINED;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != nullptr) {
    const dict_col_t *col;
    int col_no;

    col_no = dict_table_get_col_no(ib_index_def->table, name);
    ut_a(col_no != -1);

    col = dict_table_get_nth_col(ib_index_def->table, col_no);
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
    mem_heap_t *heap;
    ib_key_col_t *ib_col;

    heap = index_def->heap;
    ib_col = (ib_key_col_t *)mem_heap_zalloc(heap, sizeof(*ib_col));

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
  dict_table_t *table = nullptr;
  char *normalized_name;
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_usr_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return DB_INVALID_INPUT;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return DB_INVALID_INPUT;
  }

  normalized_name = static_cast<char *>(mem_alloc(strlen(table_name) + 1));
  ib_normalize_table_name(normalized_name, table_name);

  table = ib_lookup_table_by_name(normalized_name);

  mem_free(normalized_name);
  normalized_name = nullptr;

  if (table == nullptr) {
    err = DB_TABLE_NOT_FOUND;
  } else if (dict_table_get_index_on_name(table, name) != nullptr) {
    err = DB_DUPLICATE_KEY;
  } else {
    mem_heap_t *heap = mem_heap_create(1024);

    if (heap == nullptr) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_index_def_t *index_def;

      index_def = (ib_index_def_t *)mem_heap_zalloc(heap, sizeof(*index_def));

      if (index_def == nullptr) {
        err = DB_OUT_OF_MEMORY;

        mem_heap_free(heap);
      } else {
        index_def->heap = heap;

        index_def->table = table;

        index_def->name = mem_heap_strdup(heap, name);

        index_def->cols = new std::vector<ib_key_col_t *>();

        index_def->usr_trx = (trx_t *)ib_usr_trx;

        *ib_idx_sch = (ib_idx_sch_t)index_def;
      }
    }
  }

  return err;
}

/**
 * Get an index definition that is tagged as a clustered index.
 *
 * @param indexes The index defs. to search.
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

  auto index_def = (ib_index_def_t *)ib_idx_sch;

  /* If this index schema is part of a table schema then we need
  to check the state of the other indexes. */

  if (index_def->schema != nullptr) {
    ib_index_def_t *ib_clust_index_def;

    ib_clust_index_def = ib_find_clustered_index(*index_def->schema->indexes);

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
  ib_index_def_t *index_def;
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  index_def = (ib_index_def_t *)ib_idx_sch;
  index_def->unique = true;

  return err;
}

void ib_index_schema_delete(ib_idx_sch_t ib_idx_sch) {
  ib_index_def_t *index_def = (ib_index_def_t *)ib_idx_sch;

  ut_a(index_def->schema == nullptr);

  delete index_def->cols;
  mem_heap_free(index_def->heap);
}

/**
 * Convert the table definition table attributes to the internal format.
 *
 * @param table_def The table definition.
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
 * @param ib_index_def Key definition for index.
 * @param clustered True if clustered index.
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
 * @param usr_trx The transaction.
 * @param table The parent table of the index.
 * @param ib_index_def The index definition.
 * @param create True if part of table create.
 * @param dict_index The index created.
 * @return DB_SUCCESS or err code.
 */
static ib_err_t ib_build_secondary_index(
  trx_t *usr_trx, dict_table_t *table, ib_index_def_t *ib_index_def, bool create, dict_index_t **dict_index
) {
  ib_err_t err;
  trx_t *ddl_trx;
  const index_def_t *index_def;

  IB_CHECK_PANIC();

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (!create) {
    bool started;

    ddl_trx = trx_allocate_for_client(nullptr);
    started = trx_start(ddl_trx, ULINT_UNDEFINED);
    ut_a(started);
  } else {
    ddl_trx = usr_trx;
  }

  /* Set the CLUSTERED flag to false. */
  index_def = ib_copy_index_definition(ib_index_def, false);

  ut_a(!(index_def->ind_type & DICT_CLUSTERED));

  ddl_trx->m_op_info = "creating secondary index";

  if (!(create || ib_schema_lock_is_exclusive((ib_trx_t)usr_trx))) {
    err = ib_schema_lock_exclusive((ib_trx_t)usr_trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  if (!create) {
    /* Flag this transaction as a dictionary operation, so that
    the data dictionary will be locked in crash recovery. */
    trx_set_dict_operation(ddl_trx, TRX_DICT_OP_INDEX);
  }

  *dict_index = row_merge_create_index(ddl_trx, table, index_def);

  if (!create) {
    /* Even if the user locked the schema, we release it here and
    build the index without holding the dictionary lock. */
    err = ib_schema_unlock((ib_trx_t)usr_trx);
    ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);
  }

  err = ddl_trx->error_state;

  if (!create) {
    /* Commit the data dictionary transaction in order to release
    the table locks on the system tables. */
    auto err = trx_commit(ddl_trx);
    ut_a(err == DB_SUCCESS);

    trx_free_for_client(ddl_trx);
  }

  ut_a(usr_trx->m_conc_state != TRX_NOT_STARTED);

  if (*dict_index != nullptr) {
    ut_a(err == DB_SUCCESS);

    (*dict_index)->cmp_ctx = nullptr;

    /* Read the clustered index records and build the index. */
    err = row_merge_build_indexes(usr_trx, table, table, dict_index, 1, nullptr);
  }

  return err;
}

/**
 * Create a temporary tablename using table name and id.
 *
 * @param heap Memory heap.
 * @param id Identifier [0-9a-zA-Z].
 * @param table_name Table name.
 * @return Temporary tablename.
 */
static char *ib_table_create_temp_name(mem_heap_t *heap, char id, const char *table_name) {
  static const char suffix[] = "# ";

  auto len = strlen(table_name);
  auto name = reinterpret_cast<char *>(mem_heap_zalloc(heap, len + sizeof(suffix)));

  memcpy(name, table_name, len);
  memcpy(name + len, suffix, sizeof(suffix));
  name[len + (sizeof(suffix) - 2)] = id;

  return name;
}

/**
 * Create an index definition from the index.
 *
 * @param dict_index The index definition to copy.
 * @param index_def The index definition.
 * @param heap The heap where allocated.
 */
static void ib_index_create_def(const dict_index_t *dict_index, index_def_t *index_def, mem_heap_t *heap) {
  auto n_fields = dict_index->n_user_defined_cols;

  index_def->fields = (merge_index_field_t *)mem_heap_zalloc(heap, n_fields * sizeof(*index_def->fields));

  index_def->name = dict_index->name;
  index_def->n_fields = n_fields;
  index_def->ind_type = dict_index->type & ~DICT_CLUSTERED;

  const dict_field_t *dfield = dict_index->fields;

  for (ulint i = 0; i < n_fields; ++i, ++dfield) {
    auto def_field = &index_def->fields[i];

    def_field->field_name = dfield->name;
    def_field->prefix_len = dfield->prefix_len;
  }
}

/**
 * Create and return an array of index definitions on a table. Skip the
 * old clustered index if it's a generated clustered index. If there is a
 * user defined clustered index on the table its CLUSTERED flag will be unset.
 *
 * @param trx in: transaction
 * @param table in: table definition
 * @param heap in: heap where space for index definitions are allocated
 * @param n_indexes out: number of indexes returned
 *
 * @return index definitions or nullptr
 */
static index_def_t *ib_table_create_index_defs(trx_t *trx, const dict_table_t *table, mem_heap_t *heap, ulint *n_indexes) {
  auto sz = sizeof(index_def_t) * UT_LIST_GET_LEN(table->indexes);
  auto index_defs = reinterpret_cast<index_def_t *>(mem_heap_zalloc(heap, sz));
  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);

  ut_a(err == DB_SUCCESS);

  auto dict_index = dict_table_get_first_index(table);

  /* Skip a generated cluster index. */
  if (ib_utf8_strcasecmp(dict_index->name, GEN_CLUST_INDEX) == 0) {
    ut_a(dict_index_get_nth_col(dict_index, 0)->mtype == DATA_SYS);
    dict_index = dict_table_get_next_index(dict_index);
  }

  while (dict_index != nullptr) {
    ib_index_create_def(dict_index, index_defs++, heap);
    dict_index = dict_table_get_next_index(dict_index);
  }

  err = ib_schema_unlock((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  return index_defs;
}

/**
 * Create a cluster index specified by the user. The cluster index
 * shouldn't already exist.
 *
 * @param trx in: transaction
 * @param table in: parent table of index
 * @param ib_index_def in: index definition
 * @param dict_index out: index created
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_cluster_index(trx_t *trx, dict_table_t *table, ib_index_def_t *ib_index_def, dict_index_t **dict_index) {
  IB_CHECK_PANIC();

  ut_a(!ib_index_def->cols->empty());

  /* Set the CLUSTERED flag to true. */
  auto index_def = ib_copy_index_definition(ib_index_def, true);

  trx->m_op_info = "creating clustered index";

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  auto err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *dict_index = row_merge_create_index(trx, table, index_def);

    err = trx->error_state;
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
 * @param trx in: transaction
 * @param src_table in: table to clone from
 * @param new_table in: table to clone to
 * @param heap in: memory heap to use
 *
 * @return DB_SUCCESS or error code
 */
static ib_err_t ib_table_clone_indexes(trx_t *trx, dict_table_t *src_table, dict_table_t *new_table, mem_heap_t *heap) {
  ulint n_index_defs = 0;
  auto index_defs = ib_table_create_index_defs(trx, src_table, heap, &n_index_defs);

  ut_a(index_defs != nullptr);

  for (ulint i = 0; i < n_index_defs; ++i) {
    ut_a(!(index_defs[i].ind_type & DICT_CLUSTERED));
    auto dict_index = row_merge_create_index(trx, new_table, &index_defs[i]);

    if (dict_index == nullptr) {
      return trx->error_state;
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
 * @param trx in: transaction
 * @param src_table in: table to clone from
 * @param new_table out: cloned table
 * @param ib_index_def in: new cluster index definition
 * @param heap in: memory heap to use
 *
 * @return DB_SUCCESS or error code
 */
static ib_err_t ib_table_clone(
  trx_t *trx, dict_table_t *src_table, dict_table_t **new_table, ib_index_def_t *ib_index_def, mem_heap_t *heap
) {
  ib_err_t err;
  const index_def_t *index_def;
  char *new_table_name;

  new_table_name = ib_table_create_temp_name(heap, '1', src_table->name);

  err = ib_schema_lock_exclusive((ib_trx_t)trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  /* Set the CLUSTERED flag to true. */
  index_def = ib_copy_index_definition(ib_index_def, true);

  /* Create the new table and the cluster index. */
  *new_table = row_merge_create_temporary_table(new_table_name, index_def, src_table, trx);

  if (!new_table) {
    err = trx->error_state;
  } else {
    trx->table_id = (*new_table)->id;

    err = ib_table_clone_indexes(trx, src_table, *new_table, heap);
  }

  err = ib_schema_unlock((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS);

  return err;
}

/**
 * Copy the data from the source table to dst table.
 *
 * @param trx in: transaction
 * @param src_table in: table to copy from
 * @param dst_table in: table to copy to
 * @param heap in: heap to use
 *
 * @return DB_SUCCESS or error code
 */
static ib_err_t ib_table_copy(trx_t *trx, dict_table_t *src_table, dict_table_t *dst_table, mem_heap_t *heap) {
  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);

  if (err != DB_SUCCESS) {
    return err;
  }

  auto n_indexes = UT_LIST_GET_LEN(dst_table->indexes);
  auto indexes = reinterpret_cast<dict_index_t **>(mem_heap_zalloc(heap, n_indexes * sizeof(dict_index_t *)));
  auto dict_index = dict_table_get_first_index(dst_table);

  n_indexes = 0;

  /* Copy the indexes to an array. */
  while (dict_index != nullptr) {
    indexes[n_indexes++] = dict_index;
    dict_index = dict_table_get_next_index(dict_index);
  }
  ut_a(n_indexes == UT_LIST_GET_LEN(dst_table->indexes));

  err = ib_schema_unlock((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  /* Build the actual indexes. */
  return row_merge_build_indexes(trx, src_table, dst_table, indexes, n_indexes, nullptr);
}

/**
 * Create a default cluster index, this usually means the user didn't
 * create a table with a primary key.
 *
 * @param trx in: transaction
 * @param table in: parent table of index
 * @param dict_index out: index created
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_default_cluster_index(trx_t *trx, dict_table_t *table, dict_index_t **dict_index) {
  index_def_t index_def;

  index_def.name = GEN_CLUST_INDEX;
  index_def.ind_type = DICT_CLUSTERED;
  index_def.n_fields = 0;
  index_def.fields = nullptr;

  trx->m_op_info = "creating default clustered index";

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  ut_a(ib_schema_lock_is_exclusive((ib_trx_t)trx));

  auto err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *dict_index = row_merge_create_index(trx, table, &index_def);

    err = trx->error_state;
  }

  trx->m_op_info = "";

  return err;
}

/**
 * Create the indexes for the table. Each index is created in a separate transaction.
 * The caller is responsible for dropping any indexes that exist if there is a failure.
 *
 * @param ddl_trx in: transaction
 * @param table in: table where index created
 * @param indexes in: index defs. to create
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_indexes(trx_t *ddl_trx, dict_table_t *table, const std::vector<ib_index_def_t *> &indexes) {
  ib_err_t err = DB_ERROR;
  dict_index_t *dict_index = nullptr;
  ib_index_def_t *ib_clust_index_def = nullptr;

  auto n_indexes = indexes.size();

  if (n_indexes > 0) {
    ib_clust_index_def = ib_find_clustered_index(indexes);

    if (ib_clust_index_def != nullptr) {
      ut_a(ib_clust_index_def->clustered);

      err = ib_create_cluster_index(ddl_trx, table, ib_clust_index_def, &dict_index);
    }
  }

  if (ib_clust_index_def == nullptr) {
    err = ib_create_default_cluster_index(ddl_trx, table, &dict_index);
  }

  for (ulint i = 0; err == DB_SUCCESS && i < n_indexes; ++i) {
    auto ib_index_def = indexes.at(i);

    ut_a(!ib_index_def->cols->empty());

    if (!ib_index_def->clustered) {
      /* Since this is part of CREATE TABLE, set the create flag to true. */
      err = ib_build_secondary_index(ddl_trx, table, ib_index_def, true, &dict_index);
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
 * @param table_name in: table to find
 * @param table_id out: table id if found
 *
 * @return DB_SUCCESS if found
 */
static ib_err_t ib_table_get_id_low(const char *table_name, ib_id_t *table_id) {
  ib_err_t err = DB_TABLE_NOT_FOUND;

  *table_id = 0;

  auto table = ib_lookup_table_by_name(table_name);

  if (table != nullptr) {
    *table_id = table->id;

    err = DB_SUCCESS;
  }

  return err;
}

ib_err_t ib_table_create(ib_trx_t ib_trx, const ib_tbl_sch_t ib_tbl_sch, ib_id_t *id) {
  trx_t *ddl_trx = (trx_t *)ib_trx;
  const ib_table_def_t *table_def = (const ib_table_def_t *)ib_tbl_sch;

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
  auto table = dict_mem_table_create(table_def->name, 0, n_cols, ib_table_def_get_flags(table_def));

  auto heap = table->heap;

  /* Create the columns defined by the user. */
  for (ulint i = 0; i < n_cols; i++) {
    auto ib_col = table_def->cols->at(i);

    dict_mem_table_add_col(table, heap, ib_col->name, ib_col_get_mtype(ib_col), ib_col_get_prtype(ib_col), ib_col->len);
  }

  /* Create the table using the prototype in the data dictionary. */
  err = ddl_create_table(table, ddl_trx);

  table = nullptr;

  if (err == DB_SUCCESS) {

    table = ib_lookup_table_by_name(table_def->name);
    ut_a(table != nullptr);

    /* Bump up the reference count, so that another transaction
    doesn't delete it behind our back. */
    dict_table_increment_handle_count(table, true);

    err = ib_create_indexes(ddl_trx, table, *table_def->indexes);
  }

  /* FIXME: If ib_create_indexes() fails, it's unclear what state
  the data dictionary is in. */

  if (err == DB_SUCCESS) {
    *id = table->id;
  }

  if (table != nullptr) {
    dict_table_decrement_handle_count(table, true);
  }

  return err;
}

ib_err_t ib_table_rename(ib_trx_t ib_trx, const char *old_name, const char *new_name) {
  ib_err_t err;
  char *normalized_old_name;
  char *normalized_new_name;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    err = ib_schema_lock_exclusive(ib_trx);

    if (err != DB_SUCCESS) {
      return err;
    }
  }

  normalized_old_name = static_cast<char *>(mem_alloc(strlen(old_name) + 1));
  ib_normalize_table_name(normalized_old_name, old_name);

  normalized_new_name = static_cast<char *>(mem_alloc(strlen(new_name) + 1));
  ib_normalize_table_name(normalized_new_name, new_name);

  err = ddl_rename_table(normalized_old_name, normalized_new_name, trx);

  mem_free(normalized_old_name);
  mem_free(normalized_new_name);

  return err;
}

/**
 * @brief Create a primary index.
 *
 * The index id encodes the table id in the high 4 bytes and the index id in the lower 4 bytes.
 *
 * @param ib_idx_sch in: key definition for index
 * @param index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_primary_index(ib_idx_sch_t ib_idx_sch, ib_id_t *index_id) {
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;
  trx_t *usr_trx = ib_index_def->usr_trx;
  dict_table_t *table = ib_index_def->table;

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
  ut_a(table->n_handles_opened == 1);

  trx_set_dict_operation(usr_trx, TRX_DICT_OP_TABLE);

  ut_a(!ib_index_def->cols->empty());

  /* Set the CLUSTERED flag to true. */
  ib_copy_index_definition(ib_index_def, true);

  dict_table_t *new_table{};
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
    auto old_name = table->name;
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
 * @param ib_idx_sch in: key definition for index
 * @param index_id out: index id
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_secondary_index(ib_idx_sch_t ib_idx_sch, ib_id_t *index_id) {
  dict_index_t *dict_index = nullptr;
  trx_t *ddl_trx = nullptr;
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;
  trx_t *usr_trx = ib_index_def->usr_trx;
  dict_table_t *table = ib_index_def->table;

  IB_CHECK_PANIC();

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->schema == nullptr);
  ut_a(!ib_index_def->clustered);

  auto err = ib_trx_lock_table_with_retry(usr_trx, table, LOCK_S);

  if (err == DB_SUCCESS) {

    /* Since this is part of ALTER TABLE set the create flag to false. */
    err = ib_build_secondary_index(usr_trx, table, ib_index_def, false, &dict_index);

    err = ib_schema_lock_exclusive((ib_trx_t)usr_trx);
    ut_a(err == DB_SUCCESS);

    if (dict_index != nullptr && err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &dict_index, 1);
      dict_index = nullptr;
    } else {
      bool started;

      ddl_trx = trx_allocate_for_client(nullptr);
      started = trx_start(ddl_trx, ULINT_UNDEFINED);
      ut_a(started);
    }
  }

  ut_a(ddl_trx != nullptr || err != DB_SUCCESS);

  /* Rename from the TEMP new index to the actual name. */
  if (dict_index != nullptr && err == DB_SUCCESS) {
    err = row_merge_rename_indexes(usr_trx, table);

    if (err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &dict_index, 1);
      dict_index = nullptr;
    }
  }

  if (dict_index != nullptr && err == DB_SUCCESS) {
    /* We only support 32 bit table and index ids. Because
    we need to pack the table id into the index id. */
    ut_a((table->id & 0xFFFFFFFF00000000) == 0);
    ut_a((dict_index->id & 0xFFFFFFFF00000000) == 0);

    *index_id = table->id;
    *index_id <<= 32;
    *index_id |= dict_index->id;

    auto err = trx_commit(ddl_trx);
    ut_a(err == DB_SUCCESS);
  } else if (ddl_trx != nullptr) {
    trx_general_rollback(ddl_trx, false, nullptr);
  }

  if (ddl_trx != nullptr) {
    ddl_trx->m_op_info = "";
    trx_free_for_client(ddl_trx);
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
  ib_err_t err;
  char *normalized_name;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  err = ddl_drop_table(normalized_name, (trx_t *)ib_trx, false);

  mem_free(normalized_name);

  return err;
}

ib_err_t ib_index_drop(ib_trx_t ib_trx, ib_id_t index_id) {
  ib_err_t err;
  dict_table_t *table;
  dict_index_t *dict_index;
  ulint table_id = (ulint)(index_id >> 32);

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  table = ib_open_table_by_id(table_id, true);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  /* We use only the lower 32 bits of the uint64_t. */
  index_id &= 0xFFFFFFFFULL;

  dict_index = dict_index_get_on_id_low(table, index_id);

  if (dict_index != nullptr) {
    err = ddl_drop_index(table, dict_index, (trx_t *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  dict_table_decrement_handle_count(table, false);

  return err;
}

/**
 * Create an internal cursor instance.
 *
 * @param ib_crsr out: InnoDB cursor
 * @param table in: table instance
 * @param index_id in: index id or 0
 * @param trx in: transaction
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_create_cursor(ib_crsr_t *ib_crsr, dict_table_t *table, ib_id_t index_id, trx_t *trx) {
  ib_err_t err = DB_SUCCESS;
  const uint64_t id = index_id;

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

  cursor->prebuilt = row_prebuilt_create(table);

  auto prebuilt = cursor->prebuilt;

  prebuilt->trx = trx;
  prebuilt->table = table;
  prebuilt->select_lock_type = LOCK_NONE;

  if (index_id > 0) {
    prebuilt->index = dict_index_get_on_id_low(table, id);
  } else {
    prebuilt->index = dict_table_get_first_index(table);
  }

  ut_a(prebuilt->index != nullptr);

  if (prebuilt->trx != nullptr) {
    ++prebuilt->trx->n_client_tables_in_use;

    prebuilt->index_usable = row_merge_is_index_usable(prebuilt->trx, prebuilt->index);

    /* Assign a read view if the transaction does not have it yet */

    auto rv = trx_assign_read_view(prebuilt->trx);
    ut_a(rv != nullptr);
  }

  *ib_crsr = (ib_crsr_t)cursor;

  return err;
}

ib_err_t ib_cursor_open_table_using_id(ib_id_t table_id, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  ib_err_t err;
  dict_table_t *table;

  IB_CHECK_PANIC();

  if (ib_trx == nullptr || !ib_schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(table_id, false);
  } else {
    table = ib_open_table_by_id(table_id, true);
  }

  if (table == nullptr) {

    return DB_TABLE_NOT_FOUND;
  }

  err = ib_create_cursor(ib_crsr, table, 0, (trx_t *)ib_trx);

  return err;
}

ib_err_t ib_cursor_open_index_using_id(ib_id_t index_id, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  dict_table_t *table;
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
  auto err = ib_create_cursor(ib_crsr, table, index_id & 0xFFFFFFFFULL, (trx_t *)ib_trx);

  if (ib_crsr != nullptr) {
    auto cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->index == nullptr) {
      auto crsr_err = ib_cursor_close(*ib_crsr);
      ut_a(crsr_err == DB_SUCCESS);

      *ib_crsr = nullptr;
    }
  }

  return err;
}

ib_err_t ib_cursor_open_index_using_name(ib_crsr_t ib_open_crsr, const char *index_name, ib_crsr_t *ib_crsr) {
  dict_table_t *table;
  dict_index_t *dict_index;
  ib_id_t index_id = 0;
  ib_err_t err = DB_TABLE_NOT_FOUND;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_open_crsr;
  trx_t *trx = cursor->prebuilt->trx;

  IB_CHECK_PANIC();

  if (trx != nullptr && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    dict_mutex_enter();
  }

  /* We want to increment the ref count, so we do a redundant search. */
  table = dict_table_get_using_id(srv_config.m_force_recovery, cursor->prebuilt->table->id, true);
  ut_a(table != nullptr);

  if (trx != nullptr && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    dict_mutex_exit();
  }

  /* The first index is always the cluster index. */
  dict_index = dict_table_get_first_index(table);

  /* Traverse the user defined indexes. */
  while (dict_index != nullptr) {
    if (strcmp(dict_index->name, index_name) == 0) {
      index_id = dict_index->id;
    }
    dict_index = UT_LIST_GET_NEXT(indexes, dict_index);
  }

  *ib_crsr = nullptr;

  if (index_id > 0) {
    err = ib_create_cursor(ib_crsr, table, index_id, cursor->prebuilt->trx);
  }

  if (*ib_crsr != nullptr) {
    const ib_cursor_t *cursor;

    cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->index == nullptr) {
      err = ib_cursor_close(*ib_crsr);
      ut_a(err == DB_SUCCESS);
      *ib_crsr = nullptr;
    }
  } else {
    dict_table_decrement_handle_count(table, true);
  }

  return err;
}

ib_err_t ib_cursor_open_table(const char *name, ib_trx_t ib_trx, ib_crsr_t *ib_crsr) {
  ib_err_t err;
  dict_table_t *table;

  IB_CHECK_PANIC();

  auto normalized_name = static_cast<char *>(mem_alloc(strlen(name) + 1));
  ib_normalize_table_name(normalized_name, name);

  if (ib_trx != nullptr) {
    if (!ib_schema_lock_is_exclusive(ib_trx)) {
      table = ib_open_table_by_name(normalized_name);
    } else {
      table = ib_lookup_table_by_name(normalized_name);

      if (table != nullptr) {
        dict_table_increment_handle_count(table, true);
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
  if (table != nullptr && dict_table_get_first_index(table) == nullptr) {
    dict_table_decrement_handle_count(table, false);
    table = nullptr;
  }

  if (table != nullptr) {
    err = ib_create_cursor(ib_crsr, table, 0, (trx_t *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  return err;
}

/**
 * Free a context struct for a table handle.
 *
 * @param q_proc in, own: qproc struct
 */
static void ib_qry_proc_free(ib_qry_proc_t *q_proc) {
  que_graph_free_recursive(q_proc->grph.ins);
  que_graph_free_recursive(q_proc->grph.upd);
  que_graph_free_recursive(q_proc->grph.sel);

  memset(q_proc, 0x0, sizeof(*q_proc));
}

ib_err_t ib_cursor_reset(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  if (prebuilt->trx != nullptr && prebuilt->trx->n_client_tables_in_use > 0) {

    --prebuilt->trx->n_client_tables_in_use;
  }

  /* The fields in this data structure are allocated from
  the query heap and so need to be reset too. */
  ib_qry_proc_free(&cursor->q_proc);

  mem_heap_empty(cursor->query_heap);

  row_prebuilt_reset(prebuilt);

  return DB_SUCCESS;
}

ib_err_t ib_cursor_close(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  trx_t *trx = prebuilt->trx;

  IB_CHECK_PANIC();

  ib_qry_proc_free(&cursor->q_proc);

  /* The transaction could have been detached from the cursor. */
  if (trx != nullptr && trx->n_client_tables_in_use > 0) {
    --trx->n_client_tables_in_use;
  }

  if (trx != nullptr && ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    row_prebuilt_free(cursor->prebuilt, true);
  } else {
    row_prebuilt_free(cursor->prebuilt, false);
  }

  mem_heap_free(cursor->query_heap);
  mem_heap_free(cursor->heap);

  return DB_SUCCESS;
}

/**
 * Run the insert query and do error handling.
 *
 * @param thr in: insert query graph
 * @param node in: insert node for the query
 * @param savept in: savepoint to rollback to in case of an error
 *
 * @return DB_SUCCESS or error code
 */
static ib_err_t ib_insert_row_with_lock_retry(que_thr_t *thr, ins_node_t *node, trx_savept_t *savept) {
  ib_err_t err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    row_ins_step(thr);

    err = trx->error_state;

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
 * @param table in: table where to insert
 * @param ins_graph in: query graph
 * @param node in: insert node
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_execute_insert_query_graph(dict_table_t *table, que_fork_t *ins_graph, ins_node_t *node) {
  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  auto trx = ins_graph->trx;
  auto savept = trx_savept_take(trx);
  auto thr = que_fork_get_first_thr(ins_graph);

  que_thr_move_to_run_state(thr);

  auto err = ib_insert_row_with_lock_retry(thr, node, &savept);

  if (err == DB_SUCCESS) {
    que_thr_stop_for_client_no_error(thr, trx);

    table->stat_n_rows++;

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
 * @param cursor in: Cursor instance
 */
static void ib_insert_query_graph_create(ib_cursor_t *cursor) {
  ib_qry_proc_t *q_proc = &cursor->q_proc;
  ib_qry_node_t *node = &q_proc->node;
  trx_t *trx = cursor->prebuilt->trx;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->ins == nullptr) {
    dtuple_t *row;
    ib_qry_grph_t *grph = &q_proc->grph;
    mem_heap_t *heap = cursor->query_heap;
    dict_table_t *table = cursor->prebuilt->table;

    node->ins = row_ins_node_create(INS_DIRECT, table, heap);

    node->ins->select = nullptr;
    node->ins->values_list = nullptr;

    row = dtuple_create(heap, dict_table_get_n_cols(table));
    dict_table_copy_types(row, table);

    row_ins_node_set_new_row(node->ins, row);

    grph->ins = static_cast<que_fork_t *>(que_node_get_parent(pars_complete_graph_for_exec(node->ins, trx, heap)));

    grph->ins->state = QUE_FORK_ACTIVE;
  }
}

ib_err_t ib_cursor_insert_row(ib_crsr_t ib_crsr, const ib_tpl_t ib_tpl) {
  ulint i;
  ib_qry_node_t *node;
  ib_qry_proc_t *q_proc;
  ulint n_fields;
  dtuple_t *dst_dtuple;
  ib_err_t err = DB_SUCCESS;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  const ib_tuple_t *src_tuple = (const ib_tuple_t *)ib_tpl;

  IB_CHECK_PANIC();

  ib_insert_query_graph_create(cursor);

  ut_ad(src_tuple->type == TPL_ROW);

  q_proc = &cursor->q_proc;
  node = &q_proc->node;

  node->ins->state = INS_NODE_ALLOC_ROW_ID;
  dst_dtuple = node->ins->row;

  n_fields = dtuple_get_n_fields(src_tuple->ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_dtuple));

  /* Do a shallow copy of the data fields and check for nullptr
  constraints on columns. */
  for (i = 0; i < n_fields; i++) {
    ulint mtype;
    dfield_t *src_field;
    dfield_t *dst_field;

    src_field = dtuple_get_nth_field(src_tuple->ptr, i);

    mtype = dtype_get_mtype(dfield_get_type(src_field));

    /* Don't touch the system columns. */
    if (mtype != DATA_SYS) {
      ulint prtype;

      prtype = dtype_get_prtype(dfield_get_type(src_field));

      if ((prtype & DATA_NOT_NULL) && dfield_is_null(src_field)) {

        err = DB_DATA_MISMATCH;
        break;
      }

      dst_field = dtuple_get_nth_field(dst_dtuple, i);
      ut_ad(mtype == dtype_get_mtype(dfield_get_type(dst_field)));

      /* Do a shallow copy. */
      dfield_set_data(dst_field, src_field->data, src_field->len);

      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);
      UNIV_MEM_ASSERT_RW(dst_field->data, dst_field->len);
    }
  }

  if (err == DB_SUCCESS) {
    err = ib_execute_insert_query_graph(src_tuple->index->table, q_proc->grph.ins, node->ins);
  }

  return err;
}

/**
 * Gets pointer to a prebuilt update vector used in updates.
 *
 * @param cursor in: current cursor
 *
 * @return update vector
 */
static upd_t *ib_update_vector_create(ib_cursor_t *cursor) {
  trx_t *trx = cursor->prebuilt->trx;
  mem_heap_t *heap = cursor->query_heap;
  dict_table_t *table = cursor->prebuilt->table;
  ib_qry_proc_t *q_proc = &cursor->q_proc;
  ib_qry_grph_t *grph = &q_proc->grph;
  ib_qry_node_t *node = &q_proc->node;

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (node->upd == nullptr) {
    node->upd = row_create_update_node(table, heap);
  }

  grph->upd = static_cast<que_fork_t *>(que_node_get_parent(pars_complete_graph_for_exec(node->upd, trx, heap)));

  grph->upd->state = QUE_FORK_ACTIVE;

  return node->upd->update;
}

/**
 * Note that a column has changed.
 *
 * @param cursor in: current cursor
 * @param upd_field in/out: update field
 * @param col_no in: column number
 * @param dfield in: updated dfield
 */
static void ib_update_col(ib_cursor_t *cursor, upd_field_t *upd_field, ulint col_no, dfield_t *dfield) {
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *dict_index = dict_table_get_first_index(table);

  auto data_len = dfield_get_len(dfield);

  if (data_len == UNIV_SQL_NULL) {
    dfield_set_null(&upd_field->new_val);
  } else {
    dfield_copy_data(&upd_field->new_val, dfield);
  }

  upd_field->exp = nullptr;

  upd_field->orig_len = 0;

  upd_field->field_no = dict_col_get_clust_pos(&table->cols[col_no], dict_index);
}

/**
 * Checks which fields have changed in a row and stores the new data
 * to an update vector.
 *
 * @param cursor in: current cursor
 * @param upd in/out: update vector
 * @param old_tuple in: Old tuple in table
 * @param new_tuple in: New tuple to update
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_calc_diff(ib_cursor_t *cursor, upd_t *upd, const ib_tuple_t *old_tuple, const ib_tuple_t *new_tuple) {
  ulint n_changed = 0;
  ib_err_t err = DB_SUCCESS;
  ulint n_fields = dtuple_get_n_fields(new_tuple->ptr);

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);
  ut_a(old_tuple->index->table == new_tuple->index->table);

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

      auto upd_field = &upd->fields[n_changed];

      ib_update_col(cursor, upd_field, i, new_dfield);

      ++n_changed;
    }
  }

  if (err == DB_SUCCESS) {
    upd->info_bits = 0;
    upd->n_fields = n_changed;
  }

  return err;
}

/**
 * Run the update query and do error handling.
 *
 * @param thr in: Update query graph
 * @param node in: Update node for the query
 * @param savept in: savepoint to rollback to in case of an error
 *
 * @return DB_SUCCESS or error code
 */
static ib_err_t ib_update_row_with_lock_retry(que_thr_t *thr, upd_node_t *node, trx_savept_t *savept) {
  ib_err_t err;
  bool lock_wait;

  auto trx = thr_get_trx(thr);

  do {
    thr->run_node = node;
    thr->prev_node = node;

    row_upd_step(thr);

    err = trx->error_state;

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
 * @param cursor in: Cursor instance
 * @param pcur in: Btree persistent cursor
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_execute_update_query_graph(ib_cursor_t *cursor, Btree_pcursor *pcur) {
  trx_t *trx = cursor->prebuilt->trx;
  dict_table_t *table = cursor->prebuilt->table;
  ib_qry_proc_t *q_proc = &cursor->q_proc;

  /* The transaction must be running. */
  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto node = q_proc->node.upd;

  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  ut_a(dict_index_is_clust(pcur->get_index()));
  node->pcur->copy_stored_position(pcur);

  ut_a(node->pcur->get_rel_pos() == Btree_cursor_pos::ON);

  auto savept = trx_savept_take(trx);

  auto thr = que_fork_get_first_thr(q_proc->grph.upd);

  node->state = UPD_NODE_UPDATE_CLUSTERED;

  que_thr_move_to_run_state(thr);

  auto err = ib_update_row_with_lock_retry(thr, node, &savept);

  if (err == DB_SUCCESS) {

    que_thr_stop_for_client_no_error(thr, trx);

    if (node->is_delete) {

      if (table->stat_n_rows > 0) {
        table->stat_n_rows--;
      }

      srv_n_rows_deleted++;
    } else {
      srv_n_rows_updated++;
    }

    ib_update_statistics_if_needed(table);

  } else if (err == DB_RECORD_NOT_FOUND) {
    trx->error_state = DB_SUCCESS;
  }

  ib_wake_master_thread();

  trx->m_op_info = "";

  return err;
}

ib_err_t ib_cursor_update_row(ib_crsr_t ib_crsr, const ib_tpl_t ib_old_tpl, const ib_tpl_t ib_new_tpl) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  const ib_tuple_t *old_tuple = (const ib_tuple_t *)ib_old_tpl;
  const ib_tuple_t *new_tuple = (const ib_tuple_t *)ib_new_tpl;

  IB_CHECK_PANIC();

  Btree_pcursor *pcur;

  if (dict_index_is_clust(prebuilt->index)) {
    pcur = cursor->prebuilt->pcur;
  } else if (prebuilt->need_to_access_clustered && cursor->prebuilt->clust_pcur != nullptr) {
    pcur = cursor->prebuilt->clust_pcur;
  } else {
    return DB_ERROR;
  }

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);

  auto upd = ib_update_vector_create(cursor);
  auto err = ib_calc_diff(cursor, upd, old_tuple, new_tuple);

  if (err == DB_SUCCESS) {
    /* Note that this is not a delete. */
    cursor->q_proc.node.upd->is_delete = false;

    err = ib_execute_update_query_graph(cursor, pcur);
  }

  return err;
}

/**
 * Build the update query graph to delete a row from an index.
 *
 * @param cursor in: current cursor
 * @param pcur in: Btree persistent cursor
 * @param rec in: record to delete
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_delete_row(ib_cursor_t *cursor, Btree_pcursor *pcur, const rec_t *rec) {
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *dict_index = dict_table_get_first_index(table);

  IB_CHECK_PANIC();

  auto n_cols = dict_index_get_n_ordering_defined_by_user(dict_index);
  auto ib_tpl = ib_key_tuple_new(dict_index, n_cols);

  if (ib_tpl == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  auto tuple = (ib_tuple_t *)ib_tpl;

  auto upd = ib_update_vector_create(cursor);

  ib_read_tuple(rec, tuple);

  upd->n_fields = ib_tuple_get_n_cols(ib_tpl);

  for (ulint i = 0; i < upd->n_fields; ++i) {
    auto upd_field = &upd->fields[i];
    auto dfield = dtuple_get_nth_field(tuple->ptr, i);

    dfield_copy_data(&upd_field->new_val, dfield);

    upd_field->exp = nullptr;

    upd_field->orig_len = 0;

    upd->info_bits = 0;

    upd_field->field_no = dict_col_get_clust_pos(&table->cols[i], dict_index);
  }

  /* Note that this is a delete. */
  cursor->q_proc.node.upd->is_delete = true;

  auto err = ib_execute_update_query_graph(cursor, pcur);

  ib_tuple_delete(ib_tpl);

  return err;
}

ib_err_t ib_cursor_delete_row(ib_crsr_t ib_crsr) {
  ib_err_t err;
  Btree_pcursor *pcur;
  dict_index_t *dict_index;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  dict_index = dict_table_get_first_index(prebuilt->index->table);

  /* Check whether this is a secondary index cursor */
  if (dict_index != prebuilt->index) {
    if (prebuilt->need_to_access_clustered) {
      pcur = prebuilt->clust_pcur;
    } else {
      return DB_ERROR;
    }
  } else {
    pcur = prebuilt->pcur;
  }

  if (ib_btr_cursor_is_positioned(pcur)) {
    const rec_t *rec;

    if (!row_sel_row_cache_is_empty(prebuilt)) {
      rec = row_sel_row_cache_get(prebuilt);
      ut_a(rec != nullptr);
    } else {
      mtr_t mtr;

      mtr.start();

      if (pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Source_location{})) {

        rec = pcur->get_rec();
      } else {
        rec = nullptr;
      }

      mtr.commit();
    }

    if (rec && !rec_get_deleted_flag(rec)) {
      err = ib_delete_row(cursor, pcur, rec);
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
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  ut_a(cursor->prebuilt->trx->m_conc_state != TRX_NOT_STARTED);

  /* When searching with IB_EXACT_MATCH set, row_search_for_client()
  will not position the persistent cursor but will copy the record
  found into the row cache. It should be the only entry. */
  if (!ib_cursor_is_positioned(ib_crsr) && row_sel_row_cache_is_empty(cursor->prebuilt)) {
    err = DB_RECORD_NOT_FOUND;
  } else if (!row_sel_row_cache_is_empty(cursor->prebuilt)) {
    auto rec = row_sel_row_cache_get(cursor->prebuilt);
    ut_a(rec != nullptr);

    if (!rec_get_deleted_flag(rec)) {
      ib_read_tuple(rec, tuple);
      err = DB_SUCCESS;
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    mtr_t mtr;
    Btree_pcursor *pcur;
    row_prebuilt_t *prebuilt = cursor->prebuilt;

    if (prebuilt->need_to_access_clustered && tuple->type == TPL_ROW) {
      pcur = prebuilt->clust_pcur;
    } else {
      pcur = prebuilt->pcur;
    }

    if (pcur == nullptr) {
      return DB_ERROR;
    }

    mtr.start();

    if (pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Source_location{})) {
      auto rec = pcur->get_rec();

      if (!rec_get_deleted_flag(rec)) {
        ib_read_tuple(rec, tuple);
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
  ib_err_t err;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  row_sel_row_cache_next(prebuilt);

  err = row_search_for_client(srv_config.m_force_recovery, IB_CUR_L, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_PREV);

  return err;
}

ib_err_t ib_cursor_next(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  row_sel_row_cache_next(prebuilt);

  return row_search_for_client(srv_config.m_force_recovery, IB_CUR_G, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_NEXT);
}

/**
 * Move cursor to the first record in the table.
 *
 * @param cursor in: InnoDB cursor instance
 * @param mode in: Search mode
 *
 * @return DB_SUCCESS or err code
 */
static ib_err_t ib_cursor_position(ib_cursor_t *cursor, ib_srch_mode_t mode) {
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  /* We want to position at one of the ends, row_search_for_client()
  uses the search_tuple fields to work out what to do. */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  return row_search_for_client(srv_config.m_force_recovery, mode, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_MOVETO);
}

ib_err_t ib_cursor_first(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  return ib_cursor_position(cursor, IB_CUR_G);
}

ib_err_t ib_cursor_last(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  return ib_cursor_position(cursor, IB_CUR_L);
}

ib_err_t ib_cursor_moveto(ib_crsr_t ib_crsr, ib_tpl_t ib_tpl, ib_srch_mode_t ib_srch_mode, int *result) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  dtuple_t *search_tuple = prebuilt->search_tuple;

  IB_CHECK_PANIC();

  ut_a(tuple->type == TPL_KEY);

  auto n_fields = dict_index_get_n_ordering_defined_by_user(prebuilt->index);

  dtuple_set_n_fields(search_tuple, n_fields);
  dtuple_set_n_fields_cmp(search_tuple, n_fields);

  /* Do a shallow copy */
  for (ulint i = 0; i < n_fields; ++i) {
    dfield_copy(dtuple_get_nth_field(search_tuple, i), dtuple_get_nth_field(tuple->ptr, i));
  }

  ut_a(prebuilt->select_lock_type <= LOCK_NUM);

  auto err = row_search_for_client(srv_config.m_force_recovery, ib_srch_mode, prebuilt, (ib_match_t)cursor->match_mode, ROW_SEL_MOVETO);

  *result = prebuilt->result;

  return err;
}

void ib_cursor_attach_trx(ib_crsr_t ib_crsr, ib_trx_t ib_trx) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  ut_a(ib_trx != nullptr);
  ut_a(prebuilt->trx == nullptr);

  row_prebuilt_reset(prebuilt);
  row_prebuilt_update_trx(prebuilt, (trx_t *)ib_trx);

  /* Assign a read view if the transaction does not have it yet */
  auto rv = trx_assign_read_view(prebuilt->trx);
  ut_a(rv == nullptr);

  ut_a(prebuilt->trx->m_conc_state != TRX_NOT_STARTED);

  ++prebuilt->trx->n_client_tables_in_use;
}

void ib_set_client_compare(ib_client_cmp_t client_cmp_func) {
  ib_client_compare = client_cmp_func;
}

void ib_cursor_set_match_mode(ib_crsr_t ib_crsr, ib_match_mode_t match_mode) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  cursor->match_mode = match_mode;
}

/** Get the dfield instance for the column in the tuple.
@return	dfield instance in tuple */
static dfield_t *ib_col_get_dfield(
  ib_tuple_t *tuple, /*!< in: tuple instance */
  ulint col_no
) /*!< in: col no. in tuple */
{
  dfield_t *dfield;

  dfield = dtuple_get_nth_field(tuple->ptr, col_no);

  return dfield;
}

/** Predicate to check whether a column type contains variable length data.
@return	true or false */
static bool ib_col_is_capped(const dtype_t *dtype) /* in: column type */
{
  return dtype_get_len(dtype) > 0 && (dtype_get_mtype(dtype) == DATA_VARCHAR || dtype_get_mtype(dtype) == DATA_CHAR ||
                                      dtype_get_mtype(dtype) == DATA_CLIENT || dtype_get_mtype(dtype) == DATA_VARCLIENT ||
                                      dtype_get_mtype(dtype) == DATA_FIXBINARY || dtype_get_mtype(dtype) == DATA_BINARY);
}

ib_err_t ib_col_set_value(ib_tpl_t ib_tpl, ulint col_no, const void *s, ulint len) {
  const dtype_t *dtype;
  dfield_t *dfield;
  byte *dst = nullptr;
  auto src = static_cast<const byte *>(s);
  auto tuple = reinterpret_cast<ib_tuple_t *>(ib_tpl);

  IB_CHECK_PANIC();

  ut_d(mem_heap_verify(tuple->heap));

  dfield = ib_col_get_dfield(tuple, col_no);

  /* User wants to set the column to nullptr. */
  if (len == IB_SQL_NULL) {
    dfield_set_null(dfield);
    return DB_SUCCESS;
  }

  dtype = dfield_get_type(dfield);

  /* Not allowed to update system columns. */
  if (dtype_get_mtype(dtype) == DATA_SYS) {
    return DB_DATA_MISMATCH;
  }

  dst = static_cast<byte *>(dfield_get_data(dfield));

  /* Since TEXT/CLOB also map to DATA_VARCHAR we need to make an
  exception. Perhaps we need to set the precise type and check
  for that. */
  if (ib_col_is_capped(dtype)) {

    len = ut_min(len, dtype_get_len(dtype));

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
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  dfield = ib_col_get_dfield(tuple, i);

  data_len = dfield_get_len(dfield);

  return data_len == UNIV_SQL_NULL ? IB_SQL_NULL : data_len;
}

/**
 * Copy a column value from the tuple.
 *
 * @param ib_tpl in: tuple instance
 * @param i in: column index in tuple
 * @param dst out: copied data value
 * @param len in: max data value len to copy
 *
 * @return bytes copied or IB_SQL_NULL
 */
static ulint ib_col_copy_value_low(ib_tpl_t ib_tpl, ulint i, void *dst, ulint len) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  auto dfield = ib_col_get_dfield(tuple, i);
  auto data = static_cast<const byte *>(dfield_get_data(dfield));
  auto data_len = dfield_get_len(dfield);

  if (data_len != UNIV_SQL_NULL) {

    const dtype_t *dtype = dfield_get_type(dfield);

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
        data_len = ut_min(data_len, len);
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
 * @param prtype in: column definition
 *
 * @return precise type in api format
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
 * @param ib_tpl in: tuple instance
 * @param i in: column index in tuple
 * @param ib_col_meta out: column meta data
 *
 * @return len of column data
 */
static ulint ib_col_get_meta_low(ib_tpl_t ib_tpl, ulint i, ib_col_meta_t *ib_col_meta) {
  uint16_t prtype;
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  dfield = ib_col_get_dfield(tuple, i);

  data_len = dfield_get_len(dfield);

  /* We assume 1-1 mapping between the ENUM and internal type codes. */
  ib_col_meta->type = static_cast<ib_col_type_t>(dtype_get_mtype(dfield_get_type(dfield)));

  ib_col_meta->type_len = dtype_get_len(dfield_get_type(dfield));

  prtype = (uint16_t)dtype_get_prtype(dfield_get_type(dfield));

  ib_col_meta->attr = ib_col_get_attr(prtype);
  ib_col_meta->client_type = prtype & DATA_CLIENT_TYPE_MASK;

  return data_len;
}

/**
 * Read a signed int 8 bit column from an InnoDB tuple.
 *
 * @param ib_tpl in: InnoDB tuple
 * @param i in: column number
 * @param usign in: true if unsigned
 * @param size in: size of integer
 *
 * @return DB_SUCCESS or error
 */
static ib_err_t ib_tuple_check_int(ib_tpl_t ib_tpl, ulint i, bool usign, ulint size) {
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
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u8(ib_tpl_t ib_tpl, ulint i, uint8_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i16(ib_tpl_t ib_tpl, ulint i, int16_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u16(ib_tpl_t ib_tpl, ulint i, uint16_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i32(ib_tpl_t ib_tpl, ulint i, int32_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u32(ib_tpl_t ib_tpl, ulint i, uint32_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_i64(ib_tpl_t ib_tpl, ulint i, int64_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, false, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

ib_err_t ib_tuple_read_u64(ib_tpl_t ib_tpl, ulint i, uint64_t *ival) {
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, true, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return err;
}

const void *ib_col_get_value(ib_tpl_t ib_tpl, ulint i) {
  const void *data;
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  dfield = ib_col_get_dfield(tuple, i);

  data = dfield_get_data(dfield);
  data_len = dfield_get_len(dfield);

  return data_len != UNIV_SQL_NULL ? data : nullptr;
}

ulint ib_col_get_meta(ib_tpl_t ib_tpl, ulint i, ib_col_meta_t *ib_col_meta) {
  return ib_col_get_meta_low(ib_tpl, i, ib_col_meta);
}

ib_tpl_t ib_tuple_clear(ib_tpl_t ib_tpl) {
  const dict_index_t *dict_index;
  ulint n_cols;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_tuple_type_t type = tuple->type;
  mem_heap_t *heap = tuple->heap;

  dict_index = tuple->index;
  n_cols = dtuple_get_n_fields(tuple->ptr);

  mem_heap_empty(heap);

  if (type == TPL_ROW) {
    return ib_row_tuple_new_low(dict_index, n_cols, heap);
  } else {
    return ib_key_tuple_new_low(dict_index, n_cols, heap);
  }
}

ib_err_t ib_tuple_get_cluster_key(ib_crsr_t ib_crsr, ib_tpl_t *ib_dst_tpl, const ib_tpl_t ib_src_tpl) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  ib_tuple_t *src_tuple = (ib_tuple_t *)ib_src_tpl;

  IB_CHECK_PANIC();

  auto clust_index = dict_table_get_first_index(cursor->prebuilt->table);

  /* We need to ensure that the src tuple belongs to the same table
  as the open cursor and that it's not a tuple for a cluster index. */
  if (src_tuple->type != TPL_KEY) {
    return DB_ERROR;
  } else if (src_tuple->index->table != cursor->prebuilt->table) {
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

  auto n_fields = dict_index_get_n_unique(dst_tuple->index);

  /* Do a deep copy of the data fields. */
  for (ulint i = 0; i < n_fields; i++) {
    auto pos = dict_index_get_nth_field_pos(src_tuple->index, dst_tuple->index, i);

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
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index = cursor->prebuilt->index;

  n_cols = dict_index_get_n_unique_in_tree(dict_index);
  return ib_key_tuple_new(dict_index, n_cols);
}

ib_tpl_t ib_sec_read_tuple_create(ib_crsr_t ib_crsr) {
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index = cursor->prebuilt->index;

  n_cols = dict_index_get_n_fields(dict_index);
  return ib_row_tuple_new(dict_index, n_cols);
}

ib_tpl_t ib_clust_search_tuple_create(ib_crsr_t ib_crsr) {
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index;

  dict_index = dict_table_get_first_index(cursor->prebuilt->table);

  n_cols = dict_index_get_n_ordering_defined_by_user(dict_index);
  return ib_key_tuple_new(dict_index, n_cols);
}

ib_tpl_t ib_clust_read_tuple_create(ib_crsr_t ib_crsr) {
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index;

  dict_index = dict_table_get_first_index(cursor->prebuilt->table);

  n_cols = dict_table_get_n_cols(cursor->prebuilt->table);
  return ib_row_tuple_new(dict_index, n_cols);
}

ulint ib_tuple_get_n_user_cols(const ib_tpl_t ib_tpl) {
  const ib_tuple_t *tuple = (const ib_tuple_t *)ib_tpl;

  if (tuple->type == TPL_ROW) {
    return dict_table_get_n_user_cols(tuple->index->table);
  } else {
    return dict_index_get_n_ordering_defined_by_user(tuple->index);
  }
}

ulint ib_tuple_get_n_cols(const ib_tpl_t ib_tpl) {
  const ib_tuple_t *tuple = (const ib_tuple_t *)ib_tpl;

  return dtuple_get_n_fields(tuple->ptr);
}

void ib_tuple_delete(ib_tpl_t ib_tpl) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  mem_heap_free(tuple->heap);
}

ib_err_t ib_cursor_truncate(ib_crsr_t *ib_crsr, ib_id_t *table_id) {
  ib_err_t err;
  ib_cursor_t *cursor = *(ib_cursor_t **)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  ut_a(ib_schema_lock_is_exclusive((ib_trx_t)prebuilt->trx));

  *table_id = 0;

  err = ib_cursor_lock(*ib_crsr, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    trx_t *trx;
    dict_table_t *table = prebuilt->table;

    /* We are going to free the cursor and the prebuilt. Store
    the transaction handle locally. */
    trx = prebuilt->trx;
    err = ib_cursor_close(*ib_crsr);
    ut_a(err == DB_SUCCESS);

    *ib_crsr = nullptr;

    /* This function currently commits the transaction
    on success. */
    err = ddl_truncate_table(table, trx);

    if (err == DB_SUCCESS) {
      *table_id = table->id;
    }
  }

  return err;
}

ib_err_t ib_table_truncate(const char *table_name, ib_id_t *table_id) {
  ib_err_t err;
  dict_table_t *table;
  ib_err_t trunc_err;
  ib_trx_t ib_trx = nullptr;
  ib_crsr_t ib_crsr = nullptr;

  IB_CHECK_PANIC();

  ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  dict_mutex_enter();

  table = dict_table_get_low(table_name);

  if (table != nullptr && dict_table_get_first_index(table)) {
    dict_table_increment_handle_count(table, true);
    err = ib_create_cursor(&ib_crsr, table, 0, (trx_t *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  dict_mutex_exit();

  if (err == DB_SUCCESS) {
    err = ib_schema_lock_exclusive(ib_trx);
  }

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

  dict_mutex_enter();

  auto err = ib_table_get_id_low(table_name, table_id);

  dict_mutex_exit();

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
    auto dict_index = dict_table_get_index_on_name(table, index_name);

    if (dict_index != nullptr) {
      /* We only support 32 bit table and index ids. Because
      we need to pack the table id into the index id. */
      ut_a(table->id == 0);
      ut_a(dict_index->id == 0);

      *index_id = table->id;
      *index_id <<= 32;
      *index_id |= dict_index->id;

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

  auto ptr = (char *)mem_alloc(len + 2);

  memset(ptr, 0x0, len + 2);
  strcpy(ptr, dbname);

  ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  /* Drop all the tables in the database first. */
  /* ddl_drop_database() expects a string that ends in '/'. */
  if (ptr[len - 1] != '/') {
    ptr[len] = '/';
  }

  auto err = ddl_drop_database(ptr, (trx_t *)ib_trx);

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
  const row_prebuilt_t *prebuilt = cursor->prebuilt;

  return ib_btr_cursor_is_positioned(prebuilt->pcur);
}

ib_err_t ib_schema_lock_shared(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_S_LATCH) {

    dict_freeze_data_dictionary((trx_t *)ib_trx);
  }

  return err;
}

ib_err_t ib_schema_lock_exclusive(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH) {

    dict_lock_data_dictionary((trx_t *)ib_trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

bool ib_schema_lock_is_exclusive(const ib_trx_t ib_trx) {
  const trx_t *trx = (const trx_t *)ib_trx;

  return trx->m_dict_operation_lock_mode == RW_X_LATCH;
}

bool ib_schema_lock_is_shared(const ib_trx_t ib_trx) {
  const trx_t *trx = (const trx_t *)ib_trx;

  return trx->m_dict_operation_lock_mode == RW_S_LATCH;
}

ib_err_t ib_schema_unlock(ib_trx_t ib_trx) {
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    dict_unlock_data_dictionary((trx_t *)ib_trx);
  } else if (trx->m_dict_operation_lock_mode == RW_S_LATCH) {
    dict_unfreeze_data_dictionary((trx_t *)ib_trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return err;
}

ib_err_t ib_cursor_lock(ib_crsr_t ib_crsr, ib_lck_mode_t ib_lck_mode) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  trx_t *trx = prebuilt->trx;
  dict_table_t *table = prebuilt->table;

  IB_CHECK_PANIC();

  return (ib_trx_lock_table_with_retry(trx, table, (enum Lock_mode)ib_lck_mode));
}

ib_err_t ib_table_lock(ib_trx_t ib_trx, ib_id_t table_id, ib_lck_mode_t ib_lck_mode) {
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto table = ib_open_table_by_id(table_id, false);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  auto heap = mem_heap_create(128);

  ib_qry_proc_t q_proc;

  q_proc.node.sel = sel_node_create(heap);

  auto thr = pars_complete_graph_for_exec(q_proc.node.sel, trx, heap);

  q_proc.grph.sel = static_cast<que_fork_t *>(que_node_get_parent(thr));
  q_proc.grph.sel->state = QUE_FORK_ACTIVE;

  trx->m_op_info = "setting table lock";

  ut_a(ib_lck_mode == IB_LOCK_IS || ib_lck_mode == IB_LOCK_IX);

  auto err = srv_lock_sys->lock_table(0, table, (enum Lock_mode)ib_lck_mode, thr);

  trx->error_state = err;

  dict_table_decrement_handle_count(table, false);

  mem_heap_free(heap);

  return err;
}

ib_err_t ib_cursor_unlock(ib_crsr_t ib_crsr) {
  ib_err_t err;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  if (prebuilt->trx->client_n_tables_locked > 0) {
    --prebuilt->trx->client_n_tables_locked;
    err = DB_SUCCESS;
  } else {
    err = DB_ERROR;
  }

  return err;
}

ib_err_t ib_cursor_set_lock_mode(ib_crsr_t ib_crsr, ib_lck_mode_t ib_lck_mode) {
  ib_err_t err = DB_SUCCESS;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  if (ib_lck_mode == IB_LOCK_X) {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IX);
  } else {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IS);
  }

  if (err == DB_SUCCESS) {
    prebuilt->select_lock_type = (enum Lock_mode)ib_lck_mode;
    ut_a(prebuilt->trx->m_conc_state != TRX_NOT_STARTED);
  }

  return err;
}

void ib_cursor_set_cluster_access(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  prebuilt->need_to_access_clustered = true;
}

void ib_cursor_set_simple_select(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  prebuilt->simple_select = true;
}

void ib_savepoint_take(ib_trx_t ib_trx, const void *name, ulint name_len) {
  trx_t *trx = (trx_t *)ib_trx;

  ut_a(trx);
  ut_a(name != nullptr);
  ut_a(name_len > 0);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  auto savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  /* Check if there is a savepoint with the same name already. */
  while (savep != nullptr) {

    if (name_len == savep->name_len && 0 == memcmp(savep->name, name, name_len)) {
      break;
    }

    savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
  }

  if (savep) {
    /* There is a savepoint with the same name: free that */
    UT_LIST_REMOVE(trx->trx_savepoints, savep);

    mem_free(savep);
  }

  /* Create a new savepoint and add it as the last in the list */
  savep = static_cast<trx_named_savept_t *>(mem_alloc(sizeof(trx_named_savept_t) + name_len));

  savep->name = savep + 1;
  savep->savept = trx_savept_take(trx);

  savep->name_len = name_len;
  memcpy(savep->name, name, name_len);

  UT_LIST_ADD_LAST(trx->trx_savepoints, savep);
}

ib_err_t ib_savepoint_release(ib_trx_t ib_trx, const void *name, ulint name_len) {
  trx_named_savept_t *savep;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  /* Search for the savepoint by name and free if found. */
  while (savep != nullptr) {

    if (name_len == savep->name_len && 0 == memcmp(savep->name, name, name_len)) {

      UT_LIST_REMOVE(trx->trx_savepoints, savep);
      mem_free(savep);

      return DB_SUCCESS;
    }

    savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
  }

  return DB_NO_SAVEPOINT;
}

ib_err_t ib_savepoint_rollback(ib_trx_t ib_trx, const void *name, ulint name_len) {
  trx_t *trx = (trx_t *)ib_trx;

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

  auto savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  if (name != nullptr) {
    while (savep != nullptr) {
      if (savep->name_len == name_len && 0 == memcmp(savep->name, name, name_len)) {
        /* Found */
        break;
      }
      savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
    }
  }

  if (savep == nullptr) {

    return DB_NO_SAVEPOINT;
  }

  /* We can now free all savepoints strictly later than this one */
  trx_roll_savepoints_free(trx, savep);

  trx->m_op_info = "rollback to a savepoint";

  auto err = trx_general_rollback(trx, true, &savep->savept);

  /* Store the current undo_no of the transaction so that we know where
  to roll back if we have to roll back the next SQL statement: */
  trx_mark_sql_stat_end(trx);

  trx->m_op_info = "";

  return err;
}

/**
 * @brief Convert from internal format to the the table definition table attributes
 *
 * @param table - in: table definition
 * @param tbl_fmt - out: table format
 * @param page_size - out: page size
 */
static void ib_table_get_format(const dict_table_t *table, ib_tbl_fmt_t *tbl_fmt, ulint *page_size) {
  *page_size = 0;

  if (table->flags == DICT_TF_FORMAT_V1) {
      *tbl_fmt = IB_TBL_V1;
  } else {
    *tbl_fmt = IB_TBL_UNKNOWN;
  }
}

/**
 * @brief Call the visitor for each column in a table.
 *
 * @param table - in: table to visit
 * @param table_col - in: table column visitor
 * @param arg - in: argument to visitor
 *
 * @return return value from index_col
 */
static int ib_table_schema_visit_table_columns(const dict_table_t *table, ib_schema_visitor_t::table_col_t table_col, void *arg) {
  for (ulint i = 0; i < table->n_cols; ++i) {
    auto col = dict_table_get_nth_col(table, i);
    auto col_no = dict_col_get_no(col);
    auto name = dict_table_get_col_name(table, col_no);
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
 * @param dict_index - in: index to visit
 * @param index_col - in: index column visitor
 * @param arg - in: argument to visitor
 *
 * @return return value from index_col
 */
static int ib_table_schema_visit_index_columns(
  const dict_index_t *dict_index, ib_schema_visitor_t::index_col_t index_col, void *arg
) {
  ulint n_index_cols = dict_index->n_user_defined_cols;

  for (ulint i = 0; i < n_index_cols; ++i) {
    auto dfield = &dict_index->fields[i];
    auto user_err = index_col(arg, dfield->name, dfield->prefix_len);

    if (user_err) {
      return user_err;
    }
  }

  return 0;
}

ib_err_t ib_table_schema_visit(ib_trx_t ib_trx, const char *name, const ib_schema_visitor_t *visitor, void *arg) {
  ib_tbl_fmt_t tbl_fmt;
  ulint page_size;
  int user_err = 0;

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
    dict_table_increment_handle_count(table, true);
  } else {
    return DB_TABLE_NOT_FOUND;
  }

  ib_table_get_format(table, &tbl_fmt, &page_size);

  /* We need the count of user defined indexes only. */
  auto n_indexes = UT_LIST_GET_LEN(table->indexes);

  /* The first index is always the cluster index. */
  auto dict_index = dict_table_get_first_index(table);

  /* Only the clustered index can be auto generated. */
  if (dict_index->n_user_defined_cols == 0) {
    --n_indexes;
  }

  if (visitor->version < ib_schema_visitor_t::Version::TABLE) {

    goto func_exit;

  } else if (visitor->table) {
    user_err = visitor->table(arg, table->name, tbl_fmt, page_size, table->n_cols, n_indexes);

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
    auto n_index_cols = dict_index->n_user_defined_cols;

    /* Ignore system generated indexes. */
    if (n_index_cols > 0) {
      user_err = visitor->index(
        arg, dict_index->name, dict_index_is_unique(dict_index) != 0, dict_index_is_clust(dict_index) != 0, n_index_cols
      );
      if (user_err > 0) {
        goto func_exit;
      }

      if (visitor->version >= ib_schema_visitor_t::Version::TABLE_AND_INDEX_COL && visitor->index_col) {
        user_err = ib_table_schema_visit_index_columns(dict_index, visitor->index_col, arg);

        if (user_err > 0) {
          break;
        }
      }
    }

    dict_index = UT_LIST_GET_NEXT(indexes, dict_index);
  } while (dict_index != nullptr);

func_exit:
  ut_a(ib_schema_lock_is_exclusive(ib_trx));
  dict_table_decrement_handle_count(table, true);

  return user_err != 0 ? DB_ERROR : DB_SUCCESS;
}

ib_err_t ib_schema_tables_iterate(ib_trx_t ib_trx, ib_schema_visitor_table_all_t visitor, void *arg) {
  ib_err_t err;
  dict_table_t *table;
  ib_crsr_t ib_crsr;
  ib_err_t crsr_err;
  ib_tpl_t ib_tpl = nullptr;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  table = ib_lookup_table_by_name("SYS_TABLES");

  if (table != nullptr) {
    dict_table_increment_handle_count(table, true);
    err = ib_create_cursor(&ib_crsr, table, 0, (trx_t *)ib_trx);
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
 * @param ib_tpl - upd: tuple to write to
 * @param col_no - in: column number
 * @param value - in: integer value
 * @param value_len - in: sizeof value type
 *
 * @return DB_SUCCESS or error
 */
static ib_err_t ib_tuple_write_int(ib_tpl_t ib_tpl, ulint col_no, const void *value, ulint value_len) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

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
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  cursor->prebuilt->sql_stat_start = true;
}

ib_err_t ib_tuple_write_double(ib_tpl_t ib_tpl, int col_no, double val) {
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

ib_err_t ib_tuple_read_double(ib_tpl_t ib_tpl, ulint col_no, double *dval) {
  ib_err_t err;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

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
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  const auto dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    return ib_col_set_value(ib_tpl, col_no, &val, sizeof(val));
  } else {
    return DB_DATA_MISMATCH;
  }
}

ib_err_t ib_tuple_read_float(ib_tpl_t ib_tpl, ulint col_no, float *fval) {
  ib_err_t err;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

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
  trx_t *trx = (trx_t *)ib_trx;

  if (trx->error_info == nullptr) {
    return DB_ERROR;
  }

  *table_name = trx->error_info->table_name;
  *index_name = trx->error_info->name;

  return DB_SUCCESS;
}

ib_err_t ib_get_table_statistics(ib_crsr_t ib_crsr, ib_table_stats_t *table_stats, size_t sizeof_ib_table_stats_t) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_table_t *table = cursor->prebuilt->table;

  if (table->stat_initialized != true) {
    dict_update_statistics(table);
  }

  table_stats->stat_n_rows = table->stat_n_rows;
  table_stats->stat_clustered_index_size = table->stat_clustered_index_size * UNIV_PAGE_SIZE;
  table_stats->stat_sum_of_other_index_sizes = table->stat_sum_of_other_index_sizes * UNIV_PAGE_SIZE;
  table_stats->stat_modified_counter = table->stat_modified_counter;

  return DB_SUCCESS;
}

ib_err_t ib_get_index_stat_n_diff_key_vals(ib_crsr_t ib_crsr, const char *index_name, uint64_t *ncols, int64_t **n_diff) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *index;

  if (table->stat_initialized != true) {
    dict_update_statistics(table);
  }

  index = dict_table_get_index_on_name(table, index_name);

  if (index == nullptr) {
    return DB_NOT_FOUND;
  }

  *ncols = index->n_uniq;

  *n_diff = (int64_t *)malloc(sizeof(int64_t) * index->n_uniq);

  dict_index_stat_mutex_enter(index);

  memcpy(*n_diff, index->stat_n_diff_key_vals, sizeof(int64_t) * index->n_uniq);

  dict_index_stat_mutex_exit(index);

  return DB_SUCCESS;
}

ib_err_t ib_update_table_statistics(ib_crsr_t crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)crsr;
  dict_table_t *table = cursor->prebuilt->table;

  dict_update_statistics(table);

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

  auto trx = reinterpret_cast<trx_t *>(ib_trx);

  ut::Sharded_counter<Parallel_reader::MAX_THREADS> n_recs{};

  n_recs.clear();

  const Parallel_reader::Scan_range full_scan;

  dberr_t err{DB_SUCCESS};
  Parallel_reader reader(n_threads);
  std::vector<dict_table_t *> tables{};

  for (auto &ib_crsr : ib_crsrs) {
    auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
    Parallel_reader::Config config(full_scan, cursor->prebuilt->index);

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

static dberr_t check_table(trx_t *trx, dict_index_t *index, size_t n_threads) {
  ut_a(n_threads > 1);

  using Shards = ut::Sharded_counter<Parallel_reader::MAX_THREADS>;

  Shards n_recs{};
  Shards n_dups{};
  Shards n_corrupt{};

  n_dups.clear();
  n_recs.clear();
  n_corrupt.clear();

  using Tuples = std::vector<dtuple_t *>;
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

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Source_location{});
    }

    if (prev_tuple != nullptr) {
      ulint matched_fields = 0;

      auto cmp = prev_tuple->compare(rec, index, offsets, &matched_fields);

      /* In a unique secondary index we allow equal key values if
      they contain SQL NULLs */

      bool contains_null = false;
      const auto n_ordering = dict_index_get_n_ordering_defined_by_user(index);

      for (size_t i = 0; i < n_ordering; ++i) {
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
        rec_os << rec_to_string(rec);

        log_err(std::format(
          "Index records in a wrong order in index {} of table {}: {}, {}",
          index->name,
          index->table->name,
          dtuple_os.str(),
          rec_os.str()
        ));

        /* Continue reading */
      } else if (dict_index_is_unique(index) && !contains_null &&
                 matched_fields >= dict_index_get_n_ordering_defined_by_user(index)) {

        std::ostringstream rec_os{};
        std::ostringstream dtuple_os{};

        n_dups.inc(1, id);
        rec_os << rec_to_string(rec);
        prev_tuple->print(dtuple_os);

        log_err(
          std::format("Duplicate key in {} of table {}: {}, {}", index->name, index->table->name, dtuple_os.str(), rec_os.str())
        );
      }
    }

    if (prev_blocks[id] != block || prev_blocks[id] == nullptr) {
      mem_heap_empty(heap);

      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Source_location{});

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
    log_err("Found ", n_dups.value(), " duplicate rows in ", index->name);
    err = DB_DUPLICATE_KEY;
  }

  if (n_corrupt.value() > 0) {
    log_err("Found ", n_corrupt.value(), " rows in the wrong order in ", index->name);
    err = DB_INDEX_CORRUPT;
  }

  return err;
}

ib_err_t ib_check_table(ib_trx_t *ib_trx, ib_crsr_t ib_crsr, size_t n_threads) {
  auto trx = reinterpret_cast<trx_t *>(ib_trx);
  auto cursor = reinterpret_cast<ib_cursor_t *>(ib_crsr);
  auto prebuilt = cursor->prebuilt;
  auto index = prebuilt->index;

  ut_a(prebuilt->trx->m_conc_state == TRX_NOT_STARTED);

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

  if (trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && prebuilt->select_lock_type == LOCK_NONE && index->is_clustered() &&
      trx->client_n_tables_locked == 0) {
    n_threads = Parallel_reader::available_threads(n_threads, false);

    auto rv = trx_assign_read_view(trx);
    ut_a(rv != nullptr);

    auto trx = prebuilt->trx;

    ut_a(prebuilt->table == index->table);

    std::vector<dict_index_t *> indexes;

    indexes.push_back(index);

    return check_table(trx, index, n_threads);
  } else {
    log_err("Invalid transaction state for check table");
    return DB_ERROR;
  }
}
