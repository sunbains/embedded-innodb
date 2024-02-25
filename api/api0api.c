/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith

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

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "api0api.h"
#include "api0misc.h"
#include "api0ucode.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0crea.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "pars0pars.h"
#include "rem0cmp.h"
#include "row0ins.h"
#include "row0merge.h"
#include "row0prebuilt.h"
#include "row0sel.h"
#include "row0upd.h"
#include "row0vers.h"
#include "srv0start.h"
#include "trx0roll.h"
#include "univ.i"
#include "ut0dbg.h" /* for UT_DBG_ENTER_FUNC */

static const char *GEN_CLUST_INDEX = "GEN_CLUST_INDEX";

/* All InnoDB error messages are written to this stream. */
ib_logger_t ib_logger;
ib_stream_t ib_stream;

/* This must hold. */
#if IB_TRUE != TRUE || IB_FALSE != FALSE
#error IB_TRUE != TRUE or IB_FALSE != FALSE
#endif

static int api_api_enter_func_enabled = 0;
#define UT_DBG_ENTER_FUNC_ENABLED api_api_enter_func_enabled

/* Protected by the schema lock */
typedef struct ib_db_format_t {
  ulint id; /* Numeric representation
            of database format */

  const char *name; /* Text representation of name,
                    allocated using ut_malloc() and
                    should be automatically freed at
                    InnoDB shutdown */
} ib_db_format_t;

/* This value is read at database startup. */
static ib_db_format_t db_format;

/** @file api/api0api.c
Does a simple memcmp(3).
@return	1, 0, -1, if a is greater, equal, less than b, respectively */
static int
ib_default_compare(const ib_col_meta_t *col_meta, /*!< in: column meta data */
                   const ib_byte_t *p1,           /*!< in: packed key */
                   ib_ulint_t p1_len,             /*!< in: packed key length */
                   const ib_byte_t *p2,           /*!< in: packed key */
                   ib_ulint_t p2_len);            /*!< in: packed key length */

ib_client_cmp_t ib_client_compare = ib_default_compare;

/* InnoDB tuple types. */
typedef enum ib_tuple_type_enum {
  TPL_ROW, /* Data row tuple */
  TPL_KEY  /* Index key tuple */
} ib_tuple_type_t;

/* Query types supported. */
typedef enum ib_qry_type_enum {
  QRY_NON, /* None/Sentinel */
  QRY_INS, /* Insert operation */
  QRY_UPD, /* Update operation */
  QRY_SEL  /* Select operation */
} ib_qry_type_t;

/* Query graph types. */
typedef struct ib_qry_grph_struct {
  que_fork_t *ins; /* Innobase SQL query graph used
                   in inserts */
  que_fork_t *upd; /* Innobase SQL query graph used
                   in updates or deletes */
  que_fork_t *sel; /* dummy query graph used in
                   selects */

} ib_qry_grph_t;

/* Query node types. */
typedef struct ib_qry_node_struct {
  ins_node_t *ins; /* Innobase SQL insert node
                   used to perform inserts to the table */
  upd_node_t *upd; /* Innobase SQL update node
                   used to perform updates and deletes */
  sel_node_t *sel; /* Innobase SQL select node
                   used to perform selects on the table */
} ib_qry_node_t;

/* Query processing fields. */
typedef struct ib_qry_proc_struct {

  ib_qry_node_t node; /* Query node*/

  ib_qry_grph_t grph; /* Query graph */
} ib_qry_proc_t;

/* Cursor instance for traversing tables/indexes. This will eventually
become row_prebuilt_t. */
typedef struct ib_cursor_struct {
  mem_heap_t *heap; /* Instance heap */

  mem_heap_t *query_heap; /* Heap to use for query graphs */

  ib_qry_proc_t q_proc; /* Query processing info */

  ib_match_mode_t match_mode; /* ib_cursor_moveto match mode */

  row_prebuilt_t *prebuilt; /* For reading rows */
} ib_cursor_t;

/* InnoDB table columns used during table and index schema creation. */
typedef struct ib_col_struct {
  const char *name; /* Name of column */

  ib_col_type_t ib_col_type; /* Main type of the column */

  ulint len; /* Length of the column */

  ib_col_attr_t ib_col_attr; /* Column attributes */
} ib_col_t;

/* InnoDB index columns used during index and index schema creation. */
typedef struct ib_key_col_struct {
  const char *name; /* Name of column */

  ulint prefix_len; /* Column index prefix len or 0 */
} ib_key_col_t;

typedef struct ib_table_def_struct ib_table_def_t;

/* InnoDB index schema used during index creation */
typedef struct ib_index_def_struct {
  mem_heap_t *heap; /* Heap used to build this and all
                    its columns in the list */

  const char *name; /* Index name */

  dict_table_t *table; /* Parent InnoDB table */

  ib_table_def_t *schema; /* Parent table schema that owns
                          this instance */

  ibool clustered; /* True if clustered index */

  ibool unique; /* True if unique index */

  ib_vector_t *cols; /* Vector of columns */

  trx_t *usr_trx; /* User transacton covering the
                  DDL operations */
} ib_index_def_t;

/* InnoDB table schema used during table creation */
struct ib_table_def_struct {
  mem_heap_t *heap; /* Heap used to build this and all
                    its columns in the list */
  const char *name; /* Table name */

  ib_tbl_fmt_t ib_tbl_fmt; /* Row format */

  ulint page_size; /* Page size */

  ib_vector_t *cols; /* Vector of columns */

  ib_vector_t *indexes; /* Vector of indexes */

  dict_table_t *table; /* Table read from or NULL */
};

/* InnoDB tuple used for key operations. */
typedef struct ib_tuple_struct {
  mem_heap_t *heap; /* Heap used to build
                    this and for copying
                    the column values. */

  ib_tuple_type_t type; /* Tuple discriminitor. */

  const dict_index_t *index; /* Index for tuple can be either
                             secondary or cluster index. */

  dtuple_t *ptr; /* The internal tuple
                 instance */
} ib_tuple_t;

/* I can't see what merge has to do with creating an Index. */
typedef merge_index_def_t index_def_t;
typedef merge_index_field_t index_field_t;

/* The following counter is used to convey information to InnoDB
about server activity: in selects it is not sensible to call
srv_active_wake_master_thread after each fetch or search, we only do
it every INNOBASE_WAKE_INTERVAL'th step. */

#define INNOBASE_WAKE_INTERVAL 32

/** Does a simple memcmp(3).
@return	1, 0, -1, if a is greater, equal, less than b, respectively */
static int ib_default_compare(
    const ib_col_meta_t *ib_col_meta, /*!< in: column meta data */
    const ib_byte_t *p1,              /*!< in: packed key */
    ib_ulint_t p1_len,                /*!< in: packed key length */
    const ib_byte_t *p2,              /*!< in: packed key */
    ib_ulint_t p2_len)                /*!< in: packed key length */
{
  int ret;

  (void)ib_col_meta;

  UT_DBG_ENTER_FUNC;

  ret = memcmp(p1, p2, ut_min(p1_len, p2_len));

  if (ret == 0) {
    ret = p1_len - p2_len;
  }

  return (ret < 0 ? -1 : ((ret > 0) ? 1 : 0));
}

/** Check if the Innodb persistent cursor is positioned.
@return	IB_TRUE if positioned */
UNIV_INLINE
ib_bool_t ib_btr_cursor_is_positioned(
    btr_pcur_t *pcur) /*!< in: InnoDB persistent cursor */
{
  return (pcur->old_stored == BTR_PCUR_OLD_STORED &&
          (pcur->pos_state == BTR_PCUR_IS_POSITIONED ||
           pcur->pos_state == BTR_PCUR_WAS_POSITIONED));
}

/** Delays an INSERT, DELETE or UPDATE operation if the purge is lagging. */
static void ib_delay_dml_if_needed(void) {
  if (srv_dml_needed_delay) {
    os_thread_sleep(srv_dml_needed_delay);
  }
}

/** Open a table using the table id, if found then increment table ref count.
@return	table instance if found */
static dict_table_t *
ib_open_table_by_id(ib_id_t tid,      /*!< in: table id to lookup */
                    ib_bool_t locked) /*!< in: TRUE if own dict mutex */
{
  dict_table_t *table;
  dulint table_id;

  UT_DBG_ENTER_FUNC;

  /* We only return the lower 32 bits of the dulint. */
  ut_a(tid < 0xFFFFFFFFULL);
  table_id = ut_dulint_create(0, (ulint)tid);

  if (!locked) {
    dict_mutex_enter();
  }

  table = dict_table_get_using_id(srv_force_recovery, table_id, TRUE);

  if (table != NULL && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n",
              table->name);

    dict_table_decrement_handle_count(table, TRUE);

    table = NULL;
  }

  if (!locked) {
    dict_mutex_exit();
  }

  return (table);
}

/** Open a table using the table name, if found then increment table ref count.
@return	table instance if found */
static dict_table_t *
ib_open_table_by_name(const char *name) /*!< in: table name to lookup */
{
  dict_table_t *table;

  UT_DBG_ENTER_FUNC;

  table = dict_table_get(name, TRUE);

  if (table != NULL && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n", name);

    dict_table_decrement_handle_count(table, FALSE);

    table = NULL;
  }

  return (table);
}

/** Find table using table name.
@return	table instance if found */
static dict_table_t *
ib_lookup_table_by_name(const char *name) /*!< in: table name to lookup */
{
  dict_table_t *table;

  UT_DBG_ENTER_FUNC;

  table = dict_table_get_low(name);

  if (table != NULL && table->ibd_file_missing) {

    ib_logger(ib_stream, "The .ibd file for table %s is missing.\n", name);

    table = NULL;
  }

  return (table);
}

/** Increments innobase_active_counter and every INNOBASE_WAKE_INTERVALth
time calls srv_active_wake_master_thread. This function should be used
when a single database operation may introduce a small need for
server utility activity, like checkpointing. */
UNIV_INLINE
void ib_wake_master_thread(void) {
  static ulint ib_signal_counter = 0;

  UT_DBG_ENTER_FUNC;

  ++ib_signal_counter;

  if ((ib_signal_counter % INNOBASE_WAKE_INTERVAL) == 0) {
    srv_active_wake_master_thread();
  }
}

#if 0

/** Calculate the length of the column data less trailing space. */
static
ulint
ib_varchar_len(
	const dtype_t*	dtype,
	ib_byte_t*	ptr,
	ulint		len)
{
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
UNIV_INLINE
ulint ib_get_max_row_len(dict_index_t *cluster) /*!< in: cluster index */
{
  ulint i;
  ulint max_len = 0;
  ulint n_fields = cluster->n_fields;

  UT_DBG_ENTER_FUNC;

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

  return (max_len);
}
#endif

/** Read the colums from a rec into a tuple. */
UNIV_INLINE
void ib_read_tuple(
    const rec_t *rec,      /*!< in: Record to read */
    ib_bool_t page_format, /*!< in: IB_TRUE if compressed format */
    ib_tuple_t *tuple)     /*!< in: tuple to read into */
{
  ulint i;
  void *ptr;
  rec_t *copy;
  ulint rec_meta_data;
  ulint n_index_fields;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  dtuple_t *dtuple = tuple->ptr;
  const dict_index_t *dindex = tuple->index;

  UT_DBG_ENTER_FUNC;

  rec_offs_init(offsets_);

  offsets =
      rec_get_offsets(rec, dindex, offsets, ULINT_UNDEFINED, &tuple->heap);

  rec_meta_data = rec_get_info_bits(rec, page_format);
  dtuple_set_info_bits(dtuple, rec_meta_data);

  /* Make a copy of the rec. */
  ptr = mem_heap_alloc(tuple->heap, rec_offs_size(offsets));
  copy = rec_copy(ptr, rec, offsets);

  /* Avoid a debug assertion in rec_offs_validate(). */
  rec_offs_make_valid(rec, dindex, (ulint *)offsets);

  n_index_fields =
      ut_min(rec_offs_n_fields(offsets), dtuple_get_n_fields(dtuple));

  for (i = 0; i < n_index_fields; ++i) {
    ulint len;
    const byte *data;
    dfield_t *dfield;

    if (tuple->type == TPL_ROW) {
      const dict_col_t *col;
      ulint col_no;
      const dict_field_t *index_field;

      index_field = dict_index_get_nth_field(dindex, i);
      col = dict_field_get_col(index_field);
      col_no = dict_col_get_no(col);

      dfield = dtuple_get_nth_field(dtuple, col_no);
    } else {
      dfield = dtuple_get_nth_field(dtuple, i);
    }

    data = rec_get_nth_field(copy, offsets, i, &len);

    /* Fetch and copy any externally stored column. */
    if (rec_offs_nth_extern(offsets, i)) {

      ulint zip_size;

      zip_size = dict_table_zip_size(dindex->table);

      data = btr_rec_copy_externally_stored_field(copy, offsets, zip_size, i,
                                                  &len, tuple->heap);

      ut_a(len != UNIV_SQL_NULL);
    }

    dfield_set_data(dfield, data, len);
  }
}

/** Create an InnoDB key tuple.
@return	tuple instance created, or NULL */
static ib_tpl_t ib_key_tuple_new_low(
    const dict_index_t *dict_index, /*!< in: index for which tuple required */
    ulint n_cols,                   /*!< in: no. of user defined cols */
    mem_heap_t *heap)               /*!< in: memory heap */
{
  ib_tuple_t *tuple;
  ulint i;
  ulint n_cmp_cols;

  UT_DBG_ENTER_FUNC;

  tuple = mem_heap_alloc(heap, sizeof(*tuple));

  if (tuple == NULL) {
    mem_heap_free(heap);
    return (NULL);
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

  for (i = 0; i < n_cols; i++) {

    dfield_t *dfield;

    dfield = dtuple_get_nth_field(tuple->ptr, i);
    dfield_set_null(dfield);
  }

  n_cmp_cols = dict_index_get_n_ordering_defined_by_user(dict_index);

  dtuple_set_n_fields_cmp(tuple->ptr, n_cmp_cols);

  return ((ib_tpl_t)tuple);
}

/** Create an InnoDB key tuple.
@return	tuple instance created, or NULL */
static ib_tpl_t
ib_key_tuple_new(const dict_index_t *dict_index, /*!< in: index of tuple */
                 ulint n_cols) /*!< in: no. of user defined cols */
{
  mem_heap_t *heap;

  UT_DBG_ENTER_FUNC;

  heap = mem_heap_create(64);

  if (heap == NULL) {
    return (NULL);
  }

  return (ib_key_tuple_new_low(dict_index, n_cols, heap));
}

/** Create an InnoDB row tuple.
@return	tuple instance, or NULL */
static ib_tpl_t
ib_row_tuple_new_low(const dict_index_t *dict_index, /*!< in: index of tuple */
                     ulint n_cols,     /*!< in: no. of cols in tuple */
                     mem_heap_t *heap) /*!< in: memory heap */
{
  ib_tuple_t *tuple;

  UT_DBG_ENTER_FUNC;

  tuple = mem_heap_alloc(heap, sizeof(*tuple));

  if (tuple == NULL) {
    mem_heap_free(heap);
    return (NULL);
  }

  tuple->heap = heap;
  tuple->index = dict_index;
  tuple->type = TPL_ROW;

  tuple->ptr = dtuple_create(heap, n_cols);

  /* Copy types and set to SQL_NULL. */
  dict_table_copy_types(tuple->ptr, dict_index->table);

  return ((ib_tpl_t)tuple);
}

/** Create an InnoDB row tuple.
@return	tuple instance, or NULL */
static ib_tpl_t
ib_row_tuple_new(const dict_index_t *dict_index, /*!< in: index of tuple */
                 ulint n_cols) /*!< in: no. of cols in tuple */
{
  mem_heap_t *heap;

  UT_DBG_ENTER_FUNC;

  heap = mem_heap_create(64);

  if (heap == NULL) {
    return (NULL);
  }

  return (ib_row_tuple_new_low(dict_index, n_cols, heap));
}

/** Return the API version number, the version number format is:
| 16 bits future use | 16 bits current | 16 bits revision | 16 bits age |

- If the library source code has changed at all since the last release,
  then revision will be incremented (`c:r:a' becomes `c:r+1:a').
- If any interfaces have been added, removed, or changed since the last
  update, current will be incremented, and revision will be set to 0.
- If any interfaces have been added (but not changed or removed) since
  the last release, then age will be incremented.
- If any interfaces have been changed or removed since the last release,
  then age will be set to 0.

@return	API version number */

ib_u64_t ib_api_version(void) {
  return (((ib_u64_t)IB_API_VERSION_CURRENT << 32) |
          (IB_API_VERSION_REVISION << 16) | IB_API_VERSION_AGE);
}

/** Initialize the InnoDB engine. This must be called prior to calling
any other InnoDB API function.
@return	DB_SUCCESS or error code */

ib_err_t ib_init(void) {
  ib_err_t ib_err;

  IB_CHECK_PANIC();

  ut_mem_init();

  ib_logger = (ib_logger_t)fprintf;
  ib_stream = stderr;

  ib_err = ib_cfg_init();

  return (ib_err);
}

/** Startup the InnoDB engine.
@return	DB_SUCCESS or error code */

ib_err_t ib_startup(const char *format) /*!< in: file format name */
{
  ib_err_t err = DB_SUCCESS;

  UT_DBG_ENTER_FUNC;

  db_format.id = 0;
  db_format.name = NULL;

  /* Validate the file format if set by the user. */
  if (format != NULL) {
    db_format.id = trx_sys_file_format_name_to_id(format);

    /* Check if format name was found. */
    if (db_format.id > DICT_TF_FORMAT_MAX) {
      err = DB_UNSUPPORTED;

      ib_logger(ib_stream, "InnoDB: format '%s' unknown.", format);
    }
  }

  if (err == DB_SUCCESS) {

    db_format.name = trx_sys_file_format_id_to_name(db_format.id);

    /* Set the highest file format id supported. */
    srv_file_format = db_format.id;

    err = innobase_start_or_create();
  }

  return (err);
}

/** Shutdown the InnoDB engine.
@return	DB_SUCCESS or error code */

ib_err_t ib_shutdown(ib_shutdown_t flag) /*!< in: Shutdown flags */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_cfg_shutdown();
  if (err != DB_SUCCESS) {
    ib_logger(ib_stream,
              "ib_cfg_shutdown(): %s; "
              "continuing shutdown anyway\n",
              ib_strerror(err));
  }

  db_format.id = 0;
  db_format.name = NULL;

  err = innobase_shutdown(flag);

  return (err);
}

/** Begin a transaction.
@return	innobase txn handle */

ib_err_t
ib_trx_start(ib_trx_t ib_trx,             /*!< in: transaction to restart */
             ib_trx_level_t ib_trx_level) /*!< in: trx isolation level */
{
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  UT_DBG_ENTER_FUNC;

  ut_a(ib_trx_level >= IB_TRX_READ_UNCOMMITTED);
  ut_a(ib_trx_level <= IB_TRX_SERIALIZABLE);

  ut_ad(trx->client_thread_id == os_thread_get_curr_id());

  if (trx->conc_state == TRX_NOT_STARTED) {
    ib_bool_t started;

    started = trx_start(trx, ULINT_UNDEFINED);
    ut_a(started);

    trx->isolation_level = ib_trx_level;
  } else {
    err = DB_ERROR;
  }

  trx->client_thd = NULL;

  return (err);
}

void ib_trx_set_client_data(ib_trx_t ib_trx, /*!< in: transaction */
                            void *client_data) {
  trx_t *trx = (trx_t *)ib_trx;

  trx->client_thd = client_data;
}

/** Begin a transaction. This will allocate a new transaction handle.
@return	innobase txn handle */

ib_trx_t
ib_trx_begin(ib_trx_level_t ib_trx_level) /*!< in: trx isolation level */
{
  trx_t *trx;
  ib_bool_t started;

  UT_DBG_ENTER_FUNC;

  trx = trx_allocate_for_client(NULL);
  started = ib_trx_start((ib_trx_t)trx, ib_trx_level);
  ut_a(started);

  return ((ib_trx_t)trx);
}

/** Get the transaction's state.
@return	transaction state */

ib_trx_state_t ib_trx_state(ib_trx_t ib_trx) /*!< in: trx handle */
{
  trx_t *trx = (trx_t *)ib_trx;

  UT_DBG_ENTER_FUNC;

  return ((ib_trx_state_t)trx->conc_state);
}

/** Release the resources of the transaction.
@return	DB_SUCCESS or err code */

ib_err_t ib_trx_release(ib_trx_t ib_trx) /*!< in: trx handle */
{
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_ad(trx != NULL);
  trx_free_for_client(trx);

  return (DB_SUCCESS);
}

/** Commit a transaction. This function will also release the schema
latches too.
@return	DB_SUCCESS or err code */

ib_err_t ib_trx_commit(ib_trx_t ib_trx) /*!< in: trx handle */
{
  ib_err_t err;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  err = trx_commit(trx);

  /* It should always succeed */
  ut_a(err == DB_SUCCESS);

  ib_schema_unlock(ib_trx);

  err = ib_trx_release(ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread();

  return (DB_SUCCESS);
}

/** Rollback a transaction. This function will also release the schema
latches too.
@return	DB_SUCCESS or err code */

ib_err_t ib_trx_rollback(ib_trx_t ib_trx) /*!< in: trx handle */
{
  ib_err_t err;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  err = trx_general_rollback(trx, FALSE, NULL);

  /* It should always succeed */
  ut_a(err == DB_SUCCESS);

  ib_schema_unlock(ib_trx);

  err = ib_trx_release(ib_trx);
  ut_a(err == DB_SUCCESS);

  ib_wake_master_thread();

  return (err);
}

/** Check that the combination of values and name makes sense.
@return	TRUE if OK */
UNIV_INLINE
ibool ib_check_col_is_ok(const char *name,          /*!< in: column name */
                         ib_col_type_t ib_col_type, /*!< in: column type */
                         ib_col_attr_t ib_col_attr, /*!< in: column attribute */
                         ib_ulint_t len) /*!< in: length of the column */
{
  UT_DBG_ENTER_FUNC;

  if (ut_strlen(name) > IB_MAX_COL_NAME_LEN) {
    return (FALSE);
  } else if ((ib_col_type == IB_VARCHAR || ib_col_type == IB_CHAR ||
              ib_col_type == IB_BINARY) &&
             len == 0) {
    return (FALSE);
  } else if (ib_col_type == IB_INT) {
    switch (len) {
    case 1:
    case 2:
    case 4:
    case 8:
      break;
    default:
      return (FALSE);
    }
  } else if (ib_col_type == IB_FLOAT && len != 4) {
    return (FALSE);
  } else if (ib_col_type == IB_DOUBLE && len != 8) {
    return (FALSE);
  }

  return (TRUE);
}

/** Find an index definition from the index vector using index name.
@return	index def. if found else NULL */
UNIV_INLINE
const ib_index_def_t *
ib_table_find_index(ib_vector_t *indexes, /*!< in: vector of indexes */
                    const char *name)     /*!< in: index name */
{
  ulint i;

  UT_DBG_ENTER_FUNC;

  for (i = 0; i < ib_vector_size(indexes); ++i) {
    const ib_index_def_t *index_def;

    index_def = (ib_index_def_t *)ib_vector_get(indexes, i);

    if (ib_utf8_strcasecmp(name, index_def->name) == 0) {
      return (index_def);
    }
  }

  return (NULL);
}

/** Get the InnoDB internal precise type from the schema column definition.
@return	precise type in api format */
UNIV_INLINE
ulint ib_col_get_prtype(const ib_col_t *ib_col) /*!< in: column definition */
{
  ulint prtype = 0;

  UT_DBG_ENTER_FUNC;

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

  return (prtype);
}

/** Get the InnoDB internal main type from the schema column definition.
@return	column main type */
UNIV_INLINE
ulint ib_col_get_mtype(const ib_col_t *ib_col) /*!< in: column definition */
{
  UT_DBG_ENTER_FUNC;

  /* Note: The api0api.h types should map directly to
  the internal numeric codes. */
  return (ib_col->ib_col_type);
}

/** Find a column in the the column vector with the same name.
@return	col. def. if found else NULL */
UNIV_INLINE
const ib_col_t *
ib_table_find_col(const ib_vector_t *cols, /*!< in: column list head */
                  const char *name)        /*!< in: column name to find */
{
  ulint i;

  UT_DBG_ENTER_FUNC;

  for (i = 0; i < ib_vector_size(cols); ++i) {
    const ib_col_t *ib_col;

    ib_col = ib_vector_get((ib_vector_t *)cols, i);

    if (ib_utf8_strcasecmp(ib_col->name, name) == 0) {
      return (ib_col);
    }
  }

  return (NULL);
}

/** Find a column in the the column list with the same name.
@return	col. def. if found else NULL */
UNIV_INLINE
const ib_key_col_t *
ib_index_find_col(ib_vector_t *cols, /*!< in: column list head */
                  const char *name)  /*!< in: column name to find */
{
  ulint i;

  UT_DBG_ENTER_FUNC;

  for (i = 0; i < ib_vector_size(cols); ++i) {
    const ib_key_col_t *ib_col;

    ib_col = ib_vector_get(cols, i);

    if (ib_utf8_strcasecmp(ib_col->name, name) == 0) {
      return (ib_col);
    }
  }

  return (NULL);
}

/** Add columns to a table schema.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_schema_add_col(
    ib_tbl_sch_t ib_tbl_sch,   /*!< in: schema instance */
    const char *name,          /*!< in: name of column */
    ib_col_type_t ib_col_type, /*!< in: column main type */
    ib_col_attr_t ib_col_attr, /*!< in: column attributes */
    ib_u16_t client_type,      /*!< in: any 16 bit number
                               relevant      only to the client */
    ib_ulint_t len)            /*!< in: max length of column */
{
  ib_col_t *ib_col;
  ib_err_t err = DB_SUCCESS;
  ib_table_def_t *table_def = (ib_table_def_t *)ib_tbl_sch;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (table_def->table != NULL) {
    err = DB_ERROR;
  } else if (ib_table_find_col(table_def->cols, name) != NULL) {
    err = DB_DUPLICATE_KEY;
  } else if (!ib_check_col_is_ok(name, ib_col_type, ib_col_attr, len)) {
    err = DB_ERROR;
  } else {
    mem_heap_t *heap;

    heap = table_def->heap;

    ib_col = (ib_col_t *)mem_heap_zalloc(heap, sizeof(*ib_col));

    if (ib_col == NULL) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->name = mem_heap_strdup(heap, name);
      ib_col->ib_col_type = ib_col_type;
      ib_col->ib_col_attr = ib_col_attr;

      ib_col->len = len;

      ib_vector_push(table_def->cols, ib_col);
    }
  }

  return (err);
}

/** Create and add an index key definition to a table schema. The index
schema is owned by the table schema instance and will be freed when
the table schema instance is freed.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_schema_add_index(
    ib_tbl_sch_t ib_tbl_sch,  /*!< in/out: schema instance */
    const char *name,         /*!< in: key defn. name to create */
    ib_idx_sch_t *ib_idx_sch) /*!< out: key definition instance */
{
  ib_err_t err = DB_SUCCESS;
  ib_table_def_t *table_def = (ib_table_def_t *)ib_tbl_sch;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (table_def->table != NULL) {
    err = DB_ERROR;
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return (DB_INVALID_INPUT);
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return (DB_INVALID_INPUT);
  }
  if (ib_table_find_index(table_def->indexes, name) != NULL) {
    err = DB_DUPLICATE_KEY;
  } else {
    ib_index_def_t *index_def;
    mem_heap_t *heap = table_def->heap;

    index_def = (ib_index_def_t *)mem_heap_zalloc(heap, sizeof(*index_def));

    if (index_def == NULL) {
      err = DB_OUT_OF_MEMORY;
    } else {

      index_def->heap = heap;

      index_def->schema = table_def;

      index_def->name = mem_heap_strdup(heap, name);

      index_def->cols = ib_vector_create(heap, 8);

      ib_vector_push(table_def->indexes, index_def);

      *ib_idx_sch = (ib_idx_sch_t)index_def;
    }
  }

  return (err);
}

/** Destroy a schema. */

void ib_table_schema_delete(
    ib_tbl_sch_t ib_tbl_sch) /*!< in, own: table schema to delete */
{
  ulint i;
  ib_table_def_t *table_def = (ib_table_def_t *)ib_tbl_sch;

  UT_DBG_ENTER_FUNC;

  /* Check that all indexes are owned by the table schema. */
  for (i = 0; i < ib_vector_size(table_def->indexes); ++i) {
    ib_index_def_t *index_def;

    index_def = (ib_index_def_t *)ib_vector_get(table_def->indexes, i);

    ut_a(index_def->schema != NULL);
  }

  if (table_def->table != NULL) {
    dict_table_decrement_handle_count(table_def->table, FALSE);
  }

  mem_heap_free(table_def->heap);
}

/** Do some table page size validation. It should be set only
when ib_tbl_fmt == IB_TBL_COMPRESSED. */
static ib_err_t
ib_table_schema_check(ib_tbl_fmt_t ib_tbl_fmt, /*!< in: table format */
                      ib_ulint_t *page_size) /*!< in,out: page size requested */
{
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

#ifndef WITH_ZIP
  if (ib_tbl_fmt == IB_TBL_COMPRESSED) {
    return (DB_UNSUPPORTED);
  }
#endif /* WITH_ZIP */
  if (ib_tbl_fmt != IB_TBL_COMPRESSED) {
    /* Page size set but table format is not set to compressed.
    Reset to 0 since we ignore such values. */
    *page_size = 0;
  }

  switch (*page_size) {
  case 0:
    /* The page size value will be ignored for
    uncompressed tables. */
    if (ib_tbl_fmt == IB_TBL_COMPRESSED) {
      /* Set to the system default of 8K page size.
      Better to be conservative here. */
      *page_size = 8;
      /* Fall through. */
    } else {
      break;
    }
  case 1:
  case 2:
  case 4:
  case 8:
  case 16:
    if (!srv_file_per_table) {
      /* Compressed tables require file per table. */
      err = DB_UNSUPPORTED;
    } else if (srv_file_format < DICT_TF_FORMAT_ZIP) {
      /* File format unsuitable for compressed tables. */
      err = DB_UNSUPPORTED;
    }
    break;
  default:
    /* Unknown page size. */
    err = DB_UNSUPPORTED;
    break;
  }

  return (err);
}

#ifdef __WIN__
/** Convert a string to lower case. */
static void ib_to_lower_case(char *ptr) /*!< string to convert to lower case */
{
  while (*ptr) {
    *ptr = tolower(*ptr);
    ++ptr;
  }
}
#endif /* __WIN__ */

/** Normalizes a table name string. A normalized name consists of the
database name catenated to '/' and table name. An example:
test/mytable. On Windows normalization puts both the database name and the
table name always to lower case. This function can be called for system
tables and they don't have a database component. For tables that don't have
a database component, we don't normalize them to lower case on Windows.
The assumption is that they are system tables that reside in the system
table space. */
static void
ib_normalize_table_name(char *norm_name,  /*!< out: normalized name as a
                                          null-terminated string */
                        const char *name) /*!< in: table name string */
{
  const char *ptr = name;

  /* Scan name from the end */

  ptr += ut_strlen(name) - 1;

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

    memcpy(norm_name, db_name, ut_strlen(name) + 1 - (db_name - name));

    norm_name[table_name - db_name - 1] = '/';
#ifdef __WIN__
    ib_to_lower_case(norm_name);
#endif
  } else {
    ut_strcpy(norm_name, name);
  }
}

/** Check whether the table name conforms to our requirements. Currently
we only do a simple check for the presence of a '/'.
@return	DB_SUCCESS or err code */
static ib_err_t
ib_table_name_check(const char *name) /*!< in: table name to check */
{
  const char *slash = NULL;
  ulint len = ut_strlen(name);

  if (len < 2 || *name == '/' || name[len - 1] == '/' ||
      (name[0] == '.' && name[1] == '/') ||
      (name[0] == '.' && name[1] == '.' && name[2] == '/')) {

    return (DB_DATA_MISMATCH);
  }

  for (; *name; ++name) {
#ifdef __WIN__
    /* Check for reserved characters in DOS filenames. */
    switch (*name) {
    case ':':
    case '|':
    case '"':
    case '*':
    case '<':
    case '>':
      return (DB_DATA_MISMATCH);
    }
#endif /* __WIN__ */
    if (*name == '/') {
      if (slash) {
        return (DB_DATA_MISMATCH);
      }
      slash = name;
    }
  }

  return (slash ? DB_SUCCESS : DB_DATA_MISMATCH);
}

/** Create a table schema.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_schema_create(
    const char *name,         /*!< in: table name to create */
    ib_tbl_sch_t *ib_tbl_sch, /*!< out: schema instance */
    ib_tbl_fmt_t ib_tbl_fmt,  /*!< in: table format */
    ib_ulint_t page_size)     /*!< in: Page size or 0 for default */
{
  ib_err_t err = DB_SUCCESS;
  mem_heap_t *heap = mem_heap_create(1024);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  err = ib_table_name_check(name);

  if (err != DB_SUCCESS) {
    return (err);
  }

  err = ib_table_schema_check(ib_tbl_fmt, &page_size);
  if (err != DB_SUCCESS) {
    return (err);
  } else if (heap == NULL) {
    err = DB_OUT_OF_MEMORY;
  } else {
    ib_table_def_t *table_def;

    table_def = (ib_table_def_t *)mem_heap_zalloc(heap, sizeof(*table_def));

    if (table_def == NULL) {
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

      table_def->cols = ib_vector_create(heap, 8);

      table_def->indexes = ib_vector_create(heap, 4);

      *ib_tbl_sch = (ib_tbl_sch_t)table_def;
    }
  }

  return (err);
}

/** Get the column number within the index defnintion.
@return	-1 or column number */
static int ib_index_get_col_no(
    const ib_index_def_t *ib_index_def, /*!< in: index definition */
    const char *name)                   /*!< in: column name to search */
{
  int col_no;

  UT_DBG_ENTER_FUNC;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != NULL) {
    col_no = dict_table_get_col_no(ib_index_def->table, name);
  } else {
    ib_vector_t *cols;
    const ib_col_t *ib_col;

    cols = ib_index_def->schema->cols;
    ib_col = ib_table_find_col(cols, name);

    if (ib_col != NULL) {
      /* We simply note that we've found the column. */
      col_no = 0;
    } else {
      col_no = -1;
    }
  }

  return (col_no);
}

/** Check whether a prefix length index is allowed on the column.
@return	TRUE if allowed. */
static int ib_index_is_prefix_allowed(
    const ib_index_def_t *ib_index_def, /*!< in: index definition */
    const char *name)                   /*!< in: column name to
                                        check */
{
  ib_bool_t allowed = TRUE;
  ulint mtype = ULINT_UNDEFINED;

  UT_DBG_ENTER_FUNC;

  /* Is this column definition for an existing table ? */
  if (ib_index_def->table != NULL) {
    const dict_col_t *col;
    int col_no;

    col_no = dict_table_get_col_no(ib_index_def->table, name);
    ut_a(col_no != -1);

    col = dict_table_get_nth_col(ib_index_def->table, col_no);
    ut_a(col != NULL);

    mtype = col->mtype;
  } else {
    const ib_vector_t *cols;
    const ib_col_t *ib_col;

    cols = ib_index_def->schema->cols;
    ib_col = ib_table_find_col(cols, name);

    ut_a(ib_col != NULL);

    mtype = (ulint)ib_col->ib_col_type;
  }

  /* The following column types can't have prefix column indexes. */
  switch (mtype) {
  case DATA_INT:
  case DATA_FLOAT:
  case DATA_DOUBLE:
  case DATA_DECIMAL:
    allowed = FALSE;
    break;

  case ULINT_UNDEFINED:
    ut_error;
  }

  return (allowed);
}

/** Add columns to a schema definition.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_schema_add_col(
    ib_idx_sch_t ib_idx_sch, /*!< in/out: index schema instance */
    const char *name,        /*!< in: name of column */
    ib_ulint_t prefix_len)   /*!< in: length of prefix or 0 */
{
  ib_err_t err = DB_SUCCESS;
  ib_index_def_t *index_def = (ib_index_def_t *)ib_idx_sch;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* Check for duplicates. */
  if (ib_index_find_col(index_def->cols, name) != NULL) {
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

    if (ib_col == NULL) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_col->name = mem_heap_strdup(heap, name);
      ib_col->prefix_len = prefix_len;

      ib_vector_push(index_def->cols, ib_col);
    }
  }

  return (err);
}

/** Create an index schema instance.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_schema_create(
    ib_trx_t ib_usr_trx,      /*!< in: transaction */
    const char *name,         /*!< in: index name in schema */
    const char *table_name,   /*!< in: table name */
    ib_idx_sch_t *ib_idx_sch) /*!< out: index schema instance */
{
  dict_table_t *table = NULL;
  char *normalized_name;
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (!ib_schema_lock_is_exclusive(ib_usr_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  } else if (name[0] == TEMP_INDEX_PREFIX) {
    return (DB_INVALID_INPUT);
  } else if (ib_utf8_strcasecmp(name, GEN_CLUST_INDEX) == 0) {
    return (DB_INVALID_INPUT);
  }

  normalized_name = mem_alloc(ut_strlen(table_name) + 1);
  ib_normalize_table_name(normalized_name, table_name);

  table = ib_lookup_table_by_name(normalized_name);

  mem_free(normalized_name);
  normalized_name = NULL;

  if (table == NULL) {
    err = DB_TABLE_NOT_FOUND;
  } else if (dict_table_get_index_on_name(table, name) != NULL) {
    err = DB_DUPLICATE_KEY;
  } else {
    mem_heap_t *heap = mem_heap_create(1024);

    if (heap == NULL) {
      err = DB_OUT_OF_MEMORY;
    } else {
      ib_index_def_t *index_def;

      index_def = (ib_index_def_t *)mem_heap_zalloc(heap, sizeof(*index_def));

      if (index_def == NULL) {
        err = DB_OUT_OF_MEMORY;

        mem_heap_free(heap);
      } else {
        index_def->heap = heap;

        index_def->table = table;

        index_def->name = mem_heap_strdup(heap, name);

        index_def->cols = ib_vector_create(heap, 8);

        index_def->usr_trx = (trx_t *)ib_usr_trx;

        *ib_idx_sch = (ib_idx_sch_t)index_def;
      }
    }
  }

  return (err);
}

/** Get an index definition that is tagged as a clustered index.
@return	cluster index schema */
UNIV_INLINE
ib_index_def_t *
ib_find_clustered_index(ib_vector_t *indexes) /*!< in: index defs. to search */
{
  ulint i;
  ulint n_indexes;

  UT_DBG_ENTER_FUNC;

  n_indexes = ib_vector_size(indexes);

  for (i = 0; i < n_indexes; ++i) {
    ib_index_def_t *ib_index_def;

    ib_index_def = ib_vector_get(indexes, i);

    if (ib_index_def->clustered) {
      return (ib_index_def);
    }
  }

  return (NULL);
}

/** Set index as clustered index. Implies UNIQUE.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_schema_set_clustered(
    ib_idx_sch_t ib_idx_sch) /*!< in/out: index definition */
{
  ib_index_def_t *index_def;
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  index_def = (ib_index_def_t *)ib_idx_sch;

  /* If this index schema is part of a table schema then we need
  to check the state of the other indexes. */

  if (index_def->schema != NULL) {
    ib_index_def_t *ib_clust_index_def;

    ib_clust_index_def = ib_find_clustered_index(index_def->schema->indexes);

    if (ib_clust_index_def != NULL) {
      ut_a(ib_clust_index_def->clustered);
      ib_clust_index_def->clustered = FALSE;
    }
  }

  index_def->unique = TRUE;
  index_def->clustered = TRUE;

  return (err);
}

/** Set index as a unique index.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_schema_set_unique(
    ib_idx_sch_t ib_idx_sch) /*!< in/out: index definition */
{
  ib_index_def_t *index_def;
  ib_err_t err = DB_SUCCESS;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  index_def = (ib_index_def_t *)ib_idx_sch;
  index_def->unique = TRUE;

  return (err);
}

/** Destroy an index schema. */

void ib_index_schema_delete(
    ib_idx_sch_t ib_idx_sch) /*!< in, own: index schema to delete */
{
  ib_index_def_t *index_def = (ib_index_def_t *)ib_idx_sch;

  UT_DBG_ENTER_FUNC;

  ut_a(index_def->schema == NULL);

  mem_heap_free(index_def->heap);
}

/** Convert the table definition table attributes to the internal format.
@return	flags */
static ulint ib_table_def_get_flags(
    const ib_table_def_t *table_def) /*!< in: table definition */
{
  ulint flags = 0;

  UT_DBG_ENTER_FUNC;

  switch (table_def->ib_tbl_fmt) {
  case IB_TBL_REDUNDANT: /* Old row format */
    break;
  case IB_TBL_COMPACT: /* The compact row format */
    flags = DICT_TF_COMPACT;
    break;
  case IB_TBL_DYNAMIC: /* The dynamic row format */
    /* Dynamic format implies a page size of 0. */
    flags = DICT_TF_COMPACT | DICT_TF_FORMAT_ZIP << DICT_TF_FORMAT_SHIFT;
    break;
  case IB_TBL_COMPRESSED: { /* Compact row format and
                            compressed page */
    ulint i;
    ulint j;

    for (i = j = 1; i <= DICT_TF_ZSSIZE_MAX; ++i, j <<= 1) {

      if (j == table_def->page_size) {
        flags = i << DICT_TF_ZSSIZE_SHIFT | DICT_TF_COMPACT |
                DICT_TF_FORMAT_ZIP << DICT_TF_FORMAT_SHIFT;
        break;
      }
    }
    ut_a(flags != 0);
    ut_a(i <= DICT_TF_ZSSIZE_MAX);
    break;
  }
  default:
    ut_error;
  }

  return (flags);
}

/** Copy the index definition to row0merge.c format.

@return	converted index definition */
static const index_def_t *ib_copy_index_definition(
    ib_index_def_t *ib_index_def, /*!< in: key definition for index */
    ibool clustered)              /*!< in: IB_TRUE if clustered index */
{
  ulint i;
  ib_key_col_t *ib_col;
  ulint name_len;
  index_def_t *index_def;
  char *index_name;

  UT_DBG_ENTER_FUNC;

  index_def =
      (index_def_t *)mem_heap_zalloc(ib_index_def->heap, sizeof(*index_def));

  name_len = ut_strlen(ib_index_def->name);
  index_name = mem_heap_zalloc(ib_index_def->heap, name_len + 2);

  /* The TEMP_INDEX_PREFIX is only needed if we are rebuilding an
  index or creating a new index on a table that has records. If
  the definition is owned by a table schema then we can be sure that
  this index definition is part of a CREATE TABLE. */
  if (ib_index_def->schema == NULL) {
    *index_name = TEMP_INDEX_PREFIX;
    memcpy(index_name + 1, ib_index_def->name, name_len + 1);
  } else {
    memcpy(index_name, ib_index_def->name, name_len);
  }

  index_def->name = index_name;
  index_def->n_fields = ib_vector_size(ib_index_def->cols);

  if (ib_index_def->unique) {
    index_def->ind_type = DICT_UNIQUE;
  } else {
    index_def->ind_type = 0;
  }

  if (clustered) {
    index_def->ind_type |= DICT_CLUSTERED;
  }

  index_def->fields = (index_field_t *)mem_heap_zalloc(
      ib_index_def->heap, sizeof(index_field_t) * index_def->n_fields);

  for (i = 0; i < ib_vector_size(ib_index_def->cols); ++i) {

    ib_col = ib_vector_get(ib_index_def->cols, i);

    index_def->fields[i].field_name = ib_col->name;
    index_def->fields[i].prefix_len = ib_col->prefix_len;
  }

  return (index_def);
}

/** (Re)Create a secondary index.
@return	DB_SUCCESS or err code */
static ib_err_t ib_build_secondary_index(
    trx_t *usr_trx,               /*!< in: transaction */
    dict_table_t *table,          /*!< in: parent table of index */
    ib_index_def_t *ib_index_def, /*!< in: index definition */
    ib_bool_t create,             /*!< in: TRUE if part of table create */
    dict_index_t **dict_index)    /*!< out: index created */
{
  ib_err_t err;
  trx_t *ddl_trx;
  const index_def_t *index_def;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(usr_trx->conc_state != TRX_NOT_STARTED);

  if (!create) {
    ib_bool_t started;

    ddl_trx = trx_allocate_for_client(NULL);
    started = trx_start(ddl_trx, ULINT_UNDEFINED);
    ut_a(started);
  } else {
    ddl_trx = usr_trx;
  }

  /* Set the CLUSTERED flag to FALSE. */
  index_def = ib_copy_index_definition(ib_index_def, FALSE);

  ut_a(!(index_def->ind_type & DICT_CLUSTERED));

  ddl_trx->op_info = "creating secondary index";

  if (!(create || ib_schema_lock_is_exclusive((ib_trx_t)usr_trx))) {
    err = ib_schema_lock_exclusive((ib_trx_t)usr_trx);

    if (err != DB_SUCCESS) {
      return (err);
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
    ib_schema_unlock((ib_trx_t)usr_trx);
  }

  err = ddl_trx->error_state;

  if (!create) {
    /* Commit the data dictionary transaction in order to release
    the table locks on the system tables. */
    trx_commit(ddl_trx);

    trx_free_for_client(ddl_trx);
  }

  ut_a(usr_trx->conc_state != TRX_NOT_STARTED);

  if (*dict_index != NULL) {
    ut_a(err == DB_SUCCESS);

    (*dict_index)->cmp_ctx = NULL;

    /* Read the clustered index records and build the index. */
    err = row_merge_build_indexes(usr_trx, table, table, dict_index, 1, NULL);
  }

  return (err);
}

/** Create a temporary tablename using table name and id
@return	temporary tablename */
static char *
ib_table_create_temp_name(mem_heap_t *heap, /*!< in: memory heap */
                          char id,          /*!< in: identifier [0-9a-zA-Z] */
                          const char *table_name) /*!< in: table name */
{
  ulint len;
  char *name;
  static const char suffix[] = "# ";

  len = ut_strlen(table_name);

  name = (char *)mem_heap_zalloc(heap, len + sizeof(suffix));
  ut_memcpy(name, table_name, len);
  ut_memcpy(name + len, suffix, sizeof(suffix));
  name[len + (sizeof(suffix) - 2)] = id;

  return (name);
}

/** Create an index definition from the index. */
static void ib_index_create_def(
    const dict_index_t *dict_index, /*!< in: index definition to copy */
    index_def_t *index_def,         /*!< out: index definition */
    mem_heap_t *heap)               /*!< in: heap where allocated */
{
  ulint i;
  const dict_field_t *dfield;
  ulint n_fields;

  n_fields = dict_index->n_user_defined_cols;

  index_def->fields = (merge_index_field_t *)mem_heap_zalloc(
      heap, n_fields * sizeof(*index_def->fields));

  index_def->name = dict_index->name;
  index_def->n_fields = n_fields;
  index_def->ind_type = dict_index->type & ~DICT_CLUSTERED;

  for (i = 0, dfield = dict_index->fields; i < n_fields; ++i, ++dfield) {
    merge_index_field_t *def_field;

    def_field = &index_def->fields[i];

    def_field->field_name = dfield->name;
    def_field->prefix_len = dfield->prefix_len;
  }
}

/** Create and return an array of index definitions on a table. Skip the
old clustered index if it's a generated clustered index. If there is a
user defined clustered index on the table its CLUSTERED flag will be unset.
@return	index definitions or NULL */
static index_def_t *ib_table_create_index_defs(
    trx_t *trx,                /*!< in: transaction */
    const dict_table_t *table, /*!< in: table definition */
    mem_heap_t *heap,          /*!< in: heap where space for index
                               definitions are allocated */
    ulint *n_indexes)          /*!< out: number of indexes retuned */
{
  ulint sz;
  ib_err_t err;
  const dict_index_t *dict_index;
  index_def_t *index_defs;

  UT_DBG_ENTER_FUNC;

  sz = sizeof(*index_defs) * UT_LIST_GET_LEN(table->indexes);
  index_defs = (index_def_t *)mem_heap_zalloc(heap, sz);

  err = ib_schema_lock_exclusive((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS);

  dict_index = dict_table_get_first_index(table);

  /* Skip a generated cluster index. */
  if (ib_utf8_strcasecmp(dict_index->name, GEN_CLUST_INDEX) == 0) {
    ut_a(dict_index_get_nth_col(dict_index, 0)->mtype == DATA_SYS);
    dict_index = dict_table_get_next_index(dict_index);
  }

  while (dict_index) {
    ib_index_create_def(dict_index, index_defs++, heap);
    dict_index = dict_table_get_next_index(dict_index);
  }

  ib_schema_unlock((ib_trx_t)trx);

  return (index_defs);
}

/** Create a cluster index specified by the user . The cluster index
shouldn't already exist.
@return	DB_SUCCESS or err code */
static ib_err_t ib_create_cluster_index(
    trx_t *trx,                   /*!< in: transaction */
    dict_table_t *table,          /*!< in: parent table of index */
    ib_index_def_t *ib_index_def, /*!< in: index definition */
    dict_index_t **dict_index)    /*!< out: index created */
{
  ib_err_t err;
  const index_def_t *index_def;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(!ib_vector_is_empty(ib_index_def->cols));

  /* Set the CLUSTERED flag to TRUE. */
  index_def = ib_copy_index_definition(ib_index_def, TRUE);

  trx->op_info = "creating clustered index";

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *dict_index = row_merge_create_index(trx, table, index_def);

    err = trx->error_state;
  }

  trx->op_info = "";

  return (err);
}

/** Create the secondary indexes on new table using the index definitions
from the src table.  The assumption is that a cluster index on the
new table already exists. All the indexes in the source table will
be copied with the exception of any generated clustered indexes. */
static ib_err_t
ib_table_clone_indexes(trx_t *trx,              /*!< in: transaction */
                       dict_table_t *src_table, /*!< in: table to clone from */
                       dict_table_t *new_table, /*!< in: table to clone to */
                       mem_heap_t *heap)        /*!< in: memory heap to use */
{
  ulint i;
  index_def_t *index_defs;
  ulint n_index_defs = 0;

  index_defs = ib_table_create_index_defs(trx, src_table, heap, &n_index_defs);

  ut_a(index_defs != NULL);

  for (i = 0; i < n_index_defs; ++i) {
    dict_index_t *dict_index;

    ut_a(!(index_defs[i].ind_type & DICT_CLUSTERED));
    dict_index = row_merge_create_index(trx, new_table, &index_defs[i]);

    if (dict_index == NULL) {
      return (trx->error_state);
    }
  }

  return (DB_SUCCESS);
}

/** Clone the indexes definitions from src_table to dst_table. The cluster
index is not cloned. If it was generated then it's dropped else it's
demoted to a secondary index. A new cluster index is created for the
new table. */
static ib_err_t ib_table_clone(
    trx_t *trx,                   /*!< in: transaction */
    dict_table_t *src_table,      /*!< in: table to clone from */
    dict_table_t **new_table,     /*!< out: cloned table */
    ib_index_def_t *ib_index_def, /*!< in: new cluster index definition */
    mem_heap_t *heap)             /*!< in: memory heap to use */
{
  ib_err_t err;
  const index_def_t *index_def;
  char *new_table_name;

  new_table_name = ib_table_create_temp_name(heap, '1', src_table->name);

  err = ib_schema_lock_exclusive((ib_trx_t)trx);

  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Set the CLUSTERED flag to TRUE. */
  index_def = ib_copy_index_definition(ib_index_def, TRUE);

  /* Create the new table and the cluster index. */
  *new_table = row_merge_create_temporary_table(new_table_name, index_def,
                                                src_table, trx);

  if (!new_table) {
    err = trx->error_state;
  } else {
    trx->table_id = (*new_table)->id;

    err = ib_table_clone_indexes(trx, src_table, *new_table, heap);
  }

  ib_schema_unlock((ib_trx_t)trx);

  return (err);
}

/** Copy the data from the source table to dst table. */
static ib_err_t
ib_table_copy(trx_t *trx,              /*!< in: transaction */
              dict_table_t *src_table, /*!< in: table to copy from */
              dict_table_t *dst_table, /*!< in: table to copy to */
              mem_heap_t *heap)        /*!< in: heap to use */
{
  ib_err_t err;
  dict_index_t *dict_index;
  dict_index_t **indexes;
  ulint n_indexes;

  err = ib_schema_lock_exclusive((ib_trx_t)trx);

  if (err != DB_SUCCESS) {
    return (err);
  }

  n_indexes = UT_LIST_GET_LEN(dst_table->indexes);

  indexes =
      (dict_index_t **)mem_heap_zalloc(heap, n_indexes * sizeof(dict_index));

  n_indexes = 0;

  dict_index = dict_table_get_first_index(dst_table);
  /* Copy the indexes to an array. */
  while (dict_index) {
    indexes[n_indexes++] = dict_index;
    dict_index = dict_table_get_next_index(dict_index);
  }
  ut_a(n_indexes == UT_LIST_GET_LEN(dst_table->indexes));

  ib_schema_unlock((ib_trx_t)trx);

  /* Build the actual indexes. */
  err = row_merge_build_indexes(trx, src_table, dst_table, indexes, n_indexes,
                                NULL);

  return (err);
}

/** Create a default cluster index, this usually means the user didn't
create a table with a primary key.
@return	DB_SUCCESS or err code */
static ib_err_t ib_create_default_cluster_index(
    trx_t *trx,                /*!< in: transaction */
    dict_table_t *table,       /*!< in: parent table of index */
    dict_index_t **dict_index) /*!< out: index created */
{
  ib_err_t err;
  index_def_t index_def;

  UT_DBG_ENTER_FUNC;

  index_def.name = GEN_CLUST_INDEX;
  index_def.ind_type = DICT_CLUSTERED;
  index_def.n_fields = 0;
  index_def.fields = NULL;

  trx->op_info = "creating default clustered index";

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  ut_a(ib_schema_lock_is_exclusive((ib_trx_t)trx));

  err = ib_trx_lock_table_with_retry(trx, table, LOCK_X);

  if (err == DB_SUCCESS) {
    *dict_index = row_merge_create_index(trx, table, &index_def);

    err = trx->error_state;
  }

  trx->op_info = "";

  return (err);
}

/** Create the indexes for the table. Each index is created in a separate
transaction. The caller is responsible for dropping any indexes that
exist if there is a failure.
@return	DB_SUCCESS or err code */
static ib_err_t
ib_create_indexes(trx_t *ddl_trx,       /*!< in: transaction */
                  dict_table_t *table,  /*!< in: table where index created */
                  ib_vector_t *indexes) /*!< in: index defs. to create */
{
  ulint i = 0;
  ulint n_indexes;
  ib_index_def_t *ib_index_def;
  dict_index_t *dict_index = NULL;
  ib_err_t err = DB_ERROR;
  ib_index_def_t *ib_clust_index_def = NULL;

  UT_DBG_ENTER_FUNC;

  n_indexes = ib_vector_size(indexes);

  if (n_indexes > 0) {
    ib_clust_index_def = ib_find_clustered_index(indexes);

    if (ib_clust_index_def != NULL) {
      ut_a(ib_clust_index_def->clustered);

      err = ib_create_cluster_index(ddl_trx, table, ib_clust_index_def,
                                    &dict_index);
    }
  }

  if (ib_clust_index_def == NULL) {
    err = ib_create_default_cluster_index(ddl_trx, table, &dict_index);
  }

  for (i = 0; err == DB_SUCCESS && i < n_indexes; ++i) {

    ib_index_def = ib_vector_get(indexes, i);

    ut_a(!ib_vector_is_empty(ib_index_def->cols));

    if (!ib_index_def->clustered) {
      /* Since this is part of CREATE TABLE, set the
      create flag to IB_TRUE. */
      err = ib_build_secondary_index(ddl_trx, table, ib_index_def, IB_TRUE,
                                     &dict_index);
    } else {
      /* There can be at most one cluster definition. */
      ut_a(ib_clust_index_def == ib_index_def);
    }
  }

  return (err);
}

/** Get a table id. The caller must have acquired the dictionary mutex.
@return	DB_SUCCESS if found */
static ib_err_t
ib_table_get_id_low(const char *table_name, /*!< in: table to find */
                    ib_id_t *table_id)      /*!< out: table id if found */
{
  dict_table_t *table;
  ib_err_t err = DB_TABLE_NOT_FOUND;

  UT_DBG_ENTER_FUNC;

  *table_id = 0;

  table = ib_lookup_table_by_name(table_name);

  if (table != NULL) {
    *table_id = ut_conv_dulint_to_longlong(table->id);

    err = DB_SUCCESS;
  }

  return (err);
}

/** Create a table. If the table exists in the database then this function
will return DB_TABLE_IS_BEING_USED and id will contain that tables id.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_create(ib_trx_t ib_trx, /*!< in/out: transaction */
                         const ib_tbl_sch_t ib_tbl_sch, /*!< in: table schema */
                         ib_id_t *id)                   /*!< out: table id */
{
  ulint i;
  ib_err_t err;
  mem_heap_t *heap;
  dict_table_t *table;
  ulint n_cols = 0;
  ulint n_cluster = 0;
  ib_col_t *ib_col = NULL;
  trx_t *ddl_trx = (trx_t *)ib_trx;
  const ib_table_def_t *table_def = (const ib_table_def_t *)ib_tbl_sch;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* Another thread may have created the table already when we get
  here. We need to search the data dictionary before we attempt to
  create the table. */

  if (!ib_schema_lock_is_exclusive((ib_trx_t)ddl_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  }

  err = ib_table_get_id_low(table_def->name, id);

  if (err == DB_SUCCESS) {
    return (DB_TABLE_IS_BEING_USED);
  }

  *id = 0;

  n_cols = ib_vector_size(table_def->cols);

  if (n_cols == 0) {
    return (DB_SCHEMA_ERROR);
  }

  /* Check that all index definitions are valid. */
  for (i = 0; i < ib_vector_size(table_def->indexes); ++i) {
    ib_index_def_t *ib_index_def;

    ib_index_def = ib_vector_get(table_def->indexes, i);

    /* Check that the index definition has at least one column. */
    if (ib_vector_is_empty(ib_index_def->cols)) {
      return (DB_SCHEMA_ERROR);
    }

    /* Check for duplicate cluster definitions. */
    if (ib_index_def->clustered) {
      ++n_cluster;

      if (n_cluster > 1) {
        return (DB_SCHEMA_ERROR);
      }
    }
  }

  /* Create the table prototype. */
  table = dict_mem_table_create(table_def->name, 0, n_cols,
                                ib_table_def_get_flags(table_def));

  heap = table->heap;

  /* Create the columns defined by the user. */
  for (i = 0; i < n_cols; i++) {
    ib_col = ib_vector_get(table_def->cols, i);

    dict_mem_table_add_col(table, heap, ib_col->name, ib_col_get_mtype(ib_col),
                           ib_col_get_prtype(ib_col), ib_col->len);
  }

  /* Create the table using the prototype in the data dictionary. */
  err = ddl_create_table(table, ddl_trx);

  table = NULL;

  if (err == DB_SUCCESS) {

    table = ib_lookup_table_by_name(table_def->name);
    ut_a(table != NULL);

    /* Bump up the reference count, so that another transaction
    doesn't delete it behind our back. */
    dict_table_increment_handle_count(table, TRUE);

    err = ib_create_indexes(ddl_trx, table, table_def->indexes);
  }

  /* FIXME: If ib_create_indexes() fails, it's unclear what state
  the data dictionary is in. */

  if (err == DB_SUCCESS) {
    *id = ut_dulint_get_low(table->id);
  }

  if (table != NULL) {
    ulint format_id;
    const char *format = NULL;

    /* We update the highest file format in the system
    table space, if this table has a higher file format
    setting. */

    format_id = dict_table_get_format(table);
    trx_sys_file_format_max_upgrade(&format, format_id);

    if (format != NULL && format_id > db_format.id) {

      db_format.name = trx_sys_file_format_id_to_name(format_id);

      db_format.id = trx_sys_file_format_name_to_id(format);

      ut_a(db_format.id <= DICT_TF_FORMAT_MAX);
    }

    dict_table_decrement_handle_count(table, TRUE);
  }

  return (err);
}

/** Rename a table.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_rename(ib_trx_t ib_trx,      /*!< in/out: transaction */
                         const char *old_name, /*!< in: old name*/
                         const char *new_name) /*!< in: old name*/
{
  ib_err_t err;
  char *normalized_old_name;
  char *normalized_new_name;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    err = ib_schema_lock_exclusive(ib_trx);

    if (err != DB_SUCCESS) {
      return (err);
    }
  }

  normalized_old_name = mem_alloc(ut_strlen(old_name) + 1);
  ib_normalize_table_name(normalized_old_name, old_name);

  normalized_new_name = mem_alloc(ut_strlen(new_name) + 1);
  ib_normalize_table_name(normalized_new_name, new_name);

  err = ddl_rename_table(normalized_old_name, normalized_new_name, trx);

  mem_free(normalized_old_name);
  mem_free(normalized_new_name);

  return (err);
}

/** Create a primary index. The index id encodes the table id in the high
4 bytes and the index id in the lower 4 bytes.
@return	DB_SUCCESS or err code */
static ib_err_t ib_create_primary_index(
    ib_idx_sch_t ib_idx_sch, /*!< in: key definition for index */
    ib_id_t *index_id)       /*!< out: index id */
{
  ib_err_t err;
  mem_heap_t *heap;
  dict_table_t *new_table = NULL;
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;
  trx_t *usr_trx = ib_index_def->usr_trx;
  dict_table_t *table = ib_index_def->table;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;
  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->schema == NULL);
  ut_a(ib_index_def->clustered);

  /* Recreate the cluster index and all the secondary indexes
  on a table. If there was a user defined cluster index on the
  table, it will be recreated a secondary index. The InnoDB
  generated cluster index if one exists will be dropped. */

  usr_trx->op_info = "recreating clustered index";

  heap = mem_heap_create(1024);

  /* This transaction should be the only one operating on the table. */
  ut_a(table->n_handles_opened == 1);

  trx_set_dict_operation(usr_trx, TRX_DICT_OP_TABLE);

  ut_a(!ib_vector_is_empty(ib_index_def->cols));

  /* Set the CLUSTERED flag to TRUE. */
  ib_copy_index_definition(ib_index_def, TRUE);

  err = ib_trx_lock_table_with_retry(usr_trx, table, LOCK_X);

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
    char *tmp_name;
    const char *old_name;

    /* Swap the cloned table with the original table.
    On success drop the original table. */
    old_name = table->name;
    tmp_name = ib_table_create_temp_name(heap, '2', old_name);

    err = row_merge_rename_tables(table, new_table, tmp_name, usr_trx);

    if (err != DB_SUCCESS) {
      row_merge_drop_table(usr_trx, new_table);
    }
  }

  mem_heap_free(heap);

  usr_trx->op_info = "";

  return (err);
}

/** Create a secondary index. The index id encodes the table id in the high
4 bytes and the index id in the lower 4 bytes.
@return	DB_SUCCESS or err code */
static ib_err_t ib_create_secondary_index(
    ib_idx_sch_t ib_idx_sch, /*!< in: key definition for index */
    ib_id_t *index_id)       /*!< out: index id */
{
  ib_err_t err;
  dict_index_t *dict_index = NULL;
  trx_t *ddl_trx = NULL;
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;
  trx_t *usr_trx = ib_index_def->usr_trx;
  dict_table_t *table = ib_index_def->table;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* This should only be called on index schema instances
  created outside of table schemas. */
  ut_a(ib_index_def->schema == NULL);
  ut_a(!ib_index_def->clustered);

  err = ib_trx_lock_table_with_retry(usr_trx, table, LOCK_S);

  if (err == DB_SUCCESS) {

    /* Since this is part of ALTER TABLE set the create flag
    to IB_FALSE. */
    err = ib_build_secondary_index(usr_trx, table, ib_index_def, IB_FALSE,
                                   &dict_index);

    err = ib_schema_lock_exclusive((ib_trx_t)usr_trx);
    ut_a(err == DB_SUCCESS);

    if (dict_index != NULL && err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &dict_index, 1);
      dict_index = NULL;
    } else {
      ib_bool_t started;

      ddl_trx = trx_allocate_for_client(NULL);
      started = trx_start(ddl_trx, ULINT_UNDEFINED);
      ut_a(started);
    }
  }

  ut_a(!(ddl_trx == NULL && err == DB_SUCCESS));

  /* Rename from the TEMP new index to the actual name. */
  if (dict_index != NULL && err == DB_SUCCESS) {
    err = row_merge_rename_indexes(usr_trx, table);

    if (err != DB_SUCCESS) {
      row_merge_drop_indexes(usr_trx, table, &dict_index, 1);
      dict_index = NULL;
    }
  }

  if (dict_index != NULL && err == DB_SUCCESS) {
    /* We only support 32 bit table and index ids. Because
    we need to pack the table id into the index id. */
    ut_a(ut_dulint_get_high(table->id) == 0);
    ut_a(ut_dulint_get_high(dict_index->id) == 0);

    *index_id = ut_dulint_get_low(table->id);
    *index_id <<= 32;
    *index_id |= ut_dulint_get_low(dict_index->id);

    trx_commit(ddl_trx);
  } else if (ddl_trx != NULL) {
    trx_general_rollback(ddl_trx, FALSE, NULL);
  }

  if (ddl_trx != NULL) {
    ddl_trx->op_info = "";
    trx_free_for_client(ddl_trx);
  }

  return (err);
}

/** Create a secondary index. The index id encodes the table id in the high
4 bytes and the index id in the lower 4 bytes.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_create(
    ib_idx_sch_t ib_idx_sch, /*!< in/out: key definition for index */
    ib_id_t *index_id)       /*!< out: index id */
{
  ib_err_t err;
  ib_index_def_t *ib_index_def = (ib_index_def_t *)ib_idx_sch;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive((ib_trx_t)ib_index_def->usr_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  } else if (ib_index_def->clustered) {
    err = ib_create_primary_index(ib_idx_sch, index_id);
  } else {
    err = ib_create_secondary_index(ib_idx_sch, index_id);
  }

  return (err);
}

/** Drop a table.
@return	DB_SUCCESS or err code */

ib_err_t ib_table_drop(ib_trx_t ib_trx,  /*!< in: transaction */
                       const char *name) /*!< in: table name to drop*/
{
  ib_err_t err;
  char *normalized_name;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  }

  normalized_name = mem_alloc(ut_strlen(name) + 1);
  ib_normalize_table_name(normalized_name, name);

  err = ddl_drop_table(normalized_name, (trx_t *)ib_trx, FALSE);

  mem_free(normalized_name);

  return (err);
}

/** Drop a secondary index.
@return	DB_SUCCESS or err code */

ib_err_t ib_index_drop(ib_trx_t ib_trx,  /*!< in: transaction */
                       ib_id_t index_id) /*!< in: index id to drop */
{
  ib_err_t err;
  dict_table_t *table;
  dict_index_t *dict_index;
  ulint table_id = (ulint)(index_id >> 32);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  }

  table = ib_open_table_by_id(table_id, TRUE);

  if (table == NULL) {
    return (DB_TABLE_NOT_FOUND);
  }

  /* We use only the lower 32 bits of the dulint. */
  index_id &= 0xFFFFFFFFULL;

  dict_index =
      dict_index_get_on_id_low(table, ut_dulint_create(0, (ulint)index_id));

  if (dict_index != NULL) {
    err = ddl_drop_index(table, dict_index, (trx_t *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  dict_table_decrement_handle_count(table, FALSE);

  return (err);
}

/** Create an internal cursor instance.
@return	DB_SUCCESS or err code */
static ib_err_t ib_create_cursor(ib_crsr_t *ib_crsr,  /*!< out: InnoDB cursor */
                                 dict_table_t *table, /*!< in: table instance */
                                 ib_id_t index_id,    /*!< in: index id or 0 */
                                 trx_t *trx)          /*!< in: transaction */
{
  mem_heap_t *heap;
  ib_cursor_t *cursor;
  ib_err_t err = DB_SUCCESS;
  dulint id = ut_dulint_create(0, (ulint)index_id);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  heap = mem_heap_create(sizeof(*cursor) * 2);

  if (heap != NULL) {
    row_prebuilt_t *prebuilt;

    cursor = mem_heap_zalloc(heap, sizeof(*cursor));

    cursor->heap = heap;

    cursor->query_heap = mem_heap_create(64);

    if (cursor->query_heap == NULL) {
      mem_heap_free(heap);

      return (DB_OUT_OF_MEMORY);
    }

    cursor->prebuilt = row_prebuilt_create(table);

    prebuilt = cursor->prebuilt;

    prebuilt->trx = trx;
    prebuilt->table = table;
    prebuilt->select_lock_type = LOCK_NONE;

    if (index_id > 0) {
      prebuilt->index = dict_index_get_on_id_low(table, id);
    } else {
      prebuilt->index = dict_table_get_first_index(table);
    }

    ut_a(prebuilt->index != NULL);

    if (prebuilt->trx != NULL) {
      ++prebuilt->trx->n_client_tables_in_use;

      prebuilt->index_usable =
          row_merge_is_index_usable(prebuilt->trx, prebuilt->index);

      /* Assign a read view if the transaction does
      not have it yet */

      trx_assign_read_view(prebuilt->trx);
    }

    *ib_crsr = (ib_crsr_t)cursor;
  } else {
    err = DB_OUT_OF_MEMORY;
  }

  return (err);
}

/** Open an InnoDB table and return a cursor handle to it.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_open_table_using_id(
    ib_id_t table_id,   /*!< in: table id of table to open */
    ib_trx_t ib_trx,    /*!< in: Current transaction handle
                        can be NULL */
    ib_crsr_t *ib_crsr) /*!< out,own: InnoDB cursor */
{
  ib_err_t err;
  dict_table_t *table;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (ib_trx == NULL || !ib_schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(table_id, FALSE);
  } else {
    table = ib_open_table_by_id(table_id, TRUE);
  }

  if (table == NULL) {

    return (DB_TABLE_NOT_FOUND);
  }

  err = ib_create_cursor(ib_crsr, table, 0, (trx_t *)ib_trx);

  return (err);
}

/** Open an InnoDB index and return a cursor handle to it.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_open_index_using_id(
    ib_id_t index_id,   /*!< in: index id of index to open */
    ib_trx_t ib_trx,    /*!< in: Current transaction handle
                        can be NULL */
    ib_crsr_t *ib_crsr) /*!< out: InnoDB cursor */
{
  ib_err_t err;
  dict_table_t *table;
  ulint table_id = (ulint)(index_id >> 32);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (ib_trx == NULL || !ib_schema_lock_is_exclusive(ib_trx)) {
    table = ib_open_table_by_id(table_id, FALSE);
  } else {
    table = ib_open_table_by_id(table_id, TRUE);
  }

  if (table == NULL) {

    return (DB_TABLE_NOT_FOUND);
  }

  /* We only return the lower 32 bits of the dulint. */
  err = ib_create_cursor(ib_crsr, table, index_id & 0xFFFFFFFFULL,
                         (trx_t *)ib_trx);

  if (ib_crsr != NULL) {
    const ib_cursor_t *cursor;

    cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->index == NULL) {
      ib_err_t crsr_err;

      crsr_err = ib_cursor_close(*ib_crsr);
      ut_a(crsr_err == DB_SUCCESS);

      *ib_crsr = NULL;
    }
  }

  return (err);
}

/** Open an InnoDB secondary index cursor and return a cursor handle to it.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_open_index_using_name(
    ib_crsr_t ib_open_crsr, /*!< in: open/active cursor */
    const char *index_name, /*!< in: secondary index name */
    ib_crsr_t *ib_crsr)     /*!< out,own: InnoDB index cursor */
{
  dict_table_t *table;
  dict_index_t *dict_index;
  ib_id_t index_id = 0;
  ib_err_t err = DB_TABLE_NOT_FOUND;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_open_crsr;
  trx_t *trx = cursor->prebuilt->trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (trx != NULL && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    dict_mutex_enter();
  }

  /* We want to increment the ref count, so we do a redundant search. */
  table = dict_table_get_using_id(srv_force_recovery,
                                  cursor->prebuilt->table->id, TRUE);
  ut_a(table != NULL);

  if (trx != NULL && !ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    dict_mutex_exit();
  }

  /* The first index is always the cluster index. */
  dict_index = dict_table_get_first_index(table);

  /* Traverse the user defined indexes. */
  while (dict_index != NULL) {
    if (strcmp(dict_index->name, index_name) == 0) {
      index_id = ut_conv_dulint_to_longlong(dict_index->id);
    }
    dict_index = UT_LIST_GET_NEXT(indexes, dict_index);
  }

  *ib_crsr = NULL;

  if (index_id > 0) {
    err = ib_create_cursor(ib_crsr, table, index_id, cursor->prebuilt->trx);
  }

  if (*ib_crsr != NULL) {
    const ib_cursor_t *cursor;

    cursor = *(ib_cursor_t **)ib_crsr;

    if (cursor->prebuilt->index == NULL) {
      err = ib_cursor_close(*ib_crsr);
      ut_a(err == DB_SUCCESS);
      *ib_crsr = NULL;
    }
  } else {
    dict_table_decrement_handle_count(table, TRUE);
  }

  return (err);
}

/** Open an InnoDB table and return a cursor handle to it.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_open_table(const char *name,   /*!< in: table name */
                              ib_trx_t ib_trx,    /*!< in: Current transaction
                                                  handle    can be NULL */
                              ib_crsr_t *ib_crsr) /*!< out,own: InnoDB cursor */
{
  ib_err_t err;
  dict_table_t *table;
  char *normalized_name;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  normalized_name = mem_alloc(ut_strlen(name) + 1);
  ib_normalize_table_name(normalized_name, name);

  if (ib_trx != NULL) {
    if (!ib_schema_lock_is_exclusive(ib_trx)) {
      table = ib_open_table_by_name(normalized_name);
    } else {
      table = ib_lookup_table_by_name(normalized_name);

      if (table != NULL) {
        dict_table_increment_handle_count(table, TRUE);
      }
    }
  } else {
    table = ib_open_table_by_name(normalized_name);
  }

  mem_free(normalized_name);
  normalized_name = NULL;

  /* It can happen that another thread has created the table but
  not the cluster index or it's a broken table definition. Refuse to
  open if that's the case. */
  if (table != NULL && dict_table_get_first_index(table) == NULL) {
    dict_table_decrement_handle_count(table, FALSE);
    table = NULL;
  }

  if (table != NULL) {
    err = ib_create_cursor(ib_crsr, table, 0, (trx_t *)ib_trx);
  } else {
    err = DB_TABLE_NOT_FOUND;
  }

  return (err);
}

/** Free a context struct for a table handle. */
static void
ib_qry_proc_free(ib_qry_proc_t *q_proc) /*!< in, own: qproc struct */
{
  UT_DBG_ENTER_FUNC;

  que_graph_free_recursive(q_proc->grph.ins);
  que_graph_free_recursive(q_proc->grph.upd);
  que_graph_free_recursive(q_proc->grph.sel);

  memset(q_proc, 0x0, sizeof(*q_proc));
}

/** Reset the cursor.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_reset(ib_crsr_t ib_crsr) /*!< in/out: InnoDB cursor */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (prebuilt->trx != NULL && prebuilt->trx->n_client_tables_in_use > 0) {

    --prebuilt->trx->n_client_tables_in_use;
  }

  /* The fields in this data structure are allocated from
  the query heap and so need to be reset too. */
  ib_qry_proc_free(&cursor->q_proc);

  mem_heap_empty(cursor->query_heap);

  row_prebuilt_reset(prebuilt);

  return (DB_SUCCESS);
}

/** Close the cursor.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_close(ib_crsr_t ib_crsr) /*!< in,own: InnoDB cursor */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  trx_t *trx = prebuilt->trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ib_qry_proc_free(&cursor->q_proc);

  /* The transaction could have been detached from the cursor. */
  if (trx != NULL && trx->n_client_tables_in_use > 0) {
    --trx->n_client_tables_in_use;
  }

  if (trx && ib_schema_lock_is_exclusive((ib_trx_t)trx)) {
    row_prebuilt_free(cursor->prebuilt, TRUE);
  } else {
    row_prebuilt_free(cursor->prebuilt, FALSE);
  }

  mem_heap_free(cursor->query_heap);
  mem_heap_free(cursor->heap);

  return (DB_SUCCESS);
}

/** Run the insert query and do error handling.
@return	DB_SUCCESS or error code */
UNIV_INLINE
ib_err_t ib_insert_row_with_lock_retry(
    que_thr_t *thr,       /*!< in: insert query graph */
    ins_node_t *node,     /*!< in: insert node for the query */
    trx_savept_t *savept) /*!< in: savepoint to rollback to
                          in case of an error */
{
  trx_t *trx;
  ib_err_t err;
  ib_bool_t lock_wait;

  trx = thr_get_trx(thr);

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
      lock_wait = FALSE;
    }
  } while (lock_wait);

  return (err);
}

/** Write a row.
@return	DB_SUCCESS or err code */
static ib_err_t ib_execute_insert_query_graph(
    dict_table_t *table,   /*!< in: table where to insert */
    que_fork_t *ins_graph, /*!< in: query graph */
    ins_node_t *node)      /*!< in: insert node */
{
  trx_t *trx;
  que_thr_t *thr;
  trx_savept_t savept;
  ib_err_t err = DB_SUCCESS;

  UT_DBG_ENTER_FUNC;

  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  trx = ins_graph->trx;

  savept = trx_savept_take(trx);

  thr = que_fork_get_first_thr(ins_graph);

  que_thr_move_to_run_state(thr);

  err = ib_insert_row_with_lock_retry(thr, node, &savept);

  if (err == DB_SUCCESS) {
    que_thr_stop_for_client_no_error(thr, trx);

    table->stat_n_rows++;

    srv_n_rows_inserted++;

    ib_update_statistics_if_needed(table);

    ib_wake_master_thread();
  }

  trx->op_info = "";

  return (err);
}

/** Create an insert query graph node. */
static void
ib_insert_query_graph_create(ib_cursor_t *cursor) /*!< in: Cursor instance */
{
  ib_qry_proc_t *q_proc = &cursor->q_proc;
  ib_qry_node_t *node = &q_proc->node;
  trx_t *trx = cursor->prebuilt->trx;

  UT_DBG_ENTER_FUNC;

  ut_a(trx->conc_state != TRX_NOT_STARTED);

  if (node->ins == NULL) {
    dtuple_t *row;
    ib_qry_grph_t *grph = &q_proc->grph;
    mem_heap_t *heap = cursor->query_heap;
    dict_table_t *table = cursor->prebuilt->table;

    node->ins = row_ins_node_create(INS_DIRECT, table, heap);

    node->ins->select = NULL;
    node->ins->values_list = NULL;

    row = dtuple_create(heap, dict_table_get_n_cols(table));
    dict_table_copy_types(row, table);

    row_ins_node_set_new_row(node->ins, row);

    grph->ins =
        que_node_get_parent(pars_complete_graph_for_exec(node->ins, trx, heap));

    grph->ins->state = QUE_FORK_ACTIVE;
  }
}

/** Insert a row to a table.
@return	DB_SUCCESS or err code */

ib_err_t
ib_cursor_insert_row(ib_crsr_t ib_crsr, /*!< in/out: InnoDB cursor instance */
                     const ib_tpl_t ib_tpl) /*!< in: tuple to insert */
{
  ib_ulint_t i;
  ib_qry_node_t *node;
  ib_qry_proc_t *q_proc;
  ulint n_fields;
  dtuple_t *dst_dtuple;
  ib_err_t err = DB_SUCCESS;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  const ib_tuple_t *src_tuple = (const ib_tuple_t *)ib_tpl;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ib_insert_query_graph_create(cursor);

  ut_ad(src_tuple->type == TPL_ROW);

  q_proc = &cursor->q_proc;
  node = &q_proc->node;

  node->ins->state = INS_NODE_ALLOC_ROW_ID;
  dst_dtuple = node->ins->row;

  n_fields = dtuple_get_n_fields(src_tuple->ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_dtuple));

  /* Do a shallow copy of the data fields and check for NULL
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
    err = ib_execute_insert_query_graph(src_tuple->index->table,
                                        q_proc->grph.ins, node->ins);
  }

  return (err);
}

/** Gets pointer to a prebuilt update vector used in updates.
@return	update vector */
UNIV_INLINE
upd_t *ib_update_vector_create(ib_cursor_t *cursor) /*!< in: current cursor */
{
  trx_t *trx = cursor->prebuilt->trx;
  mem_heap_t *heap = cursor->query_heap;
  dict_table_t *table = cursor->prebuilt->table;
  ib_qry_proc_t *q_proc = &cursor->q_proc;
  ib_qry_grph_t *grph = &q_proc->grph;
  ib_qry_node_t *node = &q_proc->node;

  UT_DBG_ENTER_FUNC;

  ut_a(trx->conc_state != TRX_NOT_STARTED);

  if (node->upd == NULL) {
    node->upd = row_create_update_node(table, heap);
  }

  grph->upd =
      que_node_get_parent(pars_complete_graph_for_exec(node->upd, trx, heap));

  grph->upd->state = QUE_FORK_ACTIVE;

  return (node->upd->update);
}

/** Note that a column has changed. */
static void ib_update_col(

    ib_cursor_t *cursor,    /*!< in: current cursor */
    upd_field_t *upd_field, /*!< in/out: update field */
    ulint col_no,           /*!< in: column number */
    dfield_t *dfield)       /*!< in: updated dfield */
{
  ulint data_len;
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *dict_index = dict_table_get_first_index(table);

  UT_DBG_ENTER_FUNC;

  data_len = dfield_get_len(dfield);

  if (data_len == UNIV_SQL_NULL) {
    dfield_set_null(&upd_field->new_val);
  } else {
    dfield_copy_data(&upd_field->new_val, dfield);
  }

  upd_field->exp = NULL;

  upd_field->orig_len = 0;

  upd_field->field_no =
      dict_col_get_clust_pos(&table->cols[col_no], dict_index);
}

/** Checks which fields have changed in a row and stores the new data
to an update vector.
@return	DB_SUCCESS or err code */
static ib_err_t
ib_calc_diff(ib_cursor_t *cursor,         /*!< in: current cursor */
             upd_t *upd,                  /*!< in/out: update vector */
             const ib_tuple_t *old_tuple, /*!< in: Old tuple in table */
             const ib_tuple_t *new_tuple) /*!< in: New tuple to update */
{
  ulint i;
  ulint n_changed = 0;
  ib_err_t err = DB_SUCCESS;
  ulint n_fields = dtuple_get_n_fields(new_tuple->ptr);

  UT_DBG_ENTER_FUNC;

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);
  ut_a(old_tuple->index->table == new_tuple->index->table);

  for (i = 0; i < n_fields; ++i) {
    ulint mtype;
    ulint prtype;
    upd_field_t *upd_field;
    dfield_t *new_dfield;
    dfield_t *old_dfield;

    new_dfield = dtuple_get_nth_field(new_tuple->ptr, i);
    old_dfield = dtuple_get_nth_field(old_tuple->ptr, i);

    mtype = dtype_get_mtype(dfield_get_type(old_dfield));
    prtype = dtype_get_prtype(dfield_get_type(old_dfield));

    /* Skip the system columns */
    if (mtype == DATA_SYS) {
      continue;

    } else if ((prtype & DATA_NOT_NULL) && dfield_is_null(new_dfield)) {

      err = DB_DATA_MISMATCH;
      break;
    }

    if (dfield_get_len(new_dfield) != dfield_get_len(old_dfield) ||
        (!dfield_is_null(old_dfield) &&
         memcmp(dfield_get_data(new_dfield), dfield_get_data(old_dfield),
                dfield_get_len(old_dfield)) != 0)) {

      upd_field = &upd->fields[n_changed];

      ib_update_col(cursor, upd_field, i, new_dfield);

      ++n_changed;
    }
  }

  if (err == DB_SUCCESS) {
    upd->info_bits = 0;
    upd->n_fields = n_changed;
  }

  return (err);
}

/** Run the update query and do error handling.
@return	DB_SUCCESS or error code */
UNIV_INLINE
ib_err_t ib_update_row_with_lock_retry(
    que_thr_t *thr,       /*!< in: Update query graph */
    upd_node_t *node,     /*!< in: Update node for the query */
    trx_savept_t *savept) /*!< in: savepoint to rollback to
                          in case of an error */

{
  trx_t *trx;
  ib_err_t err;
  ib_bool_t lock_wait;

  trx = thr_get_trx(thr);

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
        lock_wait = FALSE;
      }
    } else {
      lock_wait = FALSE;
    }
  } while (lock_wait);

  return (err);
}

/** Does an update or delete of a row.
@return	DB_SUCCESS or err code */
UNIV_INLINE
ib_err_t ib_execute_update_query_graph(
    ib_cursor_t *cursor, /*!< in: Cursor instance */
    btr_pcur_t *pcur)    /*!< in: Btree persistent cursor */
{
  ib_err_t err;
  que_thr_t *thr;
  upd_node_t *node;
  trx_savept_t savept;
  trx_t *trx = cursor->prebuilt->trx;
  dict_table_t *table = cursor->prebuilt->table;
  ib_qry_proc_t *q_proc = &cursor->q_proc;

  UT_DBG_ENTER_FUNC;

  /* The transaction must be running. */
  ut_a(trx->conc_state != TRX_NOT_STARTED);

  node = q_proc->node.upd;

  /* This is a short term solution to fix the purge lag. */
  ib_delay_dml_if_needed();

  ut_a(dict_index_is_clust(pcur->btr_cur.index));
  btr_pcur_copy_stored_position(node->pcur, pcur);

  ut_a(node->pcur->rel_pos == BTR_PCUR_ON);

  savept = trx_savept_take(trx);

  thr = que_fork_get_first_thr(q_proc->grph.upd);

  node->state = UPD_NODE_UPDATE_CLUSTERED;

  que_thr_move_to_run_state(thr);

  err = ib_update_row_with_lock_retry(thr, node, &savept);

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

  trx->op_info = "";

  return (err);
}

/** Update a row in a table.
@return	DB_SUCCESS or err code */

ib_err_t
ib_cursor_update_row(ib_crsr_t ib_crsr, /*!< in: InnoDB cursor instance */
                     const ib_tpl_t ib_old_tpl, /*!< in: Old tuple in table */
                     const ib_tpl_t ib_new_tpl) /*!< in: New tuple to update */
{
  upd_t *upd;
  ib_err_t err;
  btr_pcur_t *pcur;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  const ib_tuple_t *old_tuple = (const ib_tuple_t *)ib_old_tpl;
  const ib_tuple_t *new_tuple = (const ib_tuple_t *)ib_new_tpl;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (dict_index_is_clust(prebuilt->index)) {
    pcur = cursor->prebuilt->pcur;
  } else if (prebuilt->need_to_access_clustered &&
             cursor->prebuilt->clust_pcur != NULL) {
    pcur = cursor->prebuilt->clust_pcur;
  } else {
    return (DB_ERROR);
  }

  ut_a(old_tuple->type == TPL_ROW);
  ut_a(new_tuple->type == TPL_ROW);

  upd = ib_update_vector_create(cursor);

  err = ib_calc_diff(cursor, upd, old_tuple, new_tuple);

  if (err == DB_SUCCESS) {
    /* Note that this is not a delete. */
    cursor->q_proc.node.upd->is_delete = FALSE;

    err = ib_execute_update_query_graph(cursor, pcur);
  }

  return (err);
}

/** Build the update query graph to delete a row from an index.
@return	DB_SUCCESS or err code */
static ib_err_t
ib_delete_row(ib_cursor_t *cursor, /*!< in: current cursor */
              btr_pcur_t *pcur,    /*!< in: Btree persistent cursor */
              const rec_t *rec)    /*!< in: record to delete */
{
  ulint i;
  upd_t *upd;
  ib_err_t err;
  ib_tuple_t *tuple;
  ib_tpl_t ib_tpl;
  ulint n_cols;
  upd_field_t *upd_field;
  ib_bool_t page_format;
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *dict_index = dict_table_get_first_index(table);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  n_cols = dict_index_get_n_ordering_defined_by_user(dict_index);
  ib_tpl = ib_key_tuple_new(dict_index, n_cols);

  if (!ib_tpl) {
    return (DB_OUT_OF_MEMORY);
  }

  tuple = (ib_tuple_t *)ib_tpl;

  upd = ib_update_vector_create(cursor);

  page_format = dict_table_is_comp(dict_index->table);
  ib_read_tuple(rec, page_format, tuple);

  upd->n_fields = ib_tuple_get_n_cols(ib_tpl);

  for (i = 0; i < upd->n_fields; ++i) {
    dfield_t *dfield;

    upd_field = &upd->fields[i];
    dfield = dtuple_get_nth_field(tuple->ptr, i);

    dfield_copy_data(&upd_field->new_val, dfield);

    upd_field->exp = NULL;

    upd_field->orig_len = 0;

    upd->info_bits = 0;

    upd_field->field_no = dict_col_get_clust_pos(&table->cols[i], dict_index);
  }

  /* Note that this is a delete. */
  cursor->q_proc.node.upd->is_delete = TRUE;

  err = ib_execute_update_query_graph(cursor, pcur);

  ib_tuple_delete(ib_tpl);

  return (err);
}

/** Delete a row in a table.
@return	DB_SUCCESS or err code */

ib_err_t
ib_cursor_delete_row(ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  ib_err_t err;
  btr_pcur_t *pcur;
  dict_index_t *dict_index;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  dict_index = dict_table_get_first_index(prebuilt->index->table);

  /* Check whether this is a secondary index cursor */
  if (dict_index != prebuilt->index) {
    if (prebuilt->need_to_access_clustered) {
      pcur = prebuilt->clust_pcur;
    } else {
      return (DB_ERROR);
    }
  } else {
    pcur = prebuilt->pcur;
  }

  if (ib_btr_cursor_is_positioned(pcur)) {
    const rec_t *rec;
    ib_bool_t page_format;

    page_format = dict_table_is_comp(dict_index->table);

    if (!row_sel_row_cache_is_empty(prebuilt)) {
      rec = row_sel_row_cache_get(prebuilt);
      ut_a(rec != NULL);
    } else {
      mtr_t mtr;

      mtr_start(&mtr);

      if (btr_pcur_restore_position(BTR_SEARCH_LEAF, pcur, &mtr)) {

        rec = btr_pcur_get_rec(pcur);
      } else {
        rec = NULL;
      }

      mtr_commit(&mtr);
    }

    if (rec && !rec_get_deleted_flag(rec, page_format)) {
      err = ib_delete_row(cursor, pcur, rec);
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    err = DB_RECORD_NOT_FOUND;
  }

  return (err);
}

/** Read current row.
@return	DB_SUCCESS or err code */

ib_err_t
ib_cursor_read_row(ib_crsr_t ib_crsr, /*!< in: InnoDB cursor instance */
                   ib_tpl_t ib_tpl)   /*!< out: read cols into this tuple */
{
  ib_err_t err;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(cursor->prebuilt->trx->conc_state != TRX_NOT_STARTED);

  /* When searching with IB_EXACT_MATCH set, row_search_for_client()
  will not position the persistent cursor but will copy the record
  found into the row cache. It should be the only entry. */
  if (!ib_cursor_is_positioned(ib_crsr) &&
      row_sel_row_cache_is_empty(cursor->prebuilt)) {
    err = DB_RECORD_NOT_FOUND;
  } else if (!row_sel_row_cache_is_empty(cursor->prebuilt)) {
    const rec_t *rec;
    ib_bool_t page_format;

    page_format = dict_table_is_comp(tuple->index->table);

    rec = row_sel_row_cache_get(cursor->prebuilt);
    ut_a(rec != NULL);

    if (!rec_get_deleted_flag(rec, page_format)) {
      ib_read_tuple(rec, page_format, tuple);
      err = DB_SUCCESS;
    } else {
      err = DB_RECORD_NOT_FOUND;
    }
  } else {
    mtr_t mtr;
    btr_pcur_t *pcur;
    row_prebuilt_t *prebuilt = cursor->prebuilt;

    if (prebuilt->need_to_access_clustered && tuple->type == TPL_ROW) {
      pcur = prebuilt->clust_pcur;
    } else {
      pcur = prebuilt->pcur;
    }

    if (pcur == NULL) {
      return (DB_ERROR);
    }

    mtr_start(&mtr);

    if (btr_pcur_restore_position(BTR_SEARCH_LEAF, pcur, &mtr)) {
      const rec_t *rec;
      ib_bool_t page_format;

      page_format = dict_table_is_comp(tuple->index->table);
      rec = btr_pcur_get_rec(pcur);

      if (!rec_get_deleted_flag(rec, page_format)) {
        ib_read_tuple(rec, page_format, tuple);
        err = DB_SUCCESS;
      } else {
        err = DB_RECORD_NOT_FOUND;
      }
    } else {
      err = DB_RECORD_NOT_FOUND;
    }

    mtr_commit(&mtr);
  }

  return (err);
}

/** Move cursor to the prev user record in the table.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_prev(ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  ib_err_t err;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  row_sel_row_cache_next(prebuilt);

  err = row_search_for_client(srv_force_recovery, IB_CUR_L, prebuilt,
                              ROW_SEL_DEFAULT, ROW_SEL_PREV);

  return (err);
}

/** Move cursor to the next user record in the table.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_next(ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  ib_err_t err;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* We want to move to the next record */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  row_sel_row_cache_next(prebuilt);

  err = row_search_for_client(srv_force_recovery, IB_CUR_G, prebuilt,
                              ROW_SEL_DEFAULT, ROW_SEL_NEXT);

  return (err);
}

/** Move cursor to the first record in the table.
@return	DB_SUCCESS or err code */
UNIV_INLINE
ib_err_t
ib_cursor_position(ib_cursor_t *cursor, /*!< in: InnoDB cursor instance */
                   ib_srch_mode_t mode) /*!< in: Search mode */
{
  ib_err_t err;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  /* We want to position at one of the ends, row_search_for_client()
  uses the search_tuple fields to work out what to do. */
  dtuple_set_n_fields(prebuilt->search_tuple, 0);

  err = row_search_for_client(srv_force_recovery, mode, prebuilt,
                              ROW_SEL_DEFAULT, ROW_SEL_MOVETO);

  return (err);
}

/** Move cursor to the first record in the table.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_first(ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  return (ib_cursor_position(cursor, IB_CUR_G));
}

/** Move cursor to the last record in the table.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_last(ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  IB_CHECK_PANIC();

  return (ib_cursor_position(cursor, IB_CUR_L));
}

/** Search for key.
@return	DB_SUCCESS or err code */

ib_err_t ib_cursor_moveto(ib_crsr_t ib_crsr, /*!< in: InnoDB cursor instance */
                          ib_tpl_t ib_tpl,   /*!< in: Key to search for */
                          ib_srch_mode_t ib_srch_mode, /*!< in: search mode */
                          int *result) /*!< out: -1, 0 or 1 depending on
                                       tuple eq or gt than current row */
{
  ulint i;
  ulint n_fields;
  ib_err_t err = DB_SUCCESS;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  dtuple_t *search_tuple = prebuilt->search_tuple;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(tuple->type == TPL_KEY);

  n_fields = dict_index_get_n_ordering_defined_by_user(prebuilt->index);

  dtuple_set_n_fields(search_tuple, n_fields);
  dtuple_set_n_fields_cmp(search_tuple, n_fields);

  /* Do a shallow copy */
  for (i = 0; i < n_fields; ++i) {
    dfield_copy(dtuple_get_nth_field(search_tuple, i),
                dtuple_get_nth_field(tuple->ptr, i));
  }

  ut_a(prebuilt->select_lock_type <= LOCK_NUM);

  err = row_search_for_client(srv_force_recovery, ib_srch_mode, prebuilt,
                              (ib_match_t)cursor->match_mode, ROW_SEL_MOVETO);

  *result = prebuilt->result;

  return (err);
}

/** Attach the cursor to the transaction. */

void ib_cursor_attach_trx(ib_crsr_t ib_crsr, /*!< in: cursor instance */
                          ib_trx_t ib_trx)   /*!< in: transaction */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  UT_DBG_ENTER_FUNC;

  ut_a(ib_trx != NULL);
  ut_a(prebuilt->trx == NULL);

  row_prebuilt_reset(prebuilt);
  row_prebuilt_update_trx(prebuilt, (trx_t *)ib_trx);

  /* Assign a read view if the transaction does not have it yet */
  trx_assign_read_view(prebuilt->trx);

  ut_a(prebuilt->trx->conc_state != TRX_NOT_STARTED);

  ++prebuilt->trx->n_client_tables_in_use;
}

/** Set the client comparison function for BLOBs and client types. */

void ib_set_client_compare(
    ib_client_cmp_t client_cmp_func) /*!< in: client col. compare callback */
{
  UT_DBG_ENTER_FUNC;

  ib_client_compare = client_cmp_func;
}

/** Set the cursor search mode. */

void ib_cursor_set_match_mode(
    ib_crsr_t ib_crsr,          /*!< in: Cursor instance */
    ib_match_mode_t match_mode) /*!< in: ib_cursor_moveto match mode */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  UT_DBG_ENTER_FUNC;

  cursor->match_mode = match_mode;
}

/** Get the dfield instance for the column in the tuple.
@return	dfield instance in tuple */
UNIV_INLINE
dfield_t *ib_col_get_dfield(ib_tuple_t *tuple, /*!< in: tuple instance */
                            ulint col_no)      /*!< in: col no. in tuple */
{
  dfield_t *dfield;

  UT_DBG_ENTER_FUNC;

  dfield = dtuple_get_nth_field(tuple->ptr, col_no);

  return (dfield);
}

/** Predicate to check whether a column type contains variable length data.
@return	DB_SUCCESS or error code */
UNIV_INLINE
ib_err_t ib_col_is_capped(const dtype_t *dtype) /* in: column type */
{
  return ((dtype_get_mtype(dtype) == DATA_VARCHAR ||
           dtype_get_mtype(dtype) == DATA_CHAR ||
           dtype_get_mtype(dtype) == DATA_CLIENT ||
           dtype_get_mtype(dtype) == DATA_VARCLIENT ||
           dtype_get_mtype(dtype) == DATA_FIXBINARY ||
           dtype_get_mtype(dtype) == DATA_BINARY) &&
          dtype_get_len(dtype) > 0);
}

/** Set a column of the tuple. Make a copy using the tuple's heap.
@return	DB_SUCCESS or error code */

ib_err_t ib_col_set_value(ib_tpl_t ib_tpl,   /*!< in: tuple instance */
                          ib_ulint_t col_no, /*!< in: column index in tuple */
                          const void *src,   /*!< in: data value */
                          ib_ulint_t len)    /*!< in: data value len */
{
  const dtype_t *dtype;
  dfield_t *dfield;
  void *dst = NULL;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

#ifdef UNIV_DEBUG
  mem_heap_verify(tuple->heap);
#endif

  dfield = ib_col_get_dfield(tuple, col_no);

  /* User wants to set the column to NULL. */
  if (len == IB_SQL_NULL) {
    dfield_set_null(dfield);
    return (DB_SUCCESS);
  }

  dtype = dfield_get_type(dfield);

  /* Not allowed to update system columns. */
  if (dtype_get_mtype(dtype) == DATA_SYS) {
    return (DB_DATA_MISMATCH);
  }

  dst = dfield_get_data(dfield);

  /* Since TEXT/CLOB also map to DATA_VARCHAR we need to make an
  exception. Perhaps we need to set the precise type and check
  for that. */
  if (ib_col_is_capped(dtype)) {

    len = ut_min(len, dtype_get_len(dtype));

    if (dst == NULL) {
      dst = mem_heap_alloc(tuple->heap, dtype_get_len(dtype));
      ut_a(dst != NULL);
    }
  } else if (dst == NULL || len > dfield_get_len(dfield)) {
    dst = mem_heap_alloc(tuple->heap, len);
  }

  if (dst == NULL) {
    return (DB_OUT_OF_MEMORY);
  }

  switch (dtype_get_mtype(dtype)) {
  case DATA_INT: {

    if (dtype_get_len(dtype) == len) {
      ibool usign;

      usign = dtype_get_prtype(dtype) & DATA_UNSIGNED;
      mach_write_int_type(dst, src, len, usign);
    } else {
      return (DB_DATA_MISMATCH);
    }
    break;
  }

  case DATA_FLOAT:
    if (len == sizeof(float)) {
      mach_float_ptr_write(dst, src);
    } else {
      return (DB_DATA_MISMATCH);
    }
    break;

  case DATA_DOUBLE:
    if (len == sizeof(double)) {
      mach_double_ptr_write(dst, src);
    } else {
      return (DB_DATA_MISMATCH);
    }
    break;

  case DATA_SYS:
    ut_error;
    break;

  case DATA_CHAR: {
    ulint pad_char = ULINT_UNDEFINED;

    pad_char =
        dtype_get_pad_char(dtype_get_mtype(dtype), dtype_get_prtype(dtype));

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

  return (DB_SUCCESS);
}

/** Get the size of the data available in a column of the tuple.
@return	bytes avail or IB_SQL_NULL */

ib_ulint_t ib_col_get_len(ib_tpl_t ib_tpl, /*!< in: tuple instance */
                          ib_ulint_t i)    /*!< in: column index in tuple */
{
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, i);

  data_len = dfield_get_len(dfield);

  return (data_len == UNIV_SQL_NULL ? IB_SQL_NULL : data_len);
}

/** Copy a column value from the tuple.
@return	bytes copied or IB_SQL_NULL */
UNIV_INLINE
ib_ulint_t
ib_col_copy_value_low(ib_tpl_t ib_tpl, /*!< in: tuple instance */
                      ib_ulint_t i,    /*!< in: column index in tuple */
                      void *dst,       /*!< out: copied data value */
                      ib_ulint_t len)  /*!< in: max data value len to copy */
{
  const void *data;
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, i);

  data = dfield_get_data(dfield);
  data_len = dfield_get_len(dfield);

  if (data_len != UNIV_SQL_NULL) {

    const dtype_t *dtype = dfield_get_type(dfield);

    switch (dtype_get_mtype(dfield_get_type(dfield))) {
    case DATA_INT: {
      ibool usign;

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

  return (data_len);
}

/** Copy a column value from the tuple.
@return	bytes copied or IB_SQL_NULL */

ib_ulint_t
ib_col_copy_value(ib_tpl_t ib_tpl, /*!< in: tuple instance */
                  ib_ulint_t i,    /*!< in: column index in tuple */
                  void *dst,       /*!< out: copied data value */
                  ib_ulint_t len)  /*!< in: max data value len to copy */
{
  return (ib_col_copy_value_low(ib_tpl, i, dst, len));
}

/** Get the InnoDB column attribute from the internal column precise type.
@return	precise type in api format */
UNIV_INLINE
ib_col_attr_t ib_col_get_attr(ulint prtype) /*!< in: column definition */
{
  ib_col_attr_t attr = IB_COL_NONE;

  UT_DBG_ENTER_FUNC;

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

  return (attr);
}

/** Get a column type, length and attributes from the tuple.
@return	len of column data */
UNIV_INLINE
ib_ulint_t
ib_col_get_meta_low(ib_tpl_t ib_tpl, /*!< in: tuple instance */
                    ib_ulint_t i,    /*!< in: column index in tuple */
                    ib_col_meta_t *ib_col_meta) /*!< out: column meta data */
{
  ib_u16_t prtype;
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, i);

  data_len = dfield_get_len(dfield);

  /* We assume 1-1 mapping between the ENUM and internal type codes. */
  ib_col_meta->type = dtype_get_mtype(dfield_get_type(dfield));

  ib_col_meta->type_len = dtype_get_len(dfield_get_type(dfield));

  prtype = (ib_u16_t)dtype_get_prtype(dfield_get_type(dfield));

  ib_col_meta->attr = ib_col_get_attr(prtype);
  ib_col_meta->client_type = prtype & DATA_CLIENT_TYPE_MASK;

  return (data_len);
}

/** Read a signed int 8 bit column from an InnoDB tuple. */
UNIV_INLINE
ib_err_t ib_tuple_check_int(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                            ib_ulint_t i,    /*!< in: column number */
                            ib_bool_t usign, /*!< in: true if unsigned */
                            ulint size)      /*!< in: size of integer */
{
  ib_col_meta_t ib_col_meta;

  ib_col_get_meta_low(ib_tpl, i, &ib_col_meta);

  if (ib_col_meta.type != IB_INT) {
    return (DB_DATA_MISMATCH);
  } else if (ib_col_meta.type_len == IB_SQL_NULL) {
    return (DB_UNDERFLOW);
  } else if (ib_col_meta.type_len != size) {
    return (DB_DATA_MISMATCH);
  } else if ((ib_col_meta.attr & IB_COL_UNSIGNED) && !usign) {
    return (DB_DATA_MISMATCH);
  }

  return (DB_SUCCESS);
}

/** Read a signed int 8 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_i8(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                          ib_ulint_t i,    /*!< in: column number */
                          ib_i8_t *ival)   /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, IB_FALSE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read an unsigned int 8 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_u8(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                          ib_ulint_t i,    /*!< in: column number */
                          ib_u8_t *ival)   /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, IB_TRUE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read a signed int 16 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_i16(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_i16_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, FALSE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read an unsigned int 16 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_u16(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_u16_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, IB_TRUE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read a signed int 32 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_i32(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_i32_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, FALSE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read an unsigned int 32 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_u32(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_u32_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, IB_TRUE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read a signed int 64 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_i64(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_i64_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, FALSE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Read an unsigned int 64 bit column from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_u64(ib_tpl_t ib_tpl, /*!< in: InnoDB tuple */
                           ib_ulint_t i,    /*!< in: column number */
                           ib_u64_t *ival)  /*!< out: integer value */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  err = ib_tuple_check_int(ib_tpl, i, IB_TRUE, sizeof(*ival));

  if (err == DB_SUCCESS) {
    ib_col_copy_value_low(ib_tpl, i, ival, sizeof(*ival));
  }

  return (err);
}

/** Get a column value pointer from the tuple.
@return	NULL or pointer to buffer */

const void *ib_col_get_value(ib_tpl_t ib_tpl, /*!< in: tuple instance */
                             ib_ulint_t i)    /*!< in: column index in tuple */
{
  const void *data;
  const dfield_t *dfield;
  ulint data_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, i);

  data = dfield_get_data(dfield);
  data_len = dfield_get_len(dfield);

  return (data_len != UNIV_SQL_NULL ? data : NULL);
}

/** Get a column type, length and attributes from the tuple.
@return	len of column data */

ib_ulint_t
ib_col_get_meta(ib_tpl_t ib_tpl,            /*!< in: tuple instance */
                ib_ulint_t i,               /*!< in: column index in tuple */
                ib_col_meta_t *ib_col_meta) /*!< out: column meta data */
{
  return (ib_col_get_meta_low(ib_tpl, i, ib_col_meta));
}

/** "Clear" or reset an InnoDB tuple. We free the heap and recreate the tuple.
@return	new tuple, or NULL */

ib_tpl_t ib_tuple_clear(ib_tpl_t ib_tpl) /*!< in,own: tuple (will be freed) */
{
  const dict_index_t *dict_index;
  ulint n_cols;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;
  ib_tuple_type_t type = tuple->type;
  mem_heap_t *heap = tuple->heap;

  UT_DBG_ENTER_FUNC;

  dict_index = tuple->index;
  n_cols = dtuple_get_n_fields(tuple->ptr);

  mem_heap_empty(heap);

  if (type == TPL_ROW) {
    return (ib_row_tuple_new_low(dict_index, n_cols, heap));
  } else {
    return (ib_key_tuple_new_low(dict_index, n_cols, heap));
  }
}

/** Create a new cluster key search tuple and copy the contents of  the
secondary index key tuple columns that refer to the cluster index record
to the cluster key. It does a deep copy of the column data.
@return	DB_SUCCESS or error code */

ib_err_t ib_tuple_get_cluster_key(
    ib_crsr_t ib_crsr,         /*!< in: secondary index cursor */
    ib_tpl_t *ib_dst_tpl,      /*!< out,own: destination tuple */
    const ib_tpl_t ib_src_tpl) /*!< in: source tuple */
{
  ulint i;
  ulint n_fields;
  ib_err_t err = DB_SUCCESS;
  ib_tuple_t *dst_tuple = NULL;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  ib_tuple_t *src_tuple = (ib_tuple_t *)ib_src_tpl;
  dict_index_t *clust_index;

  IB_CHECK_PANIC();

  clust_index = dict_table_get_first_index(cursor->prebuilt->table);

  /* We need to ensure that the src tuple belongs to the same table
  as the open cursor and that it's not a tuple for a cluster index. */
  if (src_tuple->type != TPL_KEY) {
    return (DB_ERROR);
  } else if (src_tuple->index->table != cursor->prebuilt->table) {
    return (DB_DATA_MISMATCH);
  } else if (src_tuple->index == clust_index) {
    return (DB_ERROR);
  }

  /* Create the cluster index key search tuple. */
  *ib_dst_tpl = ib_clust_search_tuple_create(ib_crsr);

  if (!*ib_dst_tpl) {
    return (DB_OUT_OF_MEMORY);
  }

  dst_tuple = (ib_tuple_t *)*ib_dst_tpl;
  ut_a(dst_tuple->index == clust_index);

  n_fields = dict_index_get_n_unique(dst_tuple->index);

  /* Do a deep copy of the data fields. */
  for (i = 0; i < n_fields; i++) {
    ulint pos;
    dfield_t *src_field;
    dfield_t *dst_field;

    pos = dict_index_get_nth_field_pos(src_tuple->index, dst_tuple->index, i);

    ut_a(pos != ULINT_UNDEFINED);

    src_field = dtuple_get_nth_field(src_tuple->ptr, pos);
    dst_field = dtuple_get_nth_field(dst_tuple->ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data =
          mem_heap_dup(dst_tuple->heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return (err);
}

/** Copy the contents of  source tuple to destination tuple. The tuples
must be of the same type and belong to the same table/index.
@return	DB_SUCCESS or error code */

ib_err_t ib_tuple_copy(ib_tpl_t ib_dst_tpl,       /*!< in: destination tuple */
                       const ib_tpl_t ib_src_tpl) /*!< in: source tuple */
{
  ulint i;
  ulint n_fields;
  ib_err_t err = DB_SUCCESS;
  const ib_tuple_t *src_tuple = (const ib_tuple_t *)ib_src_tpl;
  ib_tuple_t *dst_tuple = (ib_tuple_t *)ib_dst_tpl;

  IB_CHECK_PANIC();

  /* Make sure src and dst are not the same. */
  ut_a(src_tuple != dst_tuple);

  /* Make sure they are the same type and refer to the same index. */
  if (src_tuple->type != dst_tuple->type ||
      src_tuple->index != dst_tuple->index) {

    return (DB_DATA_MISMATCH);
  }

  n_fields = dtuple_get_n_fields(src_tuple->ptr);
  ut_ad(n_fields == dtuple_get_n_fields(dst_tuple->ptr));

  /* Do a deep copy of the data fields. */
  for (i = 0; i < n_fields; ++i) {
    dfield_t *src_field;
    dfield_t *dst_field;

    src_field = dtuple_get_nth_field(src_tuple->ptr, i);
    dst_field = dtuple_get_nth_field(dst_tuple->ptr, i);

    if (!dfield_is_null(src_field)) {
      UNIV_MEM_ASSERT_RW(src_field->data, src_field->len);

      dst_field->data =
          mem_heap_dup(dst_tuple->heap, src_field->data, src_field->len);

      dst_field->len = src_field->len;
    } else {
      dfield_set_null(dst_field);
    }
  }

  return (err);
}

/** Create an InnoDB tuple used for index/table search.
@return	own: Tuple for current index */

ib_tpl_t
ib_sec_search_tuple_create(ib_crsr_t ib_crsr) /*!< in: Cursor instance */
{
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index = cursor->prebuilt->index;

  UT_DBG_ENTER_FUNC;

  n_cols = dict_index_get_n_unique_in_tree(dict_index);
  return (ib_key_tuple_new(dict_index, n_cols));
}

/** Create an InnoDB tuple used for index/table search.
@return	own: Tuple for current index */

ib_tpl_t ib_sec_read_tuple_create(ib_crsr_t ib_crsr) /*!< in: Cursor instance */
{
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index = cursor->prebuilt->index;

  UT_DBG_ENTER_FUNC;

  n_cols = dict_index_get_n_fields(dict_index);
  return (ib_row_tuple_new(dict_index, n_cols));
}

/** Create an InnoDB tuple used for table key operations.
@return	own: Tuple for current table */

ib_tpl_t
ib_clust_search_tuple_create(ib_crsr_t ib_crsr) /*!< in: Cursor instance */
{
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index;

  dict_index = dict_table_get_first_index(cursor->prebuilt->table);

  UT_DBG_ENTER_FUNC;

  n_cols = dict_index_get_n_ordering_defined_by_user(dict_index);
  return (ib_key_tuple_new(dict_index, n_cols));
}

/** Create an InnoDB tuple for table row operations.
@return	own: Tuple for current table */

ib_tpl_t
ib_clust_read_tuple_create(ib_crsr_t ib_crsr) /*!< in: Cursor instance */
{
  ulint n_cols;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_index_t *dict_index;

  dict_index = dict_table_get_first_index(cursor->prebuilt->table);

  UT_DBG_ENTER_FUNC;

  n_cols = dict_table_get_n_cols(cursor->prebuilt->table);
  return (ib_row_tuple_new(dict_index, n_cols));
}

/** Return the number of user columns in the tuple definition.
@return	number of user columns */

ib_ulint_t ib_tuple_get_n_user_cols(
    const ib_tpl_t ib_tpl) /*!< in: Tuple for current table */
{
  const ib_tuple_t *tuple = (const ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  if (tuple->type == TPL_ROW) {
    return (dict_table_get_n_user_cols(tuple->index->table));
  }

  return (dict_index_get_n_ordering_defined_by_user(tuple->index));
}

/** Return the number of columns in the tuple definition.
@return	number of columns */

ib_ulint_t
ib_tuple_get_n_cols(const ib_tpl_t ib_tpl) /*!< in: Tuple for table/index */
{
  const ib_tuple_t *tuple = (const ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  return (dtuple_get_n_fields(tuple->ptr));
}

/** Destroy an InnoDB tuple. */

void ib_tuple_delete(ib_tpl_t ib_tpl) /*!< in,own: Tuple instance to delete */
{
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  mem_heap_free(tuple->heap);
}

/** Truncate a table. The cursor handle will be closed and set to NULL
on success.
@return	DB_SUCCESS or error code */

ib_err_t ib_cursor_truncate(ib_crsr_t *ib_crsr, /*!< in/out: cursor for table
                                                to truncate */
                            ib_id_t *table_id)  /*!< out: new table id */
{
  ib_err_t err;
  ib_cursor_t *cursor = *(ib_cursor_t **)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

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

    *ib_crsr = NULL;

    /* This function currently commits the transaction
    on success. */
    err = ddl_truncate_table(table, trx);

    if (err == DB_SUCCESS) {
      *table_id = ut_conv_dulint_to_longlong(table->id);
    }
  }

  return (err);
}

/** Truncate a table.
@return	DB_SUCCESS or error code */

ib_err_t ib_table_truncate(const char *table_name, /*!< in: table name */
                           ib_id_t *table_id)      /*!< out: new table id */
{
  ib_err_t err;
  dict_table_t *table;
  ib_err_t trunc_err;
  ib_trx_t ib_trx = NULL;
  ib_crsr_t ib_crsr = NULL;

  IB_CHECK_PANIC();

  ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  dict_mutex_enter();

  table = dict_table_get_low(table_name);

  if (table != NULL && dict_table_get_first_index(table)) {
    dict_table_increment_handle_count(table, TRUE);
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

  if (ib_crsr != NULL) {
    err = ib_cursor_close(ib_crsr);
    ut_a(err == DB_SUCCESS);
  }

  if (trunc_err == DB_SUCCESS) {
    ut_a(ib_trx_state(ib_trx) == IB_TRX_NOT_STARTED);

    err = ib_schema_unlock(ib_trx);
    ut_a(err == DB_SUCCESS);

    err = ib_trx_release(ib_trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = ib_trx_rollback(ib_trx);
    ut_a(err == DB_SUCCESS);
  }

  return (trunc_err);
}

/** Get a table id. This function will acquire the dictionary mutex.
@return	DB_SUCCESS if found */

ib_err_t ib_table_get_id(const char *table_name, /*!< in: table to find */
                         ib_id_t *table_id)      /*!< out: table id if found */
{
  ib_err_t err;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  dict_mutex_enter();

  err = ib_table_get_id_low(table_name, table_id);

  dict_mutex_exit();

  return (err);
}

/** Get an index id.
@return	DB_SUCCESS if found */

ib_err_t
ib_index_get_id(const char *table_name, /*!< in: find index for this table */
                const char *index_name, /*!< in: index to find */
                ib_id_t *index_id)      /*!< out: index id if found */
{
  dict_table_t *table;
  char *normalized_name;
  ib_err_t err = DB_TABLE_NOT_FOUND;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  *index_id = 0;

  normalized_name = mem_alloc(ut_strlen(table_name) + 1);
  ib_normalize_table_name(normalized_name, table_name);

  table = ib_lookup_table_by_name(normalized_name);

  mem_free(normalized_name);
  normalized_name = NULL;

  if (table != NULL) {
    dict_index_t *dict_index;

    dict_index = dict_table_get_index_on_name(table, index_name);

    if (dict_index != NULL) {
      /* We only support 32 bit table and index ids. Because
      we need to pack the table id into the index id. */
      ut_a(ut_dulint_get_high(table->id) == 0);
      ut_a(ut_dulint_get_high(dict_index->id) == 0);

      *index_id = ut_dulint_get_low(table->id);
      *index_id <<= 32;
      *index_id |= ut_dulint_get_low(dict_index->id);

      err = DB_SUCCESS;
    }
  }

  return (err);
}

/** Create a database if it doesn't exist.
@return	IB_TRUE on success */

ib_bool_t
ib_database_create(const char *dbname) /*!< in: database name to create */
{
  const char *ptr;

  UT_DBG_ENTER_FUNC;

  for (ptr = dbname; *ptr; ++ptr) {
    if (*ptr == SRV_PATH_SEPARATOR) {
      return (IB_FALSE);
    }
  }

  /* Only necessary if file per table is set. */
  if (srv_file_per_table) {
    return (fil_mkdir(dbname));
  }

  return (IB_TRUE);
}

/** Drop a database if it exists. Drop all tables in the database too.
@return	DB_SUCCESS or error code */

ib_err_t ib_database_drop(const char *dbname) /*!< in: database name to drop */
{
  ib_trx_t ib_trx;
  char *ptr = NULL;
  ib_err_t err = DB_SUCCESS;
  ulint len = ut_strlen(dbname);

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (len == 0) {
    return (DB_INVALID_INPUT);
  }

  ptr = (char *)mem_alloc(len + 2);
  memset(ptr, 0x0, len + 2);
  ut_strcpy(ptr, dbname);
#ifdef __WIN__
  ib_to_lower_case(ptr);
#endif /* __WIN__ */

  ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

  /* Drop all the tables in the database first. */
  /* ddl_drop_database() expects a string that ends in '/'. */
  if (ptr[len - 1] != '/') {
    ptr[len] = '/';
  }

  err = ddl_drop_database(ptr, (trx_t *)ib_trx);

  /* Only necessary if file per table is set. */
  if (err == DB_SUCCESS && srv_file_per_table) {
    fil_rmdir(ptr);
  }

  mem_free(ptr);

  if (err == DB_SUCCESS) {
    ib_err_t trx_err;

    trx_err = ib_trx_commit(ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  } else {
    ib_err_t trx_err;

    trx_err = ib_trx_rollback(ib_trx);
    ut_a(trx_err == DB_SUCCESS);
  }

  return (err);
}

/** Check if cursor is positioned.
@return	IB_TRUE if positioned */

ib_bool_t ib_cursor_is_positioned(
    const ib_crsr_t ib_crsr) /*!< in: InnoDB cursor instance */
{
  const ib_cursor_t *cursor = (const ib_cursor_t *)ib_crsr;
  const row_prebuilt_t *prebuilt = cursor->prebuilt;

  UT_DBG_ENTER_FUNC;

  return (ib_btr_cursor_is_positioned(prebuilt->pcur));
}

/** Latches the data dictionary in shared mode.
@return	DB_SUCCESS or error code */

ib_err_t ib_schema_lock_shared(ib_trx_t ib_trx) /*!< in/out: transaction */
{
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->dict_operation_lock_mode == 0 ||
      trx->dict_operation_lock_mode == RW_S_LATCH) {

    dict_freeze_data_dictionary((trx_t *)ib_trx);
  }

  return (err);
}

/** Latches the data dictionary in exclusive mode.
@return	DB_SUCCESS or error code */

ib_err_t ib_schema_lock_exclusive(ib_trx_t ib_trx) /*!< in/out: transaction */
{
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->dict_operation_lock_mode == 0 ||
      trx->dict_operation_lock_mode == RW_X_LATCH) {

    dict_lock_data_dictionary((trx_t *)ib_trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return (err);
}

/** Checks if the data dictionary is latched in exclusive mode.
@return	TRUE if exclusive latch */

ib_bool_t
ib_schema_lock_is_exclusive(const ib_trx_t ib_trx) /*!< in: transaction */
{
  const trx_t *trx = (const trx_t *)ib_trx;

  return (trx->dict_operation_lock_mode == RW_X_LATCH);
}

/** Checks if the data dictionary is latched in shared mode.
@return	TRUE if shared latch */

ib_bool_t
ib_schema_lock_is_shared(const ib_trx_t ib_trx) /*!< in: transaction */
{
  const trx_t *trx = (const trx_t *)ib_trx;

  return (trx->dict_operation_lock_mode == RW_S_LATCH);
}

/** Unlocks the data dictionary.
@return	DB_SUCCESS or error code */

ib_err_t ib_schema_unlock(ib_trx_t ib_trx) /*!< in/out: transaction */
{
  ib_err_t err = DB_SUCCESS;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->dict_operation_lock_mode == RW_X_LATCH) {
    dict_unlock_data_dictionary((trx_t *)ib_trx);
  } else if (trx->dict_operation_lock_mode == RW_S_LATCH) {
    dict_unfreeze_data_dictionary((trx_t *)ib_trx);
  } else {
    err = DB_SCHEMA_NOT_LOCKED;
  }

  return (err);
}

/** Set the Lock an InnoDB cursor/table.
@return	DB_SUCCESS or error code */

ib_err_t ib_cursor_lock(ib_crsr_t ib_crsr,         /*!< in/out: InnoDB cursor */
                        ib_lck_mode_t ib_lck_mode) /*!< in: InnoDB lock mode */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;
  trx_t *trx = prebuilt->trx;
  dict_table_t *table = prebuilt->table;

  IB_CHECK_PANIC();

  return (
      ib_trx_lock_table_with_retry(trx, table, (enum lock_mode)ib_lck_mode));
}

/** Set the Lock an InnoDB table using the table id.
@return	DB_SUCCESS or error code */

ib_err_t ib_table_lock(ib_trx_t ib_trx,           /*!< in/out: transaction */
                       ib_id_t table_id,          /*!< in: table id */
                       ib_lck_mode_t ib_lck_mode) /*!< in: InnoDB lock mode */
{
  ib_err_t err;
  que_thr_t *thr;
  mem_heap_t *heap;
  dict_table_t *table;
  ib_qry_proc_t q_proc;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(trx->conc_state != TRX_NOT_STARTED);

  table = ib_open_table_by_id(table_id, FALSE);

  if (table == NULL) {
    return (DB_TABLE_NOT_FOUND);
  }

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  heap = mem_heap_create(128);

  q_proc.node.sel = sel_node_create(heap);

  thr = pars_complete_graph_for_exec(q_proc.node.sel, trx, heap);

  q_proc.grph.sel = que_node_get_parent(thr);
  q_proc.grph.sel->state = QUE_FORK_ACTIVE;

  trx->op_info = "setting table lock";

  ut_a(ib_lck_mode == IB_LOCK_IS || ib_lck_mode == IB_LOCK_IX);
  err = lock_table(0, table, (enum lock_mode)ib_lck_mode, thr);

  trx->error_state = err;

  dict_table_decrement_handle_count(table, FALSE);

  mem_heap_free(heap);

  return (err);
}

/** Unlock an InnoDB table.
@return	DB_SUCCESS or error code */

ib_err_t ib_cursor_unlock(ib_crsr_t ib_crsr) /*!< in/out: InnoDB cursor */
{
  ib_err_t err;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (prebuilt->trx->client_n_tables_locked > 0) {
    --prebuilt->trx->client_n_tables_locked;
    err = DB_SUCCESS;
  } else {
    err = DB_ERROR;
  }

  return (err);
}

/** Set the Lock mode of the cursor.
@return	DB_SUCCESS or error code */

ib_err_t
ib_cursor_set_lock_mode(ib_crsr_t ib_crsr,         /*!< in/out: InnoDB cursor */
                        ib_lck_mode_t ib_lck_mode) /*!< in: InnoDB lock mode */
{
  ib_err_t err = DB_SUCCESS;
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  ut_a(ib_lck_mode <= (ib_lck_mode_t)LOCK_NUM);

  if (ib_lck_mode == IB_LOCK_X) {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IX);
  } else {
    err = ib_cursor_lock(ib_crsr, IB_LOCK_IS);
  }

  if (err == DB_SUCCESS) {
    prebuilt->select_lock_type = (enum lock_mode)ib_lck_mode;
    ut_a(prebuilt->trx->conc_state != TRX_NOT_STARTED);
  }

  return (err);
}

/** Set need to access clustered index record. */

void ib_cursor_set_cluster_access(
    ib_crsr_t ib_crsr) /*!< in/out: InnoDB cursor */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  UT_DBG_ENTER_FUNC;

  prebuilt->need_to_access_clustered = TRUE;
}

/** Set to true if it's a simple select. */

void ib_cursor_set_simple_select(
    ib_crsr_t ib_crsr) /*!< in/out: InnoDB cursor */
{
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  row_prebuilt_t *prebuilt = cursor->prebuilt;

  UT_DBG_ENTER_FUNC;

  prebuilt->simple_select = TRUE;
}

/** Creates a named savepoint. The transaction must be started. If there is
already a savepoint of the same name, this call erases that old savepoint
and replaces it with a new. Savepoints are deleted in a transaction
commit or rollback. */

void ib_savepoint_take(ib_trx_t ib_trx,     /*!< in: transaction */
                       const void *name,    /*!< in: savepoint name */
                       ib_ulint_t name_len) /*!< in: length of name in bytes */
{
  trx_named_savept_t *savep;
  trx_t *trx = (trx_t *)ib_trx;

  ut_a(trx);
  ut_a(name != NULL);
  ut_a(name_len > 0);

  ut_a(trx->conc_state != TRX_NOT_STARTED);

  savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  /* Check if there is a savepoint with the same name already. */
  while (savep != NULL) {

    if (name_len == savep->name_len &&
        0 == ut_memcmp(savep->name, name, name_len)) {
      break;
    }

    savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
  }

  if (savep) {
    /* There is a savepoint with the same name: free that */
    UT_LIST_REMOVE(trx_savepoints, trx->trx_savepoints, savep);

    mem_free(savep);
  }

  /* Create a new savepoint and add it as the last in the list */
  savep = mem_alloc(sizeof(trx_named_savept_t) + name_len);

  savep->name = savep + 1;
  savep->savept = trx_savept_take(trx);

  savep->name_len = name_len;
  ut_memcpy(savep->name, name, name_len);

  UT_LIST_ADD_LAST(trx_savepoints, trx->trx_savepoints, savep);
}

/** Releases only the named savepoint. Savepoints which were set after this
savepoint are left as is.
@return	if no savepoint of the name found then DB_NO_SAVEPOINT, otherwise
DB_SUCCESS */

ib_err_t
ib_savepoint_release(ib_trx_t ib_trx,     /*!< in: transaction handle */
                     const void *name,    /*!< in: savepoint name */
                     ib_ulint_t name_len) /*!< in: length of name in bytes */
{
  trx_named_savept_t *savep;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  /* Search for the savepoint by name and free if found. */
  while (savep != NULL) {

    if (name_len == savep->name_len &&
        0 == ut_memcmp(savep->name, name, name_len)) {

      UT_LIST_REMOVE(trx_savepoints, trx->trx_savepoints, savep);
      mem_free(savep);

      return (DB_SUCCESS);
    }

    savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
  }

  return (DB_NO_SAVEPOINT);
}

/** Rolls back a transaction back to a named savepoint. Modifications after the
savepoint are undone but InnoDB does NOT release the corresponding locks
which are stored in memory. If a lock is 'implicit', that is, a new inserted
row holds a lock where the lock information is carried by the trx id stored in
the row, these locks are naturally released in the rollback. Savepoints which
were set after this savepoint are deleted. If name equals NULL then all the
savepoints are rolled back.
@return	if no savepoint of the name found then DB_NO_SAVEPOINT, otherwise
DB_SUCCESS */

ib_err_t
ib_savepoint_rollback(ib_trx_t ib_trx,  /*!< in: transaction handle */
                      const void *name, /*!< in: savepoint name  can be NULL */
                      ib_ulint_t name_len) /*!< in: length of name in bytes */
{
  ib_err_t err;
  trx_named_savept_t *savep;
  trx_t *trx = (trx_t *)ib_trx;

  IB_CHECK_PANIC();

  if (trx->conc_state == TRX_NOT_STARTED) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  InnoDB: Error: transaction trying to rollback a  "
                         "savepoint ");
    ut_print_name(ib_stream, trx, FALSE, name);
    ib_logger(ib_stream, " though it is not started\n");

    return (DB_ERROR);
  }

  savep = UT_LIST_GET_FIRST(trx->trx_savepoints);

  if (name != NULL) {
    while (savep != NULL) {
      if (savep->name_len == name_len &&
          0 == ut_memcmp(savep->name, name, name_len)) {

        /* Found */
        break;
      }
      savep = UT_LIST_GET_NEXT(trx_savepoints, savep);
    }
  }

  if (savep == NULL) {

    return (DB_NO_SAVEPOINT);
  }

  /* We can now free all savepoints strictly later than this one */
  trx_roll_savepoints_free(trx, savep);

  trx->op_info = "rollback to a savepoint";

  err = trx_general_rollback(trx, TRUE, &savep->savept);

  /* Store the current undo_no of the transaction so that we know where
  to roll back if we have to roll back the next SQL statement: */
  trx_mark_sql_stat_end(trx);

  trx->op_info = "";

  return (err);
}

/** Convert from internal format to the the table definition table attributes */
static void
ib_table_get_format(const dict_table_t *table, /*!< in: table definition */
                    ib_tbl_fmt_t *tbl_fmt,     /*!< out: table format */
                    ulint *page_size)          /*!< out: page size */
{
  UT_DBG_ENTER_FUNC;

  *page_size = 0;
  *tbl_fmt = IB_TBL_REDUNDANT;

  switch (table->flags) {
  case 0:
    break;
  case DICT_TF_COMPACT: /* The compact row format */
    *tbl_fmt = IB_TBL_COMPACT;
    break;
  case DICT_TF_COMPACT | DICT_TF_FORMAT_ZIP << DICT_TF_FORMAT_SHIFT:
    *tbl_fmt = IB_TBL_DYNAMIC;
    break;
#if DICT_TF_FORMAT_MAX > DICT_TF_FORMAT_ZIP
#error "missing case labels for DICT_TF_FORMAT_ZIP .. DICT_TF_FORMAT_MAX"
#endif
  default:
    if (table->flags & DICT_TF_ZSSIZE_MASK) {
      *tbl_fmt = IB_TBL_COMPRESSED;

      *page_size =
          ((PAGE_ZIP_MIN_SIZE >> 1)
           << ((table->flags & DICT_TF_ZSSIZE_MASK) >> DICT_TF_ZSSIZE_SHIFT));
    }
  }
}

/** Call the visitor for each column in a table.
@return	return value from index_col */
static int ib_table_schema_visit_table_columns(
    const dict_table_t *table, /*!< in: table to visit */
    ib_schema_visitor_table_col_t table_col,
    /*!< in: table column visitor */
    void *arg) /*!< in: argument to visitor */
{
  ulint i;

  for (i = 0; i < table->n_cols; ++i) {
    dict_col_t *col;
    const char *name;
    ib_col_attr_t attr;
    ulint col_no;
    int user_err;

    col = dict_table_get_nth_col(table, i);
    col_no = dict_col_get_no(col);
    name = dict_table_get_col_name(table, col_no);
    attr = ib_col_get_attr(col->prtype);

    user_err = table_col(arg, name, col->mtype, col->len, attr);

    if (user_err) {
      return (user_err);
    }
  }

  return (0);
}

/** Call the visitor for each column in an index.
@return	return value from index_col */
static int ib_table_schema_visit_index_columns(
    const dict_index_t *dict_index, /*!< in: index to visit */
    ib_schema_visitor_index_col_t index_col,
    /*!< in: index column
    visitor */
    void *arg) /*!< in: argument to visitor */
{
  ulint i;
  ulint n_index_cols = dict_index->n_user_defined_cols;

  for (i = 0; i < n_index_cols; ++i) {
    const dict_field_t *dfield;
    int user_err;

    dfield = &dict_index->fields[i];

    user_err = index_col(arg, dfield->name, dfield->prefix_len);

    if (user_err) {
      return (user_err);
    }
  }

  return (0);
}

/** Read a table's schema using the visitor pattern. It will make the
following sequence of calls:

        visitor->table()
        visitor->table_col() for each user column
        visitor->index() for each user index
        visitor->index_col() for each column in user index

It will stop if any of the above functions returns a non-zero value.
The caller must have an exclusive lock on the InnoDB data dictionary
@return	DB_SUCCESS or DB_ERROR */

ib_err_t
ib_table_schema_visit(ib_trx_t ib_trx,  /*!< in: transaction that
                                        owns the schema lock */
                      const char *name, /*!< in: table name to read */
                      const ib_schema_visitor_t *visitor, /*!< in: visitor
                                                          functions to invoke on
                                                          each definition */
                      void *arg) /*!< Argument passed to the
                                 visitor functions. */
{
  dict_index_t *dict_index;
  dict_table_t *table;
  ib_tbl_fmt_t tbl_fmt;
  ulint page_size;
  int n_indexes;
  int user_err = 0;
  char *normalized_name;

  IB_CHECK_PANIC();

  UT_DBG_ENTER_FUNC;

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  }

  normalized_name = mem_alloc(ut_strlen(name) + 1);
  ib_normalize_table_name(normalized_name, name);

  table = ib_lookup_table_by_name(normalized_name);

  mem_free(normalized_name);
  normalized_name = NULL;

  if (table != NULL) {
    dict_table_increment_handle_count(table, TRUE);
  } else {
    return (DB_TABLE_NOT_FOUND);
  }

  ib_table_get_format(table, &tbl_fmt, &page_size);

  /* We need the count of user defined indexes only. */
  n_indexes = UT_LIST_GET_LEN(table->indexes);

  /* The first index is always the cluster index. */
  dict_index = dict_table_get_first_index(table);

  /* Only the clustered index can be auto generated. */
  if (dict_index->n_user_defined_cols == 0) {
    --n_indexes;
  }

  if (visitor->version < IB_SCHEMA_VISITOR_TABLE) {

    goto func_exit;

  } else if (visitor->table) {
    user_err = visitor->table(arg, table->name, tbl_fmt, page_size,
                              table->n_cols, n_indexes);

    if (user_err) {
      goto func_exit;
    }
  }

  if (visitor->version < IB_SCHEMA_VISITOR_TABLE_COL) {

    goto func_exit;

  } else if (visitor->table_col) {
    user_err =
        ib_table_schema_visit_table_columns(table, visitor->table_col, arg);

    if (user_err) {
      goto func_exit;
    }
  }

  if (!visitor->index) {
    goto func_exit;
  } else if (visitor->version < IB_SCHEMA_VISITOR_TABLE_AND_INDEX) {
    goto func_exit;
  }

  /* Traverse the user defined indexes. */
  do {
    ulint n_index_cols;

    n_index_cols = dict_index->n_user_defined_cols;

    /* Ignore system generated indexes. */
    if (n_index_cols > 0) {
      user_err = visitor->index(
          arg, dict_index->name, dict_index_is_unique(dict_index) != 0,
          dict_index_is_clust(dict_index) != 0, n_index_cols);
      if (user_err) {
        goto func_exit;
      }

      if (visitor->version >= IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL &&
          visitor->index_col) {
        user_err = ib_table_schema_visit_index_columns(dict_index,
                                                       visitor->index_col, arg);

        if (user_err) {
          break;
        }
      }
    }

    dict_index = UT_LIST_GET_NEXT(indexes, dict_index);
  } while (dict_index != NULL);

func_exit:
  ut_a(ib_schema_lock_is_exclusive(ib_trx));
  dict_table_decrement_handle_count(table, TRUE);

  return (user_err != 0 ? DB_ERROR : DB_SUCCESS);
}

/** List all the tables in the InnoDB's data dictionary. It will abort
if visitor returns a non-zero value.

It will call the function:
        visitor.tables(arg, const char* name, int name_len);

The function will abort if visitor.tables() returns non-zero. */

ib_err_t ib_schema_tables_iterate(
    ib_trx_t ib_trx,                       /*!< in: transaction that
                                           owns the schema lock */
    ib_schema_visitor_table_all_t visitor, /*!< in: visitor function */
    void *arg) /*!< in: argument passed to the visitor
               function */
{
  ib_err_t err;
  dict_table_t *table;
  ib_crsr_t ib_crsr;
  ib_err_t crsr_err;
  ib_tpl_t ib_tpl = NULL;

  IB_CHECK_PANIC();

  if (!ib_schema_lock_is_exclusive(ib_trx)) {
    return (DB_SCHEMA_NOT_LOCKED);
  }

  table = ib_lookup_table_by_name("SYS_TABLES");

  if (table != NULL) {
    dict_table_increment_handle_count(table, TRUE);
    err = ib_create_cursor(&ib_crsr, table, 0, (trx_t *)ib_trx);
  } else {
    return (DB_TABLE_NOT_FOUND);
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
      ib_ulint_t len;

      ptr = ib_col_get_value(ib_tpl, 0);
      /* Can't have NULL columns. */
      ut_a(ptr != NULL);

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

  return (err);
}

#if 0
/** Convert and write an INT column value to an InnoDB tuple.
@return	DB_SUCCESS or error */
UNIV_INLINE
ib_err_t ib_tuple_write_int(ib_tpl_t ib_tpl,   /*!< upd: tuple to write to */
                            ulint col_no,      /*!< in: column number */
                            const void *value, /*!< in: integer value */
                            ulint value_len)   /*!< in: sizeof value type */
{
  const dfield_t *dfield;
  ulint data_len;
  ulint type_len;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  ut_a(col_no < ib_tuple_get_n_cols(ib_tpl));

  dfield = ib_col_get_dfield(tuple, col_no);

  data_len = dfield_get_len(dfield);
  type_len = dtype_get_len(dfield_get_type(dfield));

  if (dtype_get_mtype(dfield_get_type(dfield)) != DATA_INT ||
      value_len != data_len) {

    return (DB_DATA_MISMATCH);
  }

  return (ib_col_set_value(ib_tpl, col_no, value, type_len));
}
#endif

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCESS or error */

ib_err_t ib_tuple_write_i8(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                           int col_no,      /*!< in: column number */
                           ib_i8_t val)     /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCESS or error */

ib_err_t ib_tuple_write_i16(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                            int col_no,      /*!< in: column number */
                            ib_i16_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_i32(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                            int col_no,      /*!< in: column number */
                            ib_i32_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_i64(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                            int col_no,      /*!< in: column number */
                            ib_i64_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_u8(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                           int col_no,      /*!< in: column number */
                           ib_u8_t val)     /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_u16(ib_tpl_t ib_tpl, /*!< upd: tupe to write to */
                            int col_no,      /*!< in: column number */
                            ib_u16_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_u32(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                            int col_no,      /*!< in: column number */
                            ib_u32_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Write an integer value to a column. Integers are stored in big-endian
format and will need to be converted from the host format.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_u64(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                            int col_no,      /*!< in: column number */
                            ib_u64_t val)    /*!< in: value to write */
{
  return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
}

/** Inform the cursor that it's the start of an SQL statement. */

void ib_cursor_stmt_begin(ib_crsr_t ib_crsr) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;

  cursor->prebuilt->sql_stat_start = TRUE;
}

/** Write a double value to a column.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_double(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                               int col_no,      /*!< in: column number */
                               double val)      /*!< in: value to write */
{
  const dfield_t *dfield;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
  } else {
    return (DB_DATA_MISMATCH);
  }
}

/** Read a double column value from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_double(ib_tpl_t ib_tpl,   /*!< in: InnoDB tuple */
                              ib_ulint_t col_no, /*!< in: column number */
                              double *dval)      /*!< out: double value */
{
  ib_err_t err;
  const dfield_t *dfield;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_DOUBLE) {
    ib_col_copy_value_low(ib_tpl, col_no, dval, sizeof(*dval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return (err);
}

/** Write a float value to a column.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_write_float(ib_tpl_t ib_tpl, /*!< upd: tuple to write to */
                              int col_no,      /*!< in: column number */
                              float val)       /*!< in: value to write */
{
  const dfield_t *dfield;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    return (ib_col_set_value(ib_tpl, col_no, &val, sizeof(val)));
  } else {
    return (DB_DATA_MISMATCH);
  }
}

/** Read a float value from an InnoDB tuple.
@return	DB_SUCCESS or error */

ib_err_t ib_tuple_read_float(ib_tpl_t ib_tpl,   /*!< in: InnoDB tuple */
                             ib_ulint_t col_no, /*!< in: column number */
                             float *fval)       /*!< out: float value */
{
  ib_err_t err;
  const dfield_t *dfield;
  ib_tuple_t *tuple = (ib_tuple_t *)ib_tpl;

  UT_DBG_ENTER_FUNC;

  dfield = ib_col_get_dfield(tuple, col_no);

  if (dtype_get_mtype(dfield_get_type(dfield)) == DATA_FLOAT) {
    ib_col_copy_value_low(ib_tpl, col_no, fval, sizeof(*fval));
    err = DB_SUCCESS;
  } else {
    err = DB_DATA_MISMATCH;
  }

  return (err);
}

/** Set the message logging function. */

void ib_logger_set(ib_msg_log_t ib_msg_log, /*!< in: the logging function */
                   ib_msg_stream_t ib_msg_stream) /*!< in: the message stream,
                                                  this is the first argument to
                                                  the logging function */
{
  ib_logger = (ib_logger_t)ib_msg_log;
  ib_stream = (ib_stream_t)ib_msg_stream;
}

/** Convert an error number to a human readable text message. The
returned string is static and should not be freed or modified.
@return	string, describing the error */

const char *ib_strerror(ib_err_t num) /*!< in: error number */
{
  switch (num) {
  case DB_SUCCESS:
    return ("Success");
  case DB_ERROR:
    return ("Generic error");
  case DB_OUT_OF_MEMORY:
    return ("Cannot allocate memory");
  case DB_OUT_OF_FILE_SPACE:
    return ("Out of disk space");
  case DB_LOCK_WAIT:
    return ("Lock wait");
  case DB_DEADLOCK:
    return ("Deadlock");
  case DB_ROLLBACK:
    return ("Rollback");
  case DB_DUPLICATE_KEY:
    return ("Duplicate key");
  case DB_QUE_THR_SUSPENDED:
    return ("The queue thread has been suspended");
  case DB_MISSING_HISTORY:
    return ("Required history data has been deleted");
  case DB_CLUSTER_NOT_FOUND:
    return ("Cluster not found");
  case DB_TABLE_NOT_FOUND:
    return ("Table not found");
  case DB_MUST_GET_MORE_FILE_SPACE:
    return ("More file space needed");
  case DB_TABLE_IS_BEING_USED:
    return ("Table is being used");
  case DB_TOO_BIG_RECORD:
    return ("Record too big");
  case DB_LOCK_WAIT_TIMEOUT:
    return ("Lock wait timeout");
  case DB_NO_REFERENCED_ROW:
    return ("Referenced key value not found");
  case DB_ROW_IS_REFERENCED:
    return ("Row is referenced");
  case DB_CANNOT_ADD_CONSTRAINT:
    return ("Cannot add constraint");
  case DB_CORRUPTION:
    return ("Data structure corruption");
  case DB_COL_APPEARS_TWICE_IN_INDEX:
    return ("Column appears twice in index");
  case DB_CANNOT_DROP_CONSTRAINT:
    return ("Cannot drop constraint");
  case DB_NO_SAVEPOINT:
    return ("No such savepoint");
  case DB_TABLESPACE_ALREADY_EXISTS:
    return ("Tablespace already exists");
  case DB_TABLESPACE_DELETED:
    return ("No such tablespace");
  case DB_LOCK_TABLE_FULL:
    return ("Lock structs have exhausted the buffer pool");
  case DB_FOREIGN_DUPLICATE_KEY:
    return ("Foreign key activated with duplicate keys");
  case DB_TOO_MANY_CONCURRENT_TRXS:
    return ("Too many concurrent transactions");
  case DB_UNSUPPORTED:
    return ("Unsupported");
  case DB_PRIMARY_KEY_IS_NULL:
    return ("Primary key is NULL");
  case DB_FAIL:
    return ("Failed, retry may succeed");
  case DB_OVERFLOW:
    return ("Overflow");
  case DB_UNDERFLOW:
    return ("Underflow");
  case DB_STRONG_FAIL:
    return ("Failed, retry will not succeed");
  case DB_ZIP_OVERFLOW:
    return ("Zip overflow");
  case DB_RECORD_NOT_FOUND:
    return ("Record not found");
  case DB_END_OF_INDEX:
    return ("End of index");
  case DB_SCHEMA_ERROR:
    return ("Error while validating a table or index schema");
  case DB_DATA_MISMATCH:
    return ("Type mismatch");
  case DB_SCHEMA_NOT_LOCKED:
    return ("Schema not locked");
  case DB_NOT_FOUND:
    return ("Not found");
  case DB_READONLY:
    return ("Readonly");
  case DB_INVALID_INPUT:
    return ("Invalid input");
  case DB_FATAL:
    return ("InnoDB fatal error");
  case DB_INTERRUPTED:
    return ("Operation interrupted");
    /* do not add default: in order to produce a warning if new code
    is added to the enum but not added here */
  }

  /* NOT REACHED */
  return ("Unknown error");
}

extern ib_panic_function_t ib_panic;

void ib_set_panic_handler(ib_panic_handler_t new_panic_handler) {
  ib_panic = new_panic_handler;
}

extern ib_trx_is_interrupted_handler_t ib_trx_is_interrupted;

void ib_set_trx_is_interrupted_handler(
    ib_trx_is_interrupted_handler_t handler) {
  ib_trx_is_interrupted = handler;
}

ib_err_t ib_get_duplicate_key(ib_trx_t ib_trx, const char **table_name,
                              const char **index_name) {
  trx_t *trx = (trx_t *)ib_trx;

  if (trx->error_info == NULL)
    return DB_ERROR;

  *table_name = trx->error_info->table_name;
  *index_name = trx->error_info->name;

  return DB_SUCCESS;
}

ib_err_t ib_get_table_statistics(ib_crsr_t ib_crsr,
                                 ib_table_stats_t *table_stats,
                                 size_t sizeof_ib_table_stats_t) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_table_t *table = cursor->prebuilt->table;

  if (table->stat_initialized != TRUE) {
    dict_update_statistics(table);
  }

  table_stats->stat_n_rows = table->stat_n_rows;
  table_stats->stat_clustered_index_size =
      table->stat_clustered_index_size * UNIV_PAGE_SIZE;
  table_stats->stat_sum_of_other_index_sizes =
      table->stat_sum_of_other_index_sizes * UNIV_PAGE_SIZE;
  table_stats->stat_modified_counter = table->stat_modified_counter;

  return DB_SUCCESS;
}

ib_err_t ib_get_index_stat_n_diff_key_vals(ib_crsr_t ib_crsr,
                                           const char *index_name,
                                           ib_u64_t *ncols, ib_i64_t **n_diff) {
  ib_cursor_t *cursor = (ib_cursor_t *)ib_crsr;
  dict_table_t *table = cursor->prebuilt->table;
  dict_index_t *index;

  if (table->stat_initialized != TRUE) {
    dict_update_statistics(table);
  }

  index = dict_table_get_index_on_name(table, index_name);

  if (index == NULL)
    return DB_NOT_FOUND;

  *ncols = index->n_uniq;

  *n_diff = (ib_i64_t *)malloc(sizeof(ib_i64_t) * index->n_uniq);

  dict_index_stat_mutex_enter(index);

  memcpy(*n_diff, index->stat_n_diff_key_vals,
         sizeof(ib_int64_t) * index->n_uniq);

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
    srv_panic(DB_CORRUPTION, "test panic message");
    return DB_SUCCESS;
  }

  return DB_ERROR;
}
