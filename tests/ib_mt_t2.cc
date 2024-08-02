/** Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.

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

/** This is a derived table class that can be plugged into the mt_drv test
suite. This particular table class overrides following DML and DDL
functions:
1) UPDATE
2) INSERT
3) DELETE

4) CREATE
5) ALTER

Following functions are used from generic base table class which is
defined in mt_base.c:
1) SELECT

2) DROP
3) TRUNCATE


The table definition is:
CREATE TABLE t2
        (c1		INT PRIMARY KEY,
         score		INT,
         ins_run	INT,
         upd_run	INT)
***********************************************************************/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "test0aux.h"
#include "ib_mt_drv.h"
#include "ib_mt_base.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

/* Limit for PK values. Delibrately kept low to trigger DUPLICATE_KEY
errors. */
static const int key_range = 100;

/** CREATE TABLE t2...
@return DB_SUCCESS or error code */
static ib_err_t create_t2(void *arg) /*!< in: arguments for callback */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_err_t err2 = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  ib_idx_sch_t ib_idx_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  snprintf(table_name, sizeof(table_name), "%s/%s", tbl->m_db_name.c_str(),
           tbl->m_name.c_str());

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl->format,
                               tbl->page_size);

  assert(err == DB_SUCCESS);

  err =
      ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "score", IB_INT, IB_COL_UNSIGNED, 0,
                                4);

  assert(err == DB_SUCCESS);

  /* These two columns are where we put the value of
  run_number when doing INSERT or UPDATE */
  err = ib_table_schema_add_col(ib_tbl_sch, "ins_run", IB_INT, IB_COL_UNSIGNED,
                                0, 4);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "upd_run", IB_INT, IB_COL_UNSIGNED,
                                0, 4);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PK_index", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = ib_trx_begin(ib_trx_level_t(cb_arg->isolation_level));
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);

  err2 = ib_trx_commit(ib_trx);
  assert(err2 == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  update_err_stats(cb_arg->err_st, err);
  return (err);
}

/** ALTER TABLE ...
TODO: This should have FIC stuff. No-op for now.
@return DB_SUCCESS or error code */
static ib_err_t alter_t2(void *arg) /*!< in: arguments for callback */
{
  // fprintf(stderr, "t2: ALTER\n");
  return (DB_SUCCESS);
}

/** INSERT INTO t2 VALUES (<rand 1-100>, 0, run_number, 0)
@return DB_SUCCESS or error code */
static ib_err_t insert_t2(void *arg) /*!< in: arguments for callback */
{
  int i;
  ib_err_t err;

  ib_crsr_t crsr = nullptr;
  ib_tpl_t tpl = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: INSERT\n");

  err = open_table(tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  for (i = 0; i < cb_arg->batch_size; ++i) {

    int val = 0;
    int zero = 0;

    val = random() % key_range;
    err = ib_col_set_value(tpl, 0, &val, 4);
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 1, &zero, 4);
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 2, &cb_arg->run_number, 4);
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 3, &zero, 4);
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }
    update_err_stats(cb_arg->err_st, err);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != nullptr);
  }

  goto clean_exit;

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  if (crsr != nullptr) {
    ib_err_t err2;

    err2 = ib_cursor_close(crsr);
    assert(err2 == DB_SUCCESS);
    crsr = nullptr;
  }

  return (err);
}

/** UPDATE t2 SET score = score + 100 AND upd_run = run_number
WHERE c1 == 5
@return DB_SUCCESS or error code */
static ib_err_t update_t2(void *arg) /*!< in: arguments for callback */
{
  ib_err_t err;
  int res = ~0L;
  uint32_t five = 5;
  ib_tpl_t key_tpl = nullptr;
  ib_tpl_t old_tpl = nullptr;
  ib_tpl_t new_tpl = nullptr;
  ib_crsr_t crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: UPDATE\n");

  err = open_table(tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != nullptr);

  /* Set the value to look for. */
  err = ib_col_set_value(key_tpl, 0, &five, 4);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  ib_tuple_delete(key_tpl);

  if (res != 0) {
    goto clean_exit;
  } else if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = ib_clust_read_tuple_create(crsr);
  assert(old_tpl != nullptr);

  new_tpl = ib_clust_read_tuple_create(crsr);
  assert(new_tpl != nullptr);

  /* Iterate over the records while the first column matches "a". */
  while (1) {
    uint32_t score;
    uint32_t val;
    ib_ulint_t data_len;
    ib_col_meta_t col_meta;

    err = ib_cursor_read_row(crsr, old_tpl);
    assert(err == DB_SUCCESS);

    ib_col_get_meta(old_tpl, 0, &col_meta);

    err = ib_tuple_read_u32(old_tpl, 0, &val);
    assert(err == DB_SUCCESS);

    if (val != five) {
      goto clean_exit;
    }

    /* Copy the old contents to the new tuple. */
    err = ib_tuple_copy(new_tpl, old_tpl);

    /* Update the score column in the new tuple. */
    data_len = ib_col_get_meta(old_tpl, 1, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = ib_tuple_read_u32(old_tpl, 1, &score);
    assert(err == DB_SUCCESS);
    score += 100;

    /* Set the updated value in the new tuple. */
    err = ib_tuple_write_u32(new_tpl, 1, score);
    assert(err == DB_SUCCESS);

    /* Set the updated value in the new tuple. */
    err = ib_tuple_write_u32(new_tpl, 3, cb_arg->run_number);
    assert(err == DB_SUCCESS);

    err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }
    update_err_stats(cb_arg->err_st, err);

    /* Move to the next record to update. */
    err = ib_cursor_next(crsr);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }

    /* Reset the old and new tuple instances. */
    old_tpl = ib_tuple_clear(old_tpl);
    assert(old_tpl != nullptr);

    new_tpl = ib_tuple_clear(new_tpl);
    assert(new_tpl != nullptr);
  }

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:
  if (old_tpl != nullptr) {
    ib_tuple_delete(old_tpl);
  }
  if (new_tpl != nullptr) {
    ib_tuple_delete(new_tpl);
  }

  if (crsr != nullptr) {
    ib_err_t err2;

    err2 = ib_cursor_close(crsr);
    assert(err2 == DB_SUCCESS);
    crsr = nullptr;
  }

  return (err);
}

/** DELETE FROM t2 WHERE c1 == 9
@return DB_SUCCESS or error code */
static ib_err_t delete_t2(void *arg) /*!< in: arguments for callback */
{
  ib_err_t err;
  int res = ~0L;
  int nine = 9;
  ib_tpl_t key_tpl = nullptr;
  ib_crsr_t crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: DELETE\n");

  err = open_table(tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != nullptr);

  /* Set the value to delete. */
  err = ib_col_set_value(key_tpl, 0, &nine, 4);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
  if (res != 0) {
    goto clean_exit;
  }

  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* InnoDB handles the updating of all secondary indexes. */
  err = ib_cursor_delete_row(crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }
  update_err_stats(cb_arg->err_st, err);
  goto clean_exit;

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:

  if (key_tpl != nullptr) {
    ib_tuple_delete(key_tpl);
  }

  if (crsr != nullptr) {
    ib_err_t err2;

    err2 = ib_cursor_close(crsr);
    assert(err2 == DB_SUCCESS);
    crsr = nullptr;
  }

  return (err);
}

/** Function to register this table class with mt_drv test suite */

void register_t2_table(tbl_class_t *tbl) /*!< in/out: table class to register */
{
  assert(tbl != nullptr);

  tbl->m_name.assign("t2");

  tbl->dml_fn[DML_OP_TYPE_INSERT] = insert_t2;
  tbl->dml_fn[DML_OP_TYPE_UPDATE] = update_t2;
  tbl->dml_fn[DML_OP_TYPE_DELETE] = delete_t2;

  tbl->ddl_fn[DDL_OP_TYPE_CREATE] = create_t2;
  tbl->ddl_fn[DDL_OP_TYPE_ALTER] = alter_t2;
}
