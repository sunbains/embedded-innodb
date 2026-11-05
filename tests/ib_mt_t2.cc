/***************************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
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

/** This is a derived table class that can be plugged into the mt_drv test
suite. This particular table class overrides following DML and Ddl
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
static DbErr create_t2(System &sys, void *arg) /*!< in: arguments for callback */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  DbErr err2 = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * ib_idx_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  snprintf(table_name, sizeof(table_name), "%s/%s", tbl->m_db_name.c_str(),
           tbl->m_name.c_str());

  err = TableSchema::create(table_name, &ib_tbl_sch, tbl->format);

  assert(err == DB_SUCCESS);

  err =
      ib_tbl_sch->add_col("c1", IB_INT, IB_COL_UNSIGNED, 0, 4);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("score", IB_INT, IB_COL_UNSIGNED, 0,
                                4);

  assert(err == DB_SUCCESS);

  /* These two columns are where we put the value of
  run_number when doing INSERT or UPDATE */
  err = ib_tbl_sch->add_col("ins_run", IB_INT, IB_COL_UNSIGNED,
                                0, 4);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("upd_run", IB_INT, IB_COL_UNSIGNED,
                                0, 4);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("PK_index", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = api::trx_begin(sys, TrxLevel(cb_arg->isolation_level));
  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);

  err2 = api::trx_commit(sys, ib_trx);
  assert(err2 == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    TableSchema::destroy(sys, ib_tbl_sch);
  }

  update_err_stats(cb_arg->err_st, err);
  return (err);
}

/** ALTER TABLE ...
TODO: This should have FIC stuff. No-op for now.
@return DB_SUCCESS or error code */
static DbErr alter_t2(System &sys, void *arg) /*!< in: arguments for callback */
{
  // fprintf(stderr, "t2: ALTER\n");
  return (DB_SUCCESS);
}

/** INSERT INTO t2 VALUES (<rand 1-100>, 0, run_number, 0)
@return DB_SUCCESS or error code */
static DbErr insert_t2(System &sys, void *arg) /*!< in: arguments for callback */
{
  int i;
  DbErr err;

  Cursor * crsr = nullptr;
  Tuple * tpl = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: INSERT\n");

  err = open_table(sys, tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->lock(sys, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->set_lock_mode(sys, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < cb_arg->batch_size; ++i) {

    int val = 0;
    int zero = 0;

    val = random() % key_range;
    err = tpl->set_value(0, &val, 4);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(1, &zero, 4);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(2, &cb_arg->run_number, 4);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(3, &zero, 4);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }
    update_err_stats(cb_arg->err_st, err);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  goto clean_exit;

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  if (crsr != nullptr) {
    DbErr err2;

    err2 = Cursor::close(sys, crsr);
    assert(err2 == DB_SUCCESS);
    crsr = nullptr;
  }

  return (err);
}

/** UPDATE t2 SET score = score + 100 AND upd_run = run_number
WHERE c1 == 5
@return DB_SUCCESS or error code */
static DbErr update_t2(System &sys, void *arg) /*!< in: arguments for callback */
{
  DbErr err;
  int res = ~0L;
  uint32_t five = 5;
  Tuple * key_tpl = nullptr;
  Tuple * old_tpl = nullptr;
  Tuple * new_tpl = nullptr;
  Cursor * crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: UPDATE\n");

  err = open_table(sys, tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->lock(sys, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->set_lock_mode(sys, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->set_lock_mode(sys, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  /* Create a tuple for searching an index. */
  key_tpl = crsr->sec_search_tuple_create();
  assert(key_tpl != nullptr);

  /* Set the value to look for. */
  err = key_tpl->set_value(0, &five, 4);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);

  Tuple::destroy(key_tpl);

  if (res != 0) {
    goto clean_exit;
  } else if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = crsr->clust_read_tuple_create();
  assert(old_tpl != nullptr);

  new_tpl = crsr->clust_read_tuple_create();
  assert(new_tpl != nullptr);

  /* Iterate over the records while the first column matches "a". */
  while (1) {
    uint32_t score;
    uint32_t val;
    ulint data_len;
    ColMeta col_meta;

    err = crsr->read_row(sys, old_tpl);
    assert(err == DB_SUCCESS);

    old_tpl->get_meta(0, &col_meta);

    err = old_tpl->read_u32(0, &val);
    assert(err == DB_SUCCESS);

    if (val != five) {
      goto clean_exit;
    }

    /* Copy the old contents to the new tuple. */
    err = new_tpl->copy(old_tpl);

    /* Update the score column in the new tuple. */
    data_len = old_tpl->get_meta(1, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = old_tpl->read_u32(1, &score);
    assert(err == DB_SUCCESS);
    score += 100;

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_u32(1, score);
    assert(err == DB_SUCCESS);

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_u32(3, cb_arg->run_number);
    assert(err == DB_SUCCESS);

    err = crsr->update_row(sys, old_tpl, new_tpl);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }
    update_err_stats(cb_arg->err_st, err);

    /* Move to the next record to update. */
    err = crsr->next(sys);
    if (err != DB_SUCCESS) {
      goto err_exit;
    }

    /* Reset the old and new tuple instances. */
    old_tpl = old_tpl->clear();
    assert(old_tpl != nullptr);

    new_tpl = new_tpl->clear();
    assert(new_tpl != nullptr);
  }

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:
  if (old_tpl != nullptr) {
    Tuple::destroy(old_tpl);
  }
  if (new_tpl != nullptr) {
    Tuple::destroy(new_tpl);
  }

  if (crsr != nullptr) {
    DbErr err2;

    err2 = Cursor::close(sys, crsr);
    assert(err2 == DB_SUCCESS);
    crsr = nullptr;
  }

  return (err);
}

/** DELETE FROM t2 WHERE c1 == 9
@return DB_SUCCESS or error code */
static DbErr delete_t2(System &sys, void *arg) /*!< in: arguments for callback */
{
  DbErr err;
  int res = ~0L;
  int nine = 9;
  Tuple * key_tpl = nullptr;
  Cursor * crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t2: DELETE\n");

  err = open_table(sys, tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->lock(sys, IB_LOCK_IX);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  err = crsr->set_lock_mode(sys, IB_LOCK_X);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* Create a tuple for searching an index. */
  key_tpl = crsr->sec_search_tuple_create();
  assert(key_tpl != nullptr);

  /* Set the value to delete. */
  err = key_tpl->set_value(0, &nine, 4);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);
  if (res != 0) {
    goto clean_exit;
  }

  if (err != DB_SUCCESS) {
    goto err_exit;
  }

  /* InnoDB handles the updating of all secondary indexes. */
  err = crsr->delete_row(sys);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }
  update_err_stats(cb_arg->err_st, err);
  goto clean_exit;

err_exit:
  update_err_stats(cb_arg->err_st, err);

clean_exit:

  if (key_tpl != nullptr) {
    Tuple::destroy(key_tpl);
  }

  if (crsr != nullptr) {
    DbErr err2;

    err2 = Cursor::close(sys, crsr);
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
