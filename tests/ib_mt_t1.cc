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
CREATE TABLE t1
        (first		VARCHAR(128),
         last		VARCHAR(128),
         score		INT,
         ins_run	INT,
         upd_run	INT,
         PRIMARY KEY(first, last));
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

/** CREATE TABLE t1...
@return DB_SUCCESS or error code */
static DbErr create_t1(System &sys, void *arg) /*!< in: arguments for callback */
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

  err = ib_tbl_sch->add_col("first", IB_VARCHAR, IB_COL_NONE, 0,
                                128);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("last", IB_VARCHAR, IB_COL_NONE, 0,
                                128);
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

  err = ib_tbl_sch->add_index("first_last", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("first", 0);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("last", 0);
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
static DbErr alter_t1(System &sys, void *arg) /*!< in: arguments for callback */
{
  (void)arg;
  // fprintf(stderr, "t1: ALTER\n");
  return (DB_SUCCESS);
}

/** INSERT INTO t1 VALUES (<rand 1 char>, <rand 1 char>, 0, run_number, 0)
@return DB_SUCCESS or error code */
static DbErr insert_t1(System &sys, void *arg) /*!< in: arguments for callback */
{
  int i;
  DbErr err;
  int val = 0;
  int zero = 0;
  Cursor * crsr = nullptr;
  Tuple * tpl = nullptr;
  char *ptr = (char *)malloc(8192);
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t1: INSERT\n");
  assert(sizeof(val) == 4);

  err = open_table(sys, tbl->m_db_name.c_str(), tbl->m_name.c_str(), cb_arg->trx,
                   &crsr);
  if (err == DB_SUCCESS) {
    err = crsr->lock(sys, IB_LOCK_IX);
  }

  if (err == DB_SUCCESS) {
    err = crsr->set_lock_mode(sys, IB_LOCK_X);
  }

  if (err == DB_SUCCESS) {
    tpl = crsr->clust_read_tuple_create();
    if (tpl == nullptr) {
      err = DB_OUT_OF_MEMORY;
    }
  }

  for (i = 0; i < cb_arg->batch_size && err == DB_SUCCESS; ++i) {
    int l;

    l = gen_rand_text(ptr, 2);
    err = tpl->set_value(0, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 2);
    err = tpl->set_value(1, ptr, l);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(2, &zero, 4);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(3, &cb_arg->run_number, 4);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(4, &zero, 4);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);

    if (err == DB_SUCCESS) {
      update_err_stats(cb_arg->err_st, err);
      tpl = tpl->clear();
      assert(tpl != nullptr);
    }
  }

  if (err != DB_SUCCESS) {
    update_err_stats(cb_arg->err_st, err);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  if (crsr != nullptr) {
    DbErr cur_err;

    cur_err = Cursor::close(sys, crsr);
    assert(cur_err == DB_SUCCESS);
    crsr = nullptr;
  }

  free(ptr);

  return (err);
}

/** UPDATE t1 SET score = score + 100 AND upd_run = run_number
WHERE first == 'a'
@return DB_SUCCESS or error code */
static DbErr update_t1(System &sys, void *arg) /*!< in: arguments for callback */
{
  DbErr err;
  int res = ~0L;
  Tuple * key_tpl = nullptr;
  Tuple * old_tpl = nullptr;
  Tuple * new_tpl = nullptr;
  Cursor * crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t1: UPDATE\n");

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

  /* Set the value to look for. */
  err = key_tpl->set_value(0, "a", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);

  Tuple::destroy(key_tpl);

  if (err != DB_SUCCESS) {
    goto err_exit;
  }
  /* Must be positioned on a record that's greater than search key. */
  assert(res == -1);

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
    const char *first;
    ulint data_len;
    ColMeta col_meta;

    err = crsr->read_row(sys, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the first column value. */
    first = (const char *)old_tpl->get_value(0);
    old_tpl->get_meta(0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(first != nullptr);

    /* Only update first names that are == "a". */
    if (strncmp(first, "a", 1) != 0) {
      goto clean_exit;
    }

    /* Copy the old contents to the new tuple. */
    err = new_tpl->copy(old_tpl);

    /* Update the score column in the new tuple. */
    data_len = old_tpl->get_meta(2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = old_tpl->read_u32(2, &score);
    assert(err == DB_SUCCESS);
    score += 100;

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_u32(2, score);
    assert(err == DB_SUCCESS);

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_u32(4, cb_arg->run_number);
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

/** DELETE FROM t1 WHERE first == 'x' AND last == 'z'
@return DB_SUCCESS or error code */
static DbErr delete_t1(System &sys, void *arg) /*!< in: arguments for callback */
{
  DbErr err;
  int res = ~0L;
  Tuple * key_tpl = nullptr;
  Cursor * crsr = nullptr;
  cb_args_t *cb_arg = (cb_args_t *)arg;
  tbl_class_t *tbl = cb_arg->tbl;

  // fprintf(stderr, "t1: DELETE\n");

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
  err = key_tpl->set_value(0, "x", 1);
  assert(err == DB_SUCCESS);
  err = key_tpl->set_value(1, "z", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);
  if (err != DB_SUCCESS) {
    goto err_exit;
  }
  if (res != 0) {
    goto clean_exit;
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

void register_t1_table(tbl_class_t *tbl) /*!< in/out: table class to register */
{
  assert(tbl != nullptr);

  tbl->m_name.assign("t1");

  tbl->dml_fn[DML_OP_TYPE_INSERT] = insert_t1;
  tbl->dml_fn[DML_OP_TYPE_UPDATE] = update_t1;
  tbl->dml_fn[DML_OP_TYPE_DELETE] = delete_t1;

  tbl->ddl_fn[DDL_OP_TYPE_CREATE] = create_t1;
  tbl->ddl_fn[DDL_OP_TYPE_ALTER] = alter_t1;
}
