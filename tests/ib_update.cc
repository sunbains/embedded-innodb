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

/* Simple single threaded test that does the equivalent of:
 Create a database
 CREATE TABLE t(c1 INT, c2 VARCHAR(10), PK(c1));
 FOR I IN 1 ... 10 STEP 2
   INSERT INTO t VALUES(I, CHAR('a' + I)); ...
 END FOR
 SELECT * FROM t;
 UPDATE t SET c1 = c1 / 2;
 SELECT * FROM t;
 DROP TABLE t;

 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#include "test0aux.h"

#define DATABASE "test"
#define TABLE "t"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE t(c1 INT, c2 VARCHAR(10), PRIMARY KEY(c1); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * ib_idx_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  err = TableSchema::create(table_name, &ib_tbl_sch, IB_TBL_V1);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0, 4);

  assert(err == DB_SUCCESS);

  err =
      ib_tbl_sch->add_col("c2", IB_VARCHAR, IB_COL_NONE, 0, 10);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("c1", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  /* Create the table */
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);
  assert(err == DB_SUCCESS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    TableSchema::destroy(sys, ib_tbl_sch);
  }

  return (err);
}

/** Open a table and return a cursor for the table. */
static DbErr open_table(System &sys, const char *dbname, /*!< in: database name */
                           const char *name,   /*!< in: table name */
                           Trx * ib_trx,    /*!< in: transaction */
                           Cursor **crsr)    /*!< out: innodb cursor */
{
  DbErr err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  err = api::cursor_open_table(sys, table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE(I, CHAR('a' + I)); */
static DbErr
insert_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  int i;
  char ptr[] = "a";
  Tuple * tpl = nullptr;
  DbErr err = DB_ERROR;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < 10; i += 2, ++*ptr) {

    err = tpl->write_i32(0, i);
    assert(err == DB_SUCCESS);

    err = tpl->set_value(1, ptr, 1);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  return (err);
}

/** UPDATE t SET c1 = c1 / 2; */
static DbErr update_rows(System &sys, Cursor * crsr) {
  DbErr err;
  Tuple * old_tpl = nullptr;
  Tuple * new_tpl = nullptr;

  /* Set the record lock mode, since we are doing a SELECT FOR UPDATE */
  err = crsr->set_lock_mode(sys, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  /* Prepare for a table scan. */
  err = crsr->first(sys);
  assert(err == DB_SUCCESS);

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = crsr->clust_read_tuple_create();
  assert(old_tpl != nullptr);

  new_tpl = crsr->clust_read_tuple_create();
  assert(new_tpl != nullptr);

  /* Iterate over the records and update all records read. */
  while (err == DB_SUCCESS) {
    int c1;

    err = crsr->read_row(sys, old_tpl);
    assert(err == DB_SUCCESS);

    /* Copy the old contents to the new tuple. */
    err = new_tpl->copy(old_tpl);

    /* Get the first column value. */
    err = new_tpl->read_i32(0, &c1);
    assert(err == DB_SUCCESS);

    c1 /= 2;

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_i32(0, c1);
    assert(err == DB_SUCCESS);

    err = crsr->update_row(sys, old_tpl, new_tpl);
    assert(err == DB_SUCCESS);

    /* Move to the next record to update. */
    err = crsr->next(sys);
  }

  assert(err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND);

  if (old_tpl != nullptr) {
    Tuple::destroy(old_tpl);
  }
  if (new_tpl != nullptr) {
    Tuple::destroy(new_tpl);
  }

  return (DB_SUCCESS);
}

/** SELECT * FROM T; */
static DbErr do_query(System &sys, Cursor * crsr) {
  DbErr err;
  Tuple * tpl;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  err = crsr->first(sys);
  assert(err == DB_SUCCESS);

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    print_tuple(stdout, tpl);

    err = crsr->next(sys);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }
  return (err);
}

int main(int argc, char *argv[]) {
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;
  uint64_t version;

  (void)argc;
  (void)argv;

  version = api::api_version();
  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */

  Config config;

  test_configure(config);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  err = insert_rows(sys, crsr);
  assert(err == DB_SUCCESS);

  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Update all the rows\n");
  err = update_rows(sys, crsr);
  assert(err == DB_SUCCESS);

  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = drop_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
