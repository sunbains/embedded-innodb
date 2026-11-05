/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
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
 CREATE TABLE T(c1 INT, PK(c1));
 INSERT INTO T VALUES(1); ...
 SELECT * FROM T;
 SELECT * FROM T WHERE c1 = 5;
 SELECT * FROM T WHERE c1 > 5;
 SELECT * FROM T WHERE c1 < 5;
 SELECT * FROM T WHERE c1 >= 1 AND c1 < 5;
 DROP TABLE T;

 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"
#define TABLE "t"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(c1 INT, PK(C1)); */
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

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0,
                                sizeof(int));

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  /* create table */
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

/** INSERT INTO T VALUES(0); ... 10 */
static DbErr
insert_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  int i;
  DbErr err;
  Tuple * tpl = nullptr;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < 10; ++i) {
    err = tpl->write_i32(0, i);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  return (err);
}

/** For all the select functions. */
static DbErr iterate(System &sys, Cursor * crsr, void *arg,
                        DbErr (*selector)(const Tuple *, void *arg)) {
  Tuple * tpl;
  DbErr err = DB_SUCCESS;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    err = selector(tpl, arg);

    if (err == DB_SUCCESS) {
      err = crsr->next(sys);
    }

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }
  return (err);
}

static DbErr print_all(const Tuple * tpl, void *arg) {
  (void)arg;

  print_tuple(stdout, tpl);

  return (DB_SUCCESS);
}

static DbErr print_eq_5(const Tuple * tpl, void *arg) {
  int c1;
  DbErr err;

  (void)arg;

  err = tpl->read_i32(0, &c1);
  assert(err == DB_SUCCESS);

  if (c1 == 5) {
    print_tuple(stdout, tpl);
    return (DB_SUCCESS);
  }

  return (DB_END_OF_INDEX);
}

static DbErr print_lt_5(const Tuple * tpl, void *arg) {
  int c1;
  DbErr err;

  (void)arg;

  err = tpl->read_i32(0, &c1);
  assert(err == DB_SUCCESS);

  if (c1 < 5) {
    print_tuple(stdout, tpl);
    return (DB_SUCCESS);
  }

  return (DB_END_OF_INDEX);
}

int main(int argc, char *argv[]) {
  int ret;
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;
  Tuple * tpl = nullptr;

  (void)argc;
  (void)argv;

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

  printf("SELECT * FROM T;\n");
  err = crsr->first(sys);
  assert(err == DB_SUCCESS);

  err = iterate(sys, crsr, nullptr, print_all);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 = 5;\n");
  tpl = crsr->clust_search_tuple_create();
  assert(tpl != nullptr);

  err = tpl->write_i32(0, 5);
  assert(err == DB_SUCCESS);

  err = crsr->moveto(sys, tpl, IB_CUR_GE, &ret);
  assert(err == DB_SUCCESS);
  assert(ret == 0);

  err = iterate(sys, crsr, nullptr, print_eq_5);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 > 5;\n");

  err = crsr->moveto(sys, tpl, IB_CUR_G, &ret);
  assert(err == DB_SUCCESS);
  assert(ret < 0);

  err = iterate(sys, crsr, nullptr, print_all);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 < 5;\n");
  err = crsr->first(sys);
  assert(err == DB_SUCCESS);

  err = iterate(sys, crsr, nullptr, print_lt_5);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 >= 1 AND c1 < 5;\n");
  tpl = crsr->clust_search_tuple_create();
  assert(tpl != nullptr);

  err = tpl->write_i32(0, 1);
  assert(err == DB_SUCCESS);

  err = crsr->moveto(sys, tpl, IB_CUR_GE, &ret);
  assert(err == DB_SUCCESS);
  assert(ret == 0);

  err = iterate(sys, crsr, nullptr, print_lt_5);
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
