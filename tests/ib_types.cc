/***********************************************************************
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
 CREATE TABLE T(
        c1 VARCHAR(n),
        c2 INT NOT NULL,
        c3 FLOAT,
        c4 DOUBLE,
        c5 BLOB,
        c6 DECIMAL,
        PK(c1));
 INSERT INTO T VALUES('x', 1, 2.0, 3.0, 'xxx'); ...
 SELECT * FROM T;
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

/** CREATE TABLE T(
        c1 VARCHAR(n),
        c2 INT NOT NULL,
        c3 FLOAT,
        c4 DOUBLE,
        c5 BLOB,
        c6 DECIMAL,
        PK(c1));  */
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

  /* Pass a table page size of 0, ie., use default page size. */
  err = TableSchema::create(table_name, &ib_tbl_sch, IB_TBL_V1, 0);

  assert(err == DB_SUCCESS);

  err =
      ib_tbl_sch->add_col("c1", IB_VARCHAR, IB_COL_NONE, 0, 10);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_INT, IB_COL_NOT_NULL, 0,
                                sizeof(uint32_t));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_FLOAT, IB_COL_NONE, 0,
                                sizeof(float));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c4", IB_DOUBLE, IB_COL_NONE, 0,
                                sizeof(double));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c5", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err =
      ib_tbl_sch->add_col("c6", IB_DECIMAL, IB_COL_NONE, 0, 0);
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

/** INSERT INTO T VALUE(c1, c2, c3, c4, c5, c6); */
static DbErr
insert_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  DbErr err;
  Tuple * tpl = nullptr;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  err = tpl->set_value(0, "abcdefghij", 10);
  assert(err == DB_SUCCESS);

  err = tpl->write_float(2, 2.0);
  assert(err == DB_SUCCESS);

  err = tpl->write_double(3, 3.0);
  assert(err == DB_SUCCESS);

  err = tpl->set_value(4, "BLOB", 4);
  assert(err == DB_SUCCESS);

  err = tpl->set_value(5, "1.23", 4);
  assert(err == DB_SUCCESS);

  /* Check for NULL constraint violation. */
  err = crsr->insert_row(sys, tpl);
  assert(err == DB_DATA_MISMATCH);

  /* Set column C2 value and retry operation. */
  err = tpl->write_u32(1, 1);
  assert(err == DB_SUCCESS);

  err = crsr->insert_row(sys, tpl);
  assert(err == DB_SUCCESS);

  Tuple::destroy(tpl);

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

int main(int argc, char *argv[]) {
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;

  (void)argc;
  (void)argv;

  Config config;

  test_configure(config);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  printf("Create table\n");
  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  printf("Begin transaction\n");
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  printf("Open cursor\n");
  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  printf("Lock table in IX\n");
  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  printf("Insert rows\n");
  err = insert_rows(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Close cursor\n");
  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  printf("Commit transaction\n");
  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  printf("Drop table\n");
  err = drop_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
