/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
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
 CREATE TABLE D.T1(c1 INT);
 ...
 CREATE TABLE D.Tn(c1 INT);
 DROP DATABASE D;

 InnoDB should drop all tables and remove the underlying directory.

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

#define DATABASE "drop_test"
#define TABLE "t"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE D.Tn(C1 INT); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table name */
                             int n)              /*!< in: table suffix */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);

  err = TableSchema::create(table_name, &ib_tbl_sch, IB_TBL_V1);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(uint32_t));

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
                           int n,              /*!< in: table suffix */
                           Trx * ib_trx,    /*!< in: transaction */
                           Cursor **crsr)    /*!< out: innodb cursor */
{
  DbErr err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);
  err = api::cursor_open_table(sys, table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

int main(int argc, char *argv[]) {
  int i;
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

  /* Create the tables. */
  for (i = 0; i < 10; i++) {
    err = create_table(sys, DATABASE, TABLE, i);
    assert(err == DB_SUCCESS);
  }

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  /* Open and close the cursor. */
  for (i = 0; i < 10; i++) {
    err = open_table(sys, DATABASE, TABLE, i, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;
  }

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::database_drop(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
