/** Copyright (c) 2008 Innobase Oy. All rights reserved.
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
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE D.Tn(C1 INT); */
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table name */
                             int n)              /*!< in: table suffix */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
  sprintf(table_name, "%s/%s%d", dbname, name, n);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);
#endif

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(ib_u32_t));

  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
  assert(err == DB_SUCCESS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  return (err);
}

/** Open a table and return a cursor for the table. */
static ib_err_t open_table(const char *dbname, /*!< in: database name */
                           const char *name,   /*!< in: table name */
                           int n,              /*!< in: table suffix */
                           ib_trx_t ib_trx,    /*!< in: transaction */
                           ib_crsr_t *crsr)    /*!< out: innodb cursor */
{
  ib_err_t err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
  sprintf(table_name, "%s/%s%d", dbname, name, n);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);
#endif
  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

int main(int argc, char *argv[]) {
  int i;
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;

  (void)argc;
  (void)argv;

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  err = ib_startup("barracuda");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  /* Create the tables. */
  for (i = 0; i < 10; i++) {
    err = create_table(DATABASE, TABLE, i);
    assert(err == DB_SUCCESS);
  }

  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  /* Open and close the cursor. */
  for (i = 0; i < 10; i++) {
    err = open_table(DATABASE, TABLE, i, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = ib_cursor_close(crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;
  }

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_database_drop(DATABASE);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
