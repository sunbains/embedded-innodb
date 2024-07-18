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

/* Simple single threaded test that does the equivalent of:
 Create a database
 CREATE TABLE T(
        c1 VARCHAR(n),
        c2 INT,
        C3 FLOAT,
        C4 DOUBLE,
        C5 DECIMAL,
        PRIMARY KEY(c1, 4));
 INSERT INTO T VALUES('xxxxaaaa', 1);
 INSERT INTO T VALUES('xxxxbbbb', 2); -- should result in duplicate key

 Test whether we catch attempts to create prefix length indexes on
 INT, FLOAT, DOUBLE and DECIMAL column types.

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
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        C1	VARCHAR(10),
        c2 	INT,
        C3 	FLOAT,
        C4 	DOUBLE,
        C5 	DECIMAL,
        PRIMARY KEY(C1, 4); */
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  ib_idx_sch_t ib_idx_sch = nullptr;
  ib_tbl_fmt_t tbl_fmt = IB_TBL_COMPACT;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  assert(err == DB_SUCCESS);

  err =
      ib_table_schema_add_col(ib_tbl_sch, "c1", IB_VARCHAR, IB_COL_NONE, 0, 10);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_INT, IB_COL_NONE, 0,
                                sizeof(ib_i32_t));

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_FLOAT, IB_COL_NONE, 0,
                                sizeof(float));

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c4", IB_DOUBLE, IB_COL_NONE, 0,
                                sizeof(double));

  assert(err == DB_SUCCESS);

  err =
      ib_table_schema_add_col(ib_tbl_sch, "c5", IB_DECIMAL, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "c1", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 4 for C1. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 4);
  assert(err == DB_SUCCESS);

  /* This should fail. */
  err = ib_index_schema_add_col(ib_idx_sch, "c2", 2);
  assert(err == DB_SCHEMA_ERROR);

  /* This should fail. */
  err = ib_index_schema_add_col(ib_idx_sch, "c3", 2);
  assert(err == DB_SCHEMA_ERROR);

  /* This should fail. */
  err = ib_index_schema_add_col(ib_idx_sch, "c4", 2);
  assert(err == DB_SCHEMA_ERROR);

  /* This should fail. */
  err = ib_index_schema_add_col(ib_idx_sch, "c5", 2);
  assert(err == DB_SCHEMA_ERROR);

  err = ib_index_schema_set_clustered(ib_idx_sch);
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
                           ib_trx_t ib_trx,    /*!< in: transaction */
                           ib_crsr_t *crsr)    /*!< out: innodb cursor */
{
  ib_err_t err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE('xxxxaaaa', 1);
INSERT INTO T VALUE('xxxxbbbb', 2); */
static void insert_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  ib_tpl_t tpl = nullptr;
  ib_err_t err = DB_ERROR;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  err = ib_col_set_value(tpl, 0, "xxxxaaaa", 8);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 2);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_SUCCESS);

  /* Insert a record with the same prefix as the one inserted above. */
  err = ib_col_set_value(tpl, 0, "xxxxbbbb", 8);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 2);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_DUPLICATE_KEY);

  ib_tuple_delete(tpl);
}

int main(int argc, char *argv[]) {
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;
  ib_u64_t version;

  (void)argc;
  (void)argv;

  version = ib_api_version();
  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  err = ib_startup("barracuda");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = open_table(DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  insert_rows(crsr);

  err = ib_cursor_close(crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  err = drop_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
