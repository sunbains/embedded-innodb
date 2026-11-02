/****************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (C) 2010 Stewart Smith
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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#define DATABASE "test"
#define TABLE "t"
/* A row from our test table. */
typedef struct row_t {
  char c1[32];
  char c2[32];
  uint32_t c3;
} row_t;

#define COL_LEN(n) (sizeof(((row_t *)0)->n))
/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        c1	VARCHAR(n),
        c2	VARCHAR(n),
        c3	INT,
        PRIMARY KEY(c1, c2); */
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

  err = ib_tbl_sch->add_col("c1", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c1) - 1);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c2) - 1);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_INT, IB_COL_UNSIGNED, 0,
                                COL_LEN(c3));

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("c1_c2", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c2", 0);
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

  printf("Create table\n");
  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  printf("Begin transaction\n");
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  printf("Open cursor\n");
  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  printf("Test ib_cursor_open_index_using_name doesn't segv");
  Cursor * index_crsr;
  err = crsr->open_index(sys, "foobar", &index_crsr);

  assert(err == DB_TABLE_NOT_FOUND);

  printf("Close cursors\n");
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

  return (EXIT_SUCCESS);
}
