/***************************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith
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

/* Tests getting the name of the index that caused a duplicate key error */

#include <stdio.h>
#include <assert.h>


#include "test0aux.h"

#include <stdarg.h>
#include <string.h>

#define DATABASE "test"
#define TABLE "ib_duplicate_key_name"
#define TABLE_PATH DATABASE "/" TABLE

#define PRIMARY_INDEX_NAME "PRIMARY"
#define SEC_INDEX_NAME "c2_idx"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(C1 INT, C2 INT, PK(C1), UNIQUE(C2)); */
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

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0,
                                sizeof(int32_t));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_INT, IB_COL_NONE, 0,
                                sizeof(int32_t));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index(PRIMARY_INDEX_NAME, &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index(SEC_INDEX_NAME, &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c2", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_unique();
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);

  if (err == DB_SUCCESS) {
    err = api::trx_commit(sys, ib_trx);
  } else {
    fprintf(stderr, "Table: %s create failed: %s\n", table_name,
            db_strerror(err));

    err = api::trx_rollback(sys, ib_trx);
  }
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

/** INSERT INTO T VALUE(0, RANDOM(TEXT), RANDOM(TEXT)); ... 100 */
static DbErr insert_rows(System &sys, Trx * trx, Cursor * crsr) {
  DbErr err;
  Tuple * tpl;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  /* INSERT (1, 1) */

  err = tpl->write_i32(0, 1);
  assert(err == DB_SUCCESS);

  err = tpl->write_i32(1, 1);
  assert(err == DB_SUCCESS);

  err = crsr->insert_row(sys, tpl);
  assert(err == DB_SUCCESS);

  tpl = tpl->clear();
  assert(tpl != nullptr);

  /* INSERT (2, 2) */

  err = tpl->write_i32(0, 2);
  assert(err == DB_SUCCESS);

  err = tpl->write_i32(1, 2);
  assert(err == DB_SUCCESS);

  err = crsr->insert_row(sys, tpl);
  assert(err == DB_SUCCESS);

  tpl = tpl->clear();
  assert(tpl != nullptr);

  /* INSERT (1,3) <- fail on pkey */
  err = tpl->write_i32(0, 1);
  assert(err == DB_SUCCESS);

  err = tpl->write_i32(1, 3);
  assert(err == DB_SUCCESS);

  err = crsr->insert_row(sys, tpl);
  assert(err == DB_DUPLICATE_KEY);

  const char *d_table;
  const char *d_index;
  err = api::get_duplicate_key(trx, &d_table, &d_index);
  assert(err == DB_SUCCESS);
  printf("DB_DUPLICATE_KEY: TABLE: %s  INDEX: %s\n", d_table, d_index);

  assert(strcmp(d_table, TABLE_PATH) == 0);
  assert(strcmp(d_index, PRIMARY_INDEX_NAME) == 0);

  tpl = tpl->clear();
  assert(tpl != nullptr);

  /* insert (3,1) <- fail on secondary */
  err = tpl->write_i32(0, 3);
  assert(err == DB_SUCCESS);

  err = tpl->write_i32(1, 1);
  assert(err == DB_SUCCESS);

  err = crsr->insert_row(sys, tpl);
  assert(err == DB_DUPLICATE_KEY);

  err = api::get_duplicate_key(trx, &d_table, &d_index);
  assert(err == DB_SUCCESS);
  printf("DB_DUPLICATE_KEY: TABLE: %s  INDEX: %s\n", d_table, d_index);

  assert(strcmp(d_table, TABLE_PATH) == 0);
  assert(strcmp(d_index, SEC_INDEX_NAME) == 0);

  tpl = tpl->clear();
  assert(tpl != nullptr);

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  return (DB_SUCCESS);
}

int main(int argc, char **argv) {
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

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  err = insert_rows(sys, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  return (0);
}
