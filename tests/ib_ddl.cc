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
 CREATE TABLE T(c1 INT, c2 VARCHAR(10), c3 BLOB);
 INSERT INTO T VALUES(1, '1'); ...
 CREATE INDEX T_C1 ON T(c1);
 CREATE INDEX T_C2 ON T(c2);
 CREATE INDEX T_C2 ON T(c3(10));
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
#define TABLE "ib_ddl"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(C1 INT, C2 VARCHAR(10), C3 BLOB); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = TableSchema::create(table_name, &ib_tbl_sch, IB_TBL_V1, 0);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0,
                                sizeof(int32_t));

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_VARCHAR, IB_COL_NONE, 0, 10);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_BLOB, IB_COL_NONE, 0, 0);
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
static DbErr
insert_random_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  int32_t i;
  DbErr err;
  Tuple * tpl;
  char *ptr = new char[8192];

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < 100; ++i) {
    int l;

    err = tpl->write_i32(0, i % 10);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 10);
    err = tpl->set_value(1, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 8192);
    err = tpl->set_value(2, ptr, l);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  delete[] ptr;

  return (err);
}

/** Create a secondary indexes on a table.
@return	DB_SUCCESS or error code */
static DbErr create_sec_index(System &sys, const char *table_name, /*!< in: table name */
                                 const char *col_name,   /*!< in: column name */
                                 int prefix_len) /*!< in: prefix index length */

{
  DbErr err;
  Trx * ib_trx;
  IndexSchema * ib_idx_sch = nullptr;
  char index_name[IB_MAX_TABLE_NAME_LEN];

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  snprintf(index_name, sizeof(index_name), "%s_%s", table_name, col_name);
  err = IndexSchema::create(sys, ib_trx, index_name, table_name, &ib_idx_sch);

  assert(err == DB_SUCCESS);

  err = ib_idx_sch->add_col(col_name, prefix_len);
  assert(err == DB_SUCCESS);

  uint64_t index_id = 0;

  err = ib_idx_sch->create_index(sys, &index_id);
  assert(index_id > 0);

  if (ib_idx_sch != nullptr) {
    IndexSchema::destroy(ib_idx_sch);
    ib_idx_sch = nullptr;
  }

  if (err == DB_SUCCESS) {
    err = api::trx_commit(sys, ib_trx);
  } else {
    err = api::trx_rollback(sys, ib_trx);
  }
  assert(err == DB_SUCCESS);

  return (err);
}

/** Create secondary indexes on T(C1), T(C2), T(C3). */
static DbErr create_sec_index_1(System &sys, const char *dbname, /*!< in: database name */
                                   const char *name)   /*!< in: table to drop */
{
  DbErr err;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  err = create_sec_index(sys, table_name, "c1", 0);

  if (err == DB_SUCCESS) {
    err = create_sec_index(sys, table_name, "c2", 0);
  }

  if (err == DB_SUCCESS) {
    err = create_sec_index(sys, table_name, "c3", 10);
  }

  return (err);
}

/** Open the secondary index. */
static DbErr
open_sec_index(System &sys, Cursor * crsr,         /*!< in: table cusor */
               const char *index_name) /*!< in: sec. index to open */
{
  DbErr err;
  Cursor * idx_crsr;

  err = crsr->open_index(sys, index_name, &idx_crsr);
  assert(err == DB_SUCCESS);

  err = Cursor::close(sys, idx_crsr);
  assert(err == DB_SUCCESS);

  return (err);
}
/** Open the secondary indexes on T(C1), T(C2), T(C3). */
static DbErr open_sec_index_1(System &sys, const char *dbname, /*!< in: database name */
                                 const char *name)   /*!< in: table name */
{
  Cursor * crsr;
  Trx * ib_trx;
  DbErr err = DB_SUCCESS;
  char index_name[IB_MAX_TABLE_NAME_LEN];
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%.64s/%.64s", dbname, name);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  err = api::cursor_open_table(sys, table_name, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  snprintf(index_name, sizeof(index_name), "%.64s_%.64s", table_name, "c1");
  err = open_sec_index(sys, crsr, index_name);
  assert(err == DB_SUCCESS);

  snprintf(index_name, sizeof(index_name), "%.64s_%.64s", table_name, "c2");
  err = open_sec_index(sys, crsr, index_name);
  assert(err == DB_SUCCESS);

  snprintf(index_name, sizeof(index_name), "%.64s_%.64s", table_name, "c3");
  err = open_sec_index(sys, crsr, index_name);
  assert(err == DB_SUCCESS);

  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  return (err);
}

#define TEMP_INDEX_PREFIX '\377' /* from ut0ut.h */

/** Tests creating a index with a name that InnoDB uses for temporary indexs
@return	DB_SUCCESS or error code */
static DbErr
test_create_temp_index(System &sys, const char *dbname,
                       const char *name,     /*!< in: table name */
                       const char *col_name) /*!< in: column name */
{
  DbErr err;
  Trx * ib_trx;
  IndexSchema * ib_idx_sch = nullptr;
  char index_name[IB_MAX_TABLE_NAME_LEN];
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  snprintf(index_name, sizeof(index_name), "%c%.64s_%.64s", TEMP_INDEX_PREFIX,
           table_name, col_name);
  err = IndexSchema::create(sys, ib_trx, index_name, table_name, &ib_idx_sch);

  assert(err == DB_INVALID_INPUT);

  if (ib_idx_sch != nullptr) {
    IndexSchema::destroy(ib_idx_sch);
    ib_idx_sch = nullptr;
  }

  if (err == DB_SUCCESS) {
    err = api::trx_commit(sys, ib_trx);
  } else {
    err = api::trx_rollback(sys, ib_trx);
  }
  assert(err == DB_SUCCESS);

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

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  err = insert_random_rows(sys, crsr);
  assert(err == DB_SUCCESS);

  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = create_sec_index_1(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = test_create_temp_index(sys, DATABASE, TABLE, "c2");

  err = open_sec_index_1(sys, DATABASE, TABLE);
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
