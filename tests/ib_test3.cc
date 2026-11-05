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

/* Test handling of integer types by the API
 Create a database
 CREATE TABLE T8(c1 INT8, c UINT8);
 CREATE TABLE T16(c1 INT16, c UINT16);
 CREATE TABLE T32(c1 INT32, c UINT32);
 CREATE TABLE T64(c1 INT64, c UINT64);

 INSERT INT Tn VALUES(1, -1);
 INSERT INT Tn VALUES(100, -100);

 SELECT c1, c2 FROM Tn;

TODO: Test limits, SQL_NULL and data mismatch handling.

 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
        vchar	VARCHAR(128),
        blob	VARCHAR(n),
        count	INT,
        PRIMARY KEY(vchar); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table name */
                             int size)           /*!< in: int size in bits */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  assert(size == 8 || size == 16 || size == 32 || size == 64);

  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);

  err = TableSchema::create(table_name, &ib_tbl_sch, IB_TBL_V1);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0,
                                size / 8);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_INT, IB_COL_UNSIGNED, 0,
                                size / 8);

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
                           int size,           /*!< in: int size in bits */
                           Trx * ib_trx,    /*!< in: transaction */
                           Cursor **crsr)    /*!< out: innodb cursor */
{
  DbErr err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  assert(size == 8 || size == 16 || size == 32 || size == 64);

  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);

  err = api::cursor_open_table(sys, table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO Tn VALUE(1, -1);
INSERT INTO Tn VALUE(100, -100); */
static DbErr insert(System &sys, Cursor * crsr, /*!< in, out: cursor to use for write */
                       int size)       /*!< in: int size in bits */
{
  int i;
  DbErr err;
  Tuple * tpl;

  assert(size == 8 || size == 16 || size == 32 || size == 64);

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < 100; ++i) {
    switch (size) {
    case 8: {
      uint8_t u8 = (i + 1);
      int8_t i8 = (i + 1) * -1;

      err = tpl->write_i8(0, i8);
      assert(err == DB_SUCCESS);

      err = tpl->write_u8(1, u8);
      assert(err == DB_SUCCESS);
      break;
    }
    case 16: {
      uint16_t u16 = (i + 1);
      int16_t i16 = (i + 1) * -1;

      err = tpl->write_i16(0, i16);
      assert(err == DB_SUCCESS);

      err = tpl->write_u16(1, u16);
      assert(err == DB_SUCCESS);
      break;
    }
    case 32: {
      uint32_t u32 = (i + 1);
      int32_t i32 = (i + 1) * -1;

      err = tpl->write_i32(0, i32);
      assert(err == DB_SUCCESS);

      err = tpl->write_u32(1, u32);
      assert(err == DB_SUCCESS);
      break;
    }
    case 64: {
      uint64_t u64 = (i + 1);
      int64_t i64 = (i + 1) * -1;

      err = tpl->write_i64(0, i64);
      assert(err == DB_SUCCESS);

      err = tpl->write_u64(1, u64);
      assert(err == DB_SUCCESS);
      break;
    }
    default:
      assert(0);
    }

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

/** Check int  columns in a tuple. */
static void read_col(const Tuple * tpl, int i, ColMeta *col_meta,
                     int *c) {
  DbErr err = DB_SUCCESS;

  switch (col_meta->type_len) {
  case 1: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint8_t u8;

      err = tpl->read_u8(i, &u8);
      *c += u8;
    } else {
      int8_t i8;

      err = tpl->read_i8(i, &i8);
      *c -= i8;
    }
    break;
  }
  case 2: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint16_t u16;

      err = tpl->read_u16(i, &u16);
      *c += u16;
    } else {
      int16_t i16;

      err = tpl->read_i16(i, &i16);
      *c -= i16;
    }
    break;
  }
  case 4: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint32_t u32;

      err = tpl->read_u32(i, &u32);
      *c += u32;
    } else {
      int32_t i32;

      err = tpl->read_i32(i, &i32);
      *c -= i32;
    }
    break;
  }
  case 8: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint64_t u64;

      err = tpl->read_u64(i, &u64);
      *c += u64;
    } else {
      int64_t i64;

      err = tpl->read_i64(i, &i64);
      *c -= i64;
    }
    break;
  }
  default:
    assert(0);
    break;
  }
  assert(err == DB_SUCCESS);
}

/** Check that the column value read matches what was written. */
static void check_row(Tuple * tpl, int *c1, int *c2) {
  ColMeta col_meta;

  tpl->get_meta(0, &col_meta);
  assert(col_meta.type == IB_INT);
  assert(!(col_meta.attr & IB_COL_UNSIGNED));

  read_col(tpl, 0, &col_meta, c1);

  tpl->get_meta(1, &col_meta);
  assert(col_meta.type == IB_INT);
  assert(col_meta.attr & IB_COL_UNSIGNED);

  read_col(tpl, 1, &col_meta, c2);
}

/** SELECT * FROM T; */
static DbErr read_rows(System &sys, Cursor * crsr) {
  Tuple * tpl;
  DbErr err;
  int c1 = 0;
  int c2 = 0;

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

    check_row(tpl, &c1, &c2);

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

  assert(c1 == c2);

  return (err);
}

/** Drop the table. */
static DbErr drop_table_n(System &sys, const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table to drop */
                             int size)           /*!< in: int size in bits */
{
  DbErr err;
  Trx * ib_trx;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_drop(sys, ib_trx, table_name);
  assert(err == DB_SUCCESS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  return (err);
}

int main(int argc, char *argv[]) {
  int i;
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;
  int size = 8;

  (void)argc;
  (void)argv;

  srandom(time(nullptr));

  Config config;

  test_configure(config);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < 4; ++i, size <<= 1) {

    printf("Create table %d\n", size);
    err = create_table(sys, DATABASE, TABLE, size);
    assert(err == DB_SUCCESS);

    printf("Begin transaction\n");
    ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, size, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = crsr->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    insert(sys, crsr, size);
    read_rows(sys, crsr);

    printf("Close cursor\n");
    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    printf("Commit transaction\n");
    err = api::trx_commit(sys, ib_trx);
    assert(err == DB_SUCCESS);

    printf("Drop table\n");
    err = drop_table_n(sys, DATABASE, TABLE, size);
    assert(err == DB_SUCCESS);
  }

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
