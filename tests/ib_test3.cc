/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.

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
static ib_err_t create_database(const char *name) {
  ib_bool_t err;

  err = ib_database_create(name);
  assert(err == IB_TRUE);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        vchar	VARCHAR(128),
        blob	VARCHAR(n),
        count	INT,
        PRIMARY KEY(vchar); */
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table name */
                             int size)           /*!< in: int size in bits */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = NULL;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  assert(size == 8 || size == 16 || size == 32 || size == 64);

#ifdef __WIN__
  sprintf(table_name, "%s/%s%d", dbname, name, size);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);
#endif

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0,
                                size / 8);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_INT, IB_COL_UNSIGNED, 0,
                                size / 8);

  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
  assert(err == DB_SUCCESS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != NULL) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  return (err);
}

/** Open a table and return a cursor for the table. */
static ib_err_t open_table(const char *dbname, /*!< in: database name */
                           const char *name,   /*!< in: table name */
                           int size,           /*!< in: int size in bits */
                           ib_trx_t ib_trx,    /*!< in: transaction */
                           ib_crsr_t *crsr)    /*!< out: innodb cursor */
{
  ib_err_t err = DB_SUCCESS;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  assert(size == 8 || size == 16 || size == 32 || size == 64);

#ifdef __WIN__
  sprintf(table_name, "%s/%s%d", dbname, name, size);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);
#endif

  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO Tn VALUE(1, -1);
INSERT INTO Tn VALUE(100, -100); */
static ib_err_t insert(ib_crsr_t crsr, /*!< in, out: cursor to use for write */
                       int size)       /*!< in: int size in bits */
{
  int i;
  ib_err_t err;
  ib_tpl_t tpl;

  assert(size == 8 || size == 16 || size == 32 || size == 64);

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  for (i = 0; i < 100; ++i) {
    switch (size) {
    case 8: {
      ib_u8_t u8 = (i + 1);
      ib_i8_t i8 = (i + 1) * -1;

      err = ib_tuple_write_i8(tpl, 0, i8);
      assert(err == DB_SUCCESS);

      err = ib_tuple_write_u8(tpl, 1, u8);
      assert(err == DB_SUCCESS);
      break;
    }
    case 16: {
      ib_u16_t u16 = (i + 1);
      ib_i16_t i16 = (i + 1) * -1;

      err = ib_tuple_write_i16(tpl, 0, i16);
      assert(err == DB_SUCCESS);

      err = ib_tuple_write_u16(tpl, 1, u16);
      assert(err == DB_SUCCESS);
      break;
    }
    case 32: {
      ib_u32_t u32 = (i + 1);
      ib_i32_t i32 = (i + 1) * -1;

      err = ib_tuple_write_i32(tpl, 0, i32);
      assert(err == DB_SUCCESS);

      err = ib_tuple_write_u32(tpl, 1, u32);
      assert(err == DB_SUCCESS);
      break;
    }
    case 64: {
      ib_u64_t u64 = (i + 1);
      ib_i64_t i64 = (i + 1) * -1;

      err = ib_tuple_write_i64(tpl, 0, i64);
      assert(err == DB_SUCCESS);

      err = ib_tuple_write_u64(tpl, 1, u64);
      assert(err == DB_SUCCESS);
      break;
    }
    default:
      assert(0);
    }

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != NULL);
  }

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
  }

  return (err);
}

/** Check int  columns in a tuple. */
static void read_col(const ib_tpl_t tpl, int i, ib_col_meta_t *col_meta,
                     int *c) {
  ib_err_t err = DB_SUCCESS;

  switch (col_meta->type_len) {
  case 1: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u8_t u8;

      err = ib_tuple_read_u8(tpl, i, &u8);
      *c += u8;
    } else {
      ib_i8_t i8;

      err = ib_tuple_read_i8(tpl, i, &i8);
      *c -= i8;
    }
    break;
  }
  case 2: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u16_t u16;

      err = ib_tuple_read_u16(tpl, i, &u16);
      *c += u16;
    } else {
      ib_i16_t i16;

      err = ib_tuple_read_i16(tpl, i, &i16);
      *c -= i16;
    }
    break;
  }
  case 4: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u32_t u32;

      err = ib_tuple_read_u32(tpl, i, &u32);
      *c += u32;
    } else {
      ib_i32_t i32;

      err = ib_tuple_read_i32(tpl, i, &i32);
      *c -= i32;
    }
    break;
  }
  case 8: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u64_t u64;

      err = ib_tuple_read_u64(tpl, i, &u64);
      *c += u64;
    } else {
      ib_i64_t i64;

      err = ib_tuple_read_i64(tpl, i, &i64);
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
static void check_row(ib_tpl_t tpl, int *c1, int *c2) {
  ib_col_meta_t col_meta;

  ib_col_get_meta(tpl, 0, &col_meta);
  assert(col_meta.type == IB_INT);
  assert(!(col_meta.attr & IB_COL_UNSIGNED));

  read_col(tpl, 0, &col_meta, c1);

  ib_col_get_meta(tpl, 1, &col_meta);
  assert(col_meta.type == IB_INT);
  assert(col_meta.attr & IB_COL_UNSIGNED);

  read_col(tpl, 1, &col_meta, c2);
}

/** SELECT * FROM T; */
static ib_err_t read_rows(ib_crsr_t crsr) {
  ib_tpl_t tpl;
  ib_err_t err;
  int c1 = 0;
  int c2 = 0;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  err = ib_cursor_first(crsr);
  assert(err == DB_SUCCESS);

  while (err == DB_SUCCESS) {
    err = ib_cursor_read_row(crsr, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    check_row(tpl, &c1, &c2);

    print_tuple(stdout, tpl);

    err = ib_cursor_next(crsr);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);
  }

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
  }

  if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }

  assert(c1 == c2);

  return (err);
}

/** Drop the table. */
static ib_err_t drop_table_n(const char *dbname, /*!< in: database name */
                             const char *name,   /*!< in: table to drop */
                             int size)           /*!< in: int size in bits */
{
  ib_err_t err;
  ib_trx_t ib_trx;
  char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
  sprintf(table_name, "%s/%s%d", dbname, name, size);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, size);
#endif
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != NULL);

  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_drop(ib_trx, table_name);
  assert(err == DB_SUCCESS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  return (err);
}

int main(int argc, char *argv[]) {
  int i;
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;
  int size = 8;

  (void)argc;
  (void)argv;

#ifdef __WIN__
  srand((int)time(NULL));
#else
  srandom(time(NULL));
#endif

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  err = ib_startup("barracuda");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < 4; ++i, size <<= 1) {

    printf("Create table %d\n", size);
    err = create_table(DATABASE, TABLE, size);
    assert(err == DB_SUCCESS);

    printf("Begin transaction\n");
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = open_table(DATABASE, TABLE, size, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    insert(crsr, size);
    read_rows(crsr);

    printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    assert(err == DB_SUCCESS);
    crsr = NULL;

    printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    printf("Drop table\n");
    err = drop_table_n(DATABASE, TABLE, size);
    assert(err == DB_SUCCESS);
  }

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
