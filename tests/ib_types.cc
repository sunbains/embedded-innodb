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
static ib_err_t create_database(const char *name) {
  ib_bool_t err;

  err = ib_database_create(name);
  assert(err == IB_TRUE);

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
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = NULL;
  ib_idx_sch_t ib_idx_sch = NULL;
  char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
  sprintf(table_name, "%s/%s", dbname, name);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

  assert(err == DB_SUCCESS);

  err =
      ib_table_schema_add_col(ib_tbl_sch, "c1", IB_VARCHAR, IB_COL_NONE, 0, 10);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_INT, IB_COL_NOT_NULL, 0,
                                sizeof(ib_u32_t));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_FLOAT, IB_COL_NONE, 0,
                                sizeof(float));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c4", IB_DOUBLE, IB_COL_NONE, 0,
                                sizeof(double));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c5", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err =
      ib_table_schema_add_col(ib_tbl_sch, "c6", IB_DECIMAL, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
  assert(err == DB_SUCCESS);

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

  if (ib_tbl_sch != NULL) {
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

#ifdef __WIN__
  sprintf(table_name, "%s/%s", dbname, name);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif
  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE(c1, c2, c3, c4, c5, c6); */
static ib_err_t
insert_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  ib_err_t err;
  ib_tpl_t tpl = NULL;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  err = ib_col_set_value(tpl, 0, "abcdefghij", 10);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_float(tpl, 2, 2.0);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_double(tpl, 3, 3.0);
  assert(err == DB_SUCCESS);

  err = ib_col_set_value(tpl, 4, "BLOB", 4);
  assert(err == DB_SUCCESS);

  err = ib_col_set_value(tpl, 5, "1.23", 4);
  assert(err == DB_SUCCESS);

  /* Check for NULL constraint violation. */
  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_DATA_MISMATCH);

  /* Set column C2 value and retry operation. */
  err = ib_tuple_write_u32(tpl, 1, 1);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_SUCCESS);

  ib_tuple_delete(tpl);

  return (DB_SUCCESS);
}

/** SELECT * FROM T; */
static ib_err_t do_query(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t tpl;

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

    print_tuple(stdout, tpl);

    err = ib_cursor_next(crsr);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != NULL);
  }

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
  }

  if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }
  return (err);
}

int main(int argc, char *argv[]) {
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

  printf("Create table\n");
  err = create_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  printf("Begin transaction\n");
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != NULL);

  printf("Open cursor\n");
  err = open_table(DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  printf("Lock table in IX\n");
  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  printf("Insert rows\n");
  err = insert_rows(crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(crsr);
  assert(err == DB_SUCCESS);

  printf("Close cursor\n");
  err = ib_cursor_close(crsr);
  assert(err == DB_SUCCESS);
  crsr = NULL;

  printf("Commit transaction\n");
  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  printf("Drop table\n");
  err = drop_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
