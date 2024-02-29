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
 CREATE TABLE T(c1 INT, PK(c1));
 INSERT INTO T VALUES(1); ...
 SELECT * FROM T;
 SELECT * FROM T WHERE c1 = 5;
 SELECT * FROM T WHERE c1 > 5;
 SELECT * FROM T WHERE c1 < 5;
 SELECT * FROM T WHERE c1 >= 1 AND c1 < 5;
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
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(c1 INT, PK(C1)); */
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

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0,
                                sizeof(int));

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

/** INSERT INTO T VALUES(0); ... 10 */
static ib_err_t
insert_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  int i;
  ib_err_t err;
  ib_tpl_t tpl = NULL;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  for (i = 0; i < 10; ++i) {
    err = ib_tuple_write_i32(tpl, 0, i);
    assert(err == DB_SUCCESS);

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

/** For all the select functions. */
static ib_err_t iterate(ib_crsr_t crsr, void *arg,
                        ib_err_t (*selector)(const ib_tpl_t, void *arg)) {
  ib_tpl_t tpl;
  ib_err_t err = DB_SUCCESS;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  while (err == DB_SUCCESS) {
    err = ib_cursor_read_row(crsr, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    err = selector(tpl, arg);

    if (err == DB_SUCCESS) {
      err = ib_cursor_next(crsr);
    }

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

static ib_err_t print_all(const ib_tpl_t tpl, void *arg) {
  (void)arg;

  print_tuple(stdout, tpl);

  return (DB_SUCCESS);
}

static ib_err_t print_eq_5(const ib_tpl_t tpl, void *arg) {
  int c1;
  ib_err_t err;

  (void)arg;

  err = ib_tuple_read_i32(tpl, 0, &c1);
  assert(err == DB_SUCCESS);

  if (c1 == 5) {
    print_tuple(stdout, tpl);
    return (DB_SUCCESS);
  }

  return (DB_END_OF_INDEX);
}

static ib_err_t print_lt_5(const ib_tpl_t tpl, void *arg) {
  int c1;
  ib_err_t err;

  (void)arg;

  err = ib_tuple_read_i32(tpl, 0, &c1);
  assert(err == DB_SUCCESS);

  if (c1 < 5) {
    print_tuple(stdout, tpl);
    return (DB_SUCCESS);
  }

  return (DB_END_OF_INDEX);
}

int main(int argc, char *argv[]) {
  int ret;
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;
  ib_tpl_t tpl = NULL;

  (void)argc;
  (void)argv;

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
  assert(ib_trx != NULL);

  err = open_table(DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  err = insert_rows(crsr);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T;\n");
  err = ib_cursor_first(crsr);
  assert(err == DB_SUCCESS);

  err = iterate(crsr, NULL, print_all);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 = 5;\n");
  tpl = ib_clust_search_tuple_create(crsr);
  assert(tpl != NULL);

  err = ib_tuple_write_i32(tpl, 0, 5);
  assert(err == DB_SUCCESS);

  err = ib_cursor_moveto(crsr, tpl, IB_CUR_GE, &ret);
  assert(err == DB_SUCCESS);
  assert(ret == 0);

  err = iterate(crsr, NULL, print_eq_5);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 > 5;\n");

  err = ib_cursor_moveto(crsr, tpl, IB_CUR_G, &ret);
  assert(err == DB_SUCCESS);
  assert(ret < 0);

  err = iterate(crsr, NULL, print_all);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 < 5;\n");
  err = ib_cursor_first(crsr);
  assert(err == DB_SUCCESS);

  err = iterate(crsr, NULL, print_lt_5);
  assert(err == DB_SUCCESS);

  printf("SELECT * FROM T WHERE c1 >= 1 AND c1 < 5;\n");
  tpl = ib_clust_search_tuple_create(crsr);
  assert(tpl != NULL);

  err = ib_tuple_write_i32(tpl, 0, 1);
  assert(err == DB_SUCCESS);

  err = ib_cursor_moveto(crsr, tpl, IB_CUR_GE, &ret);
  assert(err == DB_SUCCESS);
  assert(ret == 0);

  err = iterate(crsr, NULL, print_lt_5);
  assert(err == DB_SUCCESS);

  err = ib_cursor_close(crsr);
  assert(err == DB_SUCCESS);
  crsr = NULL;

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
