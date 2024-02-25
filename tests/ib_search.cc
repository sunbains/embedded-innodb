/** Copyright (c) 2010 Innobase Oy. All rights reserved.
Copyright (c) 2010 Oracle. All rights reserved.

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
 CREATE TABLE T(c1 VARCHAR(n), c2 VARCHAR(n), c3 INT, PRIMARY KEY(c1, c2));
 INSERT INTO T VALUES('abc', 'def', 1);
 INSERT INTO T VALUES('abc', 'zzz', 1);
 INSERT INTO T VALUES('ghi', 'jkl', 2);
 INSERT INTO T VALUES('mno', 'pqr', 3);
 INSERT INTO T VALUES('mno', 'xxx', 3);
 INSERT INTO T VALUES('stu', 'vwx', 4);
 SELECT * FROM T WHERE c1 = 'abc' AND c2 = 'def';
 SELECT * FROM T WHERE c1 = 'abc';
 SELECT * FROM T WHERE c1 >= 'g%';
 SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'x%';
 SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'z%';
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

/* A row from our test table. */
typedef struct row_t {
  char c1[32];
  char c2[32];
  ib_u32_t c3;
} row_t;

static row_t in_rows[] = {
    {"abc", "def", 1}, {"abc", "zzz", 1}, {"ghi", "jkl", 2}, {"mno", "pqr", 3},
    {"mno", "xxx", 3}, {"stu", "vwx", 4}, {"", "", 0}};

#define COL_LEN(n) (sizeof(((row_t *)0)->n))

/** Create an InnoDB database (sub-directory). */
static ib_err_t create_database(const char *name) {
  ib_bool_t err;

  err = ib_database_create(name);
  assert(err == IB_TRUE);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        c1	VARCHAR(n),
        c2	VARCHAR(n),
        c3	INT,
        PRIMARY KEY(c1, c2); */
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

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c1) - 1);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c2) - 1);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_INT, IB_COL_UNSIGNED, 0,
                                COL_LEN(c3));

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY_KEY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c2", 0);
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

/** INSERT INTO T VALUE('c1', 'c2', c3); */
static ib_err_t
insert_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  row_t *row;
  ib_tpl_t tpl = NULL;
  ib_err_t err = DB_ERROR;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  for (row = in_rows; *row->c1; ++row) {
    err = ib_col_set_value(tpl, 0, row->c1, strlen(row->c1));
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 1, row->c2, strlen(row->c2));
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 2, &row->c3, sizeof(row->c3));
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS);
  }

  assert(tpl != NULL);
  ib_tuple_delete(tpl);

  return (err);
}

/** SELECT * FROM T WHERE c1 = 'abc' AND c2 = 'def'; */
static ib_err_t do_moveto1(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t key_tpl;
  int res = ~0L;

  printf("SELECT * FROM T WHERE c1 = 'abc' AND c2 = 'def';\n");

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 0, "abc", 3);
  assert(err == DB_SUCCESS);
  err = ib_col_set_value(key_tpl, 1, "def", 3);
  assert(err == DB_SUCCESS);

  /* The InnoDB search function will not cache the next N records
  when this search mode is set. We should not try and do a cursor
  next/prev after this search. */
  ib_cursor_set_match_mode(crsr, IB_EXACT_MATCH);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  /* Should be no errors reported. */
  assert(err == DB_SUCCESS);

  /* Must be positioned on the record, since we've specified
  an exact match. */
  assert(res == 0);

  return (err);
}

/** SELECT * FROM T WHERE c1 = 'abc' AND c2 = 'def'; */
static ib_bool_t do_select1(ib_tpl_t tpl) {
  print_tuple(stdout, tpl);

  return (IB_FALSE);
}

/** SELECT * FROM T WHERE c1 = 'abc'; */
static ib_err_t do_moveto2(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t key_tpl;
  int res = ~0L;

  printf("SELECT * FROM T WHERE c1 = 'abc';\n");

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 0, "abc", 3);
  assert(err == DB_SUCCESS);

  /* The InnoDB search function will cache the next N records
  when this search mode is set. */
  ib_cursor_set_match_mode(crsr, IB_CLOSEST_MATCH);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  /* Should be no errors reported. */
  assert(err == DB_SUCCESS);

  /* Since we supplied an incomplete key it should be -1. */
  assert(res == -1);

  return (err);
}

/** SELECT * FROM T WHERE c1 = 'abc'; */
static ib_bool_t do_select2(ib_tpl_t tpl) {
  const char *c1;

  c1 = (const char*)ib_col_get_value(tpl, 0);

  /* There are no SQL_NULL values in our test data. */
  assert(c1 != NULL);

  if (strncmp(c1, "abc", 3) == 0) {
    print_tuple(stdout, tpl);

    return (IB_TRUE);
  }

  return (IB_FALSE);
}

/** SELECT * FROM T WHERE c1 >= 'g%'; */
static ib_err_t do_moveto3(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t key_tpl;
  int res = ~0L;

  printf("SELECT * FROM T WHERE c1 >= 'g%%';\n");

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 0, "g", 1);
  assert(err == DB_SUCCESS);

  /* The InnoDB search function will cache the next N records
  when this search mode is set. */
  ib_cursor_set_match_mode(crsr, IB_CLOSEST_MATCH);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  /* Should be no errors reported. */
  assert(err == DB_SUCCESS);

  /* Since we supplied an incomplete key it should be -1. */
  assert(res == -1);

  return (err);
}

/** SELECT * FROM T WHERE c1 >= 'g%'; */
static ib_bool_t do_select3(ib_tpl_t tpl) {
  const char *c1;

  c1 = (const char*)ib_col_get_value(tpl, 0);

  /* There are no SQL_NULL values in our test data. */
  assert(c1 != NULL);

  if (strncmp(c1, "g", 1) >= 0) {
    print_tuple(stdout, tpl);

    return (IB_TRUE);
  }

  return (IB_FALSE);
}

/** SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'x%'; */
static ib_err_t do_moveto4(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t key_tpl;
  int res = ~0L;

  printf("SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'x%%';\n");

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 0, "mno", 3);
  assert(err == DB_SUCCESS);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 1, "x", 1);
  assert(err == DB_SUCCESS);

  /* The InnoDB search function will cache the next N records
  when this search mode is set. */
  ib_cursor_set_match_mode(crsr, IB_EXACT_PREFIX);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  /* Should be no errors reported. */
  assert(err == DB_SUCCESS);

  /* Since we supplied an incomplete key it should be -1. */
  assert(res == -1);

  return (err);
}

/** SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'x%'; */
static ib_bool_t do_select4(ib_tpl_t tpl) {
  const char *c1;
  const char *c2;

  c1 = (const char*)ib_col_get_value(tpl, 0);
  c2 = (const char*)ib_col_get_value(tpl, 1);

  /* There are no SQL_NULL values in our test data. */
  assert(c1 != NULL);
  assert(c2 != NULL);

  if (strncmp(c1, "mno", 3) == 0 && strncmp(c2, "x", 1) >= 0) {
    print_tuple(stdout, tpl);

    return (IB_TRUE);
  }

  return (IB_FALSE);
}

/** SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'z%'; */
static ib_err_t do_moveto5(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t key_tpl;
  int res = ~0L;

  printf("SELECT * FROM T WHERE c1 = 'mno' AND c2 >= 'z%%';\n");

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 0, "mno", 3);
  assert(err == DB_SUCCESS);

  /* Set the value to move to. */
  err = ib_col_set_value(key_tpl, 1, "z", 1);
  assert(err == DB_SUCCESS);

  /* The InnoDB search function will cache the next N records
  when this search mode is set. */
  ib_cursor_set_match_mode(crsr, IB_EXACT_PREFIX);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);

  /* Record with given prefix doesn't exist in our test data. */
  assert(err == DB_RECORD_NOT_FOUND);

  return (err);
}

/** SELECT * FROM T <start from moveto()>; */
static ib_err_t do_query(ib_crsr_t crsr, ib_err_t (*moveto)(ib_crsr_t),
                         ib_bool_t (*select_func)(ib_tpl_t)) {
  ib_err_t err;
  ib_tpl_t tpl = NULL;

  err = moveto(crsr);

  if (err == DB_SUCCESS) {
    tpl = ib_clust_read_tuple_create(crsr);
    assert(tpl != NULL);
  }

  while (err == DB_SUCCESS) {
    err = ib_cursor_read_row(crsr, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    if (!select_func(tpl)) {
      break;
    }

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
  assert(ib_trx != NULL);

  err = open_table(DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  err = insert_rows(crsr);
  assert(err == DB_SUCCESS);

  err = do_query(crsr, do_moveto1, do_select1);
  assert(err == DB_SUCCESS);

  err = do_query(crsr, do_moveto2, do_select2);
  assert(err == DB_SUCCESS);

  err = do_query(crsr, do_moveto3, do_select3);
  assert(err == DB_SUCCESS);

  err = do_query(crsr, do_moveto4, do_select4);
  assert(err == DB_SUCCESS);

  /* There should be no records to select, pass NULL. */
  err = do_query(crsr, do_moveto5, NULL);
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
