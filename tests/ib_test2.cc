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

/* Single threaded test that does the equivalent of:
 Create a database
 FOR 1 TO 10
   CREATE TABLE T(c1 VARCHAR(128), c2 BLOB, c3 INT, PK(c1));
   ...
   FOR 1 TO 10
     BEGIN;
     FOR 1 TO 100
      INSERT INTO T VALUES(RANDOM(STRING), RANDOM(STRING), 0);
     END
     UPDATE T SET c1 = RANDOM(string), c3 = c3 + 1 WHERE c1 = RANDOM(STRING);
     COMMIT;
   END
   DROP TABLE T;
 END

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
#define TABLE "ib_test2"

/* The page size for compressed tables, if this value is > 0 then
we create compressed tables. It's set via the command line parameter
--page-size INT */
static int page_size = 0;

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
                             const char *name)   /*!< in: table name */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = NULL;
  ib_idx_sch_t ib_idx_sch = NULL;
  ib_tbl_fmt_t tbl_fmt = IB_TBL_COMPACT;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  if (page_size > 0) {
    tbl_fmt = IB_TBL_COMPRESSED;

    printf("Creating compressed table with page size %d\n", page_size);
  }

  err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, page_size);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "vchar", IB_VARCHAR, IB_COL_NONE, 0,
                                128);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "blob", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "count", IB_INT, IB_COL_UNSIGNED, 0,
                                4);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "vchar", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(ib_idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_unique(ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
  if (err != DB_SUCCESS) {
    fprintf(stderr, "Warning: table create failed: %s\n", ib_strerror(err));
    err = ib_trx_rollback(ib_trx);
  } else {
    err = ib_trx_commit(ib_trx);
  }
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

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE(RANDOM(TEXT), RANDOM(TEXT), 0); */
static ib_err_t
insert_random_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  int i;
  ib_err_t err;
  ib_tpl_t tpl = NULL;
  char *ptr = new char [8192];

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  for (i = 0; i < 100; ++i) {
    int l;

    l = gen_rand_text(ptr, 128);
    err = ib_col_set_value(tpl, 0, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 8192);
    err = ib_col_set_value(tpl, 1, ptr, l);
    assert(err == DB_SUCCESS);

    err = ib_tuple_write_u32(tpl, 2, 0);
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != NULL);
  }

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
  }

  delete [] ptr;

  return (err);
}

/** UPDATE T SET score = score + 100 WHERE first = 'a'; */
static ib_err_t update_random_rows(ib_crsr_t crsr) {
  ib_err_t err;
  int l;
  char *ptr;
  int res = ~0L;
  ib_tpl_t key_tpl;
  ib_tpl_t old_tpl = NULL;
  ib_tpl_t new_tpl = NULL;

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != NULL);

  ptr = (char *)malloc(8192);

  l = gen_rand_text(ptr, 128);
  /* Set the value to look for. */
  err = ib_col_set_value(key_tpl, 0, ptr, l);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
         err == DB_RECORD_NOT_FOUND);

  if (key_tpl != NULL) {
    ib_tuple_delete(key_tpl);
  }

  /* Match found */
  if (res == 0) {
    ib_u32_t score;
    const char *first;
    ib_ulint_t data_len;
    ib_ulint_t first_len;
    ib_col_meta_t col_meta;

    /* Create the tuple instance that we will use to update the
    table. old_tpl is used for reading the existing row and
    new_tpl will contain the update row data. */

    old_tpl = ib_clust_read_tuple_create(crsr);
    assert(old_tpl != NULL);

    new_tpl = ib_clust_read_tuple_create(crsr);
    assert(new_tpl != NULL);

    err = ib_cursor_read_row(crsr, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the first column value. */
    first = static_cast<const char*>(ib_col_get_value(old_tpl, 0));
    first_len = ib_col_get_meta(old_tpl, 0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(first != NULL);

    /* Copy the old contents to the new tuple. */
    err = ib_tuple_copy(new_tpl, old_tpl);

    /* Update the score column in the new tuple. */
    data_len = ib_col_get_meta(old_tpl, 2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = ib_tuple_read_u32(old_tpl, 2, &score);
    assert(err == DB_SUCCESS);
    ++score;

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 128);
    /* Set the new key value in the new tuple. */
    err = ib_col_set_value(new_tpl, 0, ptr, l);
    assert(err == DB_SUCCESS);
    first_len = ib_col_get_len(new_tpl, 0);
    assert(first_len == IB_SQL_NULL || first_len <= 128);

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 8192);
    /* Set the blob value in the new tuple. */
    err = ib_col_set_value(new_tpl, 1, ptr, l);
    assert(err == DB_SUCCESS);

    /* Set the updated score value in the new tuple. */
    err = ib_tuple_write_u32(new_tpl, 2, score);
    assert(err == DB_SUCCESS);

    err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);
  }

  if (old_tpl != NULL) {
    ib_tuple_delete(old_tpl);
  }
  if (new_tpl != NULL) {
    ib_tuple_delete(new_tpl);
  }

  free(ptr);

  return (err);
}

/** Set the runtime global options. */
static void set_options(int argc, char *argv[]) {
  int opt;
  int size = 0;
  struct option *longopts;
  int count = 0;

  /* Count the number of InnoDB system options. */
  while (ib_longopts[count].name) {
    ++count;
  }

  /* Add one of our options and a spot for the sentinel. */
  size = sizeof(struct option) * (count + 2);
  longopts = (struct option *)malloc(size);
  memset(longopts, 0x0, size);
  memcpy(longopts, ib_longopts, sizeof(struct option) * count);

  /* Add the local parameter (page-size). */
  longopts[count].name = "page-size";
  longopts[count].has_arg = required_argument;
  longopts[count].flag = NULL;
  longopts[count].val = USER_OPT + 1;
  ++count;

  while ((opt = getopt_long(argc, argv, "", longopts, NULL)) != -1) {
    switch (opt) {

    case USER_OPT + 1:
      page_size = strtoul(optarg, NULL, 10);
      break;

    default:
      /* If it's an InnoDB parameter, then we let the
      auxillary function handle it. */
      if (set_global_option(opt, optarg) != DB_SUCCESS) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
      }

    } /* switch */
  }

  free(longopts);
}

int main(int argc, char *argv[]) {
  int i;
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;

  srandom(time(NULL));

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  set_options(argc, argv);

  err = ib_startup("barracuda");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < 10; ++i) {
    int j;

    printf("Create table\n");
    err = create_table(DATABASE, TABLE);
    assert(err == DB_SUCCESS);

    for (j = 0; j < 10; ++j) {
      ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
      assert(ib_trx != NULL);

      err = open_table(DATABASE, TABLE, ib_trx, &crsr);
      assert(err == DB_SUCCESS);

      err = ib_cursor_lock(crsr, IB_LOCK_IX);
      assert(err == DB_SUCCESS);

      insert_random_rows(crsr);

      update_random_rows(crsr);

      err = ib_cursor_close(crsr);
      assert(err == DB_SUCCESS);
      crsr = NULL;

      err = ib_trx_commit(ib_trx);
      assert(err == DB_SUCCESS);
    }

    printf("Drop table\n");
    err = drop_table(DATABASE, TABLE);
    assert(err == DB_SUCCESS);
  }

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
