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

/* Multi-threaded test that does the equivalent of:
 Create a database
 FOR 1 TO 10

   CREATE TABLE T(
        c1 	VARCHAR(128),
        c2 	BLOB,
        c3 	INT,
        PRIMARY KEY(c1),
        INDEX(c3));

   INSERT INTO T VALUES(RANDOM(STRING), RANDOM(DATA), MOD(RANDOM(INT), 10));
   ...
   FOR 1 TO 1000
     SELECT * FROM T;
     UPDATE T SET c1 = RANDOM(string), c3 = MOD(c3 + 1, 10)
        WHERE c3 = MOD(RANDOM(INT), 10);
     SELECT * FROM T WHERE c1 LIKE RANDOM(STRING);
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
#define TABLE "t"

/** Create an InnoDB database (sub-directory). */
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        c1	VARCHAR(128),
        c2	BLOB,
        c3	INT,
        PRIMARY KEY(c1),
        INDEX(c3)); */
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

  err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_VARCHAR, IB_COL_NONE, 0,
                                128);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_INT, IB_COL_NONE, 0, 4);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(ib_idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_unique(ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Create secondary index on c3. */
  err = ib_table_schema_add_index(ib_tbl_sch, "c3", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c3", 0);
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

/** INSERT INTO T VALUE(RANDOM(TEXT), RANDOM(TEXT), 0); */
static ib_err_t
insert_random_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  ib_i32_t i;
  ib_err_t err;
  ib_tpl_t tpl;
  char *ptr = (char *)malloc(8192);

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  for (i = 0; i < 100; ++i) {
    int l;

    l = gen_rand_text(ptr, 128);
    err = ib_col_set_value(tpl, 0, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 8192);
    err = ib_col_set_value(tpl, 1, ptr, l);
    assert(err == DB_SUCCESS);

    err = ib_tuple_write_i32(tpl, 2, i % 10);
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  free(ptr);

  return (err);
}

/** UPDATE T SET c1 = RANDOM(string), c3 = MOD(c3 + 1, 10)
        WHERE c3 = MOD(RANDOM(INT), 10); */
static ib_err_t update_random_rows(ib_crsr_t crsr) {
  ib_i32_t c3;
  ib_err_t err;
  ib_i32_t key;
  int res = ~0L;
  ib_crsr_t index_crsr;
  ib_tpl_t sec_key_tpl;

  /* Open the secondary index. */
  err = ib_cursor_open_index_using_name(crsr, "c3", &index_crsr);
  assert(err == DB_SUCCESS);

  /* Set the record lock mode */
  err = ib_cursor_set_lock_mode(index_crsr, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  /* Since we will be updating the clustered index record, set the
  need to access clustered index flag in the cursor. */
  ib_cursor_set_cluster_access(index_crsr);

  /* Create a tuple for searching the secondary index. */
  sec_key_tpl = ib_sec_search_tuple_create(index_crsr);
  assert(sec_key_tpl != nullptr);

  /* Set the value to look for. */
  key = random() % 10;
  err = ib_tuple_write_i32(sec_key_tpl, 0, key);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(index_crsr, sec_key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
         err == DB_RECORD_NOT_FOUND);

  ib_tuple_delete(sec_key_tpl);

  /* Match found */
  if (res == 0) {
    int l;
    char *ptr;
    const char *first;
    ib_ulint_t data_len;
    ib_col_meta_t col_meta;
    ib_tpl_t old_tpl = nullptr;
    ib_tpl_t new_tpl = nullptr;

    /* Create the tuple instance that we will use to update the
    table. old_tpl is used for reading the existing row and
    new_tpl will contain the update row data. */

    old_tpl = ib_clust_read_tuple_create(crsr);
    assert(old_tpl != nullptr);

    new_tpl = ib_clust_read_tuple_create(crsr);
    assert(new_tpl != nullptr);

    err = ib_cursor_read_row(index_crsr, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the first column value. */
    first = static_cast<const char *>(ib_col_get_value(old_tpl, 0));
    ib_col_get_meta(old_tpl, 0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(first != nullptr);

    /* Copy the old contents to the new tuple. */
    err = ib_tuple_copy(new_tpl, old_tpl);

    /* Update the c3 column in the new tuple. */
    data_len = ib_col_get_meta(old_tpl, 2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = ib_tuple_read_i32(old_tpl, 2, &c3);
    assert(err == DB_SUCCESS);
    assert(c3 == key);
    c3 = (c3 + 1) % 10;

    ptr = (char *)malloc(8192);
    l = gen_rand_text(ptr, 128);

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 8192);
    /* Set the new key value in the new tuple. */
    err = ib_col_set_value(new_tpl, 0, ptr, l);
    assert(err == DB_SUCCESS);

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 8192);
    /* Set the c2 value in the new tuple. */
    err = ib_col_set_value(new_tpl, 1, ptr, l);
    assert(err == DB_SUCCESS);

    /* Set the updated c3 value in the new tuple. */
    err = ib_tuple_write_i32(new_tpl, 2, c3);
    assert(err == DB_SUCCESS);

    /* NOTE: We are using the secondary index cursor to update
    the record and not the cluster index cursor. */
    err = ib_cursor_update_row(index_crsr, old_tpl, new_tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

    /* Reset the old and new tuple instances. */
    old_tpl = ib_tuple_clear(old_tpl);
    assert(old_tpl != nullptr);

    new_tpl = ib_tuple_clear(new_tpl);
    assert(new_tpl != nullptr);

    free(ptr);

    ib_tuple_delete(old_tpl);
    ib_tuple_delete(new_tpl);
  }

  err = ib_cursor_close(index_crsr);
  assert(err == DB_SUCCESS);

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
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 1;
  ++count;

  while ((opt = getopt_long(argc, argv, "", longopts, nullptr)) != -1) {
    switch (opt) {

    case USER_OPT + 1:
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

  srandom(time(nullptr));

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
      assert(ib_trx != nullptr);

      err = open_table(DATABASE, TABLE, ib_trx, &crsr);
      assert(err == DB_SUCCESS);

      err = ib_cursor_lock(crsr, IB_LOCK_IX);
      assert(err == DB_SUCCESS);

      insert_random_rows(crsr);

      update_random_rows(crsr);

      err = ib_cursor_close(crsr);
      assert(err == DB_SUCCESS);
      crsr = nullptr;

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
