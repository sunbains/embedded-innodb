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
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        c1	VARCHAR(128),
        c2	BLOB,
        c3	INT,
        PRIMARY KEY(c1),
        INDEX(c3)); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * ib_idx_sch = nullptr;
  TableFormat tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_VARCHAR, IB_COL_NONE, 0,
                                128);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_INT, IB_COL_NONE, 0, 4);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_unique();
  assert(err == DB_SUCCESS);

  /* Create secondary index on c3. */
  err = ib_tbl_sch->add_index("c3", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c3", 0);
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

/** INSERT INTO T VALUE(RANDOM(TEXT), RANDOM(TEXT), 0); */
static DbErr
insert_random_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  int32_t i;
  DbErr err;
  Tuple * tpl;
  char *ptr = (char *)malloc(8192);

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = 0; i < 100; ++i) {
    int l;

    l = gen_rand_text(ptr, 128);
    err = tpl->set_value(0, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, 8192);
    err = tpl->set_value(1, ptr, l);
    assert(err == DB_SUCCESS);

    err = tpl->write_i32(2, i % 10);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  free(ptr);

  return (err);
}

/** UPDATE T SET c1 = RANDOM(string), c3 = MOD(c3 + 1, 10)
        WHERE c3 = MOD(RANDOM(INT), 10); */
static DbErr update_random_rows(System &sys, Cursor * crsr) {
  int32_t c3;
  DbErr err;
  int32_t key;
  int res = ~0L;
  Cursor * index_crsr;
  Tuple * sec_key_tpl;

  /* Open the secondary index. */
  err = crsr->open_index(sys, "c3", &index_crsr);
  assert(err == DB_SUCCESS);

  /* Set the record lock mode */
  err = index_crsr->set_lock_mode(sys, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  /* Since we will be updating the clustered index record, set the
  need to access clustered index flag in the cursor. */
  index_crsr->set_cluster_access();

  /* Create a tuple for searching the secondary index. */
  sec_key_tpl = index_crsr->sec_search_tuple_create();
  assert(sec_key_tpl != nullptr);

  /* Set the value to look for. */
  key = random() % 10;
  err = sec_key_tpl->write_i32(0, key);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = index_crsr->moveto(sys, sec_key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
         err == DB_RECORD_NOT_FOUND);

  Tuple::destroy(sec_key_tpl);

  /* Match found */
  if (res == 0) {
    int l;
    char *ptr;
    const char *first;
    ulint data_len;
    ColMeta col_meta;
    Tuple * old_tpl = nullptr;
    Tuple * new_tpl = nullptr;

    /* Create the tuple instance that we will use to update the
    table. old_tpl is used for reading the existing row and
    new_tpl will contain the update row data. */

    old_tpl = crsr->clust_read_tuple_create();
    assert(old_tpl != nullptr);

    new_tpl = crsr->clust_read_tuple_create();
    assert(new_tpl != nullptr);

    err = index_crsr->read_row(sys, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the first column value. */
    first = static_cast<const char *>(old_tpl->get_value(0));
    old_tpl->get_meta(0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(first != nullptr);

    /* Copy the old contents to the new tuple. */
    err = new_tpl->copy(old_tpl);

    /* Update the c3 column in the new tuple. */
    data_len = old_tpl->get_meta(2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = old_tpl->read_i32(2, &c3);
    assert(err == DB_SUCCESS);
    assert(c3 == key);
    c3 = (c3 + 1) % 10;

    ptr = (char *)malloc(8192);
    l = gen_rand_text(ptr, 128);

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 8192);
    /* Set the new key value in the new tuple. */
    err = new_tpl->set_value(0, ptr, l);
    assert(err == DB_SUCCESS);

    /* Get the new text to insert. */
    l = gen_rand_text(ptr, 8192);
    /* Set the c2 value in the new tuple. */
    err = new_tpl->set_value(1, ptr, l);
    assert(err == DB_SUCCESS);

    /* Set the updated c3 value in the new tuple. */
    err = new_tpl->write_i32(2, c3);
    assert(err == DB_SUCCESS);

    /* NOTE: We are using the secondary index cursor to update
    the record and not the cluster index cursor. */
    err = index_crsr->update_row(sys, old_tpl, new_tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

    /* Reset the old and new tuple instances. */
    old_tpl = old_tpl->clear();
    assert(old_tpl != nullptr);

    new_tpl = new_tpl->clear();
    assert(new_tpl != nullptr);

    free(ptr);

    Tuple::destroy(old_tpl);
    Tuple::destroy(new_tpl);
  }

  err = Cursor::close(sys, index_crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** Set the runtime global options. */
static void set_options(Config &config, int argc, char *argv[]) {
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
      if (set_global_option(config, opt, optarg) != DB_SUCCESS) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
      }

    } /* switch */
  }

  free(longopts);
}

int main(int argc, char *argv[]) {
  int i;
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;

  srandom(time(nullptr));

  Config config;

  test_configure(config);

  set_options(config, argc, argv);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < 10; ++i) {
    int j;

    printf("Create table\n");
    err = create_table(sys, DATABASE, TABLE);
    assert(err == DB_SUCCESS);

    for (j = 0; j < 10; ++j) {
      ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
      assert(ib_trx != nullptr);

      err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
      assert(err == DB_SUCCESS);

      err = crsr->lock(sys, IB_LOCK_IX);
      assert(err == DB_SUCCESS);

      insert_random_rows(sys, crsr);

      update_random_rows(sys, crsr);

      err = Cursor::close(sys, crsr);
      assert(err == DB_SUCCESS);
      crsr = nullptr;

      err = api::trx_commit(sys, ib_trx);
      assert(err == DB_SUCCESS);
    }

    printf("Drop table\n");
    err = drop_table(sys, DATABASE, TABLE);
    assert(err == DB_SUCCESS);
  }

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
