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
 CREATE TABLE T(c1 INT, c2 VARCHAR(n), c3 BLOB, PK(c1));
 INSERT INTO T VALUES(n, RANDOM(n), RANDOM(n)); ...

 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"
#define TABLE "t"

static const int N_TRX = 10;
static const int N_RECS = 100;

static const int C2_MAX_LEN = 256;
static const int C3_MAX_LEN = 8192;

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(c1 INT, c2 VARCHAR(n), c3 BLOB, PK(c1)); */
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

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_NONE, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_VARCHAR, IB_COL_NONE, 0,
                                C2_MAX_LEN);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("c1", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);
  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

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

/** INSERT INTO T VALUE(N, RANDOM(n), RANDOM(n)); */
static DbErr
insert_rows(System &sys, Cursor * crsr, /*!< in, out: cursor to use for write */
            int start,      /*!< in: in: start of c1 value */
            int count)      /*!< in: number of rows to insert */
{
  int i;
  int dups = 0;
  Tuple * tpl = nullptr;
  DbErr err = DB_ERROR;
  char *ptr = new char[8192];

  assert(ptr != nullptr);

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = start; i < start + count; ++i) {
    int l;

    err = tpl->write_i32(0, i);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, C2_MAX_LEN);
    err = tpl->set_value(1, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, C3_MAX_LEN);
    err = tpl->set_value(2, ptr, l);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);
    if (err == DB_DUPLICATE_KEY) {
      err = DB_SUCCESS;
      ++dups;
    }
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  delete[] ptr;

  if (err == DB_SUCCESS) {
    if (dups == count) {
      err = DB_DUPLICATE_KEY;
      printf("Recover OK\n");
    } else {
      assert(dups == 0);
      printf("Insert OK\n");
    }
  }

  return err;
}

/** Set the runtime global options. */
static void set_options(Config &config, int argc, char *argv[]) {
  int opt;

  while ((opt = getopt_long(argc, argv, "", ib_longopts, nullptr)) != -1) {

    /* If it's an InnoDB parameter, then we let the
    auxillary function handle it. */
    if (set_global_option(config, opt, optarg) != DB_SUCCESS) {
      print_usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }
}

static DbErr test_phase_I(System &sys) {
  int i;
  DbErr err;
  int dups = 0;

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < N_TRX; ++i) {
    Cursor * crsr;
    Trx * ib_trx;

    ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = crsr->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(sys, crsr, i * N_RECS, N_RECS);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);
    if (err == DB_DUPLICATE_KEY) {
      ++dups;
    }

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    err = api::trx_commit(sys, ib_trx);
    assert(err == DB_SUCCESS);
  }

  assert(dups == 0 || dups == N_TRX);
  return (dups == N_TRX ? DB_DUPLICATE_KEY : DB_SUCCESS);
}

/** Restart the process with the same args. */
static void restart(int argc, char *argv[]) {
  execvp(argv[0], argv);
  perror("execvp");
  abort();
}

int main(int argc, char *argv[]) {
  print_version();

  Config config;

  DbErr err;

  test_configure(config);

  set_options(config, argc, argv);

  System sys;

  test_startup(sys, config);

  err = test_phase_I(sys);

  if (err == DB_SUCCESS) {
    restart(argc, argv);
    /* Shouldn't get here. */
    abort();
  } else {
    /* Recovery was successful. */
    assert(err == DB_DUPLICATE_KEY);

    err = truncate_table(sys, DATABASE, TABLE);
    assert(err == DB_SUCCESS);

    err = drop_table(sys, DATABASE, TABLE);
    assert(err == DB_SUCCESS);

    err = sys.shutdown(IB_SHUTDOWN_NORMAL);
    assert(err == DB_SUCCESS);
  }

  return (EXIT_SUCCESS);
}
