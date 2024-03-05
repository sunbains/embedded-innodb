/** Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.

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
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(c1 INT, c2 VARCHAR(n), c3 BLOB, PK(c1)); */
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = NULL;
  ib_idx_sch_t ib_idx_sch = NULL;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_VARCHAR, IB_COL_NONE, 0,
                                C2_MAX_LEN);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "c1", &ib_idx_sch);
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
  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

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

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE(N, RANDOM(n), RANDOM(n)); */
static ib_err_t
insert_rows(ib_crsr_t crsr, /*!< in, out: cursor to use for write */
            int start,      /*!< in: in: start of c1 value */
            int count)      /*!< in: number of rows to insert */
{
  int i;
  int dups = 0;
  ib_tpl_t tpl = NULL;
  ib_err_t err = DB_ERROR;
  char *ptr = new char[8192];

  assert(ptr != NULL);

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  for (i = start; i < start + count; ++i) {
    int l;

    err = ib_tuple_write_i32(tpl, 0, i);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, C2_MAX_LEN);
    err = ib_col_set_value(tpl, 1, ptr, l);
    assert(err == DB_SUCCESS);

    l = gen_rand_text(ptr, C3_MAX_LEN);
    err = ib_col_set_value(tpl, 2, ptr, l);
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);
    if (err == DB_DUPLICATE_KEY) {
      err = DB_SUCCESS;
      ++dups;
    }
  }

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
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
static void set_options(int argc, char *argv[]) {
  int opt;

  while ((opt = getopt_long(argc, argv, "", ib_longopts, NULL)) != -1) {

    /* If it's an InnoDB parameter, then we let the
    auxillary function handle it. */
    if (set_global_option(opt, optarg) != DB_SUCCESS) {
      print_usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }
}

static ib_err_t test_phase_I(void) {
  int i;
  ib_err_t err;
  int dups = 0;

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  for (i = 0; i < N_TRX; ++i) {
    ib_crsr_t crsr;
    ib_trx_t ib_trx;

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = open_table(DATABASE, TABLE, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(crsr, i * N_RECS, N_RECS);
    assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);
    if (err == DB_DUPLICATE_KEY) {
      ++dups;
    }

    err = ib_cursor_close(crsr);
    assert(err == DB_SUCCESS);
    crsr = NULL;

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);
  }

  assert(dups == 0 || dups == N_TRX);
  return (dups == N_TRX ? DB_DUPLICATE_KEY : DB_SUCCESS);
}

/** Restart the process with the same args. */
static void restart(int argc, char *argv[]) {
  (void)argc;

  execvp(argv[0], argv);
  perror("execvp");
  abort();
}

int main(int argc, char *argv[]) {
  ib_err_t err;

  print_version();

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  set_options(argc, argv);

  err = ib_startup("barracuda");
  assert(err == DB_SUCCESS);

  err = test_phase_I();

  if (err == DB_SUCCESS) {
    restart(argc, argv);
    /* Shouldn't get here. */
    abort();
  } else {
    /* Recovery was successful. */
    assert(err == DB_DUPLICATE_KEY);

    err = drop_table(DATABASE, TABLE);
    assert(err == DB_SUCCESS);

    err = ib_shutdown(IB_SHUTDOWN_NORMAL);
    assert(err == DB_SUCCESS);
  }

  return (EXIT_SUCCESS);
}
