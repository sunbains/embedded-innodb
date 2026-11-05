/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith
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

/* Test table statistics API*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "test0aux.h"

#include <inttypes.h>

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"
#define TABLE "ib_table_statistics"

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        pk	INT,
        a       INT,
        b       INT,
        PRIMARY KEY(pk),
        INDEX (a),
        INDEX (b),
; */
static DbErr create_table_t2(System &sys, const char *dbname, /*!< in: database name */
                                const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * ib_idx_sch = nullptr;
  IndexSchema * ib_idx_sch_a = nullptr;
  IndexSchema * ib_idx_sch_b = nullptr;
  TableFormat tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("pk", IB_INT, IB_COL_NONE, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("a", IB_INT, IB_COL_NONE, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("b", IB_INT, IB_COL_NONE, 0, 4);
  assert(err == DB_SUCCESS);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("PRIMARY", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("pk", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_unique();
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("a", &ib_idx_sch_a);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch_a->add_col("a", 0);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("b", &ib_idx_sch_b);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch_b->add_col("b", 0);
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);
  if (err != DB_SUCCESS) {
    fprintf(stderr, "Warning: table create failed: %s\n", db_strerror(err));
    err = api::trx_rollback(sys, ib_trx);
  } else {
    err = api::trx_commit(sys, ib_trx);
  }
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

/** INSERT INTO T VALUE(int); */
static DbErr insert_rows(System &sys, Cursor * crsr, int start, int count) {
  int i;
  DbErr err;
  Tuple * tpl = nullptr;
  char *ptr = reinterpret_cast<char *>(malloc(8192));

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = start; i < start + count; ++i) {

    err = tpl->write_u32(0, i);
    assert(err == DB_SUCCESS);

    err = tpl->write_u32(1, i % 10);
    assert(err == DB_SUCCESS);

    err = tpl->write_u32(1, rand());
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  free(ptr);

  return (err);
}

static void display_table_stats(System &sys, Cursor * crsr) {
  DbErr err;
  TableStats tstats;
  err = crsr->get_table_statistics(sys, &tstats);
  assert(err == DB_SUCCESS);

  printf("Table Statistics\n");
  printf("----------------\n");
  printf("Rows:\t%" PRIi64 "\n", tstats.m_stat_n_rows);
  printf("Clustered Index Size:\t%" PRIu64 "\n",
         tstats.m_stat_clustered_index_size);
  printf("Other Index Size:\t%" PRIu64 "\n",
         tstats.m_stat_sum_of_other_index_sizes);
  printf("Modified Counter:\t%" PRIu64 "\n", tstats.m_stat_modified_counter);
  printf("\n");

  printf("\n\nINSERT 10,000 ROWS\n\n");

  printf("\n\n");
  printf("INDEX Statistics: n_diff_key_vals\n");
  printf("---------------------------------\n");

  uint64_t ncols;
  int64_t *n_diff;
  unsigned int col;
  printf("PRIMARY:\t");
  err = crsr->get_index_stat_n_diff_key_vals(sys, "PRIMARY", &ncols, &n_diff);
  for (col = 0; col < ncols; col++)
    printf("%" PRIi64 "\t", n_diff[col]);
  printf("\n\n");
  free(n_diff);

  printf("a:\t");
  err = crsr->get_index_stat_n_diff_key_vals(sys, "a", &ncols, &n_diff);
  for (col = 0; col < ncols; col++)
    printf("%" PRIi64 "\t", n_diff[col]);
  printf("\n\n");
  free(n_diff);

  printf("b:\t");
  err = crsr->get_index_stat_n_diff_key_vals(sys, "b", &ncols, &n_diff);
  for (col = 0; col < ncols; col++)
    printf("%" PRIi64 "\t", n_diff[col]);
  printf("\n\n");
  free(n_diff);
}

int main(int argc, char *argv[]) {
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;

  Config config;

  test_configure(config);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  printf("Create table\n");
  err = create_table_t2(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  insert_rows(sys, crsr, 1, 100);

  TableStats tstats;
  err = crsr->get_table_statistics(sys, &tstats);
  assert(err == DB_SUCCESS);

  display_table_stats(sys, crsr);

  for (int i = 0; i < 5; i++) {
    insert_rows(sys, crsr, i * 10000 + (i + 1 + 101), 10000);
    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;
    err = api::trx_commit(sys, ib_trx);
    assert(err == DB_SUCCESS);
    ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    /* Inserts need an IX lock on the table in each transaction. */
    err = crsr->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    display_table_stats(sys, crsr);
  }

  TableStats tstats2;
  err = crsr->get_table_statistics(sys, &tstats2);
  assert(err == DB_SUCCESS);

  assert(tstats.m_stat_n_rows < tstats2.m_stat_n_rows);
  assert(tstats.m_stat_clustered_index_size < tstats2.m_stat_clustered_index_size);
  assert(tstats.m_stat_modified_counter < tstats2.m_stat_modified_counter);

  OK(crsr->update_table_statistics(sys));

  display_table_stats(sys, crsr);

  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  printf("Drop table\n");
  err = drop_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
