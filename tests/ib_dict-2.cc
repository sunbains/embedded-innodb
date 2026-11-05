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

/* Check create table, index and drop database.

Create a database
CREATE TABLE t (
        A INT,
        D INT,
        B BLOB,
        C TEXT,
        PRIMARY KEY(B(10), A, D),
        INDEX(D),
        INDEX(A),
        INDEX(C(255), B(255)),
        INDEX(B(5), C(10), A));

The test will create all the relevant sub-directories in the current working
directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

/* Test parameters. For now hard coded. */
#define DATABASE "test"
#define TABLE "t"

/**
 * Create an InnoDB database (sub-directory).
 */
static DbErr create_database(System &sys, const char *name) noexcept {
  const auto success = api::database_create(sys, name);
  assert(success);

  return DB_SUCCESS;
}

/**
 * Drop the database.
 *
 * @param[in] dbname Database name.
 */
static DbErr drop_database(System &sys, const char *dbname) noexcept {
  return api::database_drop(sys, dbname);
}

static DbErr create_table(System &sys, const char *dbname, const char *name) {
  uint64_t table_id{};
  IndexSchema * idx_sch{};
  TableSchema * ib_tbl_sch{};
  TableFormat tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  auto err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt);
  assert(err == DB_SUCCESS);

  /* Define the table columns */
  err = ib_tbl_sch->add_col("A", IB_INT, IB_COL_UNSIGNED, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("D", IB_INT, IB_COL_UNSIGNED, 0, 4);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("B", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  // err = ib_tbl_sch->add_col("C", IB_VARCHAR, IB_COL_NONE, 0, MAX_TEXT_LEN);
  err = ib_tbl_sch->add_col("C", IB_BLOB, IB_COL_NONE, 0, 0);
  assert(err == DB_SUCCESS);

  /* Add primary key */
  err = ib_tbl_sch->add_index("PRIMARY", &idx_sch);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("B", 10);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("A", 0);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("D", 0);
  assert(err == DB_SUCCESS);

  err = idx_sch->set_clustered();
  assert(err == DB_SUCCESS);

  err = idx_sch->set_unique();
  assert(err == DB_SUCCESS);

  /* Add secondary indexes */
  err = ib_tbl_sch->add_index("SEC_0", &idx_sch);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("D", 0);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("SEC_1", &idx_sch);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("A", 0);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("SEC_2", &idx_sch);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("C", 255);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("B", 255);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("SEC_3", &idx_sch);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("B", 5);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("C", 10);
  assert(err == DB_SUCCESS);

  err = idx_sch->add_col("A", 0);
  assert(err == DB_SUCCESS);

  /* create table */
  auto ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);

  /* We assume it's the same table. */
  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    TableSchema::destroy(sys, ib_tbl_sch);
  }

  return (err);
}

int main(int argc, char *argv[]) {
  Config config;
  DbErr err;

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = drop_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = drop_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif /* UNIV_DEBUG_VALGRIND */

  return EXIT_SUCCESS;
}
