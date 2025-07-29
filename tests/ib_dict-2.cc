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
static ib_err_t create_database(const char *name) noexcept {
  const auto success = ib_database_create(name);
  assert(success);

  return DB_SUCCESS;
}

/**
 * Drop the database.
 *
 * @param[in] dbname Database name.
 */
static ib_err_t drop_database(const char *dbname) noexcept {
  return ib_database_drop(dbname);
}

static ib_err_t create_table(const char *dbname, const char *name) {
  ib_id_t table_id{};
  ib_idx_sch_t idx_sch{};
  ib_tbl_sch_t ib_tbl_sch{};
  ib_tbl_fmt_t tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  auto err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, 0);
  assert(err == DB_SUCCESS);

  /* Define the table columns */
  err = ib_tbl_sch_add_u32_col(ib_tbl_sch, "A");
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch_add_u32_col(ib_tbl_sch, "D");
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch_add_blob_col(ib_tbl_sch, "B");
  assert(err == DB_SUCCESS);

  // err = ib_tbl_sch_add_text_col(ib_tbl_sch, "C");
  err = ib_tbl_sch_add_blob_col(ib_tbl_sch, "C");
  assert(err == DB_SUCCESS);

  /* Add primary key */
  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "B", 10);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "A", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "D", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_unique(idx_sch);
  assert(err == DB_SUCCESS);

  /* Add secondary indexes */
  err = ib_table_schema_add_index(ib_tbl_sch, "SEC_0", &idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "D", 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "SEC_1", &idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "A", 0);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "SEC_2", &idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "C", 255);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "B", 255);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "SEC_3", &idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "B", 5);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "C", 10);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "A", 0);
  assert(err == DB_SUCCESS);

  /* create table */
  auto ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);

  /* We assume it's the same table. */
  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  return (err);
}

int main(int argc, char *argv[]) {
  auto err = ib_init();
  assert(err == DB_SUCCESS);

  err = ib_startup("default");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = drop_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = drop_database(DATABASE);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif /* UNIV_DEBUG_VALGRIND */

  return EXIT_SUCCESS;
}
