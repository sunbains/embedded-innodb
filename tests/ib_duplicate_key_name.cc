/** Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith

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

/* Tests getting the name of the index that caused a duplicate key error */

#include <stdio.h>
#include <assert.h>

#include "innodb.h"

#include "test0aux.h"

#include <stdarg.h>
#include <string.h>

#define DATABASE "test"
#define TABLE "ib_duplicate_key_name"
#define TABLE_PATH DATABASE "/" TABLE

#define PRIMARY_INDEX_NAME "PRIMARY"
#define SEC_INDEX_NAME "c2_idx"

/** Create an InnoDB database (sub-directory). */
static ib_err_t create_database(const char *name) {
  ib_bool_t err;

  err = ib_database_create(name);
  assert(err == IB_TRUE);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(C1 INT, C2 INT, PK(C1), UNIQUE(C2)); */
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
                                sizeof(ib_i32_t));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_INT, IB_COL_NONE, 0,
                                sizeof(ib_i32_t));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, PRIMARY_INDEX_NAME, &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(ib_idx_sch);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, SEC_INDEX_NAME, &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_index_schema_add_col(ib_idx_sch, "c2", 0);
  assert(err == DB_SUCCESS);

  err = ib_index_schema_set_unique(ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* create table */
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);

  if (err == DB_SUCCESS) {
    err = ib_trx_commit(ib_trx);
  } else {
    fprintf(stderr, "Table: %s create failed: %s\n", table_name,
            ib_strerror(err));

    err = ib_trx_rollback(ib_trx);
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

#ifdef __WIN__
  sprintf(table_name, "%s/%s", dbname, name);
#else
  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

  err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** INSERT INTO T VALUE(0, RANDOM(TEXT), RANDOM(TEXT)); ... 100 */
static ib_err_t insert_rows(ib_trx_t trx, ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t tpl;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != NULL);

  /* INSERT (1, 1) */

  err = ib_tuple_write_i32(tpl, 0, 1);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 1);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_SUCCESS);

  tpl = ib_tuple_clear(tpl);
  assert(tpl != NULL);

  /* INSERT (2, 2) */

  err = ib_tuple_write_i32(tpl, 0, 2);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 2);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_SUCCESS);

  tpl = ib_tuple_clear(tpl);
  assert(tpl != NULL);

  /* INSERT (1,3) <- fail on pkey */
  err = ib_tuple_write_i32(tpl, 0, 1);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 3);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_DUPLICATE_KEY);

  char *d_table, *d_index;
  err = ib_get_duplicate_key(trx, &d_table, &d_index);
  assert(err == DB_SUCCESS);
  printf("DB_DUPLICATE_KEY: TABLE: %s  INDEX: %s\n", d_table, d_index);

  assert(strcmp(d_table, TABLE_PATH) == 0);
  assert(strcmp(d_index, PRIMARY_INDEX_NAME) == 0);

  tpl = ib_tuple_clear(tpl);
  assert(tpl != NULL);

  /* insert (3,1) <- fail on secondary */
  err = ib_tuple_write_i32(tpl, 0, 3);
  assert(err == DB_SUCCESS);

  err = ib_tuple_write_i32(tpl, 1, 1);
  assert(err == DB_SUCCESS);

  err = ib_cursor_insert_row(crsr, tpl);
  assert(err == DB_DUPLICATE_KEY);

  err = ib_get_duplicate_key(trx, &d_table, &d_index);
  assert(err == DB_SUCCESS);
  printf("DB_DUPLICATE_KEY: TABLE: %s  INDEX: %s\n", d_table, d_index);

  assert(strcmp(d_table, TABLE_PATH) == 0);
  assert(strcmp(d_index, SEC_INDEX_NAME) == 0);

  tpl = ib_tuple_clear(tpl);
  assert(tpl != NULL);

  if (tpl != NULL) {
    ib_tuple_delete(tpl);
  }

  return (DB_SUCCESS);
}

int main(int argc, char **argv) {
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;

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

  err = insert_rows(ib_trx, crsr);
  assert(err == DB_SUCCESS);

  err = ib_cursor_close(crsr);
  assert(err == DB_SUCCESS);
  crsr = NULL;

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  return (0);
}
