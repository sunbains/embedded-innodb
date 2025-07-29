/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith.
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
  uint32_t c3;
} row_t;

static row_t in_rows[] = {{"a", "t", 1},  {"b", "u", 2}, {"c", "b", 3},
                          {"d", "n", 4},  {"e", "s", 5}, {"e", "j", 6},
                          {"d", "f", 7},  {"c", "n", 8}, {"b", "z", 9},
                          {"a", "i", 10}, {"", "", 0}};

#define COL_LEN(n) (sizeof(((row_t *)0)->n))

/** Create an InnoDB database (sub-directory). */
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

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
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  ib_idx_sch_t ib_idx_sch = nullptr;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_V1, 0);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_VARCHAR_ANYCHARSET,
                                IB_COL_NONE, 0, COL_LEN(c1) - 1);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c2) - 1);
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c3", IB_INT, IB_COL_UNSIGNED, 0,
                                COL_LEN(c3));

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "c1_c2", &ib_idx_sch);
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

/** INSERT INTO T VALUE('c1', 'c2', c3); */
static ib_err_t
insert_rows(ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
  row_t *row;
  ib_tpl_t tpl = nullptr;
  ib_err_t err = DB_ERROR;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  for (row = in_rows; *row->c1; ++row) {
    err = ib_col_set_value(tpl, 0, row->c1, strlen(row->c1));
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 1, row->c2, strlen(row->c2));
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 2, &row->c3, sizeof(row->c3));
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  return (err);
}

/** UPDATE T SET c3 = c3 + 100 WHERE c1 = 'a'; */
static ib_err_t update_a_row(ib_crsr_t crsr) {
  ib_err_t err;
  int res = ~0L;
  ib_tpl_t key_tpl;
  ib_tpl_t old_tpl = nullptr;
  ib_tpl_t new_tpl = nullptr;

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != nullptr);

  /* Set the value to look for. */
  err = ib_col_set_value(key_tpl, 0, "a", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS);
  /* Must be positioned on a record that's greater than search key. */
  assert(res == -1);

  if (key_tpl != nullptr) {
    ib_tuple_delete(key_tpl);
  }

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = ib_clust_read_tuple_create(crsr);
  assert(old_tpl != nullptr);

  new_tpl = ib_clust_read_tuple_create(crsr);
  assert(new_tpl != nullptr);

  /* Iterate over the records while the c1 column matches "a". */
  while (err == DB_SUCCESS) {
    const char *c1;
    uint32_t c3;
    ulint c1_len;
    ulint data_len;
    ib_col_meta_t col_meta;

    err = ib_cursor_read_row(crsr, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the c1 column value. */
    c1 = ib_col_get_value(old_tpl, 0);
    c1_len = ib_col_get_meta(old_tpl, 0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(c1 != nullptr);

    /* Only update c1 values that are == "a". */
    if (strncmp(c1, "a", 1) != 0) {
      break;
    }

    /* Copy the old contents to the new tuple. */
    err = ib_tuple_copy(new_tpl, old_tpl);

    /* Update the c3 column in the new tuple. */
    data_len = ib_col_get_meta(old_tpl, 2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = ib_tuple_read_u32(old_tpl, 2, &c3);
    assert(err == DB_SUCCESS);
    c3 += 100;

    /* Set the updated value in the new tuple. */
    err = ib_tuple_write_u32(new_tpl, 2, c3);
    assert(err == DB_SUCCESS);

    err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
    assert(err == DB_SUCCESS);

    /* NOTE: we go in *REVERSE* order since we are using custom
       compare function that does in reverse binary order.
       i.e. 'a' is *LAST*, not first */
    /* Move to the next record to update. */
    err = ib_cursor_prev(crsr);
    /* Since we are searching for "a" it must always succeed. */
    assert(err == DB_SUCCESS);

    /* Reset the old and new tuple instances. */
    old_tpl = ib_tuple_clear(old_tpl);
    assert(old_tpl != nullptr);

    new_tpl = ib_tuple_clear(new_tpl);
    assert(new_tpl != nullptr);
  }

  if (old_tpl != nullptr) {
    ib_tuple_delete(old_tpl);
  }
  if (new_tpl != nullptr) {
    ib_tuple_delete(new_tpl);
  }

  return (err);
}

/** DELETE RFOM T WHERE c1 = 'b' and c2 = 'z'; */
static ib_err_t delete_a_row(ib_crsr_t crsr) {
  ib_err_t err;
  int res = ~0L;
  ib_tpl_t key_tpl;

  /* Create a tuple for searching an index. */
  key_tpl = ib_sec_search_tuple_create(crsr);
  assert(key_tpl != nullptr);

  /* Set the value to delete. */
  err = ib_col_set_value(key_tpl, 0, "b", 1);
  assert(err == DB_SUCCESS);
  err = ib_col_set_value(key_tpl, 1, "z", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS);
  /* Must be positioned on the record to delete, since
  we've specified an exact prefix match. */
  assert(res == 0);

  if (key_tpl != nullptr) {
    ib_tuple_delete(key_tpl);
  }

  /* InnoDB handles the updating of all secondary indexes. */
  err = ib_cursor_delete_row(crsr);
  assert(err == DB_SUCCESS);

  return (err);
}

/** SELECT * FROM T; */
static ib_err_t do_query(ib_crsr_t crsr) {
  ib_err_t err;
  ib_tpl_t tpl;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  err = ib_cursor_first(crsr);
  assert(err == DB_SUCCESS);

  char last_char = 'z';

  while (err == DB_SUCCESS) {
    err = ib_cursor_read_row(crsr, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    const char *ptr;

    ptr = ib_col_get_value(tpl, 0);
    fprintf(stdout, "COL 0:%s\tlast %c this %c", ptr, last_char, ptr[0]);
    assert(ptr[0] <= last_char);
    last_char = ptr[0];

    print_tuple(stdout, tpl);

    err = ib_cursor_next(crsr);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    tpl = ib_tuple_clear(tpl);
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }
  return (err);
}

#define min(x, y) (x < y ? x : y)

/* the same as default_compare, but in reverse! */
int reverse_compare(
    /*!< out: 1, 0, -1, if a is greater,
    equal, less than b, respectively */
    const ib_col_meta_t *col_meta, /*!< in: column meta data */
    const ib_byte_t *p1,           /*!< in: key */
    ulint p1_len,                  /*!< in: key length */
    const ib_byte_t *p2,           /*!< in: key */
    ulint p2_len)                  /*!< in: key length */
{
  int ret;

  (void)col_meta;

  ret = memcmp(p1, p2, min(p1_len, p2_len));

  if (ret == 0) {
    ret = p1_len - p2_len;
  }

  return (ret < 0 ? 1 : ((ret > 0) ? -1 : 0));
}

int main(int argc, char *argv[]) {
  ib_err_t err;
  ib_crsr_t crsr;
  ib_trx_t ib_trx;
  uint64_t version;

  (void)argc;
  (void)argv;

  version = ib_api_version();
  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  ib_set_client_compare(reverse_compare);

  err = ib_startup("default");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  printf("Create table\n");
  err = create_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  printf("Begin transaction\n");
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  printf("Open cursor\n");
  err = open_table(DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  printf("Lock table in IX\n");
  err = ib_cursor_lock(crsr, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  printf("Insert rows\n");
  err = insert_rows(crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(crsr);
  assert(err == DB_SUCCESS);

  printf("Update a row\n");
  err = update_a_row(crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(crsr);
  assert(err == DB_SUCCESS);

  printf("Delete a row\n");
  err = delete_a_row(crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(crsr);
  assert(err == DB_SUCCESS);

  printf("Close cursor\n");
  err = ib_cursor_close(crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  printf("Commit transaction\n");
  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  printf("Drop table\n");
  err = drop_table(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
