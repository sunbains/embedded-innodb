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
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T(
        c1	VARCHAR(n),
        c2	VARCHAR(n),
        c3	INT,
        PRIMARY KEY(c1, c2); */
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

  err = ib_tbl_sch->add_col("c1", IB_VARCHAR_ANYCHARSET,
                                IB_COL_NONE, 0, COL_LEN(c1) - 1);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_VARCHAR, IB_COL_NONE, 0,
                                COL_LEN(c2) - 1);
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c3", IB_INT, IB_COL_UNSIGNED, 0,
                                COL_LEN(c3));

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("c1_c2", &ib_idx_sch);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c1", 0);
  assert(err == DB_SUCCESS);

  /* Set prefix length to 0. */
  err = ib_idx_sch->add_col("c2", 0);
  assert(err == DB_SUCCESS);

  err = ib_idx_sch->set_clustered();
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

/** INSERT INTO T VALUE('c1', 'c2', c3); */
static DbErr
insert_rows(System &sys, Cursor * crsr) /*!< in, out: cursor to use for write */
{
  row_t *row;
  Tuple * tpl = nullptr;
  DbErr err = DB_ERROR;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (row = in_rows; *row->c1; ++row) {
    err = tpl->set_value(0, row->c1, strlen(row->c1));
    assert(err == DB_SUCCESS);

    err = tpl->set_value(1, row->c2, strlen(row->c2));
    assert(err == DB_SUCCESS);

    err = tpl->set_value(2, &row->c3, sizeof(row->c3));
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  return (err);
}

/** UPDATE T SET c3 = c3 + 100 WHERE c1 = 'a'; */
static DbErr update_a_row(System &sys, Cursor * crsr) {
  DbErr err;
  int res = ~0L;
  Tuple * key_tpl;
  Tuple * old_tpl = nullptr;
  Tuple * new_tpl = nullptr;

  /* Create a tuple for searching an index. */
  key_tpl = crsr->sec_search_tuple_create();
  assert(key_tpl != nullptr);

  /* Set the value to look for. */
  err = key_tpl->set_value(0, "a", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS);
  /* Must be positioned on a record that's greater than search key. */
  assert(res == -1);

  if (key_tpl != nullptr) {
    Tuple::destroy(key_tpl);
  }

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = crsr->clust_read_tuple_create();
  assert(old_tpl != nullptr);

  new_tpl = crsr->clust_read_tuple_create();
  assert(new_tpl != nullptr);

  /* Iterate over the records while the c1 column matches "a". */
  while (err == DB_SUCCESS) {
    const char *c1;
    uint32_t c3;
    ulint c1_len;
    ulint data_len;
    ColMeta col_meta;

    err = crsr->read_row(sys, old_tpl);
    assert(err == DB_SUCCESS);

    /* Get the c1 column value. */
    c1 = old_tpl->get_value(0);
    c1_len = old_tpl->get_meta(0, &col_meta);

    /* There are no SQL_NULL values in our test data. */
    assert(c1 != nullptr);

    /* Only update c1 values that are == "a". */
    if (strncmp(c1, "a", 1) != 0) {
      break;
    }

    /* Copy the old contents to the new tuple. */
    err = new_tpl->copy(old_tpl);

    /* Update the c3 column in the new tuple. */
    data_len = old_tpl->get_meta(2, &col_meta);
    assert(data_len != IB_SQL_NULL);
    err = old_tpl->read_u32(2, &c3);
    assert(err == DB_SUCCESS);
    c3 += 100;

    /* Set the updated value in the new tuple. */
    err = new_tpl->write_u32(2, c3);
    assert(err == DB_SUCCESS);

    err = crsr->update_row(sys, old_tpl, new_tpl);
    assert(err == DB_SUCCESS);

    /* NOTE: we go in *REVERSE* order since we are using custom
       compare function that does in reverse binary order.
       i.e. 'a' is *LAST*, not first */
    /* Move to the next record to update. */
    err = crsr->prev(sys);
    /* Since we are searching for "a" it must always succeed. */
    assert(err == DB_SUCCESS);

    /* Reset the old and new tuple instances. */
    old_tpl = old_tpl->clear();
    assert(old_tpl != nullptr);

    new_tpl = new_tpl->clear();
    assert(new_tpl != nullptr);
  }

  if (old_tpl != nullptr) {
    Tuple::destroy(old_tpl);
  }
  if (new_tpl != nullptr) {
    Tuple::destroy(new_tpl);
  }

  return (err);
}

/** DELETE RFOM T WHERE c1 = 'b' and c2 = 'z'; */
static DbErr delete_a_row(System &sys, Cursor * crsr) {
  DbErr err;
  int res = ~0L;
  Tuple * key_tpl;

  /* Create a tuple for searching an index. */
  key_tpl = crsr->sec_search_tuple_create();
  assert(key_tpl != nullptr);

  /* Set the value to delete. */
  err = key_tpl->set_value(0, "b", 1);
  assert(err == DB_SUCCESS);
  err = key_tpl->set_value(1, "z", 1);
  assert(err == DB_SUCCESS);

  /* Search for the key using the cluster index (PK) */
  err = crsr->moveto(sys, key_tpl, IB_CUR_GE, &res);
  assert(err == DB_SUCCESS);
  /* Must be positioned on the record to delete, since
  we've specified an exact prefix match. */
  assert(res == 0);

  if (key_tpl != nullptr) {
    Tuple::destroy(key_tpl);
  }

  /* InnoDB handles the updating of all secondary indexes. */
  err = crsr->delete_row(sys);
  assert(err == DB_SUCCESS);

  return (err);
}

/** SELECT * FROM T; */
static DbErr do_query(System &sys, Cursor * crsr) {
  DbErr err;
  Tuple * tpl;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  err = crsr->first(sys);
  assert(err == DB_SUCCESS);

  char last_char = 'z';

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    const char *ptr;

    ptr = tpl->get_value(0);
    fprintf(stdout, "COL 0:%s\tlast %c this %c", ptr, last_char, ptr[0]);
    assert(ptr[0] <= last_char);
    last_char = ptr[0];

    print_tuple(stdout, tpl);

    err = crsr->next(sys);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    tpl = tpl->clear();
    assert(tpl != nullptr);
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
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
    const ColMeta *col_meta, /*!< in: column meta data */
    const byte *p1,           /*!< in: key */
    ulint p1_len,                  /*!< in: key length */
    const byte *p2,           /*!< in: key */
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
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;
  uint64_t version;

  (void)argc;
  (void)argv;

  version = api::api_version();
  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */

  Config config;

  test_configure(config);

  ib_set_client_compare(reverse_compare);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  printf("Create table\n");
  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  printf("Begin transaction\n");
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  printf("Open cursor\n");
  err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
  assert(err == DB_SUCCESS);

  printf("Lock table in IX\n");
  err = crsr->lock(sys, IB_LOCK_IX);
  assert(err == DB_SUCCESS);

  printf("Insert rows\n");
  err = insert_rows(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Update a row\n");
  err = update_a_row(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Delete a row\n");
  err = delete_a_row(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Query table\n");
  err = do_query(sys, crsr);
  assert(err == DB_SUCCESS);

  printf("Close cursor\n");
  err = Cursor::close(sys, crsr);
  assert(err == DB_SUCCESS);
  crsr = nullptr;

  printf("Commit transaction\n");
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
