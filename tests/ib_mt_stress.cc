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

/* Multi-threaded test that is equivalent of ibtest3. It roughly does
the following:

Create a database
CREATE TABLE blobt3(
        A	INT,
        D	INT,
        B	BLOB,
        C	TEXT,
        PRIMARY KEY(B(10), A, D),
        INDEX(D),
        INDEX(A),
        INDEX(C(255), B(255)),
        INDEX(B(5), C(10), A));

Insert <num_rows> into the table.

Create four type of worker threads (total threads being NUM_THREADS)

1) Insert worker thread that does the following:
 INSERT INTO blobt3 VALUES(sys, 
        RANDOM(INT),
        5,
        RANDOM(BLOB),
        RANDOM(TEXT))
 Insert workers insert rows in batches and then commit or rollback
 the transaction based on rollback_percent.

2) Update worker thread that does the following:
 UPDATE blobt3
 SET B = <random_string>
 WHERE A = <random_integer>;
 Update workers update rows in batches and then commit or rollback
 the transaction based on rollback_percent.

3) Delete workers (no-op for now)
4) Select workers (no-op for now)

 The test will create all the relevant sub-directories in the current
 working directory. */

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
#define TABLE "blobt3"

/* Total number of worker threads for SEL, UPD, INS and DEL */
#define NUM_THREADS 64
static pthread_t tid[NUM_THREADS];

/* Initial number of rows. */
static const int num_rows = 2000;

/* batch size for DML. Commit after that many rows are worked upon */
static const int batch_size = 10;

/* %age transactions to be rolled back */
static const int rollback_percent = 10;

/* isolation level for transactions */
static const int isolation_level = IB_TRX_REPEATABLE_READ;

/* blob/text column field length */
#define BLOB_LEN 34000

/* array to hold random data strings */
static char **rnd_str;

/* array to hold length of strings in rnd_str */
static int *rnd_str_len;

/* array to hold pre-defined prefixes for blob */
static char pre_str[][128] = {
    "kjgclgrtfuylfluyfyufyulfulfyyulofuyolfyufyufuyfyufyufyufyui"
    "fyyufujhfghd",
    "khd", "kh"};

/* Test duration in seconds */
static int test_time = 1800;

/* Flag used by worker threads to finish the run */
static bool test_running = false;

/* to hold statistics of a particular type of DML */
typedef struct dml_op_struct {
  int n_ops;                  /* Total ops performed */
  int n_errs;                 /* Total errors */
  int errs[DB_DATA_MISMATCH]; /* This is taken from DbErr.h
                      and it is going to be a very sparse
                      array but we can live with it for
                      testing. */
  pthread_mutex_t mutex;      /*mutex protecting this struct. */
} dml_op_t;

static dml_op_t sel_stats;
static dml_op_t del_stats;
static dml_op_t upd_stats;
static dml_op_t ins_stats;

enum op_type {
  DML_OP_TYPE_SELECT = 0,
  DML_OP_TYPE_INSERT,
  DML_OP_TYPE_UPDATE,
  DML_OP_TYPE_DELETE,
};

/* Update statistics for any given dml_op_struct */
#define UPDATE_ERR_STATS(x, y)                                                 \
  do {                                                                         \
    pthread_mutex_lock(&((x).mutex));                                          \
    (x).n_ops++;                                                               \
    if ((y) != DB_SUCCESS) {                                                   \
      (x).n_errs++;                                                            \
      (x).errs[(y)]++;                                                         \
    }                                                                          \
    pthread_mutex_unlock(&((x).mutex));                                        \
  } while (0)

/** Populate rnd_str with random strings having one of the prefixes from
the three hardcoded prefixes from pre_str. */
static void gen_random_data(void) {
  int i;

  assert(num_rows > 0);
  rnd_str = (char **)malloc(sizeof(*rnd_str) * num_rows);
  assert(rnd_str);

  for (i = 0; i < num_rows; ++i) {
    rnd_str[i] = (char *)malloc(BLOB_LEN);
    assert(rnd_str[i]);
  }

  rnd_str_len = (int *)malloc(sizeof(int) * num_rows);
  assert(rnd_str_len);

  /* Now generate the random text strings */
  for (i = 0; i < num_rows; ++i) {
    char *ptr;
    int len;

    strcpy(rnd_str[i], pre_str[random() % 3]);
    len = strlen(rnd_str[i]);
    ptr = rnd_str[i] + len;
    len += gen_rand_text(ptr, BLOB_LEN - 128);
    rnd_str_len[i] = len;
  }
}

#if 0
/** Print character array of give size or upto 256 chars */
static
void
print_char_array(
	FILE*		stream,	/*!< in: stream to print to */
	const char*	array,	/*!< in: char array */
	int		len)	/*!< in: length of data */
{
	int		j;
	const char*	ptr = array;

	for (j = 0; j < 256 && j < len; ++j) {
		fprintf(stream, "%c", *(ptr + j));
	}
}

/** Print the random strings generated by gen_random_data(). Just for
debugging to check we are generating good data. */
static
void
print_random_data(void)
{
	int	i;

	for (i = 0; i < num_rows; ++i) {
		fprintf(stderr, "%d:", rnd_str_len[i]);
		print_char_array(stderr, rnd_str[i], rnd_str_len[i]);
		fprintf(stderr, "\n");
	}
}
#endif

/** Create an InnoDB database (sub-directory). */
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** Drop the database. */
static DbErr drop_database(System &sys, const char *dbname) /*!< in: db to drop */
{
  DbErr err;
  err = api::database_drop(sys, dbname);

  return (err);
}

/** CREATE TABLE blobt3(
        A	INT,
        D	INT,
        B	BLOB,
        C	BLOB,
        PRIMARY KEY(B(10), A, D),
        INDEX(D),
        INDEX(A),
        INDEX(C(255), B(255)),
        INDEX(B(5), C(10), A)); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * idx_sch = nullptr;
  TableFormat tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt, 0);

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
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
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

/** Depending on rollback_percent decides whether to commit or rollback a
given transaction */
static void
commit_or_rollback(System &sys, Trx * trx, /*!< in: trx to commit or rollback */
                   int cnt)      /*!< in: trx counter */
{

  DbErr err;

  if (api::trx_state(trx) == IB_TRX_NOT_STARTED) {
    err = api::trx_release(sys, trx);
    assert(err == DB_SUCCESS);
  } else if (cnt % (100 / rollback_percent)) {
    // printf("Commit transaction\n");
    err = api::trx_commit(sys, trx);
    assert(err == DB_SUCCESS);
  } else {
    // printf("Rollback transaction\n");
    err = api::trx_rollback(sys, trx);
    assert(err == DB_SUCCESS);
  }
}

/** Inserts one row into the table.
@return	error from insert row */
static DbErr
insert_one_row(System &sys, Cursor * crsr, /*!< in, out: cursor to use for write */
               Tuple * tpl)   /*!< in: tpl to work on */
{
  int j;
  DbErr err;

  err = tpl->write_u32(0, random() % num_rows);
  assert(err == DB_SUCCESS);

  err = tpl->write_u32(1, 5);
  assert(err == DB_SUCCESS);

  j = random() % num_rows;
  err = tpl->set_value(2, rnd_str[j], rnd_str_len[j]);
  assert(err == DB_SUCCESS);

  j = random() % num_rows;
  err = tpl->set_value(3, rnd_str[j], rnd_str_len[j]);
  assert(err == DB_SUCCESS);

  // print_tuple(stderr, tpl);
  err = crsr->insert_row(sys, tpl);
  return (err);
}

/** Insert one batch of rows which constitute one transactions.
@return	number of row inserted we return on first error. */
static int insert_row_batch(System &sys, Cursor * crsr, /*!< in, out: cursor to use */
                            int n_rows,     /*!< in: num rows to insert */
                            DbErr *err)  /*!< out: error code */
{
  int i;
  int cnt = 0;
  Tuple * tpl;

  assert(n_rows <= num_rows);

  tpl = crsr->clust_read_tuple_create();
  if (tpl == nullptr) {
    *err = DB_OUT_OF_MEMORY;
    return (0);
  }
  *err = DB_SUCCESS;

  for (i = 0; i < n_rows; ++i) {
    *err = insert_one_row(sys, crsr, tpl);
    UPDATE_ERR_STATS(ins_stats, *err);

    if (*err == DB_SUCCESS) {
      ++cnt;
    } else if (*err == DB_DEADLOCK || *err == DB_LOCK_WAIT_TIMEOUT) {
      break;
    } else {
      tpl = tpl->clear();
      break;
    }
    tpl = tpl->clear();
    if (tpl == nullptr) {
      break;
    }
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  } else {
    *err = DB_OUT_OF_MEMORY;
  }

  return (cnt);
}

/** Insert worker thread. Will do the following in batches:
INSERT INTO blobt3 VALUES(System &sys, 
        RANDOM(INT),
        5,
        RANDOM(BLOB),
        RANDOM(BLOB)) */
static void *ins_worker(void *arg) /*!< in: the instance */
{
  auto &sys = *static_cast<System *>(arg);
  int cnt = 0;


  printf("ins_worker up\n");
  do {
    Trx * trx;
    Cursor * crsr;
    DbErr err;
    DbErr ins_err = DB_SUCCESS;

    ++cnt;

    // printf("Begin INSERT transaction\n");
    trx = api::trx_begin(sys, TrxLevel(isolation_level));
    assert(trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, trx, &crsr);
    assert(err == DB_SUCCESS);

    if (crsr->lock(sys, IB_LOCK_IX) == DB_SUCCESS) {
      insert_row_batch(sys, crsr, batch_size, &ins_err);
    }

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    if (ins_err == DB_DEADLOCK || ins_err == DB_LOCK_WAIT_TIMEOUT) {

      err = api::trx_release(sys, trx);
      assert(err == DB_SUCCESS);
    } else {
      commit_or_rollback(sys, trx, cnt);
    }
  } while (test_running);

  return (nullptr);
}

/** Update one row in the table.
@return	error returned from ib_crsr_moveto() or ()->update_row(sys) */
static DbErr update_one_row(System &sys, Cursor * crsr) /*!< in: cursor on the table
                                               or the secondary index */
{
  int j;
  DbErr err;
  Tuple * old_tpl;
  Tuple * new_tpl;
  ulint data_len;
  ColMeta col_meta;

  /* Create the tuple instance that we will use to update the
  table. old_tpl is used for reading the existing row and
  new_tpl will contain the update row data. */

  old_tpl = crsr->clust_read_tuple_create();
  assert(old_tpl != nullptr);

  new_tpl = crsr->clust_read_tuple_create();
  assert(new_tpl != nullptr);

  err = crsr->read_row(sys, old_tpl);
  assert(err == DB_SUCCESS);

  /* Copy the old contents to the new tuple. */
  err = new_tpl->copy(old_tpl);

  /* Update the B column in the new tuple. */
  data_len = old_tpl->get_meta(2, &col_meta);
  assert(data_len != IB_SQL_NULL);

  j = random() % num_rows;
  err = new_tpl->set_value(2, rnd_str[j], rnd_str_len[j]);
  assert(err == DB_SUCCESS);

  err = crsr->update_row(sys, old_tpl, new_tpl);

  /* delete the old and new tuple instances. */
  if (old_tpl != nullptr) {
    Tuple::destroy(old_tpl);
  }

  if (new_tpl != nullptr) {
    Tuple::destroy(new_tpl);
  }

  return (err);
}

/** Update or delete a batch of rows that constitute one transaction. */
static void process_row_batch(System &sys, Cursor * crsr,    /*!< in, out: cursor to use
                                                 for write */
                              enum op_type type, /*!< in: DML_OP_TYPE_UPDATE or
                                                 DML_OP_TYPE_DELETE */
                              int n_rows)        /*!< in: rows to update */
{
  int i;
  int key;
  int res = ~0L;
  Cursor * index_crsr;
  DbErr err = DB_SUCCESS;

  assert(n_rows <= num_rows);

  /* Open the secondary index. */
  err = crsr->open_index(sys, "SEC_1", &index_crsr);

  assert(err == DB_SUCCESS);

  err = index_crsr->set_lock_mode(sys, IB_LOCK_X);
  assert(err == DB_SUCCESS);

  index_crsr->set_cluster_access();

  for (i = 0; i < n_rows && err != DB_DEADLOCK && err != DB_LOCK_WAIT_TIMEOUT;
       ++i) {
    Tuple * sec_key_tpl;

    /* Create a tuple for searching the secondary index. */
    sec_key_tpl = index_crsr->sec_search_tuple_create();
    assert(sec_key_tpl != nullptr);

    /* Set the value to look for. */
    key = random() % num_rows;

    err = sec_key_tpl->set_value(0, &key, sizeof(key));

    assert(err == DB_SUCCESS);

    /* Search for the key using the secondary index "SEC_1" */
    err = index_crsr->moveto(sys, sec_key_tpl, IB_CUR_GE, &res);

    assert(err == DB_SUCCESS || err == DB_DEADLOCK || err == DB_END_OF_INDEX ||
           err == DB_LOCK_WAIT_TIMEOUT || err == DB_RECORD_NOT_FOUND);

    if (sec_key_tpl != nullptr) {
      Tuple::destroy(sec_key_tpl);
      sec_key_tpl = nullptr;
    }

    /* Match found in secondary index "SEC_1" */
    if (err == DB_SUCCESS && res == 0) {
      if (type == DML_OP_TYPE_UPDATE) {
        /* update the row in the table */
        err = update_one_row(sys, index_crsr);
        UPDATE_ERR_STATS(upd_stats, err);
      } else {
        /* Now delete cluster the cluster index
        row using the secondary index. */
        err = index_crsr->delete_row(sys);
        assert(err == DB_SUCCESS);
        UPDATE_ERR_STATS(del_stats, err);
      }
    }
  }

  err = Cursor::close(sys, index_crsr);
  assert(err == DB_SUCCESS);
}

/** UPDATE worker thread that does the following:
UPDATE blobt3
SET B = <random_string>
WHERE A = <random_integer>;

TODO: It may be that multiple rows match the where clause. Currently we
just update the first row that matches the where clause. */
static void *upd_worker(void *arg) /*!< in: the instance */
{
  auto &sys = *static_cast<System *>(arg);
  Trx * trx;
  Cursor * crsr;
  DbErr err;
  int cnt = 0;


  printf("upd_worker up\n");
  while (1) {
    ++cnt;
    // printf("Begin UPDATE transaction\n");
    trx = api::trx_begin(sys, TrxLevel(isolation_level));
    assert(trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, trx, &crsr);
    assert(err == DB_SUCCESS);

    if (crsr->lock(sys, IB_LOCK_IX) == DB_SUCCESS) {
      process_row_batch(sys, crsr, DML_OP_TYPE_UPDATE, batch_size);
    }

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    commit_or_rollback(sys, trx, cnt);

    if (test_running == false) {
      break;
    }
  }
  return (nullptr);
}

/** DELETE worker thread that does the following:
DELETE FROM blobt3
WHERE A = <random_integer>;

TODO: It may be that multiple rows match the where clause. Currently we
just update the first row that matches the where clause. */
static void *del_worker(void *arg) /*!< in: the instance */
{
  auto &sys = *static_cast<System *>(arg);
  int cnt = 0;


  printf("del_worker up\n");
  do {
    Trx * trx;
    Cursor * crsr;
    DbErr err;

    ++cnt;
    // printf("Begin DELETE transaction\n");
    trx = api::trx_begin(sys, TrxLevel(isolation_level));
    assert(trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, trx, &crsr);
    assert(err == DB_SUCCESS);

    if (crsr->lock(sys, IB_LOCK_IX) == DB_SUCCESS) {
      process_row_batch(sys, crsr, DML_OP_TYPE_DELETE, batch_size);
    }

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    commit_or_rollback(sys, trx, cnt);
  } while (test_running != false);

  return (nullptr);
}

/** SELECT * FROM blobt3; */
static DbErr do_query(System &sys, Cursor * crsr) {
  DbErr err;
  Tuple * tpl;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  err = crsr->first(sys);
  assert(err == DB_SUCCESS || err == DB_END_OF_INDEX);

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
      break;
    }

    err = crsr->next(sys);

    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX ||
           err == DB_RECORD_NOT_FOUND);

    UPDATE_ERR_STATS(sel_stats, err);
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

/** SELECT * FROM blobt3; */
static void *sel_worker(void *arg) /*!< in: the instance */
{
  auto &sys = *static_cast<System *>(arg);
  Cursor * crsr;
  DbErr err;


  printf("sel_worker up\n");

  do {
    Trx * trx;

    // printf("Begin SELECT transaction\n");
    trx = api::trx_begin(sys, TrxLevel(isolation_level));
    assert(trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, trx, &crsr);
    assert(err == DB_SUCCESS);

    do_query(sys, crsr);

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    if (api::trx_state(trx) == IB_TRX_NOT_STARTED) {
      err = api::trx_release(sys, trx);
      assert(err == DB_SUCCESS);
    } else {
      err = api::trx_commit(sys, trx);
      assert(err == DB_SUCCESS);
    }

  } while (test_running);

  return (nullptr);
}

#if 0
/** dummy worker */
static
void* dummy_worker(
	void*	dummy)		/*!< in: unused */
{
	while (1) {
		usleep(100000);
		if (test_running == false) {
			return(NULL);
		}
	}
	return(NULL);
}
#endif

/** Create worker threads of one type.
@return	number of threads created */
static int
create_dml_threads(System &sys, int ind,             /*!< in: index in tid array */
                   void *(*fn)(void *)) /*!< in: worker thread function */
{
  int rc;
  int count = 0;
  int i;
  for (i = 0; i < NUM_THREADS / 4; ++i) {
    rc = pthread_create(&tid[ind + i], nullptr, fn, &sys);
    assert(!rc);
    ++count;
  }
  return (count);
}

/** Create worker threads. */
static void create_worker_threads(System &sys) {
  int i = 0;

  /* comment any of these and uncomment same number
  of dummy workers below if you want to run a specific
  workload.
  For example, if you want to run just select and
  updates then comment ins_worker and del_worker
  and uncomment two lines of dummy_worker.
  If you want to change the number of each type of
  threads then change NUM_THREADS. */
  i += create_dml_threads(sys, i, ins_worker);
  i += create_dml_threads(sys, i, upd_worker);
  i += create_dml_threads(sys, i, del_worker);
  i += create_dml_threads(sys, i, sel_worker);

  // i += create_dml_threads(sys, i, dummy_worker);
  // i += create_dml_threads(sys, i, dummy_worker);
  // i += create_dml_threads(sys, i, dummy_worker);
  // i += create_dml_threads(sys, i, dummy_worker);

  assert(i == NUM_THREADS);
}

/** Initialize the structure to hold error statistics */
static void
init_dml_op_struct(dml_op_t *st) /*!< in/out: struct to initialize */
{
  int rc;
  memset(st, 0x00, sizeof(*st));
  rc = pthread_mutex_init(&st->mutex, nullptr);
  assert(!rc);
}

/** Initialize statistic structures. */
static void init_stat_structs(void) {
  init_dml_op_struct(&sel_stats);
  init_dml_op_struct(&upd_stats);
  init_dml_op_struct(&ins_stats);
  init_dml_op_struct(&del_stats);
}

/** Free up the resources used in the test */
static void clean_up(void) {
  int i;

  assert(rnd_str);
  assert(rnd_str_len);

  for (i = 0; i < num_rows; ++i) {
    assert(rnd_str[i]);
    free(rnd_str[i]);
  }

  free(rnd_str);
  free(rnd_str_len);

  pthread_mutex_destroy(&sel_stats.mutex);
  pthread_mutex_destroy(&del_stats.mutex);
  pthread_mutex_destroy(&upd_stats.mutex);
  pthread_mutex_destroy(&ins_stats.mutex);
}

/** Print statistics at the end of the test */
static void print_one_struct(dml_op_t *st) {
  int i;

  fprintf(stderr, "n_ops = %d n_err = %d\n", st->n_ops, st->n_errs);

  fprintf(stderr, "err  freq\n");
  fprintf(stderr, "=========\n");
  for (i = 0; i < DB_DATA_MISMATCH; ++i) {
    if (st->errs[i] != 0) {
      fprintf(stderr, "%d   %d\n", i, st->errs[i]);
    }
  }
  fprintf(stderr, "=========\n");
}

/** Print statistics at the end of the test */
static void print_results(void) {
  fprintf(stderr, "SELECT: ");
  print_one_struct(&sel_stats);
  fprintf(stderr, "DELETE: ");
  print_one_struct(&del_stats);
  fprintf(stderr, "UPDATE: ");
  print_one_struct(&upd_stats);
  fprintf(stderr, "INSERT: ");
  print_one_struct(&ins_stats);
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
  int cnt;
  void *res;
  DbErr err;
  Cursor * crsr;
  Trx * ib_trx;
  time_t start_time;

  (void)argc;

  srandom(time(nullptr));

  Config config;

  test_configure(config);

  set_options(config, argc, argv);

  gen_random_data();

  init_stat_structs();

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  cnt = 0;
  for (i = 0; i < num_rows; ++i) {
    /* Insert initial rows */
    // printf("Begin transaction\n");
    ib_trx = api::trx_begin(sys, TrxLevel(isolation_level));
    assert(ib_trx != nullptr);

    err = open_table(sys, DATABASE, TABLE, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    if (crsr->lock(sys, IB_LOCK_IX) == DB_SUCCESS) {
      cnt += insert_row_batch(sys, crsr, 1, &err);
    }

    err = Cursor::close(sys, crsr);
    assert(err == DB_SUCCESS);
    crsr = nullptr;

    // printf("Commit transaction\n");
    if (api::trx_state(ib_trx) == IB_TRX_NOT_STARTED) {
      err = api::trx_release(sys, ib_trx);
      assert(err == DB_SUCCESS);
    } else {
      err = api::trx_commit(sys, ib_trx);
      assert(err == DB_SUCCESS);
    }
  }

  fprintf(stderr, "initial insertions = %d\n", cnt);

  /* start the test. */
  test_running = true;
  create_worker_threads(sys);

  start_time = time(nullptr);

  /* Sleep can be interrupted by a signal. */
  do {
    /* sleep for test duration */
    if (sleep(test_time) != 0) {
      test_time -= (int)time(nullptr) - start_time;
    } else {
      break;
    }

  } while (test_time > 0);

  /* stop test and let workers exit */
  test_running = false;
  for (i = 0; i < NUM_THREADS; ++i) {
    pthread_join(tid[i], &res);
  }

  err = drop_table(sys, DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  err = drop_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  print_results();

  clean_up();

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
