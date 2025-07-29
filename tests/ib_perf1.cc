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

/* Simple multi-threaded test that does the equivalent of:
 Create a database
 CREATE TABLE Tn1(c1 INT AUTOINCREMENT, c2 INT, PK(c1));
 CREATE TABLE Tn2(c1 INT AUTOINCREMENT, c2 INT, PK(c1));
 INSERT N million rows into Tn1;

 Since the API doesn't support autoincrement, the attribute will be ignored.

 The aim of this test is to:

 1. How long does it take to copy N million rows from Tn1 to an empty Tn2 ?
    INSERT INTO Tn2 SELECT * FROM Tn1;

 2. How long does the query :
    SELECT COUNT(*) FROM Tn1, Tn2 WHERE Tn1.c1 = Tn2.c1;
    which joins n million rows take?

 The above tests measure the internal speed of the database engine.

 We startup N threads each running the above test but on a self contained
 set of tables independent of each other. The objective of the test is to
 measure any unnecessary locking issues/contention for resources. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>

#include <getopt.h> /* For getopt_long() */

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"

#define MILLION 1000000

typedef enum ib_op_t { INSERT, COPY, JOIN } ib_op_t;

typedef struct ib_op_time {
  time_t *time;
  int n_elems;
} ib_op_time_t;

typedef struct ib_op_stats {
  pthread_mutex_t mutex;

  ib_op_time_t copy;
  ib_op_time_t join;
  ib_op_time_t insert;
} ib_op_stats_t;

static uint32_t n_threads = 1;
static uint32_t n_rows = MILLION;
static const uint32_t BATCH_SIZE = 10000;

/* The page size for compressed tables, if this value is > 0 then
we create compressed tables. It's set via the command line parameter
--page-size INT */
static int page_size = 0;

static ib_op_stats_t ib_op_stats;

/* Barrier to synchronize all threads */
static pthread_barrier_t barrier;

/** Allocate memory. */
static void ib_op_time_alloc(ib_op_time_t *ib_op_time, int n_threads) {
  memset(ib_op_time, 0x0, sizeof(*ib_op_time));
  ib_op_time->time = (time_t *)malloc(sizeof(time_t) * n_threads);
}

/** Allocate memory for the global stats collector. */
static void ib_op_stats_alloc(int n_threads) {
  int ret;

  memset(&ib_op_stats, 0x0, sizeof(ib_op_stats));

  ib_op_time_alloc(&ib_op_stats.copy, n_threads);
  ib_op_time_alloc(&ib_op_stats.join, n_threads);
  ib_op_time_alloc(&ib_op_stats.insert, n_threads);

  ret = pthread_mutex_init(&ib_op_stats.mutex, nullptr);
  assert(ret == 0);
}

/** Inverse of ib_op_time_alloc() */
static void ib_op_time_free(ib_op_time_t *ib_op_time) {
  free(ib_op_time->time);
  memset(ib_op_time, 0x0, sizeof(*ib_op_time));
}

/** Free the global stats collector. */
static void ib_op_stats_free(void) {
  int ret;

  ib_op_time_free(&ib_op_stats.copy);
  ib_op_time_free(&ib_op_stats.join);
  ib_op_time_free(&ib_op_stats.insert);

  ret = pthread_mutex_destroy(&ib_op_stats.mutex);
  assert(ret == 0);
}

/** Add timing info for operation. */
static void ib_stats_collect(ib_op_t op, time_t elapsed_time) {
  int ret;
  ib_op_time_t *collect = nullptr;

  ret = pthread_mutex_lock(&ib_op_stats.mutex);
  assert(ret == 0);

  switch (op) {
  case INSERT:
    collect = &ib_op_stats.insert;
    break;
  case COPY:
    collect = &ib_op_stats.copy;
    break;
  case JOIN:
    collect = &ib_op_stats.join;
    break;
  default:
    assert(0);
  }

  assert(collect->n_elems < (int)n_threads);

  collect->time[collect->n_elems++] = elapsed_time;

  ret = pthread_mutex_unlock(&ib_op_stats.mutex);
  assert(ret == 0);
}

/** Create an InnoDB database (sub-directory). */
static ib_err_t create_database(const char *name) {
  bool err;

  err = ib_database_create(name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T (c1 INT, c2 INT, PRIMARY KEY(c1)); */
static ib_err_t create_table(const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  ib_trx_t ib_trx;
  ib_id_t table_id = 0;
  ib_err_t err = DB_SUCCESS;
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  ib_idx_sch_t ib_idx_sch = nullptr;
  ib_tbl_fmt_t tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(uint32_t));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_col(ib_tbl_sch, "c2", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(uint32_t));
  assert(err == DB_SUCCESS);

  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
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
  assert(err == DB_SUCCESS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  return (err);
}

/** Open a table and return a cursor for the table. */
static ib_err_t open_table(const char *dbname, const char *name,
                           ib_trx_t ib_trx, ib_crsr_t *crsr) {
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%.64s/%.64s", dbname, name);
  auto err = ib_cursor_open_table(table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return err;
}

/** INSERT INTO T VALUE(i, i); */
static ib_err_t
insert_rows(ib_crsr_t crsr,    /*!< in, out: cursor to use for write */
            uint32_t start,    /*!< in: start of column value */
            uint32_t n_values) /*!< in: no. of values to insert */
{
  uint32_t i;
  ib_tpl_t tpl = nullptr;
  ib_err_t err = DB_SUCCESS;

  tpl = ib_clust_read_tuple_create(crsr);
  assert(tpl != nullptr);

  for (i = start; i < start + n_values; ++i) {
    err = ib_col_set_value(tpl, 0, &i, sizeof(i));
    assert(err == DB_SUCCESS);

    err = ib_col_set_value(tpl, 1, &i, sizeof(i));
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(crsr, tpl);
    assert(err == DB_SUCCESS);

    /* Since we are writing fixed length columns (all INTs),
    there is no need to reset the tuple. */
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  return (err);
}

/** INSERT INTO T2 SELECT * FROM T2; */
static ib_err_t
copy_table(ib_crsr_t dst_crsr, /*!< in, out: dest table */
           ib_crsr_t src_crsr, /*!< in, out: source table */
           uint32_t n_values)  /*!< in: no. of rows to copy in a batch */
{
  uint32_t i;
  ib_tpl_t src_tpl = nullptr;
  ib_tpl_t dst_tpl = nullptr;
  ib_err_t err = DB_SUCCESS;

  src_tpl = ib_clust_read_tuple_create(src_crsr);
  assert(src_tpl != nullptr);

  dst_tpl = ib_clust_read_tuple_create(dst_crsr);
  assert(dst_tpl != nullptr);

  for (i = 0; i < n_values; ++i) {
    uint32_t v;

    err = ib_cursor_read_row(src_crsr, src_tpl);
    assert(err == DB_SUCCESS);

    /* Since we know that both the tables are identical, we
    simply copy corresponding columns. */

    /* Get and set the c1 column value. */
    err = ib_tuple_read_u32(src_tpl, 0, &v);
    assert(err == DB_SUCCESS);

    err = ib_tuple_write_u32(dst_tpl, 0, v);
    assert(err == DB_SUCCESS);

    /* Get and set the c2 column value. */
    err = ib_tuple_read_u32(src_tpl, 1, &v);
    assert(err == DB_SUCCESS);

    err = ib_tuple_write_u32(dst_tpl, 1, v);
    assert(err == DB_SUCCESS);

    err = ib_cursor_insert_row(dst_crsr, dst_tpl);
    assert(err == DB_SUCCESS);

    /* Since we are writing fixed length columns (all INTs),
    there is no need to reset the tuple. */

    if ((i % 100) == 0) {
      /* The source tuple makes a copy of the record
      therefore it needs to be reset. */
      src_tpl = ib_tuple_clear(src_tpl);
      assert(dst_tpl != nullptr);
    }

    err = ib_cursor_next(src_crsr);

    /* We don't expect any other kind of error. */
    if (err == DB_END_OF_INDEX) {
      break;
    }

    assert(err == DB_SUCCESS);
  }

  if (src_tpl != nullptr) {
    ib_tuple_delete(src_tpl);
  }
  if (dst_tpl != nullptr) {
    ib_tuple_delete(dst_tpl);
  }

  return (err);
}

/** SELECT COUNT(*) FROM T1, T2 WHERE T1.c1 = T2.c1; */
static ib_err_t join_on_c1(ib_crsr_t t1_crsr, /*!< in: table */
                           ib_crsr_t t2_crsr, /*!< in: table */
                           uint32_t *count) /*!< in: no. of rows that matched */
{
  ib_err_t t2_err;
  ib_err_t t1_err;
  ib_trx_t ib_trx;
  ib_tpl_t t1_tpl = nullptr;
  ib_tpl_t t2_tpl = nullptr;

  // printf("Begin transaction\n");
  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  ib_cursor_attach_trx(t1_crsr, ib_trx);
  ib_cursor_attach_trx(t2_crsr, ib_trx);

  t1_tpl = ib_clust_read_tuple_create(t1_crsr);
  assert(t1_tpl != nullptr);

  t2_tpl = ib_clust_read_tuple_create(t2_crsr);
  assert(t2_tpl != nullptr);

  t1_err = ib_cursor_first(t1_crsr);
  assert(t1_err == DB_SUCCESS);

  t2_err = ib_cursor_first(t2_crsr);
  assert(t2_err == DB_SUCCESS);

  /* Since we know that both tables have the same number of rows
  we iterate over the tables together. */
  while (t1_err == DB_SUCCESS && t2_err == DB_SUCCESS) {
    uint32_t t1_c1;

    t1_err = ib_cursor_read_row(t1_crsr, t1_tpl);
    assert(t1_err == DB_SUCCESS);

    t1_err = ib_tuple_read_u32(t1_tpl, 0, &t1_c1);
    assert(t1_err == DB_SUCCESS);

    while (t2_err == DB_SUCCESS) {
      uint32_t t2_c1;

      t2_err = ib_cursor_read_row(t2_crsr, t2_tpl);
      assert(t2_err == DB_SUCCESS);

      t2_err = ib_tuple_read_u32(t2_tpl, 0, &t2_c1);
      assert(t2_err == DB_SUCCESS);

      if ((*count % 100) == 0) {
        t2_tpl = ib_tuple_clear(t2_tpl);
        assert(t2_tpl != nullptr);
      }

      if (t1_c1 == t2_c1) {
        ++*count;
        t2_err = ib_cursor_next(t2_crsr);
        /* We don't expect any other kind of error. */
        if (t2_err == DB_END_OF_INDEX) {
          break;
        }
      } else {
        break;
      }

      assert(t2_err == DB_SUCCESS);
    }

    if ((*count % 100) == 0) {
      t1_tpl = ib_tuple_clear(t1_tpl);
      assert(t1_tpl != nullptr);
    }

    t1_err = ib_cursor_next(t1_crsr);

    /* We don't expect any other kind of error. */
    if (t1_err == DB_END_OF_INDEX) {
      break;
    }

    assert(t1_err == DB_SUCCESS);
  }

  assert(t1_err == DB_END_OF_INDEX);

  if (t1_tpl != nullptr) {
    ib_tuple_delete(t1_tpl);
  }
  if (t2_tpl != nullptr) {
    ib_tuple_delete(t2_tpl);
  }

  t1_err = ib_cursor_reset(t1_crsr);
  assert(t1_err == DB_SUCCESS);

  t2_err = ib_cursor_reset(t2_crsr);
  assert(t2_err == DB_SUCCESS);

  // printf("Commit transaction\n");
  t1_err = ib_trx_commit(ib_trx);
  assert(t1_err == DB_SUCCESS);

  return (t1_err);
}

/** Run the test. */
static void *worker_thread(void *arg) {
  int i;
  int ret;
  ib_err_t err;
  time_t end;
  time_t start;
  uint32_t count = 0;
  char table1[BUFSIZ];
  char table2[BUFSIZ];
  ib_crsr_t src_crsr = nullptr;
  ib_crsr_t dst_crsr = nullptr;
  int *table_id = (int *)arg;
  bool positioned = false;

  snprintf(table1, sizeof(table1), "T%d", *table_id);
  snprintf(table2, sizeof(table2), "T%d", *table_id + 1);

  /* We are done with the arg. */
  free(arg);
  table_id = nullptr;

  err = create_table(DATABASE, table1);
  assert(err == DB_SUCCESS);

  err = create_table(DATABASE, table2);
  assert(err == DB_SUCCESS);

  err = open_table(DATABASE, table1, nullptr, &src_crsr);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start insert...\n");
  }

  start = time(nullptr);
  for (i = 0; i < (int)n_rows; i += BATCH_SIZE) {
    ib_trx_t ib_trx;

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    ib_cursor_attach_trx(src_crsr, ib_trx);

    err = ib_cursor_lock(src_crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(src_crsr, i, BATCH_SIZE);
    assert(err == DB_SUCCESS);

    err = ib_cursor_reset(src_crsr);
    assert(err == DB_SUCCESS);

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);
  }
  end = time(nullptr);

  ib_stats_collect(INSERT, end - start);

  err = open_table(DATABASE, table2, nullptr, &dst_crsr);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start copy...\n");
  }

  start = time(nullptr);
  for (i = 0; i < (int)n_rows && err == DB_SUCCESS; i += BATCH_SIZE) {
    ib_trx_t ib_trx;

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    ib_cursor_attach_trx(src_crsr, ib_trx);
    ib_cursor_attach_trx(dst_crsr, ib_trx);

    err = ib_cursor_lock(src_crsr, IB_LOCK_IS);
    assert(err == DB_SUCCESS);
    err = ib_cursor_lock(dst_crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    if (!positioned) {
      err = ib_cursor_first(src_crsr);
      assert(err == DB_SUCCESS);

      positioned = true;
    }

    err = copy_table(dst_crsr, src_crsr, BATCH_SIZE);
    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX);

    err = ib_cursor_reset(src_crsr);
    assert(err == DB_SUCCESS);

    err = ib_cursor_reset(dst_crsr);
    assert(err == DB_SUCCESS);

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);
  }
  end = time(nullptr);

  ib_stats_collect(COPY, end - start);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start join ...\n");
  }

  start = time(nullptr);
  err = join_on_c1(src_crsr, dst_crsr, &count);
  assert(err == DB_SUCCESS);
  end = time(nullptr);
  assert(count == n_rows);

  ib_stats_collect(JOIN, end - start);

  if (src_crsr) {
    err = ib_cursor_close(src_crsr);
    assert(err == DB_SUCCESS);
    src_crsr = nullptr;
  }

  if (dst_crsr) {
    err = ib_cursor_close(dst_crsr);
    assert(err == DB_SUCCESS);
    dst_crsr = nullptr;
  }

  err = drop_table(DATABASE, table1);
  assert(err == DB_SUCCESS);

  err = drop_table(DATABASE, table2);
  assert(err == DB_SUCCESS);

  pthread_exit(0);
}

/** Callback for qsort(3). */
static int op_time_compare(const void *p1, const void *p2) {
  return (((int)(*(time_t *)p1 - *(time_t *)p2)));
}

/** Sort the elapsed time data and  print to stdout. */
static void print_data(ib_op_time_t *ib_op_time, const char *title) {
  int i;
  double avg = 0.0;
  double stdev = 0.0;
  time_t *elapsed_time = ib_op_time->time;

  qsort(elapsed_time, ib_op_time->n_elems, sizeof(*elapsed_time),
        op_time_compare);

  printf("%s\n", title);

  for (i = 0; i < ib_op_time->n_elems; ++i) {
    avg += elapsed_time[i];
    printf("%d%c", (int)elapsed_time[i],
           i < ib_op_time->n_elems - 1 ? ' ' : '\n');
  }

  avg /= ib_op_time->n_elems;

  for (i = 0; i < ib_op_time->n_elems; ++i) {
    stdev += pow(avg - elapsed_time[i], 2);
  }

  stdev = sqrt(stdev / ib_op_time->n_elems);

  printf("avg: %5.2lfs stddev: %5.2lf low: %d high: %d\n", avg, stdev,
         (int)elapsed_time[0], (int)elapsed_time[ib_op_time->n_elems - 1]);
}

/** Set the runtime global options. */
static void set_options(int argc, char *argv[]) {
  int opt;
  int optind;
  int size = 0;
  struct option *longopts;
  int count = 0;

  /* Count the number of InnoDB system options. */
  while (ib_longopts[count].name) {
    ++count;
  }

  /* Add two of our options and a spot for the sentinel. */
  size = sizeof(struct option) * (count + 4);
  longopts = (struct option *)malloc(size);
  memset(longopts, 0x0, size);
  memcpy(longopts, ib_longopts, sizeof(struct option) * count);

  /* Add the local parameters (threads, rows and page_size). */
  longopts[count].name = "threads";
  longopts[count].has_arg = required_argument;
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 1;
  ++count;

  longopts[count].name = "rows";
  longopts[count].has_arg = required_argument;
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 2;
  ++count;

  longopts[count].name = "page_size";
  longopts[count].has_arg = required_argument;
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 3;

  while ((opt = getopt_long(argc, argv, "", longopts, &optind)) != -1) {
    switch (opt) {

    case USER_OPT + 1:
      n_threads = strtoul(optarg, nullptr, 10);
      break;

    case USER_OPT + 2:
      n_rows = strtoul(optarg, nullptr, 10);
      break;

    case USER_OPT + 3:
      page_size = strtoul(optarg, nullptr, 10);
      break;

    default:
      /* If it's an InnoDB parameter, then we let the
      auxillary function handle it. */
      if (set_global_option(opt, optarg) != DB_SUCCESS) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
      }

    } /* switch */
  }

  free(longopts);
}

/** Print the statistics. */
static void print_stats(void) {
  print_data(&ib_op_stats.insert, "op: insert");
  print_data(&ib_op_stats.copy, "op: copy");
  print_data(&ib_op_stats.join, "op: join");
}

int main(int argc, char *argv[]) {
  int i;
  int ret;
  ib_err_t err;
  pthread_t *pthreads;

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  set_options(argc, argv);

  err = ib_cfg_set_int("open_files", 8192);
  assert(err == DB_SUCCESS);

  err = ib_startup("default");
  assert(err == DB_SUCCESS);

  err = create_database(DATABASE);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_init(&barrier, nullptr, n_threads);
  assert(ret == 0);

  pthreads = (pthread_t *)malloc(sizeof(*pthreads) * n_threads);
  memset(pthreads, 0, sizeof(*pthreads) * n_threads);

  ib_op_stats_alloc(n_threads);

  printf("About to spawn %d threads ", n_threads);

  for (i = 0; i < (int)n_threads; ++i) {
    int retval;
    int *ptr = (int *)malloc(sizeof(int));

    assert(ptr != nullptr);
    *ptr = i * 2;

    /* worker_thread owns the argument and is responsible for
    freeing it. */
    retval = pthread_create(&pthreads[i], nullptr, worker_thread, ptr);

    if (retval != 0) {
      fprintf(stderr,
              "Error spawning thread %d, "
              "pthread_create() returned %d\n",
              i, retval);
      exit(EXIT_FAILURE);
    }
    printf(".");
  }

  printf("\nWaiting for threads to finish ...\n");

  for (i = 0; i < (int)n_threads; ++i) {
    pthread_join(pthreads[i], nullptr);
  }

  free(pthreads);
  pthreads = nullptr;

  ret = pthread_barrier_destroy(&barrier);
  assert(ret == 0);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  print_stats();

  ib_op_stats_free();

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
