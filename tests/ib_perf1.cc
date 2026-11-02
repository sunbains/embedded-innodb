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

#include <chrono>
#include <functional>
#include <thread>
#include <vector>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"

#define MILLION 1000000

typedef enum TestOp { INSERT, COPY, JOIN } TestOp;

typedef struct ib_op_time {
  time_t *time;
  int n_elems;
} TestOpTime;

typedef struct ib_op_stats {
  pthread_mutex_t mutex;

  TestOpTime copy;
  TestOpTime join;
  TestOpTime insert;
} TestOpStats;

static uint32_t n_threads = 1;
static uint32_t n_rows = MILLION;
static const uint32_t BATCH_SIZE = 10000;

/* The page size for compressed tables, if this value is > 0 then
we create compressed tables. It's set via the command line parameter
--page-size INT */
static int page_size = 0;

static TestOpStats ib_op_stats;

/* Barrier to synchronize all threads */
static pthread_barrier_t barrier;

/** Allocate memory. */
static void ib_op_time_alloc(TestOpTime *ib_op_time, int n_threads) {
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
static void ib_op_time_free(TestOpTime *ib_op_time) {
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
static void ib_stats_collect(TestOp op, time_t elapsed_time) {
  int ret;
  TestOpTime *collect = nullptr;

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
static DbErr create_database(System &sys, const char *name) {
  bool err;

  err = api::database_create(sys, name);
  assert(err == true);

  return (DB_SUCCESS);
}

/** CREATE TABLE T (c1 INT, c2 INT, PRIMARY KEY(c1)); */
static DbErr create_table(System &sys, const char *dbname, /*!< in: database name */
                             const char *name)   /*!< in: table name */
{
  Trx * ib_trx;
  uint64_t table_id = 0;
  DbErr err = DB_SUCCESS;
  TableSchema * ib_tbl_sch = nullptr;
  IndexSchema * ib_idx_sch = nullptr;
  TableFormat tbl_fmt = IB_TBL_V1;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c1", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(uint32_t));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("c2", IB_INT, IB_COL_UNSIGNED, 0,
                                sizeof(uint32_t));
  assert(err == DB_SUCCESS);

  err = ib_tbl_sch->add_index("PRIMARY", &ib_idx_sch);
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
  assert(err == DB_SUCCESS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    TableSchema::destroy(sys, ib_tbl_sch);
  }

  return (err);
}

/** Open a table and return a cursor for the table. */
static DbErr open_table(System &sys, const char *dbname, const char *name,
                           Trx * ib_trx, Cursor **crsr) {
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%.64s/%.64s", dbname, name);
  auto err = api::cursor_open_table(sys, table_name, ib_trx, crsr);
  assert(err == DB_SUCCESS);

  return err;
}

/** INSERT INTO T VALUE(i, i); */
static DbErr
insert_rows(System &sys, Cursor * crsr,    /*!< in, out: cursor to use for write */
            uint32_t start,    /*!< in: start of column value */
            uint32_t n_values) /*!< in: no. of values to insert */
{
  uint32_t i;
  Tuple * tpl = nullptr;
  DbErr err = DB_SUCCESS;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = start; i < start + n_values; ++i) {
    err = tpl->set_value(0, &i, sizeof(i));
    assert(err == DB_SUCCESS);

    err = tpl->set_value(1, &i, sizeof(i));
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);
    assert(err == DB_SUCCESS);

    /* Since we are writing fixed length columns (all INTs),
    there is no need to reset the tuple. */
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  return (err);
}

/** INSERT INTO T2 SELECT * FROM T2; */
static DbErr
copy_table(System &sys, Cursor * dst_crsr, /*!< in, out: dest table */
           Cursor * src_crsr, /*!< in, out: source table */
           uint32_t n_values)  /*!< in: no. of rows to copy in a batch */
{
  uint32_t i;
  Tuple * src_tpl = nullptr;
  Tuple * dst_tpl = nullptr;
  DbErr err = DB_SUCCESS;

  src_tpl = src_crsr->clust_read_tuple_create();
  assert(src_tpl != nullptr);

  dst_tpl = dst_crsr->clust_read_tuple_create();
  assert(dst_tpl != nullptr);

  for (i = 0; i < n_values; ++i) {
    uint32_t v;

    err = src_crsr->read_row(sys, src_tpl);
    assert(err == DB_SUCCESS);

    /* Since we know that both the tables are identical, we
    simply copy corresponding columns. */

    /* Get and set the c1 column value. */
    err = src_tpl->read_u32(0, &v);
    assert(err == DB_SUCCESS);

    err = dst_tpl->write_u32(0, v);
    assert(err == DB_SUCCESS);

    /* Get and set the c2 column value. */
    err = src_tpl->read_u32(1, &v);
    assert(err == DB_SUCCESS);

    err = dst_tpl->write_u32(1, v);
    assert(err == DB_SUCCESS);

    err = dst_crsr->insert_row(sys, dst_tpl);
    assert(err == DB_SUCCESS);

    /* Since we are writing fixed length columns (all INTs),
    there is no need to reset the tuple. */

    if ((i % 100) == 0) {
      /* The source tuple makes a copy of the record
      therefore it needs to be reset. */
      src_tpl = src_tpl->clear();
      assert(dst_tpl != nullptr);
    }

    err = src_crsr->next(sys);

    /* We don't expect any other kind of error. */
    if (err == DB_END_OF_INDEX) {
      break;
    }

    assert(err == DB_SUCCESS);
  }

  if (src_tpl != nullptr) {
    Tuple::destroy(src_tpl);
  }
  if (dst_tpl != nullptr) {
    Tuple::destroy(dst_tpl);
  }

  return (err);
}

/** SELECT COUNT(*) FROM T1, T2 WHERE T1.c1 = T2.c1; */
static DbErr join_on_c1(System &sys, Cursor * t1_crsr, /*!< in: table */
                           Cursor * t2_crsr, /*!< in: table */
                           uint32_t *count) /*!< in: no. of rows that matched */
{
  DbErr t2_err;
  DbErr t1_err;
  Trx * ib_trx;
  Tuple * t1_tpl = nullptr;
  Tuple * t2_tpl = nullptr;

  // printf("Begin transaction\n");
  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  t1_crsr->attach_trx(sys, ib_trx);
  t2_crsr->attach_trx(sys, ib_trx);

  t1_tpl = t1_crsr->clust_read_tuple_create();
  assert(t1_tpl != nullptr);

  t2_tpl = t2_crsr->clust_read_tuple_create();
  assert(t2_tpl != nullptr);

  t1_err = t1_crsr->first(sys);
  assert(t1_err == DB_SUCCESS);

  t2_err = t2_crsr->first(sys);
  assert(t2_err == DB_SUCCESS);

  /* Since we know that both tables have the same number of rows
  we iterate over the tables together. */
  while (t1_err == DB_SUCCESS && t2_err == DB_SUCCESS) {
    uint32_t t1_c1;

    t1_err = t1_crsr->read_row(sys, t1_tpl);
    assert(t1_err == DB_SUCCESS);

    t1_err = t1_tpl->read_u32(0, &t1_c1);
    assert(t1_err == DB_SUCCESS);

    while (t2_err == DB_SUCCESS) {
      uint32_t t2_c1;

      t2_err = t2_crsr->read_row(sys, t2_tpl);
      assert(t2_err == DB_SUCCESS);

      t2_err = t2_tpl->read_u32(0, &t2_c1);
      assert(t2_err == DB_SUCCESS);

      if ((*count % 100) == 0) {
        t2_tpl = t2_tpl->clear();
        assert(t2_tpl != nullptr);
      }

      if (t1_c1 == t2_c1) {
        ++*count;
        t2_err = t2_crsr->next(sys);
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
      t1_tpl = t1_tpl->clear();
      assert(t1_tpl != nullptr);
    }

    t1_err = t1_crsr->next(sys);

    /* We don't expect any other kind of error. */
    if (t1_err == DB_END_OF_INDEX) {
      break;
    }

    assert(t1_err == DB_SUCCESS);
  }

  assert(t1_err == DB_END_OF_INDEX);

  if (t1_tpl != nullptr) {
    Tuple::destroy(t1_tpl);
  }
  if (t2_tpl != nullptr) {
    Tuple::destroy(t2_tpl);
  }

  t1_err = t1_crsr->reset(sys);
  assert(t1_err == DB_SUCCESS);

  t2_err = t2_crsr->reset(sys);
  assert(t2_err == DB_SUCCESS);

  // printf("Commit transaction\n");
  t1_err = api::trx_commit(sys, ib_trx);
  assert(t1_err == DB_SUCCESS);

  return (t1_err);
}

/** Run the test. */
static void *worker_thread(System &sys, int thread_id) {
  int i;
  int ret;
  DbErr err;
  time_t end;
  time_t start;
  uint32_t count = 0;
  char table1[BUFSIZ];
  char table2[BUFSIZ];
  Cursor * src_crsr = nullptr;
  Cursor * dst_crsr = nullptr;
  bool positioned = false;

  snprintf(table1, sizeof(table1), "T%d", thread_id);
  snprintf(table2, sizeof(table2), "T%d", thread_id + 1);

  err = create_table(sys, DATABASE, table1);
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, table2);
  assert(err == DB_SUCCESS);

  err = open_table(sys, DATABASE, table1, nullptr, &src_crsr);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start insert...\n");
  }

  start = time(nullptr);
  for (i = 0; i < (int)n_rows; i += BATCH_SIZE) {
    Trx * ib_trx;

    ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    src_crsr->attach_trx(sys, ib_trx);

    err = src_crsr->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(sys, src_crsr, i, BATCH_SIZE);
    assert(err == DB_SUCCESS);

    err = src_crsr->reset(sys);
    assert(err == DB_SUCCESS);

    err = api::trx_commit(sys, ib_trx);
    assert(err == DB_SUCCESS);
  }
  end = time(nullptr);

  ib_stats_collect(INSERT, end - start);

  err = open_table(sys, DATABASE, table2, nullptr, &dst_crsr);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start copy...\n");
  }

  start = time(nullptr);
  for (i = 0; i < (int)n_rows && err == DB_SUCCESS; i += BATCH_SIZE) {
    Trx * ib_trx;

    ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    assert(ib_trx != nullptr);

    src_crsr->attach_trx(sys, ib_trx);
    dst_crsr->attach_trx(sys, ib_trx);

    err = src_crsr->lock(sys, IB_LOCK_IS);
    assert(err == DB_SUCCESS);
    err = dst_crsr->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    if (!positioned) {
      err = src_crsr->first(sys);
      assert(err == DB_SUCCESS);

      positioned = true;
    }

    err = copy_table(sys, dst_crsr, src_crsr, BATCH_SIZE);
    assert(err == DB_SUCCESS || err == DB_END_OF_INDEX);

    err = src_crsr->reset(sys);
    assert(err == DB_SUCCESS);

    err = dst_crsr->reset(sys);
    assert(err == DB_SUCCESS);

    err = api::trx_commit(sys, ib_trx);
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
  err = join_on_c1(sys, src_crsr, dst_crsr, &count);
  assert(err == DB_SUCCESS);
  end = time(nullptr);
  assert(count == n_rows);

  ib_stats_collect(JOIN, end - start);

  if (src_crsr) {
    err = Cursor::close(sys, src_crsr);
    assert(err == DB_SUCCESS);
    src_crsr = nullptr;
  }

  if (dst_crsr) {
    err = Cursor::close(sys, dst_crsr);
    assert(err == DB_SUCCESS);
    dst_crsr = nullptr;
  }

  err = drop_table(sys, DATABASE, table1);
  assert(err == DB_SUCCESS);

  err = drop_table(sys, DATABASE, table2);
  assert(err == DB_SUCCESS);

  pthread_exit(0);
}

/** Callback for qsort(3). */
static int op_time_compare(const void *p1, const void *p2) {
  return (((int)(*(time_t *)p1 - *(time_t *)p2)));
}

/** Sort the elapsed time data and  print to stdout. */
static void print_data(TestOpTime *ib_op_time, const char *title) {
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
static void set_options(Config &config, int argc, char *argv[]) {
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
      if (set_global_option(config, opt, optarg) != DB_SUCCESS) {
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
  DbErr err;
  std::vector<std::thread> threads;

  Config config;

  test_configure(config);

  set_options(config, argc, argv);

  config.m_fil.m_max_n_open_files = 8192;

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_init(&barrier, nullptr, n_threads);
  assert(ret == 0);


  ib_op_stats_alloc(n_threads);

  printf("About to spawn %d threads ", n_threads);

  for (i = 0; i < (int)n_threads; ++i) {
    threads.emplace_back(worker_thread, std::ref(sys), int(i * 2));
    printf(".");
  }

  printf("\nWaiting for threads to finish ...\n");

  for (auto &thread : threads) {
    thread.join();
  }


  ret = pthread_barrier_destroy(&barrier);
  assert(ret == 0);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  print_stats();

  ib_op_stats_free();

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
