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

/* Create the conditions for a deadlock.

 Create a database
 CREATE TABLE T1(c1 INT, c2 INT, PK(c1));
 CREATE TABLE T2(c1 INT, c2 INT, PK(c1));

 In multiple threads:

 BEGIN;
 INSERT INTO Tx VALUES(1, 1);
 INSERT INTO Ty VALUES(N, N);
 -- sleep 60 seconds
 COMMIT;
*/

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

static uint32_t n_rows = 100;
static uint32_t n_threads = 2;

/* Barrier to synchronize all threads */
static pthread_barrier_t barrier;

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

/** INSERT INTO T VALUE(i, i); */
static DbErr
insert_rows(System &sys, Cursor * crsr,    /*!< in, out: cursor to use for write */
            uint32_t start,    /*!< in: start of column value */
            uint32_t n_values, /*!< in: no. of values to insert */
            int thread_id)     /*!< in: id of thread doing insert */
{
  uint32_t i;
  Tuple * tpl;
  DbErr err = DB_SUCCESS;

  tpl = crsr->clust_read_tuple_create();
  assert(tpl != nullptr);

  for (i = start; i < start + n_values; ++i) {
    err = tpl->write_u32(0, i);
    assert(err == DB_SUCCESS);

    err = tpl->write_u32(1, (uint32_t)thread_id);
    assert(err == DB_SUCCESS);

    err = crsr->insert_row(sys, tpl);

    if (err != DB_SUCCESS) {
      break;
    }

    /* Since we are writing fixed length columns (all INTs),
    there is no need to reset the tuple. */
  }

  Tuple::destroy(tpl);

  return (err);
}

/** Run the test. */
static void *worker_thread(System &sys, int thread_id) {
  int ret;
  DbErr err;
  Trx * ib_trx;
  Cursor * crsr1 = nullptr;
  Cursor * crsr2 = nullptr;
  bool deadlock = false;

  err = open_table(sys, DATABASE, "T1", nullptr, &crsr1);
  assert(err == DB_SUCCESS);

  err = open_table(sys, DATABASE, "T2", nullptr, &crsr2);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_wait(&barrier);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("Start insert...\n");
  }

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  crsr1->attach_trx(sys, ib_trx);
  crsr2->attach_trx(sys, ib_trx);

  if (!(thread_id % 2)) {
    err = crsr1->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = crsr2->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(sys, crsr1, 0, n_rows, thread_id);
    assert(err == DB_SUCCESS || err == DB_DEADLOCK ||
           err == DB_LOCK_WAIT_TIMEOUT);

    if (err == DB_SUCCESS) {
      sleep(3);

      err = insert_rows(sys, crsr2, 0, n_rows, thread_id);
      assert(err == DB_SUCCESS || err == DB_DEADLOCK ||
             err == DB_LOCK_WAIT_TIMEOUT);

      if (err == DB_SUCCESS) {
        sleep(3);
      }
    }
  } else {
    err = crsr2->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = crsr1->lock(sys, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    err = insert_rows(sys, crsr2, 0, n_rows, thread_id);
    assert(err == DB_SUCCESS || err == DB_DEADLOCK ||
           err == DB_LOCK_WAIT_TIMEOUT);

    if (err == DB_SUCCESS) {
      sleep(3);

      err = insert_rows(sys, crsr1, 0, n_rows, thread_id);
      assert(err == DB_SUCCESS || err == DB_DEADLOCK ||
             err == DB_LOCK_WAIT_TIMEOUT);

      if (err == DB_SUCCESS) {
        sleep(3);
      }
    }
  }

  if (err != DB_SUCCESS) {
    deadlock = true;
  }

  err = crsr1->reset(sys);
  assert(err == DB_SUCCESS);

  err = crsr2->reset(sys);
  assert(err == DB_SUCCESS);

  if (!deadlock) {
    /* If all went well then the transaction should still
    be active, we need to commit it. */
    assert(api::trx_state(ib_trx) == IB_TRX_ACTIVE);
    err = api::trx_commit(sys, ib_trx);
    assert(err == DB_SUCCESS);
    printf("Thread#%d - trx committed.\n", thread_id);
  } else {
    /* The transaction should have been rolled back
    by InnoDB, we can only release the handle now. */
    assert(api::trx_state(ib_trx) != IB_TRX_ACTIVE);
    err = api::trx_release(sys, ib_trx);
    assert(err == DB_SUCCESS);

    printf("Thread#%d - deadlock, trx rolled back.\n", thread_id);
  }

  if (crsr1) {
    err = Cursor::close(sys, crsr1);
    assert(err == DB_SUCCESS);
    crsr1 = nullptr;
  }

  if (crsr2) {
    err = Cursor::close(sys, crsr2);
    assert(err == DB_SUCCESS);
    crsr2 = nullptr;
  }

  pthread_exit(0);
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

/** Create the tables required for the test
@return DB_SUCCESS if all went well.*/
static DbErr create_tables(System &sys) {
  DbErr err;

  err = create_table(sys, DATABASE, "T1");
  assert(err == DB_SUCCESS);

  err = create_table(sys, DATABASE, "T2");
  assert(err == DB_SUCCESS);

  return (err);
}

/** Drop the tables required for the test
@return DB_SUCCESS if all went well.*/
static DbErr drop_tables(System &sys) {
  DbErr err;

  err = drop_table(sys, DATABASE, "T1");
  assert(err == DB_SUCCESS);

  err = drop_table(sys, DATABASE, "T2");
  assert(err == DB_SUCCESS);

  return (err);
}

int main(int argc, char *argv[]) {
  int ret;
  DbErr err;
  std::vector<std::thread> threads;

  Config config;

  test_configure(config);

  set_options(config, argc, argv);

  config.m_fil.m_max_n_open_files = 8192;

  /* Reduce the timeout to trigger lock timeout quickly. */
  config.m_lock.m_wait_timeout = std::chrono::seconds(3);

  System sys;

  test_startup(sys, config);

  err = create_database(sys, DATABASE);
  assert(err == DB_SUCCESS);

  err = create_tables(sys);
  assert(err == DB_SUCCESS);

  ret = pthread_barrier_init(&barrier, nullptr, n_threads);
  assert(ret == 0);


  printf("About to spawn %d threads ", n_threads);

  for (uint32_t i = 0; i < n_threads; ++i) {
    threads.emplace_back(worker_thread, std::ref(sys), int(i));
    printf(".");
  }

  printf("\nWaiting for threads to finish ...\n");

  for (auto &thread : threads) {
    thread.join();
  }


  ret = pthread_barrier_destroy(&barrier);
  assert(ret == 0);

  err = drop_tables(sys);
  assert(err == DB_SUCCESS);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif

  return (EXIT_SUCCESS);
}
