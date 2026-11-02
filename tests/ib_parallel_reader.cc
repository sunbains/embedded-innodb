/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

/** Test the parallel reder code. It does the following:
the following:

Create a database
CREATE TABLE T1(C1 INT, C2 TEXT, PRIMARY KEY(C1), INDEX(C2(32)));

Insert <NUMBER_OF_ROWS> into the table.

Run elect SELECT COUNT((*) FROM T1;

 The test will create all the relevant sub-directories in the current
 working directory. */

#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <iostream>
#include <format>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif /* UNIV_DEBUG_VALGRIND */

#include "test0aux.h"
#include "../include/ut0logger.h"

using lint = long int;

/** Test parameters. For now hard coded. */
constexpr const char *DATABASE = "test";
constexpr const char *TABLE = "t1";

/** Total number of parallel read threads 64. */
constexpr lint NUM_THREADS = 64;

/** Initial number of rows. */
constexpr lint NUMBER_OF_ROWS = 1000000;

/** Batch size for DML. Commit after that many rows are worked upon */
constexpr lint BATCH_SIZE = 50000;

constexpr lint NUMBER_OF_UNIQUE_VARCHARS = 512;

/* isolation level for transactions */
constexpr auto ISOLATION_LEVEL = IB_TRX_REPEATABLE_READ;

/** Text column field length */
constexpr lint TEXT_LEN = 128;

struct Rnd_str {
  char *m_ptr{};
  int m_len{};
};

static constexpr char PRE_STR[][128] = {
    "kljakdjdouusdfljqwpeoljlkjpipoi66546s5df4654sf654fs654sf64i"
    "fydf645R546sf6ufujhftl;a",
    "pox", "sd"};

static std::vector<Rnd_str> random_strings{};

using Generator = std::function<int()>;

static bool drop = false;
static bool prepare = false;

/** Populate rnd_str with random strings having one of the prefixes from
the three hardcoded prefixes from PRE_STR. */
static void gen_random_data(int n) {
  ut_a(n > 0);
  ut_a(TEXT_LEN > 0);

  random_strings.resize(n);

  for (auto &rnd_str : random_strings) {
    rnd_str.m_ptr = new (std::nothrow) char[TEXT_LEN];
    ut_a(rnd_str.m_ptr);

    gen_rand_text(rnd_str.m_ptr, TEXT_LEN);
    rnd_str.m_len = strlen(rnd_str.m_ptr);
  }
}

static DbErr create_database(System &sys, const char *name) {
  auto err = api::database_create(sys, name);

  ut_a(err);

  return DB_SUCCESS;
}

static DbErr drop_database(System &sys, const char *dbname) {
  return api::database_drop(sys, dbname);
}

static DbErr create_table(System &sys, const char *dbname, const char *name) {
  uint64_t table_id = 0;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  TableSchema * ib_tbl_sch = nullptr;
  TableFormat tbl_fmt = IB_TBL_V1;
  auto err = TableSchema::create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  ut_a(err == DB_SUCCESS);

  /* Define the table columns */
  err = ib_tbl_sch->add_col("C1", IB_INT, IB_COL_UNSIGNED, 0, 4);
  ut_a(err == DB_SUCCESS);

  err = ib_tbl_sch->add_col("C2", IB_VARCHAR, IB_COL_NONE, 0, MAX_TEXT_LEN);
  ut_a(err == DB_SUCCESS);

  /* Add primary key */
  IndexSchema * idx_sch = nullptr;
  err = ib_tbl_sch->add_index("PRIMARY", &idx_sch);
  ut_a(err == DB_SUCCESS);

  err = idx_sch->add_col("C1", 0);
  ut_a(err == DB_SUCCESS);

  err = idx_sch->set_clustered();
  ut_a(err == DB_SUCCESS);

  err = idx_sch->set_unique();
  ut_a(err == DB_SUCCESS);

  /* Add secondary indexes */
  err = ib_tbl_sch->add_index("C2", &idx_sch);
  ut_a(err == DB_SUCCESS);

  err = idx_sch->add_col("C2", 16);
  ut_a(err == DB_SUCCESS);

  /* Create table */
  auto ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  err = api::schema_lock_exclusive(sys, ib_trx);
  ut_a(err == DB_SUCCESS);

  err = api::table_create(sys, ib_trx, ib_tbl_sch, &table_id);

  /* We assume it's the same table. */
  ut_a(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

  err = api::trx_commit(sys, ib_trx);
  ut_a(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    TableSchema::destroy(sys, ib_tbl_sch);
  }

  return err;
}

static DbErr open_table(System &sys, const char *dbname, const char *name,
                           Trx * ib_trx, Cursor **crsr) {
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  auto err = api::cursor_open_table(sys, table_name, ib_trx, crsr);
  ut_a(err == DB_SUCCESS);

  return err;
}

static void commit(System &sys, Trx * trx) {
  ut_a(api::trx_state(trx) != IB_TRX_NOT_STARTED);
  log_info("Commit transaction");
  auto err = api::trx_commit(sys, trx);
  ut_a(err == DB_SUCCESS);
}

/** Inserts one row into the table.
@return	error from insert row */
static DbErr insert_one_row(System &sys, Cursor * crsr, Tuple * tpl, Generator g) {

  auto err = tpl->write_u32(0, g());
  ut_a(err == DB_SUCCESS);

  auto i = random() % random_strings.size();
  err = tpl->set_value(1, random_strings[i].m_ptr,
                         random_strings[i].m_len);
  ut_a(err == DB_SUCCESS);

  return crsr->insert_row(sys, tpl);
}

/** Insert one batch of rows which constitute one transactions.
@return	an error code or success. */
static DbErr insert_row_batch(System &sys, Cursor * crsr, int n_rows, Generator g) {
  ut_a(n_rows <= NUMBER_OF_ROWS);

  auto tpl = crsr->clust_read_tuple_create();
  auto err = tpl == nullptr ? DB_OUT_OF_MEMORY : DB_SUCCESS;

  for (int i = 0; i < n_rows && tpl != nullptr; ++i) {
    err = insert_one_row(sys, crsr, tpl, g);
    ut_a(err == DB_SUCCESS);

    tpl = tpl->clear();

    if (tpl == nullptr) {
      break;
    }
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  } else {
    err = DB_OUT_OF_MEMORY;
  }

  log_info("Batch: ", n_rows, " inserted");

  return err;
}

static void insert_rows(System &sys, int n_rows, int batch_size, Generator g) {
  ut_a(n_rows > 0);
  ut_a(batch_size > 0);

  log_info("Starting insert of ", n_rows, " rows");

  Trx * trx;

  for (int i = 0; i < n_rows; i += batch_size) {
    Cursor * crsr;

    trx = api::trx_begin(sys, TrxLevel(ISOLATION_LEVEL));
    ut_a(trx != nullptr);

    auto err = open_table(sys, DATABASE, TABLE, trx, &crsr);
    ut_a(err == DB_SUCCESS);

    if (crsr->lock(sys, IB_LOCK_IX) == DB_SUCCESS) {
      auto err = insert_row_batch(sys, crsr, batch_size, g);
      ut_a(err == DB_SUCCESS);
    }

    err = Cursor::close(sys, crsr);
    ut_a(err == DB_SUCCESS);

    crsr = nullptr;

    commit(sys, trx);
  }

  log_info("Total : ", n_rows, " inserted");
}

static lint do_query(System &sys, Cursor * crsr) {
  auto tpl = crsr->clust_read_tuple_create();
  ut_a(tpl != nullptr);

  auto err = crsr->first(sys);
  ut_a(err == DB_SUCCESS || err == DB_END_OF_INDEX);

  lint n{};

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    if (err == DB_END_OF_INDEX) {
      break;
    } else {
      ut_a(err == DB_SUCCESS);
    }

    err = crsr->next(sys);

    ut_a(err == DB_SUCCESS || err == DB_END_OF_INDEX);

    tpl = tpl->clear();
    ut_a(tpl != nullptr);

    ++n;
  }

  if (tpl != nullptr) {
    Tuple::destroy(tpl);
  }

  if (err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }

  return n;
}

static void select_all_rows(System &sys) {
  log_info("SELECT * FROM ", std::format("{}.{}", DATABASE, TABLE));

  auto trx = api::trx_begin(sys, TrxLevel(ISOLATION_LEVEL));
  ut_a(trx != nullptr);

  Cursor * crsr;
  auto err = open_table(sys, DATABASE, TABLE, trx, &crsr);
  ut_a(err == DB_SUCCESS);

  const auto n = do_query(sys, crsr);

  err = Cursor::close(sys, crsr);
  ut_a(err == DB_SUCCESS);

  crsr = nullptr;

  if (api::trx_state(trx) == IB_TRX_NOT_STARTED) {
    err = api::trx_release(sys, trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = api::trx_commit(sys, trx);
    ut_a(err == DB_SUCCESS);
  }

  log_info("Read ", n, " rows");
}

static void select_count_star(System &sys) {
  log_info("SELECT COUNT(*) FROM ", std::format("{}.{}", DATABASE, TABLE));

  auto trx = api::trx_begin(sys, TrxLevel(ISOLATION_LEVEL));
  ut_a(trx != nullptr);

  Cursor * crsr;
  auto err = open_table(sys, DATABASE, TABLE, trx, &crsr);
  ut_a(err == DB_SUCCESS);

  std::vector<Cursor *> crsrs;

  crsrs.push_back(crsr);

  uint64_t n{};

  err = api::parallel_select_count_star(sys, trx, crsrs, 4, n);
  ut_a(err == DB_SUCCESS);

  err = Cursor::close(sys, crsr);
  ut_a(err == DB_SUCCESS);

  crsr = nullptr;

  if (api::trx_state(trx) == IB_TRX_NOT_STARTED) {
    err = api::trx_release(sys, trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = api::trx_commit(sys, trx);
    ut_a(err == DB_SUCCESS);
  }

  log_info("Read ", n, " rows");
}

/** Set the runtime global options. */
static void set_options(Config &config, int argc, char *argv[]) {
  int opt;
  int count{};

  /* Count the number of InnoDB system options. */
  while (ib_longopts[count].name) {
    ++count;
  }

  /* Add two of our options plus a spot for the sentinel. */
  auto size = sizeof(struct option) * (count + 3);
  auto longopts = reinterpret_cast<struct option *>(malloc(size));

  memset(longopts, 0x0, size);
  memcpy(longopts, ib_longopts, sizeof(struct option) * count);

  /* Add the local parameter --drop. */
  longopts[count].name = "drop";
  longopts[count].has_arg = no_argument;
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 1;
  ++count;

  /* Add the local parameter --prepare. */
  longopts[count].name = "prepare";
  longopts[count].has_arg = no_argument;
  longopts[count].flag = nullptr;
  longopts[count].val = USER_OPT + 2;
  ++count;

  while ((opt = getopt_long(argc, argv, "", longopts, nullptr)) != -1) {
    switch (opt) {

    case USER_OPT + 1:
      drop = true;
      break;

    case USER_OPT + 2:
      prepare = true;
      break;

    default:
      /* If it's an InnoDB parameter, then we let the auxillary function handle
       * it. */
      if (set_global_option(config, opt, optarg) != DB_SUCCESS) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
      }
    }
  }

  free(longopts);
}

static void startup(System &sys, int argc, char *argv[], const std::string &db, const std::string &table) {
  srandom(time(nullptr));

  Config config;

  test_configure(config);

  set_options(config, argc, argv);

  gen_random_data(NUMBER_OF_UNIQUE_VARCHARS);

  test_startup(sys, config);

  log_info("Creating database ", db);

  auto err = create_database(sys, db.c_str());
  ut_a(err == DB_SUCCESS);

  log_info("Creating table ", table);

  err = create_table(sys, db.c_str(), table.c_str());

  if (err == DB_TABLE_EXISTS) {
    log_info("Table ", table, " exists");
  }

  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);
}

static void shutdown(System &sys, const std::string &db, const std::string &table) {
  if (drop) {
    log_info("Dropping table ", table);
    auto err = drop_table(sys, db.c_str(), table.c_str());
    ut_a(err == DB_SUCCESS);

    log_info("Dropping database ", db);
    err = drop_database(sys, db.c_str());
    ut_a(err == DB_SUCCESS);
  }

  auto err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  ut_a(err == DB_SUCCESS);
}

int main(int argc, char *argv[]) {
  System sys;

  startup(sys, argc, argv, DATABASE, TABLE);

  if (prepare) {
    int c1{};
    auto g = [&c1] -> int { return c1++; };

    insert_rows(sys, NUMBER_OF_ROWS, BATCH_SIZE, g);
  }

  select_all_rows(sys);
  select_count_star(sys);

  shutdown(sys, DATABASE, TABLE);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif /* UNIV_DEBUG_VALGRIND */

  return EXIT_SUCCESS;
}
