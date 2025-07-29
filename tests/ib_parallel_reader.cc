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

#define ut_a(e) assert(e)
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

static ib_err_t create_database(const char *name) {
  auto err = ib_database_create(name);

  ut_a(err);

  return DB_SUCCESS;
}

static ib_err_t drop_database(const char *dbname) {
  return ib_database_drop(dbname);
}

static ib_err_t create_table(const char *dbname, const char *name) {
  ib_id_t table_id = 0;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  /* Pass a table page size of 0, ie., use default page size. */
  ib_tbl_sch_t ib_tbl_sch = nullptr;
  ib_tbl_fmt_t tbl_fmt = IB_TBL_V1;
  auto err = ib_table_schema_create(table_name, &ib_tbl_sch, tbl_fmt, 0);

  ut_a(err == DB_SUCCESS);

  /* Define the table columns */
  err = ib_tbl_sch_add_u32_col(ib_tbl_sch, "C1");
  ut_a(err == DB_SUCCESS);

  err = ib_tbl_sch_add_text_col(ib_tbl_sch, "C2");
  ut_a(err == DB_SUCCESS);

  /* Add primary key */
  ib_idx_sch_t idx_sch = nullptr;
  err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &idx_sch);
  ut_a(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "C1", 0);
  ut_a(err == DB_SUCCESS);

  err = ib_index_schema_set_clustered(idx_sch);
  ut_a(err == DB_SUCCESS);

  err = ib_index_schema_set_unique(idx_sch);
  ut_a(err == DB_SUCCESS);

  /* Add secondary indexes */
  err = ib_table_schema_add_index(ib_tbl_sch, "C2", &idx_sch);
  ut_a(err == DB_SUCCESS);

  err = ib_index_schema_add_col(idx_sch, "C2", 16);
  ut_a(err == DB_SUCCESS);

  /* Create table */
  auto ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  err = ib_schema_lock_exclusive(ib_trx);
  ut_a(err == DB_SUCCESS);

  err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);

  /* We assume it's the same table. */
  ut_a(err == DB_SUCCESS || err == DB_TABLE_EXISTS);

  err = ib_trx_commit(ib_trx);
  ut_a(err == DB_SUCCESS);

  if (ib_tbl_sch != nullptr) {
    ib_table_schema_delete(ib_tbl_sch);
  }

  return err;
}

static ib_err_t open_table(const char *dbname, const char *name,
                           ib_trx_t ib_trx, ib_crsr_t *crsr) {
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
  auto err = ib_cursor_open_table(table_name, ib_trx, crsr);
  ut_a(err == DB_SUCCESS);

  return err;
}

static void commit(ib_trx_t trx) {
  ut_a(ib_trx_state(trx) != IB_TRX_NOT_STARTED);
  log_info("Commit transaction");
  auto err = ib_trx_commit(trx);
  ut_a(err == DB_SUCCESS);
}

/** Inserts one row into the table.
@return	error from insert row */
static ib_err_t insert_one_row(ib_crsr_t crsr, ib_tpl_t tpl, Generator g) {

  auto err = ib_tuple_write_u32(tpl, 0, g());
  ut_a(err == DB_SUCCESS);

  auto i = random() % random_strings.size();
  err = ib_col_set_value(tpl, 1, random_strings[i].m_ptr,
                         random_strings[i].m_len);
  ut_a(err == DB_SUCCESS);

  return ib_cursor_insert_row(crsr, tpl);
}

/** Insert one batch of rows which constitute one transactions.
@return	an error code or success. */
static ib_err_t insert_row_batch(ib_crsr_t crsr, int n_rows, Generator g) {
  ut_a(n_rows <= NUMBER_OF_ROWS);

  auto tpl = ib_clust_read_tuple_create(crsr);
  auto err = tpl == nullptr ? DB_OUT_OF_MEMORY : DB_SUCCESS;

  for (int i = 0; i < n_rows && tpl != nullptr; ++i) {
    err = insert_one_row(crsr, tpl, g);
    ut_a(err == DB_SUCCESS);

    tpl = ib_tuple_clear(tpl);

    if (tpl == nullptr) {
      break;
    }
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  } else {
    err = DB_OUT_OF_MEMORY;
  }

  log_info("Batch: ", n_rows, " inserted");

  return err;
}

static void insert_rows(int n_rows, int batch_size, Generator g) {
  ut_a(n_rows > 0);
  ut_a(batch_size > 0);

  log_info("Starting insert of ", n_rows, " rows");

  ib_trx_t trx;

  for (int i = 0; i < n_rows; i += batch_size) {
    ib_crsr_t crsr;

    trx = ib_trx_begin(ib_trx_level_t(ISOLATION_LEVEL));
    ut_a(trx != nullptr);

    auto err = open_table(DATABASE, TABLE, trx, &crsr);
    ut_a(err == DB_SUCCESS);

    if (ib_cursor_lock(crsr, IB_LOCK_IX) == DB_SUCCESS) {
      auto err = insert_row_batch(crsr, batch_size, g);
      ut_a(err == DB_SUCCESS);
    }

    err = ib_cursor_close(crsr);
    ut_a(err == DB_SUCCESS);

    crsr = nullptr;

    commit(trx);
  }

  log_info("Total : ", n_rows, " inserted");
}

static lint do_query(ib_crsr_t crsr) {
  auto tpl = ib_clust_read_tuple_create(crsr);
  ut_a(tpl != nullptr);

  auto err = ib_cursor_first(crsr);
  ut_a(err == DB_SUCCESS || err == DB_END_OF_INDEX);

  lint n{};

  while (err == DB_SUCCESS) {
    err = ib_cursor_read_row(crsr, tpl);

    if (err == DB_END_OF_INDEX) {
      break;
    } else {
      ut_a(err == DB_SUCCESS);
    }

    err = ib_cursor_next(crsr);

    ut_a(err == DB_SUCCESS || err == DB_END_OF_INDEX);

    tpl = ib_tuple_clear(tpl);
    ut_a(tpl != nullptr);

    ++n;
  }

  if (tpl != nullptr) {
    ib_tuple_delete(tpl);
  }

  if (err == DB_END_OF_INDEX) {
    err = DB_SUCCESS;
  }

  return n;
}

static void select_all_rows() {
  log_info("SELECT * FROM ", std::format("{}.{}", DATABASE, TABLE));

  auto trx = ib_trx_begin(ib_trx_level_t(ISOLATION_LEVEL));
  ut_a(trx != nullptr);

  ib_crsr_t crsr;
  auto err = open_table(DATABASE, TABLE, trx, &crsr);
  ut_a(err == DB_SUCCESS);

  const auto n = do_query(crsr);

  err = ib_cursor_close(crsr);
  ut_a(err == DB_SUCCESS);

  crsr = nullptr;

  if (ib_trx_state(trx) == IB_TRX_NOT_STARTED) {
    err = ib_trx_release(trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = ib_trx_commit(trx);
    ut_a(err == DB_SUCCESS);
  }

  log_info("Read ", n, " rows");
}

static void select_count_star() {
  log_info("SELECT COUNT(*) FROM ", std::format("{}.{}", DATABASE, TABLE));

  auto trx = ib_trx_begin(ib_trx_level_t(ISOLATION_LEVEL));
  ut_a(trx != nullptr);

  ib_crsr_t crsr;
  auto err = open_table(DATABASE, TABLE, trx, &crsr);
  ut_a(err == DB_SUCCESS);

  std::vector<ib_crsr_t> crsrs;

  crsrs.push_back(crsr);

  uint64_t n{};

  err = ib_parallel_select_count_star(trx, crsrs, 4, n);
  ut_a(err == DB_SUCCESS);

  err = ib_cursor_close(crsr);
  ut_a(err == DB_SUCCESS);

  crsr = nullptr;

  if (ib_trx_state(trx) == IB_TRX_NOT_STARTED) {
    err = ib_trx_release(trx);
    ut_a(err == DB_SUCCESS);
  } else {
    err = ib_trx_commit(trx);
    ut_a(err == DB_SUCCESS);
  }

  log_info("Read ", n, " rows");
}

/** Set the runtime global options. */
static void set_options(int argc, char *argv[]) {
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
      if (set_global_option(opt, optarg) != DB_SUCCESS) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
      }
    }
  }

  free(longopts);
}

static void startup(int argc, char *argv[], const std::string &db,
                    const std::string &table) {
  srandom(time(nullptr));

  auto err = ib_init();
  ut_a(err == DB_SUCCESS);

  test_configure();

  set_options(argc, argv);

  gen_random_data(NUMBER_OF_UNIQUE_VARCHARS);

  err = ib_startup("default");
  ut_a(err == DB_SUCCESS);

  log_info("Creating database ", db);

  err = create_database(db.c_str());
  ut_a(err == DB_SUCCESS);

  log_info("Creating table ", table);

  err = create_table(db.c_str(), table.c_str());

  if (err == DB_TABLE_EXISTS) {
    log_info("Table ", table, " exists");
  }

  assert(err == DB_SUCCESS || err == DB_TABLE_EXISTS);
}

static void shutdown(const std::string &db, const std::string &table) {
  if (drop) {
    log_info("Dropping table ", table);
    auto err = drop_table(db.c_str(), table.c_str());
    ut_a(err == DB_SUCCESS);

    log_info("Dropping database ", db);
    err = drop_database(db.c_str());
    ut_a(err == DB_SUCCESS);
  }

  auto err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  ut_a(err == DB_SUCCESS);
}

int main(int argc, char *argv[]) {
  startup(argc, argv, DATABASE, TABLE);

  if (prepare) {
    int c1{};
    auto g = [&c1] -> int { return c1++; };

    insert_rows(NUMBER_OF_ROWS, BATCH_SIZE, g);
  }

  select_all_rows();
  select_count_star();

  shutdown(DATABASE, TABLE);

#ifdef UNIV_DEBUG_VALGRIND
  VALGRIND_DO_LEAK_CHECK;
#endif /* UNIV_DEBUG_VALGRIND */

  return EXIT_SUCCESS;
}
