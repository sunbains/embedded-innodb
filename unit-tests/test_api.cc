/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file unit-tests/test_api.cc
The C++ API replacements of the removed C API facilities:
  - Config and Config::normalize() replace ib_cfg_set()/ib_cfg_get().
  - logger::level replaces ib_logger_set().
  - log_fatal() replaces the panic handler: a fatal error terminates.
  - api::trx_interrupt() replaces the trx_is_interrupted callback.

Then the api:: functions themselves: schemas, DDL, every column type, tuples,
cursors, DML, transactions, isolation, locks, savepoints, indexes, statistics,
status variables. And their performance (ApiPerf). */

#include "api0api.h"
#include "db0err.h"
#include "srv0cfg.h"
#include "srv0srv.h"
#include "srv0sys.h"
#include "trx0sys.h"
#include "ut0env.h"
#include "ut0logger.h"
#include "ut0perf.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <random>
#include <ranges>
#include <variant>
#include <filesystem>
#include <format>
#include <future>
#include <thread>

namespace fs = std::filesystem;

using namespace std::chrono_literals;

/** Replaces the ib_cfg test: the configuration is validated and completed by
Config::normalize(), invalid values are rejected with DB_INVALID_INPUT. */
TEST(ConfigTest, DefaultsAreValid) {
  Config config;

  EXPECT_EQ(config.normalize(), DB_SUCCESS);
}

TEST(ConfigTest, DataHomeGetsATrailingSeparator) {
  Config config;

  config.m_fil.m_data_home = "/some/path";
  ASSERT_EQ(config.normalize(), DB_SUCCESS);
  EXPECT_EQ(config.m_fil.m_data_home, "/some/path/");

  /* Normalizing again leaves it alone. */
  ASSERT_EQ(config.normalize(), DB_SUCCESS);
  EXPECT_EQ(config.m_fil.m_data_home, "/some/path/");
}

TEST(ConfigTest, EmptyDataHomeIsRejected) {
  Config config;

  config.m_fil.m_data_home.clear();
  EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);
}

TEST(ConfigTest, OldLruRatioMustBeFiveToNinetyFivePercent) {
  for (ulint pct = 0; pct <= 100; ++pct) {
    Config config;

    config.m_buf_pool.m_lru_old_blocks_pct = pct;

    if (pct >= 5 && pct <= 95) {
      ASSERT_EQ(config.normalize(), DB_SUCCESS) << pct;
      EXPECT_EQ(config.m_buf_pool.m_lru_old_blocks_pct, pct);
    } else {
      EXPECT_EQ(config.normalize(), DB_INVALID_INPUT) << pct;
    }
  }
}

TEST(ConfigTest, ValuesWithoutLimitsAreKept) {
  Config config;

  config.m_buf_pool.m_lru_block_access_recency = 123;
  config.m_fil.m_max_n_open_files = 123;
  config.m_buf_pool.m_size = 0xFFFFFFFFUL - 5;

  ASSERT_EQ(config.normalize(), DB_SUCCESS);
  EXPECT_EQ(config.m_buf_pool.m_lru_block_access_recency, 123UL);
  EXPECT_EQ(config.m_fil.m_max_n_open_files, 123UL);
  EXPECT_EQ(config.m_buf_pool.m_size, 0xFFFFFFFFUL - 5);
}

TEST(ConfigTest, DerivedValuesAreFilledIn) {
  Config config;

  config.m_buf_pool.m_size = 32 * 1024 * 1024;
  config.m_lock.m_table_size = 0;
  config.m_srv.m_max_n_threads = 0;

  ASSERT_EQ(config.normalize(), DB_SUCCESS);
  EXPECT_EQ(config.m_lock.m_table_size, 5 * (config.m_buf_pool.m_size / UNIV_PAGE_SIZE));
  EXPECT_EQ(config.m_srv.m_max_n_threads, 4096UL);

  /* Values set by the caller win. */
  config.m_lock.m_table_size = 77;
  config.m_srv.m_max_n_threads = 99;

  ASSERT_EQ(config.normalize(), DB_SUCCESS);
  EXPECT_EQ(config.m_lock.m_table_size, 77UL);
  EXPECT_EQ(config.m_srv.m_max_n_threads, 99UL);
}

TEST(ConfigTest, CheckpointAgeIsChecked) {
  Config config;

  /* The page flushing paces itself by the checkpoint age: it must not be tiny. */
  config.m_log.m_checkpoint_age = 1024;
  EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);

  config.m_log.m_checkpoint_age = 1024 * 1024;
  EXPECT_EQ(config.normalize(), DB_SUCCESS);
}

TEST(ConfigTest, TooManyIoThreadsAreRejected) {
  Config config;

  config.m_aio.m_n_read_threads = SRV_MAX_N_IO_THREADS;
  EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);
}

/** Replaces the panic handler test: a fatal error terminates the process,
whatever the log level. */
TEST(FatalDeathTest, FatalErrorTerminates) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  EXPECT_DEATH(log_fatal("simulated corruption"), "simulated corruption");
}

TEST(FatalDeathTest, FatalErrorTerminatesWhenLoggingIsOff) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  EXPECT_DEATH(
    {
      logger::level = int(logger::Fatal) + 1;
      log_fatal("not printed");
    },
    ""
  );
}

/** A database in a directory of its own. */
class ApiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_dir = ut::scratch_root() / std::format("innodb-api-test-{}", ::getpid());
    fs::remove_all(m_dir);
    fs::create_directories(m_dir);

    m_config.m_fil.m_data_home = m_dir.string();
    m_config.m_log.m_log_dir = m_dir.string();
    m_config.m_buf_pool.m_size = 32 * 1024 * 1024;

    /* Lock waits must end by interruption, not by timeout. */
    m_config.m_lock.m_wait_timeout = 60s;
  }

  void TearDown() override {
    logger::level = m_saved_level;
    fs::remove_all(m_dir);
  }

  /** CREATE TABLE test/t (c1 INT UNSIGNED, PRIMARY KEY(c1)) */
  void create_table(System &sys) {
    ASSERT_TRUE(api::database_create(sys, "test"));

    api::TableSchema *tbl_sch{};
    api::IndexSchema *idx_sch{};

    ASSERT_EQ(api::TableSchema::create("test/t", &tbl_sch, IB_TBL_V1), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_col("c1", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);
    ASSERT_EQ(idx_sch->add_col("c1", 0), DB_SUCCESS);
    ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    uint64_t table_id;

    ASSERT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);
    ASSERT_EQ(api::table_create(sys, trx, tbl_sch, &table_id), DB_SUCCESS);
    ASSERT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

    api::TableSchema::destroy(sys, tbl_sch);
  }

  /** INSERT INTO test/t VALUES (c1) in trx, the cursor is closed after. */
  static DbErr insert(System &sys, Trx *trx, uint32_t c1) {
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(sys, "test/t", trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IX);

    if (err == DB_SUCCESS) {
      auto tpl = crsr->clust_read_tuple_create();

      err = tpl->write_u32(0, c1);

      if (err == DB_SUCCESS) {
        err = crsr->insert_row(sys, tpl);
      }

      api::Tuple::destroy(tpl);
    }

    (void)api::Cursor::close(sys, crsr);

    return err;
  }

  fs::path m_dir;
  Config m_config;
  int m_saved_level{logger::level};
};

/** Replaces the ib_logger test: the application chooses which messages are
written with logger::level, the engine starts and stops with logging off. */
TEST_F(ApiTest, StartsAndStopsWithLoggingOff) {
  logger::level = int(logger::Fatal);

  ::testing::internal::CaptureStderr();

  {
    System sys;

    ASSERT_EQ(sys.init(m_config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }

  const auto output = ::testing::internal::GetCapturedStderr();

  EXPECT_EQ(output.find(" inf "), std::string::npos) << output;
  EXPECT_EQ(output.find(" wrn "), std::string::npos) << output;
}

/** Replaces the trx_is_interrupted test: a transaction waiting for a row lock
is interrupted from another thread, its insert fails with DB_INTERRUPTED and
it can still be rolled back. The holder of the lock is not affected. */
TEST_F(ApiTest, InterruptEndsALockWait) {
  System sys;

  ASSERT_EQ(sys.init(m_config), DB_SUCCESS);
  ASSERT_EQ(sys.startup(), DB_SUCCESS);

  create_table(sys);

  /* The holder inserts 1 and keeps its implicit lock on it. */
  auto holder = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(insert(sys, holder, 1), DB_SUCCESS);

  /* The waiter's insert of 1 must wait for the holder. */
  auto waiter = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  auto result = std::async(std::launch::async, [&] { return insert(sys, waiter, 1); });

  ASSERT_EQ(result.wait_for(2s), std::future_status::timeout) << "the insert did not wait";

  const auto start = std::chrono::steady_clock::now();

  api::trx_interrupt(waiter);

  /* The lock timeout thread looks at the waits once a second. */
  ASSERT_EQ(result.wait_for(10s), std::future_status::ready) << "the interrupt did not end the wait";
  EXPECT_EQ(result.get(), DB_INTERRUPTED);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 10s);

  EXPECT_EQ(api::trx_rollback(sys, waiter), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(sys, holder), DB_SUCCESS);

  /* The row of the holder is there: inserting it again is a duplicate. */
  auto check = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  EXPECT_EQ(insert(sys, check, 1), DB_DUPLICATE_KEY);
  EXPECT_EQ(api::trx_rollback(sys, check), DB_SUCCESS);

  ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
}

/* ------------------------------------------------------------------------
The api:: functions are the unit under test below. One instance is shared by
the tests of a suite (starting and stopping one costs seconds): each test
works on tables of its own, named after the test. */

namespace {

/** A column of a table to create. */
struct ColDef {
  const char *m_name;
  ColType m_type;
  ColAttr m_attr;
  ulint m_len;
};

/** An index of a table to create. */
struct IdxDef {
  const char *m_name;
  std::vector<const char *> m_cols;
  bool m_clustered{};
  bool m_unique{};
};

/** A tuple, destroyed at the end of the scope. */
struct Tpl {
  explicit Tpl(api::Tuple *tpl) : m_tpl(tpl) {}

  ~Tpl() {
    if (m_tpl != nullptr) {
      api::Tuple::destroy(m_tpl);
    }
  }

  Tpl(const Tpl &) = delete;
  Tpl &operator=(const Tpl &) = delete;

  api::Tuple *operator->() const { return m_tpl; }

  api::Tuple *get() const { return m_tpl; }

  /** Tuple::clear() returns a new tuple in the same heap. */
  void clear() { m_tpl = m_tpl->clear(); }

  api::Tuple *m_tpl;
};

/** A cursor on a table, closed at the end of the scope. */
struct Crsr {
  Crsr(System &sys, Trx *trx, const std::string &name, CursorLockMode mode = IB_LOCK_IX) : m_sys(sys) {
    m_err = api::cursor_open_table(sys, name.c_str(), trx, &m_crsr);

    if (m_err == DB_SUCCESS && mode != IB_LOCK_NONE) {
      m_err = m_crsr->lock(sys, mode);
    }
  }

  ~Crsr() { close(); }

  Crsr(const Crsr &) = delete;
  Crsr &operator=(const Crsr &) = delete;

  void close() {
    if (m_crsr != nullptr) {
      EXPECT_EQ(api::Cursor::close(m_sys, m_crsr), DB_SUCCESS);
      m_crsr = nullptr;
    }
  }

  api::Cursor *operator->() const { return m_crsr; }

  System &m_sys;
  api::Cursor *m_crsr{};
  DbErr m_err{DB_ERROR};
};

/** Create a table, the schema lock is taken in a trx of its own. */
DbErr create_table(
  System &sys, const std::string &name, const std::vector<ColDef> &cols, const std::vector<IdxDef> &idxs, uint64_t *id = nullptr
) {
  api::TableSchema *tbl_sch{};

  if (auto err = api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1); err != DB_SUCCESS) {
    return err;
  }

  auto err = DB_SUCCESS;

  for (const auto &col : cols) {
    if (err == DB_SUCCESS) {
      err = tbl_sch->add_col(col.m_name, col.m_type, col.m_attr, 0, col.m_len);
    }
  }

  for (const auto &idx : idxs) {
    api::IndexSchema *idx_sch{};

    if (err == DB_SUCCESS) {
      err = tbl_sch->add_index(idx.m_name, &idx_sch);
    }

    for (auto col : idx.m_cols) {
      if (err == DB_SUCCESS) {
        err = idx_sch->add_col(col, 0);
      }
    }

    if (err == DB_SUCCESS && idx.m_clustered) {
      err = idx_sch->set_clustered();
    }

    if (err == DB_SUCCESS && idx.m_unique) {
      err = idx_sch->set_unique();
    }
  }

  if (err == DB_SUCCESS) {
    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    uint64_t table_id{};

    err = api::schema_lock_exclusive(sys, trx);

    if (err == DB_SUCCESS) {
      err = api::table_create(sys, trx, tbl_sch, &table_id);
    }

    if (err == DB_SUCCESS) {
      EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
    } else {
      EXPECT_EQ(api::trx_rollback(sys, trx), DB_SUCCESS);
    }

    if (id != nullptr) {
      *id = table_id;
    }
  }

  api::TableSchema::destroy(sys, tbl_sch);

  return err;
}

/** DROP TABLE name in a trx of its own. */
DbErr drop_table(System &sys, const std::string &name) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  auto err = api::schema_lock_exclusive(sys, trx);

  if (err == DB_SUCCESS) {
    err = api::table_drop(sys, trx, name.c_str());
  }

  if (err == DB_SUCCESS) {
    EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
  } else {
    EXPECT_EQ(api::trx_rollback(sys, trx), DB_SUCCESS);
  }

  return err;
}

/** CREATE TABLE name (k INT UNSIGNED, v INT UNSIGNED, PRIMARY KEY(k), INDEX V(v)) */
DbErr create_kv(System &sys, const std::string &name, bool sec = true) {
  std::vector<IdxDef> idxs{{"PRIMARY", {"k"}, true}};

  if (sec) {
    idxs.push_back({"V", {"v"}});
  }

  return create_table(sys, name, {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"v", IB_INT, IB_COL_UNSIGNED, 4}}, idxs);
}

/** INSERT INTO name VALUES (k, v), a table made by create_kv(). */
DbErr insert_kv(System &sys, Trx *trx, const std::string &name, uint32_t k, uint32_t v) {
  Crsr crsr(sys, trx, name);

  if (crsr.m_err != DB_SUCCESS) {
    return crsr.m_err;
  }

  Tpl tpl(crsr->clust_read_tuple_create());

  EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
  EXPECT_EQ(tpl->write_u32(1, v), DB_SUCCESS);

  return crsr->insert_row(sys, tpl.get());
}

/** Position crsr, on the clustered index of a create_kv() table, on k. */
DbErr seek_kv(System &sys, api::Cursor *crsr, uint32_t k) {
  Tpl key(crsr->clust_search_tuple_create());
  int res{};

  EXPECT_EQ(key->write_u32(0, k), DB_SUCCESS);

  auto err = crsr->moveto(sys, key.get(), IB_CUR_GE, &res);

  if (err == DB_SUCCESS && res != 0) {
    err = DB_RECORD_NOT_FOUND;
  } else if (err == DB_END_OF_INDEX) {
    err = DB_RECORD_NOT_FOUND;
  }

  return err;
}

/** UPDATE name SET v = v WHERE k = k. */
DbErr update_kv(System &sys, Trx *trx, const std::string &name, uint32_t k, uint32_t v) {
  Crsr crsr(sys, trx, name);

  if (crsr.m_err != DB_SUCCESS) {
    return crsr.m_err;
  }

  auto err = crsr->set_lock_mode(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    err = seek_kv(sys, crsr.m_crsr, k);
  }

  if (err == DB_SUCCESS) {
    Tpl old_tpl(crsr->clust_read_tuple_create());
    Tpl new_tpl(crsr->clust_read_tuple_create());

    err = crsr->read_row(sys, old_tpl.get());

    if (err == DB_SUCCESS) {
      err = new_tpl->copy(old_tpl.get());
    }

    if (err == DB_SUCCESS) {
      err = new_tpl->write_u32(1, v);
    }

    if (err == DB_SUCCESS) {
      err = crsr->update_row(sys, old_tpl.get(), new_tpl.get());
    }
  }

  return err;
}

/** DELETE FROM name WHERE k = k. */
DbErr delete_kv(System &sys, Trx *trx, const std::string &name, uint32_t k) {
  Crsr crsr(sys, trx, name);

  if (crsr.m_err != DB_SUCCESS) {
    return crsr.m_err;
  }

  auto err = crsr->set_lock_mode(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    err = seek_kv(sys, crsr.m_crsr, k);
  }

  if (err == DB_SUCCESS) {
    err = crsr->delete_row(sys);
  }

  return err;
}

/** SELECT v FROM name WHERE k = k: the value, or nullopt with the error in err. */
std::optional<uint32_t> select_kv(System &sys, Trx *trx, const std::string &name, uint32_t k, DbErr *err = nullptr) {
  Crsr crsr(sys, trx, name, IB_LOCK_IS);
  DbErr e = crsr.m_err;
  std::optional<uint32_t> v;

  if (e == DB_SUCCESS) {
    e = seek_kv(sys, crsr.m_crsr, k);
  }

  if (e == DB_SUCCESS) {
    Tpl tpl(crsr->clust_read_tuple_create());
    uint32_t val{};

    e = crsr->read_row(sys, tpl.get());

    if (e == DB_SUCCESS) {
      e = tpl->read_u32(1, &val);
      v = val;
    }
  }

  if (err != nullptr) {
    *err = e;
  }

  return v;
}

/** @return the (k, v) rows of a create_kv() table visible to trx, in k order. */
std::vector<std::pair<uint32_t, uint32_t>> scan_kv(System &sys, Trx *trx, const std::string &name) {
  Crsr crsr(sys, trx, name, IB_LOCK_IS);
  std::vector<std::pair<uint32_t, uint32_t>> rows;

  EXPECT_EQ(crsr.m_err, DB_SUCCESS);

  if (crsr.m_err != DB_SUCCESS) {
    return rows;
  }

  Tpl tpl(crsr->clust_read_tuple_create());

  for (auto err = crsr->first(sys); err == DB_SUCCESS; err = crsr->next(sys)) {
    uint32_t k{}, v{};

    EXPECT_EQ(crsr->read_row(sys, tpl.get()), DB_SUCCESS);
    EXPECT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
    EXPECT_EQ(tpl->read_u32(1, &v), DB_SUCCESS);

    rows.emplace_back(k, v);
    tpl.clear();
  }

  return rows;
}

/** Insert the rows (k, v(k)) for k in [from, to) in one committed trx. */
void load_kv(System &sys, const std::string &name, uint32_t from, uint32_t to, const std::function<uint32_t(uint32_t)> &v) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  Crsr crsr(sys, trx, name);

  ASSERT_EQ(crsr.m_err, DB_SUCCESS);

  Tpl tpl(crsr->clust_read_tuple_create());

  for (auto k = from; k < to; ++k) {
    ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    ASSERT_EQ(tpl->write_u32(1, v(k)), DB_SUCCESS);
    ASSERT_EQ(crsr->insert_row(sys, tpl.get()), DB_SUCCESS) << k;
  }

  crsr.close();

  ASSERT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
}

/** A started instance shared by the tests of a suite. Lock waits time out
after a second. */
class ApiDb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    s_dir = new ut::TestDir();

    auto config = ut::make_config(s_dir->m_path);

    config.m_lock.m_wait_timeout = std::chrono::seconds(1);

    s_sys = new System();

    ASSERT_EQ(s_sys->init(config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
    ASSERT_TRUE(api::database_create(*s_sys, "test"));
  }

  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      delete s_sys;
      s_sys = nullptr;
    }

    delete s_dir;
    s_dir = nullptr;
  }

  void SetUp() override { ASSERT_NE(s_sys, nullptr); }

  /** @return a table name of the test: test/<test name><suffix> */
  static std::string table(const char *suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  Trx *begin(TrxLevel level = IB_TRX_REPEATABLE_READ) { return api::trx_begin(m_sys, level); }

  void commit(Trx *trx) { EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS); }

  void rollback(Trx *trx) { EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS); }

  static inline ut::TestDir *s_dir{};
  static inline System *s_sys{};

  System &m_sys{*s_sys};
};

}  // namespace

/* ---- Schemas: no instance needed. ---- */

/** A table name is "db/table": anything else is rejected. */
TEST(ApiSchema, TableNames) {
  api::TableSchema *sch{};

  for (auto name : {"t", "/t", "db/", "a/b/c", "./t", "../t", "x"}) {
    EXPECT_EQ(api::TableSchema::create(name, &sch, IB_TBL_V1), DB_DATA_MISMATCH) << name;
  }

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);
  EXPECT_STREQ(sch->m_name, "db/t");
  EXPECT_EQ(sch->m_tbl_fmt, IB_TBL_V1);

  /* destroy() only uses the System for a schema read from a table. */
  System none;

  api::TableSchema::destroy(none, sch);
}

/** Column names are unique, case insensitive. */
TEST(ApiSchema, DuplicateColumn) {
  api::TableSchema *sch{};

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);
  EXPECT_EQ(sch->add_col("c1", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  EXPECT_EQ(sch->add_col("c1", IB_INT, IB_COL_NONE, 0, 4), DB_DUPLICATE_KEY);
  EXPECT_EQ(sch->add_col("C1", IB_VARCHAR, IB_COL_NONE, 0, 4), DB_DUPLICATE_KEY);
  EXPECT_EQ(sch->m_cols->size(), 1UL);

  System none;

  api::TableSchema::destroy(none, sch);
}

/** The lengths of the fixed size types are checked. */
TEST(ApiSchema, ColumnLengths) {
  api::TableSchema *sch{};

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);

  for (ulint len : {1, 2, 4, 8}) {
    EXPECT_EQ(sch->add_col(std::format("i{}", len).c_str(), IB_INT, IB_COL_NONE, 0, len), DB_SUCCESS) << len;
  }

  for (ulint len : {0, 3, 5, 16}) {
    EXPECT_EQ(sch->add_col(std::format("bad{}", len).c_str(), IB_INT, IB_COL_NONE, 0, len), DB_ERROR) << len;
  }

  EXPECT_EQ(sch->add_col("f", IB_FLOAT, IB_COL_NONE, 0, 8), DB_ERROR);
  EXPECT_EQ(sch->add_col("f", IB_FLOAT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  EXPECT_EQ(sch->add_col("d", IB_DOUBLE, IB_COL_NONE, 0, 4), DB_ERROR);
  EXPECT_EQ(sch->add_col("d", IB_DOUBLE, IB_COL_NONE, 0, 8), DB_SUCCESS);
  EXPECT_EQ(sch->add_col("vc", IB_VARCHAR, IB_COL_NONE, 0, 0), DB_ERROR);
  EXPECT_EQ(sch->add_col("ch", IB_CHAR, IB_COL_NONE, 0, 0), DB_ERROR);
  EXPECT_EQ(sch->add_col("bn", IB_BINARY, IB_COL_NONE, 0, 0), DB_ERROR);

  /* A BLOB has no maximum length. */
  EXPECT_EQ(sch->add_col("b", IB_BLOB, IB_COL_NONE, 0, 0), DB_SUCCESS);

  /* The longest column name is 192 bytes. */
  EXPECT_EQ(sch->add_col(std::string(api::IB_MAX_COL_NAME_LEN, 'n').c_str(), IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  EXPECT_EQ(sch->add_col(std::string(api::IB_MAX_COL_NAME_LEN + 1, 'm').c_str(), IB_INT, IB_COL_NONE, 0, 4), DB_ERROR);

  System none;

  api::TableSchema::destroy(none, sch);
}

/** GEN_CLUST_INDEX and temporary index names are reserved; index names are unique. */
TEST(ApiSchema, IndexNames) {
  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("c", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);

  EXPECT_EQ(sch->add_index("GEN_CLUST_INDEX", &idx), DB_INVALID_INPUT);
  EXPECT_EQ(sch->add_index("gen_clust_index", &idx), DB_INVALID_INPUT);
  EXPECT_EQ(sch->add_index("\377tmp", &idx), DB_INVALID_INPUT);

  EXPECT_EQ(sch->add_index("I", &idx), DB_SUCCESS);
  EXPECT_EQ(sch->add_index("i", &idx), DB_DUPLICATE_KEY);
  EXPECT_EQ(sch->m_indexes->size(), 1UL);

  System none;

  api::TableSchema::destroy(none, sch);
}

/** Index columns: once each, must exist, no prefix on numbers. */
TEST(ApiSchema, IndexColumns) {
  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("i", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("d", IB_DOUBLE, IB_COL_NONE, 0, 8), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("dec", IB_DECIMAL, IB_COL_NONE, 0, 10), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("s", IB_VARCHAR, IB_COL_NONE, 0, 64), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("I", &idx), DB_SUCCESS);

  EXPECT_EQ(idx->add_col("i", 0), DB_SUCCESS);
  EXPECT_EQ(idx->add_col("i", 0), DB_COL_APPEARS_TWICE_IN_INDEX);
  EXPECT_EQ(idx->add_col("I", 0), DB_COL_APPEARS_TWICE_IN_INDEX);
  EXPECT_EQ(idx->add_col("none", 0), DB_NOT_FOUND);
  EXPECT_EQ(idx->add_col("d", 4), DB_SCHEMA_ERROR);
  EXPECT_EQ(idx->add_col("dec", 4), DB_SCHEMA_ERROR);
  EXPECT_EQ(idx->add_col("s", 8), DB_SUCCESS);
  EXPECT_EQ(idx->m_cols->size(), 2UL);
  EXPECT_EQ(idx->m_cols->at(1)->m_prefix_len, 8UL);

  System none;

  api::TableSchema::destroy(none, sch);
}

/** Only one index of a table schema is clustered, the last one set. */
TEST(ApiSchema, OneClusteredIndex) {
  api::TableSchema *sch{};
  api::IndexSchema *a{}, *b{};

  ASSERT_EQ(api::TableSchema::create("db/t", &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("c", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("A", &a), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("B", &b), DB_SUCCESS);

  EXPECT_EQ(a->set_clustered(), DB_SUCCESS);
  EXPECT_TRUE(a->m_clustered && a->m_unique);

  EXPECT_EQ(b->set_clustered(), DB_SUCCESS);
  EXPECT_TRUE(b->m_clustered);
  EXPECT_FALSE(a->m_clustered);

  /* set_unique() does not make an index clustered. */
  EXPECT_EQ(a->set_unique(), DB_SUCCESS);
  EXPECT_TRUE(a->m_unique);
  EXPECT_FALSE(a->m_clustered);

  System none;

  api::TableSchema::destroy(none, sch);
}

/** The API version is current:revision:age. */
TEST(ApiSchema, Version) {
  const auto v = api::api_version();

  EXPECT_NE(v, 0UL);
  EXPECT_EQ(v >> 48, 0UL);
}

/** Every error code has a message. */
TEST(ApiSchema, ErrorStrings) {
  for (auto err : {DB_SUCCESS, DB_ERROR, DB_DUPLICATE_KEY, DB_TABLE_NOT_FOUND, DB_LOCK_WAIT_TIMEOUT, DB_DEADLOCK,
                   DB_SCHEMA_NOT_LOCKED, DB_DATA_MISMATCH, DB_END_OF_INDEX, DB_RECORD_NOT_FOUND, DB_NO_SAVEPOINT}) {
    const auto msg = db_strerror(err);

    ASSERT_NE(msg, nullptr) << err;
    EXPECT_GT(strlen(msg), 0UL) << err;
  }

  EXPECT_STRNE(db_strerror(DB_SUCCESS), db_strerror(DB_DUPLICATE_KEY));
}

/** The status variables are known by name, case insensitive; unknown ones are not. */
TEST_F(ApiDb, StatusVariables) {
  const auto names = api::status_get_all();

  ASSERT_FALSE(names.empty());

  for (auto name : names) {
    int64_t v{-1};

    EXPECT_EQ(api::status_get_i64(m_sys, name, &v), DB_SUCCESS) << name;
    EXPECT_GE(v, 0) << name;
  }

  int64_t page_size{};

  EXPECT_EQ(api::status_get_i64(m_sys, "PAGE_SIZE", &page_size), DB_SUCCESS);
  EXPECT_EQ(page_size, int64_t(UNIV_PAGE_SIZE));

  int64_t v{42};

  EXPECT_EQ(api::status_get_i64(m_sys, "no_such_variable", &v), DB_NOT_FOUND);
  EXPECT_EQ(v, 42);
}

/** The row counters follow the DML. */
TEST_F(ApiDb, StatusRowCounters) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  int64_t ins0{}, upd0{}, del0{}, read0{};

  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_inserted", &ins0), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_updated", &upd0), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_deleted", &del0), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_read", &read0), DB_SUCCESS);

  auto trx = begin();

  for (uint32_t k = 0; k < 10; ++k) {
    ASSERT_EQ(insert_kv(m_sys, trx, table(), k, k), DB_SUCCESS);
  }

  ASSERT_EQ(update_kv(m_sys, trx, table(), 1, 100), DB_SUCCESS);
  ASSERT_EQ(delete_kv(m_sys, trx, table(), 2), DB_SUCCESS);
  EXPECT_EQ(scan_kv(m_sys, trx, table()).size(), 9UL);
  commit(trx);

  int64_t ins{}, upd{}, del{}, read{};

  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_inserted", &ins), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_updated", &upd), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_deleted", &del), DB_SUCCESS);
  ASSERT_EQ(api::status_get_i64(m_sys, "row_total_read", &read), DB_SUCCESS);

  /* Other suites may run at the same time in this process: at least. */
  EXPECT_GE(ins - ins0, 10);
  EXPECT_GE(upd - upd0, 1);
  EXPECT_GE(del - del0, 1);
  EXPECT_GE(read - read0, 9);
}

/* ---- DDL ---- */

/** DDL needs the exclusive schema lock. */
TEST_F(ApiDb, DdlNeedsSchemaLock) {
  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create(table().c_str(), &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("c", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);

  auto trx = begin();
  uint64_t id{77};

  EXPECT_EQ(api::table_create(m_sys, trx, sch, &id), DB_SCHEMA_NOT_LOCKED);
  EXPECT_EQ(api::table_drop(m_sys, trx, table().c_str()), DB_SCHEMA_NOT_LOCKED);
  EXPECT_EQ(api::index_drop(m_sys, trx, 1), DB_SCHEMA_NOT_LOCKED);
  EXPECT_EQ(api::IndexSchema::create(m_sys, trx, "I", table().c_str(), &idx), DB_SCHEMA_NOT_LOCKED);

  ASSERT_EQ(api::schema_lock_shared(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::table_create(m_sys, trx, sch, &id), DB_SCHEMA_NOT_LOCKED);
  ASSERT_EQ(api::schema_unlock(m_sys, trx), DB_SUCCESS);

  rollback(trx);
  api::TableSchema::destroy(m_sys, sch);

  uint64_t table_id{};

  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &table_id), DB_TABLE_NOT_FOUND);
}

/** The schema lock: shared, exclusive, and not both. */
TEST_F(ApiDb, SchemaLockModes) {
  auto trx = begin();

  EXPECT_FALSE(api::schema_lock_is_shared(trx));
  EXPECT_FALSE(api::schema_lock_is_exclusive(trx));
  EXPECT_EQ(api::schema_unlock(m_sys, trx), DB_SCHEMA_NOT_LOCKED);

  ASSERT_EQ(api::schema_lock_shared(m_sys, trx), DB_SUCCESS);
  EXPECT_TRUE(api::schema_lock_is_shared(trx));
  EXPECT_FALSE(api::schema_lock_is_exclusive(trx));

  /* No upgrade from shared to exclusive. */
  EXPECT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SCHEMA_NOT_LOCKED);
  EXPECT_TRUE(api::schema_lock_is_shared(trx));

  ASSERT_EQ(api::schema_unlock(m_sys, trx), DB_SUCCESS);
  EXPECT_FALSE(api::schema_lock_is_shared(trx));

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_TRUE(api::schema_lock_is_exclusive(trx));
  EXPECT_FALSE(api::schema_lock_is_shared(trx));
  ASSERT_EQ(api::schema_unlock(m_sys, trx), DB_SUCCESS);
  EXPECT_FALSE(api::schema_lock_is_exclusive(trx));

  /* The commit releases a schema lock still held. */
  ASSERT_EQ(api::schema_lock_shared(m_sys, trx), DB_SUCCESS);
  commit(trx);

  trx = begin();
  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  rollback(trx);
}

/** A table with no columns or an index with no columns can't be created. */
TEST_F(ApiDb, CreateEmptyDefinitions) {
  EXPECT_EQ(create_table(m_sys, table(), {}, {}), DB_SCHEMA_ERROR);
  EXPECT_EQ(create_table(m_sys, table(), {{"c", IB_INT, IB_COL_NONE, 4}}, {{"I", {}}}), DB_SCHEMA_ERROR);

  uint64_t id{};

  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(id, 0UL);
}

/** Creating an existing table returns DB_TABLE_EXISTS and its id. */
TEST_F(ApiDb, CreateExisting) {
  uint64_t id{}, id2{};

  ASSERT_EQ(create_table(m_sys, table(), {{"c", IB_INT, IB_COL_NONE, 4}}, {{"P", {"c"}, true}}, &id), DB_SUCCESS);
  EXPECT_NE(id, 0UL);

  api::TableSchema *sch{};

  ASSERT_EQ(api::TableSchema::create(table().c_str(), &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("x", IB_INT, IB_COL_NONE, 0, 8), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::table_create(m_sys, trx, sch, &id2), DB_TABLE_EXISTS);
  EXPECT_EQ(id2, id);
  rollback(trx);

  api::TableSchema::destroy(m_sys, sch);

  uint64_t got{};

  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &got), DB_SUCCESS);
  EXPECT_EQ(got, id);
}

/** Table ids are unique and grow. */
TEST_F(ApiDb, TableIdsGrow) {
  uint64_t prev{};

  for (int i = 0; i < 5; ++i) {
    uint64_t id{};

    ASSERT_EQ(create_table(m_sys, table(std::format("_{}", i).c_str()), {{"c", IB_INT, IB_COL_NONE, 4}}, {}, &id), DB_SUCCESS);
    EXPECT_GT(id, prev);
    prev = id;
  }

  /* An id is not reused after a drop. */
  ASSERT_EQ(drop_table(m_sys, table("_4")), DB_SUCCESS);

  uint64_t id{};

  ASSERT_EQ(create_table(m_sys, table("_4"), {{"c", IB_INT, IB_COL_NONE, 4}}, {}, &id), DB_SUCCESS);
  EXPECT_GT(id, prev);
}

/** The system column names can't be used. */
TEST_F(ApiDb, ReservedColumnNames) {
  for (auto name : {"DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"}) {
    EXPECT_NE(create_table(m_sys, table(), {{name, IB_INT, IB_COL_NONE, 4}}, {}), DB_SUCCESS) << name;

    uint64_t id{};

    EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_TABLE_NOT_FOUND) << name;
  }
}

/** A table without a primary key gets a generated clustered index: equal rows are allowed. */
TEST_F(ApiDb, NoPrimaryKey) {
  ASSERT_EQ(create_table(m_sys, table(), {{"c", IB_INT, IB_COL_UNSIGNED, 4}}, {}), DB_SUCCESS);

  auto trx = begin();

  for (int i = 0; i < 3; ++i) {
    Crsr crsr(m_sys, trx, table());
    Tpl tpl(crsr->clust_read_tuple_create());

    ASSERT_EQ(tpl->write_u32(0, 7), DB_SUCCESS);
    ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());
  int n{};

  for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys)) {
    uint32_t c{};

    ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(0, &c), DB_SUCCESS);
    EXPECT_EQ(c, 7U);
    ++n;
  }

  EXPECT_EQ(n, 3);

  crsr.close();
  commit(trx);
}

/** table_get_id finds tables by name, not other names. */
TEST_F(ApiDb, TableGetId) {
  uint64_t id{}, got{};

  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);
  EXPECT_NE(id, 0UL);

  EXPECT_EQ(api::table_get_id(m_sys, table("x").c_str(), &got), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(got, 0UL);

  /* The SYS_ tables have names without a database. */
  EXPECT_EQ(api::table_get_id(m_sys, "SYS_TABLES", &got), DB_SUCCESS);
  EXPECT_NE(got, id);
}

/** index_get_id() of an existing index. It looks the table up without taking the
dictionary mutex (as upstream did): the caller holds it, here through the exclusive
schema lock.
Regression: api/api0api.cc:3521 asserts ut_a(table->m_id == 0) and ut_a(index->m_id == 0):
upstream (7c21eac0 api0api.c:4800) checks that the HIGH 32 bits are 0. Any real
table aborts the process. */
TEST_F(ApiDb, IndexGetId) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  uint64_t table_id{}, index_id{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &table_id), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::index_get_id(m_sys, table().c_str(), "V", &index_id), DB_SUCCESS);
  rollback(trx);

  EXPECT_EQ(index_id >> 32, table_id);
}

/** index_get_id() of a missing table or index: nothing found, no id. */
TEST_F(ApiDb, IndexGetIdMissing) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  uint64_t index_id{99};
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::index_get_id(m_sys, table("x").c_str(), "V", &index_id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(index_id, 0UL);
  EXPECT_EQ(api::index_get_id(m_sys, table().c_str(), "NONE", &index_id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(index_id, 0UL);
  rollback(trx);
}

/** Drop: the table and its rows are gone, the name can be used again. */
TEST_F(ApiDb, Drop) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k; });

  ASSERT_EQ(drop_table(m_sys, table()), DB_SUCCESS);

  uint64_t id{};

  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_TABLE_NOT_FOUND);

  api::Cursor *crsr{};
  auto trx = begin();

  EXPECT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_TABLE_NOT_FOUND);
  rollback(trx);

  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  trx = begin();
  EXPECT_TRUE(scan_kv(m_sys, trx, table()).empty());
  commit(trx);
}

/** Dropping a missing table fails. */
TEST_F(ApiDb, DropMissing) {
  EXPECT_NE(drop_table(m_sys, table()), DB_SUCCESS);
}

/** Rename: the rows go with the name, the old name is free.
Regression: ddl/ddl0ddl.cc:977-979, the RENAME_CONSTRAINT_IDS procedure of
Ddl::rename_table() has "UPDATE SYS_FOREIGN_COLS SET ID = new_foreign_id WHERE ID =
foreign_id;" followed by the left-over continuation lines "SET ID = new_foreign_id
WHERE ID = foreign_id;" (upstream 7c21eac0 ddl0ddl.c:1304 had the statement over
three lines): a syntax error, and a parser error is fatal. Every rename aborts. */
TEST_F(ApiDb, Rename) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 5, [](uint32_t k) { return k * 2; });

  uint64_t id{}, id2{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_rename(m_sys, trx, table().c_str(), table("_new").c_str()), DB_SUCCESS);
  commit(trx);

  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &id2), DB_TABLE_NOT_FOUND);
  ASSERT_EQ(api::table_get_id(m_sys, table("_new").c_str(), &id2), DB_SUCCESS);
  EXPECT_EQ(id2, id);

  trx = begin();

  const auto rows = scan_kv(m_sys, trx, table("_new"));

  ASSERT_EQ(rows.size(), 5UL);
  EXPECT_EQ(rows[3], std::make_pair(3U, 6U));
  commit(trx);

  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
}

/** Rename takes the schema lock itself if the trx doesn't hold it.
Regression: as Rename, the rename aborts in the parser. */
TEST_F(ApiDb, RenameLocksSchema) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::table_rename(m_sys, trx, table().c_str(), table("_new").c_str()), DB_SUCCESS);
  EXPECT_TRUE(api::schema_lock_is_exclusive(trx));
  commit(trx);

  uint64_t id{};

  EXPECT_EQ(api::table_get_id(m_sys, table("_new").c_str(), &id), DB_SUCCESS);
}

/** Renaming onto an existing table fails and changes nothing. */
TEST_F(ApiDb, RenameOntoExisting) {
  ASSERT_EQ(create_kv(m_sys, table("_a")), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, table("_b")), DB_SUCCESS);

  uint64_t a{}, b{};

  ASSERT_EQ(api::table_get_id(m_sys, table("_a").c_str(), &a), DB_SUCCESS);
  ASSERT_EQ(api::table_get_id(m_sys, table("_b").c_str(), &b), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_NE(api::table_rename(m_sys, trx, table("_a").c_str(), table("_b").c_str()), DB_SUCCESS);
  rollback(trx);

  uint64_t a2{}, b2{};

  EXPECT_EQ(api::table_get_id(m_sys, table("_a").c_str(), &a2), DB_SUCCESS);
  EXPECT_EQ(api::table_get_id(m_sys, table("_b").c_str(), &b2), DB_SUCCESS);
  EXPECT_EQ(a2, a);
  EXPECT_EQ(b2, b);
}

/** Truncate: no rows, a new id, the same columns and indexes. */
TEST_F(ApiDb, Truncate) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 100, [](uint32_t k) { return 1000 - k; });

  uint64_t id{}, new_id{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);
  ASSERT_EQ(api::table_truncate(m_sys, table().c_str(), &new_id), DB_SUCCESS);
  EXPECT_NE(new_id, id);

  uint64_t got{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &got), DB_SUCCESS);
  EXPECT_EQ(got, new_id);

  auto trx = begin();

  EXPECT_TRUE(scan_kv(m_sys, trx, table()).empty());

  /* Both indexes work. */
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 5, 50), DB_SUCCESS);
  EXPECT_EQ(insert_kv(m_sys, trx, table(), 5, 51), DB_DUPLICATE_KEY);

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);
  EXPECT_EQ(sec->first(m_sys), DB_SUCCESS);
  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);

  crsr.close();
  commit(trx);
}

/** Truncating a missing table. */
TEST_F(ApiDb, TruncateMissing) {
  uint64_t id{5};

  EXPECT_EQ(api::table_truncate(m_sys, table().c_str(), &id), DB_TABLE_NOT_FOUND);
}

/** cursor_truncate() closes the cursor and commits. */
TEST_F(ApiDb, CursorTruncate) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k; });

  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);

  uint64_t id{};

  ASSERT_EQ(api::cursor_truncate(m_sys, &crsr, &id), DB_SUCCESS);
  EXPECT_EQ(crsr, nullptr);
  EXPECT_NE(id, 0UL);
  EXPECT_EQ(api::trx_state(trx), IB_TRX_NOT_STARTED);

  EXPECT_EQ(api::schema_unlock(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::trx_release(m_sys, trx), DB_SUCCESS);

  trx = begin();
  EXPECT_TRUE(scan_kv(m_sys, trx, table()).empty());
  commit(trx);
}

/** A database is a directory: no separators in its name; an empty name can't be dropped. */
TEST_F(ApiDb, DatabaseNames) {
  EXPECT_FALSE(api::database_create(m_sys, "a/b"));
  EXPECT_TRUE(api::database_create(m_sys, "dbn"));
  EXPECT_TRUE(api::database_create(m_sys, "dbn"));
  EXPECT_TRUE(ut::fs::is_directory(s_dir->m_path / "dbn"));
  EXPECT_EQ(api::database_drop(m_sys, ""), DB_INVALID_INPUT);
}

/** Dropping a database with one table. Tables of other databases are left alone. */
TEST_F(ApiDb, DatabaseDropOne) {
  ASSERT_TRUE(api::database_create(m_sys, "dbo"));
  ASSERT_EQ(create_kv(m_sys, "dbo/t1"), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, "dbo/t1", 0, 10, [](uint32_t k) { return k; });

  ASSERT_EQ(api::database_drop(m_sys, "dbo"), DB_SUCCESS);

  uint64_t id{};

  EXPECT_EQ(api::table_get_id(m_sys, "dbo/t1", &id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);
}

/** Dropping a database drops all its tables.
Regression: ddl/ddl0ddl.cc:1185-1230 (Ddl::drop_database): after the first table is
dropped, table_name is set to nullptr and the inner while loop "break"s, so the
outer do { } while (table_name != nullptr) ends: only the first table of the
database is dropped, DB_SUCCESS is returned (the inner "auto err" also shadows
the returned err). Upstream (7c21eac0 ddl0ddl.c:1572) loops until no table is
left and "goto loop"s only after waiting for open handles. */
TEST_F(ApiDb, DatabaseDropAll) {
  ASSERT_TRUE(api::database_create(m_sys, "dbx"));
  ASSERT_EQ(create_kv(m_sys, "dbx/t1"), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, "dbx/t2"), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, "dbx/t3"), DB_SUCCESS);

  ASSERT_EQ(api::database_drop(m_sys, "dbx"), DB_SUCCESS);

  uint64_t id{};

  EXPECT_EQ(api::table_get_id(m_sys, "dbx/t1", &id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::table_get_id(m_sys, "dbx/t2", &id), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::table_get_id(m_sys, "dbx/t3", &id), DB_TABLE_NOT_FOUND);
}

/** Visit a table: its format, user and system columns, user indexes. */
TEST_F(ApiDb, SchemaVisit) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"k", IB_INT, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL), 8}, {"s", IB_VARCHAR, IB_COL_NONE, 32}, {"u", IB_INT, IB_COL_NONE, 4}},
      {{"PRIMARY", {"k"}, true}, {"S", {"s", "u"}}}
    ),
    DB_SUCCESS
  );

  struct Seen {
    std::string m_table;
    TableFormat m_fmt{};
    int m_n_cols{}, m_n_indexes{};
    std::vector<std::tuple<std::string, ColType, ulint, ColAttr>> m_cols;
    std::vector<std::tuple<std::string, bool, bool, int>> m_indexes;
    std::vector<std::string> m_index_cols;
  } seen;

  api::SchemaVisitor visitor{};

  visitor.version = api::SchemaVisitor::TABLE_AND_INDEX_COL;
  visitor.table = [](void *arg, const char *name, TableFormat fmt, int n_cols, int n_indexes) {
    auto s = static_cast<Seen *>(arg);
    s->m_table = name;
    s->m_fmt = fmt;
    s->m_n_cols = n_cols;
    s->m_n_indexes = n_indexes;
    return 0;
  };
  visitor.table_col = [](void *arg, const char *name, ColType type, ulint len, ColAttr attr) {
    static_cast<Seen *>(arg)->m_cols.emplace_back(name, type, len, attr);
    return 0;
  };
  visitor.index = [](void *arg, const char *name, bool clustered, bool unique, int n_cols) {
    static_cast<Seen *>(arg)->m_indexes.emplace_back(name, clustered, unique, n_cols);
    return 0;
  };
  visitor.index_col = [](void *arg, const char *name, ulint) {
    static_cast<Seen *>(arg)->m_index_cols.emplace_back(name);
    return 0;
  };

  auto trx = begin();

  EXPECT_EQ(api::table_schema_visit(m_sys, trx, table().c_str(), &visitor, &seen), DB_SCHEMA_NOT_LOCKED);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_schema_visit(m_sys, trx, table().c_str(), &visitor, &seen), DB_SUCCESS);
  EXPECT_EQ(api::table_schema_visit(m_sys, trx, table("x").c_str(), &visitor, &seen), DB_TABLE_NOT_FOUND);
  rollback(trx);

  EXPECT_EQ(seen.m_table, table());
  EXPECT_EQ(seen.m_fmt, IB_TBL_V1);
  EXPECT_EQ(seen.m_n_indexes, 2);

  /* The 3 user columns, then the system columns. */
  ASSERT_GE(seen.m_cols.size(), 3UL);
  EXPECT_EQ(seen.m_n_cols, int(seen.m_cols.size()));
  EXPECT_EQ(std::get<0>(seen.m_cols[0]), "k");
  EXPECT_EQ(std::get<1>(seen.m_cols[0]), IB_INT);
  EXPECT_EQ(std::get<2>(seen.m_cols[0]), 8UL);
  EXPECT_EQ(std::get<3>(seen.m_cols[0]), ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL));
  EXPECT_EQ(std::get<0>(seen.m_cols[1]), "s");
  EXPECT_EQ(std::get<1>(seen.m_cols[1]), IB_VARCHAR);
  EXPECT_EQ(std::get<2>(seen.m_cols[1]), 32UL);
  EXPECT_EQ(std::get<0>(seen.m_cols[2]), "u");
  EXPECT_EQ(std::get<3>(seen.m_cols[2]), IB_COL_NONE);

  ASSERT_EQ(seen.m_indexes.size(), 2UL);
  EXPECT_EQ(std::get<0>(seen.m_indexes[0]), "PRIMARY");
  EXPECT_EQ(std::get<3>(seen.m_indexes[0]), 1);
  EXPECT_EQ(std::get<0>(seen.m_indexes[1]), "S");
  EXPECT_EQ(std::get<3>(seen.m_indexes[1]), 2);

  EXPECT_EQ(seen.m_index_cols, (std::vector<std::string>{"k", "s", "u"}));
}

/** The index visitor gets (clustered, unique) as declared in api0api.h.
Regression: api/api0api.cc:3959 calls visitor->index(arg, name, is_unique(), is_clustered(), n):
the two flags are swapped (also upstream, 7c21eac0 api0api.c:5490, against innodb.h:670).
A unique secondary index is reported as clustered. */
TEST_F(ApiDb, SchemaVisitIndexFlags) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"u", IB_INT, IB_COL_NONE, 4}},
      {{"PRIMARY", {"k"}, true}, {"U", {"u"}, false, true}}
    ),
    DB_SUCCESS
  );

  std::vector<std::tuple<std::string, bool, bool>> seen;
  api::SchemaVisitor visitor{};

  visitor.version = api::SchemaVisitor::TABLE_AND_INDEX;
  visitor.index = [](void *arg, const char *name, bool clustered, bool unique, int) {
    static_cast<std::vector<std::tuple<std::string, bool, bool>> *>(arg)->emplace_back(name, clustered, unique);
    return 0;
  };

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_schema_visit(m_sys, trx, table().c_str(), &visitor, &seen), DB_SUCCESS);
  rollback(trx);

  ASSERT_EQ(seen.size(), 2UL);
  EXPECT_EQ(seen[0], std::make_tuple(std::string("PRIMARY"), true, true));
  EXPECT_EQ(seen[1], std::make_tuple(std::string("U"), false, true));
}

/** A visitor that returns non-zero stops the visit with DB_ERROR. */
TEST_F(ApiDb, SchemaVisitAbort) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  int n{};
  api::SchemaVisitor visitor{};

  visitor.version = api::SchemaVisitor::TABLE_AND_INDEX_COL;
  visitor.table_col = [](void *arg, const char *, ColType, ulint, ColAttr) {
    ++*static_cast<int *>(arg);
    return 1;
  };
  visitor.index = [](void *, const char *, bool, bool, int) {
    ADD_FAILURE() << "visited an index after an abort";
    return 0;
  };

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::table_schema_visit(m_sys, trx, table().c_str(), &visitor, &n), DB_ERROR);
  rollback(trx);

  EXPECT_EQ(n, 1);
}

/** The table iterator lists every table, the SYS_ ones too, and stops when asked. */
TEST_F(ApiDb, TablesIterate) {
  ASSERT_EQ(create_kv(m_sys, table("_a")), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, table("_b")), DB_SUCCESS);

  std::vector<std::string> names;
  auto trx = begin();

  auto collect = [](void *arg, const char *name, int len) {
    static_cast<std::vector<std::string> *>(arg)->emplace_back(name, len);
    return 0;
  };

  EXPECT_EQ(api::schema_tables_iterate(m_sys, trx, collect, &names), DB_SCHEMA_NOT_LOCKED);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::schema_tables_iterate(m_sys, trx, collect, &names), DB_SUCCESS);

  EXPECT_NE(std::ranges::find(names, table("_a")), names.end());
  EXPECT_NE(std::ranges::find(names, table("_b")), names.end());
  EXPECT_NE(std::ranges::find(names, "SYS_FOREIGN"), names.end());
  EXPECT_TRUE(std::ranges::is_sorted(names));

  int n{};

  auto stop = [](void *arg, const char *, int) {
    ++*static_cast<int *>(arg);
    return 1;
  };

  ASSERT_EQ(api::schema_tables_iterate(m_sys, trx, stop, &n), DB_SUCCESS);
  EXPECT_EQ(n, 1);
  rollback(trx);
}

/* ---- Column types and tuples ---- */

namespace {

/** The integer columns of the IntTypes table, after k. */
const std::vector<ColDef> int_cols{
  {"k", IB_INT, IB_COL_UNSIGNED, 4},
  {"i8", IB_INT, IB_COL_NONE, 1},
  {"u8", IB_INT, IB_COL_UNSIGNED, 1},
  {"i16", IB_INT, IB_COL_NONE, 2},
  {"u16", IB_INT, IB_COL_UNSIGNED, 2},
  {"i32", IB_INT, IB_COL_NONE, 4},
  {"u32", IB_INT, IB_COL_UNSIGNED, 4},
  {"i64", IB_INT, IB_COL_NONE, 8},
  {"u64", IB_INT, IB_COL_UNSIGNED, 8},
};

/** A row of the IntTypes table. */
struct Ints {
  int8_t i8;
  uint8_t u8;
  int16_t i16;
  uint16_t u16;
  int32_t i32;
  uint32_t u32;
  int64_t i64;
  uint64_t u64;

  bool operator==(const Ints &) const = default;
};

template <typename T>
using Lim = std::numeric_limits<T>;

}  // namespace

/** Every integer size, signed and unsigned, keeps its extremes. */
TEST_F(ApiDb, IntExtremes) {
  ASSERT_EQ(create_table(m_sys, table(), int_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  const std::vector<Ints> rows{
    {Lim<int8_t>::min(), 0, Lim<int16_t>::min(), 0, Lim<int32_t>::min(), 0, Lim<int64_t>::min(), 0},
    {Lim<int8_t>::max(), Lim<uint8_t>::max(), Lim<int16_t>::max(), Lim<uint16_t>::max(), Lim<int32_t>::max(),
     Lim<uint32_t>::max(), Lim<int64_t>::max(), Lim<uint64_t>::max()},
    {-1, 1, -1, 1, -1, 1, -1, 1},
    {0, 0x80, 0, 0x8000, 0, 0x80000000U, 0, 0x8000000000000000ULL},
  };

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table());
    Tpl tpl(crsr->clust_read_tuple_create());

    for (uint32_t k = 0; k < rows.size(); ++k) {
      const auto &r = rows[k];

      ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
      ASSERT_EQ(tpl->write_i8(1, r.i8), DB_SUCCESS);
      ASSERT_EQ(tpl->write_u8(2, r.u8), DB_SUCCESS);
      ASSERT_EQ(tpl->write_i16(3, r.i16), DB_SUCCESS);
      ASSERT_EQ(tpl->write_u16(4, r.u16), DB_SUCCESS);
      ASSERT_EQ(tpl->write_i32(5, r.i32), DB_SUCCESS);
      ASSERT_EQ(tpl->write_u32(6, r.u32), DB_SUCCESS);
      ASSERT_EQ(tpl->write_i64(7, r.i64), DB_SUCCESS);
      ASSERT_EQ(tpl->write_u64(8, r.u64), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
    }
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());
  uint32_t k{};

  for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys), ++k) {
    Ints r{};
    uint32_t key{};

    ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(0, &key), DB_SUCCESS);
    ASSERT_EQ(tpl->read_i8(1, &r.i8), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u8(2, &r.u8), DB_SUCCESS);
    ASSERT_EQ(tpl->read_i16(3, &r.i16), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u16(4, &r.u16), DB_SUCCESS);
    ASSERT_EQ(tpl->read_i32(5, &r.i32), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(6, &r.u32), DB_SUCCESS);
    ASSERT_EQ(tpl->read_i64(7, &r.i64), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u64(8, &r.u64), DB_SUCCESS);

    EXPECT_EQ(key, k);
    ASSERT_LT(k, rows.size());
    EXPECT_EQ(r, rows[k]) << "row " << k;

    tpl.clear();
  }

  EXPECT_EQ(k, rows.size());

  crsr.close();
  commit(trx);
}

/** Reads and writes must match the size and the signedness of the column. */
TEST_F(ApiDb, IntTypeChecks) {
  ASSERT_EQ(create_table(m_sys, table(), int_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  /* Writes of the wrong size. */
  EXPECT_EQ(tpl->write_u16(0, 1), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->write_u64(0, 1), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->write_i32(1, 1), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->write_i8(7, 1), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->write_double(5, 1.0), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->write_float(5, 1.0f), DB_DATA_MISMATCH);

  ASSERT_EQ(tpl->write_i32(5, -7), DB_SUCCESS);
  ASSERT_EQ(tpl->write_u32(6, 7), DB_SUCCESS);

  /* Reads of the wrong size. */
  int16_t i16{};
  int64_t i64{};
  uint8_t u8{};
  double d{};
  float f{};

  EXPECT_EQ(tpl->read_i16(5, &i16), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->read_i64(5, &i64), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->read_u8(6, &u8), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->read_double(5, &d), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->read_float(5, &f), DB_DATA_MISMATCH);

  /* A signed read of an unsigned column is refused, the other way round is allowed. */
  int32_t i32{};
  uint32_t u32{};

  EXPECT_EQ(tpl->read_i32(6, &i32), DB_DATA_MISMATCH);
  EXPECT_EQ(tpl->read_u32(5, &u32), DB_SUCCESS);
  EXPECT_EQ(tpl->read_i32(5, &i32), DB_SUCCESS);
  EXPECT_EQ(i32, -7);

  crsr.close();
  commit(trx);
}

namespace {

/** Insert the keys into a table with an integer PK of type T, in a random order,
and check a scan returns them in numeric order. */
template <typename T>
void int_order(System &sys, const std::string &name, std::vector<T> keys) {
  const ColAttr attr = std::is_signed_v<T> ? IB_COL_NONE : IB_COL_UNSIGNED;

  ASSERT_EQ(create_table(sys, name, {{"k", IB_INT, attr, sizeof(T)}}, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  std::mt19937 rng(42);
  std::ranges::shuffle(keys, rng);

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  {
    Crsr crsr(sys, trx, name);
    Tpl tpl(crsr->clust_read_tuple_create());

    for (auto k : keys) {
      ASSERT_EQ(tpl->set_value(0, &k, sizeof(k)), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(sys, tpl.get()), DB_SUCCESS) << int64_t(k);
    }
  }

  std::vector<T> got;

  {
    Crsr crsr(sys, trx, name, IB_LOCK_IS);
    Tpl tpl(crsr->clust_read_tuple_create());

    for (auto err = crsr->first(sys); err == DB_SUCCESS; err = crsr->next(sys)) {
      T k{};

      ASSERT_EQ(crsr->read_row(sys, tpl.get()), DB_SUCCESS);
      ASSERT_EQ(tpl->copy_value(0, &k, sizeof(k)), sizeof(k));
      got.push_back(k);
    }
  }

  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  std::ranges::sort(keys);
  EXPECT_EQ(got, keys);
}

}  // namespace

/** Signed keys sort numerically, negatives first, for every size. */
TEST_F(ApiDb, IntOrderSigned8) {
  int_order<int8_t>(m_sys, table(), {-128, -127, -2, -1, 0, 1, 2, 126, 127});
}

TEST_F(ApiDb, IntOrderSigned16) {
  int_order<int16_t>(m_sys, table(), {-32768, -256, -255, -1, 0, 1, 255, 256, 32767});
}

TEST_F(ApiDb, IntOrderSigned32) {
  int_order<int32_t>(m_sys, table(), {Lim<int32_t>::min(), -65536, -1, 0, 1, 65536, Lim<int32_t>::max()});
}

TEST_F(ApiDb, IntOrderSigned64) {
  int_order<int64_t>(m_sys, table(), {Lim<int64_t>::min(), -(1LL << 32), -1, 0, 1, 1LL << 32, Lim<int64_t>::max()});
}

/** Unsigned keys with the top bit set sort after the others. */
TEST_F(ApiDb, IntOrderUnsigned64) {
  int_order<uint64_t>(m_sys, table(), {0, 1, 0x7FFFFFFFFFFFFFFFULL, 0x8000000000000000ULL, Lim<uint64_t>::max()});
}

TEST_F(ApiDb, IntOrderUnsigned8) {
  int_order<uint8_t>(m_sys, table(), {0, 1, 0x7F, 0x80, 0xFF});
}

/** FLOAT and DOUBLE values come back bit for bit and sort numerically in an index. */
TEST_F(ApiDb, FloatDouble) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"f", IB_FLOAT, IB_COL_NONE, 4}, {"d", IB_DOUBLE, IB_COL_NONE, 8}},
      {{"PRIMARY", {"k"}, true}, {"D", {"d"}}, {"F", {"f"}}}
    ),
    DB_SUCCESS
  );

  const std::vector<double> ds{1e300, -1.5, 0.0, 2.5, -1e300, Lim<double>::denorm_min(), -Lim<double>::infinity(),
                               Lim<double>::infinity()};
  const std::vector<float> fs{1e30f, -1.5f, 0.0f, 2.5f, -1e30f, Lim<float>::denorm_min(), -Lim<float>::infinity(),
                              Lim<float>::max()};

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table());
    Tpl tpl(crsr->clust_read_tuple_create());

    /* A float can't be written with set_value() of another length. */
    double wide{1.0};

    EXPECT_EQ(tpl->set_value(1, &wide, sizeof(wide)), DB_DATA_MISMATCH);

    for (uint32_t k = 0; k < ds.size(); ++k) {
      ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
      ASSERT_EQ(tpl->write_float(1, fs[k]), DB_SUCCESS);
      ASSERT_EQ(tpl->write_double(2, ds[k]), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
    }
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);

  /* Read back by primary key. */
  {
    Tpl tpl(crsr->clust_read_tuple_create());
    uint32_t k{};

    for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys), ++k) {
      float f{};
      double d{};

      ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
      ASSERT_EQ(tpl->read_float(1, &f), DB_SUCCESS);
      ASSERT_EQ(tpl->read_double(2, &d), DB_SUCCESS);
      EXPECT_EQ(f, fs[k]) << k;
      EXPECT_EQ(d, ds[k]) << k;
    }

    EXPECT_EQ(k, ds.size());
  }

  /* Secondary index order. */
  for (auto [index, col] : {std::pair("D", 2), std::pair("F", 1)}) {
    api::Cursor *sec{};

    ASSERT_EQ(crsr->open_index(m_sys, index, &sec), DB_SUCCESS);
    sec->set_cluster_access();

    Tpl tpl(sec->clust_read_tuple_create());
    std::vector<double> got;

    for (auto err = sec->first(m_sys); err == DB_SUCCESS; err = sec->next(m_sys)) {
      ASSERT_EQ(sec->read_row(m_sys, tpl.get()), DB_SUCCESS);

      if (col == 2) {
        double d{};
        ASSERT_EQ(tpl->read_double(2, &d), DB_SUCCESS);
        got.push_back(d);
      } else {
        float f{};
        ASSERT_EQ(tpl->read_float(1, &f), DB_SUCCESS);
        got.push_back(f);
      }
    }

    EXPECT_EQ(got.size(), ds.size()) << index;
    EXPECT_TRUE(std::ranges::is_sorted(got)) << index;

    EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);
  }

  crsr.close();
  commit(trx);
}

namespace {

/** The columns of the Strings table. */
const std::vector<ColDef> str_cols{
  {"k", IB_INT, IB_COL_UNSIGNED, 4},
  {"vc", IB_VARCHAR, IB_COL_NONE, 10},
  {"ch", IB_CHAR, IB_COL_NONE, 8},
  {"vb", IB_VARBINARY, IB_COL_NONE, 16},
  {"bl", IB_BLOB, IB_COL_NONE, 0},
  {"nn", IB_VARCHAR, IB_COL_NOT_NULL, 5},
  {"dec", IB_DECIMAL, IB_COL_NONE, 12},
  {"vca", IB_VARCHAR_ANYCHARSET, IB_COL_NONE, 10},
};

/** @return column i of tpl as a string, or nullopt if NULL. */
std::optional<std::string> str(const api::Tuple *tpl, ulint i) {
  const auto len = tpl->get_len(i);

  if (len == api::IB_SQL_NULL) {
    EXPECT_EQ(tpl->get_value(i), nullptr);
    return std::nullopt;
  }

  std::string s(len, '\0');

  if (len > 0) {
    EXPECT_EQ(tpl->copy_value(i, s.data(), len), len);
  }

  return s;
}

}  // namespace

/** Strings: VARCHAR is capped at its length, CHAR is padded with spaces,
binary strings keep zero bytes, a BLOB holds anything from 0 bytes to many
pages, DECIMAL is kept as given. */
TEST_F(ApiDb, StringTypes) {
  ASSERT_EQ(create_table(m_sys, table(), str_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  const std::string bin("a\0b\0\xff", 5);
  const auto big = ut::blob(1, 100000);

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table());
    Tpl tpl(crsr->clust_read_tuple_create());

    auto put = [&](uint32_t k, const std::string &vc, const std::string &ch, const std::string &vb, const std::string &bl) {
      ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(1, vc.data(), vc.size()), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(2, ch.data(), ch.size()), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(3, vb.data(), vb.size()), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(4, bl.data(), bl.size()), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(5, "x", 1), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(6, "-123.45", 7), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(7, "any", 3), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS) << k;
      tpl.clear();
    };

    put(0, "abc", "abcdefgh", bin, "");
    put(1, "0123456789abcdef", "12345678", "", big);
    put(2, "", "zzzzzzzz", std::string(16, '\xff'), std::string(1, '\0'));

    /* The cap applies when the value is set. */
    ASSERT_EQ(tpl->set_value(1, "0123456789abcdef", 16), DB_SUCCESS);
    EXPECT_EQ(tpl->get_len(1), 10UL);
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  ASSERT_EQ(crsr->first(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
  EXPECT_EQ(str(tpl.get(), 1), "abc");
  EXPECT_EQ(str(tpl.get(), 2), "abcdefgh");
  EXPECT_EQ(str(tpl.get(), 3), bin);
  EXPECT_EQ(str(tpl.get(), 4), "");
  EXPECT_EQ(str(tpl.get(), 5), "x");
  EXPECT_EQ(str(tpl.get(), 6), "-123.45");
  EXPECT_EQ(str(tpl.get(), 7), "any");

  tpl.clear();
  ASSERT_EQ(crsr->next(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
  EXPECT_EQ(str(tpl.get(), 1), "0123456789");
  EXPECT_EQ(str(tpl.get(), 2), "12345678");
  EXPECT_EQ(str(tpl.get(), 3), "");
  EXPECT_EQ(str(tpl.get(), 4), big);

  tpl.clear();
  ASSERT_EQ(crsr->next(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
  EXPECT_EQ(str(tpl.get(), 1), "");
  EXPECT_EQ(str(tpl.get(), 2), "zzzzzzzz");
  EXPECT_EQ(str(tpl.get(), 3), std::string(16, '\xff'));
  EXPECT_EQ(str(tpl.get(), 4), std::string(1, '\0'));

  EXPECT_EQ(crsr->next(m_sys), DB_END_OF_INDEX);

  crsr.close();
  commit(trx);
}

/** A CHAR value shorter than the column is padded with spaces.
Regression: api/api0api.cc:2925-2943 (Tuple::set_value, DATA_CHAR) pads dst with spaces,
sets len to the column length, then falls through to memcpy(dst, src, len): the
padding is overwritten with the bytes that follow the caller's value in memory, a
read past the end of src (also upstream, 7c21eac0 api0api.c:3971). Upstream InnoDB
copied len bytes before padding. */
TEST_F(ApiDb, CharPadding) {
  ASSERT_EQ(create_table(m_sys, table(), str_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  /* The bytes after the value are not spaces. */
  const char value[] = "ab\x01\x02\x03\x04\x05\x06";

  ASSERT_EQ(tpl->set_value(2, value, 2), DB_SUCCESS);
  EXPECT_EQ(str(tpl.get(), 2), "ab      ");

  ASSERT_EQ(tpl->set_value(2, value, 0), DB_SUCCESS);
  EXPECT_EQ(str(tpl.get(), 2), "        ");

  crsr.close();
  commit(trx);
}

/** copy_value() copies at most the given length. */
TEST_F(ApiDb, CopyValueTruncates) {
  ASSERT_EQ(create_table(m_sys, table(), str_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  ASSERT_EQ(tpl->set_value(1, "abcdef", 6), DB_SUCCESS);

  char buf[8]{};

  EXPECT_EQ(tpl->copy_value(1, buf, 3), 3UL);
  EXPECT_EQ(std::string(buf), "abc");

  crsr.close();
  commit(trx);
}

/** NULL: written, stored and read back as NULL; NOT NULL columns refuse it. */
TEST_F(ApiDb, Nulls) {
  ASSERT_EQ(create_table(m_sys, table(), str_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table());
    Tpl tpl(crsr->clust_read_tuple_create());

    /* A new tuple is all NULL. */
    for (ulint i = 0; i < str_cols.size(); ++i) {
      EXPECT_EQ(tpl->get_len(i), api::IB_SQL_NULL) << i;
    }

    ASSERT_EQ(tpl->write_u32(0, 1), DB_SUCCESS);

    /* nn is NOT NULL. */
    EXPECT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_DATA_MISMATCH);

    ASSERT_EQ(tpl->set_value(5, "y", 1), DB_SUCCESS);
    ASSERT_EQ(tpl->set_value(1, "z", 1), DB_SUCCESS);
    ASSERT_EQ(tpl->set_value(1, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
    EXPECT_EQ(tpl->get_len(1), api::IB_SQL_NULL);
    EXPECT_EQ(tpl->get_value(1), nullptr);

    char c{'#'};

    EXPECT_EQ(tpl->copy_value(1, &c, 1), api::IB_SQL_NULL);
    EXPECT_EQ(c, '#');

    ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  ASSERT_EQ(crsr->first(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);

  for (ulint i : {1, 2, 3, 4, 6, 7}) {
    EXPECT_EQ(str(tpl.get(), i), std::nullopt) << i;
  }

  EXPECT_EQ(str(tpl.get(), 5), "y");

  crsr.close();
  commit(trx);
}

/** A NULL integer: the typed read leaves the value alone. */
TEST_F(ApiDb, NullInt) {
  ASSERT_EQ(create_table(m_sys, table(), int_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  int32_t v{12345};

  EXPECT_EQ(tpl->get_len(5), api::IB_SQL_NULL);
  EXPECT_EQ(tpl->read_i32(5, &v), DB_SUCCESS);
  EXPECT_EQ(v, 12345);

  crsr.close();
  commit(trx);
}

/** A value set after a NULL, larger than the one before the NULL, gets a buffer of its size. */
TEST_F(ApiDb, SetValueAfterNull) {
  ASSERT_EQ(create_table(m_sys, table(), str_cols, {{"PRIMARY", {"k"}, true}}), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());
  const auto big = ut::blob(3, 5000);

  ASSERT_EQ(tpl->set_value(4, "ab", 2), DB_SUCCESS);
  ASSERT_EQ(tpl->set_value(4, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
  ASSERT_EQ(tpl->set_value(4, big.data(), big.size()), DB_SUCCESS);

  /* Another allocation from the tuple's heap must not overlap the BLOB. */
  ASSERT_EQ(tpl->set_value(3, std::string(16, 'z').data(), 16), DB_SUCCESS);

  EXPECT_EQ(str(tpl.get(), 4), big);

  crsr.close();
  commit(trx);
}

/** get_meta() reports the type, length and attributes of a column. */
TEST_F(ApiDb, Meta) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"k", IB_INT, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL), 8},
       {"s", IB_VARCHAR, IB_COL_CUSTOM1, 20},
       {"d", IB_DOUBLE, IB_COL_NONE, 8},
       {"b", IB_BLOB, IB_COL_NONE, 0}},
      {{"PRIMARY", {"k"}, true}}
    ),
    DB_SUCCESS
  );

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());
  ColMeta meta{};

  ASSERT_EQ(tpl->write_u64(0, 9), DB_SUCCESS);
  EXPECT_EQ(tpl->get_meta(0, &meta), 8UL);
  EXPECT_EQ(meta.type, IB_INT);
  EXPECT_EQ(meta.type_len, 8U);
  EXPECT_EQ(meta.attr, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL));

  ASSERT_EQ(tpl->set_value(1, "hello", 5), DB_SUCCESS);
  EXPECT_EQ(tpl->get_meta(1, &meta), 5UL);
  EXPECT_EQ(meta.type, IB_VARCHAR);
  EXPECT_EQ(meta.type_len, 20U);
  EXPECT_EQ(meta.attr, IB_COL_CUSTOM1);

  EXPECT_EQ(tpl->get_meta(2, &meta), api::IB_SQL_NULL);
  EXPECT_EQ(meta.type, IB_DOUBLE);
  EXPECT_EQ(meta.attr, IB_COL_NONE);

  EXPECT_EQ(tpl->get_meta(3, &meta), api::IB_SQL_NULL);
  EXPECT_EQ(meta.type, IB_BLOB);

  /* The system columns follow the user columns. */
  EXPECT_EQ(tpl->get_n_user_cols(), 4UL);
  EXPECT_EQ(tpl->get_n_cols(), 4UL + api::IB_N_SYS_COLS);

  tpl->get_meta(4, &meta);
  EXPECT_EQ(meta.type, IB_SYS);

  /* System columns can't be written. */
  EXPECT_EQ(tpl->set_value(4, "12345678", 6), DB_DATA_MISMATCH);

  /* A key tuple has the columns of the key only. */
  Tpl key(crsr->clust_search_tuple_create());

  EXPECT_EQ(key->get_n_user_cols(), 1UL);
  EXPECT_EQ(key->get_n_cols(), 1UL);
  EXPECT_EQ(key->m_type, api::TPL_KEY);
  EXPECT_EQ(tpl->m_type, api::TPL_ROW);

  crsr.close();
  commit(trx);
}

/** copy() is deep and needs the same kind of tuple of the same index; clear() makes it all NULL. */
TEST_F(ApiDb, TupleCopyClear) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl a(crsr->clust_read_tuple_create());
  Tpl b(crsr->clust_read_tuple_create());
  Tpl key(crsr->clust_search_tuple_create());

  ASSERT_EQ(a->write_u32(0, 1), DB_SUCCESS);
  ASSERT_EQ(a->write_u32(1, 2), DB_SUCCESS);
  ASSERT_EQ(b->copy(a.get()), DB_SUCCESS);

  /* Deep: changing the source leaves the copy alone. */
  ASSERT_EQ(a->write_u32(1, 3), DB_SUCCESS);

  uint32_t v{};

  ASSERT_EQ(b->read_u32(1, &v), DB_SUCCESS);
  EXPECT_EQ(v, 2U);

  EXPECT_EQ(key->copy(a.get()), DB_DATA_MISMATCH);

  /* The secondary index row tuple belongs to another index. */
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);

  {
    Tpl s(sec->sec_read_tuple_create());

    EXPECT_EQ(s->copy(a.get()), DB_DATA_MISMATCH);
    EXPECT_EQ(s->get_n_cols(), 2UL);
  }

  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);

  a.clear();
  EXPECT_EQ(a->get_len(0), api::IB_SQL_NULL);
  EXPECT_EQ(a->get_len(1), api::IB_SQL_NULL);
  EXPECT_EQ(a->m_type, api::TPL_ROW);

  key.clear();
  EXPECT_EQ(key->m_type, api::TPL_KEY);

  crsr.close();
  commit(trx);
}

/* ---- Cursors ---- */

/** Opening a missing table. */
TEST_F(ApiDb, OpenMissing) {
  auto trx = begin();
  api::Cursor *crsr{};

  EXPECT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::cursor_open_table_using_id(m_sys, 0xFFFFFFF0, trx, &crsr), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::cursor_open_index_using_id(m_sys, 0xFFFFFFF0ULL << 32 | 1, trx, &crsr), DB_TABLE_NOT_FOUND);
  rollback(trx);
}

/** A table opened by id is the table of that name. */
TEST_F(ApiDb, OpenUsingId) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 3, [](uint32_t k) { return k + 10; });

  uint64_t id{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);

  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table_using_id(m_sys, id, trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IS), DB_SUCCESS);
  ASSERT_EQ(crsr->last(m_sys), DB_SUCCESS);

  {
    Tpl tpl(crsr->clust_read_tuple_create());
    uint32_t v{};

    ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(1, &v), DB_SUCCESS);
    EXPECT_EQ(v, 12U);
  }

  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  commit(trx);
}

/** An empty table: every positioning reports the end of the index. */
TEST_F(ApiDb, EmptyTable) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());
  Tpl key(crsr->clust_search_tuple_create());
  int res{};

  EXPECT_FALSE(crsr->is_positioned());
  EXPECT_EQ(crsr->read_row(m_sys, tpl.get()), DB_RECORD_NOT_FOUND);
  EXPECT_EQ(crsr->first(m_sys), DB_END_OF_INDEX);
  EXPECT_EQ(crsr->last(m_sys), DB_END_OF_INDEX);

  ASSERT_EQ(key->write_u32(0, 5), DB_SUCCESS);
  EXPECT_EQ(crsr->moveto(m_sys, key.get(), IB_CUR_GE, &res), DB_END_OF_INDEX);

  /* An exact match finds nothing. */
  crsr->set_match_mode(IB_EXACT_MATCH);
  EXPECT_EQ(crsr->moveto(m_sys, key.get(), IB_CUR_GE, &res), DB_RECORD_NOT_FOUND);

  crsr.close();
  commit(trx);
}

/** Forward with first/next, backward with last/prev. */
TEST_F(ApiDb, ScanBothWays) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 500, [](uint32_t k) { return k * 3; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  Tpl tpl(crsr->clust_read_tuple_create());

  std::vector<uint32_t> fwd, bwd;

  for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys)) {
    uint32_t k{};

    EXPECT_TRUE(crsr->is_positioned());
    ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
    fwd.push_back(k);
  }

  for (auto err = crsr->last(m_sys); err == DB_SUCCESS; err = crsr->prev(m_sys)) {
    uint32_t k{};

    ASSERT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
    bwd.push_back(k);
  }

  ASSERT_EQ(fwd.size(), 500UL);
  EXPECT_TRUE(std::ranges::is_sorted(fwd));
  EXPECT_TRUE(std::ranges::equal(fwd, std::views::reverse(bwd)));

  crsr.close();
  commit(trx);
}

/** moveto() in each search mode, on a key that exists and one that doesn't. */
TEST_F(ApiDb, MovetoModes) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();

  for (uint32_t k : {10, 20, 30}) {
    ASSERT_EQ(insert_kv(m_sys, trx, table(), k, k), DB_SUCCESS);
  }

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);

  /* @return the key the cursor is on after moveto(k, mode), or the error. */
  auto at = [&](uint32_t k, SearchMode mode) -> std::variant<uint32_t, DbErr> {
    Tpl key(crsr->clust_search_tuple_create());
    Tpl tpl(crsr->clust_read_tuple_create());
    int res{};

    EXPECT_EQ(key->write_u32(0, k), DB_SUCCESS);

    if (auto err = crsr->moveto(m_sys, key.get(), mode, &res); err != DB_SUCCESS) {
      return err;
    }

    uint32_t got{};

    EXPECT_EQ(crsr->read_row(m_sys, tpl.get()), DB_SUCCESS);
    EXPECT_EQ(tpl->read_u32(0, &got), DB_SUCCESS);

    /* res is 0 only on an exact match. */
    EXPECT_EQ(res == 0, got == k) << "k " << k << " mode " << mode << " res " << res;

    return got;
  };

  using R = std::variant<uint32_t, DbErr>;

  EXPECT_EQ(at(20, IB_CUR_GE), R(20U));
  EXPECT_EQ(at(20, IB_CUR_G), R(30U));
  EXPECT_EQ(at(20, IB_CUR_LE), R(20U));
  EXPECT_EQ(at(20, IB_CUR_L), R(10U));

  EXPECT_EQ(at(25, IB_CUR_GE), R(30U));
  EXPECT_EQ(at(25, IB_CUR_G), R(30U));
  EXPECT_EQ(at(25, IB_CUR_LE), R(20U));
  EXPECT_EQ(at(25, IB_CUR_L), R(20U));

  EXPECT_EQ(at(5, IB_CUR_GE), R(10U));
  EXPECT_EQ(at(5, IB_CUR_L), R(DB_END_OF_INDEX));
  EXPECT_EQ(at(35, IB_CUR_LE), R(30U));
  EXPECT_EQ(at(35, IB_CUR_G), R(DB_END_OF_INDEX));

  /* next() continues from the moveto() position. */
  EXPECT_EQ(at(10, IB_CUR_GE), R(10U));
  EXPECT_EQ(crsr->next(m_sys), DB_SUCCESS);
  EXPECT_EQ(crsr->next(m_sys), DB_SUCCESS);
  EXPECT_EQ(crsr->next(m_sys), DB_END_OF_INDEX);

  crsr.close();
  commit(trx);
}

/** The match modes on a two column key: an exact match needs the whole key, a prefix match the given columns. */
TEST_F(ApiDb, MatchModes) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"a", IB_VARCHAR, IB_COL_NONE, 16}, {"b", IB_VARCHAR, IB_COL_NONE, 16}, {"c", IB_INT, IB_COL_UNSIGNED, 4}},
      {{"PRIMARY", {"a", "b"}, true}}
    ),
    DB_SUCCESS
  );

  auto trx = begin();
  Crsr crsr(m_sys, trx, table());

  {
    Tpl tpl(crsr->clust_read_tuple_create());

    for (auto [a, b] : {std::pair("abc", "def"), std::pair("abc", "zzz"), std::pair("ghi", "jkl"), std::pair("mno", "pqr")}) {
      ASSERT_EQ(tpl->set_value(0, a, strlen(a)), DB_SUCCESS);
      ASSERT_EQ(tpl->set_value(1, b, strlen(b)), DB_SUCCESS);
      ASSERT_EQ(tpl->write_u32(2, 1), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
    }
  }

  crsr.close();

  /* @return the rows read from the moveto() on (a, b) in mode to the end, with
  a cursor of its own. An exact match doesn't position the cursor on the
  row (it is in the row cache): only that row is read. */
  auto count = [&](const char *a, const char *b, MatchMode mode) {
    Crsr c(m_sys, trx, table(), IB_LOCK_IS);
    Tpl key(c->clust_search_tuple_create());
    Tpl tpl(c->clust_read_tuple_create());
    int res{};
    int n{};

    EXPECT_EQ(key->set_value(0, a, strlen(a)), DB_SUCCESS);

    if (b != nullptr) {
      EXPECT_EQ(key->set_value(1, b, strlen(b)), DB_SUCCESS);
    }

    c->set_match_mode(mode);

    for (auto err = c->moveto(m_sys, key.get(), IB_CUR_GE, &res); err == DB_SUCCESS; err = c->next(m_sys)) {
      if (c->read_row(m_sys, tpl.get()) != DB_SUCCESS) {
        break;
      }

      ++n;

      /* The match mode applies to moveto() only: next() doesn't check it. */
      if (mode != IB_CLOSEST_MATCH) {
        break;
      }
    }

    return n;
  };

  /* Closest: from the key to the end. */
  EXPECT_EQ(count("abc", "def", IB_CLOSEST_MATCH), 4);
  EXPECT_EQ(count("ghi", "a", IB_CLOSEST_MATCH), 2);

  /* Exact: the row with the key, nothing for a missing one. */
  EXPECT_EQ(count("abc", "zzz", IB_EXACT_MATCH), 1);
  EXPECT_EQ(count("abc", "zz", IB_EXACT_MATCH), 0);

  /* Prefix: the first column equal, the second one starting with the given string,
  on the row moveto() finds. */
  EXPECT_EQ(count("abc", "d", IB_EXACT_PREFIX), 1);
  EXPECT_EQ(count("abc", "zz", IB_EXACT_PREFIX), 1);
  EXPECT_EQ(count("mno", "p", IB_EXACT_PREFIX), 1);
  EXPECT_EQ(count("mno", "x", IB_EXACT_PREFIX), 0);
  EXPECT_EQ(count("xyz", "a", IB_EXACT_PREFIX), 0);

  commit(trx);
}

/** A cursor opened without a trx is used after attach_trx(). */
TEST_F(ApiDb, AttachTrx) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 4, [](uint32_t k) { return k; });

  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), nullptr, &crsr), DB_SUCCESS);

  for (int i = 0; i < 3; ++i) {
    auto trx = begin();
    int n{};

    crsr->attach_trx(m_sys, trx);
    ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IS), DB_SUCCESS);

    for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys)) {
      ++n;
    }

    EXPECT_EQ(n, 4);

    /* Detach: reset() lets another trx use the cursor. */
    ASSERT_EQ(crsr->reset(m_sys), DB_SUCCESS);
    commit(trx);
  }

  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
}

/* ---- DML ---- */

/** A duplicate primary key is refused; get_duplicate_key() names the index. */
TEST_F(ApiDb, DuplicatePrimaryKey) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(insert_kv(m_sys, trx, table(), 1, 1), DB_SUCCESS);
  EXPECT_EQ(insert_kv(m_sys, trx, table(), 1, 2), DB_DUPLICATE_KEY);

  const char *tname{}, *iname{};

  ASSERT_EQ(api::get_duplicate_key(trx, &tname, &iname), DB_SUCCESS);
  EXPECT_EQ(std::string(tname), table());
  EXPECT_STREQ(iname, "PRIMARY");

  /* The statement was rolled back, not the trx. */
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}}));
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 2, 2), DB_SUCCESS);
  commit(trx);

  trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()).size(), 2UL);
  commit(trx);
}

/** get_duplicate_key() without a duplicate key error. */
TEST_F(ApiDb, NoDuplicateKey) {
  auto trx = begin();
  const char *tname{}, *iname{};

  EXPECT_EQ(api::get_duplicate_key(trx, &tname, &iname), DB_ERROR);
  rollback(trx);
}

/** A unique secondary index refuses a duplicate value. */
TEST_F(ApiDb, DuplicateUniqueSecondary) {
  ASSERT_EQ(
    create_table(
      m_sys,
      table(),
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"v", IB_INT, IB_COL_UNSIGNED, 4}},
      {{"PRIMARY", {"k"}, true}, {"U", {"v"}, false, true}}
    ),
    DB_SUCCESS
  );

  auto trx = begin();

  ASSERT_EQ(insert_kv(m_sys, trx, table(), 1, 7), DB_SUCCESS);
  EXPECT_EQ(insert_kv(m_sys, trx, table(), 2, 7), DB_DUPLICATE_KEY);

  const char *tname{}, *iname{};

  ASSERT_EQ(api::get_duplicate_key(trx, &tname, &iname), DB_SUCCESS);
  EXPECT_STREQ(iname, "U");

  /* Moving the value to a free one and reusing the old one is fine. */
  ASSERT_EQ(update_kv(m_sys, trx, table(), 1, 8), DB_SUCCESS);
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 2, 7), DB_SUCCESS);
  EXPECT_EQ(update_kv(m_sys, trx, table(), 2, 8), DB_DUPLICATE_KEY);
  commit(trx);

  trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 8}, {2, 7}}));
  commit(trx);
}

/** Update: of a column, of the key (the row moves), onto an existing key. */
TEST_F(ApiDb, UpdateKey) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 1, 4, [](uint32_t k) { return k * 10; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table());

  ASSERT_EQ(crsr->set_lock_mode(m_sys, IB_LOCK_X), DB_SUCCESS);

  auto move_key = [&](uint32_t from, uint32_t to) {
    Tpl old_tpl(crsr->clust_read_tuple_create());
    Tpl new_tpl(crsr->clust_read_tuple_create());

    EXPECT_EQ(seek_kv(m_sys, crsr.m_crsr, from), DB_SUCCESS);
    EXPECT_EQ(crsr->read_row(m_sys, old_tpl.get()), DB_SUCCESS);
    EXPECT_EQ(new_tpl->copy(old_tpl.get()), DB_SUCCESS);
    EXPECT_EQ(new_tpl->write_u32(0, to), DB_SUCCESS);

    return crsr->update_row(m_sys, old_tpl.get(), new_tpl.get());
  };

  EXPECT_EQ(move_key(1, 100), DB_SUCCESS);
  EXPECT_EQ(move_key(2, 3), DB_DUPLICATE_KEY);
  crsr.close();

  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{2, 20}, {3, 30}, {100, 10}}));
  commit(trx);
}

/** An update that changes nothing succeeds and leaves the row alone. */
TEST_F(ApiDb, UpdateNoChange) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 1, 2, [](uint32_t) { return 5; });

  auto trx = begin();

  EXPECT_EQ(update_kv(m_sys, trx, table(), 1, 5), DB_SUCCESS);
  EXPECT_EQ(update_kv(m_sys, trx, table(), 9, 5), DB_RECORD_NOT_FOUND);
  commit(trx);

  trx = begin();
  EXPECT_EQ(select_kv(m_sys, trx, table(), 1), 5U);
  commit(trx);
}

/** Delete: positioned only; the row is gone for later reads in the trx. */
TEST_F(ApiDb, Delete) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 5, [](uint32_t k) { return k; });

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table());

    ASSERT_EQ(crsr->set_lock_mode(m_sys, IB_LOCK_X), DB_SUCCESS);

    /* Not positioned yet. */
    EXPECT_EQ(crsr->delete_row(m_sys), DB_RECORD_NOT_FOUND);

    ASSERT_EQ(seek_kv(m_sys, crsr.m_crsr, 2), DB_SUCCESS);
    ASSERT_EQ(crsr->delete_row(m_sys), DB_SUCCESS);
  }

  EXPECT_EQ(scan_kv(m_sys, trx, table()).size(), 4UL);
  EXPECT_EQ(select_kv(m_sys, trx, table(), 2), std::nullopt);

  /* The key can be inserted again. */
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 2, 22), DB_SUCCESS);
  commit(trx);

  trx = begin();
  EXPECT_EQ(select_kv(m_sys, trx, table(), 2), 22U);
  commit(trx);
}

/** The row a cursor is on is deleted: deleting or reading it again finds nothing.
Regression: api/api0api.cc:2622 (Cursor::delete_row) and :2664 (read_row) use the row
cache when it isn't empty: moveto() left a copy of the row there, taken before
the delete, without the delete mark. The second delete_row() delete-marks the
row again and asserts in BtreeCursor::del_mark_set_clust_rec (btr0cur.cc:1584,
!rec.get_deleted_flag()); read_row() returns the deleted row. The same code is
upstream (7c21eac0 ib_cursor_delete_row). */
TEST_F(ApiDb, DeleteTwice) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 5, [](uint32_t k) { return k; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table());

  ASSERT_EQ(crsr->set_lock_mode(m_sys, IB_LOCK_X), DB_SUCCESS);
  ASSERT_EQ(seek_kv(m_sys, crsr.m_crsr, 2), DB_SUCCESS);
  ASSERT_EQ(crsr->delete_row(m_sys), DB_SUCCESS);

  Tpl tpl(crsr->clust_read_tuple_create());

  EXPECT_EQ(crsr->read_row(m_sys, tpl.get()), DB_RECORD_NOT_FOUND);
  EXPECT_EQ(crsr->delete_row(m_sys), DB_RECORD_NOT_FOUND);

  crsr.close();
  rollback(trx);
}

/** Update and delete through a secondary index need access to the clustered index. */
TEST_F(ApiDb, SecondaryDml) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return 100 - k; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table());
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);
  ASSERT_EQ(sec->set_lock_mode(m_sys, IB_LOCK_X), DB_SUCCESS);

  /* Find v = 95, i.e. k = 5. */
  auto find = [&](uint32_t v) {
    Tpl key(sec->sec_search_tuple_create());
    int res{};

    EXPECT_EQ(key->write_u32(0, v), DB_SUCCESS);
    EXPECT_EQ(sec->moveto(m_sys, key.get(), IB_CUR_GE, &res), DB_SUCCESS);
    EXPECT_EQ(res, 0);
  };

  find(95);

  {
    Tpl old_tpl(sec->clust_read_tuple_create());
    Tpl new_tpl(sec->clust_read_tuple_create());

    EXPECT_EQ(sec->delete_row(m_sys), DB_ERROR);
    EXPECT_EQ(sec->update_row(m_sys, old_tpl.get(), new_tpl.get()), DB_ERROR);
  }

  sec->set_cluster_access();
  find(95);

  {
    Tpl old_tpl(sec->clust_read_tuple_create());
    Tpl new_tpl(sec->clust_read_tuple_create());
    uint32_t k{};

    ASSERT_EQ(sec->read_row(m_sys, old_tpl.get()), DB_SUCCESS);
    ASSERT_EQ(old_tpl->read_u32(0, &k), DB_SUCCESS);
    EXPECT_EQ(k, 5U);

    ASSERT_EQ(new_tpl->copy(old_tpl.get()), DB_SUCCESS);
    ASSERT_EQ(new_tpl->write_u32(1, 1000), DB_SUCCESS);
    ASSERT_EQ(sec->update_row(m_sys, old_tpl.get(), new_tpl.get()), DB_SUCCESS);
  }

  find(96);
  ASSERT_EQ(sec->delete_row(m_sys), DB_SUCCESS);

  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);
  crsr.close();

  const auto rows = scan_kv(m_sys, trx, table());

  EXPECT_EQ(rows.size(), 9UL);
  EXPECT_EQ(select_kv(m_sys, trx, table(), 5), 1000U);
  EXPECT_EQ(select_kv(m_sys, trx, table(), 4), std::nullopt);
  commit(trx);
}

/** A secondary index search tuple maps to a clustered key with get_cluster_key(). */
TEST_F(ApiDb, ClusterKey) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k + 50; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);

  {
    Tpl key(sec->sec_search_tuple_create());
    Tpl rec(sec->sec_read_tuple_create());
    int res{};

    ASSERT_EQ(key->write_u32(0, 57), DB_SUCCESS);
    ASSERT_EQ(sec->moveto(m_sys, key.get(), IB_CUR_GE, &res), DB_SUCCESS);
    ASSERT_EQ(sec->read_row(m_sys, rec.get()), DB_SUCCESS);

    /* A secondary index "row" tuple has the columns in table order: k, v. */
    uint32_t v{}, k{};

    ASSERT_EQ(rec->read_u32(0, &k), DB_SUCCESS);
    ASSERT_EQ(rec->read_u32(1, &v), DB_SUCCESS);
    EXPECT_EQ(v, 57U);
    EXPECT_EQ(k, 7U);

    /* A row tuple is refused, a key tuple of the clustered index too. */
    api::Tuple *ck{};

    EXPECT_EQ(sec->get_cluster_key(&ck, rec.get()), DB_ERROR);

    Tpl clust_key(crsr->clust_search_tuple_create());

    EXPECT_EQ(crsr->get_cluster_key(&ck, clust_key.get()), DB_ERROR);

    /* A secondary key tuple (v, k) gives the clustered key (k). */
    Tpl sec_key(sec->sec_search_tuple_create());

    ASSERT_EQ(sec_key->get_n_cols(), 2UL);
    ASSERT_EQ(sec_key->write_u32(0, 57), DB_SUCCESS);
    ASSERT_EQ(sec_key->write_u32(1, 7), DB_SUCCESS);
    ASSERT_EQ(sec->get_cluster_key(&ck, sec_key.get()), DB_SUCCESS);

    Tpl ck_tpl(ck);
    uint32_t ck_k{};

    EXPECT_EQ(ck_tpl->get_n_cols(), 1UL);
    ASSERT_EQ(ck_tpl->read_u32(0, &ck_k), DB_SUCCESS);
    EXPECT_EQ(ck_k, 7U);

    /* It finds the row in the clustered index. */
    Tpl row(crsr->clust_read_tuple_create());

    ASSERT_EQ(crsr->moveto(m_sys, ck_tpl.get(), IB_CUR_GE, &res), DB_SUCCESS);
    EXPECT_EQ(res, 0);
    ASSERT_EQ(crsr->read_row(m_sys, row.get()), DB_SUCCESS);
    ASSERT_EQ(row->read_u32(1, &v), DB_SUCCESS);
    EXPECT_EQ(v, 57U);
  }

  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);
  crsr.close();
  commit(trx);
}

/** open_index() of a missing index. */
TEST_F(ApiDb, OpenIndexMissing) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::Cursor *sec{reinterpret_cast<api::Cursor *>(1)};

  EXPECT_EQ(crsr->open_index(m_sys, "NONE", &sec), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(sec, nullptr);

  /* The clustered index can be opened by name. */
  ASSERT_EQ(crsr->open_index(m_sys, "PRIMARY", &sec), DB_SUCCESS);
  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);

  crsr.close();
  commit(trx);

  /* The table is not left with an open handle: it can be dropped. */
  ASSERT_EQ(drop_table(m_sys, table()), DB_SUCCESS);
}

/* ---- Transactions ---- */

/** The states of a trx; a started trx can't be started again. */
TEST_F(ApiDb, TrxStates) {
  auto trx = begin();

  EXPECT_EQ(api::trx_state(trx), IB_TRX_ACTIVE);
  EXPECT_EQ(api::trx_start(m_sys, trx, IB_TRX_REPEATABLE_READ), DB_ERROR);

  int data{};

  api::trx_set_client_data(trx, &data);
  commit(trx);
}

/** Rollback undoes inserts, updates and deletes; commit keeps them. */
TEST_F(ApiDb, CommitRollback) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 3, [](uint32_t k) { return k; });

  const std::vector<std::pair<uint32_t, uint32_t>> before{{0, 0}, {1, 1}, {2, 2}};

  auto trx = begin();

  ASSERT_EQ(insert_kv(m_sys, trx, table(), 10, 10), DB_SUCCESS);
  ASSERT_EQ(update_kv(m_sys, trx, table(), 1, 100), DB_SUCCESS);
  ASSERT_EQ(delete_kv(m_sys, trx, table(), 2), DB_SUCCESS);
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{0, 0}, {1, 100}, {10, 10}}));
  rollback(trx);

  trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), before);

  ASSERT_EQ(insert_kv(m_sys, trx, table(), 10, 10), DB_SUCCESS);
  ASSERT_EQ(update_kv(m_sys, trx, table(), 1, 100), DB_SUCCESS);
  ASSERT_EQ(delete_kv(m_sys, trx, table(), 2), DB_SUCCESS);
  commit(trx);

  trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{0, 0}, {1, 100}, {10, 10}}));
  commit(trx);
}

/** A trx that was never started can be released. */
TEST_F(ApiDb, TrxRelease) {
  auto trx = begin();

  commit(trx);

  /* trx_start() on a fresh handle works, it is then committed. */
  trx = m_sys.m_trx_sys->create_user_trx(nullptr);

  EXPECT_EQ(api::trx_state(trx), IB_TRX_NOT_STARTED);
  EXPECT_EQ(api::trx_start(m_sys, trx, IB_TRX_READ_COMMITTED), DB_SUCCESS);
  EXPECT_EQ(api::trx_state(trx), IB_TRX_ACTIVE);
  commit(trx);

  trx = m_sys.m_trx_sys->create_user_trx(nullptr);
  EXPECT_EQ(api::trx_release(m_sys, trx), DB_SUCCESS);
}

/** READ UNCOMMITTED sees the changes of an active trx, the other levels don't. */
TEST_F(ApiDb, IsolationUncommitted) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 1, [](uint32_t) { return 1; });

  auto writer = begin();

  ASSERT_EQ(insert_kv(m_sys, writer, table(), 5, 5), DB_SUCCESS);
  ASSERT_EQ(update_kv(m_sys, writer, table(), 0, 2), DB_SUCCESS);

  for (auto level : {IB_TRX_READ_UNCOMMITTED, IB_TRX_READ_COMMITTED, IB_TRX_REPEATABLE_READ, IB_TRX_SERIALIZABLE}) {
    auto reader = begin(level);
    const auto rows = scan_kv(m_sys, reader, table());

    if (level == IB_TRX_READ_UNCOMMITTED) {
      EXPECT_EQ(rows, (std::vector<std::pair<uint32_t, uint32_t>>{{0, 2}, {5, 5}}));
    } else {
      EXPECT_EQ(rows, (std::vector<std::pair<uint32_t, uint32_t>>{{0, 1}})) << level;
    }

    commit(reader);
  }

  rollback(writer);
}

/** REPEATABLE READ: a snapshot, taken at the first read, that commits of others don't change. */
TEST_F(ApiDb, IsolationRepeatableRead) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 2, [](uint32_t k) { return k; });

  auto reader = begin(IB_TRX_REPEATABLE_READ);
  const auto first = scan_kv(m_sys, reader, table());

  auto writer = begin();

  ASSERT_EQ(insert_kv(m_sys, writer, table(), 7, 7), DB_SUCCESS);
  ASSERT_EQ(update_kv(m_sys, writer, table(), 0, 9), DB_SUCCESS);
  ASSERT_EQ(delete_kv(m_sys, writer, table(), 1), DB_SUCCESS);
  commit(writer);

  EXPECT_EQ(scan_kv(m_sys, reader, table()), first);
  commit(reader);

  /* A new trx sees the commit. */
  reader = begin(IB_TRX_REPEATABLE_READ);
  EXPECT_EQ(scan_kv(m_sys, reader, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{0, 9}, {7, 7}}));
  commit(reader);
}

/** A row lock held by another trx: the waiter times out after the lock wait timeout. */
TEST_F(ApiDb, LockWaitTimeout) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 2, [](uint32_t k) { return k; });

  auto holder = begin();

  ASSERT_EQ(update_kv(m_sys, holder, table(), 1, 10), DB_SUCCESS);

  auto waiter = begin();
  const auto start = std::chrono::steady_clock::now();

  EXPECT_EQ(update_kv(m_sys, waiter, table(), 1, 20), DB_LOCK_WAIT_TIMEOUT);

  const auto waited = std::chrono::steady_clock::now() - start;

  /* 1 s timeout, the lock timeout thread looks once a second. */
  EXPECT_GE(waited, 900ms);
  EXPECT_LT(waited, 10s);

  rollback(waiter);

  /* Other rows were not locked. */
  waiter = begin();
  EXPECT_EQ(update_kv(m_sys, waiter, table(), 0, 30), DB_SUCCESS);
  commit(waiter);

  commit(holder);

  auto trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{0, 30}, {1, 10}}));
  commit(trx);
}

/** Set the lock wait timeout. The 1 s timeout of the suite, now exact (the
waiter times out by itself at its deadline, no lock timeout thread rounding
it up to 2 or 3 s), is too short for a commit or a rollback that ends a wait
on a loaded host (the sanitizer suites): the tests that expect a grant use
10 s, so that a lost grant still fails them.
@return the old timeout. */
static std::chrono::seconds set_wait_timeout(System &sys, std::chrono::seconds t) {
  mutex_enter(&sys.m_trx_sys->m_mutex);
  const auto old = sys.m_config.m_lock.m_wait_timeout;
  sys.m_config.m_lock.m_wait_timeout = t;
  mutex_exit(&sys.m_trx_sys->m_mutex);
  return old;
}

/** A lock wait ends when the holder commits: the waiter goes on. */
TEST_F(ApiDb, LockWaitGranted) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 1, [](uint32_t) { return 0; });

  const auto old_timeout = set_wait_timeout(m_sys, 10s);

  auto holder = begin();

  ASSERT_EQ(update_kv(m_sys, holder, table(), 0, 1), DB_SUCCESS);

  auto waiter = begin();
  auto result = std::async(std::launch::async, [&] { return update_kv(m_sys, waiter, table(), 0, 2); });

  std::this_thread::sleep_for(200ms);
  commit(holder);

  const auto err = result.get();

  (void)set_wait_timeout(m_sys, old_timeout);

  /* The commit came before the timeout. */
  EXPECT_EQ(err, DB_SUCCESS);

  if (err == DB_SUCCESS) {
    commit(waiter);
  } else {
    rollback(waiter);
  }

  auto trx = begin();
  EXPECT_EQ(select_kv(m_sys, trx, table(), 0), err == DB_SUCCESS ? 2U : 1U);
  commit(trx);
}

/** Two trxs that wait for each other: one of them is chosen as the victim and rolled back. */
TEST_F(ApiDb, Deadlock) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 2, [](uint32_t) { return 0; });

  /* The winner waits for the rollback of the victim. */
  const auto old_timeout = set_wait_timeout(m_sys, 10s);

  auto t1 = begin();
  auto t2 = begin();

  ASSERT_EQ(update_kv(m_sys, t1, table(), 0, 1), DB_SUCCESS);
  ASSERT_EQ(update_kv(m_sys, t2, table(), 1, 2), DB_SUCCESS);

  auto r1 = std::async(std::launch::async, [&] { return update_kv(m_sys, t1, table(), 1, 1); });

  std::this_thread::sleep_for(100ms);

  const auto e2 = update_kv(m_sys, t2, table(), 0, 2);
  const auto e1 = r1.get();

  (void)set_wait_timeout(m_sys, old_timeout);

  /* One got the deadlock, the other the row: no timeouts. */
  EXPECT_TRUE((e1 == DB_DEADLOCK) != (e2 == DB_DEADLOCK)) << e1 << " " << e2;
  EXPECT_TRUE(e1 == DB_SUCCESS || e2 == DB_SUCCESS) << e1 << " " << e2;

  /* The victim was rolled back. */
  auto victim = e1 == DB_DEADLOCK ? t1 : t2;
  auto winner = e1 == DB_DEADLOCK ? t2 : t1;
  const uint32_t v = winner == t1 ? 1 : 2;

  EXPECT_EQ(api::trx_state(victim), IB_TRX_NOT_STARTED);
  rollback(victim);
  commit(winner);

  auto trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{0, v}, {1, v}}));
  commit(trx);
}

/** Table locks: X excludes the intention locks of others, IS and IX don't exclude each other. */
TEST_F(ApiDb, TableLocks) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  uint64_t id{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &id), DB_SUCCESS);

  auto t1 = begin();
  auto t2 = begin();

  EXPECT_EQ(api::table_lock(m_sys, t1, id, IB_LOCK_IS), DB_SUCCESS);
  EXPECT_EQ(api::table_lock(m_sys, t2, id, IB_LOCK_IX), DB_SUCCESS);
  EXPECT_EQ(api::table_lock(m_sys, t1, 0xFFFFFFF0, IB_LOCK_IS), DB_TABLE_NOT_FOUND);
  commit(t1);
  commit(t2);

  t1 = begin();
  t2 = begin();

  {
    Crsr c1(m_sys, t1, table(), IB_LOCK_X);

    ASSERT_EQ(c1.m_err, DB_SUCCESS);

    Crsr c2(m_sys, t2, table(), IB_LOCK_IS);

    EXPECT_EQ(c2.m_err, DB_LOCK_WAIT_TIMEOUT);
  }

  commit(t1);
  rollback(t2);

  /* S and S are compatible, S and IX are not. */
  t1 = begin();
  t2 = begin();

  {
    Crsr c1(m_sys, t1, table(), IB_LOCK_S);
    Crsr c2(m_sys, t2, table(), IB_LOCK_S);

    EXPECT_EQ(c1.m_err, DB_SUCCESS);
    EXPECT_EQ(c2.m_err, DB_SUCCESS);
  }

  commit(t2);

  {
    auto t3 = begin();

    EXPECT_EQ(insert_kv(m_sys, t3, table(), 1, 1), DB_LOCK_WAIT_TIMEOUT);
    rollback(t3);
  }

  commit(t1);
}

/** A savepoint can be released once; an unknown one is not found. */
TEST_F(ApiDb, SavepointRelease) {
  auto trx = begin();

  api::savepoint_take(trx, "a", 1);
  api::savepoint_take(trx, "b", 1);

  EXPECT_EQ(api::savepoint_release(trx, "a", 1), DB_SUCCESS);
  EXPECT_EQ(api::savepoint_release(trx, "a", 1), DB_NO_SAVEPOINT);
  EXPECT_EQ(api::savepoint_release(trx, "c", 1), DB_NO_SAVEPOINT);

  /* Names are compared with their length. */
  EXPECT_EQ(api::savepoint_release(trx, "bb", 1), DB_SUCCESS);
  rollback(trx);
}

/** Roll back to a named savepoint: later changes are undone, earlier ones kept,
the savepoint stays, later savepoints are gone; an unknown name isn't found. */
TEST_F(ApiDb, SavepointRollback) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(insert_kv(m_sys, trx, table(), 1, 1), DB_SUCCESS);
  api::savepoint_take(trx, "a", 1);
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 2, 2), DB_SUCCESS);
  api::savepoint_take(trx, "b", 1);
  ASSERT_EQ(insert_kv(m_sys, trx, table(), 3, 3), DB_SUCCESS);

  ASSERT_EQ(api::savepoint_rollback(m_sys, trx, "b", 1), DB_SUCCESS);
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}, {2, 2}}));

  ASSERT_EQ(api::savepoint_rollback(m_sys, trx, "a", 1), DB_SUCCESS);
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}}));

  /* "b" was after "a": gone. "a" is still there. */
  EXPECT_EQ(api::savepoint_rollback(m_sys, trx, "b", 1), DB_NO_SAVEPOINT);
  EXPECT_EQ(api::savepoint_release(trx, "a", 1), DB_SUCCESS);
  EXPECT_EQ(api::savepoint_rollback(m_sys, trx, "x", 1), DB_NO_SAVEPOINT);

  commit(trx);

  trx = begin();
  EXPECT_EQ(scan_kv(m_sys, trx, table()), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}}));
  commit(trx);
}

/** A savepoint rollback of a trx that was not started. */
TEST_F(ApiDb, SavepointRollbackNotStarted) {
  auto trx = m_sys.m_trx_sys->create_user_trx(nullptr);

  EXPECT_EQ(api::savepoint_rollback(m_sys, trx, "a", 1), DB_ERROR);
  EXPECT_EQ(api::trx_release(m_sys, trx), DB_SUCCESS);
}

/* ---- Indexes ---- */

/** The checks of an index schema made for an existing table. */
TEST_F(ApiDb, CreateIndexErrors) {
  ASSERT_EQ(create_kv(m_sys, table(), false), DB_SUCCESS);

  auto trx = begin();
  api::IndexSchema *idx{};

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::IndexSchema::create(m_sys, trx, "I", table("x").c_str(), &idx), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::IndexSchema::create(m_sys, trx, "PRIMARY", table().c_str(), &idx), DB_DUPLICATE_KEY);
  EXPECT_EQ(api::IndexSchema::create(m_sys, trx, "GEN_CLUST_INDEX", table().c_str(), &idx), DB_INVALID_INPUT);
  EXPECT_EQ(api::IndexSchema::create(m_sys, trx, "\377I", table().c_str(), &idx), DB_INVALID_INPUT);
  ASSERT_EQ(api::IndexSchema::create(m_sys, trx, "I", table().c_str(), &idx), DB_SUCCESS);
  EXPECT_EQ(idx->add_col("none", 0), DB_NOT_FOUND);
  EXPECT_EQ(idx->add_col("v", 4), DB_SCHEMA_ERROR);
  EXPECT_EQ(idx->add_col("v", 0), DB_SUCCESS);
  EXPECT_EQ(idx->add_col("v", 0), DB_COL_APPEARS_TWICE_IN_INDEX);
  api::IndexSchema::destroy(idx);
  rollback(trx);
}

namespace {

/** CREATE INDEX I ON table (v), in a trx of its own. */
DbErr create_index_v(System &sys, const std::string &table, uint64_t *index_id, bool unique = false) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  api::IndexSchema *idx{};

  EXPECT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);

  auto err = api::IndexSchema::create(sys, trx, "I", table.c_str(), &idx);

  if (err == DB_SUCCESS) {
    EXPECT_EQ(idx->add_col("v", 0), DB_SUCCESS);

    if (unique) {
      EXPECT_EQ(idx->set_unique(), DB_SUCCESS);
    }

    err = idx->create_index(sys, index_id);
    api::IndexSchema::destroy(idx);
  }

  if (err == DB_SUCCESS) {
    EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
  } else {
    EXPECT_EQ(api::trx_rollback(sys, trx), DB_SUCCESS);
  }

  return err;
}

/** @return the v values read through index I, by name or by id. */
std::vector<uint32_t> scan_index_v(System &sys, const std::string &table, uint64_t index_id, bool by_id) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  std::vector<uint32_t> vs;

  {
    Crsr crsr(sys, trx, table, IB_LOCK_IS);
    api::Cursor *sec{};

    if (by_id) {
      EXPECT_EQ(api::cursor_open_index_using_id(sys, index_id, trx, &sec), DB_SUCCESS);
    } else {
      EXPECT_EQ(crsr->open_index(sys, "I", &sec), DB_SUCCESS);
    }

    if (sec != nullptr) {
      Tpl tpl(sec->sec_read_tuple_create());

      for (auto err = sec->first(sys); err == DB_SUCCESS; err = sec->next(sys)) {
        uint32_t v{};

        EXPECT_EQ(sec->read_row(sys, tpl.get()), DB_SUCCESS);
        EXPECT_EQ(tpl->read_u32(1, &v), DB_SUCCESS);
        vs.push_back(v);
      }

      EXPECT_EQ(api::Cursor::close(sys, sec), DB_SUCCESS);
    }
  }

  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  return vs;
}

}  // namespace

/** A secondary index created on an empty table indexes the rows inserted later;
it is found by name and by id; its id has the table id in the high 32 bits. */
TEST_F(ApiDb, CreateIndexEmpty) {
  ASSERT_EQ(create_kv(m_sys, table(), false), DB_SUCCESS);

  uint64_t index_id{}, table_id{};

  ASSERT_EQ(create_index_v(m_sys, table(), &index_id), DB_SUCCESS);
  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &table_id), DB_SUCCESS);
  EXPECT_EQ(index_id >> 32, table_id);

  load_kv(m_sys, table(), 0, 300, [](uint32_t k) { return (k * 7919) % 1000; });

  for (bool by_id : {false, true}) {
    const auto vs = scan_index_v(m_sys, table(), index_id, by_id);

    EXPECT_EQ(vs.size(), 300UL) << by_id;
    EXPECT_TRUE(std::ranges::is_sorted(vs)) << by_id;
  }
}

/** A secondary index created on a table with rows indexes them all.
Regression: row/row0merge.cc:769-773 (row_merge_write_rec) computes the size of a merge
record from offsets.extra_size() BEFORE it is normalized (++extra_size): the
record is written 1 byte short of the length the reader skips, each record
overwrites the last byte of the one before. Upstream (7c21eac0 row0merge.c:895)
normalizes first: extra_size = rec_offs_extra_size(offsets) + 1. With 3 rows the
index ends up with 1 entry made of pieces of two rows. */
TEST_F(ApiDb, CreateIndexOnRows) {
  ASSERT_EQ(create_kv(m_sys, table(), false), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 300, [](uint32_t k) { return (k * 7919) % 1000; });

  uint64_t index_id{};

  ASSERT_EQ(create_index_v(m_sys, table(), &index_id), DB_SUCCESS);

  const auto vs = scan_index_v(m_sys, table(), index_id, false);

  EXPECT_EQ(vs.size(), 300UL);
  EXPECT_TRUE(std::ranges::is_sorted(vs));
}

/** Drop a secondary index: it can't be opened any more, the table can.
Regression: ddl/ddl0ddl.cc:812-823, the DROP_INDEX_PROC of Ddl::drop_index() has a
"COMMIT WORK;" before its DELETE statements: the trx is committed (NOT_STARTED)
and the DELETE's SELECT asserts ut_a(trx->m_conc_state != TRX_NOT_STARTED) in
RowSel::step (row0sel.cc:1444). The same SQL and assertion are upstream (7c21eac0):
ib_index_drop() could never work in a debug build. Then see DropIndexMissing. */
TEST_F(ApiDb, DropIndex) {
  ASSERT_EQ(create_kv(m_sys, table(), false), DB_SUCCESS);

  uint64_t index_id{};

  ASSERT_EQ(create_index_v(m_sys, table(), &index_id), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k; });

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::index_drop(m_sys, trx, index_id), DB_SUCCESS);
  commit(trx);

  trx = begin();

  {
    Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
    api::Cursor *sec{};

    EXPECT_EQ(crsr->open_index(m_sys, "I", &sec), DB_TABLE_NOT_FOUND);
  }

  EXPECT_EQ(scan_kv(m_sys, trx, table()).size(), 10UL);
  commit(trx);
}

/** A unique index can't be created on duplicate values.
BUG (besides CreateIndexOnRows): api/api0api.cc:1761 (create_secondary_index) overwrites the error of
build_secondary_index() with the one of schema_lock_exclusive() (always
DB_SUCCESS), so a failed build is reported as a success: DB_SUCCESS is returned
and the bad index stays. */
TEST_F(ApiDb, CreateUniqueIndexOnDuplicates) {
  ASSERT_EQ(create_kv(m_sys, table(), false), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k % 3; });

  uint64_t index_id{};

  EXPECT_EQ(create_index_v(m_sys, table(), &index_id, true), DB_DUPLICATE_KEY);

  auto trx = begin();

  {
    Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
    api::Cursor *sec{};

    EXPECT_EQ(crsr->open_index(m_sys, "I", &sec), DB_TABLE_NOT_FOUND);
  }

  commit(trx);
}

/** Dropping an index that doesn't exist.
Regression: api/api0api.cc:1873 (index_drop) calls table_decrement_handle_count(table,
false) although the caller holds the dictionary mutex (exclusive schema lock,
the table was opened with open_table_by_id(.., true)): mutex_enter() of a mutex
the thread owns asserts (debug) or self-deadlocks. Must be true. Also upstream
(7c21eac0 api0api.c:2629). Every index_drop() of an existing table hits it. */
TEST_F(ApiDb, DropIndexMissing) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  uint64_t table_id{};

  ASSERT_EQ(api::table_get_id(m_sys, table().c_str(), &table_id), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::index_drop(m_sys, trx, table_id << 32 | 0xFFFFFF), DB_TABLE_NOT_FOUND);
  EXPECT_EQ(api::index_drop(m_sys, trx, 0xFFFFFFF0ULL << 32 | 1), DB_TABLE_NOT_FOUND);
  rollback(trx);
}

/* ---- Statistics ---- */

/** The statistics of an empty table and of a table with rows. */
TEST_F(ApiDb, Statistics) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::TableStats stats{};

  ASSERT_EQ(crsr->update_table_statistics(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->get_table_statistics(m_sys, &stats), DB_SUCCESS);
  EXPECT_EQ(stats.m_stat_n_rows, 0);

  /* An index has at least its root page. */
  EXPECT_EQ(stats.m_stat_clustered_index_size, UNIV_PAGE_SIZE);
  EXPECT_EQ(stats.m_stat_sum_of_other_index_sizes, UNIV_PAGE_SIZE);

  crsr.close();
  commit(trx);

  load_kv(m_sys, table(), 0, 5000, [](uint32_t k) { return k % 10; });

  trx = begin();

  Crsr crsr2(m_sys, trx, table(), IB_LOCK_IS);

  ASSERT_EQ(crsr2->update_table_statistics(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr2->get_table_statistics(m_sys, &stats), DB_SUCCESS);

  /* Estimates from a sample of the pages. */
  EXPECT_GT(stats.m_stat_n_rows, 1000);
  EXPECT_LT(stats.m_stat_n_rows, 25000);
  EXPECT_GT(stats.m_stat_clustered_index_size, UNIV_PAGE_SIZE);
  EXPECT_GT(stats.m_stat_sum_of_other_index_sizes, UNIV_PAGE_SIZE);
  EXPECT_EQ(stats.m_stat_modified_counter, 0UL);

  uint64_t ncols{};
  int64_t *n_diff{};

  EXPECT_EQ(crsr2->get_index_stat_n_diff_key_vals(m_sys, "NONE", &ncols, &n_diff), DB_NOT_FOUND);

  ASSERT_EQ(crsr2->get_index_stat_n_diff_key_vals(m_sys, "V", &ncols, &n_diff), DB_SUCCESS);

  /* V is (v, k): unique on both. */
  EXPECT_EQ(ncols, 2UL);
  free(n_diff);

  crsr2.close();
  commit(trx);
}

/** The DML counts the modifications until the next statistics update. */
TEST_F(ApiDb, StatisticsModifiedCounter) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10, [](uint32_t k) { return k; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::TableStats stats{};

  ASSERT_EQ(crsr->get_table_statistics(m_sys, &stats), DB_SUCCESS);
  EXPECT_GE(stats.m_stat_modified_counter, 10UL);

  ASSERT_EQ(crsr->update_table_statistics(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->get_table_statistics(m_sys, &stats), DB_SUCCESS);
  EXPECT_EQ(stats.m_stat_modified_counter, 0UL);

  crsr.close();
  commit(trx);
}

/** The number of distinct values of the key prefixes of an index: the last one is
the number of distinct keys.
Regression: api/api0api.cc:4197 copies m_n_diff_key_vals[0 .. n_uniq-1], but the array
is indexed 1 .. n_uniq (btr0cur.cc:2111, dict0fk.cc:1317 read [n_unique]): the
first value returned is the meaningless [0] and the full key count [n_uniq] is
missing (also upstream, 7c21eac0 api0api.c:5999). */
TEST_F(ApiDb, StatisticsDistinctKeys) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 2000, [](uint32_t k) { return k % 10; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  uint64_t ncols{};
  int64_t *n_diff{};

  ASSERT_EQ(crsr->update_table_statistics(m_sys), DB_SUCCESS);

  /* PRIMARY: k, all distinct. */
  ASSERT_EQ(crsr->get_index_stat_n_diff_key_vals(m_sys, "PRIMARY", &ncols, &n_diff), DB_SUCCESS);
  ASSERT_EQ(ncols, 1UL);
  EXPECT_GT(n_diff[0], 500);
  free(n_diff);

  /* V: (v, k), 10 values of v, all distinct with k. */
  ASSERT_EQ(crsr->get_index_stat_n_diff_key_vals(m_sys, "V", &ncols, &n_diff), DB_SUCCESS);
  ASSERT_EQ(ncols, 2UL);
  EXPECT_LT(n_diff[0], 100);
  EXPECT_GT(n_diff[1], 500);
  free(n_diff);

  crsr.close();
  commit(trx);
}

/** A parallel count(*) counts every row, of several indexes. */
TEST_F(ApiDb, ParallelCount) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 3000, [](uint32_t k) { return k / 2; });

  auto trx = begin();
  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);

  for (size_t n_threads : {2, 4, 8}) {
    for (auto c : {crsr.m_crsr, sec}) {
      std::vector<api::Cursor *> crsrs{c};
      uint64_t n_rows{};

      ASSERT_EQ(api::parallel_select_count_star(m_sys, trx, crsrs, n_threads, n_rows), DB_SUCCESS);
      EXPECT_EQ(n_rows, 3000UL) << n_threads;
    }

    /* Both indexes in one run. */
    std::vector<api::Cursor *> both{crsr.m_crsr, sec};
    uint64_t n_rows{};

    ASSERT_EQ(api::parallel_select_count_star(m_sys, trx, both, n_threads, n_rows), DB_SUCCESS);
    EXPECT_EQ(n_rows, 6000UL) << n_threads;
  }

  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);
  crsr.close();
  commit(trx);
}

/** A parallel scan of a secondary index sees the snapshot of its read view:
the entries of rows changed or deleted after it are decided by the version
of the row in the clustered index (the secondary index scan used to stop on
ut_error). */
TEST_F(ApiDb, ParallelCountSecSnapshot) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 3000, [](uint32_t k) { return k % 50; });

  /* The reader's snapshot is taken before the changes. */
  auto reader = begin();
  Crsr crsr(m_sys, reader, table(), IB_LOCK_IS);
  api::Cursor *sec{};

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);

  auto count = [&](Trx *trx, api::Cursor *c) {
    std::vector<api::Cursor *> crsrs{c};
    uint64_t n_rows{};

    EXPECT_EQ(api::parallel_select_count_star(m_sys, trx, crsrs, 4, n_rows), DB_SUCCESS);
    return n_rows;
  };

  ASSERT_EQ(count(reader, sec), 3000UL);

  /* Move the secondary key of 500 rows, delete 300, insert 200. */
  auto writer = begin();

  for (uint32_t k = 0; k < 500; ++k) {
    ASSERT_EQ(update_kv(m_sys, writer, table(), k, 1000 + k), DB_SUCCESS);
  }

  for (uint32_t k = 1000; k < 1300; ++k) {
    ASSERT_EQ(delete_kv(m_sys, writer, table(), k), DB_SUCCESS);
  }

  for (uint32_t k = 5000; k < 5200; ++k) {
    ASSERT_EQ(insert_kv(m_sys, writer, table(), k, k % 7), DB_SUCCESS);
  }

  commit(writer);

  /* The reader still counts its snapshot, through both indexes. */
  EXPECT_EQ(count(reader, sec), 3000UL);
  EXPECT_EQ(count(reader, crsr.m_crsr), 3000UL);

  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);
  crsr.close();
  commit(reader);

  /* A new snapshot counts the changes. */
  auto now = begin();
  Crsr crsr2(m_sys, now, table(), IB_LOCK_IS);
  api::Cursor *sec2{};

  ASSERT_EQ(crsr2->open_index(m_sys, "V", &sec2), DB_SUCCESS);
  EXPECT_EQ(count(now, sec2), 2900UL);
  EXPECT_EQ(count(now, crsr2.m_crsr), 2900UL);

  EXPECT_EQ(api::Cursor::close(m_sys, sec2), DB_SUCCESS);
  crsr2.close();
  commit(now);
}

/** check_table() finds nothing wrong in a good table, with deleted rows too.
Regression: api/api0api.cc:4433 (check_table) asserts ut_a(prebuilt->m_trx->m_conc_state
== TRX_NOT_STARTED), but a cursor can only be opened with a started trx
(create_cursor() -> Trx::assign_read_view() asserts TRX_ACTIVE) or none (then
prebuilt->m_trx is nullptr and dereferenced): check_table() can't be called on any
cursor. It also shadows its trx parameter with the cursor's trx (line 4453). */
TEST_F(ApiDb, CheckTable) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 2000, [](uint32_t k) { return k % 100; });

  auto trx = begin();

  for (uint32_t k = 0; k < 2000; k += 7) {
    ASSERT_EQ(delete_kv(m_sys, trx, table(), k), DB_SUCCESS);
  }

  commit(trx);

  trx = begin();

  Crsr crsr(m_sys, trx, table(), IB_LOCK_IS);
  api::Cursor *sec{};

  EXPECT_EQ(api::check_table(m_sys, trx, crsr.m_crsr, 2), DB_SUCCESS);

  ASSERT_EQ(crsr->open_index(m_sys, "V", &sec), DB_SUCCESS);
  EXPECT_EQ(api::check_table(m_sys, trx, sec, 4), DB_SUCCESS);
  EXPECT_EQ(api::Cursor::close(m_sys, sec), DB_SUCCESS);

  crsr.close();
  commit(trx);
}

/** Threads inserting disjoint keys into one table: every row is there once. */
TEST_F(ApiDb, ConcurrentInserts) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  constexpr uint32_t n_threads = 8;
  constexpr uint32_t n_rows = 500;

  std::vector<std::thread> threads;

  for (uint32_t t = 0; t < n_threads; ++t) {
    threads.emplace_back([&, t] {
      for (uint32_t i = 0; i < n_rows; i += 50) {
        auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

        for (uint32_t j = i; j < i + 50; ++j) {
          const auto k = j * n_threads + t;

          EXPECT_EQ(insert_kv(m_sys, trx, table(), k, t), DB_SUCCESS) << k;
        }

        EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  auto trx = begin();
  const auto rows = scan_kv(m_sys, trx, table());

  ASSERT_EQ(rows.size(), n_threads * n_rows);

  for (uint32_t k = 0; k < rows.size(); ++k) {
    EXPECT_EQ(rows[k], std::make_pair(k, k % n_threads));
  }

  commit(trx);
}

/** Threads updating the same rows: the updates serialize, none is lost. */
TEST_F(ApiDb, ConcurrentUpdates) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 4, [](uint32_t) { return 0; });

  constexpr int n_threads = 4;
  constexpr int n_incs = 50;

  std::atomic<int> n_retries{};
  std::vector<std::thread> threads;

  for (int t = 0; t < n_threads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < n_incs; ++i) {
        for (;;) {
          auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
          const uint32_t k = (t + i) % 4;

          /* Read the row with an X lock, then write it back + 1. */
          Crsr crsr(m_sys, trx, table());
          auto err = crsr->set_lock_mode(m_sys, IB_LOCK_X);

          if (err == DB_SUCCESS) {
            err = seek_kv(m_sys, crsr.m_crsr, k);
          }

          if (err == DB_SUCCESS) {
            Tpl old_tpl(crsr->clust_read_tuple_create());
            Tpl new_tpl(crsr->clust_read_tuple_create());
            uint32_t v{};

            err = crsr->read_row(m_sys, old_tpl.get());

            if (err == DB_SUCCESS) {
              EXPECT_EQ(old_tpl->read_u32(1, &v), DB_SUCCESS);
              EXPECT_EQ(new_tpl->copy(old_tpl.get()), DB_SUCCESS);
              EXPECT_EQ(new_tpl->write_u32(1, v + 1), DB_SUCCESS);
              err = crsr->update_row(m_sys, old_tpl.get(), new_tpl.get());
            }
          }

          crsr.close();

          if (err == DB_SUCCESS) {
            EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
            break;
          }

          EXPECT_TRUE(err == DB_DEADLOCK || err == DB_LOCK_WAIT_TIMEOUT) << err;
          EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS);
          ++n_retries;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  auto trx = begin();
  uint32_t sum{};

  for (auto [k, v] : scan_kv(m_sys, trx, table())) {
    sum += v;
  }

  EXPECT_EQ(sum, uint32_t(n_threads * n_incs));
  commit(trx);
}

/* ---- Performance ---- */

namespace {

/** The shared instance of the performance tests. */
class ApiPerf : public ApiDb {};

/** Run body(trx, i) for i in [0, n) in trxs of batch operations each. */
void batched(System &sys, uint64_t i, uint64_t batch, Trx *&trx, const std::function<void(Trx *)> &body) {
  if (i % batch == 0) {
    trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  }

  body(trx);

  if (i % batch == batch - 1) {
    EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
    trx = nullptr;
  }
}

}  // namespace

/** Insert, point select, update and delete by primary key, one thread, 1000 rows a trx. */
TEST_F(ApiPerf, Dml) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  constexpr uint64_t n = 20000;
  constexpr uint64_t batch = 1000;
  Trx *trx{};

  /* Keys spread over the key space, not in order. */
  auto key = [](uint64_t i) { return uint32_t((i * 2654435761ULL) % 1000003); };

  auto ins = ut::bench("insert", n, [&](uint64_t i) {
    batched(m_sys, i, batch, trx, [&](Trx *t) { EXPECT_EQ(insert_kv(m_sys, t, table(), key(i), uint32_t(i)), DB_SUCCESS); });
  });

  auto sel = ut::bench("select", n, [&](uint64_t i) {
    batched(m_sys, i, batch, trx, [&](Trx *t) { EXPECT_EQ(select_kv(m_sys, t, table(), key(i)), uint32_t(i)); });
  });

  auto upd = ut::bench("update", n, [&](uint64_t i) {
    batched(m_sys, i, batch, trx, [&](Trx *t) { EXPECT_EQ(update_kv(m_sys, t, table(), key(i), uint32_t(i + 1)), DB_SUCCESS); });
  });

  auto del = ut::bench("delete", n, [&](uint64_t i) {
    batched(m_sys, i, batch, trx, [&](Trx *t) { EXPECT_EQ(delete_kv(m_sys, t, table(), key(i)), DB_SUCCESS); });
  });

  /* Debug build, each op opens a cursor: generous bounds. */
  EXPECT_LT(ins.ns_per_op(), 2e6);
  EXPECT_LT(sel.ns_per_op(), 2e6);
  EXPECT_LT(upd.ns_per_op(), 2e6);
  EXPECT_LT(del.ns_per_op(), 2e6);
}

/** Insert with one open cursor, the way a bulk load does. */
TEST_F(ApiPerf, InsertOneCursor) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();
  Crsr crsr(m_sys, trx, table());
  Tpl tpl(crsr->clust_read_tuple_create());

  auto r = ut::bench("insert_one_cursor", 50000, [&](uint64_t i) {
    EXPECT_EQ(tpl->write_u32(0, uint32_t(i)), DB_SUCCESS);
    EXPECT_EQ(tpl->write_u32(1, uint32_t(i)), DB_SUCCESS);
    EXPECT_EQ(crsr->insert_row(m_sys, tpl.get()), DB_SUCCESS);
  });

  crsr.close();
  commit(trx);

  EXPECT_LT(r.ns_per_op(), 1e6);

  /* And a full scan of what was inserted. */
  trx = begin();

  Crsr scan(m_sys, trx, table(), IB_LOCK_IS);
  Tpl row(scan->clust_read_tuple_create());
  uint64_t n{};
  auto err = scan->first(m_sys);

  auto s = ut::bench("scan", 50000, [&](uint64_t) {
    EXPECT_EQ(err, DB_SUCCESS);
    EXPECT_EQ(scan->read_row(m_sys, row.get()), DB_SUCCESS);
    err = scan->next(m_sys);
    ++n;
  });

  EXPECT_EQ(err, DB_END_OF_INDEX);
  EXPECT_EQ(n, 50000UL);
  EXPECT_LT(s.ns_per_op(), 1e6);

  scan.close();
  commit(trx);
}

/** Insert and select in 1 to 16 threads, disjoint keys. */
TEST_F(ApiPerf, DmlThreads) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  constexpr uint64_t n = 2000;
  constexpr uint64_t batch = 500;
  uint32_t base{};

  for (int n_threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs(n_threads);

    auto key = [&](int t, uint64_t i) { return base + uint32_t(i) * 16 + uint32_t(t); };

    auto ins = ut::bench_mt(std::format("insert_t{}", n_threads), n_threads, n, [&](int t, uint64_t i) {
      batched(m_sys, i, batch, trxs[t], [&](Trx *trx) { EXPECT_EQ(insert_kv(m_sys, trx, table(), key(t, i), 1), DB_SUCCESS); });
    });

    auto sel = ut::bench_mt(std::format("select_t{}", n_threads), n_threads, n, [&](int t, uint64_t i) {
      batched(m_sys, i, batch, trxs[t], [&](Trx *trx) { EXPECT_EQ(select_kv(m_sys, trx, table(), key(t, i)), 1U); });
    });

    EXPECT_LT(ins.ns_per_op(), 2e6);
    EXPECT_LT(sel.ns_per_op(), 2e6);

    base += uint32_t(n) * 16;
  }
}

/** A read only trx: begin and commit. */
TEST_F(ApiPerf, TrxBeginCommit) {
  auto r = ut::bench("trx_begin_commit", 20000, [&](uint64_t) {
    auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  });

  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** A write trx of one row: the commit flushes the log. */
TEST_F(ApiPerf, TrxInsertCommit) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto r = ut::bench("trx_insert_commit", 1000, [&](uint64_t i) {
    auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
    EXPECT_EQ(insert_kv(m_sys, trx, table(), uint32_t(i), 0), DB_SUCCESS);
    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  });

  EXPECT_LT(r.ns_per_op(), 50e6);
}

/** Open and close a cursor on a table. */
TEST_F(ApiPerf, CursorOpenClose) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto trx = begin();

  auto r = ut::bench("cursor_open_close", 20000, [&](uint64_t) {
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  });

  commit(trx);

  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** CREATE TABLE and DROP TABLE. */
TEST_F(ApiPerf, CreateDropTable) {
  constexpr uint64_t n = 20;

  auto c = ut::bench("create_table", n, [&](uint64_t i) {
    EXPECT_EQ(create_kv(m_sys, table(std::format("_{}", i).c_str())), DB_SUCCESS);
  });

  auto d = ut::bench("drop_table", n, [&](uint64_t i) { EXPECT_EQ(drop_table(m_sys, table(std::format("_{}", i).c_str())), DB_SUCCESS); });

  EXPECT_LT(c.ns_per_op(), 2e9);
  EXPECT_LT(d.ns_per_op(), 2e9);
}
