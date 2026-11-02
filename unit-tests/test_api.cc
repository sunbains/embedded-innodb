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
  - api::trx_interrupt() replaces the trx_is_interrupted callback. */

#include "api0api.h"
#include "srv0cfg.h"
#include "srv0srv.h"
#include "srv0sys.h"
#include "ut0logger.h"

#include <gtest/gtest.h>

#include <chrono>
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

TEST(ConfigTest, LogLimitsAreChecked) {
  {
    Config config;

    config.m_log.m_n_files = 1;
    EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);
  }

  {
    Config config;

    /* The combined size of the log files must stay below 4 GB. */
    config.m_log.m_n_files = 2;
    config.m_log.m_file_size = 2UL * 1024 * 1024 * 1024;
    EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);
  }
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
    m_dir = fs::temp_directory_path() / std::format("innodb-api-test-{}", ::getpid());
    fs::remove_all(m_dir);
    fs::create_directories(m_dir);

    m_config.m_fil.m_data_home = m_dir.string();
    m_config.m_log.m_group_home_dir = m_dir.string();
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

    ASSERT_EQ(api::TableSchema::create("test/t", &tbl_sch, IB_TBL_V1, 0), DB_SUCCESS);
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
