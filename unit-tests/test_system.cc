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

/** @file unit-tests/test_system.cc
Lifecycle of a System: create a database, shut it down, open it again. Then
the Config limits, init() alone and its retry, startup failures, start/stop
cycles, two instances in one process, reopening with another config, crash
recovery, and the time to start and stop (SystemPerf). */

#include "srv0srv.h"
#include "srv0sys.h"
#include "ut0env.h"
#include "ut0perf.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <format>

namespace fs = std::filesystem;

class SystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_dir = ut::scratch_root() / std::format("innodb-system-test-{}", ::getpid());
    fs::remove_all(m_dir);
    fs::create_directories(m_dir);
  }

  void TearDown() override { fs::remove_all(m_dir); }

  [[nodiscard]] Config make_config() const {
    Config config;

    config.m_fil.m_data_home = m_dir.string();
    config.m_log.m_log_dir = m_dir.string();
    config.m_buf_pool.m_size = 32 * 1024 * 1024;

    return config;
  }

  fs::path m_dir;
};

TEST_F(SystemTest, CreateShutdownReopen) {
  const auto config = make_config();

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);
    ASSERT_TRUE(fs::exists(m_dir / "system.ibd"));
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }
}

TEST_F(SystemTest, InvalidConfigIsRejected) {
  auto config = make_config();

  config.m_log.m_checkpoint_age = 0;

  System sys;

  ASSERT_NE(sys.init(config), DB_SUCCESS);
}

/* ------------------------------------------------------------------------
More of the lifecycle: init without startup, the limits of the Config, startup
failures, start/stop cycles, crash recovery, two instances in one process, and
the time to start and stop (SystemPerf). */

namespace {

/** CREATE TABLE name (k INT UNSIGNED, PRIMARY KEY(k)) with the keys [0, n). */
void create_rows(System &sys, const char *name, uint32_t n) {
  (void)api::database_create(sys, "test");

  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create(name, &sch, IB_TBL_V1), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("PRIMARY", &idx), DB_SUCCESS);
  ASSERT_EQ(idx->add_col("k", 0), DB_SUCCESS);
  ASSERT_EQ(idx->set_clustered(), DB_SUCCESS);

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  uint64_t id{};

  EXPECT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::table_create(sys, trx, sch, &id), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
  api::TableSchema::destroy(sys, sch);

  trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(sys, name, trx, &crsr), DB_SUCCESS);
  EXPECT_EQ(crsr->lock(sys, IB_LOCK_IX), DB_SUCCESS);

  auto tpl = crsr->clust_read_tuple_create();

  for (uint32_t k = 0; k < n; ++k) {
    EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    EXPECT_EQ(crsr->insert_row(sys, tpl), DB_SUCCESS);
  }

  api::Tuple::destroy(tpl);
  EXPECT_EQ(api::Cursor::close(sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
}

/** @return the number of rows of name, or -1 if it can't be opened. */
int64_t count_rows(System &sys, const char *name) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};
  int64_t n{-1};

  if (api::cursor_open_table(sys, name, trx, &crsr) == DB_SUCCESS) {
    n = 0;

    for (auto err = crsr->first(sys); err == DB_SUCCESS; err = crsr->next(sys)) {
      ++n;
    }

    EXPECT_EQ(api::Cursor::close(sys, crsr), DB_SUCCESS);
  }

  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  return n;
}

/** A test with a directory of its own. */
class SysLife : public ::testing::Test {
 protected:
  /** Start a System on m_dir with config. */
  std::unique_ptr<System> start(const Config &config) {
    auto sys = std::make_unique<System>();

    EXPECT_EQ(sys->init(config), DB_SUCCESS);
    EXPECT_EQ(sys->startup(), DB_SUCCESS);

    return sys;
  }

  ut::TestDir m_dir;
  Config m_config{ut::make_config(m_dir.m_path)};
};

/* ---- Config ---- */

/** The checkpoint age: at least 1 MB. */
TEST(SysConfig, CheckpointAge) {
  Config config;

  config.m_log.m_checkpoint_age = 1024 * 1024 - 1;
  EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);

  config.m_log.m_checkpoint_age = 1024 * 1024;
  EXPECT_EQ(config.normalize(), DB_SUCCESS);
}

/** The i/o threads: the log thread and the read and write threads, at most SRV_MAX_N_IO_THREADS. */
TEST(SysConfig, IoThreads) {
  Config config;

  config.m_aio.m_n_read_threads = 16;
  config.m_aio.m_n_write_threads = SRV_MAX_N_IO_THREADS - 1 - 16;
  EXPECT_EQ(config.normalize(), DB_SUCCESS);

  ++config.m_aio.m_n_write_threads;
  EXPECT_EQ(config.normalize(), DB_INVALID_INPUT);

  config.m_aio.m_n_read_threads = 1;
  config.m_aio.m_n_write_threads = 1;
  EXPECT_EQ(config.normalize(), DB_SUCCESS);
}

/** The old LRU sublist: 5 to 95 percent, the boundaries included. */
TEST(SysConfig, OldBlocksPct) {
  for (auto [pct, ok] : {std::pair(4UL, false), std::pair(5UL, true), std::pair(95UL, true), std::pair(96UL, false)}) {
    Config config;

    config.m_buf_pool.m_lru_old_blocks_pct = pct;
    EXPECT_EQ(config.normalize(), ok ? DB_SUCCESS : DB_INVALID_INPUT) << pct;
  }
}

/** The thread limit and the lock table size are derived from the buffer pool size. */
TEST(SysConfig, Derived) {
  for (auto [size, n] : {std::pair(4UL << 20, 128UL), std::pair(8UL << 20, 4096UL), std::pair(1000UL << 20, 8192UL)}) {
    Config config;

    config.m_buf_pool.m_size = size;
    ASSERT_EQ(config.normalize(), DB_SUCCESS);
    EXPECT_EQ(config.m_srv.m_max_n_threads, n) << size;
    EXPECT_EQ(config.m_lock.m_table_size, 5 * size / UNIV_PAGE_SIZE) << size;
  }
}

/** init() checks the Config before anything is created, and can be retried. */
TEST_F(SysLife, InitRetry) {
  System sys;
  auto bad = m_config;

  bad.m_buf_pool.m_lru_old_blocks_pct = 1;

  EXPECT_EQ(sys.init(bad), DB_INVALID_INPUT);
  EXPECT_EQ(sys.m_srv, nullptr);
  EXPECT_EQ(sys.m_buf_pool, nullptr);

  ASSERT_EQ(sys.init(m_config), DB_SUCCESS);
  EXPECT_NE(sys.m_srv, nullptr);

  /* The copy of the config is the normalized one. */
  EXPECT_EQ(sys.m_config.m_fil.m_data_home.back(), '/');
  EXPECT_NE(sys.m_config.m_srv.m_max_n_threads, 0UL);
}

/** init() alone creates every sub-system and touches no file. */
TEST_F(SysLife, InitOnly) {
  {
    System sys;

    ASSERT_EQ(sys.init(m_config), DB_SUCCESS);

    EXPECT_NE(sys.m_aio, nullptr);
    EXPECT_NE(sys.m_fil, nullptr);
    EXPECT_NE(sys.m_buf_pool, nullptr);
    EXPECT_NE(sys.m_log, nullptr);
    EXPECT_NE(sys.m_trx_sys, nullptr);
    EXPECT_NE(sys.m_lock_sys, nullptr);
    EXPECT_NE(sys.m_dict, nullptr);
    EXPECT_NE(sys.m_parser, nullptr);
    EXPECT_NE(sys.m_srv, nullptr);
    EXPECT_FALSE(sys.m_srv->m_was_started);
    EXPECT_EQ(sys.m_srv->n_background_threads(), 0);
  }

  EXPECT_TRUE(fs::is_empty(m_dir.m_path));
}

/** Startup on a data directory that doesn't exist fails; the System can be destroyed.
Regression: srv/srv0sys.cc:586-602 (System::startup) creates the i/o handler threads
(line 588), then returns DB_ERROR for a missing data home (line 601) without
stopping them: the System destructor destroys the sub-systems under the running
threads, a segmentation fault every time. */
TEST_F(SysLife, StartupMissingDir) {
  auto config = m_config;

  config.m_fil.m_data_home = (m_dir.m_path / "none").string();
  config.m_log.m_log_dir = config.m_fil.m_data_home;

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    EXPECT_EQ(sys.startup(), DB_ERROR);
  }

  EXPECT_FALSE(fs::exists(m_dir.m_path / "none"));
}

/** Start and stop three times: the data stays, the LSN only grows. */
TEST_F(SysLife, Cycles) {
  lsn_t prev{};

  for (int i = 0; i < 3; ++i) {
    auto sys = start(m_config);

    ASSERT_TRUE(sys->m_srv->m_was_started);

    if (i == 0) {
      create_rows(*sys, "test/t", 100);
    } else {
      EXPECT_EQ(count_rows(*sys, "test/t"), 100) << i;
    }

    EXPECT_GE(sys->m_srv->m_start_lsn, prev) << i;
    prev = sys->m_srv->m_start_lsn;

    ASSERT_EQ(sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);

    /* shutdown() destroyed the sub-systems. */
    EXPECT_EQ(sys->m_srv, nullptr);
    EXPECT_EQ(sys->m_buf_pool, nullptr);
  }
}

/** Two instances in one process, at the same time: nothing is shared.
BUG (root cause not found): the startup() of the second instance, while the first
one runs, hangs for good in the checkpoint after creating the database:
System::startup (srv0sys.cc:674) -> Log::make_checkpoint_at (log0log.cc:180) ->
BufFlush::wait_batch_end (buf0flu.cc:904) waits on m_no_flush[BUF_FLUSH_LIST] of
the second buffer pool. Both instances have their own Fil, Aio and i/o handler
threads (seen in gdb); no static state found in the engine sources. Suspect some
process-wide state in the WAL backend (wal-cpp keeps function-static pools and
thread_local caches, include/util/memory_pool.h:303,409,419). */
TEST_F(SysLife, TwoInstances) {
  ut::TestDir other;

  auto a = start(m_config);
  auto b = start(ut::make_config(other.m_path));

  create_rows(*a, "test/t", 10);
  create_rows(*b, "test/t", 20);

  EXPECT_EQ(count_rows(*a, "test/t"), 10);
  EXPECT_EQ(count_rows(*b, "test/t"), 20);

  /* The dictionary of one doesn't know the tables of the other. */
  create_rows(*a, "test/only_a", 1);
  EXPECT_EQ(count_rows(*b, "test/only_a"), -1);

  EXPECT_EQ(a->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);

  /* b is not affected by a's shutdown. */
  EXPECT_EQ(count_rows(*b, "test/t"), 20);
  EXPECT_EQ(b->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
}

/** An existing database opens with another buffer pool size and other i/o threads. */
TEST_F(SysLife, ReopenOtherConfig) {
  {
    auto sys = start(m_config);

    create_rows(*sys, "test/t", 500);
    ASSERT_EQ(sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }

  auto config = m_config;

  config.m_buf_pool.m_size = 16 * 1024 * 1024;
  config.m_aio.m_n_read_threads = 2;
  config.m_aio.m_n_write_threads = 2;

  auto sys = start(config);

  EXPECT_EQ(count_rows(*sys, "test/t"), 500);
  EXPECT_EQ(sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
}

/** Killed with committed and uncommitted changes: recovery keeps the committed
ones only; then killed again during more work, recovery again. */
using SysCrash = ut::DbTest;

TEST_F(SysCrash, CommittedSurvive) {
  ut::Rows committed;

  for (uint32_t k = 0; k < 300; ++k) {
    committed[k] = ut::Row{k, k % 13, ut::blob(k, 80)};
  }

  crash([&] {
    load(committed);

    /* Not committed: rolled back by the recovery. */
    auto trx = begin();

    for (uint32_t k = 1000; k < 1100; ++k) {
      ASSERT_EQ(insert(trx, ut::Row{k, k, "lost"}), DB_SUCCESS);
    }

    ASSERT_EQ(update(trx, 5, ut::Row{5, 99999, "lost"}), DB_SUCCESS);
    ASSERT_EQ(remove(trx, 6), DB_SUCCESS);
  });

  expect_rows(committed);

  /* Again, on the recovered database. */
  crash([&] {
    auto trx = begin();

    ASSERT_EQ(remove(trx, 7), DB_SUCCESS);
    commit(trx);

    trx = begin();
    ASSERT_EQ(remove(trx, 8), DB_SUCCESS);
  });

  committed.erase(7);
  expect_rows(committed);
}

}  // namespace

/* ---- Performance ---- */

class SystemPerf : public SysLife {};

/** Startup and shutdown: a new database, an existing empty one, one with rows. */
TEST_F(SystemPerf, StartupShutdown) {
  std::unique_ptr<System> sys;

  auto create = ut::bench("startup_create", 1, [&](uint64_t) { sys = start(m_config); });
  auto stop = ut::bench("shutdown_empty", 1, [&](uint64_t) { EXPECT_EQ(sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS); });

  sys.reset();

  auto open = ut::bench("startup_existing", 1, [&](uint64_t) { sys = start(m_config); });

  create_rows(*sys, "test/t", 20000);

  auto stop_rows = ut::bench("shutdown_20000_rows", 1, [&](uint64_t) { EXPECT_EQ(sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS); });

  sys.reset();

  auto open_rows = ut::bench("startup_20000_rows", 1, [&](uint64_t) { sys = start(m_config); });

  EXPECT_EQ(count_rows(*sys, "test/t"), 20000);

  auto fast = ut::bench("shutdown_fast", 1, [&](uint64_t) { EXPECT_EQ(sys->shutdown(IB_SHUTDOWN_NO_IBUFMERGE_PURGE), DB_SUCCESS); });

  for (const auto &r : {create, stop, open, stop_rows, open_rows, fast}) {
    EXPECT_LT(r.m_secs, 120.0) << r.m_name;
  }
}
