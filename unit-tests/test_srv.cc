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

/** @file unit-tests/test_srv.cc
Srv: the server threads, the background drop of tables, the monitor. Then on a
shared instance (SrvDb): the threads, master wake-ups, the exported status, lock
wait timeouts, the monitor with locks, the monitor tables, the background drop by
the master thread; the shutdown modes followed by a restart (SrvShutdown); and
their performance (SrvPerf). */

#include "ut0env.h"
#include "ut0perf.h"

#include "ddl0ddl.h"
#include "dict0dict.h"
#include "log0log.h"
#include "srv0srv.h"
#include "trx0sys.h"

#include <cstdio>
#include <format>
#include <functional>
#include <future>
#include <thread>

using namespace std::chrono_literals;

namespace {

using SrvTest = ut::DbTest;

/** The background threads run while the instance is up. */
TEST_F(SrvTest, Threads) {
  EXPECT_GT(m_sys->m_srv->n_background_threads(), 0);

  m_sys->m_srv->wake_master_thread();
  m_sys->m_srv->active_wake_master_thread();
}

/** A table dropped while a cursor has it open goes to the background drop
queue; the master thread drops it once the cursor is closed. */
TEST_F(SrvTest, BackgroundDrop) {
  load({{1, ut::Row{1, 1, "x"}}});

  auto reader = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, ut::TABLE, reader, &crsr), DB_SUCCESS);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
  commit(trx);

  ASSERT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  commit(reader);

  /* The master thread works the queue once a second, more or less. */
  bool gone{};

  for (int i = 0; i < 100 && !gone; ++i) {
    m_sys->m_srv->wake_master_thread();
    std::this_thread::sleep_for(100ms);

    m_sys->m_dict->mutex_acquire();
    gone = m_sys->m_dict->table_get(ut::TABLE) == nullptr;
    m_sys->m_dict->mutex_release();
  }

  EXPECT_TRUE(gone);
  EXPECT_FALSE(ut::fs::exists(m_dir.m_path / "test" / "t.ibd"));

  restart();

  m_sys->m_dict->mutex_acquire();
  EXPECT_EQ(m_sys->m_dict->table_get(ut::TABLE), nullptr);
  m_sys->m_dict->mutex_release();
}

/** The monitor output is produced without blocking. */
TEST_F(SrvTest, Monitor) {
  load({{1, ut::Row{1, 1, "x"}}});

  auto out = std::tmpfile();

  ASSERT_NE(out, nullptr);

  ulint start{}, end{};

  /* The report goes to the log; it used to release the trx sys mutex
  before taking it. */
  EXPECT_TRUE(m_sys->m_srv->printf_innodb_monitor(out, false, &start, &end));

  std::fclose(out);

  m_sys->m_srv->export_innodb_status();
}


/* ------------------------------------------------------------------------
On one instance shared by the suite (SrvDb): lock waits time out after 1 s. */

/** CREATE TABLE name (k INT UNSIGNED, v INT UNSIGNED, PRIMARY KEY(k)) */
void create_kv(System &sys, const std::string &name) {
  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create(name.c_str(), &sch, IB_TBL_V1, 0), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("v", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("PRIMARY", &idx), DB_SUCCESS);
  ASSERT_EQ(idx->add_col("k", 0), DB_SUCCESS);
  ASSERT_EQ(idx->set_clustered(), DB_SUCCESS);

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  uint64_t id{};

  EXPECT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);
  EXPECT_EQ(api::table_create(sys, trx, sch, &id), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
  api::TableSchema::destroy(sys, sch);
}

/** INSERT (k, v) in trx. */
DbErr insert_kv(System &sys, Trx *trx, const std::string &name, uint32_t k, uint32_t v) {
  api::Cursor *crsr{};

  if (auto err = api::cursor_open_table(sys, name.c_str(), trx, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->lock(sys, IB_LOCK_IX);

  if (err == DB_SUCCESS) {
    auto tpl = crsr->clust_read_tuple_create();

    EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    EXPECT_EQ(tpl->write_u32(1, v), DB_SUCCESS);
    err = crsr->insert_row(sys, tpl);
    api::Tuple::destroy(tpl);
  }

  EXPECT_EQ(api::Cursor::close(sys, crsr), DB_SUCCESS);

  return err;
}

/** A started instance shared by the suite. */
class SrvDb : public ::testing::Test {
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

  static std::string table(const std::string &suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  /** @return the value of a status variable. */
  int64_t status(const char *name) {
    int64_t v{-1};

    EXPECT_EQ(api::status_get_i64(m_sys, name, &v), DB_SUCCESS) << name;

    return v;
  }

  /** Wait up to 10 s for pred. @return pred() */
  static bool wait_for(const std::function<bool()> &pred) {
    for (int i = 0; i < 1000 && !pred(); ++i) {
      std::this_thread::sleep_for(10ms);
    }

    return pred();
  }

  static inline ut::TestDir *s_dir{};
  static inline System *s_sys{};

  System &m_sys{*s_sys};
  Srv &m_srv{*s_sys->m_srv};
};

/** The background threads: the i/o handlers, the master, the lock timeout and the monitors. */
TEST_F(SrvDb, ThreadsRunning) {
  const auto n_io = 1 + m_sys.m_config.m_aio.m_n_read_threads + m_sys.m_config.m_aio.m_n_write_threads;

  EXPECT_GT(m_srv.n_background_threads(), int(n_io));
  EXPECT_TRUE(m_srv.m_was_started);
  EXPECT_FALSE(m_srv.m_is_being_started);
  EXPECT_EQ(m_srv.m_shutdown_state, SRV_SHUTDOWN_NONE);
  /* The threads set the flags when they run: they may be just starting. */
  EXPECT_TRUE(wait_for([&] { return m_srv.m_monitor_active && m_srv.m_error_monitor_active; }));
  EXPECT_LE(m_srv.m_start_lsn, m_sys.m_log->get_lsn());
}

/** Waking the master thread counts as activity, it can be done from many threads at once. */
TEST_F(SrvDb, WakeMaster) {
  const auto before = m_srv.m_activity_count;
  std::vector<std::thread> threads;

  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 1000; ++i) {
        if (i % 2 == 0) {
          m_srv.wake_master_thread();
        } else {
          m_srv.active_wake_master_thread();
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  /* Not atomic: at least the ones of one thread. */
  EXPECT_GE(m_srv.m_activity_count - before, 1000UL);
}

/** The exported status reflects the buffer pool and the DML. */
TEST_F(SrvDb, ExportStatus) {
  create_kv(m_sys, table());

  const auto ins0 = status("row_total_inserted");
  const auto reads0 = status("buffer_pool_read_reqs");

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  for (uint32_t k = 0; k < 100; ++k) {
    ASSERT_EQ(insert_kv(m_sys, trx, table(), k, k), DB_SUCCESS);
  }

  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  m_srv.export_innodb_status();

  const auto &v = m_srv.m_export_vars;

  EXPECT_EQ(v.m_page_size, UNIV_PAGE_SIZE);
  EXPECT_EQ(v.m_buffer_pool_pages_total, m_sys.m_config.m_buf_pool.m_size / UNIV_PAGE_SIZE);
  EXPECT_GT(v.m_buffer_pool_pages_data, 0UL);
  EXPECT_LE(v.m_buffer_pool_pages_data + v.m_buffer_pool_pages_free, v.m_buffer_pool_pages_total);
  EXPECT_LE(v.m_buffer_pool_pages_dirty, v.m_buffer_pool_pages_data);

  EXPECT_GE(status("row_total_inserted") - ins0, 100);
  EXPECT_GT(status("buffer_pool_read_reqs"), reads0);
  EXPECT_GT(status("log_write_reqs"), 0);
}

/** A lock wait ends after the lock wait timeout, the wait is counted. */
TEST_F(SrvDb, LockWaitTimeout) {
  create_kv(m_sys, table());

  auto holder = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(insert_kv(m_sys, holder, table(), 1, 1), DB_SUCCESS);

  const auto waits0 = status("lock_row_waits");
  auto waiter = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  const auto start = std::chrono::steady_clock::now();

  auto result = std::async(std::launch::async, [&] { return insert_kv(m_sys, waiter, table(), 1, 2); });

  /* The wait is visible while it lasts. */
  EXPECT_TRUE(wait_for([&] { return status("lock_row_waiting") == 1; }));

  const auto err = result.get();
  const auto waited = std::chrono::steady_clock::now() - start;

  EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);
  EXPECT_GE(waited, 900ms);
  EXPECT_LT(waited, 5s);

  EXPECT_EQ(status("lock_row_waiting"), 0);
  EXPECT_EQ(status("lock_row_waits") - waits0, 1);
  /* In milliseconds, despite the name (as upstream: innodb_row_lock_time_max). */
  EXPECT_GE(status("lock_max_wait_time_in_secs"), 900);
  EXPECT_LE(status("lock_max_wait_time_in_secs"), 5000);

  EXPECT_EQ(api::trx_rollback(m_sys, waiter), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, holder), DB_SUCCESS);
}

/** The monitor output, with transactions holding locks. */
TEST_F(SrvDb, MonitorWithLocks) {
  create_kv(m_sys, table());

  auto holder = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  auto idle = api::trx_begin(m_sys, IB_TRX_READ_COMMITTED);

  for (uint32_t k = 0; k < 10; ++k) {
    ASSERT_EQ(insert_kv(m_sys, holder, table(), k, k), DB_SUCCESS);
  }

  ::testing::internal::CaptureStderr();

  ulint start{}, end{};

  const auto printed = m_srv.printf_innodb_monitor(stderr, false, &start, &end);
  const auto output = ::testing::internal::GetCapturedStderr();

  EXPECT_EQ(api::trx_commit(m_sys, idle), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, holder), DB_SUCCESS);

  EXPECT_TRUE(printed);
  EXPECT_NE(output.find("INNODB MONITOR OUTPUT"), std::string::npos);
  EXPECT_NE(output.find("BACKGROUND THREAD"), std::string::npos);
  EXPECT_NE(output.find("TRANSACTIONS"), std::string::npos);
  EXPECT_NE(output.find("LIST OF TRANSACTIONS FOR EACH SESSION"), std::string::npos);
}

/** The monitor output with a transaction waiting for a lock.
Regression: lock/lock0lock.cc:2602 (LockSys::print_info_all_transactions) prints the
lock a waiting trx waits for with lock->to_string(), but "lock" is the loop's
local, still nullptr there: a null dereference (Lock::type, this=0x0). Upstream
(7c21eac0 lock0lock.c:3985) printed trx->wait_lock. */
TEST_F(SrvDb, MonitorWithLockWait) {
  create_kv(m_sys, table());

  auto holder = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(insert_kv(m_sys, holder, table(), 1, 1), DB_SUCCESS);

  auto waiter = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  auto result = std::async(std::launch::async, [&] { return insert_kv(m_sys, waiter, table(), 1, 2); });

  ASSERT_TRUE(wait_for([&] { return status("lock_row_waiting") == 1; }));

  ::testing::internal::CaptureStderr();

  ulint start{}, end{};

  EXPECT_TRUE(m_srv.printf_innodb_monitor(stderr, true, &start, &end));

  const auto output = ::testing::internal::GetCapturedStderr();

  EXPECT_EQ(result.get(), DB_LOCK_WAIT_TIMEOUT);
  EXPECT_EQ(api::trx_rollback(m_sys, waiter), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, holder), DB_SUCCESS);

  EXPECT_NE(output.find("INNODB MONITOR OUTPUT"), std::string::npos);
  EXPECT_NE(output.find("BACKGROUND THREAD"), std::string::npos);
  EXPECT_NE(output.find("TRANSACTIONS"), std::string::npos) << output.substr(0, 4000);
}

/** CREATE TABLE innodb_monitor turns the periodic monitor output on, DROP turns it off;
the same for the table monitor. */
TEST_F(SrvDb, MonitorTables) {
  EXPECT_FALSE(m_srv.m_print_innodb_monitor);

  create_kv(m_sys, "test/innodb_monitor");
  EXPECT_TRUE(m_srv.m_print_innodb_monitor);

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(m_sys, trx, "test/innodb_monitor"), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  EXPECT_FALSE(m_srv.m_print_innodb_monitor);

  create_kv(m_sys, "test/innodb_table_monitor");
  EXPECT_TRUE(m_srv.m_print_innodb_table_monitor);

  trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(m_sys, trx, "test/innodb_table_monitor"), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  EXPECT_FALSE(m_srv.m_print_innodb_table_monitor);
}

/** The master thread drops a table in the background drop queue by itself once it is closed. */
TEST_F(SrvDb, BackgroundDropByMaster) {
  create_kv(m_sys, table());

  auto reader = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), reader, &crsr), DB_SUCCESS);

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(m_sys, trx, table().c_str()), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  /* While open, the master thread leaves it. */
  std::this_thread::sleep_for(1500ms);

  m_sys.m_dict->mutex_acquire();
  EXPECT_NE(m_sys.m_dict->table_check_if_in_cache(table().c_str()), nullptr);
  m_sys.m_dict->mutex_release();

  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, reader), DB_SUCCESS);

  EXPECT_TRUE(wait_for([&] {
    m_sys.m_dict->mutex_acquire();
    const auto gone = m_sys.m_dict->table_check_if_in_cache(table().c_str()) == nullptr;
    m_sys.m_dict->mutex_release();
    return gone;
  }));

  mutex_enter(&m_sys.m_trx_sys->m_mutex);
  EXPECT_EQ(m_sys.m_dict->m_ddl.get_background_drop_list_len(), 0UL);
  mutex_exit(&m_sys.m_trx_sys->m_mutex);
}

/** request_monitor_print() turns the periodic monitor output on (printed every
15 s by the monitor thread, too slow to wait for here). */
TEST_F(SrvDb, RequestMonitorPrint) {
  m_srv.request_monitor_print();
  EXPECT_TRUE(m_srv.m_print_innodb_monitor);
  m_srv.m_print_innodb_monitor = false;

  /* The instance works on. */
  create_kv(m_sys, table());

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  EXPECT_EQ(insert_kv(m_sys, trx, table(), 1, 1), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
}

/* ---- Shutdown modes, each followed by a restart: a DbTest each ---- */

using SrvShutdown = ut::DbTest;

/** Shut down in mode, start again: the committed rows are there. */
void shutdown_restart(std::unique_ptr<System> &sys, const Config &config, ShutdownMode mode) {
  ASSERT_EQ(sys->shutdown(mode), DB_SUCCESS);
  sys.reset();

  sys = std::make_unique<System>();
  ASSERT_EQ(sys->init(config), DB_SUCCESS);
  ASSERT_EQ(sys->startup(), DB_SUCCESS);
}

/** A fast shutdown (no purge) and a restart. */
TEST_F(SrvShutdown, Fast) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 200; ++k) {
    rows[k] = ut::Row{k, k % 7, ut::blob(k, 50)};
  }

  load(rows);

  /* Leave work for the purge: deletes. */
  auto trx = begin();

  for (uint32_t k = 0; k < 200; k += 2) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    rows.erase(k);
  }

  commit(trx);

  shutdown_restart(m_sys, m_config, IB_SHUTDOWN_NO_IBUFMERGE_PURGE);
  expect_rows(rows);
}

/** The fastest shutdown, no buffer pool flush, is a crash: the restart recovers
the committed rows from the log.
Regression: srv/srv0sys.cc:808-810 (System::shutdown) returns right after
prepare_for_shutdown() for IB_SHUTDOWN_NO_BUFPOOL_FLUSH, without threads_shutdown(),
m_log->shutdown() or destroy() (upstream returned there too, the process was about
to exit). The System is then destroyed with its i/o handler and server threads
running: they call m_sys->m_srv->thread_exited() on a destroyed Srv
(srv0sys.cc:93, this=0x0) and crash. */
TEST_F(SrvShutdown, NoFlush) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 300; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  shutdown_restart(m_sys, m_config, IB_SHUTDOWN_NO_BUFPOOL_FLUSH);
  expect_rows(rows);
}

/* ---- Performance ---- */

class SrvPerf : public SrvDb {};

/** Waking the master thread, and exporting the status. */
TEST_F(SrvPerf, WakeAndStatus) {
  auto w = ut::bench("wake_master_thread", 20000, [&](uint64_t) { m_srv.wake_master_thread(); });
  auto a = ut::bench("active_wake_master_thread", 200000, [&](uint64_t) { m_srv.active_wake_master_thread(); });
  auto e = ut::bench("export_innodb_status", 5000, [&](uint64_t) { m_srv.export_innodb_status(); });

  int64_t v{};

  auto s = ut::bench("status_get_i64", 5000, [&](uint64_t) { EXPECT_EQ(api::status_get_i64(m_sys, "row_total_read", &v), DB_SUCCESS); });

  EXPECT_LT(w.ns_per_op(), 1e6);
  EXPECT_LT(a.ns_per_op(), 100000);
  EXPECT_LT(e.ns_per_op(), 5e6);
  EXPECT_LT(s.ns_per_op(), 5e6);
}

/** The time from the timeout of a lock wait to the waiter running again. */
TEST_F(SrvPerf, LockWaitTimeout) {
  create_kv(m_sys, table());

  auto holder = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(insert_kv(m_sys, holder, table(), 1, 1), DB_SUCCESS);

  auto r = ut::bench("lock_wait_timeout_1s", 3, [&](uint64_t) {
    auto waiter = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
    EXPECT_EQ(insert_kv(m_sys, waiter, table(), 1, 2), DB_LOCK_WAIT_TIMEOUT);
    EXPECT_EQ(api::trx_rollback(m_sys, waiter), DB_SUCCESS);
  });

  EXPECT_EQ(api::trx_commit(m_sys, holder), DB_SUCCESS);

  /* 1 s timeout, checked once a second. */
  EXPECT_LT(r.ns_per_op(), 30e9);
}

}  // namespace
