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

/** @file unit-tests/test_row_undo.cc
RowUndo: undoing inserts, updates and deletes in the clustered and the
secondary indexes, by a rollback, a statement rollback and the rollback of
the transactions recovered after a crash. Most tests share a started
instance; each starts with an empty table (its rows may still be there,
delete marked, until purged: an insert of their key is an update).

  RowUndoTest       the shared instance.
  RowUndoCrashTest  an instance of its own, killed and recovered.
  RowUndoPerf       performance. */

#include "ut0env.h"
#include "ut0perf.h"

#include <sys/prctl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <functional>
#include <mutex>
#include <thread>
#include <future>

namespace {

/** A DbTest whose started instance is shared by the tests of a suite: the
first test starts it, the end of the suite shuts it down. Each test starts
with an empty test table. */
class SharedDb : public ut::DbTest {
 protected:
  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      s_sys.reset();
    }

    s_dir.reset();
  }

  void SetUp() override {
    if (s_dir == nullptr) {
      s_dir = std::make_unique<ut::TestDir>();
    }

    m_config = ut::make_config(s_dir->m_path);

    if (s_sys == nullptr) {
      start();
    } else {
      m_sys = std::move(s_sys);
    }

    clear();
  }

  void TearDown() override { s_sys = std::move(m_sys); }

  /** Delete all the rows of the test table. */
  void clear() {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (const auto &[k, row] : scan(trx)) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
  }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;
};

using RowUndoTest = SharedDb;

ut::Rows make_rows(uint32_t n) {
  ut::Rows rows;

  for (uint32_t k = 0; k < n; ++k) {
    rows[k] = ut::Row{k, k % 10, ut::blob(k, k % 5 == 0 ? 9000 : 60)};
  }

  return rows;
}

TEST_F(RowUndoTest, Insert) {
  load(make_rows(10));

  auto trx = begin();

  for (uint32_t k = 100; k < 300; ++k) {
    ASSERT_EQ(insert(trx, ut::Row{k, k % 3, ut::blob(k, 500)}), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(make_rows(10));
}

TEST_F(RowUndoTest, Delete) {
  const auto rows = make_rows(100);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 100; k += 3) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** Undo of an update of the secondary key: the new entry in SEC_A goes, the
old one is undeleted. The undo restored the position of the wrong cursor
to look for older versions that need the new entry. */
TEST_F(RowUndoTest, SecKeyUpdate) {
  const auto rows = make_rows(100);

  load(rows);

  auto trx = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_a = 1000 + k;
    ASSERT_EQ(update(trx, k, r), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** The same while an older read view needs the old versions: the SEC_A
entries are delete marked rather than removed. */
TEST_F(RowUndoTest, SecKeyUpdateWithReader) {
  const auto rows = make_rows(50);

  load(rows);

  auto reader = begin();

  EXPECT_EQ(scan(reader).size(), rows.size());

  auto first = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_a = 2000 + k;
    ASSERT_EQ(update(first, k, r), DB_SUCCESS);
  }

  commit(first);

  auto second = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_a = 3000 + k;
    ASSERT_EQ(update(second, k, r), DB_SUCCESS);
  }

  rollback(second);

  /* The reader still sees the original rows, through both indexes. */
  EXPECT_EQ(scan(reader), rows);
  EXPECT_EQ(scan_sec(reader).size(), rows.size());
  commit(reader);

  auto expected = rows;

  for (auto &[k, row] : expected) {
    row.m_a = 2000 + k;
  }

  expect_rows(expected);
}

/** Undo of primary key updates (delete mark and insert) and blob changes. */
TEST_F(RowUndoTest, PkAndBlobUpdate) {
  const auto rows = make_rows(60);

  load(rows);

  auto trx = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    if (k % 2 == 0) {
      r.m_k = k + 5000;
    } else {
      r.m_b = ut::blob(k + 9, k % 3 == 0 ? 40000 : 5);
    }

    ASSERT_EQ(update(trx, k, r), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** Insert, update and delete of the same row in one transaction. */
TEST_F(RowUndoTest, SameRow) {
  const auto rows = make_rows(5);

  load(rows);

  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{10, 1, "a"}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 10, ut::Row{10, 2, ut::blob(1, 20000)}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 10, ut::Row{11, 3, "b"}), DB_SUCCESS);
  ASSERT_EQ(remove(trx, 11), DB_SUCCESS);
  ASSERT_EQ(insert(trx, ut::Row{11, 4, "c"}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 2, ut::Row{2, 9, "d"}), DB_SUCCESS);
  ASSERT_EQ(remove(trx, 2), DB_SUCCESS);

  rollback(trx);
  expect_rows(rows);
}

/** An update of the secondary key and of a blob that goes off page, at
READ COMMITTED, rolled back. */
TEST_F(RowUndoTest, SecKeyAndBlob) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 50; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  for (auto level : {IB_TRX_READ_COMMITTED, IB_TRX_REPEATABLE_READ}) {
    auto trx = begin(level);

    for (uint32_t k = 0; k < 50; k += 2) {
      ASSERT_EQ(update(trx, k, ut::Row{k, k + 100, ut::blob(k, 9000)}), DB_SUCCESS);
    }

    rollback(trx);
    expect_rows(rows);
  }
}

/** Undo of an update of the non key columns in place and with a blob that
goes off the page and comes back. */
TEST_F(RowUndoTest, UpdateInPlace) {
  const auto rows = make_rows(40);

  load(rows);

  auto trx = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_b = ut::blob(k + 77, k % 3 == 0 ? 25000 : (k % 3 == 1 ? 3 : 0));
    ASSERT_EQ(update(trx, k, r), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** A row deleted and inserted again with the same key in one transaction:
the insert went to the delete marked record; the undo restores the row. */
TEST_F(RowUndoTest, DeleteThenInsert) {
  const auto rows = make_rows(20);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 20; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    ASSERT_EQ(insert(trx, ut::Row{k, 1000 + k, ut::blob(k, k % 2 ? 9000 : 9)}), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** Rows the transaction inserted and deleted again: nothing is left. */
TEST_F(RowUndoTest, InsertThenDelete) {
  const auto rows = make_rows(5);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 1000; k < 1030; ++k) {
    ASSERT_EQ(insert(trx, ut::Row{k, k, ut::blob(k, 300)}), DB_SUCCESS);
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** The same row updated many times in one transaction: the oldest values
come back. */
TEST_F(RowUndoTest, ManyUpdatesOneRow) {
  const auto rows = make_rows(3);

  load(rows);

  auto trx = begin();

  for (uint32_t i = 0; i < 200; ++i) {
    ASSERT_EQ(update(trx, 1, ut::Row{1, i % 7, ut::blob(i, i % 50 == 0 ? 20000 : i)}), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** Two rows swap their secondary keys through a third value: the undo
must put the entries back without a duplicate. */
TEST_F(RowUndoTest, SecKeySwap) {
  ut::Rows rows{{1, ut::Row{1, 10, "a"}}, {2, ut::Row{2, 20, "b"}}};

  load(rows);

  auto trx = begin();

  ASSERT_EQ(update(trx, 1, ut::Row{1, 30, "a"}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 2, ut::Row{2, 10, "b"}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 1, ut::Row{1, 20, "a"}), DB_SUCCESS);

  rollback(trx);
  expect_rows(rows);
}

/** A primary key moved onto the key of a row deleted earlier (committed):
the insert reuses the delete marked record; the undo marks it deleted again
and brings the moved row back. */
TEST_F(RowUndoTest, PkMoveOntoDeleted) {
  load({{1, ut::Row{1, 1, "one"}}, {2, ut::Row{2, 2, "two"}}});

  auto del = begin();

  ASSERT_EQ(remove(del, 2), DB_SUCCESS);
  commit(del);

  auto trx = begin();

  ASSERT_EQ(update(trx, 1, ut::Row{2, 5, "moved"}), DB_SUCCESS);

  rollback(trx);
  expect_rows({{1, ut::Row{1, 1, "one"}}});
}

/** Enough inserts and updates to split pages in both indexes, rolled back:
the undo finds the records where the splits put them. */
TEST_F(RowUndoTest, AfterSplits) {
  const auto rows = make_rows(300);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 300; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, 5000 - k, ut::blob(k, 400)}), DB_SUCCESS);
    ASSERT_EQ(insert(trx, ut::Row{10000 + k, k, ut::blob(k, 400)}), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** A reader with an older view during the rollback keeps seeing the
original rows; afterwards everybody does. */
TEST_F(RowUndoTest, ReaderDuringRollback) {
  const auto rows = make_rows(100);

  load(rows);

  auto reader = begin();

  ASSERT_EQ(scan(reader), rows);

  auto trx = begin();

  for (const auto &[k, row] : rows) {
    if (k % 3 == 0) {
      ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    } else if (k % 3 == 1) {
      ASSERT_EQ(update(trx, k, ut::Row{k + 500, k, "m"}), DB_SUCCESS);
    } else {
      ASSERT_EQ(update(trx, k, ut::Row{k, k + 900, ut::blob(k, 9000)}), DB_SUCCESS);
    }
  }

  auto rb = std::async(std::launch::async, [&] { rollback(trx); });

  EXPECT_EQ(scan(reader), rows);
  EXPECT_EQ(scan_sec(reader).size(), rows.size());

  rb.get();

  EXPECT_EQ(scan(reader), rows);
  commit(reader);
  expect_rows(rows);
}

/** Statement rollbacks of updates that change the secondary key, between
ones that succeed, in a transaction that commits. */
TEST_F(RowUndoTest, StmtRollbackSecKey) {
  auto rows = make_rows(20);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 20; k += 2) {
    ASSERT_EQ(update(trx, k, ut::Row{k, 700 + k, "c"}), DB_SUCCESS);
    rows[k] = ut::Row{k, 700 + k, "c"};

    /* The key k + 1 moves onto an existing key: rolled back. */
    EXPECT_EQ(update(trx, k + 1, ut::Row{(k + 3) % 20, 800, "x"}), DB_DUPLICATE_KEY);
  }

  commit(trx);
  expect_rows(rows);
}

/** Uncommitted inserts, updates of the secondary key, primary key moves,
blob changes and deletes at the time of a crash: the recovery rolls them
back. (An instance of its own: READ COMMITTED, disjoint keys.) */
using RowUndoCrashTest = ut::DbTest;

TEST_F(RowUndoCrashTest, Recovery) {
  const auto rows = make_rows(60);

  load(rows);

  crash([this] {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (uint32_t k = 0; k < 60; ++k) {
      switch (k % 5) {
        case 0:
          EXPECT_EQ(update(trx, k, ut::Row{k, 300 + k, "s"}), DB_SUCCESS);
          break;
        case 1:
          EXPECT_EQ(update(trx, k, ut::Row{k + 1000, k, "m"}), DB_SUCCESS);
          break;
        case 2:
          EXPECT_EQ(update(trx, k, ut::Row{k, k, ut::blob(k + 3, 12000)}), DB_SUCCESS);
          break;
        case 3:
          EXPECT_EQ(remove(trx, k), DB_SUCCESS);
          break;
        case 4:
          EXPECT_EQ(insert(trx, ut::Row{k + 2000, k, "i"}), DB_SUCCESS);
          break;
      }
    }

    auto flush = begin(IB_TRX_READ_COMMITTED);

    EXPECT_EQ(insert(flush, ut::Row{5000, 0, "flush"}), DB_SUCCESS);
    commit(flush);
  });

  auto expected = rows;

  expected[5000] = ut::Row{5000, 0, "flush"};
  expect_rows(expected);
}

/** Rollbacks, timed. */
class RowUndoPerf : public SharedDb {};

/** Roll back updates that change the secondary key and move the primary key. */
TEST_F(RowUndoPerf, RollbackKeyChanges) {
  constexpr uint32_t N = 2000;

  load(make_rows(N));

  auto trx = begin();

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(update(trx, k, k % 2 ? ut::Row{k, k + 50000, "s"} : ut::Row{k + 100000, k, "m"}), DB_SUCCESS);
  }

  auto r = ut::bench("rollback_key_change_rows", 1, [&](uint64_t) { rollback(trx); });
  ut::BenchResult rows{"rollback_key_change_row", 1, N, r.m_secs};

  ut::report(rows);

  EXPECT_LT(rows.ns_per_op(), 2000000);
}

}  // namespace

/* --- Performance in depth ----------------------------------------------- */

namespace {

/** Ends the program with the stacks of all its threads if it is not
destroyed within the time given: a hang (a latch or lock never released, a
lost wake up) fails the test instead of blocking the run. */
class Watchdog {
 public:
  explicit Watchdog(std::chrono::seconds secs)
    : m_thread([this, secs] {
        std::unique_lock lock(m_mutex);

        if (!m_cv.wait_for(lock, secs, [this] { return m_done; })) {
          /* Allow gdb, a child, to attach to us (Yama ptrace_scope 1). */
          ::prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY);
          std::fprintf(stderr, "WATCHDOG: no progress in %lds, the stacks:\n", long(secs.count()));

          const auto cmd = std::format("gdb -p {} -batch -ex 'thread apply all bt 30' 1>&2", ::getpid());

          [[maybe_unused]] const auto rc = std::system(cmd.c_str());
          std::abort();
        }
      }) {}

  ~Watchdog() {
    {
      std::lock_guard lock(m_mutex);
      m_done = true;
    }

    m_cv.notify_one();
    m_thread.join();
  }

  Watchdog(const Watchdog &) = delete;
  Watchdog &operator=(const Watchdog &) = delete;

 private:
  std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_done{};
  std::thread m_thread;
};

/** The configuration of the performance tests: as the tests', without the
sync of the log at commit, the tables in the system tablespace (created at
its full size). The disk of the test host takes ~100 ms per sync: a new
tablespace is extended a page at a time for its first 64 pages, each
extension synced (Fil::extend_space_to_desired_size(), even with
SRV_UNIX_NOSYNC), and the numbers would measure the disk. */
Config perf_config(const ut::fs::path &dir) {
  auto config = ut::make_config(dir);

  config.m_fil.m_flush_method = SRV_UNIX_NOSYNC;
  config.m_fil.m_file_per_table = false;
  config.m_log.m_flush_at_trx_commit = 2;

  return config;
}

/** A shared instance for the performance tests below, with perf_config(). */
class RowUndoMtPerf : public SharedDb {
 protected:
  void SetUp() override {
    if (s_dir == nullptr) {
      s_dir = std::make_unique<ut::TestDir>();
    }

    m_config = perf_config(s_dir->m_path);

    if (s_sys == nullptr) {
      start();
    } else {
      m_sys = std::move(s_sys);
    }

    clear();
  }

  /** Commit the rows (k, k % 10, b of len bytes) for k in [base, base + n). */
  void load_n(uint32_t base, uint32_t n, size_t len) {
    auto trx = begin();

    for (uint32_t k = base; k < base + n; ++k) {
      ASSERT_EQ(insert(trx, ut::Row{k, k % 10, std::string(len, 'b')}), DB_SUCCESS) << k;
    }

    commit(trx);
  }
};

/** Rollbacks of one transaction, timed per row: inserts of rows of 50
bytes, 1 KB and 20 KB (b external), updates in place, updates of the
secondary key, deletes. The table is as before each. */
TEST_F(RowUndoMtPerf, Kinds) {
  struct Case {
    const char *m_name;
    uint32_t m_n;
    size_t m_len;
  };

  uint32_t base = 1000000;

  for (const auto &c : {Case{"rollback_insert_50B", 3000, 42}, Case{"rollback_insert_1KB", 1000, 1016},
                        Case{"rollback_insert_20KB", 100, 20000}}) {
    auto trx = begin();

    for (uint32_t k = base; k < base + c.m_n; ++k) {
      ASSERT_EQ(insert(trx, ut::Row{k, k % 10, std::string(c.m_len, 'i')}), DB_SUCCESS);
    }

    auto r = ut::bench(c.m_name, 1, [&](uint64_t) { rollback(trx); });

    ut::report(ut::BenchResult{std::string(c.m_name) + "_row", 1, c.m_n, r.m_secs});
    EXPECT_LT(r.m_secs / c.m_n, 0.02) << c.m_name;
    base += c.m_n;
  }

  expect_rows({});

  constexpr uint32_t N = 3000;

  load_n(0, N, 42);

  for (const char *kind : {"rollback_update_in_place", "rollback_update_sec_key", "rollback_delete"}) {
    auto trx = begin();

    for (uint32_t k = 0; k < N; ++k) {
      DbErr err;

      if (std::string_view(kind) == "rollback_update_in_place") {
        err = update(trx, k, ut::Row{k, k % 10, std::string(42, 'u')});
      } else if (std::string_view(kind) == "rollback_update_sec_key") {
        err = update(trx, k, ut::Row{k, k + 100, std::string(42, 'b')});
      } else {
        err = remove(trx, k);
      }

      ASSERT_EQ(err, DB_SUCCESS) << kind << " " << k;
    }

    auto r = ut::bench(kind, 1, [&](uint64_t) { rollback(trx); });

    ut::report(ut::BenchResult{std::string(kind) + "_row", 1, N, r.m_secs});
    EXPECT_LT(r.m_secs / N, 0.02) << kind;
  }

  ut::Rows rows;

  for (uint32_t k = 0; k < N; ++k) {
    rows[k] = ut::Row{k, k % 10, std::string(42, 'b')};
  }

  expect_rows(rows);
}

/** 1 to 16 transactions, each with updates of the secondary key of its own
rows, rolled back at the same time; then readers scan while more roll
back. The table is as before. */
TEST_F(RowUndoMtPerf, Threads) {
  constexpr uint32_t PER = 300;
  constexpr uint32_t N = 16 * PER;

  load_n(0, N, 42);

  for (int threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;

    for (int t = 0; t < threads; ++t) {
      trxs.push_back(begin());

      for (uint32_t k = t * PER; k < (t + 1) * PER; ++k) {
        ASSERT_EQ(update(trxs.back(), k, ut::Row{k, k + 50000, "moved"}), DB_SUCCESS);
      }
    }

    Watchdog watchdog(std::chrono::seconds(300));

    auto r = ut::bench_mt("rollback_mt_trx", threads, 1, [&](int t, uint64_t) { rollback(trxs[t]); });

    ut::report(ut::BenchResult{"rollback_mt_row", threads, uint64_t(threads) * PER, r.m_secs});
  }

  /* Rollbacks with readers through both indexes. */
  {
    constexpr int T = 4;
    std::vector<Trx *> trxs;
    std::atomic<int> done{};
    std::atomic<uint64_t> scans{};

    for (int t = 0; t < T; ++t) {
      trxs.push_back(begin());

      for (uint32_t k = t; k < N; k += T * 4) {
        ASSERT_EQ(update(trxs.back(), k, ut::Row{k, k + 70000, std::string(300, 'x')}), DB_SUCCESS);
      }
    }

    Watchdog watchdog(std::chrono::seconds(300));
    std::vector<std::thread> readers;

    for (int r = 0; r < T; ++r) {
      readers.emplace_back([&, r] {
        while (done.load() < T) {
          auto trx = begin();
          const auto n = r % 2 == 0 ? scan(trx).size() : scan_sec(trx).size();

          EXPECT_EQ(n, size_t(N));
          commit(trx);
          ++scans;
        }
      });
    }

    std::vector<std::thread> rollers;

    for (int t = 0; t < T; ++t) {
      rollers.emplace_back([&, t] {
        rollback(trxs[t]);
        ++done;
      });
    }

    for (auto &t : rollers) {
      t.join();
    }

    for (auto &t : readers) {
      t.join();
    }

    EXPECT_GT(scans.load(), 0UL);
  }

  ut::Rows rows;

  for (uint32_t k = 0; k < N; ++k) {
    rows[k] = ut::Row{k, k % 10, std::string(42, 'b')};
  }

  expect_rows(rows);
}

}  // namespace
