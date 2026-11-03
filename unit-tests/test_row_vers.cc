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

/** @file unit-tests/test_row_vers.cc
RowVers: building the old versions of a row for a consistent read, through
the clustered and the secondary index, and the implicit locks found from
the versions. The tests share a started instance; each starts with an
empty table, with keys of its own where the delete marked rows of the
tests before it could be in the way. */

#include "ut0env.h"
#include "ut0perf.h"

#include "trx0sys.h"
#include "trx0trx.h"

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
#include <thread>

using namespace std::chrono_literals;

namespace {

ut::Rows make_rows(uint32_t n, uint32_t base = 0) {
  ut::Rows rows;

  for (uint32_t k = base; k < base + n; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 80)};
  }

  return rows;
}

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

  /** Expect trx to see rows through both indexes. */
  void expect_view(Trx *trx, const ut::Rows &rows) {
    EXPECT_EQ(scan(trx), rows);

    const auto sec = scan_sec(trx);

    EXPECT_EQ(sec.size(), rows.size());

    for (const auto &row : sec) {
      auto it = rows.find(row.m_k);

      if (it == rows.end()) {
        ADD_FAILURE() << "SEC_A gives k " << row.m_k << " not in the view";
      } else {
        EXPECT_EQ(row, it->second) << "SEC_A k " << row.m_k;
      }
    }
  }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;
};

using RowVersTest = SharedDb;

/** A reader sees the rows as they were when its read view was created,
after a writer updated one column. The old version was built with the
update vector indexed by the column's position instead of searched by
field number: an update of fewer fields than the position asserted. */
TEST_F(RowVersTest, OneColumn) {
  const auto rows = make_rows(30);

  load(rows);

  auto reader = begin();

  ASSERT_EQ(scan(reader), rows);

  auto writer = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_b = ut::blob(k + 1000, k % 2 == 0 ? 50 : 30000);
    ASSERT_EQ(update(writer, k, r), DB_SUCCESS);
  }

  commit(writer);

  EXPECT_EQ(scan(reader), rows);
  EXPECT_EQ(scan_sec(reader).size(), rows.size());
  commit(reader);
}

/** The secondary key changed: through SEC_A the reader finds the rows by
their old value and not by the new. */
TEST_F(RowVersTest, SecKey) {
  const auto rows = make_rows(30);

  load(rows);

  auto reader = begin();

  ASSERT_EQ(scan(reader).size(), rows.size());

  auto writer = begin();

  for (const auto &[k, row] : rows) {
    auto r = row;

    r.m_a = 100 + k;
    ASSERT_EQ(update(writer, k, r), DB_SUCCESS);
  }

  commit(writer);

  for (const auto &[k, row] : rows) {
    auto found = find_sec(reader, k);

    ASSERT_TRUE(found.has_value()) << k;
    EXPECT_EQ(*found, row);
  }

  EXPECT_EQ(scan(reader), rows);

  for (uint32_t a = 130; a < 140; ++a) {
    EXPECT_FALSE(find_sec(reader, a).has_value()) << a;
  }

  commit(reader);

  auto now = begin();

  EXPECT_EQ(find_sec(now, 105)->m_k, 5u);
  commit(now);
}

/** Several committed versions: each reader sees its own. */
TEST_F(RowVersTest, Chain) {
  const auto rows = make_rows(10);

  load(rows);

  std::vector<std::pair<Trx *, ut::Rows>> readers;
  auto current = rows;

  for (uint32_t v = 1; v <= 5; ++v) {
    auto reader = begin();

    ASSERT_EQ(scan(reader), current);
    readers.emplace_back(reader, current);

    auto writer = begin();

    for (auto &[k, row] : current) {
      row.m_a = v * 100 + k;
      row.m_b = ut::blob(k * 10 + v, v % 2 == 0 ? 15000 : 20);
      ASSERT_EQ(update(writer, k, row), DB_SUCCESS);
    }

    commit(writer);
  }

  for (auto &[reader, seen] : readers) {
    EXPECT_EQ(scan(reader), seen);
    commit(reader);
  }

  expect_rows(current);
}

/** Deleted and inserted rows: the reader sees the deleted ones, not the
new ones. */
TEST_F(RowVersTest, DeleteInsert) {
  const auto rows = make_rows(20);

  load(rows);

  auto reader = begin();

  ASSERT_EQ(scan(reader), rows);

  auto writer = begin();

  for (uint32_t k = 0; k < 20; k += 2) {
    ASSERT_EQ(remove(writer, k), DB_SUCCESS);
  }

  for (uint32_t k = 100; k < 110; ++k) {
    ASSERT_EQ(insert(writer, ut::Row{k, 1, "new"}), DB_SUCCESS);
  }

  commit(writer);

  EXPECT_EQ(scan(reader), rows);
  EXPECT_EQ(scan_sec(reader).size(), rows.size());
  commit(reader);
}

/** Changes not committed are not seen by others, whatever their kind;
the writer sees its own. */
TEST_F(RowVersTest, Uncommitted) {
  const auto rows = make_rows(10, 200);

  load(rows);

  auto writer = begin();

  ASSERT_EQ(update(writer, 200, ut::Row{200, 1000, "u"}), DB_SUCCESS);
  ASSERT_EQ(remove(writer, 201), DB_SUCCESS);
  ASSERT_EQ(insert(writer, ut::Row{290, 290, "i"}), DB_SUCCESS);
  ASSERT_EQ(update(writer, 202, ut::Row{295, 202, "moved"}), DB_SUCCESS);

  auto reader = begin();

  expect_view(reader, rows);

  auto own = rows;

  own[200] = ut::Row{200, 1000, "u"};
  own.erase(201);
  own[290] = ut::Row{290, 290, "i"};
  own.erase(202);
  own[295] = ut::Row{295, 202, "moved"};

  expect_view(writer, own);

  rollback(writer);

  expect_view(reader, rows);
  commit(reader);
}

/** A primary key that moves and moves back, committed each time: each
reader sees the key of its time and the row only once. */
TEST_F(RowVersTest, PkMoves) {
  load({{300, ut::Row{300, 1, "a"}}});

  std::vector<std::pair<Trx *, ut::Rows>> readers;
  uint32_t k = 300;

  for (uint32_t to : {301u, 302u, 300u, 303u}) {
    auto reader = begin();
    auto seen = scan(reader);

    ASSERT_EQ(seen.size(), 1u);
    readers.emplace_back(reader, seen);

    auto writer = begin();

    ASSERT_EQ(update(writer, k, ut::Row{to, to, "m"}), DB_SUCCESS);
    commit(writer);
    k = to;
  }

  for (auto &[reader, seen] : readers) {
    expect_view(reader, seen);
    commit(reader);
  }

  expect_rows({{303, ut::Row{303, 303, "m"}}});
}

/** A blob going in and out of the page over versions: each version is
rebuilt with its own blob. */
TEST_F(RowVersTest, BlobVersions) {
  load({{400, ut::Row{400, 1, ut::blob(1, 10)}}});

  std::vector<std::pair<Trx *, ut::Rows>> readers;

  for (uint32_t v = 2; v < 8; ++v) {
    auto reader = begin();

    readers.emplace_back(reader, scan(reader));

    auto writer = begin();
    const size_t len = v % 2 == 0 ? 20000 + v : 7 * v;

    ASSERT_EQ(update(writer, 400, ut::Row{400, 1, ut::blob(v, len)}), DB_SUCCESS);
    commit(writer);
  }

  for (auto &[reader, seen] : readers) {
    EXPECT_EQ(scan(reader), seen);
    commit(reader);
  }
}

/** The secondary key changes A, B, A: a reader of each version finds the row
through SEC_A by its value then, once; a value of another version finds
nothing (the delete marked entries of the other versions are skipped). */
TEST_F(RowVersTest, SecKeyAba) {
  load({{500, ut::Row{500, 10, "x"}}});

  auto r1 = begin();

  ASSERT_EQ(find_sec(r1, 10)->m_a, 10u);

  auto w1 = begin();
  ASSERT_EQ(update(w1, 500, ut::Row{500, 20, "x"}), DB_SUCCESS);
  commit(w1);

  auto r2 = begin();

  ASSERT_EQ(find_sec(r2, 20)->m_a, 20u);

  auto w2 = begin();
  ASSERT_EQ(update(w2, 500, ut::Row{500, 10, "y"}), DB_SUCCESS);
  commit(w2);

  auto r3 = begin();

  EXPECT_EQ(*find_sec(r1, 10), (ut::Row{500, 10, "x"}));
  EXPECT_FALSE(find_sec(r1, 20).has_value());
  EXPECT_EQ(scan_sec(r1).size(), 1u);

  EXPECT_EQ(*find_sec(r2, 20), (ut::Row{500, 20, "x"}));
  EXPECT_FALSE(find_sec(r2, 10).has_value());
  EXPECT_EQ(scan_sec(r2).size(), 1u);

  EXPECT_EQ(*find_sec(r3, 10), (ut::Row{500, 10, "y"}));
  EXPECT_FALSE(find_sec(r3, 20).has_value());
  EXPECT_EQ(scan_sec(r3).size(), 1u);

  commit(r1);
  commit(r2);
  commit(r3);
}

/** A row deleted and a new one with the same key inserted later: the old
reader sees the old row. */
TEST_F(RowVersTest, DeleteReinsert) {
  load({{600, ut::Row{600, 1, "old"}}});

  auto reader = begin();

  ASSERT_EQ(scan(reader).size(), 1u);

  auto del = begin();
  ASSERT_EQ(remove(del, 600), DB_SUCCESS);
  commit(del);

  auto mid = begin();

  EXPECT_TRUE(scan(mid).empty());

  auto ins = begin();
  ASSERT_EQ(insert(ins, ut::Row{600, 2, "new"}), DB_SUCCESS);
  commit(ins);

  expect_view(reader, {{600, ut::Row{600, 1, "old"}}});
  expect_view(mid, {});

  commit(reader);
  commit(mid);

  expect_rows({{600, ut::Row{600, 2, "new"}}});
}

/** A long chain of versions of one row: 30 readers, each between two
commits, see each its version. */
TEST_F(RowVersTest, LongChain) {
  load({{700, ut::Row{700, 0, ""}}});

  std::vector<Trx *> readers;

  for (uint32_t v = 1; v <= 30; ++v) {
    readers.push_back(begin());
    ASSERT_EQ(scan(readers.back()).size(), 1u);

    auto writer = begin();
    ASSERT_EQ(update(writer, 700, ut::Row{700, v, std::to_string(v)}), DB_SUCCESS);
    commit(writer);
  }

  for (uint32_t v = 0; v < 30; ++v) {
    const ut::Row expected{700, v, v == 0 ? "" : std::to_string(v)};

    EXPECT_EQ(scan(readers[v]), (ut::Rows{{700, expected}})) << v;
    EXPECT_EQ(find_sec(readers[v], v), expected) << v;
    commit(readers[v]);
  }
}

/** Implicit locks through the versions: a writer that inserted a row and
updated a column of another, twice, with records of different lengths,
holds the row; a locking read through SEC_A waits for it and gets it at
its commit. RowVers::impl_x_locked_off_trx_sys_mutex() walks the versions
of the clustered record for the secondary record.

BUG: in impl_x_locked_off_trx_sys_mutex() the loop declares a new
`ulint *clust_offsets` (row/row0vers.cc:160) that shadows the one passed to
prev_version_build() at :136; from the second version on, that one still
holds the offsets of clust_rec, in heap2, freed at :138: the debug build
asserts in Rec::offs_validate() (rem0rec.cc:850) under prev_version_build(),
a release build reads freed memory and the wrong offsets. Upstream assigned
the same variable. */
TEST_F(RowVersTest, ImplicitLockSec) {
  load({{800, ut::Row{800, 80, "x"}}});

  auto writer = begin();

  ASSERT_EQ(insert(writer, ut::Row{801, 81, "touch the SEC_A page"}), DB_SUCCESS);
  ASSERT_EQ(update(writer, 800, ut::Row{800, 80, ut::blob(1, 30)}), DB_SUCCESS);
  ASSERT_EQ(update(writer, 800, ut::Row{800, 80, ut::blob(2, 300)}), DB_SUCCESS);

  auto reader = begin();

  auto result = std::async(std::launch::async, [&]() -> DbErr {
    api::Cursor *crsr{};
    api::Cursor *sec{};

    if (auto err = api::cursor_open_table(*m_sys, ut::TABLE, reader, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->open_index(*m_sys, ut::SEC_A, &sec);

    if (err == DB_SUCCESS) {
      sec->set_cluster_access();
      err = sec->set_lock_mode(*m_sys, IB_LOCK_S);

      if (err == DB_SUCCESS) {
        auto key = sec->sec_search_tuple_create();
        int res{};

        err = key->write_u32(0, 80);

        if (err == DB_SUCCESS) {
          err = sec->moveto(*m_sys, key, IB_CUR_GE, &res);
        }

        api::Tuple::destroy(key);
      }

      (void)api::Cursor::close(*m_sys, sec);
    }

    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  });

  /* The reader waits for the writer. */
  bool waited{};
  const auto end = std::chrono::steady_clock::now() + 10s;

  while (!waited && std::chrono::steady_clock::now() < end) {
    /* The lock wait state: the trx mutex's. */
    trx_mutex_enter(reader);
    waited = reader->m_que_state == TRX_QUE_LOCK_WAIT;
    trx_mutex_exit(reader);

    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(waited);

  commit(writer);

  ASSERT_EQ(result.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(result.get(), DB_SUCCESS);
  commit(reader);
}

/** Old versions for many readers of many rows, timed. */
class RowVersPerf : public SharedDb {};

/** A reader scans rows that have each V newer committed versions: every
row read builds V old versions. */
TEST_F(RowVersPerf, BuildVersions) {
  constexpr uint32_t N = 1000;
  constexpr uint32_t V = 10;

  load(make_rows(N, 0));

  auto reader = begin();

  ASSERT_EQ(scan(reader).size(), N);

  for (uint32_t v = 1; v <= V; ++v) {
    auto writer = begin();

    for (uint32_t k = 0; k < N; ++k) {
      ASSERT_EQ(update(writer, k, ut::Row{k, k + v, "v"}), DB_SUCCESS);
    }

    commit(writer);
  }

  auto r = ut::bench("build_old_versions", 1, [&](uint64_t) { EXPECT_EQ(scan(reader).size(), N); });

  ut::BenchResult versions{"old_versions_built", 1, uint64_t(N) * V, r.m_secs};

  ut::report(versions);

  EXPECT_LT(versions.ns_per_op(), 1000000);

  commit(reader);
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

          (void)std::system(cmd.c_str());
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
class RowVersMtPerf : public SharedDb {
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

/** A reader through SEC_A of rows each with V newer committed versions that
changed the secondary key: each row read checks the clustered record and
builds its old versions. */
TEST_F(RowVersMtPerf, SecVersions) {
  constexpr uint32_t N = 500;
  constexpr uint32_t V = 10;

  load_n(0, N, 42);

  auto reader = begin();

  ASSERT_EQ(scan(reader).size(), N);

  for (uint32_t v = 1; v <= V; ++v) {
    auto writer = begin();

    for (uint32_t k = 0; k < N; ++k) {
      ASSERT_EQ(update(writer, k, ut::Row{k, k % 10 + v * 1000, std::string(42, 'b')}), DB_SUCCESS);
    }

    commit(writer);
  }

  std::vector<ut::Row> seen;

  auto r = ut::bench("sec_old_versions_scan", 1, [&](uint64_t) { seen = scan_sec(reader); });

  ut::report(ut::BenchResult{"sec_old_version_built", 1, uint64_t(N) * V, r.m_secs});
  ASSERT_EQ(seen.size(), N);

  for (const auto &row : seen) {
    EXPECT_EQ(row.m_a, row.m_k % 10) << row.m_k;
  }

  EXPECT_LT(r.m_secs / (N * V), 0.01);
  commit(reader);
}

/** 1 to 16 readers, each with a read view older than V versions of every
row, scanning at the same time. */
TEST_F(RowVersMtPerf, ReaderThreads) {
  constexpr uint32_t N = 500;
  constexpr uint32_t V = 5;

  load_n(0, N, 42);

  std::vector<Trx *> readers;

  for (int t = 0; t < 16; ++t) {
    readers.push_back(begin());
    ASSERT_EQ(scan(readers.back()).size(), N);
  }

  for (uint32_t v = 1; v <= V; ++v) {
    auto writer = begin();

    for (uint32_t k = 0; k < N; ++k) {
      ASSERT_EQ(update(writer, k, ut::Row{k, k % 10, std::string(42, char('b' + v))}), DB_SUCCESS);
    }

    commit(writer);
  }

  for (int threads : {1, 2, 4, 8, 16}) {
    Watchdog watchdog(std::chrono::seconds(300));

    auto r = ut::bench_mt("old_version_scan_mt", threads, 1, [&](int t, uint64_t) {
      const auto rows = scan(readers[t]);

      EXPECT_EQ(rows.size(), N);

      for (const auto &[k, row] : rows) {
        EXPECT_EQ(row.m_b, std::string(42, 'b')) << k;
      }
    });

    ut::report(ut::BenchResult{"old_version_built_mt", threads, uint64_t(threads) * N * V, r.m_secs});
  }

  for (auto t : readers) {
    commit(t);
  }
}

/** A reader of rows of 20 KB (b external) each with V newer versions of b:
the old versions of b are read from the undo log and their BLOBs. */
TEST_F(RowVersMtPerf, BlobVersions) {
  constexpr uint32_t N = 50;
  constexpr uint32_t V = 5;

  auto trx = begin();

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(insert(trx, ut::Row{k, k, ut::blob(k, 20000)}), DB_SUCCESS);
  }

  commit(trx);

  auto reader = begin();

  ASSERT_EQ(scan(reader).size(), N);

  for (uint32_t v = 1; v <= V; ++v) {
    auto writer = begin();

    for (uint32_t k = 0; k < N; ++k) {
      ASSERT_EQ(update(writer, k, ut::Row{k, k, ut::blob(k + v * 1000, 20000)}), DB_SUCCESS);
    }

    commit(writer);
  }

  ut::Rows rows;

  auto r = ut::bench("blob_old_versions_scan", 1, [&](uint64_t) { rows = scan(reader); });

  ut::report(ut::BenchResult{"blob_old_version_built", 1, uint64_t(N) * V, r.m_secs});
  ASSERT_EQ(rows.size(), N);

  for (const auto &[k, row] : rows) {
    EXPECT_EQ(row.m_b, ut::blob(k, 20000)) << k;
  }

  commit(reader);
}

}  // namespace
