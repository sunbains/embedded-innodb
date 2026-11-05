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

/** @file unit-tests/test_pread.cc
ParallelReader (row/row0pread.cc): a scan of an index range split in sub
trees (Ctx) run by N threads, with the visibility of each record decided by
the read view of the covering transaction, for the clustered index and for
a secondary index (by the version of the row in the clustered index).

Each scan collects the keys per thread and checks: each row exactly once,
the rows of a serial scan through the api, in order within a context, the
contexts disjoint.

  PreadTest  an instance shared by the suite, the test table emptied (and
             purged) before each test.
  PreadPerf  performance: rows/s at 1..16 threads. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "data0data.h"
#include "dict0dict.h"
#include "row0pread.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0trx.h"

#include <atomic>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <thread>

using namespace std::chrono_literals;

namespace {

/** Rows [from, to) with a = k % 50 and a blob of blob_len bytes. */
ut::Rows make_rows(uint32_t from, uint32_t to, size_t blob_len = 20) {
  ut::Rows rows;

  for (auto k = from; k < to; ++k) {
    rows[k] = ut::Row{k, k % 50, ut::blob(k, blob_len)};
  }

  return rows;
}

/** Run fn in a thread of its own; abort the program if it takes more than
secs: a hang, which gdb can then show. */
void with_timeout(const char *what, std::chrono::seconds secs, const std::function<void()> &fn) {
  auto f = std::async(std::launch::async, fn);

  if (f.wait_for(secs) != std::future_status::ready) {
    std::fprintf(stderr, "%s: still running after %lld s, aborting\n", what, (long long)secs.count());
    std::abort();
  }

  f.get();
}

/** A row read by a parallel scan: its key, its a, and where it was read. */
struct Seen {
  uint32_t m_k{};
  uint32_t m_a{};
  size_t m_ctx{};
  size_t m_scan{};
};

/** What a parallel scan read. */
struct Scan {
  DbErr m_err{DB_ERROR};

  /** All the rows, in no particular order. */
  std::vector<Seen> m_rows;

  /** Per thread id: the number of rows. */
  std::map<size_t, size_t> m_per_thread;

  /** @return the keys read, sorted. */
  std::vector<uint32_t> keys() const {
    std::vector<uint32_t> v;

    for (const auto &s : m_rows) {
      v.push_back(s.m_k);
    }

    std::ranges::sort(v);

    return v;
  }

  /** @return the (a, k) read, sorted. */
  std::vector<std::pair<uint32_t, uint32_t>> pairs() const {
    std::vector<std::pair<uint32_t, uint32_t>> v;

    for (const auto &s : m_rows) {
      v.emplace_back(s.m_a, s.m_k);
    }

    std::ranges::sort(v);

    return v;
  }
};

/** A DbTest whose started instance is shared by the tests of a suite. */
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

    /* The log written at commit, flushed once a second: the commits don't
    wait for the disk. */
    m_config.m_log.m_flush_at_trx_commit = 2;

    if (s_sys == nullptr) {
      start();
    } else {
      m_sys = std::move(s_sys);
    }
  }

  void TearDown() override { s_sys = std::move(m_sys); }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;
};

class PreadTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();
    clear();

    m_heap = mem_heap_create(1024);
  }

  void TearDown() override {
    mem_heap_free(m_heap);

    EXPECT_EQ(m_sys->m_srv->m_n_parallel_read_threads.load(), 0UL) << "threads given back";

    SharedDb::TearDown();
  }

  /** Delete all the rows of the test table, purge them. */
  void clear() {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (const auto &[k, row] : scan(trx)) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
    purge();
  }

  void purge() {
    for (int i = 0; i < 100000 && m_sys->m_trx_sys->m_purge->run() > 0; ++i) {
    }
  }

  Table *table() {
    m_sys->m_dict->mutex_acquire();
    auto table = m_sys->m_dict->table_get(ut::TABLE);
    m_sys->m_dict->mutex_release();

    return table;
  }

  Index *clust() { return table()->get_first_index(); }

  Index *sec() { return clust()->get_next(); }

  /** @return a key tuple on the first field of index, of value v. */
  DTuple *key(Index *index, uint32_t v) {
    auto tuple = dtuple_create(m_heap, 1);
    auto buf = static_cast<byte *>(mem_heap_alloc(m_heap, 4));

    mach_write_to_4(buf, v);
    dfield_set_data(dtuple_get_nth_field(tuple, 0), buf, 4);
    index->copy_types(tuple, 1);
    dtuple_set_n_fields_cmp(tuple, 1);

    return tuple;
  }

  /** @return a key tuple on the first two fields of index, (v1, v2). */
  DTuple *key2(Index *index, uint32_t v1, uint32_t v2) {
    auto tuple = dtuple_create(m_heap, 2);

    for (ulint i = 0; i < 2; ++i) {
      auto buf = static_cast<byte *>(mem_heap_alloc(m_heap, 4));

      mach_write_to_4(buf, i == 0 ? v1 : v2);
      dfield_set_data(dtuple_get_nth_field(tuple, i), buf, 4);
    }

    index->copy_types(tuple, 2);
    dtuple_set_n_fields_cmp(tuple, 2);

    return tuple;
  }

  /** The callback of a scan of index: reads k and a of each row. */
  ParallelReader::F reader_fn(Index *index, std::mutex &mutex, Scan &scan) {
    auto t = table();
    const auto a_pos = index->get_nth_field_pos(t->get_col_no("a"));
    const auto k_pos = index->get_nth_field_pos(t->get_col_no("k"));

    return [&scan, &mutex, a_pos, k_pos](const ParallelReader::Ctx *ctx) -> DbErr {
      ulint len;
      Seen s;

      s.m_k = mach_read_from_4(ctx->m_rec.get_nth_field(ctx->m_offsets, k_pos, &len));
      EXPECT_EQ(len, 4UL);
      s.m_a = mach_read_from_4(ctx->m_rec.get_nth_field(ctx->m_offsets, a_pos, &len));
      EXPECT_EQ(len, 4UL);
      s.m_ctx = ctx->id();
      s.m_scan = ctx->scan_id();

      std::lock_guard g(mutex);

      scan.m_rows.push_back(s);
      ++scan.m_per_thread[ctx->thread_id()];

      return DB_SUCCESS;
    };
  }

  /** Reserve n threads for a reader, as its users must: the reader gives
  max_threads back. */
  void reserve(size_t n) {
    if (n > 0) {
      ASSERT_EQ(ParallelReader::available_threads(*m_sys, n, false), n);
    }
  }

  /** A parallel scan of index by trx with n_threads (0: in this thread). */
  Scan pscan(Trx *trx, Index *index, size_t n_threads, const ParallelReader::ScanRange &range = {}) {
    Scan scan;
    std::mutex mutex;

    reserve(n_threads);

    ParallelReader reader(n_threads);
    ParallelReader::Config config(range, index);

    scan.m_err = reader.add_scan(*m_sys, trx, config, reader_fn(index, mutex, scan));

    if (scan.m_err == DB_SUCCESS) {
      scan.m_err = reader.run(*m_sys, n_threads);
    } else if (n_threads > 0) {
      ParallelReader::release_threads(*m_sys, n_threads);
    }

    return scan;
  }

  /** Check that the rows of each context are in key order, and that the
  contexts are disjoint: the key ranges of two contexts don't overlap. */
  void expect_ctx_order(const Scan &scan, bool sec) {
    std::map<std::pair<size_t, size_t>, std::vector<std::pair<uint32_t, uint32_t>>> per_ctx;

    for (const auto &s : scan.m_rows) {
      per_ctx[{s.m_scan, s.m_ctx}].emplace_back(sec ? s.m_a : s.m_k, s.m_k);
    }

    std::vector<std::pair<std::pair<uint32_t, uint32_t>, std::pair<uint32_t, uint32_t>>> spans;

    for (auto &[id, keys] : per_ctx) {
      /* Rows of one context are read by one thread in order: the vector
      holds them in callback order. */
      EXPECT_TRUE(std::ranges::is_sorted(keys)) << "ctx " << id.second;
      spans.emplace_back(keys.front(), keys.back());
    }

    std::ranges::sort(spans);

    for (size_t i = 1; i < spans.size(); ++i) {
      EXPECT_LT(spans[i - 1].second, spans[i].first) << "contexts overlap";
    }
  }

  /** @return the keys of rows. */
  static std::vector<uint32_t> keys(const ut::Rows &rows) {
    std::vector<uint32_t> v;

    for (const auto &[k, row] : rows) {
      v.push_back(k);
    }

    return v;
  }

  /** @return the (a, k) of rows, sorted. */
  static std::vector<std::pair<uint32_t, uint32_t>> pairs(const ut::Rows &rows) {
    std::vector<std::pair<uint32_t, uint32_t>> v;

    for (const auto &[k, row] : rows) {
      v.emplace_back(row.m_a, k);
    }

    std::ranges::sort(v);

    return v;
  }

  /** Scan both indexes with each thread count, check against rows. */
  void expect_scans(Trx *trx, const ut::Rows &rows, std::initializer_list<size_t> threads = {0, 1, 2, 4, 16}) {
    for (auto n : threads) {
      auto c = pscan(trx, clust(), n);

      EXPECT_EQ(c.m_err, DB_SUCCESS) << n;
      EXPECT_EQ(c.keys(), keys(rows)) << "clustered, threads " << n;
      EXPECT_EQ(c.pairs(), pairs(rows)) << "clustered, threads " << n;
      expect_ctx_order(c, false);

      auto s = pscan(trx, sec(), n);

      EXPECT_EQ(s.m_err, DB_SUCCESS) << n;
      EXPECT_EQ(s.pairs(), pairs(rows)) << "secondary, threads " << n;
      expect_ctx_order(s, true);
    }
  }

  MemHeap *m_heap{};
};

/** An empty index: no rows, no callbacks, success, for any thread count. */
TEST_F(PreadTest, Empty) {
  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
  expect_scans(trx, {});
  commit(trx);
}

/** One row; a few rows on the root page only. */
TEST_F(PreadTest, Small) {
  for (uint32_t n : {1u, 2u, 10u}) {
    clear();

    const auto rows = make_rows(0, n);

    load(rows);

    auto trx = begin();

    ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
    expect_scans(trx, rows);
    commit(trx);
  }
}

/** A tree of several levels: every row exactly once with 0..16 threads, the
same as a serial scan through the api. */
TEST_F(PreadTest, Large) {
  const auto rows = make_rows(0, 10000, 100);

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
  ASSERT_EQ(scan(trx).size(), rows.size());

  expect_scans(trx, rows, {0, 1, 2, 4, 8, 16});

  /* Several threads do get work. */
  auto c = pscan(trx, clust(), 8);

  EXPECT_GT(c.m_per_thread.size(), 1UL);

  commit(trx);
}

/** A snapshot is read as it was: rows updated (secondary keys moved),
deleted and inserted after the view was created, committed, are seen in
their old versions, through both indexes; a new snapshot sees the new rows. */
TEST_F(PreadTest, Snapshot) {
  const auto rows = make_rows(0, 5000, 50);

  load(rows);

  auto reader = begin();

  ASSERT_NE(reader->assign_read_view(*m_sys), nullptr);

  auto now = rows;
  auto writer = begin();

  for (uint32_t k = 0; k < 5000; ++k) {
    switch (k % 5) {
      case 0: {
        /* The secondary key moves. */
        auto row = now[k];

        row.m_a = 1000 + k;
        ASSERT_EQ(update(writer, k, row), DB_SUCCESS);
        now[k] = row;
        break;
      }
      case 1:
        ASSERT_EQ(remove(writer, k), DB_SUCCESS);
        now.erase(k);
        break;
      case 2: {
        /* Not a key column. */
        auto row = now[k];

        row.m_b = ut::blob(k + 1, 70);
        ASSERT_EQ(update(writer, k, row), DB_SUCCESS);
        now[k] = row;
        break;
      }
      default:
        break;
    }
  }

  for (uint32_t k = 10000; k < 11000; ++k) {
    now[k] = ut::Row{k, k % 50, "n"};
    ASSERT_EQ(insert(writer, now[k]), DB_SUCCESS);
  }

  commit(writer);

  expect_scans(reader, rows, {0, 1, 4, 16});
  commit(reader);

  auto fresh = begin();

  ASSERT_NE(fresh->assign_read_view(*m_sys), nullptr);
  expect_scans(fresh, now, {0, 1, 4, 16});
  commit(fresh);
}

/** Changes not committed are invisible to another snapshot, through both
indexes, and to a snapshot taken after them. */
TEST_F(PreadTest, Uncommitted) {
  const auto rows = make_rows(0, 3000, 30);

  load(rows);

  auto writer = begin();

  for (uint32_t k = 0; k < 3000; k += 3) {
    ASSERT_EQ(update(writer, k, ut::Row{k, 5000 + k, "u"}), DB_SUCCESS);
  }

  for (uint32_t k = 1; k < 3000; k += 3) {
    ASSERT_EQ(remove(writer, k), DB_SUCCESS);
  }

  for (uint32_t k = 20000; k < 20500; ++k) {
    ASSERT_EQ(insert(writer, ut::Row{k, 7, "i"}), DB_SUCCESS);
  }

  auto reader = begin();

  ASSERT_NE(reader->assign_read_view(*m_sys), nullptr);
  expect_scans(reader, rows, {0, 2, 8});
  commit(reader);

  rollback(writer);
}

/** A key moved back and forth by committed transactions after the view:
the view sees the original entry once, not the entries of the later
versions (the same (a, k) is in the secondary index again, unmarked). */
TEST_F(PreadTest, SecKeyBackAndForth) {
  const auto rows = make_rows(0, 2000, 30);

  load(rows);

  auto reader = begin();

  ASSERT_NE(reader->assign_read_view(*m_sys), nullptr);

  for (uint32_t i = 0; i < 2; ++i) {
    auto writer = begin();

    for (uint32_t k = 0; k < 2000; k += 2) {
      auto row = rows.at(k);

      row.m_a = i == 0 ? 777 : rows.at(k).m_a;
      ASSERT_EQ(update(writer, k, row), DB_SUCCESS);
    }

    commit(writer);
  }

  /* And some moved away again. */
  auto writer = begin();
  auto now = rows;

  for (uint32_t k = 0; k < 2000; k += 4) {
    now[k].m_a = 888;
    ASSERT_EQ(update(writer, k, now[k]), DB_SUCCESS);
  }

  commit(writer);

  expect_scans(reader, rows, {0, 3, 8});
  commit(reader);

  auto fresh = begin();

  ASSERT_NE(fresh->assign_read_view(*m_sys), nullptr);
  expect_scans(fresh, now, {0, 3, 8});
  commit(fresh);
}

/** Read uncommitted sees the latest versions, uncommitted ones too. */
TEST_F(PreadTest, ReadUncommitted) {
  const auto rows = make_rows(0, 2000, 30);

  load(rows);

  auto writer = begin();
  auto now = rows;

  for (uint32_t k = 0; k < 2000; k += 2) {
    ASSERT_EQ(remove(writer, k), DB_SUCCESS);
    now.erase(k);
  }

  auto reader = begin(IB_TRX_READ_UNCOMMITTED);

  ASSERT_NE(reader->assign_read_view(*m_sys), nullptr);

  auto c = pscan(reader, clust(), 4);

  EXPECT_EQ(c.keys(), keys(now));

  commit(reader);
  rollback(writer);
}

/** The scan of a range [start, end) of the clustered index, for boundaries
before, inside and after the rows, on and between keys: exactly the keys in
the range. */
TEST_F(PreadTest, Ranges) {
  ut::Rows rows;

  /* Even keys only, a tree of several levels. */
  for (uint32_t k = 0; k < 40000; k += 2) {
    rows[k] = ut::Row{k, k % 50, ut::blob(k, 60)};
  }

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  const std::vector<std::pair<std::optional<uint32_t>, std::optional<uint32_t>>> ranges{
    {std::nullopt, std::nullopt},
    {std::nullopt, 0},
    {std::nullopt, 1},
    {std::nullopt, 2},
    {0, std::nullopt},
    {1, std::nullopt},
    {39998, std::nullopt},
    {39999, std::nullopt},
    {50000, std::nullopt},
    {0, 2},
    {100, 100},
    {100, 101},
    {101, 102},
    {200, 100},
    {1234, 30001},
    {1235, 30000},
    {5, 39999},
    {0, 40000},
  };

  for (const auto &[lo, hi] : ranges) {
    ParallelReader::ScanRange range(lo ? key(clust(), *lo) : nullptr, hi ? key(clust(), *hi) : nullptr);
    std::vector<uint32_t> expected;

    for (const auto &[k, row] : rows) {
      if ((!lo || k >= *lo) && (!hi || k < *hi)) {
        expected.push_back(k);
      }
    }

    for (size_t n : {0, 1, 4, 16}) {
      auto c = pscan(trx, clust(), n, range);

      EXPECT_EQ(c.m_err, DB_SUCCESS);
      EXPECT_EQ(c.keys(), expected) << "[" << (lo ? std::to_string(*lo) : "-inf") << ", " << (hi ? std::to_string(*hi) : "+inf")
                                    << ") threads " << n;
    }
  }

  commit(trx);
}

/** A range of the secondary index on full keys: [(a1, k1), (a2, k2)). */
TEST_F(PreadTest, SecRanges) {
  const auto rows = make_rows(0, 20000, 30);

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  using Key = std::pair<uint32_t, uint32_t>;

  for (const auto &[lo, hi] : std::vector<std::pair<Key, Key>>{
         {{0, 0}, {0, 1}}, {{10, 10}, {20, 20}}, {{10, 5010}, {10, 15010}}, {{25, 0}, {26, 0}}, {{49, 19999}, {50, 0}}, {{30, 0}, {10, 0}}}) {
    ParallelReader::ScanRange range(key2(sec(), lo.first, lo.second), key2(sec(), hi.first, hi.second));
    std::vector<Key> expected;

    for (const auto &[k, row] : rows) {
      const Key ak{row.m_a, k};

      if (ak >= lo && ak < hi) {
        expected.push_back(ak);
      }
    }

    std::ranges::sort(expected);

    for (size_t n : {0, 3, 8}) {
      auto s = pscan(trx, sec(), n, range);

      EXPECT_EQ(s.m_err, DB_SUCCESS);
      EXPECT_EQ(s.pairs(), expected) << "[(" << lo.first << ", " << lo.second << "), (" << hi.first << ", " << hi.second
                                     << ")) threads " << n;
    }
  }

  commit(trx);
}

/** BUG: a range of a secondary index on a prefix of its key, [lo, hi) of a:
the rows with a == lo before the last child page whose first entry has a ==
lo are skipped. ScanCtx::create_ranges() and ScanCtx::search()
(row/row0pread.cc:1064, :940) position on the non-leaf pages with
PAGE_CUR_LE: for a key prefix that equals several node pointers, LE picks
the last of them, not the child holding the first entry >= the key
(BtreeCursor::search_to_nth_level() uses PAGE_CUR_L above the leaves for a
PAGE_CUR_GE search). Full keys of a unique index are not affected. */
TEST_F(PreadTest, SecRangesPrefix) {
  const auto rows = make_rows(0, 20000, 30);

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  for (const auto &[lo, hi] : std::vector<std::pair<uint32_t, uint32_t>>{{0, 1}, {0, 50}, {10, 20}, {49, 50}, {25, 26}, {30, 10}}) {
    ParallelReader::ScanRange range(key(sec(), lo), key(sec(), hi));
    std::vector<std::pair<uint32_t, uint32_t>> expected;

    for (const auto &[k, row] : rows) {
      if (row.m_a >= lo && row.m_a < hi) {
        expected.emplace_back(row.m_a, k);
      }
    }

    std::ranges::sort(expected);

    for (size_t n : {0, 3, 8}) {
      auto s = pscan(trx, sec(), n, range);

      EXPECT_EQ(s.m_err, DB_SUCCESS);
      EXPECT_EQ(s.pairs(), expected) << "[" << lo << ", " << hi << ") threads " << n;
    }
  }

  commit(trx);
}

/** A callback that fails stops the scan: run() returns its error, the
other threads stop (no hang), the rows read are few. */
TEST_F(PreadTest, CallbackError) {
  load(make_rows(0, 30000, 50));

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  for (size_t n : {0, 1, 4, 16}) {
    std::atomic<size_t> calls{};

    reserve(n);

    ParallelReader reader(n);
    ParallelReader::Config config({}, clust());

    ASSERT_EQ(reader.add_scan(*m_sys, trx, config,
                              [&](const ParallelReader::Ctx *) -> DbErr { return ++calls == 100 ? DB_CORRUPTION : DB_SUCCESS; }),
              DB_SUCCESS);

    DbErr err{};

    with_timeout("CallbackError", 60s, [&] { err = reader.run(*m_sys, n); });

    EXPECT_EQ(err, DB_CORRUPTION) << n;
    EXPECT_GE(calls.load(), 100UL);
    EXPECT_LT(calls.load(), 30000UL) << "the scan stopped, threads " << n;
  }

  commit(trx);
}

/** The thread, context and page callbacks: a start for each finish, at each
level; a thread start callback that fails stops the scan. */
TEST_F(PreadTest, Callbacks) {
  load(make_rows(0, 8000, 50));

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  for (size_t n : {0, 1, 4, 16}) {
    std::mutex mutex;
    std::map<ParallelReader::State, int> starts;
    std::map<ParallelReader::State, int> finishes;
    std::atomic<size_t> rows{};

    reserve(n);

    ParallelReader reader(n);
    ParallelReader::Config config({}, clust());

    reader.set_start_callback([&](ParallelReader::ThreadCtx *tctx) {
      std::lock_guard g(mutex);
      ++starts[tctx->get_state()];
      return DB_SUCCESS;
    });

    reader.set_finish_callback([&](ParallelReader::ThreadCtx *tctx) {
      std::lock_guard g(mutex);
      ++finishes[tctx->get_state()];
      return DB_SUCCESS;
    });

    ASSERT_EQ(reader.add_scan(*m_sys, trx, config,
                              [&](const ParallelReader::Ctx *) {
                                ++rows;
                                return DB_SUCCESS;
                              }),
              DB_SUCCESS);
    ASSERT_EQ(reader.run(*m_sys, n), DB_SUCCESS);

    EXPECT_EQ(rows.load(), 8000UL);
    EXPECT_EQ(starts[ParallelReader::State::THREAD], int(std::max<size_t>(n, 1))) << n;
    EXPECT_EQ(finishes[ParallelReader::State::THREAD], int(std::max<size_t>(n, 1))) << n;
    EXPECT_EQ(starts[ParallelReader::State::CTX], finishes[ParallelReader::State::CTX]) << n;
    EXPECT_GT(starts[ParallelReader::State::CTX], 0) << n;
    EXPECT_EQ(starts[ParallelReader::State::PAGE], finishes[ParallelReader::State::PAGE]) << n;
    EXPECT_GE(starts[ParallelReader::State::PAGE], int(m_sys->m_btree->get_size(clust(), BTR_N_LEAF_PAGES) / 4)) << n;
  }

  /* A thread start that fails. */
  for (size_t n : {1, 4}) {
    reserve(n);

    ParallelReader reader(n);
    ParallelReader::Config config({}, clust());

    reader.set_start_callback([&](ParallelReader::ThreadCtx *tctx) {
      return tctx->get_state() == ParallelReader::State::THREAD ? DB_OUT_OF_MEMORY : DB_SUCCESS;
    });

    ASSERT_EQ(reader.add_scan(*m_sys, trx, config, [&](const ParallelReader::Ctx *) { return DB_SUCCESS; }), DB_SUCCESS);

    DbErr err{};

    with_timeout("StartError", 60s, [&] { err = reader.run(*m_sys, n); });
    EXPECT_EQ(err, DB_OUT_OF_MEMORY) << n;
  }

  commit(trx);
}

/** Several scans in one reader: both indexes of the table twice, one of
them a range; each scan reads its rows exactly once, scan_id() and
partition_id() tell them apart. */
TEST_F(PreadTest, ManyScans) {
  const auto rows = make_rows(0, 15000, 40);

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  for (size_t n : {0, 2, 8}) {
    std::mutex mutex;
    std::vector<Scan> scans(4);
    std::vector<std::set<size_t>> partitions(4);

    reserve(n);

    ParallelReader reader(n);

    ParallelReader::ScanRange range(key(clust(), 1000), key(clust(), 2000));
    std::vector<ParallelReader::Config> configs{
      ParallelReader::Config({}, clust(), 0, 0),
      ParallelReader::Config({}, sec(), 0, 1),
      ParallelReader::Config(range, clust(), 0, 2),
      ParallelReader::Config({}, sec(), 0, 3),
    };

    for (size_t i = 0; i < configs.size(); ++i) {
      auto fn = reader_fn(configs[i].m_index, mutex, scans[i]);

      ASSERT_EQ(reader.add_scan(*m_sys, trx, configs[i],
                                [&, i, fn](const ParallelReader::Ctx *ctx) {
                                  EXPECT_EQ(ctx->scan_id(), i);
                                  {
                                    std::lock_guard g(mutex);
                                    partitions[i].insert(ctx->partition_id());
                                  }
                                  return fn(ctx);
                                }),
                DB_SUCCESS);
    }

    ASSERT_EQ(reader.run(*m_sys, n), DB_SUCCESS);

    EXPECT_EQ(scans[0].keys(), keys(rows));
    EXPECT_EQ(scans[1].pairs(), pairs(rows));
    EXPECT_EQ(scans[2].keys(), keys(make_rows(1000, 2000)));
    EXPECT_EQ(scans[3].pairs(), pairs(rows));

    for (size_t i = 0; i < 4; ++i) {
      EXPECT_EQ(partitions[i], std::set<size_t>{i});
    }
  }

  commit(trx);
}

/** BUG: a read level above the leaves (Config::m_read_level = 1, the node
pointers of level 1, one per leaf page) asserts in add_scan():
ScanCtx::copy_row() (row/row0pread.cc:537) converts the first record of
each range with row_rec_to_index_entry_low(), which asserts that the record
has all the fields of the index (row/row0row.cc:229); a node pointer has
the n_unique_in_tree key fields and the child page number. The boundary
compare uses only the key: Index::build_data_tuple(rec, n_unique_in_tree)
is enough. */
TEST_F(PreadTest, ReadLevel) {
  load(make_rows(0, 40000, 100));

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

  const auto n_leaves = m_sys->m_btree->get_size(clust(), BTR_N_LEAF_PAGES);

  for (size_t n : {0, 4}) {
    std::atomic<size_t> recs{};

    reserve(n);

    ParallelReader reader(n);
    ParallelReader::Config config({}, clust(), 1);

    ASSERT_EQ(reader.add_scan(*m_sys, trx, config,
                              [&](const ParallelReader::Ctx *) {
                                ++recs;
                                return DB_SUCCESS;
                              }),
              DB_SUCCESS);
    ASSERT_EQ(reader.run(*m_sys, n), DB_SUCCESS);

    /* The leaf segment's used pages include none but leaves here (no blobs). */
    EXPECT_EQ(recs.load(), n_leaves) << n;
  }

  commit(trx);
}

/** An interrupted transaction: the scan stops with DB_INTERRUPTED. */
TEST_F(PreadTest, Interrupted) {
  load(make_rows(0, 5000, 20));

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
  trx->interrupt();

  for (size_t n : {0, 4}) {
    auto c = pscan(trx, clust(), n);

    EXPECT_EQ(c.m_err, DB_INTERRUPTED) << n;
  }

  commit(trx);
}

/** Parallel scans of the clustered index under snapshots while writers
change other rows (page splits and merges, so the index tree latch has
waiters and the scans save and restore their position on the block
boundaries) and purge runs: each snapshot reads its rows exactly once, the
same every time. See ConcurrentWriters for the secondary index. */
TEST_F(PreadTest, ConcurrentWritersClust) {
  const auto rows = make_rows(0, 8000, 100);

  load(rows);

  std::atomic<bool> stop{};
  std::atomic<size_t> n_scans{};

  with_timeout("ConcurrentWritersClust", 170s, [&] {
    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < 3; ++w) {
      threads.emplace_back([&, w] {
        uint32_t round{};

        while (!stop.load()) {
          auto trx = begin();
          const uint32_t base = 100000 + w * 100000;

          for (uint32_t i = 0; i < 300; ++i) {
            const auto k = base + i;

            if (round % 2 == 0) {
              EXPECT_EQ(insert(trx, ut::Row{k, i % 50, ut::blob(k, 300)}), DB_SUCCESS);
            } else {
              EXPECT_EQ(remove(trx, k), DB_SUCCESS);
            }
          }

          /* Move the secondary keys of some of the snapshot's rows. */
          for (uint32_t k = w; k < 8000; k += 997) {
            EXPECT_EQ(update(trx, k, ut::Row{k, (round + k) % 50, rows.at(k).m_b}), DB_SUCCESS);
          }

          commit(trx);
          ++round;
        }
      });
    }

    threads.emplace_back([&] {
      while (!stop.load()) {
        purge();
        std::this_thread::sleep_for(1ms);
      }
    });

    for (int i = 0; i < 6; ++i) {
      auto trx = begin();

      ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

      auto first = pscan(trx, clust(), 4);

      EXPECT_EQ(first.m_err, DB_SUCCESS);

      for (size_t n : {1, 8, 16}) {
        auto c = pscan(trx, clust(), n);

        EXPECT_EQ(c.m_err, DB_SUCCESS);

        auto ck = c.keys();

        EXPECT_TRUE(std::ranges::adjacent_find(ck) == ck.end()) << "a row twice";
        EXPECT_EQ(c.pairs(), first.pairs()) << "the snapshot is stable";
      }

      commit(trx);
      ++n_scans;
    }

    stop = true;

    for (auto &t : threads) {
      t.join();
    }
  });

  EXPECT_EQ(n_scans.load(), 6UL);
}

/** BUG (a hang): parallel scans of the secondary index under snapshots while
writers insert and update. ScanCtx::sec_rec_visible() (row/row0pread.cc:467)
looks the row up in the clustered index in the scan's mtr: pcur.close()
releases nothing, the clustered leaf stays S latched until the scan leaves
the secondary page (in fact until the mtr commits). A later lookup of the
same clustered page S latches it again; if a writer is waiting for its X
latch meanwhile, the second S request waits behind the writer, which waits
for the first: the scan threads, the writers (and the page flusher, waiting
for the secondary page the scan holds) hang. Seen here within seconds: two
scan threads in rw_lock_s_lock_spin() from sec_rec_visible() on a
clustered leaf, a writer in rw_lock_x_lock_wait() on it from
RowInsert::index_entry_low(), two writers and BufFlush::page() waiting for
the secondary leaf. The lookup needs a mini-transaction of its own,
committed before the next record. */
TEST_F(PreadTest, ConcurrentWriters) {
  const auto rows = make_rows(0, 8000, 100);

  load(rows);

  std::atomic<bool> stop{};
  std::atomic<size_t> n_scans{};

  with_timeout("ConcurrentWriters", 170s, [&] {
    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < 3; ++w) {
      threads.emplace_back([&, w] {
        uint32_t round{};

        while (!stop.load()) {
          auto trx = begin();
          const uint32_t base = 100000 + w * 100000;

          for (uint32_t i = 0; i < 300; ++i) {
            const auto k = base + i;

            if (round % 2 == 0) {
              EXPECT_EQ(insert(trx, ut::Row{k, i % 50, ut::blob(k, 300)}), DB_SUCCESS);
            } else {
              EXPECT_EQ(remove(trx, k), DB_SUCCESS);
            }
          }

          /* Move the secondary keys of some of the snapshot's rows. */
          for (uint32_t k = w; k < 8000; k += 997) {
            EXPECT_EQ(update(trx, k, ut::Row{k, (round + k) % 50, rows.at(k).m_b}), DB_SUCCESS);
          }

          commit(trx);
          ++round;
        }
      });
    }

    threads.emplace_back([&] {
      while (!stop.load()) {
        purge();
        std::this_thread::sleep_for(1ms);
      }
    });

    for (int i = 0; i < 6; ++i) {
      auto trx = begin();

      ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

      auto first = pscan(trx, sec(), 4);

      EXPECT_EQ(first.m_err, DB_SUCCESS);

      for (size_t n : {1, 8, 16}) {
        auto c = pscan(trx, clust(), n);
        auto s = pscan(trx, sec(), n);

        EXPECT_EQ(c.m_err, DB_SUCCESS);
        EXPECT_EQ(s.m_err, DB_SUCCESS);

        auto ck = c.keys();
        auto sk = s.keys();

        EXPECT_TRUE(std::ranges::adjacent_find(ck) == ck.end()) << "a row twice";
        EXPECT_EQ(ck, sk) << "both indexes, one snapshot";
        EXPECT_EQ(s.pairs(), first.pairs()) << "the snapshot is stable";
        EXPECT_EQ(c.pairs(), first.pairs());
      }

      commit(trx);
      ++n_scans;
    }

    stop = true;

    for (auto &t : threads) {
      t.join();
    }
  });

  EXPECT_EQ(n_scans.load(), 6UL);
}

/** BUG: a parallel scan of a clustered index whose rows have externally
stored columns asserts in a debug build: ScanCtx::check_visibility()
(row/row0pread.cc:451) has ut_ad(... || !rec_offs_any_extern(offsets)),
where MySQL (the origin of this code) asserts
!rec_offs_any_null_extern(): a null BLOB pointer (a blob being written),
not any externally stored column. Any table with a blob of more than ~8 KB
stops the scan. */
TEST_F(PreadTest, ExternFields) {
  auto rows = make_rows(0, 200, 30);

  for (uint32_t k = 0; k < 200; k += 10) {
    rows[k].m_b = ut::blob(k, 20000);
  }

  load(rows);

  auto trx = begin();

  ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
  expect_scans(trx, rows, {0, 4});
  commit(trx);
}

/** BUG: api::parallel_select_count_star() (api/api0api.cc:4244-4263) builds
the reader for n_threads, then asks available_threads() for them: when
fewer are available, ParallelReader::run() gives back the max_threads of the
reader (spawn() releases max_threads - n, run() the n used): threads it
never got. The counter of the instance's parallel read threads is left
short, the next release_threads() asserts ut_a(active >= n_threads) (or,
with none available, run(0) releases all of them at once). MySQL asks
available_threads() first and builds the reader with the answer. */
TEST_F(PreadTest, ThreadAccounting) {
  load(make_rows(0, 1000, 10));

  auto &active = m_sys->m_srv->m_n_parallel_read_threads;

  /* Others use all but 2 threads. */
  const auto others = ParallelReader::MAX_THREADS - 2;

  ASSERT_EQ(ParallelReader::available_threads(*m_sys, others, false), others);

  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IS), DB_SUCCESS);

  std::vector<api::Cursor *> crsrs{crsr};
  uint64_t n_rows{};

  EXPECT_EQ(api::parallel_select_count_star(*m_sys, trx, crsrs, 8, n_rows), DB_SUCCESS);
  EXPECT_EQ(n_rows, 1000UL);

  /* The scan gave back what it took: the others' threads are still counted. */
  EXPECT_EQ(active.load(), others);

  EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  commit(trx);

  /* Leave the counter as it was, whatever happened. */
  active.store(0);
}

/** Parallel scans, timed. */
class PreadPerf : public PreadTest {
 protected:
  /** Time scans of index with 1..16 threads, n_rows each. */
  void scans(Index *index, const char *what, size_t n_rows) {
    auto trx = begin();

    ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);

    for (size_t n : {1, 2, 4, 8, 16}) {
      std::atomic<size_t> rows{};
      constexpr int ROUNDS = 3;

      const auto start = std::chrono::steady_clock::now();

      for (int i = 0; i < ROUNDS; ++i) {
        reserve(n);

        ParallelReader reader(n);
        ParallelReader::Config config({}, index);

        ASSERT_EQ(reader.add_scan(*m_sys, trx, config,
                                  [&](const ParallelReader::Ctx *) {
                                    rows.fetch_add(1, std::memory_order_relaxed);
                                    return DB_SUCCESS;
                                  }),
                  DB_SUCCESS);
        ASSERT_EQ(reader.run(*m_sys, n), DB_SUCCESS);
      }

      const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
      ut::BenchResult r{std::format("pscan_{}", what), int(n), rows.load(), secs.count()};

      ut::report(r);

      EXPECT_EQ(rows.load(), ROUNDS * n_rows);
      EXPECT_LT(r.ns_per_op(), 100000);
    }

    commit(trx);
  }
};

/** Rows per second of a parallel scan of the clustered index. */
/** A secondary scan right after the commit that wrote the pages, and the same scan
once the watermark has passed that commit: below it the page id decides, above it
each record goes to the clustered index. */
TEST_F(PreadPerf, SecAfterCommit) {
  constexpr uint32_t N = 50000;

  load(make_rows(0, N, 20));

  const auto one = [&](const char *name) {
    const auto start = std::chrono::steady_clock::now();
    auto trx = begin();

    ASSERT_NE(trx->assign_read_view(*m_sys), nullptr);
    EXPECT_EQ(pscan(trx, sec(), 1).m_rows.size(), N);
    commit(trx);

    const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;

    ut::report(ut::BenchResult{name, 1, N, secs.count()});
  };

  one("sec_scan_after_commit");

  /* The master thread sweeps once a second. */
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  one("sec_scan_settled");
}

TEST_F(PreadPerf, Clustered) {
  constexpr uint32_t N = 200000;

  load(make_rows(0, N, 20));
  scans(clust(), "clustered", N);
}

/** Rows per second of a parallel scan of the secondary index. */
TEST_F(PreadPerf, Secondary) {
  constexpr uint32_t N = 200000;

  load(make_rows(0, N, 20));
  scans(sec(), "secondary", N);
}

/** A secondary index scan under a snapshot that has to look up the
clustered index for every record (the rows were deleted after it). */
TEST_F(PreadPerf, SecondarySnapshot) {
  constexpr uint32_t N = 100000;

  load(make_rows(0, N, 20));

  auto reader = begin();

  ASSERT_NE(reader->assign_read_view(*m_sys), nullptr);

  auto writer = begin();

  for (uint32_t k = 0; k < N; k += 2) {
    ASSERT_EQ(remove(writer, k), DB_SUCCESS);
  }

  commit(writer);

  for (size_t n : {1, 4, 16}) {
    auto start = std::chrono::steady_clock::now();
    auto s = pscan(reader, sec(), n);
    const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
    ut::BenchResult r{"pscan_secondary_snapshot", int(n), s.m_rows.size(), secs.count()};

    ut::report(r);
    EXPECT_EQ(s.m_rows.size(), N);
    EXPECT_LT(r.ns_per_op(), 1000000);
  }

  commit(reader);
}

}  // namespace
