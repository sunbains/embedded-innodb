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

/** @file unit-tests/test_mvperf.cc
Transactions and read views, timed: the same benches compile against the commit
before the clock, for a comparison. Short names, one CSV per run (ut0perf.h). */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "dict0dict.h"
#include "read0read.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0logger.h"

#include <gtest/gtest.h>

#include <atomic>
#include <format>
#include <thread>
#include <vector>

namespace {

[[maybe_unused]] const bool quiet = [] {
  logger::level = logger::Warn;
  return true;
}();

constexpr int THREADS[] = {1, 2, 4, 8, 16};

class MvccPerf : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();
    m_ts = m_sys->m_trx_sys.get();
  }

  /** Bg transactions: active, to fill the list, n of them. */
  std::vector<Trx *> actives(size_t n) {
    std::vector<Trx *> v;

    for (size_t i{}; i < n; ++i) {
      v.push_back(begin());
    }

    return v;
  }

  void finish(std::vector<Trx *> &v) {
    for (auto t : v) {
      commit(t);
    }

    v.clear();
  }

  TrxSys *m_ts{};
};

/** A begin and a commit of a transaction that changes nothing, trx object reused. */
TEST_F(MvccPerf, BeginCommit) {
  for (int n : THREADS) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      trxs.push_back(m_ts->create_user_trx(nullptr));
    }

    auto r = ut::bench_mt(std::format("begin_commit_t{}", n), n, 200000 / uint64_t(n), [&](int t, uint64_t) {
      auto ok = trxs[t]->start(*m_sys, ULINT_UNDEFINED);

      ut::keep(ok);
      (void)trxs[t]->commit(*m_sys);
    });

    for (auto t : trxs) {
      m_ts->destroy_user_trx(t);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** The API's begin and release: with the trx object, as a client does it. */
TEST_F(MvccPerf, BeginCommitObj) {
  for (int n : THREADS) {
    auto r = ut::bench_mt(std::format("begin_commit_obj_t{}", n), n, 100000 / uint64_t(n), [&](int, uint64_t) {
      auto trx = m_ts->create_user_trx(nullptr);
      auto ok = trx->start(*m_sys, ULINT_UNDEFINED);

      ut::keep(ok);
      (void)trx->commit(*m_sys);
      m_ts->destroy_user_trx(trx);
    });

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** Open and close a view, read committed style, with n others active. */
TEST_F(MvccPerf, ViewOpen) {
  for (size_t active : {size_t(0), size_t(100), size_t(1000), size_t(10000)}) {
    auto others = actives(active);

    for (int n : THREADS) {
      std::vector<Trx *> trxs;

      for (int t{}; t < n; ++t) {
        trxs.push_back(begin());
      }

      auto r = ut::bench_mt(std::format("view_open_a{}_t{}", active, n), n, 200000 / uint64_t(n), [&](int t, uint64_t) {
        ut::keep(trxs[t]->assign_read_view(*m_sys));
        m_ts->close_read_view_for_read_committed(trxs[t]);
      });

      for (auto t : trxs) {
        commit(t);
      }

      EXPECT_LT(r.ns_per_op(), 1e8);
    }

    finish(others);
  }
}

/** Begin, a view, commit, in one loop. */
TEST_F(MvccPerf, Mixed) {
  for (int n : THREADS) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      trxs.push_back(m_ts->create_user_trx(nullptr));
    }

    auto r = ut::bench_mt(std::format("begin_view_commit_t{}", n), n, 100000 / uint64_t(n), [&](int t, uint64_t) {
      auto ok = trxs[t]->start(*m_sys, ULINT_UNDEFINED);

      ut::keep(ok);
      ut::keep(trxs[t]->assign_read_view(*m_sys));
      (void)trxs[t]->commit(*m_sys);
    });

    for (auto t : trxs) {
      m_ts->destroy_user_trx(t);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** A visibility check: an old id (below W), an id that needs the table, with W held
down by a long running transaction. */
TEST_F(MvccPerf, Visible) {
  /* A fast path: an id from before anything. */
  auto done = begin();
  const auto old_id = done->m_id;

  commit(done);

  /* Sweep to raise the low water, if the tree has one. */
  for (int i{}; i < 100; ++i) {
    commit(begin());
  }

  auto reader = begin();
  auto view = reader->assign_read_view(*m_sys);
  uint64_t n{};

  /* Or this measures the table, not the fast path. */
  EXPECT_LT(old_id, view->m_watermark);

  (void)ut::bench("visible_fast", 2000000, [&](uint64_t) { n += view->changes_visible(old_id); });

  ut::keep(n);
  commit(reader);

  /* W held down: the ids above it take the table, or the array of the view. */
  auto longrun = begin();
  std::vector<trx_id_t> ids;

  for (int i{}; i < 1000; ++i) {
    auto t = begin();

    ids.push_back(t->m_id);
    commit(t);
  }

  auto reader2 = begin();
  auto view2 = reader2->assign_read_view(*m_sys);

  (void)ut::bench("visible_lookup", 2000000, [&](uint64_t i) { n += view2->changes_visible(ids[i % ids.size()]); });

  ut::keep(n);
  commit(reader2);
  commit(longrun);
}

/** Rows: insert, update, scan through the API-ish helpers; the cost of the id. */
TEST_F(MvccPerf, Rows) {
  constexpr uint32_t N = 20000;

  auto r = ut::bench(
    "row_insert",
    N,
    [&](uint64_t i) {
      auto t = begin();
      auto err = insert(t, ut::Row{uint32_t(i), uint32_t(i), "0123456789"});

      ut::keep(err);
      commit(t);
    }
  );

  EXPECT_LT(r.ns_per_op(), 1e7);

  (void)ut::bench("row_update", N, [&](uint64_t i) {
    auto t = begin();
    auto err = update(t, uint32_t(i), ut::Row{uint32_t(i), uint32_t(i), "abcdefghij"});

    ut::keep(err);
    commit(t);
  });

  (void)ut::bench("row_scan", 20, [&](uint64_t) {
    auto t = begin();

    ut::keep(scan(t).size());
    commit(t);
  });

  m_sys->m_dict->mutex_acquire();
  auto table = m_sys->m_dict->table_get(ut::TABLE);
  m_sys->m_dict->mutex_release();

  const auto pages = m_sys->m_btree->get_size(table->get_first_index(), BTR_N_LEAF_PAGES);

  /* The leaf pages, in the value of the CSV (ns per op column): N ops of a total
  "seconds" that makes ns_per_op = pages per 1000 rows. */
  ut::report(ut::BenchResult{"row_leaf_pages_per_1000", 1, N, double(pages) * 1000.0 / double(N) * double(N) / 1e9});
}

}  // namespace
