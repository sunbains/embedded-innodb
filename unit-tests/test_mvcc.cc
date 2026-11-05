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

/** @file unit-tests/test_mvcc.cc
Read views as timestamps, the commit table, the retirement, purge's view; the clock
on disk: see READ VIEWS in trx0sys.h and ut0hlc.h. */

#include "ut0env.h"

#include "api0api.h"
#include "btr0cur.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "log0log.h"
#include "mtr0log.h"
#include "os0file.h"
#include "read0read.h"
#include "row0upd.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0logger.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <thread>

using namespace std::chrono_literals;

namespace {

[[maybe_unused]] const bool quiet = [] {
  logger::level = logger::Warn;
  return true;
}();

class Mvcc : public ut::DbTest {
 protected:
  [[nodiscard]] TrxSys &ts() { return *m_sys->m_trx_sys; }

  /** Run purge until it has nothing to do. */
  void purge() {
    for (int i{}; i < 100000 && ts().m_purge->run() > 0; ++i) {
    }

    (void)ts().m_purge->run();
  }

  void set_c6_hook(std::function<void(Trx *, int)> fn) {
    m_c6 = std::move(fn);
    ts().m_commit_hook_arg = &m_c6;
    ts().m_commit_hook = [](void *arg, Trx *trx, int step) {
      (*static_cast<std::function<void(Trx *, int)> *>(arg))(trx, step);
    };
  }

  void clear_c6_hook() { ts().m_commit_hook = nullptr; }

  /** @return a row with key k: a is k too, it is unique in SEC_A. */
  [[nodiscard]] static ut::Row row(uint32_t k, const std::string &b = "x") { return ut::Row{k, k, b}; }

  /** Rewrite the DB_TRX_ID of the clustered record of key k: a row from another
  node. Redo logged, the roll pointer stays. */
  void set_row_id(uint32_t k, trx_id_t id) {
    m_sys->m_dict->mutex_acquire();
    auto table = m_sys->m_dict->table_get(ut::TABLE);
    m_sys->m_dict->mutex_release();

    auto index = table->get_first_index();
    auto heap = mem_heap_create(256);
    auto key = dtuple_create(heap, 1);

    index->copy_types(key, 1);

    auto buf = static_cast<byte *>(mem_heap_alloc(heap, 4));

    mach_write_to_4(buf, k);
    dfield_set_data(dtuple_get_nth_field(key, 0), buf, 4);
    dtuple_set_n_fields_cmp(key, 1);

    Mtr mtr;
    BtreeCursor cur;

    mtr.start();
    cur.search_to_nth_level(*m_sys, nullptr, index, 0, key, PAGE_CUR_LE, BTR_MODIFY_LEAF, &mtr, Current_location());

    auto rec = cur.get_rec();
    RecOffs offsets = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;
    const auto offset = offsets.get_nth_field(index->get_sys_col_field_pos(DATA_TRX_ID), &len);

    ASSERT_EQ(len, DATA_TRX_ID_LEN);
    mlog_write_uint64(rec.get() + offset, id, &mtr);

    mtr.commit(*m_sys);
    mem_heap_free(heap);
  }

  std::function<void(Trx *, int)> m_c6;

  /** A directory for a test that needs a new instance. */
  std::unique_ptr<ut::TestDir> m_tmp;
};

/** Each change of a secondary record makes its transaction the last modifier. */
TEST_F(Mvcc, SecIdInsert) {
  auto t = begin();
  const auto id = t->m_id;

  ASSERT_EQ(insert(t, row(1)), DB_SUCCESS);

  auto rec = sec_rec(1, 1);

  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->m_id, id);
  EXPECT_FALSE(rec->m_deleted);
  commit(t);
  EXPECT_EQ(sec_rec(1, 1)->m_id, id);
}

TEST_F(Mvcc, SecIdDelete) {
  load({{1, row(1)}});

  /* The view keeps background purge from removing the delete marked record. */
  auto r = begin();

  ASSERT_NE(r->assign_read_view(*m_sys), nullptr);

  auto t = begin();
  const auto id = t->m_id;

  ASSERT_EQ(remove(t, 1), DB_SUCCESS);
  commit(t);

  auto rec = sec_rec(1, 1);

  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->m_id, id);
  EXPECT_TRUE(rec->m_deleted);
  commit(r);
}

/** An update of the key: the old record delete marked, a new one, both this one's. */
TEST_F(Mvcc, SecIdMove) {
  load({{1, row(1)}});

  /* The view keeps background purge from removing the delete marked record. */
  auto r = begin();

  ASSERT_NE(r->assign_read_view(*m_sys), nullptr);

  auto t = begin();
  const auto id = t->m_id;

  ASSERT_EQ(update(t, 1, ut::Row{1, 9, "x"}), DB_SUCCESS);
  commit(t);

  EXPECT_EQ(sec_rec(1, 1)->m_id, id);
  EXPECT_TRUE(sec_rec(1, 1)->m_deleted);
  EXPECT_EQ(sec_rec(9, 1)->m_id, id);
  EXPECT_FALSE(sec_rec(9, 1)->m_deleted);
  commit(r);
}

/** An update of the row that leaves the key: the record is not touched. A view that
sees the old id sees the record, whichever version of the row it reads. */
TEST_F(Mvcc, SecIdOtherColumn) {
  load({{1, row(1)}});

  const auto before = sec_rec(1, 1)->m_id;
  auto t = begin();

  ASSERT_EQ(update(t, 1, ut::Row{1, 1, "other"}), DB_SUCCESS);
  commit(t);

  EXPECT_EQ(sec_rec(1, 1)->m_id, before);
  EXPECT_FALSE(sec_rec(1, 1)->m_deleted);
}

/** A delete marked record comes back by modification: the id moves with the mark. */
TEST_F(Mvcc, SecIdReuse) {
  load({{1, row(1)}});

  auto d = begin();

  ASSERT_EQ(update(d, 1, ut::Row{1, 9, "x"}), DB_SUCCESS);
  commit(d);

  auto t = begin();
  const auto id = t->m_id;

  ASSERT_EQ(update(t, 1, ut::Row{1, 1, "x"}), DB_SUCCESS);

  EXPECT_EQ(sec_rec(1, 1)->m_id, id);
  EXPECT_FALSE(sec_rec(1, 1)->m_deleted);
  commit(t);
}

/** A rollback restores the state and leaves the id: every view that doesn't see the
transaction goes to the table, a later one gets the restored state. */
TEST_F(Mvcc, SecIdRollback) {
  load({{1, row(1)}});

  auto t = begin();
  const auto id = t->m_id;

  ASSERT_EQ(remove(t, 1), DB_SUCCESS);
  EXPECT_TRUE(sec_rec(1, 1)->m_deleted);
  rollback(t);

  EXPECT_FALSE(sec_rec(1, 1)->m_deleted);
  EXPECT_EQ(sec_rec(1, 1)->m_id, id);

  auto r = begin();

  EXPECT_EQ(scan_sec_only(r).size(), 1u);
  commit(r);
}

/** The filter is on the record, not on the page: an old row on a page with a newer,
uncommitted row is decided without the watermark, and the new one isn't visible. */
TEST_F(Mvcc, SecOnePage) {
  load({{1, row(1)}});

  auto w = begin();

  ASSERT_EQ(insert(w, row(2)), DB_SUCCESS);

  auto r = begin();

  ASSERT_NE(r->assign_read_view(*m_sys), nullptr);

  EXPECT_EQ(scan_sec_only(r), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}}));
  commit(w);
  EXPECT_EQ(scan_sec_only(r), (std::vector<std::pair<uint32_t, uint32_t>>{{1, 1}}));
  commit(r);
}

/** An index only read through the API: a record delete marked by a transaction the view
doesn't see is read, and the scan goes on to the records after it. */
TEST_F(Mvcc, SecOnlyDeleted) {
  load({{1, row(1)}, {2, row(2)}, {3, row(3)}});

  auto r = begin();

  ASSERT_NE(r->assign_read_view(*m_sys), nullptr);

  auto w = begin();

  ASSERT_EQ(remove(w, 1), DB_SUCCESS);
  ASSERT_EQ(update(w, 2, ut::Row{2, 9, "x"}), DB_SUCCESS);
  commit(w);

  const std::vector<std::pair<uint32_t, uint32_t>> all{{1, 1}, {2, 2}, {3, 3}};

  EXPECT_EQ(scan_sec_only(r), all);
  commit(r);

  auto later = begin();

  EXPECT_EQ(scan_sec_only(later), (std::vector<std::pair<uint32_t, uint32_t>>{{3, 3}, {9, 2}}));
  commit(later);
}

/** The reference: from random histories, with views held across them, the index alone
gives what the table gives, in every view, with purge running in between. */
TEST_F(Mvcc, SecModel) {
  std::mt19937 rng(20261104);
  std::map<uint32_t, uint32_t> now;

  struct View {
    Trx *m_trx;
    std::vector<std::pair<uint32_t, uint32_t>> m_rows;
  };

  std::vector<View> views;

  /* Through the index, with the row from the table, and from the index alone: they agree. */
  const auto by_sec = [&](Trx *t) {
    std::vector<std::pair<uint32_t, uint32_t>> v;

    for (const auto &r : scan_sec(t)) {
      v.emplace_back(r.m_a, r.m_k);
    }

    EXPECT_EQ(v, scan_sec_only(t));
    return v;
  };

  const auto table_rows = [&](Trx *t) {
    std::vector<std::pair<uint32_t, uint32_t>> v;

    for (const auto &[k, r] : scan(t)) {
      v.emplace_back(r.m_a, k);
    }

    std::sort(v.begin(), v.end());
    return v;
  };

  for (int step{}; step < 1500; ++step) {
    const auto op = rng() % 10;

    if (op < 2 && views.size() < 6) {
      auto r = begin();

      ASSERT_NE(r->assign_read_view(*m_sys), nullptr);
      views.push_back({r, table_rows(r)});
      EXPECT_EQ(by_sec(r), views.back().m_rows) << "at open, step " << step;
    } else if (op < 3 && !views.empty()) {
      const auto i = rng() % views.size();

      EXPECT_EQ(by_sec(views[i].m_trx), views[i].m_rows) << "at close, step " << step;
      EXPECT_EQ(table_rows(views[i].m_trx), views[i].m_rows) << "at close, step " << step;
      commit(views[i].m_trx);
      views.erase(views.begin() + i);
    } else if (op == 3) {
      (void)ts().m_purge->run();
    } else {
      /* A transaction of a few changes, among few keys and values: records flip
      between delete marked and not, values come back. */
      auto t = begin();
      auto after = now;
      bool ok{true};

      for (uint32_t n = rng() % 3 + 1; n > 0 && ok; --n) {
        const uint32_t k = rng() % 12;
        const uint32_t a = rng() % 5 * 100 + k;
        const auto it = after.find(k);

        if (it == after.end()) {
          ok = insert(t, ut::Row{k, a, "x"}) == DB_SUCCESS;
          after[k] = a;
        } else if (rng() % 3 == 0) {
          ok = remove(t, k) == DB_SUCCESS;
          after.erase(k);
        } else {
          ok = update(t, k, ut::Row{k, a, "y"}) == DB_SUCCESS;
          it->second = a;
        }
      }

      ASSERT_TRUE(ok);

      /* Its own view of its own changes. */
      std::vector<std::pair<uint32_t, uint32_t>> own;

      for (const auto &[k, a] : after) {
        own.emplace_back(a, k);
      }

      std::sort(own.begin(), own.end());

      if (rng() % 4 == 0) {
        rollback(t);
      } else {
        EXPECT_EQ(by_sec(t), own) << "its own changes, step " << step;
        commit(t);
        now = after;
      }
    }

    for (const auto &v : views) {
      EXPECT_EQ(by_sec(v.m_trx), v.m_rows) << "step " << step;
    }

    if (::testing::Test::HasFailure()) {
      break;
    }
  }

  for (const auto &v : views) {
    commit(v.m_trx);
  }
}

/** A view that sees a transaction that depends on another sees that one: forced at
every step of the commit of the later one, and of the earlier one. */
TEST_F(Mvcc, ChainDet) {
  for (int step : {0, 1, 2, 3}) {
    for (bool of_a : {false, true}) {
      auto b = begin();
      auto a = begin();

      ASSERT_GT(a->m_id, b->m_id) << "the case of the list walk: A is first in a walk";

      const auto id_a = a->m_id;
      const auto id_b = b->m_id;

      ReadView *view{};
      auto reader = begin();

      set_c6_hook([&](Trx *trx, int s) {
        if (s == step && trx == (of_a ? a : b) && view == nullptr) {
          view = reader->assign_read_view(*m_sys);
        }
      });

      const uint32_t base = 1000 + uint32_t(step) * 10 + (of_a ? 5 : 0);

      ASSERT_EQ(insert(a, row(base)), DB_SUCCESS);
      commit(a);

      /* B reads A's row, then writes the next one. */
      EXPECT_TRUE(find_sec(b, base).has_value());
      ASSERT_EQ(insert(b, row(base + 1)), DB_SUCCESS);
      commit(b);

      clear_c6_hook();

      ASSERT_NE(view, nullptr);

      if (view->changes_visible(id_b)) {
        EXPECT_TRUE(view->changes_visible(id_a)) << "step " << step;
      }

      /* The answer doesn't change. */
      const auto sees_a = view->changes_visible(id_a);
      const auto sees_b = view->changes_visible(id_b);

      commit(begin());
      ts().retire();

      EXPECT_EQ(view->changes_visible(id_a), sees_a);
      EXPECT_EQ(view->changes_visible(id_b), sees_b);

      commit(reader);
    }
  }
}

/** Repeatable read under a racing commit, at each step of C6: the view opened at the
step gives one answer, before and after the commit is done; also with the clock
jumping at that step. */
TEST_F(Mvcc, RepeatableC6) {
  for (bool jump : {false, true}) {
    for (int step : {0, 1, 2, 3}) {
      auto reader = begin();
      auto t = begin();
      const auto id = t->m_id;
      ReadView *view{};
      bool at_step{};

      set_c6_hook([&](Trx *trx, int s) {
        if (trx == t && s == step) {
          view = reader->assign_read_view(*m_sys);
          at_step = view->changes_visible(id);

          if (jump) {
            ASSERT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 1000000, 7)));
          }

          ASSERT_EQ(view->changes_visible(id), at_step);
        }
      });

      commit(t);
      clear_c6_hook();

      ASSERT_NE(view, nullptr);
      EXPECT_EQ(view->changes_visible(id), at_step) << "step " << step;

      /* Only with the stamp stored can it be visible, and a helper's stamp at step 1
      or 2 is above S. */
      EXPECT_EQ(at_step, step == 3) << "step " << step;

      commit(reader);
    }
  }
}

/** Two reads of a row under one view agree with a real row in between: the row
updated and committed at each step of C6. */
TEST_F(Mvcc, RepeatableRow) {
  load({{1, row(1, "old")}});

  for (int step : {0, 1, 2, 3}) {
    auto w = begin();

    ASSERT_EQ(update(w, 1, row(1, "new")), DB_SUCCESS);

    auto reader = begin();
    ut::Rows first;

    set_c6_hook([&](Trx *trx, int s) {
      if (trx == w && s == step) {
        first = scan(reader);
      }
    });

    commit(w);
    clear_c6_hook();

    const auto second = scan(reader);

    EXPECT_EQ(first, second) << "step " << step;

    commit(reader);

    /* Back, for the next one. */
    auto r = begin();

    ASSERT_EQ(update(r, 1, row(1, "old")), DB_SUCCESS);
    commit(r);
  }
}

/** A view being opened and the retirer. Before the slot is set: a commit retired
there is before its S, the view sees it by not found. Between the slot and S: the slot's
lower bound keeps the entry, the view sees it by its stamp. After the open: its S
keeps the entry of a later commit, invisible. */
TEST_F(Mvcc, RetireVsOpen) {
  for (int step : {0, 1}) {
    /* A fresh floor and W. */
    commit(begin());
    ts().retire();

    auto t = begin();
    const auto id = t->m_id;
    auto later = begin();
    const auto later_id = later->m_id;
    auto reader = begin();

    struct Ctx {
      Mvcc_RetireVsOpen_Test *m_test;
      Trx *m_t;
      int m_step;
      bool m_done;
    } ctx{this, t, step, false};

    ts().m_view_hook_arg = &ctx;
    ts().m_view_hook = [](void *arg, int s) {
      auto c = static_cast<Ctx *>(arg);

      if (s == c->m_step && !c->m_done) {
        c->m_done = true;
        c->m_test->commit(c->m_t);
        c->m_test->ts().retire();
        c->m_test->ts().retire();
      }
    };

    auto view = reader->assign_read_view(*m_sys);

    ts().m_view_hook = nullptr;

    ASSERT_TRUE(ctx.m_done);

    EXPECT_EQ(ts().id_in_table(id), step == 1) << "retired only with no slot in its way";
    EXPECT_TRUE(view->changes_visible(id)) << "committed before S, step " << step;

    /* After the open. */
    commit(later);
    ts().retire();
    ts().retire();
    EXPECT_FALSE(view->changes_visible(later_id));
    EXPECT_TRUE(ts().id_in_table(later_id)) << "retired above the horizon";

    commit(reader);

    /* The view is gone: they can go now. */
    commit(begin());
    ts().retire();
    commit(begin());
    ts().retire();

    EXPECT_FALSE(ts().id_in_table(later_id));
    EXPECT_FALSE(ts().id_in_table(id));

    /* And read as committed. */
    auto r2 = begin();

    EXPECT_TRUE(r2->assign_read_view(*m_sys)->changes_visible(later_id));
    commit(r2);
  }
}

/** W: a stale one is only slower. A long running transaction holds it down, a view
sees past it by the table. */
TEST_F(Mvcc, Watermark) {
  auto old = begin();
  const auto old_id = old->m_id;

  std::vector<trx_id_t> done;

  for (int i{}; i < 50; ++i) {
    auto t = begin();

    done.push_back(t->m_id);
    commit(t);
  }

  ts().retire();

  auto r = begin();
  auto view = r->assign_read_view(*m_sys);

  EXPECT_LE(view->m_watermark, old_id);
  EXPECT_FALSE(view->changes_visible(old_id));

  for (auto id : done) {
    EXPECT_TRUE(view->changes_visible(id));
  }

  commit(old);
  commit(r);

  commit(begin());
  ts().retire();
  commit(begin());
  ts().retire();

  EXPECT_GT(ts().m_low_water.load(), old_id);

  auto r2 = begin();

  EXPECT_GT(r2->assign_read_view(*m_sys)->m_watermark, done.back());
  commit(r2);
}

/** The chains, with threads: each transaction reads the row of the last one and
writes the next one, with an older id than the last one (the case that breaks a walk
of a list). A view sees chains without gaps, and the same rows at a second read,
with the clock jumping, and purge running. */
TEST_F(Mvcc, Chains) {
  constexpr int N_CHAINS = 4;
  constexpr int N_READERS = 4;
  constexpr uint32_t CHAIN = 100000;

  std::atomic<bool> stop{};
  std::atomic<uint64_t> n_views{};
  std::atomic<uint64_t> n_links{};
  std::vector<std::thread> threads;

  for (int c{}; c < N_CHAINS; ++c) {
    threads.emplace_back([&, c] {
      uint32_t k = uint32_t(c + 1) * CHAIN;

      while (!stop.load(std::memory_order_relaxed)) {
        /* X starts first: its id is below Y's, its link in the chain after it. */
        auto x = begin();
        auto y = begin();

        if (k > uint32_t(c + 1) * CHAIN) {
          ASSERT_TRUE(find_sec(y, k - 1).has_value()) << "the last link is committed";
        }

        ASSERT_EQ(insert(y, row(k)), DB_SUCCESS);
        commit(y);

        ASSERT_TRUE(find_sec(x, k).has_value()) << "the link just committed";
        ASSERT_EQ(insert(x, row(k + 1)), DB_SUCCESS);
        commit(x);

        k += 2;
        n_links.fetch_add(2, std::memory_order_relaxed);

        /* A bounded table. */
        if (k - uint32_t(c + 1) * CHAIN >= 3000) {
          return;
        }
      }
    });
  }

  for (int r{}; r < N_READERS; ++r) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        auto trx = begin();
        const auto first = scan(trx);
        const auto second = scan(trx);

        EXPECT_EQ(first.size(), second.size()) << "repeatable read";
        EXPECT_EQ(first, second) << "repeatable read";

        /* No gap in any chain. */
        std::map<uint32_t, uint32_t> last;

        for (const auto &[k, rw] : first) {
          const auto c = k / CHAIN;
          const auto it = last.find(c);

          if (it != last.end()) {
            EXPECT_EQ(k, it->second + 1) << "a gap in chain " << c;
          } else {
            EXPECT_EQ(k % CHAIN, 0u) << "the chain " << c << " starts in the middle";
          }

          last[c] = k;
        }

        commit(trx);
        n_views.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  /* update(): the clock jumps during the run. */
  threads.emplace_back([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      EXPECT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 50000, 3)));
      std::this_thread::sleep_for(1ms);
    }
  });

  threads.emplace_back([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      (void)ts().m_purge->run();
      std::this_thread::sleep_for(1ms);
    }
  });

  const auto end = std::chrono::steady_clock::now() + 12s;

  while (std::chrono::steady_clock::now() < end && n_links.load() < N_CHAINS * 3000) {
    std::this_thread::sleep_for(50ms);
  }

  stop.store(true);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(n_views.load(), 0u);
  EXPECT_GT(n_links.load(), 0u);
}

/** Purge against open and close: a view keeps what it read while purge runs and other
views come and go. */
TEST_F(Mvcc, PurgeKeeps) {
  load({{1, row(1, "v0")}});

  std::atomic<bool> stop{};
  std::atomic<uint32_t> version{};
  std::vector<std::thread> threads;

  threads.emplace_back([&] {
    for (uint32_t v = 1; !stop.load(std::memory_order_relaxed); ++v) {
      auto w = begin();

      ASSERT_EQ(update(w, 1, row(1, "v" + std::to_string(v))), DB_SUCCESS);
      commit(w);
      version.store(v);
    }
  });

  for (int t{}; t < 2; ++t) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        (void)ts().m_purge->run();
      }
    });
  }

  for (int t{}; t < 4; ++t) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        auto r = begin();
        const auto first = scan(r);

        ASSERT_EQ(first.size(), 1u);

        /* Purge goes on: the view must still get to its version. */
        for (int i{}; i < 5; ++i) {
          std::this_thread::sleep_for(200us);
          ASSERT_EQ(scan(r), first);
        }

        commit(r);
      }
    });
  }

  std::this_thread::sleep_for(2s);
  stop.store(true);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(version.load(), 0u);
  purge();
}

/** Purge's order across jumps of the clock: all the history is purged, in numbers. */
TEST_F(Mvcc, PurgeOrderJump) {
  load({{1, row(1, "v0")}});

  for (int i{}; i < 200; ++i) {
    auto w = begin();

    ASSERT_EQ(update(w, 1, row(1, "v" + std::to_string(i))), DB_SUCCESS);

    if (i % 7 == 0) {
      ASSERT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 1000000, 9)));
    }

    commit(w);
  }

  purge();

  /* Purge truncates in pieces, the end of the list stays until a later batch. */
  EXPECT_LT(ts().m_rseg_history_len.load(), 100u);

  auto r = begin();
  const auto rows = scan(r);

  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows.at(1).m_b, "v199");
  commit(r);
}

/** Read committed: each statement end closes the view, the next one sees more. */
TEST_F(Mvcc, ReadCommitted) {
  load({{1, row(1, "a")}});

  auto rc = begin(IB_TRX_READ_COMMITTED);

  EXPECT_EQ(scan(rc).at(1).m_b, "a");

  auto w = begin();

  ASSERT_EQ(update(w, 1, row(1, "b")), DB_SUCCESS);

  ts().close_read_view_for_read_committed(rc);
  EXPECT_EQ(scan(rc).at(1).m_b, "a") << "not committed yet";

  commit(w);

  ts().close_read_view_for_read_committed(rc);
  EXPECT_EQ(scan(rc).at(1).m_b, "b");

  commit(rc);
}

/** A cursor view: its creator's later changes are hidden from it. */
TEST_F(Mvcc, Cursor) {
  load({{1, row(1, "a")}});

  auto t = begin();

  EXPECT_EQ(scan(t).at(1).m_b, "a");

  auto cursor = ts().create_cursor_view(t);

  ASSERT_EQ(update(t, 1, row(1, "mine")), DB_SUCCESS);

  ts().set_cursor_view(t, cursor);
  EXPECT_EQ(scan(t).at(1).m_b, "a") << "the cursor view";

  ts().set_cursor_view(t, nullptr);
  EXPECT_EQ(scan(t).at(1).m_b, "mine") << "its own view";

  ts().close_cursor_view(t, cursor);
  commit(t);
}

/** A row of another node, later than the local clock: invisible, no error, no lock of
a transaction here; once observed, visible; reads, updates, rollback, purge. */
TEST_F(Mvcc, ForeignRow) {
  load({{1, row(1, "base")}});
  load({{2, row(2, "other")}});

  const auto now = ts().m_clock.read();
  const auto foreign_hlc = now + 3600000000ULL;
  const uint32_t node = ts().m_clock.node() + 1;
  const trx_id_t foreign = ut::id_pack(ts().m_clock.epoch(), foreign_hlc, node);

  set_row_id(2, foreign);

  /* Ahead of the clock: a view doesn't see it; the insert has no previous version. */
  {
    auto r = begin();
    const auto rows = scan(r);

    EXPECT_EQ(rows.size(), 1u);
    EXPECT_TRUE(rows.contains(1));
    EXPECT_FALSE(ts().is_active(foreign));
    EXPECT_EQ(ts().pin_active(foreign), nullptr);
    commit(r);
  }

  /* The sanity check doesn't call it corrupt, a local id that far ahead is. */
  EXPECT_FALSE(ts().id_is_local(foreign));

  ts().observe_id(foreign);

  EXPECT_GE(ts().m_clock.read(), foreign_hlc);
  EXPECT_GE(ts().m_hlc_stored.load() + TRX_SYS_HLC_MARGIN_US, foreign_hlc) << "covered at a restart";

  {
    auto r = begin();

    EXPECT_GT(r->m_id, foreign) << "local ids moved past it";
    EXPECT_EQ(scan(r).size(), 2u) << "committed, the clock has passed it";
    commit(r);
  }

  /* An update of it, undone: the id is back. */
  {
    auto w = begin();

    ASSERT_EQ(update(w, 2, row(2, "local")), DB_SUCCESS);
    rollback(w);

    auto r = begin();

    EXPECT_EQ(scan(r).at(2).m_b, "other");
    commit(r);
  }

  /* An old version of it through undo. */
  {
    auto old = begin();

    EXPECT_EQ(scan(old).at(2).m_b, "other");

    auto w = begin();

    ASSERT_EQ(update(w, 2, row(2, "local")), DB_SUCCESS);
    commit(w);

    EXPECT_EQ(scan(old).at(2).m_b, "other");
    commit(old);
  }

  purge();

  auto r = begin();

  EXPECT_EQ(scan(r).at(2).m_b, "local");
  commit(r);

  /* The clock moved past it at a restart too. */
  restart();

  auto r2 = begin();

  EXPECT_GT(r2->m_id, foreign);
  EXPECT_EQ(scan(r2).at(2).m_b, "local");
  commit(r2);
}

/** A crash with an active and a prepared transaction, and a committed one: the view
after recovery sees only the last; the prepared one is in the table, not seen, its
locks kept, until it is decided. */
TEST_F(Mvcc, CrashPrepared) {
  load({{1, row(1, "base")}});

  crash([&] {
    auto done = begin();

    EXPECT_EQ(insert(done, row(2, "done")), DB_SUCCESS);
    commit(done);

    auto active = begin();

    EXPECT_EQ(insert(active, row(3, "active")), DB_SUCCESS);

    auto prep = begin();

    EXPECT_EQ(insert(prep, row(4, "prep")), DB_SUCCESS);
    prep->m_xid.formatID = 1;
    prep->m_xid.gtrid_length = 4;
    prep->m_xid.bqual_length = 0;
    memcpy(prep->m_xid.data, "gtid", 4);

    (void)prep->prepare(*m_sys);

    /* Make it durable. */
    m_sys->m_log->buffer_flush_to_disk();
  });

  /* The rollback of the active one runs in a background thread: wait for it. */
  auto r = begin();
  auto rows = scan(r);

  commit(r);

  for (int i{}; i < 500 && rows.contains(3); ++i) {
    std::this_thread::sleep_for(10ms);

    r = begin();
    rows = scan(r);
    commit(r);
  }

  EXPECT_TRUE(rows.contains(1));
  EXPECT_TRUE(rows.contains(2));
  EXPECT_FALSE(rows.contains(3));
  EXPECT_FALSE(rows.contains(4)) << "prepared: not visible";

  Xid xids[4];

  ASSERT_EQ(ts().recover(xids, 4), 1);

  Xid xid = xids[0];
  auto prep = ts().get_trx_by_xid(&xid);

  ASSERT_NE(prep, nullptr);
  EXPECT_TRUE(ts().is_active(prep->m_id));

  (void)prep->commit(*m_sys);
  ts().destroy_trx(prep);

  r = begin();
  EXPECT_TRUE(scan(r).contains(4)) << "committed now";
  commit(r);
}

/** No reissue after a crash with the clock ahead of the wall clock, and with the wall
clock behind the stored high water: the ids of the next life are above all the ids
of the last. */
TEST_F(Mvcc, CrashAhead) {
  const auto path = m_dir.m_path / "ids";

  /* An hour ahead. */
  ASSERT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 3600000000ULL, 0)));

  auto t0 = begin();

  commit(t0);

  crash([&] {
    trx_id_t last{};

    for (int i{}; i < 200; ++i) {
      auto t = begin();

      EXPECT_EQ(insert(t, row(uint32_t(i + 1))), DB_SUCCESS);
      last = t->m_id;
      commit(t);
    }

    std::ofstream(path) << last;
  });

  trx_id_t last{};

  std::ifstream(path) >> last;

  ASSERT_NE(last, 0u);

  auto t = begin();

  EXPECT_GT(t->m_id, last) << "reissued";
  EXPECT_GT(ut::id_to_hlc(t->m_id), ts().m_clock.wall()) << "ahead of the wall clock, still";
  commit(t);
}

/** The whole life of a row near the end of the clock's range, with a crash: 49 bits of
time in the row, the undo, the redo. */
TEST_F(Mvcc, NearLimit) {
  stop();

  /* A year from the end; margin of a restart well within. */
  m_config.m_trx.m_hlc_start = ut::HLC_BASE_US + ut::HLC_MAX_TIME - 31536000000000ULL;

  m_tmp = std::make_unique<ut::TestDir>();
  auto cfg = ut::make_config(m_tmp->m_path);

  cfg.m_trx.m_hlc_start = m_config.m_trx.m_hlc_start;
  m_config = cfg;

  start();

  auto check = [&]() {
    auto t = begin();

    EXPECT_GE(ut::id_to_hlc(t->m_id), m_config.m_trx.m_hlc_start);
    commit(t);
  };

  check();

  load({{1, row(1, "a")}, {2, row(2, "b")}});

  auto old = begin();

  EXPECT_EQ(scan(old).size(), 2u);

  auto w = begin();

  ASSERT_EQ(update(w, 1, row(1, "a2")), DB_SUCCESS);
  ASSERT_EQ(remove(w, 2), DB_SUCCESS);
  commit(w);

  EXPECT_EQ(scan(old).at(1).m_b, "a") << "old version through undo";
  commit(old);

  auto u = begin();

  ASSERT_EQ(update(u, 1, row(1, "lost")), DB_SUCCESS);
  rollback(u);

  purge();

  crash([&] {
    auto c = begin();

    EXPECT_EQ(insert(c, row(5, "durable")), DB_SUCCESS);
    commit(c);

    auto lost = begin();

    EXPECT_EQ(update(lost, 1, row(1, "lost")), DB_SUCCESS);
    m_sys->m_log->buffer_flush_to_disk();
  });

  auto r = begin();
  auto rows = scan(r);

  commit(r);

  for (int i{}; i < 500 && rows.at(1).m_b != "a2"; ++i) {
    std::this_thread::sleep_for(10ms);

    r = begin();
    rows = scan(r);
    commit(r);
  }

  EXPECT_EQ(rows.at(1).m_b, "a2");
  EXPECT_TRUE(rows.contains(5));
  EXPECT_FALSE(rows.contains(2));

  purge();
  check();
}

/** Round trip of an id, with its bits set, on a page: a row's DB_TRX_ID as written is
what the view reads, and rebuilds the Timestamp. Vectors of test_hlc.cc's form from
the Rust code for a value at the top of the range. */
TEST_F(Mvcc, IdOnPage) {
  load({{1, row(1, "a")}});

  /* From Rust: Timestamp::new(511, 1793491200000000 + (1 << 49) - 2, NodeId(63)). */
  const auto id = ut::id_pack(511, ut::HLC_BASE_US + ut::HLC_MAX_TIME, 63);
  const auto ts_id = ut::id_to_timestamp(id);

  EXPECT_EQ(ts_id.msb(), 33488896ULL);
  EXPECT_EQ(ts_id.lsb(), 37703058454740960ULL);
  EXPECT_EQ(ts_id.node(), 63u);
  EXPECT_EQ(ts_id.hlc(), 2356441153421310ULL);
  EXPECT_EQ(ts_id.epoch(), 511u);

  set_row_id(1, id);

  m_sys->m_dict->mutex_acquire();
  auto table = m_sys->m_dict->table_get(ut::TABLE);
  m_sys->m_dict->mutex_release();

  auto index = table->get_first_index();
  auto heap = mem_heap_create(256);
  auto key = dtuple_create(heap, 1);

  index->copy_types(key, 1);

  auto buf = static_cast<byte *>(mem_heap_alloc(heap, 4));

  mach_write_to_4(buf, 1);
  dfield_set_data(dtuple_get_nth_field(key, 0), buf, 4);
  dtuple_set_n_fields_cmp(key, 1);

  Mtr mtr;
  BtreeCursor cur;

  mtr.start();
  cur.search_to_nth_level(*m_sys, nullptr, index, 0, key, PAGE_CUR_LE, BTR_SEARCH_LEAF, &mtr, Current_location());

  auto rec = cur.get_rec();
  auto offsets = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

  EXPECT_EQ(row_get_rec_trx_id(rec, index, offsets), id);

  mtr.commit(*m_sys);
  mem_heap_free(heap);

  /* Put it back in range for shutdown: a view would call it the future. */
  set_row_id(1, ut::id_pack(ts().m_clock.epoch(), ts().m_clock.read(), ts().m_clock.node()));
}

/** Old files are refused, whatever their state, with no recovery run. */
TEST_F(Mvcc, RefuseOldFormat) {
  stop();

  const auto file = m_dir.m_path / "system.ibd";
  const off_t off = off_t(TRX_SYS_PAGE_NO) * UNIV_PAGE_SIZE + TRX_SYS + TRX_SYS_FORMAT_MAGIC;

  {
    std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
    const char zeros[12]{};

    f.seekp(off);
    f.write(zeros, sizeof(zeros));
  }

  m_sys = std::make_unique<System>();
  ASSERT_EQ(m_sys->init(m_config), DB_SUCCESS);
  EXPECT_NE(m_sys->startup(), DB_SUCCESS);
  m_sys.reset();
}

/** A version of the format that isn't ours, and a node id that isn't the files'. */
TEST_F(Mvcc, RefuseVersionNode) {
  stop();

  const auto file = m_dir.m_path / "system.ibd";
  const off_t base = off_t(TRX_SYS_PAGE_NO) * UNIV_PAGE_SIZE + TRX_SYS;

  auto cfg = m_config;

  cfg.m_trx.m_node_id = 5;

  m_sys = std::make_unique<System>();
  ASSERT_EQ(m_sys->init(cfg), DB_SUCCESS);
  EXPECT_NE(m_sys->startup(), DB_SUCCESS) << "node";
  m_sys.reset();

  std::array<byte, 4> old{};

  {
    std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);

    f.seekg(base + TRX_SYS_FORMAT_VERSION);
    f.read(reinterpret_cast<char *>(old.data()), 4);

    const char bad[4]{0, 0, 0, 99};

    f.seekp(base + TRX_SYS_FORMAT_VERSION);
    f.write(bad, 4);
  }

  m_sys = std::make_unique<System>();
  ASSERT_EQ(m_sys->init(m_config), DB_SUCCESS);
  EXPECT_NE(m_sys->startup(), DB_SUCCESS) << "version";
  m_sys.reset();

  {
    std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);

    f.seekp(base + TRX_SYS_FORMAT_VERSION);
    f.write(reinterpret_cast<const char *>(old.data()), 4);
  }

  start(false);
}

/** A view left open at shutdown: reported, nothing leaks or trips. */
TEST_F(Mvcc, ShutdownView) {
  auto heap = mem_heap_create(256);

  (void)ts().open_read_view_now(0, heap);

  EXPECT_EQ(ts().n_read_views(), 1u);

  stop();

  mem_heap_free(heap);
}

/** update() jumping the clock during the work a crash then recovers: the changes come
back, what wasn't committed goes, and the next life's ids are above all. */
TEST_F(Mvcc, CrashJump) {
  const auto path = m_dir.m_path / "ids";

  crash([&] {
    trx_id_t last{};

    for (uint32_t i{1}; i <= 100; ++i) {
      auto t = begin();

      EXPECT_EQ(insert(t, row(i)), DB_SUCCESS);
      EXPECT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 2000000, 2)));

      last = std::max(last, t->m_id);
      commit(t);
    }

    auto lost = begin();

    EXPECT_EQ(insert(lost, row(1000)), DB_SUCCESS);
    EXPECT_TRUE(ts().clock_update(ut::Timestamp(1, ts().m_clock.read() + 2000000, 2)));

    last = std::max(last, lost->m_id);
    m_sys->m_log->buffer_flush_to_disk();

    std::ofstream(path) << last;
  });

  trx_id_t last{};

  std::ifstream(path) >> last;
  ASSERT_NE(last, 0u);

  auto t = begin();

  EXPECT_GT(t->m_id, last);

  commit(t);

  ut::Rows rows;

  for (int i{}; i < 500; ++i) {
    auto r = begin();

    rows = scan(r);
    commit(r);

    if (!rows.contains(1000)) {
      break;
    }

    std::this_thread::sleep_for(10ms);
  }

  EXPECT_FALSE(rows.contains(1000));

  for (uint32_t i{1}; i <= 100; ++i) {
    EXPECT_TRUE(rows.contains(i)) << i;
  }

  purge();
}

/** A row of another node ahead of the clock, written over locally without observing
it: the lock paths take no local owner for it, nothing asserts. */
TEST_F(Mvcc, ForeignAheadLock) {
  load({{1, row(1, "a")}});

  const auto foreign = ut::id_pack(ts().m_clock.epoch(), ts().m_clock.read() + 3600000000ULL, ts().m_clock.node() + 1);

  set_row_id(1, foreign);

  /* A locking write: the implicit lock lookup of the row. */
  auto w = begin();

  ASSERT_EQ(remove(w, 1), DB_SUCCESS);
  rollback(w);

  EXPECT_FALSE(ts().is_active(foreign));

  ts().observe_id(foreign);

  auto r = begin();

  EXPECT_EQ(scan(r).at(1).m_b, "a");
  commit(r);
}

/** A foreign epoch moves the clock's: integer order keeps Accord's. */
TEST_F(Mvcc, ObserveEpoch) {
  auto t = begin();
  const auto before = t->m_id;

  commit(t);

  const auto foreign = ut::id_pack(5, ts().m_clock.read(), 9);

  ts().observe_id(foreign);

  EXPECT_EQ(ts().m_clock.epoch(), 5u);

  auto after = begin();

  EXPECT_GT(after->m_id, foreign);
  EXPECT_GT(after->m_id, before);
  EXPECT_EQ(ut::id_to_epoch(after->m_id), 5u);
  commit(after);

  restart();

  EXPECT_EQ(ts().m_clock.epoch(), 5u);
}

/** Limits of the configuration. */
TEST_F(Mvcc, ConfigLimits) {
  auto cfg = m_config;

  cfg.m_trx.m_node_id = ut::HLC_MAX_NODES;
  EXPECT_EQ(cfg.normalize(), DB_INVALID_INPUT);

  cfg = m_config;
  cfg.m_trx.m_epoch = ut::HLC_MAX_EPOCHS;
  EXPECT_EQ(cfg.normalize(), DB_INVALID_INPUT);

  cfg = m_config;
  cfg.m_trx.m_node_id = ut::HLC_MAX_NODES - 1;
  cfg.m_trx.m_epoch = ut::HLC_MAX_EPOCHS - 1;
  EXPECT_EQ(cfg.normalize(), DB_SUCCESS);
}

/** The node and epoch are the instance's, across a restart; epochs rise. */
TEST_F(Mvcc, NodeEpochStored) {
  stop();
  m_tmp = std::make_unique<ut::TestDir>();
  m_config = ut::make_config(m_tmp->m_path);
  m_config.m_trx.m_node_id = 7;
  m_config.m_trx.m_epoch = 3;
  start();

  EXPECT_EQ(ts().m_clock.node(), 7u);
  EXPECT_EQ(ts().m_clock.epoch(), 3u);

  auto t = begin();
  const auto id = t->m_id;

  EXPECT_EQ(ut::id_to_node(id), 7u);
  EXPECT_EQ(ut::id_to_epoch(id), 3u);
  commit(t);

  ts().clock_set_epoch(4);
  restart();

  EXPECT_EQ(ts().m_clock.epoch(), 4u);

  auto t2 = begin();

  EXPECT_GT(t2->m_id, id);
  EXPECT_EQ(ut::id_to_epoch(t2->m_id), 4u);
  commit(t2);
}

/** The public API: a received timestamp, a commit's and a view's. */
TEST_F(Mvcc, PublicClock) {
  auto &sys = *m_sys;
  const auto t0 = api::clock_next(sys);

  EXPECT_EQ(t0.epoch(), 1u);

  const ut::Timestamp future(1, t0.hlc() + 5000000, 3);

  EXPECT_EQ(api::clock_update(sys, future), DB_SUCCESS);
  EXPECT_GT(api::clock_next(sys), future);

  /* Below the base: nothing, and not an id. */
  const auto before = ts().m_clock.read();

  EXPECT_EQ(api::clock_update(sys, ut::Timestamp(1, 5, 0)), DB_SUCCESS);
  EXPECT_EQ(ts().m_clock.read(), before);

  trx_id_t id{};

  EXPECT_FALSE(ut::id_of_timestamp(ut::Timestamp(1, 5, 0), id));

  /* Past the last id: refused. */
  EXPECT_EQ(api::clock_update(sys, ut::Timestamp(1, ut::HLC_BASE_US + ut::HLC_MAX_TIME + 1, 0)), DB_INVALID_INPUT);
  EXPECT_EQ(ts().m_clock.read(), before);

  auto t = begin();
  const auto ts_id = api::trx_timestamp(t);

  EXPECT_EQ(ts_id, ut::id_to_timestamp(t->m_id));
  EXPECT_EQ(ts_id.node(), 0u);

  (void)t->assign_read_view(sys);

  const auto ts_view = api::trx_view_timestamp(sys, t);

  EXPECT_GE(ts_view, ts_id);

  commit(t);

  /* Commit stamp, after its id and any view of it. */
  auto w = begin();
  const auto wid = w->m_id;

  ASSERT_EQ(insert(w, row(9)), DB_SUCCESS);
  commit(w);

  EXPECT_GT(ut::id_to_hlc(w->m_commit_word), ut::id_to_hlc(wid));
}

}  // namespace
