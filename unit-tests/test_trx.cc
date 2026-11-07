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

/** @file unit-tests/test_trx.cc
TrxSys: transaction objects, ids, the list of transactions, read views, and
the transactions recovered from the undo logs at start up.

  TrxSysTest  in memory (SysTest): transactions put in the list by hand
              with ids of the test's choosing, read views over them.
  TrxDbTest   transactions of a started instance shared by the suite.
  TrxTest     an instance of its own: restarts and crashes.
  TrxPerf     performance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "mem0mem.h"
#include "read0read.h"
#include "trx0sys.h"
#include "trx0trx.h"

#include <algorithm>
#include <format>
#include <numeric>
#include <random>
#include <chrono>
#include <thread>

namespace {

/** An id of node 0 from a small number: the node of the instance. */
constexpr trx_id_t I(trx_id_t n) { return n * ut::HLC_ID_STEP; }

/** Transactions in the list with ids and states of the test's choosing. */
class TrxSysTest : public ut::SysTest {
 protected:
  void SetUp() override {
    ut::SysTest::SetUp();
    m_trx_sys = m_sys->m_trx_sys.get();
    m_heap = mem_heap_create(1024);
  }

  void TearDown() override {
    for (auto view : m_views) {
      m_trx_sys->close_read_view(view);
    }

    for (auto trx : m_trxs) {
      m_trx_sys->trx_list_remove(trx);
      trx->m_conc_state = TRX_NOT_STARTED;
      trx->m_read_view = nullptr;
    }

    for (auto &trx : m_trxs) {
      m_trx_sys->destroy_trx(trx);
    }

    mem_heap_free(m_heap);
    ut::SysTest::TearDown();
  }

  /** @return a transaction with id in state, in the list. */
  Trx *add(trx_id_t id, TrxStatus state = TRX_ACTIVE, trx_id_t no = LSN_MAX) {
    auto trx = m_trx_sys->create_trx(nullptr);

    trx->m_id = id;
    trx->m_no = no;
    trx->m_conc_state = state;

    mutex_enter(&m_trx_sys->m_mutex);
    m_trx_sys->trx_list_insert_ordered(trx);
    mutex_exit(&m_trx_sys->m_mutex);

    m_trxs.push_back(trx);

    return trx;
  }

  /** @return the ids in the list, in list order. */
  std::vector<trx_id_t> ids() {
    std::vector<trx_id_t> v;

    for (auto trx : m_trx_sys->m_recv_trx_list) {
      v.push_back(trx->m_id);
    }

    return v;
  }

  /** @return a read view opened now for creator. */
  ReadView *open(trx_id_t creator) {
    mutex_enter(&m_trx_sys->m_mutex);
    auto view = m_trx_sys->open_read_view_now(creator, m_heap);
    mutex_exit(&m_trx_sys->m_mutex);

    m_views.push_back(view);

    return view;
  }

  /** @return a view at the horizon, for purge. */
  ReadView *purge_view() {
    auto view = m_trx_sys->open_purge_view(m_heap);

    m_views.push_back(view);

    return view;
  }

  void close(ReadView *view) {
    mutex_enter(&m_trx_sys->m_mutex);
    m_trx_sys->close_read_view(view);
    mutex_exit(&m_trx_sys->m_mutex);

    std::erase(m_views, view);
  }

  TrxSys *m_trx_sys{};
  MemHeap *m_heap{};
  std::vector<Trx *> m_trxs;
  std::vector<ReadView *> m_views;
};

/** A transaction and its session point at each other, and go together. */
TEST_F(TrxSysTest, CreateDestroy) {
  auto trx_sys = m_sys->m_trx_sys.get();

  for (int i = 0; i < 100; ++i) {
    auto trx = trx_sys->create_user_trx(nullptr);

    ASSERT_NE(trx, nullptr);
    ASSERT_NE(trx->m_sess, nullptr);
    EXPECT_EQ(trx->m_sess->m_trx, trx);
    EXPECT_EQ(trx->m_conc_state, TRX_NOT_STARTED);

    trx_sys->destroy_user_trx(trx);
    EXPECT_EQ(trx, nullptr);
  }
}

/** The transaction list is in descending order of ids, whatever the order
of insertion: the read views depend on it. The recovered transactions were
inserted after the first one with a smaller id instead of before it: a
read view then missed an active recovered transaction and saw its changes. */
TEST_F(TrxSysTest, ListOrder) {
  auto trx_sys = m_sys->m_trx_sys.get();
  std::vector<Trx *> trxs;

  mutex_enter(&trx_sys->m_mutex);

  for (trx_id_t id : {1284, 1282, 1283, 1290, 1000, 1285, 1281}) {
    auto trx = trx_sys->create_trx(nullptr);

    trx->m_id = id;
    trx_sys->trx_list_insert_ordered(trx);
    trxs.push_back(trx);
  }

  std::vector<trx_id_t> ids;

  for (auto trx : trx_sys->m_recv_trx_list) {
    ids.push_back(trx->m_id);
  }

  EXPECT_EQ(ids, (std::vector<trx_id_t>{1290, 1285, 1284, 1283, 1282, 1281, 1000}));

  mutex_exit(&trx_sys->m_mutex);

  for (auto trx : trxs) {
    trx_sys->trx_list_remove(trx);
  }

  for (auto trx : trxs) {
    trx_sys->destroy_trx(trx);
  }
}

/** Any order of insertion gives the list in descending order of ids. */
TEST_F(TrxSysTest, ListOrderRandom) {
  std::vector<trx_id_t> v(200);

  std::iota(v.begin(), v.end(), 5000);
  std::shuffle(v.begin(), v.end(), std::mt19937(42));

  for (auto id : v) {
    (void)add(id);
  }

  std::ranges::sort(v, std::greater<>());
  EXPECT_EQ(ids(), v);
}

/** The first insert into an empty list, a new biggest id at the front, a
new smallest at the end, one in the middle. */
TEST_F(TrxSysTest, ListOrderEdges) {
  (void)add(100);
  EXPECT_EQ(ids(), (std::vector<trx_id_t>{100}));

  (void)add(200);
  EXPECT_EQ(ids(), (std::vector<trx_id_t>{200, 100}));

  (void)add(50);
  EXPECT_EQ(ids(), (std::vector<trx_id_t>{200, 100, 50}));

  (void)add(150);
  EXPECT_EQ(ids(), (std::vector<trx_id_t>{200, 150, 100, 50}));
}

/** get_min_trx_id() is the id at the end of the list, or the next id to
assign if the list is empty. */
TEST_F(TrxSysTest, MinTrxId) {
  m_trx_sys->set_max_trx_id(1000);

  mutex_enter(&m_trx_sys->m_mutex);
  EXPECT_EQ(m_trx_sys->get_min_trx_id(), 1000u);
  mutex_exit(&m_trx_sys->m_mutex);

  (void)add(700);
  (void)add(500);
  (void)add(900);

  mutex_enter(&m_trx_sys->m_mutex);
  EXPECT_EQ(m_trx_sys->get_min_trx_id(), 500u);
  mutex_exit(&m_trx_sys->m_mutex);
}

/** is_active(): an active or prepared transaction in the list is, a
committed one or an id not in the list is not; an id below the minimum is
not; an id from the future is reported active (corruption). */
TEST_F(TrxSysTest, IsActive) {
  m_trx_sys->set_max_trx_id(I(1000));

  auto active = add(I(500));
  auto prepared = add(I(600), TRX_PREPARED);
  auto committed = add(I(700), TRX_COMMITTED_IN_MEMORY);


  EXPECT_TRUE(m_trx_sys->is_active(I(500)));
  EXPECT_TRUE(m_trx_sys->is_active(I(600)));
  EXPECT_FALSE(m_trx_sys->is_active(I(700)));
  EXPECT_FALSE(m_trx_sys->is_active(I(650)));
  EXPECT_FALSE(m_trx_sys->is_active(I(499)));
  EXPECT_TRUE(m_trx_sys->is_active(I(1000)));

  EXPECT_EQ(m_trx_sys->get_on_id(I(500)), active);
  EXPECT_EQ(m_trx_sys->get_on_id(I(600)), prepared);
  EXPECT_EQ(m_trx_sys->get_on_id(I(700)), committed);
  EXPECT_EQ(m_trx_sys->get_on_id(I(650)), nullptr);

}

/** Ids and numbers come from one clock: strictly increasing, of this node, below
the high water mark. No header page here: the store of the high water is held off. */
TEST_F(TrxSysTest, NewTrxId) {
  m_trx_sys->m_hlc_stored.store(m_trx_sys->m_clock.wall() + 3600000000ULL);

  trx_id_t prev{};

  for (int i{}; i < 200; ++i) {
    const auto id = i % 2 == 0 ? m_trx_sys->get_new_trx_id() : m_trx_sys->get_new_trx_no(m_trx_sys->m_clock.wall());

    EXPECT_GT(id, prev);
    EXPECT_GE(id, prev + ut::HLC_ID_STEP) << "no id + 1";
    EXPECT_TRUE(m_trx_sys->id_is_local(id));
    EXPECT_LT(id, m_trx_sys->max_trx_id());
    prev = id;
  }
}

/** User and background transactions are counted, the user ones listed. */
TEST_F(TrxSysTest, Counts) {
  const ulint n_user = m_trx_sys->m_n_user_trx;
  const ulint n_bg = m_trx_sys->m_n_background_trx;

  auto user = m_trx_sys->create_user_trx(nullptr);
  auto bg = m_trx_sys->create_background_trx(nullptr);

  EXPECT_EQ(m_trx_sys->m_n_user_trx.load(), n_user + 1);
  EXPECT_EQ(m_trx_sys->m_n_background_trx.load(), n_bg + 1);

  size_t n_listed{};

  for (auto &shard : m_trx_sys->m_client_shards) {
    for (auto trx : shard.m_trxs) {
      n_listed += trx == user;
    }
  }

  EXPECT_EQ(n_listed, 1u);

  m_trx_sys->destroy_background_trx(bg);
  m_trx_sys->destroy_user_trx(user);

  EXPECT_EQ(m_trx_sys->m_n_user_trx.load(), n_user);
  EXPECT_EQ(m_trx_sys->m_n_background_trx.load(), n_bg);
  EXPECT_EQ(bg, nullptr);
}

/** With no transaction active a view sees every id up to its snapshot. */
TEST_F(TrxSysTest, ReadViewEmpty) {
  m_trx_sys->set_max_trx_id(I(1000));

  auto view = open(0);

  EXPECT_EQ(view->m_snap_word, I(999));
  EXPECT_EQ(view->m_type, ReadViewType::NORMAL);
  EXPECT_TRUE(view->changes_visible(0));
  EXPECT_TRUE(view->changes_visible(I(999)));
  EXPECT_FALSE(view->changes_visible(I(1000)));
  EXPECT_FALSE(view->changes_visible(I(5000)));
}

/** A view sees what committed before it and its creator's changes, not the
active ones nor the future ones. */
TEST_F(TrxSysTest, ReadViewIds) {
  m_trx_sys->set_max_trx_id(I(20));

  (void)add(I(5));
  (void)add(I(9));
  (void)add(I(12));

  auto view = open(I(9));

  EXPECT_EQ(view->m_creator_trx_id, I(9));
  EXPECT_EQ(view->m_watermark, I(5));

  const std::vector<std::pair<trx_id_t, bool>> cases{
    {0, true}, {I(4), true}, {I(5), false}, {I(6), true}, {I(9), true}, {I(11), true},
    {I(12), false}, {I(13), true}, {I(19), true}, {I(20), false}, {I(21), false},
  };

  for (auto [id, visible] : cases) {
    EXPECT_EQ(view->changes_visible(id), visible) << id;
  }
}

/** Committed in memory transactions are visible, prepared ones are not. */
TEST_F(TrxSysTest, ReadViewStates) {
  m_trx_sys->set_max_trx_id(I(100));

  (void)add(I(10), TRX_COMMITTED_IN_MEMORY, 50);
  (void)add(I(20), TRX_PREPARED);
  (void)add(I(30));

  auto view = open(0);

  EXPECT_TRUE(view->changes_visible(I(10)));
  EXPECT_FALSE(view->changes_visible(I(20)));
  EXPECT_FALSE(view->changes_visible(I(30)));
  EXPECT_EQ(view->m_watermark, I(20));
}

/** The limit number of purge is the smallest number of an unseen transaction in the
middle of its commit, else the next id. */
TEST_F(TrxSysTest, PurgeLowLimitNo) {
  m_trx_sys->set_max_trx_id(I(100));

  (void)add(I(10));
  (void)add(I(20), TRX_ACTIVE, I(95));
  (void)add(I(30), TRX_ACTIVE, I(97));

  EXPECT_EQ(purge_view()->m_low_limit_no, I(95));
}

/** Nothing committing: the limit is the high water mark. */
TEST_F(TrxSysTest, PurgeLimitNoneCommitting) {
  m_trx_sys->set_max_trx_id(I(100));

  (void)add(I(10));

  auto view = purge_view();

  EXPECT_EQ(view->m_low_limit_no, I(100));
  EXPECT_EQ(view->m_watermark, I(10));
  EXPECT_FALSE(view->changes_visible(I(10)));
}

/** A number in the making: T's id stands for it (THE NUMBER in trx0sys.h), the limit
can't be above a number not seen yet. */
TEST_F(TrxSysTest, PurgeNumberIntent) {
  m_trx_sys->set_max_trx_id(I(100));

  auto trx = add(I(10));

  m_trx_sys->node_no_store(trx, trx->m_id);

  EXPECT_EQ(purge_view()->m_low_limit_no, I(10));
}

/** Views are counted, each with its slot; closing one closes it only. */
TEST_F(TrxSysTest, ViewCount) {
  m_trx_sys->set_max_trx_id(I(100));

  const auto n = m_trx_sys->n_read_views();

  auto v1 = open(I(1));
  auto v2 = open(I(2));
  auto v3 = open(I(3));

  EXPECT_EQ(m_trx_sys->n_read_views(), n + 3);

  close(v2);

  EXPECT_EQ(m_trx_sys->n_read_views(), n + 2);
  EXPECT_NE(v1->m_slot, nullptr);
  EXPECT_NE(v3->m_slot, nullptr);
}

/** The purge view doesn't see past the oldest view; once that is closed it can. */
TEST_F(TrxSysTest, PurgeHorizon) {
  m_trx_sys->set_max_trx_id(I(100));

  auto t1 = add(I(10));
  auto oldest = open(0);

  m_trx_sys->m_hlc_stored.store(m_trx_sys->m_clock.wall() + 3600000000ULL);

  m_trx_sys->trx_list_remove(t1);

  auto held = purge_view();

  EXPECT_EQ(held->m_snap_word, oldest->m_snap_word);
  EXPECT_FALSE(held->changes_visible(I(10)));
  EXPECT_FALSE(oldest->changes_visible(I(10)));

  close(held);
  close(oldest);

  auto free = purge_view();

  EXPECT_TRUE(free->changes_visible(I(10)));
}

/** An id 2^31 or more above another one: compared as 64 bit integers. */
TEST_F(TrxSysTest, SeesLargeIdGap) {
  constexpr trx_id_t base = I(10);
  constexpr trx_id_t far = base + (trx_id_t(1) << 40);

  m_trx_sys->set_max_trx_id(far + I(100));

  (void)add(base);
  (void)add(far);

  auto view = open(0);

  EXPECT_FALSE(view->changes_visible(far));
  EXPECT_TRUE(view->changes_visible(far + I(1)));
}

/** A cursor view hides the changes of its creator too, is of high
granularity with the creator's undo number, and closing it gives the
transaction its global view back. */
TEST_F(TrxSysTest, CursorView) {
  m_trx_sys->set_max_trx_id(I(100));

  auto creator = add(I(40));

  (void)add(I(30));
  creator->m_undo_no = 7;

  auto global = open(I(40));

  creator->m_global_read_view = global;
  creator->m_read_view = global;

  const auto n = m_trx_sys->n_read_views();

  auto curview = m_trx_sys->create_cursor_view(creator);
  auto view = curview->read_view;

  EXPECT_EQ(m_trx_sys->n_read_views(), n + 1);
  EXPECT_EQ(view->m_type, ReadViewType::HIGH_GRANULARITY);
  EXPECT_EQ(view->m_undo_no, 7u);
  EXPECT_FALSE(view->changes_visible(I(40)));
  EXPECT_FALSE(view->changes_visible(I(30)));
  EXPECT_TRUE(global->changes_visible(I(40)));

  m_trx_sys->set_cursor_view(creator, curview);
  EXPECT_EQ(creator->m_read_view, view);

  m_trx_sys->close_cursor_view(creator, curview);

  EXPECT_EQ(creator->m_read_view, global);
  EXPECT_EQ(m_trx_sys->n_read_views(), n);

  creator->m_global_read_view = nullptr;
}

/** The deadlock victim is chosen by weight: undo records plus locks. */
TEST_F(TrxSysTest, WeightCmp) {
  auto a = add(10);
  auto b = add(11);

  a->m_undo_no = 5;
  b->m_undo_no = 3;

  EXPECT_GT(Trx::weight_cmp(a, b), 0);
  EXPECT_LT(Trx::weight_cmp(b, a), 0);

  b->m_undo_no = 5;

  EXPECT_EQ(Trx::weight_cmp(a, b), 0);
}

/* Regression: Trx::weight_cmp() returns lhs->weight() - rhs->weight(), a 64-bit
unsigned difference truncated to int (trx/trx0trx.cc:892; upstream
ut_dulint_cmp): a difference of 2^32 compares equal, one of 2^31 has the
wrong sign, and the deadlock check rolls back the heavy transaction. */
TEST_F(TrxSysTest, WeightCmpLarge) {
  auto a = add(10);
  auto b = add(11);

  a->m_undo_no = undo_no_t(1) << 32;
  b->m_undo_no = 0;

  EXPECT_GT(Trx::weight_cmp(a, b), 0);

  a->m_undo_no = undo_no_t(1) << 31;

  EXPECT_GT(Trx::weight_cmp(a, b), 0);
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

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;
};

/** Transactions of a started instance. */
using TrxDbTest = SharedDb;

/** Started transactions get increasing ids and are in the list, newest
first; the read view of one lists the others that are active. */
TEST_F(TrxDbTest, AssignReadView) {
  auto t1 = begin();
  auto t2 = begin();
  auto t3 = begin();
  auto trx_sys = m_sys->m_trx_sys.get();

  EXPECT_LT(t1->m_id, t2->m_id);
  EXPECT_LT(t2->m_id, t3->m_id);

  EXPECT_EQ(trx_sys->active_ids().front(), t3->m_id);
  EXPECT_TRUE(trx_sys->in_trx_list(t1));
  const auto max_trx_id = trx_sys->max_trx_id();

  auto view = t3->assign_read_view(*m_sys);

  ASSERT_NE(view, nullptr);
  EXPECT_EQ(t3->assign_read_view(*m_sys), view) << "assigned once";
  EXPECT_EQ(t3->m_global_read_view, view);
  EXPECT_EQ(view->m_creator_trx_id, t3->m_id);
  EXPECT_LE(max_trx_id, ut::word_id_limit(view->m_snap_word));
  EXPECT_FALSE(view->changes_visible(t2->m_id));
  EXPECT_TRUE(view->changes_visible(t3->m_id));
  EXPECT_FALSE(view->changes_visible(t1->m_id));

  commit(t1);
  commit(t2);
  commit(t3);
}

/** A commit takes the transaction out of the list and closes its view. */
TEST_F(TrxDbTest, CommitCleansUp) {
  auto trx = begin();
  auto trx_sys = m_sys->m_trx_sys.get();

  ASSERT_EQ(insert(trx, ut::Row{1, 1, "x"}), DB_SUCCESS);

  const auto id = trx->m_id;
  (void)trx->assign_read_view(*m_sys);

  const auto n_views = trx_sys->n_read_views();

  EXPECT_GE(n_views, 1u);

  commit(trx);

  EXPECT_EQ(trx_sys->get_on_id(id), nullptr);

  /* Purge takes a slot for the moment it opens its view: the count settles below the first. */
  size_t after{};

  for (int i{}; i < 10000 && (after = trx_sys->n_read_views()) >= n_views; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  /* What was seen, not another look: that could catch purge again. */
  EXPECT_LT(after, n_views);
}

/** REPEATABLE READ: the reader sees the rows as of its first read, and its
own changes; others see its changes once it commits. */
TEST_F(TrxDbTest, Snapshot) {
  load({{1, ut::Row{1, 1, "a"}}});

  auto reader = begin(IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(scan(reader).size(), 1u);

  auto writer = begin();

  ASSERT_EQ(insert(writer, ut::Row{2, 2, "b"}), DB_SUCCESS);
  ASSERT_EQ(update(writer, 1, ut::Row{1, 10, "c"}), DB_SUCCESS);

  EXPECT_EQ(scan(writer).size(), 2u) << "its own changes";
  EXPECT_EQ(scan(reader), (ut::Rows{{1, ut::Row{1, 1, "a"}}})) << "uncommitted";

  commit(writer);

  EXPECT_EQ(scan(reader), (ut::Rows{{1, ut::Row{1, 1, "a"}}})) << "committed after the view";

  ASSERT_EQ(insert(reader, ut::Row{3, 3, "r"}), DB_SUCCESS);
  EXPECT_EQ(scan(reader).size(), 2u) << "its own insert";

  commit(reader);

  expect_rows({{1, ut::Row{1, 10, "c"}}, {2, ut::Row{2, 2, "b"}}, {3, ut::Row{3, 3, "r"}}});
}

/** READ COMMITTED: the API keeps the view of the first read until the
statement end closes it (close_read_view_for_read_committed(), which the
API leaves to the application, as upstream did); the next read then sees
what committed in the meantime. REPEATABLE READ keeps its view. */
TEST_F(TrxDbTest, ReadCommitted) {
  load({{1, ut::Row{1, 1, "a"}}});

  auto rc = begin(IB_TRX_READ_COMMITTED);
  auto rr = begin(IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(scan(rc).size(), 1u);
  ASSERT_EQ(scan(rr).size(), 1u);

  auto writer = begin();

  ASSERT_EQ(insert(writer, ut::Row{2, 2, "b"}), DB_SUCCESS);
  commit(writer);

  m_sys->m_trx_sys->close_read_view_for_read_committed(rc);

  /* Not a count of the views: purge opens and closes its own at any time. */
  EXPECT_EQ(rc->m_read_view, nullptr);
  EXPECT_EQ(rc->m_global_read_view, nullptr);

  EXPECT_EQ(scan(rc).size(), 2u);
  EXPECT_EQ(scan(rr).size(), 1u);

  commit(rc);
  commit(rr);
}

/** Ids keep growing over more transactions than the margin after which
the counter is written to the system header. */
TEST_F(TrxDbTest, IdsOverMargin) {
  trx_id_t prev{};

  for (uint32_t i = 0; i < 1000; ++i) {
    auto trx = begin();

    EXPECT_GT(trx->m_id, prev);
    prev = trx->m_id;
    commit(trx);
  }
}

/** Read views over in-memory transactions, timed. */
class TrxSysPerf : public TrxSysTest {};

/** Open and close a read view with n active transactions. */
TEST_F(TrxSysPerf, ReadViewOpenClose) {
  m_trx_sys->set_max_trx_id(100000);

  trx_id_t id{10};

  for (ulint n : {0, 10, 100, 1000}) {
    while (m_trx_sys->m_recv_trx_list.size() < n) {
      (void)add(I(id++));
    }

    auto r = ut::bench(std::format("read_view_open_close_n{}", n), 20000, [&](uint64_t) {
      auto view = m_trx_sys->open_read_view_now(0, m_heap);
      m_trx_sys->close_read_view(view);
      mem_heap_empty(m_heap);
    });

    EXPECT_LT(r.ns_per_op(), 2000.0 + 400.0 * n);
  }
}

/** Visibility checks against a view of 100 active transactions. */
TEST_F(TrxSysPerf, ChangesVisible) {
  m_trx_sys->set_max_trx_id(10000);

  for (trx_id_t id = 100; id < 300; id += 2) {
    (void)add(id);
  }

  auto view = open(0);
  uint64_t n_visible{};

  auto r = ut::bench("changes_visible_100", 1000000, [&](uint64_t i) { n_visible += view->changes_visible(50 + i % 300); });

  ut::keep(n_visible);
  EXPECT_LT(r.ns_per_op(), 5000);
}

/** Transactions of a started instance, timed. */
class TrxPerf : public SharedDb {};

/** Begin and commit a transaction that changes nothing. */
TEST_F(TrxPerf, BeginCommitRo) {
  auto r = ut::bench("begin_commit_ro", 20000, [&](uint64_t) { commit(begin()); });

  EXPECT_LT(r.ns_per_op(), 200000);
}

/** Begin, insert a row and commit. */
TEST_F(TrxPerf, BeginInsertCommit) {
  auto r = ut::bench("begin_insert_commit", 3000, [&](uint64_t i) {
    auto trx = begin();

    EXPECT_EQ(insert(trx, ut::Row{uint32_t(i), 0, ""}), DB_SUCCESS);
    commit(trx);
  });

  EXPECT_LT(r.ns_per_op(), 10000000);
}

using TrxTest = ut::DbTest;

/** Ids grow with the start of each transaction. */
TEST_F(TrxTest, Ids) {
  trx_id_t prev{};

  for (int i = 0; i < 50; ++i) {
    auto trx = begin();

    ASSERT_EQ(insert(trx, ut::Row{uint32_t(i), 0, ""}), DB_SUCCESS);
    EXPECT_GT(trx->m_id, prev);
    prev = trx->m_id;

    commit(trx);
  }
}

/** Ids keep growing after a restart and after a crash. */
TEST_F(TrxTest, IdsAfterRestart) {
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{1, 0, ""}), DB_SUCCESS);
  const auto before = trx->m_id;
  commit(trx);

  restart();

  trx = begin();
  ASSERT_EQ(insert(trx, ut::Row{2, 0, ""}), DB_SUCCESS);
  EXPECT_GT(trx->m_id, before);
  const auto after_restart = trx->m_id;
  commit(trx);

  crash([this] {
    for (uint32_t k = 10; k < 20; ++k) {
      auto t = begin();

      (void)insert(t, ut::Row{k, 0, ""});
      commit(t);
    }
  });

  trx = begin();
  ASSERT_EQ(insert(trx, ut::Row{3, 0, ""}), DB_SUCCESS);
  EXPECT_GT(trx->m_id, after_restart + 10);
  commit(trx);
}

/** Transactions recovered from undo logs of one kind only: a transaction
that only updated has no insert undo log. It was created without a
session and its rollback crashed. */
TEST_F(TrxTest, RecoverUpdateOnly) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 50; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  crash([this] {
    auto upd = begin(IB_TRX_READ_COMMITTED);

    for (uint32_t k = 0; k < 50; k += 2) {
      EXPECT_EQ(update(upd, k, ut::Row{k, k + 100, ut::blob(k, 9000)}), DB_SUCCESS);
    }

    auto del = begin(IB_TRX_READ_COMMITTED);

    for (uint32_t k = 1; k < 50; k += 4) {
      EXPECT_EQ(remove(del, k), DB_SUCCESS);
    }

    auto ins = begin(IB_TRX_READ_COMMITTED);

    for (uint32_t k = 100; k < 120; ++k) {
      EXPECT_EQ(insert(ins, ut::Row{k, 1, "new"}), DB_SUCCESS);
    }

    auto flush = begin(IB_TRX_READ_COMMITTED);

    EXPECT_EQ(insert(flush, ut::Row{1000, 0, "flush"}), DB_SUCCESS);
    commit(flush);
  });

  rows[1000] = ut::Row{1000, 0, "flush"};

  expect_rows(rows);
  restart();
  expect_rows(rows);
}

/** A trx that is rolled back or committed, with and without changes.
(trx_begin() starts the trx: only a trx that never started can be released.) */
TEST_F(TrxTest, EndStates) {
  auto trx = begin();
  commit(trx);

  trx = begin();
  rollback(trx);

  trx = begin();
  ASSERT_EQ(insert(trx, ut::Row{1, 1, "x"}), DB_SUCCESS);
  rollback(trx);

  expect_rows({});
}

/** One recovered transaction that updated rows: the update is undone. */
class RecoverUpdate : public ut::DbTest, public ::testing::WithParamInterface<int> {};

TEST_P(RecoverUpdate, Undone) {
  const auto what = GetParam();
  ut::Rows rows;

  for (uint32_t k = 0; k < 20; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  crash([&] {
    auto upd = begin(IB_TRX_READ_COMMITTED);

    for (uint32_t k = 0; k < 20; k += 2) {
      auto row = rows[k];

      if (what & 1) {
        row.m_a += 100;
      }

      if (what & 2) {
        row.m_b = ut::blob(k, 9000);
      }

      if (what & 4) {
        row.m_b = ut::blob(k, 50);
      }

      EXPECT_EQ(update(upd, k, row), DB_SUCCESS);
    }

    auto flush = begin(IB_TRX_READ_COMMITTED);

    EXPECT_EQ(insert(flush, ut::Row{1000, 0, "flush"}), DB_SUCCESS);
    commit(flush);
  });

  rows[1000] = ut::Row{1000, 0, "flush"};

  expect_rows(rows);
  restart();
  expect_rows(rows);
}

/* 1: the secondary key, 2: the blob goes off page, 3: both, 4: the blob
changes in the page. */
INSTANTIATE_TEST_SUITE_P(TrxTest, RecoverUpdate, ::testing::Values(1, 2, 3, 4));

}  // namespace
