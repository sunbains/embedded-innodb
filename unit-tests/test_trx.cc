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
#include <thread>

namespace {

/** Transactions in the list with ids and states of the test's choosing. */
class TrxSysTest : public ut::SysTest {
 protected:
  void SetUp() override {
    ut::SysTest::SetUp();
    m_trx_sys = m_sys->m_trx_sys.get();
    m_heap = mem_heap_create(1024);
  }

  void TearDown() override {
    mutex_enter(&m_trx_sys->m_mutex);

    for (auto view : m_views) {
      m_trx_sys->close_read_view(view);
    }

    for (auto trx : m_trxs) {
      m_trx_sys->m_trx_list.remove(trx);
      trx->m_conc_state = TRX_NOT_STARTED;
      trx->m_read_view = nullptr;
    }

    mutex_exit(&m_trx_sys->m_mutex);

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

    for (auto trx : m_trx_sys->m_trx_list) {
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

  /** @return a copy of the oldest view, or a new one, for purge. */
  ReadView *oldest_copy() {
    mutex_enter(&m_trx_sys->m_mutex);
    auto view = m_trx_sys->oldest_copy_or_open_new(0, m_heap);
    mutex_exit(&m_trx_sys->m_mutex);

    m_views.push_back(view);

    return view;
  }

  void close(ReadView *view) {
    mutex_enter(&m_trx_sys->m_mutex);
    m_trx_sys->close_read_view(view);
    mutex_exit(&m_trx_sys->m_mutex);

    std::erase(m_views, view);
  }

  /** @return the trx ids of view, in its order. */
  static std::vector<trx_id_t> view_ids(const ReadView *view) {
    std::vector<trx_id_t> v;

    for (ulint i{}; i < view->m_n_trx_ids; ++i) {
      v.push_back(view->get_nth_trx_id(i));
    }

    return v;
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

  for (auto trx : trx_sys->m_trx_list) {
    ids.push_back(trx->m_id);
  }

  EXPECT_EQ(ids, (std::vector<trx_id_t>{1290, 1285, 1284, 1283, 1282, 1281, 1000}));

  for (auto trx : trxs) {
    trx_sys->m_trx_list.remove(trx);
  }

  mutex_exit(&trx_sys->m_mutex);

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
  m_trx_sys->m_max_trx_id = 1000;

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
  m_trx_sys->m_max_trx_id = 1000;

  auto active = add(500);
  auto prepared = add(600, TRX_PREPARED);
  auto committed = add(700, TRX_COMMITTED_IN_MEMORY);

  mutex_enter(&m_trx_sys->m_mutex);

  EXPECT_TRUE(m_trx_sys->is_active(500));
  EXPECT_TRUE(m_trx_sys->is_active(600));
  EXPECT_FALSE(m_trx_sys->is_active(700));
  EXPECT_FALSE(m_trx_sys->is_active(650));
  EXPECT_FALSE(m_trx_sys->is_active(499));
  EXPECT_TRUE(m_trx_sys->is_active(1000));

  EXPECT_EQ(m_trx_sys->get_on_id(500), active);
  EXPECT_EQ(m_trx_sys->get_on_id(600), prepared);
  EXPECT_EQ(m_trx_sys->get_on_id(700), committed);
  EXPECT_EQ(m_trx_sys->get_on_id(650), nullptr);

  mutex_exit(&m_trx_sys->m_mutex);
}

/** Ids are handed out in sequence (away from the margin where the counter
is written to the system header). */
TEST_F(TrxSysTest, NewTrxId) {
  m_trx_sys->m_max_trx_id = 1025;

  mutex_enter(&m_trx_sys->m_mutex);

  for (trx_id_t expected = 1025; expected < 1225; ++expected) {
    EXPECT_EQ(m_trx_sys->get_new_trx_id(), expected);
  }

  EXPECT_EQ(m_trx_sys->get_new_trx_no(), 1225u);
  EXPECT_EQ(m_trx_sys->m_max_trx_id, 1226u);

  mutex_exit(&m_trx_sys->m_mutex);
}

/** User and background transactions are counted, the user ones listed. */
TEST_F(TrxSysTest, Counts) {
  const auto n_user = m_trx_sys->m_n_user_trx;
  const auto n_bg = m_trx_sys->m_n_background_trx;

  auto user = m_trx_sys->create_user_trx(nullptr);
  auto bg = m_trx_sys->create_background_trx(nullptr);

  EXPECT_EQ(m_trx_sys->m_n_user_trx, n_user + 1);
  EXPECT_EQ(m_trx_sys->m_n_background_trx, n_bg + 1);
  EXPECT_EQ(m_trx_sys->m_client_trx_list.front(), user);

  m_trx_sys->destroy_background_trx(bg);
  m_trx_sys->destroy_user_trx(user);

  EXPECT_EQ(m_trx_sys->m_n_user_trx, n_user);
  EXPECT_EQ(m_trx_sys->m_n_background_trx, n_bg);
  EXPECT_EQ(bg, nullptr);
}

/** With no transaction active a view sees every id below the next one. */
TEST_F(TrxSysTest, ReadViewEmpty) {
  m_trx_sys->m_max_trx_id = 1000;

  auto view = open(0);

  EXPECT_EQ(view->m_n_trx_ids, 0u);
  EXPECT_EQ(view->m_up_limit_id, 1000u);
  EXPECT_EQ(view->m_low_limit_id, 1000u);
  EXPECT_EQ(view->m_low_limit_no, 1000u);
  EXPECT_EQ(view->m_type, ReadViewType::NORMAL);
  EXPECT_TRUE(view->changes_visible(0));
  EXPECT_TRUE(view->changes_visible(999));
  EXPECT_FALSE(view->changes_visible(1000));
  EXPECT_FALSE(view->changes_visible(5000));
}

/** A view lists exactly the other active transactions, in descending
order; it sees what committed before it and its creator's changes, not the
active ones nor the future ones. */
TEST_F(TrxSysTest, ReadViewIds) {
  m_trx_sys->m_max_trx_id = 20;

  (void)add(5);
  (void)add(9);
  (void)add(12);

  auto view = open(9);

  EXPECT_EQ(view_ids(view), (std::vector<trx_id_t>{12, 5}));
  EXPECT_EQ(view->m_creator_trx_id, 9u);
  EXPECT_EQ(view->m_up_limit_id, 5u);
  EXPECT_EQ(view->m_low_limit_id, 20u);

  const std::vector<std::pair<trx_id_t, bool>> cases{
    {0, true}, {4, true}, {5, false}, {6, true}, {9, true}, {11, true},
    {12, false}, {13, true}, {19, true}, {20, false}, {21, false},
  };

  for (auto [id, visible] : cases) {
    EXPECT_EQ(view->changes_visible(id), visible) << id;
  }
}

/** Committed in memory transactions are not in a view, prepared ones are. */
TEST_F(TrxSysTest, ReadViewStates) {
  m_trx_sys->m_max_trx_id = 100;

  (void)add(10, TRX_COMMITTED_IN_MEMORY, 50);
  (void)add(20, TRX_PREPARED);
  (void)add(30);

  auto view = open(0);

  EXPECT_EQ(view_ids(view), (std::vector<trx_id_t>{30, 20}));
  EXPECT_TRUE(view->changes_visible(10));
  EXPECT_FALSE(view->changes_visible(20));
  EXPECT_EQ(view->m_up_limit_id, 20u);
}

/** The low limit number is the smallest number of an active transaction
in the middle of its commit, else the next id. */
TEST_F(TrxSysTest, ReadViewLowLimitNo) {
  m_trx_sys->m_max_trx_id = 100;

  (void)add(10);
  (void)add(20, TRX_ACTIVE, 95);
  (void)add(30, TRX_ACTIVE, 97);

  EXPECT_EQ(open(0)->m_low_limit_no, 95u);
}

/** Views are listed newest first; closing one removes it only. */
TEST_F(TrxSysTest, ViewList) {
  m_trx_sys->m_max_trx_id = 100;

  auto v1 = open(1);
  auto v2 = open(2);
  auto v3 = open(3);

  EXPECT_EQ(m_trx_sys->m_view_list.front(), v3);
  EXPECT_EQ(m_trx_sys->m_view_list.back(), v1);
  EXPECT_EQ(m_trx_sys->m_view_list.size(), 3u);

  close(v2);

  EXPECT_EQ(m_trx_sys->m_view_list.size(), 2u);
  EXPECT_EQ(m_trx_sys->m_view_list.front(), v3);
  EXPECT_EQ(m_trx_sys->m_view_list.back(), v1);
}

/** The purge view is a copy of the oldest view that also excludes the
creator of that view, inserted in its place in the descending ids. */
TEST_F(TrxSysTest, OldestCopy) {
  m_trx_sys->m_max_trx_id = 100;

  (void)add(10);
  (void)add(20);
  (void)add(30);
  (void)add(40);

  for (auto [creator, expected] : {
         std::pair{trx_id_t(40), std::vector<trx_id_t>{40, 30, 20, 10}},
         std::pair{trx_id_t(20), std::vector<trx_id_t>{40, 30, 20, 10}},
         std::pair{trx_id_t(10), std::vector<trx_id_t>{40, 30, 20, 10}},
       }) {
    auto oldest = open(creator);
    auto copy = oldest_copy();

    EXPECT_EQ(view_ids(copy), expected) << creator;
    EXPECT_EQ(copy->m_creator_trx_id, 0u);
    EXPECT_EQ(copy->m_up_limit_id, 10u);
    EXPECT_EQ(copy->m_low_limit_id, oldest->m_low_limit_id);
    EXPECT_EQ(copy->m_low_limit_no, oldest->m_low_limit_no);
    EXPECT_FALSE(copy->changes_visible(creator));
    EXPECT_EQ(m_trx_sys->m_view_list.back(), copy) << "the copy is the oldest now";

    close(copy);
    close(oldest);
  }
}

/** No view open: the purge view is a new one. */
TEST_F(TrxSysTest, OldestCopyNoView) {
  m_trx_sys->m_max_trx_id = 100;

  (void)add(10);

  auto view = oldest_copy();

  EXPECT_EQ(view_ids(view), (std::vector<trx_id_t>{10}));
  EXPECT_EQ(view->m_low_limit_id, 100u);
}

/* Regression: oldest_copy_or_open_new() makes n = 0 when the oldest view has no
creator (a purge view, creator id 0): the copy loses all the ids of the
active transactions and sees their changes (trx/trx0sys.cc:733; upstream
kept n = old_view->n_trx_ids and only skipped the insert of the creator). */
TEST_F(TrxSysTest, OldestCopyOfPurgeView) {
  m_trx_sys->m_max_trx_id = 100;

  (void)add(10);
  (void)add(20);

  auto purge = open(0);
  auto copy = oldest_copy();

  EXPECT_EQ(view_ids(copy), view_ids(purge));
  EXPECT_FALSE(copy->changes_visible(20));
  EXPECT_FALSE(copy->changes_visible(10));
}

/* Regression: ReadView::sees_trx_id() compares trx_id with the ids of the view by
int cmp = trx_id - id: the 64-bit difference truncated to int
(include/read0read.h:82; upstream ut_dulint_cmp). An id 2^31 or more above
the smallest id of the view compares as smaller: here an active
transaction's changes are seen. */
TEST_F(TrxSysTest, SeesLargeIdGap) {
  constexpr trx_id_t base = 10;
  constexpr trx_id_t far = base + (trx_id_t(1) << 31);

  m_trx_sys->m_max_trx_id = far + 100;

  (void)add(base);
  (void)add(far);

  auto view = open(0);

  EXPECT_FALSE(view->changes_visible(far));
  EXPECT_TRUE(view->changes_visible(far + 1));
}

/** A cursor view hides the changes of its creator too, is of high
granularity with the creator's undo number, and closing it gives the
transaction its global view back. */
TEST_F(TrxSysTest, CursorView) {
  m_trx_sys->m_max_trx_id = 100;

  auto creator = add(40);

  (void)add(30);
  creator->m_undo_no = 7;

  auto global = open(40);

  creator->m_global_read_view = global;
  creator->m_read_view = global;

  auto curview = m_trx_sys->create_cursor_view(creator);
  auto view = curview->read_view;

  EXPECT_EQ(view_ids(view), (std::vector<trx_id_t>{40, 30}));
  EXPECT_EQ(view->m_type, ReadViewType::HIGH_GRANULARITY);
  EXPECT_EQ(view->m_undo_no, 7u);
  EXPECT_FALSE(view->changes_visible(40));
  EXPECT_TRUE(global->changes_visible(40));

  m_trx_sys->set_cursor_view(creator, curview);
  EXPECT_EQ(creator->m_read_view, view);

  m_trx_sys->close_cursor_view(creator, curview);

  EXPECT_EQ(creator->m_read_view, global);
  EXPECT_EQ(m_trx_sys->m_view_list.size(), 1u);

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

  mutex_enter(&trx_sys->m_mutex);
  EXPECT_EQ(trx_sys->m_trx_list.front(), t3);
  EXPECT_TRUE(trx_sys->in_trx_list(t1));
  const auto max_trx_id = trx_sys->m_max_trx_id;
  mutex_exit(&trx_sys->m_mutex);

  auto view = t3->assign_read_view(*m_sys);

  ASSERT_NE(view, nullptr);
  EXPECT_EQ(t3->assign_read_view(*m_sys), view) << "assigned once";
  EXPECT_EQ(t3->m_global_read_view, view);
  EXPECT_EQ(view->m_creator_trx_id, t3->m_id);
  EXPECT_EQ(view->m_low_limit_id, max_trx_id);

  std::vector<trx_id_t> ids;

  for (ulint i{}; i < view->m_n_trx_ids; ++i) {
    ids.push_back(view->get_nth_trx_id(i));
  }

  EXPECT_EQ(ids, (std::vector<trx_id_t>{t2->m_id, t1->m_id}));
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
  const auto view = trx->assign_read_view(*m_sys);

  commit(trx);

  mutex_enter(&trx_sys->m_mutex);

  EXPECT_EQ(trx_sys->get_on_id(id), nullptr);

  for (auto v : trx_sys->m_view_list) {
    EXPECT_NE(v, view);
  }

  mutex_exit(&trx_sys->m_mutex);
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

  const auto n_views = m_sys->m_trx_sys->m_view_list.size();

  m_sys->m_trx_sys->close_read_view_for_read_committed(rc);

  EXPECT_EQ(rc->m_read_view, nullptr);
  EXPECT_EQ(rc->m_global_read_view, nullptr);
  EXPECT_EQ(m_sys->m_trx_sys->m_view_list.size(), n_views - 1);

  EXPECT_EQ(scan(rc).size(), 2u);
  EXPECT_EQ(scan(rr).size(), 1u);

  commit(rc);
  commit(rr);
}

/** Ids keep growing over more transactions than the margin after which
the counter is written to the system header. */
TEST_F(TrxDbTest, IdsOverMargin) {
  trx_id_t prev{};

  for (uint32_t i = 0; i < 2 * TRX_SYS_TRX_ID_WRITE_MARGIN + 10; ++i) {
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
  m_trx_sys->m_max_trx_id = 100000;

  trx_id_t id{10};

  for (ulint n : {0, 10, 100, 1000}) {
    while (m_trx_sys->m_trx_list.size() < n) {
      (void)add(id++);
    }

    auto r = ut::bench(std::format("read_view_open_close_n{}", n), 20000, [&](uint64_t) {
      mutex_enter(&m_trx_sys->m_mutex);
      auto view = m_trx_sys->open_read_view_now(0, m_heap);
      m_trx_sys->close_read_view(view);
      mutex_exit(&m_trx_sys->m_mutex);
      mem_heap_empty(m_heap);
    });

    EXPECT_LT(r.ns_per_op(), 2000.0 + 400.0 * n);
  }
}

/** Visibility checks against a view of 100 active transactions. */
TEST_F(TrxSysPerf, ChangesVisible) {
  m_trx_sys->m_max_trx_id = 10000;

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
