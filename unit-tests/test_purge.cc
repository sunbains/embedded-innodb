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

/** @file unit-tests/test_purge.cc
Purge and the rollback segments: PurgeSys::run() removes the delete marked
records of the clustered and the secondary indexes and frees the externally
stored fields of the old versions once no read view needs them, truncates
the history list and frees the undo log segments; the rollback segment
header keeps the undo log slots, the history list and its size.

The master thread runs purge too: a test that looks at the records before
purge keeps a read view open (a "blocker") that holds purge back, then
closes it and runs purge itself, PurgeSys::run() serializes with the master.

  PurgeTest        an instance shared by the suite, the test table emptied
                   and purged before each test.
  RsegTest         the rollback segment of the shared instance.
  RsegSlotsTest    an instance of its own: all the undo slots used.
  PurgeRestartTest an instance of its own: restart, crash.
  PurgePerf        performance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "idx0am.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "read0read.h"
#include "trx0purge.h"
#include "trx0rseg.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0undo.h"

#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <thread>

using namespace std::chrono_literals;

namespace {

/** Rows [from, to) with a = k % 7 and a blob of blob_len bytes. */
ut::Rows make_rows(uint32_t from, uint32_t to, size_t blob_len = 50) {
  ut::Rows rows;

  for (auto k = from; k < to; ++k) {
    rows[k] = ut::Row{k, k % 7, ut::blob(k, blob_len)};
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

/** Physical records of an index: delete marked or not. */
struct Counts {
  size_t m_n{};
  size_t m_marked{};

  bool operator==(const Counts &) const = default;
};

std::ostream &operator<<(std::ostream &os, const Counts &c) {
  return os << "{n " << c.m_n << ", marked " << c.m_marked << "}";
}

/** A DbTest whose started instance is shared by the tests of a suite: the
first test starts it, the end of the suite shuts it down. */
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

/** The helpers of the purge tests, for any instance of a DbTest. */
template <typename Base>
class PurgeBase : public Base {
 protected:
  using Base::m_sys;
  using Base::begin;
  using Base::commit;
  using Base::expect_rows;
  using Base::remove;
  using Base::scan;

  /** Delete all the rows of the test table and purge them. */
  void clear() {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (const auto &[k, row] : scan(trx)) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
    purge();
  }

  Table *table() {
    m_sys->m_dict->mutex_acquire();
    auto table = m_sys->m_dict->table_get(ut::TABLE);
    m_sys->m_dict->mutex_release();

    return table;
  }

  Index *clust() { return table()->get_first_index(); }

  Index *sec() { return clust()->get_next(); }

  /** @return the physical records of index. */
  Counts count(Index *index) {
    Counts c;
    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      ++c.m_n;
      c.m_marked += pcur.get_rec().get_deleted_flag() ? 1 : 0;
    }

    pcur.close();
    mtr.commit(*m_sys);

    return c;
  }

  /** @return the (a, k) of the SEC_A records, with their delete mark. */
  std::vector<std::tuple<uint32_t, uint32_t, bool>> sec_entries() {
    std::vector<std::tuple<uint32_t, uint32_t, bool>> v;
    Mtr mtr;
    BtreePcursor pcur;
    MemHeap *heap{};
    auto index = sec();

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      auto rec = pcur.get_rec();
      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      ulint len;

      const auto a = mach_read_from_4(rec.get_nth_field(offs, 0, &len));
      const auto k = mach_read_from_4(rec.get_nth_field(offs, 1, &len));

      v.emplace_back(uint32_t(a), uint32_t(k), rec.get_deleted_flag());
    }

    pcur.close();
    mtr.commit(*m_sys);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    return v;
  }

  /** Run purge until it has nothing left to do, as the master thread does.
  @return the number of batches that handled something */
  ulint purge() {
    ulint n{};

    for (int i = 0; i < 100000 && m_sys->m_trx_sys->m_purge->run() > 0; ++i) {
      ++n;
    }

    /* The batch that finds nothing truncates the history. */
    (void)m_sys->m_trx_sys->m_purge->run();

    return n;
  }

  /** @return true if purge has handled every log in the history. */
  bool all_purged() {
    for (auto rseg : m_sys->m_trx_sys->m_rseg_list) {
      mutex_enter(&rseg->m_mutex);
      const auto last = rseg->m_last_page_no;
      mutex_exit(&rseg->m_mutex);

      if (last != FIL_NULL) {
        return false;
      }
    }

    return !m_sys->m_trx_sys->m_purge->m_next_stored;
  }

  /** Purge everything, waiting for the views of other threads (the
  instance's own, after a start) to go: up to a minute. */
  void purge_all() {
    const auto start = std::chrono::steady_clock::now();

    for (;;) {
      purge();

      if (all_purged() || std::chrono::steady_clock::now() - start > 60s) {
        break;
      }

      std::this_thread::sleep_for(10ms);
    }

    EXPECT_TRUE(all_purged());
  }

  /** @return the length of the history list. */
  ulint hist_len() {
    mutex_enter(&m_sys->m_trx_sys->m_mutex);
    const ulint len = m_sys->m_trx_sys->m_rseg_history_len;
    mutex_exit(&m_sys->m_trx_sys->m_mutex);

    return len;
  }

  /** The rollback segment headers summed up. */
  struct RsegHdr {
    /** TRX_RSEG_HISTORY_SIZE: pages of the logs in the history. */
    ulint m_hist_size{};

    /** Length of the TRX_RSEG_HISTORY list. */
    ulint m_hist_len{};

    /** Undo slots in use. */
    ulint m_used_slots{};

    /** In memory: m_curr_size. */
    ulint m_curr_size{};

    /** In memory: cached undo logs, insert and update. */
    ulint m_cached_ins{};
    ulint m_cached_upd{};

    /** In memory: pages of the cached and active undo logs. */
    ulint m_undo_pages{};
  };

  RsegHdr rseg_hdr() {
    RsegHdr h;

    for (auto rseg : m_sys->m_trx_sys->m_rseg_list) {
      Mtr mtr;

      mtr.start();
      mutex_enter(&rseg->m_mutex);

      auto hdr = trx_rsegf_get(*m_sys, rseg->m_space, rseg->m_page_no, &mtr);

      h.m_hist_size += mtr.read_ulint(hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES);
      h.m_hist_len += flst_get_len(hdr + TRX_RSEG_HISTORY, &mtr);

      for (ulint i{}; i < TRX_RSEG_N_SLOTS; ++i) {
        h.m_used_slots += trx_rsegf_get_nth_undo(hdr, i, &mtr) != FIL_NULL ? 1 : 0;
      }

      h.m_curr_size += rseg->m_curr_size;
      h.m_cached_ins += UT_LIST_GET_LEN(rseg->m_insert_undo_cached);
      h.m_cached_upd += UT_LIST_GET_LEN(rseg->m_update_undo_cached);

      for (auto list : {&rseg->m_insert_undo_cached, &rseg->m_update_undo_cached, &rseg->m_insert_undo_list,
                        &rseg->m_update_undo_list}) {
        for (auto undo : *list) {
          h.m_undo_pages += undo->m_size;
        }
      }

      mutex_exit(&rseg->m_mutex);
      mtr.commit(*m_sys);
    }

    return h;
  }

  /** @return the used pages of the leaf segment of the clustered index,
  where the externally stored fields are. */
  ulint leaf_pages() { return m_sys->am(clust()).n_data_pages(clust()); }

  /** Check the history after a complete purge: every log left in the
  history lists has been purged (its trx number is below the purge's), the
  segments of those not cached are freed (TRX_RSEG_HISTORY_SIZE is 0) and
  nothing is left to purge in any rollback segment. The logs of the cached
  segments stay in the list after purge until a newer log is added (the
  truncation stops at the first log it may not remove, there is none): as
  upstream.
  @return the number of logs left */
  ulint expect_history_purged() {
    ulint n{};
    const auto purge_trx_no = m_sys->m_trx_sys->m_purge->m_purge_trx_no;

    for (auto rseg : m_sys->m_trx_sys->m_rseg_list) {
      Mtr mtr;

      mtr.start();
      mutex_enter(&rseg->m_mutex);

      EXPECT_EQ(rseg->m_last_page_no, FIL_NULL) << "nothing left to purge";

      auto hdr = trx_rsegf_get(*m_sys, rseg->m_space, rseg->m_page_no, &mtr);

      EXPECT_EQ(mtr.read_ulint(hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES), 0UL);

      auto addr = trx_purge_get_log_from_hist(flst_get_last(hdr + TRX_RSEG_HISTORY, &mtr));

      while (addr.m_page_no != FIL_NULL) {
        auto log_hdr = m_sys->m_undo->page_get(rseg->m_space, addr.m_page_no, &mtr) + addr.m_boffset;

        EXPECT_LT(mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO), purge_trx_no);

        addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));
        ++n;
      }

      mutex_exit(&rseg->m_mutex);
      mtr.commit(*m_sys);
    }

    EXPECT_EQ(hist_len(), n);

    return n;
  }

  /** @return a transaction with a read view: it holds purge back. */
  Trx *snapshot() {
    auto trx = begin();

    EXPECT_NE(trx->assign_read_view(*m_sys), nullptr);

    return trx;
  }

  /** Check both indexes and the system tablespace. */
  void validate() {
    auto trx = begin();

    EXPECT_TRUE(m_sys->am(clust()).validate(clust(), trx));
    EXPECT_TRUE(m_sys->am(sec()).validate(sec(), trx));

    commit(trx);

    EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
  }

  /** Expect the table to hold exactly rows, physically: no delete marks
  left in either index, the secondary index entries are those of rows. */
  void expect_purged(const ut::Rows &rows) {
    EXPECT_EQ(count(clust()), (Counts{rows.size(), 0}));
    EXPECT_EQ(count(sec()), (Counts{rows.size(), 0}));

    std::vector<std::tuple<uint32_t, uint32_t, bool>> expected;

    for (const auto &[k, row] : rows) {
      expected.emplace_back(row.m_a, k, false);
    }

    std::ranges::sort(expected);
    EXPECT_EQ(sec_entries(), expected);

    expect_rows(rows);
  }
};

/** Purge on the shared instance; each test starts with an empty, purged
table, no history. */
class PurgeTest : public PurgeBase<SharedDb> {
 protected:
  void SetUp() override {
    PurgeBase<SharedDb>::SetUp();
    clear();

    ASSERT_EQ(count(clust()), (Counts{0, 0}));
    ASSERT_EQ(count(sec()), (Counts{0, 0}));
  }
};

/** The purge of a delete: the delete marked record goes from the clustered
index and its entry from the secondary index. Before purge, both are there,
delete marked. */
TEST_F(PurgeTest, DeleteMarked) {
  const auto rows = make_rows(0, 100);

  load(rows);

  auto blocker = snapshot();
  auto trx = begin();
  auto expected = rows;

  for (uint32_t k = 0; k < 100; k += 2) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    expected.erase(k);
  }

  commit(trx);

  EXPECT_EQ(count(clust()), (Counts{100, 50}));
  EXPECT_EQ(count(sec()), (Counts{100, 50}));

  /* The blocker's view still needs the deleted rows. */
  purge();
  EXPECT_EQ(count(clust()), (Counts{100, 50}));
  EXPECT_EQ(scan(blocker), rows);

  commit(blocker);
  purge();

  expect_purged(expected);
  validate();
}

/** A view opened before a delete holds its purge back; a view opened after
it does not: its rows are purged while the newer view is open. */
TEST_F(PurgeTest, ViewOrder) {
  load(make_rows(0, 40));

  auto old_view = snapshot();
  auto trx = begin();

  for (uint32_t k = 0; k < 20; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  auto new_view = snapshot();

  purge();
  EXPECT_EQ(count(clust()), (Counts{40, 20}));

  commit(old_view);
  purge();

  /* The delete is older than new_view: purged under it. */
  EXPECT_EQ(count(clust()), (Counts{20, 0}));
  EXPECT_EQ(scan(new_view), make_rows(20, 40));

  commit(new_view);
  expect_purged(make_rows(20, 40));
}

/** A delete that is not yet committed is not purged, even with no view. */
TEST_F(PurgeTest, ActiveDelete) {
  load(make_rows(0, 10));

  auto trx = begin();

  ASSERT_EQ(remove(trx, 3), DB_SUCCESS);

  purge();
  EXPECT_EQ(count(clust()), (Counts{10, 1}));

  commit(trx);
  purge();

  auto expected = make_rows(0, 10);

  expected.erase(3);
  expect_purged(expected);
}

/** Each committed transaction with an update undo log adds one log to the
history list; insert only transactions add none. Purge empties the history
(see expect_history_purged()): its length goes down, the size to 0. */
TEST_F(PurgeTest, History) {
  const auto h0 = hist_len();

  EXPECT_EQ(rseg_hdr().m_hist_len, h0);
  EXPECT_EQ(rseg_hdr().m_hist_size, 0UL);

  auto blocker = snapshot();

  for (uint32_t k = 0; k < 30; ++k) {
    auto trx = begin();

    ASSERT_EQ(insert(trx, ut::Row{k, k, "x"}), DB_SUCCESS);
    commit(trx);
  }

  EXPECT_EQ(hist_len(), h0) << "insert undo logs are not in the history";

  for (uint32_t k = 0; k < 30; ++k) {
    auto trx = begin();

    ASSERT_EQ(update(trx, k, ut::Row{k, k + 1, "y"}), DB_SUCCESS);
    commit(trx);
  }

  EXPECT_EQ(hist_len(), h0 + 30);
  EXPECT_EQ(rseg_hdr().m_hist_len, h0 + 30);

  /* The blocker needs the 30, the older tail goes (newer logs follow it). */
  purge();
  EXPECT_GE(hist_len(), 30UL);
  EXPECT_LE(hist_len(), h0 + 30);

  commit(blocker);
  purge();

  EXPECT_LT(expect_history_purged(), h0 + 30);
}

/** The logs left in the history after a purge are those of the cached
update undo segment (the truncation stops at the first log it can't remove,
or the head): the tail is bounded by what fits in a cached page, once that
fills up the segment is freed with all its logs. */
TEST_F(PurgeTest, HistoryTail) {
  load(make_rows(0, 3));

  ulint max_tail{};

  for (uint32_t i = 0; i < 600; ++i) {
    auto trx = begin();

    ASSERT_EQ(update(trx, i % 3, ut::Row{i % 3, i, "t"}), DB_SUCCESS);
    commit(trx);

    if (i % 20 == 19) {
      purge();
      max_tail = std::max(max_tail, expect_history_purged());
    }
  }

  EXPECT_LT(max_tail, 300UL);
  EXPECT_EQ(rseg_hdr().m_hist_size, 0UL);
}

/** The history size counts the pages of the logs in the history whose
segments are not cached: a large update transaction adds the size of its
undo log, purge takes it off again, and frees the pages. */
TEST_F(PurgeTest, HistorySize) {
  load(make_rows(0, 400, 20));

  const auto before = rseg_hdr();
  auto blocker = snapshot();
  auto trx = begin();

  for (uint32_t k = 0; k < 400; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, k, ut::blob(k + 1, 200)}), DB_SUCCESS);
  }

  ASSERT_NE(trx->m_update_undo, nullptr);

  const auto undo_size = trx->m_update_undo->m_size;
  const auto during = rseg_hdr();

  ASSERT_GT(undo_size, 1UL) << "a log of several pages is not cached";
  commit(trx);

  auto after = rseg_hdr();

  EXPECT_EQ(after.m_hist_size, before.m_hist_size + undo_size);
  EXPECT_EQ(after.m_hist_len, before.m_hist_len + 1);
  EXPECT_EQ(after.m_used_slots, during.m_used_slots - 1) << "the slot is freed at commit";
  EXPECT_EQ(after.m_curr_size, during.m_curr_size) << "the pages move to the history";

  commit(blocker);
  purge();

  after = rseg_hdr();

  EXPECT_EQ(after.m_hist_size, 0UL);
  EXPECT_EQ(after.m_curr_size, during.m_curr_size - undo_size) << "the segment is freed";
  expect_history_purged();
  validate();
}

/** The size of a rollback segment in memory is its header page, the pages
in the history and the pages of the undo logs, cached or active. */
TEST_F(PurgeTest, RsegCurrSize) {
  auto check = [&](const char *when) {
    const auto h = rseg_hdr();
    const auto n_rsegs = UT_LIST_GET_LEN(m_sys->m_trx_sys->m_rseg_list);

    EXPECT_EQ(h.m_curr_size, n_rsegs + h.m_hist_size + h.m_undo_pages) << when;
  };

  check("empty");

  load(make_rows(0, 300));
  check("loaded");

  auto blocker = snapshot();
  auto trx = begin();

  for (uint32_t k = 0; k < 300; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, k + 100, ut::blob(k, 300)}), DB_SUCCESS);
  }

  check("active");
  commit(trx);
  check("committed");

  commit(blocker);
  purge();
  check("purged");
}

/** An update of a secondary key delete marks the old entry and inserts the
new one; purge removes the old entry only. */
TEST_F(PurgeTest, SecKeyUpdate) {
  load(make_rows(0, 100));

  auto blocker = snapshot();
  auto trx = begin();
  auto expected = make_rows(0, 100);

  for (auto &[k, row] : expected) {
    row.m_a = 1000 + k;
    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
  }

  commit(trx);

  EXPECT_EQ(count(clust()), (Counts{100, 0}));
  EXPECT_EQ(count(sec()), (Counts{200, 100}));

  commit(blocker);
  purge();

  expect_purged(expected);
}

/** A key moved away and back: the old entry is in use again (unmarked),
purge of the first update must keep it. Moved away and back and away again:
the entries of all the old values go. */
TEST_F(PurgeTest, SecKeyBackAndForth) {
  load(make_rows(0, 50));

  auto blocker = snapshot();
  auto expected = make_rows(0, 50);

  for (uint32_t i = 0; i < 2; ++i) {
    auto trx = begin();

    for (auto &[k, row] : expected) {
      auto r = row;

      r.m_a = (i == 0) ? 500 + k : row.m_a;

      if (i == 1) {
        r.m_a = k % 7;
      }

      ASSERT_EQ(update(trx, k, r), DB_SUCCESS);
    }

    commit(trx);
  }

  /* Rows with k % 2 == 1 move once more. */
  auto trx = begin();

  for (auto &[k, row] : expected) {
    if (k % 2 == 1) {
      row.m_a = 900 + k;
      ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    }
  }

  commit(trx);
  commit(blocker);
  purge();

  expect_purged(expected);
  validate();
}

/** Many updates of the same secondary key of one row, each in a transaction
of its own, with a view that sees each version: all the old entries go. */
TEST_F(PurgeTest, SecKeyChain) {
  load({{1, ut::Row{1, 0, "c"}}});

  auto blocker = snapshot();

  for (uint32_t i = 1; i <= 200; ++i) {
    auto trx = begin();

    ASSERT_EQ(update(trx, 1, ut::Row{1, i % 50, "c"}), DB_SUCCESS);
    commit(trx);
  }

  /* The 50 values, each once: the latest version unmarked. */
  EXPECT_EQ(count(sec()), (Counts{50, 49}));

  commit(blocker);
  purge();

  expect_purged({{1, ut::Row{1, 200 % 50, "c"}}});
}

/** An update of the primary key is a delete of the old record and an insert
of a new one: the old record and its secondary entry are purged. */
TEST_F(PurgeTest, PkUpdate) {
  load(make_rows(0, 60));

  auto blocker = snapshot();
  auto trx = begin();
  ut::Rows expected;

  for (uint32_t k = 0; k < 60; ++k) {
    auto row = ut::Row{k + 10000, k % 7, ut::blob(k, 50)};

    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    expected[row.m_k] = row;
  }

  commit(trx);

  EXPECT_EQ(count(clust()), (Counts{120, 60}));
  EXPECT_EQ(count(sec()), (Counts{120, 60}));

  commit(blocker);
  purge();

  expect_purged(expected);
}

/** A row deleted and inserted again with the same key before purge: the
insert reuses the delete marked record (an update of it); purge keeps the
row and removes the old secondary entry. */
TEST_F(PurgeTest, ReinsertSameKey) {
  load(make_rows(0, 30));

  auto blocker = snapshot();
  auto trx = begin();

  for (uint32_t k = 0; k < 30; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  trx = begin();

  ut::Rows expected;

  for (uint32_t k = 0; k < 30; ++k) {
    expected[k] = ut::Row{k, 100 + k, ut::blob(k + 7, 40)};
    ASSERT_EQ(insert(trx, expected[k]), DB_SUCCESS);
  }

  commit(trx);

  EXPECT_EQ(count(clust()), (Counts{30, 0}));

  commit(blocker);
  purge();

  expect_purged(expected);
}

/** A transaction rolled back leaves nothing for purge: the table after
purge is the one before, no delete marks and no stale secondary entries. */
TEST_F(PurgeTest, AfterRollback) {
  const auto rows = make_rows(0, 200);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 200; ++k) {
    switch (k % 4) {
      case 0:
        ASSERT_EQ(update(trx, k, ut::Row{k, k + 1000, "u"}), DB_SUCCESS);
        break;
      case 1:
        ASSERT_EQ(remove(trx, k), DB_SUCCESS);
        break;
      case 2:
        ASSERT_EQ(update(trx, k, ut::Row{k + 5000, k, "m"}), DB_SUCCESS);
        break;
      case 3:
        ASSERT_EQ(insert(trx, ut::Row{k + 9000, k, "i"}), DB_SUCCESS);
        break;
    }
  }

  rollback(trx);
  purge();

  expect_purged(rows);
  expect_history_purged();
  validate();
}

/** Purge removes enough records to empty pages: the tree shrinks, merges
pages and stays valid; the freed pages go back to the tablespace. */
TEST_F(PurgeTest, ShrinkTree) {
  const auto rows = make_rows(0, 3000, 300);

  load(rows);

  const auto pages = leaf_pages();
  auto trx = begin();

  for (uint32_t k = 0; k < 3000; ++k) {
    if (k % 10 != 0) {
      ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    }
  }

  commit(trx);
  purge();

  ut::Rows expected;

  for (uint32_t k = 0; k < 3000; k += 10) {
    expected[k] = rows.at(k);
  }

  expect_purged(expected);
  EXPECT_LT(leaf_pages(), pages / 2) << "before " << pages;
  validate();
}

/** All rows deleted: purge leaves an empty tree. */
TEST_F(PurgeTest, DeleteAll) {
  load(make_rows(0, 2000, 100));

  auto trx = begin();

  for (uint32_t k = 0; k < 2000; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);
  purge();

  expect_purged({});
  validate();
}

/** The externally stored fields of deleted rows are freed by purge: the
pages of the leaf segment go back to what they were before the load. */
TEST_F(PurgeTest, BlobsOfDeleted) {
  if (m_config.m_dict.m_blob_store != BlobStoreKind::INLINE) {
    GTEST_SKIP() << "counts the pages of the inline store";
  }

  const auto base = leaf_pages();
  const auto rows = make_rows(0, 40, 30000);

  load(rows);

  const auto loaded = leaf_pages();

  ASSERT_GT(loaded, base + 40) << "two blob pages per row";

  auto trx = begin();

  for (uint32_t k = 0; k < 40; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  EXPECT_EQ(leaf_pages(), loaded) << "nothing freed before purge";

  purge();

  expect_purged({});
  EXPECT_LE(leaf_pages(), base + 1) << "loaded " << loaded;
  validate();
}

/** An update of an externally stored field writes a new blob; purge frees
the old one once no view needs it: after many updates, one version is left. */
TEST_F(PurgeTest, BlobsOfUpdated) {
  if (m_config.m_dict.m_blob_store != BlobStoreKind::INLINE) {
    GTEST_SKIP() << "counts the pages of the inline store";
  }

  auto rows = make_rows(0, 10, 30000);

  load(rows);

  const auto one_version = leaf_pages();
  auto blocker = snapshot();

  for (uint32_t i = 1; i <= 5; ++i) {
    auto trx = begin();

    for (auto &[k, row] : rows) {
      row.m_b = ut::blob(k * 100 + i, 30000);
      ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    }

    commit(trx);
  }

  const auto versions = leaf_pages();

  EXPECT_GE(versions, one_version + 5 * 10 * 2) << "the old versions are kept";

  /* The blocker reads the old blobs. */
  EXPECT_EQ(scan(blocker), make_rows(0, 10, 30000));

  commit(blocker);
  purge();

  expect_purged(rows);
  EXPECT_LE(leaf_pages(), one_version + 2) << "versions " << versions;
  validate();
}

/** A blob row deleted and inserted again with the same key before purge: the
insert is an update of the delete marked record (TRX_UNDO_UPD_DEL_REC) whose
old blob purge must free. */
TEST_F(PurgeTest, BlobsOfReinserted) {
  const auto rows = make_rows(0, 10, 30000);

  load(rows);

  const auto one_version = leaf_pages();
  auto blocker = snapshot();
  auto trx = begin();

  for (uint32_t k = 0; k < 10; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  ut::Rows expected;

  trx = begin();

  for (uint32_t k = 0; k < 10; ++k) {
    expected[k] = ut::Row{k, k % 7, ut::blob(k + 77, 30000)};
    ASSERT_EQ(insert(trx, expected[k]), DB_SUCCESS);
  }

  commit(trx);
  commit(blocker);
  purge();

  expect_purged(expected);
  EXPECT_LE(leaf_pages(), one_version + 2);
  validate();
}

/** An update that doesn't change an indexed column nor a blob needs no
purge of the indexes: the table is the same after purge, and the history
goes. */
TEST_F(PurgeTest, NoOrdChange) {
  auto rows = make_rows(0, 100);

  load(rows);

  auto trx = begin();

  for (auto &[k, row] : rows) {
    row.m_b = ut::blob(k + 3, 50);
    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
  }

  commit(trx);
  purge();

  expect_purged(rows);
  expect_history_purged();
}

/** PurgeSys::update_undo_must_exist(): the update undo log of a transaction
the purge view doesn't see must still exist. */
TEST_F(PurgeTest, UndoMustExist) {
  load(make_rows(0, 5));

  auto blocker = snapshot();
  auto trx = begin();

  ASSERT_EQ(update(trx, 1, ut::Row{1, 77, "z"}), DB_SUCCESS);

  const auto id = trx->m_id;

  commit(trx);

  /* The purge view is the blocker's, which doesn't see trx. */
  (void)m_sys->m_trx_sys->m_purge->run();

  m_sys->m_trx_sys->m_purge->m_latch.s_lock();
  EXPECT_TRUE(m_sys->m_trx_sys->m_purge->update_undo_must_exist(id));
  m_sys->m_trx_sys->m_purge->m_latch.s_unlock();

  commit(blocker);
  purge();

  m_sys->m_trx_sys->m_purge->m_latch.s_lock();
  EXPECT_FALSE(m_sys->m_trx_sys->m_purge->update_undo_must_exist(id));
  m_sys->m_trx_sys->m_purge->m_latch.s_unlock();
}

/** Purge in batches: a large history is purged a batch (20 undo log pages)
at a time; each batch returns the pages it handled, and the history goes
down monotonically. */
TEST_F(PurgeTest, Batches) {
  load(make_rows(0, 500));

  (void)purge();

  auto blocker = snapshot();

  for (uint32_t k = 0; k < 500; ++k) {
    auto trx = begin();

    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    commit(trx);
  }

  /* The view of the blocker keeps all of these. What's left of the earlier tests can go at any
  moment: the master thread purges too, and purge keeps some of it for itself. */
  EXPECT_GE(hist_len(), 500UL);

  /* The master thread purges too (every 10 s, and when idle), in batches of
  its own: count the pages handled by all the runs, not the runs of the test
  (a slow host, the sanitizers, makes the master take more of them). */
  auto purge = m_sys->m_trx_sys->m_purge;
  auto handled = [purge] {
    mutex_enter(&purge->m_mutex);
    const auto n = purge->m_n_pages_handled;
    mutex_exit(&purge->m_mutex);
    return n;
  };
  const auto handled0 = handled();

  commit(blocker);

  ulint n_batches{};
  auto prev = count(clust()).m_n;

  for (;;) {
    const auto n = m_sys->m_trx_sys->m_purge->run();

    if (n == 0) {
      break;
    }

    EXPECT_LE(n, 21UL) << "a batch handles about 20 pages";

    const auto now = count(clust()).m_n;

    EXPECT_LE(now, prev);
    prev = now;
    ++n_batches;
  }

  (void)m_sys->m_trx_sys->m_purge->run();

  EXPECT_GT(n_batches, 0UL);
  EXPECT_GE(handled() - handled0, 500UL) << "the 500 undo logs, in batches of at most 21 pages";
  expect_purged({});
  expect_history_purged();
}

/** run() is called by many threads at once and by the master thread: they
serialize; purge ends with everything purged. */
TEST_F(PurgeTest, ConcurrentRun) {
  load(make_rows(0, 1000));

  auto trx = begin();

  for (uint32_t k = 0; k < 1000; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  with_timeout("ConcurrentRun", 120s, [&] {
    std::vector<std::thread> threads;

    for (int t = 0; t < 8; ++t) {
      threads.emplace_back([&] {
        for (int i = 0; i < 200; ++i) {
          (void)m_sys->m_trx_sys->m_purge->run();
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }
  });

  purge();
  expect_purged({});
}

/** Purge runs while writers change disjoint ranges of keys (insert, move the
secondary key, delete, insert again) and readers with snapshots scan both
indexes: no hang, no assertion, the readers see consistent snapshots, and
in the end the table holds exactly the rows written, physically. */
TEST_F(PurgeTest, ConcurrentDml) {
  constexpr uint32_t N_WRITERS = 4;
  constexpr uint32_t RANGE = 200;
  std::atomic<bool> stop{};
  std::vector<ut::Rows> finals(N_WRITERS);

  with_timeout("ConcurrentDml", 170s, [&] {
    std::thread purger([&] {
      while (!stop.load()) {
        (void)m_sys->m_trx_sys->m_purge->run();
      }
    });

    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < N_WRITERS; ++w) {
      threads.emplace_back([&, w] {
        const auto base = w * 10000;
        auto &rows = finals[w];

        for (uint32_t round = 0; round < 6; ++round) {
          auto trx = begin();

          for (uint32_t i = 0; i < RANGE; ++i) {
            const auto k = base + i;
            ut::Row row{k, (k + round) % 13, ut::blob(k + round, (i % 10 == 0) ? 20000 : 30)};

            if (rows.contains(k)) {
              if ((i + round) % 3 == 0) {
                EXPECT_EQ(remove(trx, k), DB_SUCCESS);
                rows.erase(k);
              } else {
                EXPECT_EQ(update(trx, k, row), DB_SUCCESS);
                rows[k] = row;
              }
            } else {
              EXPECT_EQ(insert(trx, row), DB_SUCCESS);
              rows[k] = row;
            }
          }

          commit(trx);
        }
      });
    }

    threads.emplace_back([&] {
      while (!stop.load()) {
        auto trx = begin();
        auto a = scan(trx);
        auto b = scan(trx);

        EXPECT_EQ(a, b) << "a snapshot is stable";
        EXPECT_EQ(scan_sec(trx).size(), a.size());
        commit(trx);
      }
    });

    for (uint32_t w = 0; w < N_WRITERS; ++w) {
      threads[w].join();
    }

    stop = true;
    threads.back().join();
    purger.join();
  });

  purge();

  ut::Rows all;

  for (const auto &rows : finals) {
    all.insert(rows.begin(), rows.end());
  }

  expect_purged(all);
  validate();
}

/** Rollback segments of the shared instance. */
using RsegTest = PurgeTest;

/** The rollback segment lookup by id; one segment, in the system tablespace,
with all its slots free when no transaction is active (but for the cached
undo logs). */
TEST_F(RsegTest, Lookup) {
  ASSERT_GE(UT_LIST_GET_LEN(m_sys->m_trx_sys->m_rseg_list), 1UL);

  for (auto rseg : m_sys->m_trx_sys->m_rseg_list) {
    EXPECT_EQ(TrxRseg::get_on_id(*m_sys, rseg->m_id), rseg);
    EXPECT_EQ(rseg->m_space, SYS_TABLESPACE);
    EXPECT_EQ(rseg->m_last_page_no, FIL_NULL) << "all purged";
  }

  EXPECT_EQ(TrxRseg::get_on_id(*m_sys, TRX_SYS_N_RSEGS - 1), nullptr);

  const auto h = rseg_hdr();

  EXPECT_EQ(h.m_used_slots, h.m_cached_ins + h.m_cached_upd) << "only cached logs hold slots";
}

/** A transaction's insert and update undo logs take a slot each; commit
gives the insert log's slot to the cache, the update log's to the cache or
the history. */
TEST_F(RsegTest, SlotsOfTrx) {
  load(make_rows(0, 5));

  const auto before = rseg_hdr();
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{100, 1, "i"}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 1, ut::Row{1, 9, "u"}), DB_SUCCESS);

  auto during = rseg_hdr();
  const auto new_slots = during.m_used_slots - before.m_used_slots;
  const auto from_cache = (before.m_cached_ins - during.m_cached_ins) + (before.m_cached_upd - during.m_cached_upd);

  EXPECT_EQ(new_slots + from_cache, 2UL);
  EXPECT_NE(trx->m_insert_undo->m_id, trx->m_update_undo->m_id);

  commit(trx);

  const auto after = rseg_hdr();

  /* Both small logs are cached (the update log is in the history too). */
  EXPECT_EQ(after.m_used_slots, during.m_used_slots);
  EXPECT_EQ(after.m_cached_ins + after.m_cached_upd, during.m_cached_ins + during.m_cached_upd + 2);
}

/** The cached update undo log of a committed transaction is reused by the
next one, after purge too: no new slot, no new segment. */
TEST_F(RsegTest, CachedReuse) {
  load(make_rows(0, 5));

  for (int i = 0; i < 3; ++i) {
    auto trx = begin();

    ASSERT_EQ(update(trx, 2, ut::Row{2, uint32_t(20 + i), "u"}), DB_SUCCESS);
    commit(trx);
  }

  purge();

  const auto before = rseg_hdr();

  for (int i = 0; i < 20; ++i) {
    auto trx = begin();

    ASSERT_EQ(update(trx, 3, ut::Row{3, uint32_t(30 + i), "u"}), DB_SUCCESS);
    ASSERT_EQ(insert(trx, ut::Row{uint32_t(1000 + i), 0, ""}), DB_SUCCESS);
    commit(trx);

    if (i % 5 == 4) {
      purge();
    }
  }

  purge();

  const auto after = rseg_hdr();

  EXPECT_EQ(after.m_used_slots, before.m_used_slots);
  EXPECT_EQ(after.m_curr_size, before.m_curr_size);
}

/** Many concurrent writers: each has its own undo slots, all distinct. */
TEST_F(RsegTest, ConcurrentSlots) {
  constexpr uint32_t N = 100;
  std::vector<Trx *> trxs;
  std::set<std::pair<TrxRseg *, ulint>> slots;

  load(make_rows(0, N));

  for (uint32_t i = 0; i < N; ++i) {
    auto trx = begin();

    ASSERT_EQ(update(trx, i, ut::Row{i, i + 1, "c"}), DB_SUCCESS);
    ASSERT_EQ(insert(trx, ut::Row{i + 1000, i, "c"}), DB_SUCCESS);

    EXPECT_TRUE(slots.emplace(trx->m_rseg, trx->m_update_undo->m_id).second);
    EXPECT_TRUE(slots.emplace(trx->m_rseg, trx->m_insert_undo->m_id).second);
    trxs.push_back(trx);
  }

  EXPECT_GE(rseg_hdr().m_used_slots, 2 * N);

  for (size_t i = 0; i < trxs.size(); ++i) {
    if (i % 2 == 0) {
      commit(trxs[i]);
    } else {
      rollback(trxs[i]);
    }
  }

  purge();

  ut::Rows expected = make_rows(0, N);

  for (uint32_t i = 0; i < N; i += 2) {
    expected[i].m_a = i + 1;
    expected[i].m_b = "c";
    expected[i + 1000] = ut::Row{i + 1000, i, "c"};
  }

  expect_purged(expected);
}

/** Instances of their own. */
class PurgeRestartTest : public PurgeBase<ut::DbTest> {
 protected:
  /** As DbTest::SetUp(), the log written at commit and flushed once a
  second: a process kill loses nothing, and the commits don't wait for the
  disk. */
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_log.m_flush_at_trx_commit = 2;
    start();
  }
};

/** All the undo slots of the rollback segment used by active transactions:
the next one gets DB_TOO_MANY_CONCURRENT_TRXS, and can roll back; once the
others commit, their slots are reused. */
TEST_F(PurgeRestartTest, AllSlots) {
  const auto h = rseg_hdr();
  const auto n_rsegs = UT_LIST_GET_LEN(m_sys->m_trx_sys->m_rseg_list);
  const auto free_slots = n_rsegs * TRX_RSEG_N_SLOTS - h.m_used_slots + h.m_cached_ins;
  std::vector<Trx *> trxs;
  DbErr err{DB_SUCCESS};

  for (uint32_t k = 0; k < 2 * n_rsegs * TRX_RSEG_N_SLOTS; ++k) {
    auto trx = begin();

    err = insert(trx, ut::Row{k, k, ""});

    if (err != DB_SUCCESS) {
      rollback(trx);
      break;
    }

    trxs.push_back(trx);
  }

  EXPECT_EQ(err, DB_TOO_MANY_CONCURRENT_TRXS);
  EXPECT_EQ(trxs.size(), free_slots);

  ut::Rows rows;

  for (auto trx : trxs) {
    commit(trx);
  }

  for (uint32_t k = 0; k < trxs.size(); ++k) {
    rows[k] = ut::Row{k, k, ""};
  }

  /* The slots are cached: inserts reuse them. */
  for (uint32_t i = 0; i < 10; ++i) {
    auto trx = begin();

    ASSERT_EQ(insert(trx, ut::Row{100000 + i, 0, ""}), DB_SUCCESS);
    commit(trx);
    rows[100000 + i] = ut::Row{100000 + i, 0, ""};
  }

  expect_rows(rows);
}

/** Once all the undo slots hold cached insert undo logs (after as many
concurrent inserters as slots), an update still gets an update undo log,
also after a restart (the cached logs come back from the slots). */
TEST_F(PurgeRestartTest, CachedSlotsServeUpdates) {
  const auto n_rsegs = UT_LIST_GET_LEN(m_sys->m_trx_sys->m_rseg_list);
  std::vector<Trx *> trxs;

  for (uint32_t k = 0; k < 2 * n_rsegs * TRX_RSEG_N_SLOTS; ++k) {
    auto trx = begin();

    if (insert(trx, ut::Row{k, k, ""}) != DB_SUCCESS) {
      rollback(trx);
      break;
    }

    trxs.push_back(trx);
  }

  for (auto trx : trxs) {
    commit(trx);
  }

  /* No transaction is active: an update must work. */
  auto trx = begin();

  EXPECT_EQ(update(trx, 0, ut::Row{0, 1, "x"}), DB_SUCCESS);
  commit(trx);

  restart();

  trx = begin();
  EXPECT_EQ(update(trx, 1, ut::Row{1, 2, "y"}), DB_SUCCESS);
  commit(trx);
}


/** History left at a shutdown is purged after the restart: the history list
of the rollback segment header comes back, purge empties it. */
TEST_F(PurgeRestartTest, HistoryAfterRestart) {
  auto rows = make_rows(0, 300, 2000);

  load(rows);

  for (uint32_t k = 0; k < 300; k += 3) {
    auto trx = begin();

    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    commit(trx);
    rows.erase(k);
  }

  restart();
  purge_all();

  expect_purged(rows);
  expect_history_purged();
  validate();
}

/** A crash with history to purge (and blobs to free): after recovery, purge
completes and the table holds exactly the committed rows. */
TEST_F(PurgeRestartTest, HistoryAfterCrash) {
  auto rows = make_rows(0, 200, 100);

  for (uint32_t k = 0; k < 200; k += 10) {
    rows[k].m_b = ut::blob(k, 20000);
  }

  auto expected = rows;

  for (uint32_t k = 0; k < 200; ++k) {
    if (k % 2 == 0) {
      expected.erase(k);
    } else if (k % 3 == 0) {
      expected[k].m_a += 1000;
    }
  }

  crash([&] {
    load(rows);

    for (uint32_t k = 0; k < 200; ++k) {
      auto trx = begin();

      if (k % 2 == 0) {
        EXPECT_EQ(remove(trx, k), DB_SUCCESS);
      } else if (k % 3 == 0) {
        EXPECT_EQ(update(trx, k, expected[k]), DB_SUCCESS);
      }

      commit(trx);
    }

    /* An uncommitted change, rolled back by the recovery. */
    auto trx = begin();

    EXPECT_EQ(remove(trx, 1), DB_SUCCESS);
    EXPECT_EQ(update(trx, 5, ut::Row{5, 5555, "lost"}), DB_SUCCESS);
  });

  /* Load the table in the dictionary cache before purge: see UncachedTable. */
  (void)table();
  purge_all();

  expect_purged(expected);
  expect_history_purged();
  validate();
}

/** A crash in the middle of purge (purge running when the process is
killed): the next start finishes it. */
TEST_F(PurgeRestartTest, CrashDuringPurge) {
  auto rows = make_rows(0, 2000, 200);
  ut::Rows expected;

  for (uint32_t k = 1; k < 2000; k += 2) {
    expected[k] = rows[k];
  }

  crash([&] {
    load(rows);

    auto trx = begin();

    for (uint32_t k = 0; k < 2000; k += 2) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);

    /* A few batches, not all. */
    for (int i = 0; i < 3; ++i) {
      (void)m_sys->m_trx_sys->m_purge->run();
    }
  });

  /* Load the table in the dictionary cache before purge: see UncachedTable. */
  (void)table();
  purge_all();


  expect_purged(expected);
  validate();
}

/** The history of a table that is not in the dictionary cache, after a
crash, is purged once the table is loaded: the control case of
UncachedTable. */
TEST_F(PurgeRestartTest, CachedTableAfterCrash) {
  const auto rows = make_rows(0, 100, 50);
  ut::Rows expected;

  for (uint32_t k = 1; k < 100; k += 2) {
    expected[k] = rows.at(k);
  }

  crash([&] {
    load(rows);

    /* Hold purge back: the history survives the crash. */
    [[maybe_unused]] auto blocker = snapshot();
    auto trx = begin();

    for (uint32_t k = 0; k < 100; k += 2) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
  });

  (void)table();
  purge_all();

  expect_purged(expected);
}

/** BUG: after a restart, purge skips the undo records of every table that is
not in the dictionary cache yet, as if the table were dropped, and frees the
undo logs: the delete marked records, their secondary index entries and the
old blobs stay forever. RowPurge::parse_undo_rec() (row/row0purge.cc:424)
looks the table up with Dict::table_get_on_id(table_id), the cache only
lookup; upstream (7c21eac0 row0purge.c) used dict_table_get_on_id_low(
srv_force_recovery, table_id), which loads the table from SYS_TABLES. Here
nothing opens test/t before purge runs (the master thread or the test). */
TEST_F(PurgeRestartTest, UncachedTable) {
  const auto rows = make_rows(0, 100, 50);
  ut::Rows expected;

  for (uint32_t k = 1; k < 100; k += 2) {
    expected[k] = rows.at(k);
  }

  crash([&] {
    load(rows);

    [[maybe_unused]] auto blocker = snapshot();
    auto trx = begin();

    for (uint32_t k = 0; k < 100; k += 2) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
  });

  purge_all();

  expect_purged(expected);
}

/** Purge, timed. */
class PurgePerf : public PurgeTest {
 protected:
  /** Time purge(), report n units of work as name. */
  ut::BenchResult timed(const std::string &name, uint64_t n) {
    const auto start = std::chrono::steady_clock::now();

    purge();

    const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
    ut::BenchResult r{name, 1, n, secs.count()};

    ut::report(r);

    return r;
  }
};

/** Purge of deleted rows: rows purged per second. */
TEST_F(PurgePerf, DeletedRows) {
  constexpr uint32_t N = 20000;

  load(make_rows(0, N, 20));

  auto trx = begin();

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  auto r = timed("purge_deleted_rows", N);

  EXPECT_LT(r.ns_per_op(), 1000000);
  EXPECT_EQ(count(clust()).m_n, 0UL);
}

/** Purge of secondary key updates: old entries removed per second. */
TEST_F(PurgePerf, SecUpdates) {
  constexpr uint32_t N = 20000;

  load(make_rows(0, N, 20));

  auto trx = begin();

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, k + 1000000, ut::blob(k, 20)}), DB_SUCCESS);
  }

  commit(trx);

  auto r = timed("purge_sec_updates", N);

  EXPECT_LT(r.ns_per_op(), 1000000);
  EXPECT_EQ(count(sec()), (Counts{N, 0}));
}

/** Purge of many small transactions: undo logs (history entries) per second. */
TEST_F(PurgePerf, ManyLogs) {
  constexpr uint32_t N = 5000;

  load(make_rows(0, N, 20));

  for (uint32_t k = 0; k < N; ++k) {
    auto trx = begin();

    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    commit(trx);
  }

  auto r = timed("purge_history_logs", N);

  EXPECT_LT(r.ns_per_op(), 2000000);
  expect_history_purged();
}

/** Purge of deleted rows with blobs: blob pages freed per second. The blobs
fit in the buffer pool (32 MB): freeing a blob reads each of its pages for
the next page number, from the disk if it was evicted. */
TEST_F(PurgePerf, BlobPages) {
  constexpr uint32_t N = 300;

  const auto base = leaf_pages();

  load(make_rows(0, N, 30000));

  const auto pages = leaf_pages() - base;
  auto trx = begin();

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  commit(trx);

  auto r = timed("purge_blob_pages_freed", pages);

  EXPECT_LT(r.ns_per_op(), 2000000);
  EXPECT_LE(leaf_pages(), base + 1);
}

}  // namespace
