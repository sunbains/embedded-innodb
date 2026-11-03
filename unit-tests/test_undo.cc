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

/** @file unit-tests/test_undo.cc
Undo: the undo logs of a transaction, their records, their truncation after
a statement or partial rollback, their reuse after a commit, and their use
by a full rollback.

  UndoTest         transactions of an instance shared by the suite.
  UndoRestartTest  an instance of its own: restarts.
  UndoPerf         performance. */

#include "ut0env.h"
#include "trx0sys.h"
#include "trx0purge.h"
#include "fsp0fsp.h"
#include "ut0perf.h"

#include "dict0dict.h"
#include "mem0mem.h"
#include "row0upd.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "trx0undo_rec.h"

namespace {

ut::Rows make_rows(uint32_t from, uint32_t to, size_t blob_len = 50) {
  ut::Rows rows;

  for (auto k = from; k < to; ++k) {
    rows[k] = ut::Row{k, k % 7, ut::blob(k, blob_len)};
  }

  return rows;
}

/** The undo number of the next record of trx is one more than the top
record of either undo log: a statement rollback truncates the logs. */
void expect_undo_consistent(const Trx *trx) {
  for (auto undo : {trx->m_insert_undo, trx->m_update_undo}) {
    if (undo != nullptr && !undo->m_empty) {
      EXPECT_LT(undo->m_top_undo_no, trx->m_undo_no);
    }
  }
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

/** The undo logs of transactions, read back record by record. */
class UndoTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();

    m_sys->m_dict->mutex_acquire();
    m_table = m_sys->m_dict->table_get(ut::TABLE);
    m_sys->m_dict->mutex_release();

    m_index = m_table->get_first_index();
    m_heap = mem_heap_create(1024);
  }

  void TearDown() override {
    mem_heap_free(m_heap);
    SharedDb::TearDown();
  }

  /** The top record of undo, a log of trx, parsed. */
  struct Top {
    TrxUndoRecord::Parsed m_pars;
    byte *m_ptr{};
    roll_ptr_t m_roll_ptr{};
  };

  Top top(const Trx *trx, const TrxUndo *undo) {
    Top t;

    t.m_roll_ptr = trx_undo_build_roll_ptr(undo->m_type == TRX_UNDO_INSERT, trx->m_rseg->m_id, undo->m_top_page_no, undo->m_top_offset);

    auto rec = TrxUndoRecord::get_undo_rec_low(*m_sys, t.m_roll_ptr, m_heap);

    t.m_ptr = rec.get_pars(t.m_pars);

    return t;
  }

  /** @return the k of the row reference at ptr, which moves past it. */
  uint32_t row_ref_k(byte *&ptr) {
    DTuple *ref{};

    ptr = TrxUndoRecord::get_row_ref(ptr, m_index, &ref, m_heap);

    auto field = dtuple_get_nth_field(ref, 0);

    EXPECT_EQ(dfield_get_len(field), 4u);

    return uint32_t(mach_read_from_4(static_cast<const byte *>(dfield_get_data(field))));
  }

  /** Roll trx back to its undo number limit, as a statement or savepoint rollback does. */
  void partial_rollback(Trx *trx, undo_no_t limit) {
    TrxSavept savept{limit};

    EXPECT_EQ(trx_general_rollback(*m_sys, trx, true, &savept), DB_SUCCESS);
  }

  Table *m_table{};
  Index *m_index{};
  MemHeap *m_heap{};
};

/** An insert writes one record to the insert undo log: the type, undo
number 0, the table id and the primary key of the row. */
TEST_F(UndoTest, InsertRecord) {
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{5005, 1, "x"}), DB_SUCCESS);

  auto undo = trx->m_insert_undo;

  ASSERT_NE(undo, nullptr);
  EXPECT_EQ(trx->m_update_undo, nullptr);
  EXPECT_EQ(undo->m_type, TRX_UNDO_INSERT);
  EXPECT_FALSE(undo->m_empty);
  EXPECT_EQ(undo->m_top_undo_no, 0u);
  EXPECT_EQ(undo->m_trx_id, trx->m_id);
  EXPECT_EQ(trx->m_undo_no, 1u);

  auto t = top(trx, undo);

  EXPECT_EQ(t.m_pars.m_type, TRX_UNDO_INSERT_REC);
  EXPECT_EQ(t.m_pars.m_undo_no, 0u);
  EXPECT_EQ(t.m_pars.m_table_id, m_table->m_id);
  EXPECT_FALSE(t.m_pars.m_extern);
  EXPECT_EQ(row_ref_k(t.m_ptr), 5005u);
  EXPECT_TRUE(trx_undo_roll_ptr_is_insert(t.m_roll_ptr));

  commit(trx);
}

/** An update writes the old values: the record type, the system columns of
the old version (its trx id and roll pointer) and the old value of the
changed column only. */
TEST_F(UndoTest, UpdateRecord) {
  load({{7, ut::Row{7, 70, "old"}}});

  auto trx = begin();

  ASSERT_EQ(update(trx, 7, ut::Row{7, 71, "old"}), DB_SUCCESS);

  auto undo = trx->m_update_undo;

  ASSERT_NE(undo, nullptr);
  EXPECT_EQ(trx->m_insert_undo, nullptr);
  EXPECT_EQ(undo->m_type, TRX_UNDO_UPDATE);
  EXPECT_EQ(undo->m_top_undo_no, 0u);

  auto t = top(trx, undo);

  EXPECT_EQ(t.m_pars.m_type, TRX_UNDO_UPD_EXIST_REC);
  EXPECT_EQ(t.m_pars.m_undo_no, 0u);
  EXPECT_EQ(t.m_pars.m_table_id, m_table->m_id);
  EXPECT_FALSE(trx_undo_roll_ptr_is_insert(t.m_roll_ptr));

  trx_id_t old_trx_id{};
  roll_ptr_t old_roll_ptr{};
  ulint info_bits{};

  t.m_ptr = TrxUndoRecord::update_rec_get_sys_cols(t.m_ptr, &old_trx_id, &old_roll_ptr, &info_bits);

  EXPECT_GT(old_trx_id, 0u);
  EXPECT_LT(old_trx_id, trx->m_id);
  EXPECT_TRUE(trx_undo_roll_ptr_is_insert(old_roll_ptr)) << "the old version was inserted";
  EXPECT_EQ(row_ref_k(t.m_ptr), 7u);

  Upd *upd{};

  t.m_ptr = TrxUndoRecord::update_rec_get_update(
    t.m_ptr, m_index, t.m_pars.m_type, old_trx_id, old_roll_ptr, info_bits, trx, m_heap, &upd
  );

  ASSERT_NE(upd, nullptr);

  /* The column a, the system columns of the old version. */
  const auto a_pos = m_index->get_nth_field_pos(m_table->get_col_no("a"));
  auto uf = upd->get_field(a_pos);

  ASSERT_NE(uf, nullptr) << "the old value of a";
  EXPECT_EQ(mach_read_from_4(static_cast<const byte *>(dfield_get_data(&uf->m_new_val))), 70u);
  EXPECT_EQ(upd->get_field(m_index->get_nth_field_pos(m_table->get_col_no("b"))), nullptr) << "b did not change";

  commit(trx);
}

/** A delete is a delete mark: its record says so and the log is marked as
needing a purge. */
TEST_F(UndoTest, DeleteRecord) {
  load({{8, ut::Row{8, 80, "d"}}});

  auto trx = begin();

  ASSERT_EQ(remove(trx, 8), DB_SUCCESS);

  auto undo = trx->m_update_undo;

  ASSERT_NE(undo, nullptr);
  EXPECT_TRUE(undo->m_del_marks);

  auto t = top(trx, undo);

  EXPECT_EQ(t.m_pars.m_type, TRX_UNDO_DEL_MARK_REC);

  commit(trx);
}

/** An update of the primary key is a delete mark of the old record and an
insert of the new: a record in each log, in sequence. */
TEST_F(UndoTest, PkUpdate) {
  load({{9, ut::Row{9, 90, "p"}}});

  auto trx = begin();

  ASSERT_EQ(update(trx, 9, ut::Row{19, 90, "p"}), DB_SUCCESS);

  ASSERT_NE(trx->m_update_undo, nullptr);
  ASSERT_NE(trx->m_insert_undo, nullptr);
  EXPECT_EQ(trx->m_undo_no, 2u);

  auto del = top(trx, trx->m_update_undo);
  auto ins = top(trx, trx->m_insert_undo);

  EXPECT_EQ(del.m_pars.m_type, TRX_UNDO_DEL_MARK_REC);
  EXPECT_EQ(ins.m_pars.m_type, TRX_UNDO_INSERT_REC);
  EXPECT_EQ(del.m_pars.m_undo_no + 1, ins.m_pars.m_undo_no);

  trx_id_t trx_id{};
  roll_ptr_t roll_ptr{};
  ulint info_bits{};

  del.m_ptr = TrxUndoRecord::update_rec_get_sys_cols(del.m_ptr, &trx_id, &roll_ptr, &info_bits);

  EXPECT_EQ(row_ref_k(del.m_ptr), 9u);
  EXPECT_EQ(row_ref_k(ins.m_ptr), 19u);

  rollback(trx);
  expect_rows({{9, ut::Row{9, 90, "p"}}});
}

/** The undo numbers are one sequence over both logs: each log's top is the
number of its latest record. */
TEST_F(UndoTest, UndoNoSequence) {
  load(make_rows(0, 10));

  auto trx = begin();
  undo_no_t last_ins{};
  undo_no_t last_upd{};

  for (uint32_t i = 0; i < 10; ++i) {
    ASSERT_EQ(insert(trx, ut::Row{100 + i, 0, ""}), DB_SUCCESS);
    last_ins = trx->m_undo_no - 1;

    ASSERT_EQ(update(trx, i, ut::Row{i, 1000, ""}), DB_SUCCESS);
    last_upd = trx->m_undo_no - 1;
  }

  EXPECT_EQ(trx->m_undo_no, 20u);
  EXPECT_EQ(trx->m_insert_undo->m_top_undo_no, last_ins);
  EXPECT_EQ(trx->m_update_undo->m_top_undo_no, last_upd);
  EXPECT_EQ(top(trx, trx->m_insert_undo).m_pars.m_undo_no, last_ins);
  EXPECT_EQ(top(trx, trx->m_update_undo).m_pars.m_undo_no, last_upd);

  rollback(trx);
  expect_rows(make_rows(0, 10));
}

/** A failed insert in the middle of a transaction that commits: the rows
inserted before and after it are there. */
TEST_F(UndoTest, StmtRollbackCommit) {
  auto trx = begin();
  auto rows = make_rows(1, 6);

  for (const auto &[k, row] : rows) {
    ASSERT_EQ(insert(trx, row), DB_SUCCESS);
  }

  EXPECT_EQ(insert(trx, rows[3]), DB_DUPLICATE_KEY);
  expect_undo_consistent(trx);

  for (const auto &[k, row] : make_rows(6, 9)) {
    ASSERT_EQ(insert(trx, row), DB_SUCCESS);
    rows[k] = row;
  }

  commit(trx);
  expect_rows(rows);
}

/** The same transaction rolled back: nothing is left. Undo::truncate_end()
compared the undo numbers the wrong way and left the records of the failed
statement in the undo log: the later records reused their undo numbers and
the rollback asserted on the duplicate. */
TEST_F(UndoTest, StmtRollbackRollback) {
  auto trx = begin();
  auto rows = make_rows(1, 6);

  for (const auto &[k, row] : rows) {
    ASSERT_EQ(insert(trx, row), DB_SUCCESS);
  }

  EXPECT_EQ(insert(trx, rows[3]), DB_DUPLICATE_KEY);

  for (const auto &[k, row] : make_rows(6, 9)) {
    ASSERT_EQ(insert(trx, row), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows({});
}

/** A primary key update that fails on a duplicate, between updates that
succeed: the update undo log is truncated too. */
TEST_F(UndoTest, StmtRollbackUpdate) {
  const auto original = make_rows(0, 20);

  for (bool commit_it : {false, true}) {
    clear();
    load(original);

    auto expected = original;
    auto trx = begin();

    auto row = expected[1];
    row.m_a += 100;
    ASSERT_EQ(update(trx, 1, row), DB_SUCCESS);
    expected[1] = row;

    /* k 2 -> 3 exists: the statement is rolled back. */
    row = expected[2];
    row.m_k = 3;
    EXPECT_EQ(update(trx, 2, row), DB_DUPLICATE_KEY);
    expect_undo_consistent(trx);

    row = expected[4];
    row.m_b = ut::blob(44, 20000);
    ASSERT_EQ(update(trx, 4, row), DB_SUCCESS);
    expected[4] = row;

    if (commit_it) {
      commit(trx);
      expect_rows(expected);
    } else {
      rollback(trx);
      expect_rows(original);
    }
  }
}

/** Many failed statements between many successful ones, with undo logs
that span several pages, then a rollback. */
TEST_F(UndoTest, ManyStmtRollbacks) {
  const auto original = make_rows(0, 200);

  load(original);

  auto trx = begin();

  for (uint32_t k = 0; k < 200; ++k) {
    auto row = original.at(k);

    row.m_a += 1;
    row.m_b = ut::blob(k + 7, 300);
    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);

    if (k % 10 == 0) {
      EXPECT_EQ(insert(trx, original.at((k + 1) % 200)), DB_DUPLICATE_KEY);
      expect_undo_consistent(trx);
    }

    ASSERT_EQ(insert(trx, ut::Row{1000 + k, 1, "new"}), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(original);
}

/** Partial rollbacks of an insert undo log of several pages, down to
limits in the middle of a page, at the first record of a page (the page
goes), at the end (nothing to do) and to 0: each leaves the log with its
top below the next undo number, frees the emptied pages, and keeps exactly
the rows inserted below the limit. */
TEST_F(UndoTest, PartialRollbackLimits) {
  constexpr uint32_t N = 3000;
  /* Keys no other test uses: an insert on a delete marked row is an update. */
  constexpr uint32_t B = 50000;
  auto trx = begin();

  /* The undo numbers of the first record of each page after the first. */
  std::vector<undo_no_t> page_starts;
  ulint size{1};

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(insert(trx, ut::Row{B + k, 0, ""}), DB_SUCCESS);

    if (trx->m_insert_undo->m_size > size) {
      size = trx->m_insert_undo->m_size;
      page_starts.push_back(k);
    }
  }

  ASSERT_GE(page_starts.size(), 2u) << "the log must span pages";

  auto undo = trx->m_insert_undo;
  const auto last_start = page_starts.back();
  const auto prev_start = page_starts[page_starts.size() - 2];

  std::vector<undo_no_t> limits{
    N,                   /* nothing */
    N - 1,               /* the last record */
    last_start + 1,      /* one record left on the last page */
    last_start,          /* the whole last page */
    prev_start + 7,      /* in the middle of a page */
    prev_start,          /* a page boundary again */
    1,                   /* all but one */
  };

  for (auto limit : limits) {
    const auto size_before = undo->m_size;

    partial_rollback(trx, limit);

    EXPECT_EQ(trx->m_undo_no, limit) << limit;
    EXPECT_EQ(undo->m_top_undo_no, limit - 1) << limit;
    EXPECT_LE(undo->m_size, size_before) << limit;
    expect_undo_consistent(trx);

    if (limit == last_start) {
      EXPECT_EQ(undo->m_size, size_before - 1) << "the emptied page is freed";
    }

    EXPECT_EQ(scan(trx).size(), limit) << limit;
  }

  EXPECT_EQ(undo->m_size, 1u);

  /* New records after the partial rollbacks continue the sequence. */
  ASSERT_EQ(insert(trx, ut::Row{B + N + 1, 0, ""}), DB_SUCCESS);
  EXPECT_EQ(undo->m_top_undo_no, 1u);

  commit(trx);
  expect_rows({{B, ut::Row{B, 0, ""}}, {B + N + 1, ut::Row{B + N + 1, 0, ""}}});
}

/** A partial rollback of both logs to 0 empties them; the transaction
goes on and commits only what came after. */
TEST_F(UndoTest, PartialRollbackAll) {
  load(make_rows(0, 50));

  auto trx = begin();

  for (uint32_t k = 0; k < 50; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, 999, ut::blob(k, 200)}), DB_SUCCESS);
    ASSERT_EQ(insert(trx, ut::Row{100 + k, 1, ""}), DB_SUCCESS);
  }

  partial_rollback(trx, 0);

  EXPECT_EQ(trx->m_undo_no, 0u);
  EXPECT_TRUE(trx->m_insert_undo == nullptr || trx->m_insert_undo->m_empty);
  EXPECT_TRUE(trx->m_update_undo == nullptr || trx->m_update_undo->m_empty);
  EXPECT_EQ(scan(trx), make_rows(0, 50));

  ASSERT_EQ(update(trx, 3, ut::Row{3, 33, "c"}), DB_SUCCESS);
  commit(trx);

  auto expected = make_rows(0, 50);

  expected[3] = ut::Row{3, 33, "c"};
  expect_rows(expected);
}

/** A savepoint rolled back undoes what came after it only. BUG: see
SavepointByName: savepoint_rollback() reads the savepoint after
freeing it; this passes only while the freed memory is intact. */
TEST_F(UndoTest, Savepoint) {
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{1, 1, ""}), DB_SUCCESS);
  api::savepoint_take(trx, "a", 1);
  ASSERT_EQ(insert(trx, ut::Row{2, 2, ""}), DB_SUCCESS);
  ASSERT_EQ(update(trx, 1, ut::Row{1, 10, ""}), DB_SUCCESS);

  EXPECT_EQ(api::savepoint_rollback(*m_sys, trx, "a", 1), DB_SUCCESS);
  EXPECT_EQ(trx->m_undo_no, 1u);
  expect_undo_consistent(trx);

  commit(trx);
  expect_rows({{1, ut::Row{1, 1, ""}}});
}

/* Regression: api::savepoint_rollback() (api/api0api.cc:3794-3815) ignores the name:
with a name it takes the first savepoint of the list (the oldest), without
one it takes the first too; it then frees the savepoint and reads it after:
trx_roll_savepoints_free(trx, savep) and trx_general_rollback(..,
&savep->savept) use freed memory. Upstream looked the name up (and returned
DB_NO_SAVEPOINT if not found), freed only the later savepoints and kept the
one rolled back to. A rollback to "b" here goes back to "a". */
TEST_F(UndoTest, SavepointByName) {
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{1, 1, ""}), DB_SUCCESS);
  api::savepoint_take(trx, "a", 1);
  ASSERT_EQ(insert(trx, ut::Row{2, 2, ""}), DB_SUCCESS);
  api::savepoint_take(trx, "b", 1);
  ASSERT_EQ(insert(trx, ut::Row{3, 3, ""}), DB_SUCCESS);

  EXPECT_EQ(api::savepoint_rollback(*m_sys, trx, "b", 1), DB_SUCCESS);
  EXPECT_EQ(trx->m_undo_no, 2u);

  /* The savepoint stays: a second rollback to it is fine. */
  EXPECT_EQ(api::savepoint_rollback(*m_sys, trx, "b", 1), DB_SUCCESS);
  EXPECT_EQ(api::savepoint_rollback(*m_sys, trx, "none", 4), DB_NO_SAVEPOINT);

  commit(trx);
  expect_rows({{1, ut::Row{1, 1, ""}}, {2, ut::Row{2, 2, ""}}});
}

/** A committed insert-only transaction leaves its one page insert undo log
cached in the rollback segment; the next insert of the same segment reuses
it. */
TEST_F(UndoTest, InsertUndoCached) {
  auto trx = begin();

  ASSERT_EQ(insert(trx, ut::Row{60000, 0, ""}), DB_SUCCESS);

  const auto rseg = trx->m_rseg;
  const auto hdr_page_no = trx->m_insert_undo->m_hdr_page_no;

  commit(trx);

  mutex_enter(&rseg->m_mutex);

  bool cached{};

  for (auto undo : rseg->m_insert_undo_cached) {
    cached |= undo->m_hdr_page_no == hdr_page_no;
  }

  mutex_exit(&rseg->m_mutex);

  EXPECT_TRUE(cached);

  for (uint32_t i = 0; i < 4; ++i) {
    trx = begin();
    ASSERT_EQ(insert(trx, ut::Row{60001 + i, 0, ""}), DB_SUCCESS);

    if (trx->m_rseg == rseg) {
      EXPECT_EQ(trx->m_insert_undo->m_hdr_page_no, hdr_page_no);
      EXPECT_EQ(trx->m_insert_undo->m_top_undo_no, 0u);
    }

    commit(trx);
  }
}

/** A committed small update undo log goes to the history and its segment
is cached: the next update of the same segment reuses the page. */
TEST_F(UndoTest, UpdateUndoCached) {
  load(make_rows(0, 5));

  auto trx = begin();

  ASSERT_EQ(update(trx, 1, ut::Row{1, 11, ""}), DB_SUCCESS);

  const auto rseg = trx->m_rseg;
  const auto hdr_page_no = trx->m_update_undo->m_hdr_page_no;

  commit(trx);

  for (uint32_t i = 0; i < 4; ++i) {
    trx = begin();
    ASSERT_EQ(update(trx, 2, ut::Row{2, 20 + i, ""}), DB_SUCCESS);

    if (trx->m_rseg == rseg) {
      EXPECT_EQ(trx->m_update_undo->m_hdr_page_no, hdr_page_no);
      EXPECT_NE(trx->m_update_undo->m_hdr_offset, 0u);
    }

    commit(trx);
  }
}

/** A large transaction of every kind of change, over undo logs of many
pages, rolled back. */
TEST_F(UndoTest, RollbackLarge) {
  const auto original = make_rows(0, 2000, 30);

  load(original);

  auto trx = begin();

  for (uint32_t k = 0; k < 2000; ++k) {
    switch (k % 4) {
      case 0:
        ASSERT_EQ(update(trx, k, ut::Row{k, k + 1, ut::blob(k, 60)}), DB_SUCCESS);
        break;
      case 1:
        ASSERT_EQ(remove(trx, k), DB_SUCCESS);
        break;
      case 2:
        ASSERT_EQ(update(trx, k, ut::Row{k + 10000, k, "moved"}), DB_SUCCESS);
        break;
      case 3:
        ASSERT_EQ(insert(trx, ut::Row{k + 20000, k, "new"}), DB_SUCCESS);
        break;
    }
  }

  EXPECT_GT(trx->m_update_undo->m_size, 1u);
  EXPECT_GT(trx->m_insert_undo->m_size, 1u);

  rollback(trx);
  expect_rows(original);
}

/** Restart after rollbacks and commits: the undo logs are cleaned up. */
using UndoRestartTest = ut::DbTest;

TEST_F(UndoRestartTest, Restart) {
  auto rows = make_rows(0, 100);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 100; k += 2) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  rollback(trx);

  restart();
  expect_rows(rows);
}

/** Undo log writes and rollbacks, timed. */
class UndoPerf : public UndoTest {};

/** Insert rows in one transaction (an insert undo record each), then roll
them back. */
TEST_F(UndoPerf, InsertRollback) {
  constexpr uint32_t N = 20000;
  auto trx = begin();

  auto w = ut::bench("insert_with_undo", N, [&](uint64_t i) { EXPECT_EQ(insert(trx, ut::Row{uint32_t(i), 0, ""}), DB_SUCCESS); });

  const auto start = std::chrono::steady_clock::now();

  rollback(trx);

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
  ut::BenchResult r{"rollback_insert_rows", 1, N, secs.count()};

  ut::report(r);

  EXPECT_LT(w.ns_per_op(), 1000000);
  EXPECT_LT(r.ns_per_op(), 1000000);
}

/** Update rows in one transaction, then roll them back. */
TEST_F(UndoPerf, UpdateRollback) {
  constexpr uint32_t N = 5000;

  load(make_rows(0, N, 20));

  auto trx = begin();

  auto w = ut::bench("update_with_undo", N, [&](uint64_t i) {
    EXPECT_EQ(update(trx, uint32_t(i), ut::Row{uint32_t(i), 1, "updated"}), DB_SUCCESS);
  });

  const auto start = std::chrono::steady_clock::now();

  rollback(trx);

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
  ut::BenchResult r{"rollback_update_rows", 1, N, secs.count()};

  ut::report(r);

  EXPECT_LT(w.ns_per_op(), 1000000);
  EXPECT_LT(r.ns_per_op(), 1000000);
}

/** Purge frees the history: committed update undo logs are truncated from
the history list and their segments freed. The trx numbers were compared
as an unsigned difference, always >= 0: no history log was ever freed, and
the loop that frees a segment stopped after its first step. */
TEST_F(UndoTest, PurgeFreesHistory) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 50; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  for (uint32_t i = 0; i < 300; ++i) {
    auto trx = begin();
    auto &row = rows[i % 50];

    row.m_a += 1;
    ASSERT_EQ(update(trx, row.m_k, row), DB_SUCCESS);
    commit(trx);
  }

  auto trx_sys = m_sys->m_trx_sys.get();
  const auto before = trx_sys->m_rseg_history_len;

  /* Run purge until it has nothing left to do, as the master thread does. */
  for (int i = 0; i < 1000 && m_sys->m_trx_sys->m_purge->run() > 0; ++i) {
  }

  /* The 300 commits added about 300 logs to the history. Don't compare with
  'before': the master thread purges too, and when the updates are slow (a
  sanitizer build) it has already shortened the history by the time we look.
  Purge truncates in batches: a short tail may remain. With the bug nothing
  was ever freed. */
  EXPECT_LT(trx_sys->m_rseg_history_len, 300UL / 4) << "history before purge " << before;
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));

  expect_rows(rows);
  restart();
  expect_rows(rows);
}

}  // namespace
