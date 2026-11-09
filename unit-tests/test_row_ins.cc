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

/** @file unit-tests/test_row_ins.cc
RowInsert: inserts into the clustered and secondary indexes: duplicate
checks in primary keys and unique secondary indexes (delete marked
duplicates, NULLs, column prefixes, space padded strings, composite signed
keys), the reuse of delete marked records, many duplicates in a secondary
index, rollback. The first tests (RowInsTest) run on a fresh instance each,
the others (RowInsIdxTest) on tables of their own in a shared instance. */

#include "ut0env.h"

#include "idx0am.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "rem0rec.h"
#include "row0ins.h"
#include "trx0trx.h"

#include <sys/prctl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <future>
#include <mutex>
#include <thread>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace {

using RowInsTest = ut::DbTest;

/** Blobs of every size class: empty, in page, at the limit, off page, huge. */
TEST_F(RowInsTest, BlobSizes) {
  if (index_in_memory()) {
    GTEST_SKIP() << "no external fields in an index in memory";
  }

  ut::Rows rows;
  uint32_t k{};

  for (size_t len : {0, 1, 100, 767, 768, 4000, 8000, 8200, 16384, 100000, 1000000}) {
    rows[k] = ut::Row{k, k, ut::blob(k, len)};
    ++k;
  }

  load(rows);
  expect_rows(rows);

  restart();
  expect_rows(rows);
}

/** The primary key must be unique, the secondary key need not be. */
TEST_F(RowInsTest, Duplicates) {
  load({{1, ut::Row{1, 5, "a"}}, {2, ut::Row{2, 5, "b"}}});

  auto trx = begin();

  EXPECT_EQ(insert(trx, ut::Row{1, 6, "c"}), DB_DUPLICATE_KEY);
  EXPECT_EQ(insert(trx, ut::Row{3, 5, "d"}), DB_SUCCESS);
  commit(trx);

  expect_rows({{1, ut::Row{1, 5, "a"}}, {2, ut::Row{2, 5, "b"}}, {3, ut::Row{3, 5, "d"}}});
}

/** A key deleted and inserted again by the same and by another transaction. */
TEST_F(RowInsTest, ReinsertDeleted) {
  load({{1, ut::Row{1, 1, "old"}}});

  auto trx = begin();

  ASSERT_EQ(remove(trx, 1), DB_SUCCESS);
  ASSERT_EQ(insert(trx, ut::Row{1, 2, "new"}), DB_SUCCESS);
  commit(trx);

  trx = begin();
  ASSERT_EQ(remove(trx, 1), DB_SUCCESS);
  commit(trx);

  trx = begin();
  ASSERT_EQ(insert(trx, ut::Row{1, 3, ut::blob(1, 30000)}), DB_SUCCESS);
  commit(trx);

  expect_rows({{1, ut::Row{1, 3, ut::blob(1, 30000)}}});
}

/** Many rows in random order: page splits in both indexes. */
TEST_F(RowInsTest, RandomOrder) {
  ut::Rows rows;

  srandom(42);

  while (rows.size() < 5000) {
    const auto k = uint32_t(random());

    rows[k] = ut::Row{k, uint32_t(random() % 100), ut::blob(k, random() % 300)};
  }

  load(rows);
  expect_rows(rows);
}

/** Concurrent inserts of disjoint and of overlapping keys. */
TEST_F(RowInsTest, Concurrent) {
  constexpr uint32_t N_THREADS = 4;
  constexpr uint32_t N = 500;

  std::vector<std::future<int>> results;

  for (uint32_t t = 0; t < N_THREADS; ++t) {
    results.push_back(std::async(std::launch::async, [this, t] {
      int dups{};

      for (uint32_t i = 0; i < N; ++i) {
        /* Half the keys are shared by all threads. */
        const auto k = i % 2 == 0 ? i : t * 100000 + i;
        auto trx = begin();
        const auto err = insert(trx, ut::Row{k, k % 10, ut::blob(k, 50)});

        if (err == DB_DUPLICATE_KEY) {
          ++dups;
        } else if (err != DB_SUCCESS) {
          ADD_FAILURE() << "insert " << k << ": " << err;
        }

        if (err == DB_DEADLOCK || err == DB_LOCK_WAIT_TIMEOUT) {
          rollback(trx);
        } else {
          commit(trx);
        }
      }

      return dups;
    }));
  }

  int dups{};

  for (auto &r : results) {
    dups += r.get();
  }

  ut::Rows rows;

  for (uint32_t t = 0; t < N_THREADS; ++t) {
    for (uint32_t i = 0; i < N; ++i) {
      const auto k = i % 2 == 0 ? i : t * 100000 + i;

      rows[k] = ut::Row{k, k % 10, ut::blob(k, 50)};
    }
  }

  EXPECT_EQ(dups, int((N_THREADS - 1) * N / 2));
  expect_rows(rows);
}

}  // namespace

namespace {

/* --- A started System shared by the tests of a suite --------------------
Starting and shutting down an instance takes seconds: the tests of a suite
share one, each test works on tables of its own (Tab). */

/** A column of a table to create. */
struct ColDef {
  const char *m_name;
  ColType m_type;
  ColAttr m_attr;
  ulint m_len;
};

/** An index of a table to create: columns with their prefix lengths. */
struct IdxDef {
  const char *m_name;
  std::vector<std::pair<const char *, ulint>> m_cols;
  bool m_clustered{};
  bool m_unique{};
};

/** Create table name in sys. */
[[nodiscard]] DbErr create_table(System &sys, const std::string &name, const std::vector<ColDef> &cols, const std::vector<IdxDef> &idxs) {
  (void)api::database_create(sys, "test");

  api::TableSchema *tbl_sch{};

  if (auto err = api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1); err != DB_SUCCESS) {
    return err;
  }

  auto err = DB_SUCCESS;

  for (const auto &c : cols) {
    if (err == DB_SUCCESS) {
      err = tbl_sch->add_col(c.m_name, c.m_type, c.m_attr, 0, c.m_len);
    }
  }

  for (const auto &i : idxs) {
    api::IndexSchema *idx_sch{};

    if (err == DB_SUCCESS) {
      err = tbl_sch->add_index(i.m_name, &idx_sch);
    }

    for (const auto &[col, prefix] : i.m_cols) {
      if (err == DB_SUCCESS) {
        err = idx_sch->add_col(col, prefix);
      }
    }

    if (err == DB_SUCCESS && i.m_clustered) {
      err = idx_sch->set_clustered();
    }

    if (err == DB_SUCCESS && i.m_unique) {
      err = idx_sch->set_unique();
    }
  }

  if (err == DB_SUCCESS) {
    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    uint64_t id{};

    err = api::schema_lock_exclusive(sys, trx);

    if (err == DB_SUCCESS) {
      err = api::table_create(sys, trx, tbl_sch, &id);
    }

    (void)api::trx_commit(sys, trx);
  }

  api::TableSchema::destroy(sys, tbl_sch);

  return err;
}

/** @return the table from the dictionary cache. */
Table *get_table(System &sys, const std::string &name) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(name.c_str());
  sys.m_dict->mutex_release();

  return table;
}

/** A table with the schema of ut::TABLE:
(k INT UNSIGNED PK, a INT UNSIGNED, b BLOB, INDEX SEC_A(a)), with the row
helpers of ut::DbTest. */
struct Tab {
  /** @param[in] create     Create the table, else use an existing one. */
  Tab(System &sys, std::string name, bool create = true) : m_sys(sys), m_name(std::move(name)) {
    if (!create) {
      return;
    }

    const auto err = create_table(
      m_sys,
      m_name,
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"a", IB_INT, IB_COL_UNSIGNED, 4}, {"b", IB_BLOB, IB_COL_NONE, 0}},
      {{"PRIMARY", {{"k", 0}}, true, false}, {ut::SEC_A, {{"a", 0}}, false, false}}
    );

    EXPECT_EQ(err, DB_SUCCESS) << m_name;
  }

  [[nodiscard]] Table *table() const { return get_table(m_sys, m_name); }
  [[nodiscard]] Index *clust() const { return table()->get_first_index(); }
  [[nodiscard]] Index *sec() const { return table()->get_index_on_name(ut::SEC_A); }

  /** Open the table in trx with an intention lock. */
  [[nodiscard]] DbErr open(Trx *trx, CursorLockMode mode, api::Cursor **crsr) const {
    if (auto err = api::cursor_open_table(m_sys, m_name.c_str(), trx, crsr); err != DB_SUCCESS) {
      return err;
    }

    if (auto err = (*crsr)->lock(m_sys, mode); err != DB_SUCCESS) {
      (void)api::Cursor::close(m_sys, *crsr);
      *crsr = nullptr;
      return err;
    }

    return DB_SUCCESS;
  }

  static DbErr write_row(api::Tuple *tpl, const ut::Row &row) {
    if (auto err = tpl->write_u32(0, row.m_k); err != DB_SUCCESS) {
      return err;
    }

    if (auto err = tpl->write_u32(1, row.m_a); err != DB_SUCCESS) {
      return err;
    }

    return tpl->set_value(2, row.m_b.data(), row.m_b.size());
  }

  static ut::Row read_row(const api::Tuple *tpl) {
    ut::Row row;

    EXPECT_EQ(tpl->read_u32(0, &row.m_k), DB_SUCCESS);
    EXPECT_EQ(tpl->read_u32(1, &row.m_a), DB_SUCCESS);

    const auto len = tpl->get_len(2);

    if (len != api::IB_SQL_NULL) {
      row.m_b.resize(len);

      if (len > 0) {
        (void)tpl->copy_value(2, row.m_b.data(), len);
      }
    }

    return row;
  }

  /** Position crsr on the row with key k. */
  DbErr seek(api::Cursor *crsr, uint32_t k) const {
    auto key = crsr->clust_search_tuple_create();
    auto err = key->write_u32(0, k);
    int res{-1};

    if (err == DB_SUCCESS) {
      err = crsr->moveto(m_sys, key, IB_CUR_GE, &res);
    }

    api::Tuple::destroy(key);

    if ((err == DB_SUCCESS && res != 0) || err == DB_END_OF_INDEX) {
      err = DB_RECORD_NOT_FOUND;
    }

    return err;
  }

  [[nodiscard]] DbErr insert(Trx *trx, const ut::Row &row) const {
    api::Cursor *crsr{};

    if (auto err = open(trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto tpl = crsr->clust_read_tuple_create();
    auto err = write_row(tpl, row);

    if (err == DB_SUCCESS) {
      err = crsr->insert_row(m_sys, tpl);
    }

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(m_sys, crsr);

    return err;
  }

  [[nodiscard]] DbErr update(Trx *trx, uint32_t k, const ut::Row &row) const {
    api::Cursor *crsr{};

    if (auto err = open(trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->set_lock_mode(m_sys, IB_LOCK_X);

    if (err == DB_SUCCESS) {
      err = seek(crsr, k);
    }

    if (err == DB_SUCCESS) {
      auto old_tpl = crsr->clust_read_tuple_create();
      auto new_tpl = crsr->clust_read_tuple_create();

      err = crsr->read_row(m_sys, old_tpl);

      if (err == DB_SUCCESS) {
        err = new_tpl->copy(old_tpl);
      }

      if (err == DB_SUCCESS) {
        err = write_row(new_tpl, row);
      }

      if (err == DB_SUCCESS) {
        err = crsr->update_row(m_sys, old_tpl, new_tpl);
      }

      api::Tuple::destroy(old_tpl);
      api::Tuple::destroy(new_tpl);
    }

    (void)api::Cursor::close(m_sys, crsr);

    return err;
  }

  [[nodiscard]] DbErr remove(Trx *trx, uint32_t k) const {
    api::Cursor *crsr{};

    if (auto err = open(trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->set_lock_mode(m_sys, IB_LOCK_X);

    if (err == DB_SUCCESS) {
      err = seek(crsr, k);
    }

    if (err == DB_SUCCESS) {
      err = crsr->delete_row(m_sys);
    }

    (void)api::Cursor::close(m_sys, crsr);

    return err;
  }

  /** @return the rows visible to trx through the clustered index. */
  [[nodiscard]] ut::Rows scan(Trx *trx) const {
    api::Cursor *crsr{};
    ut::Rows rows;

    if (auto err = open(trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
      ADD_FAILURE() << "open: " << err;
      return rows;
    }

    for (auto &row : ut::read_all(m_sys, crsr, crsr->first(m_sys))) {
      EXPECT_TRUE(rows.emplace(row.m_k, row).second) << "duplicate k " << row.m_k;
    }

    (void)api::Cursor::close(m_sys, crsr);

    return rows;
  }

  /** @return the rows visible to trx through SEC_A with cluster access. */
  [[nodiscard]] std::vector<ut::Row> scan_sec(Trx *trx) const {
    api::Cursor *crsr{};
    api::Cursor *sec{};
    std::vector<ut::Row> rows;

    if (auto err = open(trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
      ADD_FAILURE() << "open: " << err;
      return rows;
    }

    if (auto err = crsr->open_index(m_sys, ut::SEC_A, &sec); err != DB_SUCCESS) {
      ADD_FAILURE() << "open_index: " << err;
    } else {
      sec->set_cluster_access();
      rows = ut::read_all(m_sys, sec, sec->first(m_sys));
      (void)api::Cursor::close(m_sys, sec);
    }

    (void)api::Cursor::close(m_sys, crsr);

    return rows;
  }

  /** Check the table holds exactly expected, through both indexes. */
  void expect_rows(const ut::Rows &expected) const {
    auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
    const auto rows = scan(trx);

    EXPECT_EQ(rows.size(), expected.size());

    for (const auto &[k, row] : expected) {
      auto it = rows.find(k);

      if (it == rows.end()) {
        ADD_FAILURE() << "missing k " << k;
      } else if (!(it->second == row)) {
        ADD_FAILURE() << "k " << k << ": a " << it->second.m_a << " expected " << row.m_a << ", b length "
                      << it->second.m_b.size() << " expected " << row.m_b.size();
      }
    }

    for (const auto &[k, row] : rows) {
      if (!expected.contains(k)) {
        ADD_FAILURE() << "unexpected k " << k;
      }
    }

    const auto sec = scan_sec(trx);

    EXPECT_EQ(sec.size(), expected.size());

    for (size_t i{}; i < sec.size(); ++i) {
      auto it = expected.find(sec[i].m_k);

      if (it == expected.end() || !(it->second == sec[i])) {
        ADD_FAILURE() << "SEC_A row " << i << " k " << sec[i].m_k << " a " << sec[i].m_a << " not as expected";
      }

      if (i > 0 && std::pair(sec[i - 1].m_a, sec[i - 1].m_k) >= std::pair(sec[i].m_a, sec[i].m_k)) {
        ADD_FAILURE() << "SEC_A out of order at " << i;
      }
    }

    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  }

  /** Insert the rows in one committed transaction. */
  void load(const ut::Rows &rows) const {
    auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

    for (const auto &[k, row] : rows) {
      ASSERT_EQ(insert(trx, row), DB_SUCCESS) << "k " << k;
    }

    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  }

  System &m_sys;
  std::string m_name;
};

/** The fixture: one System for the suite. */
class SharedDb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    s_config = ut::make_config(s_dir->m_path);
    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
  }

  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      s_sys.reset();
    }

    s_dir.reset();
  }

  void SetUp() override {
    ASSERT_NE(s_sys, nullptr);
    m_sys = s_sys.get();
  }

  /** @return a table of the test's own, named after it. */
  [[nodiscard]] std::unique_ptr<Tab> tab(const char *suffix = "") {
    const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();

    return std::make_unique<Tab>(*m_sys, std::string("test/") + info->name() + suffix);
  }

  [[nodiscard]] Trx *begin(TrxLevel level = IB_TRX_REPEATABLE_READ) {
    auto trx = api::trx_begin(*m_sys, level);

    EXPECT_NE(trx, nullptr);
    return trx;
  }

  void commit(Trx *trx) { EXPECT_EQ(api::trx_commit(*m_sys, trx), DB_SUCCESS); }
  void rollback(Trx *trx) { EXPECT_EQ(api::trx_rollback(*m_sys, trx), DB_SUCCESS); }

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline Config s_config;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
};

}  // namespace

/* --- Duplicate checks and index entries, in a shared instance ---------- */

namespace {

/** @return the records of index, in order, as (deleted, fields as strings). */
struct IdxRec {
  bool m_deleted{};
  std::vector<std::optional<std::string>> m_fields;
};

std::vector<IdxRec> index_recs(System &sys, const Index *index) {
  std::vector<IdxRec> v;
  Mtr mtr;
  IndexCursor pcur{sys};
  MemHeap *heap{};

  mtr.start();
  pcur.seek_first(index, Intent::Read, &mtr);

  while (pcur.next(&mtr)) {
    auto rec = pcur.rec();
    RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    IdxRec r;

    r.m_deleted = rec.get_deleted_flag();

    for (ulint i{}; i < offs.n_fields(); ++i) {
      ulint len;
      auto f = rec.get_nth_field(offs, i, &len);

      if (len == UNIV_SQL_NULL) {
        r.m_fields.emplace_back(std::nullopt);
      } else {
        r.m_fields.emplace_back(std::string(reinterpret_cast<const char *>(f.get()), len));
      }
    }

    v.push_back(std::move(r));
  }

  pcur.commit(&mtr);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return v;
}

/** @return v as stored in a 4 byte unsigned column. */
std::string u4(uint32_t v) {
  std::string s(4, '\0');

  mach_write_to_4(reinterpret_cast<byte *>(s.data()), v);
  return s;
}

class RowInsIdxTest : public SharedDb {
 protected:
  void TearDown() override { m_tab.reset(); }

  /** Create test/<test name>u: (k INT UNSIGNED PK, a INT UNSIGNED NULL,
  b BLOB, UNIQUE INDEX UA(a)). */
  void create_unique() {
    const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();

    m_name = std::string("test/") + info->name() + "u";

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_name,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"a", IB_INT, IB_COL_UNSIGNED, 4}, {"b", IB_BLOB, IB_COL_NONE, 0}},
        {{"PRIMARY", {{"k", 0}}, true, false}, {"UA", {{"a", 0}}, false, true}}
      ),
      DB_SUCCESS
    );
  }

  /** Insert (k, a) into m_name, a nullopt for NULL. */
  DbErr ins(Trx *trx, uint32_t k, std::optional<uint32_t> a, const std::string &table = "") {
    api::Cursor *crsr{};
    const auto &name = table.empty() ? m_name : table;

    if (auto err = api::cursor_open_table(*m_sys, name.c_str(), trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(*m_sys, IB_LOCK_IX);
    auto tpl = crsr->clust_read_tuple_create();

    if (err == DB_SUCCESS) {
      err = tpl->write_u32(0, k);
    }

    if (err == DB_SUCCESS) {
      err = a.has_value() ? tpl->write_u32(1, *a) : tpl->set_value(1, nullptr, api::IB_SQL_NULL);
    }

    if (err == DB_SUCCESS) {
      err = tpl->set_value(2, "x", 1);
    }

    if (err == DB_SUCCESS) {
      err = crsr->insert_row(*m_sys, tpl);
    }

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  }

  /** Delete the row with key k from m_name. */
  DbErr del(Trx *trx, uint32_t k) {
    Tab t(*m_sys, m_name, false);

    return t.remove(trx, k);
  }

  [[nodiscard]] Index *index(const char *name) {
    auto table = get_table(*m_sys, m_name);

    return name == nullptr ? table->get_first_index() : table->get_index_on_name(name);
  }

  std::string m_name;
  std::unique_ptr<Tab> m_tab;
};

/** A unique secondary index refuses a duplicate value; the statement is
rolled back: no clustered record and no index entry remain. */
TEST_F(RowInsIdxTest, UniqueSecDuplicate) {
  create_unique();

  auto trx = begin();

  ASSERT_EQ(ins(trx, 1, 10), DB_SUCCESS);
  EXPECT_EQ(ins(trx, 2, 10), DB_DUPLICATE_KEY);
  EXPECT_EQ(ins(trx, 3, 11), DB_SUCCESS);
  commit(trx);

  const auto clust = index_recs(*m_sys, index(nullptr));
  const auto ua = index_recs(*m_sys, index("UA"));
  size_t live{};

  for (const auto &r : clust) {
    live += !r.m_deleted;
    EXPECT_TRUE(r.m_deleted || r.m_fields[0] != u4(2)) << "the refused row is gone";
  }

  EXPECT_EQ(live, 2UL);

  size_t live_ua{};

  for (const auto &r : ua) {
    live_ua += !r.m_deleted;
  }

  EXPECT_EQ(live_ua, 2UL);

  /* In another transaction too. */
  trx = begin();
  EXPECT_EQ(ins(trx, 4, 11), DB_DUPLICATE_KEY);
  commit(trx);
}

/** NULL is not equal to NULL: any number of NULLs in a unique index. */
TEST_F(RowInsIdxTest, UniqueSecNulls) {
  create_unique();

  auto trx = begin();

  for (uint32_t k = 0; k < 50; ++k) {
    ASSERT_EQ(ins(trx, k, std::nullopt), DB_SUCCESS) << k;
  }

  ASSERT_EQ(ins(trx, 100, 7), DB_SUCCESS);
  commit(trx);

  const auto ua = index_recs(*m_sys, index("UA"));

  ASSERT_EQ(ua.size(), 51UL);

  /* NULLs sort first, by k. */
  for (uint32_t k = 0; k < 50; ++k) {
    EXPECT_EQ(ua[k].m_fields[0], std::nullopt);
    EXPECT_EQ(ua[k].m_fields[1], u4(k));
  }

  EXPECT_EQ(ua[50].m_fields[0], u4(7));
}

/** A delete marked entry with the same unique value doesn't count: the
row deleted and its value inserted again, in the same transaction and
after the commit of the delete. */
TEST_F(RowInsIdxTest, UniqueSecDeleteMarked) {
  create_unique();

  auto trx = begin();

  ASSERT_EQ(ins(trx, 1, 10), DB_SUCCESS);
  commit(trx);

  trx = begin();
  ASSERT_EQ(del(trx, 1), DB_SUCCESS);
  EXPECT_EQ(ins(trx, 2, 10), DB_SUCCESS) << "the entry (10, 1) is delete marked";
  commit(trx);

  trx = begin();
  ASSERT_EQ(del(trx, 2), DB_SUCCESS);
  commit(trx);

  trx = begin();
  EXPECT_EQ(ins(trx, 3, 10), DB_SUCCESS);
  EXPECT_EQ(ins(trx, 4, 10), DB_DUPLICATE_KEY);
  commit(trx);

  size_t live{};

  for (const auto &r : index_recs(*m_sys, index("UA"))) {
    if (!r.m_deleted) {
      ++live;
      EXPECT_EQ(r.m_fields[0], u4(10));
      EXPECT_EQ(r.m_fields[1], u4(3));
    }
  }

  EXPECT_EQ(live, 1UL);
}

/** A primary key deleted (committed, not purged) and inserted again
reuses the delete marked record: one record, not delete marked, with the
new values. */
TEST_F(RowInsIdxTest, PkReinsertReusesRecord) {
  m_tab = tab();
  m_tab->load({{1, ut::Row{1, 5, "old"}}, {2, ut::Row{2, 6, "two"}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->remove(trx, 1), DB_SUCCESS);
  commit(trx);

  trx = begin();
  ASSERT_EQ(m_tab->insert(trx, ut::Row{1, 9, "new"}), DB_SUCCESS);
  commit(trx);

  const auto recs = index_recs(*m_sys, m_tab->clust());
  size_t n1{};

  for (const auto &r : recs) {
    if (r.m_fields[0] == u4(1)) {
      ++n1;
      EXPECT_FALSE(r.m_deleted);
      EXPECT_EQ(r.m_fields[3], u4(9));
      EXPECT_EQ(r.m_fields[4], std::string("new"));
    }
  }

  EXPECT_EQ(n1, 1UL);

  /* The secondary index has the new entry live and the old one delete
  marked (until purge). */
  bool old_entry{};
  bool new_entry{};

  for (const auto &r : index_recs(*m_sys, m_tab->sec())) {
    if (r.m_fields[0] == u4(5) && r.m_fields[1] == u4(1)) {
      old_entry = true;
      EXPECT_TRUE(r.m_deleted);
    }

    if (r.m_fields[0] == u4(9) && r.m_fields[1] == u4(1)) {
      new_entry = true;
      EXPECT_FALSE(r.m_deleted);
    }
  }

  EXPECT_TRUE(new_entry);
  (void)old_entry;
  m_tab->expect_rows({{1, ut::Row{1, 9, "new"}}, {2, ut::Row{2, 6, "two"}}});
}

/** A secondary entry deleted and inserted again with the same (a, k)
reuses the delete marked entry. */
TEST_F(RowInsIdxTest, SecReinsertSameEntry) {
  m_tab = tab();
  m_tab->load({{1, ut::Row{1, 5, "x"}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->remove(trx, 1), DB_SUCCESS);
  ASSERT_EQ(m_tab->insert(trx, ut::Row{1, 5, "y"}), DB_SUCCESS);
  commit(trx);

  const auto sec = index_recs(*m_sys, m_tab->sec());

  ASSERT_EQ(sec.size(), 1UL);
  EXPECT_FALSE(sec[0].m_deleted);
  m_tab->expect_rows({{1, ut::Row{1, 5, "y"}}});
}

/** Many rows with the same secondary value: SEC_A orders them by k, the
tree splits within the duplicates, all are found. */
TEST_F(RowInsIdxTest, SecManyDuplicates) {
  m_tab = tab();

  ut::Rows rows;
  std::vector<uint32_t> keys(4000);

  std::iota(keys.begin(), keys.end(), 0);
  std::mt19937 rng(17);
  std::shuffle(keys.begin(), keys.end(), rng);

  auto trx = begin();

  for (auto k : keys) {
    ut::Row row{k, k % 1000 == 0 ? 1U : 7U, "d"};

    ASSERT_EQ(m_tab->insert(trx, row), DB_SUCCESS);
    rows[k] = row;
  }

  commit(trx);

  const auto sec = index_recs(*m_sys, m_tab->sec());

  ASSERT_EQ(sec.size(), 4000UL);

  for (size_t i = 1; i < sec.size(); ++i) {
    EXPECT_LT(std::pair(sec[i - 1].m_fields[0], sec[i - 1].m_fields[1]), std::pair(sec[i].m_fields[0], sec[i].m_fields[1]));
  }

  EXPECT_GT(m_sys->am(m_tab->sec()).n_data_pages(m_tab->sec()), 1UL);
  m_tab->expect_rows(rows);
}

/** RowInsert::dupl_error_with_rec(): an entry equal to a record in the
unique fields is a duplicate unless the record is delete marked, or, in a
secondary index, the entry has a NULL in them. */
TEST_F(RowInsIdxTest, DuplErrorWithRec) {
  create_unique();

  auto trx = begin();

  ASSERT_EQ(ins(trx, 1, 10), DB_SUCCESS);
  commit(trx);

  auto ua = index("UA");
  auto clust = index(nullptr);
  MemHeap *heap = mem_heap_create(1024);

  auto check = [&](const Index *idx, std::vector<std::optional<uint32_t>> vals) {
    auto entry = dtuple_create(heap, idx->get_n_fields());

    idx->copy_types(entry, idx->get_n_fields());

    for (ulint i{}; i < idx->get_n_fields(); ++i) {
      auto f = dtuple_get_nth_field(entry, i);

      if (i < vals.size() && vals[i].has_value()) {
        auto p = mem_heap_alloc(heap, 4);

        mach_write_to_4(static_cast<byte *>(p), *vals[i]);
        dfield_set_data(f, p, 4);
      } else if (i < vals.size()) {
        dfield_set_null(f);
      } else {
        /* System columns of the clustered index, not compared. */
        auto p = mem_heap_zalloc(heap, 7);

        dfield_set_data(f, p, idx->get_nth_field(i)->get_col()->get_fixed_size());
      }
    }

    Mtr mtr;
    IndexCursor pcur{*m_sys};

    mtr.start();
    pcur.seek_first(idx, Intent::Read, &mtr);
    EXPECT_TRUE(pcur.next(&mtr));

    auto rec = pcur.rec();
    RecOffs offs = PhyRec(idx, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    const auto dup = m_sys->m_row_ins->dupl_error_with_rec(rec, entry, idx, offs);

    pcur.commit(&mtr);

    return dup;
  };

  /* UA: (a, k). */
  EXPECT_TRUE(check(ua, {10, 2}));
  EXPECT_FALSE(check(ua, {11, 2}));
  EXPECT_FALSE(check(ua, {std::nullopt, 2}));
  /* The clustered index: k. */
  EXPECT_TRUE(check(clust, {1}));
  EXPECT_FALSE(check(clust, {2}));

  /* Delete marked: no longer a duplicate. */
  trx = begin();
  ASSERT_EQ(del(trx, 1), DB_SUCCESS);
  EXPECT_FALSE(check(ua, {10, 2}));
  EXPECT_FALSE(check(clust, {1}));
  rollback(trx);

  EXPECT_TRUE(check(ua, {10, 2}));
  mem_heap_free(heap);
}

/** Character keys compare with space padding: 'ab' and 'ab  ' are the
same primary key; 'ab\t' (below the pad) and 'ab!' (above) are not. */
TEST_F(RowInsIdxTest, VarcharPkPadding) {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();

  m_name = std::string("test/") + info->name();

  ASSERT_EQ(
    create_table(*m_sys, m_name, {{"s", IB_VARCHAR, IB_COL_NOT_NULL, 20}, {"v", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {{"s", 0}}, true, false}}),
    DB_SUCCESS
  );

  auto put = [&](Trx *trx, const std::string &s) {
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, m_name.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto tpl = crsr->clust_read_tuple_create();

    EXPECT_EQ(tpl->set_value(0, s.data(), s.size()), DB_SUCCESS);
    EXPECT_EQ(tpl->write_u32(1, 1), DB_SUCCESS);

    const auto err = crsr->insert_row(*m_sys, tpl);

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  };

  auto trx = begin();

  EXPECT_EQ(put(trx, "ab"), DB_SUCCESS);
  EXPECT_EQ(put(trx, "ab  "), DB_DUPLICATE_KEY);
  EXPECT_EQ(put(trx, "ab\t"), DB_SUCCESS);
  EXPECT_EQ(put(trx, "ab!"), DB_SUCCESS);
  EXPECT_EQ(put(trx, "a"), DB_SUCCESS);
  commit(trx);

  std::vector<std::string> keys;

  for (const auto &r : index_recs(*m_sys, index(nullptr))) {
    keys.push_back(*r.m_fields[0]);
  }

  EXPECT_EQ(keys, (std::vector<std::string>{"a", "ab\t", "ab", "ab!"}));
}

/** A unique index on a column prefix: values that agree in the prefix are
duplicates. */
TEST_F(RowInsIdxTest, UniquePrefix) {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();

  m_name = std::string("test/") + info->name();

  ASSERT_EQ(
    create_table(
      *m_sys,
      m_name,
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"s", IB_VARCHAR, IB_COL_NONE, 40}},
      {{"PRIMARY", {{"k", 0}}, true, false}, {"US", {{"s", 3}}, false, true}}
    ),
    DB_SUCCESS
  );

  auto put = [&](Trx *trx, uint32_t k, const std::string &s) {
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, m_name.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto tpl = crsr->clust_read_tuple_create();

    EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    EXPECT_EQ(tpl->set_value(1, s.data(), s.size()), DB_SUCCESS);

    const auto err = crsr->insert_row(*m_sys, tpl);

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  };

  auto trx = begin();

  EXPECT_EQ(put(trx, 1, "abcX"), DB_SUCCESS);
  EXPECT_EQ(put(trx, 2, "abcY"), DB_DUPLICATE_KEY);
  EXPECT_EQ(put(trx, 3, "abd"), DB_SUCCESS);
  EXPECT_EQ(put(trx, 4, "ab"), DB_SUCCESS);
  commit(trx);

  /* The index holds the 3 byte prefixes. */
  std::vector<std::string> prefixes;

  for (const auto &r : index_recs(*m_sys, index("US"))) {
    prefixes.push_back(*r.m_fields[0]);
  }

  EXPECT_EQ(prefixes, (std::vector<std::string>{"ab", "abc", "abd"}));
}

/** Composite primary key of a signed and an unsigned integer: duplicates
only when both match, order by the first then the second. */
TEST_F(RowInsIdxTest, CompositeSignedPk) {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();

  m_name = std::string("test/") + info->name();

  ASSERT_EQ(
    create_table(
      *m_sys,
      m_name,
      {{"x", IB_INT, IB_COL_NONE, 4}, {"y", IB_INT, IB_COL_UNSIGNED, 2}},
      {{"PRIMARY", {{"x", 0}, {"y", 0}}, true, false}}
    ),
    DB_SUCCESS
  );

  auto put = [&](Trx *trx, int32_t x, uint16_t y) {
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, m_name.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto tpl = crsr->clust_read_tuple_create();

    EXPECT_EQ(tpl->write_i32(0, x), DB_SUCCESS);
    EXPECT_EQ(tpl->write_u16(1, y), DB_SUCCESS);

    const auto err = crsr->insert_row(*m_sys, tpl);

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  };

  auto trx = begin();

  EXPECT_EQ(put(trx, -1, 5), DB_SUCCESS);
  EXPECT_EQ(put(trx, 1, 5), DB_SUCCESS);
  EXPECT_EQ(put(trx, -1, 6), DB_SUCCESS);
  EXPECT_EQ(put(trx, INT32_MIN, 65535), DB_SUCCESS);
  EXPECT_EQ(put(trx, INT32_MAX, 0), DB_SUCCESS);
  EXPECT_EQ(put(trx, -1, 5), DB_DUPLICATE_KEY);
  EXPECT_EQ(put(trx, 1, 5), DB_DUPLICATE_KEY);
  commit(trx);

  std::vector<std::pair<int32_t, uint16_t>> got;

  for (const auto &r : index_recs(*m_sys, index(nullptr))) {
    int32_t x{};
    uint16_t y{};

    mach_read_int_type(&x, reinterpret_cast<const byte *>(r.m_fields[0]->data()), 4, false);
    mach_read_int_type(&y, reinterpret_cast<const byte *>(r.m_fields[1]->data()), 2, true);
    got.emplace_back(x, y);
  }

  EXPECT_EQ(got, (std::vector<std::pair<int32_t, uint16_t>>{{INT32_MIN, 65535}, {-1, 5}, {-1, 6}, {1, 5}, {INT32_MAX, 0}}));
}

/** An insert rolled back leaves no record in either index. */
TEST_F(RowInsIdxTest, RollbackRemoves) {
  m_tab = tab();
  m_tab->load({{1, ut::Row{1, 1, "a"}}});

  auto trx = begin();

  for (uint32_t k = 2; k < 500; ++k) {
    ASSERT_EQ(m_tab->insert(trx, ut::Row{k, k, ut::blob(k, 60)}), DB_SUCCESS);
  }

  rollback(trx);

  EXPECT_EQ(index_recs(*m_sys, m_tab->clust()).size(), 1UL);
  EXPECT_EQ(index_recs(*m_sys, m_tab->sec()).size(), 1UL);
  m_tab->expect_rows({{1, ut::Row{1, 1, "a"}}});
}

/** An insert takes an implicit lock only: no explicit record lock is
created for the inserted record (the trx id in the record is the lock). */
TEST_F(RowInsIdxTest, InsertNoExplicitLock) {
  m_tab = tab();

  auto trx = begin();

  ASSERT_EQ(m_tab->insert(trx, ut::Row{1, 1, "a"}), DB_SUCCESS);

  /* The record is there, not delete marked, with the transaction's id. */
  const auto recs = index_recs(*m_sys, m_tab->clust());

  ASSERT_EQ(recs.size(), 1UL);
  EXPECT_FALSE(recs[0].m_deleted);
  EXPECT_EQ(mach_read_from_8(reinterpret_cast<const byte *>(recs[0].m_fields[1]->data())), trx->m_id);
  commit(trx);
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

using RowInsPerf = RowInsIdxTest;

/** Inserts through RowInsert (via the api): sequential and random keys,
one transaction per 1000 rows, both indexes maintained. */
TEST_F(RowInsPerf, Insert) {
  m_tab = tab();

  constexpr uint32_t N = 50000;
  Trx *trx{};

  auto rs = ut::bench("row_insert_seq", N, [&](uint64_t i) {
    if (i % 1000 == 0) {
      trx = begin();
    }

    (void)m_tab->insert(trx, ut::Row{uint32_t(i), uint32_t(i % 97), "abcdefghij"});

    if (i % 1000 == 999) {
      commit(trx);
    }
  });

  EXPECT_LT(rs.ns_per_op(), 2e6);

  std::vector<uint32_t> keys(N);

  std::iota(keys.begin(), keys.end(), N);
  std::mt19937 rng(3);
  std::shuffle(keys.begin(), keys.end(), rng);

  auto rr = ut::bench("row_insert_rnd", N, [&](uint64_t i) {
    if (i % 1000 == 0) {
      trx = begin();
    }

    (void)m_tab->insert(trx, ut::Row{keys[i], uint32_t(i % 97), "abcdefghij"});

    if (i % 1000 == 999) {
      commit(trx);
    }
  });

  EXPECT_LT(rr.ns_per_op(), 2e6);
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

/** A shared instance for the performance tests below: perf_config(). */
class RowInsMtPerf : public RowInsIdxTest {
 protected:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    s_config = perf_config(s_dir->m_path);
    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
  }

  /** Insert n rows with keys key(i) and b of len bytes in one thread, a
  commit every per_trx rows. */
  void insert_rows(const Tab &tab, uint64_t n, uint64_t per_trx, const std::function<uint32_t(uint64_t)> &key, size_t len) {
    const auto b = std::string(len, 'b');
    Trx *trx{};

    for (uint64_t i{}; i < n; ++i) {
      if (i % per_trx == 0) {
        trx = begin();
      }

      const auto k = key(i);

      ASSERT_EQ(tab.insert(trx, ut::Row{k, k % 97, b}), DB_SUCCESS) << k;

      if (i % per_trx == per_trx - 1 || i + 1 == n) {
        commit(trx);
      }
    }
  }

  /** @return a table of the test's own, test/<test>_<suffix>. */
  [[nodiscard]] std::unique_ptr<Tab> tab_named(const std::string &suffix) { return tab(std::format("_{}", suffix).c_str()); }

  /** @return the number of rows of tab through the clustered index. */
  size_t count(const Tab &tab) {
    auto trx = begin();
    const auto n = tab.scan(trx).size();

    commit(trx);

    return n;
  }
};

/** Single threaded inserts of rows of 50 bytes, 1 KB and 20 KB (b stored
externally), sequential keys, both indexes maintained. */
TEST_F(RowInsMtPerf, RowSizes) {
  struct Size {
    const char *m_name;
    size_t m_b;
    uint64_t m_n;
    uint64_t m_per_trx;
  };

  for (const auto &sz : {Size{"insert_50B", 42, 20000, 1000}, Size{"insert_1KB", 1016, 3000, 500}, Size{"insert_20KB", 20000, 300, 50}}) {
    auto tab = tab_named(sz.m_name);
    const auto b = std::string(sz.m_b, 'b');
    Trx *trx{};

    auto r = ut::bench(sz.m_name, sz.m_n, [&](uint64_t i) {
      if (i % sz.m_per_trx == 0) {
        trx = begin();
      }

      EXPECT_EQ(tab->insert(trx, ut::Row{uint32_t(i), uint32_t(i % 97), b}), DB_SUCCESS);

      if (i % sz.m_per_trx == sz.m_per_trx - 1) {
        commit(trx);
      }
    });

    EXPECT_LT(r.ns_per_op(), 5e6) << sz.m_name;
    EXPECT_EQ(count(*tab), sz.m_n);
  }
}

/** 1 to 16 threads inserting disjoint key ranges into one table, a commit
every 200 rows; the table then holds them all. */
TEST_F(RowInsMtPerf, Threads) {
  constexpr uint64_t N = 2000;

  for (int threads : {1, 2, 4, 8, 16}) {
    auto tab = tab_named(std::format("t{}", threads));
    std::vector<Trx *> trxs(threads);
    Watchdog watchdog(std::chrono::seconds(300));

    auto r = ut::bench_mt("insert_50B_mt", threads, N, [&](int t, uint64_t i) {
      if (i % 200 == 0) {
        trxs[t] = begin();
      }

      const auto k = uint32_t(t) << 20 | uint32_t(i);

      EXPECT_EQ(tab->insert(trxs[t], ut::Row{k, k % 97, "0123456789012345678901234567890123456789ab"}), DB_SUCCESS);

      if (i % 200 == 199) {
        commit(trxs[t]);
      }
    });

    EXPECT_LT(r.ns_per_op(), 5e6);
    EXPECT_EQ(count(*tab), N * threads);
  }
}

/** 8 threads inserting interleaved keys (thread t: i * 8 + t), all into the
same leaf pages, and few secondary key values (k % 7): the latching of the
pages, their splits and the duplicate checks under contention. Both
indexes hold every row in order afterwards. */
TEST_F(RowInsMtPerf, Interleaved) {
  constexpr int T = 8;
  constexpr uint64_t N = 1500;

  auto tab = tab_named("i");
  std::vector<Trx *> trxs(T);
  Watchdog watchdog(std::chrono::seconds(300));

  auto r = ut::bench_mt("insert_interleaved_mt", T, N, [&](int t, uint64_t i) {
    if (i % 100 == 0) {
      trxs[t] = begin();
    }

    const auto k = uint32_t(i * T + t);

    EXPECT_EQ(tab->insert(trxs[t], ut::Row{k, k % 7, "x"}), DB_SUCCESS);

    if (i % 100 == 99) {
      commit(trxs[t]);
    }
  });

  EXPECT_LT(r.ns_per_op(), 5e6);

  ut::Rows rows;

  for (uint32_t k{}; k < N * T; ++k) {
    rows[k] = ut::Row{k, k % 7, "x"};
  }

  tab->expect_rows(rows);
}

/** Rows of 20 KB (b external) from 1, 4 and 8 threads. */
TEST_F(RowInsMtPerf, BlobThreads) {
  /* The system tablespace is 32 MB: beyond it, extensions are synced. */
  constexpr uint64_t N = 40;
  const auto b = ut::blob(1, 20000);

  for (int threads : {1, 4, 8}) {
    auto tab = tab_named(std::format("b{}", threads));
    std::vector<Trx *> trxs(threads);
    Watchdog watchdog(std::chrono::seconds(300));

    auto r = ut::bench_mt("insert_20KB_mt", threads, N, [&](int t, uint64_t i) {
      if (i % 20 == 0) {
        trxs[t] = begin();
      }

      const auto k = uint32_t(t) << 20 | uint32_t(i);

      EXPECT_EQ(tab->insert(trxs[t], ut::Row{k, k, b}), DB_SUCCESS);

      if (i % 20 == 19) {
        commit(trxs[t]);
      }
    });

    EXPECT_LT(r.ns_per_op(), 2e7);
    EXPECT_EQ(count(*tab), N * threads);
  }
}

}  // namespace
