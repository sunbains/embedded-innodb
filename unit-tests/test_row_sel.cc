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

/** @file unit-tests/test_row_sel.cc
RowSel: positioning cursors in every search mode and match mode on the
clustered index and on a secondary index with and without access to the
clustered record (the row found and prebuilt->m_result), scans in both
directions, delete marked records, consistent reads through old versions,
the record locks of locking reads (exact, range, not found, secondary,
to the end), and RowSel::mvcc_fetch() driven directly. RowSelTest runs on
a fresh instance per test, RowSelIdxTest on tables of its own in a shared
instance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "row0prebuilt.h"
#include "row0sel.h"
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
#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

namespace {

using RowSelTest = ut::DbTest;

ut::Rows make_rows(uint32_t n, uint32_t step = 1) {
  ut::Rows rows;

  for (uint32_t k = 0; k < n * step; k += step) {
    rows[k] = ut::Row{k, k * 2, ut::blob(k, 40)};
  }

  return rows;
}

/** A search on the secondary index with access to the clustered record
reports an exact match. The result was taken from comparing the search
tuple, in SEC_A order, with the clustered record: it never matched. */
TEST_F(RowSelTest, SecExact) {
  const auto rows = make_rows(50);

  load(rows);

  auto trx = begin();

  for (const auto &[k, row] : rows) {
    auto found = find_sec(trx, row.m_a);

    ASSERT_TRUE(found.has_value()) << "a " << row.m_a;
    EXPECT_EQ(*found, row);

    /* Odd values of a don't exist. */
    EXPECT_FALSE(find_sec(trx, row.m_a + 1).has_value()) << "a " << row.m_a + 1;
  }

  commit(trx);
}

/** The same with the rows changed by the same transaction. */
TEST_F(RowSelTest, SecExactOwnChanges) {
  load(make_rows(20));

  auto trx = begin();

  ASSERT_EQ(update(trx, 3, ut::Row{3, 1001, "x"}), DB_SUCCESS);
  ASSERT_EQ(insert(trx, ut::Row{100, 1003, "y"}), DB_SUCCESS);

  EXPECT_EQ(find_sec(trx, 1001)->m_k, 3u);
  EXPECT_EQ(find_sec(trx, 1003)->m_k, 100u);
  EXPECT_FALSE(find_sec(trx, 6).has_value());

  commit(trx);
}

TEST_F(RowSelTest, Empty) {
  auto trx = begin();

  EXPECT_TRUE(scan(trx).empty());
  EXPECT_TRUE(scan_sec(trx).empty());
  EXPECT_FALSE(find_sec(trx, 1).has_value());

  commit(trx);
}

/** Scans skip the delete marked records, also when many are in a row. */
TEST_F(RowSelTest, SkipDeleted) {
  auto rows = make_rows(500);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 500; ++k) {
    if (k % 50 != 7) {
      ASSERT_EQ(remove(trx, k), DB_SUCCESS);
      rows.erase(k);
    }
  }

  commit(trx);
  expect_rows(rows);
}

/** Duplicate secondary keys: SEC_A is ordered by (a, k). */
TEST_F(RowSelTest, SecDuplicates) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 300; ++k) {
    rows[k] = ut::Row{299 - k, k % 3, ut::blob(k, 10)};
  }

  ut::Rows by_k;

  for (const auto &[i, row] : rows) {
    by_k[row.m_k] = row;
  }

  load(by_k);
  expect_rows(by_k);

  auto trx = begin();

  for (uint32_t a = 0; a < 3; ++a) {
    auto found = find_sec(trx, a);

    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->m_a, a);

    /* The first of the duplicates, in k order. */
    uint32_t first{UINT32_MAX};

    for (const auto &[k, row] : by_k) {
      if (row.m_a == a) {
        first = std::min(first, k);
      }
    }

    EXPECT_EQ(found->m_k, first);
  }

  commit(trx);
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

  if (auto err = api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1, 0); err != DB_SUCCESS) {
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

/* --- Positioning, scans, consistent and locking reads, shared instance -- */

namespace {

/** The result of a positioning: the error, prebuilt->m_result and the row. */
struct At {
  DbErr m_err{DB_ERROR};
  int m_result{-2};
  std::optional<ut::Row> m_row{};
};

/** An api cursor on a Tab: the clustered index, or SEC_A with or without
access to the clustered record, consistent or locking. The positioning
goes through RowSel::mvcc_fetch() via the api cursor or directly. */
class Cur {
 public:
  Cur(System &sys, const Tab &tab, Trx *trx, bool sec = false, bool cluster_access = true, CursorLockMode lock = IB_LOCK_NONE)
    : m_sys(sys), m_sec(sec), m_cluster_access(cluster_access) {
    EXPECT_EQ(tab.open(trx, lock == IB_LOCK_X ? IB_LOCK_IX : IB_LOCK_IS, &m_tbl), DB_SUCCESS);

    if (lock == IB_LOCK_X || lock == IB_LOCK_S) {
      EXPECT_EQ(m_tbl->set_lock_mode(m_sys, lock), DB_SUCCESS);
    }

    if (sec) {
      EXPECT_EQ(m_tbl->open_index(m_sys, ut::SEC_A, &m_crsr), DB_SUCCESS);

      if (cluster_access) {
        m_crsr->set_cluster_access();
      }

      if (lock == IB_LOCK_X || lock == IB_LOCK_S) {
        EXPECT_EQ(m_crsr->set_lock_mode(m_sys, lock), DB_SUCCESS);
      }
    } else {
      m_crsr = m_tbl;
    }
  }

  ~Cur() { close(); }

  /** Close the cursors: must be done before the trx is committed. */
  void close() {
    if (m_key != nullptr) {
      api::Tuple::destroy(m_key);
      m_key = nullptr;
    }

    if (m_tbl == nullptr) {
      return;
    }

    if (m_crsr != m_tbl) {
      (void)api::Cursor::close(m_sys, m_crsr);
    }

    (void)api::Cursor::close(m_sys, m_tbl);
    m_tbl = m_crsr = nullptr;
  }

  Cur(const Cur &) = delete;
  Cur &operator=(const Cur &) = delete;

  /** @return the row under the cursor; a from SEC_A without cluster access. */
  std::optional<ut::Row> row() {
    if (m_sec && !m_cluster_access) {
      auto tpl = m_crsr->sec_read_tuple_create();
      std::optional<ut::Row> r;

      if (m_crsr->read_row(m_sys, tpl) == DB_SUCCESS) {
        ut::Row row;

        /* A row tuple: the fields are in column order (k, a), not in index order. */
        EXPECT_EQ(tpl->read_u32(0, &row.m_k), DB_SUCCESS);
        EXPECT_EQ(tpl->read_u32(1, &row.m_a), DB_SUCCESS);
        r = row;
      }

      api::Tuple::destroy(tpl);
      return r;
    }

    auto tpl = m_crsr->clust_read_tuple_create();
    std::optional<ut::Row> r;

    if (m_crsr->read_row(m_sys, tpl) == DB_SUCCESS) {
      r = Tab::read_row(tpl);
    }

    api::Tuple::destroy(tpl);

    return r;
  }

  /** Position on key (k or a) with mode and match mode. */
  At moveto(uint32_t key, SearchMode mode, MatchMode match = IB_CLOSEST_MATCH) {
    /* The prebuilt search tuple is a shallow copy of the key: the key must
    live as long as the search tuple is used (mvcc_fetch() with it kept). */
    if (m_key != nullptr) {
      api::Tuple::destroy(m_key);
    }

    m_key = m_sec ? m_crsr->sec_search_tuple_create() : m_crsr->clust_search_tuple_create();

    At at;

    EXPECT_EQ(m_key->write_u32(0, key), DB_SUCCESS);
    m_crsr->set_match_mode(match);
    at.m_err = m_crsr->moveto(m_sys, m_key, mode, &at.m_result);

    if (at.m_err == DB_SUCCESS) {
      at.m_row = row();
    }

    return at;
  }

  At next() { return step(m_crsr->next(m_sys)); }
  At prev() { return step(m_crsr->prev(m_sys)); }
  At first() { return step(m_crsr->first(m_sys)); }
  At last() { return step(m_crsr->last(m_sys)); }

  [[nodiscard]] Prebuilt *prebuilt() const { return m_crsr->m_prebuilt; }

 private:
  At step(DbErr err) {
    At at;

    at.m_err = err;
    at.m_result = m_crsr->m_prebuilt->m_result;

    if (err == DB_SUCCESS) {
      at.m_row = row();
    }

    return at;
  }

  System &m_sys;
  bool m_sec{};
  bool m_cluster_access{};
  api::Cursor *m_tbl{};
  api::Cursor *m_crsr{};
  api::Tuple *m_key{};
};

/** A record lock of a transaction: page, the key (first field) of the
record (-1 for the supremum) and the precise mode. */
struct RecLock {
  page_no_t m_page_no;
  int64_t m_key;
  ulint m_mode;

  bool operator<(const RecLock &o) const { return std::tie(m_page_no, m_key, m_mode) < std::tie(o.m_page_no, o.m_key, o.m_mode); }
};

class RowSelIdxTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();
    m_tab = tab();
  }

  void TearDown() override { m_tab.reset(); }

  /** Rows k = 10, 20, ... 10 * n, a = 2 * k, b of 100 bytes: many pages. */
  ut::Rows load(uint32_t n) {
    ut::Rows rows;

    for (uint32_t i = 1; i <= n; ++i) {
      rows[i * 10] = ut::Row{i * 10, i * 20, ut::blob(i, 100)};
    }

    m_tab->load(rows);

    return rows;
  }

  /** @return the record locks of trx, with the keys of the records. */
  std::vector<RecLock> locks_of(const Trx *trx, const Index *index) {
    std::vector<std::pair<PageId, std::pair<ulint, ulint>>> raw;

    /* All the page shards of the lock system: latched X. */
    m_sys->m_lock_sys->x_lock();

    for (auto &shard : m_sys->m_lock_sys->m_page_shards) {
      for (const auto &[page_id, locks] : shard.m_locks) {
        for (auto lock : locks) {
          if (lock->m_trx != trx) {
            continue;
          }

          for (ulint h{}; h < lock->rec_get_n_bits(); ++h) {
            if (lock->rec_is_nth_bit_set(h)) {
              /* The mode and the gap flags, without LOCK_REC. */
              raw.emplace_back(page_id, std::pair(h, ulint(lock->precise_mode() & ~LOCK_TYPE_MASK)));
            }
          }
        }
      }
    }

    m_sys->m_lock_sys->x_unlock();

    std::vector<RecLock> v;

    for (const auto &[page_id, hm] : raw) {
      if (page_id.space_id() != index->get_space_id()) {
        continue;
      }

      Mtr mtr;

      mtr.start();

      auto page = m_sys->m_btree->page_get(page_id.space_id(), page_id.page_no(), RW_S_LATCH, &mtr);

      if (m_sys->m_btree->page_get_index_id(page) == index->m_id) {
        auto rec = page_find_rec_with_heap_no(page, hm.first);
        int64_t key{-1};

        if (!rec.is_null() && page_rec_is_user_rec(rec)) {
          ulint len;

          key = mach_read_from_4(rec.get_nth_field(0, &len));
        }

        v.push_back(RecLock{page_id.page_no(), key, hm.second});
      }

      mtr.commit(*m_sys);
    }

    std::sort(v.begin(), v.end());

    return v;
  }

  /** @return the keys locked by trx in index with exactly the precise mode. */
  std::vector<int64_t> keys_locked(const Trx *trx, const Index *index, ulint mode) {
    std::vector<int64_t> v;

    for (const auto &l : locks_of(trx, index)) {
      if (l.m_mode == mode) {
        v.push_back(l.m_key);
      }
    }

    std::sort(v.begin(), v.end());
    return v;
  }

  std::unique_ptr<Tab> m_tab;
};

/** One case of a positioning: the key, the mode and what is expected. */
struct Case {
  uint32_t m_key;
  SearchMode m_mode;
  DbErr m_err;
  uint32_t m_k;
  int m_result;
};

/** moveto() on the clustered index in every mode: on keys that exist,
between keys, before the first and after the last, the row found and
the result of the comparison of the search tuple with it. */
TEST_F(RowSelIdxTest, ClustModes) {
  load(300);

  const std::vector<Case> cases{
    {1500, IB_CUR_GE, DB_SUCCESS, 1500, 0},  {1505, IB_CUR_GE, DB_SUCCESS, 1510, -1}, {1500, IB_CUR_G, DB_SUCCESS, 1510, -1},
    {1505, IB_CUR_G, DB_SUCCESS, 1510, -1},  {1500, IB_CUR_LE, DB_SUCCESS, 1500, 0},  {1505, IB_CUR_LE, DB_SUCCESS, 1500, 1},
    {1500, IB_CUR_L, DB_SUCCESS, 1490, 1},   {1505, IB_CUR_L, DB_SUCCESS, 1500, 1},   {0, IB_CUR_GE, DB_SUCCESS, 10, -1},
    {10, IB_CUR_GE, DB_SUCCESS, 10, 0},      {10, IB_CUR_LE, DB_SUCCESS, 10, 0},      {3000, IB_CUR_LE, DB_SUCCESS, 3000, 0},
    {9999, IB_CUR_LE, DB_SUCCESS, 3000, 1},  {9999, IB_CUR_L, DB_SUCCESS, 3000, 1},   {3000, IB_CUR_GE, DB_SUCCESS, 3000, 0},
    {3000, IB_CUR_G, DB_END_OF_INDEX, 0, -1}, {9999, IB_CUR_GE, DB_END_OF_INDEX, 0, -1}, {10, IB_CUR_L, DB_END_OF_INDEX, 0, -1},
    {5, IB_CUR_LE, DB_END_OF_INDEX, 0, -1},
  };

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx);

  for (const auto &c : cases) {
    const auto at = cur.moveto(c.m_key, c.m_mode);

    EXPECT_EQ(at.m_err, c.m_err) << c.m_key << " mode " << c.m_mode;

    if (c.m_err == DB_SUCCESS && at.m_err == DB_SUCCESS) {
      ASSERT_TRUE(at.m_row.has_value());
      EXPECT_EQ(at.m_row->m_k, c.m_k) << c.m_key << " mode " << c.m_mode;
      EXPECT_EQ(at.m_row->m_a, c.m_k * 2);
      EXPECT_EQ(at.m_result, c.m_result) << c.m_key << " mode " << c.m_mode;
    }
  }

  cur.close();
  commit(trx);
}

/** The match modes on the clustered index: EXACT finds the key or
nothing; EXACT_PREFIX the same for a full key. */
TEST_F(RowSelIdxTest, ClustMatchModes) {
  load(100);

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx);

  auto at = cur.moveto(500, IB_CUR_GE, IB_EXACT_MATCH);

  EXPECT_EQ(at.m_err, DB_SUCCESS);
  EXPECT_EQ(at.m_result, 0);
  ASSERT_TRUE(at.m_row.has_value());
  EXPECT_EQ(at.m_row->m_k, 500U);

  at = cur.moveto(505, IB_CUR_GE, IB_EXACT_MATCH);
  EXPECT_EQ(at.m_err, DB_RECORD_NOT_FOUND);
  EXPECT_NE(at.m_result, 0);

  at = cur.moveto(505, IB_CUR_GE, IB_EXACT_PREFIX);
  EXPECT_EQ(at.m_err, DB_RECORD_NOT_FOUND);

  at = cur.moveto(510, IB_CUR_GE, IB_EXACT_PREFIX);
  EXPECT_EQ(at.m_err, DB_SUCCESS);
  EXPECT_EQ(at.m_row->m_k, 510U);

  /* Beyond the end. */
  at = cur.moveto(99999, IB_CUR_GE, IB_EXACT_MATCH);
  EXPECT_EQ(at.m_err, DB_RECORD_NOT_FOUND);

  cur.close();
  commit(trx);
}

/** moveto() on SEC_A, with and without access to the clustered record:
the same rows (a = 2k) and results as on the clustered index. */
TEST_F(RowSelIdxTest, SecModes) {
  load(300);

  const std::vector<Case> cases{
    {3000, IB_CUR_GE, DB_SUCCESS, 1500, 0},  {3005, IB_CUR_GE, DB_SUCCESS, 1510, -1}, {3000, IB_CUR_G, DB_SUCCESS, 1510, -1},
    {3000, IB_CUR_LE, DB_SUCCESS, 1500, 0},  {3005, IB_CUR_LE, DB_SUCCESS, 1500, 1},  {3000, IB_CUR_L, DB_SUCCESS, 1490, 1},
    {0, IB_CUR_GE, DB_SUCCESS, 10, -1},      {6000, IB_CUR_G, DB_END_OF_INDEX, 0, -1}, {20, IB_CUR_L, DB_END_OF_INDEX, 0, -1},
    {99999, IB_CUR_L, DB_SUCCESS, 3000, 1},
  };

  for (bool cluster_access : {true, false}) {
    auto trx = begin();
    Cur cur(*m_sys, *m_tab, trx, true, cluster_access);

    for (const auto &c : cases) {
      const auto at = cur.moveto(c.m_key, c.m_mode);

      EXPECT_EQ(at.m_err, c.m_err) << c.m_key << " mode " << c.m_mode << " cluster " << cluster_access;

      if (c.m_err == DB_SUCCESS && at.m_err == DB_SUCCESS) {
        ASSERT_TRUE(at.m_row.has_value());
        EXPECT_EQ(at.m_row->m_k, c.m_k) << c.m_key << " mode " << c.m_mode << " cluster " << cluster_access;
        EXPECT_EQ(at.m_row->m_a, c.m_k * 2);
        EXPECT_EQ(at.m_result, c.m_result) << c.m_key << " mode " << c.m_mode << " cluster " << cluster_access;

        if (cluster_access) {
          EXPECT_EQ(at.m_row->m_b, ut::blob(c.m_k / 10, 100));
        }
      }
    }

    cur.close();
    commit(trx);
  }
}

/** Duplicates in SEC_A: GE with EXACT_PREFIX on a goes to the first of
them (lowest k). The api's next() fetches with ROW_SEL_DEFAULT and runs on
past them; mvcc_fetch() with the match mode and the search tuple kept
stops after the last with DB_RECORD_NOT_FOUND. */
TEST_F(RowSelIdxTest, SecPrefixScan) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 500; ++k) {
    rows[k] = ut::Row{k, k % 5, "x"};
  }

  m_tab->load(rows);

  std::vector<uint32_t> expected;

  for (uint32_t k = 3; k < 500; k += 5) {
    expected.push_back(k);
  }

  for (bool cluster_access : {true, false}) {
    /* ROW_SEL_EXACT on SEC_A (not unique) caches the rows: see ExactThenNext. */
    for (auto [match, sel_match] : {std::pair(IB_EXACT_PREFIX, ROW_SEL_EXACT_PREFIX)}) {
      auto trx = begin();
      Cur cur(*m_sys, *m_tab, trx, true, cluster_access);
      std::vector<uint32_t> ks;
      auto at = cur.moveto(3, IB_CUR_GE, match);
      auto prebuilt = cur.prebuilt();
      DbErr err = at.m_err;

      while (err == DB_SUCCESS && ks.size() <= 200) {
        auto row = cur.row();

        ASSERT_TRUE(row.has_value());
        EXPECT_EQ(row->m_a, 3U);
        ks.push_back(row->m_k);

        prebuilt->m_row_cache.cache_next();
        err = m_sys->m_row_sel->mvcc_fetch(IB_RECOVERY_DEFAULT, IB_CUR_G, prebuilt, sel_match, ROW_SEL_NEXT);
      }

      EXPECT_EQ(err, DB_RECORD_NOT_FOUND) << "cluster " << cluster_access << " match " << match;
      EXPECT_EQ(ks, expected) << "cluster " << cluster_access << " match " << match;

      /* The api's next() ignores the match mode: from the first a = 3 it
      goes on to a = 4. */
      at = cur.moveto(3, IB_CUR_GE, match);
      ASSERT_EQ(at.m_err, DB_SUCCESS);

      for (size_t i = 1; i < expected.size(); ++i) {
        ASSERT_EQ(cur.next().m_err, DB_SUCCESS);
      }

      at = cur.next();
      ASSERT_EQ(at.m_err, DB_SUCCESS);
      EXPECT_EQ(at.m_row->m_a, 4U);
      cur.close();
      commit(trx);
    }
  }
}

/** moveto() with IB_EXACT_MATCH on a non-unique index caches every
matching row; next() then returns them from the cache. */
TEST_F(RowSelIdxTest, ExactThenNext) {
  /* Regression: in RowSel::mvcc_fetch() (row/row0sel.cc:2077) the row cache's
  direction is only set by a NEXT/PREV fetch that finds the cache empty.
  A MOVETO with ROW_SEL_EXACT on a non-unique index fills the cache with
  all the matching rows (row0sel.cc:2681) but leaves m_direction at
  ROW_SEL_UNDEFINED, so the api's next() finds a non-empty cache with
  another direction and hits ut_error (row0sel.cc:2080). Upstream HailDB
  has the same code; the fix is to set m_row_cache.m_direction to
  ROW_SEL_NEXT (moves_up) or ROW_SEL_PREV when a MOVETO caches rows. */
  ut::Rows rows;

  for (uint32_t k = 0; k < 50; ++k) {
    rows[k] = ut::Row{k, k % 5, "x"};
  }

  m_tab->load(rows);

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx, true, true);
  auto at = cur.moveto(3, IB_CUR_GE, IB_EXACT_MATCH);

  ASSERT_EQ(at.m_err, DB_SUCCESS);
  EXPECT_EQ(at.m_row->m_k, 3U);
  at = cur.next();
  ASSERT_EQ(at.m_err, DB_SUCCESS);
  EXPECT_EQ(at.m_row->m_k, 8U);
  cur.close();
  commit(trx);
}

/** next() and prev() from a position, back and forth across pages. */
TEST_F(RowSelIdxTest, NextPrev) {
  load(300);

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx);

  auto at = cur.moveto(1500, IB_CUR_GE);

  ASSERT_EQ(at.m_err, DB_SUCCESS);
  EXPECT_EQ(cur.next().m_row->m_k, 1510U);
  EXPECT_EQ(cur.next().m_row->m_k, 1520U);
  EXPECT_EQ(cur.prev().m_row->m_k, 1510U);
  EXPECT_EQ(cur.prev().m_row->m_k, 1500U);
  EXPECT_EQ(cur.prev().m_row->m_k, 1490U);

  /* From the last backwards over the whole index. */
  std::vector<uint32_t> ks;

  for (at = cur.last(); at.m_err == DB_SUCCESS; at = cur.prev()) {
    ks.push_back(at.m_row->m_k);
  }

  EXPECT_EQ(at.m_err, DB_END_OF_INDEX);
  ASSERT_EQ(ks.size(), 300UL);

  for (uint32_t i = 0; i < 300; ++i) {
    ASSERT_EQ(ks[i], 3000 - i * 10);
  }

  /* And forwards. */
  ks.clear();

  for (at = cur.first(); at.m_err == DB_SUCCESS; at = cur.next()) {
    ks.push_back(at.m_row->m_k);
  }

  EXPECT_EQ(ks.size(), 300UL);
  EXPECT_EQ(ks.front(), 10U);
  EXPECT_EQ(ks.back(), 3000U);
  EXPECT_EQ(cur.next().m_err, DB_END_OF_INDEX);

  cur.close();
  commit(trx);
}

/** A scan through SEC_A backwards, with cluster access. */
TEST_F(RowSelIdxTest, SecBackwards) {
  load(200);

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx, true, true);
  std::vector<uint32_t> ks;

  for (auto at = cur.moveto(2000, IB_CUR_LE); at.m_err == DB_SUCCESS; at = cur.prev()) {
    ks.push_back(at.m_row->m_k);
  }

  ASSERT_EQ(ks.size(), 100UL);
  EXPECT_EQ(ks.front(), 1000U);
  EXPECT_EQ(ks.back(), 10U);
  cur.close();
  commit(trx);
}

/** A consistent read (REPEATABLE READ) keeps seeing a row deleted and an
update made after its read view was created, through both indexes; a new
transaction sees the changes. Delete marked records are skipped. */
TEST_F(RowSelIdxTest, ConsistentRead) {
  const auto rows = load(50);

  auto reader = begin();

  /* The first read creates the read view. */
  EXPECT_EQ(m_tab->scan(reader).size(), 50UL);

  auto writer = begin();

  ASSERT_EQ(m_tab->remove(writer, 100), DB_SUCCESS);
  ASSERT_EQ(m_tab->update(writer, 200, ut::Row{200, 9999, "new"}), DB_SUCCESS);
  ASSERT_EQ(m_tab->insert(writer, ut::Row{205, 7777, "ins"}), DB_SUCCESS);
  commit(writer);

  /* The reader sees the old rows. */
  EXPECT_EQ(m_tab->scan(reader), rows);
  EXPECT_EQ(m_tab->scan_sec(reader).size(), 50UL);

  {
    Cur cur(*m_sys, *m_tab, reader);
    auto at = cur.moveto(100, IB_CUR_GE, IB_EXACT_MATCH);

    EXPECT_EQ(at.m_err, DB_SUCCESS);
    EXPECT_EQ(at.m_row->m_a, 200U);

    at = cur.moveto(200, IB_CUR_GE, IB_EXACT_MATCH);
    EXPECT_EQ(at.m_row->m_a, 400U) << "the old version";

    at = cur.moveto(205, IB_CUR_GE, IB_EXACT_MATCH);
    EXPECT_EQ(at.m_err, DB_RECORD_NOT_FOUND) << "inserted after the read view";
  }

  {
    /* Through SEC_A with cluster access: the old a finds the old row, the
    new a finds nothing. */
    Cur cur(*m_sys, *m_tab, reader, true, true);
    auto at = cur.moveto(400, IB_CUR_GE, IB_EXACT_MATCH);

    EXPECT_EQ(at.m_err, DB_SUCCESS);
    EXPECT_EQ(at.m_row->m_k, 200U);
    EXPECT_EQ(at.m_result, 0);

    at = cur.moveto(9999, IB_CUR_GE, IB_EXACT_MATCH);
    EXPECT_EQ(at.m_err, DB_RECORD_NOT_FOUND);
  }

  commit(reader);

  /* A new reader sees the changes. */
  auto now = begin();
  auto seen = m_tab->scan(now);

  EXPECT_EQ(seen.size(), 50UL);
  EXPECT_FALSE(seen.contains(100));
  EXPECT_EQ(seen[200].m_a, 9999U);
  EXPECT_TRUE(seen.contains(205));
  commit(now);
}

/** A unique exact locking read of the clustered index locks only that
record, without the gap (LOCK_REC_NOT_GAP). */
TEST_F(RowSelIdxTest, LockExactRecNotGap) {
  load(50);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx, false, true, IB_LOCK_X);
    auto at = cur.moveto(200, IB_CUR_GE, IB_EXACT_MATCH);

    ASSERT_EQ(at.m_err, DB_SUCCESS);
  }

  const auto locks = locks_of(trx, m_tab->clust());

  ASSERT_EQ(locks.size(), 1UL);
  EXPECT_EQ(locks[0].m_key, 200);
  EXPECT_EQ(locks[0].m_mode, ulint(LOCK_X | LOCK_REC_NOT_GAP));
  commit(trx);
}

/** A locking range read in REPEATABLE READ: the first record found by GE
on its exact key is locked without its gap, the following ones with
next-key locks. */
TEST_F(RowSelIdxTest, LockRangeNextKey) {
  load(50);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx, false, true, IB_LOCK_S);

    ASSERT_EQ(cur.moveto(200, IB_CUR_GE).m_err, DB_SUCCESS);
    ASSERT_EQ(cur.next().m_err, DB_SUCCESS);
    ASSERT_EQ(cur.next().m_err, DB_SUCCESS);
  }

  EXPECT_EQ(keys_locked(trx, m_tab->clust(), LOCK_S | LOCK_REC_NOT_GAP), (std::vector<int64_t>{200}));
  EXPECT_EQ(keys_locked(trx, m_tab->clust(), LOCK_S | LOCK_ORDINARY), (std::vector<int64_t>{210, 220}));
  commit(trx);
}

/** A locking read of a key that doesn't exist locks the gap before the
next record in REPEATABLE READ, nothing in READ COMMITTED. */
TEST_F(RowSelIdxTest, LockGapNotFound) {
  load(50);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx, false, true, IB_LOCK_X);

    EXPECT_EQ(cur.moveto(205, IB_CUR_GE, IB_EXACT_MATCH).m_err, DB_RECORD_NOT_FOUND);
  }

  EXPECT_EQ(keys_locked(trx, m_tab->clust(), LOCK_X | LOCK_GAP), (std::vector<int64_t>{210}));
  commit(trx);

  trx = begin(IB_TRX_READ_COMMITTED);

  {
    Cur cur(*m_sys, *m_tab, trx, false, true, IB_LOCK_X);

    EXPECT_EQ(cur.moveto(205, IB_CUR_GE, IB_EXACT_MATCH).m_err, DB_RECORD_NOT_FOUND);
  }

  EXPECT_TRUE(locks_of(trx, m_tab->clust()).empty());
  commit(trx);
}

/** A locking read through SEC_A with cluster access locks the secondary
record and the clustered record. */
TEST_F(RowSelIdxTest, LockSecAndClust) {
  load(50);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx, true, true, IB_LOCK_X);
    auto at = cur.moveto(400, IB_CUR_GE, IB_EXACT_MATCH);

    ASSERT_EQ(at.m_err, DB_SUCCESS);
    EXPECT_EQ(at.m_row->m_k, 200U);
  }

  bool sec_locked{};

  for (const auto &l : locks_of(trx, m_tab->sec())) {
    sec_locked |= l.m_key == 400 && (l.m_mode & LOCK_MODE_MASK) == LOCK_X;
  }

  EXPECT_TRUE(sec_locked);
  EXPECT_EQ(keys_locked(trx, m_tab->clust(), LOCK_X | LOCK_REC_NOT_GAP), (std::vector<int64_t>{200}));
  commit(trx);
}

/** A locking scan to the end of the index locks the supremum (the gap
after the last record) in REPEATABLE READ. */
TEST_F(RowSelIdxTest, LockScanToEnd) {
  load(5);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx, false, true, IB_LOCK_S);
    auto at = cur.first();

    while (at.m_err == DB_SUCCESS) {
      at = cur.next();
    }

    EXPECT_EQ(at.m_err, DB_END_OF_INDEX);
  }

  EXPECT_EQ(keys_locked(trx, m_tab->clust(), LOCK_S | LOCK_ORDINARY), (std::vector<int64_t>{-1, 10, 20, 30, 40, 50}));
  commit(trx);
}

/** A consistent read takes no record locks. */
TEST_F(RowSelIdxTest, ConsistentNoLocks) {
  load(20);

  auto trx = begin();

  EXPECT_EQ(m_tab->scan(trx).size(), 20UL);
  EXPECT_EQ(m_tab->scan_sec(trx).size(), 20UL);
  EXPECT_TRUE(locks_of(trx, m_tab->clust()).empty());
  EXPECT_TRUE(locks_of(trx, m_tab->sec()).empty());
  commit(trx);
}

/** Many delete marked records in a row, on several pages, are skipped by
scans in both directions and by positioning. */
TEST_F(RowSelIdxTest, SkipDeleteMarkedRuns) {
  auto rows = load(400);

  auto trx = begin();

  for (uint32_t k = 500; k <= 3500; k += 10) {
    ASSERT_EQ(m_tab->remove(trx, k), DB_SUCCESS);
    rows.erase(k);
  }

  commit(trx);

  trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx);
    auto at = cur.moveto(500, IB_CUR_GE);

    ASSERT_EQ(at.m_err, DB_SUCCESS);
    EXPECT_EQ(at.m_row->m_k, 3510U);

    at = cur.moveto(3500, IB_CUR_LE);
    ASSERT_EQ(at.m_err, DB_SUCCESS);
    EXPECT_EQ(at.m_row->m_k, 490U);

    EXPECT_EQ(cur.moveto(1000, IB_CUR_GE, IB_EXACT_MATCH).m_err, DB_RECORD_NOT_FOUND);
  }

  commit(trx);
  m_tab->expect_rows(rows);
}

/** Positioning on an empty index. */
TEST_F(RowSelIdxTest, EmptyIndex) {
  auto trx = begin();

  for (bool sec : {false, true}) {
    Cur cur(*m_sys, *m_tab, trx, sec, true);

    EXPECT_EQ(cur.first().m_err, DB_END_OF_INDEX);
    EXPECT_EQ(cur.last().m_err, DB_END_OF_INDEX);

    for (auto mode : {IB_CUR_G, IB_CUR_GE, IB_CUR_L, IB_CUR_LE}) {
      EXPECT_EQ(cur.moveto(5, mode).m_err, DB_END_OF_INDEX) << mode;
    }

    EXPECT_EQ(cur.moveto(5, IB_CUR_GE, IB_EXACT_MATCH).m_err, DB_RECORD_NOT_FOUND);
  }

  commit(trx);
}

/** RowSel::mvcc_fetch() directly: MOVETO, then NEXT and PREV with the
search tuple emptied, as the api does. */
TEST_F(RowSelIdxTest, MvccFetchDirect) {
  load(100);

  auto trx = begin();
  Cur cur(*m_sys, *m_tab, trx);
  auto prebuilt = cur.prebuilt();

  ASSERT_EQ(cur.moveto(500, IB_CUR_GE).m_err, DB_SUCCESS);

  dtuple_set_n_fields(prebuilt->m_search_tuple, 0);
  prebuilt->m_row_cache.cache_next();

  auto err = m_sys->m_row_sel->mvcc_fetch(IB_RECOVERY_DEFAULT, IB_CUR_G, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_NEXT);

  ASSERT_EQ(err, DB_SUCCESS);
  EXPECT_EQ(cur.row()->m_k, 510U);

  prebuilt->m_row_cache.cache_next();
  err = m_sys->m_row_sel->mvcc_fetch(IB_RECOVERY_DEFAULT, IB_CUR_L, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_PREV);
  ASSERT_EQ(err, DB_SUCCESS);
  EXPECT_EQ(cur.row()->m_k, 500U);

  /* Positioning at the start: no search fields, mode G. */
  prebuilt->m_row_cache.cache_next();
  err = m_sys->m_row_sel->mvcc_fetch(IB_RECOVERY_DEFAULT, IB_CUR_G, prebuilt, ROW_SEL_DEFAULT, ROW_SEL_MOVETO);
  ASSERT_EQ(err, DB_SUCCESS);
  EXPECT_EQ(cur.row()->m_k, 10U);

  cur.close();
  commit(trx);
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

using RowSelPerf = RowSelIdxTest;

/** Point lookups on the clustered index and through SEC_A with cluster
access, a range scan, and lookups from 1 to 16 threads. */
TEST_F(RowSelPerf, Lookups) {
  constexpr uint32_t N = 20000;

  load(N);

  auto trx = begin();

  {
    Cur cur(*m_sys, *m_tab, trx);

    auto r = ut::bench("clust_point_lookup", 200000, [&](uint64_t i) {
      ut::keep(cur.moveto(10 * (1 + uint32_t(i * 7919 % N)), IB_CUR_GE, IB_EXACT_MATCH).m_err);
    });

    EXPECT_LT(r.ns_per_op(), 500000.0);

    uint64_t n{};
    const auto start = std::chrono::steady_clock::now();

    for (auto at = cur.first(); at.m_err == DB_SUCCESS; at = cur.next()) {
      ++n;
    }

    ut::BenchResult rs{"clust_range_scan_rows", 1, n, std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count()};

    ut::report(rs);
    EXPECT_EQ(n, N);
    EXPECT_LT(rs.ns_per_op(), 500000.0);
  }

  {
    Cur cur(*m_sys, *m_tab, trx, true, true);

    auto r = ut::bench("sec_cluster_point_lookup", 100000, [&](uint64_t i) {
      ut::keep(cur.moveto(20 * (1 + uint32_t(i * 7919 % N)), IB_CUR_GE, IB_EXACT_MATCH).m_err);
    });

    EXPECT_LT(r.ns_per_op(), 1e6);
  }

  commit(trx);

  for (int threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;
    std::vector<std::unique_ptr<Cur>> curs;

    for (int t = 0; t < threads; ++t) {
      trxs.push_back(begin());
      curs.push_back(std::make_unique<Cur>(*m_sys, *m_tab, trxs.back()));
    }

    auto r = ut::bench_mt("clust_point_lookup_mt", threads, 20000, [&](int t, uint64_t i) {
      ut::keep(curs[t]->moveto(10 * (1 + uint32_t((i * 7919 + t * 104729) % N)), IB_CUR_GE, IB_EXACT_MATCH).m_err);
    });

    EXPECT_LT(r.ns_per_op(), 1e6);

    curs.clear();

    for (auto t : trxs) {
      commit(t);
    }
  }
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
class RowSelMtPerf : public RowSelIdxTest {
 protected:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    s_config = perf_config(s_dir->m_path);
    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
  }
};

/** Point selects of whole rows of 50 bytes, 1 KB and 20 KB (b external:
read from its pages each time) through the clustered index, in a random
order. */
TEST_F(RowSelMtPerf, PointSelectSizes) {
  struct Size {
    const char *m_name;
    size_t m_b;
    uint32_t m_n;
    uint64_t m_ops;
  };

  for (const auto &sz : {Size{"point_select_50B", 42, 5000, 50000}, Size{"point_select_1KB", 1016, 2000, 50000},
                         Size{"point_select_20KB", 20000, 200, 5000}}) {
    auto tab = this->tab(std::format("_{}", sz.m_b).c_str());
    ut::Rows rows;

    for (uint32_t k{}; k < sz.m_n; ++k) {
      rows[k] = ut::Row{k, k, ut::blob(k, sz.m_b)};
    }

    tab->load(rows);

    auto trx = begin();
    Cur cur(*m_sys, *tab, trx);

    auto r = ut::bench(sz.m_name, sz.m_ops, [&](uint64_t i) {
      const auto at = cur.moveto(uint32_t(i * 7919 % sz.m_n), IB_CUR_GE, IB_EXACT_MATCH);

      EXPECT_EQ(at.m_err, DB_SUCCESS);
      ut::keep(at);
    });

    EXPECT_LT(r.ns_per_op(), 5e6) << sz.m_name;
    cur.close();
    commit(trx);
  }
}

/** 1 to 16 threads each scanning a disjoint range of the clustered index
(consistent reads): the rows of all the ranges are read once. */
TEST_F(RowSelMtPerf, RangeScanThreads) {
  constexpr uint32_t N = 16000;

  load(N);

  for (int threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;
    std::vector<std::unique_ptr<Cur>> curs;
    std::vector<uint64_t> seen(threads);
    const uint64_t per = N / threads;
    Watchdog watchdog(std::chrono::seconds(300));

    for (int t = 0; t < threads; ++t) {
      trxs.push_back(begin());
      curs.push_back(std::make_unique<Cur>(*m_sys, *m_tab, trxs.back()));
    }

    auto r = ut::bench_mt("range_scan_rows_mt", threads, per, [&](int t, uint64_t i) {
      const auto at = i == 0 ? curs[t]->moveto(uint32_t(10 * (1 + t * per)), IB_CUR_GE) : curs[t]->next();

      if (at.m_err == DB_SUCCESS) {
        ++seen[t];
      }
    });

    EXPECT_LT(r.ns_per_op(), 1e6);

    for (int t = 0; t < threads; ++t) {
      EXPECT_EQ(seen[t], per) << t;
    }

    curs.clear();

    for (auto t : trxs) {
      commit(t);
    }
  }
}

/** 1 to 16 threads of point lookups through SEC_A with access to the
clustered record. */
TEST_F(RowSelMtPerf, SecLookupThreads) {
  constexpr uint32_t N = 10000;

  load(N);

  for (int threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;
    std::vector<std::unique_ptr<Cur>> curs;
    Watchdog watchdog(std::chrono::seconds(300));

    for (int t = 0; t < threads; ++t) {
      trxs.push_back(begin());
      curs.push_back(std::make_unique<Cur>(*m_sys, *m_tab, trxs.back(), true, true));
    }

    auto r = ut::bench_mt("sec_cluster_lookup_mt", threads, 4000, [&](int t, uint64_t i) {
      const auto a = 20 * (1 + uint32_t((i * 7919 + t * 104729) % N));
      const auto at = curs[t]->moveto(a, IB_CUR_GE, IB_EXACT_MATCH);

      EXPECT_EQ(at.m_err, DB_SUCCESS);
      EXPECT_TRUE(at.m_row.has_value() && at.m_row->m_a == a);
    });

    EXPECT_LT(r.ns_per_op(), 1e6);

    curs.clear();

    for (auto t : trxs) {
      commit(t);
    }
  }
}

/** 4 readers scanning the table (consistent reads through the clustered
index and through SEC_A) while 4 writers update disjoint rows, changing
the secondary key and the size of b: each scan sees every row exactly
once; nothing hangs. */
TEST_F(RowSelMtPerf, ReadersVsWriters) {
  constexpr uint32_t N = 4000;
  constexpr int R = 4;
  constexpr int W = 4;
  constexpr int SCANS = 4;

  const auto rows = load(N);

  std::atomic<uint64_t> read{};
  std::atomic<uint64_t> written{};
  std::atomic<bool> stop{};
  std::vector<std::thread> threads;
  Watchdog watchdog(std::chrono::seconds(300));
  const auto start = std::chrono::steady_clock::now();

  for (int w = 0; w < W; ++w) {
    threads.emplace_back([&, w] {
      uint32_t round{};

      while (!stop.load()) {
        auto trx = begin();

        for (uint32_t i = w; i < N; i += W * 8) {
          const auto k = (i + 1) * 10;

          EXPECT_EQ(m_tab->update(trx, k, ut::Row{k, k * 2 + (round % 2) * 7, ut::blob(k, 50 + (round % 3) * 400)}), DB_SUCCESS);
          ++written;
        }

        commit(trx);
        ++round;
      }
    });
  }

  std::vector<std::thread> readers;

  for (int r = 0; r < R; ++r) {
    readers.emplace_back([&, r] {
      for (int s = 0; s < SCANS; ++s) {
        auto trx = begin();
        const auto seen = r % 2 == 0 ? m_tab->scan(trx).size() : m_tab->scan_sec(trx).size();

        EXPECT_EQ(seen, size_t(N)) << "reader " << r << " scan " << s;
        read += seen;
        commit(trx);
      }
    });
  }

  for (auto &t : readers) {
    t.join();
  }

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;

  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  ut::report(ut::BenchResult{"rw_reader_rows", R, read.load(), secs.count()});
  ut::report(ut::BenchResult{"rw_writer_updates", W, written.load(), secs.count()});

  EXPECT_EQ(read.load(), uint64_t(N) * R * SCANS);
  EXPECT_EQ(rows.size(), N);
}

}  // namespace
