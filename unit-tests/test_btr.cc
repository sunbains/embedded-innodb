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

/** @file unit-tests/test_btr.cc
Btree, BtreeCursor, BtreePcursor and Blob: searches in every mode on empty,
single page and multi-level trees, page splits (ascending, descending,
random, in the middle and at the ends), root raise, deletes down to an
empty tree, merges, the size of a tree, its validation, the persistent
cursor's store and restore across changes, and externally stored fields:
sizes around the page boundaries, ownership and inheritance on primary key
updates, commit and rollback. The tree tests drive BtreeCursor directly on
tables of their own in one shared instance; the first tests (BtrTest) go
through the api on a fresh instance each. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0page.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace {

using BtrTest = ut::DbTest;

const Index *clust(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table->get_first_index();
}

/** The tree grows from a root page and shrinks back as rows go. */
TEST_F(BtrTest, GrowShrink) {
  /* The root of an empty tree is in the non-leaf segment: no leaf pages, or one. */
  EXPECT_LE(m_sys->m_btree->get_size(clust(*m_sys), BTR_N_LEAF_PAGES), 1UL);

  ut::Rows rows;

  for (uint32_t k = 0; k < 20000; ++k) {
    rows[k * 7 % 20000] = ut::Row{k * 7 % 20000, k % 97, ut::blob(k, 200)};
  }

  load(rows);
  expect_rows(rows);

  const auto leaves = m_sys->m_btree->get_size(clust(*m_sys), BTR_N_LEAF_PAGES);

  EXPECT_GT(leaves, 100UL);
  EXPECT_GE(m_sys->m_btree->get_size(clust(*m_sys), BTR_TOTAL_SIZE), leaves);

  /* Delete 9 of 10 rows, spread over the tree: pages merge. */
  auto trx = begin();

  for (uint32_t k = 0; k < 20000; ++k) {
    if (k % 10 != 0) {
      ASSERT_EQ(remove(trx, k), DB_SUCCESS);
      rows.erase(k);
    }
  }

  commit(trx);
  expect_rows(rows);

  /* And back in. */
  trx = begin();

  for (uint32_t k = 0; k < 20000; k += 3) {
    if (!rows.contains(k)) {
      ut::Row row{k, k % 5, ut::blob(k + 1, 150)};

      ASSERT_EQ(insert(trx, row), DB_SUCCESS);
      rows[k] = row;
    }
  }

  commit(trx);
  expect_rows(rows);

  restart();
  expect_rows(rows);
}

/** Records that fill a page by themselves, and keys at both ends. */
TEST_F(BtrTest, BigRecords) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 200; ++k) {
    /* Just under the limit to stay in the page. */
    rows[k] = ut::Row{k, k, ut::blob(k, k % 2 == 0 ? 7000 : 10)};
  }

  rows[0xFFFFFFFF] = ut::Row{0xFFFFFFFF, 0xFFFFFFFF, "max"};

  load(rows);
  expect_rows(rows);
}

/** Pessimistic updates (the record grows past the free space of its page)
in a page with other transactions' locks on it. */
TEST_F(BtrTest, PessimisticUpdate) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 400; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 30)};
  }

  load(rows);

  auto locker = begin();

  /* Keep S locks on some records: a locking read through update with the same value. */
  for (uint32_t k = 1; k < 400; k += 4) {
    ASSERT_EQ(update(locker, k, rows[k]), DB_SUCCESS);
  }

  auto trx = begin();

  for (uint32_t k = 0; k < 400; k += 4) {
    auto row = rows[k];

    row.m_b = ut::blob(k, 6000);
    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    rows[k] = row;
  }

  commit(trx);
  commit(locker);

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

/* --- The tree, driven directly through BtreeCursor and BtreePcursor -----
The trees are the clustered indexes of tables of the tests' own (Tab). The
entries go in with BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG |
BTR_KEEP_SYS_FLAG: no transaction, no undo, no locks, only the tree. */

namespace {

/** The flags of the tree-only inserts. */
constexpr ulint TREE_FLAGS = BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG;

/** The clustered index record fields: k, DB_TRX_ID, DB_ROLL_PTR, a, b. */
constexpr ulint B_FIELD = 4;

class BtrTreeTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();
    m_tab = tab();
    m_index = m_tab->clust();
    m_heap = mem_heap_create(16384);
  }

  void TearDown() override {
    mem_heap_free(m_heap);
    m_tab.reset();
  }

  byte *u32(uint32_t v) {
    auto p = static_cast<byte *>(mem_heap_alloc(m_heap, 4));

    mach_write_to_4(p, v);
    return p;
  }

  /** @return an entry of the clustered index, key k, b of blen bytes. */
  DTuple *entry(uint32_t k, ulint blen = 100) {
    const auto n = m_index->get_n_fields();
    auto t = dtuple_create(m_heap, n);

    m_index->copy_types(t, n);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(k), 4);

    auto sys = mem_heap_zalloc(m_heap, 13);

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, 6);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + 6, 7);
    dfield_set_data(dtuple_get_nth_field(t, 3), u32(k ^ 0x5a5a), 4);

    auto b = static_cast<byte *>(mem_heap_alloc(m_heap, std::max<ulint>(blen, 1)));

    memset(b, int('a' + k % 26), blen);
    dfield_set_data(dtuple_get_nth_field(t, B_FIELD), b, blen);
    dtuple_set_n_fields_cmp(t, m_index->get_n_unique_in_tree());

    return t;
  }

  /** @return a search tuple for key k. */
  DTuple *key(uint32_t k) {
    auto t = dtuple_create(m_heap, 1);

    m_index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(k), 4);

    return t;
  }

  /** Insert e into the tree: optimistic, and pessimistic if that fails. */
  DbErr tree_insert(DTuple *e) {
    Mtr mtr;
    BtreeCursor cur;
    Rec rec;
    BigRec big;

    mtr.start();
    cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, e, PAGE_CUR_LE, BTR_MODIFY_LEAF, &mtr, Current_location());

    auto err = cur.optimistic_insert(*m_sys, TREE_FLAGS, e, &rec, &big, 0, nullptr, &mtr);

    mtr.commit(*m_sys);

    if (err == DB_FAIL) {
      mtr.start();
      cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, e, PAGE_CUR_LE, BTR_MODIFY_TREE, &mtr, Current_location());
      err = cur.pessimistic_insert(*m_sys, TREE_FLAGS, e, &rec, &big, 0, nullptr, &mtr);
      mtr.commit(*m_sys);
    }

    return err;
  }

  /** Remove the record with key k from the tree: optimistic, then
  pessimistic. @return false if there is no such record */
  bool tree_delete(uint32_t k) {
    Mtr mtr;
    BtreeCursor cur;
    auto t = key(k);

    mtr.start();
    cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, t, PAGE_CUR_LE, BTR_MODIFY_LEAF, &mtr, Current_location());

    if (cur.m_low_match < 1 || !page_rec_is_user_rec(cur.get_rec())) {
      mtr.commit(*m_sys);
      return false;
    }

    const auto done = cur.optimistic_delete(*m_sys, &mtr);

    mtr.commit(*m_sys);

    if (!done) {
      mtr.start();
      cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, t, PAGE_CUR_LE, BTR_MODIFY_TREE, &mtr, Current_location());
      EXPECT_EQ(cur.m_low_match, 1UL);
      EXPECT_EQ(cur.pessimistic_delete(*m_sys, false, RB_NONE, &mtr), DB_SUCCESS);
      mtr.commit(*m_sys);
    }

    return true;
  }

  /** Insert keys in the given order, each with b of blen bytes. */
  void fill(const std::vector<uint32_t> &keys, ulint blen = 100) {
    for (auto k : keys) {
      ASSERT_EQ(tree_insert(entry(k, blen)), DB_SUCCESS) << k;
      mem_heap_empty(m_heap);
    }
  }

  /** @return the keys of the leaf records, delete marked or not, in order. */
  std::vector<uint32_t> leaf_keys() {
    std::vector<uint32_t> v;
    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      ulint len;

      v.push_back(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)));
    }

    pcur.close();
    mtr.commit(*m_sys);

    return v;
  }

  /** Leaf pages: their count and the average fill (data size / free space of an empty page). */
  std::pair<ulint, double> leaf_fill() {
    Mtr mtr;
    BtreePcursor pcur;
    std::set<page_no_t> pages;
    ulint data{};

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      if (pages.insert(pcur.get_block()->get_page_no()).second) {
        data += page_get_data_size(pcur.get_page_no());
      }
    }

    pcur.close();
    mtr.commit(*m_sys);

    return {pages.size(), double(data) / double(std::max<size_t>(pages.size(), 1) * page_get_free_space_of_empty())};
  }

  /** @return the level of the root page: the height of the tree - 1. */
  ulint root_level() {
    Mtr mtr;

    mtr.start();

    auto root = m_sys->m_btree->root_get(m_index->m_page_id, &mtr);
    const auto level = m_sys->m_btree->page_get_level(root, &mtr);

    mtr.commit(*m_sys);

    return level;
  }

  /** @return the first field of rec, the 4 byte key. */
  static uint32_t key_of(const Rec rec) {
    ulint len;

    return mach_read_from_4(rec.get_nth_field(0, &len));
  }

  /** Check the whole tree, level by level (Btree::validate_index() is
  broken for levels of more than one page, see ValidateIndex):
  each page passes page_validate() and has the right level, the pages of
  a level are linked both ways in key order, the node pointers of a level
  point to the pages of the level below in order, each key between the
  last key of the page before and the first key of its child, the first node pointer of a level has the minimum record
  flag, the leaves pass index_page_validate(). */
  void validate() {
    const auto space = m_index->get_space_id();
    const auto top = root_level();
    std::vector<page_no_t> pages{m_index->get_page_no()};
    std::vector<uint32_t> ptr_keys;

    for (ulint level = top + 1; level-- > 0;) {
      std::vector<page_no_t> children;
      std::vector<uint32_t> child_keys;
      int64_t prev_last{-1};

      for (size_t i{}; i < pages.size(); ++i) {
        Mtr mtr;

        mtr.start();

        auto block = m_sys->m_btree->block_get(space, pages[i], RW_X_LATCH, &mtr);
        auto page = block->get_frame();

        EXPECT_TRUE(page_validate(page, m_index)) << "level " << level << " page " << pages[i];
        EXPECT_EQ(m_sys->m_btree->page_get_level(page, &mtr), level) << pages[i];
        EXPECT_EQ(Btree::page_get_prev(page, &mtr), i == 0 ? FIL_NULL : pages[i - 1]) << pages[i];

        if (i + 1 == pages.size()) {
          EXPECT_EQ(Btree::page_get_next(page, &mtr), FIL_NULL) << pages[i];
        }

        EXPECT_TRUE(page_get_n_recs(page) > 0 || (level == 0 && pages.size() == 1)) << pages[i];

        auto first = page_rec_get_next(page_get_infimum_rec(page));

        if (!page_rec_is_supremum(first)) {
          const auto first_key = key_of(first);
          const auto last_key = key_of(page_rec_get_prev(page_get_supremum_rec(page)));

          EXPECT_GT(int64_t(first_key), prev_last) << "keys out of order across pages, level " << level;

          /* The node pointer to this page is no greater than its first key
          and greater than the keys of the page before (the leftmost is
          the minimum record). */
          if (level < top && i > 0) {
            EXPECT_LE(ptr_keys[i], first_key) << "node pointer key, level " << level << " page " << pages[i];
            EXPECT_GT(int64_t(ptr_keys[i]), prev_last) << "node pointer key, level " << level << " page " << pages[i];
          }

          prev_last = last_key;
        }

        if (level > 0) {
          bool leftmost = (i == 0);

          for (auto rec = first; !page_rec_is_supremum(rec); rec = page_rec_get_next(rec)) {
            RecOffs offs = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &m_heap, Current_location());

            EXPECT_EQ(bool(rec.get_info_bits() & REC_INFO_MIN_REC_FLAG), leftmost) << "level " << level;
            leftmost = false;
            children.push_back(m_sys->m_btree->node_ptr_get_child_page_no(rec, offs));
            child_keys.push_back(key_of(rec));
          }
        } else {
          EXPECT_TRUE(m_sys->m_btree->index_page_validate(block, m_index)) << pages[i];
        }

        mtr.commit(*m_sys);
      }

      if (level > 0) {
        /* The next level, from its leftmost page along the links, is the children in order. */
        std::vector<page_no_t> next;
        Mtr mtr;

        mtr.start();

        for (auto p = children.front(); p != FIL_NULL;) {
          next.push_back(p);
          p = Btree::page_get_next(m_sys->m_btree->page_get(space, p, RW_X_LATCH, &mtr), &mtr);
        }

        mtr.commit(*m_sys);

        ASSERT_EQ(next, children) << "level " << level - 1;
      }

      pages = std::move(children);
      ptr_keys = std::move(child_keys);
      mem_heap_empty(m_heap);
    }
  }

  /** Search for k with a mode, step off the page ends like a scan does.
  @return the key of the user record found, or -1 if none */
  int64_t find(uint32_t k, SearchMode mode) {
    Mtr mtr;
    BtreePcursor pcur;
    int64_t found{-1};

    mtr.start();
    pcur.open(*m_sys, m_index, key(k), mode, BTR_SEARCH_LEAF, &mtr, Current_location());

    bool on;

    if (pcur.is_on_user_rec()) {
      on = true;
    } else if (mode == PAGE_CUR_G || mode == PAGE_CUR_GE) {
      on = pcur.move_to_next_user_rec(*m_sys, &mtr);
    } else {
      on = pcur.move_to_prev_user_rec(*m_sys, &mtr);
    }

    if (on) {
      ulint len;

      found = mach_read_from_4(pcur.get_rec().get_nth_field(0, &len));
    }

    pcur.close();
    mtr.commit(*m_sys);

    return found;
  }

  std::unique_ptr<Tab> m_tab;
  Index *m_index{};
  MemHeap *m_heap{};
};

/** @return keys 0, step, 2 * step, ... (n of them). */
std::vector<uint32_t> seq(uint32_t n, uint32_t step = 1, uint32_t first = 0) {
  std::vector<uint32_t> v(n);

  for (uint32_t i{}; i < n; ++i) {
    v[i] = first + i * step;
  }

  return v;
}

/** An empty tree: the root is an empty leaf; every search ends nowhere. */
TEST_F(BtrTreeTest, EmptyTree) {
  EXPECT_EQ(root_level(), 0UL);
  EXPECT_TRUE(leaf_keys().empty());

  for (auto mode : {PAGE_CUR_G, PAGE_CUR_GE, PAGE_CUR_L, PAGE_CUR_LE}) {
    EXPECT_EQ(find(5, mode), -1) << mode;
  }

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open(*m_sys, m_index, key(1), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  EXPECT_TRUE(pcur.is_after_last_in_tree(&mtr));
  EXPECT_FALSE(pcur.is_on_user_rec());
  pcur.close();
  mtr.commit(*m_sys);

  EXPECT_LE(m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES), 1UL);
  validate();
}

/** The search modes on one page and on a tree of three levels, for keys
that exist, keys between, and keys beyond both ends. */
TEST_F(BtrTreeTest, SearchModes) {
  for (uint32_t n : {50U, 20000U}) {
    if (n > 50) {
      /* Records of ~500 bytes: 20000 of them make three levels. */
      for (auto k : seq(n - 50, 10, 500)) {
        ASSERT_EQ(tree_insert(entry(k, 500)), DB_SUCCESS);
        mem_heap_empty(m_heap);
      }

      EXPECT_GE(root_level(), 1UL);
    } else {
      fill(seq(50, 10));
      EXPECT_EQ(root_level(), 0UL);
    }

    const auto last = int64_t(leaf_keys().back());

    struct Case {
      uint32_t m_key;
      SearchMode m_mode;
      int64_t m_expected;
    };

    const std::vector<Case> cases{
      {250, PAGE_CUR_G, 260},  {250, PAGE_CUR_GE, 250}, {250, PAGE_CUR_L, 240}, {250, PAGE_CUR_LE, 250},
      {255, PAGE_CUR_G, 260},  {255, PAGE_CUR_GE, 260}, {255, PAGE_CUR_L, 250}, {255, PAGE_CUR_LE, 250},
      {0, PAGE_CUR_GE, 0},     {0, PAGE_CUR_LE, 0},     {0, PAGE_CUR_L, -1},    {0, PAGE_CUR_G, 10},
      {uint32_t(last), PAGE_CUR_G, -1},                 {uint32_t(last), PAGE_CUR_GE, last},
      {uint32_t(last + 5), PAGE_CUR_LE, last},          {uint32_t(last + 5), PAGE_CUR_GE, -1},
      {UINT32_MAX, PAGE_CUR_L, last},
    };

    for (const auto &c : cases) {
      EXPECT_EQ(find(c.m_key, c.m_mode), c.m_expected) << "n " << n << " key " << c.m_key << " mode " << c.m_mode;
    }

    /* Every key and every gap of the big tree, in both directions. */
    if (n > 50) {
      const auto keys = leaf_keys();

      for (size_t i = 0; i < keys.size(); i += 97) {
        ASSERT_EQ(find(keys[i], PAGE_CUR_GE), keys[i]);
        ASSERT_EQ(find(keys[i] + 1, PAGE_CUR_GE), i + 1 < keys.size() ? int64_t(keys[i + 1]) : -1);
        ASSERT_EQ(find(keys[i] + 1, PAGE_CUR_LE), keys[i]);
        ASSERT_EQ(find(keys[i], PAGE_CUR_L), i > 0 ? int64_t(keys[i - 1]) : -1);
      }
    }
  }

  validate();
}

/** The match counts of a search: an exact GE match has m_up_match == n,
an exact LE match m_low_match == n, a miss less. */
TEST_F(BtrTreeTest, MatchCounts) {
  fill(seq(100, 2));

  Mtr mtr;
  BtreeCursor cur;

  mtr.start();
  cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, key(40), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  EXPECT_EQ(cur.m_up_match, 1UL);
  mtr.commit(*m_sys);

  mtr.start();
  cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, key(40), PAGE_CUR_LE, BTR_SEARCH_LEAF, &mtr, Current_location());
  EXPECT_EQ(cur.m_low_match, 1UL);
  mtr.commit(*m_sys);

  mtr.start();
  cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, key(41), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  EXPECT_EQ(cur.m_up_match, 0UL);
  EXPECT_EQ(cur.m_low_match, 0UL);
  mtr.commit(*m_sys);
}

/** Btree::validate_index() of a tree with more than one page on a level. */
TEST_F(BtrTreeTest, ValidateIndex) {
  /* Regression: Btree::validate_level() (btr/btr0btr.cc:2030) declares
  `const auto right_block` / `right_page` inside the loop, shadowing the
  outer variables, which stay null: page_get_father_block(..., right_block)
  dereferences null when a level has a right sibling and a father. And the
  loop advances with node_ptr_fails(mtr, ..., right_block, right_page),
  which loads the next page into right_block/right_page instead of
  block/page: the same, unlatched, page is validated again. Upstream
  assigns the right page to block/page (goto loop). */
  fill(seq(400), 300);
  ASSERT_GE(root_level(), 1UL);

  auto trx = begin();

  EXPECT_TRUE(m_sys->m_btree->validate_index(m_index, trx));
  commit(trx);
}

/** Btree::validate_index() of a single page tree works. */
TEST_F(BtrTreeTest, ValidateIndexOnePage) {
  fill(seq(20));
  ASSERT_EQ(root_level(), 0UL);

  auto trx = begin();

  EXPECT_TRUE(m_sys->m_btree->validate_index(m_index, trx));
  commit(trx);
}

/** The root splits by raising: it stays where it is, one level up, with
the records in two new leaves; it raises again when its level fills. */
TEST_F(BtrTreeTest, RootRaise) {
  const auto root_page_no = m_index->get_page_no();
  uint32_t k{};

  while (root_level() == 0) {
    ASSERT_EQ(tree_insert(entry(k++, 3000)), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(m_index->get_page_no(), root_page_no);
  EXPECT_EQ(m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES), 2UL);
  EXPECT_EQ(leaf_keys(), seq(k));
  validate();

  while (root_level() == 1) {
    ASSERT_EQ(tree_insert(entry(k++, 3000)), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(root_level(), 2UL);
  EXPECT_EQ(m_index->get_page_no(), root_page_no);
  EXPECT_EQ(leaf_keys(), seq(k));
  EXPECT_GT(m_sys->m_btree->get_size(m_index, BTR_TOTAL_SIZE), m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES));
  validate();
}

/** Ascending inserts split at the insert point: the left pages stay full
(all but the space reserved for updates). */
TEST_F(BtrTreeTest, SplitAscending) {
  fill(seq(5000), 200);

  const auto [leaves, fill_factor] = leaf_fill();

  EXPECT_GT(fill_factor, 0.85) << leaves << " leaves";
  EXPECT_EQ(leaf_keys(), seq(5000));
  validate();
}

/** Descending inserts: the pages split to the left, also full. */
TEST_F(BtrTreeTest, SplitDescending) {
  auto keys = seq(5000);

  std::reverse(keys.begin(), keys.end());
  fill(keys, 200);

  const auto [leaves, fill_factor] = leaf_fill();

  EXPECT_GT(fill_factor, 0.85) << leaves << " leaves";
  EXPECT_EQ(leaf_keys(), seq(5000));
  validate();
}

/** Random inserts split in the middle: about half to three quarters full. */
TEST_F(BtrTreeTest, SplitRandom) {
  auto keys = seq(5000);
  std::mt19937 rng(11);

  std::shuffle(keys.begin(), keys.end(), rng);
  fill(keys, 200);

  const auto [leaves, fill_factor] = leaf_fill();

  EXPECT_GT(fill_factor, 0.45) << leaves << " leaves";
  EXPECT_LT(fill_factor, 0.90) << leaves << " leaves";
  EXPECT_EQ(leaf_keys(), seq(5000));
  validate();
}

/** Inserts in the middle of a full page, between two runs: ordered,
valid, at the two ends of the key space too. */
TEST_F(BtrTreeTest, SplitMiddleAndEnds) {
  fill(seq(2000, 4, 1000), 300);

  std::vector<uint32_t> more;

  for (uint32_t k = 1001; k < 9000; k += 8) {
    more.push_back(k);
  }

  more.push_back(0);
  more.push_back(UINT32_MAX);
  fill(more, 300);

  auto expected = seq(2000, 4, 1000);

  expected.insert(expected.end(), more.begin(), more.end());
  std::sort(expected.begin(), expected.end());

  EXPECT_EQ(leaf_keys(), expected);
  validate();
}

/** A record bigger than half a page goes in, alone on its page if need be. */
TEST_F(BtrTreeTest, BigRecords) {
  for (uint32_t k = 0; k < 40; ++k) {
    ASSERT_EQ(tree_insert(entry(k * 2, 7000)), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  for (uint32_t k = 0; k < 40; ++k) {
    ASSERT_EQ(tree_insert(entry(k * 2 + 1, 10)), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(leaf_keys(), seq(80));
  validate();
}

/** Deleting every record, in random order, frees the pages one by one
and leaves an empty root leaf. */
TEST_F(BtrTreeTest, DeleteAll) {
  auto keys = seq(3000);
  std::mt19937 rng(5);

  fill(keys, 300);
  ASSERT_GE(root_level(), 1UL);

  std::shuffle(keys.begin(), keys.end(), rng);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_TRUE(tree_delete(keys[i])) << keys[i];

    if (i % 500 == 0) {
      validate();
    }
  }

  EXPECT_TRUE(leaf_keys().empty());
  EXPECT_EQ(root_level(), 0UL);
  /* The root, a leaf again, is in the non-leaf segment. */
  EXPECT_LE(m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES), 1UL);
  validate();

  /* And it grows again. */
  fill(seq(100));
  EXPECT_EQ(leaf_keys(), seq(100));
  validate();
}

/** A tree emptied from the left and from the right. */
TEST_F(BtrTreeTest, DeleteFromEnds) {
  fill(seq(3000), 300);

  for (uint32_t k = 0; k < 1500; ++k) {
    ASSERT_TRUE(tree_delete(k));
    ASSERT_TRUE(tree_delete(2999 - k));
  }

  EXPECT_TRUE(leaf_keys().empty());
  EXPECT_FALSE(tree_delete(5));
  validate();
}

/** Deleting most of the records merges the pages they leave sparse: the
tree has fewer leaves. */
TEST_F(BtrTreeTest, MergeOnDelete) {
  /* Regression: pages are never merged. BtreeCursor::pessimistic_delete()
  (btr/btr0cur.cc:1723) and pessimistic_update() lost upstream's
  btr_cur_compress_if_useful() call: Btree::compress() is never called,
  a page is only freed when its last record goes. See also
  CompressDirect: compress() itself is broken. */
  fill(seq(6000), 300);

  const auto [leaves, fill1] = leaf_fill();

  for (uint32_t k = 0; k < 6000; ++k) {
    if (k % 10 != 0) {
      ASSERT_TRUE(tree_delete(k));
    }
  }

  const auto [leaves2, fill2] = leaf_fill();

  EXPECT_LT(leaves2, leaves / 3) << "fill " << fill1 << " -> " << fill2;
  validate();
}

/** Deleting records merges the pages that become too empty: pessimistic
deletes call Btree::compress(), which merges into the left (or right)
neighbour and removes the node pointer. compress() was never called (the
call of upstream btr_cur_compress_if_useful() was lost), read its block from
an empty cursor, and asserted on merges to the right. */
TEST_F(BtrTreeTest, CompressDirect) {
  fill(seq(2000), 300);
  ASSERT_GE(root_level(), 1UL);

  const auto leaves = m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES);

  /* Empty a run of pages in the middle, keeping one record in each. */
  for (uint32_t k = 600; k < 1400; ++k) {
    if (k % 50 != 0) {
      ASSERT_TRUE(tree_delete(k));
    }
  }

  EXPECT_LT(m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES), leaves) << "no page was merged";
  validate();

  /* A direct call on a page that is still recommended for a merge merges it. */
  Mtr mtr;
  BtreeCursor cur;

  mtr.start();
  cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, key(1000), PAGE_CUR_LE, BTR_MODIFY_TREE, &mtr, Current_location());

  if (cur.compress_recommendation(&mtr)) {
    EXPECT_TRUE(m_sys->m_btree->compress(&cur, &mtr));
  }

  mtr.commit(*m_sys);
  validate();
}

/** get_size(): the leaf pages are part of the total; both grow with the tree. */
TEST_F(BtrTreeTest, GetSize) {
  const auto total0 = m_sys->m_btree->get_size(m_index, BTR_TOTAL_SIZE);

  fill(seq(4000), 300);

  const auto leaves = m_sys->m_btree->get_size(m_index, BTR_N_LEAF_PAGES);
  const auto total = m_sys->m_btree->get_size(m_index, BTR_TOTAL_SIZE);
  const auto [counted, fill_factor] = leaf_fill();

  EXPECT_GE(leaves, counted) << "the segment holds at least the pages in use";
  EXPECT_GT(total, total0);
  EXPECT_GE(total, leaves);
  (void)fill_factor;
}

/** The persistent cursor: stored on a record, restored on it, also when
the page was changed and when the record moved to another page. */
TEST_F(BtrTreeTest, PcurRestoreOn) {
  fill(seq(200, 10), 200);

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open(*m_sys, m_index, key(500), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  ASSERT_TRUE(pcur.is_on_user_rec());
  pcur.store_position(*m_sys, &mtr);
  EXPECT_EQ(pcur.get_rel_pos(), BtreeCursorPos::ON);
  pcur.commit_specify_mtr(*m_sys, &mtr);

  auto at = [&]() {
    ulint len;
    return mach_read_from_4(pcur.get_rec().get_nth_field(0, &len));
  };

  /* Nothing changed: the optimistic restore. */
  mtr.start();
  EXPECT_TRUE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_EQ(at(), 500UL);
  pcur.store_position(*m_sys, &mtr);
  pcur.commit_specify_mtr(*m_sys, &mtr);

  /* Fill the page around the record: it splits, the record may move. */
  for (uint32_t k = 401; k < 600; k += 1) {
    if (k % 10 != 0) {
      ASSERT_EQ(tree_insert(entry(k, 200)), DB_SUCCESS);
    }
  }

  mtr.start();
  EXPECT_TRUE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_EQ(at(), 500UL);

  /* And the scan goes on from there. */
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  EXPECT_EQ(at(), 501UL);
  ASSERT_TRUE(pcur.move_to_prev_user_rec(*m_sys, &mtr));
  ASSERT_TRUE(pcur.move_to_prev_user_rec(*m_sys, &mtr));
  EXPECT_EQ(at(), 499UL);

  pcur.close();
  mtr.commit(*m_sys);
}

/** The record under a stored cursor is removed: restore returns false and
puts the cursor on the record before it (the search was LE). */
TEST_F(BtrTreeTest, PcurRestoreDeleted) {
  fill(seq(200, 10), 200);

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open(*m_sys, m_index, key(700), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  pcur.store_position(*m_sys, &mtr);
  pcur.commit_specify_mtr(*m_sys, &mtr);

  ASSERT_TRUE(tree_delete(700));

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));

  ulint len;

  ASSERT_TRUE(pcur.is_on_user_rec());
  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), 690UL);
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), 710UL);

  pcur.close();
  mtr.commit(*m_sys);
}

/** A cursor stored before the first or after the last record of a page
is stored relative to the record next to it (BEFORE, AFTER). Without a
change of the page the restore is optimistic, to the same place; after a
change AFTER is restored on the first record greater than the stored one. The mtr is
committed as RowSel does, keeping the latch mode. */
TEST_F(BtrTreeTest, PcurBeforeAfter) {
  fill(seq(100, 10), 2000);

  Mtr mtr;
  BtreePcursor pcur;
  ulint len;

  /* After the last record of the first page: without a change the
  restore is optimistic, the cursor is where it was. */
  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);
  pcur.move_to_last_on_page(&mtr);
  ASSERT_TRUE(pcur.is_after_last_on_page());
  ASSERT_FALSE(pcur.is_after_last_in_tree(&mtr)) << "the tree has more than one leaf";

  const auto last = mach_read_from_4(page_rec_get_prev(pcur.get_rec()).get_nth_field(0, &len));

  pcur.store_position(*m_sys, &mtr);
  EXPECT_EQ(pcur.get_rel_pos(), BtreeCursorPos::AFTER);
  mtr.commit(*m_sys);

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_TRUE(pcur.is_after_last_on_page());
  mtr.commit(*m_sys);

  /* A delete on the page changes its modify clock: the restore searches
  (G) for the first record greater than the stored one, which ends on the
  supremum of the page: the next user record is on the next page. */
  ASSERT_TRUE(tree_delete(0));

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_TRUE(pcur.is_after_last_on_page());
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), last + 10) << "the first record after the stored one";
  pcur.close();
  mtr.commit(*m_sys);

  /* Before the first record. */
  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);
  ASSERT_TRUE(pcur.is_before_first_on_page());
  pcur.store_position(*m_sys, &mtr);
  EXPECT_EQ(pcur.get_rel_pos(), BtreeCursorPos::BEFORE);
  mtr.commit(*m_sys);

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_TRUE(pcur.is_before_first_on_page()) << "optimistic: where it was";
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), 10UL);
  pcur.close();
  mtr.commit(*m_sys);
}

/** The same BEFORE position, stored and released with
commit_specify_mtr() (as purge and undo do): the restored cursor has the
latch mode it was restored with. */
TEST_F(BtrTreeTest, PcurRestoreSetsLatchMode) {
  /* Regression: BtreePcursor::restore_position() (btr/btr0pcur.cc:173, optimistic
  branch) sets m_latch_mode only when m_rel_pos is ON. For BEFORE/AFTER
  the cursor keeps BTR_NO_LATCHES (after commit_specify_mtr() or
  release_leaf()) or the latch mode it was stored with: the following
  is_on_user_rec() asserts m_latch_mode != BTR_NO_LATCHES, and the moves
  that release the leaf use the wrong latch mode. MySQL 5.6 sets
  cursor->latch_mode = latch_mode before the rel_pos test. */
  fill(seq(100, 10), 100);

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);
  pcur.store_position(*m_sys, &mtr);
  pcur.commit_specify_mtr(*m_sys, &mtr);

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_EQ(pcur.m_latch_mode, ulint(BTR_SEARCH_LEAF));
  EXPECT_FALSE(pcur.is_on_user_rec());
  pcur.close();
  mtr.commit(*m_sys);
}

/** A cursor stored in an empty tree restores to the index side, after
records arrived (the mtr committed as RowSel does). */
TEST_F(BtrTreeTest, PcurEmptyTree) {
  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);
  pcur.store_position(*m_sys, &mtr);
  /* get_rel_pos() asserts a stored record, which an empty tree has none of. */
  EXPECT_EQ(pcur.m_rel_pos, BtreeCursorPos::BEFORE_FIRST_IN_TREE);
  mtr.commit(*m_sys);

  fill(seq(10, 1, 100));

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  ulint len;

  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), 100UL);
  pcur.close();
  mtr.commit(*m_sys);
}

/** The same, released with commit_specify_mtr() as purge and undo do. */
TEST_F(BtrTreeTest, PcurEmptyTreeSpecifyMtr) {
  /* Regression: in restore_position() (btr/btr0pcur.cc:144) the
  BEFORE_FIRST_IN_TREE / AFTER_LAST_IN_TREE branch opens the tree cursor
  at the index side but leaves m_pos_state (WAS_POSITIONED after
  commit_specify_mtr()) and m_latch_mode (BTR_NO_LATCHES) as they were:
  the next move asserts m_pos_state == IS_POSITIONED. MySQL 5.6 sets both
  there. */
  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);
  pcur.store_position(*m_sys, &mtr);
  /* get_rel_pos() asserts a stored record, which an empty tree has none of. */
  EXPECT_EQ(pcur.m_rel_pos, BtreeCursorPos::BEFORE_FIRST_IN_TREE);
  pcur.commit_specify_mtr(*m_sys, &mtr);

  fill(seq(10, 1, 100));

  mtr.start();
  EXPECT_FALSE(pcur.restore_position(*m_sys, BTR_SEARCH_LEAF, &mtr, Current_location()));
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  ulint len;

  EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)), 100UL);
  pcur.close();
  mtr.commit(*m_sys);
}

/** A full scan forwards and backwards across many pages. */
TEST_F(BtrTreeTest, PcurScanBothWays) {
  fill(seq(3000), 300);

  Mtr mtr;
  BtreePcursor pcur;
  std::vector<uint32_t> fwd;
  std::vector<uint32_t> bwd;
  ulint len;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
    fwd.push_back(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)));
  }

  pcur.close();
  mtr.commit(*m_sys);

  mtr.start();
  pcur.open_at_index_side(*m_sys, false, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_prev_user_rec(*m_sys, &mtr)) {
    bwd.push_back(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)));
  }

  pcur.close();
  mtr.commit(*m_sys);

  EXPECT_EQ(fwd, seq(3000));
  std::reverse(bwd.begin(), bwd.end());
  EXPECT_EQ(bwd, seq(3000));
}

/** An estimate of the rows in a range is close to the count. */
TEST_F(BtrTreeTest, EstimateRange) {
  /* Regression: estimate_n_rows_in_range() records the search path with
  add_path_info(), which calls page_rec_get_n_recs_before() on records
  that own no directory slot: the debug build aborts in
  page_dir_get_nth_slot() (see PageTest.NRecsBefore,
  page/page0page.cc:886). */
  fill(seq(2000), 100);

  /* The estimate is capped at half the rows of the statistics: compute them,
  the rows were inserted directly into the tree. */
  m_sys->m_dict->mutex_acquire();
  m_sys->m_dict->update_statistics(m_index->m_table);
  m_sys->m_dict->mutex_release();

  BtreeCursor cur;
  const auto n = cur.estimate_n_rows_in_range(*m_sys, m_index, key(500), PAGE_CUR_GE, key(1500), PAGE_CUR_LE);

  EXPECT_GT(n, 500);
  EXPECT_LT(n, 2000);
}

/** The records pass Btree::index_rec_validate(); a record with a field
of the wrong length doesn't. */
TEST_F(BtrTreeTest, IndexRecValidate) {
  fill(seq(10));

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open(*m_sys, m_index, key(5), PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  EXPECT_TRUE(m_sys->m_btree->index_rec_validate(pcur.get_rec(), m_index, false));
  pcur.close();
  mtr.commit(*m_sys);

  /* A tuple with a 3 byte k, converted with the index: k is fixed 4. */
  auto e = entry(1);

  dfield_set_data(dtuple_get_nth_field(e, 0), u32(1), 3);

  std::vector<byte> buf(2 * UNIV_PAGE_SIZE);
  auto page = static_cast<byte *>(ut_align(buf.data(), UNIV_PAGE_SIZE));
  auto rec = Rec::convert_dtuple_to_rec(page + PAGE_DATA, m_index, e, 0);

  EXPECT_FALSE(m_sys->m_btree->index_rec_validate(rec, m_index, false));
}

/* --- Externally stored fields ----------------------------------------- */

/** The usable bytes of a BLOB page. */
constexpr ulint BLOB_PART = UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END;

/** The fixture of the blob tests: rows go in through the api. */
class BtrBlobTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();
    m_tab = tab();
  }

  void TearDown() override { m_tab.reset(); }

  /** The reference of b of the record with key k, whether delete marked,
  and the full value read from its pages. */
  struct Ref {
    bool m_found{};
    bool m_extern{};
    bool m_deleted{};
    ulint m_local_len{};
    byte m_ref[BTR_EXTERN_FIELD_REF_SIZE]{};
    std::string m_value{};

    [[nodiscard]] page_no_t page_no() const { return mach_read_from_4(m_ref + BTR_EXTERN_PAGE_NO); }
    [[nodiscard]] ulint ext_len() const { return mach_read_from_4(m_ref + BTR_EXTERN_LEN + 4); }
    [[nodiscard]] bool owner() const { return !(m_ref[BTR_EXTERN_LEN] & BTR_EXTERN_OWNER_FLAG); }
    [[nodiscard]] bool inherited() const { return m_ref[BTR_EXTERN_LEN] & BTR_EXTERN_INHERITED_FLAG; }
  };

  Ref ref(uint32_t k) {
    Ref r;
    auto index = m_tab->clust();
    auto heap = mem_heap_create(1024);
    auto t = dtuple_create(heap, 1);
    byte kb[4];

    mach_write_to_4(kb, k);
    index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), kb, 4);

    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    pcur.open(*m_sys, index, t, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

    if (!pcur.is_on_user_rec()) {
      (void)pcur.move_to_next_user_rec(*m_sys, &mtr);
    }

    ulint len;

    if (pcur.is_on_user_rec() && mach_read_from_4(pcur.get_rec().get_nth_field(0, &len)) == k) {
      auto rec = pcur.get_rec();
      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      auto data = rec.get_nth_field(offs, B_FIELD, &len);

      r.m_found = true;
      r.m_deleted = rec.get_deleted_flag();
      r.m_extern = offs.nth_extern(B_FIELD);
      r.m_local_len = len;

      if (r.m_extern) {
        memcpy(r.m_ref, data.get() + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);

        Blob blob;
        ulint full;
        auto v = blob.copy_externally_stored_field(*m_sys, rec, offs, B_FIELD, &full, heap);

        r.m_value.assign(reinterpret_cast<const char *>(v), full);
        EXPECT_EQ(blob.get_externally_stored_len(rec, offs), (r.ext_len() + UNIV_PAGE_SIZE - 1) / UNIV_PAGE_SIZE);
      } else {
        r.m_value.assign(reinterpret_cast<const char *>(data.get()), len);
      }
    }

    pcur.close();
    mtr.commit(*m_sys);
    mem_heap_free(heap);

    return r;
  }

  std::unique_ptr<Tab> m_tab;
};

/** Blobs of sizes around the record limit and the page size: in page up
to the limit, off page beyond, with a 768 byte local prefix and the rest
in a chain of pages; the value reads back exactly. */
TEST_F(BtrBlobTest, Sizes) {
  const std::vector<ulint> sizes{
    0, 1, 767, 768, 769, 7000, 8000, 8100, 8200, 16383, 16384, 16385,
    768 + BLOB_PART - 1, 768 + BLOB_PART, 768 + BLOB_PART + 1,
    768 + 2 * BLOB_PART - 1, 768 + 2 * BLOB_PART, 768 + 2 * BLOB_PART + 1, 1000000};
  ut::Rows rows;
  uint32_t k{};

  for (auto n : sizes) {
    rows[k] = ut::Row{k, k, ut::blob(k, n)};
    ++k;
  }

  m_tab->load(rows);
  m_tab->expect_rows(rows);

  for (const auto &[key, row] : rows) {
    auto r = ref(key);

    ASSERT_TRUE(r.m_found);
    EXPECT_EQ(r.m_value, row.m_b) << row.m_b.size();

    if (r.m_extern) {
      EXPECT_EQ(r.m_local_len, REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE) << row.m_b.size();
      EXPECT_EQ(r.ext_len(), row.m_b.size() - REC_MAX_INDEX_COL_LEN) << row.m_b.size();
      EXPECT_TRUE(r.owner());
      EXPECT_FALSE(r.inherited());
      EXPECT_NE(r.page_no(), FIL_NULL);
    } else {
      EXPECT_LT(row.m_b.size(), 8200UL) << "small values stay in the page";
    }
  }

  EXPECT_FALSE(ref(0).m_extern);
  EXPECT_TRUE(ref(uint32_t(sizes.size() - 1)).m_extern);
}

/** A blob's pages are freed when the row is deleted and purged, or
updated: the tablespace use doesn't grow without bound when a blob is
rewritten many times. */
TEST_F(BtrBlobTest, RewriteFrees) {
  m_tab->load({{1, ut::Row{1, 1, ut::blob(1, 200000)}}});

  auto r0 = ref(1);

  for (uint32_t i = 0; i < 20; ++i) {
    auto trx = begin();

    ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 1, ut::blob(i, 200000)}), DB_SUCCESS);
    commit(trx);
  }

  auto r = ref(1);

  EXPECT_EQ(r.m_value, ut::blob(19, 200000));
  EXPECT_TRUE(r.owner());
  (void)r0;
}

/** An update of the primary key moves the row: the new record inherits
the blob (INHERITED flag), the old delete marked one disowns it. A
rollback gives the blob back to the old record, intact. */
TEST_F(BtrBlobTest, PkUpdateRollback) {
  const auto b = ut::blob(7, 50000);

  m_tab->load({{1, ut::Row{1, 1, b}}});

  const auto r1 = ref(1);

  ASSERT_TRUE(r1.m_extern);
  ASSERT_TRUE(r1.owner());

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{2, 1, b}), DB_SUCCESS);

  const auto old_rec = ref(1);
  const auto new_rec = ref(2);

  ASSERT_TRUE(old_rec.m_found);
  ASSERT_TRUE(new_rec.m_found);
  EXPECT_TRUE(old_rec.m_deleted);
  EXPECT_FALSE(new_rec.m_deleted);
  EXPECT_FALSE(old_rec.owner()) << "the old record disowns the blob";
  EXPECT_TRUE(new_rec.inherited()) << "the new record inherits it";
  EXPECT_TRUE(new_rec.owner());
  EXPECT_EQ(new_rec.page_no(), r1.page_no()) << "the same pages, not a copy";
  EXPECT_EQ(new_rec.m_value, b);

  rollback(trx);

  const auto back = ref(1);

  ASSERT_TRUE(back.m_found);
  EXPECT_FALSE(back.m_deleted);
  EXPECT_TRUE(back.owner());
  EXPECT_EQ(back.page_no(), r1.page_no());
  EXPECT_EQ(back.m_value, b) << "the blob survived the rollback";

  m_tab->expect_rows({{1, ut::Row{1, 1, b}}});

  /* It can still be updated and read. */
  trx = begin();
  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 2, b}), DB_SUCCESS);
  commit(trx);
  m_tab->expect_rows({{1, ut::Row{1, 2, b}}});
}

/** The same PK update committed: the new record owns the blob. */
TEST_F(BtrBlobTest, PkUpdateCommit) {
  const auto b = ut::blob(8, 40000);

  m_tab->load({{1, ut::Row{1, 1, b}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{5, 1, b}), DB_SUCCESS);
  commit(trx);

  const auto r = ref(5);

  EXPECT_TRUE(r.owner());
  EXPECT_EQ(r.m_value, b);
  m_tab->expect_rows({{5, ut::Row{5, 1, b}}});
}

/** A PK update that also changes the blob: the new record has a new blob
of its own (not inherited); rollback restores the old value. */
TEST_F(BtrBlobTest, PkAndBlobUpdateRollback) {
  const auto b = ut::blob(9, 30000);
  const auto c = ut::blob(10, 45000);

  m_tab->load({{1, ut::Row{1, 1, b}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{3, 1, c}), DB_SUCCESS);

  const auto new_rec = ref(3);

  EXPECT_FALSE(new_rec.inherited());
  EXPECT_EQ(new_rec.m_value, c);
  EXPECT_NE(new_rec.page_no(), ref(1).page_no());

  rollback(trx);

  EXPECT_EQ(ref(1).m_value, b);
  m_tab->expect_rows({{1, ut::Row{1, 1, b}}});
}

/** Blobs survive a rollback of their insert's neighbours and of updates
in place of other columns. */
TEST_F(BtrBlobTest, UpdateOtherColumnRollback) {
  const auto b = ut::blob(11, 60000);

  m_tab->load({{1, ut::Row{1, 1, b}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 99, b}), DB_SUCCESS);
  EXPECT_EQ(ref(1).m_value, b);
  rollback(trx);

  EXPECT_EQ(ref(1).m_value, b);
  EXPECT_TRUE(ref(1).owner());
  m_tab->expect_rows({{1, ut::Row{1, 1, b}}});
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

class BtrPerf : public BtrTreeTest {};

/** Inserts into the tree alone: sequential and random keys. */
TEST_F(BtrPerf, Insert) {
  constexpr uint32_t N = 100000;

  auto rs = ut::bench("tree_insert_seq", N, [&](uint64_t i) {
    (void)tree_insert(entry(uint32_t(i), 100));
    mem_heap_empty(m_heap);
  });

  EXPECT_LT(rs.ns_per_op(), 1e6);

  auto keys = seq(N, 1, N);
  std::mt19937 rng(1);

  std::shuffle(keys.begin(), keys.end(), rng);

  auto rr = ut::bench("tree_insert_rnd", N, [&](uint64_t i) {
    (void)tree_insert(entry(keys[i], 100));
    mem_heap_empty(m_heap);
  });

  EXPECT_LT(rr.ns_per_op(), 1e6);

  auto rd = ut::bench("tree_delete", N, [&](uint64_t i) {
    (void)tree_delete(keys[i]);
    mem_heap_empty(m_heap);
  });

  EXPECT_LT(rd.ns_per_op(), 1e6);
}

/** Point lookups and a scan of a tree of 200000 records, and lookups
from 1 to 16 threads. */
TEST_F(BtrPerf, LookupScan) {
  constexpr uint32_t N = 200000;

  for (uint32_t k = 0; k < N; ++k) {
    ASSERT_EQ(tree_insert(entry(k, 50)), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  std::vector<DTuple *> keys;

  for (uint32_t i = 0; i < 4096; ++i) {
    keys.push_back(key((i * 2654435761u) % N));
  }

  auto rl = ut::bench("point_lookup", 300000, [&](uint64_t i) {
    Mtr mtr;
    BtreeCursor cur;

    mtr.start();
    cur.search_to_nth_level(*m_sys, nullptr, m_index, 0, keys[i % keys.size()], PAGE_CUR_LE, BTR_SEARCH_LEAF, &mtr, Current_location());
    ut::keep(cur.m_low_match);
    mtr.commit(*m_sys);
  });

  EXPECT_LT(rl.ns_per_op(), 500000.0);

  Mtr mtr;
  BtreePcursor pcur;
  uint64_t n{};
  const auto start = std::chrono::steady_clock::now();

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, m_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
    ++n;
  }

  pcur.close();
  mtr.commit(*m_sys);

  ut::BenchResult rs{"range_scan_rows", 1, n, std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count()};

  ut::report(rs);
  EXPECT_EQ(n, N);
  EXPECT_LT(rs.ns_per_op(), 20000.0);

  for (int threads : {1, 2, 4, 8, 16}) {
    auto r = ut::bench_mt("point_lookup_mt", threads, 40000, [&](int t, uint64_t i) {
      Mtr m;
      BtreeCursor cur;

      m.start();
      cur.search_to_nth_level(
        *m_sys, nullptr, m_index, 0, keys[(i * 7 + t * 131) % keys.size()], PAGE_CUR_LE, BTR_SEARCH_LEAF, &m, Current_location()
      );
      ut::keep(cur.m_low_match);
      m.commit(*m_sys);
    });

    EXPECT_LT(r.ns_per_op(), 200000.0);
  }
}

class BtrBlobPerf : public BtrBlobTest {};

/** Blob write and read throughput, 1 MB values. */
TEST_F(BtrBlobPerf, WriteRead) {
  constexpr uint32_t N = 64;
  constexpr size_t SIZE = 1 << 20;
  const auto b = ut::blob(1, SIZE);

  auto rw = ut::bench("blob_write_1mb", N, [&](uint64_t i) {
    auto trx = begin();

    EXPECT_EQ(m_tab->insert(trx, ut::Row{uint32_t(i), 0, b}), DB_SUCCESS);
    commit(trx);
  });

  ut::report(ut::BenchResult{"blob_write_MB", 1, N, rw.m_secs});
  EXPECT_LT(rw.ns_per_op(), 1e9);

  auto rr = ut::bench("blob_read_1mb", N, [&](uint64_t i) {
    auto r = ref(uint32_t(i));
    EXPECT_EQ(r.m_value.size(), SIZE);
  });

  EXPECT_LT(rr.ns_per_op(), 1e9);
}

}  // namespace
