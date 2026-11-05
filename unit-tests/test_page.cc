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

/** @file unit-tests/test_page.cc
Page and PageCursor: index pages of a real tablespace, allocated from the
segments of the test table's indexes but not linked into their trees.
Create, insert in every order until full, search, delete, the record heap,
the free list and the garbage, the directory slots and their n_owned
bounds, reorganize, and copying, moving and deleting lists of records.
page_validate() and the directory invariants are checked after each step. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "buf0buf.h"
#include "data0data.h"
#include "dict0dict.h"
#include "fsp0types.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "rem0rec.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

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

/** The fixture: the test table's indexes and helpers to make entries and
pages of them. The clustered index of a Tab has the fields
(k, DB_TRX_ID, DB_ROLL_PTR, a, b), SEC_A has (a, k). */
class PageTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();

    m_tab = tab();
    m_clust = m_tab->clust();
    m_sec = m_tab->sec();
    m_heap = mem_heap_create(16384);

    ASSERT_NE(m_sec, nullptr);
  }

  void TearDown() override {
    mem_heap_free(m_heap);
  }

  /** @return a 4 byte big endian value in m_heap. */
  byte *u32(uint32_t v) {
    auto p = static_cast<byte *>(mem_heap_alloc(m_heap, 4));

    mach_write_to_4(p, v);
    return p;
  }

  /** @return an entry (a, k) of SEC_A. */
  DTuple *sec_entry(uint32_t a, uint32_t k) {
    auto t = dtuple_create(m_heap, 2);

    m_sec->copy_types(t, 2);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(a), 4);
    dfield_set_data(dtuple_get_nth_field(t, 1), u32(k), 4);

    return t;
  }

  /** @return an entry (k, trx_id, roll_ptr, a, b) of the clustered index, b of blen bytes. */
  DTuple *clust_entry(uint32_t k, ulint blen) {
    const auto n = m_clust->get_n_fields();
    auto t = dtuple_create(m_heap, n);

    m_clust->copy_types(t, n);

    dfield_set_data(dtuple_get_nth_field(t, 0), u32(k), 4);

    auto sys = static_cast<byte *>(mem_heap_zalloc(m_heap, DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN));

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, DATA_TRX_ID_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + DATA_TRX_ID_LEN, DATA_ROLL_PTR_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 3), u32(k * 3), 4);

    auto b = static_cast<byte *>(mem_heap_alloc(m_heap, std::max<ulint>(blen, 1)));

    memset(b, int('a' + k % 26), blen);
    dfield_set_data(dtuple_get_nth_field(t, 4), b, blen);

    return t;
  }

  /** @return a new empty leaf page of index, X-latched in mtr. */
  BufBlock *alloc(Mtr &mtr, Index *index, ulint level = 0) {
    auto block = m_sys->m_btree->page_alloc(index, 0, FSP_UP, level, &mtr);

    EXPECT_NE(block, nullptr);

    if (block != nullptr) {
      m_sys->m_btree->page_create(block, index, level, &mtr);

      /* As if a transaction had inserted into the secondary index leaf:
      copying records between such pages carries the max trx id over. */
      if (!index->is_clustered() && level == 0 && m_max_trx_id) {
        page_set_max_trx_id(block, 1, &mtr);
      }
    }

    return block;
  }

  /** Give the page back to its segment. */
  void free_page(Mtr &mtr, Index *index, BufBlock *block) { m_sys->m_btree->page_free(index, block, &mtr); }

  /** Insert t at its place in the page. @return the record or nullptr if it doesn't fit. */
  Rec insert(Mtr &mtr, BufBlock *block, Index *index, const DTuple *t) {
    PageCursor cur;

    (void)cur.search(*m_sys, block, index, t, PAGE_CUR_LE);

    return cur.tuple_insert(t, index, 0, &mtr);
  }

  /** @return the offsets of rec, in m_heap. */
  RecOffs offsets(Index *index, Rec rec) {
    return PhyRec(index, rec).get_all_col_offsets(nullptr, &m_heap, Current_location());
  }

  /** Delete rec from its page. */
  void remove(Mtr &mtr, BufBlock *block, Index *index, Rec rec) {
    PageCursor cur;
    auto offs = offsets(index, rec);

    cur.position(rec, block);
    cur.delete_rec(*m_sys, index, offs, &mtr);
  }

  /** @return the first field (a 4 byte integer) of each user record, in order. */
  static std::vector<uint32_t> keys(const page_t *page) {
    std::vector<uint32_t> v;

    for (auto rec = page_rec_get_next_const(page_get_infimum_rec(page)); !page_rec_is_supremum(rec);
         rec = page_rec_get_next_const(rec)) {
      ulint len;
      auto f = rec.get_nth_field(0, &len);

      v.push_back(mach_read_from_4(f));
    }

    return v;
  }

  /** @return the user records of the page in order. */
  static std::vector<Rec> recs(const page_t *page) {
    std::vector<Rec> v;

    for (auto rec = page_rec_get_next_const(page_get_infimum_rec(page)); !page_rec_is_supremum(rec);
         rec = page_rec_get_next_const(rec)) {
      v.push_back(rec);
    }

    return v;
  }

  /** The directory: slot 0 owns the infimum alone, the last slot owns the
  supremum and 1..MAX records, every other slot MIN..MAX records; the
  slots point to records in list order, the owned counts add up to all
  records, and each slot points to a record that owns its count. */
  void check_dir(const page_t *page) {
    const auto n_slots = page_dir_get_n_slots(page);

    ASSERT_GE(n_slots, 2UL);

    ulint sum{};
    auto list_rec = page_get_infimum_rec(page);
    ulint prev_slot_no{};

    for (ulint i{}; i < n_slots; ++i) {
      auto slot = page_dir_get_nth_slot(page, i);
      auto rec = page_dir_slot_get_rec(slot);
      const auto n_owned = rec.get_n_owned();

      sum += n_owned;

      if (i == 0) {
        ASSERT_TRUE(page_rec_is_infimum(rec));
        ASSERT_EQ(n_owned, 1UL);
      } else if (i == n_slots - 1) {
        ASSERT_TRUE(page_rec_is_supremum(rec));
        ASSERT_GE(n_owned, 1UL);
        ASSERT_LE(n_owned, PAGE_DIR_SLOT_MAX_N_OWNED);
      } else {
        ASSERT_GE(n_owned, PAGE_DIR_SLOT_MIN_N_OWNED) << "slot " << i;
        ASSERT_LE(n_owned, PAGE_DIR_SLOT_MAX_N_OWNED) << "slot " << i;
      }

      /* Walk the list to the record of the slot: n_owned records, the
      last of them the slot's. */
      if (i > 0) {
        for (ulint j{}; j < n_owned; ++j) {
          list_rec = page_rec_get_next_const(list_rec);
          ASSERT_FALSE(list_rec.is_null());

          if (j + 1 < n_owned) {
            ASSERT_EQ(list_rec.get_n_owned(), 0UL) << "slot " << i << " record " << j;
          }
        }

        ASSERT_EQ(list_rec, rec) << "slot " << i;
        ASSERT_EQ(page_dir_find_owner_slot(rec), i);
      }

      prev_slot_no = i;
    }

    (void)prev_slot_no;
    ASSERT_EQ(sum, page_get_n_recs(page) + 2);
  }

  /** page_validate(), page_simple_validate() and check_dir(). */
  void validate(BufBlock *block, Index *index) {
    auto page = block->get_frame();

    ASSERT_TRUE(page_simple_validate(page));
    ASSERT_TRUE(page_validate(page, index));
    check_dir(page);
  }

  /** @return the sum of the sizes of the user records. */
  ulint data_size(Index *index, const page_t *page) {
    ulint size{};

    for (auto rec : recs(page)) {
      size += offsets(index, rec).size();
    }

    return size;
  }

  /** @return the number of records in the free list. */
  static ulint n_free(page_t *page) {
    ulint n{};

    for (auto p = page_header_get_ptr(page, PAGE_FREE); p != nullptr;) {
      Rec rec(p);

      ++n;
      p = rec.get_next_offs() == 0 ? nullptr : page + rec.get_next_offs();
    }

    return n;
  }

  std::unique_ptr<Tab> m_tab;
  bool m_max_trx_id{true};
  Index *m_clust{};
  Index *m_sec{};
  MemHeap *m_heap{};
};

/** An empty page: infimum and supremum, two slots, nothing else. */
TEST_F(PageTest, CreateEmpty) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  EXPECT_EQ(page_get_n_recs(page), 0UL);
  EXPECT_EQ(page_dir_get_n_heap(page), PAGE_HEAP_NO_USER_LOW);
  EXPECT_EQ(page_dir_get_n_slots(page), 2UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), PAGE_SUPREMUM_END);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_FREE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_LAST_INSERT), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_DIRECTION), PAGE_NO_DIRECTION);
  EXPECT_EQ(page_get_data_size(page), 0UL);
  EXPECT_TRUE(page_is_leaf(page));
  EXPECT_EQ(m_sys->m_btree->page_get_index_id(page), m_sec->m_id);

  auto inf = page_get_infimum_rec(page);
  auto sup = page_get_supremum_rec(page);

  EXPECT_EQ(page_offset(inf), PAGE_INFIMUM);
  EXPECT_EQ(page_offset(sup), PAGE_SUPREMUM);
  EXPECT_EQ(page_rec_get_next(inf), sup);
  EXPECT_TRUE(page_rec_get_next(sup).is_null());
  EXPECT_EQ(inf.get_heap_no(), PAGE_HEAP_NO_INFIMUM);
  EXPECT_EQ(sup.get_heap_no(), PAGE_HEAP_NO_SUPREMUM);
  EXPECT_EQ(inf.get_n_owned(), 1UL);
  EXPECT_EQ(sup.get_n_owned(), 1UL);
  EXPECT_TRUE(page_rec_is_infimum(inf));
  EXPECT_TRUE(page_rec_is_supremum(sup));
  EXPECT_FALSE(page_rec_is_user_rec(inf));

  /* The free space of an empty page, less a slot for one record. */
  EXPECT_EQ(page_get_max_insert_size(page, 1), page_get_free_space_of_empty() - page_dir_calc_reserved_space(1));

  validate(block, m_sec);

  /* A non-leaf page. */
  auto node = alloc(mtr, m_sec, 1);

  EXPECT_FALSE(page_is_leaf(node->get_frame()));
  EXPECT_EQ(m_sys->m_btree->page_get_level(node->get_frame(), &mtr), 1UL);

  free_page(mtr, m_sec, node);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Records inserted in ascending order: in order on the page, the insert
direction is PAGE_RIGHT and counted, the heap numbers are allocated in
order, the directory stays within its bounds. */
TEST_F(PageTest, InsertAscending) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();
  std::vector<uint32_t> expected;

  for (uint32_t i = 0; i < 300; ++i) {
    auto rec = insert(mtr, block, m_sec, sec_entry(i, i));

    ASSERT_FALSE(rec.is_null()) << i;
    EXPECT_EQ(rec.get_heap_no(), PAGE_HEAP_NO_USER_LOW + i);
    expected.push_back(i);

    if (i >= 2) {
      ASSERT_EQ(page_header_get_field(page, PAGE_DIRECTION), PAGE_RIGHT);
      ASSERT_EQ(page_header_get_field(page, PAGE_N_DIRECTION), ulint(i));
    }

    ASSERT_EQ(page_header_get_ptr(page, PAGE_LAST_INSERT), rec.get());

    if (i % 37 == 0) {
      validate(block, m_sec);
    }
  }

  EXPECT_EQ(keys(page), expected);
  EXPECT_EQ(page_get_n_recs(page), 300UL);
  EXPECT_EQ(page_dir_get_n_heap(page), 302UL);
  EXPECT_EQ(page_get_data_size(page), data_size(m_sec, page));
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Descending inserts: the direction is PAGE_LEFT. */
TEST_F(PageTest, InsertDescending) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 300; i-- > 0;) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());

    if (i <= 297) {
      ASSERT_EQ(page_header_get_field(page, PAGE_DIRECTION), PAGE_LEFT) << i;
      ASSERT_EQ(page_header_get_field(page, PAGE_N_DIRECTION), ulint(299 - i)) << i;
    }
  }

  std::vector<uint32_t> expected(300);

  std::iota(expected.begin(), expected.end(), 0);
  EXPECT_EQ(keys(page), expected);
  validate(block, m_sec);

  /* An insert in the middle resets the direction. */
  remove(mtr, block, m_sec, recs(page)[150]);
  ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(150, 1)).is_null());
  EXPECT_EQ(page_header_get_field(page, PAGE_DIRECTION), PAGE_NO_DIRECTION);
  EXPECT_EQ(page_header_get_field(page, PAGE_N_DIRECTION), 0UL);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Random order until the page is full: the insert that doesn't fit
returns nullptr and changes nothing; the space left is less than the
record; the records are in order and the data size adds up. */
TEST_F(PageTest, InsertUntilFull) {
  for (ulint blen : {ulint(0), ulint(10), ulint(100), ulint(700), ulint(3000)}) {
    Mtr mtr;

    mtr.start();

    auto block = alloc(mtr, m_clust);
    auto page = block->get_frame();
    std::mt19937 rng{uint32_t(blen)};
    std::vector<uint32_t> inserted;

    for (;;) {
      const auto k = uint32_t(rng());
      auto t = clust_entry(k, blen);
      const auto n_recs = page_get_n_recs(page);
      const auto heap_top = page_header_get_field(page, PAGE_HEAP_TOP);
      auto rec = insert(mtr, block, m_clust, t);

      if (rec.is_null()) {
        EXPECT_EQ(page_get_n_recs(page), n_recs);
        EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), heap_top);
        EXPECT_LT(page_get_max_insert_size(page, 1), rec_get_converted_size(m_clust, t, 0));
        break;
      }

      inserted.push_back(k);
    }

    std::sort(inserted.begin(), inserted.end());

    EXPECT_EQ(keys(page), inserted) << blen;
    EXPECT_EQ(page_get_data_size(page), data_size(m_clust, page)) << blen;
    /* The page is full: less than a record is left, but most of it is used. */
    EXPECT_GT(page_get_data_size(page), page_get_free_space_of_empty() * 8 / 10) << blen;
    validate(block, m_clust);

    free_page(mtr, m_clust, block);
    mtr.commit(*m_sys);
  }
}

/** The search modes: G, GE, L, LE on keys that exist, between keys and
outside the range of the page, and on an empty page. */
TEST_F(PageTest, SearchModes) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  /* An empty page: G/GE give the supremum, L/LE the infimum. */
  {
    PageCursor cur;

    (void)cur.search(*m_sys, block, m_sec, sec_entry(5, 5), PAGE_CUR_GE);
    EXPECT_TRUE(cur.is_after_last());
    (void)cur.search(*m_sys, block, m_sec, sec_entry(5, 5), PAGE_CUR_LE);
    EXPECT_TRUE(cur.is_before_first());
  }

  /* Keys 10, 20, ..., 1000 (a, a). */
  for (uint32_t a = 10; a <= 1000; a += 10) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(a, a)).is_null());
  }

  auto key_at = [&](PageCursor &cur) -> int64_t {
    if (cur.is_before_first()) {
      return -1;
    } else if (cur.is_after_last()) {
      return -2;
    }

    ulint len;

    return mach_read_from_4(cur.get_rec().get_nth_field(0, &len));
  };

  struct Case {
    uint32_t m_key;
    ulint m_mode;
    int64_t m_expected;
  };

  const std::vector<Case> cases{
    {500, PAGE_CUR_G, 510},  {500, PAGE_CUR_GE, 500}, {500, PAGE_CUR_L, 490},  {500, PAGE_CUR_LE, 500},
    {505, PAGE_CUR_G, 510},  {505, PAGE_CUR_GE, 510}, {505, PAGE_CUR_L, 500},  {505, PAGE_CUR_LE, 500},
    {10, PAGE_CUR_L, -1},    {10, PAGE_CUR_LE, 10},   {10, PAGE_CUR_GE, 10},   {5, PAGE_CUR_LE, -1},
    {5, PAGE_CUR_GE, 10},    {5, PAGE_CUR_G, 10},     {1000, PAGE_CUR_G, -2},  {1000, PAGE_CUR_GE, 1000},
    {1000, PAGE_CUR_LE, 1000}, {2000, PAGE_CUR_GE, -2}, {2000, PAGE_CUR_L, 1000}, {0, PAGE_CUR_L, -1},
  };

  for (const auto &c : cases) {
    PageCursor cur;

    (void)cur.search(*m_sys, block, m_sec, sec_entry(c.m_key, c.m_key), c.m_mode);
    EXPECT_EQ(key_at(cur), c.m_expected) << "key " << c.m_key << " mode " << c.m_mode;
  }

  /* A prefix tuple (a only): GE goes to the first record with a, LE to the last. */
  for (uint32_t a = 0; a < 3; ++a) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(500, 1000 + a)).is_null());
  }

  auto prefix = sec_entry(500, 0);

  dtuple_set_n_fields_cmp(prefix, 1);

  PageCursor cur;
  auto low_match = cur.search(*m_sys, block, m_sec, prefix, PAGE_CUR_GE);
  ulint len;

  EXPECT_EQ(mach_read_from_4(cur.get_rec().get_nth_field(1, &len)), 500UL) << "(500, 500) is the first";
  EXPECT_EQ(low_match, 0UL);

  (void)cur.search(*m_sys, block, m_sec, prefix, PAGE_CUR_LE);
  EXPECT_EQ(mach_read_from_4(cur.get_rec().get_nth_field(1, &len)), 1002UL) << "(500, 1002) is the last";

  (void)page;
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Deletes put the records in the free list and count their size as
garbage; the data size and n_recs go down; the directory rebalances. */
TEST_F(PageTest, DeleteGarbage) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_clust);
  auto page = block->get_frame();

  for (uint32_t k = 0; k < 100; ++k) {
    ASSERT_FALSE(insert(mtr, block, m_clust, clust_entry(k, 20 + k % 7)).is_null());
  }

  const auto data_before = page_get_data_size(page);
  ulint garbage{};
  ulint deleted{};

  for (uint32_t k = 0; k < 100; k += 3) {
    auto rec = recs(page)[k - deleted];
    const auto size = offsets(m_clust, rec).size();

    remove(mtr, block, m_clust, rec);
    garbage += size;
    ++deleted;

    ASSERT_EQ(page_header_get_field(page, PAGE_GARBAGE), garbage);
    ASSERT_EQ(page_get_n_recs(page), 100 - deleted);
    ASSERT_EQ(page_get_data_size(page), data_before - garbage);
    ASSERT_EQ(page_header_get_field(page, PAGE_LAST_INSERT), 0UL);
    validate(block, m_clust);
  }

  EXPECT_EQ(n_free(page), deleted);

  std::vector<uint32_t> expected;

  for (uint32_t k = 0; k < 100; ++k) {
    if (k % 3 != 0) {
      expected.push_back(k);
    }
  }

  EXPECT_EQ(keys(page), expected);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** An insert reuses the head of the free list if it is big enough: same
heap number, the heap top doesn't move, the garbage shrinks by the size of
the new record. If the head is too small the heap is used. */
TEST_F(PageTest, FreeListReuse) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_clust);
  auto page = block->get_frame();

  for (uint32_t k = 0; k < 20; ++k) {
    ASSERT_FALSE(insert(mtr, block, m_clust, clust_entry(k * 10, 50)).is_null());
  }

  auto victim = recs(page)[5];
  const auto heap_no = victim.get_heap_no();
  const auto victim_size = offsets(m_clust, victim).size();

  remove(mtr, block, m_clust, victim);

  const auto heap_top = page_header_get_field(page, PAGE_HEAP_TOP);

  ASSERT_EQ(page_header_get_field(page, PAGE_GARBAGE), victim_size);

  /* Smaller than the free record: it is reused. */
  auto t = clust_entry(55, 40);
  const auto size = rec_get_converted_size(m_clust, t, 0);
  auto rec = insert(mtr, block, m_clust, t);

  ASSERT_FALSE(rec.is_null());
  EXPECT_EQ(rec.get_heap_no(), heap_no);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), heap_top);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), victim_size - size);
  EXPECT_EQ(page_header_get_field(page, PAGE_FREE), 0UL);
  validate(block, m_clust);

  /* Free a small one, insert a bigger one: from the heap. */
  victim = recs(page)[5];
  remove(mtr, block, m_clust, victim);

  const auto garbage = page_header_get_field(page, PAGE_GARBAGE);
  const auto n_heap = page_dir_get_n_heap(page);

  rec = insert(mtr, block, m_clust, clust_entry(56, 500));
  ASSERT_FALSE(rec.is_null());
  EXPECT_EQ(rec.get_heap_no(), n_heap);
  EXPECT_GT(page_header_get_field(page, PAGE_HEAP_TOP), heap_top);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), garbage);
  EXPECT_NE(page_header_get_field(page, PAGE_FREE), 0UL);
  validate(block, m_clust);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** Directory slots split when a slot would own more than MAX records and
balance (borrow or merge) when it would own fewer than MIN. */
TEST_F(PageTest, DirSlots) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  /* Up to 8 records the supremum's slot owns them all (with itself). */
  for (uint32_t i = 0; i < 7; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
  }

  EXPECT_EQ(page_dir_get_n_slots(page), 2UL);
  EXPECT_EQ(page_get_supremum_rec(page).get_n_owned(), 8UL);

  /* The 8th record makes 9: the slot splits. */
  ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(7, 0)).is_null());
  EXPECT_EQ(page_dir_get_n_slots(page), 3UL);
  check_dir(page);

  /* Many inserts in the same place: always within bounds. */
  for (uint32_t i = 0; i < 200; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(3, 1 + i)).is_null());
    check_dir(page);
  }

  const auto n_slots = page_dir_get_n_slots(page);

  EXPECT_GE(n_slots, 2 + 208 / PAGE_DIR_SLOT_MAX_N_OWNED);

  /* Delete from the middle: slots borrow and merge, the bounds hold. */
  for (int i = 0; i < 150; ++i) {
    auto v = recs(page);

    remove(mtr, block, m_sec, v[v.size() / 2]);
    check_dir(page);
  }

  EXPECT_LT(page_dir_get_n_slots(page), n_slots);

  /* Down to nothing. */
  while (page_get_n_recs(page) > 0) {
    remove(mtr, block, m_sec, recs(page)[0]);
    check_dir(page);
  }

  EXPECT_EQ(page_dir_get_n_slots(page), 2UL);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** page_dir_find_owner_slot() and page_rec_get_prev() agree with the list. */
TEST_F(PageTest, OwnerAndPrev) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 100; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i * 7 % 100, 0)).is_null());
  }

  Rec prev = page_get_infimum_rec(page);

  for (auto rec = page_rec_get_next(prev);; rec = page_rec_get_next(rec)) {
    EXPECT_EQ(page_rec_get_prev(rec), prev);

    auto owner = page_rec_find_owner_rec(rec);
    auto slot = page_dir_get_nth_slot(page, page_dir_find_owner_slot(rec));

    EXPECT_EQ(page_dir_slot_get_rec(slot), owner);

    if (page_rec_is_supremum(rec)) {
      break;
    }

    prev = rec;
  }

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** page_rec_get_n_recs_before() is the number of records before a record,
the infimum included, for owners of directory slots and for the records
between them. */
TEST_F(PageTest, NRecsBefore) {
  /* Regression: page_rec_get_n_recs_before() (page/page0page.cc:886) walks a copy
  of rec (current_rec) to its owner, then looks for the original rec among
  the directory slots: a record that owns no slot is never found, the loop
  runs past the last slot (assertion in page_dir_get_nth_slot(), garbage
  in a release build). Upstream reassigns rec itself. Used by
  BtreeCursor::add_path_info() for estimate_n_rows_in_range(). */
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 50; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
  }

  auto v = recs(page);

  for (ulint i{}; i < v.size(); ++i) {
    EXPECT_EQ(page_rec_get_n_recs_before(v[i]), i + 1);
  }

  EXPECT_EQ(page_rec_get_n_recs_before(page_get_supremum_rec(page)), 51UL);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The same for the records that own a slot only, which works: the
infimum has 0 before it, the first user record 1, the supremum n + 1. */
TEST_F(PageTest, NRecsBeforeOwners) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 50; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
  }

  auto v = recs(page);

  for (ulint i{}; i < v.size(); ++i) {
    if (v[i].get_n_owned() > 0) {
      EXPECT_EQ(page_rec_get_n_recs_before(v[i]), i + 1);
    }
  }

  EXPECT_EQ(page_rec_get_n_recs_before(page_get_infimum_rec(page)), 0UL);
  EXPECT_EQ(page_rec_get_n_recs_before(page_get_supremum_rec(page)), 51UL);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The middle record has (n_recs + 2) / 2 - 1 user records before it. */
TEST_F(PageTest, MiddleRec) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 101; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());

    auto v = recs(page);
    auto mid = page_get_middle_rec(page);
    const auto pos = (v.size() + 2) / 2 - 1;

    if (pos < v.size()) {
      ASSERT_EQ(mid, v[pos]) << v.size();
    } else {
      ASSERT_TRUE(page_rec_is_supremum(mid)) << v.size();
    }
  }

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** page_find_rec_with_heap_no() finds every record, and not a free one. */
TEST_F(PageTest, FindHeapNo) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 30; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(29 - i, 0)).is_null());
  }

  for (auto rec : recs(page)) {
    EXPECT_EQ(page_find_rec_with_heap_no(page, rec.get_heap_no()), rec);
  }

  EXPECT_EQ(page_find_rec_with_heap_no(page, PAGE_HEAP_NO_INFIMUM), page_get_infimum_rec(page));

  auto victim = recs(page)[10];
  const auto heap_no = victim.get_heap_no();

  remove(mtr, block, m_sec, victim);
  EXPECT_TRUE(page_find_rec_with_heap_no(page, heap_no).is_null());

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Reorganize compacts the page: no garbage, no free list, the heap top
lowered by the garbage, the same records in the same order, heap numbers
renumbered in order. */
TEST_F(PageTest, Reorganize) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_clust);
  auto page = block->get_frame();
  std::mt19937 rng(3);

  for (uint32_t k = 0; k < 120; ++k) {
    ASSERT_FALSE(insert(mtr, block, m_clust, clust_entry(uint32_t(rng() % 100000), 1 + rng() % 60)).is_null());
  }

  for (int i = 0; i < 50; ++i) {
    auto v = recs(page);

    remove(mtr, block, m_clust, v[rng() % v.size()]);
  }

  const auto before = keys(page);
  const auto data = page_get_data_size(page);
  const auto garbage = page_header_get_field(page, PAGE_GARBAGE);
  const auto heap_top = page_header_get_field(page, PAGE_HEAP_TOP);
  const auto max_ins = page_get_max_insert_size_after_reorganize(page, 1);

  ASSERT_GT(garbage, 0UL);
  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_clust, &mtr));

  EXPECT_EQ(keys(page), before);
  EXPECT_EQ(page_get_data_size(page), data);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_FREE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), heap_top - garbage);
  EXPECT_EQ(page_get_max_insert_size(page, 1), max_ins);
  EXPECT_EQ(page_dir_get_n_heap(page), PAGE_HEAP_NO_USER_LOW + before.size());

  ulint heap_no = PAGE_HEAP_NO_USER_LOW;

  for (auto rec : recs(page)) {
    EXPECT_EQ(rec.get_heap_no(), heap_no++);
  }

  validate(block, m_clust);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** Copy the end of a list to an empty page and the start to another: the
source is unchanged, the copies hold the records in order. */
TEST_F(PageTest, CopyLists) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 200; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, i)).is_null());
  }

  const auto all = keys(page);
  auto split = recs(page)[120];

  auto end_block = alloc(mtr, m_sec);
  auto start_block = alloc(mtr, m_sec);

  (void)page_copy_rec_list_end(*m_sys, end_block, block, split, m_sec, &mtr);
  (void)page_copy_rec_list_start(*m_sys, start_block, block, split, m_sec, &mtr);

  EXPECT_EQ(keys(page), all);
  EXPECT_EQ(keys(end_block->get_frame()), std::vector<uint32_t>(all.begin() + 120, all.end()));
  EXPECT_EQ(keys(start_block->get_frame()), std::vector<uint32_t>(all.begin(), all.begin() + 120));

  validate(block, m_sec);
  validate(end_block, m_sec);
  validate(start_block, m_sec);

  /* Copying the list end to a page that has records puts them first:
  page_copy_rec_list_end_no_locks() inserts after the infimum. */
  auto mixed = alloc(mtr, m_sec);

  ASSERT_FALSE(insert(mtr, mixed, m_sec, sec_entry(1000, 0)).is_null());
  (void)page_copy_rec_list_end(*m_sys, mixed, block, recs(page)[190], m_sec, &mtr);

  std::vector<uint32_t> expected(all.begin() + 190, all.end());

  expected.push_back(1000);
  EXPECT_EQ(keys(mixed->get_frame()), expected);
  validate(mixed, m_sec);

  free_page(mtr, m_sec, mixed);
  free_page(mtr, m_sec, start_block);
  free_page(mtr, m_sec, end_block);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Move the end and the start of a list: the records leave the source,
the garbage of the source accounts for them. */
TEST_F(PageTest, MoveLists) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_clust);
  auto page = block->get_frame();

  for (uint32_t k = 0; k < 100; ++k) {
    ASSERT_FALSE(insert(mtr, block, m_clust, clust_entry(k, 30)).is_null());
  }

  const auto all = keys(page);
  const auto data = page_get_data_size(page);
  auto right = alloc(mtr, m_clust);

  ASSERT_TRUE(page_move_rec_list_end(*m_sys, right, block, recs(page)[60], m_clust, &mtr));

  EXPECT_EQ(keys(page), std::vector<uint32_t>(all.begin(), all.begin() + 60));
  EXPECT_EQ(keys(right->get_frame()), std::vector<uint32_t>(all.begin() + 60, all.end()));
  EXPECT_EQ(page_get_data_size(page) + page_get_data_size(right->get_frame()), data);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), page_get_data_size(right->get_frame()));
  validate(block, m_clust);
  validate(right, m_clust);

  auto left = alloc(mtr, m_clust);

  ASSERT_TRUE(page_move_rec_list_start(*m_sys, left, block, recs(page)[20], m_clust, &mtr));

  EXPECT_EQ(keys(left->get_frame()), std::vector<uint32_t>(all.begin(), all.begin() + 20));
  EXPECT_EQ(keys(page), std::vector<uint32_t>(all.begin() + 20, all.begin() + 60));
  validate(block, m_clust);
  validate(left, m_clust);

  free_page(mtr, m_clust, left);
  free_page(mtr, m_clust, right);
  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** Delete the end of the list from every position, with and without the
size and count given; and the start. */
TEST_F(PageTest, DeleteLists) {
  for (ulint split : {ulint(0), ulint(1), ulint(7), ulint(8), ulint(9), ulint(33), ulint(63)}) {
    for (bool given : {false, true}) {
      Mtr mtr;

      mtr.start();

      auto block = alloc(mtr, m_sec);
      auto page = block->get_frame();

      for (uint32_t i = 0; i < 64; ++i) {
        ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
      }

      auto v = recs(page);
      ulint size{};

      for (auto i = split; i < v.size(); ++i) {
        size += offsets(m_sec, v[i]).size();
      }

      page_delete_rec_list_end(v[split], block, m_sec, given ? 64 - split : ULINT_UNDEFINED, given ? size : ULINT_UNDEFINED, &mtr);

      std::vector<uint32_t> expected(split);

      std::iota(expected.begin(), expected.end(), 0);
      EXPECT_EQ(keys(page), expected) << split;
      EXPECT_EQ(page_get_n_recs(page), split);
      EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), size);
      validate(block, m_sec);

      /* The free records can be reused. */
      ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(100, 0)).is_null());
      validate(block, m_sec);

      free_page(mtr, m_sec, block);
      mtr.commit(*m_sys);
    }
  }

  for (ulint split : {ulint(0), ulint(1), ulint(9), ulint(40), ulint(63)}) {
    Mtr mtr;

    mtr.start();

    auto block = alloc(mtr, m_sec);
    auto page = block->get_frame();

    for (uint32_t i = 0; i < 64; ++i) {
      ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
    }

    page_delete_rec_list_start(*m_sys, recs(page)[split], block, m_sec, &mtr);

    std::vector<uint32_t> expected(64 - split);

    std::iota(expected.begin(), expected.end(), uint32_t(split));
    EXPECT_EQ(keys(page), expected) << split;
    validate(block, m_sec);

    free_page(mtr, m_sec, block);
    mtr.commit(*m_sys);
  }
}

/** page_validate() catches records out of order. */
TEST_F(PageTest, ValidateCatchesDisorder) {
  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  for (uint32_t i = 0; i < 10; ++i) {
    ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
  }

  ASSERT_TRUE(page_validate(page, m_sec));

  /* Overwrite the key of the 5th record with a bigger one. */
  ulint len;
  auto f = recs(page)[4].get_nth_field(0, &len);

  mach_write_to_4(f, 100);
  EXPECT_FALSE(page_validate(page, m_sec));

  mach_write_to_4(f, 4);
  EXPECT_TRUE(page_validate(page, m_sec));

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The max trx id of a secondary index leaf page only grows. */
TEST_F(PageTest, MaxTrxId) {
  Mtr mtr;

  mtr.start();

  m_max_trx_id = false;

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  EXPECT_EQ(page_get_max_trx_id(page), 0UL);
  page_update_max_trx_id(*m_sys, block, 100, &mtr);
  EXPECT_EQ(page_get_max_trx_id(page), 100UL);
  page_update_max_trx_id(*m_sys, block, 50, &mtr);
  EXPECT_EQ(page_get_max_trx_id(page), 100UL);
  page_update_max_trx_id(*m_sys, block, 101, &mtr);
  EXPECT_EQ(page_get_max_trx_id(page), 101UL);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

using PagePerf = PageTest;

/** Inserts into a page in sequential and random order, and searches. */
TEST_F(PagePerf, InsertSearch) {
  constexpr int ROUNDS = 400;
  constexpr uint32_t N = 400;
  std::vector<DTuple *> tuples;
  std::mt19937 rng(9);

  for (uint32_t i = 0; i < N; ++i) {
    tuples.push_back(sec_entry(i, i));
  }

  auto shuffled = tuples;

  std::shuffle(shuffled.begin(), shuffled.end(), rng);

  for (const auto *order : {"seq", "rnd"}) {
    const auto &ts = order[0] == 's' ? tuples : shuffled;
    double secs{};
    uint64_t ops{};

    for (int r = 0; r < ROUNDS; ++r) {
      Mtr mtr;

      mtr.start();

      auto block = alloc(mtr, m_sec);
      const auto start = std::chrono::steady_clock::now();

      for (auto t : ts) {
        PageCursor cur;

        (void)cur.search(*m_sys, block, m_sec, t, PAGE_CUR_LE);
        ut::keep(cur.tuple_insert(t, m_sec, 0, &mtr));
      }

      secs += std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      ops += ts.size();

      free_page(mtr, m_sec, block);
      mtr.commit(*m_sys);
    }

    ut::BenchResult res{std::string("page_insert_") + order, 1, ops, secs};

    ut::report(res);
    EXPECT_LT(res.ns_per_op(), 200000.0);
  }

  Mtr mtr;

  mtr.start();

  auto block = alloc(mtr, m_sec);

  for (auto t : tuples) {
    ASSERT_FALSE(insert(mtr, block, m_sec, t).is_null());
  }

  auto r = ut::bench("page_search", 1000000, [&](uint64_t i) {
    PageCursor cur;

    ut::keep(cur.search(*m_sys, block, m_sec, shuffled[i % N], PAGE_CUR_LE));
  });

  EXPECT_LT(r.ns_per_op(), 100000.0);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

}  // namespace
