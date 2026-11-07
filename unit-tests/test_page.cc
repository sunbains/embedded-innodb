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
bounds, reorganize, and copying, moving and deleting lists of records, and the
high key of a page.
page_validate() and the directory invariants are checked after each step. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "buf0buf.h"
#include "data0data.h"
#include "dict0dict.h"
#include "fsp0types.h"
#include "log0log.h"
#include "log0recv.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "rem0rec.h"

#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
    m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

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
      m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
    m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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

/* --- The high key ---------------------------------------------------------
PAGE_HIGH_KEY: a record in the heap that no list has, kept by everything that
rewrites a page, in the data size, and redo logged. */

/** A record made to be a high key: its origin and size with extra bytes. */
struct KeyRec {
  Rec m_rec;
  ulint m_size{};

  /** @return its bytes, from the extra bytes on. */
  [[nodiscard]] std::vector<byte> bytes() const {
    const auto start = m_rec.get() - page_rec_extra_size(m_rec);

    return {start, start + m_size};
  }
};

class HighKeyTest : public PageTest {
 protected:
  /** @return t as a physical record of index, in m_heap. */
  KeyRec key_of(Index *index, const DTuple *t) {
    const auto size = rec_get_converted_size(index, t, 0);
    auto buf = static_cast<byte *>(mem_heap_alloc(m_heap, size));

    return {Rec::convert_dtuple_to_rec(buf, index, t, 0), size};
  }

  /** @return the key (a, k) of SEC_A. */
  KeyRec sec_key(uint32_t a, uint32_t k = 0) { return key_of(m_sec, sec_entry(a, k)); }

  /** @return a new page of index with a right sibling, which a high key needs. */
  BufBlock *linked(Mtr &mtr, Index *index) {
    auto block = alloc(mtr, index);

    Btree::page_set_next(block->get_frame(), block->get_page_no() + 1, &mtr);

    return block;
  }

  /** Set the key, it must fit. */
  void set(Mtr &mtr, BufBlock *block, const KeyRec &key) { ASSERT_TRUE(page_set_high_key(block, key.m_rec, key.m_size, &mtr)); }

  /** The page has exactly this key. */
  void expect_key(const page_t *page, const KeyRec &key) {
    const auto rec = page_get_high_key(page);

    ASSERT_FALSE(rec.is_null());
    ASSERT_EQ(page_high_key_size(rec), key.m_size);
    EXPECT_EQ((KeyRec{rec, key.m_size}.bytes()), key.bytes());
  }

  /** Insert n records (i, 0) of SEC_A from first. */
  void fill(Mtr &mtr, BufBlock *block, uint32_t first, uint32_t n) {
    for (auto i = first; i < first + n; ++i) {
      ASSERT_FALSE(insert(mtr, block, m_sec, sec_entry(i, 0)).is_null());
    }
  }

  /** @return the redo log of mtr so far. */
  static std::vector<byte> log_of(const Mtr &mtr) {
    std::vector<byte> v;

    for (auto b = mtr.m_log.get_first_block(); b != nullptr; b = mtr.m_log.get_next_block(b)) {
      v.insert(v.end(), mtr.m_log.get_data(b), mtr.m_log.get_data(b) + mtr.m_log.get_used(b));
    }

    return v;
  }

  /** @return the types of the redo records of mtr from pos, only parsing them. */
  std::vector<MlogType> types(const Mtr &mtr, ulint pos) {
    auto log = log_of(mtr);
    std::vector<MlogType> v;
    auto end = log.data() + log.size();

    for (auto ptr = log.data() + pos; ptr < end;) {
      MlogType type{};
      space_id_t space{};
      page_no_t page_no{};

      ptr = mlog_parse_initial_log_record(ptr, end, &type, &space, &page_no);
      EXPECT_NE(ptr, nullptr);
      ptr = recv_parse_or_apply_log_rec_body(*m_sys, type, ptr, end, nullptr, nullptr);
      EXPECT_NE(ptr, nullptr);

      if (ptr == nullptr) {
        break;
      }

      v.push_back(type);
    }

    return v;
  }

  /** @return a page with the contents of page, for a replay on it. Both are in mtr. */
  BufBlock *twin(Mtr &mtr, Index *index, const page_t *page) {
    auto block = alloc(mtr, index);
    auto frame = block->get_frame();

    memcpy(frame + PAGE_HEADER, page + PAGE_HEADER, UNIV_PAGE_SIZE - PAGE_HEADER);
    memcpy(frame + FIL_PAGE_NEXT, page + FIL_PAGE_NEXT, 4);
    memcpy(frame + FIL_PAGE_PREV, page + FIL_PAGE_PREV, 4);

    return block;
  }

  /** Apply the redo records of mtr from pos to block as recovery does: logging off. */
  void replay(const Mtr &mtr, ulint pos, BufBlock *block) {
    auto log = log_of(mtr);
    auto end = log.data() + log.size();
    Mtr rm;

    rm.start();
    (void)rm.set_log_mode(MTR_LOG_NONE);
    rm.memo_push(block, MTR_MEMO_PAGE_X_FIX);

    for (auto ptr = log.data() + pos; ptr < end;) {
      MlogType type{};
      space_id_t space{};
      page_no_t page_no{};

      ptr = mlog_parse_initial_log_record(ptr, end, &type, &space, &page_no);
      ASSERT_NE(ptr, nullptr);
      ptr = recv_parse_or_apply_log_rec_body(*m_sys, type, ptr, end, block, &rm);
      ASSERT_NE(ptr, nullptr);
    }

    /* The block stays latched by its own mtr. */
    rm.m_memo.clear();
    rm.m_modifications = false;
    rm.commit(*m_sys);
  }

  /** The page and its replay have the same bytes, the page's own id aside. */
  static void expect_same(const page_t *a, const page_t *b) {
    EXPECT_EQ(memcmp(a + PAGE_HEADER, b + PAGE_HEADER, UNIV_PAGE_SIZE - PAGE_HEADER - FIL_PAGE_DATA_END), 0);
    EXPECT_EQ(memcmp(a + FIL_PAGE_NEXT, b + FIL_PAGE_NEXT, 4), 0);
  }

  /** Run ops in a mtr on a linked page of SEC_A, with a replay of what they log on a twin
  taken before: same bytes, and the page validates. ops may return its records' types. */
  void check_replay(const std::function<void(Mtr &, BufBlock *)> &setup, const std::function<void(Mtr &, BufBlock *)> &ops) {
    Mtr mtr;

    mtr.start();
    m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

    auto block = linked(mtr, m_sec);

    setup(mtr, block);

    auto other = twin(mtr, m_sec, block->get_frame());
    const auto pos = log_of(mtr).size();

    ops(mtr, block);
    replay(mtr, pos, other);

    expect_same(block->get_frame(), other->get_frame());
    validate(block, m_sec);
    validate(other, m_sec);

    free_page(mtr, m_sec, other);
    free_page(mtr, m_sec, block);
    mtr.commit(*m_sys);
  }

  /** The pointer's bytes, read as raw. */
  static ulint raw_field(const page_t *page) { return mach_read_from_2(page + PAGE_HEADER + PAGE_HIGH_KEY); }
};

/** No key by default, on a page linked or not, and the field reads 0 on all: page_create()
and the allocation left it so. */
TEST_F(HighKeyTest, None) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(raw_field(page), 0UL);

  Btree::page_set_next(page, FIL_NULL, &mtr);
  EXPECT_TRUE(page_get_high_key(page).is_null());

  Btree::page_set_next(page, 7, &mtr);
  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(raw_field(page), 0UL);
  EXPECT_EQ(page_get_data_size(page), 0UL);
  EXPECT_EQ(page_get_rec_data_size(page), 0UL);

  /* page_create() doesn't invent one, and reads 0 on a page that was a sibling. */
  m_sys->m_btree->page_create(block, m_sec, 0, &mtr);
  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(raw_field(page), 0UL);

  validate(block, m_sec);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** A freed page with a key, which comes back from its segment, has no key: the page
init is what makes every non-root page read 0, a former root's too. */
TEST_F(HighKeyTest, Realloc) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);

  set(mtr, block, sec_key(5));
  free_page(mtr, m_sec, block);

  auto again = alloc(mtr, m_sec);

  /* The segment hands this page or another out: the init zeroes either. */
  EXPECT_EQ(raw_field(again->get_frame()), 0UL);
  EXPECT_TRUE(page_get_high_key(again->get_frame()).is_null());

  free_page(mtr, m_sec, again);
  mtr.commit(*m_sys);
}

/** Set and get: the bytes are copied to the heap top, no heap number, not in a list. */
TEST_F(HighKeyTest, SetGet) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(77, 3);
  const auto top = page_header_get_field(page, PAGE_HEAP_TOP);

  set(mtr, block, key);

  expect_key(page, key);

  const auto rec = page_get_high_key(page);

  EXPECT_EQ(page_offset(rec), top + page_rec_extra_size(rec));
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), top + key.m_size);
  EXPECT_EQ(page_dir_get_n_heap(page), PAGE_HEAP_NO_USER_LOW);
  EXPECT_EQ(page_get_n_recs(page), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_FREE), 0UL);
  EXPECT_EQ(page_dir_get_n_slots(page), 2UL);

  /* It's a record of the format of the page: its fields read back. */
  ulint len{};

  EXPECT_EQ(mach_read_from_4(rec.get_nth_field(0, &len)), 77UL);
  EXPECT_EQ(len, 4UL);
  EXPECT_EQ(mach_read_from_4(rec.get_nth_field(1, &len)), 3UL);

  validate(block, m_sec);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** A key over the limit of 1 byte offsets: the size and the origin come from the record. */
TEST_F(HighKeyTest, Long) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

  auto block = linked(mtr, m_clust);
  auto page = block->get_frame();
  const auto key = key_of(m_clust, clust_entry(9, 2000));

  ASSERT_FALSE(key.m_rec.get_1byte_offs_flag());
  set(mtr, block, key);
  expect_key(page, key);
  EXPECT_EQ(page_get_data_size(page), key.m_size);

  ASSERT_FALSE(insert(mtr, block, m_clust, clust_entry(1, 10)).is_null());
  validate(block, m_clust);

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_clust, &mtr));
  expect_key(page, key);
  validate(block, m_clust);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** Inserts and deletes around the key don't touch it, also when the free list is reused. */
TEST_F(HighKeyTest, InsDel) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  fill(mtr, block, 0, 10);
  set(mtr, block, key);
  fill(mtr, block, 10, 30);
  expect_key(page, key);
  validate(block, m_sec);

  for (int i = 0; i < 15; ++i) {
    remove(mtr, block, m_sec, recs(page)[i]);
  }

  expect_key(page, key);
  validate(block, m_sec);

  /* The size of the records is the same: these reuse the free list. */
  fill(mtr, block, 100, 15);
  expect_key(page, key);
  EXPECT_EQ(page_get_n_recs(page), 40UL);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** A reorganize keeps the key, removes the garbage, heap numbers run from the first user one. */
TEST_F(HighKeyTest, Reorg) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  fill(mtr, block, 0, 40);
  set(mtr, block, key);

  for (int i = 0; i < 20; ++i) {
    remove(mtr, block, m_sec, recs(page)[i]);
  }

  const auto all = keys(page);
  const auto data = page_get_data_size(page);
  const auto top = page_header_get_field(page, PAGE_HEAP_TOP);
  const auto garbage = page_header_get_field(page, PAGE_GARBAGE);
  const auto max_ins = page_get_max_insert_size_after_reorganize(page, 1);

  ASSERT_GT(garbage, 0UL);
  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));

  expect_key(page, key);
  EXPECT_EQ(keys(page), all);
  EXPECT_EQ(page_get_data_size(page), data);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), top - garbage);
  EXPECT_EQ(page_get_max_insert_size(page, 1), max_ins);
  EXPECT_EQ(page_dir_get_n_heap(page), PAGE_HEAP_NO_USER_LOW + all.size());

  ulint heap_no{PAGE_HEAP_NO_USER_LOW};

  for (auto rec : recs(page)) {
    EXPECT_EQ(rec.get_heap_no(), heap_no++);
  }

  validate(block, m_sec);

  /* Records go above the key after that. */
  fill(mtr, block, 2000, 5);
  expect_key(page, key);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Moving the end and the start of the list out: the key stays with the page, the
destination gets none. */
TEST_F(HighKeyTest, MoveOut) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  fill(mtr, block, 0, 60);
  set(mtr, block, key);

  const auto data = page_get_rec_data_size(page);
  auto right = linked(mtr, m_sec);

  ASSERT_TRUE(page_move_rec_list_end(*m_sys, right, block, recs(page)[40], m_sec, &mtr));

  expect_key(page, key);
  EXPECT_TRUE(page_get_high_key(right->get_frame()).is_null());
  EXPECT_EQ(page_get_n_recs(page), 40UL);
  /* The size given to the delete of the moved records excludes the key. */
  EXPECT_EQ(page_get_rec_data_size(page) + page_get_data_size(right->get_frame()), data);
  validate(block, m_sec);
  validate(right, m_sec);

  auto left = linked(mtr, m_sec);

  ASSERT_TRUE(page_move_rec_list_start(*m_sys, left, block, recs(page)[10], m_sec, &mtr));

  expect_key(page, key);
  EXPECT_TRUE(page_get_high_key(left->get_frame()).is_null());
  EXPECT_EQ(page_get_n_recs(page), 30UL);
  validate(block, m_sec);
  validate(left, m_sec);

  free_page(mtr, m_sec, left);
  free_page(mtr, m_sec, right);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** A destination that has a key before the copy (a split could do that): its heap isn't
written over by the path for an unused heap, also if the key was cleared again. */
TEST_F(HighKeyTest, CopyIn) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);

  fill(mtr, block, 0, 30);

  auto to = linked(mtr, m_sec);
  auto to_page = to->get_frame();
  const auto key = sec_key(1000);

  set(mtr, to, key);

  (void)page_copy_rec_list_end(*m_sys, to, block, recs(block->get_frame())[10], m_sec, &mtr);

  expect_key(to_page, key);
  EXPECT_EQ(page_get_n_recs(to_page), 20UL);
  validate(to, m_sec);

  auto gone = linked(mtr, m_sec);

  set(mtr, gone, sec_key(2000));
  page_clear_high_key(gone, &mtr);

  const auto top = page_header_get_field(gone->get_frame(), PAGE_HEAP_TOP);

  ASSERT_GT(top, PAGE_SUPREMUM_END);
  (void)page_copy_rec_list_end(*m_sys, gone, block, recs(block->get_frame())[10], m_sec, &mtr);

  EXPECT_EQ(page_get_n_recs(gone->get_frame()), 20UL);
  EXPECT_GT(page_header_get_field(gone->get_frame(), PAGE_HEAP_TOP), top);
  validate(gone, m_sec);

  free_page(mtr, m_sec, gone);
  free_page(mtr, m_sec, to);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Emptied in place by a list delete: the key and nothing else is the data. */
TEST_F(HighKeyTest, DelLists) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  fill(mtr, block, 0, 30);
  set(mtr, block, key);

  page_delete_rec_list_start(*m_sys, recs(page)[10], block, m_sec, &mtr);
  expect_key(page, key);
  EXPECT_EQ(page_get_n_recs(page), 20UL);
  validate(block, m_sec);

  page_delete_rec_list_end(recs(page)[0], block, m_sec, ULINT_UNDEFINED, ULINT_UNDEFINED, &mtr);

  expect_key(page, key);
  EXPECT_EQ(page_get_n_recs(page), 0UL);
  EXPECT_EQ(page_get_data_size(page), key.m_size);
  EXPECT_EQ(page_get_rec_data_size(page), 0UL);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Btree::page_empty() recreates the page and keeps the key, as the only thing in the heap. */
TEST_F(HighKeyTest, Empty) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  fill(mtr, block, 0, 30);
  set(mtr, block, key);
  remove(mtr, block, m_sec, recs(page)[3]);

  m_sys->m_btree->page_empty(block, m_sec, 0, &mtr);

  expect_key(page, key);
  EXPECT_EQ(page_get_n_recs(page), 0UL);
  EXPECT_EQ(page_dir_get_n_heap(page), PAGE_HEAP_NO_USER_LOW);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), PAGE_SUPREMUM_END + key.m_size);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_get_data_size(page), key.m_size);
  validate(block, m_sec);

  /* It's usable as a page. */
  fill(mtr, block, 0, 10);
  expect_key(page, key);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** page_create() itself drops the key: there's no heap to keep it in. Who wants it keeps it. */
TEST_F(HighKeyTest, CreateDrops) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();

  set(mtr, block, sec_key(1000));
  m_sys->m_btree->page_create(block, m_sec, 0, &mtr);

  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(raw_field(page), 0UL);
  EXPECT_EQ(page_get_data_size(page), 0UL);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The data size counts the key and the free space shrinks by exactly its size, on an
empty page and one with records. */
TEST_F(HighKeyTest, Sizes) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  for (uint32_t n : {uint32_t(0), uint32_t(25)}) {
    fill(mtr, block, 0, n);

    const auto data = page_get_data_size(page);
    const auto max_ins = page_get_max_insert_size(page, 1);
    const auto max_reorg = page_get_max_insert_size_after_reorganize(page, 1);

    set(mtr, block, key);

    EXPECT_EQ(page_get_data_size(page), data + key.m_size);
    EXPECT_EQ(page_get_rec_data_size(page), data);
    EXPECT_EQ(page_get_rec_data_size(page), data_size(m_sec, page));
    EXPECT_EQ(page_get_max_insert_size(page, 1), max_ins - key.m_size);
    EXPECT_EQ(page_get_max_insert_size_after_reorganize(page, 1), max_reorg - key.m_size);
    validate(block, m_sec);

    page_clear_high_key(block, &mtr);
    ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  }

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Fill a page that has a key: it fits as many bytes as a page without one, less the key;
the accounting adds up at the end, and the directory and heap don't touch. */
TEST_F(HighKeyTest, Fill) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto plain = linked(mtr, m_sec);
  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  set(mtr, block, key);

  uint32_t n{};
  uint32_t n_plain{};

  while (!insert(mtr, block, m_sec, sec_entry(n, 0)).is_null()) {
    ++n;
  }

  while (!insert(mtr, plain, m_sec, sec_entry(n_plain, 0)).is_null()) {
    ++n_plain;
  }

  expect_key(page, key);
  EXPECT_LE(n, n_plain);
  EXPECT_GE(n + 2, n_plain);
  EXPECT_EQ(page_get_n_recs(page), n);
  EXPECT_EQ(page_get_data_size(page), data_size(m_sec, page) + key.m_size);
  EXPECT_LE(page_header_get_field(page, PAGE_HEAP_TOP), page_offset(page_dir_get_nth_slot(page, page_dir_get_n_slots(page) - 1)));
  validate(block, m_sec);

  /* Reorganizing a full page has to succeed with the key too. */
  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  expect_key(page, key);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  free_page(mtr, m_sec, plain);
  mtr.commit(*m_sys);
}

/** A page walk, the directory and the heap numbers are those of a page without a key,
whenever the key was set. */
TEST_F(HighKeyTest, Invisible) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto plain = linked(mtr, m_sec);
  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();

  fill(mtr, plain, 0, 40);
  fill(mtr, block, 0, 20);
  set(mtr, block, sec_key(1000));
  fill(mtr, block, 20, 20);

  const auto a = recs(page);
  const auto b = recs(plain->get_frame());

  ASSERT_EQ(a.size(), b.size());

  for (size_t i{}; i < a.size(); ++i) {
    EXPECT_EQ(a[i].get_heap_no(), b[i].get_heap_no()) << i;
    EXPECT_NE(a[i], page_get_high_key(page)) << i;
  }

  EXPECT_EQ(keys(page), keys(plain->get_frame()));
  EXPECT_EQ(page_dir_get_n_heap(page), page_dir_get_n_heap(plain->get_frame()));
  EXPECT_EQ(page_dir_get_n_slots(page), page_dir_get_n_slots(plain->get_frame()));
  /* A heap number lookup goes by the list: none is the key's. */
  EXPECT_TRUE(page_find_rec_with_heap_no(page, page_dir_get_n_heap(page)).is_null());
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  free_page(mtr, m_sec, plain);
  mtr.commit(*m_sys);
}

/** No room: false, the page is byte for byte the same and nothing is logged, for a page
with a key as well: the old one stays. */
TEST_F(HighKeyTest, NoRoom) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

  auto block = linked(mtr, m_clust);
  auto page = block->get_frame();
  const auto key = key_of(m_clust, clust_entry(9, 8));

  uint32_t k{};

  while (!insert(mtr, block, m_clust, clust_entry(k, 300)).is_null()) {
    ++k;
  }

  const auto too_big = key_of(m_clust, clust_entry(1, 2000));

  ASSERT_LT(page_get_max_insert_size(page, 0), too_big.m_size);

  for (int i = 0; i < 2; ++i) {
    std::vector<byte> before(page, page + UNIV_PAGE_SIZE);
    const auto log = log_of(mtr).size();

    EXPECT_FALSE(page_set_high_key(block, too_big.m_rec, too_big.m_size, &mtr));
    EXPECT_EQ(memcmp(before.data(), page, UNIV_PAGE_SIZE), 0);
    EXPECT_EQ(log_of(mtr).size(), log);

    if (i == 0) {
      if (page_get_max_insert_size(page, 0) < key.m_size) {
        remove(mtr, block, m_clust, recs(page)[0]);
        ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_clust, &mtr));
      }

      set(mtr, block, key);
    }
  }

  expect_key(page, key);
  validate(block, m_clust);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** A key fits to the last byte the free space reports, and not a byte over. */
TEST_F(HighKeyTest, Exact) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

  auto block = linked(mtr, m_clust);
  auto page = block->get_frame();

  /* A key of the size of all the room but one byte, then of all the room. */
  auto extra_of = [&](ulint blen) { return key_of(m_clust, clust_entry(1, blen)).m_size - blen; };
  const auto room = page_get_max_insert_size(page, 0);
  const auto over = key_of(m_clust, clust_entry(1, room + 1 - extra_of(1000)));

  ASSERT_EQ(over.m_size, room + 1);
  EXPECT_FALSE(page_set_high_key(block, over.m_rec, over.m_size, &mtr));
  EXPECT_TRUE(page_get_high_key(page).is_null());

  const auto fits = key_of(m_clust, clust_entry(1, room - extra_of(1000)));

  ASSERT_EQ(fits.m_size, room);
  set(mtr, block, fits);
  expect_key(page, fits);
  EXPECT_EQ(page_get_max_insert_size(page, 0), 0UL);
  validate(block, m_clust);

  free_page(mtr, m_clust, block);
  mtr.commit(*m_sys);
}

/** Replace: the old bytes are garbage, not on the free list, until the next reorganize. */
TEST_F(HighKeyTest, Replace) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto old_key = sec_key(1000);
  const auto new_key = sec_key(2000, 9);

  fill(mtr, block, 0, 20);
  set(mtr, block, old_key);

  const auto top = page_header_get_field(page, PAGE_HEAP_TOP);
  const auto data = page_get_data_size(page);

  set(mtr, block, new_key);

  expect_key(page, new_key);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), top + new_key.m_size);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), old_key.m_size);
  EXPECT_EQ(page_header_get_field(page, PAGE_FREE), 0UL);
  EXPECT_EQ(page_get_data_size(page), data - old_key.m_size + new_key.m_size);
  validate(block, m_sec);

  /* The garbage of the key doesn't feed the free list: a record goes on the heap top. */
  const auto n_heap = page_dir_get_n_heap(page);

  fill(mtr, block, 100, 1);
  EXPECT_EQ(page_dir_get_n_heap(page), n_heap + 1);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), old_key.m_size);

  /* The current key as the new one. */
  const KeyRec same{page_get_high_key(page), new_key.m_size};

  set(mtr, block, same);
  expect_key(page, new_key);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), old_key.m_size + new_key.m_size);
  validate(block, m_sec);

  const auto after = page_get_data_size(page);

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));

  expect_key(page, new_key);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_get_data_size(page), after);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), PAGE_SUPREMUM_END + after);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Clear: no key, its bytes are garbage until a reorganize; clearing nothing does nothing. */
TEST_F(HighKeyTest, Clear) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  const auto log0 = log_of(mtr).size();

  page_clear_high_key(block, &mtr);
  EXPECT_EQ(log_of(mtr).size(), log0);

  fill(mtr, block, 0, 20);
  set(mtr, block, key);

  const auto data = page_get_rec_data_size(page);

  page_clear_high_key(block, &mtr);

  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(raw_field(page), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), key.m_size);
  EXPECT_EQ(page_get_data_size(page), data);
  validate(block, m_sec);

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  EXPECT_EQ(page_header_get_field(page, PAGE_GARBAGE), 0UL);
  EXPECT_EQ(page_header_get_field(page, PAGE_HEAP_TOP), PAGE_SUPREMUM_END + data);
  EXPECT_TRUE(page_get_high_key(page).is_null());
  validate(block, m_sec);

  /* Cleared, so the rightmost link is allowed, and set again after a new sibling. */
  Btree::page_set_next(page, FIL_NULL, &mtr);
  Btree::page_set_next(page, 9, &mtr);
  set(mtr, block, key);
  expect_key(page, key);
  page_clear_high_key(block, &mtr);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** What each operation logs. A reorganize logs its record alone, also with a key: the
replay runs it. Without a key nothing of this changes the stream of the existing tree. */
TEST_F(HighKeyTest, Redo) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();

  fill(mtr, block, 0, 10);

  auto pos = log_of(mtr).size();

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  EXPECT_EQ(types(mtr, pos), std::vector<MlogType>{MLOG_PAGE_REORGANIZE});

  pos = log_of(mtr).size();
  m_sys->m_btree->page_empty(block, m_sec, 0, &mtr);
  EXPECT_EQ(types(mtr, pos), (std::vector<MlogType>{MLOG_PAGE_CREATE, MLOG_2BYTES}));
  EXPECT_EQ(raw_field(page), 0UL);

  /* Emptying resets it, as for a new page; a reorganize wants one on a secondary leaf. */
  page_set_max_trx_id(block, 1, &mtr);

  pos = log_of(mtr).size();
  set(mtr, block, sec_key(1000));
  EXPECT_EQ(types(mtr, pos), std::vector<MlogType>{MLOG_PAGE_HIGH_KEY});

  pos = log_of(mtr).size();
  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  EXPECT_EQ(types(mtr, pos), std::vector<MlogType>{MLOG_PAGE_REORGANIZE});

  pos = log_of(mtr).size();
  m_sys->m_btree->page_empty(block, m_sec, 0, &mtr);
  EXPECT_EQ(types(mtr, pos), (std::vector<MlogType>{MLOG_PAGE_CREATE, MLOG_PAGE_HIGH_KEY, MLOG_2BYTES}));

  pos = log_of(mtr).size();
  page_clear_high_key(block, &mtr);
  EXPECT_EQ(types(mtr, pos), std::vector<MlogType>{MLOG_PAGE_HIGH_KEY});

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The bytes of the redo record: the page's initial record, the length, the origin, the key. */
TEST_F(HighKeyTest, RedoBody) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  const auto key = sec_key(1000);
  const auto pos = log_of(mtr).size();

  set(mtr, block, key);

  auto log = log_of(mtr);
  MlogType type{};
  space_id_t space{};
  page_no_t page_no{};
  auto ptr = mlog_parse_initial_log_record(log.data() + pos, log.data() + log.size(), &type, &space, &page_no);

  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(type, MLOG_PAGE_HIGH_KEY);
  EXPECT_EQ(space, block->get_space());
  EXPECT_EQ(page_no, block->get_page_no());
  EXPECT_EQ(mach_read_from_2(ptr), 2 + key.m_size);
  EXPECT_EQ(mach_read_from_2(ptr + 2), page_rec_extra_size(key.m_rec));
  EXPECT_EQ(std::vector<byte>(ptr + 4, log.data() + log.size()), key.bytes());

  page_clear_high_key(block, &mtr);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** Each operation's redo applied to a copy of the page before it gives the same bytes. */
TEST_F(HighKeyTest, ReplaySet) {
  const auto key = sec_key(1000);

  check_replay([&](Mtr &, BufBlock *) {}, [&](Mtr &mtr, BufBlock *b) { set(mtr, b, key); });
  check_replay([&](Mtr &mtr, BufBlock *b) { fill(mtr, b, 0, 20); }, [&](Mtr &mtr, BufBlock *b) { set(mtr, b, key); });
}

TEST_F(HighKeyTest, ReplayReplace) {
  const auto key = sec_key(1000);
  const auto key2 = sec_key(2000, 5);

  check_replay([&](Mtr &mtr, BufBlock *b) { fill(mtr, b, 0, 20); set(mtr, b, key); }, [&](Mtr &mtr, BufBlock *b) { set(mtr, b, key2); });
}

TEST_F(HighKeyTest, ReplayClear) {
  const auto key = sec_key(1000);

  check_replay([&](Mtr &mtr, BufBlock *b) { fill(mtr, b, 0, 20); set(mtr, b, key); }, [&](Mtr &mtr, BufBlock *b) { page_clear_high_key(b, &mtr); });
}

/** MLOG_PAGE_REORGANIZE runs the reorganize on the twin: the key comes from its own bytes. */
TEST_F(HighKeyTest, ReplayReorg) {
  const auto key = sec_key(1000);
  const auto key2 = sec_key(2000, 5);

  check_replay(
    [&](Mtr &mtr, BufBlock *b) {
      fill(mtr, b, 0, 30);
      set(mtr, b, key);
      set(mtr, b, key2);

      for (int i = 0; i < 10; ++i) {
        remove(mtr, b, m_sec, recs(b->get_frame())[i]);
      }
    },
    [&](Mtr &mtr, BufBlock *b) {
      ASSERT_TRUE(m_sys->m_btree->page_reorganize(b, m_sec, &mtr));
      expect_key(b->get_frame(), key2);
    }
  );
}

TEST_F(HighKeyTest, ReplayEmpty) {
  const auto key = sec_key(1000);

  check_replay(
    [&](Mtr &mtr, BufBlock *b) {
      fill(mtr, b, 0, 30);
      set(mtr, b, key);
    },
    [&](Mtr &mtr, BufBlock *b) {
      m_sys->m_btree->page_empty(b, m_sec, 0, &mtr);
      expect_key(b->get_frame(), key);
    }
  );
}

/** All of it in one stream, in between ordinary record operations. */
TEST_F(HighKeyTest, ReplayAll) {
  const auto key = sec_key(1000);
  const auto key2 = sec_key(2000, 5);

  check_replay(
    [&](Mtr &, BufBlock *) {},
    [&](Mtr &mtr, BufBlock *b) {
      fill(mtr, b, 0, 10);
      set(mtr, b, key);
      fill(mtr, b, 10, 20);
      remove(mtr, b, m_sec, recs(b->get_frame())[5]);
      set(mtr, b, key2);
      ASSERT_TRUE(m_sys->m_btree->page_reorganize(b, m_sec, &mtr));
      fill(mtr, b, 100, 5);
      page_delete_rec_list_end(recs(b->get_frame())[20], b, m_sec, ULINT_UNDEFINED, ULINT_UNDEFINED, &mtr);
      m_sys->m_btree->page_empty(b, m_sec, 0, &mtr);
      page_set_max_trx_id(b, 1, &mtr);
      fill(mtr, b, 200, 5);
      page_clear_high_key(b, &mtr);
      ASSERT_TRUE(m_sys->m_btree->page_reorganize(b, m_sec, &mtr));
      EXPECT_TRUE(page_get_high_key(b->get_frame()).is_null());
    }
  );
}

/** Without a key, the replay of the ordinary operations gives identical bytes too. */
TEST_F(HighKeyTest, ReplayNoKey) {
  check_replay(
    [&](Mtr &mtr, BufBlock *b) { fill(mtr, b, 0, 30); },
    [&](Mtr &mtr, BufBlock *b) {
      ASSERT_TRUE(m_sys->m_btree->page_reorganize(b, m_sec, &mtr));
      m_sys->m_btree->page_empty(b, m_sec, 0, &mtr);
      EXPECT_EQ(raw_field(b->get_frame()), 0UL);
    }
  );
}

/** A page without a right sibling is what a root is here. Its 2 bytes are the top of the
space id of the segment header, nonzero for a space id over 65535: never read as a key,
not in the sizes, and kept by whatever rewrites the page. */
TEST_F(HighKeyTest, RootSeg) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();

  /* An fseg header as on a root: this shape is what Btree::create() leaves. */
  for (ulint i{}; i < FSEG_HEADER_SIZE * 2; ++i) {
    mlog_write_ulint(page + PAGE_HEADER + PAGE_BTR_SEG_LEAF + i, 0xA0 + i, MLOG_1BYTE, &mtr);
  }

  const std::vector<byte> seg(page + PAGE_HEADER + PAGE_BTR_SEG_LEAF, page + PAGE_HEADER + PAGE_BTR_SEG_LEAF + 2 * FSEG_HEADER_SIZE);
  auto seg_same = [&] { return memcmp(seg.data(), page + PAGE_HEADER + PAGE_BTR_SEG_LEAF, seg.size()) == 0; };

  /* Between the creation of its segments and of its links: FIL_PAGE_NEXT is 0. */
  m_sys->m_btree->page_create(block, m_sec, 0, &mtr);
  EXPECT_TRUE(seg_same());
  EXPECT_TRUE(page_get_high_key(page).is_null());
  page_set_max_trx_id(block, 1, &mtr);

  Btree::page_set_next(page, FIL_NULL, &mtr);
  Btree::page_set_prev(page, FIL_NULL, &mtr);

  fill(mtr, block, 0, 20);

  EXPECT_NE(raw_field(page), 0UL);
  EXPECT_TRUE(page_get_high_key(page).is_null());
  EXPECT_EQ(page_get_rec_data_size(page), page_get_data_size(page));
  validate(block, m_sec);

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_sec, &mtr));
  EXPECT_TRUE(seg_same());
  validate(block, m_sec);

  page_delete_rec_list_end(recs(page)[10], block, m_sec, ULINT_UNDEFINED, ULINT_UNDEFINED, &mtr);
  EXPECT_TRUE(seg_same());

  m_sys->m_btree->page_empty(block, m_sec, 0, &mtr);
  EXPECT_TRUE(seg_same());
  EXPECT_EQ(page_get_data_size(page), 0UL);
  validate(block, m_sec);

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** The real thing: the root of an index reads no key, and a reorganize of it, which runs
page_create(), keeps its segment headers. */
TEST_F(HighKeyTest, RealRoot) {
  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_clust->get_space_id(), &mtr);

  auto block = m_sys->m_btree->block_get(m_clust->get_space_id(), m_clust->get_page_no(), RW_X_LATCH, &mtr);
  auto page = block->get_frame();

  EXPECT_EQ(mach_read_from_4(page + FIL_PAGE_NEXT), FIL_NULL);
  EXPECT_TRUE(page_get_high_key(page).is_null());

  const std::vector<byte> seg(page + PAGE_HEADER + PAGE_BTR_SEG_LEAF, page + PAGE_HEADER + PAGE_BTR_SEG_LEAF + 2 * FSEG_HEADER_SIZE);

  ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, m_clust, &mtr));

  EXPECT_EQ(memcmp(seg.data(), page + PAGE_HEADER + PAGE_BTR_SEG_LEAF, seg.size()), 0);
  EXPECT_TRUE(page_get_high_key(page).is_null());
  validate(block, m_clust);

  mtr.commit(*m_sys);
}

#ifdef UNIV_DEBUG
/** Setting a key without a right sibling, as on a root, is a caller's bug that would
overwrite a segment header: an assertion, in a debug build. */
TEST_F(HighKeyTest, RootDeath) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = alloc(mtr, m_sec);
  auto page = block->get_frame();
  const auto key = sec_key(1000);

  EXPECT_DEATH((void)page_set_high_key(block, key.m_rec, key.m_size, &mtr), "");

  Btree::page_set_next(page, FIL_NULL, &mtr);
  EXPECT_DEATH((void)page_set_high_key(block, key.m_rec, key.m_size, &mtr), "");

  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}

/** A page with a key can't become rightmost before the key is cleared. */
TEST_F(HighKeyTest, RightmostDeath) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  Mtr mtr;

  mtr.start();
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

  auto block = linked(mtr, m_sec);
  auto page = block->get_frame();

  set(mtr, block, sec_key(1000));
  EXPECT_DEATH(Btree::page_set_next(page, FIL_NULL, &mtr), "");

  page_clear_high_key(block, &mtr);
  free_page(mtr, m_sec, block);
  mtr.commit(*m_sys);
}
#endif /* UNIV_DEBUG */

/* --- The high key after a crash ------------------------------------------ */

/** What the child that crashes tells the parent. */
struct CrashInfo {
  page_no_t m_page_no;
  byte m_page[UNIV_PAGE_SIZE];
};

/** A crash: the page of the child, whose redo is durable and which the page cleaner most
likely didn't write, is the same after recovery. */
class HighKeyCrash : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();

    auto mem = ::mmap(nullptr, sizeof(CrashInfo), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    ASSERT_NE(mem, MAP_FAILED);
    m_info = static_cast<CrashInfo *>(mem);
  }

  void TearDown() override {
    (void)::munmap(m_info, sizeof(CrashInfo));
    ut::DbTest::TearDown();
  }

  [[nodiscard]] Index *sec() { return get_table(*m_sys, ut::TABLE)->get_index_on_name(ut::SEC_A); }

  /** Insert the records (a, 0) from first of n of SEC_A into the page. */
  void put(Mtr &mtr, BufBlock *block, MemHeap *heap, uint32_t first, uint32_t n) {
    auto index = sec();

    for (auto a = first; a < first + n; ++a) {
      auto t = entry(index, heap, a);
      PageCursor cur;

      (void)cur.search(*m_sys, block, index, t, PAGE_CUR_LE);
      ASSERT_FALSE(cur.tuple_insert(t, index, 0, &mtr).is_null());
    }
  }

  static DTuple *entry(Index *index, MemHeap *heap, uint32_t a) {
    auto t = dtuple_create(heap, 2);
    auto v = static_cast<byte *>(mem_heap_alloc(heap, 8));

    index->copy_types(t, 2);
    mach_write_to_4(v, a);
    mach_write_to_4(v + 4, 0);
    dfield_set_data(dtuple_get_nth_field(t, 0), v, 4);
    dfield_set_data(dtuple_get_nth_field(t, 1), v + 4, 4);

    return t;
  }

  /** In the child: a page with records and a key in one mtr, ops in another, the log
  durable, the image kept for the parent. Then the parent checks the recovered page. */
  void run(const std::function<void(Mtr &, BufBlock *, Index *)> &ops) {
    crash([&] {
      auto index = sec();
      auto heap = mem_heap_create(16384);
      Mtr mtr;

      mtr.start();
      m_sys->m_fsp->latch(index->get_space_id(), &mtr);

      auto block = m_sys->m_btree->page_alloc(index, 0, FSP_UP, 0, &mtr);

      ASSERT_NE(block, nullptr);
      m_sys->m_btree->page_create(block, index, 0, &mtr);
      page_set_max_trx_id(block, 1, &mtr);
      Btree::page_set_next(block->get_frame(), block->get_page_no() + 1, &mtr);
      put(mtr, block, heap, 0, 30);

      auto t = entry(index, heap, 5000);
      const auto size = rec_get_converted_size(index, t, 0);
      auto rec = Rec::convert_dtuple_to_rec(static_cast<byte *>(mem_heap_alloc(heap, size)), index, t, 0);

      ASSERT_TRUE(page_set_high_key(block, rec, size, &mtr));

      m_info->m_page_no = block->get_page_no();
      mtr.commit(*m_sys);

      mtr.start();
      block = m_sys->m_btree->block_get(index->get_space_id(), m_info->m_page_no, RW_X_LATCH, &mtr);
      ops(mtr, block, index);
      memcpy(m_info->m_page, block->get_frame(), UNIV_PAGE_SIZE);
      mtr.commit(*m_sys);

      m_sys->m_log->buffer_flush_to_disk();
      mem_heap_free(heap);
    });

    auto index = sec();
    Mtr mtr;

    mtr.start();

    auto block = m_sys->m_btree->block_get(index->get_space_id(), m_info->m_page_no, RW_X_LATCH, &mtr);
    auto page = block->get_frame();

    EXPECT_EQ(memcmp(page + PAGE_HEADER, m_info->m_page + PAGE_HEADER, UNIV_PAGE_SIZE - PAGE_HEADER - FIL_PAGE_DATA_END), 0);
    EXPECT_TRUE(page_validate(page, index));

    m_key = page_get_high_key(page);
    EXPECT_FALSE(m_key.is_null());

    if (!m_key.is_null()) {
      ulint len{};

      EXPECT_EQ(mach_read_from_4(m_key.get_nth_field(0, &len)), 5000UL);
    }

    mtr.commit(*m_sys);
  }

  CrashInfo *m_info{};
  Rec m_key{};
};

TEST_F(HighKeyCrash, Set) {
  run([](Mtr &, BufBlock *, Index *) {});
}

TEST_F(HighKeyCrash, Reorg) {
  run([&](Mtr &mtr, BufBlock *block, Index *index) {
    ASSERT_TRUE(m_sys->m_btree->page_reorganize(block, index, &mtr));
  });
}

TEST_F(HighKeyCrash, Empty) {
  run([&](Mtr &mtr, BufBlock *block, Index *index) {
    m_sys->m_btree->page_empty(block, index, 0, &mtr);
  });
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
      m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
  m_sys->m_fsp->latch(m_sec->get_space_id(), &mtr);

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
