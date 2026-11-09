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

/** @file unit-tests/test_row_upd.cc
RowUpdate: update vectors (lookup by field number, the binary difference
of an entry and a record, the ordering fields an update changes, their
redo log), the updates of rows in place, by delete and insert, of the
primary key, to the same value, between NULL and values, with the
secondary index maintained, and rollback. RowUpdTest runs on a fresh
instance per test, RowUpdIdxTest on tables of its own in a shared one. */

#include "ut0env.h"

#include "idx0am.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "rem0rec.h"
#include "row0upd.h"
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
#include <cstring>
#include <random>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace {

using RowUpdTest = ut::DbTest;

/** @return the test table, from the dictionary cache. */
Table *test_table(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table;
}

/** An entry of index with a recognizable value in each field. */
DTuple *make_entry(const Index *index, MemHeap *heap) {
  const auto n = index->get_n_fields();
  auto entry = dtuple_create(heap, n);

  index->copy_types(entry, n);

  for (ulint i{}; i < n; ++i) {
    auto field = dtuple_get_nth_field(entry, i);
    const auto len = index->get_nth_field(i)->get_col()->get_fixed_size() > 0
                       ? index->get_nth_field(i)->get_col()->get_fixed_size()
                       : ulint(5);
    auto data = static_cast<byte *>(mem_heap_alloc(heap, len));

    memset(data, int('A' + i), len);
    dfield_set_data(field, data, len);
  }

  return entry;
}

/** An update vector with a single field: field_no of index set to value. */
Upd *make_update(const Index *index, ulint field_no, const char *value, MemHeap *heap) {
  auto update = RowUpdate::upd_create(1, heap);
  auto uf = &update->m_fields[0];

  new (uf) UpdField();
  uf->set_field_no(field_no, index, nullptr);
  dfield_set_data(&uf->m_new_val, value, strlen(value));
  update->m_info_bits = 0;

  return update;
}

/** Replacing the values of an update vector of one field in an entry of
the clustered index: only that field changes. The field was looked up by
indexing the update vector with the column's position: any position past
the first asserted (or read garbage in a release build). */
TEST_F(RowUpdTest, ReplaceOneField) {
  auto table = test_table(*m_sys);

  ASSERT_NE(table, nullptr);

  auto clust = table->get_first_index();
  const auto n = clust->get_n_fields();
  auto heap = mem_heap_create(1024);

  for (ulint pos = 0; pos < n; ++pos) {
    auto col = clust->get_nth_field(pos)->get_col();

    if (col->get_fixed_size() > 0 && col->get_fixed_size() != 4) {
      /* DB_TRX_ID, DB_ROLL_PTR: not updated by an update vector. */
      continue;
    }

    auto entry = make_entry(clust, heap);
    const char *value = col->get_fixed_size() == 4 ? "wxyz" : "new value";
    auto update = make_update(clust, pos, value, heap);

    m_sys->m_row_upd->index_replace_new_col_vals(entry, clust, update, heap);

    for (ulint i{}; i < n; ++i) {
      auto field = dtuple_get_nth_field(entry, i);
      auto data = static_cast<const char *>(dfield_get_data(field));

      if (i == pos) {
        ASSERT_EQ(dfield_get_len(field), strlen(value)) << "field " << i;
        EXPECT_EQ(std::string(data, strlen(value)), value) << "field " << i;
      } else {
        ASSERT_GT(dfield_get_len(field), 0UL);
        EXPECT_EQ(data[0], char('A' + i)) << "field " << i << " changed by an update of field " << pos;
      }
    }
  }

  mem_heap_free(heap);
}

/** The same for an entry of SEC_A: the update vector is in clustered index
positions, the entry's fields are mapped through their columns. */
TEST_F(RowUpdTest, ReplaceInSecondary) {
  auto table = test_table(*m_sys);

  ASSERT_NE(table, nullptr);

  auto clust = table->get_first_index();
  auto sec = table->get_index_on_name(ut::SEC_A);

  ASSERT_NE(sec, nullptr);

  /* SEC_A is (a, k): a is the first field. */
  const auto a_pos = clust->get_clustered_field_pos(sec->get_nth_field(0)->get_col());
  auto heap = mem_heap_create(1024);
  auto entry = make_entry(sec, heap);
  auto update = make_update(clust, a_pos, "wxyz", heap);

  m_sys->m_row_upd->index_replace_new_col_vals(entry, sec, update, heap);

  EXPECT_EQ(std::string(static_cast<const char *>(dfield_get_data(dtuple_get_nth_field(entry, 0))), 4), "wxyz");
  EXPECT_EQ(static_cast<const char *>(dfield_get_data(dtuple_get_nth_field(entry, 1)))[0], 'B');

  mem_heap_free(heap);
}

ut::Rows make_rows(uint32_t n) {
  ut::Rows rows;

  for (uint32_t k = 0; k < n; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  return rows;
}

/** Updates of each kind, committed. */
TEST_F(RowUpdTest, Kinds) {
  auto rows = make_rows(100);

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 100; ++k) {
    auto row = rows[k];

    switch (k % 6) {
      case 0: /* In place, same size. */
        row.m_a = k + 1;
        break;
      case 1: /* Same values. */
        break;
      case 2: /* The record grows. */
        row.m_b = ut::blob(k, 3000);
        break;
      case 3: /* Off page. */
        row.m_b = ut::blob(k, 50000);
        break;
      case 4: /* Shrinks. */
        row.m_b = "";
        break;
      case 5: /* The primary key. */
        row.m_k = k + 1000;
        break;
    }

    ASSERT_EQ(update(trx, k, row), DB_SUCCESS) << k;
    rows.erase(k);
    rows[row.m_k] = row;
  }

  commit(trx);
  expect_rows(rows);

  restart();
  expect_rows(rows);
}

/** A blob updated many times, between off page and in page. */
TEST_F(RowUpdTest, BlobFlipFlop) {
  ut::Rows rows{{1, ut::Row{1, 1, "x"}}};

  load(rows);

  for (uint32_t i = 0; i < 30; ++i) {
    auto trx = begin();
    auto row = rows[1];

    row.m_b = ut::blob(i, i % 2 == 0 ? 70000 + i : i);
    ASSERT_EQ(update(trx, 1, row), DB_SUCCESS);
    rows[1] = row;
    commit(trx);
  }

  expect_rows(rows);
}

/** Update to a key that exists fails and leaves the row. */
TEST_F(RowUpdTest, DuplicateKey) {
  auto rows = make_rows(3);

  load(rows);

  auto trx = begin();
  auto row = rows[0];

  row.m_k = 2;
  EXPECT_EQ(update(trx, 0, row), DB_DUPLICATE_KEY);
  commit(trx);

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

/* --- Update vectors and updates of real records, in a shared instance -- */

namespace {

/** The clustered index fields of a Tab: k, DB_TRX_ID, DB_ROLL_PTR, a, b. */
constexpr ulint K_POS = 0;
constexpr ulint A_POS = 3;
constexpr ulint B_POS = 4;

/** A record of an index and where it is. */
struct Found {
  bool m_found{};
  bool m_deleted{};
  /* Where the record is: an update in place leaves it there. */
  const byte *m_at{};
  trx_id_t m_trx_id{};
  std::vector<std::optional<std::string>> m_fields;
};

/** @return the 4 byte big endian form of v. */
std::string u4(uint32_t v) {
  std::string s(4, '\0');

  mach_write_to_4(reinterpret_cast<byte *>(s.data()), v);
  return s;
}

class RowUpdIdxTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();
    m_tab = tab();
    m_heap = mem_heap_create(4096);
  }

  void TearDown() override {
    mem_heap_free(m_heap);
    m_tab.reset();
  }

  /** @return the records of index whose first field is first. */
  std::vector<Found> find(const Index *index, uint32_t first) {
    std::vector<Found> v;
    Mtr mtr;
    IndexCursor pcur{*m_sys};
    MemHeap *heap{};

    mtr.start();
    pcur.seek_first(index, Intent::Read, &mtr);

    while (pcur.next(&mtr)) {
      auto rec = pcur.rec();
      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      ulint len;

      if (mach_read_from_4(rec.get_nth_field(offs, 0, &len)) != first) {
        continue;
      }

      Found f;

      f.m_found = true;
      f.m_deleted = rec.get_deleted_flag();
      f.m_at = rec.get();

      for (ulint i{}; i < offs.n_fields(); ++i) {
        auto d = rec.get_nth_field(offs, i, &len);

        if (len == UNIV_SQL_NULL) {
          f.m_fields.emplace_back(std::nullopt);
        } else {
          f.m_fields.emplace_back(std::string(reinterpret_cast<const char *>(d.get()), len));
        }
      }

      if (index->is_clustered()) {
        f.m_trx_id = mach_read_from_8(reinterpret_cast<const byte *>(f.m_fields[1]->data()));
      }

      v.push_back(std::move(f));
    }

    pcur.commit(&mtr);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    return v;
  }

  /** The record of the clustered index with key k, copied: the record, its
  offsets and an entry with its values (built in m_heap). */
  struct Copy {
    std::vector<byte> m_buf;
    Rec m_rec;
    RecOffs m_offs;
    DTuple *m_entry{};
  };

  Copy copy_rec(const Index *index, uint32_t first) {
    Copy c;
    Mtr mtr;
    IndexCursor pcur{*m_sys};

    mtr.start();
    pcur.seek_first(index, Intent::Read, &mtr);

    while (pcur.next(&mtr)) {
      auto rec = pcur.rec();
      ulint len;

      if (mach_read_from_4(rec.get_nth_field(0, &len)) != first) {
        continue;
      }

      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &m_heap, Current_location());

      c.m_buf.resize(offs.size());
      c.m_rec = rec.copy(c.m_buf.data(), offs);
      c.m_offs = PhyRec(index, c.m_rec).get_all_col_offsets(nullptr, &m_heap, Current_location());

      const auto n = index->get_n_fields();

      c.m_entry = dtuple_create(m_heap, n);
      index->copy_types(c.m_entry, n);
      c.m_rec.copy_prefix_to_dtuple(c.m_entry, index, n, m_heap);
      break;
    }

    pcur.commit(&mtr);

    EXPECT_NE(c.m_entry, nullptr) << first;

    return c;
  }

  /** An update vector of the clustered index: (field number, value). */
  Upd *make_update(const std::vector<std::pair<ulint, std::optional<std::string>>> &fields) {
    auto clust = m_tab->clust();
    auto update = RowUpdate::upd_create(fields.size(), m_heap);

    for (ulint i{}; i < fields.size(); ++i) {
      auto uf = update->get_nth_field(i);

      new (uf) UpdField();
      uf->set_field_no(fields[i].first, clust, nullptr);

      if (fields[i].second.has_value()) {
        auto p = mem_heap_dup(m_heap, fields[i].second->data(), fields[i].second->size());

        dfield_set_data(&uf->m_new_val, p, fields[i].second->size());
      } else {
        dfield_set_null(&uf->m_new_val);
      }
    }

    update->m_info_bits = 0;

    return update;
  }

  std::unique_ptr<Tab> m_tab;
  MemHeap *m_heap{};
};

/** An update vector is looked up by field number, not by position:
get_field(n) finds the field whose m_field_no is n. */
TEST_F(RowUpdIdxTest, GetFieldByNumber) {
  auto update = make_update({{B_POS, "bb"}, {K_POS, u4(1)}, {A_POS, u4(3)}});

  EXPECT_EQ(RowUpdate::upd_get_n_fields(update), 3UL);
  EXPECT_EQ(update->get_field(B_POS), update->get_nth_field(0));
  EXPECT_EQ(update->get_field(K_POS), update->get_nth_field(1));
  EXPECT_EQ(update->get_field(A_POS), update->get_nth_field(2));
  EXPECT_EQ(update->get_field(1), nullptr);
  EXPECT_EQ(update->get_field(2), nullptr);

  /* set_field_no() copies the type of the column. */
  EXPECT_EQ(dfield_get_type(&update->get_nth_field(2)->m_new_val)->mtype, DATA_INT);
  EXPECT_EQ(dfield_get_type(&update->get_nth_field(0)->m_new_val)->mtype, DATA_BLOB);
}

/** build_difference_binary(): the fields of an entry that differ from the
clustered record, the system columns never; none for the record itself. */
TEST_F(RowUpdIdxTest, DifferenceBinary) {
  m_tab->load({{7, ut::Row{7, 70, "seven"}}});

  auto clust = m_tab->clust();
  auto c = copy_rec(clust, 7);

  ASSERT_NE(c.m_entry, nullptr);

  auto none = m_sys->m_row_upd->build_difference_binary(clust, c.m_entry, c.m_rec, nullptr, m_heap);

  EXPECT_EQ(RowUpdate::upd_get_n_fields(none), 0UL);

  /* Change a and b, and the system columns. */
  auto e = dtuple_copy(c.m_entry, m_heap);
  auto a = mem_heap_alloc(m_heap, 4);

  mach_write_to_4(static_cast<byte *>(a), 71);
  dfield_set_data(dtuple_get_nth_field(e, A_POS), a, 4);
  dfield_set_data(dtuple_get_nth_field(e, B_POS), "SEVEN", 5);
  dfield_set_data(dtuple_get_nth_field(e, 1), mem_heap_zalloc(m_heap, 6), 6);
  dfield_set_data(dtuple_get_nth_field(e, 2), mem_heap_zalloc(m_heap, 7), 7);

  auto update = m_sys->m_row_upd->build_difference_binary(clust, e, c.m_rec, nullptr, m_heap);

  ASSERT_EQ(RowUpdate::upd_get_n_fields(update), 2UL);
  EXPECT_EQ(update->get_nth_field(0)->m_field_no, A_POS);
  EXPECT_EQ(update->get_nth_field(1)->m_field_no, B_POS);
  EXPECT_EQ(mach_read_from_4(static_cast<const byte *>(dfield_get_data(&update->get_nth_field(0)->m_new_val))), 71UL);

  /* A string of a different length but the same prefix differs too. */
  dfield_set_data(dtuple_get_nth_field(e, B_POS), "seven ", 6);
  dfield_set_data(dtuple_get_nth_field(e, A_POS), dfield_get_data(dtuple_get_nth_field(c.m_entry, A_POS)), 4);
  update = m_sys->m_row_upd->build_difference_binary(clust, e, c.m_rec, nullptr, m_heap);
  ASSERT_EQ(RowUpdate::upd_get_n_fields(update), 1UL);
  EXPECT_EQ(update->get_nth_field(0)->m_field_no, B_POS);
}

/** build_sec_rec_difference_binary() on a SEC_A record (a, k). */
TEST_F(RowUpdIdxTest, SecDifferenceBinary) {
  m_tab->load({{7, ut::Row{7, 70, "seven"}}});

  auto sec = m_tab->sec();
  auto c = copy_rec(sec, 70);

  ASSERT_NE(c.m_entry, nullptr);
  EXPECT_EQ(RowUpdate::upd_get_n_fields(RowUpdate::build_sec_rec_difference_binary(sec, c.m_entry, c.m_rec, nullptr, m_heap)), 0UL);

  auto e = dtuple_copy(c.m_entry, m_heap);
  auto a = mem_heap_alloc(m_heap, 4);

  mach_write_to_4(static_cast<byte *>(a), 99);
  dfield_set_data(dtuple_get_nth_field(e, 0), a, 4);

  auto update = RowUpdate::build_sec_rec_difference_binary(sec, e, c.m_rec, nullptr, m_heap);

  ASSERT_EQ(RowUpdate::upd_get_n_fields(update), 1UL);
  EXPECT_EQ(update->get_nth_field(0)->m_field_no, 0UL) << "the position in the secondary index";
}

/** changes_ord_field_binary(): an update changes an ordering field of
SEC_A only if it updates a to a different value (or the row is unknown). */
TEST_F(RowUpdIdxTest, ChangesOrdField) {
  auto sec = m_tab->sec();
  auto clust = m_tab->clust();
  auto table = m_tab->table();

  /* A row of the table: columns k, a, b, then the system columns. */
  const auto n_cols = table->get_n_cols();
  auto row = dtuple_create(m_heap, n_cols);

  table->copy_types(row);

  for (ulint i{}; i < n_cols; ++i) {
    dfield_set_null(dtuple_get_nth_field(row, i));
  }

  const auto a_col = sec->get_nth_field(0)->get_col()->get_no();
  const auto a = u4(70);

  dfield_set_data(dtuple_get_nth_field(row, a_col), a.data(), 4);

  EXPECT_TRUE(RowUpdate::changes_ord_field_binary(nullptr, sec, make_update({{A_POS, u4(70)}})));
  EXPECT_FALSE(RowUpdate::changes_ord_field_binary(row, sec, make_update({{A_POS, u4(70)}})));
  EXPECT_TRUE(RowUpdate::changes_ord_field_binary(row, sec, make_update({{A_POS, u4(71)}})));
  EXPECT_FALSE(RowUpdate::changes_ord_field_binary(row, sec, make_update({{B_POS, "x"}})));
  EXPECT_FALSE(RowUpdate::changes_ord_field_binary(nullptr, sec, make_update({{B_POS, "x"}})));
  /* The clustered index orders by k only. */
  EXPECT_TRUE(RowUpdate::changes_ord_field_binary(nullptr, clust, make_update({{K_POS, u4(1)}})));
  EXPECT_FALSE(RowUpdate::changes_ord_field_binary(nullptr, clust, make_update({{A_POS, u4(1)}})));
}

/** changes_some_index_ord_field_binary(): k and a are in an index, b not. */
TEST_F(RowUpdIdxTest, ChangesSomeIndexOrdField) {
  auto table = m_tab->table();

  EXPECT_TRUE(m_sys->m_row_upd->changes_some_index_ord_field_binary(table, make_update({{A_POS, u4(1)}})));
  EXPECT_TRUE(m_sys->m_row_upd->changes_some_index_ord_field_binary(table, make_update({{K_POS, u4(1)}})));
  EXPECT_FALSE(m_sys->m_row_upd->changes_some_index_ord_field_binary(table, make_update({{B_POS, "x"}})));
  EXPECT_TRUE(m_sys->m_row_upd->changes_some_index_ord_field_binary(table, make_update({{B_POS, "x"}, {A_POS, u4(2)}})));
}

/** changes_first_fields_binary() on a SEC_A entry. */
TEST_F(RowUpdIdxTest, ChangesFirstFields) {
  m_tab->load({{7, ut::Row{7, 70, "seven"}}});

  auto sec = m_tab->sec();
  auto c = copy_rec(sec, 70);

  EXPECT_FALSE(m_sys->m_row_upd->changes_first_fields_binary(c.m_entry, sec, make_update({{A_POS, u4(70)}}), 1));
  EXPECT_TRUE(m_sys->m_row_upd->changes_first_fields_binary(c.m_entry, sec, make_update({{A_POS, u4(71)}}), 1));
  EXPECT_FALSE(m_sys->m_row_upd->changes_first_fields_binary(c.m_entry, sec, make_update({{K_POS, u4(8)}}), 1));
  EXPECT_TRUE(m_sys->m_row_upd->changes_first_fields_binary(c.m_entry, sec, make_update({{K_POS, u4(8)}}), 2));
}

/** rec_in_place() overwrites fields of the same size and sets the info
bits of the update vector. */
TEST_F(RowUpdIdxTest, RecInPlace) {
  m_tab->load({{7, ut::Row{7, 70, "seven"}}});

  auto clust = m_tab->clust();
  auto c = copy_rec(clust, 7);
  auto update = make_update({{A_POS, u4(77)}, {B_POS, "SEVEN"}});

  EXPECT_FALSE(m_sys->m_row_upd->changes_field_size_or_external(clust, c.m_offs, update));

  update->m_info_bits = REC_INFO_DELETED_FLAG;
  m_sys->m_row_upd->rec_in_place(c.m_rec, clust, c.m_offs, update);

  ulint len;

  EXPECT_EQ(mach_read_from_4(c.m_rec.get_nth_field(c.m_offs, A_POS, &len)), 77UL);
  EXPECT_EQ(memcmp(c.m_rec.get_nth_field(c.m_offs, B_POS, &len).get(), "SEVEN", 5), 0);
  EXPECT_EQ(mach_read_from_4(c.m_rec.get_nth_field(c.m_offs, K_POS, &len)), 7UL);
  EXPECT_TRUE(c.m_rec.get_deleted_flag());

  /* A longer b does not fit in place. */
  EXPECT_TRUE(m_sys->m_row_upd->changes_field_size_or_external(clust, c.m_offs, make_update({{B_POS, "seven!"}})));
}

/** The redo log of an update vector parses back to the same vector. */
TEST_F(RowUpdIdxTest, WriteLogParse) {
  auto update = make_update({{A_POS, u4(5)}, {B_POS, std::string(300, 'z')}, {K_POS, std::nullopt}});

  update->m_info_bits = REC_INFO_DELETED_FLAG;

  Mtr mtr;

  mtr.start();
  /* Not written to the redo log: no page is changed in this mtr. */

  auto log_ptr = mlog_open(&mtr, MLOG_BUF_MARGIN);

  ASSERT_NE(log_ptr, nullptr);
  m_sys->m_row_upd->index_write_log(update, log_ptr, &mtr);

  const auto &log = mtr.m_log;
  std::vector<byte> buf;

  for (auto block = log.get_first_block(); block != nullptr; block = log.get_next_block(block)) {
    const auto data = log.get_data(block);

    buf.insert(buf.end(), data, data + log.get_used(block));
  }

  EXPECT_EQ(buf.size(), log.get_data_size());
  mtr.commit(*m_sys);

  Upd *parsed{};
  auto end = m_sys->m_row_upd->index_parse(buf.data(), buf.data() + buf.size(), m_heap, &parsed);

  ASSERT_EQ(end, buf.data() + buf.size());
  ASSERT_NE(parsed, nullptr);
  EXPECT_EQ(parsed->m_info_bits, REC_INFO_DELETED_FLAG);
  ASSERT_EQ(RowUpdate::upd_get_n_fields(parsed), 3UL);

  for (ulint i{}; i < 3; ++i) {
    auto a = update->get_nth_field(i);
    auto b = parsed->get_nth_field(i);

    EXPECT_EQ(a->m_field_no, b->m_field_no);
    EXPECT_TRUE(dfield_datas_are_binary_equal(&a->m_new_val, &b->m_new_val)) << i;
  }

  /* A truncated log is incomplete. */
  EXPECT_EQ(m_sys->m_row_upd->index_parse(buf.data(), buf.data() + buf.size() - 1, m_heap, &parsed), nullptr);
}

/** An update of a to a value of the same size is done in place: the
clustered record stays where it is, not delete marked, with the new trx
id; SEC_A gets a new entry and the old one is delete marked. */
TEST_F(RowUpdIdxTest, InPlaceClusteredSecMaintained) {
  m_tab->load({{1, ut::Row{1, 10, "abc"}}, {2, ut::Row{2, 20, "def"}}});

  const auto before = find(m_tab->clust(), 1);

  ASSERT_EQ(before.size(), 1UL);

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 11, "abc"}), DB_SUCCESS);

  const auto after = find(m_tab->clust(), 1);

  ASSERT_EQ(after.size(), 1UL);
  EXPECT_EQ(after[0].m_at, before[0].m_at) << "in place";
  EXPECT_FALSE(after[0].m_deleted);
  EXPECT_EQ(after[0].m_fields[A_POS], u4(11));
  EXPECT_EQ(after[0].m_trx_id, trx->m_id);

  auto old_sec = find(m_tab->sec(), 10);
  auto new_sec = find(m_tab->sec(), 11);

  ASSERT_EQ(old_sec.size(), 1UL);
  ASSERT_EQ(new_sec.size(), 1UL);
  EXPECT_TRUE(old_sec[0].m_deleted);
  EXPECT_FALSE(new_sec[0].m_deleted);

  commit(trx);
  m_tab->expect_rows({{1, ut::Row{1, 11, "abc"}}, {2, ut::Row{2, 20, "def"}}});
}

/** An update that changes only b leaves SEC_A untouched. */
TEST_F(RowUpdIdxTest, NonOrdUpdateSecUntouched) {
  m_tab->load({{1, ut::Row{1, 10, "abc"}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 10, "xyz"}), DB_SUCCESS);

  const auto sec = find(m_tab->sec(), 10);

  ASSERT_EQ(sec.size(), 1UL);
  EXPECT_FALSE(sec[0].m_deleted);
  commit(trx);
  m_tab->expect_rows({{1, ut::Row{1, 10, "xyz"}}});
}

/** A bigger b: the record is rewritten (it may move), still one record. */
TEST_F(RowUpdIdxTest, SizeChange) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 60; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 200)};
  }

  m_tab->load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 60; k += 2) {
    rows[k].m_b = ut::blob(k + 1, k % 4 == 0 ? 2000 : 20);
    ASSERT_EQ(m_tab->update(trx, k, rows[k]), DB_SUCCESS);
  }

  commit(trx);

  for (uint32_t k = 0; k < 60; ++k) {
    const auto f = find(m_tab->clust(), k);

    ASSERT_EQ(f.size(), 1UL) << k;
    EXPECT_FALSE(f[0].m_deleted);
    EXPECT_EQ(*f[0].m_fields[B_POS], rows[k].m_b);
  }

  m_tab->expect_rows(rows);
}

/** A primary key update: the old record delete marked, a new record with
the new key; both SEC_A entries exist, the old delete marked; rollback
restores the old record and removes the new one. */
TEST_F(RowUpdIdxTest, PkUpdateAndRollback) {
  m_tab->load({{1, ut::Row{1, 10, "one"}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{100, 10, "one"}), DB_SUCCESS);

  auto old_rec = find(m_tab->clust(), 1);
  auto new_rec = find(m_tab->clust(), 100);

  ASSERT_EQ(old_rec.size(), 1UL);
  ASSERT_EQ(new_rec.size(), 1UL);
  EXPECT_TRUE(old_rec[0].m_deleted);
  EXPECT_FALSE(new_rec[0].m_deleted);
  EXPECT_EQ(new_rec[0].m_fields[B_POS], std::string("one"));

  /* SEC_A (10, 1) delete marked, (10, 100) live. */
  auto sec = find(m_tab->sec(), 10);

  ASSERT_EQ(sec.size(), 2UL);
  EXPECT_EQ(sec[0].m_fields[1], u4(1));
  EXPECT_TRUE(sec[0].m_deleted);
  EXPECT_EQ(sec[1].m_fields[1], u4(100));
  EXPECT_FALSE(sec[1].m_deleted);

  rollback(trx);

  old_rec = find(m_tab->clust(), 1);
  ASSERT_EQ(old_rec.size(), 1UL);
  EXPECT_FALSE(old_rec[0].m_deleted);
  EXPECT_TRUE(find(m_tab->clust(), 100).empty() || find(m_tab->clust(), 100)[0].m_deleted);
  m_tab->expect_rows({{1, ut::Row{1, 10, "one"}}});
}

/** An update to the same values changes no data and no secondary entry. */
TEST_F(RowUpdIdxTest, SameValue) {
  m_tab->load({{1, ut::Row{1, 10, "one"}}});

  const auto before = find(m_tab->clust(), 1);
  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 10, "one"}), DB_SUCCESS);
  commit(trx);

  const auto after = find(m_tab->clust(), 1);

  ASSERT_EQ(after.size(), 1UL);
  EXPECT_EQ(after[0].m_fields[A_POS], before[0].m_fields[A_POS]);
  EXPECT_EQ(after[0].m_fields[B_POS], before[0].m_fields[B_POS]);
  EXPECT_EQ(after[0].m_at, before[0].m_at);

  const auto sec = find(m_tab->sec(), 10);

  ASSERT_EQ(sec.size(), 1UL);
  EXPECT_FALSE(sec[0].m_deleted);
}

/** Rollback of an in-place update restores the old values and the
secondary entries. */
TEST_F(RowUpdIdxTest, InPlaceRollback) {
  m_tab->load({{1, ut::Row{1, 10, "abc"}}});

  auto trx = begin();

  ASSERT_EQ(m_tab->update(trx, 1, ut::Row{1, 12, "xyz"}), DB_SUCCESS);
  rollback(trx);

  const auto f = find(m_tab->clust(), 1);

  ASSERT_EQ(f.size(), 1UL);
  EXPECT_EQ(f[0].m_fields[A_POS], u4(10));
  EXPECT_EQ(f[0].m_fields[B_POS], std::string("abc"));

  auto live = find(m_tab->sec(), 10);

  ASSERT_EQ(live.size(), 1UL);
  EXPECT_FALSE(live[0].m_deleted);

  for (const auto &e : find(m_tab->sec(), 12)) {
    EXPECT_TRUE(e.m_deleted);
  }

  m_tab->expect_rows({{1, ut::Row{1, 10, "abc"}}});
}

/** NULL columns updated to values and back, fixed and variable length. */
TEST_F(RowUpdIdxTest, NullTransitions) {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
  const auto name = std::string("test/") + info->name() + "n";

  ASSERT_EQ(
    create_table(
      *m_sys,
      name,
      {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"i", IB_INT, IB_COL_NONE, 4}, {"v", IB_VARCHAR, IB_COL_NONE, 30}},
      {{"PRIMARY", {{"k", 0}}, true, false}, {"VI", {{"v", 0}}, false, false}}
    ),
    DB_SUCCESS
  );

  using Val = std::pair<std::optional<int32_t>, std::optional<std::string>>;

  auto write = [&](Trx *trx, uint32_t k, const Val &val, bool insert) {
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, name.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto fill = [&](api::Tuple *tpl) {
      EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
      EXPECT_EQ(val.first ? tpl->write_i32(1, *val.first) : tpl->set_value(1, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
      EXPECT_EQ(
        val.second ? tpl->set_value(2, val.second->data(), val.second->size()) : tpl->set_value(2, nullptr, api::IB_SQL_NULL),
        DB_SUCCESS
      );
    };

    DbErr err;

    if (insert) {
      auto tpl = crsr->clust_read_tuple_create();

      fill(tpl);
      err = crsr->insert_row(*m_sys, tpl);
      api::Tuple::destroy(tpl);
    } else {
      EXPECT_EQ(crsr->set_lock_mode(*m_sys, IB_LOCK_X), DB_SUCCESS);

      auto key = crsr->clust_search_tuple_create();
      int res{-1};

      EXPECT_EQ(key->write_u32(0, k), DB_SUCCESS);
      EXPECT_EQ(crsr->moveto(*m_sys, key, IB_CUR_GE, &res), DB_SUCCESS);
      EXPECT_EQ(res, 0);
      api::Tuple::destroy(key);

      auto old_tpl = crsr->clust_read_tuple_create();
      auto new_tpl = crsr->clust_read_tuple_create();

      EXPECT_EQ(crsr->read_row(*m_sys, old_tpl), DB_SUCCESS);
      EXPECT_EQ(new_tpl->copy(old_tpl), DB_SUCCESS);
      fill(new_tpl);
      err = crsr->update_row(*m_sys, old_tpl, new_tpl);
      api::Tuple::destroy(old_tpl);
      api::Tuple::destroy(new_tpl);
    }

    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  };

  auto read = [&](uint32_t k) {
    auto t = get_table(*m_sys, name);
    auto f = find(t->get_first_index(), k);
    Val v;

    EXPECT_EQ(f.size(), 1UL);

    if (f.size() == 1) {
      if (f[0].m_fields[3]) {
        int32_t i{};

        mach_read_int_type(&i, reinterpret_cast<const byte *>(f[0].m_fields[3]->data()), 4, false);
        v.first = i;
      }

      v.second = f[0].m_fields[4];
    }

    return v;
  };

  const std::vector<Val> seq{
    {std::nullopt, std::nullopt}, {5, ""}, {std::nullopt, "abc"}, {-7, std::nullopt}, {std::nullopt, ""},
    {0, "xyz"}, {std::nullopt, std::nullopt}, {INT32_MIN, std::string(30, 'w')}, {INT32_MAX, std::nullopt}};

  auto trx = begin();

  ASSERT_EQ(write(trx, 1, seq[0], true), DB_SUCCESS);
  commit(trx);
  EXPECT_EQ(read(1), seq[0]);

  for (size_t i = 1; i < seq.size(); ++i) {
    trx = begin();
    ASSERT_EQ(write(trx, 1, seq[i], false), DB_SUCCESS) << i;
    commit(trx);
    EXPECT_EQ(read(1), seq[i]) << i;
  }
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

using RowUpdPerf = RowUpdIdxTest;

/** Updates in place (a non-indexed column of the same size), of an indexed
column, and of the primary key, through RowUpdate (via the api). */
TEST_F(RowUpdPerf, Update) {
  constexpr uint32_t N = 20000;
  ut::Rows rows;

  for (uint32_t k = 0; k < N; ++k) {
    rows[k] = ut::Row{k, k, "abcdefgh"};
  }

  m_tab->load(rows);

  Trx *trx{};

  auto batch = [&](uint64_t i, auto &&fn) {
    if (i % 1000 == 0) {
      trx = begin();
    }

    fn();

    if (i % 1000 == 999) {
      commit(trx);
    }
  };

  auto r1 = ut::bench("update_in_place", N, [&](uint64_t i) {
    batch(i, [&] { (void)m_tab->update(trx, uint32_t(i), ut::Row{uint32_t(i), uint32_t(i), "ABCDEFGH"}); });
  });

  EXPECT_LT(r1.ns_per_op(), 2e6);

  auto r2 = ut::bench("update_sec_key", N, [&](uint64_t i) {
    batch(i, [&] { (void)m_tab->update(trx, uint32_t(i), ut::Row{uint32_t(i), uint32_t(i + N), "ABCDEFGH"}); });
  });

  EXPECT_LT(r2.ns_per_op(), 2e6);

  auto r3 = ut::bench("update_pk", N, [&](uint64_t i) {
    batch(i, [&] { (void)m_tab->update(trx, uint32_t(i), ut::Row{uint32_t(i + 10 * N), uint32_t(i + N), "ABCDEFGH"}); });
  });

  EXPECT_LT(r3.ns_per_op(), 2e6);

  auto r4 = ut::bench("delete", N, [&](uint64_t i) { batch(i, [&] { (void)m_tab->remove(trx, uint32_t(i + 10 * N)); }); });

  EXPECT_LT(r4.ns_per_op(), 2e6);
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
class RowUpdMtPerf : public RowUpdIdxTest {
 protected:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    s_config = perf_config(s_dir->m_path);
    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
  }

  /** @return a table of n rows (k, k, b of len bytes). */
  std::unique_ptr<Tab> loaded(const std::string &suffix, uint32_t n, size_t len) {
    auto t = tab(std::format("_{}", suffix).c_str());
    ut::Rows rows;

    for (uint32_t k{}; k < n; ++k) {
      rows[k] = ut::Row{k, k, std::string(len, 'b')};
    }

    t->load(rows);

    return t;
  }
};

/** Updates from 1 to 16 threads, each on its own range of rows, a commit
every 100: in place (b, same size), of the secondary key a, and of the
primary key k (the row moves). */
TEST_F(RowUpdMtPerf, KindsThreads) {
  constexpr uint32_t N = 6400;

  for (const char *kind : {"in_place", "sec_key", "pk"}) {
    auto t = loaded(kind, N, 42);
    /* The key of row i now: the pk moves by N each round. */
    uint32_t round{};

    for (int threads : {1, 2, 4, 8, 16}) {
      const uint32_t per = N / threads;
      std::vector<Trx *> trxs(threads);
      Watchdog watchdog(std::chrono::seconds(300));

      auto r = ut::bench_mt(std::format("update_{}_mt", kind), threads, per, [&](int th, uint64_t i) {
        if (i % 100 == 0) {
          trxs[th] = begin();
        }

        const auto row = uint32_t(th) * per + uint32_t(i);
        DbErr err{};

        if (std::string_view(kind) == "in_place") {
          err = t->update(trxs[th], row, ut::Row{row, row, std::string(42, char('c' + round % 20))});
        } else if (std::string_view(kind) == "sec_key") {
          err = t->update(trxs[th], row, ut::Row{row, row + (round + 1) * N, std::string(42, 'b')});
        } else {
          const auto k = row + round * N;

          err = t->update(trxs[th], k, ut::Row{k + N, row, std::string(42, 'b')});
        }

        EXPECT_EQ(err, DB_SUCCESS) << kind << " row " << row;

        if (i % 100 == 99 || i + 1 == per) {
          commit(trxs[th]);
        }
      });

      EXPECT_LT(r.ns_per_op(), 5e6) << kind;
      ++round;
    }

    /* Every row is there once, through both indexes. */
    ut::Rows rows;

    for (uint32_t row{}; row < N; ++row) {
      if (std::string_view(kind) == "in_place") {
        rows[row] = ut::Row{row, row, std::string(42, char('c' + (round - 1) % 20))};
      } else if (std::string_view(kind) == "sec_key") {
        rows[row] = ut::Row{row, row + round * N, std::string(42, 'b')};
      } else {
        rows[row + round * N] = ut::Row{row + round * N, row, std::string(42, 'b')};
      }
    }

    t->expect_rows(rows);
  }
}

/** In place updates of b in rows of 50 bytes, 1 KB and 20 KB (b external:
its pages are written again). */
TEST_F(RowUpdMtPerf, RowSizes) {
  struct Size {
    const char *m_name;
    size_t m_b;
    uint32_t m_n;
  };

  for (const auto &sz : {Size{"update_50B", 42, 4000}, Size{"update_1KB", 1016, 2000}, Size{"update_20KB", 20000, 200}}) {
    auto t = loaded(sz.m_name, sz.m_n, sz.m_b);
    const auto b = std::string(sz.m_b, 'u');
    Trx *trx{};

    auto r = ut::bench(sz.m_name, sz.m_n, [&](uint64_t i) {
      if (i % 100 == 0) {
        trx = begin();
      }

      EXPECT_EQ(t->update(trx, uint32_t(i), ut::Row{uint32_t(i), uint32_t(i), b}), DB_SUCCESS);

      if (i % 100 == 99) {
        commit(trx);
      }
    });

    EXPECT_LT(r.ns_per_op(), 2e7) << sz.m_name;
  }
}

/** Deletes from 1, 4 and 16 threads, each of its own rows. */
TEST_F(RowUpdMtPerf, DeleteThreads) {
  constexpr uint32_t N = 6400;

  for (int threads : {1, 4, 16}) {
    auto t = loaded(std::format("del{}", threads), N, 42);
    const uint32_t per = N / threads;
    std::vector<Trx *> trxs(threads);
    Watchdog watchdog(std::chrono::seconds(300));

    auto r = ut::bench_mt("delete_mt", threads, per, [&](int th, uint64_t i) {
      if (i % 100 == 0) {
        trxs[th] = begin();
      }

      EXPECT_EQ(t->remove(trxs[th], uint32_t(th) * per + uint32_t(i)), DB_SUCCESS);

      if (i % 100 == 99 || i + 1 == per) {
        commit(trxs[th]);
      }
    });

    EXPECT_LT(r.ns_per_op(), 5e6);
    t->expect_rows({});
  }
}

/** 8 threads updating interleaved rows (thread t: rows i * 8 + t), all on
the same pages, changing the secondary key and the size of b (in place,
then not), several rounds: the latching of shared pages, page
reorganizations and secondary index delete marks under contention. The
table is as the last round left it. */
TEST_F(RowUpdMtPerf, NeighbourRows) {
  constexpr int T = 8;
  constexpr uint32_t N = 2400;
  constexpr uint32_t ROUNDS = 3;

  auto t = loaded("n", N, 42);
  std::vector<Trx *> trxs(T);
  Watchdog watchdog(std::chrono::seconds(300));
  const uint64_t per = N / T * ROUNDS;

  auto r = ut::bench_mt("update_neighbours_mt", T, per, [&](int th, uint64_t i) {
    const auto round = uint32_t(i / (N / T));
    const auto k = uint32_t(i % (N / T)) * T + th;

    if (i % 50 == 0) {
      trxs[th] = begin();
    }

    EXPECT_EQ(t->update(trxs[th], k, ut::Row{k, k + (round + 1) * 100000, std::string(42 + (round + k) % 3 * 300, 'n')}), DB_SUCCESS);

    if (i % 50 == 49 || i + 1 == per) {
      commit(trxs[th]);
    }
  });

  EXPECT_LT(r.ns_per_op(), 5e6);

  ut::Rows rows;

  for (uint32_t k{}; k < N; ++k) {
    rows[k] = ut::Row{k, k + ROUNDS * 100000, std::string(42 + (ROUNDS - 1 + k) % 3 * 300, 'n')};
  }

  t->expect_rows(rows);
}

/** 8 threads, each transaction updating 4 of the same 16 rows in a random
order (the secondary key and b), a quarter of them rolled back on purpose:
lock waits, deadlocks (the victim rolled back), rollbacks of rows others
wait for. Nothing hangs, the work goes on, and the two indexes agree. */
TEST_F(RowUpdMtPerf, SameRows) {
  constexpr int T = 8;
  constexpr uint64_t N = 150;
  constexpr uint32_t ROWS = 16;

  auto t = loaded("same", ROWS, 42);
  std::atomic<uint64_t> deadlocks{};
  std::atomic<uint64_t> committed{};
  Watchdog watchdog(std::chrono::seconds(300));

  auto r = ut::bench_mt("update_same_rows_mt", T, N, [&](int th, uint64_t i) {
    std::mt19937 rng(uint32_t(th * 1000003 + i));
    auto trx = begin();
    DbErr err{DB_SUCCESS};

    for (int j = 0; j < 4 && err == DB_SUCCESS; ++j) {
      const auto k = uint32_t(rng() % ROWS);

      err = t->update(trx, k, ut::Row{k, uint32_t(th * 100000 + i), std::string(42 + rng() % 200, char('a' + th))});
    }

    if (err == DB_DEADLOCK || err == DB_LOCK_WAIT_TIMEOUT) {
      /* The victim was rolled back: end it. */
      ++deadlocks;
      rollback(trx);
    } else {
      EXPECT_EQ(err, DB_SUCCESS);

      if (i % 4 == 0) {
        rollback(trx);
      } else {
        commit(trx);
        ++committed;
      }
    }
  });

  EXPECT_LT(r.ns_per_op(), 1e8);
  EXPECT_GT(committed.load(), 0UL);
  std::fprintf(stderr, "same rows: %lu committed, %lu deadlocks or timeouts\n", ulong(committed.load()), ulong(deadlocks.load()));

  auto trx = begin();
  const auto rows = t->scan(trx);
  const auto sec = t->scan_sec(trx);

  commit(trx);

  ASSERT_EQ(rows.size(), ROWS);
  ASSERT_EQ(sec.size(), ROWS);

  for (const auto &row : sec) {
    auto it = rows.find(row.m_k);

    ASSERT_NE(it, rows.end()) << row.m_k;
    EXPECT_EQ(it->second, row) << row.m_k;
  }
}

}  // namespace
