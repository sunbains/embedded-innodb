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

/** @file unit-tests/test_row_merge.cc
Index build by merge sort (row/row0merge.cc): the clustered index is read
into sort buffers of a merge block (1 MB) each, sorted runs are written to a
temporary file, merged pass by pass and the sorted entries inserted into the
new index. Driven through api::IndexSchema::create_index() and directly
through row_merge_create_index() / row_merge_build_indexes() /
row_merge_rename_indexes() / row_merge_drop_indexes().

Each index built is checked against the rows: the exact entries in order,
Btree::validate_index(), and against an index on the same columns that the
inserts maintained.

  MergeTest      an instance shared by the suite, a table per test.
  MergeCrashTest an instance of its own: a crash in the middle of a build.
  MergePerf      performance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "idx0am.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "fsp0fsp.h"
#include "row0merge.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0ut.h"

#include <atomic>
#include <format>
#include <optional>
#include <random>
#include <thread>

using namespace std::chrono_literals;

namespace {

/** A row of the tables of these tests:
CREATE TABLE (k INT UNSIGNED NOT NULL, v INT UNSIGNED, s VARCHAR(4000), b BLOB,
  PRIMARY KEY(k), INDEX M_V(v), INDEX M_S(s)) the M_ indexes only if asked. */
struct MRow {
  uint32_t m_k{};
  std::optional<uint32_t> m_v{};
  std::optional<std::string> m_s{};
  std::string m_b{};
};

/** A field of an index record: nullopt for SQL NULL. */
using Field = std::optional<std::string>;

/** An index record, its fields in order, and its delete mark. */
struct Entry {
  std::vector<Field> m_fields;
  bool m_deleted{};

  bool operator==(const Entry &) const = default;
  auto operator<=>(const Entry &) const = default;
};

std::ostream &operator<<(std::ostream &os, const Entry &e) {
  os << "(";

  for (const auto &f : e.m_fields) {
    if (!f) {
      os << "NULL,";
    } else if (f->size() == 4) {
      os << mach_read_from_4(reinterpret_cast<const byte *>(f->data())) << ",";
    } else {
      os << "'" << f->substr(0, 8) << (f->size() > 8 ? "..." : "") << "'[" << f->size() << "],";
    }
  }

  return os << (e.m_deleted ? " del)" : ")");
}

/** @return the 4 byte big endian image of an INT UNSIGNED. */
std::string u32(uint32_t v) {
  std::string s(4, '\0');

  mach_write_to_4(reinterpret_cast<byte *>(s.data()), v);

  return s;
}

/** A Tpl destroyed at the end of the scope. */
struct Tpl {
  explicit Tpl(api::Tuple *tpl) : m_tpl(tpl) {}

  ~Tpl() { api::Tuple::destroy(m_tpl); }

  Tpl(const Tpl &) = delete;
  Tpl &operator=(const Tpl &) = delete;

  api::Tuple *operator->() const { return m_tpl; }

  api::Tuple *m_tpl;
};

/** A started instance shared by the tests of a suite; each test has tables
of its own, named after it. */
class MergeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    s_dir = new ut::TestDir();
    s_config = ut::make_config(s_dir->m_path);

    /* The log written at commit, flushed once a second: the commits don't
    wait for the disk. */
    s_config.m_log.m_flush_at_trx_commit = 2;

    /* WriterWaits waits for a whole index build, 26 s under TSan with a loaded machine. */
    s_config.m_lock.m_wait_timeout = std::chrono::seconds(300);

    s_sys = new System();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
    (void)api::database_create(*s_sys, "test");
  }

  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      delete s_sys;
      s_sys = nullptr;
    }

    delete s_dir;
    s_dir = nullptr;
  }

  void SetUp() override {
    ASSERT_NE(s_sys, nullptr);
    m_sys = s_sys;
  }

  /** @return test/<test name><suffix> */
  static std::string name(const char *suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  Trx *begin() { return api::trx_begin(*m_sys, IB_TRX_REPEATABLE_READ); }

  void commit(Trx *trx) { EXPECT_EQ(api::trx_commit(*m_sys, trx), DB_SUCCESS); }

  void rollback(Trx *trx) { EXPECT_EQ(api::trx_rollback(*m_sys, trx), DB_SUCCESS); }

  /** Create the table tbl, with the maintained indexes M_V and M_S if
  maintained, s of s_len (VARCHAR) bytes. */
  DbErr create(const std::string &tbl, bool maintained = false, ulint s_len = 4000) {
    api::TableSchema *sch{};
    api::IndexSchema *idx{};

    EXPECT_EQ(api::TableSchema::create(tbl.c_str(), &sch, IB_TBL_V1), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("k", IB_INT, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL), 0, 4), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("v", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("s", IB_VARCHAR, IB_COL_NONE, 0, s_len), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("b", IB_BLOB, IB_COL_NONE, 0, 0), DB_SUCCESS);

    EXPECT_EQ(sch->add_index("PRIMARY", &idx), DB_SUCCESS);
    EXPECT_EQ(idx->add_col("k", 0), DB_SUCCESS);
    EXPECT_EQ(idx->set_clustered(), DB_SUCCESS);

    if (maintained) {
      EXPECT_EQ(sch->add_index("M_V", &idx), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("v", 0), DB_SUCCESS);
      EXPECT_EQ(sch->add_index("M_S", &idx), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("s", 0), DB_SUCCESS);
      EXPECT_EQ(sch->add_index("M_SP", &idx), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("s", 10), DB_SUCCESS);
      EXPECT_EQ(sch->add_index("M_BP", &idx), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("b", 20), DB_SUCCESS);
      EXPECT_EQ(sch->add_index("M_VS", &idx), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("v", 0), DB_SUCCESS);
      EXPECT_EQ(idx->add_col("s", 0), DB_SUCCESS);
    }

    auto trx = begin();
    uint64_t id{};

    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

    auto err = api::table_create(*m_sys, trx, sch, &id);

    if (err == DB_SUCCESS) {
      commit(trx);
    } else {
      rollback(trx);
    }

    api::TableSchema::destroy(*m_sys, sch);

    return err;
  }

  /** Write row to tpl, a clustered index tuple. */
  static void write(api::Tuple *tpl, const MRow &row) {
    EXPECT_EQ(tpl->write_u32(0, row.m_k), DB_SUCCESS);

    if (row.m_v) {
      EXPECT_EQ(tpl->write_u32(1, *row.m_v), DB_SUCCESS);
    } else {
      EXPECT_EQ(tpl->set_value(1, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
    }

    if (row.m_s) {
      EXPECT_EQ(tpl->set_value(2, row.m_s->data(), row.m_s->size()), DB_SUCCESS);
    } else {
      EXPECT_EQ(tpl->set_value(2, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
    }

    EXPECT_EQ(tpl->set_value(3, row.m_b.data(), row.m_b.size()), DB_SUCCESS);
  }

  /** Insert the rows in one committed transaction. */
  void load(const std::string &tbl, const std::vector<MRow> &rows) {
    auto trx = begin();
    api::Cursor *crsr{};

    ASSERT_EQ(api::cursor_open_table(*m_sys, tbl.c_str(), trx, &crsr), DB_SUCCESS);

    /* Not an ASSERT: a transaction left active would hang the shutdown of the suite. */
    const auto err = crsr->lock(*m_sys, IB_LOCK_IX);

    EXPECT_EQ(err, DB_SUCCESS);

    if (err != DB_SUCCESS) {
      EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
      rollback(trx);
      return;
    }

    {
      Tpl tpl(crsr->clust_read_tuple_create());

      for (const auto &row : rows) {
        write(tpl.m_tpl, row);
        ASSERT_EQ(crsr->insert_row(*m_sys, tpl.m_tpl), DB_SUCCESS) << row.m_k;
        tpl.m_tpl = tpl->clear();
      }
    }

    EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
    commit(trx);
  }

  /** An index column: name and prefix length. */
  struct Col {
    const char *m_name;
    ulint m_prefix{};
  };

  /** CREATE [UNIQUE] INDEX idx ON tbl (cols) through the api. */
  DbErr create_index(const std::string &tbl, const char *idx, const std::vector<Col> &cols, bool unique = false) {
    auto trx = begin();
    api::IndexSchema *sch{};

    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

    auto err = api::IndexSchema::create(*m_sys, trx, idx, tbl.c_str(), &sch);

    if (err == DB_SUCCESS) {
      for (const auto &col : cols) {
        EXPECT_EQ(sch->add_col(col.m_name, col.m_prefix), DB_SUCCESS);
      }

      if (unique) {
        EXPECT_EQ(sch->set_unique(), DB_SUCCESS);
      }

      uint64_t id{};

      err = sch->create_index(*m_sys, &id);
      api::IndexSchema::destroy(sch);
    }

    if (err == DB_SUCCESS) {
      commit(trx);
    } else {
      rollback(trx);
    }

    return err;
  }

  /** @return the table in the dictionary cache. */
  Table *table(const std::string &tbl) {
    m_sys->m_dict->mutex_acquire();
    auto table = m_sys->m_dict->table_get(tbl.c_str());
    m_sys->m_dict->mutex_release();

    return table;
  }

  /** @return the index named idx of tbl, or nullptr. */
  Index *index(const std::string &tbl, const std::string &idx) {
    auto t = table(tbl);

    if (t == nullptr) {
      return nullptr;
    }

    for (auto index : t->m_indexes) {
      if (idx == index->m_name) {
        return index;
      }
    }

    return nullptr;
  }

  /** @return the records of index in order, delete marked too. */
  std::vector<Entry> entries(Index *index) {
    std::vector<Entry> v;
    Mtr mtr;
    BtreePcursor pcur;
    MemHeap *heap{};

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      auto rec = pcur.get_rec();
      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      Entry e;

      e.m_deleted = rec.get_deleted_flag();

      /* Not the DB_TRX_ID of a secondary record: who wrote it, not what it holds. */
      const auto n_fields = index->is_clustered() ? index->get_n_fields() : index->get_n_fields() - 1;

      for (ulint i{}; i < n_fields; ++i) {
        ulint len;
        auto f = rec.get_nth_field(offs, i, &len);

        if (len == UNIV_SQL_NULL) {
          e.m_fields.emplace_back();
        } else if (index->is_clustered() && i > 0) {
          /* The system columns and the rest: not compared. */
          e.m_fields.emplace_back(std::string());
        } else {
          e.m_fields.emplace_back(std::string(reinterpret_cast<const char *>(f.get()), len));
        }
      }

      if (!index->is_clustered()) {
        v.push_back(std::move(e));
      } else {
        e.m_fields.resize(1);
        v.push_back(std::move(e));
      }
    }

    pcur.close();
    mtr.commit(*m_sys);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    return v;
  }

  /** The expected entries of an index on cols, from rows, in order. */
  static std::vector<Entry> expected(const std::vector<MRow> &rows, const std::vector<Col> &cols) {
    std::vector<Entry> v;

    for (const auto &row : rows) {
      Entry e;

      for (const auto &col : cols) {
        const std::string name = col.m_name;

        if (name == "v") {
          e.m_fields.push_back(row.m_v ? Field(u32(*row.m_v)) : Field());
        } else if (name == "s") {
          if (row.m_s) {
            e.m_fields.push_back(col.m_prefix > 0 ? row.m_s->substr(0, col.m_prefix) : *row.m_s);
          } else {
            e.m_fields.emplace_back();
          }
        } else if (name == "b") {
          e.m_fields.push_back(col.m_prefix > 0 ? row.m_b.substr(0, col.m_prefix) : row.m_b);
        } else if (name == "k") {
          e.m_fields.push_back(u32(row.m_k));
        }
      }

      /* The primary key, unless it is in the index already. */
      bool has_k{};

      for (const auto &col : cols) {
        has_k |= std::string(col.m_name) == "k";
      }

      if (!has_k) {
        e.m_fields.push_back(u32(row.m_k));
      }

      v.push_back(std::move(e));
    }

    /* NULL first, then bytewise: the test values are lower case letters. */
    std::ranges::sort(v);

    return v;
  }

  /** Check the index idx of tbl: valid, and exactly the entries of rows. */
  void expect_index(const std::string &tbl, const char *idx, const std::vector<MRow> &rows, const std::vector<Col> &cols) {
    auto ix = index(tbl, idx);

    ASSERT_NE(ix, nullptr) << idx;

    auto trx = begin();

    EXPECT_TRUE(m_sys->am(ix).validate(ix, trx)) << idx;
    commit(trx);

    const auto got = entries(ix);
    const auto exp = expected(rows, cols);

    EXPECT_EQ(got.size(), exp.size()) << idx;

    for (size_t i{}; i < std::min(got.size(), exp.size()); ++i) {
      if (!(got[i] == exp[i])) {
        ADD_FAILURE() << idx << " entry " << i << ": " << got[i] << " expected " << exp[i];
        break;
      }
    }
  }

  /** Read the index idx of tbl through the api with cluster access: the
  k of the rows in index order. */
  std::vector<uint32_t> read_index(const std::string &tbl, const char *idx) {
    std::vector<uint32_t> ks;
    auto trx = begin();
    api::Cursor *crsr{};
    api::Cursor *sec{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, tbl.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IS), DB_SUCCESS);

    if (auto err = crsr->open_index(*m_sys, idx, &sec); err == DB_SUCCESS) {
      sec->set_cluster_access();

      Tpl tpl(sec->clust_read_tuple_create());

      for (auto err = sec->first(*m_sys); err == DB_SUCCESS; err = sec->next(*m_sys)) {
        uint32_t k{};

        EXPECT_EQ(sec->read_row(*m_sys, tpl.m_tpl), DB_SUCCESS);
        EXPECT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
        ks.push_back(k);
        tpl.m_tpl = tpl->clear();
      }

      EXPECT_EQ(api::Cursor::close(*m_sys, sec), DB_SUCCESS);
    } else {
      ADD_FAILURE() << "open_index " << idx << ": " << err;
    }

    EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
    commit(trx);

    return ks;
  }

  /** Lock the table tbl in S mode for trx, as the index build does. */
  void lock_s(Trx *trx, const std::string &tbl) {
    api::Cursor *crsr{};

    ASSERT_EQ(api::cursor_open_table(*m_sys, tbl.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_S), DB_SUCCESS);
    EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  }

  /** Build the indexes defs of tbl in one row_merge_build_indexes() call,
  as api::create_secondary_index() does for one: create them (temporary
  names) in a dictionary transaction, build them with the table S locked,
  then rename them, or drop them on an error.
  @return the error of the build */
  DbErr build(const std::string &tbl, std::vector<MergeIndexDef> defs) {
    auto t = table(tbl);

    EXPECT_NE(t, nullptr);

    auto usr = begin();

    lock_s(usr, tbl);

    auto ddl = m_sys->m_trx_sys->create_user_trx(nullptr);

    EXPECT_TRUE(ddl->start(*m_sys, ULINT_UNDEFINED));

    std::vector<std::string> names;
    std::vector<Index *> indexes;

    for (auto &def : defs) {
      names.push_back(std::string(1, TEMP_INDEX_PREFIX) + def.name);
    }

    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, usr), DB_SUCCESS);
    ddl->set_dict_operation(TRX_DICT_OP_INDEX);

    for (size_t i{}; i < defs.size(); ++i) {
      defs[i].name = names[i].c_str();

      auto index = row_merge_create_index(*m_sys, ddl, t, &defs[i]);

      EXPECT_NE(index, nullptr) << defs[i].name;
      indexes.push_back(index);
    }

    EXPECT_EQ(api::schema_unlock(*m_sys, usr), DB_SUCCESS);
    EXPECT_EQ(ddl->commit(*m_sys), DB_SUCCESS);
    m_sys->m_trx_sys->destroy_user_trx(ddl);

    auto err = row_merge_build_indexes(*m_sys, usr, t, t, indexes.data(), indexes.size(), nullptr);

    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, usr), DB_SUCCESS);

    if (err == DB_SUCCESS) {
      EXPECT_EQ(row_merge_rename_indexes(*m_sys, usr, t), DB_SUCCESS);
    } else {
      row_merge_drop_indexes(*m_sys, usr, t, indexes.data(), indexes.size());
    }

    EXPECT_EQ(api::schema_unlock(*m_sys, usr), DB_SUCCESS);
    commit(usr);

    return err;
  }

  /** The rows [0, n) in a random (but fixed) key order, v random with
  NULLs every null_every rows (0: never), s of 1..s_max bytes, b of b_len. */
  static std::vector<MRow> make_rows(uint32_t n, uint32_t null_every = 0, size_t s_max = 20, size_t b_len = 10, uint32_t seed = 1) {
    std::vector<MRow> rows;
    std::mt19937 rng(seed);

    for (uint32_t k = 0; k < n; ++k) {
      MRow row;

      row.m_k = k;

      if (null_every == 0 || k % null_every != 0) {
        row.m_v = rng() % (n * 4 + 1);
        row.m_s = ut::blob(rng(), 1 + rng() % s_max);
      }

      row.m_b = ut::blob(k, b_len);
      rows.push_back(std::move(row));
    }

    std::ranges::shuffle(rows, rng);

    return rows;
  }

  static inline ut::TestDir *s_dir{};
  static inline Config s_config{};
  static inline System *s_sys{};

  System *m_sys{};
};

/** An index on an empty table: valid, empty; the rows inserted after it
are in it. */
TEST_F(MergeTest, EmptyTable) {
  ASSERT_EQ(create(name()), DB_SUCCESS);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);

  expect_index(name(), "V", {}, {{"v"}});

  const auto rows = make_rows(100);

  load(name(), rows);
  expect_index(name(), "V", rows, {{"v"}});
}

/** One row. */
TEST_F(MergeTest, OneRow) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const std::vector<MRow> rows{{7, 70, "x", "b"}};

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  expect_index(name(), "V", rows, {{"v"}});
  EXPECT_EQ(read_index(name(), "V"), std::vector<uint32_t>{7});
}

/** Rows in random key order, random values: the index holds them all, in
(v, k) order, and reads them back with cluster access. */
TEST_F(MergeTest, ManyRows) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(10000);

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  expect_index(name(), "V", rows, {{"v"}});

  auto by_v = rows;

  std::ranges::sort(by_v, [](const MRow &a, const MRow &b) { return std::pair(*a.m_v, a.m_k) < std::pair(*b.m_v, b.m_k); });

  std::vector<uint32_t> ks;

  for (const auto &row : by_v) {
    ks.push_back(row.m_k);
  }

  EXPECT_EQ(read_index(name(), "V"), ks);
}

/** NULL keys sort first, each its own entry. */
TEST_F(MergeTest, Nulls) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(3000, 3);

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  ASSERT_EQ(create_index(name(), "S", {{"s"}}), DB_SUCCESS);
  expect_index(name(), "V", rows, {{"v"}});
  expect_index(name(), "S", rows, {{"s"}});
}

/** All keys NULL. */
TEST_F(MergeTest, AllNulls) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(1000, 1);

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  expect_index(name(), "V", rows, {{"v"}});
}

/** All keys equal: a non-unique index sorts them by the primary key. */
TEST_F(MergeTest, AllEqual) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  auto rows = make_rows(5000);

  for (auto &row : rows) {
    row.m_v = 42;
  }

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  expect_index(name(), "V", rows, {{"v"}});
}

/** A unique index on distinct values with NULLs: NULLs are not duplicates. */
TEST_F(MergeTest, UniqueWithNulls) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  auto rows = make_rows(2000, 4);

  for (auto &row : rows) {
    if (row.m_v) {
      row.m_v = row.m_k;
    }
  }

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "U", {{"v"}}, true), DB_SUCCESS);
  expect_index(name(), "U", rows, {{"v"}});
}

/** A unique index on duplicate values: DB_DUPLICATE_KEY and no index is
left, the table is unchanged and can get the index once the duplicate is
gone. */
TEST_F(MergeTest, UniqueDuplicate) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  auto rows = make_rows(500);

  for (auto &row : rows) {
    row.m_v = row.m_k;
  }

  rows[100].m_v = 7;

  load(name(), rows);
  EXPECT_EQ(create_index(name(), "U", {{"v"}}, true), DB_DUPLICATE_KEY);
  EXPECT_EQ(index(name(), "U"), nullptr);
  EXPECT_EQ(index(name(), std::string(1, TEMP_INDEX_PREFIX) + "U"), nullptr);
  EXPECT_EQ(UT_LIST_GET_LEN(table(name())->m_indexes), 1UL);
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));

  /* Remove the duplicate: the index builds. */
  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, name().c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);
  ASSERT_EQ(crsr->set_lock_mode(*m_sys, IB_LOCK_X), DB_SUCCESS);

  {
    Tpl key(crsr->clust_search_tuple_create());
    int res{};

    ASSERT_EQ(key->write_u32(0, rows[100].m_k), DB_SUCCESS);
    ASSERT_EQ(crsr->moveto(*m_sys, key.m_tpl, IB_CUR_GE, &res), DB_SUCCESS);
    ASSERT_EQ(res, 0);
    ASSERT_EQ(crsr->delete_row(*m_sys), DB_SUCCESS);
  }

  EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  commit(trx);

  rows.erase(rows.begin() + 100);

  ASSERT_EQ(create_index(name(), "U", {{"v"}}, true), DB_SUCCESS);
  expect_index(name(), "U", rows, {{"v"}});
}

/** Duplicates at the two ends of the order. */
TEST_F(MergeTest, UniqueDuplicateEnds) {
  for (uint32_t which : {0u, 1u}) {
    const auto tbl = name(which == 0 ? "_lo" : "_hi");

    ASSERT_EQ(create(tbl), DB_SUCCESS);

    auto rows = make_rows(300);

    for (auto &row : rows) {
      row.m_v = row.m_k + 10;
    }

    /* Two rows with the smallest, or the largest value. */
    for (auto &row : rows) {
      if (row.m_k == 0 || row.m_k == 299) {
        row.m_v = which == 0 ? 0 : 1000000;
      }
    }

    load(tbl, rows);
    EXPECT_EQ(create_index(tbl, "U", {{"v"}}, true), DB_DUPLICATE_KEY) << tbl;
    EXPECT_EQ(index(tbl, "U"), nullptr);
  }
}

/** Long variable length keys: s of up to 3000 bytes, so many runs of a
merge block (1 MB) each, records spanning the blocks of the merge file,
merged over several passes. The number of runs varies, odd and even: the
merge of a pass pairs the blocks of the two halves of the file. */
TEST_F(MergeTest, MultiRun) {
  for (uint32_t n : {700u, 1500u, 2700u}) {
    const auto tbl = name(std::format("_{}", n).c_str());

    ASSERT_EQ(create(tbl), DB_SUCCESS);

    const auto rows = make_rows(n, 0, 3000, 10, n);

    load(tbl, rows);
    ASSERT_EQ(create_index(tbl, "S", {{"s"}}), DB_SUCCESS) << n;
    expect_index(tbl, "S", rows, {{"s"}});
  }
}

/** Many runs of small records: v and s of 1..40 bytes in 80000 rows, a
merge file of several blocks. */
TEST_F(MergeTest, ManySmallRuns) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(80000, 0, 40, 1);

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "VS", {{"v"}, {"s"}}), DB_SUCCESS);
  expect_index(name(), "VS", rows, {{"v"}, {"s"}});
}

/** A unique index on long keys with a duplicate pair in different runs:
the duplicate is found by the merge of the runs, not by the sort of a
buffer. */
TEST_F(MergeTest, UniqueDuplicateAcrossRuns) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  std::vector<MRow> rows;

  for (uint32_t k = 0; k < 2000; ++k) {
    rows.push_back(MRow{k, k, ut::blob(k, 2000), ""});
  }

  /* The first and the last row (the clustered index order, the order the
  runs are made in) have the same key. */
  rows.back().m_s = rows.front().m_s;

  load(name(), rows);
  EXPECT_EQ(create_index(name(), "U", {{"s"}}, true), DB_DUPLICATE_KEY);
  EXPECT_EQ(index(name(), "U"), nullptr);
}

/** BUG: a unique index on a column with NULLs in different runs fails with
DB_DUPLICATE_KEY: NULLs are not equal for uniqueness and the sort of a
buffer knows it (row_merge_tuple_cmp() skips them), but the merge of two
runs, row_merge_blocks() (row/row0merge.cc:1187), returns DB_DUPLICATE_KEY
on any comparison of 0, NULLs included. The same upstream (7c21eac0); MySQL
fixed it later (cmp_rec_rec_simple() reports the NULL equality). */
TEST_F(MergeTest, UniqueNullsAcrossRuns) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  std::vector<MRow> rows;

  for (uint32_t k = 0; k < 2000; ++k) {
    rows.push_back(MRow{k, k, ut::blob(k, 2000), ""});
  }

  /* NULL in the first run and in the last. */
  rows.front().m_s.reset();
  rows.back().m_s.reset();

  load(name(), rows);
  EXPECT_EQ(create_index(name(), "U", {{"s"}}, true), DB_SUCCESS);
  expect_index(name(), "U", rows, {{"s"}});
}

/** Column prefixes, of a VARCHAR and of a BLOB stored externally: the built
index equals the one the inserts maintained. */
TEST_F(MergeTest, Prefixes) {
  ASSERT_EQ(create(name(), true), DB_SUCCESS);

  auto rows = make_rows(1500, 7, 100, 30);

  for (auto &row : rows) {
    if (row.m_k % 10 == 0) {
      row.m_b = ut::blob(row.m_k + 3, 20000);
    }
  }

  load(name(), rows);

  ASSERT_EQ(create_index(name(), "SP", {{"s", 10}}), DB_SUCCESS);
  ASSERT_EQ(create_index(name(), "BP", {{"b", 20}}), DB_SUCCESS);

  expect_index(name(), "SP", rows, {{"s", 10}});
  expect_index(name(), "BP", rows, {{"b", 20}});

  EXPECT_EQ(entries(index(name(), "SP")), entries(index(name(), "M_SP")));
  EXPECT_EQ(entries(index(name(), "BP")), entries(index(name(), "M_BP")));
}

/** Every index the inserts maintained, built again: identical record for
record. */
TEST_F(MergeTest, SameAsMaintained) {
  ASSERT_EQ(create(name(), true), DB_SUCCESS);

  const auto rows = make_rows(8000, 5, 300, 20);

  load(name(), rows);

  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  ASSERT_EQ(create_index(name(), "S", {{"s"}}), DB_SUCCESS);
  ASSERT_EQ(create_index(name(), "VS", {{"v"}, {"s"}}), DB_SUCCESS);

  EXPECT_EQ(entries(index(name(), "V")), entries(index(name(), "M_V")));
  EXPECT_EQ(entries(index(name(), "S")), entries(index(name(), "M_S")));
  EXPECT_EQ(entries(index(name(), "VS")), entries(index(name(), "M_VS")));
  expect_index(name(), "VS", rows, {{"v"}, {"s"}});
}

/** The indexes built and the rows changed after: they are maintained like
the others. */
TEST_F(MergeTest, MaintainedAfter) {
  ASSERT_EQ(create(name(), true), DB_SUCCESS);

  auto rows = make_rows(2000, 9, 50);

  load(name(), rows);
  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);

  auto more = make_rows(500, 4, 50, 10, 99);

  for (auto &row : more) {
    row.m_k += 100000;
  }

  load(name(), more);
  rows.insert(rows.end(), more.begin(), more.end());

  expect_index(name(), "V", rows, {{"v"}});
  EXPECT_EQ(entries(index(name(), "V")), entries(index(name(), "M_V")));
}

/** Delete marked rows (deleted, not yet purged: a read view holds purge
back) are not in the index built. */
TEST_F(MergeTest, SkipsDeleted) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(1000);

  load(name(), rows);

  auto blocker = begin();

  ASSERT_NE(blocker->assign_read_view(*m_sys), nullptr);

  auto trx = begin();
  api::Cursor *crsr{};
  std::vector<MRow> kept;

  ASSERT_EQ(api::cursor_open_table(*m_sys, name().c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);
  ASSERT_EQ(crsr->set_lock_mode(*m_sys, IB_LOCK_X), DB_SUCCESS);

  for (const auto &row : rows) {
    if (row.m_k % 3 != 0) {
      kept.push_back(row);
      continue;
    }

    Tpl key(crsr->clust_search_tuple_create());
    int res{};

    ASSERT_EQ(key->write_u32(0, row.m_k), DB_SUCCESS);
    ASSERT_EQ(crsr->moveto(*m_sys, key.m_tpl, IB_CUR_GE, &res), DB_SUCCESS);
    ASSERT_EQ(res, 0);
    ASSERT_EQ(crsr->delete_row(*m_sys), DB_SUCCESS);
  }

  EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  commit(trx);

  ASSERT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  expect_index(name(), "V", kept, {{"v"}});

  /* The old view must not use the new index. */
  EXPECT_FALSE(row_merge_is_index_usable(blocker, index(name(), "V")));
  commit(blocker);

  auto fresh = begin();

  ASSERT_NE(fresh->assign_read_view(*m_sys), nullptr);
  EXPECT_TRUE(row_merge_is_index_usable(fresh, index(name(), "V")));
  commit(fresh);
}

/** Several indexes built by one row_merge_build_indexes(): one scan of the
clustered index fills a merge file per index. */
TEST_F(MergeTest, BuildSeveral) {
  ASSERT_EQ(create(name(), true), DB_SUCCESS);

  const auto rows = make_rows(4000, 6, 500, 20);

  load(name(), rows);

  MergeIndexField v{0, "v"};
  MergeIndexField s{0, "s"};
  MergeIndexField sp{5, "s"};
  MergeIndexField vs[2]{{0, "v"}, {0, "s"}};

  ASSERT_EQ(build(name(), {{"V", 0, 1, &v}, {"S", 0, 1, &s}, {"SP", 0, 1, &sp}, {"VS", 0, 2, vs}}), DB_SUCCESS);

  expect_index(name(), "V", rows, {{"v"}});
  expect_index(name(), "S", rows, {{"s"}});
  expect_index(name(), "SP", rows, {{"s", 5}});
  expect_index(name(), "VS", rows, {{"v"}, {"s"}});
  EXPECT_EQ(entries(index(name(), "VS")), entries(index(name(), "M_VS")));
  EXPECT_EQ(UT_LIST_GET_LEN(table(name())->m_indexes), 10UL);
}

/** Several indexes, one unique with a duplicate: the build fails with
DB_DUPLICATE_KEY, the error key is the unique one, all are dropped. */
TEST_F(MergeTest, BuildSeveralDuplicate) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  auto rows = make_rows(1000);

  for (auto &row : rows) {
    row.m_v = row.m_k % 999;
  }

  load(name(), rows);

  MergeIndexField s{0, "s"};
  MergeIndexField v{0, "v"};

  EXPECT_EQ(build(name(), {{"S", 0, 1, &s}, {"V", DICT_UNIQUE, 1, &v}}), DB_DUPLICATE_KEY);

  EXPECT_EQ(index(name(), "S"), nullptr);
  EXPECT_EQ(index(name(), "V"), nullptr);
  EXPECT_EQ(UT_LIST_GET_LEN(table(name())->m_indexes), 1UL);
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
}

/** Readers scan the table while an index is built (the build S locks the
table, the readers IS): no hang, no error, the index is right. */
TEST_F(MergeTest, ConcurrentReaders) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  const auto rows = make_rows(30000, 0, 200);

  load(name(), rows);

  std::atomic<bool> stop{};
  std::atomic<size_t> n_scans{};
  std::vector<std::thread> readers;

  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&] {
      while (!stop.load()) {
        auto trx = begin();
        api::Cursor *crsr{};
        size_t n{};

        EXPECT_EQ(api::cursor_open_table(*m_sys, name().c_str(), trx, &crsr), DB_SUCCESS);
        EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IS), DB_SUCCESS);

        for (auto err = crsr->first(*m_sys); err == DB_SUCCESS; err = crsr->next(*m_sys)) {
          ++n;
        }

        EXPECT_EQ(n, rows.size());
        EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
        commit(trx);
        ++n_scans;
      }
    });
  }

  EXPECT_EQ(create_index(name(), "S", {{"s"}}), DB_SUCCESS);

  stop = true;

  for (auto &t : readers) {
    t.join();
  }

  expect_index(name(), "S", rows, {{"s"}});
}

/** A writer waits for the build (the table is S locked by it) and its row is
in the index after. */
TEST_F(MergeTest, WriterWaits) {
  ASSERT_EQ(create(name()), DB_SUCCESS);

  auto rows = make_rows(20000, 0, 200);

  load(name(), rows);

  std::atomic<bool> started{};
  std::thread writer([&] {
    while (!started.load()) {
      std::this_thread::yield();
    }

    std::this_thread::sleep_for(20ms);

    load(name(), {{900000, 1, "w", "w"}});
  });

  started = true;
  EXPECT_EQ(create_index(name(), "V", {{"v"}}), DB_SUCCESS);
  writer.join();

  rows.push_back({900000, 1, "w", "w"});
  expect_index(name(), "V", rows, {{"v"}});
}

/** Instances of their own. */
class MergeCrashTest : public ut::DbTest {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_log.m_flush_at_trx_commit = 2;
    start();
  }
};

/** A crash after an index was created (in the dictionary, with its temporary
name) and before it was renamed: the restart drops it, and it can be
created again. */
TEST_F(MergeCrashTest, TempIndexDropped) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 500; ++k) {
    rows[k] = ut::Row{k, k % 13, ut::blob(k, 30)};
  }

  load(rows);

  const std::string temp = std::string(1, TEMP_INDEX_PREFIX) + "T_A";

  crash([&] {
    m_sys->m_dict->mutex_acquire();
    auto t = m_sys->m_dict->table_get(ut::TABLE);
    m_sys->m_dict->mutex_release();

    auto usr = begin();
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, ut::TABLE, usr, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_S), DB_SUCCESS);
    EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);

    auto ddl = m_sys->m_trx_sys->create_user_trx(nullptr);

    EXPECT_TRUE(ddl->start(*m_sys, ULINT_UNDEFINED));
    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, usr), DB_SUCCESS);
    ddl->set_dict_operation(TRX_DICT_OP_INDEX);

    MergeIndexField a{0, "a"};
    MergeIndexDef def{temp.c_str(), 0, 1, &a};
    auto index = row_merge_create_index(*m_sys, ddl, t, &def);

    EXPECT_NE(index, nullptr);
    EXPECT_EQ(api::schema_unlock(*m_sys, usr), DB_SUCCESS);
    EXPECT_EQ(ddl->commit(*m_sys), DB_SUCCESS);

    /* Half built: the build is in progress when the process dies. */
    EXPECT_EQ(row_merge_build_indexes(*m_sys, usr, t, t, &index, 1, nullptr), DB_SUCCESS);
  });

  m_sys->m_dict->mutex_acquire();
  auto t = m_sys->m_dict->table_get(ut::TABLE);
  m_sys->m_dict->mutex_release();

  ASSERT_NE(t, nullptr);

  for (auto index : t->m_indexes) {
    EXPECT_NE(*index->m_name, TEMP_INDEX_PREFIX) << "a temporary index is left";
  }

  EXPECT_EQ(UT_LIST_GET_LEN(t->m_indexes), 2UL);
  expect_rows(rows);
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));

  /* The index can be created again, under its final name. */
  auto trx = begin();
  api::IndexSchema *sch{};

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::IndexSchema::create(*m_sys, trx, "T_A", ut::TABLE, &sch), DB_SUCCESS);
  EXPECT_EQ(sch->add_col("a", 0), DB_SUCCESS);

  uint64_t id{};

  EXPECT_EQ(sch->create_index(*m_sys, &id), DB_SUCCESS);
  api::IndexSchema::destroy(sch);
  commit(trx);

  restart();
  expect_rows(rows);
}

/** Index builds, timed. */
using MergePerf = MergeTest;

/** Rows indexed per second by the build of a small (INT) index, for several
table sizes: the cost per row should stay about flat. */
TEST_F(MergePerf, BuildInt) {
  for (uint32_t n : {10000u, 50000u, 200000u}) {
    const auto tbl = name(std::format("_{}", n).c_str());

    ASSERT_EQ(create(tbl), DB_SUCCESS);
    load(tbl, make_rows(n, 0, 8, 1));

    const auto start = std::chrono::steady_clock::now();

    ASSERT_EQ(create_index(tbl, "V", {{"v"}}), DB_SUCCESS);

    const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
    ut::BenchResult r{std::format("build_int_index_{}_rows", n), 1, n, secs.count()};

    ut::report(r);
    EXPECT_LT(r.ns_per_op(), 200000);
  }
}

/** Rows indexed per second by the build of a wide (VARCHAR 1000) index: a
merge of many runs. */
TEST_F(MergePerf, BuildWide) {
  constexpr uint32_t N = 10000;

  ASSERT_EQ(create(name()), DB_SUCCESS);
  load(name(), make_rows(N, 0, 1000, 1));

  const auto start = std::chrono::steady_clock::now();

  ASSERT_EQ(create_index(name(), "S", {{"s"}}), DB_SUCCESS);

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
  ut::BenchResult r{"build_varchar_index_rows", 1, N, secs.count()};

  ut::report(r);
  EXPECT_LT(r.ns_per_op(), 1000000);
}

/** The same index maintained by the inserts, for comparison: the cost of
an insert with the index minus one without. */
TEST_F(MergePerf, InsertMaintained) {
  constexpr uint32_t N = 50000;
  const auto rows = make_rows(N, 0, 8, 1);

  ASSERT_EQ(create(name("_plain")), DB_SUCCESS);
  ASSERT_EQ(create(name("_indexed")), DB_SUCCESS);
  ASSERT_EQ(create_index(name("_indexed"), "V", {{"v"}}), DB_SUCCESS);

  for (const char *suffix : {"_plain", "_indexed"}) {
    const auto start = std::chrono::steady_clock::now();

    load(name(suffix), rows);

    const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
    ut::BenchResult r{std::format("insert{}_rows", suffix), 1, N, secs.count()};

    ut::report(r);
    EXPECT_LT(r.ns_per_op(), 1000000);
  }
}

}  // namespace
