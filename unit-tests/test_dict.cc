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

/** @file unit-tests/test_dict.cc
Dict: the dictionary cache, table statistics and DDL that changes a table
others have open. Then, on an instance shared by a suite (DictDb): the cache
(get by name and id, handle counts, the SYS_ tables), the layout of a table and
its indexes, loading from the SYS_ tables, ids, names, limits, statistics,
TRUNCATE, the background drop list, DDL with DML, foreign keys; and their
performance (DictPerf). */

#include "ut0env.h"
#include "ut0perf.h"

#include "data0type.h"
#include "ddl0ddl.h"
#include "dict0dict.h"
#include "pars0pars.h"
#include "que0que.h"
#include "ut0ut.h"
#include "trx0sys.h"

#include <atomic>
#include <format>
#include <future>
#include <thread>

using namespace std::chrono_literals;

namespace {

using DictTest = ut::DbTest;

Table *get(System &sys, const char *name) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(name);
  sys.m_dict->mutex_release();

  return table;
}

/** The test table is in the cache with its two indexes. */
TEST_F(DictTest, Cache) {
  auto table = get(*m_sys, ut::TABLE);

  ASSERT_NE(table, nullptr);
  EXPECT_NE(table->get_first_index(), nullptr);
  EXPECT_TRUE(table->get_first_index()->is_clustered());
  EXPECT_NE(table->get_index_on_name(ut::SEC_A), nullptr);
  EXPECT_EQ(table->get_index_on_name("NONE"), nullptr);

  EXPECT_EQ(get(*m_sys, "test/none"), nullptr);
}

/** The table and its space are found again after a restart. */
TEST_F(DictTest, Reload) {
  const auto space_id = get(*m_sys, ut::TABLE)->m_space_id;

  restart();

  auto table = get(*m_sys, ut::TABLE);

  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->m_space_id, space_id);
}

/** Statistics follow the size of the table. */
TEST_F(DictTest, Statistics) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 2000; ++k) {
    rows[k] = ut::Row{k, k % 50, ut::blob(k, 200)};
  }

  load(rows);

  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->update_table_statistics(*m_sys), DB_SUCCESS);

  api::TableStats stats{};

  ASSERT_EQ(crsr->get_table_statistics(*m_sys, &stats), DB_SUCCESS);

  /* An estimate from a few sampled pages. */
  EXPECT_GT(stats.m_stat_n_rows, 500);
  EXPECT_LT(stats.m_stat_n_rows, 8000);
  EXPECT_GT(stats.m_stat_clustered_index_size, UNIV_PAGE_SIZE);
  EXPECT_GT(stats.m_stat_sum_of_other_index_sizes, 0UL);

  EXPECT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
  commit(trx);
}

/** TRUNCATE while other threads open the table and compute its statistics.
The statistics were computed without anything that excluded TRUNCATE,
which frees the tablespace: they walked a freed space. */
TEST_F(DictTest, TruncateWhileOpening) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 200; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 100)};
  }

  load(rows);

  std::atomic<bool> done{};
  std::vector<std::future<void>> openers;

  for (int t = 0; t < 4; ++t) {
    openers.push_back(std::async(std::launch::async, [&] {
      while (!done) {
        auto trx = begin();
        api::Cursor *crsr{};

        if (api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr) == DB_SUCCESS) {
          (void)crsr->update_table_statistics(*m_sys);

          api::TableStats stats{};

          (void)crsr->get_table_statistics(*m_sys, &stats);
          (void)api::Cursor::close(*m_sys, crsr);
        }

        rollback(trx);
      }
    }));
  }

  const auto end = std::chrono::steady_clock::now() + 2s;
  int n_truncates{};

  while (std::chrono::steady_clock::now() < end) {
    uint64_t table_id{};

    const auto err = api::table_truncate(*m_sys, ut::TABLE, &table_id);

    EXPECT_TRUE(err == DB_SUCCESS || err == DB_LOCK_WAIT_TIMEOUT || err == DB_DEADLOCK) << err;

    if (err == DB_SUCCESS) {
      ++n_truncates;
    }

    auto trx = begin();

    for (uint32_t k = 0; k < 50; ++k) {
      (void)insert(trx, rows[k]);
    }

    commit(trx);
  }

  done = true;

  for (auto &f : openers) {
    f.get();
  }

  EXPECT_GT(n_truncates, 0);
}

/** Drop and create the table again, also with rows in it. */
TEST_F(DictTest, DropCreate) {
  load({{1, ut::Row{1, 1, "x"}}});

  for (int i = 0; i < 3; ++i) {
    auto trx = begin();

    ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
    ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
    commit(trx);

    EXPECT_EQ(get(*m_sys, ut::TABLE), nullptr);

    stop();
    start(true);

    expect_rows({});
    load({{2, ut::Row{2, 2, "y"}}});
    expect_rows({{2, ut::Row{2, 2, "y"}}});

    trx = begin();
    ASSERT_EQ(remove(trx, 2), DB_SUCCESS);
    commit(trx);
  }
}


/* ------------------------------------------------------------------------
Fine grained tests on one instance shared by the suite (starting and stopping
one costs seconds). Each test works on tables of its own, named after it. */

/** A column of a table to create. */
struct ColDef {
  std::string m_name;
  ColType m_type;
  ColAttr m_attr;
  ulint m_len;
};

/** An index of a table to create, the first one is clustered if clustered is set. */
struct IdxDef {
  std::string m_name;
  std::vector<std::string> m_cols;
  bool m_clustered{};
};

/** CREATE TABLE through the API, in a trx of its own. */
DbErr create(System &sys, const std::string &name, const std::vector<ColDef> &cols, const std::vector<IdxDef> &idxs) {
  api::TableSchema *tbl_sch{};

  if (auto err = api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1, 0); err != DB_SUCCESS) {
    return err;
  }

  auto err = DB_SUCCESS;

  for (const auto &col : cols) {
    if (err == DB_SUCCESS) {
      err = tbl_sch->add_col(col.m_name.c_str(), col.m_type, col.m_attr, 0, col.m_len);
    }
  }

  for (const auto &idx : idxs) {
    api::IndexSchema *idx_sch{};

    if (err == DB_SUCCESS) {
      err = tbl_sch->add_index(idx.m_name.c_str(), &idx_sch);
    }

    for (const auto &col : idx.m_cols) {
      if (err == DB_SUCCESS) {
        err = idx_sch->add_col(col.c_str(), 0);
      }
    }

    if (err == DB_SUCCESS && idx.m_clustered) {
      err = idx_sch->set_clustered();
    }
  }

  if (err == DB_SUCCESS) {
    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    uint64_t id{};

    err = api::schema_lock_exclusive(sys, trx);

    if (err == DB_SUCCESS) {
      err = api::table_create(sys, trx, tbl_sch, &id);
    }

    if (err == DB_SUCCESS) {
      EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
    } else {
      EXPECT_EQ(api::trx_rollback(sys, trx), DB_SUCCESS);
    }
  }

  api::TableSchema::destroy(sys, tbl_sch);

  return err;
}

/** CREATE TABLE name (k INT UNSIGNED, v INT UNSIGNED, PRIMARY KEY(k), INDEX V(v)) */
DbErr create_kv(System &sys, const std::string &name) {
  return create(
    sys, name, {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"v", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {"k"}, true}, {"V", {"v"}}}
  );
}

/** DROP TABLE name in a trx of its own. */
DbErr drop(System &sys, const std::string &name) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

  EXPECT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);

  const auto err = api::table_drop(sys, trx, name.c_str());

  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  return err;
}

/** Insert (k, v) for k in [from, to) into a create_kv() table, one trx. */
void load_kv(System &sys, const std::string &name, uint32_t from, uint32_t to) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(sys, name.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(sys, IB_LOCK_IX), DB_SUCCESS);

  auto tpl = crsr->clust_read_tuple_create();

  for (auto k = from; k < to; ++k) {
    ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    ASSERT_EQ(tpl->write_u32(1, k % 10), DB_SUCCESS);
    ASSERT_EQ(crsr->insert_row(sys, tpl), DB_SUCCESS);
  }

  api::Tuple::destroy(tpl);
  ASSERT_EQ(api::Cursor::close(sys, crsr), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
}

/** @return the number of rows of name. */
uint64_t count(System &sys, const std::string &name) {
  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};
  uint64_t n{};

  EXPECT_EQ(api::cursor_open_table(sys, name.c_str(), trx, &crsr), DB_SUCCESS);

  if (crsr != nullptr) {
    for (auto err = crsr->first(sys); err == DB_SUCCESS; err = crsr->next(sys)) {
      ++n;
    }

    EXPECT_EQ(api::Cursor::close(sys, crsr), DB_SUCCESS);
  }

  EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  return n;
}

/** What the dictionary knows of a table, to compare a cached and a reloaded table. */
struct Def {
  struct Col {
    std::string m_name;
    ulint m_mtype, m_prtype, m_len;
    auto operator<=>(const Col &) const = default;
  };

  struct Idx {
    std::string m_name;
    Dict_id m_id;
    ulint m_type, m_n_fields, m_n_uniq, m_n_user;
    page_no_t m_root;
    space_id_t m_space;
    std::vector<std::string> m_fields;
    auto operator<=>(const Idx &) const = default;
  };

  std::string m_name;
  Dict_id m_id;
  space_id_t m_space;
  ulint m_flags;
  std::vector<Col> m_cols;
  std::vector<Idx> m_idxs;

  auto operator<=>(const Def &) const = default;
};

/** @return the definition of table, from the cache. */
Def def(const Table *table) {
  Def d{table->m_name, table->m_id, table->m_space_id, table->m_flags, {}, {}};

  for (ulint i{}; i < table->get_n_cols(); ++i) {
    const auto col = table->get_nth_col(i);

    d.m_cols.push_back({table->get_col_name(i), col->mtype, col->prtype, col->len});
  }

  for (auto index : table->m_indexes) {
    Def::Idx idx{index->m_name,         index->m_id,     index->m_type,        index->get_n_fields(),
                 index->get_n_unique(), index->get_n_ordering_defined_by_user(), index->get_page_no(),
                 index->get_space_id(), {}};

    for (ulint i{}; i < index->get_n_fields(); ++i) {
      idx.m_fields.emplace_back(index->get_nth_field(i)->m_name);
    }

    d.m_idxs.push_back(idx);
  }

  return d;
}

/** A started instance shared by the tests of a suite. */
class DictDb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    s_dir = new ut::TestDir();
    s_sys = new System();

    ASSERT_EQ(s_sys->init(ut::make_config(s_dir->m_path)), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
    ASSERT_TRUE(api::database_create(*s_sys, "test"));
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

  void SetUp() override { ASSERT_NE(s_sys, nullptr); }

  /** @return a table name of the test: test/<test name><suffix> */
  static std::string table(const std::string &suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  /** @return the cached table, loaded if needed, or nullptr. */
  Table *get(const std::string &name) { return ::get(m_sys, name.c_str()); }

  /** Evict an unused table from the cache. */
  void evict(const std::string &name) {
    m_dict.mutex_acquire();

    auto table = m_dict.table_check_if_in_cache(name.c_str());

    ASSERT_NE(table, nullptr);
    ASSERT_EQ(table->m_n_handles_opened, 0U);
    m_dict.table_remove_from_cache(table);
    EXPECT_EQ(m_dict.table_check_if_in_cache(name.c_str()), nullptr);

    m_dict.mutex_release();
  }

  /** @return the length of the background drop list. */
  ulint drop_list_len() {
    mutex_enter(&m_sys.m_trx_sys->m_mutex);
    const auto n = m_dict.m_ddl.get_background_drop_list_len();
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    return n;
  }

  static inline ut::TestDir *s_dir{};
  static inline System *s_sys{};

  System &m_sys{*s_sys};
  Dict &m_dict{*s_sys->m_dict};
};

/* ---- Static helpers ---- */

/** Database and table parts of a name. */
TEST(DictName, Parts) {
  EXPECT_EQ(Dict::get_db_name_len("test/t"), 4UL);
  EXPECT_EQ(Dict::get_db_name_len("a/bcd"), 1UL);
  EXPECT_STREQ(Dict::remove_db_name("test/t"), "t");
  EXPECT_STREQ(Dict::remove_db_name("db/x/y"), "x/y");
}

/** The system column names are reserved, in any case. */
TEST(DictName, Reserved) {
  for (auto name : {"DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR", "db_row_id", "Db_Trx_Id"}) {
    EXPECT_TRUE(Dict::col_name_is_reserved(name)) << name;
  }

  for (auto name : {"k", "DB_ROW", "DB_ROW_ID_", "ROW_ID"}) {
    EXPECT_FALSE(Dict::col_name_is_reserved(name)) << name;
  }
}

/** Lower casing in place. */
TEST(DictName, Casedn) {
  char s[] = "AbC/DeF_9";

  Dict::casedn_str(s);
  EXPECT_STREQ(s, "abc/def_9");
}

/* ---- The cache ---- */

/** The SYS_ tables are always cached, in the system space, with the fixed ids. */
TEST_F(DictDb, SysTables) {
  m_dict.mutex_acquire();

  const std::pair<Table *, Dict_id> sys_tables[] = {
    {m_dict.m_sys_tables, DICT_TABLES_ID},
    {m_dict.m_sys_columns, DICT_COLUMNS_ID},
    {m_dict.m_sys_indexes, DICT_INDEXES_ID},
    {m_dict.m_sys_fields, DICT_FIELDS_ID},
  };

  for (auto [table, id] : sys_tables) {
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->m_id, id) << table->m_name;
    EXPECT_EQ(table->m_space_id, SYS_TABLESPACE) << table->m_name;
    EXPECT_TRUE(table->m_cached);
    EXPECT_EQ(m_dict.table_get_on_id(id), table);
    EXPECT_EQ(m_dict.table_check_if_in_cache(table->m_name), table);
  }

  EXPECT_STREQ(m_dict.m_sys_tables->m_name, "SYS_TABLES");
  EXPECT_STREQ(m_dict.m_sys_fields->m_name, "SYS_FIELDS");

  /* The foreign key tables are created at startup, they are ordinary tables. */
  auto foreign = m_dict.table_get("SYS_FOREIGN");

  ASSERT_NE(foreign, nullptr);
  EXPECT_GE(foreign->m_id, DICT_HDR_FIRST_ID);
  EXPECT_NE(m_dict.table_get("SYS_FOREIGN_COLS"), nullptr);

  m_dict.mutex_release();
}

/** table_get(): the same table every time, nullptr for a missing name. */
TEST_F(DictDb, TableGet) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto t1 = get(table());

  ASSERT_NE(t1, nullptr);
  EXPECT_EQ(get(table()), t1);
  EXPECT_EQ(std::string(t1->m_name), table());
  EXPECT_TRUE(t1->m_cached);

  EXPECT_EQ(get(table("x")), nullptr);
  EXPECT_EQ(get("test"), nullptr);

  /* Names are case sensitive. */
  EXPECT_EQ(get("TEST/" + table().substr(5)), nullptr);
}

/** table_get_using_id(): by id, nullptr for an unknown one. */
TEST_F(DictDb, TableGetUsingId) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto table = get(DictDb::table());

  ASSERT_NE(table, nullptr);

  m_dict.mutex_acquire();
  EXPECT_EQ(m_dict.table_get_using_id(IB_RECOVERY_DEFAULT, table->m_id, false), table);
  EXPECT_EQ(m_dict.table_get_using_id(IB_RECOVERY_DEFAULT, 0xFFFFFFF0, false), nullptr);
  EXPECT_EQ(m_dict.table_get_on_id(table->m_id), table);
  m_dict.mutex_release();
}

/** Handle counts: table_get(name, true) and table_get_using_id(.., true) add one,
a cursor holds one while open. */
TEST_F(DictDb, HandleCount) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto t = m_dict.table_get(table().c_str(), true);

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->m_n_handles_opened, 1U);

  m_dict.mutex_acquire();
  EXPECT_EQ(m_dict.table_get_using_id(IB_RECOVERY_DEFAULT, t->m_id, true), t);
  EXPECT_EQ(t->m_n_handles_opened, 2U);
  m_dict.mutex_release();

  m_dict.table_decrement_handle_count(t, false);
  m_dict.table_decrement_handle_count(t, false);
  EXPECT_EQ(t->m_n_handles_opened, 0U);

  /* A plain table_get() doesn't count. */
  EXPECT_EQ(m_dict.table_get(table().c_str(), false), t);
  EXPECT_EQ(t->m_n_handles_opened, 0U);

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *c1{}, *c2{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &c1), DB_SUCCESS);
  EXPECT_EQ(t->m_n_handles_opened, 1U);
  ASSERT_EQ(c1->open_index(m_sys, "V", &c2), DB_SUCCESS);
  EXPECT_EQ(t->m_n_handles_opened, 2U);

  EXPECT_EQ(api::Cursor::close(m_sys, c2), DB_SUCCESS);
  EXPECT_EQ(api::Cursor::close(m_sys, c1), DB_SUCCESS);
  EXPECT_EQ(t->m_n_handles_opened, 0U);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
}

/** The columns and indexes of a new table: user columns then the system columns;
the clustered index has the key, the system columns and the other columns; a
secondary index has its columns and the key. */
TEST_F(DictDb, Layout) {
  ASSERT_EQ(
    create(
      m_sys,
      table(),
      {{"a", IB_INT, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL), 8},
       {"b", IB_VARCHAR, IB_COL_NONE, 100},
       {"c", IB_BLOB, IB_COL_NONE, 0},
       {"d", IB_DOUBLE, IB_COL_NOT_NULL, 8}},
      {{"PRIMARY", {"a"}, true}, {"B", {"b", "d"}}}
    ),
    DB_SUCCESS
  );

  auto t = get(table());

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->get_n_user_cols(), 4UL);
  ASSERT_EQ(t->get_n_cols(), 4UL + DATA_N_SYS_COLS);

  const char *names[] = {"a", "b", "c", "d", "DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"};

  for (ulint i{}; i < t->get_n_cols(); ++i) {
    EXPECT_STREQ(t->get_col_name(i), names[i]) << i;
  }

  EXPECT_EQ(t->get_nth_col(0)->mtype, DATA_INT);
  EXPECT_EQ(t->get_nth_col(0)->len, 8U);
  EXPECT_TRUE(t->get_nth_col(0)->prtype & DATA_UNSIGNED);
  EXPECT_TRUE(t->get_nth_col(0)->prtype & DATA_NOT_NULL);
  EXPECT_EQ(t->get_nth_col(1)->mtype, DATA_VARCHAR);
  EXPECT_EQ(t->get_nth_col(1)->len, 100U);
  EXPECT_FALSE(t->get_nth_col(1)->prtype & DATA_NOT_NULL);
  EXPECT_EQ(t->get_nth_col(2)->mtype, DATA_BLOB);
  EXPECT_EQ(t->get_nth_col(3)->mtype, DATA_DOUBLE);
  EXPECT_EQ(t->get_nth_col(4)->mtype, DATA_SYS);

  ASSERT_EQ(t->m_indexes.size(), 2UL);

  auto clust = t->get_first_index();
  auto sec = t->get_index_on_name("B");

  ASSERT_NE(sec, nullptr);
  EXPECT_TRUE(clust->is_clustered());
  EXPECT_TRUE(clust->is_unique());
  EXPECT_FALSE(sec->is_clustered());
  EXPECT_FALSE(sec->is_unique());

  /* a, DB_TRX_ID, DB_ROLL_PTR, b, c, d */
  EXPECT_EQ(clust->get_n_unique(), 1UL);
  EXPECT_EQ(clust->get_n_fields(), 6UL);
  EXPECT_STREQ(clust->get_nth_field(1)->m_name, "DB_TRX_ID");
  EXPECT_STREQ(clust->get_nth_field(2)->m_name, "DB_ROLL_PTR");

  /* b, d, a, and the last modifier: not part of the key. */
  EXPECT_EQ(sec->get_n_ordering_defined_by_user(), 2UL);
  EXPECT_EQ(sec->get_n_fields(), 4UL);
  EXPECT_EQ(sec->get_n_unique(), 3UL);
  EXPECT_EQ(sec->get_n_unique_in_tree(), 3UL);
  EXPECT_STREQ(sec->get_nth_field(2)->m_name, "a");
  EXPECT_STREQ(sec->get_nth_field(3)->m_name, "DB_TRX_ID");

  /* Both in the table's space, with a root page each. */
  EXPECT_NE(t->m_space_id, SYS_TABLESPACE);
  EXPECT_EQ(clust->get_space_id(), t->m_space_id);
  EXPECT_EQ(sec->get_space_id(), t->m_space_id);
  EXPECT_NE(clust->get_page_no(), sec->get_page_no());
  EXPECT_NE(clust->get_page_no(), NULL_PAGE_NO);
  EXPECT_NE(clust->m_id, sec->m_id);
  EXPECT_EQ(clust->m_table, t);

  /* The file of the table. */
  EXPECT_TRUE(ut::fs::exists(s_dir->m_path / (table() + ".ibd")));
}

/** A table without a primary key: the clustered index is on DB_ROW_ID. */
TEST_F(DictDb, GeneratedClusteredIndex) {
  ASSERT_EQ(create(m_sys, table(), {{"a", IB_INT, IB_COL_NONE, 4}}, {{"A", {"a"}}}), DB_SUCCESS);

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  auto clust = t->get_first_index();

  EXPECT_STREQ(clust->m_name, "GEN_CLUST_INDEX");
  EXPECT_EQ(clust->get_n_ordering_defined_by_user(), 0UL);
  EXPECT_STREQ(clust->get_nth_field(0)->m_name, "DB_ROW_ID");

  /* The key of the secondary index ends with DB_ROW_ID. */
  auto a = t->get_index_on_name("A");

  ASSERT_NE(a, nullptr);
  EXPECT_STREQ(a->get_nth_field(a->get_n_fields() - 2)->m_name, "DB_ROW_ID");
  EXPECT_STREQ(a->get_nth_field(a->get_n_fields() - 1)->m_name, "DB_TRX_ID");
}

/** A table evicted from the cache is loaded from the SYS_ tables as it was. */
TEST_F(DictDb, Reload) {
  ASSERT_EQ(
    create(
      m_sys,
      table(),
      {{"a", IB_INT, ColAttr(IB_COL_UNSIGNED | IB_COL_NOT_NULL), 8},
       {"b", IB_VARCHAR, IB_COL_CUSTOM1, 100},
       {"c", IB_BLOB, IB_COL_NONE, 0},
       {"d", IB_FLOAT, IB_COL_NONE, 4},
       {"e", IB_CHAR, IB_COL_NOT_NULL, 3},
       {"f", IB_DECIMAL, IB_COL_NONE, 9}},
      {{"PRIMARY", {"a", "e"}, true}, {"B", {"b"}}, {"DF", {"d", "f"}}}
    ),
    DB_SUCCESS
  );

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  const auto before = def(t);

  evict(table());

  auto t2 = get(table());

  ASSERT_NE(t2, nullptr);
  EXPECT_TRUE(def(t2) == before);
  EXPECT_EQ(def(t2).m_cols, before.m_cols);
  EXPECT_EQ(def(t2).m_idxs, before.m_idxs);
}

/** A table is loaded by id too. */
TEST_F(DictDb, ReloadUsingId) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  const auto before = def(t);

  evict(table());

  m_dict.mutex_acquire();

  auto t2 = m_dict.table_get_using_id(IB_RECOVERY_DEFAULT, before.m_id, false);

  ASSERT_NE(t2, nullptr);
  EXPECT_TRUE(def(t2) == before);
  EXPECT_EQ(m_dict.table_check_if_in_cache(table().c_str()), t2);

  m_dict.mutex_release();
}

/** The rows of a reloaded table are all there. */
TEST_F(DictDb, ReloadKeepsRows) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 500);

  evict(table());

  EXPECT_EQ(count(m_sys, table()), 500UL);
}

/** Table and index ids grow, and are never reused. */
TEST_F(DictDb, IdsGrow) {
  Dict_id prev_table{}, prev_index{};

  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(create_kv(m_sys, table(std::format("_{}", i))), DB_SUCCESS);

    auto t = get(table(std::format("_{}", i)));

    ASSERT_NE(t, nullptr);
    EXPECT_GT(t->m_id, prev_table);

    for (auto index : t->m_indexes) {
      EXPECT_GT(index->m_id, prev_index);
      prev_index = index->m_id;
    }

    prev_table = t->m_id;
  }

  /* Space ids are new too. */
  EXPECT_NE(get(table("_0"))->m_space_id, get(table("_1"))->m_space_id);
}

/** Names with spaces, dots, dashes, UTF-8 and the longest length. */
TEST_F(DictDb, OddNames) {
  const std::string names[] = {
    "test/a b",
    "test/x-y.z",
    "test/\xc3\xbc" "n\xc3\xaf" "c\xc3\xb8" "d\xc3\xa9",
    "test/" + std::string(64, 'L'),
    "test/$dollar",
  };

  for (const auto &name : names) {
    ASSERT_EQ(create_kv(m_sys, name), DB_SUCCESS) << name;
    load_kv(m_sys, name, 0, 3);

    uint64_t id{};

    EXPECT_EQ(api::table_get_id(m_sys, name.c_str(), &id), DB_SUCCESS) << name;

    evict(name);

    auto t = get(name);

    ASSERT_NE(t, nullptr) << name;
    EXPECT_EQ(t->m_id, id) << name;
    EXPECT_EQ(count(m_sys, name), 3UL) << name;
    EXPECT_EQ(drop(m_sys, name), DB_SUCCESS) << name;
    EXPECT_EQ(get(name), nullptr) << name;
  }
}

/** A table name without a database is refused by Ddl::create_table(). */
TEST_F(DictDb, CreateWithoutDatabase) {
  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);

  auto t = Table::create("nodb", SYS_TABLESPACE, 1, 0, false, Current_location());

  t->add_col("a", DATA_INT, 0, 4);

  /* create_table() frees the prototype. */
  EXPECT_EQ(m_dict.m_ddl.create_table(t, trx), DB_ERROR);
  EXPECT_EQ(m_dict.table_check_if_in_cache("nodb"), nullptr);

  EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS);
}

/** Ddl::create_table() of a name in use fails and leaves the table alone. */
TEST_F(DictDb, CreateDuplicate) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto old = get(table());

  ASSERT_NE(old, nullptr);

  const auto before = def(old);
  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);

  auto t = Table::create(table().c_str(), SYS_TABLESPACE, 1, 0, false, Current_location());

  t->add_col("z", DATA_INT, 0, 4);
  EXPECT_NE(m_dict.m_ddl.create_table(t, trx), DB_SUCCESS);
  EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS);

  auto now = get(table());

  ASSERT_NE(now, nullptr);
  EXPECT_TRUE(def(now) == before);
}

/** Many columns: 1000 user columns, created, loaded and used. */
TEST_F(DictDb, ManyColumns) {
  std::vector<ColDef> cols;

  for (int i = 0; i < 1000; ++i) {
    cols.push_back({std::format("c{}", i), IB_INT, IB_COL_UNSIGNED, 4});
  }

  ASSERT_EQ(create(m_sys, table(), cols, {{"PRIMARY", {"c0"}, true}, {"LAST", {"c999"}}}), DB_SUCCESS);

  evict(table());

  auto t = get(table());

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->get_n_user_cols(), 1000UL);
  EXPECT_STREQ(t->get_col_name(999), "c999");

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);

  auto tpl = crsr->clust_read_tuple_create();

  for (int i = 0; i < 1000; ++i) {
    ASSERT_EQ(tpl->write_u32(i, uint32_t(i * 3)), DB_SUCCESS);
  }

  ASSERT_EQ(crsr->insert_row(m_sys, tpl), DB_SUCCESS);

  tpl = tpl->clear();
  ASSERT_EQ(crsr->first(m_sys), DB_SUCCESS);
  ASSERT_EQ(crsr->read_row(m_sys, tpl), DB_SUCCESS);

  uint32_t v{};

  ASSERT_EQ(tpl->read_u32(999, &v), DB_SUCCESS);
  EXPECT_EQ(v, 2997U);

  api::Tuple::destroy(tpl);
  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
}

/** An index of 16 columns. */
TEST_F(DictDb, WideIndex) {
  std::vector<ColDef> cols;
  std::vector<std::string> key;

  for (int i = 0; i < 16; ++i) {
    cols.push_back({std::format("c{}", i), IB_INT, IB_COL_UNSIGNED, 4});
    key.push_back(std::format("c{}", i));
  }

  ASSERT_EQ(create(m_sys, table(), cols, {{"PRIMARY", key, true}}), DB_SUCCESS);

  auto t = get(table());

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->get_first_index()->get_n_unique(), 16UL);
  EXPECT_EQ(t->get_first_index()->get_n_ordering_defined_by_user(), 16UL);
}

/* ---- Statistics ---- */

/** The statistics of an empty table: no rows, one page per index. */
TEST_F(DictDb, StatisticsEmpty) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  m_dict.mutex_acquire();
  m_dict.update_statistics(t);
  m_dict.mutex_release();

  EXPECT_TRUE(t->m_stats.m_initialized);
  EXPECT_EQ(t->m_stats.m_n_rows, 0);
  EXPECT_EQ(t->m_stats.m_clustered_index_size, 1U);
  EXPECT_EQ(t->m_stats.m_sum_of_secondary_index_sizes, 1U);
  EXPECT_EQ(t->m_stats.m_modified_counter, 0UL);
}

/** The statistics follow the size of the table; the modified counter is reset. */
TEST_F(DictDb, StatisticsRows) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10000);

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  /* The statistics are recomputed after 1/16 of the table changed. */
  EXPECT_LT(t->m_stats.m_modified_counter, 10000UL / 16 * 2);

  m_dict.mutex_acquire();
  m_dict.update_statistics(t);
  m_dict.mutex_release();

  EXPECT_EQ(t->m_stats.m_modified_counter, 0UL);
  EXPECT_GT(t->m_stats.m_n_rows, 2000);
  EXPECT_LT(t->m_stats.m_n_rows, 50000);
  EXPECT_GT(t->m_stats.m_clustered_index_size, 10U);
  EXPECT_GT(t->m_stats.m_sum_of_secondary_index_sizes, 5U);

  for (auto index : t->m_indexes) {
    EXPECT_LE(index->m_stats.m_n_leaf_pages, index->m_stats.m_index_size) << index->m_name;
    EXPECT_GT(index->m_stats.m_n_leaf_pages, 1U) << index->m_name;
  }

  /* V is (v, k): 10 values of v, unique with k. */
  auto v = t->get_index_on_name("V");

  ASSERT_NE(v, nullptr);
  EXPECT_LT(v->m_stats.m_n_diff_key_vals[1], 200);
  EXPECT_GT(v->m_stats.m_n_diff_key_vals[2], 2000);
}

/* ---- DDL ---- */

/** TRUNCATE: the same columns and indexes (and index ids), a new table id, no rows. */
TEST_F(DictDb, TruncateKeepsSchema) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 1000);

  auto t = get(table());

  ASSERT_NE(t, nullptr);

  const auto before = def(t);
  uint64_t new_id{};

  ASSERT_EQ(api::table_truncate(m_sys, table().c_str(), &new_id), DB_SUCCESS);

  auto t2 = get(table());

  ASSERT_NE(t2, nullptr);

  const auto after = def(t2);

  EXPECT_EQ(after.m_id, new_id);
  EXPECT_NE(after.m_id, before.m_id);
  EXPECT_EQ(after.m_cols, before.m_cols);
  ASSERT_EQ(after.m_idxs.size(), before.m_idxs.size());

  for (size_t i{}; i < after.m_idxs.size(); ++i) {
    EXPECT_EQ(after.m_idxs[i].m_name, before.m_idxs[i].m_name);
    EXPECT_EQ(after.m_idxs[i].m_fields, before.m_idxs[i].m_fields);
    EXPECT_EQ(after.m_idxs[i].m_id, before.m_idxs[i].m_id);
    EXPECT_NE(after.m_idxs[i].m_root, NULL_PAGE_NO);
  }

  m_dict.mutex_acquire();
  EXPECT_EQ(m_dict.table_get_on_id(before.m_id), nullptr);
  EXPECT_EQ(m_dict.table_get_on_id(after.m_id), t2);
  m_dict.mutex_release();

  EXPECT_EQ(count(m_sys, table()), 0UL);

  /* The new definition is the one in the SYS_ tables. */
  evict(table());

  auto t3 = get(table());

  ASSERT_NE(t3, nullptr);
  EXPECT_TRUE(def(t3) == after);

  load_kv(m_sys, table(), 0, 10);
  EXPECT_EQ(count(m_sys, table()), 10UL);
}

/** A table dropped while a cursor has it open: the drop succeeds, the table goes
to the background drop list and is dropped once the cursor is closed. */
TEST_F(DictDb, DropWithOpenHandle) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 10);

  const auto n_before = drop_list_len();
  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(drop(m_sys, table()), DB_SUCCESS);

  /* Queued, still cached for the cursor. */
  EXPECT_EQ(drop_list_len(), n_before + 1);

  m_dict.mutex_acquire();
  EXPECT_NE(m_dict.table_check_if_in_cache(table().c_str()), nullptr);
  m_dict.mutex_release();

  /* Dropping it again: it is queued already. */
  EXPECT_EQ(drop(m_sys, table()), DB_TABLESPACE_DELETED);

  /* The table can't be dropped while it is open: it stays queued. */
  EXPECT_GE(m_dict.m_ddl.drop_tables_in_background(), 1UL);
  EXPECT_EQ(drop_list_len(), n_before + 1);

  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  /* The master thread may get there first. */
  (void)m_dict.m_ddl.drop_tables_in_background();

  EXPECT_EQ(drop_list_len(), n_before);
  EXPECT_EQ(get(table()), nullptr);
  EXPECT_FALSE(ut::fs::exists(s_dir->m_path / (table() + ".ibd")));

  /* The name can be used again. */
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  EXPECT_EQ(count(m_sys, table()), 0UL);
}

/** Rename in the dictionary: the cache and the SYS_ tables have the new name.
Regression: ddl/ddl0ddl.cc:977-979, the RENAME_CONSTRAINT_IDS procedure of
Ddl::rename_table() has the left-over lines "SET ID = new_foreign_id WHERE ID =
foreign_id;" after "UPDATE SYS_FOREIGN_COLS SET ID = new_foreign_id WHERE ID =
foreign_id;": a syntax error, fatal in the parser. Every rename aborts. */
TEST_F(DictDb, Rename) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 5);

  const auto before = def(get(table()));
  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(m_dict.m_ddl.rename_table(table().c_str(), table("_new").c_str(), trx), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  EXPECT_EQ(get(table()), nullptr);

  evict(table("_new"));

  auto t = get(table("_new"));

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->m_id, before.m_id);
  EXPECT_EQ(count(m_sys, table("_new")), 5UL);
}

/** DDL on other tables while threads insert into a table: nothing is lost. */
TEST_F(DictDb, DdlWithDml) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  std::atomic<bool> done{};
  std::atomic<uint32_t> n_rows{};
  std::vector<std::thread> threads;

  for (uint32_t t = 0; t < 4; ++t) {
    threads.emplace_back([&, t] {
      for (uint32_t i = 0; !done || i < 50; ++i) {
        auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
        api::Cursor *crsr{};

        EXPECT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);
        EXPECT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);

        auto tpl = crsr->clust_read_tuple_create();

        EXPECT_EQ(tpl->write_u32(0, i * 4 + t), DB_SUCCESS);
        EXPECT_EQ(tpl->write_u32(1, t), DB_SUCCESS);
        EXPECT_EQ(crsr->insert_row(m_sys, tpl), DB_SUCCESS);

        api::Tuple::destroy(tpl);
        EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
        EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

        ++n_rows;
      }
    });
  }

  for (int i = 0; i < 10; ++i) {
    const auto name = table(std::format("_ddl{}", i));

    ASSERT_EQ(create_kv(m_sys, name), DB_SUCCESS);
    load_kv(m_sys, name, 0, 10);

    uint64_t id{};

    ASSERT_EQ(api::table_truncate(m_sys, name.c_str(), &id), DB_SUCCESS);
    ASSERT_EQ(drop(m_sys, name), DB_SUCCESS);
  }

  done = true;

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(count(m_sys, table()), uint64_t(n_rows));
}

/* ---- Foreign keys ---- */

/** A foreign key from the SQL text of a CREATE TABLE: the constraint is in the
cache of both tables, a row can't reference a missing parent, a referenced
parent row can't be deleted, the constraint is loaded again from SYS_FOREIGN.
Regression: dict/dict0fk.cc:355-362 (Dict::scan_id): for a column identifier
(table_id == false) the name is converted to UTF-8 into dst but *id is never set
to dst (only the table branch does it, twice), so *id stays nullptr: scan_col()
takes it for a syntax error, "Cannot resolve column name", and every constraint
fails with DB_CANNOT_ADD_CONSTRAINT. Upstream (7c21eac0 dict0dict.c:2469) did
*id = dst = mem_heap_alloc(heap, len) in both branches. No foreign key can be created. */
TEST_F(DictDb, Fk) {
  /* The constraint parser scans for the keywords: no "foreign" in the names. */
  const auto parent = table("_par");
  const auto child = table("_chi");

  ASSERT_EQ(create_kv(m_sys, parent), DB_SUCCESS);
  ASSERT_EQ(create_kv(m_sys, child), DB_SUCCESS);
  load_kv(m_sys, parent, 0, 10);

  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);

  const auto sql = std::format(
    "CREATE TABLE {} (k INT, v INT, FOREIGN KEY (v) REFERENCES {} (k))", Dict::remove_db_name(child.c_str()),
    Dict::remove_db_name(parent.c_str())
  );

  const auto err = m_dict.create_foreign_constraints(trx, sql.c_str(), child.c_str(), false);

  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(err, DB_SUCCESS);

  auto c = get(child);
  auto p = get(parent);

  ASSERT_NE(c, nullptr);
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(c->m_foreign_list.size(), 1UL);
  ASSERT_EQ(p->m_referenced_list.size(), 1UL);

  auto fk = c->m_foreign_list.front();

  EXPECT_EQ(fk->m_foreign_table, c);
  EXPECT_EQ(fk->m_referenced_table, p);
  EXPECT_EQ(fk->m_n_fields, 1U);
  EXPECT_EQ(std::string(fk->m_id), child + "_ibfk_1");

  /* Inserts into the child are checked. */
  trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, child.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);

  auto tpl = crsr->clust_read_tuple_create();

  ASSERT_EQ(tpl->write_u32(0, 1), DB_SUCCESS);
  ASSERT_EQ(tpl->write_u32(1, 5), DB_SUCCESS);
  EXPECT_EQ(crsr->insert_row(m_sys, tpl), DB_SUCCESS);

  ASSERT_EQ(tpl->write_u32(0, 2), DB_SUCCESS);
  ASSERT_EQ(tpl->write_u32(1, 50), DB_SUCCESS);
  EXPECT_EQ(crsr->insert_row(m_sys, tpl), DB_NO_REFERENCED_ROW);

  api::Tuple::destroy(tpl);
  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  /* The parent row 5 is referenced. */
  trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::cursor_open_table(m_sys, parent.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);
  ASSERT_EQ(crsr->set_lock_mode(m_sys, IB_LOCK_X), DB_SUCCESS);

  for (uint32_t k : {5U, 6U}) {
    auto key = crsr->clust_search_tuple_create();
    int res{};

    ASSERT_EQ(key->write_u32(0, k), DB_SUCCESS);
    ASSERT_EQ(crsr->moveto(m_sys, key, IB_CUR_GE, &res), DB_SUCCESS);
    ASSERT_EQ(res, 0);
    EXPECT_EQ(crsr->delete_row(m_sys), k == 5 ? DB_ROW_IS_REFERENCED : DB_SUCCESS) << k;
    api::Tuple::destroy(key);
  }

  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  /* The constraint comes back with the tables. */
  evict(child);
  evict(parent);

  c = get(child);
  p = get(parent);

  ASSERT_NE(c, nullptr);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(c->m_foreign_list.size(), 1UL);
  EXPECT_EQ(p->m_referenced_list.size(), 1UL);
}

/* ---- Performance ---- */

class DictPerf : public DictDb {};

/** A cached table by name and by id. */
TEST_F(DictPerf, TableGet) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  const auto name = table();
  auto t = get(name);

  ASSERT_NE(t, nullptr);

  auto by_name = ut::bench("table_get", 1000000, [&](uint64_t) {
    m_dict.mutex_acquire();
    auto table = m_dict.table_get(name.c_str());
    m_dict.mutex_release();
    ut::keep(table);
  });

  auto by_id = ut::bench("table_get_using_id", 1000000, [&](uint64_t) {
    m_dict.mutex_acquire();
    auto table = m_dict.table_get_using_id(IB_RECOVERY_DEFAULT, t->m_id, false);
    m_dict.mutex_release();
    ut::keep(table);
  });

  auto counted = ut::bench("table_get_inc_dec", 1000000, [&](uint64_t) {
    auto table = m_dict.table_get(name.c_str(), true);
    m_dict.table_decrement_handle_count(table, false);
  });

  EXPECT_LT(by_name.ns_per_op(), 20000);
  EXPECT_LT(by_id.ns_per_op(), 20000);
  EXPECT_LT(counted.ns_per_op(), 40000);
}

/** Load a table from the SYS_ tables after an eviction. */
TEST_F(DictPerf, Load) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);

  const auto name = table();

  auto r = ut::bench("evict_load", 2000, [&](uint64_t) {
    m_dict.mutex_acquire();
    m_dict.table_remove_from_cache(m_dict.table_check_if_in_cache(name.c_str()));
    auto table = m_dict.table_get(name.c_str());
    m_dict.mutex_release();
    ut::keep(table);
  });

  EXPECT_LT(r.ns_per_op(), 20e6);
}

/** Statistics of a table of 20000 rows. */
TEST_F(DictPerf, Statistics) {
  ASSERT_EQ(create_kv(m_sys, table()), DB_SUCCESS);
  load_kv(m_sys, table(), 0, 20000);

  auto t = get(table());

  auto r = ut::bench("update_statistics", 500, [&](uint64_t) {
    m_dict.mutex_acquire();
    m_dict.update_statistics(t);
    m_dict.mutex_release();
  });

  EXPECT_LT(r.ns_per_op(), 100e6);
}

/** CREATE TABLE, TRUNCATE and DROP TABLE. */
TEST_F(DictPerf, Ddl) {
  constexpr uint64_t n = 20;

  auto c = ut::bench("create", n, [&](uint64_t i) { EXPECT_EQ(create_kv(m_sys, table(std::format("_{}", i))), DB_SUCCESS); });

  auto t = ut::bench("truncate", n, [&](uint64_t i) {
    uint64_t id{};
    EXPECT_EQ(api::table_truncate(m_sys, table(std::format("_{}", i)).c_str(), &id), DB_SUCCESS);
  });

  auto d = ut::bench("drop", n, [&](uint64_t i) { EXPECT_EQ(drop(m_sys, table(std::format("_{}", i))), DB_SUCCESS); });

  EXPECT_LT(c.ns_per_op(), 2e9);
  EXPECT_LT(t.ns_per_op(), 2e9);
  EXPECT_LT(d.ns_per_op(), 2e9);
}

/** An index left with the temporary prefix in its name (a crash while it was
being created or dropped) is dropped at the next startup, the table and its
other indexes stay. Ddl::drop_all_temp_indexes() scanned SYS_TABLES with the
field numbers of SYS_INDEXES: it never found one. */
TEST(DictTempIndex, DroppedAtStartup) {
  ut::TestDir dir;
  const auto config = ut::make_config(dir.m_path);

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);

    ASSERT_TRUE(api::database_create(sys, "test"));
    ASSERT_EQ(create_kv(sys, "test/tmpidx"), DB_SUCCESS);
    load_kv(sys, "test/tmpidx", 0, 50);

    /* Mark index V temporary, as a crash in the middle of its creation leaves it. */
    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);

    ASSERT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);

    /* The rename Ddl::drop_index() commits first, by the index id. */
    auto table = sys.m_dict->table_get("test/tmpidx");
    ASSERT_NE(table, nullptr);
    auto index = table->get_index_on_name("V");
    ASSERT_NE(index, nullptr);

    auto info = pars_info_create();

    pars_info_add_uint64_literal(info, "indexid", index->m_id);

    EXPECT_EQ(
      que_eval_sql(
        sys,
        info,
        "PROCEDURE P () IS\n"
        "BEGIN\n"
        "  UPDATE SYS_INDEXES SET NAME = CONCAT('" TEMP_INDEX_PREFIX_STR "', NAME) WHERE ID = :indexid;\n"
        "END;\n",
        false,
        trx
      ),
      DB_SUCCESS
    );

    EXPECT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }

  System sys;

  ASSERT_EQ(sys.init(config), DB_SUCCESS);
  ASSERT_EQ(sys.startup(), DB_SUCCESS);

  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get("test/tmpidx");
  ASSERT_NE(table, nullptr);

  ulint n_indexes{};

  for (auto index : table->m_indexes) {
    EXPECT_NE(*index->m_name, TEMP_INDEX_PREFIX) << "a temporary index is still there";
    ++n_indexes;
  }

  sys.m_dict->mutex_release();

  EXPECT_EQ(n_indexes, 1UL) << "only the clustered index is left";

  ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
}

}  // namespace
