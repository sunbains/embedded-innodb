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

/** @file unit-tests/ut0env.cc
Scaffolding shared by the component unit tests, see ut0env.h. */

#include "ut0env.h"

#include "btr0pcur.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "rem0rec.h"
#include "trx0sys.h"

#include <cstdlib>

#include <atomic>
#include <csignal>
#include <format>

#include <sys/wait.h>
#include <unistd.h>

using namespace std::chrono_literals;

namespace ut {

namespace {

/** Distinguishes the directories of the tests of one process. */
std::atomic<int> dir_seq{};

/** Value of an api call that must succeed, inside a function that can't
use the gtest ASSERT macros (they return void). */
void check(DbErr err, const char *what) {
  if (err != DB_SUCCESS) {
    ADD_FAILURE() << what << ": " << err;
  }
}

/** Create the test table: see Row. */
void create_test_table(System &sys) {
  (void)api::database_create(sys, "test");

  api::TableSchema *tbl_sch{};
  api::IndexSchema *idx_sch{};

  check(api::TableSchema::create(TABLE, &tbl_sch, IB_TBL_V1, 0), "TableSchema::create");
  check(tbl_sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), "add_col k");
  check(tbl_sch->add_col("a", IB_INT, IB_COL_UNSIGNED, 0, 4), "add_col a");
  check(tbl_sch->add_col("b", IB_BLOB, IB_COL_NONE, 0, 0), "add_col b");

  check(tbl_sch->add_index("PRIMARY", &idx_sch), "add_index PRIMARY");
  check(idx_sch->add_col("k", 0), "PRIMARY k");
  check(idx_sch->set_clustered(), "set_clustered");

  check(tbl_sch->add_index(SEC_A, &idx_sch), "add_index SEC_A");
  check(idx_sch->add_col("a", 0), "SEC_A a");

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  uint64_t table_id{};

  check(api::schema_lock_exclusive(sys, trx), "schema_lock_exclusive");

  if (auto err = api::table_create(sys, trx, tbl_sch, &table_id); err != DB_SUCCESS && err != DB_TABLE_EXISTS) {
    ADD_FAILURE() << "table_create: " << err;
  }

  check(api::trx_commit(sys, trx), "trx_commit");

  api::TableSchema::destroy(sys, tbl_sch);
}

/** Open the test table in trx and take an intention lock. */
DbErr open(System &sys, Trx *trx, CursorLockMode mode, api::Cursor **crsr) {
  if (auto err = api::cursor_open_table(sys, TABLE, trx, crsr); err != DB_SUCCESS) {
    return err;
  }

  if (auto err = (*crsr)->lock(sys, mode); err != DB_SUCCESS) {
    (void)api::Cursor::close(sys, *crsr);
    *crsr = nullptr;
    return err;
  }

  return DB_SUCCESS;
}

/** Fill tpl, a clustered index read tuple, from row. */
DbErr write_row(api::Tuple *tpl, const Row &row) {
  if (auto err = tpl->write_u32(0, row.m_k); err != DB_SUCCESS) {
    return err;
  }

  if (auto err = tpl->write_u32(1, row.m_a); err != DB_SUCCESS) {
    return err;
  }

  return tpl->set_value(2, row.m_b.data(), row.m_b.size());
}

/** @return the row in tpl, a clustered index read tuple. */
Row read_row(const api::Tuple *tpl) {
  Row row;

  check(tpl->read_u32(0, &row.m_k), "read_u32 k");
  check(tpl->read_u32(1, &row.m_a), "read_u32 a");

  const auto len = tpl->get_len(2);

  if (len != api::IB_SQL_NULL) {
    row.m_b.resize(len);

    if (len > 0) {
      (void)tpl->copy_value(2, row.m_b.data(), len);
    }
  }

  return row;
}

/** Position crsr, on the clustered index, on the row with key k.
@return DB_SUCCESS, or DB_RECORD_NOT_FOUND if there is no such row */
DbErr seek(System &sys, api::Cursor *crsr, uint32_t k) {
  auto key = crsr->clust_search_tuple_create();

  auto err = key->write_u32(0, k);

  int res{-1};

  if (err == DB_SUCCESS) {
    err = crsr->moveto(sys, key, IB_CUR_GE, &res);
  }

  api::Tuple::destroy(key);

  if (err == DB_SUCCESS && res != 0) {
    err = DB_RECORD_NOT_FOUND;
  } else if (err == DB_END_OF_INDEX) {
    err = DB_RECORD_NOT_FOUND;
  }

  return err;
}

}  // namespace

TestDir::TestDir() {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
  const auto name = info != nullptr ? std::format("{}.{}", info->test_suite_name(), info->name()) : std::string("test");

  m_path = fs::temp_directory_path() / std::format("ibut-{}-{}-{}", ::getpid(), dir_seq++, name);

  fs::remove_all(m_path);
  fs::create_directories(m_path);
}

TestDir::~TestDir() {
  std::error_code ec;

  fs::remove_all(m_path, ec);
}

Config make_config(const fs::path &dir) {
  Config config;

  config.m_fil.m_data_home = dir.string();
  config.m_log.m_log_dir = dir.string();
  config.m_buf_pool.m_size = 32 * 1024 * 1024;
  config.m_lock.m_wait_timeout = 10s;

  /* A run of the DB suites with the clock forced high. */
  if (const auto start = std::getenv("UT_HLC_START"); start != nullptr) {
    config.m_trx.m_hlc_start = std::strtoull(start, nullptr, 10);
  }

  return config;
}

void SysTest::SetUp() {
  m_config = make_config(fs::temp_directory_path());
  m_sys = std::make_unique<System>();

  ASSERT_EQ(m_sys->init(m_config), DB_SUCCESS);
}

void SysTest::TearDown() {
  m_sys.reset();
}

std::string blob(uint32_t seed, size_t n) {
  std::string s(n, '\0');

  uint32_t x = seed * 2654435761u + 1;

  for (auto &c : s) {
    x = x * 1103515245u + 12345u;
    c = char('a' + (x >> 16) % 26);
  }

  return s;
}

void DbTest::SetUp() {
  m_config = make_config(m_dir.m_path);
  start();
}

void DbTest::TearDown() {
  if (m_sys != nullptr) {
    stop();
  }
}

void DbTest::start(bool create_table) {
  ASSERT_EQ(m_sys, nullptr);

  m_sys = std::make_unique<System>();

  ASSERT_EQ(m_sys->init(m_config), DB_SUCCESS);
  ASSERT_EQ(m_sys->startup(), DB_SUCCESS);

  if (create_table) {
    create_test_table(*m_sys);
  }
}

void DbTest::stop() {
  ASSERT_NE(m_sys, nullptr);
  ASSERT_EQ(m_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);

  m_sys.reset();
}

void DbTest::restart() {
  stop();
  start(false);
}

void DbTest::crash(const std::function<void()> &fn) {
  stop();

  /* The child is single threaded at the fork: the parent's instance is gone. */
  const auto pid = ::fork();

  ASSERT_NE(pid, -1);

  if (pid == 0) {
    m_sys = std::make_unique<System>();

    if (m_sys->init(m_config) != DB_SUCCESS || m_sys->startup() != DB_SUCCESS) {
      ::_exit(2);
    }

    fn();

    if (::testing::Test::HasFailure()) {
      ::_exit(3);
    }

    ::raise(SIGKILL);
    ::_exit(4);
  }

  int status{};

  const auto waited = ::waitpid(pid, &status, 0);

  /* Whatever happened to the child, the database is usable again. */
  start(false);

  ASSERT_EQ(waited, pid);
  ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL)
    << "the child failed before the kill, status " << status;
}

Trx *DbTest::begin(TrxLevel level) {
  auto trx = api::trx_begin(*m_sys, level);

  EXPECT_NE(trx, nullptr);

  return trx;
}

void DbTest::commit(Trx *trx) {
  EXPECT_EQ(api::trx_commit(*m_sys, trx), DB_SUCCESS);
}

void DbTest::rollback(Trx *trx) {
  EXPECT_EQ(api::trx_rollback(*m_sys, trx), DB_SUCCESS);
}

DbErr DbTest::insert(Trx *trx, const Row &row) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};

  if (auto err = open(sys, trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto tpl = crsr->clust_read_tuple_create();
  auto err = write_row(tpl, row);

  if (err == DB_SUCCESS) {
    err = crsr->insert_row(sys, tpl);
  }

  api::Tuple::destroy(tpl);
  (void)api::Cursor::close(sys, crsr);

  return err;
}

DbErr DbTest::update(Trx *trx, uint32_t k, const Row &row) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};

  if (auto err = open(sys, trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->set_lock_mode(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    err = seek(sys, crsr, k);
  }

  if (err == DB_SUCCESS) {
    auto old_tpl = crsr->clust_read_tuple_create();
    auto new_tpl = crsr->clust_read_tuple_create();

    err = crsr->read_row(sys, old_tpl);

    if (err == DB_SUCCESS) {
      err = new_tpl->copy(old_tpl);
    }

    if (err == DB_SUCCESS) {
      err = write_row(new_tpl, row);
    }

    if (err == DB_SUCCESS) {
      err = crsr->update_row(sys, old_tpl, new_tpl);
    }

    api::Tuple::destroy(old_tpl);
    api::Tuple::destroy(new_tpl);
  }

  (void)api::Cursor::close(sys, crsr);

  return err;
}

DbErr DbTest::remove(Trx *trx, uint32_t k) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};

  if (auto err = open(sys, trx, IB_LOCK_IX, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->set_lock_mode(sys, IB_LOCK_X);

  if (err == DB_SUCCESS) {
    err = seek(sys, crsr, k);
  }

  if (err == DB_SUCCESS) {
    err = crsr->delete_row(sys);
  }

  (void)api::Cursor::close(sys, crsr);

  return err;
}

std::vector<Row> read_all(System &sys, api::Cursor *crsr, DbErr err) {
  std::vector<Row> rows;
  auto tpl = crsr->clust_read_tuple_create();

  while (err == DB_SUCCESS) {
    err = crsr->read_row(sys, tpl);

    if (err != DB_SUCCESS) {
      ADD_FAILURE() << "read_row: " << err;
      break;
    }

    rows.push_back(read_row(tpl));

    tpl = tpl->clear();
    err = crsr->next(sys);
  }

  if (err != DB_END_OF_INDEX && err != DB_SUCCESS) {
    ADD_FAILURE() << "scan: " << err;
  }

  api::Tuple::destroy(tpl);

  return rows;
}

Rows DbTest::scan(Trx *trx) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};
  Rows rows;

  if (auto err = open(sys, trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
    ADD_FAILURE() << "open: " << err;
    return rows;
  }

  for (auto &row : read_all(sys, crsr, crsr->first(sys))) {
    EXPECT_TRUE(rows.emplace(row.m_k, row).second) << "duplicate k " << row.m_k;
  }

  (void)api::Cursor::close(sys, crsr);

  return rows;
}

std::vector<Row> DbTest::scan_sec(Trx *trx) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};
  api::Cursor *sec{};
  std::vector<Row> rows;

  if (auto err = open(sys, trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
    ADD_FAILURE() << "open: " << err;
    return rows;
  }

  if (auto err = crsr->open_index(sys, SEC_A, &sec); err != DB_SUCCESS) {
    ADD_FAILURE() << "open_index: " << err;
  } else {
    sec->set_cluster_access();
    rows = read_all(sys, sec, sec->first(sys));
    (void)api::Cursor::close(sys, sec);
  }

  (void)api::Cursor::close(sys, crsr);

  return rows;
}

std::optional<DbTest::SecRec> DbTest::sec_rec(uint32_t a, uint32_t k) {
  m_sys->m_dict->mutex_acquire();
  auto table = m_sys->m_dict->table_get(TABLE);
  m_sys->m_dict->mutex_release();

  auto index = table->get_index_on_name(SEC_A);
  std::optional<SecRec> found;
  Mtr mtr;
  BtreePcursor pcur;
  MemHeap *heap{};

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
    auto rec = pcur.get_rec();
    RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;

    const auto a_rec = mach_read_from_4(rec.get_nth_field(offs, 0, &len).get());
    const auto k_rec = mach_read_from_4(rec.get_nth_field(offs, 1, &len).get());

    if (a_rec == a && k_rec == k) {
      found = SecRec{TrxSys::read_trx_id(rec.get_nth_field(offs, index->get_n_fields() - 1, &len).get()), rec.get_deleted_flag() != 0};
      break;
    }
  }

  pcur.close();
  mtr.commit(*m_sys);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return found;
}

trx_id_t DbTest::clust_id(uint32_t k) {
  m_sys->m_dict->mutex_acquire();
  auto table = m_sys->m_dict->table_get(TABLE);
  m_sys->m_dict->mutex_release();

  auto index = table->get_first_index();
  trx_id_t id{};
  Mtr mtr;
  BtreePcursor pcur;
  MemHeap *heap{};

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
    auto rec = pcur.get_rec();
    RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;

    if (mach_read_from_4(rec.get_nth_field(offs, 0, &len).get()) == k) {
      id = TrxSys::read_trx_id(rec.get_nth_field(offs, index->get_sys_col_field_pos(DATA_TRX_ID), &len).get());
      break;
    }
  }

  pcur.close();
  mtr.commit(*m_sys);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return id;
}

std::vector<std::pair<uint32_t, uint32_t>> DbTest::scan_sec_only(Trx *trx) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};
  api::Cursor *sec{};
  std::vector<std::pair<uint32_t, uint32_t>> rows;

  if (auto err = open(sys, trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
    ADD_FAILURE() << "open: " << err;
    return rows;
  }

  if (auto err = crsr->open_index(sys, SEC_A, &sec); err != DB_SUCCESS) {
    ADD_FAILURE() << "open_index: " << err;
  } else {
    auto tpl = sec->sec_read_tuple_create();
    auto err2 = sec->first(sys);

    while (err2 == DB_SUCCESS) {
      uint32_t a{};
      uint32_t k{};

      err2 = sec->read_row(sys, tpl);

      if (err2 != DB_SUCCESS) {
        break;
      }

      /* The columns of a row tuple are those of the table. */
      EXPECT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
      EXPECT_EQ(tpl->read_u32(1, &a), DB_SUCCESS);
      rows.emplace_back(a, k);
      err2 = sec->next(sys);
      tpl = tpl->clear();
    }

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(sys, sec);
  }

  (void)api::Cursor::close(sys, crsr);

  return rows;
}

std::optional<Row> DbTest::find_sec(Trx *trx, uint32_t a) {
  auto &sys = *m_sys;
  api::Cursor *crsr{};
  api::Cursor *sec{};
  std::optional<Row> found;

  if (auto err = open(sys, trx, IB_LOCK_IS, &crsr); err != DB_SUCCESS) {
    ADD_FAILURE() << "open: " << err;
    return found;
  }

  if (auto err = crsr->open_index(sys, SEC_A, &sec); err != DB_SUCCESS) {
    ADD_FAILURE() << "open_index: " << err;
  } else {
    sec->set_cluster_access();

    auto key = sec->sec_search_tuple_create();
    int res{-1};

    err = key->write_u32(0, a);

    if (err == DB_SUCCESS) {
      err = sec->moveto(sys, key, IB_CUR_GE, &res);
    }

    api::Tuple::destroy(key);

    if (err == DB_SUCCESS && res == 0) {
      auto tpl = sec->clust_read_tuple_create();

      if (sec->read_row(sys, tpl) == DB_SUCCESS) {
        found = read_row(tpl);
      }

      api::Tuple::destroy(tpl);
    } else if (err != DB_SUCCESS && err != DB_END_OF_INDEX && err != DB_RECORD_NOT_FOUND) {
      ADD_FAILURE() << "moveto: " << err;
    }

    (void)api::Cursor::close(sys, sec);
  }

  (void)api::Cursor::close(sys, crsr);

  return found;
}

void DbTest::expect_rows(const Rows &expected) {
  auto trx = begin();

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

  /* The secondary index has an entry for each row, in (a, k) order. */
  const auto sec = scan_sec(trx);

  EXPECT_EQ(sec.size(), expected.size());

  for (size_t i{}; i < sec.size(); ++i) {
    auto it = expected.find(sec[i].m_k);

    if (it == expected.end() || !(it->second == sec[i])) {
      ADD_FAILURE() << "SEC_A row " << i << " k " << sec[i].m_k << " a " << sec[i].m_a << " not as expected";
    }

    if (i > 0 && std::pair(sec[i - 1].m_a, sec[i - 1].m_k) >= std::pair(sec[i].m_a, sec[i].m_k)) {
      ADD_FAILURE() << "SEC_A out of order at " << i << ": (a " << sec[i - 1].m_a << ", k " << sec[i - 1].m_k << ") before (a "
                    << sec[i].m_a << ", k " << sec[i].m_k << ")";
    }
  }

  commit(trx);
}

void DbTest::load(const Rows &rows) {
  auto trx = begin();

  for (const auto &[k, row] : rows) {
    ASSERT_EQ(insert(trx, row), DB_SUCCESS) << "k " << k;
  }

  commit(trx);
}

}  // namespace ut

#if defined(__SANITIZE_THREAD__)
/** ThreadSanitizer defaults for the unit tests (TSAN_OPTIONS still
overrides them). allocator_may_return_null: the death tests
(UtMemDeathTest) check that ut_new()/ut_realloc() of an impossible size
die with "Cannot allocate"; that needs malloc to return nullptr, by default
TSan aborts in the allocator with its own report instead. */
extern "C" const char *__tsan_default_options() {
  return "allocator_may_return_null=1";
}
#endif /* __SANITIZE_THREAD__ */
