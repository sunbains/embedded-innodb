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

/** @file unit-tests/ut0env.h
Scaffolding shared by the component unit tests.

  SysTest   A System that is init()ed only: every sub-system exists, nothing
            is on disk. For the components that work in memory (LockSys,
            Mtr, the dictionary cache, ...).

  DbTest    A System started in a directory of its own, with a test table
            and helpers to change and read it, restart the instance, or kill
            it the hard way and recover.

Each component has its own test program, test_<component>, built with
ib_unit_test() in unit-tests/CMakeLists.txt. They all carry the ctest label
"unit": `cmake --build build --target unit` builds and runs them. */

#pragma once

#include "api0api.h"
#include "srv0srv.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace ut {

namespace fs = std::filesystem;

/** A directory of its own for a test, under $TMPDIR, removed at the end. */
struct TestDir {
  TestDir();
  ~TestDir();

  TestDir(const TestDir &) = delete;
  TestDir &operator=(const TestDir &) = delete;

  fs::path m_path;
};

/** The configuration the tests use: small, data and log in dir. */
[[nodiscard]] Config make_config(const fs::path &dir);

/** A System with its sub-systems created, nothing on disk. */
class SysTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  Config m_config;
  std::unique_ptr<System> m_sys;
};

/** A row of the test table:
CREATE TABLE test/t (k INT UNSIGNED, a INT UNSIGNED, b BLOB,
  PRIMARY KEY(k), INDEX SEC_A(a)) */
struct Row {
  uint32_t m_k{};
  uint32_t m_a{};
  std::string m_b{};

  bool operator==(const Row &) const = default;
};

using Rows = std::map<uint32_t, Row>;

/** The test table's name. */
constexpr const char *TABLE = "test/t";

/** The secondary index on a. */
constexpr const char *SEC_A = "SEC_A";

/** A blob of n bytes derived from seed, n can span many pages. */
[[nodiscard]] std::string blob(uint32_t seed, size_t n = 100);

/** A started System in a directory of its own, with the test table. */
class DbTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  /** Start an instance on m_dir.
  @param[in] create_table       Create the test table if it doesn't exist. */
  void start(bool create_table = true);

  /** Shut the instance down normally. */
  void stop();

  /** Shut down normally and start again: the data must come back. */
  void restart();

  /** Run fn in a child process on this database and kill it with SIGKILL
  at the end of fn, no shutdown: the parent then starts the instance,
  which runs crash recovery. The instance of the parent is stopped first.
  In the child, m_sys is the child's instance: fn uses the helpers. */
  void crash(const std::function<void()> &fn);

  /** @return a new transaction. */
  [[nodiscard]] Trx *begin(TrxLevel level = IB_TRX_REPEATABLE_READ);

  /** Commit trx. */
  void commit(Trx *trx);

  /** Roll trx back. */
  void rollback(Trx *trx);

  /** INSERT a row in trx. */
  [[nodiscard]] DbErr insert(Trx *trx, const Row &row);

  /** UPDATE the row with key k to row (row.m_k may differ: the key moves). */
  [[nodiscard]] DbErr update(Trx *trx, uint32_t k, const Row &row);

  /** DELETE the row with key k. */
  [[nodiscard]] DbErr remove(Trx *trx, uint32_t k);

  /** @return the rows visible to trx, read through the clustered index. */
  [[nodiscard]] Rows scan(Trx *trx);

  /** @return the rows visible to trx, read through SEC_A, in (a, k) order. */
  [[nodiscard]] std::vector<Row> scan_sec(Trx *trx);

  /** Read the row with SEC_A value a through SEC_A with cluster access.
  @return the row, or nullopt if no row has a. */
  [[nodiscard]] std::optional<Row> find_sec(Trx *trx, uint32_t a);

  /** Check the table holds exactly expected, through both indexes. */
  void expect_rows(const Rows &expected);

  /** Insert the rows in one committed transaction. */
  void load(const Rows &rows);

  TestDir m_dir;
  Config m_config;
  std::unique_ptr<System> m_sys;
};

/** @return the rows read from crsr from its current position to the end. */
[[nodiscard]] std::vector<Row> read_all(System &sys, api::Cursor *crsr, DbErr err);

}  // namespace ut
