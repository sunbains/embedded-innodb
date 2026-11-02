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

/** @file unit-tests/test_system.cc
Lifecycle of a System: create a database, shut it down, open it again. */

#include "srv0srv.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <format>

namespace fs = std::filesystem;

class SystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_dir = fs::temp_directory_path() / std::format("innodb-system-test-{}", ::getpid());
    fs::remove_all(m_dir);
    fs::create_directories(m_dir);
  }

  void TearDown() override { fs::remove_all(m_dir); }

  [[nodiscard]] Config make_config() const {
    Config config;

    config.m_fil.m_data_home = m_dir.string();
    config.m_log.m_group_home_dir = m_dir.string();
    config.m_buf_pool.m_size = 32 * 1024 * 1024;

    return config;
  }

  fs::path m_dir;
};

TEST_F(SystemTest, CreateShutdownReopen) {
  const auto config = make_config();

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);
    ASSERT_TRUE(fs::exists(m_dir / "system.ibd"));
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }

  {
    System sys;

    ASSERT_EQ(sys.init(config), DB_SUCCESS);
    ASSERT_EQ(sys.startup(), DB_SUCCESS);
    ASSERT_EQ(sys.shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
  }
}

TEST_F(SystemTest, InvalidConfigIsRejected) {
  auto config = make_config();

  config.m_log.m_n_files = 1;

  System sys;

  ASSERT_NE(sys.init(config), DB_SUCCESS);
}
