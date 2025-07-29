/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

#include <cstdint>
#include <cstring>

#include "innodb0types.h"

#include "dyn0dyn.h"
#include "log0log.h"
#include "mem0mem.h"
#include "ut0lst.h"

#include "gtest/gtest.h"

class DynArrayTest : public ::testing::Test {
 protected:
  Dyn_array m_dyn_arr{};

  void SetUp() override {}

  void TearDown() override {}
};

// Test basic construction and destruction
TEST_F(DynArrayTest, Construction) {
  EXPECT_EQ(m_dyn_arr.get_first_block(), m_dyn_arr.get_last_block());
  EXPECT_EQ(m_dyn_arr.get_next_block(m_dyn_arr.get_first_block()), nullptr);
}

// Test pushing single elements
TEST_F(DynArrayTest, PushSingleElement) {
  const char *test_str = "Hello";
  auto len = strlen(test_str);

  void *ptr = m_dyn_arr.push(len);
  std::memcpy(ptr, test_str, len);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len);

  m_dyn_arr.eq(0, test_str, len);
}

// Test pushing multiple elements that fit in one block
TEST_F(DynArrayTest, PushMultipleElementsSingleBlock) {
  const char *test_str1 = "Hello";
  const char *test_str2 = "World";
  size_t len1 = strlen(test_str1);
  size_t len2 = strlen(test_str2);

  auto ptr1 = m_dyn_arr.push(len1);
  std::memcpy(ptr1, test_str1, len1);

  auto ptr2 = m_dyn_arr.push(len2);
  std::memcpy(ptr2, test_str2, len2);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len1 + len2);

  m_dyn_arr.eq(0, test_str1, len1);
  m_dyn_arr.eq(len1, test_str2, len2);
}

// Test pushing elements that require multiple blocks
TEST_F(DynArrayTest, PushMultipleBlocks) {
  // Create a string that's larger than DYN_ARRAY_DATA_SIZE
  std::string large_str(DYN_ARRAY_DATA_SIZE + 100, 'A');

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(large_str.data()), large_str.size());

  // Verify data is split across blocks
  EXPECT_GT(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE);
  m_dyn_arr.eq(0, large_str.c_str(), large_str.size());
}

// Test open/close buffer functionality
TEST_F(DynArrayTest, OpenCloseBuffer) {
  const char *test_str = "Hello World";
  size_t len = strlen(test_str);

  auto buf = m_dyn_arr.open(len);
  std::memcpy(buf, test_str, len);
  m_dyn_arr.close(buf + len);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len);
  m_dyn_arr.eq(0, test_str, len);
}

// Test push_string method
TEST_F(DynArrayTest, PushString) {
  const char *test_str = "Hello World";
  size_t len = strlen(test_str);

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(test_str), len);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len);
  m_dyn_arr.eq(0, test_str, len);
}

// Test large string that requires multiple blocks
TEST_F(DynArrayTest, PushLargeString) {
  std::string large_str(DYN_ARRAY_DATA_SIZE + 100, 'B');

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(large_str.data()), large_str.size());

  EXPECT_GT(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE);
  m_dyn_arr.eq(0, large_str.c_str(), large_str.size());
}

// Test get_data_size
TEST_F(DynArrayTest, GetDataSize) {
  const char *test_str = "Hello World";
  size_t len = strlen(test_str);

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(test_str), len);

  EXPECT_EQ(m_dyn_arr.get_data_size(), len);
}

// Test get_element with multiple blocks
TEST_F(DynArrayTest, GetElementMultipleBlocks) {
  // Create two strings that will span multiple blocks
  std::string str1(DYN_ARRAY_DATA_SIZE - 10, 'A');
  std::string str2(20, 'B');

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(str1.data()), str1.size());
  m_dyn_arr.push_string(reinterpret_cast<const byte *>(str2.data()), str2.size());

  // Test accessing elements in both blocks
  m_dyn_arr.eq(0, str1.c_str(), str1.size());
  m_dyn_arr.eq(str1.size(), str2.c_str(), str2.size());
}

// Test block full flag
TEST_F(DynArrayTest, BlockNotFullFlag) {
  // Fill a block completely
  std::string full_block(DYN_ARRAY_DATA_SIZE, 'D');
  m_dyn_arr.push_string(reinterpret_cast<const byte *>(full_block.data()), full_block.size());

  // Verify block is marked as full
  EXPECT_TRUE(!(m_dyn_arr.get_last_block()->m_used & DYN_BLOCK_FULL_FLAG));
}

TEST_F(DynArrayTest, BlockFullFlag) {
  // Fill a block completely
  std::string full_block(DYN_ARRAY_DATA_SIZE + 1, 'D');
  m_dyn_arr.push_string(reinterpret_cast<const byte *>(full_block.data()), full_block.size());

  // Verify block is marked as full
  EXPECT_TRUE(m_dyn_arr.get_first_block()->m_used & DYN_BLOCK_FULL_FLAG);
}

// Test error cases
TEST_F(DynArrayTest, ErrorCases) {
  // Test pushing zero size
  EXPECT_DEATH(m_dyn_arr.push(0), ".*");

  // Test pushing size larger than block
  EXPECT_DEATH(m_dyn_arr.push(DYN_ARRAY_DATA_SIZE + 1), ".*");

  // Test opening buffer with zero size
  EXPECT_DEATH(m_dyn_arr.open(0), ".*");

  // Test opening buffer with size larger than block
  EXPECT_DEATH(m_dyn_arr.open(DYN_ARRAY_DATA_SIZE + 1), ".*");
}
