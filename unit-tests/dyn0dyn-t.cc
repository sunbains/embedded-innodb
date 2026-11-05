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

  ut_d(m_dyn_arr.eq(0, test_str, len));
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

  ut_d(m_dyn_arr.eq(0, test_str1, len1));
  ut_d(m_dyn_arr.eq(len1, test_str2, len2));
}

// Test pushing elements that require multiple blocks
TEST_F(DynArrayTest, PushMultipleBlocks) {
  // Create a string that's larger than DYN_ARRAY_DATA_SIZE
  std::string large_str(DYN_ARRAY_DATA_SIZE + 100, 'A');

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(large_str.data()), large_str.size());

  // Verify data is split across blocks
  EXPECT_GT(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE);
  ut_d(m_dyn_arr.eq(0, large_str.c_str(), large_str.size()));
}

// Test open/close buffer functionality
TEST_F(DynArrayTest, OpenCloseBuffer) {
  const char *test_str = "Hello World";
  size_t len = strlen(test_str);

  auto buf = m_dyn_arr.open(len);
  std::memcpy(buf, test_str, len);
  m_dyn_arr.close(buf + len);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len);
  ut_d(m_dyn_arr.eq(0, test_str, len));
}

// Test push_string method
TEST_F(DynArrayTest, PushString) {
  const char *test_str = "Hello World";
  size_t len = strlen(test_str);

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(test_str), len);

  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_first_block()), len);
  ut_d(m_dyn_arr.eq(0, test_str, len));
}

// Test large string that requires multiple blocks
TEST_F(DynArrayTest, PushLargeString) {
  std::string large_str(DYN_ARRAY_DATA_SIZE + 100, 'B');

  m_dyn_arr.push_string(reinterpret_cast<const byte *>(large_str.data()), large_str.size());

  EXPECT_GT(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE);
  ut_d(m_dyn_arr.eq(0, large_str.c_str(), large_str.size()));
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
  ut_d(m_dyn_arr.eq(0, str1.c_str(), str1.size()));
  ut_d(m_dyn_arr.eq(str1.size(), str2.c_str(), str2.size()));
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
#ifdef UNIV_DEBUG
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
#endif /* UNIV_DEBUG */

/** clear() empties the array: one block, nothing used, ready for reuse. */
TEST_F(DynArrayTest, Clear) {
  m_dyn_arr.push_string(reinterpret_cast<const byte *>("abc"), 3);
  m_dyn_arr.clear();

  EXPECT_EQ(m_dyn_arr.get_data_size(), 0UL);
  EXPECT_TRUE(m_dyn_arr.single_block());
  EXPECT_EQ(m_dyn_arr.get_next_block(m_dyn_arr.get_first_block()), nullptr);
}

/** clear() of an array of several blocks frees all but the first. */
TEST_F(DynArrayTest, ClearBlocks) {
  std::string s(5000, 'x');

  for (int round = 0; round < 3; ++round) {
    m_dyn_arr.push_string(reinterpret_cast<const byte *>(s.data()), s.size());

    EXPECT_EQ(m_dyn_arr.get_data_size(), s.size());
    EXPECT_FALSE(m_dyn_arr.single_block());

    m_dyn_arr.clear();

    EXPECT_EQ(m_dyn_arr.get_data_size(), 0UL);
    EXPECT_TRUE(m_dyn_arr.single_block());
    EXPECT_EQ(m_dyn_arr.get_first_block(), m_dyn_arr.get_last_block());
  }

  /* Usable after the clear. */
  auto ptr = m_dyn_arr.push(4);

  std::memcpy(ptr, "wxyz", 4);
  ut_d(m_dyn_arr.eq(0, "wxyz", 4));
}

#include "ut0perf.h"

#include <random>
#include <vector>

namespace {

/** @return the bytes of arr, read block by block. */
std::vector<byte> contents(Dyn_array &arr) {
  std::vector<byte> v;

  for (auto block = arr.get_first_block(); block != nullptr; block = arr.get_next_block(block)) {
    auto data = arr.get_data(block);

    v.insert(v.end(), data, data + arr.get_used(block));
  }

  return v;
}

/** @return the number of blocks of arr. */
size_t n_blocks(const Dyn_array &arr) {
  size_t n{};

  for (auto block = arr.get_first_block(); block != nullptr; block = arr.get_next_block(block)) {
    ++n;
  }

  return n;
}

/** A push that exactly fills the block stays in it. */
TEST_F(DynArrayTest, PushExactFill) {
  (void)m_dyn_arr.push(DYN_ARRAY_DATA_SIZE - 10);
  (void)m_dyn_arr.push(10);

  EXPECT_TRUE(m_dyn_arr.single_block());
  EXPECT_EQ(n_blocks(m_dyn_arr), 1UL);
  EXPECT_EQ(m_dyn_arr.get_data_size(), ulint(DYN_ARRAY_DATA_SIZE));
}

/** A push that does not fit goes whole into a new block: an element is
never split, the old block keeps its used size and gets the full flag. */
TEST_F(DynArrayTest, PushNoSplit) {
  auto p1 = static_cast<byte *>(m_dyn_arr.push(DYN_ARRAY_DATA_SIZE - 10));
  auto p2 = static_cast<byte *>(m_dyn_arr.push(11));

  EXPECT_FALSE(m_dyn_arr.single_block());
  EXPECT_EQ(n_blocks(m_dyn_arr), 2UL);

  auto first = m_dyn_arr.get_first_block();
  auto second = m_dyn_arr.get_next_block(first);

  EXPECT_EQ(m_dyn_arr.get_used(first), DYN_ARRAY_DATA_SIZE - 10);
  EXPECT_NE(first->m_used & DYN_BLOCK_FULL_FLAG, 0U);
  EXPECT_EQ(m_dyn_arr.get_used(second), 11U);
  EXPECT_EQ(second->m_used & DYN_BLOCK_FULL_FLAG, 0U);

  EXPECT_EQ(p1, m_dyn_arr.get_data(first));
  EXPECT_EQ(p2, m_dyn_arr.get_data(second));
  EXPECT_EQ(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE + 1UL);
}

/** A whole block in one push. */
TEST_F(DynArrayTest, PushWholeBlock) {
  (void)m_dyn_arr.push(1);
  (void)m_dyn_arr.push(DYN_ARRAY_DATA_SIZE);

  EXPECT_EQ(n_blocks(m_dyn_arr), 2UL);
  EXPECT_EQ(m_dyn_arr.get_used(m_dyn_arr.get_last_block()), DYN_ARRAY_DATA_SIZE);
  EXPECT_EQ(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE + 1UL);
}

/** Fixed size slots, like the mtr memo: get_element() finds every slot by
its offset, in any block. */
TEST_F(DynArrayTest, GetElementSlots) {
  struct Slot {
    uint64_t m_a;
    uint64_t m_b;
  };

  constexpr ulint N = 1000;

  for (ulint i = 0; i < N; ++i) {
    *static_cast<Slot *>(m_dyn_arr.push(sizeof(Slot))) = Slot{i, ~i};
  }

  EXPECT_EQ(m_dyn_arr.get_data_size(), N * sizeof(Slot));
  EXPECT_EQ(n_blocks(m_dyn_arr), (N * sizeof(Slot) + DYN_ARRAY_DATA_SIZE - 1) / DYN_ARRAY_DATA_SIZE);

  for (ulint i = 0; i < N; ++i) {
    auto slot = static_cast<Slot *>(m_dyn_arr.get_element(i * sizeof(Slot)));

    ASSERT_EQ(slot->m_a, i);
    ASSERT_EQ(slot->m_b, ~i);
  }
}

/** Odd sized elements leave unused bytes at the end of the blocks:
get_element() skips them, an offset is a logical position. */
TEST_F(DynArrayTest, GetElementOddSizes) {
  std::vector<ulint> offsets;
  ulint off{};

  for (ulint i = 0; i < 300; ++i) {
    const ulint len = 1 + (i * 37) % 200;
    auto p = static_cast<byte *>(m_dyn_arr.push(len));

    memset(p, int(i & 0xFF), len);
    offsets.push_back(off);
    off += len;
  }

  EXPECT_EQ(m_dyn_arr.get_data_size(), off);

  for (ulint i = 0; i < offsets.size(); ++i) {
    EXPECT_EQ(*static_cast<byte *>(m_dyn_arr.get_element(offsets[i])), byte(i & 0xFF)) << i;
  }
}

/** close() records the bytes used, not the bytes opened. */
TEST_F(DynArrayTest, OpenClosePartial) {
  auto p = m_dyn_arr.open(100);

  memcpy(p, "abc", 3);
  m_dyn_arr.close(p + 3);

  EXPECT_EQ(m_dyn_arr.get_data_size(), 3UL);

  p = m_dyn_arr.open(50);
  m_dyn_arr.close(p);

  EXPECT_EQ(m_dyn_arr.get_data_size(), 3UL);
  ut_d(m_dyn_arr.eq(0, "abc", 3));
}

/** open() of more than is left in the block opens in a new block, the
bytes are contiguous. */
TEST_F(DynArrayTest, OpenNewBlock) {
  (void)m_dyn_arr.push(DYN_ARRAY_DATA_SIZE - 5);

  auto p = m_dyn_arr.open(10);

  for (int i = 0; i < 10; ++i) {
    p[i] = byte(i);
  }

  m_dyn_arr.close(p + 10);

  EXPECT_EQ(n_blocks(m_dyn_arr), 2UL);
  EXPECT_EQ(p, m_dyn_arr.get_data(m_dyn_arr.get_last_block()));
  EXPECT_EQ(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE + 5UL);

  /* A small open after that goes on in the last block. */
  auto q = m_dyn_arr.open(5);

  EXPECT_EQ(q, p + 10);
  m_dyn_arr.close(q + 5);
  EXPECT_EQ(m_dyn_arr.get_data_size(), DYN_ARRAY_DATA_SIZE + 10UL);
}

/** An open() that leaves the first block with room must still append after
the last block: the first block is full once a second block exists. */
TEST_F(DynArrayTest, OpenAfterManyBlocks) {
  std::vector<byte> model;

  for (int i = 0; i < 50; ++i) {
    const ulint len = i % 2 == 0 ? 400 : 3;
    auto p = m_dyn_arr.open(len);

    for (ulint j = 0; j < len; ++j) {
      p[j] = byte(i + j);
      model.push_back(byte(i + j));
    }

    m_dyn_arr.close(p + len);
  }

  EXPECT_EQ(contents(m_dyn_arr), model);
}

/** push_string() of exact multiples of the block size fills whole blocks. */
TEST_F(DynArrayTest, PushStringMultiples) {
  std::vector<byte> s(3 * DYN_ARRAY_DATA_SIZE);

  for (size_t i = 0; i < s.size(); ++i) {
    s[i] = byte(i * 13);
  }

  m_dyn_arr.push_string(s.data(), s.size());

  EXPECT_EQ(n_blocks(m_dyn_arr), 3UL);
  EXPECT_EQ(contents(m_dyn_arr), s);
}

/** A random mix of push, push_string and open/close against a model. */
TEST_F(DynArrayTest, RandomModel) {
  std::mt19937 rng(3);
  std::vector<byte> model;

  for (int i = 0; i < 5000; ++i) {
    std::vector<byte> v(1 + rng() % (rng() % 10 == 0 ? 2000 : 100));

    for (auto &b : v) {
      b = byte(rng());
    }

    switch (rng() % 3) {
      case 0:
        if (v.size() <= DYN_ARRAY_DATA_SIZE) {
          memcpy(m_dyn_arr.push(v.size()), v.data(), v.size());
          break;
        }
        [[fallthrough]];
      case 1:
        m_dyn_arr.push_string(v.data(), v.size());
        break;
      default:
        if (v.size() > DYN_ARRAY_DATA_SIZE) {
          v.resize(DYN_ARRAY_DATA_SIZE);
        }

        auto p = m_dyn_arr.open(v.size());

        memcpy(p, v.data(), v.size());
        m_dyn_arr.close(p + v.size());
    }

    model.insert(model.end(), v.begin(), v.end());

    if (i % 500 == 0) {
      ASSERT_EQ(m_dyn_arr.get_data_size(), model.size());
    }
  }

  EXPECT_EQ(m_dyn_arr.get_data_size(), model.size());
  EXPECT_EQ(contents(m_dyn_arr), model);
}

/** clear() between rounds of many blocks: no block is lost or reused
twice (the memory checks of the debug build catch either). */
TEST_F(DynArrayTest, ClearManyRounds) {
  for (int round = 0; round < 100; ++round) {
    for (int i = 0; i < round; ++i) {
      (void)m_dyn_arr.push(200);
    }

    EXPECT_EQ(m_dyn_arr.get_data_size(), ulint(round) * 200);

    m_dyn_arr.clear();

    EXPECT_TRUE(m_dyn_arr.single_block());
    EXPECT_EQ(m_dyn_arr.get_first_block()->m_used, 0U);
  }
}

/** A template instance of another block size. */
TEST(DynArraySizeTest, SmallBlocks) {
  DynamicArray<16> arr;

  for (int i = 0; i < 100; ++i) {
    *static_cast<uint32_t *>(arr.push(4)) = uint32_t(i);
  }

  EXPECT_EQ(arr.get_data_size(), 400UL);

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(*static_cast<uint32_t *>(arr.get_element(i * 4)), uint32_t(i));
  }
}

}  // namespace

namespace {

/** Push of a memo slot sized element, many blocks, then a clear. */
TEST(DynArrayPerf, Push) {
  Dyn_array arr;

  auto r = ut::bench("push_16", 10000000, [&](uint64_t i) {
    *static_cast<uint64_t *>(arr.push(16)) = i;

    if ((i & 1023) == 1023) {
      arr.clear();
    }
  });

  EXPECT_LT(r.ns_per_op(), 1000.0);
}

/** push_string() of 100 bytes. */
TEST(DynArrayPerf, PushString) {
  Dyn_array arr;
  std::array<byte, 100> s{};

  auto r = ut::bench("push_string_100", 5000000, [&](uint64_t i) {
    arr.push_string(s.data(), s.size());

    if ((i & 255) == 255) {
      arr.clear();
    }
  });

  EXPECT_LT(r.ns_per_op(), 2000.0);
}

/** open/close of a log record sized buffer in a single block array. */
TEST(DynArrayPerf, OpenClose) {
  Dyn_array arr;

  auto r = ut::bench("open_close_18", 10000000, [&](uint64_t i) {
    auto p = arr.open(18);

    p[0] = byte(i);
    arr.close(p + 7);

    if ((i & 63) == 63) {
      arr.clear();
    }
  });

  EXPECT_LT(r.ns_per_op(), 1000.0);
}

/** get_element() in a 64 block array: linear in the number of blocks. */
TEST(DynArrayPerf, GetElement) {
  Dyn_array arr;

  for (int i = 0; i < 64 * 32; ++i) {
    (void)arr.push(16);
  }

  uint64_t sum{};

  auto r = ut::bench("get_element_64_blocks", 1000000, [&](uint64_t i) {
    sum += reinterpret_cast<uintptr_t>(arr.get_element((i % (64 * 32)) * 16));
  });

  ut::keep(sum);
  EXPECT_LT(r.ns_per_op(), 20000.0);
}

}  // namespace
