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

/** @file unit-tests/test_mach.cc
Big-endian fixed width integers: every width reads back what was written. */

#include "mach0data.h"

#include <gtest/gtest.h>

#include <array>

TEST(MachTest, SixBytesRoundTrip) {
  for (const uint64_t v : {0ULL, 1ULL, 0x0000123456789ABCULL, 0x0000FFFFFFFFFFFFULL}) {
    std::array<byte, 8> buf{};

    mach_write_to_6(buf.data(), v);
    EXPECT_EQ(mach_read_from_6(buf.data()), v);
  }
}

TEST(MachTest, SevenBytesRoundTrip) {
  /* Roll pointers are 7 bytes with the insert flag in bit 55. */
  for (const uint64_t v : {0ULL, 1ULL, 0x00800000002A0084ULL, 0x00FFFFFFFFFFFFFFULL}) {
    std::array<byte, 8> buf{};

    mach_write_to_7(buf.data(), v);
    EXPECT_EQ(mach_read_from_7(buf.data()), v);
  }
}

TEST(MachTest, ReadDoesNotLookPastTheField) {
  /* The byte after the field must not leak into the value. */
  std::array<byte, 8> buf;

  buf.fill(0xFF);
  mach_write_to_6(buf.data(), 0x010203040506ULL);
  EXPECT_EQ(mach_read_from_6(buf.data()), 0x010203040506ULL);

  buf.fill(0xFF);
  mach_write_to_7(buf.data(), 0x01020304050607ULL);
  EXPECT_EQ(mach_read_from_7(buf.data()), 0x01020304050607ULL);
}

#include "trx0undo.h"

TEST(RollPtrTest, BuildDecodeAndInsertFlag) {
  for (const bool is_insert : {false, true}) {
    const auto roll_ptr = trx_undo_build_roll_ptr(is_insert, 5, 0x12345678, 0x9ABC);

    EXPECT_EQ(trx_undo_roll_ptr_is_insert(roll_ptr), is_insert);

    bool decoded_insert;
    ulint rseg_id;
    page_no_t page_no;
    ulint offset;

    trx_undo_decode_roll_ptr(roll_ptr, &decoded_insert, &rseg_id, &page_no, &offset);

    EXPECT_EQ(decoded_insert, is_insert);
    EXPECT_EQ(rseg_id, 5UL);
    EXPECT_EQ(page_no, 0x12345678U);
    EXPECT_EQ(offset, 0x9ABCUL);

    /* A roll pointer survives the trip through a record. */
    std::array<byte, 8> buf{};

    trx_write_roll_ptr(buf.data(), roll_ptr);
    EXPECT_EQ(trx_read_roll_ptr(buf.data()), roll_ptr);
  }
}
