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
