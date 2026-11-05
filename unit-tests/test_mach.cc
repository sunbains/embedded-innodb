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

/** The stored bytes of integers order them with memcmp(): unsigned values
as they are, signed values with negative values first. The sign bit was
flipped for the unsigned values instead of the signed ones: unsigned
values >= 2^31 sorted before 0, negative values after the positive. */
TEST(MachTest, IntTypeOrder) {
  auto store = [](auto v, bool usign) {
    std::array<byte, sizeof(v)> b{};

    mach_write_int_type(b.data(), reinterpret_cast<const byte *>(&v), sizeof(v), usign);

    return b;
  };

  const std::array<uint32_t, 5> u{0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF};

  for (size_t i = 1; i < u.size(); ++i) {
    EXPECT_LT(store(u[i - 1], true), store(u[i], true)) << u[i - 1] << " " << u[i];
  }

  const std::array<int32_t, 5> s{INT32_MIN, -1, 0, 1, INT32_MAX};

  for (size_t i = 1; i < s.size(); ++i) {
    EXPECT_LT(store(s[i - 1], false), store(s[i], false)) << s[i - 1] << " " << s[i];
  }

  const std::array<int64_t, 4> s64{INT64_MIN, -5, 5, INT64_MAX};

  for (size_t i = 1; i < s64.size(); ++i) {
    EXPECT_LT(store(s64[i - 1], false), store(s64[i], false));
  }
}

/** What is written is read back, signed and unsigned, of every width. */
TEST(MachTest, IntTypeRoundTrip) {
  auto round_trip = [](auto v, bool usign) {
    std::array<byte, sizeof(v)> b{};
    decltype(v) r{};

    mach_write_int_type(b.data(), reinterpret_cast<const byte *>(&v), sizeof(v), usign);
    mach_read_int_type(&r, b.data(), sizeof(v), usign);

    return r;
  };

  for (int64_t v : {INT64_MIN, int64_t(-1), int64_t(0), int64_t(42), INT64_MAX}) {
    EXPECT_EQ(round_trip(v, false), v);
    EXPECT_EQ(round_trip(int32_t(v), false), int32_t(v));
    EXPECT_EQ(round_trip(int16_t(v), false), int16_t(v));
    EXPECT_EQ(round_trip(int8_t(v), false), int8_t(v));
    EXPECT_EQ(round_trip(uint64_t(v), true), uint64_t(v));
    EXPECT_EQ(round_trip(uint32_t(v), true), uint32_t(v));
  }

  std::array<byte, 8> b{};

  mach_write_uint64(b.data(), 0x8000000000000001ULL);
  EXPECT_EQ(mach_read_uint64(b.data()), 0x8000000000000001ULL);
  EXPECT_EQ(b[0], 0x80);

  mach_write_int64(b.data(), -2);
  EXPECT_EQ(mach_read_int64(b.data()), -2);
  EXPECT_EQ(b[0], 0x7F);
}

#include "ut0perf.h"

#include <cmath>
#include <limits>
#include <random>
#include <vector>

namespace {

/** A 32-bit varint and the size of its encoding. */
struct Enc {
  ulint m_val;
  ulint m_size;
};

/** Every size boundary of the varint 32-bit encoding, on both sides:
1-4 bytes for small values, 5 bytes in the middle, and the "extended" 2-4
byte forms for values close to 2^32 (small negative numbers). */
constexpr Enc ENC32[] = {
  {0, 1},
  {1, 1},
  {0x7F, 1},
  {0x80, 2},
  {0x3FFF, 2},
  {0x4000, 3},
  {0x1FFFFF, 3},
  {0x200000, 4},
  {0xFFFFFFF, 4},
  {0x10000000, 5},
  {0x80000000, 5},
  {0xFEFFFFFF, 5},
  {0xFF000000, 4},
  {0xFFFDFFFF, 4},
  {0xFFFE0000, 3},
  {0xFFFFFBFF, 3},
  {0xFFFFFC00, 2},
  {0xFFFFFFFE, 2},
  {0xFFFFFFFF, 2},
};

/** The 64-bit values whose high half crosses the size boundaries. */
constexpr uint64_t VAL64[] = {
  0,
  1,
  0xFFFFFFFFULL,
  0x100000000ULL,
  0x7FFFFFFFFFFFFFFFULL,
  0x0000007F00000000ULL,
  0x0000008000000000ULL,
  0x00003FFF12345678ULL,
  0x0000400000000001ULL,
  0x0FFFFFFF00000000ULL,
  0x1000000000000000ULL,
  0x8000000000000000ULL,
  0xFF000000ABCDEF01ULL,
  0xFFFFFC0000000000ULL,
  0xFFFFFFFFFFFFFFFFULL,
};

/** mach_write_to_n writes big endian: the most significant byte first. */
TEST(MachTest, FixedBigEndian) {
  std::array<byte, 8> b{};

  mach_write_to_1(b.data(), 0xA5);
  EXPECT_EQ(b[0], 0xA5);
  EXPECT_EQ(mach_read_from_1(b.data()), 0xA5UL);

  mach_write_to_2(b.data(), 0x1234);
  EXPECT_EQ(b[0], 0x12);
  EXPECT_EQ(b[1], 0x34);
  EXPECT_EQ(mach_read_from_2(b.data()), 0x1234UL);

  mach_write_to_3(b.data(), 0x123456);
  EXPECT_EQ(b[0], 0x12);
  EXPECT_EQ(b[2], 0x56);
  EXPECT_EQ(mach_read_from_3(b.data()), 0x123456UL);

  mach_write_to_4(b.data(), 0x89ABCDEF);
  EXPECT_EQ(b[0], 0x89);
  EXPECT_EQ(b[3], 0xEF);
  EXPECT_EQ(mach_read_from_4(b.data()), 0x89ABCDEFUL);

  mach_write_to_8(b.data(), 0x0102030405060708ULL);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(b[i], i + 1);
  }

  EXPECT_EQ(mach_read_from_8(b.data()), 0x0102030405060708ULL);
}

/** The extreme values of each fixed width read back unchanged. */
TEST(MachTest, FixedExtremes) {
  std::array<byte, 8> b{};

  for (ulint v : {0UL, 1UL, 0x7FUL, 0x80UL, 0xFFUL}) {
    mach_write_to_1(b.data(), v);
    EXPECT_EQ(mach_read_from_1(b.data()), v);
  }

  for (ulint v : {0UL, 0xFFUL, 0x100UL, 0x7FFFUL, 0x8000UL, 0xFFFFUL}) {
    mach_write_to_2(b.data(), v);
    EXPECT_EQ(mach_read_from_2(b.data()), v);
  }

  for (uint32_t v : {0U, 0xFFFFU, 0x10000U, 0xFFFFFFU}) {
    mach_write_to_3(b.data(), v);
    EXPECT_EQ(mach_read_from_3(b.data()), v);
  }

  for (uint32_t v : {0U, 0xFFFFFFU, 0x1000000U, 0x7FFFFFFFU, 0x80000000U, 0xFFFFFFFFU}) {
    mach_write_to_4(b.data(), v);
    EXPECT_EQ(mach_read_from_4(b.data()), v);
  }

  for (uint64_t v : VAL64) {
    mach_write_to_8(b.data(), v);
    EXPECT_EQ(mach_read_from_8(b.data()), v);
  }
}

/** mach_write_to_3 keeps the low 24 bits of a wider value: callers must not
pass more (the debug check is commented out). */
TEST(MachTest, ThreeBytesTruncates) {
  std::array<byte, 4> b{};

  b[3] = 0x77;
  mach_write_to_3(b.data(), 0xAB123456);
  EXPECT_EQ(mach_read_from_3(b.data()), 0x123456UL);
  EXPECT_EQ(b[3], 0x77);
}

/** The canonical 2-byte form compares bytewise. */
TEST(MachTest, Encode2) {
  for (ulint v : {0UL, 1UL, 0x1234UL, 0xFFFFUL}) {
    const auto e = mach_encode_2(v);
    const auto p = reinterpret_cast<const byte *>(&e);

    EXPECT_EQ(p[0], byte(v >> 8));
    EXPECT_EQ(p[1], byte(v));
    EXPECT_EQ(mach_decode_2(e), v);
  }
}

/** Each boundary value takes the documented size, and write, size, read and
the stream reader agree. */
TEST(MachTest, Varint32Sizes) {
  for (const auto &[v, size] : ENC32) {
    std::array<byte, 8> b;

    b.fill(0xEE);

    EXPECT_EQ(varint::encode(b.data(), v), size) << std::hex << v;
    EXPECT_EQ(varint::size(v), size) << std::hex << v;
    EXPECT_EQ(varint::decode(b.data()), v) << std::hex << v;

    /* Nothing written past the encoding. */
    EXPECT_EQ(b[size], 0xEE) << std::hex << v;

    const byte *p = b.data();

    EXPECT_EQ(varint::decode_next(p), uint32_t(v)) << std::hex << v;
    EXPECT_EQ(p, b.data() + size) << std::hex << v;
  }
}

/** The first byte announces the length: 0xxxxxxx, 10xxxxxx, 110xxxxx,
1110xxxx, 11110000 and the extended 111110xx, 1111110x, 11111110. */
TEST(MachTest, Varint32Prefix) {
  std::array<byte, 8> b{};

  (void)varint::encode(b.data(), 0x7F);
  EXPECT_EQ(b[0], 0x7F);

  (void)varint::encode(b.data(), 0x80);
  EXPECT_EQ(b[0], 0x80);
  EXPECT_EQ(b[1], 0x80);

  (void)varint::encode(b.data(), 0x4000);
  EXPECT_EQ(b[0], 0xC0);

  (void)varint::encode(b.data(), 0x200000);
  EXPECT_EQ(b[0], 0xE0);

  (void)varint::encode(b.data(), 0x12345678);
  EXPECT_EQ(b[0], 0xF0);
  EXPECT_EQ(mach_read_from_4(b.data() + 1), 0x12345678UL);

  (void)varint::encode(b.data(), 0xFFFFFFFF);
  EXPECT_EQ(b[0] & 0xFC, 0xF8);

  (void)varint::encode(b.data(), 0xFFFE0000);
  EXPECT_EQ(b[0] & 0xFE, 0xFC);

  (void)varint::encode(b.data(), 0xFF000000);
  EXPECT_EQ(b[0], 0xFE);
}

/** The encoding preserves order within each size class but not across
the classes: it is not memcmp()-comparable and must not be used in keys. */
TEST(MachTest, Varint32SweepLow) {
  std::array<byte, 8> b{};

  for (ulint v = 0; v < (1UL << 22); v += 1 + v / 1024) {
    const auto n = varint::encode(b.data(), v);

    ASSERT_EQ(n, varint::size(v)) << v;
    ASSERT_EQ(varint::decode(b.data()), v) << v;
  }
}

/** Random 32-bit values, and the values near 2^32, round trip. */
TEST(MachTest, Varint32Random) {
  std::mt19937_64 rng(7);
  std::array<byte, 8> b{};

  for (int i = 0; i < 200000; ++i) {
    ulint v = uint32_t(rng());

    if (i % 4 == 1) {
      v = 0xFFFFFFFF - (v & 0xFFFFFF);
    } else if (i % 4 == 2) {
      v &= 0x3FFFFF;
    }

    const auto n = varint::encode(b.data(), v);

    ASSERT_EQ(n, varint::size(v)) << v;
    ASSERT_EQ(varint::decode(b.data()), v) << v;

    byte *p = b.data();

    ASSERT_EQ(varint::parse(p, b.data() + n), uint32_t(v)) << v;
    ASSERT_EQ(p, b.data() + n) << v;
  }
}

/** The log parser's reader takes exactly the encoding from a buffer that
holds it, and reports a truncated one with a null pointer. */
TEST(MachTest, ParseVarint) {
  for (const auto &[v, size] : ENC32) {
    std::array<byte, 8> b{};

    (void)varint::encode(b.data(), v);

    byte *p = b.data();

    EXPECT_EQ(varint::parse(p, b.data() + size), uint32_t(v)) << std::hex << v;
    EXPECT_EQ(p, b.data() + size) << std::hex << v;

    /* Bytes after the value don't matter. */
    p = b.data();
    EXPECT_EQ(varint::parse(p, b.data() + b.size()), uint32_t(v)) << std::hex << v;
    EXPECT_EQ(p, b.data() + size) << std::hex << v;

    for (ulint len = 0; len < size; ++len) {
      p = b.data();

      EXPECT_EQ(varint::parse(p, b.data() + len), 0U) << std::hex << v << " len " << len;
      EXPECT_EQ(p, nullptr) << std::hex << v << " len " << len;
    }
  }
}

/** Several values back to back are read in sequence. */
TEST(MachTest, ParseVarintStream) {
  std::vector<byte> buf(5 * std::size(ENC32));
  byte *w = buf.data();

  for (const auto &e : ENC32) {
    w += varint::encode(w, e.m_val);
  }

  byte *p = buf.data();

  for (const auto &e : ENC32) {
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(varint::parse(p, w), uint32_t(e.m_val));
  }

  EXPECT_EQ(p, w);
}

/** 64-bit varint: the high half a varint, the low half in 4 bytes. */
TEST(MachTest, Varint64) {
  for (auto v : VAL64) {
    std::array<byte, 16> b;

    b.fill(0xEE);

    const auto n = varint::encode64(b.data(), v);

    EXPECT_EQ(n, varint::size64(v)) << std::hex << v;
    EXPECT_EQ(n, 4 + varint::size(ulint(v >> 32))) << std::hex << v;
    EXPECT_GE(n, 5UL);
    EXPECT_LE(n, 9UL);
    EXPECT_EQ(b[n], 0xEE) << std::hex << v;
    EXPECT_EQ(varint::decode64(b.data()), v) << std::hex << v;
    EXPECT_EQ(mach_read_from_4(b.data() + n - 4), ulint(uint32_t(v))) << std::hex << v;
  }
}

/** The log parser's 64-bit reader: complete, with trailing bytes, and every
truncation of the encoding. */
TEST(MachTest, ParseVarint64) {
  for (auto v : VAL64) {
    std::array<byte, 16> b{};
    const auto n = varint::encode64(b.data(), v);
    uint64_t r{};

    EXPECT_EQ(varint::parse64(b.data(), b.data() + n, &r), b.data() + n) << std::hex << v;
    EXPECT_EQ(r, v);

    r = 0;
    EXPECT_EQ(varint::parse64(b.data(), b.data() + b.size(), &r), b.data() + n) << std::hex << v;
    EXPECT_EQ(r, v);

    for (ulint len = 0; len < n; ++len) {
      EXPECT_EQ(varint::parse64(b.data(), b.data() + len, &r), nullptr) << std::hex << v << " len " << len;
    }
  }
}

/** The id form of undo numbers and table ids: 0 in one byte,
other values below 2^32 as 0xFF, the varint high half, the varint low
half. Size and write agree. */
TEST(MachTest, VarintIdSize) {
  for (auto v : VAL64) {
    std::array<byte, 16> b{};
    const auto n = varint::encode_id(b.data(), v);

    EXPECT_EQ(n, varint::size_id(v)) << std::hex << v;

    if (v == 0) {
      EXPECT_EQ(n, 1UL);
      EXPECT_EQ(b[0], 0);
    } else {
      EXPECT_EQ(b[0], 0xFF) << std::hex << v;
    }
  }
}

/** Values below 2^32 read back. */
TEST(MachTest, VarintIdLow) {
  for (uint64_t v : {0ULL, 1ULL, 0x7FULL, 0x80ULL, 0x12345ULL, 0xFFFFFFFFULL}) {
    std::array<byte, 16> b{};

    (void)varint::encode_id(b.data(), v);
    EXPECT_EQ(varint::decode_id(b.data()), v) << std::hex << v;
  }
}

/* Regression: varint::decode_id() mis-reads every value >= 2^32. It
shifts the high half before taking its encoded size:
  high = uint64_t(varint::decode(b + 1)) << 32;
  size = 1 + varint::size(high);
varint::size(high) of a value >= 2^32 is 2 (the "extended" form
for values >= 0xFFFFFC00), not the size of the high half: the low half is
read from the wrong offset. The undo number and table id of an undo record
(trx0rec.cc, trx0undo_rec.cc) go through it; upstream
The old decoder took the size of the unshifted high half.
The debug build stops on ut_ad(val == 0xFE) in varint::decode(). */
TEST(MachTest, VarintIdHigh) {
  for (auto v : VAL64) {
    std::array<byte, 16> b{};

    (void)varint::encode_id(b.data(), v);
    EXPECT_EQ(varint::decode_id(b.data()), v) << std::hex << v;
  }
}

/** Little endian integers of 1 to 8 bytes. */
TEST(MachTest, LittleEndian) {
  std::array<byte, 8> b{};

  for (ulint n = 1; n <= 8; ++n) {
    const ulint v = n == 8 ? 0x8877665544332211UL : (0x8877665544332211UL & ((1UL << (8 * n)) - 1));

    mach_write_to_n_little_endian(b.data(), n, v);
    EXPECT_EQ(b[0], 0x11);
    EXPECT_EQ(mach_read_from_n_little_endian(b.data(), n), v) << n;
  }

  mach_write_to_2_little_endian(b.data(), 0xBEEF);
  EXPECT_EQ(b[0], 0xEF);
  EXPECT_EQ(b[1], 0xBE);
  EXPECT_EQ(mach_read_from_2_little_endian(b.data()), 0xBEEFUL);
}

/** mach_swap_byte_order reverses 1 to 8 bytes. */
TEST(MachTest, SwapByteOrder) {
  const std::array<byte, 8> src{1, 2, 3, 4, 5, 6, 7, 8};

  for (ulint len = 1; len <= 8; ++len) {
    std::array<byte, 8> dst{};

    mach_swap_byte_order(dst.data(), src.data(), len);

    for (ulint i = 0; i < len; ++i) {
      EXPECT_EQ(dst[i], src[len - 1 - i]) << len;
    }

    for (ulint i = len; i < 8; ++i) {
      EXPECT_EQ(dst[i], 0) << len;
    }
  }
}

/** Floating point values are stored little endian and read back bit exact. */
TEST(MachTest, FloatDouble) {
  std::array<byte, 8> b{};

  for (double d : {0.0, -0.0, 1.5, -3.25e300, std::numeric_limits<double>::denorm_min(), std::numeric_limits<double>::infinity()}) {
    mach_double_write(b.data(), d);
    EXPECT_EQ(std::memcmp(b.data(), &d, sizeof(d)), 0);
    EXPECT_EQ(std::bit_cast<uint64_t>(mach_double_read(b.data())), std::bit_cast<uint64_t>(d));
  }

  for (float f : {0.0f, -0.0f, 1.5f, -3.25e30f, std::numeric_limits<float>::max()}) {
    mach_float_write(b.data(), f);
    EXPECT_EQ(std::bit_cast<uint32_t>(mach_float_read(b.data())), std::bit_cast<uint32_t>(f));
  }

  const double nan = std::numeric_limits<double>::quiet_NaN();

  mach_double_write(b.data(), nan);
  EXPECT_TRUE(std::isnan(mach_double_read(b.data())));
}

/** The typed 32 and 64-bit readers: the signed ones flip the sign bit. */
TEST(MachTest, Int32Int64) {
  std::array<byte, 8> b{};

  for (int32_t v : {INT32_MIN, -1, 0, 1, INT32_MAX}) {
    mach_write_int32(b.data(), v);
    EXPECT_EQ(mach_read_int32(b.data()), v);
  }

  for (uint32_t v : {0U, 1U, 0x80000000U, 0xFFFFFFFFU}) {
    mach_write_uint32(b.data(), v);
    EXPECT_EQ(mach_read_uint32(b.data()), v);
  }

  mach_write_int32(b.data(), 0);
  EXPECT_EQ(b[0], 0x80);

  mach_write_uint32(b.data(), 1);
  EXPECT_EQ(b[3], 1);
  EXPECT_EQ(b[0], 0);

  for (int64_t v : {INT64_MIN, int64_t(-1), int64_t(0), INT64_MAX}) {
    mach_write_int64(b.data(), v);
    EXPECT_EQ(mach_read_int64(b.data()), v);
  }
}

#ifdef UNIV_DEBUG
/** The debug build rejects a value too wide for 1 or 2 bytes. */
TEST(MachDeathTest, TooWide) {
  std::array<byte, 4> b{};

  EXPECT_DEATH(mach_write_to_1(b.data(), 0x100), "");
  EXPECT_DEATH(mach_write_to_2(b.data(), 0x10000), "");
}
#endif /* UNIV_DEBUG */

}  // namespace

namespace {

/** Values of every size class of the varint encoding, for the benchmarks. */
std::vector<ulint> bench_values() {
  std::vector<ulint> v;
  std::mt19937_64 rng(1);

  for (int i = 0; i < 4096; ++i) {
    v.push_back(ENC32[rng() % std::size(ENC32)].m_val ^ (rng() & 0x3F));
  }

  return v;
}

/** Encode a 32-bit varint. */
TEST(MachPerf, WriteVarint) {
  const auto vals = bench_values();
  std::array<byte, 8> b{};
  ulint sum{};

  auto r = ut::bench("write_varint", 20000000, [&](uint64_t i) {
    sum += varint::encode(b.data(), vals[i & 4095]);
    ut::keep(b);
  });

  ut::keep(sum);
  EXPECT_LT(r.ns_per_op(), 200.0);
}

/** Decode a 32-bit varint. */
TEST(MachPerf, ReadVarint) {
  const auto vals = bench_values();
  std::vector<byte> buf(vals.size() * 5);
  std::vector<byte *> ptrs;
  byte *w = buf.data();

  for (auto v : vals) {
    ptrs.push_back(w);
    w += varint::encode(w, v);
  }

  ulint sum{};

  auto r = ut::bench("read_varint", 20000000, [&](uint64_t i) { sum += varint::decode(ptrs[i & 4095]); });

  ut::keep(sum);
  EXPECT_LT(r.ns_per_op(), 200.0);
}

/** The log parser's bounds checked reader over a stream of values. */
TEST(MachPerf, ParseVarint) {
  const auto vals = bench_values();
  std::vector<byte> buf(vals.size() * 5);
  byte *w = buf.data();

  for (auto v : vals) {
    w += varint::encode(w, v);
  }

  ulint sum{};

  auto r = ut::bench("parse_varint_4096", 5000, [&](uint64_t) {
    byte *p = buf.data();

    while (p < w) {
      sum += varint::parse(p, w);
    }
  });

  ut::keep(sum);
  /* Per stream of 4096 values. */
  EXPECT_LT(r.ns_per_op(), 4096 * 200.0);
}

/** Encode and parse a 64-bit varint. */
TEST(MachPerf, Varint64) {
  std::array<byte, 16> b{};
  uint64_t sum{};

  auto r = ut::bench("uint64_varint_round_trip", 10000000, [&](uint64_t i) {
    const auto n = varint::encode64(b.data(), VAL64[i % std::size(VAL64)]);
    uint64_t v;

    (void)varint::parse64(b.data(), b.data() + n, &v);
    sum += v;
  });

  ut::keep(sum);
  EXPECT_LT(r.ns_per_op(), 500.0);
}

/** Fixed width 4 and 8 byte big endian write and read. */
TEST(MachPerf, Fixed) {
  std::array<byte, 16> b{};
  uint64_t sum{};

  auto r = ut::bench("fixed_4_8_round_trip", 20000000, [&](uint64_t i) {
    mach_write_to_4(b.data(), uint32_t(i));
    mach_write_to_8(b.data() + 4, i);
    ut::keep(b);
    sum += mach_read_from_4(b.data()) + mach_read_from_8(b.data() + 4);
  });

  ut::keep(sum);
  EXPECT_LT(r.ns_per_op(), 200.0);
}

}  // namespace
