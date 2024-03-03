/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/mach0data.h
Utilities for converting data from the database file
to the machine format.

Created 11/28/1995 Heikki Tuuri
***********************************************************************/

#pragma once

#include "innodb0types.h"

#include "ut0byte.h"
#include "ut0mem.h"

/** The following function is used to store data in one byte
@param[in,out] b                Pointer where you want to write
@param[in] v                    integer to be stored, >= 0, < 256 */
inline void mach_write_to_1(byte *b, ulint v) {
  ut_ad((v | 0xFFUL) <= 0xFFUL);

  b[0] = byte(v);
}

/** The following function is used to fetch data from one byte.
@param[in] b                    Pointer from where you want to read
@return	ulint integer, >= 0, < 256 */
[[nodiscard]] inline ulint mach_read_from_1(const byte *b) {
  return ulint(b[0]);
}

/** The following function is used to store data in two consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in]                      Value to write to the pointer. */
inline void mach_write_to_2(byte *b, ulint v) {
  ut_ad((v | 0xFFFF) <= 0xFFFFUL);

  b[0] = byte(v >> 8);
  b[1] = byte(v);
}

/** The following function is used to fetch data from 2 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	ulint integer */
[[nodiscard]] inline ulint mach_read_from_2(const byte *b) {
  return (ulint(b[0]) << 8) + ulint(b[1]);
}

/** The following function is used to convert a 16-bit data item
to the canonical format, for fast bytewise equality test
against memory.
@param[in] v                    Integer in machine-dependent format.
@return	16-bit integer in canonical format */
[[nodiscard]] inline uint16_t mach_encode_2(ulint v) {
  uint16_t n;

  mach_write_to_2(reinterpret_cast<byte *>(&n), v);

  return n;
}

/** The following function is used to convert a 16-bit data item
from the canonical format, for fast bytewise equality test
against memory.
@param[in] v                    Integer in canonical format.
@return	integer in machine-dependent format */
[[nodiscard]] inline ulint mach_decode_2(uint16_t v) {
  return mach_read_from_2(reinterpret_cast<const byte *>(&v));
}

/** The following function is used to store data in 3 consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_to_3(byte *b, uint32_t v) {
  // FIXME:
  // ut_ad((v 0xFFFFFFUL) <= 0xFFFFFFUL);

  b[0] = byte(v >> 16);
  b[1] = byte(v >> 8);
  b[2] = byte(v);
}

/** The following function is used to fetch data from 3 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	ulint integer */
[[nodiscard]] inline ulint mach_read_from_3(const byte *b) {
  return (ulint(b[0]) << 16) + (ulint(b[1]) << 8) + ulint(b[2]);
}

/** The following function is used to store data in four consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in]                      Value to write to the pointer. */
inline void mach_write_to_4(byte *b, uint32_t v) {
  b[0] = byte(v >> 24);
  b[1] = byte(v >> 16);
  b[2] = byte(v >> 8);
  b[3] = byte(v);
}

/** The following function is used to fetch data from 4 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	ulint integer */
[[nodiscard]] inline ulint mach_read_from_4(const byte *b) {
  return (ulint(b[0]) << 24) + (ulint(b[1]) << 16) + (ulint(b[2]) << 8) + ulint(b[3]);
}

/** Writes a ulint in a compressed form where the first byte codes the
length of the stored ulint. We look at the most significant bits of
the byte. If the most significant bit is zero, it means 1-byte storage,
else if the 2nd bit is 0, it means 2-byte storage, else if 3rd is 0,
it means 3-byte storage, else if 4th is 0, it means 4-byte storage,
else the storage is 5-byte.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer.
@return	compressed size in bytes */
[[nodiscard]] inline ulint mach_write_compressed(byte *b, ulint v) {
  if (v < 0x80) {
    /* 0nnnnnnn (7 bits) */
    mach_write_to_1(b, v);
    return 1;
  } else if (v < 0x4000) {
    /* 10nnnnnn nnnnnnnn (14 bits) */
    mach_write_to_2(b, v | 0x8000);
    return 2;
  } else if (v < 0x200000) {
    /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
    mach_write_to_3(b, v | 0xC00000);
    return 3;
  } else if (v < 0x10000000) {
    /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
    mach_write_to_4(b, v | 0xE0000000);
    return 4;
  } else if (v >= 0xFFFFFC00) {
    /* 111110nn nnnnnnnn (10 bits) (extended) */
    mach_write_to_2(b, (v & 0x3FF) | 0xF800);
    return 2;
  } else if (v >= 0xFFFE0000) {
    /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
    mach_write_to_3(b, (v & 0x1FFFF) | 0xFC0000);
    return 3;
  } else if (v >= 0xFF000000) {
    /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
    mach_write_to_4(b, (v & 0xFFFFFF) | 0xFE000000);
    return 4;
  } else {
    /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
    mach_write_to_1(b, 0xF0);
    mach_write_to_4(b + 1, v);
    return 5;
  }
}

/** Returns the size of a ulint when written in the compressed form.
@param[in] v                    Integer (< 2^32) to be stored
@return	compressed size in bytes */
[[nodiscard]] inline ulint mach_get_compressed_size(ulint v) {
  if (v < 0x80) {
    /* 0nnnnnnn (7 bits) */
    return 1;
  } else if (v < 0x4000) {
    /* 10nnnnnn nnnnnnnn (14 bits) */
    return 2;
  } else if (v < 0x200000) {
    /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
    return 3;
  } else if (v < 0x10000000) {
    /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
    return 4;
  } else if (v >= 0xFFFFFC00) {
    /* 111110nn nnnnnnnn (10 bits) (extended) */
    return 2;
  } else if (v >= 0xFFFE0000) {
    /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
    return 3;
  } else if (v >= 0xFF000000) {
    /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
    return 4;
  } else {
    /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
    return 5;
  }
}

/** Reads a ulint in a compressed form.
@param[in] b                    Pointer from where you want to read
@return	read integer (< 2^32) */
[[nodiscard]] inline ulint mach_read_compressed(const byte *b) {
  auto val = mach_read_from_1(b);

  if (val < 0x80) {
    /* 0nnnnnnn (7 bits) */
  } else if (val < 0xC0) {
    /* 10nnnnnn nnnnnnnn (14 bits) */
    val = mach_read_from_2(b) & 0x3FFF;
    ut_ad(val > 0x7F);
  } else if (val < 0xE0) {
    /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
    val = mach_read_from_3(b) & 0x1FFFFF;
    ut_ad(val > 0x3FFF);
  } else if (val < 0xF0) {
    /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
    val = mach_read_from_4(b) & 0xFFFFFFF;
    ut_ad(val > 0x1FFFFF);
  } else if (val < 0xF8) {
    /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
    ut_ad(val == 0xF0);
    val = mach_read_from_4(b + 1);
    /* this can treat not-extended format also. */
    ut_ad(val > 0xFFFFFFF);
  } else if (val < 0xFC) {
    /* 111110nn nnnnnnnn (10 bits) (extended) */
    val = (mach_read_from_2(b) & 0x3FF) | 0xFFFFFC00;
  } else if (val < 0xFE) {
    /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
    val = (mach_read_from_3(b) & 0x1FFFF) | 0xFFFE0000;
    ut_ad(val < 0xFFFFFC00);
  } else {
    /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
    ut_ad(val == 0xFE);
    val = mach_read_from_3(b + 1) | 0xFF000000;
    ut_ad(val < 0xFFFE0000);
  }

  return val;
}

/** The following function is used to store data in 8 consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_to_8(byte *b, uint64_t v) {
  mach_write_to_4(b, ulint(v >> 32));
  mach_write_to_4(b + 4, ulint(v));
}

/** The following function is used to fetch data from 8 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	64 bit integer */
[[nodiscard]] inline uint64_t mach_read_from_8(const byte *b) {
  auto v = uint64_t(mach_read_from_4(b)) << 32;
  return v | mach_read_from_4(b + 4);
}

/** The following function is used to store data in 7 consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_to_7(byte *b, uint64_t v) {
  mach_write_to_3(b, v >> 32);
  mach_write_to_4(b + 3, ulint(v));
}

/** The following function is used to fetch data from 7 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	64 bit integer */
[[nodiscard]] inline uint64_t mach_read_from_7(const byte *b) {
  const auto high = uint64_t(mach_read_from_4(b)) << 32;

  return high | mach_read_from_4(b + 3);
}

/** The following function is used to store data in 6 consecutive
bytes. We store the most significant byte to the lowest address.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_to_6(byte *b, uint64_t v) {
  mach_write_to_2(b, ulint(v >> 32));
  mach_write_to_4(b + 2, ulint(v));
}

/** The following function is used to fetch data from 6 consecutive
bytes. The most significant byte is at the lowest address.
@param[in] b                    Pointer from where you want to read
@return	64 bit integer */
[[nodiscard]] inline uint64_t mach_read_from_6(const byte *b) {
  const auto high = uint64_t(mach_read_from_4(b)) << 32;

  return high | mach_read_from_4(b + 2);
}

/** Writes a  64 bit integer in a compressed form (5..9 bytes).
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer.
@return	size in bytes */
[[nodiscard]] inline ulint mach_uint64_write_compressed(byte *b, uint64_t v) {
  const auto size = mach_write_compressed(b, ulint(v >> 32));

  mach_write_to_4(b + size, ulint(v));

  return size + 4;
}

/** Returns the size of a uint64_t when written in the compressed form.
@param[in]                      Value to write to the pointer.
@return	compressed size in bytes */
[[nodiscard]] inline ulint mach_uint64_get_compressed_size(uint64_t v) {
  return 4 + mach_get_compressed_size(ulint(v >> 32));
}

/** Read a 32-bit integer in a compressed form.
@param[in,out]  b       pointer to memory where to read;
advanced by the number of bytes consumed
@return unsigned value */
inline uint32_t mach_uint32_read_compressed(const byte *&b) {
  ulint val = mach_read_from_1(b);

  if (val < 0x80) {
    /* 0nnnnnnn (7 bits) */
    ++b;
  } else if (val < 0xC0) {
    /* 10nnnnnn nnnnnnnn (14 bits) */
    val = mach_read_from_2(b) & 0x3FFF;
    ut_ad(val > 0x7F);
    b += 2;
  } else if (val < 0xE0) {
    /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
    val = mach_read_from_3(b) & 0x1FFFFF;
    ut_ad(val > 0x3FFF);
    b += 3;
  } else if (val < 0xF0) {
    /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
    val = mach_read_from_4(b) & 0xFFFFFFF;
    ut_ad(val > 0x1FFFFF);
    b += 4;
  } else if (val < 0xF8) {
    /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
    ut_ad(val == 0xF0);
    val = mach_read_from_4(b + 1);
    /* this can treat not-extended format also. */
    ut_ad(val > 0xFFFFFFF);
    b += 5;
  } else if (val < 0xFC) {
    /* 111110nn nnnnnnnn (10 bits) (extended) */
    val = (mach_read_from_2(b) & 0x3FF) | 0xFFFFFC00;
    b += 2;
  } else if (val < 0xFE) {
    /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
    val = (mach_read_from_3(b) & 0x1FFFF) | 0xFFFE0000;
    ut_ad(val < 0xFFFFFC00);
    b += 3;
  } else {
    /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
    ut_ad(val == 0xFE);
    val = mach_read_from_3(b + 1) | 0xFF000000;
    ut_ad(val < 0xFFFE0000);
    b += 4;
  }

  return static_cast<uint32_t>(val);
}

/** Reads a 64 bit integer in a compressed form.
@param[in] b                    Pointer from where you want to read
@return	read a 64 bit integer*/
[[nodiscard]] inline uint64_t mach_uint64_read_compressed(const byte *b) {
  const auto high = uint64_t(mach_uint32_read_compressed(b)) << 32;
  const auto low = mach_read_from_4(b);

  return high | low;
}

/** Writes a 64 bit integer in a compressed form (1..11 bytes).
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer.
@return	size in bytes */
[[nodiscard]] inline ulint mach_uint64_write_much_compressed(byte *b, uint64_t v) {
  if (v == 0) {
    return mach_write_compressed(b, v);
  } else {
    *b = byte(0xFF);

    auto size = 1 + mach_write_compressed(b + 1, ulint(v) >> 32);

    size += mach_write_compressed(b + size, ulint(v & 0xffffffff));

    return size;
  }
}

/** Returns the size of a uint64_t when written in the compressed form.
@param[in] v                    Value to check.
@return	compressed size in bytes */
[[nodiscard]] inline ulint mach_uint64_get_much_compressed_size(uint64_t v) {
  if (v == 0) {
    return mach_get_compressed_size(ulint(0));
  } else {
    return 1 + mach_get_compressed_size(ulint(v >> 32)) + mach_get_compressed_size(ulint(v & 0xffffffff));
  }
}

/** Reads a 64 bit ingeger in a compressed form.
@param[in] b                    Pointer from where you want to read
@return	read 64 bit integer */
[[nodiscard]] inline uint64_t mach_uint64_read_much_compressed(const byte *b) {
  ulint size{};
  uint64_t high{};

  if (*b == byte(0xFF)) {
    high = uint64_t(mach_read_compressed(b + 1)) << 32;
    size = 1 + mach_get_compressed_size(high);
  }

  return high | mach_read_compressed(b + size);
}

/** Reads a double. It is stored in a little-endian format.
@param[in] b                    Pointer from where you want to read
@return	double read */
[[nodiscard]] inline double mach_double_read(const byte *b) {
  double d;
  auto ptr = reinterpret_cast<byte *>(&d);

  for (ulint i = 0; i < sizeof(d); i++) {
#ifdef WORDS_BIGENDIAN
    ptr[sizeof(double) - i - 1] = b[i];
#else /* WORDS_BIGENDIAN */
    ptr[i] = b[i];
#endif /* WORDS_BIGENDIAN */
  }

  return d;
}

/** Writes a pointer to a double. It is stored in a little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in] ptr                  Pointer to value to write. */
inline void mach_double_ptr_write(byte *b, const byte *ptr) {
  for (ulint i = 0; i < sizeof(double); i++) {
#ifdef WORDS_BIGENDIAN
    b[i] = ptr[sizeof(double) - i - 1];
#else /* WORDS_BIGENDIAN */
    b[i] = ptr[i];
#endif /* WORDS_BIGENDIAN */
  }
}

/** Writes a double. It is stored in a little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_double_write(byte *b, double v) {
  mach_double_ptr_write(b, reinterpret_cast<const byte *>(&v));
}

/** Reads a float. It is stored in a little-endian format.
@param[in] b                    Pointer from where you want to read
@return	float read */
[[nodiscard]] inline float mach_float_read(const byte *b) {
  float f;
  auto ptr = reinterpret_cast<byte *>(&f);

  for (ulint i = 0; i < sizeof(f); i++) {
#ifdef WORDS_BIGENDIAN
    ptr[sizeof(float) - i - 1] = b[i];
#else /* WORDS_BIGENDIAN */
    ptr[i] = b[i];
#endif /* WORDS_BIGENDIAN */
  }

  return f;
}

/** Writes a pointer to float. It is stored in a little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in] ptr                  Pointer to value to write. */
inline void mach_float_ptr_write(byte *b, const byte *ptr) {
  for (ulint i = 0; i < sizeof(float); i++) {
#ifdef WORDS_BIGENDIAN
    b[i] = ptr[sizeof(float) - i - 1];
#else /* WORDS_BIGENDIAN */
    b[i] = ptr[i];
#endif /* WORDS_BIGENDIAN */
  }
}

/** Writes a float. It is stored in a little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_float_write(byte *b, float v) {
  mach_float_ptr_write(b, reinterpret_cast<const byte *>(&v));
}

/** Reads a ulint stored in the little-endian format.
@param[in] b                    Pointer from where you want to read
@param[in] n                    Number of bytes to read.
@return	unsigned long int */
[[nodiscard]] inline ulint mach_read_from_n_little_endian(const byte *buf, ulint n) {
  ut_ad(n > 0);
  ut_ad(n <= sizeof(ulint));

  ulint v{};
  auto ptr = buf + n;

  for (;;) {
    ptr--;

    v = v << 8;

    v += ulint(*ptr);

    if (ptr == buf) {
      break;
    }
  }

  return v;
}

/** Writes a ulint in the little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in] dest_size            Number of bytes to write.
@param[in] n                    Value to write to the pointer. */
inline void mach_write_to_n_little_endian(byte *b, ulint dest_size, ulint n) {
  ut_ad(dest_size > 0);
  ut_ad(dest_size <= sizeof(ulint));

  auto end = b + dest_size;

  for (;;) {
    *b= byte(n & 0xFF);

    n = n >> 8;

    ++b;

    if (b == end) {
      break;
    }
  }

  ut_ad(n == 0);
}

/** Reads a ulint stored in the little-endian format.
@param[in] b                    Pointer from where you want to read
@return	a ulint */
[[nodiscard]] inline ulint mach_read_from_2_little_endian(const byte *b) {
  return ulint(*b) + (ulint(*(b + 1))) * 256;
}

/** Writes a ulint in the little-endian format.
@param[in,out] b                Pointer where you want to write.
@param[in]                      Value to write to the pointer. */
inline void mach_write_to_2_little_endian(byte *b, ulint n)
{
  ut_ad(n < 256 * 256);

  *b = byte(n & 0xFFUL);
  n = n >> 8;

  ++b;

  *b= byte(n & 0xFFUL);
}

/** Swap byte ordering.
@param[out] dest               where to write
@param[in] src                 where to read from
@param[in] len                 length of src */
inline void mach_swap_byte_order(void *dest, const byte *from, ulint len) {
  auto d = reinterpret_cast<byte *>(dest);

  ut_ad(len > 0);
  ut_ad(len <= 8);

  d += len;

  switch (len & 0x7) {
  case 0:
    *--d = *from++;
  case 7:
    *--d = *from++;
  case 6:
    *--d = *from++;
  case 5:
    *--d = *from++;
  case 4:
    *--d = *from++;
  case 3:
    *--d = *from++;
  case 2:
    *--d = *from++;
  case 1:
    *--d = *from;
  }
}

/** Convert integral type from storage byte order (big-endian) to
host byte order.
@param[in,out] dst              Pointer where you want to write.
@param[in,out] src              Pointer where to yout to read from.
@param[in] len                  Length of the source pointer (b)
@param[in] usign                Signed or unsigned flag.
@return	value in host byte order */
inline void mach_read_int_type(void *dst, const byte *src, ulint len, bool usign) {
#ifdef WORDS_BIGENDIAN

  memcpy(dst, src, len);

#else /* WORDS_BIGENDIAN */

  mach_swap_byte_order(dst, src, len);

  if (usign) {

    *(reinterpret_cast<byte *>(dst) + len - 1) ^= 0x80;
  }
#endif /* WORDS_BIGENDIAN */
}

/** Convert integral type from host byte order (big-endian) storage
byte order.
@param[in,out] dst              Pointer where you want to write.
@param[in,out] src              Pointer where to yout to read from.
@param[in] len                  Length of the source pointer (b)
@param[in] usign                Signed or unsigned flag. */
inline void mach_write_int_type(byte *dest, const byte *src, ulint len, bool usign) {
#ifdef WORDS_BIGENDIAN

  memcpy(dest, src, len);

#else /* WORDS_BIGENDIAN */

  mach_swap_byte_order(dest, src, len);

  if (usign) {

    *dest ^= 0x80;
  }
#endif /* WORDS_BIGENDIAN */
}

/** Convert a 64 bit big endian unsigned integral type to the host
byte order.
@param[in] b                    Pointer from where you want to read
@return	value in host byte order */
[[nodiscard]] inline uint64_t mach_read_uint64(const byte *b) {
  uint64_t v;

  mach_read_int_type(&v, b, sizeof(v), true);

  return v;
}

/** Convert a 64 bit big endian signed integral type to the host
byte order.
@param[in] b                    Pointer from where you want to read
@return	value in host byte order */
[[nodiscard]] inline int64_t mach_read_int64(const byte *b) {
  uint64_t v;

  mach_read_int_type(&v, b, sizeof(v), false);

  return v;
}

/** Convert a 32 bit big endian unsigned integral type to the host
byte order.
@param[in] b                    Pointer from where you want to read
@return	value in host byte order */
[[nodiscard]] inline uint32_t mach_read_uint32(const byte *b) {
  uint32_t v;

  mach_read_int_type(&v, b, sizeof(v), true);

  return v;
}

/** Convert a 32 bit big endian signed integral type to the host
byte order.
@param[in] b                    Pointer from where you want to read
@return	value in host byte order */
[[nodiscard]] inline int32_t mach_read_int32(const byte *b) {
  int32_t v;

  mach_read_int_type(&v, b, sizeof(v), false);

  return v;
}

/** Convert a 64 bit unsigned integral type to big endian from host
byte order. 
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_uint64(byte *b, uint64_t v) {
  mach_write_int_type(b, reinterpret_cast<const byte *>(&v), sizeof(v), true);
}

/** Convert a 64 bit signed integral type to big endian from host
byte order.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_int64(byte *b, int64_t v) {
  mach_write_int_type(b, reinterpret_cast<const byte *>(&v), sizeof(v), false);
}

/** Convert a 32 bit unsigned integral type to big endian from host
byte order.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_uint32(byte *b, uint32_t v) {
  mach_write_int_type(b, reinterpret_cast<const byte *>(&v), sizeof(v), true);
}

/** Convert a 32 bit signed integral type to big endian from host
byte order.
@param[in,out] b                Pointer where you want to write.
@param[in] v                    Value to write to the pointer. */
inline void mach_write_int32(byte *b, int32_t v) {
  mach_write_int_type(b, reinterpret_cast<const byte *>(&v), sizeof(v), false);
}

/** Reads a ulint in a compressed form if the log record fully contains it.
@param[in] ptr                  Pointer from where you want to read
@param[in] end_ptr              Pointer to end of the buffer
@return	pointer to end of the stored field, NULL if not complete */
[[nodiscard]] byte *mach_parse_compressed(byte *ptr, byte *end_ptr, ulint *v);

/** Reads a 64 bit integer in a compressed form if the log record fully contains it.
@param[in] ptr                  Pointer from where you want to read
@param[in] end_ptr              Pointer to end of the buffer
@param[out] v                   Where to write value read
@return	pointer to end of the stored field, NULL if not complete */
[[nodiscard]] byte *mach_uint64_parse_compressed(byte *ptr, byte *end_ptr, uint64_t *v);

