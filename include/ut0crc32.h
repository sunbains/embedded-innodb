/*****************************************************************************

Copyright (c) 2009, 2010 Facebook, Inc.
Copyright (c) 2011, 2021, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.

This program is also distributed with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have included with MySQL.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License, version 2.0, for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#pragma once

#include <nmmintrin.h>
#include <wmmintrin.h>
#include <stdint.h>

#include "innodb0types.h"

#include <functional>

namespace crc32 {

/**
 * Note: This is copied from the original source code of MySQL 8.x.
 * The original source code is located at:
 * 
 *  mysql-server:storage/innobase/include/ut0crc32.h
 * 
 * in case we want to copy code for tthe other platforms later.
 * 
 * CRC32_x86_64_DEFAULT
 *   An environment which seems to be like gcc or clang, and thus we can use
 *   inline assembly to get `cpuid`.
 *   Also, we can/have to use __attribute__(target(...)) on functions which
 *   use intrinsics, and may need to use __attribute__(flatten) at top level
 *   to ensure that the run-time selection of target-specific variant of the
 *   function happens just once at the top, not in every leaf, which would
 *   break inlining and optimizations.
 */
constexpr ulint NO_CHECKSUM = 0x1EDC6F41;

/** The CRC-32C polynomial without the implicit highest 1 at x^32 */
constexpr uint32_t CRC32C_POLYNOMIAL = 0x1EDC6F41;

using Checksum = std::function<uint32_t(const byte*, size_t)>;

extern Checksum checksum;

/** Executes cpuid assembly instruction and returns the ecx register's value.
 * 
 * @return ecx value produced by cpuid
 */
static inline uint32_t get_cpuid_ecx() noexcept {
  uint32_t features_ecx;
  asm("cpuid" : "=c"(features_ecx) : "a"(1) : "ebx", "edx");
  return features_ecx;
}

/** @return true if we can use hardware CRC32. */
inline bool can_use_crc32() noexcept {
  return get_cpuid_ecx() & (1U << 20);
}

/** Checks if hardware accelerated polynomial multiplication instructions are
* available to this process right now. */
inline bool can_use_poly_mul() noexcept {
  return get_cpuid_ecx() & (1U << 1);
}

/** A helper template to statically unroll a loop with a fixed number of
 * iterations, where the iteration number itself is constexpr. So, instead of:
 *
 *   Something::template run<0>(a,b);
 *   Something::template run<1>(a,b);
 *   Something::template run<2>(a,b);
 *
 * you can write:
 *
 *    Loop<3>::template run<Something>(a, b);
 */
template <size_t iterations>
struct Loop {
  template <typename Step_executor, typename... Args>
  static void run(Args &&... args) noexcept {
    Loop<iterations - 1>::template run<Step_executor, Args...>(
        std::forward<Args>(args)...);
    Step_executor::template run<iterations - 1>(std::forward<Args>(args)...);
  }
};

template <>
struct Loop<0> {
  template <typename Step_executor, typename... Args>
  static void run(Args &&... args) noexcept {}
};

/** Computes x^(len*8) modulo CRC32-C polynomial, which is useful, when you need
 * to conceptually append len bytes of zeros to already computed hash.
 *
 * @param[in]  len               The value of len in the x^(len*8) mod CRC32-C
 *
 * @return the rest of x^(len*8) mod CRC32-C, with the most significant coefficient
 *  of the resulting rest - the one which is for x^31 - stored as the most
 *  significant bit of the uint32_t (the one at 1U<<31)
 */
inline constexpr uint32_t compute_x_to_8len(size_t len) noexcept {
  /* The x^(len*8) mod CRC32 polynomial depends on len only. */
  uint32_t x_to_8len = 1;

  /* Push len bytes worth of zeros. */
  for (size_t i = 0; i < len * 8; ++i) {
    const bool will_wrap = x_to_8len >> 31 & 1;

    x_to_8len <<= 1;

    if (will_wrap) {
      x_to_8len ^= CRC32C_POLYNOMIAL;
    }
  }

  return x_to_8len;
}

/**
 * Produces a 64-bit result by moving i-th bit of 32-bit input to the
 * 32-i-th position (zeroing the other bits). Please note that in particular this
 * moves 0-th bit to 32-nd, and 31-st bit to 1-st, so the range in which data
 * resides is not only mirrored, but also shifted one bit. Such operation is useful
 * for implementing polynomial multiplication when one of the operands is given in
 * reverse and we need the result reversed, too (as is the case in CRC32-C):
 *   rev(w * v) =   rev(w)*flip_at_32(v)
 * proof:
 *   rev(w * v)[i] = (w * v)[63-i] = sum(0<=j<=31){w[j]*v[63-i-j]} =
 *   sum(0<=j<=31){rev(w)[31-j]*v[63-i-j]} =
 *   sum(0<=j<=31){rev(w)[31-j]*flip_at_32(v)[32-63+i+j]} =
 *   sum(0<=j<=31){rev(w)[31-j]*flip_at_32(v)[i-(j-31)]} =
 *   sum(0<=j<=31){rev(w)[j]*flip_at_32(v)[i-j]} =
 *   rev(w)*flip_at_32(v)[i]
 * So, for example, if crc32=rev(w) is the variable storing the CRC32-C hash of a
 * buffer, and you want to conceptually append len bytes of zeros to it, then you
 * can precompute v = compute_x_to_8len(len), and are interested in rev(w*v), which
 * you can achieve by crc32 * flip_at_32(compute_x_to_8len(len)).
 * @param[in]  w   The input 32-bit polynomial
 * @return The polynomial flipped and shifted, so that i-th bit becomes 32-i-th.
 */
inline constexpr uint64_t flip_at_32(uint32_t w) noexcept {
  uint64_t f{0};

  for (int i = 0; i < 32; ++i) {
    if (((w >> i) & 1)) {
      f ^= uint64_t{1} << (32 - i);
    }
  }
  return f;
}

/**
 * The collection of functions implementing hardware accelerated updating of
 * CRC32-C hash by processing a few (1,2,4 or 8) bytes of input. They are grouped
 * together in a type, so it's easier to swap their implementation by providing
 * algo_to_use template argument to higher level functions.
 */
struct crc32_impl {
  static inline uint32_t update(uint32_t crc, unsigned char data) noexcept;
  static inline uint32_t update(uint32_t crc, uint16_t data) noexcept;
  static inline uint32_t update(uint32_t crc, uint32_t data) noexcept;
  static inline uint64_t update(uint64_t crc, uint64_t data) noexcept;
};

__attribute__((target("sse4.2")))
inline uint32_t crc32_impl::update(uint32_t crc, unsigned char data) noexcept {
  return _mm_crc32_u8(crc, data);
}

__attribute__((target("sse4.2")))
inline uint32_t crc32_impl::update(uint32_t crc, uint16_t data) noexcept {
  return _mm_crc32_u16(crc, data);
}

__attribute__((target("sse4.2")))
inline uint32_t crc32_impl::update(uint32_t crc, uint32_t data) noexcept {
  return _mm_crc32_u32(crc, data);
}

__attribute__((target("sse4.2")))
inline uint64_t crc32_impl::update(uint64_t crc, uint64_t data) noexcept {
  return _mm_crc32_u64(crc, data);
}

/**
 * Implementation of polynomial_mul_rev<w>(rev_u) function which uses hardware
 * accelerated polynomial multiplication to compute rev(w*u), where rev_u=rev(u).
 * This is accomplished by using rev_u * flip_at_32(w).
 * 
 * @see flip_at_32 for an explanation why it works and why this is useful.
 */
struct use_pclmul : crc32_impl {
  template <uint32_t w>
  inline static uint64_t polynomial_mul_rev(uint32_t rev_u) noexcept;
};

template <uint32_t w>
__attribute__((target("sse4.2,pclmul")))
uint64_t use_pclmul::polynomial_mul_rev(uint32_t rev_u) noexcept {
  constexpr uint64_t flipped_w = flip_at_32(w);
  return _mm_cvtsi128_si64(_mm_clmulepi64_si128(
      _mm_set_epi64x(0, rev_u), _mm_set_epi64x(0, flipped_w), 0x00));
}

/**
 * Implementation of polynomial_mul_rev<w>(rev_u) function which uses a simple
 * loop over i: if(w>>i&1)result^=rev_u<<(32-i), which is equivalent to
 * w * flip_at_32(rev_u), which in turn is equivalent to rev(rev(w) * rev_u),
 * 
 * @see flip_at_32 for explanation why this holds,
 * 
 * @see use_pclmul for explanation of what polynomial_mul_rev is computing.
 * 
 * This implementation is to be used when hardware accelerated polynomial
 * multiplication is not available. It tries to unroll the simple loop, so just
 * the few xors and shifts for non-zero bits of w are emitted.
 */
struct use_unrolled_loop_poly_mul : crc32_impl {
  template <uint32_t x_to_len_8>
  struct Polynomial_mul_rev_step_executor {
    template <size_t i>

    static void run(uint64_t &acc, const uint32_t hash_1) noexcept {
      if (x_to_len_8 >> ((uint32_t)i) & 1) {
        acc ^= uint64_t{hash_1} << (32 - i);
      }
    }
  };

  template <uint32_t w>
  inline static uint64_t polynomial_mul_rev(uint32_t rev_u) noexcept {
    uint64_t rev_w_times_u{0};
    Loop<32>::run<Polynomial_mul_rev_step_executor<w>>(rev_w_times_u, rev_u);
    return rev_w_times_u;
  }
};

/**
 * Rolls the crc forward by len bytes, that is updates it as if 8*len zero bits
 * were processed.
 * 
 * @param[in] crc               Initial value of the hash
 * 
 * @return Updated value of the hash: rev(rev(crc)*(x^{8*len} mod CRC32-C)  )
 */
template <size_t len, typename algo_to_use>
inline static uint64_t roll(uint32_t crc) {
  return algo_to_use::template polynomial_mul_rev<compute_x_to_8len(len)>(crc);
}

/**
 * Takes a 64-bit reversed representation of a polynomial, and computes the
 * 32-bit reveresed representation of it modulo CRC32-C.
 * 
 * @param[in] big               The 64-bit representation of polynomial w,
 *                              with the most significant coefficient (the one
 *                              for x^63) stored at least significant bit
 *                              (the one at 1<<0).
 *  @return The 32-bit representation of w mod CRC-32, in which the most significant
 *    coefficient (the one for x^31) stored at least significant bit
 *    (the one at 1<<0).
 */
template <typename algo_to_use>
static inline uint32_t fold_64_to_32(uint64_t big) {
  /* CRC is stored in bit-reversed format, so "significant part of uint64_t" is
  actually the least significant part of the polynomial, and the "insignificant
  part of uint64_t" are the coefficients of highest degrees for which we need to
  compute the rest mod crc32-c polynomial. */

  return algo_to_use::update((uint32_t)big, uint32_t{0}) ^ (big >> 32);
}

/**
 * The body of unrolled loop used to process slices in parallel, which in i-th
 * iteration processes 8 bytes from the i-th slice of data, where each slice has
 * slice_len bytes.
 */
template <typename algo_to_use, size_t slice_len>
struct Update_step_executor {
  template <size_t i>
  static void run(uint64_t *crc, const uint64_t *data64) noexcept {
    crc[i] = algo_to_use::update(crc[i], *(data64 + i * (slice_len / 8)));
  }
};

/**
 * The body of unrolled loop used to combine partial results from each slice
 * into the final hash of whole chunk, which in i-th iteration takes the crc of
 * i-th slice and "rolls it forward" by virtually processing as many zeros as there
 * are from the end of the i-th slice to the end of the chunk.
 */
template <typename algo_to_use, size_t slice_len, size_t slices_count>
struct Combination_step_executor {
  template <size_t i>
  static void run(uint64_t &combined, const uint64_t *crc) noexcept {
    combined ^= roll<slice_len *(slices_count - 1 - i), algo_to_use>(crc[i]);
  }
};

/**
 * Updates the crc checksum by processing slices_count*slice_len bytes of data.
* The chunk is processed as slice_count independent slices of length slice_len,
* and the results are combined together at the end to compute correct result.
* 
* @param[in] crc0               Initial value of the hash
* @param[in] data               Data over which to calculate CRC32-C
* 
* @return The value of _crc updated by processing the range
*   data[0]...data[slices_count*slice_len-1]. */
template <size_t slice_len, size_t slices_count, typename algo_to_use>
static inline uint32_t consume_chunk(uint32_t crc0, const unsigned char *data) noexcept {
  static_assert(slices_count > 0, "there must be at least one slice");

  const uint64_t *data64 = (const uint64_t *)data;

  /* crc[i] is the hash for i-th slice, data[i*slice_len...(i+1)*slice_len)
   * where the initial value for each crc[i] is zero, except crc[0] for which we
   * use the initial value crc0 passed in by the caller. */
  uint64_t crc[slices_count]{crc0};

  /*
   * Each iteration of the for() loop will eat 8 bytes (single uint64_t) from
   * each slice.
   */
  static_assert(
      slice_len % sizeof(uint64_t) == 0,
      "we must be able to process a slice efficiently using 8-byte updates");

  constexpr auto iters = slice_len / sizeof(uint64_t);

  for (size_t i = 0; i < iters; ++i) {
    Loop<slices_count>::template run<
        Update_step_executor<algo_to_use, slice_len>>(crc, data64);
    ++data64;
  }

  /* m_combined_crc = sum crc[i]*x^{slices_count-i-1} mod CRC32-C */
  uint64_t combined_crc{0};

  /* This ugly if is here to ensure that fold_64_to_32 is called just once, as
  opposed to being done as part of combining each individual slice, which would
  also be correct, but would mean that CPU can't process roll()s in parallel.
  That is:

    combined ^= roll<"n-1 slices">(crc[0])
    combined ^= roll<"n-2 slices">(crc[1])
    ...
    combined ^= roll<1>(crc[n-2])
    return fold_64_to_32(combined) ^ crc[n-1]

  is faster than:

    crc[1] ^= fold_64_to_32(roll<"n-1 slices">(crc[0]))
    crc[2] ^= fold_64_to_32(roll<"n-1 slices">(crc[1]))
    ...
    crc[n-1] ^= fold_64_to_32(roll<1>(crc[n-2]))

    return crc[n-1]

  as the inputs to roll()s are independent, but now fold_64_to_32 is only needed
  conditionally, when slices_count > 1. */
  if (1 < slices_count) {

    Loop<slices_count - 1>::template run<
        Combination_step_executor<algo_to_use, slice_len, slices_count>>(
        combined_crc, crc);

    combined_crc = fold_64_to_32<algo_to_use>(combined_crc);
  }

  return combined_crc ^ crc[slices_count - 1];
}

/**
 * Updates the crc checksum by processing at most len bytes of data.
 * The data is consumed in chunks of size slice_len*slices_count, and stops when
 * no more full chunks can be fit into len bytes.
 * Each chunk is processed as slice_count independent slices of length slice_len,
 * and the results are combined together at the end to compute correct result.
 * 
 * @param[in,out] crc           Initial value of the hash. Updated by this function by
 *                              processing data[0]...data[B*(slice_len * slices_count)],
 *                              where B = floor(len / (slice_len * slices_count)).
 * 
 * @param[in,out] data          Data over which to calculate CRC32-C. Advanced by this
 *                              function to point to unprocessed part of the buffer.
 * 
 * @param[in,out] len           Data length to be processed. Updated by this function
 *                              to be len % (slice_len * slices_count).
 */
template <size_t slice_len, size_t slices_count, typename algo_to_use>
static inline void consume_chunks(uint32_t &crc, const byte *&data, size_t &len) noexcept {
  while (len >= slice_len * slices_count) {
    crc = consume_chunk<slice_len, slices_count, algo_to_use>(crc, data);
    len -= slice_len * slices_count;
    data += slice_len * slices_count;
  }
}

/**
 * Updates the crc checksum by processing Chunk (1,2 or 4 bytes) of data,
 * but only when the len of the data provided, when decomposed into powers of two,
 * has a Chunk of this length. This is used to process the prefix of the buffer to
 * get to the position which is aligned mod 8, and to process the remaining suffix
 * which starts at position aligned  mod 8, but has less than 8 bytes.
 * 
 * @param[in,out] crc           Initial value of the hash. Updated by this function by
 *                              processing Chunk pointed by data.
 * 
 * @param[in,out] data          Data over which to calculate CRC32-C. Advanced by this
 *                              function to point to unprocessed part of the buffer.
 * 
 * @param[in,out] len           Data length, allowed to be processed.
 */
template <typename Chunk, typename algo_to_use>
static inline void consume_pow2(uint32_t &crc, const byte *&data, size_t len) noexcept {
  if (len & sizeof(Chunk)) {
    crc = algo_to_use::update(crc, *(Chunk *)data);
    data += sizeof(Chunk);
  }
}

/**
 * The hardware accelerated implementation of CRC32-C exploiting within-core
 * parallelism on reordering processors, by consuming the data in large chunks
 * split into 3 independent slices each. It's optimized for handling buffers of
 * length typical for 16kb pages and redo log blocks, but it works correctly for
 * any len and alignment.
 * 
 * @param[in] crc               Initial value of the hash (0 for first block, or the
 *                              result of CRC32-C for the data processed so far)
 * 
 * @param[in] data              Data over which to calculate CRC32-C
 * 
 * @param[in] len               Data length
 * 
 * @return CRC-32C (polynomial 0x11EDC6F41)
 */
template <typename algo_to_use>
static inline uint32_t calculate_with(uint32_t crc, const byte *data, size_t len) noexcept {
  crc = ~crc;

  /* For performance, the main loop will operate on uint64_t[].
  On some  platforms unaligned reads are not allowed, on others they are
  slower, so we start by consuming the unaligned prefix of the data. */
  const size_t prefix_len = (8 - reinterpret_cast<uintptr_t>(data)) & 7;

  /* data address may be unaligned, so we read just one byte if it's odd */
  consume_pow2<byte, algo_to_use>(crc, data, prefix_len);

  /* data address is now aligned mod 2, but may be unaligned mod 4*/
  consume_pow2<uint16_t, algo_to_use>(crc, data, prefix_len);

  /* data address is now aligned mod 4, but may be unaligned mod 8 */
  consume_pow2<uint32_t, algo_to_use>(crc, data, prefix_len);

  /* data is now aligned mod 8 */
  len -= prefix_len;

  /* The suffix_len will be needed later, but we can compute it here already,
  as len will only be modified by subtracting multiples of 8, and hopefully,
  learning suffix_len sooner will make it easier for branch predictor later.*/
  const size_t suffix_len = len & 7;

  /* A typical page is 16kb, but the part for which we compute crc32 is a bit
  shorter, thus 5440*3 is the largest multiple of 8*3 that fits. For pages
  larger than 16kb, there's not much gain from handling them specially. */
  consume_chunks<5440, 3, algo_to_use>(crc, data, len);

  /* A typical redo log block is 0.5kb, but 168*3 is the largest multiple of
  8*3 which fits in the part for which we compute crc32. */
  consume_chunks<168, 3, algo_to_use>(crc, data, len);

  /* In general there can be some left-over (smaller than 168*3) which we
  consume 8*1 bytes at a time. */

  consume_chunks<8, 1, algo_to_use>(crc, data, len);

  /* Finally, there might be unprocessed suffix_len < 8, which we deal with
  minimal number of computations caring about proper alignment. */
  consume_pow2<uint32_t, algo_to_use>(crc, data, suffix_len);
  consume_pow2<uint16_t, algo_to_use>(crc, data, suffix_len);
  consume_pow2<byte, algo_to_use>(crc, data, suffix_len);

  return ~crc;
}

/** The specialization of calculate<> template for use_pclmul and 0 as initial
 * value of the hash. Used on platforms which support hardware accelerated
 * Polynomial multiplication.
 *
 * It's non-static so it can be unit-tested.
 *
 * @param[in] data              Data over which to calculate CRC32-C
 * @param[in] lend              Data length
 * 
 * @return CRC-32C (polynomial 0x11EDC6F41)
 */
__attribute__((target("sse4.2,pclmul"), flatten))
static inline uint32_t pclmul(const byte *data, size_t len) noexcept {
  return calculate_with<use_pclmul>(0, data, len);
}

/** The specialization of crc32<> template for use_unrolled_loop_poly_mul and 0
 * as initial value of the hash. Used on platforms which do not support hardware
 * accelerated polynomial multiplication.
 * 
 * It's non-static so it can be unit-tested.
 * 
 * @param[in] data                 Data over which to calculate CRC32-C
 * @param[in] len                  Data length
 * 
 * @return CRC-32C (polynomial 0x11EDC6F41)
 */
__attribute__((target("sse4.2"), flatten))
static inline uint32_t unrolled_loop_poly_mul(const byte *data, size_t len) noexcept {
  return calculate_with<use_unrolled_loop_poly_mul>(0, data, len);
}

static inline Checksum init() noexcept { // Provide complete type for Checksum
  const auto cpu_enabled = can_use_crc32();
  const auto mul_cpu_enabled = can_use_poly_mul();

  if (cpu_enabled) {
    if (mul_cpu_enabled) {
      return pclmul;
    } else {
      return unrolled_loop_poly_mul;
    }
  } else {
    return [] (const byte *, size_t ) {
      return NO_CHECKSUM;
    };
  }
}

} // crc32
