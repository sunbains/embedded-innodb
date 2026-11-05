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

/** @file unit-tests/test_ut.cc
The utilities: CRC-32C and the page checksum, folds and random numbers,
ut_new()/ut_delete(), the string, time and bit helpers, the intrusive
two-way list (ut0lst.h), the Unicode stubs and the logger. */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "fil0types.h"
#include "ut0byte.h"
#include "ut0crc32.h"
#include "ut0logger.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "ut0rnd.h"
#include "ut0ucode.h"
#include "ut0ut.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <fstream>
#include <mutex>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

/** Defined in ut/ut0mem.cc but declared in no header. */
ulint ut_strlcpy_rev(char *dst, const char *src, ulint size);

namespace {

namespace fs = std::filesystem;

/* ---------------------------------------------------------------- CRC-32C */

/** A CRC-32C implementation under test. */
struct Impl {
  const char *m_name;
  uint32_t (*m_fn)(const byte *, size_t);
};

/** @return the implementations this CPU can run: the table driven one always,
the hardware ones if the CPU has SSE4.2 (and PCLMUL). */
std::vector<Impl> impls() {
  std::vector<Impl> v{{"software", crc32::software}};

#if defined(__x86_64__)
  if (crc32::can_use_crc32()) {
    v.push_back({"unrolled_loop_poly_mul", crc32::unrolled_loop_poly_mul});

    if (crc32::can_use_poly_mul()) {
      v.push_back({"pclmul", crc32::pclmul});
    }
  }
#endif /* __x86_64__ */

  return v;
}

/** The function crc32::checksum was built on, nullptr if it is not a plain function. */
uint32_t (*checksum_target())(const byte *, size_t) {
  using Fn = uint32_t (*)(const byte *, size_t) noexcept;
  using Fn2 = uint32_t (*)(const byte *, size_t);

  if (auto p = crc32::checksum.target<Fn>(); p != nullptr) {
    return *p;
  } else if (auto p2 = crc32::checksum.target<Fn2>(); p2 != nullptr) {
    return *p2;
  }

  return nullptr;
}

/** @return n random bytes from seed. */
std::vector<byte> random_bytes(size_t n, uint32_t seed) {
  std::mt19937 rng(seed);
  std::vector<byte> v(n);

  for (auto &b : v) {
    b = byte(rng());
  }

  return v;
}

uint32_t crc(const Impl &impl, const std::string &s) {
  return impl.m_fn(reinterpret_cast<const byte *>(s.data()), s.size());
}

/** The check value of CRC-32C: "123456789" -> 0xE3069283, and the empty
input -> 0, for every implementation and for crc32::checksum. */
TEST(Crc32Test, CheckValue) {
  for (const auto &impl : impls()) {
    EXPECT_EQ(crc(impl, "123456789"), 0xE3069283U) << impl.m_name;
    EXPECT_EQ(crc(impl, ""), 0U) << impl.m_name;
    EXPECT_EQ(impl.m_fn(nullptr, 0), 0U) << impl.m_name;
  }

  const std::string s{"123456789"};

  EXPECT_EQ(crc32::checksum(reinterpret_cast<const byte *>(s.data()), s.size()), 0xE3069283U);
}

/** The test vectors of RFC 3720 (iSCSI), B.4. */
TEST(Crc32Test, Rfc3720Vectors) {
  std::array<byte, 32> zeros{}, ones{}, inc{}, dec{};

  ones.fill(0xFF);

  for (size_t i = 0; i < 32; ++i) {
    inc[i] = byte(i);
    dec[i] = byte(31 - i);
  }

  /* An iSCSI read command PDU. */
  const std::array<byte, 48> read_pdu{0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                                      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  for (const auto &impl : impls()) {
    EXPECT_EQ(impl.m_fn(zeros.data(), 32), 0x8A9136AAU) << impl.m_name;
    EXPECT_EQ(impl.m_fn(ones.data(), 32), 0x62A8AB43U) << impl.m_name;
    EXPECT_EQ(impl.m_fn(inc.data(), 32), 0x46DD794EU) << impl.m_name;
    EXPECT_EQ(impl.m_fn(dec.data(), 32), 0x113FDB5CU) << impl.m_name;
    EXPECT_EQ(impl.m_fn(read_pdu.data(), read_pdu.size()), 0xD9963A56U) << impl.m_name;
  }
}

/** Single bytes 0..255: a CRC is a bijection on one byte, all differ. */
TEST(Crc32Test, SingleBytes) {
  for (const auto &impl : impls()) {
    std::set<uint32_t> seen;

    for (int b = 0; b < 256; ++b) {
      /* Aligned: a 1 byte buffer at an odd address is ShortUnaligned. */
      alignas(8) const byte v = byte(b);

      EXPECT_EQ(impl.m_fn(&v, 1), crc32::software(&v, 1)) << impl.m_name << " " << b;
      seen.insert(impl.m_fn(&v, 1));
    }

    EXPECT_EQ(seen.size(), 256UL) << impl.m_name;
  }
}

/** Every implementation gives the software value on random buffers of
every length 0..4096 at the 8 alignments mod 8, where the buffer is not
shorter than the unaligned prefix (see ShortUnaligned). */
TEST(Crc32Test, ImplsAgreeAllLengths) {
  const auto data = random_bytes(4096 + 64, 1);

  /* Align the base to 8, the offsets give each alignment. */
  const auto base = static_cast<const byte *>(ut_align(data.data(), 8));

  for (size_t align = 0; align < 8; ++align) {
    const auto p = base + align;
    const size_t prefix = (8 - align) & 7;

    for (size_t len = prefix; len <= 4096; ++len) {
      const auto expected = crc32::software(p, len);

      for (const auto &impl : impls()) {
        ASSERT_EQ(impl.m_fn(p, len), expected) << impl.m_name << " align " << align << " len " << len;
      }

      ASSERT_EQ(crc32::checksum(p, len), expected) << "align " << align << " len " << len;
    }
  }
}

/** The lengths around the chunk sizes the hardware versions special case
(3 x 5440 for pages, 3 x 168 for log blocks, 8) and a whole page, at odd
alignments. */
TEST(Crc32Test, ImplsAgreeChunkEdges) {
  const auto data = random_bytes(3 * 5440 * 2 + 64, 2);
  const auto base = static_cast<const byte *>(ut_align(data.data(), 8));
  std::vector<size_t> lens{UNIV_PAGE_SIZE, UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_END_LSN_CHKSUM, 2 * 3 * 5440};

  for (const size_t edge : {size_t(8), size_t(3 * 168), size_t(3 * 5440)}) {
    for (size_t k = 1; k <= 2; ++k) {
      for (size_t d = 0; d < 17; ++d) {
        lens.push_back(k * edge + d);

        if (k * edge >= d) {
          lens.push_back(k * edge - d);
        }
      }
    }
  }

  for (const size_t align : {0UL, 1UL, 3UL, 4UL, 7UL}) {
    for (const auto len : lens) {
      if (len < ((8 - align) & 7) || len + align > data.size() - 8) {
        continue;
      }

      const auto expected = crc32::software(base + align, len);

      for (const auto &impl : impls()) {
        EXPECT_EQ(impl.m_fn(base + align, len), expected) << impl.m_name << " align " << align << " len " << len;
      }
    }
  }
}

/** The bytes outside [p, p + len) don't change the value: nothing reads
before or past the buffer. */
TEST(Crc32Test, NoReadOutsideBuffer) {
  auto data = random_bytes(512, 3);
  const auto base = static_cast<byte *>(ut_align(data.data() + 16, 8));

  for (size_t align = 0; align < 8; ++align) {
    for (const size_t len : {8UL, 9UL, 15UL, 64UL, 100UL, 300UL}) {
      const auto p = base + align;

      for (const auto &impl : impls()) {
        const auto v = impl.m_fn(p, len);

        /* Scribble the 8 bytes on both sides. */
        for (size_t i = 1; i <= 8; ++i) {
          p[-ptrdiff_t(i)] ^= 0xA5;
          p[len + i - 1] ^= 0x5A;
        }

        EXPECT_EQ(impl.m_fn(p, len), v) << impl.m_name << " align " << align << " len " << len;

        for (size_t i = 1; i <= 8; ++i) {
          p[-ptrdiff_t(i)] ^= 0xA5;
          p[len + i - 1] ^= 0x5A;
        }
      }
    }
  }
}

/* A buffer shorter than its unaligned prefix: calculate_with() must stop at its
end, or len wraps and consume_chunks() runs off the end of memory. Each case runs in a
child: the wrap around crashes. */
TEST(Crc32Test, ShortUnaligned) {
  const auto data = random_bytes(64, 4);
  const auto base = static_cast<const byte *>(ut_align(data.data(), 8));

  for (size_t align = 1; align < 8; ++align) {
    for (size_t len = 0; len < ((8 - align) & 7); ++len) {
      for (const auto &impl : impls()) {
        const auto expected = crc32::software(base + align, len);
        const auto pid = fork();

        if (pid == 0) {
          _exit(impl.m_fn(base + align, len) == expected ? 0 : 1);
        }

        int status{};

        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
          << impl.m_name << " align " << align << " len " << len
          << (WIFSIGNALED(status) ? " killed by signal " + std::to_string(WTERMSIG(status)) : " wrong value");
      }
    }
  }
}

/** crc32::checksum is a real CRC, not the constant NO_CHECKSUM that it was
when the hardware code was compiled out, and on a CPU with SSE4.2 it is
not the table driven fallback. */
TEST(Crc32Test, ChecksumIsReal) {
  const auto a = random_bytes(UNIV_PAGE_SIZE, 5);
  auto b = a;

  b[1000] ^= 1;

  const auto ca = crc32::checksum(a.data(), a.size());
  const auto cb = crc32::checksum(b.data(), b.size());

  EXPECT_NE(ca, cb);
  EXPECT_NE(ca, uint32_t(crc32::NO_CHECKSUM));
  EXPECT_NE(cb, uint32_t(crc32::NO_CHECKSUM));
  EXPECT_EQ(ca, crc32::software(a.data(), a.size()));

  ASSERT_TRUE(bool(crc32::checksum));

#if defined(__x86_64__)
  const auto target = checksum_target();

  ASSERT_NE(target, nullptr);

  if (crc32::can_use_crc32()) {
    EXPECT_NE(target, &crc32::software) << "the hardware CRC-32C exists but the software one was chosen";
  }
#endif /* __x86_64__ */
}

/** Every single bit flip in a page changes the CRC (a CRC detects all
single bit errors). */
TEST(Crc32Test, DetectsEveryBitFlip) {
  auto page = random_bytes(UNIV_PAGE_SIZE, 6);
  const auto c0 = crc32::checksum(page.data(), page.size());

  for (size_t bit = 0; bit < page.size() * 8; bit += 7) {
    page[bit / 8] ^= byte(1 << (bit % 8));
    ASSERT_NE(crc32::checksum(page.data(), page.size()), c0) << bit;
    page[bit / 8] ^= byte(1 << (bit % 8));
  }

  EXPECT_EQ(crc32::checksum(page.data(), page.size()), c0);
}

#if defined(__x86_64__)
/** calculate_with() continues from a previous value: crc(a ++ b) is
crc(b) started from crc(a), for the hardware versions. */
TEST(Crc32Test, Continuation) {
  if (!crc32::can_use_crc32()) {
    GTEST_SKIP() << "no SSE4.2";
  }

  const auto data = random_bytes(3000, 7);

  for (const size_t split : {0UL, 1UL, 7UL, 8UL, 504UL, 1000UL, 2999UL, 3000UL}) {
    const auto first = crc32::calculate_with<crc32::UseUnrolledLoopPolyMul>(0, data.data(), split);
    const auto all = crc32::calculate_with<crc32::UseUnrolledLoopPolyMul>(first, data.data() + split, data.size() - split);

    EXPECT_EQ(all, crc32::software(data.data(), data.size())) << split;
  }
}
#endif /* __x86_64__ */

/** x^(8 len) mod P: len 0 is 1, and appending len zero bytes multiplies by it. */
TEST(Crc32Test, PolyHelpers) {
  static_assert(crc32::compute_x_to_8len(0) == 1);
  static_assert(crc32::flip_at_32(1) == (uint64_t{1} << 32));
  static_assert(crc32::flip_at_32(0x80000000U) == 2);
  static_assert(crc32::flip_at_32(0) == 0);

  /* x^8 needs no reduction. */
  EXPECT_EQ(crc32::compute_x_to_8len(1), 0x100U);
  EXPECT_EQ(crc32::compute_x_to_8len(3), 0x1000000U);
  /* x^32 = P without its top term. */
  EXPECT_EQ(crc32::compute_x_to_8len(4), crc32::CRC32C_POLYNOMIAL);
}

/** The implementations can run in many threads at once (no shared state). */
TEST(Crc32Test, Concurrent) {
  const auto data = random_bytes(UNIV_PAGE_SIZE, 8);
  const auto expected = crc32::software(data.data(), data.size());
  std::atomic<int> bad{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 200; ++i) {
        if (crc32::checksum(data.data(), data.size()) != expected) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(bad, 0);
}

/* ---------------------------------------------------------- page checksum */

/** @return the checksum of page, computed with the table driven CRC. */
uint32_t reference_page_checksum(const byte *page) {
  return crc32::software(page + FIL_PAGE_OFFSET, FIL_PAGE_FILE_FLUSH_LSN - FIL_PAGE_OFFSET) ^
         crc32::software(page + FIL_PAGE_DATA, UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_END_LSN_CHKSUM);
}

/** An aligned page of random bytes. */
struct Page {
  explicit Page(uint32_t seed) : m_mem(random_bytes(2 * UNIV_PAGE_SIZE, seed)) {
    m_page = static_cast<byte *>(ut_align(m_mem.data(), UNIV_PAGE_SIZE));
  }

  std::vector<byte> m_mem;
  byte *m_page{};
};

/** The checksum is the XOR of the CRC of the header and of the body. */
TEST(PageChecksumTest, Formula) {
  for (uint32_t seed = 0; seed < 8; ++seed) {
    Page p(seed);

    EXPECT_EQ(buf_page_data_calc_checksum(p.m_page), reference_page_checksum(p.m_page));
  }

  std::vector<byte> zero(2 * UNIV_PAGE_SIZE);
  const auto page = static_cast<byte *>(ut_align(zero.data(), UNIV_PAGE_SIZE));

  EXPECT_EQ(buf_page_data_calc_checksum(page), reference_page_checksum(page));
  EXPECT_NE(buf_page_data_calc_checksum(page), uint32_t(crc32::NO_CHECKSUM));
}

/** The checksum doesn't depend on the alignment of the page frame. */
TEST(PageChecksumTest, AnyAlignment) {
  Page p(9);
  std::vector<byte> copy(UNIV_PAGE_SIZE + 16);
  const auto expected = buf_page_data_calc_checksum(p.m_page);

  for (size_t align = 0; align < 16; ++align) {
    std::memcpy(copy.data() + align, p.m_page, UNIV_PAGE_SIZE);
    EXPECT_EQ(buf_page_data_calc_checksum(copy.data() + align), expected) << align;
  }
}

/** A change of any byte in [FIL_PAGE_OFFSET, FIL_PAGE_FILE_FLUSH_LSN) or in
[FIL_PAGE_DATA, UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM) changes the checksum. */
TEST(PageChecksumTest, CoveredBytes) {
  Page p(10);
  const auto c0 = buf_page_data_calc_checksum(p.m_page);

  for (ulint i = 0; i < UNIV_PAGE_SIZE; ++i) {
    const bool covered = (i >= FIL_PAGE_OFFSET && i < FIL_PAGE_FILE_FLUSH_LSN) ||
                         (i >= FIL_PAGE_DATA && i < UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM);

    if (!covered) {
      continue;
    }

    p.m_page[i] ^= 0x01;
    ASSERT_NE(buf_page_data_calc_checksum(p.m_page), c0) << i;
    p.m_page[i] ^= 0x81;
    ASSERT_NE(buf_page_data_calc_checksum(p.m_page), c0) << i;
    p.m_page[i] ^= 0x80;
  }

  EXPECT_EQ(buf_page_data_calc_checksum(p.m_page), c0);
}

/** The checksum field [0, 4), the flush LSN and space id [26, 38) and the
trailer (the last 8 bytes) are not covered. */
TEST(PageChecksumTest, UncoveredBytes) {
  Page p(11);
  const auto c0 = buf_page_data_calc_checksum(p.m_page);
  std::vector<ulint> offs;

  for (ulint i = FIL_PAGE_SPACE_OR_CHKSUM; i < FIL_PAGE_OFFSET; ++i) {
    offs.push_back(i);
  }

  for (ulint i = FIL_PAGE_FILE_FLUSH_LSN; i < FIL_PAGE_DATA; ++i) {
    offs.push_back(i);
  }

  for (ulint i = UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM; i < UNIV_PAGE_SIZE; ++i) {
    offs.push_back(i);
  }

  for (const auto i : offs) {
    for (int v = 0; v < 256; v += 17) {
      p.m_page[i] = byte(v);
      ASSERT_EQ(buf_page_data_calc_checksum(p.m_page), c0) << i;
    }
  }

  /* All of them at once. */
  for (const auto i : offs) {
    p.m_page[i] = 0xFF;
  }

  EXPECT_EQ(buf_page_data_calc_checksum(p.m_page), c0);
}

/** Writing the checksum into its own field doesn't change it. */
TEST(PageChecksumTest, StoredChecksumIsStable) {
  Page p(12);
  const auto c = buf_page_data_calc_checksum(p.m_page);

  mach_write_to_4(p.m_page + FIL_PAGE_SPACE_OR_CHKSUM, c);
  EXPECT_EQ(buf_page_data_calc_checksum(p.m_page), c);
  EXPECT_EQ(mach_read_from_4(p.m_page + FIL_PAGE_SPACE_OR_CHKSUM), c);
}

/** Different pages get different checksums. */
TEST(PageChecksumTest, Distinct) {
  std::set<uint32_t> seen;

  for (uint32_t seed = 100; seed < 300; ++seed) {
    Page p(seed);

    seen.insert(buf_page_data_calc_checksum(p.m_page));
  }

  EXPECT_EQ(seen.size(), 200UL);
}

/* ----------------------------------------------------------------- folds */

/** ut_fold_ulint_pair() is the documented formula; deterministic, order matters. */
TEST(FoldTest, UlintPair) {
  auto formula = [](ulint n1, ulint n2) {
    return ((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1) ^ UT_HASH_RANDOM_MASK) + n2;
  };

  for (const ulint a : {0UL, 1UL, 255UL, 0xFFFFFFFFUL, ~0UL}) {
    for (const ulint b : {0UL, 1UL, 7UL, 0x12345678UL, ~0UL}) {
      EXPECT_EQ(ut_fold_ulint_pair(a, b), formula(a, b));
    }
  }

  EXPECT_NE(ut_fold_ulint_pair(1, 2), ut_fold_ulint_pair(2, 1));
  EXPECT_EQ(ut_fold_ulint_pair(0, 0), formula(0, 0));
}

/** ut_uint64_fold() folds the low and the high 32 bits. */
TEST(FoldTest, Uint64) {
  for (const uint64_t d : {0ULL, 1ULL, 0xFFFFFFFFULL, 0x100000000ULL, 0x123456789ABCDEF0ULL, ~0ULL}) {
    EXPECT_EQ(ut_uint64_fold(d), ut_fold_ulint_pair(ulint(d & 0xFFFFFFFF), ulint(d >> 32)));
  }

  EXPECT_NE(ut_uint64_fold(1), ut_uint64_fold(1ULL << 32));
}

/** The empty string and buffer fold to 0; a string folds like its bytes. */
TEST(FoldTest, StringAndBinary) {
  EXPECT_EQ(ut_fold_string(""), 0UL);
  EXPECT_EQ(ut_fold_binary(nullptr, 0), 0UL);

  for (const char *s : {"a", "test/t", "SYS_TABLES", "a much longer name of a table in a database"}) {
    const auto b = reinterpret_cast<const byte *>(s);

    EXPECT_EQ(ut_fold_string(s), ut_fold_binary(b, strlen(s))) << s;

    ulint fold{};

    for (size_t i = 0; s[i] != '\0'; ++i) {
      fold = ut_fold_ulint_pair(fold, ulint(byte(s[i])));
    }

    EXPECT_EQ(ut_fold_string(s), fold) << s;
  }

  /* Byte order matters, and a prefix differs from the whole. */
  EXPECT_NE(ut_fold_string("ab"), ut_fold_string("ba"));
  EXPECT_NE(ut_fold_string("a"), ut_fold_string("aa"));
  /* A leading NUL byte counts in the binary fold. */
  const byte nul[2]{0, 'a'};

  EXPECT_NE(ut_fold_binary(nul, 2), ut_fold_binary(nul + 1, 1));
}

/** Bytes >= 0x80: ut_fold_string() sign-extends char, ut_fold_binary()
doesn't, so the two folds of the same UTF-8 name differ. Nothing in the
engine folds a name both ways today (names: ut_fold_string(), field data:
ut_fold_binary()), so this pins the behaviour rather than flags a bug. */
TEST(FoldTest, HighBitBytes) {
  const char *s = "caf\xc3\xa9";
  const auto b = reinterpret_cast<const byte *>(s);

  ulint signed_fold{};

  for (size_t i = 0; s[i] != '\0'; ++i) {
    signed_fold = ut_fold_ulint_pair(signed_fold, ulint(s[i]));
  }

  EXPECT_EQ(ut_fold_string(s), signed_fold);

  if (std::is_signed_v<char>) {
    EXPECT_NE(ut_fold_string(s), ut_fold_binary(b, strlen(s)));
  }
}

/** ut_hash_ulint() is in [0, table_size) and spreads consecutive keys. */
TEST(FoldTest, HashUlint) {
  for (const ulint size : {1UL, 2UL, 101UL, 1009UL, 65537UL}) {
    std::set<ulint> cells;

    for (ulint k = 0; k < 10000; ++k) {
      const auto h = ut_hash_ulint(k, size);

      ASSERT_LT(h, size);
      cells.insert(h);
    }

    EXPECT_EQ(cells.size(), std::min<ulint>(size, 10000)) << size;
  }
}

bool is_prime(ulint n) {
  if (n < 2) {
    return false;
  }

  for (ulint i = 2; i * i <= n; ++i) {
    if (n % i == 0) {
      return false;
    }
  }

  return true;
}

/** ut_find_prime(n) is a prime > n, and not within 5% of a power of two. */
TEST(FoldTest, FindPrime) {
  for (const ulint n : {1UL, 100UL, 101UL, 1000UL, 1024UL, 4000UL, 65536UL, 100000UL, 1UL << 20, 3000000UL}) {
    const auto p = ut_find_prime(n);

    EXPECT_TRUE(is_prime(p)) << n << " -> " << p;
    EXPECT_GT(p, n);

    const auto pow2 = ut_2_power_up(p);

    EXPECT_FALSE(double(p) > 0.97 * double(pow2) || double(p) < 1.03 * double(pow2 / 2)) << n << " -> " << p;
  }

  /* Deterministic. */
  EXPECT_EQ(ut_find_prime(5000), ut_find_prime(5000));
}

/* ---------------------------------------------------------------- random */

/** The generator is deterministic for a seed and the seed is per thread. */
TEST(RndTest, SeedAndThreadLocal) {
  ut_rnd_set_seed(42);

  std::vector<ulint> a;

  for (int i = 0; i < 100; ++i) {
    a.push_back(ut_rnd_gen_ulint());
  }

  ut_rnd_set_seed(42);

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(ut_rnd_gen_ulint(), a[i]) << i;
  }

  /* Another thread starts from the default state, untouched by our seed. */
  ulint other_first{}, other_after_seed{};

  std::thread t([&] {
    other_first = ut_rnd_gen_ulint();
    ut_rnd_set_seed(42);
    other_after_seed = ut_rnd_gen_ulint();
  });

  t.join();

  EXPECT_EQ(other_after_seed, a[0]);

  std::thread t2([&] { EXPECT_EQ(ut_rnd_gen_ulint(), other_first); });

  t2.join();

  /* No short cycle and few repeats. */
  std::set<ulint> seen(a.begin(), a.end());

  EXPECT_EQ(seen.size(), a.size());
}

/** ut_rnd_gen_next_ulint() is a pure function of its argument. */
TEST(RndTest, NextIsPure) {
  for (const ulint v : {0UL, 1UL, 12345UL, ~0UL}) {
    EXPECT_EQ(ut_rnd_gen_next_ulint(v), ut_rnd_gen_next_ulint(v));
  }

  EXPECT_NE(ut_rnd_gen_next_ulint(1), ut_rnd_gen_next_ulint(2));
}

/** ut_rnd_interval() stays in [low, high], hits both ends of a small range. */
TEST(RndTest, Interval) {
  ut_rnd_set_seed(7);

  EXPECT_EQ(ut_rnd_interval(5, 5), 5UL);
  EXPECT_EQ(ut_rnd_interval(0, 0), 0UL);
  EXPECT_EQ(ut_rnd_interval(~0UL, ~0UL), ~0UL);

  std::set<ulint> seen;

  for (int i = 0; i < 10000; ++i) {
    const auto v = ut_rnd_interval(10, 20);

    ASSERT_GE(v, 10UL);
    ASSERT_LE(v, 20UL);
    seen.insert(v);
  }

  EXPECT_EQ(seen.size(), 11UL);

  /* The spin delay range, as used by the mutexes. */
  for (int i = 0; i < 1000; ++i) {
    ASSERT_LE(ut_rnd_interval(0, 6), 6UL);
  }

  /* A range one short of the whole space. */
  for (int i = 0; i < 1000; ++i) {
    ASSERT_GE(ut_rnd_interval(1, ~0UL), 1UL);
  }
}

/* BUG: ut_rnd_interval(0, ULINT_MAX) (include/ut0rnd.h:173) divides by
high - low + 1, which wraps to 0: SIGFPE. No caller passes the whole range
today. Fix:
-  return (low + (rnd % (high - low + 1)));
+  const ulint span = high - low + 1;
+  return span == 0 ? rnd : low + rnd % span; */
TEST(RndDeathTest, IntervalWholeRange) {
  EXPECT_EXIT(
    {
      const auto v = ut_rnd_interval(0, ~0UL);
      (void)v;
      _exit(0);
    },
    ::testing::ExitedWithCode(0),
    ""
  );
}

/** ut_rnd_gen_bool() gives both values about as often. */
TEST(RndTest, Bool) {
  ut_rnd_set_seed(99);

  int n_true{};

  for (int i = 0; i < 10000; ++i) {
    n_true += ut_rnd_gen_bool() ? 1 : 0;
  }

  EXPECT_GT(n_true, 4000);
  EXPECT_LT(n_true, 6000);
}

/* ---------------------------------------------------------------- ut_new */

/** ut_new() returns zeroed memory aligned for any type. */
TEST(UtMemTest, NewZeroedAndAligned) {
  for (const ulint n : {1UL, 7UL, 8UL, 16UL, 100UL, 4096UL, 1UL << 20}) {
    auto p = static_cast<byte *>(ut_new(n));

    ASSERT_NE(p, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % alignof(std::max_align_t), 0UL) << n;

    for (ulint i = 0; i < n; ++i) {
      ASSERT_EQ(p[i], 0) << n << " " << i;
    }

    std::memset(p, 0xEE, n);
    ut_delete(p);
  }
}

/** ut_new(0) gives a pointer that can be freed; ut_delete(nullptr) is a no-op. */
TEST(UtMemTest, ZeroAndNull) {
  auto p = ut_new(0);

  ut_delete(p);
  ut_delete(nullptr);
}

/** Many live blocks don't overlap. */
TEST(UtMemTest, NoOverlap) {
  std::vector<byte *> blocks;

  for (int i = 0; i < 256; ++i) {
    auto p = static_cast<byte *>(ut_new(64));

    std::memset(p, i, 64);
    blocks.push_back(p);
  }

  for (int i = 0; i < 256; ++i) {
    for (int j = 0; j < 64; ++j) {
      ASSERT_EQ(blocks[i][j], byte(i));
    }

    ut_delete(blocks[i]);
  }
}

/** ut_realloc() keeps the contents, acts as malloc on nullptr and as free on 0. */
TEST(UtMemTest, Realloc) {
  auto p = static_cast<byte *>(ut_realloc(nullptr, 16));

  ASSERT_NE(p, nullptr);

  for (int i = 0; i < 16; ++i) {
    p[i] = byte(i);
  }

  p = static_cast<byte *>(ut_realloc(p, 1 << 20));

  for (int i = 0; i < 16; ++i) {
    EXPECT_EQ(p[i], byte(i));
  }

  p = static_cast<byte *>(ut_realloc(p, 4));

  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(p[i], byte(i));
  }

  EXPECT_EQ(ut_realloc(p, 0), nullptr);
}

/** An allocation that cannot be satisfied is fatal, not nullptr. */
TEST(UtMemDeathTest, NewTooBig) {
  EXPECT_DEATH(
    {
      auto p = ut_new(ulint(1) << 62);
      ut::keep(p);
    },
    "Cannot allocate"
  );
}

TEST(UtMemDeathTest, ReallocTooBig) {
  EXPECT_DEATH(
    {
      auto p = ut_new(8);
      p = ut_realloc(p, ulint(1) << 62);
      ut::keep(p);
    },
    "Cannot reallocate"
  );
}

/** ut_strlcpy(): BSD strlcpy(): truncates, always terminates, returns strlen(src). */
TEST(UtStrTest, Strlcpy) {
  char buf[8];

  std::memset(buf, 'X', sizeof(buf));
  EXPECT_EQ(ut_strlcpy(buf, "abc", sizeof(buf)), 3UL);
  EXPECT_STREQ(buf, "abc");

  EXPECT_EQ(ut_strlcpy(buf, "abcdefghij", sizeof(buf)), 10UL);
  EXPECT_STREQ(buf, "abcdefg");

  EXPECT_EQ(ut_strlcpy(buf, "abcdefg", sizeof(buf)), 7UL);
  EXPECT_STREQ(buf, "abcdefg");

  EXPECT_EQ(ut_strlcpy(buf, "xyz", 1), 3UL);
  EXPECT_STREQ(buf, "");

  /* Size 0 writes nothing. */
  std::memset(buf, 'X', sizeof(buf));
  EXPECT_EQ(ut_strlcpy(buf, "xyz", 0), 3UL);
  EXPECT_EQ(buf[0], 'X');

  EXPECT_EQ(ut_strlcpy(buf, "", sizeof(buf)), 0UL);
  EXPECT_STREQ(buf, "");
}

/** ut_strlcpy_rev() keeps the end of src. */
TEST(UtStrTest, StrlcpyRev) {
  char buf[4];

  EXPECT_EQ(ut_strlcpy_rev(buf, "abcdef", sizeof(buf)), 6UL);
  EXPECT_STREQ(buf, "def");

  EXPECT_EQ(ut_strlcpy_rev(buf, "ab", sizeof(buf)), 2UL);
  EXPECT_STREQ(buf, "ab");

  EXPECT_EQ(ut_strlcpy_rev(buf, "abc", sizeof(buf)), 3UL);
  EXPECT_STREQ(buf, "abc");

  EXPECT_EQ(ut_strlcpy_rev(buf, "abc", 1), 3UL);
  EXPECT_STREQ(buf, "");

  buf[0] = 'X';
  EXPECT_EQ(ut_strlcpy_rev(buf, "abc", 0), 3UL);
  EXPECT_EQ(buf[0], 'X');
}

/** ut_strlcat(): BSD strlcat(). */
TEST(UtStrTest, Strlcat) {
  char buf[8] = "ab";

  EXPECT_EQ(ut_strlcat(buf, "cd", sizeof(buf)), 4UL);
  EXPECT_STREQ(buf, "abcd");

  EXPECT_EQ(ut_strlcat(buf, "efghij", sizeof(buf)), 10UL);
  EXPECT_STREQ(buf, "abcdefg");

  /* Full: nothing appended. */
  EXPECT_EQ(ut_strlcat(buf, "z", sizeof(buf)), 8UL);
  EXPECT_STREQ(buf, "abcdefg");

  /* dst not terminated within size: size + strlen(src). */
  EXPECT_EQ(ut_strlcat(buf, "zz", 3), 5UL);
  EXPECT_STREQ(buf, "abcdefg");

  EXPECT_EQ(ut_strlcat(buf, "", sizeof(buf)), 7UL);
}

/** ut_strncpy() copies at most n - 1 characters and terminates. */
TEST(UtStrTest, Strncpy) {
  char buf[8];

  EXPECT_EQ(ut_strncpy(buf, "abc", sizeof(buf)), buf);
  EXPECT_STREQ(buf, "abc");

  ut_strncpy(buf, "abcdefghij", sizeof(buf));
  EXPECT_STREQ(buf, "abcdefg");

  ut_strncpy(buf, "abc", 1);
  EXPECT_STREQ(buf, "");
}

/* BUG: ut_strncpy(dst, src, 0) (ut/ut0ut.cc:294) writes dst[n - 1], i.e.
dst[SIZE_MAX], one byte before dst: it zeroes the byte before the buffer.
Fix:
 char *ut_strncpy(char *dst, const char *src, size_t n) noexcept {
+  if (n == 0) {
+    return dst;
+  }
   strncpy(dst, src, n); */
TEST(UtStrTest, StrncpyZero) {
  char buf[4] = "xyz";

  ut_strncpy(buf + 1, "abc", 0);
  EXPECT_EQ(buf[0], 'x');
}

/** The thin wrappers of the C library. */
TEST(UtStrTest, Wrappers) {
  EXPECT_EQ(ut_strlen(""), 0UL);
  EXPECT_EQ(ut_strlen("abc"), 3UL);
  EXPECT_EQ(ut_strcmp("a", "a"), 0);
  EXPECT_LT(ut_strcmp("a", "b"), 0);
  EXPECT_GT(ut_strcmp("b", "a"), 0);
  EXPECT_EQ(ut_strncmp("abc", "abd", 2), 0);
  EXPECT_LT(ut_strncmp("abc", "abd", 3), 0);
  EXPECT_EQ(ut_strncmp("x", "y", 0), 0);

  char buf[16];

  EXPECT_EQ(ut_snprintf(buf, sizeof(buf), "%d-%s", 42, "x"), 4);
  EXPECT_STREQ(buf, "42-x");
  EXPECT_EQ(ut_snprintf(buf, 3, "%s", "abcdef"), 6);
  EXPECT_STREQ(buf, "ab");
  EXPECT_EQ(ut_snprintf(nullptr, 0, "%s", "abc"), 3);
}

/** ut_format_name() quotes a name, and a db/table name as "db"."table". */
TEST(UtStrTest, FormatName) {
  char buf[64];

  EXPECT_EQ(ut_format_name(buf, sizeof(buf), "t1"), 4UL);
  EXPECT_STREQ(buf, "\"t1\"");

  EXPECT_EQ(ut_format_name(buf, sizeof(buf), "test/t1"), 11UL);
  EXPECT_STREQ(buf, "\"test\".\"t1\"");

  /* Only the first slash splits. */
  ut_format_name(buf, sizeof(buf), "a/b/c");
  EXPECT_STREQ(buf, "\"a\".\"b/c\"");

  ut_format_name(buf, sizeof(buf), "");
  EXPECT_STREQ(buf, "\"\"");

  ut_format_name(buf, sizeof(buf), "/t");
  EXPECT_STREQ(buf, "\"\".\"t\"");

  /* Size 0: nothing is written. */
  buf[0] = 'X';
  EXPECT_EQ(ut_format_name(buf, 0, "t1"), 0UL);
  EXPECT_EQ(buf[0], 'X');

  /* Truncated output is terminated; the return value is snprintf()'s: the
  length it would have had, not the number of characters written as
  documented in ut0ut.h. No caller uses the return value. */
  EXPECT_EQ(ut_format_name(buf, 4, "test/t1"), 11UL);
  EXPECT_STREQ(buf, "\"te");
}

/* ------------------------------------------------------------ time, bits */

/** The clocks agree with each other and with the C library. */
TEST(UtTimeTest, Clocks) {
  const auto t = ut_time();
  uint64_t us_out{};
  const auto us = ut_time_us(&us_out);
  const auto ms = ut_time_ms();
  ulint sec{}, usec{};

  EXPECT_EQ(us, us_out);
  EXPECT_EQ(ut_usectime(&sec, &usec), 0);
  EXPECT_LT(usec, 1000000UL);

  EXPECT_LE(std::abs(double(us / 1000000) - double(t)), 1.0);
  EXPECT_LE(std::abs(double(ms / 1000) - double(t)), 1.0);
  EXPECT_LE(std::abs(double(sec) - double(t)), 1.0);
  EXPECT_EQ(ut_time_us(nullptr) >= us, true);

  EXPECT_EQ(ut_difftime(t + 5, t), 5.0);
  EXPECT_EQ(ut_difftime(t, t + 5), -5.0);
}

/** ut_time_us() moves forward across a sleep. */
TEST(UtTimeTest, Advances) {
  const auto a = ut_time_us(nullptr);
  const auto ma = ut_time_ms();

  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  EXPECT_GE(ut_time_us(nullptr) - a, 19000UL);
  EXPECT_GE(ut_time_ms() - ma, 19UL);
}

/** ut_sprintf_timestamp(): "YYMMDD HH:MM:SS", 15 characters, the hour
space padded (the comment in ut0ut.h says 13..14). */
TEST(UtTimeTest, SprintfTimestamp) {
  char buf[64];

  std::memset(buf, 'X', sizeof(buf));
  ut_sprintf_timestamp(buf);

  const std::string s{buf};

  EXPECT_EQ(s.size(), 15UL) << s;
  EXPECT_TRUE(std::regex_match(s, std::regex(R"(\d{6} [ \d]\d:\d\d:\d\d)"))) << s;
  EXPECT_EQ(buf[16], 'X');
}

/** ut_print_timestamp() prints the same format to a FILE. */
TEST(UtTimeTest, PrintTimestamp) {
  ut::TestDir dir;
  const auto path = dir.m_path / "ts";
  auto f = fopen(path.c_str(), "w+");

  ASSERT_NE(f, nullptr);
  ut_print_timestamp(f);
  fflush(f);
  rewind(f);

  char buf[64]{};

  ASSERT_NE(fgets(buf, sizeof(buf), f), nullptr);
  fclose(f);

  EXPECT_TRUE(std::regex_match(std::string(buf), std::regex(R"(\d{6} [ \d]\d:\d\d:\d\d)"))) << buf;
}

/** ut_delay() returns and its return value depends on the delay. */
TEST(UtTimeTest, Delay) {
  EXPECT_EQ(ut_delay(0), 0UL);
  ut::keep(ut_delay(10));
}

/** ut_get_high32(), ut_2_exp(), ut_2_log(), ut_2_power_up(). */
TEST(UtBitsTest, Powers) {
  EXPECT_EQ(ut_get_high32(0x123456789ABCDEF0UL), 0x12345678UL);
  EXPECT_EQ(ut_get_high32(0xFFFFFFFFUL), 0UL);
  EXPECT_EQ(ut_get_high32(0x7FFFFFFF00000000UL), 0x7FFFFFFFUL);

  for (ulint n = 0; n < 64; ++n) {
    EXPECT_EQ(ut_2_exp(n), ulint(1) << n);
    EXPECT_EQ(ut_2_power_up(ulint(1) << n), ulint(1) << n) << n;

    if (n > 1) {
      EXPECT_EQ(ut_2_power_up((ulint(1) << n) - 1), ulint(1) << n) << n;
      EXPECT_EQ(ut_2_log(ulint(1) << n), n) << n;
      EXPECT_EQ(ut_2_log((ulint(1) << n) + 1), n + 1) << n;
    }
  }

  EXPECT_EQ(ut_2_power_up(1), 1UL);
  EXPECT_EQ(ut_2_power_up(3), 4UL);

  EXPECT_EQ(ut_2_log(2), 1UL);
  EXPECT_EQ(ut_2_log(3), 2UL);
  /* ut_2_log(1) is 1, not 0 (log2(1) rounded up): the loop always adds one.
  Kept from InnoDB; the callers size hash tables with it and expect >= 1. */
  EXPECT_EQ(ut_2_log(1), 1UL);
}

/* BUG: ut_get_high32(a) (ut/ut0ut.cc:51) shifts a as int64_t: for a >= 2^63
the shift is arithmetic and the result is sign extended,
ut_get_high32(~0UL) is ~0UL instead of 0xFFFFFFFF. No caller today. Fix:
-  auto i = (int64_t)a;
-  i = i >> 32;
-  return (ulint)i;
+  return a >> 32; */
TEST(UtBitsTest, High32TopBit) {
  EXPECT_EQ(ut_get_high32(~0UL), 0xFFFFFFFFUL);
  EXPECT_EQ(ut_get_high32(0x8000000000000000UL), 0x80000000UL);
}

/* BUG: ut_2_power_up(n) for n > 2^63 (ut/ut0ut.cc:213) doubles res until it
wraps to 0 and then loops forever (res < n always). The only caller passes
small values, but the function is total on its documented domain (n > 0).
Fix:
-  while (res < n) {
+  if (n > (ulint(1) << 63)) {
+    return 0;  // or ut_error: no power of two >= n fits
+  }
+  while (res < n) {
Runs in a child with an alarm. */
TEST(UtBitsDeathTest, PowerUpOverflow) {
  EXPECT_EXIT(
    {
      alarm(2);
      ut::keep(ut_2_power_up((ulint(1) << 63) + 1));
      _exit(0);
    },
    ::testing::KilledBySignal(SIGABRT),
    ""
  );
}

/** The power of two macros. */
TEST(UtBitsTest, Macros) {
  EXPECT_TRUE(ut_is_2pow(0UL));
  EXPECT_TRUE(ut_is_2pow(1UL));
  EXPECT_TRUE(ut_is_2pow(1UL << 63));
  EXPECT_FALSE(ut_is_2pow(3UL));
  EXPECT_FALSE(ut_is_2pow(~0UL));

  EXPECT_EQ(ut_2pow_remainder(13UL, 8UL), 5UL);
  EXPECT_EQ(ut_2pow_round(13UL, 8UL), 8UL);
  EXPECT_EQ(ut_calc_align_down(16UL, 8UL), 16UL);
  EXPECT_EQ(ut_calc_align(13UL, 8UL), 16UL);
  EXPECT_EQ(ut_calc_align(16UL, 8UL), 16UL);
  EXPECT_EQ(ut_calc_align(0UL, 8UL), 0UL);
  EXPECT_EQ(UT_BITS_IN_BYTES(0), 0);
  EXPECT_EQ(UT_BITS_IN_BYTES(1), 1);
  EXPECT_EQ(UT_BITS_IN_BYTES(8), 1);
  EXPECT_EQ(UT_BITS_IN_BYTES(9), 2);
}

/** Min, max and comparisons of pairs. */
TEST(UtBitsTest, Compare) {
  EXPECT_EQ(ut_max(1, 2), 2UL);
  EXPECT_EQ(ut_max(~0UL, 0), ~0UL);
  EXPECT_EQ(ut_ulint_cmp(1, 2), -1);
  EXPECT_EQ(ut_ulint_cmp(2, 2), 0);
  EXPECT_EQ(ut_ulint_cmp(~0UL, 2), 1);
  EXPECT_EQ(ut_pair_cmp(1, 5, 1, 5), 0);
  EXPECT_EQ(ut_pair_cmp(1, 5, 2, 0), -1);
  EXPECT_EQ(ut_pair_cmp(2, 0, 1, 5), 1);
  EXPECT_EQ(ut_pair_cmp(1, 4, 1, 5), -1);
  EXPECT_EQ(ut_pair_cmp(1, 6, 1, 5), 1);

  ulint a{}, b{};

  ut_pair_min(&a, &b, 1, 9, 2, 0);
  EXPECT_EQ(a, 1UL);
  EXPECT_EQ(b, 9UL);
  ut_pair_min(&a, &b, 3, 9, 2, 7);
  EXPECT_EQ(a, 2UL);
  EXPECT_EQ(b, 7UL);
  ut_pair_min(&a, &b, 3, 9, 3, 7);
  EXPECT_EQ(a, 3UL);
  EXPECT_EQ(b, 7UL);
}

/** ut0byte.h: alignment of pointers and 64 bit numbers, bit get/set. */
TEST(UtBitsTest, Bytes) {
  alignas(64) byte buf[128];

  for (size_t i = 0; i < 64; ++i) {
    const auto p = buf + i;

    EXPECT_EQ(ut_align(p, 16), buf + ut_calc_align(i, 16UL)) << i;
    EXPECT_EQ(ut_align_down(p, 16), buf + (i & ~15UL)) << i;
    EXPECT_EQ(ut_align_offset(p, 16), i & 15) << i;
    EXPECT_EQ(ut_align(p, 1), p);
  }

  EXPECT_EQ(ut_uint64_align_down(0x1234567, 0x1000), 0x1234000ULL);
  EXPECT_EQ(ut_uint64_align_up(0x1234567, 0x1000), 0x1235000ULL);
  EXPECT_EQ(ut_uint64_align_up(0x1235000, 0x1000), 0x1235000ULL);
  EXPECT_EQ(ut_uint64_align_up(0, 8), 0ULL);
  EXPECT_EQ(ut_uint64_align_down(~0ULL, 1ULL << 40), ~0ULL << 40);

  for (ulint n = 0; n < 64; ++n) {
    const auto v = ut_bit_set_nth(0, n, true);

    EXPECT_EQ(v, ulint(1) << n);
    EXPECT_TRUE(ut_bit_get_nth(v, n));
    EXPECT_FALSE(ut_bit_get_nth(v, (n + 1) % 64));
    EXPECT_EQ(ut_bit_set_nth(~0UL, n, false), ~(ulint(1) << n));
    EXPECT_EQ(ut_bit_set_nth(v, n, true), v);
  }
}

/** Asserting that n < 64 in debug builds. */
#ifdef UNIV_DEBUG
TEST(UtBitsDeathTest, BitIndexOutOfRange) {
  EXPECT_DEATH(ut::keep(ut_bit_get_nth(1, 64)), "");
}
#endif /* UNIV_DEBUG */

/* ------------------------------------------------------------- ut0ucode */

/** The Unicode functions are ASCII stand-ins; pin what they do. */
TEST(UcodeTest, Stubs) {
  EXPECT_EQ(ib_ucode_get_connection_charset(), nullptr);
  EXPECT_EQ(ib_ucode_get_charset(33), nullptr);

  ulint mn{9}, mx{9};

  ib_ucode_get_charset_width(nullptr, &mn, &mx);
  EXPECT_EQ(mn, 0UL);
  EXPECT_EQ(mx, 0UL);

  EXPECT_EQ(ib_utf8_strcasecmp("ABC", "abc"), 0);
  EXPECT_LT(ib_utf8_strcasecmp("abc", "ABD"), 0);
  EXPECT_GT(ib_utf8_strcasecmp("b", "A"), 0);
  EXPECT_EQ(ib_utf8_strncasecmp("ABCx", "abcy", 3), 0);
  EXPECT_NE(ib_utf8_strncasecmp("ABCx", "abcy", 4), 0);
  EXPECT_EQ(ib_utf8_strncasecmp("x", "y", 0), 0);

  char s[] = "MiXeD/Case_1";

  ib_utf8_casedown(s);
  EXPECT_STREQ(s, "mixed/case_1");

  char empty[] = "";

  ib_utf8_casedown(empty);
  EXPECT_STREQ(empty, "");

  EXPECT_NE(ib_utf8_isspace(nullptr, ' '), 0);
  EXPECT_NE(ib_utf8_isspace(nullptr, '\t'), 0);
  EXPECT_EQ(ib_utf8_isspace(nullptr, 'a'), 0);

  EXPECT_EQ(ib_ucode_get_storage_size(nullptr, 10, 4, "abcd"), 4UL);
  EXPECT_EQ(ib_ucode_get_storage_size(nullptr, 3, 4, "abcd"), 3UL);
  EXPECT_EQ(ib_ucode_get_storage_size(nullptr, 0, 4, "abcd"), 0UL);

  char to[16];

  ib_utf8_convert_from_id(nullptr, to, "db/t", sizeof(to));
  EXPECT_STREQ(to, "db/t");
  ib_utf8_convert_from_table_id(nullptr, to, "db/t2", sizeof(to));
  EXPECT_STREQ(to, "db/t2");
}

/** ib_utf8_casedown() on bytes >= 0x80: tolower() of a negative char other
than EOF is undefined; in the C locale glibc leaves them alone. */
TEST(UcodeTest, CasedownHighBit) {
  char s[] = "\xc3\x89T\xc3\xa9";

  ib_utf8_casedown(s);
  EXPECT_STREQ(s, "\xc3\x89t\xc3\xa9");
}

/* ------------------------------------------------------- the hex printer */

/** buf_to_hex_string() prints the length, the bytes in hex, and as text. */
TEST(UtPrintTest, HexString) {
  const byte b[3]{0x01, 0xAB, 'A'};
  std::ostringstream os;

  buf_to_hex_string(os, b, 3);

  const auto s = os.str();

  EXPECT_NE(s.find("len: 3"), std::string::npos) << s;
  EXPECT_NE(s.find("0xab"), std::string::npos) << s;

  std::ostringstream empty;

  buf_to_hex_string(empty, b, 0);
  EXPECT_NE(empty.str().find("len: 0"), std::string::npos);
}

/* BUG: buf_to_hex_string() (ut/ut0ut.cc:181) is documented as "hex and
ascii", but prints the hex bytes with no separator ("0x10xab0x41" can't be
split back: 0x1 0xab or 0x10 0xab?) and the "asc" part as decimal numbers
(std::format("{}", byte) formats an integer), so 'A' shows as 65. Fix:
-    o << std::format("{:#x}", *data);
+    o << std::format("{:02x} ", *data);
...
-    o << std::format("{}", *data);
+    o << (std::isprint(*data) ? char(*data) : '.'); */
TEST(UtPrintTest, HexStringReadable) {
  const byte b[3]{0x01, 0xAB, 'A'};
  std::ostringstream os;

  buf_to_hex_string(os, b, 3);

  const auto s = os.str();
  const auto asc = s.substr(s.find("asc"));

  EXPECT_NE(asc.find('A'), std::string::npos) << s;
  EXPECT_EQ(s.find("0x10xab"), std::string::npos) << s;
}

/* ---------------------------------------------------------------- logger */

/** Redirect stderr to a file for the life of the object. */
struct CaptureStderr {
  CaptureStderr() {
    std::fflush(stderr);
    std::cerr.flush();
    m_saved = dup(STDERR_FILENO);
    m_path = m_dir.m_path / "stderr";

    const int fd = open(m_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);

    dup2(fd, STDERR_FILENO);
    close(fd);
  }

  ~CaptureStderr() { restore(); }

  /** @return what was written so far, and stop capturing. */
  std::string text() {
    restore();

    std::ifstream in(m_path);
    std::stringstream ss;

    ss << in.rdbuf();

    return ss.str();
  }

  void restore() {
    if (m_saved >= 0) {
      std::fflush(stderr);
      std::cerr.flush();
      dup2(m_saved, STDERR_FILENO);
      close(m_saved);
      m_saved = -1;
    }
  }

  ut::TestDir m_dir;
  fs::path m_path;
  int m_saved{-1};
};

/** Set logger::level for the life of the object. */
struct LogLevel {
  explicit LogLevel(int l) : m_saved(logger::level) { logger::level = l; }
  ~LogLevel() { logger::level = m_saved; }
  int m_saved;
};

/** Messages below the level are dropped, the others printed with a header
"<date> <time> <lvl> <progname> <file:line> <thread>" and a newline. */
TEST(LoggerTest, Levels) {
  CaptureStderr cap;

  {
    LogLevel l(logger::Warn);

    log_dbg("dropped-dbg");
    log_info("dropped-info");
    log_warn("kept-warn ", 42);
    log_err("kept-err");
  }

  {
    LogLevel l(logger::Debug);

    log_dbg("kept-dbg");
    log_info_hdr("hdr-");
    log_info_msg("msg");
    log_info_msg("\n");
  }

  const auto s = cap.text();

  EXPECT_EQ(s.find("dropped"), std::string::npos) << s;
  EXPECT_NE(s.find("kept-warn 42\n"), std::string::npos) << s;
  EXPECT_NE(s.find("kept-err\n"), std::string::npos) << s;
  EXPECT_NE(s.find("kept-dbg\n"), std::string::npos) << s;
  EXPECT_NE(s.find("hdr-msg\n"), std::string::npos) << s;

  const std::regex hdr(R"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d wrn innodb-unit-test test_ut\.cc:\d+ \d+ kept-warn 42)");

  EXPECT_TRUE(std::regex_search(s, hdr)) << s;
}

/* BUG: log_err() (include/ut0logger.h:165) passes hdr = false, so errors,
the most important messages, are the only ones printed without the
timestamp, level, file:line and thread header (log_err_hdr() has it, and
log_info()/log_warn() print it). Fix:
-      logger::write(std::move(NOW), std::move(FUNC), "err", false, true, args);
+      logger::write(std::move(NOW), std::move(FUNC), "err", true, true, args); */
TEST(LoggerTest, ErrorHasHeader) {
  CaptureStderr cap;

  log_err("an-error");

  const auto s = cap.text();

  EXPECT_TRUE(std::regex_search(s, std::regex(R"( err innodb-unit-test test_ut\.cc:\d+ \d+ an-error)"))) << s;
}

/** Lines written by many threads at once are not torn (osyncstream). */
TEST(LoggerTest, ConcurrentLinesIntact) {
  CaptureStderr cap;
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([t] {
      for (int i = 0; i < 200; ++i) {
        log_info("line-", t, "-", i, "-end");
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::istringstream in(cap.text());
  std::string line;
  int n{};
  const std::regex ok(R"(.* inf innodb-unit-test test_ut\.cc:\d+ \d+ line-\d-\d+-end)");

  while (std::getline(in, line)) {
    ASSERT_TRUE(std::regex_match(line, ok)) << line;
    ++n;
  }

  EXPECT_EQ(n, 8 * 200);
}

/** log_fatal() aborts, whatever the level, and prints at Fatal level. */
TEST(LoggerDeathTest, FatalAborts) {
  EXPECT_DEATH(log_fatal("fatal-msg ", 7), "fatal innodb-unit-test .* fatal-msg 7");
  EXPECT_DEATH(
    {
      logger::level = logger::Fatal + 1;
      log_fatal("silent");
    },
    ""
  );
}

/** func_name() keeps the base name and the line. */
TEST(LoggerTest, FuncName) {
  EXPECT_EQ(logger::func_name("/a/b/c.cc", 12), "c.cc:12");
  EXPECT_EQ(logger::func_name("a/c.cc", 1), "c.cc:1");
}

/* BUG: logger::func_name() (include/ut0logger.h:40) asserts when the only
'/' of the file name is its first character ("/c.cc"): the scan stops at
ptr == filename before looking at it. And a name without '/' (a compiler
run in the source directory gives __FILE__ "c.cc") asserts too, while it
could print the name as is. Low impact: CMake passes absolute paths. Fix:
-  while (ptr != filename && *ptr != '/') {
-    --ptr;
-  }
-  assert(ptr != filename && *ptr == '/');
-  std::string location{ptr + 1};
+  while (ptr != filename && *ptr != '/') {
+    --ptr;
+  }
+  std::string location{*ptr == '/' ? ptr + 1 : ptr}; */
TEST(LoggerDeathTest, FuncNameShortPaths) {
  /* In a child: today it dies on the assert. */
  EXPECT_EXIT(_exit(logger::func_name("/c.cc", 1) == "c.cc:1" ? 0 : 1), ::testing::ExitedWithCode(0), "");
  EXPECT_EXIT(_exit(logger::func_name("c.cc", 2) == "c.cc:2" ? 0 : 1), ::testing::ExitedWithCode(0), "");
}

/* --------------------------------------------------------------- ut0lst */

struct Elem;

/** An element on two lists at once, one of them declared before the type
is complete (the _EXTERN variant). */
struct Elem {
  explicit Elem(int v) : m_v(v) {}

  int m_v;
  UT_LIST_NODE_T(Elem) m_a;
  UT_LIST_NODE_T(Elem) m_b;
};

UT_LIST_NODE_GETTER_DEFINITION(Elem, m_b);

using ListA = UT_LIST_BASE_NODE_T(Elem, m_a);
using ListB = UT_LIST_BASE_NODE_T_EXTERN(Elem, m_b);

/** @return the values of list, front to back, checking the back links. */
template <typename List>
std::vector<int> values(List &list) {
  std::vector<int> v;

  for (auto e : list) {
    v.push_back(e->m_v);
  }

  /* Back to front agrees. */
  std::vector<int> r;

  for (auto e = UT_LIST_GET_LAST(list); e != nullptr; e = List::prev(*e)) {
    r.insert(r.begin(), e->m_v);
  }

  EXPECT_EQ(v, r);
  EXPECT_EQ(v.size(), list.size());

  if (!v.empty()) {
    EXPECT_EQ(List::prev(*UT_LIST_GET_FIRST(list)), nullptr);
    EXPECT_EQ(List::next(*UT_LIST_GET_LAST(list)), nullptr);
  }

  UT_LIST_CHECK(list);

  return v;
}

struct UtListTest : public ::testing::Test {
  void SetUp() override {
    for (int i = 0; i < 10; ++i) {
      m_elems.emplace_back(std::make_unique<Elem>(i));
    }
  }

  Elem *e(int i) { return m_elems[i].get(); }

  std::vector<std::unique_ptr<Elem>> m_elems;
  ListA m_list;
};

/** A new list is empty. */
TEST_F(UtListTest, Empty) {
  EXPECT_TRUE(m_list.empty());
  EXPECT_EQ(UT_LIST_GET_LEN(m_list), 0UL);
  EXPECT_EQ(UT_LIST_GET_FIRST(m_list), nullptr);
  EXPECT_EQ(UT_LIST_GET_LAST(m_list), nullptr);
  EXPECT_EQ(m_list.front(), nullptr);
  EXPECT_EQ(m_list.back(), nullptr);
  EXPECT_TRUE(m_list.begin() == m_list.end());
  EXPECT_TRUE(values(m_list).empty());
}

/** push_back() appends, push_front() prepends. */
TEST_F(UtListTest, PushBackFront) {
  UT_LIST_ADD_LAST(m_list, e(1));
  UT_LIST_ADD_LAST(m_list, e(2));
  UT_LIST_ADD_FIRST(m_list, e(0));
  m_list.push_back(e(3));

  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 2, 3}));
  EXPECT_EQ(m_list.front(), e(0));
  EXPECT_EQ(m_list.back(), e(3));
  EXPECT_EQ(UT_LIST_GET_NEXT(m_a, e(1)), e(2));
  EXPECT_EQ(UT_LIST_GET_PREV(m_a, e(1)), e(0));
  EXPECT_EQ(ut_list_get_len(m_list), 4UL);
}

/** A single element is both ends; push_front() on an empty list sets both. */
TEST_F(UtListTest, Single) {
  m_list.push_front(e(5));

  EXPECT_EQ(m_list.front(), e(5));
  EXPECT_EQ(m_list.back(), e(5));
  EXPECT_EQ(values(m_list), (std::vector<int>{5}));

  m_list.remove(e(5));
  EXPECT_TRUE(m_list.empty());
  EXPECT_EQ(m_list.front(), nullptr);
  EXPECT_EQ(m_list.back(), nullptr);
}

/** insert after the first, a middle and the last element. */
TEST_F(UtListTest, InsertAfter) {
  UT_LIST_ADD_LAST(m_list, e(0));
  UT_LIST_INSERT_AFTER(m_list, e(0), e(2));
  UT_LIST_INSERT_AFTER(m_list, e(0), e(1));
  UT_LIST_INSERT_AFTER(m_list, e(2), e(3));

  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 2, 3}));
  EXPECT_EQ(m_list.back(), e(3));
}

/** remove() of the first, a middle, the last element unlinks it and clears its node. */
TEST_F(UtListTest, Remove) {
  for (int i = 0; i < 5; ++i) {
    UT_LIST_ADD_LAST(m_list, e(i));
  }

  UT_LIST_REMOVE(m_list, e(2));
  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 3, 4}));
  EXPECT_EQ(e(2)->m_a.m_next, nullptr);
  EXPECT_EQ(e(2)->m_a.m_prev, nullptr);

  UT_LIST_REMOVE(m_list, e(0));
  EXPECT_EQ(values(m_list), (std::vector<int>{1, 3, 4}));
  EXPECT_EQ(m_list.front(), e(1));

  UT_LIST_REMOVE(m_list, e(4));
  EXPECT_EQ(values(m_list), (std::vector<int>{1, 3}));
  EXPECT_EQ(m_list.back(), e(3));

  /* Removed elements can go back in. */
  UT_LIST_ADD_FIRST(m_list, e(4));
  EXPECT_EQ(values(m_list), (std::vector<int>{4, 1, 3}));
}

/** remove() on an empty list asserts. */
TEST_F(UtListTest, RemoveFromEmptyAsserts) {
  EXPECT_DEATH(m_list.remove(e(0)), "");
}

/** The same elements on two lists in different orders. */
TEST_F(UtListTest, TwoLists) {
  ListB other;

  for (int i = 0; i < 5; ++i) {
    m_list.push_back(e(i));
    other.push_front(e(i));
  }

  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 2, 3, 4}));
  EXPECT_EQ(values(other), (std::vector<int>{4, 3, 2, 1, 0}));

  other.remove(e(2));
  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 2, 3, 4}));
  EXPECT_EQ(values(other), (std::vector<int>{4, 3, 1, 0}));
}

/** const iteration and ut_list_map() visit every element once. */
TEST_F(UtListTest, ConstIterateAndMap) {
  for (int i = 0; i < 7; ++i) {
    m_list.push_back(e(i));
  }

  const auto &cl = m_list;
  int sum{};

  for (const Elem *x : cl) {
    sum += x->m_v;
  }

  EXPECT_EQ(sum, 21);

  struct Sum {
    void operator()(const Elem *x) { m_sum += x->m_v; }
    int m_sum{};
  } f;

  ut_list_map(cl, f);
  EXPECT_EQ(f.m_sum, 21);
}

/** UT_LIST_CHECK() asserts when the count disagrees with the links. */
TEST_F(UtListTest, CheckDetectsBadCount) {
  m_list.push_back(e(0));
  m_list.push_back(e(1));
  m_list.count.store(3);

  EXPECT_DEATH(UT_LIST_CHECK(m_list), "");

  m_list.count.store(2);
}

/** removable(): removing the current element while iterating visits the others once. */
TEST_F(UtListTest, RemovableRemoveCurrent) {
  for (int i = 0; i < 10; ++i) {
    m_list.push_back(e(i));
  }

  std::vector<int> seen;

  for (auto x : m_list.removable()) {
    seen.push_back(x->m_v);

    if (x->m_v % 2 == 0) {
      m_list.remove(x);
    }
  }

  EXPECT_EQ(seen, (std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  EXPECT_EQ(values(m_list), (std::vector<int>{1, 3, 5, 7, 9}));
}

/** removable(): removing the current one and the ones after it. */
TEST_F(UtListTest, RemovableRemoveAhead) {
  for (int i = 0; i < 10; ++i) {
    m_list.push_back(e(i));
  }

  std::vector<int> seen;

  for (auto x : m_list.removable()) {
    seen.push_back(x->m_v);

    if (x->m_v == 3) {
      m_list.remove(e(4));
      m_list.remove(e(5));
      m_list.remove(x);
    }
  }

  EXPECT_EQ(seen, (std::vector<int>{0, 1, 2, 3, 6, 7, 8, 9}));
  EXPECT_EQ(values(m_list), (std::vector<int>{0, 1, 2, 6, 7, 8, 9}));
}

/** removable(): "move to front" of the current element is not revisited;
an element appended during the loop is visited. */
TEST_F(UtListTest, RemovableMoveToFrontAndAppend) {
  for (int i = 0; i < 5; ++i) {
    m_list.push_back(e(i));
  }

  std::vector<int> seen;

  for (auto x : m_list.removable()) {
    seen.push_back(x->m_v);

    if (x->m_v == 2) {
      m_list.remove(x);
      m_list.push_front(x);
      m_list.push_back(e(9));
    }
  }

  EXPECT_EQ(seen, (std::vector<int>{0, 1, 2, 3, 4, 9}));
  EXPECT_EQ(values(m_list), (std::vector<int>{2, 0, 1, 3, 4, 9}));
}

/** removable() on an empty list and removing everything. */
TEST_F(UtListTest, RemovableEmptyAndAll) {
  for (auto x : m_list.removable()) {
    ADD_FAILURE() << x->m_v;
  }

  for (int i = 0; i < 4; ++i) {
    m_list.push_back(e(i));
  }

  int n{};

  for (auto x : m_list.removable()) {
    m_list.remove(x);
    ++n;
  }

  EXPECT_EQ(n, 4);
  EXPECT_TRUE(m_list.empty());
  EXPECT_EQ(m_list.front(), nullptr);
}

/** clear() forgets the elements; UT_LIST_INIT re-initializes in place;
the copy constructor copies the ends and the count. */
TEST_F(UtListTest, ClearInitCopy) {
  for (int i = 0; i < 3; ++i) {
    m_list.push_back(e(i));
  }

  ListA copy(m_list);

  EXPECT_EQ(copy.size(), 3UL);
  EXPECT_EQ(copy.front(), e(0));
  EXPECT_EQ(copy.back(), e(2));

  m_list.clear();
  EXPECT_TRUE(m_list.empty());
  EXPECT_EQ(m_list.front(), nullptr);

  UT_LIST_INIT(copy);
  EXPECT_TRUE(copy.empty());
  EXPECT_EQ(UT_LIST_GET_FIRST(copy), nullptr);
}

/** A list in memory that was not constructed is caught in debug builds. */
#ifdef UNIV_DEBUG
TEST_F(UtListTest, UninitialisedAsserts) {
  alignas(ListA) byte mem[sizeof(ListA)];

  std::memset(mem, 0, sizeof(mem));

  auto list = reinterpret_cast<ListA *>(mem);

  EXPECT_DEATH(list->push_back(e(0)), "");

  UT_LIST_INIT(*list);
  list->push_back(e(0));
  EXPECT_EQ(list->size(), 1UL);
}
#endif /* UNIV_DEBUG */

/* BUG: ut_list_reverse() (include/ut0lst.h:636) and ut_list_move_to_front()
(include/ut0lst.h:686) use list.first_element, a member UtListBase doesn't
have (m_first_element): any use fails to compile. They are templates that
nothing instantiates, so the build doesn't see it. ut_list_move_to_front()
also calls ut_list_exists() in ut_ad(), defined only under UNIV_DEBUG,
which is fine, but needs the fix too:
-  for (auto elem = list.first_element; elem != nullptr; elem = List::prev(*elem)) {
+  for (auto elem = list.m_first_element; elem != nullptr; elem = List::prev(*elem)) {
...
-  if (list.first_element != elem) {
+  if (list.m_first_element != elem) {
Fixed. */
TEST_F(UtListTest, ReverseAndMoveToFront) {
  for (int i = 0; i < 4; ++i) {
    m_list.push_back(e(i));
  }

  UT_LIST_REVERSE(m_list);
  EXPECT_EQ(values(m_list), (std::vector<int>{3, 2, 1, 0}));

  ut_list_move_to_front(m_list, e(1));
  EXPECT_EQ(values(m_list), (std::vector<int>{1, 3, 2, 0}));
}

/** The count can be read without the latch while a writer changes the list
under it: the reader sees only counts the list had. */
TEST_F(UtListTest, UnlatchedSizeRead) {
  std::mutex latch;
  std::atomic<bool> done{};
  std::atomic<bool> bad{};

  std::thread reader([&] {
    while (!done.load()) {
      const auto n = m_list.size();

      if (n > m_elems.size()) {
        bad = true;
      }
    }
  });

  for (int round = 0; round < 20000; ++round) {
    std::lock_guard<std::mutex> g(latch);

    if (round % 2 == 0) {
      for (size_t i = 0; i < m_elems.size(); ++i) {
        m_list.push_back(e(int(i)));
      }
    } else {
      for (size_t i = 0; i < m_elems.size(); ++i) {
        m_list.remove(e(int(i)));
      }
    }
  }

  done = true;
  reader.join();

  EXPECT_FALSE(bad);
  EXPECT_TRUE(m_list.empty());
}

/** Many threads change one list under the caller's latch: no element is
lost or duplicated. */
TEST(UtListMtTest, LatchedConcurrentChanges) {
  constexpr int N_THREADS = 8;
  constexpr int N_PER = 2000;
  std::vector<std::unique_ptr<Elem>> elems;

  for (int i = 0; i < N_THREADS * N_PER; ++i) {
    elems.emplace_back(std::make_unique<Elem>(i));
  }

  ListA list;
  std::mutex latch;
  std::vector<std::thread> threads;

  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < N_PER; ++i) {
        auto x = elems[t * N_PER + i].get();
        std::lock_guard<std::mutex> g(latch);

        if (i % 3 == 0) {
          list.push_front(x);
        } else {
          list.push_back(x);
        }

        /* Take every other one out again. */
        if (i % 2 == 1) {
          list.remove(x);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(list.size(), size_t(N_THREADS * N_PER / 2));

  std::set<int> seen;

  for (auto x : list) {
    EXPECT_EQ(x->m_v % 2, 0);
    seen.insert(x->m_v);
  }

  EXPECT_EQ(seen.size(), list.size());
  UT_LIST_CHECK(list);
}

}  // namespace

/* ------------------------------------------------------------- perf */

namespace {

struct CrcPerf : public ::testing::Test {};

/** CRC-32C GB/s of every implementation, at 64 B, 1 KB, 16 KB. */
TEST_F(CrcPerf, Implementations) {
  const auto data = random_bytes(UNIV_PAGE_SIZE + 8, 20);

  for (const size_t len : {64UL, 1024UL, 16384UL}) {
    const uint64_t n = (64UL << 20) / len;

    for (const auto &impl : impls()) {
      uint32_t acc{};
      auto r = ut::bench(std::format("crc32c_{}_{}", impl.m_name, len), impl.m_name == std::string("software") ? n / 8 : n,
                         [&](uint64_t) { acc ^= impl.m_fn(data.data(), len); });

      ut::keep(acc);

      const double gbs = double(len) / r.ns_per_op();

      std::cout << std::format("PERF crc32c {:>24} {:>6} B {:8.2f} GB/s\n", impl.m_name, len, gbs);

      /* Generous: the table driven one does ~0.3 GB/s at -O0. */
      EXPECT_GT(gbs, 0.01) << impl.m_name << " " << len;
    }

    uint32_t acc{};
    auto r = ut::bench(std::format("crc32c_checksum_{}", len), n, [&](uint64_t) { acc ^= crc32::checksum(data.data(), len); });

    ut::keep(acc);
    std::cout << std::format("PERF crc32c {:>24} {:>6} B {:8.2f} GB/s\n", "crc32::checksum", len, double(len) / r.ns_per_op());
  }
}

/** crc32::checksum runs at the speed of the hardware version, not the
software one, on a CPU that has SSE4.2. */
TEST_F(CrcPerf, ChecksumIsFast) {
#if defined(__x86_64__)
  if (!crc32::can_use_crc32()) {
    GTEST_SKIP() << "no SSE4.2";
  }

  const auto data = random_bytes(UNIV_PAGE_SIZE, 21);
  uint32_t acc{};
  const auto sw = ut::bench("crc32c_sw_16k", 2000, [&](uint64_t) { acc ^= crc32::software(data.data(), data.size()); });
  const auto hw = ut::bench("crc32c_cs_16k", 20000, [&](uint64_t) { acc ^= crc32::checksum(data.data(), data.size()); });

  ut::keep(acc);

  EXPECT_LT(hw.ns_per_op() * 2, sw.ns_per_op());
#else
  GTEST_SKIP();
#endif /* __x86_64__ */
}

struct PageChecksumPerf : public ::testing::Test {};

/** buf_page_data_calc_checksum() ns per 16 KB page. */
TEST_F(PageChecksumPerf, Page) {
  Page p(22);
  uint32_t acc{};
  auto r = ut::bench("page_checksum", 50000, [&](uint64_t i) {
    p.m_page[100] = byte(i);
    acc ^= buf_page_data_calc_checksum(p.m_page);
  });

  ut::keep(acc);
  EXPECT_LT(r.ns_per_op(), 200000);
}

struct FoldPerf : public ::testing::Test {};

/** The folds and the hash. */
TEST_F(FoldPerf, Folds) {
  ulint acc{};
  const char *name = "database/table_1";
  const auto bin = random_bytes(64, 23);

  auto r1 = ut::bench("fold_ulint_pair", 10000000, [&](uint64_t i) { acc ^= ut_fold_ulint_pair(acc, i); });
  auto r2 = ut::bench("fold_string_16", 1000000, [&](uint64_t) {
    ut::keep(name);
    acc ^= ut_fold_string(name);
  });
  auto r3 = ut::bench("fold_binary_64", 1000000, [&](uint64_t) {
    ut::keep(bin);
    acc ^= ut_fold_binary(bin.data(), bin.size());
  });
  auto r4 = ut::bench("hash_ulint", 10000000, [&](uint64_t i) { acc ^= ut_hash_ulint(i, 1000003); });
  auto r5 = ut::bench("rnd_gen_ulint", 10000000, [&](uint64_t) { acc ^= ut_rnd_gen_ulint(); });

  ut::keep(acc);

  EXPECT_LT(r1.ns_per_op(), 100);
  EXPECT_LT(r2.ns_per_op(), 2000);
  EXPECT_LT(r3.ns_per_op(), 5000);
  EXPECT_LT(r4.ns_per_op(), 100);
  EXPECT_LT(r5.ns_per_op(), 200);
}

struct UtMemPerf : public ::testing::Test {};

/** ut_new()/ut_delete() against malloc()/free(): ut_new() is calloc(), it
zeroes every block, which shows at 16 KB. */
TEST_F(UtMemPerf, NewDelete) {
  for (const ulint n : {64UL, 16384UL}) {
    auto r1 = ut::bench(std::format("ut_new_delete_{}", n), 1000000, [&](uint64_t) {
      auto p = ut_new(n);
      ut::keep(p);
      ut_delete(p);
    });
    auto r2 = ut::bench(std::format("malloc_free_{}", n), 1000000, [&](uint64_t) {
      auto p = std::malloc(n);
      ut::keep(p);
      std::free(p);
    });

    EXPECT_LT(r1.ns_per_op(), 20000);
    EXPECT_LT(r2.ns_per_op(), 20000);
  }
}

struct UtListPerf : public ::testing::Test {};

/** push_back + remove of one element. */
TEST_F(UtListPerf, PushRemove) {
  std::vector<std::unique_ptr<Elem>> elems;

  for (int i = 0; i < 1024; ++i) {
    elems.emplace_back(std::make_unique<Elem>(i));
  }

  ListA list;

  for (int i = 0; i < 512; ++i) {
    list.push_back(elems[i].get());
  }

  auto r = ut::bench("ut_list_push_remove", 5000000, [&](uint64_t i) {
    auto x = elems[512 + i % 512].get();
    list.push_back(x);
    list.remove(x);
  });

  EXPECT_LT(r.ns_per_op(), 2000);
}

}  // namespace
