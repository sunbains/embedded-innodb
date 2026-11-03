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

/** @file unit-tests/test_mem.cc
Memory heaps: a stack of blocks that allocations are carved from, freed at
the top or all at once; and mem_alloc()/mem_free(), a heap per buffer. */

#include "ut0env.h"
#include "ut0perf.h"

#include "mem0mem.h"

#include <cstring>
#include <random>
#include <set>
#include <vector>

namespace {

/** @return the number of blocks of heap. */
ulint n_blocks(MemHeap *heap) {
  ulint n{};

  for (auto block = UT_LIST_GET_FIRST(heap->base); block != nullptr; block = UT_LIST_GET_NEXT(list, block)) {
    ++n;
  }

  return n;
}

/** @return the sum of the lengths of the blocks of heap. */
ulint sum_len(MemHeap *heap) {
  ulint n{};

  for (auto block = UT_LIST_GET_FIRST(heap->base); block != nullptr; block = UT_LIST_GET_NEXT(list, block)) {
    n += block->len;
  }

  return n;
}

/** mem_heap_create(0) makes a block of the default start size; the heap
is its own first block. */
TEST(MemHeapTest, CreateDefault) {
  auto heap = mem_heap_create(0);

  EXPECT_EQ(n_blocks(heap), 1UL);
  EXPECT_EQ(UT_LIST_GET_FIRST(heap->base), heap);
  EXPECT_EQ(heap->len, mem_block_header_size() + MEM_BLOCK_START_SIZE);
  EXPECT_EQ(mem_heap_get_size(heap), heap->len);
  EXPECT_EQ(heap->free, mem_block_header_size());
  EXPECT_EQ(heap->start, heap->free);
  EXPECT_EQ(heap->type, MEM_HEAP_DYNAMIC);

  mem_heap_free(heap);
}

/** A heap created for n bytes takes one allocation of n without a new block. */
TEST(MemHeapTest, CreateSized) {
  for (ulint n : {1UL, 7UL, 8UL, 100UL, 1000UL, 100000UL}) {
    auto heap = mem_heap_create(n);
    auto p = mem_heap_alloc(heap, n);

    EXPECT_EQ(n_blocks(heap), 1UL) << n;
    EXPECT_EQ(p, reinterpret_cast<byte *>(heap) + mem_block_header_size()) << n;

    memset(p, 0x5A, n);

    mem_heap_free(heap);
  }
}

/** Allocations are aligned, disjoint, and keep what was written. */
TEST(MemHeapTest, AlignedDisjoint) {
  auto heap = mem_heap_create(0);
  std::vector<std::pair<byte *, ulint>> allocs;

  for (ulint i = 0; i < 2000; ++i) {
    const ulint n = 1 + (i * 131) % 700;
    auto p = mem_heap_alloc(heap, n);

    ASSERT_NE(p, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % UNIV_MEM_ALIGNMENT, 0UL) << i;

    memset(p, int(i & 0xFF), n);
    allocs.emplace_back(p, n);
  }

  std::vector<std::pair<byte *, ulint>> sorted(allocs);

  std::sort(sorted.begin(), sorted.end());

  for (size_t i = 1; i < sorted.size(); ++i) {
    EXPECT_LE(sorted[i - 1].first + sorted[i - 1].second, sorted[i].first);
  }

  for (size_t i = 0; i < allocs.size(); ++i) {
    const auto [p, n] = allocs[i];

    for (ulint j = 0; j < n; ++j) {
      ASSERT_EQ(p[j], byte(i & 0xFF)) << i;
    }
  }

  mem_heap_free(heap);
}

/** Each new block is twice the previous one, up to the standard size;
mem_heap_get_size() is the sum of the block lengths. */
TEST(MemHeapTest, BlockGrowth) {
  auto heap = mem_heap_create(0);
  ulint prev_len = heap->len;

  for (int i = 0; i < 2000; ++i) {
    (void)mem_heap_alloc(heap, 24);
  }

  ASSERT_GT(n_blocks(heap), 3UL);
  EXPECT_EQ(mem_heap_get_size(heap), sum_len(heap));

  auto block = UT_LIST_GET_NEXT(list, UT_LIST_GET_FIRST(heap->base));

  for (; block != nullptr; block = UT_LIST_GET_NEXT(list, block)) {
    const auto expected = std::min(2 * prev_len, mem_block_standard_size());

    EXPECT_EQ(block->len, mem_block_header_size() + MEM_SPACE_NEEDED(expected));
    prev_len = block->len;
  }

  mem_heap_free(heap);
}

/** An allocation bigger than the standard block size gets a block of its own size. */
TEST(MemHeapTest, BigAlloc) {
  auto heap = mem_heap_create(0);
  const ulint n = 10 * mem_block_standard_size();
  auto p = mem_heap_alloc(heap, n);

  memset(p, 1, n);

  EXPECT_EQ(n_blocks(heap), 2UL);
  EXPECT_EQ(UT_LIST_GET_LAST(heap->base)->len, mem_block_header_size() + n);

  mem_heap_free(heap);
}

/** zalloc returns zeroed memory, even reusing space that held data. */
TEST(MemHeapTest, Zalloc) {
  auto heap = mem_heap_create(0);
  auto top = static_cast<byte *>(mem_heap_get_top(heap, 0));
  auto p = mem_heap_alloc(heap, 40);

  memset(p, 0xFF, 40);
  mem_heap_free_heap_top(heap, top);

  auto z = mem_heap_zalloc(heap, 40);

  EXPECT_EQ(z, p);

  for (int i = 0; i < 40; ++i) {
    EXPECT_EQ(z[i], 0);
  }

  mem_heap_free(heap);
}

/** get_top(n) is the last allocation of n bytes, free_top(n) gives it back
and the next allocation reuses the space. */
TEST(MemHeapTest, Top) {
  auto heap = mem_heap_create(0);

  (void)mem_heap_alloc(heap, 16);

  auto p = mem_heap_alloc(heap, 20);

  EXPECT_EQ(mem_heap_get_top(heap, 20), p);

  const auto free_before = UT_LIST_GET_LAST(heap->base)->free;

  mem_heap_free_top(heap, 20);

  EXPECT_EQ(UT_LIST_GET_LAST(heap->base)->free, free_before - MEM_SPACE_NEEDED(20));
  EXPECT_EQ(mem_heap_alloc(heap, 20), p);

  mem_heap_free(heap);
}

/** free_top() of the only allocation in a later block frees the block. */
TEST(MemHeapTest, FreeTopFreesBlock) {
  auto heap = mem_heap_create(0);

  (void)mem_heap_alloc(heap, MEM_BLOCK_START_SIZE);
  ASSERT_EQ(n_blocks(heap), 1UL);

  (void)mem_heap_alloc(heap, 64);
  ASSERT_EQ(n_blocks(heap), 2UL);

  const auto size = mem_heap_get_size(heap);

  mem_heap_free_top(heap, 64);

  EXPECT_EQ(n_blocks(heap), 1UL);
  EXPECT_LT(mem_heap_get_size(heap), size);
  EXPECT_EQ(mem_heap_get_size(heap), heap->len);

  mem_heap_free(heap);
}

/** free_heap_top() back to a top in the first block frees all the later
blocks and makes the top the next allocation. */
TEST(MemHeapTest, FreeHeapTop) {
  auto heap = mem_heap_create(0);

  (void)mem_heap_alloc(heap, 8);

  auto top = static_cast<byte *>(mem_heap_get_top(heap, 0));

  for (int i = 0; i < 500; ++i) {
    (void)mem_heap_alloc(heap, 100);
  }

  ASSERT_GT(n_blocks(heap), 2UL);

  mem_heap_free_heap_top(heap, top);

  EXPECT_EQ(n_blocks(heap), 1UL);
  EXPECT_EQ(mem_heap_get_size(heap), heap->len);
  EXPECT_EQ(mem_heap_alloc(heap, 8), top);

  mem_heap_free(heap);
}

/** free_heap_top() to a top inside a middle block keeps the blocks up to it. */
TEST(MemHeapTest, FreeHeapTopMiddle) {
  auto heap = mem_heap_create(0);

  for (int i = 0; i < 20; ++i) {
    (void)mem_heap_alloc(heap, 100);
  }

  const auto blocks = n_blocks(heap);
  auto top = static_cast<byte *>(mem_heap_get_top(heap, 0));

  /* The top is the free pointer of the last block: allocate on past it. */
  for (int i = 0; i < 300; ++i) {
    (void)mem_heap_alloc(heap, 100);
  }

  ASSERT_GT(n_blocks(heap), blocks);

  mem_heap_free_heap_top(heap, top);

  EXPECT_EQ(n_blocks(heap), blocks);
  EXPECT_EQ(mem_heap_get_size(heap), sum_len(heap));
  EXPECT_EQ(mem_heap_alloc(heap, 8), top);

  mem_heap_free(heap);
}

/** mem_heap_empty() keeps only the first block, empty. */
TEST(MemHeapTest, Empty) {
  auto heap = mem_heap_create(0);

  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 300; ++i) {
      (void)mem_heap_alloc(heap, 50);
    }

    mem_heap_empty(heap);

    EXPECT_EQ(n_blocks(heap), 1UL);
    EXPECT_EQ(heap->free, heap->start);
    EXPECT_EQ(mem_heap_get_size(heap), heap->len);
  }

  mem_heap_free(heap);
}

/** The string helpers copy into the heap. */
TEST(MemHeapTest, Strings) {
  auto heap = mem_heap_create(0);

  const char *s = "hello";
  auto d = mem_heap_strdup(heap, s);

  EXPECT_STREQ(d, s);
  EXPECT_NE(d, s);

  EXPECT_STREQ(mem_heap_strdupl(heap, "abcdef", 3), "abc");
  EXPECT_STREQ(mem_heap_strdupl(heap, "", 0), "");
  EXPECT_STREQ(mem_heap_strcat(heap, "foo", "bar"), "foobar");
  EXPECT_STREQ(mem_heap_strcat(heap, "", ""), "");

  const byte data[] = {0, 1, 2, 0, 3};
  auto dup = static_cast<byte *>(mem_heap_dup(heap, data, sizeof(data)));

  EXPECT_EQ(memcmp(dup, data, sizeof(data)), 0);

  /* A string longer than the start block. */
  const std::string big(5000, 'q');

  EXPECT_EQ(std::string(mem_heap_strdup(heap, big.c_str())), big);

  mem_heap_free(heap);
}

/** mem_heap_printf() supports %s, %lu and %%. */
TEST(MemHeapTest, Printf) {
  auto heap = mem_heap_create(0);

  EXPECT_STREQ(mem_heap_printf(heap, "a%sb%luc%%", "XY", 1234567890123UL), "aXYb1234567890123c%");
  EXPECT_STREQ(mem_heap_printf(heap, "%s", ""), "");
  EXPECT_STREQ(mem_heap_printf(heap, "plain"), "plain");
  EXPECT_STREQ(mem_heap_printf(heap, "%lu", 0UL), "0");

  mem_heap_free(heap);
}

/** mem_alloc()/mem_free(): a heap of one buffer just after the header. */
TEST(MemAllocTest, AllocFree) {
  for (ulint n : {1UL, 8UL, 1000UL, 1UL << 20}) {
    auto p = static_cast<byte *>(mem_alloc(n));
    auto heap = reinterpret_cast<MemHeap *>(p - mem_block_header_size());

    EXPECT_EQ(heap->magic_n, MEM_BLOCK_MAGIC_N);
    memset(p, 0x11, n);
    mem_free(p);
  }
}

/** mem_alloc2() returns the usable size of the block, at least what was asked. */
TEST(MemAllocTest, Alloc2) {
  for (ulint n : {1UL, 9UL, 100UL}) {
    ulint size{};
    auto p = static_cast<byte *>(mem_alloc2(n, &size));

    EXPECT_GE(size, n);
    EXPECT_EQ(size, MEM_SPACE_NEEDED(n));
    memset(p, 0x22, size);
    mem_free(p);
  }
}

/** mem_zalloc(), mem_strdup(), mem_strdupl(). */
TEST(MemAllocTest, Helpers) {
  auto z = static_cast<byte *>(mem_zalloc(100));

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(z[i], 0);
  }

  mem_free(z);

  auto s = mem_strdup("embedded");

  EXPECT_STREQ(s, "embedded");
  mem_free(s);

  auto t = mem_strdupl("embedded", 3);

  EXPECT_STREQ(t, "emb");
  mem_free(t);
}

/** A heap that may grow into the buffer pool: no block is bigger than a page. */
TEST(MemHeapTest, BufferHeap) {
  auto heap = mem_heap_create_in_buffer(0);

  EXPECT_EQ(heap->type, MEM_HEAP_BUFFER);

  for (int i = 0; i < 100; ++i) {
    (void)mem_heap_alloc(heap, MEM_MAX_ALLOC_IN_BUF / 3);
  }

  (void)mem_heap_alloc(heap, MEM_MAX_ALLOC_IN_BUF);

  for (auto block = UT_LIST_GET_FIRST(heap->base); block != nullptr; block = UT_LIST_GET_NEXT(list, block)) {
    EXPECT_LE(block->len, UNIV_PAGE_SIZE);
  }

#ifdef UNIV_DEBUG
  EXPECT_TRUE(mem_heap_validate(heap));
#endif /* UNIV_DEBUG */

  mem_heap_free(heap);
}

/** A random mix of allocs, free_top and free_heap_top against a model of the
live allocations: their contents survive the operations on the others. */
TEST(MemHeapTest, RandomModel) {
  auto heap = mem_heap_create(0);
  std::mt19937 rng(5);

  struct Alloc {
    byte *m_ptr;
    ulint m_len;
    byte m_fill;
  };

  std::vector<Alloc> live;

  for (int i = 0; i < 20000; ++i) {
    const auto op = rng() % 10;

    if (op < 7 || live.empty()) {
      const ulint n = 1 + rng() % (rng() % 20 == 0 ? 20000 : 200);
      auto p = mem_heap_alloc(heap, n);
      const auto fill = byte(rng());

      memset(p, fill, n);
      live.push_back({p, n, fill});
    } else if (op < 9) {
      mem_heap_free_top(heap, live.back().m_len);
      live.pop_back();
    } else {
      const auto keep = rng() % live.size();
      auto top = live[keep].m_ptr;

      mem_heap_free_heap_top(heap, top);
      live.resize(keep);
    }

    if (i % 1000 == 0) {
      for (const auto &a : live) {
        for (ulint j = 0; j < a.m_len; j += 7) {
          ASSERT_EQ(a.m_ptr[j], a.m_fill);
        }
      }

      ASSERT_EQ(mem_heap_get_size(heap), sum_len(heap));
    }
  }

  mem_heap_free(heap);
}

}  // namespace

namespace {

/** Small allocations from a heap, emptied every 1024. */
TEST(MemPerf, HeapAlloc) {
  auto heap = mem_heap_create(0);

  auto r = ut::bench("heap_alloc_32", 20000000, [&](uint64_t i) {
    ut::keep(mem_heap_alloc(heap, 32));

    if ((i & 1023) == 1023) {
      mem_heap_empty(heap);
    }
  });

  mem_heap_free(heap);
  EXPECT_LT(r.ns_per_op(), 1000.0);
}

/** Create, one allocation, free: the per-row heap pattern. */
TEST(MemPerf, HeapCreateFree) {
  auto r = ut::bench("heap_create_alloc_free", 5000000, [&](uint64_t) {
    auto heap = mem_heap_create(256);

    ut::keep(mem_heap_alloc(heap, 100));
    mem_heap_free(heap);
  });

  EXPECT_LT(r.ns_per_op(), 5000.0);
}

/** mem_alloc()/mem_free() against malloc()/free(). */
TEST(MemPerf, AllocFree) {
  auto r = ut::bench("mem_alloc_free_200", 5000000, [&](uint64_t) {
    auto p = mem_alloc(200);

    ut::keep(p);
    mem_free(p);
  });

  auto m = ut::bench("malloc_free_200", 5000000, [&](uint64_t) {
    auto p = malloc(200);

    ut::keep(p);
    free(p);
  });

  EXPECT_LT(r.ns_per_op(), 10 * m.ns_per_op() + 1000.0);
}

/** Multi-threaded heap create/alloc/free: each thread its own heap, the
malloc underneath is shared. */
TEST(MemPerf, HeapThreads) {
  for (int n_threads : {1, 2, 4, 8, 16}) {
    auto r = ut::bench_mt(std::format("heap_create_alloc_free_t{}", n_threads), n_threads, 500000, [&](int, uint64_t) {
      auto heap = mem_heap_create(0);

      for (int i = 0; i < 4; ++i) {
        ut::keep(mem_heap_alloc(heap, 64));
      }

      mem_heap_free(heap);
    });

    EXPECT_LT(r.ns_per_op(), 50000.0);
  }
}

}  // namespace
