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

/** @file unit-tests/test_blink.cc
The B-link tree, idx0blink.h: through IndexAm, and the pages looked at directly. */

#include "test_blink.h"

namespace {

using namespace std::chrono_literals;


/** A new tree is a leaf root. */
TEST_F(Blink, Empty) {
  const auto shape = validated();

  EXPECT_EQ(shape.m_height, 0UL);
  EXPECT_TRUE(shape.m_keys.empty());
  EXPECT_FALSE(has(1, m_heap));
}

TEST_F(Blink, OneRec) {
  put_all(7, 7);

  const auto shape = validated();

  EXPECT_EQ(shape.m_keys, std::vector<uint32_t>{7});
  EXPECT_TRUE(has(7, m_heap));
  EXPECT_FALSE(has(6, m_heap));
  EXPECT_FALSE(has(8, m_heap));
}

/** The root keeps its number and gets a level, its records are on a page below. */
TEST_F(Blink, RootRaise) {
  const auto root_no = m_index->get_page_no();

  for (uint32_t k{1};; ++k) {
    ASSERT_EQ(put(k, 400, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);

    if (validated().m_height > 0) {
      break;
    }
  }

  const auto shape = validated();

  EXPECT_EQ(m_index->get_page_no(), root_no);
  EXPECT_EQ(shape.m_height, 1UL);
  EXPECT_EQ(shape.m_levels[1], std::vector<page_no_t>{root_no});
  EXPECT_EQ(shape.m_levels[0].size(), 2UL);

  on_page(root_no, [&](page_t *page, Mtr *) {
    EXPECT_TRUE(page_get_high_key(page).is_null());
    EXPECT_EQ(page_get_n_recs(page), 2UL);
    EXPECT_NE(page_rec_get_next(page_get_infimum_rec(page)).get_info_bits() & REC_INFO_MIN_REC_FLAG, 0UL);
  });
}

/** After a split: the left page's high key is the first key of the right page, and that key
goes up as the separator; the rightmost page has none. */
TEST_F(Blink, HighKeys) {
  uint32_t k{1};

  while (validated().m_height == 0) {
    ASSERT_EQ(put(k++, 400, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  const auto shape = validated();
  const auto left_no = shape.m_levels[0][0];
  const auto right_no = shape.m_levels[0][1];
  uint32_t high{};
  uint32_t first{};

  on_page(left_no, [&](page_t *page, Mtr *) {
    const auto high_key = page_get_high_key(page);

    ASSERT_FALSE(high_key.is_null());
    high = key_of(m_index, high_key);

    /* The bytes of the key are in the data size, not the records'. */
    EXPECT_EQ(page_get_data_size(page), page_get_rec_data_size(page) + page_high_key_size(high_key));
  });

  on_page(right_no, [&](page_t *page, Mtr *) {
    EXPECT_TRUE(page_get_high_key(page).is_null());
    first = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page)));
  });

  EXPECT_EQ(high, first);

  on_page(m_index->get_page_no(), [&](page_t *page, Mtr *) {
    auto second = page_rec_get_next(page_rec_get_next(page_get_infimum_rec(page)));

    EXPECT_EQ(key_of(m_index, second), first);
  });
}

/** A page keeps the key that it got, whatever happens to the records. */
TEST_F(Blink, HighKeyFixed) {
  put_all(1, 200, 400);

  const auto shape = validated();

  ASSERT_GT(shape.m_levels[0].size(), 2UL);

  const auto page_no = shape.m_levels[0][0];
  uint32_t high{};

  on_page(page_no, [&](page_t *page, Mtr *) { high = key_of(m_index, page_get_high_key(page)); });

  for (uint32_t k{1}; k < high; ++k) {
    EXPECT_TRUE(del(k, m_heap)) << k;
    mem_heap_empty(m_heap);
  }

  on_page(page_no, [&](page_t *page, Mtr *) {
    EXPECT_EQ(page_get_n_recs(page), 0UL);
    EXPECT_EQ(key_of(m_index, page_get_high_key(page)), high);
  });

  (void)validated();
}

/** The new right sibling takes the old high key. */
TEST_F(Blink, HighKeyInherited) {
  put_all(1, 100, 400);

  /* Split the first leaf only, by inserts in a gap of it: numbers above the existing ones
  would go right. */
  const auto before = validated();
  const auto page_no = before.m_levels[0][0];
  uint32_t high{};

  on_page(page_no, [&](page_t *page, Mtr *) { high = key_of(m_index, page_get_high_key(page)); });

  const auto n_recs = high - 1;

  /* The same keys grown: the page splits, nothing of the upper part is touched. */
  for (uint32_t k{1}; k <= n_recs; ++k) {
    ASSERT_TRUE(del(k, m_heap));
    ASSERT_EQ(put(k, 1200, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_GT(after.m_levels[0].size(), before.m_levels[0].size());

  /* The rightmost of what came from the page ends where the page ended. */
  auto it = std::find(after.m_levels[0].begin(), after.m_levels[0].end(), before.m_levels[0][1]);

  ASSERT_NE(it, after.m_levels[0].end());
  ASSERT_NE(it, after.m_levels[0].begin());

  on_page(*(it - 1), [&](page_t *page, Mtr *) { EXPECT_EQ(key_of(m_index, page_get_high_key(page)), high); });
}

/** Right links and left links agree on all the levels, with many splits. */
TEST_F(Blink, Links) {
  put_all(1, 3000, 300);

  const auto shape = validated();

  EXPECT_GE(shape.m_height, 1UL);
  EXPECT_EQ(shape.m_keys.size(), 3000UL);

  for (uint32_t k{1}; k <= 3000; ++k) {
    EXPECT_EQ(shape.m_keys[k - 1], k);
  }
}

/** Levels above the first: splits of parents and the root, in any insert order. */
TEST_F(Blink, MultiLevel) {
  std::vector<uint32_t> keys(20000);

  for (uint32_t i{}; i < keys.size(); ++i) {
    keys[i] = i + 1;
  }

  std::mt19937 rng(42);

  std::shuffle(keys.begin(), keys.end(), rng);

  for (const auto k : keys) {
    ASSERT_EQ(put(k, 600, m_heap), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  const auto shape = validated();

  EXPECT_GE(shape.m_height, 2UL);
  EXPECT_EQ(shape.m_keys.size(), keys.size());

  for (uint32_t i{}; i < shape.m_keys.size(); ++i) {
    EXPECT_EQ(shape.m_keys[i], i + 1);
  }
}

/** Descending inserts: each split leaves the new record on the left. */
TEST_F(Blink, Descending) {
  for (uint32_t k{5000}; k > 0; --k) {
    ASSERT_EQ(put(k, 200, m_heap), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  const auto shape = validated();

  EXPECT_EQ(shape.m_keys.size(), 5000UL);
  EXPECT_TRUE(std::is_sorted(shape.m_keys.begin(), shape.m_keys.end()));
}

/** A reader with a page number from before the splits gets right by the links: read_node()
tells it to go right from a stale parent, and then the way down. */
TEST_F(Blink, StaleParent) {
  put_all(1, 40000, 600);

  const auto before = validated();

  ASSERT_GE(before.m_height, 2UL);

  /* The parent of the first leaves, as a reader saw it. */
  const auto stale = before.m_levels[1][0];

  put_all(40001, 90000, 600);

  (void)validated();

  const uint32_t k{89000};
  auto page_no = stale;
  ulint n_right{};

  for (;;) {
    const auto step = blink::read_node(*m_sys, m_index, page_no, key(k, m_heap), PAGE_CUR_LE, blink::Edge::KEY, 0);

    if (step.m_kind == blink::NodeStep::STOP) {
      break;
    }

    n_right += step.m_kind == blink::NodeStep::RIGHT;
    page_no = step.m_page;

    if (step.m_kind == blink::NodeStep::CHILD && step.m_level == 1) {
      break;
    }
  }

  EXPECT_GT(n_right, 0UL);

  on_page(page_no, [&](page_t *page, Mtr *mtr) {
    EXPECT_EQ(m_sys->m_btree->page_get_level(page, mtr), 0UL);

    const auto high_key = page_get_high_key(page);

    if (!high_key.is_null()) {
      EXPECT_LT(k, key_of(m_index, high_key));
    }
  });
}

/** read_node(): the parts of its answer, and that it holds nothing on return. */
TEST_F(Blink, ReadNode) {
  put_all(1, 2000, 300);

  const auto shape = validated();
  const auto root_no = m_index->get_page_no();

  const auto stop = blink::read_node(*m_sys, m_index, root_no, key(1, m_heap), PAGE_CUR_LE, blink::Edge::KEY, shape.m_height);

  EXPECT_EQ(stop.m_kind, blink::NodeStep::STOP);
  EXPECT_EQ(stop.m_page, root_no);
  EXPECT_EQ(stop.m_level, shape.m_height);

  const auto first = blink::read_node(*m_sys, m_index, root_no, nullptr, 0, blink::Edge::FIRST, 0);
  const auto last = blink::read_node(*m_sys, m_index, root_no, nullptr, 0, blink::Edge::LAST, 0);

  EXPECT_EQ(first.m_kind, blink::NodeStep::CHILD);
  EXPECT_EQ(last.m_kind, blink::NodeStep::CHILD);
  EXPECT_NE(first.m_page, last.m_page);

  /* Nothing held: an X latch on what it read comes at once in the same thread. */
  Mtr mtr;

  mtr.start();
  (void)m_sys->m_btree->block_get(m_index->get_space_id(), root_no, RW_X_LATCH, &mtr);
  mtr.commit(*m_sys);
}

/** Find by key in all of the modes. */
TEST_F(Blink, Modes) {
  for (uint32_t k{2}; k <= 4000; k += 2) {
    ASSERT_EQ(put(k, 200, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  ASSERT_GE(validated().m_height, 1UL);

  const auto at = [&](uint32_t k, SearchMode mode) -> int64_t {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.position(m_index, key(k, m_heap), mode, Intent::Read, &mtr);

    /* Not on a record means the end of a page: the answer is on the next or before. */
    if (mode == PAGE_CUR_GE || mode == PAGE_CUR_G) {
      if (!cur.on_rec() && !cur.next(&mtr)) {
        cur.commit(&mtr);
        return -1;
      }
    } else {
      if (!cur.on_rec() && !cur.prev(&mtr)) {
        cur.commit(&mtr);
        return -1;
      }
    }

    const auto found = key_of(m_index, cur.rec());

    cur.commit(&mtr);

    return found;
  };

  for (uint32_t k{2}; k <= 4000; k += 2) {
    EXPECT_EQ(at(k, PAGE_CUR_GE), k);
    EXPECT_EQ(at(k, PAGE_CUR_LE), k);
    EXPECT_EQ(at(k, PAGE_CUR_G), k < 4000 ? int64_t(k + 2) : -1);
    EXPECT_EQ(at(k, PAGE_CUR_L), k > 2 ? int64_t(k - 2) : -1);
    EXPECT_EQ(at(k - 1, PAGE_CUR_GE), k);
    EXPECT_EQ(at(k - 1, PAGE_CUR_G), k);
    EXPECT_EQ(at(k + 1, PAGE_CUR_LE), k);
    EXPECT_EQ(at(k + 1, PAGE_CUR_L), k);
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(at(5000, PAGE_CUR_GE), -1);
  EXPECT_EQ(at(1, PAGE_CUR_LE), -1);
}

/** Forward and backward through all of it. */
TEST_F(Blink, Scans) {
  put_all(1, 3000, 300);

  Mtr mtr;
  IndexCursor cur{*m_sys};
  uint32_t k{};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  while (cur.next(&mtr)) {
    EXPECT_EQ(key_of(m_index, cur.rec()), ++k);
  }

  cur.commit(&mtr);

  EXPECT_EQ(k, 3000U);

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);

  while (cur.prev(&mtr)) {
    EXPECT_EQ(key_of(m_index, cur.rec()), k--);
  }

  cur.commit(&mtr);

  EXPECT_EQ(k, 0U);
}

/** A forward scan restored in the middle sees everything else, when splits ran in between. */
TEST_F(Blink, ScanFwdAcrossSplits) {
  for (uint32_t k{10}; k <= 20000; k += 10) {
    ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  Mtr mtr;
  IndexCursor cur{*m_sys};
  uint32_t last{};
  ulint n{};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  while (cur.next(&mtr)) {
    const auto k = key_of(m_index, cur.rec());

    EXPECT_LT(last, k);
    last = k;
    ++n;

    if (n % 100 == 0) {
      cur.save(&mtr);
      cur.commit(&mtr);

      /* Split pages under the cursor: new keys around its position. */
      for (uint32_t i{1}; i <= 200; ++i) {
        const auto split_key = k + 1 + i % 8;

        if (split_key % 10 != 0 && !has(split_key, m_heap)) {
          ASSERT_EQ(put(split_key, 1500, m_heap), DB_SUCCESS);
        }

        mem_heap_empty(m_heap);
      }

      mtr.start();

      EXPECT_TRUE(cur.restore(Intent::Read, &mtr));
      EXPECT_EQ(key_of(m_index, cur.rec()), k);
    }
  }

  cur.commit(&mtr);

  EXPECT_EQ(last, 20000U);

  (void)validated();
}

/** A backward scan: left links and the walk right from a stale one. */
TEST_F(Blink, ScanBackAcrossSplits) {
  for (uint32_t k{10}; k <= 10000; k += 10) {
    ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  Mtr mtr;
  IndexCursor cur{*m_sys};
  uint32_t last{10001};
  std::set<uint32_t> seen;

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);

  ulint n{};

  while (cur.prev(&mtr)) {
    const auto k = key_of(m_index, cur.rec());

    EXPECT_LT(k, last);
    last = k;

    seen.insert(k);

    if (++n % 50 == 0) {
      cur.save(&mtr);
      cur.commit(&mtr);

      /* The pages to the left split: below the position only. */
      for (uint32_t i{1}; k > 100 && i <= 60; ++i) {
        const auto split_key = k - 100 + i % 9 + 1;

        if (split_key % 10 != 0 && !has(split_key, m_heap)) {
          ASSERT_EQ(put(split_key, 1500, m_heap), DB_SUCCESS);
        }

        mem_heap_empty(m_heap);
      }

      mtr.start();

      EXPECT_TRUE(cur.restore(Intent::Read, &mtr));
    }
  }

  cur.commit(&mtr);

  for (uint32_t k{10}; k <= 10000; k += 10) {
    EXPECT_EQ(seen.count(k), 1UL) << k;
  }

  (void)validated();
}

/** The left walk: a page's left link stale after its left sibling split, in the move itself. */
TEST_F(Blink, MoveLeftAfterSplit) {
  put_all(1, 1000, 300);

  const auto shape = validated();

  ASSERT_GT(shape.m_levels[0].size(), 3UL);

  uint32_t mid_first{};

  on_page(shape.m_levels[0][2], [&](page_t *page, Mtr *) { mid_first = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page))); });

  /* The position is first on that page; split what's left of it after saving. */
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek(m_index, key(mid_first, m_heap), PAGE_CUR_GE, Intent::Read, &mtr);
  EXPECT_EQ(key_of(m_index, cur.rec()), mid_first);
  cur.save(&mtr);
  cur.commit(&mtr);

  ASSERT_TRUE(del(mid_first - 1, m_heap));
  ASSERT_EQ(put(mid_first - 1, 5000, m_heap), DB_SUCCESS);
  ASSERT_TRUE(del(mid_first - 2, m_heap));
  ASSERT_EQ(put(mid_first - 2, 5000, m_heap), DB_SUCCESS);

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Read, &mtr));
  EXPECT_TRUE(cur.prev(&mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), mid_first - 1);
  EXPECT_TRUE(cur.prev(&mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), mid_first - 2);
  cur.commit(&mtr);

  (void)validated();
}

/** An empty leaf that nothing merged, its neighbours were too full: searches and scans go
through it. */
TEST_F(Blink, EmptyLeaf) {
  blink::set_merge(*m_sys, false);

  put_all(1, 1000, 300);

  const auto before = validated();

  ASSERT_GT(before.m_levels[0].size(), 3UL);

  const auto page_no = before.m_levels[0][1];
  uint32_t low{};
  uint32_t high{};

  on_page(before.m_levels[0][0], [&](page_t *page, Mtr *) { low = key_of(m_index, page_get_high_key(page)); });
  on_page(page_no, [&](page_t *page, Mtr *) { high = key_of(m_index, page_get_high_key(page)); });

  for (auto k = low; k < high; ++k) {
    ASSERT_TRUE(del(k, m_heap)) << k;
    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_EQ(after.m_levels[0], before.m_levels[0]) << "no page goes away";
  EXPECT_EQ(after.m_keys.size(), 1000UL - (high - low));

  on_page(page_no, [&](page_t *page, Mtr *) { EXPECT_EQ(page_get_n_recs(page), 0UL); });

  for (auto k = low; k < high; ++k) {
    EXPECT_FALSE(has(k, m_heap)) << k;
    mem_heap_empty(m_heap);
  }

  Mtr mtr;
  IndexCursor cur{*m_sys};

  /* Seek into the gap: the first record after it. */
  mtr.start();
  cur.seek(m_index, key(low, m_heap), PAGE_CUR_GE, Intent::Read, &mtr);
  EXPECT_TRUE(cur.on_rec());
  EXPECT_EQ(key_of(m_index, cur.rec()), high);

  /* Backward over it. */
  EXPECT_TRUE(cur.prev(&mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), low - 1);

  /* Forward over it. */
  EXPECT_TRUE(cur.next(&mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), high);

  cur.commit(&mtr);

  /* A position on the empty page stays there. */
  mtr.start();
  cur.position(m_index, key(low, m_heap), PAGE_CUR_LE, Intent::Read, &mtr);
  EXPECT_FALSE(cur.on_rec());
  EXPECT_EQ(cur.block()->get_page_no(), page_no);
  cur.save(&mtr);
  EXPECT_EQ(cur.saved_at(), SavedAt::Before);
  cur.commit(&mtr);

  /* It's back where the records come. */
  put_all(low, high - 1, 300);

  mtr.start();
  EXPECT_FALSE(cur.restore(Intent::Read, &mtr));
  EXPECT_TRUE(cur.next(&mtr));
  cur.commit(&mtr);

  EXPECT_EQ(validated().m_keys.size(), 1000UL);
}

/** Removing it all: the tree stays as high as it was, and works from there. */
TEST_F(Blink, RemoveAll) {
  put_all(1, 2000, 300);

  const auto before = validated();

  for (uint32_t k{1}; k <= 2000; ++k) {
    ASSERT_TRUE(del(k, m_heap));
    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_TRUE(after.m_keys.empty());
  EXPECT_GT(before.m_height, 0UL);
  EXPECT_EQ(after.m_height, 0UL) << "the merges take it down to the root";
  EXPECT_LT(used_pages(), before.n_pages());

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);
  EXPECT_FALSE(cur.next(&mtr));
  cur.commit(&mtr);

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);
  EXPECT_FALSE(cur.prev(&mtr));
  cur.commit(&mtr);

  put_all(1, 500, 300);

  EXPECT_EQ(validated().m_keys.size(), 500UL);
}

/** An update that doesn't fit splits the page, in one call. */
/** Pages with little on them merge, on every level, and it can grow again. */
TEST_F(Blink, Merge) {
  put_all(1, 3000, 300);

  const auto before = validated();
  std::vector<uint32_t> left;

  for (uint32_t k{1}; k <= 3000; ++k) {
    if (k % 10 == 0) {
      left.push_back(k);
    } else {
      ASSERT_TRUE(del(k, m_heap)) << k;
    }

    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_EQ(after.m_keys, left);
  EXPECT_LT(after.m_levels[0].size(), before.m_levels[0].size() / 2);
  EXPECT_LT(used_pages(), before.n_pages() / 2 + before.n_pages() / 4) << "freed";
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));

  for (uint32_t k{1}; k <= 3000; ++k) {
    if (k % 10 != 0) {
      ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS) << k;
    }

    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(validated().m_keys.size(), 3000UL);
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));
}

/** The same from the other end: what's left is on the left. */
TEST_F(Blink, MergeDescending) {
  put_all(1, 3000, 300);

  for (uint32_t k{3000}; k > 100; --k) {
    ASSERT_TRUE(del(k, m_heap)) << k;
    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_EQ(after.m_keys.size(), 100UL);
  /* What 100 records take on half full pages, and a few: at any page size. */
  EXPECT_LT(after.m_levels[0].size(), 100 * 340 / (UNIV_PAGE_SIZE / 2) + 8);
}

/** Scans in both directions see all that's left, after merges. */
TEST_F(Blink, MergeScans) {
  put_all(1, 3000, 300);

  std::vector<uint32_t> left;

  for (uint32_t k{1}; k <= 3000; ++k) {
    if (k % 7 == 0) {
      left.push_back(k);
    } else {
      ASSERT_TRUE(del(k, m_heap));
    }

    mem_heap_empty(m_heap);
  }

  Mtr mtr;
  IndexCursor cur{*m_sys};
  std::vector<uint32_t> seen;

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  while (cur.next(&mtr)) {
    seen.push_back(key_of(m_index, cur.rec()));
  }

  cur.commit(&mtr);

  EXPECT_EQ(seen, left);

  seen.clear();

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);

  while (cur.prev(&mtr)) {
    seen.push_back(key_of(m_index, cur.rec()));
  }

  cur.commit(&mtr);

  std::reverse(seen.begin(), seen.end());

  EXPECT_EQ(seen, left);
}

/** A cursor that saved its position on a page that gets merged away finds its way. */
TEST_F(Blink, MergeRestore) {
  put_all(1, 2000, 300);

  const auto shape = validated();

  ASSERT_GT(shape.m_levels[0].size(), 4UL);

  uint32_t first{};

  on_page(shape.m_levels[0][1], [&](page_t *page, Mtr *) { first = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page))); });

  /* On the first record of the second page, which then merges. */
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(first + 1, m_heap), Intent::Read, &mtr));
  cur.save(&mtr);
  cur.commit(&mtr);

  mem_heap_empty(m_heap);

  /* Everything on the first page and most of the second, without the saved record. */
  for (uint32_t k{1}; k < first + 40; ++k) {
    if (k != first + 1) {
      (void)del(k, m_heap);
      mem_heap_empty(m_heap);
    }
  }

  EXPECT_LT(validated().m_levels[0].size(), shape.m_levels[0].size());

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Read, &mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), first + 1);
  cur.commit(&mtr);
}

/** A cursor saved after the last record of a page, waiting for a lock on the supremum, when the
page takes in its right sibling: it's what follows the position now, and it can't be skipped. */
TEST_F(Blink, MergeAfterLast) {
  put_all(1, 2000, 300);

  const auto shape = validated();
  const auto n_leaves = shape.m_levels[0].size();

  ASSERT_GT(n_leaves, 4UL);

  uint32_t lowest{};
  uint32_t first{};

  on_page(shape.m_levels[0][n_leaves - 2], [&](page_t *page, Mtr *) { lowest = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page))); });
  on_page(shape.m_levels[0][n_leaves - 1], [&](page_t *page, Mtr *) { first = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page))); });

  ASSERT_LT(first + 1, 2000U) << "two records on the rightmost page";

  /* Little on the last two pages, with no merge yet. A delete on the left page would change its
  modify clock, so nothing is deleted from it after the position is saved. */
  blink::set_merge(*m_sys, false);

  for (uint32_t k{lowest}; k <= 2000; ++k) {
    if (k != first - 1 && k != first && k != first + 1) {
      ASSERT_TRUE(del(k, m_heap)) << k;
    }

    mem_heap_empty(m_heap);
  }

  blink::set_merge(*m_sys, true);

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(first - 1, m_heap), Intent::Read, &mtr));
  (void)cur.next_pos(&mtr);
  ASSERT_FALSE(cur.on_rec()) << "on the supremum";
  cur.save(&mtr);
  cur.commit(&mtr);

  /* The rightmost page has nothing on its right to take in: it goes into the left page. */
  ASSERT_TRUE(del(first + 1, m_heap));
  mem_heap_empty(m_heap);

  ASSERT_EQ(validated().m_levels[0].size(), n_leaves - 1);

  mtr.start();
  (void)cur.restore(Intent::Read, &mtr);

  if (!cur.on_rec()) {
    ASSERT_TRUE(cur.next(&mtr)) << "past what the merge brought";
  }

  EXPECT_EQ(key_of(m_index, cur.rec()), first);
  cur.commit(&mtr);
}

/** A page of this tree is held by its bits, with no RW latch, for reading a record and for changing one. */
TEST_F(Blink, NoRwLatch) {
  put_all(1, 2000, 300);

  for (const auto intent : {Intent::Read, Intent::Write}) {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    ASSERT_TRUE(cur.find(m_index, key(500, m_heap), intent, &mtr));

    const auto block = cur.block();

    EXPECT_EQ(rw_lock_get_writer(&block->m_rw_lock), RW_LOCK_NOT_LOCKED);
    EXPECT_EQ(rw_lock_get_reader_count(&block->m_rw_lock), 0UL);
    EXPECT_NE(block->m_bits.load(), 0UL);
    EXPECT_EQ(block->read_begin() % 2, 1UL);

    cur.commit(&mtr);

    EXPECT_EQ(block->m_bits.load(), 0UL);
    EXPECT_EQ(block->read_begin() % 2, 0UL);
  }
}

/** A read that holds nothing finds what a read with the page held does, in all the modes, and walks the
same records both ways. It really holds nothing, and what it has stays when the page changes. */
TEST_F(Blink, Snapshot) {
  put_all(1, 1000, 300);

  for (uint32_t k{}; k <= 1001; k += 13) {
    for (const ulint mode : {PAGE_CUR_G, PAGE_CUR_GE, PAGE_CUR_L, PAGE_CUR_LE}) {
      Mtr a_mtr;
      Mtr b_mtr;
      IndexCursor a{*m_sys};
      IndexCursor b{*m_sys};

      a_mtr.start();
      a.position(m_index, key(k, m_heap), SearchMode(mode), Intent::Read, &a_mtr);

      const auto a_key = a.on_rec() ? key_of(m_index, a.rec()) : 0;

      a.commit(&a_mtr);

      b_mtr.start();
      b.position(m_index, key(k, m_heap), SearchMode(mode), Intent::Snapshot, &b_mtr);

      const auto b_key = b.on_rec() ? key_of(m_index, b.rec()) : 0;

      b.commit(&b_mtr);

      EXPECT_EQ(a_key, b_key) << k << " mode " << mode;

      mem_heap_empty(m_heap);
    }
  }

  std::vector<uint32_t> keys;
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Snapshot, &mtr);

  while (cur.next(&mtr)) {
    keys.push_back(key_of(m_index, cur.rec()));
  }

  cur.commit(&mtr);

  ASSERT_EQ(keys.size(), 1000UL);

  for (uint32_t i{}; i < 1000; ++i) {
    ASSERT_EQ(keys[i], i + 1);
  }

  keys.clear();

  mtr.start();
  cur.seek_last(m_index, Intent::Snapshot, &mtr);

  while (cur.prev(&mtr)) {
    keys.push_back(key_of(m_index, cur.rec()));
  }

  cur.commit(&mtr);

  ASSERT_EQ(keys.size(), 1000UL);

  for (uint32_t i{}; i < 1000; ++i) {
    ASSERT_EQ(keys[i], 1000 - i);
  }

  /* Holding nothing, a writer goes through the same page, and the copy doesn't see it. */
  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(500, m_heap), Intent::Snapshot, &mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), 500U);

  auto w = std::thread([&] {
    auto heap = mem_heap_create(1024);

    EXPECT_TRUE(del(501, heap));
    EXPECT_TRUE(del(500, heap));
    mem_heap_free(heap);
  });

  w.join();

  EXPECT_EQ(key_of(m_index, cur.rec()), 500U);

  /* The page changed under it: it finds its place by key. */
  ASSERT_TRUE(cur.next(&mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), 502U);

  cur.save(&mtr);
  cur.commit(&mtr);

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Snapshot, &mtr));
  EXPECT_EQ(key_of(m_index, cur.rec()), 502U);
  cur.commit(&mtr);
}

/** Scans that hold nothing, with writers changing the pages all the time: every record that's always
there is seen once, in order, in both directions. */
TEST_F(Blink, SnapshotMt) {
  constexpr uint32_t N = 4000;

  /* Even keys stay, odd ones come and go. */
  for (uint32_t k{2}; k <= N; k += 2) {
    ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  std::atomic<bool> stop{};
  std::vector<std::thread> threads;

  for (uint32_t t{}; t < 3; ++t) {
    threads.emplace_back([&, t] {
      auto heap = mem_heap_create(16384);
      uint32_t round{};

      while (!stop.load()) {
        for (uint32_t k{1 + t * 2}; k < N; k += 6) {
          if (round % 2 == 0) {
            EXPECT_EQ(put(k, 300, heap), DB_SUCCESS);
          } else {
            EXPECT_TRUE(del(k, heap));
          }

          mem_heap_empty(heap);
        }

        ++round;
      }

      /* Back to even. */
      if (round % 2 == 1) {
        for (uint32_t k{1 + t * 2}; k < N; k += 6) {
          EXPECT_TRUE(del(k, heap));
          mem_heap_empty(heap);
        }
      }

      mem_heap_free(heap);
    });
  }

  for (int scan{}; scan < 30; ++scan) {
    const bool forward = scan % 2 == 0;
    uint32_t expected = forward ? 2 : N;
    uint32_t last = forward ? 0 : UINT32_MAX;
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    if (forward) {
      cur.seek_first(m_index, Intent::Snapshot, &mtr);
    } else {
      cur.seek_last(m_index, Intent::Snapshot, &mtr);
    }

    while (forward ? cur.next(&mtr) : cur.prev(&mtr)) {
      const auto k = key_of(m_index, cur.rec());

      ASSERT_TRUE(forward ? k > last : k < last) << "scan " << scan << " " << last << " then " << k;

      last = k;

      if (k % 2 == 0) {
        ASSERT_EQ(k, expected) << "scan " << scan;

        expected = forward ? expected + 2 : expected - 2;
      }
    }

    cur.commit(&mtr);

    ASSERT_EQ(expected, forward ? N + 2 : 0U) << "scan " << scan;
  }

  stop = true;

  for (auto &thread : threads) {
    thread.join();
  }

  (void)validated();
}

/** A cursor to delete mark or update in place holds the group of its record and nothing else: others
on the page go on, what needs all of the page waits for it. The changes themselves are in the row tests. */
TEST_F(Blink, Regions) {
  put_all(1, 200, 100);

  blink::set_merge(*m_sys, false);

  /* A key that goes on the page of 100, with room for it. */
  uint32_t gap{99};

  {
    Mtr probe;
    IndexCursor p{*m_sys};

    probe.start();
    ASSERT_TRUE(p.find(m_index, key(100, m_heap), Intent::Read, &probe));
    ASSERT_GT(page_get_n_recs(p.block()->get_frame()), 4UL);

    if (const auto next = page_rec_get_next(p.rec()); page_rec_is_user_rec(next)) {
      gap = key_of(m_index, next);
    }

    p.commit(&probe);
  }

  ASSERT_TRUE(del(gap, m_heap));
  mem_heap_empty(m_heap);

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(100, m_heap), Intent::Mark, &mtr));

  const auto block = cur.block();
  const auto bits = block->m_bits.load();

  EXPECT_EQ(__builtin_popcountll(bits), 1) << "one group";
  EXPECT_EQ(bits & BufBlock::HEADER_BIT, 0UL);
  EXPECT_EQ(rw_lock_get_writer(&block->m_rw_lock), RW_LOCK_NOT_LOCKED);
  EXPECT_EQ(block->read_begin() % 2, 0UL) << "the structure isn't owned";

  const auto version = block->read_begin();

  /* A record of another group on the page, by its bit. */
  uint32_t other{};

  for (uint32_t k{1}; k <= 200 && other == 0; ++k) {
    Mtr probe;
    IndexCursor p{*m_sys};

    probe.start();

    if (k != gap && p.find(m_index, key(k, m_heap), Intent::Snapshot, &probe)) {
      ulint bit{};

      /* In the same page: a copy has the page number of what it was copied from. */
      if (page_get_page_no(p.rec().page_align()) == block->get_page_no()) {
        const auto owned = blink::read_group_raw(block->get_frame(), page_offset(p.rec()), bit);

        if (owned && ((uint64_t{1} << bit) & bits) == 0 && k != 100) {
          other = k;
        }
      }
    }

    p.commit(&probe);
    mem_heap_empty(m_heap);
  }

  ASSERT_NE(other, 0U);

  std::atomic<int> done{};

  /* Another group, to change and to read: these go on. */
  std::thread marker([&] {
    auto heap = mem_heap_create(1024);
    Mtr m;
    IndexCursor c{*m_sys};

    m.start();
    EXPECT_TRUE(c.find(m_index, key(other, heap), Intent::Mark, &m));
    c.commit(&m);
    mem_heap_free(heap);

    ++done;
  });

  marker.join();
  EXPECT_EQ(done.load(), 1) << "it didn't wait";
  EXPECT_EQ(block->read_begin(), version) << "the structure didn't change";

  /* A read of the record itself that holds nothing: it needn't wait for the holder either. */
  std::thread([&] {
    auto heap = mem_heap_create(1024);
    Mtr m;
    IndexCursor c{*m_sys};

    m.start();
    EXPECT_TRUE(c.find(m_index, key(100, heap), Intent::Snapshot, &m));
    c.commit(&m);
    mem_heap_free(heap);
  }).join();

  /* All of the page, and a read that holds the record: these wait. */
  std::atomic<bool> inserted{};
  std::atomic<bool> read{};

  std::thread inserter([&] {
    auto heap = mem_heap_create(16384);

    EXPECT_EQ(put(gap, 100, heap), DB_SUCCESS);
    mem_heap_free(heap);

    inserted = true;
  });

  std::thread reader([&] {
    auto heap = mem_heap_create(1024);

    EXPECT_TRUE(has(100, heap));
    mem_heap_free(heap);

    read = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  EXPECT_FALSE(inserted.load()) << "needs all of the page";
  EXPECT_FALSE(read.load()) << "holds the page, for a record to stay";

  cur.commit(&mtr);

  inserter.join();
  reader.join();

  EXPECT_EQ(block->m_bits.load(), 0UL);

  blink::set_merge(*m_sys, true);

  (void)validated();
}

namespace {

/** Reorganizes a page in the middle of a read of it, once. */
struct ReorganizeInCopy {
  static inline std::atomic<bool> s_armed{};
  static inline const Index *s_index{};
  static inline page_no_t s_page{};

  static void hook(System &sys, blink::TestPoint point) {
    if (point == blink::TestPoint::SNAP_COPY && s_armed.exchange(false)) {
      Mtr mtr;

      mtr.start();

      const auto block = sys.m_btree->block_get(s_index->get_space_id(), s_page, RW_B_LATCH, &mtr);

      ut_a(sys.m_btree->page_reorganize(block, const_cast<Index *>(s_index), &mtr));

      mtr.commit(sys);
    }
  }
};

}  // namespace

/** A copy that fails because the page changed under it doesn't lose the place: what the cursor is on is
what it finds the next record from. The offset it was copying from means something else on the
reorganized page, and a copy of what's there can land anywhere. */
TEST_F(Blink, SnapshotCopyFails) {
  /* Records of many sizes with holes in between, so that a reorganize moves them around. */
  for (uint32_t k{1}; k <= 60; ++k) {
    ASSERT_EQ(put(k, 20 + (k * 37) % 180, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  blink::set_merge(*m_sys, false);
  blink::set_test_hook(*m_sys, ReorganizeInCopy::hook);

  ReorganizeInCopy::s_index = m_index;

  std::set<uint32_t> keys;

  for (uint32_t k{1}; k <= 60; ++k) {
    keys.insert(k);
  }

  std::mt19937 rng(7);

  for (int round{}; round < 200; ++round) {
    /* Make holes, and fill some with other sizes. */
    const auto victim = uint32_t(1 + rng() % 60);

    if (keys.count(victim) != 0) {
      if (keys.size() > 10) {
        ASSERT_TRUE(del(victim, m_heap));
        keys.erase(victim);
      }
    } else {
      ASSERT_EQ(put(victim, 20 + rng() % 180, m_heap), DB_SUCCESS);
      keys.insert(victim);
    }

    mem_heap_empty(m_heap);

    const auto from = *std::next(keys.begin(), long(rng() % (keys.size() - 1)));
    const auto expected = *keys.upper_bound(from);

    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    ASSERT_TRUE(cur.find(m_index, key(from, m_heap), Intent::Snapshot, &mtr));

    /* Now, not for the find: it has to be for the copy of the next one. */
    ReorganizeInCopy::s_page = page_get_page_no(cur.rec().page_align());
    ReorganizeInCopy::s_armed = true;

    ASSERT_TRUE(cur.next(&mtr));
    EXPECT_EQ(key_of(m_index, cur.rec()), expected) << "from " << from << " in round " << round;

    cur.commit(&mtr);

    ReorganizeInCopy::s_armed = false;

    mem_heap_empty(m_heap);
  }

  blink::set_test_hook(*m_sys, nullptr);
  blink::set_merge(*m_sys, true);

  (void)validated();
}

/** Only for a clustered index: the lock check of a secondary record writes to the header of the page,
which isn't what one group's bit is for. It stops, rather than letting that value go backwards. */
TEST_F(Blink, MarkClusteredOnly) {
  put_all(1, 10, 100);

  const auto sec = m_index->get_next();

  ASSERT_NE(sec, nullptr);

  EXPECT_DEATH(
    {
      auto heap = mem_heap_create(1024);
      auto t = dtuple_create(heap, 1);

      sec->copy_types(t, 1);
      dfield_set_data(dtuple_get_nth_field(t, 0), u32(heap, 1), 4);
      dtuple_set_n_fields_cmp(t, 1);

      Mtr mtr;
      IndexCursor cur{*m_sys};

      mtr.start();
      (void)cur.find(sec, t, Intent::Mark, &mtr);
    },
    ""
  );
}

/** Inserts that find their page owned queue on it, and the next owner does them all. */
TEST_F(Blink, InsertBatch) {
  constexpr uint32_t N_WAITERS = 8;

  /* Whatever this run of the tests has it set to. */
  m_sys->m_config.m_dict.m_insert_batch = true;

  blink::set_merge(*m_sys, false);

  /* Room on a page, in keys and in bytes. More than the root: it's read on the way down, and a page
  that's held and is the root stops them there, before there's a page to queue on. */
  for (uint32_t k{1}; k <= 1000; ++k) {
    ASSERT_EQ(put(k * 1000, 50, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  /* An ascending load leaves its pages full. */
  for (uint32_t k{480}; k <= 520; ++k) {
    if (k != 500) {
      ASSERT_TRUE(del(k * 1000, m_heap));
      mem_heap_empty(m_heap);
    }
  }

  /* Hold the page they want, and see the queue build up. */
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(500000, m_heap), Intent::Write, &mtr));

  const auto block = cur.block();
  const auto before = blink::n_batched(*m_sys);
  std::atomic<uint32_t> n_done{};
  std::vector<std::thread> threads;

  for (uint32_t t{}; t < N_WAITERS; ++t) {
    threads.emplace_back([&, t] {
      auto heap = mem_heap_create(16384);
      IndexCursor c{*m_sys};

      EXPECT_EQ(c.insert_batched(m_index, entry(500001 + t, 50, heap), DML_FLAGS, nullptr), DB_SUCCESS);

      mem_heap_free(heap);

      ++n_done;
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  EXPECT_EQ(n_done.load(), 0U) << "the page is held";
  EXPECT_NE(block->m_queued_ops.load(), nullptr);

  cur.commit(&mtr);

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(block->m_queued_ops.load(), nullptr);
  EXPECT_GT(blink::n_batched(*m_sys), before) << "somebody did the work of somebody else";

  for (uint32_t t{}; t < N_WAITERS; ++t) {
    EXPECT_TRUE(has(500001 + t, m_heap)) << t;
    mem_heap_empty(m_heap);
  }

  blink::set_merge(*m_sys, true);

  (void)validated();
}

/** What's not for a batch comes back with nothing done. */
TEST_F(Blink, InsertBatchRefuses) {
  m_sys->m_config.m_dict.m_insert_batch = true;

  put_all(1, 100, 100);

  IndexCursor cur{*m_sys};

  /* A record that's there: a duplicate, or one to reuse. */
  EXPECT_EQ(cur.insert_batched(m_index, entry(50, 100, m_heap), DML_FLAGS, nullptr), DB_FAIL);

  /* Too big for a page: its fields go outside. */
  EXPECT_EQ(cur.insert_batched(m_index, entry(5000, UNIV_PAGE_SIZE, m_heap), DML_FLAGS, nullptr), DB_FAIL);

  EXPECT_FALSE(has(5000, m_heap));

  /* A full page: nothing else on it fits. */
  ulint n_refused{};

  for (uint32_t k{1000}; k < 3000; ++k) {
    const auto err = cur.insert_batched(m_index, entry(k, 300, m_heap), DML_FLAGS, nullptr);

    ASSERT_TRUE(err == DB_SUCCESS || err == DB_FAIL) << err;

    if (err == DB_FAIL) {
      ++n_refused;
      ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS);
    }

    mem_heap_empty(m_heap);
  }

  EXPECT_GT(n_refused, 0UL);

  (void)validated();

  /* And turned off. */
  const auto before = blink::n_batched(*m_sys);

  m_sys->m_config.m_dict.m_insert_batch = false;

  EXPECT_EQ(cur.insert_batched(m_index, entry(9000, 100, m_heap), DML_FLAGS, nullptr), DB_FAIL);
  EXPECT_FALSE(has(9000, m_heap));
  EXPECT_EQ(blink::n_batched(*m_sys), before);

  m_sys->m_config.m_dict.m_insert_batch = true;
}

/** A dead page sends a stale reader back to the root, until it can be freed, and it isn't
freed before that. */
TEST_F(Blink, DeadPage) {
  put_all(1, 2000, 300);

  const auto before = validated();

  blink::hold_frees(*m_sys, true);
  m_holding_frees = true;

  const auto used = used_pages();

  for (uint32_t k{1}; k <= 1500; ++k) {
    ASSERT_TRUE(del(k, m_heap));
    mem_heap_empty(m_heap);
  }

  const auto after = validated();

  EXPECT_LT(after.m_levels[0].size(), before.m_levels[0].size());
  EXPECT_EQ(used_pages(), used) << "nothing freed while it's held";

  /* A page that isn't on the level any more. */
  page_no_t dead{FIL_NULL};

  for (const auto page_no : before.m_levels[0]) {
    if (std::find(after.m_levels[0].begin(), after.m_levels[0].end(), page_no) == after.m_levels[0].end()) {
      dead = page_no;
      break;
    }
  }

  ASSERT_NE(dead, FIL_NULL);

  const auto step = blink::read_node(*m_sys, m_index, dead, key(1, m_heap), PAGE_CUR_LE, blink::Edge::KEY, 0);

  EXPECT_EQ(step.m_kind, blink::NodeStep::RESTART);

  blink::hold_frees(*m_sys, false);
  m_holding_frees = false;

  m_sys->am(m_index).quiesce();

  EXPECT_EQ(used_pages(), after.n_pages());
}

/** The tree that's dropped frees the pages that wait: not twice. */
TEST_F(Blink, DropDeadPages) {
  put_all(1, 2000, 300);

  blink::hold_frees(*m_sys, true);

  for (uint32_t k{1}; k <= 1500; ++k) {
    ASSERT_TRUE(del(k, m_heap));
    mem_heap_empty(m_heap);
  }

  blink::hold_frees(*m_sys, false);

  auto trx = api::trx_begin(*m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(*m_sys, trx), DB_SUCCESS);

  restart();
}

TEST_F(Blink, UpdateGrows) {
  put_all(1, 300, 100);

  auto trx = begin();
  auto thr = pars_complete_graph_for_exec(SelNode::create(m_heap), trx, m_heap);

  for (uint32_t k{1}; k <= 300; k += 3) {
    Mtr mtr;
    IndexCursor cur{*m_sys};
    MemHeap *heap{};
    BigRec big{};

    auto upd = RowUpdate::upd_create(1, m_heap);
    auto field = upd->get_nth_field(0);
    /* Bigger than what the page can take in place, and small enough to stay on the page. */
    const ulint grown{UNIV_PAGE_SIZE / 5};
    auto b = static_cast<byte *>(mem_heap_alloc(m_heap, grown));

    memset(b, 'z', grown);
    field->m_field_no = B_FIELD;
    field->m_orig_len = 0;
    field->m_exp = nullptr;
    dfield_set_data(&field->m_new_val, b, grown);

    mtr.start();
    ASSERT_TRUE(cur.find(m_index, key(k, m_heap), Intent::Write, &mtr));
    ASSERT_EQ(cur.update(Intent::Write, DML_FLAGS, &heap, &big, upd, 0, thr, &mtr), DB_SUCCESS) << k;
    cur.commit(&mtr);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    EXPECT_EQ(big.n_fields, 0UL);
  }

  commit(trx);

  const auto shape = validated();

  EXPECT_EQ(shape.m_keys.size(), 300UL);
  EXPECT_GE(shape.m_height, 1UL);
}

/** Records of the largest size: one to a page, and the high key on it. */
TEST_F(Blink, LargeRecs) {
  for (uint32_t k{1}; k <= 200; ++k) {
    ASSERT_EQ(put(k * 2, 7000, m_heap), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  for (uint32_t k{1}; k <= 200; ++k) {
    ASSERT_EQ(put(k * 2 - 1, 7000, m_heap), DB_SUCCESS) << k;
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(validated().m_keys.size(), 400UL);
}

/** Long keys: the high key takes real room on a page, up to the limit, which is an error
for the record and nothing else. */
TEST_F(Blink, LongKeys) {
  constexpr ulint N_COLS{8};
  constexpr ulint COL_LEN{768};
  constexpr const char *NAME = "test/longkey";

  api::TableSchema *tbl_sch{};
  api::IndexSchema *idx_sch{};

  ASSERT_EQ(api::TableSchema::create(NAME, &tbl_sch, IB_TBL_V1), DB_SUCCESS);

  for (ulint i{}; i < N_COLS; ++i) {
    ASSERT_EQ(tbl_sch->add_col(std::format("c{}", i).c_str(), IB_VARCHAR, IB_COL_NONE, 0, COL_LEN), DB_SUCCESS);
  }

  ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);

  for (ulint i{}; i < N_COLS; ++i) {
    ASSERT_EQ(idx_sch->add_col(std::format("c{}", i).c_str(), 0), DB_SUCCESS);
  }

  ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

  auto trx = begin();
  uint64_t table_id{};

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_create(*m_sys, trx, tbl_sch, &table_id), DB_SUCCESS);
  commit(trx);

  api::TableSchema::destroy(*m_sys, tbl_sch);

  const auto put_row = [&](uint32_t n, ulint len, ulint n_full) {
    auto trx = begin();
    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, NAME, trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto tpl = crsr->clust_read_tuple_create();

    /* The rows differ in the last column: their keys share all of it before. */
    for (ulint i{}; i < N_COLS; ++i) {
      std::string val(i < n_full ? len : 1, 'a');

      if (i == N_COLS - 1) {
        val = std::format("{:0>8}", n);
      }

      EXPECT_EQ(tpl->set_value(i, val.data(), val.size()), DB_SUCCESS);
    }

    const auto err = crsr->insert_row(*m_sys, tpl);

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(*m_sys, crsr);

    if (err == DB_SUCCESS) {
      commit(trx);
    } else {
      rollback(trx);
    }

    return err;
  };

  /* A key of 7 full columns is past the limit. */
  EXPECT_EQ(put_row(1, COL_LEN, 7), DB_TOO_BIG_RECORD);

  /* A key of 7 columns of this length fits, and high keys of it split fine. */
  const ulint LEN{std::min<ulint>(650, (blink::max_key_size() - REC_N_EXTRA_BYTES - 2 * N_COLS - 8) / 7)};
  constexpr uint32_t N_ROWS{500};

  ASSERT_LE(REC_N_EXTRA_BYTES + 2 * N_COLS + 7 * LEN + 8, blink::max_key_size());

  for (uint32_t n{}; n < N_ROWS; ++n) {
    ASSERT_EQ(put_row(n * 7919 % N_ROWS, LEN, 7), DB_SUCCESS) << n;
  }

  m_sys->m_dict->mutex_acquire();
  m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
  m_sys->m_dict->mutex_release();

  Mtr mtr;
  IndexCursor cur{*m_sys};
  uint32_t n{};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  while (cur.next(&mtr)) {
    ++n;
  }

  cur.commit(&mtr);

  EXPECT_EQ(n, N_ROWS);

  /* What the pages look like, for the file space: nothing leaked. */
  ulint n_pages{};
  auto page_no = m_index->get_page_no();

  on_page(page_no, [&](page_t *page, Mtr *m) { EXPECT_GE(m_sys->m_btree->page_get_level(page, m), 1UL); });

  page_no_t child{FIL_NULL};

  while (page_no != FIL_NULL) {
    ++n_pages;

    page_no_t next{FIL_NULL};

    on_page(page_no, [&](page_t *page, Mtr *m) {
      next = Btree::page_get_next(page, m);

      EXPECT_TRUE(page_validate(page, m_index));
      EXPECT_EQ(page_get_high_key(page).is_null(), next == FIL_NULL);

      if (m_sys->m_btree->page_get_level(page, m) > 0 && Btree::page_get_prev(page, m) == FIL_NULL) {
        MemHeap *heap{};
        const auto rec = page_rec_get_next(page_get_infimum_rec(page));
        RecOffs offs = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

        child = m_sys->m_btree->node_ptr_get_child_page_no(rec, offs);

        mem_heap_free(heap);
      }
    });

    if (next != FIL_NULL) {
      page_no = next;
    } else {
      page_no = child;
      child = FIL_NULL;
    }
  }

  EXPECT_GT(n_pages, 10UL);

  open_index();
}

/** Through the SQL level, with transactions, undo and locks. */
TEST_F(Blink, Rows) {
  ut::Rows rows;

  for (uint32_t k{1}; k <= 3000; ++k) {
    rows.emplace(k, ut::Row{k, k, std::string(200, 'x')});
  }

  load(rows);
  expect_rows(rows);

  auto trx = begin();

  for (uint32_t k{1}; k <= 500; ++k) {
    ut::Row row{k, k, std::string(1500, 'u')};

    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);

  trx = begin();

  for (uint32_t k{1}; k <= 500; ++k) {
    rows[k].m_b = std::string(1500, 'u');
    ASSERT_EQ(update(trx, k, rows[k]), DB_SUCCESS);
  }

  commit(trx);
  expect_rows(rows);

  (void)validated();
}

/** Blobs: the record split off its fields goes through a split too. */
TEST_F(Blink, Blobs) {
  ut::Rows rows;

  for (uint32_t k{1}; k <= 200; ++k) {
    rows.emplace(k, ut::Row{k, k, ut::blob(k, k % 7 == 0 ? 200000 : 9000)});
  }

  load(rows);
  expect_rows(rows);

  auto trx = begin();

  for (uint32_t k{1}; k <= 50; ++k) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  rollback(trx);
  expect_rows(rows);
}

/** Crash recovery of whatever is committed: the tree as it was, and consistent. */
TEST_F(Blink, Restart) {
  put_all(1, 3000, 300);

  const auto before = validated();

  restart();
  open_index();

  const auto after = validated();

  EXPECT_EQ(after.m_keys, before.m_keys);
  EXPECT_EQ(after.m_levels, before.m_levels);
}

/** A file for a child to tell how far it got. */
void flush_log(System &sys) {
  sys.m_log->buffer_flush_to_disk();
}

/** A hook of a split in a child: at the nth point, kill it with the log durable. */
std::atomic<int> g_n_to_kill{};
blink::TestPoint g_kill_at{};

void kill_hook(System &sys, blink::TestPoint point) {
  if (point == g_kill_at && g_n_to_kill.fetch_sub(1) == 1) {
    flush_log(sys);
    ::raise(SIGKILL);
  }
}

/** A crash after a merge, before its dead page is free: restart frees it. */
TEST_F(Blink, CrashAfterMerge) {
  put_all(1, 2000, 300);

  std::atomic<uint32_t> *n_done = static_cast<std::atomic<uint32_t> *>(
    mmap(nullptr, sizeof(std::atomic<uint32_t>), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
  );

  ASSERT_NE(n_done, MAP_FAILED);

  crash([&]() {
    open_index();

    g_kill_at = blink::TestPoint::MERGED;
    g_n_to_kill = 5;
    blink::set_test_hook(*m_sys, kill_hook);

    /* A record that was removed before the kill is removed after it: its mini-transaction is
    what the merge commits behind. */
    MemHeap *heap = mem_heap_create(16384);

    for (uint32_t k{1};; ++k) {
      n_done->store(k);
      (void)del(k, heap);
      mem_heap_empty(heap);
    }
  });

  open_index();

  const auto after = validated();
  const auto n = n_done->load();

  munmap(n_done, sizeof(std::atomic<uint32_t>));

  ASSERT_GT(n, 1UL);

  for (uint32_t k{}; k < after.m_keys.size(); ++k) {
    EXPECT_GE(after.m_keys[k], n - 1) << "lost or brought back";
  }

  EXPECT_EQ(after.m_keys.back(), 2000UL);
  EXPECT_EQ(used_pages(), after.n_pages()) << "a dead page wasn't given back";
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));
}

/** Dead pages that have waited for a long time, over a checkpoint: restart frees them. */
TEST_F(Blink, CrashDeadPages) {
  put_all(1, 2000, 300);

  crash([&]() {
    open_index();

    blink::hold_frees(*m_sys, true);

    MemHeap *heap = mem_heap_create(16384);

    for (uint32_t k{1}; k <= 1500; ++k) {
      (void)del(k, heap);
      mem_heap_empty(heap);
    }

    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);

    flush_log(*m_sys);
  });

  open_index();

  const auto after = validated();

  EXPECT_EQ(after.m_keys.size(), 500UL);
  EXPECT_EQ(used_pages(), after.n_pages()) << "a dead page wasn't given back";
}

/** A crash in the middle of a split with everything logged so far durable: nothing of it is
there, and the page it allocated is back. */
TEST_F(Blink, CrashMidSplit) {
  put_all(1, 2000, 300);

  crash([&]() {
    open_index();

    g_kill_at = blink::TestPoint::SPLIT;
    g_n_to_kill = 3;
    blink::set_test_hook(*m_sys, kill_hook);

    MemHeap *heap = mem_heap_create(16384);

    for (uint32_t k{2001};; ++k) {
      (void)put(k, 300, heap);
      mem_heap_empty(heap);
    }
  });

  open_index();

  const auto shape = validated();

  ASSERT_GE(shape.m_keys.size(), 2000UL);

  for (uint32_t i{}; i < shape.m_keys.size(); ++i) {
    EXPECT_EQ(shape.m_keys[i], i + 1);
  }

  put_all(uint32_t(shape.m_keys.size() + 1), 4000, 300);

  EXPECT_EQ(validated().m_keys.size(), 4000UL);
}

/** A crash between the levels of a split: the tree has the new page by the link only, every record is
there, nothing happens at restart, and the writers that come put the node pointers in. */
TEST_F(Blink, CrashHalfSplit) {
  const auto n_unlinked = [&]() {
    size_t n{};
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);
    mtr.commit(*m_sys);

    auto page_no = m_index->get_page_no();

    /* Down the left edge, then along the leaves. */
    for (;;) {
      const auto step = blink::read_node(*m_sys, m_index, page_no, nullptr, 0, blink::Edge::FIRST, 0);

      if (step.m_kind == blink::NodeStep::STOP) {
        break;
      }

      page_no = step.m_page;
    }

    while (page_no != FIL_NULL) {
      Mtr page_mtr;

      page_mtr.start();

      const auto page = m_sys->m_btree->block_get(m_index->get_space_id(), page_no, RW_B_LATCH, &page_mtr)->get_frame();

      n += page_right_unlinked(page) ? 1 : 0;
      page_no = Btree::page_get_next(page, &page_mtr);

      page_mtr.commit(*m_sys);
    }

    return n;
  };

  /* Room for the writers after, in the pages from before the crash. */
  for (uint32_t k{1}; k <= 2000; ++k) {
    ASSERT_EQ(put(k * 2, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  ASSERT_EQ(n_unlinked(), 0UL) << "they are all complete with nothing going on";

  crash([&]() {
    open_index();

    g_kill_at = blink::TestPoint::UNLINKED;
    g_n_to_kill = 3;
    blink::set_test_hook(*m_sys, kill_hook);

    MemHeap *heap = mem_heap_create(16384);

    for (uint32_t k{2001};; ++k) {
      (void)put(k * 2, 300, heap);
      mem_heap_empty(heap);
    }
  });

  open_index();

  EXPECT_GE(n_unlinked(), 1UL) << "the split that the crash stopped is on the page";

  const auto shape = validated();

  ASSERT_GE(shape.m_keys.size(), 2000UL);

  for (uint32_t i{}; i < shape.m_keys.size(); ++i) {
    ASSERT_EQ(shape.m_keys[i], (i + 1) * 2);
  }

  /* On every leaf, the odd keys. */
  const auto n = uint32_t(shape.m_keys.size());

  for (uint32_t k{}; k <= n; ++k) {
    ASSERT_EQ(put(k * 2 + 1, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(n_unlinked(), 0UL);
  EXPECT_EQ(validated().m_keys.size(), size_t(n) * 2 + 1);
}

/** The same with a page just allocated, in the short mini-transaction before the split's. */
TEST_F(Blink, CrashAfterAlloc) {
  put_all(1, 2000, 300);

  crash([&]() {
    open_index();

    g_kill_at = blink::TestPoint::ALLOCATED;
    g_n_to_kill = 2;
    blink::set_test_hook(*m_sys, kill_hook);

    MemHeap *heap = mem_heap_create(16384);

    for (uint32_t k{2001};; ++k) {
      (void)put(k, 300, heap);
      mem_heap_empty(heap);
    }
  });

  open_index();

  const auto shape = validated();

  for (uint32_t i{}; i < shape.m_keys.size(); ++i) {
    EXPECT_EQ(shape.m_keys[i], i + 1);
  }
}

/** A page with no link at the crash is back in the segment, by its system transaction. */
TEST_F(Blink, CrashUnlinked) {
  put_all(1, 2000, 300);

  const auto before = validated();

  crash([&]() {
    open_index();

    const auto used = used_pages();

    (void)blink::alloc_unlinked(*m_sys, m_index);

    EXPECT_EQ(used_pages(), used + 1);

    flush_log(*m_sys);
  });

  open_index();

  const auto after = validated();

  EXPECT_EQ(after.m_keys, before.m_keys);
  EXPECT_EQ(used_pages(), before.n_pages());
}

/** A crash in a loop: each life gets its own splits, each recovery validates. */
TEST_F(Blink, CrashLoop) {
  uint32_t next{1};

  put_all(1, 1000, 400);
  next = 1001;

  for (int life{}; life < 3; ++life) {
    crash([&]() {
      open_index();

      g_kill_at = life % 2 == 0 ? blink::TestPoint::SPLIT : blink::TestPoint::ALLOCATED;
      g_n_to_kill = 2 + life;
      blink::set_test_hook(*m_sys, kill_hook);

      MemHeap *heap = mem_heap_create(16384);

      for (auto k = next;; ++k) {
        (void)put(k, 400, heap);
        mem_heap_empty(heap);
      }
    });

    open_index();

    const auto shape = validated();

    for (uint32_t i{}; i < shape.m_keys.size(); ++i) {
      ASSERT_EQ(shape.m_keys[i], i + 1);
    }

    next = uint32_t(shape.m_keys.size() + 1);
  }
}


/** How long the threads have, also on a sanitizer. */
constexpr auto WATCHDOG = 300s;

/** Concurrency, against a model: each writer owns the keys that are its number modulo the
writers, and knows exactly which it has. Keys in the stable set are never removed: readers must
always find them, scans must see them once. The sizes vary for many splits. */
class BlinkMt : public Blink {
 protected:
  static constexpr uint32_t N_WRITERS = 4;
  static constexpr uint32_t N_STABLE = 500;

  /** Stable keys are multiples of this: they belong to nobody. */
  static constexpr uint32_t STABLE_STEP = 16;

  struct Model {
    std::set<uint32_t> m_keys;
  };

  void writer(uint32_t w, uint32_t n_ops, uint32_t key_space, Model &model) {
    std::mt19937 rng(1000 + w);
    auto heap = mem_heap_create(16384);

    for (uint32_t op{}; op < n_ops; ++op) {
      /* A key of this writer, not a stable one. */
      auto k = (rng() % key_space) * N_WRITERS * STABLE_STEP + 1 + w;

      if (model.m_keys.count(k) == 0) {
        const ulint blen = 50 + rng() % 1200;

        if (put(k, blen, heap) != DB_SUCCESS) {
          ++m_errors;
        }

        model.m_keys.insert(k);
      } else {
        if (!del(k, heap)) {
          ++m_errors;
        }

        model.m_keys.erase(k);
      }

      mem_heap_empty(heap);
    }

    mem_heap_free(heap);
  }

  void reader(uint32_t id) {
    std::mt19937 rng(2000 + id);
    auto heap = mem_heap_create(16384);

    while (!m_stop.load(std::memory_order_relaxed)) {
      const auto k = (rng() % N_STABLE + 1) * STABLE_STEP * N_WRITERS;

      if (!has(k, heap)) {
        ++m_errors;
      }

      mem_heap_empty(heap);
      ++m_n_reads;
    }

    mem_heap_free(heap);
  }

  /** One pass forward; a save and a restore on the way, as a long scan does. */
  void scan_fwd() {
    Mtr mtr;
    IndexCursor cur{*m_sys};
    uint32_t last{};
    uint32_t n_stable{};
    ulint n{};
    bool resync{};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);

    while (cur.next(&mtr)) {
      const auto k = key_of(m_index, cur.rec());

      /* The saved record went: restore() is before it, new keys can be between. */
      if (resync && k <= last) {
        continue;
      }

      resync = false;

      if (k <= last) {
        ++m_errors;
      }

      last = k;

      n_stable += k % (STABLE_STEP * N_WRITERS) == 0;

      if (++n % 64 == 0) {
        cur.save(&mtr);
        cur.commit(&mtr);
        mtr.start();
        resync = !cur.restore(Intent::Read, &mtr);
      }
    }

    cur.commit(&mtr);

    if (n_stable != N_STABLE) {
      ++m_errors;
    }
  }

  void scan_back() {
    Mtr mtr;
    IndexCursor cur{*m_sys};
    uint32_t last{UINT32_MAX};
    uint32_t n_stable{};

    mtr.start();
    cur.seek_last(m_index, Intent::Read, &mtr);

    while (cur.prev(&mtr)) {
      const auto k = key_of(m_index, cur.rec());

      if (k >= last) {
        ++m_errors;
      }

      last = k;

      n_stable += k % (STABLE_STEP * N_WRITERS) == 0;
    }

    cur.commit(&mtr);

    if (n_stable != N_STABLE) {
      ++m_errors;
    }
  }

  void load_stable() {
    for (uint32_t i{1}; i <= N_STABLE; ++i) {
      ASSERT_EQ(put(i * STABLE_STEP * N_WRITERS, 100, m_heap), DB_SUCCESS);
      mem_heap_empty(m_heap);
    }
  }

  void run(uint32_t n_ops, uint32_t key_space, uint32_t n_readers, bool scans) {
    Watchdog dog{WATCHDOG};
    auto &models = m_models;
    std::vector<std::thread> writers;
    std::vector<std::thread> others;

    for (uint32_t w{}; w < N_WRITERS; ++w) {
      writers.emplace_back([&, w]() { writer(w, n_ops, key_space, models[w]); });
    }

    for (uint32_t r{}; r < n_readers; ++r) {
      others.emplace_back([this, r]() { reader(r); });
    }

    if (scans) {
      others.emplace_back([this]() {
        while (!m_stop.load(std::memory_order_relaxed)) {
          scan_fwd();
          ++m_n_scans;
        }
      });
      others.emplace_back([this]() {
        while (!m_stop.load(std::memory_order_relaxed)) {
          scan_back();
          ++m_n_scans;
        }
      });
    }

    for (auto &thread : writers) {
      thread.join();
    }

    m_stop = true;

    for (auto &thread : others) {
      thread.join();
    }

    EXPECT_EQ(m_errors.load(), 0UL);

    /* Exactly what the model has: no key lost, none twice. */
    std::vector<uint32_t> expected;

    for (const auto &model : models) {
      expected.insert(expected.end(), model.m_keys.begin(), model.m_keys.end());
    }

    for (uint32_t i{1}; i <= N_STABLE; ++i) {
      expected.push_back(i * STABLE_STEP * N_WRITERS);
    }

    std::sort(expected.begin(), expected.end());

    const auto shape = validated();

    EXPECT_EQ(shape.m_keys, expected);

    /* The test was of something. */
    EXPECT_GE(shape.m_height, 1UL);
    EXPECT_GT(shape.m_levels[0].size(), 8UL);
    EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));
  }

  /** What each writer has, also from the runs before this one. */
  std::vector<Model> m_models{N_WRITERS};

  std::atomic<bool> m_stop{};
  std::atomic<ulint> m_errors{};
  std::atomic<ulint> m_n_reads{};
  std::atomic<ulint> m_n_scans{};
};

/** Writers alone, from a leaf root: raises and splits race. */
TEST_F(BlinkMt, Writers) {
  load_stable();
  run(30000, 3000, 0, false);
}

/** With readers on the keys that stay. */
TEST_F(BlinkMt, Readers) {
  load_stable();
  run(30000, 3000, 3, false);
  EXPECT_GT(m_n_reads.load(), 0UL);
}

/** With scans both ways. */
TEST_F(BlinkMt, Scans) {
  load_stable();
  run(30000, 3000, 2, true);
  EXPECT_GT(m_n_scans.load(), 0UL);
}

/** The tree grows and goes down again under readers and scans: merges race with everything. */
TEST_F(BlinkMt, Merges) {
  load_stable();
  run(30000, 2000, 2, true);

  /* Writers on a tree that's mostly being taken apart. */
  m_stop = false;
  run(30000, 200, 2, true);

  EXPECT_GT(m_n_scans.load(), 0UL);
}

/** Inserts that keep hitting the same few pages, also from an empty tree. */
TEST_F(BlinkMt, Hot) {
  load_stable();
  run(30000, 400, 1, false);
}

/** The search with no latch decides as the one with the latch, on every level, in all the modes. */
TEST_F(Blink, NodeRead) {
  put_all(1, 6000, 300);

  for (uint32_t k : {0U, 1U, 2U, 3000U, 5999U, 6000U, 6001U, 100000U}) {
    for (const ulint mode : {PAGE_CUR_G, PAGE_CUR_GE, PAGE_CUR_L, PAGE_CUR_LE}) {
      auto page_no = m_index->get_page_no();

      for (;;) {
        const auto t = key(k, m_heap);
        const auto a = blink::read_node(*m_sys, m_index, page_no, t, mode, blink::Edge::KEY, 0);
        const auto b = blink::read_node_latched(*m_sys, m_index, page_no, t, mode, blink::Edge::KEY, 0);

        ASSERT_EQ(a.m_kind, b.m_kind) << k << " mode " << mode;
        ASSERT_EQ(a.m_page, b.m_page) << k << " mode " << mode;
        ASSERT_EQ(a.m_level, b.m_level);

        if (a.m_kind == blink::NodeStep::STOP) {
          break;
        }

        page_no = a.m_page;
      }

      mem_heap_empty(m_heap);
    }
  }

  EXPECT_EQ(blink::n_node_retries(*m_sys), 0UL) << "nothing was writing";

  /* Going down the edges. */
  for (const auto edge : {blink::Edge::FIRST, blink::Edge::LAST}) {
    auto page_no = m_index->get_page_no();

    for (;;) {
      const auto a = blink::read_node(*m_sys, m_index, page_no, nullptr, 0, edge, 0);
      const auto b = blink::read_node_latched(*m_sys, m_index, page_no, nullptr, 0, edge, 0);

      ASSERT_EQ(a.m_kind, b.m_kind);
      ASSERT_EQ(a.m_page, b.m_page);

      if (a.m_kind == blink::NodeStep::STOP) {
        break;
      }

      page_no = a.m_page;
    }
  }
}

/** Nothing on a page takes the search out of it or stops the process. */
TEST_F(Blink, NodeGarbage) {
  put_all(1, 6000, 300);

  std::vector<byte> frame(UNIV_PAGE_SIZE);

  {
    Mtr mtr;

    mtr.start();
    memcpy(frame.data(), m_sys->m_btree->block_get(m_index->get_space_id(), m_index->get_page_no(), RW_S_LATCH, &mtr)->get_frame(), UNIV_PAGE_SIZE);
    mtr.commit(*m_sys);
  }

  std::mt19937 rng(1);
  std::vector<byte> copy(UNIV_PAGE_SIZE);
  const auto t = key(3000, m_heap);
  blink::NodeStep step{};
  size_t n_ok{};

  /* The id has to stay and the level has to be above the stop, or the search ends at the top. */
  const auto keep = [&](std::vector<byte> &p) {
    memcpy(p.data() + PAGE_HEADER + PAGE_INDEX_ID, frame.data() + PAGE_HEADER + PAGE_INDEX_ID, 8);
    mach_write_to_2(p.data() + PAGE_HEADER + PAGE_LEVEL, 1);
  };

  for (int round{}; round < 20000; ++round) {
    copy = frame;

    if (round % 2 == 0) {
      /* A page with some of it changed: mostly what a torn read looks like. */
      for (int i = 1 + int(rng() % 64); i > 0; --i) {
        copy[rng() % UNIV_PAGE_SIZE] = byte(rng());
      }
    } else {
      for (auto &b : copy) {
        b = byte(rng());
      }
    }

    keep(copy);

    for (const ulint mode : {PAGE_CUR_G, PAGE_CUR_GE, PAGE_CUR_L, PAGE_CUR_LE}) {
      n_ok += blink::read_node_raw(m_index, copy.data(), 3, t, mode, blink::Edge::KEY, 0, step) ? 1 : 0;
    }

    for (const auto edge : {blink::Edge::FIRST, blink::Edge::LAST, blink::Edge::RANDOM}) {
      (void)blink::read_node_raw(m_index, copy.data(), 3, nullptr, 0, edge, 0, step);
    }

    blink::RawFamily fam;
    ulint size;

    (void)blink::read_family_raw(m_index, copy.data(), t, 1, fam);
    (void)blink::read_rec_size_raw(m_index, copy.data(), 1, size);

    /* And what a leaf does with it. */
    {
      std::vector<byte> leaf_copy = copy;
      blink::RawLeaf leaf;
      ulint prev{};
      std::vector<byte> to(UNIV_PAGE_SIZE);

      mach_write_to_2(leaf_copy.data() + PAGE_HEADER + PAGE_LEVEL, 0);

      for (const ulint mode : {PAGE_CUR_G, PAGE_CUR_GE, PAGE_CUR_L, PAGE_CUR_LE}) {
        if (blink::read_leaf_raw(m_index, leaf_copy.data(), t, mode, blink::Edge::KEY, leaf) && leaf.m_kind == blink::RawLeaf::FOUND) {
          (void)blink::read_rec_copy_raw(leaf_copy.data(), leaf.m_offs, to.data());
          (void)blink::read_prev_raw(leaf_copy.data(), leaf.m_offs, prev);
        }
      }

      (void)blink::read_rec_copy_raw(leaf_copy.data(), rng() % 65536, to.data());
      {
        ulint bit{};

        (void)blink::read_group_raw(leaf_copy.data(), rng() % 65536, bit);
      }

      (void)blink::read_prev_raw(leaf_copy.data(), rng() % 65536, prev);
    }
  }

  EXPECT_GT(n_ok, 0UL) << "some were still pages";
}

/** A reader waits for a writer on a non-leaf page with no latch, and finds what's there after. */
TEST_F(Blink, NodeWriter) {
  put_all(1, 6000, 300);

  const auto root = m_index->get_page_no();
  const auto before = blink::n_node_retries(*m_sys);

  Mtr mtr;

  mtr.start();

  (void)m_sys->m_btree->block_get(m_index->get_space_id(), root, RW_X_LATCH, &mtr);

  std::atomic<bool> done{};

  std::thread reader([&] {
    auto heap = mem_heap_create(1024);

    done = has(3000, heap);
    mem_heap_free(heap);
  });

  /* It can't get through with the version odd, whatever it does. */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(done.load());

  mtr.commit(*m_sys);
  reader.join();

  EXPECT_TRUE(done.load());
  EXPECT_GT(blink::n_node_retries(*m_sys), before);
}

/** What a page latch costs in the operations it's for: inserts and point selects on a tree, each
through the index interface, with no row layer, undo or locks around them, and everything in the
buffer pool so that I/O doesn't hide it. A lock is chosen on these numbers, in a build without
UNIV_DEBUG, on an idle machine. PERF lines: BlinkPerf.<test>, one for each thread count. */
class BlinkPerf : public Blink {
 protected:
  [[nodiscard]] ulint pool_size() const override { return 4096UL * 1024 * 1024; }

  /** The rows in the tree before a run. */
  static constexpr uint32_t N_ROWS = 1000000;

  /** The operations in a run, over all the threads. */
  static constexpr uint64_t N_OPS = 1000000;

  /** The size of a row's b. */
  static constexpr ulint BLEN = 100;

  /** The keys at the top are for appends: see InsertAppend. */
  static constexpr uint32_t MIX_MASK = 0x7FFFFFFF;

  /** @return the keys in a different order, none twice in fewer than 2^31 of them: an odd
  multiplier is a bijection modulo 2^31. */
  [[nodiscard]] static uint32_t mix(uint32_t n) noexcept { return (n * 2654435761U) & MIX_MASK; }

  /** Load by n_threads threads, also what makes the tree one that merges and splits are done on. */
  void load(uint32_t n_rows) {
    constexpr int N_LOADERS = 16;

    const auto per = n_rows / N_LOADERS;
    std::vector<std::thread> threads;

    for (int t{}; t < N_LOADERS; ++t) {
      threads.emplace_back([&, t]() {
        auto heap = mem_heap_create(16384);

        for (uint32_t i{}; i < per; ++i) {
          ASSERT_EQ(put(mix(uint32_t(t) * per + i), BLEN, heap), DB_SUCCESS);
          mem_heap_empty(heap);
        }

        mem_heap_free(heap);
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    m_n_loaded = per * N_LOADERS;
    m_next = m_n_loaded;
  }

  template <typename Body>
  void each_thread_count(const char *name, Body body) {
    for (const int n_threads : {1, 4, 16, 48, 96}) {
      std::vector<MemHeap *> heaps;

      for (int t{}; t < n_threads; ++t) {
        heaps.push_back(mem_heap_create(16384));
      }

      const auto per = N_OPS / uint64_t(n_threads);
      const auto base = m_next.load();

      m_next += uint32_t(per * uint64_t(n_threads));

      const auto n_retries = blink::n_node_retries(*m_sys);

      (void)ut::bench_mt(name, n_threads, per, [&](int t, uint64_t i) {
        auto heap = heaps[size_t(t)];

        body(base, uint32_t(t), uint32_t(per), uint32_t(i), heap);

        mem_heap_empty(heap);
      });

      /* How often a read of a non-leaf page had to go again because of a writer. */
      std::printf("NODE_RETRIES %s/%dT %lu\n", name, n_threads, (unsigned long)(blink::n_node_retries(*m_sys) - n_retries));

      for (auto heap : heaps) {
        mem_heap_free(heap);
      }
    }
  }

  [[nodiscard]] uint32_t loaded_key(uint32_t t, uint64_t i) const noexcept {
    /* A different walk over the rows for each thread, none of them in the order of the keys. */
    return mix(uint32_t((uint64_t(t + 1) * 7919 + i * 104729) % m_n_loaded));
  }

  uint32_t m_n_loaded{};
  std::atomic<uint32_t> m_next{};
  std::atomic<uint32_t> m_top{};
};

/** Inserts all over the tree: each thread is on pages all the others are on, splits at every level. */
TEST_F(BlinkPerf, InsertRandom) {
  load(N_ROWS);

  each_thread_count("insert_random", [&](uint32_t base, uint32_t t, uint32_t per, uint32_t i, MemHeap *heap) {
    ut_a(put(mix(base + t * per + i), BLEN, heap) == DB_SUCCESS);
  });
}

/** Inserts at the end of the tree, by all the threads: the hottest page there is. */
TEST_F(BlinkPerf, InsertAppend) {
  load(N_ROWS);

  each_thread_count("insert_append", [&](uint32_t, uint32_t, uint32_t, uint32_t, MemHeap *heap) {
    /* Above what load() has: N_ROWS of the mix can land up there, so its keys stop short. */
    ut_a(put(UINT32_MAX - m_top.fetch_add(1), BLEN, heap) == DB_SUCCESS);
  });
}

/** Point selects: non-leaf pages S latched by everybody, the root above all. */
TEST_F(BlinkPerf, Select) {
  load(N_ROWS);

  each_thread_count("select", [&](uint32_t, uint32_t t, uint32_t, uint32_t i, MemHeap *heap) {
    ut_a(has(loaded_key(t, i), heap));
  });
}

/** One insert to nine selects: readers that have to get past writers. */
TEST_F(BlinkPerf, Mixed) {
  load(N_ROWS);

  each_thread_count("mixed", [&](uint32_t base, uint32_t t, uint32_t per, uint32_t i, MemHeap *heap) {
    if (i % 10 == 0) {
      ut_a(put(mix(base + t * per + i), BLEN, heap) == DB_SUCCESS);
    } else {
      ut_a(has(loaded_key(t, i), heap));
    }
  });
}

/** A tuple of one field of the given type, for the fingerprints. */
[[nodiscard]] DTuple *typed_key(MemHeap *heap, ulint mtype, ulint prtype, const void *data, ulint len) {
  auto t = dtuple_create(heap, 1);
  auto f = dtuple_get_nth_field(t, 0);

  dtype_set(dfield_get_type(f), mtype, prtype, len == UNIV_SQL_NULL ? 0 : len);

  if (len == UNIV_SQL_NULL) {
    dfield_set_null(f);
  } else {
    dfield_set_data(f, data, len);
  }

  return t;
}

/** Keys equal under the comparison have one fingerprint, different keys don't: CHAR and VARCHAR without
their trailing spaces, binary as it is, NULL apart from empty. */
TEST_F(Blink, LockKeyOf) {
  const auto fold = [&](ulint mtype, ulint prtype, const char *data, ulint len) {
    return lock::key_of(m_index, typed_key(m_heap, mtype, prtype, data, len)).m_fold;
  };

  const auto text = DATA_ENGLISH;
  const auto bin = dtype_form_prtype(0, DATA_CLIENT_BINARY_CHARSET_COLL);

  EXPECT_EQ(fold(DATA_VARCHAR, text, "ab", 2), fold(DATA_VARCHAR, text, "ab  ", 4)) << "the padding is no part of it";
  EXPECT_EQ(fold(DATA_CHAR, text, "ab", 2), fold(DATA_CHAR, text, "ab ", 3));
  EXPECT_NE(fold(DATA_VARCHAR, text, "ab", 2), fold(DATA_VARCHAR, text, "ac", 2));
  EXPECT_NE(fold(DATA_VARCHAR, text, "ab", 2), fold(DATA_VARCHAR, text, " ab", 3)) << "only trailing spaces";
  EXPECT_NE(fold(DATA_BINARY, bin, "ab", 2), fold(DATA_BINARY, bin, "ab ", 3)) << "binary isn't padded";
  EXPECT_NE(fold(DATA_BINARY, bin, "ab", 2), fold(DATA_BINARY, bin, "ab\0", 3));
  EXPECT_NE(fold(DATA_VARCHAR, text, nullptr, UNIV_SQL_NULL), fold(DATA_VARCHAR, text, "", 0)) << "NULL isn't empty";

  /* The edges: nothing, all padding, a zero byte where there's no padding. */
  EXPECT_EQ(fold(DATA_VARCHAR, text, "", 0), fold(DATA_VARCHAR, text, "   ", 3)) << "all padding is empty";
  EXPECT_EQ(fold(DATA_CHAR, text, "", 0), fold(DATA_CHAR, text, " ", 1));
  EXPECT_NE(fold(DATA_BINARY, bin, "", 0), fold(DATA_BINARY, bin, "\0", 1));
  EXPECT_NE(fold(DATA_VARCHAR, text, "a", 1), fold(DATA_VARCHAR, text, "a\0", 2)) << "a zero byte isn't padding";

  /* The longest key there can be, and one byte different at its end. */
  {
    std::string longest(blink::max_key_size(), 'k');
    auto other = longest;

    other.back() = 'l';

    EXPECT_NE(fold(DATA_VARCHAR, text, longest.data(), longest.size()), fold(DATA_VARCHAR, text, other.data(), other.size()));
    EXPECT_EQ(fold(DATA_VARCHAR, text, longest.data(), longest.size()), fold(DATA_VARCHAR, text, (longest + "  ").data(), longest.size() + 2));
  }

  /* Lengths that go over a word. */
  EXPECT_EQ(fold(DATA_VARCHAR, text, "abcdefghij", 10), fold(DATA_VARCHAR, text, "abcdefghij      ", 16));
  EXPECT_NE(fold(DATA_VARCHAR, text, "abcdefghij", 10), fold(DATA_VARCHAR, text, "abcdefghik", 10));

  /* Each fingerprint of 1000 keys is its own, and none is the end of the index. */
  std::set<uint64_t> folds;

  for (uint32_t k{}; k < 1000; ++k) {
    folds.insert(lock::key_of(m_index, key(k, m_heap)).m_fold);
    mem_heap_empty(m_heap);
  }

  EXPECT_EQ(folds.size(), 1000UL);
  EXPECT_EQ(folds.count(lock::end_of(m_index).m_fold), 0UL);
}

/** A record and its search key have one fingerprint, and so the lock of a search and the lock of a
change meet. The fingerprint is the index's: another index has another for the same key. */
TEST_F(Blink, LockKeyRec) {
  put_all(1, 500, 100);

  uint32_t n{};
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  while (cur.next(&mtr)) {
    const auto k = key_of(m_index, cur.rec());

    EXPECT_EQ(Btree::lock_key(m_index, cur.rec()), lock::key_of(m_index, key(k, m_heap))) << k;
    EXPECT_FALSE(cur.lock_pos(cur.rec()).m_fence);
    mem_heap_empty(m_heap);
    ++n;
  }

  cur.commit(&mtr);

  EXPECT_EQ(n, 500U);

  m_sys->m_dict->mutex_acquire();
  const auto other = m_sys->m_dict->m_sys_tables->get_first_index();
  m_sys->m_dict->mutex_release();

  EXPECT_NE(lock::key_of(other, key(7, m_heap)), lock::key_of(m_index, key(7, m_heap)));
}

/** The boundaries of the leaves for the locks: after a load in order, the fence of a page is its high
key, which is the key of the first record of the page to the right; the last page's fence is the end of
the index; after the last record of a page comes its fence. */
TEST_F(Blink, LockFence) {
  put_all(1, 3000, 300);

  const auto shape = validated();
  const auto &leaves = shape.m_levels[0];

  ASSERT_GE(leaves.size(), 3UL);

  for (size_t i{}; i < leaves.size(); ++i) {
    Mtr mtr;

    mtr.start();

    const auto block = m_sys->m_btree->block_get(m_index->get_space_id(), leaves[i], RW_B_LATCH, &mtr);
    const auto page = block->get_frame();
    const auto fence = Btree::lock_fence(m_index, block);
    const auto last = page_rec_get_prev(page_get_supremum_rec(page));

    EXPECT_EQ(Btree::lock_next(m_index, block, last), fence) << i;
    EXPECT_EQ(Btree::lock_pos(m_index, block, page_get_supremum_rec(page)).m_key, fence);
    EXPECT_TRUE(Btree::lock_pos(m_index, block, page_get_supremum_rec(page)).m_fence);
    EXPECT_EQ(Btree::lock_first(m_index, block), Btree::lock_key(m_index, page_rec_get_next(page_get_infimum_rec(page))));

    if (i + 1 == leaves.size()) {
      EXPECT_EQ(fence, lock::end_of(m_index)) << "the last page";
    } else {
      EXPECT_EQ(fence, Btree::lock_key(m_index, page_get_high_key(page)));

      const auto right = m_sys->m_btree->block_get(m_index->get_space_id(), leaves[i + 1], RW_B_LATCH, &mtr);

      EXPECT_EQ(fence, Btree::lock_first(m_index, right)) << "the high key is the first key on the right, " << i;
    }

    mtr.commit(*m_sys);
  }
}

/** For a merge: transactions that hold locks commit on every way out of a test, else the shutdown waits
for them. */
struct Commits {
  Commits(Blink &test) : m_test(test) {}

  ~Commits() {
    for (auto trx : m_trxs) {
      m_test.end(trx);
    }
  }

  Trx *begin() { return m_trxs.emplace_back(m_test.start_trx()); }

  Blink &m_test;
  std::vector<Trx *> m_trxs;
};

/** The first and the last key of a leaf. */
std::pair<uint32_t, uint32_t> Blink::page_keys(page_no_t page_no) const {
  std::pair<uint32_t, uint32_t> keys;

  on_page(page_no, [&](page_t *page, Mtr *) {
    keys.first = key_of(m_index, page_rec_get_next(page_get_infimum_rec(page)));
    keys.second = key_of(m_index, page_rec_get_prev(page_get_supremum_rec(page)));
  });

  return keys;
}

/** Delete [from, to], all of them there. */
void Blink::del_range(uint32_t from, uint32_t to) {
  for (auto k = from; k <= to; ++k) {
    ASSERT_TRUE(del(k, m_heap)) << k;
    mem_heap_empty(m_heap);
  }
}

/** A gap lock on a fence that a merge takes off still stops an insert into that gap: it's copied to the
boundary after it. The fence has no record of its own when it's locked (the first records on the right
went before), so only the merge can hand the lock on: a delete of a record hands on its own locks. */
TEST_F(Blink, LockMergeFence) {
  put_all(1, 3000, 300);

  const auto leaves = validated().m_levels[0];

  ASSERT_GE(leaves.size(), 4UL);

  LockKey fence;

  on_page(leaves[1], [&](page_t *, Mtr *mtr) {
    fence = Btree::lock_fence(m_index, m_sys->m_btree->block_get(m_index->get_space_id(), leaves[1], RW_B_LATCH, mtr));
  });

  const auto [left_first, left_last] = page_keys(leaves[1]);
  const auto [right_first, right_last] = page_keys(leaves[2]);

  ASSERT_EQ(right_first, left_last + 1);
  ASSERT_EQ(fence, lock::key_of(m_index, key(right_first, m_heap)));

  /* The right page without its first three: the fence is a key with no record now. Then the lock, then
two records left on the left and two after kept on the right, which makes the merge. */
  const auto kept = right_first + 3;

  ASSERT_NO_FATAL_FAILURE(del_range(right_first, right_first + 2));
  ASSERT_EQ(page_keys(leaves[2]).first, kept) << "no merge yet";

  Commits trxs{*this};
  auto holder = trxs.begin();

  ASSERT_EQ(m_sys->m_lock_sys->rec_lock_for(holder, LockMode(LOCK_X | LOCK_GAP), fence, m_index), DB_SUCCESS);

  ASSERT_NO_FATAL_FAILURE(del_range(left_first + 2, left_last));
  ASSERT_NO_FATAL_FAILURE(del_range(kept + 2, right_last));

  ASSERT_LT(validated().m_levels[0].size(), leaves.size()) << "a merge";

  /* An insert between the left records and kept, where the fence was: kept is its boundary now. */
  const auto next = lock::key_of(m_index, key(kept, m_heap));
  auto inserter = trxs.begin();

  LockSys::KeyLatch latch(*m_sys->m_lock_sys, next);

  EXPECT_NE(m_sys->m_lock_sys->rec_other_has_conflicting(next, LockMode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), inserter), nullptr)
    << "the gap lock went with the gap";
}

/** The edges of the fences: a tree of one page has the end of the index after its last record; an empty
tree has nothing but the end, its first boundary is that too. */
TEST_F(Blink, LockFenceRoot) {
  const auto root = [&](auto check) {
    on_page(m_index->get_page_no(), [&](page_t *, Mtr *mtr) {
      check(m_sys->m_btree->block_get(m_index->get_space_id(), m_index->get_page_no(), RW_B_LATCH, mtr));
    });
  };

  root([&](BufBlock *block) {
    ASSERT_EQ(page_get_n_recs(block->get_frame()), 0UL);
    EXPECT_EQ(Btree::lock_fence(m_index, block), lock::end_of(m_index));
    EXPECT_EQ(Btree::lock_first(m_index, block), lock::end_of(m_index)) << "an empty tree: the end is all there is";
  });

  put_all(1, 3, 100);

  root([&](BufBlock *block) {
    const auto page = block->get_frame();

    ASSERT_EQ(page_get_n_recs(page), 3UL);
    EXPECT_EQ(Btree::lock_next(m_index, block, page_rec_get_prev(page_get_supremum_rec(page))), lock::end_of(m_index));
    EXPECT_EQ(Btree::lock_next(m_index, block, page_get_infimum_rec(page)), Btree::lock_first(m_index, block));
  });
}

/** A merge whose right page kept its first record: the fence of the left page and that record are one
key, nothing moves, and the gap lock on it still stops an insert into the gap. */
TEST_F(Blink, LockMergeSameKey) {
  put_all(1, 3000, 300);

  const auto leaves = validated().m_levels[0];

  ASSERT_GE(leaves.size(), 4UL);

  const auto [left_first, left_last] = page_keys(leaves[1]);
  const auto [right_first, right_last] = page_keys(leaves[2]);
  const auto fence = lock::key_of(m_index, key(right_first, m_heap));

  Commits trxs{*this};
  auto holder = trxs.begin();

  ASSERT_EQ(m_sys->m_lock_sys->rec_lock_for(holder, LockMode(LOCK_X | LOCK_GAP), fence, m_index), DB_SUCCESS);

  ASSERT_NO_FATAL_FAILURE(del_range(left_first + 2, left_last));
  ASSERT_NO_FATAL_FAILURE(del_range(right_first + 3, right_last));

  ASSERT_LT(validated().m_levels[0].size(), leaves.size()) << "a merge";

  auto inserter = trxs.begin();

  LockSys::KeyLatch latch(*m_sys->m_lock_sys, fence);

  EXPECT_NE(m_sys->m_lock_sys->rec_other_has_conflicting(fence, LockMode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), inserter), nullptr);
}

/** A merge of the last page with nothing left on it: the gap below the fence goes to the end of the
index, the boundary after it now. The last page is emptied before the fence is locked, so that only the
merge hands the lock on. */
TEST_F(Blink, LockMergeToEnd) {
  put_all(1, 3000, 300);

  const auto leaves = validated().m_levels[0];

  ASSERT_GE(leaves.size(), 3UL);

  const auto [left_first, left_last] = page_keys(leaves[leaves.size() - 2]);
  const auto [last_first, last_last] = page_keys(leaves.back());
  const auto fence = lock::key_of(m_index, key(last_first, m_heap));

  ASSERT_EQ(last_last, 3000U);

  /* All of the last page: an empty page stays until the one before is small too. */
  ASSERT_NO_FATAL_FAILURE(del_range(last_first, last_last));
  ASSERT_EQ(validated().m_levels[0].size(), leaves.size()) << "no merge yet";

  Commits trxs{*this};
  auto holder = trxs.begin();

  ASSERT_EQ(m_sys->m_lock_sys->rec_lock_for(holder, LockMode(LOCK_X | LOCK_GAP), fence, m_index), DB_SUCCESS);

  ASSERT_NO_FATAL_FAILURE(del_range(left_first + 2, left_last));

  ASSERT_LT(validated().m_levels[0].size(), leaves.size()) << "a merge";

  const auto end = lock::end_of(m_index);
  auto inserter = trxs.begin();

  LockSys::KeyLatch latch(*m_sys->m_lock_sys, end);

  EXPECT_NE(m_sys->m_lock_sys->rec_other_has_conflicting(end, LockMode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), inserter), nullptr)
    << "the gap lock is at the end of the index";
}

/** What a key costs for its locks: the fingerprint of a search tuple by the length of the key, and of a
record on its page (with the offsets of its fields). Linear in the length, best of three. */
class LockKeyPerf : public Blink {};

TEST_F(LockKeyPerf, KeyOf) {
  const auto text = DATA_ENGLISH;
  const auto per_key = [&](ulint len) {
    std::string data(len, 'k');
    const auto t = typed_key(m_heap, DATA_VARCHAR, text, data.data(), len);
    double best{1e18};

    for (int round{}; round < 3; ++round) {
      auto r = ut::bench(std::format("key_of_varchar_{}", len), 200000, [&](uint64_t i) {
        /* A different key each time, the same length. */
        data[i % len] = char('a' + i % 26);
        ut::keep(lock::key_of(m_index, t).m_fold);
      });

      best = std::min(best, r.ns_per_op());
    }

    return best;
  };

  /* The tuple once, its value changed in place: the fingerprint is what's timed. */
  const auto t = key(0, m_heap);
  const auto int_data = static_cast<byte *>(dfield_get_data(dtuple_get_nth_field(t, 0)));
  double int_ns{1e18};

  for (int round{}; round < 3; ++round) {
    const auto r = ut::bench("key_of_int", 200000, [&](uint64_t i) {
      mach_write_to_4(int_data, uint32_t(i));
      ut::keep(lock::key_of(m_index, t).m_fold);
    });

    int_ns = std::min(int_ns, r.ns_per_op());
  }

  const auto short_key = per_key(32);
  const auto long_key = per_key(512);

  std::printf("KeyOf: int %.0f ns, 32 bytes %.0f ns, 512 bytes %.0f ns\n", int_ns, short_key, long_key);

  /* 16 times the bytes, not more than 32 times the cost: linear, with what's fixed. */
  EXPECT_LT(long_key, 32 * short_key);
}

TEST_F(LockKeyPerf, KeyOfRec) {
  put_all(1, 200, 100);

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);
  ASSERT_TRUE(cur.next(&mtr));

  const auto rec = cur.rec();
  auto r = ut::bench("lock_key_rec", 200000, [&](uint64_t) { ut::keep(Btree::lock_key(m_index, rec).m_fold); });

  cur.commit(&mtr);

  EXPECT_LT(r.ns_per_op(), 20000);
}

/** Runs fn at the point a batched insert has found its page, once. */
struct AtBatchFound {
  static inline std::atomic<bool> s_armed{};
  static inline std::function<void()> s_fn;

  static void hook(System &, blink::TestPoint point) {
    if (point == blink::TestPoint::BATCH_FOUND && s_armed.exchange(false)) {
      s_fn();
    }
  }
};

/** A batched insert that found its page waits for whoever owns it: in that time the page can be merged
away, and its number can be a new page of the tree, for other keys. The insert must not go there: the
page is kept from being freed until the insert is done, so it's dead when the insert is applied, and the
insert goes back to its thread for the normal way. Without that, it lands on the new page, below the
keys of that page. */
TEST_F(Blink, BatchPageReused) {
  m_sys->m_config.m_dict.m_insert_batch = true;

  /* Even keys: the odd ones are room for the insert. */
  for (uint32_t k{1}; k <= 3000; ++k) {
    ASSERT_EQ(put(k * 2, 300, m_heap), DB_SUCCESS);
    mem_heap_empty(m_heap);
  }

  const auto leaves = validated().m_levels[0];

  ASSERT_GE(leaves.size(), 4UL);

  const auto victim = leaves[2];
  const auto [left_first, left_last] = page_keys(leaves[1]);
  const auto [first, last] = page_keys(victim);
  const auto x = first + 1;
  bool reused{};

  AtBatchFound::s_fn = [&] {
    /* The insert holds the guard that keeps the victim from being freed: the tree has a page less than
    the segment until it's done. */
    m_holding_frees = true;

    /* Two records left on each: the victim is taken in by its left sibling, it's dead. */
    for (auto k = left_first + 4; k <= left_last; k += 2) {
      ASSERT_TRUE(del(k, m_heap)) << k;
      mem_heap_empty(m_heap);
    }

    for (auto k = first + 4; k <= last; k += 2) {
      ASSERT_TRUE(del(k, m_heap)) << k;
      mem_heap_empty(m_heap);
    }

    /* Appends, until a split takes the page, if it's free. */
    for (uint32_t k{7000}; k < 7000 + 2 * 3000 && !reused; k += 2) {
      ASSERT_EQ(put(k, 300, m_heap), DB_SUCCESS);
      mem_heap_empty(m_heap);

      if (k % 64 == 0) {
        const auto now = validated().m_levels[0];

        reused = std::find(now.begin(), now.end(), victim) != now.end() && page_keys(victim).first >= 7000;
      }
    }

    m_holding_frees = false;
  };

  blink::set_test_hook(*m_sys, AtBatchFound::hook);
  AtBatchFound::s_armed = true;

  IndexCursor cur{*m_sys};
  const auto err = cur.insert_batched(m_index, entry(x, 50, m_heap), DML_FLAGS, nullptr);

  blink::set_test_hook(*m_sys, nullptr);
  ASSERT_FALSE(HasFailure());

  EXPECT_FALSE(reused) << "the page was freed with an insert queued on it";
  EXPECT_EQ(err, DB_FAIL) << "the page it found is dead: the normal way";

  if (err == DB_FAIL) {
    ASSERT_EQ(put(x, 50, m_heap), DB_SUCCESS);
  }

  mem_heap_empty(m_heap);

  EXPECT_TRUE(has(x, m_heap));
  (void)validated();
}

/** The redo records of the B-link tree are applied by the tree: it registers them, the page format's
records stay with recovery, and a type past the last has nothing. */
TEST_F(Blink, RedoApply) {
  const auto &recv = *m_sys->m_recv;

  EXPECT_NE(recv.redo_apply(MLOG_PAGE_HIGH_KEY), nullptr);
  EXPECT_EQ(recv.redo_apply(MLOG_REC_INSERT), nullptr) << "the page format's own";
  EXPECT_EQ(recv.redo_apply(MlogType(MLOG_BIGGEST_TYPE + 1)), nullptr);

  /* The same again is no change: a second instance of the tree registers what the first did. */
  m_sys->m_recv->set_redo_apply(MLOG_PAGE_HIGH_KEY, recv.redo_apply(MLOG_PAGE_HIGH_KEY));
}

}  // namespace
