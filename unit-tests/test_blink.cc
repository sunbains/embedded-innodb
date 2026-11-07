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
The B-link tree, idx0blink.h: through IndexAm, and the pages looked at directly. A validator
walks every level of the tree, and is used all over. */

#include "ut0env.h"
#include "ut0perf.h"

#include "api0api.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "fsp0fsp.h"
#include "idx0am.h"
#include "idx0blink.h"
#include "log0log.h"
#include "mach0data.h"
#include "page0page.h"
#include "pars0pars.h"
#include "rem0rec.h"
#include "row0sel.h"
#include "row0upd.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

#include <sys/mman.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <charconv>
#include <condition_variable>
#include <csignal>
#include <format>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

/** The redo context only: no undo log and locks. */
constexpr ulint DML_FLAGS = BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG;

/** The field of the record with b. */
constexpr ulint B_FIELD = 4;

/** What a walk of the tree finds. */
struct Shape {
  /** The root's level. */
  ulint m_height{};

  /** The pages on each level, left to right, leaf first. */
  std::vector<std::vector<page_no_t>> m_levels;

  /** The keys of the leaves in order. */
  std::vector<uint32_t> m_keys;

  /** The pages in all. */
  [[nodiscard]] size_t n_pages() const {
    size_t n{};

    for (const auto &level : m_levels) {
      n += level.size();
    }

    return n;
  }
};

/** The record's key, the first field of records and of high keys alike. */
[[nodiscard]] uint32_t key_of(const Index *index, Rec rec) {
  MemHeap *heap{};
  RecOffs offs = PhyRec(index, rec).get_col_offsets(nullptr, 1, &heap, Current_location());
  ulint len;
  const auto offset = offs.get_nth_field(0, &len);
  const auto k = uint32_t(mach_read_from_4(rec.get() + offset));

  mem_heap_free(heap);

  return k;
}

class Blink : public ut::DbTest {
 protected:
  /** @return the size of the buffer pool. */
  [[nodiscard]] virtual ulint pool_size() const { return m_config.m_buf_pool.m_size; }

  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_dict.m_index_am = IndexAmKind::BLINK;
    m_config.m_buf_pool.m_size = pool_size();

    start();

    open_index();

    m_heap = mem_heap_create(16384);
  }

  void TearDown() override {
    blink::set_test_hook(*m_sys, nullptr);
    mem_heap_free(m_heap);
    ut::DbTest::TearDown();
  }

  void open_index() {
    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(ut::TABLE)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  [[nodiscard]] static byte *u32(MemHeap *heap, uint32_t v) {
    auto p = static_cast<byte *>(mem_heap_alloc(heap, 4));

    mach_write_to_4(p, v);

    return p;
  }

  /** @return a search key for k. */
  [[nodiscard]] DTuple *key(uint32_t k, MemHeap *heap) const {
    auto t = dtuple_create(heap, 1);

    m_index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(heap, k), 4);
    dtuple_set_n_fields_cmp(t, 1);

    return t;
  }

  /** @return a record for k, with a b of blen bytes. */
  [[nodiscard]] DTuple *entry(uint32_t k, ulint blen, MemHeap *heap) const {
    const auto n = m_index->get_n_fields();
    auto t = dtuple_create(heap, n);

    m_index->copy_types(t, n);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(heap, k), 4);

    auto sys = mem_heap_zalloc(heap, DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, DATA_TRX_ID_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + DATA_TRX_ID_LEN, DATA_ROLL_PTR_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 3), u32(heap, k), 4);

    auto b = static_cast<byte *>(mem_heap_alloc(heap, blen));

    memset(b, 'y', blen);
    dfield_set_data(dtuple_get_nth_field(t, B_FIELD), b, blen);
    dtuple_set_n_fields_cmp(t, m_index->get_n_unique_in_tree());

    return t;
  }

  /** Insert a new record, as a caller does in one call: it never takes a second. */
  [[nodiscard]] DbErr put(uint32_t k, ulint blen, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};
    BigRec big{};

    auto e = entry(k, blen, heap);

    mtr.start();
    cur.seek_insert(m_index, e, Intent::Write, nullptr, &mtr);

    const auto err = cur.insert(Intent::Write, DML_FLAGS, e, &big, 0, nullptr, &mtr);

    cur.commit(&mtr);

    return err;
  }

  void put_all(uint32_t from, uint32_t to, ulint blen = 200) {
    for (auto k = from; k <= to; ++k) {
      ASSERT_EQ(put(k, blen, m_heap), DB_SUCCESS) << k;
      mem_heap_empty(m_heap);
    }
  }

  /** Remove the record for k.
  @return true if there was one. */
  [[nodiscard]] bool del(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, key(k, heap), Intent::Write, &mtr);

    if (found) {
      const auto err = cur.remove(Intent::Write, RB_NONE, &mtr);

      ut_a(err == DB_SUCCESS);
    }

    cur.commit(&mtr);

    return found;
  }

  [[nodiscard]] bool has(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, key(k, heap), Intent::Read, &mtr);

    cur.commit(&mtr);

    return found;
  }

  /** @return the page numbers the tree uses in its segments, as the file space sees it. */
  [[nodiscard]] ulint used_pages() const {
    Mtr mtr;
    ulint used_leaf{};
    ulint used_top{};

    mtr.start();

    m_sys->m_fsp->latch(m_index->get_space_id(), &mtr);

    auto root = m_sys->m_btree->seg_root(m_index, &mtr);

    (void)m_sys->m_fsp->fseg_n_reserved_pages(root + PAGE_HEADER + PAGE_BTR_SEG_LEAF, &used_leaf, &mtr);
    (void)m_sys->m_fsp->fseg_n_reserved_pages(root + PAGE_HEADER + PAGE_BTR_SEG_TOP, &used_top, &mtr);

    mtr.commit(*m_sys);

    return used_leaf + used_top;
  }

  /** Run fn on a page. */
  template <typename F>
  void on_page(page_no_t page_no, F fn) const {
    Mtr mtr;

    mtr.start();

    auto block = m_sys->m_btree->block_get(m_index->get_space_id(), page_no, RW_S_LATCH, &mtr);

    fn(block->get_frame(), &mtr);

    mtr.commit(*m_sys);
  }

  /** Walk every level and check all there is to check, for a tree nothing is changing.
  @param[out] shape             What it found. */
  void validate(Shape &shape) const {
    const auto root_no = m_index->get_page_no();

    on_page(root_no, [&](page_t *page, Mtr *mtr) {
      shape.m_height = m_sys->m_btree->page_get_level(page, mtr);
      EXPECT_EQ(Btree::page_get_next(page, mtr), FIL_NULL) << "a root is the only page on its level";
      EXPECT_EQ(Btree::page_get_prev(page, mtr), FIL_NULL) << "a root is the only page on its level";
    });

    shape.m_levels.assign(shape.m_height + 1, {});

    /* The children of the level above in order: what this level has to be. */
    std::vector<page_no_t> children{root_no};

    for (auto lv = shape.m_height + 1; lv-- > 0;) {
      auto &pages = shape.m_levels[lv];
      std::vector<page_no_t> next_children;

      /* The keys of the node pointers above: the bound below a page, and with it the high
      key of the page to the left of it. */
      ASSERT_FALSE(children.empty());

      auto page_no = children[0];
      page_no_t prev_no = FIL_NULL;
      bool has_prev_key{};
      uint32_t prev_key{};
      bool has_last{};
      uint32_t last{};

      /* The next node pointer of the level above, and whether the page on the left says this page
      has none: a split that a crash stopped. */
      size_t at{};
      bool unlinked{};

      while (page_no != FIL_NULL) {
        if (!unlinked) {
          ASSERT_LT(at, children.size()) << "level " << lv << " has more pages than node pointers";
          ASSERT_EQ(children[at], page_no) << "level " << lv << " page " << pages.size();
          ++at;
        }

        pages.push_back(page_no);

        page_no_t next_no{};

        on_page(page_no, [&](page_t *page, Mtr *mtr) {
          EXPECT_TRUE(page_validate(page, m_index)) << page_no;
          EXPECT_EQ(m_sys->m_btree->page_get_level(page, mtr), lv) << page_no;
          EXPECT_EQ(m_sys->m_btree->page_get_index_id(page), m_index->m_id) << page_no;
          EXPECT_EQ(Btree::page_get_prev(page, mtr), prev_no) << page_no;

          next_no = Btree::page_get_next(page, mtr);
          unlinked = page_right_unlinked(page);

          const auto high_key = page_get_high_key(page);

          EXPECT_EQ(high_key.is_null(), next_no == FIL_NULL) << "a page has a high key if it has a right sibling: " << page_no;

          bool has_high{};
          uint32_t high{};

          if (!high_key.is_null()) {
            has_high = true;
            high = key_of(m_index, high_key);

            EXPECT_EQ(high_key.get_n_fields(), m_index->get_n_unique_in_tree()) << page_no;
            EXPECT_LE(page_high_key_size(high_key), blink::max_key_size()) << page_no;

            if (has_prev_key) {
              EXPECT_LT(prev_key, high) << "high keys in order, page " << page_no;
            }
          }

          bool first_rec{true};

          for (auto rec = page_rec_get_next(page_get_infimum_rec(page)); !page_rec_is_supremum(rec); rec = page_rec_get_next(rec)) {
            const bool min_rec = lv > 0 && (rec.get_info_bits() & REC_INFO_MIN_REC_FLAG) != 0;

            /* On a non-leaf level only the first of the level has no key to go by. */
            EXPECT_EQ(min_rec, lv > 0 && first_rec && prev_no == FIL_NULL) << page_no;

            const auto k = key_of(m_index, rec);

            if (!min_rec) {
              if (has_last) {
                EXPECT_LT(last, k) << "order, page " << page_no;
              }

              /* A page holds keys >= the high key of its left sibling. */
              if (has_prev_key) {
                EXPECT_LE(prev_key, k) << "low bound, page " << page_no;
              }

              /* The separator is the first key of the page on a non-leaf level. */
              if (lv > 0 && first_rec && has_prev_key) {
                EXPECT_EQ(prev_key, k) << "separator, page " << page_no;
              }

              if (has_high) {
                EXPECT_LT(k, high) << "high key, page " << page_no;
              }

              has_last = true;
              last = k;
            }

            if (lv == 0) {
              shape.m_keys.push_back(k);
            } else {
              MemHeap *heap{};
              RecOffs offs = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

              next_children.push_back(m_sys->m_btree->node_ptr_get_child_page_no(rec, offs));

              mem_heap_free(heap);
            }

            first_rec = false;
          }

          /* Only a leaf can be empty. */
          if (lv > 0) {
            EXPECT_FALSE(first_rec) << "empty non-leaf page " << page_no;
          }

          has_prev_key = has_high;
          prev_key = high;
        });

        prev_no = page_no;
        page_no = next_no;
      }

      EXPECT_EQ(at, children.size()) << "level " << lv << ": every node pointer's page is on the level";

      children = std::move(next_children);
    }

    /* The file space and the tree agree: no page is lost or linked twice. Dead pages are in
    the segment until they're freed: a thread that merged can have left them for the next. */
    if (!m_holding_frees) {
      m_sys->am(m_index).quiesce();

      EXPECT_EQ(used_pages(), shape.n_pages());
    }
  }

  [[nodiscard]] Shape validated() const {
    Shape shape;

    validate(shape);

    return shape;
  }

  /** Dead pages are being kept: see blink::hold_frees(). */
  bool m_holding_frees{};

  Index *m_index{};
  MemHeap *m_heap{};
};

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

/** A scan at a level of a tree that has gone down past that level ends, it doesn't look for it. */
TEST_F(Blink, LevelGone) {
  put_all(1, 10, 100);

  Mtr mtr;
  BtreePcursor pcur{};

  pcur.get_btr_cur()->m_index = m_index;
  pcur.m_read_level = 1;
  pcur.m_old_stored = true;
  pcur.m_rel_pos = BtreeCursorPos::BEFORE_FIRST_IN_TREE;

  mtr.start();
  EXPECT_FALSE(m_sys->am(m_index).restore_tree_cursor(pcur, BTR_SEARCH_LEAF, &mtr, Current_location()));
  EXPECT_TRUE(pcur.get_btr_cur()->get_page_cur()->is_after_last());
  mtr.commit(*m_sys);
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

/** Fails instead of hanging when threads don't finish: a latch deadlock looks like that. */
class Watchdog {
 public:
  explicit Watchdog(std::chrono::seconds limit)
      : m_thread([this, limit]() {
          std::unique_lock<std::mutex> lock(m_mutex);

          if (!m_cond.wait_for(lock, limit, [this]() { return m_done; })) {
            std::fprintf(stderr, "Watchdog: threads didn't finish, a deadlock?\n");
            std::abort();
          }
        }) {}

  ~Watchdog() {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_done = true;
    }

    m_cond.notify_all();
    m_thread.join();
  }

 private:
  std::mutex m_mutex;
  std::condition_variable m_cond;
  bool m_done{};
  std::thread m_thread;
};

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

/** The test to run while developing: about a minute, concurrent, and at any page size. With small pages
(IB_PAGE_SIZE_SHIFT) and long keys a few hundred rows make a tree 3 or 4 levels deep, so splits, merges
and root changes happen all the time and the limits of a page are easy to reach. Sizes come from the page,
the depth from growing the tree. What's chosen comes from one seed, printed: IB_TEST_SEED. How long:
IB_TEST_SECS. */
class BlinkChurn : public Blink {
 protected:
  static constexpr const char *NAME = "test/churn";
  static constexpr ulint KEY_COL_LEN{768};
  static constexpr ulint B_COL_LEN{8000};
  static constexpr uint32_t N_WRITERS{8};
  static constexpr uint32_t N_STABLE{64};

  /* Fields of a clustered record. */
  static constexpr ulint F_KEY{0};
  static constexpr ulint F_B{3};

  /** Keys that are multiples of this are stable, the digit below it says whose the rest are. */
  static constexpr uint32_t SPAN{1024};

  /** Past a stable key: the keys below this are the long ones of the churn, the rest are the short
  ones of the hot page. */
  static constexpr uint32_t NORMAL{256};

  /** Digits at the start of a key: they order the keys, what's after them is padding. */
  static constexpr ulint N_DIGITS{10};

  void create_table() {
    api::TableSchema *tbl_sch{};
    api::IndexSchema *idx_sch{};

    ASSERT_EQ(api::TableSchema::create(NAME, &tbl_sch, IB_TBL_V1), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_col("k", IB_VARCHAR, IB_COL_NONE, 0, KEY_COL_LEN), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_col("b", IB_VARCHAR, IB_COL_NONE, 0, B_COL_LEN), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);
    ASSERT_EQ(idx_sch->add_col("k", 0), DB_SUCCESS);
    ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

    auto trx = begin();
    uint64_t table_id{};

    ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
    ASSERT_EQ(api::table_create(*m_sys, trx, tbl_sch, &table_id), DB_SUCCESS);
    commit(trx);

    api::TableSchema::destroy(*m_sys, tbl_sch);

    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  /** @return the largest record's worth of key and b together, that stays on the page. */
  [[nodiscard]] static ulint largest() noexcept { return page_get_free_space_of_empty() / 2 - 64; }

  /** @return the length of the key of k. Always the same for a k: short ones, and the longest the
  column and the page allow, that make the fan-out small. */
  [[nodiscard]] static ulint key_len(uint32_t k) noexcept {
    const auto longest = std::min(KEY_COL_LEN, blink::max_key_size() - 64);

    /* Short, for many records on the leaf that all the writers are on. */
    if (k % SPAN == 0 || k % SPAN >= NORMAL) {
      return N_DIGITS;
    }

    switch ((k * 2654435761U >> 24) % 4) {
      case 0:
        return longest;
      case 1:
        return longest - 1;
      case 2:
        return longest / 2;
      default:
        return N_DIGITS + (k * 2654435761U >> 20) % (longest - N_DIGITS);
    }
  }

  [[nodiscard]] static byte *key_bytes(uint32_t k, MemHeap *heap, ulint &len) noexcept {
    len = key_len(k);

    auto p = static_cast<byte *>(mem_heap_alloc(heap, len));

    memset(p, 'x', len);

    (void)std::format_to_n(reinterpret_cast<char *>(p), N_DIGITS, "{:010}", k);

    return p;
  }

  [[nodiscard]] DTuple *ckey(uint32_t k, MemHeap *heap) const {
    auto t = dtuple_create(heap, 1);
    ulint len;
    const auto p = key_bytes(k, heap, len);

    m_index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), p, len);
    dtuple_set_n_fields_cmp(t, 1);

    return t;
  }

  [[nodiscard]] DTuple *centry(uint32_t k, ulint blen, MemHeap *heap) const {
    const auto n = m_index->get_n_fields();
    auto t = dtuple_create(heap, n);
    ulint len;
    const auto p = key_bytes(k, heap, len);

    m_index->copy_types(t, n);
    dfield_set_data(dtuple_get_nth_field(t, F_KEY), p, len);

    auto sys = mem_heap_zalloc(heap, DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, DATA_TRX_ID_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + DATA_TRX_ID_LEN, DATA_ROLL_PTR_LEN);

    auto b = static_cast<byte *>(mem_heap_alloc(heap, std::max<ulint>(blen, 1)));

    memset(b, 'y', blen);
    dfield_set_data(dtuple_get_nth_field(t, F_B), b, blen);
    dtuple_set_n_fields_cmp(t, m_index->get_n_unique_in_tree());

    return t;
  }

  [[nodiscard]] uint32_t ckey_of(Rec rec, MemHeap *heap) const {
    const auto offs = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;
    const auto data = rec.get_nth_field(offs, F_KEY, &len);

    ut_a(len >= N_DIGITS);

    uint32_t k{};

    (void)std::from_chars(reinterpret_cast<const char *>(data.get()), reinterpret_cast<const char *>(data.get()) + N_DIGITS, k);

    return k;
  }

  /** Insert the record for k, by the batch or the normal way, as it takes.
  @return true if it went in. */
  [[nodiscard]] bool cput(uint32_t k, ulint blen, MemHeap *heap, bool batched) {
    const auto e = centry(k, blen, heap);
    IndexCursor cur{*m_sys};

    if (batched) {
      const auto err = cur.insert_batched(m_index, e, DML_FLAGS, nullptr);

      if (err != DB_FAIL) {
        return err == DB_SUCCESS;
      }

      m_n_refused.fetch_add(1, std::memory_order_relaxed);
    }

    Mtr mtr;
    BigRec big{};

    mtr.start();
    cur.seek_insert(m_index, e, Intent::Write, nullptr, &mtr);

    const auto err = cur.insert(Intent::Write, DML_FLAGS, e, &big, 0, nullptr, &mtr);

    cur.commit(&mtr);

    return err == DB_SUCCESS;
  }

  [[nodiscard]] bool cdel(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, ckey(k, heap), Intent::Write, &mtr);

    if (found) {
      ut_a(cur.remove(Intent::Write, RB_NONE, &mtr) == DB_SUCCESS);
    }

    cur.commit(&mtr);

    return found;
  }

  [[nodiscard]] bool chas(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, ckey(k, heap), Intent::Read, &mtr);

    cur.commit(&mtr);

    return found;
  }

  void fail(const std::string &what) {
    ++m_errors;

    std::fprintf(stderr, "%s\n", what.c_str());
  }

  /** The size of b at a limit of the page, for a key of this length: nothing, a byte, the largest
  record or a byte less, and what makes pages of a few records. */
  [[nodiscard]] static ulint boundary_size(std::mt19937 &rng, ulint klen) noexcept {
    const auto empty = page_get_free_space_of_empty();
    const auto top = largest() - klen;
    const auto fit = [&](ulint want) { return want > klen + 64 ? want - klen - 64 : ulint{0}; };

    switch (rng() % 8) {
      case 0:
        return 0;
      case 1:
        return 1;
      case 2:
        return top;
      case 3:
        return top - 1;
      case 4:
        return fit(empty / 3);
      case 5:
        return fit(empty / 4) + rng() % 3;
      case 6:
        return fit(empty / 8);
      default:
        return rng() % top;
    }
  }

  /** @return a key of this writer: right after a stable key, right before the next, or between. */
  [[nodiscard]] static uint32_t writer_key(std::mt19937 &rng, uint32_t w) noexcept {
    const auto base = uint32_t(rng() % N_STABLE + 1) * SPAN;
    const auto slots = (NORMAL - 1) / N_WRITERS;
    uint32_t slot;

    switch (rng() % 4) {
      case 0:
        slot = 0;
        break;
      case 1:
        slot = slots - 1;
        break;
      default:
        slot = uint32_t(rng() % slots);
        break;
    }

    return base + slot * N_WRITERS + 1 + w;
  }

  /** An operation of a writer while the writers are all after the keys past the stable key hot: mostly
  batched inserts on a page that also splits, and when going down merges, under the ones that wait. */
  void hot_op(std::mt19937 &rng, uint32_t w, uint32_t hot, bool grow, std::set<uint32_t> &keys, MemHeap *heap) {
    const auto slots = (SPAN - NORMAL - 1) / N_WRITERS;
    const auto k = hot + NORMAL + uint32_t(rng() % slots) * N_WRITERS + 1 + w;

    if (keys.count(k) == 0) {
      if (!cput(k, rng() % 16, heap, true)) {
        fail(std::format("hot insert of {} failed", k));
      }

      keys.insert(k);

    } else if (rng() % (grow ? 8 : 2) == 0) {
      if (!cdel(k, heap)) {
        fail(std::format("hot {} wasn't there to delete", k));
      }

      keys.erase(k);

    } else {
      /* A key that's there: a batch never decides that, it comes back with nothing done. */
      IndexCursor cur{*m_sys};

      if (const auto err = cur.insert_batched(m_index, centry(k, 8, heap), DML_FLAGS, nullptr); err != DB_FAIL) {
        fail(std::format("batched insert of {}, already there: {}", k, int(err)));
      }
    }
  }

  void writer_loop(uint32_t w, uint32_t seed, std::set<uint32_t> &keys) {
    std::mt19937 rng(seed + w);
    auto heap = mem_heap_create(UNIV_PAGE_SIZE * 2);

    while (!m_stop.load(std::memory_order_relaxed)) {
      /* Growing or shrinking, in turns, for the root to go up and down. */
      const bool grow = (m_phase.load(std::memory_order_relaxed) & 1) == 0;

      if (const auto hot = m_hot.load(std::memory_order_relaxed); hot != 0) {
        hot_op(rng, w, hot, grow, keys, heap);
        mem_heap_empty(heap);
        m_n_ops.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      auto k = writer_key(rng, w);

      /* What's there to delete, near where it was going: a random key mostly isn't, on a tree that's
      going down. */
      if (!grow && !keys.empty()) {
        const auto at = keys.lower_bound(k);

        k = at != keys.end() ? *at : *keys.begin();
      }

      if (keys.count(k) == 0) {
        if (grow || rng() % 8 == 0) {
          if (!cput(k, boundary_size(rng, key_len(k)), heap, rng() % 2 == 0)) {
            fail(std::format("insert of {} failed", k));
          }

          keys.insert(k);
        }
      } else if (!grow || rng() % 8 == 0) {
        if (!cdel(k, heap)) {
          fail(std::format("{} wasn't there to delete", k));
        }

        keys.erase(k);
      }

      mem_heap_empty(heap);
      m_n_ops.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** A scan that holds nothing: all the stable keys, once each, in order. */
  void scan_loop(uint32_t id) {
    auto heap = mem_heap_create(16384);

    for (uint32_t pass{id}; !m_stop.load(std::memory_order_relaxed); ++pass) {
      const bool forward = pass % 2 == 0;
      uint32_t expected = forward ? SPAN : N_STABLE * SPAN;
      uint32_t last = forward ? 0 : UINT32_MAX;
      uint32_t n_seen{};
      Mtr mtr;
      IndexCursor cur{*m_sys};

      mtr.start();

      if (forward) {
        cur.seek_first(m_index, Intent::Snapshot, &mtr);
      } else {
        cur.seek_last(m_index, Intent::Snapshot, &mtr);
      }

      while (forward ? cur.next(&mtr) : cur.prev(&mtr)) {
        const auto k = ckey_of(cur.rec(), heap);

        mem_heap_empty(heap);

        if (forward ? k <= last : k >= last) {
          fail(std::format("scan out of order: {} then {}", last, k));
          break;
        }

        last = k;

        if (k % SPAN == 0) {
          if (k != expected) {
            fail(std::format("scan wanted stable key {}, got {}", expected, k));
            break;
          }

          ++n_seen;

          expected = forward ? expected + SPAN : expected - SPAN;
        }
      }

      cur.commit(&mtr);

      if (n_seen != N_STABLE) {
        fail(std::format("a scan saw {} stable keys", n_seen));
      }

      m_n_scans.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** Point reads and in-place changes of stable records, by their group's bit alone. */
  void mark_loop(uint32_t id, uint32_t seed) {
    std::mt19937 rng(seed + 100 + id);
    auto heap = mem_heap_create(16384);

    while (!m_stop.load(std::memory_order_relaxed)) {
      const auto k = uint32_t(rng() % N_STABLE + 1) * SPAN;

      {
        Mtr mtr;
        IndexCursor cur{*m_sys};

        mtr.start();

        if (!cur.find(m_index, ckey(k, heap), Intent::Mark, &mtr)) {
          fail(std::format("stable key {} is gone", k));
        } else {
          /* Bytes of the record that a read has to see together. Nothing on disk: both go. */
          BufBlock::RecWrite write{cur.block()};
          auto rec = cur.rec();

          rec.set_deleted_flag(true);
          rec.set_deleted_flag(false);
        }

        cur.commit(&mtr);
      }

      if (!chas(k, heap)) {
        fail(std::format("stable key {} can't be read", k));
      }

      mem_heap_empty(heap);
      m_n_reads.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** @return the level of the root. */
  [[nodiscard]] ulint root_level() const {
    return blink::read_node(*m_sys, m_index, m_index->get_page_no(), nullptr, 0, blink::Edge::FIRST, ULINT_MAX).m_level;
  }

  std::atomic<bool> m_stop{};
  std::atomic<uint32_t> m_phase{};

  /** The stable key that the writers are all after, 0 for none. */
  std::atomic<uint32_t> m_hot{};
  std::atomic<uint64_t> m_n_refused{};
  std::atomic<uint64_t> m_n_ops{};
  std::atomic<uint64_t> m_n_scans{};
  std::atomic<uint64_t> m_n_reads{};
  std::atomic<uint64_t> m_errors{};
};

TEST_F(BlinkChurn, Run) {
  const auto env = [](const char *name, uint32_t otherwise) {
    const auto value = std::getenv(name);

    return value != nullptr ? uint32_t(std::strtoul(value, nullptr, 10)) : otherwise;
  };

  const auto seed = env("IB_TEST_SEED", std::random_device{}());
  const auto secs = env("IB_TEST_SECS", 60);

  std::printf("CHURN seed=%u secs=%u page=%lu\n", seed, secs, ulint(UNIV_PAGE_SIZE));

  ASSERT_NO_FATAL_FAILURE(create_table());

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    ASSERT_TRUE(cput(i * SPAN, 8, m_heap, false));
    mem_heap_empty(m_heap);
  }

  Watchdog dog{std::chrono::seconds(secs) + WATCHDOG};
  std::vector<std::set<uint32_t>> models(N_WRITERS);
  std::vector<std::thread> threads;

  for (uint32_t w{}; w < N_WRITERS; ++w) {
    threads.emplace_back([&, w] { writer_loop(w, seed, models[w]); });
  }

  for (uint32_t i{}; i < 2; ++i) {
    threads.emplace_back([&, i] { scan_loop(i); });
    threads.emplace_back([&, i] { mark_loop(i, seed); });
  }

  /* Watch the depth, and turn the writers around so that it keeps going up and down. How deep is for
the page size: the same long keys are a bigger fan-out on a bigger page. */
  const ulint deep = UNIV_PAGE_SIZE <= 4096 ? 3 : 2;
  ulint low{ULINT_UNDEFINED};
  ulint high{};
  const auto start = std::chrono::steady_clock::now();
  std::mt19937 hot_rng(seed + 200);

  for (uint32_t tick{}; std::chrono::steady_clock::now() - start < std::chrono::seconds(secs); ++tick) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    /* A fifth of a second on one page out of every 0.8: the rest of the time is for the depth. */
    if (tick % 16 == 0) {
      m_hot = uint32_t(hot_rng() % N_STABLE + 1) * SPAN;
    } else if (tick % 16 == 4) {
      m_hot = 0;
    }

    const auto level = root_level();

    low = std::min(low, level);
    high = std::max(high, level);

    if (const auto phase = m_phase.load(); phase % 2 == 0 ? level >= deep : level <= 1) {
      m_phase.store(phase + 1);
    }
  }

  m_stop = true;

  for (auto &thread : threads) {
    thread.join();
  }

  std::printf(
    "CHURN ops=%lu scans=%lu reads=%lu phases=%u height=%lu..%lu batched=%lu refused=%lu retries=%lu errors=%lu\n", ulint(m_n_ops.load()),
    ulint(m_n_scans.load()), ulint(m_n_reads.load()), m_phase.load(), low, high, ulint(blink::n_batched(*m_sys)), ulint(m_n_refused.load()),
    ulint(blink::n_node_retries(*m_sys)), ulint(m_errors.load())
  );

  EXPECT_EQ(m_errors.load(), 0UL) << "seed " << seed;
  EXPECT_GE(high, deep) << "a deep tree, seed " << seed;
  EXPECT_GE(m_phase.load(), 2U) << "the root went up and down, seed " << seed;
  EXPECT_GT(m_n_scans.load(), 0UL);

  if (m_sys->m_config.m_dict.m_insert_batch) {
    EXPECT_GT(blink::n_batched(*m_sys), 0UL) << "somebody did the work of somebody else, seed " << seed;
  }

  /* All of them, and nothing else, and the tree is what it should be. */
  std::set<uint32_t> expected;

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    expected.insert(i * SPAN);
  }

  for (const auto &model : models) {
    expected.insert(model.begin(), model.end());
  }

  std::vector<uint32_t> found;

  {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);

    while (cur.next(&mtr)) {
      found.push_back(ckey_of(cur.rec(), m_heap));
      mem_heap_empty(m_heap);
    }

    cur.commit(&mtr);
  }

  EXPECT_EQ(found, std::vector<uint32_t>(expected.begin(), expected.end())) << "seed " << seed;
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr)) << "seed " << seed;
}

/** The catalog says what an index is, not the configuration: a database has both kinds, and each
goes on as what it is through restarts, a crash and a drop under either setting. */
class MixedAm : public BlinkChurn {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_dict.m_index_am = IndexAmKind::BTREE;

    start();

    m_heap = mem_heap_create(16384);
  }

  void start_as(IndexAmKind kind) {
    stop();
    m_config.m_dict.m_index_am = kind;
    start(false);
  }

  void open_churn() {
    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  [[nodiscard]] bool test_table_is_blink() {
    m_sys->m_dict->mutex_acquire();

    const auto table = m_sys->m_dict->table_get(ut::TABLE);
    bool all{true};
    bool none{true};

    for (auto index : table->m_indexes) {
      all = all && index->is_blink();
      none = none && !index->is_blink();
    }

    m_sys->m_dict->mutex_release();

    ut_a(all || none);

    return all;
  }

  [[nodiscard]] bool sys_tables_are_blink() {
    m_sys->m_dict->mutex_acquire();

    const auto blink = m_sys->m_dict->m_sys_tables->get_first_index()->is_blink();

    m_sys->m_dict->mutex_release();

    return blink;
  }

  [[nodiscard]] std::set<uint32_t> churn_keys() {
    std::set<uint32_t> keys;
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);

    while (cur.next(&mtr)) {
      keys.insert(ckey_of(cur.rec(), m_heap));
      mem_heap_empty(m_heap);
    }

    cur.commit(&mtr);

    return keys;
  }

  void more_rows(ut::Rows &rows, uint32_t from, uint32_t n) {
    auto trx = begin();

    for (uint32_t k{from}; k < from + n; ++k) {
      const ut::Row row{k, k * 2, ut::blob(k)};

      ASSERT_EQ(insert(trx, row), DB_SUCCESS);
      rows[k] = row;
    }

    commit(trx);
  }

  void more_churn(std::set<uint32_t> &keys, uint32_t from, uint32_t n) {
    for (uint32_t k{from}; k < from + n; ++k) {
      ASSERT_TRUE(cput(k * 7 + 1, 40, m_heap, k % 2 == 0));
      mem_heap_empty(m_heap);
      keys.insert(k * 7 + 1);
    }
  }
};

TEST_F(MixedAm, Run) {
  ut::Rows rows;
  std::set<uint32_t> keys;

  ASSERT_FALSE(sys_tables_are_blink());
  ASSERT_FALSE(test_table_is_blink());
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 1, 500));

  /* New indexes are B-link, what was there isn't. */
  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BLINK));
  EXPECT_FALSE(sys_tables_are_blink());
  EXPECT_FALSE(test_table_is_blink());
  expect_rows(rows);

  ASSERT_NO_FATAL_FAILURE(create_table());
  EXPECT_TRUE(m_index->is_blink());
  EXPECT_TRUE(m_sys->m_dict->m_sys_tables->get_first_index()->is_blink() == false);
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 1000, 500));
  ASSERT_NO_FATAL_FAILURE(more_churn(keys, 1, 400));
  expect_rows(rows);
  EXPECT_EQ(churn_keys(), keys);

  /* The setting back: they stay what they are. */
  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BTREE));
  open_churn();
  EXPECT_FALSE(test_table_is_blink());
  EXPECT_TRUE(m_index->is_blink());
  expect_rows(rows);
  EXPECT_EQ(churn_keys(), keys);
  ASSERT_NO_FATAL_FAILURE(more_churn(keys, 1000, 400));
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 2000, 200));
  EXPECT_EQ(churn_keys(), keys);
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));

  /* A crash with both in the log, recovery has no index to ask. */
  crash([&] {
    open_churn();

    auto trx = begin();

    for (uint32_t k{5000}; k < 5200; ++k) {
      ut_a(cput(k * 7 + 1, 40, m_heap, false));
      mem_heap_empty(m_heap);
    }

    commit(trx);
  });

  open_churn();
  EXPECT_TRUE(m_index->is_blink());
  expect_rows(rows);

  const auto after = churn_keys();

  EXPECT_TRUE(std::includes(after.begin(), after.end(), keys.begin(), keys.end())) << "what was there before the crash";
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));

  /* A drop by what the catalog says, with the setting the other way. */
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, NAME), DB_SUCCESS);
  commit(trx);

  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BLINK));
  expect_rows(rows);

  m_sys->m_dict->mutex_acquire();
  EXPECT_EQ(m_sys->m_dict->table_get(NAME), nullptr);
  m_sys->m_dict->mutex_release();
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

}  // namespace
