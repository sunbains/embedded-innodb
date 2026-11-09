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
/** @file unit-tests/test_blink.h
What the tests of the B-link tree share: a fixture with records of a table, and a validator that walks
every level of the tree. */

#pragma once


#include "ut0env.h"
#include "ut0perf.h"

#include "api0api.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "fsp0fsp.h"
#include "idx0am.h"
#include "idx0blink.h"
#include "lock0lock.h"
#include "log0recv.h"
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
  /** @return the first and the last key of a leaf. */
  [[nodiscard]] std::pair<uint32_t, uint32_t> page_keys(page_no_t page_no) const;

  /** Delete [from, to], all of them there. */
  void del_range(uint32_t from, uint32_t to);

 public:
  /** A transaction, and its commit, for helpers outside the fixture. */
  [[nodiscard]] Trx *start_trx() { return begin(); }

  void end(Trx *trx) { commit(trx); }

 protected:

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

}  // namespace
