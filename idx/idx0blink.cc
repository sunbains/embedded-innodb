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

/** @file idx/idx0blink.cc
The Lehman & Yao B-link tree: see idx0blink.h for the protocol. */

#include "idx0blink.h"
#include "idx0blinkint.h"

#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0systrx.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "srv0sys.h"
#include "trx0trx.h"

#include <array>
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <cstdlib>
#include <vector>

namespace blink {

namespace {

/** What PAGE_INDEX_ID says on a page a merge took off the tree: see the protocol in the header. */
constexpr uint64_t DEAD_INDEX_ID = ~uint64_t{};

/** A page that was merged away and is waiting to be freed. */
struct DeadPage {
  /** A crash gives the page back, by its compensation. */
  std::unique_ptr<SysTrx> m_trx;

  space_id_t m_space{};
  page_no_t m_root{};
  page_no_t m_page{};

  /** Where the segment header is on the root. */
  ulint m_seg{};

  /** The grace periods started when it was taken off the tree. */
  uint64_t m_flips{};
};

/**
 * Tells when nobody can hold the number of a page that's off the tree. A descent holds page
 * numbers with no latch: it's in a guard for as long as it does. A page goes back to the file
 * when every guard that was in when it went off the tree has left. As SRCU: the guards count on
 * one of two sides, a flip makes new guards count on the other side, and it takes two flips with
 * the side being left empty. Nothing here waits: a thread that tries to free and can't leaves the
 * page for the next one. It can hold latches a guard is waiting for.
 */
class Drain {
 public:
  struct Stripe {
    alignas(64) std::atomic<uint64_t> m_n[2]{};
  };

  class Guard {
   public:
    explicit Guard(Drain &drain) noexcept : m_stripe(drain.stripe()) {
      for (;;) {
        const auto flips = drain.m_flips.load();

        m_side = flips & 1;
        m_stripe.m_n[m_side].fetch_add(1);

        if (drain.m_flips.load() == flips) {
          return;
        }

        /* It counted on a side that a flip can be waiting for: not for long. */
        m_stripe.m_n[m_side].fetch_sub(1);
      }
    }

    ~Guard() noexcept { m_stripe.m_n[m_side].fetch_sub(1); }

    Guard(const Guard &) = delete;
    Guard &operator=(const Guard &) = delete;

   private:
    Stripe &m_stripe;
    uint32_t m_side{};
  };

  /** A page is about to go off the tree. Before it's visible: a Path read after this sees
  the tree without the page. */
  void note_unlink() noexcept { m_unlinks.fetch_add(1); }

  /** @return what a Path has to remember to know that none of its pages have left the tree. */
  [[nodiscard]] uint64_t unlinks() const noexcept { return m_unlinks.load(); }

  /** Add a page that's off the tree. The mini-transaction that did it has committed. */
  void add(DeadPage &&page) noexcept {
    std::lock_guard lock{m_mutex};

    page.m_flips = m_flips.load();
    m_pages.push_back(std::move(page));
    m_n_pages.store(m_pages.size());
  }

  /** Free what can be freed, if nobody else is. Nothing is waited for but the tablespace latch. */
  void try_free(System &sys) noexcept {
    if (m_n_pages.load() == 0) {
      return;
    }

    std::unique_lock lock{m_mutex, std::try_to_lock};

    if (!lock.owns_lock()) {
      return;
    }

    while (!m_pages.empty() && try_flip()) {
      const auto flips = m_flips.load();

      /* In order of m_flips: the front is the first to be ready. */
      while (!m_pages.empty() && flips >= m_pages.front().m_flips + 2) {
        free_page(sys, m_pages.front());
        m_pages.pop_front();
      }
    }

    m_n_pages.store(m_pages.size());
  }

  /** The instance is going away. What's still here is in the log, for restart. */
  void release_all() noexcept {
    for (auto &page : m_pages) {
      (void)page.m_trx.release();
    }

    m_pages.clear();
  }

  /** The tree is going away: first the pages that died, now, each in its mini-transaction with the
  end of its system transaction. Nobody can be reading a tree that's dropped, so there's nothing to
  wait for. Left to the segment's free, a crash in between would have them neither free nor on a
  system transaction. */
  void forget(System &sys, space_id_t space, page_no_t root) noexcept {
    std::lock_guard lock{m_mutex};

    std::erase_if(m_pages, [&](DeadPage &page) {
      if (page.m_space != space || page.m_root != root) {
        return false;
      }

      free_page(sys, page);

      return true;
    });

    m_n_pages.store(m_pages.size());
  }

 private:
  [[nodiscard]] Stripe &stripe() noexcept { return m_stripes[std::hash<std::thread::id>{}(std::this_thread::get_id()) % m_stripes.size()]; }

  /** @return true if there was a flip: no guard is left on the side that new guards go to. */
  [[nodiscard]] bool try_flip() noexcept {
    const auto flips = m_flips.load();
    const auto side = (flips + 1) & 1;

    for (const auto &stripe : m_stripes) {
      if (stripe.m_n[side].load() != 0) {
        return false;
      }
    }

    m_flips.store(flips + 1);

    return true;
  }

  /** In a mini-transaction of its own, with the end of its system transaction. No latch on the
  root: see Btree::seg_root() and the proof in the header. */
  static void free_page(System &sys, DeadPage &page) noexcept {
    Mtr mtr;

    mtr.start();

    const auto block = sys.m_btree->block_get(page.m_space, page.m_page, RW_B_LATCH, &mtr);

    buf_block_modify_clock_inc(block);

    sys.m_fsp->latch(page.m_space, &mtr);

    BufPool::Request req{
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = {page.m_space, page.m_root},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    const auto root = sys.m_buf_pool->get(req, nullptr)->get_frame();

    sys.m_fsp->fseg_free_page(root + page.m_seg, page.m_space, page.m_page, &mtr);

    page.m_trx->commit(&mtr);
  }

  std::array<Stripe, 64> m_stripes{};
  std::atomic<uint64_t> m_flips{};
  std::atomic<uint64_t> m_unlinks{};

  /** Covers m_pages, and what's being freed: forget() has to wait for that. */
  std::mutex m_mutex;
  std::deque<DeadPage> m_pages;
  std::atomic<size_t> m_n_pages{};
};

/** What the trees of an instance share. */
struct Shared {
  Drain m_drain;

#ifdef UNIT_TESTING
  std::atomic<TestHook> m_hook{};
  std::atomic<bool> m_no_merge{};
  std::atomic<bool> m_hold_frees{};

  /** The times a read of a non-leaf page had to go again: there was a writer. */
  std::atomic<uint64_t> m_n_node_retries{};

  /** The inserts done by a thread for another one. */
  std::atomic<uint64_t> m_n_batched{};
#endif /* UNIT_TESTING */
};

[[nodiscard]] Shared &shared(System &sys) noexcept;

[[nodiscard]] inline bool merge_is_on(System &sys) noexcept {
#ifdef UNIT_TESTING
  return !shared(sys).m_no_merge.load();
#else
  (void)sys;

  return true;
#endif /* UNIT_TESTING */
}

inline void free_dead_pages(System &sys) noexcept;

#ifdef UNIT_TESTING
inline void test_point(System &sys, TestPoint point) noexcept {
  if (const auto hook = shared(sys).m_hook.load(); hook != nullptr) {
    hook(sys, point);
  }
}
#else
inline void test_point(System &, TestPoint) noexcept {}
#endif /* UNIT_TESTING */

inline void free_dead_pages(System &sys) noexcept {
#ifdef UNIT_TESTING
  if (shared(sys).m_hold_frees.load()) {
    return;
  }
#endif /* UNIT_TESTING */

  shared(sys).m_drain.try_free(sys);
}

}  // namespace

/** Compare key with the high key of a page.
@return what cmp_dtuple_rec() does. */
[[nodiscard]] int cmp_high_key(const Index *index, const DTuple *key, Rec high_key) noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};
  MemHeap *heap{};

  rec_offs_init(offs_);

  const RecOffs offs = PhyRec(index, high_key).get_col_offsets(offs_.data(), high_key.get_n_fields(), &heap, Current_location());
  const auto cmp = cmp_dtuple_rec(index->m_cmp_ctx, key, high_key, offs.get());

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return cmp;
}

/** @return true if a search in this mode has to go on to the right sibling of a page.

Keys on a page are < its high key. Equal to the key in the compared fields, a search for a
prefix can still have what it looks for on this page:
 - G and LE want what's after the prefix or the last of it: never here.
 - GE wants the first with the prefix, and L what's before it: they may be here, and the
   search on the page will tell, GE moves on from the supremum. A non-leaf page in these
   modes takes the same decision, its first node pointer is then < key: never the infimum.
@param[in] mode                 The mode of the search on the leaf. */
[[nodiscard]] bool past_high_key(const Index *index, const page_t *page, const DTuple *key, ulint mode) noexcept {
  const auto high_key = page_get_high_key(page);

  if (high_key.is_null()) {
    return false;
  }

  const auto cmp = cmp_high_key(index, key, high_key);

  return mode == PAGE_CUR_GE || mode == PAGE_CUR_L ? cmp > 0 : cmp >= 0;
}

namespace {

/** @return the bytes in the fields a record of index is ordered on, as for a record with
those fields: extra bytes for 2 byte offsets. */
[[nodiscard]] ulint key_size(const Index *index, const DTuple *entry) noexcept {
  const auto n = index->get_n_unique_in_tree();
  ulint size{REC_N_EXTRA_BYTES + 2 * n};

  for (ulint i{}; i < n; ++i) {
    const auto field = dtuple_get_nth_field(entry, i);

    size += dfield_is_null(field) ? dtype_get_sql_null_size(dfield_get_type(field)) : dfield_get_len(field);
  }

  return size;
}

}  // namespace

ulint max_key_size() noexcept {
  /* Two node pointers, each with a child page number and its 2 byte field offset,
  and a high key. Rounded down, there are directory slots as well. */
  return page_get_free_space_of_empty() / 3 - 64;
}


namespace {

/** Save a position, also on a page with nothing on it: a merge can have left it empty, with no
record to save a position on, so it's before the page's high key. */
void save_position(System &sys, BtreePcursor &pcur, Mtr *mtr) noexcept {
  const auto page = pcur.get_block()->get_frame();

  if (likely(page_get_n_recs(page) > 0) || (!page_has_right(page) && Btree::page_get_prev(page, mtr) == FIL_NULL)) {
    pcur.store_position(sys, mtr);
    return;
  }

  pcur.m_old_stored = true;

  if (const auto key = page_get_high_key(page); key.is_null()) {
    pcur.m_rel_pos = BtreeCursorPos::AFTER_LAST_IN_TREE;
  } else {
    const auto index = pcur.get_btr_cur()->m_index;

    pcur.m_rel_pos = BtreeCursorPos::BEFORE;
    pcur.m_old_rec = index->copy_rec_order_prefix(key, &pcur.m_old_n_fields, pcur.m_old_rec_buf, pcur.m_buf_size);
    pcur.m_block_when_stored = pcur.get_block();
    pcur.m_modify_clock = buf_block_get_modify_clock(pcur.m_block_when_stored);
  }
}

}  // namespace

/** The page numbers on the way down, by level. Nothing else: it's valid with no latch held, and
stale values are repaired by moving right, as long as the pages are on the tree: see m_unlinks. */
struct Path {
  /** @return the page at level, FIL_NULL if the last descent didn't see that level. */
  [[nodiscard]] page_no_t at(ulint level) const noexcept {
    return m_height != ULINT_UNDEFINED && level <= m_height ? m_no[level] : FIL_NULL;
  }

  void set(ulint level, page_no_t page_no) noexcept {
    ut_a(level < BTR_MAX_DEPTH);

    if (m_height == ULINT_UNDEFINED || level > m_height) {
      for (ulint i = m_height == ULINT_UNDEFINED ? 0 : m_height + 1; i < level; ++i) {
        m_no[i] = FIL_NULL;
      }

      m_height = level;
    }

    m_no[level] = page_no;
  }

  void clear() noexcept { m_height = ULINT_UNDEFINED; }

  /** The highest level seen. */
  ulint m_height{ULINT_UNDEFINED};

  /** Drain::unlinks() when it was read: if that changes a page number here can be anything. */
  uint64_t m_unlinks{};

  std::array<page_no_t, BTR_MAX_DEPTH> m_no;
};

/** @return the page at level for key, or at level and to the left of its place: whatever the
caller latches there it moves right from. Nothing is held on return. To be called in a guard,
which the caller has to stay in until it has latched that page. */
[[nodiscard]] page_no_t descend(System &sys, const Index *index, const DTuple *key, ulint mode, Edge edge, ulint level, Path &path) noexcept {
  auto page_no = index->get_page_no();

  for (;;) {
    const auto step = read_node(sys, index, page_no, key, mode, edge, level);

    /* It was merged away, in one mini-transaction with its parent: the tree doesn't lead there. */
    if (step.m_kind == NodeStep::RESTART) {
      page_no = index->get_page_no();
      continue;
    }

    path.set(step.m_level, page_no);

    switch (step.m_kind) {
      case NodeStep::RIGHT:
        page_no = step.m_page;
        break;
      case NodeStep::CHILD:
        page_no = step.m_page;

        /* Levels change only on the root, which this isn't: no need to read it to know. */
        if (step.m_level == level + 1) {
          path.set(level, page_no);
          return page_no;
        }
        break;
      case NodeStep::STOP:
        return page_no;
      case NodeStep::RESTART:
        ut_error;
    }
  }
}

/**
 * A structure modification: everything that happens for one insert or update when a page is full.
 * The split of the leaf is in the mini-transaction of the operation, each level above in one of its
 * own: see the protocol in the header.
 *
 * What's held in a mini-transaction is kept here as a chain of blocks in level order, each the right
 * sibling of the one before: moving right on a level goes through it before it latches anything, so
 * a page is never latched twice, by the structure of the calls, and without a look in the memo.
 */
struct Smo final : BtreeCursor::TreeOps {
  /** A page this operation holds. */
  struct Held {
    BufBlock *m_block{};

    /** Where its latch is in the memo, to release it. */
    ulint m_savepoint{};

    /** It was changed: its latch stays to the commit. */
    bool m_dirty{};
  };

  struct Level {
    /** What's held on this level, left to right. */
    std::vector<Held> m_chain;

    /** Where in m_chain the next insert goes. */
    size_t m_at{};
  };

  /** A page that may have to have its right sibling put on the level above. */
  struct Unlinked {
    page_no_t m_page_no{};
    ulint m_level{};
  };

  Smo(System &sys, Index *index, Path &path, ulint leaf_savepoint, Mtr *mtr) noexcept
      : m_sys(sys), m_index(index), m_path(path), m_leaf_savepoint(leaf_savepoint), m_mtr(mtr) {}

  ~Smo() noexcept override {
    give_back();

    if (m_heap != nullptr) {
      mem_heap_free(m_heap);
    }

    if (m_key_buf != nullptr) {
      mem_free(m_key_buf);
    }
  }

  [[nodiscard]] bool always_reserve() const noexcept override { return true; }

  /** No tablespace latch in this mini-transaction: reserve() has its own. */
  void prepare(const Index *, Mtr *) noexcept override {}

  /** A short mini-transaction: the reservation stays in the space, the latch doesn't. After this
  the allocations of the split can't fail: nothing has changed if it does. */
  [[nodiscard]] bool reserve(const Index *index, ulint, ulint alloc_type, Mtr *) noexcept override {
    if (m_reserved) {
      return true;
    }

    Mtr mtr;

    mtr.start();

    m_sys.m_fsp->latch(index->get_space_id(), &mtr);

    /* The height as the last descent saw it, as the B-tree reckons. */
    const auto height = m_path.m_height == ULINT_UNDEFINED ? 1 : m_path.m_height + 1;

    m_reserved = m_sys.m_fsp->reserve_free_extents(&m_n_reserved, index->get_space_id(), height / 16 + 3, alloc_type, &mtr);

    mtr.commit(m_sys);

    return m_reserved;
  }

  /** The reservation stays until the levels above are done too. */
  void release(const Index *) noexcept override {}

  void free_updated(const Index *index, Rec rec, const RecOffs &offsets, const Upd *update, TrxRbCtx rb_ctx, Mtr *mtr) noexcept override {
    Mtr blob_mtr;
    Blob blob{};

    /* Not in the mini-transaction of the operation: it would hold the tablespace latch while it
    goes on to latch parents. */
    blob_mtr.start(mtr);

    m_sys.m_fsp->latch(index->get_space_id(), &blob_mtr);

    blob.free_updated_extern_fields(m_sys, index, rec, offsets, update, rb_ctx, &blob_mtr);

    blob_mtr.commit(m_sys);
  }

  /** A merge is for what remove() leaves: an update that shrinks a page doesn't try. */
  void after_reinsert(BtreeCursor *, Mtr *) noexcept override {}

  [[nodiscard]] Rec split_and_insert(BtreeCursor *cur, const DTuple *entry, ulint n_ext, Mtr *mtr) noexcept override {
    ut_a(mtr == m_mtr);

    m_heap = mem_heap_create(2048);

    level(0).m_chain.push_back(Held{cur->get_block(), m_leaf_savepoint, true});

    m_rec = split_insert(cur, 0, entry, n_ext);

    return m_rec;
  }

  /** @return true if there is a split to complete. */
  [[nodiscard]] bool started() const noexcept { return !m_unlinked.empty(); }

  /** The record went in on a page without a split, and the page has a split to complete. */
  void complete_for(BufBlock *block, Rec rec) noexcept {
    ut_ad(!started());

    m_heap = mem_heap_create(2048);
    m_rec = rec;
    m_unlinked.push_back(Unlinked{block->get_page_no(), 0});
  }

  /** Commit the mini-transaction with what it did on its level, then put in the node pointers up the
  tree. On return the mini-transaction is started again, holding nothing.
  @param[out] key               The key of the record.
  @param[in,out] heap           For key. */
  void complete(DTuple **key, MemHeap *heap) noexcept {
    test_point(m_sys, TestPoint::SPLIT);

    ulint n_fields;

    const auto copy = m_index->copy_rec_order_prefix(m_rec, &n_fields, m_key_buf, m_key_buf_size);

    *key = m_index->build_data_tuple(copy, n_fields, heap);

    {
      /* From before anything is released: what's in m_unlinked stays a page of this tree or a dead
      one, not something else. */
      Drain::Guard guard{shared(m_sys).m_drain};
      const auto mtr = m_mtr;

      commit_level();

      test_point(m_sys, TestPoint::UNLINKED);

      /* A way to go on if the space ran out: the flags are still there, for the next writer. */
      if (m_reserved || reserve(m_index, 0, FSP_NORMAL, nullptr)) {
        /* It grows as parents split. */
        for (size_t i{}; i < m_unlinked.size(); ++i) {
          Mtr up;

          up.start();
          m_mtr = &up;

          link_right(m_unlinked[i]);
          commit_level();
        }
      }

      m_mtr = mtr;
    }

    m_mtr->start();

    give_back();
  }

 private:
  void give_back() noexcept {
    if (m_reserved) {
      m_sys.m_fil->space_release_free_extents(m_index->get_space_id(), m_n_reserved);
      m_reserved = false;
    }
  }

  [[nodiscard]] Level &level(ulint lv) noexcept {
    while (m_levels.size() <= lv) {
      m_levels.emplace_back();
    }

    return m_levels[lv];
  }

  [[nodiscard]] bool is_root(const BufBlock *block) const noexcept { return block->get_page_no() == m_index->get_page_no(); }

  /** Commit what's held: the links, the keys and the end of the allocations are one atomic change. */
  void commit_level() noexcept {
    if (m_trx.has_value()) {
      m_trx->commit(m_mtr);
      m_trx.reset();
    } else {
      m_mtr->commit(m_sys);
    }

    m_levels.clear();
  }

  /** If the page still has a right sibling without a node pointer, put that in, in the
  mini-transaction that clears the flag. All of it comes from the page: it can be that somebody else
  has done this, that the page has split again since, or that it's a page from before a crash. */
  void link_right(const Unlinked &unlinked) noexcept {
    const auto block = m_sys.m_btree->block_get(m_index->get_space_id(), unlinked.m_page_no, RW_B_LATCH, m_mtr);
    const auto page = block->get_frame();

    if (is_dead(page, m_index) || m_sys.m_btree->page_get_level(page, m_mtr) != unlinked.m_level || !page_right_unlinked(page)) {
      return;
    }

    /* Copies what it takes from the key: a reorganize below can't move what this has. */
    const auto sep = m_index->build_node_ptr(page_get_high_key(page), Btree::page_get_next(page, m_mtr), m_heap, unlinked.m_level);

    insert_sep(unlinked.m_level + 1, sep);

    page_set_right_unlinked(block, false, m_mtr);
  }

  /** Allocate a page and give it to the mini-transaction of the split, X latched. Nobody can
  reach the page until that commits: the wait for it is on nothing but a flush. */
  [[nodiscard]] BufBlock *new_page(ulint lv, page_no_t hint, byte direction) noexcept {
    ut_ad(m_reserved);

    if (!m_trx.has_value()) {
      m_trx.emplace(m_sys);
    }

    const auto space = m_index->get_space_id();

    Mtr mtr;

    mtr.start();

    m_sys.m_fsp->latch(space, &mtr);

    const auto block = m_sys.m_btree->page_alloc(m_index, hint, direction, lv, &mtr);

    /* The reservation is for this. */
    ut_a(block != nullptr);

    const auto page_no = block->get_page_no();

    /* The compensation is to give it back: FREE_PAGE, the root holds the segment header. */
    byte args[4 + 4 + 2 + 4];

    mach_write_to_4(args, space);
    mach_write_to_4(args + 4, m_index->get_page_no());
    mach_write_to_2(args + 8, PAGE_HEADER + (lv == 0 ? PAGE_BTR_SEG_LEAF : PAGE_BTR_SEG_TOP));
    mach_write_to_4(args + 10, page_no);

    m_trx->step(&mtr, SysTrxKind::FREE_PAGE, args, sizeof(args));
    m_trx->commit_step(&mtr);

    test_point(m_sys, TestPoint::ALLOCATED);

    return m_sys.m_btree->block_get(space, page_no, RW_B_LATCH, m_mtr);
  }

  /** The root is what the cursor is on: it becomes the parent of a new page, the cursor
  moves to that page. Holding the root X, nothing here waits for a tree page. */
  void raise(BtreeCursor *cur, ulint lv, const DTuple *tuple) noexcept {
    auto &st = level(lv);
    const auto root = st.m_chain[st.m_at];

    ut_ad(root.m_block == cur->get_block());
    ut_ad(st.m_chain.size() == 1);

    const auto new_block = new_page(lv, 0, FSP_NO_DIR);

    m_sys.m_btree->root_raise(cur, new_block, tuple, m_mtr);

    st.m_chain[st.m_at] = Held{new_block, 0, true};

    auto &up = level(lv + 1);

    ut_a(up.m_chain.empty());

    up.m_chain.push_back(Held{root.m_block, root.m_savepoint, true});

    m_path.set(lv + 1, m_index->get_page_no());
    m_path.set(lv, new_block->get_page_no());
  }

  /** @return true if limit as the first record to move leaves a left page that takes its high key. */
  [[nodiscard]] bool left_fits(page_t *page, Rec limit, ulint high_key_size) noexcept {
    ulint size{};
    ulint n{};
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};
    MemHeap *heap{};

    rec_offs_init(offs_);

    auto offs = offs_.data();

    for (auto rec = page_rec_get_next(page_get_infimum_rec(page)); rec != limit; rec = page_rec_get_next(rec)) {
      offs = PhyRec(m_index, rec).get_all_col_offsets(offs, &heap, Current_location());
      size += RecOffs(offs).size();
      ++n;
    }

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    return size + high_key_size + page_dir_calc_reserved_space(n) <= page_get_free_space_of_empty();
  }

  /** @return the size of the order prefix of rec, as a high key. */
  [[nodiscard]] ulint prefix_size(Rec rec) noexcept {
    const auto n = m_index->get_n_unique_in_tree();

    return REC_N_EXTRA_BYTES + n * (rec.get_1byte_offs_flag() ? 1 : 2) + rec.get_field_start_offs(n);
  }

  /** Decide where to split the page of the cursor, as the old tree does, for the same fill factor
  in the same patterns. It differs in that the upper half always moves: the left page keeps its
  number, a parent and stale readers find their way right from there. And the tuple, when it's
  the first of the upper half, goes there: a left page holds keys < its high key.
  @return the first record that moves, nullptr if that's the tuple. */
  [[nodiscard]] Rec split_point(BtreeCursor *cur, const DTuple *tuple, ulint n_ext, ulint iter) noexcept {
    const auto btree = m_sys.m_btree.get();
    const auto page = cur->get_page_no();
    const auto n_uniq = m_index->get_n_unique_in_tree();
    const auto first = page_rec_get_next(page_get_infimum_rec(page));
    Rec rec{};

    if (iter > 0) {
      rec = btree->page_get_split_rec(cur, tuple, n_ext);

      if (rec.is_null() && btree->page_tuple_smaller(cur, tuple, nullptr, n_uniq, &m_heap)) {
        rec = first;
      }

    } else if (btree->page_get_split_rec_right(cur, rec) || btree->page_get_split_rec_left(cur, rec)) {
      /* A pattern of inserts. */

    } else if (page_get_n_recs(page) > 1) {
      rec = page_get_middle_rec(page);

    } else if (page_get_n_recs(page) == 1 && btree->page_tuple_smaller(cur, tuple, nullptr, n_uniq, &m_heap)) {
      rec = first;
    }

    /* The left page has to take the new high key. The patterns above can leave it full: then a
    split in the middle or further left, which fits: a key is at most a third of a page. */
    const auto limit = rec.is_null() ? page_rec_get_next(cur->get_rec()) : rec;
    const auto high_key_size = rec.is_null() ? key_size(m_index, tuple) : prefix_size(rec);

    if (left_fits(page, limit, high_key_size)) {
      return rec;
    }

    rec = page_get_middle_rec(page);

    while (!left_fits(page, rec, prefix_size(rec))) {
      const auto prev = page_rec_get_prev(rec);

      ut_a(prev != first);

      rec = prev;
    }

    return rec;
  }

  /** Split the page of the cursor and try to insert tuple.
  @return the record, null if it doesn't fit: the cursor is then where it goes. */
  [[nodiscard]] Rec split_once(BtreeCursor *cur, ulint lv, const DTuple *tuple, ulint n_ext, ulint iter) noexcept {
    auto &st = level(lv);
    const auto btree = m_sys.m_btree.get();
    const auto block = cur->get_block();
    const auto page = block->get_frame();
    const auto page_no = block->get_page_no();
    const auto space = m_index->get_space_id();
    const auto split_rec = split_point(cur, tuple, n_ext, iter);

    /* Then the flag says it of the new page, for the sibling after that. */
    const auto was_unlinked = page_right_unlinked(page);

    ut_ad(block == st.m_chain[st.m_at].m_block);

    bool insert_left{};
    Rec first_rec{};
    Rec move_limit{};

    if (split_rec.is_null()) {
      /* The tuple is the first of the upper half, its order prefix is in a record for that. */
      const auto size = rec_get_converted_size(m_index, tuple, n_ext);

      first_rec = Rec::convert_dtuple_to_rec(static_cast<byte *>(mem_heap_alloc(m_heap, size)), m_index, tuple, n_ext);
      move_limit = page_rec_get_next(cur->get_rec());
    } else {
      std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};
      MemHeap *heap{};

      rec_offs_init(offs_);

      const auto offs = PhyRec(m_index, split_rec).get_all_col_offsets(offs_.data(), &heap, Current_location());

      insert_left = cmp_dtuple_rec(m_index->m_cmp_ctx, tuple, split_rec, offs) < 0;

      if (heap != nullptr) {
        mem_heap_free(heap);
      }

      first_rec = move_limit = split_rec;
    }

    /* Before the records move: first_rec may be garbage after. */
    ulint n_fields;
    const auto high_key = m_index->copy_rec_order_prefix(first_rec, &n_fields, m_key_buf, m_key_buf_size);
    const auto high_key_size = page_rec_extra_size(high_key) + high_key.get_data_size();

    ut_a(high_key_size <= max_key_size());

    /* The new page inherits the bound of this one, a copy: a reorganize moves it. */
    const auto old_key = page_get_high_key(page);
    Rec old_key_copy{};
    ulint old_key_size{};

    if (!old_key.is_null()) {
      const auto extra = page_rec_extra_size(old_key);

      old_key_size = page_high_key_size(old_key);

      auto buf = static_cast<byte *>(mem_heap_alloc(m_heap, old_key_size));

      memcpy(buf, old_key.get() - extra, old_key_size);
      old_key_copy = Rec(buf + extra);
    }

    /* The latches: this page, then the new one that nobody can reach, then right. Allocating
    with latches held: see the proof in the header. */
    const auto new_block = new_page(lv, page_no + 1, FSP_UP);
    const auto new_no = new_block->get_page_no();
    const auto new_frame = new_block->get_frame();

    btree->page_create(new_block, m_index, lv, m_mtr);

    const auto right_no = Btree::page_get_next(page, m_mtr);

    /* The right sibling's left link, with this page held, left to right: its writers
    don't want this one. It's the next in the chain if we hold it already. */
    if (right_no != FIL_NULL) {
      BufBlock *right;

      if (st.m_at + 1 < st.m_chain.size()) {
        right = st.m_chain[st.m_at + 1].m_block;
        st.m_chain[st.m_at + 1].m_dirty = true;
      } else {
        const auto savepoint = m_mtr->set_savepoint();

        right = btree->block_get(space, right_no, RW_B_LATCH, m_mtr);
        st.m_chain.push_back(Held{right, savepoint, true});
      }

      ut_ad(right->get_page_no() == right_no);

      Btree::page_set_prev(right->get_frame(), new_no, m_mtr);
    }

    /* The links first: a key needs the right link to be there, see PAGE_HIGH_KEY. */
    Btree::page_set_prev(new_frame, page_no, m_mtr);
    Btree::page_set_next(new_frame, right_no, m_mtr);

    const auto moved = page_move_rec_list_end(m_sys, new_block, block, move_limit, m_index, m_mtr);

    ut_a(moved);

    m_sys.m_lock_sys->update_split_right(new_block, block);

    /* After the list copy, its fast path is for a new page. */
    if (!old_key_copy.is_null()) {
      const auto installed = page_set_high_key(new_block, old_key_copy, old_key_size, m_mtr);

      ut_a(installed);
    }

    Btree::page_set_next(page, new_no, m_mtr);

    if (!page_set_high_key(block, high_key, high_key_size, m_mtr)) {
      /* What the records left is garbage, and the old key that the new page has now:
      the key takes the space of a reorganize without it. Under the X latch, and all in this
      mini-transaction, nobody sees the page without one. */
      page_clear_high_key(block, m_mtr);

      const auto reorganized = btree->page_reorganize(block, m_index, m_mtr);

      ut_a(reorganized);

      const auto installed = page_set_high_key(block, high_key, high_key_size, m_mtr);

      ut_a(installed);
    }

    page_set_right_unlinked(block, true, m_mtr);

    m_unlinked.push_back(Unlinked{page_no, lv});

    if (was_unlinked) {
      page_set_right_unlinked(new_block, true, m_mtr);

      m_unlinked.push_back(Unlinked{new_no, lv});
    }

    st.m_chain.insert(st.m_chain.begin() + st.m_at + 1, Held{new_block, 0, true});

    if (!insert_left) {
      ++st.m_at;
    }

    const auto target = insert_left ? block : new_block;
    const auto page_cur = cur->get_page_cur();

    page_cur->search(m_sys, target, m_index, tuple, PAGE_CUR_LE);

    auto rec = page_cur->tuple_insert(tuple, m_index, n_ext, m_mtr);

    if (rec.is_null()) {
      const auto reorganized = btree->page_reorganize(target, m_index, m_mtr);

      ut_a(reorganized);

      page_cur->search(m_sys, target, m_index, tuple, PAGE_CUR_LE);

      rec = page_cur->tuple_insert(tuple, m_index, n_ext, m_mtr);
    }

    ut_ad(page_validate(page, m_index));
    ut_ad(page_validate(new_frame, m_index));

    return rec;
  }

  /** Make room on the page of the cursor for tuple, by a root raise and splits, and insert it.
  @return the record. */
  [[nodiscard]] Rec split_insert(BtreeCursor *cur, ulint lv, const DTuple *tuple, ulint n_ext) noexcept {
    if (is_root(cur->get_block())) {
      raise(cur, lv, tuple);
    }

    for (ulint iter{};; ++iter) {
      /* A page halves each time, and a page takes the largest record and a key. */
      ut_a(iter < 16);

      const auto rec = split_once(cur, lv, tuple, n_ext, iter);

      if (!rec.is_null()) {
        return rec;
      }
    }
  }

  /** @return the parent level's page to start from for sep, X latched. Only for a level with
  nothing held. From the path if it's still at that level: a page's level changes only on the
  root, and then it's found to be wrong here. Else a new descent, by level, holding what we
  hold below that level: it waits for nothing lower. */
  [[nodiscard]] Held latch_parent(ulint lv, const DTuple *sep) noexcept {
    const auto space = m_index->get_space_id();
    auto &drain = shared(m_sys).m_drain;
    Drain::Guard guard{drain};

    /* Its numbers can be pages of anything once a page has left a tree. */
    if (m_path.m_unlinks != drain.unlinks()) {
      m_path.clear();
    }

    auto page_no = m_path.at(lv);

    for (;;) {
      if (page_no == FIL_NULL) {
        m_path.m_unlinks = drain.unlinks();

        page_no = descend(m_sys, m_index, sep, PAGE_CUR_LE, Edge::KEY, lv, m_path);
      }

      const auto savepoint = m_mtr->set_savepoint();
      const auto block = m_sys.m_btree->block_get(space, page_no, RW_B_LATCH, m_mtr);
      const auto page = block->get_frame();

      if (!is_dead(page, m_index) && m_sys.m_btree->page_get_level(page, m_mtr) == lv) {
        return Held{block, savepoint, false};
      }

      m_mtr->release_block_at_savepoint(savepoint, block);

      /* A page left the tree since the path was read: the rest of it can be anything too, and
      the new generation below would make it look read just now. */
      m_path.clear();

      page_no = FIL_NULL;
    }
  }

  /** Insert a node pointer, on the level above the page it points to. */
  void insert_sep(ulint lv, const DTuple *sep) noexcept {
    auto &st = level(lv);
    const auto btree = m_sys.m_btree.get();

    if (st.m_chain.empty()) {
      st.m_chain.push_back(latch_parent(lv, sep));
      st.m_at = 0;
    }

    /* Move right from a stale parent: left to right coupling. Through what we hold, then by latching.
    A page we didn't change has no reason to stay latched. */
    while (past_high_key(m_index, st.m_chain[st.m_at].m_block->get_frame(), sep, PAGE_CUR_LE)) {
      const auto held = st.m_chain[st.m_at];

      if (st.m_at + 1 < st.m_chain.size()) {
        ++st.m_at;
        continue;
      }

      const auto right_no = Btree::page_get_next(held.m_block->get_frame(), m_mtr);
      const auto savepoint = m_mtr->set_savepoint();
      const auto right = btree->block_get(m_index->get_space_id(), right_no, RW_B_LATCH, m_mtr);

      m_path.set(lv, right_no);

      if (held.m_dirty) {
        st.m_chain.push_back(Held{right, savepoint, false});
        ++st.m_at;
      } else {
        m_mtr->release_block_at_savepoint(held.m_savepoint, held.m_block);
        st.m_chain[st.m_at] = Held{right, savepoint, false};
      }
    }

    const auto block = st.m_chain[st.m_at].m_block;

    BtreeCursor cur{};
    const auto page_cur = cur.get_page_cur();

    page_cur->search(m_sys, block, m_index, sep, PAGE_CUR_LE);
    cur.position(m_index, page_cur->get_rec(), block);

    auto rec = page_cur->tuple_insert(sep, m_index, 0, m_mtr);

    if (rec.is_null()) {
      const auto page = block->get_frame();

      if (page_get_max_insert_size_after_reorganize(page, 1) >= rec_get_converted_size(m_index, sep, 0)) {
        const auto reorganized = btree->page_reorganize(block, m_index, m_mtr);

        ut_a(reorganized);

        page_cur->search(m_sys, block, m_index, sep, PAGE_CUR_LE);

        rec = page_cur->tuple_insert(sep, m_index, 0, m_mtr);

        ut_a(!rec.is_null());
      }
    }

    st.m_chain[st.m_at].m_dirty = true;

    if (rec.is_null()) {
      (void)split_insert(&cur, lv, sep, 0);

    } else if (page_right_unlinked(block->get_frame())) {
      /* From a crash or a split in progress, either way it's cheap: a writer that finds one does it. */
      m_unlinked.push_back(Unlinked{block->get_page_no(), lv});
    }
  }

  System &m_sys;
  Index *m_index;
  Path &m_path;

  /** Where the latch of the leaf is in the memo. */
  ulint m_leaf_savepoint;

  /** The mini-transaction of the operation. */
  Mtr *m_mtr;

  /** The levels of this mini-transaction, from the leaf up. A deque: references stay valid while it grows. */
  std::deque<Level> m_levels;

  /** What's left to do on the levels above. */
  std::vector<Unlinked> m_unlinked;

  /** The allocations of this mini-transaction: until the new pages are linked, a crash gives them back. */
  std::optional<SysTrx> m_trx;

  /** The record that was inserted. */
  Rec m_rec{};

  /** The node pointers and keys for this operation. */
  MemHeap *m_heap{};

  /** For a key that's being set. */
  byte *m_key_buf{};
  ulint m_key_buf_size{};

  ulint m_n_reserved{};
  bool m_reserved{};
};

/**
 * The other way: a page with little on it takes in its right sibling, see the protocol in the
 * header. All of it is best effort. Whatever isn't as it was when it looked, it leaves alone.
 * Nothing is held on entry, and nothing on return.
 */
class Merge {
 public:
  Merge(System &sys, Index *index, const DTuple *key) noexcept : m_sys(sys), m_index(index), m_key(key), m_shared(shared(sys)) {}

  /** @return true if a page with size bytes of records wants to be merged. */
  [[nodiscard]] static bool is_small(ulint size) noexcept { return size < page_get_free_space_of_empty() / 4; }

  /** Merge around the key on the levels that want it. */
  void run() noexcept {
    for (int sweep{}; sweep < 4; ++sweep) {
      bool merged{};
      bool went_up{};

      for (ulint lv{};; ++lv) {
        auto result = at_level(lv);
        auto here = false;

        for (int i{}; result == Result::MERGED && i < 8; ++i) {
          here = merged = true;
          result = at_level(lv);
        }

        if (result == Result::TOP) {
          break;
        } else if (result == Result::ALONE) {
          went_up = true;
        } else if (!here) {
          /* Nothing changed: nothing changed above. */
          break;
        }
      }

      /* A merge above has given a page below a sibling. */
      if (!merged || !went_up) {
        break;
      }
    }

    free_dead_pages(m_sys);
  }

 private:
  enum class Result : uint8_t {
    /** Nothing to do at this level, or it isn't as it was. */
    NONE,

    /** A merge. */
    MERGED,

    /** The page wants a merge, and its parent has no other child. */
    ALONE,

    /** This level is the root. */
    TOP
  };

  /** The page for the key at a level, and its neighbours under the same parent. */
  struct Family {
    page_no_t m_parent{FIL_NULL};
    page_no_t m_left{FIL_NULL};
    page_no_t m_page{FIL_NULL};
    page_no_t m_right{FIL_NULL};
    ulint m_n_children{};
  };

  [[nodiscard]] page_no_t child_of(Rec rec, MemHeap **heap) noexcept {
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};

    rec_offs_init(offs_);

    const RecOffs offs = PhyRec(m_index, rec).get_all_col_offsets(offs_.data(), heap, Current_location());

    return m_sys.m_btree->node_ptr_get_child_page_no(rec, offs);
  }

  /** With no latch: what's read here only tells what to try, merge() looks again under its latches.
  @return false if the parent's level isn't there: the root is at lv. */
  [[nodiscard]] bool family(ulint lv, Family &fam) noexcept {
    Path path;
    const auto space = m_index->get_space_id();
    auto page_no = descend(m_sys, m_index, m_key, PAGE_CUR_LE, Edge::KEY, lv + 1, path);

    for (;;) {
      Mtr mtr;

      mtr.start();

      const auto block = m_sys.m_btree->block_get(space, page_no, RW_NO_LATCH, &mtr);
      RawFamily raw{};

      read_stable(m_sys, block, [&]() noexcept { return read_family_raw(m_index, block->get_frame(), m_key, lv + 1, raw); });

      mtr.commit(m_sys);

      switch (raw.m_kind) {
        case RawFamily::DEAD:
          return true;

        case RawFamily::OTHER_LEVEL:
          return page_no != m_index->get_page_no();

        case RawFamily::RIGHT:
          page_no = raw.m_next;
          continue;

        case RawFamily::FOUND:
          fam.m_parent = page_no;
          fam.m_n_children = raw.m_n_children;
          fam.m_page = raw.m_page;
          fam.m_left = raw.m_left;
          fam.m_right = raw.m_right;
          return true;
      }

      ut_error;
    }
  }

  [[nodiscard]] Result at_level(ulint lv) noexcept {
    /* The page numbers below stay pages of this tree until this returns. */
    Drain::Guard guard{m_shared.m_drain};
    Family fam;

    if (!family(lv, fam)) {
      return Result::TOP;
    }

    if (fam.m_page == FIL_NULL) {
      return Result::NONE;
    }

    if (fam.m_n_children == 1) {
      if (fam.m_parent != m_index->get_page_no()) {
        return page_small(lv, fam.m_page) ? Result::ALONE : Result::NONE;
      }

      return lift(lv, fam);
    }

    if (fam.m_right != FIL_NULL) {
      const auto result = merge(lv, fam, fam.m_page, fam.m_right, fam.m_page);

      /* Not with what's on the right: it can be with what's on the left. */
      if (result != Result::NONE || fam.m_left == FIL_NULL) {
        return result;
      }
    }

    return merge(lv, fam, fam.m_left, fam.m_page, fam.m_page);
  }

  [[nodiscard]] bool page_small(ulint lv, page_no_t page_no) noexcept {
    Mtr mtr;

    mtr.start();

    const auto block = m_sys.m_btree->block_get(m_index->get_space_id(), page_no, RW_NO_LATCH, &mtr);
    ulint size{};

    read_stable(m_sys, block, [&]() noexcept { return read_rec_size_raw(m_index, block->get_frame(), lv, size); });

    mtr.commit(m_sys);

    return size != ULINT_UNDEFINED && is_small(size);
  }

  /** @return a page of the level, X latched, null if it's not on the tree as that any more. */
  [[nodiscard]] BufBlock *latch(page_no_t page_no, ulint lv, Mtr *mtr) noexcept {
    const auto block = m_sys.m_btree->block_get(m_index->get_space_id(), page_no, RW_B_LATCH, mtr);
    const auto page = block->get_frame();

    return is_dead(page, m_index) || m_sys.m_btree->page_get_level(page, mtr) != lv ? nullptr : block;
  }

  /** Take a page off the tree, in the mini-transaction that takes the pointers to it away.
  That commits here: the page is in the system transaction from then on. */
  void kill(BufBlock *block, ulint lv, Mtr *mtr) noexcept {
    const auto space = m_index->get_space_id();
    const auto page_no = block->get_page_no();
    const auto seg = PAGE_HEADER + (lv == 0 ? PAGE_BTR_SEG_LEAF : PAGE_BTR_SEG_TOP);

    mlog_write_uint64(block->get_frame() + PAGE_HEADER + PAGE_INDEX_ID, DEAD_INDEX_ID, mtr);

    buf_block_modify_clock_inc(block);

    /* The compensation is to give it back: FREE_PAGE, the root holds the segment header. */
    byte args[4 + 4 + 2 + 4];

    mach_write_to_4(args, space);
    mach_write_to_4(args + 4, m_index->get_page_no());
    mach_write_to_2(args + 8, seg);
    mach_write_to_4(args + 10, page_no);

    auto trx = std::make_unique<SysTrx>(m_sys);

    trx->step(mtr, SysTrxKind::FREE_PAGE, args, sizeof(args));
    trx->commit_step(mtr);

    test_point(m_sys, TestPoint::MERGED);

    m_shared.m_drain.add(DeadPage{std::move(trx), space, m_index->get_page_no(), page_no, seg, 0});
  }

  /** Merge right into left.
  @param[in] target             The page that wants it: left or right.
  @return Result::NONE if it doesn't want it or it doesn't fit. */
  [[nodiscard]] Result merge(ulint lv, const Family &fam, page_no_t left_no, page_no_t right_no, page_no_t target) noexcept {
    const auto btree = m_sys.m_btree.get();
    Mtr mtr;

    mtr.start();

    /* The order of the latches is the order of a split: left to right, then up. */
    const auto left = latch(left_no, lv, &mtr);

    if (left == nullptr || Btree::page_get_next(left->get_frame(), &mtr) != right_no) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto right = latch(right_no, lv, &mtr);

    if (right == nullptr) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto left_page = left->get_frame();
    const auto right_page = right->get_frame();

    /* A split in progress, or from before a crash. Its flag is what makes that pointer go in. */
    if (page_right_unlinked(left_page) || page_right_unlinked(right_page)) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto target_size = page_get_rec_data_size(target == left_no ? left_page : right_page);
    const auto high_key = page_get_high_key(right_page);
    const auto high_key_size = high_key.is_null() ? 0 : page_high_key_size(high_key);
    const auto n_recs = page_get_n_recs(left_page) + page_get_n_recs(right_page);
    const auto right_size = page_get_rec_data_size(right_page);
    const auto size = page_get_rec_data_size(left_page) + right_size + high_key_size + page_dir_calc_reserved_space(n_recs);

    /* Not to full: the next inserts would split it again. */
    if (!is_small(target_size) || size > page_get_free_space_of_empty() / 4 * 3) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto next_no = Btree::page_get_next(right_page, &mtr);
    BufBlock *next{};

    if (next_no != FIL_NULL) {
      next = btree->block_get(m_index->get_space_id(), next_no, RW_B_LATCH, &mtr);
    }

    const auto parent = latch(fam.m_parent, lv + 1, &mtr);

    if (parent == nullptr) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    /* Both have to be there, next to each other. If not it's another parent by now. */
    MemHeap *heap{};
    Rec right_ptr{};

    for (auto rec = page_rec_get_next(page_get_infimum_rec(parent->get_frame())); page_rec_is_user_rec(rec); rec = page_rec_get_next(rec)) {
      if (child_of(rec, &heap) == right_no) {
        const auto prev = page_rec_get_prev(rec);

        if (page_rec_is_user_rec(prev) && child_of(prev, &heap) == left_no) {
          right_ptr = rec;
        }

        break;
      }
    }

    if (right_ptr.is_null()) {
      if (heap != nullptr) {
        mem_heap_free(heap);
      }

      mtr.commit(m_sys);
      return Result::NONE;
    }

    /* Nothing below this fails. Before the tree changes: see the header on a Path. */
    m_shared.m_drain.note_unlink();

    page_clear_high_key(left, &mtr);

    if (page_get_max_insert_size(left_page, page_get_n_recs(right_page)) < right_size + high_key_size) {
      const auto reorganized = btree->page_reorganize(left, m_index, &mtr);

      ut_a(reorganized);
    }

    /* What comes after a position on this page isn't what it was. A cursor saved after the last
    record would come back on the supremum, and go on to the next page past all of this. */
    buf_block_modify_clock_inc(left);

    const auto orig_pred = page_copy_rec_list_start(m_sys, left, right, page_get_supremum_rec(right_page), m_index, &mtr);

    ut_a(!orig_pred.is_null());

    m_sys.m_lock_sys->update_merge_left(left, orig_pred, right);

    /* The links first: a key needs the right link to be there, see PAGE_HIGH_KEY. */
    Btree::page_set_next(left_page, next_no, &mtr);

    if (next != nullptr) {
      Btree::page_set_prev(next->get_frame(), left_no, &mtr);
    }

    if (!high_key.is_null()) {
      const auto installed = page_set_high_key(left, high_key, high_key_size, &mtr);

      ut_a(installed);
    }

    std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};

    rec_offs_init(offs_);

    const RecOffs offs = PhyRec(m_index, right_ptr).get_all_col_offsets(offs_.data(), &heap, Current_location());

    PageCursor cur{};

    cur.position(right_ptr, parent);
    cur.delete_rec(m_sys, m_index, offs, &mtr);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    ut_ad(page_validate(left_page, m_index));

    kill(right, lv, &mtr);

    return Result::MERGED;
  }

  /** The root has one child: it takes the records of that, and the tree is a level lower. The
  reverse of a raise: the root keeps its number. */
  [[nodiscard]] Result lift(ulint lv, const Family &fam) noexcept {
    const auto btree = m_sys.m_btree.get();
    Mtr mtr;

    mtr.start();

    const auto child = latch(fam.m_page, lv, &mtr);

    if (child == nullptr) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto root = latch(fam.m_parent, lv + 1, &mtr);

    if (root == nullptr || page_get_n_recs(root->get_frame()) != 1) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    MemHeap *heap{};
    const auto only = child_of(page_rec_get_next(page_get_infimum_rec(root->get_frame())), &heap);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    if (only != fam.m_page) {
      mtr.commit(m_sys);
      return Result::NONE;
    }

    const auto child_page = child->get_frame();

    /* A right sibling that has no node pointer yet. */
    if (page_has_right(child_page)) {
      ut_a(page_right_unlinked(child_page));
      mtr.commit(m_sys);
      return Result::NONE;
    }

    /* It's the only page of its level, so it holds what the level holds. */
    ut_a(Btree::page_get_prev(child_page, &mtr) == FIL_NULL);

    m_shared.m_drain.note_unlink();

    btree->page_empty(root, m_index, lv, &mtr);

    buf_block_modify_clock_inc(root);

    (void)page_copy_rec_list_end(m_sys, root, child, page_get_infimum_rec(child_page), m_index, &mtr);

    m_sys.m_lock_sys->update_copy_and_discard(root, child);

    ut_ad(page_validate(root->get_frame(), m_index));

    kill(child, lv, &mtr);

    return Result::MERGED;
  }

  System &m_sys;
  Index *m_index;
  const DTuple *m_key;
  Shared &m_shared;
};

namespace {

[[nodiscard]] ulint leaf_latch(Intent intent) noexcept { return intent != Intent::Write && intent != Intent::Restructure ? BTR_SEARCH_LEAF : BTR_MODIFY_LEAF; }

struct BlinkCursor final : IndexAm::Cursor {
  explicit BlinkCursor(System &sys) noexcept : m_sys(sys) {}

  /* A read that holds nothing, Intent::Snapshot. The page has a buffer fix only. What the caller sees
  is in m_snap_frame, memory of the cursor's own: the header of the page and the current record, at the
  offsets they have on the page, so that what can be told from a record's address still can. It's what
  the page had when its version was m_snap_version. As long as the page still has that version the
  next record can be read from it. If not, the place is found again by key. */

  /** @return the memory for the copy, of a page's size and alignment: page_align() works on it. */
  [[nodiscard]] byte *snap_frame() noexcept {
    if (m_snap_frame == nullptr) {
      m_snap_frame.reset(static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE)));
      ut_a(m_snap_frame != nullptr);
    }

    return m_snap_frame.get();
  }

  [[nodiscard]] bool snap_on_rec() const noexcept { return m_snap_offs != PAGE_INFIMUM && m_snap_offs != PAGE_SUPREMUM; }

  /** Keep what a place that's not a record is relative to. */
  void snap_keep_seek(const DTuple *key, ulint mode) noexcept {
    auto heap = mem_heap_create(256);
    DTuple *copy{};

    if (key != nullptr) {
      copy = dtuple_copy(key, heap);

      for (ulint i{}; i < dtuple_get_n_fields(copy); ++i) {
        dfield_dup(dtuple_get_nth_field(copy, i), heap);
      }
    }

    if (m_snap_heap != nullptr) {
      mem_heap_free(m_snap_heap);
    }

    m_snap_heap = heap;
    m_snap_seek = copy;
    m_snap_mode = mode;
  }

  /** Copy a place on a page to the cursor's memory, for a page with this version. To the spare copy,
  which becomes the copy only when it's good: what's in the copy is what the place is found again from.
  @return false if the page isn't at that version any more. */
  [[nodiscard]] bool snap_copy(const BufBlock *block, ulint offs, uint64_t version) noexcept {
    const auto page = block->get_frame();

    test_point(m_sys, TestPoint::SNAP_COPY);

    (void)snap_frame();

    if (m_snap_spare == nullptr) {
      m_snap_spare.reset(static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE)));
      ut_a(m_snap_spare != nullptr);
    }

    for (;;) {
      uint32_t begun{};

      if (block->rec_read_begin(begun)) {
        bool ok;

        {
          [[maybe_unused]] RacyReads racy;

          ok = read_rec_copy_raw(page, offs, m_snap_spare.get());
        }

        if (!block->read_is_valid(version)) {
          return false;
        }

        if (block->rec_read_is_valid(begun)) {
          ut_a(ok);

          std::swap(m_snap_frame, m_snap_spare);

          return true;
        }
      }

      /* One record being written, somewhere on the page. */
      UT_RELAX_CPU();
    }
  }

  /** Go to where a search says, and read what's there. */
  void snap_open(const Index *index, const DTuple *key, ulint mode, Edge edge, Mtr *mtr) noexcept {
    const auto space = index->get_space_id();
    auto &drain = shared(m_sys).m_drain;

    ut_d(mtr->m_reads_copies = true);

    m_pcur.get_btr_cur()->m_index = const_cast<Index *>(index);

    positioned(mode, BTR_SEARCH_LEAF);

    Drain::Guard guard{drain};
    Path path;

    auto page_no = descend(m_sys, index, key, mode, edge, 0, path);

    for (;;) {
      const auto savepoint = mtr->set_savepoint();
      const auto block = m_sys.m_btree->block_get(space, page_no, RW_NO_LATCH, mtr);
      RawLeaf leaf{};

      const auto version = read_stable(m_sys, block, [&]() noexcept { return read_leaf_raw(index, block->get_frame(), key, mode, edge, leaf); });

      if (leaf.m_kind == RawLeaf::FOUND && !snap_copy(block, leaf.m_offs, version)) {
        /* It changed: from the same page again. */
        mtr->release_block_at_savepoint(savepoint, block);
        continue;
      }

      if (leaf.m_kind == RawLeaf::FOUND) {
        m_snap = true;
        m_snap_block = block;
        m_snap_savepoint = savepoint;
        m_snap_version = version;
        m_snap_offs = leaf.m_offs;
        m_snap_last = snap_on_rec() ? leaf.m_offs : 0;

        if (!snap_on_rec()) {
          snap_keep_seek(key, mode);
        }

        return;
      }

      mtr->release_block_at_savepoint(savepoint, block);

      if (leaf.m_kind == RawLeaf::RIGHT) {
        page_no = leaf.m_next;
      } else {
        /* A merge took it, or the root went up since it was read. */
        page_no = descend(m_sys, index, key, mode, edge, 0, path);
      }
    }
  }

  /** @return the key of what the cursor is on or last was on, for searching again: null if there
  never was a record, then m_snap_seek is it. */
  [[nodiscard]] const DTuple *snap_key(MemHeap *heap) noexcept {
    const auto offs = snap_on_rec() ? m_snap_offs : m_snap_last;

    if (offs == 0) {
      return nullptr;
    }

    const auto index = m_pcur.get_btr_cur()->m_index;
    byte *buf{};
    ulint buf_size{};
    ulint n_fields;
    const auto copy = index->copy_rec_order_prefix(Rec(m_snap_frame.get() + offs), &n_fields, buf, buf_size);
    const auto rec_bytes = static_cast<byte *>(mem_heap_alloc(heap, buf_size));

    /* The tuple refers to the bytes, which are for the heap to keep. */
    const auto offset = static_cast<size_t>(copy.get() - buf);

    memcpy(rec_bytes, buf, buf_size);
    mem_free(buf);

    return index->build_data_tuple(Rec(rec_bytes + offset), n_fields, heap);
  }

  /** The page isn't what it was: find the place after the current one again, by key. */
  void snap_again(bool forward, Mtr *mtr) noexcept {
    const auto index = m_pcur.get_btr_cur()->m_index;
    auto heap = mem_heap_create(256);

    /* It has what's in the copy, this page has nothing more to give. */
    mtr->release_block_at_savepoint(m_snap_savepoint, m_snap_block);

    m_snap_block = nullptr;

    if (const auto key = snap_key(heap); key != nullptr) {
      /* Before what's on a supremum means the last one, which has gone with the same key or not. */
      const auto mode = forward ? PAGE_CUR_G : (m_snap_offs == PAGE_SUPREMUM ? PAGE_CUR_LE : PAGE_CUR_L);

      snap_open(index, key, mode, Edge::KEY, mtr);

    } else if (m_snap_seek != nullptr) {

      snap_open(index, m_snap_seek, forward ? (m_snap_mode == PAGE_CUR_G || m_snap_mode == PAGE_CUR_LE ? PAGE_CUR_G : PAGE_CUR_GE) : PAGE_CUR_L, Edge::KEY, mtr);

    } else {

      snap_open(index, nullptr, 0, forward ? Edge::FIRST : Edge::LAST, mtr);
    }

    mem_heap_free(heap);
  }

  /** On to a sibling, for a cursor that has run off a page that hasn't changed.
  @return false if that page isn't where the link says: the caller finds the place again. */
  [[nodiscard]] bool snap_sibling(page_no_t page_no, bool right, Mtr *mtr) noexcept {
    const auto index = m_pcur.get_btr_cur()->m_index;
    const auto from = m_snap_block;
    const auto from_savepoint = m_snap_savepoint;
    const auto frame = snap_frame();
    auto &drain = shared(m_sys).m_drain;
    Drain::Guard guard{drain};

    /* The number was read before the guard: a merge could have freed that page. If nothing has
    changed here since, it's still on the tree now, and from now on it stays a page of the tree. */
    if (from->read_begin() != m_snap_version) {
      return false;
    }

    const auto savepoint = mtr->set_savepoint();
    const auto block = m_sys.m_btree->block_get(index->get_space_id(), page_no, RW_NO_LATCH, mtr);
    const auto from_no = from->get_page_no();
    const auto offs = right ? PAGE_INFIMUM : PAGE_SUPREMUM;
    bool ok{};

    /* What's kept across pages for a place that's not a record. */
    {
      auto heap = mem_heap_create(256);

      if (const auto key = snap_key(heap); key != nullptr) {
        snap_keep_seek(key, right ? PAGE_CUR_G : PAGE_CUR_L);
      }

      mem_heap_free(heap);
    }

    const auto version = read_stable(m_sys, block, [&]() noexcept {
      const auto page = block->get_frame();

      /* Only the link back tells it's still the neighbour: what's left of a page can have changed. */
      ok = !is_dead(page, index) && (right || mach_read_from_4(page + FIL_PAGE_NEXT) == from_no);

      return !ok || read_rec_copy_raw(page, offs, frame);
    });

    if (!ok) {
      mtr->release_block_at_savepoint(savepoint, block);
      return false;
    }

    mtr->release_block_at_savepoint(from_savepoint, from);

    m_snap_block = block;
    m_snap_savepoint = savepoint;
    m_snap_version = version;
    m_snap_offs = offs;
    m_snap_last = 0;

    return true;
  }

  [[nodiscard]] bool snap_next(Mtr *mtr) noexcept {
    const auto frame = m_snap_frame.get();

    m_pcur.m_old_stored = false;

    if (m_snap_block->read_begin() == m_snap_version) {
      if (m_snap_offs == PAGE_SUPREMUM) {
        /* From the copy: it's what the page has. */
        if (!page_has_right(frame)) {
          return false;
        }

        if (snap_sibling(mach_read_from_4(frame + FIL_PAGE_NEXT), true, mtr)) {
          return true;
        }

      } else {
        const auto next = mach_read_from_2(frame + m_snap_offs - REC_NEXT);

        if (next == PAGE_SUPREMUM ? m_snap_block->read_is_valid(m_snap_version) : snap_copy(m_snap_block, next, m_snap_version)) {
          m_snap_offs = next;

          if (next != PAGE_SUPREMUM) {
            m_snap_last = next;
          }

          return true;
        }
      }
    }

    /* From the start of a key beyond this there can be nothing to go to, the search says. */
    const auto at_end = m_snap_offs == PAGE_SUPREMUM && m_snap_last == 0 && m_snap_seek == nullptr;

    if (at_end) {
      return false;
    }

    snap_again(true, mtr);

    return true;
  }

  [[nodiscard]] bool snap_prev(Mtr *mtr) noexcept {
    const auto frame = m_snap_frame.get();

    m_pcur.m_old_stored = false;

    if (const auto version = m_snap_version; m_snap_block->read_begin() == version) {
      if (m_snap_offs == PAGE_INFIMUM) {
        const auto prev_no = mach_read_from_4(frame + FIL_PAGE_PREV);

        if (prev_no == FIL_NULL) {
          return false;
        }

        if (snap_sibling(prev_no, false, mtr)) {
          return true;
        }

      } else {
        ulint prev{};
        bool ok;

        {
          [[maybe_unused]] RacyReads racy;

          ok = read_prev_raw(m_snap_block->get_frame(), m_snap_offs, prev);
        }

        const auto valid = m_snap_block->read_is_valid(version);

        ut_a(!valid || ok);

        /* The infimum needs no copy: the last record stays there, it's the key of this place. */
        if (valid && (prev == PAGE_INFIMUM || snap_copy(m_snap_block, prev, version))) {
          m_snap_offs = prev;

          /* At the infimum the last record stays in the copy: it's the key of this place. */
          if (prev != PAGE_INFIMUM) {
            m_snap_last = prev;
          }

          return true;
        }
      }
    }

    snap_again(false, mtr);

    return true;
  }

  /** For Intent::Mark: find the record with no ownership of anything, then own its group. The search
  needs nobody to hold still: nothing it reads changes under one group's bit, and the version tells
  when something else did. The cursor is on the page itself, like any other that holds it.
  @return true if the record is there. */
  [[nodiscard]] bool mark_open(const Index *index, const DTuple *key, Mtr *mtr) noexcept {
    /* The lock check of a secondary record writes PAGE_MAX_TRX_ID, which a group bit doesn't cover. */
    ut_a(index->is_clustered());

    const auto space = index->get_space_id();
    auto &drain = shared(m_sys).m_drain;
    Drain::Guard guard{drain};

    m_snap = false;
    m_path.clear();
    m_path.m_unlinks = drain.unlinks();

    auto page_no = descend(m_sys, index, key, PAGE_CUR_LE, Edge::KEY, 0, m_path);

    for (;;) {
      const auto savepoint = mtr->set_savepoint();
      const auto block = m_sys.m_btree->block_get(space, page_no, RW_NO_LATCH, mtr);
      RawLeaf leaf{};
      ulint bit{};

      const auto version = read_stable(m_sys, block, [&]() noexcept {
        const auto page = block->get_frame();

        return read_leaf_raw(index, page, key, PAGE_CUR_LE, Edge::KEY, leaf) &&
               (leaf.m_kind != RawLeaf::FOUND || leaf.m_offs == PAGE_INFIMUM || read_group_raw(page, leaf.m_offs, bit));
      });

      if (leaf.m_kind == RawLeaf::RIGHT) {
        mtr->release_block_at_savepoint(savepoint, block);
        page_no = leaf.m_next;
        continue;
      }

      if (leaf.m_kind != RawLeaf::FOUND) {
        mtr->release_block_at_savepoint(savepoint, block);
        page_no = descend(m_sys, index, key, PAGE_CUR_LE, Edge::KEY, 0, m_path);
        continue;
      }

      const auto cur = m_pcur.get_btr_cur();
      const auto on_rec = leaf.m_offs != PAGE_INFIMUM;

      if (on_rec) {
        block->group_lock(bit);

        /* The structure of the page has changed since the search: go by what it is now. */
        if (block->read_begin() != version) {
          block->group_unlock(bit);
          mtr->release_block_at_savepoint(savepoint, block);
          continue;
        }

        mtr->hold_group_at_savepoint(savepoint, block, bit);
      }

      block->set_check_index_page_at_flush();

      m_leaf_savepoint = savepoint;
      m_path.set(0, page_no);

      const Rec rec(block->get_frame() + leaf.m_offs);

      cur->get_page_cur()->m_index = const_cast<Index *>(index);
      cur->position(index, rec, block);

      positioned(PAGE_CUR_LE, BTR_MODIFY_LEAF);

      if (!on_rec) {
        return false;
      }

      MemHeap *heap{};
      const auto offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      const auto equal = cmp_dtuple_rec(index->m_cmp_ctx, key, rec, offs) == 0;

      if (heap != nullptr) {
        mem_heap_free(heap);
      }

      return equal;
    }
  }

  /** An insert in the frame of a thread that waits for it, on the stack of a page. */
  struct InsertOp {
    const Index *m_index{};
    DTuple *m_entry{};
    ulint m_flags{};
    QueThr *m_thr{};
    DbErr m_err{DB_FAIL};
    InsertOp *m_next{};

    /** The result is there, nothing of this is touched by the owner after this is set. */
    std::atomic<bool> m_done{};
  };

  /** Do one insert on a page this thread owns, in its mini-transaction.
  @return DB_FAIL with nothing done if it's for the normal way. */
  [[nodiscard]] DbErr apply_insert(BufBlock *block, const InsertOp &op, Mtr *mtr) noexcept {
    const auto index = op.m_index;
    const auto entry = op.m_entry;
    const auto page = block->get_frame();

    if (is_dead(page, index) || m_sys.m_btree->page_get_level(page, mtr) != 0 || past_high_key(index, page, entry, PAGE_CUR_LE)) {
      return DB_FAIL;
    }

    const auto cur = m_pcur.get_btr_cur();
    const auto page_cur = cur->get_page_cur();
    ulint up_match{};
    ulint up_bytes{};
    ulint low_match{};
    ulint low_bytes{};

    page_cur->search_with_match(m_sys, block, index, entry, PAGE_CUR_LE, &up_match, &up_bytes, &low_match, &low_bytes);

    /* A duplicate, or a delete marked record to reuse: not something a batch decides. */
    const auto n_unique = index->get_n_unique();

    if (up_match >= n_unique || low_match >= n_unique) {
      return DB_FAIL;
    }

    cur->position(index, page_cur->get_rec(), block);
    cur->m_thr = op.m_thr;

    BigRec big_rec{};
    Rec rec;

    /* It fits the page or it doesn't: DB_FAIL. A lock it has to wait for is the transaction's, its
    thread sees DB_LOCK_WAIT as it would have here. */
    return cur->optimistic_insert(m_sys, op.m_flags, entry, &rec, &big_rec, 0, op.m_thr, mtr);
  }

  /** Own a page, do what's queued on it, commit. The queue is taken once: what comes after that is for
  the next owner, or for the ones that queued it to become that. The waiters go on after the commit,
  which is the point of the batch: all of this is one trip to the log and one release of the page.
  @return false if the page wasn't free. */
  [[nodiscard]] bool run_batch(BufBlock *block) noexcept {
    if (!block->bits_try_lock()) {
      return false;
    }

    auto op = static_cast<InsertOp *>(block->m_queued_ops.exchange(nullptr, std::memory_order_acquire));

    if (op == nullptr) {
      block->bits_unlock();

      return true;
    }

    Mtr mtr;

    mtr.start();

    const auto savepoint = mtr.set_savepoint();

    (void)m_sys.m_btree->block_get(block->get_space(), block->get_page_no(), RW_NO_LATCH, &mtr);

    mtr.hold_bits_at_savepoint(savepoint, block);

    /* In the order they came: the stack has them the other way around. */
    InsertOp *ordered{};

    while (op != nullptr) {
      const auto next = op->m_next;

      op->m_next = ordered;
      ordered = op;
      op = next;
    }

    for (auto at = ordered; at != nullptr; at = at->m_next) {
      at->m_err = apply_insert(block, *at, &mtr);
    }

    mtr.commit(m_sys);

    size_t n_ops{};

    while (ordered != nullptr) {
      const auto next = ordered->m_next;

      ++n_ops;

      ordered->m_done.store(true, std::memory_order_release);

      ordered = next;
    }

#ifdef UNIT_TESTING
    shared(m_sys).m_n_batched.fetch_add(n_ops - 1, std::memory_order_relaxed);
#else
    (void)n_ops;
#endif /* UNIT_TESTING */

    return true;
  }

  DbErr insert_batched(const Index *index, DTuple *entry, ulint flags, QueThr *thr) noexcept override {
    if (!m_sys.m_config.m_dict.m_insert_batch || unlikely(key_size(index, entry) > max_key_size()) ||
        page_rec_needs_ext(rec_get_converted_size(index, entry, 0))) {
      return DB_FAIL;
    }

    m_snap = false;

    m_pcur.get_btr_cur()->m_index = const_cast<Index *>(index);

    /* Nothing needs this to be a page of the tree later: whoever does the work looks. */
    Mtr mtr;
    BufBlock *block;

    mtr.start();

    {
      Drain::Guard guard{shared(m_sys).m_drain};
      Path path;

      block = m_sys.m_btree->block_get(index->get_space_id(), descend(m_sys, index, entry, PAGE_CUR_LE, Edge::KEY, 0, path), RW_NO_LATCH, &mtr);
    }

    InsertOp op;

    op.m_index = index;
    op.m_entry = entry;
    op.m_flags = flags;
    op.m_thr = thr;

    /* Doing it itself is a batch of one, or more if some came while it found the page. */
    auto head = block->m_queued_ops.load(std::memory_order_relaxed);

    do {
      op.m_next = static_cast<InsertOp *>(head);
    } while (!block->m_queued_ops.compare_exchange_weak(head, &op, std::memory_order_release, std::memory_order_relaxed));

    constexpr int N_SPINS = 64;

    for (int i{}; !op.m_done.load(std::memory_order_acquire); ++i) {
      if (run_batch(block)) {
        continue;
      }

      if (i < N_SPINS) {
        UT_RELAX_CPU();

      } else if (const auto bits = block->m_bits.load(std::memory_order_relaxed); (bits & BufBlock::ALL_BITS) != 0) {
        /* Only with somebody holding it: their release wakes this. */
        block->bits_wait(bits);

      } else {
        std::this_thread::yield();
      }
    }

    mtr.commit(m_sys);

    return op.m_err;
  }

  /** The usual way back for Intent::Mark: the row was found just now and the page hasn't changed.
  @return true if it's held. */
  [[nodiscard]] bool mark_guess(Mtr *mtr) noexcept {
    ulint savepoint{};
    const auto block = guess_page(savepoint, mtr);

    if (block == nullptr) {
      return false;
    }

    const auto version = m_guess_version;
    const auto offs = m_guess_offs;
    ulint bit{};
    bool ok;

    {
      [[maybe_unused]] RacyReads racy;

      ok = read_group_raw(block->get_frame(), offs, bit);
    }

    if (block->read_is_valid(version)) {
      ut_a(ok);

      block->group_lock(bit);

      if (block->read_begin() == version) {
        mtr->hold_group_at_savepoint(savepoint, block, bit);

        const auto cur = m_pcur.get_btr_cur();

        m_snap = false;
        m_leaf_savepoint = savepoint;
        m_path.clear();

        cur->position(cur->m_index, Rec(block->get_frame() + offs), block);

        positioned(PAGE_CUR_LE, BTR_MODIFY_LEAF);

        m_pcur.m_old_stored = true;
        m_pcur.m_rel_pos = BtreeCursorPos::ON;

        return true;
      }

      block->group_unlock(bit);
    }

    mtr->release_block_at_savepoint(savepoint, block);

    return false;
  }

  /** Save the place by key: there's nothing to come back to on the page. */
  void snap_save() noexcept {
    const auto index = m_pcur.get_btr_cur()->m_index;
    const auto offs = snap_on_rec() ? m_snap_offs : m_snap_last;

    m_guess_block = m_snap_block;
    m_guess_version = m_snap_version;
    m_guess_snap = true;
    m_snap_mode_open = ulint(m_pcur.m_search_mode);

    m_pcur.m_old_stored = true;
    m_pcur.m_block_when_stored = nullptr;

    if (offs != 0) {
      m_pcur.m_rel_pos = snap_on_rec() ? BtreeCursorPos::ON : (m_snap_offs == PAGE_SUPREMUM ? BtreeCursorPos::AFTER : BtreeCursorPos::BEFORE);
      m_pcur.m_old_rec = index->copy_rec_order_prefix(Rec(m_snap_frame.get() + offs), &m_pcur.m_old_n_fields, m_pcur.m_old_rec_buf, m_pcur.m_buf_size);
      return;
    }

    if (m_snap_seek == nullptr) {
      m_pcur.m_rel_pos = m_snap_offs == PAGE_SUPREMUM ? BtreeCursorPos::AFTER_LAST_IN_TREE : BtreeCursorPos::BEFORE_FIRST_IN_TREE;
      return;
    }

    /* Nothing was found on the way here but where it was looked for: the key, as a record. */
    const auto size = rec_get_converted_size(index, m_snap_seek, 0);

    if (m_pcur.m_buf_size < size) {
      if (m_pcur.m_old_rec_buf != nullptr) {
        mem_free(m_pcur.m_old_rec_buf);
      }

      m_pcur.m_old_rec_buf = static_cast<byte *>(mem_alloc(size));
      m_pcur.m_buf_size = size;
    }

    m_pcur.m_old_rec = Rec::convert_dtuple_to_rec(m_pcur.m_old_rec_buf, index, m_snap_seek, 0);
    m_pcur.m_old_n_fields = dtuple_get_n_fields(m_snap_seek);
    m_pcur.m_rel_pos = m_snap_mode == PAGE_CUR_G || m_snap_mode == PAGE_CUR_LE ? BtreeCursorPos::AFTER : BtreeCursorPos::BEFORE;
  }

  /** Back on a page saved from, buffer fixed, if it's still the page it was: then the place is too.
  @return the block, null if there's no telling. */
  [[nodiscard]] BufBlock *guess_page(ulint &savepoint, Mtr *mtr) noexcept {
    if (m_guess_block == nullptr) {
      return nullptr;
    }

    savepoint = mtr->set_savepoint();

    BufPool::Request req{.m_rw_latch = RW_NO_LATCH, .m_guess = m_guess_block, .m_mode = BUF_KEEP_OLD, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr};

    const auto block = m_guess_block;

    m_guess_block = nullptr;

    if (!m_sys.m_buf_pool->try_get(req)) {
      return nullptr;
    }

    if (block->read_begin() == m_guess_version) {
      return block;
    }

    mtr->release_block_at_savepoint(savepoint, block);

    return nullptr;
  }

  [[nodiscard]] bool snap_restore(Mtr *mtr) noexcept {
    const auto index = m_pcur.get_btr_cur()->m_index;
    const auto rel_pos = m_pcur.m_rel_pos;

    /* A scan saves and restores for each row: nothing has moved, most of the time. */
    if (ulint savepoint{}; m_snap && m_guess_snap) {
      if (const auto block = guess_page(savepoint, mtr); block != nullptr) {
        ut_d(mtr->m_reads_copies = true);

        /* The record can have been written since, it's the same record. */
        if (rel_pos != BtreeCursorPos::ON || snap_copy(block, m_snap_offs, m_guess_version)) {
          m_snap_block = block;
          m_snap_savepoint = savepoint;
          m_snap_version = m_guess_version;

          m_pcur.m_old_stored = true;

          positioned(m_snap_mode_open, BTR_SEARCH_LEAF);

          m_pcur.m_old_stored = true;

          return rel_pos == BtreeCursorPos::ON;
        }

        mtr->release_block_at_savepoint(savepoint, block);
      }
    }

    if (rel_pos == BtreeCursorPos::AFTER_LAST_IN_TREE || rel_pos == BtreeCursorPos::BEFORE_FIRST_IN_TREE) {
      snap_open(index, nullptr, 0, rel_pos == BtreeCursorPos::BEFORE_FIRST_IN_TREE ? Edge::FIRST : Edge::LAST, mtr);

      m_pcur.m_rel_pos = rel_pos;
      m_pcur.m_old_stored = true;

      return false;
    }

    auto heap = mem_heap_create(256);
    const auto key = index->build_data_tuple(m_pcur.m_old_rec, m_pcur.m_old_n_fields, heap);
    const auto mode = rel_pos == BtreeCursorPos::ON ? PAGE_CUR_LE : (rel_pos == BtreeCursorPos::AFTER ? PAGE_CUR_G : PAGE_CUR_L);

    snap_open(index, key, mode, Edge::KEY, mtr);

    bool found{};

    if (rel_pos == BtreeCursorPos::ON && snap_on_rec()) {
      const Rec rec(m_snap_frame.get() + m_snap_offs);
      const auto offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

      found = cmp_dtuple_rec(index->m_cmp_ctx, key, rec, offs) == 0;
    }

    m_pcur.m_rel_pos = rel_pos;
    m_pcur.m_old_stored = true;

    mem_heap_free(heap);

    return found;
  }

  struct FrameFree {
    void operator()(byte *frame) const noexcept { std::free(frame); }
  };

  std::unique_ptr<byte, FrameFree> m_snap_frame;
  std::unique_ptr<byte, FrameFree> m_snap_spare;

  /** The page of the saved position, and its version then. */
  BufBlock *m_guess_block{};
  uint64_t m_guess_version{};
  ulint m_guess_offs{};

  /** The saved position is of a read that holds nothing, and the copy still has it. */
  bool m_guess_snap{};
  ulint m_snap_mode_open{};
  bool m_snap{};
  BufBlock *m_snap_block{};
  ulint m_snap_savepoint{};
  uint64_t m_snap_version{};
  ulint m_snap_offs{};

  /** The last record copied from this page, 0 if none: it's still in the copy. */
  ulint m_snap_last{};

  MemHeap *m_snap_heap{};
  DTuple *m_snap_seek{};
  ulint m_snap_mode{};

  ~BlinkCursor() noexcept override {
    if (m_snap_heap != nullptr) {
      mem_heap_free(m_snap_heap);
    }
  }

  /** Latch a leaf in mtr. Its latch is the only thing this adds to the memo: m_leaf_savepoint.
  @return the block, null if the page isn't a leaf: the root on a raise, or a page that isn't on
  the tree any more. Only then, a page reached by a sibling's link is always on it. */
  [[nodiscard]] BufBlock *latch_leaf(const Index *index, page_no_t page_no, ulint latch, Mtr *mtr) noexcept {
    const auto savepoint = mtr->set_savepoint();
    const auto block = m_sys.m_btree->block_get(index->get_space_id(), page_no, RW_B_LATCH, mtr);

    const auto page = block->get_frame();

    if (unlikely(is_dead(page, index) || m_sys.m_btree->page_get_level(page, mtr) != 0)) {
      mtr->release_block_at_savepoint(savepoint, block);
      return nullptr;
    }

    block->set_check_index_page_at_flush();

    m_leaf_savepoint = savepoint;

    return block;
  }

  /** Move a latch to the right sibling: left to right coupling. */
  [[nodiscard]] BufBlock *couple_right(const Index *index, BufBlock *block, ulint latch, Mtr *mtr) noexcept {
    const auto left_savepoint = m_leaf_savepoint;
    const auto right = latch_leaf(index, Btree::page_get_next(block->get_frame(), mtr), latch, mtr);

    ut_a(right != nullptr);

    mtr->release_block_at_savepoint(left_savepoint, block);

    return right;
  }

  void positioned(ulint mode, ulint latch) noexcept {
    m_pcur.m_latch_mode = latch;
    m_pcur.m_search_mode = SearchMode(mode);
    m_pcur.m_old_stored = false;
    m_pcur.m_trx_if_known = nullptr;
    m_pcur.m_pos_state = BtrPcurPositioned::IS_POSITIONED;
  }

  /** @return the leaf for key, with latch, moved right as far as it takes. */
  [[nodiscard]] BufBlock *to_leaf(const Index *index, const DTuple *key, ulint mode, ulint latch, Mtr *mtr) noexcept {
    auto &drain = shared(m_sys).m_drain;
    BufBlock *block{};

    {
      Drain::Guard guard{drain};

      m_path.clear();
      m_path.m_unlinks = drain.unlinks();

      while (block == nullptr) {
        block = latch_leaf(index, descend(m_sys, index, key, mode, Edge::KEY, 0, m_path), latch, mtr);
      }
    }

    while (past_high_key(index, block->get_frame(), key, mode)) {
      block = couple_right(index, block, latch, mtr);
    }

    m_path.set(0, block->get_page_no());

    return block;
  }

  /** Position with the search of the leaf. */
  void search(const Index *index, const DTuple *key, ulint mode, ulint latch, Mtr *mtr) noexcept {
    auto block = to_leaf(index, key, mode, latch, mtr);
    auto cur = m_pcur.get_btr_cur();
    ulint up_match{};
    ulint up_bytes{};
    ulint low_match{};
    ulint low_bytes{};

    cur->get_page_cur()->search_with_match(m_sys, block, index, key, mode, &up_match, &up_bytes, &low_match, &low_bytes);

    cur->position(index, cur->get_page_cur()->get_rec(), block);

    cur->m_up_match = up_match;
    cur->m_up_bytes = up_bytes;
    cur->m_low_match = low_match;
    cur->m_low_bytes = low_bytes;
    cur->m_tree_height = m_path.m_height + 1;

    positioned(mode, latch);
  }

  void seek(Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation) noexcept override {
    ut_a(mode == PAGE_CUR_GE || mode == PAGE_CUR_G);

    if (intent == Intent::Snapshot) {
      snap_open(index, key, mode, Edge::KEY, mtr);

      if (!on_rec()) {
        (void)next(mtr);
      }

      return;
    }

    m_snap = false;

    search(index, key, mode, leaf_latch(intent), mtr);

    if (m_pcur.is_after_last_on_page()) {
      (void)next(mtr);
    }
  }

  void position(const Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation) noexcept override {
    if (intent == Intent::Snapshot) {
      snap_open(index, key, mode, Edge::KEY, mtr);
      return;
    }

    m_snap = false;

    search(index, key, mode, leaf_latch(intent), mtr);
  }

  void seek_insert(const Index *index, const DTuple *entry, Intent intent, QueThr *thr, Mtr *mtr, SourceLocation) noexcept override {
    ut_ad(intent != Intent::Read && intent != Intent::Snapshot);

    m_snap = false;

    m_pcur.get_btr_cur()->m_thr = thr;

    search(index, entry, PAGE_CUR_LE, BTR_MODIFY_LEAF, mtr);
  }

  bool find(const Index *index, const DTuple *key, Intent intent, Mtr *mtr, SourceLocation) noexcept override {
    if (intent == Intent::Snapshot) {
      snap_open(index, key, PAGE_CUR_LE, Edge::KEY, mtr);

      if (!snap_on_rec()) {
        return false;
      }

      MemHeap *heap{};
      const Rec rec(m_snap_frame.get() + m_snap_offs);
      const auto offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
      const auto equal = cmp_dtuple_rec(index->m_cmp_ctx, key, rec, offs) == 0;

      if (heap != nullptr) {
        mem_heap_free(heap);
      }

      return equal;
    }

    if (intent == Intent::Mark) {
      return mark_open(index, key, mtr);
    }

    m_snap = false;

    search(index, key, PAGE_CUR_LE, leaf_latch(intent), mtr);

    return !page_rec_is_infimum(m_pcur.get_rec()) && m_pcur.get_low_match() == dtuple_get_n_fields_cmp(key);
  }

  void seek_edge(const Index *index, bool first, Intent intent, Mtr *mtr, SourceLocation) noexcept override {
    if (intent == Intent::Snapshot) {
      snap_open(index, nullptr, 0, first ? Edge::FIRST : Edge::LAST, mtr);
      return;
    }

    m_snap = false;

    const auto latch = leaf_latch(intent);
    const auto edge = first ? Edge::FIRST : Edge::LAST;
    auto &drain = shared(m_sys).m_drain;
    BufBlock *block{};

    {
      Drain::Guard guard{drain};

      m_path.clear();
      m_path.m_unlinks = drain.unlinks();

      while (block == nullptr) {
        block = latch_leaf(index, descend(m_sys, index, nullptr, 0, edge, 0, m_path), latch, mtr);
      }
    }

    /* A split while nothing was held: the last page is further right. */
    while (!first && page_has_right(block->get_frame())) {
      block = couple_right(index, block, latch, mtr);
    }

    auto cur = m_pcur.get_btr_cur();

    if (first) {
      cur->get_page_cur()->set_before_first(block);
    } else {
      cur->get_page_cur()->set_after_last(block);
    }

    cur->position(index, cur->get_page_cur()->get_rec(), block);

    positioned(first ? PAGE_CUR_G : PAGE_CUR_L, latch);
  }

  /** To the right sibling: left to right coupling. Before its first record. */
  void move_right(Mtr *mtr) noexcept {
    auto cur = m_pcur.get_btr_cur();
    const auto block = couple_right(cur->m_index, cur->get_block(), m_pcur.m_latch_mode, mtr);

    cur->get_page_cur()->set_before_first(block);
  }

  /** To the left sibling, as Postgres: left links are best effort, and nothing waits for a page to
  the left holding a latch. So with nothing held, latch the page the left link said; accept it if
  its right link is the page we were on. If not it has split, or one of them was merged: go back by
  the key, which is what the cursor saved, to the record before. Else after its last record. The mtr
  commits, as for the old tree. */
  void move_left(Mtr *mtr) noexcept {
    auto cur = m_pcur.get_btr_cur();
    const auto index = cur->m_index;
    const auto latch = m_pcur.m_latch_mode;
    const auto block = cur->get_block();
    const auto page_no = block->get_page_no();
    const auto left_no = Btree::page_get_prev(block->get_frame(), mtr);

    save(mtr);

    /* It leads to where the last search went, and a split moves right from there: from here on
    that can be too far. */
    m_path.clear();

    {
      /* The left link is a page of this tree until this has looked. */
      Drain::Guard guard{shared(m_sys).m_drain};

      mtr->commit(m_sys);
      mtr->start();

      const auto left = latch_leaf(index, left_no, latch, mtr);

      if (left != nullptr) {
        if (Btree::page_get_next(left->get_frame(), mtr) == page_no) {
          cur->get_page_cur()->set_after_last(left);
          cur->position(index, cur->get_page_cur()->get_rec(), left);
          m_pcur.m_pos_state = BtrPcurPositioned::IS_POSITIONED;
          m_pcur.m_old_stored = false;
          return;
        }

        mtr->release_block_at_savepoint(m_leaf_savepoint, left);
      }
    }

    (void)restore(latch == BTR_SEARCH_LEAF ? Intent::Read : Intent::Write, mtr, Current_location());

    m_pcur.m_old_stored = false;
  }

  bool next_pos(Mtr *mtr) noexcept override {
    if (m_snap) {
      return snap_next(mtr);
    }

    m_pcur.m_old_stored = false;

    if (m_pcur.is_after_last_on_page()) {
      if (!page_has_right(m_pcur.get_block()->get_frame())) {
        return false;
      }

      move_right(mtr);

      return true;
    }

    m_pcur.get_page_cur()->move_to_next();

    return true;
  }

  bool prev_pos(Mtr *mtr) noexcept override {
    if (m_snap) {
      return snap_prev(mtr);
    }

    m_pcur.m_old_stored = false;

    if (m_pcur.is_before_first_on_page()) {
      if (Btree::page_get_prev(m_pcur.get_block()->get_frame(), mtr) == FIL_NULL) {
        return false;
      }

      move_left(mtr);

      return true;
    }

    m_pcur.get_page_cur()->move_to_prev();

    return true;
  }

  /* Empty pages are legal: both go on until there's a record. */
  bool next(Mtr *mtr) noexcept override {
    while (next_pos(mtr)) {
      if (on_rec()) {
        return true;
      }
    }

    return false;
  }

  bool prev(Mtr *mtr) noexcept override {
    while (prev_pos(mtr)) {
      if (on_rec()) {
        return true;
      }
    }

    return false;
  }

  void skip_rest(Mtr *mtr) noexcept override {
    if (m_snap) {
      m_snap_offs = PAGE_SUPREMUM;
    } else {
      m_pcur.move_to_last_on_page(mtr);
    }
  }

  bool on_rec() const noexcept override { return m_snap ? snap_on_rec() : m_pcur.is_on_user_rec(); }

  Rec rec() noexcept override { return m_snap ? Rec(m_snap_frame.get() + m_snap_offs) : m_pcur.get_rec(); }

  const Index *index() const noexcept override { return m_pcur.get_btr_cur()->m_index; }

  /* These are about a page that's held. */
  ulint low_match() noexcept override {
    ut_a(!m_snap);

    return m_pcur.get_low_match();
  }

  ulint up_match() noexcept override {
    ut_a(!m_snap);

    return m_pcur.get_up_match();
  }

  SearchMode search_mode() const noexcept override { return m_pcur.m_search_mode; }

  BufBlock *block() noexcept override {
    ut_a(!m_snap);

    return m_pcur.get_block();
  }

  /** An empty page has no record to be relative to, as in the old tree, but here it can be
  anywhere on the level: just before the high key is where it is. Without a high key it's after
  the last record, whichever the side of the empty page. Only when it's the only page is it
  also the old tree's case, of the empty tree. */
  void save(Mtr *mtr) noexcept override {
    if (m_snap) {
      snap_save();

      m_guess_offs = m_snap_offs;

      return;
    }

    save_position(m_sys, m_pcur, mtr);

    /* For Intent::Mark: this holds all of the page, the version is one more when it lets go. */
    if (const auto block = m_pcur.get_block(); m_pcur.m_rel_pos == BtreeCursorPos::ON) {
      const auto version = block->read_begin();

      m_guess_block = block;
      m_guess_version = version + (version & 1);
      m_guess_offs = page_offset(m_pcur.get_rec());
      m_guess_snap = false;
    } else {
      m_guess_block = nullptr;
    }
  }

  bool restore(Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    ut_a(m_pcur.m_old_stored);

    if (intent == Intent::Snapshot) {
      return snap_restore(mtr);
    }

    if (intent == Intent::Mark) {
      /* A record is all it's ever saved on for this. */
      ut_a(m_pcur.m_rel_pos == BtreeCursorPos::ON);
      ut_a(m_pcur.get_btr_cur()->m_index->is_clustered());

      if (mark_guess(mtr)) {
        return true;
      }

      const auto index = m_pcur.get_btr_cur()->m_index;
      auto heap = mem_heap_create(256);
      const auto found = mark_open(index, index->build_data_tuple(m_pcur.m_old_rec, m_pcur.m_old_n_fields, heap), mtr);

      m_pcur.m_old_stored = true;
      m_pcur.m_rel_pos = BtreeCursorPos::ON;

      mem_heap_free(heap);

      return found;
    }

    m_snap = false;

    const auto index = m_pcur.get_btr_cur()->m_index;
    const auto latch = leaf_latch(intent);

    if (m_pcur.m_rel_pos == BtreeCursorPos::AFTER_LAST_IN_TREE || m_pcur.m_rel_pos == BtreeCursorPos::BEFORE_FIRST_IN_TREE) {
      const auto saved = m_pcur.m_rel_pos;

      seek_edge(index, saved == BtreeCursorPos::BEFORE_FIRST_IN_TREE, intent, mtr, loc);

      m_pcur.m_rel_pos = saved;
      m_pcur.m_old_stored = true;
      m_pcur.m_block_when_stored = m_pcur.get_block();

      return false;
    }

    m_leaf_savepoint = mtr->set_savepoint();

    /* See move_left(). A search below sets it again. */
    m_path.clear();

    bool on_rec;

    if (m_pcur.m_block_when_stored != nullptr && m_pcur.restore_optimistic(m_sys, latch, mtr, loc, on_rec, RW_B_LATCH)) {
      /* The next move takes it from where it is, nothing here cares how it got there. */
      m_pcur.m_pos_state = BtrPcurPositioned::IS_POSITIONED;

      return on_rec;
    }

    /* The page changed: seek by the key. */
    auto heap = mem_heap_create(256);
    const auto key = index->build_data_tuple(m_pcur.m_old_rec, m_pcur.m_old_n_fields, heap);
    const auto saved_mode = m_pcur.m_search_mode;
    const auto rel_pos = m_pcur.m_rel_pos;

    ulint mode;

    if (rel_pos == BtreeCursorPos::ON) {
      mode = PAGE_CUR_LE;
    } else if (rel_pos == BtreeCursorPos::AFTER) {
      mode = PAGE_CUR_G;
    } else {
      ut_ad(rel_pos == BtreeCursorPos::BEFORE);
      mode = PAGE_CUR_L;
    }

    search(index, key, mode, latch, mtr);

    m_pcur.m_search_mode = saved_mode;

    bool found{};
    const auto rec = m_pcur.get_rec();

    if (rel_pos == BtreeCursorPos::ON && m_pcur.is_on_user_rec()) {
      const auto offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

      found = cmp_dtuple_rec(index->m_cmp_ctx, key, rec, offs) == 0;
    }

    if (found) {
      /* The new values, it can be on another page, but the same key. */
      m_pcur.m_block_when_stored = m_pcur.get_block();
      m_pcur.m_modify_clock = buf_block_get_modify_clock(m_pcur.m_block_when_stored);
      m_pcur.m_old_stored = true;
    } else {
      save(mtr);
    }

    mem_heap_free(heap);

    return found;
  }

  SavedAt saved_at() const noexcept override {
    ut_ad(m_pcur.m_old_stored);

    switch (m_pcur.m_rel_pos) {
      case BtreeCursorPos::UNSET:
        return SavedAt::None;
      case BtreeCursorPos::ON:
        return SavedAt::On;
      case BtreeCursorPos::BEFORE:
      case BtreeCursorPos::BEFORE_FIRST_IN_TREE:
        return SavedAt::Before;
      case BtreeCursorPos::AFTER:
      case BtreeCursorPos::AFTER_LAST_IN_TREE:
        return SavedAt::After;
    }

    ut_error;
  }

  bool has_saved() const noexcept override {
    return m_pcur.m_old_stored &&
           (m_pcur.m_pos_state == BtrPcurPositioned::IS_POSITIONED || m_pcur.m_pos_state == BtrPcurPositioned::WAS_POSITIONED);
  }

  void copy_saved(IndexAm::Cursor &src) noexcept override { m_guess_block = nullptr; m_pcur.copy_stored_position(&static_cast<BlinkCursor &>(src).m_pcur); }

  bool give_up_page(Mtr *mtr) noexcept override {
    if (!m_snap || m_snap_block == nullptr) {
      return false;
    }

    mtr->release_block_at_savepoint(m_snap_savepoint, m_snap_block);

    m_snap_block = nullptr;

    return true;
  }

  void commit(Mtr *mtr) noexcept override {
    m_snap_block = nullptr;
    m_pcur.commit_specify_mtr(m_sys, mtr);
  }

  void finish(Smo &smo, Mtr *mtr) noexcept {
    if (!smo.started()) {
      return;
    }

    auto heap = mem_heap_create(256);
    DTuple *key;

    const auto index = m_pcur.get_btr_cur()->m_index;

    smo.complete(&key, heap);

    /* What the caller saved before this call is for it to restore(), by key, after. */
    const auto old_stored = m_pcur.m_old_stored;

    /* The mini-transaction was committed by the split. Back on the record, by its key: since then
    its page can have split, or been merged. */
    search(index, key, PAGE_CUR_LE, BTR_MODIFY_LEAF, mtr);

    m_pcur.m_old_stored = old_stored;

    mem_heap_free(heap);
  }

  /** When a page gets small this commits the mini-transaction for a merge, and starts it again
  holding nothing: all a caller does after this is commit. */
  DbErr remove(Intent intent, TrxRbCtx rb_ctx, Mtr *mtr) noexcept override {
    ut_a(intent != Intent::Read);

    auto cur = m_pcur.get_btr_cur();
    const auto index = cur->get_index();
    const auto rec = cur->get_rec();
    const auto block = cur->get_block();

    ut_ad(page_is_leaf(block->get_frame()));
    ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

    auto heap = mem_heap_create(1024);
    RecOffs offsets = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

    const auto page = block->get_frame();
    DTuple *merge_key{};
    byte *key_buf{};

    if (merge_is_on(m_sys) && block->get_page_no() != index->get_page_no() && Merge::is_small(page_get_rec_data_size(page) - offsets.size())) {
      ulint n_fields;
      ulint key_buf_size{};

      const auto copy = index->copy_rec_order_prefix(rec, &n_fields, key_buf, key_buf_size);

      merge_key = index->build_data_tuple(copy, n_fields, heap);
    }

    if (offsets.any_extern()) {
      Mtr blob_mtr;
      Blob blob{};

      /* A short one: see the proof in the header. */
      blob_mtr.start(mtr);

      m_sys.m_fsp->latch(index->get_space_id(), &blob_mtr);

      blob.free_externally_stored_fields(m_sys, index, rec, offsets, rb_ctx, &blob_mtr);

      blob_mtr.commit(m_sys);
    }

    m_sys.m_lock_sys->update_delete(block, rec);

    cur->get_page_cur()->delete_rec(m_sys, index, offsets, mtr);

    if (merge_key != nullptr) {
      mtr->commit(m_sys);

      Merge(m_sys, index, merge_key).run();

      mtr->start();
    } else {
      free_dead_pages(m_sys);
    }

    if (key_buf != nullptr) {
      mem_free(key_buf);
    }

    mem_heap_free(heap);

    return DB_SUCCESS;
  }

  DbErr update(
    Intent intent, ulint flags, MemHeap **heap, BigRec *big_rec, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr
  ) noexcept override {
    ut_a(intent != Intent::Read);

    auto cur = m_pcur.get_btr_cur();

    /* The one case of DB_FAIL: a caller that gave no place for the fields that may have to be
    stored externally asks for a Write only, see IndexAm: it can't get them. */
    if (big_rec == nullptr) {
      const auto err = cur->optimistic_update(m_sys, flags, update, cmpl_info, thr, mtr);

      return err == DB_OVERFLOW || err == DB_UNDERFLOW ? DB_FAIL : err;
    }

    Smo smo{m_sys, cur->get_index(), m_path, m_leaf_savepoint, mtr};

    const auto err = cur->update_restructure(m_sys, smo, flags, heap, big_rec, update, cmpl_info, thr, mtr);

    finish(smo, mtr);

    return err;
  }

  DbErr update_in_place(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr) noexcept override {
    return m_pcur.get_btr_cur()->update_in_place(m_sys, flags, update, cmpl_info, thr, mtr);
  }

  DbErr insert(Intent intent, ulint flags, DTuple *entry, BigRec *big_rec, ulint n_ext, QueThr *thr, Mtr *mtr) noexcept override {
    ut_a(intent != Intent::Read);

    auto cur = m_pcur.get_btr_cur();
    const auto index = cur->get_index();

    if (unlikely(key_size(index, entry) > max_key_size())) {
      *big_rec = BigRec{};

      return DB_TOO_BIG_RECORD;
    }

    Smo smo{m_sys, index, m_path, m_leaf_savepoint, mtr};
    Rec rec;

    const auto err = cur->insert_restructure(m_sys, smo, flags, entry, &rec, big_rec, n_ext, thr, mtr);

    if (err == DB_SUCCESS && !smo.started() && page_right_unlinked(cur->get_block()->get_frame())) {
      smo.complete_for(cur->get_block(), rec);
    }

    finish(smo, mtr);

    return err;
  }

  DbErr store_big_rec(const RecOffs &offsets, const BigRec &big_rec, Mtr *mtr) noexcept override {
    Blob blob{};

    return blob.store_big_rec_extern_fields(m_sys, m_pcur.get_btr_cur()->m_index, m_pcur.get_block(), m_pcur.get_rec(), offsets, big_rec, mtr);
  }

  DbErr set_delete_mark(ulint flags, bool val, QueThr *thr, Mtr *mtr) noexcept override {
    auto cur = m_pcur.get_btr_cur();

    if (cur->get_index()->is_clustered()) {
      return cur->del_mark_set_clust_rec(m_sys, flags, val, thr, mtr);
    } else {
      return cur->del_mark_set_sec_rec(m_sys, flags, val, thr, mtr);
    }
  }

  void close() noexcept override {
    m_snap = false;
    m_pcur.close();
    m_path.clear();
  }

  System &m_sys;
  BtreePcursor m_pcur{};

  /** The way down, for a split. */
  Path m_path{};

  /** Where the latch of the page of the cursor is in the mini-transaction's memo. */
  ulint m_leaf_savepoint{};
};

struct Am final : IndexAm {
  explicit Am(System &sys) noexcept : m_sys(sys) {}

  ~Am() noexcept override {
    /* After the shutdown: a system transaction that's still open is for the next start. */
    m_shared.m_drain.release_all();
  }

  std::unique_ptr<IndexAm::Cursor> cursor() override { return std::make_unique<BlinkCursor>(m_sys); }

  /* A new tree and a dropped one are the same pages as the B-tree's. */
  Root create(ulint type, space_id_t space, uint64_t index_id, Index *index, Mtr *mtr) noexcept override {
    return m_sys.m_btree->create(type, space, index_id, index, mtr);
  }

  void drop_data(space_id_t space, Root root) noexcept override {
    m_shared.m_drain.forget(m_sys, space, root);

    m_sys.m_btree->free_but_not_root(space, root);
  }

  void drop_root(space_id_t space, Root root, Mtr *mtr) noexcept override { m_sys.m_btree->free_root(space, root, mtr); }

  ulint n_pages(const Index *index) noexcept override {
    free_dead_pages(m_sys);

    return m_sys.m_btree->get_size(index, BTR_TOTAL_SIZE);
  }

  ulint n_data_pages(const Index *index) noexcept override {
    free_dead_pages(m_sys);

    return m_sys.m_btree->get_size(index, BTR_N_LEAF_PAGES);
  }

  void quiesce() noexcept override { free_dead_pages(m_sys); }

  bool restore_tree_cursor(BtreePcursor &pcur, ulint latch, Mtr *mtr, SourceLocation loc) noexcept override {
    return restore_at_level(pcur, latch, mtr, loc);
  }

  bool validate(Index *index, Trx *) noexcept override { return validate_levels(index); }

  bool uses_index_latch() const noexcept override { return false; }

  ulint page_read_latch() const noexcept override { return RW_B_LATCH; }

  ulint page_held_latch() const noexcept override { return RW_B_HELD; }

  void save_tree_cursor(BtreePcursor &pcur, Mtr *mtr) noexcept override { save_position(m_sys, pcur, mtr); }

  void read_scan_node(
    const Index *index, page_no_t page_no, const DTuple *start, const DTuple *end, ulint stop_level, size_t max, ScanNode &node
  ) noexcept override {
    Mtr mtr;

    mtr.start();

    const auto block = m_sys.m_btree->block_get(index->get_space_id(), page_no, RW_NO_LATCH, &mtr);

    read_stable(m_sys, block, [&]() noexcept { return read_scan_node_raw(index, block->get_frame(), start, end, stop_level, max, node); });

    mtr.commit(m_sys);
  }

  std::unique_ptr<PagePin> pin_pages() noexcept override {
    struct Pin final : PagePin {
      explicit Pin(Drain &drain) noexcept : m_guard(drain) {}

      Drain::Guard m_guard;
    };

    return std::make_unique<Pin>(m_shared.m_drain);
  }

  void estimate_distinct(const Index *index) noexcept override {
    BtreeCursor cur{};

    cur.estimate_n_diff(m_sys, index, [&](Mtr *mtr) {
      Path path;
      BufBlock *block{};
      Drain::Guard guard{m_shared.m_drain};

      while (block == nullptr) {
        const auto page_no = descend(m_sys, index, nullptr, 0, Edge::RANDOM, 0, path);
        const auto savepoint = mtr->set_savepoint();

        block = m_sys.m_btree->block_get(index->get_space_id(), page_no, RW_B_LATCH, mtr);

        if (is_dead(block->get_frame(), index) || m_sys.m_btree->page_get_level(block->get_frame(), mtr) != 0) {
          mtr->release_block_at_savepoint(savepoint, block);
          block = nullptr;
        }
      }

      const auto page_cur = cur.get_page_cur();

      page_cur->open_on_rnd_user_rec(m_sys, block);
      cur.position(index, page_cur->get_rec(), block);
    });
  }

  void print(Index *) noexcept override {}

 private:
  /** Back on the position of a scan that goes along a level of the tree, the leaves or above. */
  [[nodiscard]] bool restore_at_level(BtreePcursor &pcur, ulint latch, Mtr *mtr, SourceLocation loc) noexcept {
    ut_a(pcur.m_old_stored);

    const auto cur = pcur.get_btr_cur();
    const auto index = cur->m_index;
    const auto level = pcur.m_read_level;
    const auto rel_pos = pcur.m_rel_pos;
    const auto at_edge = rel_pos == BtreeCursorPos::AFTER_LAST_IN_TREE || rel_pos == BtreeCursorPos::BEFORE_FIRST_IN_TREE;

    if (!at_edge) {
      bool on_rec;

      if (pcur.restore_optimistic(m_sys, latch, mtr, loc, on_rec, RW_B_LATCH)) {
        return on_rec;
      }
    }

    auto heap = mem_heap_create(256);
    const DTuple *key{};
    ulint mode{};
    auto edge = rel_pos == BtreeCursorPos::AFTER_LAST_IN_TREE ? Edge::LAST : Edge::FIRST;

    if (!at_edge) {
      edge = Edge::KEY;
      key = index->build_data_tuple(pcur.m_old_rec, pcur.m_old_n_fields, heap);
      mode = rel_pos == BtreeCursorPos::ON ? PAGE_CUR_LE : (rel_pos == BtreeCursorPos::AFTER ? PAGE_CUR_G : PAGE_CUR_L);
    }

    const auto space = index->get_space_id();
    BufBlock *block{};
    ulint savepoint{};

    /* The tree went down past this level: there's no more of it to read. */
    bool gone{};

    {
      Drain::Guard guard{m_shared.m_drain};
      Path path;

      while (block == nullptr) {
        const auto page_no = descend(m_sys, index, key, mode, edge, level, path);

        savepoint = mtr->set_savepoint();
        block = m_sys.m_btree->block_get(space, page_no, RW_B_LATCH, mtr);

        const auto dead = is_dead(block->get_frame(), index);

        if (!dead && m_sys.m_btree->page_get_level(block->get_frame(), mtr) < level) {
          gone = true;
        } else if (dead || m_sys.m_btree->page_get_level(block->get_frame(), mtr) != level) {
          mtr->release_block_at_savepoint(savepoint, block);
          block = nullptr;
        }
      }
    }

    /* Left to right coupling. */
    while (!gone && (edge == Edge::KEY ? past_high_key(index, block->get_frame(), key, mode)
                                       : edge == Edge::LAST && page_has_right(block->get_frame()))) {
      const auto right_savepoint = mtr->set_savepoint();
      const auto right = m_sys.m_btree->block_get(space, Btree::page_get_next(block->get_frame(), mtr), RW_B_LATCH, mtr);

      mtr->release_block_at_savepoint(savepoint, block);

      savepoint = right_savepoint;
      block = right;
    }

    const auto page_cur = cur->get_page_cur();

    if (gone) {
      page_cur->set_after_last(block);
    } else if (edge == Edge::KEY) {
      page_cur->search(m_sys, block, index, key, mode);
    } else if (edge == Edge::LAST) {
      page_cur->set_after_last(block);
    } else {
      page_cur->set_before_first(block);
    }

    cur->position(index, page_cur->get_rec(), block);

    pcur.m_latch_mode = latch;
    pcur.m_pos_state = BtrPcurPositioned::IS_POSITIONED;
    pcur.m_block_when_stored = block;

    bool found{};

    if (rel_pos == BtreeCursorPos::ON && page_rec_is_user_rec(page_cur->get_rec())) {
      const auto rec = page_cur->get_rec();
      const auto offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

      found = cmp_dtuple_rec(index->m_cmp_ctx, key, rec, offs) == 0;
    }

    mem_heap_free(heap);

    return found;
  }

  /** The bounds of a tree nothing is changing, level by level: every page is what its parent and
  its neighbours say it is, and its records are within what its high key and its right sibling's
  first record say. The empty leaf is legal. */
  [[nodiscard]] bool validate_levels(Index *index) noexcept {
    const auto space = index->get_space_id();
    auto heap = mem_heap_create(4096);
    std::vector<page_no_t> children{index->get_page_no()};
    bool ok{true};

    const auto check = [&](bool cond, const char *what, page_no_t page_no) {
      if (!cond) {
        log_err(std::format("B-link tree {} page {}: {}", index->m_id, page_no, what));
        ok = false;
      }
    };

    /* The level above: a child has that minus one. */
    auto above = ULINT_UNDEFINED;
    ulint level{};

    while (!children.empty() && ok) {
      std::vector<page_no_t> next_children;
      page_no_t prev_no{FIL_NULL};
      const DTuple *prev_key{};
      auto page_no = children[0];
      size_t at{};

      /* The page on the left says this one has no node pointer. */
      bool unlinked{};

      while (page_no != FIL_NULL && ok) {
        Mtr mtr;

        mtr.start();

        const auto page = m_sys.m_btree->block_get(space, page_no, RW_B_LATCH, &mtr)->get_frame();
        const auto lv = m_sys.m_btree->page_get_level(page, &mtr);

        check(!is_dead(page, index), "not on the tree", page_no);
        check(unlinked || (at < children.size() && children[at] == page_no), "not a child of the level above", page_no);
        check(above == ULINT_UNDEFINED ? page_no == index->get_page_no() : lv + 1 == above, "wrong level", page_no);
        check(Btree::page_get_prev(page, &mtr) == prev_no, "wrong left link", page_no);
        check(page_validate(page, index), "page_validate", page_no);

        const auto next_no = Btree::page_get_next(page, &mtr);
        const auto high_key = page_get_high_key(page);

        check((next_no == FIL_NULL) == high_key.is_null(), "a high key if and only if a right sibling", page_no);

        for (auto rec = page_rec_get_next(page_get_infimum_rec(page)); page_rec_is_user_rec(rec); rec = page_rec_get_next(rec)) {
          std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};
          byte *buf{};
          ulint buf_size{};
          ulint n_fields;

          rec_offs_init(offs_);

          const auto copy = index->copy_rec_order_prefix(rec, &n_fields, buf, buf_size);
          const auto tuple = index->build_data_tuple(copy, n_fields, heap);

          /* The first on the leftmost page of a non-leaf level is lower than anything. */
          const auto first = page_rec_get_prev(rec) == page_get_infimum_rec(page);
          const auto min_rec = first && lv > 0 && prev_no == FIL_NULL;

          if (first && !min_rec && prev_key != nullptr) {
            const auto offs = PhyRec(index, rec).get_all_col_offsets(offs_.data(), &heap, Current_location());

            check(cmp_dtuple_rec(index->m_cmp_ctx, prev_key, rec, offs) <= 0, "first record below the high key on the left", page_no);
          }

          if (!high_key.is_null()) {
            check(cmp_high_key(index, tuple, high_key) < 0, "a record is not below the high key", page_no);
          }

          if (lv > 0) {
            MemHeap *offs_heap{};
            rec_offs_init(offs_);

            const RecOffs offs = PhyRec(index, rec).get_all_col_offsets(offs_.data(), &offs_heap, Current_location());

            next_children.push_back(m_sys.m_btree->node_ptr_get_child_page_no(rec, offs));

            if (offs_heap != nullptr) {
              mem_heap_free(offs_heap);
            }
          }

          mem_free(buf);
        }

        prev_key = nullptr;

        if (!high_key.is_null()) {
          ulint n_fields;
          byte *buf{};
          ulint buf_size{};

          const auto copy = index->copy_rec_order_prefix(high_key, &n_fields, buf, buf_size);
          const auto bytes = static_cast<byte *>(mem_heap_alloc(heap, buf_size));

          memcpy(bytes, buf, buf_size);
          mem_free(buf);

          prev_key = index->build_data_tuple(Rec(bytes + (copy.get() - buf)), n_fields, heap);
        }

        level = lv;
        prev_no = page_no;
        page_no = next_no;

        if (!unlinked) {
          ++at;
        }

        unlinked = page_right_unlinked(page);

        mtr.commit(m_sys);
      }

      check(at == children.size(), "the level has fewer pages than node pointers", prev_no);

      if (level == 0) {
        break;
      }

      above = level;
      children = std::move(next_children);
    }

    mem_heap_free(heap);

    return ok;
  }

  System &m_sys;

 public:
  Shared m_shared{};
};

Shared &shared(System &sys) noexcept { return static_cast<Am &>(sys.am_of_type(DICT_BLINK)).m_shared; }

}  // namespace

#ifdef UNIT_TESTING
void set_test_hook(System &sys, TestHook hook) noexcept { shared(sys).m_hook.store(hook); }

void set_merge(System &sys, bool on) noexcept { shared(sys).m_no_merge.store(!on); }

void hold_frees(System &sys, bool hold) noexcept { shared(sys).m_hold_frees.store(hold); }

void node_retried(System &sys) noexcept {
#ifdef UNIT_TESTING
  shared(sys).m_n_node_retries.fetch_add(1, std::memory_order_relaxed);
#else
  (void)sys;
#endif /* UNIT_TESTING */
}

uint64_t n_node_retries(System &sys) noexcept { return shared(sys).m_n_node_retries.load(); }

uint64_t n_batched(System &sys) noexcept { return shared(sys).m_n_batched.load(); }

page_no_t alloc_unlinked(System &sys, const Index *index) noexcept {
  /* It's not freed: the caller crashes with it open. */
  auto trx = new SysTrx(sys);
  Mtr mtr;

  mtr.start();

  sys.m_fsp->latch(index->get_space_id(), &mtr);

  const auto block = sys.m_btree->page_alloc(index, 0, FSP_NO_DIR, 0, &mtr);

  ut_a(block != nullptr);

  const auto page_no = block->get_page_no();

  byte args[4 + 4 + 2 + 4];

  mach_write_to_4(args, index->get_space_id());
  mach_write_to_4(args + 4, index->get_page_no());
  mach_write_to_2(args + 8, PAGE_HEADER + PAGE_BTR_SEG_LEAF);
  mach_write_to_4(args + 10, page_no);

  trx->step(&mtr, SysTrxKind::FREE_PAGE, args, sizeof(args));
  trx->commit_step(&mtr);

  return page_no;
}
#endif /* UNIT_TESTING */

}  // namespace blink

std::unique_ptr<IndexAm> index_am_blink(System &sys) { return std::make_unique<blink::Am>(sys); }
