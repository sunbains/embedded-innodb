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

/** @file idx/idx0am.cc
The index access method, and the B-tree behind it. */

#include "idx0am.h"

#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "page0page.h"
#include "srv0sys.h"

namespace {

[[nodiscard]] ulint latch_mode(Intent intent) noexcept {
  switch (intent) {
    case Intent::Read:
    case Intent::Snapshot:
      return BTR_SEARCH_LEAF;
    case Intent::Mark:
    case Intent::Write:
      return BTR_MODIFY_LEAF;
    case Intent::Restructure:
      return BTR_MODIFY_TREE;
  }

  ut_error;
}

struct BtreeCursorAm final : IndexAm::Cursor {
  explicit BtreeCursorAm(System &sys) noexcept : m_sys(sys) {}

  void seek(Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    m_pcur.open_on_user_rec(m_sys, index, key, mode, latch_mode(intent), mtr, loc);
  }

  void position(const Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    /* No init: a cursor that is opened again keeps the buffer of its saved position. */
    m_pcur.open_with_no_init(m_sys, index, key, mode, latch_mode(intent), 0, mtr, loc);
  }

  void seek_insert(const Index *index, const DTuple *entry, Intent intent, QueThr *thr, Mtr *mtr, SourceLocation loc) noexcept override {
    ut_ad(intent != Intent::Read);

    const auto mode = latch_mode(intent);

    m_pcur.get_btr_cur()->m_thr = thr;

    /* BTR_INSERT only tells the search what's planned, it isn't a state of the cursor. */
    m_pcur.open_with_no_init(m_sys, index, entry, PAGE_CUR_LE, mode | BTR_INSERT, 0, mtr, loc);
    m_pcur.m_latch_mode = mode;
  }

  bool find(const Index *index, const DTuple *key, Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    position(index, key, PAGE_CUR_LE, intent, mtr, loc);

    return !page_rec_is_infimum(m_pcur.get_rec()) && m_pcur.get_low_match() == dtuple_get_n_fields_cmp(key);
  }

  void seek_edge(const Index *index, bool first, Intent intent, Mtr *mtr, SourceLocation) noexcept override {
    /* No init: a cursor that is opened again keeps the buffer of its saved position. */
    m_pcur.open_at_index_side(m_sys, first, index, latch_mode(intent), false, 0, mtr);
  }

  bool next(Mtr *mtr) noexcept override { return m_pcur.move_to_next_user_rec(m_sys, mtr); }

  bool prev(Mtr *mtr) noexcept override { return m_pcur.move_to_prev_user_rec(m_sys, mtr); }

  bool next_pos(Mtr *mtr) noexcept override { return m_pcur.move_to_next(m_sys, mtr); }

  bool prev_pos(Mtr *mtr) noexcept override { return m_pcur.move_to_prev(m_sys, mtr); }

  void skip_rest(Mtr *mtr) noexcept override { m_pcur.move_to_last_on_page(mtr); }

  bool on_rec() const noexcept override { return m_pcur.is_on_user_rec(); }

  Rec rec() noexcept override { return m_pcur.get_rec(); }

  const Index *index() const noexcept override { return m_pcur.get_btr_cur()->m_index; }

  ulint low_match() noexcept override { return m_pcur.get_low_match(); }

  ulint up_match() noexcept override { return m_pcur.get_up_match(); }

  SearchMode search_mode() const noexcept override { return m_pcur.m_search_mode; }

  BufBlock *block() noexcept override { return m_pcur.get_block(); }

  void save(Mtr *mtr) noexcept override { m_pcur.store_position(m_sys, mtr); }

  bool restore(Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    return m_pcur.restore_position(m_sys, latch_mode(intent), mtr, loc);
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

  void copy_saved(IndexAm::Cursor &src) noexcept override { m_pcur.copy_stored_position(&static_cast<BtreeCursorAm &>(src).m_pcur); }

  void commit(Mtr *mtr) noexcept override { m_pcur.commit_specify_mtr(m_sys, mtr); }

  DbErr remove(Intent intent, TrxRbCtx rb_ctx, Mtr *mtr) noexcept override {
    const auto cur = m_pcur.get_btr_cur();

    if (intent == Intent::Restructure) {
      return cur->pessimistic_delete(m_sys, false, rb_ctx, mtr);
    }

    ut_a(intent == Intent::Write);

    return cur->optimistic_delete(m_sys, mtr) ? DB_SUCCESS : DB_FAIL;
  }

  DbErr update(
    Intent intent, ulint flags, MemHeap **heap, BigRec *big_rec, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr
  ) noexcept override {
    const auto cur = m_pcur.get_btr_cur();

    if (intent == Intent::Restructure) {
      return cur->pessimistic_update(m_sys, flags, heap, big_rec, update, cmpl_info, thr, mtr);
    }

    ut_a(intent == Intent::Write);

    const auto err = cur->optimistic_update(m_sys, flags, update, cmpl_info, thr, mtr);

    return err == DB_OVERFLOW || err == DB_UNDERFLOW ? DB_FAIL : err;
  }

  /* A caller may have this choice to make and it can't be update()'s: rollback replays the
  cmpl_info of the undo record, with UPD_NODE_NO_SIZE_CHANGE from a parsed update of a fixed
  size column, and the sizes checked in optimistic_update() have to decide for it. */
  DbErr update_in_place(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr) noexcept override {
    return m_pcur.get_btr_cur()->update_in_place(m_sys, flags, update, cmpl_info, thr, mtr);
  }

  DbErr insert(Intent intent, ulint flags, DTuple *entry, BigRec *big_rec, ulint n_ext, QueThr *thr, Mtr *mtr) noexcept override {
    const auto cur = m_pcur.get_btr_cur();
    Rec rec;

    if (intent == Intent::Restructure) {
      return cur->pessimistic_insert(m_sys, flags, entry, &rec, big_rec, n_ext, thr, mtr);
    }

    ut_a(intent == Intent::Write);

    const auto err = cur->optimistic_insert(m_sys, flags, entry, &rec, big_rec, n_ext, thr, mtr);

    return err == DB_OVERFLOW || err == DB_UNDERFLOW ? DB_FAIL : err;
  }

  DbErr store_big_rec(const RecOffs &offsets, const BigRec &big_rec, Mtr *mtr) noexcept override {
    Blob blob{};

    return blob.store_big_rec_extern_fields(m_sys, m_pcur.get_btr_cur()->m_index, m_pcur.get_block(), m_pcur.get_rec(), offsets, big_rec, mtr);
  }

  DbErr set_delete_mark(ulint flags, bool val, QueThr *thr, Mtr *mtr) noexcept override {
    const auto cur = m_pcur.get_btr_cur();

    if (cur->get_index()->is_clustered()) {
      return cur->del_mark_set_clust_rec(m_sys, flags, val, thr, mtr);
    } else {
      return cur->del_mark_set_sec_rec(m_sys, flags, val, thr, mtr);
    }
  }

  void close() noexcept override { m_pcur.close(); }

  System &m_sys;
  BtreePcursor m_pcur{};
};

struct BtreeAm final : IndexAm {
  explicit BtreeAm(System &sys) noexcept : m_sys(sys) {}

  std::unique_ptr<Cursor> cursor() override { return std::make_unique<BtreeCursorAm>(m_sys); }

  Root create(ulint type, space_id_t space, uint64_t index_id, Index *index, Mtr *mtr) noexcept override {
    return m_sys.m_btree->create(type, space, index_id, index, mtr);
  }

  void drop_data(space_id_t space, Root root) noexcept override { m_sys.m_btree->free_but_not_root(space, root); }

  void drop_root(space_id_t space, Root root, Mtr *mtr) noexcept override { m_sys.m_btree->free_root(space, root, mtr); }

  ulint n_pages(const Index *index) noexcept override { return m_sys.m_btree->get_size(index, BTR_TOTAL_SIZE); }

  ulint n_data_pages(const Index *index) noexcept override { return m_sys.m_btree->get_size(index, BTR_N_LEAF_PAGES); }

  void estimate_distinct(const Index *index) noexcept override {
    BtreeCursor cur{};

    cur.estimate_number_of_different_key_vals(m_sys, index);
  }

  bool validate(Index *index, Trx *trx) noexcept override { return m_sys.m_btree->validate_index(index, trx); }

  bool restore_tree_cursor(BtreePcursor &pcur, ulint latch, Mtr *mtr, SourceLocation loc) noexcept override {
    return pcur.restore_position(m_sys, latch, mtr, loc);
  }

  void save_tree_cursor(BtreePcursor &pcur, Mtr *mtr) noexcept override { pcur.store_position(m_sys, mtr); }

  void read_scan_node(
    const Index *index, page_no_t page_no, const DTuple *start, const DTuple *end, ulint stop_level, size_t max, ScanNode &node
  ) noexcept override {
    Mtr mtr;

    mtr.start();

    const auto block = m_sys.m_btree->block_get(index->get_space_id(), page_no, RW_S_LATCH, &mtr);
    const auto page = block->get_frame();

    node.m_children.clear();
    node.m_dead = m_sys.m_btree->page_get_index_id(page) != index->m_id;
    node.m_level = m_sys.m_btree->page_get_level(page, &mtr);

    if (!node.m_dead && node.m_level > stop_level) {
      PageCursor cur;

      cur.m_index = index;

      if (start != nullptr) {
        cur.search(m_sys, block, index, start, PAGE_CUR_L);
      } else {
        cur.set_before_first(block);
      }

      if (cur.is_before_first()) {
        cur.move_to_next();
      }

      MemHeap *heap{};

      while (!cur.is_after_last() && node.m_children.size() < max) {
        const auto rec = cur.get_rec();
        const RecOffs offsets = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

        if (end != nullptr && end->compare(rec, index, offsets) <= 0) {
          break;
        }

        node.m_children.push_back(m_sys.m_btree->node_ptr_get_child_page_no(rec, offsets));

        cur.move_to_next();
      }

      if (heap != nullptr) {
        mem_heap_free(heap);
      }
    }

    mtr.commit(m_sys);
  }

  void print(Index *index) noexcept override {
#ifdef UNIV_BTR_PRINT
    m_sys.m_btree->print_size(index);
    m_sys.m_btree->print_index(index, 7);
#else
    (void)index;
#endif /* UNIV_BTR_PRINT */
  }

  System &m_sys;
};

}  // namespace

std::unique_ptr<IndexAm> index_am_btree(System &sys) { return std::make_unique<BtreeAm>(sys); }

void IndexCursor::bind(const Index *index) {
  /* The indexes of a table can be of different kinds, one cursor goes through them. */
  if (auto &am = m_sys->am(index); m_cur == nullptr || m_am != &am) {
    m_cur = am.cursor();
    m_am = &am;
  }
}

bool IndexCursor::find(const Index *index, const DTuple *key, Intent intent, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);

  return m_cur->find(index, key, intent, mtr, loc);
}

void IndexCursor::seek(Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);
  m_cur->seek(index, key, mode, intent, mtr, loc);
}

void IndexCursor::position(const Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);
  m_cur->position(index, key, mode, intent, mtr, loc);
}

void IndexCursor::seek_insert(const Index *index, const DTuple *entry, Intent intent, QueThr *thr, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);
  m_cur->seek_insert(index, entry, intent, thr, mtr, loc);
}

DbErr IndexCursor::insert_batched(const Index *index, DTuple *entry, ulint flags, QueThr *thr) noexcept {
  bind(index);

  return m_cur->insert_batched(index, entry, flags, thr);
}

void IndexCursor::copy_saved(IndexCursor &src) noexcept {
  bind(src.index());
  m_cur->copy_saved(*src.m_cur);
}

void IndexCursor::seek_first(const Index *index, Intent intent, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);
  m_cur->seek_edge(index, true, intent, mtr, loc);
}

void IndexCursor::seek_last(const Index *index, Intent intent, Mtr *mtr, SourceLocation loc) noexcept {
  bind(index);
  m_cur->seek_edge(index, false, intent, mtr, loc);
}
