/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file btr/btr0pcur.c
The index tree persistent cursor

Created 2/23/1996 Heikki Tuuri
*******************************************************/

#include "btr0pcur.h"
#include "btr0types.h"
#include "trx0trx.h"

Btree_pcursor::Btree_pcursor(FSP *fsp, Btree *btree) noexcept : m_btr_cur(fsp, btree) {
  m_btr_cur.m_index = nullptr;
  init(0);
}

Btree_pcursor::~Btree_pcursor() noexcept {
  if (m_old_rec_buf != nullptr) {
    mem_free(m_old_rec_buf);

    m_old_rec_buf = nullptr;
  }


  m_old_rec = nullptr;
  m_old_n_fields = 0;
  m_latch_mode = BTR_NO_LATCHES;
  m_btr_cur.m_page_cur.m_rec = nullptr;
  m_old_stored = false;
  m_pos_state = Btr_pcur_positioned::UNSET;
}

void Btree_pcursor::store_position(mtr_t *mtr) noexcept {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  auto block = get_block();
  auto index = m_btr_cur.m_index;
  auto page_cursor = get_page_cur();

  auto rec = page_cur_get_rec(page_cursor);
  auto page = page_align(rec);
  auto offs = page_offset(rec);

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_S_FIX) || mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  ut_a(m_latch_mode != BTR_NO_LATCHES);

  if (unlikely(page_get_n_recs(page) == 0)) {
    /* It must be an empty index tree; NOTE that in this case
    we do not store the modify_clock, but always do a search
    if we restore the cursor position */

    ut_a(m_btr_cur.m_btree->page_get_next(page, mtr) == FIL_NULL);
    ut_a(m_btr_cur.m_btree->page_get_prev(page, mtr) == FIL_NULL);

    m_old_stored = true;

    if (page_rec_is_supremum_low(offs)) {

      m_rel_pos = Btree_cursor_pos::AFTER_LAST_IN_TREE;
    } else {

      m_rel_pos = Btree_cursor_pos::BEFORE_FIRST_IN_TREE;
    }

    return;
  }

  if (page_rec_is_supremum_low(offs)) {

    rec = page_rec_get_prev(rec);

    m_rel_pos = Btree_cursor_pos::AFTER;

  } else if (page_rec_is_infimum_low(offs)) {

    rec = page_rec_get_next(rec);

    m_rel_pos = Btree_cursor_pos::BEFORE;
  } else {

    m_rel_pos = Btree_cursor_pos::ON;
  }

  m_old_stored = true;
  m_old_rec = index->copy_rec_order_prefix(rec, &m_old_n_fields, m_old_rec_buf, m_buf_size);

  m_block_when_stored = block;
  m_modify_clock = buf_block_get_modify_clock(block);
}

void Btree_pcursor::copy_stored_position(Btree_pcursor *src) noexcept {
  if (m_old_rec_buf != nullptr) {
    mem_free(m_old_rec_buf);
  }

  *this = *src;

  if (src->m_old_rec_buf != nullptr) {

    m_old_rec_buf = static_cast<byte*>(mem_alloc(src->m_buf_size));

    memcpy(m_old_rec_buf, src->m_old_rec_buf, src->m_buf_size);

    m_old_rec = m_old_rec_buf + (src->m_old_rec - src->m_old_rec_buf);
  }

  m_old_n_fields = src->m_old_n_fields;
}

bool Btree_pcursor::restore_position(ulint latch_mode, mtr_t *mtr, Source_location loc) noexcept {
  ut_ad(mtr->m_state == MTR_ACTIVE);

  auto index = m_btr_cur.m_index;

  if (unlikely(!m_old_stored) ||
      unlikely(m_pos_state != Btr_pcur_positioned::WAS_POSITIONED &&
               m_pos_state != Btr_pcur_positioned::IS_POSITIONED)) {

    log_warn_buf(this, sizeof(Btree_pcursor));

    if (m_trx_if_known != nullptr) {
      log_err(trx_to_string(m_trx_if_known, 0));
    }

    ut_error;
  }

  if (unlikely(m_rel_pos == Btree_cursor_pos::AFTER_LAST_IN_TREE ||
      m_rel_pos == Btree_cursor_pos::BEFORE_FIRST_IN_TREE)) {

    /* In these cases we do not try an optimistic restoration, but always do a search */

    m_btr_cur.open_at_index_side(nullptr, m_rel_pos == Btree_cursor_pos::BEFORE_FIRST_IN_TREE, index, latch_mode, m_read_level, mtr, loc);

    m_block_when_stored = get_block();

    return false;
  }

  ut_a(m_old_rec != nullptr);
  ut_a(m_old_n_fields > 0);

  if (likely(latch_mode == BTR_SEARCH_LEAF) || likely(latch_mode == BTR_MODIFY_LEAF)) {

    /* Try optimistic restoration */

    Buf_pool::Request req {
      .m_rw_latch = latch_mode,
      .m_guess = m_block_when_stored,
      .m_modify_clock = m_modify_clock,
      .m_file = loc.m_from.file_name(),
      .m_line = loc.m_from.line(),
      .m_mtr = mtr
    };

    if (likely(srv_buf_pool->try_get(req))) {
      m_pos_state = Btr_pcur_positioned::IS_POSITIONED;

      buf_block_dbg_add_level(IF_SYNC_DEBUG(get_block(), SYNC_TREE_NODE));

      if (m_rel_pos == Btree_cursor_pos::ON) {
        m_latch_mode = latch_mode;

#ifdef UNIV_DEBUG
        auto rec = get_rec();
        Phy_rec record(index, rec);
        Phy_rec old_record(index, m_old_rec);
        auto heap = mem_heap_create(256);
        auto offsets2 = record.get_col_offsets(nullptr, m_old_n_fields, &heap, Current_location());
        auto offsets1 = old_record.get_col_offsets(nullptr, m_old_n_fields, &heap, Current_location());
        ut_ad(!cmp_rec_rec(m_old_rec, rec, offsets1, offsets2, index));
        mem_heap_free(heap);
#endif /* UNIV_DEBUG */

        return true;

      } else {

        /* This is the same record as stored, may need to be adjusted
        for BTR_PCUR_BEFORE/AFTER, depending on search mode and direction. */
        if (is_on_user_rec()) {
          m_pos_state = Btr_pcur_positioned::IS_POSITIONED_OPTIMISTIC;
        }

        return false;
      }
    }
  }

  /* If optimistic restoration did not succeed, open the cursor anew */

  auto heap = mem_heap_create(256);
  auto tuple = index->build_data_tuple(m_old_rec, m_old_n_fields, heap);

  /* Save the old search mode of the cursor */
  ib_srch_mode_t search_mode;
  auto old_search_mode = m_search_mode;

  if (likely(m_rel_pos == Btree_cursor_pos::ON)) {
    search_mode = PAGE_CUR_LE;
  } else if (m_rel_pos == Btree_cursor_pos::AFTER) {
    search_mode = PAGE_CUR_G;
  } else {
    ut_ad(m_rel_pos == Btree_cursor_pos::BEFORE);
    search_mode = PAGE_CUR_L;
  }

  open_with_no_init(index, tuple, search_mode, latch_mode, 0, mtr, loc);

  /* Restore the old search mode */
  m_search_mode = old_search_mode;

  bool ret;
  ulint *offsets{};
  auto rec = get_rec();

  {
    Phy_rec record(index, rec);

    offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (m_rel_pos == Btree_cursor_pos::ON && is_on_user_rec() && cmp_dtuple_rec(index->m_cmp_ctx, tuple, rec, offsets) == 0) {

    /* We have to store the NEW value for the modify clock, since
    the cursor can now be on a different page! But we can retain
    the value of old_rec */

    m_block_when_stored = get_block();
    m_modify_clock = buf_block_get_modify_clock(m_block_when_stored);
    m_old_stored = true;

    ret = true;

  } else {
    /* We have to store new position information, modify_clock etc.,
    to the cursor because it can now be on a different page, the record
     under it may have been removed, etc. */

     store_position(mtr);

     ret = false;
  }

  mem_heap_free(heap);

  return ret;
}

void Btree_pcursor::release_leaf(mtr_t *mtr) noexcept {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  auto block = get_block();

  m_btr_cur.m_btree->leaf_page_release(block, m_latch_mode, mtr);

  m_latch_mode = BTR_NO_LATCHES;

  m_pos_state = Btr_pcur_positioned::WAS_POSITIONED;
}

void Btree_pcursor::move_to_next_page(mtr_t *mtr) noexcept {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);
  ut_ad(is_after_last_on_page());

  m_old_stored = false;

  auto page = get_page_no();
  const auto space_id = get_block()->get_space();
  const auto next_page_no = m_btr_cur.m_btree->page_get_next(page, mtr);

  ut_ad(next_page_no != FIL_NULL);

  auto next_block = m_btr_cur.m_btree->block_get(space_id, next_page_no, m_latch_mode, mtr);
  auto next_page = next_block->get_frame();

#ifdef UNIV_BTR_DEBUG
  ut_a(m_btr_cur.m_btree->page_get_prev(next_page, mtr) == get_block()->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  next_block->m_check_index_page_at_flush = true;

  m_btr_cur.m_btree->leaf_page_release(get_block(), m_latch_mode, mtr);

  page_cur_set_before_first(next_block, get_page_cur());

  page_check_dir(next_page);
}

void Btree_pcursor::move_backward_from_page(mtr_t *mtr) noexcept {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);
  ut_ad(is_before_first_on_page());
  ut_ad(!is_before_first_in_tree(mtr));

  ulint latch_mode2;
  auto latch_mode = m_latch_mode;

  if (latch_mode == BTR_SEARCH_LEAF) {

    latch_mode2 = BTR_SEARCH_PREV;

  } else if (latch_mode == BTR_MODIFY_LEAF) {

    latch_mode2 = BTR_MODIFY_PREV;

  } else {

    latch_mode2 = 0; /* To eliminate compiler warning */
    ut_error;

  }

  store_position(mtr);

  mtr->commit();

  mtr->start();

  (void) restore_position(latch_mode2, mtr, Current_location());

  auto page = get_page_no();
  auto prev_page_no = m_btr_cur.m_btree->page_get_prev(page, mtr);

  if (prev_page_no == FIL_NULL) {
     ;
  } else if (is_before_first_on_page()) {

    auto prev_block = m_btr_cur.m_left_block;

    m_btr_cur.m_btree->leaf_page_release(get_block(), latch_mode, mtr);

    page_cur_set_after_last(prev_block, get_page_cur());
  } else {

    /* The repositioned cursor did not end on an infimum record on
    a page. Cursor repositioning acquired a latch also on the
    previous page, but we do not need the latch: release it. */

    auto prev_block = m_btr_cur.m_left_block;

    m_btr_cur.m_btree->leaf_page_release(prev_block, latch_mode, mtr);
  }

  m_latch_mode = latch_mode;

  m_old_stored = false;
}

void Btree_pcursor::open_on_user_rec(
  Index *index,
  const DTuple *tuple,
  ib_srch_mode_t search_mode,
  ulint latch_mode,
  mtr_t *mtr,
  Source_location loc
) noexcept {
  open(index, tuple, search_mode, latch_mode, mtr, loc);

  if (search_mode == PAGE_CUR_GE || search_mode == PAGE_CUR_G) {

    if (is_after_last_on_page()) {

      (void) move_to_next_user_rec(mtr);
    }
  } else {
    ut_ad(search_mode == PAGE_CUR_LE || search_mode == PAGE_CUR_L);

    /* Not implemented yet */

    ut_error;
  }
}

void Btree_pcursor::open_on_user_rec(const page_cur_t &page_cursor, ib_srch_mode_t mode, ulint latch_mode) noexcept {
  m_btr_cur.m_index = const_cast<Index *>(page_cursor.m_index);

  auto page_cur = get_page_cur();

  memcpy(page_cur, &page_cursor, sizeof(*page_cur));

  m_search_mode = mode;
  
  m_pos_state = Btr_pcur_positioned::IS_POSITIONED;
  
  m_latch_mode = BTR_LATCH_MODE_WITHOUT_FLAGS(latch_mode);

  m_trx_if_known = nullptr;
}
