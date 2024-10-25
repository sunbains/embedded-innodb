/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** @file btr/btr0cur.c
The index tree cursor

All changes that row operations make to a B-tree or the records
there must go through this module! Undo log records are written here
of every modify or insert of a clustered index record.

                        NOTE!!!
To make sure we do not run out of disk space during a pessimistic
insert or update, we have to reserve 2 x the height of the index tree
many pages in the tablespace before we start the operation, because
if leaf splitting has been started, it is difficult to undo, except
by crashing the database and doing a roll-forward.

Created 10/16/1994 Heikki Tuuri
*******************************************************/

#include "innodb0types.h"

#include "btr0cur.h"

#include "btr0btr.h"
#include "btr0blob.h"
#include "buf0lru.h"
#include "dict0types.h"
#include "lock0lock.h"
#include "mtr0log.h"
#include "page0page.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0row.h"
#include "row0upd.h"
#include "srv0srv.h"
#include "trx0rec.h"
#include "trx0roll.h"


/** In the optimistic insert, if the insert does not fit, but this much space
can be released by page reorganize, then it is reorganized */
constexpr ulint BTR_CUR_PAGE_REORGANIZE_LIMIT = UNIV_PAGE_SIZE / 32;


/**
 * The following function is used to set the deleted bit of a record.
 * 
 * @param[in,out] rec The physical record.
 * @param[in] flag true  to delete mark the record
 */
inline void set_deleted_flag(rec_t *rec, bool set_flag) noexcept {
  rec_set_deleted_flag(rec, set_flag);
}

void Btree_cursor::latch_leaves(page_t *page, space_id_t space, page_no_t page_no, ulint latch_mode, mtr_t *mtr) noexcept {
  ulint mode;
  Buf_block *block;
  page_no_t left_page_no;
  page_no_t right_page_no;

  switch (latch_mode) {
    case BTR_SEARCH_LEAF:
    case BTR_MODIFY_LEAF:
      mode = latch_mode == BTR_SEARCH_LEAF ? RW_S_LATCH : RW_X_LATCH;
      block = m_btree->block_get(space, page_no, mode, mtr);
      block->m_check_index_page_at_flush = true;
      return;

    case BTR_MODIFY_TREE:
      /* x-latch also brothers from left to right */
      left_page_no = m_btree->page_get_prev(page, mtr);

      if (left_page_no != FIL_NULL) {
        block = m_btree->block_get(space, left_page_no, RW_X_LATCH, mtr);
#ifdef UNIV_BTR_DEBUG
        ut_a(m_btree->page_get_next(block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        block->m_check_index_page_at_flush = true;
      }

      block = m_btree->block_get(space, page_no, RW_X_LATCH, mtr);
      block->m_check_index_page_at_flush = true;

      right_page_no = m_btree->page_get_next(page, mtr);

      if (right_page_no != FIL_NULL) {
        block = m_btree->block_get(space, right_page_no, RW_X_LATCH, mtr);
#ifdef UNIV_BTR_DEBUG
        ut_a(m_btree->page_get_prev(block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        block->m_check_index_page_at_flush = true;
      }

      return;

    case BTR_SEARCH_PREV:
    case BTR_MODIFY_PREV:
      mode = latch_mode == BTR_SEARCH_PREV ? RW_S_LATCH : RW_X_LATCH;
      /* latch also left brother */
      left_page_no = m_btree->page_get_prev(page, mtr);

      if (left_page_no != FIL_NULL) {
        block = m_btree->block_get(space, left_page_no, mode, mtr);
        m_left_block = block;
#ifdef UNIV_BTR_DEBUG
        ut_a(m_btree->page_get_next(m_left_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        m_left_block->m_check_index_page_at_flush = true;
      }

      block = m_btree->block_get(space, page_no, mode, mtr);
      block->m_check_index_page_at_flush = true;
      return;
  }

  ut_error;
}

void Btree_cursor::search_to_nth_level(Paths *paths, const Index *index, ulint level, const DTuple *tuple, ulint mode, ulint latch_mode, mtr_t *mtr, Source_location loc) noexcept{
  page_t *page;
  rec_t *node_ptr;
  page_no_t page_no;
  space_id_t space;
  ulint up_match;
  ulint up_bytes;
  ulint low_match;
  ulint low_bytes;
  ulint height;
  ulint savepoint;
  ulint page_mode;
  page_cur_t *page_cursor;
  IF_DEBUG(ulint insert_planned;)
  ulint estimate;
  ulint root_height{};
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  /* Currently, PAGE_CUR_LE is the only search mode used for searches
  ending to upper levels */
  ut_ad(level == 0 || mode == PAGE_CUR_LE);
  ut_ad(index->check_search_tuple(tuple));
  ut_ad(dtuple_check_typed(tuple));

  IF_DEBUG(
    m_up_match = ULINT_UNDEFINED;
    m_low_match = ULINT_UNDEFINED;
    insert_planned = latch_mode & BTR_INSERT;
  )

  estimate = latch_mode & BTR_ESTIMATE;
  latch_mode = latch_mode & ~(BTR_INSERT | BTR_ESTIMATE);

  ut_ad(!insert_planned || (mode == PAGE_CUR_LE));

  m_flag = BTR_CUR_BINARY;
  m_index = index;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched leaf node(s) */

  savepoint = mtr->set_savepoint();

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(index->get_lock(), mtr);

  } else if (latch_mode == BTR_CONT_MODIFY_TREE) {
    /* Do nothing */
    ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  } else {
    mtr_s_lock(index->get_lock(), mtr);
  }

  page_cursor = get_page_cur();

  space = index->get_space_id();
  page_no = index->get_page_no();

  up_match = 0;
  up_bytes = 0;
  low_match = 0;
  low_bytes = 0;

  height = ULINT_UNDEFINED;

  /* We use these modified search modes on non-leaf levels of the
  B-tree. These let us end up in the right B-tree leaf. In that leaf
  we use the original search mode. */

  switch (mode) {
    case PAGE_CUR_GE:
      page_mode = PAGE_CUR_L;
      break;
    case PAGE_CUR_G:
      page_mode = PAGE_CUR_LE;
      break;
    default:
#ifdef PAGE_CUR_LE_OR_EXTENDS
      ut_ad(mode == PAGE_CUR_L || mode == PAGE_CUR_LE || mode == PAGE_CUR_LE_OR_EXTENDS);
#else  /* PAGE_CUR_LE_OR_EXTENDS */
      ut_ad(mode == PAGE_CUR_L || mode == PAGE_CUR_LE);
#endif /* PAGE_CUR_LE_OR_EXTENDS */
      page_mode = mode;
      break;
  }

  /* Loop and search until we arrive at the desired level */

  for (;;) {
    ulint rw_latch;
    ulint buf_mode;
    Buf_block *block;

    rw_latch = RW_NO_LATCH;
    buf_mode = BUF_GET;

    if (height == 0 && latch_mode <= BTR_MODIFY_LEAF) {

      rw_latch = latch_mode;
    }

  retry_page_get:

    Buf_pool::Request req {
      .m_rw_latch = rw_latch,
      .m_page_id = { space, page_no },
      .m_mode = buf_mode,
      .m_file = loc.m_from.file_name(),
      .m_line = loc.m_from.line(),
      .m_mtr = mtr
    };

    block = get_buf_pool()->get(req, nullptr);

    if (block == nullptr) {

      ut_ad(m_thr != nullptr);
      ut_ad(insert_planned);

      /* Insert to the insert buffer did not succeed: retry page get */

      buf_mode = BUF_GET;

      goto retry_page_get;
    }

    page = block->get_frame();

    block->m_check_index_page_at_flush = true;

    if (rw_latch != RW_NO_LATCH) {

      buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE));
    }

    ut_ad(index->m_id == m_btree->page_get_index_id(page));

    if (unlikely(height == ULINT_UNDEFINED)) {
      /* We are in the root node */

      height = m_btree->page_get_level(page, mtr);
      root_height = height;
      m_tree_height = root_height + 1;
    }

    if (height == 0) {
      if (rw_latch == RW_NO_LATCH) {

        latch_leaves(page, space, page_no, latch_mode, mtr);
      }

      if ((latch_mode != BTR_MODIFY_TREE) && (latch_mode != BTR_CONT_MODIFY_TREE)) {

        /* Release the tree s-latch */

        mtr->release_s_latch_at_savepoint(savepoint, index->get_lock());
      }

      page_mode = mode;
    }

    page_cur_search_with_match(block, index, tuple, page_mode, &up_match, &up_bytes, &low_match, &low_bytes, page_cursor);

    if (estimate) {
      add_path_info(paths, height, root_height);
    }

    /* If this is the desired level, leave the loop */

    ut_ad(height == m_btree->page_get_level(page_cur_get_page(page_cursor), mtr));

    if (level == height) {

      if (level > 0) {
        /* x-latch the page */
        page = m_btree->page_get(space, page_no, RW_X_LATCH, mtr);
      }

      break;
    }

    ut_ad(height > 0);

    height--;

    node_ptr = page_cur_get_rec(page_cursor);

    {
      Phy_rec record{m_index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    /* Go to the child node */
    page_no = m_btree->node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (level == 0) {
    m_low_match = low_match;
    m_low_bytes = low_bytes;
    m_up_match = up_match;
    m_up_bytes = up_bytes;

    ut_ad(m_up_match != ULINT_UNDEFINED || mode != PAGE_CUR_GE);
    ut_ad(m_up_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
    ut_ad(m_low_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
  }
}

void Btree_cursor::open_at_index_side(
  Paths *paths,
  bool from_left,
  const Index *index,
  ulint latch_mode,
  ulint level,
  mtr_t *mtr,
  Source_location loc
) noexcept {
  ulint root_height{};
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_a(level != ULINT_UNDEFINED);

  auto estimate = latch_mode & BTR_ESTIMATE;

  latch_mode = latch_mode & ~BTR_ESTIMATE;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched the leaf node */

  auto savepoint = mtr->set_savepoint();

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(index->get_lock(), mtr);
  } else {
    mtr_s_lock(index->get_lock(), mtr);
  }

  auto page_cursor = get_page_cur();
  m_index = index;

  auto space = index->get_space_id();
  auto page_no = index->get_page_no();

  auto height = ULINT_UNDEFINED;

  for (;;) {
  
    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { space, page_no },
      .m_mode = BUF_GET,
      .m_file = loc.m_from.file_name(),
      .m_line = loc.m_from.line(),
      .m_mtr = mtr
    };

    auto block = get_buf_pool()->get(req, nullptr);
    auto page = block->get_frame();

    ut_ad(index->m_id == get_btree()->page_get_index_id(page));

    block->m_check_index_page_at_flush = true;

    if (height == ULINT_UNDEFINED) {
      /* We are in the root node */

      height = get_btree()->page_get_level(page, mtr);
      root_height = height;
    }

    if (height == 0) {
      latch_leaves(page, space, page_no, latch_mode, mtr);

      if (latch_mode != BTR_MODIFY_TREE && latch_mode != BTR_CONT_MODIFY_TREE) {

        /* Release the tree s-latch */

        mtr->release_s_latch_at_savepoint(savepoint, index->get_lock());
      }
    }

    if (from_left) {
      page_cur_set_before_first(block, page_cursor);
    } else {
      page_cur_set_after_last(block, page_cursor);
    }

    if (height == 0 || height == level) {
      if (estimate) {
        add_path_info(paths, height, root_height);
      }

      break;
    }

    ut_ad(height > 0);

    if (from_left) {
      page_cur_move_to_next(page_cursor);
    } else {
      page_cur_move_to_prev(page_cursor);
    }

    if (estimate) {
      add_path_info(paths, height, root_height);
    }

    --height;

    auto node_ptr = page_cur_get_rec(page_cursor);

    {
      Phy_rec record{m_index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    /* Go to the child node */
    page_no = get_btree()->node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

void Btree_cursor::open_at_rnd_pos(const Index *index, ulint latch_mode, mtr_t *mtr, Source_location loc) noexcept {
  rec_t *node_ptr;
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(index->get_lock(), mtr);
  } else {
    mtr_s_lock(index->get_lock(), mtr);
  }

  auto page_cursor = get_page_cur();

  m_index = index;

  auto space = index->get_space_id();
  auto page_no = index->get_page_no();

  auto height = ULINT_UNDEFINED;

  for (;;) {

    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { space, page_no },
      .m_mode = BUF_GET,
      .m_file = loc.m_from.file_name(),
      .m_line = loc.m_from.line(),
      .m_mtr = mtr
    };

    auto block = get_buf_pool()->get(req, nullptr);
    auto page = block->get_frame();

    ut_ad(index->m_id == m_btree->page_get_index_id(page));

    if (height == ULINT_UNDEFINED) {
      /* We are in the root node */

      height = m_btree->page_get_level(page, mtr);
    }

    if (height == 0) {
      latch_leaves(page, space, page_no, latch_mode, mtr);
    }

    page_cur_open_on_rnd_user_rec(block, page_cursor);

    if (height == 0) {

      break;
    }

    ut_ad(height > 0);

    --height;

    node_ptr = page_cur_get_rec(page_cursor);

    {
      Phy_rec record{m_index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    /* Go to the child node */
    page_no = m_btree->node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

rec_t *Btree_cursor::insert_if_possible(const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept {
  ut_ad(dtuple_check_typed(tuple));

  auto block = get_block();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  auto page_cursor = get_page_cur();

  /* Now, try the insert */
  auto rec = page_cur_tuple_insert(page_cursor, tuple, m_index, n_ext, mtr);

  if (unlikely(rec == nullptr)) {
    /* If record did not fit, reorganize */

    if (m_btree->page_reorganize(block, m_index, mtr)) {

      page_cur_search(block, m_index, tuple, PAGE_CUR_LE, page_cursor);

      rec = page_cur_tuple_insert(page_cursor, tuple, m_index, n_ext, mtr);
    }
  }

  return rec;
}

inline db_err Btree_cursor::ins_lock_and_undo(ulint flags, const DTuple *entry, que_thr_t *thr, mtr_t *mtr, bool *inherit) noexcept {
  /* Check if we have to wait for a lock: enqueue an explicit lock request if yes */

  auto rec = get_rec();
  auto index = m_index;
  auto err = m_lock_sys->rec_insert_check_and_lock(flags, rec, get_block(), index, thr, mtr, inherit);

  if (err != DB_SUCCESS) {

    return err;
  }

  roll_ptr_t roll_ptr;

  if (index->is_clustered()) {

    err = trx_undo_report_row_operation(flags, TRX_UNDO_INSERT_OP, thr, index, entry, nullptr, 0, nullptr, &roll_ptr);

    if (err != DB_SUCCESS) {

      return err;
    }

    /* Now we can fill in the roll ptr field in entry */

    if (!(flags & BTR_KEEP_SYS_FLAG)) {

      srv_row_upd->index_entry_sys_field(entry, index, DATA_ROLL_PTR, roll_ptr);
    }
  }

  return DB_SUCCESS;
}

#ifdef UNIV_DEBUG
void Btree_cursor::trx_report(Trx *trx, const Index *index, const char *op) noexcept{
  log_info(std::format("Trx with id {} op {} going to {} ", trx->m_id, op, index->m_name));
}
#endif /* UNIV_DEBUG */

db_err Btree_cursor::optimistic_insert(ulint flags, DTuple *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr) noexcept {
  big_rec_t *big_rec_vec = nullptr;
  page_cur_t *page_cursor;
  ulint max_size;
  rec_t *dummy_rec;
  bool leaf;
  bool reorg;
  bool inherit;
  ulint rec_size;
  db_err err;

  *big_rec = nullptr;

  auto block = get_block();
  auto page = block->get_frame();
  auto index = m_index;

  if (!dtuple_check_typed_no_assert(entry)) {
    log_err(std::format("Tuple to insert into {}", index->m_name));
  }

#ifdef UNIV_DEBUG
  if (m_print_record_ops && thr != nullptr) {
    trx_report(thr_get_trx(thr), index, "insert into ");
    dtuple_print(ib_stream, entry);
  }
#endif /* UNIV_DEBUG */

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  max_size = page_get_max_insert_size_after_reorganize(page, 1);
  leaf = page_is_leaf(page);

  /* Calculate the record size when entry is converted to a record */
  rec_size = rec_get_converted_size(index, entry, n_ext);

  if (page_rec_needs_ext(rec_size)) {
    /* The record is so big that we have to store some fields
    externally on separate database pages */
    big_rec_vec = dtuple_convert_big_rec(index, entry, &n_ext);

    if (unlikely(big_rec_vec == nullptr)) {

      return DB_TOO_BIG_RECORD;
    }

    rec_size = rec_get_converted_size(index, entry, n_ext);
  }

  /* If there have been many consecutive inserts, and we are on the leaf
  level, check if we have to split the page to reserve enough free space
  for future updates of records. */

  if (index->is_clustered() && page_get_n_recs(page) >= 2 &&
       likely(leaf) && Dict::index_get_space_reserve() + rec_size > max_size &&
       (m_btree->page_get_split_rec_to_right(this, dummy_rec) ||
        m_btree->page_get_split_rec_to_left(this, dummy_rec))) {

    if (big_rec_vec) {
      dtuple_convert_back_big_rec(entry, big_rec_vec);
    }

    return DB_FAIL;
  }

  if (unlikely(max_size < BTR_CUR_PAGE_REORGANIZE_LIMIT || max_size < rec_size) && likely(page_get_n_recs(page) > 1) && page_get_max_insert_size(page, 1) < rec_size) {

    if (big_rec_vec != nullptr) {
      dtuple_convert_back_big_rec(entry, big_rec_vec);
    }

    return DB_FAIL;
  }

  /* Check locks and write to the undo log, if specified */
  err = ins_lock_and_undo(flags, entry, thr, mtr, &inherit);

  if (unlikely(err != DB_SUCCESS)) {

    if (big_rec_vec) {
      dtuple_convert_back_big_rec(entry, big_rec_vec);
    }

    return err;
  }

  page_cursor = get_page_cur();

  /* Now, try the insert */

  {
    const auto page_cursor_rec = page_cur_get_rec(page_cursor);

    *rec = page_cur_tuple_insert(page_cursor, entry, index, n_ext, mtr);

    reorg = page_cursor_rec != page_cur_get_rec(page_cursor);

    ut_a(!reorg);
  }

  if (unlikely(!*rec) && likely(!reorg)) {
    /* If the record did not fit, reorganize */
    auto success = m_btree->page_reorganize(block, index, mtr);
    ut_a(success);

    ut_ad(page_get_max_insert_size(page, 1) == max_size);

    reorg = true;

    page_cur_search(block, index, entry, PAGE_CUR_LE, page_cursor);

    *rec = page_cur_tuple_insert(page_cursor, entry, index, n_ext, mtr);

    if (unlikely(!*rec)) {
      log_err(std::format("Cannot insert tuple into {}: ", index->m_name));
      dtuple_print(ib_stream, entry);
      ut_error;
    }
  }

  if (!(flags & BTR_NO_LOCKING_FLAG) && inherit) {

    m_lock_sys->update_insert(block, *rec);
  }

  *big_rec = big_rec_vec;

  return DB_SUCCESS;
}

db_err Btree_cursor::pessimistic_insert(ulint flags, DTuple *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr) noexcept {
  db_err err;
  bool success;
  bool dummy_inh;
  ulint n_extents{};
  ulint n_reserved{};
  mem_heap_t *heap{};
  auto index = m_index;
  big_rec_t *big_rec_vec{};

  ut_ad(dtuple_check_typed(entry));

  *big_rec = nullptr;

  ut_ad(mtr->memo_contains(get_index()->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(get_block(), MTR_MEMO_PAGE_X_FIX));

  /* Try first an optimistic insert; reset the cursor flag: we do not
  assume anything of how it was positioned */

  m_flag = BTR_CUR_BINARY;

  err = optimistic_insert(flags, entry, rec, big_rec, n_ext, thr, mtr);
  if (err != DB_FAIL) {

    return err;
  }

  /* Retry with a pessimistic insert. Check locks and write to undo log,
  if specified */

  err = ins_lock_and_undo(flags, entry, thr, mtr, &dummy_inh);

  if (err != DB_SUCCESS) {

    return err;
  }

  if (!(flags & BTR_NO_UNDO_LOG_FLAG)) {
    /* First reserve enough free space for the file segments
    of the index tree, so that the insert will not fail because
    of lack of space */

    n_extents = m_tree_height / 16 + 3;

    success = m_fsp->reserve_free_extents(&n_reserved, index->get_space_id(), n_extents, FSP_NORMAL, mtr);

    if (!success) {
      return DB_OUT_OF_FILE_SPACE;
    }
  }

  if (page_rec_needs_ext(rec_get_converted_size(index, entry, n_ext))) {

    /* The record is so big that we have to store some fields
    externally on separate database pages */

    if (likely_null(big_rec_vec)) {
      /* This should never happen, but we handle the situation in a robust manner. */
      ut_ad(0);
      dtuple_convert_back_big_rec(entry, big_rec_vec);
    }

    big_rec_vec = dtuple_convert_big_rec(index, entry, &n_ext);

    if (big_rec_vec == nullptr) {

      if (n_extents > 0) {
        m_fsp->m_fil->space_release_free_extents(index->get_space_id(), n_reserved);
      }
      return DB_TOO_BIG_RECORD;
    }
  }

  if (index->get_page_no() == get_block()->get_page_no()) {

    /* The page is the root page */
    *rec = m_btree->root_raise_and_insert(this, entry, n_ext, mtr);
  } else {
    *rec = m_btree->page_split_and_insert(this, entry, n_ext, mtr);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  ut_ad(page_rec_get_next(get_rec()) == *rec);

  if (!(flags & BTR_NO_LOCKING_FLAG)) {

    m_lock_sys->update_insert(get_block(), *rec);
  }

  if (n_extents > 0) {
    m_fsp->m_fil->space_release_free_extents(index->get_space_id(), n_reserved);
  }

  *big_rec = big_rec_vec;

  return DB_SUCCESS;
}

db_err Btree_cursor::upd_lock_and_undo(
  ulint flags,
  const upd_t *update,
  ulint cmpl_info,
  que_thr_t *thr,
  mtr_t *mtr,
  roll_ptr_t *roll_ptr
) noexcept {
  ut_ad(update && thr && roll_ptr);

  auto rec = get_rec();
  auto index = m_index;

  if (!index->is_clustered()) {
    /* We do undo logging only when we update a clustered index record */
    return m_lock_sys->sec_rec_modify_check_and_lock(flags, get_block(), rec, index, thr, mtr);
  }

  /* Check if we have to wait for a lock: enqueue an explicit lock request if yes */

  auto err = DB_SUCCESS;

  if (!(flags & BTR_NO_LOCKING_FLAG)) {
    ulint *offsets;
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    rec_offs_init(offsets_);

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &heap, Current_location());
    }

    err = m_lock_sys->clust_rec_modify_check_and_lock(flags, get_block(), rec, index, offsets, thr);

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  /* Append the info about the update in the undo log */

  err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr, index, nullptr, update, cmpl_info, rec, roll_ptr);

  return err;
}

void Btree_cursor::update_in_place_log(ulint flags, rec_t *rec, const Index *index, const upd_t *update, Trx *trx, roll_ptr_t roll_ptr, mtr_t *mtr) noexcept {
  ut_ad(flags < 256);

  auto log_ptr = mlog_open_and_write_index(
    mtr,
    rec,
    MLOG_REC_UPDATE_IN_PLACE,
    1 + DATA_ROLL_PTR_LEN + 14 + 2 + MLOG_BUF_MARGIN
  );

  if (log_ptr == nullptr) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  /* The code below assumes index is a clustered index: change index to
  the clustered index if we are updating a secondary index record (or we
  could as well skip writing the sys col values to the log in this case
  because they are not needed for a secondary index record update) */

  index = index->m_table->get_first_index();

  mach_write_to_1(log_ptr, flags);
  log_ptr++;

  log_ptr = srv_row_upd->write_sys_vals_to_log(index, trx, roll_ptr, log_ptr, mtr);
  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  srv_row_upd->index_write_log(update, log_ptr, mtr);
}

byte *Btree_cursor::parse_update_in_place(byte *ptr, byte *end_ptr, page_t *page, Index *index) noexcept {
  ulint pos;
  upd_t *update;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;

  if (end_ptr < ptr + 1) {

    return nullptr;
  }

  auto flags = mach_read_from_1(ptr);
  ++ptr;

  ptr = srv_row_upd->parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  auto rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  auto heap = mem_heap_create(256);

  ptr = srv_row_upd->index_parse(ptr, end_ptr, heap, &update);

  if (ptr != nullptr && page != nullptr) {
    auto rec = page + rec_offset;
    Phy_rec record{index, rec};
    auto offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      srv_row_upd->rec_sys_fields_in_recovery(rec, offsets, pos, trx_id, roll_ptr);
    }

    srv_row_upd->rec_in_place(rec, index, offsets, update);
  }

  mem_heap_free(heap);

  return ptr;
}

db_err Btree_cursor::update_in_place(ulint flags, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) noexcept {
  mem_heap_t *heap{};
  roll_ptr_t roll_ptr{};
  ulint was_delete_marked;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  auto rec = get_rec();
  auto index = m_index;

  /* The insert buffer tree should never be updated in place. */

  auto trx = thr_get_trx(thr);

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

#ifdef UNIV_DEBUG
  if (m_print_record_ops && thr != nullptr) {
    trx_report(trx, index, "update ");
    log_err(rec_to_string(rec));
  }
#endif /* UNIV_DEBUG */

  /* Do lock checking and undo logging */
  auto err = upd_lock_and_undo(flags, update, cmpl_info, thr, mtr, &roll_ptr);

  if (unlikely(err != DB_SUCCESS)) {

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
    return err;
  }

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    Row_update::rec_sys_fields(rec, index, offsets, trx, roll_ptr);
  }

  was_delete_marked = rec_get_deleted_flag(rec);

  srv_row_upd->rec_in_place(rec, index, offsets, update);

  update_in_place_log(flags, rec, index, update, trx, roll_ptr, mtr);

  if (was_delete_marked && !rec_get_deleted_flag(rec)) {
    /* The new updated record owns its possible externally stored fields */

    Blob blob{m_fsp, m_btree};

    blob.unmark_extern_fields(rec, index, offsets, mtr);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return DB_SUCCESS;
}

db_err Btree_cursor::optimistic_update(ulint flags, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) noexcept {
  ulint i;
  db_err err;
  Trx *trx;
  ulint n_ext;
  ulint max_size;
  ulint new_rec_size;
  ulint old_rec_size;
  roll_ptr_t roll_ptr;
  page_cur_t *page_cursor;

  auto block = get_block();
  auto page = block->get_frame();
  auto rec = get_rec();
  auto index = m_index;

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  auto heap = mem_heap_create(1024);

  ulint *offsets{};

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
  }

#ifdef UNIV_DEBUG
  if (m_print_record_ops && thr != nullptr) {
    trx_report(thr_get_trx(thr), index, "update ");
    log_err(rec_to_string(rec));
  }
#endif /* UNIV_DEBUG */

  if (!srv_row_upd->changes_field_size_or_external(index, offsets, update)) {

    /* The simplest and the most common case: the update does not
    change the size of any field and none of the updated fields is
    externally stored in rec or update. */

    mem_heap_free(heap);

    return update_in_place(flags, update, cmpl_info, thr, mtr);
  }

  if (rec_offs_any_extern(offsets)) {
  any_extern:
    /* Externally stored fields are treated in pessimistic
    update */

    mem_heap_free(heap);
    return DB_OVERFLOW;
  }

  for (i = 0; i < Row_update::upd_get_n_fields(update); i++) {
    if (dfield_is_ext(&update->get_nth_field(i)->m_new_val)) {

      goto any_extern;
    }
  }

  page_cursor = get_page_cur();

  auto new_entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

  /* We checked above that there are no externally stored fields. */
  ut_a(!n_ext);

  /* The page containing the clustered index record
  corresponding to new_entry is latched in mtr.
  Thus the following call is safe. */
  srv_row_upd->index_replace_new_col_vals_index_pos(new_entry, index, update, false, heap);
  old_rec_size = rec_offs_size(offsets);
  new_rec_size = rec_get_converted_size(index, new_entry, 0);

  if (unlikely(new_rec_size >= page_get_free_space_of_empty() / 2)) {

    err = DB_OVERFLOW;
    goto err_exit;
  }

  if (unlikely(page_get_data_size(page) - old_rec_size + new_rec_size < BTR_CUR_PAGE_LOW_FILL_LIMIT)) {

    /* The page would become too empty */

    err = DB_UNDERFLOW;
    goto err_exit;
  }

  max_size = old_rec_size + page_get_max_insert_size_after_reorganize(page, 1);

  if (!((max_size >= BTR_CUR_PAGE_REORGANIZE_LIMIT && max_size >= new_rec_size) || page_get_n_recs(page) <= 1)) {

    /* There was not enough space, or it did not pay to
    reorganize: for simplicity, we decide what to do assuming a
    reorganization is needed, though it might not be necessary */

    err = DB_OVERFLOW;
    goto err_exit;
  }

  /* Do lock checking and undo logging */
  err = upd_lock_and_undo(flags, update, cmpl_info, thr, mtr, &roll_ptr);

  if (err != DB_SUCCESS) {
  err_exit:
    mem_heap_free(heap);
    return err;
  }

  /* Ok, we may do the replacement. Store on the page infimum the
  explicit locks on rec, before deleting rec (see the comment in
  pessimistic_update). */

  m_lock_sys->rec_store_on_page_infimum(block, rec);

  /* The call to row_rec_to_index_entry(ROW_COPY_DATA, ...) above
  invokes rec_offs_make_valid() to point to the copied record that
  the fields of new_entry point to.  We have to undo it here. */
  ut_ad(rec_offs_validate(nullptr, index, offsets));
  ut_d(rec_offs_make_valid(page_cur_get_rec(page_cursor), index, offsets));

  page_cur_delete_rec(page_cursor, index, offsets, mtr);

  page_cur_move_to_prev(page_cursor);

  trx = thr_get_trx(thr);

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    srv_row_upd->index_entry_sys_field(new_entry, index, DATA_ROLL_PTR, roll_ptr);
    srv_row_upd->index_entry_sys_field(new_entry, index, DATA_TRX_ID, trx->m_id);
  }

  /* There are no externally stored columns in new_entry */
  rec = insert_if_possible(new_entry, 0 /*n_ext*/, mtr);

  /* We calculated above the insert would fit */
  ut_a(rec != nullptr);

  /* Restore the old explicit lock state on the record */

  m_lock_sys->rec_restore_from_page_infimum(block, rec, block);

  page_cur_move_to_next(page_cursor);

  mem_heap_free(heap);

  return DB_SUCCESS;
}

void Btree_cursor::pess_upd_restore_supremum(Buf_block *block, const rec_t *rec, mtr_t *mtr) noexcept {
  auto page = block->get_frame();

  if (page_rec_get_next(page_get_infimum_rec(page)) != rec) {
    /* Updated record is not the first user record on its page */

    return;
  }

  const auto space = block->get_space();
  const auto prev_page_no = m_btree->page_get_prev(page, mtr);

  ut_ad(prev_page_no != FIL_NULL);

  Buf_pool::Request req {
    .m_rw_latch = RW_NO_LATCH,
    .m_page_id = { space, prev_page_no },
    .m_mode = BUF_GET_NO_LATCH,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto prev_block = m_btree->m_buf_pool->get(req, nullptr);

#ifdef UNIV_BTR_DEBUG
  ut_a(m_btree->page_get_next(prev_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

  /* We must already have an x-latch on prev_block! */
  ut_ad(mtr->memo_contains(prev_block, MTR_MEMO_PAGE_X_FIX));

  m_lock_sys->rec_reset_and_inherit_gap_locks(prev_block, block, PAGE_HEAP_NO_SUPREMUM, page_rec_get_heap_no(rec));
}

db_err Btree_cursor::pessimistic_update(
  ulint flags,
  mem_heap_t **heap,
  big_rec_t **big_rec,
  const upd_t *update,
  ulint cmpl_info,
  que_thr_t *thr,
  mtr_t *mtr
) noexcept {

  big_rec_t *big_rec_vec = nullptr;
  big_rec_t *dummy_big_rec;
  page_cur_t *page_cursor;
  roll_ptr_t roll_ptr;
  bool was_first;
  ulint n_extents = 0;
  ulint n_reserved;
  ulint n_ext;
  ulint *offsets = nullptr;

  *big_rec = nullptr;

  auto index = m_index;
  auto block = get_block();

  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  auto optim_err = optimistic_update(flags, update, cmpl_info, thr, mtr);

  switch (optim_err) {
    case DB_UNDERFLOW:
    case DB_OVERFLOW:
      break;
    default:
      return optim_err;
  }

  /* Do lock checking and undo logging */
  auto err = upd_lock_and_undo(flags, update, cmpl_info, thr, mtr, &roll_ptr);

  if (err != DB_SUCCESS) {

    return err;
  }

  if (optim_err == DB_OVERFLOW) {
    /* First reserve enough free space for the file segments
    of the index tree, so that the update will not fail because
    of lack of space */

    n_extents = m_tree_height / 16 + 3;

    auto reserve_flag = flags & BTR_NO_UNDO_LOG_FLAG ? FSP_CLEANING : FSP_NORMAL;

    if (!m_fsp->reserve_free_extents(&n_reserved, index->get_space_id(), n_extents, reserve_flag, mtr)) {
      return DB_OUT_OF_FILE_SPACE;
    }
  }

  if (*heap == nullptr) {
    *heap = mem_heap_create(1024);
  }

  auto rec = get_rec();

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, heap, Current_location()); 
  }

  auto trx = thr_get_trx(thr);

  auto new_entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, *heap);

  /* The call to row_rec_to_index_entry(ROW_COPY_DATA, ...) above
  invokes rec_offs_make_valid() to point to the copied record that
  the fields of new_entry point to.  We have to undo it here. */
  ut_ad(rec_offs_validate(nullptr, index, offsets));
  ut_d(rec_offs_make_valid(rec, index, offsets));

  /* The page containing the clustered index record
  corresponding to new_entry is latched in mtr.  If the
  clustered index record is delete-marked, then its externally
  stored fields cannot have been purged yet, because then the
  purge would also have removed the clustered index record
  itself.  Thus the following call is safe. */
  srv_row_upd->index_replace_new_col_vals_index_pos(new_entry, index, update, false, *heap);

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    srv_row_upd->index_entry_sys_field(new_entry, index, DATA_ROLL_PTR, roll_ptr);
    srv_row_upd->index_entry_sys_field(new_entry, index, DATA_TRX_ID, trx->m_id);
  }

  Blob blob{m_fsp, m_btree};

  if ((flags & BTR_NO_UNDO_LOG_FLAG) && rec_offs_any_extern(offsets)) {
    /* We are in a transaction rollback undoing a row
    update: we must free possible externally stored fields
    which got new values in the update, if they are not
    inherited values. They can be inherited if we have
    updated the primary key to another value, and then
    update it back again. */


    ut_ad(big_rec_vec == nullptr);

    blob.free_updated_extern_fields(index, rec, offsets, update, trx_is_recv(trx) ? RB_RECOVERY : RB_NORMAL, mtr);
  }

  /* We have to set appropriate extern storage bits in the new
  record to be inserted: we have to remember which fields were such */

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, heap, Current_location());
  }

  n_ext += blob.push_update_extern_fields(new_entry, update, *heap);

  if (page_rec_needs_ext(rec_get_converted_size(index, new_entry, n_ext))) {
    big_rec_vec = dtuple_convert_big_rec(index, new_entry, &n_ext);

    if (unlikely(big_rec_vec == nullptr)) {

      err = DB_TOO_BIG_RECORD;
      goto return_after_reservations;
    }
  }

  /* Store state of explicit locks on rec on the page infimum record,
  before deleting rec. The page infimum acts as a dummy carrier of the
  locks, taking care also of lock releases, before we can move the locks
  back on the actual record. There is a special case: if we are
  inserting on the root page and the insert causes a call of
  btr_root_raise_and_insert. Therefore we cannot in the lock system
  delete the lock structs set on the root page even if the root
  page carries just node pointers. */

  m_lock_sys->rec_store_on_page_infimum(block, rec);

  page_cursor = get_page_cur();

  page_cur_delete_rec(page_cursor, index, offsets, mtr);

  page_cur_move_to_prev(page_cursor);

  rec = insert_if_possible(new_entry, n_ext, mtr);

  if (rec != nullptr) {
    m_lock_sys->rec_restore_from_page_infimum(get_block(), rec, block);

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, heap, Current_location());
    }

    if (!rec_get_deleted_flag(rec)) {
      /* The new inserted record owns its possible externally
      stored fields */
      Blob blob{m_fsp, m_btree};

      blob.unmark_extern_fields(rec, index, offsets, mtr);
    }

    err = DB_SUCCESS;
    goto return_after_reservations;
  } else {
    ut_a(optim_err != DB_UNDERFLOW);
  }

  /* Was the record to be updated positioned as the first user
  record on its page? */
  was_first = page_cur_is_before_first(page_cursor);

  /* The first parameter means that no lock checking and undo logging
  is made in the insert */

  err = pessimistic_insert(BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG, new_entry, &rec, &dummy_big_rec, n_ext, nullptr, mtr);

  ut_a(rec != nullptr);
  ut_a(err == DB_SUCCESS);
  ut_a(dummy_big_rec == nullptr);

  if (!index->is_clustered()) {
    /* Update PAGE_MAX_TRX_ID in the index page header.
    It was not updated by pessimistic_insert()
    because of BTR_NO_LOCKING_FLAG. */
    auto rec_block = get_block();

    page_update_max_trx_id(rec_block, trx->m_id, mtr);
  }

  if (!rec_get_deleted_flag(rec)) {
    /* The new inserted record owns its possible externally
    stored fields */

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, heap, Current_location());
    }

    Blob blob{m_fsp, m_btree};

    blob.unmark_extern_fields(rec, index, offsets, mtr);
  }

  m_lock_sys->rec_restore_from_page_infimum(get_block(), rec, block);

  /* If necessary, restore also the correct lock state for a new,
  preceding supremum record created in a page split. While the old
  record was nonexistent, the supremum might have inherited its locks
  from a wrong record. */

  if (!was_first) {
    pess_upd_restore_supremum(get_block(), rec, mtr);
  }

return_after_reservations:

  if (n_extents > 0) {
    m_fsp->m_fil->space_release_free_extents(index->get_space_id(), n_reserved);
  }

  *big_rec = big_rec_vec;

  return err;
}

void Btree_cursor::del_mark_set_clust_rec_log(ulint flags, rec_t *rec, const Index *index, bool val, Trx *trx, roll_ptr_t roll_ptr, mtr_t *mtr) noexcept {
  ut_ad(flags < 256);

  auto log_ptr = mlog_open_and_write_index(
    mtr,
    rec,
    MLOG_REC_CLUST_DELETE_MARK,
    1 + 1 + DATA_ROLL_PTR_LEN + 14 + 2
  );

  if (log_ptr == nullptr) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  mach_write_to_1(log_ptr, flags);
  ++log_ptr;

  mach_write_to_1(log_ptr, (ulint)val);
  ++log_ptr;

  log_ptr = srv_row_upd->write_sys_vals_to_log(index, trx, roll_ptr, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(rec));

  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

byte *Btree_cursor::parse_del_mark_set_clust_rec(byte *ptr, byte *end_ptr, page_t *page, Index *index) noexcept{
  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  auto flags = mach_read_from_1(ptr);
  ++ptr;

  auto val = mach_read_from_1(ptr);
  ++ptr;

  ulint pos;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;

  ptr = srv_row_upd->parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  auto offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page != nullptr) {
    auto rec = page + offset;

    set_deleted_flag(rec, val > 0);

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      mem_heap_t *heap = nullptr;
      ulint offsets_[REC_OFFS_NORMAL_SIZE];
      rec_offs_init(offsets_);

      ulint *offsets;

      {
        Phy_rec record{index, rec};

        offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &heap, Current_location());
      }

      srv_row_upd->rec_sys_fields_in_recovery(rec, offsets, pos, trx_id, roll_ptr);

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }
    }
  }

  return ptr;
}

db_err Btree_cursor::del_mark_set_clust_rec(ulint flags, bool val, que_thr_t *thr, mtr_t *mtr) noexcept {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  auto rec = get_rec();
  auto index = m_index;

  mem_heap_t *heap{};

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

#ifdef UNIV_DEBUG
  if (m_print_record_ops && thr != nullptr) {
    trx_report(thr_get_trx(thr), index, "del mark ");
    log_err(rec_to_string(rec));
  }
#endif /* UNIV_DEBUG */

  ut_ad(index->is_clustered());
  ut_ad(!rec_get_deleted_flag(rec));

  auto err = m_lock_sys->clust_rec_modify_check_and_lock(flags, get_block(), rec, index, offsets, thr);

  if (err == DB_SUCCESS) {
    roll_ptr_t roll_ptr;

    err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr, index, nullptr, nullptr, 0, rec, &roll_ptr);

    if (err == DB_SUCCESS) {
      set_deleted_flag(rec, val > 0);

      auto trx = thr_get_trx(thr);

      if (!(flags & BTR_KEEP_SYS_FLAG)) {
        Row_update::rec_sys_fields(rec, index, offsets, trx, roll_ptr);
      }

      del_mark_set_clust_rec_log(flags, rec, index, val, trx, roll_ptr, mtr);
    }
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return err;
}

void Btree_cursor::del_mark_set_sec_rec_log(rec_t *rec, bool val, mtr_t *mtr) noexcept {
  auto log_ptr = mlog_open(mtr, 11 + 1 + 2);

  if (log_ptr == nullptr) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns nullptr */
    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_DELETE_MARK, log_ptr, mtr);

  mach_write_to_1(log_ptr, (ulint)val);
  ++log_ptr;

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

byte *Btree_cursor::parse_del_mark_set_sec_rec(byte *ptr, byte *end_ptr, page_t *page) noexcept {
  if (end_ptr < ptr + 3) {

    return nullptr;
  }

  auto val = mach_read_from_1(ptr);
  ++ptr;

  auto offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page != nullptr) {
    auto rec = page + offset;

    set_deleted_flag(rec, val > 0);
  }

  return ptr;
}

db_err Btree_cursor::del_mark_set_sec_rec(ulint flags, bool val, que_thr_t *thr, mtr_t *mtr) noexcept {
  auto rec = get_rec();

#ifdef UNIV_DEBUG
  if (m_print_record_ops && thr != nullptr) {
    trx_report(thr_get_trx(thr), m_index, "del mark ");
    log_err(rec_to_string(rec));
  }
#endif /* UNIV_DEBUG */

  auto err = m_lock_sys->sec_rec_modify_check_and_lock(flags, get_block(), rec, m_index, thr, mtr);

  if (err != DB_SUCCESS) {

    return err;
  }

  set_deleted_flag(rec, val > 0);

  del_mark_set_sec_rec_log(rec, val, mtr);

  return DB_SUCCESS;
}

bool Btree_cursor::optimistic_delete(mtr_t *mtr) noexcept {
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(mtr->memo_contains(get_block(), MTR_MEMO_PAGE_X_FIX));

  /* This is intended only for leaf page deletions */
  auto block = get_block();

  ut_ad(page_is_leaf(block->get_frame()));

  auto rec = get_rec();

  {
    Phy_rec record{m_index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  bool deleted{};

  if (!rec_offs_any_extern(offsets) && delete_will_underflow(rec_offs_size(offsets), mtr)) {
    auto page = block->get_frame();

    m_lock_sys->update_delete(block, rec);

    page_get_max_insert_size_after_reorganize(page, 1);

    page_cur_delete_rec(get_page_cur(), m_index, offsets, mtr);

    deleted = true;
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return deleted;
}

void Btree_cursor::pessimistic_delete(db_err *err, bool has_reserved_extents, trx_rb_ctx rb_ctx, mtr_t *mtr) noexcept {
  ulint n_extents{};
  ulint n_reserved{};

  auto block = get_block();
  auto page = block->get_frame();
  auto index = get_index();

  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  if (!has_reserved_extents) {
    /* First reserve enough free space for the file segments
    of the index tree, so that the node pointer updates will
    not fail because of lack of space */

    n_extents = m_tree_height / 32 + 1;

    auto success = m_fsp->reserve_free_extents(&n_reserved, index->get_space_id(), n_extents, FSP_CLEANING, mtr);

    if (!success) {
      *err = DB_OUT_OF_FILE_SPACE;
    }
  }

  auto heap = mem_heap_create(1024);
  auto rec = get_rec();
  ulint *offsets{};

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (rec_offs_any_extern(offsets)) {
    Blob blob{m_fsp, m_btree};

    blob.free_externally_stored_fields(index, rec, offsets, rb_ctx, mtr);
  }

  if (page_get_n_recs(page) < 2 && index->get_page_no() != block->get_page_no()) {

    /* If there is only one record, drop the whole page in
    btr_discard_page, if this is not the root page */

    m_btree->discard_page(this, mtr);

    *err = DB_SUCCESS;

  } else {
    m_lock_sys->update_delete(block, rec);

    const auto level = m_btree->page_get_level(page, mtr);

    if (level > 0 && rec == page_rec_get_next(page_get_infimum_rec(page))) {

      auto next_rec = page_rec_get_next(rec);

      if (m_btree->page_get_prev(page, mtr) == FIL_NULL) {

        /* If we delete the leftmost node pointer on a
        non-leaf level, we must mark the new leftmost node
        pointer as the predefined minimum record */

        m_btree->set_min_rec_mark(next_rec, mtr);

      } else {

        /* Otherwise, if we delete the leftmost node pointer
        on a page, we have to change the father node pointer
        so that it is equal to the new leftmost node pointer
        on the page */

        m_btree->node_ptr_delete(index, block, mtr);

        auto node_ptr = index->build_node_ptr(next_rec, block->get_page_no(), heap, level);

        m_btree->insert_on_non_leaf_level(index, level + 1, node_ptr, mtr, Current_location());
      }
    }

    page_cur_delete_rec(get_page_cur(), index, offsets, mtr);

    ut_ad(m_btree->check_node_ptr(index, block, mtr));

    *err = DB_SUCCESS;
  }

  mem_heap_free(heap);

  if (n_extents > 0) {
    m_fsp->m_fil->space_release_free_extents(index->get_space_id(), n_reserved);
  }
}

void Btree_cursor::add_path_info(Paths *path, ulint height, ulint root_height) noexcept {
  if (root_height >= BTR_PATH_ARRAY_N_SLOTS - 1) {
    /* Do nothing; return empty path */

    auto slot = path->begin();
    slot->m_nth_rec = ULINT_UNDEFINED;

    return;
  }

  if (height == 0) {
    /* Mark end of slots for path */
    auto slot = path->begin() + root_height + 1;
    slot->m_nth_rec = ULINT_UNDEFINED;
  }

  auto rec = get_rec();

  auto slot = path->begin() + (root_height - height);

  slot->m_nth_rec = page_rec_get_n_recs_before(rec);
  slot->m_n_recs = page_get_n_recs(page_align(rec));
}

int64_t Btree_cursor::estimate_n_rows_in_range(Index *index, const DTuple *tuple1, ulint mode1, const DTuple *tuple2, ulint mode2) noexcept {
  Paths path1;

  mtr_t mtr;

  mtr.start();

  if (dtuple_get_n_fields(tuple1) > 0) {
    search_to_nth_level(&path1, index, 0, tuple1, mode1, BTR_SEARCH_LEAF | BTR_ESTIMATE, &mtr, Current_location());
  } else {
    open_at_index_side(&path1, true, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, 0, &mtr, Current_location());
  }

  mtr.commit();

  Paths path2;

  mtr.start();

  if (dtuple_get_n_fields(tuple2) > 0) {
    search_to_nth_level(&path2, index, 0, tuple2, mode2, BTR_SEARCH_LEAF | BTR_ESTIMATE, &mtr, Current_location());
  } else {
    open_at_index_side(&path2, false, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, 0, &mtr, Current_location());
  }

  mtr.commit();

  /* We have the path information for the range in path1 and path2 */

  int64_t n_rows = 1;

  /* This becomes true when the path is not the same any more */
  bool diverged{};

  /* This becomes true when the paths are not the same or adjacent any more */
  bool diverged_lot{};

  /* This is the level where paths diverged a lot */
  ulint divergence_level = 1000000;

  for (ulint i{};; i++) {
    ut_ad(i < BTR_PATH_ARRAY_N_SLOTS);

    auto slot1 = &path1[i];
    auto slot2 = &path2[i];

    if (slot1->m_nth_rec == ULINT_UNDEFINED || slot2->m_nth_rec == ULINT_UNDEFINED) {

      if (i > divergence_level + 1) {
        /* In trees whose height is > 1 our algorithm
        tends to underestimate: multiply the estimate
        by 2: */

        n_rows = n_rows * 2;
      }

      /* Do not estimate the number of rows in the range
      to over 1 / 2 of the estimated rows in the whole
      table */

      if (n_rows > index->m_table->m_stats.m_n_rows / 2) {
        n_rows = index->m_table->m_stats.m_n_rows / 2;

        /* If there are just 0 or 1 rows in the table,
        then we estimate all rows are in the range */

        if (n_rows == 0) {
          n_rows = index->m_table->m_stats.m_n_rows;
        }
      }

      return n_rows;
    }

    if (!diverged && slot1->m_nth_rec != slot2->m_nth_rec) {

      diverged = true;

      if (slot1->m_nth_rec < slot2->m_nth_rec) {
        n_rows = slot2->m_nth_rec - slot1->m_nth_rec;

        if (n_rows > 1) {
          diverged_lot = true;
          divergence_level = i;
        }
      } else {
        /* Maybe the tree has changed between
        searches */

        return 10;
      }

    } else if (diverged && !diverged_lot) {

      if (slot1->m_nth_rec < slot1->m_n_recs || slot2->m_nth_rec > 1) {

        diverged_lot = true;
        divergence_level = i;

        n_rows = 0;

        if (slot1->m_nth_rec < slot1->m_n_recs) {
          n_rows += slot1->m_n_recs - slot1->m_nth_rec;
        }

        if (slot2->m_nth_rec > 1) {
          n_rows += slot2->m_nth_rec - 1;
        }
      }
    } else if (diverged_lot) {

      n_rows = (n_rows * (slot1->m_n_recs + slot2->m_n_recs)) / 2;
    }
  }
}

void Btree_cursor::estimate_number_of_different_key_vals(const Index *index) noexcept {
  page_t *page;
  rec_t *rec;
  ulint n_cols;
  ulint matched_fields;
  ulint matched_bytes;
  int64_t *n_diff;
  uint64_t n_sample_pages; /* number of pages to sample */
  ulint not_empty_flag = 0;
  ulint total_external_size = 0;
  ulint j;
  uint64_t add_on;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_rec_[REC_OFFS_NORMAL_SIZE];
  ulint offsets_next_rec_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets_rec = offsets_rec_;
  ulint *offsets_next_rec = offsets_next_rec_;
  rec_offs_init(offsets_rec_);
  rec_offs_init(offsets_next_rec_);

  n_cols = index->get_n_unique();

  n_diff = (int64_t *)mem_zalloc((n_cols + 1) * sizeof(int64_t));

  /* It makes no sense to test more pages than are contained
  in the index, thus we lower the number if it is too high */
  if (srv_config.m_stats_sample_pages > index->m_stats.m_index_size) {
    if (index->m_stats.m_index_size > 0) {
      n_sample_pages = index->m_stats.m_index_size;
    } else {
      n_sample_pages = 1;
    }
  } else {
    n_sample_pages = srv_config.m_stats_sample_pages;
  }

  /* We sample some pages in the index to get an estimate */

  Blob blob(m_fsp, m_btree);

  for (ulint i = 0; i < n_sample_pages; i++) {
    rec_t *supremum;
    mtr.start();

    open_at_rnd_pos(index, BTR_SEARCH_LEAF, &mtr, Current_location());

    /* Count the number of different key values for each prefix of
    the key on this index page. If the prefix does not determine
    the index record uniquely in the B-tree, then we subtract one
    because otherwise our algorithm would give a wrong estimate
    for an index where there is just one key value. */

    page = get_page_no();

    supremum = page_get_supremum_rec(page);

    rec = page_rec_get_next(page_get_infimum_rec(page));

    if (rec != supremum) {
      not_empty_flag = 1;

      Phy_rec record{index, rec};

      offsets_rec = record.get_col_offsets(offsets_rec, ULINT_UNDEFINED, &heap, Current_location());
    }

    while (rec != supremum) {
      auto next_rec = page_rec_get_next(rec);

      if (next_rec == supremum) {
        break;
      }

      matched_fields = 0;
      matched_bytes = 0;

      {
        Phy_rec record{index, next_rec};

        offsets_next_rec = record.get_col_offsets(offsets_next_rec, ULINT_UNDEFINED, &heap, Current_location());
      }

      cmp_rec_rec_with_match(rec, next_rec, offsets_rec, offsets_next_rec, index, &matched_fields, &matched_bytes);

      for (j = matched_fields + 1; j <= n_cols; j++) {
        /* We add one if this index record has
        a different prefix from the previous */

        ++n_diff[j];
      }


      total_external_size += blob.get_externally_stored_len(rec, offsets_rec);

      rec = next_rec;
      /* Initialize offsets_rec for the next round
      and assign the old offsets_rec buffer to
      offsets_next_rec. */
      {
        ulint *offsets_tmp = offsets_rec;
        offsets_rec = offsets_next_rec;
        offsets_next_rec = offsets_tmp;
      }
    }

    if (n_cols == index->get_n_unique_in_tree()) {

      /* If there is more than one leaf page in the tree,
      we add one because we know that the first record
      on the page certainly had a different prefix than the
      last record on the previous index page in the
      alphabetical order. Before this fix, if there was
      just one big record on each clustered index page, the
      algorithm grossly underestimated the number of rows
      in the table. */

      if (m_btree->page_get_prev(page, &mtr) != FIL_NULL || m_btree->page_get_next(page, &mtr) != FIL_NULL) {

        ++n_diff[n_cols];
      }
    }

    {
      Phy_rec record{index, rec};

      offsets_rec = record.get_col_offsets(offsets_rec, ULINT_UNDEFINED, &heap, Current_location());
    }

    total_external_size += blob.get_externally_stored_len(rec, offsets_rec);

    mtr.commit();
  }

  /* If we saw k borders between different key values on
  n_sample_pages leaf pages, we can estimate how many
  there will be in index->stat_n_leaf_pages */

  /* We must take into account that our sample actually represents
  also the pages used for external storage of fields (those pages are
  included in index->stat_n_leaf_pages) */

  srv_dict_sys->index_stat_mutex_enter(index);

  for (j = 0; j <= n_cols; j++) {
    index->m_stats.m_n_diff_key_vals[j] =
      ((n_diff[j] * (int64_t)index->m_stats.m_n_leaf_pages + n_sample_pages - 1 + total_external_size + not_empty_flag) /
       (n_sample_pages + total_external_size));

    /* If the tree is small, smaller than
    10 * n_sample_pages + total_external_size, then
    the above estimate is ok. For bigger trees it is common that we
    do not see any borders between key values in the few pages
    we pick. But still there may be n_sample_pages
    different key values, or even more. Let us try to approximate
    that: */

    add_on = index->m_stats.m_n_leaf_pages / (10 * (n_sample_pages + total_external_size));

    if (add_on > n_sample_pages) {
      add_on = n_sample_pages;
    }

    index->m_stats.m_n_diff_key_vals[j] += add_on;
  }

  srv_dict_sys->index_stat_mutex_exit(index);

  mem_free(n_diff);

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}


