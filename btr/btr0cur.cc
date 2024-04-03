/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.

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

/** If the following is set to true, this module prints a lot of
trace information of individual record operations */
IF_DEBUG(bool btr_cur_print_record_ops = false;)

/** In the optimistic insert, if the insert does not fit, but this much space
can be released by page reorganize, then it is reorganized */
constexpr ulint BTR_CUR_PAGE_REORGANIZE_LIMIT = UNIV_PAGE_SIZE / 32;

/** The structure of a BLOB part header */
/* @{ */
/*--------------------------------------*/

/** BLOB part len on this page */
constexpr ulint BTR_BLOB_HDR_PART_LEN = 0;

/** next BLOB part page no, FIL_NULL if none */
constexpr ulint BTR_BLOB_HDR_NEXT_PAGE_NO = 4;

/*--------------------------------------*/
/** Size of a BLOB  part header, in bytes */
constexpr ulint BTR_BLOB_HDR_SIZE = 8;

/* @} */

/** A BLOB field reference full of zero, for use in assertions and tests.
Initially, BLOB field references are set to zero, in
dtuple_convert_big_rec(). */
const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE] = {};

/** Marks all extern fields in a record as owned by the record. This function
should be called if the delete mark of a record is removed: a not delete
marked record always owns all its extern fields. */
static void btr_cur_unmark_extern_fields(
  rec_t *rec,           /*!< in/out: record in a clustered index */
  dict_index_t *index,  /*!< in: index of the page */
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  mtr_t *mtr
); /*!< in: mtr, or nullptr if not logged */

/** Adds path information to the cursor for the current page, for which
the binary search has been performed. */
static void btr_cur_add_path_info(
  btr_cur_t *cursor, /*!< in: cursor positioned on a page */
  ulint height,      /*!< in: height of the page in tree;
                                         0 means leaf node */
  ulint root_height
); /*!< in: root node height in tree */

/** Frees the externally stored fields for a record, if the field is mentioned
in the update vector. */
static void btr_rec_free_updated_extern_fields(
  dict_index_t *index,    /*!< in: index of rec; the index tree MUST be
                            X-latched */
  rec_t *rec,             /*!< in: record */
  const ulint *offsets,   /*!< in: rec_get_offsets(rec, index) */
  const upd_t *update,    /*!< in: update vector */
  enum trx_rb_ctx rb_ctx, /*!< in: rollback context */
  mtr_t *mtr
); /*!< in: mini-transaction handle which contains
                            an X-latch to record page and to the tree */

/** Frees the externally stored fields for a record. */
static void btr_rec_free_externally_stored_fields(
  dict_index_t *index,    /*!< in: index of the data, the index
                            tree MUST be X-latched */
  rec_t *rec,             /*!< in: record */
  const ulint *offsets,   /*!< in: rec_get_offsets(rec, index) */
  enum trx_rb_ctx rb_ctx, /*!< in: rollback context */
  mtr_t *mtr
); /*!< in: mini-transaction handle which contains
                            an X-latch to record page and to the index
                            tree */

/** Gets the externally stored size of a record, in units of a database page.
@return	externally stored part, in units of a database page */
static ulint btr_rec_get_externally_stored_len(
  rec_t *rec, /*!< in: record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */

/** The following function is used to set the deleted bit of a record. */
inline void btr_rec_set_deleted_flag(
  rec_t *rec, /*!< in/out: physical record */
  ulint flag
) /*!< in: nonzero if delete marked */
{
  if (page_rec_is_comp(rec)) {
    rec_set_deleted_flag_new(rec, flag);
  } else {
    rec_set_deleted_flag_old(rec, flag);
  }
}

void btr_cur_var_init() {
  ut_d(btr_cur_print_record_ops = false);
}

/** Latches the leaf page or pages requested. */
static void btr_cur_latch_leaves(
  page_t *page,         /*!< in: leaf page where the search
                                           converged */
  ulint space,          /*!< in: space id */
  ulint page_no, /*!< in: page number of the leaf */
  ulint latch_mode,     /*!< in: BTR_SEARCH_LEAF, ... */
  btr_cur_t *cursor,    /*!< in: cursor */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint mode;
  ulint left_page_no;
  ulint right_page_no;
  buf_block_t *get_block;

  ut_ad(page && mtr);

  switch (latch_mode) {
    case BTR_SEARCH_LEAF:
    case BTR_MODIFY_LEAF:
      mode = latch_mode == BTR_SEARCH_LEAF ? RW_S_LATCH : RW_X_LATCH;
      get_block = btr_block_get(space, page_no, mode, mtr);
#ifdef UNIV_BTR_DEBUG
      ut_a(page_is_comp(get_block->m_frame) == page_is_comp(page));
#endif /* UNIV_BTR_DEBUG */
      get_block->m_check_index_page_at_flush = true;
      return;
    case BTR_MODIFY_TREE:
      /* x-latch also brothers from left to right */
      left_page_no = btr_page_get_prev(page, mtr);

      if (left_page_no != FIL_NULL) {
        get_block = btr_block_get(space, left_page_no, RW_X_LATCH, mtr);
#ifdef UNIV_BTR_DEBUG
        ut_a(page_is_comp(get_block->m_frame) == page_is_comp(page));
        ut_a(btr_page_get_next(get_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        get_block->m_check_index_page_at_flush = true;
      }

      get_block = btr_block_get(space, page_no, RW_X_LATCH, mtr);
#ifdef UNIV_BTR_DEBUG
      ut_a(page_is_comp(get_block->get_frame()) == page_is_comp(page));
#endif /* UNIV_BTR_DEBUG */
      get_block->m_check_index_page_at_flush = true;

      right_page_no = btr_page_get_next(page, mtr);

      if (right_page_no != FIL_NULL) {
        get_block = btr_block_get(space, right_page_no, RW_X_LATCH, mtr);
#ifdef UNIV_BTR_DEBUG
        ut_a(page_is_comp(get_block->get_frame()) == page_is_comp(page));
        ut_a(btr_page_get_prev(get_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        get_block->m_check_index_page_at_flush = true;
      }

      return;

    case BTR_SEARCH_PREV:
    case BTR_MODIFY_PREV:
      mode = latch_mode == BTR_SEARCH_PREV ? RW_S_LATCH : RW_X_LATCH;
      /* latch also left brother */
      left_page_no = btr_page_get_prev(page, mtr);

      if (left_page_no != FIL_NULL) {
        get_block = btr_block_get(space, left_page_no, mode, mtr);
        cursor->left_block = get_block;
#ifdef UNIV_BTR_DEBUG
        ut_a(page_is_comp(get_block->get_frame()) == page_is_comp(page));
        ut_a(btr_page_get_next(get_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */
        get_block->m_check_index_page_at_flush = true;
      }

      get_block = btr_block_get(space, page_no, mode, mtr);
#ifdef UNIV_BTR_DEBUG
      ut_a(page_is_comp(get_block->get_frame()) == page_is_comp(page));
#endif /* UNIV_BTR_DEBUG */
      get_block->m_check_index_page_at_flush = true;
      return;
  }

  ut_error;
}

void btr_cur_search_to_nth_level(
  dict_index_t *dict_index, ulint level, const dtuple_t *tuple, ulint mode, ulint latch_mode, btr_cur_t *cursor,
  ulint has_search_latch, const char *file, ulint line, mtr_t *mtr
) {
  page_cur_t *page_cursor;
  page_t *page;
  rec_t *node_ptr;
  ulint page_no;
  ulint space;
  ulint up_match;
  ulint up_bytes;
  ulint low_match;
  ulint low_bytes;
  ulint height;
  ulint savepoint;
  ulint page_mode;
  ulint insert_planned;
  ulint estimate;
  ulint root_height = 0; /* remove warning */
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);
  /* Currently, PAGE_CUR_LE is the only search mode used for searches
  ending to upper levels */

  ut_ad(level == 0 || mode == PAGE_CUR_LE);
  ut_ad(dict_index_check_search_tuple(dict_index, tuple));
  ut_ad(dtuple_check_typed(tuple));

#ifdef UNIV_DEBUG
  cursor->up_match = ULINT_UNDEFINED;
  cursor->low_match = ULINT_UNDEFINED;
#endif
  insert_planned = latch_mode & BTR_INSERT;
  estimate = latch_mode & BTR_ESTIMATE;
  latch_mode = latch_mode & ~(BTR_INSERT | BTR_ESTIMATE);

  ut_ad(!insert_planned || (mode == PAGE_CUR_LE));

  cursor->flag = BTR_CUR_BINARY;
  cursor->m_index = dict_index;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched leaf node(s) */

  savepoint = mtr_set_savepoint(mtr);

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(dict_index_get_lock(dict_index), mtr);

  } else if (latch_mode == BTR_CONT_MODIFY_TREE) {
    /* Do nothing */
    ut_ad(mtr_memo_contains(mtr, dict_index_get_lock(dict_index), MTR_MEMO_X_LOCK));
  } else {
    mtr_s_lock(dict_index_get_lock(dict_index), mtr);
  }

  page_cursor = btr_cur_get_page_cur(cursor);

  space = dict_index_get_space(dict_index);
  page_no = dict_index_get_page(dict_index);

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
    buf_block_t *block;
    ulint rw_latch;
    ulint buf_mode;

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
      .m_file = file,
      .m_line = line,
      .m_mtr = mtr
    };

    block = buf_pool->get(req, nullptr);

    if (block == nullptr) {

      ut_ad(cursor->thr);
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

    ut_ad(dict_index->id == btr_page_get_index_id(page));

    if (unlikely(height == ULINT_UNDEFINED)) {
      /* We are in the root node */

      height = btr_page_get_level(page, mtr);
      root_height = height;
      cursor->tree_height = root_height + 1;
    }

    if (height == 0) {
      if (rw_latch == RW_NO_LATCH) {

        btr_cur_latch_leaves(page, space, page_no, latch_mode, cursor, mtr);
      }

      if ((latch_mode != BTR_MODIFY_TREE) && (latch_mode != BTR_CONT_MODIFY_TREE)) {

        /* Release the tree s-latch */

        mtr_release_s_latch_at_savepoint(mtr, savepoint, dict_index_get_lock(dict_index));
      }

      page_mode = mode;
    }

    page_cur_search_with_match(block, dict_index, tuple, page_mode, &up_match, &up_bytes, &low_match, &low_bytes, page_cursor);

    if (estimate) {
      btr_cur_add_path_info(cursor, height, root_height);
    }

    /* If this is the desired level, leave the loop */

    ut_ad(height == btr_page_get_level(page_cur_get_page(page_cursor), mtr));

    if (level == height) {

      if (level > 0) {
        /* x-latch the page */
        page = btr_page_get(space, page_no, RW_X_LATCH, mtr);

        ut_a((bool)!!page_is_comp(page) == dict_table_is_comp(dict_index->table));
      }

      break;
    }

    ut_ad(height > 0);

    height--;

    node_ptr = page_cur_get_rec(page_cursor);
    offsets = rec_get_offsets(node_ptr, cursor->m_index, offsets, ULINT_UNDEFINED, &heap);
    /* Go to the child node */
    page_no = btr_node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (level == 0) {
    cursor->low_match = low_match;
    cursor->low_bytes = low_bytes;
    cursor->up_match = up_match;
    cursor->up_bytes = up_bytes;

    ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_GE);
    ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
    ut_ad(cursor->low_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
  }
}

void btr_cur_open_at_index_side_func(
  bool from_left,
  dict_index_t *dict_index,
  ulint latch_mode,
  btr_cur_t *cursor,
  ulint level,
  const char *file,
  ulint line,
  mtr_t *mtr
)
{
  ulint root_height{};
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_a(level != ULINT_UNDEFINED);

  auto estimate = latch_mode & BTR_ESTIMATE;

  latch_mode = latch_mode & ~BTR_ESTIMATE;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched the leaf node */

  auto savepoint = mtr_set_savepoint(mtr);

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(dict_index_get_lock(dict_index), mtr);
  } else {
    mtr_s_lock(dict_index_get_lock(dict_index), mtr);
  }

  auto page_cursor = btr_cur_get_page_cur(cursor);
  cursor->m_index = dict_index;

  auto space = dict_index_get_space(dict_index);
  auto page_no = dict_index_get_page(dict_index);

  auto height = ULINT_UNDEFINED;

  for (;;) {
  
    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { space, page_no },
      .m_mode = BUF_GET,
      .m_file = file,
      .m_line = line,
      .m_mtr = mtr
    };

    auto block = buf_pool->get(req, nullptr);
    auto page = block->get_frame();

    ut_ad(dict_index->id == btr_page_get_index_id(page));

    block->m_check_index_page_at_flush = true;

    if (height == ULINT_UNDEFINED) {
      /* We are in the root node */

      height = btr_page_get_level(page, mtr);
      root_height = height;
    }

    if (height == 0) {
      btr_cur_latch_leaves(page, space, page_no, latch_mode, cursor, mtr);

      if (latch_mode != BTR_MODIFY_TREE && latch_mode != BTR_CONT_MODIFY_TREE) {

        /* Release the tree s-latch */

        mtr_release_s_latch_at_savepoint(mtr, savepoint, dict_index_get_lock(dict_index));
      }
    }

    if (from_left) {
      page_cur_set_before_first(block, page_cursor);
    } else {
      page_cur_set_after_last(block, page_cursor);
    }

    if (height == 0 || height == level) {
      if (estimate) {
        btr_cur_add_path_info(cursor, height, root_height);
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
      btr_cur_add_path_info(cursor, height, root_height);
    }

    --height;

    auto node_ptr = page_cur_get_rec(page_cursor);

    offsets = rec_get_offsets(node_ptr, cursor->m_index, offsets, ULINT_UNDEFINED, &heap);

    /* Go to the child node */
    page_no = btr_node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

void btr_cur_open_at_rnd_pos_func(dict_index_t *dict_index, ulint latch_mode, btr_cur_t *cursor, const char *file, ulint line, mtr_t *mtr) {
  rec_t *node_ptr;
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  if (latch_mode == BTR_MODIFY_TREE) {
    mtr_x_lock(dict_index_get_lock(dict_index), mtr);
  } else {
    mtr_s_lock(dict_index_get_lock(dict_index), mtr);
  }

  auto page_cursor = btr_cur_get_page_cur(cursor);

  cursor->m_index = dict_index;

  auto space = dict_index_get_space(dict_index);
  auto page_no = dict_index_get_page(dict_index);

  auto height = ULINT_UNDEFINED;

  for (;;) {

    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { space, page_no },
      .m_mode = BUF_GET,
      .m_file = file,
      .m_line = line,
      .m_mtr = mtr
    };

    auto block = buf_pool->get(req, nullptr);
    auto page = block->get_frame();

    ut_ad(dict_index->id == btr_page_get_index_id(page));

    if (height == ULINT_UNDEFINED) {
      /* We are in the root node */

      height = btr_page_get_level(page, mtr);
    }

    if (height == 0) {
      btr_cur_latch_leaves(page, space, page_no, latch_mode, cursor, mtr);
    }

    page_cur_open_on_rnd_user_rec(block, page_cursor);

    if (height == 0) {

      break;
    }

    ut_ad(height > 0);

    height--;

    node_ptr = page_cur_get_rec(page_cursor);
    offsets = rec_get_offsets(node_ptr, cursor->m_index, offsets, ULINT_UNDEFINED, &heap);
    /* Go to the child node */
    page_no = btr_node_ptr_get_child_page_no(node_ptr, offsets);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

/** Inserts a record if there is enough space, or if enough space can
be freed by reorganizing. Differs from btr_cur_optimistic_insert because
no heuristics is applied to whether it pays to use CPU time for
reorganizing the page or not.
@return	pointer to inserted record if succeed, else nullptr */
static rec_t *btr_cur_insert_if_possible(
  btr_cur_t *cursor,     /*!< in: cursor on page after which to insert;
                           cursor stays valid */
  const dtuple_t *tuple, /*!< in: tuple to insert; the size info need not
                           have been stored to tuple */
  ulint n_ext,           /*!< in: number of externally stored columns */
  mtr_t *mtr
) /*!< in: mtr */
{
  page_cur_t *page_cursor;
  buf_block_t *block;
  rec_t *rec;

  ut_ad(dtuple_check_typed(tuple));

  block = btr_cur_get_block(cursor);

  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));
  page_cursor = btr_cur_get_page_cur(cursor);

  /* Now, try the insert */
  rec = page_cur_tuple_insert(page_cursor, tuple, cursor->m_index, n_ext, mtr);

  if (unlikely(!rec)) {
    /* If record did not fit, reorganize */

    if (btr_page_reorganize(block, cursor->m_index, mtr)) {

      page_cur_search(block, cursor->m_index, tuple, PAGE_CUR_LE, page_cursor);

      rec = page_cur_tuple_insert(page_cursor, tuple, cursor->m_index, n_ext, mtr);
    }
  }

  return rec;
}

/**
 * For an insert, checks the locks and does the undo logging if desired.
 *
 * @param flags   Undo logging and locking flags: if not zero, the parameters index and thr should be specified.
 * @param cursor  Cursor on page after which to insert.
 * @param entry   Entry to insert.
 * @param thr     Query thread or nullptr.
 * @param mtr     Mini-transaction.
 * @param inherit True if the inserted new record maybe should inherit LOCK_GAP type locks from the successor record.
 *
 * @return DB_SUCCESS, DB_WAIT_LOCK, DB_FAIL, or error number.
 */
inline db_err btr_cur_ins_lock_and_undo(
  ulint flags,
  btr_cur_t *cursor,
  const dtuple_t *entry,
  que_thr_t *thr,
  mtr_t *mtr,
  bool *inherit
) {
  dict_index_t *dict_index;
  db_err err;
  rec_t *rec;
  roll_ptr_t roll_ptr;

  /* Check if we have to wait for a lock: enqueue an explicit lock
  request if yes */

  rec = btr_cur_get_rec(cursor);
  dict_index = cursor->m_index;

  err = lock_rec_insert_check_and_lock(flags, rec, btr_cur_get_block(cursor), dict_index, thr, mtr, inherit);

  if (err != DB_SUCCESS) {

    return err;
  }

  if (dict_index_is_clust(dict_index)) {

    err = trx_undo_report_row_operation(flags, TRX_UNDO_INSERT_OP, thr, dict_index, entry, nullptr, 0, nullptr, &roll_ptr);

    if (err != DB_SUCCESS) {

      return err;
    }

    /* Now we can fill in the roll ptr field in entry */

    if (!(flags & BTR_KEEP_SYS_FLAG)) {

      row_upd_index_entry_sys_field(entry, dict_index, DATA_ROLL_PTR, roll_ptr);
    }
  }

  return DB_SUCCESS;
}

#ifdef UNIV_DEBUG
/**
 * Report information about a transaction.
 *
 * @param trx   The transaction.
 * @param index The index.
 * @param op    The operation.
 */
static void btr_cur_trx_report(trx_t *trx, const dict_index_t *index, const char *op) {
  ib_logger(ib_stream, "Trx with id %lu going to ", TRX_ID_PREP_PRINTF(trx->id));
  ib_logger(ib_stream, "%s", op);
  dict_index_name_print(ib_stream, trx, index);
  ib_logger(ib_stream, "\n");
}
#endif /* UNIV_DEBUG */

db_err btr_cur_optimistic_insert(
  ulint flags, btr_cur_t *cursor, dtuple_t *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr
) {
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

  auto block = btr_cur_get_block(cursor);
  auto page = block->get_frame();
  auto dict_index = cursor->m_index;

  if (!dtuple_check_typed_no_assert(entry)) {
    ib_logger(ib_stream, "Error in a tuple to insert into ");
    dict_index_name_print(ib_stream, thr_get_trx(thr), dict_index);
  }

#ifdef UNIV_DEBUG
  if (btr_cur_print_record_ops && thr) {
    btr_cur_trx_report(thr_get_trx(thr), dict_index, "insert into ");
    dtuple_print(ib_stream, entry);
  }
#endif /* UNIV_DEBUG */

  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));
  max_size = page_get_max_insert_size_after_reorganize(page, 1);
  leaf = page_is_leaf(page);

  /* Calculate the record size when entry is converted to a record */
  rec_size = rec_get_converted_size(dict_index, entry, n_ext);

  if (page_rec_needs_ext(rec_size, page_is_comp(page))) {
    /* The record is so big that we have to store some fields
    externally on separate database pages */
    big_rec_vec = dtuple_convert_big_rec(dict_index, entry, &n_ext);

    if (unlikely(big_rec_vec == nullptr)) {

      return DB_TOO_BIG_RECORD;
    }

    rec_size = rec_get_converted_size(dict_index, entry, n_ext);
  }

  /* If there have been many consecutive inserts, and we are on the leaf
  level, check if we have to split the page to reserve enough free space
  for future updates of records. */

  if (dict_index_is_clust(dict_index) && page_get_n_recs(page) >= 2 && likely(leaf) && dict_index_get_space_reserve() + rec_size > max_size && (btr_page_get_split_rec_to_right(cursor, &dummy_rec) || btr_page_get_split_rec_to_left(cursor, &dummy_rec))) {

    if (big_rec_vec) {
      dtuple_convert_back_big_rec(dict_index, entry, big_rec_vec);
    }

    return DB_FAIL;
  }

  if (unlikely(max_size < BTR_CUR_PAGE_REORGANIZE_LIMIT || max_size < rec_size) && likely(page_get_n_recs(page) > 1) && page_get_max_insert_size(page, 1) < rec_size) {

    if (big_rec_vec) {
      dtuple_convert_back_big_rec(dict_index, entry, big_rec_vec);
    }

    return DB_FAIL;
  }

  /* Check locks and write to the undo log, if specified */
  err = btr_cur_ins_lock_and_undo(flags, cursor, entry, thr, mtr, &inherit);

  if (unlikely(err != DB_SUCCESS)) {

    if (big_rec_vec) {
      dtuple_convert_back_big_rec(dict_index, entry, big_rec_vec);
    }

    return err;
  }

  page_cursor = btr_cur_get_page_cur(cursor);

  /* Now, try the insert */

  {
    const auto page_cursor_rec = page_cur_get_rec(page_cursor);

    *rec = page_cur_tuple_insert(page_cursor, entry, dict_index, n_ext, mtr);

    reorg = page_cursor_rec != page_cur_get_rec(page_cursor);

    ut_a(!reorg);
  }

  if (unlikely(!*rec) && likely(!reorg)) {
    /* If the record did not fit, reorganize */
    auto success = btr_page_reorganize(block, dict_index, mtr);
    ut_a(success);

    ut_ad(page_get_max_insert_size(page, 1) == max_size);

    reorg = true;

    page_cur_search(block, dict_index, entry, PAGE_CUR_LE, page_cursor);

    *rec = page_cur_tuple_insert(page_cursor, entry, dict_index, n_ext, mtr);

    if (unlikely(!*rec)) {
      ib_logger(ib_stream, "Error: cannot insert tuple ");
      dtuple_print(ib_stream, entry);
      ib_logger(ib_stream, " into ");
      dict_index_name_print(ib_stream, thr_get_trx(thr), dict_index);
      ib_logger(ib_stream, "\nmax insert size %lu\n", (ulong)max_size);
      ut_error;
    }
  }

  if (!(flags & BTR_NO_LOCKING_FLAG) && inherit) {

    lock_update_insert(block, *rec);
  }

  *big_rec = big_rec_vec;

  return DB_SUCCESS;
}

db_err btr_cur_pessimistic_insert(
  ulint flags, btr_cur_t *cursor, dtuple_t *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr
) {
  dict_index_t *dict_index = cursor->m_index;
  big_rec_t *big_rec_vec = nullptr;
  mem_heap_t *heap = nullptr;
  db_err err;
  bool dummy_inh;
  bool success;
  ulint n_extents = 0;
  ulint n_reserved;

  ut_ad(dtuple_check_typed(entry));

  *big_rec = nullptr;

  ut_ad(mtr_memo_contains(mtr, dict_index_get_lock(btr_cur_get_index(cursor)), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains(mtr, btr_cur_get_block(cursor), MTR_MEMO_PAGE_X_FIX));

  /* Try first an optimistic insert; reset the cursor flag: we do not
  assume anything of how it was positioned */

  cursor->flag = BTR_CUR_BINARY;

  err = btr_cur_optimistic_insert(flags, cursor, entry, rec, big_rec, n_ext, thr, mtr);
  if (err != DB_FAIL) {

    return err;
  }

  /* Retry with a pessimistic insert. Check locks and write to undo log,
  if specified */

  err = btr_cur_ins_lock_and_undo(flags, cursor, entry, thr, mtr, &dummy_inh);

  if (err != DB_SUCCESS) {

    return err;
  }

  if (!(flags & BTR_NO_UNDO_LOG_FLAG)) {
    /* First reserve enough free space for the file segments
    of the index tree, so that the insert will not fail because
    of lack of space */

    n_extents = cursor->tree_height / 16 + 3;

    success = fsp_reserve_free_extents(&n_reserved, dict_index->space, n_extents, FSP_NORMAL, mtr);
    if (!success) {
      return DB_OUT_OF_FILE_SPACE;
    }
  }

  if (page_rec_needs_ext(rec_get_converted_size(dict_index, entry, n_ext), dict_table_is_comp(dict_index->table))) {

    /* The record is so big that we have to store some fields
    externally on separate database pages */

    if (likely_null(big_rec_vec)) {
      /* This should never happen, but we handle
      the situation in a robust manner. */
      ut_ad(0);
      dtuple_convert_back_big_rec(dict_index, entry, big_rec_vec);
    }

    big_rec_vec = dtuple_convert_big_rec(dict_index, entry, &n_ext);

    if (big_rec_vec == nullptr) {

      if (n_extents > 0) {
        fil_space_release_free_extents(dict_index->space, n_reserved);
      }
      return DB_TOO_BIG_RECORD;
    }
  }

  if (dict_index_get_page(dict_index) == btr_cur_get_block(cursor)->get_page_no()) {

    /* The page is the root page */
    *rec = btr_root_raise_and_insert(cursor, entry, n_ext, mtr);
  } else {
    *rec = btr_page_split_and_insert(cursor, entry, n_ext, mtr);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  ut_ad(page_rec_get_next(btr_cur_get_rec(cursor)) == *rec);

  if (!(flags & BTR_NO_LOCKING_FLAG)) {

    lock_update_insert(btr_cur_get_block(cursor), *rec);
  }

  if (n_extents > 0) {
    fil_space_release_free_extents(dict_index->space, n_reserved);
  }

  *big_rec = big_rec_vec;

  return DB_SUCCESS;
}

/** For an update, checks the locks and does the undo logging.
@return	DB_SUCCESS, DB_WAIT_LOCK, or error number */
inline db_err btr_cur_upd_lock_and_undo(
  ulint flags,         /*!< in: undo logging and locking flags */
  btr_cur_t *cursor,   /*!< in: cursor on record to update */
  const upd_t *update, /*!< in: update vector */
  ulint cmpl_info,     /*!< in: compiler info on secondary index
                        updates */
  que_thr_t *thr,      /*!< in: query thread */
  mtr_t *mtr,          /*!< in/out: mini-transaction */
  roll_ptr_t *roll_ptr
) /*!< out: roll pointer */
{
  dict_index_t *dict_index;
  rec_t *rec;
  db_err err;

  ut_ad(cursor && update && thr && roll_ptr);

  rec = btr_cur_get_rec(cursor);
  dict_index = cursor->m_index;

  if (!dict_index_is_clust(dict_index)) {
    /* We do undo logging only when we update a clustered index record */
    return lock_sec_rec_modify_check_and_lock(flags, btr_cur_get_block(cursor), rec, dict_index, thr, mtr);
  }

  /* Check if we have to wait for a lock: enqueue an explicit lock request if
   * yes */

  err = DB_SUCCESS;

  if (!(flags & BTR_NO_LOCKING_FLAG)) {
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    rec_offs_init(offsets_);

    err = lock_clust_rec_modify_check_and_lock(
      flags, btr_cur_get_block(cursor), rec, dict_index, rec_get_offsets(rec, dict_index, offsets_, ULINT_UNDEFINED, &heap), thr
    );
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
    if (err != DB_SUCCESS) {

      return err;
    }
  }

  /* Append the info about the update in the undo log */

  err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr, dict_index, nullptr, update, cmpl_info, rec, roll_ptr);

  return err;
}

/** Writes a redo log record of updating a record in-place. */
inline void btr_cur_update_in_place_log(
  ulint flags,              /*!< in: flags */
  rec_t *rec,               /*!< in: record */
  dict_index_t *dict_index, /*!< in: index where cursor positioned */
  const upd_t *update,      /*!< in: update vector */
  trx_t *trx,               /*!< in: transaction */
  roll_ptr_t roll_ptr,      /*!< in: roll ptr */
  mtr_t *mtr
) /*!< in: mtr */
{
  byte *log_ptr;
  page_t *page = page_align(rec);
  ut_ad(flags < 256);
  ut_ad(!!page_is_comp(page) == dict_table_is_comp(dict_index->table));

  log_ptr = mlog_open_and_write_index(
    mtr,
    rec,
    dict_index,
    page_is_comp(page) ? MLOG_COMP_REC_UPDATE_IN_PLACE : MLOG_REC_UPDATE_IN_PLACE,
    1 + DATA_ROLL_PTR_LEN + 14 + 2 + MLOG_BUF_MARGIN
  );

  if (!log_ptr) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  /* The code below assumes index is a clustered index: change index to
  the clustered index if we are updating a secondary index record (or we
  could as well skip writing the sys col values to the log in this case
  because they are not needed for a secondary index record update) */

  dict_index = dict_table_get_first_index(dict_index->table);

  mach_write_to_1(log_ptr, flags);
  log_ptr++;

  log_ptr = row_upd_write_sys_vals_to_log(dict_index, trx, roll_ptr, log_ptr, mtr);
  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  row_upd_index_write_log(update, log_ptr, mtr);
}

byte *btr_cur_parse_update_in_place(byte *ptr, byte *end_ptr, page_t *page, dict_index_t *dict_index) {
  ulint flags;
  rec_t *rec;
  upd_t *update;
  ulint pos;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;
  ulint rec_offset;
  mem_heap_t *heap;
  ulint *offsets;

  if (end_ptr < ptr + 1) {

    return nullptr;
  }

  flags = mach_read_from_1(ptr);
  ptr++;

  ptr = row_upd_parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  heap = mem_heap_create(256);

  ptr = row_upd_index_parse(ptr, end_ptr, heap, &update);

  if (!ptr || !page) {

    goto func_exit;
  }

  ut_a((bool)!!page_is_comp(page) == dict_table_is_comp(dict_index->table));
  rec = page + rec_offset;

  offsets = rec_get_offsets(rec, dict_index, nullptr, ULINT_UNDEFINED, &heap);

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    row_upd_rec_sys_fields_in_recovery(rec, offsets, pos, trx_id, roll_ptr);
  }

  row_upd_rec_in_place(rec, dict_index, offsets, update);

func_exit:
  mem_heap_free(heap);

  return ptr;
}

db_err btr_cur_update_in_place(ulint flags, btr_cur_t *cursor, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) {
  dict_index_t *dict_index;
  buf_block_t *block;
  db_err err;
  rec_t *rec;
  roll_ptr_t roll_ptr{};
  trx_t *trx;
  ulint was_delete_marked;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  rec = btr_cur_get_rec(cursor);
  dict_index = cursor->m_index;
  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(dict_index->table));
  /* The insert buffer tree should never be updated in place. */

  trx = thr_get_trx(thr);
  offsets = rec_get_offsets(rec, dict_index, offsets, ULINT_UNDEFINED, &heap);
#ifdef UNIV_DEBUG
  if (btr_cur_print_record_ops && thr) {
    btr_cur_trx_report(trx, dict_index, "update ");
    rec_print_new(ib_stream, rec, offsets);
  }
#endif /* UNIV_DEBUG */

  block = btr_cur_get_block(cursor);

  /* Do lock checking and undo logging */
  err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, mtr, &roll_ptr);

  if (unlikely(err != DB_SUCCESS)) {

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
    return err;
  }

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    row_upd_rec_sys_fields(rec, dict_index, offsets, trx, roll_ptr);
  }

  was_delete_marked = rec_get_deleted_flag(rec, page_is_comp(block->get_frame()));

  row_upd_rec_in_place(rec, dict_index, offsets, update);

  btr_cur_update_in_place_log(flags, rec, dict_index, update, trx, roll_ptr, mtr);

  if (was_delete_marked && !rec_get_deleted_flag(rec, page_is_comp(block->get_frame()))) {
    /* The new updated record owns its possible externally stored fields */

    btr_cur_unmark_extern_fields(rec, dict_index, offsets, mtr);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return DB_SUCCESS;
}

db_err btr_cur_optimistic_update(ulint flags, btr_cur_t *cursor, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) {
  page_cur_t *page_cursor;
  db_err err;
  ulint max_size;
  ulint new_rec_size;
  ulint old_rec_size;
  roll_ptr_t roll_ptr;
  trx_t *trx;
  ulint i;
  ulint n_ext;

  auto block = btr_cur_get_block(cursor);
  auto page = block->get_frame();
  auto rec = btr_cur_get_rec(cursor);
  auto index = cursor->m_index;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));
  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

  auto heap = mem_heap_create(1024);
  auto offsets = rec_get_offsets(rec, index, nullptr, ULINT_UNDEFINED, &heap);

#ifdef UNIV_DEBUG
  if (btr_cur_print_record_ops && thr) {
    btr_cur_trx_report(thr_get_trx(thr), index, "update ");
    rec_print_new(ib_stream, rec, offsets);
  }
#endif /* UNIV_DEBUG */

  if (!row_upd_changes_field_size_or_external(index, offsets, update)) {

    /* The simplest and the most common case: the update does not
    change the size of any field and none of the updated fields is
    externally stored in rec or update, and there is enough space
    on the compressed page to log the update. */

    mem_heap_free(heap);
    return btr_cur_update_in_place(flags, cursor, update, cmpl_info, thr, mtr);
  }

  if (rec_offs_any_extern(offsets)) {
  any_extern:
    /* Externally stored fields are treated in pessimistic
    update */

    mem_heap_free(heap);
    return DB_OVERFLOW;
  }

  for (i = 0; i < upd_get_n_fields(update); i++) {
    if (dfield_is_ext(&upd_get_nth_field(update, i)->new_val)) {

      goto any_extern;
    }
  }

  page_cursor = btr_cur_get_page_cur(cursor);

  auto new_entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

  /* We checked above that there are no externally stored fields. */
  ut_a(!n_ext);

  /* The page containing the clustered index record
  corresponding to new_entry is latched in mtr.
  Thus the following call is safe. */
  row_upd_index_replace_new_col_vals_index_pos(new_entry, index, update, false, heap);
  old_rec_size = rec_offs_size(offsets);
  new_rec_size = rec_get_converted_size(index, new_entry, 0);

  if (unlikely(new_rec_size >= (page_get_free_space_of_empty(page_is_comp(page)) / 2))) {

    err = DB_OVERFLOW;
    goto err_exit;
  }

  if (unlikely(page_get_data_size(page) - old_rec_size + new_rec_size < BTR_CUR_PAGE_LOW_FILL_LIMIT)) {

    /* The page would become too empty */

    err = DB_UNDERFLOW;
    goto err_exit;
  }

  max_size = old_rec_size + page_get_max_insert_size_after_reorganize(page, 1);

  if (!(((max_size >= BTR_CUR_PAGE_REORGANIZE_LIMIT) && (max_size >= new_rec_size)) || (page_get_n_recs(page) <= 1))) {

    /* There was not enough space, or it did not pay to
    reorganize: for simplicity, we decide what to do assuming a
    reorganization is needed, though it might not be necessary */

    err = DB_OVERFLOW;
    goto err_exit;
  }

  /* Do lock checking and undo logging */
  err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, mtr, &roll_ptr);
  if (err != DB_SUCCESS) {
  err_exit:
    mem_heap_free(heap);
    return err;
  }

  /* Ok, we may do the replacement. Store on the page infimum the
  explicit locks on rec, before deleting rec (see the comment in
  btr_cur_pessimistic_update). */

  lock_rec_store_on_page_infimum(block, rec);

  /* The call to row_rec_to_index_entry(ROW_COPY_DATA, ...) above
  invokes rec_offs_make_valid() to point to the copied record that
  the fields of new_entry point to.  We have to undo it here. */
  ut_ad(rec_offs_validate(nullptr, index, offsets));
  rec_offs_make_valid(page_cur_get_rec(page_cursor), index, offsets);

  page_cur_delete_rec(page_cursor, index, offsets, mtr);

  page_cur_move_to_prev(page_cursor);

  trx = thr_get_trx(thr);

  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    row_upd_index_entry_sys_field(new_entry, index, DATA_ROLL_PTR, roll_ptr);
    row_upd_index_entry_sys_field(new_entry, index, DATA_TRX_ID, trx->id);
  }

  /* There are no externally stored columns in new_entry */
  rec = btr_cur_insert_if_possible(cursor, new_entry, 0 /*n_ext*/, mtr);
  ut_a(rec); /* <- We calculated above the insert would fit */

  /* Restore the old explicit lock state on the record */

  lock_rec_restore_from_page_infimum(block, rec, block);

  page_cur_move_to_next(page_cursor);

  mem_heap_free(heap);

  return DB_SUCCESS;
}

/** If, in a split, a new supremum record was created as the predecessor of the
updated record, the supremum record must inherit exactly the locks on the
updated record. In the split it may have inherited locks from the successor
of the updated record, which is not correct. This function restores the
right locks for the new supremum. */
static void btr_cur_pess_upd_restore_supremum(
  buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,   /*!< in: updated record */
  mtr_t *mtr
) /*!< in: mtr */
{
  auto page = block->get_frame();

  if (page_rec_get_next(page_get_infimum_rec(page)) != rec) {
    /* Updated record is not the first user record on its page */

    return;
  }

  auto space = block->get_space();
  auto prev_page_no = btr_page_get_prev(page, mtr);

  ut_ad(prev_page_no != FIL_NULL);

  Buf_pool::Request req {
    .m_rw_latch = RW_NO_LATCH,
    .m_page_id = { space, prev_page_no },
    .m_mode = BUF_GET_NO_LATCH,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto prev_block = buf_pool->get(req, nullptr);

#ifdef UNIV_BTR_DEBUG
  ut_a(btr_page_get_next(prev_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

  /* We must already have an x-latch on prev_block! */
  ut_ad(mtr_memo_contains(mtr, prev_block, MTR_MEMO_PAGE_X_FIX));

  lock_rec_reset_and_inherit_gap_locks(prev_block, block, PAGE_HEAP_NO_SUPREMUM, page_rec_get_heap_no(rec));
}

db_err btr_cur_pessimistic_update(
  ulint flags, btr_cur_t *cursor, mem_heap_t **heap, big_rec_t **big_rec, const upd_t *update, ulint cmpl_info, que_thr_t *thr,
  mtr_t *mtr
) {
  big_rec_t *big_rec_vec = nullptr;
  big_rec_t *dummy_big_rec;
  page_cur_t *page_cursor;
  dtuple_t *new_entry;
  db_err err;
  roll_ptr_t roll_ptr;
  trx_t *trx;
  bool was_first;
  ulint n_extents = 0;
  ulint n_reserved;
  ulint n_ext;
  ulint *offsets = nullptr;

  *big_rec = nullptr;

  auto block = btr_cur_get_block(cursor);
  auto page = block->get_frame();
  auto rec = btr_cur_get_rec(cursor);
  auto index = cursor->m_index;

  ut_ad(mtr_memo_contains(mtr, dict_index_get_lock(index), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

  auto optim_err = btr_cur_optimistic_update(flags, cursor, update, cmpl_info, thr, mtr);

  switch (optim_err) {
    case DB_UNDERFLOW:
    case DB_OVERFLOW:
      break;
    default:
      return optim_err;
  }

  /* Do lock checking and undo logging */
  err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, mtr, &roll_ptr);
  if (err != DB_SUCCESS) {

    return err;
  }

  if (optim_err == DB_OVERFLOW) {
    ulint reserve_flag;

    /* First reserve enough free space for the file segments
    of the index tree, so that the update will not fail because
    of lack of space */

    n_extents = cursor->tree_height / 16 + 3;

    if (flags & BTR_NO_UNDO_LOG_FLAG) {
      reserve_flag = FSP_CLEANING;
    } else {
      reserve_flag = FSP_NORMAL;
    }

    if (!fsp_reserve_free_extents(&n_reserved, index->space, n_extents, reserve_flag, mtr)) {
      return DB_OUT_OF_FILE_SPACE;
    }
  }

  if (*heap == nullptr) {
    *heap = mem_heap_create(1024);
  }
  offsets = rec_get_offsets(rec, index, nullptr, ULINT_UNDEFINED, heap);

  trx = thr_get_trx(thr);

  new_entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, *heap);
  /* The call to row_rec_to_index_entry(ROW_COPY_DATA, ...) above
  invokes rec_offs_make_valid() to point to the copied record that
  the fields of new_entry point to.  We have to undo it here. */
  ut_ad(rec_offs_validate(nullptr, index, offsets));
  rec_offs_make_valid(rec, index, offsets);

  /* The page containing the clustered index record
  corresponding to new_entry is latched in mtr.  If the
  clustered index record is delete-marked, then its externally
  stored fields cannot have been purged yet, because then the
  purge would also have removed the clustered index record
  itself.  Thus the following call is safe. */
  row_upd_index_replace_new_col_vals_index_pos(new_entry, index, update, false, *heap);
  if (!(flags & BTR_KEEP_SYS_FLAG)) {
    row_upd_index_entry_sys_field(new_entry, index, DATA_ROLL_PTR, roll_ptr);
    row_upd_index_entry_sys_field(new_entry, index, DATA_TRX_ID, trx->id);
  }

  if ((flags & BTR_NO_UNDO_LOG_FLAG) && rec_offs_any_extern(offsets)) {
    /* We are in a transaction rollback undoing a row
    update: we must free possible externally stored fields
    which got new values in the update, if they are not
    inherited values. They can be inherited if we have
    updated the primary key to another value, and then
    update it back again. */

    ut_ad(big_rec_vec == nullptr);

    btr_rec_free_updated_extern_fields(index, rec, offsets, update, trx_is_recv(trx) ? RB_RECOVERY : RB_NORMAL, mtr);
  }

  /* We have to set appropriate extern storage bits in the new
  record to be inserted: we have to remember which fields were such */

  ut_ad(!page_is_comp(page) || !rec_get_node_ptr_flag(rec));
  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, heap);
  n_ext += btr_push_update_extern_fields(new_entry, update, *heap);

  if (page_rec_needs_ext(rec_get_converted_size(index, new_entry, n_ext), page_is_comp(page))) {
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

  lock_rec_store_on_page_infimum(block, rec);

  page_cursor = btr_cur_get_page_cur(cursor);

  page_cur_delete_rec(page_cursor, index, offsets, mtr);

  page_cur_move_to_prev(page_cursor);

  rec = btr_cur_insert_if_possible(cursor, new_entry, n_ext, mtr);

  if (rec != nullptr) {
    lock_rec_restore_from_page_infimum(btr_cur_get_block(cursor), rec, block);

    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, heap);

    if (!rec_get_deleted_flag(rec, rec_offs_comp(offsets))) {
      /* The new inserted record owns its possible externally
      stored fields */
      btr_cur_unmark_extern_fields(rec, index, offsets, mtr);
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

  err = btr_cur_pessimistic_insert(
    BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG, cursor, new_entry, &rec, &dummy_big_rec, n_ext, nullptr, mtr
  );
  ut_a(rec);
  ut_a(err == DB_SUCCESS);
  ut_a(dummy_big_rec == nullptr);

  if (dict_index_is_sec(index)) {
    /* Update PAGE_MAX_TRX_ID in the index page header.
    It was not updated by btr_cur_pessimistic_insert()
    because of BTR_NO_LOCKING_FLAG. */
    auto rec_block = btr_cur_get_block(cursor);

    page_update_max_trx_id(rec_block, trx->id, mtr);
  }

  if (!rec_get_deleted_flag(rec, rec_offs_comp(offsets))) {
    /* The new inserted record owns its possible externally
    stored fields */

    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, heap);
    btr_cur_unmark_extern_fields(rec, index, offsets, mtr);
  }

  lock_rec_restore_from_page_infimum(btr_cur_get_block(cursor), rec, block);

  /* If necessary, restore also the correct lock state for a new,
  preceding supremum record created in a page split. While the old
  record was nonexistent, the supremum might have inherited its locks
  from a wrong record. */

  if (!was_first) {
    btr_cur_pess_upd_restore_supremum(btr_cur_get_block(cursor), rec, mtr);
  }

return_after_reservations:
  if (n_extents > 0) {
    fil_space_release_free_extents(index->space, n_reserved);
  }

  *big_rec = big_rec_vec;

  return err;
}

/** Writes the redo log record for delete marking or unmarking of an index
record. */
inline void btr_cur_del_mark_set_clust_rec_log(
  ulint flags,         /*!< in: flags */
  rec_t *rec,          /*!< in: record */
  dict_index_t *index, /*!< in: index of the record */
  bool val,            /*!< in: value to set */
  trx_t *trx,          /*!< in: deleting transaction */
  roll_ptr_t roll_ptr, /*!< in: roll ptr to the undo log record */
  mtr_t *mtr
) /*!< in: mtr */
{
  ut_ad(flags < 256);

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  auto log_ptr = mlog_open_and_write_index(
    mtr,
    rec,
    index,
    page_rec_is_comp(rec) ? MLOG_COMP_REC_CLUST_DELETE_MARK : MLOG_REC_CLUST_DELETE_MARK,
    1 + 1 + DATA_ROLL_PTR_LEN + 14 + 2
  );

  if (!log_ptr) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  mach_write_to_1(log_ptr, flags);
  log_ptr++;
  mach_write_to_1(log_ptr, (ulint)val);
  log_ptr++;

  log_ptr = row_upd_write_sys_vals_to_log(index, trx, roll_ptr, log_ptr, mtr);
  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

byte *btr_cur_parse_del_mark_set_clust_rec(byte *ptr, byte *end_ptr, page_t *page, dict_index_t *index) {
  ut_ad(!page || !!page_is_comp(page) == dict_table_is_comp(index->table));

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  auto flags = mach_read_from_1(ptr);
  ptr++;

  auto val = mach_read_from_1(ptr);
  ptr++;

  ulint pos;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;

  ptr = row_upd_parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  auto offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page) {
    auto rec = page + offset;

    btr_rec_set_deleted_flag(rec, val);

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      mem_heap_t *heap = nullptr;
      ulint offsets_[REC_OFFS_NORMAL_SIZE];
      rec_offs_init(offsets_);

      row_upd_rec_sys_fields_in_recovery(rec, rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &heap), pos, trx_id, roll_ptr);

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }
    }
  }

  return ptr;
}

db_err btr_cur_del_mark_set_clust_rec(ulint flags, btr_cur_t *cursor, bool val, que_thr_t *thr, mtr_t *mtr) {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  auto rec = btr_cur_get_rec(cursor);
  auto index = cursor->m_index;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  mem_heap_t *heap{};
  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

#ifdef UNIV_DEBUG
  if (btr_cur_print_record_ops && thr) {
    btr_cur_trx_report(thr_get_trx(thr), index, "del mark ");
    rec_print_new(ib_stream, rec, offsets);
  }
#endif /* UNIV_DEBUG */

  ut_ad(dict_index_is_clust(index));
  ut_ad(!rec_get_deleted_flag(rec, rec_offs_comp(offsets)));

  auto err = lock_clust_rec_modify_check_and_lock(flags, btr_cur_get_block(cursor), rec, index, offsets, thr);

  if (err == DB_SUCCESS) {
    roll_ptr_t roll_ptr;

    err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr, index, nullptr, nullptr, 0, rec, &roll_ptr);

    if (err == DB_SUCCESS) {
      btr_rec_set_deleted_flag(rec, val);

      auto trx = thr_get_trx(thr);

      if (!(flags & BTR_KEEP_SYS_FLAG)) {
        row_upd_rec_sys_fields(rec, index, offsets, trx, roll_ptr);
      }

      btr_cur_del_mark_set_clust_rec_log(flags, rec, index, val, trx, roll_ptr, mtr);
    }
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return err;
}

/** Writes the redo log record for a delete mark setting of a secondary
index record. */
inline void btr_cur_del_mark_set_sec_rec_log(
  rec_t *rec, /*!< in: record */
  bool val,   /*!< in: value to set */
  mtr_t *mtr
) /*!< in: mtr */
{
  auto log_ptr = mlog_open(mtr, 11 + 1 + 2);

  if (!log_ptr) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns nullptr */
    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_DELETE_MARK, log_ptr, mtr);
  mach_write_to_1(log_ptr, (ulint)val);
  log_ptr++;

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

byte *btr_cur_parse_del_mark_set_sec_rec(byte *ptr, byte *end_ptr, page_t *page) {
  if (end_ptr < ptr + 3) {

    return nullptr;
  }

  auto val = mach_read_from_1(ptr);
  ptr++;

  auto offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page != nullptr) {
    auto rec = page + offset;

    btr_rec_set_deleted_flag(rec, val);
  }

  return ptr;
}

db_err btr_cur_del_mark_set_sec_rec(ulint flags, btr_cur_t *cursor, bool val, que_thr_t *thr, mtr_t *mtr) {
  auto rec = btr_cur_get_rec(cursor);

#ifdef UNIV_DEBUG
  if (btr_cur_print_record_ops && thr) {
    btr_cur_trx_report(thr_get_trx(thr), cursor->m_index, "del mark ");
    rec_print(ib_stream, rec, cursor->m_index);
  }
#endif /* UNIV_DEBUG */

  auto err = lock_sec_rec_modify_check_and_lock(flags, btr_cur_get_block(cursor), rec, cursor->m_index, thr, mtr);

  if (err != DB_SUCCESS) {

    return err;
  }

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(cursor->m_index->table));

  btr_rec_set_deleted_flag(rec, val);

  btr_cur_del_mark_set_sec_rec_log(rec, val, mtr);

  return DB_SUCCESS;
}

bool btr_cur_optimistic_delete(btr_cur_t *cursor, mtr_t *mtr) {
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(mtr_memo_contains(mtr, btr_cur_get_block(cursor), MTR_MEMO_PAGE_X_FIX));

  /* This is intended only for leaf page deletions */
  auto block = btr_cur_get_block(cursor);

  ut_ad(page_is_leaf(block->get_frame()));

  auto rec = btr_cur_get_rec(cursor);

  offsets = rec_get_offsets(rec, cursor->m_index, offsets, ULINT_UNDEFINED, &heap);

  bool deleted{};

  if (!rec_offs_any_extern(offsets) && btr_cur_delete_will_underflow(cursor, rec_offs_size(offsets), mtr)) {
    auto page = block->get_frame();

    lock_update_delete(block, rec);

    page_get_max_insert_size_after_reorganize(page, 1);

    page_cur_delete_rec(btr_cur_get_page_cur(cursor), cursor->m_index, offsets, mtr);

    deleted = true;
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return deleted;
}

void btr_cur_pessimistic_delete(db_err *err, bool has_reserved_extents, btr_cur_t *cursor, trx_rb_ctx rb_ctx, mtr_t *mtr) {
  ulint n_extents{};
  ulint n_reserved{};

  auto block = btr_cur_get_block(cursor);
  auto page = block->get_frame();
  auto index = btr_cur_get_index(cursor);

  ut_ad(mtr_memo_contains(mtr, dict_index_get_lock(index), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

  if (!has_reserved_extents) {
    /* First reserve enough free space for the file segments
    of the index tree, so that the node pointer updates will
    not fail because of lack of space */

    n_extents = cursor->tree_height / 32 + 1;

    auto success = fsp_reserve_free_extents(&n_reserved, index->space, n_extents, FSP_CLEANING, mtr);

    if (!success) {
      *err = DB_OUT_OF_FILE_SPACE;
    }
  }

  auto heap = mem_heap_create(1024);
  auto rec = btr_cur_get_rec(cursor);
  auto offsets = rec_get_offsets(rec, index, nullptr, ULINT_UNDEFINED, &heap);

  if (rec_offs_any_extern(offsets)) {
    btr_rec_free_externally_stored_fields(index, rec, offsets, rb_ctx, mtr);
  }

  if (page_get_n_recs(page) < 2 && dict_index_get_page(index) != block->get_page_no()) {

    /* If there is only one record, drop the whole page in
    btr_discard_page, if this is not the root page */

    btr_discard_page(cursor, mtr);

    *err = DB_SUCCESS;

  } else {
    lock_update_delete(block, rec);

    const auto level = btr_page_get_level(page, mtr);

    if (level > 0 && rec == page_rec_get_next(page_get_infimum_rec(page))) {

      auto next_rec = page_rec_get_next(rec);

      if (btr_page_get_prev(page, mtr) == FIL_NULL) {

        /* If we delete the leftmost node pointer on a
        non-leaf level, we must mark the new leftmost node
        pointer as the predefined minimum record */

        btr_set_min_rec_mark(next_rec, mtr);

      } else {

        /* Otherwise, if we delete the leftmost node pointer
        on a page, we have to change the father node pointer
        so that it is equal to the new leftmost node pointer
        on the page */

        btr_node_ptr_delete(index, block, mtr);

        auto node_ptr = dict_index_build_node_ptr(index, next_rec, block->get_page_no(), heap, level);

        btr_insert_on_non_leaf_level(index, level + 1, node_ptr, mtr);
      }
    }

    page_cur_delete_rec(btr_cur_get_page_cur(cursor), index, offsets, mtr);

    ut_ad(btr_check_node_ptr(index, block, mtr));

    *err = DB_SUCCESS;
  }

  mem_heap_free(heap);

  if (n_extents > 0) {
    fil_space_release_free_extents(index->space, n_reserved);
  }
}

/** Adds path information to the cursor for the current page, for which
the binary search has been performed. */
static void btr_cur_add_path_info(
  btr_cur_t *cursor, /*!< in: cursor positioned on a page */
  ulint height,      /*!< in: height of the page in tree;
                                         0 means leaf node */
  ulint root_height
) /*!< in: root node height in tree */
{
  btr_path_t *slot;
  rec_t *rec;

  ut_a(cursor->path_arr);

  if (root_height >= BTR_PATH_ARRAY_N_SLOTS - 1) {
    /* Do nothing; return empty path */

    slot = cursor->path_arr;
    slot->nth_rec = ULINT_UNDEFINED;

    return;
  }

  if (height == 0) {
    /* Mark end of slots for path */
    slot = cursor->path_arr + root_height + 1;
    slot->nth_rec = ULINT_UNDEFINED;
  }

  rec = btr_cur_get_rec(cursor);

  slot = cursor->path_arr + (root_height - height);

  slot->nth_rec = page_rec_get_n_recs_before(rec);
  slot->n_recs = page_get_n_recs(page_align(rec));
}

/** Estimates the number of rows in a given index range.
@return	estimated number of rows */

int64_t btr_estimate_n_rows_in_range(
  dict_index_t *index,    /*!< in: index */
  const dtuple_t *tuple1, /*!< in: range start, may also be empty tuple */
  ulint mode1,            /*!< in: search mode for range start */
  const dtuple_t *tuple2, /*!< in: range end, may also be empty tuple */
  ulint mode2
) /*!< in: search mode for range end */
{
  btr_path_t path1[BTR_PATH_ARRAY_N_SLOTS];
  btr_path_t path2[BTR_PATH_ARRAY_N_SLOTS];
  btr_cur_t cursor;
  btr_path_t *slot1;
  btr_path_t *slot2;
  bool diverged;
  bool diverged_lot;
  ulint divergence_level;
  int64_t n_rows;
  ulint i;
  mtr_t mtr;

  mtr_start(&mtr);

  cursor.path_arr = path1;

  if (dtuple_get_n_fields(tuple1) > 0) {

    btr_cur_search_to_nth_level(index, 0, tuple1, mode1, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, __FILE__, __LINE__, &mtr);
  } else {
    btr_cur_open_at_index_side(true, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, &mtr);
  }

  mtr_commit(&mtr);

  mtr_start(&mtr);

  cursor.path_arr = path2;

  if (dtuple_get_n_fields(tuple2) > 0) {

    btr_cur_search_to_nth_level(index, 0, tuple2, mode2, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, __FILE__, __LINE__, &mtr);
  } else {
    btr_cur_open_at_index_side(false, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, &mtr);
  }

  mtr_commit(&mtr);

  /* We have the path information for the range in path1 and path2 */

  n_rows = 1;
  diverged = false;           /* This becomes true when the path is not
                              the same any more */
  diverged_lot = false;       /* This becomes true when the paths are
                              not the same or adjacent any more */
  divergence_level = 1000000; /* This is the level where paths diverged
                              a lot */
  for (i = 0;; i++) {
    ut_ad(i < BTR_PATH_ARRAY_N_SLOTS);

    slot1 = path1 + i;
    slot2 = path2 + i;

    if (slot1->nth_rec == ULINT_UNDEFINED || slot2->nth_rec == ULINT_UNDEFINED) {

      if (i > divergence_level + 1) {
        /* In trees whose height is > 1 our algorithm
        tends to underestimate: multiply the estimate
        by 2: */

        n_rows = n_rows * 2;
      }

      /* Do not estimate the number of rows in the range
      to over 1 / 2 of the estimated rows in the whole
      table */

      if (n_rows > index->table->stat_n_rows / 2) {
        n_rows = index->table->stat_n_rows / 2;

        /* If there are just 0 or 1 rows in the table,
        then we estimate all rows are in the range */

        if (n_rows == 0) {
          n_rows = index->table->stat_n_rows;
        }
      }

      return n_rows;
    }

    if (!diverged && slot1->nth_rec != slot2->nth_rec) {

      diverged = true;

      if (slot1->nth_rec < slot2->nth_rec) {
        n_rows = slot2->nth_rec - slot1->nth_rec;

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

      if (slot1->nth_rec < slot1->n_recs || slot2->nth_rec > 1) {

        diverged_lot = true;
        divergence_level = i;

        n_rows = 0;

        if (slot1->nth_rec < slot1->n_recs) {
          n_rows += slot1->n_recs - slot1->nth_rec;
        }

        if (slot2->nth_rec > 1) {
          n_rows += slot2->nth_rec - 1;
        }
      }
    } else if (diverged_lot) {

      n_rows = (n_rows * (slot1->n_recs + slot2->n_recs)) / 2;
    }
  }
}

/** Estimates the number of different key values in a given index, for
each n-column prefix of the index where n <= dict_index_get_n_unique(index).
The estimates are stored in the array index->stat_n_diff_key_vals. */

void btr_estimate_number_of_different_key_vals(dict_index_t *index) /*!< in: index */
{
  btr_cur_t cursor;
  page_t *page;
  rec_t *rec;
  ulint n_cols;
  ulint matched_fields;
  ulint matched_bytes;
  int64_t *n_diff;
  uint64_t n_sample_pages; /* number of pages to sample */
  ulint not_empty_flag = 0;
  ulint total_external_size = 0;
  ulint i;
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

  n_cols = dict_index_get_n_unique(index);

  n_diff = (int64_t *)mem_zalloc((n_cols + 1) * sizeof(int64_t));

  /* It makes no sense to test more pages than are contained
  in the index, thus we lower the number if it is too high */
  if (srv_stats_sample_pages > index->stat_index_size) {
    if (index->stat_index_size > 0) {
      n_sample_pages = index->stat_index_size;
    } else {
      n_sample_pages = 1;
    }
  } else {
    n_sample_pages = srv_stats_sample_pages;
  }

  /* We sample some pages in the index to get an estimate */

  for (i = 0; i < n_sample_pages; i++) {
    rec_t *supremum;
    mtr_start(&mtr);

    btr_cur_open_at_rnd_pos(index, BTR_SEARCH_LEAF, &cursor, &mtr);

    /* Count the number of different key values for each prefix of
    the key on this index page. If the prefix does not determine
    the index record uniquely in the B-tree, then we subtract one
    because otherwise our algorithm would give a wrong estimate
    for an index where there is just one key value. */

    page = btr_cur_get_page(&cursor);

    supremum = page_get_supremum_rec(page);
    rec = page_rec_get_next(page_get_infimum_rec(page));

    if (rec != supremum) {
      not_empty_flag = 1;
      offsets_rec = rec_get_offsets(rec, index, offsets_rec, ULINT_UNDEFINED, &heap);
    }

    while (rec != supremum) {
      rec_t *next_rec = page_rec_get_next(rec);
      if (next_rec == supremum) {
        break;
      }

      matched_fields = 0;
      matched_bytes = 0;
      offsets_next_rec = rec_get_offsets(next_rec, index, offsets_next_rec, n_cols, &heap);

      cmp_rec_rec_with_match(rec, next_rec, offsets_rec, offsets_next_rec, index, &matched_fields, &matched_bytes);

      for (j = matched_fields + 1; j <= n_cols; j++) {
        /* We add one if this index record has
        a different prefix from the previous */

        n_diff[j]++;
      }

      total_external_size += btr_rec_get_externally_stored_len(rec, offsets_rec);

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

    if (n_cols == dict_index_get_n_unique_in_tree(index)) {

      /* If there is more than one leaf page in the tree,
      we add one because we know that the first record
      on the page certainly had a different prefix than the
      last record on the previous index page in the
      alphabetical order. Before this fix, if there was
      just one big record on each clustered index page, the
      algorithm grossly underestimated the number of rows
      in the table. */

      if (btr_page_get_prev(page, &mtr) != FIL_NULL || btr_page_get_next(page, &mtr) != FIL_NULL) {

        n_diff[n_cols]++;
      }
    }

    offsets_rec = rec_get_offsets(rec, index, offsets_rec, ULINT_UNDEFINED, &heap);
    total_external_size += btr_rec_get_externally_stored_len(rec, offsets_rec);
    mtr_commit(&mtr);
  }

  /* If we saw k borders between different key values on
  n_sample_pages leaf pages, we can estimate how many
  there will be in index->stat_n_leaf_pages */

  /* We must take into account that our sample actually represents
  also the pages used for external storage of fields (those pages are
  included in index->stat_n_leaf_pages) */

  dict_index_stat_mutex_enter(index);

  for (j = 0; j <= n_cols; j++) {
    index->stat_n_diff_key_vals[j] =
      ((n_diff[j] * (int64_t)index->stat_n_leaf_pages + n_sample_pages - 1 + total_external_size + not_empty_flag) /
       (n_sample_pages + total_external_size));

    /* If the tree is small, smaller than
    10 * n_sample_pages + total_external_size, then
    the above estimate is ok. For bigger trees it is common that we
    do not see any borders between key values in the few pages
    we pick. But still there may be n_sample_pages
    different key values, or even more. Let us try to approximate
    that: */

    add_on = index->stat_n_leaf_pages / (10 * (n_sample_pages + total_external_size));

    if (add_on > n_sample_pages) {
      add_on = n_sample_pages;
    }

    index->stat_n_diff_key_vals[j] += add_on;
  }

  dict_index_stat_mutex_exit(index);

  mem_free(n_diff);
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

/** Gets the externally stored size of a record, in units of a database page.
@return	externally stored part, in units of a database page */
static ulint btr_rec_get_externally_stored_len(
  rec_t *rec, /*!< in: record */
  const ulint *offsets
) /*!< in: array returned by rec_get_offsets() */
{
  ulint n_fields;
  ulint local_len;
  ulint extern_len;
  ulint total_extern_len = 0;
  ulint i;

  ut_ad(!rec_offs_comp(offsets) || !rec_get_node_ptr_flag(rec));
  n_fields = rec_offs_n_fields(offsets);

  for (i = 0; i < n_fields; i++) {
    if (rec_offs_nth_extern(offsets, i)) {

      auto data = (byte *)rec_get_nth_field(rec, offsets, i, &local_len);

      local_len -= BTR_EXTERN_FIELD_REF_SIZE;

      extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);

      total_extern_len += ut_calc_align(extern_len, UNIV_PAGE_SIZE);
    }
  }

  return total_extern_len / UNIV_PAGE_SIZE;
}

/** Sets the ownership bit of an externally stored field in a record. */
static void btr_cur_set_ownership_of_extern_field(
  rec_t *rec,           /*!< in/out: clustered index record */
  dict_index_t *index,  /*!< in: index of the page */
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint i,              /*!< in: field number */
  bool val,             /*!< in: value to set */
  mtr_t *mtr
) /*!< in: mtr, or nullptr if not logged */
{
  ulint local_len;

  auto data = (byte *)rec_get_nth_field(rec, offsets, i, &local_len);

  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  auto byte_val = mach_read_from_1(data + local_len + BTR_EXTERN_LEN);

  if (val) {
    byte_val = byte_val & (~BTR_EXTERN_OWNER_FLAG);
  } else {
    byte_val = byte_val | BTR_EXTERN_OWNER_FLAG;
  }

  if (likely(mtr != nullptr)) {
    mlog_write_ulint(data + local_len + BTR_EXTERN_LEN, byte_val, MLOG_1BYTE, mtr);
  } else {
    mach_write_to_1(data + local_len + BTR_EXTERN_LEN, byte_val);
  }
}

void btr_cur_mark_extern_inherited_fields(rec_t *rec, dict_index_t *index, const ulint *offsets, const upd_t *update, mtr_t *mtr) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(!rec_offs_comp(offsets) || !rec_get_node_ptr_flag(rec));

  if (!rec_offs_any_extern(offsets)) {

    return;
  }

  auto n = rec_offs_n_fields(offsets);

  for (ulint i = 0; i < n; i++) {
    if (rec_offs_nth_extern(offsets, i)) {

      /* Check it is not in updated fields */

      if (update) {
        for (ulint j = 0; j < upd_get_n_fields(update); j++) {
          if (upd_get_nth_field(update, j)->field_no == i) {

            continue;
          }
        }
      }

      btr_cur_set_ownership_of_extern_field(rec, index, offsets, i, false, mtr);
    }
  }
}

/** The complement of the previous function: in an update entry may inherit
some externally stored fields from a record. We must mark them as inherited
in entry, so that they are not freed in a rollback. */

void btr_cur_mark_dtuple_inherited_extern(
  dtuple_t *entry, /*!< in/out: updated entry to be
                         inserted to clustered index */
  const upd_t *update
) /*!< in: update vector */
{
  ulint i;

  for (i = 0; i < dtuple_get_n_fields(entry); i++) {
    auto dfield = dtuple_get_nth_field(entry, i);

    if (!dfield_is_ext(dfield)) {
      continue;
    }

    /* Check if it is in updated fields */

    ulint len;
    byte *data;

    for (ulint j = 0; j < upd_get_n_fields(update); j++) {
      if (upd_get_nth_field(update, j)->field_no == i) {

        continue;
      }
    }

    data = (byte *)dfield_get_data(dfield);
    len = dfield_get_len(dfield);

    data[len - BTR_EXTERN_FIELD_REF_SIZE + BTR_EXTERN_LEN] |= BTR_EXTERN_INHERITED_FLAG;
  }
}

/** Marks all extern fields in a record as owned by the record. This function
should be called if the delete mark of a record is removed: a not delete
marked record always owns all its extern fields. */
static void btr_cur_unmark_extern_fields(
  rec_t *rec,           /*!< in/out: record in a clustered index */
  dict_index_t *index,  /*!< in: index of the page */
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  mtr_t *mtr
) /*!< in: mtr, or nullptr if not logged */
{
  ut_ad(!rec_offs_comp(offsets) || !rec_get_node_ptr_flag(rec));

  auto n = rec_offs_n_fields(offsets);

  if (!rec_offs_any_extern(offsets)) {

    return;
  }

  for (ulint i = 0; i < n; i++) {
    if (rec_offs_nth_extern(offsets, i)) {

      btr_cur_set_ownership_of_extern_field(rec, index, offsets, i, true, mtr);
    }
  }
}

/** Marks all extern fields in a dtuple as owned by the record. */

void btr_cur_unmark_dtuple_extern_fields(dtuple_t *entry) /*!< in/out: clustered index entry */
{
  ulint i;

  for (i = 0; i < dtuple_get_n_fields(entry); i++) {
    dfield_t *dfield = dtuple_get_nth_field(entry, i);

    if (dfield_is_ext(dfield)) {
      auto data = (byte *)dfield_get_data(dfield);
      auto len = dfield_get_len(dfield);

      data[len - BTR_EXTERN_FIELD_REF_SIZE + BTR_EXTERN_LEN] &= ~BTR_EXTERN_OWNER_FLAG;
    }
  }
}

ulint btr_push_update_extern_fields(dtuple_t *tuple, const upd_t *update, mem_heap_t *heap) {
  ulint n;
  ulint n_pushed = 0;
  const upd_field_t *uf;

  ut_ad(tuple);
  ut_ad(update);

  uf = update->fields;
  n = upd_get_n_fields(update);

  for (; n--; uf++) {
    if (dfield_is_ext(&uf->new_val)) {
      dfield_t *field = dtuple_get_nth_field(tuple, uf->field_no);

      if (!dfield_is_ext(field)) {
        dfield_set_ext(field);
        n_pushed++;
      }

      switch (uf->orig_len) {
        byte *data;
        ulint len;
        byte *buf;
        case 0:
          break;
        case BTR_EXTERN_FIELD_REF_SIZE:
          /* Restore the original locally stored
        part of the column.  In the undo log,
        InnoDB writes a longer prefix of externally
        stored columns, so that column prefixes
        in secondary indexes can be reconstructed. */
          dfield_set_data(
            field, (byte *)dfield_get_data(field) + dfield_get_len(field) - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE
          );
          dfield_set_ext(field);
          break;
        default:
          /* Reconstruct the original locally
        stored part of the column.  The data
        will have to be copied. */
          ut_a(uf->orig_len > BTR_EXTERN_FIELD_REF_SIZE);

          data = (byte *)dfield_get_data(field);
          len = dfield_get_len(field);

          buf = (byte *)mem_heap_alloc(heap, uf->orig_len);
          /* Copy the locally stored prefix. */
          memcpy(buf, data, uf->orig_len - BTR_EXTERN_FIELD_REF_SIZE);
          /* Copy the BLOB pointer. */
          memcpy(buf + uf->orig_len - BTR_EXTERN_FIELD_REF_SIZE, data + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);

          dfield_set_data(field, buf, uf->orig_len);
          dfield_set_ext(field);
      }
    }
  }

  return n_pushed;
}

/** Returns the length of a BLOB part stored on the header page.
@return	part length */
static ulint btr_blob_get_part_len(const byte *blob_header) {
  return mach_read_from_4(blob_header + BTR_BLOB_HDR_PART_LEN);
}

/** Returns the page number where the next BLOB part is stored.
@return	page number or FIL_NULL if no more pages */
static ulint btr_blob_get_next_page_no(const byte *blob_header) {
  return mach_read_from_4(blob_header + BTR_BLOB_HDR_NEXT_PAGE_NO);
}

/** Deallocate a buffer block that was reserved for a BLOB part.
@param[in,out] block            Block to free.
@param[in] unused
@param[in,out] mtr              Min-transaction. */
static void btr_blob_free(buf_block_t *block, bool, mtr_t *mtr) {
  auto space = block->get_space();
  auto page_no = block->get_page_no();

  ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

  mtr_commit(mtr);

  buf_pool_mutex_enter();
  mutex_enter(&block->m_mutex);

  /* Only free the block if it is still allocated to the same file page. */

  if (block->get_state() == BUF_BLOCK_FILE_PAGE && block->get_space() == space && block->get_page_no() == page_no) {

    auto block_status = buf_pool->m_LRU->free_block(&block->m_page, nullptr);
    ut_a(block_status == Buf_LRU::Block_status::FREED);
  }

  buf_pool_mutex_exit();
  mutex_exit(&block->m_mutex);
}

db_err btr_store_big_rec_extern_fields(
  dict_index_t *index, buf_block_t *rec_block, rec_t *rec, const ulint *offsets, big_rec_t *big_rec_vec,
  mtr_t *local_mtr __attribute__((unused))
) {
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mtr_memo_contains(local_mtr, dict_index_get_lock(index), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains(local_mtr, rec_block, MTR_MEMO_PAGE_X_FIX));
  ut_ad(rec_block->get_frame() == page_align(rec));
  ut_a(dict_index_is_clust(index));

  auto space_id = rec_block->get_space();
  auto rec_page_no = rec_block->get_page_no();
  ut_a(fil_page_get_type(page_align(rec)) == FIL_PAGE_INDEX);

  /* We have to create a file segment to the tablespace
  for each field and put the pointer to the field in rec */

  for (ulint i = 0; i < big_rec_vec->n_fields; i++) {
    byte *field_ref;

    ut_ad(rec_offs_nth_extern(offsets, big_rec_vec->fields[i].field_no));

    {
      ulint local_len;

      field_ref = rec_get_nth_field(rec, offsets, big_rec_vec->fields[i].field_no, &local_len);

      ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

      local_len -= BTR_EXTERN_FIELD_REF_SIZE;

      field_ref += local_len;
    }

    auto extern_len = big_rec_vec->fields[i].len;

    ut_a(extern_len > 0);

    page_no_t prev_page_no = FIL_NULL;

    for (;;) {
      mtr_t mtr;
      ulint hint_page_no;

      mtr_start(&mtr);

      if (prev_page_no == FIL_NULL) {
        hint_page_no = 1 + rec_page_no;
      } else {
        hint_page_no = prev_page_no + 1;
      }

      auto block = btr_page_alloc(index, hint_page_no, FSP_NO_DIR, 0, &mtr);

      if (unlikely(block == nullptr)) {

        mtr_commit(&mtr);

        return DB_OUT_OF_FILE_SPACE;
      }

      auto page_no = block->get_page_no();
      auto page = block->get_frame();

      if (prev_page_no != FIL_NULL) {

        Buf_pool::Request req {
          .m_rw_latch = RW_X_LATCH,
          .m_page_id = { space_id, prev_page_no },
	  .m_mode = BUF_GET,
	  .m_file = __FILE__,
	  .m_line = __LINE__,
	  .m_mtr = &mtr
        };

        auto prev_block = buf_pool->get(req, nullptr);

        buf_block_dbg_add_level(IF_SYNC_DEBUG(prev_block, SYNC_EXTERN_STORAGE));

        auto prev_page = prev_block->get_frame();

        mlog_write_ulint(prev_page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, page_no, MLOG_4BYTES, &mtr);
      }

      mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_BLOB, MLOG_2BYTES, &mtr);

      ulint store_len;

      if (extern_len > (UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END)) {
        store_len = UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END;
      } else {
        store_len = extern_len;
      }

      mlog_write_string(
        page + FIL_PAGE_DATA + BTR_BLOB_HDR_SIZE,
        (const byte *)big_rec_vec->fields[i].data + big_rec_vec->fields[i].len - extern_len,
        store_len,
        &mtr
      );

      mlog_write_ulint(page + FIL_PAGE_DATA + BTR_BLOB_HDR_PART_LEN, store_len, MLOG_4BYTES, &mtr);

      mlog_write_ulint(page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, FIL_NULL, MLOG_4BYTES, &mtr);

      extern_len -= store_len;

        Buf_pool::Request req {
          .m_rw_latch = RW_X_LATCH,
          .m_page_id = { space_id, rec_page_no },
	  .m_mode = BUF_GET,
	  .m_file = __FILE__,
	  .m_line = __LINE__,
	  .m_mtr = &mtr
        };

      rec_block = buf_pool->get(req, nullptr);

      buf_block_dbg_add_level(IF_SYNC_DEBUG(rec_block, SYNC_NO_ORDER_CHECK));

      mlog_write_ulint(field_ref + BTR_EXTERN_LEN, 0, MLOG_4BYTES, &mtr);

      mlog_write_ulint(field_ref + BTR_EXTERN_LEN + 4, big_rec_vec->fields[i].len - extern_len, MLOG_4BYTES, &mtr);

      if (prev_page_no == FIL_NULL) {
        mlog_write_ulint(field_ref + BTR_EXTERN_SPACE_ID, space_id, MLOG_4BYTES, &mtr);

        mlog_write_ulint(field_ref + BTR_EXTERN_PAGE_NO, page_no, MLOG_4BYTES, &mtr);

        mlog_write_ulint(field_ref + BTR_EXTERN_OFFSET, FIL_PAGE_DATA, MLOG_4BYTES, &mtr);
      }

      prev_page_no = page_no;

      mtr_commit(&mtr);

      if (extern_len == 0) {
        break;
      }
    }
  }

  return DB_SUCCESS;
}

/** Check the FIL_PAGE_TYPE on an uncompressed BLOB page. */
static void btr_check_blob_fil_page_type(
  ulint space_id,     /*!< in: space id */
  ulint page_no,      /*!< in: page number */
  const page_t *page, /*!< in: page */
  bool read
) /*!< in: true=read, false=purge */
{
  ulint type = fil_page_get_type(page);

  ut_a(space_id == page_get_space_id(page));
  ut_a(page_no == page_get_page_no(page));

  if (unlikely(type != FIL_PAGE_TYPE_BLOB)) {
    ulint flags = fil_space_get_flags(space_id);

    if (likely((flags & DICT_TF_FORMAT_MASK) == DICT_TF_FORMAT_51)) {
      /* Old versions of InnoDB did not initialize
      FIL_PAGE_TYPE on BLOB pages.  Do not print
      anything about the type mismatch when reading
      a BLOB page that is in Antelope format.*/
      return;
    }

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  FIL_PAGE_TYPE=%lu"
      " on BLOB %s space %lu page %lu flags %lx\n",
      (ulong)type,
      read ? "read" : "purge",
      (ulong)space_id,
      (ulong)page_no,
      (ulong)flags
    );
    ut_error;
  }
}

void btr_free_externally_stored_field(
  dict_index_t *index, byte *field_ref, const rec_t *rec, const ulint *offsets, ulint i, enum trx_rb_ctx rb_ctx,
  mtr_t *local_mtr __attribute__((unused))
) {

#ifdef UNIV_DEBUG
  ut_ad(mtr_memo_contains(local_mtr, dict_index_get_lock(index), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains_page(local_mtr, field_ref, MTR_MEMO_PAGE_X_FIX));
  ut_ad(!rec || rec_offs_validate(rec, index, offsets));

  if (rec != nullptr) {
    ulint local_len;
    const byte *f = rec_get_nth_field(rec, offsets, i, &local_len);

    ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

    local_len -= BTR_EXTERN_FIELD_REF_SIZE;

    f += local_len;

    ut_ad(f == field_ref);
  }
#endif /* UNIV_DEBUG */

  if (unlikely(!memcmp(field_ref, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE))) {
    /* In the rollback of uncommitted transactions, we may
    encounter a clustered index record whose BLOBs have
    not been written.  There is nothing to free then. */
    ut_a(rb_ctx == RB_RECOVERY || rb_ctx == RB_RECOVERY_PURGE_REC);
    return;
  }

  auto space_id = mach_read_from_4(field_ref + BTR_EXTERN_SPACE_ID);

  if (unlikely(space_id != dict_index_get_space(index))) {
    /* This must be an undo log record in the system tablespace,
    that is, in row_purge_upd_exist_or_extern().
    Currently, externally stored records are stored in the
    same tablespace as the referring records. */
    ut_ad(!page_get_space_id(page_align(field_ref)));
    ut_ad(!rec);
  }

  mtr_t mtr;

  for (;;) {
    mtr_start(&mtr);

    auto ptr{page_align(field_ref)};

    Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { page_get_space_id(ptr), page_get_page_no(ptr) },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    IF_SYNC_DEBUG({
     auto rec_block = buf_pool->get(req, nullptr);
     buf_block_dbg_add_level(rec_block, SYNC_NO_ORDER_CHECK)
    });

    auto page_no = mach_read_from_4(field_ref + BTR_EXTERN_PAGE_NO);

    if (/* There is no external storage data */
        page_no == FIL_NULL
        /* This field does not own the externally stored field */
        || (mach_read_from_1(field_ref + BTR_EXTERN_LEN) & BTR_EXTERN_OWNER_FLAG)
        /* Rollback and inherited field */
        || ((rb_ctx == RB_NORMAL || rb_ctx == RB_RECOVERY) &&
            (mach_read_from_1(field_ref + BTR_EXTERN_LEN) & BTR_EXTERN_INHERITED_FLAG))) {

      /* Do not free */
      mtr_commit(&mtr);

      return;
    }

    req.m_page_id.m_space_id = space_id;
    req.m_page_id.m_page_no = page_no;
    req.m_line = __LINE__;

    auto ext_block = buf_pool->get(req, nullptr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(ext_block, SYNC_EXTERN_STORAGE));

    auto page = ext_block->get_frame();

    btr_check_blob_fil_page_type(space_id, page_no, page, false);

    auto next_page_no = mach_read_from_4(page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO);

    /* We must supply the page level (= 0) as an argument
    because we did not store it on the page (we save the
    space overhead from an index page header. */

    btr_page_free_low(index, ext_block, 0, &mtr);

    mlog_write_ulint(field_ref + BTR_EXTERN_PAGE_NO, next_page_no, MLOG_4BYTES, &mtr);

    /* Zero out the BLOB length. If the server crashes during the execution of
    this function, trx_rollback_or_clean_all_recovered() could dereference the
    half-deleted BLOB, fetching a wrong prefix for the BLOB. */

    mlog_write_ulint(field_ref + BTR_EXTERN_LEN + 4, 0, MLOG_4BYTES, &mtr);

    /* Commit mtr and release the BLOB block to save memory. */
    btr_blob_free(ext_block, true, &mtr);
  }
}

/** Frees the externally stored fields for a record. */
static void btr_rec_free_externally_stored_fields(
  dict_index_t *index,    /*!< in: index of the data, the index
                            tree MUST be X-latched */
  rec_t *rec,             /*!< in/out: record */
  const ulint *offsets,   /*!< in: rec_get_offsets(rec, index) */
  enum trx_rb_ctx rb_ctx, /*!< in: rollback context */
  mtr_t *mtr
) /*!< in: mini-transaction handle which contains
                            an X-latch to record page and to the index
                            tree */
{
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mtr_memo_contains_page(mtr, rec, MTR_MEMO_PAGE_X_FIX));
  /* Free possible externally stored fields in the record */

  ut_ad(dict_table_is_comp(index->table) == !!rec_offs_comp(offsets));

  auto n_fields = rec_offs_n_fields(offsets);

  for (ulint i = 0; i < n_fields; i++) {
    if (rec_offs_nth_extern(offsets, i)) {
      ulint len;
      auto data = (byte *)rec_get_nth_field(rec, offsets, i, &len);

      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);

      btr_free_externally_stored_field(index, data + len - BTR_EXTERN_FIELD_REF_SIZE, rec, offsets, i, rb_ctx, mtr);
    }
  }
}

/** Frees the externally stored fields for a record, if the field is mentioned
in the update vector. */
static void btr_rec_free_updated_extern_fields(
  dict_index_t *index,    /*!< in: index of rec; the index tree MUST be
                            X-latched */
  rec_t *rec,             /*!< in/out: record */
  const ulint *offsets,   /*!< in: rec_get_offsets(rec, index) */
  const upd_t *update,    /*!< in: update vector */
  enum trx_rb_ctx rb_ctx, /*!< in: rollback context */
  mtr_t *mtr
) /*!< in: mini-transaction handle which contains
                            an X-latch to record page and to the tree */
{
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mtr_memo_contains_page(mtr, rec, MTR_MEMO_PAGE_X_FIX));

  /* Free possible externally stored fields in the record */

  auto n_fields = upd_get_n_fields(update);

  for (ulint i = 0; i < n_fields; i++) {
    const upd_field_t *ufield = upd_get_nth_field(update, i);

    if (rec_offs_nth_extern(offsets, ufield->field_no)) {
      ulint len;
      auto data = (byte *)rec_get_nth_field(rec, offsets, ufield->field_no, &len);
      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);

      btr_free_externally_stored_field(index, data + len - BTR_EXTERN_FIELD_REF_SIZE, rec, offsets, ufield->field_no, rb_ctx, mtr);
    }
  }
}

/** Copies the prefix of an uncompressed BLOB.  The clustered index record
that points to this BLOB must be protected by a lock or a page latch.
@return	number of bytes written to buf */
static ulint btr_copy_blob_prefix(
  byte *buf,      /*!< out: the externally stored part of
                    the field, or a prefix of it */
  ulint len,      /*!< in: length of buf, in bytes */
  ulint space_id, /*!< in: space id of the BLOB pages */
  ulint page_no,  /*!< in: page number of the first BLOB page */
  ulint offset
) /*!< in: offset on the first BLOB page */
{
  ulint copied_len = 0;

  for (;;) {
    mtr_t mtr;

    mtr_start(&mtr);

    Buf_pool::Request req {
      .m_rw_latch = RW_S_LATCH,
      .m_page_id = { space_id, page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    auto block = buf_pool->get(req, nullptr);
    const auto page = block->get_frame();

    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_EXTERN_STORAGE));

    btr_check_blob_fil_page_type(space_id, page_no, page, true);

    const auto blob_header = page + offset;
    auto part_len = btr_blob_get_part_len(blob_header);
    auto copy_len = ut_min(part_len, len - copied_len);

    memcpy(buf + copied_len, blob_header + BTR_BLOB_HDR_SIZE, copy_len);
    copied_len += copy_len;

    page_no = btr_blob_get_next_page_no(blob_header);

    mtr_commit(&mtr);

    if (page_no == FIL_NULL || copy_len != part_len) {
      return copied_len;
    }

    /* On other BLOB pages except the first the BLOB header
    always is at the page data start: */

    offset = FIL_PAGE_DATA;

    ut_ad(copied_len <= len);
  }
}

/** Copies the prefix of an externally stored field of a record.  The
clustered index record that points to this BLOB must be protected by a
lock or a page latch.
@return	number of bytes written to buf */
static ulint btr_copy_externally_stored_field_prefix_low(
  byte *buf,             /*!< out: the externally stored part of
                           the field, or a prefix of it */
  ulint len,             /*!< in: length of buf, in bytes */
  ulint, ulint space_id, /*!< in: space id of the first BLOB page */
  ulint page_no,         /*!< in: page number of the first BLOB page */
  ulint offset
) /*!< in: offset on the first BLOB page */
{
  if (unlikely(len == 0)) {
    return 0;
  }

  return btr_copy_blob_prefix(buf, len, space_id, page_no, offset);
}

ulint btr_copy_externally_stored_field_prefix(byte *buf, ulint len, const byte *data, ulint local_len) {
  ulint space_id;
  ulint page_no;
  ulint offset;

  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  if (unlikely(local_len >= len)) {
    memcpy(buf, data, len);
    return len;
  }

  memcpy(buf, data, local_len);
  data += local_len;

  ut_a(memcmp(data, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE));

  if (!mach_read_from_4(data + BTR_EXTERN_LEN + 4)) {
    /* The externally stored part of the column has been
    (partially) deleted.  Signal the half-deleted BLOB
    to the caller. */

    return 0;
  }

  space_id = mach_read_from_4(data + BTR_EXTERN_SPACE_ID);

  page_no = mach_read_from_4(data + BTR_EXTERN_PAGE_NO);

  offset = mach_read_from_4(data + BTR_EXTERN_OFFSET);

  return (local_len + btr_copy_externally_stored_field_prefix_low(buf + local_len, len - local_len, 0, space_id, page_no, offset));
}

/** Copies an externally stored field of a record to mem heap.  The
clustered index record must be protected by a lock or a page latch.
@return	the whole field copied to heap */
static byte *btr_copy_externally_stored_field(
  ulint *len,             /*!< out: length of the whole field */
  const byte *data,       /*!< in: 'internally' stored part of the
                            field containing also the reference to
                            the external part; must be protected by
                            a lock or a page latch */
  ulint, ulint local_len, /*!< in: length of data */
  mem_heap_t *heap
) /*!< in: mem heap */
{
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  auto space_id = mach_read_from_4(data + local_len + BTR_EXTERN_SPACE_ID);

  auto page_no = mach_read_from_4(data + local_len + BTR_EXTERN_PAGE_NO);

  auto offset = mach_read_from_4(data + local_len + BTR_EXTERN_OFFSET);

  /* Currently a BLOB cannot be bigger than 4 GB; we
  leave the 4 upper bytes in the length field unused */

  auto extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);

  auto buf = (byte *)mem_heap_alloc(heap, local_len + extern_len);

  memcpy(buf, data, local_len);

  *len = local_len + btr_copy_externally_stored_field_prefix_low(buf + local_len, extern_len, 0, space_id, page_no, offset);

  return buf;
}

byte *btr_rec_copy_externally_stored_field(const rec_t *rec, const ulint *offsets, ulint no, ulint *len, mem_heap_t *heap) {
  ut_a(rec_offs_nth_extern(offsets, no));

  /* An externally stored field can contain some initial
  data from the field, and in the last 20 bytes it has the
  space id, page number, and offset where the rest of the
  field data is stored, and the data length in addition to
  the data stored locally. We may need to store some data
  locally to get the local record length above the 128 byte
  limit so that field offsets are stored in two bytes, and
  the extern bit is available in those two bytes. */

  ulint local_len;

  auto data = (byte *)rec_get_nth_field(rec, offsets, no, &local_len);

  return btr_copy_externally_stored_field(len, data, 0, local_len, heap);
}
