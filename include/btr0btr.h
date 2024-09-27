/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/btr0btr.h
The B-tree

Created 6/2/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "btr0types.h"
#include "data0data.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0cur.h"

/** Gets the root node of a tree and x-latches it.
@param[in] index                Index tree.
@param[in,out]                  Mini-transaction.
@return	root page, x-latched */
page_t *btr_root_get(dict_index_t *index, mtr_t *mtr);

/** Gets pointer to the previous user record in the tree. It is assumed
that the caller has appropriate latches on the page and its neighbor.
@param[in,out] rec              Record on leaf level
@param[in,out] mtr              Mini-tranaction.
@return	previous user record, nullptr if there is none */
rec_t *btr_get_prev_user_rec(rec_t *rec, mtr_t *mtr);

/** Gets pointer to the next user record in the tree. It is assumed
that the caller has appropriate latches on the page and its neighbor.
@param[in,out] rec              Record on leaf level
@param[in,out] mtr              holding a latch on the page, and if needed,
                                also to the next page
@return	next user record, nullptr if there is none */
rec_t *btr_get_next_user_rec(rec_t *rec, mtr_t *mtr);

/** Creates the root node for a new index tree.
@param[in] type                 type of the index
@param[in] space                space where create
@param[in] index_id             index id
@param[in,out] index            index
@param[in,out] mtr              Mini-transaction handle
@return	page number of the created root, FIL_NULL if did not succeed */
page_no_t btr_create(ulint type, space_id_t space, uint64_t index_id, dict_index_t *index, mtr_t *mtr);

/** Frees a B-tree except the root page, which MUST be freed after this
by calling btr_free_root.
@param[in] space                Space wwhere created
@param[in] root_page_no         Root page number. */
void btr_free_but_not_root(space_id_t space, page_no_t root_page_no);

/** Frees the B-tree root page. Other tree MUST already have been freed.
@param[in] spac                 space where created
@param[in] root_page_no         Root page number
@param[in,out] mtr              A mini-transaction which has already been started */
void btr_free_root(space_id_t space, page_no_t root_page_no, mtr_t *mtr);

/** Makes tree one level higher by splitting the root, and inserts
the tuple. It is assumed that mtr contains an x-latch on the tree.
NOTE that the operation of this function must always succeed,
we cannot reverse it: therefore enough free disk space must be
guaranteed to be available before this function is called.
ww
@param[in] cursor,              Cursor at which to insert: must be on the
                                root page; when the function returns, the
				cursor is positioned on the predecessor
				of the inserted record
@param[in] tuple                Tuple to insert
@param[in] n_ext                Number of externally stored columns
@param[in,out] mtr              Mini-transaction.
@return	inserted record */
rec_t *btr_root_raise_and_insert(btr_cur_t *cursor, const dtuple_t *tuple, ulint n_ext, mtr_t *mtr);

/** Reorganizes an index page.
IMPORTANT: if btr_page_reorganize() is invoked on a compressed leaf
page of a non-clustered index, the caller must update the insert
buffer free bits in the same mini-transaction in such a way that the
modification will be redo-logged.
@param[in] block                Page to be reorganized
@param[in] index                Record descriptor
@param[in] mtr                  Mini-transaction
@return	true on success, false on failure */
bool btr_page_reorganize(buf_block_t *block, dict_index_t *index, mtr_t *mtr);

/** Decides if the page should be split at the convergence point of
inserts converging to left.
@param[in] cursor               Cursor at which to insert
@param[out] split_rec           If split recommended, the first record on
                                upper half page, or nullptr if tuple should be first
@return	true if split recommended */
bool btr_page_get_split_rec_to_left(btr_cur_t *cursor, rec_t **split_rec);

/** Decides if the page should be split at the convergence point of
inserts converging to right.
@param[in] cursor               Cursor at which to insert
@param[in] split_rec            If split recommended, the first record on
                                upper half page, or nullptr if tuple should
				be first.
@return	true if split recommended */
bool btr_page_get_split_rec_to_right(btr_cur_t *cursor, rec_t **split_rec);

/** Splits an index page to halves and inserts the tuple. It is assumed
that mtr holds an x-latch to the index tree. NOTE: the tree x-latch is
released within this function! NOTE that the operation of this
function must always succeed, we cannot reverse it: therefore enough
free disk space (2 pages) must be guaranteed to be available before
this function is called.
@param[in] cursor               Cursor at which to insert; when the function
                                returns, the cursor is positioned on the
				predecessor of the inserted record
@param[in] tuple                Tuple to insert
@param[in] n_ext                Number of externally stored columns
@param[in,out] mtr              Mini-transaction
@return inserted record */
rec_t *btr_page_split_and_insert(btr_cur_t *cursor, const dtuple_t *tuple, ulint n_ext, mtr_t *mtr);

/** Inserts a data tuple to a tree on a non-leaf level. It is assumed
that mtr holds an x-latch on the tree.
@param[in] index                Index
@param[in] level                Level, must be > 0
@param[in] tuple                The record to be inserted
@param[in] file                 File name
@param[in] line                 Line where called
@param[in,out] mtr              Mini-transaction. */
void btr_insert_on_non_leaf_level_func(dict_index_t *index, ulint level, dtuple_t *tuple, const char *file, ulint line, mtr_t *mtr);

#define btr_insert_on_non_leaf_level(i, l, t, m) btr_insert_on_non_leaf_level_func(i, l, t, __FILE__, __LINE__, m)

/** Sets a record as the predefined minimum record.
@param[in,out] rec              Record
@param[in,out] mtr              Mini-transaction. */
void btr_set_min_rec_mark(rec_t *rec, mtr_t *mtr);

/** Deletes on the upper level the node pointer to a page.
@param[in,out] index            Index tree
@param[in,out] block            Page whose node pointer is deleted
@param[in,out] mtr              Mini-transaction. */
void btr_node_ptr_delete(dict_index_t *index, buf_block_t *block, mtr_t *mtr);

#ifdef UNIV_DEBUG
/** Checks that the node pointer to a page is appropriate.
@param[in,out] index            Index tree
@param[in,out] block            Index page
@param[in,out] mtr              Mini-transaction
@return	true */
bool btr_check_node_ptr(dict_index_t *index, buf_block_t *block, mtr_t *mtr);
#endif /* UNIV_DEBUG */

/** Tries to merge the page first to the left immediate brother if such a
brother exists, and the node pointers to the current page and to the
brother reside on the same page. If the left brother does not satisfy these
conditions, looks at the right brother. If the page is the only one on that
level lifts the records of the page to the father page, thus reducing the
tree height. It is assumed that mtr holds an x-latch on the tree and on the
page. If cursor is on the leaf level, mtr must also hold x-latches to
the brothers, if they exist.
@param[in,out] cursor           Cursor on the page to merge or
                                lift; the page must not be empty: in
                                record delete use btr_discard_page if the
                                page would become empty
@param[in,out] mtr              Mini-transaction.
@return	true on success */
bool btr_compress(btr_cur_t *cursor, mtr_t *mtr);

/** Discards a page from a B-tree. This is used to remove the last record from
a B-tree page: the whole page must be removed at the same time. This cannot
be used for the root page, which is allowed to be empty.
@param[in,out] cursor           Cursor on the page to discard: not on the root page
@param[in,out] mtr              Mini-transaction. */
void btr_discard_page(btr_cur_t *cursor, mtr_t *mtr);

/** Parses the redo log record for setting an index record as the predefined
minimum record.
@param[in,out] ptr,             Buffer
@param[in,out] end_ptr          Buffer end
@param[in,out] page             Page or nullptr
@param[in,out] mtr              Mini-transaction or nullptr
@return	end of log record or nullptr */
byte *btr_parse_set_min_rec_mark(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr);

/** Parses a redo log record of reorganizing a page.
@param[in,out] ptr              Buffer
@param[in,out] end_ptr          Buffer end
@param[in,out] index            Record descriptor
@param[in,out] block            Page to be reorganized, or nullptr
@param[in,out] mtr              Mini-transaction or nullptr
@return	end of log record or nullptr */
byte *btr_parse_page_reorganize(byte *ptr, byte *end_ptr, dict_index_t *index, buf_block_t *block, mtr_t *mtr);

/** Gets the number of pages in a B-tree.
@param[in] index                Index
@param[in] flag                 BTR_N_LEAF_PAGES or BTR_TOTAL_SIZE
@return	number of pages */
ulint btr_get_size(dict_index_t *index, ulint flag);

/** Allocates a new file page to be used in an index tree. NOTE: we assume
that the caller has made the reservation for free extents!
@param[in] index                Index tree
@param[in] hint_page_no         Hint of a good page
@param[in] file_direction       Direction where a possible page split is made
@param[in] level                Level where the page is placed in the tree
@param[in] mtr                  Mini-transaction
@return	new allocated block, x-latched; nullptr if out of space */
buf_block_t *btr_page_alloc(dict_index_t *index, page_no_t hint_page_no, byte file_direction, ulint level, mtr_t *mtr);

/** Frees a file page used in an index tree. NOTE: cannot free field external
storage pages because the page must contain info on its level.
@param[in,out] index            Index tree
@param[in,out] block            Block to be freed, x-latched
@param[in,out] mtr              Mini-transaction.  */
void btr_page_free(dict_index_t *index, buf_block_t *block, mtr_t *mtr);

/** Frees a file page used in an index tree. Can be used also to BLOB
external storage pages, because the page level 0 can be given as an
argument.
@param[in,out] index            Index tree
@param[in,out] block            Block to be freed, x-latched
@param[in] level                Page level
@param[in,out] mtr              Mini-transaction */
void btr_page_free_low(dict_index_t *index, buf_block_t *block, ulint level, mtr_t *mtr);

#ifdef UNIV_BTR_PRINT
/** Prints size info of a B-tree.
@param[in] index                Index */
void btr_print_size(dict_index_t *index); /** in: index tree */

/** Prints directories and other info of all nodes in the index.
@param[in] index                Index
@param[in] width                Print this many entries from start and end */
void btr_print_index(dict_index_t *index, ulint width);
#endif /* UNIV_BTR_PRINT */

/** Checks the size and number of fields in a record based on the definition of
the index.
@param[in,out] rec              Index record
@param[in,out] index            Index
@param[in] dump_on_error        true if the function should print hex dump of
                                record and page on error
@return	true if ok */
bool btr_index_rec_validate(const rec_t *rec, const dict_index_t *index, bool dump_on_error);

/** Checks the consistency of an index tree.
@param[in,out] index            Index
@param[in,out] trx              Transaction or nullptr
@return	true if ok */
bool btr_validate_index(dict_index_t *index, trx_t *trx);

/** Gets a buffer page and declares its latching order level.
@param[in] space_id             Space id
@param[in] page_no              Page number
@param[in] rw_latch             Latch mode
@param[in,out] mtr              Mini-transaction. */
inline buf_block_t *btr_block_get(space_id_t space_id, page_no_t page_no, ulint rw_latch, mtr_t *mtr) {
  Buf_pool::Request req {
    .m_rw_latch = rw_latch,
    .m_page_id = { space_id, page_no },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);

  if (rw_latch != RW_NO_LATCH) {

    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE));
  }

  return block;
}

/** Gets a buffer page and declares its latching order level.
@param[in] space                Space id
@param[in] page_no              Page number
@param[in] mode                 Latch mode
@param[in,out] mtr              Mini-transaction. */
inline page_t *btr_page_get(space_id_t space, page_no_t page_no, ulint rw_latch, mtr_t *mtr) {
  return btr_block_get(space, page_no, rw_latch, mtr)->get_frame();
}

/** Sets the index id field of a page.
@param[in,out] page             Page to be created
@param[in] id                   Index id
@param[in,out] mtr              Mini-transaction. */
inline void btr_page_set_index_id(page_t *page, uint64_t id, mtr_t *mtr) {
  mlog_write_uint64(page + (PAGE_HEADER + PAGE_INDEX_ID), id, mtr);
}

/** Gets the index id field of a page.
@param[in,out] page             Get index ID from this page
@return	index id */
inline uint64_t btr_page_get_index_id(const page_t *page) {
  return mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID);
}

/** Gets the node level field in an index page.
@param[in,out] page             Get node level from this page
@return	level, leaf level == 0 */
inline ulint btr_page_get_level_low(const page_t *page) {
  const auto level = mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL);

  ut_ad(level <= BTR_MAX_DEPTH);

  return level;
}

/** Gets the node level field in an index page.
@param[in,out] page             Get node level from this page
@return	level, leaf level == 0 */
inline ulint btr_page_get_level(const page_t *page, mtr_t *) {
  return btr_page_get_level_low(page);
}

/** Sets the node level field in an index page.
@param[in,out] page             Index page
@param[in] level                leaf level == 0
@param[in,out] mtr              Mini-transaction */
inline void btr_page_set_level(page_t *page, ulint level, mtr_t *mtr) {
  ut_ad(level <= BTR_MAX_DEPTH);

  /** 16 levels is a very big tree. */
  ut_ad(level < 16);

  mlog_write_ulint(page + (PAGE_HEADER + PAGE_LEVEL), level, MLOG_2BYTES, mtr);
}

/** Gets the next index page number.
@param[in] page                 Index page
@return	next page number */
inline page_no_t btr_page_get_next(const page_t *page, mtr_t *mtr) {
  ut_ad(mtr->memo_contains_page(page, MTR_MEMO_PAGE_X_FIX) || mtr->memo_contains_page(page, MTR_MEMO_PAGE_S_FIX));

  return mach_read_from_4(page + FIL_PAGE_NEXT);
}

/** Sets the next index page field.
@param[in,out] page             Index page
@param[in] next                 Next page number
@param[in,out]                  Mini-transaction. */
inline void btr_page_set_next(page_t *page, page_no_t next, mtr_t *mtr) {
  mlog_write_ulint(page + FIL_PAGE_NEXT, next, MLOG_4BYTES, mtr);
}

/** Gets the previous index page number.
@param[in] page                 Index page
@return	prev page number */
inline page_no_t btr_page_get_prev(const page_t *page, mtr_t *) {
  return mach_read_from_4(page + FIL_PAGE_PREV);
}

/** Sets the previous index page field.
@param[in,out] page             Index page
@param[in] prev                 Prev page number
@param[in,out]                  Mini-transaction. */
inline void btr_page_set_prev(page_t *page, page_no_t prev, mtr_t *mtr) {
  mlog_write_ulint(page + FIL_PAGE_PREV, prev, MLOG_4BYTES, mtr);
}

/** Gets the child node file address in a node pointer.
NOTE: the offsets array must contain all offsets for the record since
we read the last field according to offsets and assume that it contains
the child page number. In other words offsets must have been retrieved
with Phy_rec::get_col_offsets(n_fields=ULINT_UNDEFINED).
@param[in] rec,                 Node pointer record 
@param[in] offsets              Array returned by Phy_rec::get_col_offsets()
@return	child node address */
inline ulint btr_node_ptr_get_child_page_no(const rec_t *rec, const ulint *offsets) {
  ulint len;

  /* The child address is in the last field */
  auto field = rec_get_nth_field(rec, offsets, rec_offs_n_fields(offsets) - 1, &len);

  ut_ad(len == 4);

  page_no_t page_no = mach_read_from_4(field);

  if (unlikely(page_no == 0)) {
    ib_logger(ib_stream, "a nonsensical page number 0 in a node ptr record at offset %lun", (ulong)page_offset(rec));
    buf_page_print(page_align(rec), 0);
  }

  return page_no;
}

/** Releases the latches on a leaf page and bufferunfixes it. 
@param[in,out] block,           Buffer block
@param[in] latch_mode           BTR_SEARCH_LEAF or BTR_MODIFY_LEAF
@param[in,out] mtr              Mini-transaction */
inline void btr_leaf_page_release(buf_block_t *block, ulint latch_mode, mtr_t *mtr) {
  ut_ad(!mtr->memo_contains(block, MTR_MEMO_MODIFY));
  ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);

  mtr->memo_release(block, latch_mode == BTR_SEARCH_LEAF ? MTR_MEMO_PAGE_S_FIX : MTR_MEMO_PAGE_X_FIX);
}
