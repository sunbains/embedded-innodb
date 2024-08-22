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

/** @file btr/btr0btr.c
The B-tree

Created 6/2/1994 Heikki Tuuri
*******************************************************/

#include "btr0btr.h"

#include "btr0cur.h"
#include "btr0pcur.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "trx0trx.h"

/*
Latching strategy of the InnoDB B-tree
--------------------------------------
A tree latch protects all non-leaf nodes of the tree. Each node of a tree
also has a latch of its own.

A B-tree operation normally first acquires an S-latch on the tree. It
searches down the tree and releases the tree latch when it has the
leaf node latch. To save CPU time we do not acquire any latch on
non-leaf nodes of the tree during a search, those pages are only bufferfixed.

If an operation needs to restructure the tree, it acquires an X-latch on
the tree before searching to a leaf node. If it needs, for example, to
split a leaf,
(1) InnoDB decides the split point in the leaf,
(2) allocates a new page,
(3) inserts the appropriate node pointer to the first non-leaf level,
(4) releases the tree X-latch,
(5) and then moves records from the leaf to the new allocated page.

Node pointers
-------------
Leaf pages of a B-tree contain the index records stored in the
tree. On levels n > 0 we store 'node pointers' to pages on level
n - 1. For each page there is exactly one node pointer stored:
thus the our tree is an ordinary B-tree, not a B-link tree.

A node pointer contains a prefix P of an index record. The prefix
is long enough so that it determines an index record uniquely.
The file page number of the child page is added as the last
field. To the child page we can store node pointers or index records
which are >= P in the alphabetical order, but < P1 if there is
a next node pointer on the level, and P1 is its prefix.

If a node pointer with a prefix P points to a non-leaf child,
then the leftmost record in the child must have the same
prefix P. If it points to a leaf node, the child is not required
to contain any record with a prefix equal to P. The leaf case
is decided this way to allow arbitrary deletions in a leaf node
without touching upper levels of the tree.

We have predefined a special minimum record which we
define as the smallest record in any alphabetical order.
A minimum record is denoted by setting a bit in the record
header. A minimum record acts as the prefix of a node pointer
which points to a leftmost node on any level of the tree.

File page allocation
--------------------
In the root node of a B-tree there are two file segment headers.
The leaf pages of a tree are allocated from one file segment, to
make them consecutive on disk if possible. From the other file segment
we allocate pages for the non-leaf levels of the tree.
*/

#ifdef UNIV_BTR_DEBUG
/**
 * Validates the root file segment.
 *
 * This function checks the validity of the root file segment by verifying the segment header
 * and the tablespace identifier. It ensures that the offset is within the valid range.
 *
 * @param seg_header The segment header to validate.
 * @param space The tablespace identifier.
 * 
 * @return Returns true if the root file segment is valid, otherwise false.
 */
static bool btr_root_fseg_validate(const fseg_header_t *seg_header, ulint space) {
  ulint offset = mach_read_from_2(seg_header + FSEG_HDR_OFFSET);

  ut_a(mach_read_from_4(seg_header + FSEG_HDR_SPACE) == space);
  ut_a(offset >= FIL_PAGE_DATA);
  ut_a(offset <= UNIV_PAGE_SIZE - FIL_PAGE_DATA_END);

  return true;
}
#endif /* UNIV_BTR_DEBUG */

/** Gets the root node of a tree and x-latches it.
@param[in,out] dict_index       Dict index tree.
@param[in,out] mtr              Mini-tranaction covering the operation.
@return	root page, x-latched */
static buf_block_t *btr_root_block_get(dict_index_t *dict_index, mtr_t *mtr) {
  auto space = dict_index_get_space(dict_index);
  auto root_page_no = dict_index_get_page(dict_index);
  auto block = btr_block_get(space, root_page_no, RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
  const page_t *root = block->get_frame();

  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

  return block;
}

page_t *btr_root_get(dict_index_t *dict_index, mtr_t *mtr) {
  return btr_root_block_get(dict_index, mtr)->get_frame();
}

rec_t *btr_get_prev_user_rec(rec_t *rec, mtr_t *mtr) {
  if (!page_rec_is_infimum(rec)) {

    auto prev_rec = page_rec_get_prev(rec);

    if (!page_rec_is_infimum(prev_rec)) {

      return prev_rec;
    }
  }

  auto page = page_align(rec);
  auto prev_page_no = btr_page_get_prev(page, mtr);

  if (prev_page_no != FIL_NULL) {

    Buf_pool::Request  req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { page_get_space_id(page), prev_page_no },
      .m_mode = BUF_GET_NO_LATCH,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto prev_block = srv_buf_pool->get(req, nullptr);
    auto prev_page = prev_block->get_frame();

    /* The caller must already have a latch to the brother */
    ut_ad(mtr->memo_contains(prev_block, MTR_MEMO_PAGE_S_FIX) ||
          mtr->memo_contains(prev_block, MTR_MEMO_PAGE_X_FIX));

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_next(prev_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    return page_rec_get_prev(page_get_supremum_rec(prev_page));

  } else {

    return nullptr;
  }
}

rec_t *btr_get_next_user_rec(rec_t *rec, mtr_t *mtr) {
  if (!page_rec_is_supremum(rec)) {

    auto next_rec = page_rec_get_next(rec);

    if (!page_rec_is_supremum(next_rec)) {

      return next_rec;
    }
  }

  auto page = page_align(rec);
  auto next_page_no = btr_page_get_next(page, mtr);

  if (next_page_no != FIL_NULL) {

    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { page_get_space_id(page), next_page_no },
      .m_mode = BUF_GET_NO_LATCH,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto next_block = srv_buf_pool->get(req, nullptr);
    auto next_page = next_block->get_frame();

    /* The caller must already have a latch to the brother */
    ut_ad(mtr->memo_contains(next_block, MTR_MEMO_PAGE_S_FIX) ||
          mtr->memo_contains(next_block, MTR_MEMO_PAGE_X_FIX));

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_prev(next_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    return page_rec_get_next(page_get_infimum_rec(next_page));

  } else {

    return nullptr;
  }
}

/** Creates a new index page (not the root, and also not
used in page reorganization).  @see btr_page_empty().
@param[in,out] block            Page to be created.
@param[in,out] dict_index       The index in which to create
@param[in] level                Btree level of the page
@param[in,out] mtr              Mini-transaction coverring the operation. */
static void btr_page_create(buf_block_t *block, dict_index_t *dict_index, ulint level, mtr_t *mtr) {
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  page_create(block, mtr);

  /* Set the level of the new index page */
  btr_page_set_level(page, level, mtr);

  block->m_check_index_page_at_flush = true;

  btr_page_set_index_id(page, dict_index->id, mtr);
}

buf_block_t *btr_page_alloc(dict_index_t *dict_index, ulint hint_page_no, byte file_direction, ulint level, mtr_t *mtr) {
  auto root = btr_root_get(dict_index, mtr);

  fseg_header_t *seg_header;

  if (level == 0) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  } else {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;
  }

  /* Parameter true below states that the caller has made the
  reservation for free extents, and thus we know that a page can
  be allocated: */

  auto new_page_no = srv_fsp->fseg_alloc_free_page_general(seg_header, hint_page_no, file_direction, true, mtr);

  if (new_page_no == FIL_NULL) {

    return nullptr;

  } else {

    Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { dict_index_get_space(dict_index), new_page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto new_block = srv_buf_pool->get(req, nullptr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(new_block, SYNC_TREE_NODE_NEW));

    return new_block;
  }
}

ulint btr_get_size(dict_index_t *dict_index, ulint flag) {
  fseg_header_t *seg_header;
  ulint n;
  ulint dummy;
  mtr_t mtr;

  mtr.start();

  mtr_s_lock(dict_index_get_lock(dict_index), &mtr);

  auto root = btr_root_get(dict_index, &mtr);

  if (flag == BTR_N_LEAF_PAGES) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

    srv_fsp->fseg_n_reserved_pages(seg_header, &n, &mtr);

  } else if (flag == BTR_TOTAL_SIZE) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

    n = srv_fsp->fseg_n_reserved_pages(seg_header, &dummy, &mtr);

    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

    n += srv_fsp->fseg_n_reserved_pages(seg_header, &dummy, &mtr);
  } else {
    ut_error;
  }

  mtr.commit();

  return n;
}

void btr_page_free_low(dict_index_t *dict_index, buf_block_t *block, ulint level, mtr_t *mtr) {
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  /* The page gets invalid for optimistic searches: increment the frame
  modify clock */

  buf_block_modify_clock_inc(block);

  auto root = btr_root_get(dict_index, mtr);

  fseg_header_t *seg_header;

  if (level == 0) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  } else {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;
  }

  srv_fsp->fseg_free_page(seg_header, block->get_space(), block->get_page_no(), mtr);
}

void btr_page_free(dict_index_t *dict_index, buf_block_t *block, mtr_t *mtr) {
  auto level = btr_page_get_level(block->get_frame(), mtr);

  btr_page_free_low(dict_index, block, level, mtr);
}

/** Sets the child node file address in a node pointer.
@param[in,out] rec              Node pointer record
@param[in] offsets              Array returned by Phy_rec::get_col_offsets() 
@param[in] page_no              child node address
@param[in,out] mtr              Mini-transaction covering the operation.  */
static void btr_node_ptr_set_child_page_no(rec_t *rec, const ulint *offsets, ulint page_no, mtr_t *mtr) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(!page_is_leaf(page_align(rec)));

  ulint len;

  /* The child address is in the last field */
  auto offset = rec_get_nth_field_offs(offsets, rec_offs_n_fields(offsets) - 1, &len);

  ut_ad(len == REC_NODE_PTR_SIZE);

  mlog_write_ulint(rec + offset, page_no, MLOG_4BYTES, mtr);
}

/** Returns the child page of a node pointer and x-latches it.
@param[in]node_ptr              Node pointer
@param[in,out]dict_index        Dict index tree
@param[in]offsets,              Array returned by Phy_rec::get_col_offsets()
@param[in,out]mtr               Mini-transaction covering the operation.
@return	child page, x-latched */
static buf_block_t *btr_node_ptr_get_child(const rec_t *node_ptr, dict_index_t *dict_index, const ulint *offsets, mtr_t *mtr) {
  ut_ad(rec_offs_validate(node_ptr, dict_index, offsets));
  auto space = page_get_space_id(page_align(node_ptr));
  auto page_no = btr_node_ptr_get_child_page_no(node_ptr, offsets);

  return btr_block_get(space, page_no, RW_X_LATCH, mtr);
}

/**
 * Returns the upper level node pointer to a page. It is assumed that mtr holds
 * an x-latch on the tree.
 *
 * @param offsets The work area for the return value.
 * @param heap The memory heap to use.
 * @param cursor The cursor pointing to the user record. After the function call,
 *   it will be on the node pointer record with its page x-latched.
 * @param file The file name.
 * @param line The line where the function is called.
 * @param mtr The mtr object.
 * @return The father node pointer as an array of offsets.
 */
static ulint *btr_page_get_father_node_ptr_func(ulint *offsets, mem_heap_t *heap, btr_cur_t *cursor, const char *file, ulint line, mtr_t *mtr) {
  auto page_no = btr_cur_get_block(cursor)->get_page_no();
  auto dict_index = btr_cur_get_index(cursor);

  ut_ad(mtr->memo_contains(dict_index_get_lock(dict_index), MTR_MEMO_X_LOCK));

  ut_ad(dict_index_get_page(dict_index) != page_no);

  auto level = btr_page_get_level(btr_cur_get_page(cursor), mtr);
  auto user_rec = btr_cur_get_rec(cursor);
  ut_a(page_rec_is_user_rec(user_rec));
  auto tuple = dict_index_build_node_ptr(dict_index, user_rec, 0, heap, level);

  btr_cur_search_to_nth_level(dict_index, level + 1, tuple, PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, cursor, 0, file, line, mtr);

  auto node_ptr = btr_cur_get_rec(cursor);

  {
    Phy_rec record{dict_index, node_ptr};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
  }

  if (unlikely(btr_node_ptr_get_child_page_no(node_ptr, offsets) != page_no)) {
    rec_t *print_rec;
    ib_logger(ib_stream, "Dump of the child page:\n");
    buf_page_print(page_align(user_rec), 0);
    ib_logger(ib_stream, "Dump of the parent page:\n");
    buf_page_print(page_align(node_ptr), 0);
    ib_logger(ib_stream, "Corruption of an index tree: table ");
    ut_print_name(dict_index->table_name);
    ib_logger(ib_stream, ", index ");
    ut_print_name(dict_index->name);
    ib_logger(
      ib_stream,
      ",\n"
      "father ptr page no %lu, child page no %lu\n",
      (ulong)btr_node_ptr_get_child_page_no(node_ptr, offsets),
      (ulong)page_no
    );

    print_rec = page_rec_get_next(page_get_infimum_rec(page_align(user_rec)));

    {
      Phy_rec record{dict_index, print_rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    page_rec_print(print_rec, offsets);

    {
      Phy_rec record{dict_index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    page_rec_print(node_ptr, offsets);

    log_fatal(
      "You should dump + drop + reimport the table to fix the"
      " corruption. If the crash happens at the database startup, see"
      " InnoDB website for details about forcing recovery. Then"
      " dump + drop + reimport."
    );
  }

  return (offsets);
}

#define btr_page_get_father_node_ptr(of, heap, cur, mtr) btr_page_get_father_node_ptr_func(of, heap, cur, __FILE__, __LINE__, mtr)

/** Returns the upper level node pointer to a page. It is assumed that mtr holds
an x-latch on the tree.
@return	Rec::get_col_offsets() of the node pointer record */
static ulint *btr_page_get_father_block(
  ulint *offsets,           /*!< in: work area for the return value */
  mem_heap_t *heap,         /*!< in: memory heap to use */
  dict_index_t *dict_index, /*!< in: b-tree index */
  buf_block_t *block,       /*!< in: child page in the index */
  mtr_t *mtr,               /*!< in: mtr */
  btr_cur_t *cursor
) /*!< out: cursor on node pointer record,
                              its page x-latched */
{
  rec_t *rec = page_rec_get_next(page_get_infimum_rec(block->get_frame()));
  btr_cur_position(dict_index, rec, block, cursor);
  return (btr_page_get_father_node_ptr(offsets, heap, cursor, mtr));
}

/** Seeks to the upper level node pointer to a page.
It is assumed that mtr holds an x-latch on the tree. */
static void btr_page_get_father(
  dict_index_t *dict_index, /*!< in: b-tree index */
  buf_block_t *block,       /*!< in: child page in the index */
  mtr_t *mtr,               /*!< in: mtr */
  btr_cur_t *cursor
) /*!< out: cursor on node pointer record,
                                        its page x-latched */
{
  mem_heap_t *heap;
  rec_t *rec = page_rec_get_next(page_get_infimum_rec(block->get_frame()));
  btr_cur_position(dict_index, rec, block, cursor);

  heap = mem_heap_create(100);
  btr_page_get_father_node_ptr(nullptr, heap, cursor, mtr);
  mem_heap_free(heap);
}

ulint btr_create(ulint type, ulint space, uint64_t index_id, dict_index_t *dict_index, mtr_t *mtr) {
  auto block = srv_fsp->fseg_create(space, 0, PAGE_HEADER + PAGE_BTR_SEG_TOP, mtr);

  if (block == nullptr) {

    return FIL_NULL;
  }

  auto page_no = block->get_page_no();

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE_NEW));

  if (!srv_fsp->fseg_create(space, page_no, PAGE_HEADER + PAGE_BTR_SEG_LEAF, mtr)) {
    /* Not enough space for new segment, free root segment before return. */
    btr_free_root(space, page_no, mtr);
    return FIL_NULL;
  }

  /* The fseg create acquires a second latch on the page,
  therefore we must declare it: */
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE_NEW));

  /* Create a new index page on the allocated segment page */
  auto page = page_create(block, mtr);

  /* Set the level of the new index page */
  btr_page_set_level(page, 0, mtr);

  block->m_check_index_page_at_flush = true;

  /* Set the index id of the page */
  btr_page_set_index_id(page, index_id, mtr);

  /* Set the next node and previous node fields */
  btr_page_set_next(page, FIL_NULL, mtr);
  btr_page_set_prev(page, FIL_NULL, mtr);

  /* In the following assertion we test that two records of maximum
  allowed size fit on the root page: this fact is needed to ensure
  correctness of split algorithms */

  ut_ad(page_get_max_insert_size(page, 2) > 2 * BTR_PAGE_MAX_REC_SIZE);

  return (page_no);
}

void btr_free_but_not_root(ulint space, ulint root_page_no) {
  bool finished;
  page_t *root;
  mtr_t mtr;

leaf_loop:
  mtr.start();

  root = btr_page_get(space, root_page_no, RW_X_LATCH, &mtr);
#ifdef UNIV_BTR_DEBUG
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

  /* NOTE: page hash indexes are dropped when a page is freed inside
  fsp0fsp. */

  finished = srv_fsp->fseg_free_step(root + PAGE_HEADER + PAGE_BTR_SEG_LEAF, &mtr);

  mtr.commit();

  if (!finished) {

    goto leaf_loop;
  }
top_loop:
  mtr.start();

  root = btr_page_get(space, root_page_no, RW_X_LATCH, &mtr);

#ifdef UNIV_BTR_DEBUG
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

  finished = srv_fsp->fseg_free_step_not_header(root + PAGE_HEADER + PAGE_BTR_SEG_TOP, &mtr);

  mtr.commit();

  if (!finished) {

    goto top_loop;
  }
}

void btr_free_root(ulint space, ulint root_page_no, mtr_t *mtr) {
  auto block = btr_block_get(space, root_page_no, RW_X_LATCH, mtr);

  auto header = block->get_frame() + PAGE_HEADER + PAGE_BTR_SEG_TOP;

#ifdef UNIV_BTR_DEBUG
  ut_a(btr_root_fseg_validate(header, space));
#endif /* UNIV_BTR_DEBUG */

  while (!srv_fsp->fseg_free_step(header, mtr))
    ;
}

/** Reorganizes an index page. */
static bool btr_page_reorganize_low(
  bool recovery,            /*!< in: true if called in recovery:
                                            locks should not be updated, i.e.,
                                            there cannot exist locks on the
                                            page, and a hash index should not be
                                            dropped: it cannot exist */
  buf_block_t *block,       /*!< in: page to be reorganized */
  dict_index_t *dict_index, /*!< in: record descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  bool success = false;
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  auto data_size1 = page_get_data_size(page);
  auto max_ins_size1 = page_get_max_insert_size_after_reorganize(page, 1);

  /* Write the log record */
  mlog_open_and_write_index(mtr, page, dict_index, MLOG_PAGE_REORGANIZE, 0);

  /* Turn logging off */
  auto log_mode = mtr->set_log_mode(MTR_LOG_NONE);

  auto temp_block = srv_buf_pool->block_alloc();
  auto temp_page = temp_block->get_frame();

  /* Copy the old page to temporary space */
  buf_frame_copy(temp_page, page);

  block->m_check_index_page_at_flush = true;

  /* Recreate the page: note that global data on page (possible
  segment headers, next page-field, etc.) is preserved intact */

  page_create(block, mtr);

  /* Copy the records from the temporary space to the recreated page;
  do not copy the lock bits yet */

  page_copy_rec_list_end_no_locks(block, temp_block, page_get_infimum_rec(temp_page), dict_index, mtr);

  if (dict_index_is_sec(dict_index) && page_is_leaf(page)) {

    auto max_trx_id = page_get_max_trx_id(temp_page);

    page_set_max_trx_id(block, max_trx_id, mtr);

    ut_ad(max_trx_id > 0 || recovery);
  }

  if (likely(!recovery)) {
    /* Update the record lock bitmaps */
    lock_move_reorganize_page(block, temp_block);
  }

  auto data_size2 = page_get_data_size(page);
  auto max_ins_size2 = page_get_max_insert_size_after_reorganize(page, 1);

  if (unlikely(data_size1 != data_size2) || unlikely(max_ins_size1 != max_ins_size2)) {
    buf_page_print(page, 0);
    buf_page_print(temp_page, 0);

    ib_logger(
      ib_stream,
      "Error: page old data size %lu new data size %lu\n"
      "Error: page old max ins size %lu new max ins size %lu\n"
      "Submit a detailed bug report, check the InnoDB website for details",
      (unsigned long)data_size1,
      (unsigned long)data_size2,
      (unsigned long)max_ins_size1,
      (unsigned long)max_ins_size2
    );
  } else {
    success = true;
  }

  srv_buf_pool->block_free(temp_block);

  /* Restore logging mode */
  auto old_mode = mtr->set_log_mode(log_mode);
  ut_a(old_mode == MTR_LOG_NONE);

  return success;
}

/** Reorganizes an index page.
IMPORTANT: if btr_page_reorganize() is invoked on a compressed leaf
page of a non-clustered index, the caller must update the insert
buffer free bits in the same mini-transaction in such a way that the
modification will be redo-logged.
@return	true on success, false on failure */

bool btr_page_reorganize(
  buf_block_t *block,       /*!< in: page to be reorganized */
  dict_index_t *dict_index, /*!< in: record descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  return (btr_page_reorganize_low(false, block, dict_index, mtr));
}

/** Parses a redo log record of reorganizing a page.
@return	end of log record or nullptr */

byte *btr_parse_page_reorganize(
  byte *ptr, /*!< in: buffer */
  byte *end_ptr __attribute__((unused)),
  /*!< in: buffer end */
  dict_index_t *dict_index, /*!< in: record descriptor */
  buf_block_t *block,       /*!< in: page to be reorganized, or nullptr */
  mtr_t *mtr
) /*!< in: mtr or nullptr */
{
  ut_ad(ptr && end_ptr);

  /* The record is empty, except for the record initial part */

  if (likely(block != nullptr)) {
    btr_page_reorganize_low(true, block, dict_index, mtr);
  }

  return (ptr);
}

/** Empties an index page.  @see btr_page_create().
@param[in,out] block            Page to emptied.
@param[out] ignored
@param[in] level                Btree level of the page
@param[in,out] mtr              Min-transaction covering the operation.  */
static void btr_page_empty(buf_block_t *block, dict_index_t *dict_index, ulint level, mtr_t *mtr) {
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  /* Recreate the page: note that global data on page (possible
  segment headers, next page-field, etc.) is preserved intact */

  page_create(block, mtr);
  btr_page_set_level(page, level, mtr);

  block->m_check_index_page_at_flush = true;
}

rec_t *btr_root_raise_and_insert(btr_cur_t *cursor, const dtuple_t *tuple, ulint n_ext, mtr_t *mtr) {
  dict_index_t *dict_index;
  page_t *root;
  page_t *new_page;
  ulint new_page_no;
  rec_t *rec;
  mem_heap_t *heap;
  dtuple_t *node_ptr;
  ulint level;
  rec_t *node_ptr_rec;
  page_cur_t *page_cursor;
  buf_block_t *root_block;
  buf_block_t *new_block;

  root = btr_cur_get_page(cursor);
  root_block = btr_cur_get_block(cursor);
  dict_index = btr_cur_get_index(cursor);
#ifdef UNIV_BTR_DEBUG
  ulint space = dict_index_get_space(dict_index);

  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));

  ut_a(dict_index_get_page(dict_index) == page_get_page_no(root));
#endif /* UNIV_BTR_DEBUG */
  ut_ad(mtr->memo_contains(dict_index_get_lock(dict_index), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(root_block, MTR_MEMO_PAGE_X_FIX));

  /* Allocate a new page to the tree. Root splitting is done by first
  moving the root records to the new page, emptying the root, putting
  a node pointer to the new page, and then splitting the new page. */

  level = btr_page_get_level(root, mtr);

  new_block = btr_page_alloc(dict_index, 0, FSP_NO_DIR, level, mtr);
  new_page = new_block->get_frame();

  btr_page_create(new_block, dict_index, level, mtr);

  /* Set the next node and previous node fields of new page */
  btr_page_set_next(new_page, FIL_NULL, mtr);
  btr_page_set_prev(new_page, FIL_NULL, mtr);

  /* Copy the records from root to the new page one by one. */
  auto success{page_copy_rec_list_end(new_block, root_block, page_get_infimum_rec(root), dict_index, mtr)};
  ut_a(success);

  /* If this is a pessimistic insert which is actually done to
  perform a pessimistic update then we have stored the lock
  information of the record to be inserted on the infimum of the
  root page: we cannot discard the lock structs on the root page */

  lock_update_root_raise(new_block, root_block);

  /* Create a memory heap where the node pointer is stored */
  heap = mem_heap_create(100);

  rec = page_rec_get_next(page_get_infimum_rec(new_page));
  new_page_no = new_block->get_page_no();

  /* Build the node pointer (= node key and page address) for the
  child */

  node_ptr = dict_index_build_node_ptr(dict_index, rec, new_page_no, heap, level);
  /* The node pointer must be marked as the predefined minimum record,
  as there is no lower alphabetical limit to records in the leftmost
  node of a level: */
  dtuple_set_info_bits(node_ptr, dtuple_get_info_bits(node_ptr) | REC_INFO_MIN_REC_FLAG);

  /* Rebuild the root page to get free space */
  btr_page_empty(root_block, dict_index, level + 1, mtr);

  btr_page_set_next(root, FIL_NULL, mtr);
  btr_page_set_prev(root, FIL_NULL, mtr);

  page_cursor = btr_cur_get_page_cur(cursor);

  /* Insert node pointer to the root */

  page_cur_set_before_first(root_block, page_cursor);

  node_ptr_rec = page_cur_tuple_insert(page_cursor, node_ptr, dict_index, 0, mtr);

  /* The root page should only contain the node pointer
  to new_page at this point.  Thus, the data should fit. */
  ut_a(node_ptr_rec);

  /* Free the memory heap */
  mem_heap_free(heap);

  /* Reposition the cursor to the child node */
  page_cur_search(new_block, dict_index, tuple, PAGE_CUR_LE, page_cursor);

  /* Split the child and insert tuple */
  return (btr_page_split_and_insert(cursor, tuple, n_ext, mtr));
}

/** Decides if the page should be split at the convergence point of inserts
converging to the left.
@return	true if split recommended */

bool btr_page_get_split_rec_to_left(
  btr_cur_t *cursor, /*!< in: cursor at which to insert */
  rec_t **split_rec
) /*!< out: if split recommended,
                    the first record on upper half page,
                    or nullptr if tuple to be inserted should
                    be first */
{
  page_t *page;
  rec_t *insert_point;
  rec_t *infimum;

  page = btr_cur_get_page(cursor);
  insert_point = btr_cur_get_rec(cursor);

  if (page_header_get_ptr(page, PAGE_LAST_INSERT) == page_rec_get_next(insert_point)) {

    infimum = page_get_infimum_rec(page);

    /* If the convergence is in the middle of a page, include also
    the record immediately before the new insert to the upper
    page. Otherwise, we could repeatedly move from page to page
    lots of records smaller than the convergence point. */

    if (infimum != insert_point && page_rec_get_next(infimum) != insert_point) {

      *split_rec = insert_point;
    } else {
      *split_rec = page_rec_get_next(insert_point);
    }

    return (true);
  }

  return (false);
}

/** Decides if the page should be split at the convergence point of inserts
converging to the right.
@return	true if split recommended */

bool btr_page_get_split_rec_to_right(
  btr_cur_t *cursor, /*!< in: cursor at which to insert */
  rec_t **split_rec
) /*!< out: if split recommended,
                    the first record on upper half page,
                    or nullptr if tuple to be inserted should
                    be first */
{
  page_t *page;
  rec_t *insert_point;

  page = btr_cur_get_page(cursor);
  insert_point = btr_cur_get_rec(cursor);

  /* We use eager heuristics: if the new insert would be right after
  the previous insert on the same page, we assume that there is a
  pattern of sequential inserts here. */

  if (likely(page_header_get_ptr(page, PAGE_LAST_INSERT) == insert_point)) {

    rec_t *next_rec;

    next_rec = page_rec_get_next(insert_point);

    if (page_rec_is_supremum(next_rec)) {
    split_at_new:
      /* Split at the new record to insert */
      *split_rec = nullptr;
    } else {
      rec_t *next_next_rec = page_rec_get_next(next_rec);
      if (page_rec_is_supremum(next_next_rec)) {

        goto split_at_new;
      }

      /* If there are >= 2 user records up from the insert
      point, split all but 1 off. We want to keep one because
      then sequential inserts can use the adaptive hash
      index, as they can do the necessary checks of the right
      search position just by looking at the records on this
      page. */

      *split_rec = next_next_rec;
    }

    return (true);
  }

  return (false);
}

/**
 * Calculates a split record such that the tuple will certainly fit on
 * its half-page when the split is performed. We assume in this function
 * only that the cursor page has at least one user record.
 * 
 * @param cursor The cursor at which the insert should be made.
 * @param tuple The tuple to insert.
 * @param n_ext Number of externally stored columns.
 * 
 * @return split record, or nullptr if tuple will be the first record on
 *  the lower or upper half-page (determined by btr_page_tuple_smaller()) */
static rec_t *btr_page_get_split_rec(btr_cur_t *cursor, const dtuple_t *tuple, ulint n_ext) {
  auto page = btr_cur_get_page(cursor);
  auto insert_size = rec_get_converted_size(cursor->m_index, tuple, n_ext);
  auto free_space = page_get_free_space_of_empty();

  /* free_space is now the free space of a created new page */

  auto total_data = page_get_data_size(page) + insert_size;
  auto total_n_recs = page_get_n_recs(page) + 1;
  ut_ad(total_n_recs >= 2);
  auto total_space = total_data + page_dir_calc_reserved_space(total_n_recs);

  ulint n = 0;
  ulint incl_data = 0;
  ulint *offsets = nullptr;
  mem_heap_t *heap = nullptr;
  auto ins_rec = btr_cur_get_rec(cursor);
  auto rec = page_get_infimum_rec(page);

  /* We start to include records to the left half, and when the
  space reserved by them exceeds half of total_space, then if
  the included records fit on the left page, they will be put there
  if something was left over also for the right page,
  otherwise the last included record will be the first on the right
  half page */

  do {
    /* Decide the next record to include */
    if (rec == ins_rec) {

      /* nullptr denotes that tuple is now included */
      rec = nullptr;

    } else if (rec == nullptr) {
      rec = page_rec_get_next(ins_rec);
    } else {
      rec = page_rec_get_next(rec);
    }

    if (rec == nullptr) {
      /* Include tuple */
      incl_data += insert_size;
    } else {
      Phy_rec record{cursor->m_index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});

      incl_data += rec_offs_size(offsets);
    }

    ++n;

  } while (incl_data + page_dir_calc_reserved_space(n) < total_space / 2);

  if (incl_data + page_dir_calc_reserved_space(n) <= free_space) {
    /* The next record will be the first on the right half page
    if it is not the supremum record of page */

    rec_t *next_rec;

    if (rec == ins_rec) {
      rec = nullptr;

      goto func_exit;
    } else if (rec == nullptr) {
      next_rec = page_rec_get_next(ins_rec);
    } else {
      next_rec = page_rec_get_next(rec);
    }

    ut_ad(next_rec != nullptr);

    if (!page_rec_is_supremum(next_rec)) {
      rec = next_rec;
    }
  }

func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return (rec);
}

/**
 * Returns true if the insert fits on the appropriate half-page with the
 * chosen split_rec.
 *
 * @param cursor The cursor at which the insert should be made.
 * @param split_rec Suggestion for the first record on the upper half-page,
 *   or nullptr if the tuple to be inserted should be first.
 * @param offsets Offsets of the split_rec record.
 * @param tuple The tuple to insert.
 * @param n_ext Number of externally stored columns.
 * @param heap Temporary memory heap.
 * @return	true if fits
 */
static bool btr_page_insert_fits(btr_cur_t *cursor, const rec_t *split_rec, const ulint *offsets, const dtuple_t *tuple, ulint n_ext, mem_heap_t *heap) {
  ulint *offs;
  const rec_t *rec;
  const rec_t *end_rec;

  auto page = btr_cur_get_page(cursor);

  ut_ad(!split_rec == !offsets);
  ut_ad(!offsets || rec_offs_validate(split_rec, cursor->m_index, offsets));

  auto insert_size = rec_get_converted_size(cursor->m_index, tuple, n_ext);
  auto free_space = page_get_free_space_of_empty();

  /* free_space is now the free space of a created new page */

  auto total_data = page_get_data_size(page) + insert_size;
  auto total_n_recs = page_get_n_recs(page) + 1;

  /* We determine which records (from rec to end_rec, not including
  end_rec) will end up on the other half page from tuple when it is
  inserted. */

  if (split_rec == nullptr) {
    rec = page_rec_get_next(page_get_infimum_rec(page));
    end_rec = page_rec_get_next(btr_cur_get_rec(cursor));

  } else if (cmp_dtuple_rec(cursor->m_index->cmp_ctx, tuple, split_rec, offsets) >= 0) {

    rec = page_rec_get_next(page_get_infimum_rec(page));
    end_rec = split_rec;
  } else {
    rec = split_rec;
    end_rec = page_get_supremum_rec(page);
  }

  if (total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space) {

    /* Ok, there will be enough available space on the
    half page where the tuple is inserted */

    return (true);
  }

  offs = nullptr;

  while (rec != end_rec) {
    Phy_rec record{cursor->m_index, rec};

    /* In this loop we calculate the amount of reserved
    space after rec is removed from page. */

    offs = record.get_col_offsets(offs, ULINT_UNDEFINED, &heap, Source_location{});

    total_data -= rec_offs_size(offs);
    total_n_recs--;

    if (total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space) {

      /* Ok, there will be enough available space on the
      half page where the tuple is inserted */

      return true;
    }

    rec = page_rec_get_next_const(rec);
  }

  return false;
}

/** Inserts a data tuple to a tree on a non-leaf level. It is assumed
that mtr holds an x-latch on the tree. */

void btr_insert_on_non_leaf_level_func(
  dict_index_t *dict_index, /*!< in: index */
  ulint level,              /*!< in: level, must be > 0 */
  dtuple_t *tuple,          /*!< in: the record to be inserted */
  const char *file,         /*!< in: file name */
  ulint line,               /*!< in: line where called */
  mtr_t *mtr
) /*!< in: mtr */
{
  big_rec_t *dummy_big_rec;
  btr_cur_t cursor;
  ulint err;
  rec_t *rec;

  ut_ad(level > 0);

  btr_cur_search_to_nth_level(dict_index, level, tuple, PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, &cursor, 0, file, line, mtr);

  err = btr_cur_pessimistic_insert(
    BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG | BTR_NO_UNDO_LOG_FLAG, &cursor, tuple, &rec, &dummy_big_rec, 0, nullptr, mtr
  );
  ut_a(err == DB_SUCCESS);
}

/** Attaches the halves of an index page on the appropriate level in an
index tree. */
static void btr_attach_half_pages(
  dict_index_t *dict_index, /*!< in: the index tree */
  buf_block_t *block,       /*!< in/out: page to be split */
  rec_t *split_rec,         /*!< in: first record on upper
                                                half page */
  buf_block_t *new_block,   /*!< in/out: the new half page */
  ulint direction,          /*!< in: FSP_UP or FSP_DOWN */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint space;
  ulint prev_page_no;
  ulint next_page_no;
  ulint level;
  page_t *page = block->get_frame();
  page_t *lower_page;
  page_t *upper_page;
  ulint lower_page_no;
  ulint upper_page_no;
  dtuple_t *node_ptr_upper;
  mem_heap_t *heap;

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains(new_block, MTR_MEMO_PAGE_X_FIX));

  /* Create a memory heap where the data tuple is stored */
  heap = mem_heap_create(1024);

  /* Based on split direction, decide upper and lower pages */
  if (direction == FSP_DOWN) {

    btr_cur_t cursor;
    ulint *offsets;

    lower_page = new_block->get_frame();
    lower_page_no = new_block->get_page_no();
    upper_page = block->get_frame();
    upper_page_no = block->get_page_no();

    /* Look up the index for the node pointer to page */
    offsets = btr_page_get_father_block(nullptr, heap, dict_index, block, mtr, &cursor);

    /* Replace the address of the old child node (= page) with the
    address of the new lower half */

    btr_node_ptr_set_child_page_no(btr_cur_get_rec(&cursor), offsets, lower_page_no, mtr);
    mem_heap_empty(heap);
  } else {
    lower_page = block->get_frame();
    lower_page_no = block->get_page_no();
    upper_page = new_block->get_frame();
    upper_page_no = new_block->get_page_no();
  }

  /* Get the level of the split pages */
  level = btr_page_get_level(block->get_frame(), mtr);
  ut_ad(level == btr_page_get_level(new_block->get_frame(), mtr));

  /* Build the node pointer (= node key and page address) for the upper
  half */

  node_ptr_upper = dict_index_build_node_ptr(dict_index, split_rec, upper_page_no, heap, level);

  /* Insert it next to the pointer to the lower half. Note that this
  may generate recursion leading to a split on the higher level. */

  btr_insert_on_non_leaf_level(dict_index, level + 1, node_ptr_upper, mtr);

  /* Free the memory heap */
  mem_heap_free(heap);

  /* Get the previous and next pages of page */

  prev_page_no = btr_page_get_prev(page, mtr);
  next_page_no = btr_page_get_next(page, mtr);
  space = block->get_space();

  /* Update page links of the level */

  if (prev_page_no != FIL_NULL) {
    auto prev_block = btr_block_get(space, prev_page_no, RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_next(prev_block->get_frame(), mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

    btr_page_set_next(prev_block->get_frame(), lower_page_no, mtr);
  }

  if (next_page_no != FIL_NULL) {
    auto next_block = btr_block_get(space, next_page_no, RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_prev(next_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    btr_page_set_prev(next_block->get_frame(), upper_page_no, mtr);
  }

  btr_page_set_prev(lower_page, prev_page_no, mtr);
  btr_page_set_next(lower_page, upper_page_no, mtr);

  btr_page_set_prev(upper_page, lower_page_no, mtr);
  btr_page_set_next(upper_page, next_page_no, mtr);
}

/** Determine if a tuple is smaller than any record on the page.
@return true if smaller */
static bool btr_page_tuple_smaller(
  btr_cur_t *cursor,     /*!< in: b-tree cursor */
  const dtuple_t *tuple, /*!< in: tuple to consider */
  ulint *offsets,        /*!< in/out: temporary storage */
  ulint n_uniq,          /*!< in: number of unique fields
                                              in the index page records */
  mem_heap_t **heap
) /*!< in/out: heap for offsets */
{
  buf_block_t *block;
  const rec_t *first_rec;
  page_cur_t pcur;

  /* Read the first user record in the page. */
  block = btr_cur_get_block(cursor);
  page_cur_set_before_first(block, &pcur);
  page_cur_move_to_next(&pcur);
  first_rec = page_cur_get_rec(&pcur);

  {
    Phy_rec record{cursor->m_index, first_rec};

    offsets = record.get_col_offsets(offsets, n_uniq, heap, Source_location{});
  }

  return cmp_dtuple_rec(cursor->m_index->cmp_ctx, tuple, first_rec, offsets) < 0;
}

rec_t *btr_page_split_and_insert(btr_cur_t *cursor, const dtuple_t *tuple, ulint n_ext, mtr_t *mtr) {
  byte *buf{};
  buf_block_t *block;
  page_t *page;
  ulint page_no;
  byte direction;
  ulint hint_page_no;
  buf_block_t *new_block;
  rec_t *split_rec;
  buf_block_t *left_block;
  buf_block_t *right_block;
  buf_block_t *insert_block;
  page_cur_t *page_cursor;
  rec_t *first_rec;
  rec_t *move_limit;
  bool insert_will_fit;
  bool insert_left;
  ulint n_iterations = 0;
  rec_t *rec;
  mem_heap_t *heap;
  ulint n_uniq;
  ulint *offsets;

  heap = mem_heap_create(1024);
  n_uniq = dict_index_get_n_unique_in_tree(cursor->m_index);
func_start:
  mem_heap_empty(heap);
  offsets = nullptr;

  ut_ad(mtr->memo_contains(dict_index_get_lock(cursor->m_index), MTR_MEMO_X_LOCK));
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(dict_index_get_lock(cursor->index), RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  block = btr_cur_get_block(cursor);
  page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  ut_ad(page_get_n_recs(page) >= 1);

  page_no = block->get_page_no();

  /* 1. Decide the split record; split_rec == nullptr means that the
  tuple to be inserted should be the first record on the upper
  half-page */
  insert_left = false;

  if (n_iterations > 0) {
    direction = FSP_UP;
    hint_page_no = page_no + 1;
    split_rec = btr_page_get_split_rec(cursor, tuple, n_ext);

    if (unlikely(split_rec == nullptr)) {
      insert_left = btr_page_tuple_smaller(cursor, tuple, offsets, n_uniq, &heap);
    }
  } else if (btr_page_get_split_rec_to_right(cursor, &split_rec)) {
    direction = FSP_UP;
    hint_page_no = page_no + 1;

  } else if (btr_page_get_split_rec_to_left(cursor, &split_rec)) {
    direction = FSP_DOWN;
    hint_page_no = page_no - 1;
    ut_ad(split_rec);
  } else {
    direction = FSP_UP;
    hint_page_no = page_no + 1;

    /* If there is only one record in the index page, we
    can't split the node in the middle by default. We need
    to determine whether the new record will be inserted
    to the left or right. */

    if (page_get_n_recs(page) > 1) {
      split_rec = page_get_middle_rec(page);
    } else if (btr_page_tuple_smaller(cursor, tuple, offsets, n_uniq, &heap)) {
      split_rec = page_rec_get_next(page_get_infimum_rec(page));
    } else {
      split_rec = nullptr;
    }
  }

  /* 2. Allocate a new page to the index */
  new_block = btr_page_alloc(cursor->m_index, hint_page_no, direction, btr_page_get_level(page, mtr), mtr);
  btr_page_create(new_block, cursor->m_index, btr_page_get_level(page, mtr), mtr);

  /* 3. Calculate the first record on the upper half-page, and the
  first record (move_limit) on original page which ends up on the
  upper half */

  if (split_rec != nullptr) {
    first_rec = move_limit = split_rec;

    Phy_rec record{cursor->m_index, split_rec};

    offsets = record.get_col_offsets(offsets, n_uniq, &heap, Source_location{});

    insert_left = cmp_dtuple_rec(cursor->m_index->cmp_ctx, tuple, split_rec, offsets) < 0;
  } else {
    ut_ad(!split_rec);
    buf = (byte *)mem_alloc(rec_get_converted_size(cursor->m_index, tuple, n_ext));

    first_rec = rec_convert_dtuple_to_rec(buf, cursor->m_index, tuple, n_ext);
    move_limit = page_rec_get_next(btr_cur_get_rec(cursor));
  }

  /* 4. Do first the modifications in the tree structure */

  btr_attach_half_pages(cursor->m_index, block, first_rec, new_block, direction, mtr);

  /* If the split is made on the leaf level and the insert will fit
  on the appropriate half-page, we may release the tree x-latch.
  We can then move the records after releasing the tree latch,
  thus reducing the tree latch contention. */

  if (split_rec != nullptr) {
    insert_will_fit = btr_page_insert_fits(cursor, split_rec, offsets, tuple, n_ext, heap);
  } else {
    mem_free(buf);
    insert_will_fit = btr_page_insert_fits(cursor, nullptr, nullptr, tuple, n_ext, heap);
  }

  if (insert_will_fit && page_is_leaf(page)) {

    mtr->memo_release(dict_index_get_lock(cursor->m_index), MTR_MEMO_X_LOCK);
  }

  /* 5. Move then the records to the new page */
  if (direction == FSP_DOWN
#ifdef UNIV_BTR_AVOID_COPY
      && page_rec_is_supremum(move_limit)) {
    /* Instead of moving all records, make the new page
    the empty page. */

    left_block = block;
    right_block = new_block;
  } else if (
    direction == FSP_DOWN
#endif /* UNIV_BTR_AVOID_COPY */
  ) {
    auto success{page_move_rec_list_start(new_block, block, move_limit, cursor->m_index, mtr)};
    ut_a(success);

    left_block = new_block;
    right_block = block;

    lock_update_split_left(right_block, left_block);
#ifdef UNIV_BTR_AVOID_COPY
  } else if (split_rec == nullptr) {
    /* Instead of moving all records, make the new page the empty page. */

    left_block = new_block;
    right_block = block;
#endif /* UNIV_BTR_AVOID_COPY */
  } else {
    auto success{page_move_rec_list_end(new_block, block, move_limit, cursor->m_index, mtr)};
    ut_a(success);

    left_block = block;
    right_block = new_block;
    lock_update_split_right(right_block, left_block);
  }

  /* At this point, split_rec, move_limit and first_rec may point
  to garbage on the old page. */

  /* 6. The split and the tree modification is now completed. Decide the
  page where the tuple should be inserted */

  if (insert_left) {
    insert_block = left_block;
  } else {
    insert_block = right_block;
  }

  /* 7. Reposition the cursor for insert and try insertion */
  page_cursor = btr_cur_get_page_cur(cursor);

  page_cur_search(insert_block, cursor->m_index, tuple, PAGE_CUR_LE, page_cursor);

  rec = page_cur_tuple_insert(page_cursor, tuple, cursor->m_index, n_ext, mtr);

  if (likely(rec != nullptr)) {

    goto func_exit;
  }

  /* 8. If insert did not fit, try page reorganization */

  if (unlikely(!btr_page_reorganize(insert_block, cursor->m_index, mtr))) {

    goto insert_failed;
  }

  page_cur_search(insert_block, cursor->m_index, tuple, PAGE_CUR_LE, page_cursor);
  rec = page_cur_tuple_insert(page_cursor, tuple, cursor->m_index, n_ext, mtr);

  if (unlikely(rec == nullptr)) {
    /* The insert did not fit on the page: loop back to the
    start of the function for a new split */
  insert_failed:
    n_iterations++;
    ut_ad(n_iterations < 2);
    ut_ad(!insert_will_fit);

    goto func_start;
  }

func_exit:
  ut_ad(page_validate(left_block->get_frame(), cursor->m_index));
  ut_ad(page_validate(right_block->get_frame(), cursor->m_index));

  mem_heap_free(heap);
  return rec;
}

/** Removes a page from the level list of pages. */
static void btr_level_list_remove(
  ulint space,         /*!< in: space where removed */
  ulint, page_t *page, /*!< in: page to remove */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint prev_page_no;
  ulint next_page_no;

  ut_ad(page && mtr);
  ut_ad(mtr->memo_contains_page(page, MTR_MEMO_PAGE_X_FIX));
  ut_ad(space == page_get_space_id(page));
  /* Get the previous and next page numbers of page */

  prev_page_no = btr_page_get_prev(page, mtr);
  next_page_no = btr_page_get_next(page, mtr);

  /* Update page links of the level */

  if (prev_page_no != FIL_NULL) {
    auto prev_block = btr_block_get(space, prev_page_no, RW_X_LATCH, mtr);
    page_t *prev_page = prev_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_next(prev_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    btr_page_set_next(prev_page, next_page_no, mtr);
  }

  if (next_page_no != FIL_NULL) {
    auto next_block = btr_block_get(space, next_page_no, RW_X_LATCH, mtr);
    auto next_page = next_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_prev(next_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    btr_page_set_prev(next_page, prev_page_no, mtr);
  }
}

/** Writes the redo log record for setting an index record as the predefined
minimum record. */
static void btr_set_min_rec_mark_log(
  rec_t *rec, /*!< in: record */
  mlog_type_t type,  /*!< in: MLOG_COMP_REC_MIN_MARK or MLOG_REC_MIN_MARK */
  mtr_t *mtr
) /*!< in: mtr */
{
  mlog_write_initial_log_record(rec, type, mtr);

  /* Write rec offset as a 2-byte ulint */
  mlog_catenate_ulint(mtr, page_offset(rec), MLOG_2BYTES);
}

byte *btr_parse_set_min_rec_mark(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) {
  if (end_ptr < ptr + 2) {

    return (nullptr);
  }

  if (page != nullptr) {

    auto rec = page + mach_read_from_2(ptr);

    btr_set_min_rec_mark(rec, mtr);
  }

  return ptr + 2;
}

void btr_set_min_rec_mark(rec_t *rec, mtr_t *mtr) {
  const auto info_bits = rec_get_info_bits(rec);

  rec_set_info_bits(rec, info_bits | REC_INFO_MIN_REC_FLAG);

  btr_set_min_rec_mark_log(rec, MLOG_REC_MIN_MARK, mtr);
}

void btr_node_ptr_delete(dict_index_t *dict_index, buf_block_t *block, mtr_t *mtr) {
  db_err err;
  btr_cur_t cursor;

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  /* Delete node pointer on father page */
  btr_page_get_father(dict_index, block, mtr, &cursor);

  btr_cur_pessimistic_delete(&err, true, &cursor, RB_NONE, mtr);
  ut_a(err == DB_SUCCESS);
}

/**
 * If page is the only on its level, this function moves its records to the
 * father page, thus reducing the tree height.
 * 
 * @param[in] dict_index Index tree
 * @param[in] block Page which is the only on its level; must not be empty;
 *  use btr_discard_only_page_on_level if the last record from the page should be removed
 */
static void btr_lift_page_up(dict_index_t *dict_index, buf_block_t *block, mtr_t *mtr) {
  buf_block_t *father_block;
  IF_DEBUG(page_t *father_page;)
  ulint page_level;
  page_t *page = block->get_frame();
  ulint root_page_no;
  buf_block_t *blocks[BTR_MAX_DEPTH];
  ulint n_blocks; /*!< last used index in blocks[] */
  ulint i;

  ut_ad(btr_page_get_prev(page, mtr) == FIL_NULL);
  ut_ad(btr_page_get_next(page, mtr) == FIL_NULL);
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  page_level = btr_page_get_level(page, mtr);
  root_page_no = dict_index_get_page(dict_index);

  {
    btr_cur_t cursor;
    mem_heap_t *heap = mem_heap_create(100);
    ulint *offsets;
    buf_block_t *b;

    offsets = btr_page_get_father_block(nullptr, heap, dict_index, block, mtr, &cursor);
    father_block = btr_cur_get_block(&cursor);
    IF_DEBUG(father_page = father_block->get_frame();)

    n_blocks = 0;

    /* Store all ancestor pages so we can reset their
    levels later on.  We have to do all the searches on
    the tree now because later on, after we've replaced
    the first level, the tree is in an inconsistent state
    and can not be searched. */
    for (b = father_block; b->get_page_no() != root_page_no;) {
      ut_a(n_blocks < BTR_MAX_DEPTH);

      offsets = btr_page_get_father_block(offsets, heap, dict_index, b, mtr, &cursor);

      blocks[n_blocks++] = b = btr_cur_get_block(&cursor);
    }

    mem_heap_free(heap);
  }

  /* Make the father empty */
  btr_page_empty(father_block, dict_index, page_level, mtr);

  lock_update_copy_and_discard(father_block, block);

  /* Go upward to root page, decrementing levels by one. */
  for (i = 0; i < n_blocks; i++, page_level++) {
    auto page = blocks[i]->get_frame();

    ut_ad(btr_page_get_level(page, mtr) == page_level + 1);

    btr_page_set_level(page, page_level, mtr);
  }

  btr_page_free(dict_index, block, mtr);

  ut_ad(page_validate(father_page, dict_index));
  ut_ad(btr_check_node_ptr(dict_index, father_block, mtr));
}

bool btr_compress(btr_cur_t *cursor, mtr_t *mtr) {
  dict_index_t *dict_index;
  ulint space;
  ulint left_page_no;
  ulint right_page_no;
  buf_block_t *merge_block;
  page_t *merge_page;
  bool is_left;
  buf_block_t *block;
  page_t *page;
  btr_cur_t father_cursor;
  mem_heap_t *heap;
  ulint *offsets;
  ulint data_size;
  ulint n_recs;
  ulint max_ins_size;
  ulint max_ins_size_reorg;

  block = btr_cur_get_block(cursor);
  page = btr_cur_get_page(cursor);
  dict_index = btr_cur_get_index(cursor);

  ut_ad(mtr->memo_contains(dict_index_get_lock(dict_index), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  space = dict_index_get_space(dict_index);

  left_page_no = btr_page_get_prev(page, mtr);
  right_page_no = btr_page_get_next(page, mtr);

  heap = mem_heap_create(100);
  offsets = btr_page_get_father_block(nullptr, heap, dict_index, block, mtr, &father_cursor);

  /* Decide the page to which we try to merge and which will inherit
  the locks */

  is_left = left_page_no != FIL_NULL;

  if (is_left) {

    merge_block = btr_block_get(space, left_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_next(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else if (right_page_no != FIL_NULL) {

    merge_block = btr_block_get(space, right_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_prev(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else {
    /* The page is the only one on the level, lift the records to the father */
    btr_lift_page_up(dict_index, block, mtr);
    mem_heap_free(heap);
    return (true);
  }

  n_recs = page_get_n_recs(page);
  data_size = page_get_data_size(page);
#ifdef UNIV_BTR_DEBUG
#endif /* UNIV_BTR_DEBUG */

  max_ins_size_reorg = page_get_max_insert_size_after_reorganize(merge_page, n_recs);
  if (data_size > max_ins_size_reorg) {

    /* No space for merge */
  err_exit:
    mem_heap_free(heap);
    return (false);
  }

  ut_ad(page_validate(merge_page, dict_index));

  max_ins_size = page_get_max_insert_size(merge_page, n_recs);

  if (unlikely(data_size > max_ins_size)) {

    /* We have to reorganize merge_page */

    if (unlikely(!btr_page_reorganize(merge_block, dict_index, mtr))) {

      goto err_exit;
    }

    max_ins_size = page_get_max_insert_size(merge_page, n_recs);

    ut_ad(page_validate(merge_page, dict_index));
    ut_ad(max_ins_size == max_ins_size_reorg);

    if (unlikely(data_size > max_ins_size)) {

      /* Add fault tolerance, though this should
      never happen */

      goto err_exit;
    }
  }

  /* Move records to the merge page */
  if (is_left) {
    rec_t *orig_pred = page_copy_rec_list_start(merge_block, block, page_get_supremum_rec(page), dict_index, mtr);

    if (unlikely(!orig_pred)) {
      goto err_exit;
    }

    /* Remove the page from the level list */
    btr_level_list_remove(space, 0, page, mtr);

    btr_node_ptr_delete(dict_index, block, mtr);
    lock_update_merge_left(merge_block, orig_pred, block);
  } else {
    auto orig_succ = page_copy_rec_list_end(merge_block, block, page_get_infimum_rec(page), cursor->m_index, mtr);

    ut_a(orig_succ == nullptr);

    /* Remove the page from the level list */
    btr_level_list_remove(space, 0, page, mtr);

    /* Replace the address of the old child node (= page) with the
    address of the merge page to the right */

    btr_node_ptr_set_child_page_no(btr_cur_get_rec(&father_cursor), offsets, right_page_no, mtr);
    btr_node_ptr_delete(dict_index, merge_block, mtr);

    lock_update_merge_right(merge_block, orig_succ, block);
  }

  mem_heap_free(heap);

  ut_ad(page_validate(merge_page, dict_index));

  btr_page_free(dict_index, block, mtr);

  ut_ad(btr_check_node_ptr(dict_index, merge_block, mtr));

  return true;
}

/** Discards a page that is the only page on its level.  This will empty
the whole B-tree, leaving just an empty root page.  This function
should never be reached, because btr_compress(), which is invoked in
delete operations, calls btr_lift_page_up() to flatten the B-tree. */
static void btr_discard_only_page_on_level(
  dict_index_t *dict_index, /*!< in: index tree */
  buf_block_t *block,       /*!< in: page which is the only on its level */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint page_level = 0;

  /* Save the PAGE_MAX_TRX_ID from the leaf page. */
  auto max_trx_id = page_get_max_trx_id(block->get_frame());

  while (block->get_page_no() != dict_index_get_page(dict_index)) {
    btr_cur_t cursor;
    buf_block_t *father;
    const page_t *page = block->get_frame();

    ut_a(page_get_n_recs(page) == 1);
    ut_a(page_level == btr_page_get_level(page, mtr));
    ut_a(btr_page_get_prev(page, mtr) == FIL_NULL);
    ut_a(btr_page_get_next(page, mtr) == FIL_NULL);

    ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

    btr_page_get_father(dict_index, block, mtr, &cursor);
    father = btr_cur_get_block(&cursor);

    lock_update_discard(father, PAGE_HEAP_NO_SUPREMUM, block);

    /* Free the file page */
    btr_page_free(dict_index, block, mtr);

    block = father;
    page_level++;
  }

  /* block is the root page, which must be empty, except
  for the node pointer to the (now discarded) block(s). */

#ifdef UNIV_BTR_DEBUG
  const page_t *root = block->get_frame();
  const ulint space = dict_index_get_space(dict_index);
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(btr_root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

  if (dict_index_is_sec(dict_index) && page_is_leaf(block->get_frame())) {
    ut_a(max_trx_id > 0);
    page_set_max_trx_id(block, max_trx_id, mtr);
  }

  btr_page_empty(block, dict_index, 0, mtr);
}

void btr_discard_page(btr_cur_t *cursor, mtr_t *mtr) {
  dict_index_t *dict_index;
  ulint space;
  ulint left_page_no;
  ulint right_page_no;
  buf_block_t *merge_block;
  page_t *merge_page;
  buf_block_t *block;
  page_t *page;
  rec_t *node_ptr;

  block = btr_cur_get_block(cursor);
  dict_index = btr_cur_get_index(cursor);

  ut_ad(dict_index_get_page(dict_index) != block->get_page_no());
  ut_ad(mtr->memo_contains(dict_index_get_lock(dict_index), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  space = dict_index_get_space(dict_index);

  /* Decide the page which will inherit the locks */

  left_page_no = btr_page_get_prev(block->get_frame(), mtr);
  right_page_no = btr_page_get_next(block->get_frame(), mtr);

  if (left_page_no != FIL_NULL) {
    merge_block = btr_block_get(space, left_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_next(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else if (right_page_no != FIL_NULL) {
    merge_block = btr_block_get(space, right_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(btr_page_get_prev(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else {
    btr_discard_only_page_on_level(dict_index, block, mtr);

    return;
  }

  page = block->get_frame();

  if (left_page_no == FIL_NULL && !page_is_leaf(page)) {

    /* We have to mark the leftmost node pointer on the right
    side page as the predefined minimum record */
    node_ptr = page_rec_get_next(page_get_infimum_rec(merge_page));

    ut_ad(page_rec_is_user_rec(node_ptr));

    btr_set_min_rec_mark(node_ptr, mtr);
  }

  btr_node_ptr_delete(dict_index, block, mtr);

  /* Remove the page from the level list */
  btr_level_list_remove(space, 0, page, mtr);

  if (left_page_no != FIL_NULL) {
    lock_update_discard(merge_block, PAGE_HEAP_NO_SUPREMUM, block);
  } else {
    lock_update_discard(merge_block, lock_get_min_heap_no(merge_block), block);
  }

  /* Free the file page */
  btr_page_free(dict_index, block, mtr);

  ut_ad(btr_check_node_ptr(dict_index, merge_block, mtr));
}

#ifdef UNIV_BTR_PRINT
void btr_print_size(dict_index_t *index) /*!< in: index tree */
{
  page_t *root;
  fseg_header_t *seg;
  mtr_t mtr;

  mtr_start(&mtr);

  root = btr_root_get(index, &mtr);

  seg = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

  ib_logger(ib_stream, "INFO OF THE NON-LEAF PAGE SEGMENT\n");
  fseg_print(seg, &mtr);

  mtr.commit();
}

/** Prints recursively index tree pages. */
static void btr_print_recursive(
  dict_index_t *index, /*!< in: index tree */
  buf_block_t *block,  /*!< in: index page */
  ulint width,         /*!< in: print this many entries from start
                         and end */
  mem_heap_t **heap,   /*!< in/out: heap for Phy_rec::get_col_offsets() */
  ulint **offsets,     /*!< in/out: buffer for Phy_rec::get_col_offsets() */
  mtr_t *mtr
) /*!< in: mtr */
{
  const page_t *page = block->get_frame();
  page_cur_t cursor;
  ulint n_recs;
  ulint i = 0;
  mtr_t mtr2;

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  ib_logger(
    ib_stream, "NODE ON LEVEL %lu page number %lu\n", (ulong)btr_page_get_level(page, mtr), (ulong)block->get_page_no()
  );

  page_print(block, index, width, width);

  n_recs = page_get_n_recs(page);

  page_cur_set_before_first(block, &cursor);
  page_cur_move_to_next(&cursor);

  while (!page_cur_is_after_last(&cursor)) {

    if (page_is_leaf(page)) {

      /* If this is the leaf level, do nothing */

    } else if ((i <= width) || (i >= n_recs - width)) {

      const rec_t *node_ptr;

      mtr_start(&mtr2);

      node_ptr = page_cur_get_rec(&cursor);

      {
        Phy_rec record{index, node_ptr};

        *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, heap, Source_location{});
      }

      btr_print_recursive(index, btr_node_ptr_get_child(node_ptr, index, *offsets, &mtr2), width, heap, offsets, &mtr2);
      mtr2.commit();
    }

    page_cur_move_to_next(&cursor);
    i++;
  }
}

/** Prints directories and other info of all nodes in the tree. */

void btr_print_index(
  dict_index_t *index, /*!< in: index */
  ulint width
) /*!< in: print this many entries from start
                                  and end */
{
  mtr_t mtr;
  buf_block_t *root;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ib_logger(
    ib_stream,
    "--------------------------\n"
    "INDEX TREE PRINT\n"
  );

  mtr_start(&mtr);

  root = btr_root_block_get(index, &mtr);

  btr_print_recursive(index, root, width, &heap, &offsets, &mtr);
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  mtr.commit();

  btr_validate_index(index, nullptr);
}
#endif /* UNIV_BTR_PRINT */

#ifdef UNIV_DEBUG
/** Checks that the node pointer to a page is appropriate.
@return	true */

bool btr_check_node_ptr(
  dict_index_t *index, /*!< in: index tree */
  buf_block_t *block,  /*!< in: index page */
  mtr_t *mtr
) /*!< in: mtr */
{
  mem_heap_t *heap;
  dtuple_t *tuple;
  ulint *offsets;
  btr_cur_t cursor;
  page_t *page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  if (dict_index_get_page(index) == block->get_page_no()) {

    return (true);
  }

  heap = mem_heap_create(256);
  offsets = btr_page_get_father_block(nullptr, heap, index, block, mtr, &cursor);

  if (page_is_leaf(page)) {

    goto func_exit;
  }

  tuple = dict_index_build_node_ptr(index, page_rec_get_next(page_get_infimum_rec(page)), 0, heap, btr_page_get_level(page, mtr));

  ut_a(!cmp_dtuple_rec(index->cmp_ctx, tuple, btr_cur_get_rec(&cursor), offsets));
func_exit:
  mem_heap_free(heap);

  return (true);
}
#endif /* UNIV_DEBUG */

/** Display identification information for a record. */
static void btr_index_rec_validate_report(
  const page_t *page, /*!< in: index page */
  const rec_t *rec,   /*!< in: index record */
  const dict_index_t *dict_index
) /*!< in: index */
{
  ib_logger(ib_stream, "Record in ");
  dict_index_name_print(ib_stream, nullptr, dict_index);
  ib_logger(ib_stream, ", page %lu, at offset %lu\n", page_get_page_no(page), (ulint)page_offset(rec));
}

bool btr_index_rec_validate(const rec_t *rec, const dict_index_t *dict_index, bool dump_on_error) {
  ulint len;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  auto page = page_align(rec);
  const auto n = dict_index_get_n_fields(dict_index);

  if (unlikely(rec_get_n_fields(rec) != n)) {
    btr_index_rec_validate_report(page, rec, dict_index);

    ib_logger(ib_stream, "has %lu fields, should have %lu\n", (ulong)rec_get_n_fields(rec), (ulong)n);

    if (dump_on_error) {
      buf_page_print(page, 0);

      log_err("corrupt record ");
      rec_print(rec);
    }
    return false;
  }

  {
    Phy_rec record{dict_index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
  }

  for (ulint i = 0; i < n; i++) {
    ulint fixed_size = dict_col_get_fixed_size(dict_index_get_nth_col(dict_index, i));

    rec_get_nth_field_offs(offsets, i, &len);

    /* Note that if fixed_size != 0, it equals the
    length of a fixed-size column in the clustered index.
    A prefix index of the column is of fixed, but different
    length.  When fixed_size == 0, prefix_len is the maximum
    length of the prefix index column. */

    if ((dict_index_get_nth_field(dict_index, i)->prefix_len == 0 && len != UNIV_SQL_NULL && fixed_size && len != fixed_size) ||
        (dict_index_get_nth_field(dict_index, i)->prefix_len > 0 && len != UNIV_SQL_NULL &&
         len > dict_index_get_nth_field(dict_index, i)->prefix_len)) {

      btr_index_rec_validate_report(page, rec, dict_index);

      log_err(std::format("field {} len is {}, should be {}", i, len, fixed_size));

      if (dump_on_error) {
        buf_page_print(page, 0);

        ib_logger(ib_stream, "corrupt record ");
        rec_print(rec);
        ib_logger(ib_stream, "\n");
      }

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }

      return false;
    }
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return true;
}

/**
 * Checks the size and number of fields in records based on the definition of
 * the index.
 * @param[in] block Index page
 * @param[in] dict_index Index tree
 * @return	true if ok
 */
static bool btr_index_page_validate(buf_block_t *block, dict_index_t *dict_index) {
  page_cur_t cur;
  bool ret = true;

  page_cur_set_before_first(block, &cur);
  page_cur_move_to_next(&cur);

  for (;;) {
    if (page_cur_is_after_last(&cur)) {

      break;
    }

    if (!btr_index_rec_validate(cur.m_rec, dict_index, true)) {

      return false;
    }

    page_cur_move_to_next(&cur);
  }

  return ret;
}

/** Report an error on one page of an index tree. */
static void btr_validate_report1(
  dict_index_t *dict_index, /*!< in: index */
  ulint level,              /*!< in: B-tree level */
  const buf_block_t *block
) /*!< in: index page */
{
  ib_logger(ib_stream, "Error in page %lu of ", block->get_page_no());
  dict_index_name_print(ib_stream, nullptr, dict_index);
  if (level) {
    ib_logger(ib_stream, ", index tree level %lu", level);
  }
  ib_logger(ib_stream, "\n");
}

/** Report an error on two pages of an index tree. */
static void btr_validate_report2(
  const dict_index_t *dict_index, /*!< in: index */
  ulint level,                    /*!< in: B-tree level */
  const buf_block_t *block1,      /*!< in: first index page */
  const buf_block_t *block2
) /*!< in: second index page */
{
  ib_logger(ib_stream, "Error in pages %lu and %lu of ", block1->get_page_no(), block2->get_page_no());
  dict_index_name_print(ib_stream, nullptr, dict_index);
  if (level) {
    ib_logger(ib_stream, ", index tree level %lu", level);
  }
  ib_logger(ib_stream, "\n");
}

/** Validates index tree level.
@return	true if ok */
static bool btr_validate_level(
  dict_index_t *dict_index, /*!< in: index tree */
  trx_t *trx,               /*!< in: transaction or nullptr */
  ulint level
) /*!< in: level number */
{
  buf_block_t *block;
  page_t *page;
  buf_block_t *right_block = 0; /* remove warning */
  page_t *right_page = 0;       /* remove warning */
  page_t *father_page;
  btr_cur_t node_cur;
  btr_cur_t right_node_cur;
  rec_t *rec;
  ulint right_page_no;
  ulint left_page_no;
  page_cur_t cursor;
  dtuple_t *node_ptr_tuple;
  bool ret = true;
  mtr_t mtr;
  mem_heap_t *heap = mem_heap_create(256);
  ulint *offsets = nullptr;
  ulint *offsets2 = nullptr;

  mtr.start();

  mtr_x_lock(dict_index_get_lock(dict_index), &mtr);

  block = btr_root_block_get(dict_index, &mtr);
  page = block->get_frame();

  auto space = dict_index_get_space(dict_index);

  while (level != btr_page_get_level(page, &mtr)) {
    ut_a(space == block->get_space());
    ut_a(space == page_get_space_id(page));
    ut_a(!page_is_leaf(page));

    page_cur_set_before_first(block, &cursor);
    page_cur_move_to_next(&cursor);

    auto node_ptr = page_cur_get_rec(&cursor);

    {
      Phy_rec record{dict_index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    block = btr_node_ptr_get_child(node_ptr, dict_index, offsets, &mtr);
    page = block->get_frame();
  }

  /* Now we are on the desired level. Loop through the pages on that
  level. */
loop:
  if (trx_is_interrupted(trx)) {
    mtr.commit();
    mem_heap_free(heap);
    return (ret);
  }
  mem_heap_empty(heap);
  offsets = offsets2 = nullptr;
  mtr_x_lock(dict_index_get_lock(dict_index), &mtr);

  /* Check ordering etc. of records */

  if (!page_validate(page, dict_index)) {
    btr_validate_report1(dict_index, level, block);

    ret = false;
  } else if (level == 0) {
    /* We are on level 0. Check that the records have the right
    number of fields, and field lengths are right. */

    if (!btr_index_page_validate(block, dict_index)) {

      ret = false;
    }
  }

  ut_a(btr_page_get_level(page, &mtr) == level);

  right_page_no = btr_page_get_next(page, &mtr);
  left_page_no = btr_page_get_prev(page, &mtr);

  ut_a(page_get_n_recs(page) > 0 || (level == 0 && page_get_page_no(page) == dict_index_get_page(dict_index)));

  if (right_page_no != FIL_NULL) {
    const rec_t *right_rec;
    right_block = btr_block_get(space, right_page_no, RW_X_LATCH, &mtr);
    right_page = right_block->get_frame();
    if (unlikely(btr_page_get_prev(right_page, &mtr) != page_get_page_no(page))) {
      btr_validate_report2(dict_index, level, block, right_block);
      ib_logger(
        ib_stream,
        "broken FIL_PAGE_NEXT"
        " or FIL_PAGE_PREV links\n"
      );
      buf_page_print(page, 0);
      buf_page_print(right_page, 0);

      ret = false;
    }

    rec = page_rec_get_prev(page_get_supremum_rec(page));
    right_rec = page_rec_get_next(page_get_infimum_rec(right_page));

    {
      Phy_rec record{dict_index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Source_location{});
    }

    {
      Phy_rec record{dict_index, right_rec};

      offsets2 = record.get_col_offsets(offsets2, ULINT_UNDEFINED, &heap, Source_location{});
    }

    if (unlikely(cmp_rec_rec(rec, right_rec, offsets, offsets2, dict_index) >= 0)) {

      btr_validate_report2(dict_index, level, block, right_block);

      ib_logger(ib_stream, "records in wrong order on adjacent pages\n");

      buf_page_print(page, 0);
      buf_page_print(right_page, 0);

      ib_logger(ib_stream, "record ");
      rec = page_rec_get_prev(page_get_supremum_rec(page));
      rec_print(rec);
      ib_logger(ib_stream, "\n");
      ib_logger(ib_stream, "record ");
      rec = page_rec_get_next(page_get_infimum_rec(right_page));
      rec_print(rec);
      ib_logger(ib_stream, "\n");

      ret = false;
    }
  }

  if (level > 0 && left_page_no == FIL_NULL) {
    ut_a(REC_INFO_MIN_REC_FLAG & rec_get_info_bits(page_rec_get_next(page_get_infimum_rec(page))));
  }

  if (block->get_page_no() != dict_index_get_page(dict_index)) {

    /* Check father node pointers */

    rec_t *node_ptr;

    offsets = btr_page_get_father_block(offsets, heap, dict_index, block, &mtr, &node_cur);
    father_page = btr_cur_get_page(&node_cur);
    node_ptr = btr_cur_get_rec(&node_cur);

    btr_cur_position(dict_index, page_rec_get_prev(page_get_supremum_rec(page)), block, &node_cur);
    offsets = btr_page_get_father_node_ptr(offsets, heap, &node_cur, &mtr);

    if (unlikely(node_ptr != btr_cur_get_rec(&node_cur)) || unlikely(btr_node_ptr_get_child_page_no(node_ptr, offsets) != block->get_page_no())) {

      btr_validate_report1(dict_index, level, block);

      ib_logger(ib_stream, "node pointer to the page is wrong\n");

      buf_page_print(father_page, 0);
      buf_page_print(page, 0);

      ib_logger(ib_stream, "node ptr ");
      rec_print(node_ptr);

      rec = btr_cur_get_rec(&node_cur);
      ib_logger(
        ib_stream,
        "\n"
        "node ptr child page n:o %lu\n",
        (ulong)btr_node_ptr_get_child_page_no(rec, offsets)
      );

      ib_logger(ib_stream, "record on page ");
      rec_print(rec);
      ib_logger(ib_stream, "\n");
      ret = false;

      goto node_ptr_fails;
    }

    if (!page_is_leaf(page)) {
      node_ptr_tuple = dict_index_build_node_ptr(
        dict_index, page_rec_get_next(page_get_infimum_rec(page)), 0, heap, btr_page_get_level(page, &mtr)
      );

      if (cmp_dtuple_rec(dict_index->cmp_ctx, node_ptr_tuple, node_ptr, offsets)) {
        const rec_t *first_rec = page_rec_get_next(page_get_infimum_rec(page));

        btr_validate_report1(dict_index, level, block);

        buf_page_print(father_page, 0);
        buf_page_print(page, 0);

        log_err("Node ptrs differ on levels > 0 node ptr");
        rec_print(node_ptr);
        ib_logger(ib_stream, "first rec ");
        rec_print(first_rec);
        ib_logger(ib_stream, "\n");
        ret = false;

        goto node_ptr_fails;
      }
    }

    if (left_page_no == FIL_NULL) {
      ut_a(node_ptr == page_rec_get_next(page_get_infimum_rec(father_page)));
      ut_a(btr_page_get_prev(father_page, &mtr) == FIL_NULL);
    }

    if (right_page_no == FIL_NULL) {
      ut_a(node_ptr == page_rec_get_prev(page_get_supremum_rec(father_page)));
      ut_a(btr_page_get_next(father_page, &mtr) == FIL_NULL);
    } else {
      const rec_t *right_node_ptr = page_rec_get_next(node_ptr);

      offsets = btr_page_get_father_block(offsets, heap, dict_index, right_block, &mtr, &right_node_cur);

      if (right_node_ptr != page_get_supremum_rec(father_page)) {

        if (btr_cur_get_rec(&right_node_cur) != right_node_ptr) {
          ret = false;

          log_err("Node pointer to the right page is wrong");

          btr_validate_report1(dict_index, level, block);

          buf_page_print(father_page, 0);
          buf_page_print(page, 0);
          buf_page_print(right_page, 0);
        }
      } else {
        page_t *right_father_page = btr_cur_get_page(&right_node_cur);

        if (btr_cur_get_rec(&right_node_cur) != page_rec_get_next(page_get_infimum_rec(right_father_page))) {
          ret = false;
          log_err("Node pointer 2 to the right page is wrong");

          btr_validate_report1(dict_index, level, block);

          buf_page_print(father_page, 0);
          buf_page_print(right_father_page, 0);
          buf_page_print(page, 0);
          buf_page_print(right_page, 0);
        }

        if (page_get_page_no(right_father_page) != btr_page_get_next(father_page, &mtr)) {

          ret = false;

          log_err("Node pointer 3 to the right page is wrong");

          btr_validate_report1(dict_index, level, block);

          buf_page_print(father_page, 0);
          buf_page_print(right_father_page, 0);
          buf_page_print(page, 0);
          buf_page_print(right_page, 0);
        }
      }
    }
  }

node_ptr_fails:
  /* Commit the mini-transaction to release the latch on 'page'.
  Re-acquire the latch on right_page, which will become 'page'
  on the next loop.  The page has already been checked. */
  mtr.commit();

  if (right_page_no != FIL_NULL) {
    mtr.start();

    block = btr_block_get(space, right_page_no, RW_X_LATCH, &mtr);
    page = block->get_frame();

    goto loop;
  }

  mem_heap_free(heap);
  return ret;
}

bool btr_validate_index(dict_index_t *dict_index, trx_t *trx) {
  mtr_t mtr;

  mtr.start();

  mtr_x_lock(dict_index_get_lock(dict_index), &mtr);

  auto root = btr_root_get(dict_index, &mtr);
  auto n = btr_page_get_level(root, &mtr);

  for (ulint i = 0; i <= n && !trx_is_interrupted(trx); i++) {
    if (!btr_validate_level(dict_index, trx, n - i)) {

      mtr.commit();

      return (false);
    }
  }

  mtr.commit();

  return true;
}
