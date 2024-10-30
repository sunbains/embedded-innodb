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
#include "ut0mem.h"

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

Btree *srv_btree_sys = nullptr;

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
bool Btree::root_fseg_validate(const fseg_header_t *seg_header, space_id_t space) noexcept {
  ulint offset = mach_read_from_2(seg_header + FSEG_HDR_OFFSET);

  ut_a(mach_read_from_4(seg_header + FSEG_HDR_SPACE) == space);
  ut_a(offset >= FIL_PAGE_DATA);
  ut_a(offset <= UNIV_PAGE_SIZE - FIL_PAGE_DATA_END);

  return true;
}
#endif /* UNIV_BTR_DEBUG */

Buf_block *Btree::root_block_get(Page_id page_id, mtr_t *mtr) noexcept {
  auto block = block_get(page_id.space_id(), page_id.page_no(), RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
  const auto root = block->get_frame();

  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, page_id.space_id()));
  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, page_id.space_id()));
#endif /* UNIV_BTR_DEBUG */

  return block;
}

page_t *Btree::root_get(Page_id page_id, mtr_t *mtr) noexcept{
  return root_block_get(page_id, mtr)->get_frame();
}

rec_t *Btree::get_prev_user_rec(rec_t *rec, mtr_t *mtr) noexcept {
  if (!page_rec_is_infimum(rec)) {

    auto prev_rec = page_rec_get_prev(rec);

    if (!page_rec_is_infimum(prev_rec)) {

      return prev_rec;
    }
  }

  auto page = page_align(rec);
  page_no_t prev_page_no = page_get_prev(page, mtr);

  if (prev_page_no != FIL_NULL) {

    Buf_pool::Request  req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { page_get_space_id(page), prev_page_no },
      .m_mode = BUF_GET_NO_LATCH,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto prev_block = m_buf_pool->get(req, nullptr);
    auto prev_page = prev_block->get_frame();

    /* The caller must already have a latch to the brother */
    ut_ad(mtr->memo_contains(prev_block, MTR_MEMO_PAGE_S_FIX) ||
          mtr->memo_contains(prev_block, MTR_MEMO_PAGE_X_FIX));

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_next(prev_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    return page_rec_get_prev(page_get_supremum_rec(prev_page));

  } else {

    return nullptr;
  }
}

rec_t *Btree::get_next_user_rec(rec_t *rec, mtr_t *mtr) noexcept {
  if (!page_rec_is_supremum(rec)) {

    auto next_rec = page_rec_get_next(rec);

    if (!page_rec_is_supremum(next_rec)) {

      return next_rec;
    }
  }

  auto page = page_align(rec);
  const auto next_page_no = page_get_next(page, mtr);

  if (next_page_no != FIL_NULL) {

    Buf_pool::Request req {
      .m_rw_latch = RW_NO_LATCH,
      .m_page_id = { page_get_space_id(page), next_page_no },
      .m_mode = BUF_GET_NO_LATCH,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    const auto next_block = m_buf_pool->get(req, nullptr);
    const auto next_page = next_block->get_frame();

    /* The caller must already have a latch to the brother */
    ut_ad(mtr->memo_contains(next_block, MTR_MEMO_PAGE_S_FIX) ||
          mtr->memo_contains(next_block, MTR_MEMO_PAGE_X_FIX));

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_prev(next_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    return page_rec_get_next(page_get_infimum_rec(next_page));

  } else {

    return nullptr;
  }
}

void Btree::page_create(Buf_block *block, Index *index, ulint level, mtr_t *mtr) noexcept {
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  ::page_create(index, block, mtr);

  /* Set the level of the new index page */
  page_set_level(page, level, mtr);

  block->m_check_index_page_at_flush = true;

  page_set_index_id(page, index->m_id, mtr);
}

Buf_block *Btree::page_alloc(const Index *index, page_no_t hint_page_no, byte file_direction, ulint level, mtr_t *mtr) noexcept {
  auto root = root_get(index->m_page_id, mtr);

  fseg_header_t *seg_header;

  if (level == 0) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  } else {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;
  }

  /* Parameter true below states that the caller has made the
  reservation for free extents, and thus we know that a page can
  be allocated: */

  const auto new_page_no = m_fsp->fseg_alloc_free_page_general(seg_header, hint_page_no, file_direction, true, mtr);

  if (new_page_no == FIL_NULL) {

    return nullptr;

  } else {

    Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { index->get_space_id(), new_page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto new_block = m_buf_pool->get(req, nullptr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(new_block, SYNC_TREE_NODE_NEW));

    return new_block;
  }
}

ulint Btree::get_size(const Index *index, ulint flag) noexcept{
  ulint n;

  mtr_t mtr;

  mtr.start();

  mtr_s_lock(index->get_lock(), &mtr);

  auto root = root_get(index->m_page_id, &mtr);

  if (flag == BTR_N_LEAF_PAGES) {
    auto seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

    m_fsp->fseg_n_reserved_pages(seg_header, &n, &mtr);

  } else if (flag == BTR_TOTAL_SIZE) {
    ulint dummy;
    auto seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

    n = m_fsp->fseg_n_reserved_pages(seg_header, &dummy, &mtr);

    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

    n += m_fsp->fseg_n_reserved_pages(seg_header, &dummy, &mtr);

  } else {
    ut_error;
  }

  mtr.commit();

  return n;
}

void Btree::page_free_low(const Index *index, Buf_block *block, ulint level, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  /* The page gets invalid for optimistic searches: increment the frame
  modify clock */

  buf_block_modify_clock_inc(block);

  const auto root = root_get(index->m_page_id, mtr);

  fseg_header_t *seg_header;

  if (level == 0) {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  } else {
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;
  }

  m_fsp->fseg_free_page(seg_header, block->get_space(), block->get_page_no(), mtr);
}

void Btree::page_free(const Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  const auto level = page_get_level(block->get_frame(), mtr);

  page_free_low(index, block, level, mtr);
}

void Btree::node_ptr_set_child_page_no(rec_t *rec, const ulint *offsets, ulint page_no, mtr_t *mtr) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(!page_is_leaf(page_align(rec)));

  ulint len;

  /* The child address is in the last field */
  auto offset = rec_get_nth_field_offs(offsets, rec_offs_n_fields(offsets) - 1, &len);

  ut_ad(len == REC_NODE_PTR_SIZE);

  mlog_write_ulint(rec + offset, page_no, MLOG_4BYTES, mtr);
}

Buf_block *Btree::node_ptr_get_child(const rec_t *node_ptr, Index *index, const ulint *offsets, mtr_t *mtr) noexcept {
  ut_ad(rec_offs_validate(node_ptr, index, offsets));
  const auto space = page_get_space_id(page_align(node_ptr));
  const auto page_no = node_ptr_get_child_page_no(node_ptr, offsets);

  return block_get(space, page_no, RW_X_LATCH, mtr);
}

ulint *Btree::page_get_father_node_ptr(ulint *offsets, mem_heap_t *heap, Btree_cursor *btr_cur, mtr_t *mtr, Source_location loc) noexcept {
  const auto page_no = btr_cur->get_block()->get_page_no();
  const auto index = btr_cur->get_index();

  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));

  ut_ad(index->get_page_no() != page_no);

  const auto level = page_get_level(btr_cur->get_page_no(), mtr);
  const auto user_rec = btr_cur->get_rec();
  ut_a(page_rec_is_user_rec(user_rec));
  const auto tuple = index->build_node_ptr(user_rec, 0, heap, level);

  btr_cur->search_to_nth_level(nullptr, index, level + 1, tuple, PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, mtr, loc);

  const auto node_ptr = btr_cur->get_rec();

  {
    Phy_rec record{index, node_ptr};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (unlikely(node_ptr_get_child_page_no(node_ptr, offsets) != page_no)) {
    log_err("Dump of the child page:");
    buf_page_print(page_align(user_rec), 0);
    log_err("Dump of the parent page:");
    buf_page_print(page_align(node_ptr), 0);
    log_err("Corruption of an index tree: table ", index->m_table->m_name, ", index ", index->m_name);
    log_err(std::format(", father ptr page no {}, child page no {}", node_ptr_get_child_page_no(node_ptr, offsets), page_no));

    const auto print_rec = page_rec_get_next(page_get_infimum_rec(page_align(user_rec)));

    {
      Phy_rec record{index, print_rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    page_rec_print(print_rec, offsets);

    {
      Phy_rec record{index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    page_rec_print(node_ptr, offsets);

    log_fatal(
      "You should dump + drop + reimport the table to fix the"
      " corruption. If the crash happens at the database startup, see"
      " InnoDB website for details about forcing recovery. Then"
      " dump + drop + reimport."
    );
  }

  return offsets;
}

ulint *Btree::page_get_father_block(ulint *offsets, mem_heap_t *heap, Index *index, Buf_block *block, mtr_t *mtr, Btree_cursor *btr_cur) noexcept {
  const auto rec = page_rec_get_next(page_get_infimum_rec(block->get_frame()));

  btr_cur->position(index, rec, block);

  return page_get_father_node_ptr(offsets, heap, btr_cur, mtr, Current_location());
}

void Btree::page_get_father(Index *index, Buf_block *block, mtr_t *mtr, Btree_cursor *btr_cur) noexcept {
  auto rec = page_rec_get_next(page_get_infimum_rec(block->get_frame()));

  btr_cur->position(index, rec, block);

  auto heap = mem_heap_create(100);

  (void) page_get_father_node_ptr(nullptr, heap, btr_cur, mtr, Current_location());

  mem_heap_free(heap);
}

page_no_t Btree::create(ulint type, space_id_t space, uint64_t index_id, Index *index, mtr_t *mtr) noexcept {
  auto block = m_fsp->fseg_create(space, 0, PAGE_HEADER + PAGE_BTR_SEG_TOP, mtr);

  if (block == nullptr) {

    return FIL_NULL;
  }

  const auto page_no = block->get_page_no();

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE_NEW));

  if (!m_fsp->fseg_create(space, page_no, PAGE_HEADER + PAGE_BTR_SEG_LEAF, mtr)) {
    /* Not enough space for new segment, free root segment before return. */
    free_root(space, page_no, mtr);
    return FIL_NULL;
  }

  /* The fseg create acquires a second latch on the page,
  therefore we must declare it: */
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE_NEW));

  /* Create a new index page on the allocated segment page */
  auto page = ::page_create(index, block, mtr);

  /* Set the level of the new index page */
  page_set_level(page, 0, mtr);

  block->m_check_index_page_at_flush = true;

  /* Set the index id of the page */
  page_set_index_id(page, index_id, mtr);

  /* Set the next node and previous node fields */
  page_set_next(page, FIL_NULL, mtr);
  page_set_prev(page, FIL_NULL, mtr);

  /* In the following assertion we test that two records of maximum
  allowed size fit on the root page: this fact is needed to ensure
  correctness of split algorithms */

  ut_ad(page_get_max_insert_size(page, 2) > 2 * BTR_PAGE_MAX_REC_SIZE);

  return page_no;
}

void Btree::free_but_not_root(space_id_t space, page_no_t root_page_no) noexcept {
  mtr_t mtr;
  bool finished{};

  do {
    mtr.start();

    const auto root = page_get(space, root_page_no, RW_X_LATCH, &mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
    ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

    /* NOTE: page hash indexes are dropped when a page is freed inside fsp0fsp. */

    finished = m_fsp->fseg_free_step(root + PAGE_HEADER + PAGE_BTR_SEG_LEAF, &mtr);

    mtr.commit();

  } while (!finished);

  do {
    mtr.start();

    const auto root = page_get(space, root_page_no, RW_X_LATCH, &mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

    finished = m_fsp->fseg_free_step_not_header(root + PAGE_HEADER + PAGE_BTR_SEG_TOP, &mtr);

    mtr.commit();

  } while (!finished);
}

void Btree::free_root(space_id_t space, page_no_t root_page_no, mtr_t *mtr) noexcept {
  auto block = block_get(space, root_page_no, RW_X_LATCH, mtr);
  auto header = block->get_frame() + PAGE_HEADER + PAGE_BTR_SEG_TOP;

#ifdef UNIV_BTR_DEBUG
  ut_a(root_fseg_validate(header, space));
#endif /* UNIV_BTR_DEBUG */

  while (!m_fsp->fseg_free_step(header, mtr)) {}
}

bool Btree::page_reorganize_low(bool recovery, Buf_block *block, const Index *index, mtr_t *mtr) noexcept {
  bool success{};
  auto page{block->get_frame()};

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  const auto data_size1 = page_get_data_size(page);
  const auto max_ins_size1 = page_get_max_insert_size_after_reorganize(page, 1);

  /* Write the log record */
  mlog_open_and_write_index(mtr, page, MLOG_PAGE_REORGANIZE, 0);

  /* Turn logging off */
  const auto log_mode = mtr->set_log_mode(MTR_LOG_NONE);
  const auto temp_block = m_buf_pool->block_alloc();
  const auto temp_page = temp_block->get_frame();

  /* Copy the old page to temporary space */
  buf_frame_copy(temp_page, page);

  block->m_check_index_page_at_flush = true;

  /* Recreate the page: note that global data on page (possible
  segment headers, next page-field, etc.) is preserved intact */

  ::page_create(index, block, mtr);

  /* Copy the records from the temporary space to the recreated page;
  do not copy the lock bits yet */

  page_copy_rec_list_end_no_locks(block, temp_block, page_get_infimum_rec(temp_page), index, mtr);

  if (!index->is_clustered() && page_is_leaf(page)) {

    const auto max_trx_id = page_get_max_trx_id(temp_page);

    page_set_max_trx_id(block, max_trx_id, mtr);

    ut_ad(max_trx_id > 0 || recovery);
  }

  if (likely(!recovery)) {
    /* Update the record lock bitmaps */
    m_lock_sys->move_reorganize_page(block, temp_block);
  }

  auto data_size2 = page_get_data_size(page);
  auto max_ins_size2 = page_get_max_insert_size_after_reorganize(page, 1);

  if (unlikely(data_size1 != data_size2) || unlikely(max_ins_size1 != max_ins_size2)) {
    buf_page_print(page, 0);
    buf_page_print(temp_page, 0);

    log_err(std::format(
      "Page old data size {} new data size {} page old max ins size {} new max ins size {}"
      " Submit a detailed bug report, check the InnoDB website for details",
      data_size1, data_size2, max_ins_size1, max_ins_size2));
  } else {
    success = true;
  }

  m_buf_pool->block_free(temp_block);

  /* Restore logging mode */
  const auto old_mode = mtr->set_log_mode(log_mode);
  ut_a(old_mode == MTR_LOG_NONE);

  return success;
}

bool Btree::page_reorganize(Buf_block *block, const Index *index, mtr_t *mtr) noexcept {
  return page_reorganize_low(false, block, index, mtr);
}

byte *Btree::parse_page_reorganize(byte *ptr, byte *, Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  /* The record is empty, except for the record initial part */

  if (likely(block != nullptr)) {
    (void) page_reorganize_low(true, block, index, mtr);
  }

  return ptr;
}

void Btree::page_empty(Buf_block *block, Index *index, ulint level, mtr_t *mtr) noexcept {
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  /* Recreate the page: note that global data on page (possible
  segment headers, next page-field, etc.) is preserved intact */

  auto new_page = ::page_create(index, block, mtr);
  ut_ad(new_page != nullptr);

  page_set_level(page, level, mtr);

  block->m_check_index_page_at_flush = true;
}

rec_t *Btree::root_raise_and_insert(Btree_cursor *btr_cur, const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept {
  const auto root = btr_cur->get_page_no();
  const auto root_block = btr_cur->get_block();
  const auto index = btr_cur->get_index();

#ifdef UNIV_BTR_DEBUG
  auto space = index->get_space_id();

  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));

  ut_a(index->get_page_no() == page_get_page_no(root));
#endif /* UNIV_BTR_DEBUG */

  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(root_block, MTR_MEMO_PAGE_X_FIX));

  /* Allocate a new page to the tree. Root splitting is done by first
  moving the root records to the new page, emptying the root, putting
  a node pointer to the new page, and then splitting the new page. */

  const auto level = page_get_level(root, mtr);
  const auto new_block = page_alloc(index, 0, FSP_NO_DIR, level, mtr);
  const auto new_page = new_block->get_frame();

  page_create(new_block, index, level, mtr);

  /* Set the next node and previous node fields of new page */
  page_set_next(new_page, FIL_NULL, mtr);
  page_set_prev(new_page, FIL_NULL, mtr);

  /* Copy the records from root to the new page one by one. */
  const auto success{page_copy_rec_list_end(new_block, root_block, page_get_infimum_rec(root), index, mtr)};
  ut_a(success);

  /* If this is a pessimistic insert which is actually done to
  perform a pessimistic update then we have stored the lock
  information of the record to be inserted on the infimum of the
  root page: we cannot discard the lock structs on the root page */

  m_lock_sys->update_root_raise(new_block, root_block);

  /* Create a memory heap where the node pointer is stored */
  auto heap = mem_heap_create(100);
  const auto rec = page_rec_get_next(page_get_infimum_rec(new_page));
  const auto new_page_no = new_block->get_page_no();

  /* Build the node pointer (= node key and page address) for the child */
  auto node_ptr = index->build_node_ptr(rec, new_page_no, heap, level);

  /* The node pointer must be marked as the predefined minimum record,
  as there is no lower alphabetical limit to records in the leftmost
  node of a level: */
  dtuple_set_info_bits(node_ptr, dtuple_get_info_bits(node_ptr) | REC_INFO_MIN_REC_FLAG);

  /* Rebuild the root page to get free space */
  page_empty(root_block, index, level + 1, mtr);

  page_set_next(root, FIL_NULL, mtr);
  page_set_prev(root, FIL_NULL, mtr);

  auto page_cursor = btr_cur->get_page_cur();

  /* Insert node pointer to the root */

  page_cur_set_before_first(root_block, page_cursor);

  const auto node_ptr_rec = page_cur_tuple_insert(page_cursor, node_ptr, index, 0, mtr);

  /* The root page should only contain the node pointer
  to new_page at this point.  Thus, the data should fit. */
  ut_a(node_ptr_rec != nullptr);

  /* Free the memory heap */
  mem_heap_free(heap);

  /* Reposition the cursor to the child node */
  page_cur_search(new_block, index, tuple, PAGE_CUR_LE, page_cursor);

  /* Split the child and insert tuple */
  return page_split_and_insert(btr_cur, tuple, n_ext, mtr);
}

bool Btree::page_get_split_rec_to_left(Btree_cursor *btr_cur, rec_t *&split_rec) noexcept {
  const auto page = btr_cur->get_page_no();
  const auto insert_point = btr_cur->get_rec();

  if (page_header_get_ptr(page, PAGE_LAST_INSERT) == page_rec_get_next(insert_point)) {

    auto infimum = page_get_infimum_rec(page);

    /* If the convergence is in the middle of a page, include also
    the record immediately before the new insert to the upper
    page. Otherwise, we could repeatedly move from page to page
    lots of records smaller than the convergence point. */

    if (infimum != insert_point && page_rec_get_next(infimum) != insert_point) {
      split_rec = insert_point;
    } else {
      split_rec = page_rec_get_next(insert_point);
    }

    return true;
  }

  return false;
}

bool Btree::page_get_split_rec_to_right(Btree_cursor *btr_cur, rec_t *&split_rec) noexcept {
  const auto page = btr_cur->get_page_no();
  const auto insert_point = btr_cur->get_rec();

  /* We use eager heuristics: if the new insert would be right after
  the previous insert on the same page, we assume that there is a
  pattern of sequential inserts here. */

  if (likely(page_header_get_ptr(page, PAGE_LAST_INSERT) == insert_point)) {

    const auto next_rec = page_rec_get_next(insert_point);

    if (page_rec_is_supremum(next_rec)) {

      /* Split at the new record to insert */
      split_rec = nullptr;

    } else {
      const auto next_next_rec = page_rec_get_next(next_rec);

      if (page_rec_is_supremum(next_next_rec)) {

        /* Split at the new record to insert */
        split_rec = nullptr;

      } else {

        /* If there are >= 2 user records up from the insert
        point, split all but 1 off. We want to keep one because
        then sequential inserts can use the adaptive hash
        index, as they can do the necessary checks of the right
        search position just by looking at the records on this
        page. */

        split_rec = next_next_rec;
      }
    }

    return true;
  }

  return false;
}

rec_t *Btree::page_get_split_rec(Btree_cursor *btr_cur, const DTuple *tuple, ulint n_ext) noexcept {
  const auto page = btr_cur->get_page_no();
  const auto insert_size = rec_get_converted_size(btr_cur->get_index(), tuple, n_ext);
  const auto free_space = page_get_free_space_of_empty();

  /* free_space is now the free space of a created new page */

  const auto total_data = page_get_data_size(page) + insert_size;
  const auto total_n_recs = page_get_n_recs(page) + 1;
  ut_ad(total_n_recs >= 2);

  auto total_space = total_data + page_dir_calc_reserved_space(total_n_recs);

  ulint n{};
  ulint incl_data{};
  ulint *offsets{};
  mem_heap_t *heap{};
  auto ins_rec = btr_cur->get_rec();
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
      Phy_rec record{btr_cur->get_index(), rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());

      incl_data += rec_offs_size(offsets);
    }

    ++n;

  } while (incl_data + page_dir_calc_reserved_space(n) < total_space / 2);

  if (incl_data + page_dir_calc_reserved_space(n) <= free_space) {
    /* The next record will be the first on the right half page
    if it is not the supremum record of page */

    if (rec == ins_rec) {
      rec = nullptr;
    } else {
      rec_t *next_rec;

      if (rec == nullptr) {
        next_rec = page_rec_get_next(ins_rec);
      } else {
        next_rec = page_rec_get_next(rec);
      }

      ut_ad(next_rec != nullptr);

      if (!page_rec_is_supremum(next_rec)) {
        rec = next_rec;
      }
    }
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return rec;
}

bool Btree::page_insert_fits(Btree_cursor *btr_cur, const rec_t *split_rec, const ulint *offsets, const DTuple *tuple, ulint n_ext, mem_heap_t *heap) noexcept {
  auto page = btr_cur->get_page_no();

  ut_ad(!split_rec == !offsets);
  ut_ad(!offsets || rec_offs_validate(split_rec, btr_cur->get_index(), offsets));

  const auto insert_size = rec_get_converted_size(btr_cur->get_index(), tuple, n_ext);
  const auto free_space = page_get_free_space_of_empty();

  /* free_space is now the free space of a created new page */

  auto total_data = page_get_data_size(page) + insert_size;
  auto total_n_recs = page_get_n_recs(page) + 1;

  /* We determine which records (from rec to end_rec, not including
  end_rec) will end up on the other half page from tuple when it is
  inserted. */

  const rec_t *rec;
  const rec_t *end_rec;

  if (split_rec == nullptr) {

    rec = page_rec_get_next(page_get_infimum_rec(page));
    end_rec = page_rec_get_next(btr_cur->get_rec());

  } else if (cmp_dtuple_rec(btr_cur->get_index()->m_cmp_ctx, tuple, split_rec, offsets) >= 0) {

    rec = page_rec_get_next(page_get_infimum_rec(page));
    end_rec = split_rec;

  } else {

    rec = split_rec;
    end_rec = page_get_supremum_rec(page);
  }

  if (total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space) {

    /* Ok, there will be enough available space on the
    half page where the tuple is inserted */

    return true;
  }

  ulint *offs{};

  while (rec != end_rec) {
    Phy_rec record{btr_cur->get_index(), rec};

    /* In this loop we calculate the amount of reserved
    space after rec is removed from page. */

    offs = record.get_col_offsets(offs, ULINT_UNDEFINED, &heap, Current_location());

    total_data -= rec_offs_size(offs);
    --total_n_recs;

    if (total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space) {

      /* Ok, there will be enough available space on the
      half page where the tuple is inserted */

      return true;
    }

    rec = page_rec_get_next_const(rec);
  }

  return false;
}

void Btree::insert_on_non_leaf_level(Index *index, ulint level, DTuple *tuple, mtr_t *mtr, Source_location loc) noexcept {
  rec_t *rec;
  Btree_cursor btr_cur(m_fsp, this);
  big_rec_t *dummy_big_rec;

  ut_ad(level > 0);

  btr_cur.search_to_nth_level(nullptr, index, level, tuple, PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, mtr, loc);

  const auto err = btr_cur.pessimistic_insert(BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG | BTR_NO_UNDO_LOG_FLAG, tuple, &rec, &dummy_big_rec, 0, nullptr, mtr);
  ut_a(err == DB_SUCCESS);
}

void Btree::attach_half_pages(Index *index, Buf_block *block, rec_t *split_rec, Buf_block *new_block, ulint direction, mtr_t *mtr) noexcept {
  page_t *lower_page;
  page_t *upper_page;
  ulint lower_page_no;
  ulint upper_page_no;
  auto page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains(new_block, MTR_MEMO_PAGE_X_FIX));

  /* Create a memory heap where the data tuple is stored */
  auto heap = mem_heap_create(1024);

  /* Based on split direction, decide upper and lower pages */
  if (direction == FSP_DOWN) {

    Btree_cursor btr_cur(m_fsp, this);

    lower_page = new_block->get_frame();
    lower_page_no = new_block->get_page_no();
    upper_page = block->get_frame();
    upper_page_no = block->get_page_no();

    /* Look up the index for the node pointer to page */
    auto offsets = page_get_father_block(nullptr, heap, index, block, mtr, &btr_cur);

    /* Replace the address of the old child node (= page) with the
    address of the new lower half */

    node_ptr_set_child_page_no(btr_cur.get_rec(), offsets, lower_page_no, mtr);
    mem_heap_empty(heap);

  } else {

    lower_page = block->get_frame();
    lower_page_no = block->get_page_no();
    upper_page = new_block->get_frame();
    upper_page_no = new_block->get_page_no();
  }

  /* Get the level of the split pages */
  const auto level = page_get_level(block->get_frame(), mtr);
  ut_ad(level == page_get_level(new_block->get_frame(), mtr));

  /* Build the node pointer (= node key and page address) for the upper half */

  const auto node_ptr_upper = index->build_node_ptr(split_rec, upper_page_no, heap, level);

  /* Insert it next to the pointer to the lower half. Note that this
  may generate recursion leading to a split on the higher level. */

  insert_on_non_leaf_level(index, level + 1, node_ptr_upper, mtr, Current_location());

  /* Free the memory heap */
  mem_heap_free(heap);

  /* Get the previous and next pages of page */

  const auto prev_page_no = page_get_prev(page, mtr);
  const auto next_page_no = page_get_next(page, mtr);
  const auto space = block->get_space();

  /* Update page links of the level */

  if (prev_page_no != FIL_NULL) {
    auto prev_block = block_get(space, prev_page_no, RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_next(prev_block->get_frame(), mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

    page_set_next(prev_block->get_frame(), lower_page_no, mtr);
  }

  if (next_page_no != FIL_NULL) {
    auto next_block = block_get(space, next_page_no, RW_X_LATCH, mtr);

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_prev(next_block->get_frame(), mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    page_set_prev(next_block->get_frame(), upper_page_no, mtr);
  }

  page_set_prev(lower_page, prev_page_no, mtr);
  page_set_next(lower_page, upper_page_no, mtr);

  page_set_prev(upper_page, lower_page_no, mtr);
  page_set_next(upper_page, next_page_no, mtr);
}

bool Btree::page_tuple_smaller(Btree_cursor *btr_cur, const DTuple *tuple, ulint *offsets, ulint n_uniq, mem_heap_t **heap) noexcept {
  page_cur_t pcur;

  /* Read the first user record in the page. */
  auto block = btr_cur->get_block();

  page_cur_set_before_first(block, &pcur);
  page_cur_move_to_next(&pcur);

  auto first_rec = page_cur_get_rec(&pcur);

  {
    Phy_rec record{btr_cur->get_index(), first_rec};

    offsets = record.get_col_offsets(offsets, n_uniq, heap, Current_location());
  }

  return cmp_dtuple_rec(btr_cur->get_index()->m_cmp_ctx, tuple, first_rec, offsets) < 0;
}

rec_t *Btree::page_split_and_insert(Btree_cursor *btr_cur, const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept {
  Buf_block *left_block;
  Buf_block *right_block;

  auto heap = mem_heap_create(1024);
  const auto n_uniq = btr_cur->get_index()->get_n_unique_in_tree();

  ulint n_iterations{};

  rec_t *rec{};

  for (;;) {
    mem_heap_empty(heap);

    ulint *offsets = nullptr;

    ut_ad(mtr->memo_contains(btr_cur->get_index()->get_lock(), MTR_MEMO_X_LOCK));

  #ifdef UNIV_SYNC_DEBUG
    ut_ad(rw_lock_own(btr_cur->get_index()->get_lock(), RW_LOCK_EX));
  #endif /* UNIV_SYNC_DEBUG */

    auto block = btr_cur->get_block();
    auto page = block->get_frame();

    ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));
    ut_ad(page_get_n_recs(page) >= 1);

    auto page_no = block->get_page_no();

    /* 1. Decide the split record; split_rec == nullptr means that the
    tuple to be inserted should be the first record on the upper
    half-page */

    byte direction;
    rec_t *split_rec{};
    bool insert_left{};
    page_no_t hint_page_no;

    if (n_iterations > 0) {

      direction = FSP_UP;
      hint_page_no = page_no + 1;
      split_rec = page_get_split_rec(btr_cur, tuple, n_ext);

      if (unlikely(split_rec == nullptr)) {
        insert_left = page_tuple_smaller(btr_cur, tuple, offsets, n_uniq, &heap);
      }

    } else if (page_get_split_rec_to_right(btr_cur, split_rec)) {

      direction = FSP_UP;
      hint_page_no = page_no + 1;

    } else if (page_get_split_rec_to_left(btr_cur, split_rec)) {

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
      } else if (page_tuple_smaller(btr_cur, tuple, offsets, n_uniq, &heap)) {
        split_rec = page_rec_get_next(page_get_infimum_rec(page));
      } else {
        split_rec = nullptr;
      }
    }

    /* 2. Allocate a new page to the index */
    auto new_block = page_alloc(btr_cur->get_index(), hint_page_no, direction, page_get_level(page, mtr), mtr);

    page_create(new_block, btr_cur->get_index(), page_get_level(page, mtr), mtr);

    /* 3. Calculate the first record on the upper half-page, and the
    first record (move_limit) on original page which ends up on the
    upper half */

    byte *buf{};
    rec_t *first_rec{};
    rec_t *move_limit{};

    if (split_rec != nullptr) {
      first_rec = move_limit = split_rec;

      Phy_rec record{btr_cur->get_index(), split_rec};

      offsets = record.get_col_offsets(offsets, n_uniq, &heap, Current_location());

      insert_left = cmp_dtuple_rec(btr_cur->get_index()->m_cmp_ctx, tuple, split_rec, offsets) < 0;

    } else {

      buf = reinterpret_cast<byte *>(mem_alloc(rec_get_converted_size(btr_cur->get_index(), tuple, n_ext)));

      first_rec = rec_convert_dtuple_to_rec(buf, btr_cur->get_index(), tuple, n_ext);

      move_limit = page_rec_get_next(btr_cur->get_rec());
    }

    /* 4. Do first the modifications in the tree structure */

    attach_half_pages(btr_cur->get_index(), block, first_rec, new_block, direction, mtr);

    /* If the split is made on the leaf level and the insert will fit
    on the appropriate half-page, we may release the tree x-latch.
    We can then move the records after releasing the tree latch,
    thus reducing the tree latch contention. */

    bool insert_will_fit;

    if (split_rec != nullptr) {
      insert_will_fit = page_insert_fits(btr_cur, split_rec, offsets, tuple, n_ext, heap);
    } else {
      mem_free(buf);
      insert_will_fit = page_insert_fits(btr_cur, nullptr, nullptr, tuple, n_ext, heap);
    }

    if (insert_will_fit && page_is_leaf(page)) {

      mtr->memo_release(btr_cur->get_index()->get_lock(), MTR_MEMO_X_LOCK);
    }

    /* 5. Move then the records to the new page */
    if (direction == FSP_DOWN
  #ifdef UNIV_BTR_AVOID_COPY
        && page_rec_is_supremum(move_limit)) {
      /* Instead of moving all records, make the new page the empty page. */
      left_block = block;
      right_block = new_block;
    } else if (
      direction == FSP_DOWN
  #endif /* UNIV_BTR_AVOID_COPY */
    ) {
      auto success{page_move_rec_list_start(new_block, block, move_limit, btr_cur->get_index(), mtr)};
      ut_a(success);

      left_block = new_block;
      right_block = block;

      m_lock_sys->update_split_left(right_block, left_block);

  #ifdef UNIV_BTR_AVOID_COPY
    } else if (split_rec == nullptr) {
      /* Instead of moving all records, make the new page the empty page. */
      left_block = new_block;
      right_block = block;
  #endif /* UNIV_BTR_AVOID_COPY */

    } else {

      auto success{page_move_rec_list_end(new_block, block, move_limit, btr_cur->get_index(), mtr)};
      ut_a(success);

      left_block = block;
      right_block = new_block;

      m_lock_sys->update_split_right(right_block, left_block);
    }

    /* At this point, split_rec, move_limit and first_rec may point
    to garbage on the old page. */

    /* 6. The split and the tree modification is now completed. Decide the
    page where the tuple should be inserted */

    auto insert_block = insert_left ? left_block : right_block;

    /* 7. Reposition the cursor for insert and try insertion */
    auto page_cursor = btr_cur->get_page_cur();

    page_cur_search(insert_block, btr_cur->get_index(), tuple, PAGE_CUR_LE, page_cursor);

    rec = page_cur_tuple_insert(page_cursor, tuple, btr_cur->get_index(), n_ext, mtr);

    if (unlikely(rec == nullptr)) {

      /* 8. If insert did not fit, try page reorganization */

      if (unlikely(!page_reorganize(insert_block, btr_cur->get_index(), mtr))) {

        ++n_iterations;

        ut_ad(n_iterations < 2);
        ut_ad(!insert_will_fit);

        continue;

      } else {

        page_cur_search(insert_block, btr_cur->get_index(), tuple, PAGE_CUR_LE, page_cursor);

        rec = page_cur_tuple_insert(page_cursor, tuple, btr_cur->get_index(), n_ext, mtr);

        if (unlikely(rec == nullptr)) {
          /* The insert did not fit on the page: loop back to the start of the function for a new split */
          ++n_iterations;

          ut_ad(n_iterations < 2);
          ut_ad(!insert_will_fit);

          continue;
        }
      }
    }

    break;
  }

  ut_ad(page_validate(left_block->get_frame(), btr_cur->get_index()));
  ut_ad(page_validate(right_block->get_frame(), btr_cur->get_index()));

  mem_heap_free(heap);

  return rec;
}

void Btree::level_list_remove(space_id_t space, page_t *page, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(page, MTR_MEMO_PAGE_X_FIX));
  ut_ad(space == page_get_space_id(page));
  /* Get the previous and next page numbers of page */

  auto prev_page_no = page_get_prev(page, mtr);
  auto next_page_no = page_get_next(page, mtr);

  /* Update page links of the level */

  if (prev_page_no != FIL_NULL) {
    auto prev_block = block_get(space, prev_page_no, RW_X_LATCH, mtr);
    auto prev_page = prev_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_next(prev_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    page_set_next(prev_page, next_page_no, mtr);
  }

  if (next_page_no != FIL_NULL) {
    auto next_block = block_get(space, next_page_no, RW_X_LATCH, mtr);
    auto next_page = next_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_prev(next_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

    page_set_prev(next_page, prev_page_no, mtr);
  }
}

void Btree::set_min_rec_mark_log(rec_t *rec, mlog_type_t type, mtr_t *mtr) noexcept {
  mlog_write_initial_log_record(rec, type, mtr);

  /* Write rec offset as a 2-byte ulint */
  mlog_catenate_ulint(mtr, page_offset(rec), MLOG_2BYTES);
}

byte *Btree::parse_set_min_rec_mark(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) noexcept {
  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  if (page != nullptr) {

    auto rec = page + mach_read_from_2(ptr);

    set_min_rec_mark(rec, mtr);
  }

  return ptr + 2;
}

void Btree::set_min_rec_mark(rec_t *rec, mtr_t *mtr) noexcept {
  const auto info_bits = rec_get_info_bits(rec);

  rec_set_info_bits(rec, info_bits | REC_INFO_MIN_REC_FLAG);

  set_min_rec_mark_log(rec, MLOG_REC_MIN_MARK, mtr);
}

void Btree::node_ptr_delete(Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  Btree_cursor btr_cur(m_fsp, this);

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  /* Delete node pointer on father page */
  page_get_father(index, block, mtr, &btr_cur);


  auto err = btr_cur.pessimistic_delete(true,RB_NONE, mtr);
  ut_a(err == DB_SUCCESS);
}

void Btree::lift_page_up(Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  auto page = block->get_frame();
  std::array<Buf_block *, BTR_MAX_DEPTH> blocks;

  ut_ad(page_get_prev(page, mtr) == FIL_NULL);
  ut_ad(page_get_next(page, mtr) == FIL_NULL);
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  auto page_level = page_get_level(page, mtr);
  const auto root_page_no = index->get_page_no();

   /* Last used index in blocks[] */
  ulint n_blocks{};
  Buf_block *father_block;
  IF_DEBUG(page_t *father_page;)

  {
    Btree_cursor btr_cur(m_fsp, this);
    auto heap = mem_heap_create(100);
    auto offsets = page_get_father_block(nullptr, heap, index, block, mtr, &btr_cur);

    father_block = btr_cur.get_block();

    IF_DEBUG(father_page = father_block->get_frame();)

    n_blocks = 0;

    /* Store all ancestor pages so we can reset their
    levels later on.  We have to do all the searches on
    the tree now because later on, after we've replaced
    the first level, the tree is in an inconsistent state
    and can not be searched. */
    for (auto b = father_block; b->get_page_no() != root_page_no;) {
      ut_a(n_blocks < BTR_MAX_DEPTH);

      offsets = page_get_father_block(offsets, heap, index, b, mtr, &btr_cur);

      blocks[n_blocks++] = b = btr_cur.get_block();
    }

    mem_heap_free(heap);
  }

  /* Make the father empty */
  page_empty(father_block, index, page_level, mtr);

  m_lock_sys->update_copy_and_discard(father_block, block);

  /* Go upward to root page, decrementing levels by one. */
  for (ulint i{}; i < n_blocks; ++i, ++page_level) {
    auto page = blocks[i]->get_frame();

    ut_ad(page_get_level(page, mtr) == page_level + 1);

    page_set_level(page, page_level, mtr);
  }

  page_free(index, block, mtr);

  ut_ad(page_validate(father_page, index));
  ut_ad(check_node_ptr(index, father_block, mtr));
}

bool Btree::compress(Btree_cursor *cursor, mtr_t *mtr) noexcept {
  page_t *merge_page;
  Btree_cursor btr_cur(m_fsp, this);

  auto block = btr_cur.get_block();
  auto page = btr_cur.get_page_no();
  auto index = btr_cur.get_index();

  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  const auto space = index->get_space_id();
  const auto left_page_no = page_get_prev(page, mtr);
  const auto right_page_no = page_get_next(page, mtr);

  auto heap = mem_heap_create(100);
  auto offsets = page_get_father_block(nullptr, heap, index, block, mtr, &btr_cur);

  /* Decide the page to which we try to merge and which will inherit the locks */

  Buf_block *merge_block;
  const auto is_left = left_page_no != FIL_NULL;

  if (is_left) {

    merge_block = block_get(space, left_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_next(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else if (right_page_no != FIL_NULL) {

    merge_block = block_get(space, right_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_prev(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else {
    /* The page is the only one on the level, lift the records to the father */
    lift_page_up(index, block, mtr);

    mem_heap_free(heap);

    return true;
  }

  const auto n_recs = page_get_n_recs(page);
  const auto data_size = page_get_data_size(page);
  const auto max_ins_size_reorg = page_get_max_insert_size_after_reorganize(merge_page, n_recs);

  auto err_clean_up = [&]() {
    mem_heap_free(heap);
    return false;
  };

  if (data_size > max_ins_size_reorg) {
    /* No space for merge */
    return err_clean_up();

  }

  ut_ad(page_validate(merge_page, index));

  auto max_ins_size = page_get_max_insert_size(merge_page, n_recs);

  if (unlikely(data_size > max_ins_size)) {

    /* We have to reorganize merge_page */

    if (unlikely(!page_reorganize(merge_block, index, mtr))) {

      return err_clean_up();
    }

    max_ins_size = page_get_max_insert_size(merge_page, n_recs);

    ut_ad(page_validate(merge_page, index));
    ut_ad(max_ins_size == max_ins_size_reorg);

    if (unlikely(data_size > max_ins_size)) {

      /* Add fault tolerance, though this should
      never happen */

      return err_clean_up();
    }
  }

  /* Move records to the merge page */
  if (is_left) {
    const auto orig_pred = page_copy_rec_list_start(merge_block, block, page_get_supremum_rec(page), index, mtr);

    if (unlikely(!orig_pred)) {
      return err_clean_up();
    }

    /* Remove the page from the level list */
    level_list_remove(space, page, mtr);

    node_ptr_delete(index, block, mtr);

    m_lock_sys->update_merge_left(merge_block, orig_pred, block);

  } else {
    auto orig_succ = page_copy_rec_list_end(merge_block, block, page_get_infimum_rec(page), cursor->m_index, mtr);

    ut_a(orig_succ == nullptr);

    /* Remove the page from the level list */
    level_list_remove(space, page, mtr);

    /* Replace the address of the old child node (= page) with the
    address of the merge page to the right */

    node_ptr_set_child_page_no(btr_cur.get_rec(), offsets, right_page_no, mtr);
    node_ptr_delete(index, merge_block, mtr);

    m_lock_sys->update_merge_right(merge_block, orig_succ, block);
  }

  mem_heap_free(heap);

  ut_ad(page_validate(merge_page, index));

  page_free(index, block, mtr);

  ut_ad(check_node_ptr(index, merge_block, mtr));

  return true;
}

void Btree::discard_only_page_on_level(Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  ulint page_level{};

  /* Save the PAGE_MAX_TRX_ID from the leaf page. */
  const auto max_trx_id = page_get_max_trx_id(block->get_frame());

  while (block->get_page_no() != index->get_page_no()) {
    Btree_cursor btr_cur(m_fsp, this);
    const auto page = block->get_frame();

    ut_a(page_get_n_recs(page) == 1);
    ut_a(page_level == page_get_level(page, mtr));
    ut_a(page_get_prev(page, mtr) == FIL_NULL);
    ut_a(page_get_next(page, mtr) == FIL_NULL);

    ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

    page_get_father(index, block, mtr, &btr_cur);
    auto father = btr_cur.get_block();

    m_lock_sys->update_discard(father, PAGE_HEAP_NO_SUPREMUM, block);

    /* Free the file page */
    page_free(index, block, mtr);

    block = father;

    ++page_level;
  }

  /* block is the root page, which must be empty, except
  for the node pointer to the (now discarded) block(s). */

#ifdef UNIV_BTR_DEBUG
  const auto root = block->get_frame();
  const auto space = index->get_space_id();
  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_LEAF + root, space));
  ut_a(root_fseg_validate(FIL_PAGE_DATA + PAGE_BTR_SEG_TOP + root, space));
#endif /* UNIV_BTR_DEBUG */

  if (!index->is_clustered() && page_is_leaf(block->get_frame())) {
    ut_a(max_trx_id > 0);
    page_set_max_trx_id(block, max_trx_id, mtr);
  }

  page_empty(block, index, 0, mtr);
}

void Btree::discard_page(Btree_cursor *btr_cur, mtr_t *mtr) noexcept{
  const auto block = btr_cur->get_block();
  const auto index = btr_cur->get_index();

  ut_ad(index->get_page_no() != block->get_page_no());
  ut_ad(mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  const auto space = index->get_space_id();

  /* Decide the page which will inherit the locks */

  const auto left_page_no = page_get_prev(block->get_frame(), mtr);
  const auto right_page_no = page_get_next(block->get_frame(), mtr);

  page_t *merge_page;
  Buf_block *merge_block;

  if (left_page_no != FIL_NULL) {
    merge_block = block_get(space, left_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_next(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else if (right_page_no != FIL_NULL) {
    merge_block = block_get(space, right_page_no, RW_X_LATCH, mtr);
    merge_page = merge_block->get_frame();

#ifdef UNIV_BTR_DEBUG
    ut_a(page_get_prev(merge_page, mtr) == block->get_page_no());
#endif /* UNIV_BTR_DEBUG */

  } else {

    discard_only_page_on_level(index, block, mtr);

    return;
  }

  const auto page = block->get_frame();

  if (left_page_no == FIL_NULL && !page_is_leaf(page)) {

    /* We have to mark the leftmost node pointer on the right
    side page as the predefined minimum record */
    auto node_ptr = page_rec_get_next(page_get_infimum_rec(merge_page));

    ut_ad(page_rec_is_user_rec(node_ptr));

    set_min_rec_mark(node_ptr, mtr);
  }

  node_ptr_delete(index, block, mtr);

  /* Remove the page from the level list */
  level_list_remove(space, page, mtr);

  if (left_page_no != FIL_NULL) {
    m_lock_sys->update_discard(merge_block, PAGE_HEAP_NO_SUPREMUM, block);
  } else {
    m_lock_sys->update_discard(merge_block, m_lock_sys->get_min_heap_no(merge_block), block);
  }

  /* Free the file page */
  page_free(index, block, mtr);

  ut_ad(check_node_ptr(index, merge_block, mtr));
}

#ifdef UNIV_BTR_PRINT
void Btree::print_size(Index *index) noexcept {
  mtr_t mtr;

  mtr.start();

  const auto root = root_get(index->m_page_id, &mtr);
  const auto seg = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

  log_info("NON-LEAF PAGE SEGMENT");
  m_fsp->fseg_print(seg, &mtr);

  mtr.commit();
}

void Btree::print_recursive(Index *index, Buf_block *block, ulint width, mem_heap_t **heap, ulint **offsets, mtr_t *mtr) noexcept {
  ulint i{};
  const page_t *page = block->get_frame();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  log_info(std::format("NODE ON LEVEL {} page number {}", page_get_level(page, mtr), block->get_page_no()));

  page_print(block, index, width, width);

  const auto n_recs = page_get_n_recs(page);

  page_cur_t cursor;

  page_cur_set_before_first(block, &cursor);
  page_cur_move_to_next(&cursor);

  while (!page_cur_is_after_last(&cursor)) {

    if (page_is_leaf(page)) {

      /* If this is the leaf level, do nothing */

    } else if (i <= width || i >= n_recs - width) {
      mtr_t mtr2;

      mtr2.start();

      const auto node_ptr = page_cur_get_rec(&cursor);

      {
        Phy_rec record{index, node_ptr};

        *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, heap, Current_location());
      }

      print_recursive(index, node_ptr_get_child(node_ptr, index, *offsets, &mtr2), width, heap, offsets, &mtr2);

      mtr2.commit();
    }

    page_cur_move_to_next(&cursor);
    ++i;
  }
}

void Btree::print_index(Index *index, ulint width) noexcept {
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  log_info(
    "--------------------------\n"
    "INDEX TREE PRINT\n"
  );

  mtr_t mtr;

  mtr.start();

  const auto root = root_block_get(index->m_page_id, &mtr);

  print_recursive(index, root, width, &heap, &offsets, &mtr);

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  mtr.commit();

  ut_ad(validate_index(index, nullptr));
}
#endif /* UNIV_BTR_PRINT */

#ifdef UNIV_DEBUG
bool Btree::check_node_ptr(Index *index, Buf_block *block, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  if (index->get_page_no() == block->get_page_no()) {

    return true;
  }

  Btree_cursor btr_cur(m_fsp, this);
  const auto page = block->get_frame();
  auto heap = mem_heap_create(256);
  const auto offsets = page_get_father_block(nullptr, heap, index, block, mtr, &btr_cur);

  if (!page_is_leaf(page)) {
    const auto tuple = index->build_node_ptr(page_rec_get_next(page_get_infimum_rec(page)), 0, heap, page_get_level(page, mtr));

    ut_a(!cmp_dtuple_rec(index->m_cmp_ctx, tuple, btr_cur.get_rec(), offsets));
  }

  mem_heap_free(heap);

  return true;
}
#endif /* UNIV_DEBUG */

void Btree::index_rec_validate_report(const page_t *page, const rec_t *rec, const Index *index) noexcept {
  log_err("Record in ", index->m_name);
  log_err(std::format(", page {}, at offset {}", page_get_page_no(page), page_offset(rec)));
}

bool Btree::index_rec_validate(const rec_t *rec, const Index *index, bool dump_on_error) noexcept {
  ulint len;
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  const auto page = page_align(rec);
  const auto n = index->get_n_fields();

  if (unlikely(rec_get_n_fields(rec) != n)) {
    index_rec_validate_report(page, rec, index);

    log_err(std::format("has {} fields, should have {}", rec_get_n_fields(rec), n));

    if (dump_on_error) {
      buf_page_print(page, 0);

      log_err("corrupt record ");
      log_err(rec_to_string(rec));
    }
    return false;
  }

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  for (ulint i{}; i < n; ++i) {
    const ulint fixed_size = index->get_nth_col(i)->get_fixed_size();

    rec_get_nth_field_offs(offsets, i, &len);

    /* Note that if fixed_size != 0, it equals the length of a fixed-size column in the clustered index.
    A prefix index of the column is of fixed, but different length.  When fixed_size == 0, prefix_len is the maximum
    length.  When fixed_size == 0, prefix_len is the maximum length of the prefix index column. */

    if ((index->get_nth_field(i)->m_prefix_len == 0 && len != UNIV_SQL_NULL && fixed_size > 0 && len != fixed_size) ||
        (index->get_nth_field(i)->m_prefix_len > 0 && len != UNIV_SQL_NULL && len > index->get_nth_field(i)->m_prefix_len)) {

      index_rec_validate_report(page, rec, index);

      log_err(std::format("field {} len is {}, should be {}", i, len, fixed_size));

      if (dump_on_error) {
        buf_page_print(page, 0);

        log_err("corrupt record: {}", rec_to_string(rec));
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

bool Btree::index_page_validate(Buf_block *block, Index *index) noexcept {
  page_cur_t cur;

  page_cur_set_before_first(block, &cur);
  page_cur_move_to_next(&cur);

  for (;;) {
    if (page_cur_is_after_last(&cur)) {

      break;
    }

    if (!index_rec_validate(cur.m_rec, index, true)) {

      return false;
    }

    page_cur_move_to_next(&cur);
  }

  return true;
}

void Btree::validate_report1(Index *index, ulint level, const Buf_block *block) noexcept {
  log_err(std::format("Page {} of {}", block->get_page_no(), index->m_name));

  if (level > 0) {
    log_err(", index tree level ", level);
  }
}

void Btree::validate_report2(const Index *index, ulint level, const Buf_block *block1, const Buf_block *block2) noexcept {
  log_err(std::format("Pages {} and {} of {}", block1->get_page_no(), block2->get_page_no(), index->m_name));

  if (level > 0) {
    log_err(", index tree level ", level);
  }
}

bool Btree::validate_level(Index *index, Trx *trx, ulint level) noexcept {
  rec_t *rec;
  bool ret{true};
  page_cur_t cursor;
  page_t *right_page{};
  ulint *offsets2 = nullptr;
  Buf_block *right_block{};
  Btree_cursor node_cur(m_fsp, this);
  Btree_cursor right_node_cur(m_fsp, this);
  mem_heap_t *heap = mem_heap_create(256);

  mtr_t mtr;

  mtr.start();

  mtr_x_lock(index->get_lock(), &mtr);

  ulint *offsets{};
  auto block = root_block_get(index->m_page_id, &mtr);
  auto page = block->get_frame();
  const auto space = index->get_space_id();

  while (level != page_get_level(page, &mtr)) {
    ut_a(space == block->get_space());
    ut_a(space == page_get_space_id(page));
    ut_a(!page_is_leaf(page));

    page_cur_set_before_first(block, &cursor);
    page_cur_move_to_next(&cursor);

    auto node_ptr = page_cur_get_rec(&cursor);

    {
      Phy_rec record{index, node_ptr};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    block = node_ptr_get_child(node_ptr, index, offsets, &mtr);
    page = block->get_frame();
  }

  auto node_ptr_fails = [this](mtr_t &mtr, Page_id page_id, Buf_block *&block, page_t *&page) -> bool {
    /* Commit the mini-transaction to release the latch on 'page'. Re-acquire the latch on right_page,
    which will become 'page' on the next loop.  The page has already been checked. */
    mtr.commit();

    if (page_id.m_page_no != FIL_NULL) {
      mtr.start();

      block = block_get(page_id.m_space_id, page_id.m_page_no, RW_X_LATCH, &mtr);
      page = block->get_frame();

      return false;
    } else {
      return true;
    }
  };

  /* Now we are on the desired level. Loop through the pages on that level. */
  page_no_t right_page_no{ULINT32_UNDEFINED};

  do {

    if (trx->is_interrupted()) {
      mtr.commit();
      mem_heap_free(heap);
      return (ret);
    }

    mem_heap_empty(heap);

    auto offsets = offsets2 = nullptr;

    mtr_x_lock(index->get_lock(), &mtr);

    /* Check ordering etc. of records */

    if (!page_validate(page, index)) {
      validate_report1(index, level, block);

      ret = false;

    } else if (level == 0) {
      /* We are on level 0. Check that the records have the right
      number of fields, and field lengths are right. */

      if (!index_page_validate(block, index)) {

        ret = false;
      }
    }

    ut_a(page_get_level(page, &mtr) == level);

    right_page_no = page_get_next(page, &mtr);

    const auto left_page_no = page_get_prev(page, &mtr);

    ut_a(page_get_n_recs(page) > 0 || (level == 0 && page_get_page_no(page) == index->get_page_no()));

    if (right_page_no != FIL_NULL) {
      const rec_t *right_rec;

      const auto right_block = block_get(space, right_page_no, RW_X_LATCH, &mtr);
      const auto right_page = right_block->get_frame();

      if (unlikely(page_get_prev(right_page, &mtr) != page_get_page_no(page))) {
        validate_report2(index, level, block, right_block);
        log_err("broken FIL_PAGE_NEXT or FIL_PAGE_PREV links");
        buf_page_print(page, 0);
        buf_page_print(right_page, 0);

        ret = false;
      }

      rec = page_rec_get_prev(page_get_supremum_rec(page));
      right_rec = page_rec_get_next(page_get_infimum_rec(right_page));

      {
        Phy_rec record{index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      {
        Phy_rec record{index, right_rec};

        offsets2 = record.get_col_offsets(offsets2, ULINT_UNDEFINED, &heap, Current_location());
      }

      if (unlikely(cmp_rec_rec(rec, right_rec, offsets, offsets2, index) >= 0)) {

        validate_report2(index, level, block, right_block);

        log_err("records in wrong order on adjacent pages");

        buf_page_print(page, 0);
        buf_page_print(right_page, 0);

        log_err("record ");
        rec = page_rec_get_prev(page_get_supremum_rec(page));
        log_err(rec_to_string(rec));
        log_err("record ");
        rec = page_rec_get_next(page_get_infimum_rec(right_page));
        log_err(rec_to_string(rec));

        ret = false;
      }
    }

    if (level > 0 && left_page_no == FIL_NULL) {
      ut_a(REC_INFO_MIN_REC_FLAG & rec_get_info_bits(page_rec_get_next(page_get_infimum_rec(page))));
    }

    if (block->get_page_no() != index->get_page_no()) {

      /* Check father node pointers */

      offsets = page_get_father_block(offsets, heap, index, block, &mtr, &node_cur);

      const auto father_page = node_cur.get_page_no();

      auto node_ptr = node_cur.get_rec();

      node_cur.position(index, page_rec_get_prev(page_get_supremum_rec(page)), block);

      offsets = page_get_father_node_ptr(offsets, heap, &node_cur, &mtr, Current_location());

      if (unlikely(node_ptr != node_cur.get_rec()) ||
        unlikely(node_ptr_get_child_page_no(node_ptr, offsets) != block->get_page_no())) {

        validate_report1(index, level, block);

        log_err("node pointer to the page is wrong");

        buf_page_print(father_page, 0);
        buf_page_print(page, 0);

        log_err("node ptr: ", rec_to_string(node_ptr));

        rec = node_cur.get_rec();

        log_err("node ptr child page n:o ", node_ptr_get_child_page_no(rec, offsets));

        log_err("record on page ");
        log_err(rec_to_string(rec));
        ret = false;

        if (node_ptr_fails(mtr, {space,right_page_no}, right_block, right_page)) {
          break;
        } else {
          continue;
        }
      }

      if (!page_is_leaf(page)) {
        const auto node_ptr_tuple = index->build_node_ptr(page_rec_get_next(page_get_infimum_rec(page)), 0, heap, page_get_level(page, &mtr));

        if (cmp_dtuple_rec(index->m_cmp_ctx, node_ptr_tuple, node_ptr, offsets)) {
          const rec_t *first_rec = page_rec_get_next(page_get_infimum_rec(page));

          validate_report1(index, level, block);

          buf_page_print(father_page, 0);
          buf_page_print(page, 0);

          log_err("Node ptrs differ on levels > 0 node ptr");
          log_err(rec_to_string(node_ptr));
          log_err("first rec ");
          log_err(rec_to_string(first_rec));

          ret = false;

          if (node_ptr_fails(mtr, {space,right_page_no}, right_block, right_page)) {
            break;
          } else {
            continue;
          }
        }
      }

      if (left_page_no == FIL_NULL) {
        ut_a(node_ptr == page_rec_get_next(page_get_infimum_rec(father_page)));
        ut_a(page_get_prev(father_page, &mtr) == FIL_NULL);
      }

      if (right_page_no == FIL_NULL) {
        ut_a(node_ptr == page_rec_get_prev(page_get_supremum_rec(father_page)));
        ut_a(page_get_next(father_page, &mtr) == FIL_NULL);
      } else {
        const rec_t *right_node_ptr = page_rec_get_next(node_ptr);

        offsets = page_get_father_block(offsets, heap, index, right_block, &mtr, &right_node_cur);

        if (right_node_ptr != page_get_supremum_rec(father_page)) {

          if (right_node_cur.get_rec() != right_node_ptr) {
            ret = false;

            log_err("Node pointer to the right page is wrong");

            validate_report1(index, level, block);

            buf_page_print(father_page, 0);
            buf_page_print(page, 0);
            buf_page_print(right_page, 0);
          }
        } else {
          const auto right_father_page = right_node_cur.get_page_no();

          if (right_node_cur.get_rec() != page_rec_get_next(page_get_infimum_rec(right_father_page))) {
            ret = false;

            log_err("Node pointer 2 to the right page is wrong");

            validate_report1(index, level, block);

            buf_page_print(father_page, 0);
            buf_page_print(right_father_page, 0);
            buf_page_print(page, 0);
            buf_page_print(right_page, 0);
          }

          if (page_get_page_no(right_father_page) != page_get_next(father_page, &mtr)) {

            ret = false;

            log_err("Node pointer 3 to the right page is wrong");

            validate_report1(index, level, block);

            buf_page_print(father_page, 0);
            buf_page_print(right_father_page, 0);
            buf_page_print(page, 0);
            buf_page_print(right_page, 0);
          }
        }
      }
    }
  } while (!node_ptr_fails(mtr, {space, right_page_no}, right_block, right_page));

  mem_heap_free(heap);

  return ret;
}

bool Btree::validate_index(Index *index, Trx *trx) noexcept {
  mtr_t mtr;

  mtr.start();

  mtr_x_lock(index->get_lock(), &mtr);

  const auto root = root_get(index->m_page_id, &mtr);
  const auto n = page_get_level(root, &mtr);

  for (ulint i = 0; i <= n && !trx->is_interrupted(); ++i) {
    if (!validate_level(index, trx, n - i)) {

      mtr.commit();

      return (false);
    }
  }

  mtr.commit();

  return true;
}

Btree *Btree::create(Lock_sys *lock_sys, FSP *fsp, Buf_pool *buf_pool) noexcept {
  auto ptr = ut_new(sizeof(Btree));
  return new (ptr) Btree(lock_sys, fsp, buf_pool);
}

void Btree::destroy(Btree *&btree) noexcept {
  call_destructor(btree);
  ut_delete(btree);
  btree = nullptr;
}
