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

struct Btree {

  /**
   * Constructor.
   * 
   * @param[in] lock_sys        Lock system.
   * @param[in] fsp             File space.
   * @param[in] buf_pool        Buffer pool.
   */
  Btree(Lock_sys *lock_sys, FSP *fsp, Buf_pool *buf_pool) noexcept
  : m_lock_sys{lock_sys}, m_fsp{fsp}, m_buf_pool{buf_pool} {}

  /**
   * Destructor.
   */
  ~Btree() = default;

  /**
   * Gets the root node of a tree and x-latches it.
   * 
   * @param[in] page_id         Page id.
   * @param[in,out]             Mini-transaction.
   * 
   * @return	root page, x-latched
   */
  [[nodiscard]] page_t *root_get(Page_id page_id, mtr_t *mtr) noexcept;

  /**
   * Gets pointer to the previous user record in the tree. It is assumed
   * that the caller has appropriate latches on the page and its neighbor.
   * 
   * @param[in,out] rec         Record on leaf level
   * @param[in,out] mtr         Mini-tranaction.
   * 
   * @return	previous user record, nullptr if there is none
   */
  [[nodiscard]] rec_t *get_prev_user_rec(rec_t *rec, mtr_t *mtr) noexcept;

  /**
   * Gets pointer to the next user record in the tree. It is assumed
   * that the caller has appropriate latches on the page and its neighbor.
   * 
   * @param[in,out] rec         Record on leaf level
   * @param[in,out] mtr         holding a latch on the page, and if needed,
                                also to the next page
   * 
   * @return	next user record, nullptr if there is none
   */
  [[nodiscard]] rec_t *get_next_user_rec(rec_t *rec, mtr_t *mtr) noexcept;

  /**
   * Creates the root node for a new index tree.
   * 
   * @param[in] type            type of the index
   * @param[in] space           space where create
   * @param[in] index_id        index id
   * @param[in,out] index       index
   * @param[in,out] mtr         Mini-transaction handle
   * 
   * @return	page number of the created root, FIL_NULL if did not succeed
   */
  [[nodiscard]] page_no_t create(ulint type, space_id_t space, uint64_t index_id, Index *index, mtr_t *mtr) noexcept;

  /**
   * Frees a B-tree except the root page, which MUST be freed after this
   * by calling btr_free_root.
   * 
   * @param[in] space           Space wwhere created
   * @param[in] root_page_no    Root page number.
   */
  void free_but_not_root(space_id_t space, page_no_t root_page_no) noexcept;

  /**
   * Frees the B-tree root page. Other tree MUST already have been freed.
   * 
   * @param[in] space           space where created
   * @param[in] root_page_no    Root page number
   * @param[in,out] mtr         A mini-transaction which has already been started
   */
  void free_root(space_id_t space, page_no_t root_page_no, mtr_t *mtr) noexcept;

  /**
   * Makes tree one level higher by splitting the root, and inserts
   * the tuple. It is assumed that mtr contains an x-latch on the tree.
   * NOTE that the operation of this function must always succeed,
   * we cannot reverse it: therefore enough free disk space must be
   * guaranteed to be available before this function is called.
   * 
   * @param[in] cursor          Cursor at which to insert: must be on the
   *                            root page; when the function returns, the
   *                            cursor is positioned on the predecessor
   *                            of the inserted record
   * @param[in] tuple           Tuple to insert
   * @param[in] n_ext           Number of externally stored columns
   * @param[in,out] mtr         Mini-transaction.
   * 
   * @return	inserted record
   */
  [[nodiscard]] rec_t *root_raise_and_insert(Btree_cursor *cursor, const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept;

  /**
   * Reorganizes an index page.
   * 
   * IMPORTANT: if btr_page_reorganize() is invoked on a compressed leaf
   * page of a non-clustered index, the caller must update the insert
   * buffer free bits in the same mini-transaction in such a way that the
   * modification will be redo-logged.
   * 
   * @param[in] block           Page to be reorganized
   * @param[in] index           Record descriptor
   * @param[in] mtr             Mini-transaction
   * 
   * @return	true on success, false on failure
   */
  [[nodiscard]] bool page_reorganize(Buf_block *block, const Index *index, mtr_t *mtr) noexcept;

  /**
   * Decides if the page should be split at the convergence point of
   * inserts converging to left.
   * 
   * @param[in] cursor          Cursor at which to insert
   * @param[out] split_rec      If split recommended, the first record on
   *                            upper half page, or nullptr if tuple should be first
   * 
   * @return	true if split recommended
   */
  [[nodiscard]] bool page_get_split_rec_to_left(Btree_cursor *cursor, rec_t *&split_rec) noexcept;

  /**
   * Decides if the page should be split at the convergence point of
   * inserts converging to right.
   * 
   * @param[in] cursor          Cursor at which to insert
   * @param[in] split_rec       If split recommended, the first record on
   *                            upper half page, or nullptr if tuple should
   *                            be first.
   * 
   * @return	true if split recommended
   */
  [[nodiscard]] bool page_get_split_rec_to_right(Btree_cursor *cursor, rec_t *&split_rec) noexcept;

  /**
   * Splits an index page to halves and inserts the tuple. It is assumed
   * that mtr holds an x-latch to the index tree. NOTE: the tree x-latch is
   * released within this function! NOTE that the operation of this
   * function must always succeed, we cannot reverse it: therefore enough
   * free disk space (2 pages) must be guaranteed to be available before
   * this function is called.
   * 
   * @param[in] cursor          Cursor at which to insert; when the function
   *                            returns, the cursor is positioned on the
   *                            predecessor of the inserted record
   * @param[in] tuple           Tuple to insert
   * @param[in] n_ext           Number of externally stored columns
   * @param[in,out] mtr         Mini-transaction
   * 
   * @return inserted record
   */
  [[nodiscard]] rec_t *page_split_and_insert(Btree_cursor *cursor, const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept;

  /**
   * Inserts a data tuple to a tree on a non-leaf level. It is assumed
   * that mtr holds an x-latch on the tree.
   * 
   * @param[in] index           Index
   * @param[in] level           Level, must be > 0
   * @param[in] tuple           The record to be inserted
   * @param[in] file            File name
   * @param[in] line            Line where called
   * @param[in,out] mtr         Mini-transaction.
   */
  void insert_on_non_leaf_level(Index *index, ulint level, DTuple *tuple, mtr_t *mtr, Source_location location) noexcept;

  /**
   * Sets a record as the predefined minimum record.
   * 
   * @param[in,out] rec         Record
   * @param[in,out] mtr         Mini-transaction.
   */
  void set_min_rec_mark(rec_t *rec, mtr_t *mtr) noexcept;

  /**
   * Deletes on the upper level the node pointer to a page.
   * 
   * @param[in,out] index       Index tree
   * @param[in,out] block       Page whose node pointer is deleted
   * @param[in,out] mtr         Mini-transaction.
   */
  void node_ptr_delete(Index *index, Buf_block *block, mtr_t *mtr) noexcept;

  #ifdef UNIV_DEBUG
  /**
   * Checks that the node pointer to a page is appropriate.
   * @param[in,out] index       Index tree
   * @param[in,out] block       Index page
   * @param[in,out] mtr         Mini-transaction
   * @return	true
   */
  [[nodiscard]] bool check_node_ptr(Index *index, Buf_block *block, mtr_t *mtr) noexcept;
  #endif /* UNIV_DEBUG */

  /**
   *  Tries to merge the page first to the left immediate brother if such a
   *  brother exists, and the node pointers to the current page and to the
   *  brother reside on the same page. If the left brother does not satisfy
   *  these conditions, looks at the right brother. If the page is the only one
   *  on that level lifts the records of the page to the father page, thus
   *  reducing the tree height. It is assumed that mtr holds an x-latch on the
   *  tree and on the page. If cursor is on the leaf level, mtr must also hold
   *  x-latches to the brothers, if they exist.
   * 
   * @param[in,out] cursor      Cursor on the page to merge or
   *                            lift; the page must not be empty: in
   *                            record delete use btr_discard_page if the
   *                            page would become empty
   * @param[in,out] mtr         Mini-transaction.
   *
   * @return	true on success
   */
  [[nodiscard]] bool compress(Btree_cursor *cursor, mtr_t *mtr) noexcept;

  /**
   * Discards a page from a B-tree. This is used to remove the last record from
   * a B-tree page: the whole page must be removed at the same time. This cannot
   * be used for the root page, which is allowed to be empty.
   * 
   * @param[in,out] cursor      Cursor on the page to discard: not on the root page
   * @param[in,out] mtr         Mini-transaction.
   */
  void discard_page(Btree_cursor *cursor, mtr_t *mtr) noexcept;

  /**
   * Parses the redo log record for setting an index record as the predefined
   * minimum record.
   * 
   * @param[in,out] ptr,        Buffer
   * @param[in,out] end_ptr     Buffer end
   * @param[in,out] page        Page or nullptr
   * @param[in,out] mtr         Mini-transaction or nullptr
   * 
   * @return	end of log record or nullptr
   */
  [[nodiscard]] byte *parse_set_min_rec_mark(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) noexcept;

  /**
   * Parses a redo log record of reorganizing a page.
   * 
   * @param[in,out] ptr         Buffer
   * @param[in,out] end_ptr     Buffer end
   * @param[in,out] index       Record descriptor
   * @param[in,out] block       Page to be reorganized, or nullptr
   * @param[in,out] mtr         Mini-transaction or nullptr
   * 
   * @return	end of log record or nullptr
   */
  [[nodiscard]] byte *parse_page_reorganize(byte *ptr, byte *end_ptr, Index *index, Buf_block *block, mtr_t *mtr) noexcept;

  /**
   * Gets the number of pages in a B-tree.
   * 
   * @param[in] index           Index
   * @param[in] flag            BTR_N_LEAF_PAGES or BTR_TOTAL_SIZE
   * 
   * @return	number of pages
   */
  [[nodiscard]] ulint get_size(const Index *index, ulint flag) noexcept;

  /**
   * Allocates a new file page to be used in an index tree. NOTE: we assume
   * that the caller has made the reservation for free extents!
   * 
   * @param[in] index           Index tree
   * @param[in] hint_page_no    Hint of a good page
   * @param[in] file_direction  Direction where a possible page split is made
   * @param[in] level           Level where the page is placed in the tree
   * @param[in] mtr             Mini-transaction
   * 
   * @return	new allocated block, x-latched; nullptr if out of space
   */
  [[nodiscard]] Buf_block *page_alloc(const Index *index, page_no_t hint_page_no, byte file_direction, ulint level, mtr_t *mtr) noexcept;

  /**
   * Frees a file page used in an index tree. NOTE: cannot free field external
   *  storage pages because the page must contain info on its level.
   * 
   * @param[in,out] index       Index tree
   * @param[in,out] block       Block to be freed, x-latched
   * @param[in,out] mtr         Mini-transaction.
   */
  void page_free(const Index *index, Buf_block *block, mtr_t *mtr) noexcept;

  /**
   * Frees a file page used in an index tree. Can be used also to BLOB
   * external storage pages, because the page level 0 can be given as an
   * argument.
   * 
   * @param[in,out] index       Index tree
   * @param[in,out] block       Block to be freed, x-latched
   * @param[in] level           Page level
   * @param[in,out] mtr         Mini-transaction
   */
  void page_free_low(const Index *index, Buf_block *block, ulint level, mtr_t *mtr) noexcept;

  /**
   * Checks the size and number of fields in a record based on the definition of
   * the index.
   * @param[in,out] rec         Index record
   * @param[in,out] index       Index
   * @param[in] dump_on_error   true if the function should print hex dump of
   *                            record and page on error
   * @return	true if ok
   */
  [[nodiscard]] bool index_rec_validate(const rec_t *rec, const Index *index, bool dump_on_error) noexcept;

  /**
   * Checks the consistency of an index tree.
   * 
   * @param[in,out] index       Index
   * @param[in,out] trx         Transaction or nullptr
   * 
   * @return	true if ok
   */
  [[nodiscard]] bool validate_index(Index *index, Trx *trx) noexcept;

  /**
   * Gets a buffer page and declares its latching order level.
   * 
   * @param[in] space_id        Space id
   * @param[in] page_no         Page number
   * @param[in] rw_latch        Latch mode
   * @param[in,out] mtr         Mini-transaction.
   * 
   * @return	buffer block
   */
  [[nodiscard]]inline Buf_block *block_get(space_id_t space_id, page_no_t page_no, ulint rw_latch, mtr_t *mtr) noexcept {
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

  /**
   * Gets a buffer page and declares its latching order level.
   * 
   * @param[in] space           Space id
   * @param[in] page_no         Page number
   * @param[in] mode            Latch mode
   * @param[in,out] mtr         Mini-transaction.
   * 
   * @return	page
   */
  [[nodiscard]] inline page_t *page_get(space_id_t space, page_no_t page_no, ulint rw_latch, mtr_t *mtr) noexcept {
    return block_get(space, page_no, rw_latch, mtr)->get_frame();
  }

  /**
   * Sets the index id field of a page.
   * 
   * @param[in,out] page        Page to be created
   * @param[in] id              Index id
   * @param[in,out] mtr         Mini-transaction.
   */
  inline void page_set_index_id(page_t *page, uint64_t id, mtr_t *mtr) noexcept {
    mlog_write_uint64(page + (PAGE_HEADER + PAGE_INDEX_ID), id, mtr);
  }

  /** Gets the index id field of a page.
   * 
   * @param[in,out] page             Get index ID from this page
   * 
   * @return	index id
   */
  [[nodiscard]] inline uint64_t page_get_index_id(const page_t *page) noexcept {
    return mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID);
  }

  /** Gets the node level field in an index page.
   * 
   * @param[in,out] page             Get node level from this page
   * 
   * @return	level, leaf level == 0
   */
  [[nodiscard]] inline ulint page_get_level_low(const page_t *page) noexcept {
    const auto level = mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL);

    ut_ad(level <= BTR_MAX_DEPTH);

    return level;
  }

  /** Gets the node level field in an index page.
   * 
   * @param[in,out] page             Get node level from this page
   * 
   * @return	level, leaf level == 0
   */
  [[nodiscard]] inline ulint page_get_level(const page_t *page, mtr_t *) noexcept {
    return page_get_level_low(page);
  }

  /**
   * Sets the node level field in an index page.
   * 
   * @param[in,out] page             Index page
   * @param[in] level                leaf level == 0
   * @param[in,out] mtr              Mini-transaction
   */
  inline void page_set_level(page_t *page, ulint level, mtr_t *mtr) noexcept {
    ut_ad(level <= BTR_MAX_DEPTH);

    /** 16 levels is a very big tree. */
    ut_ad(level < 16);

    mlog_write_ulint(page + (PAGE_HEADER + PAGE_LEVEL), level, MLOG_2BYTES, mtr);
  }

  /** Gets the next index page number.
   * 
   * @param[in] page                 Index page
   * 
   * @return	next page number
   */
  [[nodiscard]] static inline page_no_t page_get_next(const page_t *page, mtr_t *mtr) noexcept {
    ut_ad(mtr->memo_contains_page(page, MTR_MEMO_PAGE_X_FIX) || mtr->memo_contains_page(page, MTR_MEMO_PAGE_S_FIX));

    return mach_read_from_4(page + FIL_PAGE_NEXT);
  }

  /**
   * Sets the next index page field.
   * 
   * @param[in,out] page             Index page
   * @param[in] next                 Next page number
   * @param[in,out]                  Mini-transaction.
   */
  inline void static page_set_next(page_t *page, page_no_t next, mtr_t *mtr) noexcept {
    mlog_write_ulint(page + FIL_PAGE_NEXT, next, MLOG_4BYTES, mtr);
  }

  /** Gets the previous index page number.
   * 
   * @param[in] page                 Index page
   * 
   * @return	prev page number
   */
  [[nodiscard]] static inline page_no_t page_get_prev(const page_t *page, mtr_t *) noexcept {
    return mach_read_from_4(page + FIL_PAGE_PREV);
  }

  /**
   * Sets the previous index page field.
   * 
   * @param[in,out] page             Index page
   * @param[in] prev                 Prev page number
   * @param[in,out]                  Mini-transaction.
   */
  inline void static page_set_prev(page_t *page, page_no_t prev, mtr_t *mtr) noexcept {
    mlog_write_ulint(page + FIL_PAGE_PREV, prev, MLOG_4BYTES, mtr);
  }

  /**
   * Gets the child node file address in a node pointer.
   * NOTE: the offsets array must contain all offsets for the record since
   * we read the last field according to offsets and assume that it contains
   * the child page number. In other words offsets must have been retrieved
   * with Phy_rec::get_col_offsets(n_fields=ULINT_UNDEFINED).
   * 
   * @param[in] rec                Node pointer record 
   * @param[in] offsets            Array returned by Phy_rec::get_col_offsets()
   * @return	child node address
   */
  [[nodiscard]] inline ulint node_ptr_get_child_page_no(const rec_t *rec, const ulint *offsets) noexcept {
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

  /**
   * Releases the latches on a leaf page and bufferunfixes it. 
   * 
   * @param[in,out] block,           Buffer block
   * @param[in] latch_mode           BTR_SEARCH_LEAF or BTR_MODIFY_LEAF
   * @param[in,out] mtr              Mini-transaction
   */
  inline void leaf_page_release(Buf_block *block, ulint latch_mode, mtr_t *mtr) noexcept {
    ut_ad(!mtr->memo_contains(block, MTR_MEMO_MODIFY));
    ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);

    mtr->memo_release(block, latch_mode == BTR_SEARCH_LEAF ? MTR_MEMO_PAGE_S_FIX : MTR_MEMO_PAGE_X_FIX);
  }

  /**
   * Creates a new B-tree instance.
   * 
   * @param lock_sys Lock manager to use
   * @param fsp File segment manager to use
   * @param buf_pool Buffer pool to use
   * 
   * @return Newly created B-tree instance
   */
  static Btree *create(Lock_sys *lock_sys, FSP *fsp, Buf_pool *buf_pool) noexcept;

  /**
   * Destroys a B-tree instance.
   * 
   * @param tree B-tree instance to destroy
   */
  static void destroy(Btree *&btree) noexcept;

#ifdef UNIT_TEST
private:
#endif /* UNIT_TEST */

#ifdef UNIV_BTR_DEBUG
  /**
   * Validates the root file segment.
   *
   * This function checks the validity of the root file segment by verifying the segment header
   * and the tablespace identifier. It ensures that the offset is within the valid range.
   *
   * @param seg_header          The segment header to validate.
   * @param space               The tablespace identifier.
   * 
   * @return Returns true if the root file segment is valid, otherwise false.
   */
  [[nodiscard]] static bool root_fseg_validate(const fseg_header_t *seg_header, space_id_t space) noexcept;
  #endif /* UNIV_BTR_DEBUG */

  /**
   * Gets the root node of a tree and x-latches it.
   *
   * @param[in,out] index       An index
   * @param[in,out] mtr         Mini-tranaction covering the operation.
   * @return root page, x-latched
   */
  [[nodiscard]] Buf_block *root_block_get(Page_id page_id, mtr_t *mtr) noexcept;

  /**
   * Creates a new index page (not the root, and also not
   * used in page reorganization).  @see page_empty().
   *
   * @param[in,out] block       Page to be created.
   * @param[in,out] index       The index in which to create
   * @param[in] level           Btree level of the page
   * @param[in,out] mtr         Mini-transaction coverring the operation.
   */
   void page_create(Buf_block *block, Index *index, ulint level, mtr_t *mtr) noexcept;

  /**
   * Sets the child node file address in a node pointer.
   *
   * @param[in,out] rec         Node pointer record
   * @param[in] offsets         Array returned by Phy_rec::get_col_offsets()
   * @param[in] page_no         child node address
   * @param[in,out] mtr         Mini-transaction covering the operation.
   */
  void node_ptr_set_child_page_no(rec_t *rec, const ulint *offsets, ulint page_no, mtr_t *mtr) noexcept;

  /**
   * Returns the child page of a node pointer and x-latches it.
   *
   * @param[in] node_ptr        Node pointer
   * @param[in,out] index       Dict index tree
   * @param[in] offsets         returned by Phy_rec::get_col_offsets()
   * @param[in,out] mtr         Mini-transaction covering the operation.
   * 
   * @return child page, x-latched
   */
  [[nodiscard]] Buf_block *node_ptr_get_child(const rec_t *node_ptr, Index *index, const ulint *offsets, mtr_t *mtr) noexcept;

  /**
   * Returns the upper level node pointer to a page. It is assumed that mtr holds
   * an x-latch on the tree.
   *
   * @param[in] node_ptr        Node pointer
   * @param[in,out] index       Dict index tree
   * @param[in] offsets         Array returned by Phy_rec::get_col_offsets()
   * @param[in,out] mtr         Mini-transaction covering the operation.
   * 
   * @return The father node pointer as an array of offsets.
   */
  [[nodiscard]] ulint *page_get_father_node_ptr(ulint *offsets, mem_heap_t *heap, Btree_cursor *cursor, mtr_t *mtr, Source_location location) noexcept;

  /**
   * Returns the upper level node pointer to a page.
   * It is assumed that mtr holds an x-latch on the tree.
   *
   * @param[in] offsets         Work area for the return value.
   * @param[in] heap            Memory heap to use.
   * @param[in] index           B-tree index.
   * @param[in] block           Child page in the index.
   * @param[in] mtr             Mini-transaction handle.
   * @param[out] cursor         Cursor on node pointer record, its page x-latched.
   *
   * @return Rec::get_col_offsets() of the node pointer record.
   */
  [[nodiscard]] ulint *page_get_father_block(ulint *offsets, mem_heap_t *heap, Index *index, Buf_block *block, mtr_t *mtr, Btree_cursor *cursor) noexcept;

  /**
   * Seeks to the upper level node pointer to a page.
   * It is assumed that mtr holds an x-latch on the tree.
   *
   * @param[in] index           B-tree index
   * @param[in] block           Child page in the index
   * @param[in] mtr             Mini-transaction handle
   * @param[out] cursor         Cursor on node pointer record, its page x-latched
   */
  void page_get_father(Index *index, Buf_block *block, mtr_t *mtr, Btree_cursor *cursor) noexcept;

  /**
   * Reorganizes an index page.
   *
   * @param[in] recovery        True if called in recovery: locks should not be updated,
   *                            i.e., there cannot exist locks on the page, and a hash
   *                            index cannot be used.
   * @param[in] block           Page to be reorganized.
   * @param[in] index           Record descriptor.
   * @param[in] mtr             Mini-transaction handle.
   *
   * @return True on success, false on failure.
   */
  [[nodiscard]] bool page_reorganize_low(bool recovery, Buf_block *block, const Index *index, mtr_t *mtr) noexcept;

  /**
   * Empties an index page.
   *
   * @param[in,out] block       Page to emptied.
   * @param[in] level           Btree level of the page
   * @param[in,out] mtr         Min-transaction covering the operation.
   */
  void page_empty(Buf_block *block, Index *index, ulint level, mtr_t *mtr) noexcept;

  /**
   * Calculates a split record such that the tuple will certainly fit on
   * its half-page when the split is performed. We assume in this function
   * only that the cursor page has at least one user record.
   * 
   * @param[in] cursor          The cursor at which the insert should be made.
   * @param[in] tuple           The tuple to insert.
   * @param[in] n_ext           Number of externally stored columns.
   * 
   * @return split record, or nullptr if tuple will be the first record on
   *  the lower or upper half-page (determined by btr_page_tuple_smaller())
   */
  [[nodiscard]] rec_t *page_get_split_rec(Btree_cursor *cursor, const DTuple *tuple, ulint n_ext) noexcept;

  /**
   * Returns true if the insert fits on the appropriate half-page with the
   * chosen split_rec.
   *
   * @param[in] cursor          The cursor at which the insert should be made.
   * @param[in] split_rec       Suggestion for the first record on the upper half-page,
   *                            or nullptr if the tuple to be inserted should be first.
   * @param[in] offsets         Offsets of the split_rec record.
   * @param[in] tuple           The tuple to insert.
   * @param[in] n_ext           Number of externally stored columns.
   * @param[in] heap            Temporary memory heap.
   * @return	true if fits
   */
  [[nodiscard]] bool page_insert_fits(Btree_cursor *cursor, const rec_t *split_rec, const ulint *offsets, const DTuple *tuple, ulint n_ext, mem_heap_t *heap) noexcept;

  /**
   * Attaches the halves of an index page on the appropriate level in an
   * index tree.
   *
   * @param[in] index           The index tree.
   * @param[in,out] block       Page to be split.
   * @param[in] split_rec       First record on upper half page.
   * @param[in,out] new_block   The new half page.
   * @param[in] direction       FSP_UP or FSP_DOWN.
   * @param[in] mtr             Mini-transaction handle.
   */
  void attach_half_pages(Index *index, Buf_block *block, rec_t *split_rec, Buf_block *new_block, ulint direction, mtr_t *mtr) noexcept;

  /**
   * Determine if a tuple is smaller than any record on the page.
   *
   * @param[in] cursor          b-tree cursor
   * @param[in] tuple           tuple to consider
   * @param[in] offsets         temporary storage
   * @param[in] n_uniq          number of unique fields in the index page records
   * @param[in] heap            heap for offsets
   * 
   * @return true if smaller
   */
  [[nodiscard]] bool page_tuple_smaller(Btree_cursor *cursor, const DTuple *tuple, ulint *offsets, ulint n_uniq, mem_heap_t **heap) noexcept;

  /**
   * Removes a page from the level list of pages.
   *
   * @param[in] space            The space where the page is removed.
   * @param[in] page             The page to remove.
   * @param[in,out] mtr          The mini-transaction handle.
   */
  void level_list_remove(space_id_t space, page_t *page, mtr_t *mtr) noexcept;

  /**
   * Writes the redo log record for setting an index record as the predefined
   * minimum record.
   *
   * @param[in] rec              The record to be marked as the minimum record.
   * @param[in] type             The type of log record, either MLOG_COMP_REC_MIN_MARK or MLOG_REC_MIN_MARK.
   * @param[in] mtr              The mini-transaction handle.
   */
  void set_min_rec_mark_log(rec_t *rec, mlog_type_t type, mtr_t *mtr) noexcept;

  /**
   * If page is the only on its level, this function moves its records to the
   * father page, thus reducing the tree height.
   *
   * @param[in] index             Index tree
   * @param[in] block             Page which is the only on its level; must not be empty;
   *                              use btr_discard_only_page_on_level if the last record from the page should be removed
   * @param[in] mtr               The mini-transaction handle.
   */
  void lift_page_up(Index *index, Buf_block *block, mtr_t *mtr) noexcept;

  /**
   * Discards a page that is the only page on its level. This will empty
   * the whole B-tree, leaving just an empty root page. This function
   * should never be reached, because btr_compress(), which is invoked in
   * delete operations, calls btr_lift_page_up() to flatten the B-tree.
   *
   * @param[in] index           The index tree.
   * @param[in] block           The page which is the only on its level.
   * @param[in] mtr             The mini-transaction handle.
   */
  void discard_only_page_on_level(Index *index, Buf_block *block, mtr_t *mtr) noexcept;

#ifdef UNIV_BTR_PRINT
  /**
   * Prints the size information of the B-tree index.
   *
   * @param[in] index  The index tree to print the size information for.
   */
  void print_size(Index *index) noexcept;

  /**
   * Prints recursively index tree pages.
   *
   * @param[in] index           The index tree.
   * @param[in] block           The index page.
   * @param[in] width           Print this many entries from start and end.
   * @param[in,out] heap        Heap for Phy_rec::get_col_offsets().
   * @param[in,out] offsets     Buffer for Phy_rec::get_col_offsets().
   * @param[in] mtr             The mini-transaction handle.
   */
  void print_recursive(Index *index, Buf_block *block, ulint width, mem_heap_t **heap, ulint **offsets, mtr_t *mtr) noexcept;

  /**
   * Prints the index tree.
   *
   * @param[in] index           The index tree to print.
   * @param[in] width          Print this many entries from start and end.
   */
  void print_index(Index *index, ulint width) noexcept;
#endif /* UNIV_BTR_PRINT */

  /**
   * Display identification information for a record.
   *
   * @param[in] page            The index page containing the record.
   * @param[in] rec             The index record to display information for.
   * @param[in] index           The index to which the record belongs.
   */
  void index_rec_validate_report(const page_t *page, const rec_t *rec, const Index *index) noexcept;

  /**
   * Checks the size and number of fields in records based on the definition of
   * the index.
   * 
   * @param[in] block           Index page
   * @param[in] index           Index tree
   * 
   * @return true if ok
   */
  [[nodiscard]] bool index_page_validate(Buf_block *block, Index *index) noexcept;

  /**
   * Report an error on a single page of an index tree.
   *
   * @param[in] index           The index tree.
   * @param[in] level           The B-tree level.
   * @param[in] block           The index page.
   */
  void validate_report1(Index *index, ulint level, const Buf_block *block) noexcept; 

  /**
   * Report an error on two pages of an index tree.
   *
   * @param[in] index           The index tree.
   * @param[in] level           The B-tree level.
   * @param[in] block1          The first index page.
   * @param[in] block2          The second index page.
   */
  void validate_report2(const Index *index, ulint level, const Buf_block *block1, const Buf_block *block2) noexcept;

  /**
   * Validates index tree level.
   *
   * @param[in] index           The index tree.
   * @param[in] trx             The transaction or nullptr.
   * @param[in] level           The level number.
   * 
   * @return true if ok.
   */
  [[nodiscard]] bool validate_level(Index *index, Trx *trx, ulint level) noexcept;

  /** Lock manager to use */
  Lock_sys *m_lock_sys{};

  /** File segment manager to use */

  FSP *m_fsp{};

  /** Buffer pool to use */
  Buf_pool *m_buf_pool{};
};
