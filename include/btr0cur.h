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

/*** @file include/btr0cur.h
The index tree cursor

Created 10/16/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0btr.h"
#include "dict0dict.h"
#include "innodb0types.h"
#include "page0cur.h"

struct FSP;
struct Btree;
struct Buf_pool;
struct Lock_sys;

/** Mode flags for Btree cursor operations; these can be ORed */

/** Do no undo logging */
constexpr ulint BTR_NO_UNDO_LOG_FLAG = 1;

/** Do no record lock checking */
constexpr ulint BTR_NO_LOCKING_FLAG = 2;

/** Sys fields will be found from the   update vector or inserted entry */
constexpr ulint BTR_KEEP_SYS_FLAG = 4;

#include "que0types.h"
#include "row0types.h"

#define BTR_CUR_ADAPT
#define BTR_CUR_HASH_ADAPT

/** In the pessimistic delete, if the page data size drops below this
limit, merging it to a neighbor is tried */
constexpr ulint BTR_CUR_PAGE_LOW_FILL_LIMIT = UNIV_PAGE_SIZE / 2;

  /** If pessimistic delete fails because of lack of file space, there
  is still a good change of success a little later.  Try this many
  times. */
  constexpr ulint BTR_CUR_RETRY_DELETE_N_TIMES = 100;
  /** If pessimistic delete fails because of lack of file space, there
  is still a good change of success a little later.  Sleep this many
  microseconds between retries. */
  constexpr ulint BTR_CUR_RETRY_SLEEP_TIME = 50000;

  

/** Size of path array (in slots) */
constexpr ulint BTR_PATH_ARRAY_N_SLOTS = 250;

struct Btree_cursor {
  /**
   * A slot in the path array. We store here info on a search path down the tree.
   * Each slot contains data on a single level of the tree.
   */
  struct Path {
    /** Index of the record where the page cursor stopped on this level
      (index in alphabetical order); value ULINT_UNDEFINED denotes array end */
    ulint m_nth_rec;

    /** Number of records on the page */
    ulint m_n_recs;
  };

  /**
   * Values for the flag documenting the used search method.
   */
  enum Method {
    /** Unset */
    BTR_CUR_NONE = 0,

    /** Successful shortcut using the hash index. */
    BTR_CUR_HASH = 1,

    /** Failure using hash, success using binary search: the misleading
      hash reference is stored in the field hash_node, and might be
      necessary to update. */
    BTR_CUR_HASH_FAIL,

    /** Success using the binary search. */
    BTR_CUR_BINARY
  };

  using Paths = std::array<Path, BTR_PATH_ARRAY_N_SLOTS> ;

  Btree_cursor() = delete;

  /**
   * Constructor.
   *
   * @param[in] fsp             The file segment manager.
   * @param[in] btree           The B-tree.
   */
  Btree_cursor(FSP *fsp, Btree *btree) : m_fsp(fsp), m_btree(btree), m_lock_sys(btree->m_lock_sys) {}
  /**
   * Searches to the nth level of the index tree.
   *
   * @param[in,out] paths      The paths array, used in estimating the number of rows in range,
   *                           nullptr otherwise.
   * @param[in,out] index      The index to search in.
   * @param[in] level          The tree level of the search.
   * @param[in] tuple          The data tuple to search for.
   *                           Note that `n_fields_cmp` in the tuple must be set so
   *                           that it cannot get compared to the node ptr page
   *                           number field.
   * @param[in] mode               The search mode.
   *                           Possible values are PAGE_CUR_L, PAGE_CUR_LE,
   *                           PAGE_CUR_GE, etc.
   *                           If the search is made using a unique prefix of a
   *                           record, mode should be PAGE_CUR_LE, not PAGE_CUR_GE,
   *                           as the latter may end up on the previous page of
   *                           the record.
   *                           Inserts should always be made using PAGE_CUR_LE to
   *                           search the position.
   * @param[in] latch_mode     The latch mode.
   *                           Possible values are BTR_SEARCH_LEAF, BTR_INSERT,
   *                           BTR_ESTIMATE, etc.
   *                           The cursor's `left_block` is used to store a
   *                           pointer to the left neighbor page in the cases
   *                           BTR_SEARCH_PREV and BTR_MODIFY_PREV.
   *                           Possible values are RW_S_LATCH or 0.
   * @param[in] mtr            The mini-transaction handle.
   * @param[in] loc            The source location.
   */
  void search_to_nth_level(
    Paths *paths,
    const Index *index,
    ulint level,
    const DTuple *tuple,
    ulint mode,
    ulint latch_mode,
    mtr_t *mtr,
    Source_location loc
  ) noexcept;

  /**
   * Opens a cursor at either end of an index.
   *
   * @param[in,out] paths      The paths array, used in estimating the number of rows in range,
   *                           nullptr otherwise.
   * @param[in] from_left       true if open to the low end, false if open to the high end
   * @param[in] index           the index to open the cursor on
   * @param[in] latch_mode      the latch mode to use
   * @param[in] level           the level to position at
   * @param[in] mtr             the mini-transaction handle
   * @param[in] loc             the source location
   */
  void open_at_index_side(
    Paths *paths,
    bool from_left,
    const Index *index,
    ulint latch_mode,
    ulint level,
    mtr_t *mtr,
    Source_location source_location) noexcept;

  /**
   * Positions a cursor at a randomly chosen position within a B-tree.
   *
   * @param[in] index           The index.
   * @param[in] latch_mode      The latch mode (BTR_SEARCH_LEAF, ...).
   * @param[in] mtr             The mini-transaction handle.
   * @param[in] loc             The source location.
   */
  void open_at_rnd_pos(const Index *index, ulint latch_mode, mtr_t *mtr, Source_location loc) noexcept;

  /**
   * Tries to perform an insert to a page in an index tree, next to cursor.
   * It is assumed that mtr holds an x-latch on the page. The operation does
   * not succeed if there is too little space on the page. If there is just
   * one record on the page, the insert will always succeed; this is to
   * prevent trying to split a page with just one record.
   *
   * @param[in] flags           Undo logging and locking flags: if not zero, the parameters
   *                            index and thr should be specified
   * @param[in] entry           entry to insert
   * @param[out] rec            Pointer to inserted record if succeed
   * @param[out] big_rec        big rec vector whose fields have to be stored externally by
   *                            the caller, or nullptr
   * @param[in] n_ext           Number of externally stored columns
   * @param[in] thr             Query thread or nullptr
   * @param[in] mtr             If this function returns DB_SUCCESS on a leaf page of
   *                            a secondary index in a compressed tablespace, the mtr must be
   *                            committed before latching any further pages
   *
   * @return DB_SUCCESS, DB_WAIT_LOCK, DB_FAIL, or error number
   */
  [[nodiscard]] db_err optimistic_insert(ulint flags, DTuple *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Performs an insert on a page of an index tree. It is assumed that mtr holds an x-latch
   * on the tree and on the cursor page. If the insert is made on the leaf level, to avoid
   * deadlocks, mtr must also own x-latches to brothers of page, if those brothers exist.
   *
   * @param[in] flags           Undo logging and locking flags: if not zero, the parameter thr should be
   *                            specified; if no undo logging is specified, then the caller must have reserved
   *                            enough free extents in the file space so that the insertion will certainly succeed
   * @param[in] entry           Entry to insert
   * @param[out] rec            Pointer to inserted record if succeed
   * @param[out] big_rec        Big rec vector whose fields have to be stored externally by the caller, or nullptr
   * @param[in] n_ext           Number of externally stored columns
   * @param[in] thr             Query thread or nullptr
   * @param[in] mtr             mtr
   *
   * @return          DB_SUCCESS or error number
   */
  [[nodiscard]] db_err pessimistic_insert(ulint flags, DTuple *entry, rec_t **rec, big_rec_t **big_rec, ulint n_ext, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Updates a record when the update causes no size changes in its fields.
   *
   * @param[in] flags           Undo logging and locking flags
   * @param[in] update          Update vector
   * @param[in] cmpl_info       Compiler info on secondary index updates
   * @param[in] thr             Query thread
   * @param[in] mtr             mtr; must be committed before latching any further pages
   *
   * @return  DB_SUCCESS or error number
   */
  [[nodiscard]] db_err update_in_place(ulint flags, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Tries to update a record on a page in an index tree. It is assumed that mtr
   * holds an x-latch on the page. The operation does not succeed if there is too
   * little space on the page or if the update would result in too empty a page,
   * so that tree compression is recommended.
   *
   * @param[in] flags           Undo logging and locking flags
   * @param[in] update          Update vector; this must also contain trx id and roll
   *                            ptr fields
   * @param[in] cmpl_info       Compiler info on secondary index updates
   * @param[in] thr             Query thread
   * @param[in] mtr             Must be committed before latching any further pages
   *
   * @return DB_SUCCESS, or DB_OVERFLOW if the updated record does not fit,
   *         DB_UNDERFLOW if the page would become too empty.
   */
  [[nodiscard]] db_err optimistic_update(ulint flags, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Performs an update of a record on a page of a tree. It is assumed that mtr holds an x-latch
   * on the tree and on the cursor page. If the update is made on the leaf level, to avoid deadlocks,
   * mtr must also own x-latches to brothers of page, if those brothers exist.
   *
   * @param[in] flags           Undo logging, locking, and rollback flags
   * @param[in] heap            Pointer to memory heap, or nullptr
   * @param[out] big_rec        Big rec vector whose fields have to be stored externally by the caller, or nullptr
   * @param[in] update          Update vector; this is allowed also contain trx id and roll ptr fields,
   *                            but the values in update vector have no effect
   * @param[in] cmpl_info       Compiler info on secondary index updates
   * @param[in] thr             Query thread
   * @param[in] mtr             Must be committed before latching any further pages
   *
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err pessimistic_update(ulint flags, mem_heap_t **heap, big_rec_t **big_rec, const upd_t *update, ulint cmpl_info, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Marks a clustered index record deleted. Writes an undo log record to undo log on this delete marking.
   * Writes in the trx id field the id of the deleting transaction, and in the roll ptr field pointer to the
   * undo log record created.
   *
   * @param[in] flags           Undo logging and locking flags
   * @param[in] val             Value to set
   * @param[in] thr             Query thread
   * @param[in] mtr             Mtr
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, or error number
   */
  [[nodiscard]] db_err del_mark_set_clust_rec(ulint flags, bool val, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Sets a secondary index record delete mark to true or false.
   *
   * @param[in] flags           Locking flag
   * @param[in] val             Value to set
   * @param[in] thr             Query thread
   * @param[in] mtr             Mtr
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, or error number
   */
  [[nodiscard]] db_err del_mark_set_sec_rec(ulint flags, bool val, que_thr_t *thr, mtr_t *mtr) noexcept;

  /**
   * Removes the record on which the tree cursor is positioned. It is assumed that
   * the mtr has an x-latch on the page where the cursor is positioned, but no
   * latch on the whole tree.
   *
   * Cursor on the record to delete; cursor stays valid: if deletion succeeds,
   * on function exit it points to the successor of the deleted record.
   * 
   * @param[in] mtr             Mtr; if this function returns true on a leaf page of a
   *                            secondary index, the mtr must be committed before latching
   *                            any further pages
   *
   * @return true on success.
   */
  [[nodiscard]] bool optimistic_delete(mtr_t *mtr) noexcept;

  /**
   * Removes the record on which the tree cursor is positioned. Tries to compress the page if its
   * fillfactor drops below a threshold or if it is the only page on the level. It is assumed that
   * mtr holds an x-latch on the tree and on the cursor page. To avoid deadlocks, mtr must also own
   * x-latches to brothers of page, if those brothers exist.
   * 
   * Cursor on the record to delete; if compression does not occur, the cursor stays valid: it
   * points to Successor of deleted record on function exit
   *
   * @param[out] err            DB_SUCCESS or DB_OUT_OF_FILE_SPACE; the latter may occur because we may have to update node pointers
   *                             on upper levels, and in the case of variable length keys these may actually grow in size
   * @param[in] has_reserved_extents true if the caller has already reserved enough free extents so that he knows that the operation will succeed
   * @param[in] rb_ctx          Rollback context
   * @param[in] mtr             Mini-transaction handle
   *
   * @return                     true if compression occurred
   */
  void pessimistic_delete(db_err *err, bool has_reserved_extents, trx_rb_ctx rb_ctx, mtr_t *mtr) noexcept;

  /**
   * Parses a redo log record of updating a record in-place.
   *
   * @param[in] ptr             Buffer
   * @param[in] end_ptr         Buffer end
   * @param[in] page            Page or nullptr
   * @param[in] index           Index corresponding to page
   *
   * @return End of log record or nullptr
   */
  [[nodiscard]] static byte *parse_update_in_place(byte *ptr, byte *end_ptr, page_t *page, Index *index) noexcept;

  /**
   * Parses the redo log record for delete marking or unmarking of a clustered index record.
   *
   * @param[in] ptr             Buffer
   * @param[in] end_ptr         Buffer end
   * @param[in] page            Page or nullptr
   * @param[in] index           Index corresponding to page
   *
   * @return End of log record or nullptr
   */
  [[nodiscard]] static byte *parse_del_mark_set_clust_rec(byte *ptr, byte *end_ptr, page_t *page, Index *index) noexcept;

  /**
   * Parses the redo log record for delete marking or unmarking of a secondary index record.
   *
   * @param[in] ptr             Buffer
   * @param[in] end_ptr         Buffer end
   * @param[in] page            Page or nullptr
   *
   * @return End of log record or nullptr
   */
  [[nodiscard]] static byte *parse_del_mark_set_sec_rec(byte *ptr, byte *end_ptr, page_t *page) noexcept;

  /**
   * Estimates the number of rows in a given index range.
   *
   * @param[in] index           Index
   * @param[in] tuple1          Range start, may also be empty tuple
   * @param[in] mode1           Search mode for range start
   * @param[in] tuple2          Range end, may also be empty tuple
   * @param[in] mode2           Search mode for range end
   *
   * @return         estimated number of rows
   */
  [[nodiscard]] int64_t estimate_n_rows_in_range(Index *index, const DTuple *tuple1, ulint mode1, const DTuple *tuple2, ulint mode2) noexcept;

  /**
   * Estimates the number of different key values in a given index, for each n-column prefix of the index
   * where n <= Dict::index_get_n_unique(index). The estimates are stored in the array index->stat_n_diff_key_vals.
   *
   * @param[in] index           Index
   */
  void estimate_number_of_different_key_vals(const Index *index) noexcept;

  /**
   * Returns the page cursor component of a tree cursor.
   * 
   * @return Pointer to the page cursor component.
   */
  [[nodiscard]] inline page_cur_t *get_page_cur() noexcept {
    return &m_page_cur;
  }

  /**
   * Returns the page cursor component of a tree cursor.
   * 
   * @return Pointer to the page cursor component.
   */
  [[nodiscard]] inline const page_cur_t *get_page_cur() const noexcept {
    return &m_page_cur;
  }

  /**
   * Returns the buffer block on which the tree cursor is positioned.
   * 
   * @return Pointer to the buffer block.
   */
  [[nodiscard]] inline Buf_block *get_block() noexcept {
    return page_cur_get_block(&m_page_cur);
  }

  /**
   * Returns the record pointer of a tree cursor.
   * 
   * @return Pointer to the record.
   */
  [[nodiscard]] inline rec_t *get_rec() noexcept {
    return page_cur_get_rec(&m_page_cur);
  }

  /**
   * Invalidates a tree cursor by setting the record pointer to nullptr.
   */
  inline void invalidate() noexcept {
    page_cur_invalidate(&m_page_cur);
  }

  /**
   * Returns the page of a tree cursor.
   * 
   * @return Pointer to the page.
   */
  [[nodiscard]] inline page_t *get_page_no() noexcept {
    return page_align(page_cur_get_rec(&m_page_cur));
  }

  /**
   * Returns the index of a cursor.
   * 
   * @return The index.
   */
  [[nodiscard]] inline Index *get_index() noexcept {
    return const_cast<Index *>(m_index);
  }

  /**
   * Positions a tree cursor at a given record.
   * 
   * @param[in] index           The dictionary index.
   * @param[in] rec             The record in the tree.
   * @param[in] block           The buffer block of the record.
   */
  inline void position(const Index *index, rec_t *rec, Buf_block *block) noexcept {
    ut_ad(page_align(rec) == block->m_frame);

    page_cur_position(rec, block, &m_page_cur);

    m_index = index;
  }

  /**
   * Checks if compressing an index page where a btr cursor is placed makes sense.
   * 
   * @param[in] mtr             The minit-transaction
   * 
   * @return True if merge is recommended.
   */
  [[nodiscard]] inline bool compress_recommendation(mtr_t *mtr) noexcept {
    ut_ad(mtr->memo_contains(get_block(), MTR_MEMO_PAGE_X_FIX));

    auto page = get_page_no();

    if (page_get_data_size(page) < BTR_CUR_PAGE_LOW_FILL_LIMIT ||
        (Btree::page_get_next(page, mtr) == FIL_NULL && Btree::page_get_prev(page, mtr) == FIL_NULL)) {

      // The page fillfactor has dropped below a predefined minimum value
      // OR the level in the B-tree contains just one page: we recommend
      // merge if this is not the root page.
      return m_index->get_page_no() != page_get_page_no(page);
    } else {
      return false;
    }
  }

  /**
   * Checks if the record on which the cursor is placed can be deleted without
   * making merge necessary (or, recommended).
   * 
   * @param[in] cursor          The btr cursor.
   * @param[in] rec_size        The size of the record.
   * @param[in] mtr             The mini-transaction
   * 
   * @return True if the record can be deleted without recommended merging.
   */
  [[nodiscard]] inline bool delete_will_underflow(ulint rec_size, mtr_t *mtr) noexcept {
    ut_ad(mtr->memo_contains(get_block(), MTR_MEMO_PAGE_X_FIX));

    auto page = get_page_no();

    if ((page_get_data_size(page) - rec_size < BTR_CUR_PAGE_LOW_FILL_LIMIT) ||
        (Btree::page_get_next(page, mtr) == FIL_NULL && Btree::page_get_prev(page, mtr) == FIL_NULL) ||
        page_get_n_recs(page) < 2) {

      /* The page fillfactor will drop below a predefined minimum value, OR the level in the B-tree contains just one page,
      OR the page will become empty: we recommend merge if this is not the root page. */
      return m_index->get_page_no() == page_get_page_no(page);
    } else {
      return true;
    }
  }

#ifndef UNIT_TESTING
private:
#endif /* UNIT_TESTING */

  

  /**
   * Adds path information to the cursor for the current page, for which
   * the binary search has been performed.
   *
   * @param[in] path            The path array, used in estimating the number of rows in range.
   * @param[in] height          The height of the page in the tree. A value of 0 means leaf node.
   * @param[in] root_height     The height of the root node in the tree.
   */
  void add_path_info(Paths *path, ulint height, ulint root_height) noexcept;

 

  /**
   * Latches the leaf page or pages requested.
   * 
   * @param[in] page            The leaf page where the search converged.
   * @param[in] space           The space id.
   * @param[in] page_no         The page number of the leaf.
   * @param[in] latch_mode      The latch mode BTR_SEARCH_LEAF, BTR_MODIFY_LEAF,
   *                            BTR_MODIFY_TREE, BTR_SEARCH_PREV, or BTR_MODIFY_PREV.
   * @param[in,out] mtr         The mini-transaction handle.
   */
  void latch_leaves(page_t *page, space_id_t space, page_no_t page_no, ulint latch_mode, mtr_t *mtr) noexcept;

    /**
     * Inserts a record if there is enough space, or if enough space can
     * be freed by reorganizing. Differs from optimistic_insert because
     * no heuristics is applied to whether it pays to use CPU time for
     * reorganizing the page or not.
     *
     * @param[in] tuple         Tuple to insert; the size info need not have been stored to tuple
     * @param[in] n_ext         Number of externally stored columns
     * @param[in] mtr           Mini-transaction handle
     *
     * @return Pointer to inserted record if succeed, else nullptr
     */
  [[nodiscard]] rec_t *insert_if_possible(const DTuple *tuple, ulint n_ext, mtr_t *mtr) noexcept;

  /**
   * For an insert, checks the locks and does the undo logging if desired.
   *
   * @param[in] flags           Undo logging and locking flags: if not zero, the parameters index and thr should be specified.
   * @param[in] entry           Entry to insert.
   * @param[in] thr             Query thread or nullptr.
   * @param[in] mtr             Mini-transaction.
   * @param[in] inherit         True if the inserted new record maybe should inherit LOCK_GAP type locks from the successor record.
   *
   * @return DB_SUCCESS, DB_WAIT_LOCK, DB_FAIL, or error number.
   */
  [[nodiscard]] inline db_err ins_lock_and_undo(ulint flags, const DTuple *entry, que_thr_t *thr, mtr_t *mtr, bool *inherit) noexcept;

  #ifdef UNIV_DEBUG
  /**
   * Report information about a transaction.
   *
   * @param[in] trx             The transaction.
   * @param[in] index           The index.
   * @param[in] op              The operation.
   */
  void trx_report(Trx *trx, const Index *index, const char *op) noexcept;
  #endif /* UNIV_DEBUG */

  /**
   * For an update, checks the locks and does the undo logging.
   *
   * @param[in] flags           The undo logging and locking flags.
   * @param[in] update          The update vector.
   * @param[in] cmpl_info       The compiler info on secondary index updates.
   * @param[in] thr             The query thread.
   * @param[in] mtr             The mini-transaction.
   * @param[in] roll_ptr        The roll pointer.
   * 
   * @return DB_SUCCESS, DB_WAIT_LOCK, or error number
   */
  [[nodiscard]] inline db_err upd_lock_and_undo(
    ulint flags,
    const upd_t *update,
    ulint cmpl_info,
    que_thr_t *thr,
    mtr_t *mtr,
    roll_ptr_t *roll_ptr
  ) noexcept;

  /**
   * Writes a redo log record of updating a record in-place.
   *
   * @param[in] flags           Flags for the operation.
   * @param[in] rec             The record to be updated.
   * @param[in] index           The index where the cursor is positioned.
   * @param[in] update          The update vector.
   * @param[in] trx             The transaction.
   * @param[in] roll_ptr        The roll pointer.
   * @param[in] mtr             The mini-transaction handle.
   */
  inline void update_in_place_log(
    ulint flags,
    rec_t *rec,
    const Index *index,
    const upd_t *update,
    Trx *trx,
    roll_ptr_t roll_ptr,
    mtr_t *mtr
  ) noexcept;

  /**
   * Restores the correct locks for the new supremum record created during a split.
   *
   * If, in a split, a new supremum record was created as the predecessor of the
   * updated record, the supremum record must inherit exactly the locks on the
   * updated record. In the split, it may have inherited locks from the successor
   * of the updated record, which is not correct. This function restores the
   * right locks for the new supremum.
   *
   * @param[in] block           Buffer block of the record.
   * @param[in] rec             Updated record.
   * @param[in] mtr             Mini-transaction handle.
   */
  void pess_upd_restore_supremum(Buf_block *block, const rec_t *rec, mtr_t *mtr) noexcept;

  /**
   * Writes the redo log record for delete marking or unmarking of an index record.
   *
   * @param[in] flags           Flags for the operation.
   * @param[in] rec             The record to be marked or unmarked.
   * @param[in] index           The index of the record.
   * @param[in] val             The value to set (true for marking, false for unmarking).
   * @param[in] trx             The transaction performing the operation.
   * @param[in] roll_ptr        The roll pointer to the undo log record.
   * @param[in] mtr             The mini-transaction handle.
   */
  inline void del_mark_set_clust_rec_log(
    ulint flags,
    rec_t *rec,
    const Index *index,
    bool val,
    Trx *trx,
    roll_ptr_t roll_ptr,
    mtr_t *mtr) noexcept;

  /**
   * Writes the redo log record for a delete mark setting of a secondary
   * index record.
   *
   * @param[in] rec             The record to modify.
   * @param[in] val             The value to set for the delete mark.
   * @param[in] mtr             The mini-transaction handle.
   */
  inline void del_mark_set_sec_rec_log(rec_t *rec, bool val, mtr_t *mtr) noexcept;

  
  /**
   * Gets the file interface.
   * 
   * @return File interface
   */
  inline Fil *get_fil() noexcept {
    return m_fsp->m_fil;  
  }

  /**
   * Gets the buffer pool.
   * 
   * @return Buffer pool
   */
  inline Buf_pool *get_buf_pool() noexcept {
    return m_fsp->m_buf_pool;
  }

  /**
   * Gets the B-tree system.
   * 
   * @return B-tree system
   */
  inline Btree *get_btree() noexcept {
    return m_btree;
  }

  /**
   * Gets the lock system.
   * 
   * @return Lock system
   */
  inline Lock_sys *get_lock_sys() noexcept {
    return m_lock_sys;
  }

#ifndef UNIT_TESTING
private:
#endif /* UNIT_TESTING */

  /** Index where positioned */
  const Index *m_index{};

  /** Page cursor */
  page_cur_t m_page_cur{};

  /** This field is used to store a pointer to the left neighbor page, in the
  * cases BTR_SEARCH_PREV and BTR_MODIFY_PREV */
  Buf_block *m_left_block{};

  /** This field is only used when search_to_nth_level is called for an
   * index entry insertion: the calling query thread is passed here to be used
   * in the insert buffer */
  que_thr_t *m_thr{};

  /** Search method used */
  Method m_flag{BTR_CUR_NONE};

  /** Tree height if the search is done for a pessimistic insert or update
  * operation */
  ulint m_tree_height{};

  /** If the search mode was PAGE_CUR_LE, the number of matched fields to the
  * the first user record to the right of the cursor record after
  * search_to_nth_level; for the mode PAGE_CUR_GE, the matched fields
  * to the first user record AT THE CURSOR or to the right of it; NOTE that the
  * up_match and low_match values may exceed the correct values for comparison
  * to the adjacent user record if that record is on a different leaf page!
  * (See the note in row_ins_duplicate_key.) */
  ulint m_up_match{};

  /** number of matched bytes to the right at the time cursor positioned; only
  * used internally in searches: not defined after the search */
  ulint m_up_bytes{};

  /** if search mode was PAGE_CUR_LE, the number of matched fields to the first
  * user record AT THE CURSOR or to the left of it after
  * search_to_nth_level; NOT defined for PAGE_CUR_GE or any other search
  * modes; see also the NOTE in up_match! */
  ulint m_low_match{};

  /** number of matched bytes to the right at the time cursor positioned; only
   * used internally in searches: not defined after the search */
  ulint m_low_bytes{};

  /** prefix length used in a hash search if hash_node != nullptr */
  ulint m_n_fields{};

  /** hash prefix bytes if hash_node != nullptr */
  ulint m_n_bytes{};

  /** fold value used in the search if flag is BTR_CUR_HASH */
  ulint m_fold{};

  /** If the following is set to true, this module prints a lot of
  trace information of individual record operations */
  IF_DEBUG(bool m_print_record_ops{};)

  /** The file segment manager */
  FSP *m_fsp{};

  /** The B-tree system */
  Btree *m_btree{}; 

  /** The lock system */
  Lock_sys *m_lock_sys{};
};
