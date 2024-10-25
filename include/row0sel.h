/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/row0sel.h
Select

Created 12/19/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "api0api.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "pars0sym.h"
#include "que0que.h"
#include "read0read.h"
#include "row0types.h"
#include "trx0types.h"

struct Dict;
struct Plan;
struct Lock_sys;
struct sel_buf_t;
struct sel_node_t;

enum class Search_status {
  UNDEFINED = -1,
  FOUND = 0,
  EXHAUSTED = 1,
  RETRY = 2
};

struct Row_sel {
  /*  *
   * @brief Constructor.
   */
  Row_sel(Dict *dict, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Destructor.
   */
  ~Row_sel() = default;

  /**
   * @brief Creates a new Row_sel object.
   */
  [[nodiscard]] static Row_sel *create(Dict *dict, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Destroys a Row_sel object.
   */
  static void destroy(Row_sel *&row_sel) noexcept;

  /**
   * @brief Frees a prefetch buffer for a column.
   *
   * This function frees a prefetch buffer for a column, including the dynamically allocated
   * memory for data stored there.
   *
   * @param[in, own] sel_buf The prefetch buffer to be freed.
     */
  static void free_prefetch_buf(sel_buf_t *sel_buf) noexcept;
  
  /**
   * @brief Performs a select step.
   *
   * This is a high-level function used in SQL execution graphs.
   *
   * @param[in] thr The query thread.
   * 
   * @return The query thread to run next or nullptr.
   */
  [[nodiscard]] que_thr_t *step(que_thr_t *thr) noexcept;

  /**
   * @brief Performs an execution step of an open or close cursor statement node.
   *
   * This function executes a step in the process of opening or closing a cursor
   * statement node within a query execution graph.
   *
   * @param[in] thr The query thread.
   * 
   * @return The query thread to run next or nullptr.
   */
  [[nodiscard]] inline que_thr_t *open_step(que_thr_t *thr) noexcept;

  /**
   * @brief Performs a fetch for a cursor.
   *
   * @param[in] thr The query thread.
   * @return The query thread to run next or nullptr.
   */
  [[nodiscard]] que_thr_t *fetch_step(que_thr_t *thr) noexcept;

  /**
   * @brief Sample callback function for fetch that prints each row.
   *
   * This function is a sample callback used during a fetch operation. It prints each row.
   *
   * @param[in] row The row to be printed (of type sel_node_t*).
   * 
   * @return Always returns a non-nullptr.
   */
  [[nodiscard]] void *fetch_print(void *row) noexcept;

  /**
   * @brief Callback function for fetch that stores an unsigned 4-byte integer to the location pointed.
   *
   * This function is used as a callback during a fetch operation. It stores an unsigned 4-byte integer
   * to the location pointed by the user argument. The column's type must be DATA_INT, DATA_UNSIGNED, 
   * with a length of 4.
   *
   * @param[in] row The row to be processed (of type sel_node_t*).
   * @param[in] user_arg Pointer to the location where the data will be stored.
   * 
   * @return Always returns nullptr.
   */
  [[nodiscard]] void *fetch_store_uint4(void *row, void *user_arg) noexcept;

  /**
   * @brief Prints a row in a select result.
   *
   * This function prints a row in the result of a select query.
   *
   * @param[in] thr The query thread.
   * 
   * @return The query thread to run next or nullptr.
   */
  [[nodiscard]] que_thr_t *printf_step(que_thr_t *thr) noexcept;

  /**
   * @brief Builds a dummy query graph used in selects.
   *
   * This function constructs a dummy query graph for use in select operations.
   *
   * @param[in] prebuilt The prebuilt handle.
   */
  void prebuild_graph(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief This function performs multiple operations including:
   * 
   * 1. Move to/Search for a record
   * 2. Fetch the next record
   * 3. Fetch the previous record
   * 4. Set appropriate locks (gap and table locks)
   * 5. Handle rollback
   * 
   * This function opens a cursor and also implements fetch next and fetch
   * previous operations. Note that if a search is performed with a full key value
   * from a unique index (ROW_SEL_EXACT), the cursor position will not be stored,
   * and fetch next or fetch previous operations must not be attempted on the cursor.
   * 
   * @param[in] recovery Recovery flag.
   * @param[in] mode Search mode.
   * @param[in] prebuilt Prebuilt struct for the table handle; this contains the info
   *   of search_tuple, index; if search tuple contains 0 fields then the cursor is
   *   positioned at the start or the end of the index, depending on 'mode'.
   * @param[in] match_mode Mode for matching the key.
   * @param[in] direction Cursor operation. Note: if this is not ROW_SEL_MOVETO, then
   *  prebuilt must have a pcur with stored position! In opening of a cursor
   *  'direction' should be ROW_SEL_MOVETO.
   * 
   * @return DB_SUCCESS, DB_RECORD_NOT_FOUND, DB_END_OF_INDEX, DB_DEADLOCK,
   *         DB_LOCK_TABLE_FULL, DB_CORRUPTION, or DB_TOO_BIG_RECORD.
   */
  [[nodiscard]] db_err mvcc_fetch(
    ib_recovery_t recovery,
    ib_srch_mode_t mode,
    row_prebuilt_t *prebuilt,
    ib_match_t match_mode,
    ib_cur_op_t direction) noexcept;

  /**
   * @brief Unlocks a row for a client.
   *
   * @param[in] prebuilt prebuilt struct.
   * @param[in] has_latches_on_recs true if the thread currently has the search latch locked in s-mode.
   *
   * @return DB_SUCCESS.
   */
  [[nodiscard]] db_err unlock_for_client(row_prebuilt_t *prebuilt, bool has_latches_on_recs) noexcept; 

  /**
   * @brief Resets the fetch cache.
   *
   * @param[in] prebuilt prebuilt struct.
   */
  inline static void cache_reset(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Checks if the fetch cache is empty.
   *
   * @param[in] prebuilt prebuilt struct.
   * 
   * @return true if the fetch cache is empty.
   */
  [[nodiscard]] static bool is_cache_empty(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Checks if the fetch cache is fetching in progress.
   *
   * @param[in] prebuilt prebuilt struct.
   * 
   * @return true if the fetch cache is fetching in progress.
   */
  [[nodiscard]] static bool is_cache_fetch_in_progress(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Checks if the fetch cache is full.
   *
   * @param[in] prebuilt prebuilt struct.
   * 
   * @return true if the fetch cache is full.
   */
  [[nodiscard]] static bool is_cache_full(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Gets a row from the fetch cache.
   *
   * @param[in] prebuilt prebuilt struct.
   * 
   * @return a pointer to the row.
   */
  [[nodiscard]] static const rec_t *cache_get_row(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Moves to the next row in the fetch cache.
   *
   * @param[in] prebuilt prebuilt struct.
   */
  static void cache_next(row_prebuilt_t *prebuilt) noexcept;

  /**
   * @brief Add a record to the fetch cache.
   * 
   * @param[in] prebuilt prebuilt struct
   * @param[in] rec record to push; must be protected by a page latch
   * @param[in] offsets Phy_rec::get_col_offsets()
   */
  inline static void cache_add_row(row_prebuilt_t *prebuilt, const rec_t *rec, const ulint *offsets) noexcept;

#ifdef UNIT_TEST
  private:
#endif /* UNIT_TEST */

  /**
   * Sets a lock on a record.
   *
   * @param[in] block buffer block of rec
   * @param[in] rec record
   * @param[in] index index
   * @param[in] offsets Phy_rec::get_col_offsets(index, rec)
   * @param[in] mode lock mode
   * @param[in] type LOCK_ORDINARY, LOCK_GAP, or LOC_REC_NOT_GAP
   * @param[in] thr query thread
   *
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err set_rec_lock(const Buf_block *block, const rec_t *rec, Index *index, const ulint *offsets, Lock_mode mode, ulint type, que_thr_t *thr) noexcept;

  /**
   * @brief Fetches the column values from a record.
   *
   * @param[in] index record index
   * @param[in] rec record in a clustered or non-clustered index; must be protected by a page latch
   * @param[in] offsets Phy_rec::get_col_offsets(index, rec)
   * @param[in] column first column in a column list, or NULL
   */
  void fetch_columns(Index *index, const rec_t *rec, const ulint *offsets, sym_node_t *column) noexcept;

  /**
   * @brief Allocates a prefetch buffer for a column when prefetch is first time done.
   *
   * @param[in] column symbol table node for a column
   */
  void prefetch_buf_alloc(sym_node_t *column) noexcept;

  /**
   * @brief Performs a select step.
   *
   * @param[in] sel_node The select node.
   * @param[in] thr The query thread.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err select(sel_node_t *sel_node, que_thr_t *thr) noexcept;

  /**
   * Retrieves the clustered index record corresponding to a record in a non-clustered
   * index. Does the necessary locking.
   * 
   * @param[in] prebuilt prebuilt struct in the handle
   * @param[in] sec_index secondary index where rec resides
   * @param[in] rec record in a non-clustered index; if this is a locking read,
   *  then rec is not allowed to be delete-marked, and that would not make sense either
   * @param[in] thr query thread
   * @param[out] out_rec clustered record or an old version of it, NULL if the old version
   *   did not exist in the read view, i.e., it was a fresh inserted version
   * @param[in] offsets offsets returned by Phy_rec::get_col_offsets(sec_index, rec); out: offsets
   *   returned by Phy_rec::get_col_offsets(clust_index, out_rec)
   * @param[in] offset_heap memory heap from which the offsets are allocated
   * @param[in] mtr mtr used to get access to the non-clustered record; the same mtr is
   *    used to access the clustered index
   * 
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err get_clust_rec_with_prebuilt(
    row_prebuilt_t *prebuilt,
    Index *sec_index,
    const rec_t *rec,
    que_thr_t *thr,
    const rec_t **out_rec,
    ulint **offsets,
    mem_heap_t **offset_heap,
    mtr_t *mtr) noexcept;

  /**
   * @brief Restores cursor position after it has been stored. We have to take into
   * account that the record cursor was positioned on may have been deleted.
   * Then we may have to move the cursor one step up or down.
   * 
   * @param[out] same_user_rec true if we were able to restore the cursor on a user
   * record with the same ordering prefix in the B-tree index
   * @param[in] latch_mode latch mode wished in restoration
   * @param[in] pcur cursor whose position has been stored
   * @param[in] moves_up true if the cursor moves up in the index
   * @param[in] mtr mtr; CAUTION: may commit mtr temporarily!
   * 
   * @return true if we may need to process the record the cursor is now positioned on
   * (i.e. we should not go to the next record yet)
   */
  [[nodiscard]] bool restore_position(bool *same_user_rec, ulint latch_mode, Btree_pcursor *pcur, bool moves_up, mtr_t *mtr) noexcept;

    /**
     * Builds a previous version of a clustered index record for a consistent read
     *
     * @param[in] read_view read view
     * @param[in] index plan node for table
     * @param[in] rec record in a clustered index
     * @param[in, out] offsets  offsets returned by Rec:get_offsets(plan->index, rec)
     * @param[in, out] offset_heap   memory heap from which the offsets are allocated
     * @param[out] old_vers_heap old version heap to use
     * @param[out] old_vers old version, or NULL if the record does not exist in the view: i.e., it was freshly inserted afterwards
     * @param [in] mtr mtr
     * 
     * @return DB_SUCCESS or error code
     */
  [[nodiscard]] db_err build_prev_vers(
    read_view_t *read_view,
    Index *index,
    const rec_t *rec,
    ulint **offsets,
    mem_heap_t **offset_heap,
    mem_heap_t **old_vers_heap,
    rec_t **old_vers,
    mtr_t *mtr) noexcept;

  /**
   * @brief Returns true if the user-defined column values in a secondary index record
   * are alphabetically the same as the corresponding columns in the clustered
   * index record.
   * NOTE: the comparison is NOT done as a binary comparison, but character
   * fields are compared with collation!
   *
   * @param[in] sec_rec    secondary index record
   * @param[in] sec_index  secondary index
   * @param[in] clust_rec  clustered index record; must be protected by a lock or
   *  a page latch against deletion in rollback or purge
   * 
   * @param[in] clust_index clustered index
   *
   * @return true if the secondary record is equal to the corresponding fields in the
   *  clustered record, when compared with collation; false if not equal or if the
   * clustered record has been marked for deletion
   */
  [[nodiscard]] bool sec_rec_is_for_clust_rec(const rec_t *sec_rec, Index *sec_index, const rec_t *clust_rec, Index *clust_index) noexcept;

  /**
   * @brief Returns true if the user-defined column in a secondary index record
   * is alphabetically the same as the corresponding BLOB column in the clustered
   * index record.
   *
   * @param[in] mtype  main type
   * @param[in] prtype precise type
   * @param[in] mbminlen minimum length of a multi-byte character
   * @param[in] mbmaxlen  maximum length of a multi-byte character
   * @param[in] clust_field  the locally stored part of the clustered index column,
   *   including the BLOB pointer; the clustered index record must be covered by a
   *   lock or a page latch to protect it against deletion (rollback or purge)
   * @param[in] clust_len length of clust_field
   * @param[in] sec_field column in secondary index
   * @param[in] sec_len length of sec_field
   *
   * @return true if the columns are equal
   */
  [[nodiscard]] bool sec_rec_is_for_blob(
    ulint mtype,
    ulint prtype,
    ulint mbminlen,
    ulint mbmaxlen,
    const byte *clust_field,
    ulint clust_len,
    const byte *sec_field,
    ulint sec_len) noexcept;

  /**
   * Tries to do a shortcut to fetch a clustered index record with a unique key,
   * using the hash index if possible (not always). We assume that the search
   * mode is PAGE_CUR_GE, it is a consistent read, there is a read view in trx,
   * btr search latch has been locked in S-mode.
   *
   * @param[out] out_rec record if found
   * @param[in] prebuilt prebuilt struct
   * @param[in,out] offsets for Rec:get_offsets(*out_rec)
   * @param[in,out] heap heap for Phy_rec::get_col_offsets()
   * @param[in] mtr started mtr
   *
   * @return Search_status::FOUND, Search_status::EXHAUSTED, Search_status::RETRY
   */
  [[nodiscard]] Search_status try_search_shortcut_for_prebuilt(
    const rec_t **out_rec,
    row_prebuilt_t *prebuilt,
    ulint **offsets,
    mem_heap_t **heap,
    mtr_t *mtr) noexcept;

public:
  /** Dictionary */
  Dict *m_dict{};

  /** Lock system. */
  Lock_sys *m_lock_sys{};
};

/** A structure for caching column values for prefetched rows */
struct sel_buf_t {
  /**
   * @brief Default constructor for sel_buf_t.
   */
  sel_buf_t() = default;

  /**
   * @brief Destructor for sel_buf_t.
   */
  ~sel_buf_t() = default;

  /**
   * @brief Clears the prefetch buffer.
   *
   * This function clears the prefetch buffer, freeing the dynamically
   * allocated memory for data stored there.
   */ 
  void clear() noexcept {
    if (m_val_buf_size > 0) {
      mem_free(m_data);
    }
  }

  /** data, or nullptr; if not nullptr, this field has allocated memory
   * which must be explicitly freed; can be != nullptr even when len is UNIV_SQL_NULL */
  byte *m_data{};

  /** data length or UNIV_SQL_NULL */
  ulint m_len{}; 

  /** size of memory buffer allocated for data: this can be more than len;
   * this is defined when data != nullptr */
  ulint m_val_buf_size{};
};

/** Query plan */
struct Plan {
  /**
   * @brief Constructor for Plan.
   *
   * @param[in] fsp The file space.
   * @param[in] btree The B-tree.
   */
  Plan(FSP *fsp, Btree *btree, Lock_sys *lock_sys, Row_sel *row_sel) noexcept
  : m_pcur{fsp, btree},
    m_clust_pcur{fsp, btree},
    m_lock_sys{lock_sys},
    m_sel{row_sel} { }

  /**
   * Pops the column values for a prefetched, cached row from the column prefetch
   * buffers and places them to the val fields in the column nodes.
   */
  inline void pop_prefetched_row() noexcept;

  /**
   * Pushes the column values for a prefetched, cached row to the column prefetch
   * buffers from the val fields in the column nodes.
   */
  inline void push_prefetched_row() noexcept;

  /**
   * Tests the conditions which determine when the index segment we are searching
   * through has been exhausted.
   *
   * @return true if row passed the tests
   */
  inline bool test_end_conds() noexcept;

  /**
   * Tests the other conditions.
   *
   * @return true if row passed the tests
   */
  inline bool test_other_conds() noexcept;

  /**
   * Opens a pcur to a table index.
   *
   * @param[in] search_latch_locked   true if the thread currently has the search latch locked in s-mode
   * @param[in] mtr mini-transaction
   */
  void open_pcur(bool search_latch_locked, mtr_t *mtr) noexcept;

  /**
   * Restores a stored pcur position to a table index.
   *
   * @param[in] mtr mtr
   *
   * @return true if the cursor should be moved to the next record after we
   * return from this function (moved to the previous, in the case of a
   * descending cursor) without processing again the current cursor
   * record
   */
  bool restore_pcur_pos(mtr_t *mtr) noexcept;

  /**
   * Resets a plan cursor to a closed state.
   */
  inline void reset_cursor() noexcept {
    m_pcur_is_open = false;
    m_cursor_at_end = false;
    m_n_rows_fetched = 0;
    m_n_rows_prefetched = 0;
  }

  /**
   * Retrieves the clustered index record corresponding to a record in a
   * non-clustered index. Does the necessary locking.
   *
   * @param[in] sel_node select_node
   * @param[in] rec  record in a non-clustered index
   * @param[in] thr  query thread
   * @param[out] out_rec clustered record or an old version of it, NULL if the
   *  old version did not exist in the read view, i.e., it was a fresh inserted version
   * @param[in] mtr mtr used to get access to the non-clustered record; the same
   *  mtr is used to access the clustered index
   * 
   * @return          DB_SUCCESS or error code
   */
  [[nodiscard]] db_err get_clust_rec(sel_node_t *sel_node, rec_t *rec, que_thr_t *thr, rec_t **out_rec, mtr_t *mtr) noexcept;

  /**
   * Tries to do a shortcut to fetch a clustered index record with a unique key,
   * using the hash index if possible (not always).
   *
   * @param[in] sel_node select node for a consistent read
   * @param[in] mtr mtr
   *
   * @return SEL_FOUND, SEL_EXHAUSTED, SEL_RETRY
   */
  [[nodiscard]] Search_status try_search_shortcut(sel_node_t *sel_node, mtr_t *mtr) noexcept;

  /**
   * Closes cursor and free the old version heap.
   */
  void close() noexcept {
    m_clust_pcur.close();

    if (likely(m_old_vers_heap != nullptr)) {
      mem_heap_free(m_old_vers_heap);
      m_old_vers_heap = nullptr;
    }
  }

  /** Table struct in the dictionary cache */
  Table *m_table{};

  /** table index used in the search */
  Index *m_index{};

  /** persistent cursor used to search the index */
  Btree_pcursor m_pcur{srv_fsp, srv_btree_sys};

  /** true if cursor traveling upwards */
  bool m_asc{};

  /** true if pcur has been positioned and we can try to fetch new rows */
  bool m_pcur_is_open{};

  /** true if the cursor is open but we know that there are no more qualifying
   * rows left to retrieve from the index tree; NOTE though, that there may still
   * be unprocessed rows in the prefetch stack; always false when pcur_is_open is false */
  bool m_cursor_at_end{};

  /** true if the pcur position has been stored and the record it is positioned
   * on has already been processed */
  bool m_stored_cursor_rec_processed{};

  /** array of expressions which are used to calculate the field values in the
   * search tuple: there is one expression for each field in the search tuple */
  que_node_t **m_tuple_exps{};

  /** search tuple */
  DTuple *m_tuple{};

  /** search mode: PAGE_CUR_G, ... */
  ib_srch_mode_t m_mode{};

  /** number of first fields in the search tuple which must be exactly matched */
  ulint m_n_exact_match{};

  /** true if we are searching an index record with a unique key */
  bool m_unique_search{};

  /** number of rows fetched using pcur after it was opened */
  ulint m_n_rows_fetched{};

  /** number of prefetched rows cached for fetch: fetching several rows in the same mtr saves CPU time */
  ulint m_n_rows_prefetched{};

  /** index of the first cached row in select buffer arrays for each column */
  ulint m_first_prefetched{};

  /** no prefetch for this table */
  bool m_no_prefetch{};

  /** symbol table nodes for the columns to retrieve from the table */
  sym_node_list_t m_columns{};

  /** conditions which determine the fetch limit of the index segment we have to look at:
   * when one of these fails, the result set has been exhausted for the cursor in this index;
   * these conditions are normalized so that in a comparison the column for this table is the
   * first argument */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, cond_list) m_end_conds{};

  /** the rest of search conditions we can test at this table in a join */
  UT_LIST_BASE_NODE_T_EXTERN(func_node_t, func_node_list) m_other_conds{};

  /** true if index is a non-clustered index and we must also fetch the clustered index record;
   * this is the case if the non-clustered record does not contain all the needed columns,
   * or if this is a single-table explicit cursor, or a searched update or delete */
  bool m_must_get_clust{};

  /** map telling how clust_ref is built from the fields of a non-clustered record */
  ulint *m_clust_map{};

  /** the reference to the clustered index entry is built here if index is a non-clustered index */
  DTuple *m_clust_ref{};

  /** if index is non-clustered, we use this pcur to search the clustered index */
  Btree_pcursor m_clust_pcur{srv_fsp, srv_btree_sys};

  /** memory heap used in building an old version of a row, or nullptr */
  mem_heap_t *m_old_vers_heap{};

  /** lock system */
  Lock_sys *m_lock_sys{};

  /** row selector */
  Row_sel *m_sel{};
};

/** Select node states */
enum sel_node_state {
  /** it is a declared cursor which is not currently open */
  SEL_NODE_CLOSED,     

  /** intention locks not yet set on tables */
  SEL_NODE_OPEN,       

  /** intention locks have been set */
  SEL_NODE_FETCH,      

  /** cursor has reached the result set end */
  SEL_NODE_NO_MORE_ROWS
};

/** Select statement node */
struct sel_node_t {
  /**
   * @brief Default constructor for sel_node_t.
   */
  sel_node_t() noexcept;

  /**
   * @brief Destructor for sel_node_t.
   */
  ~sel_node_t() = default;

  /**
   * @brief Creates a select node struct.
   *
   * This function allocates and initializes a select node struct using the provided memory heap.
   *
   * @param[in] heap The memory heap where the select node struct will be created.
   * 
   * @return A pointer to the newly created select node struct.
   */
  [[nodiscard]] static sel_node_t *create(mem_heap_t *heap) noexcept;

  /**
   * This function retrieves the plan node for the specified table index in a join.
   *
   * @param[in] i The index of the table in the join.
   * 
   * @return A pointer to the plan node for the nth table.
   */
  inline Plan *get_nth_plan(ulint i) {
    ut_ad(i < m_n_tables);
    return &m_plans[i];
  }

  /**
   * @brief Evaluates the values in a select list. If there are aggregate functions,
   * their argument value is added to the aggregate total.
   */
  void eval_select_list() noexcept;

  /**
   * @brief Resets the aggregate value totals in the select list of an aggregate type query.
   */
  void reset_aggregate_vals() noexcept;

  /**
   * @brief Assigns the values in the select list to the possible into-variables in SELECT ... INTO ...
   *
   * @param var [in] first variable in a list of variables
   */
  void assign_into_var_values(sym_node_t *var) noexcept;

  /**
   * Copies the input variable values when an explicit cursor is opened.
   *
   * @param node [in] select node
   */
  void copy_input_variable_vals() noexcept;

  /**
   * @brief Closes the plans for all tables.
   */
  void clear() noexcept;

  /**
   * @brief Resets the cursor defined by sel_node to the SEL_NODE_OPEN state.
   *
   * This function resets the cursor defined by sel_node to the SEL_NODE_OPEN state,
   * which means that it will start fetching from the start of the result set again,
   * regardless of where it was before, and it will set intention locks on the tables.
   */
  inline void reset_cursor() noexcept {
    m_state = SEL_NODE_OPEN;
  }

  /** node type: QUE_NODE_SELECT */
  que_common_t m_common{};

  /** node state */
  sel_node_state m_state{SEL_NODE_CLOSED};

  /** select list */
  que_node_t *m_select_list{};

  /** variables list or nullptr */
  sym_node_t *m_into_list{};    

  /** table list */
  sym_node_t *m_table_list{};   

  /** true if the rows should be fetched in an ascending order */
  bool m_asc{};                 

  /** true if the cursor is for update or delete, which means that a row x-lock should be placed on the cursor row */
  bool m_set_x_locks{};         

  /** LOCK_X or LOCK_S */
  Lock_mode m_row_lock_mode{};  

  /** number of tables */
  uint32_t m_n_tables{};           

  /** number of the next table to access in the join */
  uint32_t m_fetch_table{};        

  /** array of n_tables many plan nodes containing the search plan and the search data structures */
  Plan *m_plans{};            

  /** search condition */
  que_node_t *m_search_cond{};  

  /** if the query is a non-locking consistent read, its read view is placed here, otherwise nullptr */
  read_view_t *m_read_view{};   

  /** true if the select is a consistent, non-locking read */
  bool m_consistent_read{};     

  /** order by column definition, or nullptr */
  order_node_t *m_order_by{};   

  /** true if the select list consists of aggregate functions */
  bool m_is_aggregate{};        

  /** true if the aggregate row has already been fetched for the current cursor */
  bool m_aggregate_already_fetched{};

  /** this is true if the select is in a single-table explicit cursor which can get updated within the stored
   * procedure, or in a searched update or delete; NOTE that to determine of an explicit cursor if it can
   * get updated, the parser checks from a stored procedure if it contains positioned update or delete statements */
  bool m_can_get_updated{};       

  /** not nullptr if an explicit cursor */
  sym_node_t *m_explicit_cursor{};

  /** variables whose values we have to copy when an explicit cursor is opened, so that they do not
   * change between fetches */
  UT_LIST_BASE_NODE_T(sym_node_t, sym_list) m_copy_variables{};
};

/** Fetch statement node */
struct fetch_node_struct {
  /** type: QUE_NODE_FETCH */
  que_common_t m_common;   

  /** cursor definition */
  sel_node_t *m_cursor_def{};

  /** variables to set */
  sym_node_t *m_into_list{}; 

  /** User callback function or nullptr.  The first argument to the function is a sel_node_t*, containing the
   * results of the SELECT operation for one row. If the function returns nullptr, it is not interested in
   * further rows and the cursor is modified so (cursor % NOTFOUND) is true. If it returns not-nullptr,
   * continue normally. See row_fetch_print() for an example (and a useful debugging tool). */
  pars_user_func_t *m_func{};
};

/** Open or close cursor operation type */
enum open_node_op {
  /** open cursor */
  ROW_SEL_OPEN_CURSOR,

  /** close cursor */
  ROW_SEL_CLOSE_CURSOR
};

/** Open or close cursor statement node */
struct open_node_struct {
  /** type: QUE_NODE_OPEN */
  que_common_t m_common{};

  /** operation type: open or close cursor */
  open_node_op m_op_type{};

  /** cursor definition */
  sel_node_t *m_cursor_def{};   
};

/** Row printf statement node */
struct row_printf_node_struct {
  /** type: QUE_NODE_ROW_PRINTF */
  que_common_t m_common{}; 

  /** select */
  sel_node_t *m_sel_node{};
};

/**
 * @brief Gets the plan node for the nth table in a join.
 *
 * This function retrieves the plan node for the specified table index in a join.
 *
 * @param[in] node The select node.
 * @param[in] i The index of the table in the join.
 * @return A pointer to the plan node for the nth table.
 */
inline static Plan *get_nth_plan(sel_node_t *sel_node, ulint i) {
  return sel_node->get_nth_plan(i);
}

/**
 * @brief Performs an execution step of an open or close cursor statement node.
 *
 * This function executes a step in the process of opening or closing a cursor
 * statement node within a query execution graph.
 *
 * @param[in] thr The query thread.
 * @return The query thread to run next or nullptr.
 */
inline que_thr_t *open_step(que_thr_t *thr) {
  auto open_node = static_cast<open_node_t *>(thr->run_node);
  ut_ad(que_node_get_type(open_node) == QUE_NODE_OPEN);

  db_err err = DB_SUCCESS;
  auto sel_node = open_node->m_cursor_def;

  if (open_node->m_op_type == ROW_SEL_OPEN_CURSOR) {

    /*		if (sel_node->m_state == SEL_NODE_CLOSED) { */

    sel_node->reset_cursor();
    /*		} else {
    err = DB_ERROR;
    } */
  } else if (sel_node->m_state != SEL_NODE_CLOSED) {
    sel_node->m_state = SEL_NODE_CLOSED;
  } else {
    err = DB_ERROR;
  }

  if (expect(err, DB_SUCCESS) != DB_SUCCESS) {
    /* SQL error detected */
    log_fatal("SQL error ", (ulong)err);

    ut_error;
  }

  thr->run_node = que_node_get_parent(open_node);

  return thr;
}
