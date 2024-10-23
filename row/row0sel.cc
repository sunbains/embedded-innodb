/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** @file row/row0sel.c
Select

Created 12/19/1997 Heikki Tuuri
*******************************************************/

#include "row0sel.h"
#include "row0prebuilt.h"

#include "api0misc.h"
#include "api0ucode.h"
#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0lru.h"
#include "dict0store.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "mach0data.h"
#include "pars0pars.h"
#include "pars0sym.h"
#include "que0que.h"
#include "read0read.h"
#include "rem0cmp.h"
#include "row0row.h"
#include "row0upd.h"
#include "row0vers.h"
#include "trx0trx.h"
#include "trx0undo.h"

/** Maximum number of rows to prefetch. */
constexpr auto SEL_MAX_N_PREFETCH = FETCH_CACHE_SIZE;

/* Number of rows fetched, after which to start prefetching. */
constexpr ulint SEL_PREFETCH_LIMIT = 1;

/** When a select has accessed about this many pages, it returns control back
to que_run_threads: this is to allow canceling runaway queries */
constexpr ulint SEL_COST_LIMIT = 100;

/* Flags for search shortcut */
constexpr ulint SEL_FOUND = 0;
constexpr ulint SEL_EXHAUSTED = 1;
constexpr ulint SEL_RETRY = 2;

/**
 * @brief Returns true if the user-defined column in a secondary index record
 * is alphabetically the same as the corresponding BLOB column in the clustered
 * index record.
 *
 * @param mtype     [in] main type
 * @param prtype    [in] precise type
 * @param mbminlen  [in] minimum length of a multi-byte character
 * @param mbmaxlen  [in] maximum length of a multi-byte character
 * @param clust_field   [in] the locally stored part of the clustered index column, including the BLOB pointer; the clustered index record must be covered by a lock or a page latch to protect it against deletion (rollback or purge)
 * @param clust_len [in] length of clust_field
 * @param sec_field [in] column in secondary index
 * @param sec_len   [in] length of sec_field
 *
 * @return true if the columns are equal
 */
static bool row_sel_sec_rec_is_for_blob(
  ulint mtype,
  ulint prtype,
  ulint mbminlen,
  ulint mbmaxlen,
  const byte *clust_field,
  ulint clust_len,
  const byte *sec_field,
  ulint sec_len
) {
  ulint len;
  byte buf[DICT_MAX_INDEX_COL_LEN];

  Blob blob(srv_fsp, srv_btree_sys);

  len = blob.copy_externally_stored_field_prefix(buf, sizeof buf, clust_field, clust_len);

  if (unlikely(len == 0)) {
    /* The BLOB was being deleted as the server crashed.
    There should not be any secondary index records
    referring to this clustered index record, because
    btr_free_externally_stored_field() is called after all
    secondary index entries of the row have been purged. */
    return false;
  }

  len = dtype_get_at_most_n_mbchars(prtype, mbminlen, mbmaxlen, sec_len, len, (const char *)buf);

  /* FIXME: Pass a NULL compare context, the compare context will be
  required once we support comparison operations outside of rem0cmp.c. */
  return !cmp_data_data(nullptr, mtype, prtype, buf, len, sec_field, sec_len);
}

/**
 * @brief Returns true if the user-defined column values in a secondary index record
 * are alphabetically the same as the corresponding columns in the clustered
 * index record.
 * NOTE: the comparison is NOT done as a binary comparison, but character
 * fields are compared with collation!
 *
 * @param sec_rec    [in] secondary index record
 * @param sec_index  [in] secondary index
 * @param clust_rec  [in] clustered index record; must be protected by a lock or
 *  a page latch against deletion in rollback or purge
 * @param clust_index [in] clustered index
 *
 * @return true if the secondary record is equal to the corresponding fields in the
 *  clustered record, when compared with collation; false if not equal or if the
 * clustered record has been marked for deletion
 */
static bool row_sel_sec_rec_is_for_clust_rec(
  const rec_t *sec_rec,
  Index *sec_index,
  const rec_t *clust_rec,
  Index *clust_index
) {
  const byte *sec_field;
  ulint sec_len;
  const byte *clust_field;
  ulint n;
  ulint i;
  mem_heap_t *heap = nullptr;
  ulint clust_offsets_[REC_OFFS_NORMAL_SIZE];
  ulint sec_offsets_[REC_OFFS_SMALL_SIZE];
  ulint *clust_offs = clust_offsets_;
  ulint *sec_offs = sec_offsets_;
  bool is_equal = true;

  rec_offs_init(clust_offsets_);
  rec_offs_init(sec_offsets_);

  if (rec_get_deleted_flag(clust_rec)) {

    /* The clustered index record is delete-marked;
    it is not visible in the read view.  Besides,
    if there are any externally stored columns,
    some of them may have already been purged. */
    return false;
  }

  {
    Phy_rec record{clust_index, clust_rec};

    clust_offs = record.get_col_offsets(clust_offs, ULINT_UNDEFINED, &heap, Current_location());
  }

  {
    Phy_rec record{sec_index, sec_rec};

    sec_offs = record.get_col_offsets(sec_offs, ULINT_UNDEFINED, &heap, Current_location());
  }


  n = sec_index->get_n_ordering_defined_by_user();

  for (i = 0; i < n; i++) {
    const auto ifield = sec_index->get_nth_field(i);
    const auto col = ifield->get_col();
    auto clust_pos = clust_index->get_clustered_field_pos(col);

    ulint clust_len;

    clust_field = rec_get_nth_field(clust_rec, clust_offs, clust_pos, &clust_len);
    sec_field = rec_get_nth_field(sec_rec, sec_offs, i, &sec_len);

    auto len = clust_len;

    if (ifield->m_prefix_len > 0 && len != UNIV_SQL_NULL) {

      if (rec_offs_nth_extern(clust_offs, clust_pos)) {
        len -= BTR_EXTERN_FIELD_REF_SIZE;
      }

      len = dtype_get_at_most_n_mbchars(col->prtype, col->mbminlen, col->mbmaxlen, ifield->m_prefix_len, len, (char *)clust_field);

      if (rec_offs_nth_extern(clust_offs, clust_pos) && len < sec_len) {
        if (!row_sel_sec_rec_is_for_blob(
              col->mtype, col->prtype, col->mbminlen, col->mbmaxlen, clust_field, clust_len, sec_field, sec_len
            )) {
          goto inequal;
        }

        continue;
      }
    }

    if (cmp_data_data(clust_index->m_cmp_ctx, col->mtype, col->prtype, clust_field, len, sec_field, sec_len) != 0) {
    inequal:
      is_equal = false;
      goto func_exit;
    }
  }

func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return is_equal;
}

sel_node_t *sel_node_create(mem_heap_t *heap) {
  auto node = reinterpret_cast<sel_node_t *>(mem_heap_alloc(heap, sizeof(sel_node_t)));

  node->common.type = QUE_NODE_SELECT;
  node->state = SEL_NODE_OPEN;

  node->m_plans = nullptr;

  return node;
}

void sel_node_free_private(sel_node_t *node) {
  if (node->m_plans != nullptr) {
    for (ulint i = 0; i < node->n_tables; i++) {
      auto plan = sel_node_get_nth_plan(node, i);

      plan->pcur.close();
      plan->clust_pcur.close();

      if (plan->old_vers_heap) {
        mem_heap_free(plan->old_vers_heap);
      }
    }
  }
}

/**
 * @brief Evaluates the values in a select list. If there are aggregate functions,
 * their argument value is added to the aggregate total.
 *
 * @param node [in] select node
 */
inline void sel_eval_select_list(sel_node_t *node) {
  que_node_t *exp;

  exp = node->select_list;

  while (exp) {
    eval_exp(exp);

    exp = que_node_get_next(exp);
  }
}

/**
 * @brief Assigns the values in the select list to the possible into-variables in SELECT ... INTO ...
 *
 * @param var [in] first variable in a list of variables
 * @param node [in] select node
 */
inline void sel_assign_into_var_values(sym_node_t *var, sel_node_t *node) {
  que_node_t *exp;

  if (var == nullptr) {

    return;
  }

  exp = node->select_list;

  while (var) {
    ut_ad(exp);

    eval_node_copy_val(var->alias, exp);

    exp = que_node_get_next(exp);
    var = static_cast<sym_node_t *>(que_node_get_next(var));
  }
}

/**
 * @brief Resets the aggregate value totals in the select list of an aggregate type query.
 *
 * @param node [in] select node
 */
inline void sel_reset_aggregate_vals(sel_node_t *node) {
  ut_ad(node->is_aggregate);

  auto func_node = static_cast<func_node_t *>(node->select_list);

  while (func_node) {
    eval_node_set_int_val(func_node, 0);

    func_node = static_cast<func_node_t *>(que_node_get_next(func_node));
  }

  node->aggregate_already_fetched = false;
}

/**
 * Copies the input variable values when an explicit cursor is opened.
 *
 * @param node [in] select node
 */
inline void row_sel_copy_input_variable_vals(sel_node_t *node) {
  auto var = UT_LIST_GET_FIRST(node->copy_variables);

  while (var != nullptr) {
    eval_node_copy_val(var, var->alias);

    var->indirection = nullptr;

    var = UT_LIST_GET_NEXT(col_var_list, var);
  }
}

/**
 * @brief Fetches the column values from a record.
 *
 * @param index [in] record index
 * @param rec [in] record in a clustered or non-clustered index; must be protected by a page latch
 * @param offsets [in] Phy_rec::get_col_offsets(index, rec)
 * @param column [in] first column in a column list, or NULL
 */
static void row_sel_fetch_columns(
  Index *index,
  const rec_t *rec,
  const ulint *offsets,
  sym_node_t *column
) {
  dfield_t *val;
  ulint index_type;
  ulint field_no;
  const byte *data;
  ulint len;

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (index->is_clustered()) {
    index_type = SYM_CLUST_FIELD_NO;
  } else {
    index_type = SYM_SEC_FIELD_NO;
  }

  Blob blob(srv_fsp, srv_btree_sys);

  while (column) {
    mem_heap_t *heap = nullptr;
    bool needs_copy;

    field_no = column->field_nos[index_type];

    if (field_no != ULINT_UNDEFINED) {

      if (unlikely(rec_offs_nth_extern(offsets, field_no))) {

        /* Copy an externally stored field to the
        temporary heap */

        heap = mem_heap_create(1);

        data = blob.copy_externally_stored_field(rec, offsets, field_no, &len, heap);

        ut_a(len != UNIV_SQL_NULL);

        needs_copy = true;
      } else {
        data = rec_get_nth_field(rec, offsets, field_no, &len);

        needs_copy = column->copy_val;
      }

      if (needs_copy) {
        eval_node_copy_and_alloc_val(column, data, len);
      } else {
        val = que_node_get_val(column);
        dfield_set_data(val, data, len);
      }

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }
    }

    column = UT_LIST_GET_NEXT(col_var_list, column);
  }
}

/**
 * Allocates a prefetch buffer for a column when prefetch is first time done.
 *
 * @param column  in: symbol table node for a column
 */
static void sel_col_prefetch_buf_alloc(sym_node_t *column) {
  sel_buf_t *sel_buf;
  ulint i;

  ut_ad(que_node_get_type(column) == QUE_NODE_SYMBOL);

  column->prefetch_buf = static_cast<sel_buf_t *>(mem_alloc(SEL_MAX_N_PREFETCH * sizeof(sel_buf_t)));
  for (i = 0; i < SEL_MAX_N_PREFETCH; i++) {
    sel_buf = column->prefetch_buf + i;

    sel_buf->data = nullptr;

    sel_buf->val_buf_size = 0;
  }
}

void sel_col_prefetch_buf_free(sel_buf_t *prefetch_buf) {
  sel_buf_t *sel_buf;

  for (ulint i = 0; i < SEL_MAX_N_PREFETCH; i++) {
    sel_buf = prefetch_buf + i;

    if (sel_buf->val_buf_size > 0) {

      mem_free(sel_buf->data);
    }
  }
}

/**
 * Pops the column values for a prefetched, cached row from the column prefetch
 * buffers and places them to the val fields in the column nodes.
 *
 * @param[in  ] plan  in: plan node for a table
 */
static void sel_pop_prefetched_row(Plan *plan) noexcept{
  ut_ad(plan->n_rows_prefetched > 0);

  for (auto column : plan->columns) {
    auto val = que_node_get_val(column);

    if (!column->copy_val) {
      /* We did not really push any value for the
      column */

      ut_ad(!column->prefetch_buf);
      ut_ad(que_node_get_val_buf_size(column) == 0);
      ut_d(dfield_set_null(val));

      continue;
    }

    ut_ad(column->prefetch_buf);
    ut_ad(!dfield_is_ext(val));

    auto sel_buf = column->prefetch_buf + plan->first_prefetched;

    auto len = sel_buf->len;
    auto data = sel_buf->data;
    auto val_buf_size = sel_buf->val_buf_size;

    /* We must keep track of the allocated memory for
    column values to be able to free it later: therefore
    we swap the values for sel_buf and val */

    sel_buf->len = dfield_get_len(val);
    sel_buf->data = static_cast<byte *>(dfield_get_data(val));
    sel_buf->val_buf_size = que_node_get_val_buf_size(column);

    dfield_set_data(val, data, len);
    que_node_set_val_buf_size(column, val_buf_size);
  }

  --plan->n_rows_prefetched;
  ++plan->first_prefetched;
}

/**
 * Pushes the column values for a prefetched, cached row to the column prefetch
 * buffers from the val fields in the column nodes.
 *
 * @param[in] plan  in: plan node for a table
 */
inline void sel_push_prefetched_row(Plan *plan) {
  sel_buf_t *sel_buf;
  dfield_t *val;
  byte *data;
  ulint len;
  ulint pos;
  ulint val_buf_size;

  if (plan->n_rows_prefetched == 0) {
    pos = 0;
    plan->first_prefetched = 0;
  } else {
    pos = plan->n_rows_prefetched;

    /* We have the convention that pushing new rows starts only
    after the prefetch stack has been emptied: */

    ut_ad(plan->first_prefetched == 0);
  }

  plan->n_rows_prefetched++;

  ut_ad(pos < SEL_MAX_N_PREFETCH);

  for (auto column : plan->columns) {
    if (!column->copy_val) {
      /* There is no sense to push pointers to database
      page fields when we do not keep latch on the page! */

      continue;
    }

    if (!column->prefetch_buf) {
      /* Allocate a new prefetch buffer */

      sel_col_prefetch_buf_alloc(column);
    }

    sel_buf = column->prefetch_buf + pos;

    val = que_node_get_val(column);

    data = (byte *)dfield_get_data(val);
    len = dfield_get_len(val);
    val_buf_size = que_node_get_val_buf_size(column);

    /* We must keep track of the allocated memory for
    column values to be able to free it later: therefore
    we swap the values for sel_buf and val */

    dfield_set_data(val, sel_buf->data, sel_buf->len);
    que_node_set_val_buf_size(column, sel_buf->val_buf_size);

    sel_buf->data = data;
    sel_buf->len = len;
    sel_buf->val_buf_size = val_buf_size;
  }
}

/**
 * Tests the conditions which determine when the index segment we are searching
 * through has been exhausted.
 *
 * @param[in] plan  in: plan for the table; the column values must already have been retrieved and
 *   the right sides of comparisons evaluated
 *
 * @return      true if row passed the tests
 */
inline bool row_sel_test_end_conds(Plan *plan) {
  /* All conditions in end_conds are comparisons of a column to an expression */

  for (auto cond : plan->end_conds) {
    /* Evaluate the left side of the comparison, i.e., get the
    column value if there is an indirection */

    eval_sym(static_cast<sym_node_t *>(cond->args));

    /* Do the comparison */

    if (!eval_cmp(cond)) {

      return false;
    }
  }

  return true;
}

/**
 * Tests the other conditions.
 *
 * @param plan  in: plan for the table; the column values must already have been retrieved
 *
 * @return      true if row passed the tests
 */
inline bool row_sel_test_other_conds(Plan *plan) noexcept{
  for (auto cond : plan->other_conds) {

    eval_exp(cond);

    if (!eval_node_get_bool_val(cond)) {
      return false;
    }
  }

  return true;
}

/**
 * Builds a previous version of a clustered index record for a consistent read
 *
 * @param[in] read_view     read view
 * @param[in] index         plan node for table
 * @param[in] rec           record in a clustered index
 * @param[in, out] offsets  offsets returned by Rec:get_offsets(plan->index, rec)
 * @param[in, out] offset_heap   memory heap from which the offsets are allocated
 * @param[out] old_vers_heap old version heap to use
 * @param[out] old_vers      old version, or NULL if the record does not exist in the view:
 *                          i.e., it was freshly inserted afterwards
 * @param mtr           mtr
 * @return              DB_SUCCESS or error code
 */
static db_err row_sel_build_prev_vers(
  read_view_t *read_view,
  Index *index,
  const rec_t *rec,
  ulint **offsets,
  mem_heap_t **offset_heap,
  mem_heap_t **old_vers_heap,
  rec_t **old_vers,
  mtr_t *mtr
) {
  if (*old_vers_heap != nullptr) {
    mem_heap_empty(*old_vers_heap);
  } else {
    *old_vers_heap = mem_heap_create(512);
  }

  return row_vers_build_for_consistent_read(rec, mtr, index, offsets, read_view, offset_heap, *old_vers_heap, old_vers);
}

/**
 * Retrieves the clustered index record corresponding to a record in a
 * non-clustered index. Does the necessary locking.
 *
 * @param[in] node      select_node
 * @param[in] plan      plan node for table
 * @param[in] rec       record in a non-clustered index
 * @param[in] thr       query thread
 * @param[out] out_rec   clustered record or an old version of it, NULL if the
 *  old version did not exist in the read view, i.e., it was a fresh inserted version
 * @param[in] mtr       mtr used to get access to the non-clustered record; the same
 *  mtr is used to access the clustered index
 * 
 * @return          DB_SUCCESS or error code
 */
static db_err row_sel_get_clust_rec(
  sel_node_t *node,
  Plan *plan,
  rec_t *rec,
  que_thr_t *thr,
  rec_t **out_rec,
  mtr_t *mtr) {

  db_err err;
  rec_t *old_vers;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  *out_rec = nullptr;

{
    Phy_rec record{plan->pcur.get_btr_cur()->m_index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  row_build_row_ref_fast(plan->clust_ref, plan->clust_map, rec, offsets);

  auto index = plan->table->get_first_index();

  plan->clust_pcur.open_with_no_init(index, plan->clust_ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, 0, mtr, Current_location());

  auto clust_rec = plan->clust_pcur.get_rec();

  /* Note: only if the search ends up on a non-infimum record is the
  low_match value the real match to the search tuple */

  if (!page_rec_is_user_rec(clust_rec) || plan->clust_pcur.get_low_match() < index->get_n_unique()) {

    ut_a(rec_get_deleted_flag(rec));
    ut_a(node->read_view);

    /* In a rare case it is possible that no clust rec is found
    for a delete-marked secondary index record: if in row0umod.c
    in row_undo_mod_remove_clust_low() we have already removed
    the clust rec, while purge is still cleaning and removing
    secondary index records associated with earlier versions of
    the clustered index record. In that case we know that the
    clustered index record did not exist in the read view of
    trx. */

    goto func_exit;
  }

  {
    Phy_rec record{index, clust_rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (!node->read_view) {
    /* Try to place a lock on the index record */

    /* If this session is using READ COMMITTED isolation level
    we lock only the record, i.e., next-key locking is
    not used. */
    Trx *trx;
    ulint lock_type;

    trx = thr_get_trx(thr);

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED) {
      lock_type = LOCK_REC_NOT_GAP;
    } else {
      lock_type = LOCK_ORDINARY;
    }

    err = srv_lock_sys->clust_rec_read_check_and_lock(
      0, plan->clust_pcur.get_block(), clust_rec, index, offsets, node->row_lock_mode, lock_type, thr
    );

    if (err != DB_SUCCESS) {

      goto err_exit;
    }
  } else {
    /* This is a non-locking consistent read: if necessary, fetch
    a previous version of the record */

    old_vers = nullptr;

    if (!srv_lock_sys->clust_rec_cons_read_sees(clust_rec, index, offsets, node->read_view)) {

      err = row_sel_build_prev_vers(node->read_view, index, clust_rec, &offsets, &heap, &plan->old_vers_heap, &old_vers, mtr);

      if (err != DB_SUCCESS) {

        goto err_exit;
      }

      clust_rec = old_vers;

      if (clust_rec == nullptr) {
        goto func_exit;
      }
    }

    /* If we had to go to an earlier version of row or the
    secondary index record is delete marked, then it may be that
    the secondary index record corresponding to clust_rec
    (or old_vers) is not rec; in that case we must ignore
    such row because in our snapshot rec would not have existed.
    Remember that from rec we cannot see directly which transaction
    id corresponds to it: we have to go to the clustered index
    record. A query where we want to fetch all rows where
    the secondary index value is in some interval would return
    a wrong result if we would not drop rows which we come to
    visit through secondary index records that would not really
    exist in our snapshot. */

    if ((old_vers || rec_get_deleted_flag(rec)) && !row_sel_sec_rec_is_for_clust_rec(rec, plan->index, clust_rec, index)) {
      goto func_exit;
    }
  }

  /* Fetch the columns needed in test conditions.  The clustered
  index record is protected by a page latch that was acquired
  when plan->clust_pcur was positioned.  The latch will not be
  released until mtr_commit(mtr). */

  row_sel_fetch_columns(index, clust_rec, offsets, UT_LIST_GET_FIRST(plan->columns));
  *out_rec = clust_rec;
func_exit:
  err = DB_SUCCESS;
err_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

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
inline db_err sel_set_rec_lock(
  const Buf_block *block,
  const rec_t *rec,
  Index *index,
  const ulint *offsets,
  Lock_mode mode,
  ulint type,
  que_thr_t *thr
) {
  db_err err;

  auto trx = thr_get_trx(thr);

  if (trx->m_trx_locks.size() > 10000) {
    if (srv_buf_pool->m_LRU->buf_pool_running_out()) {

      return DB_LOCK_TABLE_FULL;
    }
  }

  if (index->is_clustered()) {
    err = srv_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, mode, type, thr);
  } else {
    err = srv_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, mode, type, thr);
  }

  return err;
}

/**
 * Opens a pcur to a table index.
 *
 * @param[in] plan                  table plan
 * @param[in] search_latch_locked   true if the thread currently has the search latch locked in s-mode
 * @param[in] mtr                   mtr
 */
static void row_sel_open_pcur(Plan *plan, bool search_latch_locked, mtr_t *mtr) noexcept{
  que_node_t *exp;
  ulint n_fields;
  ulint has_search_latch = 0; /* RW_S_LATCH or 0 */
  ulint i;

  if (search_latch_locked) {
    has_search_latch = RW_S_LATCH;
  }

  auto index = plan->index;

  /* Calculate the value of the search tuple: the exact match columns
  get their expressions evaluated when we evaluate the right sides of
  end_conds */

  auto cond = UT_LIST_GET_FIRST(plan->end_conds);

  while (cond) {
    eval_exp(que_node_get_next(cond->args));

    cond = UT_LIST_GET_NEXT(cond_list, cond);
  }

  if (plan->tuple) {
    n_fields = dtuple_get_n_fields(plan->tuple);

    if (plan->n_exact_match < n_fields) {
      /* There is a non-exact match field which must be
      evaluated separately */

      eval_exp(plan->tuple_exps[n_fields - 1]);
    }

    for (i = 0; i < n_fields; i++) {
      exp = plan->tuple_exps[i];

      dfield_copy_data(dtuple_get_nth_field(plan->tuple, i), que_node_get_val(exp));
    }

    /* Open pcur to the index */

    plan->pcur.open_with_no_init(index, plan->tuple, plan->mode, BTR_SEARCH_LEAF, has_search_latch, mtr, Current_location());

  } else {
    /* Open the cursor to the start or the end of the index
    (false: no init) */

    plan->pcur.open_at_index_side(plan->asc, index, BTR_SEARCH_LEAF, false, 0, mtr);
  }

  ut_ad(plan->n_rows_prefetched == 0);
  ut_ad(plan->n_rows_fetched == 0);
  ut_ad(plan->cursor_at_end == false);

  plan->pcur_is_open = true;
}

/**
 * Restores a stored pcur position to a table index.
 *
 * @param[in] plan table plan
 * @param[in] mtr mtr
 *
 * @return true if the cursor should be moved to the next record after we
 * return from this function (moved to the previous, in the case of a
 * descending cursor) without processing again the current cursor
 * record
 */
static bool row_sel_restore_pcur_pos(Plan *plan, mtr_t *mtr) noexcept {
  ut_ad(!plan->cursor_at_end);

  auto relative_position = plan->pcur.get_rel_pos();

  auto equal_position = plan->pcur.restore_position(BTR_SEARCH_LEAF, mtr, Current_location());

  /* If the cursor is traveling upwards, and relative_position is

  (1) Btree_cursor_pos::BEFORE: this is not allowed, as we did not have a lock
  yet on the successor of the page infimum;
  (2) Btree_cursor_pos::AFTER: btr_pcur_t::restore_position placed the cursor on the
  first record GREATER than the predecessor of a page supremum; we have
  not yet processed the cursor record: no need to move the cursor to the
  next record;
  (3) Btree_cursor_pos::ON: btr_pcur_t::restore_position placed the cursor on the
  last record LESS or EQUAL to the old stored user record; (a) if
  equal_position is false, this means that the cursor is now on a record
  less than the old user record, and we must move to the next record;
  (b) if equal_position is true, then if
  plan->stored_cursor_rec_processed is true, we must move to the next
  record, else there is no need to move the cursor. */

  if (plan->asc) {
    if (relative_position == Btree_cursor_pos::ON) {

      if (equal_position) {

        return plan->stored_cursor_rec_processed;
      }

      return true;
    }

    ut_ad(relative_position == Btree_cursor_pos::AFTER || relative_position == Btree_cursor_pos::AFTER_LAST_IN_TREE);

    return false;
  }

  /* If the cursor is traveling downwards, and relative_position is

  (1) Btree_cursor_pos::BEFORE: btr_pcur_t::restore_position placed the cursor on
  the last record LESS than the successor of a page infimum; we have not
  processed the cursor record: no need to move the cursor;
  (2) Btree_cursor_pos::AFTER: btr_pcur_t::restore_position placed the cursor on the
  first record GREATER than the predecessor of a page supremum; we have
  processed the cursor record: we should move the cursor to the previous
  record;
  (3) Btree_cursor_pos::ON: btr_pcur_t::restore_position placed the cursor on the
  last record LESS or EQUAL to the old stored user record; (a) if
  equal_position is false, this means that the cursor is now on a record
  less than the old user record, and we need not move to the previous
  record; (b) if equal_position is true, then if
  plan->stored_cursor_rec_processed is true, we must move to the previous
  record, else there is no need to move the cursor. */

  if (relative_position == Btree_cursor_pos::BEFORE || relative_position == Btree_cursor_pos::BEFORE_FIRST_IN_TREE) {

    return false;
  }

  if (relative_position == Btree_cursor_pos::ON) {

    if (equal_position) {

      return plan->stored_cursor_rec_processed;
    }

    return false;
  }

  ut_ad(relative_position == Btree_cursor_pos::AFTER || relative_position == Btree_cursor_pos::AFTER_LAST_IN_TREE);

  return true;
}

/**
 * Resets a plan cursor to a closed state.
 *
 * @param[in] plan plan
 */
inline void plan_reset_cursor(Plan *plan) noexcept {
  plan->pcur_is_open = false;
  plan->cursor_at_end = false;
  plan->n_rows_fetched = 0;
  plan->n_rows_prefetched = 0;
}

/**
 * Tries to do a shortcut to fetch a clustered index record with a unique key,
 * using the hash index if possible (not always).
 *
 * @param[in] node select node for a consistent read
 * @param[in] plan plan for a unique search in clustered index
 * @param[in] mtr mtr
 *
 * @return SEL_FOUND, SEL_EXHAUSTED, SEL_RETRY
 */
static ulint row_sel_try_search_shortcut(sel_node_t *node, Plan *plan, mtr_t *mtr) noexcept {
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  ulint ret;
  rec_offs_init(offsets_);

  auto index = plan->index;

  ut_ad(node->read_view);
  ut_ad(plan->unique_search);
  ut_ad(!plan->must_get_clust);

  row_sel_open_pcur(plan, true, mtr);

  auto rec = plan->pcur.get_rec();

  if (!page_rec_is_user_rec(rec)) {

    return SEL_RETRY;
  }

  ut_ad(plan->mode == PAGE_CUR_GE);

  /* As the cursor is now placed on a user record after a search with
  the mode PAGE_CUR_GE, the up_match field in the cursor tells how many
  fields in the user record matched to the search tuple */

  if (plan->pcur.get_up_match() < plan->n_exact_match) {

    return SEL_EXHAUSTED;
  }

  /* This is a non-locking consistent read: if necessary, fetch
  a previous version of the record */

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (index->is_clustered()) {
    if (!srv_lock_sys->clust_rec_cons_read_sees(rec, index, offsets, node->read_view)) {
      ret = SEL_RETRY;
      goto func_exit;
    }
  } else if (!srv_lock_sys->sec_rec_cons_read_sees(rec, node->read_view)) {

    ret = SEL_RETRY;
    goto func_exit;
  }

  /* Test the deleted flag. */

  if (rec_get_deleted_flag(rec)) {

    ret = SEL_EXHAUSTED;
    goto func_exit;
  }

  /* Fetch the columns needed in test conditions.  The index
  record is protected by a page latch that was acquired when
  plan->pcur was positioned.  The latch will not be released
  until mtr_commit(mtr). */

  row_sel_fetch_columns(index, rec, offsets, UT_LIST_GET_FIRST(plan->columns));

  /* Test the rest of search conditions */

  if (!row_sel_test_other_conds(plan)) {

    ret = SEL_EXHAUSTED;
    goto func_exit;
  }

  ut_ad(plan->pcur.m_latch_mode == BTR_SEARCH_LEAF);

  plan->n_rows_fetched++;
  ret = SEL_FOUND;
func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return ret;
}

/**
 * Performs a select step.
 *
 * @param[in] node The select node.
 * @param[in] thr The query thread.
 *
 * @return DB_SUCCESS or error code.
 */
static db_err row_sel(sel_node_t *node, que_thr_t *thr) noexcept {
  Index *index;
  Plan *plan;
  mtr_t mtr;
  bool moved;
  rec_t *rec;
  rec_t *old_vers;
  rec_t *clust_rec;
  bool search_latch_locked;
  bool consistent_read;

  /* The following flag becomes true when we are doing a
  consistent read from a non-clustered index and we must look
  at the clustered index to find out the previous delete mark
  state of the non-clustered record: */

  bool cons_read_requires_clust_rec = false;
  ulint cost_counter = 0;
  bool cursor_just_opened;
  bool must_go_to_next;
  bool mtr_has_extra_clust_latch = false;
  /* true if the search was made using
  a non-clustered index, and we had to
  access the clustered record: now &mtr
  contains a clustered index latch, and
  &mtr must be committed before we move
  to the next non-clustered record */
  ulint found_flag;
  db_err err;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(thr->run_node == node);

  search_latch_locked = false;

  if (node->read_view) {
    /* In consistent reads, we try to do with the hash index and
    not to use the buffer page get. This is to reduce memory bus
    load resulting from semaphore operations. The search latch
    will be s-locked when we access an index with a unique search
    condition, but not locked when we access an index with a
    less selective search condition. */

    consistent_read = true;
  } else {
    consistent_read = false;
  }

table_loop:
  /* TABLE LOOP
  ----------
  This is the outer major loop in calculating a join. We come here when
  node->fetch_table changes, and after adding a row to aggregate totals
  and, of course, when this function is called. */

  ut_ad(mtr_has_extra_clust_latch == false);

  plan = sel_node_get_nth_plan(node, node->fetch_table);
  index = plan->index;

  if (plan->n_rows_prefetched > 0) {
    sel_pop_prefetched_row(plan);

    goto next_table_no_mtr;
  }

  if (plan->cursor_at_end) {
    /* The cursor has already reached the result set end: no more
    rows to process for this table cursor, as also the prefetch
    stack was empty */

    ut_ad(plan->pcur_is_open);

    goto table_exhausted_no_mtr;
  }

  /* Open a cursor to index, or restore an open cursor position */

  mtr.start();

  if (consistent_read && plan->unique_search && !plan->pcur_is_open && !plan->must_get_clust && !plan->table->m_big_rows) {

    found_flag = row_sel_try_search_shortcut(node, plan, &mtr);

    if (found_flag == SEL_FOUND) {

      goto next_table;

    } else if (found_flag == SEL_EXHAUSTED) {

      goto table_exhausted;
    }

    ut_ad(found_flag == SEL_RETRY);

    plan_reset_cursor(plan);

    mtr.commit();

    mtr.start();
  }

  if (!plan->pcur_is_open) {
    /* Evaluate the expressions to build the search tuple and
    open the cursor */

    row_sel_open_pcur(plan, search_latch_locked, &mtr);

    cursor_just_opened = true;

    /* A new search was made: increment the cost counter */
    cost_counter++;
  } else {
    /* Restore pcur position to the index */

    must_go_to_next = row_sel_restore_pcur_pos(plan, &mtr);

    cursor_just_opened = false;

    if (must_go_to_next) {
      /* We have already processed the cursor record: move
      to the next */

      goto next_rec;
    }
  }

rec_loop:
  /* RECORD LOOP
  -----------
  In this loop we use pcur and try to fetch a qualifying row, and
  also fill the prefetch buffer for this table if n_rows_fetched has
  exceeded a threshold. While we are inside this loop, the following
  holds:
  (1) &mtr is started,
  (2) pcur is positioned and open.

  NOTE that if cursor_just_opened is true here, it means that we came
  to this point right after row_sel_open_pcur. */

  ut_ad(mtr_has_extra_clust_latch == false);

  rec = plan->pcur.get_rec();

  /* PHASE 1: Set a lock if specified */

  if (!node->asc && cursor_just_opened && !page_rec_is_supremum(rec)) {

    /* When we open a cursor for a descending search, we must set
    a next-key lock on the successor record: otherwise it would
    be possible to insert new records next to the cursor position,
    and it might be that these new records should appear in the
    search result set, resulting in the phantom problem. */

    if (!consistent_read) {

      /* If this session is using READ COMMITTED isolation
      level, we lock only the record, i.e., next-key
      locking is not used. */

      rec_t *next_rec = page_rec_get_next(rec);
      ulint lock_type;
      Trx *trx;

      trx = thr_get_trx(thr);

      {
        Phy_rec record{index, next_rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED) {

        if (page_rec_is_supremum(next_rec)) {

          goto skip_lock;
        }

        lock_type = LOCK_REC_NOT_GAP;
      } else {
        lock_type = LOCK_ORDINARY;
      }

      err = sel_set_rec_lock(plan->pcur.get_block(), next_rec, index, offsets, node->row_lock_mode, lock_type, thr);

      if (err != DB_SUCCESS) {
        /* Note that in this case we will store in pcur
        the PREDECESSOR of the record we are waiting
        the lock for */

        goto lock_wait_or_error;
      }
    }
  }

skip_lock:
  if (page_rec_is_infimum(rec)) {

    /* The infimum record on a page cannot be in the result set,
    and neither can a record lock be placed on it: we skip such
    a record. We also increment the cost counter as we may have
    processed yet another page of index. */

    cost_counter++;

    goto next_rec;
  }

  if (!consistent_read) {
    /* Try to place a lock on the index record */

    /* If this session is using READ COMMITTED isolation level,
    we lock only the record, i.e., next-key locking is
    not used. */

    ulint lock_type;
    Trx *trx;

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    trx = thr_get_trx(thr);

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED) {

      if (page_rec_is_supremum(rec)) {

        goto next_rec;
      }

      lock_type = LOCK_REC_NOT_GAP;
    } else {
      lock_type = LOCK_ORDINARY;
    }

    err = sel_set_rec_lock(plan->pcur.get_block(), rec, index, offsets, node->row_lock_mode, lock_type, thr);

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }
  }

  if (page_rec_is_supremum(rec)) {

    /* A page supremum record cannot be in the result set: skip
    it now when we have placed a possible lock on it */

    goto next_rec;
  }

  ut_ad(page_rec_is_user_rec(rec));

  if (cost_counter > SEL_COST_LIMIT) {

    /* Now that we have placed the necessary locks, we can stop
    for a while and store the cursor position; NOTE that if we
    would store the cursor position BEFORE placing a record lock,
    it might happen that the cursor would jump over some records
    that another transaction could meanwhile insert adjacent to
    the cursor: this would result in the phantom problem. */

    goto stop_for_a_while;
  }

  /* PHASE 2: Check a mixed index mix id if needed */

  if (plan->unique_search && cursor_just_opened) {

    ut_ad(plan->mode == PAGE_CUR_GE);

    /* As the cursor is now placed on a user record after a search
    with the mode PAGE_CUR_GE, the up_match field in the cursor
    tells how many fields in the user record matched to the search
    tuple */

    if (plan->pcur.get_up_match() < plan->n_exact_match) {
      goto table_exhausted;
    }

    /* Ok, no need to test end_conds or mix id */
  }

  /* We are ready to look at a possible new index entry in the result
  set: the cursor is now placed on a user record */

  /* PHASE 3: Get previous version in a consistent read */

  cons_read_requires_clust_rec = false;
  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (consistent_read) {
    /* This is a non-locking consistent read: if necessary, fetch
    a previous version of the record */

    if (index->is_clustered()) {

      if (!srv_lock_sys->clust_rec_cons_read_sees(rec, index, offsets, node->read_view)) {

        err = row_sel_build_prev_vers(node->read_view, index, rec, &offsets, &heap, &plan->old_vers_heap, &old_vers, &mtr);

        if (err != DB_SUCCESS) {

          goto lock_wait_or_error;
        }

        if (old_vers == nullptr) {
          {
            Phy_rec record{index, rec};

            offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
          }

          /* Fetch the columns needed in
          test conditions. The clustered
          index record is protected by a
          page latch that was acquired
          by row_sel_open_pcur() or
          row_sel_restore_pcur_pos().
          The latch will not be released
          until mtr_commit(mtr). */

          row_sel_fetch_columns(index, rec, offsets, UT_LIST_GET_FIRST(plan->columns));

          if (!row_sel_test_end_conds(plan)) {

            goto table_exhausted;
          }

          goto next_rec;
        }

        rec = old_vers;
      }
    } else if (!srv_lock_sys->sec_rec_cons_read_sees(rec, node->read_view)) {
      cons_read_requires_clust_rec = true;
    }
  }

  /* PHASE 4: Test search end conditions and deleted flag */

  /* Fetch the columns needed in test conditions.  The record is
  protected by a page latch that was acquired by
  row_sel_open_pcur() or row_sel_restore_pcur_pos().  The latch
  will not be released until mtr_commit(mtr). */

  row_sel_fetch_columns(index, rec, offsets, UT_LIST_GET_FIRST(plan->columns));

  /* Test the selection end conditions: these can only contain columns
  which already are found in the index, even though the index might be
  non-clustered */

  if (plan->unique_search && cursor_just_opened) {

    /* No test necessary: the test was already made above */

  } else if (!row_sel_test_end_conds(plan)) {

    goto table_exhausted;
  }

  if (rec_get_deleted_flag(rec) && !cons_read_requires_clust_rec) {

    /* The record is delete marked: we can skip it if this is
    not a consistent read which might see an earlier version
    of a non-clustered index record */

    if (plan->unique_search) {

      goto table_exhausted;
    }

    goto next_rec;
  }

  /* PHASE 5: Get the clustered index record, if needed and if we did
  not do the search using the clustered index */

  if (plan->must_get_clust || cons_read_requires_clust_rec) {

    /* It was a non-clustered index and we must fetch also the
    clustered index record */

    err = row_sel_get_clust_rec(node, plan, rec, thr, &clust_rec, &mtr);
    mtr_has_extra_clust_latch = true;

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }

    /* Retrieving the clustered record required a search:
    increment the cost counter */

    cost_counter++;

    if (clust_rec == nullptr) {
      /* The record did not exist in the read view */
      ut_ad(consistent_read);

      goto next_rec;
    }

    if (rec_get_deleted_flag(clust_rec)) {

      /* The record is delete marked: we can skip it */

      goto next_rec;
    }

    if (node->can_get_updated) {

      plan->clust_pcur.store_position(&mtr);
    }
  }

  /* PHASE 6: Test the rest of search conditions */

  if (!row_sel_test_other_conds(plan)) {

    if (plan->unique_search) {

      goto table_exhausted;
    }

    goto next_rec;
  }

  /* PHASE 7: We found a new qualifying row for the current table; push
  the row if prefetch is on, or move to the next table in the join */

  plan->n_rows_fetched++;

  ut_ad(plan->pcur.m_latch_mode == BTR_SEARCH_LEAF);

  if ((plan->n_rows_fetched <= SEL_PREFETCH_LIMIT) || plan->unique_search || plan->no_prefetch || plan->table->m_big_rows > 0) {

    /* No prefetch in operation: go to the next table */

    goto next_table;
  }

  sel_push_prefetched_row(plan);

  if (plan->n_rows_prefetched == SEL_MAX_N_PREFETCH) {

    /* The prefetch buffer is now full */

    sel_pop_prefetched_row(plan);

    goto next_table;
  }

next_rec:
  ut_ad(!search_latch_locked);

  if (mtr_has_extra_clust_latch) {

    /* We must commit &mtr if we are moving to the next
    non-clustered index record, because we could break the
    latching order if we would access a different clustered
    index page right away without releasing the previous. */

    goto commit_mtr_for_a_while;
  }

  if (node->asc) {
    moved = plan->pcur.move_to_next(&mtr);
  } else {
    moved = plan->pcur.move_to_prev(&mtr);
  }

  if (!moved) {

    goto table_exhausted;
  }

  cursor_just_opened = false;

  /* END OF RECORD LOOP
  ------------------ */
  goto rec_loop;

next_table:
  /* We found a record which satisfies the conditions: we can move to
  the next table or return a row in the result set */

  ut_ad(plan->pcur.is_on_user_rec());

  if (plan->unique_search && !node->can_get_updated) {

    plan->cursor_at_end = true;
  } else {
    ut_ad(!search_latch_locked);

    plan->stored_cursor_rec_processed = true;

    plan->pcur.store_position(&mtr);
  }

  mtr.commit();

  mtr_has_extra_clust_latch = false;

next_table_no_mtr:
  /* If we use 'goto' to this label, it means that the row was popped
  from the prefetched rows stack, and &mtr is already committed */

  if (node->fetch_table + 1 == node->n_tables) {

    sel_eval_select_list(node);

    if (node->is_aggregate) {

      goto table_loop;
    }

    sel_assign_into_var_values(node->into_list, node);

    thr->run_node = que_node_get_parent(node);

    err = DB_SUCCESS;
    goto func_exit;
  }

  node->fetch_table++;

  /* When we move to the next table, we first reset the plan cursor:
  we do not care about resetting it when we backtrack from a table */

  plan_reset_cursor(sel_node_get_nth_plan(node, node->fetch_table));

  goto table_loop;

table_exhausted:
  /* The table cursor pcur reached the result set end: backtrack to the
  previous table in the join if we do not have cached prefetched rows */

  plan->cursor_at_end = true;

  mtr.commit();

  mtr_has_extra_clust_latch = false;

  if (plan->n_rows_prefetched > 0) {
    /* The table became exhausted during a prefetch */

    sel_pop_prefetched_row(plan);

    goto next_table_no_mtr;
  }

table_exhausted_no_mtr:
  if (node->fetch_table == 0) {
    err = DB_SUCCESS;

    if (node->is_aggregate && !node->aggregate_already_fetched) {

      node->aggregate_already_fetched = true;

      sel_assign_into_var_values(node->into_list, node);

      thr->run_node = que_node_get_parent(node);
    } else {
      node->state = SEL_NODE_NO_MORE_ROWS;

      thr->run_node = que_node_get_parent(node);
    }

    goto func_exit;
  }

  node->fetch_table--;

  goto table_loop;

stop_for_a_while:
  /* Return control for a while to que_run_threads, so that runaway
  queries can be canceled. NOTE that when we come here, we must, in a
  locking read, have placed the necessary (possibly waiting request)
  record lock on the cursor record or its successor: when we reposition
  the cursor, this record lock guarantees that nobody can meanwhile have
  inserted new records which should have appeared in the result set,
  which would result in the phantom problem. */

  ut_ad(!search_latch_locked);

  plan->stored_cursor_rec_processed = false;
  plan->pcur.store_position(&mtr);

  mtr.commit();

#ifdef UNIV_SYNC_DEBUG
  ut_ad(sync_thread_levels_empty_gen(true));
#endif /* UNIV_SYNC_DEBUG */
  err = DB_SUCCESS;
  goto func_exit;

commit_mtr_for_a_while:
  /* Stores the cursor position and commits &mtr; this is used if
  &mtr may contain latches which would break the latching order if
  &mtr would not be committed and the latches released. */

  plan->stored_cursor_rec_processed = true;

  ut_ad(!search_latch_locked);
  plan->pcur.store_position(&mtr);

  mtr.commit();

  mtr_has_extra_clust_latch = false;

#ifdef UNIV_SYNC_DEBUG
  ut_ad(sync_thread_levels_empty_gen(true));
#endif /* UNIV_SYNC_DEBUG */

  goto table_loop;

lock_wait_or_error:
  /* See the note at stop_for_a_while: the same holds for this case */

  ut_ad(!plan->pcur.is_before_first_on_page() || !node->asc);
  ut_ad(!search_latch_locked);

  plan->stored_cursor_rec_processed = false;
  plan->pcur.store_position(&mtr);

  mtr.commit();

#ifdef UNIV_SYNC_DEBUG
  ut_ad(sync_thread_levels_empty_gen(true));
#endif /* UNIV_SYNC_DEBUG */

func_exit:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return err;
}

que_thr_t *row_sel_step(que_thr_t *thr) {
  db_err err;
  Lock_mode i_lock_mode;
  sym_node_t *table_node;

  ut_ad(thr);

  auto node = static_cast<sel_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_SELECT);

  /* If this is a new time this node is executed (or when execution
  resumes after wait for a table intention lock), set intention locks
  on the tables, or assign a read view */

  if (node->into_list && (thr->prev_node == que_node_get_parent(node))) {

    node->state = SEL_NODE_OPEN;
  }

  if (node->state == SEL_NODE_OPEN) {

    /* It may be that the current session has not yet started
    its transaction, or it has been committed: */

    ut_a(thr_get_trx(thr)->m_conc_state != TRX_NOT_STARTED);

    plan_reset_cursor(sel_node_get_nth_plan(node, 0));

    if (node->consistent_read) {
      /* Assign a read view for the query */
      node->read_view = thr_get_trx(thr)->assign_read_view();
    } else {
      if (node->set_x_locks) {
        i_lock_mode = LOCK_IX;
      } else {
        i_lock_mode = LOCK_IS;
      }

      table_node = node->table_list;

      while (table_node) {
        err = srv_lock_sys->lock_table(0, table_node->table, i_lock_mode, thr);
        if (err != DB_SUCCESS) {
          thr_get_trx(thr)->m_error_state = err;

          return nullptr;
        }

        table_node = static_cast<sym_node_t *>(que_node_get_next(table_node));
      }
    }

    /* If this is an explicit cursor, copy stored procedure
    variable values, so that the values cannot change between
    fetches (currently, we copy them also for non-explicit
    cursors) */

    if (node->explicit_cursor && UT_LIST_GET_FIRST(node->copy_variables)) {

      row_sel_copy_input_variable_vals(node);
    }

    node->state = SEL_NODE_FETCH;
    node->fetch_table = 0;

    if (node->is_aggregate) {
      /* Reset the aggregate total values */
      sel_reset_aggregate_vals(node);
    }
  }

  err = row_sel(node, thr);

  /* NOTE! if queries are parallelized, the following assignment may
  have problems; the assignment should be made only if thr is the
  only top-level thr in the graph: */

  thr->graph->last_sel_node = node;

  if (err != DB_SUCCESS) {
    thr_get_trx(thr)->m_error_state = err;

    return nullptr;
  }

  return thr;
}

que_thr_t *fetch_step(que_thr_t *thr) {

  ut_ad(thr);

  auto node = static_cast<fetch_node_t *>(thr->run_node);
  auto sel_node = node->cursor_def;

  ut_ad(que_node_get_type(node) == QUE_NODE_FETCH);

  if (thr->prev_node != que_node_get_parent(node)) {

    if (sel_node->state != SEL_NODE_NO_MORE_ROWS) {

      if (node->into_list) {
        sel_assign_into_var_values(node->into_list, sel_node);
      } else {
        void *ret = (*node->func->func)(sel_node, node->func->arg);

        if (!ret) {
          sel_node->state = SEL_NODE_NO_MORE_ROWS;
        }
      }
    }

    thr->run_node = que_node_get_parent(node);

    return thr;
  }

  /* Make the fetch node the parent of the cursor definition for
  the time of the fetch, so that execution knows to return to this
  fetch node after a row has been selected or we know that there is
  no row left */

  sel_node->common.parent = node;

  if (sel_node->state == SEL_NODE_CLOSED) {
    ib_logger(ib_stream, "Error: fetch called on a closed cursor\n");

    thr_get_trx(thr)->m_error_state = DB_ERROR;

    return nullptr;
  }

  thr->run_node = sel_node;

  return thr;
}

void *row_fetch_print(void *row, void *user_arg) {
  ulint i = 0;
  que_node_t *exp;
  auto node = static_cast<sel_node_t *>(row);

  UT_NOT_USED(user_arg);

  ib_logger(ib_stream, "row_fetch_print: row %p\n", row);

  exp = node->select_list;

  while (exp) {
    dfield_t *dfield = que_node_get_val(exp);
    const dtype_t *type = dfield_get_type(dfield);

    ib_logger(ib_stream, " column %lu:\n", (ulong)i);

    dtype_print(type);
    ib_logger(ib_stream, "\n");

    if (dfield_get_len(dfield) != UNIV_SQL_NULL) {
      log_warn_buf(dfield_get_data(dfield), dfield_get_len(dfield));
    } else {
      ib_logger(ib_stream, " <NULL>;\n");
    }

    exp = que_node_get_next(exp);
    i++;
  }

  return (void *)42;
}

void *row_fetch_store_uint4(void *row, void *user_arg) {
  ulint tmp;
  auto node = static_cast<sel_node_t *>(row);
  uint32_t *val = static_cast<uint32_t *>(user_arg);

  dfield_t *dfield = que_node_get_val(node->select_list);
  const dtype_t *type = dfield_get_type(dfield);
  ulint len = dfield_get_len(dfield);

  ut_a(dtype_get_mtype(type) == DATA_INT);
  ut_a(dtype_get_prtype(type) & DATA_UNSIGNED);
  ut_a(len == 4);

  tmp = mach_read_from_4((byte *)dfield_get_data(dfield));
  *val = (uint32_t)tmp;

  return nullptr;
}

que_thr_t *row_printf_step(que_thr_t *thr) {
  que_node_t *arg;

  ut_ad(thr);

  auto node = static_cast<row_printf_node_t *>(thr->run_node);
  auto sel_node = static_cast<sel_node_t *>(node->sel_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_ROW_PRINTF);

  if (thr->prev_node == que_node_get_parent(node)) {

    /* Reset the cursor */
    sel_node->state = SEL_NODE_OPEN;

    /* Fetch next row to print */

    thr->run_node = sel_node;

    return thr;
  }

  if (sel_node->state != SEL_NODE_FETCH) {

    ut_ad(sel_node->state == SEL_NODE_NO_MORE_ROWS);

    /* No more rows to print */

    thr->run_node = que_node_get_parent(node);

    return thr;
  }

  arg = sel_node->select_list;

  while (arg) {
    dfield_print_also_hex(que_node_get_val(arg));

    ib_logger(ib_stream, " ::: ");

    arg = que_node_get_next(arg);
  }

  ib_logger(ib_stream, "\n");

  /* Fetch next row to print */

  thr->run_node = sel_node;

  return thr;
}

void row_sel_prebuild_graph(row_prebuilt_t *prebuilt) /*!< in: prebuilt handle */
{
  sel_node_t *node;

  ut_ad(prebuilt && prebuilt->trx);

  if (prebuilt->sel_graph == nullptr) {

    node = sel_node_create(prebuilt->heap);

    prebuilt->sel_graph =
      static_cast<que_fork_t *>(que_node_get_parent(pars_complete_graph_for_exec(node, prebuilt->trx, prebuilt->heap)));

    prebuilt->sel_graph->state = QUE_FORK_ACTIVE;
  }
}

/**
 * Retrieves the clustered index record corresponding to a record in a non-clustered
 * index. Does the necessary locking.
 * 
 * @param prebuilt in: prebuilt struct in the handle
 * @param sec_index in: secondary index where rec resides
 * @param rec in: record in a non-clustered index; if this is a locking read,
 *  then rec is not allowed to be delete-marked, and that would not make sense either
 * @param thr in: query thread
 * @param out_rec out: clustered record or an old version of it, NULL if the old version
 *   did not exist in the read view, i.e., it was a fresh inserted version
 * @param offsets in: offsets returned by Phy_rec::get_col_offsets(sec_index, rec); out: offsets
 *   returned by Phy_rec::get_col_offsets(clust_index, out_rec)
 * @param offset_heap in/out: memory heap from which the offsets are allocated
 * @param mtr in: mtr used to get access to the non-clustered record; the same mtr is
 *  used to access the clustered index
 * @return DB_SUCCESS or error code
 */
static ulint row_sel_get_clust_rec_with_prebuilt(
  row_prebuilt_t *prebuilt,
  Index *sec_index,
  const rec_t *rec,
  que_thr_t *thr,
  const rec_t **out_rec,
  ulint **offsets,
  mem_heap_t **offset_heap,
  mtr_t *mtr
) {

  const rec_t *clust_rec;
  rec_t *old_vers;
  ulint err;
  Trx *trx;

  *out_rec = nullptr;
  trx = thr_get_trx(thr);

  row_build_row_ref_in_tuple(prebuilt->clust_ref, rec, sec_index, *offsets, trx);

  auto clust_index = sec_index->m_table->get_clustered_index();

  prebuilt->clust_pcur->open_with_no_init(clust_index, prebuilt->clust_ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, 0, mtr, Current_location());

  clust_rec = prebuilt->clust_pcur->get_rec();

  prebuilt->clust_pcur->m_trx_if_known = trx;

  /* Note: only if the search ends up on a non-infimum record is the
  low_match value the real match to the search tuple */

  if (!page_rec_is_user_rec(clust_rec) || prebuilt->clust_pcur->get_low_match() < clust_index->get_n_unique()) {

    /* In a rare case it is possible that no clust rec is found
    for a delete-marked secondary index record: if in row0umod.c
    in row_undo_mod_remove_clust_low() we have already removed
    the clust rec, while purge is still cleaning and removing
    secondary index records associated with earlier versions of
    the clustered index record. In that case we know that the
    clustered index record did not exist in the read view of
    trx. */

    if (!rec_get_deleted_flag(rec) || prebuilt->select_lock_type != LOCK_NONE) {
      log_err("Clustered record for sec rec not found");
      srv_dict_sys->index_name_print(trx, sec_index);
      log_err("sec index record ");
      log_err(rec_to_string(rec));
      log_err("clust index record\nclust index record ");
      log_err(rec_to_string(clust_rec));
      log_err(trx->to_string(600));

      log_err("Submit a detailed bug report, check the Embedded InnoDB website for details");
    }

    clust_rec = nullptr;

    goto func_exit;
  }

  {
    Phy_rec record{clust_index, clust_rec};

    *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, offset_heap, Current_location());
  }

  if (prebuilt->select_lock_type != LOCK_NONE) {
    /* Try to place a lock on the index record; we are searching
    the clust rec with a unique condition, hence
    we set a LOCK_REC_NOT_GAP type lock */

    err = srv_lock_sys->clust_rec_read_check_and_lock(
      0,
      prebuilt->clust_pcur->get_block(),
      clust_rec,
      clust_index,
      *offsets,
      prebuilt->select_lock_type,
      LOCK_REC_NOT_GAP,
      thr
    );
    if (err != DB_SUCCESS) {

      goto err_exit;
    }
  } else {
    /* This is a non-locking consistent read: if necessary, fetch
    a previous version of the record */

    old_vers = nullptr;

    /* If the isolation level allows reading of uncommitted data,
    then we never look for an earlier version */

    if (trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && !srv_lock_sys->clust_rec_cons_read_sees(clust_rec, clust_index, *offsets, trx->m_read_view)) {

      /* The following call returns 'offsets' associated with
      'old_vers' */
      err = row_sel_build_prev_vers(trx->m_read_view, clust_index, clust_rec, offsets, offset_heap, &prebuilt->old_vers_heap, &old_vers, mtr);

      if (err != DB_SUCCESS || old_vers == nullptr) {

        goto err_exit;
      }

      clust_rec = old_vers;
    }

    /* If we had to go to an earlier version of row or the
    secondary index record is delete marked, then it may be that
    the secondary index record corresponding to clust_rec
    (or old_vers) is not rec; in that case we must ignore
    such row because in our snapshot rec would not have existed.
    Remember that from rec we cannot see directly which transaction
    id corresponds to it: we have to go to the clustered index
    record. A query where we want to fetch all rows where
    the secondary index value is in some interval would return
    a wrong result if we would not drop rows which we come to
    visit through secondary index records that would not really
    exist in our snapshot. */

    if (clust_rec &&
        (old_vers ||
        trx->m_isolation_level <= TRX_ISO_READ_UNCOMMITTED ||
        rec_get_deleted_flag(rec)) &&
        !row_sel_sec_rec_is_for_clust_rec(rec, sec_index, clust_rec, clust_index)) {
      clust_rec = nullptr;
    }
  }

func_exit:
  *out_rec = clust_rec;

  if (prebuilt->select_lock_type != LOCK_NONE) {
    /* We may use the cursor in update or in unlock_row():
    store its position */

    prebuilt->clust_pcur->store_position(mtr);
  }

  err = DB_SUCCESS;
err_exit:
  return err;
}

/**
 * @brief Restores cursor position after it has been stored. We have to take into
 * account that the record cursor was positioned on may have been deleted.
 * Then we may have to move the cursor one step up or down.
 * @param[out] same_user_rec true if we were able to restore the cursor on a user
 * record with the same ordering prefix in the B-tree index
 * @param[in] latch_mode latch mode wished in restoration
 * @param[in] pcur cursor whose position has been stored
 * @param[in] moves_up true if the cursor moves up in the index
 * @param[in] mtr mtr; CAUTION: may commit mtr temporarily!
 * @return true if we may need to process the record the cursor is now positioned on
 * (i.e. we should not go to the next record yet)
 */
static bool row_sel_restore_position(
  bool *same_user_rec,
  ulint latch_mode,
  Btree_pcursor *pcur,
  bool moves_up,
  mtr_t *mtr
) {
  bool success;

  auto relative_position = pcur->get_rel_pos();

  success = pcur->restore_position(latch_mode, mtr, Current_location());

  *same_user_rec = success;

  if (relative_position == Btree_cursor_pos::ON) {
    if (success) {
      return false;
    }

    if (moves_up) {
      (void) pcur->move_to_next(mtr);
    }

    return true;
  }

  if (relative_position == Btree_cursor_pos::AFTER || relative_position == Btree_cursor_pos::AFTER_LAST_IN_TREE) {

    if (moves_up) {
      return true;
    }

    if (pcur->is_on_user_rec()) {
      (void) pcur->move_to_prev(mtr);
    }

    return true;
  }

  ut_ad(relative_position == Btree_cursor_pos::BEFORE || relative_position == Btree_cursor_pos::BEFORE_FIRST_IN_TREE);

  if (moves_up && pcur->is_on_user_rec()) {
    (void) pcur->move_to_next(mtr);
  }

  return true;
}

/**
 * @brief Reset the row cache. The memory is not freed only the stack pointers
 * are reset.
 * @param[in] prebuilt prebuilt struct
 */
inline void row_sel_row_cache_reset(row_prebuilt_t *prebuilt) {
  prebuilt->row_cache.first = 0;
  prebuilt->row_cache.n_cached = 0;
}

bool row_sel_row_cache_is_empty(row_prebuilt_t *prebuilt) {
  return prebuilt->row_cache.n_cached == 0;
}

bool row_sel_row_cache_fetch_in_progress(row_prebuilt_t *prebuilt) {
  return (prebuilt->row_cache.first > 0 && prebuilt->row_cache.first < prebuilt->row_cache.n_size);
}

/**
 * Check if row cache is full.
 *
 * @param prebuilt in: prebuilt struct
 * @return true if row cache is full, false otherwise
 */
inline bool row_sel_row_cache_is_full(row_prebuilt_t *prebuilt) {
  ut_a(prebuilt->row_cache.n_cached <= prebuilt->row_cache.n_size);
  return prebuilt->row_cache.n_cached == prebuilt->row_cache.n_size - 1;
}

const rec_t *row_sel_row_cache_get(row_prebuilt_t *prebuilt) {
  ib_cached_row_t *row;

  ut_ad(!row_sel_row_cache_is_empty(prebuilt));

  row = &prebuilt->row_cache.ptr[prebuilt->row_cache.first];

  return row->rec;
}

void row_sel_row_cache_next(row_prebuilt_t *prebuilt) {
  if (!row_sel_row_cache_is_empty(prebuilt)) {
    --prebuilt->row_cache.n_cached;
    ++prebuilt->row_cache.first;
    ;

    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->row_cache.first = 0;
    }
  }
}

/**
 * @brief Add a record to the fetch cache.
 * @param[in] prebuilt prebuilt struct
 * @param[in] rec record to push; must be protected by a page latch
 * @param[in] offsets Phy_rec::get_col_offsets()
 */
inline void row_sel_row_cache_add(row_prebuilt_t *prebuilt, const rec_t *rec, const ulint *offsets) {
  ib_cached_row_t *row;
  ulint rec_len;
  ib_row_cache_t *row_cache;

  row_cache = &prebuilt->row_cache;
  ut_a(row_cache->first == 0);
  ut_ad(!row_sel_row_cache_is_full(prebuilt));
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  row = &prebuilt->row_cache.ptr[row_cache->n_cached];

  /* Get the size of the physical record in the page */
  rec_len = rec_offs_size(offsets);

  /* Check if there is enough space for the record being added
  to the cache. Free existing memory if it won't fit. */
  if (row->max_len < rec_len) {
    if (row->ptr != nullptr) {
      ut_a(row->max_len > 0);
      ut_a(row->rec_len > 0);
      mem_free(row->ptr);

      row->ptr = nullptr;
      row->rec = nullptr;
      row->max_len = 0;
      row->rec_len = 0;
    } else {
      ut_a(row->ptr == nullptr);
      ut_a(row->rec == nullptr);
      ut_a(row->max_len == 0);
      ut_a(row->rec_len == 0);
    }
  }

  row->rec_len = rec_len;

  if (row->ptr == nullptr) {
    row->max_len = row->rec_len * 2;
    row->ptr = static_cast<byte *>(mem_alloc(row->max_len));
  }

  ut_a(row->max_len >= row->rec_len);

  /* Note that the pointer returned by rec_copy() is actually an
  offset into cache->ptr, to the start of the record data. */
  row->rec = rec_copy(row->ptr, rec, offsets);

  ++row_cache->n_cached;
  ut_a(row_cache->n_cached < row_cache->n_size);
}

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
 * @return SEL_FOUND, SEL_EXHAUSTED, SEL_RETRY
 */
static ulint row_sel_try_search_shortcut_for_prebuilt(
  const rec_t **out_rec,
  row_prebuilt_t *prebuilt,
  ulint **offsets,
  mem_heap_t **heap,
  mtr_t *mtr
) {
  Index *index = prebuilt->index;
  const DTuple *search_tuple = prebuilt->search_tuple;
  Btree_pcursor *pcur = prebuilt->pcur;
  Trx *trx = prebuilt->trx;
  const rec_t *rec;

  ut_ad(index->is_clustered());

  pcur->open_with_no_init(index, search_tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, RW_S_LATCH, mtr, Current_location());

  rec = pcur->get_rec();

  if (!page_rec_is_user_rec(rec)) {

    return SEL_RETRY;
  }

  /* As the cursor is now placed on a user record after a search with
  the mode PAGE_CUR_GE, the up_match field in the cursor tells how many
  fields in the user record matched to the search tuple */

  if (pcur->get_up_match() < dtuple_get_n_fields(search_tuple)) {

    return SEL_EXHAUSTED;
  }

  /* This is a non-locking consistent read: if necessary, fetch
  a previous version of the record */

  {
    Phy_rec record{index, rec};

    *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, heap, Current_location());
  }

  if (!srv_lock_sys->clust_rec_cons_read_sees(rec, index, *offsets, trx->m_read_view)) {

    return SEL_RETRY;
  }

  if (rec_get_deleted_flag(rec)) {

    return SEL_EXHAUSTED;
  }

  *out_rec = rec;

  return SEL_FOUND;
}

int row_unlock_for_client(row_prebuilt_t *prebuilt, bool has_latches_on_recs) {
  Btree_pcursor *pcur = prebuilt->pcur;
  Btree_pcursor *clust_pcur = prebuilt->clust_pcur;
  Trx *trx = prebuilt->trx;
  rec_t *rec;
  mtr_t mtr;

  ut_ad(prebuilt && trx);

  if (unlikely(trx->m_isolation_level != TRX_ISO_READ_COMMITTED)) {

    log_err("row_unlock_for_client called though this session is not using READ COMMITTED isolation level.");

    return DB_SUCCESS;
  }

  trx->m_op_info = "unlock_row";

  if (prebuilt->new_rec_locks >= 1) {

    mtr.start();

    /* Restore the cursor position and find the record */

    if (!has_latches_on_recs) {
      (void) pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }

    rec = pcur->get_rec();

    srv_lock_sys->rec_unlock(trx, pcur->get_block(), rec, prebuilt->select_lock_type);

    mtr.commit();

    /* If the search was done through the clustered index, then
    we have not used clust_pcur at all, and we must NOT try to
    reset locks on clust_pcur. The values in clust_pcur may be
    garbage! */

    if (prebuilt->index->is_clustered()) {

      goto func_exit;
    }
  }

  if (prebuilt->new_rec_locks >= 1) {
    mtr.start();

    /* Restore the cursor position and find the record */

    if (!has_latches_on_recs) {
      (void)clust_pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }

    rec = clust_pcur->get_rec();

    srv_lock_sys->rec_unlock(trx, clust_pcur->get_block(), rec, prebuilt->select_lock_type);

    mtr.commit();
  }

func_exit:
  trx->m_op_info = "";

  return DB_SUCCESS;
}

db_err row_search_mvcc(ib_recovery_t recovery, ib_srch_mode_t mode, row_prebuilt_t *prebuilt, ib_match_t match_mode, ib_cur_op_t direction) {
  Index *index = prebuilt->index;
  const DTuple *search_tuple = prebuilt->search_tuple;
  Btree_pcursor *pcur = prebuilt->pcur;
  Trx *trx = prebuilt->trx;
  Index *clust_index;
  que_thr_t *thr;
  const rec_t *rec;
  const rec_t *result_rec;
  const rec_t *clust_rec;
  enum db_err err = DB_SUCCESS;
  bool unique_search = false;
  bool unique_search_from_clust_index = false;
  bool mtr_has_extra_clust_latch = false;
  bool moves_up = false;
  bool set_also_gap_locks = true;
  /* if the returned record was locked and we did a semi-consistent
  read (fetch the newest committed version), then this is set to
  true */
  ulint next_offs;
  bool same_user_rec;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  void *cmp_ctx = index->m_cmp_ctx;

  rec_offs_init(offsets_);

  prebuilt->result = -1;

  ut_ad(index && pcur && search_tuple);

  if (unlikely(prebuilt->table->m_ibd_file_missing)) {
    log_err(std::format(
      "The client is trying to use a table handle but the .ibd file for"
      " table {} does not exist. Have you deleted the .ibd file"
      " from the database directory under the datadir, or have you discarded the"
      " tablespace? Check the InnoDB website for details on how you can resolve"
      "the problem.",
      prebuilt->table->m_name
    ));

    return DB_ERROR;
  }

  if (unlikely(!prebuilt->index_usable)) {

    return DB_MISSING_HISTORY;
  }

  if (unlikely(prebuilt->magic_n != ROW_PREBUILT_ALLOCATED)) {
    log_fatal(std::format(
      "Trying to free a corrupt table handle. Magic n {}, table name {}",
      prebuilt->magic_n,
      prebuilt->table->m_name
    ));
  }

#if 0
	/* August 19, 2005 by Heikki: temporarily disable this error
	print until the cursor lock count is done correctly.
	See bugs #12263 and #12456!*/

	if (trx->m_n_tables_in_use == 0
	    && unlikely(prebuilt->select_lock_type == LOCK_NONE)) {
		/* Note that if the client uses an InnoDB temp table that it
		created inside LOCK TABLES, then n_client_tables_in_use can
		be zero; in that case select_lock_type is set to LOCK_X in
		::start_stmt. */

		ib_logger(ib_stram,
		      "Error: Client is trying to perform a read\n"
		      "but it has not locked"
		      " any tables.\n");
		trx_print(ib_stream, trx, 600);
		ib_logger(ib_stream, "\n");
	}
#endif

  /* Reset the new record lock info if session is using a
  READ COMMITED isolation level. Then we are able to remove
  the record locks set here on an individual row. */
  prebuilt->new_rec_locks = 0;

  /*-------------------------------------------------------------*/
  /* PHASE 1: Try to pop the row from the prefetch cache */

  if (unlikely(direction == ROW_SEL_MOVETO)) {
    trx->m_op_info = "starting index read";

    row_sel_row_cache_reset(prebuilt);

    if (prebuilt->sel_graph == nullptr) {
      /* Build a dummy select query graph */
      row_sel_prebuild_graph(prebuilt);
    }
  } else {
    trx->m_op_info = "fetching rows";

    /* Is this the first row being fetched by the cursor ? */
    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->row_cache.direction = direction;
    }

    if (unlikely(direction != prebuilt->row_cache.direction)) {

      if (!row_sel_row_cache_is_empty(prebuilt)) {
        ut_error;
        /* TODO: scrollable cursor: restore cursor to
        the place of the latest returned row,
        or better: prevent caching for a scroll
        cursor! */
      }

      row_sel_row_cache_reset(prebuilt);

    } else if (likely(!row_sel_row_cache_is_empty(prebuilt))) {
      err = DB_SUCCESS;
      srv_n_rows_read++;

      goto func_exit;

    } else if (row_sel_row_cache_fetch_in_progress(prebuilt)) {

      /* The previous returned row was popped from the fetch
      cache, but the cache was not full at the time of the
      popping: no more rows can exist in the result set */

      err = DB_RECORD_NOT_FOUND;
      goto func_exit;
    }

    mode = pcur->m_search_mode;
  }

  /* In a search where at most one record in the index may match, we
  can use a LOCK_REC_NOT_GAP type record lock when locking a
  non-delete-marked matching record.

  Note that in a unique secondary index there may be different
  delete-marked versions of a record where only the primary key
  values differ: thus in a secondary index we must use next-key
  locks when locking delete-marked records. */

  if (match_mode == ROW_SEL_EXACT && index->is_unique() &&
      dtuple_get_n_fields(search_tuple) == index->get_n_unique() &&
      (index->is_clustered() || !dtuple_contains_null(search_tuple))) {

    /* Note above that a UNIQUE secondary index can contain many
    rows with the same key value if one of the columns is the SQL
    null. A clustered index should never contain null PK columns
    because we demand that all the columns in primary key are
    non-null. */

    unique_search = true;
  }

  mtr.start();

  /*-------------------------------------------------------------*/
  /* PHASE 2: Try fast adaptive hash index search if possible */

  /* Next test if this is the special case where we can use the fast
  adaptive hash index to try the search. Since we must release the
  search system latch when we retrieve an externally stored field, we
  cannot use the adaptive hash index in a search in the case the row
  may be long and there may be externally stored fields */

  if (unlikely(direction == ROW_SEL_MOVETO) && unique_search && index->is_clustered()) {

    mode = IB_CUR_GE;

    unique_search_from_clust_index = true;

    if (trx->m_client_n_tables_locked == 0 && prebuilt->select_lock_type == LOCK_NONE &&
        trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && trx->m_read_view) {

      /* This is a SELECT query done as a consistent read,
      and the read view has already been allocated:
      let us try a search shortcut through the hash
      index.
      NOTE that we must also test that
      client_n_tables_locked == 0, because this might
      also be INSERT INTO ... SELECT ... or
      CREATE TABLE ... SELECT ... . Our algorithm is
      NOT prepared to inserts interleaved with the SELECT,
      and if we try that, we can deadlock on the adaptive
      hash index semaphore! */

      switch (row_sel_try_search_shortcut_for_prebuilt(&rec, prebuilt, &offsets, &heap, &mtr)) {
        case SEL_FOUND:
          /* When searching for an exact match we don't
        position the persistent cursor therefore we
        must read in the record found into the
        pre-fetch cache for the user to access. */
          if (match_mode == ROW_SEL_EXACT) {
            /* There can be no previous entries
          when doing a search with this match
          mode set. */
            ut_a(row_sel_row_cache_is_empty(prebuilt));

            row_sel_row_cache_add(prebuilt, rec, offsets);
          }

          mtr.commit();

          srv_n_rows_read++;

          prebuilt->result = 0;
          err = DB_SUCCESS;
          goto func_exit;

        case SEL_EXHAUSTED:
          mtr.commit();

          err = DB_RECORD_NOT_FOUND;
          goto func_exit;

        case SEL_RETRY:
          break;

        default:
          ut_ad(0);
      }

      mtr.commit();
      mtr.start();
    }
  }

  /*-------------------------------------------------------------*/
  /* PHASE 3: Open or restore index cursor position */

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  if (trx->m_isolation_level <= TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE && prebuilt->simple_select) {
    /* It is a plain locking SELECT and the isolation
    level is low: do not lock gaps */

    set_also_gap_locks = false;
  }

  /* Note that if the search mode was GE or G, then the cursor
  naturally moves upward (in fetch next) in alphabetical order,
  otherwise downward */

  if (unlikely(direction == ROW_SEL_MOVETO)) {
    if (mode == IB_CUR_GE || mode == IB_CUR_G) {
      moves_up = true;
    }
  } else if (direction == ROW_SEL_NEXT) {
    moves_up = true;
  }

  thr = que_fork_get_first_thr(prebuilt->sel_graph);

  que_thr_move_to_run_state_for_client(thr, trx);

  clust_index = index->get_clustered_index();

  if (likely(direction != ROW_SEL_MOVETO)) {

    if (!row_sel_restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr)) {

      goto next_rec;
    }

  } else if (dtuple_get_n_fields(search_tuple) > 0) {

    pcur->open_with_no_init(index, search_tuple, mode, BTR_SEARCH_LEAF, 0, &mtr, Current_location());

    pcur->m_trx_if_known = trx;

    rec = pcur->get_rec();

    if (!moves_up &&
        !page_rec_is_supremum(rec) &&
        set_also_gap_locks &&
        trx->m_isolation_level != TRX_ISO_READ_COMMITTED &&
        prebuilt->select_lock_type != LOCK_NONE) {

      /* Try to place a gap lock on the next index record
      to prevent phantoms in ORDER BY ... DESC queries */
      const rec_t *next = page_rec_get_next_const(rec);

      {
        Phy_rec record{index, next};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      err = sel_set_rec_lock(pcur->get_block(), next, index, offsets, prebuilt->select_lock_type, LOCK_GAP, thr);

      if (err != DB_SUCCESS) {

        goto lock_wait_or_error;
      }
    }
  } else if (mode == IB_CUR_G) {
    pcur->open_at_index_side(true, index, BTR_SEARCH_LEAF, false, 0, &mtr);
  } else if (mode == IB_CUR_L) {
    pcur->open_at_index_side(false, index, BTR_SEARCH_LEAF, false, 0, &mtr);
  }

  if (!prebuilt->sql_stat_start) {
    /* No need to set an intention lock or assign a read view */

    if (trx->m_read_view == nullptr && prebuilt->select_lock_type == LOCK_NONE) {

      ib_logger(
        ib_stream,
        "Error: The client is trying to"
        " perform a consistent read\n"
        "but the read view is not assigned!\n"
      );
      log_info(trx->to_string(600));
      ib_logger(ib_stream, "\n");
      ut_a(0);
    }
  } else if (prebuilt->select_lock_type == LOCK_NONE) {
    /* This is a consistent read */
    /* Assign a read view for the query */

    auto rv = trx->assign_read_view();
    ut_a(rv != nullptr);

    prebuilt->sql_stat_start = false;
  } else {
    Lock_mode lck_mode;
    if (prebuilt->select_lock_type == LOCK_S) {
      lck_mode = LOCK_IS;
    } else {
      lck_mode = LOCK_IX;
    }
    err = srv_lock_sys->lock_table(0, index->m_table, lck_mode, thr);

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }
    prebuilt->sql_stat_start = false;
  }

rec_loop:
  /*-------------------------------------------------------------*/
  /* PHASE 4: Look for matching records in a loop */

  rec = pcur->get_rec();

  if (page_rec_is_infimum(rec)) {

    /* The infimum record on a page cannot be in the result set,
    and neither can a record lock be placed on it: we skip such
    a record. */

    goto next_rec;
  }

  if (page_rec_is_supremum(rec)) {

    if (set_also_gap_locks && trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE) {

      /* Try to place a lock on the index record */

      /* If this session is using a READ COMMITTED isolation
      level we do not lock gaps. Supremum record is really
      a gap and therefore we do not set locks there. */

      {
        Phy_rec record{index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      err = sel_set_rec_lock(pcur->get_block(), rec, index, offsets, prebuilt->select_lock_type, LOCK_ORDINARY, thr);

      if (err != DB_SUCCESS) {

        goto lock_wait_or_error;
      }
    }
    /* A page supremum record cannot be in the result set: skip
    it now that we have placed a possible lock on it */

    goto next_rec;
  }

  /*-------------------------------------------------------------*/
  /* Do sanity checks in case our cursor has bumped into page
  corruption */

  next_offs = rec_get_next_offs(rec);

  if (unlikely(next_offs < PAGE_SUPREMUM)) {

    goto wrong_offs;
  }

  if (unlikely(next_offs >= UNIV_PAGE_SIZE - PAGE_DIR)) {

  wrong_offs:
    if (recovery == IB_RECOVERY_DEFAULT || moves_up == false) {

      ut_print_timestamp(ib_stream);
      buf_page_print(page_align(rec), 0);
      ib_logger(
        ib_stream,
        "\nrec address %p,"
        " buf block fix count %lu\n",
        (void *)rec,
        (ulong)pcur->get_block()->m_page.m_buf_fix_count
      );
      ib_logger(
        ib_stream,
        "Index corruption: rec offs %lu"
        " next offs %lu, page no %lu,\n"
        "",
        (ulong)page_offset(rec),
        (ulong)next_offs,
        (ulong)page_get_page_no(page_align(rec))
      );
      srv_dict_sys->index_name_print(trx, index);
      ib_logger(
        ib_stream,
        ". Run CHECK TABLE. You may need to\n"
        "restore from a backup, or"
        " dump + drop + reimport the table.\n"
      );

      err = DB_CORRUPTION;

      goto lock_wait_or_error;
    } else {
      /* The user may be dumping a corrupt table. Jump
      over the corruption to recover as much as possible. */

      ib_logger(
        ib_stream,
        "Index corruption: rec offs %lu"
        " next offs %lu, page no %lu,\n"
        "",
        (ulong)page_offset(rec),
        (ulong)next_offs,
        (ulong)page_get_page_no(page_align(rec))
      );
      srv_dict_sys->index_name_print(trx, index);
      ib_logger(ib_stream, ". We try to skip the rest of the page.\n");

      pcur->move_to_last_on_page(&mtr);

      goto next_rec;
    }
  }
  /*-------------------------------------------------------------*/

  /* Calculate the 'offsets' associated with 'rec' */

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (unlikely(recovery != IB_RECOVERY_DEFAULT)) {
    if (!rec_validate(rec, offsets) || !srv_btree_sys->index_rec_validate(rec, index, false)) {
      ib_logger(
        ib_stream,
        "Index corruption: rec offs %lu"
        " next offs %lu, page no %lu,\n"
        "",
        (ulong)page_offset(rec),
        (ulong)next_offs,
        (ulong)page_get_page_no(page_align(rec))
      );
      srv_dict_sys->index_name_print(trx, index);
      ib_logger(ib_stream, ". We try to skip the record.\n");

      goto next_rec;
    }
  }

  /* Note that we cannot trust the up_match value in the cursor at this
  place because we can arrive here after moving the cursor! Thus
  we have to recompare rec and search_tuple to determine if they
  match enough. */

  if (match_mode == ROW_SEL_EXACT) {
    int result;

    /* Test if the index record matches completely to search_tuple
    in prebuilt: if not, then we return with DB_RECORD_NOT_FOUND */

    /* ib_logger(ib_stream, "Comparing rec and search tuple\n"); */

    result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);

    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->result = result;
    }

    if (result != 0) {

      goto not_found;
    }

  } else if (match_mode == ROW_SEL_EXACT_PREFIX) {

    if (!cmp_dtuple_is_prefix_of_rec(cmp_ctx, search_tuple, rec, offsets)) {

      prebuilt->result = -1;
    not_found:
      if (set_also_gap_locks && trx->m_isolation_level != TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE) {

        /* Try to place a gap lock on the index
        record only if this session is not
        using a READ COMMITTED isolation level. */

        err = (db_err)sel_set_rec_lock(pcur->get_block(), rec, index, offsets, prebuilt->select_lock_type, LOCK_GAP, thr);

        if (err != DB_SUCCESS) {

          goto lock_wait_or_error;
        }
      }

      pcur->store_position(&mtr);

      err = DB_RECORD_NOT_FOUND;

      goto normal_return;
    }
  }

  /* We are ready to look at a possible new index entry in the result
  set: the cursor is now placed on a user record */

  if (prebuilt->select_lock_type != LOCK_NONE) {
    int result = -1;

    /* Try to place a lock on the index record; note that delete
    marked records are a special case in a unique search. If there
    is a non-delete marked record, then it is enough to lock its
    existence with LOCK_REC_NOT_GAP. */

    /* If this session is using a READ COMMITED isolation
    level we lock only the record, i.e., next-key locking is
    not used. */

    ulint lock_type;

    if (!set_also_gap_locks || trx->m_isolation_level == TRX_ISO_READ_COMMITTED || (unique_search && !unlikely(rec_get_deleted_flag(rec)))) {

      goto no_gap_lock;
    } else {
      lock_type = LOCK_ORDINARY;
    }

    /* If we are doing a 'greater or equal than a primary key
    value' search from a clustered index, and we find a record
    that has that exact primary key value, then there is no need
    to lock the gap before the record, because no insert in the
    gap can be in our search range. That is, no phantom row can
    appear that way.

    An example: if col1 is the primary key, the search is WHERE
    col1 >= 100, and we find a record where col1 = 100, then no
    need to lock the gap before that record. */

    if (index == clust_index && mode == IB_CUR_GE && direction == ROW_SEL_MOVETO && dtuple_get_n_fields_cmp(search_tuple) == index->get_n_unique() && !(result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets))) {
    no_gap_lock:
      lock_type = LOCK_REC_NOT_GAP;
    }

    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->result = result;
    }

    err = (db_err)sel_set_rec_lock(pcur->get_block(), rec, index, offsets, prebuilt->select_lock_type, lock_type, thr);

    if (err != DB_SUCCESS) {
      goto lock_wait_or_error;
    }
  } else {
    /* This is a non-locking consistent read: if necessary, fetch
    a previous version of the record */

    if (trx->m_isolation_level == TRX_ISO_READ_UNCOMMITTED) {

      /* Do nothing: we let a non-locking SELECT read the
      latest version of the record */

    } else if (index == clust_index) {

      /* Fetch a previous version of the row if the current
      one is not visible in the snapshot; if we have a very
      high force recovery level set, we try to avoid crashes
      by skipping this lookup */

      if (likely(recovery < IB_RECOVERY_NO_UNDO_LOG_SCAN) && !srv_lock_sys->clust_rec_cons_read_sees(rec, index, offsets, trx->m_read_view)) {

        rec_t *old_vers;
        /* The following call returns 'offsets'
        associated with 'old_vers' */
        err = (db_err
        )row_sel_build_prev_vers(trx->m_read_view, clust_index, rec, &offsets, &heap, &prebuilt->old_vers_heap, &old_vers, &mtr);

        if (err != DB_SUCCESS) {

          goto lock_wait_or_error;
        }

        if (old_vers == nullptr) {
          /* The row did not exist yet in
          the read view */

          goto next_rec;
        }

        rec = old_vers;
      }
    } else if (!srv_lock_sys->sec_rec_cons_read_sees(rec, trx->m_read_view)) {
      /* We are looking into a non-clustered index,
      and to get the right version of the record we
      have to look also into the clustered index: this
      is necessary, because we can only get the undo
      information via the clustered index record. */

      ut_ad(index != clust_index);

      if (direction == ROW_SEL_MOVETO && row_sel_row_cache_is_empty(prebuilt)) {

        prebuilt->result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
      }

      goto requires_clust_rec;
    }
  }

  /* NOTE that at this point rec can be an old version of a clustered
  index record built for a consistent read. We cannot assume after this
  point that rec is on a buffer pool page. */

  if (unlikely(rec_get_deleted_flag(rec))) {

    /* The record is delete-marked: we can skip it */

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE) {

      /* No need to keep a lock on a delete-marked record
      if we do not want to use next-key locking. */

      row_unlock_for_client(prebuilt, true);
    }

    /* This is an optimization to skip setting the next key lock
    on the record that follows this delete-marked record. This
    optimization works because of the unique search criteria
    which precludes the presence of a range lock between this
    delete marked record and the record following it.

    For now this is applicable only to clustered indexes while
    doing a unique search. There is scope for further optimization
    applicable to unique secondary indexes. Current behaviour is
    to widen the scope of a lock on an already delete marked record
    if the same record is deleted twice by the same transaction */
    if (index == clust_index && unique_search) {
      prebuilt->result = -1;
      err = DB_RECORD_NOT_FOUND;

      goto normal_return;
    }

    goto next_rec;
  } else if (direction == ROW_SEL_MOVETO && row_sel_row_cache_is_empty(prebuilt)) {

    prebuilt->result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
  }

  /* Get the clustered index record if needed, if we did not do the
  search using the clustered index. */

  if (index != clust_index && prebuilt->need_to_access_clustered) {

  requires_clust_rec:
    /* We use a 'goto' to the preceding label if a consistent
    read of a secondary index record requires us to look up old
    versions of the associated clustered index record. */

    ut_ad(rec_offs_validate(rec, index, offsets));

    /* It was a non-clustered index and we must fetch also the
    clustered index record */

    mtr_has_extra_clust_latch = true;

    /* The following call returns 'offsets' associated with
    'clust_rec'. Note that 'clust_rec' can be an old version
    built for a consistent read. */

    err = (db_err)row_sel_get_clust_rec_with_prebuilt(prebuilt, index, rec, thr, &clust_rec, &offsets, &heap, &mtr);

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }

    if (clust_rec == nullptr) {
      /* The record did not exist in the read view */
      ut_ad(prebuilt->select_lock_type == LOCK_NONE);

      goto next_rec;
    }

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE) {
      /* Note that both the secondary index record
      and the clustered index record were locked. */
      ut_ad(prebuilt->new_rec_locks == 1);
      prebuilt->new_rec_locks = 2;
    }

    if (unlikely(rec_get_deleted_flag(clust_rec))) {

      /* The record is delete marked: we can skip it */

      if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->select_lock_type != LOCK_NONE) {

        /* No need to keep a lock on a delete-marked
        record if we do not want to use next-key
        locking. */

        row_unlock_for_client(prebuilt, true);
      }

      goto next_rec;
    }

    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->result = cmp_dtuple_rec(cmp_ctx, search_tuple, clust_rec, offsets);
    }

    if (prebuilt->need_to_access_clustered) {

      result_rec = clust_rec;

      ut_ad(rec_offs_validate(result_rec, clust_index, offsets));
    } else {
      /* We used 'offsets' for the clust rec, recalculate them for 'rec' */
      {
        Phy_rec record{index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      result_rec = rec;
    }
  } else {
    result_rec = rec;

    if (row_sel_row_cache_is_empty(prebuilt)) {
      prebuilt->result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
    }
  }

  /* We found a qualifying record 'result_rec'. At this point,
  'offsets' are associated with 'result_rec'. */

  ut_ad(rec_offs_validate(result_rec, result_rec != rec ? clust_index : index, offsets));

  /* At this point, the clustered index record is protected
  by a page latch that was acquired when pcur was positioned.
  The latch will not be released until mtr_commit(&mtr). */

  if ((match_mode == ROW_SEL_EXACT || prebuilt->row_cache.n_cached >= prebuilt->row_cache.n_size - 1) &&
      prebuilt->select_lock_type == LOCK_NONE && !prebuilt->clust_index_was_generated) {

    /* Inside an update, for example, we do not cache rows,
    since we may use the cursor position to do the actual
    update, that is why we require ...lock_type == LOCK_NONE.

    FIXME: How do we handle scrollable cursors ? */

    row_sel_row_cache_add(prebuilt, result_rec, offsets);

    /* An exact match means a unique lookup, no need to
    fill the cache with more records. */
    if (unique_search || row_sel_row_cache_is_full(prebuilt)) {

      goto got_row;
    }

    goto next_rec;
  }

  ut_a(!row_sel_row_cache_is_full(prebuilt));
  row_sel_row_cache_add(prebuilt, result_rec, offsets);

  /* From this point on, 'offsets' are invalid. */
got_row:

  /* We have an optimization to save CPU time: if this is a consistent
  read on a unique condition on the clustered index, then we do not
  store the pcur position, because any fetch next or prev will anyway
  return 'end of file'. Exceptions are locking reads and where the
  user can move the cursor with PREV or NEXT even after a unique
  search. */

  if (!unique_search_from_clust_index || prebuilt->select_lock_type != LOCK_NONE) {

    /* Inside an update always store the cursor position */

    pcur->store_position(&mtr);
  }

  err = DB_SUCCESS;

  goto normal_return;

next_rec:
  prebuilt->new_rec_locks = 0;

  /*-------------------------------------------------------------*/
  /* PHASE 5: Move the cursor to the next index record */

  if (unlikely(mtr_has_extra_clust_latch)) {
    /* We must commit mtr if we are moving to the next
    non-clustered index record, because we could break the
    latching order if we would access a different clustered
    index page right away without releasing the previous. */

    pcur->store_position(&mtr);

    mtr.commit();
    mtr_has_extra_clust_latch = false;

    mtr.start();
    if (row_sel_restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr)) {

      goto rec_loop;
    }
  }

  if (moves_up) {
    if (unlikely(!pcur->move_to_next(&mtr))) {
    not_moved:
      pcur->store_position(&mtr);

      if (match_mode != ROW_SEL_DEFAULT) {
        err = DB_RECORD_NOT_FOUND;
      } else {
        err = DB_END_OF_INDEX;
      }

      prebuilt->result = -1;
      goto normal_return;
    }
  } else if (unlikely(!pcur->move_to_prev(&mtr))) {
    goto not_moved;
  }

  goto rec_loop;

lock_wait_or_error:
  /*-------------------------------------------------------------*/

  pcur->store_position(&mtr);

  mtr.commit();
  mtr_has_extra_clust_latch = false;

  trx->m_error_state = err;

  /* Stop the dummy thread we created for this query. */
  que_thr_stop_client(thr);

  thr->lock_state = QUE_THR_LOCK_ROW;

  if (ib_handle_errors(&err, trx, thr, nullptr)) {
    /* It was a lock wait, and it ended */

    thr->lock_state = QUE_THR_LOCK_NOLOCK;
    mtr.start();

    row_sel_restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr);

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && !same_user_rec) {

      /* Since we were not able to restore the cursor
      on the same user record, we cannot use
      row_unlock_for_client() to unlock any records, and
      we must thus reset the new rec lock info. Since
      in lock0lock.c we have blocked the inheriting of gap
      X-locks, we actually do not have any new record locks
      set in this case.

      Note that if we were able to restore on the 'same'
      user record, it is still possible that we were actually
      waiting on a delete-marked record, and meanwhile
      it was removed by purge and inserted again by some
      other user. But that is no problem, because in
      rec_loop we will again try to set a lock, and
      new_rec_lock_info in trx will be right at the end. */

      prebuilt->new_rec_locks = 0;
    }

    mode = static_cast<ib_srch_mode_t>(pcur->m_search_mode);

    goto rec_loop;
  }

  thr->lock_state = QUE_THR_LOCK_NOLOCK;

  goto func_exit;

normal_return:
  /*-------------------------------------------------------------*/
  que_thr_stop_for_client_no_error(thr, trx);

  mtr.commit();

  if (prebuilt->row_cache.n_cached > 0) {
    err = DB_SUCCESS;
  }

  if (err == DB_SUCCESS) {
    srv_n_rows_read++;
  }

func_exit:
  trx->m_op_info = "";

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return err;
}
