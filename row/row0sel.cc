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
Select node implementation.

Created 12/19/1997 Heikki Tuuri
*******************************************************/

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
#include "row0prebuilt.h"
#include "row0row.h"
#include "row0sel.h"
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

Row_sel *srv_row_sel;

Row_sel::Row_sel(Dict *dict, Lock_sys *lock_sys) noexcept 
  : m_dict(dict), m_lock_sys(lock_sys) {}

Row_sel *Row_sel::create(Dict *dict, Lock_sys *lock_sys) noexcept {
  auto ptr = ut_new(sizeof(Row_sel));
  return new (ptr) Row_sel(dict, lock_sys);
}

void Row_sel::destroy(Row_sel *&row_sel) noexcept {
  call_destructor(row_sel);
  ut_delete(row_sel);
  row_sel = nullptr;
}

bool Row_sel::sec_rec_is_for_blob(
  ulint mtype,
  ulint prtype,
  ulint mbminlen,
  ulint mbmaxlen,
  const byte *clust_field,
  ulint clust_len,
  const byte *sec_field,
  ulint sec_len) noexcept {

  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Blob blob(fsp, btree);
  std::array<byte, DICT_MAX_INDEX_COL_LEN> buf{};

  auto len = blob.copy_externally_stored_field_prefix(buf.data(), buf.size(), clust_field, clust_len);

  if (unlikely(len == 0)) {
    /* The BLOB was being deleted as the server crashed.
    There should not be any secondary index records
    referring to this clustered index record, because
    btr_free_externally_stored_field() is called after all
    secondary index entries of the row have been purged. */
    return false;
  }

  len = dtype_get_at_most_n_mbchars(prtype, mbminlen, mbmaxlen, sec_len, len, reinterpret_cast<const char *>(buf.data()));

  /* FIXME: Pass a NULL compare context, the compare context will be
  required once we support comparison operations outside of rem0cmp.c. */
  return cmp_data_data(nullptr, mtype, prtype, buf.data(), len, sec_field, sec_len) == 0;
}

bool Row_sel::sec_rec_is_for_clust_rec(const rec_t *sec_rec, Index *sec_index, const rec_t *clust_rec, Index *clust_index) noexcept {
  std::array<ulint, REC_OFFS_SMALL_SIZE> sec_offsets{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> clust_offsets{};
  auto sec_offs = sec_offsets.data();
  auto clust_offs = clust_offsets.data();

  rec_offs_set_n_alloc(sec_offs, sec_offsets.size());
  rec_offs_set_n_alloc(clust_offs, clust_offsets.size());

  if (unlikely(rec_get_deleted_flag(clust_rec))) {

    /* The clustered index record is delete-marked;
    it is not visible in the read view.  Besides,
    if there are any externally stored columns,
    some of them may have already been purged. */
    return false;
  }

  mem_heap_t *heap{};

  {
    Phy_rec record{clust_index, clust_rec};

    clust_offs = record.get_col_offsets(clust_offs, ULINT_UNDEFINED, &heap, Current_location());
  }

  {
    Phy_rec record{sec_index, sec_rec};

    sec_offs = record.get_col_offsets(sec_offs, ULINT_UNDEFINED, &heap, Current_location());
  }


  const auto n = sec_index->get_n_ordering_defined_by_user();

  for (ulint i{}; i < n; ++i) {
    const auto ifield = sec_index->get_nth_field(i);
    const auto col = ifield->get_col();
    auto clust_pos = clust_index->get_clustered_field_pos(col);

    ulint sec_len;
    ulint clust_len;
    const auto sec_field = rec_get_nth_field(sec_rec, sec_offs, i, &sec_len);
    const auto clust_field = rec_get_nth_field(clust_rec, clust_offs, clust_pos, &clust_len);

    auto len = clust_len;

    if (ifield->m_prefix_len > 0 && len != UNIV_SQL_NULL) {

      if (rec_offs_nth_extern(clust_offs, clust_pos)) {
        len -= BTR_EXTERN_FIELD_REF_SIZE;
      }

      len = dtype_get_at_most_n_mbchars(col->prtype, col->mbminlen, col->mbmaxlen, ifield->m_prefix_len, len, (char *)clust_field);

      if (rec_offs_nth_extern(clust_offs, clust_pos) && len < sec_len) {
        if (!sec_rec_is_for_blob(col->mtype, col->prtype, col->mbminlen, col->mbmaxlen, clust_field, clust_len, sec_field, sec_len)) {
          if (unlikely(heap != nullptr)) {
            mem_heap_free(heap);
          }
          return false;
        } else {  
          continue;
        }
      }
    }

    if (cmp_data_data(clust_index->m_cmp_ctx, col->mtype, col->prtype, clust_field, len, sec_field, sec_len) != 0) {
      if (unlikely(heap != nullptr)) {
        mem_heap_free(heap);
      }
      return false;
    }
  }

  if (unlikely(heap != nullptr)) {
    mem_heap_free(heap);
  }

  return true;
}

sel_node_t::sel_node_t() noexcept {
  m_common.type = QUE_NODE_SELECT;
  m_state = SEL_NODE_OPEN;
  m_plans = nullptr;
}

sel_node_t *sel_node_t::create(mem_heap_t *heap) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(sel_node_t));
  return new (ptr) sel_node_t();
}

void sel_node_t::clear() noexcept {
  if (unlikely(m_plans == nullptr)) {
    return;
  }

  for (ulint i{}; i < m_n_tables; ++i) {
    auto plan = get_nth_plan(i);
    plan->close();
  }
}

void sel_node_t::eval_select_list() noexcept {
  auto exp = m_select_list;

  while (exp != nullptr) {
    eval_exp(exp);

    exp = que_node_get_next(exp);
  }
}

void sel_node_t::assign_into_var_values(sym_node_t *var) noexcept {
  ut_a(var != nullptr);

  auto exp = m_select_list;

  while (var != nullptr) {
    eval_node_copy_val(var->alias, exp);

    exp = que_node_get_next(exp);
    var = static_cast<sym_node_t *>(que_node_get_next(var));
  }
}

void sel_node_t::reset_aggregate_vals() noexcept {
  ut_ad(m_is_aggregate);

  auto func_node = static_cast<func_node_t *>(m_select_list);

  while (func_node != nullptr) {
    eval_node_set_int_val(func_node, 0);

    func_node = static_cast<func_node_t *>(que_node_get_next(func_node));
  }

  m_aggregate_already_fetched = false;
}

void sel_node_t::copy_input_variable_vals() noexcept {
  for (auto var : m_copy_variables) {
    eval_node_copy_val(var, var->alias);
    var->indirection = nullptr;
  }
}

void Row_sel::fetch_columns(Index *index, const rec_t *rec, const ulint *offsets, sym_node_t *column) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));

  const auto index_type = index->is_clustered() ? SYM_CLUST_FIELD_NO : SYM_SEC_FIELD_NO;

  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Blob blob(fsp, btree);

  while (column != nullptr) {
    const auto field_no = column->field_nos[index_type];

    if (field_no != ULINT_UNDEFINED) {
      ulint len;
      bool needs_copy;
      const byte *data{};
      mem_heap_t *heap{};

      if (unlikely(rec_offs_nth_extern(offsets, field_no))) {
        /* Copy an externally stored field to the temporary heap */
        heap = mem_heap_create(1);

        data = blob.copy_externally_stored_field(rec, offsets, field_no, &len, heap);

        ut_a(len != UNIV_SQL_NULL);

        needs_copy = true;

      } else {

        data = rec_get_nth_field(rec, offsets, field_no, &len);

        needs_copy = column->copy_val;
      }

      if (likely(needs_copy)) {

        eval_node_copy_and_alloc_val(column, data, len);

      } else {

        auto val = que_node_get_val(column);

        dfield_set_data(val, data, len);
      }

      if (unlikely(heap != nullptr)) {
        mem_heap_free(heap);
      }
    }

    column = UT_LIST_GET_NEXT(col_var_list, column);
  }
}

static void prefetch_buf_alloc(sym_node_t *column) noexcept {
  ut_ad(que_node_get_type(column) == QUE_NODE_SYMBOL);

  column->prefetch_buf = static_cast<sel_buf_t *>(mem_alloc(SEL_MAX_N_PREFETCH * sizeof(sel_buf_t)));

  for (ulint i{}; i < SEL_MAX_N_PREFETCH; ++i) {
    new (&column->prefetch_buf[i]) sel_buf_t();
  }
}

void Row_sel::free_prefetch_buf(sel_buf_t *sel_buf) noexcept {
  for (ulint i{}; i < SEL_MAX_N_PREFETCH; ++i) {
    sel_buf[i].clear();
  }
}

inline void Plan::pop_prefetched_row() noexcept {
  ut_ad(m_n_rows_prefetched > 0);

  for (auto column : m_columns) {
    auto val = que_node_get_val(column);

    if (unlikely(!column->copy_val)) {
      /* We did not really push any value for the column */

      ut_ad(column->prefetch_buf == nullptr);
      ut_ad(que_node_get_val_buf_size(column) == 0);
      ut_d(dfield_set_null(val));

    } else {

      ut_ad(column->prefetch_buf);
      ut_ad(!dfield_is_ext(val));

      auto sel_buf = column->prefetch_buf + m_first_prefetched;

      auto len = sel_buf->m_len;
      auto data = sel_buf->m_data;
      auto val_buf_size = sel_buf->m_val_buf_size;

      /* We must keep track of the allocated memory for
      column values to be able to free it later: therefore
      we swap the values for sel_buf and val */

      sel_buf->m_len = dfield_get_len(val);
      sel_buf->m_data = static_cast<byte *>(dfield_get_data(val));
      sel_buf->m_val_buf_size = que_node_get_val_buf_size(column);

      dfield_set_data(val, data, len);
      que_node_set_val_buf_size(column, val_buf_size);
    }
  }

  --m_n_rows_prefetched;
  ++m_first_prefetched;
}

inline void Plan::push_prefetched_row() noexcept {
  ulint pos;

  if (m_n_rows_prefetched == 0) {
    pos = 0;
    m_first_prefetched = 0;
  } else {
    pos = m_n_rows_prefetched;

    /* We have the convention that pushing new rows starts only
    after the prefetch stack has been emptied: */

    ut_ad(m_first_prefetched == 0);
  }

  ++m_n_rows_prefetched;

  ut_ad(pos < SEL_MAX_N_PREFETCH);

  for (auto column : m_columns) {
    if (likely(column->copy_val)) {

      if (unlikely(column->prefetch_buf == nullptr)) {
        /* Allocate a new prefetch buffer */
        prefetch_buf_alloc(column);
      }

      auto sel_buf = &column->prefetch_buf[pos];
      auto val = que_node_get_val(column);
      auto data = static_cast<byte *>(dfield_get_data(val));
      auto len = dfield_get_len(val);
      auto val_buf_size = que_node_get_val_buf_size(column);

      /* We must keep track of the allocated memory for
      column values to be able to free it later: therefore
      we swap the values for sel_buf and val */

      dfield_set_data(val, sel_buf->m_data, sel_buf->m_len);
      que_node_set_val_buf_size(column, sel_buf->m_val_buf_size);

      sel_buf->m_data = data;
      sel_buf->m_len = len;
      sel_buf->m_val_buf_size = val_buf_size;
    }
  }
}

inline bool Plan::test_end_conds() noexcept {
  /* All conditions in end_conds are comparisons of a column to an expression */

  for (auto cond : m_end_conds) {
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

inline bool Plan::test_other_conds() noexcept {
  for (auto cond : m_other_conds) {
    eval_exp(cond);

    if (!eval_node_get_bool_val(cond)) {
      return false;
    }
  }

  return true;
}

[[nodiscard]] db_err Row_sel::build_prev_vers(Row_vers::Row &row) noexcept {
  if (row.m_old_row_heap != nullptr) {
    mem_heap_empty(row.m_old_row_heap);
  } else {
    row.m_old_row_heap = mem_heap_create(512);
  }

  return srv_row_vers->build_for_consistent_read(row);
}

db_err Plan::get_clust_rec(sel_node_t *sel_node, const rec_t *rec, que_thr_t *thr, const rec_t *&out_rec, mtr_t *mtr) noexcept {
  mem_heap_t *heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets{};

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  out_rec = nullptr;

  auto func_exit = [&heap](db_err err) -> auto {
    if (unlikely(heap != nullptr)) {
      mem_heap_free(heap);
    }

    return DB_SUCCESS;
  };

  ulint *offsets;

  {
    Phy_rec record{m_pcur.get_btr_cur()->m_index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  row_build_row_ref_fast(m_clust_ref, m_clust_map, rec, offsets);

  auto index = m_table->get_clustered_index();

  m_clust_pcur.open_with_no_init(index, m_clust_ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, 0, mtr, Current_location());

  const rec_t *clust_rec = m_clust_pcur.get_rec();

  /* Note: only if the search ends up on a non-infimum record is the
  low_match value the real match to the search tuple */

  if (!page_rec_is_user_rec(clust_rec) || m_clust_pcur.get_low_match() < index->get_n_unique()) {

    ut_a(rec_get_deleted_flag(rec));
    ut_a(sel_node->m_read_view);

    /* In a rare case it is possible that no clust rec is found for a delete-marked
    secondary index record: if in row0umod. in row_undo_mod_remove_clust_low() we
    have already removed the clust rec, while purge is still cleaning and removing
    secondary index records associated with earlier versions of the clustered index
    record. In that case we know that the clustered index record did not exist in
    the read view of trx. */

    return func_exit(DB_SUCCESS);
  }

  {
    Phy_rec record{index, clust_rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (sel_node->m_read_view == nullptr) {
    /* Try to place a lock on the index record */

    /* If this session is using READ COMMITTED isolation level we lock only the record, i.e., next-key locking is not used. */
    auto trx = thr_get_trx(thr);
    auto block = m_clust_pcur.get_block();
    const auto lock_mode = sel_node->m_row_lock_mode;
    const auto lock_type = trx->m_isolation_level == TRX_ISO_READ_COMMITTED ? LOCK_REC_NOT_GAP : LOCK_ORDINARY;
    const auto err = m_lock_sys->clust_rec_read_check_and_lock(0, block, clust_rec, index, offsets, lock_mode, lock_type, thr);

    if (unlikely(err != DB_SUCCESS)) {
      return func_exit(err);
    }

  } else {
    /* This is a non-locking consistent read: if necessary, fetch a previous version of the record */

    const rec_t *old_vers{};

    if (!m_lock_sys->clust_rec_cons_read_sees(clust_rec, index, offsets, sel_node->m_read_view)) {

      Row_vers::Row row {
        .m_cluster_rec = clust_rec,
        .m_mtr = mtr,
        .m_cluster_index = index,
        .m_cluster_offsets = offsets,
        .m_consistent_read_view = sel_node->m_read_view,
        .m_cluster_offset_heap = heap,
        .m_old_row_heap = m_old_row_heap,
        .m_old_rec = old_vers
      };

      const auto err = m_sel->build_prev_vers(row);

      if (unlikely(err != DB_SUCCESS)) {

        return func_exit(err);
      }

      clust_rec = old_vers;

      if (clust_rec == nullptr) {
        return func_exit(DB_SUCCESS);
      }
    }

    /* If we had to go to an earlier version of row or the secondary index record is delete marked, then it may be that
    the secondary index record corresponding to clust_rec (or old_vers) is not rec; in that case we must ignore
    such row because in our snapshot rec would not have existed.  Remember that from rec we cannot see directly which transaction
    id corresponds to it: we have to go to the clustered index record. A query where we want to fetch all rows where
    the secondary index value is in some interval would return a wrong result if we would not drop rows which we come to
    visit through secondary index records that would not really exist in our snapshot. */

    if ((old_vers != nullptr || rec_get_deleted_flag(rec)) && !m_sel->sec_rec_is_for_clust_rec(rec, m_index, clust_rec, index)) {
      return func_exit(DB_SUCCESS);
    }
  }

  /* Fetch the columns needed in test conditions.  The clustered index record is protected by a page latch that was acquired
  when plan->clust_pcur was positioned.  The latch will not be released until mtr_commit(mtr). */

  m_sel->fetch_columns(index, clust_rec, offsets, m_columns.front());

  out_rec = clust_rec;

  if (unlikely(heap != nullptr)) {
    mem_heap_free(heap);
  }

  return DB_SUCCESS;
}

inline db_err Row_sel::set_rec_lock(const Buf_block *block, const rec_t *rec, Index *index, const ulint *offsets, Lock_mode mode, ulint type, que_thr_t *thr) noexcept {
  auto trx = thr_get_trx(thr);
  auto buf_pool = m_dict->m_store.m_btree->m_buf_pool;

  if (trx->m_trx_locks.size() > 10000 && buf_pool->m_LRU->buf_pool_running_out()) {
    return DB_LOCK_TABLE_FULL;
  } else  if (index->is_clustered()) {
    return m_lock_sys->clust_rec_read_check_and_lock(0, block, rec, index, offsets, mode, type, thr);
  } else {
    return m_lock_sys->sec_rec_read_check_and_lock(0, block, rec, index, offsets, mode, type, thr);
  }
}

void Plan::open_pcur(bool search_latch_locked, mtr_t *mtr) noexcept {
  ulint has_search_latch{}; /* RW_S_LATCH or 0 */

  if (search_latch_locked) {
    has_search_latch = RW_S_LATCH;
  }

  /* Calculate the value of the search tuple: the exact match columns
  get their expressions evaluated when we evaluate the right sides of
  end_conds */

  for (auto cond : m_end_conds) {
    eval_exp(que_node_get_next(cond->args));
  }

  if (m_tuple != nullptr) {
    const auto n_fields = dtuple_get_n_fields(m_tuple);

    if (m_n_exact_match < n_fields) {
      /* There is a non-exact match field which must be
      evaluated separately */

      eval_exp(m_tuple_exps[n_fields - 1]);
    }

    for (ulint i{}; i < n_fields; ++i) {
      auto exp = m_tuple_exps[i];

      dfield_copy_data(dtuple_get_nth_field(m_tuple, i), que_node_get_val(exp));
    }

    /* Open pcur to the index */

    m_pcur.open_with_no_init(m_index, m_tuple, m_mode, BTR_SEARCH_LEAF, has_search_latch, mtr, Current_location());

  } else {
    /* Open the cursor to the start or the end of the index (false: no init) */
    m_pcur.open_at_index_side(m_asc, m_index, BTR_SEARCH_LEAF, false, 0, mtr);
  }

  ut_ad(!m_cursor_at_end);
  ut_ad(m_n_rows_fetched == 0);
  ut_ad(m_n_rows_prefetched == 0);

  m_pcur_is_open = true;
}

bool Plan::restore_pcur_pos(mtr_t *mtr) noexcept {
  ut_ad(!m_cursor_at_end);

  auto relative_position = m_pcur.get_rel_pos();
  auto equal_position = m_pcur.restore_position(BTR_SEARCH_LEAF, mtr, Current_location());

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
  Plan::m_stored_cursor_rec_processed is true, we must move to the next
  record, else there is no need to move the cursor. */

  if (m_asc) {
    if (relative_position == Btree_cursor_pos::ON) {
      return equal_position ? m_stored_cursor_rec_processed : true;
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

    return equal_position ? m_stored_cursor_rec_processed : false;
  }

  ut_ad(relative_position == Btree_cursor_pos::AFTER || relative_position == Btree_cursor_pos::AFTER_LAST_IN_TREE);

  return true;
}

Search_status Plan::try_search_shortcut(sel_node_t *sel_node, mtr_t *mtr) noexcept {
  mem_heap_t *heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets{};

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  ut_ad(m_unique_search);
  ut_ad(m_must_get_clust);
  ut_ad(sel_node->m_read_view != nullptr);

  auto func_exit = [&heap](Search_status status) -> auto {
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }

    return status;
  };

  open_pcur(true, mtr);

  auto rec = m_pcur.get_rec();

  if (!page_rec_is_user_rec(rec)) {

    return Search_status::RETRY;
  }

  ut_ad(m_mode == PAGE_CUR_GE);

  /* As the cursor is now placed on a user record after a search with
  the mode PAGE_CUR_GE, the up_match field in the cursor tells how many
  fields in the user record matched to the search tuple */

  if (m_pcur.get_up_match() < m_n_exact_match) {

    return Search_status::EXHAUSTED;
  }

  /* This is a non-locking consistent read: if necessary, fetch
  a previous version of the record */

  ulint *offsets;

  {
    Phy_rec record{m_index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  auto lock_sys = m_sel->m_lock_sys;

  if (m_index->is_clustered()) {
    if (!lock_sys->clust_rec_cons_read_sees(rec, m_index, offsets, sel_node->m_read_view)) {
      return func_exit(Search_status::RETRY);
    }
  } else if (!lock_sys->sec_rec_cons_read_sees(rec, sel_node->m_read_view)) {
      return func_exit(Search_status::RETRY);
  }

  if (rec_get_deleted_flag(rec)) {
    /* Test the deleted flag. */
    return func_exit(Search_status::EXHAUSTED);
  }

  /* Fetch the columns needed in test conditions.  The index
  record is protected by a page latch that was acquired when
  plan->pcur was positioned.  The latch will not be released
  until mtr_commit(mtr). */

  m_sel->fetch_columns(m_index, rec, offsets, m_columns.front());

  /* Test the rest of search conditions */

  if (!test_other_conds()) {
    return func_exit(Search_status::EXHAUSTED);
  }

  ut_ad(m_pcur.m_latch_mode == BTR_SEARCH_LEAF);

  ++m_n_rows_fetched;

  return func_exit(Search_status::FOUND);
}

db_err Row_sel::select(sel_node_t *sel_node, que_thr_t *thr) noexcept {
  mtr_t mtr;
  Plan *plan;
  bool moved;
  Index *index;
  const rec_t *rec;
  const rec_t *old_vers;
  const rec_t *clust_rec;

  /* The following flag becomes true when we are doing a
  consistent read from a non-clustered index and we must look
  at the clustered index to find out the previous delete mark
  state of the non-clustered record: */

  ulint cost_counter{};
  bool cursor_just_opened;
  bool mtr_has_extra_clust_latch = false;
  bool cons_read_requires_clust_rec = false;

  /* true if the search was made using a non-clustered index, and we had to
  access the clustered record: now &mtr contains a clustered index latch, and
  &mtr must be committed before we move to the next non-clustered record */
  db_err err;

  mem_heap_t *heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets{};
  ulint *offsets = rec_offsets.data();

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  ut_ad(thr->run_node == sel_node);

  bool search_latch_locked{};
  const auto consistent_read = sel_node->m_read_view != nullptr;

table_loop:
  /* TABLE LOOP
  ----------
  This is the outer major loop in calculating a join. We come here when
  node->m_fetch_table changes, and after adding a row to aggregate totals
  and, of course, when this function is called. */

  ut_ad(mtr_has_extra_clust_latch == false);

  plan = sel_node->get_nth_plan(sel_node->m_fetch_table);
  index = plan->m_index;

  if (plan->m_n_rows_prefetched > 0) {
    plan->pop_prefetched_row();

    goto next_table_no_mtr;
  }

  if (plan->m_cursor_at_end) {
    /* The cursor has already reached the result set end: no more
    rows to process for this table cursor, as also the prefetch
    stack was empty */

    ut_ad(plan->m_pcur_is_open);

    goto table_exhausted_no_mtr;
  }

  /* Open a cursor to index, or restore an open cursor position */

  mtr.start();

  if (consistent_read && plan->m_unique_search && !plan->m_pcur_is_open && !plan->m_must_get_clust && !plan->m_table->m_big_rows) {

    const auto found_flag = plan->try_search_shortcut(sel_node, &mtr);

    if (found_flag == Search_status::FOUND) {

      goto next_table;

    } else if (found_flag == Search_status::EXHAUSTED) {

      goto table_exhausted;
    }

    ut_ad(found_flag == Search_status::RETRY);

    plan->reset_cursor();

    mtr.commit();

    mtr.start();
  }

  if (!plan->m_pcur_is_open) {
    /* Evaluate the expressions to build the search tuple and
    open the cursor */

    plan->open_pcur(search_latch_locked, &mtr);

    cursor_just_opened = true;

    /* A new search was made: increment the cost counter */
    ++cost_counter;

  } else {
    /* Restore pcur position to the index */

    const auto must_go_to_next = plan->restore_pcur_pos(&mtr);

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

  rec = plan->m_pcur.get_rec();

  /* PHASE 1: Set a lock if specified */

  if (!sel_node->m_asc && cursor_just_opened && !page_rec_is_supremum(rec)) {

    /* When we open a cursor for a descending search, we must set
    a next-key lock on the successor record: otherwise it would
    be possible to insert new records next to the cursor position,
    and it might be that these new records should appear in the
    search result set, resulting in the phantom problem. */

    if (!consistent_read) {

      /* If this session is using READ COMMITTED isolation
      level, we lock only the record, i.e., next-key
      locking is not used. */

      auto next_rec = page_rec_get_next(rec);
      auto trx = thr_get_trx(thr);

      {
        Phy_rec record{index, next_rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      ulint lock_type;

      if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED) {

        if (page_rec_is_supremum(next_rec)) {

          goto skip_lock;
        }

        lock_type = LOCK_REC_NOT_GAP;
      } else {
        lock_type = LOCK_ORDINARY;
      }

      {
        auto block = plan->m_pcur.get_block();
        const auto lock_mode = sel_node->m_row_lock_mode;

        err = set_rec_lock(block, next_rec, index, offsets, lock_mode, lock_type, thr);

        if (err != DB_SUCCESS) {
          /* Note that in this case we will store in pcur the PREDECESSOR of the record
           * we are waiting the lock for */

          goto lock_wait_or_error;
        }
      }
    }
  }

skip_lock:
  if (page_rec_is_infimum(rec)) {

    /* The infimum record on a page cannot be in the result set,
    and neither can a record lock be placed on it: we skip such
    a record. We also increment the cost counter as we may have
    processed yet another page of index. */

    ++cost_counter;

    goto next_rec;
  }

  if (!consistent_read) {
    /* Try to place a lock on the index record */

    /* If this session is using READ COMMITTED isolation level,
    we lock only the record, i.e., next-key locking is
    not used. */

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
    }

    ulint lock_type;
    auto trx = thr_get_trx(thr);

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED) {

      if (page_rec_is_supremum(rec)) {

        goto next_rec;
      }

      lock_type = LOCK_REC_NOT_GAP;
    } else {
      lock_type = LOCK_ORDINARY;
    }

    {
      auto block = plan->m_pcur.get_block();
      const auto lock_mode = sel_node->m_row_lock_mode;

      err = set_rec_lock(block, rec, index, offsets, lock_mode, lock_type, thr);
    }

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

  if (plan->m_unique_search && cursor_just_opened) {

    ut_ad(plan->m_mode == PAGE_CUR_GE);

    /* As the cursor is now placed on a user record after a search
    with the mode PAGE_CUR_GE, the up_match field in the cursor
    tells how many fields in the user record matched to the search
    tuple */

    if (plan->m_pcur.get_up_match() < plan->m_n_exact_match) {
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

      if (!m_lock_sys->clust_rec_cons_read_sees(rec, index, offsets, sel_node->m_read_view)) {

        Row_vers::Row row {
          .m_cluster_rec = rec,
          .m_mtr = &mtr,
          .m_cluster_index = index,
          .m_cluster_offsets = offsets,
          .m_consistent_read_view = sel_node->m_read_view,
          .m_cluster_offset_heap = heap,
          .m_old_row_heap = plan->m_old_row_heap,
          .m_old_rec = old_vers
      };

        err = build_prev_vers(row);

        if (err != DB_SUCCESS) {

          goto lock_wait_or_error;
        }

        if (old_vers == nullptr) {
          {
            Phy_rec record{index, rec};

            offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
          }

          /* Fetch the columns needed in test conditions. The clustered
          index record is protected by a page latch that was acquired
          by row_sel_open_pcur() or row_sel_restore_pcur_pos().
          The latch will not be released until mtr_commit(mtr). */

          Row_sel::fetch_columns(index, rec, offsets, UT_LIST_GET_FIRST(plan->m_columns));

          if (!plan->test_end_conds()) {

            goto table_exhausted;
          }

          goto next_rec;
        }

        rec = old_vers;
      }
    } else if (!m_lock_sys->sec_rec_cons_read_sees(rec, sel_node->m_read_view)) {
      cons_read_requires_clust_rec = true;
    }
  }

  /* PHASE 4: Test search end conditions and deleted flag */

  /* Fetch the columns needed in test conditions. The record is protected by a page latch that was acquired by
  Plan::open_pcur() or Plan::restore_pcur_pos(). The latch will not be released until mtr_commit(mtr). */

  fetch_columns(index, rec, offsets, plan->m_columns.front());

  /* Test the selection end conditions: these can only contain columns
  which already are found in the index, even though the index might be
  non-clustered */

  if (plan->m_unique_search && cursor_just_opened) {

    /* No test necessary: the test was already made above */

  } else if (!plan->test_end_conds()) {

    goto table_exhausted;
  }

  if (rec_get_deleted_flag(rec) && !cons_read_requires_clust_rec) {

    /* The record is delete marked: we can skip it if this is not a consistent read which might see an earlier version
    of a non-clustered index record */

    if (plan->m_unique_search) {

      goto table_exhausted;
    }

    goto next_rec;
  }

  /* PHASE 5: Get the clustered index record, if needed and if we did
  not do the search using the clustered index */

  if (plan->m_must_get_clust || cons_read_requires_clust_rec) {

    /* It was a non-clustered index and we must fetch also the clustered index record */

    err = plan->get_clust_rec(sel_node, rec, thr, clust_rec, &mtr);

    mtr_has_extra_clust_latch = true;

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }

    /* Retrieving the clustered record required a search: increment the cost counter */

    ++cost_counter;

    if (clust_rec == nullptr) {
      /* The record did not exist in the read view */
      ut_ad(consistent_read);

      goto next_rec;
    }

    if (rec_get_deleted_flag(clust_rec)) {

      /* The record is delete marked: we can skip it */

      goto next_rec;
    }

    if (sel_node->m_can_get_updated) {

      plan->m_clust_pcur.store_position(&mtr);
    }
  }

  /* PHASE 6: Test the rest of search conditions */

  if (!plan->test_other_conds()) {

    if (plan->m_unique_search) {

      goto table_exhausted;
    }

    goto next_rec;
  }

  /* PHASE 7: We found a new qualifying row for the current table; push
  the row if prefetch is on, or move to the next table in the join */

  ++plan->m_n_rows_fetched;

  ut_ad(plan->m_pcur.m_latch_mode == BTR_SEARCH_LEAF);

  if ((plan->m_n_rows_fetched <= SEL_PREFETCH_LIMIT) || plan->m_unique_search || plan->m_no_prefetch || plan->m_table->m_big_rows > 0) {

    /* No prefetch in operation: go to the next table */

    goto next_table;
  }

  plan->push_prefetched_row();

  if (plan->m_n_rows_prefetched == SEL_MAX_N_PREFETCH) {

    /* The prefetch buffer is now full */

    plan->pop_prefetched_row();

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

  if (sel_node->m_asc) {
    moved = plan->m_pcur.move_to_next(&mtr);
  } else {
    moved = plan->m_pcur.move_to_prev(&mtr);
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

  ut_ad(plan->m_pcur.is_on_user_rec());

  if (plan->m_unique_search && !sel_node->m_can_get_updated) {

    plan->m_cursor_at_end = true;

  } else {
    ut_ad(!search_latch_locked);

    plan->m_stored_cursor_rec_processed = true;

    plan->m_pcur.store_position(&mtr);
  }

  mtr.commit();

  mtr_has_extra_clust_latch = false;

next_table_no_mtr:
  /* If we use 'goto' to this label, it means that the row was popped
  from the prefetched rows stack, and &mtr is already committed */

  if (sel_node->m_fetch_table + 1 == sel_node->m_n_tables) {

    sel_node->eval_select_list();

    if (sel_node->m_is_aggregate) {

      goto table_loop;
    }

    if (sel_node->m_into_list != nullptr) {
      sel_node->assign_into_var_values(sel_node->m_into_list);
    }

    thr->run_node = que_node_get_parent(sel_node);

    err = DB_SUCCESS;
    goto func_exit;
  }

  ++sel_node->m_fetch_table;

  /* When we move to the next table, we first reset the plan cursor:
  we do not care about resetting it when we backtrack from a table */

  sel_node->get_nth_plan(sel_node->m_fetch_table)->reset_cursor();

  goto table_loop;

table_exhausted:
  /* The table cursor pcur reached the result set end: backtrack to the
  previous table in the join if we do not have cached prefetched rows */

  plan->m_cursor_at_end = true;

  mtr.commit();

  mtr_has_extra_clust_latch = false;

  if (plan->m_n_rows_prefetched > 0) {
    /* The table became exhausted during a prefetch */

    plan->pop_prefetched_row();

    goto next_table_no_mtr;
  }

table_exhausted_no_mtr:
  if (sel_node->m_fetch_table == 0) {
    err = DB_SUCCESS;

    if (sel_node->m_is_aggregate && !sel_node->m_aggregate_already_fetched) {

      sel_node->m_aggregate_already_fetched = true;

      if (sel_node->m_into_list != nullptr) {
        sel_node->assign_into_var_values(sel_node->m_into_list);
      }

      thr->run_node = que_node_get_parent(sel_node);
    } else {
      sel_node->m_state = SEL_NODE_NO_MORE_ROWS;

      thr->run_node = que_node_get_parent(sel_node);
    }

    goto func_exit;
  }

  --sel_node->m_fetch_table;

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

  plan->m_stored_cursor_rec_processed = false;
  plan->m_pcur.store_position(&mtr);

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

  plan->m_stored_cursor_rec_processed = true;

  ut_ad(!search_latch_locked);
  plan->m_pcur.store_position(&mtr);

  mtr.commit();

  mtr_has_extra_clust_latch = false;

#ifdef UNIV_SYNC_DEBUG
  ut_ad(sync_thread_levels_empty_gen(true));
#endif /* UNIV_SYNC_DEBUG */

  goto table_loop;

lock_wait_or_error:
  /* See the note at stop_for_a_while: the same holds for this case */

  ut_ad(!plan->m_pcur.is_before_first_on_page() || !sel_node->m_asc);
  ut_ad(!search_latch_locked);

  plan->m_stored_cursor_rec_processed = false;
  plan->m_pcur.store_position(&mtr);

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

que_thr_t *Row_sel::step(que_thr_t *thr) noexcept {
  auto sel_node = static_cast<sel_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(sel_node) == QUE_NODE_SELECT);

  /* If this is a new time this node is executed (or when execution
  resumes after wait for a table intention lock), set intention locks
  on the tables, or assign a read view */

  if (sel_node->m_into_list != nullptr && thr->prev_node == que_node_get_parent(sel_node)) {

    sel_node->m_state = SEL_NODE_OPEN;
  }

  if (sel_node->m_state == SEL_NODE_OPEN) {

    /* It may be that the current session has not yet started
    its transaction, or it has been committed: */

    ut_a(thr_get_trx(thr)->m_conc_state != TRX_NOT_STARTED);

    sel_node->get_nth_plan(0)->reset_cursor();

    if (sel_node->m_consistent_read) {
      /* Assign a read view for the query */
      sel_node->m_read_view = thr_get_trx(thr)->assign_read_view();

    } else {

      auto table_node = sel_node->m_table_list;
      const auto lock_mode = sel_node->m_set_x_locks ? LOCK_IX : LOCK_IS;

      while (table_node != nullptr) {
        const auto err = m_lock_sys->lock_table(0, table_node->table, lock_mode, thr);

        if (err != DB_SUCCESS) {
          thr_get_trx(thr)->m_error_state = err;

          return nullptr;
        }

        table_node = static_cast<sym_node_t *>(que_node_get_next(table_node));
      }
    }

    /* If this is an explicit cursor, copy stored procedure variable values, so that the values cannot change between
    fetches (currently, we copy them also for non-explicit cursors) */

    if (sel_node->m_explicit_cursor && !sel_node->m_copy_variables.empty()) {

      sel_node->copy_input_variable_vals();
    }

    sel_node->m_fetch_table = 0;
    sel_node->m_state = SEL_NODE_FETCH;

    if (sel_node->m_is_aggregate) {
      /* Reset the aggregate total values */
      sel_node->reset_aggregate_vals();
    }
  }

  const auto err = select(sel_node, thr);

  /* NOTE! if queries are parallelized, the following assignment may
  have problems; the assignment should be made only if thr is the
  only top-level thr in the graph: */

  thr->graph->last_sel_node = sel_node;

  if (err != DB_SUCCESS) {
    thr_get_trx(thr)->m_error_state = err;
    return nullptr;
  } else {
    return thr;
  }
}

que_thr_t *Row_sel::fetch_step(que_thr_t *thr) noexcept {
  ut_ad(thr);

  auto fetch_node = static_cast<fetch_node_t *>(thr->run_node);
  auto sel_node = fetch_node->m_cursor_def;

  ut_ad(que_node_get_type(fetch_node) == QUE_NODE_FETCH);

  if (thr->prev_node != que_node_get_parent(fetch_node)) {

    if (sel_node->m_state != SEL_NODE_NO_MORE_ROWS) {

      if (fetch_node->m_into_list != nullptr) {
        sel_node->assign_into_var_values(fetch_node->m_into_list);
      } else {
        void *ret = (*fetch_node->m_func->func)(sel_node, fetch_node->m_func->arg);

        if (ret == nullptr) {
          sel_node->m_state = SEL_NODE_NO_MORE_ROWS;
        }
      }
    }

    thr->run_node = que_node_get_parent(fetch_node);

    return thr;
  }

  /* Make the fetch node the parent of the cursor definition for
  the time of the fetch, so that execution knows to return to this
  fetch node after a row has been selected or we know that there is
  no row left */

  sel_node->m_common.parent = fetch_node;

  if (sel_node->m_state == SEL_NODE_CLOSED) {
    ib_logger(ib_stream, "Error: fetch called on a closed cursor\n");

    thr_get_trx(thr)->m_error_state = DB_ERROR;

    return nullptr;
  }

  thr->run_node = sel_node;

  return thr;
}

void *Row_sel::fetch_print(void *row) noexcept {
  auto sel_node = static_cast<sel_node_t *>(row);

  log_info(std::format("Row_sel::fetch_print: row {}", (void*)row));

  ulint i{};
  auto exp = sel_node->m_select_list;

  while (exp != nullptr) {
    auto dfield = que_node_get_val(exp);
    const auto type = dfield_get_type(dfield);

    log_warn(" column: ", i);

    dtype_print(type);
    log_info("");

    if (dfield_get_len(dfield) != UNIV_SQL_NULL) {
      log_warn_buf(dfield_get_data(dfield), dfield_get_len(dfield));
    } else {
      log_warn(" <NULL>;");
    }

    exp = que_node_get_next(exp);
    ++i;
  }

  return (void*) 42;
}

void *Row_sel::fetch_store_uint4(void *row, void *user_arg) noexcept {
  auto sel_node = static_cast<sel_node_t *>(row);
  auto val = static_cast<uint32_t *>(user_arg);

  auto dfield = que_node_get_val(sel_node->m_select_list);
  const auto type = dfield_get_type(dfield);
  const auto len = dfield_get_len(dfield);

  ut_a(dtype_get_mtype(type) == DATA_INT);
  ut_a(dtype_get_prtype(type) & DATA_UNSIGNED);
  ut_a(len == 4);

  uint32_t tmp = mach_read_from_4(static_cast<byte *>(dfield_get_data(dfield)));

  *val = tmp;

  return nullptr;
}

que_thr_t *Row_sel::printf_step(que_thr_t *thr) noexcept {
  auto row_printf_node = static_cast<row_printf_node_t *>(thr->run_node);
  auto sel_node = static_cast<sel_node_t *>(row_printf_node->m_sel_node);

  ut_ad(que_node_get_type(row_printf_node) == QUE_NODE_ROW_PRINTF);

  if (thr->prev_node == que_node_get_parent(row_printf_node)) {

    /* Reset the cursor */
    sel_node->m_state = SEL_NODE_OPEN;

    /* Fetch next row to print */

    thr->run_node = sel_node;

    return thr;
  }

  if (sel_node->m_state != SEL_NODE_FETCH) {

    ut_ad(sel_node->m_state == SEL_NODE_NO_MORE_ROWS);

    /* No more rows to print */

    thr->run_node = que_node_get_parent(row_printf_node);

    return thr;
  }

  auto arg = sel_node->m_select_list;

  while (arg != nullptr) {
    dfield_print_also_hex(que_node_get_val(arg));

    log_info(" ::: ");

    arg = que_node_get_next(arg);
  }

  log_info("");

  /* Fetch next row to print */

  thr->run_node = sel_node;

  return thr;
}

void Prebuilt::prebuild_graph() noexcept{
  ut_ad(m_trx != nullptr);

  if (m_sel_graph == nullptr) {

    auto sel_node = sel_node_t::create(m_heap);
    auto thr = pars_complete_graph_for_exec(sel_node, m_trx, m_heap);

    m_sel_graph = static_cast<que_fork_t *>(que_node_get_parent(thr));
    m_sel_graph->state = QUE_FORK_ACTIVE;
  }
}

db_err Row_sel::get_clust_rec_with_prebuilt(
  Prebuilt *prebuilt,
  Index *sec_index,
  const rec_t *rec,
  que_thr_t *thr,
  const rec_t **out_rec,
  ulint **offsets,
  mem_heap_t **offset_heap,
  mtr_t *mtr) noexcept {

  *out_rec = nullptr;

  auto func_exit = [this](Prebuilt *prebuilt, const rec_t **out_rec, const rec_t *clust_rec, mtr_t *mtr) noexcept {
    *out_rec = clust_rec;

    if (prebuilt->m_select_lock_type != LOCK_NONE) {
      /* We may use the cursor in update or in unlock_row(): store its position */

      prebuilt->m_clust_pcur->store_position(mtr);
    }

    return DB_SUCCESS;
  };

  auto trx = thr_get_trx(thr);

  row_build_row_ref_in_tuple(prebuilt->m_clust_ref, rec, sec_index, *offsets, trx);

  auto clust_index = sec_index->m_table->get_clustered_index();

  prebuilt->m_clust_pcur->open_with_no_init(clust_index, prebuilt->m_clust_ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, 0, mtr, Current_location());

  const rec_t *clust_rec = prebuilt->m_clust_pcur->get_rec();

  prebuilt->m_clust_pcur->m_trx_if_known = trx;

  /* Note: only if the search ends up on a non-infimum record is the
  low_match value the real match to the search tuple */

  if (!page_rec_is_user_rec(clust_rec) || prebuilt->m_clust_pcur->get_low_match() < clust_index->get_n_unique()) {

    /* In a rare case it is possible that no clust rec is found
    for a delete-marked secondary index record: if in row0umod.c
    in row_undo_mod_remove_clust_low() we have already removed
    the clust rec, while purge is still cleaning and removing
    secondary index records associated with earlier versions of
    the clustered index record. In that case we know that the
    clustered index record did not exist in the read view of
    trx. */

    if (!rec_get_deleted_flag(rec) || prebuilt->m_select_lock_type != LOCK_NONE) {
      log_err("Clustered record for sec rec not found");
      m_dict->index_name_print(trx, sec_index);
      log_err("sec index record ");
      log_err(rec_to_string(rec));
      log_err("clust index record\nclust index record ");
      log_err(rec_to_string(clust_rec));
      log_err(trx->to_string(600));

      log_err("Submit a detailed bug report, check the Embedded InnoDB website for details");
    }

    clust_rec = nullptr;

    return func_exit(prebuilt, out_rec, clust_rec, mtr);
  }

  {
    Phy_rec record{clust_index, clust_rec};

    *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, offset_heap, Current_location());
  }

  if (prebuilt->m_select_lock_type != LOCK_NONE) {
    /* Try to place a lock on the index record; we are searching the clust rec with a unique condition, hence
    we set a LOCK_REC_NOT_GAP type lock */

    auto block = prebuilt->m_clust_pcur->get_block();
    const auto lock_type = prebuilt->m_select_lock_type;

    const auto err = m_lock_sys->clust_rec_read_check_and_lock(0, block, clust_rec, clust_index, *offsets, lock_type, LOCK_REC_NOT_GAP, thr);

    if (err != DB_SUCCESS) {

      return err;
    }

  } else {
    /* This is a non-locking consistent read: if necessary, fetch
    a previous version of the record */

    const rec_t *old_vers{};

    /* If the isolation level allows reading of uncommitted data,
    then we never look for an earlier version */

    if (trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && !m_lock_sys->clust_rec_cons_read_sees(clust_rec, clust_index, *offsets, trx->m_read_view)) {

      Row_vers::Row row {
        .m_cluster_rec = clust_rec,
        .m_mtr = mtr,
        .m_cluster_index = clust_index,
        .m_cluster_offsets = *offsets,
        .m_consistent_read_view = trx->m_read_view,
        .m_cluster_offset_heap = *offset_heap,
        .m_old_row_heap = prebuilt->m_old_vers_heap,
        .m_old_rec = old_vers
      };

      /* The following call returns 'offsets' associated with 'old_vers' */
      const auto err = build_prev_vers(row);

      if (err != DB_SUCCESS || old_vers == nullptr) {

        return err;
      }

      clust_rec = old_vers;
    }

    /* If we had to go to an earlier version of row or the secondary index record is delete marked, then it may be that
    the secondary index record corresponding to clust_rec (or old_vers) is not rec; in that case we must ignore
    such row because in our snapshot rec would not have existed.  Remember that from rec we cannot see directly which transaction
    id corresponds to it: we have to go to the clustered index record. A query where we want to fetch all rows where
    the secondary index value is in some interval would return a wrong result if we would not drop rows which we come to
    visit through secondary index records that would not really exist in our snapshot. */

    if (clust_rec != nullptr &&
        (old_vers != nullptr || trx->m_isolation_level <= TRX_ISO_READ_UNCOMMITTED || rec_get_deleted_flag(rec)) &&
        !sec_rec_is_for_clust_rec(rec, sec_index, clust_rec, clust_index)) {

      clust_rec = nullptr;
    }
  }

  return func_exit(prebuilt, out_rec, clust_rec, mtr);
}

bool Row_sel::restore_position(bool *same_user_rec, ulint latch_mode, Btree_pcursor *pcur, bool moves_up, mtr_t *mtr) noexcept {
  const auto relative_position = pcur->get_rel_pos();
  const auto success = pcur->restore_position(latch_mode, mtr, Current_location());

  *same_user_rec = success;

  if (relative_position == Btree_cursor_pos::ON) {
    if (success) {
      return false;
    }

    if (moves_up) {
      (void) pcur->move_to_next(mtr);
    }

  } else if (relative_position == Btree_cursor_pos::AFTER || relative_position == Btree_cursor_pos::AFTER_LAST_IN_TREE) {

    if (moves_up) {
      return true;
    }

    if (pcur->is_on_user_rec()) {
      (void) pcur->move_to_prev(mtr);
    }

  } else {

    ut_ad(relative_position == Btree_cursor_pos::BEFORE || relative_position == Btree_cursor_pos::BEFORE_FIRST_IN_TREE);

    if (moves_up && pcur->is_on_user_rec()) {
      (void) pcur->move_to_next(mtr);
    }
  }

  return true;
}

inline void Prebuilt::Row_cache::add_row(const rec_t *rec, const ulint *offsets) noexcept {
  ut_a(m_first == 0);
  ut_ad(!is_cache_full());
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  ut_a(m_n_cached < m_n_size - 1);
  m_cached_rows[m_n_cached++].add_row(rec, offsets);
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
 * @return Search_status::FOUND, Search_status::EXHAUSTED, Search_status::RETRY
 */
Search_status Row_sel::try_search_shortcut_for_prebuilt(const rec_t **out_rec, Prebuilt *prebuilt, ulint **offsets, mem_heap_t **heap, mtr_t *mtr) noexcept {
  auto trx = prebuilt->m_trx;
  auto pcur = prebuilt->m_pcur;
  auto index = prebuilt->m_index;
  auto search_tuple = prebuilt->m_search_tuple;

  ut_ad(index->is_clustered());

  pcur->open_with_no_init(index, search_tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, RW_S_LATCH, mtr, Current_location());

  auto rec = pcur->get_rec();

  if (!page_rec_is_user_rec(rec)) {

    return Search_status::RETRY;
  }

  /* As the cursor is now placed on a user record after a search with
  the mode PAGE_CUR_GE, the up_match field in the cursor tells how many
  fields in the user record matched to the search tuple */

  if (pcur->get_up_match() < dtuple_get_n_fields(search_tuple)) {

    return Search_status::EXHAUSTED;
  }

  /* This is a non-locking consistent read: if necessary, fetch
  a previous version of the record */

  {
    Phy_rec record{index, rec};

    *offsets = record.get_col_offsets(*offsets, ULINT_UNDEFINED, heap, Current_location());
  }

  if (!m_lock_sys->clust_rec_cons_read_sees(rec, index, *offsets, trx->m_read_view)) {

    return Search_status::RETRY;

  } else if (rec_get_deleted_flag(rec)) {

    return Search_status::EXHAUSTED;
  }

  *out_rec = rec;

  return Search_status::FOUND;
}

db_err Row_sel::unlock_for_client(Prebuilt *prebuilt, bool has_latches_on_recs) noexcept{
  auto trx = prebuilt->m_trx;

  if (unlikely(trx->m_isolation_level != TRX_ISO_READ_COMMITTED)) {

    log_err("row_unlock_for_client called though this session is not using READ COMMITTED isolation level.");

    return DB_SUCCESS;
  }

  trx->m_op_info = "unlock_row";

  if (prebuilt->m_new_rec_locks >= 1) {

    mtr_t mtr;
    auto pcur = prebuilt->m_pcur;

    mtr.start();

    /* Restore the cursor position and find the record */

    if (!has_latches_on_recs) {
      (void) pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }

    auto rec = pcur->get_rec();

    m_lock_sys->rec_unlock(trx, pcur->get_block(), rec, prebuilt->m_select_lock_type);

    mtr.commit();

    /* If the search was done through the clustered index, then
    we have not used clust_pcur at all, and we must NOT try to
    reset locks on clust_pcur. The values in clust_pcur may be
    garbage! */

    if (prebuilt->m_index->is_clustered()) {

      trx->m_op_info = "";

      return DB_SUCCESS;
    }
  }

  if (prebuilt->m_new_rec_locks >= 1) {
    mtr_t mtr;
    auto clust_pcur = prebuilt->m_clust_pcur;

    mtr.start();

    /* Restore the cursor position and find the record */

    if (!has_latches_on_recs) {
      (void) clust_pcur->restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }

    auto rec = clust_pcur->get_rec();

    m_lock_sys->rec_unlock(trx, clust_pcur->get_block(), rec, prebuilt->m_select_lock_type);

    mtr.commit();
  }

  trx->m_op_info = "";

  return DB_SUCCESS;
}

db_err Row_sel::mvcc_fetch(ib_recovery_t recovery, ib_srch_mode_t mode, Prebuilt *prebuilt, ib_match_t match_mode, ib_cur_op_t direction) noexcept {
  auto index = prebuilt->m_index;
  const auto search_tuple = prebuilt->m_search_tuple;
  auto pcur = prebuilt->m_pcur;
  auto trx = prebuilt->m_trx;

  Index *clust_index;
  que_thr_t *thr;
  const rec_t *rec;
  const rec_t *result_rec;
  const rec_t *clust_rec;
  db_err err = DB_SUCCESS;
  bool moves_up = false;
  bool unique_search{};
  bool mtr_has_extra_clust_latch{};
  bool unique_search_from_clust_index{};
  bool set_also_gap_locks = true;

  /* if the returned record was locked and we did a semi-consistent read (fetch the newest committed version), then this is set to true */
  ulint next_offs;
  bool same_user_rec;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  void *cmp_ctx = index->m_cmp_ctx;

  rec_offs_init(offsets_);

  prebuilt->m_result = -1;

  ut_ad(index && pcur && search_tuple);

  if (unlikely(prebuilt->m_table->m_ibd_file_missing)) {
    log_err(std::format(
      "The client is trying to use a table handle but the .ibd file for"
      " table {} does not exist. Have you deleted the .ibd file"
      " from the database directory under the datadir, or have you discarded the"
      " tablespace? Check the InnoDB website for details on how you can resolve"
      "the problem.",
      prebuilt->m_table->m_name
    ));

    return DB_ERROR;
  }

  if (unlikely(!prebuilt->m_index_usable)) {

    return DB_MISSING_HISTORY;
  }

  if (unlikely(prebuilt->m_magic_n != ROW_PREBUILT_ALLOCATED)) {
    log_fatal(std::format(
      "Trying to free a corrupt table handle. Magic n {}, table name {}",
      prebuilt->m_magic_n,
      prebuilt->m_table->m_name
    ));
  }

#if 0
	/* August 19, 2005 by Heikki: temporarily disable this error
	print until the cursor lock count is done correctly.
	See bugs #12263 and #12456!*/

	if (trx->m_n_tables_in_use == 0 && unlikely(prebuilt->m_select_lock_type == LOCK_NONE)) {
		/* Note that if the client uses an InnoDB temp table that it
		created inside LOCK TABLES, then n_client_tables_in_use can
		be zero; in that case select_lock_type is set to LOCK_X in
		::start_stmt. */

		log_err("Client is trying to perform a read but it has not locked  any tables.");
		log_err(trx->to_string(600));
	}
#endif

  /* Reset the new record lock info if session is using a
  READ COMMITED isolation level. Then we are able to remove
  the record locks set here on an individual row. */
  prebuilt->m_new_rec_locks = 0;

  /*-------------------------------------------------------------*/
  /* PHASE 1: Try to pop the row from the prefetch cache */

  if (unlikely(direction == ROW_SEL_MOVETO)) {
    trx->m_op_info = "starting index read";

    prebuilt->m_row_cache.clear();

    if (prebuilt->m_sel_graph == nullptr) {
      /* Build a dummy select query graph */
      prebuilt->prebuild_graph();
    }
  } else {
    trx->m_op_info = "fetching rows";

    /* Is this the first row being fetched by the cursor ? */
    if (prebuilt->m_row_cache.is_cache_empty()) {
      prebuilt->m_row_cache.m_direction = direction;
    }

    if (unlikely(direction != prebuilt->m_row_cache.m_direction)) {

      if (!prebuilt->m_row_cache.is_cache_empty()) {
        ut_error;
        /* TODO: scrollable cursor: restore cursor to the place of the latest returned row,
        or better: prevent caching for a scroll cursor! */
      }

      prebuilt->m_row_cache.clear();

    } else if (likely(!prebuilt->m_row_cache.is_cache_empty())) {
      err = DB_SUCCESS;

      ++srv_n_rows_read;

      goto func_exit;

    } else if (prebuilt->m_row_cache.is_cache_fetch_in_progress()) {

      /* The previous returned row was popped from the fetch cache, but the cache was not full at the time of the
      popping: no more rows can exist in the result set */

      err = DB_RECORD_NOT_FOUND;
      goto func_exit;
    }

    mode = pcur->m_search_mode;
  }

  /* In a search where at most one record in the index may match, we can use a LOCK_REC_NOT_GAP type record lock when locking a
  non-delete-marked matching record.

  Note that in a unique secondary index there may be different delete-marked versions of a record where only the primary key
  values differ: thus in a secondary index we must use next-key locks when locking delete-marked records. */

  if (match_mode == ROW_SEL_EXACT && index->is_unique() && dtuple_get_n_fields(search_tuple) == index->get_n_unique() &&
      (index->is_clustered() || !dtuple_contains_null(search_tuple))) {

    /* Note above that a UNIQUE secondary index can contain many rows with the same key value if one of the columns is the SQL
    null. A clustered index should never contain null PK columns because we demand that all the columns in primary key are
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

    if (trx->m_client_n_tables_locked == 0 && prebuilt->m_select_lock_type == LOCK_NONE &&
        trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && trx->m_read_view) {

      /* This is a SELECT query done as a consistent read, and the read view has already been allocated:
      let us try a search shortcut through the hash index.

      NOTE that we must also test that client_n_tables_locked == 0, because this might
      also be INSERT INTO ... SELECT ... or CREATE TABLE ... SELECT ... . Our algorithm is
      NOT prepared to inserts interleaved with the SELECT, and if we try that, we can deadlock
      on the adaptive hash index semaphore! */

      switch (try_search_shortcut_for_prebuilt(&rec, prebuilt, &offsets, &heap, &mtr)) {
        case Search_status::FOUND:
          /* When searching for an exact match we don't position the persistent cursor therefore we
         must read in the record found into the pre-fetch cache for the user to access. */
          if (match_mode == ROW_SEL_EXACT) {
            /* There can be no previous entries when doing a search with this match mode set. */
            ut_a(prebuilt->m_row_cache.is_cache_empty());

            prebuilt->m_row_cache.add_row(rec, offsets);
          }

          mtr.commit();

          ++srv_n_rows_read;

          prebuilt->m_result = 0;

          err = DB_SUCCESS;

          goto func_exit;

        case Search_status::EXHAUSTED:
          mtr.commit();

          err = DB_RECORD_NOT_FOUND;
          goto func_exit;

        case Search_status::RETRY:
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

  if (trx->m_isolation_level <= TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE && prebuilt->m_simple_select) {
    /* It is a plain locking SELECT and the isolation level is low: do not lock gaps */

    set_also_gap_locks = false;
  }

  /* Note that if the search mode was GE or G, then the cursor naturally moves upward (in fetch next) in alphabetical order,
  otherwise downward */

  if (unlikely(direction == ROW_SEL_MOVETO)) {
    if (mode == IB_CUR_GE || mode == IB_CUR_G) {
      moves_up = true;
    }
  } else if (direction == ROW_SEL_NEXT) {
    moves_up = true;
  }

  thr = que_fork_get_first_thr(prebuilt->m_sel_graph);

  que_thr_move_to_run_state_for_client(thr, trx);

  clust_index = index->get_clustered_index();

  if (likely(direction != ROW_SEL_MOVETO)) {

    if (!restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr)) {

      goto next_rec;
    }

  } else if (dtuple_get_n_fields(search_tuple) > 0) {

    pcur->open_with_no_init(index, search_tuple, mode, BTR_SEARCH_LEAF, 0, &mtr, Current_location());

    pcur->m_trx_if_known = trx;

    rec = pcur->get_rec();

    if (!moves_up && !page_rec_is_supremum(rec) && set_also_gap_locks && trx->m_isolation_level != TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {

      /* Try to place a gap lock on the next index record to prevent phantoms in ORDER BY ... DESC queries */
      const rec_t *next = page_rec_get_next_const(rec);

      {
        Phy_rec record{index, next};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      err = set_rec_lock(pcur->get_block(), next, index, offsets, prebuilt->m_select_lock_type, LOCK_GAP, thr);

      if (err != DB_SUCCESS) {

        goto lock_wait_or_error;
      }
    }
  } else if (mode == IB_CUR_G) {
    pcur->open_at_index_side(true, index, BTR_SEARCH_LEAF, false, 0, &mtr);
  } else if (mode == IB_CUR_L) {
    pcur->open_at_index_side(false, index, BTR_SEARCH_LEAF, false, 0, &mtr);
  }

  if (!prebuilt->m_sql_stat_start) {
    /* No need to set an intention lock or assign a read view */

    if (trx->m_read_view == nullptr && prebuilt->m_select_lock_type == LOCK_NONE) {

      log_err("The client is trying to perform a consistent read but the read view is not assigned!n");
      log_info(trx->to_string(600));
      ut_error;
    }
  } else if (prebuilt->m_select_lock_type == LOCK_NONE) {
    /* This is a consistent read,  assign a read view for the query */

    auto rv = trx->assign_read_view();
    ut_a(rv != nullptr);

    prebuilt->m_sql_stat_start = false;

  } else {
    auto lck_mode = prebuilt->m_select_lock_type == LOCK_S ? LOCK_IS : LOCK_IX;

    err = m_lock_sys->lock_table(0, index->m_table, lck_mode, thr);

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }
    prebuilt->m_sql_stat_start = false;
  }

rec_loop:
  /*-------------------------------------------------------------*/
  /* PHASE 4: Look for matching records in a loop */

  rec = pcur->get_rec();

  if (page_rec_is_infimum(rec)) {

    /* The infimum record on a page cannot be in the result set, and neither can a record lock be placed on it: we skip such
    a record. */

    goto next_rec;
  }

  if (page_rec_is_supremum(rec)) {

    if (set_also_gap_locks && trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {

      /* Try to place a lock on the index record */

      /* If this session is using a READ COMMITTED isolation level we do not lock gaps. Supremum record is really
      a gap and therefore we do not set locks there. */

      {
        Phy_rec record{index, rec};

        offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &heap, Current_location());
      }

      const auto lock_type = prebuilt->m_select_lock_type;

      err = set_rec_lock(pcur->get_block(), rec, index, offsets, lock_type, LOCK_ORDINARY, thr);

      if (err != DB_SUCCESS) {

        goto lock_wait_or_error;
      }
    }
    /* A page supremum record cannot be in the result set: skip it now that we have placed a possible lock on it */

    goto next_rec;
  }

  /*-------------------------------------------------------------*/
  /* Do sanity checks in case our cursor has bumped into page corruption */

  next_offs = rec_get_next_offs(rec);

  if (unlikely(next_offs < PAGE_SUPREMUM)) {

    goto wrong_offs;
  }

  if (unlikely(next_offs >= UNIV_PAGE_SIZE - PAGE_DIR)) {

  wrong_offs:
    if (recovery == IB_RECOVERY_DEFAULT || moves_up == false) {

      buf_page_print(page_align(rec), 0);
      log_err(std::format("\nrec address {}, buf block with fix count {}", (void *)rec, (int) pcur->get_block()->m_page.m_buf_fix_count));
      log_err(std::format("Index corruption: rec offs {} next offs {}, page no {},", page_offset(rec), next_offs, page_get_page_no(page_align(rec))));
      m_dict->index_name_print(trx, index);
      log_err(". Run CHECK TABLE. You may need to restore from a backup, or dump + drop + reimport the table.");

      err = DB_CORRUPTION;

      goto lock_wait_or_error;

    } else {
      /* The user may be dumping a corrupt table. Jump over the corruption to recover as much as possible. */

      log_err(std::format("Index corruption: rec offs {} next offs {}, page no {},", page_offset(rec), next_offs, page_get_page_no(page_align(rec))));
      m_dict->index_name_print(trx, index);
      log_err(". We try to skip the rest of the page.");

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
    auto btree = m_dict->m_store.m_btree;

    if (!rec_validate(rec, offsets) || !btree->index_rec_validate(rec, index, false)) {
      log_err(std::format("Index corruption: rec offs {} next offs {}, page no {} ", page_offset(rec), next_offs, page_get_page_no(page_align(rec))));
      m_dict->index_name_print(trx, index);
      log_err(". We try to skip the record.");

      goto next_rec;
    }
  }

  /* Note that we cannot trust the up_match value in the cursor at this place because we can arrive here after moving the cursor! Thus
  we have to recompare rec and search_tuple to determine if they match enough. */

  if (match_mode == ROW_SEL_EXACT) {
    int result;

    /* Test if the index record matches completely to search_tuple in prebuilt: if not, then we return with DB_RECORD_NOT_FOUND */

    result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);

    if (prebuilt->m_row_cache.is_cache_empty()) {
      prebuilt->m_result = result;
    }

    if (result != 0) {

      goto not_found;
    }

  } else if (match_mode == ROW_SEL_EXACT_PREFIX) {

    if (!cmp_dtuple_is_prefix_of_rec(cmp_ctx, search_tuple, rec, offsets)) {

      prebuilt->m_result = -1;

    not_found:
      if (set_also_gap_locks && trx->m_isolation_level != TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {

        /* Try to place a gap lock on the index record only if this session is not using a READ COMMITTED isolation level. */

        const auto lock_type = prebuilt->m_select_lock_type;
        err = set_rec_lock(pcur->get_block(), rec, index, offsets, lock_type, LOCK_GAP, thr);

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

  if (prebuilt->m_select_lock_type != LOCK_NONE) {
    int result = -1;

    /* Try to place a lock on the index record; note that delete marked records are a special case in a unique search. If there
    is a non-delete marked record, then it is enough to lock its existence with LOCK_REC_NOT_GAP. */

    /* If this session is using a READ COMMITED isolation level we lock only the record, i.e., next-key locking is not used. */

    ulint lock_type;

    if (!set_also_gap_locks || trx->m_isolation_level == TRX_ISO_READ_COMMITTED || (unique_search && !unlikely(rec_get_deleted_flag(rec)))) {

      goto no_gap_lock;
    } else {
      lock_type = LOCK_ORDINARY;
    }

    /* If we are doing a 'greater or equal than a primary key value' search from a clustered index, and we find a record
    that has that exact primary key value, then there is no need to lock the gap before the record, because no insert in the
    gap can be in our search range. That is, no phantom row can appear that way.

    An example: if col1 is the primary key, the search is WHERE col1 >= 100, and we find a record where col1 = 100, then no
    need to lock the gap before that record. */

    if (index == clust_index && mode == IB_CUR_GE &&
        direction == ROW_SEL_MOVETO &&
        dtuple_get_n_fields_cmp(search_tuple) == index->get_n_unique() &&
        !(result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets))) {
    no_gap_lock:
      lock_type = LOCK_REC_NOT_GAP;
    }

    if (prebuilt->m_row_cache.is_cache_empty()) {
      prebuilt->m_result = result;
    }

    {
      auto block = pcur->get_block();

      err = set_rec_lock(block, rec, index, offsets, prebuilt->m_select_lock_type, lock_type, thr);
    }

    if (err != DB_SUCCESS) {
      goto lock_wait_or_error;
    }
  } else {
    /* This is a non-locking consistent read: if necessary, fetch a previous version of the record */

    if (trx->m_isolation_level == TRX_ISO_READ_UNCOMMITTED) {

      /* Do nothing: we let a non-locking SELECT read the latest version of the record */

    } else if (index == clust_index) {

      /* Fetch a previous version of the row if the current one is not visible in the snapshot; if we have a very
      high force recovery level set, we try to avoid crashes by skipping this lookup */

      if (likely(recovery < IB_RECOVERY_NO_UNDO_LOG_SCAN) && !srv_lock_sys->clust_rec_cons_read_sees(rec, index, offsets, trx->m_read_view)) {

        const rec_t *old_vers;

        Row_vers::Row row {
          .m_cluster_rec = rec,
          .m_mtr = &mtr,
          .m_cluster_index = clust_index,
          .m_cluster_offsets = offsets,
          .m_consistent_read_view = trx->m_read_view,
          .m_cluster_offset_heap = heap,
          .m_old_row_heap = prebuilt->m_old_vers_heap,
          .m_old_rec = old_vers
        };

        /* The following call returns 'offsets' associated with 'old_vers' */
        err = build_prev_vers(row);

        if (err != DB_SUCCESS) {

          goto lock_wait_or_error;
        }

        if (old_vers == nullptr) {
          /* The row did not exist yet in the read view */

          goto next_rec;
        }

        rec = old_vers;
      }
    } else if (!m_lock_sys->sec_rec_cons_read_sees(rec, trx->m_read_view)) {
      /* We are looking into a non-clustered index, and to get the right version of the record we
      have to look also into the clustered index: this is necessary, because we can only get the undo
      information via the clustered index record. */

      ut_ad(index != clust_index);

      if (direction == ROW_SEL_MOVETO && prebuilt->m_row_cache.is_cache_empty()) {

        prebuilt->m_result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
      }

      goto requires_clust_rec;
    }
  }

  /* NOTE that at this point rec can be an old version of a clustered
  index record built for a consistent read. We cannot assume after this
  point that rec is on a buffer pool page. */

  if (unlikely(rec_get_deleted_flag(rec))) {

    /* The record is delete-marked: we can skip it */

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {

      /* No need to keep a lock on a delete-marked record if we do not want to use next-key locking. */

      (void) unlock_for_client(prebuilt, true);
    }

    /* This is an optimization to skip setting the next key lock on the record that follows this delete-marked record. This
    optimization works because of the unique search criteria which precludes the presence of a range lock between this
    delete marked record and the record following it.

    For now this is applicable only to clustered indexes while doing a unique search. There is scope for further optimization
    applicable to unique secondary indexes. Current behaviour is to widen the scope of a lock on an already delete marked record
    if the same record is deleted twice by the same transaction */
    if (index == clust_index && unique_search) {
      prebuilt->m_result = -1;
      err = DB_RECORD_NOT_FOUND;

      goto normal_return;
    }

    goto next_rec;
  } else if (direction == ROW_SEL_MOVETO && prebuilt->m_row_cache.is_cache_empty()) {

    prebuilt->m_result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
  }

  /* Get the clustered index record if needed, if we did not do the
  search using the clustered index. */

  if (index != clust_index && prebuilt->m_need_to_access_clustered) {

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

    err = get_clust_rec_with_prebuilt(prebuilt, index, rec, thr, &clust_rec, &offsets, &heap, &mtr);

    if (err != DB_SUCCESS) {

      goto lock_wait_or_error;
    }

    if (clust_rec == nullptr) {
      /* The record did not exist in the read view */
      ut_ad(prebuilt->m_select_lock_type == LOCK_NONE);

      goto next_rec;
    }

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {
      /* Note that both the secondary index record
      and the clustered index record were locked. */
      ut_ad(prebuilt->m_new_rec_locks == 1);
      prebuilt->m_new_rec_locks = 2;
    }

    if (unlikely(rec_get_deleted_flag(clust_rec))) {

      /* The record is delete marked: we can skip it */

      if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && prebuilt->m_select_lock_type != LOCK_NONE) {

        /* No need to keep a lock on a delete-marked
        record if we do not want to use next-key
        locking. */

        (void) unlock_for_client(prebuilt, true);
      }

      goto next_rec;
    }

    if (prebuilt->m_row_cache.is_cache_empty()) {
      prebuilt->m_result = cmp_dtuple_rec(cmp_ctx, search_tuple, clust_rec, offsets);
    }

    if (prebuilt->m_need_to_access_clustered) {

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

    if (prebuilt->m_row_cache.is_cache_empty()) {
      prebuilt->m_result = cmp_dtuple_rec(cmp_ctx, search_tuple, rec, offsets);
    }
  }

  /* We found a qualifying record 'result_rec'. At this point,
  'offsets' are associated with 'result_rec'. */

  ut_ad(rec_offs_validate(result_rec, result_rec != rec ? clust_index : index, offsets));

  /* At this point, the clustered index record is protected by a page latch that was acquired when
  pcur was positioned.  The latch will not be released until mtr_commit(&mtr). */

  if ((match_mode == ROW_SEL_EXACT || prebuilt->m_row_cache.m_n_cached >= prebuilt->m_row_cache.m_n_size - 1) &&
      prebuilt->m_select_lock_type == LOCK_NONE && !prebuilt->m_clust_index_was_generated) {

    /* Inside an update, for example, we do not cache rows, since we may use the cursor position
    to do the actual update, that is why we require ...lock_type == LOCK_NONE.

    FIXME: How do we handle scrollable cursors ? */

    prebuilt->m_row_cache.add_row(result_rec, offsets);

    /* An exact match means a unique lookup, no need to fill the cache with more records. */
    if (unique_search || prebuilt->m_row_cache.is_cache_full()) {

      goto got_row;
    }

    goto next_rec;
  }

  ut_a(!prebuilt->m_row_cache.is_cache_full());
  prebuilt->m_row_cache.add_row(result_rec, offsets);

  /* From this point on, 'offsets' are invalid. */
got_row:

  /* We have an optimization to save CPU time: if this is a consistent read on a unique condition on the
  clustered index, then we do not store the pcur position, because any fetch next or prev will anyway
  return 'end of file'. Exceptions are locking reads and where the user can move the cursor with
  PREV or NEXT even after a unique search. */

  if (!unique_search_from_clust_index || prebuilt->m_select_lock_type != LOCK_NONE) {

    /* Inside an update always store the cursor position */

    pcur->store_position(&mtr);
  }

  err = DB_SUCCESS;

  goto normal_return;

next_rec:
  prebuilt->m_new_rec_locks = 0;

  /*-------------------------------------------------------------*/
  /* PHASE 5: Move the cursor to the next index record */

  if (unlikely(mtr_has_extra_clust_latch)) {
    /* We must commit mtr if we are moving to the next non-clustered index record, because we could break the
    latching order if we would access a different clustered index page right away without releasing the previous. */

    pcur->store_position(&mtr);

    mtr.commit();
    mtr_has_extra_clust_latch = false;

    mtr.start();

    if (restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr)) {

      goto rec_loop;
    }
  }

  if (moves_up) {
    if (unlikely(!pcur->move_to_next(&mtr))) {
    not_moved:
      pcur->store_position(&mtr);

      err = match_mode != ROW_SEL_DEFAULT ? DB_RECORD_NOT_FOUND : DB_END_OF_INDEX;

      prebuilt->m_result = -1;
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

    (void) restore_position(&same_user_rec, BTR_SEARCH_LEAF, pcur, moves_up, &mtr);

    if (trx->m_isolation_level == TRX_ISO_READ_COMMITTED && !same_user_rec) {

      /* Since we were not able to restore the cursor on the same user record, we cannot use
      row_unlock_for_client() to unlock any records, and we must thus reset the new rec lock info. Since
      in lock0lock.c we have blocked the inheriting of gap X-locks, we actually do not have any new record locks
      set in this case.

      Note that if we were able to restore on the 'same' user record, it is still possible that we were actually
      waiting on a delete-marked record, and meanwhile it was removed by purge and inserted again by some
      other user. But that is no problem, because in rec_loop we will again try to set a lock, and
      new_rec_lock_info in trx will be right at the end. */

      prebuilt->m_new_rec_locks = 0;
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

  if (prebuilt->m_row_cache.m_n_cached > 0) {
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
