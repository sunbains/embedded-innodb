/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file row/row0vers.c
Row versions

Created 2/6/1997 Heikki Tuuri
*******************************************************/

#include "row0vers.h"
#include "lock0lock.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0trx.h"
#include "trx0undo.h"

Row_vers *srv_row_vers;

Row_vers::Row_vers(Trx_sys *trx_sys, Lock_sys *lock_sys) noexcept : m_trx_sys{trx_sys}, m_lock_sys{lock_sys} {}

Row_vers *Row_vers::create(Trx_sys *trx_sys, Lock_sys *lock_sys) noexcept {
  auto ptr = ut_new(sizeof(Row_vers));
  return new (ptr) Row_vers{trx_sys, lock_sys};
}

void Row_vers::destroy(Row_vers *&row_vers) noexcept {
  call_destructor(row_vers);
  ut_delete(row_vers);
  row_vers = nullptr;
}

Trx *Row_vers::impl_x_locked_off_kernel(const rec_t *rec, const Index *index, const ulint *offsets) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(&purge_sys->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  mutex_exit(&kernel_mutex);

  auto func_exit = [](mtr_t &mtr, mem_heap_t *heap, Trx *trx) -> auto {
    mtr.commit();
    mem_heap_free(heap);

    return trx;
  };

  mtr_t mtr;

  mtr.start();

  /* Search for the clustered index record: this is a time-consuming operation: therefore we release the kernel mutex;
  also, the release is required by the latching order convention. The latch on the clustered index locks the top of
  the stack of versions. We also reserve purge_latch to lock the bottom of the version stack. */

  Index *clust_index;

  auto clust_rec = row_get_clust_rec(BTR_SEARCH_LEAF, rec, index, &clust_index, &mtr);

  if (clust_rec == nullptr) {
    /* In a rare case it is possible that no clust rec is found for a secondary index record: if in row0undo.cc
    row_undo_mod_remove_clust_low() we have already removed the clust rec, while purge is still cleaning and
    removing secondary index records associated with earlier versions of the clustered index record. In that
    case there cannot be any implicit lock on the secondary index record, because an active transaction which
    has modified the secondary index record has also modified the clustered index record. And in a rollback
    we always undo the modifications to secondary index records before the clustered index record. */

    mutex_enter(&kernel_mutex);

    mtr.commit();

    return nullptr;
  }

  ulint *clust_offsets;
  auto heap = mem_heap_create(1024);

  {
    Phy_rec record{clust_index, clust_rec};

    clust_offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
  }

  auto trx_id = row_get_rec_trx_id(clust_rec, clust_index, clust_offsets);

  mtr_s_lock(&m_trx_sys->m_purge->m_latch, &mtr);

  mutex_enter(&kernel_mutex);

  Trx *trx{};

  if (!m_trx_sys->is_active(trx_id)) {
    /* The transaction that modified or inserted clust_rec is no
    longer active: no implicit lock on rec */
    return func_exit(mtr, heap, nullptr);
  }

  if (!m_lock_sys->check_trx_id_sanity(trx_id, clust_rec, clust_index, clust_offsets, true)) {
    /* Corruption noticed: try to avoid a crash by returning */
    return func_exit(mtr, heap, nullptr);
  }

  ut_ad(index->m_table == clust_index->m_table);

  /* We look up if some earlier version, which was modified by the trx_id
  transaction, of the clustered index record would require rec to be in
  a different state (delete marked or unmarked, or have different field
  values, or not existing). If there is such a version, then rec was
  modified by the trx_id transaction, and it has an implicit x-lock on
  rec. Note that if clust_rec itself would require rec to be in a
  different state, then the trx_id transaction has not yet had time to
  modify rec, and does not necessarily have an implicit x-lock on rec. */

  trx = nullptr;

  auto version = clust_rec;
  auto rec_del = rec_get_deleted_flag(rec);

  for (;;) {
    row_ext_t *ext;
    rec_t *prev_version;

    mutex_exit(&kernel_mutex);

    /* While we retrieve an earlier version of clust_rec, we
    release the kernel mutex, because it may take time to access
    the disk. After the release, we have to check if the trx_id
    transaction is still active. We keep the semaphore in mtr on
    the clust_rec page, so that no other transaction can update
    it and get an implicit x-lock on rec. */

    auto heap2 = heap;

    heap = mem_heap_create(1024);

    auto err = trx_undo_prev_version_build(clust_rec, &mtr, version, clust_index, clust_offsets, heap, &prev_version);

    mem_heap_free(heap2); /* free version and clust_offsets */

    if (prev_version == nullptr) {
      mutex_enter(&kernel_mutex);

      if (!m_trx_sys->is_active(trx_id)) {
        /* Transaction no longer active: no implicit x-lock */

        break;
      }

      /* If the transaction is still active, clust_rec must be a fresh insert, because no
      previous version was found. */
      ut_a(err == DB_SUCCESS);

      /* It was a freshly inserted version: there is an implicit x-lock on rec */

      trx = m_trx_sys->get_on_id(trx_id);

      break;
    }

    ulint *clust_offsets;

    {
      Phy_rec record{clust_index, prev_version};

      clust_offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
    }

    auto vers_del = rec_get_deleted_flag(prev_version);
    auto prev_trx_id = row_get_rec_trx_id(prev_version, clust_index, clust_offsets);

    /* If the trx_id and prev_trx_id are different and if the prev_version is marked deleted then the
    prev_trx_id must have already committed for the trx_id to be able to modify the row. Therefore, prev_trx_id
    cannot hold any implicit lock. */
    if (vers_del != 0 && trx_id != prev_trx_id) {

      mutex_enter(&kernel_mutex);

      break;
    }

    /* The stack of versions is locked by mtr. Thus, it is safe to fetch the prefixes for
    externally stored columns. */
    auto row = row_build(ROW_COPY_POINTERS, clust_index, prev_version, clust_offsets, nullptr, &ext, heap);

    auto entry = row_build_index_entry(row, ext, index, heap);

    /* entry may be NULL if a record was inserted in place of a deleted record, and the BLOB pointers of the new
    record were not initialized yet.  But in that case, prev_version should be NULL. */
    ut_a(entry != nullptr);

    mutex_enter(&kernel_mutex);

    if (!m_trx_sys->is_active(trx_id)) {
      /* Transaction no longer active: no implicit x-lock */

      break;
    }

    /* If we get here, we know that the trx_id transaction is still active and it has modified
    prev_version. Let us check still active and it has modified prev_version. Let us check if
    prev_version would require rec to be in a different state. */

    /* The previous version of clust_rec must be accessible, because the transaction is still active
    and clust_rec was not a fresh insert. */
    ut_ad(err == DB_SUCCESS);

    /* We check if entry and rec are identified in the alphabetical ordering */
    if (cmp_dtuple_rec(index->m_cmp_ctx, entry, rec, offsets) == 0) {

      /* The delete marks of rec and prev_version should be equal for rec to be in the state
      required by prev_version */

      if (rec_del != vers_del) {
        trx = m_trx_sys->get_on_id(trx_id);

        break;
      }

      /* It is possible that the row was updated so that the secondary index record remained
      the same in alphabetical ordering, but the field values changed still. For example,
      'abc' -> 'ABC'. Check also that. */

      dtuple_set_types_binary(entry, dtuple_get_n_fields(entry));

      if (cmp_dtuple_rec(index->m_cmp_ctx, entry, rec, offsets) != 0) {

        trx = m_trx_sys->get_on_id(trx_id);

        break;
      }

    } else if (rec_del == 0) {

      /* The delete mark should be set in rec for it to be in the state required by prev_version */

      trx = m_trx_sys->get_on_id(trx_id);

      break;
    }

    if (trx_id != prev_trx_id) {
      /* The versions modified by the trx_id transaction end to prev_version: no implicit x-lock */

      break;
    }

    version = prev_version;
  }

  return func_exit(mtr, heap, trx);
}

bool Row_vers::must_preserve_del_marked(trx_id_t trx_id, mtr_t *mtr) noexcept {

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(&m_trx_sys->m_purge->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  mtr_s_lock(&m_trx_sys->m_purge->m_latch, mtr);

  /* A purge operation is not yet allowed to remove this delete marked record  (if true) */
  return m_trx_sys->m_purge->update_undo_must_exist(trx_id);
}

bool Row_vers::old_has_index_entry(bool also_curr, const rec_t *rec, mtr_t *mtr, Index *index, const DTuple *ientry) noexcept {
  //const rec_t *version;
  //mem_heap_t *heap2;
  //const DTuple *row;
  //const DTuple *entry;
  //db_err err;

  ut_ad(mtr->memo_contains_page(rec, MTR_MEMO_PAGE_X_FIX) || mtr->memo_contains_page(rec, MTR_MEMO_PAGE_S_FIX));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(&m_trx_sys->m_purge->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  mtr_s_lock(&m_trx_sys->m_purge->m_latch, mtr);

  auto heap = mem_heap_create(1024);
  auto clust_index = index->m_table->get_first_index();

  ulint *clust_offsets;

  {
    Phy_rec record{clust_index, rec};

    clust_offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
  }

  if (also_curr && !rec_get_deleted_flag(rec)) {
    row_ext_t *ext;

    /* The stack of versions is locked by mtr.  Thus, it is safe to fetch the prefixes for
    externally stored columns. */
    auto row = row_build(ROW_COPY_POINTERS, clust_index, rec, clust_offsets, nullptr, &ext, heap);

    auto entry = row_build_index_entry(row, ext, index, heap);

    /* If entry == NULL, the record contains unset BLOB pointers.  This must be a freshly inserted record.  If
    this is called from row_purge_remove_sec_if_poss_low(), the thread will hold latches on the clustered index
    and the secondary index.  Because the insert works in three steps:

      (1) insert the record to clustered index
      (2) store the BLOBs and update BLOB pointers
      (3) insert records to secondary indexes

    the purge thread can safely ignore freshly inserted records and delete the secondary index record.  The
    thread that inserted the new record will be inserting the secondary index records. */

    /* NOTE that we cannot do the comparison as binary fields because the row is maybe being modified so that
    the clustered index record has already been updated to a different binary value in a char field, but the
    collation identifies the old and new value anyway! */

    if (entry != nullptr && dtuple_coll_cmp(index->m_cmp_ctx, ientry, entry) == 0) {

      mem_heap_free(heap);

      return true;
    }
  }

  auto version = rec;

  for (;;) {
    auto heap2 = heap;
    rec_t *prev_version;

    heap = mem_heap_create(1024);

    auto err = trx_undo_prev_version_build(rec, mtr, version, clust_index, clust_offsets, heap, &prev_version);

    mem_heap_free(heap2); /* free version and clust_offsets */

    if (err != DB_SUCCESS || prev_version == nullptr) {
      /* Versions end here */

      mem_heap_free(heap);

      return false;
    }

    {
      Phy_rec record{clust_index, prev_version};

      clust_offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());
    }

    if (rec_get_deleted_flag(prev_version) == 0) {
      row_ext_t *ext;

      /* The stack of versions is locked by mtr.  Thus, it is safe to fetch the prefixes for
      externally stored columns. */
      auto row = row_build(ROW_COPY_POINTERS, clust_index, prev_version, clust_offsets, nullptr, &ext, heap);
      auto entry = row_build_index_entry(row, ext, index, heap);

      /* If entry == NULL, the record contains unset BLOB pointers.  This must be a freshly
      inserted record that we can safely ignore.  For the justification, see the comments after
      the previous row_build_index_entry() call. */

      /* NOTE that we cannot do the comparison as binary fields because maybe the secondary index record has
      already been updated to a different binary value in a char field, but the collation identifies the old
      and new value anyway! */

      if (entry != nullptr && dtuple_coll_cmp(index->m_cmp_ctx, ientry, entry) == 0) {

        mem_heap_free(heap);

        return true;
      }
    }

    version = prev_version;
  }
}

db_err Row_vers::build_for_consistent_read(Row &row) noexcept {
  ut_ad(row.m_cluster_index->is_clustered());
  ut_ad(
    row.m_mtr->memo_contains_page(row.m_cluster_rec, MTR_MEMO_PAGE_X_FIX) ||
    row.m_mtr->memo_contains_page(row.m_cluster_rec, MTR_MEMO_PAGE_S_FIX)
  );

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(&m_trx_sys->m_purge->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  ut_ad(rec_offs_validate(row.m_cluster_rec, row.m_cluster_index, row.m_cluster_offsets));

  auto trx_id = row_get_rec_trx_id(row.m_cluster_rec, row.m_cluster_index, row.m_cluster_offsets);

  ut_ad(!read_view_sees_trx_id(row.m_consistent_read_view, trx_id));

  rw_lock_s_lock(&m_trx_sys->m_purge->m_latch);

  auto version = row.m_cluster_rec;
  mem_heap_t *heap{};
  db_err err{DB_SUCCESS};

  for (;;) {
    mem_heap_t *clust_heap = heap;

    heap = mem_heap_create(1024);

    /* If we have high-granularity consistent read view and creating transaction of the view is the same as trx_id in
    the record we see this record only in the case when undo_no of the record is < undo_no in the view. */

    if (row.m_consistent_read_view->type == VIEW_HIGH_GRANULARITY && row.m_consistent_read_view->creator_trx_id == trx_id) {

      auto roll_ptr = row_get_rec_roll_ptr(version, row.m_cluster_index, row.m_cluster_offsets);
      auto undo_rec = trx_undo_get_undo_rec_low(roll_ptr, heap);
      auto undo_no = trx_undo_rec_get_undo_no(undo_rec);

      mem_heap_empty(heap);

      if (row.m_consistent_read_view->undo_no > undo_no) {
        /* The view already sees this version: we can copy it to in_heap and return */

        auto buf = mem_heap_alloc(row.m_old_row_heap, rec_offs_size(row.m_cluster_offsets));

        row.m_old_rec = rec_copy(buf, version, row.m_cluster_offsets);

        ut_d(rec_offs_make_valid(row.m_old_rec, row.m_cluster_index, row.m_cluster_offsets));

        err = DB_SUCCESS;

        break;
      }
    }

    rec_t *prev_version;

    err = trx_undo_prev_version_build(
      row.m_cluster_rec, row.m_mtr, version, row.m_cluster_index, row.m_cluster_offsets, heap, &prev_version
    );

    if (clust_heap != nullptr) {
      mem_heap_free(clust_heap);
    }

    if (err != DB_SUCCESS) {
      break;
    }

    if (prev_version == nullptr) {
      /* It was a freshly inserted version */
      row.m_old_rec = nullptr;
      err = DB_SUCCESS;
      break;
    }

    {
      Phy_rec record{row.m_cluster_index, prev_version};

      row.m_cluster_offsets =
        record.get_col_offsets(row.m_cluster_offsets, ULINT_UNDEFINED, &row.m_cluster_offset_heap, Current_location());
    }

    trx_id = row_get_rec_trx_id(prev_version, row.m_cluster_index, row.m_cluster_offsets);

    if (read_view_sees_trx_id(row.m_consistent_read_view, trx_id)) {

      /* The view already sees this version: we can copy it to in_heap and return */

      auto buf = mem_heap_alloc(row.m_old_row_heap, rec_offs_size(row.m_cluster_offsets));

      row.m_old_rec = rec_copy(buf, prev_version, row.m_cluster_offsets);

      ut_d(rec_offs_make_valid(row.m_old_rec, row.m_cluster_index, row.m_cluster_offsets));

      err = DB_SUCCESS;

      break;
    }

    version = prev_version;
  }

  mem_heap_free(heap);
  rw_lock_s_unlock(&m_trx_sys->m_purge->m_latch);

  return err;
}

db_err Row_vers::build_for_semi_consistent_read(Row &row) noexcept {
  ut_ad(row.m_cluster_index->is_clustered());
  ut_ad(
    row.m_mtr->memo_contains_page(row.m_cluster_rec, MTR_MEMO_PAGE_X_FIX) ||
    row.m_mtr->memo_contains_page(row.m_cluster_rec, MTR_MEMO_PAGE_S_FIX)
  );

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(&m_trx_sys->m_purge->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  ut_ad(rec_offs_validate(row.m_cluster_rec, row.m_cluster_index, row.m_cluster_offsets));

  rw_lock_s_lock(&m_trx_sys->m_purge->m_latch);

  /* The S-latch on purge_sys prevents the purge view from changing.  Thus, if we have an uncommitted transaction at
  this point, then purge cannot remove its undo log even if the transaction could commit now. */

  mem_heap_t *heap{};
  db_err err{DB_SUCCESS};
  rec_t *prev_version{};
  auto version = row.m_cluster_rec;

  for (;;) {
    trx_id_t rec_trx_id{};
    auto version_trx_id = row_get_rec_trx_id(version, row.m_cluster_index, row.m_cluster_offsets);

    if (row.m_cluster_rec == version) {
      rec_trx_id = version_trx_id;
    }

    mutex_enter(&kernel_mutex);

    auto version_trx = m_trx_sys->get_on_id(version_trx_id);

    mutex_exit(&kernel_mutex);

    if (version_trx == nullptr || version_trx->m_conc_state == TRX_NOT_STARTED ||
        version_trx->m_conc_state == TRX_COMMITTED_IN_MEMORY) {

      /* We found a version that belongs to a committed transaction: return it. */

      if (row.m_cluster_rec == version) {
        row.m_old_rec = row.m_cluster_rec;
        err = DB_SUCCESS;
        break;
      }

      /* We assume that a rolled-back transaction stays in TRX_ACTIVE state until all the changes have been
      rolled back and the transaction is removed from the global list of transactions. */

      if (rec_trx_id == version_trx_id) {
        /* The transaction was committed while we searched for earlier versions.  Return the current version
        as a semi-consistent read. */

        version = row.m_cluster_rec;

        {
          Phy_rec record{row.m_cluster_index, version};

          row.m_cluster_offsets =
            record.get_col_offsets(row.m_cluster_offsets, ULINT_UNDEFINED, &row.m_cluster_offset_heap, Current_location());
        }
      }

      auto buf = mem_heap_alloc(row.m_old_row_heap, rec_offs_size(row.m_cluster_offsets));

      row.m_old_rec = rec_copy(buf, version, row.m_cluster_offsets);

      ut_d(rec_offs_make_valid(row.m_old_rec, row.m_cluster_index, row.m_cluster_offsets));

      err = DB_SUCCESS;

      break;
    }

    auto cluster_heap = heap;

    heap = mem_heap_create(1024);

    err = trx_undo_prev_version_build(
      row.m_cluster_rec, row.m_mtr, version, row.m_cluster_index, row.m_cluster_offsets, heap, &prev_version
    );

    if (cluster_heap != nullptr) {
      mem_heap_free(cluster_heap);
    }

    if (unlikely(err != DB_SUCCESS)) {
      break;
    }

    if (prev_version == nullptr) {
      /* It was a freshly inserted version */
      row.m_old_rec = nullptr;

      err = DB_SUCCESS;

      break;
    }

    version = prev_version;

    {
      Phy_rec record{row.m_cluster_index, version};

      row.m_cluster_offsets =
        record.get_col_offsets(row.m_cluster_offsets, ULINT_UNDEFINED, &row.m_cluster_offset_heap, Current_location());
    }
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  rw_lock_s_unlock(&m_trx_sys->m_purge->m_latch);

  return err;
}
