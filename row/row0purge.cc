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

/** @file row/row0purge.c
Purge obsolete records

Created 3/14/1997 Heikki Tuuri
*******************************************************/

#include "row0purge.h"

#include "btr0blob.h"
#include "fsp0fsp.h"
#include "log0log.h"
#include "mach0data.h"
#include "que0que.h"
#include "row0row.h"
#include "row0upd.h"
#include "row0vers.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"

Row_purge *Row_purge::node_create(que_thr_t *parent, mem_heap_t *heap) {
  ut_ad(parent != nullptr);

  auto ptr = mem_heap_alloc(heap, sizeof(Row_purge));
  auto node = new (ptr) Row_purge();

  node->m_common.type = QUE_NODE_PURGE;
  node->m_common.parent = parent;

  node->m_heap = mem_heap_create(256);

  return node;
}

Row_purge *row_purge_node_create(que_thr_t *parent, mem_heap_t *heap) {
  return static_cast<Row_purge *>(Row_purge::node_create(parent, heap));
}

bool Row_purge::reposition_pcur(ulint latch_mode, mtr_t *mtr) {
  if (m_found_clust) {
    return m_pcur.restore_position(latch_mode, mtr, Current_location());
  } else {
    m_found_clust = row_search_on_row_ref(&m_pcur, latch_mode, m_table, m_ref, mtr);

    if (m_found_clust) {
      m_pcur.store_position(mtr);
    }

    return m_found_clust;
  }
}

bool Row_purge::remove_clust_if_poss_low(ulint mode) {
  mtr_t mtr;
  db_err err;
  mem_heap_t *heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};

  rec_offs_init(offsets_);

  auto btr_cur = m_pcur.get_btr_cur();
  auto index = m_table->get_first_index();

  mtr.start();

  auto success = reposition_pcur(mode, &mtr);

  if (!success) {
    /* The record is already removed */

    m_pcur.commit_specify_mtr(&mtr);

    return (true);
  }

  auto rec = m_pcur.get_rec();
  auto offsets = offsets_.data();

  {
    Phy_rec record{index, rec};

    offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
  }

  if (m_roll_ptr != row_get_rec_roll_ptr(rec, index, offsets)) {

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
    /* Someone else has modified the record later: do not remove */
    m_pcur.commit_specify_mtr(&mtr);

    return (true);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (mode == BTR_MODIFY_LEAF) {
    success = btr_cur->optimistic_delete(&mtr);
  } else {
    ut_ad(mode == BTR_MODIFY_TREE);
    err = btr_cur->pessimistic_delete(false, RB_NONE, &mtr);

    if (err == DB_SUCCESS) {
      success = true;
    } else if (err == DB_OUT_OF_FILE_SPACE) {
      success = false;
    } else {
      ut_error;
    }
  }

  m_pcur.commit_specify_mtr(&mtr);

  return (success);
}

void Row_purge::remove_clust_if_poss() {
  ulint n_tries{};

  /*	ib_logger(ib_stream, "Purge: Removing clustered record\n"); */

  auto success = remove_clust_if_poss_low(BTR_MODIFY_LEAF);

  if (success) {

    return;
  }
retry:
  success = remove_clust_if_poss_low(BTR_MODIFY_TREE);
  /* The delete operation may fail if we have little
  file space left: TODO: easiest to crash the database
  and restart with more file space */

  if (!success && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES) {
    n_tries++;

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

    goto retry;
  }

  ut_a(success);
}

bool Row_purge::remove_sec_if_poss_low(Index *index, const DTuple *entry, ulint mode) {
  Btree_pcursor pcur(srv_fsp, srv_btree_sys);
  Btree_cursor *btr_cur;
  bool success;
  bool old_has{};
  db_err err;
  mtr_t mtr;
  mtr_t mtr_vers;

  log_sys->free_check();

  mtr.start();

  auto found = row_search_index_entry(index, entry, mode, &pcur, &mtr);

  if (!found) {
    /* Not found.  This is a legitimate condition.  In a
    rollback, InnoDB will remove secondary recs that would
    be purged anyway.  Then the actual purge will not find
    the secondary index record.  Also, the purge itself is
    eager: if it comes to consider a secondary index
    record, and notices it does not need to exist in the
    index, it will remove it.  Then if/when the purge
    comes to consider the secondary index record a second
    time, it will not exist any more in the index. */

    /* ib_logger(ib_stream,
             "PURGE:........sec entry not found\n"); */
    /* dtuple_print(ib_stream, entry); */

    pcur.close();

    mtr.commit();

    return (true);
  }

  btr_cur = pcur.get_btr_cur();

  /* We should remove the index record if no later version of the row,
  which cannot be purged yet, requires its existence. If some requires,
  we should do nothing. */

  mtr_vers.start();

  success = reposition_pcur(BTR_SEARCH_LEAF, &mtr_vers);

  if (success) {
    old_has = srv_row_vers->old_has_index_entry(true, pcur.get_rec(), &mtr_vers, index, entry);
  }

  pcur.commit_specify_mtr(&mtr_vers);

  if (!success || !old_has) {
    /* Remove the index record */

    if (mode == BTR_MODIFY_LEAF) {
      success = btr_cur->optimistic_delete(&mtr);
    } else {
      ut_ad(mode == BTR_MODIFY_TREE);
      err = btr_cur->pessimistic_delete(false, RB_NONE, &mtr);
      success = err == DB_SUCCESS;
      ut_a(success || err == DB_OUT_OF_FILE_SPACE);
    }
  }

  pcur.close();
  mtr.commit();

  return (success);
}

void Row_purge::remove_sec_if_poss(Index *index, DTuple *entry) {
  ulint n_tries{};

  /* ib_logger(ib_stream, "Purge: Removing secondary record\n"); */

  auto success = remove_sec_if_poss_low(index, entry, BTR_MODIFY_LEAF);

  if (success) {

    return;
  }

retry:
  success = remove_sec_if_poss_low(index, entry, BTR_MODIFY_TREE);
  /* The delete operation may fail if we have little
  file space left: TODO: easiest to crash the database
  and restart with more file space */

  if (!success && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES) {

    n_tries++;

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

    goto retry;
  }

  ut_a(success);
}

void Row_purge::del_mark() {
  auto heap = mem_heap_create(1024);

  for (; m_index != nullptr; m_index = m_index->get_next()) {
    auto idx = m_index;

    /* Build the index entry */
    auto entry = row_build_index_entry(m_row, nullptr, idx, heap);
    ut_a(entry != nullptr);

    remove_sec_if_poss(idx, entry);
  }

  mem_heap_free(heap);

  remove_clust_if_poss();
}

void Row_purge::upd_exist_or_extern() {
  Index *idx;
  bool is_insert;
  ulint rseg_id;
  page_no_t page_no;
  ulint offset;
  mtr_t mtr;

  Blob blob(srv_fsp, srv_btree_sys);
  auto heap = mem_heap_create(1024);

  if (m_rec_type == TRX_UNDO_UPD_DEL_REC) {

    goto skip_secondaries;
  }

  for (; m_index != nullptr; m_index = m_index->get_next()) {
    auto idx = m_index;

    if (srv_row_upd->changes_ord_field_binary(nullptr, m_index, m_update)) {
      /* Build the older version of the index entry */
      auto entry = row_build_index_entry(m_row, nullptr, idx, heap);
      ut_a(entry);

      remove_sec_if_poss(idx, entry);
    }
  }

  mem_heap_free(heap);

skip_secondaries:
  /* Free possible externally stored fields */
  for (ulint i = 0; i < Row_update::upd_get_n_fields(m_update); i++) {

    auto ufield = m_update->get_nth_field(i);

    if (dfield_is_ext(&ufield->m_new_val)) {
      Buf_block *block;
      ulint internal_offset;
      byte *data_field;

      /* We use the fact that new_val points to
      node->undo_rec and get thus the offset of
      dfield data inside the undo record. Then we
      can calculate from node->roll_ptr the file
      address of the new_val data */

      internal_offset = ((const byte *)dfield_get_data(&ufield->m_new_val)) - m_undo_rec;

      ut_a(internal_offset < UNIV_PAGE_SIZE);

      trx_undo_decode_roll_ptr(m_roll_ptr, &is_insert, &rseg_id, &page_no, &offset);
      mtr.start();

      idx = m_table->get_first_index();

      /* We have to acquire an X-latch to the clustered
      index tree */

      mtr_x_lock(idx->get_lock(), &mtr);

      /* NOTE: we must also acquire an X-latch to the
      root page of the tree. We will need it when we
      free pages from the tree. If the tree is of height 1,
      the tree X-latch does NOT protect the root page,
      because it is also a leaf page. Since we will have a
      latch on an undo log page, we would break the
      latching order if we would only later latch the
      root page of such a tree! */

      (void)srv_btree_sys->root_get(idx->m_page_id, &mtr);

      /* We assume in purge of externally stored fields
      that the space id of the undo log record is 0! */

      Buf_pool::Request req{
        .m_rw_latch = RW_X_LATCH,
        .m_page_id = {SYS_TABLESPACE, page_no},
        .m_mode = BUF_GET,
        .m_file = __FILE__,
        .m_line = __LINE__,
        .m_mtr = &mtr
      };

      block = srv_buf_pool->get(req, nullptr);
      buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_UNDO_PAGE));

      data_field = block->get_frame() + offset + internal_offset;

      ut_a(dfield_get_len(&ufield->m_new_val) >= BTR_EXTERN_FIELD_REF_SIZE);
      blob.free_externally_stored_field(
        idx, data_field + dfield_get_len(&ufield->m_new_val) - BTR_EXTERN_FIELD_REF_SIZE, nullptr, nullptr, 0, RB_NONE, &mtr
      );
      mtr.commit();
    }
  }
}

bool Row_purge::parse_undo_rec(bool *updated_extern, que_thr_t *thr) {
  Undo_rec_pars pars;
  auto trx = thr_get_trx(thr);

  auto ptr = trx_undo_rec_get_pars(m_undo_rec, pars);

  m_rec_type = pars.m_type;

  if (pars.m_type == TRX_UNDO_UPD_DEL_REC && !pars.m_extern) {

    return false;
  }

  *updated_extern = pars.m_extern;

  trx_id_t trx_id;
  ulint info_bits;
  roll_ptr_t roll_ptr;

  ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);

  m_table = nullptr;

  if (pars.m_type == TRX_UNDO_UPD_EXIST_REC && (pars.m_cmpl_info & UPD_NODE_NO_ORD_CHANGE) && !pars.m_extern) {

    /* Purge requires no changes to indexes: we may return */

    return false;
  }

  /* Prevent DROP TABLE etc. from running when we are doing the purge
  for this row */

  srv_dict_sys->freeze_data_dictionary(trx);

  srv_dict_sys->mutex_acquire();

  // FIXME: srv_force_recovery should be passed in as an arg
  m_table = srv_dict_sys->table_get_on_id(pars.m_table_id);

  srv_dict_sys->mutex_release();

  if (m_table == nullptr || m_table->m_ibd_file_missing) {
    /* The table has been dropped or the .ibd file is missing: no need to do purge */

    srv_dict_sys->unfreeze_data_dictionary(trx);

    return false;
  }

  auto clust_index = m_table->get_clustered_index();

  if (clust_index == nullptr) {
    /* The table was corrupt in the data dictionary */

    srv_dict_sys->unfreeze_data_dictionary(trx);

    return false;
  }

  ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &m_ref, m_heap);

  ptr = trx_undo_update_rec_get_update(ptr, clust_index, pars.m_type, trx_id, roll_ptr, info_bits, trx, m_heap, &m_update);

  /* Read to the partial row the fields that occur in indexes */

  if (!(pars.m_cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
    ptr = trx_undo_rec_get_partial_row(ptr, clust_index, &m_row, pars.m_type == TRX_UNDO_UPD_DEL_REC, m_heap);
  }

  return true;
}

ulint Row_purge::purge(que_thr_t *thr) {
  roll_ptr_t roll_ptr_val;
  bool purge_needed;
  bool updated_extern;
  Trx *trx;

  trx = thr_get_trx(thr);

  m_undo_rec = srv_trx_sys->m_purge->fetch_next_rec(&roll_ptr_val, &m_reservation, m_heap);

  if (m_undo_rec == nullptr) {
    /* Purge completed for this query thread */

    thr->run_node = que_node_get_parent(this);

    return DB_SUCCESS;
  }

  m_roll_ptr = roll_ptr_val;

  if (m_undo_rec == &trx_purge_dummy_rec) {
    purge_needed = false;
  } else {
    purge_needed = parse_undo_rec(&updated_extern, thr);
    /* If purge_needed == true, we must also remember to unfreeze
    data dictionary! */
  }

  if (purge_needed) {
    auto clust_index = m_table->get_first_index();

    m_found_clust = false;

    m_index = clust_index->get_next();

    if (m_rec_type == TRX_UNDO_DEL_MARK_REC) {
      del_mark();

    } else if (updated_extern || m_rec_type == TRX_UNDO_UPD_EXIST_REC) {

      upd_exist_or_extern();
    }

    if (m_found_clust) {
      m_pcur.close();
    }

    srv_dict_sys->unfreeze_data_dictionary(trx);
  }

  /* Do some cleanup */
  srv_trx_sys->m_purge->rec_release(m_reservation);

  mem_heap_empty(m_heap);

  thr->run_node = this;

  return (DB_SUCCESS);
}

que_thr_t *Row_purge::step(que_thr_t *thr) {
  auto node = static_cast<Row_purge *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_PURGE);

  auto err = node->purge(thr);

  ut_a(err == DB_SUCCESS);

  return thr;
}

que_thr_t *row_purge_step(que_thr_t *thr) {
  return Row_purge::step(thr);
}
