/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.

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

/** Creates a purge node to a query graph.
@return	own: purge node */

purge_node_t *row_purge_node_create(
  que_thr_t *parent, /*!< in: parent node, i.e., a thr node */
  mem_heap_t *heap
) /*!< in: memory heap where created */
{
  ut_ad(parent);
  ut_ad(heap);

  auto node = reinterpret_cast<purge_node_t *>(mem_heap_alloc(heap, sizeof(purge_node_t)));

  node->common.type = QUE_NODE_PURGE;
  node->common.parent = parent;

  node->heap = mem_heap_create(256);

  return (node);
}

/** Repositions the pcur in the purge node on the clustered index record,
if found.
@return	true if the record was found */
static bool row_purge_reposition_pcur(ulint latch_mode, purge_node_t *node, mtr_t *mtr) {
  if (node->found_clust) {
    return node->pcur.restore_position(latch_mode, mtr, Source_location{});
  } else {
    node->found_clust = row_search_on_row_ref(&node->pcur, latch_mode, node->table, node->ref, mtr);

    if (node->found_clust) {
      node->pcur.store_position(mtr);
    }

    return node->found_clust;
  }
}

/** Removes a delete marked clustered index record if possible.
@return true if success, or if not found, or if modified after the
delete marking */
static bool row_purge_remove_clust_if_poss_low(
  purge_node_t *node, /*!< in: row purge node */
  ulint mode
) /*!< in: BTR_MODIFY_LEAF or BTR_MODIFY_TREE */
{
  mtr_t mtr;
  db_err err;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

  auto pcur = &node->pcur;
  auto btr_cur = pcur->get_btr_cur();
  auto index = dict_table_get_first_index(node->table);

  mtr_start(&mtr);

  auto success = row_purge_reposition_pcur(mode, node, &mtr);

  if (!success) {
    /* The record is already removed */

    pcur->commit_specify_mtr(&mtr);

    return (true);
  }

  auto rec = pcur->get_rec();
  ulint *offsets{};

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &heap, Source_location{});
  }

  if (node->roll_ptr != row_get_rec_roll_ptr(rec, index, offsets)) {

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
    /* Someone else has modified the record later: do not remove */
    pcur->commit_specify_mtr(&mtr);

    return (true);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (mode == BTR_MODIFY_LEAF) {
    success = btr_cur_optimistic_delete(btr_cur, &mtr);
  } else {
    ut_ad(mode == BTR_MODIFY_TREE);
    btr_cur_pessimistic_delete(&err, false, btr_cur, RB_NONE, &mtr);

    if (err == DB_SUCCESS) {
      success = true;
    } else if (err == DB_OUT_OF_FILE_SPACE) {
      success = false;
    } else {
      ut_error;
    }
  }

  pcur->commit_specify_mtr(&mtr);

  return (success);
}

/** Removes a clustered index record if it has not been modified after the
delete marking. */
static void row_purge_remove_clust_if_poss(purge_node_t *node) /*!< in: row purge node */
{
  bool success;
  ulint n_tries = 0;

  /*	ib_logger(ib_stream, "Purge: Removing clustered record\n"); */

  success = row_purge_remove_clust_if_poss_low(node, BTR_MODIFY_LEAF);
  if (success) {

    return;
  }
retry:
  success = row_purge_remove_clust_if_poss_low(node, BTR_MODIFY_TREE);
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

/** Removes a secondary index entry if possible.
@return	true if success or if not found */
static bool row_purge_remove_sec_if_poss_low(
  purge_node_t *node,    /*!< in: row purge node */
  dict_index_t *index,   /*!< in: index */
  const dtuple_t *entry, /*!< in: index entry */
  ulint mode
) /*!< in: latch mode BTR_MODIFY_LEAF
                                             or BTR_MODIFY_TREE */
{
  btr_pcur_t pcur;
  btr_cur_t *btr_cur;
  bool success;
  bool old_has = 0; /* remove warning */
  bool found;
  db_err err;
  mtr_t mtr;
  mtr_t mtr_vers;

  log_free_check();
  mtr_start(&mtr);

  found = row_search_index_entry(index, entry, mode, &pcur, &mtr);

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

    mtr_commit(&mtr);

    return (true);
  }

  btr_cur = pcur.get_btr_cur();

  /* We should remove the index record if no later version of the row,
  which cannot be purged yet, requires its existence. If some requires,
  we should do nothing. */

  mtr_start(&mtr_vers);

  success = row_purge_reposition_pcur(BTR_SEARCH_LEAF, node, &mtr_vers);

  if (success) {
    old_has = row_vers_old_has_index_entry(true, node->pcur.get_rec(), &mtr_vers, index, entry);
  }

  pcur.commit_specify_mtr(&mtr_vers);

  if (!success || !old_has) {
    /* Remove the index record */

    if (mode == BTR_MODIFY_LEAF) {
      success = btr_cur_optimistic_delete(btr_cur, &mtr);
    } else {
      ut_ad(mode == BTR_MODIFY_TREE);
      btr_cur_pessimistic_delete(&err, false, btr_cur, RB_NONE, &mtr);
      success = err == DB_SUCCESS;
      ut_a(success || err == DB_OUT_OF_FILE_SPACE);
    }
  }

  pcur.close();
  mtr_commit(&mtr);

  return (success);
}

/** Removes a secondary index entry if possible. */
static void row_purge_remove_sec_if_poss(
  purge_node_t *node,  /*!< in: row purge node */
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry
) /*!< in: index entry */
{
  bool success;
  ulint n_tries = 0;

  /* ib_logger(ib_stream, "Purge: Removing secondary record\n"); */

  success = row_purge_remove_sec_if_poss_low(node, index, entry, BTR_MODIFY_LEAF);
  if (success) {

    return;
  }
retry:
  success = row_purge_remove_sec_if_poss_low(node, index, entry, BTR_MODIFY_TREE);
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

/** Purges a delete marking of a record. */
static void row_purge_del_mark(purge_node_t *node) /*!< in: row purge node */
{
  mem_heap_t *heap;
  dtuple_t *entry;
  dict_index_t *index;

  ut_ad(node);

  heap = mem_heap_create(1024);

  while (node->index != nullptr) {
    index = node->index;

    /* Build the index entry */
    entry = row_build_index_entry(node->row, nullptr, index, heap);
    ut_a(entry);
    row_purge_remove_sec_if_poss(node, index, entry);

    node->index = dict_table_get_next_index(node->index);
  }

  mem_heap_free(heap);

  row_purge_remove_clust_if_poss(node);
}

/** Purges an update of an existing record. Also purges an update of a delete
marked record if that record contained an externally stored field. */
static void row_purge_upd_exist_or_extern(purge_node_t *node) /*!< in: row purge node */
{
  mem_heap_t *heap;
  dtuple_t *entry;
  dict_index_t *index;
  bool is_insert;
  ulint rseg_id;
  ulint page_no;
  ulint offset;
  ulint i;
  mtr_t mtr;

  ut_ad(node);

  if (node->rec_type == TRX_UNDO_UPD_DEL_REC) {

    goto skip_secondaries;
  }

  heap = mem_heap_create(1024);

  while (node->index != nullptr) {
    index = node->index;

    if (row_upd_changes_ord_field_binary(nullptr, node->index, node->update)) {
      /* Build the older version of the index entry */
      entry = row_build_index_entry(node->row, nullptr, index, heap);
      ut_a(entry);
      row_purge_remove_sec_if_poss(node, index, entry);
    }

    node->index = dict_table_get_next_index(node->index);
  }

  mem_heap_free(heap);

skip_secondaries:
  /* Free possible externally stored fields */
  for (i = 0; i < upd_get_n_fields(node->update); i++) {

    const upd_field_t *ufield = upd_get_nth_field(node->update, i);

    if (dfield_is_ext(&ufield->new_val)) {
      buf_block_t *block;
      ulint internal_offset;
      byte *data_field;

      /* We use the fact that new_val points to
      node->undo_rec and get thus the offset of
      dfield data inside the undo record. Then we
      can calculate from node->roll_ptr the file
      address of the new_val data */

      internal_offset = ((const byte *)dfield_get_data(&ufield->new_val)) - node->undo_rec;

      ut_a(internal_offset < UNIV_PAGE_SIZE);

      trx_undo_decode_roll_ptr(node->roll_ptr, &is_insert, &rseg_id, &page_no, &offset);
      mtr_start(&mtr);

      /* We have to acquire an X-latch to the clustered
      index tree */

      index = dict_table_get_first_index(node->table);

      mtr_x_lock(dict_index_get_lock(index), &mtr);

      /* NOTE: we must also acquire an X-latch to the
      root page of the tree. We will need it when we
      free pages from the tree. If the tree is of height 1,
      the tree X-latch does NOT protect the root page,
      because it is also a leaf page. Since we will have a
      latch on an undo log page, we would break the
      latching order if we would only later latch the
      root page of such a tree! */

      btr_root_get(index, &mtr);

      /* We assume in purge of externally stored fields
      that the space id of the undo log record is 0! */

      Buf_pool::Request req {
        .m_rw_latch = RW_X_LATCH,
        .m_page_id = { SYS_TABLESPACE, page_no },
        .m_mode = BUF_GET,
        .m_file = __FILE__,
        .m_line = __LINE__,
        .m_mtr = &mtr
      };

      block = srv_buf_pool->get(req, nullptr);
      buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_UNDO_PAGE));

      data_field = block->get_frame() + offset + internal_offset;

      ut_a(dfield_get_len(&ufield->new_val) >= BTR_EXTERN_FIELD_REF_SIZE);
      btr_free_externally_stored_field(
        index, data_field + dfield_get_len(&ufield->new_val) - BTR_EXTERN_FIELD_REF_SIZE, nullptr, nullptr, 0, RB_NONE, &mtr
      );
      mtr_commit(&mtr);
    }
  }
}

/** Parses the row reference and other info in a modify undo log record.
@return true if purge operation required: NOTE that then the CALLER
must unfreeze data dictionary! */
static bool row_purge_parse_undo_rec(
  purge_node_t *node, /*!< in: row undo node */
  bool *updated_extern,
  /*!< out: true if an externally stored field
                         was updated */
  que_thr_t *thr
) /*!< in: query thread */
{
  dict_index_t *clust_index;
  byte *ptr;
  trx_t *trx;
  undo_no_t undo_no;
  uint64_t table_id;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;
  ulint info_bits;
  ulint type;
  ulint cmpl_info;

  ut_ad(node && thr);

  trx = thr_get_trx(thr);

  ptr = trx_undo_rec_get_pars(node->undo_rec, &type, &cmpl_info, updated_extern, &undo_no, &table_id);
  node->rec_type = type;

  if (type == TRX_UNDO_UPD_DEL_REC && !(*updated_extern)) {

    return (false);
  }

  ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);
  node->table = nullptr;

  if (type == TRX_UNDO_UPD_EXIST_REC && cmpl_info & UPD_NODE_NO_ORD_CHANGE && !(*updated_extern)) {

    /* Purge requires no changes to indexes: we may return */

    return (false);
  }

  /* Prevent DROP TABLE etc. from running when we are doing the purge
  for this row */

  dict_freeze_data_dictionary(trx);

  mutex_enter(&(dict_sys->mutex));

  // FIXME: srv_force_recovery should be passed in as an arg
  node->table = dict_table_get_on_id_low(ib_recovery_t(srv_force_recovery), table_id);

  mutex_exit(&(dict_sys->mutex));

  if (node->table == nullptr) {
    /* The table has been dropped: no need to do purge */
  err_exit:
    dict_unfreeze_data_dictionary(trx);
    return (false);
  }

  if (node->table->ibd_file_missing) {
    /* We skip purge of missing .ibd files */

    node->table = nullptr;

    goto err_exit;
  }

  clust_index = dict_table_get_first_index(node->table);

  if (clust_index == nullptr) {
    /* The table was corrupt in the data dictionary */

    goto err_exit;
  }

  ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &(node->ref), node->heap);

  ptr = trx_undo_update_rec_get_update(ptr, clust_index, type, trx_id, roll_ptr, info_bits, trx, node->heap, &(node->update));

  /* Read to the partial row the fields that occur in indexes */

  if (!(cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
    ptr = trx_undo_rec_get_partial_row(ptr, clust_index, &node->row, type == TRX_UNDO_UPD_DEL_REC, node->heap);
  }

  return (true);
}

/** Fetches an undo log record and does the purge for the recorded operation.
If none left, or the current purge completed, returns the control to the
parent node, which is always a query thread node.
@return	DB_SUCCESS if operation successfully completed, else error code */
static ulint row_purge(
  purge_node_t *node, /*!< in: row purge node */
  que_thr_t *thr
) /*!< in: query thread */
{
  roll_ptr_t roll_ptr;
  bool purge_needed;
  bool updated_extern;
  trx_t *trx;

  ut_ad(node && thr);

  trx = thr_get_trx(thr);

  node->undo_rec = trx_purge_fetch_next_rec(&roll_ptr, &(node->reservation), node->heap);
  if (!node->undo_rec) {
    /* Purge completed for this query thread */

    thr->run_node = que_node_get_parent(node);

    return (DB_SUCCESS);
  }

  node->roll_ptr = roll_ptr;

  if (node->undo_rec == &trx_purge_dummy_rec) {
    purge_needed = false;
  } else {
    purge_needed = row_purge_parse_undo_rec(node, &updated_extern, thr);
    /* If purge_needed == true, we must also remember to unfreeze
    data dictionary! */
  }

  if (purge_needed) {
    dict_index_t *clust_index;

    clust_index = dict_table_get_first_index(node->table);

    node->found_clust = false;

    node->index = dict_table_get_next_index(clust_index);

    if (node->rec_type == TRX_UNDO_DEL_MARK_REC) {
      row_purge_del_mark(node);

    } else if (updated_extern || node->rec_type == TRX_UNDO_UPD_EXIST_REC) {

      row_purge_upd_exist_or_extern(node);
    }

    if (node->found_clust) {
      node->pcur.close();
    }

    dict_unfreeze_data_dictionary(trx);
  }

  /* Do some cleanup */
  trx_purge_rec_release(node->reservation);
  mem_heap_empty(node->heap);

  thr->run_node = node;

  return (DB_SUCCESS);
}

que_thr_t *row_purge_step(que_thr_t *thr) {
  ut_ad(thr);

  auto node = static_cast<purge_node_t *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_PURGE);

  auto err = row_purge(node, thr);

  ut_a(err == DB_SUCCESS);

  return thr;
}
