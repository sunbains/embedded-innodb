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

/** @file row/row0undo.c
Row undo

Created 1/8/1997 Heikki Tuuri
*******************************************************/

#include "fsp0fsp.h"
#include "mach0data.h"
#include "que0que.h"
#include "row0row.h"
#include "row0undo.h"
#include "row0upd.h"
#include "row0vers.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"

/* How to undo row operations?
(1) For an insert, we have stored a prefix of the clustered index record
in the undo log. Using it, we look for the clustered record, and using
that we look for the records in the secondary indexes. The insert operation
may have been left incomplete, if the database crashed, for example.
We may have look at the trx id and roll ptr to make sure the record in the
clustered index is really the one for which the undo log record was
written. We can use the framework we get from the original insert op.
(2) Delete marking: We can use the framework we get from the original
delete mark op. We only have to check the trx id.
(3) Update: This may be the most complicated. We have to use the framework
we get from the original update op.

What if the same trx repeatedly deletes and inserts an identical row.
Then the row id changes and also roll ptr. What if the row id was not
part of the ordering fields in the clustered index? Maybe we have to write
it to undo log. Well, maybe not, because if we order the row id and trx id
in descending order, then the only undeleted copy is the first in the
index. Our searches in row operations always position the cursor before
the first record in the result set. But, if there is no key defined for
a table, then it would be desirable that row id is in ascending order.
So, lets store row id in descending order only if it is not an ordering
field in the clustered index.

NOTE: Deletes and inserts may lead to situation where there are identical
records in a secondary index. Is that a problem in the B-tree? Yes.
Also updates can lead to this, unless trx id and roll ptr are included in
ord fields.
(1) Fix in clustered indexes: include row id, trx id, and roll ptr
in node pointers of B-tree.
(2) Fix in secondary indexes: include all fields in node pointers, and
if an entry is inserted, check if it is equal to the right neighbor,
in which case update the right neighbor: the neighbor must be delete
marked, set it unmarked and write the trx id of the current transaction.

What if the same trx repeatedly updates the same row, updating a secondary
index field or not? Updating a clustered index ordering field?

(1) If it does not update the secondary index and not the clustered index
ord field. Then the secondary index record stays unchanged, but the
trx id in the secondary index record may be smaller than in the clustered
index record. This is no problem?
(2) If it updates secondary index ord field but not clustered: then in
secondary index there are delete marked records, which differ in an
ord field. No problem.
(3) Updates clustered ord field but not secondary, and secondary index
is unique. Then the record in secondary index is just updated at the
clustered ord field.
(4)

Problem with duplicate records:
Fix 1: Add a trx op no field to all indexes. A problem: if a trx with a
bigger trx id has inserted and delete marked a similar row, our trx inserts
again a similar row, and a trx with an even bigger id delete marks it. Then
the position of the row should change in the index if the trx id affects
the alphabetical ordering.

Fix 2: If an insert encounters a similar row marked deleted, we turn the
insert into an 'update' of the row marked deleted. Then we must write undo
info on the update. A problem: what if a purge operation tries to remove
the delete marked row?

We can think of the database row versions as a linked list which starts
from the record in the clustered index, and is linked by roll ptrs
through undo logs. The secondary index records are references which tell
what kinds of records can be found in this linked list for a record
in the clustered index.

How to do the purge? A record can be removed from the clustered index
if its linked list becomes empty, i.e., the row has been marked deleted
and its roll ptr points to the record in the undo log we are going through,
doing the purge. Similarly, during a rollback, a record can be removed
if the stored roll ptr in the undo log points to a trx already (being) purged,
or if the roll ptr is nullptr, i.e., it was a fresh insert. */

/* Considerations on undoing a modify operation.
(1) Undoing a delete marking: all index records should be found. Some of
them may have delete mark already false, if the delete mark operation was
stopped underway, or if the undo operation ended prematurely because of a
system crash.
(2) Undoing an update of a delete unmarked record: the newer version of
an updated secondary index entry should be removed if no prior version
of the clustered index record requires its existence. Otherwise, it should
be delete marked.
(3) Undoing an update of a delete marked record. In this kind of update a
delete marked clustered index record was delete unmarked and possibly also
some of its fields were changed. Now, it is possible that the delete marked
version has become obsolete at the time the undo is started. */

Row_undo *srv_row_undo;

db_err Undo_node::remove_secondary_index_entry(ulint mode, Index *index, DTuple *entry) noexcept {
  ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_MODIFY_TREE);

  m_dict->m_store.m_fsp->m_log->free_check();

  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  mtr_t mtr;

  mtr.start();

  auto found = row_search_index_entry(index, entry, mode, &pcur, &mtr);
  auto btr_cur = pcur.get_btr_cur();

  if (unlikely(!found)) {
    /* Not found */

    pcur.close();
    mtr.commit();

    return DB_SUCCESS;
  }

  db_err err;

  if (mode == BTR_MODIFY_LEAF) {
    auto success = btr_cur->optimistic_delete(&mtr);
    err = success ? DB_SUCCESS : DB_FAIL;
  } else {
    /* No need to distinguish RB_RECOVERY here, because we are deleting a secondary index record:
    the distinction between RB_NORMAL and RB_RECOVERY only matters when deleting a record that
    contains externally stored columns. */
    ut_ad(!index->is_clustered());
    err = btr_cur->pessimistic_delete(false, RB_NORMAL, &mtr);
  }

  pcur.close();
  mtr.commit();

  return err;
}

db_err Undo_node::remove_secondary_index_entry(Index *index, DTuple *entry) noexcept {
  /* Try first optimistic descent to the B-tree */
  auto err = remove_secondary_index_entry(BTR_MODIFY_LEAF, index, entry);

  if (err == DB_SUCCESS) {

    return err;
  }

  ulint n_tries{};

  /* Try then pessimistic descent to the B-tree */
  for (;; ++n_tries) {
    err = remove_secondary_index_entry(BTR_MODIFY_TREE, index, entry);

    /* The delete operation may fail if we have little
    file space left: TODO: easiest to crash the database
    and restart with more file space */

    if (err == DB_SUCCESS || n_tries >= BTR_CUR_RETRY_DELETE_N_TIMES) {
      return err;
    }

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);
  }
}

Row_undo *Row_undo::create(Dict *dict) noexcept {
  auto ptr = ut_new(sizeof(Row_undo));
  return new (ptr) Row_undo(dict);
}

void Row_undo::destroy(Row_undo *&row_undo) noexcept {
  ut_delete(row_undo);
  row_undo = nullptr;
}

Undo_node::Undo_node(Dict *dict, Trx *trx, que_thr_t *parent) noexcept
: m_trx{}, m_pcur(dict->m_store.m_fsp, dict->m_store.m_btree), m_dict(dict) {

  m_common.parent = parent;
  m_common.type = QUE_NODE_UNDO;

  m_trx = trx;
  m_state = UNDO_NODE_FETCH_NEXT;

  m_pcur.init(0);

  m_heap = mem_heap_create(256);
}

Undo_node *Undo_node::create(Dict *dict, Trx *trx, que_thr_t *parent, mem_heap_t *heap) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(Undo_node));
  return new (ptr) Undo_node(dict, trx, parent);
}

bool Undo_node::search_cluster_index_and_persist_position() noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  mtr_t mtr;

  mtr.start();

  auto clust_index = m_table->get_clustered_index();
  const auto found = row_search_on_row_ref(&m_pcur, BTR_MODIFY_LEAF, m_table, m_ref, &mtr);
  auto rec = m_pcur.get_rec();

  ulint *offsets{};
  mem_heap_t *offs_heap{};

  {
    Phy_rec record{clust_index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &offs_heap, Current_location());
  }

  bool success{};

  if (!found || m_roll_ptr != row_get_rec_roll_ptr(rec, clust_index, offsets)) {

    /* We must remove the reservation on the undo log record BEFORE releasing the latch on
    the clustered index page: this is to make sure that some thread will eventually undo the
    modification corresponding to node->roll_ptr. */

  } else {

    m_row = row_build(ROW_COPY_DATA, clust_index, rec, offsets, nullptr, &m_ext, m_heap);

    if (m_update != nullptr) {
      m_undo_row = dtuple_copy(m_row, m_heap);
      Row_update::replace(m_undo_row, &m_undo_ext, clust_index, m_update, m_heap);
    } else {
      m_undo_row = nullptr;
      m_undo_ext = nullptr;
    }

    m_pcur.store_position(&mtr);

    success = true;
  }

  m_pcur.commit_specify_mtr(&mtr);

  if (unlikely(offs_heap != nullptr)) {
    mem_heap_free(offs_heap);
  }

  return success;
}

db_err Undo_node::remove_cluster_rec() noexcept {
  mtr_t mtr;
  mtr.start();

  {
    const auto success = m_pcur.restore_position(BTR_MODIFY_LEAF, &mtr, Current_location());
    ut_a(success);
  }

  if (m_table->m_id == DICT_INDEXES_ID) {
    ut_ad(m_trx->m_dict_operation_lock_mode == RW_X_LATCH);

    /* Drop the index tree associated with the row in SYS_INDEXES table: */

    m_dict->m_store.drop_index_tree(m_pcur.get_rec(), &mtr);

    mtr.commit();

    mtr.start();

    const auto success = m_pcur.restore_position(BTR_MODIFY_LEAF, &mtr, Current_location());
    ut_a(success);
  }

  auto btr_cur = m_pcur.get_btr_cur();

  {
    const auto success = btr_cur->optimistic_delete(&mtr);

    m_pcur.commit_specify_mtr(&mtr);

    if (success) {
      trx_undo_rec_release(m_trx, m_undo_no);

      return DB_SUCCESS;
    }
  }

  db_err err;

  for (ulint n_tries{}; /* nop  */; ++n_tries) {
    /* If did not succeed, try pessimistic descent to tree */
    mtr.start();

    {
      const auto success = m_pcur.restore_position(BTR_MODIFY_TREE, &mtr, Current_location());
      ut_a(success);
    }

    {
      const auto rb_ctx = trx_is_recv(m_trx) ? RB_RECOVERY : RB_NORMAL;
      err = btr_cur->pessimistic_delete(false, rb_ctx, &mtr);
    }

    /* The delete operation may fail if we have little file space left:
    TODO: easiest to crash the database and restart with more file space */

    if (err != DB_OUT_OF_FILE_SPACE || n_tries >= BTR_CUR_RETRY_DELETE_N_TIMES) {
      break;
    }

    m_pcur.commit_specify_mtr(&mtr);

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);
  }

  m_pcur.commit_specify_mtr(&mtr);

  trx_undo_rec_release(m_trx, m_undo_no);

  return err;
}

db_err Undo_node::fetch_undo_log_and_undo(que_thr_t *thr) noexcept {
  auto trx = m_trx;

  if (m_state == UNDO_NODE_FETCH_NEXT) {
    roll_ptr_t roll_ptr;

    m_undo_rec = trx_roll_pop_top_rec_of_trx(trx, trx->m_roll_limit, &roll_ptr, m_heap);

    if (!m_undo_rec) {
      /* Rollback completed for this query thread */

      thr->run_node = que_node_get_parent(this);

      return DB_SUCCESS;
    }

    m_roll_ptr = roll_ptr;
    m_undo_no = trx_undo_rec_get_undo_no(m_undo_rec);

    m_state = trx_undo_roll_ptr_is_insert(roll_ptr) ?UNDO_NODE_INSERT : UNDO_NODE_MODIFY;

  } else if (m_state == UNDO_NODE_PREV_VERS) {

    /* Undo should be done to the same clustered index record
    again in this same rollback, restoring the previous version */

    auto roll_ptr = m_new_roll_ptr;

    m_undo_rec = trx_undo_get_undo_rec_low(roll_ptr, m_heap);
    m_roll_ptr = roll_ptr;
    m_undo_no = trx_undo_rec_get_undo_no(m_undo_rec);

    m_state =   trx_undo_roll_ptr_is_insert(roll_ptr) ? UNDO_NODE_INSERT : UNDO_NODE_MODIFY;
  }

  /* Prevent DROP TABLE etc. while we are rolling back this row.
  If we are doing a TABLE CREATE or some other dictionary operation,
  then we already have dict_operation_lock locked in x-mode. Do not
  try to lock again, because that would cause a hang. */

  auto locked_data_dict = (trx->m_dict_operation_lock_mode == 0);

  if (locked_data_dict) {

    m_dict->lock_data_dictionary(trx);
    ut_a(trx->m_dict_operation_lock_mode != 0);
  }

  db_err err{};

  if (m_state == UNDO_NODE_INSERT) {

    err = undo_insert();
    m_state = UNDO_NODE_FETCH_NEXT;

  } else {

    ut_ad(m_state == UNDO_NODE_MODIFY);
    err = undo_update(thr);
  }

  if (locked_data_dict) {

    m_dict->unlock_data_dictionary(trx);
  }

  /* Do some cleanup */
  m_pcur.close();

  mem_heap_empty(m_heap);

  thr->run_node = this;

  return err;
}

db_err Undo_node::undo_insert() noexcept {
  ut_ad(m_state == UNDO_NODE_INSERT);

  parse_insert_undo_rec(srv_config.m_force_recovery);

  if (m_table == nullptr || !search_cluster_index_and_persist_position()) {
    trx_undo_rec_release(m_trx, m_undo_no);

    return DB_SUCCESS;
  }

  /* Iterate over all the secondary indexes and undo the insert.*/

  for (auto index : m_table->m_indexes) {
    /* Skip the clustered index (the first index) */
    if (index->is_clustered()) {
      continue;
    }

    auto entry = row_build_index_entry(m_row, m_ext, index, m_heap);

    if (unlikely(entry == nullptr)) {
      /* The database must have crashed after inserting a clustered index record but before
      writing all the externally stored columns of that record.  Because secondary index entries
      are inserted after the clustered index record, we may assume that the secondary index record
      does not exist.  However, this situation may only occur during the rollback of incomplete
      transactions. */
      ut_a(trx_is_recv(m_trx));
    } else {
      const auto err = remove_secondary_index_entry(index, entry);

      if (err != DB_SUCCESS) {

        return err;
      }
    }
  }

  return remove_cluster_rec();
}

inline bool Undo_node::undo_previous_version(undo_no_t *undo_no) noexcept {
  auto trx = m_trx;

  if (m_new_trx_id != trx->m_id) {

    *undo_no = 0;

    return false;

  } else {

    auto undo_rec = trx_undo_get_undo_rec_low(m_new_roll_ptr, m_heap);

    *undo_no = trx_undo_rec_get_undo_no(undo_rec);

    return trx->m_roll_limit <= *undo_no;
  }
}

db_err Undo_node::undo_clustered_index_modification(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept {
  auto btr_cur = m_pcur.get_btr_cur();
  auto success = m_pcur.restore_position(mode, mtr, Current_location());

  ut_a(success);

  db_err err;
  const auto flags = BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG;

  if (mode == BTR_MODIFY_LEAF) {

    err = btr_cur->optimistic_update(flags, m_update, m_cmpl_info, thr, mtr);

  } else {

    mem_heap_t *heap{};
    big_rec_t *dummy_big_rec;

    ut_ad(mode == BTR_MODIFY_TREE);

    err = btr_cur->pessimistic_update(flags, &heap, &dummy_big_rec, m_update, m_cmpl_info, thr, mtr);

    ut_a(dummy_big_rec == nullptr);

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }

  return err;
}

db_err Undo_node::remove_clustered_index_record(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept {
  ut_ad(m_rec_type == TRX_UNDO_UPD_DEL_REC);

  auto btr_cur = m_pcur.get_btr_cur();
  auto success = m_pcur.restore_position(mode, mtr, Current_location());

  if (!success) {

    return DB_SUCCESS;
  }

  /* Find out if we can remove the whole clustered index record */

  if (m_rec_type == TRX_UNDO_UPD_DEL_REC && !srv_row_vers->must_preserve_del_marked(m_new_trx_id, mtr)) {

    /* Ok, we can remove */

  } else {

    return DB_SUCCESS;
  }

  if (mode == BTR_MODIFY_LEAF) {

    return btr_cur->optimistic_delete(mtr) ? DB_SUCCESS : DB_FAIL;

  } else {

    db_err err;

    ut_ad(mode == BTR_MODIFY_TREE);

    /* This operation is analogous to purge, we can free also inherited externally stored fields */

    const auto rb_ctx = thr_is_recv(thr) ? RB_RECOVERY_PURGE_REC : RB_NONE;

    err = btr_cur->pessimistic_delete(false, rb_ctx, mtr);

    /* The delete operation may fail if we have little file space left:
    TODO: easiest to crash the database and restart with more file space */
    return err;
  }
}

db_err Undo_node::remove_clustered_index_record_low(que_thr_t *thr, mtr_t *mtr, ulint mode) noexcept {
  ut_ad(m_rec_type == TRX_UNDO_UPD_DEL_REC);
  ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_MODIFY_TREE);

  auto btr_cur = m_pcur.get_btr_cur();
  auto success = m_pcur.restore_position(mode, mtr, Current_location());

  if (!success) {

    return DB_SUCCESS;

  } else if (m_rec_type != TRX_UNDO_UPD_DEL_REC || srv_row_vers->must_preserve_del_marked(m_new_trx_id, mtr)) {

    /* We cannot remove the whole clustered index record */

    return DB_SUCCESS;

  } else if (mode == BTR_MODIFY_LEAF) {

    return btr_cur->optimistic_delete(mtr) ? DB_SUCCESS : DB_FAIL;

  } else {

    /* This operation is analogous to purge, we can free also inherited externally stored fields */

    return btr_cur->pessimistic_delete(false, thr_is_recv(thr) ? RB_RECOVERY_PURGE_REC : RB_NONE, mtr);

    /* The delete operation may fail if we have little file space left: TODO: easiest to crash the database
    and restart with more file space */
  }
}

db_err Undo_node::undo_clustered_index_modification(que_thr_t *thr) noexcept {

  /* Check if also the previous version of the clustered index record
  should be undone in this same rollback operation */

  undo_no_t new_undo_no;

  auto more_vers = undo_previous_version(&new_undo_no);

  mtr_t mtr;

  mtr.start();

  /* Try optimistic processing of the record, keeping changes within the index page */

  auto err = undo_clustered_index_modification(thr, &mtr, BTR_MODIFY_LEAF);

  if (err != DB_SUCCESS) {
    m_pcur.commit_specify_mtr(&mtr);

    /* We may have to modify tree structure: do a pessimistic descent down the index tree */

    mtr.start();

    err = undo_clustered_index_modification(thr, &mtr, BTR_MODIFY_TREE);
  }

  m_pcur.commit_specify_mtr(&mtr);

  if (err == DB_SUCCESS && m_rec_type == TRX_UNDO_UPD_DEL_REC) {

    mtr.start();

    err = remove_clustered_index_record_low(thr, &mtr, BTR_MODIFY_LEAF);

    if (err != DB_SUCCESS) {

      m_pcur.commit_specify_mtr(&mtr);

      /* We may have to modify tree structure: do a pessimistic descent down the index tree */

      mtr.start();

      err = remove_clustered_index_record(thr, &mtr, BTR_MODIFY_TREE);
    }

    m_pcur.commit_specify_mtr(&mtr);
  }

  m_state = UNDO_NODE_FETCH_NEXT;

  trx_undo_rec_release(m_trx, m_undo_no);

  if (more_vers && err == DB_SUCCESS) {

    /* Reserve the undo log record to the prior version after committing &mtr: this is necessary to comply
    with the latching order, as &mtr may contain the fsp latch which is lower in the latch hierarchy than
    trx->undo_mutex. */

    if (trx_undo_rec_reserve(m_trx, new_undo_no)) {
      m_state = UNDO_NODE_PREV_VERS;
    }
  }

  return err;
}

db_err Undo_node::delete_mark_or_remove_secondary_index_entry(que_thr_t *thr, Index *index, DTuple *entry, ulint mode) noexcept {
  ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_MODIFY_TREE);

  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  m_dict->m_store.m_fsp->m_log->free_check();

  mtr_t mtr;
  db_err err;

  mtr.start();

  if (likely(row_search_index_entry(index, entry, mode, &pcur, &mtr))) {
    /* We should remove the index record if no prior version of the row, which cannot be purged yet, requires
    its existence. If some requires, we should delete mark the record. */

    mtr_t mtr_vers;
    auto btr_cur = pcur.get_btr_cur();

    mtr_vers.start();

    {
      auto success = pcur.restore_position(BTR_SEARCH_LEAF, &mtr_vers, Current_location());
      ut_a(success);
    }

    if (srv_row_vers->old_has_index_entry(false, m_pcur.get_rec(), &mtr_vers, index, entry)) {

      err = btr_cur->del_mark_set_sec_rec(BTR_NO_LOCKING_FLAG, true, thr, &mtr);
      ut_ad(err == DB_SUCCESS);

    } else {
      /* Remove the index record */

      if (mode == BTR_MODIFY_LEAF) {
        err = btr_cur->optimistic_delete(&mtr) ? DB_SUCCESS : DB_FAIL;
      } else {
        /* No need to distinguish RB_RECOVERY_PURGE here, because we are deleting a secondary index record:
        the distinction between RB_NORMAL and RB_RECOVERY_PURGE only matters when deleting a record that
        contains externally stored columns. */

        ut_ad(!index->is_clustered());
        err = btr_cur->pessimistic_delete(false, RB_NORMAL, &mtr);

        /* The delete operation may fail if we have little file space left: TODO: easiest to crash the database
        and restart with more file space */
      }
    }

    m_pcur.commit_specify_mtr(&mtr_vers);

  } else {

    /* In crash recovery, the secondary index record may be missing if the UPDATE did not have time to insert
    the secondary index records before the crash.  When we are undoing that UPDATE in crash recovery, the record
    may be missing.

    In normal processing, if an update ends in a deadlock before it has inserted all updated secondary index
    records, then the undo will not find those records. */

    err = DB_SUCCESS;

  }

  pcur.close();

  mtr.commit();

  return err;
}

db_err Undo_node::delete_mark_or_remove_secondary_index_entry(que_thr_t *thr, Index *index, DTuple *entry) noexcept {
  if (auto err = delete_mark_or_remove_secondary_index_entry(thr, index, entry, BTR_MODIFY_LEAF); err == DB_SUCCESS) {
    return err;
  } else {
    return delete_mark_or_remove_secondary_index_entry(thr, index, entry, BTR_MODIFY_TREE);
  }
}

db_err Undo_node::del_unmark_secondary_index_and_undo_update(ulint mode, que_thr_t *thr, Index *index, const DTuple *entry) noexcept {
  ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_MODIFY_TREE);

  auto trx = thr_get_trx(thr);
  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  /* Ignore indexes that are being created. */
  if (unlikely(*index->m_name == TEMP_INDEX_PREFIX)) {

    return DB_SUCCESS;
  }

  m_dict->m_store.m_fsp->m_log->free_check();

  mtr_t mtr;
  db_err err;

  mtr.start();

  if (unlikely(!row_search_index_entry(index, entry, mode, &pcur, &mtr))) {
    log_err("Sec index del undo failed in ", index->m_name, " tuple: ");
    dtuple_print(ib_stream, entry);
    log_err("\nrecord ");
    log_err("\nrec: ", rec_to_string(pcur.get_rec()));
    log_err("\ntrx: ", trx->to_string(0));
    log_err("\nSubmit a detailed bug report, check the Embedded InnoDB website for details");

    err = DB_ERROR;

  } else {
    auto btr_cur = pcur.get_btr_cur();
    const auto flags{BTR_KEEP_SYS_FLAG | BTR_NO_LOCKING_FLAG};

    err = btr_cur->del_mark_set_sec_rec(BTR_NO_LOCKING_FLAG, false, thr, &mtr);

    ut_a(err == DB_SUCCESS);

    auto heap = mem_heap_create(100);
    auto update = Row_update::build_sec_rec_difference_binary(index, entry, btr_cur->get_rec(), trx, heap);

    if (Row_update::upd_get_n_fields(update) == 0) {

      /* Do nothing */

    } else if (mode == BTR_MODIFY_LEAF) {
      /* Try an optimistic updating of the record, keeping changes within the page */

      err = btr_cur->optimistic_update(flags, update, 0, thr, &mtr);

      switch (err) {
        case DB_OVERFLOW:
        case DB_UNDERFLOW:
          err = DB_FAIL;
          break;

        default:
          break;
      }
    } else {
      big_rec_t *dummy_big_rec{};

      err = btr_cur->pessimistic_update(flags, &heap, &dummy_big_rec, update, 0, thr, &mtr);
      ut_a(!dummy_big_rec);
    }

    mem_heap_free(heap);
  }

  pcur.close();

  mtr.commit();

  return err;
}

db_err Undo_node::undo_delete_in_secondary_indexes(que_thr_t *thr) noexcept {
  db_err err{DB_SUCCESS};

  ut_ad(m_rec_type == TRX_UNDO_UPD_DEL_REC);
  auto heap = mem_heap_create(1024);

  for (; m_index != nullptr; m_index = m_index->get_next()) {
    auto index = m_index;
    auto entry = row_build_index_entry(m_row, m_ext, index, heap);

    if (unlikely(entry == nullptr)) {
      /* The database must have crashed after inserting a clustered index record but before
      writing all the externally stored columns of that record.  Because secondary index entries
      are inserted after the clustered index record, we may assume that the secondary index record
      does not exist.  However, this situation may only occur during the rollback of incomplete
      transactions. */
      ut_a(thr_is_recv(thr));

    } else {

      err = delete_mark_or_remove_secondary_index_entry(thr, index, entry);

      if (err != DB_SUCCESS) {

        break;
      }
    }

    mem_heap_empty(heap);
  }

  mem_heap_free(heap);

  return err;
}

db_err Undo_node::undo_del_mark_secondary_indexes(que_thr_t *thr) noexcept {
  auto heap = mem_heap_create(1024);

  for (; m_index != nullptr; m_index = m_index->get_next()) {
    auto index = m_index;
    auto entry = row_build_index_entry(m_row, m_ext, index, heap);
    ut_a(entry != nullptr);

    auto err = del_unmark_secondary_index_and_undo_update(BTR_MODIFY_LEAF, thr, index, entry);

    if (err == DB_FAIL) {
      err = del_unmark_secondary_index_and_undo_update(BTR_MODIFY_TREE, thr, index, entry);
    }

    if (err != DB_SUCCESS) {

      mem_heap_free(heap);

      return err;
    }
  }

  mem_heap_free(heap);

  return DB_SUCCESS;
}

db_err Undo_node::undo_update_of_secondary_indexes(que_thr_t *thr) noexcept {
  if (m_cmpl_info & UPD_NODE_NO_ORD_CHANGE) {
    /* No change in secondary indexes */

    return DB_SUCCESS;
  }

  db_err err{DB_SUCCESS};
  auto heap = mem_heap_create(1024);

  for (; m_index != nullptr; m_index = m_index->get_next()) {
    auto index = m_index;

    if (Row_update::changes_ord_field_binary(m_row, m_index, m_update)) {

      /* Build the newest version of the index entry */
      auto entry = row_build_index_entry(m_row, m_ext, index, heap);
      ut_a(entry != nullptr);

      /* NOTE that if we updated the fields of a delete-marked secondary index record so that
      alphabetically they stayed the same, e.g., 'abc' -> 'aBc', we cannot return to the original
      values because we do not know them. But this should not cause problems because in row0sel.cs,
      in queries we always retrieve the clustered index record or an earlier version of it, if the
      secondary index record through which we do the search is delete-marked. */

      auto err = delete_mark_or_remove_secondary_index_entry(thr, index, entry);

      if (err != DB_SUCCESS) {
        break;
      }

      /* We may have to update the delete mark in the secondary index record of the previous version of
      the row. We also need to update the fields of the secondary index record if we updated its fields
      but alphabetically they stayed the same, e.g.,
      'abc' -> 'aBc'. */
      mem_heap_empty(heap);

      entry = row_build_index_entry(m_undo_row, m_undo_ext, index, heap);

      ut_a(entry != nullptr);

      err = del_unmark_secondary_index_and_undo_update(BTR_MODIFY_LEAF, thr, index, entry);

      if (err == DB_FAIL) {
        err = del_unmark_secondary_index_and_undo_update(BTR_MODIFY_TREE, thr, index, entry);
      }

      if (err != DB_SUCCESS) {
        break;
      }
    }
  }

  mem_heap_free(heap);

  return err;
}

void Undo_node::parse_insert_undo_rec(ib_recovery_t recovery) noexcept {
  Undo_rec_pars pars;
  auto ptr = trx_undo_rec_get_pars(m_undo_rec, pars);

  ut_ad(pars.m_type == TRX_UNDO_INSERT_REC);
  m_rec_type = pars.m_type;

  m_update = nullptr;
  m_table = m_dict->table_get_on_id(recovery, pars.m_table_id, m_trx);

  /* Skip the UNDO if we can't find the table or the .ibd file. */
  if (unlikely(m_table == nullptr)) {
   /* Table was dropped. */ 
  } else if (unlikely(m_table->m_ibd_file_missing)) {
    /* We skip undo operations to missing .ibd files */
    m_table = nullptr;
  } else {
    auto clust_index = m_table->get_clustered_index();

    if (clust_index != nullptr) {
      ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &m_ref, m_heap);
    } else {
      log_err("  table ", m_table->m_name, " has no indexes, ignoring the table");

      m_table = nullptr;
    }
  }
}

void Undo_node::parse_update_undo_rec(ib_recovery_t recovery, que_thr_t *thr) noexcept {
  Undo_rec_pars pars;
  auto trx = thr_get_trx(thr);
  auto ptr = trx_undo_rec_get_pars(m_undo_rec, pars);

  m_rec_type = pars.m_type;

  m_table = m_dict->table_get_on_id(recovery, pars.m_table_id, trx);

  /* TODO: other fixes associated with DROP TABLE + rollback in the same table by another user */

  if (m_table == nullptr) {
    /* Table was dropped */
    return;
  }

  if (m_table->m_ibd_file_missing) {
    /* We skip undo operations to missing .ibd files */
    m_table = nullptr;

    return;
  }

  auto clust_index = m_table->get_clustered_index();

  trx_id_t trx_id;
  ulint info_bits;
  roll_ptr_t roll_ptr;

  ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);

  ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &m_ref, m_heap);

  trx_undo_update_rec_get_update(ptr, clust_index, pars.m_type, trx_id, roll_ptr, info_bits, trx, m_heap, &m_update);

  m_new_trx_id = trx_id;
  m_new_roll_ptr = roll_ptr;
  m_cmpl_info = pars.m_cmpl_info;
}

db_err Undo_node::undo_update(que_thr_t *thr) noexcept {
  db_err err;
  ut_a(m_rec_type == TRX_UNDO_UPD_EXIST_REC || m_rec_type == TRX_UNDO_DEL_MARK_REC || m_rec_type == TRX_UNDO_UPD_DEL_REC);

  ut_ad(m_state == UNDO_NODE_MODIFY);

  // FIXME: Get rid of this global variable access
  parse_update_undo_rec(srv_config.m_force_recovery, thr);

  if (m_table == nullptr || !search_cluster_index_and_persist_position()) {
    /* It is already undone, or will be undone by another query thread, or table was dropped */

    trx_undo_rec_release(m_trx, m_undo_no);
    m_state = UNDO_NODE_FETCH_NEXT;

    return DB_SUCCESS;
  }

  /* Get first secondary index */
  m_index = m_table->get_first_secondary_index();

  switch (m_rec_type) {
    case TRX_UNDO_UPD_EXIST_REC:
      err = undo_update_of_secondary_indexes(thr);
      break;

    case TRX_UNDO_DEL_MARK_REC:
      err = undo_del_mark_secondary_indexes(thr);
      break;

    case TRX_UNDO_UPD_DEL_REC:
      err = undo_delete_in_secondary_indexes(thr);
      break;

    default:
      ut_error;;
  }

  if (err != DB_SUCCESS) {
    return err;
  } else {
    return undo_clustered_index_modification(thr);
  }
}

que_thr_t *Row_undo::step(que_thr_t *thr) noexcept {
  ++srv_activity_count;

  auto trx = thr_get_trx(thr);
  auto undo_node = static_cast<Undo_node *>(thr->run_node);

  ut_ad(que_node_get_type(undo_node) == QUE_NODE_UNDO);

  const auto err = undo_node->fetch_undo_log_and_undo(thr);

  trx->m_error_state = err;

  if (err != DB_SUCCESS) {
    /* SQL error detected */
    log_fatal(std::format("Fatal error {} in rollback.", (int)err));
  }

  return thr;
}

Undo_node *Row_undo::create_undo_node(Trx *trx, que_thr_t *parent, mem_heap_t *heap) noexcept {
  return Undo_node::create(m_dict, trx, parent, heap);
}