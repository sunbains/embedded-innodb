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

/** @file row/row0uins.c
Fresh insert undo

Created 2/25/1997 Heikki Tuuri
*******************************************************/

#include "row0uins.h"

#include "btr0btr.h"
#include "dict0boot.h"
#include "dict0crea.h"
#include "dict0dict.h"
#include "ibuf0ibuf.h"
#include "log0log.h"
#include "mach0data.h"
#include "que0que.h"
#include "row0row.h"
#include "row0undo.h"
#include "row0upd.h"
#include "row0vers.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0trx.h"
#include "trx0undo.h"

/** Removes a clustered index record. The pcur in node was positioned on the
record, now it is detached.
@param[in,out] node             Undo node.
@return	DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
static db_err
row_undo_ins_remove_clust_rec(undo_node_t *node) {
  btr_cur_t *btr_cur;
  db_err err;
  ulint n_tries{};
  mtr_t mtr;

  mtr_start(&mtr);

  auto success = btr_pcur_restore_position(BTR_MODIFY_LEAF, &(node->pcur), &mtr);
  ut_a(success);

  if (ut_dulint_cmp(node->table->id, DICT_INDEXES_ID) == 0) {
    ut_ad(node->trx->dict_operation_lock_mode == RW_X_LATCH);

    /* Drop the index tree associated with the row in
    SYS_INDEXES table: */

    dict_drop_index_tree(btr_pcur_get_rec(&(node->pcur)), &mtr);

    mtr_commit(&mtr);

    mtr_start(&mtr);

    success = btr_pcur_restore_position(BTR_MODIFY_LEAF, &(node->pcur), &mtr);
    ut_a(success);
  }

  btr_cur = btr_pcur_get_btr_cur(&(node->pcur));

  success = btr_cur_optimistic_delete(btr_cur, &mtr);

  btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);

  if (success) {
    trx_undo_rec_release(node->trx, node->undo_no);

    return DB_SUCCESS;
  }
retry:
  /* If did not succeed, try pessimistic descent to tree */
  mtr_start(&mtr);

  success = btr_pcur_restore_position(BTR_MODIFY_TREE, &(node->pcur), &mtr);
  ut_a(success);

  btr_cur_pessimistic_delete(&err, FALSE, btr_cur, trx_is_recv(node->trx) ? RB_RECOVERY : RB_NORMAL, &mtr);

  /* The delete operation may fail if we have little
  file space left: TODO: easiest to crash the database
  and restart with more file space */

  if (err == DB_OUT_OF_FILE_SPACE && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES) {

    btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);

    ++n_tries;

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

    goto retry;
  }

  btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);

  trx_undo_rec_release(node->trx, node->undo_no);

  return err;
}

/** Removes a secondary index entry if found.
@param[in,out] mode             BTR_MODIFY_LEAF or BTR_MODIFY_TREE,
                                depending on whether we wish optimistic or
                                pessimistic descent down the index tree
@param[in] index                Remove entry from this index
@param[in] entry                Index entry to remove
@return	DB_SUCCESS, DB_FAIL, or DB_OUT_OF_FILE_SPACE */
static db_err row_undo_ins_remove_sec_low(ulint mode, dict_index_t *index, dtuple_t *entry) {
  btr_pcur_t pcur;
  btr_cur_t *btr_cur;
  ibool found;
  ibool success;
  db_err err;
  mtr_t mtr;

  log_free_check();
  mtr_start(&mtr);

  found = row_search_index_entry(index, entry, mode, &pcur, &mtr);

  btr_cur = btr_pcur_get_btr_cur(&pcur);

  if (!found) {
    /* Not found */

    btr_pcur_close(&pcur);
    mtr_commit(&mtr);

    return DB_SUCCESS;
  }

  if (mode == BTR_MODIFY_LEAF) {
    success = btr_cur_optimistic_delete(btr_cur, &mtr);

    if (success) {
      err = DB_SUCCESS;
    } else {
      err = DB_FAIL;
    }
  } else {
    ut_ad(mode == BTR_MODIFY_TREE);

    /* No need to distinguish RB_RECOVERY here, because we
    are deleting a secondary index record: the distinction
    between RB_NORMAL and RB_RECOVERY only matters when
    deleting a record that contains externally stored
    columns. */
    ut_ad(!dict_index_is_clust(index));
    btr_cur_pessimistic_delete(&err, FALSE, btr_cur, RB_NORMAL, &mtr);
  }

  btr_pcur_close(&pcur);
  mtr_commit(&mtr);

  return err;
}

/** Removes a secondary index entry from the index if found. Tries first
optimistic, then pessimistic descent down the tree.
@param[in,out] index            Remove entry from this secondary index.
@param[in] entry                Entry to remove.
@return	DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
static db_err
row_undo_ins_remove_sec(dict_index_t *index, dtuple_t *entry) {

  /* Try first optimistic descent to the B-tree */

  auto err = row_undo_ins_remove_sec_low(BTR_MODIFY_LEAF, index, entry);

  if (err == DB_SUCCESS) {

    return err;
  }

  ulint n_tries{};

  /* Try then pessimistic descent to the B-tree */
  for (;; ++n_tries) {
    err = row_undo_ins_remove_sec_low(BTR_MODIFY_TREE, index, entry);

    /* The delete operation may fail if we have little
    file space left: TODO: easiest to crash the database
    and restart with more file space */

    if (err == DB_SUCCESS || n_tries >= BTR_CUR_RETRY_DELETE_N_TIMES) {
      return err;
    }

    os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);
  }
}

/** Parses the row reference and other info in a fresh insert undo record.
@param[in] recovery             Recovery flag
@param[in,out] node             Ros undo node. */
static void
row_undo_ins_parse_undo_rec(ib_recovery_t recovery, undo_node_t *node) {
  ulint type;
  ulint dummy;
  dulint table_id;
  undo_no_t undo_no;
  ibool dummy_extern;

  auto ptr = trx_undo_rec_get_pars(node->undo_rec, &type, &dummy, &dummy_extern, &undo_no, &table_id);

  ut_ad(type == TRX_UNDO_INSERT_REC);
  node->rec_type = type;

  node->update = nullptr;
  node->table = dict_table_get_on_id(ib_recovery_t(srv_force_recovery), table_id, node->trx);

  /* Skip the UNDO if we can't find the table or the .ibd file. */
  if (UNIV_UNLIKELY(node->table == nullptr)) {
  } else if (UNIV_UNLIKELY(node->table->ibd_file_missing)) {
    node->table = nullptr;
  } else {
    auto clust_index = dict_table_get_first_index(node->table);

    if (clust_index != nullptr) {
      ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &node->ref, node->heap);
    } else {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  InnoDB: table ");
      ut_print_name(ib_stream, node->trx, TRUE, node->table->name);
      ib_logger(ib_stream, " has no indexes, "
                           "ignoring the table\n");

      node->table = nullptr;
    }
  }
}

db_err row_undo_ins(undo_node_t *node) {
  ut_ad(node);
  ut_ad(node->state == UNDO_NODE_INSERT);

  row_undo_ins_parse_undo_rec(ib_recovery_t(srv_force_recovery), node);

  if (!node->table || !row_undo_search_clust_to_pcur(node)) {
    trx_undo_rec_release(node->trx, node->undo_no);

    return DB_SUCCESS;
  }

  /* Iterate over all the indexes and undo the insert.*/

  /* Skip the clustered index (the first index) */
  node->index = dict_table_get_next_index(dict_table_get_first_index(node->table));

  while (node->index != nullptr) {
    auto entry = row_build_index_entry(node->row, node->ext, node->index, node->heap);

    if (UNIV_UNLIKELY(!entry)) {
      /* The database must have crashed after
      inserting a clustered index record but before
      writing all the externally stored columns of
      that record.  Because secondary index entries
      are inserted after the clustered index record,
      we may assume that the secondary index record
      does not exist.  However, this situation may
      only occur during the rollback of incomplete
      transactions. */
      ut_a(trx_is_recv(node->trx));
    } else {
      auto err = row_undo_ins_remove_sec(node->index, entry);

      if (err != DB_SUCCESS) {

        return err;
      }
    }

    node->index = dict_table_get_next_index(node->index);
  }

  return row_undo_ins_remove_clust_rec(node);
}
