/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file row/row0upd.c
Update of a row

Created 12/27/1996 Heikki Tuuri
*******************************************************/

#include "btr0blob.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0lru.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "log0log.h"
#include "mach0data.h"
#include "pars0sym.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0ext.h"
#include "row0ins.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0upd.h"
#include "trx0undo.h"

/* What kind of latch and lock can we assume when the control comes to an update node?
   ----------------------------------------------------------------------------------
Efficiency of massive updates would require keeping an x-latch on a
clustered index page through many updates, and not setting an explicit
x-lock on clustered index records, as they anyway will get an implicit
x-lock when they are updated. A problem is that the read nodes in the
graph should know that they must keep the latch when passing the control
up to the update node, and not set any record lock on the record which
will be updated. Another problem occurs if the execution is stopped,
as the kernel switches to another query thread, or the transaction must
wait for a lock. Then we should be able to release the latch and, maybe,
acquire an explicit x-lock on the record.
        Because this seems too complicated, we conclude that the less
efficient solution of releasing all the latches when the control is
transferred to another node, and acquiring explicit x-locks, is better. */

/* How is a delete performed? If there is a delete without an
explicit cursor, i.e., a searched delete, there are at least
two different situations:
the implicit select cursor may run on (1) the clustered index or
on (2) a secondary index. The delete is performed by setting
the delete bit in the record and substituting the id of the
deleting transaction for the original trx id, and substituting a
new roll ptr for previous roll ptr. The old trx id and roll ptr
are saved in the undo log record. Thus, no physical changes occur
in the index tree structure at the time of the delete. Only
when the undo log is purged, the index records will be physically
deleted from the index trees.

The query graph executing a searched delete would consist of
a delete node which has as a subtree a select subgraph.
The select subgraph should return a (persistent) cursor
in the clustered index, placed on page which is x-latched.
The delete node should look for all secondary index records for
this clustered index entry and mark them as deleted. When is
the x-latch freed? The most efficient way for performing a
searched delete is obviously to keep the x-latch for several
steps of query graph execution. */

Row_update *srv_row_upd{};

Row_update::Row_update(Dict *dict, Lock_sys *lock_sys) noexcept : m_dict(dict), m_lock_sys(lock_sys) {

}

Row_update::~Row_update() noexcept {
}

Row_update *Row_update::create(Dict *dict, Lock_sys *lock_sys) noexcept {
  auto ptr = ut_new(sizeof(Row_update));

  return new (ptr) Row_update(dict, lock_sys);
}

void Row_update::destroy(Row_update *&row_upd) noexcept {
  call_destructor(row_upd);
  ut_delete(row_upd);
  row_upd = nullptr;
}

bool Row_update::index_is_referenced(const Index *index, Trx *trx) noexcept {
  auto table = index->m_table;

  if (table->m_referenced_list.empty()) {

    return false;
  }

  bool froze_data_dict{};

  if (trx->m_dict_operation_lock_mode == 0) {
    m_dict->freeze_data_dictionary(trx);
    froze_data_dict = true;
  }

  bool is_referenced{};

  for (auto foreign : table->m_referenced_list) {
    if (foreign->m_referenced_index == index) {
      is_referenced = true;
      break;
    }
  }

  if (froze_data_dict) {
    m_dict->unfreeze_data_dictionary(trx);
  }

  return is_referenced;
}

db_err Row_update::check_references_constraints(upd_node_t *node, Btree_pcursor *pcur, Table *table, const Index *index, ulint *offsets, que_thr_t *thr, mtr_t *mtr) noexcept {
  db_err err;
  ulint n_ext;
  bool got_s_lock{};

  if (table->m_referenced_list.empty()) {

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);
  auto rec = pcur->get_rec();
  ut_ad(rec_offs_validate(rec, index, offsets));

  auto heap = mem_heap_create(500);
  auto entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

  mtr->commit();

  mtr->start();

  if (trx->m_dict_operation_lock_mode == 0) {
    got_s_lock = true;

    m_dict->freeze_data_dictionary(trx);
  }

  for (auto foreign : table->m_referenced_list) {
    /* Note that we may have an update which updates the index
    record, but does NOT update the first fields which are
    referenced in a foreign key constraint. Then the update does
    NOT break the constraint. */

    if (foreign->m_referenced_index == index &&
        (node->m_is_delete ||
         changes_first_fields_binary(entry, index, node->m_update, foreign->m_n_fields))) {

      if (foreign->m_foreign_table == nullptr) {
        (void) m_dict->table_get(foreign->m_foreign_table_name, false);
      }

      if (foreign->m_foreign_table) {
        m_dict->mutex_acquire();

        ++foreign->m_foreign_table->m_n_foreign_key_checks_running;

        m_dict->mutex_release();
      }

      /* NOTE that if the thread ends up waiting for a lock
      we will release dict_operation_lock temporarily!
      But the counter on the table protects 'foreign' from
      being dropped while the check is running. */

      err = srv_row_ins->check_foreign_constraint(false, foreign, table, entry, thr);

      if (foreign->m_foreign_table) {
        m_dict->mutex_acquire();

        ut_a(foreign->m_foreign_table->m_n_foreign_key_checks_running > 0);

        --foreign->m_foreign_table->m_n_foreign_key_checks_running;

        m_dict->mutex_release();
      }

      if (err != DB_SUCCESS) {

        goto func_exit;
      }
    }
  }

  err = DB_SUCCESS;

func_exit:
  if (got_s_lock) {
    m_dict->unfreeze_data_dictionary(trx);
  }

  mem_heap_free(heap);

  return err;
}

upd_node_t *Row_update::node_create(mem_heap_t *heap) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(upd_node_t));
  auto node = new (ptr) upd_node_t();

  node->m_heap = mem_heap_create(128);
  node->m_common.type = QUE_NODE_UPDATE;
  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  return node;
}

void Row_update::rec_sys_fields_in_recovery(rec_t *rec, const ulint *offsets, ulint pos, trx_id_t trx_id, roll_ptr_t roll_ptr) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  ulint len;

  auto col_offset = rec_get_nth_field_offs(offsets, pos, &len);
  ut_ad(len == DATA_TRX_ID_LEN);

  auto field = rec + col_offset;

  static_assert(DATA_TRX_ID + 1 == DATA_ROLL_PTR, "error DATA_TRX_ID + 1 != DATA_ROLL_PTR");

  Trx_sys::write_trx_id(field, trx_id);
  trx_write_roll_ptr(field + DATA_TRX_ID_LEN, roll_ptr);
}

void Row_update::index_entry_sys_field(const DTuple *entry, const Index *index, ulint type, uint64_t val) noexcept {
  ut_ad(index->is_clustered());

  auto pos = index->get_sys_col_field_pos(type);

  auto dfield = dtuple_get_nth_field(entry, pos);
  auto field = dfield_get_data(dfield);

  if (type == DATA_TRX_ID) {
    Trx_sys::write_trx_id(reinterpret_cast<byte *>(field), val);
  } else {
    ut_ad(type == DATA_ROLL_PTR);
    trx_write_roll_ptr((byte *)field, val);
  }
}

bool Row_update::changes_field_size_or_external(const Index *index, const ulint *offsets, const upd_t *update) noexcept {
  ut_ad(rec_offs_validate(nullptr, index, offsets));
  const auto n_fields = upd_get_n_fields(update);

  for (ulint i{}; i < n_fields; i++) {
    auto upd_field = update->get_nth_field(i);

    auto new_val = &(upd_field->m_new_val);
    auto new_len = dfield_get_len(new_val);

    if (dfield_is_null(new_val)) {
      new_len = index->get_nth_col(upd_field->m_field_no)->get_fixed_size();
    }

    const auto old_len = rec_offs_nth_size(offsets, upd_field->m_field_no);

    if (dfield_is_ext(new_val) || old_len != new_len || rec_offs_nth_extern(offsets, upd_field->m_field_no)) {

      return true;
    }
  }

  return false;
}

void Row_update::rec_in_place(rec_t *rec, const Index *index, const ulint *offsets, const upd_t *update) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));

  rec_set_info_bits(rec, update->m_info_bits);

  const auto n_fields = upd_get_n_fields(update);

  for (ulint i{}; i < n_fields; i++) {
    auto upd_field = update->get_nth_field(i);
    auto new_val = &upd_field->m_new_val;

    ut_ad(!dfield_is_ext(new_val) == !rec_offs_nth_extern(offsets, upd_field->m_field_no));

    rec_set_nth_field(rec, offsets, upd_field->m_field_no, dfield_get_data(new_val), dfield_get_len(new_val));
  }
}

byte *Row_update::write_sys_vals_to_log(const Index *index, Trx *trx, roll_ptr_t roll_ptr, byte *log_ptr, mtr_t *mtr __attribute__((unused))) noexcept {
  ut_ad(index->is_clustered());

  log_ptr += mach_write_compressed(log_ptr, index->get_sys_col_field_pos(DATA_TRX_ID));

  trx_write_roll_ptr(log_ptr, roll_ptr);
  log_ptr += DATA_ROLL_PTR_LEN;

  log_ptr += mach_uint64_write_compressed(log_ptr, trx->m_id);

  return log_ptr;
}

byte *Row_update::parse_sys_vals(byte *ptr, byte *end_ptr, ulint *pos, trx_id_t *trx_id, roll_ptr_t *roll_ptr) noexcept {
  *pos = static_cast<ulint>(mach_parse_compressed(ptr, end_ptr));

  if (ptr == nullptr) {

    return nullptr;
  }

  if (end_ptr < ptr + DATA_ROLL_PTR_LEN) {

    return nullptr;
  }

  *roll_ptr = trx_read_roll_ptr(ptr);
  ptr += DATA_ROLL_PTR_LEN;

  ptr = mach_uint64_parse_compressed(ptr, end_ptr, trx_id);

  return ptr;
}

void Row_update::index_write_log(const upd_t *update, byte *log_ptr, mtr_t *mtr) noexcept {
  auto buf_end = log_ptr + MLOG_BUF_MARGIN;
  const auto n_fields = upd_get_n_fields(update);

  mach_write_to_1(log_ptr, update->m_info_bits);
  log_ptr++;
  log_ptr += mach_write_compressed(log_ptr, n_fields);

  for (ulint i{}; i < n_fields; i++) {

    static_assert(MLOG_BUF_MARGIN > 30, "error MLOG_BUF_MARGIN <= 30");

    if (log_ptr + 30 > buf_end) {
      mlog_close(mtr, log_ptr);

      log_ptr = mlog_open(mtr, MLOG_BUF_MARGIN);
      buf_end = log_ptr + MLOG_BUF_MARGIN;
    }

    auto upd_field = update->get_nth_field(i);
    auto new_val = &(upd_field->m_new_val);
    auto len = dfield_get_len(new_val);

    log_ptr += mach_write_compressed(log_ptr, upd_field->m_field_no);
    log_ptr += mach_write_compressed((byte *)log_ptr, len);

    if (len != UNIV_SQL_NULL) {
      if (log_ptr + len < buf_end) {
        memcpy(log_ptr, dfield_get_data(new_val), len);

        log_ptr += len;
      } else {
        mlog_close(mtr, log_ptr);

        mlog_catenate_string(mtr, (byte *)dfield_get_data(new_val), len);

        log_ptr = mlog_open(mtr, MLOG_BUF_MARGIN);
        buf_end = log_ptr + MLOG_BUF_MARGIN;
      }
    }
  }

  mlog_close(mtr, log_ptr);
}

byte *Row_update::index_parse(byte *ptr, byte *end_ptr, mem_heap_t *heap, upd_t **update_out) noexcept {
  if (end_ptr < ptr + 1) {

    return nullptr;
  }

  auto info_bits = mach_read_from_1(ptr);

  ptr++;

  auto n_fields = mach_parse_compressed(ptr, end_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  auto update = upd_create(n_fields, heap);
  update->m_info_bits = info_bits;

  for (ulint i{}; i < n_fields; ++i) {
    auto upd_field = update->get_nth_field(i);
    auto new_val = &upd_field->m_new_val;
    auto field_no = mach_parse_compressed(ptr, end_ptr);

    if (ptr == nullptr) {

      return nullptr;
    }

    upd_field->m_field_no = field_no;

    auto len = mach_parse_compressed(ptr, end_ptr);

    if (ptr == nullptr) {

      return nullptr;
    }

    if (len != UNIV_SQL_NULL) {

      if (end_ptr < ptr + len) {

        return nullptr;
      }

      dfield_set_data(new_val, mem_heap_dup(heap, ptr, len), len);
      ptr += len;
    } else {
      dfield_set_null(new_val);
    }
  }

  *update_out = update;

  return ptr;
}

upd_t *Row_update::build_sec_rec_difference_binary(const Index *index, const DTuple *entry, const rec_t *rec, Trx *trx, mem_heap_t *heap) noexcept {
  /* This function is used only for a secondary index */
  ut_a(!index->is_clustered());

  ulint n_diff{};
  const auto update = upd_create(dtuple_get_n_fields(entry), heap);

  ulint *offsets;
  std::array<ulint, REC_OFFS_SMALL_SIZE> rec_offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  for (ulint i{}; i < dtuple_get_n_fields(entry); ++i) {

    ulint len;
    const auto data = rec_get_nth_field(rec, offsets, i, &len);
    const auto dfield = dtuple_get_nth_field(entry, i);

    /* NOTE that it may be that len != dfield_get_len(dfield) if we
    are updating in a character set and collation where strings of
    different length can be equal in an alphabetical comparison,
    and also in the case where we have a column prefix index
    and the last characters in the index field are spaces; the
    latter case probably caused the assertion failures reported at
    row0upd.c line 713 in versions 4.0.14 - 4.0.16. */

    /* NOTE: we compare the fields as binary strings!
    (No collation) */

    if (!dfield_data_is_binary_equal(dfield, len, data)) {

      auto upd_field = update->get_nth_field(n_diff);

      dfield_copy(&(upd_field->m_new_val), dfield);

      upd_field->set_field_no(i, index, trx);

      ++n_diff;
    }
  }

  update->m_n_fields = n_diff;

  return update;
}

upd_t *Row_update::build_difference_binary(const Index *index, const DTuple *entry, const rec_t *rec, Trx *trx, mem_heap_t *heap) noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  /* This function is used only for a clustered index */
  ut_a(index->is_clustered());

  ulint n_diff{};
  auto update = upd_create(dtuple_get_n_fields(entry), heap);

  const auto roll_ptr_pos = index->get_sys_col_field_pos(DATA_ROLL_PTR);
  const auto trx_id_pos = index->get_sys_col_field_pos(DATA_TRX_ID);

  ulint *offsets;

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  for (ulint i{}; i < dtuple_get_n_fields(entry); ++i) {

    ulint len;
    const auto data = rec_get_nth_field(rec, offsets, i, &len);
    const auto dfield = dtuple_get_nth_field(entry, i);

    /* NOTE: we compare the fields as binary strings!
    (No collation) */

    if (i == trx_id_pos || i == roll_ptr_pos) {

      goto skip_compare;
    }

    if (unlikely(!dfield_is_ext(dfield) != !rec_offs_nth_extern(offsets, i)) || !dfield_data_is_binary_equal(dfield, len, data)) {

      auto upd_field = update->get_nth_field(n_diff);

      dfield_copy(&(upd_field->m_new_val), dfield);

      upd_field->set_field_no(i, index, trx);

      ++n_diff;
    }
  skip_compare:;
  }

  update->m_n_fields = n_diff;

  return update;
}

byte *Row_update::ext_fetch(const byte *data, ulint local_len, ulint *len, mem_heap_t *heap) noexcept {
  auto buf = mem_heap_alloc(heap, *len);
  Blob blob(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  *len = blob.copy_externally_stored_field_prefix(buf, *len, data, local_len);
  /* We should never update records containing a half-deleted BLOB. */
  ut_a(*len);

  return buf;
}

void Row_update::index_replace_new_col_val(dfield_t *dfield, const Field *field, const Column *col, const upd_field_t *uf, mem_heap_t *heap) noexcept {
  dfield_copy_data(dfield, &uf->m_new_val);

  if (dfield_is_null(dfield)) {
    return;
  }

  auto len = dfield_get_len(dfield);
  auto data = (const byte *)dfield_get_data(dfield);

  if (field->m_prefix_len > 0) {
    bool fetch_ext = dfield_is_ext(dfield) && len < (ulint)field->m_prefix_len + BTR_EXTERN_FIELD_REF_SIZE;

    if (fetch_ext) {
      ulint l = len;

      len = field->m_prefix_len;

      data = ext_fetch(data, l, &len, heap);
    }

    len = dtype_get_at_most_n_mbchars(col->prtype, col->mbminlen, col->mbmaxlen, field->m_prefix_len, len, (const char *)data);

    dfield_set_data(dfield, data, len);

    if (!fetch_ext) {
      dfield_dup(dfield, heap);
    }

    return;
  }

  switch (uf->m_orig_len) {
    byte *buf;
    case BTR_EXTERN_FIELD_REF_SIZE:
      /* Restore the original locally stored part of the column. In the undo log,
      InnoDB writes a longer prefix of externally stored columns, so that column prefixes
      in secondary indexes can be reconstructed. */
      dfield_set_data(dfield, data + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);
      dfield_set_ext(dfield);
      /* fall through */
    case 0:
      dfield_dup(dfield, heap);
      break;
    default:
      /* Reconstruct the original locally stored part of the column.  The data will have to be copied. */
      ut_a(uf->m_orig_len > BTR_EXTERN_FIELD_REF_SIZE);
      buf = mem_heap_alloc(heap, uf->m_orig_len);

      /* Copy the locally stored prefix. */
      memcpy(buf, data, uf->m_orig_len - BTR_EXTERN_FIELD_REF_SIZE);

      /* Copy the BLOB pointer. */
      memcpy(buf + uf->m_orig_len - BTR_EXTERN_FIELD_REF_SIZE, data + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);

      dfield_set_data(dfield, buf, uf->m_orig_len);
      dfield_set_ext(dfield);
      break;
  }
}

void Row_update::index_replace_new_col_vals_index_pos(DTuple *entry, const Index *index, const upd_t *update, bool order_only, mem_heap_t *heap) noexcept {
  dtuple_set_info_bits(entry, update->m_info_bits);

  const auto n_fields = order_only ? index->get_n_unique() : index->get_n_fields();

  for (ulint i{}; i < n_fields; ++i) {
    auto field = index->get_nth_field(i);
    auto col = field->get_col();
    auto uf = update->get_field(i);

    if (uf != nullptr) {
      index_replace_new_col_val(dtuple_get_nth_field(entry, i), field, col, uf, heap);
    }
  }
}

void Row_update::index_replace_new_col_vals(DTuple *entry, Index *index, const upd_t *update, mem_heap_t *heap) noexcept {
  const auto clust_index = index->m_table->get_first_index();

  dtuple_set_info_bits(entry, update->m_info_bits);

  for (ulint i{}; i < index->get_n_fields(); ++i) {
    auto field = index->get_nth_field(i);
    auto col = field->get_col();
    auto uf = update->get_nth_field(clust_index->get_clustered_field_pos(col));

    if (uf != nullptr) {
      index_replace_new_col_val(dtuple_get_nth_field(entry, i), field, col, uf, heap);
    }
  }
}

void Row_update::replace(DTuple *row, row_ext_t **ext, const Index *index, const upd_t *update, mem_heap_t *heap) noexcept {
  ut_ad(index->is_clustered());

  const auto table = index->m_table;
  auto n_cols = dtuple_get_n_fields(row);
  ut_ad(n_cols == table->get_n_cols());

  ulint n_ext_cols{};
  auto ext_cols = reinterpret_cast<ulint *>(mem_heap_alloc(heap, n_cols * sizeof(ulint)));

  dtuple_set_info_bits(row, update->m_info_bits);

  for (ulint col_no{}; col_no < n_cols; col_no++) {

    auto col = table->get_nth_col(col_no);
    auto clust_pos = index->get_clustered_field_pos(col);

    if (unlikely(clust_pos == ULINT_UNDEFINED)) {

      continue;
    }

    auto dfield = dtuple_get_nth_field(row, col_no);

    for (ulint i{}; i < upd_get_n_fields(update); i++) {

      auto upd_field = update->get_nth_field(i);

      if (upd_field->m_field_no != clust_pos) {

        continue;
      }

      dfield_copy_data(dfield, &upd_field->m_new_val);
      break;
    }

    if (dfield_is_ext(dfield) && col->m_ord_part) {
      ext_cols[n_ext_cols++] = col_no;
    }
  }

  if (n_ext_cols) {
    *ext = row_ext_create(n_ext_cols, ext_cols, row, heap);
  } else {
    *ext = nullptr;
  }
}

bool Row_update::changes_ord_field_binary(const DTuple *row, Index *index, const upd_t *update) noexcept {
  ut_ad(update);

  const auto n_unique = index->get_n_unique();
  const auto n_upd_fields = upd_get_n_fields(update);
  const auto clust_index = index->get_clustered_index();

  for (ulint i{}; i < n_unique; ++i) {
    auto ind_field = index->get_nth_field(i);
    auto col = ind_field->get_col();
    auto col_pos = clust_index->get_clustered_field_pos(col);
    auto col_no = col->get_no();

    for (ulint j = 0; j < n_upd_fields; j++) {

      auto upd_field = update->get_nth_field(j);

      /* Note that if the index field is a column prefix
      then it may be that row does not contain an externally
      stored part of the column value, and we cannot compare
      the datas */

      if (col_pos == upd_field->m_field_no &&
          (row == nullptr || ind_field->m_prefix_len > 0 ||
           !dfield_datas_are_binary_equal(dtuple_get_nth_field(row, col_no), &(upd_field->m_new_val)))) {

        return true;
      }
    }
  }

  return false;
}

bool Row_update::changes_some_index_ord_field_binary(const Table *table, const upd_t *update) noexcept {
  const auto index = table->get_first_index();

  for (ulint i{}; i < upd_get_n_fields(update); i++) {

    auto upd_field = update->get_nth_field(i);

    if (index->get_nth_field(upd_field->m_field_no)->get_col()->m_ord_part) {

      return true;
    }
  }

  return false;
}

bool Row_update::changes_first_fields_binary(DTuple *entry, const Index *index, const upd_t *update, ulint n) noexcept {
  ut_ad(n <= index->get_n_fields());

  const auto n_upd_fields = upd_get_n_fields(update);
  const auto clust_index = index->m_table->get_first_index();

  for (ulint i{}; i < n; ++i) {

    const auto ind_field = index->get_nth_field(i);
    const auto col = ind_field->get_col();
    auto col_pos = clust_index->get_clustered_field_pos(col);

    ut_a(ind_field->m_prefix_len == 0);

    for (ulint j{}; j < n_upd_fields; ++j) {

      auto upd_field = update->get_nth_field(j);

      if (col_pos == upd_field->m_field_no && !dfield_datas_are_binary_equal(dtuple_get_nth_field(entry, i), &(upd_field->m_new_val))) {
        return true;
      }
    }
  }

  return false;
}

inline void Row_update::copy_columns(rec_t *rec, const ulint *offsets, sym_node_t *column) noexcept {
  while (column != nullptr) {
    ulint len;
    auto col_offset = rec_get_nth_field_offs(offsets, column->field_nos[SYM_CLUST_FIELD_NO], &len);
    auto data = rec + col_offset;

    eval_node_copy_and_alloc_val(column, data, len);

    column = UT_LIST_GET_NEXT(col_var_list, column);
  }
}

inline void Row_update::eval_new_vals(upd_t *update) noexcept {
  const auto n_fields = upd_get_n_fields(update);

  for (ulint i{}; i < n_fields; ++i) {
    auto upd_field = update->get_nth_field(i);
    auto exp = upd_field->m_exp;

    eval_exp(exp);

    dfield_copy_data(&upd_field->m_new_val, que_node_get_val(exp));
  }
}

void Row_update::store_row(upd_node_t *node) noexcept {
  mem_heap_t *heap{};


  ut_ad(node->m_pcur->m_latch_mode != BTR_NO_LATCHES);

  if (node->m_row != nullptr) {
    mem_heap_empty(node->m_heap);
  }

  const auto clust_index = node->m_table->get_clustered_index();

  auto rec = node->m_pcur->get_rec();

  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  ulint *offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  {
    Phy_rec record{clust_index, rec};

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  node->m_row = row_build(ROW_COPY_DATA, clust_index, rec, offsets, nullptr, &node->m_ext, node->m_heap);

  if (node->m_is_delete) {
    node->m_upd_row = nullptr;
    node->m_upd_ext = nullptr;
  } else {
    node->m_upd_row = dtuple_copy(node->m_row, node->m_heap);
    replace(node->m_upd_row, &node->m_upd_ext, clust_index, node->m_update, node->m_heap);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

db_err Row_update::sec_index_entry(upd_node_t *node, que_thr_t *thr) noexcept {
  db_err err = DB_SUCCESS;
  auto trx = thr_get_trx(thr);
  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  auto index = node->m_index;
  auto heap = mem_heap_create(1024);
  auto check_ref = index_is_referenced(index, trx);
  auto entry = row_build_index_entry(node->m_row, node->m_ext, index, heap);
  ut_a(entry != nullptr);

  log_sys->free_check();

  mtr_t mtr;

  mtr.start();

  auto found = row_search_index_entry(index, entry, BTR_MODIFY_LEAF, &pcur, &mtr);

  auto btr_cur = pcur.get_btr_cur();

  auto rec = btr_cur->get_rec();

  if (unlikely(!found)) {
    log_err("Sec index entry update in ");
    m_dict->index_name_print(trx, index);
    log_err("\ntuple ");
    dtuple_print(ib_stream, entry);
    log_err("record ");
    log_err(rec_to_string(rec));
    log_info(trx->to_string(0));
    log_err("Submit a detailed bug report");
  } else {
    /* Delete mark the old index record; it can already be
    delete marked if we return after a lock wait in
    row_ins_index_entry below */

    if (!rec_get_deleted_flag(rec)) {

      err = btr_cur->del_mark_set_sec_rec(0, true, thr, &mtr);

      if (err == DB_SUCCESS && check_ref) {
        Phy_rec record{index, rec};
        auto offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());

        /* NOTE that the following call loses the position of pcur ! */
        err = check_references_constraints(node, &pcur, index->m_table, index, offsets, thr, &mtr);
      }
    }
  }

  pcur.close();
  mtr.commit();

  if (node->m_is_delete || err != DB_SUCCESS) {

    goto func_exit;
  }

  /* Build a new index entry */
  entry = row_build_index_entry(node->m_upd_row, node->m_upd_ext, index, heap);
  ut_a(entry);

  /* Insert new index entry */
  err = srv_row_ins->index_entry(index, entry, 0, true, thr);

func_exit:
  mem_heap_free(heap);

  return err;
}

inline db_err Row_update::sec_step(upd_node_t *node, que_thr_t *thr) noexcept {
  ut_ad((node->m_state == UPD_NODE_UPDATE_ALL_SEC) || (node->m_state == UPD_NODE_UPDATE_SOME_SEC));
  ut_ad(!node->m_index->is_clustered());

  if (node->m_state == UPD_NODE_UPDATE_ALL_SEC || changes_ord_field_binary(node->m_row, node->m_index, node->m_update)) {
    return sec_index_entry(node, thr);
  }

  return DB_SUCCESS;
}

db_err Row_update::clust_rec_by_insert(upd_node_t *upd_node, const Index *index, que_thr_t *thr, bool check_ref, mtr_t *mtr) noexcept {
  db_err err;
  DTuple *entry;
  mem_heap_t *heap{};

  ut_ad(index->is_clustered());

  auto trx = thr_get_trx(thr);
  auto table = upd_node->m_table;
  auto pcur = upd_node->m_pcur;
  auto btr_cur = pcur->get_btr_cur();

  Blob blob(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  if (upd_node->m_state != UPD_NODE_INSERT_CLUSTERED) {
    rec_t *rec;
    Index *index;

    err = btr_cur->del_mark_set_clust_rec(BTR_NO_LOCKING_FLAG, true, thr, mtr);

    if (err != DB_SUCCESS) {
      mtr->commit();
      return err;
    }

    /* Mark as not-owned the externally stored fields which the new
    row inherits from the delete marked record: purge should not
    free those externally stored fields even if the delete marked
    record is removed from the index tree, or updated. */

    rec = btr_cur->get_rec();
    index = table->get_first_index();

    ulint *offsets;
    std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

    {
      Phy_rec record{index, rec};

      rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());
      offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
    }

    blob.mark_extern_inherited_fields(rec, index, offsets, upd_node->m_update, mtr);

    if (check_ref) {
      /* NOTE that the following call loses the position of pcur ! */
      err = check_references_constraints(upd_node, pcur, table, index, offsets, thr, mtr);

      if (err != DB_SUCCESS) {
        mtr->commit();

        if (likely_null(heap)) {
          mem_heap_free(heap);
        }
        return err;
      }
    }
  }

  mtr->commit();

  if (likely(heap == nullptr)) {
    heap = mem_heap_create(500);
  }
  upd_node->m_state = UPD_NODE_INSERT_CLUSTERED;

  entry = row_build_index_entry(upd_node->m_upd_row, upd_node->m_upd_ext, index, heap);
  ut_a(entry);

  index_entry_sys_field(entry, index, DATA_TRX_ID, trx->m_id);

  if (upd_node->m_upd_ext) {
    /* If we return from a lock wait, for example, we may have
    extern fields marked as not-owned in entry (marked in the
    if-branch above). We must unmark them. */

    blob.unmark_dtuple_extern_fields(entry);

    /* We must mark non-updated extern fields in entry as
    inherited, so that a possible rollback will not free them. */

    blob.mark_dtuple_inherited_extern(entry, upd_node->m_update);
  }

  err = srv_row_ins->index_entry(index, entry, upd_node->m_upd_ext ? upd_node->m_upd_ext->n_ext : 0, true, thr);

  mem_heap_free(heap);

  return err;
}

db_err Row_update::clust_rec(upd_node_t *upd_node, const Index *index, que_thr_t *thr, mtr_t *mtr) noexcept {
  mem_heap_t *heap = nullptr;
  big_rec_t *big_rec = nullptr;
  db_err err;

  ut_ad(index->is_clustered());

  auto pcur = upd_node->m_pcur;
  auto btr_cur = pcur->get_btr_cur();

  ut_ad(!rec_get_deleted_flag(pcur->get_rec()));

  /* Try optimistic updating of the record, keeping changes within
  the page; we do not check locks because we assume the x-lock on the
  record to update */

  if (upd_node->m_cmpl_info & UPD_NODE_NO_SIZE_CHANGE) {
    err = btr_cur->update_in_place(BTR_NO_LOCKING_FLAG, upd_node->m_update, upd_node->m_cmpl_info, thr, mtr);
  } else {
    err = btr_cur->optimistic_update(BTR_NO_LOCKING_FLAG, upd_node->m_update, upd_node->m_cmpl_info, thr, mtr);
  }

  mtr->commit();

  if (likely(err == DB_SUCCESS)) {

    return DB_SUCCESS;
  }

  if (m_dict->m_store.m_fsp->m_buf_pool->m_LRU->buf_pool_running_out()) {

    return DB_LOCK_TABLE_FULL;
  }
  /* We may have to modify the tree structure: do a pessimistic descent down the
   * index tree */

  mtr->start();

  /* NOTE: this transaction has an s-lock or x-lock on the record and
  therefore other transactions cannot modify the record when we have no
  latch on the page. In addition, we assume that other query threads of
  the same transaction do not modify the record in the meantime.
  Therefore we can assert that the restoration of the cursor succeeds. */

  ut_a(pcur->restore_position(BTR_MODIFY_TREE, mtr, Current_location()));

  ut_ad(!rec_get_deleted_flag(pcur->get_rec()));

  err = btr_cur->pessimistic_update(BTR_NO_LOCKING_FLAG, &heap, &big_rec, upd_node->m_update, upd_node->m_cmpl_info, thr, mtr);

  mtr->commit();

  if (err == DB_SUCCESS && big_rec) {
    rec_t *rec;


    mtr->start();

    ut_a(pcur->restore_position(BTR_MODIFY_TREE, mtr, Current_location()));

    rec = btr_cur->get_rec();

    ulint *offsets{};
    std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
    }

    Blob blob(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

    err = blob.store_big_rec_extern_fields(index, btr_cur->get_block(), rec, offsets, big_rec, mtr);

    mtr->commit();
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (big_rec != nullptr) {
    dtuple_big_rec_free(big_rec);
  }

  return err;
}

db_err Row_update::del_mark_clust_rec(upd_node_t *node, const Index *index, ulint *offsets, que_thr_t *thr, bool check_ref, mtr_t *mtr) noexcept {
  db_err err;

  ut_ad(node->m_is_delete);
  ut_ad(index->is_clustered());

  auto pcur = node->m_pcur;
  auto btr_cur = pcur->get_btr_cur();

  /* Store row because we have to build also the secondary index
  entries */

  store_row(node);

  /* Mark the clustered index record deleted; we do not have to check
  locks, because we assume that we have an x-lock on the record */

  err = btr_cur->del_mark_set_clust_rec(BTR_NO_LOCKING_FLAG, true, thr, mtr);

  if (err == DB_SUCCESS && check_ref) {
    /* NOTE that the following call loses the position of pcur ! */

    err = check_references_constraints(node, pcur, index->m_table, index, offsets, thr, mtr);
  }

  mtr->commit();

  return err;
}

db_err Row_update::clust_step(upd_node_t *node, que_thr_t *thr) noexcept {
  db_err err;
  rec_t *rec;
  mem_heap_t *heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> rec_offsets;

  rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

  auto index = node->m_table->get_first_index();
  auto check_ref = index_is_referenced(index, thr_get_trx(thr));
  auto pcur = node->m_pcur;

  /* We have to restore the cursor to its position */
  mtr_t mtr_local;
  auto mtr = &mtr_local;

  mtr->start();

  /* If the restoration does not succeed, then the same
  transaction has deleted the record on which the cursor was,
  and that is an SQL error. If the restoration succeeds, it may
  still be that the same transaction has successively deleted
  and inserted a record with the same ordering fields, but in
  that case we know that the transaction has at least an
  implicit x-lock on the record. */

  ut_a(pcur->m_rel_pos == Btree_cursor_pos::ON);

  auto success = pcur->restore_position(BTR_MODIFY_LEAF, mtr, Current_location());

  if (!success) {
    mtr->commit();
    return DB_RECORD_NOT_FOUND;
  }

  /* If this is a row in SYS_INDEXES table of the data dictionary,
  then we have to free the file segments of the index tree associated
  with the index */

  if (node->m_is_delete && node->m_table->m_id == DICT_INDEXES_ID) {

    m_dict->m_store.drop_index_tree(pcur->get_rec(), mtr);

    mtr->commit();

    mtr->start();

    success = pcur->restore_position(BTR_MODIFY_LEAF, mtr, Current_location());

    if (!success) {
      mtr->commit();
      return DB_ERROR;
    }
  }

  rec = pcur->get_rec();

  ulint *offsets;

  {
    Phy_rec record{index, rec};

    rec_offs_set_n_alloc(rec_offsets.data(), rec_offsets.size());

    offsets = record.get_col_offsets(rec_offsets.data(), ULINT_UNDEFINED, &heap, Current_location());
  }

  if (!node->m_has_clust_rec_x_lock) {

    err = m_lock_sys->clust_rec_modify_check_and_lock(0, pcur->get_block(), rec, index, offsets, thr);

    if (err != DB_SUCCESS) {
      mtr->commit();
      goto exit_func;
    }
  }

  /* NOTE: the following function calls will also commit mtr */

  if (node->m_is_delete) {
    err = del_mark_clust_rec(node, index, offsets, thr, check_ref, mtr);

    if (err == DB_SUCCESS) {
      node->m_state = UPD_NODE_UPDATE_ALL_SEC;
      node->m_index = index->get_next();
    }

  exit_func:
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }

    return err;
  }

  /* If the update is made for a user, we already have the update vector
  ready, else we have to do some evaluation: */

  if (unlikely(!node->m_in_client_interface)) {
    /* Copy the necessary columns from clust_rec and calculate the
    new values to set */
    copy_columns(rec, offsets, UT_LIST_GET_FIRST(node->m_columns));
    eval_new_vals(node->m_update);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (node->m_cmpl_info & UPD_NODE_NO_ORD_CHANGE) {

    return clust_rec(node, index, thr, mtr);
  }

  store_row(node);

  if (changes_ord_field_binary(node->m_row, index, node->m_update)) {

    /* Update causes an ordering field (ordering fields within
    the B-tree) of the clustered index record to change: perform
    the update by delete marking and inserting.

    TODO! What to do to the 'Halloween problem', where an update
    moves the record forward in index so that it is again
    updated when the cursor arrives there? Solution: the
    read operation must check the undo record undo number when
    choosing records to update.

    FIXME: MySQL used to solve the problem externally! */

    err = clust_rec_by_insert(node, index, thr, check_ref, mtr);

    if (err != DB_SUCCESS) {

      return err;
    }

    node->m_state = UPD_NODE_UPDATE_ALL_SEC;
  } else {

    err = clust_rec(node, index, thr, mtr);

    if (err != DB_SUCCESS) {

      return err;
    }

    node->m_state = UPD_NODE_UPDATE_SOME_SEC;
  }

  node->m_index = index->get_next();

  return err;
}

db_err Row_update::update(upd_node_t *node, que_thr_t *thr) noexcept {
  db_err err = DB_SUCCESS;

  if (likely(node->m_in_client_interface)) {

    /* We do not get the cmpl_info value from the user
    interpreter: we must calculate it on the fly: */

    if (node->m_is_delete || changes_some_index_ord_field_binary(node->m_table, node->m_update)) {
      node->m_cmpl_info = 0;
    } else {
      node->m_cmpl_info = UPD_NODE_NO_ORD_CHANGE;
    }
  }

  if (node->m_state == UPD_NODE_UPDATE_CLUSTERED || node->m_state == UPD_NODE_INSERT_CLUSTERED) {

    err = clust_step(node, thr);

    if (err != DB_SUCCESS) {

      goto function_exit;
    }
  }

  if (!node->m_is_delete && (node->m_cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {

    goto function_exit;
  }

  while (node->m_index != nullptr) {
    err = sec_step(node, thr);

    if (err != DB_SUCCESS) {

      goto function_exit;
    }

    node->m_index = node->m_index->get_next();
  }

function_exit:
  if (err == DB_SUCCESS) {
    /* Do some cleanup */

    if (node->m_row != nullptr) {
      node->m_row = nullptr;
      node->m_ext = nullptr;
      node->m_upd_row = nullptr;
      node->m_upd_ext = nullptr;
      mem_heap_empty(node->m_heap);
    }

    node->m_state = UPD_NODE_UPDATE_CLUSTERED;
  }

  return err;
}

que_thr_t *Row_update::step(que_thr_t *thr) noexcept {
  db_err err = DB_SUCCESS;

  ut_ad(thr);

  auto trx = thr_get_trx(thr);
  auto node = static_cast<upd_node_t *>(thr->run_node);
  auto sel_node = node->m_select;
  auto parent = que_node_get_parent(node);

  ut_ad(que_node_get_type(node) == QUE_NODE_UPDATE);

  if (thr->prev_node == parent) {
    node->m_state = UPD_NODE_SET_IX_LOCK;
  }

  if (node->m_state == UPD_NODE_SET_IX_LOCK) {

    if (!node->m_has_clust_rec_x_lock) {
      /* It may be that the current session has not yet
      started its transaction, or it has been committed: */

      err = m_lock_sys->lock_table(0, node->m_table, LOCK_IX, thr);

      if (err != DB_SUCCESS) {

        goto error_handling;
      }
    }

    node->m_state = UPD_NODE_UPDATE_CLUSTERED;

    if (node->m_searched_update) {
      /* Reset the cursor */
      sel_node->m_state = SEL_NODE_OPEN;

      /* Fetch a row to update */

      thr->run_node = sel_node;

      return thr;
    }
  }

  /* sel_node is nullptr if we are in the client interface */

  if (sel_node != nullptr && sel_node->m_state != SEL_NODE_FETCH) {

    if (!node->m_searched_update) {
      /* An explicit cursor should be positioned on a row
      to update */

      ut_error;

      err = DB_ERROR;

      goto error_handling;
    }

    ut_ad(sel_node->m_state == SEL_NODE_NO_MORE_ROWS);

    /* No more rows to update, or the select node performed the
    updates directly in-place */

    thr->run_node = parent;

    return thr;
  }

  /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */

  err = update(node, thr);

error_handling:
  trx->m_error_state = err;

  if (err != DB_SUCCESS) {
    return nullptr;
  }

  /* DO THE TRIGGER ACTIONS HERE */

  if (node->m_searched_update) {
    /* Fetch next row to update */

    thr->run_node = sel_node;
  } else {
    /* It was an explicit cursor update */

    thr->run_node = parent;
  }

  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  return thr;
}

upd_node_t *Row_update::create_update_node(Table *table, mem_heap_t *heap) noexcept {
  auto node = node_create(heap);

  node->m_table = table;
  node->m_in_client_interface = true;
  node->m_pcur = new Btree_pcursor(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  node->m_has_clust_rec_x_lock = true;
  node->m_update_n_fields = table->get_n_cols();
  node->m_update = upd_create(table->get_n_cols(), heap);

  return node;
}
