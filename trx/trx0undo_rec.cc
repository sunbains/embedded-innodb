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

/** @file trx/trx0undo_rec.cc
Transaction undo log record class implementation

*******************************************************/

#include "trx0undo_rec.h"

#include "btr0blob.h"
#include "dict0dict.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "que0que.h"
#include "row0ext.h"
#include "row0row.h"
#include "row0upd.h"
#include "trx0purge.h"
#include "trx0rseg.h"
#include "trx0undo.h"
#include "ut0mem.h"

// FIXME: There must be a better way
// Forward declarations to avoid circular dependency
extern byte *trx_undo_parse_add_undo_rec(byte *ptr, byte *end_ptr, Raw_page page);

extern byte *trx_undo_parse_erase_page_end(byte *ptr, byte *end_ptr, Raw_page page, mtr_t *mtr);

extern db_err trx_undo_report_row_operation(
  ulint flags, ulint op_type, que_thr_t *thr, const Index *index, const DTuple *clust_entry, const upd_t *update, ulint cmpl_info,
  const Rec rec, roll_ptr_t *roll_ptr
);

byte *Trx_undo_record::get_pars(Trx_undo_record::Parsed &undo_rec_pars) const noexcept {
  return get_pars(m_undo_rec, undo_rec_pars);
}

byte *Trx_undo_record::get_pars(trx_undo_rec_t undo_rec, Trx_undo_record::Parsed &undo_rec_pars) noexcept {

  auto ptr = undo_rec + 2;
  ulint type_cmpl = mach_read_from_1(ptr);

  ptr++;

  if (type_cmpl & TRX_UNDO_UPD_EXTERN) {
    undo_rec_pars.m_extern = true;
    type_cmpl -= TRX_UNDO_UPD_EXTERN;
  } else {
    undo_rec_pars.m_extern = false;
  }

  undo_rec_pars.m_type = type_cmpl & (TRX_UNDO_CMPL_INFO_MULT - 1);
  undo_rec_pars.m_cmpl_info = type_cmpl / TRX_UNDO_CMPL_INFO_MULT;

  undo_rec_pars.m_undo_no = mach_uint64_read_much_compressed(ptr);
  ptr += mach_uint64_get_much_compressed_size(undo_rec_pars.m_undo_no);

  undo_rec_pars.m_table_id = mach_uint64_read_much_compressed(ptr);
  ptr += mach_uint64_get_much_compressed_size(undo_rec_pars.m_table_id);

  return ptr;
}

/**
 * @brief Reads from an undo log record a stored column value.
 *
 * @param[in] ptr Pointer to remaining part of undo log record.
 * @param[out] field Pointer to stored field.
 * @param[out] len Length of the field, or UNIV_SQL_NULL.
 * @param[out] orig_len Original length of the locally stored part of an externally stored column, or 0.
 *
 * @return Remaining part of undo log record after reading these values.
 */
static byte *trx_undo_rec_get_col_val(byte *ptr, byte **field, ulint *len, ulint *orig_len) {
  *len = mach_read_compressed(ptr);
  ptr += mach_get_compressed_size(*len);

  *orig_len = 0;

  switch (*len) {
    case UNIV_SQL_NULL:
      *field = nullptr;
      break;
    case UNIV_EXTERN_STORAGE_FIELD:
      *orig_len = mach_read_compressed(ptr);
      ptr += mach_get_compressed_size(*orig_len);
      *len = mach_read_compressed(ptr);
      ptr += mach_get_compressed_size(*len);
      *field = ptr;
      ptr += *len;

      ut_ad(*orig_len >= BTR_EXTERN_FIELD_REF_SIZE);
      ut_ad(*len > *orig_len);
      /* @see dtuple_convert_big_rec() */
      ut_ad(*len >= BTR_EXTERN_FIELD_REF_SIZE * 2);
      /* we do not have access to index->table here
      ut_ad(dict_table_get_format(index->table) >= DICT_TF_FORMAT_V1
          || *len >= REC_MAX_INDEX_COL_LEN
          + BTR_EXTERN_FIELD_REF_SIZE);
      */

      *len += UNIV_EXTERN_STORAGE_FIELD;
      break;
    default:
      *field = ptr;
      if (*len >= UNIV_EXTERN_STORAGE_FIELD) {
        ptr += *len - UNIV_EXTERN_STORAGE_FIELD;
      } else {
        ptr += *len;
      }
  }

  return ptr;
}

byte *Trx_undo_record::get_row_ref(byte *ptr, Index *index, DTuple **ref, mem_heap_t *heap) {
  ulint ref_len;
  ulint i;

  ut_ad(index && ptr && ref && heap);
  ut_a(index->is_clustered());

  ref_len = index->get_n_unique();

  *ref = dtuple_create(heap, ref_len);

  index->copy_types(*ref, ref_len);

  for (i = 0; i < ref_len; i++) {
    dfield_t *dfield;
    byte *field;
    ulint len;
    ulint orig_len;

    dfield = dtuple_get_nth_field(*ref, i);

    ptr = trx_undo_rec_get_col_val(ptr, &field, &len, &orig_len);

    dfield_set_data(dfield, field, len);
  }

  return ptr;
}

byte *Trx_undo_record::skip_row_ref(byte *ptr, Index *index) {
  ulint ref_len;
  ulint i;

  ut_ad(index && ptr);
  ut_a(index->is_clustered());

  ref_len = index->get_n_unique();

  for (i = 0; i < ref_len; i++) {
    byte *field;
    ulint len;
    ulint orig_len;

    ptr = trx_undo_rec_get_col_val(ptr, &field, &len, &orig_len);
  }

  return ptr;
}

byte *Trx_undo_record::update_rec_get_sys_cols(byte *ptr, trx_id_t *trx_id, roll_ptr_t *roll_ptr, ulint *info_bits) {
  /* Read the state of the info bits */
  *info_bits = mach_read_from_1(ptr);
  ptr += 1;

  /* Read the values of the system columns */

  *trx_id = mach_uint64_read_compressed(ptr);
  ptr += mach_uint64_get_compressed_size(*trx_id);

  *roll_ptr = mach_uint64_read_compressed(ptr);
  ptr += mach_uint64_get_compressed_size(*roll_ptr);

  return ptr;
}

/**
 * @brief Reads from an update undo log record the number of updated fields.
 *
 * @param[in] ptr Pointer to remaining part of undo log record.
 * @param[out] n Number of fields.
 *
 * @return Remaining part of undo log record after reading this value.
 */
inline byte *trx_undo_update_rec_get_n_upd_fields(byte *ptr, ulint *n) /*!< out: number of fields */
{
  *n = mach_read_compressed(ptr);
  ptr += mach_get_compressed_size(*n);

  return ptr;
}

/**
 * @brief Reads from an update undo log record a stored field number.
 *
 * @param[in] ptr Pointer to remaining part of undo log record.
 * @param[out] field_no Field number.
 *
 * @return Remaining part of undo log record after reading this value.
 */
inline byte *trx_undo_update_rec_get_field_no(
  byte *ptr, /*!< in: pointer to remaining part of undo log record */
  ulint *field_no
) /*!< out: field number */
{
  *field_no = mach_read_compressed(ptr);
  ptr += mach_get_compressed_size(*field_no);

  return ptr;
}

byte *Trx_undo_record::update_rec_get_update(
  byte *ptr, Index *index, ulint type, trx_id_t trx_id, roll_ptr_t roll_ptr, ulint info_bits, Trx *trx, mem_heap_t *heap,
  upd_t **upd
) {
  ulint n_fields;
  byte *buf;

  ut_a(index->is_clustered());

  if (type != TRX_UNDO_DEL_MARK_REC) {
    ptr = trx_undo_update_rec_get_n_upd_fields(ptr, &n_fields);
  } else {
    n_fields = 0;
  }

  auto update = Row_update::upd_create(n_fields + 2, heap);

  update->m_info_bits = info_bits;

  /* Store first trx id and roll ptr to update vector */

  auto upd_field = update->get_nth_field(n_fields);
  buf = mem_heap_alloc(heap, DATA_TRX_ID_LEN);
  srv_trx_sys->write_trx_id(buf, trx_id);

  upd_field->set_field_no(index->get_sys_col_field_pos(DATA_TRX_ID), index, trx);
  dfield_set_data(&upd_field->m_new_val, buf, DATA_TRX_ID_LEN);

  upd_field = update->get_nth_field(n_fields + 1);
  buf = mem_heap_alloc(heap, DATA_ROLL_PTR_LEN);
  trx_write_roll_ptr(buf, roll_ptr);

  upd_field->set_field_no(index->get_sys_col_field_pos(DATA_ROLL_PTR), index, trx);
  dfield_set_data(&upd_field->m_new_val, buf, DATA_ROLL_PTR_LEN);

  /* Store then the updated ordinary columns to the update vector */

  for (ulint i{}; i < n_fields; ++i) {

    ulint len;
    byte *field;
    ulint field_no;

    ptr = trx_undo_update_rec_get_field_no(ptr, &field_no);

    if (field_no >= index->get_n_fields()) {
      log_err(std::format(
        "Trying to access update undo rec field {} in {} but index has only {} fields."
        " Submit a detailed bug report, check the Embedded InnoDB website for details."
        " : n_fields = {}, i = {}, ptr {}",
        field_no,
        index->m_name,
        index->get_n_fields(),
        n_fields,
        i,
        (void *)ptr
      ));

      return nullptr;
    }

    upd_field = update->get_nth_field(i);

    upd_field->set_field_no(field_no, index, trx);

    ulint orig_len;

    ptr = trx_undo_rec_get_col_val(ptr, &field, &len, &orig_len);

    upd_field->m_orig_len = orig_len;

    if (len == UNIV_SQL_NULL) {
      dfield_set_null(&upd_field->m_new_val);
    } else if (len < UNIV_EXTERN_STORAGE_FIELD) {
      dfield_set_data(&upd_field->m_new_val, field, len);
    } else {
      len -= UNIV_EXTERN_STORAGE_FIELD;

      dfield_set_data(&upd_field->m_new_val, field, len);
      dfield_set_ext(&upd_field->m_new_val);
    }
  }

  *upd = update;

  return ptr;
}

byte *Trx_undo_record::get_partial_row(byte *ptr, Index *index, DTuple **row, bool ignore_prefix, mem_heap_t *heap) {
  ut_ad(index);
  ut_ad(ptr);
  ut_ad(row);
  ut_ad(heap);
  ut_ad(index->is_clustered());

  auto row_len = index->m_table->get_n_cols();

  *row = dtuple_create(heap, row_len);

  index->m_table->copy_types(*row);

  auto end_ptr = ptr + mach_read_from_2(ptr);

  ptr += 2;

  while (ptr != end_ptr) {
    ulint field_no;
    ulint len;
    ulint orig_len;

    ptr = trx_undo_update_rec_get_field_no(ptr, &field_no);

    auto col = index->get_nth_col(field_no);

    auto col_no = col->get_no();

    byte *field;

    ptr = trx_undo_rec_get_col_val(ptr, &field, &len, &orig_len);

    auto dfield = dtuple_get_nth_field(*row, col_no);

    dfield_set_data(dfield, field, len);

    if (len != UNIV_SQL_NULL && len >= UNIV_EXTERN_STORAGE_FIELD) {
      dfield_set_len(dfield, len - UNIV_EXTERN_STORAGE_FIELD);
      dfield_set_ext(dfield);
      /* If the prefix of this column is indexed,
      ensure that enough prefix is stored in the
      undo log record. */
      if (!ignore_prefix && col->m_ord_part) {
        ut_a(dfield_get_len(dfield) >= 2 * BTR_EXTERN_FIELD_REF_SIZE);
        ut_a(dfield_get_len(dfield) >= REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE);
      }
    }
  }

  return ptr;
}

Trx_undo_record Trx_undo_record::get_undo_rec_low(roll_ptr_t roll_ptr, mem_heap_t *heap) {
  ulint offset;
  ulint rseg_id;
  bool is_insert;
  page_no_t page_no;

  trx_undo_decode_roll_ptr(roll_ptr, &is_insert, &rseg_id, &page_no, &offset);
  auto rseg = Trx_rseg::get_on_id(rseg_id);

  mtr_t mtr;

  mtr.start();

  auto undo_page = srv_undo->page_get_s_latched(rseg->m_space, page_no, &mtr);

  auto undo_rec = Trx_undo_record(undo_page + offset).copy(heap);

  mtr.commit();

  return undo_rec;
}

db_err Trx_undo_record::get_undo_rec(roll_ptr_t roll_ptr, trx_id_t trx_id, Trx_undo_record *undo_rec, mem_heap_t *heap) {
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&purge_sys->latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  if (!srv_trx_sys->m_purge->update_undo_must_exist(trx_id)) {

    /* It may be that the necessary undo log has already been
    deleted */

    return DB_MISSING_HISTORY;
  }

  *undo_rec = get_undo_rec_low(roll_ptr, heap);

  return DB_SUCCESS;
}

db_err Trx_undo_record::prev_version_build(
  const Rec index_rec, mtr_t *index_mtr __attribute__((unused)), const Rec rec, Index *index, ulint *offsets,
  mem_heap_t *heap, Rec *old_vers
) {
  byte *buf;
  upd_t *update;
  DTuple *entry;
  ulint info_bits;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;
  roll_ptr_t old_roll_ptr;
  Trx_undo_record undo_rec{};

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&purge_sys->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  ut_ad(
    index_mtr->memo_contains_page(index_rec, MTR_MEMO_PAGE_S_FIX) || index_mtr->memo_contains_page(index_rec, MTR_MEMO_PAGE_X_FIX)
  );

  ut_ad(rec.offs_validate(index, offsets));

  if (!index->is_clustered()) {
    log_err(std::format(
      "Trying to access update undo rec for non-clustered index {}"
      " Submit a detailed bug report, check the Embedded InnoDB website for details"
      "index record ",
      index->m_name
    ));
    log_err(index_rec.to_string());
    log_err("record version ");
    log_err(rec.to_string());

    return DB_ERROR;
  }

  roll_ptr = row_get_rec_roll_ptr(rec, index, offsets);
  old_roll_ptr = roll_ptr;

  *old_vers = nullptr;

  if (trx_undo_roll_ptr_is_insert(roll_ptr)) {

    /* The record rec is the first inserted version */

    return DB_SUCCESS;
  }

  auto rec_trx_id = row_get_rec_trx_id(rec, index, offsets);

  auto err = get_undo_rec(roll_ptr, rec_trx_id, &undo_rec, heap);

  if (unlikely(err != DB_SUCCESS)) {
    /* The undo record may already have been purged.
    This should never happen in InnoDB. */

    return err;
  }

  Trx_undo_record::Parsed pars;
  auto ptr = undo_rec.get_pars(pars);

  ptr = update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);

  /* (a) If a clustered index record version is such that the
  trx id stamp in it is bigger than purge_sys->view, then the
  BLOBs in that version are known to exist (the purge has not
  progressed that far);

  (b) if the version is the first version such that trx id in it
  is less than purge_sys->view, and it is not delete-marked,
  then the BLOBs in that version are known to exist (the purge
  cannot have purged the BLOBs referenced by that version
  yet).

  This function does not fetch any BLOBs.  The callers might, by
  possibly invoking row_ext_create() via row_build().  However,
  they should have all needed information in the *old_vers
  returned by this function.  This is because *old_vers is based
  on the transaction undo log records.  The function
  trx_undo_page_fetch_ext() will write BLOB prefixes to the
  transaction undo log that are at least as long as the longest
  possible column prefix in a secondary index.  Thus, secondary
  index entries for *old_vers can be constructed without
  dereferencing any BLOB pointers. */

  ptr = skip_row_ref(ptr, index);

  ptr = update_rec_get_update(ptr, index, pars.m_type, trx_id, roll_ptr, info_bits, nullptr, heap, &update);

  if (pars.m_table_id != index->m_table->m_id) {
    ptr = nullptr;

    log_err(std::format(
      "Trying to access update undo rec for table {} but the table id in the"
      " undo record is wrong. Submit a detailed bug report, check Embedded InnoDB"
      " website for details. Run also CHECK TABLE {}",
      index->m_table->m_name,
      index->m_table->m_name
    ));
  }

  if (ptr == nullptr) {
    /* The record was corrupted, return an error; these printfs
    should catch an elusive bug in row_vers_old_has_index_entry */

    log_err(std::format(
      "Table {}, index {}, n_uniq {} undo rec address {}, type {}, cmpl_info {}, undo rec table id {}, index table id {}",
      " dump of 150 bytes in undo rec: ",
      index->m_table->m_name,
      index->m_name,
      index->get_n_unique(),
      (void *)undo_rec,
      pars.m_type,
      pars.m_cmpl_info,
      pars.m_table_id,
      index->m_table->m_id
    ));

    log_warn_buf(undo_rec, 150);
    log_warn("index record ");
    log_warn(index_rec.to_string());
    log_warn("record version ");
    log_warn(rec.to_string());
    log_warn(std::format(
      "Record trx id {}, update rec trx id {} Roll ptr in rec {}, in update rec {}", rec_trx_id, trx_id, old_roll_ptr, roll_ptr
    ));

    log_info(srv_trx_sys->m_purge->to_string());

    return DB_ERROR;
  }

  if (srv_row_upd->changes_field_size_or_external(index, offsets, update)) {
    ulint n_ext;

    /* We have to set the appropriate extern storage bits in the
    old version of the record: the extern bits in rec for those
    fields that update does NOT update, as well as the bits for
    those fields that update updates to become externally stored
    fields. Store the info: */

    entry = row_rec_to_index_entry(ROW_COPY_DATA, rec, index, offsets, &n_ext, heap);

    Blob blob(srv_fsp, srv_btree_sys);

    n_ext += blob.push_update_extern_fields(entry, update, heap);

    /* The page containing the clustered index record
    corresponding to entry is latched in mtr.  Thus the
    following call is safe. */
    srv_row_upd->index_replace_new_col_vals(entry, index, update, heap);

    buf = mem_heap_alloc(heap, rec_get_converted_size(index, entry, n_ext));

    *old_vers = Rec::convert_dtuple_to_rec(buf, index, entry, n_ext);

  } else {

    buf = mem_heap_alloc(heap, rec_offs_size(offsets));
    *old_vers = rec.copy(buf, offsets);
    ut_d(old_vers->offs_make_valid(index, offsets));
    srv_row_upd->rec_in_place(*old_vers, index, offsets, update);
  }

  return DB_SUCCESS;
}

// Note: These functions delegate to the original implementations in trx0rec.cc
// as they deal with page-level operations and are closely tied to existing undo log page management

db_err Trx_undo_record::report_row_operation(
  ulint flags, ulint op_type, que_thr_t *thr, const Index *index, const DTuple *clust_entry, const upd_t *update, ulint cmpl_info,
  const Rec rec, roll_ptr_t *roll_ptr
) {
  return ::trx_undo_report_row_operation(flags, op_type, thr, index, clust_entry, update, cmpl_info, rec, roll_ptr);
}

byte *Trx_undo_record::parse_add_undo_rec(byte *ptr, byte *end_ptr, page_t *page) {
  return ::trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
}

byte *Trx_undo_record::parse_erase_page_end(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr) {
  return ::trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);
}
