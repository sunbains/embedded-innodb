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

/** @file trx/trx0rec.c
Transaction undo log record

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0rec.h"

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

/**
 * Writes the mtr log entry of the inserted undo log record on the undo log
 *
 * @param undo_page The undo log page.
 * @param old_free The start offset of the inserted entry.
 * @param new_free The end offset of the entry.
 * @param mtr The mtr (mini-transaction) object.
 */
inline void trx_undof_page_add_undo_rec_log(page_t *undo_page, ulint old_free, ulint new_free, mtr_t *mtr) noexcept {
  auto log_ptr = mlog_open(mtr, 11 + 13 + MLOG_BUF_MARGIN);

  if (log_ptr == nullptr) {

    return;
  }

  auto log_end = &log_ptr[11 + 13 + MLOG_BUF_MARGIN];
  log_ptr = mlog_write_initial_log_record_fast(undo_page, MLOG_UNDO_INSERT, log_ptr, mtr);
  auto len = new_free - old_free - 4;

  mach_write_to_2(log_ptr, len);
  log_ptr += 2;

  if (log_ptr + len <= log_end) {
    memcpy(log_ptr, undo_page + old_free + 2, len);
    mlog_close(mtr, log_ptr + len);
  } else {
    mlog_close(mtr, log_ptr);
    mlog_catenate_string(mtr, undo_page + old_free + 2, len);
  }
}

byte *trx_undo_parse_add_undo_rec(byte *ptr, byte *end_ptr, page_t *page) {
  ulint len;
  byte *rec;
  ulint first_free;

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  len = mach_read_from_2(ptr);
  ptr += 2;

  if (end_ptr < ptr + len) {

    return nullptr;
  }

  if (page == nullptr) {

    return ptr + len;
  }

  first_free = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  rec = page + first_free;

  mach_write_to_2(rec, first_free + 4 + len);
  mach_write_to_2(rec + 2 + len, first_free);

  mach_write_to_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, first_free + 4 + len);
  memcpy(rec + 2, ptr, len);

  return ptr + len;
}

/**
 * @brief Calculates the free space left for extending an undo log record.
 *
 * @param[in] page Undo log page
 * @param[in] ptr Pointer to page
 *
 * @return ulint Bytes left
 */
inline ulint trx_undo_left(const page_t *page, const byte *ptr) noexcept {
  /* The '- 10' is a safety margin, in case we have some small
  calculation error below */

  return UNIV_PAGE_SIZE - (ptr - page) - 10 - FIL_PAGE_DATA_END;
}

/**
 * @brief Set the next and previous pointers in the undo page for the undo record
 * that was written to ptr. Update the first free value by the number of bytes
 * written for this undo record.
 *
 * @param[in,out] undo_page Undo log page
 * @param[in] ptr Pointer up to where data has been written on this undo page
 * @param[in] mtr Mini-transaction
 *
 * @return Offset of the inserted entry on the page if succeeded, 0 if fail
 */
static ulint trx_undo_page_set_next_prev_and_add(page_t *undo_page, byte *ptr, mtr_t *mtr) noexcept {
  ut_ad(ptr > undo_page);
  ut_ad(ptr < undo_page + UNIV_PAGE_SIZE);

  if (unlikely(trx_undo_left(undo_page, ptr) < 2)) {

    return 0;
  }

  /* pointer within undo_page that points to the next free offset value within undo_page.*/
  auto ptr_to_first_free = undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE;

  /* Read the first free offset value from the undo page header */
  auto first_free = mach_read_from_2(ptr_to_first_free);

  /* Write offset of the previous undo log record */
  mach_write_to_2(ptr, first_free);
  ptr += 2;

  /* Calculate the end of the record */
  auto end_of_rec = ptr - undo_page;

  /* Write offset of the next undo log record */
  mach_write_to_2(undo_page + first_free, end_of_rec);

  /* Update the offset to first free undo record */
  mach_write_to_2(ptr_to_first_free, end_of_rec);

  /* Write this log entry to the UNDO log */
  trx_undof_page_add_undo_rec_log(undo_page, first_free, end_of_rec, mtr);

  return first_free;
}

/**
 * @brief Reports in the undo log of an insert of a clustered index record.
 *
 * @param[in] undo_page Undo log page
 * @param[in] trx Transaction
 * @param[in] index Clustered index
 * @param[in] clust_entry Index entry which will be inserted to the clustered index
 * @param[in] mtr Mini-transaction
 *
 * @return Offset of the inserted entry on the page if succeed, 0 if fail
 */
static ulint trx_undo_page_report_insert(page_t *undo_page, Trx *trx, const Index *index, const DTuple *clust_entry, mtr_t *mtr) {
  ulint first_free;
  byte *ptr;
  ulint i;

  ut_ad(index->is_clustered());
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT);

  first_free = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  ptr = undo_page + first_free;

  ut_ad(first_free <= UNIV_PAGE_SIZE);

  if (trx_undo_left(undo_page, ptr) < 2 + 1 + 11 + 11) {

    /* Not enough space for writing the general parameters */

    return 0;
  }

  /* Reserve 2 bytes for the pointer to the next undo log record */
  ptr += 2;

  /* Store first some general parameters to the undo log */
  *ptr = TRX_UNDO_INSERT_REC;
  ++ptr;

  ptr += mach_uint64_write_much_compressed(ptr, trx->m_undo_no);
  ptr += mach_uint64_write_much_compressed(ptr, index->m_table->m_id);
  /*----------------------------------------*/
  /* Store then the fields required to uniquely determine the record
  to be inserted in the clustered index */

  for (i = 0; i < index->get_n_unique(); i++) {

    const dfield_t *field = dtuple_get_nth_field(clust_entry, i);
    ulint flen = dfield_get_len(field);

    if (trx_undo_left(undo_page, ptr) < 5) {

      return 0;
    }

    ptr += mach_write_compressed(ptr, flen);

    if (flen != UNIV_SQL_NULL) {
      if (trx_undo_left(undo_page, ptr) < flen) {

        return 0;
      }

      memcpy(ptr, dfield_get_data(field), flen);
      ptr += flen;
    }
  }

  return trx_undo_page_set_next_prev_and_add(undo_page, ptr, mtr);
}

byte *trx_undo_rec_get_pars(trx_undo_rec_t *undo_rec, Undo_rec_pars &undo_rec_pars) noexcept {

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

byte *trx_undo_rec_get_row_ref(byte *ptr, Index *index, DTuple **ref, mem_heap_t *heap) {
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

byte *trx_undo_rec_skip_row_ref(byte *ptr, Index *index) {
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

/** Fetch a prefix of an externally stored column, for writing to the undo log
of an update or delete marking of a clustered index record.
@return	ext_buf */
static byte *trx_undo_page_fetch_ext(
  byte *ext_buf,     /*!< in: a buffer of
                       REC_MAX_INDEX_COL_LEN
                       + BTR_EXTERN_FIELD_REF_SIZE */
  const byte *field, /*!< in: an externally stored column */
  ulint *len
) /*!< in: length of field;
                       out: used length of ext_buf */
{
  /* Fetch the BLOB. */
  Blob blob(srv_fsp, srv_btree_sys);
  ulint ext_len = blob.copy_externally_stored_field_prefix(ext_buf, REC_MAX_INDEX_COL_LEN, field, *len);
  /* BLOBs should always be nonempty. */
  ut_a(ext_len);
  /* Append the BLOB pointer to the prefix. */
  memcpy(ext_buf + ext_len, field + *len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);
  *len = ext_len + BTR_EXTERN_FIELD_REF_SIZE;
  return ext_buf;
}

/** Writes to the undo log a prefix of an externally stored column.
@return	undo log position */
static byte *trx_undo_page_report_modify_ext(
  byte *ptr,          /*!< in: undo log position,
                        at least 15 bytes must be available */
  byte *ext_buf,      /*!< in: a buffer of
                        REC_MAX_INDEX_COL_LEN
                        + BTR_EXTERN_FIELD_REF_SIZE,
                        or nullptr when should not fetch
                        a longer prefix */
  const byte **field, /*!< in/out: the locally stored part of
                        the externally stored column */
  ulint *len
) /*!< in/out: length of field, in bytes */
{
  if (ext_buf) {
    /* If an ordering column is externally stored, we will
    have to store a longer prefix of the field.  In this
    case, write to the log a marker followed by the
    original length and the real length of the field. */
    ptr += mach_write_compressed(ptr, UNIV_EXTERN_STORAGE_FIELD);

    ptr += mach_write_compressed(ptr, *len);

    *field = trx_undo_page_fetch_ext(ext_buf, *field, len);

    ptr += mach_write_compressed(ptr, *len);
  } else {
    ptr += mach_write_compressed(ptr, UNIV_EXTERN_STORAGE_FIELD + *len);
  }

  return ptr;
}

/**
 * @brief Reports in the undo log of an update or delete marking of a clustered index record.
 *
 * @param[in] undo_page Undo log page
 * @param[in] trx Transaction
 * @param[in] index Clustered index where update or delete marking is done
 * @param[in] rec Clustered index record which has NOT yet been modified
 * @param[in] offsets Phy_rec::get_all_col_offsets(index, rec)
 * @param[in] update Update vector which tells the columns to be updated; in the case of a delete, this should be set to nullptr
 * @param[in] cmpl_info Compiler info on secondary index updates
 * @param[in] mtr Mini-transaction
 *
 * @return byte offset of the inserted undo log entry on the page if succeed, 0 if fail
 */
static ulint trx_undo_page_report_modify(
  page_t *undo_page, Trx *trx, const Index *index, const rec_t *rec, const ulint *offsets, const upd_t *update, ulint cmpl_info,
  mtr_t *mtr
) {
  byte *ptr;
  ulint flen;
  const byte *field;
  ulint type_cmpl;
  byte *type_cmpl_ptr;
  trx_id_t trx_id;
  bool ignore_prefix = false;
  byte ext_buf[REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE];

  ut_a(index->is_clustered());
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE);
  auto table = index->m_table;

  auto first_free = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  ptr = undo_page + first_free;

  ut_ad(first_free <= UNIV_PAGE_SIZE);

  if (trx_undo_left(undo_page, ptr) < 50) {

    /* NOTE: the value 50 must be big enough so that the general
    fields written below fit on the undo log page */

    return 0;
  }

  /* Reserve 2 bytes for the pointer to the next undo log record */
  ptr += 2;

  /* Store first some general parameters to the undo log */

  if (!update) {
    type_cmpl = TRX_UNDO_DEL_MARK_REC;
  } else if (rec_get_deleted_flag(rec)) {
    type_cmpl = TRX_UNDO_UPD_DEL_REC;
    /* We are about to update a delete marked record.
    We don't typically need the prefix in this case unless
    the delete marking is done by the same transaction
    (which we check below). */
    ignore_prefix = true;
  } else {
    type_cmpl = TRX_UNDO_UPD_EXIST_REC;
  }

  type_cmpl |= cmpl_info * TRX_UNDO_CMPL_INFO_MULT;
  type_cmpl_ptr = ptr;

  *ptr++ = (byte)type_cmpl;
  ptr += mach_uint64_write_much_compressed(ptr, trx->m_undo_no);

  ptr += mach_uint64_write_much_compressed(ptr, table->m_id);

  /*----------------------------------------*/
  /* Store the state of the info bits */

  *ptr++ = (byte)rec_get_info_bits(rec);

  /* Store the values of the system columns */
  field = rec_get_nth_field(rec, offsets, index->get_sys_col_field_pos(DATA_TRX_ID), &flen);
  ut_ad(flen == DATA_TRX_ID_LEN);

  trx_id = srv_trx_sys->read_trx_id(field);

  /* If it is an update of a delete marked record, then we are
  allowed to ignore blob prefixes if the delete marking was done
  by some other trx as it must have committed by now for us to
  allow an over-write. */
  if (ignore_prefix) {
    ignore_prefix = trx_id != trx->m_id;
  }

  ptr += mach_uint64_write_compressed(ptr, trx_id);

  field = rec_get_nth_field(rec, offsets, index->get_sys_col_field_pos(DATA_ROLL_PTR), &flen);
  ut_ad(flen == DATA_ROLL_PTR_LEN);

  const auto roll_ptr = trx_read_roll_ptr(field);

  ptr += mach_uint64_write_compressed(ptr, roll_ptr);

  /*----------------------------------------*/
  /* Store then the fields required to uniquely determine the
  record which will be modified in the clustered index */

  for (ulint i = 0; i < index->get_n_unique(); i++) {

    field = rec_get_nth_field(rec, offsets, i, &flen);

    /* The ordering columns must not be stored externally. */
    ut_ad(!rec_offs_nth_extern(offsets, i));
    ut_ad(index->get_nth_col(i)->m_ord_part);

    if (trx_undo_left(undo_page, ptr) < 5) {

      return 0;
    }

    ptr += mach_write_compressed(ptr, flen);

    if (flen != UNIV_SQL_NULL) {
      if (trx_undo_left(undo_page, ptr) < flen) {

        return 0;
      }

      memcpy(ptr, field, flen);
      ptr += flen;
    }
  }

  /*----------------------------------------*/
  /* Save to the undo log the old values of the columns to be updated. */

  if (update) {
    if (trx_undo_left(undo_page, ptr) < 5) {

      return 0;
    }

    ptr += mach_write_compressed(ptr, Row_update::upd_get_n_fields(update));

    for (ulint i = 0; i < Row_update::upd_get_n_fields(update); i++) {

      auto pos = update->get_nth_field(i)->m_field_no;

      /* Write field number to undo log */
      if (trx_undo_left(undo_page, ptr) < 5) {

        return 0;
      }

      ptr += mach_write_compressed(ptr, pos);

      /* Save the old value of field */
      field = rec_get_nth_field(rec, offsets, pos, &flen);

      if (trx_undo_left(undo_page, ptr) < 15) {

        return 0;
      }

      if (rec_offs_nth_extern(offsets, pos)) {
        ptr = trx_undo_page_report_modify_ext(
          ptr,
          index->get_nth_col(pos)->m_ord_part && !ignore_prefix && flen < REC_MAX_INDEX_COL_LEN ? ext_buf : nullptr,
          &field,
          &flen
        );

        /* Notify purge that it eventually has to
        free the old externally stored field */

        trx->m_update_undo->m_del_marks = true;

        *type_cmpl_ptr |= TRX_UNDO_UPD_EXTERN;
      } else {
        ptr += mach_write_compressed(ptr, flen);
      }

      if (flen != UNIV_SQL_NULL) {
        if (trx_undo_left(undo_page, ptr) < flen) {

          return 0;
        }

        memcpy(ptr, field, flen);
        ptr += flen;
      }
    }
  }

  /*----------------------------------------*/
  /* In the case of a delete marking, and also in the case of an update
  where any ordering field of any index changes, store the values of all
  columns which occur as ordering fields in any index. This info is used
  in the purge of old versions where we use it to build and search the
  delete marked index records, to look if we can remove them from the
  index tree. Note that starting from 4.0.14 also externally stored
  fields can be ordering in some index. Starting from 5.2, we no longer
  store REC_MAX_INDEX_COL_LEN first bytes to the undo log record,
  but we can construct the column prefix fields in the index by
  fetching the first page of the BLOB that is pointed to by the
  clustered index. This works also in crash recovery, because all pages
  (including BLOBs) are recovered before anything is rolled back. */

  if (!update || !(cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
    byte *old_ptr = ptr;

    trx->m_update_undo->m_del_marks = true;

    if (trx_undo_left(undo_page, ptr) < 5) {

      return 0;
    }

    /* Reserve 2 bytes to write the number of bytes the stored
    fields take in this undo record */

    ptr += 2;

    for (ulint col_no = 0; col_no < table->get_n_cols(); col_no++) {

      const Column *col = table->get_nth_col(col_no);

      if (col->m_ord_part) {
        /* Write field number to undo log */
        if (trx_undo_left(undo_page, ptr) < 5 + 15) {

          return 0;
        }

        auto pos = index->get_nth_field_pos(col_no);

        ptr += mach_write_compressed(ptr, pos);

        /* Save the old value of field */
        field = rec_get_nth_field(rec, offsets, pos, &flen);

        if (rec_offs_nth_extern(offsets, pos)) {
          ptr =
            trx_undo_page_report_modify_ext(ptr, flen < REC_MAX_INDEX_COL_LEN && !ignore_prefix ? ext_buf : nullptr, &field, &flen);
        } else {
          ptr += mach_write_compressed(ptr, flen);
        }

        if (flen != UNIV_SQL_NULL) {
          if (trx_undo_left(undo_page, ptr) < flen) {

            return 0;
          }

          memcpy(ptr, field, flen);
          ptr += flen;
        }
      }
    }

    mach_write_to_2(old_ptr, ptr - old_ptr);
  }

  /*----------------------------------------*/
  /* Write pointers to the previous and the next undo log records */
  if (trx_undo_left(undo_page, ptr) < 2) {

    return 0;
  }

  mach_write_to_2(ptr, first_free);
  ptr += 2;
  mach_write_to_2(undo_page + first_free, ptr - undo_page);

  mach_write_to_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, ptr - undo_page);

  /* Write to the REDO log about this change in the UNDO log */

  trx_undof_page_add_undo_rec_log(undo_page, first_free, ptr - undo_page, mtr);
  return first_free;
}

byte *trx_undo_update_rec_get_sys_cols(byte *ptr, trx_id_t *trx_id, roll_ptr_t *roll_ptr, ulint *info_bits) {
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

/** Reads from an update undo log record the number of updated fields.
@return	remaining part of undo log record after reading this value */
inline byte *trx_undo_update_rec_get_n_upd_fields(
  byte *ptr, /*!< in: pointer to remaining part of undo log record */
  ulint *n
) /*!< out: number of fields */
{
  *n = mach_read_compressed(ptr);
  ptr += mach_get_compressed_size(*n);

  return ptr;
}

/** Reads from an update undo log record a stored field number.
@return	remaining part of undo log record after reading this value */
inline byte *trx_undo_update_rec_get_field_no(
  byte *ptr, /*!< in: pointer to remaining part of undo log record */
  ulint *field_no
) /*!< out: field number */
{
  *field_no = mach_read_compressed(ptr);
  ptr += mach_get_compressed_size(*field_no);

  return ptr;
}

byte *trx_undo_update_rec_get_update(
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

byte *trx_undo_rec_get_partial_row(byte *ptr, Index *index, DTuple **row, bool ignore_prefix, mem_heap_t *heap) {
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

/**
 * @brief Erases the unused undo log page end.
 *
 * @param[in] undo_page Undo page whose end to erase
 * @param[in] mtr Mini-transaction
 */
static void trx_undo_erase_page_end(page_t *undo_page, mtr_t *mtr) {
  auto first_free = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);

  memset(undo_page + first_free, 0xff, (UNIV_PAGE_SIZE - FIL_PAGE_DATA_END) - first_free);

  mlog_write_initial_log_record(undo_page, MLOG_UNDO_ERASE_END, mtr);
}

byte *trx_undo_parse_erase_page_end(byte *ptr, byte *end_ptr __attribute__((unused)), page_t *page, mtr_t *mtr) {
  ut_ad(ptr && end_ptr);

  if (page == nullptr) {

    return ptr;
  }

  trx_undo_erase_page_end(page, mtr);

  return ptr;
}

db_err trx_undo_report_row_operation(
  ulint flags, ulint op_type, que_thr_t *thr, const Index *index, const DTuple *clust_entry, const upd_t *update, ulint cmpl_info,
  const rec_t *rec, roll_ptr_t *roll_ptr
) {
  trx_undo_t *undo;
  db_err err = DB_SUCCESS;
  mem_heap_t *heap = nullptr;
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();

  rec_offs_init(offsets_);

  ut_a(index->is_clustered());

  if (flags & BTR_NO_UNDO_LOG_FLAG) {

    *roll_ptr = 0;

    return DB_SUCCESS;
  }

  ut_ad(thr != nullptr);
  ut_ad(op_type != TRX_UNDO_INSERT_OP || (clust_entry && !update && !rec));

  auto trx = thr_get_trx(thr);
  auto rseg = trx->m_rseg;

  mutex_enter(&trx->m_undo_mutex);

  /* If the undo log is not assigned yet, assign one */

  if (op_type == TRX_UNDO_INSERT_OP) {

    if (trx->m_insert_undo == nullptr) {

      err = srv_undo->assign_undo(trx, TRX_UNDO_INSERT);
    }

    undo = trx->m_insert_undo;

    if (unlikely(!undo)) {
      /* Did not succeed */
      mutex_exit(&trx->m_undo_mutex);

      return err;
    }
  } else {
    ut_ad(op_type == TRX_UNDO_MODIFY_OP);

    if (trx->m_update_undo == nullptr) {

      err = srv_undo->assign_undo(trx, TRX_UNDO_UPDATE);
    }

    undo = trx->m_update_undo;

    if (unlikely(!undo)) {
      /* Did not succeed */
      mutex_exit(&trx->m_undo_mutex);
      return err;
    }

    {
      Phy_rec record(index, rec);

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }
  }

  auto page_no = undo->m_last_page_no;

  mtr_t mtr;

  mtr.start();

  for (;;) {

    Buf_pool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {undo->m_space, page_no},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    auto undo_block = srv_buf_pool->get(req, undo->m_guess_block);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(undo_block, SYNC_TRX_UNDO_PAGE));

    auto undo_page = undo_block->get_frame();

    ulint offset;

    if (op_type == TRX_UNDO_INSERT_OP) {
      offset = trx_undo_page_report_insert(undo_page, trx, index, clust_entry, &mtr);
    } else {
      offset = trx_undo_page_report_modify(undo_page, trx, index, rec, offsets, update, cmpl_info, &mtr);
    }

    if (unlikely(offset == 0)) {
      /* The record did not fit on the page. We erase the
      end segment of the undo log page and write a log
      record of it: this is to ensure that in the debug
      version the replicate page constructed using the log
      records stays identical to the original page */

      trx_undo_erase_page_end(undo_page, &mtr);
      mtr.commit();
    } else {
      /* Success */

      mtr.commit();

      undo->m_empty = false;
      undo->m_top_page_no = page_no;
      undo->m_top_offset = offset;
      undo->m_top_undo_no = trx->m_undo_no;
      undo->m_guess_block = undo_block;

      ++trx->m_undo_no;

      mutex_exit(&trx->m_undo_mutex);

      *roll_ptr = trx_undo_build_roll_ptr(op_type == TRX_UNDO_INSERT_OP, rseg->m_id, page_no, offset);

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }

      return DB_SUCCESS;
    }

    ut_ad(page_no == undo->m_last_page_no);

    /* We have to extend the undo log by one page */

    mtr.start();

    /* When we add a page to an undo log, this is analogous to
    a pessimistic insert in a B-tree, and we must reserve the
    counterpart of the tree latch, which is the rseg mutex. */

    mutex_enter(&rseg->m_mutex);

    page_no = srv_undo->add_page(trx, undo, &mtr);

    mutex_exit(&rseg->m_mutex);

    if (unlikely(page_no == FIL_NULL)) {
      /* Did not succeed: out of space */

      mutex_exit(&trx->m_undo_mutex);

      mtr.commit();

      if (likely_null(heap)) {
        mem_heap_free(heap);
      }

      return DB_OUT_OF_FILE_SPACE;
    }
  }
}

trx_undo_rec_t *trx_undo_get_undo_rec_low(roll_ptr_t roll_ptr, mem_heap_t *heap) {
  ulint offset;
  ulint rseg_id;
  bool is_insert;
  page_no_t page_no;

  trx_undo_decode_roll_ptr(roll_ptr, &is_insert, &rseg_id, &page_no, &offset);
  auto rseg = Trx_rseg::get_on_id(rseg_id);

  mtr_t mtr;

  mtr.start();

  auto undo_page = srv_undo->page_get_s_latched(rseg->m_space, page_no, &mtr);

  auto undo_rec = trx_undo_rec_copy(undo_page + offset, heap);

  mtr.commit();

  return undo_rec;
}

db_err trx_undo_get_undo_rec(roll_ptr_t roll_ptr, trx_id_t trx_id, trx_undo_rec_t **undo_rec, mem_heap_t *heap) {
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&purge_sys->latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  if (!srv_trx_sys->m_purge->update_undo_must_exist(trx_id)) {

    /* It may be that the necessary undo log has already been
    deleted */

    return DB_MISSING_HISTORY;
  }

  *undo_rec = trx_undo_get_undo_rec_low(roll_ptr, heap);

  return DB_SUCCESS;
}

db_err trx_undo_prev_version_build(
  const rec_t *index_rec, mtr_t *index_mtr __attribute__((unused)), const rec_t *rec, Index *index, ulint *offsets,
  mem_heap_t *heap, rec_t **old_vers
) {
  byte *buf;
  upd_t *update;
  DTuple *entry;
  ulint info_bits;
  trx_id_t trx_id;
  trx_id_t rec_trx_id;
  roll_ptr_t roll_ptr;
  roll_ptr_t old_roll_ptr;
  trx_undo_rec_t *undo_rec{};

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&purge_sys->m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  ut_ad(
    index_mtr->memo_contains_page(index_rec, MTR_MEMO_PAGE_S_FIX) || index_mtr->memo_contains_page(index_rec, MTR_MEMO_PAGE_X_FIX)
  );

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (!index->is_clustered()) {
    log_err(std::format(
      "Trying to access update undo rec for non-clustered index {}"
      " Submit a detailed bug report, check the Embedded InnoDB website for details"
      "index record ",
      index->m_name
    ));
    log_err(rec_to_string(index_rec));
    log_err("record version ");
    log_err(rec_to_string(rec));

    return DB_ERROR;
  }

  roll_ptr = row_get_rec_roll_ptr(rec, index, offsets);
  old_roll_ptr = roll_ptr;

  *old_vers = nullptr;

  if (trx_undo_roll_ptr_is_insert(roll_ptr)) {

    /* The record rec is the first inserted version */

    return DB_SUCCESS;
  }

  rec_trx_id = row_get_rec_trx_id(rec, index, offsets);

  auto err = trx_undo_get_undo_rec(roll_ptr, rec_trx_id, &undo_rec, heap);

  if (unlikely(err != DB_SUCCESS)) {
    /* The undo record may already have been purged.
    This should never happen in InnoDB. */

    return err;
  }

  Undo_rec_pars pars;
  auto ptr = trx_undo_rec_get_pars(undo_rec, pars);

  ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);

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

  ptr = trx_undo_rec_skip_row_ref(ptr, index);

  ptr = trx_undo_update_rec_get_update(ptr, index, pars.m_type, trx_id, roll_ptr, info_bits, nullptr, heap, &update);

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
    log_warn(rec_to_string(index_rec));
    log_warn("record version ");
    log_warn(rec_to_string(rec));
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

    *old_vers = rec_convert_dtuple_to_rec(buf, index, entry, n_ext);

  } else {

    buf = mem_heap_alloc(heap, rec_offs_size(offsets));
    *old_vers = rec_copy(buf, rec, offsets);
    ut_d(rec_offs_make_valid(*old_vers, index, offsets));
    srv_row_upd->rec_in_place(*old_vers, index, offsets, update);
  }

  return DB_SUCCESS;
}
