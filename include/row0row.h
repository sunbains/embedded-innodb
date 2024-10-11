/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/row0row.h
General row routines

Created 4/20/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0types.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "read0types.h"
#include "rem0types.h"
#include "row0types.h"
#include "trx0types.h"
#include "dict0dict.h"
#include "rem0rec.h"
#include "trx0undo.h"

constexpr ulint ROW_COPY_DATA = 1;
constexpr ulint ROW_COPY_POINTERS = 2;

/**
 * @brief Gets the offset of the trx id field, in bytes relative to the origin of
 * a clustered index record.
 * 
 * @param[in] rec Record
 * @param[in] index Clustered index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
 * 
 * @return Offset of DATA_TRX_ID
 */
ulint row_get_trx_id_offset(const rec_t *rec, const Index *index, const ulint *offsets);

/**
 * @brief Reads the trx id field from a clustered index record.
 * 
 * @param[in] rec Record
 * @param[in] index Clustered index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
 * 
 * @return Value of the trx id field
 */
inline trx_id_t row_get_rec_trx_id(const rec_t *rec, const Index *index, const ulint *offsets);

/**
 * @brief Reads the roll pointer field from a clustered index record.
 * 
 * @param[in] rec Record
 * @param[in] index Clustered index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
 * 
 * @return Value of the roll pointer field
 */
inline roll_ptr_t row_get_rec_roll_ptr(const rec_t *rec, Index *index, const ulint *offsets);

/**
 * @brief When an insert or purge to a table is performed, this function builds
 * the entry to be inserted into or purged from an index on the table.
 * 
 * @param[in] row Row which should be inserted or purged.
 * @param[in] ext Externally stored column prefixes, or NULL.
 * @param[in] index Index on the table.
 * @param[in] heap Memory heap from which the memory for the index entry is allocated.
 * 
 * @return Index entry which should be inserted or purged, or NULL if the
 * externally stored columns in the clustered index record are
 * unavailable and ext != NULL.
 */
DTuple *row_build_index_entry(const DTuple *row, row_ext_t *ext, const Index *index, mem_heap_t *heap);

/**
 * @brief An inverse function to row_build_index_entry. Builds a row from a
 * record in a clustered index.
 * 
 * @param[in] type ROW_COPY_POINTERS or ROW_COPY_DATA; the latter copies also the data fields to heap while the first only
 *   places pointers to data fields on the index page, and thus is more efficient
 * 
 * @param[in] index Clustered index
 * 
 * @param[in] rec Record in the clustered index; NOTE: in the case ROW_COPY_POINTERS the data fields in the row will point
 *   directly into this record, therefore, the buffer page of this record must be at least s-latched and the latch held
 *   as long as the row dtuple is used!
 * 
 * @param[in] offsets Phy_rec::get_col_offsets(rec,index) or NULL, in which case this function
 *   will invoke Phy_rec::get_col_offsets()
 * 
 * @param[in] col_table Table, to check which * externally stored columns * occur in the ordering columns
 *   of an index, or NULL if * index->table should be * consulted instead; the user * columns in this table should be
 *   the same columns as in index->table
 * 
 * @param[out] ext Cache of externally stored column prefixes, or NULL
 * @param[in] heap Memory heap from which the memory needed is allocated
 * 
 * @return own: row built; see the NOTE below!
 */
DTuple *row_build(ulint type, const Index *index, const rec_t *rec, const ulint *offsets, const Table *col_table, row_ext_t **ext, mem_heap_t *heap);

/**
 * @brief Converts an index record to a typed data tuple.
 * 
 * @param[in] rec Record in the index.
 * @param[in] index Index.
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index).
 * @param[out] n_ext Number of externally stored columns.
 * @param[in] heap Memory heap from which the memory needed is allocated.
 * 
 * @return Index entry built; does not set info_bits, and the data fields in the entry will point directly to rec.
 */
DTuple *row_rec_to_index_entry_low(const rec_t *rec, const Index *index, const ulint *offsets, ulint *n_ext, mem_heap_t *heap);

/**
 * @brief Converts an index record to a typed data tuple. 
 * 
 * NOTE that externally stored (often big) fields are NOT copied to heap.
 * 
 * @param[in] type ROW_COPY_DATA, or ROW_COPY_POINTERS: the former copies also the data fields
 *   to heap as the latter only places pointers to data fields on the index page
 * 
 * @param[in] rec Record in the index; NOTE: in the case ROW_COPY_POINTERS the data fields in
 *   the row will point directly into this record, therefore, the buffer page of this record must
 *   be at least s-latched and the latch held as long as the dtuple is used!
 * 
 * @param[in] index Index
 * @param[in,out] offsets Phy_rec::get_col_offsets(rec)
 * @param[out] n_ext Number of externally stored columns
 * @param[in] heap Memory heap from which the memory needed is allocated
 * 
 * @return Index entry built; see the NOTE below!
 */
DTuple *row_rec_to_index_entry(ulint type, const rec_t *rec, const Index *index, ulint *offsets, ulint *n_ext, mem_heap_t *heap);

/**
 * @brief Builds from a secondary index record a row reference with which we can
 * search the clustered index record.
 * 
 * @param[in] type ROW_COPY_DATA, or ROW_COPY_POINTERS: the former copies also the data fields to
 *  heap, whereas the latter only places pointers to data fields on the index page.
 * 
 * @param[in] index Secondary index.
 * 
 * @param[in] rec Record in the index; NOTE: in the case ROW_COPY_POINTERS the data fields in the row will point
 *  directly into this record, therefore, the buffer page of this record must be at least s-latched
 *  and the latch held as long as the row reference is used.
 * 
 * @param[in] heap Memory heap from which the memory needed is allocated.
 * 
 * @return DTuple* Row reference built; see the NOTE below.
 */
DTuple *row_build_row_ref(ulint type, const Index *index, const rec_t *rec, mem_heap_t *heap);

/**
 * @brief Builds from a secondary index record a row reference with which we can
 * search the clustered index record.
 * 
 * @param[in,out] ref Row reference built; see the NOTE below!
 * 
 * @param[in] rec Record in the index; NOTE: the data fields in ref will point directly into this
 *  record, therefore, the buffer page of this record must be at least s-latched and
 *  the latch held as long as the row reference is used!
 * 
 * @param[in] index Secondary index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index) or NULL
 * @param[in] trx Transaction
 */
void row_build_row_ref_in_tuple(DTuple *ref, const rec_t *rec, const Index *index, ulint *offsets, trx_t *trx);

/**
 * @brief Builds from a secondary index record a row reference with which we can
 * search the clustered index record.
 * 
 * @param[in,out] ref Typed data tuple where the reference is built.
 * @param[in] map Array of field numbers in rec telling how ref should be built from the fields of rec.
 * @param[in] rec Record in the index; must be preserved while ref is used, as we do not copy field values to heap.
 * @param[in] offsets Array returned by Phy_rec::get_col_offsets().
 */
inline void row_build_row_ref_fast(DTuple *ref, const ulint *map, const rec_t *rec, const ulint *offsets);

/**
 * @brief Searches the clustered index record for a row, if we have the row reference.
 * 
 * @param[out] pcur Persistent cursor, which must be closed by the caller.
 * @param[in] mode BTR_MODIFY_LEAF, ...
 * @param[in] table Table.
 * @param[in] ref Row reference.
 * @param[in,out] mtr Mini-transaction.
 * 
 * @return true if found.
 */
bool row_search_on_row_ref(Btree_pcursor *pcur, ulint mode, const Table *table, const DTuple *ref, mtr_t *mtr);

/**
 * @brief Fetches the clustered index record for a secondary index record. 
 * The latches on the secondary index record are preserved.
 * 
 * @param[in] mode BTR_MODIFY_LEAF, ...
 * @param[in] rec Record in a secondary index
 * @param[in] index Secondary index
 * @param[out] clust_index Clustered index
 * @param[in,out] mtr Mini-transaction
 * 
 * @return rec_t* Record or NULL, if no record found
 */
rec_t *row_get_clust_rec(ulint mode, const rec_t *rec, const Index *index, Index **clust_index, mtr_t *mtr);

/**
 * @brief Searches an index record.
 * 
 * @param[in] index Index
 * @param[in] entry Index entry
 * @param[in] mode BTR_MODIFY_LEAF, ...
 * @param[in,out] pcur Persistent cursor, which must be closed by the caller
 * @param[in] mtr Mini-transaction
 * 
 * @return true if found
 */
bool row_search_index_entry(Index *index, const DTuple *entry, ulint mode, Btree_pcursor *pcur, mtr_t *mtr);

/**
 * @brief The allowed latching order of index records is the following:
 * (1) a secondary index record ->
 * (2) the clustered index record ->
 * (3) rollback segment data for the clustered index record.
 *
 * No new latches may be obtained while the kernel mutex is reserved.
 * However, the kernel mutex can be reserved while latches are owned.
 */

/**
 * @brief Formats the raw data in "data" (in InnoDB on-disk format) using
 * "dict_field" and writes the result to "buf".
 * Not more than "buf_size" bytes are written to "buf".
 * The result is always NUL-terminated (provided buf_size is positive) and the
 * number of bytes that were written to "buf" is returned (including the
 * terminating NUL).
 * 
 * @param[in] data Raw data
 * @param[in] data_len Raw data length in bytes
 * @param[in] dict_field Index field
 * @param[out] buf Output buffer
 * @param[in] buf_size Output buffer size in bytes
 * 
 * @return Number of bytes that were written
 */
ulint row_raw_format(const char *data, ulint data_len, const Field *dict_field, char *buf, ulint buf_size);

/**
 * @brief Reads the trx id field from a clustered index record.
 * 
 * @param[in] rec Record
 * @param[in] index Clustered index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
 * 
 * @return Value of the field
 */
inline trx_id_t row_get_rec_trx_id(const rec_t *rec, const Index *index, const ulint *offsets) {
  ut_ad(index->is_clustered());
  ut_ad(rec_offs_validate(rec, index, offsets));

  auto offset = index->m_trx_id_offset;

  if (offset == 0) {
    offset = row_get_trx_id_offset(rec, index, offsets);
  }

  return Trx_sys::read_trx_id(rec + offset);
}

/**
 * @brief Reads the roll pointer field from a clustered index record.
 * 
 * @param[in] rec Record
 * @param[in] index Clustered index
 * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
 * 
 * @return Value of the field
 */
inline roll_ptr_t row_get_rec_roll_ptr(const rec_t *rec, Index *index, const ulint *offsets) {
  ut_ad(index->is_clustered());
  ut_ad(rec_offs_validate(rec, index, offsets));

  auto offset = index->m_trx_id_offset;

  if (offset == 0) {
    offset = row_get_trx_id_offset(rec, index, offsets);
  }

  return trx_read_roll_ptr(rec + offset + DATA_TRX_ID_LEN);
}

/**
 * @brief Builds from a secondary index record a row reference with which we can
 * search the clustered index record.
 * 
 * @param[in,out] ref    Typed data tuple where the reference is built
 * @param[in] map        Array of field numbers in rec telling how ref should be built from the fields of rec
 * @param[in] rec        Record in the index; must be preserved while ref is used, as we do not copy field values to heap
 * @param[in] offsets    Array returned by Phy_rec::get_col_offsets()
 */
inline void row_build_row_ref_fast(DTuple *ref, const ulint *map, const rec_t *rec, const ulint *offsets) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(!rec_offs_any_extern(offsets));

  const auto ref_len = dtuple_get_n_fields(ref);

  for (ulint i{}; i < ref_len; ++i) {
    auto dfield = dtuple_get_nth_field(ref, i);
    auto field_no = *(map + i);

    if (field_no != ULINT_UNDEFINED) {
      ulint len;
      auto field = rec_get_nth_field(rec, offsets, field_no, &len);

      dfield_set_data(dfield, field, len);
    }
  }
}
