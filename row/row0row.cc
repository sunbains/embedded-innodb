/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/** @file row/row0row.c
General row routines

Created 4/20/1996 Heikki Tuuri
*******************************************************/

#include "row0row.h"

#ifdef UNIV_NONINL
#include "row0row.ic"
#endif

#include "api0ucode.h"
#include "btr0btr.h"
#include "data0type.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "que0que.h"
#include "read0read.h"
#include "rem0cmp.h"
#include "row0ext.h"
#include "row0upd.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "ut0mem.h"

/** Gets the offset of trx id field, in bytes relative to the origin of
a clustered index record.
@return	offset of DATA_TRX_ID */

ulint row_get_trx_id_offset(
    const rec_t *rec __attribute__((unused)),
    /*!< in: record */
    dict_index_t *index,  /*!< in: clustered index */
    const ulint *offsets) /*!< in: rec_get_offsets(rec, index) */
{
  ulint pos;
  ulint offset;
  ulint len;

  ut_ad(dict_index_is_clust(index));
  ut_ad(rec_offs_validate(rec, index, offsets));

  pos = dict_index_get_sys_col_pos(index, DATA_TRX_ID);

  offset = rec_get_nth_field_offs(offsets, pos, &len);

  ut_ad(len == DATA_TRX_ID_LEN);

  return (offset);
}

/** When an insert or purge to a table is performed, this function builds
the entry to be inserted into or purged from an index on the table.
@return index entry which should be inserted or purged, or nullptr if the
externally stored columns in the clustered index record are
unavailable and ext != nullptr */

dtuple_t *row_build_index_entry(
    const dtuple_t *row, /*!< in: row which should be
                         inserted or purged */
    row_ext_t *ext,      /*!< in: externally stored column prefixes,
                         or nullptr */
    dict_index_t *index, /*!< in: index on the table */
    mem_heap_t *heap)    /*!< in: memory heap from which the memory for
                         the index entry is allocated */
{
  dtuple_t *entry;
  ulint entry_len;
  ulint i;

  ut_ad(row && index && heap);
  ut_ad(dtuple_check_typed(row));

  entry_len = dict_index_get_n_fields(index);
  entry = dtuple_create(heap, entry_len);

  dtuple_set_n_fields_cmp(entry, dict_index_get_n_unique_in_tree(index));
  if (dict_index_is_clust(index)) {
    /* Do not fetch externally stored columns to
    the clustered index.  Such columns are handled
    at a higher level. */
    ext = nullptr;
  }

  for (i = 0; i < entry_len; i++) {
    const dict_field_t *ind_field = dict_index_get_nth_field(index, i);
    const dict_col_t *col = ind_field->col;
    ulint col_no = dict_col_get_no(col);
    dfield_t *dfield = dtuple_get_nth_field(entry, i);
    const dfield_t *dfield2 = dtuple_get_nth_field(row, col_no);
    ulint len = dfield_get_len(dfield2);

    dfield_copy(dfield, dfield2);

    if (dfield_is_null(dfield)) {
    } else if (likely_null(ext)) {
      /* See if the column is stored externally. */
      const byte *buf = row_ext_lookup(ext, col_no, &len);
      if (likely_null(buf)) {
        if (unlikely(buf == field_ref_zero)) {
          return (nullptr);
        }
        dfield_set_data(dfield, buf, len);
      }
    } else if (dfield_is_ext(dfield)) {
      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);
      len -= BTR_EXTERN_FIELD_REF_SIZE;
      ut_a(ind_field->prefix_len <= len || dict_index_is_clust(index));
    }

    /* If a column prefix index, take only the prefix */
    if (ind_field->prefix_len > 0 && !dfield_is_null(dfield)) {
      ut_ad(col->ord_part);
      len = dtype_get_at_most_n_mbchars(col->prtype, col->mbminlen,
                                        col->mbmaxlen, ind_field->prefix_len,
                                        len, (char *)dfield_get_data(dfield));
      dfield_set_len(dfield, len);
    }
  }

  ut_ad(dtuple_check_typed(entry));

  return (entry);
}

/** An inverse function to row_build_index_entry. Builds a row from a
record in a clustered index.
@return	own: row built; see the NOTE below! */

dtuple_t *row_build(ulint type,                /*!< in: ROW_COPY_POINTERS or
                                               ROW_COPY_DATA; the latter
                                               copies also the data fields to
                                               heap while the first only
                                               places pointers to data fields
                                               on the index page, and thus is
                                               more efficient */
                    const dict_index_t *index, /*!< in: clustered index */
                    const rec_t *rec,          /*!< in: record in the clustered
                                               index; NOTE: in the case
                                               ROW_COPY_POINTERS the data
                                               fields in the row will point
                                               directly into this record,
                                               therefore, the buffer page of
                                               this record must be at least
                                               s-latched and the latch held
                                               as long as the row dtuple is used! */
                    const ulint *offsets, /*!< in: rec_get_offsets(rec,index)
                                          or nullptr, in which case this function
                                          will invoke rec_get_offsets() */
                    const dict_table_t *col_table,
                    /*!< in: table, to check which
                    externally stored columns
                    occur in the ordering columns
                    of an index, or nullptr if
                    index->table should be
                    consulted instead */
                    row_ext_t **ext,  /*!< out, own: cache of
                                      externally stored column
                                      prefixes, or nullptr */
                    mem_heap_t *heap) /*!< in: memory heap from which
                                      the memory needed is allocated */
{
  dtuple_t *row;
  const dict_table_t *table;
  ulint n_fields;
  ulint n_ext_cols;
  ulint *ext_cols = nullptr; /* remove warning */
  ulint len;
  ulint row_len;
  byte *buf;
  ulint i;
  ulint j;
  mem_heap_t *tmp_heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

  ut_ad(index && rec && heap);
  ut_ad(dict_index_is_clust(index));

  if (!offsets) {
    offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &tmp_heap);
  } else {
    ut_ad(rec_offs_validate(rec, index, offsets));
  }

  if (type != ROW_COPY_POINTERS) {
    /* Take a copy of rec to heap */
    buf = mem_heap_alloc(heap, rec_offs_size(offsets));
    rec = rec_copy(buf, rec, offsets);
    /* Avoid a debug assertion in rec_offs_validate(). */
    rec_offs_make_valid(rec, index, (ulint *)offsets);
  }

  table = index->table;
  row_len = dict_table_get_n_cols(table);

  row = dtuple_create(heap, row_len);

  dict_table_copy_types(row, table);

  dtuple_set_info_bits(row, rec_get_info_bits(rec, dict_table_is_comp(table)));

  n_fields = rec_offs_n_fields(offsets);
  n_ext_cols = rec_offs_n_extern(offsets);
  if (n_ext_cols) {
    ext_cols = reinterpret_cast<ulint *>(
        mem_heap_alloc(heap, n_ext_cols * sizeof(*ext_cols)));
  }

  for (i = j = 0; i < n_fields; i++) {
    dict_field_t *ind_field = dict_index_get_nth_field(index, i);
    const dict_col_t *col = dict_field_get_col(ind_field);
    ulint col_no = dict_col_get_no(col);
    dfield_t *dfield = dtuple_get_nth_field(row, col_no);

    if (ind_field->prefix_len == 0) {

      const byte *field = rec_get_nth_field(rec, offsets, i, &len);

      dfield_set_data(dfield, field, len);
    }

    if (rec_offs_nth_extern(offsets, i)) {
      dfield_set_ext(dfield);

      if (likely_null(col_table)) {
        ut_a(col_no < dict_table_get_n_cols(col_table));
        col = dict_table_get_nth_col(col_table, col_no);
      }

      if (col->ord_part) {
        /* We will have to fetch prefixes of
        externally stored columns that are
        referenced by column prefixes. */
        ext_cols[j++] = col_no;
      }
    }
  }

  ut_ad(dtuple_check_typed(row));

  if (j) {
    *ext = row_ext_create(j, ext_cols, row, heap);
  } else {
    *ext = nullptr;
  }

  if (tmp_heap) {
    mem_heap_free(tmp_heap);
  }

  return (row);
}

/** Converts an index record to a typed data tuple.
@return index entry built; does not set info_bits, and the data fields
in the entry will point directly to rec */

dtuple_t *row_rec_to_index_entry_low(
    const rec_t *rec,          /*!< in: record in the index */
    const dict_index_t *index, /*!< in: index */
    const ulint *offsets,      /*!< in: rec_get_offsets(rec, index) */
    ulint *n_ext,              /*!< out: number of externally
                               stored columns */
    mem_heap_t *heap)          /*!< in: memory heap from which
                               the memory needed is allocated */
{
  dtuple_t *entry;
  dfield_t *dfield;
  ulint i;
  const byte *field;
  ulint len;
  ulint rec_len;

  ut_ad(rec && heap && index);
  /* Because this function may be invoked by row0merge.c
  on a record whose header is in different format, the check
  rec_offs_validate(rec, index, offsets) must be avoided here. */
  ut_ad(n_ext);
  *n_ext = 0;

  rec_len = rec_offs_n_fields(offsets);

  entry = dtuple_create(heap, rec_len);

  dtuple_set_n_fields_cmp(entry, dict_index_get_n_unique_in_tree(index));
  ut_ad(rec_len == dict_index_get_n_fields(index));

  dict_index_copy_types(entry, index, rec_len);

  for (i = 0; i < rec_len; i++) {

    dfield = dtuple_get_nth_field(entry, i);
    field = rec_get_nth_field(rec, offsets, i, &len);

    dfield_set_data(dfield, field, len);

    if (rec_offs_nth_extern(offsets, i)) {
      dfield_set_ext(dfield);
      (*n_ext)++;
    }
  }

  ut_ad(dtuple_check_typed(entry));

  return (entry);
}

/** Converts an index record to a typed data tuple. NOTE that externally
stored (often big) fields are NOT copied to heap.
@return	own: index entry built; see the NOTE below! */

dtuple_t *
row_rec_to_index_entry(ulint type,                /*!< in: ROW_COPY_DATA, or
                                                  ROW_COPY_POINTERS: the former
                                                  copies also the data fields to
                                                  heap as the latter only places
                                                  pointers to data fields on the
                                                  index page */
                       const rec_t *rec,          /*!< in: record in the index;
                                                  NOTE: in the case
                                                  ROW_COPY_POINTERS the data
                                                  fields in the row will point
                                                  directly into this record,
                                                  therefore, the buffer page of
                                                  this record must be at least
                                                  s-latched and the latch held
                                                  as long as the dtuple is used! */
                       const dict_index_t *index, /*!< in: index */
                       ulint *offsets,   /*!< in/out: rec_get_offsets(rec) */
                       ulint *n_ext,     /*!< out: number of externally
                                         stored columns */
                       mem_heap_t *heap) /*!< in: memory heap from which
                                         the memory needed is allocated */
{
  dtuple_t *entry;
  byte *buf;

  ut_ad(rec && heap && index);
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (type == ROW_COPY_DATA) {
    /* Take a copy of rec to heap */
    buf = mem_heap_alloc(heap, rec_offs_size(offsets));
    rec = rec_copy(buf, rec, offsets);
    /* Avoid a debug assertion in rec_offs_validate(). */
    rec_offs_make_valid(rec, index, offsets);
  }

  entry = row_rec_to_index_entry_low(rec, index, offsets, n_ext, heap);

  dtuple_set_info_bits(entry, rec_get_info_bits(rec, rec_offs_comp(offsets)));

  return (entry);
}

/** Builds from a secondary index record a row reference with which we can
search the clustered index record.
@return	own: row reference built; see the NOTE below! */

dtuple_t *
row_build_row_ref(ulint type, /*!< in: ROW_COPY_DATA, or ROW_COPY_POINTERS:
                              the former copies also the data fields to
                              heap, whereas the latter only places pointers
                              to data fields on the index page */
                  dict_index_t *index, /*!< in: secondary index */
                  const rec_t *rec,    /*!< in: record in the index;
                                       NOTE: in the case ROW_COPY_POINTERS
                                       the data fields in the row will point
                                       directly into this record, therefore,
                                       the buffer page of this record must be
                                       at least s-latched and the latch held
                                       as long as the row reference is used! */
                  mem_heap_t *heap) /*!< in: memory heap from which the memory
                                    needed is allocated */
{
  dict_table_t *table;
  dict_index_t *clust_index;
  dfield_t *dfield;
  dtuple_t *ref;
  const byte *field;
  ulint len;
  ulint ref_len;
  ulint pos;
  byte *buf;
  ulint clust_col_prefix_len;
  ulint i;
  mem_heap_t *tmp_heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(index && rec && heap);
  ut_ad(!dict_index_is_clust(index));

  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &tmp_heap);
  /* Secondary indexes must not contain externally stored columns. */
  ut_ad(!rec_offs_any_extern(offsets));

  if (type == ROW_COPY_DATA) {
    /* Take a copy of rec to heap */

    buf = mem_heap_alloc(heap, rec_offs_size(offsets));

    rec = rec_copy(buf, rec, offsets);
    /* Avoid a debug assertion in rec_offs_validate(). */
    rec_offs_make_valid(rec, index, offsets);
  }

  table = index->table;

  clust_index = dict_table_get_first_index(table);

  ref_len = dict_index_get_n_unique(clust_index);

  ref = dtuple_create(heap, ref_len);

  dict_index_copy_types(ref, clust_index, ref_len);

  for (i = 0; i < ref_len; i++) {
    dfield = dtuple_get_nth_field(ref, i);

    pos = dict_index_get_nth_field_pos(index, clust_index, i);

    ut_a(pos != ULINT_UNDEFINED);

    field = rec_get_nth_field(rec, offsets, pos, &len);

    dfield_set_data(dfield, field, len);

    /* If the primary key contains a column prefix, then the
    secondary index may contain a longer prefix of the same
    column, or the full column, and we must adjust the length
    accordingly. */

    clust_col_prefix_len = dict_index_get_nth_field(clust_index, i)->prefix_len;

    if (clust_col_prefix_len > 0) {
      if (len != UNIV_SQL_NULL) {

        const dtype_t *dtype = dfield_get_type(dfield);

        dfield_set_len(dfield,
                       dtype_get_at_most_n_mbchars(
                           dtype->prtype, dtype->mbminlen, dtype->mbmaxlen,
                           clust_col_prefix_len, len, (char *)field));
      }
    }
  }

  ut_ad(dtuple_check_typed(ref));
  if (tmp_heap) {
    mem_heap_free(tmp_heap);
  }

  return (ref);
}

/** Builds from a secondary index record a row reference with which we can
search the clustered index record. */

void row_build_row_ref_in_tuple(
    dtuple_t *ref,             /*!< in/out: row reference built;
                               see the NOTE below! */
    const rec_t *rec,          /*!< in: record in the index;
                               NOTE: the data fields in ref
                               will point directly into this
                               record, therefore, the buffer
                               page of this record must be at
                               least s-latched and the latch
                               held as long as the row
                               reference is used! */
    const dict_index_t *index, /*!< in: secondary index */
    ulint *offsets,            /*!< in: rec_get_offsets(rec, index)
                               or nullptr */
    trx_t *trx)                /*!< in: transaction */
{
  const dict_index_t *clust_index;
  dfield_t *dfield;
  const byte *field;
  ulint len;
  ulint ref_len;
  ulint pos;
  ulint clust_col_prefix_len;
  ulint i;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

  ut_a(ref);
  ut_a(index);
  ut_a(rec);
  ut_ad(!dict_index_is_clust(index));

  if (unlikely(!index->table)) {
    ib_logger(ib_stream, "table ");
  notfound:
    ut_print_name(ib_stream, trx, true, index->table_name);
    ib_logger(ib_stream, " for index ");
    ut_print_name(ib_stream, trx, false, index->name);
    ib_logger(ib_stream, " not found\n");
    ut_error;
  }

  clust_index = dict_table_get_first_index(index->table);

  if (unlikely(!clust_index)) {
    ib_logger(ib_stream, "clust index for table ");
    goto notfound;
  }

  if (!offsets) {
    offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &heap);
  } else {
    ut_ad(rec_offs_validate(rec, index, offsets));
  }

  /* Secondary indexes must not contain externally stored columns. */
  ut_ad(!rec_offs_any_extern(offsets));
  ref_len = dict_index_get_n_unique(clust_index);

  ut_ad(ref_len == dtuple_get_n_fields(ref));

  dict_index_copy_types(ref, clust_index, ref_len);

  for (i = 0; i < ref_len; i++) {
    dfield = dtuple_get_nth_field(ref, i);

    pos = dict_index_get_nth_field_pos(index, clust_index, i);

    ut_a(pos != ULINT_UNDEFINED);

    field = rec_get_nth_field(rec, offsets, pos, &len);

    dfield_set_data(dfield, field, len);

    /* If the primary key contains a column prefix, then the
    secondary index may contain a longer prefix of the same
    column, or the full column, and we must adjust the length
    accordingly. */

    clust_col_prefix_len = dict_index_get_nth_field(clust_index, i)->prefix_len;

    if (clust_col_prefix_len > 0) {
      if (len != UNIV_SQL_NULL) {

        const dtype_t *dtype = dfield_get_type(dfield);

        dfield_set_len(dfield,
                       dtype_get_at_most_n_mbchars(
                           dtype->prtype, dtype->mbminlen, dtype->mbmaxlen,
                           clust_col_prefix_len, len, (char *)field));
      }
    }
  }

  ut_ad(dtuple_check_typed(ref));
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

/** Searches the clustered index record for a row, if we have the row reference.
@return	true if found */

bool row_search_on_row_ref(btr_pcur_t *pcur, /*!< out: persistent cursor, which
                                              must be closed by the caller */
                           ulint mode,       /*!< in: BTR_MODIFY_LEAF, ... */
                           const dict_table_t *table, /*!< in: table */
                           const dtuple_t *ref,       /*!< in: row reference */
                           mtr_t *mtr)                /*!< in/out: mtr */
{
  ulint low_match;
  rec_t *rec;
  dict_index_t *index;

  ut_ad(dtuple_check_typed(ref));

  index = dict_table_get_first_index(table);

  ut_a(dtuple_get_n_fields(ref) == dict_index_get_n_unique(index));

  btr_pcur_open(index, ref, PAGE_CUR_LE, mode, pcur, mtr);

  low_match = btr_pcur_get_low_match(pcur);

  rec = btr_pcur_get_rec(pcur);

  if (page_rec_is_infimum(rec)) {

    return (false);
  }

  if (low_match != dtuple_get_n_fields(ref)) {

    return (false);
  }

  return (true);
}

rec_t *
row_get_clust_rec(ulint mode, const rec_t *rec, dict_index_t *index, dict_index_t **clust_index, mtr_t *mtr) {
  mem_heap_t *heap;
  dtuple_t *ref;
  dict_table_t *table;
  btr_pcur_t pcur;
  bool found;
  rec_t *clust_rec;

  ut_ad(!dict_index_is_clust(index));

  table = index->table;

  heap = mem_heap_create(256);

  ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, heap);

  found = row_search_on_row_ref(&pcur, mode, table, ref, mtr);

  clust_rec = found ? btr_pcur_get_rec(&pcur) : nullptr;

  mem_heap_free(heap);

  btr_pcur_close(&pcur);

  *clust_index = dict_table_get_first_index(table);

  return (clust_rec);
}

bool row_search_index_entry(dict_index_t *index, const dtuple_t *entry, ulint mode, btr_pcur_t *pcur, mtr_t *mtr) {
  ulint n_fields;
  ulint low_match;
  rec_t *rec;

  ut_ad(dtuple_check_typed(entry));

  btr_pcur_open(index, entry, PAGE_CUR_LE, mode, pcur, mtr);
  low_match = btr_pcur_get_low_match(pcur);

  rec = btr_pcur_get_rec(pcur);

  n_fields = dtuple_get_n_fields(entry);

  return (!page_rec_is_infimum(rec) && low_match == n_fields);
}
