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

ulint row_get_trx_id_offset(const rec_t *rec __attribute__((unused)), dict_index_t *index, const ulint *offsets) {

  ut_ad(dict_index_is_clust(index));
  ut_ad(rec_offs_validate(rec, index, offsets));

  auto pos = dict_index_get_sys_col_pos(index, DATA_TRX_ID);

  ulint len;

  auto offset = rec_get_nth_field_offs(offsets, pos, &len);

  ut_ad(len == DATA_TRX_ID_LEN);

  return (offset);
}

dtuple_t *row_build_index_entry(const dtuple_t *row, row_ext_t *ext, dict_index_t *index, mem_heap_t *heap) {
  ut_ad(dtuple_check_typed(row));

  auto entry_len = dict_index_get_n_fields(index);
  auto entry = dtuple_create(heap, entry_len);

  dtuple_set_n_fields_cmp(entry, dict_index_get_n_unique_in_tree(index));
  if (dict_index_is_clust(index)) {
    /* Do not fetch externally stored columns to
    the clustered index.  Such columns are handled
    at a higher level. */
    ext = nullptr;
  }

  for (ulint i = 0; i < entry_len; i++) {
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
      len = dtype_get_at_most_n_mbchars(
        col->prtype, col->mbminlen, col->mbmaxlen, ind_field->prefix_len, len, (char *)dfield_get_data(dfield)
      );
      dfield_set_len(dfield, len);
    }
  }

  ut_ad(dtuple_check_typed(entry));

  return (entry);
}

dtuple_t *row_build(
  ulint type,
  const dict_index_t *index,
  const rec_t *rec,
  const ulint *offsets,
  const dict_table_t *col_table,
  row_ext_t **ext,
  mem_heap_t *heap
) {
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
    ext_cols = reinterpret_cast<ulint *>(mem_heap_alloc(heap, n_ext_cols * sizeof(*ext_cols)));
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

dtuple_t *row_rec_to_index_entry_low(
  const rec_t *rec,
  const dict_index_t *index,
  const ulint *offsets,
  ulint *n_ext,
  mem_heap_t *heap
) {
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

  return entry;
}

dtuple_t *row_rec_to_index_entry(
  ulint type,
  const rec_t *rec,
  const dict_index_t *index,
  ulint *offsets,
  ulint *n_ext,
  mem_heap_t *heap) {

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (type == ROW_COPY_DATA) {
    /* Take a copy of rec to heap */
    auto buf = mem_heap_alloc(heap, rec_offs_size(offsets));

    rec = rec_copy(buf, rec, offsets);

    /* Avoid a debug assertion in rec_offs_validate(). */
    rec_offs_make_valid(rec, index, offsets);
  }

  auto entry = row_rec_to_index_entry_low(rec, index, offsets, n_ext, heap);

  dtuple_set_info_bits(entry, rec_get_info_bits(rec, rec_offs_comp(offsets)));

  return entry;
}

dtuple_t *row_build_row_ref(ulint type, dict_index_t *index, const rec_t *rec, mem_heap_t *heap) {
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

        dfield_set_len(
          dfield,
          dtype_get_at_most_n_mbchars(dtype->prtype, dtype->mbminlen, dtype->mbmaxlen, clust_col_prefix_len, len, (char *)field)
        );
      }
    }
  }

  ut_ad(dtuple_check_typed(ref));
  if (tmp_heap) {
    mem_heap_free(tmp_heap);
  }

  return (ref);
}

void row_build_row_ref_in_tuple(
  dtuple_t *ref,
  const rec_t *rec,
  const dict_index_t *index,
  ulint *offsets,
  trx_t *trx
) {
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

        dfield_set_len(
          dfield,
          dtype_get_at_most_n_mbchars(dtype->prtype, dtype->mbminlen, dtype->mbmaxlen, clust_col_prefix_len, len, (char *)field)
        );
      }
    }
  }

  ut_ad(dtuple_check_typed(ref));
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

bool row_search_on_row_ref(btr_pcur_t *pcur, ulint mode, const dict_table_t *table, const dtuple_t *ref, mtr_t *mtr) {
  ut_ad(dtuple_check_typed(ref));

  auto index = dict_table_get_first_index(table);

  ut_a(dtuple_get_n_fields(ref) == dict_index_get_n_unique(index));

  pcur->open(index, ref, PAGE_CUR_LE, mode, mtr, Source_location{});

  auto low_match = pcur->get_low_match();

  auto rec = pcur->get_rec();

  if (page_rec_is_infimum(rec)) {

    return false;
  }

  return low_match == dtuple_get_n_fields(ref);
}

rec_t *row_get_clust_rec(ulint mode, const rec_t *rec, dict_index_t *index, dict_index_t **clust_index, mtr_t *mtr) {
  btr_pcur_t pcur;

  ut_ad(!dict_index_is_clust(index));

  auto table = index->table;

  auto heap = mem_heap_create(256);

  auto ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, heap);

  auto found = row_search_on_row_ref(&pcur, mode, table, ref, mtr);

  auto clust_rec = found ? pcur.get_rec() : nullptr;

  mem_heap_free(heap);

  pcur.close();

  *clust_index = dict_table_get_first_index(table);

  return clust_rec;
}

bool row_search_index_entry(dict_index_t *index, const dtuple_t *entry, ulint mode, btr_pcur_t *pcur, mtr_t *mtr) {
  ut_ad(dtuple_check_typed(entry));

  pcur->open(index, entry, PAGE_CUR_LE, mode, mtr, Source_location{});

  auto low_match = pcur->get_low_match();

  auto rec = pcur->get_rec();

  auto n_fields = dtuple_get_n_fields(entry);

  return !page_rec_is_infimum(rec) && low_match == n_fields;
}
