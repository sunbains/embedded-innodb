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

ulint row_get_trx_id_offset(const rec_t *rec __attribute__((unused)), const Index *index, const ulint *offsets) {

  ut_ad(index->is_clustered());
  ut_ad(rec_offs_validate(rec, index, offsets));

  const auto pos = index->get_sys_col_field_pos(DATA_TRX_ID);

  ulint len;
  const auto offset = rec_get_nth_field_offs(offsets, pos, &len);

  ut_ad(len == DATA_TRX_ID_LEN);

  return offset;
}

DTuple *row_build_index_entry(const DTuple *row, row_ext_t *ext, const Index *index, mem_heap_t *heap) {
  ut_ad(dtuple_check_typed(row));

  const auto entry_len = index->get_n_fields();
  auto entry = dtuple_create(heap, entry_len);

  dtuple_set_n_fields_cmp(entry, index->get_n_unique_in_tree());

  if (index->is_clustered()) {
    /* Do not fetch externally stored columns to
    the clustered index.  Such columns are handled
    at a higher level. */
    ext = nullptr;
  }

  for (ulint i{}; i < entry_len; ++i) {
    const auto ind_field = index->get_nth_field(i);
    const auto col = ind_field->m_col;
    ulint col_no = col->get_no();
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
          return nullptr;
        }
        dfield_set_data(dfield, buf, len);
      }

    } else if (dfield_is_ext(dfield)) {
      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);
      len -= BTR_EXTERN_FIELD_REF_SIZE;
      ut_a(ind_field->m_prefix_len <= len || index->is_clustered());
    }

    /* If a column prefix index, take only the prefix */
    if (ind_field->m_prefix_len > 0 && !dfield_is_null(dfield)) {
      ut_ad(col->m_ord_part);

      len = dtype_get_at_most_n_mbchars(
        col->prtype, col->mbminlen, col->mbmaxlen, ind_field->m_prefix_len, len, (char *)dfield_get_data(dfield)
      );

      dfield_set_len(dfield, len);
    }
  }

  ut_ad(dtuple_check_typed(entry));

  return entry;
}

DTuple *row_build(ulint type, const Index *index, const rec_t *rec, const ulint *offsets, const Table *col_table, row_ext_t **ext, mem_heap_t *heap) {
  mem_heap_t *tmp_heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

  ut_ad(index && rec && heap);
  ut_ad(index->is_clustered());

  if (offsets == nullptr) {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &tmp_heap, Current_location());
  } else {
    ut_ad(rec_offs_validate(rec, index, offsets));
  }

  if (type != ROW_COPY_POINTERS) {
    /* Take a copy of rec to heap */
    auto buf = mem_heap_alloc(heap, rec_offs_size(offsets));

    rec = rec_copy(buf, rec, offsets);

    /* Avoid a debug assertion in rec_offs_validate(). */
    ut_d(rec_offs_make_valid(rec, index, (ulint *)offsets));
  }

  auto table = index->m_table;
  auto row_len = table->get_n_cols();
  auto row = dtuple_create(heap, row_len);

  table->copy_types(row);

  dtuple_set_info_bits(row, rec_get_info_bits(rec));

  const auto n_fields = rec_offs_n_fields(offsets);
  const auto n_ext_cols = rec_offs_n_extern(offsets);

  ulint *ext_cols;

  if (unlikely(n_ext_cols > 0)) {
    ext_cols = reinterpret_cast<ulint *>(mem_heap_alloc(heap, n_ext_cols * sizeof(*ext_cols)));
  } else {
    ext_cols = nullptr;
  }

  ulint j{};

  for (ulint i{}, j = 0; i < n_fields; ++i) {
    const auto ind_field = index->get_nth_field(i);
    Column const *col = ind_field->m_col;
    const ulint col_no = col->get_no();
    auto dfield = dtuple_get_nth_field(row, col_no);

    if (ind_field->m_prefix_len == 0) {
      ulint len;
      const auto field = rec_get_nth_field(rec, offsets, i, &len);

      dfield_set_data(dfield, field, len);
    }

    if (rec_offs_nth_extern(offsets, i)) {
      dfield_set_ext(dfield);

      if (likely_null(col_table)) {
        ut_a(col_no < col_table->get_n_cols());
        col = col_table->get_nth_col(col_no);
      }

      if (col->m_ord_part > 0) {
        /* We will have to fetch prefixes of externally stored columns that are
        referenced by column prefixes. */
        ext_cols[j++] = col_no;
      }
    }
  }

  ut_ad(dtuple_check_typed(row));

  if (j > 0) {
    *ext = row_ext_create(j, ext_cols, row, heap);
  } else {
    *ext = nullptr;
  }

  if (tmp_heap != nullptr) {
    mem_heap_free(tmp_heap);
  }

  return row;
}

DTuple *row_rec_to_index_entry_low(const rec_t *rec, const Index *index, const ulint *offsets, ulint *n_ext, mem_heap_t *heap) {
  /* Because this function may be invoked by row0merge.c
  on a record whose header is in different format, the check
  rec_offs_validate(rec, index, offsets) must be avoided here. */
  ut_ad(n_ext != nullptr);

  *n_ext = 0;

  const auto rec_len = rec_offs_n_fields(offsets);
  auto entry = dtuple_create(heap, rec_len);

  dtuple_set_n_fields_cmp(entry, index->get_n_unique_in_tree());
  ut_ad(rec_len == index->get_n_fields());

  index->copy_types(entry, rec_len);

  for (ulint i{}; i < rec_len; ++i) {
    ulint len;
    auto dfield = dtuple_get_nth_field(entry, i);
    auto field = rec_get_nth_field(rec, offsets, i, &len);

    dfield_set_data(dfield, field, len);

    if (rec_offs_nth_extern(offsets, i)) {
      dfield_set_ext(dfield);
      (*n_ext)++;
    }
  }

  ut_ad(dtuple_check_typed(entry));

  return entry;
}

DTuple *row_rec_to_index_entry(ulint type, const rec_t *rec, const Index *index, ulint *offsets, ulint *n_ext, mem_heap_t *heap) {
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (type == ROW_COPY_DATA) {
    /* Take a copy of rec to heap */
    auto buf = mem_heap_alloc(heap, rec_offs_size(offsets));

    rec = rec_copy(buf, rec, offsets);

    /* Avoid a debug assertion in rec_offs_validate(). */
    ut_d(rec_offs_make_valid(rec, index, offsets));
  }

  auto entry = row_rec_to_index_entry_low(rec, index, offsets, n_ext, heap);

  dtuple_set_info_bits(entry, rec_get_info_bits(rec));

  return entry;
}

DTuple *row_build_row_ref(ulint type, const Index *index, const rec_t *rec, mem_heap_t *heap) {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(index && rec && heap);
  ut_ad(!index->is_clustered());

  mem_heap_t *tmp_heap{};

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, ULINT_UNDEFINED, &tmp_heap, Current_location());
  }

  /* Secondary indexes must not contain externally stored columns. */
  ut_ad(!rec_offs_any_extern(offsets));

  if (type == ROW_COPY_DATA) {
    /* Take a copy of rec to heap */

    auto buf = mem_heap_alloc(heap, rec_offs_size(offsets));

    rec = rec_copy(buf, rec, offsets);
    /* Avoid a debug assertion in rec_offs_validate(). */
    ut_d(rec_offs_make_valid(rec, index, offsets));
  }

  auto table = index->m_table;
  auto clust_index = table->get_clustered_index();
  const auto n_unique = clust_index->get_n_unique();
  auto dtuple = dtuple_create(heap, n_unique);

  clust_index->copy_types(dtuple, n_unique);

  for (ulint i{}; i < n_unique; ++i) {
    auto dfield = dtuple_get_nth_field(dtuple, i);
    auto pos = index->get_nth_field_pos(clust_index, i);

    ut_a(pos != ULINT_UNDEFINED);
    ulint len;
    const auto field = rec_get_nth_field(rec, offsets, pos, &len);

    dfield_set_data(dfield, field, len);

    /* If the primary key contains a column prefix, then the
    secondary index may contain a longer prefix of the same
    column, or the full column, and we must adjust the length
    accordingly. */

    auto clust_col_prefix_len = index->get_nth_field(i)->m_prefix_len;

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

  ut_ad(dtuple_check_typed(dtuple));
  if (tmp_heap != nullptr) {
    mem_heap_free(tmp_heap);
  }

  return dtuple;
}

void row_build_row_ref_in_tuple(DTuple *ref, const rec_t *rec, const Index *index, ulint *offsets, trx_t *trx) {
  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

  ut_ad(!index->is_clustered());

  if (unlikely(index->m_table == nullptr)) {
    ib_logger(ib_stream, "table ");
  notfound:
    ut_print_name(index->m_table->m_name);
    ib_logger(ib_stream, " for index ");
    ut_print_name(index->m_name);
    ib_logger(ib_stream, " not found\n");
    ut_error;
  }

  auto clust_index = index->get_clustered_index();

  if (unlikely(!clust_index)) {
    ib_logger(ib_stream, "clust index for table ");
    goto notfound;
  }

  if (offsets == nullptr) {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &heap, Current_location());
  } else {
    ut_ad(rec_offs_validate(rec, index, offsets));
  }

  /* Secondary indexes must not contain externally stored columns. */
  ut_ad(!rec_offs_any_extern(offsets));
  auto n_unique = clust_index->get_n_unique();

  ut_ad(n_unique == dtuple_get_n_fields(ref));

  clust_index->copy_types(ref, n_unique);

  for (ulint i{}; i < n_unique; ++i) {
    auto dfield = dtuple_get_nth_field(ref, i);
    auto pos = index->get_nth_field_pos(clust_index, i);

    ut_a(pos != ULINT_UNDEFINED);

    ulint len;
    const auto field = rec_get_nth_field(rec, offsets, pos, &len);

    dfield_set_data(dfield, field, len);

    /* If the primary key contains a column prefix, then the
    secondary index may contain a longer prefix of the same
    column, or the full column, and we must adjust the length
    accordingly. */

    const auto clust_col_prefix_len = index->get_nth_field(i)->m_prefix_len;

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

bool row_search_on_row_ref(Btree_pcursor *pcur, ulint mode, const Table *table, const DTuple *ref, mtr_t *mtr) {
  ut_ad(dtuple_check_typed(ref));

  auto index = table->get_first_index();

  ut_a(dtuple_get_n_fields(ref) == index->get_n_unique());

  pcur->open(index, ref, PAGE_CUR_LE, mode, mtr, Current_location());

  auto low_match = pcur->get_low_match();

  auto rec = pcur->get_rec();

  if (page_rec_is_infimum(rec)) {

    return false;
  }

  return low_match == dtuple_get_n_fields(ref);
}

rec_t *row_get_clust_rec(ulint mode, const rec_t *rec, const Index *index, Index **clust_index, mtr_t *mtr) {
  Btree_pcursor pcur(srv_fsp, srv_btree_sys);

  ut_ad(!index->is_clustered());

  auto table = index->m_table;

  auto heap = mem_heap_create(256);

  auto ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, heap);

  auto found = row_search_on_row_ref(&pcur, mode, table, ref, mtr);

  auto clust_rec = found ? pcur.get_rec() : nullptr;

  mem_heap_free(heap);

  pcur.close();

  *clust_index = table->get_first_index();

  return clust_rec;
}

bool row_search_index_entry(Index *index, const DTuple *entry, ulint mode, Btree_pcursor *pcur, mtr_t *mtr) {
  ut_ad(dtuple_check_typed(entry));

  pcur->open(index, entry, PAGE_CUR_LE, mode, mtr, Current_location());

  auto low_match = pcur->get_low_match();

  auto rec = pcur->get_rec();

  auto n_fields = dtuple_get_n_fields(entry);

  return !page_rec_is_infimum(rec) && low_match == n_fields;
}
