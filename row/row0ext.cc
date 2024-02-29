/****************************************************************************
Copyright (c) 2006, 2009, Innobase Oy. All Rights Reserved.

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

/** @file row/row0ext.c
Caching of externally stored column prefixes

Created September 2006 Marko Makela
*******************************************************/

#include "row0ext.h"

#ifdef UNIV_NONINL
#include "row0ext.ic"
#endif

#include "btr0cur.h"

/** Fills the column prefix cache of an externally stored column. */
static void
row_ext_cache_fill(row_ext_t *ext, /*!< in/out: column prefix cache */
                   ulint i,        /*!< in: index of ext->ext[] */
                   ulint zip_size, /*!< compressed page size in bytes, or 0 */
                   const dfield_t *dfield) /*!< in: data field */
{
  auto field = (const byte *)dfield_get_data(dfield);
  auto f_len = dfield_get_len(dfield);
  auto buf = ext->buf + i * REC_MAX_INDEX_COL_LEN;

  ut_ad(i < ext->n_ext);
  ut_ad(dfield_is_ext(dfield));
  ut_a(f_len >= BTR_EXTERN_FIELD_REF_SIZE);

  if (unlikely(!memcmp(field_ref_zero,
                       field + f_len - BTR_EXTERN_FIELD_REF_SIZE,
                       BTR_EXTERN_FIELD_REF_SIZE))) {
    /* The BLOB pointer is not set: we cannot fetch it */
    ext->len[i] = 0;
  } else {
    /* Fetch at most REC_MAX_INDEX_COL_LEN of the column.
    The column should be non-empty.  However,
    trx_rollback_or_clean_all_recovered() may try to
    access a half-deleted BLOB if the server previously
    crashed during the execution of
    btr_free_externally_stored_field(). */
    ext->len[i] = btr_copy_externally_stored_field_prefix(
        buf, REC_MAX_INDEX_COL_LEN, zip_size, field, f_len);
  }
}

row_ext_t *row_ext_create(ulint n_ext, const ulint *ext, const dtuple_t *tuple,
                          ulint zip_size, mem_heap_t *heap) {
  auto row_ext = reinterpret_cast<row_ext_t *>(mem_heap_alloc(
      heap, (sizeof(row_ext_t)) + (n_ext - 1) * sizeof(row_ext_t::len)));

  ut_ad(ut_is_2pow(zip_size));
  ut_ad(zip_size <= UNIV_PAGE_SIZE);

  row_ext->n_ext = n_ext;
  row_ext->ext = ext;
  row_ext->buf = mem_heap_alloc(heap, n_ext * REC_MAX_INDEX_COL_LEN);

#ifdef UNIV_DEBUG
  memset(row_ext->buf, 0xaa, n_ext * REC_MAX_INDEX_COL_LEN);
  UNIV_MEM_ALLOC(row_ext->buf, n_ext * REC_MAX_INDEX_COL_LEN);
#endif /* UNIV_DEBUG */

  /* Fetch the BLOB prefixes */
  for (ulint i = 0; i < n_ext; i++) {
    auto dfield = dtuple_get_nth_field(tuple, ext[i]);
    row_ext_cache_fill(row_ext, i, zip_size, dfield);
  }

  return row_ext;
}
