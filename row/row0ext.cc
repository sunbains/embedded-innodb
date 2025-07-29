/****************************************************************************
Copyright (c) 2006, 2009, Innobase Oy. All Rights Reserved.
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

/** @file row/row0ext.c
Caching of externally stored column prefixes

Created September 2006 Marko Makela
*******************************************************/

#include "row0ext.h"
#include "btr0blob.h"
#include "btr0cur.h"

/** Fills the column prefix cache of an externally stored column.
@param[in,out] ext              Column prefix cache
@param[in] i                    Index of ext->extp[]
@paramp[in] dfield              Data field. */
static void row_ext_cache_fill(row_ext_t *ext, ulint i, const dfield_t *dfield) {
  Blob blob(srv_fsp, srv_btree_sys);
  auto field = (const byte *)dfield_get_data(dfield);
  auto f_len = dfield_get_len(dfield);
  auto buf = ext->buf + i * REC_MAX_INDEX_COL_LEN;

  ut_ad(i < ext->n_ext);
  ut_ad(dfield_is_ext(dfield));
  ut_a(f_len >= BTR_EXTERN_FIELD_REF_SIZE);

  if (unlikely(!memcmp(field_ref_zero, field + f_len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE))) {
    /* The BLOB pointer is not set: we cannot fetch it */
    ext->len[i] = 0;
  } else {
    /* Fetch at most REC_MAX_INDEX_COL_LEN of the column.
    The column should be non-empty.  However,
    trx_rollback_or_clean_all_recovered() may try to
    access a half-deleted BLOB if the server previously
    crashed during the execution of
    btr_free_externally_stored_field(). */
    ext->len[i] = blob.copy_externally_stored_field_prefix(buf, REC_MAX_INDEX_COL_LEN, field, f_len);
  }
}

row_ext_t *row_ext_create(ulint n_ext, const ulint *ext, const DTuple *tuple, mem_heap_t *heap) {
  auto row_ext = reinterpret_cast<row_ext_t *>(mem_heap_alloc(heap, (sizeof(row_ext_t)) + (n_ext - 1) * sizeof(row_ext_t::len)));

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
    row_ext_cache_fill(row_ext, i, dfield);
  }

  return row_ext;
}
