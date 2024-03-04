/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** Row select prebuilt structure function.

Created 02/03/2009 Sunny Bains
*******************************************************/

#include "row0prebuilt.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0ins.h"
#include "row0merge.h"

/** Create a prebuilt struct for a user table handle.
@return	own: a prebuilt struct */

row_prebuilt_t *
row_prebuilt_create(dict_table_t *table) /*!< in: Innobase table handle */
{
  ulint sz;
  dtuple_t *ref;
  ulint ref_len;
  ib_row_cache_t *row_cache;
  dict_index_t *clust_index;

  auto heap = mem_heap_create(128);
  auto prebuilt = reinterpret_cast<row_prebuilt_t *>(
      mem_heap_zalloc(heap, sizeof(row_prebuilt_t)));

  prebuilt->magic_n = ROW_PREBUILT_ALLOCATED;
  prebuilt->magic_n2 = ROW_PREBUILT_ALLOCATED;

  prebuilt->heap = heap;

  prebuilt->table = table;

  prebuilt->sql_stat_start = true;

  prebuilt->pcur = btr_pcur_create();
  prebuilt->clust_pcur = btr_pcur_create();

  prebuilt->select_lock_type = LOCK_NONE;

  prebuilt->search_tuple =
      dtuple_create(heap, 2 * dict_table_get_n_cols(table));

  clust_index = dict_table_get_first_index(table);

  /* Make sure that search_tuple is long enough for clustered index */
  ut_a(2 * dict_table_get_n_cols(table) >= clust_index->n_fields);

  ref_len = dict_index_get_n_unique(clust_index);

  ref = dtuple_create(heap, ref_len);

  dict_index_copy_types(ref, clust_index, ref_len);

  prebuilt->clust_ref = ref;

  row_cache = &prebuilt->row_cache;

  row_cache->n_max = FETCH_CACHE_SIZE;
  row_cache->n_size = row_cache->n_max;

  sz = sizeof(*row_cache->ptr) * row_cache->n_max;

  row_cache->heap = mem_heap_create(sz);

  row_cache->ptr =
      reinterpret_cast<ib_cached_row_t *>(mem_heap_zalloc(row_cache->heap, sz));

  return (prebuilt);
}

/** Free a prebuilt struct for a user table handle. */

void row_prebuilt_free(
    row_prebuilt_t *prebuilt, /*!< in, own: prebuilt struct */
    bool dict_locked)         /*!< in: true if dict was locked */
{
  ulint i;
  ib_row_cache_t *row_cache;

  if (prebuilt->magic_n != ROW_PREBUILT_ALLOCATED ||
      prebuilt->magic_n2 != ROW_PREBUILT_ALLOCATED) {
    ib_logger(ib_stream,
              "Error: trying to free a corrupt\n"
              "table handle. Magic n %lu,"
              " magic n2 %lu, table name",
              (ulong)prebuilt->magic_n, (ulong)prebuilt->magic_n2);
    ut_print_name(ib_stream, NULL, true, prebuilt->table->name);
    ib_logger(ib_stream, "\n");

    ut_error;
  }

  prebuilt->magic_n = ROW_PREBUILT_FREED;
  prebuilt->magic_n2 = ROW_PREBUILT_FREED;

  btr_pcur_free(prebuilt->pcur);
  btr_pcur_free(prebuilt->clust_pcur);

  if (prebuilt->sel_graph) {
    que_graph_free_recursive(prebuilt->sel_graph);
  }

  if (prebuilt->old_vers_heap) {
    mem_heap_free(prebuilt->old_vers_heap);
  }

  row_cache = &prebuilt->row_cache;

  for (i = 0; i < row_cache->n_max; i++) {
    ib_cached_row_t *row = &row_cache->ptr[i];

    if (row->ptr != NULL) {
      mem_free(row->ptr);
    }
  }

  mem_heap_free(row_cache->heap);

  if (prebuilt->table != NULL) {
    dict_table_decrement_handle_count(prebuilt->table, dict_locked);
  }

  mem_heap_free(prebuilt->heap);
}

/** Reset a prebuilt struct for a user table handle. */

void row_prebuilt_reset(
    row_prebuilt_t *prebuilt) /*!< in/out: prebuilt struct */
{
  ut_a(prebuilt->magic_n == ROW_PREBUILT_ALLOCATED);
  ut_a(prebuilt->magic_n2 == ROW_PREBUILT_ALLOCATED);

  prebuilt->sql_stat_start = true;
  prebuilt->client_has_locked = false;
  prebuilt->need_to_access_clustered = false;

  prebuilt->index_usable = false;

  prebuilt->simple_select = false;
  prebuilt->select_lock_type = LOCK_NONE;

  if (prebuilt->old_vers_heap) {
    mem_heap_free(prebuilt->old_vers_heap);
    prebuilt->old_vers_heap = NULL;
  }

  prebuilt->trx = NULL;

  if (prebuilt->sel_graph) {
    prebuilt->sel_graph->trx = NULL;
  }
}

/** Updates the transaction pointers in query graphs stored in the prebuilt
struct. */

void row_prebuilt_update_trx(
    row_prebuilt_t *prebuilt, /*!< in/out: prebuilt struct handle */
    trx_t *trx)               /*!< in: transaction handle */
{
  ut_a(trx != NULL);

  if (trx->magic_n != TRX_MAGIC_N) {
    ib_logger(ib_stream,
              "Error: trying to use a corrupt\n"
              "trx handle. Magic n %lu\n",
              (ulong)trx->magic_n);

    ut_error;
  } else if (prebuilt->magic_n != ROW_PREBUILT_ALLOCATED) {
    ib_logger(ib_stream,
              "Error: trying to use a corrupt\n"
              "table handle. Magic n %lu, table name",
              (ulong)prebuilt->magic_n);
    ut_print_name(ib_stream, NULL, true, prebuilt->table->name);
    ib_logger(ib_stream, "\n");

    ut_error;
  } else {
    prebuilt->trx = trx;

    if (prebuilt->sel_graph) {
      prebuilt->sel_graph->trx = trx;
    }

    prebuilt->index_usable =
        row_merge_is_index_usable(prebuilt->trx, prebuilt->index);
  }
}
