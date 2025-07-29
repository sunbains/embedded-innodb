/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

Prebuilt::Prebuilt(FSP *fsp, Btree *btree, Table *table, mem_heap_t *heap) noexcept : m_table(table) {

  m_magic_n = ROW_PREBUILT_ALLOCATED;
  m_heap = heap;

  {
    auto ptr = ut_new(sizeof(Btree_pcursor));
    m_pcur = new (ptr) Btree_pcursor(fsp, btree);
  }

  {
    auto ptr = ut_new(sizeof(Btree_pcursor));
    m_clust_pcur = new (ptr) Btree_pcursor(fsp, btree);
  }

  m_search_tuple = dtuple_create(m_heap, 2 * m_table->get_n_cols());

  auto clust_index = m_table->get_clustered_index();

  /* Make sure that search_tuple is long enough for clustered index */
  ut_a(2 * m_table->get_n_cols() >= clust_index->get_n_fields());

  const auto ref_len = clust_index->get_n_unique();

  m_clust_ref = dtuple_create(m_heap, ref_len);

  clust_index->copy_types(m_clust_ref, ref_len);

  m_row_cache.m_n_max = FETCH_CACHE_SIZE;
  m_row_cache.m_n_size = m_row_cache.m_n_max;

  {
    const auto sz = sizeof(*m_row_cache.m_cached_rows) * m_row_cache.m_n_max;

    m_row_cache.m_heap = mem_heap_create(sz);

    auto ptr = mem_heap_zalloc(m_row_cache.m_heap, sz);

    m_row_cache.m_cached_rows = new (ptr) Cached_row[m_row_cache.m_n_max];
  }

  m_magic_n2 = ROW_PREBUILT_ALLOCATED;
}

Prebuilt::~Prebuilt() noexcept {
  if (m_magic_n != ROW_PREBUILT_ALLOCATED || m_magic_n2 != ROW_PREBUILT_ALLOCATED) {
    log_fatal(std::format(
      "Trying to free a corrupt table handle. Magic n {}, magic n2 {}, table name {}", m_magic_n, m_magic_n2, m_table->m_name
    ));
  }

  m_magic_n = ROW_PREBUILT_FREED;
  m_magic_n2 = ROW_PREBUILT_FREED;

  ut_delete(m_pcur);
  ut_delete(m_clust_pcur);

  if (m_sel_graph != nullptr) {
    que_graph_free_recursive(m_sel_graph);
  }

  if (m_old_vers_heap != nullptr) {
    mem_heap_free(m_old_vers_heap);
  }

  for (ulint i{}; i < m_row_cache.m_n_max; ++i) {
    auto &cached_row = m_row_cache.m_cached_rows[i];

    if (cached_row.m_ptr != nullptr) {
      mem_free(cached_row.m_ptr);
    }
  }

  mem_heap_free(m_row_cache.m_heap);
}

Prebuilt *Prebuilt::create(FSP *fsp, Btree *btree, Table *table) noexcept {
  auto heap = mem_heap_create(128);
  auto ptr = reinterpret_cast<Prebuilt *>(mem_heap_zalloc(heap, sizeof(Prebuilt)));

  return new (ptr) Prebuilt(fsp, btree, table, heap);
}

void Prebuilt::destroy(Dict *dict, Prebuilt *prebuilt, bool dict_locked) noexcept {
  if (prebuilt->m_table != nullptr) {
    dict->table_decrement_handle_count(prebuilt->m_table, dict_locked);
  }

  call_destructor(prebuilt);

  mem_heap_free(prebuilt->m_heap);
}

void Prebuilt::clear() noexcept {
  ut_a(m_magic_n == ROW_PREBUILT_ALLOCATED);
  ut_a(m_magic_n2 == ROW_PREBUILT_ALLOCATED);

  m_index_usable = false;
  m_simple_select = false;
  m_sql_stat_start = true;
  m_client_has_locked = false;
  m_need_to_access_clustered = false;
  m_select_lock_type = LOCK_NONE;

  if (m_old_vers_heap != nullptr) {
    mem_heap_free(m_old_vers_heap);
    m_old_vers_heap = nullptr;
  }

  m_trx = nullptr;

  if (m_sel_graph != nullptr) {
    m_sel_graph->trx = nullptr;
  }
}

void Prebuilt::update_trx(Trx *trx) noexcept {
  ut_a(trx != nullptr);

  if (trx->m_magic_n != TRX_MAGIC_N) {

    log_fatal(std::format("Trying to use a corrupt trx handle. Magic n {}", trx->m_magic_n));

  } else if (m_magic_n != ROW_PREBUILT_ALLOCATED) {

    log_fatal(std::format("Trying to use a corrupt table handle: {}. Magic n {}, table name", m_table->m_name, m_magic_n));

  } else {

    m_trx = trx;

    if (m_sel_graph != nullptr) {
      m_sel_graph->trx = trx;
    }

    m_index_usable = row_merge_is_index_usable(m_trx, m_index);
  }
}
