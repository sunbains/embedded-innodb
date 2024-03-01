/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.

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

/** @file dict/dict0mem.c
Data dictionary memory object creation

Created 1/8/1996 Heikki Tuuri
***********************************************************************/

#include "dict0mem.h"

#include "dict0dict.h"
#include "lock0types.h"

/** initial memory heap size when creating a table or index object */
constexpr ulint DICT_HEAP_SIZE=  100;

dict_table_t *
dict_mem_table_create(const char *name, ulint space, ulint n_cols, ulint flags) {
  ut_ad(name != nullptr);
  ut_a(!(flags & (~0UL << DICT_TF2_BITS)));

  auto heap = mem_heap_create(DICT_HEAP_SIZE);
  auto table = (dict_table_t *)mem_heap_zalloc(heap, sizeof(dict_table_t));

  table->heap = heap;

  table->flags = (unsigned int)flags;
  table->name = mem_heap_strdup(heap, name);
  table->space = (unsigned int)space;
  table->n_cols = (unsigned int)(n_cols + DATA_N_SYS_COLS);

  table->cols = (dict_col_t *)mem_heap_alloc(heap, (n_cols + DATA_N_SYS_COLS) *
                                                       sizeof(dict_col_t));

  ut_d(table->magic_n = DICT_TABLE_MAGIC_N);
  return table;
}

void dict_mem_table_free(dict_table_t *table) {
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_d(table->cached = false);

  mem_heap_free(table->heap);
}

/** Append 'name' to 'col_names'.  @see dict_table_t::col_names
@param[in] col_names            Existing column names or nullptr
@param[in] cols                 Number of existing columns
@param[in] name                 New column name
@param[in,out] heap             Heap for allocation.
@return	new column names array */
static const char *
dict_add_col_name(const char *col_names, ulint cols, const char *name, mem_heap_t *heap) {
  ut_ad(!cols == !col_names);

  ulint old_len;

  /* Find out length of existing array. */
  if (col_names != nullptr) {
    auto s = col_names;

    for (ulint i = 0; i < cols; i++) {
      s += strlen(s) + 1;
    }

    old_len = s - col_names;
  } else {
    old_len = 0;
  }

  auto new_len = strlen(name) + 1;
  auto total_len = old_len + new_len;
  auto ptr = (char *)mem_heap_alloc(heap, total_len);

  if (old_len > 0) {
    memcpy(ptr, col_names, old_len);
  }

  memcpy(ptr + old_len, name, new_len);

  return ptr;
}

void dict_mem_table_add_col( dict_table_t *table, mem_heap_t *heap, const char *name, ulint mtype, ulint prtype, ulint len) {
  ut_ad(table != nullptr);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(!heap == !name);

  auto i = table->n_def++;

  if (name != nullptr) {
    if (table->n_def == table->n_cols) {
      heap = table->heap;
    }
    if (i > 0 && !table->col_names) {
      /* All preceding column names are empty. */
      table->col_names = (char *)mem_heap_zalloc(heap, table->n_def);
    }

    table->col_names = dict_add_col_name(table->col_names, i, name, heap);
  }

  auto col = dict_table_get_nth_col(table, i);

  col->ind = (unsigned int)i;
  col->ord_part = 0;

  col->mtype = (unsigned int)mtype;
  col->prtype = (unsigned int)prtype;
  col->len = (unsigned int)len;

  ulint mbminlen;
  ulint mbmaxlen;

  dtype_get_mblen(mtype, prtype, &mbminlen, &mbmaxlen);

  col->mbminlen = (unsigned int)mbminlen;
  col->mbmaxlen = (unsigned int)mbmaxlen;
}

dict_index_t * dict_mem_index_create(const char *table_name, const char *index_name, ulint space, ulint type, ulint n_fields) {
  ut_ad(table_name != nullptr);
  ut_ad(index_name != nullptr);

  auto heap = mem_heap_create(DICT_HEAP_SIZE);
  auto index = (dict_index_t *)mem_heap_zalloc(heap, sizeof(dict_index_t));

  index->heap = heap;

  index->type = type;
  index->space = (unsigned int)space;
  index->name = mem_heap_strdup(heap, index_name);
  index->table_name = table_name;
  index->n_fields = (unsigned int)n_fields;

  index->fields = (dict_field_t *)mem_heap_alloc(heap, 1 + n_fields * sizeof(dict_field_t));

  /* The '1 +' above prevents allocation of an empty mem block */

#ifdef UNIV_DEBUG
  index->magic_n = DICT_INDEX_MAGIC_N;
#endif /* UNIV_DEBUG */

  return index;
}

dict_foreign_t *dict_mem_foreign_create(void) {
  auto heap = mem_heap_create(100);
  auto foreign =
      (dict_foreign_t *)mem_heap_zalloc(heap, sizeof(dict_foreign_t));

  foreign->heap = heap;

  return foreign;
}

void dict_mem_index_add_field(dict_index_t *index, const char *name, ulint prefix_len) {
  ut_ad(index != nullptr);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  index->n_def++;

  auto field = dict_index_get_nth_field(index, index->n_def - 1);

  field->name = name;
  field->prefix_len = (unsigned int)prefix_len;
}

void dict_mem_index_free(dict_index_t *index) {
  ut_ad(index != nullptr);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  mem_heap_free(index->heap);
}
