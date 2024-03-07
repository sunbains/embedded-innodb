/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.

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

/** @file ha/hash0hash.c
The simple hash table utility

Created 5/20/1997 Heikki Tuuri
*******************************************************/

#include "hash0hash.h"
#include "mem0mem.h"

void hash_mutex_enter(hash_table_t *table, ulint fold) {
  mutex_enter(hash_get_mutex(table, fold));
}

void hash_mutex_exit(hash_table_t *table, ulint fold) {
  mutex_exit(hash_get_mutex(table, fold));
}

void hash_mutex_enter_all(hash_table_t *table) {
  for (ulint i = 0; i < table->n_mutexes; i++) {

    mutex_enter(table->mutexes + i);
  }
}

void hash_mutex_exit_all(hash_table_t *table) {
  for (ulint i = 0; i < table->n_mutexes; i++) {

    mutex_exit(table->mutexes + i);
  }
}

hash_table_t *hash_create(ulint n) {
  auto prime = ut_find_prime(n);
  auto table = (hash_table_t *)mem_alloc(sizeof(hash_table_t));
  auto array = (hash_cell_t *)ut_malloc(sizeof(hash_cell_t) * prime);

  table->array = array;
  table->n_cells = prime;
  table->n_mutexes = 0;
  table->mutexes = NULL;
  table->heaps = NULL;
  table->heap = NULL;
  ut_d(table->magic_n = HASH_TABLE_MAGIC_N);

  /* Initialize the cell array */
  hash_table_clear(table);

  return table;
}

void hash_table_free(hash_table_t *table) {
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  ut_a(table->mutexes == NULL);

  ut_free(table->array);
  mem_free(table);
}

void hash_create_mutexes_func(
  hash_table_t *table,
#ifdef UNIV_SYNC_DEBUG
  ulint sync_level,
#endif /* UNIV_SYNC_DEBUG */
  ulint n_mutexes
) {

  ut_a(n_mutexes > 0);
  ut_a(ut_is_2pow(n_mutexes));
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

  table->mutexes = (mutex_t *)mem_alloc(n_mutexes * sizeof(mutex_t));

  for (ulint i = 0; i < n_mutexes; i++) {
    mutex_create(table->mutexes + i, sync_level);
  }

  table->n_mutexes = n_mutexes;
}

void hash_free_mutexes_func(hash_table_t *table) {
  for (ulint i = 0; i < table->n_mutexes; i++) {
    mutex_free(&table->mutexes[i]);
#ifdef UNIV_DEBUG
    memset(&table->mutexes[i], 0x0, sizeof(table->mutexes[i]));
#endif /* UNIV_DEBUG */
  }

  mem_free(table->mutexes);
}
