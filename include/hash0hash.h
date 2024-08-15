/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/hash0hash.h
The simple hash table utility

Created 5/20/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "mem0mem.h"
#include "sync0sync.h"
#include "ut0rnd.h"

struct hash_table_t;
struct hash_cell_t;

using hash_node_t = void *;

/* To avoid symbol name clashes. */
#define hash_create ib_hash_create

/** Creates a hash table with >= n array cells. The actual number
of cells is chosen to be a prime number slightly bigger than n.
@return	own: created table */
hash_table_t *hash_create(ulint n); /*!< in: number of array cells */

/** Creates a mutex array to protect a hash table. */
void hash_create_mutexes_func(
  hash_table_t *table, /*!< in: hash table */
#ifdef UNIV_SYNC_DEBUG
  ulint sync_level, /*!< in: latching order level of the
                      mutexes: used in the debug version */
#endif              /* UNIV_SYNC_DEBUG */
  ulint n_mutexes
); /*!< in: number of mutexes */

#ifdef UNIV_SYNC_DEBUG

#define hash_create_mutexes(t, n, level) hash_create_mutexes_func(t, level, n)

#else /* UNIV_SYNC_DEBUG */

#define hash_create_mutexes(t, n, level) hash_create_mutexes_func(t, n)

#endif /* UNIV_SYNC_DEBUG */

/** Frees a mutex array created with hash_create_mutexes_func(). */
void hash_free_mutexes_func(hash_table_t *table); /*!< in,own: hash table */

#define hash_free_mutexes(t) hash_free_mutexes_func(t)

/** Frees a hash table. */
void hash_table_free(hash_table_t *table); /*!< in, own: hash table */

/** Assert that the mutex for the table in a hash operation is owned. */
#define HASH_ASSERT_OWNED(TABLE, FOLD) ut_ad(!(TABLE)->mutexes || mutex_own(hash_get_mutex(TABLE, FOLD)));

/** Inserts a struct to a hash table. */

#define HASH_INSERT(TYPE, NAME, TABLE, FOLD, DATA)                    \
  do {                                                                \
    hash_cell_t *cell3333;                                            \
    TYPE *struct3333;                                                 \
                                                                      \
    HASH_ASSERT_OWNED(TABLE, FOLD)                                    \
                                                                      \
    (DATA)->NAME = nullptr;                                           \
                                                                      \
    cell3333 = hash_get_nth_cell(TABLE, hash_calc_hash(FOLD, TABLE)); \
                                                                      \
    if (cell3333->node == nullptr) {                                  \
      cell3333->node = DATA;                                          \
    } else {                                                          \
      struct3333 = (TYPE *)cell3333->node;                            \
                                                                      \
      while (struct3333->NAME != nullptr) {                           \
                                                                      \
        struct3333 = (TYPE *)struct3333->NAME;                        \
      }                                                               \
                                                                      \
      struct3333->NAME = DATA;                                        \
    }                                                                 \
  } while (0)

#ifdef UNIV_HASH_DEBUG
#define HASH_ASSERT_VALID(DATA) ut_a((void *)(DATA) != (void *)-1)
#define HASH_INVALIDATE(DATA, NAME) DATA->NAME = (void *)-1
#else
#define HASH_ASSERT_VALID(DATA) \
  do {                          \
  } while (0)
#define HASH_INVALIDATE(DATA, NAME) \
  do {                              \
  } while (0)
#endif

/** Deletes a struct from a hash table. */

#define HASH_DELETE(TYPE, NAME, TABLE, FOLD, DATA)                    \
  do {                                                                \
    hash_cell_t *cell3333;                                            \
    TYPE *struct3333;                                                 \
                                                                      \
    HASH_ASSERT_OWNED(TABLE, FOLD)                                    \
                                                                      \
    cell3333 = hash_get_nth_cell(TABLE, hash_calc_hash(FOLD, TABLE)); \
                                                                      \
    if (cell3333->node == DATA) {                                     \
      HASH_ASSERT_VALID(DATA->NAME);                                  \
      cell3333->node = DATA->NAME;                                    \
    } else {                                                          \
      struct3333 = (TYPE *)cell3333->node;                            \
                                                                      \
      while (struct3333->NAME != DATA) {                              \
                                                                      \
        struct3333 = (TYPE *)struct3333->NAME;                        \
        ut_a(struct3333);                                             \
      }                                                               \
                                                                      \
      struct3333->NAME = DATA->NAME;                                  \
    }                                                                 \
    HASH_INVALIDATE(DATA, NAME);                                      \
  } while (0)

/** Gets the first struct in a hash chain, nullptr if none. */

#define HASH_GET_FIRST(TABLE, HASH_VAL) (hash_get_nth_cell(TABLE, HASH_VAL)->node)

/** Gets the next struct in a hash chain, nullptr if none. */

#define HASH_GET_NEXT(NAME, DATA) ((DATA)->NAME)

/** Looks for a struct in a hash table. */
#define HASH_SEARCH(NAME, TABLE, FOLD, TYPE, DATA, ASSERTION, TEST)    \
  {                                                                    \
                                                                       \
    HASH_ASSERT_OWNED(TABLE, FOLD)                                     \
                                                                       \
    (DATA) = (TYPE)HASH_GET_FIRST(TABLE, hash_calc_hash(FOLD, TABLE)); \
    HASH_ASSERT_VALID(DATA);                                           \
                                                                       \
    while ((DATA) != nullptr) {                                        \
      ASSERTION;                                                       \
      if (TEST) {                                                      \
        break;                                                         \
      } else {                                                         \
        HASH_ASSERT_VALID(HASH_GET_NEXT(NAME, DATA));                  \
        (DATA) = (TYPE)HASH_GET_NEXT(NAME, DATA);                      \
      }                                                                \
    }                                                                  \
  }

/** Looks for an item in all hash buckets. */
#define HASH_SEARCH_ALL(NAME, TABLE, TYPE, DATA, ASSERTION, TEST) \
  do {                                                            \
    ulint i3333;                                                  \
                                                                  \
    for (i3333 = (TABLE)->n_cells; i3333--;) {                    \
      (DATA) = (TYPE)HASH_GET_FIRST(TABLE, i3333);                \
                                                                  \
      while ((DATA) != nullptr) {                                 \
        HASH_ASSERT_VALID(DATA);                                  \
        ASSERTION;                                                \
                                                                  \
        if (TEST) {                                               \
          break;                                                  \
        }                                                         \
                                                                  \
        (DATA) = (TYPE)HASH_GET_NEXT(NAME, DATA);                 \
      }                                                           \
                                                                  \
      if ((DATA) != nullptr) {                                    \
        break;                                                    \
      }                                                           \
    }                                                             \
  } while (0)

/** Deletes a struct which is stored in the heap of the hash table, and compacts
the heap. The fold value must be stored in the struct NODE in a field named
'fold'. */

#define HASH_DELETE_AND_COMPACT(TYPE, NAME, TABLE, NODE)                                 \
  do {                                                                                   \
    TYPE *node111;                                                                       \
    TYPE *top_node111;                                                                   \
    hash_cell_t *cell111;                                                                \
    ulint fold111;                                                                       \
                                                                                         \
    fold111 = (NODE)->fold;                                                              \
                                                                                         \
    HASH_DELETE(TYPE, NAME, TABLE, fold111, NODE);                                       \
                                                                                         \
    top_node111 = (TYPE *)mem_heap_get_top(hash_get_heap(TABLE, fold111), sizeof(TYPE)); \
                                                                                         \
    /* If the node to remove is not the top node in the heap, compact the      \
    heap of nodes by moving the top node in the place of NODE. */         \
                                                                                         \
    if (NODE != top_node111) {                                                           \
                                                                                         \
      /* Copy the top node in place of NODE */                                           \
                                                                                         \
      *(NODE) = *top_node111;                                                            \
                                                                                         \
      cell111 = hash_get_nth_cell(TABLE, hash_calc_hash(top_node111->fold, TABLE));      \
                                                                                         \
      /* Look for the pointer to the top node, to update it */                           \
                                                                                         \
      if (cell111->node == top_node111) {                                                \
        /* The top node is the first in the chain */                                     \
                                                                                         \
        cell111->node = NODE;                                                            \
      } else {                                                                           \
        /* We have to look for the predecessor of the top                      \
        node */         \
        node111 = (TYPE *)cell111->node;                                                 \
                                                                                         \
        while (top_node111 != HASH_GET_NEXT(NAME, node111)) {                            \
                                                                                         \
          node111 = HASH_GET_NEXT(NAME, node111);                                        \
        }                                                                                \
                                                                                         \
        /* Now we have the predecessor node */                                           \
                                                                                         \
        node111->NAME = NODE;                                                            \
      }                                                                                  \
    }                                                                                    \
                                                                                         \
    /* Free the space occupied by the top node */                                        \
                                                                                         \
    mem_heap_free_top(hash_get_heap(TABLE, fold111), sizeof(TYPE));                      \
  } while (0)

/** Move all hash table entries from OLD_TABLE to NEW_TABLE. */

#define HASH_MIGRATE(OLD_TABLE, NEW_TABLE, NODE_TYPE, PTR_NAME, FOLD_FUNC) \
  do {                                                                     \
    ulint i2222;                                                           \
    ulint cell_count2222;                                                  \
                                                                           \
    cell_count2222 = hash_get_n_cells(OLD_TABLE);                          \
                                                                           \
    for (i2222 = 0; i2222 < cell_count2222; i2222++) {                     \
      NODE_TYPE *node2222 = HASH_GET_FIRST((OLD_TABLE), i2222);            \
                                                                           \
      while (node2222) {                                                   \
        NODE_TYPE *next2222 = node2222->PTR_NAME;                          \
        ulint fold2222 = FOLD_FUNC(node2222);                              \
                                                                           \
        HASH_INSERT(NODE_TYPE, PTR_NAME, (NEW_TABLE), fold2222, node2222); \
                                                                           \
        node2222 = next2222;                                               \
      }                                                                    \
    }                                                                      \
  } while (0)

/** Reserves the mutex for a fold value in a hash table. */
void hash_mutex_enter(
  hash_table_t *table, /*!< in: hash table */
  ulint fold
); /*!< in: fold */

/** Releases the mutex for a fold value in a hash table. */
void hash_mutex_exit(
  hash_table_t *table, /*!< in: hash table */
  ulint fold
); /*!< in: fold */

/** Reserves all the mutexes of a hash table, in an ascending order. */
void hash_mutex_enter_all(hash_table_t *table); /*!< in: hash table */

/** Releases all the mutexes of a hash table. */
void hash_mutex_exit_all(hash_table_t *table); /*!< in: hash table */

struct hash_cell_t {
  /** Hash chain node, nullptr if none */
  hash_node_t node;
};

constexpr ulint HASH_TABLE_MAGIC_N = 76561114;

/* The hash table structure */
struct hash_table_t {
  /** Number of cells in the hash table */
  ulint n_cells;

  /** Pointer to cell array */
  hash_cell_t *array;

  /** If mutexes != nullptr, then the number of mutexes,
  must be a power of 2 */
  ulint n_mutexes;

  /* nullptr, or an array of mutexes used to protect
  segments of the hash table */
  mutex_t *mutexes;

  /** If this is non-nullptr, hash chain nodes for external
  chaining can be allocated from these memory heaps; there
  are then n_mutexes many of these heaps */
  mem_heap_t **heaps;

  mem_heap_t *heap;

#ifdef UNIV_DEBUG
  ulint magic_n;
#endif /* UNIV_DEBUG */
};

/** Gets the nth cell in a hash table.
@return	pointer to cell */
inline hash_cell_t *hash_get_nth_cell(
  hash_table_t *table, /*!< in: hash table */
  ulint n
) /*!< in: cell index */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  ut_ad(n < table->n_cells);

  return (table->array + n);
}

/** Clears a hash table so that all the cells become empty. */
inline void hash_table_clear(hash_table_t *table) /*!< in/out: hash table */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  memset(table->array, 0x0, table->n_cells * sizeof(*table->array));
}

/** Returns the number of cells in a hash table.
@return	number of cells */
inline ulint hash_get_n_cells(hash_table_t *table) /*!< in: table */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  return (table->n_cells);
}

/** Calculates the hash value from a folded value.
@return	hashed value */
inline ulint hash_calc_hash(
  ulint fold, /*!< in: folded value */
  hash_table_t *table
) /*!< in: hash table */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  return (ut_hash_ulint(fold, table->n_cells));
}

/** Gets the mutex index for a fold value in a hash table.
@return	mutex number */
inline ulint hash_get_mutex_no(
  hash_table_t *table, /*!< in: hash table */
  ulint fold
) /*!< in: fold */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  ut_ad(ut_is_2pow(table->n_mutexes));
  return (ut_2pow_remainder(hash_calc_hash(fold, table), table->n_mutexes));
}

/** Gets the nth heap in a hash table.
@return	mem heap */
inline mem_heap_t *hash_get_nth_heap(
  hash_table_t *table, /*!< in: hash table */
  ulint i
) /*!< in: index of the heap */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  ut_ad(i < table->n_mutexes);

  return (table->heaps[i]);
}

/** Gets the heap for a fold value in a hash table.
@return	mem heap */
inline mem_heap_t *hash_get_heap(
  hash_table_t *table, /*!< in: hash table */
  ulint fold
) /*!< in: fold */
{
  ulint i;

  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

  if (table->heap) {
    return (table->heap);
  }

  i = hash_get_mutex_no(table, fold);

  return (hash_get_nth_heap(table, i));
}

/** Gets the nth mutex in a hash table.
@return	mutex */
inline mutex_t *hash_get_nth_mutex(
  hash_table_t *table, /*!< in: hash table */
  ulint i
) /*!< in: index of the mutex */
{
  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
  ut_ad(i < table->n_mutexes);

  return (table->mutexes + i);
}

/** Gets the mutex for a fold value in a hash table.
@return	mutex */
inline mutex_t *hash_get_mutex(
  hash_table_t *table, /*!< in: hash table */
  ulint fold
) /*!< in: fold */
{
  ulint i;

  ut_ad(table);
  ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

  i = hash_get_mutex_no(table, fold);

  return (hash_get_nth_mutex(table, i));
}
