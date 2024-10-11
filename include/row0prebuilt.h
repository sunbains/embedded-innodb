/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** Row select prebuilt structure definition.

Created 02/03/2009 Sunny Bains
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "lock0types.h"
#include "row0sel.h"

struct trx_t;
struct Table;

constexpr ulint FETCH_CACHE_SIZE = 6;

/* After fetching this many rows, we start caching them in fetch_cache */
constexpr ulint FETCH_CACHE_THRESHOLD  = 4;

/* Values for hint_need_to_fetch_extra_cols */
constexpr ulint ROW_RETRIEVE_PRIMARY_KEY = 1;

constexpr ulint ROW_RETRIEVE_ALL_COLS = 2;

constexpr ulint ROW_PREBUILT_ALLOCATED = 78540783;

constexpr ulint ROW_PREBUILT_FREED = 26423527;

constexpr ulint ROW_PREBUILT_FETCH_MAGIC_N = 465765687;

/**
 * Create a prebuilt struct for a user table handle.
 *
 * @param table Innobase table handle
 * @return own: a prebuilt struct
 */
row_prebuilt_t *row_prebuilt_create(Table *table);

/**
 * Free a prebuilt struct for a user table handle.
 *
 * @param prebuilt in, own: prebuilt struct
 * @param dict_locked true if dict was locked
 */
void row_prebuilt_free(row_prebuilt_t *prebuilt, bool dict_locked);

/**
 * Reset a prebuilt struct for a user table handle.
 *
 * @param prebuilt in/out: prebuilt struct
 */
void row_prebuilt_reset(row_prebuilt_t *prebuilt);

/**
 * Updates the transaction pointers in query graphs stored in the prebuilt struct.
 *
 * @param prebuilt in/out: prebuilt struct handle
 * @param trx transaction handle
 */
void row_prebuilt_update_trx(row_prebuilt_t *prebuilt, trx_t *trx);

/* An InnoDB cached row. */
struct ib_cached_row_t {
  /** Max len of rec if not nullptr */
  ulint max_len;

  /** Length of valid data in rec */
  ulint rec_len;

  /** Cached record, pointer into the start of the record data */
  rec_t *rec;

  /* Pointer to start of record */
  rec_t *ptr;
};

/** Cache for rows fetched when positioning the cursor. */
struct ib_row_cache_t {
   /** Memory heap for cached rows */
  mem_heap_t *heap;

  /** A cache for fetched rows if we fetch many rows from
  the same cursor: it saves CPU time to fetch them in a batch. */
  ib_cached_row_t *ptr;

  /** Max size of the row cache. */
  unsigned n_max : 10;

  /** Current max setting, must be <= n_max */
  unsigned n_size : 10;

  /* Position of the first not yet fetched row in fetch_cache */
  unsigned first : 10;

  /** Number of not yet accessed rows in fetch_cache */
  unsigned n_cached : 10;

  /** ROW_SEL_NEXT or ROW_SEL_PREV */
  ib_cur_op_t direction;
};

/** A struct for (sometimes lazily) prebuilt structures in an Innobase table
handle used within the API; these are used to save CPU time. */
struct row_prebuilt_struct {
  /** This magic number is set to ROW_PREBUILT_ALLOCATED when
   * created, or ROW_PREBUILT_FREED when the struct has been freed */
  ulint magic_n;

  /** True when we start processing of an SQL statement:
   * we may have to set an intention lock on the table, create
   * a consistent read view etc. */
  bool sql_stat_start;

  /** This is set true when a client calls explicit lock on this
   * handle with a lock flag, and set false when with unlocked */
  bool client_has_locked;

  /** If the user did not define a primary key, then Innobase automatically
   * generated a clustered index where the ordering column is the row id:
   * in this case this flag is set to true */
  bool clust_index_was_generated;

  /** If we are fetching columns through a secondary index and at least
   * one column is not in the secondary index, then this is set to true */
  bool need_to_access_clustered;

  /** Caches the value of row_merge_is_index_usable(trx,index) */
  bool index_usable;

  /** true if plain select */
  bool simple_select;

  /** Normally 0; if srv_locks_unsafe_for_binlog is true or session is using
   * READ COMMITTED isolation level, in a cursor search, if we set a new
   * record lock on an index, this is incremented; this is used in releasing
   * the locks under the cursors if we are performing an UPDATE and we determine
   * after retrieving the row that it does not need to be locked; thus,
   * these can be used to implement a 'mini-rollback' that releases the
   * latest record locks */
  unsigned new_rec_locks : 2;

  /** memory heap from which these auxiliary structures are allocated when needed */
  mem_heap_t *heap;

  /** Table handle */
  Table *table;

  /** Current index for a search, if any */
  Index *index;

  /** Current transaction handle */
  trx_t *trx;

  /** Persistent cursor used in selects and updates */
  Btree_pcursor *pcur;

  /** Persistent cursor used in some selects and updates */
  Btree_pcursor *clust_pcur;

  /** Dummy query graph used in selects */
  que_fork_t *sel_graph;

  /** Prebuilt dtuple used in selects */
  DTuple *search_tuple;

  /** if the clustered index was generated, the row id of the last row
   * fetched is stored here */
  byte row_id[DATA_ROW_ID_LEN];

  /** Prebuilt dtuple used in sel/upd/del */
  DTuple *clust_ref;

  /** LOCK_NONE, LOCK_S, or LOCK_X */
  Lock_mode select_lock_type;

  /** Memory heap where a previous version is built in consistent read */
  mem_heap_t *old_vers_heap;

  /** Rows cached by select read ahead */
  ib_row_cache_t row_cache;

  /** Result of the last compare in row_search_mvcc(). */
  int result;

  /** This should be the same as magic_n */
  ulint magic_n2;
};
