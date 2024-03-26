/**
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.

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

#ifndef row0prebuilt_h
#define row0prebuilt_h

#include "lock0types.h"
#include "row0sel.h"

/** Create a prebuilt struct for a user table handle.
@return	own: a prebuilt struct */

row_prebuilt_t *row_prebuilt_create(dict_table_t *table); /*!< in: Innobase table handle */

/** Free a prebuilt struct for a user table handle. */

void row_prebuilt_free(
  row_prebuilt_t *prebuilt, /*!< in, own: prebuilt struct */
  bool dict_locked
); /*!< in: true if dict was locked */

/** Reset a prebuilt struct for a user table handle. */

void row_prebuilt_reset(row_prebuilt_t *prebuilt); /*!< in/out: prebuilt struct */

/** Updates the transaction pointers in query graphs stored in the prebuilt
struct. */

void row_prebuilt_update_trx(
  row_prebuilt_t *prebuilt, /*!< in/out: prebuilt struct handle */
  trx_t *trx
); /*!< in: transaction handle */

#define FETCH_CACHE_SIZE 16
/* After fetching this many rows, we start caching them in fetch_cache */
#define FETCH_CACHE_THRESHOLD 4

/* Values for hint_need_to_fetch_extra_cols */
#define ROW_RETRIEVE_PRIMARY_KEY 1
#define ROW_RETRIEVE_ALL_COLS 2

#define ROW_PREBUILT_ALLOCATED 78540783
#define ROW_PREBUILT_FREED 26423527

#define ROW_PREBUILT_FETCH_MAGIC_N 465765687

/* An InnoDB cached row. */
typedef struct ib_cached_row_struct {
  ulint max_len; /* max len of rec if not nullptr */
  ulint rec_len; /* length of valid data in rec */
  rec_t *rec;    /* cached record, pointer into the
                 start of the record data */
  byte *ptr;     /* pointer to start of record */
} ib_cached_row_t;

/** Cache for rows fetched when positioning the cursor. */
typedef struct ib_row_cache_struct {
  mem_heap_t *heap; /* memory heap for cached rows */

  ib_cached_row_t *ptr; /* a cache for fetched rows if we
                        fetch many rows from the same cursor:
                        it saves CPU time to fetch them in a
                        batch. */
  unsigned n_max : 10;  /* max size of the row cache. */

  unsigned n_size : 10;   /* current max setting, must be <=
                          n_max */
  unsigned first : 10;    /* position of the first not yet
                          fetched row in fetch_cache */
  unsigned n_cached : 10; /* number of not yet accessed rows
                          in fetch_cache */
  ib_cur_op_t direction;  /* ROW_SEL_NEXT or ROW_SEL_PREV */
} ib_row_cache_t;

/* A struct for (sometimes lazily) prebuilt structures in an Innobase table
handle used within the API; these are used to save CPU time. */

struct row_prebuilt_struct {
  ulint magic_n;               /* this magic number is set to
                               ROW_PREBUILT_ALLOCATED when created,
                               or ROW_PREBUILT_FREED when the
                               struct has been freed */
  unsigned sql_stat_start : 1; /* true when we start processing of
                              an SQL statement: we may have to set
                              an intention lock on the table,
                              create a consistent read view etc. */
  unsigned client_has_locked : 1;
  /* this is set true when a client
  calls explicit lock on this handle
  with a lock flag, and set false when
  with unlocked */
  unsigned clust_index_was_generated : 1;
  /* if the user did not define a
  primary key, then Innobase
  automatically generated a clustered
  index where the ordering column is
  the row id: in this case this flag
  is set to true */
  unsigned need_to_access_clustered : 1;
  /* if we are fetching
  columns through a secondary index
  and at least one column is not in
  the secondary index, then this is
  set to true */
  unsigned index_usable : 1;  /* caches the value of
                              row_merge_is_index_usable(trx,index) */
  unsigned simple_select : 1; /* true if plain select */
  unsigned new_rec_locks : 2; /* normally 0; if
                              srv_locks_unsafe_for_binlog is
                              true or session is using READ
                              COMMITTED isolation level, in a
                              cursor search, if we set a new
                              record lock on an index, this is
                              incremented; this is used in
                              releasing the locks under the
                              cursors if we are performing an
                              UPDATE and we determine after
                              retrieving the row that it does
                              not need to be locked; thus,
                              these can be used to implement a
                              'mini-rollback' that releases
                              the latest record locks */
  mem_heap_t *heap;           /* memory heap from which
                              these auxiliary structures are
                              allocated when needed */
  dict_table_t *table;        /* Innobase table handle */
  dict_index_t *index;        /* current index for a search, if
                              any */
  trx_t *trx;                 /* current transaction handle */
  btr_pcur_t *pcur;           /* persistent cursor used in selects
                              and updates */
  btr_pcur_t *clust_pcur;     /* persistent cursor used in
                              some selects and updates */
  que_fork_t *sel_graph;      /* dummy query graph used in
                              selects */
  dtuple_t *search_tuple;     /* prebuilt dtuple used in selects */
  byte row_id[DATA_ROW_ID_LEN];
  /* if the clustered index was
  generated, the row id of the
  last row fetched is stored
  here */
  dtuple_t *clust_ref;             /* prebuilt dtuple used in
                                   sel/upd/del */
  enum lock_mode select_lock_type; /* LOCK_NONE, LOCK_S, or LOCK_X */
  mem_heap_t *old_vers_heap;       /* memory heap where a previous
                                   version is built in consistent read */
  ib_row_cache_t row_cache;        /* rows cached by select read ahead */

  int result;     /* Result of the last compare in
                  row_search_for_client(). */
  ulint magic_n2; /* this should be the same as
                  magic_n */
};

#endif /* row0prebuilt_h */
