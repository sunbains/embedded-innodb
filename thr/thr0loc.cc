/**
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file thr/thr0loc.c
The thread local storage

Created 10/5/1995 Heikki Tuuri
*******************************************************/

#include "thr0loc.h"
#ifdef UNIV_NONINL
#include "thr0loc.ic"
#endif

#include "hash0hash.h"
#include "mem0mem.h"
#include "srv0srv.h"
#include "sync0sync.h"

/*
        IMPLEMENTATION OF THREAD LOCAL STORAGE
        ======================================

The threads sometimes need private data which depends on the thread id.
This is implemented as a hash table, where the hash value is calculated
from the thread id, to prepare for a large number of threads. The hash table
is protected by a mutex. If you need modify the program and put new data to
the thread local storage, just add it to struct thr_local_struct in the
header file. */

/** Mutex protecting thr_local_hash */
static mutex_t thr_local_mutex;

/** The hash table. The module is not yet initialized when it is nullptr. */
static hash_table_t *thr_local_hash = nullptr;

/** Thread local data */
typedef struct thr_local_struct thr_local_t;

/** @brief Thread local data.
The private data for each thread should be put to
the structure below and the accessor functions written
for the field. */
struct thr_local_struct {
  os_thread_id_t id;  /*!< id of the thread which owns this struct */
  os_thread_t handle; /*!< operating system handle to the thread */
  ulint slot_no;      /*!< the index of the slot in the thread table
                      for this thread */
  bool in_ibuf;       /*!< true if the thread is doing an ibuf
                       operation */
  hash_node_t hash;   /*!< hash chain node */
  ulint magic_n;      /*!< magic number (THR_LOCAL_MAGIC_N) */
};

/** The value of thr_local_struct::magic_n */
#define THR_LOCAL_MAGIC_N 1231234

/** Returns the local storage struct for a thread.
@return	local storage */
static thr_local_t *thr_local_get(os_thread_id_t id) /*!< in: thread id of the thread */
{
  thr_local_t *local;

try_again:
  ut_ad(thr_local_hash);
  ut_ad(mutex_own(&thr_local_mutex));

  /* Look for the local struct in the hash table */

  local = nullptr;

  HASH_SEARCH(hash, thr_local_hash, os_thread_pf(id), thr_local_t *, local, , os_thread_eq(local->id, id));

  if (local == nullptr) {
    mutex_exit(&thr_local_mutex);

    thr_local_create();

    mutex_enter(&thr_local_mutex);

    goto try_again;
  }

  ut_ad(local->magic_n == THR_LOCAL_MAGIC_N);

  return (local);
}

ulint thr_local_get_slot_no(os_thread_id_t id) /*!< in: thread id of the thread */
{
  ulint slot_no;
  thr_local_t *local;

  mutex_enter(&thr_local_mutex);

  local = thr_local_get(id);

  slot_no = local->slot_no;

  mutex_exit(&thr_local_mutex);

  return (slot_no);
}

void thr_local_set_slot_no(os_thread_id_t id, ulint slot_no) {
  thr_local_t *local;

  mutex_enter(&thr_local_mutex);

  local = thr_local_get(id);

  local->slot_no = slot_no;

  mutex_exit(&thr_local_mutex);
}

bool *thr_local_get_in_ibuf_field(void) {
  thr_local_t *local;

  mutex_enter(&thr_local_mutex);

  local = thr_local_get(os_thread_get_curr_id());

  mutex_exit(&thr_local_mutex);

  return (&(local->in_ibuf));
}

void thr_local_create(void) {
  thr_local_t *local;

  if (thr_local_hash == nullptr) {
    thr_local_init();
  }

  local = static_cast<thr_local_t *>(mem_alloc(sizeof(thr_local_t)));

  local->id = os_thread_get_curr_id();
  local->handle = os_thread_get_curr();
  local->magic_n = THR_LOCAL_MAGIC_N;

  local->in_ibuf = false;

  mutex_enter(&thr_local_mutex);

  HASH_INSERT(thr_local_t, hash, thr_local_hash, os_thread_pf(os_thread_get_curr_id()), local);

  mutex_exit(&thr_local_mutex);
}

void thr_local_free(os_thread_id_t id) /*!< in: thread id */
{
  thr_local_t *local;

  mutex_enter(&thr_local_mutex);

  HASH_SEARCH(hash, thr_local_hash, os_thread_pf(id), thr_local_t *, local, , os_thread_eq(local->id, id));
  if (local == nullptr) {
    mutex_exit(&thr_local_mutex);

    return;
  }

  HASH_DELETE(thr_local_t, hash, thr_local_hash, os_thread_pf(id), local);

  mutex_exit(&thr_local_mutex);

  ut_a(local->magic_n == THR_LOCAL_MAGIC_N);

  mem_free(local);
}

void thr_local_init(void) {
  ut_a(thr_local_hash == nullptr);

  thr_local_hash = hash_create(OS_THREAD_MAX_N + 100);

  mutex_create(&thr_local_mutex, SYNC_THR_LOCAL);
}

void thr_local_close() {
  ulint i;

  ut_a(thr_local_hash != nullptr);

  /* Free the hash elements. We don't remove them from the table
  because we are going to destroy the table anyway. */
  for (i = 0; i < hash_get_n_cells(thr_local_hash); i++) {
    auto local = static_cast<thr_local_t *>(HASH_GET_FIRST(thr_local_hash, i));

    while (local != nullptr) {
      thr_local_t *prev_local = local;

      local = static_cast<thr_local_t *>(HASH_GET_NEXT(hash, prev_local));
      ut_a(prev_local->magic_n == THR_LOCAL_MAGIC_N);
      mem_free(prev_local);
    }
  }

  hash_table_free(thr_local_hash);
  thr_local_hash = nullptr;
}
