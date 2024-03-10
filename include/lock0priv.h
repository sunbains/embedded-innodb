/****************************************************************************
Copyright (c) 2007, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/lock0priv.h
Lock module internal structures and methods.

Created July 12, 2007 Vasil Dimov
*******************************************************/

#pragma once

#ifndef LOCK_MODULE_IMPLEMENTATION
/* If you need to access members of the structures defined in this
file, please write appropriate functions that retrieve them and put
those functions in lock/ */
#error Do not include lock0priv.h outside of the lock/ module
#endif

#include "dict0types.h"
#include "hash0hash.h"
#include "innodb0types.h"
#include "trx0types.h"
#include "ut0lst.h"

/** A table lock */
typedef struct lock_table_struct lock_table_t;

/** A table lock */
struct lock_table_struct {
  /** Database table in dictionary cache */
  dict_table_t *table;

  /** List of locks on the same table */
  UT_LIST_NODE_T(lock_t) locks;
};

/** Record lock for a page */
typedef struct lock_rec_struct lock_rec_t;

/** Record lock for a page */
struct lock_rec_struct {
  /** Tablespace ID. */
  space_id_t space;

  /** Page number in space. */
  page_no_t page_no;

  /** Number of bits in the lock bitmap; NOTE: the lock bitmap
  is placed immediately after the lock struct */
  ulint n_bits;
};

/** Lock struct */
struct lock_struct {
  /** Transaction owning the lock */
  trx_t *trx;

  /** list of the locks of the transaction */
  UT_LIST_NODE_T(lock_t) trx_locks;

  /*r< Lock type, mode, LOCK_GAP or LOCK_REC_NOT_GAP, LOCK_INSERT_INTENTION, wait flag, ORed */
  ulint type_mode;

  /** Hash chain node for a record lock */
  hash_node_t hash;

  /** Index for a record lock */
  dict_index_t *index;

  union {
    /** Table lock */
    lock_table_t tab_lock;

    /** Record lock */
    lock_rec_t rec_lock;

  /** lock details */
  } un_member;
};

UT_LIST_NODE_GETTER_DEFINITION(lock_t, trx_locks);

/** Gets the previous record lock set on a record.
@return	previous lock on the same record, NULL if none exists */
const lock_t *lock_rec_get_prev(
  const lock_t *in_lock, /*!< in: record lock */
  ulint heap_no
); /*!< in: heap number of the record */


/** Gets the type of a lock.
@param[in]c lock      Get the type for this lock.
@return	LOCK_TABLE or LOCK_REC */
inline ulint lock_get_type_low(const lock_t *lock) {
  return lock->type_mode & LOCK_TYPE_MASK;
}
