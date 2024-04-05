/*****************************************************************************

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

/**************************************************/ /**
 @file include/lock0types.h
 The transaction lock system global types

 Created 5/7/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"
#include "ut0lst.h"

struct trx_t;
struct Lock;
struct lock_sys_t;
struct dict_table_t;
struct dict_index_t;

/* Basic lock modes */
enum Lock_mode {
  /* Intention shared */
  LOCK_IS = 0,

  /* Intention exclusive */
  LOCK_IX,             

  /* Shared */
  LOCK_S,

  /* Exclusive */
  LOCK_X,

  /* Locks the auto-inc counter of a table in an exclusive mode */
  LOCK_AUTO_INC,       

  /* This is used elsewhere to note consistent read */
  LOCK_NONE,

  /* Number of lock modes */
  LOCK_NUM = LOCK_NONE
};

/** A table lock */
struct Table_lock {
  /** Database table in dictionary cache */
  dict_table_t *table;

  /** List of locks on the same table */
  UT_LIST_NODE_T(Lock) locks;
};

/** Record lock for a page */
struct Rec_lock {
  /** Tablespace ID. */
  space_id_t space;

  /** Page number in space. */
  page_no_t page_no;

  /** Number of bits in the lock bitmap; NOTE: the lock bitmap
  is placed immediately after the lock struct */
  ulint n_bits;
};

/** Lock struct */
struct Lock {
  /** Transaction owning the lock */
  trx_t *trx;

  /** list of the locks of the transaction */
  UT_LIST_NODE_T(Lock) trx_locks;

  /*r< Lock type, mode, LOCK_GAP or LOCK_REC_NOT_GAP, LOCK_INSERT_INTENTION, wait flag, ORed */
  ulint type_mode;

  /** Hash chain node for a record lock */
  void* hash;

  /** Index for a record lock */
  dict_index_t *index;

  union {
    /** Table lock */
    Table_lock tab_lock;

    /** Record lock */
    Rec_lock rec_lock;

  /** lock details */
  } un_member;
};

