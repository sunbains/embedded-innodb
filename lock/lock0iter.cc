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

/** @file lock/lock0iter.c
Lock queue iterator. Can iterate over table and record
lock queues.

Created July 16, 2007 Vasil Dimov
*******************************************************/

#define LOCK_MODULE_IMPLEMENTATION

#include "lock0iter.h"
#include "innodb0types.h"
#include "lock0lock.h"
#include "lock0priv.h"
#include "ut0dbg.h"
#include "ut0lst.h"

void lock_queue_iterator_reset(lock_queue_iterator_t *iter, const lock_t *lock, ulint bit_no) {
  iter->current_lock = lock;

  if (bit_no != ULINT_UNDEFINED) {

    iter->bit_no = bit_no;
  } else {

    switch (lock_get_type_low(lock)) {
      case LOCK_TABLE:
        iter->bit_no = ULINT_UNDEFINED;
        break;
      case LOCK_REC:
        iter->bit_no = lock_rec_find_set_bit(lock);
        ut_a(iter->bit_no != ULINT_UNDEFINED);
        break;
      default:
        ut_error;
    }
  }
}

const lock_t *lock_queue_iterator_get_prev(lock_queue_iterator_t *iter) {
  const lock_t *prev_lock;

  switch (lock_get_type_low(iter->current_lock)) {
    case LOCK_REC:
      prev_lock = lock_rec_get_prev(iter->current_lock, iter->bit_no);
      break;
    case LOCK_TABLE:
      prev_lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, iter->current_lock);
      break;
    default:
      ut_error;
  }

  if (prev_lock != nullptr) {

    iter->current_lock = prev_lock;
  }

  return (prev_lock);
}
