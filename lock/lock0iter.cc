/****************************************************************************
Copyright (c) 2007, 2009, Innobase Oy. All Rights Reserved.
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

/** @file lock/lock0iter.c
Lock queue iterator. Can iterate over table and record
lock queues.

Created July 16, 2007 Vasil Dimov
*******************************************************/

#include "innodb0types.h"
#include "lock0iter.h"
#include "lock0lock.h"
#include "ut0dbg.h"
#include "ut0lst.h"

void Lock_iterator::reset(const Lock *lock, ulint bit_no) noexcept {
  m_current_lock = lock;

  if (bit_no != ULINT_UNDEFINED) {
    m_bit_no = bit_no;
  } else {

    switch (lock->type()) {
      case LOCK_TABLE:
        m_bit_no = ULINT_UNDEFINED;
        break;
      case LOCK_REC:
        m_bit_no = lock->rec_find_set_bit();
        ut_a(m_bit_no != ULINT_UNDEFINED);
        break;
      default:
        ut_error;
    }
  }
}

const Lock *Lock_iterator::prev() noexcept {
  const Lock *prev_lock;

  switch (m_current_lock->type()) {
    case LOCK_REC:
      prev_lock = m_lock_sys->rec_get_prev(m_current_lock, m_bit_no);
      break;
    case LOCK_TABLE:
      prev_lock = UT_LIST_GET_PREV(m_table.m_locks, m_current_lock);
      break;
    default:
      ut_error;
  }

  if (prev_lock != nullptr) {
    m_current_lock = prev_lock;
  }

  return prev_lock;
}
