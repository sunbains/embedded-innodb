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

/** @file include/lock0iter.h
Lock queue iterator type and function prototypes.

Created July 16, 2007 Vasil Dimov
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "lock0types.h"

struct Lock_sys;

/**
 * @brief Iterator for traversing lock queues.
 *
 * This struct provides an iterator for traversing lock queues. It can iterate over record
 * locks or table locks. The iterator maintains the current position in the queue and
 * provides methods to move forward or backward in the queue.
 */
struct Lock_iterator {
  explicit Lock_iterator(Lock_sys *lock_sys) noexcept : m_lock_sys(lock_sys) {}

  /**
   * @brief Initialize lock queue iterator.
   *
   * This function initializes the lock queue iterator so that it starts to iterate from
   * the specified lock. The bit_no parameter specifies the record number within the heap
   * where the record is stored. It can be undefined (ULINT_UNDEFINED) in two cases:
   * 1. If the lock is a table lock, thus we have a table lock queue;
   * 2. If the lock is a record lock and it is a wait lock. In this case, bit_no is 
   *    calculated in this function by using lock_rec_find_set_bit(). There is exactly 
   *    one bit set in the bitmap of a wait lock.
   *
   * @param[out] iter The iterator to initialize.
   * @param[in] lock The lock to start from.
   * @param[in] bit_no The record number in the heap.
   */
  void reset(const Lock *lock, ulint bit_no) noexcept;

  /**
   * @brief Get the previous lock in the lock queue.
   *
   * This function retrieves the previous lock in the lock queue. If there are no more locks
   * (i.e., the current lock is the first one), it returns nullptr. The iterator is receded
   * (if not-nullptr is returned).
   *
   * @return The previous lock or nullptr.
   */
  const Lock *prev() noexcept;

  /** In case this is a record lock queue (not table lock queue)
  then bit_no is the record number within the heap in which the
  record is stored. */
  ulint m_bit_no{ULINT_UNDEFINED};

  /** The current lock in the lock queue. */
  const Lock *m_current_lock{nullptr};

  /** The lock system. */
  Lock_sys *m_lock_sys{};
};
