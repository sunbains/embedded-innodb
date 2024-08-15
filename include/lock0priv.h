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

/** @file include/lock0priv.h
Lock module internal structures and methods.

Created July 12, 2007 Vasil Dimov
*******************************************************/

#pragma once

#include "lock0types.h"

UT_LIST_NODE_GETTER_DEFINITION(Lock, trx_locks);

/**
 * Gets the previous record lock set on a record.
 *
 * @param[in] in_lock   in: record lock
 * @param[in] heap_no   in: heap number of the record
 * @return              previous lock on the same record, nullptr if none exists
 */
const Lock *lock_rec_get_prev(const Lock *in_lock, ulint heap_no);

/** Gets the type of a lock.
@param[in]c lock      Get the type for this lock.
@return	LOCK_TABLE or LOCK_REC */
inline ulint lock_get_type_low(const Lock *lock) {
  return lock->type_mode & LOCK_TYPE_MASK;
}
