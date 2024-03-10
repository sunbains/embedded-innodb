/****************************************************************************
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

/** @file include/sync0arr.h
The wait array used in synchronization primitives

Created 9/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "os0thread.h"
#include "ut0lst.h"
#include "ut0mem.h"

/** Synchronization wait array cell */
struct Sync_cell;

/** Synchronization wait array */
struct Sync_check;

/** Parameters for sync_array_create() @{ */

/** protected by os_mutex_t */
constexpr ulint SYNC_ARRAY_OS_MUTEX = 1;

/** protected by mutex_t */
constexpr ulint SYNC_ARRAY_MUTEX = 2;

/* @} */

/** Creates a synchronization wait array. It is protected by a mutex
which is automatically reserved when the functions operating on it
are called.
@return	own: created wait array */
Sync_check *sync_array_create(
  ulint n_cells, /** in: number of cells in the array
                                     to create */
  ulint protection
); /** in: either SYNC_ARRAY_OS_MUTEX or
                                     SYNC_ARRAY_MUTEX: determines the type
                                     of mutex protecting the data structure */

/** Frees the resources in a wait array. */
void sync_array_free(Sync_check *arr); /** in, own: sync wait array */

/** Reserves a wait array cell for waiting for an object.
The event of the cell is reset to nonsignalled state. */
void sync_array_reserve_cell(
  Sync_check *arr, /** in: wait array */
  void *object,      /** in: pointer to the object to wait for */
  ulint type,        /** in: lock request type */
  const char *file,  /** in: file where requested */
  ulint line,        /** in: line where requested */
  ulint *index
); /** out: index of the reserved cell */

/** This function should be called when a thread starts to wait on
a wait array cell. In the debug version this function checks
if the wait for a semaphore will result in a deadlock, in which
case prints info and asserts. */
void sync_array_wait_event(
  Sync_check *arr, /** in: wait array */
  ulint index
); /** in: index of the reserved cell */

/** Frees the cell. NOTE! sync_array_wait_event frees the cell
automatically! */
void sync_array_free_cell(
  Sync_check *arr, /** in: wait array */
  ulint index
); /** in: index of the cell in array */

/** Note that one of the wait objects was signalled. */
void sync_array_object_signalled(Sync_check *arr); /** in: wait array */

/** If the wakeup algorithm does not work perfectly at semaphore relases,
this function will do the waking (see the comment in mutex_exit). This
function should be called about every 1 second in the server. */
void sync_arr_wake_threads_if_sema_free();

/** Prints warnings of long semaphore waits to stderr.
@return	true if fatal semaphore wait threshold was exceeded */
bool sync_array_print_long_waits(void);

/** Prints info of the wait array. */
void sync_array_print_info(
  ib_stream_t ib_stream, /** in: stream where to print */
  Sync_check *arr
); /** in: wait array */
