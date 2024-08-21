/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/os0thread.h
The interface to the operating system
process and thread control primitives

Created 9/8/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include <pthread.h>

/* Maximum number of threads which can be created in the program;
this is also the size of the wait slot array for user threads which
can wait inside InnoDB */

#define OS_THREAD_MAX_N srv_max_n_threads

/* Possible fixed priorities for threads */
constexpr ulint OS_THREAD_PRIORITY_NONE = 100;
constexpr ulint OS_THREAD_PRIORITY_BACKGROUND = 1;
constexpr ulint OS_THREAD_PRIORITY_NORMAL = 2;
constexpr ulint OS_THREAD_PRIORITY_ABOVE_NORMAL = 3;

using os_thread_t = pthread_t;

/** In Unix we use the thread handle itself as the id of the thread */
using os_thread_id_t = os_thread_t;

/**
 * Compares two thread ids for equality.
 *
 * @param a - OS thread or thread id
 * @param b - OS thread or thread id
 * @return true if equal
 */
bool os_thread_eq(os_thread_id_t a, os_thread_id_t b);

/**
 * Converts an OS thread id to a ulint. It is NOT guaranteed that the ulint is
 * unique for the thread though!
 *
 * @param a - OS thread identifier
 * @return thread identifier as a number
 */
ulint os_thread_pf(os_thread_id_t a);

/** Creates a new thread of execution. The execution starts from the function
given. The start function takes a void* parameter and returns a ulint.
NOTE: We count the number of threads in os_thread_exit(). A created
thread should always use that to exit and not use return() to exit.

@param[in] f                    Function to run.
@param[in] arg                  Argument to function if any
@param[out] thread_id           The new thread ID.

@return	handle to the thread. */
os_thread_t os_thread_create(void *(*f)(void *), void *arg, os_thread_id_t *thread_id);

/** Exits the current thread. */
void os_thread_exit();

/** Returns the thread identifier of current thread.
@return	current thread identifier */
os_thread_id_t os_thread_get_curr_id();

/** Returns handle to the current thread.
@return	current thread handle */
os_thread_t os_thread_get_curr();

/** Advises the os to give up remainder of the thread's time slice. */
void os_thread_yield() noexcept;

/**
 * The thread sleeps at least the time given in microseconds.
 *
 * @param tm - time in microseconds
 */
void os_thread_sleep(ulint tm) noexcept;

/** Gets the last operating system error code for the calling thread.
@return	last error on Windows, 0 otherwise */
ulint os_thread_get_last_error();
