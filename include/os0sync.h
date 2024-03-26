/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** @file include/os0sync.h
The interface to the operating system
synchronization primitives.

Created 9/6/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "ut0lst.h"

#include <pthread.h>

#include <atomic>
#include <chrono>

/** Native mutex */
typedef pthread_mutex_t os_fast_mutex_t;

/** An asynchronous signal sent between threads */
struct OS_cond {
  /** this mutex protects the next fields */
  os_fast_mutex_t os_mutex;

  /** this is true when the event is in the signaled state,
  i.e., a thread does not stop if it tries to wait for this event */
  bool is_set;

  /** this is incremented each time the event becomes signaled */
  int64_t signal_count;

  /** condition variable is used in waiting for the event */
  pthread_cond_t cond_var;

  /** list of all created events */
  UT_LIST_NODE_T(OS_cond) os_event_list;
};

/** Operating system mutex */
typedef struct os_mutex_struct os_mutex_str_t;

/** Operating system mutex handle */
typedef os_mutex_str_t *os_mutex_t;

/** Denotes an infinite delay for os_event_wait_time() */
constexpr ulint OS_SYNC_INFINITE_TIME = ((ulint)(-1));

/** Return value of os_event_wait_time() when the time is exceeded */
constexpr ulint OS_SYNC_TIME_EXCEEDED = 1;

/** Mutex protecting counts and the event and OS 'slow' mutex lists */
extern os_mutex_t os_sync_mutex;

/** This is incremented by 1 in os_thread_create and decremented by 1 in
os_thread_exit */
extern std::atomic_int os_thread_count;

extern ulint os_event_count;
extern ulint os_mutex_count;
extern ulint os_fast_mutex_count;

/** Initializes global event and OS 'slow' mutex lists. */
void os_sync_init(void);

/** Frees created events and OS 'slow' mutexes. */
void os_sync_free(void);

/** Creates an event semaphore, i.e., a semaphore which may just have two
states: signaled and nonsignaled. The created event is manual reset: it must be
reset explicitly by calling sync_os_reset_event.
@return	the event handle */
OS_cond* os_event_create(const char *name); /** in: the name of the event, if nullptr
                                   the event is created without a name */

/** Sets an event semaphore to the signaled state: lets waiting threads
proceed. */
void os_event_set(OS_cond* event); /** in: event to set */

/** Resets an event semaphore to the nonsignaled state. Waiting threads will
stop to wait for the event.
The return value should be passed to os_even_wait_low() if it is desired
that this thread should not wait in case of an intervening call to
os_event_set() between this os_event_reset() and the
os_event_wait_low() call. See comments for os_event_wait_low(). */
int64_t os_event_reset(OS_cond* event); /** in: event to reset */

/** Frees an event object. */
void os_event_free(OS_cond* event); /** in: event to free */

/** Waits for an event object until it is in the signaled state. If
srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS this also exits the
waiting thread when the event becomes signaled (or immediately if the
event is already in the signaled state).

Typically, if the event has been signalled after the os_event_reset()
we'll return immediately because event->is_set == true.
There are, however, situations (e.g.: sync_array code) where we may
lose this information. For example:

thread A calls os_event_reset()
thread B calls os_event_set()   [event->is_set == true]
thread C calls os_event_reset() [event->is_set == false]
thread A calls os_event_wait()  [infinite wait!]
thread C calls os_event_wait()  [infinite wait!]

@param[in,out] event            Event to wait
@param[in] reset_sig_count      Zero or the value returned by previous call of os_event_reset().

Where such a scenario is possible, to avoid infinite wait, the
value returned by os_event_reset() should be passed in as
reset_sig_count. */
void os_event_wait_low(OS_cond* event, int64_t reset_sig_count);


/** Waits for an event object until it is in the signaled state or
a timeout is exceeded. In Unix the timeout is always infinite.
@param[in,out] event            Event to wait
@param[in] timeout              Timeout, or std::chrono::microseconds::max()
@param[in] reset_sig_count      Zero or the value returned by previous call of os_event_reset().
@return 0 if success, OS_SYNC_TIME_EXCEEDED if timeout was exceeded */
ulint os_event_wait_time_low(OS_cond* event, std::chrono::microseconds timeout, int64_t reset_sig_count);

#define os_event_wait(event) os_event_wait_low(event, 0)

/** Waits for an event object until it is in the signaled state or
a timeout is exceeded. In Unix the timeout is always infinite.
@return	0 if success, OS_SYNC_TIME_EXCEEDED if timeout was exceeded */

ulint os_event_wait_time(
  OS_cond* event, /** in: event to wait */
  ulint time
); /** in: timeout in microseconds, or
                                           OS_SYNC_INFINITE_TIME */

/** Creates an operating system mutex semaphore. Because these are slow, the
mutex semaphore of InnoDB itself (mutex_t) should be used where possible.
@return	the mutex handle */
os_mutex_t os_mutex_create(const char *name); /** in: the name of the mutex, if nullptr
                                   the mutex is created without a name */

/** Acquires ownership of a mutex semaphore. */
void os_mutex_enter(os_mutex_t mutex); /** in: mutex to acquire */

/** Releases ownership of a mutex. */
void os_mutex_exit(os_mutex_t mutex); /** in: mutex to release */

/** Frees an mutex object. */
void os_mutex_free(os_mutex_t mutex); /** in: mutex to free */


/** Releases ownership of a fast mutex. */
void os_fast_mutex_unlock(os_fast_mutex_t *fast_mutex); /** in: mutex to release */

/** Initializes an operating system fast mutex semaphore. */
void os_fast_mutex_init(os_fast_mutex_t *fast_mutex); /** in: fast mutex */

/** Acquires ownership of a fast mutex. */
void os_fast_mutex_lock(os_fast_mutex_t *fast_mutex); /** in: mutex to acquire */

/** Frees an mutex object. */
void os_fast_mutex_free(os_fast_mutex_t *fast_mutex); /** in: mutex to free */

/** Reset the variables. */
void os_sync_var_init();
