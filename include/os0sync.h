/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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
#include <mutex>
#include <condition_variable>

/** An asynchronous signal sent between threads */
struct Cond_var {
  /**
   * Constructor
   * 
   * Note: Why is signal_count set to 1? We return this value in reset(),
   * which can then be be used to pass to the wait_low(). The value
   * of zero is reserved in wait() for the case when the caller
   * does not * want to pass any signal_count value. To distinguish
   * between the two cases we initialize signal_count to 1 here.
   */
  Cond_var() : m_is_set(), m_signal_count(1) { }

  /**
   * Destructor
   */
  ~Cond_var() = default;

  /**
  * Sets an event semaphore to the signaled state: lets waiting threads proceed.
  */
  void set();

  /**
  * Resets an event semaphore to the nonsignaled state. Waiting threads will
  * stop to wait for the event. The return value should be passed to wait()
  * if it is desired that this thread should not wait in case of an intervening
  * call to set() between this os_event_reset() and the wait() call.
  * See comments wait().
  * @return the value to be passed to wait()
  */
  int64_t reset();

  /** Waits for an event object until it is in the signaled state. If
  * srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS this also exits the
  * waiting thread when the event becomes signaled (or immediately if the
  * event is already in the signaled state).
  *
  * Typically, if the event has been signalled after the reset()
  * we'll return immediately because is_set == true.
  * There are, however, situations (e.g.: sync_array code) where we may
  * lose this information. For example:
  *
  * thread A calls reset()
  * thread B calls set()   [is_set == true]
  * thread C calls reset() [is_set == false]
  * thread A calls wait()  [infinite wait!]
  * thread C calls wait()  [infinite wait!]
  *
  * @param[in] reset_sig_count      Zero or the value returned by previous call of reset().
  *
  * Where such a scenario is possible, to avoid infinite wait, the
  * value returned by reset() should be passed in as reset_sig_count. */
  void wait(int64_t reset_sig_count);

  /**
   * Waits for an event object until it is in the signaled state or
   * a timeout is exceeded. In Unix the timeout is always infinite.
   * @param[in] timeout              Timeout, or std::chrono::microseconds::max()
   * @param[in] reset_sig_count      Zero or the value returned by previous call of reset().
   * @return 0 if success, OS_SYNC_TIME_EXCEEDED if timeout was exceeded */
  ulint wait_time(std::chrono::microseconds timeout, int64_t reset_sig_count);

  /**
   * Creates an event semaphore, i.e., a semaphore which may just have two
   * states: signaled and nonsignaled. The created event is manual reset: it must be
   * reset explicitly by calling sync_os_reset_event.
   * @param[in] name the name of the event, if nullptr the event is created without a name
   * @return the event handle
   */
  static Cond_var* create(const char *name);

  /**
   * Frees an event object.
   * @param[in] event event to free
   */
  static void destroy(Cond_var* event);

  /** This mutex protects the next fields */
  std::mutex m_mutex{};

  /** This is true when the event is in the signaled state,
   * i.e., a thread does not stop if it tries to wait for this event */
  bool m_is_set;

  /** This is incremented each time the event becomes signaled */
  int64_t m_signal_count;

  /** Condition variable is used in waiting for the event */
  std::condition_variable m_cond_var{};

  /** List of all created events */
  UT_LIST_NODE_T(Cond_var) m_os_event_list;

  using Events = UT_LIST_BASE_NODE_T(Cond_var, m_os_event_list);

  /** Mutex protecting s_events && s_count */
  static std::mutex s_mutex;

  /* For tracking all the created events. */
  static Events s_events;

  /* Number of events created. */
  static ulint s_count;
};

/** Operating system mutex */
struct OS_mutex;

/** Denotes an infinite delay for os_event_wait_time() */
constexpr ulint OS_SYNC_INFINITE_TIME = ((ulint)(-1));

/** Return value of os_event_wait_time() when the time is exceeded */
constexpr ulint OS_SYNC_TIME_EXCEEDED = 1;

/** This is incremented by 1 in os_thread_create and decremented by 1 in
os_thread_exit */
extern std::atomic_int os_thread_count;

/** Initializes global event and OS 'slow' mutex lists. */
void os_sync_init();

/** Frees created events and OS 'slow' mutexes. */
void os_sync_free();

/**
 * Creates an event semaphore, i.e., a semaphore which may just have two
 * states: signaled and nonsignaled. The created event is manual reset: it must be
 * reset explicitly by calling sync_os_reset_event.
 * @param[in] name the name of the event, if nullptr the event is created without a name
 * @return the event handle
 */
#define os_event_create(n) Cond_var::create(n)

#define os_event_free(e) Cond_var::destroy(e)

#define os_event_set(e) (e)->set()

#define os_event_reset(e) e->reset()

#define os_event_wait_low(e, r) (e)->wait(r)

#define os_event_wait(e) (e)->wait(0)

#define os_fast_mutex_lock(m) (m)->lock()

#define os_fast_mutex_unlock(m) (m)->unlock()

/**
 * Creates an operating system mutex semaphore. Because these are slow, the
 * mutex semaphore of InnoDB itself (mutex_t) should be used where possible.
 *
 * @return The mutex handle
 */
OS_mutex *os_mutex_create(const char *);

/**
 * Locks the mutex.
 *
 * @param[in] mutex Mutex to acquire
 */
void os_mutex_enter(OS_mutex *mutex);

/**
 * Unlocks the mutex
 *
 * @param[in] mutex Mutex to acquire
 */
void os_mutex_exit(OS_mutex *mutex);

/**
 * Frees an mutex object.
 *
 * @param[in] mutex Mutex to free
 */
void os_mutex_destroy(OS_mutex *mutex);

/** @return the number of mutexes created. */
ulint os_mutex_count();

/**
 * Reset the variables.
 */
void os_sync_var_init();
