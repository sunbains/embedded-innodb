/*****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file os/os0sync.c
The interface to the operating system
synchronization primitives.

Created 9/6/1995 Heikki Tuuri
*******************************************************/

#include <atomic>
#include <thread>
#include "os0sync.h"

#include "srv0srv.h"
#include "ut0mem.h"

/** The number of microseconds in a second. */
constexpr uint64_t MICROSECS_IN_A_SECOND = 1000000;

/** The number of nanoseconds in a second. */
constexpr uint64_t NANOSECS_IN_A_SECOND = 1000 * MICROSECS_IN_A_SECOND;

/* Type definition for an operating system mutex struct */
struct OsMutex {
  /** Constructor */
  OsMutex() : m_event(nullptr), m_ptr(nullptr), m_count(0) {
    auto std_mutex = new (std::nothrow) std::mutex;
    m_ptr = std_mutex;
    m_event = CondVar::create(nullptr);
  }

  /** Destructor */
  ~OsMutex() {
    delete static_cast<std::mutex *>(m_ptr);
    CondVar::destroy(m_event);
  }

  /* Create an instance of a mutex. 
  @return the mutex */
  static OsMutex *create(const char *);

  /** Destroy an instance of a mutex created with create()
   * @param mutex the mutex to destroy */
  static void destroy(OsMutex *);

  void lock() {
    ut_a(m_owner.load(std::memory_order_relaxed) != std::this_thread::get_id());

    static_cast<std::mutex *>(m_ptr)->lock();

    m_owner.store(std::this_thread::get_id(), std::memory_order_relaxed);

    ++m_count;

    ut_a(m_count == 1);
  }

  void unlock() {
    ut_a(m_count == 1);

    --m_count;

    m_owner.store(std::thread::id{}, std::memory_order_relaxed);

    static_cast<std::mutex *>(m_ptr)->unlock();
  }

  /** Used by sync0arr.c for queing threads */
  CondVar *m_event{};

  /** OS handle to mutex */
  void *m_ptr{};

  /** we use this counter to check that the same thread
  does not recursively lock the mutex: we do not assume
  that the OS mutex supports recursive locking, though NT seems to do that */
  uint16_t m_count{};

  /** The thread holding the mutex, to catch a recursive lock. */
  std::atomic<std::thread::id> m_owner{};

};

CondVar *CondVar::create(const char *) {
  return new CondVar;
}

void CondVar::set() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (!m_is_set) {
    m_is_set = true;
    ++m_signal_count;
    m_cond_var.notify_all();
  }
}

int64_t CondVar::reset() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (m_is_set) {
    m_is_set = false;
  }

  return m_signal_count;
}

void CondVar::destroy(CondVar *event) {
  delete event;
}

void CondVar::wait(int64_t reset_sig_count) {
  std::unique_lock<std::mutex> lk(m_mutex);
  auto old_sig_count = reset_sig_count != 0 ? reset_sig_count : m_signal_count;

  m_cond_var.wait(lk, [this, old_sig_count] {
    return m_is_set || m_signal_count != old_sig_count;
  });
}

ulint CondVar::wait_time(std::chrono::microseconds timeout, int64_t reset_sig_count) {
  using Clock = std::chrono::steady_clock;

  const auto now = Clock::now();
  const auto room = std::chrono::duration_cast<std::chrono::microseconds>(Clock::time_point::max() - now);
  auto timepoint = Clock::time_point::max();

  if (timeout.count() >= 0 && timeout < room) {
    timepoint = now + timeout;
  }

  std::unique_lock<std::mutex> lk(m_mutex);

  if (reset_sig_count == 0) {
    reset_sig_count = m_signal_count;
  }

  auto ret = m_cond_var.wait_until(lk, timepoint, [this, reset_sig_count] {
    return m_is_set || m_signal_count != reset_sig_count;
  });

  return ret ? 0 : OS_SYNC_TIME_EXCEEDED;
}

OsMutex *OsMutex::create(const char *) {
  return new (std::nothrow) OsMutex;
}

void os_mutex_enter(OsMutex *mutex) {
  mutex->lock();
}

void os_mutex_exit(OsMutex *mutex) {
  mutex->unlock();
}

void OsMutex::destroy(OsMutex *mutex) {
  delete mutex;
}

OsMutex *os_mutex_create(const char *) {
  return OsMutex::create(nullptr);
}

void os_mutex_destroy(OsMutex *mutex) {
  OsMutex::destroy(mutex);
}

