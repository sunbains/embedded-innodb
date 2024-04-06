/*****************************************************************************
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

/** @file os/os0sync.c
The interface to the operating system
synchronization primitives.

Created 9/6/1995 Heikki Tuuri
*******************************************************/

#include "os0sync.h"

#include "srv0start.h"
#include "ut0mem.h"

/** The number of microseconds in a second. */
constexpr uint64_t MICROSECS_IN_A_SECOND = 1000000;

/** The number of nanoseconds in a second. */
constexpr uint64_t NANOSECS_IN_A_SECOND = 1000 * MICROSECS_IN_A_SECOND;

/* Type definition for an operating system mutex struct */
struct OS_mutex {
  /** Constructor */
  OS_mutex() : m_event(nullptr), m_ptr(nullptr), m_count(0) {
    auto std_mutex = new (std::nothrow) std::mutex;
    m_ptr = std_mutex;
    m_event = Cond_var::create(nullptr);
   }

  /** Destructor */
   ~OS_mutex() {
     delete static_cast<std::mutex*>(m_ptr);
     Cond_var::destroy(m_event);
   }

  /* Create an instance of a mutex. 
  @return the mutex */
  static OS_mutex* create(const char*);

  /** Destroy an instance of a mutex created with create()
   * @param mutex the mutex to destroy */
  static void destroy(OS_mutex*);

  void lock() {
    static_cast<std::mutex*>(m_ptr)->lock();

    ++m_count;

    ut_a(m_count == 1);
  }

  void unlock() {
    ut_a(m_count == 1);

    --m_count;

    static_cast<std::mutex *>(m_ptr)->unlock();
  }

  /** Used by sync0arr.c for queing threads */
  Cond_var* m_event{};

  /** OS handle to mutex */
  void *m_ptr{};

  /** we use this counter to check that the same thread
  does not recursively lock the mutex: we do not assume
  that the OS mutex supports recursive locking, though NT seems to do that */
  uint16_t m_count{};

  /** list of all 'slow' OS mutexes created */
  UT_LIST_NODE_T(OS_mutex) m_os_mutex_list;

  using Mutexes = UT_LIST_BASE_NODE_T(OS_mutex, m_os_mutex_list);

  /** Mutex protecting counts and the lists of OS mutexes and events */
  static std::mutex s_mutex;

  /* For tracking all mutexes created. */
  static Mutexes s_mutexes;

  /** Number of mutexes created. */
  static ulint s_count;
};

/** This is incremented by 1 in os_thread_create and decremented by 1 in
os_thread_exit */
std::atomic_int os_thread_count{};

std::mutex OS_mutex::s_mutex{};
std::mutex Cond_var::s_mutex{};

OS_mutex::Mutexes OS_mutex::s_mutexes{};
Cond_var::Events Cond_var::s_events{};

ulint OS_mutex::s_count = 0;
ulint Cond_var::s_count = 0;

void os_sync_var_init() {
  os_thread_count.store(0, std::memory_order_release);
}

void os_sync_init() {
  UT_LIST_INIT(Cond_var::s_events);
  UT_LIST_INIT(OS_mutex::s_mutexes);
}

void os_sync_free() {
  for (auto event = UT_LIST_GET_FIRST(Cond_var::s_events);
       event != nullptr;
       event = UT_LIST_GET_FIRST(Cond_var::s_events)) {

    Cond_var::destroy(event);
  }

  for (auto mutex = UT_LIST_GET_FIRST(OS_mutex::s_mutexes);
       mutex != nullptr;
       mutex = UT_LIST_GET_FIRST(OS_mutex::s_mutexes)) {
     
    OS_mutex::destroy(mutex);
  }
}

Cond_var* Cond_var::create(const char *) {
  auto event = new Cond_var;

  std::lock_guard<std::mutex> lock(s_mutex);

  /* Put to the list of events */
  UT_LIST_ADD_FIRST(Cond_var::s_events, event);

  ++s_count;

  return event;
}

void Cond_var::set() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (!m_is_set) {
    m_is_set = true;
    ++m_signal_count;
    m_cond_var.notify_all();
  }
}

int64_t Cond_var::reset() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (m_is_set) {
    m_is_set = false;
  }

  return m_signal_count;
}

void Cond_var::destroy(Cond_var* event) {
  std::lock_guard<std::mutex> lock(s_mutex);

  UT_LIST_REMOVE(s_events, event);

  --s_count;

  delete event;
}

void Cond_var::wait(int64_t reset_sig_count) {
  std::unique_lock<std::mutex> lk(m_mutex);
  auto old_sig_count = reset_sig_count != 0 ? reset_sig_count : m_signal_count;

  m_cond_var.wait(lk, [this, old_sig_count] {
    if (srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS) {
      log_err("srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS");
      os_thread_exit(nullptr);
    }
    return m_is_set || m_signal_count != old_sig_count; }
  );
}

ulint Cond_var::wait_time(std::chrono::microseconds timeout, int64_t reset_sig_count) {
  std::chrono::time_point<std::chrono::system_clock> timepoint;

  if (timeout != std::chrono::microseconds::max()) {
    timepoint = std::chrono::system_clock::now() + timeout;
  } else {
    timepoint = std::chrono::time_point<std::chrono::system_clock>::max();
  } 

  std::unique_lock<std::mutex> lk(m_mutex);

  if (reset_sig_count == 0) {
    reset_sig_count = m_signal_count;
  }

  auto ret = m_cond_var.wait_until(lk, timepoint, [this, reset_sig_count] {
    if (srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS) {
      log_err("srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS");
      os_thread_exit(nullptr);
    }
    return m_is_set || m_signal_count != reset_sig_count; }
  );

  return ret ? 0 : OS_SYNC_TIME_EXCEEDED;
}

OS_mutex *OS_mutex::create(const char *) {
  auto mutex = new (std::nothrow) OS_mutex;

  std::lock_guard<std::mutex> lock(s_mutex);

  UT_LIST_ADD_FIRST(s_mutexes, mutex);

  ++s_count;

  return mutex;
}

void os_mutex_enter(OS_mutex *mutex) {
  mutex->lock();
}

void os_mutex_exit(OS_mutex *mutex) {
  mutex->unlock();
}

void OS_mutex::destroy(OS_mutex *mutex) {
  std::lock_guard<std::mutex> lock(s_mutex);

  UT_LIST_REMOVE(s_mutexes, mutex);

  --s_count;

  delete mutex;
}

OS_mutex *os_mutex_create(const char*) {
  return OS_mutex::create(nullptr);
}

void os_mutex_destroy(OS_mutex *mutex) {
  OS_mutex::destroy(mutex);
}

ulint os_mutex_count() {
  return OS_mutex::s_count;
}