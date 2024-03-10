/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file sync/sync0arr.c
The wait array used in synchronization primitives

Created 9/5/1995 Heikki Tuuri
*******************************************************/

#include <sstream>
#include <vector>

#include "sync0arr.h"

#include "os0file.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"

/*
                        WAIT ARRAY
                        ==========

The wait array consists of cells each of which has an an operating system
event object created for it. The threads waiting for a mutex, for example,
can reserve a cell in the array and suspend themselves to wait for the event
to become signaled. When using the wait array, remember to make sure that
some thread holding the synchronization object will eventually know that
there is a waiter in the array and signal the object, to prevent infinite
wait.  Why we chose to implement a wait array? First, to make mutexes fast,
we had to code our own implementation of them, which only in usually
uncommon cases resorts to using slow operating system primitives. Then we
had the choice of assigning a unique OS event for each mutex, which would
be simpler, or using a global wait array. In some operating systems, the
global wait array solution is more efficient and flexible, because we can
do with a very small number of OS events, say 200. In NT 3.51, allocating
events seems to be a quadratic algorithm, because 10 000 events are created
fast, but 100 000 events takes a couple of minutes to create.

As of 5.0.30 the above mentioned design is changed. Since now OS can
handle millions of wait events efficiently, we no longer have this concept
of each cell of wait array having one event.  Instead, now the event that
a thread wants to wait on is embedded in the wait object (mutex or rw_lock).
We still keep the global wait array for the sake of diagnostics and also
to avoid infinite wait The error_monitor thread scans the global wait
array to signal any waiting threads who have missed the signal. */

/** A cell where an individual thread may wait suspended
until a resource is released. The suspending is implemented
using an operating system event semaphore. */
struct Sync_cell {
  /** Determines if we can wake up the thread waiting for a sempahore. */
  bool can_wake_up();

  /** Returns the event that the thread owning the cell waits for. */
  OS_cond* get_event();

  /** Reports info of a wait array cell.
  @param[in,out] stream         Where to print */
  void print(ib_stream_t ib_stream);

  /** pointer to the object the thread is waiting for; if nullptr the cell is free for use */
  void *m_wait_object{};

  /** the latest wait mutex in cell */
  mutex_t *m_old_wait_mutex{};

  /** the latest wait rw-lock in cell */
  rw_lock_t *m_old_wait_rw_lock{};

  /** lock type requested on the object */
  ulint m_request_type{};

  /** in debug version file where requested */
  const char *m_file{};

  /** in debug version line where requested */
  ulint m_line{};

  /** thread id of this waiting thread */
  os_thread_id_t m_thread{};

  /** true if the thread has already called sync_array_event_wait on this cell */
  bool m_waiting{};

  /** We capture the signal_count of the m_wait_object when we reset
  the event. This value is then passed on to os_event_wait and we
  wait only if the event has not been signalled in the period between
  the reset and wait call. */
  int64_t m_signal_count{};

  /** time when the thread reserved the wait cell */
  time_t m_reservation_time{};
};

/* NOTE: It is allowed for a thread to wait
for an event allocated for the array without owning the
protecting mutex (depending on the case: OS or database mutex), but
all changes (set or reset) to the state of the event must be made
while owning the mutex. */

using Cells = std::vector<Sync_cell>;

/** Synchronization array */
struct Sync_check {

  /** Constructor
  @param[in] n                    Number of cells.
  @param[in] p                    Type of latch. */
  explicit Sync_check(ulint n, ulint p);

  ~Sync_check();

  /** Looks for a cell with the given thread id.
  @param[in] thread               Thread ID.
  @return pointer to cell or nullptr if not found */
  Sync_cell *find_thead(os_thread_id_t thread);

  /** Recursion step for deadlock detection.
  @param[in] start                Cell where recursive search started
  @param[in] thread               Thread to look at
  @param[in] pass                 Pass value
  @param[in] depth                Recursion depth
  @return true if deadlock detected */
  bool deadlock_step(Sync_cell *start, os_thread_id_t thread, ulint pass, ulint depth);

  /** Prints info of the wait array.
  NOTE! caller must own the mutex
  @param[in,out] ib_stream        Where to print. */
  void output_info(ib_stream_t ib_stream);

  /** Reserves the mutex semaphore protecting a sync array. */
  void acquire();

  /** Releases the mutex semaphore protecting a sync array. */
  void release();

  /** Validate the instance. */
  void validate();

#ifdef UNIV_SYNC_DEBUG
  /** This function is called only in the debug version. Detects a deadlock
  of one or more threads because of waits of semaphores.
  NOTE! caller must own the mutex
  @param[in] start              Cell where recursuze search start
  @param[in] cell               Cell to search
  @param[in] depth              Recursion depth
  @return true if deadlock detected */
  bool detect_deadlock(Sync_cell *start, Sync_cell *cell, ulint depth);
#endif /* UNIV_SYNC_DEBUG */

  /** Number of currently reserved cells in the wait array */
  ulint m_n_reserved{};

  /** Wait array */
  Cells m_cells{};

  /** This flag tells which mutex protects the data */
  ulint m_protection{};

  /** Possible database mutex protecting this data structure */
  mutex_t m_mutex{};

  /** Possible operating system mutex protecting the data structure.
  As this data structure is used in constructing the database mutex, to
  prevent infinite recursion in implementation, we fall back to an OS mutex. */
  os_mutex_t m_os_mutex{};

  /** Count of how many times an object has been signalled */
  std::atomic<ulint> m_sg_count{};

  /** Count of cell reservations since creation of the array */
  ulint m_res_count{};
};

Sync_check::Sync_check(ulint n, ulint p) : m_protection(p) {
  m_cells.resize(n);

  /* Then create the mutex to protect the wait array complex */
  if (m_protection == SYNC_ARRAY_OS_MUTEX) {
    m_os_mutex = os_mutex_create(nullptr);
  } else if (m_protection == SYNC_ARRAY_MUTEX) {
    mutex_create(&m_mutex, SYNC_NO_ORDER_CHECK);
  } else {
    ut_error;
  }
}

Sync_check::~Sync_check() {
  /* Free the mutex protecting the wait array. */
  if (m_protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_free(m_os_mutex);
  } else if (m_protection == SYNC_ARRAY_MUTEX) {
    mutex_free(&m_mutex);
  } else {
    ut_error;
  }
}

void Sync_check::acquire() {
  if (m_protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_enter(m_os_mutex);
  } else if (m_protection == SYNC_ARRAY_MUTEX) {
    mutex_enter(&m_mutex);
  } else {
    ut_error;
  }
}

void Sync_check::release() {
  if (m_protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_exit(m_os_mutex);
  } else if (m_protection == SYNC_ARRAY_MUTEX) {
    mutex_exit(&m_mutex);
  } else {
    ut_error;
  }
}

void Sync_check::validate() {
  ulint count{};

  acquire();

  for (auto &cell : m_cells) {
    if (cell.m_wait_object != nullptr) {
      count++;
    }
  }

  ut_a(count == m_n_reserved);

  release();
}

Sync_check *sync_array_create(ulint n_cells, ulint protection) {
  ut_a(n_cells > 0);

  /* Allocate memory for the data structures */
  return new Sync_check(n_cells, protection);
}

void sync_array_free(Sync_check *arr) {
  ut_a(arr->m_n_reserved == 0);

  arr->validate();

  delete arr;
}

/** Returns the event that the thread owning the cell waits for. */
OS_cond* Sync_cell::get_event() {
  if (m_request_type == SYNC_MUTEX) {
    return reinterpret_cast<mutex_t *>(m_wait_object)->event;
  } else if (m_request_type == RW_LOCK_WAIT_EX) {
    return reinterpret_cast<rw_lock_t *>(m_wait_object)->m_wait_ex_event;
  } else { /* RW_LOCK_SHARED and RW_LOCK_EX wait on the same event */
    return reinterpret_cast<rw_lock_t *>(m_wait_object)->m_event;
  }
}

void sync_array_reserve_cell(Sync_check *arr, void *object, ulint type, const char *file, ulint line, ulint *index) {
  ut_a(index != nullptr);
  ut_a(object != nullptr);

  arr->acquire();

  ++arr->m_res_count;

  ulint i{};

  /* Reserve a new cell. */
  for (auto &cell : arr->m_cells) {

    ++i;

    if (cell.m_wait_object != nullptr) {
      continue;
    }

    cell.m_waiting = false;
    cell.m_wait_object = object;

    if (type == SYNC_MUTEX) {
      cell.m_old_wait_mutex = static_cast<mutex_t *>(object);
    } else {
      cell.m_old_wait_rw_lock = static_cast<rw_lock_t *>(object);
    }

    cell.m_request_type = type;

    cell.m_file = file;
    cell.m_line = line;

    ++arr->m_n_reserved;

    *index = i - 1;

    arr->release();

    /* Make sure the event is reset and also store the value of
    m_signal_count at which the event was reset. */
    cell.m_signal_count = os_event_reset(cell.get_event());

    cell.m_reservation_time = time(nullptr);

    cell.m_thread = os_thread_get_curr_id();

    return;
  }

  /* No free cell found */
  ut_error;

  return;
}

void sync_array_wait_event(Sync_check *arr, ulint index) {
  arr->acquire();

  auto &cell = arr->m_cells[index];

  ut_a(!cell.m_waiting);
  ut_a(cell.m_wait_object != nullptr);
  ut_ad(os_thread_get_curr_id() == cell.m_thread);

  auto event = cell.get_event();

  cell.m_waiting = true;

#ifdef UNIV_SYNC_DEBUG

  /* We use simple acquire to the mutex below, because if
  we cannot acquire it at once, mutex_enter would call
  recursively sync_array routines, leading to trouble.
  rw_lock_debug_mutex freezes the debug lists. */

  rw_lock_debug_mutex_enter();

  if (sync_array_detect_deadlock(arr, &cell, &cell, 0)) {

    ib_logger(ib_stream, "########################################\n");
    ut_error;
  }

  rw_lock_debug_mutex_exit();
#endif /* UNIV_SYNC_DEBUG */

  arr->release();

  os_event_wait_low(event, cell.m_signal_count);

  sync_array_free_cell(arr, index);
}

void Sync_cell::print(ib_stream_t ib_stream) {
  const auto type = m_request_type;

  ib_logger(
    ib_stream,
    "--Thread %lu has waited at %s line %lu for %.2f seconds the semaphore:\n",
    (ulong)os_thread_pf(m_thread), m_file, (ulong)m_line, difftime(time(nullptr), m_reservation_time));

  if (type == SYNC_MUTEX) {
    /* We use m_old_wait_mutex in case the cell has already been freed meanwhile */
    auto mutex = m_old_wait_mutex;

    ib_logger(
      ib_stream,
      "Mutex at %p created file %s line %lu, lock var %lu\n"
#ifdef UNIV_SYNC_DEBUG
      "Last time reserved in file %s line %lu, "
#endif /* UNIV_SYNC_DEBUG */
      "waiters flag %lu\n",
      (void *)mutex, mutex->cfile_name, (ulong)mutex->cline, (ulong)mutex->lock_word,
#ifdef UNIV_SYNC_DEBUG
      mutex->file_name, (ulong)mutex->line,
#endif /* UNIV_SYNC_DEBUG */
      (ulong)mutex->m_waiters.load()
    );

  } else if (type == RW_LOCK_EX || type == RW_LOCK_WAIT_EX || type == RW_LOCK_SHARED) {

    ib_logger(ib_stream, "%s", type == RW_LOCK_EX ? "X-lock on" : "S-lock on");

    auto rwlock = m_old_wait_rw_lock;

    ib_logger(
      ib_stream, " RW-latch at %p created in file %s line %lu\n", (void *)rwlock, rwlock->m_cfile_name, (ulong)rwlock->m_cline
    );

    auto writer = rw_lock_get_writer(rwlock);

    if (writer != RW_LOCK_NOT_LOCKED) {
      std::stringstream ss;

      ss << rwlock->m_writer_thread.load();

      auto thread_id = std::stoull(ss.str());

      ib_logger(
        ib_stream,
        "a writer (thread id %lu) has reserved it in mode %s",
        (ulong)thread_id, writer == RW_LOCK_EX ? " exclusive" : " wait exclusive"
      );
    }

    ib_logger(
      ib_stream,
      "number of readers %lu, waiters flag %lu, "
      "lock_word: %lx\n"
      "Last time read locked in file %s line %lu\n"
      "Last time write locked in file %s line %lu\n",
      (ulong)rw_lock_get_reader_count(rwlock),
      (ulong)rwlock->m_waiters,
      (ulong)rwlock->m_lock_word.load(),
      rwlock->m_last_s_file_name,
      (ulong)rwlock->m_last_s_line,
      rwlock->m_last_x_file_name,
      (ulong)rwlock->m_last_x_line
    );
  } else {
    ut_error;
  }

  if (!m_waiting) {
    ib_logger(ib_stream, "wait has ended\n");
  }
}

#ifdef UNIV_SYNC_DEBUG
/** Looks for a cell with the given thread id.
@param[in] thread               Thread ID.
@return	pointer to cell or nullptr if not found */
Sync_cell *find_thead(os_thread_id_t thread) {

  for (auto& cell : m_cells) {
    if (cell.m_wait_object != nullptr && os_thread_eq(cell.m_thread, thread)) {

      return cell;
    }
  }

  return nullptr;
}

bool Sync_check::deadlock_step(Sync_cell *start, os_thread_id_t thread, ulint pass, ulint depth) {
  ++depth;

  if (pass != 0) {
    /* If pass != 0, then we do not know which threads are
    responsible of releasing the lock, and no deadlock can
    be detected. */

    return false;
  }

  auto cell arr->find_thread(thread);

  if (cell == start) {
    /* Stop running of other threads */

    ut_dbg_stop_threads = true;

    /* Deadlock */
    ib_logger(
      ib_stream,
      "########################################\n"
      "DEADLOCK of threads detected!\n"
    );

    return true;

  } else if (cell != nullptr) {
    return sync_array_detect_deadlock(arr, start, cell, depth);
  }
}

bool Sync_check::detect_deadlock(Sync_cell *start, Sync_cell *cell, ulint depth) {
  bool ret;
  rw_lock_t *lock;
  rw_lock_debug_t *debug;

  ut_ad(depth < 100);
  ut_ad(cell->m_wait_object != nullptr);
  ut_ad(os_thread_get_curr_id() == start->m_thread);

  ++depth;

  if (!cell->m_waiting) {

    /* No deadlock here */
    return false;
  }

  if (cell->m_request_type == SYNC_MUTEX) {

    mutex_t *mutex = cell->m_wait_object;

    if (mutex_get_lock_word(mutex) != 0) {

      auto thread = mutex->thread_id;

      /* Note that mutex->thread_id above may be also OS_THREAD_ID_UNDEFINED, because the
      thread which held the mutex maybe has not yet updated the value, or it has already
      released the mutex: in this case no deadlock can occur, as the wait array cannot contain
      a thread with ID_UNDEFINED value. */

      if (deadlock_step(start, thread, 0, depth)) {
        ib_logger(
          ib_stream, "Mutex %p owned by thread %lu file %s " "line %lu\n",
          mutex, (ulong)os_thread_pf(mutex->thread_id), mutex->file_name, (ulong)mutex->line
        );

        cell->print(ib_stream);

        return true;
      }
    }

    /* No deadlock */
    return false;

  } else if (cell->m_request_type == RW_LOCK_EX || cell->m_request_type == RW_LOCK_WAIT_EX) {

    rw_lock_t *lock = cell->m_wait_object;

    for (auto debug : lock->debug_list) {

      auto thread = debug->thread_id;

      if (((debug->lock_type == RW_LOCK_EX) && !os_thread_eq(thread, cell->m_thread)) ||
          ((debug->lock_type == RW_LOCK_WAIT_EX) && !os_thread_eq(thread, cell->m_thread)) ||
	  debug->lock_type == RW_LOCK_SHARED) {

        /* The (wait) x-lock request can block infinitely only if someone (can be also cell
        thread) is holding s-lock, or someone (cannot be cell thread) (wait) x-lock, and
        he is blocked by start thread */

        if (deadlock_step(start, thread, debug->pass, depth)) {
          ib_logger(ib_stream, "rw-lock %p ", (void *)lock);
          cell->_print(ib_stream);
          rw_lock_debug_print(debug);
          return true;
        }
      }
    }

    return false;

  } else if (cell->m_request_type == RW_LOCK_SHARED) {

    rw_lock_t *lock = cell->m_wait_object;

    for (auto debug : lock->debug_list) {

      auto thread = debug->thread_id;

      if (debug->lock_type == RW_LOCK_EX || debug->lock_type == RW_LOCK_WAIT_EX) {

        /* The s-lock request can block infinitely only if someone (can also
	be cell thread) is holding (wait) x-lock, and he is blocked by
	start thread */

        if (deadlock_step(start, m_thread, debug->pass, depth)) {
          ib_logger(ib_stream, "rw-lock %p ", (void *)lock);
          cell->print(ib_stream);
          rw_lock_debug_print(debug);
          return true;
        }
      }
    }

    return false;

  } else {
    ut_error;
  }

  /* Execution never reaches this line. */
  return true;
}
#endif /* UNIV_SYNC_DEBUG */

bool Sync_cell::can_wake_up() {
  if (m_request_type == SYNC_MUTEX) {

    auto mutex = static_cast<mutex_t *>(m_wait_object);

    if (mutex_get_lock_word(mutex) == 0) {

      return true;
    }

  } else if (m_request_type == RW_LOCK_EX) {

    auto lock = static_cast<rw_lock_t *>(m_wait_object);

    if (lock->m_lock_word > 0) {

      /* Either unlocked or only read locked. */

      return true;
    }

  } else if (m_request_type == RW_LOCK_WAIT_EX) {

    auto lock = static_cast<rw_lock_t *>(m_wait_object);

    /* lock_word == 0 means all readers have left */
    if (lock->m_lock_word == 0) {

      return true;
    }

  } else if (m_request_type == RW_LOCK_SHARED) {

    auto lock = static_cast<rw_lock_t *>(m_wait_object);

    /* lock_word > 0 means no writer or reserved writer */
    if (lock->m_lock_word > 0) {

      return true;
    }
  }

  return false;
}

void sync_array_free_cell(Sync_check *arr, ulint index) {
  arr->acquire();

  auto &cell = arr->m_cells[index];

  ut_a(cell.m_wait_object != nullptr);

  cell.m_waiting = false;
  cell.m_wait_object = nullptr;
  cell.m_signal_count = 0;

  ut_a(arr->m_n_reserved > 0);
  --arr->m_n_reserved;

  arr->release();
}

void sync_array_object_signalled(Sync_check *arr) {
  arr->m_sg_count.fetch_add(1);
}

void sync_arr_wake_threads_if_sema_free() {
  auto arr = sync_primary_wait_array;

  arr->acquire();

  ulint i{};
  ulint count{};

  while (count < arr->m_n_reserved) {
    auto &cell = arr->m_cells[i];

    ++i;

    if (cell.m_wait_object == nullptr) {

      continue;
    }

    ++count;

    if (cell.can_wake_up()) {

      os_event_set(cell.get_event());
    }
  }

  arr->release();
}

bool sync_array_print_long_waits() {
  auto arr = sync_primary_wait_array;

  bool fatal{};
  bool noticed{};
  const auto fatal_timeout = srv_fatal_semaphore_wait_threshold;

  for (auto &cell : arr->m_cells) {

    if (cell.m_wait_object != nullptr && cell.m_waiting && difftime(time(nullptr), cell.m_reservation_time) > 240) {
      ib_logger(ib_stream, "Warning: a long semaphore wait:\n");
      cell.print(ib_stream);
      noticed = true;
    }

    if (cell.m_wait_object != nullptr && cell.m_waiting && difftime(time(nullptr), cell.m_reservation_time) > fatal_timeout) {
      fatal = true;
    }
  }

  if (noticed) {

    ib_logger(ib_stream, "###### Starts InnoDB Monitor for 30 secs to print diagnostic info:");

    auto old_val = srv_print_innodb_monitor;

    /* If some crucial semaphore is reserved, then also the InnoDB
    Monitor can hang, and we do not get diagnostics. Since in
    many cases an InnoDB hang is caused by a pwrite() or a pread()
    call hanging inside the operating system, let us print right
    now the values of pending calls of these. */

    ib_logger(ib_stream, "Pending preads %lu, pwrites %lu\n", (ulong)os_file_n_pending_preads, (ulong)os_file_n_pending_pwrites);

    srv_print_innodb_monitor = true;

    os_event_set(srv_lock_timeout_thread_event);

    os_thread_sleep(30000000);

    srv_print_innodb_monitor = old_val;
    ib_logger(ib_stream, "###### Diagnostic info printed to the " "standard error stream");
  }

  return fatal;
}

void Sync_check::output_info(ib_stream_t ib_stream) {
  ib_logger(ib_stream, "OS WAIT ARRAY INFO: reservation count %ld, signal count %ld\n", (long)m_res_count, (long)m_sg_count);

  ulint i{};
  ulint count{};

  while (count < m_n_reserved) {

    auto &cell = m_cells[i];

    if (cell.m_wait_object != nullptr) {
      count++;
      cell.print(ib_stream);
    }

    i++;
  }
}

void sync_array_print_info(ib_stream_t ib_stream, Sync_check *arr) {
  arr->acquire();

  arr->output_info(ib_stream);

  arr->release();
}
