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

#include "sync0arr.h"

#ifdef UNIV_NONINL
#include "sync0arr.ic"
#endif

#include "os0file.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"

/*
                        WAIT ARRAY
                        ==========

The wait array consists of cells each of which has an
an operating system event object created for it. The threads
waiting for a mutex, for example, can reserve a cell
in the array and suspend themselves to wait for the event
to become signaled. When using the wait array, remember to make
sure that some thread holding the synchronization object
will eventually know that there is a waiter in the array and
signal the object, to prevent infinite wait.
Why we chose to implement a wait array? First, to make
mutexes fast, we had to code our own implementation of them,
which only in usually uncommon cases resorts to using
slow operating system primitives. Then we had the choice of
assigning a unique OS event for each mutex, which would
be simpler, or using a global wait array. In some operating systems,
the global wait array solution is more efficient and flexible,
because we can do with a very small number of OS events,
say 200. In NT 3.51, allocating events seems to be a quadratic
algorithm, because 10 000 events are created fast, but
100 000 events takes a couple of minutes to create.

As of 5.0.30 the above mentioned design is changed. Since now
OS can handle millions of wait events efficiently, we no longer
have this concept of each cell of wait array having one event.
Instead, now the event that a thread wants to wait on is embedded
in the wait object (mutex or rw_lock). We still keep the global
wait array for the sake of diagnostics and also to avoid infinite
wait The error_monitor thread scans the global wait array to signal
any waiting threads who have missed the signal. */

/** A cell where an individual thread may wait suspended
until a resource is released. The suspending is implemented
using an operating system event semaphore. */
struct sync_cell_struct {
  void *wait_object;       /*!< pointer to the object the
                           thread is waiting for; if nullptr
                           the cell is free for use */
  mutex_t *old_wait_mutex; /*!< the latest wait mutex in cell */
  rw_lock_t *old_wait_rw_lock;
  /*!< the latest wait rw-lock
  in cell */
  ulint request_type;      /*!< lock type requested on the
                           object */
  const char *file;        /*!< in debug version file where
                           requested */
  ulint line;              /*!< in debug version line where
                           requested */
  os_thread_id_t thread;   /*!< thread id of this waiting
                           thread */
  bool waiting;            /*!< true if the thread has already
                            called sync_array_event_wait
                            on this cell */
  int64_t signal_count;    /*!< We capture the signal_count
                              of the wait_object when we
                              reset the event. This value is
                              then passed on to os_event_wait
                              and we wait only if the event
                              has not been signalled in the
                              period between the reset and
                              wait call. */
  time_t reservation_time; /*!< time when the thread reserved
                          the wait cell */
};

/* NOTE: It is allowed for a thread to wait
for an event allocated for the array without owning the
protecting mutex (depending on the case: OS or database mutex), but
all changes (set or reset) to the state of the event must be made
while owning the mutex. */

/** Synchronization array */
struct sync_array_struct {
  ulint n_reserved;    /*!< number of currently reserved
                       cells in the wait array */
  ulint n_cells;       /*!< number of cells in the
                       wait array */
  sync_cell_t *array;  /*!< pointer to wait array */
  ulint protection;    /*!< this flag tells which
                       mutex protects the data */
  mutex_t mutex;       /*!< possible database mutex
                       protecting this data structure */
  os_mutex_t os_mutex; /*!< Possible operating system mutex
                       protecting the data structure.
                       As this data structure is used in
                       constructing the database mutex,
                       to prevent infinite recursion
                       in implementation, we fall back to
                       an OS mutex. */
  ulint sg_count;      /*!< count of how many times an
                       object has been signalled */
  ulint res_count;     /*!< count of cell reservations
                       since creation of the array */
};

#ifdef UNIV_SYNC_DEBUG
/** This function is called only in the debug version. Detects a deadlock
of one or more threads because of waits of semaphores.
@return	true if deadlock detected */
static bool sync_array_detect_deadlock(
    sync_array_t *arr,  /*!< in: wait array; NOTE! the caller must
                        own the mutex to array */
    sync_cell_t *start, /*!< in: cell where recursive search started */
    sync_cell_t *cell,  /*!< in: cell to search */
    ulint depth);       /*!< in: recursion depth */
#endif                  /* UNIV_SYNC_DEBUG */

/** Gets the nth cell in array.
@return	cell */
static sync_cell_t *
sync_array_get_nth_cell(sync_array_t *arr, /*!< in: sync array */
                        ulint n)           /*!< in: index */
{
  ut_a(arr);
  ut_a(n < arr->n_cells);

  return arr->array + n;
}

/** Reserves the mutex semaphore protecting a sync array. */
static void sync_array_enter(sync_array_t *arr) /*!< in: sync wait array */
{
  ulint protection;

  protection = arr->protection;

  if (protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_enter(arr->os_mutex);
  } else if (protection == SYNC_ARRAY_MUTEX) {
    mutex_enter(&(arr->mutex));
  } else {
    ut_error;
  }
}

/** Releases the mutex semaphore protecting a sync array. */
static void sync_array_exit(sync_array_t *arr) /*!< in: sync wait array */
{
  ulint protection;

  protection = arr->protection;

  if (protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_exit(arr->os_mutex);
  } else if (protection == SYNC_ARRAY_MUTEX) {
    mutex_exit(&(arr->mutex));
  } else {
    ut_error;
  }
}

sync_array_t *sync_array_create(ulint n_cells, ulint protection) {
  ut_a(n_cells > 0);

  /* Allocate memory for the data structures */
  auto arr = static_cast<sync_array_t *>(ut_malloc(sizeof(sync_array_t)));

  memset(arr, 0x0, sizeof(*arr));

  auto sz = sizeof(sync_cell_t) * n_cells;

  arr->array = static_cast<sync_cell_t *>(ut_malloc(sz));

  memset(arr->array, 0x0, sz);

  arr->n_cells = n_cells;
  arr->protection = protection;

  /* Then create the mutex to protect the wait array complex */
  if (protection == SYNC_ARRAY_OS_MUTEX) {
    arr->os_mutex = os_mutex_create(nullptr);
  } else if (protection == SYNC_ARRAY_MUTEX) {
    mutex_create(&arr->mutex, SYNC_NO_ORDER_CHECK);
  } else {
    ut_error;
  }

  return arr;
}

void sync_array_free(sync_array_t *arr) {
  ulint protection;

  ut_a(arr->n_reserved == 0);

  sync_array_validate(arr);

  protection = arr->protection;

  /* Release the mutex protecting the wait array complex */

  if (protection == SYNC_ARRAY_OS_MUTEX) {
    os_mutex_free(arr->os_mutex);
  } else if (protection == SYNC_ARRAY_MUTEX) {
    mutex_free(&(arr->mutex));
  } else {
    ut_error;
  }

  ut_free(arr->array);
  ut_free(arr);
}

void sync_array_validate(sync_array_t *arr) {
  ulint count{};

  sync_array_enter(arr);

  for (ulint i = 0; i < arr->n_cells; i++) {
    auto cell = sync_array_get_nth_cell(arr, i);
    if (cell->wait_object != nullptr) {
      count++;
    }
  }

  ut_a(count == arr->n_reserved);

  sync_array_exit(arr);
}

/** Returns the event that the thread owning the cell waits for. */
static os_event_t
sync_cell_get_event(sync_cell_t *cell) /*!< in: non-empty sync array cell */
{
  ulint type = cell->request_type;

  if (type == SYNC_MUTEX) {
    return ((mutex_t *)cell->wait_object)->event;
  } else if (type == RW_LOCK_WAIT_EX) {
    return ((rw_lock_t *)cell->wait_object)->m_wait_ex_event;
  } else { /* RW_LOCK_SHARED and RW_LOCK_EX wait on the same event */
    return ((rw_lock_t *)cell->wait_object)->m_event;
  }
}

void sync_array_reserve_cell(sync_array_t *arr, void *object, ulint type,
                             const char *file, ulint line, ulint *index) {
  sync_cell_t *cell;
  os_event_t event;
  ulint i;

  ut_a(object);
  ut_a(index);

  sync_array_enter(arr);

  arr->res_count++;

  /* Reserve a new cell. */
  for (i = 0; i < arr->n_cells; i++) {
    cell = sync_array_get_nth_cell(arr, i);

    if (cell->wait_object == nullptr) {

      cell->waiting = false;
      cell->wait_object = object;

      if (type == SYNC_MUTEX) {
        cell->old_wait_mutex = static_cast<ib_mutex_t *>(object);
      } else {
        cell->old_wait_rw_lock = static_cast<rw_lock_t *>(object);
      }

      cell->request_type = type;

      cell->file = file;
      cell->line = line;

      arr->n_reserved++;

      *index = i;

      sync_array_exit(arr);

      /* Make sure the event is reset and also store
      the value of signal_count at which the event
      was reset. */
      event = sync_cell_get_event(cell);
      cell->signal_count = os_event_reset(event);

      cell->reservation_time = time(nullptr);

      cell->thread = os_thread_get_curr_id();

      return;
    }
  }

  ut_error; /* No free cell found */

  return;
}

void sync_array_wait_event(sync_array_t *arr, ulint index) {
  sync_cell_t *cell;
  os_event_t event;

  ut_a(arr);

  sync_array_enter(arr);

  cell = sync_array_get_nth_cell(arr, index);

  ut_a(cell->wait_object);
  ut_a(!cell->waiting);
  ut_ad(os_thread_get_curr_id() == cell->thread);

  event = sync_cell_get_event(cell);
  cell->waiting = true;

#ifdef UNIV_SYNC_DEBUG

  /* We use simple enter to the mutex below, because if
  we cannot acquire it at once, mutex_enter would call
  recursively sync_array routines, leading to trouble.
  rw_lock_debug_mutex freezes the debug lists. */

  rw_lock_debug_mutex_enter();

  if (true == sync_array_detect_deadlock(arr, cell, cell, 0)) {

    ib_logger(ib_stream, "########################################\n");
    ut_error;
  }

  rw_lock_debug_mutex_exit();
#endif
  sync_array_exit(arr);

  os_event_wait_low(event, cell->signal_count);

  sync_array_free_cell(arr, index);
}

/** Reports info of a wait array cell. */
static void
sync_array_cell_print(ib_stream_t ib_stream, /*!< in: stream where to print */
                      sync_cell_t *cell)     /*!< in: sync cell */
{
  mutex_t *mutex;
  rw_lock_t *rwlock;
  ulint type;
  ulint writer;

  type = cell->request_type;

  ib_logger(ib_stream,
            "--Thread %lu has waited at %s line %lu"
            " for %.2f seconds the semaphore:\n",
            (ulong)os_thread_pf(cell->thread), cell->file, (ulong)cell->line,
            difftime(time(nullptr), cell->reservation_time));

  if (type == SYNC_MUTEX) {
    /* We use old_wait_mutex in case the cell has already
    been freed meanwhile */
    mutex = cell->old_wait_mutex;

    ib_logger(ib_stream,
              "Mutex at %p created file %s line %lu, lock var %lu\n"
#ifdef UNIV_SYNC_DEBUG
              "Last time reserved in file %s line %lu, "
#endif /* UNIV_SYNC_DEBUG */
              "waiters flag %lu\n",
              (void *)mutex, mutex->cfile_name, (ulong)mutex->cline,
              (ulong)mutex->lock_word,
#ifdef UNIV_SYNC_DEBUG
              mutex->file_name, (ulong)mutex->line,
#endif /* UNIV_SYNC_DEBUG */
              (ulong)mutex->waiters);

  } else if (type == RW_LOCK_EX || type == RW_LOCK_WAIT_EX ||
             type == RW_LOCK_SHARED) {

    ib_logger(ib_stream, "%s", type == RW_LOCK_EX ? "X-lock on" : "S-lock on");

    rwlock = cell->old_wait_rw_lock;

    ib_logger(ib_stream, " RW-latch at %p created in file %s line %lu\n",
              (void *)rwlock, rwlock->m_cfile_name, (ulong)rwlock->m_cline);

    writer = rw_lock_get_writer(rwlock);

    if (writer != RW_LOCK_NOT_LOCKED) {
      std::stringstream ss;

      ss << rwlock->m_writer_thread.load();

      auto thread_id = std::stoull(ss.str());

      ib_logger(ib_stream,
                "a writer (thread id %lu) has reserved it in mode %s",
                (ulong)thread_id,
                writer == RW_LOCK_EX ? " exclusive\n" : " wait exclusive\n");
    }

    ib_logger(ib_stream,
              "number of readers %lu, waiters flag %lu, "
              "lock_word: %lx\n"
              "Last time read locked in file %s line %lu\n"
              "Last time write locked in file %s line %lu\n",
              (ulong)rw_lock_get_reader_count(rwlock), (ulong)rwlock->m_waiters,
              (ulong)rwlock->m_lock_word.load(), rwlock->m_last_s_file_name,
              (ulong)rwlock->m_last_s_line, rwlock->m_last_x_file_name,
              (ulong)rwlock->m_last_x_line);
  } else {
    ut_error;
  }

  if (!cell->waiting) {
    ib_logger(ib_stream, "wait has ended\n");
  }
}

#ifdef UNIV_SYNC_DEBUG
/** Looks for a cell with the given thread id.
@return	pointer to cell or nullptr if not found */
static sync_cell_t *
sync_array_find_thread(sync_array_t *arr,     /*!< in: wait array */
                       os_thread_id_t thread) /*!< in: thread id */
{
  ulint i;
  sync_cell_t *cell;

  for (i = 0; i < arr->n_cells; i++) {

    cell = sync_array_get_nth_cell(arr, i);

    if (cell->wait_object != nullptr && os_thread_eq(cell->thread, thread)) {

      return cell; /* Found */
    }
  }

  return nullptr; /* Not found */
}

/** Recursion step for deadlock detection.
@return	true if deadlock detected */
static bool sync_array_deadlock_step(
    sync_array_t *arr,     /*!< in: wait array; NOTE! the caller must
                           own the mutex to array */
    sync_cell_t *start,    /*!< in: cell where recursive search
                           started */
    os_thread_id_t thread, /*!< in: thread to look at */
    ulint pass,            /*!< in: pass value */
    ulint depth)           /*!< in: recursion depth */
{
  sync_cell_t *new;
  bool ret;

  depth++;

  if (pass != 0) {
    /* If pass != 0, then we do not know which threads are
    responsible of releasing the lock, and no deadlock can
    be detected. */

    return false;
  }

  new = sync_array_find_thread(arr, thread);

  if (new == start) {
    /* Stop running of other threads */

    ut_dbg_stop_threads = true;

    /* Deadlock */
    ib_logger(ib_stream, "########################################\n"
                         "DEADLOCK of threads detected!\n");

    return true;

  } else if (new) {
    ret = sync_array_detect_deadlock(arr, start, new, depth);

    if (ret) {
      return true;
    }
  }
  return false;
}

/** This function is called only in the debug version. Detects a deadlock
of one or more threads because of waits of semaphores.
@return	true if deadlock detected */
static bool sync_array_detect_deadlock(
    sync_array_t *arr,  /*!< in: wait array; NOTE! the caller must
                        own the mutex to array */
    sync_cell_t *start, /*!< in: cell where recursive search started */
    sync_cell_t *cell,  /*!< in: cell to search */
    ulint depth)        /*!< in: recursion depth */
{
  mutex_t *mutex;
  rw_lock_t *lock;
  os_thread_id_t thread;
  bool ret;
  rw_lock_debug_t *debug;

  ut_a(arr);
  ut_a(start);
  ut_a(cell);
  ut_ad(cell->wait_object);
  ut_ad(os_thread_get_curr_id() == start->thread);
  ut_ad(depth < 100);

  depth++;

  if (!cell->waiting) {

    return false; /* No deadlock here */
  }

  if (cell->request_type == SYNC_MUTEX) {

    mutex = cell->wait_object;

    if (mutex_get_lock_word(mutex) != 0) {

      thread = mutex->thread_id;

      /* Note that mutex->thread_id above may be
      also OS_THREAD_ID_UNDEFINED, because the
      thread which held the mutex maybe has not
      yet updated the value, or it has already
      released the mutex: in this case no deadlock
      can occur, as the wait array cannot contain
      a thread with ID_UNDEFINED value. */

      ret = sync_array_deadlock_step(arr, start, thread, 0, depth);
      if (ret) {
        ib_logger(ib_stream,
                  "Mutex %p owned by thread %lu file %s "
                  "line %lu\n",
                  mutex, (ulong)os_thread_pf(mutex->thread_id),
                  mutex->file_name, (ulong)mutex->line);
        sync_array_cell_print(ib_stream, cell);

        return true;
      }
    }

    return false; /* No deadlock */

  } else if (cell->request_type == RW_LOCK_EX ||
             cell->request_type == RW_LOCK_WAIT_EX) {

    lock = cell->wait_object;

    debug = UT_LIST_GET_FIRST(lock->debug_list);

    while (debug != nullptr) {

      thread = debug->thread_id;

      if (((debug->lock_type == RW_LOCK_EX) &&
           !os_thread_eq(thread, cell->thread)) ||
          ((debug->lock_type == RW_LOCK_WAIT_EX) &&
           !os_thread_eq(thread, cell->thread)) ||
          (debug->lock_type == RW_LOCK_SHARED)) {

        /* The (wait) x-lock request can block
        infinitely only if someone (can be also cell
        thread) is holding s-lock, or someone
        (cannot be cell thread) (wait) x-lock, and
        he is blocked by start thread */

        ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
        if (ret) {
        print:
          ib_logger(ib_stream, "rw-lock %p ", (void *)lock);
          sync_array_cell_print(ib_stream, cell);
          rw_lock_debug_print(debug);
          return true;
        }
      }

      debug = UT_LIST_GET_NEXT(list, debug);
    }

    return false;

  } else if (cell->request_type == RW_LOCK_SHARED) {

    lock = cell->wait_object;
    debug = UT_LIST_GET_FIRST(lock->debug_list);

    while (debug != nullptr) {

      thread = debug->thread_id;

      if ((debug->lock_type == RW_LOCK_EX) ||
          (debug->lock_type == RW_LOCK_WAIT_EX)) {

        /* The s-lock request can block infinitely
        only if someone (can also be cell thread) is
        holding (wait) x-lock, and he is blocked by
        start thread */

        ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
        if (ret) {
          goto print;
        }
      }

      debug = UT_LIST_GET_NEXT(list, debug);
    }

    return false;

  } else {
    ut_error;
  }

  return true; /* Execution never reaches this line: for compiler
                 fooling only */
}
#endif /* UNIV_SYNC_DEBUG */

/** Determines if we can wake up the thread waiting for a sempahore. */
static bool
sync_arr_cell_can_wake_up(sync_cell_t *cell) /*!< in: cell to search */
{
  mutex_t *mutex;
  rw_lock_t *lock;

  if (cell->request_type == SYNC_MUTEX) {

    mutex = static_cast<ib_mutex_t *>(cell->wait_object);

    if (mutex_get_lock_word(mutex) == 0) {

      return true;
    }

  } else if (cell->request_type == RW_LOCK_EX) {

    lock = static_cast<rw_lock_t *>(cell->wait_object);

    if (lock->m_lock_word > 0) {
      /* Either unlocked or only read locked. */

      return true;
    }

  } else if (cell->request_type == RW_LOCK_WAIT_EX) {

    lock = static_cast<rw_lock_t *>(cell->wait_object);

    /* lock_word == 0 means all readers have left */
    if (lock->m_lock_word == 0) {

      return true;
    }
  } else if (cell->request_type == RW_LOCK_SHARED) {
    lock = static_cast<rw_lock_t *>(cell->wait_object);

    /* lock_word > 0 means no writer or reserved writer */
    if (lock->m_lock_word > 0) {

      return true;
    }
  }

  return false;
}

void sync_array_free_cell(sync_array_t *arr, ulint index) {
  sync_cell_t *cell;

  sync_array_enter(arr);

  cell = sync_array_get_nth_cell(arr, index);

  ut_a(cell->wait_object != nullptr);

  cell->waiting = false;
  cell->wait_object = nullptr;
  cell->signal_count = 0;

  ut_a(arr->n_reserved > 0);
  arr->n_reserved--;

  sync_array_exit(arr);
}

void sync_array_object_signalled(sync_array_t *arr) {
#ifdef HAVE_ATOMIC_BUILTINS
  (void)os_atomic_increment_ulint(&arr->sg_count, 1);
#else
  sync_array_enter(arr);

  arr->sg_count++;

  sync_array_exit(arr);
#endif
}

void sync_arr_wake_threads_if_sema_free(void) {
  sync_array_t *arr = sync_primary_wait_array;
  sync_cell_t *cell;
  ulint count;
  ulint i;
  os_event_t event;

  sync_array_enter(arr);

  i = 0;
  count = 0;

  while (count < arr->n_reserved) {

    cell = sync_array_get_nth_cell(arr, i);
    i++;

    if (cell->wait_object == nullptr) {
      continue;
    }
    count++;

    if (sync_arr_cell_can_wake_up(cell)) {

      event = sync_cell_get_event(cell);

      os_event_set(event);
    }
  }

  sync_array_exit(arr);
}

/** Prints warnings of long semaphore waits to ib_stream.
@return	true if fatal semaphore wait threshold was exceeded */

bool sync_array_print_long_waits(void) {
  sync_cell_t *cell;
  bool old_val;
  bool noticed = false;
  ulint i;
  ulint fatal_timeout = srv_fatal_semaphore_wait_threshold;
  bool fatal = false;

  for (i = 0; i < sync_primary_wait_array->n_cells; i++) {

    cell = sync_array_get_nth_cell(sync_primary_wait_array, i);

    if (cell->wait_object != nullptr && cell->waiting &&
        difftime(time(nullptr), cell->reservation_time) > 240) {
      ib_logger(ib_stream, "Warning: a long semaphore wait:\n");
      sync_array_cell_print(ib_stream, cell);
      noticed = true;
    }

    if (cell->wait_object != nullptr && cell->waiting &&
        difftime(time(nullptr), cell->reservation_time) > fatal_timeout) {
      fatal = true;
    }
  }

  if (noticed) {
    ib_logger(ib_stream, "###### Starts InnoDB Monitor"
                         " for 30 secs to print diagnostic info:\n");
    old_val = srv_print_innodb_monitor;

    /* If some crucial semaphore is reserved, then also the InnoDB
    Monitor can hang, and we do not get diagnostics. Since in
    many cases an InnoDB hang is caused by a pwrite() or a pread()
    call hanging inside the operating system, let us print right
    now the values of pending calls of these. */

    ib_logger(ib_stream, "Pending preads %lu, pwrites %lu\n",
              (ulong)os_file_n_pending_preads,
              (ulong)os_file_n_pending_pwrites);

    srv_print_innodb_monitor = true;
    os_event_set(srv_lock_timeout_thread_event);

    os_thread_sleep(30000000);

    srv_print_innodb_monitor = old_val;
    ib_logger(ib_stream, "###### Diagnostic info printed to the "
                         "standard error stream\n");
  }

  return fatal;
}

/** Prints info of the wait array. */
static void
sync_array_output_info(ib_stream_t ib_stream, /*!< in: stream where to print */
                       sync_array_t *arr)     /*!< in: wait array; NOTE! caller
                                              must own the mutex */
{
  ulint count;
  ulint i;

  ib_logger(ib_stream,
            "OS WAIT ARRAY INFO: reservation count %ld, signal count %ld\n",
            (long)arr->res_count, (long)arr->sg_count);
  i = 0;
  count = 0;

  while (count < arr->n_reserved) {

    auto cell = sync_array_get_nth_cell(arr, i);

    if (cell->wait_object != nullptr) {
      count++;
      sync_array_cell_print(ib_stream, cell);
    }

    i++;
  }
}

void sync_array_print_info(ib_stream_t ib_stream, sync_array_t *arr) {
  sync_array_enter(arr);

  sync_array_output_info(ib_stream, arr);

  sync_array_exit(arr);
}
