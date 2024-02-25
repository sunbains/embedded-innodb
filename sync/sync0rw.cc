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

/** @file sync/sync0rw.c
The read-write lock (for thread synchronization)

Created 9/11/1995 Heikki Tuuri
*******************************************************/

#include "sync0rw.h"

#include "mem0mem.h"
#include "os0sync.h"
#include "os0thread.h"
#include "srv0srv.h"

/*
        IMPLEMENTATION OF THE RW_LOCK
        =============================
The status of a rw_lock is held in lock_word. The initial value of lock_word is
X_LOCK_DECR. lock_word is decremented by 1 for each s-lock and by X_LOCK_DECR
for each x-lock. This describes the lock state for each value of lock_word:

lock_word == X_LOCK_DECR:      Unlocked.
0 < lock_word < X_LOCK_DECR:   Read locked, no waiting writers.
                               (X_LOCK_DECR - lock_word) is the
                               number of readers that hold the lock.
lock_word == 0:		       Write locked
-X_LOCK_DECR < lock_word < 0:  Read locked, with a waiting writer.
                               (-lock_word) is the number of readers
                               that hold the lock.
lock_word <= -X_LOCK_DECR:     Recursively write locked. lock_word has been
                               decremented by X_LOCK_DECR once for each lock,
                               so the number of locks is:
                               ((-lock_word) / X_LOCK_DECR) + 1
When lock_word <= -X_LOCK_DECR, we also know that lock_word % X_LOCK_DECR == 0:
other values of lock_word are invalid.

The lock_word is always read and updated atomically and consistently, so that
it always represents the state of the lock, and the state of the lock changes
with a single atomic operation. This lock_word holds all of the information
that a thread needs in order to determine if it is eligible to gain the lock
or if it must spin or sleep. The one exception to this is that writer_thread
must be verified before recursive write locks: to solve this scenario, we make
writer_thread readable by all threads, but only writeable by the x-lock holder.

The other members of the lock obey the following rules to remain consistent:

recursive:	This and the writer_thread field together control the
                behaviour of recursive x-locking.
                lock->m_recursive must be FALSE in following states:
                        1) The writer_thread contains garbage i.e.: the
                        lock has just been initialized.
                        2) The lock is not x-held and there is no
                        x-waiter waiting on WAIT_EX event.
                        3) The lock is x-held or there is an x-waiter
                        waiting on WAIT_EX event but the 'pass' value
                        is non-zero.
                lock->m_recursive is TRUE iff:
                        1) The lock is x-held or there is an x-waiter
                        waiting on WAIT_EX event and the 'pass' value
                        is zero.
                This flag must be set after the writer_thread field
                has been updated with a memory ordering barrier.
                It is unset before the lock_word has been incremented.
writer_thread:	Is used only in recursive x-locking. Can only be safely
                read iff lock->m_recursive flag is TRUE.
                This field is uninitialized at lock creation time and
                is updated atomically when x-lock is acquired or when
                move_ownership is called. A thread is only allowed to
                set the value of this field to it's thread_id i.e.: a
                thread cannot set writer_thread to some other thread's
                id.
waiters:	May be set to 1 anytime, but to avoid unnecessary wake-up
                signals, it should only be set to 1 when there are threads
                waiting on event. Must be 1 when a writer starts waiting to
                ensure the current x-locking thread sends a wake-up signal
                during unlock. May only be reset to 0 immediately before a
                a wake-up signal is sent to event. On most platforms, a
                memory barrier is required after waiters is set, and before
                verifying lock_word is still held, to ensure some unlocker
                really does see the flags new value.
event:		Threads wait on event for read or writer lock when another
                thread has an x-lock or an x-lock reservation (wait_ex). A
                thread may only	wait on event after performing the following
                actions in order:
                   (1) Record the counter value of event (with os_event_reset).
                   (2) Set waiters to 1.
                   (3) Verify lock_word <= 0.
                (1) must come before (2) to ensure signal is not missed.
                (2) must come before (3) to ensure a signal is sent.
                These restrictions force the above ordering.
                Immediately before sending the wake-up signal, we should:
                   (1) Verify lock_word == X_LOCK_DECR (unlocked)
                   (2) Reset waiters to 0.
wait_ex_event:	A thread may only wait on the wait_ex_event after it has
                performed the following actions in order:
                   (1) Decrement lock_word by X_LOCK_DECR.
                   (2) Record counter value of wait_ex_event (os_event_reset,
                       called from sync_array_reserve_cell).
                   (3) Verify that lock_word < 0.
                (1) must come first to ensures no other threads become reader
                or next writer, and notifies unlocker that signal must be sent.
                (2) must come before (3) to ensure the signal is not missed.
                These restrictions force the above ordering.
                Immediately before sending the wake-up signal, we should:
                   Verify lock_word == 0 (waiting thread holds x_lock)
*/

/** number of spin waits on rw-latches,
resulted during shared (read) locks */
int64_t rw_s_spin_wait_count = 0;
/** number of spin loop rounds on rw-latches,
resulted during shared (read) locks */
int64_t rw_s_spin_round_count = 0;

/** number of OS waits on rw-latches,
resulted during shared (read) locks */
int64_t rw_s_os_wait_count = 0;

/** number of unlocks (that unlock shared locks),
set only when UNIV_SYNC_PERF_STAT is defined */
int64_t rw_s_exit_count = 0;

/** number of spin waits on rw-latches,
resulted during exclusive (write) locks */
int64_t rw_x_spin_wait_count = 0;
/** number of spin loop rounds on rw-latches,
resulted during exclusive (write) locks */
int64_t rw_x_spin_round_count = 0;

/** number of OS waits on rw-latches,
resulted during exclusive (write) locks */
int64_t rw_x_os_wait_count = 0;

/** number of unlocks (that unlock exclusive locks),
set only when UNIV_SYNC_PERF_STAT is defined */
int64_t rw_x_exit_count = 0;

/* The global list of rw-locks */
rw_lock_list_t rw_lock_list;
mutex_t rw_lock_list_mutex;

#ifdef UNIV_SYNC_DEBUG
/** The global mutex which protects debug info lists of all rw-locks.
To modify the debug info list of an rw-lock, this mutex has to be
acquired in addition to the mutex protecting the lock. */
mutex_t rw_lock_debug_mutex;

/** If deadlock detection does not get immediately the mutex,
it may wait for this event */
os_event_t rw_lock_debug_event;

/** This is set to TRUE, if there may be waiters for the event */
ibool rw_lock_debug_waiters;

/** Creates a debug info struct. */
static rw_lock_debug_t *rw_lock_debug_create(void);

/** Frees a debug info struct. */
static void rw_lock_debug_free(rw_lock_debug_t *info);

/** Creates a debug info struct.
@return	own: debug info struct */
static rw_lock_debug_t *rw_lock_debug_create(void) {
  return (rw_lock_debug_t *)mem_alloc(sizeof(rw_lock_debug_t));
}

/** Frees a debug info struct. */
static void rw_lock_debug_free(rw_lock_debug_t *info) { mem_free(info); }
#endif /* UNIV_SYNC_DEBUG */

void rw_lock_var_init() {
  rw_s_spin_wait_count = 0;
  rw_s_os_wait_count = 0;
  rw_s_exit_count = 0;
  rw_x_spin_wait_count = 0;
  rw_x_os_wait_count = 0;
  rw_x_exit_count = 0;

  memset(&rw_lock_list, 0x0, sizeof(rw_lock_list));
  memset(&rw_lock_list_mutex, 0x0, sizeof(rw_lock_list_mutex));

#ifdef UNIV_SYNC_DEBUG
  memset(&rw_lock_debug_mutex, 0x0, sizeof(rw_lock_debug_mutex));
  memset(&rw_lock_debug_event, 0x0, sizeof(rw_lock_debug_event));
  memset(&rw_lock_debug_waiters, 0x0, sizeof(rw_lock_debug_waiters));
#endif /* UNIV_SYNC_DEBUG */
}

void rw_lock_create_func(
    rw_lock_t *lock, /*!< in: pointer to memory */
#ifdef UNIV_DEBUG
#ifdef UNIV_SYNC_DEBUG
    ulint level,
#endif /* UNIV_SYNC_DEBUG */
    const char *cmutex_name,
#endif /* UNIV_DEBUG */
    const char *cfile_name,
    ulint cline)
{
  /* If this is the very first time a synchronization object is
  created, then the following call initializes the sync system. */

#ifdef UNIV_DEBUG
  UT_NOT_USED(cmutex_name);
#endif /* UNIV_DEBUG */

  lock->m_waiters = 0;
  lock->m_lock_word = X_LOCK_DECR;

  /* We set this value to signify that lock->m_writer_thread
  contains garbage at initialization and cannot be used for
  recursive x-locking. */
  lock->m_recursive = false;

#ifdef UNIV_SYNC_DEBUG
  UT_LIST_INIT(lock->m_debug_list);

  lock->m_level = level;
#endif /* UNIV_SYNC_DEBUG */

  lock->m_magic_n = RW_LOCK_MAGIC_N;

  lock->m_cfile_name = cfile_name;
  lock->m_cline = (unsigned int)cline;

  lock->m_count_os_wait = 0;
  lock->m_last_s_file_name = "not yet reserved";
  lock->m_last_x_file_name = "not yet reserved";
  lock->m_last_s_line = 0;
  lock->m_last_x_line = 0;
  lock->m_event = os_event_create(nullptr);
  lock->m_wait_ex_event = os_event_create(nullptr);

  mutex_enter(&rw_lock_list_mutex);

  if (UT_LIST_GET_LEN(rw_lock_list) > 0) {
    ut_a(UT_LIST_GET_FIRST(rw_lock_list)->m_magic_n == RW_LOCK_MAGIC_N);
  }

  UT_LIST_ADD_FIRST(list, rw_lock_list, lock);

  mutex_exit(&rw_lock_list_mutex);
}

void rw_lock_free(rw_lock_t *lock) {
  ut_ad(rw_lock_validate(lock));
  ut_a(lock->m_lock_word == X_LOCK_DECR);

  lock->m_magic_n = 0;

  mutex_enter(&rw_lock_list_mutex);
  os_event_free(lock->m_event);

  os_event_free(lock->m_wait_ex_event);

  if (UT_LIST_GET_PREV(list, lock)) {
    ut_a(UT_LIST_GET_PREV(list, lock)->m_magic_n == RW_LOCK_MAGIC_N);
  }
  if (UT_LIST_GET_NEXT(list, lock)) {
    ut_a(UT_LIST_GET_NEXT(list, lock)->m_magic_n == RW_LOCK_MAGIC_N);
  }

  UT_LIST_REMOVE(list, rw_lock_list, lock);

  mutex_exit(&rw_lock_list_mutex);
}

#ifdef UNIV_DEBUG
ibool rw_lock_validate(rw_lock_t *lock)
{
  ut_a(lock != nullptr);

  ulint waiters = rw_lock_get_waiters(lock);
  lint lock_word = lock->m_lock_word;

  ut_a(lock->m_magic_n == RW_LOCK_MAGIC_N);
  ut_a(waiters == 0 || waiters == 1);
  ut_a(lock_word > -X_LOCK_DECR || (-lock_word) % X_LOCK_DECR == 0);

  return TRUE;
}
#endif /* UNIV_DEBUG */

void rw_lock_s_lock_spin(rw_lock_t *lock, ulint pass, const char *file_name, ulint line) {
  ulint i{};
  ulint index; /* index of the reserved wait cell */

  ut_ad(rw_lock_validate(lock));

  rw_s_spin_wait_count++; /*!< Count calls to this function */
lock_loop:

  /* Spin waiting for the writer field to become free */
  while (i < SYNC_SPIN_ROUNDS && lock->m_lock_word <= 0) {
    if (srv_spin_wait_delay) {
      ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
    }

    i++;
  }

  if (i == SYNC_SPIN_ROUNDS) {
    os_thread_yield();
  }

  if (srv_print_latch_waits) {
    ib_logger(ib_stream,
              "Thread %lu spin wait rw-s-lock at %p"
              " cfile %s cline %lu rnds %lu\n",
              (ulong)os_thread_pf(os_thread_get_curr_id()), (void *)lock,
              lock->m_cfile_name, (ulong)lock->m_cline, (ulong)i);
  }

  /* We try once again to obtain the lock */
  if (TRUE == rw_lock_s_lock_low(lock, pass, file_name, line)) {
    rw_s_spin_round_count += i;

    return; /* Success */
  } else {

    if (i < SYNC_SPIN_ROUNDS) {
      goto lock_loop;
    }

    rw_s_spin_round_count += i;

    sync_array_reserve_cell(sync_primary_wait_array, lock, RW_LOCK_SHARED,
                            file_name, line, &index);

    /* Set waiters before checking lock_word to ensure wake-up
    signal is sent. This may lead to some unnecessary signals. */
    rw_lock_set_waiter_flag(lock);

    if (TRUE == rw_lock_s_lock_low(lock, pass, file_name, line)) {
      sync_array_free_cell(sync_primary_wait_array, index);
      return; /* Success */
    }

    if (srv_print_latch_waits) {
      ib_logger(ib_stream,
                "Thread %lu OS wait rw-s-lock at %p"
                " cfile %s cline %lu\n",
                os_thread_pf(os_thread_get_curr_id()), (void *)lock,
                lock->m_cfile_name, (ulong)lock->m_cline);
    }

    /* these stats may not be accurate */
    lock->m_count_os_wait++;
    rw_s_os_wait_count++;

    sync_array_wait_event(sync_primary_wait_array, index);

    i = 0;
    goto lock_loop;
  }
}

void rw_lock_x_lock_move_ownership(rw_lock_t *lock) {
  ut_ad(rw_lock_is_locked(lock, RW_LOCK_EX));

  rw_lock_set_writer_id_and_recursion_flag(lock, TRUE);
}

/** Function for the next writer to call. Waits for readers to exit.
The caller must have already decremented lock_word by X_LOCK_DECR.
@param[in] lock                 Select the next writer waiting on this lock.
@param[in] pass                 Value; != 0, if the lock will be passed
                                to another thread to unlock
@param[in] file_name            File name where lock requested
@param[in] line                 Line where requested */
inline
void rw_lock_x_lock_wait(
    rw_lock_t *lock,
#ifdef UNIV_SYNC_DEBUG
    ulint pass,
#endif /* UNIV_SYNC_DEBUG */
    const char *file_name,
    ulint line)
{
  ulint index;
  ulint i = 0;

  ut_ad(lock->m_lock_word <= 0);

  while (lock->m_lock_word < 0) {
    if (srv_spin_wait_delay) {
      ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
    }

    if (i < SYNC_SPIN_ROUNDS) {
      i++;
      continue;
    }

    /* If there is still a reader, then go to sleep.*/
    rw_x_spin_round_count += i;

    i = 0;

    sync_array_reserve_cell(sync_primary_wait_array, lock, RW_LOCK_WAIT_EX, file_name, line, &index);

    /* Check lock_word to ensure wake-up isn't missed.*/
    if (lock->m_lock_word < 0) {

      /* These stats may not be accurate */
      ++rw_x_os_wait_count;
      ++lock->m_count_os_wait;

      /* Add debug info as it is needed to detect possible
      deadlock. We must add info for WAIT_EX thread for
      deadlock detection to work properly. */
#ifdef UNIV_SYNC_DEBUG
      rw_lock_add_debug_info(lock, pass, RW_LOCK_WAIT_EX, file_name, line);
#endif /* UNIV_SYNC_DEBUG */

      sync_array_wait_event(sync_primary_wait_array, index);
#ifdef UNIV_SYNC_DEBUG
      rw_lock_remove_debug_info(lock, pass, RW_LOCK_WAIT_EX);
#endif /* UNIV_SYNC_DEBUG */

      /* It is possible to wake when lock_word < 0.
      We must pass the while-loop check to proceed.*/
    } else {
      sync_array_free_cell(sync_primary_wait_array, index);
    }
  }

  rw_x_spin_round_count += i;
}

/** Low-level function for acquiring an exclusive lock.
@param[in,out] lock,       Lock instance on which to acquire an x-lock
@param[in] pass            Value; != 0, if the lock will be passed to
                           another thread to unlock
@param[in] file_name       File name where lock requested
@param[in] line            Line in filen_ame where requested
@return	RW_LOCK_NOT_LOCKED if did not succeed, RW_LOCK_EX if success. */
inline
bool rw_lock_x_lock_low(rw_lock_t *lock, ulint pass, const char *file_name, ulint line) {
  if (rw_lock_lock_word_decr(lock, X_LOCK_DECR)) {

    /* lock->m_recursive also tells us if the writer_thread
    field is stale or active. As we are going to write
    our own thread id in that field it must be that the
    current writer_thread value is not active. */
    ut_a(!lock->m_recursive);

    /* Decrement occurred: we are writer or next-writer. */
    rw_lock_set_writer_id_and_recursion_flag(lock, pass ? FALSE : TRUE);

    rw_lock_x_lock_wait(lock,
#ifdef UNIV_SYNC_DEBUG
                        pass,
#endif /* UNIV_SYNC_DEBUG */
                        file_name, line);

  } else {
    /* Decrement failed: relock or failed lock */
    if (!pass && lock->m_recursive && lock->m_writer_thread.load() == std::this_thread::get_id()) {
      /* Relock */
      lock->m_lock_word -= X_LOCK_DECR;
    } else {
      /* Another thread locked before us */
      return false;
    }
  }
#ifdef UNIV_SYNC_DEBUG
  rw_lock_add_debug_info(lock, pass, RW_LOCK_EX, file_name, line);
#endif /* UNIV_SYNC_DEBUG */

  lock->m_last_x_file_name = file_name;
  lock->m_last_x_line = (unsigned int)line;

  return true;
}

void rw_lock_x_lock_func(rw_lock_t *lock, ulint pass, const char *file_name, ulint line) {
  ulint i;
  ulint index;
  ibool spinning{FALSE};

  ut_ad(rw_lock_validate(lock));

  i = 0;

lock_loop:

  if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
    rw_x_spin_round_count += i;

    return; /* Locking succeeded */

  } else {

    if (!spinning) {
      spinning = TRUE;
      rw_x_spin_wait_count++;
    }

    /* Spin waiting for the lock_word to become free */
    while (i < SYNC_SPIN_ROUNDS && lock->m_lock_word <= 0) {
      if (srv_spin_wait_delay) {
        ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
      }

      i++;
    }
    if (i == SYNC_SPIN_ROUNDS) {
      os_thread_yield();
    } else {
      goto lock_loop;
    }
  }

  rw_x_spin_round_count += i;

  if (srv_print_latch_waits) {
    ib_logger(ib_stream,
              "Thread %lu spin wait rw-x-lock at %p"
              " cfile %s cline %lu rnds %lu\n",
              os_thread_pf(os_thread_get_curr_id()), (void *)lock,
              lock->m_cfile_name, (ulong)lock->m_cline, (ulong)i);
  }

  sync_array_reserve_cell(sync_primary_wait_array, lock, RW_LOCK_EX, file_name,
                          line, &index);

  /* Waiters must be set before checking lock_word, to ensure signal
  is sent. This could lead to a few unnecessary wake-up signals. */
  rw_lock_set_waiter_flag(lock);

  if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
    sync_array_free_cell(sync_primary_wait_array, index);
    return; /* Locking succeeded */
  }

  if (srv_print_latch_waits) {
    ib_logger(ib_stream,
              "Thread %lu OS wait for rw-x-lock at %p"
              " cfile %s cline %lu\n",
              os_thread_pf(os_thread_get_curr_id()), (void *)lock,
              lock->m_cfile_name, (ulong)lock->m_cline);
  }

  /* these stats may not be accurate */
  lock->m_count_os_wait++;
  rw_x_os_wait_count++;

  sync_array_wait_event(sync_primary_wait_array, index);

  i = 0;
  goto lock_loop;
}

#ifdef UNIV_SYNC_DEBUG
void rw_lock_debug_mutex_enter() {
loop:
  if (0 == mutex_enter_nowait(&rw_lock_debug_mutex)) {
    return;
  }

  os_event_reset(rw_lock_debug_event);

  rw_lock_debug_waiters = TRUE;

  if (0 == mutex_enter_nowait(&rw_lock_debug_mutex)) {
    return;
  }

  os_event_wait(rw_lock_debug_event);

  goto loop;
}

void rw_lock_debug_mutex_exit(void) {
  mutex_exit(&rw_lock_debug_mutex);

  if (rw_lock_debug_waiters) {
    rw_lock_debug_waiters = FALSE;
    os_event_set(rw_lock_debug_event);
  }
}

void rw_lock_add_debug_info(rw_lock_t *lock, ulint pass, ulint lock_type, const char *file_name, ulint line) {
  ut_ad(lock);
  ut_ad(file_name);

  auto info = rw_lock_debug_create();

  rw_lock_debug_mutex_enter();

  info->file_name = file_name;
  info->line = line;
  info->lock_type = lock_type;
  info->thread_id = os_thread_get_curr_id();
  info->pass = pass;

  UT_LIST_ADD_FIRST(list, lock->m_debug_list, info);

  rw_lock_debug_mutex_exit();

  if ((pass == 0) && (lock_type != RW_LOCK_WAIT_EX)) {
    sync_thread_add_level(lock, lock->m_level);
  }
}

void rw_lock_remove_debug_info(rw_lock_t *lock, ulint pass, ulint lock_type) {
  ut_ad(lock);

  if ((pass == 0) && (lock_type != RW_LOCK_WAIT_EX)) {
    sync_thread_reset_level(lock);
  }

  rw_lock_debug_mutex_enter();

  auto info = UT_LIST_GET_FIRST(lock->m_debug_list);

  while (info != nullptr) {
    if ((pass == info->pass) &&
        ((pass != 0) ||
         os_thread_eq(info->thread_id, os_thread_get_curr_id())) &&
        (info->lock_type == lock_type)) {

      /* Found! */
      UT_LIST_REMOVE(list, lock->m_debug_list, info);
      rw_lock_debug_mutex_exit();

      rw_lock_debug_free(info);

      return;
    }

    info = UT_LIST_GET_NEXT(list, info);
  }

  ut_error;
}

ibool rw_lock_own(rw_lock_t *lock, ulint lock_type) {
  ut_ad(lock);
  ut_ad(rw_lock_validate(lock));

  rw_lock_debug_mutex_enter();

  auto info = UT_LIST_GET_FIRST(lock->m_debug_list);

  while (info != nullptr) {

    if (os_thread_eq(info->thread_id, os_thread_get_curr_id()) &&
        (info->pass == 0) && (info->lock_type == lock_type)) {

      rw_lock_debug_mutex_exit();

      /* Found! */
      return TRUE;
    }

    info = UT_LIST_GET_NEXT(list, info);
  }
  rw_lock_debug_mutex_exit();

  return FALSE;
}

#endif /* UNIV_SYNC_DEBUG */

ibool rw_lock_is_locked(rw_lock_t *lock, ulint lock_type) {
  ibool ret = FALSE;

  ut_ad(lock);
  ut_ad(rw_lock_validate(lock));

  if (lock_type == RW_LOCK_SHARED) {
    if (rw_lock_get_reader_count(lock) > 0) {
      ret = TRUE;
    }
  } else if (lock_type == RW_LOCK_EX) {
    if (rw_lock_get_writer(lock) == RW_LOCK_EX) {
      ret = TRUE;
    }
  } else {
    ut_error;
  }

  return ret;
}

#ifdef UNIV_SYNC_DEBUG
void rw_lock_list_print_info(ib_stream_t ib_stream) {
  rw_lock_t *lock;
  ulint count = 0;
  rw_lock_debug_t *info;

  mutex_enter(&rw_lock_list_mutex);

  ib_logger(ib_stream, "-------------\n"
                       "RW-LATCH INFO\n"
                       "-------------\n");

  lock = UT_LIST_GET_FIRST(rw_lock_list);

  while (lock != nullptr) {

    count++;

    if (lock->m_lock_word != X_LOCK_DECR) {

      ib_logger(ib_stream, "RW-LOCK: %p ", (void *)lock);

      if (rw_lock_get_waiters(lock)) {
        ib_logger(ib_stream, " Waiters for the lock exist\n");
      } else {
        ib_logger(ib_stream, "\n");
      }

      info = UT_LIST_GET_FIRST(lock->m_debug_list);
      while (info != nullptr) {
        rw_lock_debug_print(info);
        info = UT_LIST_GET_NEXT(list, info);
      }
    }

    lock = UT_LIST_GET_NEXT(list, lock);
  }

  ib_logger(ib_stream, "Total number of rw-locks %ld\n", count);
  mutex_exit(&rw_lock_list_mutex);
}

void rw_lock_print(rw_lock_t *lock) {
  rw_lock_debug_t *info;

  ib_logger(ib_stream,
            "-------------\n"
            "RW-LATCH INFO\n"
            "RW-LATCH: %p ",
            (void *)lock);

  if (lock->m_lock_word != X_LOCK_DECR) {

    if (rw_lock_get_waiters(lock)) {
      ib_logger(ib_stream, " Waiters for the lock exist\n");
    } else {
      ib_logger(ib_stream, "\n");
    }

    info = UT_LIST_GET_FIRST(lock->m_debug_list);
    while (info != nullptr) {
      rw_lock_debug_print(info);
      info = UT_LIST_GET_NEXT(list, info);
    }
  }
}

void rw_lock_debug_print(rw_lock_debug_t *info) {
  auto rwt = info->lock_type;

  ib_logger(ib_stream, "Locked: thread %ld file %s line %ld  ",
            (ulong)os_thread_pf(info->thread_id), info->file_name,
            (ulong)info->line);

  if (rwt == RW_LOCK_SHARED) {
    ib_logger(ib_stream, "S-LOCK");
  } else if (rwt == RW_LOCK_EX) {
    ib_logger(ib_stream, "X-LOCK");
  } else if (rwt == RW_LOCK_WAIT_EX) {
    ib_logger(ib_stream, "WAIT X-LOCK");
  } else {
    ut_error;
  }

  if (info->pass != 0) {
    ib_logger(ib_stream, " pass value %lu", (ulong)info->pass);
  }

  ib_logger(ib_stream, "\n");
}

ulint rw_lock_n_locked() {
  rw_lock_t *lock;
  ulint count = 0;

  mutex_enter(&rw_lock_list_mutex);

  lock = UT_LIST_GET_FIRST(rw_lock_list);

  while (lock != nullptr) {

    if (lock->m_lock_word != X_LOCK_DECR) {
      count++;
    }

    lock = UT_LIST_GET_NEXT(list, lock);
  }

  mutex_exit(&rw_lock_list_mutex);

  return (count);
}
#endif /* UNIV_SYNC_DEBUG */
