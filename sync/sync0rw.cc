/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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
                lock->m_recursive must be false in following states:
                        1) The writer_thread contains garbage i.e.: the
                        lock has just been initialized.
                        2) The lock is not x-held and there is no
                        x-waiter waiting on WAIT_EX event.
                        3) The lock is x-held or there is an x-waiter
                        waiting on WAIT_EX event but the 'pass' value
                        is non-zero.
                lock->m_recursive is true iff:
                        1) The lock is x-held or there is an x-waiter
                        waiting on WAIT_EX event and the 'pass' value
                        is zero.
                This flag must be set after the writer_thread field
                has been updated with a memory ordering barrier.
                It is unset before the lock_word has been incremented.
writer_thread:	Is used only in recursive x-locking. Can only be safely
                read iff lock->m_recursive flag is true.
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
                   (2) Record counter value of wait_ex_event (os_event_reset).
                   (3) Verify that lock_word < 0.
                (1) must come first to ensures no other threads become reader
                or next writer, and notifies unlocker that signal must be sent.
                (2) must come before (3) to ensure the signal is not missed.
                These restrictions force the above ordering.
                Immediately before sending the wake-up signal, we should:
                   Verify lock_word == 0 (waiting thread holds x_lock)
*/


void rw_lock_create_func(
  RwLock *lock, /*!< in: pointer to memory */
#ifdef UNIV_DEBUG
  const char *cmutex_name,
#endif /* UNIV_DEBUG */
  const char *cfile_name, ulint cline
) {
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


  lock->m_magic_n = RW_LOCK_MAGIC_N;

  lock->m_cfile_name = cfile_name;
  lock->m_cline = (unsigned int)cline;

  lock->m_count_os_wait.store(0, std::memory_order_relaxed);
  lock->m_last_x_file_name = "not yet reserved";
  lock->m_last_x_line = 0;
  lock->m_event = os_event_create(nullptr);
  lock->m_wait_ex_event = os_event_create(nullptr);
}

void rw_lock_free(RwLock *lock) {
  ut_ad(rw_lock_validate(lock));
  ut_a(lock->m_lock_word == X_LOCK_DECR);

  lock->m_magic_n = 0;

  os_event_free(lock->m_event);

  os_event_free(lock->m_wait_ex_event);
}

#ifdef UNIV_DEBUG
bool rw_lock_validate(RwLock *lock) {
  ut_a(lock != nullptr);

  ulint waiters = rw_lock_get_waiters(lock);
  lint lock_word = lock->m_lock_word;

  ut_a(lock->m_magic_n == RW_LOCK_MAGIC_N);
  ut_a(waiters == 0 || waiters == 1);
  ut_a(lock_word > -X_LOCK_DECR || (-lock_word) % X_LOCK_DECR == 0);

  return true;
}
#endif /* UNIV_DEBUG */

void rw_lock_s_lock_spin(RwLock *lock, ulint pass, const char *file_name, ulint line) {
  ulint i{};

  ut_ad(rw_lock_validate(lock));

lock_loop:

  /* Spin waiting for the writer field to become free */
  while (i < SYNC_SPIN_ROUNDS && lock->m_lock_word <= 0) {
    ut_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));

    i++;
  }

  if (i == SYNC_SPIN_ROUNDS) {
    os_thread_yield();
  }

  /* We try once again to obtain the lock */
  if (true == rw_lock_s_lock_low(lock, pass, file_name, line)) {

    return; /* Success */
  } else {

    if (i < SYNC_SPIN_ROUNDS) {
      goto lock_loop;
    }


    const auto sig_count = os_event_reset(lock->m_event);

    /* Set waiters before checking lock_word to ensure wake-up
    signal is sent. This may lead to some unnecessary signals. */
    rw_lock_set_waiter_flag(lock);

    if (true == rw_lock_s_lock_low(lock, pass, file_name, line)) {
      return; /* Success */
    }

    /* these stats may not be accurate */
    lock->m_count_os_wait.fetch_add(1, std::memory_order_relaxed);

    os_event_wait_low(lock->m_event, sig_count);

    i = 0;
    goto lock_loop;
  }
}

void rw_lock_x_lock_move_ownership(RwLock *lock) {
  ut_ad(rw_lock_is_locked(lock, RW_LOCK_EX));

  rw_lock_set_writer_id_and_recursion_flag(lock, true);
}

/** Function for the next writer to call. Waits for readers to exit.
The caller must have already decremented lock_word by X_LOCK_DECR.
@param[in] lock                 Select the next writer waiting on this lock.
@param[in] pass                 Value; != 0, if the lock will be passed
                                to another thread to unlock
@param[in] file_name            File name where lock requested
@param[in] line                 Line where requested */
inline void rw_lock_x_lock_wait(
  RwLock *lock,
  const char *file_name, ulint line
) {
  ulint i = 0;

  ut_ad(lock->m_lock_word <= 0);

  while (lock->m_lock_word < 0) {
    ut_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));

    if (i < SYNC_SPIN_ROUNDS) {
      i++;
      continue;
    }

    /* If there is still a reader, then go to sleep.*/

    i = 0;

    const auto sig_count = os_event_reset(lock->m_wait_ex_event);

    /* Check lock_word to ensure wake-up isn't missed.*/
    if (lock->m_lock_word < 0) {

      /* These stats may not be accurate */
      lock->m_count_os_wait.fetch_add(1, std::memory_order_relaxed);

      /* Add debug info as it is needed to detect possible
      deadlock. We must add info for WAIT_EX thread for
      deadlock detection to work properly. */

      os_event_wait_low(lock->m_wait_ex_event, sig_count);

      /* It is possible to wake when lock_word < 0.
      We must pass the while-loop check to proceed.*/
    } else {
    }
  }

}

/** Low-level function for acquiring an exclusive lock.
@param[in,out] lock,       Lock instance on which to acquire an x-lock
@param[in] pass            Value; != 0, if the lock will be passed to
                           another thread to unlock
@param[in] file_name       File name where lock requested
@param[in] line            Line in filen_ame where requested
@return	RW_LOCK_NOT_LOCKED if did not succeed, RW_LOCK_EX if success. */
inline bool rw_lock_x_lock_low(RwLock *lock, ulint pass, const char *file_name, ulint line) {
  if (rw_lock_lock_word_decr(lock, X_LOCK_DECR)) {

    /* lock->m_recursive also tells us if the writer_thread
    field is stale or active. As we are going to write
    our own thread id in that field it must be that the
    current writer_thread value is not active. */
    ut_a(!lock->m_recursive);

    /* Decrement occurred: we are writer or next-writer. */
    rw_lock_set_writer_id_and_recursion_flag(lock, pass ? false : true);

    rw_lock_x_lock_wait(
      lock,
      file_name,
      line
    );

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

  lock->m_last_x_file_name = file_name;
  lock->m_last_x_line = (unsigned int)line;

  return true;
}

void rw_lock_x_lock_func(RwLock *lock, ulint pass, const char *file_name, ulint line) {
  ulint i;
  bool spinning{false};

  ut_ad(rw_lock_validate(lock));

  i = 0;

lock_loop:

  if (rw_lock_x_lock_low(lock, pass, file_name, line)) {

    return; /* Locking succeeded */

  } else {

    if (!spinning) {
      spinning = true;
    }

    /* Spin waiting for the lock_word to become free */
    while (i < SYNC_SPIN_ROUNDS && lock->m_lock_word <= 0) {
      ut_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));

      i++;
    }
    if (i == SYNC_SPIN_ROUNDS) {
      os_thread_yield();
    } else {
      goto lock_loop;
    }
  }


  const auto sig_count = os_event_reset(lock->m_event);

  /* Waiters must be set before checking lock_word, to ensure signal
  is sent. This could lead to a few unnecessary wake-up signals. */
  rw_lock_set_waiter_flag(lock);

  if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
    return; /* Locking succeeded */
  }

  /* these stats may not be accurate */
  lock->m_count_os_wait.fetch_add(1, std::memory_order_relaxed);

  os_event_wait_low(lock->m_event, sig_count);

  i = 0;
  goto lock_loop;
}


bool rw_lock_is_locked(RwLock *lock, ulint lock_type) {
  bool ret = false;

  ut_ad(lock);
  ut_ad(rw_lock_validate(lock));

  if (lock_type == RW_LOCK_SHARED) {
    if (rw_lock_get_reader_count(lock) > 0) {
      ret = true;
    }
  } else if (lock_type == RW_LOCK_EX) {
    if (rw_lock_get_writer(lock) == RW_LOCK_EX) {
      ret = true;
    }
  } else {
    ut_error;
  }

  return ret;
}

