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

/** @file sync/sync0sync.c
Mutex, the basic synchronization primitive

Created 9/5/1995 Heikki Tuuri
*******************************************************/

#include "sync0sync.h"

#include "buf0buf.h"
#include "buf0types.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "sync0rw.h"

/*
        REASONS FOR IMPLEMENTING THE SPIN LOCK MUTEX
        ============================================

Semaphore operations in operating systems are slow: Solaris on a 1993 Sparc
takes 3 microseconds (us) for a lock-unlock pair and Windows NT on a 1995
Pentium takes 20 microseconds for a lock-unlock pair. Therefore, we have to
implement our own efficient spin lock mutex. Future operating systems may
provide efficient spin locks, but we cannot count on that.

Another reason for implementing a spin lock is that on multiprocessor systems
it can be more efficient for a processor to run a loop waiting for the
semaphore to be released than to switch to a different thread. A thread switch
takes 25 us on both platforms mentioned above. See Gray and Reuter's book
Transaction processing for background.

How long should the spin loop last before suspending the thread? On a
uniprocessor, spinning does not help at all, because if the thread owning the
mutex is not executing, it cannot be released. Spinning actually wastes
resources.

On a multiprocessor, we do not know if the thread owning the mutex is
executing or not. Thus it would make sense to spin as long as the operation
guarded by the mutex would typically last assuming that the thread is
executing. If the mutex is not released by that time, we may assume that the
thread owning the mutex is not executing and suspend the waiting thread.

A typical operation (where no i/o involved) guarded by a mutex or a read-write
lock may last 1 - 20 us on the current Pentium platform. The longest
operations are the binary searches on an index node.

We conclude that the best choice is to set the spin time at 20 us. Then the
system should work well on a multiprocessor. On a uniprocessor we have to
make sure that thread swithches due to mutex collisions are not frequent,
i.e., they do not happen every 100 us or so, because that wastes too much
resources. If the thread switches are not frequent, the 20 us wasted in spin
loop is not too much.

Empirical studies on the effect of spin time should be done for different
platforms.

        IMPLEMENTATION OF THE MUTEX
        ===========================

For background, see Curt Schimmel's book on Unix implementation on modern
architectures. The key points in the implementation are atomicity and
serialization of memory accesses. The test-and-set instruction (XCHG in
Pentium) must be atomic. As new processors may have weak memory models, also
serialization of memory references may be necessary. The successor of Pentium,
P6, has at least one mode where the memory model is weak. As far as we know,
in Pentium all memory accesses are serialized in the program order and we do
not have to worry about the memory model. On other processors there are
special machine instructions called a fence, memory barrier, or storage
barrier (STBAR in Sparc), which can be used to serialize the memory accesses
to happen in program order relative to the fence instruction.

Leslie Lamport has devised a "bakery algorithm" to implement a mutex without
the atomic test-and-set, but his algorithm should be modified for weak memory
models. We do not use Lamport's algorithm, because we guess it is slower than
the atomic test-and-set.

Our mutex implementation works as follows: After that we perform the atomic
test-and-set instruction on the memory word. If the test returns zero, we
know we got the lock first. If the test returns not zero, some other thread
was quicker and got the lock: then we spin in a loop reading the memory word,
waiting it to become zero. It is wise to just read the word in the loop, not
perform numerous test-and-set instructions, because they generate memory
traffic between the cache and the main memory. The read loop can just access
the cache, saving bus bandwidth.

If we cannot acquire the mutex lock in the specified time, we reserve a cell
in the wait array, set the m_waiters byte in the mutex to 1. To avoid a race
condition, after setting the m_waiters byte and before suspending the waiting
thread, we still have to check that the mutex is reserved, because it may
have happened that the thread which was holding the mutex has just released
it and did not see the m_waiters byte set to 1, a case which would lead the
other thread to an infinite wait.

LEMMA 1: After a thread resets the event of a mutex (or rw_lock), some
=======
thread will eventually call os_event_set() on that particular event.
Thus no infinite wait is possible in this case.

Proof:	After making the reservation the thread sets the m_waiters field in the
mutex to 1. Then it checks that the mutex is still reserved by some thread,
or it reserves the mutex for itself. In any case, some thread (which may be
also some earlier thread, not necessarily the one currently holding the mutex)
will set the m_waiters field to 0 in mutex_exit, and then call
os_event_set() with the mutex as an argument.
Q.E.D.

LEMMA 2: If an os_event_set() call is made after some thread has called
=======
the os_event_reset() and before it starts wait on that event, the call
will not be lost to the second thread. This is true even if there is an
intervening call to os_event_reset() by another thread.
Thus no infinite wait is possible in this case.

Proof (non-windows platforms): os_event_reset() returns a monotonically
increasing value of signal_count. This value is increased at every
call of os_event_set() If thread A has called os_event_reset() followed
by thread B calling os_event_set() and then some other thread C calling
os_event_reset(), the is_set flag of the event will be set to false;
but now if thread A calls os_event_wait_low() with the signal_count
value returned from the earlier call of os_event_reset(), it will
return immediately without waiting.
Q.E.D.

Proof (windows): If there is a writer thread which is forced to wait for
the lock, it may be able to set the state of rw_lock to RW_LOCK_WAIT_EX
The design of rw_lock ensures that there is one and only one thread
that is able to change the state to RW_LOCK_WAIT_EX and this thread is
guaranteed to acquire the lock after it is released by the current
holders and before any other waiter gets the lock.
On windows this thread waits on a separate event i.e.: wait_ex_event.
Since only one thread can wait on this event there is no chance
of this event getting reset before the writer starts wait on it.
Therefore, this thread is guaranteed to catch the os_set_event()
signalled unconditionally at the release of the lock.
Q.E.D. */

void mutex_create(Mutex *mutex, IF_DEBUG(const char *cmutex_name, )  SourceLocation loc) {
  new (mutex) Mutex;

  mutex->init();

  

  

  ut_d(mutex->cmutex_name = cmutex_name);

  /* Check that lock_word is aligned; this is important on Intel */
  ut_ad(((ulint)(&(mutex->lock_word))) % 4 == 0);

}

static void mutex_destroy(Mutex *mutex) noexcept {
  if (mutex->event != nullptr) {
    os_event_free(mutex->event);
    mutex->event = nullptr;
  }

  ut_d(mutex->magic_n = 0);
}

void mutex_free(Mutex *mutex) {
  ut_ad(mutex_validate(mutex));
  ut_a(mutex_get_lock_word(mutex) == 0);
  ut_a(mutex_get_waiters(mutex) == 0);

  mutex_destroy(mutex);
}

ulint mutex_enter_nowait_func(Mutex *mutex ) {
  ut_ad(mutex_validate(mutex));

  if (!mutex_test_and_set(mutex)) {

    ut_d(mutex->thread_id.store(os_thread_get_curr_id(), std::memory_order_relaxed));
    

    /* Succeeded! */
    return 0;
  } else {
    return 1;
  }
}

#ifdef UNIV_DEBUG
bool mutex_validate(const Mutex *mutex) {
  ut_a(mutex != nullptr);
  ut_a(mutex->magic_n == MUTEX_MAGIC_N);
  return true;
}

bool mutex_own(const Mutex *mutex) {
  ut_ad(mutex_validate(mutex));

  return mutex_get_lock_word(mutex) == 1 && os_thread_eq(mutex->thread_id.load(std::memory_order_relaxed), os_thread_get_curr_id());
}
#endif /* UNIV_DEBUG */

void mutex_set_waiters(Mutex *mutex, ulint n) {
  mutex->m_waiters.store(n);
}

void mutex_spin_wait(Mutex *mutex, const char *file_name, ulint line) {
  int64_t sig_count{};
  ulint i;     /* spin round count */

mutex_loop:

  i = 0;

  /* Spin waiting for the lock word to become zero. Note that we do
  not have to assume that the read access to the lock word is atomic,
  as the actual locking is always committed with atomic test-and-set.
  In reality, however, all processors probably have an atomic read of
  a memory word. */

spin_loop:
  ut_d(mutex->count_spin_loop.fetch_add(1, std::memory_order_relaxed));

  while (mutex_get_lock_word(mutex) != 0 && i < SYNC_SPIN_ROUNDS) {
    ut_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));

    i++;
  }

  if (i == SYNC_SPIN_ROUNDS) {
    ut_d(mutex->count_os_yield.fetch_add(1, std::memory_order_relaxed));
    os_thread_yield();
  }

  ut_d(mutex->count_spin_rounds.fetch_add(i, std::memory_order_relaxed));

  if (mutex_test_and_set(mutex) == 0) {
    /* Succeeded! */

    ut_d(mutex->thread_id.store(os_thread_get_curr_id(), std::memory_order_relaxed));
    ;

    goto finish_timing;
  }

  /* We may end up with a situation where lock_word is 0 but the OS
  fast mutex is still reserved. On FreeBSD the OS does not seem to
  schedule a thread which is constantly calling pthread_mutex_trylock
  (in mutex_test_and_set implementation). Then we could end up
  spinning here indefinitely. The following 'i++' stops this infinite
  spin. */

  i++;

  if (i < SYNC_SPIN_ROUNDS) {
    goto spin_loop;
  }

  sig_count = os_event_reset(mutex->event);

  /* The memory order of the array reservation and the change in the
  m_waiters field is important: when we suspend a thread, we first
  reserve the cell and then set m_waiters field to 1. When threads are
  released in mutex_exit, the m_waiters field is first set to zero and
  then the event is set to the signaled state. */

  mutex_set_waiters(mutex, 1);

  /* Try to reserve still a few times */
  for (i = 0; i < 4; i++) {
    if (mutex_test_and_set(mutex) == 0) {
      /* Succeeded! Free the reserved wait cell */

      ut_d(mutex->thread_id.store(os_thread_get_curr_id(), std::memory_order_relaxed));

      

      goto finish_timing;

      /* Note that in this case we leave the m_waiters field
      set to 1. We cannot reset it to zero, as we do not
      know if there are other m_waiters. */
    }
  }

  /* Now we know that there has been some thread holding the mutex
  after the change in the wait array and the m_waiters field was made.
  Now there is no risk of infinite wait on the event. */

  mutex->count_os_wait.fetch_add(1, std::memory_order_relaxed);

  os_event_wait_low(mutex->event, sig_count);
  goto mutex_loop;

finish_timing:

  return;
}

void mutex_signal_object(Mutex *mutex) {
  mutex_set_waiters(mutex, 0);

  /* The memory order of resetting the m_waiters field and
  signaling the object is important. See LEMMA 1 above. */
  os_event_set(mutex->event);
}
