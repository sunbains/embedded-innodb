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

/* Number of spin waits on mutexes: for performance monitoring */

/** The number of iterations in the mutex_spin_wait() spin loop.
Intended for performance monitoring. */
static counter_t mutex_spin_round_count{};

/** The number of mutex_spin_wait() calls.  Intended for performance monitoring. */
static counter_t mutex_spin_wait_count{};

/** The number of OS waits in mutex_spin_wait().  Intended for performance monitoring. */
static counter_t mutex_os_wait_count{};

/** The number of mutex_exit() calls. Intended for performance monitoring. */
counter_t mutex_exit_count{};

/** The global array of wait cells for implementation of the database's own
mutexes and read-write locks */
Sync_check *sync_primary_wait_array;

/** This variable is set to true when sync_init is called */
bool sync_initialized = false;

/** An acquired mutex or rw-lock and its level in the latching order */
typedef struct sync_level_struct sync_level_t;
/** Mutexes or rw-locks held by a thread */
typedef struct sync_thread_struct sync_thread_t;

#ifdef UNIV_SYNC_DEBUG
/** The latch levels currently owned by threads are stored in this data
structure; the size of this array is OS_THREAD_MAX_N */

sync_thread_t *sync_thread_level_arrays;

/** Mutex protecting sync_thread_level_arrays */
static mutex_t sync_thread_mutex;
#endif /* UNIV_SYNC_DEBUG */

/** Global list of database mutexes (not OS mutexes) created. */
ut_mutex_list_base_node_t mutex_list{};

/** Mutex protecting the mutex_list variable */
mutex_t mutex_list_mutex;

/** Latching order checks start when this is set true */
IF_SYNC_DEBUG(bool sync_order_checks_on = false;)

struct sync_thread_struct {
  /** OS thread id */
  os_thread_id_t id;

  /** Level array for this thread; if this is NULL this slot is unused */
  sync_level_t *levels;
};

/** Number of slots reserved for each OS thread in the sync level array */
constexpr ulint SYNC_THREAD_N_LEVELS = 10000;

/** An acquired mutex or rw-lock and its level in the latching order */
struct sync_level_struct {
  /** Pointer to a mutex or an rw-lock; NULL means that the slot is empty */
  void *latch;

  /** Level of the latch in the latching order */
  ulint level;
};

void sync_var_init() {
  mutex_spin_round_count.clear();
  mutex_spin_wait_count.clear();
  mutex_os_wait_count.clear();
  mutex_exit_count.clear();
  sync_primary_wait_array = nullptr;
  sync_initialized = false;

  IF_SYNC_DEBUG(sync_thread_level_arrays = NULL; sync_order_checks_on = false;)
}

void mutex_create(mutex_t *mutex, IF_DEBUG(const char *cmutex_name, ) IF_SYNC_DEBUG(ulint level, ) Source_location loc) {
  new (mutex) mutex_t;

  mutex->init();

  IF_SYNC_DEBUG(mutex->file_name = "not yet reserved"; mutex->level = level;)

  IF_SYNC_DEBUG(mutex->cfile_name = cfile_name; mutex->cline = cline;)

  ut_d(mutex->cmutex_name = cmutex_name);

  /* Check that lock_word is aligned; this is important on Intel */
  ut_ad(((ulint)(&(mutex->lock_word))) % 4 == 0);

  /* NOTE! The very first mutexes are not put to the mutex list */

  if (mutex == &mutex_list_mutex IF_SYNC_DEBUG(|| mutex == &sync_thread_mutex)) {

    return;
  }

  mutex_enter(&mutex_list_mutex);

  ut_ad(UT_LIST_GET_LEN(mutex_list) == 0 || UT_LIST_GET_FIRST(mutex_list)->magic_n == MUTEX_MAGIC_N);

  UT_LIST_ADD_FIRST(mutex_list, mutex);

  mutex_exit(&mutex_list_mutex);
}

static void mutex_destroy(mutex_t *mutex) noexcept {
  if (mutex->event != nullptr) {
    os_event_free(mutex->event);
    mutex->event = nullptr;
  }

  /* If we free the mutex protecting the mutex list (freeing is
  necessary), we have to reset the magic number AFTER removing
  it from the list. */

  ut_d(mutex->magic_n = 0);
}

void mutex_free(mutex_t *mutex) {
  ut_ad(mutex_validate(mutex));
  ut_a(mutex_get_lock_word(mutex) == 0);
  ut_a(mutex_get_waiters(mutex) == 0);

  if (mutex != &mutex_list_mutex IF_SYNC_DEBUG(&&mutex != &sync_thread_mutex)) {
    mutex_enter(&mutex_list_mutex);

    ut_ad(!UT_LIST_GET_PREV(list, mutex) || UT_LIST_GET_PREV(list, mutex)->magic_n == MUTEX_MAGIC_N);
    ut_ad(!UT_LIST_GET_NEXT(list, mutex) || UT_LIST_GET_NEXT(list, mutex)->magic_n == MUTEX_MAGIC_N);

    UT_LIST_REMOVE(mutex_list, mutex);

    mutex_exit(&mutex_list_mutex);
  }

  mutex_destroy(mutex);
}

ulint mutex_enter_nowait_func(mutex_t *mutex IF_SYNC_DEBUG(, const char *file_name, ulint line)) {
  ut_ad(mutex_validate(mutex));

  if (!mutex_test_and_set(mutex)) {

    ut_d(mutex->thread_id = os_thread_get_curr_id());
    IF_SYNC_DEBUG(mutex_set_debug_info(mutex, file_name, line);)

    /* Succeeded! */
    return 0;
  } else {
    return 1;
  }
}

#ifdef UNIV_DEBUG
bool mutex_validate(const mutex_t *mutex) {
  ut_a(mutex != nullptr);
  ut_a(mutex->magic_n == MUTEX_MAGIC_N);
  return true;
}

bool mutex_own(const mutex_t *mutex) {
  ut_ad(mutex_validate(mutex));

  return mutex_get_lock_word(mutex) == 1 && os_thread_eq(mutex->thread_id, os_thread_get_curr_id());
}
#endif /* UNIV_DEBUG */

void mutex_set_waiters(mutex_t *mutex, ulint n) {
  mutex->m_waiters.store(n);
}

void mutex_spin_wait(mutex_t *mutex, const char *file_name, ulint line) {
  ulint i;     /* spin round count */
  ulint index; /* index of the reserved wait cell */

  /* This update is not thread safe, but we don't mind if the count
  isn't exact. Moved out of ifdef that follows because we are willing
  to sacrifice the cost of counting this as the data is valuable.
  Count the number of calls to mutex_spin_wait. */
  mutex_spin_wait_count.inc(1);

mutex_loop:

  i = 0;

  /* Spin waiting for the lock word to become zero. Note that we do
  not have to assume that the read access to the lock word is atomic,
  as the actual locking is always committed with atomic test-and-set.
  In reality, however, all processors probably have an atomic read of
  a memory word. */

spin_loop:
  ut_d(mutex->count_spin_loop++);

  while (mutex_get_lock_word(mutex) != 0 && i < SYNC_SPIN_ROUNDS) {
    if (srv_spin_wait_delay) {
      ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
    }

    i++;
  }

  if (i == SYNC_SPIN_ROUNDS) {
    ut_d(mutex->count_os_yield++);
    os_thread_yield();
  }

  mutex_spin_round_count.inc(i);

  ut_d(mutex->count_spin_rounds += i);

  if (mutex_test_and_set(mutex) == 0) {
    /* Succeeded! */

    ut_d(mutex->thread_id = os_thread_get_curr_id());
    IF_SYNC_DEBUG(mutex_set_debug_info(mutex, file_name, line));

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

  sync_array_reserve_cell(sync_primary_wait_array, mutex, SYNC_MUTEX, file_name, line, &index);

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

      sync_array_free_cell(sync_primary_wait_array, index);

      ut_d(mutex->thread_id = os_thread_get_curr_id());

      IF_SYNC_DEBUG(mutex_set_debug_info(mutex, file_name, line);)

      goto finish_timing;

      /* Note that in this case we leave the m_waiters field
      set to 1. We cannot reset it to zero, as we do not
      know if there are other m_waiters. */
    }
  }

  /* Now we know that there has been some thread holding the mutex
  after the change in the wait array and the m_waiters field was made.
  Now there is no risk of infinite wait on the event. */

  mutex_os_wait_count.inc(1);

  mutex->count_os_wait++;

  sync_array_wait_event(sync_primary_wait_array, index);
  goto mutex_loop;

finish_timing:

  return;
}

void mutex_signal_object(mutex_t *mutex) {
  mutex_set_waiters(mutex, 0);

  /* The memory order of resetting the m_waiters field and
  signaling the object is important. See LEMMA 1 above. */
  os_event_set(mutex->event);
  sync_array_object_signalled(sync_primary_wait_array);
}

#ifdef UNIV_SYNC_DEBUG
void mutex_set_debug_info(mutex_t *mutex, const char *file_name, ulint line) {
  ut_ad(mutex);
  ut_ad(file_name);

  sync_thread_add_level(mutex, mutex->level);

  mutex->file_name = file_name;
  mutex->line = line;
}

void mutex_get_debug_info(mutex_t *mutex, const char **file_name, ulint *line, os_thread_id_t *thread_id) {
  ut_ad(mutex);

  *file_name = mutex->file_name;
  *line = mutex->line;
  *thread_id = mutex->thread_id;
}

/** Prints debug info of currently reserved mutexes. */
static void mutex_list_print_info(ib_stream_t ib_stream) /*!< in: stream where to print */
{
  mutex_t *mutex;
  const char *file_name;
  ulint line;
  os_thread_id_t thread_id;
  ulint count = 0;

  ib_logger(
    ib_stream,
    "----------\n"
    "MUTEX INFO\n"
    "----------\n"
  );

  mutex_enter(&mutex_list_mutex);

  mutex = UT_LIST_GET_FIRST(mutex_list);

  while (mutex != NULL) {
    count++;

    if (mutex_get_lock_word(mutex) != 0) {
      mutex_get_debug_info(mutex, &file_name, &line, &thread_id);
      ib_logger(
        ib_stream,
        "Locked mutex: addr %p thread %ld"
        " file %s line %ld\n",
        (void *)mutex,
        os_thread_pf(thread_id),
        file_name,
        line
      );
    }

    mutex = UT_LIST_GET_NEXT(list, mutex);
  }

  ib_logger(ib_stream, "Total number of mutexes %ld\n", count);

  mutex_exit(&mutex_list_mutex);
}

ulint mutex_n_reserved() {
  mutex_t *mutex;
  ulint count = 0;

  mutex_enter(&mutex_list_mutex);

  mutex = UT_LIST_GET_FIRST(mutex_list);

  while (mutex != NULL) {
    if (mutex_get_lock_word(mutex) != 0) {

      count++;
    }

    mutex = UT_LIST_GET_NEXT(list, mutex);
  }

  mutex_exit(&mutex_list_mutex);

  ut_a(count >= 1);

  return (count - 1); /* Subtract one, because this function itself
                      was holding one mutex (mutex_list_mutex) */
}

bool sync_all_freed(void) {
  return (mutex_n_reserved() + rw_lock_n_locked() == 0);
}

/** Gets the value in the nth slot in the thread level arrays.
@return	pointer to thread slot */
static sync_thread_t *sync_thread_level_arrays_get_nth(ulint n) /*!< in: slot number */
{
  ut_ad(n < OS_THREAD_MAX_N);

  return (sync_thread_level_arrays + n);
}

/** Looks for the thread slot for the calling thread.
@return	pointer to thread slot, NULL if not found */
static sync_thread_t *sync_thread_level_arrays_find_slot(void)

{
  sync_thread_t *slot;
  os_thread_id_t id;
  ulint i;

  id = os_thread_get_curr_id();

  for (i = 0; i < OS_THREAD_MAX_N; i++) {

    slot = sync_thread_level_arrays_get_nth(i);

    if (slot->levels && os_thread_eq(slot->id, id)) {

      return (slot);
    }
  }

  return (NULL);
}

/** Looks for an unused thread slot.
@return	pointer to thread slot */
static sync_thread_t *sync_thread_level_arrays_find_free(void)

{
  sync_thread_t *slot;
  ulint i;

  for (i = 0; i < OS_THREAD_MAX_N; i++) {

    slot = sync_thread_level_arrays_get_nth(i);

    if (slot->levels == NULL) {

      return (slot);
    }
  }

  return (NULL);
}

/** Gets the value in the nth slot in the thread level array.
@return	pointer to level slot */
static sync_level_t *sync_thread_levels_get_nth(
  sync_level_t *arr, /*!< in: pointer to level array
                                              for an OS thread */
  ulint n
) /*!< in: slot number */
{
  ut_ad(n < SYNC_THREAD_N_LEVELS);

  return (arr + n);
}

/** Checks if all the level values stored in the level array are greater than
the given limit.
@return	true if all greater */
static bool sync_thread_levels_g(
  sync_level_t *arr, /*!< in: pointer to level array for an
                                        OS thread */
  ulint limit,       /*!< in: level limit */
  ulint warn
) /*!< in: true=display a diagnostic message */
{
  sync_level_t *slot;
  rw_lock_t *lock;
  mutex_t *mutex;
  ulint i;

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(arr, i);

    if (slot->latch != NULL) {
      if (slot->level <= limit) {

        if (!warn) {

          return (false);
        }

        lock = slot->latch;
        mutex = slot->latch;

        ib_logger(
          ib_stream,
          "sync levels should be"
          " > %lu but a level is %lu\n",
          (ulong)limit,
          (ulong)slot->level
        );

        if (mutex->magic_n == MUTEX_MAGIC_N) {
          ib_logger(ib_stream, "Mutex created at %s %lu\n", mutex->cfile_name, (ulong)mutex->cline);

          if (mutex_get_lock_word(mutex) != 0) {
            const char *file_name;
            ulint line;
            os_thread_id_t thread_id;

            mutex_get_debug_info(mutex, &file_name, &line, &thread_id);

            ib_logger(
              ib_stream,
              "Locked mutex:"
              " addr %p thread %ld"
              " file %s line %ld\n",
              (void *)mutex,
              os_thread_pf(thread_id),
              file_name,
              (ulong)line
            );
          } else {
            ib_logger(ib_stream, "Not locked\n");
          }
        } else {
          rw_lock_print(lock);
        }

        return (false);
      }
    }
  }

  return (true);
}

/** Checks if the level value is stored in the level array.
@return	true if stored */
static bool sync_thread_levels_contain(
  sync_level_t *arr, /*!< in: pointer to level array
                                              for an OS thread */
  ulint level
) /*!< in: level */
{
  sync_level_t *slot;
  ulint i;

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(arr, i);

    if (slot->latch != NULL) {
      if (slot->level == level) {

        return (true);
      }
    }
  }

  return (false);
}

void *sync_thread_levels_contains(ulint level) {
  sync_level_t *arr;
  sync_thread_t *thread_slot;
  sync_level_t *slot;
  ulint i;

  if (!sync_order_checks_on) {

    return (NULL);
  }

  mutex_enter(&sync_thread_mutex);

  thread_slot = sync_thread_level_arrays_find_slot();

  if (thread_slot == NULL) {

    mutex_exit(&sync_thread_mutex);

    return (NULL);
  }

  arr = thread_slot->levels;

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(arr, i);

    if (slot->latch != NULL && slot->level == level) {

      mutex_exit(&sync_thread_mutex);
      return (slot->latch);
    }
  }

  mutex_exit(&sync_thread_mutex);

  return (NULL);
}

void *sync_thread_levels_nonempty_gen(bool dict_mutex_allowed) {
  sync_level_t *arr;
  sync_thread_t *thread_slot;
  sync_level_t *slot;
  ulint i;

  if (!sync_order_checks_on) {

    return (NULL);
  }

  mutex_enter(&sync_thread_mutex);

  thread_slot = sync_thread_level_arrays_find_slot();

  if (thread_slot == NULL) {

    mutex_exit(&sync_thread_mutex);

    return (NULL);
  }

  arr = thread_slot->levels;

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(arr, i);

    if (slot->latch != NULL && (!dict_mutex_allowed || (slot->level != SYNC_DICT && slot->level != SYNC_DICT_OPERATION))) {

      mutex_exit(&sync_thread_mutex);
      ut_error;

      return (slot->latch);
    }
  }

  mutex_exit(&sync_thread_mutex);

  return (NULL);
}

/** Checks that the level array for the current thread is empty.
@return	true if empty */

bool sync_thread_levels_empty(void) {
  return (sync_thread_levels_empty_gen(false));
}

void sync_thread_add_level(void *latch, ulint level) {
  sync_level_t *array;
  sync_level_t *slot;
  sync_thread_t *thread_slot;
  ulint i;

  if (!sync_order_checks_on) {

    return;
  }

  if ((latch == (void *)&sync_thread_mutex) || (latch == (void *)&mutex_list_mutex) || (latch == (void *)&rw_lock_debug_mutex) ||
      (latch == (void *)&rw_lock_list_mutex)) {

    return;
  }

  if (level == SYNC_LEVEL_VARYING) {

    return;
  }

  mutex_enter(&sync_thread_mutex);

  thread_slot = sync_thread_level_arrays_find_slot();

  if (thread_slot == NULL) {
    /* We have to allocate the level array for a new thread */
    array = ut_new(sizeof(sync_level_t) * SYNC_THREAD_N_LEVELS);

    thread_slot = sync_thread_level_arrays_find_free();

    thread_slot->id = os_thread_get_curr_id();
    thread_slot->levels = array;

    for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

      slot = sync_thread_levels_get_nth(array, i);

      slot->latch = NULL;
    }
  }

  array = thread_slot->levels;

  /* NOTE that there is a problem with _NODE and _LEAF levels: if the
  B-tree height changes, then a leaf can change to an internal node
  or the other way around. We do not know at present if this can cause
  unnecessary assertion failures below. */

  switch (level) {
    case SYNC_NO_ORDER_CHECK:
    case SYNC_EXTERN_STORAGE:
    case SYNC_TREE_NODE_FROM_HASH:
      /* Do no order checking */
      break;
    case SYNC_MEM_POOL:
    case SYNC_MEM_HASH:
    case SYNC_RECV:
    case SYNC_WORK_QUEUE:
    case SYNC_LOG:
    case SYNC_THR_LOCAL:
    case SYNC_ANY_LATCH:
    case SYNC_TRX_SYS_HEADER:
    case SYNC_FILE_FORMAT_TAG:
    case SYNC_DOUBLEWRITE:
    case SYNC_BUF_POOL:
    case SYNC_SEARCH_SYS:
    case SYNC_SEARCH_SYS_CONF:
    case SYNC_TRX_LOCK_HEAP:
    case SYNC_KERNEL:
    case SYNC_RSEG:
    case SYNC_TRX_UNDO:
    case SYNC_PURGE_LATCH:
    case SYNC_PURGE_SYS:
    case SYNC_DICT_AUTOINC_MUTEX:
    case SYNC_DICT_OPERATION:
    case SYNC_DICT_HEADER:
    case SYNC_TRX_I_S_RWLOCK:
    case SYNC_TRX_I_S_LAST_READ:
      if (!sync_thread_levels_g(array, level, true)) {
        ib_logger(
          ib_stream,
          "sync_thread_levels_g(array, %lu)"
          " does not hold!\n",
          level
        );
        ut_error;
      }
      break;
    case SYNC_BUF_BLOCK:
      /* The thread must own the buffer pool mutex
    (buf_pool_mutex), or it is allowed to latch only ONE
    buffer block (block->mutex). */
      if (!sync_thread_levels_g(array, level, false)) {
        ut_a(sync_thread_levels_g(array, level - 1, true));
        ut_a(sync_thread_levels_contain(array, SYNC_BUF_POOL));
      }
      break;
    case SYNC_REC_LOCK:
      if (sync_thread_levels_contain(array, SYNC_KERNEL)) {
        ut_a(sync_thread_levels_g(array, SYNC_REC_LOCK - 1, true));
      } else {
        ut_a(sync_thread_levels_g(array, SYNC_REC_LOCK, true));
      }
      break;
    case SYNC_FSP_PAGE:
      ut_a(sync_thread_levels_contain(array, SYNC_FSP));
      break;
    case SYNC_FSP:
      ut_a(sync_thread_levels_contain(array, SYNC_FSP) || sync_thread_levels_g(array, SYNC_FSP, true));
      break;
    case SYNC_TRX_UNDO_PAGE:
      ut_a(
        sync_thread_levels_contain(array, SYNC_TRX_UNDO) || sync_thread_levels_contain(array, SYNC_RSEG) ||
        sync_thread_levels_contain(array, SYNC_PURGE_SYS) || sync_thread_levels_g(array, SYNC_TRX_UNDO_PAGE, true)
      );
      break;
    case SYNC_RSEG_HEADER:
      ut_a(sync_thread_levels_contain(array, SYNC_RSEG));
      break;
    case SYNC_RSEG_HEADER_NEW:
      ut_a(sync_thread_levels_contain(array, SYNC_KERNEL) && sync_thread_levels_contain(array, SYNC_FSP_PAGE));
      break;
    case SYNC_TREE_NODE:
      ut_a(
        sync_thread_levels_contain(array, SYNC_INDEX_TREE) || sync_thread_levels_contain(array, SYNC_DICT_OPERATION) ||
        sync_thread_levels_g(array, SYNC_TREE_NODE - 1, true)
      );
      break;
    case SYNC_TREE_NODE_NEW:
      ut_a(sync_thread_levels_contain(array, SYNC_FSP_PAGE));
      break;
    case SYNC_INDEX_TREE:
      if (sync_thread_levels_contain(array, SYNC_FSP)) {
        ut_a(sync_thread_levels_g(array, SYNC_FSP_PAGE - 1, true));
      } else {
        ut_a(sync_thread_levels_g(array, SYNC_TREE_NODE - 1, true));
      }
      break;
    case SYNC_DICT:
#ifdef UNIV_DEBUG
      ut_a(buf_debug_prints || sync_thread_levels_g(array, SYNC_DICT, true));
#else  /* UNIV_DEBUG */
      ut_a(sync_thread_levels_g(array, SYNC_DICT, true));
#endif /* UNIV_DEBUG */
      break;
    default:
      ut_error;
  }

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(array, i);

    if (slot->latch == NULL) {
      slot->latch = latch;
      slot->level = level;

      break;
    }
  }

  ut_a(i < SYNC_THREAD_N_LEVELS);

  mutex_exit(&sync_thread_mutex);
}

/** Removes a latch from the thread level array if it is found there.
@return true if found in the array; it is no error if the latch is
not found, as we presently are not able to determine the level for
every latch reservation the program does */

bool sync_thread_reset_level(void *latch) /*!< in: pointer to a mutex or an rw-lock */
{
  sync_level_t *array;
  sync_level_t *slot;
  sync_thread_t *thread_slot;
  ulint i;

  if (!sync_order_checks_on) {

    return (false);
  }

  if ((latch == (void *)&sync_thread_mutex) || (latch == (void *)&mutex_list_mutex) || (latch == (void *)&rw_lock_debug_mutex) ||
      (latch == (void *)&rw_lock_list_mutex)) {

    return (false);
  }

  mutex_enter(&sync_thread_mutex);

  thread_slot = sync_thread_level_arrays_find_slot();

  if (thread_slot == NULL) {

    ut_error;

    mutex_exit(&sync_thread_mutex);
    return (false);
  }

  array = thread_slot->levels;

  for (i = 0; i < SYNC_THREAD_N_LEVELS; i++) {

    slot = sync_thread_levels_get_nth(array, i);

    if (slot->latch == latch) {
      slot->latch = NULL;

      mutex_exit(&sync_thread_mutex);

      return (true);
    }
  }

  if (((mutex_t *)latch)->magic_n != MUTEX_MAGIC_N) {
    rw_lock_t *rw_lock;

    rw_lock = (rw_lock_t *)latch;

    if (rw_lock->level == SYNC_LEVEL_VARYING) {
      mutex_exit(&sync_thread_mutex);

      return (true);
    }
  }

  ut_error;

  mutex_exit(&sync_thread_mutex);

  return (false);
}
#endif /* UNIV_SYNC_DEBUG */

void sync_init() {

  ut_a(sync_initialized == false);

  sync_initialized = true;

  /* Create the primary system wait array which is protected by an OS
  mutex */

  sync_primary_wait_array = sync_array_create(OS_THREAD_MAX_N, SYNC_ARRAY_OS_MUTEX);

  IF_SYNC_DEBUG(
    /* Create the thread latch level array where the latch levels
    are stored for each OS thread */

    sync_thread_level_arrays = new sync_thread_t[OS_THREAD_MAX_N];

    for (ulint i = 0; i < OS_THREAD_MAX_N; i++) {
      auto thread_slot = sync_thread_level_arrays_get_nth(i);
      thread_slot->levels = nullptr;
    }
  )

  /* Init the mutex list and create the mutex to protect it. */
  UT_LIST_INIT(mutex_list);
  mutex_create(&mutex_list_mutex, IF_DEBUG("mutex_list_mutex", ) IF_SYNC_DEBUG(SYNC_NO_ORDER_CHECK, ) Current_location());

  IF_SYNC_DEBUG(mutex_create(&sync_thread_mutex, IF_DEBUG("sync_thread", ) SYNC_NO_ORDER_CHECK, Current_location());)

  /* Init the rw-lock list and create the mutex to protect it. */

  UT_LIST_INIT(rw_lock_list);
  mutex_create(&rw_lock_list_mutex, IF_DEBUG("rw_lock_list_mutex", ) IF_SYNC_DEBUG(SYNC_NO_ORDER_CHECK, ) Current_location());

  IF_SYNC_DEBUG(mutex_create(&rw_lock_debug_mutex, "rw_lock_debug_mutex", SYNC_NO_ORDER_CHECK, Current_location());

                rw_lock_debug_event = os_event_create(nullptr);
                rw_lock_debug_m_waiters = false;)
}

void sync_close() {
  sync_array_free(sync_primary_wait_array);

  for (auto mutex = UT_LIST_GET_FIRST(mutex_list); mutex != nullptr; mutex = UT_LIST_GET_FIRST(mutex_list)) {
    mutex_free(mutex);
  }

  mutex_free(&mutex_list_mutex);

  IF_SYNC_DEBUG(mutex_free(&sync_thread_mutex); sync_order_checks_on = false;

                /* Switch latching order checks on in sync0sync.c */
                sync_order_checks_on = false;

                delete {} sync_thread_level_arrays;
                sync_thread_level_arrays = nullptr;)

  sync_initialized = false;
}

void sync_print_wait_info(ib_stream_t ib_stream) {
  IF_SYNC_DEBUG(
    ib_logger(ib_stream, "Mutex exits %lu, rws exits %lu, rwx exits %lu\n", mutex_exit_count, rw_s_exit_count, rw_x_exit_count);
  )

  ib_logger(
    ib_stream,
    "Mutex spin waits %lu, rounds %lu, OS waits %lu\n"
    "RW-shared spins %lu, OS waits %lu;"
    " RW-excl spins %lu, OS waits %lu\n",
    mutex_spin_wait_count.value(),
    mutex_spin_round_count.value(),
    mutex_os_wait_count.value(),
    rw_s_spin_wait_count,
    rw_s_os_wait_count,
    rw_x_spin_wait_count,
    rw_x_os_wait_count
  );

  ib_logger(
    ib_stream,
    "Spin rounds per wait: %.2f mutex, %.2f RW-shared, %.2f RW-excl\n",
    (double)mutex_spin_round_count.value() / (mutex_spin_wait_count.value() ? mutex_spin_wait_count.value() : 1),
    (double)rw_s_spin_round_count / (rw_s_spin_wait_count ? rw_s_spin_wait_count : 1),
    (double)rw_x_spin_round_count / (rw_x_spin_wait_count ? rw_x_spin_wait_count : 1)
  );
}

void sync_print(ib_stream_t ib_stream) {
  IF_SYNC_DEBUG(mutex_list_print_info(ib_stream); rw_lock_list_print_info(ib_stream);)

  sync_array_print_info(ib_stream, sync_primary_wait_array);

  sync_print_wait_info(ib_stream);
}
