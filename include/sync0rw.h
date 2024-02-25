/*****************************************************************************
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

/** @file include/sync0rw.h
The read-write lock (for threads, not for database transactions)

Created 9/11/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "univ.i"

#include <atomic>

#include "os0sync.h"
#include "sync0sync.h"
#include "ut0lst.h"

/** Latch types; these are used also in btr0btr.h: keep the numerical values
smaller than 30 and the order of the numerical values like below! */
constexpr int RW_S_LATCH = 1;
constexpr int RW_X_LATCH = 2;
constexpr int RW_NO_LATCH = 3;

/** We decrement m_lock_word by this amount for each x_lock. It is also the
start value for the m_lock_word, meaning that it limits the maximum number
of concurrent read locks before the rw_lock breaks. The current value of
0x00100000 allows 1,048,575 concurrent readers and 2047 recursive writers.*/
constexpr int X_LOCK_DECR = 0x00100000;

typedef struct rw_lock_struct rw_lock_t;

#ifdef UNIV_SYNC_DEBUG
typedef struct rw_lock_debug_struct rw_lock_debug_t;
#endif /* UNIV_SYNC_DEBUG */

typedef UT_LIST_BASE_NODE_T(rw_lock_t) rw_lock_list_t;

extern rw_lock_list_t rw_lock_list;
extern mutex_t rw_lock_list_mutex;

#ifdef UNIV_SYNC_DEBUG
/** The global mutex which protects debug info lists of all rw-locks.
To modify the debug info list of an rw-lock, this mutex has to be

acquired in addition to the mutex protecting the lock. */
extern mutex_t rw_lock_debug_mutex;

/** If deadlock detection does not get immediately the mutex it may wait for this event */
extern os_event_t rw_lock_debug_event;

/** This is set to TRUE, if there may be waiters for the event */
extern ibool rw_lock_debug_waiters;
#endif /* UNIV_SYNC_DEBUG */

/** number of spin waits on rw-latches,
resulted during exclusive (write) locks */
extern int64_t rw_s_spin_wait_count;

/** number of spin loop rounds on rw-latches,
resulted during exclusive (write) locks */
extern int64_t rw_s_spin_round_count;

/** number of unlocks (that unlock shared locks),
set only when UNIV_SYNC_PERF_STAT is defined */
extern int64_t rw_s_exit_count;

/** number of OS waits on rw-latches,
resulted during shared (read) locks */
extern int64_t rw_s_os_wait_count;

/** number of spin waits on rw-latches,
resulted during shared (read) locks */
extern int64_t rw_x_spin_wait_count;

/** number of spin loop rounds on rw-latches,
resulted during shared (read) locks */
extern int64_t rw_x_spin_round_count;

/** number of OS waits on rw-latches,
resulted during exclusive (write) locks */
extern int64_t rw_x_os_wait_count;

/** number of unlocks (that unlock exclusive locks),
set only when UNIV_SYNC_PERF_STAT is defined */
extern int64_t rw_x_exit_count;

/** Creates, or rather, initializes an rw-lock object in a specified memory
location (which must be appropriately aligned). The rw-lock is initialized
to the non-locked state. Explicit freeing of the rw-lock with rw_lock_free
is necessary only if the memory block containing it is freed. */
#ifdef UNIV_DEBUG
#ifdef UNIV_SYNC_DEBUG
#define rw_lock_create(L, level)                                               \
  rw_lock_create_func((L), (level), #L, __FILE__, __LINE__)
#else /* UNIV_SYNC_DEBUG */
#define rw_lock_create(L, level)                                               \
  rw_lock_create_func((L), #L, __FILE__, __LINE__)
#endif /* UNIV_SYNC_DEBUG */
#else  /* UNIV_DEBUG */
#define rw_lock_create(L, level) rw_lock_create_func((L), __FILE__, __LINE__)
#endif /* UNIV_DEBUG */

/** Creates, or rather, initializes an rw-lock object in a specified memory
location (which must be appropriately aligned). The rw-lock is initialized
to the non-locked state. Explicit freeing of the rw-lock with rw_lock_free
is necessary only if the memory block containing it is freed. */
void rw_lock_create_func(
    rw_lock_t *lock, /** in: pointer to memory */
#ifdef UNIV_DEBUG
#ifdef UNIV_SYNC_DEBUG
    ulint level,             /** in: level */
#endif /* UNIV_SYNC_DEBUG */
    const char *cmutex_name, /** in: mutex name */
#endif                       /* UNIV_DEBUG */
    const char *cfile_name,  /** in: file name where created */
    ulint cline);            /** in: file line where created */

/** Calling this function is obligatory only if the memory buffer containing
the rw-lock is freed. Removes an rw-lock object from the global list. The
rw-lock is checked to be in the non-locked state. */
void rw_lock_free(rw_lock_t *lock); /** in: rw-lock */

#ifdef UNIV_DEBUG
/** Checks that the rw-lock has been initialized and that there are no
simultaneous shared and exclusive locks.
@return	TRUE */
ibool rw_lock_validate(rw_lock_t *lock); /** in: rw-lock */
#endif /* UNIV_DEBUG */

/** NOTE! The following macros should be used in rw s-locking, not the
corresponding function. */

#define rw_lock_s_lock(M) rw_lock_s_lock_func((M), 0, __FILE__, __LINE__)
/** NOTE! The following macros should be used in rw s-locking, not the
corresponding function. */

#define rw_lock_s_lock_gen(M, P)                                               \
  rw_lock_s_lock_func((M), (P), __FILE__, __LINE__)

/** NOTE! The following macros should be used in rw s-locking, not the
corresponding function. */

#define rw_lock_s_lock_nowait(M, F, L) rw_lock_s_lock_low((M), 0, (F), (L))

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_s_unlock_gen(L, P) rw_lock_s_unlock_func(P, L)
#else
#define rw_lock_s_unlock_gen(L, P) rw_lock_s_unlock_func(L)
#endif

/** Releases a shared mode lock. */
#define rw_lock_s_unlock(L) rw_lock_s_unlock_gen(L, 0)

/** NOTE! The following macro should be used in rw x-locking, not the
corresponding function. */

#define rw_lock_x_lock(M) rw_lock_x_lock_func((M), 0, __FILE__, __LINE__)
/** NOTE! The following macro should be used in rw x-locking, not the
corresponding function. */

#define rw_lock_x_lock_gen(M, P)                                               \
  rw_lock_x_lock_func((M), (P), __FILE__, __LINE__)

/** NOTE! The following macros should be used in rw x-locking, not the
corresponding function. */

#define rw_lock_x_lock_nowait(M)                                               \
  rw_lock_x_lock_func_nowait((M), __FILE__, __LINE__)

/** NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in exclusive mode for the current thread. If the rw-lock is locked
in shared or exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the lock, before suspending the thread. If the same thread has an x-lock
on the rw-lock, locking succeed, with the following exception: if pass != 0,
only a single x-lock may be taken on the lock. NOTE: If the same thread has
an s-lock, locking does not succeed! */

void rw_lock_x_lock_func(
    rw_lock_t *lock,       /** in: pointer to rw-lock */
    ulint pass,            /** in: pass value; != 0, if the lock will
                           be passed to another thread to unlock */
    const char *file_name, /** in: file name where lock requested */
    ulint line);           /** in: line where requested */

/** Releases an exclusive mode lock. */
inline
void rw_lock_x_unlock_func(
#ifdef UNIV_SYNC_DEBUG
    ulint pass, /** in: pass value; != 0, if the lock may have
                been passed to another thread to unlock */
#endif
    rw_lock_t *lock); /** in/out: rw-lock */

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_x_unlock_gen(L, P) rw_lock_x_unlock_func(P, L)
#else
#define rw_lock_x_unlock_gen(L, P) rw_lock_x_unlock_func(L)
#endif /* UNIV_SYNC_DEBUG */

/** Releases an exclusive mode lock. */
#define rw_lock_x_unlock(L) rw_lock_x_unlock_gen(L, 0)

/** This function is used in the insert buffer to move the ownership of an
x-latch on a buffer frame to the current thread. The x-latch was set by
the buffer read operation and it protected the buffer frame while the
read was done. The ownership is moved because we want that the current
thread is able to acquire a second x-latch which is stored in an mtr.
This, in turn, is needed to pass the debug checks of index page
operations. */
void rw_lock_x_lock_move_ownership(
    rw_lock_t *lock); /** in: lock which was x-locked in the
                      buffer read */

#ifdef UNIV_SYNC_DEBUG
/** Checks if the thread has locked the rw-lock in the specified mode, with
the pass value == 0. */

ibool rw_lock_own(rw_lock_t *lock, /** in: rw-lock */
                  ulint lock_type) /** in: lock type: RW_LOCK_SHARED,
                                   RW_LOCK_EX */
    __attribute__((warn_unused_result));
#endif /* UNIV_SYNC_DEBUG */

/** Checks if somebody has locked the rw-lock in the specified mode. */
ibool rw_lock_is_locked(rw_lock_t *lock,  /** in: rw-lock */
                        ulint lock_type); /** in: lock type: RW_LOCK_SHARED,
                                          RW_LOCK_EX */

#ifdef UNIV_SYNC_DEBUG
/** Prints debug info of an rw-lock. */
void rw_lock_print(rw_lock_t *lock); /** in: rw-lock */
/** Prints debug info of currently locked rw-locks. */

void rw_lock_list_print_info(
    ib_stream_t ib_stream); /** in: stream where to print */

/** Returns the number of currently locked rw-locks.
Works only in the debug version.
@return	number of locked rw-locks */
ulint rw_lock_n_locked(void);

/*#####################################################################*/

/** Acquires the debug mutex. We cannot use the mutex defined in sync0sync,
because the debug mutex is also acquired in sync0arr while holding the OS
mutex protecting the sync array, and the ordinary mutex_enter might
recursively call routines in sync0arr, leading to a deadlock on the OS
mutex. */
void rw_lock_debug_mutex_enter(void);

/** Releases the debug mutex. */
void rw_lock_debug_mutex_exit(void);

/** Prints info of a debug struct. */
void rw_lock_debug_print(rw_lock_debug_t *info); /** in: debug struct */
#endif /* UNIV_SYNC_DEBUG */

/** Reset the variables. */
void rw_lock_var_init(void);

/* NOTE! The structure appears here only for the compiler to know its size.
Do not use its fields directly! */

/** The structure used in the spin lock implementation of a read-write
lock. Several threads may have a shared lock simultaneously in this
lock, but only one writer may have an exclusive lock, in which case no
shared locks are allowed. To prevent starving of a writer blocked by
readers, a writer may queue for x-lock by decrementing m_lock_word: no
new readers will be let in while the thread waits for readers to
exit. */
struct rw_lock_struct {
  /** Holds the state of the lock. */
  std::atomic<int32_t> m_lock_word;

   /** true if there are waiters */
  std::atomic<bool> m_waiters;

  /** Default value FALSE which means the lock is non-recursive.
  The value is typically set to TRUE making normal rw_locks recursive.
  In case of asynchronous IO, when a non-zero value of 'pass' is passed
  then we keep the lock non-recursive. This flag also tells us about
  the state of writer_thread field. If this flag is set then writer_thread
  MUST contain the thread id of the current x-holder or wait-x thread.
  This flag must be reset in x_unlock functions before incrementing
  the m_lock_word */
  std::atomic<bool> m_recursive;

  /** Default value FALSE which means the lock is non-recursive. The
  value is typically set to TRUE making normal rw_locks recursive. In
  case of asynchronous IO, when a non-zero value of 'pass' is passed
  then we keep the lock non-recursive.  This flag also tells us about
  the state of writer_thread field. If this flag is set then m_writer_thread
  MUST contain the thread id of the current x-holder or wait-x thread.
  This flag must be reset in x_unlock functions before incrementing
  the m_lock_word */
  std::atomic<std::thread::id> m_writer_thread;

  /** Thread id of writer thread. Is only
  guaranteed to have sane and non-stale
  value iff recursive flag is set. */

  /** Used by sync0arr.c for thread queueing */
  os_event_t m_event;

  /** Event for next-writer to wait on. A thread
  must decrement m_lock_word before waiting. */
  os_event_t m_wait_ex_event;

  /** Event for next-writer to wait on. A thread
  must decrement m_lock_word before waiting. */

  /** All allocated rw locks are put into a list */
  UT_LIST_NODE_T(rw_lock_t) list;

#ifdef UNIV_SYNC_DEBUG
  /** In the debug version: pointer to the debug
  info list of the lock */
  UT_LIST_BASE_NODE_T(rw_lock_debug_t) debug_list;

  /** Level in the global latching order. */
  ulint m_level;
#endif /* UNIV_SYNC_DEBUG */

  /** Count of os_waits. May not be accurate */
  ulint m_count_os_wait;

  /** File name where lock created */
  const char *m_cfile_name;

  /* last s-lock file/line is not guaranteed to be correct */

  /** File name where last s-locked */
  const char *m_last_s_file_name;

  /** File name where last x-locked */
  const char *m_last_x_file_name;

  /** This is TRUE if the writer field is
  RW_LOCK_WAIT_EX; this field is located far
  from the memory update hotspot fields which
  are at the start of this struct, thus we can
  peek this field without causing much memory
  bus traffic */
  bool m_writer_is_wait_ex;

  /** Line where created */
  unsigned m_cline : 14;

  /** Line number where last time s-locked */
  unsigned m_last_s_line : 14;

  /** Line number where last time x-locked */
  unsigned m_last_x_line : 14;

  /** RW_LOCK_MAGIC_N */
  ulint m_magic_n;
};

/** Value of rw_lock_struct::magic_n */
#define RW_LOCK_MAGIC_N 22643

#ifdef UNIV_SYNC_DEBUG
/** The structure for storing debug info of an rw-lock */
struct rw_lock_debug_struct {

  /** The thread id of the thread which locked the rw-lock */
  os_thread_id_t thread_id;

  /** Pass value given in the lock operation */
  ulint pass;

  /** Type of the lock: RW_LOCK_EX, RW_LOCK_SHARED, RW_LOCK_WAIT_EX */
  ulint lock_type;

  /** File name where the lock was obtained */
  const char *file_name;

  /** Line where the rw-lock was locked */
  ulint line;

  /** Debug structs are linked in a two-way list */
  UT_LIST_NODE_T(rw_lock_debug_t) list;
};
#endif /* UNIV_SYNC_DEBUG */

/** Lock an rw-lock in shared mode for the current thread. If the rw-lock is
locked in exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS),
waiting for the lock before suspending the thread.
@param[in,out] lock             Lock to spin on.
@param[in,out] pass             Pass value; != 0, if the lock will
                                be passed to another thread to unlock
@param[in] file_name            Namne of file where requested.
@param[in] line                 Line in filename where requested. */
void rw_lock_s_lock_spin(rw_lock_t *lock, ulint pass, const char *file_name, ulint line);

#ifdef UNIV_SYNC_DEBUG

/** Inserts the debug information for an rw-lock.
@param[in,out] lock             Lock to track
@param[in,out] pass             Pass value; != 0, if the lock will
                                be passed to another thread to unlock
@param[in] file_name            Namne of file where requested.
@param[in] line                 Line in filename where requested. */
void rw_lock_add_debug_info(rw_lock_t *lock, ulint pass, ulint lock_type, const char *file_name, ulint line);

/** Removes a debug information struct for an rw-lock.
@param[in,out] lock             Lock to remove.
@param[in] pass                 Pass value
@param[in] type                 lock type */
void rw_lock_remove_debug_info(rw_lock_t *lock, ulint pass, ulint lock_type);
#endif /* UNIV_SYNC_DEBUG */

/** Check if there are threads waiting for the rw-lock.
@param[in,out] lock             Lock to check.
@return	true if waiters, false otherwise */
inline
bool rw_lock_get_waiters(const rw_lock_t *lock) {
  return lock->m_waiters.load();
}

/** Sets lock->m_waiters to true. It is not an error if lock->m_waiters is already
true. 
@param[in,out] lock             Lock that has waiters. */
inline
void rw_lock_set_waiter_flag(rw_lock_t *lock) {
  bool f{};
  lock->m_waiters.compare_exchange_strong(f, true);
}

/** Resets lock->m_waiters to false. It is not an error if lock->m_waiters is already
false.
@param[in,out] lock             Lock to set. */
inline
void rw_lock_reset_waiter_flag(rw_lock_t *lock) {
  bool t{true};;
  lock->m_waiters.compare_exchange_strong(t, false);
}

/** Returns the write-status of the lock - this function made more sense
with the old rw_lock implementation.
@param[in] lock                 Lock for which we want the writer count.
@return	RW_LOCK_NOT_LOCKED, RW_LOCK_EX, RW_LOCK_WAIT_EX */
inline
ulint rw_lock_get_writer(const rw_lock_t *lock) {
  auto lock_word = lock->m_lock_word.load();

  if (lock_word > 0) {
    /* return NOT_LOCKED in s-lock state, like the writer
    member of the old lock implementation. */
    return RW_LOCK_NOT_LOCKED;
  } else if (((-lock_word) % X_LOCK_DECR) == 0) {
    return RW_LOCK_EX;
  } else {
    ut_ad(lock_word > -X_LOCK_DECR);
    return RW_LOCK_WAIT_EX;
  }
}

/** Returns the number of readers.
@param[in] lock                 Lock for which number of readers required.
@return	number of readers */
inline
ulint rw_lock_get_reader_count(const rw_lock_t *lock) {
  auto lock_word = lock->m_lock_word.load();

  if (lock_word > 0) {
    /* s-locked, no x-waiters */
    return X_LOCK_DECR - lock_word;
  } else if (lock_word < 0 && lock_word > -X_LOCK_DECR) {
    /* s-locked, with x-waiters */
    return (ulint)(-lock_word);
  } else {
    return 0;
  }
}

/** Returns the value of writer_count for the lock. Does not reserve the lock
mutex, so the caller must be sure it is not changed during the call.
@param[in] lock                 Lock for which writer count required.
@return	value of writer_count */
inline
ulint rw_lock_get_x_lock_count(const rw_lock_t *lock) {
  auto lock_copy = lock->m_lock_word.load();
  ut_ad(lock_copy <= X_LOCK_DECR);

  /* If there is a reader, m_lock_word is not divisible by X_LOCK_DECR */
  if (lock_copy > 0 || (-lock_copy) % X_LOCK_DECR != 0) {
    return 0;
  } else {
    return ((-lock_copy) / X_LOCK_DECR) + 1;
  }
}

/** Two different implementations for decrementing the m_lock_word of a rw_lock:
one for systems supporting atomic operations, one for others. This does
does not support recusive x-locks: they should be handled by the caller and
need not be atomic since they are performed by the current lock holder.
Returns true if the decrement was made, false if not.
@param[in,out] lock             Lock to decrement.
@param[in] amount               Amount to decrement.
@return	TRUE if decr occurs */
inline
bool rw_lock_lock_word_decr(rw_lock_t *lock, ulint amount) {
  auto local_lock_word = lock->m_lock_word.load();

  while (local_lock_word > 0) {
    if (lock->m_lock_word.compare_exchange_strong(local_lock_word, local_lock_word - amount)) {
      return true;
    }
    local_lock_word = lock->m_lock_word;
  }
  return false;
}

/** Increments m_lock_word the specified amount and returns new value.
@param[in,out] lock             Lock to increment.
@param[in] amount               Amount to increment
@return	lock->m_lock_word after increment */
inline
lint rw_lock_lock_word_incr(rw_lock_t *lock, ulint amount) {
  return lock->m_lock_word.fetch_add(amount) + amount;
}

/** This function sets the lock->m_writer_thread and lock->m_recursive fields.
For platforms where we are using atomic builtins instead of lock->mutex it
sets the lock->m_writer_thread field using atomics to ensure memory ordering.
Note that it is assumed that the caller of this function effectively owns
the lock i.e.: nobody else is allowed to modify lock->m_writer_thread at this
point in time. The protocol is that lock->m_writer_thread MUST be updated
BEFORE the lock->m_recursive flag is set.
@param[in,out] lock              Lock to update
@param[in] recursive             Recursion flag (true if allowed). */
inline
void rw_lock_set_writer_id_and_recursion_flag(rw_lock_t *lock, bool recursive) {
  lock->m_writer_thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
  lock->m_recursive.store(recursive, std::memory_order_release);
}

/** Low-level function which tries to lock an rw-lock in s-mode. Performs no
spinning.
@param[in] lock                 Lock instance to lock in S-mode.
@param[in] pass                 pass != 0, if the lock will be passed to another
                                thread to unlock (owner transfer).
@param[in] file_name            File name where lock requested
@param[in] line                 Line in file_name where requested
@return	true on success */
inline
bool rw_lock_s_lock_low(rw_lock_t *lock, ulint pass, const char *file_name, ulint line) {
  if (!rw_lock_lock_word_decr(lock, 1)) {
    /* Locking did not succeed */
    return false;
  } else {
#ifdef UNIV_SYNC_DEBUG
    rw_lock_add_debug_info(lock, pass, RW_LOCK_SHARED, file_name, line);
#endif /* UNIV_SYNC_DEBUG */

    /* These debugging values are not set safely: they may be incorrect
    or even refer to a line that is invalid for the file name. */
    lock->m_last_s_line = line;
    lock->m_last_s_file_name = file_name;

    return true;
  }
}

/** NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in shared mode for the current thread. If the rw-lock is locked
in exclusive mode, or there is an exclusive lock request waiting, the
function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for
the lock, before suspending the thread.
@param[in] lock                 Lock instance to lock in S-mode.
@param[in] pass                 pass != 0, if the lock will be passed to another
                                thread to unlock (owner transfer).
@param[in] file_name            File name where lock requested
@param[in] line                 Line in file_name where requested
@return	true on success */
inline
void rw_lock_s_lock_func(rw_lock_t *lock, ulint pass, const char *file_name, ulint line) {
  /* NOTE: As we do not know the thread ids for threads which have
  s-locked a latch, and s-lockers will be served only after waiting
  x-lock requests have been fulfilled, then if this thread already
  owns an s-lock here, it may end up in a deadlock with another thread
  which requests an x-lock here. Therefore, we will forbid recursive
  s-locking of a latch: the following assert will warn the programmer
  of the possibility of this kind of a deadlock. If we want to implement
  safe recursive s-locking, we should keep in a list the thread ids of
  the threads which have s-locked a latch. This would use some CPU
  time. */

#ifdef UNIV_SYNC_DEBUG
  ut_ad(!rw_lock_own(lock, RW_LOCK_SHARED)); /* see NOTE above */
#endif /* UNIV_SYNC_DEBUG */

  if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
    return; /* Success */
  } else {
    /* Did not succeed, try spin wait */
    rw_lock_s_lock_spin(lock, pass, file_name, line);
    return;
  }
}

/** NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in exclusive mode for the current thread if the lock can be
obtained immediately.
@param[in] lock                 Lock instance to lock in x-mode.
@param[in] file_name            File name where lock requested
@param[in] line                 Line in file_name where requested
@return	true if success */
inline
bool rw_lock_x_lock_func_nowait(rw_lock_t *lock, const char *file_name, ulint line) {
  int32_t x_lock_decr{X_LOCK_DECR};

  if (lock->m_lock_word.compare_exchange_strong(x_lock_decr, 0)) {
    rw_lock_set_writer_id_and_recursion_flag(lock, TRUE);
  } else if (lock->m_recursive.load(std::memory_order_acquire) && lock->m_writer_thread.load(std::memory_order_relaxed) == std::this_thread::get_id()) {
    /* Relock: this lock_word modification is safe since no other
    threads can modify (lock, unlock, or reserve) m_lock_word while
    there is an exclusive writer and this is the writer thread. */

    if (lock->m_lock_word == 0) {
      /* There is another X-LOCK. */
      lock->m_lock_word -= X_LOCK_DECR;
    } else if (lock->m_lock_word <= -X_LOCK_DECR) {
      /* THere is more than one X-LOCK. */
      --lock->m_lock_word;
    } else {
       return false;
    }

    ut_a(lock->m_lock_word < 0);

  } else {
    return false;
  }

#ifdef UNIV_SYNC_DEBUG
  rw_lock_add_debug_info(lock, 0, RW_LOCK_EX, file_name, line);
#endif /* UNIV_SYNC_DEBUG */

  lock->m_last_x_line = line;
  lock->m_last_x_file_name = file_name;

  ut_ad(rw_lock_validate(lock));

  return true;
}

/** Releases a shared mode lock.
@param[in,out] lock             Lock instance to s-unlock
@param[in] pass                 Value; != 0, if the lock may have
                                been passed to another thread to unlock */
inline
void rw_lock_s_unlock_func(
#ifdef UNIV_SYNC_DEBUG
    ulint pass,
#endif /* UNIV_SYNC_DEBUG */
    rw_lock_t *lock)
{
  ut_ad((lock->m_lock_word % X_LOCK_DECR) != 0);

#ifdef UNIV_SYNC_DEBUG
  rw_lock_remove_debug_info(lock, pass, RW_LOCK_SHARED);
#endif /* UNIV_SYNC_DEBUG */

  /* Increment lock_word to indicate 1 less reader */
  if (rw_lock_lock_word_incr(lock, 1) == 0) {

    /* wait_ex waiter exists. It may not be asleep, but we signal
    anyway. We do not wake other waiters, because they can't
    exist without wait_ex waiter and wait_ex waiter goes first.*/
    os_event_set(lock->m_wait_ex_event);
    sync_array_object_signalled(sync_primary_wait_array);
  }

  ut_ad(rw_lock_validate(lock));
}

/** Releases an exclusive mode lock.
@param[in,out] lock             Lock instance to s-unlock
@param[in] pass                 Value; != 0, if the lock may have
                                been passed to another thread to unlock */
inline
void rw_lock_x_unlock_func(
#ifdef UNIV_SYNC_DEBUG
    ulint pass,
#endif /* UNIV_SYNC_DEBUG */
    rw_lock_t *lock)
{
  ut_ad(lock->m_lock_word == 0
	|| lock->m_lock_word <= -X_LOCK_DECR);

  /* lock->m_recursive flag also indicates if lock->m_writer_thread is
  valid or stale. If we are the last of the recursive callers
  then we must unset lock->m_recursive flag to indicate that the
  lock->m_writer_thread is now stale.

  Note that since we still hold the x-lock we can safely read the lock_word. */
  if (lock->m_lock_word == 0) {
    /* Last caller in a possible recursive chain. */
    lock->m_recursive = false;
  }

#ifdef UNIV_SYNC_DEBUG
  rw_lock_remove_debug_info(lock, pass, RW_LOCK_EX);
#endif /* UNIV_SYNC_DEBUG */

  if (rw_lock_lock_word_incr(lock, X_LOCK_DECR) == X_LOCK_DECR) {
    /* Lock is now free. May have to signal read/write waiters.
    We do not need to signal wait_ex waiters, since they cannot
    exist when there is a writer. */
    if (lock->m_waiters.load()) {
      rw_lock_reset_waiter_flag(lock);
      os_event_set(lock->m_event);
      sync_array_object_signalled(sync_primary_wait_array);
    }
  }

  ut_ad(rw_lock_validate(lock));
}
