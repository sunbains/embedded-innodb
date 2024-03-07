#pragma once

#include "innodb0types.h"

#include "os0sync.h"
#include "os0thread.h"
#include "ut0lst.h"

using lock_word_t = byte;

/** Value of mutex_struct::magic_n */
constexpr ulint MUTEX_MAGIC_N = 979585;

/** InnoDB mutex */
struct mutex_t {
  /** Used by sync0arr.c for the wait queue */
  os_event_t event;

  /** lock_word is the target of the atomic test-and-set
  instruction when atomic operations are enabled. */
  volatile lock_word_t lock_word;

#if !defined(HAVE_ATOMIC_BUILTINS)
  /** We use this OS mutex in place of lock_word when
  atomic operations are not enabled */
  os_fast_mutex_t os_fast_mutex;
#endif
  /** This ulint is set to 1 if there are (or may be) threads waiting in
  the global wait array for this mutex to be released. Otherwise, this is 0. */
  ulint waiters;

  /** All allocated mutexes are put into a list. Pointers to the
  next and prev. */
  UT_LIST_NODE_T(mutex_t) list;

#ifdef UNIV_SYNC_DEBUG
  /** File where the mutex was locked */
  const char *file_name;

  /** Line where the mutex was locked */
  ulint line;

  /** Level in the global latching order */
  ulint level;
#endif /* UNIV_SYNC_DEBUG */

  /** File name where mutex created */
  const char *cfile_name;

  /** Line where created */
  ulint cline;

#ifdef UNIV_DEBUG
  /** The thread id of the thread which locked the mutex. */
  os_thread_id_t thread_id;

  /** MUTEX_MAGIC_N */
  ulint magic_n;
#endif /* UNIV_DEBUG */

  /** count of os_wait */
  ulong count_os_wait;

#ifdef UNIV_DEBUG
  /** count of times mutex used */
  ulong count_using;

  /** count of spin loops */
  ulong count_spin_loop;

  /** count of spin rounds */
  ulong count_spin_rounds;

  /** count of os_wait */
  ulong count_os_yield;

  /** mutex os_wait timer msec */
  uint64_t lspent_time;

  /** mutex os_wait timer msec */
  uint64_t lmax_spent_time;

  /** mutex name */
  const char *cmutex_name;

  /** 0=usual mutex, 1=rw_lock mutex */
  ulint mutex_type;
#endif /* UNIV_DEBUG */
};
