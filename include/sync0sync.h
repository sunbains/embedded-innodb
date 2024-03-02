/****************************************************************************
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

/** @file include/sync0sync.h
Mutex, the basic synchronization primitive

Created 9/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "os0sync.h"
#include "os0thread.h"
#include "sync0arr.h"
#include "sync0types.h"
#include "ut0lst.h"
#include "ut0mem.h"

using lock_word_t = byte;

/** Initializes the synchronization data structures. */
void sync_init();
/** Frees the resources in synchronization data structures. */

void sync_close(void);
/** Creates, or rather, initializes a mutex object to a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */

#ifdef UNIV_DEBUG
#ifdef UNIV_SYNC_DEBUG
#define mutex_create(M, level)
  mutex_create_func((M), #M, (level), __FILE__, __LINE__)
#else
#define mutex_create(M, level) mutex_create_func((M), #M, __FILE__, __LINE__)
#endif
#else
#define mutex_create(M, level) mutex_create_func((M), __FILE__, __LINE__)
#endif

/** Creates, or rather, initializes a mutex object in a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */
void mutex_create_func(
    mutex_t *mutex, /** in: pointer to memory */
#ifdef UNIV_DEBUG
    const char *cmutex_name, /** in: mutex name */
#ifdef UNIV_SYNC_DEBUG
    ulint level,            /** in: level */
#endif                      /* UNIV_SYNC_DEBUG */
#endif                      /* UNIV_DEBUG */
    const char *cfile_name, /** in: file name where created */
    ulint cline);           /** in: file line where created */

#undef mutex_free /* Fix for MacOS X */

/** Calling this function is obligatory only if the memory buffer containing
the mutex is freed. Removes a mutex object from the mutex list. The mutex
is checked to be in the reset state. */
void mutex_free(mutex_t *mutex); /** in: mutex */

/** NOTE! The following macro should be used in mutex locking, not the
corresponding function. */
#define mutex_enter(M) mutex_enter_func((M), __FILE__, __LINE__)

/** NOTE! The following macro should be used in mutex locking, not the
corresponding function. */

/* NOTE! currently same as mutex_enter! */

#define mutex_enter_fast(M) mutex_enter_func((M), __FILE__, __LINE__)

/** NOTE! The following macro should be used in mutex locking, not the
corresponding function. */
#define mutex_enter_nowait(M) mutex_enter_nowait_func((M), __FILE__, __LINE__)

/** NOTE! Use the corresponding macro in the header file, not this function
directly. Tries to lock the mutex for the current thread. If the lock is not
acquired immediately, returns with return value 1.
@return	0 if succeed, 1 if not */
ulint mutex_enter_nowait_func(mutex_t *mutex, /** in: pointer to mutex */
                              const char *file_name, /** in: file name where
                                                     mutex requested */
                              ulint line); /** in: line where requested */

#ifdef UNIV_SYNC_DEBUG
/** Returns true if no mutex or rw-lock is currently locked.
Works only in the debug version.
@return	true if no mutexes and rw-locks reserved */
bool sync_all_freed(void);
#endif /* UNIV_SYNC_DEBUG */

/*#####################################################################
FUNCTION PROTOTYPES FOR DEBUGGING */
/** Prints wait info of the sync system. */
void sync_print_wait_info(
    ib_stream_t ib_stream); /** in: stream where to print */

/** Prints info of the sync system. */
void sync_print(ib_stream_t ib_stream); /** in: stream where to print */

#ifdef UNIV_DEBUG
/** Checks that the mutex has been initialized.
@return	true */
bool mutex_validate(const mutex_t *mutex); /** in: mutex */

/** Checks that the current thread owns the mutex. Works only
in the debug version.
@return	true if owns */
bool mutex_own(const mutex_t *mutex) /** in: mutex */
    __attribute__((warn_unused_result));

#endif /* UNIV_DEBUG */
#ifdef UNIV_SYNC_DEBUG

/** Adds a latch and its level in the thread level array. Allocates the memory
for the array if called first time for this OS thread. Makes the checks
against other latch levels stored in the array for this thread. */
void sync_thread_add_level(
    void *latch,  /** in: pointer to a mutex or an rw-lock */
    ulint level); /** in: level in the latching order; if
                  SYNC_LEVEL_VARYING, nothing is done */

/** Removes a latch from the thread level array if it is found there.
@return true if found in the array; it is no error if the latch is
not found, as we presently are not able to determine the level for
every latch reservation the program does */
bool sync_thread_reset_level(
    void *latch); /** in: pointer to a mutex or an rw-lock */

/** Checks that the level array for the current thread is empty.
@return	true if empty */
bool sync_thread_levels_empty(void);

/** Checks if the level array for the current thread contains a
mutex or rw-latch at the specified level.
@return	a matching latch, or NULL if not found */
void *sync_thread_levels_contains(ulint level); /** in: latching order level
                                                (SYNC_DICT, ...)*/

/** Checks if the level array for the current thread is empty.
@return	a latch, or NULL if empty except the exceptions specified below */
void *sync_thread_levels_nonempty_gen(
    bool dict_mutex_allowed); /** in: true if dictionary mutex is
                               allowed to be owned by the thread,
                               also purge_is_running mutex is
                               allowed */

#define sync_thread_levels_empty_gen(d) (!sync_thread_levels_nonempty_gen(d))

/** Gets the debug information for a reserved mutex. */
void mutex_get_debug_info(
    mutex_t *mutex,             /** in: mutex */
    const char **file_name,     /** out: file where requested */
    ulint *line,                /** out: line where requested */
    os_thread_id_t *thread_id); /** out: id of the thread which owns
                                the mutex */

/** Counts currently reserved mutexes. Works only in the debug version.
@return	number of reserved mutexes */
ulint mutex_n_reserved(void);

#endif /* UNIV_SYNC_DEBUG */

/** Reset variables. */
void sync_var_init();

/*
                LATCHING ORDER WITHIN THE DATABASE
                ==================================

The mutex or latch in the central memory object, for instance, a rollback
segment object, must be acquired before acquiring the latch or latches to
the corresponding file data structure. In the latching order below, these
file page object latches are placed immediately below the corresponding
central memory object latch or mutex.

Synchronization object			Notes
----------------------			-----

Dictionary mutex			If we have a pointer to a dictionary
|					object, e.g., a table, it can be
|					accessed without reserving the
|					dictionary mutex. We must have a
|					reservation, a memoryfix, to the
|					appropriate table object in this case,
|					and the table must be explicitly
|					released later.
V
Dictionary header
|
V
Secondary index tree latch		The tree latch protects also all
|					the B-tree non-leaf pages. These
V					can be read with the page only
Secondary index non-leaf		bufferfixed to save CPU time,
|					no s-latch is needed on the page.
|					Modification of a page requires an
|					x-latch on the page, however. If a
|					thread owns an x-latch to the tree,
|					it is allowed to latch non-leaf pages
|					even after it has acquired the fsp
|					latch.
V
Secondary index leaf			The latch on the secondary index leaf
|					can be kept while accessing the
|					clustered index, to save CPU time.
V
Clustered index tree latch		To increase concurrency, the tree
|					latch is usually released when the
|					leaf page latch has been acquired.
V
Clustered index non-leaf
|
V
Clustered index leaf
|
V
Transaction system header
|
V
Transaction undo mutex			The undo log entry must be written
|					before any index page is modified.
|					Transaction undo mutex is for the undo
|					logs the analogue of the tree latch
|					for a B-tree. If a thread has the
|					trx undo mutex reserved, it is allowed
|					to latch the undo log pages in any
|					order, and also after it has acquired
|					the fsp latch.
V
Rollback segment mutex			The rollback segment mutex must be
|					reserved, if, e.g., a new page must
|					be added to an undo log. The rollback
|					segment and the undo logs in its
|					history list can be seen as an
|					analogue of a B-tree, and the latches
|					reserved similarly, using a version of
|					lock-coupling. If an undo log must be
|					extended by a page when inserting an
|					undo log record, this corresponds to
|					a pessimistic insert in a B-tree.
V
Rollback segment header
|
V
Purge system latch
|
V
Undo log pages				If a thread owns the trx undo mutex,
|					or for a log in the history list, the
|					rseg mutex, it is allowed to latch
|					undo log pages in any order, and even
|					after it has acquired the fsp latch.
|					If a thread does not have the
|					appropriate mutex, it is allowed to
|					latch only a single undo log page in
|					a mini-transaction.
V
File space management latch		If a mini-transaction must allocate
|					several file pages, it can do that,
|					because it keeps the x-latch to the
|					file space management in its memo.
V
File system pages
|
V
Kernel mutex				If a kernel operation needs a file
|					page allocation, it must reserve the
|					fsp x-latch before acquiring the kernel
|					mutex.
V
Search system mutex
|
V
Buffer pool mutex
|
V
Log mutex
|
Any other latch
|
V
Memory pool mutex */

/* Latching order levels */

/* User transaction locks are higher than any of the latch levels below:
no latches are allowed when a thread goes to wait for a normal table
or row lock! */

constexpr ulint SYNC_USER_TRX_LOCK =  9999;

/** this can be used to suppress latching order checking */
constexpr ulint SYNC_NO_ORDER_CHECK =  3000;

/** Level is varying. Only used with buffer pool page locks, which do not have
a fixed level, but instead have their level set after the page is locked;
see e.g.  ibuf_bitmap_get_map_page(). */
constexpr ulint SYNC_LEVEL_VARYING =  2000;

/* Used for trx_i_s_cache_t::rw_lock */
constexpr ulint SYNC_TRX_I_S_RWLOCK =  1910;

/** Used for trx_i_s_cache_t::last_read_mutex */
constexpr ulint SYNC_TRX_I_S_LAST_READ =  1900;

/** Used to serialize access to the file format tag */
constexpr ulint SYNC_FILE_FORMAT_TAG =  1200;

/** table create, drop, etc. reserve this in X-mode, implicit or backround
operations purge, rollback, foreign key checks reserve this in S-mode */
constexpr ulint SYNC_DICT_OPERATION =  1001;

constexpr ulint SYNC_DICT =  1000;
constexpr ulint SYNC_DICT_AUTOINC_MUTEX =  999;
constexpr ulint SYNC_DICT_HEADER =  995;
constexpr ulint SYNC_IBUF_HEADER =  914;
constexpr ulint SYNC_IBUF_PESS_INSERT_MUTEX =  912;

/** ibuf mutex is really below SYNC_FSP_PAGE: we assign a value this high
only to make the program to pass the debug checks */
constexpr ulint SYNC_IBUF_MUTEX =  910;

/*-------------------------------*/
constexpr ulint SYNC_INDEX_TREE =  900;
constexpr ulint SYNC_TREE_NODE_NEW =  892;
constexpr ulint SYNC_TREE_NODE_FROM_HASH =  891;
constexpr ulint SYNC_TREE_NODE =  890;
constexpr ulint SYNC_PURGE_SYS =  810;
constexpr ulint SYNC_PURGE_LATCH =  800;
constexpr ulint SYNC_TRX_UNDO =  700;
constexpr ulint SYNC_RSEG =  600;
constexpr ulint SYNC_RSEG_HEADER_NEW =  591;
constexpr ulint SYNC_RSEG_HEADER =  590;
constexpr ulint SYNC_TRX_UNDO_PAGE =  570;
constexpr ulint SYNC_EXTERN_STORAGE =  500;
constexpr ulint SYNC_FSP =  400;
constexpr ulint SYNC_FSP_PAGE =  395;

/*------------------------------------- Insert buffer headers */

/*------------------------------------- ibuf_mutex */

/*------------------------------------- Insert buffer tree */

constexpr ulint SYNC_IBUF_BITMAP_MUTEX =  351;
constexpr ulint SYNC_IBUF_BITMAP =  350;

/*-------------------------------*/

constexpr ulint SYNC_KERNEL =  300;
constexpr ulint SYNC_REC_LOCK =  299;
constexpr ulint SYNC_TRX_LOCK_HEAP =  298;
constexpr ulint SYNC_TRX_SYS_HEADER =  290;
constexpr ulint SYNC_LOG =  170;
constexpr ulint SYNC_RECV =  168;
constexpr ulint SYNC_WORK_QUEUE =  162;

/** For assigning btr_search_enabled */
constexpr ulint SYNC_SEARCH_SYS_CONF =  161;

/* NOTE that if we have a memory heap that can be extended to the buffer pool,
its logical level is SYNC_SEARCH_SYS, as memory allocation can call routines
there! Otherwise the level is SYNC_MEM_HASH. */
constexpr ulint SYNC_SEARCH_SYS =  160;

constexpr ulint SYNC_BUF_POOL =  150;
constexpr ulint SYNC_BUF_BLOCK =  149;
constexpr ulint SYNC_DOUBLEWRITE =  140;
constexpr ulint SYNC_ANY_LATCH =  135;
constexpr ulint SYNC_THR_LOCAL =  133;
constexpr ulint SYNC_MEM_HASH =  131;
constexpr ulint SYNC_MEM_POOL =  130;

/* Codes used to designate lock operations */
constexpr ulint RW_LOCK_NOT_LOCKED =  350;
constexpr ulint RW_LOCK_EX =  351;
constexpr ulint RW_LOCK_EXCLUSIVE =  351;
constexpr ulint RW_LOCK_SHARED =  352;
constexpr ulint RW_LOCK_WAIT_EX =  353;
constexpr ulint SYNC_MUTEX =  354;

/* NOTE! The structure appears here only for the compiler to know its size.
Do not use its fields directly! The structure used in the spin lock
implementation of a mutual exclusion semaphore. */

/** Value of mutex_struct::magic_n */
constexpr ulint MUTEX_MAGIC_N = 979585;

/** InnoDB mutex */
struct mutex_struct {
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
#endif  /* UNIV_DEBUG */
};

/* Appears here for debugging purposes only! */

/** The global array of wait cells for implementation of the databases own
mutexes and read-write locks. */
extern sync_array_t *sync_primary_wait_array;

/** Constant determining how long spin wait is continued before suspending
the thread. A value 600 rounds on a 1995 100 MHz Pentium seems to correspond
to 20 microseconds. */

#define SYNC_SPIN_ROUNDS srv_n_spin_wait_rounds

/** The number of mutex_exit calls. Intended for performance monitoring. */
extern int64_t mutex_exit_count;

#ifdef UNIV_SYNC_DEBUG
/** Latching order checks start when this is set true */
extern bool sync_order_checks_on;
#endif /* UNIV_SYNC_DEBUG */

/** This variable is set to true when sync_init is called */
extern bool sync_initialized;

/** Global list of database mutexes (not OS mutexes) created. */
typedef UT_LIST_BASE_NODE_T(mutex_t) ut_list_base_node_t;

/** Global list of database mutexes (not OS mutexes) created. */
extern ut_list_base_node_t mutex_list;

/** Mutex protecting the mutex_list variable */
extern mutex_t mutex_list_mutex;

/** Sets the waiters field in a mutex. */
void mutex_set_waiters(mutex_t *mutex, /*!< in: mutex */
                       ulint n);       /*!< in: value to set */

/** Reserves a mutex for the current thread. If the mutex is reserved, the
function spins a preset time (controlled by SYNC_SPIN_ROUNDS) waiting
for the mutex before suspending the thread. */
void mutex_spin_wait(mutex_t *mutex,        /*!< in: pointer to mutex */
                     const char *file_name, /*!< in: file name where mutex
                                            requested */
                     ulint line);           /*!< in: line where requested */

#ifdef UNIV_SYNC_DEBUG
/** Sets the debug information for a reserved mutex. */
void mutex_set_debug_info(
    mutex_t *mutex,        /*!< in: mutex */
    const char *file_name, /*!< in: file where requested */
    ulint line);           /*!< in: line where requested */
#endif                     /* UNIV_SYNC_DEBUG */

/** Releases the threads waiting in the primary wait array for this mutex. */
void mutex_signal_object(mutex_t *mutex); /*!< in: mutex */

/** Performs an atomic test-and-set instruction to the lock_word field of a
mutex.
@return	the previous value of lock_word: 0 or 1 */
inline byte mutex_test_and_set(mutex_t *mutex) /*!< in: mutex */
{
#if defined(HAVE_ATOMIC_BUILTINS)
  return (os_atomic_test_and_set_byte(&mutex->lock_word, 1));
#else
  bool ret;

  ret = os_fast_mutex_trylock(&(mutex->os_fast_mutex));

  if (ret == 0) {
    /* We check that os_fast_mutex_trylock does not leak
    and allow race conditions */
    ut_a(mutex->lock_word == 0);

    mutex->lock_word = 1;
  }

  return ((byte)ret);
#endif
}

/** Performs a reset instruction to the lock_word field of a mutex. This
instruction also serializes memory operations to the program order. */
inline void mutex_reset_lock_word(mutex_t *mutex) /*!< in: mutex */
{
#if defined(HAVE_ATOMIC_BUILTINS)
  /* In theory __sync_lock_release should be used to release the lock.
  Unfortunately, it does not work properly alone. The workaround is
  that more conservative __sync_lock_test_and_set is used instead. */
  os_atomic_test_and_set_byte(&mutex->lock_word, 0);
#else
  mutex->lock_word = 0;

  os_fast_mutex_unlock(&(mutex->os_fast_mutex));
#endif
}

/** Gets the value of the lock word. */
inline lock_word_t mutex_get_lock_word(const mutex_t *mutex) /*!< in: mutex */
{
  ut_ad(mutex);

  return (mutex->lock_word);
}

/** Gets the waiters field in a mutex.
@return	value to set */
inline ulint mutex_get_waiters(const mutex_t *mutex) /*!< in: mutex */
{
  const volatile ulint *ptr; /*!< declared volatile to ensure that
                             the value is read from memory */
  ut_ad(mutex);

  ptr = &(mutex->waiters);

  return (*ptr); /* Here we assume that the read of a single
                 word from memory is atomic */
}

/** Unlocks a mutex owned by the current thread. */
inline void mutex_exit(mutex_t *mutex) /*!< in: pointer to mutex */
{
  ut_ad(mutex_own(mutex));

  ut_d(mutex->thread_id = (os_thread_id_t)ULINT_UNDEFINED);

#ifdef UNIV_SYNC_DEBUG
  sync_thread_reset_level(mutex);
#endif
  mutex_reset_lock_word(mutex);

  /* A problem: we assume that mutex_reset_lock word
  is a memory barrier, that is when we read the waiters
  field next, the read must be serialized in memory
  after the reset. A speculative processor might
  perform the read first, which could leave a waiting
  thread hanging indefinitely.

  Our current solution call every second
  sync_arr_wake_threads_if_sema_free()
  to wake up possible hanging threads if
  they are missed in mutex_signal_object. */

  if (mutex_get_waiters(mutex) != 0) {

    mutex_signal_object(mutex);
  }

#ifdef UNIV_SYNC_PERF_STAT
  mutex_exit_count++;
#endif
}

/** Locks a mutex for the current thread. If the mutex is reserved, the function
spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for the mutex
before suspending the thread. */
inline void
mutex_enter_func(mutex_t *mutex,        /*!< in: pointer to mutex */
                 const char *file_name, /*!< in: file name where locked */
                 ulint line)            /*!< in: line where locked */
{
  ut_ad(mutex_validate(mutex));
  ut_ad(!mutex_own(mutex));

  /* Note that we do not peek at the value of lock_word before trying
  the atomic test_and_set; we could peek, and possibly save time. */

  ut_d(mutex->count_using++);

  if (!mutex_test_and_set(mutex)) {
    ut_d(mutex->thread_id = os_thread_get_curr_id());
#ifdef UNIV_SYNC_DEBUG
    mutex_set_debug_info(mutex, file_name, line);
#endif
    return; /* Succeeded! */
  }

  mutex_spin_wait(mutex, file_name, line);
}
