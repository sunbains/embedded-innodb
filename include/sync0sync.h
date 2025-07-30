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

/** @file include/sync0sync.h
Mutex, the basic synchronization primitive

Created 9/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "os0sync.h"
#include "os0thread.h"
#include "sync0arr.h"
#include "sync0mutex.h"
#include "ut0counter.h"
#include "ut0mem.h"

/** Initializes the synchronization data structures. */
void sync_init();

/** Frees the resources in synchronization data structures. */
void sync_close();

/** Creates, or rather, initializes a mutex object to a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */

/** Creates, or rather, initializes a mutex object in a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */
void mutex_create(mutex_t *mutex, IF_DEBUG(const char *cmutex_name, ) IF_SYNC_DEBUG(ulint level, ) Source_location loc);

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
#define mutex_enter_nowait(M) mutex_enter_nowait_func((M)IF_SYNC_DEBUG(, __FILE__, __LINE__))

/** NOTE! Use the corresponding macro in the header file, not this function
directly. Tries to lock the mutex for the current thread. If the lock is not
acquired immediately, returns with return value 1.
@return	0 if succeed, 1 if not */
ulint mutex_enter_nowait_func(mutex_t *mutex IF_SYNC_DEBUG(, const char *file_name, ulint line));

#ifdef UNIV_SYNC_DEBUG
/** Returns true if no mutex or rw-lock is currently locked.
Works only in the debug version.
@return	true if no mutexes and rw-locks reserved */
bool sync_all_freed(void);
#endif /* UNIV_SYNC_DEBUG */

/*#####################################################################
FUNCTION PROTOTYPES FOR DEBUGGING */
/** Prints wait info of the sync system. */
void sync_print_wait_info(ib_stream_t ib_stream); /** in: stream where to print */

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
  void *latch, /** in: pointer to a mutex or an rw-lock */
  ulint level
); /** in: level in the latching order; if
                  SYNC_LEVEL_VARYING, nothing is done */

/** Removes a latch from the thread level array if it is found there.
@return true if found in the array; it is no error if the latch is
not found, as we presently are not able to determine the level for
every latch reservation the program does */
bool sync_thread_reset_level(void *latch); /** in: pointer to a mutex or an rw-lock */

/** Checks that the level array for the current thread is empty.
@return	true if empty */
bool sync_thread_levels_empty(void);

/** Checks if the level array for the current thread contains a
mutex or rw-latch at the specified level.
@return	a matching latch, or nullptr if not found */
void *sync_thread_levels_contains(ulint level); /** in: latching order level
                                                (SYNC_DICT, ...)*/

/** Checks if the level array for the current thread is empty.
@return	a latch, or nullptr if empty except the exceptions specified below */
void *sync_thread_levels_nonempty_gen(bool dict_mutex_allowed); /** in: true if dictionary mutex is
                               allowed to be owned by the thread,
                               also purge_is_running mutex is
                               allowed */

#define sync_thread_levels_empty_gen(d) (!sync_thread_levels_nonempty_gen(d))

/** Gets the debug information for a reserved mutex. */
void mutex_get_debug_info(
  mutex_t *mutex,         /** in: mutex */
  const char **file_name, /** out: file where requested */
  ulint *line,            /** out: line where requested */
  os_thread_id_t *thread_id
); /** out: id of the thread which owns
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

Synchronization object			      Notes
──────────────────────────────		────────────────────────────────────

Dictionary mutex			            If we have a pointer to a dictionary
│					                        object, e.g., a table, it can be
│					                        accessed without reserving the
│					                        dictionary mutex. We must have a
│					                        reservation, a memoryfix, to the
│					                        appropriate table object in this case,
│					                        and the table must be explicitly
│					                        released later.
▼
Dictionary header                 (To be removed)
│
▼
Secondary index tree latch		    The tree latch protects also all
│					                        the B-tree non-leaf pages. These
▼					                        can be read with the page only
Secondary index non-leaf		      bufferfixed to save CPU time,
│					                        no s-latch is needed on the page.
│					                        Modification of a page requires an
│					                        x-latch on the page, however. If a
│					                        thread owns an x-latch to the tree,
│					                        it is allowed to latch non-leaf pages
│					                        even after it has acquired the fsp
│					                        latch.
▼
Secondary index leaf			        The latch on the secondary index leaf
│					                        can be kept while accessing the
│					                        clustered index, to save CPU time.
▼
Clustered index tree latch		    To increase concurrency, the tree
│					                        latch is usually released when the
│					                        leaf page latch has been acquired.
▼
Clustered index non-leaf          (Probably something Heikki had considered)
│
▼
Clustered index leaf              (Probably something Heikki had considered)
│
▼
Transaction system header         Transaction system mutex.
│
▼
Transaction undo mutex			      The undo log entry must be written
│					                        before any index page is modified.
│					                        Transaction undo mutex is for the undo
│					                        logs the analogue of the tree latch
│					                        for a B-tree. If a thread has the
│					                        trx undo mutex reserved, it is allowed
│					                        to latch the undo log pages in any
│					                        order, and also after it has acquired
│					                        the fsp latch.
▼
Rollback segment mutex			      The rollback segment mutex must be
│					                        reserved, if, e.g., a new page must
│					                        be added to an undo log. The rollback
│					                        segment and the undo logs in its
│					                        history list can be seen as an
│					                        analogue of a B-tree, and the latches
│					                        reserved similarly, using a version of
│					                        lock-coupling. If an undo log must be
│					                        extended by a page when inserting an
│					                        undo log record, this corresponds to
│					                        a pessimistic insert in a B-tree.
▼
Rollback segment header           Rollback system mutex.
│
▼
Purge system latch                Purge system mutex.
│
▼
Undo log pages				            If a thread owns the trx undo mutex,
│					                        or for a log in the history list, the
│					                        rseg mutex, it is allowed to latch
│					                        undo log pages in any order, and even
│					                        after it has acquired the fsp latch.
│					                        If a thread does not have the
│					                        appropriate mutex, it is allowed to
│					                        latch only a single undo log page in
│					                        a mini-transaction.
▼
File space management latch		    If a mini-transaction must allocate
│					                        several file pages, it can do that,
│					                        because it keeps the x-latch to the
│					                        file space management in its memo.
▼
File system pages                 FSP mutex
│
▼
Kernel mutex				              If a kernel operation needs a file
│					                        page allocation, it must reserve the
│					                        fsp x-latch before acquiring the kernel
│					                        mutex.
│
Lock mutex				                Used by the lock manager for managing
│					                        transaction locks, deadlock detection
│					                        and resolution when a transaction acquires
│					                        a lock or is waiting for a lock.
V
|
|
Trx sys mutex                     Transaction system mutex.
|
|
Trx mutex                         Transaction mutex.
|
|
▼
Search system mutex (removed)
│
▼
Buffer pool mutex
│
▼
Log mutex
│
▼
Any other latch
│
▼
Memory pool mutex */

/* Latching order levels */

/* User transaction locks are higher than any of the latch levels below:
no latches are allowed when a thread goes to wait for a normal table
or row lock! */

constexpr ulint SYNC_USER_TRX_LOCK = 9999;

/** this can be used to suppress latching order checking */
constexpr ulint SYNC_NO_ORDER_CHECK = 3000;

/** Level is varying. Only used with buffer pool page locks, which do not have
a fixed level, but instead have their level set after the page is locked;
see e.g.  ibuf_bitmap_get_map_page(). */
constexpr ulint SYNC_LEVEL_VARYING = 2000;

/* Used for trx_i_s_cache_t::rw_lock */
constexpr ulint SYNC_TRX_I_S_RWLOCK = 1910;

/** Used for trx_i_s_cache_t::last_read_mutex */
constexpr ulint SYNC_TRX_I_S_LAST_READ = 1900;

/** Used to serialize access to the file format tag */
constexpr ulint SYNC_FILE_FORMAT_TAG = 1200;

/** table create, drop, etc. reserve this in X-mode, implicit or backround
operations purge, rollback, foreign key checks reserve this in S-mode */
constexpr ulint SYNC_DICT_OPERATION = 1001;

constexpr ulint SYNC_DICT = 1000;
constexpr ulint SYNC_DICT_AUTOINC_MUTEX = 999;
constexpr ulint SYNC_DICT_HEADER = 995;

/*-------------------------------*/
constexpr ulint SYNC_INDEX_TREE = 900;
constexpr ulint SYNC_TREE_NODE_NEW = 892;
constexpr ulint SYNC_TREE_NODE_FROM_HASH = 891;
constexpr ulint SYNC_TREE_NODE = 890;
constexpr ulint SYNC_PURGE_SYS = 810;
constexpr ulint SYNC_PURGE_LATCH = 800;
constexpr ulint SYNC_TRX_UNDO = 700;
constexpr ulint SYNC_RSEG = 600;
constexpr ulint SYNC_RSEG_HEADER_NEW = 591;
constexpr ulint SYNC_RSEG_HEADER = 590;
constexpr ulint SYNC_TRX_UNDO_PAGE = 570;
constexpr ulint SYNC_EXTERN_STORAGE = 500;
constexpr ulint SYNC_FSP = 400;
constexpr ulint SYNC_FSP_PAGE = 395;

/*-------------------------------*/

constexpr ulint SYNC_TRX_SYS = 300;
constexpr ulint SYNC_LOCK = 298;
constexpr ulint SYNC_TRX = 297;
constexpr ulint SYNC_REC_LOCK = 296;
constexpr ulint SYNC_TRX_LOCK_HEAP = 295;
constexpr ulint SYNC_TRX_SYS_HEADER = 294;

/*-------------------------------*/

constexpr ulint SYNC_LOG = 170;
constexpr ulint SYNC_RECV = 168;
constexpr ulint SYNC_WORK_QUEUE = 162;

/** For assigning btr_search_enabled */
constexpr ulint SYNC_SEARCH_SYS_CONF = 161;

/* NOTE that if we have a memory heap that can be extended to the buffer pool,
its logical level is SYNC_SEARCH_SYS, as memory allocation can call routines
there! Otherwise the level is SYNC_MEM_HASH. */
constexpr ulint SYNC_SEARCH_SYS = 160;

constexpr ulint SYNC_BUF_POOL = 150;
constexpr ulint SYNC_BUF_BLOCK = 149;
constexpr ulint SYNC_PARALLEL_READ = 148;
constexpr ulint SYNC_DOUBLEWRITE = 140;
constexpr ulint SYNC_ANY_LATCH = 135;
constexpr ulint SYNC_THR_LOCAL = 133;
constexpr ulint SYNC_MEM_HASH = 131;
constexpr ulint SYNC_MEM_POOL = 130;

/* Codes used to designate lock operations */
constexpr ulint RW_LOCK_NOT_LOCKED = 350;
constexpr ulint RW_LOCK_EX = 351;
constexpr ulint RW_LOCK_EXCLUSIVE = 351;
constexpr ulint RW_LOCK_SHARED = 352;
constexpr ulint RW_LOCK_WAIT_EX = 353;
constexpr ulint SYNC_MUTEX = 354;

/* NOTE! The structure appears here only for the compiler to know its size.
Do not use its fields directly! The structure used in the spin lock
implementation of a mutual exclusion semaphore. */

/* Appears here for debugging purposes only! */

/** The global array of wait cells for implementation of the databases own
mutexes and read-write locks. */
extern Sync_check *sync_primary_wait_array;

/** Constant determining how long spin wait is continued before suspending
the thread. A value 600 rounds on a 1995 100 MHz Pentium seems to correspond
to 20 microseconds. */

#define SYNC_SPIN_ROUNDS srv_n_spin_wait_rounds

/** The number of mutex_exit calls. Intended for performance monitoring. */
using counter_t = ut::CPU_sharded_counter<8>;
extern counter_t mutex_exit_count;

#ifdef UNIV_SYNC_DEBUG
/** Latching order checks start when this is set true */
extern bool sync_order_checks_on;
#endif /* UNIV_SYNC_DEBUG */

/** This variable is set to true when sync_init is called */
extern bool sync_initialized;

/** Global list of database mutexes (not OS mutexes) created. */
using ut_mutex_list_base_node_t = UT_LIST_BASE_NODE_T(mutex_t, list);

/** Global list of database mutexes (not OS mutexes) created. */
extern ut_mutex_list_base_node_t mutex_list;

/** Mutex protecting the mutex_list variable */
extern mutex_t mutex_list_mutex;

/**
 * Sets the waiters field in a mutex.
 *
 * @param mutex - mutex
 * @param n - value to set
 */
void mutex_set_waiters(mutex_t *mutex, ulint n);

/**
 * Reserves a mutex for the current thread. If the mutex is reserved, the
 * function spins a preset time (controlled by SYNC_SPIN_ROUNDS) waiting
 * for the mutex before suspending the thread.
 *
 * @param mutex - pointer to mutex
 * @param file_name - file name where mutex requested
 * @param line - line where requested
 */
void mutex_spin_wait(mutex_t *mutex, const char *file_name, ulint line);

#ifdef UNIV_SYNC_DEBUG
/**
 * Sets the debug information for a reserved mutex.
 *
 * @param mutex - mutex
 * @param file_name - file where requested
 * @param line - line where requested
 */
void mutex_set_debug_info(mutex_t *mutex, const char *file_name, ulint line);
#endif /* UNIV_SYNC_DEBUG */

/**
 * Releases the threads waiting in the primary wait array for this mutex.
 *
 * @param mutex - mutex
 */
void mutex_signal_object(mutex_t *mutex);

/**
 * Performs an atomic test-and-set instruction to the lock_word field of a mutex.
 *
 * @param mutex - mutex
 * @return the previous value of lock_word: 0 or 1
 */
inline byte mutex_test_and_set(mutex_t *mutex) {
  return mutex->lock_word.exchange(true);
}

/** Performs a reset instruction to the lock_word field of a mutex. This
instruction also serializes memory operations to the program order. */
inline void mutex_reset_lock_word(mutex_t *mutex) /*!< in: mutex */
{

  mutex->lock_word.store(false);
}

/**
 * Gets the value of the lock word.
 *
 * @param mutex - mutex
 * @return the value of the lock word
 */
inline lock_word_t mutex_get_lock_word(const mutex_t *mutex) {
  return mutex->lock_word.load();
}

/** Gets the waiters field in a mutex.
@param[in] mutex                Get the waiters or this mutex.
@return	value to set */
inline ulint mutex_get_waiters(const mutex_t *mutex) {
  return mutex->m_waiters.load();
}

/**
 * Unlocks a mutex owned by the current thread.
 *
 * @param mutex - pointer to mutex
 */
inline void mutex_exit(mutex_t *mutex) {
  ut_ad(mutex_own(mutex));
  ut_d(mutex->thread_id = (os_thread_id_t)ULINT_UNDEFINED);

  IF_SYNC_DEBUG(sync_thread_reset_level(mutex);)

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
}

/**
 * Locks a mutex for the current thread. If the mutex is reserved, the function
 * spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for the mutex
 * before suspending the thread.
 *
 * @param mutex - pointer to mutex
 * @param file_name - file name where locked
 * @param line - line where locked
 */
inline void mutex_enter_func(mutex_t *mutex, const char *file_name, ulint line) {
  ut_ad(mutex_validate(mutex));
  ut_ad(!mutex_own(mutex));

  /* Note that we do not peek at the value of lock_word before trying
  the atomic test_and_set; we could peek, and possibly save time. */

  ut_d(mutex->count_using++);

  if (!mutex_test_and_set(mutex)) {
    ut_d(mutex->thread_id = os_thread_get_curr_id());
    IF_SYNC_DEBUG(mutex_set_debug_info(mutex, file_name, line);)

    /* Succeeded! */
    return;
  }

  mutex_spin_wait(mutex, file_name, line);
}
