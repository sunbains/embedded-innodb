/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file include/lock0record.h
Record lock management

*******************************************************/

#pragma once

#include "lock0types.h"

struct Buf_block;
struct Index;
struct Lock;
struct que_thr_t;
struct rec_t;

/** Safety margin when creating a new record lock: this many extra records
can be inserted to the page without need to create a lock with a bigger
bitmap */
constexpr ulint LOCK_PAGE_BITMAP_MARGIN = 64;

/**
 * @brief Record lock manager for the lock system
 * 
 * This class provides functionality to manage record-level locks including
 * lock creation, queuing, granting, and conflict detection.
 */
struct Record_lock_manager {
  /**
   * @brief Constructor for Record_lock_manager
   * 
   * @param[in] lock_sys Pointer to the main lock system
   */
  explicit Record_lock_manager(Lock_sys *lock_sys) noexcept;

  /**
   * @brief Removes a granted record lock of a transaction from the queue.
   *
   * This function removes a granted record lock of a transaction from the queue
   * and grants locks to other transactions waiting in the queue if they are now
   * entitled to a lock.
   *
   * @param[in] trx Transaction that has set a record lock.
   * @param[in] block Buffer block containing the record.
   * @param[in] rec Record for which the lock is set.
   * @param[in] lock_mode Lock mode, either LOCK_S or LOCK_X.
   */
  void unlock(Trx *trx, const Buf_block *block, const rec_t *rec, Lock_mode lock_mode) noexcept;

  /**
   * @brief Checks if there are explicit record locks on a page.
   *
   * This function determines whether there are any explicit record locks
   * present on the specified page within the given tablespace.
   *
   * @param[in] page_id The page id.
   *
   * @return true if there are explicit record locks on the page, false otherwise.
   */
  [[nodiscard]] bool expl_exist_on_page(Page_id page_id) noexcept;

  /**
   * @brief Gets the previous record lock set on a record.
   *
   * @param[in] in_lock   in: record lock
   * @param[in] heap_no   in: heap number of the record
   * @return              previous lock on the same record, nullptr if none exists
   */
  [[nodiscard]] const Lock *get_prev(const Lock *in_lock, ulint heap_no) noexcept;

  /**
   * @brief Checks if a transaction has a GRANTED explicit lock on a record that is stronger or equal to the specified mode.
   *
   * This function verifies whether the given transaction (trx) holds a granted explicit lock on the specified record
   * that is of the specified mode or stronger. The mode can be LOCK_S or LOCK_X, possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP.
   * For a supremum record, this is always regarded as a gap type request.
   *
   * @param[in] page_id The page id containing the record.
   * @param[in] precise_mode The precise lock mode to check for, which can be LOCK_S or LOCK_X, possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] heap_no The heap number of the record.
   * @param[in] trx The transaction to check for the lock.
   * 
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] const Lock *has_expl(Page_id page_id, ulint precise_mode, ulint heap_no, const Trx *trx) const noexcept;

  /**
   * @brief Checks if some other transaction has a conflicting explicit lock request in the queue.
   *
   * This function determines whether any other transaction has a conflicting explicit lock request
   * on the specified record in the queue, such that the current transaction has to wait.
   *
   * @param[in] page_id  The page id containing the record.
   * @param[in] mode     The lock mode to check for, either LOCK_S or LOCK_X, possibly ORed with LOCK_GAP, LOCK_REC_NOT_GAP, or LOCK_INSERT_INTENTION.
   * @param[in] heap_no  The heap number of the record.
   * @param[in] trx      The transaction to check against.
   * 
   * @return A pointer to the conflicting lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] Lock *other_has_conflicting(Page_id page_id, Lock_mode mode, ulint heap_no, Trx *trx) noexcept;

  /**
   * @brief Finds a similar record lock on the same page for the same transaction.
   *
   * This function searches for a record lock of the same type and transaction on the same page.
   * It can be used to save space when a new record lock should be set on a page, as no new struct
   * is needed if a suitable existing one is found.
   *
   * @param[in] type_mode The type_mode field of the lock.
   * @param[in] heap_no The gap number of the record.
   * @param[in] lock The first lock on the page.
   * @param[in] trx The transaction to check for.
   * 
   * @return A pointer to the similar lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] Lock *find_similar_on_page(Lock_mode type_mode, ulint heap_no, Lock *lock, const Trx *trx) noexcept;

  /**
   * @brief Creates a new record lock and inserts it into the lock queue.
   *
   * This function creates a new record lock for the specified transaction and 
   * inserts it into the lock queue. The lock mode and wait flag are specified 
   * by the type_mode parameter, but the type is ignored and replaced by LOCK_REC.
   * It does not perform any checks for deadlocks or lock compatibility.
   *
   * @param[in] page_id Page ID.
   * @param[in] type_mode Lock mode and wait flag, type is ignored and replaced by LOCK_REC.
   * @param[in] heap_no Heap number in the page.
   * @param[in] n_bits Number of bits in the lock bitmap.
   * @param[in] index Index of the record.
   * @param[in,out] trx Transaction that wants to create the record lock.
   * 
   * @return A pointer to the created lock.
   */
  [[nodiscard]] Lock *create_lock_low(
    Page_id page_id, Lock_mode type_mode, ulint heap_no, ulint n_bits, const Index *index, Trx *trx
  ) noexcept;

  /**
   * @brief Creates a new record lock and inserts it into the lock queue.
   *
   * This function creates a new record lock for the specified transaction and 
   * inserts it into the lock queue. The lock mode and wait flag are specified 
   * by the type_mode parameter, but the type is ignored and replaced by LOCK_REC.
   *
   * @param[in] type_mode Lock mode and wait flag, type is ignored and replaced by LOCK_REC.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] trx Transaction requesting the lock.
   * 
   * @return The created lock.
   */
  [[nodiscard]] Lock *create_lock(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
  ) noexcept;

  /**
   * @brief Adds a record lock request in the record queue.
   *
   * The request is normally added as the last in the queue, but if there are no waiting lock requests
   * on the record, and the request to be added is not a waiting request, we can reuse a suitable 
   * record lock object already existing on the same page, just setting the appropriate bit in its bitmap.
   * This is a low-level function which does NOT check for deadlocks or lock compatibility!
   *
   * @param[in] type_mode Lock mode, wait, gap etc. flags; type is ignored and replaced by LOCK_REC.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] trx Transaction.
   * 
   * @return Lock where the bit was set.
   */
  [[nodiscard]] Lock *add_to_queue(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
  ) noexcept;

  /**
   * @brief Fast routine for locking a record in the most common cases.
   *
   * This function handles locking a record when there are no explicit locks on the page,
   * or there is just one lock, owned by this transaction, and of the right type_mode.
   * It is a low-level function that does NOT consider implicit locks and checks lock
   * compatibility within explicit locks. The function sets a normal next-key lock, or
   * in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl  If true, no lock is set if no wait is necessary. It is assumed that
   *                  the caller will set an implicit lock.
   * @param[in] mode  Lock mode: LOCK_X or LOCK_S possibly ORed with either LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] thr   Query thread.
   * 
   * @return true if locking succeeded.
   */
  [[nodiscard]] bool lock_fast(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief General, slower routine for locking a record.
   *
   * This is a low-level function which does NOT look at implicit locks. It checks 
   * lock compatibility within explicit locks and sets a normal next-key lock, or 
   * in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl   If true, no lock is set if no wait is necessary. It is assumed 
   *                   that the caller will set an implicit lock.
   * @param[in] mode   Lock mode: LOCK_X or LOCK_S possibly ORed with either LOCK_GAP 
   *                   or LOCK_REC_NOT_GAP.
   * @param[in] block  Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index  Index of the record.
   * @param[in] thr    Query thread.
   * 
   * @return DB_SUCCESS, DB_LOCK_WAIT, or an error code.
   */
  [[nodiscard]] db_err lock_slow(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Tries to lock the specified record in the mode requested.
   *
   * If not immediately possible, enqueues a waiting lock request. This is a 
   * low-level function which does NOT look at implicit locks! Checks lock 
   * compatibility within explicit locks. This function sets a normal next-key 
   * lock, or in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl   If true, no lock is set if no wait is necessary:
   *                   we assume that the caller will set an implicit lock.
   * @param[in] mode   Lock mode: LOCK_X or LOCK_S possibly ORed to either
   *                   LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] block  Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index  Index of the record.
   * @param[in] thr    Query thread.
   * 
   * @return DB_SUCCESS, DB_LOCK_WAIT, or an error code.
   */
  [[nodiscard]] db_err lock_record(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Enqueues a waiting request for a lock which cannot be granted immediately and checks for deadlocks.
   *
   * This function enqueues a lock request that cannot be granted immediately and checks for deadlocks.
   * If a deadlock is detected, the lock request is removed and an error code is returned.
   *
   * @param[in] type_mode Lock mode this transaction is requesting: LOCK_S or LOCK_X,
   *                      possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP, ORed with LOCK_INSERT_INTENTION
   *                      if this waiting lock request is set when performing an insert of an index record.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] thr Query thread.
   * 
   * @return DB_LOCK_WAIT if the lock request is enqueued and waiting.
   * @return DB_DEADLOCK if a deadlock is detected and the lock request is removed.
   * @return DB_QUE_THR_SUSPENDED if the query thread should be stopped.
   * @return DB_SUCCESS if there was a deadlock but another transaction was chosen as a victim, and the lock was granted immediately.
   */
  [[nodiscard]] db_err enqueue_waiting(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if a waiting record lock request still has to wait in a queue.
   *
   * This function determines whether a waiting record lock request still needs 
   * to wait in the lock queue. It iterates through the locks on the same page 
   * and checks if any of them require the waiting lock to wait.
   *
   * @param[in] rec_locks The record locks to check.
   * @param[in] wait_lock The lock request that is waiting.
   * @param[in] heap_no The heap number of the record.
   * 
   * @return true if the lock request still has to wait, false otherwise.
   */
  [[nodiscard]] bool has_to_wait_in_queue(const Rec_locks &rec_locks, Lock *wait_lock, ulint heap_no) const noexcept;

  /**
   * @brief Grants a lock to a waiting lock request and releases the waiting transaction.
   *
   * This function grants a lock to a waiting lock request and releases the 
   * transaction that was waiting for the lock. It resets the wait flag and 
   * the back pointer in the transaction to indicate that the lock wait has ended.
   *
   * @param[in] lock The lock request to be granted.
   */
  void grant(Lock *lock) noexcept;

  /**
   * @brief Cancels a waiting record lock request and releases the waiting transaction.
   *
   * This function cancels a waiting record lock request and releases the transaction
   * that requested it. It does not check if waiting lock requests behind this one 
   * can now be granted.
   *
   * @param[in] lock The lock request to be canceled.
   */
  void cancel(Lock *lock) noexcept;

  /**
   * @brief Removes a record lock request from the queue and grants locks to other transactions.
   *
   * This function removes a record lock request, whether waiting or granted, from the queue.
   * It also checks if other transactions waiting behind this lock request are now entitled 
   * to a lock and grants it to them if they qualify. Note that all record locks contained 
   * in the provided lock object are removed.
   *
   * @param[in] in_lock The record lock object. All record locks contained in this lock object 
   *                    are removed. Transactions waiting behind will get their lock requests 
   *                    granted if they are now qualified.
   */
  void dequeue_from_page(Lock *in_lock) noexcept;

  /**
   * @brief Removes a record lock request, waiting or granted, from the queue.
   *
   * This function removes a record lock request from the queue, whether it is 
   * currently waiting or has already been granted. All record locks contained 
   * in the provided lock object are removed.
   *
   * @param[in,out] in_lock The record lock object. All record locks contained 
   *                        in this lock object are removed.
   */
  void discard(Lock *in_lock) noexcept;

  /**
   * @brief Removes record lock objects set on an index page which is discarded.
   *
   * This function removes all record lock objects associated with an index page
   * that is being discarded. It does not move locks or check for waiting locks,
   * so the lock bitmaps must already be reset when this function is called.
   *
   * @param[in] page_id The page id to be discarded.
   */
  void free_all_from_discard_page(Page_id page_id) noexcept;

  /**
   * @brief Resets the lock bits for a single record and releases transactions waiting for lock requests.
   *
   * This function resets the lock bits for a specified record within a block and releases any transactions
   * that are waiting for lock requests on that record.
   *
   * @param[in] page_id  The page id containing the record.
   * @param[in] heap_no  The heap number of the record.
   */
  void reset_and_release_wait(Page_id page_id, ulint heap_no) noexcept;

#ifdef UNIV_DEBUG
  /**
   * @brief Gets the first explicit lock request on a record.
   *
   * This function retrieves the first explicit lock request on a record
   * identified by the page id and the heap number.  It iterates through
   * the locks and returns the first lock that matches the specified heap number.
   *
   * @param[in] page_id The page id.
   * @param[in] heap_no The heap number of the record.
   * 
   * @return The first lock on the specified record, or nullptr if none exists.
   */
  [[nodiscard]] Lock *get_first(Page_id page_id, ulint heap_no) noexcept;
#endif /* UNIV_DEBUG */

 private:
  /** Pointer to the main lock system */
  Lock_sys *m_lock_sys;
};
