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

/** @file include/lock0deadlock.h
Deadlock detection and resolution

*******************************************************/

#pragma once

#include <stack>
#include <vector>
#include "lock0types.h"

struct Lock;
struct Trx;
struct Lock_sys;

/** Restricts the length of search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK = 1000000;

/** Restricts the recursion depth of the search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK = 200;

/** Deadlock check return values */
constexpr ulint LOCK_VICTIM_IS_START = 1;
constexpr ulint LOCK_VICTIM_IS_OTHER = 2;
constexpr ulint LOCK_EXCEED_MAX_DEPTH = 3;

/**
 * @brief Deadlock detector for the lock system
 *
 * This class provides functionality to detect deadlocks in the transaction
 * lock wait graph and resolve them by choosing victim transactions.
 */
struct Deadlock_detector {
  /**
   * @brief Constructor for Deadlock_detector
   *
   * @param[in] lock_sys Pointer to the main lock system
   */
  explicit Deadlock_detector(Lock_sys *lock_sys) noexcept;

  /**
   * @brief Checks if a lock request results in a deadlock.
   *
   * This function determines whether a lock request by a transaction
   * results in a deadlock situation. If a deadlock is detected, it
   * decides whether the requesting transaction should be chosen as
   * the victim to resolve the deadlock.
   *
   * @param[in] lock Lock transaction is requesting.
   * @param[in] trx Transaction requesting the lock.
   *
   * @return true if a deadlock was detected and the requesting transaction
   *         was chosen as the victim; false if no deadlock was detected, or
   *         if a deadlock was detected but other transaction(s) were chosen
   *         as victim(s).
   */
  [[nodiscard]] bool check_deadlock(Lock *lock, Trx *trx) noexcept;

  /**
   * @brief Looks iteratively for a deadlock using an explicit stack.
   *
   * This function performs an iterative search to detect deadlocks in the lock system.
   * It checks if the transaction `trx` waiting for the lock `wait_lock` causes a deadlock
   * starting from the transaction `start`. Uses an explicit stack instead of recursion
   * to avoid stack overflow and improve performance.
   *
   * @param[in] start The search starting point.
   * @param[in] trx A transaction waiting for a lock.
   * @param[in] wait_lock The lock that is waiting to be granted.
   * @param[in,out] cost The number of calculation steps thus far. If this exceeds
   *                     LOCK_MAX_N_STEPS_..., the function returns LOCK_EXCEED_MAX_DEPTH.
   * @param[in] depth The initial depth. If this exceeds LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK,
   *                  the function returns LOCK_EXCEED_MAX_DEPTH.
   *
   * @return 0 if no deadlock is found.
   * @return LOCK_VICTIM_IS_START if a deadlock is found and 'start' is chosen as the victim.
   * @return LOCK_VICTIM_IS_OTHER if a deadlock is found and another transaction is chosen as the victim.
   *         In this case, the search must be repeated as there may be another deadlock.
   * @return LOCK_EXCEED_MAX_DEPTH if the lock search exceeds the maximum steps or depth.
   */
  [[nodiscard]] ulint search_iterative(Trx *start, Trx *trx, Lock *wait_lock, ulint *cost, ulint depth) noexcept;

 private:
  /**
   * @brief Stack frame for iterative deadlock detection.
   *
   * This structure holds the state needed for each step in the iterative
   * deadlock detection algorithm.
   */
  struct Search_frame {
    Trx *trx;                /**< Current transaction being examined */
    Lock *wait_lock;         /**< Lock that the transaction is waiting for */
    ulint depth;             /**< Current depth in the search */
    ulint heap_no;           /**< Heap number for record locks */
    Lock *found_lock;        /**< Current lock being examined in the queue */
    bool is_first_iteration; /**< Whether this is the first iteration for this frame */

    Search_frame(Trx *t, Lock *wl, ulint d, ulint hn, Lock *fl, bool first = true)
        : trx(t), wait_lock(wl), depth(d), heap_no(hn), found_lock(fl), is_first_iteration(first) {}
  };

  /**
   * @brief Chooses a victim transaction from a deadlock cycle.
   *
   * This function selects which transaction should be chosen as the victim
   * to resolve a deadlock. The decision is based on factors like transaction
   * age, number of locks held, etc.
   *
   * @param[in] trx1 First transaction in the deadlock cycle.
   * @param[in] trx2 Second transaction in the deadlock cycle.
   *
   * @return The transaction that should be chosen as the victim.
   */
  [[nodiscard]] Trx *choose_victim(Trx *trx1, Trx *trx2) noexcept;

  /**
   * @brief Reports a deadlock to the error log and latest deadlock buffer.
   *
   * This function logs information about the detected deadlock including
   * the transactions involved and their waiting locks.
   *
   * @param[in] victim The transaction chosen as the victim.
   * @param[in] wait_lock The lock that caused the deadlock detection.
   */
  void report_deadlock(Trx *victim, Lock *wait_lock) noexcept;

  /** Pointer to the main lock system */
  Lock_sys *m_lock_sys;

  /** Stack for iterative deadlock detection */
  std::stack<Search_frame> m_search_stack;
};
