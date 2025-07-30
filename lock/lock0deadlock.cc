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

/** @file lock/lock0deadlock.cc
Deadlock detection and resolution

*******************************************************/

#include "lock0deadlock.h"

#include "lock0lock.h"
#include "srv0srv.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0dbg.h"
#include "ut0logger.h"

Deadlock_detector::Deadlock_detector(Lock_sys *lock_sys) noexcept : m_lock_sys(lock_sys) {}

bool Deadlock_detector::check_deadlock(Lock *lock, Trx *trx) noexcept {
  ulint ret;
  ulint cost{};

  ut_ad(mutex_own(&m_lock_sys->m_trx_sys->m_mutex));

retry:
  /* We check that adding this trx to the waits-for graph
  does not produce a cycle. First mark all active transactions
  with 0: */

  for (auto mark_trx : m_lock_sys->m_trx_sys->m_trx_list) {
    mark_trx->m_deadlock_mark = 0;
  }

  ret = search_iterative(trx, trx, lock, &cost, 0);

  switch (ret) {
    case LOCK_VICTIM_IS_OTHER:
      /* We chose some other trx as a victim: retry if there still
    is a deadlock */
      goto retry;

    case LOCK_EXCEED_MAX_DEPTH:
      log_info("TOO DEEP OR LONG SEARCH IN THE LOCK TABLE WAITS-FOR GRAPH, WE WILL ROLL BACK FOLLOWING TRANSACTION");
      log_info("\n*** TRANSACTION:\n");

      log_info(trx->to_string(3000));
      log_info("*** TRANSACTION ENDS\n");

      break;

    case LOCK_VICTIM_IS_START:
      log_info("*** WE ROLL BACK TRANSACTION (2)");
      break;

    default:
      /* No deadlock detected*/
      return false;
  }

  m_lock_sys->set_deadlock_found();

  return true;
}

ulint Deadlock_detector::search_iterative(Trx *start, Trx *trx, Lock *wait_lock, ulint *cost, ulint depth) noexcept {
  ut_ad(mutex_own(&m_lock_sys->m_trx_sys->m_mutex));

  // Clear the stack before starting
  while (!m_search_stack.empty()) {
    m_search_stack.pop();
  }

  // Initialize the first frame
  ulint heap_no{ULINT_UNDEFINED};
  Lock *found_lock{};

  if (wait_lock->type() == LOCK_REC) {
    heap_no = wait_lock->rec_find_set_bit();
    ut_a(heap_no != ULINT_UNDEFINED);

    auto page_id = wait_lock->page_id();

    if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
      found_lock = UT_LIST_GET_LAST(it->second);
    }
  } else {
    ut_ad(wait_lock->type() == LOCK_TABLE);
    found_lock = UT_LIST_GET_LAST(wait_lock->m_table.m_table->m_locks);
  }

  // Push the initial frame
  m_search_stack.emplace(trx, wait_lock, depth, heap_no, found_lock);

  while (!m_search_stack.empty()) {
    auto &frame = m_search_stack.top();

    // Check if we've already searched this transaction
    if (frame.trx->m_deadlock_mark == 1) {
      m_search_stack.pop();
      continue;
    }

    // Increment cost
    ++*cost;

    if (*cost > LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK) {
      return LOCK_EXCEED_MAX_DEPTH;
    }

    if (frame.depth > LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK) {
      return LOCK_EXCEED_MAX_DEPTH;
    }

    // If this is the first iteration for this frame, initialize found_lock
    if (frame.is_first_iteration) {
      frame.is_first_iteration = false;

      if (frame.wait_lock->type() == LOCK_REC) {
        frame.heap_no = frame.wait_lock->rec_find_set_bit();
        ut_a(frame.heap_no != ULINT_UNDEFINED);

        auto page_id = frame.wait_lock->page_id();

        if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
          frame.found_lock = UT_LIST_GET_LAST(it->second);
        }
      } else {
        ut_ad(frame.wait_lock->type() == LOCK_TABLE);
        frame.found_lock = UT_LIST_GET_LAST(frame.wait_lock->m_table.m_table->m_locks);
      }
    }

    // Look for a conflicting lock which is ahead of wait_lock in the queue
    while (frame.found_lock != nullptr) {
      if (frame.found_lock == frame.wait_lock) {
        frame.found_lock = nullptr;
      } else if (frame.wait_lock->type() == LOCK_REC) {
        frame.found_lock = frame.found_lock->prev();
      } else {
        ut_ad(frame.wait_lock->type() == LOCK_TABLE);
        frame.found_lock = UT_LIST_GET_PREV(m_table.m_locks, frame.found_lock);
      }

      if (frame.found_lock == nullptr) {
        // We can mark this subtree as searched
        frame.trx->m_deadlock_mark = 1;
        m_search_stack.pop();
        break;
      }

      if (frame.wait_lock->has_to_wait_for(frame.found_lock, frame.heap_no)) {
        auto lock_trx = frame.found_lock->m_trx;

        if (lock_trx == start) {
          // We came back to the search starting point: a deadlock detected

          log_info("\n*** (1) TRANSACTION:");
          log_info(frame.wait_lock->m_trx->to_string(3000));

          log_info("*** (1) HOLDS THE LOCK(S):");
          log_info(m_lock_sys->lock_to_string(frame.wait_lock));

          /* It is possible that the transaction has also locks on tables and records */

          log_info("\n*** (2) TRANSACTION:");
          log_info(lock_trx->to_string(3000));

          log_info("*** (2) HOLDS THE LOCK(S):");
          log_info(m_lock_sys->lock_to_string(frame.found_lock));

          log_info("*** (2) WAITING FOR THIS LOCK TO BE GRANTED:");
          log_info(m_lock_sys->lock_to_string(frame.wait_lock));

          if (*cost > LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK) {
            /* If the search was too long, we choose 'start' as the victim and roll back it */
            return LOCK_VICTIM_IS_START;
          }

          m_lock_sys->set_deadlock_found();

          /* Let us choose the transaction of wait_lock as a victim to try
          to avoid deadlocking our search starting point transaction */

          log_info("*** WE ROLL BACK TRANSACTION (1)");

          frame.wait_lock->m_trx->m_was_chosen_as_deadlock_victim = true;

          m_lock_sys->cancel_waiting_and_release(frame.wait_lock);

          /* Since trx and wait_lock are no longer in the waits-for graph, we can return false;
          note that our selective algorithm can choose several transactions as victims, but still
          we may end up rolling back also the search starting point transaction! */

          return LOCK_VICTIM_IS_OTHER;
        }

        if (m_lock_sys->m_trx_sys->in_trx_list(lock_trx)) {
          /* If lock_trx is not committed, then check if it is waiting for some lock */

          if (lock_trx->m_que_state == TRX_QUE_LOCK_WAIT) {
            /* Another trx ahead has requested lock in an incompatible mode, and is itself waiting for a lock */

            // Push a new frame for the recursive call
            ulint new_heap_no{ULINT_UNDEFINED};
            Lock *new_found_lock{};

            if (lock_trx->m_wait_lock->type() == LOCK_REC) {
              new_heap_no = lock_trx->m_wait_lock->rec_find_set_bit();
              ut_a(new_heap_no != ULINT_UNDEFINED);

              auto page_id = lock_trx->m_wait_lock->page_id();

              if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
                new_found_lock = UT_LIST_GET_LAST(it->second);
              }
            } else {
              ut_ad(lock_trx->m_wait_lock->type() == LOCK_TABLE);
              new_found_lock = UT_LIST_GET_LAST(lock_trx->m_wait_lock->m_table.m_table->m_locks);
            }

            m_search_stack.emplace(lock_trx, lock_trx->m_wait_lock, frame.depth + 1, new_heap_no, new_found_lock);
            break;  // Continue with the new frame
          }
        }
      }
    }
  }

  return 0;
}

Trx *Deadlock_detector::choose_victim(Trx *trx1, Trx *trx2) noexcept {
  /* Choose the transaction with fewer locks as victim to minimize the amount of work needed for rollback */

  ulint trx1_locks = 0;
  ulint trx2_locks = 0;

  for (auto lock [[maybe_unused]] : trx1->m_trx_locks) {
    trx1_locks++;
  }

  for (auto lock [[maybe_unused]] : trx2->m_trx_locks) {
    trx2_locks++;
  }

  if (trx1_locks < trx2_locks) {
    return trx1;
  } else if (trx2_locks < trx1_locks) {
    return trx2;
  } else {
    /* If equal number of locks, choose the newer transaction */
    return (trx1->m_id > trx2->m_id) ? trx1 : trx2;
  }
}

void Deadlock_detector::report_deadlock(Trx *victim, Lock *wait_lock) noexcept {
  log_info("*** DEADLOCK DETECTED ***");
  log_info("\nVictim transaction:");
  log_info(victim->to_string(3000));
  log_info("\nWaiting for lock:");
  log_info(m_lock_sys->lock_to_string(wait_lock));
}
