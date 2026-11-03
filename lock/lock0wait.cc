/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file lock/lock0wait.cc
The transaction lock system: lock waits: entering one, its end by a grant or a cancel (deadlock victim, timeout, interrupt), and the release of the locks of a transaction. The latching is in the LATCHING
comment of include/lock0lock.h.
*******************************************************/

#include "lock0priv.h"

DbErr LockSys::enter_lock_wait(Trx *trx, QueThr *thr) noexcept {
  /* The deadlock check, latch free first (the walk of the one edge graph):
  the full check, with the lock system latched X, runs once the caller has
  released its shard latch (deadlock_resolve()), if the walk can't rule a
  deadlock out. */
  if (deadlock_suspect(trx)) {
    suspect() = trx;
  }

  trx_mutex_enter(trx);

  ut_ad(trx->m_wait_lock != nullptr);

  trx->m_wait_started = time(nullptr);
  trx->m_que_state = TRX_QUE_LOCK_WAIT;
  trx->m_was_chosen_as_deadlock_victim = false;

  if (thr != nullptr) {
    /* que_thr_stop() for a lock wait: the query thread waits, until a grant
    or a cancel ends the wait (Trx::end_lock_wait()). */
    ut_ad(thr->state == QUE_THR_RUNNING);
    thr->state = QUE_THR_LOCK_WAIT;
    trx->m_wait_thrs.push_front(thr);
  }

  trx_mutex_exit(trx);

  return DB_LOCK_WAIT;
}

void LockSys::grant(Lock *lock) noexcept {
  ut_ad(lock->type() == LOCK_TABLE ? owns_table(lock->m_table.m_table) : owns_page(lock->page_id()));

  auto trx = lock->m_trx;

  /* The wait state of trx: its mutex. */
  trx_mutex_enter(trx);

  lock->reset();

  /* If we are resolving a deadlock by choosing another transaction
  as a victim, then our original transaction may not be in the
  TRX_QUE_LOCK_WAIT state, and there is no need to end the lock wait
  for it */

  if (trx->m_que_state == TRX_QUE_LOCK_WAIT) {
    trx->end_lock_wait(m_sys);
  }

  trx_mutex_exit(trx);
}

void LockSys::grant_or_edge(Lock *lock, const Trx *blocker, bool multi, GrantPlan *plan) noexcept {
  if (plan != nullptr) {
    /* Only the decision, for the tests: nothing changes. */
    plan->emplace_back(lock, blocker == nullptr);
  } else if (blocker == nullptr) {
    grant(lock);
  } else {
    wait_edge(lock->m_trx, blocker, multi);
  }
}

void LockSys::reset_wait(Lock *lock) noexcept {
  trx_mutex_enter(lock->m_trx);
  lock->reset();
  trx_mutex_exit(lock->m_trx);
}

void LockSys::wait_edge(Trx *trx, const Trx *blocker, bool multi) noexcept {
  ut_ad(latched());
  ut_ad(blocker != nullptr && blocker != trx);

  /* The blocker the edge named may be gone: name one that is there. A
  release only removes edges of the full waits-for graph, it never makes a
  cycle (see lock0deadlock.h): no deadlock check here. */
  trx->m_blocking_multi.store(multi, std::memory_order_relaxed);
  trx->m_blocking_trx.store(const_cast<Trx *>(blocker), std::memory_order_release);
}

void LockSys::release(Trx *trx) noexcept {
  /* C5 of the commit: no latch held (lock0lock.h), or the lock system
  latched X (the tests). */
  ut_ad(x_own() || nothing_latched());
  ut_ad(!mutex_own(&m_sys.m_trx_sys->m_mutex));

  /* The last lock of trx, read with its mutex: another thread may add a
  lock for trx meanwhile (an inheritance or a move by a page operation),
  at the end of the list. */
  auto back = [trx]() {
    trx_mutex_enter(trx);
    auto lock = trx->m_trx_locks.back();
    trx_mutex_exit(trx);
    return lock;
  };

  for (auto lock = back(); lock != nullptr; lock = back()) {
    /* Release the locks of trx shard by shard: the latch of one shard at a
    time, for up to LOCK_RELEASE_BATCH locks in a row of that shard (a
    bound on how long others wait for it), and no other latch. */
    if (lock->type() == LOCK_REC) {
      const auto shard = page_shard_no(lock->page_id());

      PageLatch latch(*this, lock->page_id());

      /* Read again with the latch: the lock read without it may have gone
      (a page discard frees the empty locks of the page). */
      lock = back();

      for (ulint n{}; n < LOCK_RELEASE_BATCH && lock != nullptr && lock->type() == LOCK_REC && page_shard_no(lock->page_id()) == shard; ++n) {
        rec_dequeue_from_page(lock);
        lock = back();
      }

    } else {
      ut_ad(lock->type() == LOCK_TABLE);

      const auto table = lock->m_table.m_table;

      TableLatch latch(*this, table);

      lock = back();

      for (ulint n{}; n < LOCK_RELEASE_BATCH && lock != nullptr && lock->type() == LOCK_TABLE && table_shard_no(lock->m_table.m_table) == table_shard_no(table); ++n) {
        table_dequeue(lock);
        lock = back();
      }
    }
  }

  trx_mutex_enter(trx);
  mem_heap_empty(trx->m_lock_heap);
  trx_mutex_exit(trx);
}

void LockSys::cancel_waiting_and_release(Lock *lock) noexcept {
  ut_ad(lock->type() == LOCK_TABLE ? owns_table(lock->m_table.m_table) : owns_page(lock->page_id()));

  if (lock->type() == LOCK_REC) {

    rec_dequeue_from_page(lock);
  } else {
    ut_ad(lock->type() == LOCK_TABLE);

    table_dequeue(lock);
  }

  auto trx = lock->m_trx;

  trx_mutex_enter(trx);

  /* Reset the wait flag and the back pointer to lock in trx */

  lock->reset();

  /* The following function releases the trx from lock wait */

  if (trx->m_que_state == TRX_QUE_LOCK_WAIT) {
    trx->end_lock_wait(m_sys);
  }

  trx_mutex_exit(trx);
}

bool LockSys::cancel_wait(Trx *trx) noexcept {
  ut_ad(nothing_latched());

  for (;;) {
    trx_mutex_enter(trx);
    auto lock = trx->m_wait_lock;
    trx_mutex_exit(trx);

    if (lock == nullptr) {
      /* Not waiting (granted, or cancelled), or a page operation is moving
      the waiting lock: decide with the lock system latched X. */
      Exclusive x(*this);

      trx_mutex_enter(trx);

      lock = trx->m_wait_lock;

      if (lock == nullptr) {
        const bool waiting = trx->m_que_state == TRX_QUE_LOCK_WAIT;

        if (waiting) {
          trx->end_lock_wait(m_sys);
        }

        trx_mutex_exit(trx);

        return waiting;
      }

      trx_mutex_exit(trx);

      cancel_waiting_and_release(lock);

      return true;
    }

    /* The latch of the queue of lock, then check it still waits there. The
    lock memory stays: it is in the lock heap of trx, which only its own
    thread (this one) empties. */
    if (lock->type() == LOCK_REC) {
      PageLatch latch(*this, lock->page_id());

      trx_mutex_enter(trx);
      const bool still = trx->m_wait_lock == lock;
      trx_mutex_exit(trx);

      if (still) {
        cancel_waiting_and_release(lock);
        return true;
      }
    } else {
      TableLatch latch(*this, lock->m_table.m_table);

      trx_mutex_enter(trx);
      const bool still = trx->m_wait_lock == lock;
      trx_mutex_exit(trx);

      if (still) {
        cancel_waiting_and_release(lock);
        return true;
      }
    }

    trx_mutex_enter(trx);
    const bool waiting = trx->m_wait_lock != nullptr || trx->m_que_state == TRX_QUE_LOCK_WAIT;
    trx_mutex_exit(trx);

    if (!waiting) {
      /* A grant came first. */
      return false;
    }
  }
}

