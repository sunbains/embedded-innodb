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
  /* The deadlock check, latch free first (the walk of the one edge graph),
  then in the queue of the request, with its shard latch: the full check,
  with the lock system latched X, runs once the caller has released its
  shard latch (deadlock_resolve()), if neither can rule a deadlock out. */
  if (deadlock_suspect_req(trx)) {
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

  /* The locks of trx shard by shard, from the end of its list: the latch of
  one shard at a time, for up to LOCK_RELEASE_BATCH locks in a row of that
  shard (a bound on how long others wait for it), and no other latch. With
  the shard latch, the batch is taken out of the list of trx with one hold
  of its mutex (another thread may add a lock for trx meanwhile, at the end
  of the list: an inheritance or a move by a page operation, of a lock of
  trx in another shard), then out of the queues: those locks are only seen
  through the queues of the shard, latched. */
  std::array<Lock *, LOCK_RELEASE_BATCH> batch;

  /* The shard to start with: where the last lock was created, a hint (no
  lock there: the batch is empty, and the next shard is read with it). */
  const auto hint = trx->m_lock_hint.load(std::memory_order_relaxed);

  bool rec = !(hint & Trx::LOCK_HINT_TABLE);
  uint32_t shard = hint & ~Trx::LOCK_HINT_TABLE;

  /* The heap is emptied here only if it grew (its size as the monitor
  shows it): else by the next lock creation for trx, which holds the trx
  mutex anyway. */
  bool big_heap{};

  for (;;) {
    Lock *next;

    /* With the shard latch: the batch out of the list, then the next lock
    to release (of another shard), with one hold of the trx mutex. Once
    the list is empty, no lock can be created for trx (a page operation
    creates one only from a lock of trx in a queue it latches, and those of
    the batch are only in queues of this shard): the batch is the last. */
    auto take = [&]() {
      auto in_shard = [&](const Lock *l) {
        return rec ? l->type() == LOCK_REC && page_shard_no(l->page_id()) == shard
                   : l->type() == LOCK_TABLE && table_shard_no(l->m_table.m_table) == shard;
      };

      size_t n{};

      trx_mutex_enter(trx);

      auto l = trx->m_trx_locks.back();

      for (; n < batch.size() && l != nullptr && in_shard(l); l = trx->m_trx_locks.back()) {
        trx->m_trx_locks.remove(l);
        batch[n++] = l;
      }

      next = l;
      big_heap = l == nullptr && mem_heap_get_size(trx->m_lock_heap) > 400;

      trx_mutex_exit(trx);

      return n;
    };

    if (rec) {
      PageLatch latch(*this, ShardNo{shard});

      const auto n = take();

      for (size_t i{}; i < n; ++i) {
        rec_dequeue_from_page(batch[i], false);
      }
    } else {
      TableLatch latch(*this, ShardNo{shard});

      const auto n = take();

      for (size_t i{}; i < n; ++i) {
        table_dequeue(batch[i], false);
      }
    }

    if (next == nullptr) {
      break;
    }

    /* The lock, read without the shard latch, may go meanwhile (a page
    discard frees the empty locks of the page; its memory stays, in the lock
    heap of trx): the next batch is read again with the latch. */
    rec = next->type() == LOCK_REC;
    shard = rec ? page_shard_no(next->page_id()) : table_shard_no(next->m_table.m_table);
  }

  if (big_heap) {
    trx_mutex_enter(trx);
    mem_heap_empty(trx->m_lock_heap);
    trx_mutex_exit(trx);
  }
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

