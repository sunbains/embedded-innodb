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

#include "lock0priv.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0logger.h"

/* The waits-for graph and the search: see include/lock0deadlock.h. */

bool LockSys::deadlock_occurs(Lock *lock, Trx *trx) noexcept {
  /* The full search reads queues of any shard, and the cancel of a victim
  changes them: the lock system latched X. */
  ut_ad(x_own());
  ut_ad(trx->m_wait_lock == lock);

  bool victim{};

  for (;;) {
    Trx *pred{};

    if (trx->m_wait_lock == nullptr) {
      /* The cancel of another victim granted the request. */
      break;
    }

    const auto walk = deadlock_find(trx, &pred);

    if (walk == DeadlockWalk::NONE) {
      break;

    } else if (walk == DeadlockWalk::TOO_DEEP) {
      log_info("TOO DEEP OR LONG SEARCH IN THE LOCK TABLE WAITS-FOR GRAPH, WE WILL ROLL BACK FOLLOWING TRANSACTION");
      log_info("\n*** TRANSACTION:\n");
      log_info(trx->to_string(3000));
      log_info("*** WAITING FOR THIS LOCK TO BE GRANTED:");
      log_info(lock->to_string(m_sys));

      set_deadlock_found();
      victim = true;
      break;
    }

    ut_ad(walk == DeadlockWalk::CYCLE);

    set_deadlock_found();

    if (deadlock_victim(trx, pred) == trx) {
      log_info("*** WE ROLL BACK TRANSACTION (2)");
      victim = true;
      break;
    }

    /* Let us choose the transaction waiting for the requester as a victim
    to try to avoid deadlocking the requester; repeat, there may be
    another cycle. Note that we may end up rolling back the requester too! */

    log_info("*** WE ROLL BACK TRANSACTION (1)");

    choose_victim(pred);
  }

  return victim;
}

void LockSys::choose_victim(Trx *victim) noexcept {
  ut_ad(x_own());

  /* The wait state of the victim: its mutex. Its thread learns it when its
  wait ends (Srv::suspend_user_thread()): DB_DEADLOCK. */
  trx_mutex_enter(victim);
  victim->m_was_chosen_as_deadlock_victim = true;
  auto lock = victim->m_wait_lock;
  trx_mutex_exit(victim);

  ut_a(lock != nullptr);

  cancel_waiting_and_release(lock);
}

Lock *LockSys::waiting_lock(Trx *trx) noexcept {
  ut_ad(x_own());

  /* With X, m_wait_lock can't change (it changes with the latch of its
  queue); m_que_state also changes with the trx mutex only (the owner's
  query code): read both with it. */
  trx_mutex_enter(trx);
  auto lock = trx->m_que_state == TRX_QUE_LOCK_WAIT ? trx->m_wait_lock : nullptr;
  trx_mutex_exit(trx);

  return lock;
}

bool LockSys::deadlock_suspect(const Trx *trx) const noexcept {
  Trx *pred{};
  bool single{};

  /* Latch free: the edges are atomics, a Trx is type stable (TrxSys pool). */
  const auto walk = deadlock_search(trx, 0, &pred, &single);

  return walk != DeadlockWalk::NONE || !single;
}

DbErr LockSys::deadlock_resolve(Trx *trx) noexcept {
  ut_ad(x_own() || nothing_latched());

  Exclusive x(*this);

  trx_mutex_enter(trx);
  auto lock = trx->m_wait_lock;
  trx_mutex_exit(trx);

  if (lock == nullptr) {
    /* The wait ended meanwhile (granted, or cancelled as the victim of
    another check): the caller's lock wait sees it. */
    return DB_LOCK_WAIT;
  }

  if (deadlock_occurs(lock, trx)) {
    /* The requester is the victim: cancel its request; the wait ends
    (the query thread runs again) and the request returns DB_DEADLOCK. */
    trx_mutex_enter(trx);
    lock = trx->m_wait_lock;
    trx_mutex_exit(trx);

    if (lock != nullptr) {
      cancel_waiting_and_release(lock);
    }

    return DB_DEADLOCK;
  }

  return DB_LOCK_WAIT;
}

DbErr LockSys::deadlock_after_wait(DbErr err) noexcept {
  auto &s = suspect();
  auto trx = s;

  s = nullptr;

  if (err == DB_LOCK_WAIT && trx != nullptr) {
    return deadlock_resolve(trx);
  }

  return err;
}

DeadlockWalk LockSys::deadlock_search(const Trx *start, ulint depth, Trx **pred, bool *single) const noexcept {
  /* Latch free: only the edges, atomics, are read; a Trx is type stable
  (the TrxSys pool), a stale edge is caught by the full check under X. */

  auto trx = start;

  if (single != nullptr) {
    *single = true;
  }

  for (;; ++depth) {
    /* trx waits for blocker: an edge of the graph. */
    auto blocker = trx->m_blocking_trx.load(std::memory_order_seq_cst);

    if (single != nullptr && trx->m_blocking_multi.load(std::memory_order_relaxed)) {
      /* trx has other blockers: the one edge path is not the whole graph. */
      *single = false;
    }

    if (blocker == nullptr) {
      return DeadlockWalk::NONE;

    } else if (blocker == start) {
      /* Back at the start: a cycle, closed by the edge trx -> start. */
      *pred = const_cast<Trx *>(trx);
      return DeadlockWalk::CYCLE;

    } else if (depth > LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK) {
      return DeadlockWalk::TOO_DEEP;
    }

    /* If the blocker does not wait (it runs, it will release its locks),
    its edge is nullptr: the next step ends the walk. */
    trx = blocker;
  }
}

DeadlockWalk LockSys::deadlock_find(Trx *start, Trx **pred) noexcept {
  ut_ad(x_own());
  ut_ad(start->m_wait_lock != nullptr);

  bool single{};

  /* The fast path: the one edge walk. */
  const auto walk = deadlock_search(start, 0, pred, &single);

  if (walk == DeadlockWalk::CYCLE || single) {
    /* A cycle; or a path along which every transaction has one blocker:
    that path is all of the waits-for graph reachable from start. */
    return walk;
  }

  /* The slow path: some transaction on the path has other blockers. The
  full search over the blocker sets, read from the queues. */
  ++m_deadlock_epoch;

  return deadlock_dfs(start, start, start->m_wait_lock, 0, pred);
}

DeadlockWalk LockSys::deadlock_dfs(const Trx *start, Trx *trx, const Lock *wait_lock, ulint depth, Trx **pred) noexcept {
  ut_ad(x_own());

  if (trx->m_deadlock_mark == m_deadlock_epoch) {
    /* Its subtree was searched by this search: no cycle back to start. */
    return DeadlockWalk::NONE;
  }

  /* trx waits for wait_lock, which waits for every lock ahead of it in its
  queue that it has to wait for: their owners are the blockers of trx. */
  auto edge = [&](Trx *blocker) -> DeadlockWalk {
    if (blocker == start) {
      *pred = trx;
      return DeadlockWalk::CYCLE;
    } else if (depth > LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK) {
      return DeadlockWalk::TOO_DEEP;
    } else if (auto wait_lock = waiting_lock(blocker); wait_lock != nullptr) {
      return deadlock_dfs(start, blocker, wait_lock, depth + 1, pred);
    } else {
      return DeadlockWalk::NONE;
    }
  };

  if (wait_lock->type() == LOCK_REC) {
    const auto heap_no = wait_lock->rec_find_set_bit();
    const auto queue = rec_queue(wait_lock->page_id());

    ut_a(heap_no != ULINT_UNDEFINED);
    ut_a(queue != nullptr);

    for (auto lock : *queue) {
      if (lock == wait_lock) {
        break;
      } else if (lock->rec_is_nth_bit_set(heap_no) && wait_lock->has_to_wait_for(lock, heap_no)) {
        if (const auto r = edge(lock->m_trx); r != DeadlockWalk::NONE) {
          return r;
        }
      }
    }
  } else {
    for (auto lock : wait_lock->m_table.m_table->m_locks) {
      if (lock == wait_lock) {
        break;
      } else if (wait_lock->has_to_wait_for(lock, ULINT_UNDEFINED)) {
        if (const auto r = edge(lock->m_trx); r != DeadlockWalk::NONE) {
          return r;
        }
      }
    }
  }

  trx->m_deadlock_mark = m_deadlock_epoch;

  return DeadlockWalk::NONE;
}

Trx *LockSys::deadlock_victim(Trx *start, Trx *pred) noexcept {
  ut_ad(x_own());

  log_info("\n*** (1) TRANSACTION:");
  log_info(pred->to_string(3000));
  log_info("*** (1) WAITING FOR THIS LOCK TO BE GRANTED:");
  log_info(pred->m_wait_lock->to_string(m_sys));
  log_info("*** (2) TRANSACTION:");
  log_info(start->to_string(3000));
  log_info("*** (2) WAITING FOR THIS LOCK TO BE GRANTED:");
  log_info(start->m_wait_lock->to_string(m_sys));

  /* start closed the cycle; pred waits for it. If pred does not weigh less,
  start is the victim. */
  return Trx::weight_cmp(pred, start) >= 0 ? start : pred;
}

void LockSys::deadlock_check_moved() noexcept {
  ut_ad(x_own() || nothing_latched());

  auto &m = moved();

  if (m.empty()) {
    return;
  }

  std::vector<Trx *> todo;

  todo.swap(m);

  for (auto trx : todo) {
    /* The waiting lock of trx was moved by a page operation: new edges, is
    there a cycle through them now? The latch free walk first. A path too
    deep here is not a deadlock: only a requester is rolled back for that. */
    if (!deadlock_suspect(trx)) {
      continue;
    }

    Exclusive x(*this);

    while (waiting_lock(trx) != nullptr) {
      Trx *pred{};

      if (deadlock_find(trx, &pred) != DeadlockWalk::CYCLE) {
        break;
      }

      set_deadlock_found();

      auto victim = deadlock_victim(trx, pred);

      log_info("*** WE ROLL BACK TRANSACTION ", victim == trx ? "(2)" : "(1)");

      choose_victim(victim);
    }
  }
}
