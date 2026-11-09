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

/** @file lock/lock0rec.cc
The transaction lock system: the record lock queues, by key, their grant pass, and the inheritance of gap locks from key to key when keys come and go. The latching is in the LATCHING
comment of include/lock0lock.h.
*******************************************************/

#include "lock0priv.h"

#ifdef UNIV_DEBUG
const Lock *LockSys::rec_exists(const Rec_locks &rec_locks) const noexcept {
  ut_ad(rec_locks.empty() || owns_key(rec_locks.front()->key()));

  for (auto lock : rec_locks) {
    if (lock->rec_is_nth_bit_set(LOCK_BIT)) {
      return lock;
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

const Lock *LockSys::rec_get_prev(const Lock *in_lock) noexcept {
  ut_ad(in_lock != nullptr);
  ut_ad(owns_key(in_lock->key()));
  ut_ad(in_lock->type() == LOCK_REC);

  if (auto q = rec_queue(in_lock->key()); likely(q != nullptr)) {

    Lock *prev_lock{};

    for (auto lock : (*q)) {
      ut_ad(lock->type() == LOCK_REC);

      if (lock == in_lock) {
        return prev_lock;
      }

      if (lock->rec_is_nth_bit_set(LOCK_BIT)) {
        prev_lock = lock;
      }
    }
  }

  return nullptr;
}

const Lock *LockSys::rec_has_expl(LockKey page_id, ulint precise_mode, const Trx *trx) const noexcept {
  ut_ad(owns_key(page_id));
  ut_ad((precise_mode & LOCK_MODE_MASK) == LOCK_S || (precise_mode & LOCK_MODE_MASK) == LOCK_X);
  ut_ad(!(precise_mode & LOCK_INSERT_INTENTION));

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {

      if (lock->m_trx == trx && lock->rec_is_nth_bit_set(LOCK_BIT) &&
          Lock::mode_stronger_or_eq(lock->mode(), LockMode(precise_mode & LOCK_MODE_MASK)) && !lock->is_waiting() &&
          (!lock->rec_is_not_gap() || (precise_mode & LOCK_REC_NOT_GAP)) &&
          (!lock->rec_is_gap() || (precise_mode & LOCK_GAP)) &&
          (!lock->rec_is_insert_intention())) {

        return lock;
      }
    }
  }

  return nullptr;
}

#ifdef UNIV_DEBUG
const Lock *LockSys::rec_other_has_expl_req(LockKey page_id, LockMode mode, ulint gap, ulint wait, const Trx *trx)
  const noexcept {
  ut_ad(owns_key(page_id));
  ut_ad(mode == LOCK_X || mode == LOCK_S);
  ut_ad(gap == 0 || gap == LOCK_GAP);
  ut_ad(wait == 0 || wait == LOCK_WAIT);

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {

      if (lock->m_trx != trx && lock->rec_is_nth_bit_set(LOCK_BIT) &&
          (gap || !(lock->rec_is_gap())) && (wait || !lock->is_waiting()) &&
          Lock::mode_stronger_or_eq(lock->mode(), mode)) {

        return lock;
      }
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

Lock *LockSys::rec_other_has_conflicting(LockKey page_id, LockMode mode, Trx *trx) noexcept {
  ut_ad(owns_key(page_id));

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {
      if (unlikely(lock->rec_is_nth_bit_set(LOCK_BIT) && lock->rec_blocks(trx, mode))) {
        return lock;
      }
    }
  }

  return nullptr;
}

Lock *LockSys::rec_find_similar_on_page(LockMode type_mode, Lock *lock, const Trx *trx) noexcept {
  ut_ad(latched());

  for (/* No op */; lock != nullptr; lock = lock->next()) {
    if (lock->m_trx == trx && lock->m_type_mode == type_mode && lock->rec_get_n_bits() > LOCK_BIT) {
      return lock;
    }
  }

  return nullptr;
}

Lock *LockSys::rec_create_low(LockKey page_id, LockMode type_mode, const Index *index, Trx *trx) noexcept {
  ut_ad(owns_key(page_id));

  /* One key to a lock: a byte of bitmap. */
  constexpr ulint n_bytes{1};

  /* The lock heap and the lock list of trx: its mutex. Another thread may
  create a lock for trx (an inheritance, a move, an implicit lock
  conversion) while trx itself requests one in another shard. */
  trx_mutex_enter(trx);

  /* The lock heap of trx is emptied when its list is (LockSys::release()
  leaves a small heap for here): no lock of trx is in use then (a lock is
  only created for trx from a lock of trx in its list, or by trx). */
  if (trx->m_trx_locks.empty()) {
    mem_heap_empty(trx->m_lock_heap);
  }

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->m_lock_heap, sizeof(Lock) + n_bytes));

  trx->m_trx_locks.push_back(lock);
  trx->m_lock_hint.store(key_shard_no(page_id), std::memory_order_relaxed);

  lock->m_trx = trx;

  lock->m_type_mode = LockMode((type_mode & ~LOCK_TYPE_MASK) | LOCK_REC);

  lock->m_rec.m_index = index;
  lock->m_rec.m_fold = page_id.m_fold;
  lock->m_n_bits = n_bytes * 8;

  /* Reset to zero the bitmap which resides immediately after the lock struct */

  lock->rec_bitmap_reset();

  /* Set the bit corresponding to rec */
  lock->rec_set_nth_bit(LOCK_BIT);

  /* Appended, never inserted (lock0deadlock.h relies on it). */
  auto &queue = rec_queue_add(page_id);

  queue.push_back(lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock->set_trx_wait(trx);

    /* Publish the edge of the waits-for graph: a transaction with a lock
    ahead that the waiting lock has to wait for. A request waits only if
    there is one. */
    bool multi{};
    auto blocker = rec_find_blocker(queue, lock, &multi);

    trx->m_blocking_multi.store(multi, std::memory_order_relaxed);
    /* Sequentially consistent, as the loads of the deadlock walk: two
    requests that close a cycle in two shards at the same time each publish
    their edge, then walk; with release and acquire only, both walks could
    miss the other's edge (a store then a load, Dekker); with seq_cst one of
    them sees it and runs the full check. */
    trx->m_blocking_trx.store(const_cast<Trx *>(blocker), std::memory_order_seq_cst);
  }

  trx_mutex_exit(trx);

  return lock;
}

const Trx *LockSys::rec_find_blocker(const Rec_locks &rec_locks, const Lock *wait_lock, bool *multi) const noexcept {
  ut_ad(owns_key(wait_lock->key()));

  Blockers b;

  for (auto lock : rec_locks) {
    if (lock == wait_lock) {
      break;
    } else if (lock->rec_is_nth_bit_set(LOCK_BIT) && wait_lock->has_to_wait_for(lock)) {
      b.add(lock->m_trx);
    }
  }

  *multi = b.m_n > 1;

  return b.pick();
}

Lock *LockSys::rec_create(LockMode type_mode, LockKey key, const Index *index, const Trx *trx) noexcept {
  ut_ad(owns_key(key));

  return rec_create_low(key, type_mode, index, const_cast<Trx *>(trx));
}

DbErr LockSys::rec_enqueue_waiting(LockMode type_mode, LockKey key, const Index *index, Trx *trx, QueThr *thr) noexcept {
  ut_ad(owns_key(key));

  switch (trx->get_dict_operation()) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      log_err(std::format(
        "A record lock wait happens in a dictionary operation! trx_id: {} index: {} table: {} index_id: {} index_name: {}",
        trx->m_id,
        index->m_name,
        index->get_table_name(),
        index->m_id,
        index->m_name
      ));
  }

  /* Enqueue the lock request that will wait to be granted */
  (void)rec_create(LockMode(type_mode | LOCK_WAIT), key, index, trx);

  return enter_lock_wait(trx, thr);
}

Lock *LockSys::rec_add_to_queue(LockMode type_mode, LockKey key, const Index *index, const Trx *trx) noexcept {
  ut_ad(owns_key(key));

#ifdef UNIV_DEBUG
  switch (type_mode & LOCK_MODE_MASK) {
    case LOCK_X:
    case LOCK_S:
      break;
    default:
      ut_error;
  }

  if (!(type_mode & (LOCK_WAIT | LOCK_GAP))) {
    auto mode = (type_mode & LOCK_MODE_MASK) == LOCK_S ? LOCK_X : LOCK_S;
    auto other_lock = rec_other_has_expl_req(key, mode, 0, LOCK_WAIT, trx);
    ut_a(other_lock == nullptr);
  }
#endif /* UNIV_DEBUG */

  type_mode = LockMode(type_mode | LOCK_REC);

  /* Look for a waiting lock request on the same record or on a gap */

  auto q = rec_queue(key);

  if (q != nullptr) {
    for (auto lock : *q) {
      if (lock->is_waiting() && lock->rec_is_nth_bit_set(LOCK_BIT)) {
        return rec_create(type_mode, key, index, trx);
      }
    }
  }

  if (likely(!(type_mode & LOCK_WAIT))) {

    /* Look for a similar record lock on the same page:
    if one is found and there are no waiting lock requests,
    we can just set the bit */

    auto lock = q != nullptr ? q->front() : nullptr;

    lock = rec_find_similar_on_page(type_mode, lock, trx);

    if (likely(lock != nullptr)) {

      /* The struct may be ahead of other locks in the queue: no lock waits
      on the record (checked above), so no waiting lock gets a new granted
      lock ahead of it (the completeness of the deadlock check relies on
      it, see lock0deadlock.h). */
      ut_ad(q == nullptr || std::none_of(q->begin(), q->end(), [](const Lock *l) {
        return l->is_waiting() && l->rec_is_nth_bit_set(LOCK_BIT);
      }));

      lock->rec_set_nth_bit(LOCK_BIT);

      return lock;
    }
  }

  return rec_create(type_mode, key, index, trx);
}

bool LockSys::rec_lock_fast(bool impl, LockMode mode, LockKey key, const Index *index, Trx *trx) noexcept {
  ut_ad(owns_key(key));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  auto q = rec_queue(key);

  if (q == nullptr) {

    if (!impl) {
      (void)rec_create(mode, key, index, trx);
    }

    return true;
  }

  auto lock = q->front();

  if (lock->next() != nullptr) {

    return false;
  }

  if (lock->m_trx != trx || lock->m_type_mode != (mode | LOCK_REC) || lock->rec_get_n_bits() <= LOCK_BIT) {

    return false;
  }

  if (!impl) {
    /* If the nth bit of the record lock is already set then we
    do not set a new lock bit, otherwise we do set */

    if (!lock->rec_is_nth_bit_set(LOCK_BIT)) {
      /* The only lock of the queue, not waiting: no waiter gets a lock ahead. */
      ut_ad(!lock->is_waiting());
      lock->rec_set_nth_bit(LOCK_BIT);
    }
  }

  return true;
}

DbErr LockSys::rec_lock_slow(bool impl, LockMode mode, LockKey key, const Index *index, Trx *trx, QueThr *thr) noexcept {
  ut_ad(owns_key(key));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  DbErr err;

  if (rec_has_expl(key, mode, trx)) {
    /* The trx already has a strong enough lock on rec: do
    nothing */

    err = DB_SUCCESS;
  } else if (rec_other_has_conflicting(key, LockMode(mode), trx)) {

    /* If another transaction has a non-gap conflicting request in
    the queue, as this transaction does not have a lock strong
    enough already granted on the record, we have to wait. */

    err = rec_enqueue_waiting(mode, key, index, trx, thr);

  } else {

    if (!impl) {
      /* Set the requested lock on the record */

      (void)rec_add_to_queue(LockMode(LOCK_REC | mode), key, index, trx);
    }

    err = DB_SUCCESS;
  }

  return err;
}

DbErr LockSys::rec_lock(bool impl, LockMode mode, LockKey key, const Index *index, Trx *trx, QueThr *thr) noexcept {
  ut_ad(owns_key(key));

  if (rec_lock_fast(impl, mode, key, index, trx)) {

    /* We try a simplified and faster subroutine for the most common cases */

    return DB_SUCCESS;

  } else {
    return rec_lock_slow(impl, mode, key, index, trx, thr);
  }
}

bool LockSys::rec_has_to_wait_in_queue(const Rec_locks &rec_locks, Lock *waiting_lock) const noexcept {
  ut_ad(owns_key(waiting_lock->key()));
  ut_ad(waiting_lock->is_waiting());
  ut_ad(waiting_lock->type() == LOCK_REC);
  ut_ad(LOCK_BIT == waiting_lock->rec_find_set_bit());

  for (auto lock = rec_locks.front(); lock != waiting_lock; lock = lock->next()) {

    ut_ad(lock->type() == LOCK_REC);
    ut_ad(lock->key() == waiting_lock->key());

    if (LOCK_BIT < lock->rec_get_n_bits() && lock->rec_is_nth_bit_set(LOCK_BIT) && waiting_lock->has_to_wait_for(lock)) {
      return true;
    }
  }

  return false;
}

void LockSys::rec_cancel(Lock *lock) noexcept {
  ut_ad(lock->type() == LOCK_REC);
  ut_ad(owns_key(lock->key()));

  /* Reset the bit (there can be only one set bit) in the lock bitmap */
  lock->rec_reset_nth_bit(lock->rec_find_set_bit());

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

void LockSys::rec_dequeue_from_page(Lock *lock, bool in_trx) noexcept {
  ut_ad(lock->type() == LOCK_REC);
  ut_ad(owns_key(lock->key()));

  auto &rec_locks = *rec_queue(lock->key());

  ut_a(!rec_locks.empty());

  rec_locks.remove(lock);

  const bool rec_locks_empty = rec_locks.empty();

  if (rec_locks_empty) {
    rec_queue_erase(lock->key());
  }

  if (in_trx) {
    auto trx = lock->m_trx;

    trx_mutex_enter(trx);
    trx->m_trx_locks.remove(lock);
    trx_mutex_exit(trx);
  }

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  if (!rec_locks_empty) {
    rec_grant_pass(rec_locks);
  }
}

void LockSys::rec_grant_pass(Rec_locks &rec_locks, GrantPlan *plan) noexcept {
  /* One key to a queue: every waiting lock waits for that key. */
  rec_grant_pass_on(rec_locks, plan);
}

void LockSys::rec_grant_pass_on(Rec_locks &rec_locks, GrantPlan *plan) noexcept {
  ut_ad(rec_locks.empty() || owns_key(rec_locks.front()->key()));

  RecSeen seen;

  for (auto lock : rec_locks) {
#ifdef UNIT_TESTING
    m_n_grant_steps.fetch_add(1, std::memory_order_relaxed);
#endif /* UNIT_TESTING */

    if (!lock->rec_is_nth_bit_set(LOCK_BIT)) {
      continue;
    }

    if (lock->is_waiting()) {
      bool multi{};
      auto blocker = seen.blocker(lock, &multi);

      grant_or_edge(lock, blocker, multi, plan);
    }

    seen.add(lock);
  }
}

void LockSys::rec_discard(Lock *in_lock) noexcept {
  ut_ad(in_lock->type() == LOCK_REC);
  ut_ad(owns_key(in_lock->key()));

  auto &rec_locks = *rec_queue(in_lock->key());

  /* Other locks on the page stay: drop the page entry only with its last lock. */
  rec_locks.remove(in_lock);

  if (rec_locks.empty()) {
    rec_queue_erase(in_lock->key());
  }

  auto trx = in_lock->m_trx;

  trx_mutex_enter(trx);
  trx->m_trx_locks.remove(in_lock);
  trx_mutex_exit(trx);
}

void LockSys::rec_inherit_to_gap(LockKey heir, LockKey key) noexcept {
  ut_ad(owns_key(heir) && owns_key(key));

  /* Two keys with one fingerprint, the queue is theirs already: and a copy into the queue it walks
  would walk its copies. */
  if (heir == key) {
    return;
  }

  /* If session is using READ COMMITTED isolation level, we do not want locks set by an UPDATE or a DELETE to be inherited as gap type locks.
  But we DO want S-locks set by a consistency constraint to be inherited also then. */
  if (auto q = rec_queue(key); q != nullptr) {
    for (auto lock : (*q)) {
      if (lock->rec_is_nth_bit_set(LOCK_BIT) && !lock->rec_is_insert_intention() &&
          !(lock->m_trx->m_isolation_level == TRX_ISO_READ_COMMITTED && lock->mode() == LOCK_X)) {
        (void)rec_add_to_queue(LockMode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), heir, lock->rec_index(), lock->m_trx);
      }
    }
  }
}

void LockSys::rec_inherit_to_gap_if_gap_lock(LockKey heir, LockKey key) noexcept {
  ut_ad(owns_key(heir) && owns_key(key));

  /* See rec_inherit_to_gap(). */
  if (heir == key) {
    return;
  }

  if (auto q = rec_queue(key); q != nullptr) {
    for (auto lock : (*q)) {
      if (lock->rec_is_nth_bit_set(LOCK_BIT) && !lock->rec_is_insert_intention() && !lock->rec_is_not_gap()) {
        (void)rec_add_to_queue(LockMode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), heir, lock->rec_index(), lock->m_trx);
      }
    }
  }
}

void LockSys::update_insert(const Index *, LockKey key, LockKey next) noexcept {
  /* The gap below next is now two: the new key's and next's. The gap locks of next cover both. */
  KeyLatch latch(*this, key, next);

  rec_inherit_to_gap_if_gap_lock(key, next);
}

void LockSys::update_delete(const Index *, LockKey key, LockKey next) noexcept {
  KeyLatch latch(*this, key, next);

  /* The gap below key and the key itself become the gap below next. */
  rec_inherit_to_gap(next, key);

  /* What was on the record goes, its gap stays: key can still bound a gap as a fence (the high key of
  the page to the left, when it was the first record of its page), and a gap lock never conflicts but
  with an insert into it. Who waited for the record goes on and finds what's there now. */
  if (auto q = rec_queue(key); q != nullptr) {
    for (auto lock : *q) {
      if (!lock->rec_is_nth_bit_set(LOCK_BIT)) {
        continue;
      } else if (lock->is_waiting()) {
        rec_cancel(lock);
      } else if (lock->rec_is_not_gap()) {
        lock->rec_reset_nth_bit(LOCK_BIT);
      } else if (!lock->rec_is_gap() && !lock->rec_is_insert_intention()) {
        lock->m_type_mode = LockMode(lock->m_type_mode | LOCK_GAP);
      }
    }

    rec_grant_pass(*q);
  }
}

void LockSys::inherit_gap(const Index *, LockKey heir, LockKey key) noexcept {
  if (heir == key) {
    return;
  }

  KeyLatch latch(*this, heir, key);

  rec_inherit_to_gap(heir, key);
}

void LockSys::rec_unlock(Trx *trx, LockKey key, LockMode lock_mode) noexcept {
  KeyLatch latch(*this, key);

  Lock *release_lock{};

  /* Find the last lock with the same mode and transaction on the key. */
  auto q = rec_queue(key);
  ut_a(q != nullptr);

  for (auto lock : *q) {
    if (lock->rec_is_nth_bit_set(LOCK_BIT) && lock->m_trx == trx && lock->mode() == lock_mode) {
      release_lock = lock;
      ut_a(!lock->is_waiting());
    }
  }

  /* If a record lock is found, release the record lock */
  if (likely(release_lock != nullptr)) {
    release_lock->rec_reset_nth_bit(LOCK_BIT);
  } else {
    log_err(std::format("Unlock row could not find a {} mode lock on the record", to_int(lock_mode)));
    return;
  }

  /* Check if we can now grant waiting lock requests */
  rec_grant_pass_on(*q);
}

