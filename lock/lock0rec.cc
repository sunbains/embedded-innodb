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
The transaction lock system: the record lock queues, their grant pass, and the page operations that move and inherit record locks. The latching is in the LATCHING
comment of include/lock0lock.h.
*******************************************************/

#include "lock0priv.h"

#ifdef UNIV_DEBUG
const Lock *LockSys::rec_exists(const Rec_locks &rec_locks, ulint heap_no) const noexcept {
  ut_ad(rec_locks.empty() || owns_page(rec_locks.front()->page_id()));

  for (auto lock : rec_locks) {
    if (lock->rec_is_nth_bit_set(heap_no)) {
      return lock;
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

const Lock *LockSys::rec_get_prev(const Lock *in_lock, ulint heap_no) noexcept {
  ut_ad(in_lock != nullptr);
  ut_ad(owns_page(in_lock->page_id()));
  ut_ad(in_lock->type() == LOCK_REC);

  if (auto q = rec_queue(in_lock->page_id()); likely(q != nullptr)) {

    Lock *prev_lock{};

    for (auto lock : (*q)) {
      ut_ad(lock->type() == LOCK_REC);

      if (lock == in_lock) {
        return prev_lock;
      }

      if (lock->rec_is_nth_bit_set(heap_no)) {
        prev_lock = lock;
      }
    }
  }

  return nullptr;
}

const Lock *LockSys::rec_has_expl(PageId page_id, ulint precise_mode, ulint heap_no, const Trx *trx) const noexcept {
  ut_ad(owns_page(page_id));
  ut_ad((precise_mode & LOCK_MODE_MASK) == LOCK_S || (precise_mode & LOCK_MODE_MASK) == LOCK_X);
  ut_ad(!(precise_mode & LOCK_INSERT_INTENTION));

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {

      if (lock->m_trx == trx && lock->rec_is_nth_bit_set(heap_no) &&
          Lock::mode_stronger_or_eq(lock->mode(), LockMode(precise_mode & LOCK_MODE_MASK)) && !lock->is_waiting() &&
          (!lock->rec_is_not_gap() || (precise_mode & LOCK_REC_NOT_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
          (!lock->rec_is_gap() || (precise_mode & LOCK_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
          (!lock->rec_is_insert_intention())) {

        return lock;
      }
    }
  }

  return nullptr;
}

#ifdef UNIV_DEBUG
const Lock *LockSys::rec_other_has_expl_req(PageId page_id, LockMode mode, ulint gap, ulint wait, ulint heap_no, const Trx *trx)
  const noexcept {
  ut_ad(owns_page(page_id));
  ut_ad(mode == LOCK_X || mode == LOCK_S);
  ut_ad(gap == 0 || gap == LOCK_GAP);
  ut_ad(wait == 0 || wait == LOCK_WAIT);

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {

      if (lock->m_trx != trx && lock->rec_is_nth_bit_set(heap_no) &&
          (gap || !(lock->rec_is_gap() || heap_no == PAGE_HEAP_NO_SUPREMUM)) && (wait || !lock->is_waiting()) &&
          Lock::mode_stronger_or_eq(lock->mode(), mode)) {

        return lock;
      }
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

Lock *LockSys::rec_other_has_conflicting(PageId page_id, LockMode mode, ulint heap_no, Trx *trx) noexcept {
  ut_ad(owns_page(page_id));

  if (auto q = rec_queue(page_id); likely(q != nullptr)) {
    for (auto lock : (*q)) {
      if (unlikely(lock->rec_is_nth_bit_set(heap_no) && lock->rec_blocks(trx, mode, heap_no == PAGE_HEAP_NO_SUPREMUM))) {
        return lock;
      }
    }
  }

  return nullptr;
}

Lock *LockSys::rec_find_similar_on_page(LockMode type_mode, ulint heap_no, Lock *lock, const Trx *trx) noexcept {
  ut_ad(latched());

  for (/* No op */; lock != nullptr; lock = lock->next()) {
    if (lock->m_trx == trx && lock->m_type_mode == type_mode && lock->rec_get_n_bits() > heap_no) {
      return lock;
    }
  }

  return nullptr;
}

Lock *LockSys::rec_create_low(
  PageId page_id, LockMode type_mode, ulint heap_no, ulint n_bits, const Index *index, Trx *trx
) noexcept {
  ut_ad(owns_page(page_id));

  /* If rec is the supremum record, then we reset the gap and
  LOCK_REC_NOT_GAP bits, as all locks on the supremum are
  automatically of the gap type */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(type_mode & LOCK_REC_NOT_GAP));

    type_mode = LockMode(type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP));
  }

  /* Make lock bitmap bigger by a safety margin */
  const auto n_bytes = 1 + (n_bits + LOCK_PAGE_BITMAP_MARGIN) / 8;

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
  trx->m_lock_hint.store(page_shard_no(page_id), std::memory_order_relaxed);

  lock->m_trx = trx;

  lock->m_type_mode = LockMode((type_mode & ~LOCK_TYPE_MASK) | LOCK_REC);

  lock->m_rec.m_index = index;
  lock->m_rec.m_page_id = page_id;
  lock->m_n_bits = n_bytes * 8;

  /* Reset to zero the bitmap which resides immediately after the lock struct */

  lock->rec_bitmap_reset();

  /* Set the bit corresponding to rec */
  lock->rec_set_nth_bit(heap_no);

  /* Appended, never inserted (lock0deadlock.h relies on it). */
  auto &queue = rec_queue_add(page_id);

  queue.push_back(lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock->set_trx_wait(trx);

    /* Publish the edge of the waits-for graph: a transaction with a lock
    ahead that the waiting lock has to wait for. A request waits only if
    there is one; a lock moved to another record by a page operation keeps
    the order of its queue, so its blockers are ahead of it there too. */
    bool multi{};
    auto blocker = rec_find_blocker(queue, lock, heap_no, &multi);

    trx->m_blocking_multi.store(multi, std::memory_order_relaxed);
    /* Sequentially consistent, as the loads of the deadlock walk: two
    requests that close a cycle in two shards at the same time each publish
    their edge, then walk; with release and acquire only, both walks could
    miss the other's edge (a store then a load, Dekker); with seq_cst one of
    them sees it and runs the full check. */
    trx->m_blocking_trx.store(const_cast<Trx *>(blocker), std::memory_order_seq_cst);

    /* A transaction already in the lock wait state: its waiting lock was
    moved by a page operation (with page latches, not the X latch: tests
    under X skip it): new edges, checked once the page operation released
    its latches (deadlock_check_moved()). A request checks itself. */
    if (trx->m_que_state == TRX_QUE_LOCK_WAIT && !x_own()) {
      moved().push_back(trx);
    }
  }

  trx_mutex_exit(trx);

  return lock;
}

const Trx *LockSys::rec_find_blocker(const Rec_locks &rec_locks, const Lock *wait_lock, ulint heap_no, bool *multi) const noexcept {
  ut_ad(owns_page(wait_lock->page_id()));

  Blockers b;

  for (auto lock : rec_locks) {
    if (lock == wait_lock) {
      break;
    } else if (lock->rec_is_nth_bit_set(heap_no) && wait_lock->has_to_wait_for(lock, heap_no)) {
      b.add(lock->m_trx);
    }
  }

  *multi = b.m_n > 1;

  return b.pick();
}

Lock *LockSys::rec_create(
  LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, const Trx *trx
) noexcept {
  ut_ad(owns_page(block->get_page_id()));

  auto page = block->m_frame;
  const auto n_bits = page_dir_get_n_heap(page);

  return rec_create_low(block->get_page_id(), type_mode, heap_no, n_bits, index, const_cast<Trx *>(trx));
}

DbErr LockSys::rec_enqueue_waiting(
  LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
) noexcept {
  ut_ad(owns_page(block->get_page_id()));

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
  (void)rec_create(LockMode(type_mode | LOCK_WAIT), block, heap_no, index, trx);

  return enter_lock_wait(trx, thr);
}

Lock *LockSys::rec_add_to_queue(
  LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, const Trx *trx
) noexcept {
  ut_ad(owns_page(block->get_page_id()));

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
    auto other_lock = rec_other_has_expl_req(block->get_page_id(), mode, 0, LOCK_WAIT, heap_no, trx);
    ut_a(other_lock == nullptr);
  }
#endif /* UNIV_DEBUG */

  type_mode = LockMode(type_mode | LOCK_REC);

  /* If rec is the supremum record, then we can reset the gap bit, as
  all locks on the supremum are automatically of the gap type, and we
  try to avoid unnecessary memory consumption of a new record lock
  struct for a gap type lock */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(Lock_mode_type(type_mode) & LOCK_REC_NOT_GAP));

    /* There should never be LOCK_REC_NOT_GAP on a supremum
    record, but let us play safe */

    type_mode = LockMode(type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP));
  }

  /* Look for a waiting lock request on the same record or on a gap */

  auto q = rec_queue(block->get_page_id());

  if (q != nullptr) {
    for (auto lock : *q) {
      if (lock->is_waiting() && lock->rec_is_nth_bit_set(heap_no)) {
        return rec_create(type_mode, block, heap_no, index, trx);
      }
    }
  }

  if (likely(!(type_mode & LOCK_WAIT))) {

    /* Look for a similar record lock on the same page:
    if one is found and there are no waiting lock requests,
    we can just set the bit */

    auto lock = q != nullptr ? q->front() : nullptr;

    lock = rec_find_similar_on_page(type_mode, heap_no, lock, trx);

    if (likely(lock != nullptr)) {

      /* The struct may be ahead of other locks in the queue: no lock waits
      on the record (checked above), so no waiting lock gets a new granted
      lock ahead of it (the completeness of the deadlock check relies on
      it, see lock0deadlock.h). */
      ut_ad(q == nullptr || std::none_of(q->begin(), q->end(), [heap_no](const Lock *l) {
        return l->is_waiting() && l->rec_is_nth_bit_set(heap_no);
      }));

      lock->rec_set_nth_bit(heap_no);

      return lock;
    }
  }

  return rec_create(type_mode, block, heap_no, index, trx);
}

bool LockSys::rec_lock_fast(
  bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx
) noexcept {
  ut_ad(owns_page(block->get_page_id()));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  auto q = rec_queue(block->get_page_id());

  if (q == nullptr) {

    if (!impl) {
      (void)rec_create(mode, block, heap_no, index, trx);
    }

    return true;
  }

  auto lock = q->front();

  if (lock->next() != nullptr) {

    return false;
  }

  if (lock->m_trx != trx || lock->m_type_mode != (mode | LOCK_REC) || lock->rec_get_n_bits() <= heap_no) {

    return false;
  }

  if (!impl) {
    /* If the nth bit of the record lock is already set then we
    do not set a new lock bit, otherwise we do set */

    if (!lock->rec_is_nth_bit_set(heap_no)) {
      /* The only lock of the queue, not waiting: no waiter gets a lock ahead. */
      ut_ad(!lock->is_waiting());
      lock->rec_set_nth_bit(heap_no);
    }
  }

  return true;
}

DbErr LockSys::rec_lock_slow(
  bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
) noexcept {
  ut_ad(owns_page(block->get_page_id()));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  DbErr err;

  if (rec_has_expl(block->get_page_id(), mode, heap_no, trx)) {
    /* The trx already has a strong enough lock on rec: do
    nothing */

    err = DB_SUCCESS;
  } else if (rec_other_has_conflicting(block->get_page_id(), LockMode(mode), heap_no, trx)) {

    /* If another transaction has a non-gap conflicting request in
    the queue, as this transaction does not have a lock strong
    enough already granted on the record, we have to wait. */

    err = rec_enqueue_waiting(mode, block, heap_no, index, trx, thr);

  } else {

    if (!impl) {
      /* Set the requested lock on the record */

      (void)rec_add_to_queue(LockMode(LOCK_REC | mode), block, heap_no, index, trx);
    }

    err = DB_SUCCESS;
  }

  return err;
}

DbErr LockSys::rec_lock(
  bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
) noexcept {
  ut_ad(owns_page(block->get_page_id()));

  if (rec_lock_fast(impl, mode, block, heap_no, index, trx)) {

    /* We try a simplified and faster subroutine for the most common cases */

    return DB_SUCCESS;

  } else {
    return rec_lock_slow(impl, mode, block, heap_no, index, trx, thr);
  }
}

bool LockSys::rec_has_to_wait_in_queue(const Rec_locks &rec_locks, Lock *waiting_lock, ulint heap_no) const noexcept {
  ut_ad(owns_page(waiting_lock->page_id()));
  ut_ad(waiting_lock->is_waiting());
  ut_ad(waiting_lock->type() == LOCK_REC);
  ut_ad(heap_no == waiting_lock->rec_find_set_bit());

  for (auto lock = rec_locks.front(); lock != waiting_lock; lock = lock->next()) {

    ut_ad(lock->type() == LOCK_REC);
    ut_ad(lock->page_id() == waiting_lock->page_id());

    if (heap_no < lock->rec_get_n_bits() && lock->rec_is_nth_bit_set(heap_no) && waiting_lock->has_to_wait_for(lock, heap_no)) {
      return true;
    }
  }

  return false;
}

void LockSys::rec_cancel(Lock *lock) noexcept {
  ut_ad(lock->type() == LOCK_REC);
  ut_ad(owns_page(lock->page_id()));

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
  ut_ad(owns_page(lock->page_id()));

  auto &rec_locks = *rec_queue(lock->page_id());

  ut_a(!rec_locks.empty());

  rec_locks.remove(lock);

  const bool rec_locks_empty = rec_locks.empty();

  if (rec_locks_empty) {
    rec_queue_erase(lock->page_id());
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
  ut_ad(rec_locks.empty() || owns_page(rec_locks.front()->page_id()));

  /* Pass 1: the waiting locks, in queue order, and the records they wait for.
  Scratch space of the thread, kept from pass to pass: no allocation on the
  release path. A pass does not nest (the grants it makes only wake their
  threads, the deadlock checks run after it). */
  thread_local std::vector<std::pair<Lock *, ulint>> waiting;
  thread_local std::vector<ulint> heaps;
  thread_local std::vector<RecSeen> seen;

  waiting.clear();
  heaps.clear();

  for (auto lock : rec_locks) {
    if (lock->is_waiting()) {
      const auto heap_no = lock->rec_find_set_bit();

      ut_ad(heap_no != ULINT_UNDEFINED);
      waiting.emplace_back(lock, heap_no);
      heaps.push_back(heap_no);
    }
  }

  if (waiting.empty()) {
    return;
  } else if (waiting.size() == 1 && plan == nullptr) {
    /* The common case, one waiter: only the locks on its record matter. */
    rec_grant_pass_on(rec_locks, waiting.front().second);
    return;
  }

  std::sort(heaps.begin(), heaps.end());
  heaps.erase(std::unique(heaps.begin(), heaps.end()), heaps.end());

  seen.assign(heaps.size(), RecSeen{});

  auto seen_of = [&](ulint heap_no) -> RecSeen * {
    auto it = std::lower_bound(heaps.begin(), heaps.end(), heap_no);
    return it != heaps.end() && *it == heap_no ? &seen[it - heaps.begin()] : nullptr;
  };

  /* Pass 2: in queue order, decide each waiting lock against the locks
  ahead of it on its record, then add each lock to the records it covers
  that some lock waits for. */
  size_t next_wait{};

  for (auto lock : rec_locks) {
    if (next_wait < waiting.size() && waiting[next_wait].first == lock) {
      const auto heap_no = waiting[next_wait++].second;
      auto s = seen_of(heap_no);

      bool multi{};
      auto blocker = s->blocker(lock, heap_no == PAGE_HEAP_NO_SUPREMUM, &multi);

      grant_or_edge(lock, blocker, multi, plan);

      s->add(lock);

    } else if (heaps.size() <= 8) {
      for (size_t i{}; i < heaps.size(); ++i) {
        if (lock->rec_is_nth_bit_set(heaps[i])) {
          seen[i].add(lock);
        }
      }

    } else {
      for (auto h = lock->rec_find_set_bit(); h != ULINT_UNDEFINED; h = lock->rec_find_next_set_bit(h + 1)) {
        if (auto s = seen_of(h); s != nullptr) {
          s->add(lock);
        }
      }
    }
  }
}

void LockSys::rec_grant_pass_on(Rec_locks &rec_locks, ulint heap_no, GrantPlan *plan) noexcept {
  ut_ad(rec_locks.empty() || owns_page(rec_locks.front()->page_id()));

  RecSeen seen;
  const bool sup = heap_no == PAGE_HEAP_NO_SUPREMUM;

  for (auto lock : rec_locks) {
    if (!lock->rec_is_nth_bit_set(heap_no)) {
      continue;
    }

    if (lock->is_waiting()) {
      bool multi{};
      auto blocker = seen.blocker(lock, sup, &multi);

      grant_or_edge(lock, blocker, multi, plan);
    }

    seen.add(lock);
  }
}

void LockSys::rec_discard(Lock *in_lock) noexcept {
  ut_ad(in_lock->type() == LOCK_REC);
  ut_ad(owns_page(in_lock->page_id()));

  auto &rec_locks = *rec_queue(in_lock->page_id());

  /* Other locks on the page stay: drop the page entry only with its last lock. */
  rec_locks.remove(in_lock);

  if (rec_locks.empty()) {
    rec_queue_erase(in_lock->page_id());
  }

  auto trx = in_lock->m_trx;

  trx_mutex_enter(trx);
  trx->m_trx_locks.remove(in_lock);
  trx_mutex_exit(trx);
}

void LockSys::rec_free_all_from_discard_page(PageId page_id) noexcept {
  ut_ad(owns_page(page_id));

  if (auto q = rec_queue(page_id); q != nullptr) {
    auto lock = q->front();

    while (lock != nullptr) {
      ut_ad(lock->rec_find_set_bit() == ULINT_UNDEFINED);
      ut_ad(!lock->is_waiting());

      auto next_lock = lock->next();

      rec_discard(lock);

      lock = next_lock;
    }
  }
}

void LockSys::rec_reset_and_release_wait(PageId page_id, ulint heap_no) noexcept {
  ut_ad(owns_page(page_id));

  if (auto q = rec_queue(page_id); q != nullptr) {
    for (auto lock : (*q)) {
      if (lock->rec_is_nth_bit_set(heap_no)) {

        if (lock->is_waiting()) {
          rec_cancel(lock);
        } else {
          lock->rec_reset_nth_bit(heap_no);
        }
      }
    }
  }
}

void LockSys::rec_inherit_to_gap(const BufBlock *heir_block, const BufBlock *block, ulint heir_heap_no, ulint heap_no) noexcept {
  ut_ad(owns_page(heir_block->get_page_id()) && owns_page(block->get_page_id()));

  /* If session is using READ COMMITTED isolation level, we do not want locks set by an UPDATE or a DELETE to be inherited as gap type locks.
  But we DO want S-locks set by a consistency constraint to be inherited also then. */
  if (auto q = rec_queue(block->get_page_id()); q != nullptr) {
    for (auto lock : (*q)) {
      if (lock->rec_is_nth_bit_set(heap_no) && !lock->rec_is_insert_intention() &&
          !(lock->m_trx->m_isolation_level == TRX_ISO_READ_COMMITTED && lock->mode() == LOCK_X)) {
        (void)rec_add_to_queue(
          LockMode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), heir_block, heir_heap_no, lock->rec_index(), lock->m_trx
        );
      }
    }
  }
}

void LockSys::rec_inherit_to_gap_if_gap_lock(const BufBlock *block, ulint heir_heap_no, ulint heap_no) noexcept {
  ut_ad(owns_page(block->get_page_id()));

  if (auto q = rec_queue(block->get_page_id()); q != nullptr) {
    for (auto lock : (*q)) {
      if (lock->rec_is_nth_bit_set(heap_no) && !lock->rec_is_insert_intention() &&
          (heap_no == PAGE_HEAP_NO_SUPREMUM || !lock->rec_is_not_gap())) {
        (void)rec_add_to_queue(
          LockMode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), block, heir_heap_no, lock->rec_index(), lock->m_trx
        );
      }
    }
  }
}

void LockSys::rec_move(
  const BufBlock *receiver, const BufBlock *donator, ulint receiver_heap_no, ulint donator_heap_no
) noexcept {
    ut_ad(owns_page(receiver->get_page_id()) && owns_page(donator->get_page_id()));

  IF_DEBUG({
    if (auto rq = rec_queue(receiver->get_page_id()); rq != nullptr) {
      ut_ad(rec_exists(*rq, receiver_heap_no) == nullptr);
    }
  })

  if (auto q = rec_queue(donator->get_page_id()); q != nullptr) {
    /* rec_add_to_queue() may add the receiver page to a map: a rehash does
    not invalidate the reference to the donator's list. */
    auto &locks = *q;

    for (auto lock : locks) {
      if (lock->rec_is_nth_bit_set(donator_heap_no)) {
        const auto type_mode = lock->m_type_mode;

        lock->rec_reset_nth_bit(donator_heap_no);

        if (unlikely(type_mode & LOCK_WAIT)) {
          reset_wait(lock);
        }

        /* Note that we FIRST reset the bit, and then set the lock:
        the function works also if donator == receiver */

        (void)rec_add_to_queue(type_mode, receiver, receiver_heap_no, lock->m_rec.m_index, lock->m_trx);
      }
    }

    ut_ad(rec_exists(locks, donator_heap_no) == nullptr);
  }
}

void LockSys::move_reorganize_page(const BufBlock *block, const BufBlock *oblock) noexcept {
  UT_LIST_BASE_NODE_T(Lock, m_trx_locks) old_locks;

  {
  PageLatch latch(*this, block->get_page_id());

  auto q = rec_queue(block->get_page_id());

  if (q == nullptr) {
    return;
  }

  auto heap = mem_heap_create(256);

  /* Copy first all the locks on the page to heap and reset the
  bitmaps in the original locks; chain the copies of the locks
  using the trx_locks field in them. */

  for (auto lock : *q) {
    /* Make a copy of the lock */
    auto old_lock = lock->rec_clone(heap);

    old_locks.push_back(old_lock);

    /* Reset bitmap of lock */
    lock->rec_bitmap_reset();

    if (lock->is_waiting()) {
      reset_wait(lock);
    }
  }

  for (auto lock : old_locks) {
    /* NOTE: we copy also the locks set on the infimum and supremum of the page; the infimum may carry locks if an
    update of a record is occurring on the page, and its locks were temporarily stored on the infimum */
    PageCursor cur1;
    PageCursor cur2;

    cur1.set_before_first(block);
    cur2.set_before_first(oblock);

    /* Set locks according to old locks */
    for (;;) {
      ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), cur2.get_rec().get_data_size()));

      const auto old_heap_no = cur2.get_rec().get_heap_no();
      const auto new_heap_no = cur1.get_rec().get_heap_no();

      if (lock->rec_is_nth_bit_set(old_heap_no)) {

        /* Clear the bit in old_lock. */
        ut_d(lock->rec_reset_nth_bit(old_heap_no));

        /* NOTE that the old lock bitmap could be too
        small for the new heap number! */

        (void)rec_add_to_queue(lock->m_type_mode, block, new_heap_no, lock->m_rec.m_index, lock->m_trx);
      }

      if (unlikely(new_heap_no == PAGE_HEAP_NO_SUPREMUM)) {
        ut_ad(old_heap_no == PAGE_HEAP_NO_SUPREMUM);
        break;
      }

      cur1.move_to_next();
      cur2.move_to_next();
    }

#ifdef UNIV_DEBUG
    {
      const auto i = lock->rec_find_set_bit();

      /* Check that all locks were moved. */
      if (unlikely(i != ULINT_UNDEFINED)) {
        log_fatal(std::format("move_reorganize_page(): {} not moved in {}", i, lock->to_string(m_sys)));
      }
    }
#endif /* UNIV_DEBUG */
  }

  mem_heap_free(heap);
  }

  deadlock_check_moved();

  ut_ad(rec_validate_page(block->get_page_id()));
}

void LockSys::move_rec_list_end(const BufBlock *new_block, const BufBlock *block, const Rec rec) noexcept {
  {
  PageLatch latch(*this, new_block->get_page_id(), block->get_page_id());

  /* Note: when we move locks from record to record, waiting locks
  and possible granted gap type locks behind them are enqueued in
  the original order, because new elements are inserted to a hash
  table to the end of the hash chain, and lock_rec_add_to_queue
  does not reuse locks if there are waiters in the queue. */

  if (auto q = rec_queue(block->get_page_id()); q != nullptr) {

    for (auto lock : (*q)) {
      PageCursor cur1;
      PageCursor cur2;
      const auto type_mode = lock->m_type_mode;

      cur1.position(rec, block);

      if (cur1.is_before_first()) {
        cur1.move_to_next();
      }

      cur2.set_before_first(new_block);
      cur2.move_to_next();

      /* Copy lock requests on user records to new page and
      reset the lock bits on the old */

      while (!cur1.is_after_last()) {
        auto heap_no = cur1.get_rec().get_heap_no();

        ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), cur2.get_rec().get_data_size()));

        if (lock->rec_is_nth_bit_set(heap_no)) {
          lock->rec_reset_nth_bit(heap_no);

          if (unlikely(type_mode & LOCK_WAIT)) {
            reset_wait(lock);
          }

          heap_no = cur2.get_rec().get_heap_no();

          (void)rec_add_to_queue(type_mode, new_block, heap_no, lock->m_rec.m_index, lock->m_trx);
        }

        cur1.move_to_next();
        cur2.move_to_next();
      }
    }
  }
  }

  deadlock_check_moved();

  ut_ad(rec_validate_page(block->get_page_id()));
  ut_ad(rec_validate_page(new_block->get_page_id()));
}

void LockSys::move_rec_list_start(const BufBlock *new_block, const BufBlock *block, const Rec rec, const Rec old_end) noexcept {
  ut_ad(block->m_frame == rec.page_align());
  ut_ad(new_block->m_frame == old_end.page_align());

  {
  PageLatch latch(*this, new_block->get_page_id(), block->get_page_id());

  if (auto q = rec_queue(block->get_page_id()); q != nullptr) {

    for (auto lock : (*q)) {
      PageCursor cur1;
      PageCursor cur2;
      const auto type_mode = lock->m_type_mode;

      cur1.set_before_first(block);
      cur1.move_to_next();

      cur2.position(old_end, new_block);
      cur2.move_to_next();

      /* Copy lock requests on user records to new page and
      reset the lock bits on the old */

      while (cur1.get_rec() != rec) {

        auto heap_no = cur1.get_rec().get_heap_no();
        ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), cur2.get_rec().get_data_size()));

        if (lock->rec_is_nth_bit_set(heap_no)) {
          lock->rec_reset_nth_bit(heap_no);

          if (unlikely(type_mode & LOCK_WAIT)) {
            reset_wait(lock);
          }

          heap_no = cur2.get_rec().get_heap_no();

          (void)rec_add_to_queue(type_mode, new_block, heap_no, lock->m_rec.m_index, lock->m_trx);
        }

        cur1.move_to_next();
        cur2.move_to_next();
      }

      IF_DEBUG(if (page_rec_is_supremum(rec)) {
        for (auto i = PAGE_HEAP_NO_USER_LOW; i < lock->rec_get_n_bits(); ++i) {
          if (unlikely(lock->rec_is_nth_bit_set(i))) {
            log_fatal("move_rec_list_start(): {} not moved in {}", i, lock->to_string(m_sys));
          }
        }
      })
    }
  }
  }

  deadlock_check_moved();

  ut_ad(rec_validate_page(block->get_page_id()));
}

void LockSys::update_split_right(const BufBlock *right_block, const BufBlock *left_block) noexcept {
  const ulint heap_no = get_min_heap_no(right_block);

  {
    PageLatch latch(*this, right_block->get_page_id(), left_block->get_page_id());

    /* Move the locks on the supremum of the left page to the supremum
    of the right page */

    rec_move(right_block, left_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

    /* Inherit the locks to the supremum of left page from the successor
    of the infimum on right page */

    rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);
  }

  deadlock_check_moved();
}

void LockSys::update_merge_right(const BufBlock *right_block, const Rec orig_succ, const BufBlock *left_block) noexcept {
  {
    PageLatch latch(*this, right_block->get_page_id(), left_block->get_page_id());

    /* Inherit the locks from the supremum of the left page to the
    original successor of infimum on the right page, to which the left
    page was merged */

    rec_inherit_to_gap(right_block, left_block, page_rec_get_heap_no(orig_succ), PAGE_HEAP_NO_SUPREMUM);

    /* Reset the locks on the supremum of the left page, releasing
    waiting transactions */

    rec_reset_and_release_wait(left_block->get_page_id(), PAGE_HEAP_NO_SUPREMUM);

    rec_free_all_from_discard_page(left_block->get_page_id());
  }

  deadlock_check_moved();
}

void LockSys::update_root_raise(const BufBlock *block, const BufBlock *root) noexcept {
  {
    PageLatch latch(*this, block->get_page_id(), root->get_page_id());

    /* Move the locks on the supremum of the root to the supremum of block */
    rec_move(block, root, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);
  }

  deadlock_check_moved();
}

void LockSys::update_copy_and_discard(const BufBlock *new_block, const BufBlock *block) noexcept {
  {
    PageLatch latch(*this, new_block->get_page_id(), block->get_page_id());

    /* Move the locks on the supremum of the old page to the supremum
    of new_page */

    rec_move(new_block, block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);
    rec_free_all_from_discard_page(block->get_page_id());
  }

  deadlock_check_moved();
}

void LockSys::update_split_left(const BufBlock *right_block, const BufBlock *left_block) noexcept {
  const ulint heap_no = get_min_heap_no(right_block);

  PageLatch latch(*this, right_block->get_page_id(), left_block->get_page_id());

  /* Inherit the locks to the supremum of the left page from the
  successor of the infimum on the right page */

  rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);
}

void LockSys::update_merge_left(const BufBlock *left_block, const Rec orig_pred, const BufBlock *right_block) noexcept {
  ut_ad(left_block->m_frame == orig_pred.page_align());

  {
    PageLatch latch(*this, left_block->get_page_id(), right_block->get_page_id());

    const auto left_next_rec = page_rec_get_next_const(orig_pred);

    if (!page_rec_is_supremum(left_next_rec)) {

      /* Inherit the locks on the supremum of the left page to the
      first record which was moved from the right page */

      rec_inherit_to_gap(left_block, left_block, page_rec_get_heap_no(left_next_rec), PAGE_HEAP_NO_SUPREMUM);

      /* Reset the locks on the supremum of the left page,
      releasing waiting transactions */

      rec_reset_and_release_wait(left_block->get_page_id(), PAGE_HEAP_NO_SUPREMUM);
    }

    /* Move the locks from the supremum of right page to the supremum
    of the left page */

    rec_move(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

    rec_free_all_from_discard_page(right_block->get_page_id());
  }

  deadlock_check_moved();
}

void LockSys::rec_reset_and_inherit_gap_locks(
  const BufBlock *heir_block, const BufBlock *block, ulint heir_heap_no, ulint heap_no
) noexcept {
  PageLatch latch(*this, heir_block->get_page_id(), block->get_page_id());

  rec_reset_and_release_wait(heir_block->get_page_id(), heir_heap_no);

  rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);
}

void LockSys::update_discard(const BufBlock *heir_block, ulint heir_heap_no, const BufBlock *block) noexcept {
  const auto page = block->m_frame;

  PageLatch latch(*this, heir_block->get_page_id(), block->get_page_id());

  if (rec_queue(block->get_page_id()) == nullptr) {
    /* No locks exist on page, nothing to do */
    return;
  }

  /* Inherit all the locks on the page to the record and reset all the locks on the page */

  ulint heap_no;
  auto rec_ptr{page + PAGE_INFIMUM};
  Rec rec(rec_ptr);

  do {
    heap_no = rec.get_heap_no();

    rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

    rec_reset_and_release_wait(block->get_page_id(), heap_no);

    rec_ptr = page + rec.get_next_offs();
    rec = Rec(rec_ptr);

  } while (heap_no != PAGE_HEAP_NO_SUPREMUM);

  rec_free_all_from_discard_page(block->get_page_id());
}

void LockSys::update_insert(const BufBlock *block, const Rec rec) noexcept {
  ut_ad(block->m_frame == rec.page_align());

  /* Inherit the gap-locking locks for rec, in gap mode, from the next
  record */

  const auto receiver_heap_no = rec.get_heap_no();
  const auto donator_heap_no = page_rec_get_heap_no(page_rec_get_next_low(rec));

  PageLatch latch(*this, block->get_page_id());

  rec_inherit_to_gap_if_gap_lock(block, receiver_heap_no, donator_heap_no);
}

void LockSys::update_delete(const BufBlock *block, const Rec rec) noexcept {
  const page_t *page = block->m_frame;

  ut_ad(page == rec.page_align());

  const auto heap_no = rec.get_heap_no();
  const auto next_heap_no = page_rec_get_heap_no(page + rec.get_next_offs());

  PageLatch latch(*this, block->get_page_id());

  /* Let the next record inherit the locks from rec, in gap mode */

  rec_inherit_to_gap(block, block, next_heap_no, heap_no);

  /* Reset the lock bits on rec and release waiting transactions */

  rec_reset_and_release_wait(block->get_page_id(), heap_no);
}

void LockSys::rec_store_on_page_infimum(const BufBlock *block, const Rec rec) noexcept {
  const auto heap_no = page_rec_get_heap_no(rec);

  ut_ad(block->m_frame == rec.page_align());

  {
    PageLatch latch(*this, block->get_page_id());

    rec_move(block, block, PAGE_HEAP_NO_INFIMUM, heap_no);
  }

  deadlock_check_moved();
}

void LockSys::rec_restore_from_page_infimum(const BufBlock *block, const Rec rec, const BufBlock *donator) noexcept {
  const auto heap_no = page_rec_get_heap_no(rec);

  {
    PageLatch latch(*this, block->get_page_id(), donator->get_page_id());

    rec_move(block, donator, heap_no, PAGE_HEAP_NO_INFIMUM);
  }

  deadlock_check_moved();
}

void LockSys::rec_unlock(Trx *trx, const BufBlock *block, const Rec rec, LockMode LockMode) noexcept {
  ut_ad(block->m_frame == rec.page_align());

  const auto heap_no = page_rec_get_heap_no(rec);

  PageLatch latch(*this, block->get_page_id());

  Lock *release_lock{};

  /* Find the last lock with the same LockMode and transaction from the record. */
  auto q = rec_queue(block->get_page_id());
  ut_a(q != nullptr);

  for (auto lock : *q) {
    if (lock->rec_is_nth_bit_set(heap_no)) {
      if (lock->m_trx == trx && lock->mode() == LockMode) {
        release_lock = lock;
        ut_a(!lock->is_waiting());
      }
    }
  }

  /* If a record lock is found, release the record lock */
  if (likely(release_lock != nullptr)) {
    release_lock->rec_reset_nth_bit(heap_no);
  } else {
    log_err(std::format("Unlock row could not find a {} mode lock on the record", to_int(LockMode)));
    return;
  }

  /* Check if we can now grant waiting lock requests */
  rec_grant_pass_on(*q, heap_no);
}

