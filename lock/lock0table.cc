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

/** @file lock/lock0table.cc
The transaction lock system: the table lock queues. The latching is in the LATCHING
comment of include/lock0lock.h.
*******************************************************/

#include "lock0priv.h"

Table *LockSys::get_src_table(Trx *trx, Table *dest, LockMode *mode) noexcept {
  /* DDL, rare: the table queues of any shard. */
  Exclusive x(*this);

  Table *src{};

  *mode = LOCK_NONE;

  for (auto lock : trx->m_trx_locks) {
    if (!(lock->type() & LOCK_TABLE)) {
      /* We are only interested in table locks. */
      continue;
    }

    auto &tab_lock = lock->m_table;

    if (dest == tab_lock.m_table) {
      /* We are not interested in the destination table. */
      continue;
    } else if (src == nullptr) {
      /* This presumably is the source table. */
      src = tab_lock.m_table;

      if (src->m_locks.size() != 1 || src->m_locks.front() != lock) {
        /* We only support the case when
        there is only one lock on this table. */
        return nullptr;
      }
    } else if (src != tab_lock.m_table) {
      /* The transaction is locking more than
      two tables (src and dest): abort */
      return nullptr;
    }

    /* Check that the source table is locked by LOCK_IX or LOCK_IS. */
    const auto LockMode = lock->mode();

    if (LockMode == LOCK_IX || LockMode == LOCK_IS) {
      if (*mode != LOCK_NONE && *mode != LockMode) {
        /* There are multiple locks on src. */
        return nullptr;
      }

      *mode = LockMode;
    }
  }

  if (src == nullptr) {
    /* No source table lock found: flag the situation to caller */
    src = dest;
  }

  return src;
}

bool LockSys::is_table_exclusive(Table *table, Trx *trx) noexcept {
  bool ok{false};

  TableLatch latch(*this, table);

  for (auto lock : table->m_locks) {
    if (lock->m_trx != trx) {
      /* A lock on the table is held by some other transaction. */
      ok = false;
      break;

    } else if (lock->type() != LOCK_TABLE) {
      /* We are interested in table locks only. */
      continue;

    } else if (lock->mode() == LOCK_IX) {
      ok = true;

    } else {
      /* Other table locks than LOCK_IX are not allowed. */
      ok = false;
      break;
    }
  }

  return ok;
}

Lock *LockSys::table_has(Trx *trx, Table *table, LockMode mode) noexcept {
  /* Look for stronger locks the same trx already has on the table: in its
  own lock list, with its mutex (no table latch needed: only trx adds table
  locks for itself; DDL removes them with the lock system latched X). */
  Lock *found{};

  trx_mutex_enter(trx);

  for (auto lock = UT_LIST_GET_LAST(trx->m_trx_locks); lock != nullptr; lock = UT_LIST_GET_PREV(m_trx_locks, lock)) {

    if (lock->type() == LOCK_TABLE && lock->m_table.m_table == table && !lock->is_waiting() &&
        Lock::mode_stronger_or_eq(lock->mode(), mode)) {

      /* The same trx already has locked the table in
      a mode stronger or equal to the mode given */

      found = lock;
      break;
    }
  }

  trx_mutex_exit(trx);

  return found;
}

Lock *LockSys::table_create(Table *table, LockMode type_mode, Trx *trx) noexcept {
  ut_ad(owns_table(table));

  /* The lock heap and the lock list of trx: its mutex. */
  trx_mutex_enter(trx);

  /* The lock heap of trx is emptied when its list is (LockSys::release()
  leaves a small heap for here): no lock of trx is in use then (a lock is
  only created for trx from a lock of trx in its list, or by trx). */
  if (trx->m_trx_locks.empty()) {
    mem_heap_empty(trx->m_lock_heap);
  }

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->m_lock_heap, sizeof(Lock)));

  trx->m_trx_locks.push_back(lock);
  trx->m_lock_hint.store(Trx::LOCK_HINT_TABLE | table_shard_no(table), std::memory_order_relaxed);

  lock->m_trx = trx;
  lock->m_type_mode = LockMode(Lock_mode_type(type_mode) | LOCK_TABLE);

  lock->m_table.m_table = table;

  /* Appended, never inserted (lock0deadlock.h relies on it). */
  table->m_locks.push_back(lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock->set_trx_wait(trx);

    /* The edge of the waits-for graph, see rec_create_low(). */
    Blockers b;

    for (auto l : table->m_locks) {
      if (l == lock) {
        break;
      } else if (lock->has_to_wait_for(l, ULINT_UNDEFINED)) {
        b.add(l->m_trx);
      }
    }

    trx->m_blocking_multi.store(b.m_n > 1, std::memory_order_relaxed);
    /* Sequentially consistent: see rec_create_low(). */
    trx->m_blocking_trx.store(const_cast<Trx *>(b.pick()), std::memory_order_seq_cst);
  }

  trx_mutex_exit(trx);

  return lock;
}

void LockSys::table_remove_low(Lock *lock, bool in_trx) noexcept {
  auto trx = lock->m_trx;
  auto table = lock->m_table.m_table;

  ut_ad(owns_table(table));

  if (in_trx) {
    trx_mutex_enter(trx);
    trx->m_trx_locks.remove(lock);
    trx_mutex_exit(trx);
  }

  table->m_locks.remove(lock);
}

[[nodiscard]] DbErr LockSys::table_enqueue_waiting(LockMode mode, Table *table, Trx *trx, QueThr *thr) noexcept {
  ut_ad(owns_table(table));

  switch (trx->get_dict_operation()) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      log_err(std::format(
        "A table lock wait happens"
        " in a dictionary operation! Table name ",
        table->m_name
      ));
      log_err("Submit a detailed bug report, check the InnoDB website for details");
  }

  /* Enqueue the lock request that will wait to be granted */

  (void)table_create(table, LockMode(mode | LOCK_WAIT), trx);

  return enter_lock_wait(trx, thr);
}

Lock *LockSys::table_other_has_incompatible(Trx *trx, ulint wait, Table *table, LockMode mode) noexcept {
  ut_ad(owns_table(table));

  auto lock = table->m_locks.back();

  while (lock != nullptr) {

    if (lock->m_trx != trx && !Lock::mode_compatible(lock->mode(), mode) && (wait || !lock->is_waiting())) {

      return lock;
    }

    lock = UT_LIST_GET_PREV(m_table.m_locks, lock);
  }

  return nullptr;
}

DbErr LockSys::lock_table(ulint flags, Table *table, LockMode mode, QueThr *thr) noexcept {
  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  ut_a(flags == 0);

  return table_lock_for(thr_get_trx(thr), table, mode, thr);
}

DbErr LockSys::table_lock_for(Trx *trx, Table *table, LockMode mode, QueThr *thr) noexcept {
  DbErr err{DB_SUCCESS};

  {
    TableLatch latch(*this, table);

    /* Look for stronger locks the same trx already has on the table */

    if (table_has(trx, table, mode)) {

      return DB_SUCCESS;
    }

    /* We have to check if the new lock is compatible with any locks
    other transactions have in the table lock queue. */

    if (table_other_has_incompatible(trx, LOCK_WAIT, table, mode)) {

      /* Another trx has a request on the table in an incompatible
      mode: this trx may have to wait */

      err = table_enqueue_waiting(mode, table, trx, thr);

    } else {
      (void)table_create(table, mode, trx);
    }
  }

  return deadlock_after_wait(err);
}

bool LockSys::table_has_to_wait_in_queue(Lock *wait_lock) noexcept {
  ut_ad(wait_lock->is_waiting());
  ut_ad(owns_table(wait_lock->m_table.m_table));

  auto table = wait_lock->m_table.m_table;

  for (auto lock : table->m_locks) {
    if (lock == wait_lock) {
      return false;
    }

    if (wait_lock->has_to_wait_for(lock, ULINT_UNDEFINED)) {

      return true;
    }
  }

  return false;
}

void LockSys::table_dequeue(Lock *in_lock, bool in_trx) noexcept {
  ut_a(in_lock->type() == LOCK_TABLE);

  auto table = in_lock->m_table.m_table;

  ut_ad(owns_table(table));

  table_remove_low(in_lock, in_trx);

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  table_grant_pass(table);
}

void LockSys::table_grant_pass(Table *table, GrantPlan *plan) noexcept {
  ut_ad(owns_table(table));

  TableSeen seen;

  for (auto lock : table->m_locks) {
    if (lock->is_waiting()) {
      bool multi{};
      auto blocker = seen.blocker(lock, &multi);

      grant_or_edge(lock, blocker, multi, plan);
    }

    seen.add(lock);
  }
}

/* True if a lock mode is S or X */
#define IS_LOCK_S_OR_X(lock) (lock->mode() == LOCK_S || lock->mode() == LOCK_X)

void LockSys::remove_all_on_table_for_trx(Table *table, Trx *trx, bool remove_sx_locks) noexcept {
  /* The locks of trx in any shard: the lock system latched X (the lock list
  of trx can't change under it). */
  ut_ad(x_own());

  auto lock = trx->m_trx_locks.back();

  while (lock != nullptr) {
    auto prev_lock = UT_LIST_GET_PREV(m_trx_locks, lock);

    if (lock->type() == LOCK_REC && lock->rec_index()->m_table == table) {
      ut_a(!lock->is_waiting());

      /* Grant the waiters the lock blocked, if they can go now, and move
      the edges of the others: a bare discard would leave them waiting for
      nothing. */
      rec_dequeue_from_page(lock);

    } else if (lock->type() == LOCK_TABLE && lock->m_table.m_table == table && (remove_sx_locks || !IS_LOCK_S_OR_X(lock))) {

      ut_a(!lock->is_waiting());

      table_remove_low(lock);
      table_grant_pass(table);
    }

    lock = prev_lock;
  }
}

void LockSys::remove_all_on_table(Table *table, bool remove_sx_locks) noexcept {
  /* DDL: rare, and it spans the shards of the pages of the table. */
  Exclusive x(*this);

  auto lock = table->m_locks.front();

  while (lock != nullptr) {

    auto prev_lock = UT_LIST_GET_PREV(m_table.m_locks, lock);

    /* If we should remove all locks (remove_sx_locks
    is true), or if the lock is not table-level S or X lock,
    then check we are not going to remove a wait lock. */
    if (remove_sx_locks || !(lock->type() == LOCK_TABLE && IS_LOCK_S_OR_X(lock))) {

      // HACK: For testing
      if (lock->is_waiting()) {
        if (remove_sx_locks) {
          ut_error;
        } else {
          goto next;
        }
      }
    }

    remove_all_on_table_for_trx(table, lock->m_trx, remove_sx_locks);

    if (prev_lock == nullptr) {
      if (lock == table->m_locks.front()) {
        /* lock was not removed, pick its successor */
        lock = UT_LIST_GET_NEXT(m_table.m_locks, lock);
      } else {
        /* lock was removed, pick the first one */
        lock = table->m_locks.front();
      }
    } else if (UT_LIST_GET_NEXT(m_table.m_locks, prev_lock) != lock) {
      /* If lock was removed by lock_remove_all_on_table_for_trx() then pick the
      successor of prev_lock ... */
      lock = UT_LIST_GET_NEXT(m_table.m_locks, prev_lock);
    } else {
    next:
      /* ... otherwise pick the successor of lock. */
      lock = UT_LIST_GET_NEXT(m_table.m_locks, lock);
    }
  }

}

