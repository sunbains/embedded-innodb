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

/** @file lock/lock0priv.h
The lock system: what its source files share (lock0lock.cc, lock0rec.cc,
lock0table.cc, lock0wait.cc, lock0deadlock.cc): the compatibility and
strength matrices, the inline members of Lock, the conflict rule of record
locks and the summaries of the grant pass. Private to lock/.
*******************************************************/

#pragma once

#include "lock0lock.h"
#include "log0recv.h"

#include "trx0purge.h"
#include "ut0ucode.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstring>
#include <functional>
#include <new>
#include <thread>
#include <vector>

/** Safety margin when creating a new record lock: this many extra records
can be inserted to the page without need to create a lock with a bigger
bitmap */
constexpr ulint LOCK_PAGE_BITMAP_MARGIN = 64;

static const bool Lock_compatibility_matrix[4][4] = {
  /**         IS     IX       S     X   */
  /* IS */ {true, true, true, false},
  /* IX */ {true, true, false, false},
  /* S  */ {true, false, true, false},
  /* X  */ {false, false, false, false},
};

/**
 * Define the lock compatibility matrix in a bool.  The first line below
 * defines the diagonal entries. The following lines define the compatibility
 * for LOCK_IX, LOCK_S, since the matrix is symmetric. */

/* STRONGER-OR-EQUAL RELATION (mode1=row, mode2=column)
 *    IS IX S  X
 * IS +  -  -  -
 * IX +  +  -  -
 * S  +  -  +  -
 * X  +  +  +  +
 */

/** Define the stronger-or-equal lock relation in a ulint.  This relation
contains all pairs LK(mode1, mode2) where mode1 is stronger than or
equal to mode2. */
static const bool Lock_strength_matrix[4][4] = {
  /**         IS     IX       S     X    */
  /* IS */ {true, false, false, false},
  /* IX */ {true, true, false, false},
  /* S  */ {true, false, true, false},
  /* X  */ {true, true, true, true},
};

/* The lock system */

/**
 * @brief Validate the transaction state.
 *
 * @param[in] trx The transaction to validate.
 */
static inline void validate_trx_state(const Trx *trx) noexcept {
  /* The state changes with the trx mutex (C4 of a commit, lock0lock.h). */
  trx_mutex_enter(trx);
  const TrxStatus state = trx->m_conc_state;
  trx_mutex_exit(trx);

  switch (state) {
    case TRX_ACTIVE:
    case TRX_PREPARED:
    case TRX_COMMITTED_IN_MEMORY:
      break;
    default:
      ut_error;
  }
}

/** We store info on the latest deadlock error to this buffer. InnoDB
Monitor will then fetch it and print */

struct TableLockGetNode {
  /**
   * @brief Functor for accessing the embedded node within a table lock.
   *
   * @param[in] lock The lock to get the node from.
   *
   * @return The embedded node within the lock.
   */
  static const UtListNode<Lock> &get_node(const Lock &lock) { return lock.m_table.m_locks; }
};


trx_id_t Lock::trx_id() const noexcept {
  return m_trx->m_id;
}

void Lock::set_trx_wait(Trx *trx) noexcept {
  ut_ad(trx_mutex_own(trx));
  ut_ad(trx->m_wait_lock == nullptr);

  trx->m_wait_lock = this;
  m_type_mode = LockMode(Lock_mode_type(m_type_mode) | LOCK_WAIT);
}

void Lock::reset() noexcept {
  ut_ad(trx_mutex_own(m_trx));
  ut_ad(is_waiting());
  ut_ad(m_trx->m_wait_lock == this);

  /* Reset the back pointer in trx to this waiting lock request */

  m_type_mode = LockMode(Lock_mode_type(m_type_mode) & ~LOCK_WAIT);
  m_trx->m_wait_lock = nullptr;

  /* The wait ended: the transaction leaves the waits-for graph. */
  m_trx->m_blocking_multi.store(false, std::memory_order_relaxed);
  m_trx->m_blocking_trx.store(nullptr, std::memory_order_release);
}

bool Lock::mode_compatible(LockMode lhs, LockMode rhs) noexcept {
  ut_ad(lhs == LOCK_X || lhs == LOCK_S || lhs == LOCK_IX || lhs == LOCK_IS);
  ut_ad(rhs == LOCK_X || rhs == LOCK_S || rhs == LOCK_IX || rhs == LOCK_IS);

  return Lock_compatibility_matrix[lhs][rhs];
}

/**
 * @brief The conflict rule of record locks of two different transactions.
 *
 * @param[in] type_mode The precise mode of the request: LOCK_S or LOCK_X,
 *  possibly ORed with LOCK_GAP, LOCK_REC_NOT_GAP or LOCK_INSERT_INTENTION.
 * @param[in] held The precise mode of a lock ahead of it in the queue,
 *  granted or waiting.
 * @param[in] lock_is_on_supremum True if the record is the supremum: every
 *  lock on it is a gap lock.
 *
 * @return true if the request has to wait for the lock.
 */
static inline bool rec_mode_blocks(Lock_mode_type type_mode, Lock_mode_type held, bool lock_is_on_supremum) noexcept {
  if (Lock::mode_compatible(LockMode(LOCK_MODE_MASK & type_mode), LockMode(LOCK_MODE_MASK & held))) {
    return false;
  }

  /* We have somewhat complex rules when gap type record locks cause waits */

  if ((lock_is_on_supremum || (type_mode & LOCK_GAP)) && !(type_mode & LOCK_INSERT_INTENTION)) {

    /* Gap type locks without LOCK_INSERT_INTENTION flag do not need to wait for anything. This is because
    different users can have conflicting lock types on gaps. */

    return false;
  }

  if (!(type_mode & LOCK_INSERT_INTENTION) && (held & LOCK_GAP)) {

    /* Record lock (LOCK_ORDINARY or LOCK_REC_NOT_GAP does not need to wait for a gap type lock */

    return false;
  }

  if ((type_mode & LOCK_GAP) && (held & LOCK_REC_NOT_GAP)) {

    /* Lock on gap does not need to wait for a LOCK_REC_NOT_GAP type lock */

    return false;
  }

  /* No lock request needs to wait for an insert intention lock to be removed. This is ok since our
  rules allow conflicting locks on gaps. This eliminates a spurious deadlock caused by a next-key lock waiting
  for an insert intention lock; when the insert intention lock was granted, the insert deadlocked on
  the waiting next-key lock.

  Also, insert intention locks do not disturb each other. */

  return !(held & LOCK_INSERT_INTENTION);
}

bool Lock::rec_blocks(const Trx *trx, Lock_mode_type type_mode, bool lock_is_on_supremum) const noexcept {
  ut_ad(type() == LOCK_REC);

  return trx != m_trx && rec_mode_blocks(type_mode, m_type_mode, lock_is_on_supremum);
}

namespace {

/* The grant pass. A release removes a lock from a queue and then grants
every waiting lock that no lock ahead of it, of another transaction, blocks.
Asking that of each waiter by scanning the queue from its front (what
rec_has_to_wait_in_queue() does) costs O(W^2) for W waiters. Instead one
pass in queue order keeps, per record, a summary of the locks seen so far,
and a waiter is decided against the summary in O(1).

Whether a lock blocks a request depends only on the two precise modes, the
supremum, and whether the transactions differ (rec_mode_blocks()). So the
summary keeps the locks by class (their mode, S or X, and their gap, record
only and insert intention flags: 16 classes) and, per class, up to three
distinct transactions: a waiter is blocked by a class that conflicts with it
unless every lock of the class is its own (two distinct transactions are
enough to tell), and three tell whether it has more than one blocker (see
cls_add()). */

/** @return the class of a record lock mode, 0..15. */
inline uint32_t rec_cls(Lock_mode_type type_mode) noexcept {
  return ((type_mode & LOCK_MODE_MASK) == LOCK_X ? 1U : 0U) | ((type_mode & LOCK_GAP) ? 2U : 0U) |
         ((type_mode & LOCK_REC_NOT_GAP) ? 4U : 0U) | ((type_mode & LOCK_INSERT_INTENTION) ? 8U : 0U);
}

/** @return the precise mode of the class c. */
inline Lock_mode_type cls_mode(uint32_t c) noexcept {
  return Lock_mode_type(
    ((c & 1) ? LOCK_X : LOCK_S) | ((c & 2) ? LOCK_GAP : 0) | ((c & 4) ? LOCK_REC_NOT_GAP : 0) | ((c & 8) ? LOCK_INSERT_INTENTION : 0)
  );
}

/** rec_mode_blocks() by class: [supremum][request class] is the mask of the held classes that block it. */
struct ClsBlocks {
  ClsBlocks() noexcept {
    for (uint32_t sup{}; sup < 2; ++sup) {
      for (uint32_t req{}; req < 16; ++req) {
        m_mask[sup][req] = 0;

        for (uint32_t held{}; held < 16; ++held) {
          if (rec_mode_blocks(cls_mode(req), cls_mode(held), sup != 0)) {
            m_mask[sup][req] |= uint16_t(1U << held);
          }
        }
      }
    }
  }

  uint16_t m_mask[2][16];
};

const ClsBlocks cls_blocks;

/** The distinct transactions found to block a waiting lock: the edge of the
waits-for graph names one of them, preferably one that waits itself, so
that the one edge walk goes on through it (more cycles are found by the
fast walk, see lock0deadlock.h). */
struct Blockers {
  void add(const Trx *trx) noexcept {
    if (m_first == nullptr) {
      m_first = trx;
      m_n = 1;
    } else if (trx != m_first && m_n == 1) {
      m_n = 2;
    }

    if (m_waiting == nullptr && trx->m_blocking_trx.load(std::memory_order_relaxed) != nullptr) {
      m_waiting = trx;
    }
  }

  [[nodiscard]] const Trx *pick() const noexcept { return m_waiting != nullptr ? m_waiting : m_first; }

  const Trx *m_first{};
  const Trx *m_waiting{};

  /** 0, 1, or 2 for more than one. */
  uint32_t m_n{};
};

/** Note trx in the transactions of a class: up to three distinct ones. Three,
not two: one may be the waiter itself, which the waiter's blockers exclude,
and two others are needed to tell that it has several (m_blocking_multi,
which decides whether the full deadlock search runs). */
inline void cls_add(std::array<const Trx *, 3> &t, const Trx *trx) noexcept {
  for (auto &e : t) {
    if (e == trx) {
      return;
    } else if (e == nullptr) {
      e = trx;
      return;
    }
  }
}

/** The locks on one record seen so far by a grant pass. */
struct RecSeen {
  /** Note the lock, in queue order. */
  void add(const Lock *lock) noexcept {
    const auto c = rec_cls(lock->precise_mode());
    auto &t = m_trx[c];

    if (!(m_mask & (1U << c))) {
      m_mask |= uint16_t(1U << c);
      t = {lock->m_trx, nullptr, nullptr};
    } else {
      cls_add(t, lock->m_trx);
    }
  }

  /** @return a transaction with a lock seen so far that blocks the waiting
  lock, preferring one that waits itself (its edge leads on in the waits-for
  graph, see pick_blocker()), or nullptr.
  @param[out] multi true if several transactions block it */
  [[nodiscard]] const Trx *blocker(const Lock *wait, bool sup, bool *multi) const noexcept {
    auto mask = uint32_t(m_mask & cls_blocks.m_mask[sup][rec_cls(wait->precise_mode())]);
    Blockers b;

    while (mask != 0) {
      const auto c = uint32_t(std::countr_zero(mask));

      for (auto t : m_trx[c]) {
        if (t != nullptr && t != wait->m_trx) {
          b.add(t);
        }
      }

      mask &= mask - 1;
    }

    *multi = b.m_n > 1;

    return b.pick();
  }

  /** The classes seen. */
  uint16_t m_mask{};

  /** Up to three distinct transactions per class seen (cls_add()), valid for the classes in m_mask. */
  std::array<std::array<const Trx *, 3>, 16> m_trx;
};

/** The table locks seen so far by a grant pass, by mode: the same summary for the table queue. */
struct TableSeen {
  void add(const Lock *lock) noexcept {
    const auto m = uint32_t(lock->mode());
    auto &t = m_trx[m];

    ut_ad(m < LOCK_NUM);

    if (!(m_mask & (1U << m))) {
      m_mask |= uint8_t(1U << m);
      t = {lock->m_trx, nullptr, nullptr};
    } else {
      cls_add(t, lock->m_trx);
    }
  }

  [[nodiscard]] const Trx *blocker(const Lock *wait, bool *multi) const noexcept {
    Blockers b;

    for (uint32_t m{}; m < LOCK_NUM; ++m) {
      if ((m_mask & (1U << m)) && !Lock::mode_compatible(wait->mode(), LockMode(m))) {
        for (auto t : m_trx[m]) {
          if (t != nullptr && t != wait->m_trx) {
            b.add(t);
          }
        }
      }
    }

    *multi = b.m_n > 1;

    return b.pick();
  }

  uint8_t m_mask{};
  std::array<std::array<const Trx *, 3>, LOCK_NUM> m_trx;
};

}  // namespace

Lock *Lock::rec_clone(MemHeap *heap) const noexcept {
  ut_ad(type() == LOCK_REC);

  const auto size = sizeof(Lock) + rec_get_n_bits() / 8;

  /* Not the node of the list of the locks of the trx: the thread that
  appends a lock to it writes the node of its last lock, with the trx mutex
  only (the page shard latch of that lock is not needed). The other fields
  and the bitmap change only under the page shard latch, held here. */
  constexpr auto node = offsetof(Lock, m_trx_locks);
  constexpr auto rest = offsetof(Lock, m_n_bits);

  auto clone = static_cast<byte *>(mem_heap_alloc(heap, size));
  ut_a(clone != nullptr);
  auto src = reinterpret_cast<const byte *>(this);

  std::memcpy(clone, src, node);
  std::memcpy(clone + rest, src + rest, size - rest);

  auto lock = reinterpret_cast<Lock *>(clone);

  new (&lock->m_trx_locks) UT_LIST_NODE_T(Lock){};

  return lock;
}

uint64_t Lock::table_id() const noexcept {
  ut_ad(type() == LOCK_TABLE);
  return table()->m_id;
}

const char *Lock::table_name() const noexcept {
  ut_ad(type() == LOCK_TABLE);
  return table()->m_name;
}

const char *Lock::rec_index_name() const noexcept {
  ut_a(type() == LOCK_REC);
  return m_rec.m_index->m_name;
}

const char *Lock::get_mode_str() const noexcept {
  const auto is_gap_lock = type() == LOCK_REC && rec_is_gap();

  switch (mode()) {
    case LOCK_S:
      if (is_gap_lock) {
        return "S,GAP";
      } else {
        return "S";
      }
    case LOCK_X:
      if (is_gap_lock) {
        return "X,GAP";
      } else {
        return "X";
      }
    case LOCK_IS:
      if (is_gap_lock) {
        return "IS,GAP";
      } else {
        return "IS";
      }
    case LOCK_IX:
      if (is_gap_lock) {
        return "IX,GAP";
      } else {
        return "IX";
      }
    default:
      return "UNKNOWN";
  }
}

const Table *Lock::table() const noexcept {
  switch (type()) {
    case LOCK_REC:
      return m_rec.m_index->m_table;
    case LOCK_TABLE:
      return m_table.m_table;
    default:
      ut_error;
      return nullptr;
  }
}

