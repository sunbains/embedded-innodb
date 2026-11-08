/****************************************************************************
Copyright (c) 2024, 2026 Sunny Bains. All rights reserved.

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

/** @file unit-tests/test_lock.cc
LockSys: record and table locks.

  LockTest      in memory (SysTest: no disk), locks on keys that exist only
                in the lock table: the queues, the conflict rules, the
                grants, the inheritance of gap locks from key to key.
  LockDbTest    deadlocks, waits and grants of transactions of a started
                instance, in memory and through the API.
  LockPerf      performance.

A record lock queue is a key's (LockKey, a fingerprint of the index and the
key), every lock is on one key. The tests name keys by a page and a heap
number, key(): heap number 1, the supremum of the page, was a lock on the gap
before it; it is a fence now, a key whose requests are gap requests (fence()). */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "page0page.h"
#include "trx0sys.h"
#include "trx0trx.h"

#include <algorithm>
#include <cstdlib>
#include <format>
#include <cstring>

#include <array>
#include <future>
#include <random>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

/** @return the key of the record heap_no of the fake page page_no, as FakePage::key() of
lock_mt_util.h names it: the lock system knows only keys. */
LockKey key(page_no_t page_no, ulint heap_no) {
  return LockKey{uint64_t(page_no) << 16 | heap_no};
}

/** @return mode as a request on a fence makes it (LockSys::fence_mode()): a gap request. */
ulint fence(ulint mode) {
  return LockSys::fence_mode(LockPos{LockKey{}, true}, LockMode(mode));
}

/** @return the queue of a key, or nullptr: looked up with the lock system
latched X (the tests are single threaded here: the queue stays). */
Rec_locks *queue_of(LockSys *lock_sys, LockKey key) {
  LockSys::Exclusive x(*lock_sys);
  return lock_sys->rec_queue(key);
}

/** @return true if a key has record locks. */
bool has_queue(LockSys *lock_sys, LockKey key) {
  return queue_of(lock_sys, key) != nullptr;
}

/** @return a zeroed Lock of type with a bitmap of n_bits, for the checks of
pure functions (Lock::has_to_wait_for): it is in no queue. */
Lock *make_lock(std::vector<std::array<byte, 128>> &mem, Trx *trx, ulint type_mode, ulint heap_no = ULINT_UNDEFINED) {
  auto &buf = mem.emplace_back();

  buf.fill(0);

  auto lock = reinterpret_cast<Lock *>(buf.data());

  lock->m_trx = trx;
  lock->m_type_mode = LockMode(type_mode);

  if (heap_no != ULINT_UNDEFINED) {
    lock->m_n_bits = (buf.size() - sizeof(Lock)) * 8;
    lock->rec_set_nth_bit(heap_no);
  }

  return lock;
}

/** In-memory lock queues: transactions that are never started, locks on
fake keys, no index. */
class LockTest : public ut::SysTest {
 protected:
  void SetUp() override {
    ut::SysTest::SetUp();
    m_lock_sys = m_sys->m_lock_sys.get();
    m_mutex = &m_sys->m_trx_sys->m_mutex;
  }

  void TearDown() override {
    /* Release all the locks first: a release can grant a waiting lock of
    another transaction, that must still be marked as waiting then. */
    m_lock_sys->x_lock();

    for (auto t : m_trxs) {
      m_lock_sys->release(t);
    }

    for (auto t : m_trxs) {
      t->m_wait_lock = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    m_lock_sys->x_unlock();

    for (auto &t : m_trxs) {
      m_sys->m_trx_sys->destroy_user_trx(t);
    }

    m_trxs.clear();

    ut::SysTest::TearDown();
  }

  Trx *trx() {
    m_trxs.push_back(m_sys->m_trx_sys->create_user_trx(nullptr));
    return m_trxs.back();
  }

  /** A record lock of trx on key, created with the lock system latched X. */
  Lock *rec_lock(LockKey key, ulint type_mode, Trx *trx) {
    m_lock_sys->x_lock();
    auto lock = m_lock_sys->rec_create(LockMode(type_mode), key, nullptr, trx);
    m_lock_sys->x_unlock();

    return lock;
  }

  /** A record lock request added to the queue, as the engine does once it knows it need not wait. */
  Lock *add(LockKey key, ulint type_mode, Trx *trx) {
    m_lock_sys->x_lock();
    auto lock = m_lock_sys->rec_add_to_queue(LockMode(type_mode), key, nullptr, trx);
    m_lock_sys->x_unlock();

    return lock;
  }

  /** @return true if a request of trx for type_mode on key would have to wait. */
  bool waits(LockKey key, ulint type_mode, Trx *trx) {
    m_lock_sys->x_lock();
    auto lock = m_lock_sys->rec_other_has_conflicting(key, LockMode(type_mode), trx);
    m_lock_sys->x_unlock();

    return lock != nullptr;
  }

  /** @return true if the waiting lock has to keep waiting behind the locks ahead of it. */
  bool must_wait(Lock *lock) {
    m_lock_sys->x_lock();
    const auto &queue = *m_lock_sys->rec_queue(lock->key());
    auto ret = m_lock_sys->rec_has_to_wait_in_queue(queue, lock);
    m_lock_sys->x_unlock();

    return ret;
  }

  /** Remove lock from its queue, granting the waiters that can go. */
  void dequeue(Lock *lock) {
    m_lock_sys->x_lock();
    m_lock_sys->rec_dequeue_from_page(lock);
    m_lock_sys->x_unlock();
  }

  void release(Trx *trx) {
    m_lock_sys->x_lock();
    m_lock_sys->release(trx);
    m_lock_sys->x_unlock();
  }

  /** @return the lock of trx on key with its bit set, or nullptr. */
  const Lock *find(LockKey key, const Trx *trx) {
    auto q = queue_of(m_lock_sys, key);

    if (q == nullptr) {
      return nullptr;
    }

    for (auto lock : *q) {
      if (lock->m_trx == trx && lock->rec_is_nth_bit_set(LOCK_BIT)) {
        return lock;
      }
    }

    return nullptr;
  }

  /** @return the locks on key with their bit set, in queue order. */
  std::vector<const Lock *> queue(LockKey key) {
    std::vector<const Lock *> locks;

    if (auto q = queue_of(m_lock_sys, key); q != nullptr) {
      for (auto lock : *q) {
        if (lock->rec_is_nth_bit_set(LOCK_BIT)) {
          locks.push_back(lock);
        }
      }
    }

    return locks;
  }

  /** @return the number of locks on key, bit set or not. */
  size_t n_locks(LockKey key) {
    auto q = queue_of(m_lock_sys, key);

    return q == nullptr ? 0 : q->size();
  }

  LockSys *m_lock_sys{};
  Mutex *m_mutex{};
  std::vector<Trx *> m_trxs;
};

/** The compatibility matrix of the table lock modes, through
Lock::has_to_wait_for() on table locks of two transactions:
         IS IX S  X
      IS +  +  +  -
      IX +  +  -  -
      S  +  -  +  -
      X  -  -  -  -
A transaction never waits for its own locks. */
TEST_F(LockTest, CompatMatrix) {
  constexpr std::array<LockMode, 4> modes{LOCK_IS, LOCK_IX, LOCK_S, LOCK_X};
  constexpr bool compat[4][4] = {
    {true, true, true, false},
    {true, true, false, false},
    {true, false, true, false},
    {false, false, false, false},
  };

  auto a = trx();
  auto b = trx();
  std::vector<std::array<byte, 128>> mem;

  mem.reserve(64);

  for (size_t i{}; i < modes.size(); ++i) {
    for (size_t j{}; j < modes.size(); ++j) {
      auto req = make_lock(mem, a, LOCK_TABLE | modes[i]);
      auto held = make_lock(mem, b, LOCK_TABLE | modes[j]);
      auto own = make_lock(mem, a, LOCK_TABLE | modes[j]);

      EXPECT_EQ(req->has_to_wait_for(held), !compat[i][j]) << i << " " << j;
      EXPECT_EQ(compat[i][j], compat[j][i]) << "the matrix is symmetric";
      EXPECT_FALSE(req->has_to_wait_for(own)) << i << " " << j;
    }
  }
}

/** The stronger-or-equal relation:
         IS IX S  X
      IS +  -  -  -
      IX +  +  -  -
      S  +  -  +  -
      X  +  +  +  + */
TEST_F(LockTest, StrengthMatrix) {
  constexpr std::array<LockMode, 4> modes{LOCK_IS, LOCK_IX, LOCK_S, LOCK_X};
  constexpr bool stronger[4][4] = {
    {true, false, false, false},
    {true, true, false, false},
    {true, false, true, false},
    {true, true, true, true},
  };

  for (size_t i{}; i < modes.size(); ++i) {
    for (size_t j{}; j < modes.size(); ++j) {
      EXPECT_EQ(Lock::mode_stronger_or_eq(modes[i], modes[j]), stronger[i][j]) << i << " " << j;
    }
  }
}

/** rec_create() keeps every flag, sets the one bit of its key, appends the
lock to the queue of the key and to the locks of the transaction, and a
waiting lock becomes the wait lock of the transaction. */
TEST_F(LockTest, RecCreateFlags) {
  const std::array<ulint, 8> flags{
    0,
    LOCK_GAP,
    LOCK_REC_NOT_GAP,
    LOCK_GAP | LOCK_INSERT_INTENTION,
    LOCK_WAIT,
    LOCK_GAP | LOCK_WAIT,
    LOCK_REC_NOT_GAP | LOCK_WAIT,
    LOCK_GAP | LOCK_INSERT_INTENTION | LOCK_WAIT
  };
  ulint heap_no{PAGE_HEAP_NO_USER_LOW};

  for (auto mode : {LOCK_S, LOCK_X}) {
    for (auto f : flags) {
      const auto k = key(1, heap_no);
      auto t = trx();
      auto lock = rec_lock(k, mode | f, t);

      ASSERT_NE(lock, nullptr);
      EXPECT_EQ(lock->type(), LOCK_REC);
      EXPECT_EQ(lock->mode(), mode);
      EXPECT_EQ(lock->precise_mode(), LockMode(LOCK_REC | mode | f)) << f;
      EXPECT_EQ(lock->key(), k);
      EXPECT_EQ(lock->rec_find_set_bit(), LOCK_BIT);
      EXPECT_EQ(lock->m_trx, t);
      EXPECT_EQ(t->m_trx_locks.back(), lock);
      EXPECT_EQ(queue_of(m_lock_sys, k)->back(), lock);
      EXPECT_EQ(lock->is_waiting(), (f & LOCK_WAIT) != 0);
      EXPECT_EQ(t->m_wait_lock, (f & LOCK_WAIT) ? lock : nullptr);

      ++heap_no;
    }
  }
}

/** A request on a fence is a gap request: fence_mode() adds the gap flag
and drops the record only flag, the insert intention flag stays; on a
record it changes nothing. The lock keeps the gap flag (on the supremum it
was not stored: every lock there was a gap lock). */
TEST_F(LockTest, FenceMode) {
  const auto k = key(2, 5);

  EXPECT_EQ(fence(LOCK_X), ulint(LOCK_X | LOCK_GAP));
  EXPECT_EQ(fence(LOCK_S | LOCK_REC_NOT_GAP), ulint(LOCK_S | LOCK_GAP));
  EXPECT_EQ(fence(LOCK_X | LOCK_GAP), ulint(LOCK_X | LOCK_GAP));
  EXPECT_EQ(fence(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), ulint(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION));
  EXPECT_EQ(LockSys::fence_mode(LockPos{k, false}, LockMode(LOCK_X | LOCK_REC_NOT_GAP)), LockMode(LOCK_X | LOCK_REC_NOT_GAP));

  auto gap = rec_lock(k, fence(LOCK_X), trx());
  auto ii = rec_lock(k, fence(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), trx());

  EXPECT_EQ(gap->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_TRUE(gap->rec_is_gap());
  EXPECT_EQ(ii->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION));
}

/** A lock is on one key: its bitmap has the one bit LOCK_BIT, in a byte;
the bits past it read as unset. (It was a bitmap over the heap numbers of a
page, with a margin for the records inserted later.) */
TEST_F(LockTest, RecCreateBit) {
  auto lock = rec_lock(key(3, 7), LOCK_S, trx());

  EXPECT_EQ(lock->rec_get_n_bits() % 8, 0u);
  EXPECT_LE(lock->rec_get_n_bits(), 8u);
  EXPECT_TRUE(lock->rec_is_nth_bit_set(LOCK_BIT));
  EXPECT_EQ(lock->rec_find_set_bit(), LOCK_BIT);
  EXPECT_FALSE(lock->rec_is_nth_bit_set(lock->rec_get_n_bits()));
  EXPECT_FALSE(lock->rec_is_nth_bit_set(lock->rec_get_n_bits() + 1000));

  for (ulint i = LOCK_BIT + 1; i < lock->rec_get_n_bits(); ++i) {
    EXPECT_FALSE(lock->rec_is_nth_bit_set(i)) << i;
  }
}

/** The locks of a key are queued in the order of their creation, whatever
the transaction. */
TEST_F(LockTest, QueueOrder) {
  const auto k = key(4, 5);
  std::vector<const Lock *> created;
  std::array<Trx *, 3> trxs{trx(), trx(), trx()};

  for (int i{}; i < 9; ++i) {
    created.push_back(rec_lock(k, LOCK_S | LOCK_GAP, trxs[i % 3]));
  }

  EXPECT_EQ(queue(k), created);
}

/** One case of the conflict rules: a request of trx b for req, with a lock
of trx a for held on the same key, a record or a fence. */
struct Conflict {
  const char *m_what;
  ulint m_held;
  ulint m_req;
  bool m_fence;
  bool m_waits;
};

/** The rules of Lock::rec_blocks(): when a record lock request has to wait
for a lock of another transaction on the same key. On a fence (the supremum
was one) the held and the requested modes are gap modes (fence()). */
TEST_F(LockTest, ConflictRules) {
  constexpr bool REC = false;
  constexpr bool FENCE = true;
  constexpr ulint II = LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION;

  const std::vector<Conflict> cases{
    {"S vs S", LOCK_S, LOCK_S, REC, false},
    {"X vs S", LOCK_S, LOCK_X, REC, true},
    {"S vs X", LOCK_X, LOCK_S, REC, true},
    {"X vs X", LOCK_X, LOCK_X, REC, true},
    {"rec-only X vs rec-only X", LOCK_X | LOCK_REC_NOT_GAP, LOCK_X | LOCK_REC_NOT_GAP, REC, true},
    {"gap S request never waits", LOCK_X, LOCK_S | LOCK_GAP, REC, false},
    {"gap X request never waits", LOCK_X, LOCK_X | LOCK_GAP, REC, false},
    {"next-key vs gap lock", LOCK_X | LOCK_GAP, LOCK_X, REC, false},
    {"rec-only vs gap lock", LOCK_X | LOCK_GAP, LOCK_S | LOCK_REC_NOT_GAP, REC, false},
    {"insert intention vs gap lock", LOCK_S | LOCK_GAP, II, REC, true},
    {"insert intention vs next-key", LOCK_S, II, REC, true},
    {"insert intention vs rec-only", LOCK_X | LOCK_REC_NOT_GAP, II, REC, false},
    {"insert intention vs insert intention", II, II, REC, false},
    {"nothing waits for insert intention", II, LOCK_X, REC, false},
    {"rec-only vs next-key", LOCK_S, LOCK_X | LOCK_REC_NOT_GAP, REC, true},
    {"fence is a gap", LOCK_X, LOCK_X, FENCE, false},
    {"fence S vs X", LOCK_S, LOCK_X, FENCE, false},
    {"fence rec-only is a gap", LOCK_X | LOCK_REC_NOT_GAP, LOCK_X | LOCK_REC_NOT_GAP, FENCE, false},
    {"insert intention on fence", LOCK_S, II, FENCE, true},
    {"insert intention vs waiting gap", LOCK_X | LOCK_GAP | LOCK_WAIT, II, REC, true},
    {"waiting X blocks S", LOCK_X | LOCK_WAIT, LOCK_S, REC, true},
    {"waiting S blocks X", LOCK_S | LOCK_WAIT, LOCK_X | LOCK_REC_NOT_GAP, REC, true},
    {"waiting insert intention blocks nothing", II | LOCK_WAIT, LOCK_X, REC, false},
  };

  page_no_t page_no{100};

  for (const auto &c : cases) {
    const auto k = key(page_no++, PAGE_HEAP_NO_USER_LOW + 3);
    const auto held = c.m_fence ? fence(c.m_held) : c.m_held;
    const auto req = c.m_fence ? fence(c.m_req) : c.m_req;
    auto a = trx();
    auto b = trx();

    ASSERT_NE(rec_lock(k, held, a), nullptr);
    EXPECT_EQ(waits(k, req, b), c.m_waits) << c.m_what;

    /* Never for one's own lock, nor on another key. */
    EXPECT_FALSE(waits(k, req, a)) << c.m_what;
    EXPECT_FALSE(waits(key(page_no, PAGE_HEAP_NO_USER_LOW + 3), req, b)) << c.m_what;
  }
}

/** Two different keys never block each other, whatever the modes: a key of
the same page, of another page, the fence of the page. A waiting lock with
locks only on other keys does not wait. */
TEST_F(LockTest, KeysApart) {
  constexpr ulint II = LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION;
  const std::array<ulint, 5> modes{LOCK_X, LOCK_X | LOCK_REC_NOT_GAP, LOCK_X | LOCK_GAP, LOCK_S, II};
  const std::array<LockKey, 3> others{key(10, 3), key(11, 2), key(10, PAGE_HEAP_NO_SUPREMUM)};
  const auto k = key(10, 2);
  auto a = trx();
  auto b = trx();

  for (ulint m : {ulint(LOCK_X), ulint(LOCK_X | LOCK_GAP), ulint(LOCK_S | LOCK_REC_NOT_GAP)}) {
    (void)rec_lock(k, m, a);
  }

  for (auto other : others) {
    for (auto m : modes) {
      EXPECT_FALSE(waits(other, m, b)) << m;
    }
  }

  /* On the key itself the X lock of a blocks. */
  EXPECT_TRUE(waits(k, LOCK_S, b));

  auto w = rec_lock(others[0], LOCK_X | LOCK_WAIT, b);

  EXPECT_FALSE(must_wait(w));
}

/** rec_other_has_conflicting() returns the first lock in the queue that
blocks the request. */
TEST_F(LockTest, ConflictIsFirstBlocker) {
  const auto k = key(5, 4);
  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(k, LOCK_S | LOCK_GAP, a);
  auto first = rec_lock(k, LOCK_S, b);
  (void)rec_lock(k, LOCK_S, c);

  m_lock_sys->x_lock();
  EXPECT_EQ(m_lock_sys->rec_other_has_conflicting(k, LOCK_X, a), first);
  m_lock_sys->x_unlock();
}

/** rec_add_to_queue() reuses a lock of the same transaction and precise
mode on the key, rather than creating another lock. A lock is on one key: a
request on another key, of the same page too, is a lock of its own. */
TEST_F(LockTest, AddToQueueReuse) {
  const auto k = key(6, 3);
  auto a = trx();
  auto b = trx();

  auto l1 = add(k, LOCK_REC | LOCK_S, a);
  auto l2 = add(k, LOCK_REC | LOCK_S, a);

  EXPECT_EQ(l1, l2);
  EXPECT_TRUE(l1->rec_is_nth_bit_set(LOCK_BIT));
  EXPECT_EQ(n_locks(k), 1u);

  /* Another precise mode or transaction: a lock of its own. */
  auto gap = add(k, LOCK_REC | LOCK_S | LOCK_GAP, a);
  auto other = add(k, LOCK_REC | LOCK_S, b);

  EXPECT_NE(gap, l1);
  EXPECT_NE(other, l1);
  EXPECT_NE(other, gap);
  EXPECT_EQ(n_locks(k), 3u);
  EXPECT_EQ(a->m_trx_locks.size(), 2u);

  /* Another key of the page. */
  auto next = add(key(6, 4), LOCK_REC | LOCK_S, a);

  EXPECT_NE(next, l1);
  EXPECT_EQ(next->key(), key(6, 4));
  EXPECT_EQ(a->m_trx_locks.size(), 3u);
}

/** A request behind a waiting lock on the same key gets a lock of its
own, after the waiting one (no lock goes ahead of a waiting one), even if
the transaction has a similar lock ahead of the waiter. */
TEST_F(LockTest, AddToQueueBehindWaiter) {
  const auto k = key(7, 3);
  auto a = trx();
  auto b = trx();

  auto similar = add(k, LOCK_REC | LOCK_S | LOCK_GAP, a);
  auto waiting = rec_lock(k, LOCK_X | LOCK_WAIT, b);
  auto lock = add(k, LOCK_REC | LOCK_S | LOCK_GAP, a);

  EXPECT_NE(lock, similar);
  EXPECT_EQ(queue(k), (std::vector<const Lock *>{similar, waiting, lock}));
}

/** A waiting request is always a lock of its own. */
TEST_F(LockTest, AddToQueueWaitNew) {
  const auto k = key(8, 3);
  auto a = trx();
  auto granted = add(k, LOCK_REC | LOCK_X | LOCK_GAP, a);
  auto waiting = add(k, LOCK_REC | LOCK_X | LOCK_GAP | LOCK_WAIT, a);

  EXPECT_NE(granted, waiting);
  EXPECT_TRUE(waiting->is_waiting());
  EXPECT_EQ(a->m_wait_lock, waiting);
}

/** A request on a fence is a gap request, which the plain lock of the
transaction on the key covers: rec_lock() creates nothing. (On the supremum
the gap request was a plain request that reused the plain lock.) */
TEST_F(LockTest, FenceCoveredByPlain) {
  const auto k = key(9, PAGE_HEAP_NO_SUPREMUM);
  auto a = trx();
  auto plain = add(k, LOCK_REC | LOCK_S, a);

  m_lock_sys->x_lock();
  EXPECT_EQ(m_lock_sys->rec_lock(false, LockMode(fence(LOCK_S)), k, nullptr, a, nullptr), DB_SUCCESS);
  m_lock_sys->x_unlock();

  EXPECT_EQ(queue(k), (std::vector<const Lock *>{plain}));
  EXPECT_EQ(a->m_trx_locks.size(), 1u);
}

/** Waiters are granted in the order they came: releasing the holder grants
the first waiter only, releasing it grants the next. */
TEST_F(LockTest, GrantFifo) {
  const auto k = key(10, 4);
  auto a = trx();
  auto b = trx();
  auto c = trx();

  auto la = rec_lock(k, LOCK_X, a);
  auto lb = rec_lock(k, LOCK_X | LOCK_WAIT, b);
  auto lc = rec_lock(k, LOCK_X | LOCK_WAIT, c);

  EXPECT_TRUE(must_wait(lb));
  EXPECT_TRUE(must_wait(lc));

  dequeue(la);

  EXPECT_FALSE(lb->is_waiting());
  EXPECT_EQ(b->m_wait_lock, nullptr);
  EXPECT_TRUE(lc->is_waiting());
  EXPECT_EQ(c->m_wait_lock, lc);

  dequeue(lb);

  EXPECT_FALSE(lc->is_waiting());
  EXPECT_EQ(c->m_wait_lock, nullptr);
}

/** Compatible waiters are granted together. */
TEST_F(LockTest, GrantShared) {
  const auto k = key(11, 4);
  auto la = rec_lock(k, LOCK_X, trx());
  auto lb = rec_lock(k, LOCK_S | LOCK_WAIT, trx());
  auto lc = rec_lock(k, LOCK_S | LOCK_WAIT, trx());

  dequeue(la);

  EXPECT_FALSE(lb->is_waiting());
  EXPECT_FALSE(lc->is_waiting());
}

/** A request compatible with the holder still waits behind an incompatible
waiter: no barging. Releasing the holder grants the waiter ahead only. */
TEST_F(LockTest, NoBarging) {
  const auto k = key(12, 4);
  auto la = rec_lock(k, LOCK_S, trx());
  auto lb = rec_lock(k, LOCK_X | LOCK_WAIT, trx());
  auto lc = rec_lock(k, LOCK_S | LOCK_WAIT, trx());

  EXPECT_TRUE(must_wait(lc));

  dequeue(la);

  EXPECT_FALSE(lb->is_waiting());
  EXPECT_TRUE(lc->is_waiting());

  dequeue(lb);

  EXPECT_FALSE(lc->is_waiting());
}

/** Only the locks ahead in the queue count: a waiting lock first in the
queue need not wait for those behind it. */
TEST_F(LockTest, WaitOnlyForAhead) {
  const auto k = key(13, 4);
  auto first = rec_lock(k, LOCK_X | LOCK_WAIT, trx());

  (void)rec_lock(k, LOCK_X, trx());

  EXPECT_FALSE(must_wait(first));
}

/** An insert intention waiting for a gap lock is granted when the gap
lock goes. */
TEST_F(LockTest, GrantInsertIntention) {
  const auto k = key(14, 4);
  auto gap = rec_lock(k, LOCK_S | LOCK_GAP, trx());
  auto ii = rec_lock(k, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION | LOCK_WAIT, trx());

  EXPECT_TRUE(must_wait(ii));

  dequeue(gap);

  EXPECT_FALSE(ii->is_waiting());
  EXPECT_TRUE(ii->rec_is_insert_intention());
}

/** Releasing a lock on another key of the page grants nothing. */
TEST_F(LockTest, GrantOtherRecord) {
  const auto k = key(15, 4);
  auto holder = rec_lock(k, LOCK_X, trx());
  auto other = rec_lock(key(15, 5), LOCK_X, trx());
  auto waiter = rec_lock(k, LOCK_X | LOCK_WAIT, trx());

  dequeue(other);

  EXPECT_TRUE(waiter->is_waiting());

  dequeue(holder);

  EXPECT_FALSE(waiter->is_waiting());
}

/** Releasing a transaction frees all its locks, removes the keys left
without locks from the lock table, keeps the locks of the others and grants
their waiting locks. */
TEST_F(LockTest, ReleaseAll) {
  auto a = trx();
  auto b = trx();

  for (page_no_t p = 20; p < 23; ++p) {
    for (ulint h = 2; h < 6; ++h) {
      (void)rec_lock(key(p, h), LOCK_X | LOCK_REC_NOT_GAP, a);
    }
  }

  auto kept = rec_lock(key(21, 7), LOCK_S | LOCK_GAP, b);
  auto waiter = rec_lock(key(22, 3), LOCK_S | LOCK_WAIT, b);

  EXPECT_TRUE(must_wait(waiter));

  release(a);

  EXPECT_TRUE(a->m_trx_locks.empty());

  for (ulint h = 2; h < 6; ++h) {
    EXPECT_FALSE(has_queue(m_lock_sys, key(20, h))) << h;
    EXPECT_FALSE(has_queue(m_lock_sys, key(21, h))) << h;
  }

  EXPECT_EQ(n_locks(key(21, 7)), 1u);
  EXPECT_EQ(queue(key(21, 7)), (std::vector<const Lock *>{kept}));
  EXPECT_FALSE(waiter->is_waiting());
  EXPECT_EQ(b->m_trx_locks.size(), 2u);
}

/** A release of more locks than a batch (LOCK_RELEASE_BATCH) of a shard
latch hold frees them all. */
TEST_F(LockTest, ReleaseMany) {
  auto a = trx();
  auto b = trx();

  for (page_no_t p = 0; p < 2500; ++p) {
    (void)rec_lock(key(100000 + p, 2 + p % 50), LOCK_S, a);
  }

  auto kept = rec_lock(key(100007, 60), LOCK_S, b);

  /* The keys with locks: one lock each here. */
  EXPECT_EQ(m_lock_sys->get_n_rec_locks(), 2501u);

  release(a);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_EQ(m_lock_sys->get_n_rec_locks(), 1u);
  EXPECT_EQ(queue(key(100007, 60)), (std::vector<const Lock *>{kept}));
}

/** The heir of a key gets the X locks on the donor (REPEATABLE READ) as
granted gap locks, waiting ones too; the donor keeps its locks. */
TEST_F(LockTest, InheritX) {
  const auto heir = key(31, 6);
  const auto donor = key(31, 4);
  auto a = trx();
  auto b = trx();

  (void)rec_lock(donor, LOCK_X | LOCK_REC_NOT_GAP, a);
  (void)rec_lock(donor, LOCK_X | LOCK_WAIT, b);

  m_lock_sys->x_lock();
  m_lock_sys->rec_inherit_to_gap(heir, donor);
  m_lock_sys->x_unlock();

  auto ha = find(heir, a);
  auto hb = find(heir, b);

  ASSERT_NE(ha, nullptr);
  ASSERT_NE(hb, nullptr);
  EXPECT_EQ(ha->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(hb->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(queue(donor).size(), 2u);
}

/** Under READ COMMITTED the X locks of an UPDATE or DELETE are not
inherited, nor are insert intentions. */
TEST_F(LockTest, InheritSkips) {
  const auto heir = key(32, 6);
  const auto donor = key(32, 4);
  auto rc = trx();
  auto ii = trx();

  rc->m_isolation_level = TRX_ISO_READ_COMMITTED;

  (void)rec_lock(donor, LOCK_X, rc);
  (void)rec_lock(donor, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, ii);

  m_lock_sys->x_lock();
  m_lock_sys->rec_inherit_to_gap(heir, donor);
  m_lock_sys->x_unlock();

  EXPECT_TRUE(queue(heir).empty());
}

/* Regression: rec_inherit_to_gap() inherits only X locks: the S next-key locks of a
locking read are dropped when the record is removed (purge, page split or
merge), and the gap they protected opens to inserts: a phantom. InnoDB
inherits every lock except insert intentions and, under READ COMMITTED, X
locks (the comment above the loop says so: "we DO want S-locks set by a
consistency constraint to be inherited also then"). 7c21eac0 has the same
condition, lock/lock0lock.cc:1449. */
TEST_F(LockTest, InheritS) {
  const auto heir = key(33, 6);
  const auto donor = key(33, 4);
  auto rr = trx();
  auto rc = trx();

  rc->m_isolation_level = TRX_ISO_READ_COMMITTED;

  (void)rec_lock(donor, LOCK_S, rr);
  (void)rec_lock(donor, LOCK_S | LOCK_REC_NOT_GAP, rc);

  m_lock_sys->x_lock();
  m_lock_sys->rec_inherit_to_gap(heir, donor);
  m_lock_sys->x_unlock();

  ASSERT_NE(find(heir, rr), nullptr);
  ASSERT_NE(find(heir, rc), nullptr);
  EXPECT_EQ(find(heir, rr)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
}

/** A key inserted before a locked key inherits its locks that cover the
gap (next-key and gap locks, S or X) as gap locks; not the record-only locks
nor the insert intentions. */
TEST_F(LockTest, InheritIfGapLock) {
  const auto heir = key(35, 4);
  const auto donor = key(35, 5);
  auto s = trx();
  auto gap = trx();
  auto rec = trx();
  auto ii = trx();

  (void)rec_lock(donor, LOCK_S, s);
  (void)rec_lock(donor, LOCK_X | LOCK_GAP, gap);
  (void)rec_lock(donor, LOCK_X | LOCK_REC_NOT_GAP, rec);
  (void)rec_lock(donor, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, ii);

  m_lock_sys->x_lock();
  m_lock_sys->rec_inherit_to_gap_if_gap_lock(heir, donor);
  m_lock_sys->x_unlock();

  ASSERT_NE(find(heir, s), nullptr);
  ASSERT_NE(find(heir, gap), nullptr);
  EXPECT_EQ(find(heir, s)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
  EXPECT_EQ(find(heir, gap)->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(find(heir, rec), nullptr);
  EXPECT_EQ(find(heir, ii), nullptr);
}

/** update_insert(): the new key gets the locks of the key after it that
cover the gap, as gap locks; the key after it keeps its locks. */
TEST_F(LockTest, InsertKey) {
  const auto k = key(36, 5);
  const auto next = key(36, 6);
  auto a = trx();
  auto b = trx();

  (void)rec_lock(next, LOCK_S, a);
  (void)rec_lock(next, LOCK_X | LOCK_REC_NOT_GAP, b);

  m_lock_sys->update_insert(nullptr, k, next);

  ASSERT_EQ(queue(k).size(), 1u);
  EXPECT_EQ(find(k, a)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
  EXPECT_EQ(find(k, b), nullptr);
  EXPECT_EQ(queue(next).size(), 2u);
}

/** update_delete(): the key after the deleted one gets gap copies of its
locks (waiting ones too, granted); on the deleted key the waiter is
cancelled (its wait ends, its bit is reset), a record only lock is reset, a
plain lock becomes a gap lock, a gap lock stays. The key keeps its gap: an
insert into it still waits, a request for the key does not. */
TEST_F(LockTest, DeleteKey) {
  const auto k = key(37, 5);
  const auto next = key(37, 6);
  auto rec = trx();
  auto plain = trx();
  auto gap = trx();
  auto waiter = trx();
  auto other = trx();

  auto lr = rec_lock(k, LOCK_S | LOCK_REC_NOT_GAP, rec);
  auto lp = rec_lock(k, LOCK_S, plain);
  auto lg = rec_lock(k, LOCK_X | LOCK_GAP, gap);
  auto lw = rec_lock(k, LOCK_X | LOCK_WAIT, waiter);

  waiter->m_que_state = TRX_QUE_LOCK_WAIT;

  ASSERT_TRUE(must_wait(lw));

  m_lock_sys->update_delete(nullptr, k, next);

  for (auto t : {rec, plain, gap, waiter}) {
    ASSERT_NE(find(next, t), nullptr);
    EXPECT_EQ(find(next, t)->precise_mode() & ~LOCK_MODE_MASK, LockMode(LOCK_REC | LOCK_GAP));
    EXPECT_FALSE(find(next, t)->is_waiting());
  }

  EXPECT_FALSE(lw->is_waiting());
  EXPECT_EQ(waiter->m_wait_lock, nullptr);
  EXPECT_EQ(waiter->m_que_state, TRX_QUE_RUNNING);
  EXPECT_FALSE(lr->rec_is_nth_bit_set(LOCK_BIT));
  EXPECT_FALSE(lw->rec_is_nth_bit_set(LOCK_BIT));
  EXPECT_EQ(lp->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
  EXPECT_EQ(lg->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(queue(k), (std::vector<const Lock *>{lp, lg}));

  EXPECT_FALSE(waits(k, LOCK_X, other));
  EXPECT_FALSE(waits(k, LOCK_X | LOCK_REC_NOT_GAP, other));
  EXPECT_TRUE(waits(k, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, other));
}

/** inherit_gap(): the heir gets gap copies of the locks on the key, the
key keeps them; an insert into the gap of the heir waits. */
TEST_F(LockTest, InheritGap) {
  const auto heir = key(38, PAGE_HEAP_NO_SUPREMUM);
  const auto k = key(39, PAGE_HEAP_NO_SUPREMUM);
  auto a = trx();
  auto b = trx();

  auto la = rec_lock(k, fence(LOCK_S), a);

  m_lock_sys->inherit_gap(nullptr, heir, k);

  ASSERT_NE(find(heir, a), nullptr);
  EXPECT_NE(find(heir, a), la);
  EXPECT_EQ(find(heir, a)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
  EXPECT_EQ(queue(k), (std::vector<const Lock *>{la}));
  EXPECT_TRUE(waits(heir, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, b));
}

/** inherit_gap() with the heir the key itself does nothing: no copy. */
TEST_F(LockTest, InheritGapSelf) {
  const auto k = key(40, 4);
  auto a = trx();

  auto la = rec_lock(k, LOCK_X, a);

  m_lock_sys->inherit_gap(nullptr, k, k);

  EXPECT_EQ(n_locks(k), 1u);
  EXPECT_EQ(queue(k), (std::vector<const Lock *>{la}));
  EXPECT_EQ(la->precise_mode(), LockMode(LOCK_REC | LOCK_X));
  EXPECT_EQ(a->m_trx_locks.size(), 1u);
}

/** Many transactions with record locks on random keys; the check for
waiters must not fail, every lock is released. */
TEST_F(LockTest, ManyTrxs) {
  constexpr int N_TRXS = 8;
  constexpr int N_LOCKS = 64;

  for (int i = 0; i < N_TRXS; ++i) {
    auto t = trx();

    for (int j = 0; j < N_LOCKS; ++j) {
      const auto k = key(page_no_t(random() % 100000), random() % 104);
      const auto mode = (j % 50) == 0 ? LOCK_X : LOCK_S;

      ASSERT_NE(rec_lock(k, mode, t), nullptr);
    }
  }

  for (auto t : m_trxs) {
    (void)m_lock_sys->trx_has_no_waiters(t);
  }
}

/* Regression: trx_has_no_waiters() returns true when it finds a waiting lock and
false when there is none: the inverse of its name and documentation
(lock/lock0lock.cc:3229 returns true on a waiter, :3252 false at the end).
It also counts a waiting lock on any record of the page, not only on the
records the transaction locks. Not called by the engine. */
TEST_F(LockTest, TrxHasNoWaiters) {
  const auto k = key(43, 4);
  auto a = trx();
  auto b = trx();

  (void)rec_lock(k, LOCK_X, a);

  EXPECT_TRUE(m_lock_sys->trx_has_no_waiters(a));

  (void)rec_lock(k, LOCK_X | LOCK_WAIT, b);

  EXPECT_FALSE(m_lock_sys->trx_has_no_waiters(a));
}

/** Discarding one lock of a key keeps the others and the queue; the last
one takes the queue with it. */
TEST_F(LockTest, DiscardOne) {
  const auto k = key(44, 4);
  auto a = trx();
  auto b = trx();

  auto la = rec_lock(k, LOCK_S, a);
  auto lb = rec_lock(k, LOCK_S, b);

  m_lock_sys->x_lock();
  m_lock_sys->rec_discard(la);
  m_lock_sys->x_unlock();

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_EQ(n_locks(k), 1u);
  EXPECT_EQ(queue(k), (std::vector<const Lock *>{lb}));

  m_lock_sys->x_lock();
  m_lock_sys->rec_discard(lb);
  m_lock_sys->x_unlock();

  EXPECT_TRUE(b->m_trx_locks.empty());
  EXPECT_FALSE(has_queue(m_lock_sys, k));
}

/** @return the test table or another table, from the dictionary cache. */
Table *get_table(System &sys, const char *name = ut::TABLE) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(name);
  sys.m_dict->mutex_release();

  return table;
}

/** A DbTest whose started instance is shared by the tests of a suite: the
first test starts it, the end of the suite shuts it down. Each test starts
with an empty test table. */
class SharedDb : public ut::DbTest {
 protected:
  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      s_sys.reset();
    }

    s_dir.reset();
  }

  void SetUp() override {
    if (s_dir == nullptr) {
      s_dir = std::make_unique<ut::TestDir>();
    }

    m_config = ut::make_config(s_dir->m_path);

    if (s_sys == nullptr) {
      start();
    } else {
      m_sys = std::move(s_sys);
    }

    clear();
  }

  void TearDown() override { s_sys = std::move(m_sys); }

  /** Delete all the rows of the test table. */
  void clear() {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (const auto &[k, row] : scan(trx)) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
  }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;
};

/** Deadlocks, waits and grants of started transactions. */
class LockDbTest : public SharedDb {
 protected:
  void SetUp() override {
    SharedDb::SetUp();

    m_table = get_table(*m_sys);
    m_index = m_table->get_first_index();
    m_lock_sys = m_sys->m_lock_sys.get();
    m_mutex = &m_sys->m_trx_sys->m_mutex;
  }

  void TearDown() override {
    finish();
    SharedDb::TearDown();
  }

  /** @return a started transaction whose locks the test creates in memory. */
  Trx *trx() {
    m_trxs.push_back(begin());
    return m_trxs.back();
  }

  /** Release the in-memory locks and commit the transactions of trx(). */
  void finish() {
    m_lock_sys->x_lock();

    for (auto t : m_trxs) {
      m_lock_sys->release(t);
    }

    for (auto t : m_trxs) {
      t->m_wait_lock = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    m_lock_sys->x_unlock();

    for (auto t : m_trxs) {
      commit(t);
    }

    m_trxs.clear();
  }

  /** A record lock of trx on key of the clustered index; a waiting one puts trx in the lock wait state. */
  Lock *rec_lock(LockKey key, ulint type_mode, Trx *trx) {
    m_lock_sys->x_lock();

    auto lock = m_lock_sys->rec_create(LockMode(type_mode), key, m_index, trx);

    if (type_mode & LOCK_WAIT) {
      trx->m_que_state = TRX_QUE_LOCK_WAIT;
    }

    m_lock_sys->x_unlock();

    return lock;
  }

  /** @return true if the waiting request lock of trx closes a cycle and trx is the victim. */
  bool deadlock(Lock *lock, Trx *trx) {
    m_lock_sys->x_lock();
    auto ret = m_lock_sys->deadlock_occurs(lock, trx);
    m_lock_sys->x_unlock();

    return ret;
  }

  /** @return true once trx waits for a lock, false after timeout. */
  bool wait_for_lock_wait(Trx *trx, std::chrono::milliseconds timeout = 10s) {
    const auto end = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < end) {
      trx_mutex_enter(trx);
      const auto waiting = trx->m_que_state == TRX_QUE_LOCK_WAIT;
      trx_mutex_exit(trx);

      if (waiting) {
        return true;
      }

      std::this_thread::sleep_for(1ms);
    }

    return false;
  }

  /** @return the result of an operation of trx that must not wait: if it
  waits, it is interrupted and the test fails. */
  DbErr no_wait(std::future<DbErr> &result, Trx *trx) {
    if (result.wait_for(3s) != std::future_status::ready) {
      ADD_FAILURE() << "the operation waited for a lock";
      api::trx_interrupt(trx);
    }

    return result.get();
  }

  /** UPDATE k in trx in another thread. */
  std::future<DbErr> async_update(Trx *trx, uint32_t k, uint32_t a) {
    return std::async(std::launch::async, [this, trx, k, a] { return update(trx, k, ut::Row{k, a, "u"}); });
  }

  /** A locking read of all the rows in trx, with lock mode S or X. */
  DbErr locking_scan(Trx *trx, CursorLockMode mode, size_t *n_rows = nullptr) {
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->set_lock_mode(*m_sys, mode);

    if (err == DB_SUCCESS) {
      const auto rows = ut::read_all(*m_sys, crsr, crsr->first(*m_sys));

      if (n_rows != nullptr) {
        *n_rows = rows.size();
      }
    }

    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  }

  /** Lock the test table in trx with mode, through a cursor. */
  DbErr lock_table(Trx *trx, CursorLockMode mode) {
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(*m_sys, mode);

    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  }

  Table *m_table{};
  Index *m_index{};
  LockSys *m_lock_sys{};
  Mutex *m_mutex{};
  std::vector<Trx *> m_trxs;
};

/** Two transactions, each waiting for the record the other holds: the
requester that closes the cycle is the victim when it does not weigh more. */
TEST_F(LockDbTest, DeadlockTwo) {
  const auto r1 = key(1, 2);
  const auto r2 = key(2, 2);
  auto a = trx();
  auto b = trx();

  (void)rec_lock(r1, LOCK_X, a);
  (void)rec_lock(r2, LOCK_X, b);
  (void)rec_lock(r1, LOCK_X | LOCK_WAIT, b);

  auto req = rec_lock(r2, LOCK_X | LOCK_WAIT, a);

  EXPECT_TRUE(deadlock(req, a));
  EXPECT_FALSE(b->m_was_chosen_as_deadlock_victim);
  EXPECT_NE(b->m_wait_lock, nullptr);
}

/** The same cycle, but the requester has more locks: the other transaction
is the victim, its wait is cancelled, and the requester keeps waiting with
no deadlock left. */
TEST_F(LockDbTest, DeadlockOtherVictim) {
  const auto r1 = key(1, 2);
  const auto r2 = key(2, 2);
  auto a = trx();
  auto b = trx();

  (void)rec_lock(r1, LOCK_X, a);

  for (ulint h = 3; h < 10; ++h) {
    (void)rec_lock(key(3, h), LOCK_X, a);
  }

  (void)rec_lock(r2, LOCK_X, b);
  auto bw = rec_lock(r1, LOCK_X | LOCK_WAIT, b);
  auto req = rec_lock(r2, LOCK_X | LOCK_WAIT, a);

  EXPECT_FALSE(deadlock(req, a));
  EXPECT_TRUE(b->m_was_chosen_as_deadlock_victim);
  EXPECT_EQ(b->m_wait_lock, nullptr);
  EXPECT_EQ(b->m_que_state, TRX_QUE_RUNNING);
  EXPECT_TRUE(std::none_of(b->m_trx_locks.begin(), b->m_trx_locks.end(), [&](const Lock *l) { return l == bw; })) << "the waiting lock of the victim is gone";
  EXPECT_TRUE(req->is_waiting());
}

/** A cycle of three. */
TEST_F(LockDbTest, DeadlockThree) {
  std::array<Trx *, 3> t{trx(), trx(), trx()};
  std::array<LockKey, 3> r{key(1, 2), key(2, 2), key(3, 2)};

  for (size_t i{}; i < 3; ++i) {
    (void)rec_lock(r[i], LOCK_X | LOCK_REC_NOT_GAP, t[i]);
  }

  (void)rec_lock(r[1], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, t[0]);
  (void)rec_lock(r[2], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, t[1]);

  auto req = rec_lock(r[0], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, t[2]);

  EXPECT_TRUE(deadlock(req, t[2]));
}

/** Waits that form no cycle: a chain, a lock that is compatible, and a gap
lock, which no lock waits for. */
TEST_F(LockDbTest, NoDeadlock) {
  const auto r1 = key(1, 2);
  const auto r2 = key(2, 2);
  auto a = trx();
  auto b = trx();
  auto c = trx();

  /* b waits for c, a waits for b. */
  (void)rec_lock(r1, LOCK_X, b);
  (void)rec_lock(r2, LOCK_X, c);
  (void)rec_lock(r2, LOCK_X | LOCK_WAIT, b);

  auto req = rec_lock(r1, LOCK_X | LOCK_WAIT, a);

  EXPECT_FALSE(deadlock(req, a));

  /* a holds only a gap lock on what c waits for: no edge from c to a. */
  (void)rec_lock(key(3, 2), LOCK_X | LOCK_GAP, a);
  auto creq = rec_lock(key(3, 2), LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, c);

  EXPECT_FALSE(deadlock(creq, c));
}

/** A chain of waits longer than the depth limit of the search: the
requester is rolled back as if it had deadlocked. A shorter chain is fine. */
TEST_F(LockDbTest, DeadlockTooDeep) {
  for (auto [n, expected] : {std::pair{150, false}, std::pair{230, true}}) {
    std::vector<Trx *> t;

    for (int i = 0; i < n; ++i) {
      t.push_back(trx());
      (void)rec_lock(key(page_no_t(i), 2), LOCK_X, t.back());
    }

    /* t[i] waits for the record of t[i + 1], the last does not wait. */
    for (int i = 1; i + 1 < n; ++i) {
      (void)rec_lock(key(page_no_t(i + 1), 2), LOCK_X | LOCK_WAIT, t[i]);
    }

    auto req = rec_lock(key(1, 2), LOCK_X | LOCK_WAIT, t[0]);

    EXPECT_EQ(deadlock(req, t[0]), expected) << n;

    finish();
  }
}

/** A row lock wait ends when the holder commits; the waiter then has the row. */
TEST_F(LockDbTest, WaitGrant) {
  load({{1, ut::Row{1, 1, "a"}}});

  auto holder = begin();

  ASSERT_EQ(update(holder, 1, ut::Row{1, 10, "h"}), DB_SUCCESS);

  auto waiter = begin();
  auto result = async_update(waiter, 1, 20);

  ASSERT_TRUE(wait_for_lock_wait(waiter));
  EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout);

  commit(holder);

  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.get(), DB_SUCCESS);
  commit(waiter);

  expect_rows({{1, ut::Row{1, 20, "u"}}});
}

/** Rows locked by different transactions: no wait. */
TEST_F(LockDbTest, NoWaitDisjoint) {
  load({{1, ut::Row{1, 1, "a"}}, {2, ut::Row{2, 2, "b"}}});

  auto a = begin();
  auto b = begin();

  ASSERT_EQ(update(a, 1, ut::Row{1, 10, "x"}), DB_SUCCESS);

  auto result = async_update(b, 2, 20);

  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.get(), DB_SUCCESS);

  commit(a);
  commit(b);
}

/** REPEATABLE READ: a locking read sets next-key locks; an insert into a
gap it read waits until it commits. */
TEST_F(LockDbTest, InsertWaitsForNextKey) {
  load({{10, ut::Row{10, 1, "a"}}, {20, ut::Row{20, 2, "b"}}});

  auto reader = begin(IB_TRX_REPEATABLE_READ);
  size_t n{};

  ASSERT_EQ(locking_scan(reader, IB_LOCK_S, &n), DB_SUCCESS);
  EXPECT_EQ(n, 2u);

  auto writer = begin();
  auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{15, 3, "c"}); });

  ASSERT_TRUE(wait_for_lock_wait(writer));

  commit(reader);

  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.get(), DB_SUCCESS);
  commit(writer);
}

/** READ COMMITTED: a locking read locks the records only, an insert into a
gap between them does not wait. (Keys of their own: the rows of the other
tests may still be there, delete marked.) */
TEST_F(LockDbTest, InsertNoWaitRc) {
  load({{110, ut::Row{110, 1, "a"}}, {120, ut::Row{120, 2, "b"}}});

  auto reader = begin(IB_TRX_READ_COMMITTED);

  ASSERT_EQ(locking_scan(reader, IB_LOCK_S), DB_SUCCESS);

  auto writer = begin();
  auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{115, 3, "c"}); });

  EXPECT_EQ(no_wait(result, writer), DB_SUCCESS);

  commit(writer);
  commit(reader);
}

/* Regression: row_search (row/row0sel.cc:2303) locks the supremum when the
isolation level IS READ COMMITTED and not otherwise: the condition is
inverted (the comment under it says READ COMMITTED does not lock gaps;
InnoDB has isolation_level != TRX_ISO_READ_COMMITTED; 7c21eac0 has the same
inversion). Under REPEATABLE READ a locking read of a range up to the end
of a page leaves the gap before the supremum open: an insert after the last
row does not wait, a phantom. */
TEST_F(LockDbTest, InsertAtEndWaitsRr) {
  load({{210, ut::Row{210, 1, "a"}}, {220, ut::Row{220, 2, "b"}}});

  auto reader = begin(IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(locking_scan(reader, IB_LOCK_S), DB_SUCCESS);

  auto writer = begin();
  auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{230, 3, "c"}); });

  EXPECT_TRUE(wait_for_lock_wait(writer, 2s)) << "a phantom: the insert after the last row read did not wait";

  commit(reader);

  EXPECT_EQ(no_wait(result, writer), DB_SUCCESS);
  commit(writer);
}

/* Regression: the same inversion: under READ COMMITTED the supremum is locked and an
insert after the last row read waits for the reader. */
TEST_F(LockDbTest, InsertAtEndNoWaitRc) {
  load({{310, ut::Row{310, 1, "a"}}, {320, ut::Row{320, 2, "b"}}});

  auto reader = begin(IB_TRX_READ_COMMITTED);

  ASSERT_EQ(locking_scan(reader, IB_LOCK_S), DB_SUCCESS);

  auto writer = begin();
  auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{330, 3, "c"}); });

  EXPECT_EQ(no_wait(result, writer), DB_SUCCESS);

  commit(writer);
  commit(reader);
}

/** Table locks: an X request waits behind S, an IS request behind the
waiting X (no barging); the commits grant them in order. */
TEST_F(LockDbTest, TableLockFifo) {
  auto a = begin();
  auto b = begin();
  auto c = begin();

  ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);

  auto rb = std::async(std::launch::async, [&] { return lock_table(b, IB_LOCK_X); });

  ASSERT_TRUE(wait_for_lock_wait(b));

  auto rc = std::async(std::launch::async, [&] { return lock_table(c, IB_LOCK_IS); });

  ASSERT_TRUE(wait_for_lock_wait(c));

  commit(a);

  ASSERT_EQ(rb.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(rb.get(), DB_SUCCESS);
  EXPECT_EQ(rc.wait_for(100ms), std::future_status::timeout) << "IS was granted with X held";

  commit(b);

  ASSERT_EQ(rc.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(rc.get(), DB_SUCCESS);
  commit(c);
}

/** Compatible table locks are granted at once. */
TEST_F(LockDbTest, TableLockCompatible) {
  auto a = begin();
  auto b = begin();
  auto c = begin();

  EXPECT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);
  EXPECT_EQ(lock_table(b, IB_LOCK_IX), DB_SUCCESS);
  EXPECT_EQ(lock_table(c, IB_LOCK_IS), DB_SUCCESS);

  commit(a);
  commit(b);
  commit(c);
}

/** A deadlock of two transactions updating two rows in opposite orders:
the one that closes the cycle gets DB_DEADLOCK, the other gets its row when
the victim rolls back. */
TEST_F(LockDbTest, DeadlockApi) {
  load({{1, ut::Row{1, 1, "a"}}, {2, ut::Row{2, 2, "b"}}});

  auto a = begin();
  auto b = begin();

  ASSERT_EQ(update(a, 1, ut::Row{1, 10, "a"}), DB_SUCCESS);
  ASSERT_EQ(update(b, 2, ut::Row{2, 20, "b"}), DB_SUCCESS);

  auto ra = async_update(a, 2, 11);

  ASSERT_TRUE(wait_for_lock_wait(a));

  EXPECT_EQ(update(b, 1, ut::Row{1, 21, "b"}), DB_DEADLOCK);
  rollback(b);

  ASSERT_EQ(ra.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(ra.get(), DB_SUCCESS);
  commit(a);

  expect_rows({{1, ut::Row{1, 10, "a"}}, {2, ut::Row{2, 11, "u"}}});
}

/** A deadlock of three: the last waiter, which closes the cycle, is the victim. */
TEST_F(LockDbTest, DeadlockApiThree) {
  load({{1, ut::Row{1, 1, ""}}, {2, ut::Row{2, 2, ""}}, {3, ut::Row{3, 3, ""}}});

  auto a = begin();
  auto b = begin();
  auto c = begin();

  ASSERT_EQ(update(a, 1, ut::Row{1, 10, ""}), DB_SUCCESS);
  ASSERT_EQ(update(b, 2, ut::Row{2, 20, ""}), DB_SUCCESS);
  ASSERT_EQ(update(c, 3, ut::Row{3, 30, ""}), DB_SUCCESS);

  auto ra = async_update(a, 2, 11);

  ASSERT_TRUE(wait_for_lock_wait(a));

  auto rb = async_update(b, 3, 21);

  ASSERT_TRUE(wait_for_lock_wait(b));

  EXPECT_EQ(update(c, 1, ut::Row{1, 31, ""}), DB_DEADLOCK);
  rollback(c);

  ASSERT_EQ(rb.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(rb.get(), DB_SUCCESS);
  commit(b);

  ASSERT_EQ(ra.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(ra.get(), DB_SUCCESS);
  commit(a);
}

/** The victim is the lighter transaction: here the waiting one, which made
fewer changes; its wait ends with DB_DEADLOCK, and the heavy requester gets
the row once the victim rolls back. */
TEST_F(LockDbTest, DeadlockApiVictimWeight) {
  ut::Rows rows;

  for (uint32_t k = 1; k <= 30; ++k) {
    rows[k] = ut::Row{k, k, ""};
  }

  load(rows);

  auto heavy = begin();
  auto light = begin();

  for (uint32_t k = 3; k <= 30; ++k) {
    ASSERT_EQ(update(heavy, k, ut::Row{k, k + 100, ""}), DB_SUCCESS);
  }

  ASSERT_EQ(update(heavy, 1, ut::Row{1, 101, ""}), DB_SUCCESS);
  ASSERT_EQ(update(light, 2, ut::Row{2, 202, ""}), DB_SUCCESS);

  auto rl = async_update(light, 1, 201);

  ASSERT_TRUE(wait_for_lock_wait(light));

  auto rh = async_update(heavy, 2, 102);

  ASSERT_EQ(rl.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(rl.get(), DB_DEADLOCK);
  rollback(light);

  ASSERT_EQ(rh.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(rh.get(), DB_SUCCESS);
  commit(heavy);
}

/** The one pass grant decision of a table queue (table_grant_pass()) against
the scan of the locks ahead of each waiter (table_has_to_wait_in_queue()),
on 2,000 random queues of IS, IX, S and X, granted and waiting, several of a
mode, of different and of the same transactions. They agree for every waiter. */
TEST_F(LockDbTest, TableGrantPassEquivalence) {
  constexpr std::array<LockMode, 4> modes{LOCK_IS, LOCK_IX, LOCK_S, LOCK_X};

  std::vector<Trx *> trxs;

  for (int i{}; i < 12; ++i) {
    trxs.push_back(trx());
  }

  std::mt19937 rng(78);
  size_t n_waiters{};

  for (int round{}; round < 2000; ++round) {
    std::vector<bool> waits(trxs.size());

    m_lock_sys->x_lock();

    for (int i = 2 + int(rng() % 10); i > 0; --i) {
      const auto t = rng() % trxs.size();
      const bool wait = rng() % 2 == 0 && !waits[t];

      (void)m_lock_sys->table_create(m_table, LockMode(modes[rng() % modes.size()] | (wait ? LOCK_WAIT : 0)), trxs[t]);
      waits[t] = waits[t] || wait;
    }

    LockSys::GrantPlan plan;

    m_lock_sys->table_grant_pass(m_table, &plan);

    for (auto [lock, granted] : plan) {
      EXPECT_EQ(granted, !m_lock_sys->table_has_to_wait_in_queue(const_cast<Lock *>(lock))) << "round " << round;
      ++n_waiters;
    }

    for (auto t : trxs) {
      m_lock_sys->release(t);
      t->m_wait_lock = nullptr;
      t->m_blocking_trx = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    m_lock_sys->x_unlock();
  }

  EXPECT_GT(n_waiters, 0u);
}

/** The deadlock that the one edge walk misses (lock0deadlock.h): a, b and i
hold S on a record; a requests X: it waits for b and i, its edge names one
of them; b requests X: it waits for a (a's S and a's waiting X) and for i;
a and b are deadlocked whatever i does, i is an idle transaction that never
ends. The walk from b goes b -> a -> i (or b -> i) and stops at i, which does
not wait; the full search finds b -> a -> b at once: the request of b gets
the deadlock (a and b weigh the same: b, the requester, is the victim). */
TEST_F(LockDbTest, DeadlockPastIdleBlocker) {
  const auto r = key(1, 2);

  for (int order{}; order < 2; ++order) {
    auto a = trx();
    auto b = trx();
    auto i = trx();

    /* The idle holder first in the queue in one round, last in the other. */
    if (order == 0) {
      (void)rec_lock(r, LOCK_S | LOCK_REC_NOT_GAP, i);
    }

    (void)rec_lock(r, LOCK_S | LOCK_REC_NOT_GAP, a);
    (void)rec_lock(r, LOCK_S | LOCK_REC_NOT_GAP, b);

    if (order == 1) {
      (void)rec_lock(r, LOCK_S | LOCK_REC_NOT_GAP, i);
    }

    auto areq = rec_lock(r, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, a);

    EXPECT_FALSE(deadlock(areq, a));

    auto breq = rec_lock(r, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, b);

    EXPECT_TRUE(deadlock(breq, b)) << "order " << order << ": a deadlock past an idle blocker was missed";
    EXPECT_FALSE(a->m_was_chosen_as_deadlock_victim);

    finish();
  }
}

/** The queue local deadlock check against the full search (lock0deadlock.h,
step 3): random requests on one record, as the engine makes them (a request
waits if it conflicts with a lock in the queue, a transaction waits for at
most one lock; upgrades of S holders to X), some transactions also waiting on
another key. At each new wait both decide from that transaction: if the
check says no cycle, the full search finds none; when every wait is in the
queue, the check is exact. A wait that closes a cycle is cancelled, as the
victim's would be: no older cycle in the graph. 2000 rounds. */
TEST_F(LockDbTest, LocalCheckEquivalence) {
  const auto r = key(1, 2);
  const auto other = key(2, 2);
  std::mt19937 rng(17);
  size_t n_cycles{};
  size_t n_exact{};
  size_t n_outside{};

  for (int round{}; round < 2000; ++round) {
    const int n = 2 + int(rng() % 5);
    std::vector<Trx *> t;

    for (int i{}; i < n; ++i) {
      t.push_back(trx());
    }

    std::vector<bool> waits(n, false);

    /* The checks of the new wait of t[i], on lock. */
    auto check = [&](int i, Lock *lock) {
      m_lock_sys->x_lock();

      ASSERT_EQ(t[i]->m_wait_lock, lock);

      const bool local = m_lock_sys->deadlock_local(t[i]);
      Trx *pred{};
      const auto walk = m_lock_sys->deadlock_find(t[i], &pred);

      ASSERT_NE(walk, DeadlockWalk::TOO_DEEP) << "round " << round;

      const bool cycle = walk == DeadlockWalk::CYCLE;

      bool in_queue{true};

      for (auto x : t) {
        if (x->m_wait_lock != nullptr && x->m_wait_lock->key() != lock->key()) {
          in_queue = false;
        }
      }

      if (!local) {
        EXPECT_FALSE(cycle) << "round " << round << ": a cycle the queue local check ruled out";
      } else if (in_queue) {
        EXPECT_TRUE(cycle) << "round " << round << ": the queue local check saw a cycle the full search did not";
      }

      n_exact += in_queue ? 1 : 0;
      n_outside += in_queue ? 0 : 1;
      n_cycles += cycle ? 1 : 0;

      if (cycle) {
        m_lock_sys->cancel_waiting_and_release(lock);
        waits[i] = false;
      }

      m_lock_sys->x_unlock();
    };

    for (int step{}; step < 4 * n; ++step) {
      const int i = int(rng() % n);

      if (waits[i]) {
        continue;
      }

      if (rng() % 8 == 0) {
        /* A wait outside the queue: for X on the other key, held by another. */
        const int h = int(rng() % n);

        if (h != i) {
          (void)rec_lock(other, LOCK_X | LOCK_REC_NOT_GAP, t[h]);

          waits[i] = true;
          check(i, rec_lock(other, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, t[i]));
        }

        continue;
      }

      const ulint mode = (rng() % 2 ? LOCK_S : LOCK_X) | (rng() % 2 ? LOCK_REC_NOT_GAP : 0);

      m_lock_sys->x_lock();
      const bool conflict = m_lock_sys->rec_other_has_conflicting(r, LockMode(mode), t[i]) != nullptr;
      m_lock_sys->x_unlock();

      if (conflict) {
        waits[i] = true;
        check(i, rec_lock(r, mode | LOCK_WAIT, t[i]));
      } else {
        (void)rec_lock(r, mode, t[i]);
      }
    }

    finish();
  }

  std::printf("LocalCheckEquivalence: %zu cycles, %zu checks with all waits in the queue, %zu with one outside\n", n_cycles, n_exact, n_outside);

  EXPECT_GT(n_cycles, 0u);
  EXPECT_GT(n_exact, 0u);
  EXPECT_GT(n_outside, 0u);
}

/** A cycle closed through an edge that the one edge graph does not hold, at
depth 2: d holds X on r4, then waits for X on q, which i (idle) and a hold
in S: d's edge names one of them (neither waits yet). e holds X on r3 and
waits for r4 (held by d). a requests r3 (held by e): the cycle a -> e -> d
-> a closes through the edge d -> a, which d's edge may not name. The walk
from a may end at i; the full search finds it: a deadlock at a's request. */
TEST_F(LockDbTest, DeadlockClosedOffEdge) {
  const auto q = key(1, 2);
  const auto r3 = key(3, 2);
  const auto r4 = key(4, 2);

  for (int order{}; order < 2; ++order) {
    auto a = trx();
    auto d = trx();
    auto e = trx();
    auto i = trx();

    (void)rec_lock(r4, LOCK_X | LOCK_REC_NOT_GAP, d);
    (void)rec_lock(r3, LOCK_X | LOCK_REC_NOT_GAP, e);

    (void)rec_lock(q, LOCK_S | LOCK_REC_NOT_GAP, order == 0 ? i : a);
    (void)rec_lock(q, LOCK_S | LOCK_REC_NOT_GAP, order == 0 ? a : i);

    auto dreq = rec_lock(q, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, d);
    EXPECT_FALSE(deadlock(dreq, d));

    auto ereq = rec_lock(r4, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, e);
    EXPECT_FALSE(deadlock(ereq, e));

    auto areq = rec_lock(r3, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, a);
    const bool victim = deadlock(areq, a);

    /* Either a is the victim, or another member of the cycle was chosen. */
    EXPECT_TRUE(victim || d->m_was_chosen_as_deadlock_victim || e->m_was_chosen_as_deadlock_victim)
      << "order " << order << ": the cycle a -> e -> d -> a was missed";

    finish();
  }
}

/** remove_all_on_table() (DROP and TRUNCATE) removes the table locks of
the transactions and their record locks on the table. */
TEST_F(LockDbTest, RemoveAllOnTableTableLocks) {
  auto a = trx();

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);
  ASSERT_FALSE(m_table->m_locks.empty());

  m_lock_sys->remove_all_on_table(m_table, true);

  EXPECT_TRUE(m_table->m_locks.empty());
  EXPECT_TRUE(a->m_trx_locks.empty());
}

/* Regression: remove_all_on_table_for_trx() tests lock->m_table.m_table == table
for a record lock: the union arm of a table lock. For a record lock it is
m_rec.m_index, never the table, so the record locks of the dropped or
truncated table stay in the lock table, on pages of a dropped table, with
a dangling index (lock/lock0lock.cc:2428; upstream: lock->index->table). */
TEST_F(LockDbTest, RemoveAllOnTableRecLocks) {
  const auto k = key(1, 2);
  auto a = trx();

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);
  (void)rec_lock(k, LOCK_X | LOCK_REC_NOT_GAP, a);

  m_lock_sys->remove_all_on_table(m_table, true);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_FALSE(has_queue(m_lock_sys, k));
}

/** A transaction that alone holds IX on a table has it exclusively. */
TEST_F(LockDbTest, TableExclusive) {
  auto a = trx();

  EXPECT_FALSE(m_lock_sys->is_table_exclusive(m_table, a));

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);

  EXPECT_TRUE(m_lock_sys->is_table_exclusive(m_table, a));
}

/* Regression: is_table_exclusive() returns true at the first IX lock of trx in the
queue without looking at the locks after it: an IS or IX lock of another
transaction later in the queue is missed (lock/lock0lock.cc:799 returns
where upstream only set ok = TRUE and went on with the loop). Not called by
the engine. */
TEST_F(LockDbTest, TableExclusiveOther) {
  auto a = trx();
  auto b = trx();

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);
  ASSERT_EQ(lock_table(b, IB_LOCK_IS), DB_SUCCESS);

  EXPECT_FALSE(m_lock_sys->is_table_exclusive(m_table, a));
  EXPECT_FALSE(m_lock_sys->is_table_exclusive(m_table, b));
}

/** A transaction locking only the destination of an ALTER has no source table. */
TEST_F(LockDbTest, SrcTable) {
  auto a = trx();
  LockMode mode{LOCK_X};

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);

  EXPECT_EQ(m_lock_sys->get_src_table(a, m_table, &mode), m_table);
  EXPECT_EQ(mode, LOCK_NONE);
}

/** The one pass grant decision of a release (rec_grant_pass()) against the
old one, a scan of the locks ahead of each waiter (rec_has_to_wait_in_queue()),
on 3,000 random key queues: every mode class (S, X; plain, gap, record only,
insert intention), a fence in a quarter of them (all gap requests), several
locks of a class from different transactions and from the waiter's own
transaction, up to 12 waiters. They agree for every waiter. */
TEST_F(LockTest, GrantPassEquivalence) {
  const std::array<ulint, 7> modes{
    LOCK_S, LOCK_X, LOCK_S | LOCK_GAP, LOCK_X | LOCK_GAP, LOCK_S | LOCK_REC_NOT_GAP, LOCK_X | LOCK_REC_NOT_GAP,
    LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION
  };
  std::vector<Trx *> trxs;

  for (int i{}; i < 16; ++i) {
    trxs.push_back(trx());
  }

  std::mt19937 rng(77);
  size_t n_waiters{};
  size_t n_granted{};

  for (int round{}; round < 3000; ++round) {
    const auto k = key(page_no_t(1000 + round), 2);
    const bool on_fence = rng() % 4 == 0;
    const int n = 2 + int(rng() % 14);
    std::vector<bool> waits(trxs.size());

    for (int i{}; i < n; ++i) {
      auto t = int(rng() % trxs.size());
      auto mode = modes[rng() % modes.size()];
      const bool wait = rng() % 2 == 0 && !waits[t];

      if (on_fence) {
        mode = fence(mode);
      }

      (void)rec_lock(k, mode | (wait ? LOCK_WAIT : 0), trxs[t]);

      waits[t] = waits[t] || wait;
    }

    m_lock_sys->x_lock();

    auto &queue = *m_lock_sys->rec_queue(k);
    LockSys::GrantPlan plan;

    m_lock_sys->rec_grant_pass(queue, &plan);

    size_t n_wait{};

    for (auto lock : queue) {
      if (lock->is_waiting()) {
        ++n_wait;
      }
    }

    EXPECT_EQ(plan.size(), n_wait) << "round " << round;

    for (auto [lock, granted] : plan) {
      const auto must_wait = m_lock_sys->rec_has_to_wait_in_queue(queue, const_cast<Lock *>(lock));

      EXPECT_EQ(granted, !must_wait) << "round " << round << " lock " << Lock_mode_type(lock->precise_mode()) << " fence " << on_fence;

      ++n_waiters;
      n_granted += granted ? 1 : 0;
    }

    /* Release all, as the teardown does: the waits end. */
    for (auto t : trxs) {
      m_lock_sys->release(t);
      t->m_wait_lock = nullptr;
      t->m_blocking_trx = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    m_lock_sys->x_unlock();
  }

  EXPECT_GT(n_granted, 0u);
  EXPECT_LT(n_granted, n_waiters);

  std::printf("GrantPassEquivalence: %zu waiters, %zu would be granted\n", n_waiters, n_granted);
}

/** The in-memory lock queues, timed. */
class LockPerf : public LockTest {};

/** Create and release one record lock, uncontended. */
TEST_F(LockPerf, RecCreateRelease) {
  auto a = trx();

  auto r = ut::bench("rec_create_release", 200000, [&](uint64_t i) {
    const auto k = key(page_no_t(i % 64), 2);

    {
      LockSys::KeyLatch latch(*m_lock_sys, k);
      ut::keep(m_lock_sys->rec_create(LOCK_X, k, nullptr, a));
    }

    m_lock_sys->release(a);
  });

  EXPECT_LT(r.ns_per_op(), 20000);
}

/** Lock the records of a page one by one: a lock and a queue per key. */
TEST_F(LockPerf, AddToQueue) {
  auto a = trx();

  auto r = ut::bench("rec_add_to_queue", 200000, [&](uint64_t i) {
    const auto k = key(1, 2 + i % 70);

    {
      LockSys::KeyLatch latch(*m_lock_sys, k);
      ut::keep(m_lock_sys->rec_add_to_queue(LockMode(LOCK_REC | LOCK_S), k, nullptr, a));
    }

    if (i % 70 == 69) {
      m_lock_sys->release(a);
    }
  });

  EXPECT_LT(r.ns_per_op(), 20000);
}

/** A release is linear in the locks of the transaction: the cost of a lock of a release of 64 times as
many locks is about the same, best of three. One lock to a key, keys apart in all the shards. */
TEST_F(LockPerf, ReleaseByKey) {
  const auto per_lock = [&](uint32_t n) {
    auto a = trx();

    for (uint32_t i{}; i < n; ++i) {
      const auto k = key(page_no_t(1 + i / 4096), 2 + i % 4096);
      LockSys::KeyLatch latch(*m_lock_sys, k);

      ut::keep(m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP), k, nullptr, a));
    }

    const auto start = std::chrono::steady_clock::now();

    m_lock_sys->release(a);

    return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / double(n);
  };

  double small{1e18};
  double big{1e18};

  for (int i{}; i < 3; ++i) {
    small = std::min(small, per_lock(1024));
    big = std::min(big, per_lock(65536));
  }

  std::printf("ReleaseByKey: %.0f ns per lock at 1024 locks, %.0f ns at 65536\n", small, big);

  EXPECT_LT(big, 4 * small) << "a release that isn't linear in the locks";
}

/** A lock request on a key that has no queue, and on one that has the transaction's own lock: what a
scan pays for each record. */
TEST_F(LockPerf, LockKey) {
  auto a = trx();

  auto fresh = ut::bench("rec_lock_new_key", 200000, [&](uint64_t i) {
    const auto k = key(page_no_t(1 + i / 4096), 2 + i % 4096);
    LockSys::KeyLatch latch(*m_lock_sys, k);

    ut::keep(m_lock_sys->rec_lock(false, LockMode(LOCK_X | LOCK_REC_NOT_GAP), k, nullptr, a, nullptr));
  });

  auto again = ut::bench("rec_lock_held_key", 200000, [&](uint64_t i) {
    const auto k = key(page_no_t(1 + (i % 4096) / 4096), 2 + i % 4096);
    LockSys::KeyLatch latch(*m_lock_sys, k);

    ut::keep(m_lock_sys->rec_lock(false, LockMode(LOCK_X | LOCK_REC_NOT_GAP), k, nullptr, a, nullptr));
  });

  m_lock_sys->release(a);

  EXPECT_LT(fresh.ns_per_op(), 20000);
  EXPECT_LT(again.ns_per_op(), 20000);
}

/** Threads each with a transaction locking keys of its own, releasing
every 64 locks: the key shard latches, no global mutex. */
TEST_F(LockPerf, DisjointMt) {
  for (int n_threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n_threads; ++t) {
      trxs.push_back(trx());
    }

    auto r = ut::bench_mt(std::format("disjoint_rec_lock_t{}", n_threads), n_threads, 50000, [&](int t, uint64_t i) {
      const auto k = key(page_no_t(1000 * (1 + t) + i % 64), 2);

      {
        LockSys::KeyLatch latch(*m_lock_sys, k);
        ut::keep(m_lock_sys->rec_create(LOCK_X, k, nullptr, trxs[t]));
      }

      if (i % 64 == 63) {
        m_lock_sys->release(trxs[t]);
      }
    });

    EXPECT_LT(r.ns_per_op(), 100000) << n_threads;
  }
}

/** A lock handed over: a request waits behind the holder, the holder's
lock goes and the waiter is granted; then the roles swap. */
TEST_F(LockPerf, Handoff) {
  const auto k = key(1, 2);
  std::array<Trx *, 2> t{trx(), trx()};
  auto holder = rec_lock(k, LOCK_X, t[0]);

  auto r = ut::bench("grant_handoff", 100000, [&](uint64_t i) {
    auto next = t[(i + 1) % 2];

    LockSys::KeyLatch latch(*m_lock_sys, k);
    auto waiter = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_WAIT), k, nullptr, next);
    m_lock_sys->rec_dequeue_from_page(holder);

    holder = waiter;
  });

  EXPECT_FALSE(holder->is_waiting());
  EXPECT_LT(r.ns_per_op(), 20000);
}

/** Started transactions, timed. */
class LockDbPerf : public LockDbTest {};

/** The deadlock check of a request at the head of a chain of waits, no cycle. */
TEST_F(LockDbPerf, DeadlockCheck) {
  for (int n : {10, 50, 190}) {
    std::vector<Trx *> t;

    for (int i = 0; i < n; ++i) {
      t.push_back(trx());
      (void)rec_lock(key(page_no_t(i), 2), LOCK_X, t.back());
    }

    for (int i = 1; i + 1 < n; ++i) {
      (void)rec_lock(key(page_no_t(i + 1), 2), LOCK_X | LOCK_WAIT, t[i]);
    }

    auto req = rec_lock(key(1, 2), LOCK_X | LOCK_WAIT, t[0]);

    /* The check itself, with the lock system latched X for the whole loop (a
    request takes X only for the full search, not for the latch free walk). */
    m_lock_sys->x_lock();
    auto r = ut::bench(std::format("deadlock_check_chain{}", n), 2000, [&](uint64_t) { EXPECT_FALSE(deadlock(req, t[0])); });
    m_lock_sys->x_unlock();

    EXPECT_LT(r.ns_per_op(), 5000.0 * n);

    finish();
  }
}

/** A row lock handed over between two transactions of two threads: the
time from the holder's commit to the waiter's return. */
TEST_F(LockDbPerf, ApiHandoff) {
  constexpr int N = 100;

  load({{1, ut::Row{1, 1, ""}}});

  std::chrono::duration<double> total{};

  for (int i{}; i < N; ++i) {
    auto holder = begin();

    ASSERT_EQ(update(holder, 1, ut::Row{1, uint32_t(i), ""}), DB_SUCCESS);

    auto waiter = begin();
    std::chrono::steady_clock::time_point granted;

    auto result = std::async(std::launch::async, [&] {
      auto err = update(waiter, 1, ut::Row{1, uint32_t(i + 1000), ""});
      granted = std::chrono::steady_clock::now();
      return err;
    });

    ASSERT_TRUE(wait_for_lock_wait(waiter));

    const auto start = std::chrono::steady_clock::now();

    commit(holder);

    ASSERT_EQ(result.get(), DB_SUCCESS);
    total += granted - start;
    commit(waiter);
  }

  ut::BenchResult r{"api_lock_handoff", 1, N, total.count()};

  ut::report(r);

  EXPECT_LT(r.ns_per_op(), 50e6);
}

}  // namespace
