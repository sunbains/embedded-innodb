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

  LockTest      in memory (SysTest: no disk), locks on page ids that exist
                only in the lock table: the queues, the conflict rules, the
                grants, inheritance and moves of locks.
  LockPageTest  the page operations (split, merge, reorganize, ...) on copies
                of real index pages, with the lock queues validated against
                the real pages.
  LockDbTest    deadlocks, waits and grants of transactions of a started
                instance, in memory and through the API.
  LockPerf      performance. */

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
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

/** Bits in the record lock bitmaps the tests create with rec_create_low(). */
constexpr ulint N_BITS = 104;

/** A space id no real tablespace has: locks on its pages exist only in the lock table. */
constexpr space_id_t FAKE_SPACE = 0xFFFF0;

/** The frame of a page for a block: lock creation sizes its bitmap from
the number of records in the page heap. */
struct Frame {
  Frame() : m_buf(static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE))) {
    memset(m_buf, 0, UNIV_PAGE_SIZE);
    mach_write_to_2(m_buf + PAGE_HEADER + PAGE_N_HEAP, 10);
  }

  ~Frame() { std::free(m_buf); }

  Frame(const Frame &) = delete;
  Frame &operator=(const Frame &) = delete;

  byte *m_buf;
};

/** A free block of the buffer pool, taken for page_id. */
void init_block(BufBlock &block, Frame &frame, const PageId &page_id) {
  block.m_frame = frame.m_buf;
  buf_block_set_state(&block, BUF_BLOCK_READY_FOR_USE);
  buf_block_set_file_page(&block, page_id);
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
fake pages, no index. */
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
    mutex_enter(m_mutex);

    for (auto t : m_trxs) {
      m_lock_sys->release_off_trx_sys_mutex(t);
    }

    for (auto t : m_trxs) {
      t->m_wait_lock = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    mutex_exit(m_mutex);

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

  /** A record lock of trx on page_id, heap_no, created with the trx sys mutex. */
  Lock *rec_lock(const PageId &page_id, ulint type_mode, ulint heap_no, Trx *trx) {
    mutex_enter(m_mutex);
    auto lock = m_lock_sys->rec_create_low(page_id, LockMode(type_mode), heap_no, N_BITS, nullptr, trx);
    mutex_exit(m_mutex);

    return lock;
  }

  /** A record lock request added to the queue, as the engine does once it knows it need not wait. */
  Lock *add(const BufBlock &block, ulint type_mode, ulint heap_no, Trx *trx) {
    mutex_enter(m_mutex);
    auto lock = m_lock_sys->rec_add_to_queue(LockMode(type_mode), &block, heap_no, nullptr, trx);
    mutex_exit(m_mutex);

    return lock;
  }

  /** @return true if a request of trx for type_mode on page_id, heap_no would have to wait. */
  bool waits(const PageId &page_id, ulint type_mode, ulint heap_no, Trx *trx) {
    mutex_enter(m_mutex);
    auto lock = m_lock_sys->rec_other_has_conflicting(page_id, LockMode(type_mode), heap_no, trx);
    mutex_exit(m_mutex);

    return lock != nullptr;
  }

  /** @return true if the waiting lock has to keep waiting behind the locks ahead of it. */
  bool must_wait(Lock *lock) {
    mutex_enter(m_mutex);
    const auto &queue = m_lock_sys->m_rec_locks.at(lock->page_id());
    auto ret = m_lock_sys->rec_has_to_wait_in_queue(queue, lock, lock->rec_find_set_bit());
    mutex_exit(m_mutex);

    return ret;
  }

  /** Remove lock from its queue, granting the waiters that can go. */
  void dequeue(Lock *lock) {
    mutex_enter(m_mutex);
    m_lock_sys->rec_dequeue_from_page(lock);
    mutex_exit(m_mutex);
  }

  void release(Trx *trx) {
    mutex_enter(m_mutex);
    m_lock_sys->release_off_trx_sys_mutex(trx);
    mutex_exit(m_mutex);
  }

  /** @return the lock of trx on page_id with bit heap_no set, or nullptr. */
  const Lock *find(const PageId &page_id, ulint heap_no, const Trx *trx) {
    auto it = m_lock_sys->m_rec_locks.find(page_id);

    if (it == m_lock_sys->m_rec_locks.end()) {
      return nullptr;
    }

    for (auto lock : it->second) {
      if (lock->m_trx == trx && lock->rec_is_nth_bit_set(heap_no)) {
        return lock;
      }
    }

    return nullptr;
  }

  /** @return the locks on page_id with bit heap_no set, in queue order. */
  std::vector<const Lock *> queue(const PageId &page_id, ulint heap_no) {
    std::vector<const Lock *> locks;

    if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
      for (auto lock : it->second) {
        if (lock->rec_is_nth_bit_set(heap_no)) {
          locks.push_back(lock);
        }
      }
    }

    return locks;
  }

  /** @return the number of locks on page_id, set bits or not. */
  size_t n_locks(const PageId &page_id) {
    auto it = m_lock_sys->m_rec_locks.find(page_id);

    return it == m_lock_sys->m_rec_locks.end() ? 0 : it->second.size();
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

      EXPECT_EQ(req->has_to_wait_for(held, ULINT_UNDEFINED), !compat[i][j]) << i << " " << j;
      EXPECT_EQ(compat[i][j], compat[j][i]) << "the matrix is symmetric";
      EXPECT_FALSE(req->has_to_wait_for(own, ULINT_UNDEFINED)) << i << " " << j;
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

/** rec_create_low() keeps every flag, sets only the bit of the record,
appends the lock to the page queue and to the locks of the transaction, and
a waiting lock becomes the wait lock of the transaction. */
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
  const PageId page_id{FAKE_SPACE, 1};
  ulint heap_no{PAGE_HEAP_NO_USER_LOW};

  for (auto mode : {LOCK_S, LOCK_X}) {
    for (auto f : flags) {
      auto t = trx();
      auto lock = rec_lock(page_id, mode | f, heap_no, t);

      ASSERT_NE(lock, nullptr);
      EXPECT_EQ(lock->type(), LOCK_REC);
      EXPECT_EQ(lock->mode(), mode);
      EXPECT_EQ(lock->precise_mode(), LockMode(LOCK_REC | mode | f)) << f;
      EXPECT_EQ(lock->rec_find_set_bit(), heap_no);
      EXPECT_EQ(lock->m_trx, t);
      EXPECT_EQ(t->m_trx_locks.back(), lock);
      EXPECT_EQ(m_lock_sys->m_rec_locks.at(page_id).back(), lock);
      EXPECT_EQ(lock->is_waiting(), (f & LOCK_WAIT) != 0);
      EXPECT_EQ(t->m_wait_lock, (f & LOCK_WAIT) ? lock : nullptr);

      ++heap_no;
    }
  }
}

/** A lock on the supremum is a gap lock: the gap bit is not stored. The
insert intention flag stays. */
TEST_F(LockTest, RecCreateSupremum) {
  const PageId page_id{FAKE_SPACE, 2};

  auto gap = rec_lock(page_id, LOCK_X | LOCK_GAP, PAGE_HEAP_NO_SUPREMUM, trx());
  auto ii = rec_lock(page_id, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, PAGE_HEAP_NO_SUPREMUM, trx());

  EXPECT_EQ(gap->precise_mode(), LockMode(LOCK_REC | LOCK_X));
  EXPECT_FALSE(gap->rec_is_gap());
  EXPECT_EQ(ii->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_INSERT_INTENTION));
}

/** The bitmap has a margin for records inserted later and is a whole number
of bytes; the bits past its end read as unset. */
TEST_F(LockTest, RecCreateBitmap) {
  auto lock = rec_lock(PageId{FAKE_SPACE, 3}, LOCK_S, N_BITS - 1, trx());

  EXPECT_EQ(lock->rec_get_n_bits() % 8, 0u);
  EXPECT_GE(lock->rec_get_n_bits(), N_BITS + 64);
  EXPECT_TRUE(lock->rec_is_nth_bit_set(N_BITS - 1));
  EXPECT_FALSE(lock->rec_is_nth_bit_set(lock->rec_get_n_bits()));
  EXPECT_FALSE(lock->rec_is_nth_bit_set(lock->rec_get_n_bits() + 1000));

  for (ulint i{}; i < N_BITS - 1; ++i) {
    EXPECT_FALSE(lock->rec_is_nth_bit_set(i)) << i;
  }
}

/** The locks of a page are queued in the order of their creation, whatever
the transaction. */
TEST_F(LockTest, QueueOrder) {
  const PageId page_id{FAKE_SPACE, 4};
  std::vector<const Lock *> created;
  std::array<Trx *, 3> trxs{trx(), trx(), trx()};

  for (int i{}; i < 9; ++i) {
    created.push_back(rec_lock(page_id, LOCK_S | LOCK_GAP, 5, trxs[i % 3]));
  }

  EXPECT_EQ(queue(page_id, 5), created);
}

/** One case of the conflict rules: a request of trx b for req, with a lock
of trx a for held on the same record. */
struct Conflict {
  const char *m_what;
  ulint m_held;
  ulint m_req;
  ulint m_heap_no;
  bool m_waits;
};

/** The rules of Lock::rec_blocks(): when a record lock request has to wait
for a lock of another transaction on the same record. */
TEST_F(LockTest, ConflictRules) {
  constexpr ulint REC = PAGE_HEAP_NO_USER_LOW + 3;
  constexpr ulint SUP = PAGE_HEAP_NO_SUPREMUM;
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
    {"supremum is a gap", LOCK_X, LOCK_X, SUP, false},
    {"supremum S vs X", LOCK_S, LOCK_X, SUP, false},
    {"insert intention on supremum", LOCK_S, II, SUP, true},
    {"insert intention vs waiting gap", LOCK_X | LOCK_GAP | LOCK_WAIT, II, REC, true},
    {"waiting X blocks S", LOCK_X | LOCK_WAIT, LOCK_S, REC, true},
    {"waiting S blocks X", LOCK_S | LOCK_WAIT, LOCK_X | LOCK_REC_NOT_GAP, REC, true},
    {"waiting insert intention blocks nothing", II | LOCK_WAIT, LOCK_X, REC, false},
  };

  page_no_t page_no{100};

  for (const auto &c : cases) {
    const PageId page_id{FAKE_SPACE, page_no++};
    auto a = trx();
    auto b = trx();

    ASSERT_NE(rec_lock(page_id, c.m_held, c.m_heap_no, a), nullptr);
    EXPECT_EQ(waits(page_id, c.m_req, c.m_heap_no, b), c.m_waits) << c.m_what;

    /* Never for one's own lock, nor on another record. */
    EXPECT_FALSE(waits(page_id, c.m_req, c.m_heap_no, a)) << c.m_what;
    EXPECT_FALSE(waits(page_id, c.m_req, c.m_heap_no + 1, b)) << c.m_what;
  }
}

/** rec_other_has_conflicting() returns the first lock in the queue that
blocks the request. */
TEST_F(LockTest, ConflictIsFirstBlocker) {
  const PageId page_id{FAKE_SPACE, 5};
  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(page_id, LOCK_S | LOCK_GAP, 4, a);
  auto first = rec_lock(page_id, LOCK_S, 4, b);
  (void)rec_lock(page_id, LOCK_S, 4, c);

  mutex_enter(m_mutex);
  EXPECT_EQ(m_lock_sys->rec_other_has_conflicting(page_id, LOCK_X, 4, a), first);
  mutex_exit(m_mutex);
}

/** rec_add_to_queue() sets a bit in a lock of the same transaction and
precise mode on the page, rather than creating another lock. */
TEST_F(LockTest, AddToQueueReuse) {
  const PageId page_id{FAKE_SPACE, 6};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto b = trx();

  auto l3 = add(block, LOCK_REC | LOCK_S, 3, a);
  auto l4 = add(block, LOCK_REC | LOCK_S, 4, a);

  EXPECT_EQ(l3, l4);
  EXPECT_TRUE(l3->rec_is_nth_bit_set(3));
  EXPECT_TRUE(l3->rec_is_nth_bit_set(4));
  EXPECT_EQ(n_locks(page_id), 1u);

  /* Another precise mode or transaction: a lock of its own. */
  auto gap = add(block, LOCK_REC | LOCK_S | LOCK_GAP, 5, a);
  auto other = add(block, LOCK_REC | LOCK_S, 5, b);

  EXPECT_NE(gap, l3);
  EXPECT_NE(other, l3);
  EXPECT_NE(other, gap);
  EXPECT_EQ(n_locks(page_id), 3u);
  EXPECT_EQ(a->m_trx_locks.size(), 2u);
}

/** A request behind a waiting lock on the same record gets a lock of its
own, after the waiting one (no lock goes ahead of a waiting one), even if
the transaction has a similar lock on the page. */
TEST_F(LockTest, AddToQueueBehindWaiter) {
  const PageId page_id{FAKE_SPACE, 7};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto b = trx();

  auto similar = add(block, LOCK_REC | LOCK_S | LOCK_GAP, 4, a);
  auto waiting = rec_lock(page_id, LOCK_X | LOCK_WAIT, 3, b);
  auto lock = add(block, LOCK_REC | LOCK_S | LOCK_GAP, 3, a);

  EXPECT_NE(lock, similar);
  EXPECT_FALSE(similar->rec_is_nth_bit_set(3));
  EXPECT_EQ(queue(page_id, 3), (std::vector<const Lock *>{waiting, lock}));
}

/** A waiting request is always a lock of its own. */
TEST_F(LockTest, AddToQueueWaitNew) {
  const PageId page_id{FAKE_SPACE, 8};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto granted = add(block, LOCK_REC | LOCK_X | LOCK_GAP, 3, a);
  auto waiting = add(block, LOCK_REC | LOCK_X | LOCK_GAP | LOCK_WAIT, 4, a);

  EXPECT_NE(granted, waiting);
  EXPECT_TRUE(waiting->is_waiting());
  EXPECT_EQ(a->m_wait_lock, waiting);
}

/** On the supremum a gap request is a plain request: it reuses the plain
lock of the transaction on the page. */
TEST_F(LockTest, AddToQueueSupremum) {
  const PageId page_id{FAKE_SPACE, 9};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto plain = add(block, LOCK_REC | LOCK_S, 3, a);
  auto sup = add(block, LOCK_REC | LOCK_S | LOCK_GAP, PAGE_HEAP_NO_SUPREMUM, a);

  EXPECT_EQ(plain, sup);
  EXPECT_TRUE(plain->rec_is_nth_bit_set(PAGE_HEAP_NO_SUPREMUM));
}

/** Waiters are granted in the order they came: releasing the holder grants
the first waiter only, releasing it grants the next. */
TEST_F(LockTest, GrantFifo) {
  const PageId page_id{FAKE_SPACE, 10};
  auto a = trx();
  auto b = trx();
  auto c = trx();

  auto la = rec_lock(page_id, LOCK_X, 4, a);
  auto lb = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, b);
  auto lc = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, c);

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
  const PageId page_id{FAKE_SPACE, 11};
  auto la = rec_lock(page_id, LOCK_X, 4, trx());
  auto lb = rec_lock(page_id, LOCK_S | LOCK_WAIT, 4, trx());
  auto lc = rec_lock(page_id, LOCK_S | LOCK_WAIT, 4, trx());

  dequeue(la);

  EXPECT_FALSE(lb->is_waiting());
  EXPECT_FALSE(lc->is_waiting());
}

/** A request compatible with the holder still waits behind an incompatible
waiter: no barging. Releasing the holder grants the waiter ahead only. */
TEST_F(LockTest, NoBarging) {
  const PageId page_id{FAKE_SPACE, 12};
  auto la = rec_lock(page_id, LOCK_S, 4, trx());
  auto lb = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, trx());
  auto lc = rec_lock(page_id, LOCK_S | LOCK_WAIT, 4, trx());

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
  const PageId page_id{FAKE_SPACE, 13};
  auto first = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, trx());

  (void)rec_lock(page_id, LOCK_X, 4, trx());

  EXPECT_FALSE(must_wait(first));
}

/** An insert intention waiting for a gap lock is granted when the gap
lock goes. */
TEST_F(LockTest, GrantInsertIntention) {
  const PageId page_id{FAKE_SPACE, 14};
  auto gap = rec_lock(page_id, LOCK_S | LOCK_GAP, 4, trx());
  auto ii = rec_lock(page_id, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION | LOCK_WAIT, 4, trx());

  EXPECT_TRUE(must_wait(ii));

  dequeue(gap);

  EXPECT_FALSE(ii->is_waiting());
  EXPECT_TRUE(ii->rec_is_insert_intention());
}

/** Releasing a lock on another record of the page grants nothing. */
TEST_F(LockTest, GrantOtherRecord) {
  const PageId page_id{FAKE_SPACE, 15};
  auto holder = rec_lock(page_id, LOCK_X, 4, trx());
  auto other = rec_lock(page_id, LOCK_X, 5, trx());
  auto waiter = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, trx());

  dequeue(other);

  EXPECT_TRUE(waiter->is_waiting());

  dequeue(holder);

  EXPECT_FALSE(waiter->is_waiting());
}

/** Releasing a transaction frees all its locks, removes the pages left
without locks from the lock table, keeps the locks of the others and grants
their waiting locks. */
TEST_F(LockTest, ReleaseAll) {
  auto a = trx();
  auto b = trx();

  for (page_no_t p = 20; p < 23; ++p) {
    for (ulint h = 2; h < 6; ++h) {
      (void)rec_lock(PageId{FAKE_SPACE, p}, LOCK_X | LOCK_REC_NOT_GAP, h, a);
    }
  }

  auto kept = rec_lock(PageId{FAKE_SPACE, 21}, LOCK_S | LOCK_GAP, 7, b);
  auto waiter = rec_lock(PageId{FAKE_SPACE, 22}, LOCK_S | LOCK_WAIT, 3, b);

  EXPECT_TRUE(must_wait(waiter));

  release(a);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(PageId{FAKE_SPACE, 20}));
  EXPECT_EQ(n_locks(PageId{FAKE_SPACE, 21}), 1u);
  EXPECT_EQ(queue(PageId{FAKE_SPACE, 21}, 7), (std::vector<const Lock *>{kept}));
  EXPECT_FALSE(waiter->is_waiting());
  EXPECT_EQ(b->m_trx_locks.size(), 2u);
}

/** A release of more locks than the interval after which the trx sys mutex
is let go for a while frees them all. */
TEST_F(LockTest, ReleaseMany) {
  auto a = trx();
  auto b = trx();

  for (page_no_t p = 0; p < 2500; ++p) {
    (void)rec_lock(PageId{FAKE_SPACE + 1, p}, LOCK_S, 2 + p % 50, a);
  }

  auto kept = rec_lock(PageId{FAKE_SPACE + 1, 7}, LOCK_S, 60, b);

  EXPECT_EQ(m_lock_sys->m_rec_locks.size(), 2500u);

  release(a);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_EQ(m_lock_sys->m_rec_locks.size(), 1u);
  EXPECT_EQ(queue(PageId{FAKE_SPACE + 1, 7}, 60), (std::vector<const Lock *>{kept}));
}

/** rec_reset_and_release_wait() clears the bit of the record in every
lock and ends the wait of a waiting one; the lock structs stay. */
TEST_F(LockTest, ResetAndReleaseWait) {
  const PageId page_id{FAKE_SPACE, 30};
  auto a = trx();
  auto b = trx();

  auto la = rec_lock(page_id, LOCK_S, 4, a);
  auto lb = rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, b);
  auto other = rec_lock(page_id, LOCK_S, 5, b);

  b->m_que_state = TRX_QUE_LOCK_WAIT;

  mutex_enter(m_mutex);
  m_lock_sys->rec_reset_and_release_wait(page_id, 4);
  mutex_exit(m_mutex);

  EXPECT_TRUE(queue(page_id, 4).empty());
  EXPECT_FALSE(la->rec_is_nth_bit_set(4));
  EXPECT_FALSE(lb->is_waiting());
  EXPECT_EQ(b->m_wait_lock, nullptr);
  EXPECT_EQ(b->m_que_state, TRX_QUE_RUNNING);
  EXPECT_TRUE(other->rec_is_nth_bit_set(5));
  EXPECT_EQ(n_locks(page_id), 3u);
}

/** The heir of a record gets the X locks of the donor (REPEATABLE READ) as
granted gap locks, waiting ones too; the donor keeps its locks. */
TEST_F(LockTest, InheritX) {
  const PageId page_id{FAKE_SPACE, 31};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto b = trx();

  (void)rec_lock(page_id, LOCK_X | LOCK_REC_NOT_GAP, 4, a);
  (void)rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, b);

  mutex_enter(m_mutex);
  m_lock_sys->rec_inherit_to_gap(&block, &block, 6, 4);
  mutex_exit(m_mutex);

  auto ha = find(page_id, 6, a);
  auto hb = find(page_id, 6, b);

  ASSERT_NE(ha, nullptr);
  ASSERT_NE(hb, nullptr);
  EXPECT_EQ(ha->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(hb->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(queue(page_id, 4).size(), 2u);
}

/** Under READ COMMITTED the X locks of an UPDATE or DELETE are not
inherited, nor are insert intentions. */
TEST_F(LockTest, InheritSkips) {
  const PageId page_id{FAKE_SPACE, 32};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto rc = trx();
  auto ii = trx();

  rc->m_isolation_level = TRX_ISO_READ_COMMITTED;

  (void)rec_lock(page_id, LOCK_X, 4, rc);
  (void)rec_lock(page_id, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, 4, ii);

  mutex_enter(m_mutex);
  m_lock_sys->rec_inherit_to_gap(&block, &block, 6, 4);
  mutex_exit(m_mutex);

  EXPECT_TRUE(queue(page_id, 6).empty());
}

/* Regression: rec_inherit_to_gap() inherits only X locks: the S next-key locks of a
locking read are dropped when the record is removed (purge, page split or
merge), and the gap they protected opens to inserts: a phantom. InnoDB
inherits every lock except insert intentions and, under READ COMMITTED, X
locks (the comment above the loop says so: "we DO want S-locks set by a
consistency constraint to be inherited also then"). 7c21eac0 has the same
condition, lock/lock0lock.cc:1449. */
TEST_F(LockTest, InheritS) {
  const PageId page_id{FAKE_SPACE, 33};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto rr = trx();
  auto rc = trx();

  rc->m_isolation_level = TRX_ISO_READ_COMMITTED;

  (void)rec_lock(page_id, LOCK_S, 4, rr);
  (void)rec_lock(page_id, LOCK_S | LOCK_REC_NOT_GAP, 4, rc);

  mutex_enter(m_mutex);
  m_lock_sys->rec_inherit_to_gap(&block, &block, 6, 4);
  mutex_exit(m_mutex);

  ASSERT_NE(find(page_id, 6, rr), nullptr);
  ASSERT_NE(find(page_id, 6, rc), nullptr);
  EXPECT_EQ(find(page_id, 6, rr)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
}

/** Inherited on the supremum, a lock is a plain lock (all are gap locks there). */
TEST_F(LockTest, InheritToSupremum) {
  const PageId page_id{FAKE_SPACE, 34};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();

  (void)rec_lock(page_id, LOCK_X, 4, a);

  mutex_enter(m_mutex);
  m_lock_sys->rec_inherit_to_gap(&block, &block, PAGE_HEAP_NO_SUPREMUM, 4);
  mutex_exit(m_mutex);

  auto lock = find(page_id, PAGE_HEAP_NO_SUPREMUM, a);

  ASSERT_NE(lock, nullptr);
  EXPECT_EQ(lock->precise_mode(), LockMode(LOCK_REC | LOCK_X));
}

/** A record inserted before a locked record inherits its locks that cover
the gap (next-key and gap locks, S or X) as gap locks; not the record-only
locks nor the insert intentions. */
TEST_F(LockTest, InheritIfGapLock) {
  const PageId page_id{FAKE_SPACE, 35};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto s = trx();
  auto gap = trx();
  auto rec = trx();
  auto ii = trx();

  (void)rec_lock(page_id, LOCK_S, 5, s);
  (void)rec_lock(page_id, LOCK_X | LOCK_GAP, 5, gap);
  (void)rec_lock(page_id, LOCK_X | LOCK_REC_NOT_GAP, 5, rec);
  (void)rec_lock(page_id, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, 5, ii);

  mutex_enter(m_mutex);
  m_lock_sys->rec_inherit_to_gap_if_gap_lock(&block, 4, 5);
  mutex_exit(m_mutex);

  ASSERT_NE(find(page_id, 4, s), nullptr);
  ASSERT_NE(find(page_id, 4, gap), nullptr);
  EXPECT_EQ(find(page_id, 4, s)->precise_mode(), LockMode(LOCK_REC | LOCK_S | LOCK_GAP));
  EXPECT_EQ(find(page_id, 4, gap)->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_EQ(find(page_id, 4, rec), nullptr);
  EXPECT_EQ(find(page_id, 4, ii), nullptr);
}

/** The heir loses its own locks (a waiter is released) and gets the X
locks of the donor as gap locks. */
TEST_F(LockTest, ResetAndInheritGapLocks) {
  const PageId page_id{FAKE_SPACE, 36};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(page_id, LOCK_S, 6, a);
  auto lb = rec_lock(page_id, LOCK_X | LOCK_WAIT, 6, b);
  (void)rec_lock(page_id, LOCK_X, 4, c);

  b->m_que_state = TRX_QUE_LOCK_WAIT;

  m_lock_sys->rec_reset_and_inherit_gap_locks(&block, &block, 6, 4);

  EXPECT_FALSE(lb->is_waiting());
  EXPECT_EQ(find(page_id, 6, a), nullptr);
  EXPECT_EQ(find(page_id, 6, b), nullptr);
  ASSERT_NE(find(page_id, 6, c), nullptr);
  EXPECT_EQ(find(page_id, 6, c)->precise_mode(), LockMode(LOCK_REC | LOCK_X | LOCK_GAP));
  EXPECT_NE(find(page_id, 4, c), nullptr);
}

/** Moving the locks of a record keeps their precise mode: a waiting lock
stays waiting, a gap lock stays a gap lock. rec_move() passed only the
mode (S/X), so a waiting X lock behind a granted S lock became a granted
next-key X lock: two conflicting granted locks (seen in ib_mt_stress when a
pessimistic update parked the locks on the infimum). */
TEST_F(LockTest, RecMoveKeepsFlags) {
  const PageId page_id{9, 3};
  constexpr ulint heap_no = 5;

  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto holder = trx();
  auto waiter = trx();
  auto gapper = trx();

  ASSERT_NE(rec_lock(page_id, LOCK_S, heap_no, holder), nullptr);
  ASSERT_NE(rec_lock(page_id, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, heap_no, waiter), nullptr);
  ASSERT_NE(rec_lock(page_id, LOCK_S | LOCK_GAP, heap_no, gapper), nullptr);

  mutex_enter(m_mutex);
  m_lock_sys->rec_move(&block, &block, PAGE_HEAP_NO_INFIMUM, heap_no);
  mutex_exit(m_mutex);

  for (auto t : {holder, waiter, gapper}) {
    EXPECT_EQ(find(page_id, heap_no, t), nullptr) << "a lock was left on the donor";
  }

  auto s = find(page_id, PAGE_HEAP_NO_INFIMUM, holder);
  auto x = find(page_id, PAGE_HEAP_NO_INFIMUM, waiter);
  auto g = find(page_id, PAGE_HEAP_NO_INFIMUM, gapper);

  ASSERT_NE(s, nullptr);
  ASSERT_NE(x, nullptr);
  ASSERT_NE(g, nullptr);

  EXPECT_EQ(s->mode(), LOCK_S);
  EXPECT_FALSE(s->is_waiting());

  EXPECT_EQ(x->mode(), LOCK_X);
  EXPECT_TRUE(x->is_waiting());
  EXPECT_TRUE(x->m_type_mode & LOCK_REC_NOT_GAP);

  EXPECT_TRUE(g->rec_is_gap());
  EXPECT_FALSE(g->is_waiting());
}

/** Moving back restores the locks on the record. */
TEST_F(LockTest, RecMoveRoundTrip) {
  const PageId page_id{9, 4};
  constexpr ulint heap_no = 7;

  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto a = trx();
  auto b = trx();

  ASSERT_NE(rec_lock(page_id, LOCK_X | LOCK_REC_NOT_GAP, heap_no, a), nullptr);
  ASSERT_NE(rec_lock(page_id, LOCK_S | LOCK_WAIT, heap_no, b), nullptr);

  mutex_enter(m_mutex);
  m_lock_sys->rec_move(&block, &block, PAGE_HEAP_NO_INFIMUM, heap_no);
  m_lock_sys->rec_move(&block, &block, heap_no, PAGE_HEAP_NO_INFIMUM);
  mutex_exit(m_mutex);

  EXPECT_EQ(find(page_id, PAGE_HEAP_NO_INFIMUM, a), nullptr);
  EXPECT_EQ(find(page_id, PAGE_HEAP_NO_INFIMUM, b), nullptr);

  auto la = find(page_id, heap_no, a);
  auto lb = find(page_id, heap_no, b);

  ASSERT_NE(la, nullptr);
  ASSERT_NE(lb, nullptr);
  EXPECT_FALSE(la->is_waiting());
  EXPECT_TRUE(lb->is_waiting());
}

/** A move to another page keeps the queue order: the waiters still wait
in the same order, the first is granted first. */
TEST_F(LockTest, RecMoveKeepsOrder) {
  const PageId from_id{FAKE_SPACE, 40};
  const PageId to_id{FAKE_SPACE, 41};

  BufBlock from{};
  BufBlock to{};
  Frame from_frame;
  Frame to_frame;

  init_block(from, from_frame, from_id);
  init_block(to, to_frame, to_id);

  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(from_id, LOCK_X, 5, a);
  (void)rec_lock(from_id, LOCK_X | LOCK_WAIT, 5, b);
  (void)rec_lock(from_id, LOCK_S | LOCK_WAIT, 5, c);

  mutex_enter(m_mutex);
  m_lock_sys->rec_move(&to, &from, 3, 5);
  mutex_exit(m_mutex);

  EXPECT_TRUE(queue(from_id, 5).empty());

  const auto moved = queue(to_id, 3);

  ASSERT_EQ(moved.size(), 3u);
  EXPECT_EQ(moved[0]->m_trx, a);
  EXPECT_EQ(moved[1]->m_trx, b);
  EXPECT_EQ(moved[2]->m_trx, c);
  EXPECT_FALSE(moved[0]->is_waiting());
  EXPECT_TRUE(moved[1]->is_waiting());
  EXPECT_TRUE(moved[2]->is_waiting());
  EXPECT_EQ(b->m_wait_lock, moved[1]);

  dequeue(const_cast<Lock *>(moved[0]));

  EXPECT_FALSE(moved[1]->is_waiting());
  EXPECT_TRUE(moved[2]->is_waiting());
}

/** Moving the locks of a record without locks changes nothing. */
TEST_F(LockTest, RecMoveNothing) {
  const PageId page_id{FAKE_SPACE, 42};
  BufBlock block{};
  Frame frame;

  init_block(block, frame, page_id);

  auto lock = rec_lock(page_id, LOCK_S, 4, trx());

  mutex_enter(m_mutex);
  m_lock_sys->rec_move(&block, &block, 6, 5);
  mutex_exit(m_mutex);

  EXPECT_EQ(queue(page_id, 4), (std::vector<const Lock *>{lock}));
  EXPECT_TRUE(queue(page_id, 6).empty());
  EXPECT_EQ(n_locks(page_id), 1u);
}

/** Many transactions with record locks on random records; the check for
waiters must not fail, every lock is released. */
TEST_F(LockTest, ManyTrxs) {
  constexpr int N_TRXS = 8;
  constexpr int N_LOCKS = 64;

  for (int i = 0; i < N_TRXS; ++i) {
    auto t = trx();

    for (int j = 0; j < N_LOCKS; ++j) {
      const PageId page_id(random() % 100, random() % 1000);
      const auto mode = (j % 50) == 0 ? LOCK_X : LOCK_S;

      ASSERT_NE(rec_lock(page_id, mode, random() % N_BITS, t), nullptr);
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
  const PageId page_id{FAKE_SPACE, 43};
  auto a = trx();
  auto b = trx();

  (void)rec_lock(page_id, LOCK_X, 4, a);

  EXPECT_TRUE(m_lock_sys->trx_has_no_waiters(a));

  (void)rec_lock(page_id, LOCK_X | LOCK_WAIT, 4, b);

  EXPECT_FALSE(m_lock_sys->trx_has_no_waiters(a));
}

/** Discard a page that holds record locks of two transactions: every lock
goes and so does the page entry. rec_discard() used to erase the whole
page entry with the first lock, then assert on the second. */
TEST_F(LockTest, DiscardPage) {
  const PageId page_id{7, 42};
  ulint heap_no{PAGE_HEAP_NO_USER_LOW};

  std::array<Trx *, 2> trxs{trx(), trx()};

  for (auto t : trxs) {
    auto lock = rec_lock(page_id, LOCK_S, heap_no, t);

    ASSERT_NE(lock, nullptr);

    /* A discarded page has no records left: the locks carry no bits. */
    lock->rec_reset_nth_bit(heap_no);
    ++heap_no;
  }

  mutex_enter(m_mutex);
  m_lock_sys->rec_free_all_from_discard_page(page_id);
  mutex_exit(m_mutex);

  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(page_id));

  for (auto t : trxs) {
    EXPECT_TRUE(t->m_trx_locks.empty());
  }
}

/** Discarding one lock of a page keeps the others and the page entry. */
TEST_F(LockTest, DiscardOne) {
  const PageId page_id{FAKE_SPACE, 44};
  auto a = trx();
  auto b = trx();

  auto la = rec_lock(page_id, LOCK_S, 4, a);
  auto lb = rec_lock(page_id, LOCK_S, 5, b);

  mutex_enter(m_mutex);
  m_lock_sys->rec_discard(la);
  mutex_exit(m_mutex);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_EQ(n_locks(page_id), 1u);
  EXPECT_EQ(queue(page_id, 5), (std::vector<const Lock *>{lb}));
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

/** Create a table like the test table: see ut::Row. */
void create_table(System &sys, const char *name) {
  (void)api::database_create(sys, "test");

  api::TableSchema *tbl_sch{};
  api::IndexSchema *idx_sch{};

  ASSERT_EQ(api::TableSchema::create(name, &tbl_sch, IB_TBL_V1, 0), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_col("a", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_col("b", IB_BLOB, IB_COL_NONE, 0, 0), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);
  ASSERT_EQ(idx_sch->add_col("k", 0), DB_SUCCESS);
  ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  uint64_t table_id{};

  ASSERT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_create(sys, trx, tbl_sch, &table_id), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  api::TableSchema::destroy(sys, tbl_sch);
}

/** INSERT row into the table name in trx. */
DbErr insert_into(System &sys, Trx *trx, const char *name, const ut::Row &row) {
  api::Cursor *crsr{};

  if (auto err = api::cursor_open_table(sys, name, trx, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->lock(sys, IB_LOCK_IX);
  auto tpl = crsr->clust_read_tuple_create();

  if (err == DB_SUCCESS) {
    err = tpl->write_u32(0, row.m_k);
  }

  if (err == DB_SUCCESS) {
    err = tpl->write_u32(1, row.m_a);
  }

  if (err == DB_SUCCESS) {
    err = tpl->set_value(2, row.m_b.data(), row.m_b.size());
  }

  if (err == DB_SUCCESS) {
    err = crsr->insert_row(sys, tpl);
  }

  api::Tuple::destroy(tpl);
  (void)api::Cursor::close(sys, crsr);

  return err;
}

/** Copy the page page_id from the buffer pool to frame. */
void copy_page(System &sys, const PageId &page_id, Frame &frame) {
  Mtr mtr;

  mtr.start();

  BufPool::Request req{
    .m_rw_latch = RW_S_LATCH, .m_page_id = page_id, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
  };

  auto block = sys.m_buf_pool->get(req, nullptr);

  memcpy(frame.m_buf, block->get_frame(), UNIV_PAGE_SIZE);

  mtr.commit(sys);
}

/** The locks of the page operations: on copies of the leaf pages of two
tables of 20 rows, P1 and P2, that never change. A block for a copy has the
page id of a real page where the debug checks validate the lock queue
against the real page: the locks are then of started transactions on
records of the clustered index of P1. The user records have the heap
numbers 2..21 in key order. */
class LockPageTest : public SharedDb {
 protected:
  static constexpr const char *P1 = "test/p1";
  static constexpr const char *P2 = "test/p2";
  static constexpr uint32_t N_ROWS = 20;

  void SetUp() override {
    SharedDb::SetUp();

    for (auto name : {P1, P2}) {
      if (get_table(*m_sys, name) == nullptr) {
        create_table(*m_sys, name);

        auto trx = begin();

        for (uint32_t k = 0; k < N_ROWS; ++k) {
          ASSERT_EQ(insert_into(*m_sys, trx, name, ut::Row{k, k, ut::blob(k, 40)}), DB_SUCCESS);
        }

        commit(trx);
      }
    }

    m_index = get_table(*m_sys, P1)->get_first_index();
    m_p1 = PageId{m_index->get_space_id(), m_index->get_page_no()};

    auto index2 = get_table(*m_sys, P2)->get_first_index();

    m_p2 = PageId{index2->get_space_id(), index2->get_page_no()};

    m_lock_sys = m_sys->m_lock_sys.get();
    m_mutex = &m_sys->m_trx_sys->m_mutex;
  }

  void TearDown() override {
    finish();
    SharedDb::TearDown();
  }

  /** Release the locks of the transactions of the test and commit them. */
  void finish() {
    mutex_enter(m_mutex);

    for (auto t : m_trxs) {
      m_lock_sys->release_off_trx_sys_mutex(t);
    }

    for (auto t : m_trxs) {
      t->m_wait_lock = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    mutex_exit(m_mutex);

    for (auto t : m_trxs) {
      commit(t);
    }

    m_trxs.clear();
  }

  /** @return a started transaction. */
  Trx *trx() {
    m_trxs.push_back(begin());
    return m_trxs.back();
  }

  /** A block for a copy of P1 with the page id page_id. */
  void make_block(BufBlock &block, Frame &frame, const PageId &page_id) {
    copy_page(*m_sys, m_p1, frame);
    block.m_frame = frame.m_buf;
    buf_block_set_state(&block, BUF_BLOCK_READY_FOR_USE);
    buf_block_set_file_page(&block, page_id);
    /* As if the test had the block fixed, for get_frame(). */
    block.m_page.m_buf_fix_count = 1;
  }

  /** @return the record with heap_no in block. */
  static Rec rec(const BufBlock &block, ulint heap_no) {
    return page_find_rec_with_heap_no(block.m_frame, heap_no);
  }

  /** @return the infimum of block. */
  static Rec infimum(const BufBlock &block) { return Rec(block.m_frame + PAGE_INFIMUM); }

  Lock *rec_lock(const PageId &page_id, ulint type_mode, ulint heap_no, Trx *trx) {
    mutex_enter(m_mutex);
    auto lock = m_lock_sys->rec_create_low(page_id, LockMode(type_mode), heap_no, N_BITS, m_index, trx);
    mutex_exit(m_mutex);

    return lock;
  }

  /** @return the locks on page_id with bit heap_no set, in queue order. */
  std::vector<const Lock *> queue(const PageId &page_id, ulint heap_no) {
    std::vector<const Lock *> locks;

    mutex_enter(m_mutex);

    if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
      for (auto lock : it->second) {
        if (lock->rec_is_nth_bit_set(heap_no)) {
          locks.push_back(lock);
        }
      }
    }

    mutex_exit(m_mutex);

    return locks;
  }

  /** @return true if no lock on page_id has a bit set. */
  bool no_bits(const PageId &page_id) {
    for (ulint h{}; h < N_BITS; ++h) {
      if (!queue(page_id, h).empty()) {
        return false;
      }
    }

    return true;
  }

  /** Expect the locks on page_id, heap_no to be of trxs with precise modes, in this order. */
  void expect_q(const PageId &page_id, ulint heap_no, std::vector<std::pair<Trx *, ulint>> expected) {
    const auto locks = queue(page_id, heap_no);

    ASSERT_EQ(locks.size(), expected.size()) << "heap_no " << heap_no;

    for (size_t i{}; i < locks.size(); ++i) {
      EXPECT_EQ(locks[i]->m_trx, expected[i].first) << "heap_no " << heap_no << " lock " << i;
      EXPECT_EQ(locks[i]->precise_mode(), LockMode(LOCK_REC | expected[i].second)) << "heap_no " << heap_no << " lock " << i;
    }
  }

  Index *m_index{};
  PageId m_p1{};
  PageId m_p2{};
  LockSys *m_lock_sys{};
  Mutex *m_mutex{};
  std::vector<Trx *> m_trxs;
};

/** The copies have the records of the table in key order, heap numbers
2..21: what the other tests rely on. */
TEST_F(LockPageTest, Layout) {
  BufBlock block{};
  Frame frame;

  make_block(block, frame, m_p1);

  auto r = page_rec_get_next(infimum(block));
  ulint n{};

  while (!page_rec_is_supremum(r)) {
    EXPECT_EQ(r.get_heap_no(), PAGE_HEAP_NO_USER_LOW + n);
    r = page_rec_get_next(r);
    ++n;
  }

  EXPECT_EQ(n, N_ROWS);
}

/* Regression: see SplitRight: get_min_heap_no() is the heap number of the
first user record, 2 here, it returns 0 (the infimum). */
TEST_F(LockPageTest, MinHeapNo) {
  BufBlock block{};
  Frame frame;

  make_block(block, frame, m_p1);

  EXPECT_EQ(m_lock_sys->get_min_heap_no(&block), page_rec_get_next(infimum(block)).get_heap_no());
  EXPECT_EQ(m_lock_sys->get_min_heap_no(&block), PAGE_HEAP_NO_USER_LOW);
}

/** move_rec_list_end() moves the locks of the records from rec to the end
to the new page, with their precise modes and queue order; the supremum
locks stay. (The new page holds the moved records only: all of them here.) */
TEST_F(LockPageTest, MoveRecListEnd) {
  BufBlock old_block{};
  BufBlock new_block{};
  Frame old_frame;
  Frame new_frame;

  make_block(old_block, old_frame, m_p1);
  make_block(new_block, new_frame, m_p2);

  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(m_p1, LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);
  (void)rec_lock(m_p1, LOCK_S, 4, a);
  (void)rec_lock(m_p1, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 4, c);
  (void)rec_lock(m_p1, LOCK_S | LOCK_GAP, 6, b);
  (void)rec_lock(m_p1, LOCK_X | LOCK_REC_NOT_GAP, 21, b);

  m_lock_sys->move_rec_list_end(&new_block, &old_block, rec(old_block, 2));

  expect_q(m_p1, PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_S}});
  expect_q(m_p1, 4, {});
  expect_q(m_p1, 6, {});
  expect_q(m_p1, 21, {});

  expect_q(m_p2, 4, {{a, LOCK_S}, {c, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT}});
  expect_q(m_p2, 6, {{b, LOCK_S | LOCK_GAP}});
  expect_q(m_p2, 21, {{b, LOCK_X | LOCK_REC_NOT_GAP}});
  expect_q(m_p2, PAGE_HEAP_NO_SUPREMUM, {});
  EXPECT_EQ(c->m_wait_lock, queue(m_p2, 4).back());
}

/** move_rec_list_start() moves the locks of the records before rec to the
new page after old_end; the locks from rec on stay. */
TEST_F(LockPageTest, MoveRecListStart) {
  BufBlock old_block{};
  BufBlock new_block{};
  Frame old_frame;
  Frame new_frame;

  make_block(old_block, old_frame, m_p1);
  make_block(new_block, new_frame, PageId{FAKE_SPACE, 1});

  const auto new_id = new_block.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(m_p1, LOCK_X, 2, a);
  (void)rec_lock(m_p1, LOCK_S | LOCK_WAIT, 2, b);
  (void)rec_lock(m_p1, LOCK_S | LOCK_GAP, 9, b);
  (void)rec_lock(m_p1, LOCK_S, 10, b);
  (void)rec_lock(m_p1, LOCK_S, PAGE_HEAP_NO_SUPREMUM, b);

  m_lock_sys->move_rec_list_start(&new_block, &old_block, rec(old_block, 10), infimum(new_block));

  expect_q(new_id, 2, {{a, LOCK_X}, {b, LOCK_S | LOCK_WAIT}});
  expect_q(new_id, 9, {{b, LOCK_S | LOCK_GAP}});
  expect_q(new_id, 10, {});
  expect_q(m_p1, 2, {});
  expect_q(m_p1, 9, {});
  expect_q(m_p1, 10, {{b, LOCK_S}});
  expect_q(m_p1, PAGE_HEAP_NO_SUPREMUM, {{b, LOCK_S}});
}

/** A reorganize keeps every lock where its record is, with its precise
mode, in queue order, the locks on the infimum and the supremum too. */
TEST_F(LockPageTest, MoveReorganizePage) {
  BufBlock block{};
  BufBlock old_block{};
  Frame frame;
  Frame old_frame;

  make_block(block, frame, m_p1);
  make_block(old_block, old_frame, m_p1);

  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(m_p1, LOCK_S, 3, a);
  (void)rec_lock(m_p1, LOCK_X | LOCK_WAIT, 3, b);
  (void)rec_lock(m_p1, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, 7, c);
  (void)rec_lock(m_p1, LOCK_X | LOCK_REC_NOT_GAP, PAGE_HEAP_NO_INFIMUM, c);
  (void)rec_lock(m_p1, LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);
  (void)rec_lock(m_p1, LOCK_S, 20, a);

  m_lock_sys->move_reorganize_page(&block, &old_block);

  expect_q(m_p1, 3, {{a, LOCK_S}, {b, LOCK_X | LOCK_WAIT}});
  expect_q(m_p1, 7, {{c, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION}});
  expect_q(m_p1, PAGE_HEAP_NO_INFIMUM, {{c, LOCK_X | LOCK_REC_NOT_GAP}});
  expect_q(m_p1, PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_S}});
  expect_q(m_p1, 20, {{a, LOCK_S}});
  EXPECT_EQ(b->m_wait_lock, queue(m_p1, 3).back());
}

/* Regression: LockSys::get_min_heap_no() (include/lock0lock.h:653) returns the heap
number of the infimum itself, Rec(page + PAGE_INFIMUM).get_heap_no() == 0,
not of the record after it as upstream lock_get_min_heap_no() did
(rec_get_heap_no_old(page + rec_get_next_offs(page + PAGE_OLD_INFIMUM))).
update_split_right/left() then let the left supremum inherit the locks of
the right page's infimum (none) instead of its first record: the gap before
that record loses its lock after a split, an insert into it does not wait
(a phantom); btr0btr.cc:1688 update_discard() inherits on the infimum of
the merge page. */
/** A split to the right: the locks on the left supremum go to the right
supremum, the left supremum inherits the X locks of the first record of
the right page. */
TEST_F(LockPageTest, SplitRight) {
  BufBlock left{};
  BufBlock right{};
  Frame left_frame;
  Frame right_frame;

  make_block(left, left_frame, PageId{FAKE_SPACE, 10});
  make_block(right, right_frame, PageId{FAKE_SPACE, 11});

  const auto l = left.get_page_id();
  const auto r = right.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(l, LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);
  (void)rec_lock(r, LOCK_X, 2, b);

  m_lock_sys->update_split_right(&right, &left);

  expect_q(r, PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_S}});
  expect_q(l, PAGE_HEAP_NO_SUPREMUM, {{b, LOCK_X}});
  expect_q(r, 2, {{b, LOCK_X}});
}

/** A split to the left: the left supremum inherits from the first record
of the right page. BUG: see SplitRight. */
TEST_F(LockPageTest, SplitLeft) {
  BufBlock left{};
  BufBlock right{};
  Frame left_frame;
  Frame right_frame;

  make_block(left, left_frame, PageId{FAKE_SPACE, 12});
  make_block(right, right_frame, PageId{FAKE_SPACE, 13});

  auto a = trx();

  (void)rec_lock(right.get_page_id(), LOCK_X | LOCK_REC_NOT_GAP, 2, a);

  m_lock_sys->update_split_left(&right, &left);

  expect_q(left.get_page_id(), PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_X}});
}

/** A merge to the right: the original successor of the infimum on the
right page inherits the locks of the left supremum, whose waiters are
released, and the left page loses all its locks. */
TEST_F(LockPageTest, MergeRight) {
  BufBlock left{};
  BufBlock right{};
  Frame left_frame;
  Frame right_frame;

  make_block(left, left_frame, PageId{FAKE_SPACE, 14});
  make_block(right, right_frame, PageId{FAKE_SPACE, 15});

  const auto l = left.get_page_id();
  const auto r = right.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(l, LOCK_X, PAGE_HEAP_NO_SUPREMUM, a);
  auto waiting = rec_lock(l, LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION | LOCK_WAIT, PAGE_HEAP_NO_SUPREMUM, b);

  b->m_que_state = TRX_QUE_LOCK_WAIT;

  m_lock_sys->update_merge_right(&right, rec(right, 5), &left);

  expect_q(r, 5, {{a, LOCK_X | LOCK_GAP}});
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(l));
  EXPECT_EQ(b->m_wait_lock, nullptr);
  EXPECT_EQ(b->m_que_state, TRX_QUE_RUNNING);
  (void)waiting;
}

/** A merge to the left: the first record moved from the right page
inherits the locks of the left supremum, the right supremum locks go to the
left supremum, the right page loses all its locks. */
TEST_F(LockPageTest, MergeLeft) {
  BufBlock left{};
  BufBlock right{};
  Frame left_frame;
  Frame right_frame;

  make_block(left, left_frame, PageId{FAKE_SPACE, 16});
  make_block(right, right_frame, PageId{FAKE_SPACE, 17});

  const auto l = left.get_page_id();
  const auto r = right.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(l, LOCK_X, PAGE_HEAP_NO_SUPREMUM, a);
  (void)rec_lock(r, LOCK_S, PAGE_HEAP_NO_SUPREMUM, b);

  /* orig_pred: the record of the left page before the moved records. */
  m_lock_sys->update_merge_left(&left, rec(left, 9), &right);

  expect_q(l, 10, {{a, LOCK_X | LOCK_GAP}});
  expect_q(l, PAGE_HEAP_NO_SUPREMUM, {{b, LOCK_S}});
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(r));
}

/** A raise of the root moves the locks of the root supremum to the
supremum of the new page. */
TEST_F(LockPageTest, RootRaise) {
  BufBlock root{};
  BufBlock block{};
  Frame root_frame;
  Frame frame;

  make_block(root, root_frame, PageId{FAKE_SPACE, 18});
  make_block(block, frame, PageId{FAKE_SPACE, 19});

  auto a = trx();

  (void)rec_lock(root.get_page_id(), LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);
  (void)rec_lock(root.get_page_id(), LOCK_X | LOCK_REC_NOT_GAP, 4, a);

  m_lock_sys->update_root_raise(&block, &root);

  expect_q(block.get_page_id(), PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_S}});
  expect_q(root.get_page_id(), PAGE_HEAP_NO_SUPREMUM, {});
  expect_q(root.get_page_id(), 4, {{a, LOCK_X | LOCK_REC_NOT_GAP}});
}

/** A copy and discard moves the supremum locks and frees the locks of the old page. */
TEST_F(LockPageTest, CopyAndDiscard) {
  BufBlock old_block{};
  BufBlock new_block{};
  Frame old_frame;
  Frame new_frame;

  make_block(old_block, old_frame, PageId{FAKE_SPACE, 20});
  make_block(new_block, new_frame, PageId{FAKE_SPACE, 21});

  auto a = trx();

  (void)rec_lock(old_block.get_page_id(), LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);

  m_lock_sys->update_copy_and_discard(&new_block, &old_block);

  expect_q(new_block.get_page_id(), PAGE_HEAP_NO_SUPREMUM, {{a, LOCK_S}});
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(old_block.get_page_id()));
}

/** A discarded page: the heir inherits the X locks of all its records,
waiters are released, the page loses all its locks. */
TEST_F(LockPageTest, Discard) {
  BufBlock heir{};
  BufBlock block{};
  Frame heir_frame;
  Frame frame;

  make_block(heir, heir_frame, PageId{FAKE_SPACE, 22});
  make_block(block, frame, PageId{FAKE_SPACE, 23});

  const auto p = block.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(p, LOCK_X, 3, a);
  (void)rec_lock(p, LOCK_X | LOCK_REC_NOT_GAP, 7, b);
  (void)rec_lock(p, LOCK_X | LOCK_WAIT, 3, b);
  (void)rec_lock(p, LOCK_S, PAGE_HEAP_NO_SUPREMUM, a);

  b->m_que_state = TRX_QUE_LOCK_WAIT;

  m_lock_sys->update_discard(&heir, 5, &block);

  /* S locks are inherited too (only X locks were, before the fix of
  rec_inherit_to_gap). */
  expect_q(heir.get_page_id(), 5, {{a, LOCK_X | LOCK_GAP}, {b, LOCK_X | LOCK_GAP}, {a, LOCK_S | LOCK_GAP}});
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(p));
  EXPECT_EQ(b->m_wait_lock, nullptr);
}

/** An inserted record inherits the gap covering locks of its successor. */
TEST_F(LockPageTest, Insert) {
  BufBlock block{};
  Frame frame;

  make_block(block, frame, PageId{FAKE_SPACE, 24});

  const auto p = block.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(p, LOCK_S, 6, a);
  (void)rec_lock(p, LOCK_X | LOCK_REC_NOT_GAP, 6, b);

  /* As if the record with heap number 5 had just been inserted before 6. */
  m_lock_sys->update_insert(&block, rec(block, 5));

  expect_q(p, 5, {{a, LOCK_S | LOCK_GAP}});
  expect_q(p, 6, {{a, LOCK_S}, {b, LOCK_X | LOCK_REC_NOT_GAP}});
}

/** A deleted record: its successor inherits its X locks as gap locks, its
own locks go and its waiters are released. */
TEST_F(LockPageTest, Delete) {
  BufBlock block{};
  Frame frame;

  make_block(block, frame, PageId{FAKE_SPACE, 25});

  const auto p = block.get_page_id();
  auto a = trx();
  auto b = trx();

  (void)rec_lock(p, LOCK_X | LOCK_REC_NOT_GAP, 5, a);
  (void)rec_lock(p, LOCK_X | LOCK_WAIT, 5, b);

  b->m_que_state = TRX_QUE_LOCK_WAIT;

  m_lock_sys->update_delete(&block, rec(block, 5));

  expect_q(p, 5, {});
  expect_q(p, 6, {{a, LOCK_X | LOCK_GAP}, {b, LOCK_X | LOCK_GAP}});
  EXPECT_EQ(b->m_wait_lock, nullptr);
}

/** The locks of a record stored on the infimum during an update and
restored on the record, on the same page or another, keep their precise
modes and order. */
TEST_F(LockPageTest, StoreRestoreInfimum) {
  BufBlock block{};
  BufBlock other{};
  Frame frame;
  Frame other_frame;

  make_block(block, frame, PageId{FAKE_SPACE, 26});
  make_block(other, other_frame, PageId{FAKE_SPACE, 27});

  const auto p = block.get_page_id();
  auto a = trx();
  auto b = trx();
  auto c = trx();

  (void)rec_lock(p, LOCK_S, 8, a);
  (void)rec_lock(p, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 8, b);
  (void)rec_lock(p, LOCK_S | LOCK_GAP, 8, c);

  const std::vector<std::pair<Trx *, ulint>> locks{
    {a, LOCK_S}, {b, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT}, {c, LOCK_S | LOCK_GAP}
  };

  m_lock_sys->rec_store_on_page_infimum(&block, rec(block, 8));

  expect_q(p, 8, {});
  expect_q(p, PAGE_HEAP_NO_INFIMUM, locks);

  m_lock_sys->rec_restore_from_page_infimum(&block, rec(block, 8), &block);

  expect_q(p, PAGE_HEAP_NO_INFIMUM, {});
  expect_q(p, 8, locks);

  /* The record moved to another page in a pessimistic update. */
  m_lock_sys->rec_store_on_page_infimum(&block, rec(block, 8));
  m_lock_sys->rec_restore_from_page_infimum(&other, rec(other, 12), &block);

  expect_q(p, PAGE_HEAP_NO_INFIMUM, {});
  expect_q(other.get_page_id(), 12, locks);
}

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
    mutex_enter(m_mutex);

    for (auto t : m_trxs) {
      m_lock_sys->release_off_trx_sys_mutex(t);
    }

    for (auto t : m_trxs) {
      t->m_wait_lock = nullptr;
      t->m_que_state = TRX_QUE_RUNNING;
    }

    mutex_exit(m_mutex);

    for (auto t : m_trxs) {
      commit(t);
    }

    m_trxs.clear();
  }

  /** A record lock of trx on the clustered index; a waiting one puts trx in the lock wait state. */
  Lock *rec_lock(const PageId &page_id, ulint type_mode, ulint heap_no, Trx *trx) {
    mutex_enter(m_mutex);

    auto lock = m_lock_sys->rec_create_low(page_id, LockMode(type_mode), heap_no, N_BITS, m_index, trx);

    if (type_mode & LOCK_WAIT) {
      trx->m_que_state = TRX_QUE_LOCK_WAIT;
    }

    mutex_exit(m_mutex);

    return lock;
  }

  /** @return true if the waiting request lock of trx closes a cycle and trx is the victim. */
  bool deadlock(Lock *lock, Trx *trx) {
    mutex_enter(m_mutex);
    auto ret = m_lock_sys->deadlock_occurs(lock, trx);
    mutex_exit(m_mutex);

    return ret;
  }

  /** @return true once trx waits for a lock, false after timeout. */
  bool wait_for_lock_wait(Trx *trx, std::chrono::milliseconds timeout = 10s) {
    const auto end = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < end) {
      mutex_enter(m_mutex);
      const auto waiting = trx->m_que_state == TRX_QUE_LOCK_WAIT;
      mutex_exit(m_mutex);

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
  const PageId r1{FAKE_SPACE, 1};
  const PageId r2{FAKE_SPACE, 2};
  auto a = trx();
  auto b = trx();

  (void)rec_lock(r1, LOCK_X, 2, a);
  (void)rec_lock(r2, LOCK_X, 2, b);
  (void)rec_lock(r1, LOCK_X | LOCK_WAIT, 2, b);

  auto req = rec_lock(r2, LOCK_X | LOCK_WAIT, 2, a);

  EXPECT_TRUE(deadlock(req, a));
  EXPECT_FALSE(b->m_was_chosen_as_deadlock_victim);
  EXPECT_NE(b->m_wait_lock, nullptr);
}

/** The same cycle, but the requester has more locks: the other transaction
is the victim, its wait is cancelled, and the requester keeps waiting with
no deadlock left. */
TEST_F(LockDbTest, DeadlockOtherVictim) {
  const PageId r1{FAKE_SPACE, 1};
  const PageId r2{FAKE_SPACE, 2};
  auto a = trx();
  auto b = trx();

  (void)rec_lock(r1, LOCK_X, 2, a);

  for (ulint h = 3; h < 10; ++h) {
    (void)rec_lock(PageId{FAKE_SPACE, 3}, LOCK_X, h, a);
  }

  (void)rec_lock(r2, LOCK_X, 2, b);
  auto bw = rec_lock(r1, LOCK_X | LOCK_WAIT, 2, b);
  auto req = rec_lock(r2, LOCK_X | LOCK_WAIT, 2, a);

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
  std::array<PageId, 3> r{PageId{FAKE_SPACE, 1}, PageId{FAKE_SPACE, 2}, PageId{FAKE_SPACE, 3}};

  for (size_t i{}; i < 3; ++i) {
    (void)rec_lock(r[i], LOCK_X | LOCK_REC_NOT_GAP, 2, t[i]);
  }

  (void)rec_lock(r[1], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 2, t[0]);
  (void)rec_lock(r[2], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 2, t[1]);

  auto req = rec_lock(r[0], LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 2, t[2]);

  EXPECT_TRUE(deadlock(req, t[2]));
}

/** Waits that form no cycle: a chain, a lock that is compatible, and a gap
lock, which no lock waits for. */
TEST_F(LockDbTest, NoDeadlock) {
  const PageId r1{FAKE_SPACE, 1};
  const PageId r2{FAKE_SPACE, 2};
  auto a = trx();
  auto b = trx();
  auto c = trx();

  /* b waits for c, a waits for b. */
  (void)rec_lock(r1, LOCK_X, 2, b);
  (void)rec_lock(r2, LOCK_X, 2, c);
  (void)rec_lock(r2, LOCK_X | LOCK_WAIT, 2, b);

  auto req = rec_lock(r1, LOCK_X | LOCK_WAIT, 2, a);

  EXPECT_FALSE(deadlock(req, a));

  /* a holds only a gap lock on what c waits for: no edge from c to a. */
  (void)rec_lock(PageId{FAKE_SPACE, 3}, LOCK_X | LOCK_GAP, 2, a);
  auto creq = rec_lock(PageId{FAKE_SPACE, 3}, LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT, 2, c);

  EXPECT_FALSE(deadlock(creq, c));
}

/** A chain of waits longer than the depth limit of the search: the
requester is rolled back as if it had deadlocked. A shorter chain is fine. */
TEST_F(LockDbTest, DeadlockTooDeep) {
  for (auto [n, expected] : {std::pair{150, false}, std::pair{230, true}}) {
    std::vector<Trx *> t;

    for (int i = 0; i < n; ++i) {
      t.push_back(trx());
      (void)rec_lock(PageId{FAKE_SPACE, page_no_t(i)}, LOCK_X, 2, t.back());
    }

    /* t[i] waits for the record of t[i + 1], the last does not wait. */
    for (int i = 1; i + 1 < n; ++i) {
      (void)rec_lock(PageId{FAKE_SPACE, page_no_t(i + 1)}, LOCK_X | LOCK_WAIT, 2, t[i]);
    }

    auto req = rec_lock(PageId{FAKE_SPACE, 1}, LOCK_X | LOCK_WAIT, 2, t[0]);

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
  const PageId page_id{FAKE_SPACE, 1};
  auto a = trx();

  ASSERT_EQ(lock_table(a, IB_LOCK_IX), DB_SUCCESS);
  (void)rec_lock(page_id, LOCK_X | LOCK_REC_NOT_GAP, 2, a);

  m_lock_sys->remove_all_on_table(m_table, true);

  EXPECT_TRUE(a->m_trx_locks.empty());
  EXPECT_FALSE(m_lock_sys->m_rec_locks.contains(page_id));
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

/** The in-memory lock queues, timed. */
class LockPerf : public LockTest {};

/** Create and release one record lock, uncontended. */
TEST_F(LockPerf, RecCreateRelease) {
  auto a = trx();

  auto r = ut::bench("rec_create_release", 200000, [&](uint64_t i) {
    mutex_enter(m_mutex);
    ut::keep(m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(i % 64)}, LOCK_X, 2, N_BITS, nullptr, a));
    m_lock_sys->release_off_trx_sys_mutex(a);
    mutex_exit(m_mutex);
  });

  EXPECT_LT(r.ns_per_op(), 20000);
}

/** Lock records of a page one by one: the bits go in one lock. */
TEST_F(LockPerf, AddToQueue) {
  BufBlock block{};
  Frame frame;
  auto a = trx();

  init_block(block, frame, PageId{FAKE_SPACE, 1});

  auto r = ut::bench("rec_add_to_queue", 200000, [&](uint64_t i) {
    mutex_enter(m_mutex);
    ut::keep(m_lock_sys->rec_add_to_queue(LockMode(LOCK_REC | LOCK_S), &block, 2 + i % 70, nullptr, a));

    if (i % 70 == 69) {
      m_lock_sys->release_off_trx_sys_mutex(a);
    }

    mutex_exit(m_mutex);
  });

  EXPECT_LT(r.ns_per_op(), 20000);
}

/** Threads each with a transaction locking records of pages of its own,
releasing every 64 locks: the cost of the shared trx sys mutex. */
TEST_F(LockPerf, DisjointMt) {
  for (int n_threads : {1, 2, 4, 8, 16}) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n_threads; ++t) {
      trxs.push_back(trx());
    }

    auto r = ut::bench_mt(std::format("disjoint_rec_lock_t{}", n_threads), n_threads, 50000, [&](int t, uint64_t i) {
      mutex_enter(m_mutex);
      ut::keep(m_lock_sys->rec_create_low(PageId{FAKE_SPACE + 1 + t, page_no_t(i % 64)}, LOCK_X, 2, N_BITS, nullptr, trxs[t]));

      if (i % 64 == 63) {
        m_lock_sys->release_off_trx_sys_mutex(trxs[t]);
      }

      mutex_exit(m_mutex);
    });

    EXPECT_LT(r.ns_per_op(), 100000) << n_threads;
  }
}

/** A lock handed over: a request waits behind the holder, the holder's
lock goes and the waiter is granted; then the roles swap. */
TEST_F(LockPerf, Handoff) {
  const PageId page_id{FAKE_SPACE, 1};
  std::array<Trx *, 2> t{trx(), trx()};
  auto holder = rec_lock(page_id, LOCK_X, 2, t[0]);

  auto r = ut::bench("grant_handoff", 100000, [&](uint64_t i) {
    auto next = t[(i + 1) % 2];

    mutex_enter(m_mutex);
    auto waiter = m_lock_sys->rec_create_low(page_id, LockMode(LOCK_X | LOCK_WAIT), 2, N_BITS, nullptr, next);
    m_lock_sys->rec_dequeue_from_page(holder);
    mutex_exit(m_mutex);

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
      (void)rec_lock(PageId{FAKE_SPACE, page_no_t(i)}, LOCK_X, 2, t.back());
    }

    for (int i = 1; i + 1 < n; ++i) {
      (void)rec_lock(PageId{FAKE_SPACE, page_no_t(i + 1)}, LOCK_X | LOCK_WAIT, 2, t[i]);
    }

    auto req = rec_lock(PageId{FAKE_SPACE, 1}, LOCK_X | LOCK_WAIT, 2, t[0]);

    auto r = ut::bench(std::format("deadlock_check_chain{}", n), 2000, [&](uint64_t) { EXPECT_FALSE(deadlock(req, t[0])); });

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
