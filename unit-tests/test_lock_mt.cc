/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file unit-tests/test_lock_mt.cc
LockSys under concurrency: many threads request, wait for and release
locks, and the invariants are checked against a model. The single threaded
cases are in test_lock.cc.

  LockMt          in-memory lock queues on fake pages, of started
                  transactions without query threads: a thread requests a
                  lock with LockSys::rec_lock_for() (what the check and
                  lock functions do, the shard latches and all) and polls
                  for the grant (Srv::suspend_user_thread() sleeps instead).
                  The storms make their requests with the lock system
                  latched X, so that a model can check each grant against
                  what the others hold, atomically with it; a checker
                  thread validates the queues, with X, while they run.
  LockMtPerf      the in-memory lock queues, timed against the thread count.

The tests through the API are in test_lock_mt_db.cc; the scaffolding of
both in lock_mt_util.h. */

#include "lock_mt_util.h"

namespace {

/** In-memory lock queues on fake pages, of started transactions. */
class LockMt : public SharedDb {
 protected:
  enum Got { GRANTED, WAIT, DEADLOCK };

  /** A lock the model says a transaction holds. */
  struct Held {
    const Trx *m_trx;
    page_no_t m_page;
    ulint m_heap_no;
    ulint m_mode;
    uint64_t m_seq;
  };

  void TearDown() override {
    report_errors();
    EXPECT_EQ(n_fake_pages(), 0u) << "record locks left on the fake pages";
    SharedDb::TearDown();
  }

  /** A record lock request of trx, made by LockSys::rec_lock_for(): what
  the check and lock functions do (an insert intention as
  rec_insert_check_and_lock(): no lock unless it waits), without a query
  thread. Called with the lock system latched X (the latches it takes are
  then no-ops) by the storms, without it by the perf tests.
  @return GRANTED, WAIT (trx->m_wait_lock is the request), or DEADLOCK: the
  request is cancelled and trx is the victim. */
  Got rec_request(Trx *trx, const BufBlock &block, ulint mode, ulint heap_no) {
    const auto err = m_lock_sys->rec_lock_for(trx, LockMode(mode), &block, heap_no, m_index);

    if (err == DB_SUCCESS) {
      return GRANTED;
    } else if (err == DB_DEADLOCK) {
      return DEADLOCK;
    }

    EXPECT_EQ(err, DB_LOCK_WAIT);

    trx_mutex_enter(trx);
    const bool granted = trx->m_wait_lock == nullptr && !trx->m_was_chosen_as_deadlock_victim;
    trx_mutex_exit(trx);

    /* Another transaction was the deadlock victim and the request was granted. */
    return granted ? GRANTED : WAIT;
  }

  /** Poll until the lock wait of trx ends: its wait state, with its trx
  mutex. on_grant() runs with the lock system latched X when it ends with a
  grant (the wait state of trx does not change any more).
  @return true if the request was granted, false if trx was chosen as a
  deadlock victim, or if the wait lasted longer than limit: a lost grant,
  the test fails and the wait is cancelled. */
  template <typename F>
  bool await(Trx *trx, F &&on_grant, std::chrono::milliseconds limit = 20s) {
    const auto end = Clock::now() + limit;

    for (int i{};; ++i) {
      trx_mutex_enter(trx);
      const bool waits = trx->m_wait_lock != nullptr;
      trx_mutex_exit(trx);

      if (!waits) {
        x_enter();

        trx_mutex_enter(trx);
        const auto victim = trx->m_was_chosen_as_deadlock_victim;
        trx->m_was_chosen_as_deadlock_victim = false;
        trx_mutex_exit(trx);

        if (!victim) {
          on_grant();
        }

        x_exit();

        return !victim;
      }

      if (Clock::now() > end) {
        x_enter();

        if (trx->m_wait_lock != nullptr) {
          const auto queue = dump(trx->m_wait_lock);

          m_lock_sys->cancel_waiting_and_release(trx->m_wait_lock);
          ADD_FAILURE() << "trx " << trx->m_id << " waited for more than " << limit.count() << " ms, the queue:" << queue;
        }

        x_exit();

        return false;
      }

      if (i < 100) {
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(20us);
      }
    }
  }

  bool await(Trx *trx) {
    return await(trx, [] {});
  }

  /** Poll until the lock wait of trx ends, with its trx mutex only (no X
  latch: the perf tests). @return true if granted. */
  bool await_fast(Trx *trx) {
    for (int i{};; ++i) {
      trx_mutex_enter(trx);
      const bool waits = trx->m_wait_lock != nullptr;
      const bool victim = !waits && trx->m_was_chosen_as_deadlock_victim;

      if (victim) {
        trx->m_was_chosen_as_deadlock_victim = false;
      }

      trx_mutex_exit(trx);

      if (!waits) {
        return !victim;
      }

      if (i < 100) {
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(20us);
      }
    }
  }

  /** @return the locks of the queue of lock, for a failure message. */
  std::string dump(const Lock *lock) {
    std::string s;

    if (lock->type() == LOCK_TABLE) {
      for (auto l : m_table->m_locks) {
        s += std::format(" [trx {} mode {:#x}]", l->m_trx->m_id, Lock_mode_type(l->precise_mode()));
      }
    } else if (auto q = m_lock_sys->rec_queue(lock->page_id()); q != nullptr) {
      for (auto l : *q) {
        s += std::format(" [trx {} mode {:#x} heap_no", l->m_trx->m_id, Lock_mode_type(l->precise_mode()));

        for (ulint h{}; h < l->rec_get_n_bits(); ++h) {
          if (l->rec_is_nth_bit_set(h)) {
            s += std::format(" {}", h);
          }
        }

        s += "]";
      }
    }

    return s;
  }

  /** Note a broken invariant; the trx sys mutex is held. */
  void error(std::string what) {
    if (m_errors.size() < 1000) {
      m_errors.push_back(std::move(what));
    }
  }

  /** Fail the test with the broken invariants found. */
  void report_errors() {
    x_enter();
    auto errors = std::move(m_errors);
    m_errors.clear();
    x_exit();

    for (size_t i{}; i < std::min<size_t>(errors.size(), 20); ++i) {
      ADD_FAILURE() << errors[i];
    }

    EXPECT_TRUE(errors.empty()) << errors.size() << " broken invariants";
  }

  /** The model: trx was granted mode on (page, heap_no): no transaction
  holds a lock it conflicts with. An insert intention is checked only
  against the locks held since its request: a gap lock requested after it
  does not wait for it (gap requests never wait) and does not stop it. The
  trx sys mutex is held. */
  void model_grant(const Trx *trx, page_no_t page, ulint heap_no, ulint mode, uint64_t since) {
    for (const auto &h : m_held) {
      if (h.m_trx != trx && h.m_page == page && h.m_heap_no == heap_no && conflicts(mode, h.m_mode, heap_no == SUP) &&
          (!(mode & LOCK_INSERT_INTENTION) || h.m_seq <= since)) {
        error(std::format(
          "trx {} granted {:#x} on page {} heap_no {} while trx {} holds {:#x}",
          trx->m_id, mode, page, heap_no, h.m_trx->m_id, h.m_mode
        ));
      }
    }

    if (!(mode & LOCK_INSERT_INTENTION)) {
      m_held.push_back(Held{trx, page, heap_no, mode, ++m_seq});
    }
  }

  /** The model: trx is about to release its locks. The trx sys mutex is held. */
  void model_drop(const Trx *trx) {
    std::erase_if(m_held, [trx](const Held &h) { return h.m_trx == trx; });
  }

  /** Validate the queues of the fake pages and of the test table: no two
  granted locks of different transactions conflict (a granted insert
  intention only marks an insert that was let in); every waiting lock has a
  conflicting lock ahead of it (else its grant was lost); a waiting lock is
  the wait lock of its transaction. The trx sys mutex is held. */
  void check_queues() {
    std::vector<const Lock *> q;

    for (auto &shard : m_lock_sys->m_page_shards) {
     for (const auto &[page_id, locks] : shard.m_locks) {
      if (page_id.m_space_id != FAKE_SPACE) {
        continue;
      }

      ulint n_bits{};

      for (auto l : locks) {
        n_bits = std::max<ulint>(n_bits, l->rec_get_n_bits());
      }

      for (ulint h{}; h < n_bits; ++h) {
        q.clear();

        for (auto l : locks) {
          if (l->rec_is_nth_bit_set(h)) {
            q.push_back(l);
          }
        }

        if (!q.empty()) {
          check_queue(q, h, page_id.m_page_no);
        }
      }
     }
    }

    q.clear();

    for (auto l : m_table->m_locks) {
      q.push_back(l);
    }

    for (size_t i{}; i < q.size(); ++i) {
      auto l = q[i];

      if (l->is_waiting()) {
        if (l->m_trx->m_wait_lock != l) {
          error(std::format("waiting table lock {:#x} is not the wait lock of trx {}", Lock_mode_type(l->precise_mode()), l->m_trx->m_id));
        }

        bool blocked{};

        for (size_t j{}; j < i && !blocked; ++j) {
          blocked = q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode());
        }

        if (!blocked) {
          error(std::format("table lock {:#x} of trx {} waits for nothing: a lost grant", Lock_mode_type(l->precise_mode()), l->m_trx->m_id));
        }
      } else {
        for (size_t j{}; j < i; ++j) {
          if (q[j]->is_waiting() && q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode())) {
            error(std::format("granted table lock {:#x} of trx {} is behind a waiting {:#x} of trx {}: it barged",
              Lock_mode_type(l->precise_mode()), l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id));
          }
        }

        for (size_t j{}; j < q.size(); ++j) {
          if (j != i && !q[j]->is_waiting() && q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode())) {
            error(std::format("granted table locks {:#x} of trx {} and {:#x} of trx {} conflict",
              Lock_mode_type(l->precise_mode()), l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id));
          }
        }
      }
    }
  }

  /** check_queues() of the locks q on one record. */
  void check_queue(const std::vector<const Lock *> &q, ulint heap_no, page_no_t page) {
    const bool sup = heap_no == SUP;

    for (size_t i{}; i < q.size(); ++i) {
      auto l = q[i];
      const auto mode = Lock_mode_type(l->precise_mode()) & ~LOCK_WAIT;

      if (l->is_waiting()) {
        if (l->m_trx->m_wait_lock != l) {
          error(std::format("page {} heap_no {}: waiting lock {:#x} is not the wait lock of trx {}", page, heap_no, mode, l->m_trx->m_id));
        }

        bool blocked{};

        for (size_t j{}; j < i && !blocked; ++j) {
          blocked = q[j]->m_trx != l->m_trx && conflicts(mode, q[j]->precise_mode() & ~LOCK_WAIT, sup);
        }

        if (!blocked) {
          error(std::format("page {} heap_no {}: lock {:#x} of trx {} waits for nothing: a lost grant", page, heap_no, mode, l->m_trx->m_id));
        }
      } else if (!(mode & LOCK_INSERT_INTENTION)) {
        for (size_t j{}; j < q.size(); ++j) {
          if (j != i && !q[j]->is_waiting() && q[j]->m_trx != l->m_trx && conflicts(mode, q[j]->precise_mode(), sup)) {
            error(std::format(
              "page {} heap_no {}: granted locks {:#x} of trx {} and {:#x} of trx {} conflict",
              page, heap_no, mode, l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id
            ));
          }
        }
      }
    }
  }

  /** A thread that runs check_queues() every period until stopped. */
  struct Checker {
    Checker(LockMt *test, std::chrono::microseconds period) : m_thread([this, test, period] {
      while (!m_stop.load(std::memory_order_acquire)) {
        test->x_enter();
        test->check_queues();
        test->x_exit();
        ++m_n;
        std::this_thread::sleep_for(period);
      }
    }) {}

    ~Checker() { stop(); }

    void stop() {
      m_stop.store(true, std::memory_order_release);

      if (m_thread.joinable()) {
        m_thread.join();
      }
    }

    std::atomic<bool> m_stop{};
    std::atomic<uint64_t> m_n{};
    std::thread m_thread;
  };

  /** @return the number of fake pages with record locks. */
  size_t n_fake_pages() {
    x_enter();

    size_t n{};

    for (auto &shard : m_lock_sys->m_page_shards) {
      n += size_t(std::count_if(shard.m_locks.begin(), shard.m_locks.end(), [](const auto &e) {
        return e.first.m_space_id == FAKE_SPACE;
      }));
    }

    x_exit();

    return size_t(n);
  }

  /** Release the locks of trx in memory and commit it. */
  void finish(Trx *trx) {
    x_enter();
    model_drop(trx);
    x_exit();
    commit(trx);
  }

  /** The model: the record locks the threads hold. Protected by the trx sys mutex. */
  std::vector<Held> m_held;

  /** The order of the grants of the model. Protected by the trx sys mutex. */
  uint64_t m_seq{};

  /** The broken invariants found. Protected by the trx sys mutex. */
  std::vector<std::string> m_errors;
};

/** Threads lock records of two pages at random with every record lock mode
- plain (next-key), gap, record only and insert intention, S and X - on the
supremum and on the first and last user records of the page, 1 to 4 locks
per transaction, then commit or roll back. Upgrades (S then X) and
deadlocks happen. A checker validates the queues while they run, and the
model checks each grant against the locks the others hold: no two granted
locks conflict, no insert into a locked gap, no waiting lock that nothing
blocks (a lost grant). Every request ends granted or as a deadlock victim,
and no lock remains. */
TEST_F(LockMt, RecStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_ROUNDS = 300;

  FakePage p1(1);
  FakePage p2(2);
  const std::array<FakePage *, 2> pages{&p1, &p2};

  /* The supremum, the first user record, two more, and the last record of the page (n_heap 10). */
  const std::array<ulint, 5> heaps{SUP, PAGE_HEAP_NO_USER_LOW, 3, 4, 9};
  const std::array<ulint, 7> modes{
    LOCK_S, LOCK_X, LOCK_S | LOCK_GAP, LOCK_X | LOCK_GAP, LOCK_S | LOCK_REC_NOT_GAP, LOCK_X | LOCK_REC_NOT_GAP, II
  };
  const std::array<ulint, 3> sup_modes{LOCK_S, LOCK_X, II};

  std::atomic<uint64_t> n_req{};
  std::atomic<uint64_t> n_granted{};
  std::atomic<uint64_t> n_victims{};
  std::atomic<uint64_t> n_waits{};

  Checker checker(this, 100us);
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t + 1);

      for (int r{}; r < N_ROUNDS; ++r) {
        auto trx = begin();
        bool victim{};
        const int n = 1 + int(rng() % 4);

        for (int j{}; j < n && !victim; ++j) {
          auto page = pages[rng() % pages.size()];
          const auto heap_no = heaps[rng() % heaps.size()];
          const auto mode = heap_no == SUP ? sup_modes[rng() % sup_modes.size()] : modes[rng() % modes.size()];
          const auto page_no = page->id().m_page_no;

          ++n_req;

          x_enter();

          const auto since = m_seq;
          const auto got = rec_request(trx, page->m_block, mode, heap_no);

          if (got == GRANTED) {
            model_grant(trx, page_no, heap_no, mode, since);
          }

          x_exit();

          if (got == WAIT) {
            ++n_waits;
            victim = !await(trx, [&] { model_grant(trx, page_no, heap_no, mode, since); });
          } else {
            victim = got == DEADLOCK;
          }

          ++(victim ? n_victims : n_granted);
        }

        for (int k = int(rng() % 20); k > 0; --k) {
          std::this_thread::yield();
        }

        x_enter();
        model_drop(trx);
        x_exit();

        if (victim || rng() % 4 == 0) {
          rollback(trx);
        } else {
          commit(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  x_enter();
  check_queues();
  x_exit();

  EXPECT_EQ(n_req.load(), n_granted.load() + n_victims.load());
  EXPECT_GT(n_waits.load(), 0u) << "no request waited: the storm tests nothing";
  EXPECT_GT(checker.m_n.load(), 0u);

  std::printf("RecStorm: %lu requests, %lu waits, %lu deadlock victims, %lu checks\n",
    (unsigned long)n_req.load(), (unsigned long)n_waits.load(), (unsigned long)n_victims.load(), (unsigned long)checker.m_n.load());
}

/** Cycles of 2 to 32 transactions formed concurrently: each holds a record,
and its thread requests the record of the next one, all at the same time.
Exactly one transaction per cycle is the victim, the others are granted in
turn. The victim follows the weight rule of deadlock_occurs(): the
requester that closed the cycle, unless the transaction that waits for it
(the one before it in the cycle) weighs less (Trx::weight_cmp(): locks and
undo records), then that one. Each transaction weighs 2 + extra locks + 1
for its waiting lock, the extra locks a permutation: no ties. */
TEST_F(LockMt, DeadlockCycles) {
  constexpr std::array<int, 8> sizes{2, 2, 3, 3, 4, 5, 8, 32};

  FakePage ring(1, 40);
  std::mt19937 rng(42);
  std::map<int, int> n_by_size;

  for (int round{}; round < 40; ++round) {
    const int n = sizes[rng() % sizes.size()];
    std::vector<int> extra(n);
    std::vector<Trx *> trxs;

    std::iota(extra.begin(), extra.end(), 0);
    std::shuffle(extra.begin(), extra.end(), rng);

    for (int i{}; i < n; ++i) {
      trxs.push_back(begin());
    }

    x_enter();

    for (int i{}; i < n; ++i) {
      EXPECT_EQ(rec_request(trxs[i], ring.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), GRANTED);

      for (int e{}; e < extra[i]; ++e) {
        (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(1000 + i * 64 + e)}, LOCK_X, 2, 16, m_index, trxs[i]);
      }
    }

    x_exit();

    std::barrier sync(n);
    std::atomic<int> closer{-1};
    std::vector<int> victim(n);
    std::vector<std::thread> threads;

    for (int i{}; i < n; ++i) {
      threads.emplace_back([&, i] {
        auto trx = trxs[i];

        sync.arrive_and_wait();

        x_enter();

        const auto got = rec_request(trx, ring.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + (i + 1) % n);

        /* The request that closes the cycle finds the deadlock: it is the
        victim, or it chooses another one, which has not seen it yet (that
        needs the mutex). */
        if (got == DEADLOCK || std::any_of(trxs.begin(), trxs.end(), [](const Trx *t) { return t->m_was_chosen_as_deadlock_victim; })) {
          EXPECT_EQ(closer.exchange(i), -1) << "two requests closed the cycle";
        }

        x_exit();

        victim[i] = got == DEADLOCK || (got == WAIT && !await(trx));

        if (victim[i]) {
          rollback(trx);
        } else {
          commit(trx);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    ++n_by_size[n];

    const auto n_victims = std::count(victim.begin(), victim.end(), 1);

    ASSERT_EQ(n_victims, 1) << "cycle of " << n;
    ASSERT_GE(closer.load(), 0);

    const int c = closer.load();
    const int p = (c + n - 1) % n;
    const int v = int(std::find(victim.begin(), victim.end(), 1) - victim.begin());

    if (v == c) {
      EXPECT_GT(extra[p], extra[c]) << "the requester was the victim, but the one waiting for it weighs less";
    } else {
      EXPECT_EQ(v, p) << "the victim is neither the requester nor the transaction waiting for it";
      EXPECT_LT(extra[p], extra[c]) << "the other transaction was the victim, but it weighs more";
    }

    EXPECT_EQ(n_fake_pages(), 0u);
  }

  for (auto [n, count] : n_by_size) {
    std::printf("DeadlockCycles: %d cycles of %d\n", count, n);
  }
}

/** Cycles of 2 to 16 transactions formed concurrently in different page
shards, with no lock system latch held by the test: each transaction holds a
record of a page of its own, and its thread requests the record of the next
one, all at the same time. Each request publishes its edge and walks latch
free (lock0deadlock.h): two requests closing a cycle at once, in two shards,
must not both miss the other's edge (the edges are stored and loaded
sequentially consistent). Exactly one victim per cycle, the others granted,
nobody waits for a timeout (await() fails after 20 s). 60 rounds. */
TEST_F(LockMt, DeadlockCyclesSharded) {
  constexpr std::array<int, 6> sizes{2, 2, 3, 4, 8, 16};

  std::mt19937 rng(43);

  for (int round{}; round < 60; ++round) {
    const int n = sizes[rng() % sizes.size()];
    std::vector<std::unique_ptr<FakePage>> pages;
    std::vector<Trx *> trxs;

    for (int i{}; i < n; ++i) {
      pages.push_back(std::make_unique<FakePage>(page_no_t(5000 + 97 * i)));
      trxs.push_back(begin());
      ASSERT_EQ(rec_request(trxs[i], pages[i]->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), GRANTED);
    }

    std::barrier sync(n);
    std::vector<int> victim(n);
    std::vector<std::thread> threads;

    for (int i{}; i < n; ++i) {
      threads.emplace_back([&, i] {
        auto trx = trxs[i];

        sync.arrive_and_wait();

        const auto got = rec_request(trx, pages[(i + 1) % n]->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2);

        victim[i] = got == DEADLOCK || (got == WAIT && !await(trx));

        if (victim[i]) {
          rollback(trx);
        } else {
          commit(trx);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    EXPECT_EQ(std::count(victim.begin(), victim.end(), 1), 1) << "round " << round << ": cycle of " << n;
    EXPECT_EQ(n_fake_pages(), 0u);
  }
}

/** The depth limit of the deadlock search (200): a chain of waits whose
last waiting transaction is at depth 200 is searched to its end, no
deadlock; one transaction more and the search gives up: the requester is
rolled back as if it had deadlocked. */
TEST_F(LockMt, DeadlockDepthEdge) {
  for (auto [n, expected] : {std::pair{int(MAX_DEPTH) + 2, false}, std::pair{int(MAX_DEPTH) + 3, true}}) {
    std::vector<Trx *> t;

    for (int i{}; i < n; ++i) {
      t.push_back(begin());
    }

    x_enter();

    for (int i{}; i < n; ++i) {
      (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(i)}, LOCK_X, 2, 16, m_index, t[i]);
    }

    /* t[i] waits for the record of t[i + 1] (at depth i of the search), the last does not wait. */
    for (int i = 1; i + 1 < n; ++i) {
      (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(i + 1)}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, t[i]);
      t[i]->m_que_state = TRX_QUE_LOCK_WAIT;
    }

    auto req = m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, t[0]);

    EXPECT_EQ(m_lock_sys->deadlock_occurs(req, t[0]), expected) << n << " transactions";

    for (auto trx : t) {
      m_lock_sys->release(trx);
    }

    for (auto trx : t) {
      trx->m_wait_lock = nullptr;
      trx->m_que_state = TRX_QUE_RUNNING;
    }

    x_exit();

    for (auto trx : t) {
      commit(trx);
    }
  }
}

/** The depth limit of the deadlock search at its edge: a chain a -> b -> c
(a waits for b, which waits for c). The search follows one edge per waiting
transaction (Trx::m_blocking_trx) and gives up at an edge when it is past
the limit: started at depth 200 it gives up at b's edge, at 199 it does not.
There is no step limit any more (it was 1,000,000 transactions visited by
the old depth first search over the queues): a walk of one edge per
transaction takes at most depth steps, the depth limit bounds it. */
TEST_F(LockMt, SearchLimits) {
  auto a = begin();
  auto b = begin();
  auto c = begin();

  x_enter();

  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LOCK_X, 2, 16, m_index, b);
  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 2}, LOCK_X, 2, 16, m_index, c);
  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 2}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, b);
  b->m_que_state = TRX_QUE_LOCK_WAIT;

  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, a);

  EXPECT_EQ(a->m_blocking_trx.load(), b);
  EXPECT_EQ(b->m_blocking_trx.load(), c);
  EXPECT_EQ(c->m_blocking_trx.load(), nullptr);

  Trx *pred{};

  EXPECT_EQ(m_lock_sys->deadlock_search(a, MAX_DEPTH, &pred), DeadlockWalk::TOO_DEEP);
  EXPECT_EQ(m_lock_sys->deadlock_search(a, MAX_DEPTH - 1, &pred), DeadlockWalk::NONE);
  EXPECT_EQ(m_lock_sys->deadlock_search(a, 0, &pred), DeadlockWalk::NONE);

  for (auto t : {a, b, c}) {
    m_lock_sys->release(t);
    t->m_wait_lock = nullptr;
    t->m_que_state = TRX_QUE_RUNNING;
  }

  x_exit();

  for (auto t : {a, b, c}) {
    commit(t);
  }
}

/** The edges of the waits-for graph: a waiting transaction publishes a
transaction that blocks it (Trx::m_blocking_trx) when it starts to wait; a
release that leaves it waiting moves the edge to a blocker that is still
there; the grant clears it. a and b hold S on a record, w waits for X: its
edge names a or b. The one it names commits: the edge moves to the other.
That one commits: w is granted, no edge. */
TEST_F(LockMt, WaitEdges) {
  FakePage page(1);

  auto a = begin();
  auto b = begin();
  auto w = begin();

  x_enter();
  ASSERT_EQ(rec_request(a, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
  ASSERT_EQ(rec_request(b, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
  ASSERT_EQ(rec_request(w, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), WAIT);

  auto first = w->m_blocking_trx.load();

  ASSERT_TRUE(first == a || first == b);

  auto second = first == a ? b : a;

  x_exit();

  commit(first);

  x_enter();
  EXPECT_EQ(w->m_blocking_trx.load(), second) << "the edge did not move to the remaining blocker";
  EXPECT_NE(w->m_wait_lock, nullptr);
  x_exit();

  commit(second);

  EXPECT_TRUE(await(w));

  x_enter();
  EXPECT_EQ(w->m_blocking_trx.load(), nullptr);
  x_exit();

  commit(w);
}

/** A cycle that the one edge graph does not hold: a and b hold S on record
2, w holds X on record 3 and waits for X on record 2 (blocked by a and b; a
comes first in the queue and neither waits, its edge names a); b requests
record 3, held by w. The full graph has the cycle b -> w -> b; the one edge
walk from b goes b -> w -> a and stops at a, which runs (and could be an
idle client transaction that never ends). w has two blockers, so the full
search runs and finds the cycle at b's request (lock0deadlock.h): b gets the
deadlock at once (it closed the cycle, and w weighs the same), not when a
ends. a commits, w is granted. 20 rounds. */
TEST_F(LockMt, DeadlockPastRunningBlocker) {
  FakePage page(1);

  for (int round{}; round < 20; ++round) {
    auto a = begin();
    auto b = begin();
    auto w = begin();

    x_enter();
    ASSERT_EQ(rec_request(a, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
    ASSERT_EQ(rec_request(b, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
    ASSERT_EQ(rec_request(w, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 3), GRANTED);
    ASSERT_EQ(rec_request(w, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), WAIT);

    /* The queue order makes a the chosen blocker; there are two. */
    EXPECT_EQ(w->m_blocking_trx.load(), a);
    EXPECT_TRUE(w->m_blocking_multi.load());

    const auto got = rec_request(b, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 3);

    EXPECT_EQ(got, DEADLOCK) << "round " << round << ": the cycle b -> w -> b past the running a was missed";
    EXPECT_FALSE(w->m_was_chosen_as_deadlock_victim);
    x_exit();

    rollback(b);
    commit(a);

    EXPECT_TRUE(await(w));
    commit(w);

    EXPECT_EQ(n_fake_pages(), 0u);
  }
}

/** Upgrades with idle readers: k transactions (2 to 8) and m idle ones (0
to 3) hold S on a record, in a random order in the queue; then the k
request X, one after the other in a random order. Each upgrader is blocked
by every other holder, the idle ones included, so the waits-for graph of
the upgraders has cycles that pass by idle transactions that never end: the
one edge walk can stop at an idle one, the full search must find them (see
lock0deadlock.h). Each request ends at once (no hang until a timeout); the
victims roll back; in the end exactly k - 1 upgraders are victims, and the
last one is granted when the idle ones commit. 300 rounds. */
TEST_F(LockMt, UpgradesPastIdleReaders) {
  FakePage page(1);
  std::mt19937 rng(31);

  for (int round{}; round < 300; ++round) {
    const int k = 2 + int(rng() % 7);
    const int m = int(rng() % 4);
    std::vector<Trx *> up;
    std::vector<Trx *> idle;
    std::vector<Trx *> all;

    for (int i{}; i < k; ++i) {
      up.push_back(begin());
      all.push_back(up.back());
    }

    for (int i{}; i < m; ++i) {
      idle.push_back(begin());
      all.push_back(idle.back());
    }

    std::shuffle(all.begin(), all.end(), rng);

    x_enter();

    for (auto t : all) {
      ASSERT_EQ(rec_request(t, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
    }

    x_exit();

    std::shuffle(up.begin(), up.end(), rng);

    int n_victims{};
    std::set<Trx *> gone;

    for (auto t : up) {
      x_enter();
      const auto got = rec_request(t, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2);

      std::vector<Trx *> victims;

      if (got == DEADLOCK) {
        victims.push_back(t);
      }

      for (auto u : up) {
        if (u->m_was_chosen_as_deadlock_victim) {
          u->m_was_chosen_as_deadlock_victim = false;
          victims.push_back(u);
        }
      }

      x_exit();

      for (auto v : victims) {
        ++n_victims;
        gone.insert(v);
        rollback(v);
      }
    }

    EXPECT_EQ(n_victims, k - 1) << "round " << round << ": k " << k << " m " << m;

    for (auto t : idle) {
      commit(t);
    }

    for (auto t : up) {
      if (!gone.contains(t)) {
        EXPECT_TRUE(await(t, [] {}, 10s)) << "round " << round;
        commit(t);
      }
    }

    EXPECT_EQ(n_fake_pages(), 0u);
  }
}

/** A record held in X with 16 requests queued behind it in a known order,
X and S mixed: when the holder commits they are granted in queue order,
compatible S requests together, each exactly once, never with an
incompatible lock ahead still held; each waiter commits as soon as it is
granted, racing the grants of the next ones. 200 rounds. */
TEST_F(LockMt, HandoffFifo) {
  constexpr int N = 16;
  constexpr ulint HEAP_NO = 2;

  FakePage page(1);
  std::mt19937 rng(7);

  for (int round{}; round < 200; ++round) {
    std::vector<ulint> modes(N);
    std::vector<Trx *> trxs;
    std::vector<int> granted(N);
    std::vector<int> released(N);

    for (auto &m : modes) {
      m = (rng() % 3 == 0 ? LOCK_X : LOCK_S) | LOCK_REC_NOT_GAP;
    }

    auto holder = begin();

    x_enter();
    ASSERT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, HEAP_NO), GRANTED);
    x_exit();

    for (int i{}; i < N; ++i) {
      trxs.push_back(begin());
    }

    std::atomic<int> ticket{};
    std::vector<std::thread> threads;

    for (int i{}; i < N; ++i) {
      threads.emplace_back([&, i] {
        while (ticket.load(std::memory_order_acquire) != i) {
          std::this_thread::yield();
        }

        x_enter();
        const auto got = rec_request(trxs[i], page.m_block, modes[i], HEAP_NO);
        x_exit();

        ticket.store(i + 1, std::memory_order_release);

        EXPECT_EQ(got, WAIT) << i;

        const auto ok = await(trxs[i], [&] {
          for (int j{}; j < i; ++j) {
            /* trxs[j] is alive until released[j] is set. */
            if (!released[j] && trxs[j]->m_wait_lock != nullptr) {
              error(std::format("round {}: request {} granted before request {} ahead of it", round, i, j));
            }

            if ((modes[i] & LOCK_MODE_MASK) == LOCK_X || (modes[j] & LOCK_MODE_MASK) == LOCK_X) {
              if (!released[j]) {
                error(std::format("round {}: request {} granted while request {} ahead holds an incompatible lock", round, i, j));
              }
            }
          }

          ++granted[i];
        });

        EXPECT_TRUE(ok) << i;

        x_enter();
        released[i] = 1;
        x_exit();

        commit(trxs[i]);
      });
    }

    while (ticket.load(std::memory_order_acquire) != N) {
      std::this_thread::yield();
    }

    commit(holder);

    for (auto &t : threads) {
      t.join();
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(granted[i], 1) << "round " << round << " request " << i;
    }
  }
}

/** 256 transactions on one record at once: X, X record only, insert
intentions, S gap locks and plain S. Each request is granted exactly once
and the model and the checker find no conflict: a large queue of one record
is granted correctly. */
TEST_F(LockMt, ManyOnOne) {
  constexpr int N = 256;
  constexpr ulint HEAP_NO = 2;
  const std::array<ulint, 8> modes{LOCK_X, LOCK_X | LOCK_REC_NOT_GAP, II, LOCK_S | LOCK_GAP, LOCK_S, LOCK_S, LOCK_S | LOCK_REC_NOT_GAP, LOCK_S};

  FakePage page(1);
  std::vector<Trx *> trxs;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
  }

  std::atomic<int> n_granted{};
  std::atomic<int> n_victims{};
  std::barrier sync(N);
  Checker checker(this, 500us);
  std::vector<std::thread> threads;

  for (int i{}; i < N; ++i) {
    threads.emplace_back([&, i] {
      auto trx = trxs[i];
      const auto mode = modes[i % modes.size()];

      sync.arrive_and_wait();

      x_enter();

      const auto since = m_seq;
      const auto got = rec_request(trx, page.m_block, mode, HEAP_NO);

      if (got == GRANTED) {
        model_grant(trx, 1, HEAP_NO, mode, since);
      }

      x_exit();

      bool ok = got == GRANTED;

      if (got == WAIT) {
        ok = await(trx, [&] { model_grant(trx, 1, HEAP_NO, mode, since); }, 60s);
      }

      ++(ok ? n_granted : n_victims);

      std::this_thread::sleep_for(50us);

      finish(trx);
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  /* Only upgrades deadlock, and there are none: one request per transaction. */
  EXPECT_EQ(n_granted.load(), N);
  EXPECT_EQ(n_victims.load(), 0);
}

/** One transaction holds 100,000 record locks on 2,000 pages (in most of
the page shards), and the record with the largest heap number of a page
with 8,192 records. 8 threads wait for records of it. Its commit releases
the locks shard by shard, at most LOCK_RELEASE_BATCH locks per hold of a
shard latch, while the checker runs (with X, between the batches): every
waiter is granted, no lock is left. */
TEST_F(LockMt, BigTrx) {
  constexpr page_no_t N_PAGES = 2000;
  constexpr ulint N_HEAPS = 50;
  constexpr int N_WAITERS = 8;

  std::vector<std::unique_ptr<FakePage>> pages;

  for (page_no_t p{}; p < N_PAGES; ++p) {
    pages.push_back(std::make_unique<FakePage>(10 + p, N_HEAPS + 2));
  }

  FakePage big(5, MAX_HEAP_NO + 1);

  auto holder = begin();

  x_enter();

  for (auto &page : pages) {
    for (ulint h = 2; h < N_HEAPS + 2; ++h) {
      ASSERT_EQ(rec_request(holder, page->m_block, LOCK_X | LOCK_REC_NOT_GAP, h), GRANTED);
    }
  }

  ASSERT_EQ(rec_request(holder, big.m_block, LOCK_X | LOCK_REC_NOT_GAP, MAX_HEAP_NO), GRANTED);

  const auto last = holder->m_trx_locks.back();

  EXPECT_GE(last->rec_get_n_bits(), MAX_HEAP_NO + 1);
  EXPECT_TRUE(last->rec_is_nth_bit_set(MAX_HEAP_NO));

  ASSERT_EQ(rec_request(holder, big.m_block, LOCK_X, SUP), GRANTED);

  const auto n_structs = holder->m_trx_locks.size();

  x_exit();

  EXPECT_GT(n_structs, 2 * LockSys::LOCK_RELEASE_BATCH) << "the release must take several batches";

  std::atomic<int> n_waiting{};
  std::atomic<int> n_granted{};
  std::vector<std::thread> threads;
  Checker checker(this, 5ms);

  for (int t{}; t < N_WAITERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      auto trx = begin();
      auto &block = t == 0 ? big.m_block : pages[rng() % N_PAGES]->m_block;
      const auto heap_no = t == 0 ? MAX_HEAP_NO : 2 + rng() % N_HEAPS;

      x_enter();
      const auto got = rec_request(trx, block, LOCK_S | LOCK_REC_NOT_GAP, heap_no);
      x_exit();

      EXPECT_EQ(got, WAIT);
      ++n_waiting;

      if (await(trx)) {
        ++n_granted;
      }

      commit(trx);
    });
  }

  while (n_waiting.load() < N_WAITERS) {
    std::this_thread::sleep_for(1ms);
  }

  const auto start = Clock::now();

  commit(holder);

  const std::chrono::duration<double, std::milli> ms = Clock::now() - start;

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  EXPECT_EQ(n_granted.load(), N_WAITERS);
  std::printf("BigTrx: commit of %lu locks in %lu structs: %.1f ms\n", (unsigned long)(N_PAGES * N_HEAPS + 2), (unsigned long)n_structs, ms.count());
}

/** The release of a lock with W waiters behind it grants them in one pass
over the page queue, O(W) (the per record summaries of the grant pass), not
O(W^2) like a scan from the front of the queue for each waiter. Two shapes,
each at W = 256 and W = 4096: (1) one holder of W records of a page and an X
waiter for each record; (2) one holder of a record in X and W waiters for it
in S, all granted together. Every waiter is granted. The time per grant at
4096 is less than 4 times the time at 256, the best of 3 runs (a quadratic
release is 16 times). */
TEST_F(LockMt, ReleaseLinear) {
  auto run = [&](ulint w, bool one_rec) -> double {
    FakePage page(1, one_rec ? 10 : w + 2);
    auto holder = begin();
    std::vector<Trx *> trxs;

    for (ulint i{}; i < w; ++i) {
      trxs.push_back(begin());
    }

    x_enter();

    for (ulint i{}; i < (one_rec ? 1 : w); ++i) {
      EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), GRANTED);
    }

    for (ulint i{}; i < w; ++i) {
      const auto mode = one_rec ? LOCK_S | LOCK_REC_NOT_GAP : LOCK_X | LOCK_REC_NOT_GAP;
      EXPECT_EQ(rec_request(trxs[i], page.m_block, mode, one_rec ? 2 : 2 + i), WAIT);
    }

    const auto start = Clock::now();

    m_lock_sys->release(holder);

    const std::chrono::duration<double> secs = Clock::now() - start;

    for (auto trx : trxs) {
      EXPECT_EQ(trx->m_wait_lock, nullptr) << "a waiter was not granted";
    }

    x_exit();

    commit(holder);

    for (auto trx : trxs) {
      commit(trx);
    }

    return secs.count() / double(w);
  };

  for (bool one_rec : {false, true}) {
    double small{1e9};
    double big{1e9};

    /* A busy host can push a few rounds over the limit, a release that is
    not linear stays over it: keep the best time of each size, and measure
    again while the ratio is over. */
    for (int i{}; i < 12; ++i) {
      small = std::min(small, run(256, one_rec));
      big = std::min(big, run(4096, one_rec));

      if (i >= 2 && big < 4 * small) {
        break;
      }
    }

    std::printf("ReleaseLinear %s: %.0f ns per grant at W=256, %.0f ns at W=4096\n", one_rec ? "one record" : "W records", small * 1e9, big * 1e9);

    EXPECT_LT(big, 4 * small) << (one_rec ? "one record" : "W records") << ": the release is not linear in the waiters";
  }
}

/** The deadlock check of a request that waits costs the same with 10 and
with 4,000 idle active transactions (it used to clear a mark on every active
transaction first, O(active)): at 4,000 it is less than 3 times the cost at
10, plus 2 us for noise, the best of 5 batches of 200 checks. */
TEST_F(LockMt, DeadlockIdle) {
  FakePage page(1);

  auto cost = [&](int n_idle) -> double {
    std::vector<Trx *> idle;

    for (int i{}; i < n_idle; ++i) {
      idle.push_back(begin());
    }

    auto holder = begin();
    auto req_trx = begin();

    x_enter();
    EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), GRANTED);
    auto req = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT), &page.m_block, 2, m_index, req_trx);
    x_exit();

    double best{1e9};

    for (int b{}; b < 5; ++b) {
      x_enter();

      const auto start = Clock::now();

      for (int i{}; i < 200; ++i) {
        EXPECT_FALSE(m_lock_sys->deadlock_occurs(req, req_trx));
      }

      best = std::min(best, std::chrono::duration<double>(Clock::now() - start).count() / 200);

      x_exit();
    }

    x_enter();
    m_lock_sys->release(req_trx);
    req_trx->m_wait_lock = nullptr;
    x_exit();

    commit(req_trx);
    commit(holder);

    for (auto trx : idle) {
      commit(trx);
    }

    return best;
  };

  const auto few = cost(10);
  const auto many = cost(4000);

  std::printf("DeadlockIdle: %.0f ns per check with 10 idle transactions, %.0f ns with 4000\n", few * 1e9, many * 1e9);

  EXPECT_LT(many, 3 * few + 2e-6) << "the deadlock check depends on the number of active transactions";
}

/** @return two page numbers of FAKE_SPACE after from: one in the same page
shard as from, one in another. */
std::pair<page_no_t, page_no_t> shard_pages(page_no_t from) {
  const auto shard = LockSys::page_shard_no(PageId{FAKE_SPACE, from});
  page_no_t same{};
  page_no_t other{};

  for (page_no_t p = from + 1; same == 0 || other == 0; ++p) {
    if (LockSys::page_shard_no(PageId{FAKE_SPACE, p}) == shard) {
      same = same == 0 ? p : same;
    } else {
      other = other == 0 ? p : other;
    }
  }

  return {same, other};
}

/** Locks moved between two pages by a page operation
(update_copy_and_discard(): the locks on the supremum of one page go to the
supremum of the other, the first page is discarded), the
two pages in the same page shard and in two different shards (the operation
takes both shard latches, in the order of their numbers). A holds S on the
supremum of the donor (a gap lock), w waits there with an insert intention;
the move takes both to the receiver, w still waiting behind a there, its
edge to a, the donor without locks. a commits: w is granted on the
receiver. Meanwhile 4 threads lock and release records of other pages of the
same two shards. 50 rounds of each. */
TEST_F(LockMt, ShardBoundary) {
  const page_no_t donor_no = 7;
  const auto [same_no, other_no] = shard_pages(donor_no);

  ASSERT_EQ(LockSys::page_shard_no(PageId{FAKE_SPACE, same_no}), LockSys::page_shard_no(PageId{FAKE_SPACE, donor_no}));
  ASSERT_NE(LockSys::page_shard_no(PageId{FAKE_SPACE, other_no}), LockSys::page_shard_no(PageId{FAKE_SPACE, donor_no}));

  std::atomic<bool> stop{};
  std::atomic<uint64_t> n_ops{};
  std::vector<std::thread> threads;

  /* The noise: pages of the shards of the donor and of the receivers. */
  const auto [same2, other2] = shard_pages(other_no);
  std::vector<std::unique_ptr<FakePage>> noise;

  for (auto p : {shard_pages(same_no).first, same2, other2, shard_pages(donor_no + 1000).first}) {
    noise.push_back(std::make_unique<FakePage>(p));
  }

  for (int t{}; t < 4; ++t) {
    threads.emplace_back([&, t] {
      auto trx = begin();

      while (!stop.load()) {
        auto &page = *noise[t % noise.size()];

        EXPECT_EQ(rec_request(trx, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + n_ops % 5), GRANTED);
        m_lock_sys->release(trx);
        ++n_ops;
      }

      commit(trx);
    });
  }

  for (auto receiver_no : {same_no, other_no}) {
    for (int round{}; round < 50; ++round) {
      FakePage donor(donor_no);
      FakePage receiver(receiver_no);

      auto a = begin();
      auto w = begin();

      ASSERT_EQ(rec_request(a, donor.m_block, LOCK_S, SUP), GRANTED);
      ASSERT_EQ(rec_request(w, donor.m_block, II, SUP), WAIT);

      /* The page operation, with its own latches: the donor is discarded. */
      m_lock_sys->update_copy_and_discard(&receiver.m_block, &donor.m_block);

      x_enter();

      EXPECT_EQ(m_lock_sys->rec_queue(donor.id()), nullptr) << "locks left on the donor";
      ASSERT_NE(w->m_wait_lock, nullptr);
      EXPECT_EQ(w->m_wait_lock->page_id(), receiver.id());
      EXPECT_EQ(w->m_blocking_trx.load(), a);

      x_exit();

      commit(a);

      EXPECT_TRUE(await(w)) << "receiver " << receiver_no << " round " << round;

      commit(w);
    }
  }

  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(n_ops.load(), 0u);
  EXPECT_EQ(n_fake_pages(), 0u);
}

/** A commit of a transaction with 100,000 record locks, on 100,000 pages
(spread over all the page shards), while 8 threads lock and release records
of pages of their own, in the same shards: the commit holds a shard latch
for at most LOCK_RELEASE_BATCH locks and nothing global, so no other thread
waits long. The longest lock and release of the others during the commit is
under 20 ms (100 ms with a sanitizer); it was a hold of the trx sys mutex for
1,000 locks at a time, and every other operation needed that mutex. */
TEST_F(LockMt, BigCommitOtherShards) {
  constexpr page_no_t N_LOCKS = 100000;
  constexpr int N_THREADS = 8;

  auto holder = begin();

  /* Pages without frames: the locks are made directly, each under the
  latch of its shard. */
  for (page_no_t p{}; p < N_LOCKS; ++p) {
    const PageId page_id{FAKE_SPACE, 200000 + p};
    LockSys::PageLatch latch(*m_lock_sys, page_id);

    (void)m_lock_sys->rec_create_low(page_id, LockMode(LOCK_X | LOCK_REC_NOT_GAP), 2, 16, m_index, holder);
  }

  std::atomic<bool> committing{};
  std::atomic<bool> stop{};
  std::vector<std::vector<double>> lat(N_THREADS);
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      FakePage mine(100000 + t);
      auto trx = begin();

      while (!stop.load()) {
        const auto start = Clock::now();

        EXPECT_EQ(rec_request(trx, mine.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), GRANTED);
        m_lock_sys->release(trx);

        if (committing.load()) {
          lat[t].push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
        }
      }

      commit(trx);
    });
  }

  std::this_thread::sleep_for(20ms);

  committing = true;

  const auto start = Clock::now();

  commit(holder);

  const std::chrono::duration<double, std::milli> ms = Clock::now() - start;

  committing = false;
  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  std::vector<double> all;

  for (auto &v : lat) {
    all.insert(all.end(), v.begin(), v.end());
  }

  ASSERT_FALSE(all.empty());

  const auto max = *std::max_element(all.begin(), all.end());

  std::printf("BigCommitOtherShards: commit of %lu locks %.1f ms; others: %zu ops, p50 %.3f ms, p99 %.3f ms, max %.3f ms\n",
    (unsigned long)N_LOCKS, ms.count(), all.size(), percentile(all, 50), percentile(all, 99), max);

  EXPECT_LT(max, SANITIZER ? 100.0 : 20.0) << "another thread waited long for the commit";
  EXPECT_EQ(n_fake_pages(), 0u);
}

/** A lock bitmap is sized for the records the page has when the lock is
made, plus a margin of 64. After the page grew, a request of the same
transaction for a record past the end of the bitmap gets a new lock with a
larger bitmap, not a bit past the end of the old one; a record at the last
bit of the old bitmap still goes in the old lock. A waiter for the new
record is granted when the holder commits. */
TEST_F(LockMt, BitmapGrow) {
  FakePage page(1, 10);

  auto a = begin();
  auto b = begin();

  x_enter();

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 5), GRANTED);

  auto first = a->m_trx_locks.back();
  const auto n_bits = first->rec_get_n_bits();

  ASSERT_LT(n_bits, 200u);

  page.m_frame.set_n_heap(300);

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, n_bits - 1), GRANTED);
  EXPECT_EQ(a->m_trx_locks.size(), 1u) << "the last bit of the old bitmap goes in the old lock";
  EXPECT_TRUE(first->rec_is_nth_bit_set(n_bits - 1));

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 250), GRANTED);
  ASSERT_EQ(a->m_trx_locks.size(), 2u) << "a new lock for a record past the old bitmap";

  auto second = a->m_trx_locks.back();

  EXPECT_GT(second->rec_get_n_bits(), 250u);
  EXPECT_TRUE(second->rec_is_nth_bit_set(250));
  EXPECT_FALSE(first->rec_is_nth_bit_set(250));

  x_exit();

  auto result = std::async(std::launch::async, [&] {
    x_enter();
    const auto got = rec_request(b, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 250);
    x_exit();

    return got == WAIT && await(b);
  });

  ASSERT_TRUE(wait_for_lock_wait(b));

  commit(a);

  EXPECT_TRUE(result.get());

  commit(b);
}

/** Threads take table locks on the test table through the API (real
waits), IS, IX, S and X, one or two per transaction, the second an upgrade
of the first (IS to IX, S or X, IX to X, S to X), and commit. Upgrades
deadlock: every request ends granted or as a deadlock victim. The checker
validates the table queue while they run: no two granted locks of
different transactions conflict, a granted lock is never behind a waiting
incompatible lock of another transaction (no barging), every waiting lock
is blocked by a lock ahead. The model checks each grant against the locks
the others hold (noted after the grant, dropped before the commit). */
TEST_F(LockMt, TableStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_ROUNDS = 150;
  const std::array<std::pair<LockMode, LockMode>, 8> plans{{
    {LOCK_IS, LOCK_IS}, {LOCK_IX, LOCK_IX}, {LOCK_IS, LOCK_IX}, {LOCK_IS, LOCK_S},
    {LOCK_IS, LOCK_X}, {LOCK_IX, LOCK_X}, {LOCK_S, LOCK_X}, {LOCK_S, LOCK_S},
  }};

  /* A lock the model says a thread holds: its transaction is round r of thread t. */
  struct TableHeld {
    const Trx *m_trx;
    LockMode m_mode;
    int m_t;
    int m_r;
  };

  /* A grant that conflicts with a lock of round r of thread t. A deadlock
  victim is rolled back inside lock_table() (row_handle_errors()), before
  the model drops its locks: a grant that conflicts only with a victim's
  locks is no error. Protected by the trx sys mutex, as held and victims. */
  struct Violation {
    int m_t;
    int m_r;
    std::string m_what;
  };

  std::vector<TableHeld> held;
  std::vector<Violation> violations;
  std::set<std::pair<int, int>> victims;
  std::atomic<uint64_t> n_req{};
  std::atomic<uint64_t> n_ok{};
  std::atomic<uint64_t> n_victims{};

  Checker checker(this, 100us);
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(100 + t);

      for (int r{}; r < N_ROUNDS; ++r) {
        auto trx = begin();
        const auto plan = plans[rng() % plans.size()];
        bool victim{};

        for (auto mode : {plan.first, plan.second}) {
          ++n_req;

          const auto err = lock_table(trx, CursorLockMode(mode));

          if (err != DB_SUCCESS) {
            EXPECT_EQ(err, DB_DEADLOCK);
            victim = true;
            ++n_victims;

            x_enter();
            victims.emplace(t, r);
            x_exit();

            break;
          }

          ++n_ok;

          x_enter();

          for (const auto &h : held) {
            if (h.m_trx != trx && !table_compatible(mode, h.m_mode)) {
              violations.push_back(Violation{h.m_t, h.m_r,
                std::format("trx {} granted table lock {} while trx {} holds {}", trx->m_id, int(mode), h.m_trx->m_id, int(h.m_mode))});
            }
          }

          held.push_back(TableHeld{trx, mode, t, r});
          x_exit();
        }

        for (int k = int(rng() % 20); k > 0; --k) {
          std::this_thread::yield();
        }

        x_enter();
        std::erase_if(held, [trx](const auto &h) { return h.m_trx == trx; });
        x_exit();

        if (victim) {
          rollback(trx);
        } else {
          commit(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  x_enter();

  for (const auto &v : violations) {
    if (!victims.contains(std::pair{v.m_t, v.m_r})) {
      error(v.m_what);
    }
  }

  x_exit();

  EXPECT_EQ(n_req.load(), n_ok.load() + n_victims.load());
  EXPECT_EQ(n_locks().second, 0u) << "table locks left";

  std::printf("TableStorm: %lu requests, %lu deadlock victims\n", (unsigned long)n_req.load(), (unsigned long)n_victims.load());
}

/** X against the shard latches: 4 threads add to a plain counter per shard
under PageLatch (one page or two, in either order), TableLatch, and a page
then a table latch; 2 threads take X (a reentrant X inside, too), add to all
the counters and check they do not change while they hold it. No deadlock, no
lost add (and TSan sees no race on the plain counters). 1 s. */
TEST_F(LockMt, XMix) {
  constexpr size_t N_PAGES = 64;

  std::array<uint64_t, LockSys::N_PAGE_SHARDS> pages{};
  std::array<uint64_t, LockSys::N_TABLE_SHARDS> tables{};
  std::atomic<uint64_t> n_added{};
  std::atomic<bool> stop{};
  std::vector<std::thread> threads;

  auto page_id = [](uint32_t i) { return PageId{FAKE_SPACE, page_no_t(i % N_PAGES)}; };

  for (int t{}; t < 4; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);

      while (!stop.load(std::memory_order_relaxed)) {
        const auto a = page_id(rng());
        const auto b = page_id(rng());
        const auto sa = LockSys::page_shard_no(a);
        const auto sb = LockSys::page_shard_no(b);

        switch (rng() % 3) {
          case 0: {
            LockSys::PageLatch latch(*m_lock_sys, a, b);

            ++pages[sa];
            ++n_added;

            if (sb != sa) {
              ++pages[sb];
              ++n_added;
            }
            break;
          }
          case 1: {
            LockSys::TableLatch latch(*m_lock_sys, m_table);

            ++tables[LockSys::table_shard_no(m_table)];
            ++n_added;
            break;
          }
          default: {
            LockSys::PageLatch p(*m_lock_sys, a);
            LockSys::TableLatch l(*m_lock_sys, m_table);

            ++pages[sa];
            ++tables[LockSys::table_shard_no(m_table)];
            n_added += 2;
          }
        }
      }
    });
  }

  std::atomic<uint64_t> n_x{};

  for (int t{}; t < 2; ++t) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        x_enter();
        x_enter();

        const auto p0 = pages;
        const auto t0 = tables;

        std::this_thread::yield();

        EXPECT_EQ(pages, p0) << "a page shard latch held under X";
        EXPECT_EQ(tables, t0) << "a table shard latch held under X";

        x_exit();
        x_exit();

        ++n_x;
      }
    });
  }

  std::this_thread::sleep_for(1s);

  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  uint64_t sum{};

  for (auto v : pages) {
    sum += v;
  }

  for (auto v : tables) {
    sum += v;
  }

  EXPECT_EQ(sum, n_added.load()) << "an add lost";
  EXPECT_GT(n_x.load(), 0u);
}

/** The in-memory lock queues under threads, timed. */
class LockMtPerf : public LockMt {};


/** Each thread locks and releases records of a page of its own: no
conflict; the shard latches (different shards) and the slots of the global
latch (S) are all they take. */
TEST_F(LockMtPerf, RecUncontended) {
  for (int n : THREADS) {
    std::vector<std::unique_ptr<FakePage>> pages;
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      pages.push_back(std::make_unique<FakePage>(1 + t));
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_uncontended_t{}", n), n, 20000, [&](int t, uint64_t i) {
      ut::keep(rec_request(trxs[t], pages[t]->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i % 8));
      m_lock_sys->release(trxs[t]);
    });

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e6);
  }
}

/** All the threads lock and release the same record in S: no waits, the
queue of the record holds up to one lock per thread. */
TEST_F(LockMtPerf, RecHotS) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_hot_s_t{}", n), n, 20000, [&](int t, uint64_t) {
      ut::keep(rec_request(trxs[t], page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2));
      m_lock_sys->release(trxs[t]);
    });

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e6);
  }
}

/** All the threads take turns on one record in X: each request waits for
the holder, the release grants the next. Throughput, and the latency of a
request from its enqueue to its grant (p50, p99). */
TEST_F(LockMtPerf, RecHotX) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;
    std::vector<std::vector<double>> lat(n);

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_hot_x_t{}", n), n, n == 1 ? 20000 : 4000, [&](int t, uint64_t) {
      const auto start = Clock::now();

      const auto got = rec_request(trxs[t], page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2);

      if (got == WAIT) {
        (void)await_fast(trxs[t]);
      }

      lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - start).count());

      m_lock_sys->release(trxs[t]);
    });

    std::vector<double> all;

    for (auto &v : lat) {
      all.insert(all.end(), v.begin(), v.end());
    }

    report_ns(std::format("rec_hot_x_grant_p50_t{}", n), n, percentile(all, 50));
    report_ns(std::format("rec_hot_x_grant_p99_t{}", n), n, percentile(all, 99));

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** One transaction holds W records of a page in X (one lock), W
transactions wait each for one of them. Its release grants them all in one
pass over the page queue (rec_grant_pass()), O(W) for the release: the time
per grant does not grow with W. (It was a scan of the queue from its front
for each waiter, O(W^2).) */
TEST_F(LockMtPerf, GrantScan) {
  for (ulint w : {64, 256, 1024, 4096}) {
    FakePage page(1, w + 2);
    auto holder = begin();
    std::vector<Trx *> trxs;

    for (ulint i{}; i < w; ++i) {
      trxs.push_back(begin());
    }

    x_enter();

    for (ulint i{}; i < w; ++i) {
      EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), GRANTED);
    }

    const auto enqueue_start = Clock::now();

    for (ulint i{}; i < w; ++i) {
      EXPECT_EQ(rec_request(trxs[i], page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), WAIT);
    }

    const std::chrono::duration<double> enqueue = Clock::now() - enqueue_start;
    const auto start = Clock::now();

    m_lock_sys->release(holder);

    const std::chrono::duration<double> secs = Clock::now() - start;

    x_exit();

    ut::report(ut::BenchResult{std::format("grant_scan_w{}", w), 1, w, secs.count()});
    ut::report(ut::BenchResult{std::format("enqueue_wait_w{}", w), 1, w, enqueue.count()});

    commit(holder);

    for (auto trx : trxs) {
      EXPECT_EQ(trx->m_wait_lock, nullptr);
      commit(trx);
    }
  }
}

/** The cost of the deadlock check of a request that waits, no deadlock:
(1) behind L transactions holding S on the record; (2) behind one holder
with T idle transactions active, which the check does not visit. */
TEST_F(LockMtPerf, DeadlockCost) {
  FakePage page(1);

  for (int l : {10, 100, 1000}) {
    std::vector<Trx *> trxs;

    for (int i{}; i < l; ++i) {
      trxs.push_back(begin());
    }

    auto req_trx = begin();

    x_enter();

    for (auto trx : trxs) {
      EXPECT_EQ(rec_request(trx, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
    }

    auto req = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT), &page.m_block, 2, m_index, req_trx);

    x_exit();

    /* The full check, with X held for the whole loop; and the latch free
    walk of a request (deadlock_suspect()), which decides if the full check
    must run: behind S holders (several blockers) it must. */
    x_enter();
    auto r = ut::bench(std::format("deadlock_check_queue{}", l), 2000, [&](uint64_t) {
      ut::keep(m_lock_sys->deadlock_occurs(req, req_trx));
    });
    x_exit();

    ut::keep(ut::bench(std::format("deadlock_walk_queue{}", l), 2000, [&](uint64_t) {
      ut::keep(m_lock_sys->deadlock_suspect(req_trx));
    }));

    x_enter();
    m_lock_sys->release(req_trx);
    req_trx->m_wait_lock = nullptr;
    x_exit();

    commit(req_trx);

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }

  for (int n_idle : {10, 100, 1000, 4000}) {
    std::vector<Trx *> idle;

    for (int i{}; i < n_idle; ++i) {
      idle.push_back(begin());
    }

    auto holder = begin();
    auto req_trx = begin();

    x_enter();
    EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), GRANTED);
    auto req = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT), &page.m_block, 2, m_index, req_trx);
    x_exit();

    x_enter();
    auto r = ut::bench(std::format("deadlock_check_idle{}", n_idle), 2000, [&](uint64_t) {
      ut::keep(m_lock_sys->deadlock_occurs(req, req_trx));
    });
    x_exit();

    /* What a request pays: the latch free walk, one edge to a running
    holder, no full check. */
    ut::keep(ut::bench(std::format("deadlock_walk_idle{}", n_idle), 2000, [&](uint64_t) {
      ut::keep(m_lock_sys->deadlock_suspect(req_trx));
    }));

    x_enter();
    m_lock_sys->release(req_trx);
    req_trx->m_wait_lock = nullptr;
    x_exit();

    commit(req_trx);
    commit(holder);

    for (auto trx : idle) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** The hot S record load: the latency of a lock and its release (the page
shard latch of the record shared by all),
p50 and p99, by thread count; and the release of a transaction with 1,000
locks on 1,000 pages: shard by shard, nothing global held (it was one hold
of the trx sys mutex). */
TEST_F(LockMtPerf, MutexHold) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;
    std::vector<std::vector<double>> ops(n);

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    ut::bench_mt(std::format("latch_hot_s_t{}", n), n, 10000, [&](int t, uint64_t) {
      const auto t0 = Clock::now();
      ut::keep(rec_request(trxs[t], page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2));
      m_lock_sys->release(trxs[t]);
      ops[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
    });

    std::vector<double> all;

    for (int t{}; t < n; ++t) {
      all.insert(all.end(), ops[t].begin(), ops[t].end());
    }

    report_ns(std::format("latch_op_p50_t{}", n), n, percentile(all, 50));
    report_ns(std::format("latch_op_p99_t{}", n), n, percentile(all, 99));

    for (auto trx : trxs) {
      commit(trx);
    }
  }

  std::vector<std::unique_ptr<FakePage>> pages;

  for (page_no_t p{}; p < 1000; ++p) {
    pages.push_back(std::make_unique<FakePage>(100 + p));
  }

  auto big = begin();

  for (auto &p : pages) {
    ut::keep(rec_request(big, p->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2));
  }

  const auto start = Clock::now();

  m_lock_sys->release(big);

  const std::chrono::duration<double, std::nano> ns = Clock::now() - start;

  report_ns("release_1000_locks", 1, ns.count());

  commit(big);
}

}  // namespace
