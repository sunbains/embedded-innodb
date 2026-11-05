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

/** @file unit-tests/test_lock_mt_db.cc
LockSys under concurrency, through the API: transactions of a started
instance; the in-memory cases are in test_lock_mt.cc.

  LockMtDb        real waits and wakeups, deadlocks, commits and rollbacks,
                  page splits and purge with gap locks held, isolation
                  levels, lock wait timeouts (1 s, 0 s: precise, the waiter
                  times out by itself), timeouts racing grants, interrupts,
                  implicit locks racing the commit of their owner.
  LockMtDbPerf    the hand-over of a row between threads, timed. */

#include "lock_mt_util.h"

namespace {

/** Transactions of a started instance, through the API. */
class LockMtDb : public SharedDb {
 protected:
  /** The lock wait timeout of the instance, for the life of a Timeout,
  restored after. Set with the trx sys mutex held, when no thread waits for
  a lock: a thread that starts to wait reads it with the mutex. */
  struct Timeout {
    Timeout(LockMtDb *test, std::chrono::seconds timeout) : m_test(test) {
      m_old = set(timeout);
    }

    ~Timeout() { (void)set(m_old); }

    std::chrono::seconds set(std::chrono::seconds timeout) {
      mutex_enter(m_test->m_mutex);
      const auto old = m_test->m_sys->m_config.m_lock.m_wait_timeout;
      m_test->m_sys->m_config.m_lock.m_wait_timeout = timeout;
      mutex_exit(m_test->m_mutex);

      return old;
    }

    LockMtDb *m_test;
    std::chrono::seconds m_old{};
  };

  /** Position crsr on the row with key k. @return DB_SUCCESS or DB_RECORD_NOT_FOUND */
  DbErr seek(api::Cursor *crsr, uint32_t k) {
    auto key = crsr->clust_search_tuple_create();
    int res{-1};
    auto err = key->write_u32(0, k);

    if (err == DB_SUCCESS) {
      err = crsr->moveto(*m_sys, key, IB_CUR_GE, &res);
    }

    api::Tuple::destroy(key);

    if ((err == DB_SUCCESS && res != 0) || err == DB_END_OF_INDEX) {
      err = DB_RECORD_NOT_FOUND;
    }

    return err;
  }

  /** a += 1 in the row k, read and written under an X lock.
  @param[out] old the value of a read */
  DbErr incr(Trx *trx, uint32_t k, uint32_t *old = nullptr) {
    auto &sys = *m_sys;
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IX);

    if (err == DB_SUCCESS) {
      err = crsr->set_lock_mode(sys, IB_LOCK_X);
    }

    if (err == DB_SUCCESS) {
      err = seek(crsr, k);
    }

    if (err == DB_SUCCESS) {
      auto old_tpl = crsr->clust_read_tuple_create();
      auto new_tpl = crsr->clust_read_tuple_create();
      uint32_t a{};

      err = crsr->read_row(sys, old_tpl);

      if (err == DB_SUCCESS) {
        err = old_tpl->read_u32(1, &a);
      }

      if (err == DB_SUCCESS) {
        err = new_tpl->copy(old_tpl);
      }

      if (err == DB_SUCCESS) {
        err = new_tpl->write_u32(1, a + 1);
      }

      if (err == DB_SUCCESS) {
        err = crsr->update_row(sys, old_tpl, new_tpl);
      }

      if (err == DB_SUCCESS && old != nullptr) {
        *old = a;
      }

      api::Tuple::destroy(old_tpl);
      api::Tuple::destroy(new_tpl);
    }

    (void)api::Cursor::close(sys, crsr);

    return err;
  }

  /** A locking read (S) of the keys in [lo, hi]: from lo up to the first
  row after hi, which it locks too. @param[out] keys the keys in [lo, hi] */
  DbErr range(Trx *trx, uint32_t lo, uint32_t hi, std::vector<uint32_t> *keys) {
    auto &sys = *m_sys;
    api::Cursor *crsr{};

    keys->clear();

    if (auto err = api::cursor_open_table(sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IS);

    if (err == DB_SUCCESS) {
      err = crsr->set_lock_mode(sys, IB_LOCK_S);
    }

    if (err == DB_SUCCESS) {
      auto key = crsr->clust_search_tuple_create();
      int res{};

      err = key->write_u32(0, lo);

      if (err == DB_SUCCESS) {
        err = crsr->moveto(sys, key, IB_CUR_GE, &res);
      }

      api::Tuple::destroy(key);
    }

    auto tpl = crsr->clust_read_tuple_create();

    while (err == DB_SUCCESS) {
      uint32_t k{};

      err = crsr->read_row(sys, tpl);

      if (err == DB_SUCCESS) {
        err = tpl->read_u32(0, &k);
      }

      if (err != DB_SUCCESS || k > hi) {
        break;
      }

      keys->push_back(k);
      tpl = tpl->clear();
      err = crsr->next(sys);
    }

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(sys, crsr);

    return err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND ? DB_SUCCESS : err;
  }

  /** A locking read (X) of the row with SEC_A value a through SEC_A, no
  clustered index access: the lock is on the secondary index record.
  @param[out] found true if the row was there
  @return DB_SUCCESS or the error of the lock */
  DbErr sec_x(Trx *trx, uint32_t a, bool *found) {
    auto &sys = *m_sys;
    api::Cursor *crsr{};
    api::Cursor *sec{};

    *found = false;

    if (auto err = api::cursor_open_table(sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IX);

    if (err == DB_SUCCESS) {
      err = crsr->open_index(sys, ut::SEC_A, &sec);
    }

    if (err == DB_SUCCESS) {
      err = sec->set_lock_mode(sys, IB_LOCK_X);

      auto key = sec->sec_search_tuple_create();
      int res{-1};

      if (err == DB_SUCCESS) {
        err = key->write_u32(0, a);
      }

      if (err == DB_SUCCESS) {
        err = sec->moveto(sys, key, IB_CUR_GE, &res);
      }

      api::Tuple::destroy(key);

      if (err == DB_SUCCESS) {
        *found = res == 0;
      } else if (err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND) {
        err = DB_SUCCESS;
      }

      (void)api::Cursor::close(sys, sec);
    }

    (void)api::Cursor::close(sys, crsr);

    return err;
  }

  /** A thread that checks, every period until stopped, that every record
  lock on the test table belongs to a started transaction: a lock left
  behind by a commit (made for it by an implicit to explicit conversion
  that raced the commit) would belong to a transaction that is no longer
  started. */
  struct LeftLocks {
    LeftLocks(LockMtDb *test, std::chrono::microseconds period) : m_thread([this, test, period] {
      while (!m_stop.load(std::memory_order_acquire)) {
        test->x_enter();

        for (auto &shard : test->m_lock_sys->m_page_shards) {
          for (const auto &[page_id, locks] : shard.m_locks) {
            for (auto l : locks) {
              /* The state: with the trx mutex (C4 of a commit). */
              trx_mutex_enter(l->m_trx);
              const TrxStatus state = l->m_trx->m_conc_state;
              trx_mutex_exit(l->m_trx);

              if (l->rec_index()->m_table == test->m_table && state == TRX_NOT_STARTED) {
                ++m_n_left;
              }
            }
          }
        }

        test->x_exit();

        ++m_n;
        std::this_thread::sleep_for(period);
      }
    }) {}

    ~LeftLocks() { stop(); }

    void stop() {
      m_stop.store(true, std::memory_order_release);

      if (m_thread.joinable()) {
        m_thread.join();
      }
    }

    std::atomic<bool> m_stop{};
    std::atomic<uint64_t> m_n{};
    std::atomic<uint64_t> m_n_left{};
    std::thread m_thread;
  };

  /** @return the committed rows. */
  ut::Rows rows() {
    auto trx = begin(IB_TRX_READ_COMMITTED);
    auto rows = scan(trx);

    commit(trx);

    return rows;
  }

  /** @return the a of row k, committed. */
  uint32_t value(uint32_t k) {
    const auto all = rows();
    auto it = all.find(k);

    EXPECT_NE(it, all.end()) << "no row " << k;

    return it == all.end() ? 0 : it->second.m_a;
  }

  /** @return the result of the operation of trx in another thread, which
  must end within timeout: if it does not, trx is interrupted (so that the
  thread ends) and the test fails. */
  template <typename T>
  T get(std::future<T> &result, Trx *trx, std::chrono::milliseconds timeout = 10s) {
    if (result.wait_for(timeout) != std::future_status::ready) {
      ADD_FAILURE() << "the operation of trx " << trx->m_id << " did not end in " << timeout.count() << " ms";
      api::trx_interrupt(trx);
    }

    return result.get();
  }
};

/** Read-modify-write of random rows: 8 threads, each transaction adds 1
to 2 or 3 of 16 rows in random order, deadlocks happen and the victims roll
back. Every committed increment is there at the end and no other: no
update is lost (the X locks exclude each other through the real waits and
wakeups), no deadlock victim leaves a change. No lock is left. */
TEST_F(LockMtDb, UpdateStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_TRXS = 120;
  constexpr uint32_t N_ROWS = 16;

  ut::Rows initial;

  for (uint32_t k = 1; k <= N_ROWS; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  std::vector<std::array<uint32_t, N_ROWS + 1>> counts(N_THREADS);
  std::atomic<int> n_deadlocks{};
  std::atomic<int> n_other{};
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      auto &count = counts[t];

      count.fill(0);

      for (int i{}; i < N_TRXS; ++i) {
        auto trx = begin();
        std::vector<uint32_t> keys;
        DbErr err{DB_SUCCESS};

        for (int n = 2 + int(rng() % 2); n > 0; --n) {
          keys.push_back(1 + rng() % N_ROWS);
        }

        for (auto k : keys) {
          if ((err = incr(trx, k)) != DB_SUCCESS) {
            break;
          }
        }

        if (err == DB_SUCCESS) {
          commit(trx);

          for (auto k : keys) {
            ++count[k];
          }
        } else {
          ++(err == DB_DEADLOCK ? n_deadlocks : n_other);

          if (err != DB_DEADLOCK) {
            ADD_FAILURE() << "incr: " << err;
          }

          rollback(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  const auto all = rows();

  for (uint32_t k = 1; k <= N_ROWS; ++k) {
    uint32_t expected{};

    for (const auto &c : counts) {
      expected += c[k];
    }

    EXPECT_EQ(all.at(k).m_a, expected) << "row " << k;
  }

  EXPECT_EQ(n_other.load(), 0);
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";

  std::printf("UpdateStorm: %d deadlocks\n", n_deadlocks.load());
}

/** One row held by an update, 16 updates of it queued behind in a known
order: when the holder commits they are granted in queue order, one at a
time, each reading the value the one ahead wrote: FIFO, no lost update and
no wakeup lost in the hand-over from each committing thread to the next. */
TEST_F(LockMtDb, HotRowFifo) {
  constexpr int N = 16;

  load({{1, ut::Row{1, 0, ""}}});

  for (int round{}; round < 3; ++round) {
    const auto base = value(1);
    auto holder = begin();

    ASSERT_EQ(incr(holder, 1), DB_SUCCESS);

    std::vector<Trx *> trxs;
    std::vector<uint32_t> seen(N);
    std::vector<DbErr> errs(N, DB_ERROR);
    std::vector<std::thread> threads;

    for (int i{}; i < N; ++i) {
      auto trx = begin();

      trxs.push_back(trx);

      threads.emplace_back([&, i, trx] {
        errs[i] = incr(trx, 1, &seen[i]);

        if (errs[i] == DB_SUCCESS) {
          commit(trx);
        } else {
          rollback(trx);
        }
      });

      EXPECT_TRUE(wait_for_lock_wait(trxs[i])) << i;
    }

    commit(holder);

    for (auto &t : threads) {
      t.join();
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(errs[i], DB_SUCCESS) << i;
      EXPECT_EQ(seen[i], base + 1 + i) << "request " << i << " was not granted in queue order";
    }

    EXPECT_EQ(value(1), base + 1 + N);
  }
}

/** 256 transactions wait for one row at once, each to add 1 to it: when
the holder commits each is granted once and sees a different value, the
row ends 257 higher: 256 threads suspended and woken in turn. */
TEST_F(LockMtDb, ManyWaiters) {
  constexpr int N = 256;

  load({{1, ut::Row{1, 0, ""}}});

  auto holder = begin();

  ASSERT_EQ(incr(holder, 1), DB_SUCCESS);

  std::vector<Trx *> trxs;
  std::vector<uint32_t> seen(N);
  std::vector<DbErr> errs(N, DB_ERROR);
  std::vector<std::thread> threads;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
  }

  for (int i{}; i < N; ++i) {
    threads.emplace_back([&, i] {
      errs[i] = incr(trxs[i], 1, &seen[i]);

      if (errs[i] == DB_SUCCESS) {
        commit(trxs[i]);
      } else {
        rollback(trxs[i]);
      }
    });
  }

  for (int i{}; i < N; ++i) {
    EXPECT_TRUE(wait_for_lock_wait(trxs[i], 30s)) << i;
  }

  commit(holder);

  for (auto &t : threads) {
    t.join();
  }

  std::sort(seen.begin(), seen.end());

  for (int i{}; i < N; ++i) {
    EXPECT_EQ(errs[i], DB_SUCCESS) << i;
    EXPECT_EQ(seen[i], uint32_t(i + 1)) << "two transactions read the same value: a lost update";
  }

  EXPECT_EQ(value(1), uint32_t(N + 1));
}

/** Cycles of 2, 3 and 8 transactions through the API, made at the same
time from their threads: each updates its own row, then the next one's.
Exactly one gets DB_DEADLOCK per cycle and is rolled back; the others get
their rows in turn and commit: all rows but the victim's end up 2 higher
in all, the victim's two changes are gone. */
TEST_F(LockMtDb, DeadlockRounds) {
  ut::Rows initial;

  for (uint32_t k = 1; k <= 8; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  for (auto [n, rounds] : {std::pair{2, 8}, std::pair{3, 5}, std::pair{8, 3}}) {
    for (int r{}; r < rounds; ++r) {
      uint32_t before{};

      for (const auto &[k, row] : rows()) {
        before += row.m_a;
      }

      std::barrier sync(n);
      std::vector<DbErr> errs(n, DB_ERROR);
      std::vector<std::thread> threads;

      for (int i{}; i < n; ++i) {
        threads.emplace_back([&, i] {
          auto trx = begin();
          auto err = incr(trx, 1 + i);

          sync.arrive_and_wait();

          if (err == DB_SUCCESS) {
            err = incr(trx, 1 + (i + 1) % n);
          }

          if (err == DB_SUCCESS) {
            commit(trx);
          } else {
            rollback(trx);
          }

          errs[i] = err;
        });
      }

      for (auto &t : threads) {
        t.join();
      }

      EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_DEADLOCK), 1) << "cycle of " << n;
      EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_SUCCESS), n - 1) << "cycle of " << n;

      uint32_t after{};

      for (const auto &[k, row] : rows()) {
        after += row.m_a;
      }

      EXPECT_EQ(after - before, uint32_t(2 * n - 2)) << "cycle of " << n;
    }
  }

  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** A transaction holds 8 rows, an update waits for each: it rolls back
(round 0) or commits (round 1), every waiter is granted and reads what the
end of the holder left: the old value after the rollback, the holder's
after the commit. */
TEST_F(LockMtDb, ReleaseWakesAll) {
  constexpr int N = 8;
  ut::Rows initial;

  for (uint32_t k = 1; k <= N; ++k) {
    initial[k] = ut::Row{k, 10, ""};
  }

  load(initial);

  for (int round{}; round < 2; ++round) {
    const bool commit_holder = round == 1;
    const auto base = value(1);
    auto holder = begin();

    for (uint32_t k = 1; k <= N; ++k) {
      ASSERT_EQ(incr(holder, k), DB_SUCCESS);
    }

    std::vector<Trx *> trxs;
    std::vector<uint32_t> seen(N);
    std::vector<std::future<DbErr>> results;

    for (int i{}; i < N; ++i) {
      auto trx = begin();

      trxs.push_back(trx);
      results.push_back(std::async(std::launch::async, [&, i, trx] { return incr(trx, 1 + i, &seen[i]); }));
    }

    for (int i{}; i < N; ++i) {
      EXPECT_TRUE(wait_for_lock_wait(trxs[i])) << i;
    }

    if (commit_holder) {
      commit(holder);
    } else {
      rollback(holder);
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS) << i;
      EXPECT_EQ(seen[i], uint32_t(base + (commit_holder ? 1 : 0))) << i;
      commit(trxs[i]);
    }
  }

  for (const auto &[k, row] : rows()) {
    /* Round 0: +1 by the waiter; round 1: +1 by the holder, +1 by the waiter. */
    EXPECT_EQ(row.m_a, 13u) << k;
  }
}

/** A rollback to a savepoint undoes the changes after it but keeps their
locks (api::savepoint_rollback()): an update waiting for a row changed after
the savepoint still waits after the rollback to it, and when the holder
commits it reads the value from before the savepoint. */
TEST_F(LockMtDb, SavepointKeepsLocks) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto t = begin();

  ASSERT_EQ(incr(t, 1), DB_SUCCESS);

  api::savepoint_take(t, "sp", 2);

  ASSERT_EQ(incr(t, 2), DB_SUCCESS);

  auto w = begin();
  uint32_t seen{99};
  auto result = std::async(std::launch::async, [&] { return incr(w, 2, &seen); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  EXPECT_EQ(api::savepoint_rollback(*m_sys, t, "sp", 2), DB_SUCCESS);

  EXPECT_EQ(result.wait_for(300ms), std::future_status::timeout) << "the rollback to the savepoint released the lock";
  EXPECT_TRUE(waiting(w));

  commit(t);

  EXPECT_EQ(get(result, w), DB_SUCCESS);
  EXPECT_EQ(seen, 0u) << "the change after the savepoint was not undone";

  commit(w);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);
}

/** An insert after a savepoint, and an insert of the same key by another
transaction that waits for it (the duplicate check). The rollback to the
savepoint removes the row: the waiter is woken, but the gap where the row
was stays locked by the first transaction (its lock on the row is inherited
by the next one as a gap lock), so the second insert waits until it
commits, then succeeds. One row, no hang. */
TEST_F(LockMtDb, SavepointInsert) {
  load({{40, ut::Row{40, 0, ""}}, {60, ut::Row{60, 0, ""}}});

  auto t = begin();

  ASSERT_EQ(incr(t, 40), DB_SUCCESS);

  api::savepoint_take(t, "sp", 2);

  ASSERT_EQ(insert(t, ut::Row{50, 1, "t"}), DB_SUCCESS);

  auto w = begin();
  auto result = std::async(std::launch::async, [&] { return insert(w, ut::Row{50, 2, "w"}); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  EXPECT_EQ(api::savepoint_rollback(*m_sys, t, "sp", 2), DB_SUCCESS);

  EXPECT_EQ(result.wait_for(300ms), std::future_status::timeout) << "the insert got into the gap the first transaction still locks";

  commit(t);

  EXPECT_EQ(get(result, w), DB_SUCCESS);

  commit(w);

  const auto all = rows();

  ASSERT_TRUE(all.contains(50));
  EXPECT_EQ(all.at(50).m_a, 2u);
  EXPECT_EQ(all.size(), 3u);
}

/** Locking range reads (S) against concurrent inserts and deletes, with
rows big enough that the inserts split pages and the deletes, once purged,
merge them, while the gap locks are held. Each reader reads its range twice
in one transaction. REPEATABLE READ: the two reads are the same, no
phantom (the next-key locks and their inheritance through splits, merges
and purge keep the gaps closed). READ COMMITTED: the rows of the first read
are all in the second (they stay locked), new ones may appear. */
TEST_F(LockMtDb, PhantomStorm) {
  constexpr int N_WRITERS = 4;
  constexpr int N_READERS = 4;
  constexpr uint32_t N_KEYS = 2000;
  constexpr auto DURATION = 2500ms;

  ut::Rows initial;

  for (uint32_t k = 10; k <= N_KEYS; k += 10) {
    initial[k] = ut::Row{k, k, ut::blob(k, 50)};
  }

  load(initial);

  std::atomic<bool> stop{};
  std::atomic<int> n_inserts{};
  std::atomic<int> n_deletes{};
  std::atomic<int> n_reads{};
  std::atomic<int> n_new_rc{};
  std::atomic<int> n_deadlocks{};
  std::vector<std::thread> threads;

  for (int t{}; t < N_WRITERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      std::vector<uint32_t> mine;

      while (!stop.load()) {
        auto trx = begin();
        DbErr err;

        if (!mine.empty() && rng() % 3 == 0) {
          const auto i = rng() % mine.size();

          err = remove(trx, mine[i]);

          if (err == DB_SUCCESS) {
            mine[i] = mine.back();
            mine.pop_back();
            ++n_deletes;
          }
        } else {
          uint32_t k = 1 + rng() % (N_KEYS + 50);

          k += k % 10 == 0 ? 1 : 0;
          err = insert(trx, ut::Row{k, k, ut::blob(k, 300)});

          if (err == DB_SUCCESS) {
            mine.push_back(k);
            ++n_inserts;
          }
        }

        if (err == DB_SUCCESS || err == DB_DUPLICATE_KEY || err == DB_RECORD_NOT_FOUND) {
          commit(trx);
        } else {
          /* The insert or the delete did not happen. */
          EXPECT_EQ(err, DB_DEADLOCK);
          ++n_deadlocks;
          rollback(trx);
        }
      }
    });
  }

  for (int t{}; t < N_READERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(100 + t);
      const auto level = t % 2 == 0 ? IB_TRX_REPEATABLE_READ : IB_TRX_READ_COMMITTED;

      while (!stop.load()) {
        const uint32_t lo = rng() % (N_KEYS + 100);
        const uint32_t hi = lo + 60;
        std::vector<uint32_t> first;
        std::vector<uint32_t> second;
        auto trx = begin(level);

        auto err = range(trx, lo, hi, &first);

        if (err == DB_SUCCESS) {
          std::this_thread::sleep_for(std::chrono::microseconds(rng() % 3000));
          err = range(trx, lo, hi, &second);
        }

        if (err != DB_SUCCESS) {
          EXPECT_EQ(err, DB_DEADLOCK);
          ++n_deadlocks;
          rollback(trx);
          continue;
        }

        commit(trx);
        ++n_reads;

        if (level == IB_TRX_REPEATABLE_READ) {
          EXPECT_EQ(first, second) << "a phantom in [" << lo << ", " << hi << "] under REPEATABLE READ";
        } else {
          EXPECT_TRUE(std::includes(second.begin(), second.end(), first.begin(), first.end()))
            << "a row read and locked under READ COMMITTED disappeared from [" << lo << ", " << hi << "]";
          n_new_rc += second.size() > first.size() ? 1 : 0;
        }
      }
    });
  }

  std::this_thread::sleep_for(DURATION);
  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(n_reads.load(), 0);
  EXPECT_GT(n_inserts.load(), 0);
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";

  std::printf("PhantomStorm: %d inserts, %d deletes, %d range reads, %d deadlocks, %d READ COMMITTED reads saw new rows\n",
    n_inserts.load(), n_deletes.load(), n_reads.load(), n_deadlocks.load(), n_new_rc.load());
}

/** A locking read holds a lock on a delete-marked row, then purge removes
the row: the gap stays locked (the lock is inherited by the next row as a
gap lock), inserts on both sides of the removed row wait until the reader
commits. */
TEST_F(LockMtDb, PurgedGap) {
  load({{10, ut::Row{10, 10, ""}}, {20, ut::Row{20, 20, ""}}, {30, ut::Row{30, 30, ""}}});

  auto d = begin();

  ASSERT_EQ(remove(d, 20), DB_SUCCESS);
  commit(d);

  auto reader = begin(IB_TRX_REPEATABLE_READ);
  std::vector<uint32_t> keys;

  ASSERT_EQ(range(reader, 15, 25, &keys), DB_SUCCESS);
  EXPECT_TRUE(keys.empty());

  purge();

  std::vector<Trx *> trxs;
  std::vector<std::future<DbErr>> results;

  for (uint32_t k : {15u, 25u}) {
    trxs.push_back(begin());
    results.push_back(std::async(std::launch::async, [&, k, trx = trxs.back()] { return insert(trx, ut::Row{k, k, ""}); }));
  }

  for (auto trx : trxs) {
    EXPECT_TRUE(wait_for_lock_wait(trx, 3s)) << "an insert into the gap of a purged row the reader locked did not wait";
  }

  commit(reader);

  for (size_t i{}; i < trxs.size(); ++i) {
    EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS);
    commit(trxs[i]);
  }

  EXPECT_EQ(rows().size(), 4u);
}

/** A locking read of an empty table locks the supremum of the root page
(REPEATABLE READ): an insert waits until the reader commits; under READ
COMMITTED the read locks no gap and the insert does not wait. */
TEST_F(LockMtDb, EmptyTableGap) {
  for (auto level : {IB_TRX_REPEATABLE_READ, IB_TRX_READ_COMMITTED}) {
    auto reader = begin(level);
    std::vector<uint32_t> keys;

    ASSERT_EQ(range(reader, 0, 1000, &keys), DB_SUCCESS);
    EXPECT_TRUE(keys.empty());

    auto writer = begin();
    auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{5, 5, ""}); });

    if (level == IB_TRX_REPEATABLE_READ) {
      EXPECT_TRUE(wait_for_lock_wait(writer, 3s)) << "an insert into an empty table read under REPEATABLE READ did not wait";
      commit(reader);
      EXPECT_EQ(get(result, writer), DB_SUCCESS);
    } else {
      EXPECT_EQ(get(result, writer, 3s), DB_SUCCESS) << "an insert waited for a READ COMMITTED read";
      commit(reader);
    }

    rollback(writer);
    purge();
  }
}

/** Table locks through the API: a holds S, b waits for X, then 8
transactions request IS, which S allows but the waiting X does not: they
all wait behind it (no barging). a commits: b is granted, the IS requests
still wait; b commits: they are all granted. */
TEST_F(LockMtDb, TableNoBarging) {
  constexpr int N = 8;

  auto a = begin();
  auto b = begin();

  ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);

  auto rb = std::async(std::launch::async, [&] { return lock_table(b, IB_LOCK_X); });

  ASSERT_TRUE(wait_for_lock_wait(b));

  std::vector<Trx *> trxs;
  std::vector<std::future<DbErr>> results;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
    results.push_back(std::async(std::launch::async, [&, trx = trxs.back()] { return lock_table(trx, IB_LOCK_IS); }));
  }

  for (auto trx : trxs) {
    EXPECT_TRUE(wait_for_lock_wait(trx)) << "an IS request barged past a waiting X";
  }

  commit(a);

  EXPECT_EQ(get(rb, b), DB_SUCCESS);

  std::this_thread::sleep_for(100ms);

  for (auto &r : results) {
    EXPECT_EQ(r.wait_for(0ms), std::future_status::timeout) << "IS granted while X is held";
  }

  commit(b);

  for (int i{}; i < N; ++i) {
    EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS);
    commit(trxs[i]);
  }
}

/** Two transactions with S on the table both upgrade to X: a deadlock.
Exactly one gets DB_DEADLOCK and is rolled back, the other gets X. */
TEST_F(LockMtDb, TableUpgradeDeadlock) {
  for (int round{}; round < 4; ++round) {
    auto a = begin();
    auto b = begin();

    ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);
    ASSERT_EQ(lock_table(b, IB_LOCK_S), DB_SUCCESS);

    std::barrier sync(2);
    std::array<DbErr, 2> errs{DB_ERROR, DB_ERROR};
    std::array<Trx *, 2> trxs{a, b};
    std::vector<std::thread> threads;

    for (int i{}; i < 2; ++i) {
      threads.emplace_back([&, i] {
        sync.arrive_and_wait();
        errs[i] = lock_table(trxs[i], IB_LOCK_X);

        if (errs[i] == DB_SUCCESS) {
          commit(trxs[i]);
        } else {
          rollback(trxs[i]);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_DEADLOCK), 1);
    EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_SUCCESS), 1);
  }
}

/** A cycle through a table lock and a record lock: a and b each update a
row (IX on the table, X on the row), b asks for S on the table and waits
for a's IX, a asks for b's row. The deadlock search goes from a's record
lock wait to b's table lock wait and back to a: exactly one of them gets
DB_DEADLOCK, the other completes. */
TEST_F(LockMtDb, MixedDeadlock) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  for (int round{}; round < 3; ++round) {
    auto a = begin();
    auto b = begin();

    ASSERT_EQ(incr(a, 1), DB_SUCCESS);
    ASSERT_EQ(incr(b, 2), DB_SUCCESS);

    auto rb = std::async(std::launch::async, [&] { return lock_table(b, IB_LOCK_S); });

    ASSERT_TRUE(wait_for_lock_wait(b));

    auto ra = std::async(std::launch::async, [&] { return incr(a, 2); });

    const auto ea = get(ra, a);
    const auto eb = get(rb, b);

    EXPECT_EQ(int(ea == DB_DEADLOCK) + int(eb == DB_DEADLOCK), 1) << "a: " << ea << " b: " << eb;

    /* The survivor waited for the victim's rollback, then got its lock. */
    EXPECT_TRUE(ea == DB_SUCCESS || eb == DB_SUCCESS);

    if (ea == DB_SUCCESS) {
      rollback(b);
      commit(a);
    } else {
      rollback(a);
      commit(b);
    }
  }

  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** The commit order against the read views: a writer t updates rows 1 and
2 and commits, racing a waiter w that updates row 1 (it waits for t's lock
on it); as soon as w is granted, its thread opens a new snapshot (a new
transaction, REPEATABLE READ or READ COMMITTED: the API opens the read view
of a transaction at its first cursor) and reads row 2: t's commit may be
still releasing its locks then, but t is off the list of active
transactions before its locks are released (C6 before C5, lock0lock.h): the
snapshot must see t's change of row 2. 100 rounds, the commit at a random
moment. */
TEST_F(LockMtDb, CommitVisibility) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  std::mt19937 rng(41);

  for (int round{}; round < 100; ++round) {
    const auto level = round % 2 == 0 ? IB_TRX_REPEATABLE_READ : IB_TRX_READ_COMMITTED;
    const auto before = value(2);
    auto t = begin();

    ASSERT_EQ(incr(t, 1), DB_SUCCESS);
    ASSERT_EQ(incr(t, 2), DB_SUCCESS);

    auto w = begin();
    uint32_t seen2{};

    auto r = std::async(std::launch::async, [&] {
      auto err = incr(w, 1);

      if (err == DB_SUCCESS) {
        auto snapshot = begin(level);

        seen2 = scan(snapshot).at(2).m_a;
        commit(snapshot);
      }

      return err;
    });

    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 2000));

    commit(t);

    ASSERT_EQ(get(r, w), DB_SUCCESS);

    EXPECT_EQ(seen2, before + 1) << "round " << round << (level == IB_TRX_REPEATABLE_READ ? " RR" : " RC")
                                 << ": a waiter granted by the commit did not see its change";

    commit(w);
  }
}

/** A conversion pinned before the C2 of the owner, and a request made after
it: w has an implicit lock on a record; a converter pins w (V1) and stops
before V2; w commits up to C3 (it waits for the pin); b then does not find
w (its V1) and is granted X on the record. The converter's V3 must not
create a lock for w: two granted X locks on the record. */
TEST_F(LockMtDb, ConvertAfterC2) {
  FakePage page(1);
  constexpr ulint heap_no = 2;
  const auto mode = LockMode(LOCK_X | LOCK_REC_NOT_GAP);

  auto w = begin();
  const auto id = w->m_id;
  auto pinned = m_sys->m_trx_sys->pin_active(id);

  ASSERT_EQ(pinned, w);

  std::thread committer([&] { commit(w); });

  /* Until w passed C2: it left the active ids. */
  while (m_sys->m_trx_sys->in_ids(id)) {
    std::this_thread::yield();
  }

  auto b = begin();

  ASSERT_EQ(m_lock_sys->rec_lock_for(b, mode, &page.m_block, heap_no, m_index), DB_SUCCESS);

  {
    LockSys::PageLatch latch(*m_lock_sys, page.id());

    m_lock_sys->impl_to_expl(pinned, &page.m_block, heap_no, m_index);

    EXPECT_EQ(m_lock_sys->rec_has_expl(page.id(), mode, heap_no, pinned), nullptr) << "a lock for a transaction past C2";
    EXPECT_NE(m_lock_sys->rec_has_expl(page.id(), mode, heap_no, b), nullptr);
  }

  TrxSys::unpin(pinned);
  committer.join();
  commit(b);

  x_enter();
  EXPECT_EQ(m_lock_sys->rec_queue(page.id()), nullptr) << "record locks left on the fake page";
  x_exit();
}

/** A stale lookup by a rollback, step by step: w stays active (in the
active ids, as through a rollback); a converter A found w as the owner of
the implicit lock of a record and pinned it (V1); w's change is undone; a
reader b then finds no implicit lock and is granted X on the record; A's V3
must not create a lock for w past b's granted lock. Then the same with b's
lock gone by V3: w gets an explicit X lock, released with its locks. */
TEST_F(LockMtDb, ConvertAfterUndo) {
  FakePage page(1);
  constexpr ulint heap_no = 2;
  const auto mode = LockMode(LOCK_X | LOCK_REC_NOT_GAP);

  auto w = begin();
  auto pinned = m_sys->m_trx_sys->pin_active(w->m_id);

  ASSERT_EQ(pinned, w);

  auto b = begin();

  ASSERT_EQ(m_lock_sys->rec_lock_for(b, mode, &page.m_block, heap_no, m_index), DB_SUCCESS);

  {
    LockSys::PageLatch latch(*m_lock_sys, page.id());

    m_lock_sys->impl_to_expl(pinned, &page.m_block, heap_no, m_index);

    EXPECT_EQ(m_lock_sys->rec_has_expl(page.id(), mode, heap_no, w), nullptr) << "a lock for w past a granted lock of b";
  }

  commit(b);

  {
    LockSys::PageLatch latch(*m_lock_sys, page.id());

    m_lock_sys->impl_to_expl(pinned, &page.m_block, heap_no, m_index);

    EXPECT_NE(m_lock_sys->rec_has_expl(page.id(), mode, heap_no, w), nullptr);
  }

  TrxSys::unpin(pinned);
  rollback(w);

  x_enter();
  EXPECT_EQ(m_lock_sys->rec_queue(page.id()), nullptr) << "record locks left on the fake page";
  x_exit();
}

/** Lookups of active transactions by id (TrxSys::pin_active()) racing their
begins and commits, for TSan and for the pins: 4 threads begin and commit
transactions as fast as they can, publishing their ids; 4 others look the
recent ids up: a transaction found is pinned, so its id is the one looked
up and it is active (its state, read with its mutex) until it is unpinned,
though its commit may run meanwhile (it waits for the pin at C3). 1.5 s. */
TEST_F(LockMtDb, IdLookupRace) {
  constexpr int N = 4;
  constexpr size_t RING = 64;

  std::array<std::atomic<trx_id_t>, RING> ids{};
  std::atomic<uint64_t> next{};
  std::atomic<bool> stop{};
  std::atomic<uint64_t> n_found{};
  std::atomic<uint64_t> n_bad{};
  std::vector<std::thread> threads;

  for (int t{}; t < N; ++t) {
    threads.emplace_back([&] {
      while (!stop.load()) {
        auto trx = begin();

        ids[next++ % RING].store(trx->m_id);

        commit(trx);
      }
    });
  }

  for (int t{}; t < N; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);

      while (!stop.load()) {
        const auto id = ids[rng() % RING].load();

        if (id == 0) {
          continue;
        }

        auto trx = m_sys->m_trx_sys->pin_active(id);

        if (trx != nullptr) {
          ++n_found;

          trx_mutex_enter(trx);
          const bool ok = trx->m_id == id && (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED);
          trx_mutex_exit(trx);

          n_bad += ok ? 0 : 1;

          TrxSys::unpin(trx);
        }
      }
    });
  }

  std::this_thread::sleep_for(1500ms);

  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  std::printf("IdLookupRace: %lu lookups found an active transaction\n", (unsigned long)n_found.load());

  EXPECT_GT(n_found.load(), 0u);
  EXPECT_EQ(n_bad.load(), 0u) << "a pinned transaction was not the one looked up, or not active";
}

/** Two upgraders and an idle reader, through the API: a, b and i read the
row in S (locking reads); a updates it (X): it waits for b and i; b updates
it: a and b are deadlocked whatever i does, and i is idle. One of a and b
gets DB_DEADLOCK at once (well under 1 s, not at a lock wait timeout), the
other waits for i, and is granted when i commits. The same with table
locks: a and b hold S on the table, i holds IS; a and b request X. */
TEST_F(LockMtDb, UpgradeWithIdleReader) {
  load({{1, ut::Row{1, 0, ""}}});

  for (int round{}; round < 4; ++round) {
    const bool table = round % 2 == 1;
    auto a = begin();
    auto b = begin();
    auto i = begin();
    std::vector<uint32_t> keys;

    if (table) {
      ASSERT_EQ(lock_table(i, IB_LOCK_IS), DB_SUCCESS);
      ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);
      ASSERT_EQ(lock_table(b, IB_LOCK_S), DB_SUCCESS);
    } else {
      ASSERT_EQ(range(i, 1, 1, &keys), DB_SUCCESS);
      ASSERT_EQ(range(a, 1, 1, &keys), DB_SUCCESS);
      ASSERT_EQ(range(b, 1, 1, &keys), DB_SUCCESS);
    }

    auto op = [&](Trx *t) { return table ? lock_table(t, IB_LOCK_X) : incr(t, 1); };

    auto ra = std::async(std::launch::async, [&] { return op(a); });

    ASSERT_TRUE(wait_for_lock_wait(a));

    const auto start = Clock::now();
    auto rb = std::async(std::launch::async, [&] { return op(b); });

    /* The deadlock is found at b's request: one of the two ends at once. */
    while (ra.wait_for(0ms) != std::future_status::ready && rb.wait_for(0ms) != std::future_status::ready &&
           Clock::now() - start < 10s) {
      std::this_thread::sleep_for(1ms);
    }

    const std::chrono::duration<double> secs = Clock::now() - start;

    EXPECT_LT(secs.count(), 1.0) << (table ? "table" : "record") << ": the deadlock was not found at once";

    const bool a_done = ra.wait_for(0ms) == std::future_status::ready;
    auto &done = a_done ? ra : rb;
    auto &other = a_done ? rb : ra;
    auto victim = a_done ? a : b;
    auto survivor = a_done ? b : a;

    EXPECT_EQ(done.get(), DB_DEADLOCK);
    rollback(victim);

    EXPECT_EQ(other.wait_for(200ms), std::future_status::timeout) << "the survivor did not wait for the idle reader";

    commit(i);

    EXPECT_EQ(get(other, survivor), DB_SUCCESS);
    commit(survivor);

    std::printf("UpgradeWithIdleReader %s: deadlock found in %.1f ms\n", table ? "table" : "record", secs.count() * 1e3);
  }

  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** A transaction waiting for a row is interrupted (api::trx_interrupt()):
its wait ends with DB_INTERRUPTED at once (the interrupt sets the wait event
of the transaction, there is no lock timeout thread to poll for it: within
500 ms here), its statement is rolled back but not the transaction: it
still holds the row it updated before, a third transaction waits for that.
Then an interrupt races the holder's commit: the waiter either gets the row
or DB_INTERRUPTED without changing it, never both. */
TEST_F(LockMtDb, InterruptWait) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);
  ASSERT_EQ(incr(w, 2), DB_SUCCESS);

  auto result = std::async(std::launch::async, [&] { return incr(w, 1); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  const auto start = Clock::now();

  api::trx_interrupt(w);

  EXPECT_EQ(get(result, w, 5s), DB_INTERRUPTED);
  EXPECT_LT(Clock::now() - start, SLOW * 500ms);

  auto t3 = begin();
  auto r3 = std::async(std::launch::async, [&] { return incr(t3, 2); });

  EXPECT_TRUE(wait_for_lock_wait(t3)) << "the interrupt released the locks of the earlier statements";

  rollback(w);

  EXPECT_EQ(get(r3, t3), DB_SUCCESS);
  commit(t3);
  commit(h);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);

  /* The race. */
  for (int round{}; round < 2; ++round) {
    const auto before = value(1);
    auto h2 = begin();
    auto w2 = begin();

    ASSERT_EQ(incr(h2, 1), DB_SUCCESS);

    auto r = std::async(std::launch::async, [&] { return incr(w2, 1); });

    ASSERT_TRUE(wait_for_lock_wait(w2));

    api::trx_interrupt(w2);

    if (round == 1) {
      std::this_thread::sleep_for(500ms);
    }

    commit(h2);

    const auto err = get(r, w2, 5s);

    commit(w2);

    if (err == DB_SUCCESS) {
      EXPECT_EQ(value(1), before + 2);
    } else {
      EXPECT_EQ(err, DB_INTERRUPTED);
      EXPECT_EQ(value(1), before + 1);
    }
  }
}

/** A wait for a row with a lock wait timeout of 1 s ends with
DB_LOCK_WAIT_TIMEOUT within [1.0 s, 1.2 s) (2.5 s under a sanitizer): the waiter waits for its event
until a steady clock deadline and times out by itself. The whole
transaction is rolled back (rollback on timeout): the row it changed before
is free at once. The holder is unaffected. 3 rounds. */
TEST_F(LockMtDb, TimeoutBounds) {
  const Timeout timeout(this, 1s);

  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);
  ASSERT_EQ(incr(w, 2), DB_SUCCESS);

  /* Two waits by other transactions first: the bounds, round after round. */
  for (int round{}; round < 2; ++round) {
    auto o = begin();
    const auto start = Clock::now();
    const auto err = incr(o, 1);
    const std::chrono::duration<double> secs = Clock::now() - start;

    EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);
    EXPECT_GE(secs.count(), 1.0);
    EXPECT_LT(secs.count(), TIMEOUT_1S_MAX) << "round " << round;

    std::printf("TimeoutBounds: a 1 s timeout fired after %.3f s\n", secs.count());

    rollback(o);
  }

  const auto start = Clock::now();
  const auto err = incr(w, 1);
  const std::chrono::duration<double> secs = Clock::now() - start;

  EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);
  EXPECT_GE(secs.count(), 1.0);
  EXPECT_LT(secs.count(), TIMEOUT_1S_MAX);

  auto t3 = begin();
  auto r3 = std::async(std::launch::async, [&] { return incr(t3, 2); });

  EXPECT_EQ(get(r3, t3, 500ms), DB_SUCCESS) << "the timed out transaction still holds its row";

  commit(t3);
  rollback(w);
  commit(h);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);
}

/** Many short timeouts at once: 32 transactions wait for a row held by
another with a timeout of 1 s, all at the same time; each times out within
[1.0 s, 1.2 s) of its own start, no wait is lost or ends early. */
TEST_F(LockMtDb, TimeoutMany) {
  constexpr int N = 32;
  const Timeout timeout(this, 1s);

  load({{1, ut::Row{1, 0, ""}}});

  auto h = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);

  std::vector<double> secs(N);
  std::vector<DbErr> errs(N, DB_ERROR);
  std::vector<std::thread> threads;

  for (int i{}; i < N; ++i) {
    threads.emplace_back([&, i] {
      auto trx = begin();
      const auto start = Clock::now();

      errs[i] = incr(trx, 1);
      secs[i] = std::chrono::duration<double>(Clock::now() - start).count();
      rollback(trx);
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  commit(h);

  for (int i{}; i < N; ++i) {
    EXPECT_EQ(errs[i], DB_LOCK_WAIT_TIMEOUT) << i;
    EXPECT_GE(secs[i], 1.0) << i;
    EXPECT_LT(secs[i], TIMEOUT_1S_MAX) << i;
  }
}

/** Implicit locks racing the commit of their owner, clustered index: a
writer inserts a row (an implicit X lock only: its trx id is in the
clustered record, no explicit lock), then 4 readers update the row (a
locking read in X that must first convert the writer's implicit lock to an
explicit one, for the writer) while the writer commits after a random
delay. A reader may only get the row after the writer started to commit (no
missed conflict: the implicit lock never disappears while the writer is
active), and every update counts. Meanwhile a checker finds no record lock
of a transaction that is no longer started (no lock left for a committed
writer by a conversion that raced its commit). 150 rounds, a new key each. */
TEST_F(LockMtDb, ImplicitClustRace) {
  constexpr int N_READERS = 4;

  LeftLocks left(this, 200us);
  std::mt19937 rng(11);
  int n_early{};

  for (uint32_t round{}; round < 150; ++round) {
    const uint32_t k = 1 + round;
    auto w = begin();

    ASSERT_EQ(insert(w, ut::Row{k, 0, ""}), DB_SUCCESS);

    std::atomic<bool> committing{};
    std::atomic<int> early{};
    std::vector<std::thread> readers;

    for (int i{}; i < N_READERS; ++i) {
      const auto delay = std::chrono::microseconds(rng() % 1500);

      readers.emplace_back([&, delay] {
        std::this_thread::sleep_for(delay);

        auto r = begin();
        const auto err = incr(r, k);

        if (err == DB_SUCCESS) {
          if (!committing.load(std::memory_order_acquire)) {
            ++early;
          }

          commit(r);
        } else {
          ADD_FAILURE() << "reader: " << err;
          rollback(r);
        }
      });
    }

    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 1500));

    committing.store(true, std::memory_order_release);
    commit(w);

    for (auto &t : readers) {
      t.join();
    }

    n_early += early.load();

    EXPECT_EQ(value(k), uint32_t(N_READERS)) << "round " << round;
  }

  left.stop();

  EXPECT_EQ(n_early, 0) << "a reader got the row of an active writer: a missed conflict";
  EXPECT_EQ(left.m_n_left.load(), 0u) << "a record lock of a transaction that is no longer started";
  EXPECT_GT(left.m_n.load(), 0u);
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** The same race through a secondary index: a writer inserts a row (an
implicit X lock on its secondary index record, found from the page max trx
id and the clustered record, row_vers), 4 readers lock that secondary
record in X while the writer commits after a random delay. A reader that
finds the row may only lock it after the writer started to commit; no lock
is left for a committed writer. 150 rounds, a new key each. */
TEST_F(LockMtDb, ImplicitSecRace) {
  constexpr int N_READERS = 4;

  LeftLocks left(this, 200us);
  std::mt19937 rng(12);
  int n_early{};
  int n_found{};

  for (uint32_t round{}; round < 150; ++round) {
    const uint32_t k = 1000 + round;
    auto w = begin();

    ASSERT_EQ(insert(w, ut::Row{k, k, ""}), DB_SUCCESS);

    std::atomic<bool> committing{};
    std::atomic<int> early{};
    std::atomic<int> found{};
    std::vector<std::thread> readers;

    for (int i{}; i < N_READERS; ++i) {
      const auto delay = std::chrono::microseconds(rng() % 1500);

      readers.emplace_back([&, delay] {
        std::this_thread::sleep_for(delay);

        auto r = begin();
        bool there{};
        const auto err = sec_x(r, k, &there);

        if (err == DB_SUCCESS) {
          if (there) {
            ++found;

            if (!committing.load(std::memory_order_acquire)) {
              ++early;
            }
          }

          commit(r);
        } else {
          /* Readers locking the same record in X can deadlock on the gap of the next one. */
          EXPECT_EQ(err, DB_DEADLOCK);
          rollback(r);
        }
      });
    }

    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 1500));

    committing.store(true, std::memory_order_release);
    commit(w);

    for (auto &t : readers) {
      t.join();
    }

    n_early += early.load();
    n_found += found.load();
  }

  left.stop();

  std::printf("ImplicitSecRace: %d locking reads found the row\n", n_found);

  EXPECT_GT(n_found, 0);
  EXPECT_EQ(n_early, 0) << "a reader locked the secondary record of an active writer: a missed conflict";
  EXPECT_EQ(left.m_n_left.load(), 0u) << "a record lock of a transaction that is no longer started";
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** The holder commits close to the 1 s timeout of the waiter, a little
earlier and later from round to round, so that the grant and the timeout
race: the waiter either gets the row (DB_SUCCESS, its change is there) or
times out (its transaction is rolled back, its change is not there, it holds
no lock), never both. Exactly one of them wins, decided with the trx sys
mutex held: a grant moves the query thread out of the lock wait state, a
timeout cancels the request first. Far from the deadline the outcome is
certain: granted at 300 ms, timed out at 1500 ms (a commit takes time, on a
loaded host 100 ms and more: the margins are wide). */
TEST_F(LockMtDb, TimeoutGrantRace) {
  const Timeout timeout(this, 1s);

  load({{1, ut::Row{1, 0, ""}}});

  int n_granted{};
  int n_timeouts{};

  for (auto delay : {300ms, 995ms, 1000ms, 1005ms, 1500ms}) {
    const auto before = value(1);
    auto h = begin();
    auto w = begin();

    ASSERT_EQ(incr(h, 1), DB_SUCCESS);

    auto r = std::async(std::launch::async, [&] { return incr(w, 1); });

    ASSERT_TRUE(wait_for_lock_wait(w));

    std::this_thread::sleep_for(delay);

    commit(h);

    const auto err = get(r, w, 5s);

    if (delay == 300ms) {
      EXPECT_EQ(err, DB_SUCCESS) << "a commit 700 ms before the deadline did not grant";
    } else if (delay == 1500ms) {
      EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT) << "a commit 500 ms after the deadline was granted";
    }

    if (err == DB_SUCCESS) {
      ++n_granted;
      commit(w);
      EXPECT_EQ(value(1), before + 2);
    } else {
      ++n_timeouts;
      EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);

      trx_mutex_enter(w);
      EXPECT_TRUE(w->m_trx_locks.empty()) << "a timed out transaction holds locks";
      trx_mutex_exit(w);

      rollback(w);
      EXPECT_EQ(value(1), before + 1);
    }
  }

  std::printf("TimeoutGrantRace: %d granted, %d timed out\n", n_granted, n_timeouts);
}

/** A timeout of 0 s is not "wait forever": a wait ends with
DB_LOCK_WAIT_TIMEOUT at once (its deadline is when it starts), within
200 ms with the statement around it. */
TEST_F(LockMtDb, Timeout0) {
  const Timeout timeout(this, 0s);

  load({{1, ut::Row{1, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);

  const auto start = Clock::now();
  auto r = std::async(std::launch::async, [&] { return incr(w, 1); });

  EXPECT_EQ(get(r, w, 5s), DB_LOCK_WAIT_TIMEOUT);
  EXPECT_LT(Clock::now() - start, SLOW * 200ms);

  std::printf("Timeout0: a 0 s timeout fired after %.3f s\n", std::chrono::duration<double>(Clock::now() - start).count());

  rollback(w);
  commit(h);
}

/** Transactions of a started instance, timed. */
class LockMtDbPerf : public LockMtDb {};

/** Each thread runs transactions that take an intention lock on the test
table, IS or IX, through the API, and commit: compatible, the table queue
holds up to one lock per thread. The same without the lock, as the base. */
TEST_F(LockMtDbPerf, TableIntent) {
  for (int n : THREADS) {
    ut::bench_mt(std::format("trx_begin_commit_t{}", n), n, 2000, [&](int, uint64_t) {
      auto trx = begin();
      commit(trx);
    });

    auto r = ut::bench_mt(std::format("table_is_ix_trx_t{}", n), n, 2000, [&](int t, uint64_t) {
      auto trx = begin();
      EXPECT_EQ(lock_table(trx, t % 2 == 0 ? IB_LOCK_IS : IB_LOCK_IX), DB_SUCCESS);
      commit(trx);
    });

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** P pairs of threads, each pair handing a row of its own over: the
holder commits while the other waits for the row; the time from the start
of the commit to the return of the waiter's update, by number of pairs
running at once (the wakeup through the server slot event, the trx sys
mutex shared by all). */
TEST_F(LockMtDbPerf, HandoffPairs) {
  constexpr int N = 40;

  ut::Rows initial;

  for (uint32_t k = 1; k <= 8; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  for (int pairs : {1, 2, 4, 8}) {
    std::vector<std::vector<double>> lat(pairs);
    std::vector<std::thread> threads;

    for (int p{}; p < pairs; ++p) {
      threads.emplace_back([&, p] {
        const uint32_t k = 1 + p;

        for (int i{}; i < N; ++i) {
          auto holder = begin();

          if (incr(holder, k) != DB_SUCCESS) {
            ADD_FAILURE() << "incr";
            rollback(holder);
            return;
          }

          auto waiter = begin();
          Clock::time_point granted;

          auto result = std::async(std::launch::async, [&] {
            auto err = incr(waiter, k);
            granted = Clock::now();
            return err;
          });

          EXPECT_TRUE(wait_for_lock_wait(waiter));

          const auto start = Clock::now();

          commit(holder);

          EXPECT_EQ(get(result, waiter), DB_SUCCESS);
          lat[p].push_back(std::chrono::duration<double, std::nano>(granted - start).count());
          commit(waiter);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    std::vector<double> all;

    for (auto &v : lat) {
      all.insert(all.end(), v.begin(), v.end());
    }

    report_ns(std::format("api_handoff_p50_pairs{}", pairs), pairs, percentile(all, 50));
    report_ns(std::format("api_handoff_p99_pairs{}", pairs), pairs, percentile(all, 99));
  }
}

}  // namespace
