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

/** @file unit-tests/test_sync.cc
The synchronization primitives: the spin-then-wait Mutex, the RwLock with
recursive x-locks and writer priority, and the CondVar event. There is no
SX mode. */

#include "ut0env.h"
#include "ut0perf.h"

#include "os0sync.h"
#include "sync0rw.h"
#include "sync0sync.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

/** A Mutex created and freed with the test. */
struct MutexTest : public ::testing::Test {
  void SetUp() override { mutex_create(&m_mutex, IF_DEBUG("test_mutex", ) Current_location()); }

  void TearDown() override { mutex_free(&m_mutex); }

  Mutex m_mutex;
};

/** Run fn in another thread and wait for it. */
template <typename F>
void in_thread(F &&fn) {
  std::thread t(std::forward<F>(fn));

  t.join();
}

/** Wait, up to 10 s, for pred to hold. @return pred() */
template <typename P>
bool eventually(P &&pred) {
  const auto end = std::chrono::steady_clock::now() + 10s;

  while (!pred()) {
    if (std::chrono::steady_clock::now() > end) {
      return false;
    }

    std::this_thread::sleep_for(100us);
  }

  return true;
}

/** A new mutex is free and has no waiters. */
TEST_F(MutexTest, Created) {
  EXPECT_EQ(mutex_get_lock_word(&m_mutex), 0);
  EXPECT_EQ(mutex_get_waiters(&m_mutex), 0UL);
  EXPECT_NE(m_mutex.event, nullptr);
}

/** enter/exit set and clear the lock word and the owner. */
TEST_F(MutexTest, EnterExit) {
  mutex_enter(&m_mutex);

  EXPECT_EQ(mutex_get_lock_word(&m_mutex), 1);
#ifdef UNIV_DEBUG
  EXPECT_TRUE(mutex_own(&m_mutex));
#endif /* UNIV_DEBUG */

  mutex_exit(&m_mutex);

  EXPECT_EQ(mutex_get_lock_word(&m_mutex), 0);
#ifdef UNIV_DEBUG
  EXPECT_FALSE(mutex_own(&m_mutex));
#endif /* UNIV_DEBUG */
}

/** nowait returns 0 and takes a free mutex, 1 and leaves a taken one. */
TEST_F(MutexTest, Nowait) {
  EXPECT_EQ(mutex_enter_nowait(&m_mutex), 0UL);

  in_thread([&] { EXPECT_EQ(mutex_enter_nowait(&m_mutex), 1UL); });

  mutex_exit(&m_mutex);

  in_thread([&] {
    EXPECT_EQ(mutex_enter_nowait(&m_mutex), 0UL);
    mutex_exit(&m_mutex);
  });
}

#ifdef UNIV_DEBUG
/** Another thread does not own a mutex this thread holds. */
TEST_F(MutexTest, OwnPerThread) {
  mutex_enter(&m_mutex);

  in_thread([&] { EXPECT_FALSE(mutex_own(&m_mutex)); });

  mutex_exit(&m_mutex);
}

/** Entering a mutex the thread holds is a programming error. */
TEST_F(MutexTest, RecursiveEnterAsserts) {
  EXPECT_DEATH(
    {
      mutex_enter(&m_mutex);
      mutex_enter(&m_mutex);
    },
    ""
  );
}
#endif /* UNIV_DEBUG */

/** A thread blocked on the mutex goes on when it is released, and the
waiters flag is clear again at the end. */
TEST_F(MutexTest, Blocks) {
  std::atomic<bool> entered{};

  mutex_enter(&m_mutex);

  std::thread t([&] {
    mutex_enter(&m_mutex);
    entered = true;
    mutex_exit(&m_mutex);
  });

  /* Long enough to spin out and sleep on the event. */
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(entered);

  mutex_exit(&m_mutex);
  t.join();

  EXPECT_TRUE(entered);
  EXPECT_EQ(mutex_get_waiters(&m_mutex), 0UL);
}

/** Many threads incrementing a plain counter under the mutex lose nothing;
only one thread is ever inside. */
TEST_F(MutexTest, Counter) {
  constexpr int THREADS = 16;
  constexpr int N = 20000;
  uint64_t counter{};
  std::atomic<int> inside{};
  std::atomic<int> max_inside{};
  std::vector<std::thread> threads;

  for (int t = 0; t < THREADS; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < N; ++i) {
        mutex_enter(&m_mutex);

        const auto n = ++inside;

        if (n > max_inside) {
          max_inside = n;
        }

        ++counter;
        --inside;

        mutex_exit(&m_mutex);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(counter, uint64_t(THREADS) * N);
  EXPECT_EQ(max_inside, 1);
  EXPECT_EQ(mutex_get_lock_word(&m_mutex), 0);
  EXPECT_EQ(mutex_get_waiters(&m_mutex), 0UL);
}

/** Many mutexes with threads over them in different orders (always
ascending, so no deadlock): every counter is exact. */
TEST(MutexMany, Counters) {
  constexpr int M = 8;
  std::array<Mutex, M> mutexes;
  std::array<uint64_t, M> counters{};

  for (auto &m : mutexes) {
    mutex_create(&m, IF_DEBUG("test_mutex", ) Current_location());
  }

  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 10000; ++i) {
        const int a = (t + i) % M;
        const int b = (a + 1 + i % (M - 1)) % M;
        const int lo = std::min(a, b);
        const int hi = std::max(a, b);

        mutex_enter(&mutexes[lo]);
        mutex_enter(&mutexes[hi]);
        ++counters[lo];
        ++counters[hi];
        mutex_exit(&mutexes[hi]);
        mutex_exit(&mutexes[lo]);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  uint64_t sum{};

  for (int i = 0; i < M; ++i) {
    sum += counters[i];
    mutex_free(&mutexes[i]);
  }

  EXPECT_EQ(sum, 8ULL * 10000 * 2);
}

/** An RwLock created and freed with the test. */
struct RwLockTest : public ::testing::Test {
  void SetUp() override { rw_lock_create(&m_lock, SYNC_NO_ORDER_CHECK); }

  void TearDown() override { rw_lock_free(&m_lock); }

  RwLock m_lock;
};

/** A new lock is free: lock word X_LOCK_DECR, no readers, no writer. */
TEST_F(RwLockTest, Created) {
  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
  EXPECT_EQ(rw_lock_get_writer(&m_lock), RW_LOCK_NOT_LOCKED);
  EXPECT_EQ(rw_lock_get_reader_count(&m_lock), 0UL);
  EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), 0UL);
  EXPECT_FALSE(rw_lock_is_locked(&m_lock, RW_LOCK_SHARED));
  EXPECT_FALSE(rw_lock_is_locked(&m_lock, RW_LOCK_EX));
  EXPECT_FALSE(rw_lock_get_waiters(&m_lock));
}

/** Each s-lock takes one from the lock word; readers are counted. */
TEST_F(RwLockTest, SharedCount) {
  for (int i = 1; i <= 5; ++i) {
    rw_lock_s_lock(&m_lock);
    EXPECT_EQ(rw_lock_get_reader_count(&m_lock), ulint(i));
  }

  EXPECT_TRUE(rw_lock_is_locked(&m_lock, RW_LOCK_SHARED));
  EXPECT_FALSE(rw_lock_is_locked(&m_lock, RW_LOCK_EX));
  EXPECT_EQ(rw_lock_get_writer(&m_lock), RW_LOCK_NOT_LOCKED);

  for (int i = 5; i >= 1; --i) {
    rw_lock_s_unlock(&m_lock);
    EXPECT_EQ(rw_lock_get_reader_count(&m_lock), ulint(i - 1));
  }

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** Readers in other threads share the lock. */
TEST_F(RwLockTest, SharedThreads) {
  rw_lock_s_lock(&m_lock);

  in_thread([&] {
    EXPECT_TRUE(rw_lock_s_lock_nowait(&m_lock, __FILE__, __LINE__));
    EXPECT_EQ(rw_lock_get_reader_count(&m_lock), 2UL);
    rw_lock_s_unlock(&m_lock);
  });

  rw_lock_s_unlock(&m_lock);
}

/** An x-lock excludes readers and other writers. */
TEST_F(RwLockTest, ExclusiveExcludes) {
  rw_lock_x_lock(&m_lock);

  EXPECT_EQ(m_lock.m_lock_word, 0);
  EXPECT_EQ(rw_lock_get_writer(&m_lock), RW_LOCK_EX);
  EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), 1UL);
  EXPECT_TRUE(rw_lock_is_locked(&m_lock, RW_LOCK_EX));

  in_thread([&] {
    EXPECT_FALSE(rw_lock_s_lock_nowait(&m_lock, __FILE__, __LINE__));
    EXPECT_FALSE(rw_lock_x_lock_nowait(&m_lock));
  });

  rw_lock_x_unlock(&m_lock);

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
  EXPECT_FALSE(m_lock.m_recursive);
}

/** A reader excludes a writer that does not wait. */
TEST_F(RwLockTest, SharedExcludesNowaitWriter) {
  rw_lock_s_lock(&m_lock);

  in_thread([&] { EXPECT_FALSE(rw_lock_x_lock_nowait(&m_lock)); });

  rw_lock_s_unlock(&m_lock);

  in_thread([&] {
    EXPECT_TRUE(rw_lock_x_lock_nowait(&m_lock));
    rw_lock_x_unlock(&m_lock);
  });
}

/** The x-lock holder can x-lock again: each lock takes X_LOCK_DECR. */
TEST_F(RwLockTest, ExclusiveRecursion) {
  for (ulint i = 1; i <= 4; ++i) {
    rw_lock_x_lock(&m_lock);
    EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), i);
    EXPECT_EQ(m_lock.m_lock_word, -int32_t(i - 1) * X_LOCK_DECR);
  }

  for (ulint i = 4; i >= 1; --i) {
    EXPECT_TRUE(m_lock.m_recursive);
    rw_lock_x_unlock(&m_lock);
    EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), i - 1);
  }

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
  EXPECT_FALSE(m_lock.m_recursive);
}

/** One recursive x-lock with nowait after a blocking x-lock. */
TEST_F(RwLockTest, NowaitRecursionOnce) {
  rw_lock_x_lock(&m_lock);
  EXPECT_TRUE(rw_lock_x_lock_nowait(&m_lock));
  EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), 2UL);

  rw_lock_x_unlock(&m_lock);
  rw_lock_x_unlock(&m_lock);

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/* Regression: the third recursive x-lock taken with rw_lock_x_lock_func_nowait()
corrupts the lock word. sync0rw.h:491, the relock branch:
    if (lock->m_lock_word == 0) {
      lock->m_lock_word -= X_LOCK_DECR;
    } else if (lock->m_lock_word <= -X_LOCK_DECR) {
      --lock->m_lock_word;
    }
The second branch takes 1 instead of X_LOCK_DECR: the lock word becomes
-X_LOCK_DECR - 1, a state that means "readers with a waiting writer" mixed
with recursion. rw_lock_validate() fails (ut_a: lock_word > -X_LOCK_DECR ||
(-lock_word) % X_LOCK_DECR == 0) in the debug build; in a release build
rw_lock_get_x_lock_count() returns 0 and the x-unlocks never bring the lock
word back to X_LOCK_DECR: the lock stays taken forever. Upstream (7c21eac0
sync0rw.ic) always did lock_word -= X_LOCK_DECR. The buffer pool uses this
function (buf0buf.cc) on blocks the thread may already hold. */
TEST_F(RwLockTest, NowaitRecursionDeep) {
  rw_lock_x_lock(&m_lock);

  for (ulint i = 2; i <= 4; ++i) {
    ASSERT_TRUE(rw_lock_x_lock_nowait(&m_lock));
    EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), i);
  }

  for (ulint i = 0; i < 4; ++i) {
    rw_lock_x_unlock(&m_lock);
  }

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** An x-lock taken with pass != 0 is not recursive: it may be released by
another thread (an i/o completion), and its holder can't relock it. */
TEST_F(RwLockTest, Pass) {
  rw_lock_x_lock_gen(&m_lock, 1);

  EXPECT_FALSE(m_lock.m_recursive);
  EXPECT_FALSE(rw_lock_x_lock_nowait(&m_lock));

  in_thread([&] { rw_lock_x_unlock_gen(&m_lock, 1); });

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** move_ownership() lets the thread that takes over a passed x-lock relock it. */
TEST_F(RwLockTest, MoveOwnership) {
  rw_lock_x_lock_gen(&m_lock, 1);

  in_thread([&] {
    EXPECT_FALSE(rw_lock_x_lock_nowait(&m_lock));

    rw_lock_x_lock_move_ownership(&m_lock);

    EXPECT_TRUE(m_lock.m_recursive);
    EXPECT_TRUE(rw_lock_x_lock_nowait(&m_lock));
    EXPECT_EQ(rw_lock_get_x_lock_count(&m_lock), 2UL);

    rw_lock_x_unlock(&m_lock);
    rw_lock_x_unlock(&m_lock);
  });

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** A writer waiting for readers reserves the lock (WAIT_EX): new readers
are kept out, the writer gets the lock when the last reader leaves. */
TEST_F(RwLockTest, WriterPriority) {
  std::atomic<bool> got_x{};

  rw_lock_s_lock(&m_lock);

  std::thread writer([&] {
    rw_lock_x_lock(&m_lock);
    got_x = true;
    std::this_thread::sleep_for(20ms);
    rw_lock_x_unlock(&m_lock);
  });

  ASSERT_TRUE(eventually([&] { return rw_lock_get_writer(&m_lock) == RW_LOCK_WAIT_EX; }));

  EXPECT_EQ(rw_lock_get_reader_count(&m_lock), 1UL);
  EXPECT_FALSE(got_x);

  /* A new reader is refused while the writer waits. */
  in_thread([&] { EXPECT_FALSE(rw_lock_s_lock_nowait(&m_lock, __FILE__, __LINE__)); });

  rw_lock_s_unlock(&m_lock);

  ASSERT_TRUE(eventually([&] { return got_x.load(); }));

  /* A blocking reader waits for the writer. */
  rw_lock_s_lock(&m_lock);
  EXPECT_EQ(rw_lock_get_writer(&m_lock), RW_LOCK_NOT_LOCKED);
  rw_lock_s_unlock(&m_lock);

  writer.join();

  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** Readers blocked behind a writer all get in when it leaves. */
TEST_F(RwLockTest, ReadersWakeAfterWriter) {
  std::atomic<int> readers{};
  std::vector<std::thread> threads;

  rw_lock_x_lock(&m_lock);

  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&] {
      rw_lock_s_lock(&m_lock);
      ++readers;
      rw_lock_s_unlock(&m_lock);
    });
  }

  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(readers, 0);

  rw_lock_x_unlock(&m_lock);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(readers, 8);
  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** Readers and writers hammering the lock: writers update two plain
counters that readers check are equal; nobody reads while a writer is in. */
TEST_F(RwLockTest, Stress) {
  uint64_t a{};
  uint64_t b{};
  std::atomic<int> writers_in{};
  std::atomic<int> readers_in{};
  std::atomic<bool> bad{};
  std::vector<std::thread> threads;

  constexpr int WRITERS = 4;
  constexpr int READERS = 12;
  constexpr int N = 5000;

  for (int t = 0; t < WRITERS; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < N; ++i) {
        rw_lock_x_lock(&m_lock);

        if (++writers_in != 1 || readers_in != 0) {
          bad = true;
        }

        ++a;
        ++b;

        --writers_in;
        rw_lock_x_unlock(&m_lock);
      }
    });
  }

  for (int t = 0; t < READERS; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < N; ++i) {
        rw_lock_s_lock(&m_lock);
        ++readers_in;

        if (writers_in != 0 || a != b) {
          bad = true;
        }

        --readers_in;
        rw_lock_s_unlock(&m_lock);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_FALSE(bad);
  EXPECT_EQ(a, uint64_t(WRITERS) * N);
  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

/** Writers that x-lock recursively under contention. */
TEST_F(RwLockTest, StressRecursive) {
  uint64_t counter{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 5000; ++i) {
        rw_lock_x_lock(&m_lock);
        rw_lock_x_lock(&m_lock);
        ++counter;
        rw_lock_x_unlock(&m_lock);
        ++counter;
        rw_lock_x_unlock(&m_lock);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(counter, 8ULL * 5000 * 2);
  EXPECT_EQ(m_lock.m_lock_word, X_LOCK_DECR);
}

#ifdef UNIV_DEBUG
/** Freeing a taken lock is caught. */
TEST_F(RwLockTest, FreeLockedAsserts) {
  EXPECT_DEATH(
    {
      rw_lock_x_lock(&m_lock);
      rw_lock_free(&m_lock);
    },
    ""
  );
}
#endif /* UNIV_DEBUG */

/** A CondVar created and destroyed with the test. */
struct EventTest : public ::testing::Test {
  void SetUp() override { m_event = os_event_create(nullptr); }

  void TearDown() override { os_event_free(m_event); }

  CondVar *m_event{};
};

/** A new event is not set; set() sets it, reset() clears it. */
TEST_F(EventTest, SetReset) {
  EXPECT_FALSE(m_event->m_is_set);

  const auto c0 = os_event_reset(m_event);

  os_event_set(m_event);
  EXPECT_TRUE(m_event->m_is_set);

  const auto c1 = os_event_reset(m_event);

  EXPECT_FALSE(m_event->m_is_set);
  EXPECT_EQ(c1, c0 + 1);
}

/** set() of a set event does not count again. */
TEST_F(EventTest, SetTwice) {
  const auto c0 = os_event_reset(m_event);

  os_event_set(m_event);
  os_event_set(m_event);

  EXPECT_EQ(os_event_reset(m_event), c0 + 1);
}

/** wait() on a set event returns at once. */
TEST_F(EventTest, WaitSet) {
  os_event_set(m_event);
  os_event_wait(m_event);
  os_event_wait_low(m_event, 0);

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(1000), 0), 0UL);
}

/** wait_time() on an event that is not set times out after the timeout. */
TEST_F(EventTest, WaitTimeout) {
  const auto start = std::chrono::steady_clock::now();

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(20000), 0), OS_SYNC_TIME_EXCEEDED);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
}

/** A set() in another thread wakes a waiter. */
TEST_F(EventTest, Wake) {
  std::atomic<bool> woke{};
  const auto sig = os_event_reset(m_event);

  std::thread t([&] {
    os_event_wait_low(m_event, sig);
    woke = true;
  });

  std::this_thread::sleep_for(20ms);
  EXPECT_FALSE(woke);

  os_event_set(m_event);
  t.join();

  EXPECT_TRUE(woke);
}

/** set() wakes all the waiters (manual reset event). */
TEST_F(EventTest, WakeAll) {
  std::atomic<int> woke{};
  const auto sig = os_event_reset(m_event);
  std::vector<std::thread> threads;

  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&] {
      os_event_wait_low(m_event, sig);
      ++woke;
    });
  }

  std::this_thread::sleep_for(20ms);
  os_event_set(m_event);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(woke, 8);
}

/** A set() between the reset() and the wait() is not lost, even if another
reset() clears the event in between: the wait passes the count from its
reset() (LEMMA 2 in sync0sync.cc). */
TEST_F(EventTest, NoLostSignal) {
  const auto sig = os_event_reset(m_event);

  os_event_set(m_event);
  (void)os_event_reset(m_event);

  /* Returns at once, the signal count moved. */
  os_event_wait_low(m_event, sig);

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(1000), sig), 0UL);
}

/** wait_time() with a signal count that is current waits for a new set(). */
TEST_F(EventTest, WaitTimeCurrentCount) {
  const auto sig = os_event_reset(m_event);

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(5000), sig), OS_SYNC_TIME_EXCEEDED);

  std::thread t([&] {
    std::this_thread::sleep_for(10ms);
    os_event_set(m_event);
  });

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(10000000), sig), 0UL);
  t.join();
}

/** The OS mutex serializes threads. */
TEST(OsMutexTest, Counter) {
  auto m = os_mutex_create(nullptr);
  uint64_t counter{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 10000; ++i) {
        os_mutex_enter(m);
        ++counter;
        os_mutex_exit(m);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(counter, 80000ULL);
  os_mutex_destroy(m);
}

}  // namespace

namespace {

using MutexPerf = MutexTest;

/** Uncontended enter/exit. */
TEST_F(MutexPerf, Uncontended) {
  auto r = ut::bench("mutex_enter_exit", 20000000, [&](uint64_t) {
    mutex_enter(&m_mutex);
    mutex_exit(&m_mutex);
  });

  EXPECT_LT(r.ns_per_op(), 1000.0);
}

/** Uncontended nowait/exit. */
TEST_F(MutexPerf, Nowait) {
  auto r = ut::bench("mutex_nowait_exit", 20000000, [&](uint64_t) {
    (void)mutex_enter_nowait(&m_mutex);
    mutex_exit(&m_mutex);
  });

  EXPECT_LT(r.ns_per_op(), 1000.0);
}

/** 1 to 16 threads on one mutex, a short critical section. */
TEST_F(MutexPerf, Contended) {
  for (int n : {1, 2, 4, 8, 16}) {
    uint64_t counter{};

    auto r = ut::bench_mt(std::format("mutex_contended_t{}", n), n, 200000, [&](int, uint64_t) {
      mutex_enter(&m_mutex);
      ++counter;
      mutex_exit(&m_mutex);
    });

    EXPECT_EQ(counter, 200000ULL * n);
    EXPECT_LT(r.ns_per_op(), 100000.0);
  }
}

using RwLockPerf = RwLockTest;

/** Uncontended s and x lock/unlock. */
TEST_F(RwLockPerf, Uncontended) {
  auto s = ut::bench("rw_s_lock_unlock", 20000000, [&](uint64_t) {
    rw_lock_s_lock(&m_lock);
    rw_lock_s_unlock(&m_lock);
  });

  auto x = ut::bench("rw_x_lock_unlock", 20000000, [&](uint64_t) {
    rw_lock_x_lock(&m_lock);
    rw_lock_x_unlock(&m_lock);
  });

  EXPECT_LT(s.ns_per_op(), 1000.0);
  EXPECT_LT(x.ns_per_op(), 1000.0);
}

/** 1 to 16 readers: s-locks share, but they all write the lock word. */
TEST_F(RwLockPerf, SharedContended) {
  for (int n : {1, 2, 4, 8, 16}) {
    auto r = ut::bench_mt(std::format("rw_s_t{}", n), n, 200000, [&](int, uint64_t) {
      rw_lock_s_lock(&m_lock);
      rw_lock_s_unlock(&m_lock);
    });

    EXPECT_LT(r.ns_per_op(), 100000.0);
  }
}

/** 1 to 16 writers. */
TEST_F(RwLockPerf, ExclusiveContended) {
  for (int n : {1, 2, 4, 8, 16}) {
    uint64_t counter{};

    auto r = ut::bench_mt(std::format("rw_x_t{}", n), n, 100000, [&](int, uint64_t) {
      rw_lock_x_lock(&m_lock);
      ++counter;
      rw_lock_x_unlock(&m_lock);
    });

    EXPECT_EQ(counter, 100000ULL * n);
    EXPECT_LT(r.ns_per_op(), 200000.0);
  }
}

/** 90% readers, 10% writers over 1 to 16 threads. */
TEST_F(RwLockPerf, Mixed) {
  for (int n : {1, 2, 4, 8, 16}) {
    uint64_t counter{};

    auto r = ut::bench_mt(std::format("rw_mixed_90_10_t{}", n), n, 100000, [&](int, uint64_t i) {
      if (i % 10 == 0) {
        rw_lock_x_lock(&m_lock);
        ++counter;
        rw_lock_x_unlock(&m_lock);
      } else {
        rw_lock_s_lock(&m_lock);
        ut::keep(counter);
        rw_lock_s_unlock(&m_lock);
      }
    });

    EXPECT_LT(r.ns_per_op(), 200000.0);
  }
}

using EventPerf = EventTest;

/** set + reset of an event nobody waits on. */
TEST_F(EventPerf, SetReset) {
  auto r = ut::bench("event_set_reset", 5000000, [&](uint64_t) {
    os_event_set(m_event);
    (void)os_event_reset(m_event);
  });

  EXPECT_LT(r.ns_per_op(), 5000.0);
}

/** Ping-pong between two threads through two events: the wake-up latency. */
TEST_F(EventPerf, PingPong) {
  auto pong = os_event_create(nullptr);
  constexpr uint64_t N = 20000;
  std::atomic<int64_t> ping_sig{os_event_reset(m_event)};

  std::thread t([&] {
    for (uint64_t i = 0; i < N; ++i) {
      os_event_wait_low(m_event, ping_sig.load());
      ping_sig = os_event_reset(m_event);
      os_event_set(pong);
    }
  });

  auto r = ut::bench("event_ping_pong", N, [&](uint64_t) {
    const auto sig = os_event_reset(pong);

    os_event_set(m_event);
    os_event_wait_low(pong, sig);
  });

  t.join();
  os_event_free(pong);

  EXPECT_LT(r.ns_per_op(), 1000000.0);
}

}  // namespace
