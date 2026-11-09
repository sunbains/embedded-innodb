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

/** @file unit-tests/test_os.cc
The OS layer: threads (os0thread.cc), large memory blocks (os0proc.cc) and what test_sync.cc doesn't
cover of os0sync.cc. Files are test_file.cc. */

#include "ut0env.h"
#include "ut0perf.h"

#include "mem0mem.h"
#include "os0proc.h"
#include "os0sync.h"
#include "os0thread.h"
#include "ut0byte.h"
#include "ut0mem.h"

#include <fcntl.h>
#include <malloc.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

/* ------------------------------------------------------------ threads */

struct ThreadArg {
  std::atomic<int> m_ran{};
  os_thread_id_t m_self{};
  size_t m_guard{};
  size_t m_stack{};
};

void *thread_fn(void *p) {
  auto arg = static_cast<ThreadArg *>(p);

  arg->m_self = os_thread_get_curr_id();

  pthread_attr_t attr;

  if (pthread_getattr_np(pthread_self(), &attr) == 0) {
    pthread_attr_getguardsize(&attr, &arg->m_guard);
    pthread_attr_getstacksize(&attr, &arg->m_stack);
    pthread_attr_destroy(&attr);
  }

  ++arg->m_ran;

  return p;
}

/** os_thread_create() runs f(arg) in a joinable thread whose id it returns. */
TEST(OsThreadTest, CreateRunsAndJoins) {
  ThreadArg arg;
  os_thread_id_t id{};
  const auto t = os_thread_create(thread_fn, &arg, &id);

  EXPECT_TRUE(os_thread_eq(t, id));

  void *ret{};

  ASSERT_EQ(pthread_join(t, &ret), 0);
  EXPECT_EQ(ret, &arg);
  EXPECT_EQ(arg.m_ran, 1);
  EXPECT_TRUE(os_thread_eq(arg.m_self, id));
  EXPECT_FALSE(os_thread_eq(arg.m_self, os_thread_get_curr_id()));
  EXPECT_TRUE(os_thread_eq(os_thread_get_curr_id(), os_thread_get_curr()));
  EXPECT_NE(os_thread_pf(arg.m_self), os_thread_pf(os_thread_get_curr_id()));
}

/** The thread id out parameter is optional. */
TEST(OsThreadTest, NullIdOut) {
  ThreadArg arg;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);
  EXPECT_EQ(arg.m_ran, 1);
}

/** The threads have the default stack size. */
TEST(OsThreadTest, StackSize) {
  ThreadArg arg, ref;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);

  std::thread st([&] { thread_fn(&ref); });

  st.join();

  EXPECT_EQ(arg.m_stack, ref.m_stack);
  EXPECT_GE(arg.m_stack, size_t(256 * 1024));
}

/* BUG: os_thread_create() (os/os0thread.cc:49) passes a memset() zero
pthread_attr_t instead of one from pthread_attr_init(): glibc then creates
the thread with a guard size of 0, i.e. no guard page below the stack, so a
stack overflow in an engine thread silently overwrites the memory next to it
instead of faulting. (A zeroed attr is also not a valid attr object per
POSIX.) Fix:
-  memset(&attr, 0x0, sizeof(attr));
-
-  auto ret = pthread_create(&pthread, &attr, f, arg);
+  pthread_attr_init(&attr);
+
+  auto ret = pthread_create(&pthread, &attr, f, arg);
+
+  pthread_attr_destroy(&attr);
Also: log_fatal("pthread_create returned %d", ret, ...) prints "%d" as is. */
TEST(OsThreadTest, GuardPage) {
  ThreadArg arg, ref;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);

  std::thread st([&] { thread_fn(&ref); });

  st.join();

  EXPECT_GT(ref.m_guard, 0UL);
  EXPECT_EQ(arg.m_guard, ref.m_guard);
}

/** @return the virtual size of this process in KB. */
size_t vm_size_kb() {
  std::ifstream in("/proc/self/status");
  std::string line;

  while (std::getline(in, line)) {
    if (line.rfind("VmSize:", 0) == 0) {
      return std::stoul(line.substr(7));
    }
  }

  return 0;
}

/* BUG: os_thread_create() makes joinable threads, but nothing in the engine
joins them or detaches them (Srv::create_thread(), srv/srv0srv.cc:1545, the
only caller, drops the id; there is no os_thread_join()). A joinable thread
that exits keeps its stack (8 MB of address space, plus the TLS and the
descriptor) until it is joined: every start and shutdown of a System leaks
the stacks of its background threads, which an embedded engine that is
started and stopped in a long-lived process (or in these tests) accumulates.
Fix: detach them (pthread_attr_setdetachstate(&attr,
PTHREAD_CREATE_DETACHED) after pthread_attr_init()), the engine already
waits for them with Srv::n_background_threads(); or add os_thread_join()
and join them at shutdown. */
TEST(OsThreadTest, ExitedThreadsAreReclaimed) {
  constexpr int N = 32;
  ThreadArg args[N];

  /* One malloc arena: glibc gives threads that run together arenas of their own, 64 MB of address
  space each that isn't a stack and that stays, more of them the busier the host. What's measured
  is the stacks only. */
#if defined(__SANITIZE_ADDRESS__)
  /* Its allocator isn't glibc's, and its shadow memory is the address space measured. */
  GTEST_SKIP() << "the address space under ASan isn't what this measures";
#endif /* __SANITIZE_ADDRESS__ */

  ASSERT_EQ(mallopt(M_ARENA_MAX, 1), 1);

  /* Warm up the allocator's own caches: as many threads at once as below. */
  {
    ThreadArg warm[N];
    pthread_t ids[N];

    for (int i = 0; i < N; ++i) {
      ids[i] = os_thread_create(thread_fn, &warm[i], nullptr);
    }

    for (auto id : ids) {
      pthread_join(id, nullptr);
    }
  }

  const auto before = vm_size_kb();

  for (auto &a : args) {
    pthread_detach(os_thread_create(thread_fn, &a, nullptr));
  }

  for (auto &a : args) {
    while (a.m_ran.load() == 0) {
      std::this_thread::sleep_for(1ms);
    }
  }

  std::this_thread::sleep_for(50ms);

  const auto after = vm_size_kb();

  /* Each kept stack is ~8 MB: allow far less than one per thread. */
  EXPECT_LT(after - before, size_t(N) * 1024) << "VmSize grew by " << (after - before) << " KB for " << N << " exited threads";
}

/** Many threads in a row and many at once, each runs exactly once. */
TEST(OsThreadTest, Many) {
  for (int i = 0; i < 500; ++i) {
    ThreadArg arg;

    ASSERT_EQ(pthread_join(os_thread_create(thread_fn, &arg, nullptr), nullptr), 0);
    ASSERT_EQ(arg.m_ran, 1);
  }

  constexpr int N = 128;
  std::vector<ThreadArg> args(N);
  std::vector<os_thread_t> threads;
  std::set<ulint> ids;

  for (auto &a : args) {
    threads.push_back(os_thread_create(thread_fn, &a, nullptr));
  }

  for (size_t i = 0; i < threads.size(); ++i) {
    ASSERT_EQ(pthread_join(threads[i], nullptr), 0);
    EXPECT_EQ(args[i].m_ran, 1);
    ids.insert(os_thread_pf(args[i].m_self));
  }

  /* Live at the same time: all ids differ. */
  EXPECT_EQ(ids.size(), size_t(N));
}

/** Threads that create threads at once. */
TEST(OsThreadTest, CreateFromManyThreads) {
  std::atomic<int> ran{};
  std::vector<std::thread> creators;

  for (int c = 0; c < 8; ++c) {
    creators.emplace_back([&] {
      for (int i = 0; i < 50; ++i) {
        ThreadArg arg;
        const auto t = os_thread_create(thread_fn, &arg, nullptr);

        pthread_join(t, nullptr);
        ran += arg.m_ran;
      }
    });
  }

  for (auto &t : creators) {
    t.join();
  }

  EXPECT_EQ(ran, 400);
}

/** os_thread_sleep() sleeps at least the time, 0 returns at once; yield returns. */
TEST(OsThreadTest, SleepYield) {
  auto start = std::chrono::steady_clock::now();

  os_thread_sleep(20000);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);

  start = std::chrono::steady_clock::now();
  os_thread_sleep(0);
  os_thread_yield();
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}

/* ---------------------------------------------------------- large memory */

/** os_mem_alloc_large() rounds up to the system page, zero filled, writable. */
TEST(OsProcTest, AllocLarge) {
  const ulint page = ulint(getpagesize());

  for (const ulint n : {1UL, page - 1, page, page + 1, 10 * page + 3, 64UL << 20}) {
    ulint size = n;
    auto p = static_cast<byte *>(os_mem_alloc_large(&size));

    ASSERT_NE(p, nullptr) << n;
    EXPECT_EQ(size, ut_calc_align(n, page)) << n;
    EXPECT_EQ(ut_align_offset(p, page), 0UL);

    /* Zero filled, and every page writable. */
    for (ulint i = 0; i < size; i += page) {
      ASSERT_EQ(p[i], 0);
      p[i] = 1;
    }

    p[size - 1] = 2;
    os_mem_free_large(p, size);
  }
}

/** 0 bytes and an impossible size: nullptr, the size rounded. */
TEST(OsProcTest, AllocLargeEdge) {
  ulint size{0};

  EXPECT_EQ(os_mem_alloc_large(&size), nullptr);
  EXPECT_EQ(size, 0UL);

  size = ulint(1) << 50;
  EXPECT_EQ(os_mem_alloc_large(&size), nullptr);
}

/** os_mem_free_large() of a bad range logs, doesn't crash. */
TEST(OsProcTest, FreeLargeBad) {
  os_mem_free_large(reinterpret_cast<void *>(1), 4096);
}

/** os_proc_get_number() is the process id. */
TEST(OsProcTest, ProcNumber) {
  EXPECT_EQ(os_proc_get_number(), ulint(getpid()));
}

/* -------------------------------------------- os0sync, beyond test_sync */

/** An OS mutex has its own event; unlocking one that isn't locked asserts. */
TEST(OsMutexTest, UnlockUnlockedAsserts) {
  auto m = os_mutex_create(nullptr);

  ASSERT_NE(m, nullptr);
  EXPECT_DEATH(os_mutex_exit(m), "");

  os_mutex_enter(m);
  os_mutex_exit(m);
  os_mutex_destroy(m);
}

/** The lock excludes another thread while held. */
TEST(OsMutexTest, Excludes) {
  auto m = os_mutex_create(nullptr);
  std::atomic<bool> got{};

  os_mutex_enter(m);

  std::thread t([&] {
    os_mutex_enter(m);
    got = true;
    os_mutex_exit(m);
  });

  std::this_thread::sleep_for(20ms);
  EXPECT_FALSE(got);
  os_mutex_exit(m);
  t.join();
  EXPECT_TRUE(got);
  os_mutex_destroy(m);
}

/* BUG: OsMutex::lock() (os/os0sync.cc:60) checks m_count == 1 after
std::mutex::lock() to catch a thread that locks the mutex it holds, as the
comment on m_count says, but a recursive std::mutex::lock() deadlocks (or is
undefined) before the check is reached: the check can never fire. Fix:
record the owner and assert before locking:
+  std::atomic<std::thread::id> m_owner{};
   void lock() {
+    ut_a(m_owner.load(std::memory_order_relaxed) != std::this_thread::get_id());
     static_cast<std::mutex *>(m_ptr)->lock();
+    m_owner.store(std::this_thread::get_id(), std::memory_order_relaxed);
   ...
   void unlock() {
+    m_owner.store({}, std::memory_order_relaxed);
In a child: today it hangs until the alarm. */
TEST(OsMutexTest, RecursiveLockAsserts) {
  EXPECT_DEATH(
    {
      alarm(2);
      auto m = os_mutex_create(nullptr);
      os_mutex_enter(m);
      os_mutex_enter(m);
    },
    "Assertion failure"
  );
}

struct OsEventTest : public ::testing::Test {
  void SetUp() override { m_event = os_event_create(nullptr); }
  void TearDown() override { os_event_free(m_event); }

  CondVar *m_event{};
};

/** wait_time() with a timeout of 0: at once, exceeded unless set. */
TEST_F(OsEventTest, WaitTimeZero) {
  const auto start = std::chrono::steady_clock::now();

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(0), 0), OS_SYNC_TIME_EXCEEDED);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);

  os_event_set(m_event);
  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(0), 0), 0UL);
}

/** wait_time(max) waits for the set(): the "infinite" wait of row0pread.cc. */
TEST_F(OsEventTest, WaitTimeInfinite) {
  const auto sig = os_event_reset(m_event);
  const auto start = std::chrono::steady_clock::now();

  std::thread t([&] {
    std::this_thread::sleep_for(30ms);
    os_event_set(m_event);
  });

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds::max(), sig), 0UL);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 25ms);
  t.join();
}

/* BUG: CondVar::wait_time() (os/os0sync.cc:126) computes
system_clock::now() + timeout in system_clock's int64 nanoseconds. Any
timeout other than microseconds::max() for which that overflows (more than
~234 years, e.g. microseconds(2^62) used as "forever") wraps to a time in
the past, and so does a negative one: microseconds(OS_SYNC_INFINITE_TIME),
the "infinite" constant os0sync.h offers, is microseconds(-1). Both return
OS_SYNC_TIME_EXCEEDED at once instead of waiting. The callers today pass
microseconds::max() (row/row0pread.cc:835, 904), which is special-cased.
The wait is also on the wall clock: a clock step lengthens or shortens it.
Fix: wait on steady_clock, saturate, and treat negative as infinite:
-  std::chrono::time_point<std::chrono::system_clock> timepoint;
-  if (timeout != std::chrono::microseconds::max()) {
-    timepoint = std::chrono::system_clock::now() + timeout;
-  } else {
-    timepoint = std::chrono::time_point<std::chrono::system_clock>::max();
-  }
+  using Clock = std::chrono::steady_clock;
+  const auto now = Clock::now();
+  const auto left = std::chrono::duration_cast<std::chrono::microseconds>(Clock::time_point::max() - now);
+  const auto timepoint = timeout.count() >= 0 && timeout < left ? now + timeout : Clock::time_point::max(); */
TEST_F(OsEventTest, WaitTimeHugeTimeout) {
  for (const auto timeout : {std::chrono::microseconds(int64_t(1) << 62), std::chrono::microseconds(OS_SYNC_INFINITE_TIME)}) {
    const auto sig = os_event_reset(m_event);
    const auto start = std::chrono::steady_clock::now();

    std::thread t([&] {
      std::this_thread::sleep_for(30ms);
      os_event_set(m_event);
    });

    EXPECT_EQ(m_event->wait_time(timeout, sig), 0UL) << timeout.count();
    EXPECT_GE(std::chrono::steady_clock::now() - start, 25ms) << timeout.count();
    t.join();
  }
}

/** Two threads hand a token back and forth with reset()/wait_low()/set():
no wake-up is lost (a lost one shows as a 10 s timeout). */
TEST(OsEventStress, PingPong) {
  auto ping = os_event_create(nullptr);
  auto pong = os_event_create(nullptr);
  constexpr int N = 20000;
  std::atomic<int> turn{0};
  std::atomic<int> timeouts{};

  auto player = [&](CondVar *mine, CondVar *theirs, int me) {
    for (int i = 0; i < N; ++i) {
      for (;;) {
        const auto sig = os_event_reset(mine);

        if (turn.load() % 2 == me) {
          break;
        }

        if (mine->wait_time(std::chrono::microseconds(10000000), sig) != 0) {
          ++timeouts;
          return;
        }
      }

      ++turn;
      os_event_set(theirs);
    }
  };

  std::thread a(player, ping, pong, 0);
  std::thread b(player, pong, ping, 1);

  a.join();
  b.join();

  EXPECT_EQ(timeouts, 0);
  EXPECT_EQ(turn, 2 * N);

  os_event_free(ping);
  os_event_free(pong);
}

/** Many waiters and a setter that races with resets by the waiters: every
waiter sees every generation. */
TEST(OsEventStress, ManyWaitersGenerations) {
  auto ev = os_event_create(nullptr);
  constexpr int N_WAITERS = 8;
  constexpr int N_GEN = 2000;
  std::atomic<int> gen{0};
  std::atomic<int> seen_all{};
  std::atomic<int> timeouts{};
  std::vector<std::thread> waiters;

  for (int w = 0; w < N_WAITERS; ++w) {
    waiters.emplace_back([&] {
      int last{};

      while (last < N_GEN) {
        const auto sig = os_event_reset(ev);

        if (gen.load() > last) {
          last = gen.load();
          continue;
        }

        if (ev->wait_time(std::chrono::microseconds(10000000), sig) != 0) {
          ++timeouts;
          return;
        }
      }

      ++seen_all;
    });
  }

  for (int g = 1; g <= N_GEN; ++g) {
    gen = g;
    os_event_set(ev);

    if (g % 64 == 0) {
      std::this_thread::yield();
    }
  }

  for (auto &t : waiters) {
    t.join();
  }

  EXPECT_EQ(timeouts, 0);
  EXPECT_EQ(seen_all, N_WAITERS);
  os_event_free(ev);
}

}  // namespace

/* ------------------------------------------------------------- perf */

namespace {

struct ThreadPerf : public ::testing::Test {};

/** os_thread_create() + pthread_join(). */
TEST_F(ThreadPerf, CreateJoin) {
  ThreadArg arg;
  auto r = ut::bench("os_thread_create_join", 5000, [&](uint64_t) { pthread_join(os_thread_create(thread_fn, &arg, nullptr), nullptr); });

  EXPECT_LT(r.ns_per_op(), 2e6);
}

struct OsProcPerf : public ::testing::Test {};

/** os_mem_alloc_large() + touch + free of 1 MB. */
TEST_F(OsProcPerf, AllocLarge) {
  auto r = ut::bench("os_mem_alloc_large_1m", 2000, [&](uint64_t) {
    ulint n = 1 << 20;
    auto p = static_cast<byte *>(os_mem_alloc_large(&n));
    p[0] = 1;
    os_mem_free_large(p, n);
  });

  EXPECT_LT(r.ns_per_op(), 2e6);
}

struct OsMutexPerf : public ::testing::Test {};

/** os_mutex_enter()/exit(), alone and with 4 threads. */
TEST_F(OsMutexPerf, EnterExit) {
  auto m = os_mutex_create(nullptr);
  auto r1 = ut::bench("os_mutex_uncontended", 5000000, [&](uint64_t) {
    os_mutex_enter(m);
    os_mutex_exit(m);
  });
  auto r4 = ut::bench_mt("os_mutex_contended", 4, 500000, [&](int, uint64_t) {
    os_mutex_enter(m);
    os_mutex_exit(m);
  });

  EXPECT_LT(r1.ns_per_op(), 2000);
  EXPECT_LT(r4.ns_per_op(), 50000);
  os_mutex_destroy(m);
}

}  // namespace
