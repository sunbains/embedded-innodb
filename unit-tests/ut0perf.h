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

/** @file unit-tests/ut0perf.h
Performance tests of the components.

A performance test is a gtest test whose suite name ends in "Perf", e.g.
TEST_F(LockPerf, RecCreate). ctest runs them under the "perf" label, one
program at a time (`cmake --build build --target perf`), never under "unit".

  auto r = ut::bench("rec_create", 100000, [&](uint64_t i) { ... });
  EXPECT_LT(r.ns_per_op(), 5000);   // generous: catches gross regressions

Each result is printed as a "PERF" line, recorded as a gtest property, and
appended to the CSV file $UT_PERF_OUT if set:
  time,test,name,threads,ops,seconds,ns_per_op,ops_per_sec
so runs can be compared over time. */

#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace ut {

/** The outcome of a benchmark. */
struct BenchResult {
  [[nodiscard]] double ns_per_op() const { return m_ops == 0 ? 0 : m_secs * 1e9 / double(m_ops); }

  [[nodiscard]] double ops_per_sec() const { return m_secs == 0 ? 0 : double(m_ops) / m_secs; }

  std::string m_name;
  int m_threads{1};
  uint64_t m_ops{};
  double m_secs{};
};

/** Print r, record it in the test's properties and in $UT_PERF_OUT. */
void report(const BenchResult &r);

/** Keep the compiler from optimizing away a value. */
template <typename T>
inline void keep(const T &v) {
  asm volatile("" : : "g"(&v) : "memory");
}

/** Run body(i) for i in [0, n) after warm_up calls, timed as a whole.
@return the result, already reported */
template <typename Body>
BenchResult bench(const std::string &name, uint64_t n, Body &&body, uint64_t warm_up = 0) {
  for (uint64_t i = 0; i < warm_up; ++i) {
    body(i);
  }

  const auto start = std::chrono::steady_clock::now();

  for (uint64_t i = 0; i < n; ++i) {
    body(i);
  }

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
  BenchResult r{name, 1, n, secs.count()};

  report(r);

  return r;
}

/** Run body(thread, i) for i in [0, n) in each of n_threads threads, started
together; the time is from the start to the end of the slowest.
@return the result for all the operations, already reported */
template <typename Body>
BenchResult bench_mt(const std::string &name, int n_threads, uint64_t n, Body &&body) {
  std::atomic<int> ready{};
  std::atomic<bool> go{};
  std::vector<std::thread> threads;

  for (int t = 0; t < n_threads; ++t) {
    threads.emplace_back([&, t] {
      ++ready;

      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      for (uint64_t i = 0; i < n; ++i) {
        body(t, i);
      }
    });
  }

  while (ready.load() < n_threads) {
    std::this_thread::yield();
  }

  const auto start = std::chrono::steady_clock::now();

  go.store(true, std::memory_order_release);

  for (auto &t : threads) {
    t.join();
  }

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;
  BenchResult r{name, n_threads, n * uint64_t(n_threads), secs.count()};

  report(r);

  return r;
}

}  // namespace ut
