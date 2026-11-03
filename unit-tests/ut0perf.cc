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

/** @file unit-tests/ut0perf.cc
Performance tests of the components, see ut0perf.h. */

#include "ut0perf.h"

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <format>
#include <mutex>

namespace ut {

void report(const BenchResult &r) {
  const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
  const auto test = info != nullptr ? std::format("{}.{}", info->test_suite_name(), info->name()) : std::string("-");

  std::printf(
    "PERF %s %s threads=%d ops=%llu secs=%.3f ns/op=%.1f ops/s=%.0f\n",
    test.c_str(),
    r.m_name.c_str(),
    r.m_threads,
    static_cast<unsigned long long>(r.m_ops),
    r.m_secs,
    r.ns_per_op(),
    r.ops_per_sec()
  );
  std::fflush(stdout);

  ::testing::Test::RecordProperty(r.m_name + ".ns_per_op", std::format("{:.1f}", r.ns_per_op()));
  ::testing::Test::RecordProperty(r.m_name + ".ops_per_sec", std::format("{:.0f}", r.ops_per_sec()));

  if (const auto *out = std::getenv("UT_PERF_OUT"); out != nullptr && *out != '\0') {
    static std::mutex mutex;
    std::lock_guard lock(mutex);

    if (auto f = std::fopen(out, "a"); f != nullptr) {
      std::fprintf(
        f,
        "%lld,%s,%s,%d,%llu,%.6f,%.1f,%.0f\n",
        static_cast<long long>(std::time(nullptr)),
        test.c_str(),
        r.m_name.c_str(),
        r.m_threads,
        static_cast<unsigned long long>(r.m_ops),
        r.m_secs,
        r.ns_per_op(),
        r.ops_per_sec()
      );
      std::fclose(f);
    }
  }
}

}  // namespace ut
