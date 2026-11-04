/****************************************************************************
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

/** @file unit-tests/test_latch.cc
The containers and the latch under the lock system, no database: ut::Latch,
PageQueues, TrxSys::IdMap. */

#include <gtest/gtest.h>

#include "lock0lock.h"
#include "trx0sys.h"
#include "ut0latch.h"

#include <chrono>
#include <map>
#include <random>
#include <thread>
#include <vector>

namespace {

/** Threads add to a plain counter under the latch: none is lost (and TSan
sees no race), with holds short and long, so that waiters spin and sleep. */
TEST(Latch, Mutex) {
  for (uint32_t hold_us : {0U, 50U}) {
    constexpr int N = 8;
    const int ops = hold_us == 0 ? 200000 : 500;

    ut::Latch latch;
    uint64_t counter{};
    std::vector<std::thread> threads;

    for (int t{}; t < N; ++t) {
      threads.emplace_back([&] {
        for (int i{}; i < ops; ++i) {
          latch.lock();

          ++counter;

          if (hold_us > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us));
          }

          latch.unlock();
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    EXPECT_EQ(counter, uint64_t(N) * ops);
    EXPECT_FALSE(latch.is_locked());
  }
}

/** A sleeper is woken by the release. */
TEST(Latch, Wake) {
  ut::Latch latch;
  std::atomic<bool> got{};

  latch.lock();

  std::thread waiter([&] {
    latch.lock();
    got = true;
    latch.unlock();
  });

  /* Long enough for the waiter to end its spin and sleep. */
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  EXPECT_FALSE(got.load());
  EXPECT_FALSE(latch.try_lock());

  latch.unlock();
  waiter.join();

  EXPECT_TRUE(got.load());
  EXPECT_TRUE(latch.try_lock());

  latch.unlock();
}

/** Random finds, adds and erases against a map; every queue keeps its address
while it exists; iteration sees each queue once. */
TEST(PageQueues, Model) {
  PageQueues q;
  std::map<std::pair<space_id_t, page_no_t>, Rec_locks *> model;
  std::mt19937 rng(21);

  EXPECT_TRUE(q.empty());
  EXPECT_EQ(q.find(PageId{1, 1}), nullptr);
  EXPECT_TRUE(q.begin() == q.end());

  q.erase(PageId{1, 1});

  for (int i{}; i < 200000; ++i) {
    /* A range that grows the table, then shrinks it. */
    const uint32_t range = 1 + uint32_t(i < 100000 ? i / 20 : (200000 - i) / 20);
    const PageId id{space_id_t(rng() % 3), page_no_t(rng() % range)};
    const auto key = std::pair(id.m_space_id, id.m_page_no);
    const auto it = model.find(key);

    switch (rng() % 3) {
      case 0: {
        auto &locks = q.get(id);

        if (it != model.end()) {
          ASSERT_EQ(&locks, it->second) << "a queue moved";
        } else {
          ASSERT_TRUE(locks.empty());
          model[key] = &locks;
        }
        break;
      }
      case 1:
        q.erase(id);

        if (it != model.end()) {
          model.erase(it);
        }
        break;
      default:
        ASSERT_EQ(q.find(id), it == model.end() ? nullptr : it->second);
    }

    ASSERT_EQ(q.size(), model.size());

    if (i % 5000 == 0) {
      size_t n{};

      for (auto &[page_id, locks] : q) {
        const auto m = model.find(std::pair(page_id.m_space_id, page_id.m_page_no));

        ASSERT_NE(m, model.end());
        ASSERT_EQ(&locks, m->second);
        ++n;
      }

      ASSERT_EQ(n, model.size());
    }
  }
}

/** Random inserts, finds and erases against a map, ids clustered so that the
probes run long and the erases shift entries back. */
TEST(IdMap, Model) {
  TrxSys::IdMap ids;
  std::map<trx_id_t, Trx *> model;
  std::mt19937_64 rng(22);

  EXPECT_EQ(ids.find(1), nullptr);

  ids.erase(1, nullptr);

  auto fake = [](uint64_t v) { return reinterpret_cast<Trx *>(uintptr_t(v * 16 + 16)); };

  for (int i{}; i < 500000; ++i) {
    const uint32_t range = 1 + uint32_t(i < 250000 ? i / 50 : (500000 - i) / 50);
    const trx_id_t id = 1 + rng() % range;
    const auto it = model.find(id);

    switch (rng() % 4) {
      case 0:
      case 1:
        ids.insert(id, fake(id + i));
        model[id] = fake(id + i);
        break;
      case 2:
        /* Only if it maps to the transaction given. */
        ids.erase(id, fake(id + i + 1));
        ASSERT_EQ(ids.find(id), it == model.end() ? nullptr : it->second);

        if (it != model.end()) {
          ids.erase(id, it->second);
          model.erase(it);
        }
        break;
      default:
        break;
    }

    const trx_id_t probe = 1 + rng() % range;
    const auto p = model.find(probe);

    ASSERT_EQ(ids.find(probe), p == model.end() ? nullptr : p->second) << "id " << probe;
    ASSERT_EQ(ids.contains(probe), p != model.end());
  }

  for (const auto &[id, trx] : model) {
    ASSERT_EQ(ids.find(id), trx);
  }
}

}  // namespace
