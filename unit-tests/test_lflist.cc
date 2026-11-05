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

/** @file unit-tests/test_lflist.cc
The lock free list and the pools of ut0lflist.h, apart from the engine. */

#include "ut0lflist.h"

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <map>
#include <random>
#include <set>
#include <thread>
#include <vector>

using namespace ut;

namespace {

struct Item {
  std::atomic<uint64_t> m_value{};

  /** Bumped at each allocation: a walk holding the node must see it steady. */
  std::atomic<uint64_t> m_gen{};
};

using List = LfList<Item, ListCounter>;
using Node = List::Node;

/** @return the values of the nodes live for a walk, walking from the head. */
std::vector<uint64_t> live_values(List &list) {
  std::vector<uint64_t> v;
  List::Scan scan(list);

  for (auto n = scan.head(); n != nullptr; n = List::Scan::next(n)) {
    if (scan.live(*n)) {
      v.push_back(n->m_payload.m_value.load());
    }
  }

  return v;
}

Node *make(List &list, uint64_t value) {
  auto n = list.alloc();

  n->m_payload.m_value.store(value);
  n->m_payload.m_gen.fetch_add(1);

  return n;
}

void sweep_all(List &list, uint64_t horizon = ~uint64_t(0)) {
  EXPECT_TRUE(list.try_sweep(horizon, [](Node &) {}, [](Node &) {}));
}

}  // namespace

TEST(LfList, Empty) {
  ListCounter clock;
  List list(clock);

  EXPECT_TRUE(live_values(list).empty());
  EXPECT_EQ(list.n_linked(), 0u);
  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 0u);
}

TEST(LfList, One) {
  ListCounter clock;
  List list(clock);
  auto n = make(list, 1);

  list.push(n);
  EXPECT_EQ(live_values(list), std::vector<uint64_t>{1});

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 1u) << "it is live";

  EXPECT_NE(list.remove(n), 0u);
  EXPECT_TRUE(live_values(list).empty());

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 0u);
}

/** The order is that of the pushes, newest first, and of removes wherever they are. */
TEST(LfList, Model) {
  ListCounter clock;
  List list(clock);
  std::vector<std::pair<uint64_t, Node *>> oracle;
  std::mt19937 rng(42);
  uint64_t next{1};

  for (int i{}; i < 200000; ++i) {
    switch (rng() % 8) {
      case 0:
      case 1:
      case 2:
      case 3: {
        auto n = make(list, next);

        list.push(n);
        oracle.insert(oracle.begin(), {next, n});
        ++next;
        break;
      }
      case 4:
      case 5:
      case 6:
        if (!oracle.empty()) {
          const auto at = rng() % oracle.size();

          (void)list.remove(oracle[at].second);
          oracle.erase(oracle.begin() + long(at));
        }
        break;
      default:
        sweep_all(list);
    }

    if (oracle.size() > 500) {
      for (size_t k{}; k < 400; ++k) {
        (void)list.remove(oracle.back().second);
        oracle.pop_back();
      }
    }

    if (i % 512 == 0) {
      std::vector<uint64_t> expected;

      for (const auto &[v, n] : oracle) {
        expected.push_back(v);
      }

      ASSERT_EQ(live_values(list), expected) << i;
    }
  }

  /* Node reuse: it is bounded by the live ones and the limbo, not by the pushes. */
  EXPECT_LT(list.n_nodes(), 4096u);
}

/** A push with a ticket of its own; a walker gives it one if it comes first, and
the owner takes that. */
TEST(LfList, TicketHelp) {
  ListCounter clock;
  List list(clock);
  auto n = make(list, 1);

  list.push(n);

  uint64_t helped{};

  {
    List::Scan scan(list);

    helped = scan.ticket(*n);
    EXPECT_GT(helped, scan.seq());
  }

  EXPECT_EQ(list.final_ticket(*n), helped);
  (void)list.remove(n);
}

/** A remove is PENDING for a moment: a walker finishes it, for an S before the tick;
the owner then finds the stamp. */
TEST(LfList, RemoveHelp) {
  ListCounter clock;
  List list(clock);
  auto n = make(list, 1);

  list.push(n);

  List::mark_pending(n);

  uint64_t helped{};

  {
    List::Scan scan(list);

    EXPECT_TRUE(scan.live(*n)) << "the helper's tick is above its S";
    helped = n->m_seq.load();
    EXPECT_GT(helped, scan.seq());
  }

  EXPECT_EQ(list.final_seq(*n), helped);
  List::release(n);

  /* A walk now sees it removed. */
  EXPECT_TRUE(live_values(list).empty());
}

/** A removal is seen, or not, by its stamp against S, not by the walk's timing. */
TEST(LfList, RemoveDuringWalk) {
  ListCounter clock;
  List list(clock);
  auto a = make(list, 1);
  auto b = make(list, 2);

  list.push(a);
  list.push(b);

  {
    List::Scan scan(list);

    (void)list.remove(a);
    (void)list.remove(b);

    /* After the S of the walk: live for it. */
    EXPECT_TRUE(scan.live(*a));
    EXPECT_TRUE(scan.live(*b));

    /* Not unlinked under it. */
    sweep_all(list);
    EXPECT_EQ(list.n_linked(), 2u);
  }

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 0u);
}

/** The horizon of the caller holds nodes back. */
/** When commits outrun the clock stamps step by a fixed amount. The sweep must still
come at about its rate, or nothing is retired and the watermark stops. */
TEST(LfList, SweepSteps) {
  ListCounter clock;
  List list(clock);

  for (const uint64_t step : {1, 2, 3, 4, 8, 16, 64, 1024}) {
    int due{};

    for (uint64_t i{}; i < 8000; ++i) {
      /* Odd start: with the low bits only, an even step is never 0. */
      due += list.sweep_due(1 + i * step);
    }

    EXPECT_GT(due, 500) << step;
    EXPECT_LT(due, 1500) << step;
  }
}

TEST(LfList, Horizon) {
  ListCounter clock;
  List list(clock);
  auto n = make(list, 1);

  list.push(n);

  const auto stamp = list.remove(n);

  sweep_all(list, stamp - 1);
  EXPECT_EQ(list.n_linked(), 1u);

  sweep_all(list, stamp);
  EXPECT_EQ(list.n_linked(), 0u);
}

/** The owner of a node keeps it from the sweeper until it is done with it. */
TEST(LfList, Owned) {
  ListCounter clock;
  List list(clock);
  auto n = make(list, 1);

  list.push(n);
  List::mark_pending(n);
  (void)list.final_seq(*n);

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 1u);

  List::release(n);

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 0u);
}

/** A push wins against the sweeper's CAS of the head: the head removal goes through
the pushed node. */
TEST(LfList, HeadRacesPush) {
  ListCounter clock;
  List list(clock);
  auto a = make(list, 1);
  auto b = make(list, 2);

  list.push(a);
  list.push(b);

  (void)list.remove(b);

  struct Ctx {
    List *m_list;
    Node *m_new;
  } ctx{&list, make(list, 3)};

  list.m_sweep_hook_arg = &ctx;
  list.m_sweep_hook = [](void *arg) {
    auto c = static_cast<Ctx *>(arg);

    c->m_list->push(c->m_new);
  };

  /* The hook pushes after the sweeper read the head. */
  sweep_all(list);

  list.m_sweep_hook = nullptr;

  EXPECT_EQ(live_values(list), (std::vector<uint64_t>{3, 1}));
  EXPECT_EQ(list.n_linked(), 2u);
}

/** The same, with threads: every push is found, every remove taken out. */
TEST(LfList, HeadRacesPushMt) {
  ListCounter clock;
  List list(clock);
  std::atomic<bool> stop{};

  std::thread sweeper([&] {
    while (!stop.load()) {
      (void)list.try_sweep(~uint64_t(0), [](Node &) {}, [](Node &) {});
    }
  });

  std::vector<Node *> kept;

  for (uint64_t i{1}; i <= 200000; ++i) {
    auto n = make(list, i);

    list.push(n);

    if (i % 100 == 0) {
      kept.push_back(n);
    } else {
      (void)list.remove(n);
    }
  }

  stop.store(true);
  sweeper.join();

  sweep_all(list);

  auto values = live_values(list);

  EXPECT_EQ(values.size(), kept.size());

  for (size_t i{}; i < values.size(); ++i) {
    EXPECT_EQ(values[i], (kept.size() - i) * 100);
  }

  EXPECT_EQ(list.n_linked(), kept.size());
}

/** Recovery's insert keeps the order, among pushes too. */
TEST(LfList, Ordered) {
  ListCounter clock;
  List list(clock);

  for (uint64_t t : {uint64_t(50), uint64_t(70), uint64_t(60), uint64_t(10), uint64_t(90)}) {
    auto n = make(list, t);

    n->m_ticket.store(t);
    list.insert_ordered(n);
  }

  EXPECT_EQ(live_values(list), (std::vector<uint64_t>{90, 70, 60, 50, 10}));
}

/** A node a walk stands on is never reused: not while the walk holds it, even if it
was unlinked. The pool has few nodes, so a reuse would be at once. */
TEST(LfList, ReuseHeldNode) {
  ListCounter clock;
  List list(clock, 4);

  auto n = make(list, 1);

  list.push(n);

  const auto stamp = list.remove(n);

  {
    /* After the remove: S >= stamp, the sweeper may unlink it under the walk. */
    List::Scan scan(list);

    EXPECT_GE(scan.seq(), stamp);

    auto held = scan.head();

    ASSERT_EQ(held, n);

    sweep_all(list);
    EXPECT_EQ(list.n_linked(), 0u) << "unlinked";

    /* The other three of the pool, and no more: n is in limbo. */
    std::vector<Node *> others;

    for (int i{}; i < 3; ++i) {
      others.push_back(list.alloc());
      ASSERT_NE(others.back(), nullptr);
      EXPECT_NE(others.back(), n);
    }

    EXPECT_EQ(list.alloc(), nullptr) << "reused under the walk";

    EXPECT_EQ(List::Scan::next(held), nullptr) << "its next pointer is intact";

    for (auto o : others) {
      list.push(o);
      (void)list.remove(o);
    }
  }

  /* The walk is gone: two sweeps, one to unlink the others. */
  sweep_all(list);
  sweep_all(list);

  std::set<Node *> got;

  for (int i{}; i < 4; ++i) {
    got.insert(list.alloc());
  }

  EXPECT_TRUE(got.contains(n));
  EXPECT_FALSE(got.contains(nullptr));
}

/** Concurrent pushes, removes, sweeps, walks: a walker that holds a node sees the
same generation of it while it holds it, and its own nodes are all found. Under
TSan too: all the fields are atomics, so the races it finds would be in the logic
of the payload, the generations are the check. */
TEST(LfList, Stress) {
  ListCounter clock;
  List list(clock);
  constexpr int N_WRITERS = 4;
  constexpr int N_WALKERS = 4;
  std::atomic<bool> stop{};
  std::atomic<uint64_t> n_walks{};
  std::vector<std::thread> threads;

  for (int t{}; t < N_WRITERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      std::vector<Node *> own;

      while (!stop.load(std::memory_order_relaxed)) {
        auto n = make(list, (uint64_t(t) << 32) | 1);

        list.push(n);
        (void)list.final_ticket(*n);
        own.push_back(n);

        if (own.size() > 32 || (rng() & 1) == 0) {
          const auto at = rng() % own.size();

          const auto stamp = list.remove(own[at]);

          own.erase(own.begin() + long(at));

          if (list.sweep_due(stamp)) {
            (void)list.try_sweep(~uint64_t(0), [](Node &) {}, [](Node &) {});
          }
        }
      }

      for (auto n : own) {
        (void)list.remove(n);
      }
    });
  }

  for (int t{}; t < N_WALKERS; ++t) {
    threads.emplace_back([&] {
      std::vector<std::pair<Node *, uint64_t>> seen;

      while (!stop.load(std::memory_order_relaxed)) {
        List::Scan scan(list);

        seen.clear();

        for (auto n = scan.head(); n != nullptr; n = List::Scan::next(n)) {
          seen.emplace_back(n, n->m_payload.m_gen.load());
        }

        std::this_thread::yield();

        for (const auto &[n, gen] : seen) {
          ASSERT_EQ(n->m_payload.m_gen.load(), gen) << "reused under a walk";
        }

        n_walks.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(3));
  stop.store(true);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(n_walks.load(), 0u);

  sweep_all(list);
  EXPECT_EQ(list.n_linked(), 0u);
}

/** The slots: min over the ones set, for the pool's whole width. */
TEST(SlotPool, Min) {
  SlotPool pool;

  EXPECT_EQ(pool.min(100), 100u);

  std::vector<SnapSlot *> slots;

  for (uint64_t i{}; i < 1000; ++i) {
    slots.push_back(pool.alloc());
    slots.back()->m_v.store(500 + i);
  }

  EXPECT_EQ(pool.min(10000), 500u);
  EXPECT_EQ(pool.min(100), 100u);
  EXPECT_EQ(pool.n_set(), 1000u);

  pool.free(slots[0]);

  EXPECT_EQ(pool.min(10000), 501u);

  for (size_t i = 1; i < slots.size(); ++i) {
    pool.free(slots[i]);
  }

  EXPECT_EQ(pool.n_set(), 0u);
  EXPECT_EQ(pool.min(77), 77u);

  /* Reused, each slot of the pool in one place only. */
  std::set<SnapSlot *> again;

  for (int i{}; i < 1000; ++i) {
    again.insert(pool.alloc());
  }

  EXPECT_EQ(again.size(), 1000u);
}

TEST(SlotPool, Threads) {
  SlotPool pool;
  std::vector<std::thread> threads;

  for (int t{}; t < 8; ++t) {
    threads.emplace_back([&, t] {
      std::vector<SnapSlot *> own;

      for (int i{}; i < 200000; ++i) {
        auto s = pool.alloc();

        ASSERT_EQ(s->m_v.load(), 0u);
        s->m_v.store(uint64_t(t) + 1);
        own.push_back(s);

        if (own.size() > 16) {
          ASSERT_EQ(own.front()->m_v.load(), uint64_t(t) + 1);
          pool.free(own.front());
          own.erase(own.begin());
        }
      }

      for (auto s : own) {
        pool.free(s);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(pool.n_set(), 0u);
}
