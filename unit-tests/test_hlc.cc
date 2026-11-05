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

/** @file unit-tests/test_hlc.cc
The Accord timestamp, the clock and the ids (ut0hlc.h), apart from the rest of
the engine. The vectors of Golden come from running the Rust code of
sql-rust/accord/src/primitives/timestamp.rs, not derived by hand: a throwaway
crate that includes that file and prints these fields. */

#include "ut0hlc.h"
#include "ut0latch.h"
#include "ut0perf.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <barrier>
#include <format>
#include <set>
#include <thread>
#include <vector>

using namespace ut;

namespace {

struct Gold {
  const char *m_name;
  uint64_t m_msb;
  uint64_t m_lsb;
  uint32_t m_node;
  uint64_t m_epoch;
  uint64_t m_hlc;
  uint32_t m_flags;
};

constexpr uint64_t B = HLC_BASE_US;

/** The output of the Rust program, in order. */
const Gold GOLD[] = {
  {"zero", 0ULL, 0ULL, 0U, 0ULL, 0ULL, 0U},
  {"max", 18446744073709551615ULL, 18446744073709551600ULL, 4294967295U, 281474976710655ULL, 18446744073709551615ULL, 0U},
  {"simple", 65536ULL, 16000ULL, 42U, 1ULL, 1000ULL, 0U},
  {"large", 9223372036854710279ULL, 18446744073709551600ULL, 4294967295U, 140737488355327ULL, 9223372036854775807ULL, 0U},
  {"hlc_2p60m1", 196608ULL, 18446744073709551600ULL, 7U, 3ULL, 1152921504606846975ULL, 0U},
  {"hlc_2p60", 196609ULL, 0ULL, 7U, 3ULL, 1152921504606846976ULL, 0U},
  {"hlc_2p60p1", 196609ULL, 16ULL, 7U, 3ULL, 1152921504606846977ULL, 0U},
  {"hlc_max", 65551ULL, 18446744073709551600ULL, 1U, 1ULL, 18446744073709551615ULL, 0U},
  {"flags1", 131072ULL, 80001ULL, 5U, 2ULL, 5000ULL, 1U},
  {"flags2", 131072ULL, 80002ULL, 5U, 2ULL, 5000ULL, 2U},
  {"flags4", 131072ULL, 80004ULL, 5U, 2ULL, 5000ULL, 4U},
  {"flags8", 131072ULL, 80008ULL, 5U, 2ULL, 5000ULL, 8U},
  {"flagsF", 131072ULL, 80015ULL, 5U, 2ULL, 5000ULL, 15U},
  {"flags_trunc", 131072ULL, 80015ULL, 5U, 2ULL, 5000ULL, 15U},
  {"base", 65536ULL, 28695859200000000ULL, 0U, 1ULL, 1793491200000000ULL, 0U},
  {"base1", 65536ULL, 28695859200000016ULL, 1U, 1ULL, 1793491200000001ULL, 0U},
  {"base_n4095", 589824ULL, 28695861175308624ULL, 4095U, 9ULL, 1793491323456789ULL, 0U},
  {"top52", 65536ULL, 100753453237927920ULL, 4095U, 1ULL, 6297090827370495ULL, 0U},
  {"rt", 809041920ULL, 1086251ULL, 99U, 12345ULL, 67890ULL, 11U},
  {"merge_max", 65536ULL, 3203ULL, 1U, 1ULL, 200ULL, 3U},
  {"merge_min", 65536ULL, 1603ULL, 1U, 1ULL, 100ULL, 3U},
  {"merge_max_rev", 65536ULL, 3203ULL, 1U, 1ULL, 200ULL, 3U},
  {"merge_min_rev", 65536ULL, 1603ULL, 1U, 1ULL, 100ULL, 3U},
  {"with_epoch", 327680ULL, 1610ULL, 1U, 5ULL, 100ULL, 10U},
  {"with_node", 65536ULL, 1610ULL, 2U, 1ULL, 100ULL, 10U},
  {"without_flags", 65536ULL, 1600ULL, 1U, 1ULL, 100ULL, 0U},
  {"with_added", 65536ULL, 1615ULL, 1U, 1ULL, 100ULL, 15U},
};

void check(const Gold &g, const Timestamp &t) {
  EXPECT_EQ(t.msb(), g.m_msb) << g.m_name;
  EXPECT_EQ(t.lsb(), g.m_lsb) << g.m_name;
  EXPECT_EQ(t.node(), g.m_node) << g.m_name;
  EXPECT_EQ(t.epoch(), g.m_epoch) << g.m_name;
  EXPECT_EQ(t.hlc(), g.m_hlc) << g.m_name;
  EXPECT_EQ(t.flags(), g.m_flags) << g.m_name;
}

}  // namespace

/** Each vector built as the Rust program built it, and from its bits. */
TEST(Hlc, Golden) {
  const Timestamp a(1, 100, 1, 1);
  const Timestamp b(1, 200, 1, 2);
  const Timestamp e(1, 100, 1, 0b1010);

  const Timestamp built[] = {
    Timestamp::zero(),
    Timestamp::max(),
    Timestamp(1, 1000, 42),
    Timestamp((uint64_t(1) << 47) - 1, (uint64_t(1) << 63) - 1, UINT32_MAX),
    Timestamp(3, (uint64_t(1) << 60) - 1, 7),
    Timestamp(3, uint64_t(1) << 60, 7),
    Timestamp(3, (uint64_t(1) << 60) + 1, 7),
    Timestamp(1, UINT64_MAX, 1),
    Timestamp(2, 5000, 5, TS_REJECTED),
    Timestamp(2, 5000, 5, TS_UNSTABLE),
    Timestamp(2, 5000, 5, TS_HLC_BOUND),
    Timestamp(2, 5000, 5, TS_SHARD_BOUND),
    Timestamp(2, 5000, 5, 0xF),
    Timestamp(2, 5000, 5, 0xFF),
    Timestamp(1, B, 0),
    Timestamp(1, B + 1, 1),
    Timestamp(9, B + 123456789, 4095),
    Timestamp(1, B + (uint64_t(1) << 52) - 1, 4095),
    Timestamp(12345, 67890, 99, 0b1011),
    a.merge_max(b),
    a.merge_min(b),
    b.merge_max(a),
    b.merge_min(a),
    e.with_epoch(5),
    e.with_node(2),
    e.without_flags(),
    e.with_added_flags(5),
  };

  ASSERT_EQ(std::size(GOLD), std::size(built));

  for (size_t i{}; i < std::size(GOLD); ++i) {
    check(GOLD[i], built[i]);
    check(GOLD[i], Timestamp::from_bits(GOLD[i].m_msb, GOLD[i].m_lsb, GOLD[i].m_node));
  }
}

/** test_timestamp_ordering and test_timestamp_ordering_ignores_flags of Rust. */
TEST(Hlc, Order) {
  const Timestamp t1(1, 100, 1);
  const Timestamp t2(1, 200, 1);
  const Timestamp t3(2, 50, 1);
  const Timestamp t4(1, 100, 2);

  EXPECT_LT(t1, t2);
  EXPECT_LT(t2, t3);
  EXPECT_LT(t1, t4);

  EXPECT_EQ(Timestamp(1, 100, 1, 0b0001) <=> Timestamp(1, 100, 1, 0b1111), std::strong_ordering::equal);
  EXPECT_LT(Timestamp(1000, UINT64_MAX >> 4, 1000), Timestamp::max());
  EXPECT_EQ(Timestamp(1, 100, 1, TS_UNSTABLE).flags(), TS_UNSTABLE);
  EXPECT_TRUE(Timestamp(1, 1, 1, TS_SHARD_BOUND).is_shard_bound());
}

TEST(Hlc, Merge) {
  const Timestamp a(1, 100, 1, 0b0001);
  const Timestamp b(1, 200, 1, 0b0010);

  EXPECT_EQ(a.merge_max(b).hlc(), 200u);
  EXPECT_EQ(a.merge_max(b).flags(), 0b0011);
  EXPECT_EQ(a.merge_min(b).hlc(), 100u);
  EXPECT_EQ(a.merge_min(b).flags(), 0b0011);
}

/** Packing: round trip, and the integer order is epoch, time, node. */
TEST(Hlc, IdPack) {
  EXPECT_EQ(HLC_NODE_BITS + HLC_TIME_BITS + HLC_EPOCH_BITS, 64u);

  const uint64_t top = B + HLC_MAX_TIME;

  for (uint64_t epoch : {uint64_t(0), uint64_t(1), uint64_t(HLC_MAX_EPOCHS - 1)}) {
    for (uint64_t hlc : {B + 1, B + 12345678, top}) {
      for (uint32_t node : {uint32_t(0), uint32_t(1), HLC_MAX_NODES - 1}) {
        const auto id = id_pack(epoch, hlc, node);

        EXPECT_EQ(id_to_epoch(id), epoch);
        EXPECT_EQ(id_to_hlc(id), hlc);
        EXPECT_EQ(id_to_node(id), node);

        const auto ts = id_to_timestamp(id);

        EXPECT_EQ(ts.msb(), Timestamp(epoch, hlc, node).msb());
        EXPECT_EQ(ts.lsb(), Timestamp(epoch, hlc, node).lsb());
        EXPECT_EQ(ts.node(), node);

        uint64_t back{};

        EXPECT_TRUE(id_of_timestamp(ts, back));
        EXPECT_EQ(back, id);
      }
    }
  }

  EXPECT_LT(id_pack(1, B + 2, 63), id_pack(1, B + 3, 0)) << "time before node";
  EXPECT_LT(id_pack(1, top, 63), id_pack(2, B + 1, 0)) << "epoch before time";
  EXPECT_LT(id_pack(1, B + 2, 0), id_pack(1, B + 2, 1));

  /* The id limit of the last value doesn't spill into the epoch. */
  EXPECT_LT(word_id_limit(id_pack(1, top, 0)), id_pack(2, B, 0));
  EXPECT_GT(word_id_limit(id_pack(1, top, 0)), id_pack(1, top, HLC_MAX_NODES - 1));

  /* The next id of a node is of the node. */
  EXPECT_EQ(id_to_node(id_next(id_pack(1, B + 5, 7))), 7u);
  EXPECT_EQ(id_to_hlc(id_next(id_pack(1, B + 5, 7))), B + 6);
  EXPECT_EQ(id_prev(id_next(id_pack(1, B + 5, 7))), id_pack(1, B + 5, 7));
}

/** The public way in refuses what would wrap. */
TEST(Hlc, IdRefuse) {
  uint64_t id{};

  EXPECT_FALSE(id_of_timestamp(Timestamp(1, B - 1, 1), id)) << "before the base";
  EXPECT_FALSE(id_of_timestamp(Timestamp(1, B, 1), id)) << "id time 0";
  EXPECT_FALSE(id_of_timestamp(Timestamp(1, B + HLC_MAX_TIME + 1, 1), id));
  EXPECT_FALSE(id_of_timestamp(Timestamp(1, B + 1, HLC_MAX_NODES), id));
  EXPECT_FALSE(id_of_timestamp(Timestamp(HLC_MAX_EPOCHS, B + 1, 1), id));
  EXPECT_TRUE(id_of_timestamp(Timestamp(1, B + HLC_MAX_TIME, 1), id));
  EXPECT_FALSE(hlc_fits(0));
}

/** The multiplicative hashes of TrxSys (id shard, id map slot) spread the ids of a
node, whose low bits and top bits are constant: in a row, and clumped in time. */
TEST(Hlc, IdHash) {
  constexpr uint64_t SHARD = 0x9E3779B97F4A7C15ULL;
  constexpr uint64_t SLOT = 0xD6E8FEB86659FD93ULL;

  for (uint64_t gap : {uint64_t(1), uint64_t(7), uint64_t(1000)}) {
    std::vector<uint32_t> shards(256);
    std::set<uint64_t> slots;
    constexpr uint64_t N = 256 * 256;

    for (uint64_t i{}; i < N; ++i) {
      const auto id = id_pack(1, B + 1000000 + i * gap, 3);

      ++shards[(id * SHARD) >> 56];

      if (i < 1024) {
        slots.insert((id * SLOT) >> (64 - 12));
      }
    }

    const auto [lo, hi] = std::minmax_element(shards.begin(), shards.end());

    EXPECT_GT(*lo, N / 256 / 2) << gap;
    EXPECT_LT(*hi, N / 256 * 2) << gap;
    EXPECT_GT(slots.size(), 1024u * 3 / 4) << gap;
  }
}

/** From Rust's tests: unique, monotonic. */
TEST(Hlc, ClockNext) {
  TimeService ts(1, 1);

  EXPECT_EQ(ts.epoch(), 1u);
  EXPECT_EQ(ts.node(), 1u);

  auto prev = ts.next_timestamp();

  for (int i{}; i < 1000; ++i) {
    const auto next = ts.next_timestamp();

    EXPECT_GT(next, prev);
    prev = next;
  }
}

TEST(Hlc, ClockThreads) {
  TimeService ts(1, 1);
  constexpr int N = 8;
  constexpr int M = 100000;
  std::vector<std::vector<uint64_t>> got(N);
  std::vector<std::thread> threads;
  std::barrier sync(N);

  for (int t{}; t < N; ++t) {
    threads.emplace_back([&, t] {
      got[t].reserve(M);
      sync.arrive_and_wait();

      uint64_t prev{};

      for (int i{}; i < M; ++i) {
        const auto w = ts.next_word(ts.wall());

        ASSERT_GT(w, prev);
        prev = w;
        got[t].push_back(w);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::vector<uint64_t> all;

  for (auto &v : got) {
    all.insert(all.end(), v.begin(), v.end());
  }

  std::sort(all.begin(), all.end());

  EXPECT_EQ(std::adjacent_find(all.begin(), all.end()), all.end()) << "unique";
}

TEST(Hlc, ClockUpdate) {
  TimeService ts(1, 1);
  const auto t1 = ts.next_timestamp();
  const Timestamp future(1, t1.hlc() + 100000000, 2);

  ts.update(future);

  EXPECT_EQ(ts.read(), future.hlc());
  EXPECT_GT(ts.next_timestamp(), future);

  /* Below the clock, and below the base: nothing. */
  const auto now = ts.read();

  ts.update(Timestamp(1, now - 5, 2));
  EXPECT_EQ(ts.read(), now);

  ts.update(Timestamp(1, 12345, 2));
  EXPECT_EQ(ts.read(), now);

  /* Past the range of the time: not taken. */
  ts.update(Timestamp(1, B + HLC_MAX_TIME + 1, 2));
  EXPECT_EQ(ts.read(), now);

  const Timestamp min(1, now + 1000000, 2);
  const auto least = ts.at_least(min);

  EXPECT_GT(least, min);
  EXPECT_EQ(least.hlc(), min.hlc() + 1);
}

/** The wall clock goes back: last + 1. It is ahead: it wins. */
TEST(Hlc, WallRegress) {
  TimeService ts(1, 1);

  ts.set_wall(B + 5000000);
  EXPECT_EQ(ts.next_timestamp().hlc(), B + 5000000);

  ts.set_wall(B + 100);
  EXPECT_EQ(ts.next_timestamp().hlc(), B + 5000001);
  EXPECT_EQ(ts.next_timestamp().hlc(), B + 5000002);

  ts.set_wall(B + 7000000);
  EXPECT_EQ(ts.next_timestamp().hlc(), B + 7000000);
}

/** A wall clock before the base: no underflow, no id 0. */
TEST(Hlc, WallBeforeBase) {
  TimeService ts(0, 0);

  ts.set_wall(1000);

  const auto w1 = ts.next_word(ts.wall());
  const auto w2 = ts.next_word(ts.wall());

  EXPECT_NE(w1, 0u);
  EXPECT_EQ(id_to_hlc(w1), B + 2);
  EXPECT_EQ(id_to_hlc(w2), B + 3);
  EXPECT_EQ(id_to_epoch(w1), 0u);
}

/** The time of an id is full: fatal, no wrap into the epoch. */
TEST(HlcDeathTest, TimeOverflow) {
  TimeService ts(1, 1);

  ts.reset(1, B + HLC_MAX_TIME - 1);

  EXPECT_EQ(id_to_hlc(ts.next_word(ts.wall())), B + HLC_MAX_TIME);
  EXPECT_DEATH((void)ts.next_word(ts.wall()), "ran out");
}

/** The wall clock past the last value: the same. */
TEST(HlcDeathTest, WallOverflow) {
  TimeService ts(1, 1);

  EXPECT_DEATH((void)ts.next_word(B + HLC_MAX_TIME + 1), "ran out");
}

TEST(HlcDeathTest, EpochOverflow) {
  TimeService ts(1, 1);

  ts.set_epoch(HLC_MAX_EPOCHS - 1);
  EXPECT_EQ(ts.epoch(), HLC_MAX_EPOCHS - 1);
  EXPECT_DEATH(ts.set_epoch(HLC_MAX_EPOCHS), "doesn't fit");
}

/** Epochs go up, with the time unchanged. A lower one is ignored. */
TEST(Hlc, Epoch) {
  TimeService ts(1, 1);

  ts.set_epoch(5);
  EXPECT_EQ(ts.epoch(), 5u);
  EXPECT_EQ(ts.next_timestamp().epoch(), 5u);

  const auto w = ts.read_word();

  ts.set_epoch(3);
  EXPECT_EQ(ts.epoch(), 5u);
  EXPECT_EQ(ts.read_word(), w);
}

/** set_epoch() racing next: the words of a thread strictly increase as integers.
With the two atomics of Rust a later hlc could get the older epoch. */
TEST(Hlc, EpochRace) {
  TimeService ts(1, 1);
  std::atomic<bool> stop{};
  std::vector<std::thread> threads;
  std::vector<std::vector<uint64_t>> got(4);

  for (int t{}; t < 4; ++t) {
    threads.emplace_back([&, t] {
      while (!stop.load(std::memory_order_acquire)) {
        got[t].push_back(ts.next_word(ts.wall()));
      }
    });
  }

  for (uint64_t e = 2; e < HLC_MAX_EPOCHS; ++e) {
    ts.set_epoch(e);
    std::this_thread::yield();
  }

  stop.store(true, std::memory_order_release);

  for (auto &t : threads) {
    t.join();
  }

  std::vector<uint64_t> all;

  for (auto &v : got) {
    for (size_t i = 1; i < v.size(); ++i) {
      ASSERT_GT(v[i], v[i - 1]);
    }

    all.insert(all.end(), v.begin(), v.end());
  }

  /* Merged by word: the time doesn't go back within an order of ints either. */
  std::sort(all.begin(), all.end());

  EXPECT_EQ(std::adjacent_find(all.begin(), all.end()), all.end());
  EXPECT_EQ(ts.epoch(), HLC_MAX_EPOCHS - 1);
}

/** The clock, timed. */
class HlcPerf : public ::testing::Test {};

TEST_F(HlcPerf, NextTimestamp) {
  for (int n : {1, 2, 4, 8, 16}) {
    TimeService ts(1, 1);

    auto r = ut::bench_mt(std::format("next_timestamp_t{}", n), n, 2000000, [&](int, uint64_t) { ut::keep(ts.next_timestamp()); });

    EXPECT_LT(r.ns_per_op(), 100000);
  }
}

TEST_F(HlcPerf, Wall) {
  TimeService ts(1, 1);

  auto r = ut::bench("clock_wall", 10000000, [&](uint64_t) { ut::keep(ts.wall()); });

  EXPECT_LT(r.ns_per_op(), 5000);
}
