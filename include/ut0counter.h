/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/ut0counter.h
Utilities for sharded counter

***********************************************************************/

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <new>
#include <thread>

#ifdef UNIV_LINUX
#include <sched.h>
#endif /* UNIV_LINUX */

#include "innodb0types.h"
#include "ut0logger.h"

namespace ut {

/** Struct housing an cache-line aligned atomic int*/
template <typename T=uint64_t>
struct Counter {
  using Type = T;
  using Value = std::atomic<Type>;

  [[nodiscard]] Value &get_ref() {
    return m_val;
  }

  alignas(hardware_destructive_interference_size) Value m_val;
};

/** Null indexer, doesnt do anything. */
struct Dummy_indexer {
  [[nodiscard]] std::size_t get_index() const {
    ut_error;
    return 0;
  }
};

/** Concept defining a method constraint. The type should have get_index method.
 @tparam T */
template <typename T>
concept has_get_index = requires(T t) {
  { t.get_index() } -> std::same_as<std::size_t>;
};

/** Struct with a get_index method using the hash of the thread id.
 @tparam total_buckets total number of buckets*/
template <std::int32_t Total_buckets>
struct Index_using_thread_id {
  explicit Index_using_thread_id() : m_hash{} {}

  [[nodiscard]] std::size_t get_index() const {
    return m_hash(std::this_thread::get_id()) % Total_buckets;
  }

 private:
  /** Note: This may result in collisions that could cause problems. */
  std::hash<std::thread::id> m_hash;
};

/** Struct with a get_index method using the cpu number of the thread.
 @tparam total_buckets total number of buckets*/
struct Index_using_CPU {
  [[nodiscard]] std::size_t get_index() const {
#ifdef UNIV_LINUX
     uint cpu;
     if (getcpu(&cpu, nullptr) != 0) {
      cpu = 0;
      if (!(m_n_errors++ % 10000)) {
        log_err("getcpu() failed: ", strerror(errno)) 
      }
     }
    return cpu;
#else
    static_assert(false, "Need Linux getcpu() equivalent");
#endif /* UNIV_LINUX */
  }

  mutable int m_n_errors{};
};

/** A counter that is sharded into multiple counters to reduce contention.
 @tparam shards total number of counters
 @tparam T type supporting get_index method to select the appropriate shard
 @tparam Int type of the integer */
template <std::int32_t Shards, typename T = Dummy_indexer, typename Int=uint64_t>
  requires has_get_index<T>
struct Counters {

  void inc(Int value, size_t index) {
    m_counters[index].get_ref().fetch_add(value, std::memory_order_relaxed);
  }

  void inc(Int value = 1) {
    const auto index = m_indexer.get_index() % m_counters.size();

    m_counters[index].get_ref().fetch_add(value, std::memory_order_relaxed);
  }

  [[nodiscard]] uint64_t value() {
    uint64_t total{0};
    for (auto &counter : m_counters) {
      total += counter.get_ref().load(std::memory_order_relaxed);
    }

    return total;
  }

  /** Reset the counter. */
  void clear() {
    for (auto &counter : m_counters) {
     counter.get_ref().store(0, std::memory_order_release);
    }
  }

 private:
  T m_indexer;
  Counter<Int> m_counters[Shards];
};

template <std::int32_t Shards>
using Sharded_counter = Counters<Shards>;

} // namespace ut
