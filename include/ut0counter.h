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
#include "innodb0types.h"

#ifdef __linux__
#include <utmpx.h>
#endif

// TODO(Rahul): FIX
#ifdef __cpp_lib_hardware_interference_size
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

/** Struct housing an cache-line aligned atomic int*/
struct cache_aligned_atomic_int {
  using value_t = std::atomic<ulint>;
  alignas(hardware_destructive_interference_size) value_t val;

  inline value_t &get_ref() { return val; }
};

/** Concept defining a method constraint. The type should have get_index method.
 @tparam T */
template <typename T>
concept has_get_index = requires(T t) {
  { t.get_index() } -> std::same_as<std::size_t>;
};

/** Struct with a get_index method using the hash of the thread id.
 @tparam total_buckets total number of buckets*/
template <std::int32_t total_buckets>
class index_using_thread_id {
 public:
  explicit index_using_thread_id() : hash{} {}

  std::size_t getIndex() {
    auto buck = hash(std::this_thread::get_id()) % total_buckets;
    return buck;
  }

 private:
  std::hash<std::thread::id> hash;
};

/** Struct with a get_index method using the cpu number of the thread.
 @tparam total_buckets total number of buckets*/
template <std::int32_t number_of_cpu>
struct index_using_cpu_number {
  std::size_t get_index() {
#ifdef __linux__
    return sched_getcpu() % number_of_cpu;
#else
    ut_a(false && "total number of cpus could not be determined");
#endif
  }
};

/** A counter that is sharded into multiple counters to reduce contention.
 @tparam shards total number of counters
 @tparam T type supporting get_index method to select the appropriate shard */
template <std::int32_t shards, typename T>
  requires has_get_index<T>
class sharded_counter {
 public:
  static constexpr std::int32_t SHARDS_COUNT = shards;

  void inc(ulint value = 1) {
    auto index = index_func.get_index();
    counters[index].get_ref().fetch_add(value, std::memory_order_relaxed);
  }

  ulint value() {
    ulint total{0};
    for (auto &counter : counters) {
      total += counter.get_ref().load(std::memory_order_relaxed);
    }

    return total;
  }

 private:
  cache_aligned_atomic_int counters[SHARDS_COUNT];
  T index_func;
};

// TODO(Rahul): Get the CPU count
template <std::int32_t shards>
using per_cpu_sharded_counter_t = sharded_counter<shards, index_using_cpu_number<shards>>;