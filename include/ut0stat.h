/***********************************************************************
Copyright 2026 Sunny Bains

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or Implied.
See the License for the specific language governing permissions and
limitations under the License.

***********************************************************************/

/** @file include/ut0stat.h
Relaxed atomic access to plain fields.

Some fields are read without the latch that protects their writes, on
purpose: statistics, heuristic hints, debug asserts that tolerate a stale
value. A plain access there is a data race in the C++ memory model (and
TSan reports it), even though any value the reader sees is acceptable.

These helpers make such an access formally race free through
std::atomic_ref with memory_order_relaxed. On x86-64 and AArch64 a relaxed
load or store of an aligned scalar compiles to the same instruction as the
plain access, so the hot paths don't get slower. A counter is changed
with an atomic read-modify-write, stat_inc()/stat_dec() or the operators of
Relaxed<T>: never a load and a store, which loses an update when another
thread changes it in between. Relaxed is the right order for a counter:
nothing else is published through it.

Two forms:

 - ut::Relaxed<T>: a field type. Behaves like a T (converts to T, ++, +=,
   =) but every access is a relaxed atomic one. It's copyable (copy is a
   relaxed load + store per field), so the structs holding it (BufPoolStat,
   Table::Stats, SrvStats, ...) stay copy-assignable. They stop being
   trivially copyable: no memset/memcpy on them.

 - stat_load()/stat_store()/...: free functions over a plain T, through
   std::atomic_ref, for fields that must stay plain (e.g. inside BufPage,
   whose blocks are set up in bulk) or where only a few accesses race. */

#pragma once

#include <atomic>
#include <format>
#include <type_traits>

namespace ut {

template <typename T>
concept Stat_type = std::is_trivially_copyable_v<T> && (sizeof(T) <= sizeof(void *));

/** @return x, read with a relaxed atomic load. */
template <Stat_type T>
[[nodiscard]] inline T stat_load(const T &x) noexcept {
  return std::atomic_ref<T>(const_cast<T &>(x)).load(std::memory_order_relaxed);
}

/** Store v into x with a relaxed atomic store. */
template <Stat_type T, typename V>
inline void stat_store(T &x, V v) noexcept {
  std::atomic_ref<T>(x).store(T(v), std::memory_order_relaxed);
}

/** Add n to x with a relaxed atomic RMW (concurrent writers, no lost updates). */
template <Stat_type T>
inline T stat_inc(T &x, T n = 1) noexcept {
  return std::atomic_ref<T>(x).fetch_add(n, std::memory_order_relaxed);
}

/** Subtract n from x with a relaxed atomic RMW. */
template <Stat_type T>
inline T stat_dec(T &x, T n = 1) noexcept {
  return std::atomic_ref<T>(x).fetch_sub(n, std::memory_order_relaxed);
}

/** A T whose accesses are all relaxed atomic ones. For statistics,
heuristic hints and debug counters that are read (or written) without the
latch that serializes the other writers.

++, --, += and -= are atomic read-modify-writes, relaxed: no update is lost
whoever else changes it. */
template <Stat_type T>
class Relaxed {
 public:
  Relaxed() noexcept = default;

  Relaxed(T v) noexcept : m_v(v) {}

  Relaxed(const Relaxed &rhs) noexcept : m_v(rhs.load()) {}

  Relaxed &operator=(const Relaxed &rhs) noexcept {
    store(rhs.load());
    return *this;
  }

  Relaxed &operator=(T v) noexcept {
    store(v);
    return *this;
  }

  operator T() const noexcept { return load(); }

  [[nodiscard]] T load() const noexcept { return m_v.load(std::memory_order_relaxed); }

  void store(T v) noexcept { m_v.store(v, std::memory_order_relaxed); }

  /** Exact atomic add, for counters with unserialized writers that must not lose updates. */
  T fetch_add(T n) noexcept { return m_v.fetch_add(n, std::memory_order_relaxed); }

  /** Exact atomic subtract. */
  T fetch_sub(T n) noexcept { return m_v.fetch_sub(n, std::memory_order_relaxed); }

  Relaxed &operator+=(T n) noexcept {
    (void)fetch_add(n);
    return *this;
  }

  Relaxed &operator-=(T n) noexcept {
    (void)fetch_sub(n);
    return *this;
  }

  T operator++() noexcept { return T(fetch_add(1) + 1); }

  T operator--() noexcept { return T(fetch_sub(1) - 1); }

  T operator++(int) noexcept { return fetch_add(1); }

  T operator--(int) noexcept { return fetch_sub(1); }

 private:
  std::atomic<T> m_v{};
};

}  // namespace ut

/** std::format("{}", relaxed) formats the value. */
template <typename T, typename C>
struct std::formatter<ut::Relaxed<T>, C> : std::formatter<T, C> {
  auto format(const ut::Relaxed<T> &v, auto &ctx) const { return std::formatter<T, C>::format(v.load(), ctx); }
};
