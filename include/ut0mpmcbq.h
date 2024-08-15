/*****************************************************************************
Copyright (c) 2017, 2021, Oracle and/or its affiliates.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.

This program is also distributed with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have included with MySQL.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License, version 2.0, for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#pragma once

#include <atomic>
#include <new>
#include <type_traits>

#include "innodb0types.h"

/** @see http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue */
template <typename T>
struct Bounded_channel {
  using Type = T;
  using Pos = std::size_t;

  explicit Bounded_channel(size_t n) noexcept
      : m_ptr(ut_new(sizeof(Cell) * n)),
        m_ring(new (m_ptr) Cell[n]),
        m_capacity(n - 1) {
    /* Should be a power of 2 */
    ut_a(n >= 2 && (n & (n - 1)) == 0);

    for (size_t i{}; i < n; ++i) {
      m_ring[i].m_pos.store(i, std::memory_order_relaxed);
    }

    m_epos.store(0, std::memory_order_relaxed);
    m_dpos.store(0, std::memory_order_relaxed);
  }

  ~Bounded_channel() noexcept {
    call_destructor(m_ring);
    ut_delete(m_ptr);
  }

  [[nodiscard]] bool enqueue(T const &data) noexcept {
    /* m_epos only wraps at MAX(m_epos), instead we use the capacity to
    convert the sequence to an array index. This is why the ring buffer
    must be a size which is a power of 2. This also allows the sequence
    to double as a ticket/lock. */
    auto pos{m_epos.load(std::memory_order_relaxed)};
    Cell* cell;

    for (;;) {
      cell = &m_ring[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const intptr_t diff{(intptr_t)seq - (intptr_t)pos};

      /* If they are the same then it means this cell is empty. */

      if (diff == 0) {
        /* Claim our spot by moving the head. If head isn't at the same place
        as we last checked then that means someone beat us to the punch.
        Weak compare is faster, but can return spurious results which in
        this instance is OK, because it's in the loop */

        if (m_epos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        /* The Bounded_channel is full */
        return false;
      } else {
        pos = m_epos.load(std::memory_order_relaxed);
      }
    }

    cell->m_data = data;

    /* Increment the sequence so that the tail knows it's accessible. */

    cell->m_pos.store(pos + 1, std::memory_order_release);

    return true;
  }

  [[nodiscard]] bool dequeue(T &data) noexcept {
    auto pos{m_dpos.load(std::memory_order_relaxed)};
    Cell* cell;

    for (;;) {
      cell = &m_ring[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const auto diff{(intptr_t)seq - (intptr_t)(pos + 1)};

      if (diff == 0) {
        /* Claim our spot by moving the head. If head isn't at the same place
        as we last checked then that means someone beat us to the punch. Weak
        compare is faster, but can return spurious results. Which in this instance is
        OK, because it's in the loop. */

        if (m_dpos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }

      } else if (diff < 0) {
        /* The Bounded_channel is empty */
        return false;

      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = m_dpos.load(std::memory_order_relaxed);
      }
    }

    data = cell->m_data;

    /* Set the sequence to what the head sequence should be next time around */
    cell->m_pos.store(pos + m_capacity + 1, std::memory_order_release);

    return (true);
  }

  /** @return the capacity of the Bounded_channel */
  [[nodiscard]] auto capacity() const noexcept { return m_capacity + 1; }

  /** @return true if the Bounded_channel is empty. */
  [[nodiscard]] bool empty() const noexcept {
    auto pos{m_dpos.load(std::memory_order_relaxed)};

    for (;;) {
      auto cell{&m_ring[pos & m_capacity]};
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      auto diff{ (intptr_t)seq - (intptr_t)(pos + 1)};

      if (diff == 0) {
        return false;
      } else if (diff < 0) {
        return true;
      } else {
        pos = m_dpos.load(std::memory_order_relaxed);
      }
    }

    ut_error;
    return false;
  }
  
  /** @return true if the Bounded_channel is full. */
  [[nodiscard]] bool full() const noexcept {
    auto pos{m_epos.load(std::memory_order_relaxed)};

    for (;;) {
      auto cell = &m_ring[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const intptr_t diff{(intptr_t)seq - (intptr_t)pos};

      if (diff == 0) {
        return m_epos == pos;
      } else if (diff < 0) {
        /* The Bounded_channel is full */
        return true;
      } else {
        pos = m_epos.load(std::memory_order_relaxed);
      }
    }

    ut_error;
    return false;
  }

 private:
  using Pad = std::byte[std::hardware_constructive_interference_size];

  struct alignas(T) Cell {
    T m_data{};
    std::atomic<Pos> m_pos{};
  };

  void* m_ptr{};
  Pad m_pad0;
  Cell* const m_ring{};
  Pos const m_capacity{};
  Pad m_pad1;
  std::atomic<Pos> m_epos{};
  Pad m_pad2;
  std::atomic<Pos> m_dpos{};
  Pad m_pad3;

  Bounded_channel(Bounded_channel&&) = delete;
  Bounded_channel(const Bounded_channel&) = delete;
  Bounded_channel& operator=(Bounded_channel&&) = delete;
  Bounded_channel& operator=(const Bounded_channel&) = delete;
};

