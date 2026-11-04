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

/** @file include/ut0latch.h
A mutex for short critical sections (the lock system, the trx mutex): one
atomic word, the uncontended paths inline (one atomic read-modify-write to
take, one to release), a waiter spins reading the word (test and test and
set: the cache line is not written while it is held), then sleeps on a
futex. Built on atomics only (acquire on taking, release on releasing): the
thread sanitizer sees the happens-before edges they make.

The word: 0 free, 1 held, 2 held and a thread may sleep on it (the release
then wakes one). As Drepper's "mutex3": a woken thread sets 2 again before
it tries, so no sleeper is left behind. */

#pragma once

#include <atomic>
#include <cstdint>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace ut {

/** A spin-wait hint to the CPU (about 140 cycles on Cascade Lake). */
inline void pause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#endif
}

/** Sleep while the word is v (or until woken). */
inline void futex_wait(std::atomic<uint32_t> &word, uint32_t v) noexcept {
  (void)syscall(SYS_futex, reinterpret_cast<uint32_t *>(&word), FUTEX_WAIT_PRIVATE, v, nullptr, nullptr, 0);
}

/** Wake up to n threads sleeping on the word. */
inline void futex_wake(std::atomic<uint32_t> &word, int n) noexcept {
  (void)syscall(SYS_futex, reinterpret_cast<uint32_t *>(&word), FUTEX_WAKE_PRIVATE, n, nullptr, nullptr, 0);
}

/** The reads of the word a waiter spins for (each after a pause) before it
sleeps: a few us, longer than the holds of the lock system. */
constexpr uint32_t LATCH_SPINS = 64;

/** A mutex for short critical sections. */
class Latch {
 public:
  Latch() = default;

  Latch(const Latch &) = delete;
  Latch &operator=(const Latch &) = delete;

  void lock() noexcept {
    uint32_t free{FREE};

    if (m_word.compare_exchange_strong(free, HELD, std::memory_order_acquire, std::memory_order_relaxed)) [[likely]] {
      return;
    }

    lock_wait();
  }

  [[nodiscard]] bool try_lock() noexcept {
    uint32_t free{FREE};

    return m_word.load(std::memory_order_relaxed) == FREE &&
           m_word.compare_exchange_strong(free, HELD, std::memory_order_acquire, std::memory_order_relaxed);
  }

  void unlock() noexcept {
    if (m_word.exchange(FREE, std::memory_order_release) == SLEEPERS) [[unlikely]] {
      futex_wake(m_word, 1);
    }
  }

  /** @return true if some thread holds it (for assertions only). */
  [[nodiscard]] bool is_locked() const noexcept { return m_word.load(std::memory_order_relaxed) != FREE; }

 private:
  static constexpr uint32_t FREE = 0;
  static constexpr uint32_t HELD = 1;
  static constexpr uint32_t SLEEPERS = 2;

  void lock_wait() noexcept {
    for (uint32_t i{}; i < LATCH_SPINS; ++i) {
      pause();

      if (try_lock()) {
        return;
      }
    }

    /* Sleep. SLEEPERS tells the holder to wake one waiter; set it before
    each sleep, and on taking the latch after one (other sleepers). */
    while (m_word.exchange(SLEEPERS, std::memory_order_acquire) != FREE) {
      futex_wait(m_word, SLEEPERS);
    }
  }

  std::atomic<uint32_t> m_word{FREE};
};

}  // namespace ut
