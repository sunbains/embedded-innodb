/*****************************************************************************
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

/** @file include/sync0latch.h
The RW latch of everything but buffer pool blocks and Index::m_lock: a thin wrapper
so that the lock under it, latch::RwLock from deps/latch-rwlock, can be swapped.

What stays on RwLock in sync0rw.h, and why:
 - BufBlock::m_rw_lock: page I/O takes it in one thread and releases it in the I/O
   completion thread, and a latch::RwLock costs too much per page.
 - Index::m_lock: ParallelReader::ScanCtx::index_s_lock() shares one S hold between
   its workers, the last one out releasing what the first one in took.

The rules this lock adds:
 - The thread that took a hold releases it. The library keeps a per-thread record of
   S holds, so an S unlock from another thread is lost and the lock stays S locked for
   good. The debug build asserts this.
 - S nests, in that a nested s_lock() doesn't wait for a waiting writer. Anything else
   that nests (X in X, X in S, S in X) is refused, which ends the process: all of these
   are self deadlocks, on the old lock X in X was the abort of m_no_recursion. The try
   forms return false instead.

The object is one word, the rest of the lock is in the library's pool. It comes from
mmap with no limit on the number of locks, is reused and is never returned to the OS.
Failing to map memory is an _exit(1) in the library. */

#pragma once

#include "innodb0types.h"

#include <latch/latch.hpp>

#include <memory>
#include <new>

class RwLatch {
 public:
  /** No lock yet, as for the structures from mem_alloc() that never get here: create(). */
  RwLatch() noexcept {}

  ~RwLatch() noexcept {}

  RwLatch(const RwLatch &) = delete;
  RwLatch &operator=(const RwLatch &) = delete;

  /** Must be called before anything else, also where the memory has no constructor run. */
  void create() noexcept { new (&m_lock) latch::RwLock; }

  /** The latch must be free, and must be recreated for any further use. */
  void destroy() noexcept {
    ut_a(m_lock.core().is_quiescent());

    std::destroy_at(&m_lock);
  }

  /* noexcept: a refused nesting is a self deadlock, and it ends the process here. */
  void s_lock() noexcept { m_lock.lock_shared(); }

  void x_lock() noexcept { m_lock.lock(); }

  void s_unlock() noexcept {
    ut_ad(s_own());

    m_lock.unlock_shared();
  }

  void x_unlock() noexcept {
    ut_ad(x_own());

    m_lock.unlock();
  }

  /** @return true if it was taken. False if busy, or if this thread holds it in a way that can't nest. */
  [[nodiscard]] bool try_s_lock() noexcept { return m_lock.try_lock_shared(); }

  /** @return true if it was taken. False if busy or if this thread holds it. */
  [[nodiscard]] bool try_x_lock() noexcept { return m_lock.try_lock(); }

#ifdef UNIV_DEBUG
  /** @return true if this thread holds it in S mode. */
  [[nodiscard]] bool s_own() const noexcept {
    auto &core = const_cast<latch::RwLock &>(m_lock).core();
    auto &self = latch::current_thread();

    return core.held_by(self) && !core.is_write_holder(self);
  }

  /** @return true if this thread holds it in X mode. */
  [[nodiscard]] bool x_own() const noexcept {
    return const_cast<latch::RwLock &>(m_lock).core().is_write_holder(latch::current_thread());
  }
#endif /* UNIV_DEBUG */

 private:
  /* In a union so that the lock's lifetime is create() to destroy(). */
  union {
    latch::RwLock m_lock;
  };
};
