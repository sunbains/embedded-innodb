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

/** @file unit-tests/lock_mt_util.h
The scaffolding of the multi threaded lock system tests (test_lock_mt.cc,
test_lock_mt_db.cc): fake pages, the conflict rules written out apart from
the lock system, the shared started instance. */

#pragma once

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "page0page.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0logger.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <format>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

/** The node of a table lock in the queue of its table, for walking
Table::m_locks: each file that walks it defines it (lock/lock0priv.h). */
struct TableLockGetNode {
  static const UtListNode<Lock> &get_node(const Lock &lock) { return lock.m_table.m_locks; }
};

namespace {


using Clock = std::chrono::steady_clock;

/* The deadlock storms make hundreds of deadlocks and the engine logs each
one, with its transactions and locks, at the Info level. Keep the warnings. */
[[maybe_unused]] const bool quiet = [] {
  logger::level = logger::Warn;
  return true;
}();

/** A space id no real tablespace has: locks on its pages exist only in the lock table. */
constexpr space_id_t FAKE_SPACE = 0xFFFF0;

/** The limit of the deadlock search, LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK
(include/lock0deadlock.h). There is no step limit: the search follows one
edge per waiting transaction, it takes at most depth steps. */
constexpr ulint MAX_DEPTH = LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK;

constexpr ulint SUP = PAGE_HEAP_NO_SUPREMUM;

/** True in a build with a sanitizer: the timing bounds of the lock wait
timeout tests are scaled for it (a loaded host and an instrumented engine
add hundreds of ms to a statement around a precise timeout). */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
constexpr bool SANITIZER = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
constexpr bool SANITIZER = true;
#else
constexpr bool SANITIZER = false;
#endif
#else
constexpr bool SANITIZER = false;
#endif

/** The upper bound of a 1 s lock wait timeout, measured around the statement. */
constexpr double TIMEOUT_1S_MAX = SANITIZER ? 2.5 : 1.2;

/** The scale of the other latency bounds of the wait tests. */
constexpr int SLOW = SANITIZER ? 5 : 1;

/** What an insert requests: rec_insert_check_and_lock(). */
constexpr ulint II = LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION;

/** The largest heap number of a page: 13 bits in the record header. */
constexpr ulint MAX_HEAP_NO = 8191;

/** A page frame with only the header field that lock creation reads: the
number of records in the heap, which sizes the lock bitmaps. */
struct Frame {
  explicit Frame(ulint n_heap) : m_buf(static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE))) {
    memset(m_buf, 0, UNIV_PAGE_SIZE);
    set_n_heap(n_heap);
  }

  ~Frame() { std::free(m_buf); }

  Frame(const Frame &) = delete;
  Frame &operator=(const Frame &) = delete;

  void set_n_heap(ulint n_heap) { mach_write_to_2(m_buf + PAGE_HEADER + PAGE_N_HEAP, n_heap); }

  byte *m_buf;
};

/** A page that exists only in the lock table: a frame and a block for it. */
struct FakePage {
  explicit FakePage(page_no_t page_no, ulint n_heap = 10) : m_frame(n_heap) {
    m_block.m_frame = m_frame.m_buf;
    buf_block_set_state(&m_block, BUF_BLOCK_READY_FOR_USE);
    buf_block_set_file_page(&m_block, PageId{FAKE_SPACE, page_no});
  }

  FakePage(const FakePage &) = delete;
  FakePage &operator=(const FakePage &) = delete;

  [[nodiscard]] PageId id() const { return m_block.get_page_id(); }

  /** @return the lock key of the record heap_no of this page of index: the tests name keys by a page
  and a heap number, the lock system knows only keys. */
  [[nodiscard]] LockKey key(ulint heap_no) const { return LockKey{uint64_t(m_block.get_page_no()) << 16 | heap_no}; }

  [[nodiscard]] LockKey key(const Index *, ulint heap_no) const { return key(heap_no); }

  Frame m_frame;
  BufBlock m_block{};
};

/** The conflict rules of the record locks, written out apart from
Lock::rec_blocks(): must a request for req of one transaction wait for a
lock held (or requested earlier) by another one?
- S is compatible with S.
- An insert intention never makes anything wait.
- An insert intention waits for any lock that covers the gap: a gap lock or
  a plain (next-key) lock, not a lock on the record only.
- Any other gap request never waits: gap locks only stop inserts.
- A request for the record (plain or record only) waits for a lock that
  covers the record: plain or record only, not a gap lock.
On the supremum every lock is a gap lock. */
inline bool conflicts(ulint req, ulint held, bool sup) {
  if ((req & LOCK_MODE_MASK) == LOCK_S && (held & LOCK_MODE_MASK) == LOCK_S) {
    return false;
  } else if (held & LOCK_INSERT_INTENTION) {
    return false;
  }

  const bool held_gap_only = sup || (held & LOCK_GAP);
  const bool held_rec_only = !sup && (held & LOCK_REC_NOT_GAP);

  if (req & LOCK_INSERT_INTENTION) {
    return !held_rec_only;
  } else if (sup || (req & LOCK_GAP)) {
    return false;
  } else {
    return !held_gap_only;
  }
}

/** The compatibility of the table lock modes IS, IX, S, X. */
inline bool table_compatible(ulint a, ulint b) {
  constexpr bool compat[4][4] = {
    {true, true, true, false},
    {true, true, false, false},
    {true, false, true, false},
    {false, false, false, false},
  };

  return compat[a & LOCK_MODE_MASK][b & LOCK_MODE_MASK];
}

/** @return the test table, from the dictionary cache. */
inline Table *get_table(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table;
}

/** Report a latency as a PERF line: ns is the value, shown as ns/op. */
inline void report_ns(const std::string &name, int threads, double ns) {
  ut::report(ut::BenchResult{name, threads, 1, ns / 1e9});
}

/** @return the p-th percentile of v, which it sorts. */
inline double percentile(std::vector<double> &v, double p) {
  if (v.empty()) {
    return 0;
  }

  std::sort(v.begin(), v.end());

  return v[std::min(v.size() - 1, size_t(p / 100.0 * double(v.size())))];
}

/** A DbTest whose started instance is shared by the tests of a suite: the
first test starts it, the end of the suite shuts it down. Each test starts
with an empty test table, purged. */
class SharedDb : public ut::DbTest {
 protected:
  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      s_sys.reset();
    }

    s_dir.reset();
  }

  void SetUp() override {
    if (s_dir == nullptr) {
      s_dir = std::make_unique<ut::TestDir>();
    }

    m_config = ut::make_config(s_dir->m_path);

    /* The storms commit thousands of transactions: flush the log once a second. */
    m_config.m_log.m_flush_at_trx_commit = 2;

    if (s_sys == nullptr) {
      start();
    } else {
      m_sys = std::move(s_sys);
    }

    m_lock_sys = m_sys->m_lock_sys.get();
    m_mutex = &m_sys->m_trx_sys->m_mutex;
    m_table = get_table(*m_sys);
    m_index = m_table->get_first_index();

    clear();
  }

  void TearDown() override { s_sys = std::move(m_sys); }

  /** Delete all the rows of the test table and purge them. */
  void clear() {
    auto trx = begin(IB_TRX_READ_COMMITTED);

    for (const auto &[k, row] : scan(trx)) {
      EXPECT_EQ(remove(trx, k), DB_SUCCESS);
    }

    commit(trx);
    purge();
  }

  /** Run purge until it has nothing left to do. */
  void purge() {
    for (int i = 0; i < 100000 && m_sys->m_trx_sys->m_purge->run() > 0; ++i) {
    }

    (void)m_sys->m_trx_sys->m_purge->run();
  }

  /** @return true if trx waits for a lock. */
  bool waiting(Trx *trx) {
    /* The lock wait state: the trx mutex's. */
    trx_mutex_enter(trx);
    const auto waits = trx->m_que_state == TRX_QUE_LOCK_WAIT;
    trx_mutex_exit(trx);

    return waits;
  }

  /** @return true once trx waits for a lock, false after timeout. */
  bool wait_for_lock_wait(Trx *trx, std::chrono::milliseconds timeout = 10s) {
    const auto end = Clock::now() + timeout;

    while (Clock::now() < end) {
      if (waiting(trx)) {
        return true;
      }

      std::this_thread::sleep_for(1ms);
    }

    return false;
  }

  /** Lock the test table in trx with mode, through a cursor. */
  DbErr lock_table(Trx *trx, CursorLockMode mode) {
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(*m_sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(*m_sys, mode);

    (void)api::Cursor::close(*m_sys, crsr);

    return err;
  }

  /** @return the number of pages with record locks, and of table locks on the test table. */
  std::pair<size_t, size_t> n_locks() {
    x_enter();

    size_t n_pages{};

    for (auto &shard : m_lock_sys->m_key_shards) {
      n_pages += shard.m_locks.size();
    }

    const auto n = std::pair{n_pages, size_t(m_table->m_locks.size())};

    x_exit();

    return n;
  }

  /** The lock system latched X: all of its state, for the model and the
  checks of the tests. */
  void x_enter() { m_lock_sys->x_lock(); }
  void x_exit() { m_lock_sys->x_unlock(); }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;

  LockSys *m_lock_sys{};

  /** The trx sys mutex: the lock wait timeout of the config is set with it. */
  Mutex *m_mutex{};
  Table *m_table{};
  Index *m_index{};
};

/** The thread counts of the perf tests. */
constexpr std::array<int, 5> THREADS{1, 2, 4, 8, 16};

}  // namespace
