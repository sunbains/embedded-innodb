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

/** @file unit-tests/test_lock_mt.cc
LockSys under concurrency: many threads request, wait for and release
locks, and the invariants are checked against a model. The single threaded
cases are in test_lock.cc.

  LockMt          in-memory lock queues on fake pages, of started
                  transactions without query threads: a thread requests a
                  lock as rec_lock_slow() / lock_table() do and polls for
                  the grant (Srv::suspend_user_thread() sleeps instead). A
                  checker thread validates the queues while they run, a
                  model checks each grant against what the others hold.
  LockMtDb        transactions of a started instance through the API: real
                  waits and wakeups, deadlocks, commits and rollbacks, page
                  splits and purge with gap locks held, isolation levels,
                  lock wait timeouts (1 s, 0 s) and interrupts.
  LockMtPerf      the in-memory lock queues, timed against the thread count.
  LockMtDbPerf    the hand-over of a row between threads, timed. */

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
Table::m_locks: each file that walks it defines it (lock/lock0lock.cc,
lock/lock0validator.cc). */
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

/** The limits of the deadlock search: LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK
and LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK, lock/lock0lock.cc:32 and :36. */
constexpr ulint MAX_STEPS = 1000000;
constexpr ulint MAX_DEPTH = 200;

/** LOCK_EXCEED_MAX_DEPTH, the result of LockSys::deadlock_recursive() when
the search is too deep or too long (lock/lock0lock.cc). */
constexpr ulint EXCEED = 3;

constexpr ulint SUP = PAGE_HEAP_NO_SUPREMUM;

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
bool conflicts(ulint req, ulint held, bool sup) {
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

/** Lock::reset(): the lock is granted or cancelled. (Lock::reset() and
LockSys::rec_has_expl() are inline functions defined in lock/lock0lock.cc
only: they can't be called from here, these are copies.) */
void reset_wait(Lock *lock) {
  lock->m_type_mode = LockMode(lock->m_type_mode & ~LOCK_WAIT);
  lock->m_trx->m_wait_lock = nullptr;
}

/** LockSys::rec_has_expl(): a granted lock of trx on the record at least
as strong as precise_mode. */
const Lock *has_expl(LockSys &lock_sys, PageId page_id, ulint precise_mode, ulint heap_no, const Trx *trx) {
  auto it = lock_sys.m_rec_locks.find(page_id);

  if (it == lock_sys.m_rec_locks.end()) {
    return nullptr;
  }

  for (auto lock : it->second) {
    if (lock->m_trx == trx && lock->rec_is_nth_bit_set(heap_no) &&
        Lock::mode_stronger_or_eq(lock->mode(), LockMode(precise_mode & LOCK_MODE_MASK)) && !lock->is_waiting() &&
        (!lock->rec_is_not_gap() || (precise_mode & LOCK_REC_NOT_GAP) || heap_no == SUP) &&
        (!lock->rec_is_gap() || (precise_mode & LOCK_GAP) || heap_no == SUP) && !lock->rec_is_insert_intention()) {
      return lock;
    }
  }

  return nullptr;
}

/** The compatibility of the table lock modes IS, IX, S, X. */
bool table_compatible(ulint a, ulint b) {
  constexpr bool compat[4][4] = {
    {true, true, true, false},
    {true, true, false, false},
    {true, false, true, false},
    {false, false, false, false},
  };

  return compat[a & LOCK_MODE_MASK][b & LOCK_MODE_MASK];
}

/** @return the test table, from the dictionary cache. */
Table *get_table(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table;
}

/** Report a latency as a PERF line: ns is the value, shown as ns/op. */
void report_ns(const std::string &name, int threads, double ns) {
  ut::report(ut::BenchResult{name, threads, 1, ns / 1e9});
}

/** @return the p-th percentile of v, which it sorts. */
double percentile(std::vector<double> &v, double p) {
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
    mutex_enter(m_mutex);
    const auto waits = trx->m_que_state == TRX_QUE_LOCK_WAIT;
    mutex_exit(m_mutex);

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
    mutex_enter(m_mutex);
    const auto n = std::pair{m_lock_sys->m_rec_locks.size(), size_t(m_table->m_locks.size())};
    mutex_exit(m_mutex);

    return n;
  }

  static inline std::unique_ptr<System> s_sys;
  static inline std::unique_ptr<ut::TestDir> s_dir;

  LockSys *m_lock_sys{};
  Mutex *m_mutex{};
  Table *m_table{};
  Index *m_index{};
};

/** In-memory lock queues on fake pages, of started transactions. */
class LockMt : public SharedDb {
 protected:
  enum Got { GRANTED, WAIT, DEADLOCK };

  /** A lock the model says a transaction holds. */
  struct Held {
    const Trx *m_trx;
    page_no_t m_page;
    ulint m_heap_no;
    ulint m_mode;
    uint64_t m_seq;
  };

  void TearDown() override {
    report_errors();
    EXPECT_EQ(n_fake_pages(), 0u) << "record locks left on the fake pages";
    SharedDb::TearDown();
  }

  /** A record lock request of trx, made as LockSys::rec_lock_slow() and
  rec_enqueue_waiting() make it, without the query thread; an insert
  intention as rec_insert_check_and_lock() does: no lock unless it waits.
  The trx sys mutex is held.
  @return GRANTED, WAIT (trx->m_wait_lock is the request), or DEADLOCK: the
  request is cancelled and trx is the victim. */
  Got rec_request(Trx *trx, const BufBlock &block, ulint mode, ulint heap_no) {
    const auto page_id = block.get_page_id();

    if (mode & LOCK_INSERT_INTENTION) {
      if (m_lock_sys->rec_other_has_conflicting(page_id, LockMode(mode), heap_no, trx) == nullptr) {
        return GRANTED;
      }
    } else if (has_expl(*m_lock_sys, page_id, mode, heap_no, trx) != nullptr) {
      return GRANTED;
    } else if (m_lock_sys->rec_other_has_conflicting(page_id, LockMode(mode), heap_no, trx) == nullptr) {
      (void)m_lock_sys->rec_add_to_queue(LockMode(LOCK_REC | mode), &block, heap_no, m_index, trx);
      return GRANTED;
    }

    auto lock = m_lock_sys->rec_create(LockMode(mode | LOCK_WAIT), &block, heap_no, m_index, trx);

    if (m_lock_sys->deadlock_occurs(lock, trx)) {
      reset_wait(lock);
      lock->rec_reset_nth_bit(heap_no);
      return DEADLOCK;
    }

    if (trx->m_wait_lock == nullptr) {
      /* Another transaction was the deadlock victim and the request was granted. */
      return GRANTED;
    }

    trx->m_que_state = TRX_QUE_LOCK_WAIT;
    trx->m_was_chosen_as_deadlock_victim = false;

    return WAIT;
  }

  /** Poll until the lock wait of trx ends. on_grant() runs with the trx
  sys mutex held when it ends with a grant.
  @return true if the request was granted, false if trx was chosen as a
  deadlock victim, or if the wait lasted longer than limit: a lost grant,
  the test fails and the wait is cancelled. */
  template <typename F>
  bool await(Trx *trx, F &&on_grant, std::chrono::milliseconds limit = 20s) {
    const auto end = Clock::now() + limit;

    for (int i{};; ++i) {
      mutex_enter(m_mutex);

      if (trx->m_wait_lock == nullptr) {
        const auto victim = trx->m_was_chosen_as_deadlock_victim;

        trx->m_was_chosen_as_deadlock_victim = false;

        if (!victim) {
          on_grant();
        }

        mutex_exit(m_mutex);

        return !victim;
      }

      if (Clock::now() > end) {
        const auto queue = dump(trx->m_wait_lock);

        m_lock_sys->cancel_waiting_and_release(trx->m_wait_lock);
        mutex_exit(m_mutex);

        ADD_FAILURE() << "trx " << trx->m_id << " waited for more than " << limit.count() << " ms, the queue:" << queue;

        return false;
      }

      mutex_exit(m_mutex);

      if (i < 100) {
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(20us);
      }
    }
  }

  bool await(Trx *trx) {
    return await(trx, [] {});
  }

  /** @return the locks of the queue of lock, for a failure message. */
  std::string dump(const Lock *lock) {
    std::string s;

    if (lock->type() == LOCK_TABLE) {
      for (auto l : m_table->m_locks) {
        s += std::format(" [trx {} mode {:#x}]", l->m_trx->m_id, Lock_mode_type(l->precise_mode()));
      }
    } else if (auto it = m_lock_sys->m_rec_locks.find(lock->page_id()); it != m_lock_sys->m_rec_locks.end()) {
      for (auto l : it->second) {
        s += std::format(" [trx {} mode {:#x} heap_no", l->m_trx->m_id, Lock_mode_type(l->precise_mode()));

        for (ulint h{}; h < l->rec_get_n_bits(); ++h) {
          if (l->rec_is_nth_bit_set(h)) {
            s += std::format(" {}", h);
          }
        }

        s += "]";
      }
    }

    return s;
  }

  /** Note a broken invariant; the trx sys mutex is held. */
  void error(std::string what) {
    if (m_errors.size() < 1000) {
      m_errors.push_back(std::move(what));
    }
  }

  /** Fail the test with the broken invariants found. */
  void report_errors() {
    mutex_enter(m_mutex);
    auto errors = std::move(m_errors);
    m_errors.clear();
    mutex_exit(m_mutex);

    for (size_t i{}; i < std::min<size_t>(errors.size(), 20); ++i) {
      ADD_FAILURE() << errors[i];
    }

    EXPECT_TRUE(errors.empty()) << errors.size() << " broken invariants";
  }

  /** The model: trx was granted mode on (page, heap_no): no transaction
  holds a lock it conflicts with. An insert intention is checked only
  against the locks held since its request: a gap lock requested after it
  does not wait for it (gap requests never wait) and does not stop it. The
  trx sys mutex is held. */
  void model_grant(const Trx *trx, page_no_t page, ulint heap_no, ulint mode, uint64_t since) {
    for (const auto &h : m_held) {
      if (h.m_trx != trx && h.m_page == page && h.m_heap_no == heap_no && conflicts(mode, h.m_mode, heap_no == SUP) &&
          (!(mode & LOCK_INSERT_INTENTION) || h.m_seq <= since)) {
        error(std::format(
          "trx {} granted {:#x} on page {} heap_no {} while trx {} holds {:#x}",
          trx->m_id, mode, page, heap_no, h.m_trx->m_id, h.m_mode
        ));
      }
    }

    if (!(mode & LOCK_INSERT_INTENTION)) {
      m_held.push_back(Held{trx, page, heap_no, mode, ++m_seq});
    }
  }

  /** The model: trx is about to release its locks. The trx sys mutex is held. */
  void model_drop(const Trx *trx) {
    std::erase_if(m_held, [trx](const Held &h) { return h.m_trx == trx; });
  }

  /** Validate the queues of the fake pages and of the test table: no two
  granted locks of different transactions conflict (a granted insert
  intention only marks an insert that was let in); every waiting lock has a
  conflicting lock ahead of it (else its grant was lost); a waiting lock is
  the wait lock of its transaction. The trx sys mutex is held. */
  void check_queues() {
    std::vector<const Lock *> q;

    for (const auto &[page_id, locks] : m_lock_sys->m_rec_locks) {
      if (page_id.m_space_id != FAKE_SPACE) {
        continue;
      }

      ulint n_bits{};

      for (auto l : locks) {
        n_bits = std::max<ulint>(n_bits, l->rec_get_n_bits());
      }

      for (ulint h{}; h < n_bits; ++h) {
        q.clear();

        for (auto l : locks) {
          if (l->rec_is_nth_bit_set(h)) {
            q.push_back(l);
          }
        }

        if (!q.empty()) {
          check_queue(q, h, page_id.m_page_no);
        }
      }
    }

    q.clear();

    for (auto l : m_table->m_locks) {
      q.push_back(l);
    }

    for (size_t i{}; i < q.size(); ++i) {
      auto l = q[i];

      if (l->is_waiting()) {
        if (l->m_trx->m_wait_lock != l) {
          error(std::format("waiting table lock {:#x} is not the wait lock of trx {}", Lock_mode_type(l->precise_mode()), l->m_trx->m_id));
        }

        bool blocked{};

        for (size_t j{}; j < i && !blocked; ++j) {
          blocked = q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode());
        }

        if (!blocked) {
          error(std::format("table lock {:#x} of trx {} waits for nothing: a lost grant", Lock_mode_type(l->precise_mode()), l->m_trx->m_id));
        }
      } else {
        for (size_t j{}; j < i; ++j) {
          if (q[j]->is_waiting() && q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode())) {
            error(std::format("granted table lock {:#x} of trx {} is behind a waiting {:#x} of trx {}: it barged",
              Lock_mode_type(l->precise_mode()), l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id));
          }
        }

        for (size_t j{}; j < q.size(); ++j) {
          if (j != i && !q[j]->is_waiting() && q[j]->m_trx != l->m_trx && !table_compatible(l->mode(), q[j]->mode())) {
            error(std::format("granted table locks {:#x} of trx {} and {:#x} of trx {} conflict",
              Lock_mode_type(l->precise_mode()), l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id));
          }
        }
      }
    }
  }

  /** check_queues() of the locks q on one record. */
  void check_queue(const std::vector<const Lock *> &q, ulint heap_no, page_no_t page) {
    const bool sup = heap_no == SUP;

    for (size_t i{}; i < q.size(); ++i) {
      auto l = q[i];
      const auto mode = Lock_mode_type(l->precise_mode()) & ~LOCK_WAIT;

      if (l->is_waiting()) {
        if (l->m_trx->m_wait_lock != l) {
          error(std::format("page {} heap_no {}: waiting lock {:#x} is not the wait lock of trx {}", page, heap_no, mode, l->m_trx->m_id));
        }

        bool blocked{};

        for (size_t j{}; j < i && !blocked; ++j) {
          blocked = q[j]->m_trx != l->m_trx && conflicts(mode, q[j]->precise_mode() & ~LOCK_WAIT, sup);
        }

        if (!blocked) {
          error(std::format("page {} heap_no {}: lock {:#x} of trx {} waits for nothing: a lost grant", page, heap_no, mode, l->m_trx->m_id));
        }
      } else if (!(mode & LOCK_INSERT_INTENTION)) {
        for (size_t j{}; j < q.size(); ++j) {
          if (j != i && !q[j]->is_waiting() && q[j]->m_trx != l->m_trx && conflicts(mode, q[j]->precise_mode(), sup)) {
            error(std::format(
              "page {} heap_no {}: granted locks {:#x} of trx {} and {:#x} of trx {} conflict",
              page, heap_no, mode, l->m_trx->m_id, Lock_mode_type(q[j]->precise_mode()), q[j]->m_trx->m_id
            ));
          }
        }
      }
    }
  }

  /** A thread that runs check_queues() every period until stopped. */
  struct Checker {
    Checker(LockMt *test, std::chrono::microseconds period) : m_thread([this, test, period] {
      while (!m_stop.load(std::memory_order_acquire)) {
        mutex_enter(test->m_mutex);
        test->check_queues();
        mutex_exit(test->m_mutex);
        ++m_n;
        std::this_thread::sleep_for(period);
      }
    }) {}

    ~Checker() { stop(); }

    void stop() {
      m_stop.store(true, std::memory_order_release);

      if (m_thread.joinable()) {
        m_thread.join();
      }
    }

    std::atomic<bool> m_stop{};
    std::atomic<uint64_t> m_n{};
    std::thread m_thread;
  };

  /** @return the number of fake pages with record locks. */
  size_t n_fake_pages() {
    mutex_enter(m_mutex);

    const auto n = std::count_if(m_lock_sys->m_rec_locks.begin(), m_lock_sys->m_rec_locks.end(), [](const auto &e) {
      return e.first.m_space_id == FAKE_SPACE;
    });

    mutex_exit(m_mutex);

    return size_t(n);
  }

  /** Release the locks of trx in memory and commit it. */
  void finish(Trx *trx) {
    mutex_enter(m_mutex);
    model_drop(trx);
    mutex_exit(m_mutex);
    commit(trx);
  }

  /** The model: the record locks the threads hold. Protected by the trx sys mutex. */
  std::vector<Held> m_held;

  /** The order of the grants of the model. Protected by the trx sys mutex. */
  uint64_t m_seq{};

  /** The broken invariants found. Protected by the trx sys mutex. */
  std::vector<std::string> m_errors;
};

/** Threads lock records of two pages at random with every record lock mode
- plain (next-key), gap, record only and insert intention, S and X - on the
supremum and on the first and last user records of the page, 1 to 4 locks
per transaction, then commit or roll back. Upgrades (S then X) and
deadlocks happen. A checker validates the queues while they run, and the
model checks each grant against the locks the others hold: no two granted
locks conflict, no insert into a locked gap, no waiting lock that nothing
blocks (a lost grant). Every request ends granted or as a deadlock victim,
and no lock remains. */
TEST_F(LockMt, RecStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_ROUNDS = 300;

  FakePage p1(1);
  FakePage p2(2);
  const std::array<FakePage *, 2> pages{&p1, &p2};

  /* The supremum, the first user record, two more, and the last record of the page (n_heap 10). */
  const std::array<ulint, 5> heaps{SUP, PAGE_HEAP_NO_USER_LOW, 3, 4, 9};
  const std::array<ulint, 7> modes{
    LOCK_S, LOCK_X, LOCK_S | LOCK_GAP, LOCK_X | LOCK_GAP, LOCK_S | LOCK_REC_NOT_GAP, LOCK_X | LOCK_REC_NOT_GAP, II
  };
  const std::array<ulint, 3> sup_modes{LOCK_S, LOCK_X, II};

  std::atomic<uint64_t> n_req{};
  std::atomic<uint64_t> n_granted{};
  std::atomic<uint64_t> n_victims{};
  std::atomic<uint64_t> n_waits{};

  Checker checker(this, 100us);
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t + 1);

      for (int r{}; r < N_ROUNDS; ++r) {
        auto trx = begin();
        bool victim{};
        const int n = 1 + int(rng() % 4);

        for (int j{}; j < n && !victim; ++j) {
          auto page = pages[rng() % pages.size()];
          const auto heap_no = heaps[rng() % heaps.size()];
          const auto mode = heap_no == SUP ? sup_modes[rng() % sup_modes.size()] : modes[rng() % modes.size()];
          const auto page_no = page->id().m_page_no;

          ++n_req;

          mutex_enter(m_mutex);

          const auto since = m_seq;
          const auto got = rec_request(trx, page->m_block, mode, heap_no);

          if (got == GRANTED) {
            model_grant(trx, page_no, heap_no, mode, since);
          }

          mutex_exit(m_mutex);

          if (got == WAIT) {
            ++n_waits;
            victim = !await(trx, [&] { model_grant(trx, page_no, heap_no, mode, since); });
          } else {
            victim = got == DEADLOCK;
          }

          ++(victim ? n_victims : n_granted);
        }

        for (int k = int(rng() % 20); k > 0; --k) {
          std::this_thread::yield();
        }

        mutex_enter(m_mutex);
        model_drop(trx);
        mutex_exit(m_mutex);

        if (victim || rng() % 4 == 0) {
          rollback(trx);
        } else {
          commit(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  mutex_enter(m_mutex);
  check_queues();
  mutex_exit(m_mutex);

  EXPECT_EQ(n_req.load(), n_granted.load() + n_victims.load());
  EXPECT_GT(n_waits.load(), 0u) << "no request waited: the storm tests nothing";
  EXPECT_GT(checker.m_n.load(), 0u);

  std::printf("RecStorm: %lu requests, %lu waits, %lu deadlock victims, %lu checks\n",
    (unsigned long)n_req.load(), (unsigned long)n_waits.load(), (unsigned long)n_victims.load(), (unsigned long)checker.m_n.load());
}

/** Cycles of 2 to 32 transactions formed concurrently: each holds a record,
and its thread requests the record of the next one, all at the same time.
Exactly one transaction per cycle is the victim, the others are granted in
turn. The victim follows the weight rule of deadlock_recursive(): the
requester that closed the cycle, unless the transaction that waits for it
(the one before it in the cycle) weighs less (Trx::weight_cmp(): locks and
undo records), then that one. Each transaction weighs 2 + extra locks + 1
for its waiting lock, the extra locks a permutation: no ties. */
TEST_F(LockMt, DeadlockCycles) {
  constexpr std::array<int, 8> sizes{2, 2, 3, 3, 4, 5, 8, 32};

  FakePage ring(1, 40);
  std::mt19937 rng(42);
  std::map<int, int> n_by_size;

  for (int round{}; round < 40; ++round) {
    const int n = sizes[rng() % sizes.size()];
    std::vector<int> extra(n);
    std::vector<Trx *> trxs;

    std::iota(extra.begin(), extra.end(), 0);
    std::shuffle(extra.begin(), extra.end(), rng);

    for (int i{}; i < n; ++i) {
      trxs.push_back(begin());
    }

    mutex_enter(m_mutex);

    for (int i{}; i < n; ++i) {
      EXPECT_EQ(rec_request(trxs[i], ring.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), GRANTED);

      for (int e{}; e < extra[i]; ++e) {
        (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(1000 + i * 64 + e)}, LOCK_X, 2, 16, m_index, trxs[i]);
      }
    }

    mutex_exit(m_mutex);

    std::barrier sync(n);
    std::atomic<int> closer{-1};
    std::vector<int> victim(n);
    std::vector<std::thread> threads;

    for (int i{}; i < n; ++i) {
      threads.emplace_back([&, i] {
        auto trx = trxs[i];

        sync.arrive_and_wait();

        mutex_enter(m_mutex);

        const auto got = rec_request(trx, ring.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + (i + 1) % n);

        /* The request that closes the cycle finds the deadlock: it is the
        victim, or it chooses another one, which has not seen it yet (that
        needs the mutex). */
        if (got == DEADLOCK || std::any_of(trxs.begin(), trxs.end(), [](const Trx *t) { return t->m_was_chosen_as_deadlock_victim; })) {
          EXPECT_EQ(closer.exchange(i), -1) << "two requests closed the cycle";
        }

        mutex_exit(m_mutex);

        victim[i] = got == DEADLOCK || (got == WAIT && !await(trx));

        if (victim[i]) {
          rollback(trx);
        } else {
          commit(trx);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    ++n_by_size[n];

    const auto n_victims = std::count(victim.begin(), victim.end(), 1);

    ASSERT_EQ(n_victims, 1) << "cycle of " << n;
    ASSERT_GE(closer.load(), 0);

    const int c = closer.load();
    const int p = (c + n - 1) % n;
    const int v = int(std::find(victim.begin(), victim.end(), 1) - victim.begin());

    if (v == c) {
      EXPECT_GT(extra[p], extra[c]) << "the requester was the victim, but the one waiting for it weighs less";
    } else {
      EXPECT_EQ(v, p) << "the victim is neither the requester nor the transaction waiting for it";
      EXPECT_LT(extra[p], extra[c]) << "the other transaction was the victim, but it weighs more";
    }

    EXPECT_EQ(n_fake_pages(), 0u);
  }

  for (auto [n, count] : n_by_size) {
    std::printf("DeadlockCycles: %d cycles of %d\n", count, n);
  }
}

/** The depth limit of the deadlock search (200): a chain of waits whose
last waiting transaction is at depth 200 is searched to its end, no
deadlock; one transaction more and the search gives up: the requester is
rolled back as if it had deadlocked. */
TEST_F(LockMt, DeadlockDepthEdge) {
  for (auto [n, expected] : {std::pair{int(MAX_DEPTH) + 2, false}, std::pair{int(MAX_DEPTH) + 3, true}}) {
    std::vector<Trx *> t;

    for (int i{}; i < n; ++i) {
      t.push_back(begin());
    }

    mutex_enter(m_mutex);

    for (int i{}; i < n; ++i) {
      (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(i)}, LOCK_X, 2, 16, m_index, t[i]);
    }

    /* t[i] waits for the record of t[i + 1] (at depth i of the search), the last does not wait. */
    for (int i = 1; i + 1 < n; ++i) {
      (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, page_no_t(i + 1)}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, t[i]);
      t[i]->m_que_state = TRX_QUE_LOCK_WAIT;
    }

    auto req = m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, t[0]);

    EXPECT_EQ(m_lock_sys->deadlock_occurs(req, t[0]), expected) << n << " transactions";

    for (auto trx : t) {
      m_lock_sys->release_off_trx_sys_mutex(trx);
    }

    for (auto trx : t) {
      trx->m_wait_lock = nullptr;
      trx->m_que_state = TRX_QUE_RUNNING;
    }

    mutex_exit(m_mutex);

    for (auto trx : t) {
      commit(trx);
    }
  }
}

/** The step limit (1,000,000 transactions visited) and the depth limit of
the deadlock search, at their edges: a chain a -> b -> c (a waits for b,
which waits for c). The search counts a step at each transaction it enters
and gives up at an edge when it is past either limit: started with the
step count one short of the limit it gives up at b's edge, two short it
does not; started at depth 200 it gives up, at 199 it does not. (A real
search would need a million waiting transactions to reach the step limit.) */
TEST_F(LockMt, SearchLimits) {
  auto a = begin();
  auto b = begin();
  auto c = begin();

  mutex_enter(m_mutex);

  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LOCK_X, 2, 16, m_index, b);
  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 2}, LOCK_X, 2, 16, m_index, c);
  (void)m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 2}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, b);
  b->m_que_state = TRX_QUE_LOCK_WAIT;

  auto req = m_lock_sys->rec_create_low(PageId{FAKE_SPACE, 1}, LockMode(LOCK_X | LOCK_WAIT), 2, 16, m_index, a);

  const auto search = [&](ulint cost, ulint depth) {
    for (auto t : {a, b, c}) {
      t->m_deadlock_mark = 0;
    }

    return m_lock_sys->deadlock_recursive(a, a, req, &cost, depth);
  };

  EXPECT_EQ(search(MAX_STEPS - 1, 0), EXCEED);
  EXPECT_EQ(search(MAX_STEPS - 2, 0), 0u);
  EXPECT_EQ(search(0, MAX_DEPTH), EXCEED);
  EXPECT_EQ(search(0, MAX_DEPTH - 1), 0u);

  for (auto t : {a, b, c}) {
    m_lock_sys->release_off_trx_sys_mutex(t);
    t->m_wait_lock = nullptr;
    t->m_que_state = TRX_QUE_RUNNING;
  }

  mutex_exit(m_mutex);

  for (auto t : {a, b, c}) {
    commit(t);
  }
}

/** A record held in X with 16 requests queued behind it in a known order,
X and S mixed: when the holder commits they are granted in queue order,
compatible S requests together, each exactly once, never with an
incompatible lock ahead still held; each waiter commits as soon as it is
granted, racing the grants of the next ones. 200 rounds. */
TEST_F(LockMt, HandoffFifo) {
  constexpr int N = 16;
  constexpr ulint HEAP_NO = 2;

  FakePage page(1);
  std::mt19937 rng(7);

  for (int round{}; round < 200; ++round) {
    std::vector<ulint> modes(N);
    std::vector<Trx *> trxs;
    std::vector<int> granted(N);
    std::vector<int> released(N);

    for (auto &m : modes) {
      m = (rng() % 3 == 0 ? LOCK_X : LOCK_S) | LOCK_REC_NOT_GAP;
    }

    auto holder = begin();

    mutex_enter(m_mutex);
    ASSERT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, HEAP_NO), GRANTED);
    mutex_exit(m_mutex);

    for (int i{}; i < N; ++i) {
      trxs.push_back(begin());
    }

    std::atomic<int> ticket{};
    std::vector<std::thread> threads;

    for (int i{}; i < N; ++i) {
      threads.emplace_back([&, i] {
        while (ticket.load(std::memory_order_acquire) != i) {
          std::this_thread::yield();
        }

        mutex_enter(m_mutex);
        const auto got = rec_request(trxs[i], page.m_block, modes[i], HEAP_NO);
        mutex_exit(m_mutex);

        ticket.store(i + 1, std::memory_order_release);

        EXPECT_EQ(got, WAIT) << i;

        const auto ok = await(trxs[i], [&] {
          for (int j{}; j < i; ++j) {
            /* trxs[j] is alive until released[j] is set. */
            if (!released[j] && trxs[j]->m_wait_lock != nullptr) {
              error(std::format("round {}: request {} granted before request {} ahead of it", round, i, j));
            }

            if ((modes[i] & LOCK_MODE_MASK) == LOCK_X || (modes[j] & LOCK_MODE_MASK) == LOCK_X) {
              if (!released[j]) {
                error(std::format("round {}: request {} granted while request {} ahead holds an incompatible lock", round, i, j));
              }
            }
          }

          ++granted[i];
        });

        EXPECT_TRUE(ok) << i;

        mutex_enter(m_mutex);
        released[i] = 1;
        mutex_exit(m_mutex);

        commit(trxs[i]);
      });
    }

    while (ticket.load(std::memory_order_acquire) != N) {
      std::this_thread::yield();
    }

    commit(holder);

    for (auto &t : threads) {
      t.join();
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(granted[i], 1) << "round " << round << " request " << i;
    }
  }
}

/** 256 transactions on one record at once: X, X record only, insert
intentions, S gap locks and plain S. Each request is granted exactly once
and the model and the checker find no conflict: a large queue of one record
is granted correctly. */
TEST_F(LockMt, ManyOnOne) {
  constexpr int N = 256;
  constexpr ulint HEAP_NO = 2;
  const std::array<ulint, 8> modes{LOCK_X, LOCK_X | LOCK_REC_NOT_GAP, II, LOCK_S | LOCK_GAP, LOCK_S, LOCK_S, LOCK_S | LOCK_REC_NOT_GAP, LOCK_S};

  FakePage page(1);
  std::vector<Trx *> trxs;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
  }

  std::atomic<int> n_granted{};
  std::atomic<int> n_victims{};
  std::barrier sync(N);
  Checker checker(this, 500us);
  std::vector<std::thread> threads;

  for (int i{}; i < N; ++i) {
    threads.emplace_back([&, i] {
      auto trx = trxs[i];
      const auto mode = modes[i % modes.size()];

      sync.arrive_and_wait();

      mutex_enter(m_mutex);

      const auto since = m_seq;
      const auto got = rec_request(trx, page.m_block, mode, HEAP_NO);

      if (got == GRANTED) {
        model_grant(trx, 1, HEAP_NO, mode, since);
      }

      mutex_exit(m_mutex);

      bool ok = got == GRANTED;

      if (got == WAIT) {
        ok = await(trx, [&] { model_grant(trx, 1, HEAP_NO, mode, since); }, 60s);
      }

      ++(ok ? n_granted : n_victims);

      std::this_thread::sleep_for(50us);

      finish(trx);
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  /* Only upgrades deadlock, and there are none: one request per transaction. */
  EXPECT_EQ(n_granted.load(), N);
  EXPECT_EQ(n_victims.load(), 0);
}

/** One transaction holds 100,000 record locks on 2,000 pages, more than
the 1,000 locks a release frees before it lets the trx sys mutex go
(LOCK_RELEASE_TRX_SYS_MUTEX_INTERVAL), and the record with the largest heap
number of a page with 8,192 records. 8 threads wait for records of it. Its
commit releases the locks in three slices with the mutex free in between,
while the checker runs: every waiter is granted, no lock is left. */
TEST_F(LockMt, BigTrx) {
  constexpr page_no_t N_PAGES = 2000;
  constexpr ulint N_HEAPS = 50;
  constexpr int N_WAITERS = 8;

  std::vector<std::unique_ptr<FakePage>> pages;

  for (page_no_t p{}; p < N_PAGES; ++p) {
    pages.push_back(std::make_unique<FakePage>(10 + p, N_HEAPS + 2));
  }

  FakePage big(5, MAX_HEAP_NO + 1);

  auto holder = begin();

  mutex_enter(m_mutex);

  for (auto &page : pages) {
    for (ulint h = 2; h < N_HEAPS + 2; ++h) {
      ASSERT_EQ(rec_request(holder, page->m_block, LOCK_X | LOCK_REC_NOT_GAP, h), GRANTED);
    }
  }

  ASSERT_EQ(rec_request(holder, big.m_block, LOCK_X | LOCK_REC_NOT_GAP, MAX_HEAP_NO), GRANTED);

  const auto last = holder->m_trx_locks.back();

  EXPECT_GE(last->rec_get_n_bits(), MAX_HEAP_NO + 1);
  EXPECT_TRUE(last->rec_is_nth_bit_set(MAX_HEAP_NO));

  ASSERT_EQ(rec_request(holder, big.m_block, LOCK_X, SUP), GRANTED);

  const auto n_structs = holder->m_trx_locks.size();

  mutex_exit(m_mutex);

  EXPECT_GT(n_structs, 2 * 1000u) << "the release must cross the interval twice";

  std::atomic<int> n_waiting{};
  std::atomic<int> n_granted{};
  std::vector<std::thread> threads;
  Checker checker(this, 5ms);

  for (int t{}; t < N_WAITERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      auto trx = begin();
      auto &block = t == 0 ? big.m_block : pages[rng() % N_PAGES]->m_block;
      const auto heap_no = t == 0 ? MAX_HEAP_NO : 2 + rng() % N_HEAPS;

      mutex_enter(m_mutex);
      const auto got = rec_request(trx, block, LOCK_S | LOCK_REC_NOT_GAP, heap_no);
      mutex_exit(m_mutex);

      EXPECT_EQ(got, WAIT);
      ++n_waiting;

      if (await(trx)) {
        ++n_granted;
      }

      commit(trx);
    });
  }

  while (n_waiting.load() < N_WAITERS) {
    std::this_thread::sleep_for(1ms);
  }

  const auto start = Clock::now();

  commit(holder);

  const std::chrono::duration<double, std::milli> ms = Clock::now() - start;

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  EXPECT_EQ(n_granted.load(), N_WAITERS);
  std::printf("BigTrx: commit of %lu locks in %lu structs: %.1f ms\n", (unsigned long)(N_PAGES * N_HEAPS + 2), (unsigned long)n_structs, ms.count());
}

/** A lock bitmap is sized for the records the page has when the lock is
made, plus a margin of 64. After the page grew, a request of the same
transaction for a record past the end of the bitmap gets a new lock with a
larger bitmap, not a bit past the end of the old one; a record at the last
bit of the old bitmap still goes in the old lock. A waiter for the new
record is granted when the holder commits. */
TEST_F(LockMt, BitmapGrow) {
  FakePage page(1, 10);

  auto a = begin();
  auto b = begin();

  mutex_enter(m_mutex);

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 5), GRANTED);

  auto first = a->m_trx_locks.back();
  const auto n_bits = first->rec_get_n_bits();

  ASSERT_LT(n_bits, 200u);

  page.m_frame.set_n_heap(300);

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, n_bits - 1), GRANTED);
  EXPECT_EQ(a->m_trx_locks.size(), 1u) << "the last bit of the old bitmap goes in the old lock";
  EXPECT_TRUE(first->rec_is_nth_bit_set(n_bits - 1));

  ASSERT_EQ(rec_request(a, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 250), GRANTED);
  ASSERT_EQ(a->m_trx_locks.size(), 2u) << "a new lock for a record past the old bitmap";

  auto second = a->m_trx_locks.back();

  EXPECT_GT(second->rec_get_n_bits(), 250u);
  EXPECT_TRUE(second->rec_is_nth_bit_set(250));
  EXPECT_FALSE(first->rec_is_nth_bit_set(250));

  mutex_exit(m_mutex);

  auto result = std::async(std::launch::async, [&] {
    mutex_enter(m_mutex);
    const auto got = rec_request(b, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 250);
    mutex_exit(m_mutex);

    return got == WAIT && await(b);
  });

  ASSERT_TRUE(wait_for_lock_wait(b));

  commit(a);

  EXPECT_TRUE(result.get());

  commit(b);
}

/** Threads take table locks on the test table through the API (real
waits), IS, IX, S and X, one or two per transaction, the second an upgrade
of the first (IS to IX, S or X, IX to X, S to X), and commit. Upgrades
deadlock: every request ends granted or as a deadlock victim. The checker
validates the table queue while they run: no two granted locks of
different transactions conflict, a granted lock is never behind a waiting
incompatible lock of another transaction (no barging), every waiting lock
is blocked by a lock ahead. The model checks each grant against the locks
the others hold (noted after the grant, dropped before the commit). */
TEST_F(LockMt, TableStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_ROUNDS = 150;
  const std::array<std::pair<LockMode, LockMode>, 8> plans{{
    {LOCK_IS, LOCK_IS}, {LOCK_IX, LOCK_IX}, {LOCK_IS, LOCK_IX}, {LOCK_IS, LOCK_S},
    {LOCK_IS, LOCK_X}, {LOCK_IX, LOCK_X}, {LOCK_S, LOCK_X}, {LOCK_S, LOCK_S},
  }};

  /* A lock the model says a thread holds: its transaction is round r of thread t. */
  struct TableHeld {
    const Trx *m_trx;
    LockMode m_mode;
    int m_t;
    int m_r;
  };

  /* A grant that conflicts with a lock of round r of thread t. A deadlock
  victim is rolled back inside lock_table() (row_handle_errors()), before
  the model drops its locks: a grant that conflicts only with a victim's
  locks is no error. Protected by the trx sys mutex, as held and victims. */
  struct Violation {
    int m_t;
    int m_r;
    std::string m_what;
  };

  std::vector<TableHeld> held;
  std::vector<Violation> violations;
  std::set<std::pair<int, int>> victims;
  std::atomic<uint64_t> n_req{};
  std::atomic<uint64_t> n_ok{};
  std::atomic<uint64_t> n_victims{};

  Checker checker(this, 100us);
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(100 + t);

      for (int r{}; r < N_ROUNDS; ++r) {
        auto trx = begin();
        const auto plan = plans[rng() % plans.size()];
        bool victim{};

        for (auto mode : {plan.first, plan.second}) {
          ++n_req;

          const auto err = lock_table(trx, CursorLockMode(mode));

          if (err != DB_SUCCESS) {
            EXPECT_EQ(err, DB_DEADLOCK);
            victim = true;
            ++n_victims;

            mutex_enter(m_mutex);
            victims.emplace(t, r);
            mutex_exit(m_mutex);

            break;
          }

          ++n_ok;

          mutex_enter(m_mutex);

          for (const auto &h : held) {
            if (h.m_trx != trx && !table_compatible(mode, h.m_mode)) {
              violations.push_back(Violation{h.m_t, h.m_r,
                std::format("trx {} granted table lock {} while trx {} holds {}", trx->m_id, int(mode), h.m_trx->m_id, int(h.m_mode))});
            }
          }

          held.push_back(TableHeld{trx, mode, t, r});
          mutex_exit(m_mutex);
        }

        for (int k = int(rng() % 20); k > 0; --k) {
          std::this_thread::yield();
        }

        mutex_enter(m_mutex);
        std::erase_if(held, [trx](const auto &h) { return h.m_trx == trx; });
        mutex_exit(m_mutex);

        if (victim) {
          rollback(trx);
        } else {
          commit(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  checker.stop();

  mutex_enter(m_mutex);

  for (const auto &v : violations) {
    if (!victims.contains(std::pair{v.m_t, v.m_r})) {
      error(v.m_what);
    }
  }

  mutex_exit(m_mutex);

  EXPECT_EQ(n_req.load(), n_ok.load() + n_victims.load());
  EXPECT_EQ(n_locks().second, 0u) << "table locks left";

  std::printf("TableStorm: %lu requests, %lu deadlock victims\n", (unsigned long)n_req.load(), (unsigned long)n_victims.load());
}

/** Transactions of a started instance, through the API. */
class LockMtDb : public SharedDb {
 protected:
  /** The lock wait timeout of the instance, for the life of a Timeout,
  restored after. Set with the trx sys mutex held, when no thread waits for
  a lock: the lock timeout thread reads it with the mutex, a waiting
  thread after it took the mutex once its wait ended. */
  struct Timeout {
    Timeout(LockMtDb *test, std::chrono::seconds timeout) : m_test(test) {
      m_old = set(timeout);
    }

    ~Timeout() { (void)set(m_old); }

    std::chrono::seconds set(std::chrono::seconds timeout) {
      mutex_enter(m_test->m_mutex);
      const auto old = m_test->m_sys->m_config.m_lock.m_wait_timeout;
      m_test->m_sys->m_config.m_lock.m_wait_timeout = timeout;
      mutex_exit(m_test->m_mutex);

      return old;
    }

    LockMtDb *m_test;
    std::chrono::seconds m_old{};
  };

  /** Position crsr on the row with key k. @return DB_SUCCESS or DB_RECORD_NOT_FOUND */
  DbErr seek(api::Cursor *crsr, uint32_t k) {
    auto key = crsr->clust_search_tuple_create();
    int res{-1};
    auto err = key->write_u32(0, k);

    if (err == DB_SUCCESS) {
      err = crsr->moveto(*m_sys, key, IB_CUR_GE, &res);
    }

    api::Tuple::destroy(key);

    if ((err == DB_SUCCESS && res != 0) || err == DB_END_OF_INDEX) {
      err = DB_RECORD_NOT_FOUND;
    }

    return err;
  }

  /** a += 1 in the row k, read and written under an X lock.
  @param[out] old the value of a read */
  DbErr incr(Trx *trx, uint32_t k, uint32_t *old = nullptr) {
    auto &sys = *m_sys;
    api::Cursor *crsr{};

    if (auto err = api::cursor_open_table(sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IX);

    if (err == DB_SUCCESS) {
      err = crsr->set_lock_mode(sys, IB_LOCK_X);
    }

    if (err == DB_SUCCESS) {
      err = seek(crsr, k);
    }

    if (err == DB_SUCCESS) {
      auto old_tpl = crsr->clust_read_tuple_create();
      auto new_tpl = crsr->clust_read_tuple_create();
      uint32_t a{};

      err = crsr->read_row(sys, old_tpl);

      if (err == DB_SUCCESS) {
        err = old_tpl->read_u32(1, &a);
      }

      if (err == DB_SUCCESS) {
        err = new_tpl->copy(old_tpl);
      }

      if (err == DB_SUCCESS) {
        err = new_tpl->write_u32(1, a + 1);
      }

      if (err == DB_SUCCESS) {
        err = crsr->update_row(sys, old_tpl, new_tpl);
      }

      if (err == DB_SUCCESS && old != nullptr) {
        *old = a;
      }

      api::Tuple::destroy(old_tpl);
      api::Tuple::destroy(new_tpl);
    }

    (void)api::Cursor::close(sys, crsr);

    return err;
  }

  /** A locking read (S) of the keys in [lo, hi]: from lo up to the first
  row after hi, which it locks too. @param[out] keys the keys in [lo, hi] */
  DbErr range(Trx *trx, uint32_t lo, uint32_t hi, std::vector<uint32_t> *keys) {
    auto &sys = *m_sys;
    api::Cursor *crsr{};

    keys->clear();

    if (auto err = api::cursor_open_table(sys, ut::TABLE, trx, &crsr); err != DB_SUCCESS) {
      return err;
    }

    auto err = crsr->lock(sys, IB_LOCK_IS);

    if (err == DB_SUCCESS) {
      err = crsr->set_lock_mode(sys, IB_LOCK_S);
    }

    if (err == DB_SUCCESS) {
      auto key = crsr->clust_search_tuple_create();
      int res{};

      err = key->write_u32(0, lo);

      if (err == DB_SUCCESS) {
        err = crsr->moveto(sys, key, IB_CUR_GE, &res);
      }

      api::Tuple::destroy(key);
    }

    auto tpl = crsr->clust_read_tuple_create();

    while (err == DB_SUCCESS) {
      uint32_t k{};

      err = crsr->read_row(sys, tpl);

      if (err == DB_SUCCESS) {
        err = tpl->read_u32(0, &k);
      }

      if (err != DB_SUCCESS || k > hi) {
        break;
      }

      keys->push_back(k);
      tpl = tpl->clear();
      err = crsr->next(sys);
    }

    api::Tuple::destroy(tpl);
    (void)api::Cursor::close(sys, crsr);

    return err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND ? DB_SUCCESS : err;
  }

  /** @return the committed rows. */
  ut::Rows rows() {
    auto trx = begin(IB_TRX_READ_COMMITTED);
    auto rows = scan(trx);

    commit(trx);

    return rows;
  }

  /** @return the a of row k, committed. */
  uint32_t value(uint32_t k) {
    const auto all = rows();
    auto it = all.find(k);

    EXPECT_NE(it, all.end()) << "no row " << k;

    return it == all.end() ? 0 : it->second.m_a;
  }

  /** @return the result of the operation of trx in another thread, which
  must end within timeout: if it does not, trx is interrupted (so that the
  thread ends) and the test fails. */
  template <typename T>
  T get(std::future<T> &result, Trx *trx, std::chrono::milliseconds timeout = 10s) {
    if (result.wait_for(timeout) != std::future_status::ready) {
      ADD_FAILURE() << "the operation of trx " << trx->m_id << " did not end in " << timeout.count() << " ms";
      api::trx_interrupt(trx);
    }

    return result.get();
  }
};

/** Read-modify-write of random rows: 8 threads, each transaction adds 1
to 2 or 3 of 16 rows in random order, deadlocks happen and the victims roll
back. Every committed increment is there at the end and no other: no
update is lost (the X locks exclude each other through the real waits and
wakeups), no deadlock victim leaves a change. No lock is left. */
TEST_F(LockMtDb, UpdateStorm) {
  constexpr int N_THREADS = 8;
  constexpr int N_TRXS = 120;
  constexpr uint32_t N_ROWS = 16;

  ut::Rows initial;

  for (uint32_t k = 1; k <= N_ROWS; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  std::vector<std::array<uint32_t, N_ROWS + 1>> counts(N_THREADS);
  std::atomic<int> n_deadlocks{};
  std::atomic<int> n_other{};
  std::vector<std::thread> threads;

  for (int t{}; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      auto &count = counts[t];

      count.fill(0);

      for (int i{}; i < N_TRXS; ++i) {
        auto trx = begin();
        std::vector<uint32_t> keys;
        DbErr err{DB_SUCCESS};

        for (int n = 2 + int(rng() % 2); n > 0; --n) {
          keys.push_back(1 + rng() % N_ROWS);
        }

        for (auto k : keys) {
          if ((err = incr(trx, k)) != DB_SUCCESS) {
            break;
          }
        }

        if (err == DB_SUCCESS) {
          commit(trx);

          for (auto k : keys) {
            ++count[k];
          }
        } else {
          ++(err == DB_DEADLOCK ? n_deadlocks : n_other);

          if (err != DB_DEADLOCK) {
            ADD_FAILURE() << "incr: " << err;
          }

          rollback(trx);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  const auto all = rows();

  for (uint32_t k = 1; k <= N_ROWS; ++k) {
    uint32_t expected{};

    for (const auto &c : counts) {
      expected += c[k];
    }

    EXPECT_EQ(all.at(k).m_a, expected) << "row " << k;
  }

  EXPECT_EQ(n_other.load(), 0);
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";

  std::printf("UpdateStorm: %d deadlocks\n", n_deadlocks.load());
}

/** One row held by an update, 16 updates of it queued behind in a known
order: when the holder commits they are granted in queue order, one at a
time, each reading the value the one ahead wrote: FIFO, no lost update and
no wakeup lost in the hand-over from each committing thread to the next. */
TEST_F(LockMtDb, HotRowFifo) {
  constexpr int N = 16;

  load({{1, ut::Row{1, 0, ""}}});

  for (int round{}; round < 3; ++round) {
    const auto base = value(1);
    auto holder = begin();

    ASSERT_EQ(incr(holder, 1), DB_SUCCESS);

    std::vector<Trx *> trxs;
    std::vector<uint32_t> seen(N);
    std::vector<DbErr> errs(N, DB_ERROR);
    std::vector<std::thread> threads;

    for (int i{}; i < N; ++i) {
      auto trx = begin();

      trxs.push_back(trx);

      threads.emplace_back([&, i, trx] {
        errs[i] = incr(trx, 1, &seen[i]);

        if (errs[i] == DB_SUCCESS) {
          commit(trx);
        } else {
          rollback(trx);
        }
      });

      EXPECT_TRUE(wait_for_lock_wait(trxs[i])) << i;
    }

    commit(holder);

    for (auto &t : threads) {
      t.join();
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(errs[i], DB_SUCCESS) << i;
      EXPECT_EQ(seen[i], base + 1 + i) << "request " << i << " was not granted in queue order";
    }

    EXPECT_EQ(value(1), base + 1 + N);
  }
}

/** 256 transactions wait for one row at once, each to add 1 to it: when
the holder commits each is granted once and sees a different value, the
row ends 257 higher: 256 threads suspended and woken in turn. */
TEST_F(LockMtDb, ManyWaiters) {
  constexpr int N = 256;

  load({{1, ut::Row{1, 0, ""}}});

  auto holder = begin();

  ASSERT_EQ(incr(holder, 1), DB_SUCCESS);

  std::vector<Trx *> trxs;
  std::vector<uint32_t> seen(N);
  std::vector<DbErr> errs(N, DB_ERROR);
  std::vector<std::thread> threads;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
  }

  for (int i{}; i < N; ++i) {
    threads.emplace_back([&, i] {
      errs[i] = incr(trxs[i], 1, &seen[i]);

      if (errs[i] == DB_SUCCESS) {
        commit(trxs[i]);
      } else {
        rollback(trxs[i]);
      }
    });
  }

  for (int i{}; i < N; ++i) {
    EXPECT_TRUE(wait_for_lock_wait(trxs[i], 30s)) << i;
  }

  commit(holder);

  for (auto &t : threads) {
    t.join();
  }

  std::sort(seen.begin(), seen.end());

  for (int i{}; i < N; ++i) {
    EXPECT_EQ(errs[i], DB_SUCCESS) << i;
    EXPECT_EQ(seen[i], uint32_t(i + 1)) << "two transactions read the same value: a lost update";
  }

  EXPECT_EQ(value(1), uint32_t(N + 1));
}

/** Cycles of 2, 3 and 8 transactions through the API, made at the same
time from their threads: each updates its own row, then the next one's.
Exactly one gets DB_DEADLOCK per cycle and is rolled back; the others get
their rows in turn and commit: all rows but the victim's end up 2 higher
in all, the victim's two changes are gone. */
TEST_F(LockMtDb, DeadlockRounds) {
  ut::Rows initial;

  for (uint32_t k = 1; k <= 8; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  for (auto [n, rounds] : {std::pair{2, 8}, std::pair{3, 5}, std::pair{8, 3}}) {
    for (int r{}; r < rounds; ++r) {
      uint32_t before{};

      for (const auto &[k, row] : rows()) {
        before += row.m_a;
      }

      std::barrier sync(n);
      std::vector<DbErr> errs(n, DB_ERROR);
      std::vector<std::thread> threads;

      for (int i{}; i < n; ++i) {
        threads.emplace_back([&, i] {
          auto trx = begin();
          auto err = incr(trx, 1 + i);

          sync.arrive_and_wait();

          if (err == DB_SUCCESS) {
            err = incr(trx, 1 + (i + 1) % n);
          }

          if (err == DB_SUCCESS) {
            commit(trx);
          } else {
            rollback(trx);
          }

          errs[i] = err;
        });
      }

      for (auto &t : threads) {
        t.join();
      }

      EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_DEADLOCK), 1) << "cycle of " << n;
      EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_SUCCESS), n - 1) << "cycle of " << n;

      uint32_t after{};

      for (const auto &[k, row] : rows()) {
        after += row.m_a;
      }

      EXPECT_EQ(after - before, uint32_t(2 * n - 2)) << "cycle of " << n;
    }
  }

  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** A transaction holds 8 rows, an update waits for each: it rolls back
(round 0) or commits (round 1), every waiter is granted and reads what the
end of the holder left: the old value after the rollback, the holder's
after the commit. */
TEST_F(LockMtDb, ReleaseWakesAll) {
  constexpr int N = 8;
  ut::Rows initial;

  for (uint32_t k = 1; k <= N; ++k) {
    initial[k] = ut::Row{k, 10, ""};
  }

  load(initial);

  for (int round{}; round < 2; ++round) {
    const bool commit_holder = round == 1;
    const auto base = value(1);
    auto holder = begin();

    for (uint32_t k = 1; k <= N; ++k) {
      ASSERT_EQ(incr(holder, k), DB_SUCCESS);
    }

    std::vector<Trx *> trxs;
    std::vector<uint32_t> seen(N);
    std::vector<std::future<DbErr>> results;

    for (int i{}; i < N; ++i) {
      auto trx = begin();

      trxs.push_back(trx);
      results.push_back(std::async(std::launch::async, [&, i, trx] { return incr(trx, 1 + i, &seen[i]); }));
    }

    for (int i{}; i < N; ++i) {
      EXPECT_TRUE(wait_for_lock_wait(trxs[i])) << i;
    }

    if (commit_holder) {
      commit(holder);
    } else {
      rollback(holder);
    }

    for (int i{}; i < N; ++i) {
      EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS) << i;
      EXPECT_EQ(seen[i], uint32_t(base + (commit_holder ? 1 : 0))) << i;
      commit(trxs[i]);
    }
  }

  for (const auto &[k, row] : rows()) {
    /* Round 0: +1 by the waiter; round 1: +1 by the holder, +1 by the waiter. */
    EXPECT_EQ(row.m_a, 13u) << k;
  }
}

/** A rollback to a savepoint undoes the changes after it but keeps their
locks (api::savepoint_rollback()): an update waiting for a row changed after
the savepoint still waits after the rollback to it, and when the holder
commits it reads the value from before the savepoint. */
TEST_F(LockMtDb, SavepointKeepsLocks) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto t = begin();

  ASSERT_EQ(incr(t, 1), DB_SUCCESS);

  api::savepoint_take(t, "sp", 2);

  ASSERT_EQ(incr(t, 2), DB_SUCCESS);

  auto w = begin();
  uint32_t seen{99};
  auto result = std::async(std::launch::async, [&] { return incr(w, 2, &seen); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  EXPECT_EQ(api::savepoint_rollback(*m_sys, t, "sp", 2), DB_SUCCESS);

  EXPECT_EQ(result.wait_for(300ms), std::future_status::timeout) << "the rollback to the savepoint released the lock";
  EXPECT_TRUE(waiting(w));

  commit(t);

  EXPECT_EQ(get(result, w), DB_SUCCESS);
  EXPECT_EQ(seen, 0u) << "the change after the savepoint was not undone";

  commit(w);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);
}

/** An insert after a savepoint, and an insert of the same key by another
transaction that waits for it (the duplicate check). The rollback to the
savepoint removes the row: the waiter is woken, but the gap where the row
was stays locked by the first transaction (its lock on the row is inherited
by the next one as a gap lock), so the second insert waits until it
commits, then succeeds. One row, no hang. */
TEST_F(LockMtDb, SavepointInsert) {
  load({{40, ut::Row{40, 0, ""}}, {60, ut::Row{60, 0, ""}}});

  auto t = begin();

  ASSERT_EQ(incr(t, 40), DB_SUCCESS);

  api::savepoint_take(t, "sp", 2);

  ASSERT_EQ(insert(t, ut::Row{50, 1, "t"}), DB_SUCCESS);

  auto w = begin();
  auto result = std::async(std::launch::async, [&] { return insert(w, ut::Row{50, 2, "w"}); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  EXPECT_EQ(api::savepoint_rollback(*m_sys, t, "sp", 2), DB_SUCCESS);

  EXPECT_EQ(result.wait_for(300ms), std::future_status::timeout) << "the insert got into the gap the first transaction still locks";

  commit(t);

  EXPECT_EQ(get(result, w), DB_SUCCESS);

  commit(w);

  const auto all = rows();

  ASSERT_TRUE(all.contains(50));
  EXPECT_EQ(all.at(50).m_a, 2u);
  EXPECT_EQ(all.size(), 3u);
}

/** Locking range reads (S) against concurrent inserts and deletes, with
rows big enough that the inserts split pages and the deletes, once purged,
merge them, while the gap locks are held. Each reader reads its range twice
in one transaction. REPEATABLE READ: the two reads are the same, no
phantom (the next-key locks and their inheritance through splits, merges
and purge keep the gaps closed). READ COMMITTED: the rows of the first read
are all in the second (they stay locked), new ones may appear. */
TEST_F(LockMtDb, PhantomStorm) {
  constexpr int N_WRITERS = 4;
  constexpr int N_READERS = 4;
  constexpr uint32_t N_KEYS = 2000;
  constexpr auto DURATION = 2500ms;

  ut::Rows initial;

  for (uint32_t k = 10; k <= N_KEYS; k += 10) {
    initial[k] = ut::Row{k, k, ut::blob(k, 50)};
  }

  load(initial);

  std::atomic<bool> stop{};
  std::atomic<int> n_inserts{};
  std::atomic<int> n_deletes{};
  std::atomic<int> n_reads{};
  std::atomic<int> n_new_rc{};
  std::atomic<int> n_deadlocks{};
  std::vector<std::thread> threads;

  for (int t{}; t < N_WRITERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      std::vector<uint32_t> mine;

      while (!stop.load()) {
        auto trx = begin();
        DbErr err;

        if (!mine.empty() && rng() % 3 == 0) {
          const auto i = rng() % mine.size();

          err = remove(trx, mine[i]);

          if (err == DB_SUCCESS) {
            mine[i] = mine.back();
            mine.pop_back();
            ++n_deletes;
          }
        } else {
          uint32_t k = 1 + rng() % (N_KEYS + 50);

          k += k % 10 == 0 ? 1 : 0;
          err = insert(trx, ut::Row{k, k, ut::blob(k, 300)});

          if (err == DB_SUCCESS) {
            mine.push_back(k);
            ++n_inserts;
          }
        }

        if (err == DB_SUCCESS || err == DB_DUPLICATE_KEY || err == DB_RECORD_NOT_FOUND) {
          commit(trx);
        } else {
          /* The insert or the delete did not happen. */
          EXPECT_EQ(err, DB_DEADLOCK);
          ++n_deadlocks;
          rollback(trx);
        }
      }
    });
  }

  for (int t{}; t < N_READERS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(100 + t);
      const auto level = t % 2 == 0 ? IB_TRX_REPEATABLE_READ : IB_TRX_READ_COMMITTED;

      while (!stop.load()) {
        const uint32_t lo = rng() % (N_KEYS + 100);
        const uint32_t hi = lo + 60;
        std::vector<uint32_t> first;
        std::vector<uint32_t> second;
        auto trx = begin(level);

        auto err = range(trx, lo, hi, &first);

        if (err == DB_SUCCESS) {
          std::this_thread::sleep_for(std::chrono::microseconds(rng() % 3000));
          err = range(trx, lo, hi, &second);
        }

        if (err != DB_SUCCESS) {
          EXPECT_EQ(err, DB_DEADLOCK);
          ++n_deadlocks;
          rollback(trx);
          continue;
        }

        commit(trx);
        ++n_reads;

        if (level == IB_TRX_REPEATABLE_READ) {
          EXPECT_EQ(first, second) << "a phantom in [" << lo << ", " << hi << "] under REPEATABLE READ";
        } else {
          EXPECT_TRUE(std::includes(second.begin(), second.end(), first.begin(), first.end()))
            << "a row read and locked under READ COMMITTED disappeared from [" << lo << ", " << hi << "]";
          n_new_rc += second.size() > first.size() ? 1 : 0;
        }
      }
    });
  }

  std::this_thread::sleep_for(DURATION);
  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(n_reads.load(), 0);
  EXPECT_GT(n_inserts.load(), 0);
  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";

  std::printf("PhantomStorm: %d inserts, %d deletes, %d range reads, %d deadlocks, %d READ COMMITTED reads saw new rows\n",
    n_inserts.load(), n_deletes.load(), n_reads.load(), n_deadlocks.load(), n_new_rc.load());
}

/** A locking read holds a lock on a delete-marked row, then purge removes
the row: the gap stays locked (the lock is inherited by the next row as a
gap lock), inserts on both sides of the removed row wait until the reader
commits. */
TEST_F(LockMtDb, PurgedGap) {
  load({{10, ut::Row{10, 10, ""}}, {20, ut::Row{20, 20, ""}}, {30, ut::Row{30, 30, ""}}});

  auto d = begin();

  ASSERT_EQ(remove(d, 20), DB_SUCCESS);
  commit(d);

  auto reader = begin(IB_TRX_REPEATABLE_READ);
  std::vector<uint32_t> keys;

  ASSERT_EQ(range(reader, 15, 25, &keys), DB_SUCCESS);
  EXPECT_TRUE(keys.empty());

  purge();

  std::vector<Trx *> trxs;
  std::vector<std::future<DbErr>> results;

  for (uint32_t k : {15u, 25u}) {
    trxs.push_back(begin());
    results.push_back(std::async(std::launch::async, [&, k, trx = trxs.back()] { return insert(trx, ut::Row{k, k, ""}); }));
  }

  for (auto trx : trxs) {
    EXPECT_TRUE(wait_for_lock_wait(trx, 3s)) << "an insert into the gap of a purged row the reader locked did not wait";
  }

  commit(reader);

  for (size_t i{}; i < trxs.size(); ++i) {
    EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS);
    commit(trxs[i]);
  }

  EXPECT_EQ(rows().size(), 4u);
}

/** A locking read of an empty table locks the supremum of the root page
(REPEATABLE READ): an insert waits until the reader commits; under READ
COMMITTED the read locks no gap and the insert does not wait. */
TEST_F(LockMtDb, EmptyTableGap) {
  for (auto level : {IB_TRX_REPEATABLE_READ, IB_TRX_READ_COMMITTED}) {
    auto reader = begin(level);
    std::vector<uint32_t> keys;

    ASSERT_EQ(range(reader, 0, 1000, &keys), DB_SUCCESS);
    EXPECT_TRUE(keys.empty());

    auto writer = begin();
    auto result = std::async(std::launch::async, [&] { return insert(writer, ut::Row{5, 5, ""}); });

    if (level == IB_TRX_REPEATABLE_READ) {
      EXPECT_TRUE(wait_for_lock_wait(writer, 3s)) << "an insert into an empty table read under REPEATABLE READ did not wait";
      commit(reader);
      EXPECT_EQ(get(result, writer), DB_SUCCESS);
    } else {
      EXPECT_EQ(get(result, writer, 3s), DB_SUCCESS) << "an insert waited for a READ COMMITTED read";
      commit(reader);
    }

    rollback(writer);
    purge();
  }
}

/** Table locks through the API: a holds S, b waits for X, then 8
transactions request IS, which S allows but the waiting X does not: they
all wait behind it (no barging). a commits: b is granted, the IS requests
still wait; b commits: they are all granted. */
TEST_F(LockMtDb, TableNoBarging) {
  constexpr int N = 8;

  auto a = begin();
  auto b = begin();

  ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);

  auto rb = std::async(std::launch::async, [&] { return lock_table(b, IB_LOCK_X); });

  ASSERT_TRUE(wait_for_lock_wait(b));

  std::vector<Trx *> trxs;
  std::vector<std::future<DbErr>> results;

  for (int i{}; i < N; ++i) {
    trxs.push_back(begin());
    results.push_back(std::async(std::launch::async, [&, trx = trxs.back()] { return lock_table(trx, IB_LOCK_IS); }));
  }

  for (auto trx : trxs) {
    EXPECT_TRUE(wait_for_lock_wait(trx)) << "an IS request barged past a waiting X";
  }

  commit(a);

  EXPECT_EQ(get(rb, b), DB_SUCCESS);

  std::this_thread::sleep_for(100ms);

  for (auto &r : results) {
    EXPECT_EQ(r.wait_for(0ms), std::future_status::timeout) << "IS granted while X is held";
  }

  commit(b);

  for (int i{}; i < N; ++i) {
    EXPECT_EQ(get(results[i], trxs[i]), DB_SUCCESS);
    commit(trxs[i]);
  }
}

/** Two transactions with S on the table both upgrade to X: a deadlock.
Exactly one gets DB_DEADLOCK and is rolled back, the other gets X. */
TEST_F(LockMtDb, TableUpgradeDeadlock) {
  for (int round{}; round < 4; ++round) {
    auto a = begin();
    auto b = begin();

    ASSERT_EQ(lock_table(a, IB_LOCK_S), DB_SUCCESS);
    ASSERT_EQ(lock_table(b, IB_LOCK_S), DB_SUCCESS);

    std::barrier sync(2);
    std::array<DbErr, 2> errs{DB_ERROR, DB_ERROR};
    std::array<Trx *, 2> trxs{a, b};
    std::vector<std::thread> threads;

    for (int i{}; i < 2; ++i) {
      threads.emplace_back([&, i] {
        sync.arrive_and_wait();
        errs[i] = lock_table(trxs[i], IB_LOCK_X);

        if (errs[i] == DB_SUCCESS) {
          commit(trxs[i]);
        } else {
          rollback(trxs[i]);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_DEADLOCK), 1);
    EXPECT_EQ(std::count(errs.begin(), errs.end(), DB_SUCCESS), 1);
  }
}

/** A cycle through a table lock and a record lock: a and b each update a
row (IX on the table, X on the row), b asks for S on the table and waits
for a's IX, a asks for b's row. The deadlock search goes from a's record
lock wait to b's table lock wait and back to a: exactly one of them gets
DB_DEADLOCK, the other completes. */
TEST_F(LockMtDb, MixedDeadlock) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  for (int round{}; round < 3; ++round) {
    auto a = begin();
    auto b = begin();

    ASSERT_EQ(incr(a, 1), DB_SUCCESS);
    ASSERT_EQ(incr(b, 2), DB_SUCCESS);

    auto rb = std::async(std::launch::async, [&] { return lock_table(b, IB_LOCK_S); });

    ASSERT_TRUE(wait_for_lock_wait(b));

    auto ra = std::async(std::launch::async, [&] { return incr(a, 2); });

    const auto ea = get(ra, a);
    const auto eb = get(rb, b);

    EXPECT_EQ(int(ea == DB_DEADLOCK) + int(eb == DB_DEADLOCK), 1) << "a: " << ea << " b: " << eb;

    /* The survivor waited for the victim's rollback, then got its lock. */
    EXPECT_TRUE(ea == DB_SUCCESS || eb == DB_SUCCESS);

    if (ea == DB_SUCCESS) {
      rollback(b);
      commit(a);
    } else {
      rollback(a);
      commit(b);
    }
  }

  EXPECT_EQ(n_locks(), std::pair(size_t(0), size_t(0))) << "locks left";
}

/** A transaction waiting for a row is interrupted (api::trx_interrupt()):
its wait ends with DB_INTERRUPTED within the period of the lock timeout
thread (1 s), its statement is rolled back but not the transaction: it
still holds the row it updated before, a third transaction waits for that.
Then an interrupt races the holder's commit: the waiter either gets the row
or DB_INTERRUPTED without changing it, never both. */
TEST_F(LockMtDb, InterruptWait) {
  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);
  ASSERT_EQ(incr(w, 2), DB_SUCCESS);

  auto result = std::async(std::launch::async, [&] { return incr(w, 1); });

  ASSERT_TRUE(wait_for_lock_wait(w));

  const auto start = Clock::now();

  api::trx_interrupt(w);

  EXPECT_EQ(get(result, w, 5s), DB_INTERRUPTED);
  EXPECT_LT(Clock::now() - start, 3s);

  auto t3 = begin();
  auto r3 = std::async(std::launch::async, [&] { return incr(t3, 2); });

  EXPECT_TRUE(wait_for_lock_wait(t3)) << "the interrupt released the locks of the earlier statements";

  rollback(w);

  EXPECT_EQ(get(r3, t3), DB_SUCCESS);
  commit(t3);
  commit(h);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);

  /* The race. */
  for (int round{}; round < 2; ++round) {
    const auto before = value(1);
    auto h2 = begin();
    auto w2 = begin();

    ASSERT_EQ(incr(h2, 1), DB_SUCCESS);

    auto r = std::async(std::launch::async, [&] { return incr(w2, 1); });

    ASSERT_TRUE(wait_for_lock_wait(w2));

    api::trx_interrupt(w2);

    if (round == 1) {
      std::this_thread::sleep_for(500ms);
    }

    commit(h2);

    const auto err = get(r, w2, 5s);

    commit(w2);

    if (err == DB_SUCCESS) {
      EXPECT_EQ(value(1), before + 2);
    } else {
      EXPECT_EQ(err, DB_INTERRUPTED);
      EXPECT_EQ(value(1), before + 1);
    }
  }
}

/** A wait for a row with a lock wait timeout of 1 s ends with
DB_LOCK_WAIT_TIMEOUT no sooner than 1 s and no later than the next check
of the lock timeout thread (1 s period, 1 s resolution: 3 s); the whole
transaction is rolled back (rollback on timeout): the row it changed before
is free at once. The holder is unaffected. */
TEST_F(LockMtDb, TimeoutBounds) {
  const Timeout timeout(this, 1s);

  load({{1, ut::Row{1, 0, ""}}, {2, ut::Row{2, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);
  ASSERT_EQ(incr(w, 2), DB_SUCCESS);

  const auto start = Clock::now();
  const auto err = incr(w, 1);
  const std::chrono::duration<double> secs = Clock::now() - start;

  EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);
  EXPECT_GE(secs.count(), 1.0);
  EXPECT_LT(secs.count(), 3.5);

  auto t3 = begin();
  auto r3 = std::async(std::launch::async, [&] { return incr(t3, 2); });

  EXPECT_EQ(get(r3, t3, 500ms), DB_SUCCESS) << "the timed out transaction still holds its row";

  commit(t3);
  rollback(w);
  commit(h);

  EXPECT_EQ(value(1), 1u);
  EXPECT_EQ(value(2), 1u);
}

/** The holder commits close to the 1 s timeout of the waiter, earlier and
later from round to round: the waiter either gets the row (DB_SUCCESS, its
change is there) or times out (its transaction is rolled back, its change
is not there, it holds no lock), never both. */
TEST_F(LockMtDb, TimeoutGrantRace) {
  const Timeout timeout(this, 1s);

  load({{1, ut::Row{1, 0, ""}}});

  int n_granted{};
  int n_timeouts{};

  for (auto delay : {900ms, 1600ms, 2300ms}) {
    const auto before = value(1);
    auto h = begin();
    auto w = begin();

    ASSERT_EQ(incr(h, 1), DB_SUCCESS);

    auto r = std::async(std::launch::async, [&] { return incr(w, 1); });

    ASSERT_TRUE(wait_for_lock_wait(w));

    std::this_thread::sleep_for(delay);

    commit(h);

    const auto err = get(r, w, 5s);

    if (err == DB_SUCCESS) {
      ++n_granted;
      commit(w);
      EXPECT_EQ(value(1), before + 2);
    } else {
      ++n_timeouts;
      EXPECT_EQ(err, DB_LOCK_WAIT_TIMEOUT);

      mutex_enter(m_mutex);
      EXPECT_TRUE(w->m_trx_locks.empty()) << "a timed out transaction holds locks";
      mutex_exit(m_mutex);

      rollback(w);
      EXPECT_EQ(value(1), before + 1);
    }
  }

  std::printf("TimeoutGrantRace: %d granted, %d timed out\n", n_granted, n_timeouts);
}

/** A timeout of 0 s is not "wait forever": a wait ends with
DB_LOCK_WAIT_TIMEOUT at the first check of the lock timeout thread after the
clock (1 s resolution) moved, within 2.5 s. */
TEST_F(LockMtDb, Timeout0) {
  const Timeout timeout(this, 0s);

  load({{1, ut::Row{1, 0, ""}}});

  auto h = begin();
  auto w = begin();

  ASSERT_EQ(incr(h, 1), DB_SUCCESS);

  const auto start = Clock::now();
  auto r = std::async(std::launch::async, [&] { return incr(w, 1); });

  EXPECT_EQ(get(r, w, 5s), DB_LOCK_WAIT_TIMEOUT);
  EXPECT_LT(Clock::now() - start, 2500ms);

  rollback(w);
  commit(h);
}

/** The in-memory lock queues under threads, timed. */
class LockMtPerf : public LockMt {};

constexpr std::array<int, 5> THREADS{1, 2, 4, 8, 16};

/** Each thread locks and releases records of a page of its own: no
conflict, only the trx sys mutex (the lock_sys mutex) shared. */
TEST_F(LockMtPerf, RecUncontended) {
  for (int n : THREADS) {
    std::vector<std::unique_ptr<FakePage>> pages;
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      pages.push_back(std::make_unique<FakePage>(1 + t));
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_uncontended_t{}", n), n, 20000, [&](int t, uint64_t i) {
      mutex_enter(m_mutex);
      ut::keep(rec_request(trxs[t], pages[t]->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i % 8));
      m_lock_sys->release_off_trx_sys_mutex(trxs[t]);
      mutex_exit(m_mutex);
    });

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e6);
  }
}

/** All the threads lock and release the same record in S: no waits, the
queue of the record holds up to one lock per thread. */
TEST_F(LockMtPerf, RecHotS) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_hot_s_t{}", n), n, 20000, [&](int t, uint64_t) {
      mutex_enter(m_mutex);
      ut::keep(rec_request(trxs[t], page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2));
      m_lock_sys->release_off_trx_sys_mutex(trxs[t]);
      mutex_exit(m_mutex);
    });

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e6);
  }
}

/** All the threads take turns on one record in X: each request waits for
the holder, the release grants the next. Throughput, and the latency of a
request from its enqueue to its grant (p50, p99). */
TEST_F(LockMtPerf, RecHotX) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;
    std::vector<std::vector<double>> lat(n);

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    auto r = ut::bench_mt(std::format("rec_hot_x_t{}", n), n, n == 1 ? 20000 : 4000, [&](int t, uint64_t) {
      const auto start = Clock::now();

      mutex_enter(m_mutex);
      const auto got = rec_request(trxs[t], page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2);
      mutex_exit(m_mutex);

      if (got == WAIT) {
        (void)await(trxs[t]);
      }

      lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - start).count());

      mutex_enter(m_mutex);
      m_lock_sys->release_off_trx_sys_mutex(trxs[t]);
      mutex_exit(m_mutex);
    });

    std::vector<double> all;

    for (auto &v : lat) {
      all.insert(all.end(), v.begin(), v.end());
    }

    report_ns(std::format("rec_hot_x_grant_p50_t{}", n), n, percentile(all, 50));
    report_ns(std::format("rec_hot_x_grant_p99_t{}", n), n, percentile(all, 99));

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** One transaction holds W records of a page in X (one lock), W
transactions wait each for one of them. Its release grants them all: each
grant scans the page queue from its front to the waiting lock
(rec_dequeue_from_page() -> rec_has_to_wait_in_queue()), O(W^2) for the
release. The time per grant grows with W. */
TEST_F(LockMtPerf, GrantScan) {
  for (ulint w : {64, 256, 1024, 4096}) {
    FakePage page(1, w + 2);
    auto holder = begin();
    std::vector<Trx *> trxs;

    for (ulint i{}; i < w; ++i) {
      trxs.push_back(begin());
    }

    mutex_enter(m_mutex);

    for (ulint i{}; i < w; ++i) {
      EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), GRANTED);
    }

    const auto enqueue_start = Clock::now();

    for (ulint i{}; i < w; ++i) {
      EXPECT_EQ(rec_request(trxs[i], page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2 + i), WAIT);
    }

    const std::chrono::duration<double> enqueue = Clock::now() - enqueue_start;
    const auto start = Clock::now();

    m_lock_sys->release_off_trx_sys_mutex(holder);

    const std::chrono::duration<double> secs = Clock::now() - start;

    mutex_exit(m_mutex);

    ut::report(ut::BenchResult{std::format("grant_scan_w{}", w), 1, w, secs.count()});
    ut::report(ut::BenchResult{std::format("enqueue_wait_w{}", w), 1, w, enqueue.count()});

    commit(holder);

    for (auto trx : trxs) {
      EXPECT_EQ(trx->m_wait_lock, nullptr);
      commit(trx);
    }
  }
}

/** The cost of the deadlock check of a request that waits, no deadlock:
(1) behind L transactions holding S on the record, the search visits each;
(2) behind one holder with T idle transactions active: deadlock_occurs()
first clears the mark of every active transaction, O(T) at every wait. */
TEST_F(LockMtPerf, DeadlockCost) {
  FakePage page(1);

  for (int l : {10, 100, 1000}) {
    std::vector<Trx *> trxs;

    for (int i{}; i < l; ++i) {
      trxs.push_back(begin());
    }

    auto req_trx = begin();

    mutex_enter(m_mutex);

    for (auto trx : trxs) {
      EXPECT_EQ(rec_request(trx, page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2), GRANTED);
    }

    auto req = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT), &page.m_block, 2, m_index, req_trx);

    mutex_exit(m_mutex);

    auto r = ut::bench(std::format("deadlock_check_queue{}", l), 2000, [&](uint64_t) {
      mutex_enter(m_mutex);
      ut::keep(m_lock_sys->deadlock_occurs(req, req_trx));
      mutex_exit(m_mutex);
    });

    mutex_enter(m_mutex);
    m_lock_sys->release_off_trx_sys_mutex(req_trx);
    req_trx->m_wait_lock = nullptr;
    mutex_exit(m_mutex);

    commit(req_trx);

    for (auto trx : trxs) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }

  for (int n_idle : {10, 100, 1000, 4000}) {
    std::vector<Trx *> idle;

    for (int i{}; i < n_idle; ++i) {
      idle.push_back(begin());
    }

    auto holder = begin();
    auto req_trx = begin();

    mutex_enter(m_mutex);
    EXPECT_EQ(rec_request(holder, page.m_block, LOCK_X | LOCK_REC_NOT_GAP, 2), GRANTED);
    auto req = m_lock_sys->rec_create(LockMode(LOCK_X | LOCK_REC_NOT_GAP | LOCK_WAIT), &page.m_block, 2, m_index, req_trx);
    mutex_exit(m_mutex);

    auto r = ut::bench(std::format("deadlock_check_idle{}", n_idle), 2000, [&](uint64_t) {
      mutex_enter(m_mutex);
      ut::keep(m_lock_sys->deadlock_occurs(req, req_trx));
      mutex_exit(m_mutex);
    });

    mutex_enter(m_mutex);
    m_lock_sys->release_off_trx_sys_mutex(req_trx);
    req_trx->m_wait_lock = nullptr;
    mutex_exit(m_mutex);

    commit(req_trx);
    commit(holder);

    for (auto trx : idle) {
      commit(trx);
    }

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** The trx sys mutex (which is the lock_sys mutex) under the hot S record
load: how long a thread waits to get it and how long it holds it, p50 and
p99, by thread count; and the longest hold: the release of a transaction
with 1,000 locks, the interval after which a release lets the mutex go. */
TEST_F(LockMtPerf, MutexHold) {
  FakePage page(1);

  for (int n : THREADS) {
    std::vector<Trx *> trxs;
    std::vector<std::vector<double>> waits(n);
    std::vector<std::vector<double>> holds(n);

    for (int t{}; t < n; ++t) {
      trxs.push_back(begin());
    }

    ut::bench_mt(std::format("mutex_hot_s_t{}", n), n, 10000, [&](int t, uint64_t) {
      const auto t0 = Clock::now();
      mutex_enter(m_mutex);
      const auto t1 = Clock::now();
      ut::keep(rec_request(trxs[t], page.m_block, LOCK_S | LOCK_REC_NOT_GAP, 2));
      m_lock_sys->release_off_trx_sys_mutex(trxs[t]);
      const auto t2 = Clock::now();
      mutex_exit(m_mutex);

      waits[t].push_back(std::chrono::duration<double, std::nano>(t1 - t0).count());
      holds[t].push_back(std::chrono::duration<double, std::nano>(t2 - t1).count());
    });

    std::vector<double> w;
    std::vector<double> h;

    for (int t{}; t < n; ++t) {
      w.insert(w.end(), waits[t].begin(), waits[t].end());
      h.insert(h.end(), holds[t].begin(), holds[t].end());
    }

    report_ns(std::format("mutex_wait_p50_t{}", n), n, percentile(w, 50));
    report_ns(std::format("mutex_wait_p99_t{}", n), n, percentile(w, 99));
    report_ns(std::format("mutex_hold_p50_t{}", n), n, percentile(h, 50));
    report_ns(std::format("mutex_hold_p99_t{}", n), n, percentile(h, 99));

    for (auto trx : trxs) {
      commit(trx);
    }
  }

  std::vector<std::unique_ptr<FakePage>> pages;

  for (page_no_t p{}; p < 1000; ++p) {
    pages.push_back(std::make_unique<FakePage>(100 + p));
  }

  auto big = begin();

  mutex_enter(m_mutex);

  for (auto &p : pages) {
    ut::keep(rec_request(big, p->m_block, LOCK_X | LOCK_REC_NOT_GAP, 2));
  }

  const auto start = Clock::now();

  m_lock_sys->release_off_trx_sys_mutex(big);

  const std::chrono::duration<double, std::nano> ns = Clock::now() - start;

  mutex_exit(m_mutex);

  report_ns("mutex_hold_release_1000_locks", 1, ns.count());

  commit(big);
}

/** Transactions of a started instance, timed. */
class LockMtDbPerf : public LockMtDb {};

/** Each thread runs transactions that take an intention lock on the test
table, IS or IX, through the API, and commit: compatible, the table queue
holds up to one lock per thread. The same without the lock, as the base. */
TEST_F(LockMtDbPerf, TableIntent) {
  for (int n : THREADS) {
    ut::bench_mt(std::format("trx_begin_commit_t{}", n), n, 2000, [&](int, uint64_t) {
      auto trx = begin();
      commit(trx);
    });

    auto r = ut::bench_mt(std::format("table_is_ix_trx_t{}", n), n, 2000, [&](int t, uint64_t) {
      auto trx = begin();
      EXPECT_EQ(lock_table(trx, t % 2 == 0 ? IB_LOCK_IS : IB_LOCK_IX), DB_SUCCESS);
      commit(trx);
    });

    EXPECT_LT(r.ns_per_op(), 1e7);
  }
}

/** P pairs of threads, each pair handing a row of its own over: the
holder commits while the other waits for the row; the time from the start
of the commit to the return of the waiter's update, by number of pairs
running at once (the wakeup through the server slot event, the trx sys
mutex shared by all). */
TEST_F(LockMtDbPerf, HandoffPairs) {
  constexpr int N = 40;

  ut::Rows initial;

  for (uint32_t k = 1; k <= 8; ++k) {
    initial[k] = ut::Row{k, 0, ""};
  }

  load(initial);

  for (int pairs : {1, 2, 4, 8}) {
    std::vector<std::vector<double>> lat(pairs);
    std::vector<std::thread> threads;

    for (int p{}; p < pairs; ++p) {
      threads.emplace_back([&, p] {
        const uint32_t k = 1 + p;

        for (int i{}; i < N; ++i) {
          auto holder = begin();

          if (incr(holder, k) != DB_SUCCESS) {
            ADD_FAILURE() << "incr";
            rollback(holder);
            return;
          }

          auto waiter = begin();
          Clock::time_point granted;

          auto result = std::async(std::launch::async, [&] {
            auto err = incr(waiter, k);
            granted = Clock::now();
            return err;
          });

          EXPECT_TRUE(wait_for_lock_wait(waiter));

          const auto start = Clock::now();

          commit(holder);

          EXPECT_EQ(get(result, waiter), DB_SUCCESS);
          lat[p].push_back(std::chrono::duration<double, std::nano>(granted - start).count());
          commit(waiter);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    std::vector<double> all;

    for (auto &v : lat) {
      all.insert(all.end(), v.begin(), v.end());
    }

    report_ns(std::format("api_handoff_p50_pairs{}", pairs), pairs, percentile(all, 50));
    report_ns(std::format("api_handoff_p99_pairs{}", pairs), pairs, percentile(all, 99));
  }
}

}  // namespace
