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

/** @file include/ut0lflist.h
A lock free list for walks that take no lock: the transactions of TrxSys. And a
pool of cache line separated slots, the snapshots of the read views.

THE LIST

Nodes are pooled and type stable (never freed while the list lives): the memory
of a node is always a node. Push is at the head only (compare and swap). A node
holds a user payload, a ticket and a removal stamp, both from a clock C:

  m_seq     0: live. remove(): PENDING, then a value of C.tick(), by a CAS from
            PENDING. A reader that meets PENDING helps, the same way: the stamp
            has one final value, and nobody waits for the owner.
  m_ticket  0: unassigned, then a value of C.ticket(), the same helping. Push first,
            then the ticket: so any ticket <= a value read from C is of a node
            that a walk starting after that read will find, or that is unlinked.

C is a word that only grows, with total order (sequentially consistent) reads and
ticks; tick() returns a new value above all that read() returned before it.

A remove is logical. A single sweeper at a time (a try latch) unlinks; there are no
marked pointers: only the sweeper writes the next pointer of a node in the list, and
only the sweeper CASes the head away from a node (a push CASes it to a new node).

A WALK (Scan)

  1. CAS a free slot of the list from 0 to v = C.read()
  2. S = C.read()
  3. walk from the head

A node is removed for the walk iff its final stamp is <= S.

THE RULES OF THE SWEEPER, AND WHY THEY HOLD

U (unlink). The caller gives a horizon. The sweeper unlinks a node with stamp c only
if c <= h, h the minimum of the horizon and of C.read() and the slots, read in this
order, and the owner is done with it (below). Every walk that misses the node has
c <= S, it counts the node as removed anyway: a walk never misses a node that is
live for it. A slot seen at v: v <= S of its walk. A slot seen 0 whose walk comes
later: its slot CAS is after the read of the slot in the total order, so its S is
read after the read of C, and S >= c.

R (reuse). After unlinking a batch: C1 = C.tick(). The nodes of the batch are
reused only when every slot was seen 0 or >= C1, at a read of the slots after the
tick. A walk with a pointer into the batch registered before the unlink, its slot is
< C1 while it walks, unless it read v after the tick, and then its walk began
after the unlink. A slot seen 0 and registered later: its slot CAS comes after that
read of the slot, its S load after that (the total order), so S reads the clock
after the tick: the tick and all before it, the unlink stores, happen before its
walk (release and acquire on the clock word), which can't reach the batch. The next
pointer of an unlinked node stays as it was until reuse: a walk standing on one
goes on through nodes that, by the same rule, can't be reused yet.

The owner. remove() does a CAS on a node after it may have been finished by a
helper. A helper holds something that stops reuse (a slot of the walk, or what the
user of the list makes it hold); the owner has nothing: m_owned, which remove() clears
last, and the sweeper leaves an owned node alone.

Every field that is read concurrently is an atomic.

WAITS

None in push, remove, tick, a walk: lock free. A walk has to look for a free slot,
and yields if all N_SLOTS are in use. alloc() and the free are lock free, but a new
chunk is made with a latch. Nodes of an unlinked batch wait for the walks of the time.

SLOTS (SlotPool)

The same kind of slot for long holds: type stable, one per cache line, allocated from
free lists sharded by thread so that the allocation of a thread writes a line of its
own, in the common case. Scanning them is for the holder of a sweep, not for the
open path. */

#pragma once

#include "ut0latch.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <thread>

namespace ut {

/** A clock for a list that has no other use for one. */
class ListCounter {
 public:
  [[nodiscard]] uint64_t read() const noexcept { return m_n.load(std::memory_order_seq_cst); }
  [[nodiscard]] uint64_t tick() noexcept { return m_n.fetch_add(1, std::memory_order_seq_cst) + 1; }
  [[nodiscard]] uint64_t ticket() noexcept { return tick(); }

  /** @return the bits of a stamp that change at every stamp. */
  [[nodiscard]] static uint64_t spread(uint64_t stamp) noexcept { return stamp; }

 private:
  alignas(64) std::atomic<uint64_t> m_n{1};
  char m_pad[64 - sizeof(std::atomic<uint64_t>)]{};
};

/** @return a small number of this thread: picks shards and first slots. */
[[nodiscard]] inline size_t thread_no() noexcept {
  static std::atomic<size_t> s_next{};
  thread_local const size_t no = s_next.fetch_add(1, std::memory_order_relaxed);

  return no;
}

/** A pool of type stable objects: chunks, and tagged free lists by index + 1. Each
object T has a member std::atomic<uint32_t> m_free_next and uint32_t m_index. */
template <typename T, size_t CHUNK, size_t N_CHUNKS>
class StablePool {
 public:
  explicit StablePool(size_t max_n = CHUNK * N_CHUNKS) noexcept : m_max_n(max_n) {}

  StablePool(const StablePool &) = delete;
  StablePool &operator=(const StablePool &) = delete;

  ~StablePool() noexcept {
    for (auto &chunk : m_chunks) {
      delete[] chunk.load(std::memory_order_relaxed);
    }
  }

  /** @return an object, nullptr at the limit. */
  [[nodiscard]] T *pop() noexcept {
    const auto shard = thread_no();

    for (size_t i{}; i < N_FREE; ++i) {
      if (auto p = pop_from(m_free[(shard + i) % N_FREE]); p != nullptr) {
        return p;
      }
    }

    return grow();
  }

  void push(T *p) noexcept { push_to(m_free[thread_no() % N_FREE], p); }

  /** @return the number of objects that exist. */
  [[nodiscard]] size_t size() const noexcept { return m_n.load(std::memory_order_acquire); }

  [[nodiscard]] T &at(size_t i) const noexcept { return m_chunks[i / CHUNK].load(std::memory_order_acquire)[i % CHUNK]; }

 private:
  static constexpr size_t N_FREE = 64;

  struct alignas(64) FreeHead {
    /** tag << 32 | (index + 1) */
    std::atomic<uint64_t> m_top{};
  };

  [[nodiscard]] T *pop_from(FreeHead &h) noexcept {
    auto top = h.m_top.load(std::memory_order_acquire);

    for (;;) {
      const auto idx = uint32_t(top);

      if (idx == 0) {
        return nullptr;
      }

      auto p = &at(idx - 1);
      const uint64_t next = ((top >> 32) + 1) << 32 | p->m_free_next.load(std::memory_order_relaxed);

      if (h.m_top.compare_exchange_weak(top, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return p;
      }
    }
  }

  void push_to(FreeHead &h, T *p) noexcept {
    auto top = h.m_top.load(std::memory_order_relaxed);

    for (;;) {
      p->m_free_next.store(uint32_t(top), std::memory_order_relaxed);

      const uint64_t next = ((top >> 32) + 1) << 32 | (p->m_index + 1);

      if (h.m_top.compare_exchange_weak(top, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return;
      }
    }
  }

  /** @return a new object of a new chunk, the others go to the free list. */
  [[nodiscard]] T *grow() noexcept {
    std::lock_guard<Latch> guard(m_grow_latch);

    const auto n = m_n.load(std::memory_order_relaxed);

    if (n >= m_max_n || n / CHUNK >= N_CHUNKS) {
      return nullptr;
    }

    auto chunk = new T[CHUNK];
    const auto n_new = std::min(CHUNK, m_max_n - n);

    for (size_t i{}; i < CHUNK; ++i) {
      chunk[i].m_index = uint32_t(n + i);
    }

    m_chunks[n / CHUNK].store(chunk, std::memory_order_release);
    m_n.store(n + n_new, std::memory_order_release);

    for (size_t i = 1; i < n_new; ++i) {
      push(&chunk[i]);
    }

    return &chunk[0];
  }

  const size_t m_max_n;
  std::atomic<size_t> m_n{};
  Latch m_grow_latch{};
  std::array<std::atomic<T *>, N_CHUNKS> m_chunks{};
  std::array<FreeHead, N_FREE> m_free{};
};

/** A list with walks, see the top of this file.
@tparam P A default constructible payload of the node.
@tparam C The clock. */
template <typename P, typename C>
class LfList {
 public:
  /** The stamp of a removal that began: above any value of a clock. */
  static constexpr uint64_t PENDING = ~uint64_t(0);

  /** The slots of the walks. */
  static constexpr size_t N_SLOTS = 64;

  struct alignas(64) Node {
    std::atomic<Node *> m_next{};
    std::atomic<uint64_t> m_ticket{};
    std::atomic<uint64_t> m_seq{};
    std::atomic<bool> m_owned{};
    std::atomic<uint32_t> m_free_next{};
    uint32_t m_index{};

    /** The limbo: unlinked, not yet reused. Only the sweeper's. */
    Node *m_limbo_next{};
    uint64_t m_limbo_c1{};

    P m_payload{};
  };

  /** @param[in] clock  The clock, it outlives the list.
  @param[in] max_nodes  The limit of the nodes: alloc() fails past it. */
  explicit LfList(C &clock, size_t max_nodes = CHUNK_NODES * N_CHUNKS) noexcept : m_clock(clock), m_pool(max_nodes) {}

  LfList(const LfList &) = delete;
  LfList &operator=(const LfList &) = delete;

  /** @return a node for push(): ticket and stamp 0. Nullptr if the limit is met:
  until sweeps free some. The payload is as its last user left it. */
  [[nodiscard]] Node *alloc() noexcept {
    auto n = m_pool.pop();

    if (n != nullptr) {
      n->m_ticket.store(0, std::memory_order_relaxed);
      n->m_seq.store(0, std::memory_order_relaxed);
      n->m_owned.store(true, std::memory_order_relaxed);
    }

    return n;
  }

  /** Push at the head. */
  void push(Node *n) noexcept {
    auto head = m_head.load(std::memory_order_relaxed);

    do {
      n->m_next.store(head, std::memory_order_relaxed);
    } while (!m_head.compare_exchange_weak(head, n, std::memory_order_seq_cst, std::memory_order_relaxed));
  }

  /** @return the final ticket of a node, helping if there is none yet. */
  [[nodiscard]] uint64_t final_ticket(Node &n) noexcept {
    auto t = n.m_ticket.load(std::memory_order_seq_cst);

    if (t == 0) {
      const auto mine = m_clock.ticket();

      return n.m_ticket.compare_exchange_strong(t, mine, std::memory_order_seq_cst, std::memory_order_seq_cst) ? mine : t;
    }

    return t;
  }

  /** Step 1 of a remove: from here on it can't be read as live. Before any tick of
  the stamp, so a walk with an S >= the stamp comes after this store: it can't read
  0 for a node with a stamp <= S. */
  static void mark_pending(Node *n) noexcept { n->m_seq.store(PENDING, std::memory_order_seq_cst); }

  /** @return the final stamp of a node: helping if PENDING, 0 if live. */
  [[nodiscard]] uint64_t final_seq(Node &n) noexcept {
    auto v = n.m_seq.load(std::memory_order_seq_cst);

    if (v == PENDING) {
      const auto mine = m_clock.tick();

      return n.m_seq.compare_exchange_strong(v, mine, std::memory_order_seq_cst, std::memory_order_seq_cst) ? mine : v;
    }

    return v;
  }

  /** Last step of a remove, by the owner: the node is the sweeper's. The node must
  not be touched by the owner after it. */
  static void release(Node *n) noexcept { n->m_owned.store(false, std::memory_order_release); }

  /** Remove a node logically: the steps together, for a user with nothing between.
  @return the final stamp. */
  uint64_t remove(Node *n) noexcept {
    mark_pending(n);

    const auto stamp = final_seq(*n);

    release(n);

    return stamp;
  }

  /** Insert in a list sorted by ticket, biggest first: for recovery and tests. The
  ticket is set. */
  void insert_ordered(Node *n) noexcept {
    std::lock_guard<Latch> guard(m_sweep_latch);

    const auto ticket = n->m_ticket.load(std::memory_order_relaxed);
    Node *prev{};
    auto cur = m_head.load(std::memory_order_seq_cst);

    while (cur != nullptr && cur->m_ticket.load(std::memory_order_seq_cst) > ticket) {
      prev = cur;
      cur = cur->m_next.load(std::memory_order_seq_cst);
    }

    n->m_next.store(cur, std::memory_order_seq_cst);

    if (prev != nullptr) {
      prev->m_next.store(n, std::memory_order_seq_cst);
    } else {
      auto expected = cur;

      if (!m_head.compare_exchange_strong(expected, n, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
        /* A push won: n goes between the pushed nodes and cur. */
        while (expected->m_next.load(std::memory_order_seq_cst) != cur) {
          expected = expected->m_next.load(std::memory_order_seq_cst);
        }

        expected->m_next.store(n, std::memory_order_seq_cst);
      }
    }
  }

  /** A walk: see the top of this file. */
  class Scan {
   public:
    explicit Scan(LfList &list) noexcept : m_list(list) {
      const auto v = list.m_clock.read();
      const auto first = thread_no();

      for (size_t i{};; ++i) {
        uint64_t free{};
        auto &slot = list.m_slots[(first + i) % N_SLOTS].m_v;

        if (slot.load(std::memory_order_relaxed) == 0 && slot.compare_exchange_strong(free, v, std::memory_order_seq_cst, std::memory_order_relaxed)) {
          m_slot = &slot;
          break;
        }

        if (i % N_SLOTS == N_SLOTS - 1) {
          std::this_thread::yield();
        }
      }

      m_seq = list.m_clock.read();
    }

    Scan(const Scan &) = delete;
    Scan &operator=(const Scan &) = delete;

    ~Scan() noexcept { m_slot->store(0, std::memory_order_release); }

    /** @return S: a node is removed for this walk iff its stamp is <= S. */
    [[nodiscard]] uint64_t seq() const noexcept { return m_seq; }

    [[nodiscard]] Node *head() const noexcept { return m_list.m_head.load(std::memory_order_seq_cst); }

    [[nodiscard]] static Node *next(Node *n) noexcept { return n->m_next.load(std::memory_order_seq_cst); }

    /** @return true if the node is live for this walk. */
    [[nodiscard]] bool live(Node &n) noexcept {
      const auto v = m_list.final_seq(n);

      return v == 0 || v > m_seq;
    }

    /** @return the ticket of the node, assigned if need be. */
    [[nodiscard]] uint64_t ticket(Node &n) noexcept { return m_list.final_ticket(n); }

   private:
    LfList &m_list;
    std::atomic<uint64_t> *m_slot{};
    uint64_t m_seq{};
  };

  /** Run a sweep, if no other is running.
  @param[in] horizon  Rule U: don't unlink a node with a stamp above this.
  @param[in] visit    Called for each node left in the list.
  @param[in] unlink   Called for each node about to be unlinked.
  @return true if it ran. */
  template <typename V, typename U>
  bool try_sweep(uint64_t horizon, V &&visit, U &&unlink) noexcept {
    if (!m_sweep_latch.try_lock()) {
      return false;
    }

    sweep(horizon, visit, unlink);

    m_sweep_latch.unlock();

    return true;
  }

  /** @return true if a remove with this stamp should sweep: the chance makes the
  walk of a sweep O(1) per remove in the mean. The stamp is mixed: when commits outrun
  the clock its value steps by a fixed amount, and its low bits alone may never be 0. */
  [[nodiscard]] bool sweep_due(uint64_t stamp) const noexcept {
    const auto mixed = (C::spread(stamp) * 0x9E3779B97F4A7C15ULL) >> 32;

    return (mixed & m_sweep_mask.load(std::memory_order_relaxed)) == 0;
  }

  /** @return the nodes in the list, removed or not: quiescent, for tests. */
  [[nodiscard]] size_t n_linked() const noexcept {
    size_t n{};

    for (auto p = m_head.load(std::memory_order_seq_cst); p != nullptr; p = p->m_next.load(std::memory_order_seq_cst)) {
      ++n;
    }

    return n;
  }

  /** @return the nodes that exist. */
  [[nodiscard]] size_t n_nodes() const noexcept { return m_pool.size(); }

  /** A hook of the tests, called by the sweeper after it read the head. */
  void (*m_sweep_hook)(void *){};
  void *m_sweep_hook_arg{};

 private:
  static constexpr size_t CHUNK_NODES = 1024;
  static constexpr size_t N_CHUNKS = 65536;

  struct alignas(64) Slot {
    std::atomic<uint64_t> m_v{};
  };

  /** @return the minimum of bound and the slots that are set. */
  [[nodiscard]] uint64_t min_slot(uint64_t bound) const noexcept {
    for (const auto &slot : m_slots) {
      const auto v = slot.m_v.load(std::memory_order_seq_cst);

      if (v != 0 && v < bound) {
        bound = v;
      }
    }

    return bound;
  }

  template <typename V, typename U>
  void sweep(uint64_t horizon, V &&visit, U &&unlink) noexcept {
    /* Rule U, the clock first. */
    const auto h = min_slot(std::min(horizon, m_clock.read()));

    Node *prev{};
    Node *batch{};
    Node *batch_tail{};
    size_t n_left{};

    auto first = m_head.load(std::memory_order_seq_cst);

    if (m_sweep_hook != nullptr) {
      m_sweep_hook(m_sweep_hook_arg);
    }

    for (auto n = first; n != nullptr;) {
      const auto next = n->m_next.load(std::memory_order_seq_cst);
      const auto v = n->m_seq.load(std::memory_order_seq_cst);

      if (v != 0 && v != PENDING && v <= h && !n->m_owned.load(std::memory_order_acquire)) {
        unlink(*n);

        if (prev != nullptr) {
          prev->m_next.store(next, std::memory_order_seq_cst);
        } else {
          auto cur = n;

          if (!m_head.compare_exchange_strong(cur, next, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
            /* A push won: the pushed nodes are before n. */
            while (cur->m_next.load(std::memory_order_seq_cst) != n) {
              cur = cur->m_next.load(std::memory_order_seq_cst);
            }

            cur->m_next.store(next, std::memory_order_seq_cst);
            prev = cur;
          }
        }

        n->m_limbo_next = nullptr;

        if (batch_tail != nullptr) {
          batch_tail->m_limbo_next = n;
        } else {
          batch = n;
        }

        batch_tail = n;
      } else {
        visit(*n);
        prev = n;
        ++n_left;
      }

      n = next;
    }

    /* Rule R. */
    if (batch != nullptr) {
      const auto c1 = m_clock.tick();

      for (auto n = batch; n != nullptr; n = n->m_limbo_next) {
        n->m_limbo_c1 = c1;
      }

      if (m_limbo_tail != nullptr) {
        m_limbo_tail->m_limbo_next = batch;
      } else {
        m_limbo_head = batch;
      }

      m_limbo_tail = batch_tail;
    }

    if (m_limbo_head != nullptr) {
      const auto seen = min_slot(PENDING);

      while (m_limbo_head != nullptr && m_limbo_head->m_limbo_c1 <= seen) {
        auto n = m_limbo_head;

        m_limbo_head = n->m_limbo_next;

        if (m_limbo_head == nullptr) {
          m_limbo_tail = nullptr;
        }

        m_pool.push(n);
      }
    }

    /* At least every 8th remove: a sweep reads all the slots, even of a short list. */
    const auto width = std::bit_width(n_left / 2);

    m_sweep_mask.store((uint64_t(1) << std::clamp(width, 3, 20)) - 1, std::memory_order_relaxed);
  }

  C &m_clock;
  StablePool<Node, CHUNK_NODES, N_CHUNKS> m_pool;

  /** The head: every push writes it. */
  alignas(64) std::atomic<Node *> m_head{};

  /** Read by every remove, written by the sweeper. */
  alignas(64) std::atomic<uint64_t> m_sweep_mask{7};
  Latch m_sweep_latch{};
  Node *m_limbo_head{};
  Node *m_limbo_tail{};

  alignas(64) std::array<Slot, N_SLOTS> m_slots{};
};

/** A slot of the snapshot pool: one per cache line. */
struct alignas(64) SnapSlot {
  /** 0: free. A lower bound of the snapshot, then the snapshot. */
  std::atomic<uint64_t> m_v{};
  std::atomic<uint32_t> m_free_next{};
  uint32_t m_index{};
};

/** The slots of the snapshots of the read views. */
class SlotPool {
 public:
  SlotPool() noexcept = default;

  /** @return a slot, 0. Fatal at the limit: 4 M open views. */
  [[nodiscard]] SnapSlot *alloc() noexcept {
    auto p = m_pool.pop();

    if (p == nullptr) [[unlikely]] {
      ::abort();
    }

    return p;
  }

  /** Clears it and gives it back. */
  void free(SnapSlot *p) noexcept {
    p->m_v.store(0, std::memory_order_release);
    m_pool.push(p);
  }

  /** @return the minimum of bound and the slots that are set, but for skip. */
  [[nodiscard]] uint64_t min(uint64_t bound, const SnapSlot *skip = nullptr) const noexcept {
    const auto n = m_pool.size();

    for (size_t i{}; i < n; ++i) {
      const auto &slot = m_pool.at(i);
      const auto v = slot.m_v.load(std::memory_order_seq_cst);

      if (v != 0 && v < bound && &slot != skip) {
        bound = v;
      }
    }

    return bound;
  }

  /** @return the number of slots that are set. */
  [[nodiscard]] size_t n_set() const noexcept {
    const auto n = m_pool.size();
    size_t count{};

    for (size_t i{}; i < n; ++i) {
      count += m_pool.at(i).m_v.load(std::memory_order_acquire) != 0;
    }

    return count;
  }

 private:
  StablePool<SnapSlot, 256, 16384> m_pool{};
};

}  // namespace ut
