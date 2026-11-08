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

/** @file include/lock0queues.h
The record lock queues of the pages of a page shard, by page id: a chained
hash table whose nodes are kept on a free list when a queue empties, so that
a page whose queue empties and fills again (a lock and its release) costs no
allocation; a power of two of buckets, a multiplicative hash (no division).
A node does not move while its queue exists: a reference to a queue stays
valid across the insertion of others (LockSys::rec_move() holds the
donator's queue while it adds the receiver's). The shard latch protects it.
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "lock0types.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

class KeyQueues {
 public:
  using value_type = std::pair<const LockKey, Rec_locks>;

 private:
  struct Node {
    value_type m_kv;
    Node *m_next;
  };

 public:
  /** A forward iterator over the queues. */
  template <typename V>
  class Iter {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = V;
    using difference_type = std::ptrdiff_t;
    using pointer = V *;
    using reference = V &;

    Iter(const KeyQueues *queues, size_t bucket, Node *node) noexcept : m_queues(queues), m_bucket(bucket), m_node(node) { skip(); }

    reference operator*() const noexcept { return m_node->m_kv; }
    pointer operator->() const noexcept { return &m_node->m_kv; }

    Iter &operator++() noexcept {
      m_node = m_node->m_next;
      skip();
      return *this;
    }

    bool operator==(const Iter &rhs) const noexcept { return m_node == rhs.m_node; }

   private:
    /** To the next node, through the empty buckets. */
    void skip() noexcept {
      while (m_node == nullptr && ++m_bucket < m_queues->m_buckets.size()) {
        m_node = m_queues->m_buckets[m_bucket];
      }
    }

    const KeyQueues *m_queues;
    size_t m_bucket;
    Node *m_node;
  };

  using iterator = Iter<value_type>;
  using const_iterator = Iter<const value_type>;

  KeyQueues() noexcept = default;

  KeyQueues(const KeyQueues &) = delete;
  KeyQueues &operator=(const KeyQueues &) = delete;

  ~KeyQueues() noexcept {
    for (auto node : m_buckets) {
      while (node != nullptr) {
        auto next = node->m_next;
        free_node(node);
        node = next;
      }
    }

    while (m_free != nullptr) {
      auto next = m_free->m_next;
      ::operator delete(m_free);
      m_free = next;
    }
  }

  /** @return the queue of the page, or nullptr. */
  [[nodiscard]] Rec_locks *find(LockKey page_id) const noexcept {
    if (m_n == 0) {
      return nullptr;
    }

    for (auto node = m_buckets[bucket(page_id)]; node != nullptr; node = node->m_next) {
      if (node->m_kv.first == page_id) {
        return &node->m_kv.second;
      }
    }

    return nullptr;
  }

  /** @return the queue of the page, created empty if there is none. */
  [[nodiscard]] Rec_locks &get(LockKey page_id) noexcept {
    if (auto locks = find(page_id); locks != nullptr) {
      return *locks;
    }

    if (m_n >= m_buckets.size()) {
      grow();
    }

    Node *node;

    if (m_free != nullptr) {
      node = m_free;
      m_free = node->m_next;
      --m_n_free;
    } else {
      node = static_cast<Node *>(::operator new(sizeof(Node)));
    }

    std::construct_at(&node->m_kv, std::piecewise_construct, std::forward_as_tuple(page_id), std::forward_as_tuple());

    auto &head = m_buckets[bucket(page_id)];

    node->m_next = head;
    head = node;
    ++m_n;

    return node->m_kv.second;
  }

  /** Erase the queue of the page, if there is one. */
  void erase(LockKey page_id) noexcept {
    if (m_n == 0) {
      return;
    }

    for (auto prev = &m_buckets[bucket(page_id)]; *prev != nullptr; prev = &(*prev)->m_next) {
      auto node = *prev;

      if (node->m_kv.first == page_id) {
        *prev = node->m_next;
        --m_n;
        free_node(node);
        return;
      }
    }
  }

  /** @return the number of queues. */
  [[nodiscard]] size_t size() const noexcept { return m_n; }

  [[nodiscard]] bool empty() const noexcept { return m_n == 0; }

  [[nodiscard]] iterator begin() noexcept { return iterator(this, 0, m_buckets.empty() ? nullptr : m_buckets[0]); }
  [[nodiscard]] iterator end() noexcept { return iterator(this, m_buckets.size(), nullptr); }

  [[nodiscard]] const_iterator begin() const noexcept { return const_iterator(this, 0, m_buckets.empty() ? nullptr : m_buckets[0]); }
  [[nodiscard]] const_iterator end() const noexcept { return const_iterator(this, m_buckets.size(), nullptr); }

 private:
  /** The nodes kept for reuse: up to this many, or as many as in use. */
  static constexpr size_t MIN_FREE = 64;

  /** @return the bucket of a page: the high bits of a multiplicative hash
  (the shard of the page is chosen with the highest 9 bits of another). */
  [[nodiscard]] size_t bucket(LockKey page_id) const noexcept {
    const auto h = page_id.m_fold * 0xD6E8FEB86659FD93ULL;
    return size_t(h >> m_shift);
  }

  void free_node(Node *node) noexcept {
    std::destroy_at(&node->m_kv);

    if (m_n_free < std::max(MIN_FREE, m_n)) {
      node->m_next = m_free;
      m_free = node;
      ++m_n_free;
    } else {
      ::operator delete(node);
    }
  }

  /** Double the buckets (16 at first) and relink the nodes: they stay where
  they are. */
  void grow() noexcept {
    std::vector<Node *> old;

    old.swap(m_buckets);

    const size_t n = old.empty() ? 16 : old.size() * 2;

    m_buckets.assign(n, nullptr);
    m_shift = 64 - uint32_t(std::countr_zero(n));

    for (auto node : old) {
      while (node != nullptr) {
        auto next = node->m_next;
        auto &head = m_buckets[bucket(node->m_kv.first)];

        node->m_next = head;
        head = node;
        node = next;
      }
    }
  }

  std::vector<Node *> m_buckets;
  uint32_t m_shift{64};
  size_t m_n{};
  Node *m_free{};
  size_t m_n_free{};
};
