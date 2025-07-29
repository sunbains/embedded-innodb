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

/** @file include/ut0slst.h
Single linked list implementation,sometimes the double linked list is an overkill.

Created 2025-04-14 Sunny Bains
*******************************************************/

#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <iterator>

/**
 * @brief The one-way (singly) list node.
 *
 * This structure represents a node in a singly linked list.
 *
 * @tparam Type The list node type name.
 */
template <typename Type>
struct ut_slist_node {
  Type *m_next{nullptr};
};

#define UT_SLIST_NODE_T(t) ut_slist_node<t>

/** TODO: Fix concept - temporarily disabled for compatibility */
template <typename NodeGetter, typename Type>
concept SNodeGetterFor = true;

/**
 * @brief The one-way (singly) list base node.
 *
 * The base node contains pointers to the head and tail of the list and a count of nodes in the list.
 *
 * @tparam Type The type of the list element.
 * @tparam NodeGetter A class which has a static member function
 *         ut_slist_node<Type> get_node(const Type &e) which knows how to extract a node from an element.
 */
template <typename Type, typename NodeGetter>
  requires SNodeGetterFor<NodeGetter, Type>
struct ut_slist_base {
  using elem_type = Type;
  using node_type = ut_slist_node<Type>;

  static const node_type &get_node(const elem_type &e) noexcept { return NodeGetter::get_node(e); }

  static node_type &get_node(elem_type &e) noexcept { return const_cast<node_type &>(get_node(const_cast<const elem_type &>(e))); }

  static elem_type *next(elem_type &e) noexcept { return get_node(e).m_next; }

  ut_slist_base() = default;

  ut_slist_base(const ut_slist_base &) = delete;
  ut_slist_base &operator=(const ut_slist_base &) = delete;

  size_t size() const noexcept { return m_count.load(std::memory_order_acquire); }

  bool empty() const noexcept { return size() == 0; }

  void push_front(elem_type *elem) noexcept {
    auto &node = get_node(*elem);

    node.m_next = m_head;
    m_head = elem;

    if (m_tail == nullptr) {
      m_tail = elem;
    }

    m_count.fetch_add(1, std::memory_order_release);
  }

  void push_back(elem_type *elem) noexcept {
    auto &node = get_node(*elem);

    node.m_next = nullptr;

    if (m_tail != nullptr) {
      get_node(*m_tail).m_next = elem;
    } else {
      m_head = elem;
    }

    m_tail = elem;

    m_count.fetch_add(1, std::memory_order_release);
  }

  void clear() noexcept {
    m_head = nullptr;
    m_tail = nullptr;

    m_count.store(0, std::memory_order_release);
  }

  struct iterator {
    elem_type *m_elem;

    explicit iterator(elem_type *e) : m_elem(e) {}

    bool operator==(const iterator &other) const { return m_elem == other.m_elem; }

    bool operator!=(const iterator &other) const { return !(*this == other); }

    elem_type *operator*() const { return m_elem; }

    iterator &operator++() {
      m_elem = get_node(*m_elem).m_next;
      return *this;
    }
  };

  iterator begin() { return iterator(m_head); }

  iterator end() { return iterator(nullptr); }

  /** Get the first element in the list.
   * @return Pointer to the first element or nullptr if empty */
  Type *front() noexcept { return m_head; }

  /** Get the first element in the list.
   * @return Const pointer to the first element or nullptr if empty */
  const Type *front() const noexcept { return m_head; }

  /** Get the last element in the list.
   * @return Pointer to the last element or nullptr if empty */
  Type *back() noexcept { return m_tail; }

  /** Get the last element in the list.
   * @return Const pointer to the last element or nullptr if empty */
  const Type *back() const noexcept { return m_tail; }

  elem_type *m_head{nullptr};
  elem_type *m_tail{nullptr};

  std::atomic<size_t> m_count{0};
};

/**
 * Macro helpers for defining NodeGetter and list type declarations.
 */
#define UT_SLIST_NODE_GETTER(t, m) t##_##m##_slist_getter

#define UT_SLIST_NODE_GETTER_DEF(t, m)                          \
  struct UT_SLIST_NODE_GETTER(t, m) {                           \
    static const ut_slist_node<t> &get_node(const t &element) { \
      return element.m;                                         \
    }                                                           \
  };

#define UT_SLIST_BASE_NODE_T(t, m) ut_slist_base<t, UT_SLIST_NODE_GETTER(t, m)>
