/*****************************************************************************
Copyright (c) 1995, 2023, Oracle and/or its affiliates.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/ut0lst.h
 List utilities

 Created 9/10/1995 Heikki Tuuri
 Rewritten by Sunny Bains Dec 2011.
 ***********************************************************************/

#pragma once

/* Do not include univ.i because univ.i includes this. */

#include <atomic>
#include <concepts>

#include "ut0dbg.h"

/* This module implements the two-way linear list. Note that a single
list node may belong to two or more lists, but is only on one list
at a time. */

/**
 * @brief The two-way list node.
 * 
 * This structure represents a node in a two-way linked list.
 * 
 * @tparam Type The list node type name.
 */
template <typename Type>
struct ut_list_node {
  /** Pointer to the previous node, nullptr if start of list */
  Type *m_prev{nullptr};

  /** Pointer to the next node, nullptr if end of list */
  Type *m_next{nullptr};

  /**
   * @brief Reverses the links of the node.
   * 
   * This function swaps the previous and next pointers of the node.
   */
  void reverse() noexcept {
    Type *tmp = m_prev;

    m_prev = m_next;
    m_next = tmp;
  }
};

/** Macro used for legacy reasons */
#define UT_LIST_NODE_T(t) ut_list_node<t>

/** TODO: Fix concept - temporarily disabled for compatibility */
template<typename NodeGetter, typename Type>
concept NodeGetterFor = true;

#ifdef UNIV_DEBUG
#define UT_LIST_INITIALISED 0xCAFE
#endif /* UNIV_DEBUG */

#define UT_LIST_IS_INITIALISED(b) ((b).init == UT_LIST_INITIALISED)

/**
 * @brief The two-way list base node.
 * 
 * The base node contains pointers to both ends of the list and a count of nodes in the list 
 * (excluding the base node from the count). We also store a pointer to the member field so that it 
 * doesn't have to be specified when doing list operations.
 * 
 * @tparam Type The type of the list element.
 * @tparam NodeGetter A class which has a static member function 
 *         ut_list_node<Type> get_node(const Type &e) which knows how to extract a node from an element.
 */
template <typename Type, typename NodeGetter>
  requires NodeGetterFor<NodeGetter, Type>
struct ut_list_base {
  using elem_type = Type;
  using node_type = ut_list_node<elem_type>;

  /** Copy constructor.
   * 
   * This is required to copy the count explicitly.
   */
  ut_list_base(const ut_list_base &rhs) noexcept
    : m_first_element{rhs.m_first_element},
      m_last_element{rhs.m_last_element} {
    count.store(rhs.count.load(std::memory_order_acquire));
  }

  static const node_type &get_node(const elem_type &e) noexcept {
    return NodeGetter::get_node(e);
  }

  static node_type &get_node(elem_type &e) noexcept {
    return const_cast<node_type &>(get_node(const_cast<const elem_type &>(e)));
  }

  static const elem_type *next(const elem_type &e) noexcept {
    return get_node(e).m_next;
  }

  static elem_type *next(elem_type &e) noexcept {
    return const_cast<elem_type *>(next(const_cast<const elem_type &>(e)));
  }

  static const elem_type *prev(const elem_type &e) noexcept {
    return get_node(e).m_prev;
  }

  static elem_type *prev(elem_type &e) noexcept {
    return const_cast<elem_type *>(prev(const_cast<const elem_type &>(e)));
  }

  /** Pointer to list start, nullptr if empty. */
  elem_type *m_first_element{nullptr};

  /** Pointer to list end, nullptr if empty. */
  elem_type *m_last_element{nullptr};

  ut_list_base() = default;
  ut_list_base(ut_list_base &&) = delete;
  ut_list_base &operator=(ut_list_base &&) = delete;
  ut_list_base &operator=(const ut_list_base &) = delete;

  /**
   * Returns number of nodes currently present in the list.
   */
  size_t get_length() const noexcept{
    ut_ad(UT_LIST_IS_INITIALISED(*this));
    return count.load(std::memory_order_acquire);
  }

  /** Returns the number of nodes currently present in the list. */
  inline size_t size() const noexcept {
    return get_length();
  }

  /** Returns true if the list is empty. */
  inline bool empty() const noexcept {
    return size() == 0;
  }

  inline void push_back(elem_type *elem) noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));

    auto &elem_node = get_node(*elem);

    elem_node.m_next = nullptr;
    elem_node.m_prev = m_last_element;

    if (m_last_element != nullptr) {
      ut_ad(m_last_element != elem);

      get_node(*m_last_element).m_next = elem;
    }

    m_last_element = elem;

    if (m_first_element == nullptr) {
      m_first_element = elem;
    }

    update_length(1);
  }

  void push_front(elem_type *elem) noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));

    auto &elem_node = get_node(*elem);

    elem_node.m_prev = nullptr;
    elem_node.m_next = m_first_element;

    if (m_first_element != nullptr) {
      ut_ad(m_first_element != elem);

      get_node(*m_first_element).m_prev = elem;
    }

    m_first_element = elem;

    if (m_last_element == nullptr) {
      m_last_element = elem;
    }

    update_length(1);
  }

  void remove(elem_type *elem) noexcept {
    ut_a(!empty());
    ut_ad(UT_LIST_IS_INITIALISED(*this));

    auto &node = get_node(*elem);

    if (node.m_next != nullptr) {
      get_node(*node.m_next).m_prev = node.m_prev;
    } else {
      m_last_element = node.m_prev;
    }

    if (node.m_prev != nullptr) {
      get_node(*node.m_prev).m_next = node.m_next;
    } else {
      m_first_element = node.m_next;
    }

    node.m_next = nullptr;
    node.m_prev = nullptr;

    update_length(-1);
  }

  elem_type *front() noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));
    ut_ad(m_first_element != nullptr || empty());

    return m_first_element;
  }

  const elem_type *front() const noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));
    ut_ad(m_first_element != nullptr || empty());

    return m_first_element;
  }

  elem_type *back() noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));
    ut_ad(m_last_element != nullptr || empty());

    return m_last_element;
  }

  /**
   * Updates the length of the list by the amount specified.
   * 
   * @param[in  ] diff the value by which to increase the length. Can be negative.
   */
  void update_length(int diff) {
    ut_ad(diff > 0 || static_cast<size_t>(-diff) <= get_length());
    count.store(get_length() + diff, std::memory_order_release);
  }

  void clear() noexcept {
    ut_ad(UT_LIST_IS_INITIALISED(*this));
    m_first_element = nullptr;
    m_last_element = nullptr;
    count.store(0);
  }

  void reverse() noexcept {
    Type *tmp = m_first_element;
    m_first_element = m_last_element;
    m_last_element = tmp;
  }

#ifdef UNIV_DEBUG
  /**
   * UT_LIST_INITIALISED if the list was initialised with the constructor. It
   * is used to detect if the ut_list_base object is used directly after
   * allocating memory from malloc-like calls that do not run constructor.
   */
  ulint init{UT_LIST_INITIALISED};
#endif /* UNIV_DEBUG */

  /** Number of nodes in list. It is atomic to allow unprotected reads. Writes
  must be protected by some external latch. */
  std::atomic<size_t> count{0};

  template <typename E>
  struct base_iterator {
    using pointer = E*;
    using value_type = E;
    using reference = E&;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

    base_iterator(E *elem) : m_elem(elem) {}

    bool operator==(const base_iterator &other) const {
      return m_elem == other.m_elem;
    }

    bool operator!=(const base_iterator &other) const {
      return !(*this == other);
    }

    E *operator*() const {
      return m_elem;
    }

    base_iterator &operator++() {
      m_elem = next(*m_elem);
      return *this;
    }

    E *m_elem;
  };

  using iterator = base_iterator<elem_type>;
  using const_iterator = base_iterator<const elem_type>;

  iterator begin() {
    return m_first_element;
  }

  iterator end() {
    return nullptr;
  }

  const_iterator begin() const {
    return m_first_element;
  }

  const_iterator end() const {
    return nullptr;
  }

  /** A helper wrapper class for the list, which exposes begin(),end() iterators
  which let you remove the current item or items after it during the loop, while
  still having O(1) space and time complexity.
  NOTE: do not attempt to (re)move the previous element! */

  struct Removable {

    struct iterator {
      iterator(ut_list_base &list, elem_type *elem)
          : m_list{list},
            m_elem{elem},
            m_prev_elem{elem ? prev(*elem) : nullptr} {
        // We haven't really tested any other case yet:
        ut_ad(m_prev_elem == nullptr);
      }

      bool operator==(const iterator &other) const {
        return m_elem == other.m_elem;
      }

      bool operator!=(const iterator &other) const {
        return !(*this == other);
      }

      elem_type *operator*() const {
        return m_elem;
      }

      iterator &operator++() {
        /* if m_prev_elem existed before, then it should still belong to the
        list, which we verify partially here, by checking it's linked to next
        element or is the last. If this assert fails, it means the m_prev_elem
        was removed from the list during loop, which is violation of the
        contract with the user of .removable(). */
        ut_ad(!m_prev_elem || next(*m_prev_elem) || m_list.m_last_element == m_prev_elem);

        /* The reason this is so complicated is that we want to support cases in
        which the body of the loop removed not only the current element, but
        also some elements even further after it. */
        auto here = m_prev_elem == nullptr ? m_list.m_first_element : next(*m_prev_elem);

        if (here != m_elem) {
          m_elem = here;
        } else {
          m_prev_elem = m_elem;
          m_elem = next(*m_elem);
        }
        return *this;
      }

      ut_list_base &m_list;
      elem_type *m_elem;
      elem_type *m_prev_elem;
    };

    Removable(ut_list_base &list) : m_list{list} {}

    iterator begin() {
      return iterator{m_list, m_list.m_first_element};
    }

    iterator end() {
      return iterator{m_list, nullptr};
    }

    ut_list_base &m_list;
  };

  /**
   * Returns a wrapper which lets you remove current item or items after it.
   * It can be used like in this example:
   * for (auto lock : table->locks.removable()) {
   *   lock_remove_all_on_table_for_trx(table, lock->trx,..);
   * }
   * Or in general:
   * for (auto item : list.removable()) {
   *   remove_items_which_are_similar_to(item);
   * }
   * Basically you can remove any item, except for prev(item).
   * 
   * You can also insert to the list during iteration, keeping in mind that the
   * position you insert the element at has following impact:
   * - after the current item: the new item WILL be processed eventually,
   * - before the previous item: the new item WILL NOT be processed,
   * - right before the current item: DON'T DO IT, as you risk an endless loop!
   * A safe subcase of this is reinserting the current item, in which case it
   * won't be processed again. This lets you implement "move to front" easily.
   * 
   * @see Removable
   */
  Removable removable() {
    return Removable{*this};
  }
};

template <typename Type, ut_list_node<Type> Type::*node_ptr>
struct ut_list_base_explicit_getter {
  static const ut_list_node<Type> &get_node(const Type &element) {
    return element.*node_ptr;
  }
};

/**
 * A type of a list storing pointers to t, chained by member m of t.
 * NOTE: In cases in which definition of t is not yet in scope and thus you can't
 * refer to t::m at this point yet, use UT_LIST_BASE_NODE_T_EXTERN macro instead.
 */
#define UT_LIST_BASE_NODE_T(t, m) \
  ut_list_base<t, ut_list_base_explicit_getter<t, &t::m>>

/**
 * A helper for the UT_LIST_BASE_NODE_T_EXTERN which builds a name of a node
 * getter struct from the name of elem type t, and its member name m
 */
#define UT_LIST_NODE_GETTER(t, m) t##_##m##_node_getter

/**
 * A helper for the UT_LIST_BASE_NODE_T_EXTERN which declares a node getter
 * struct which extracts member m from element of type t. Note that the definition
 * of the get_node function is inline, so this declaration/definition can appear
 * multiple times in our codebase, and the intent is that you simply put it in the
 * header which defines member m of t for the first time, so that it is accessible.
 * This way all the places in codebase which know how to access m from t, will be
 * also able to use this node getter, and thus iterate over a list chained by it.
 * This also ensures, that for(auto elem: list) loops can be fully inlined by the
 * compiler as it can see through the get_node implementation, because each place
 * in code which knows that get_node exists also knows its implementation.
 */
#define UT_LIST_NODE_GETTER_DEFINITION(t, m) \
  struct UT_LIST_NODE_GETTER(t, m)           \
      : public ut_list_base_explicit_getter<t, &t::m> {}

/**
 * A variant of UT_LIST_BASE_NODE_T to be used in rare cases where the full
 * definition of t is not yet in scope, and thus UT_LIST_BASE_NODE_T can't be used
 * yet as it needs to know how to access member m of t. The trick used here is to
 * forward declare UT_LIST_NODE_GETTER(t,m) struct to be defined later by the
 * UT_LIST_NODE_GETTER_DEFINITION(t,m) once t::m is defined.
 */
#define UT_LIST_BASE_NODE_T_EXTERN(t, m) \
  ut_list_base<t, struct UT_LIST_NODE_GETTER(t, m)>

/**
 * Initializes the base node of a two-way list.
 * 
 * @param[in] b the list base node
 */
#define UT_LIST_INIT(b)                                            \
  {                                                                \
    auto &list_ref = (b);                                          \
    new (&list_ref) std::remove_reference_t<decltype(list_ref)>(); \
  }

/**
 * Adds the node as the first element in a two-way linked list.
 * 
 * @param[in] list the base node (not a pointer to it)
 * @param[in] elem the element to add
 */
template <typename List>
void ut_list_prepend(List &list, typename List::elem_type *elem) {
  list.push_front(elem);
}

/**
 * Adds the node as the first element in a two-way linked list.
 * 
 * @param[in] list the base node (not a pointer to it)
 * @param[in] elem the element to add
 */
#define UT_LIST_ADD_FIRST(LIST, ELEM) ut_list_prepend(LIST, ELEM)

/**
 * Adds the node as the last element in a two-way linked list.
 * 
 * @param[in] list the base node
 * @param[in] elem the element to add
 */
template <typename List>
void ut_list_append(List &list, typename List::elem_type *elem) {
  list.push_back(elem);
}

/**
 * Adds the node as the last element in a two-way linked list.
 * 
 * @param LIST list base node (not a pointer to it)
 * @param ELEM the element to add
 */
#define UT_LIST_ADD_LAST(LIST, ELEM) ut_list_append(LIST, ELEM)

/**
 * Inserts a ELEM2 after ELEM1 in a list.
 * 
 * @param[in] list the base node
 * @param[in] elem1 node after which ELEM2 is inserted
 * @param[in] elem2 node being inserted after ELEM1
 */
template <typename List>
void ut_list_insert(List &list, typename List::elem_type *elem1,
                    typename List::elem_type *elem2) {
  ut_ad(elem1 != elem2);
  ut_ad(elem1 != nullptr);
  ut_ad(elem2 != nullptr);
  ut_ad(UT_LIST_IS_INITIALISED(list));

  auto &elem1_node = List::get_node(*elem1);
  auto &elem2_node = List::get_node(*elem2);

  elem2_node.m_prev = elem1;
  elem2_node.m_next = elem1_node.m_next;
  ut_ad((elem2_node.m_next == nullptr) == (list.m_last_element == elem1));
  if (elem2_node.m_next != nullptr) {
    List::get_node(*elem2_node.m_next).m_prev = elem2;
  } else {
    list.m_last_element = elem2;
  }

  elem1_node.m_next = elem2;

  list.update_length(1);
}

/**
 * Inserts a ELEM2 after ELEM1 in a list.
 * 
 * @param[in] list the base node
 * @param[in] elem1 node after which elem2 is inserted
 * @param[in] elem2 node being inserted after elem1
 */
#define UT_LIST_INSERT_AFTER(LIST, ELEM1, ELEM2) \
  ut_list_insert(LIST, ELEM1, ELEM2)

/**
 * Removes a node from a two-way linked list.
 * 
 * @param[in] list the base node
 * @param[in] elem pointer to the element to remove from the list
 */
template <typename List>
void ut_list_remove(List &list, typename List::elem_type *elem) {
  list.remove(elem);
}

/**
 * Removes a node from a two-way linked list.
 * 
 * @param[in] list the base node
 * @param[in] elem node to be removed from the list
 */
#define UT_LIST_REMOVE(LIST, ELEM) ut_list_remove(LIST, ELEM)

/**
 * Gets the next node in a two-way list.
 * 
 * @param[in] name the name of the list
 * @param[in] n pointer to a node
 * 
 * @return the successor of n in name, or nullptr
 */
#define UT_LIST_GET_NEXT(NAME, N) (((N)->NAME).m_next)

/**
 * Gets the previous node in a two-way list.
 * 
 * @param[in] name the name of the list
 * @param[in] n pointer to a node
 * 
 * @return the predecessor of n in name, or nullptr
 */
#define UT_LIST_GET_PREV(NAME, N) (((N)->NAME).m_prev)

/**
 * Alternative macro to get the number of nodes in a two-way list, i.e.,
 * its length.
 * 
 * @param[in] base the base node (not a pointer to it)
 * 
 * @return the number of nodes in the list
 */
#define UT_LIST_GET_LEN(BASE) (BASE).get_length()

template <typename List>
inline auto ut_list_get_len(List& list) {
  ut_ad(UT_LIST_IS_INITIALISED(list));
  return list.get_length();
}

/**
 * Gets the first node in a two-way list.
 * 
 * @param[in] base the base node (not a pointer to it)
 * 
 * @return first node, or nullptr if the list is empty
 */
#define UT_LIST_GET_FIRST(BASE) ut_list_get_first(BASE)

template <typename List>
inline auto ut_list_get_first(List& list) {
  ut_ad(UT_LIST_IS_INITIALISED(list));
  return list.m_first_element;
}

/**
 * Gets the last node in a two-way list.
 * 
 * @param[in] base the base node (not a pointer to it)
 * 
 * @return last node, or nullptr if the list is empty
 */
#define UT_LIST_GET_LAST(BASE) ut_list_get_last(BASE)

template <typename List>
inline auto ut_list_get_last(List& list) {
  ut_ad(UT_LIST_IS_INITIALISED(list));
  return list.m_last_element;
}

struct NullValidate {
  void operator()(const void *) {}
};

/**
 * Iterate over all the elements and call the functor for each element.
 * 
 * @param[in]     list    base node (not a pointer to it)
 * @param[in,out] functor Functor that is called for each element in the list
 */
template <typename List, class Functor>
void ut_list_map(const List &list, Functor &functor) {
  size_t count = 0;

  ut_ad(UT_LIST_IS_INITIALISED(list));

  for (auto elem : list) {
    functor(elem);
    ++count;
  }

  ut_a(count == list.get_length());
}

template <typename List>
void ut_list_reverse(List &list) {
  ut_ad(UT_LIST_IS_INITIALISED(list));
  // NOTE: we use List::prev to iterate forward as .reverse() swaps arrows
  for (auto elem = list.first_element; elem != nullptr;
       elem = List::prev(*elem)) {
    List::get_node(*elem).reverse();
  }

  list.reverse();
}

#define UT_LIST_REVERSE(LIST) ut_list_reverse(LIST)

/**
 * Checks the consistency of a two-way list.
 * 
 * @param[in] list base node (not a pointer to it)
 * @param[in,out] functor Functor that is called for each element in the list
 */
template <typename List, class Functor>
void ut_list_validate(const List &list, Functor &functor) {
  ut_list_map(list, functor);
  /* Validate the list backwards. */
  size_t count = 0;

  for (auto elem = list.m_last_element; elem != nullptr;
       elem = List::prev(*elem)) {
    ++count;
  }

  ut_a(count == list.get_length());
}

/**
 * Check the consistency of a two-way list.
 * 
 * @param[in] list base node reference
 */
#define UT_LIST_CHECK(LIST)        \
  do {                             \
    NullValidate nullV;            \
    ut_list_validate(LIST, nullV); \
  } while (0)

/**
 * Move the given element to the beginning of the list.
 * 
 * @param[in,out] list the list object
 * @param[in] elem the element of the list which will be moved to the beginning of the list.
 *   to the beginning of the list.
 */
template <typename List>
void ut_list_move_to_front(List &list, typename List::elem_type *elem) {
  ut_ad(ut_list_exists(list, elem));

  if (list.first_element != elem) {
    ut_list_remove(list, elem);
    ut_list_prepend(list, elem);
  }
}

#ifdef UNIV_DEBUG
/**
 * Check if the given element exists in the list.
 * 
 * @param[in,out] list the list object
 * @param[in] elem the element of the list which will be checked
 * 
 * @return true if the element exists in the list, false otherwise
 */
template <typename List>
bool ut_list_exists(List &list, typename List::elem_type *elem) {
  ut_ad(UT_LIST_IS_INITIALISED(list));
  for (auto e1 : list) {
    if (elem == e1) {
      return true;
    }
  }
  return false;
}
#endif /* UNIV_DEBUG */
