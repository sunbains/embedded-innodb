/*****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file include/ut0concepts.h
C++20 concepts for InnoDB utilities

Created 2025-07-26 by Sunny Bains
*******************************************************/

#pragma once

#include <concepts>
#include <functional>
#include <type_traits>

/** Concept for types that can be hashed */
template <typename T>
concept Hashable = requires(T a) {
  { std::hash<T>{}(a) } -> std::convertible_to<std::size_t>;
};

/** Concept for types that are comparable */
template <typename T>
concept Comparable = requires(T a, T b) {
  { a == b } -> std::convertible_to<bool>;
  { a != b } -> std::convertible_to<bool>;
  { a < b } -> std::convertible_to<bool>;
};

/** Concept for types that are page-like (have space_id and page_no) */
template <typename T>
concept PageLike = requires(T a) {
  { a.space_id() } -> std::convertible_to<space_id_t>;
  { a.page_no() } -> std::convertible_to<page_no_t>;
};

/** Concept for types that can be used as buffer pool keys */
template <typename T>
concept BufferPoolKey = PageLike<T> && Hashable<T> && Comparable<T>;

/** Concept for arithmetic types (extends std::integral and std::floating_point) */
template <typename T>
concept Arithmetic = std::integral<T> || std::floating_point<T>;

/** Concept for types that can be formatted */
template <typename T>
concept Formattable = requires(T a) {
  { a.to_string() } -> std::convertible_to<std::string>;
};
