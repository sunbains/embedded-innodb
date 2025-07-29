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

/** @file include/ut0expected.h
C++23 std::expected utilities for error handling

Created 2025-07-26 by Sunny Bains
*******************************************************/

#pragma once

#include <expected>
#include "innodb.h"

/** Type alias for expected results with InnoDB error codes */
template<typename T>
using Expected = std::expected<T, ib_err_t>;

/** Type alias for void expected results */
using VoidExpected = std::expected<void, ib_err_t>;

/** Helper function to create an unexpected error result */
constexpr auto make_unexpected(ib_err_t err) noexcept {
  return std::unexpected(err);
}

/** Helper macro for early return on error */
#define TRY_EXPECTED(expr) \
  do { \
    auto result = (expr); \
    if (!result) { \
      return make_unexpected(result.error()); \
    } \
  } while (0)

/** Helper macro for early return on error with value extraction */
#define TRY_EXPECTED_VALUE(var, expr) \
  auto var##_result = (expr); \
  if (!var##_result) { \
    return make_unexpected(var##_result.error()); \
  } \
  auto var = std::move(*var##_result)
