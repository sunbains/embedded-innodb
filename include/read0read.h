/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/read0read.h
Cursor read

Created 2/16/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "read0types.h"
#include "ut0byte.h"

struct Trx;

/**
 * Read_view class containing read view data and operations.
 */
struct Read_view {
  /** Check whether the changes by id are visible.
  @param[in]    id      transaction id to check against the view
  @return whether the view sees the modifications of id. */
  [[nodiscard]] bool changes_visible(trx_id_t id) const;

  /** Prints a read view to stderr.
   * @return string representation of the read view
   */
  std::string to_string() const noexcept;

  /**
   * Gets the nth trx id in a read view.
   *
   * @param n in: position
   * @return trx id
   */
  inline trx_id_t get_nth_trx_id(ulint n) const {
    ut_ad(n < m_n_trx_ids);

    return m_trx_ids[n];
  }

  /**
   * Sets the nth trx id in a read view.
   *
   * @param n in: position
   * @param trx_id in: trx id to set
   */
  inline void set_nth_trx_id(ulint n, trx_id_t trx_id) {
    ut_ad(n < m_n_trx_ids);

    m_trx_ids[n] = trx_id;
  }

  /**
   * Checks if a read view sees the specified transaction.
   *
   * @param trx_id in: trx id
   * @return true if sees
   */
  inline bool sees_trx_id(trx_id_t trx_id) const {
    if (trx_id < m_up_limit_id) {

      return true;
    }

    if (trx_id >= m_low_limit_id) {

      return false;
    }

    /* We go through the trx ids in the array smallest first: this order
    may save CPU time, because if there was a very long running
    transaction in the trx id array, its trx id is looked at first, and
    the first two comparisons may well decide the visibility of trx_id. */

    const auto n_ids = m_n_trx_ids;

    for (ulint i = 0; i < n_ids; ++i) {

      int cmp = trx_id - get_nth_trx_id(n_ids - i - 1);

      if (cmp <= 0) {
        return cmp < 0;
      }
    }

    return true;
  }

  /** Read view type */
  Read_view_type m_type;

  /** 0 or if m_type is HIGH_GRANULARITY transaction undo_no when
  this high-granularity consistent read view was created */
  undo_no_t m_undo_no;

  /** The view does not need to see the undo logs for transactions
  whose transaction number is strictly smaller (<) than this value:
  they can be removed in purge if not needed by other views */
  trx_id_t m_low_limit_no;

  /** The read should not see any transaction with trx id >= this value.
  In other words, this is the "high water mark". */
  trx_id_t m_low_limit_id;

  /** The read should see all trx ids which are strictly smaller (<) than
  this value.  In other words, this is the "low water mark". */
  trx_id_t m_up_limit_id;

  /** Number of cells in the m_trx_ids array */
  ulint m_n_trx_ids;

  /** Additional trx ids which the read should not see: typically, these are
  the active transactions at the time when the read is serialized, except the
  reading transaction itself; the trx ids in this array are in a descending
  order. These m_trx_ids should be between the "low" and "high" water marks,
  that is, m_up_limit_id and m_low_limit_id. */
  trx_id_t *m_trx_ids;

  /** trx id of creating transaction, or 0 used in purge */
  trx_id_t m_creator_trx_id;

  /** List of read views in srv_trx_sys */
  UT_LIST_NODE_T(Read_view) m_view_list;
};

UT_LIST_NODE_GETTER_DEFINITION(Read_view, m_view_list);

/** Read view types @{ */

[[nodiscard]] inline bool Read_view::changes_visible(trx_id_t id) const {
  return sees_trx_id(id);
}
