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

/** @file include/row0vers.h
Row versions

Created 2/6/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0types.h"
#include "trx0types.h"

struct Trx;
struct mtr_t;
struct Index;
struct DTuple;
struct read_view_t;
struct Lock_sys;

struct Row_vers {

  /** A version of a clustered index record */
  struct Row {
    /** Record in a clustered index */
    const rec_t *m_cluster_rec{};

    /** Mtr holding the latch on m_cluster_rec */
    mtr_t *m_mtr{};

    /** The clustered index */
    Index *m_cluster_index{};

    /** Offsets returned by Phy_rec::get_col_offsets(rec, index) */
    ulint *&m_cluster_offsets;

    /** The consistent read view */
    read_view_t *m_consistent_read_view{};

    /** Memory heap from which the offsets are allocated */
    mem_heap_t *&m_cluster_offset_heap;

    /** Memory heap from which the memory for *m_old_vers is allocated */
    mem_heap_t *&m_old_row_heap;

    /** Old version, or nullptr if the record does not exist in the view,
     * that is, it was freshly inserted afterwards */
    const rec_t *&m_old_rec;
  };

  /**
   * @brief Constructor.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] lock_sys The lock system.
   */
  Row_vers(Trx_sys *trx_sys, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Creates a Row_vers.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] lock_sys The lock system.
   */
  [[nodiscard]] static Row_vers *create(Trx_sys *trx_sys, Lock_sys *lock_sys) noexcept;

  /**
   * @brief Destroys a Row_vers.
   *
   * @param[in,out] row_vers The Row_vers to destroy.
   */
  static void destroy(Row_vers *&row_vers) noexcept;

  /**
   * @brief Finds out if an active transaction has inserted or modified a secondary index record.
   *
   * NOTE: the trx system mutex is temporarily released in this function!
   *
   * @param[in] rec Record in a secondary index.
   * @param[in] index The secondary index.
   * @param[in] offsets Phy_rec::get_col_offsets(rec, index).
   *
   * @return nullptr if committed, else the active transaction.
   */
  [[nodiscard]] Trx *impl_x_locked_off_trx_sys_mutex(const rec_t *rec, const Index *index, const ulint *offsets) noexcept;

  /**
   * @brief  Finds out if we must preserve a delete marked earlier version of a clustered
   * index record, because it is >= the purge view.
   *
   * @param[in] trx_id Transaction id in the version.
   * @param[in] mtr Mtr holding the latch on the clustered index record. It will also hold the latch on purge_view.
   *
   * @return true if earlier version should be preserved.
   */
  [[nodiscard]] bool must_preserve_del_marked(trx_id_t trx_id, mtr_t *mtr) noexcept;

  /**
   * @brief Finds out if a version of the record, where the version >= the current
   * purge view, should have ientry as its secondary index entry. We check
   * if there is any not delete marked version of the record where the trx
   * id >= purge view, and the secondary index entry == ientry; exactly in
   * this case we return true.
   *
   * @param[in] also_curr true if also rec is included in the versions to search; otherwise only versions prior to rec are searched.
   * @param[in] rec Record in a clustered index; the caller must have a latch on the page.
   * @param[in] mtr Mtr holding the latch on the clustered index record. It will also hold the latch on purge_view.
   * @param[in] index The secondary index.
   * @param[in] entry The secondary index entry.
   *
   * @return true if earlier version should have ientry as its secondary index entry.
   */
  [[nodiscard]] bool old_has_index_entry(bool also_curr, const rec_t *rec, mtr_t *mtr, Index *index, const DTuple *entry) noexcept;

  /**
   * @brief Constructs the version of a clustered index record which a consistent
   * read should see.
   *
   * @param[in,out] row Version of a clustered index record.
   *
   * @return DB_SUCCESS or DB_MISSING_HISTORY
   */
  [[nodiscard]] db_err build_for_consistent_read(Row &row) noexcept;

  /**
   * @brief Constructs the last committed version of a clustered index record,
   * which should be seen by a semi-consistent read.
   *
   * @param[in,out] row  Version of a clustered index record.
   *
   * @return DB_SUCCESS or DB_MISSING_HISTORY
   */
  [[nodiscard]] db_err build_for_semi_consistent_read(Row &row) noexcept;

#ifndef UNIT_TEST
 private:
#endif /* !UNIT_TEST */

  /** The transaction system */
  Trx_sys *m_trx_sys{};

  /** The lock system */
  Lock_sys *m_lock_sys{};
};
