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
#include "ut0hlc.h"
#include "ut0lflist.h"

struct Trx;

struct TrxSys;

/**
 * A read view is a timestamp: see READ VIEWS in trx0sys.h. Opening one is O(1): a
 * slot, and two loads. There is no list of ids.
 */
struct ReadView {
  /** Check whether the changes by id are visible.
  @param[in]    id      transaction id to check against the view
  @return whether the view sees the modifications of id. */
  [[nodiscard]] bool changes_visible(trx_id_t id) const;

  /** Prints a read view to stderr.
   * @return string representation of the read view
   */
  std::string to_string() const noexcept;

  /**
   * Checks if a read view sees the specified transaction: the rules of READ VIEWS.
   *
   * @param trx_id in: trx id
   * @return true if sees
   */
  inline bool sees_trx_id(trx_id_t trx_id) const {
    /* Its own changes. Not for a cursor view: its creator is like any active
    transaction, RowVers looks at the undo number. */
    if (trx_id == m_creator_trx_id && m_type == ReadViewType::NORMAL) {
      return true;
    }

    if (trx_id < m_watermark) {
      return true;
    }

    if ((trx_id & ~ut::HLC_NODE_MASK) > m_snap_word) {
      return false;
    }

    return lookup(trx_id);
  }

  /** @return true if the commit table says the view sees id. */
  [[nodiscard]] bool lookup(trx_id_t id) const noexcept;

  /** Read view type */
  ReadViewType m_type;

  /** 0 or if m_type is HIGH_GRANULARITY transaction undo_no when
  this high-granularity consistent read view was created */
  undo_no_t m_undo_no;

  /** The snapshot, S: the word of the clock (ut0hlc.h) at the open. The view sees a
  commit whose stamp is <= S. */
  uint64_t m_snap_word;

  /** W: all the ids below this one were committed, with a stamp <= S. */
  trx_id_t m_watermark;

  /** The purge view only: undo logs of a transaction number below this can go. */
  trx_id_t m_low_limit_no;

  /** trx id of creating transaction, or 0 used in purge */
  trx_id_t m_creator_trx_id;

  /** The slot of the snapshot. */
  ut::SnapSlot *m_slot;

  /** The transaction system of its commit table. */
  TrxSys *m_trx_sys;
};

/** Read view types @{ */

[[nodiscard]] inline bool ReadView::changes_visible(trx_id_t id) const {
  return sees_trx_id(id);
}
