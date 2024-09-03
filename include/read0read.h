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

struct trx_t;

/**
 * Opens a read view where exactly the transactions serialized before this
 * point in time are seen in the view.
 *
 * @param cr_trx_id trx_id of creating transaction, or 0 used in purge
 * @param heap memory heap from which allocated
 * @return own: read view struct
 */
read_view_t *read_view_open_now(trx_id_t cr_trx_id, mem_heap_t *heap);

/**
 * Makes a copy of the oldest existing read view, or opens a new. The view
 * must be closed with ..._close.
 *
 * @param cr_trx_id trx_id of creating transaction, or 0 used in purge
 * @param heap memory heap from which allocated
 * @return own: read view struct
 */
read_view_t *read_view_oldest_copy_or_open_new(trx_id_t cr_trx_id, mem_heap_t *heap);

/** Closes a read view.
 * @param view read view
 */
void read_view_close(read_view_t *view);

/** Closes a consistent read view for client. This function is called at an SQL
 * statement end if the trx isolation level is <= TRX_ISO_READ_COMMITTED.
 * @param trx trx which has a read view
 */
void read_view_close_for_read_committed(trx_t *trx);

/** Prints a read view to stderr.
 * @param view read view
 */
std::string to_string(const read_view_t *view) noexcept;

/** Create a consistent cursor view to be used in cursors. In this
 * consistent read view modifications done by the creating transaction or future
 * transactions are not visible.
 * @param cr_trx trx where cursor view is created
 * @return cursor view
 */
cursor_view_t *read_cursor_view_create(trx_t *cr_trx);

/** Close a given consistent cursor view and restore global read view
 * back to a transaction read view.
 * @param trx trx
 * @param curview cursor view to be closed
 */
void read_cursor_view_close(trx_t *trx, cursor_view_t *curview);

/**
 * This function sets a given consistent cursor view to a transaction
 * read view if given consistent cursor view is not nullptr. Otherwise, function
 * restores a global read view to a transaction read view.
 *
 * @param trx in: transaction where cursor is set
 * @param curview in: consistent cursor view to be set
 */
void read_cursor_set(trx_t *trx, cursor_view_t *curview);

/** Read view types @{ */

/**
 * Gets the nth trx id in a read view.
 *
 * @param view in: read view
 * @param n in: position
 * @return trx id
 */
inline trx_id_t read_view_get_nth_trx_id( const read_view_t *view, ulint n) {
  ut_ad(n < view->n_trx_ids);

  return view->trx_ids[n];
}

/**
 * Sets the nth trx id in a read view.
 *
 * @param view in: read view
 * @param n in: position
 * @param trx_id in: trx id to set
 */
inline void read_view_set_nth_trx_id(read_view_t *view, ulint n, trx_id_t trx_id) {
  ut_ad(n < view->n_trx_ids);

  view->trx_ids[n] = trx_id;
}

/**
 * Checks if a read view sees the specified transaction.
 *
 * @param view in: read view
 * @param trx_id in: trx id
 * @return true if sees
 */
inline bool read_view_sees_trx_id(const read_view_t *view, trx_id_t trx_id) {
  if (trx_id < view->up_limit_id) {

    return true;
  }

  if (trx_id >= view->low_limit_id) {

    return false;
  }

  /* We go through the trx ids in the array smallest first: this order
  may save CPU time, because if there was a very long running
  transaction in the trx id array, its trx id is looked at first, and
  the first two comparisons may well decide the visibility of trx_id. */

  const auto n_ids = view->n_trx_ids;

  for (ulint i = 0; i < n_ids; i++) {

    int cmp = trx_id - read_view_get_nth_trx_id(view, n_ids - i - 1);

    if (cmp <= 0) {
      return cmp < 0;
    }
  }

  return true;
}

[[nodiscard]] inline bool read_view_t::changes_visible(trx_id_t id) const {
  return read_view_sees_trx_id(this, id);
}

