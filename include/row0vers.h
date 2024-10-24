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

#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "read0types.h"
#include "rem0types.h"
#include "trx0types.h"

/** Finds out if an active transaction has inserted or modified a secondary
index record. NOTE: the kernel mutex is temporarily released in this
function!
@return nullptr if committed, else the active transaction */
Trx *row_vers_impl_x_locked_off_kernel(
  const rec_t *rec,    /*!< in: record in a secondary index */
  const Index *index, /*!< in: the secondary index */
  const ulint *offsets
); /*!< in: Phy_rec::get_col_offsets(rec, index) */

/** Finds out if we must preserve a delete marked earlier version of a clustered
index record, because it is >= the purge view.
@return	true if earlier version should be preserved */
bool row_vers_must_preserve_del_marked(
  trx_id_t trx_id, /*!< in: transaction id in the version */
  mtr_t *mtr
); /*!< in: mtr holding the latch on the
                     clustered index record; it will also
                     hold the latch on purge_view */

/** Finds out if a version of the record, where the version >= the current
purge view, should have ientry as its secondary index entry. We check
if there is any not delete marked version of the record where the trx
id >= purge view, and the secondary index entry == ientry; exactly in
this case we return true.
@return	true if earlier version should have */
bool row_vers_old_has_index_entry(
  bool also_curr,      /*!< in: true if also rec is included in the
                            versions to search; otherwise only versions
                            prior to it are searched */
  const rec_t *rec,    /*!< in: record in the clustered index; the
                             caller must have a latch on the page */
  mtr_t *mtr,          /*!< in: mtr holding the latch on rec; it will
                             also hold the latch on purge_view */
  Index *index, /*!< in: the secondary index */
  const DTuple *ientry
); /*!< in: the secondary index entry */

/** Constructs the version of a clustered index record which a consistent
read should see. We assume that the trx id stored in rec is such that
the consistent read should not see rec in its present version.
@return	DB_SUCCESS or DB_MISSING_HISTORY */
db_err row_vers_build_for_consistent_read(
  const rec_t *rec,         /*!< in: record in a clustered index; the
                              caller must have a latch on the page; this
                              latch locks the top of the stack of versions
                              of this records */
  mtr_t *mtr,               /*!< in: mtr holding the latch on rec; it will
                              also hold the latch on purge_view */
  Index *index,      /*!< in: the clustered index */
  ulint **offsets,          /*!< in/out: offsets returned by
                              Phy_rec::get_col_offsets(rec, index) */
  read_view_t *view,        /*!< in: the consistent read view */
  mem_heap_t **offset_heap, /*!< in/out: memory heap from which
                          the offsets are allocated */
  mem_heap_t *in_heap,      /*!< in: memory heap from which the memory for
                              *old_vers is allocated; memory for possible
                              intermediate versions is allocated and freed
                              locally within the function */
  rec_t **old_vers
); /*!< out, own: old version, or nullptr if the
                            record does not exist in the view, that is,
                            it was freshly inserted afterwards */

/** Constructs the last committed version of a clustered index record,
which should be seen by a semi-consistent read.
@return	DB_SUCCESS or DB_MISSING_HISTORY */
ulint row_vers_build_for_semi_consistent_read(
  const rec_t *rec,         /*!< in: record in a clustered index; the
                              caller must have a latch on the page; this
                              latch locks the top of the stack of versions
                              of this records */
  mtr_t *mtr,               /*!< in: mtr holding the latch on rec */
  Index *index,      /*!< in: the clustered index */
  ulint **offsets,          /*!< in/out: offsets returned by
                              Phy_rec::get_col_offsets(rec, index) */
  mem_heap_t **offset_heap, /*!< in/out: memory heap from which
                          the offsets are allocated */
  mem_heap_t *in_heap,      /*!< in: memory heap from which the memory for
                              *old_vers is allocated; memory for possible
                              intermediate versions is allocated and freed
                              locally within the function */
  const rec_t **old_vers
); /*!< out: rec, old version, or nullptr if the
                              record does not exist in the view, that is,
                            it was freshly inserted afterwards */
