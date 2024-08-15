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

/** @file include/row0purge.h
Purge obsolete records

Created 3/14/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0pcur.h"
#include "btr0types.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "que0types.h"
#include "row0types.h"
#include "trx0types.h"

/** Creates a purge node to a query graph.
@return	own: purge node */
purge_node_t *row_purge_node_create(
  que_thr_t *parent, /** in: parent node, i.e., a thr node */
  mem_heap_t *heap
); /** in: memory heap where created */

/** Does the purge operation for a single undo log record. This is a high-level
function used in an SQL execution graph.
@return	query thread to run next or nullptr */
que_thr_t *row_purge_step(que_thr_t *thr); /** in: query thread */

/* Purge node structure */

struct purge_node_struct {
  /** node type: QUE_NODE_PURGE */
  que_common_t common;

  /*----------------------*/
  /* Local storage for this graph node */

  /** roll pointer to undo log record */
  roll_ptr_t roll_ptr;

  /** undo log record */
  trx_undo_rec_t *undo_rec;

  /** reservation for the undo log record in the purge array */
  trx_undo_inf_t *reservation;

  /** undo number of the record */
  undo_no_t undo_no;

  /* undo log record type: TRX_UNDO_INSERT_REC, ... */
  ulint rec_type;

  /** persistent cursor used in searching the clustered index record */
  btr_pcur_t pcur;

  /* true if the clustered index record determined by ref was found in the clustered index, and we were able to position pcur on it */
  bool found_clust;

  /** table where purge is done */
  dict_table_t *table;

  /* compiler analysis info of an update */
  ulint cmpl_info;

  /** update vector for a clustered index record */
  upd_t *update;

  /** nullptr, or row reference to the next row to handle */
  dtuple_t *ref;

  /** nullptr, or a copy (also fields copied to heap) of the indexed fields of the row to handle */
  dtuple_t *row;

  /** nullptr, or the next index whose record should be handled */
  dict_index_t *index;

  /** memory heap used as auxiliary storage for row; this must be emptied after a successful purge of a row */
  mem_heap_t *heap;
};
