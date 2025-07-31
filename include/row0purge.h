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

/**
 * @brief Row purge node for removing obsolete records.
 * 
 * This structure represents a purge operation node that handles the removal
 * of obsolete records from both clustered and secondary indexes. It processes
 * undo log records to determine which records need to be purged.
 */
struct Row_purge {
  Row_purge() : m_pcur(srv_fsp, srv_btree_sys) {}

  /**
   * @brief Repositions the persistent cursor to the clustered index record.
   * 
   * If the cursor was previously positioned on a clustered index record,
   * restores the position. Otherwise, searches for the clustered index record
   * using the row reference and positions the cursor on it.
   * 
   * @param latch_mode Latch mode for the operation (BTR_SEARCH_LEAF, etc.)
   * @param mtr Mini-transaction for the operation
   * @return true if the record was found and cursor positioned, false otherwise
   */
  bool reposition_pcur(ulint latch_mode, mtr_t *mtr);

  /**
   * @brief Attempts to remove a clustered index record with specified mode.
   * 
   * This is a low-level function that tries to remove a clustered index record
   * using either optimistic (leaf-level) or pessimistic (tree-level) deletion.
   * Verifies that the roll pointer matches before deletion to ensure the
   * correct record version is being purged.
   * 
   * @param mode Deletion mode (BTR_MODIFY_LEAF or BTR_MODIFY_TREE)
   * @return true if successful or record already removed, false if operation failed
   */
  bool remove_clust_if_poss_low(ulint mode);

  /**
   * @brief Removes a clustered index record if possible.
   * 
   * First attempts optimistic deletion (leaf-level), and if that fails,
   * retries with pessimistic deletion (tree-level). Includes retry logic
   * for handling temporary failures due to file space constraints.
   */
  void remove_clust_if_poss();

  /**
   * @brief Attempts to remove a secondary index record with specified mode.
   * 
   * Searches for the secondary index record and removes it if no later version
   * of the row requires its existence. Uses version visibility checks to
   * determine if the record can be safely purged.
   * 
   * @param index Secondary index from which to remove the record
   * @param entry Index entry tuple representing the record to remove
   * @param mode Deletion mode (BTR_MODIFY_LEAF or BTR_MODIFY_TREE)
   * @return true if successful or record not found, false if operation failed
   */
  bool remove_sec_if_poss_low(Index *index, const DTuple *entry, ulint mode);

  /**
   * @brief Removes a secondary index record if possible.
   * 
   * Attempts to remove the secondary index record using optimistic deletion
   * first, then pessimistic deletion if needed. Includes retry logic for
   * handling temporary failures.
   * 
   * @param index Secondary index from which to remove the record
   * @param entry Index entry tuple representing the record to remove
   */
  void remove_sec_if_poss(Index *index, DTuple *entry);

  /**
   * @brief Purges records marked for deletion.
   * 
   * Processes a delete-marked record by building index entries for all
   * secondary indexes and attempting to remove them, followed by removal
   * of the clustered index record.
   */
  void del_mark();

  /**
   * @brief Purges records affected by updates or with external storage.
   * 
   * Handles purging of updated records and cleanup of externally stored fields.
   * For updated records, removes old versions of secondary index entries that
   * have changed. For records with external storage, frees the externally
   * stored field data.
   */
  void upd_exist_or_extern();

  /**
   * @brief Parses an undo log record to extract purge information.
   * 
   * Extracts necessary information from the undo log record including record type,
   * table information, row reference, and update vector. Determines if purge
   * processing is needed based on the record type and update information.
   * 
   * @param updated_extern Output parameter indicating if external fields were updated
   * @param thr Query execution thread
   * @return true if purge processing is needed, false if record can be skipped
   */
  bool parse_undo_rec(bool *updated_extern, que_thr_t *thr);

  /**
   * @brief Performs the main purge operation for a single undo log record.
   * 
   * This is the main entry point for purge processing. Fetches the next undo
   * record, parses it, and performs the appropriate purge operations based on
   * the record type (delete-marked, updated, or with external storage).
   * 
   * @param thr Query execution thread
   * @return DB_SUCCESS on successful completion
   */
  ulint purge(que_thr_t *thr);

  /**
   * @brief Creates a new purge node for query execution.
   * 
   * Allocates and initializes a new Row_purge node with the specified parent
   * thread and memory heap. Sets up the node for integration into the query
   * execution graph.
   * 
   * @param parent Parent query thread node
   * @param heap Memory heap for allocation
   * @return Pointer to the newly created purge node
   */
  static Row_purge *node_create(que_thr_t *parent, mem_heap_t *heap);

  /**
   * @brief Executes one step of the purge operation.
   * 
   * This is the main execution function called by the query execution engine.
   * Performs one purge operation and returns the thread for continued execution.
   * 
   * @param thr Query execution thread
   * @return Query thread to run next
   */
  static que_thr_t *step(que_thr_t *thr);

  /** node type: QUE_NODE_PURGE */
  que_common_t m_common;

  /*----------------------*/
  /* Local storage for this graph node */

  /** roll pointer to undo log record */
  roll_ptr_t m_roll_ptr;

  /** undo log record */
  trx_undo_rec_t *m_undo_rec;

  /** reservation for the undo log record in the purge array */
  trx_undo_inf_t *m_reservation;

  /** undo number of the record */
  undo_no_t m_undo_no;

  /* undo log record type: TRX_UNDO_INSERT_REC, ... */
  ulint m_rec_type;

  /** persistent cursor used in searching the clustered index record */
  Btree_pcursor m_pcur;

  /* true if the clustered index record determined by ref was found in the clustered index, and we were able to position pcur on it */
  bool m_found_clust;

  /** table where purge is done */
  Table *m_table;

  /* compiler analysis info of an update */
  ulint m_cmpl_info;

  /** update vector for a clustered index record */
  upd_t *m_update;

  /** nullptr, or row reference to the next row to handle */
  DTuple *m_ref;

  /** nullptr, or a copy (also fields copied to heap) of the indexed fields of the row to handle */
  DTuple *m_row;

  /** nullptr, or the next index whose record should be handled */
  Index *m_index;

  /** memory heap used as auxiliary storage for row; this must be emptied after a successful purge of a row */
  mem_heap_t *m_heap;
};

/**
 * @brief Creates a purge node for integration into a query graph.
 * 
 * This is a C-style wrapper function that creates a Row_purge node
 * and returns it as a Row_purge pointer for compatibility with
 * the existing query execution framework.
 * 
 * @param parent Parent query thread node
 * @param heap Memory heap for allocation
 * @return Pointer to the newly created purge node
 */
Row_purge *row_purge_node_create(que_thr_t *parent, mem_heap_t *heap);

/**
 * @brief Executes one purge operation step in the query execution graph.
 * 
 * This is a C-style wrapper function that performs one step of the purge
 * operation. It's designed to be used within the SQL execution graph
 * framework for processing undo log records.
 * 
 * @param thr Query execution thread
 * @return Query thread to run next, or nullptr if no more work
 */
que_thr_t *row_purge_step(que_thr_t *thr);
