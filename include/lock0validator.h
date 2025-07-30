/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/lock0validator.h
Lock system validation and debugging

*******************************************************/

#pragma once

#include "lock0types.h"

struct Buf_block;
struct Index;
struct Table;
struct Lock_sys;
using rec_t = byte;

/**
 * @brief Lock system validator for debugging and integrity checking
 * 
 * This class provides functionality to validate the integrity of the lock system,
 * including lock queues, transaction states, and lock consistency across the system.
 */
struct Lock_validator {
  /**
   * @brief Constructor for Lock_validator
   * 
   * @param[in] lock_sys Pointer to the main lock system
   */
  explicit Lock_validator(Lock_sys *lock_sys) noexcept;

#ifdef UNIV_DEBUG
  /**
   * @brief Validates the lock system.
   *
   * This function iterates through all transactions and their associated locks,
   * validating the lock table queues and record lock queues to ensure the integrity
   * of the lock system.
   *
   * @return true if the lock system is valid, false otherwise.
   */
  [[nodiscard]] bool validate() noexcept;

  /**
   * @brief Validates the record lock queues on a page.
   *
   * This function checks the validity of the record lock queues for a specific page
   * identified by its page ID. It ensures that the locks on the page
   * are consistent and correctly maintained.
   *
   * @param[in] page_id The page ID to validate.
   * @return true if the record lock queues on the page are valid, false otherwise.
   */
  [[nodiscard]] bool rec_validate_page(Page_id page_id) noexcept;

  /**
   * @brief Validates the lock queue on a table.
   *
   * This function checks the validity of the lock queue for a given table.
   * It ensures that the transactions holding the locks are in a valid state
   * and that there are no incompatible locks in the queue.
   *
   * @param[in] table The table whose lock queue is to be validated.
   * @return true if the lock queue is valid, false otherwise.
   */
  [[nodiscard]] bool table_queue_validate(Table *table) noexcept;

  /**
   * @brief Validates the lock queue on a single record.
   *
   * This function checks the validity of the lock queue for a specific record
   * within a buffer block. It ensures that the record and block are properly
   * aligned and that the offsets are valid. It also verifies the state of the
   * transactions holding the locks and checks for any waiting locks.
   *
   * @param[in] block   Buffer block containing the record.
   * @param[in] rec     Record to validate.
   * @param[in] index   Index, or nullptr if not known.
   * @param[in] offsets Column offsets obtained from Phy_rec::get_col_offsets(rec, index).
   * @return true if the lock queue is valid, false otherwise.
   */
  [[nodiscard]] bool rec_queue_validate(
    const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets
  ) noexcept;

  /**
   * @brief Checks if some other transaction has a lock request in the queue.
   *
   * This function determines whether any other transaction has a lock request
   * on the specified record in the queue. It can take into account gap locks
   * and waiting locks based on the provided parameters.
   *
   * @param[in] page_id  The page id containing the record.
   * @param[in] mode     The lock mode to check for, either LOCK_S or LOCK_X.
   * @param[in] gap      LOCK_GAP if gap locks should be considered, or 0 if not.
   * @param[in] wait     LOCK_WAIT if waiting locks should be considered, or 0 if not.
   * @param[in] heap_no  The heap number of the record.
   * @param[in] trx      The transaction to check against, or nullptr to check requests by all transactions.
   * 
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] const Lock *rec_other_has_expl_req(
    Page_id page_id, Lock_mode mode, ulint gap, ulint wait, ulint heap_no, const Trx *trx
  ) const noexcept;

  /**
   * @brief Checks if a lock exists for a given page and heap number.
   *
   * This function checks if a lock exists for a given page and heap number.
   *
   * @param[in] rec_locks The record locks to search in.
   * @param[in] heap_no The heap number.
   * 
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] const Lock *rec_exists(const Rec_locks &rec_locks, ulint heap_no) const noexcept;
#endif /* UNIV_DEBUG */

  /**
   * @brief Prints information of locks for all transactions.
   *
   * This function outputs details of locks held by all transactions to the specified stream.
   * If the kernel mutex cannot be obtained, the function exits without printing any information.
   *
   * @param[in] nowait Whether to wait for the kernel mutex.
   * 
   * @return false if not able to obtain kernel mutex and exits without printing info.
   */
  [[nodiscard]] bool print_info_summary(bool nowait) const noexcept;

  /**
   * @brief Prints information of locks for each transaction.
   *
   * This function outputs details of locks held by each transaction.
   */
  void print_info_all_transactions() noexcept;

 private:
  /** Pointer to the main lock system */
  Lock_sys *m_lock_sys;
};
