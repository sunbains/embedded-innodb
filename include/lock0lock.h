/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/lock0lock.h
The transaction lock system

Created 5/7/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0cur.h"
#include "lock0types.h"
#include "que0que.h"
#include "read0read.h"
#include "row0row.h"
#include "row0vers.h"
#include "srv0srv.h"

#include <map>
#include <unordered_map>

struct Trx_sys;
struct Buf_block;
struct Index;
struct read_view_t;
struct que_thr_t;

#ifdef UNIV_DEBUG
extern bool lock_print_waits;
#endif /* UNIV_DEBUG */

/* Buffer for storing information about the most recent deadlock error */
extern ib_stream_t lock_latest_err_stream;

struct Lock_sys {
  /**
   * Creates the lock system at database start.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] n_cells Number of slots in the lock hash table.
   */
  explicit Lock_sys(Trx_sys *trx_sys, ulint n_cells) noexcept;

  /**
   * Closes the lock system at database shutdown.
   */
  ~Lock_sys() noexcept;

  /**
   * Gets the size of a lock struct.
   *
   * @return	size in bytes
   */
  [[nodiscard]] ulint get_size() const noexcept;

  /**
   * @brief Updates the lock table when a page has been reorganized.
   *
   * This function updates the lock table to reflect changes after a page
   * has been reorganized. It also copies the locks set on the infimum of
   * the page. The infimum may carry locks if an update of a record is
   * occurring on the page, and its locks were temporarily stored on the
   * infimum.
   *
   * @param[in] block  The old index page, now reorganized.
   * @param[in] oblock A copy of the old, not reorganized page.
   */
  void move_reorganize_page(const Buf_block *block, const Buf_block *oblock) noexcept;

  /**
   * @brief Moves the explicit locks on user records to another page if a record
   * list end is moved to another page.
   *
   * This function transfers the explicit locks associated with user records
   * from one page to another when the end of a record list is moved.
   *
   * @param[in] new_block The index page to move to.
   * @param[in] block The current index page.
   * @param[in] rec The first record moved on the page.
   */
  void move_rec_list_end(const Buf_block *new_block, const Buf_block *block, const rec_t *rec) noexcept;

  /**
   * @brief Moves the explicit locks on user records to another page if a record
   * list start is moved to another page.
   *
   * This function transfers the explicit locks associated with user records
   * from one page to another when the start of a record list is moved.
   *
   * @param[in] new_block The index page to move to.
   * @param[in] block The current index page.
   * @param[in] rec The first record on the page that is NOT copied.
   * @param[in] old_end The old previous-to-last record on the new page before the records were copied.
   */
  void move_rec_list_start(const Buf_block *new_block, const Buf_block *block, const rec_t *rec, const rec_t *old_end) noexcept;

  /**
   * @brief Updates the lock table when a page is split to the right.
   *
   * This function updates the lock table to reflect changes after a page
   * has been split to the right. It ensures that the locks are correctly
   * transferred and maintained on the new right page.
   *
   * @param[in] right_block The right page after the split.
   * @param[in] left_block The left page after the split.
   */
  void update_split_right(const Buf_block *right_block, const Buf_block *left_block) noexcept;

  /**
   * @brief Updates the lock table when a page is merged to the right.
   *
   * This function updates the lock table to reflect changes after a page
   * has been merged to the right. It ensures that the locks are correctly
   * transferred and maintained on the new right page.
   *
   * @param[in] right_block The right page to which the merge occurred.
   * @param[in] orig_succ The original successor of the infimum on the right page before the merge.
   * @param[in] left_block The merged index page which will be discarded.
   */
  void update_merge_right(const Buf_block *right_block, const rec_t *orig_succ, const Buf_block *left_block) noexcept;

  /**
   * @brief Updates the lock table when the root page is copied to another in btr_root_raise_and_insert.
   *
   * This function updates the lock table to reflect changes when the root page is copied to another
   * page during the btr_root_raise_and_insert operation. Note that lock structures are left on the
   * root page, even though they do not make sense on pages other than leaf pages. The reason for this
   * is that in a pessimistic update, the infimum record of the root page will act as a dummy carrier
   * of the locks of the record to be updated.
   *
   * @param[in] block The index page to which the root page is copied.
   * @param[in] root The root page being copied.
   */
  void update_root_raise(const Buf_block *block, const Buf_block *root) noexcept;

  /**
   * @brief Updates the lock table when a page is copied to another and the original
   * page is removed from the chain of leaf pages, except if the page is the root.
   *
   * This function ensures that the lock table is updated correctly when a page
   * is copied to a new location and the original page is removed from the chain
   * of leaf pages. Note that this operation does not apply if the page being
   * copied is the root page.
   *
   * @param[in] new_block The index page to which the original page is copied.
   * @param[in] block The original index page that is being copied; NOT the root page.
   */
  void update_copy_and_discard(const Buf_block *new_block, const Buf_block *block) noexcept;

  /**
   * @brief Updates the lock table when a page is split to the left.
   *
   * This function updates the lock table to reflect changes after a page
   * has been split to the left. It ensures that the locks are correctly
   * transferred and maintained on the new left page.
   *
   * @param[in] right_block The right page after the split.
   * @param[in] left_block The left page after the split.
   */
  void update_split_left(const Buf_block *right_block, const Buf_block *left_block) noexcept;

  /**
   * @brief Updates the lock table when a page is merged to the left.
   *
   * This function updates the lock table to reflect changes after a page
   * has been merged to the left. It ensures that the locks are correctly
   * transferred and maintained on the new left page.
   *
   * @param[in] left_block The left page to which the merge occurred.
   * @param[in] orig_pred The original predecessor of the supremum on the left page before the merge.
   * @param[in] right_block The merged index page which will be discarded.
   */
  void update_merge_left(const Buf_block *left_block, const rec_t *orig_pred, const Buf_block *right_block) noexcept;

  /**
   * @brief Resets the original locks on the heir record and replaces them with gap type locks
   * inherited from the specified record.
   *
   * This function is used to reset the locks on the heir record and inherit the gap type locks
   * from another record. The locks on the original record are not reset.
   *
   * @param[in] heir_block Block containing the record which inherits the locks.
   * @param[in] block Block containing the record from which the locks are inherited; does NOT reset the locks on this record.
   * @param[in] heir_heap_no Heap number of the inheriting record.
   * @param[in] heap_no Heap number of the donating record.
   */
  void rec_reset_and_inherit_gap_locks(
    const Buf_block *heir_block, const Buf_block *block, ulint heir_heap_no, ulint heap_no
  ) noexcept;

  /**
   * @brief Updates the lock table when a page is discarded.
   *
   * This function updates the lock table to reflect changes after a page
   * has been discarded. It ensures that the locks are correctly transferred
   * and maintained on the heir record.
   *
   * @param[in] heir_block The index page which will inherit the locks.
   * @param[in] heir_heap_no The heap number of the record which will inherit the locks.
   * @param[in] block The index page which will be discarded.
   */
  void update_discard(const Buf_block *heir_block, ulint heir_heap_no, const Buf_block *block) noexcept;

  /**
   * @brief Updates the lock table when a new user record is inserted.
   *
   * This function updates the lock table to reflect the insertion of a new user record.
   * It ensures that the appropriate locks are set for the newly inserted record.
   *
   * @param[in] block Buffer block containing the inserted record.
   * @param[in] rec The inserted record.
   */
  void update_insert(const Buf_block *block, const rec_t *rec) noexcept;

  /**
   * @brief Updates the lock table when a record is removed.
   *
   * This function updates the lock table to reflect the removal of a record.
   * It ensures that the locks associated with the record are correctly
   * managed and updated in the lock table.
   *
   * @param[in] block Buffer block containing the record to be removed.
   * @param[in] rec The record to be removed.
   */
  void update_delete(const Buf_block *block, const rec_t *rec) noexcept;

  /**
   * @brief Stores the explicit locks of a record on the page infimum record.
   *
   * This function is used to store the lock state of a record when it is
   * updated and the size of the record changes in the update. The record
   * is moved, potentially to another page, during such an update. The infimum
   * record acts as a dummy carrier record, managing lock releases while the
   * actual record is being moved.
   *
   * @param[in] block Buffer block containing the record.
   * @param[in] rec Record whose lock state is stored on the infimum record of the same page; lock bits are reset on the record.
   */
  void rec_store_on_page_infimum(const Buf_block *block, const rec_t *rec) noexcept;

  /**
   * @brief Restores the state of explicit lock requests on a single record.
   *
   * This function restores the state of explicit lock requests on a single record,
   * where the state was stored on the infimum of the page. It ensures that the lock
   * state is correctly transferred from the infimum to the specified record.
   *
   * @param[in] block Buffer block containing the record.
   * @param[in] rec Record whose lock state is restored.
   * @param[in] donator Page (rec is not necessarily on this page) whose infimum stored the lock state; lock bits are reset on the infimum.
   */
  void rec_restore_from_page_infimum(const Buf_block *block, const rec_t *rec, const Buf_block *donator) noexcept;

  /**
   * @brief Checks if there are explicit record locks on a page.
   *
   * This function determines whether there are any explicit record locks
   * present on the specified page within the given tablespace.
   *
   * @param[in] page_id The page id.
   *
   * @return true if there are explicit record locks on the page, false otherwise.
   */
  [[nodiscard]] bool rec_expl_exist_on_page(Page_id page_id) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate insert of a record.
   *
   * This function checks if there are any locks held by other transactions that would
   * prevent the immediate insertion of a record. If such locks exist, it first tests
   * if the query thread should be suspended for any reason. If not, it puts the transaction
   * and the query thread into the lock wait state and inserts a waiting request for a gap
   * exclusive lock (x-lock) into the lock queue.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] rec The record after which to insert.
   * @param[in,out] block The buffer block of the record.
   * @param[in] index The index.
   * @param[in] thr The query thread.
   * @param[in,out] mtr The mini-transaction.
   * @param[out] inherit Set to true if the new inserted record may inherit LOCK_GAP type locks from the successor record.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err rec_insert_check_and_lock(
    ulint flags, const rec_t *rec, Buf_block *block, const Index *index, que_thr_t *thr, mtr_t *mtr, bool *inherit
  ) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate modify (update,
   * delete mark, or delete unmark) of a clustered index record.
   *
   * If locks of other transactions prevent the modification, this function first tests
   * if the query thread should be suspended for any reason. If not, it puts the transaction
   * and the query thread into the lock wait state and inserts a waiting request for a record
   * exclusive lock (x-lock) into the lock queue.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] block Buffer block of the record.
   * @param[in] rec Record which should be modified.
   * @param[in] index Clustered index.
   * @param[in] offsets Column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index).
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err clust_rec_modify_check_and_lock(
    ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate modify
   * (delete mark or delete unmark) of a secondary index record.
   *
   * This function checks if there are any locks held by other transactions that
   * would prevent the immediate modification of a secondary index record. If such
   * locks exist, it handles the necessary steps to either wait for the lock to be
   * released or to handle the lock conflict appropriately.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in,out] block The buffer block of the record.
   * @param[in] rec The record which should be modified. Note that as this is a
   *                secondary index, the clustered index record must always be
   *                modified first.
   * @param[in] index The secondary index.
   * @param[in] thr The query thread.
   * @param[in,out] mtr The mini-transaction.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err sec_rec_modify_check_and_lock(
    ulint flags, Buf_block *block, const rec_t *rec, const Index *index, que_thr_t *thr, mtr_t *mtr
  ) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate read, or passing
   * over by a read cursor, of a secondary index record.
   *
   * This function checks if there are any locks held by other transactions that
   * would prevent the immediate read or passing over of a secondary index record
   * by a read cursor. If such locks exist, it handles the necessary steps to either
   * wait for the lock to be released or to handle the lock conflict appropriately.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] block Buffer block of the record.
   * @param[in] rec User record or page supremum record which should be read or passed over by a read cursor.
   * @param[in] index Secondary index.
   * @param[in] offsets Column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index).
   * @param[in] mode Mode of the lock which the read cursor should set on records: LOCK_S or LOCK_X; the latter is possible in SELECT FOR UPDATE.
   * @param[in] gap_mode LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err sec_rec_read_check_and_lock(
    ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, Lock_mode mode, ulint gap_mode,
    que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate read, or passing
   * over by a read cursor, of a clustered index record.
   *
   * If locks of other transactions prevent an immediate read or passing over by a read cursor,
   * this function first tests if the query thread should be suspended for some reason. If not,
   * it puts the transaction and the query thread into the lock wait state and inserts a waiting
   * request for a record lock into the lock queue. It sets the requested mode lock on the record.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] block Buffer block of the record.
   * @param[in] rec User record or page supremum record which should be read or passed over by a read cursor.
   * @param[in] index Clustered index.
   * @param[in] offsets Column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index).
   * @param[in] mode Mode of the lock which the read cursor should set on records: LOCK_S or LOCK_X; the latter is possible in SELECT FOR UPDATE.
   * @param[in] gap_mode LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err clust_rec_read_check_and_lock(
    ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, Lock_mode mode, ulint gap_mode,
    que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if locks of other transactions prevent an immediate read, or passing
   * over by a read cursor, of a clustered index record.
   *
   * If locks of other transactions prevent an immediate read or passing over by a read cursor,
   * this function first tests if the query thread should be suspended for some reason. If not,
   * it puts the transaction and the query thread into the lock wait state and inserts a waiting
   * request for a record lock into the lock queue. It sets the requested mode lock on the record.
   *
   * This is an alternative version of lock_clust_rec_read_check_and_lock() that does not require
   * the parameter "offsets".
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] block Buffer block of the record.
   * @param[in] rec User record or page supremum record which should be read or passed over by a read cursor.
   * @param[in] index Clustered index.
   * @param[in] mode Mode of the lock which the read cursor should set on records: LOCK_S or LOCK_X; the latter is possible in SELECT FOR UPDATE.
   * @param[in] gap_mode LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err clust_rec_read_check_and_lock_alt(
    ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, Lock_mode mode, ulint gap_mode, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if a record is visible in a consistent read.
   *
   * This function determines whether a given user record in a clustered index
   * is visible to a consistent read view. If the record is not visible, an
   * earlier version of the record should be retrieved.
   *
   * @param[in] rec User record which should be read or passed over by a read cursor.
   * @param[in] index Clustered index.
   * @param[in] offsets Column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index).
   * @param[in] view Consistent read view.
   *
   * @return true if the record is visible, or false if an earlier version of the record should be retrieved.
   */
  [[nodiscard]] bool clust_rec_cons_read_sees(const rec_t *rec, Index *index, const ulint *offsets, read_view_t *view)
    const noexcept;

  /**
   * @brief Checks that a non-clustered index record is seen in a consistent read.
   *
   * NOTE that a non-clustered index page contains so little information on
   * its modifications that also in the case false, the present version of
   * rec may be the right, but we must check this from the clustered index
   * record.
   *
   * @param[in] rec User record which should be read or passed over by a read cursor.
   * @param[in] view Consistent read view.
   *
   * @return true if certainly sees, or false if an earlier version of the
   * clustered index record might be needed.
   */
  [[nodiscard]] bool sec_rec_cons_read_sees(const rec_t *rec, read_view_t *view) const noexcept;

  /**
   * @brief Locks the specified database table in the given mode.
   *
   * This function attempts to lock the specified database table in the mode provided.
   * If the lock cannot be granted immediately, the query thread is put to wait.
   *
   * @param[in] flags If BTR_NO_LOCKING_FLAG bit is set, does nothing.
   * @param[in] table Database table in dictionary cache.
   * @param[in] mode Lock mode.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] db_err lock_table(ulint flags, Table *table, Lock_mode mode, que_thr_t *thr) noexcept;

  /**
   * @brief Removes a granted record lock of a transaction from the queue.
   *
   * This function removes a granted record lock of a transaction from the queue
   * and grants locks to other transactions waiting in the queue if they are now
   * entitled to a lock.
   *
   * @param[in] trx Transaction that has set a record lock.
   * @param[in] block Buffer block containing the record.
   * @param[in] rec Record for which the lock is set.
   * @param[in] lock_mode Lock mode, either LOCK_S or LOCK_X.
   */
  void rec_unlock(Trx *trx, const Buf_block *block, const rec_t *rec, Lock_mode lock_mode) noexcept;

  /**
   * @brief Releases transaction locks and releases other transactions waiting because of these locks.
   *
   * This function releases all locks held by the specified transaction. Additionally, it releases
   * any other transactions that are waiting for these locks to be released.
   *
   * @param[in] trx The transaction whose locks are to be released.
   */
  void release_off_trx_sys_mutex(Trx *trx) noexcept;

  /**
   * @brief Cancels a waiting lock request and releases possible other transactions waiting behind it.
   *
   * This function cancels a lock request that is currently waiting and releases any other transactions
   * that are waiting behind this lock request in the queue.
   *
   * @param[in] lock The waiting lock request to be canceled.
   */
  void cancel_waiting_and_release(Lock *lock) noexcept;

  /**
   * @brief Removes locks on a table to be dropped or truncated.
   *
   * This function removes all locks on the specified table that is to be dropped or truncated.
   * If the parameter `remove_also_table_sx_locks` is set to true, then table-level S and X locks
   * are also removed in addition to other table-level and record-level locks.
   * No lock that is going to be removed is allowed to be a wait lock.
   *
   * @param[in] table The table to be dropped or truncated.
   * @param[in] remove_also_table_sx_locks If true, also removes table S and X locks.
   */
  void remove_all_on_table(Table *table, bool remove_also_table_sx_locks) noexcept;

  /**
   * @brief Gets the source table of an ALTER TABLE transaction.
   *
   * This function retrieves the source table of an ALTER TABLE transaction.
   * The table must be covered by an IX or IS table lock.
   *
   * @param[in] trx The transaction.
   * @param[in] dest The destination of the ALTER TABLE.
   * @param[out] mode The lock mode of the source table.
   *
   * @return The source table of the transaction if it is covered by an IX or IS table lock;
   *         dest if there is no source table;
   *         NULL if the transaction is locking more than two tables or an inconsistency is found.
   */
  [[nodiscard]] Table *get_src_table(Trx *trx, Table *dest, Lock_mode *mode) noexcept;

  /**
   * @brief Determine if the given table is exclusively "owned" by the given transaction.
   *
   * This function checks if the specified table is exclusively locked by the given transaction.
   * The transaction must hold a LOCK_IX and possibly a LOCK_AUTO_INC on the table.
   *
   * @param[in] table The table to check.
   * @param[in] trx The transaction to check.
   *
   * @return true if the table is only locked by the transaction with LOCK_IX and possibly LOCK_AUTO_INC.
   */
  [[nodiscard]] bool is_table_exclusive(Table *table, Trx *trx) noexcept;

  /**
   * @brief Checks that a transaction id is sensible, i.e., not in the future.
   *
   * This function verifies that a given transaction id is reasonable and not set in the future.
   *
   * @param[in] trx_id The transaction id to check.
   * @param[in] rec The user record.
   * @param[in] index The clustered index.
   * @param[in] offsets The column offsets obtained from Phy_rec::get_col_offsets(rec, index).
   * @param[in] has_trx_sys_mutex True if the caller owns the trx system mutex.
   *
   * @return true if the transaction id is sensible, false otherwise.
   */
  [[nodiscard]] bool check_trx_id_sanity(
    trx_id_t trx_id, const rec_t *rec, const Index *index, const ulint *offsets, bool has_trx_sys_mutex
  ) noexcept;

  /**
   * @brief Prints information about a table lock.
   *
   * This function outputs details of a table lock to the specified stream.
   *
   * @param[in] ib_stream The stream where the lock information will be printed.
   * @param[in] lock The table type lock whose information is to be printed.
   */
  void table_print(ib_stream_t ib_stream, const Lock *lock) noexcept;

  /**
   * @brief Prints information about a record lock.
   *
   * This function outputs details of a record lock to the specified stream.
   *
   * @param[in] lock The record type lock whose information is to be printed.
   *
   * @return A string representation of the record lock.
   */
  [[nodiscard]] std::string lock_to_string(const Lock *lock) noexcept;

  /**
   * @brief Prints information of locks for all transactions.
   *
   * This function outputs details of locks held by all transactions to the specified stream.
   * If the trx system mutex cannot be obtained, the function exits without printing any information.
   *
   * @param[in] ib_stream The stream where the lock information will be printed.
   * @param[in] nowait Whether to wait for the trx system mutex.
   *
   * @return false if not able to obtain trx system mutex and exits without printing info.
   */
  [[nodiscard]] bool print_info_summary(bool nowait) const noexcept;

  /**
   * @brief Prints information of locks for each transaction.
   *
   * This function outputs details of locks held by each transaction to the specified stream.
   */
  void print_info_all_transactions() noexcept;

  /**
   * @brief Checks if some transaction has an implicit x-lock on a record in a clustered index.
   *
   * This function checks if there is an active transaction that holds an implicit
   * exclusive lock on the given record in a clustered index.
   *
   * @param[in] rec The user record to check
   * @param[in] index The clustered index containing the record
   * @param[in] offsets The column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index)
   *
   * @return A pointer to the transaction that holds the implicit x-lock, or NULL if no such transaction exists
   *
   * @note This function assumes that the trx system mutex is held by the caller.
   * @note The index must be a clustered index.
   * @note The record must be a user record (not a supremum/infinimum record).
   */
  [[nodiscard]] inline const Trx *clust_rec_some_has_impl(const rec_t *rec, const Index *index, const ulint *offsets) noexcept {
    ut_ad(mutex_own(&m_trx_sys->m_mutex));
    ut_ad(index->is_clustered());
    ut_ad(page_rec_is_user_rec(rec));

    const auto trx_id = row_get_rec_trx_id(rec, index, offsets);

    if (srv_trx_sys->is_active(trx_id)) {
      /* The modifying or inserting transaction is active */

      return srv_trx_sys->get_on_id(trx_id);
    } else {
      return nullptr;
    }
  }

  /**
   * Gets the heap_no of the smallest user record on a page.
   *
   * @param[in] block buffer block
   *
   * @return	heap_no of smallest user record, or PAGE_HEAP_NO_SUPREMUM
   */
  [[nodiscard]] inline ulint get_min_heap_no(const Buf_block *block) noexcept {
    const auto page = block->m_frame;

    return rec_get_heap_no(page + rec_get_next_offs(page + PAGE_INFIMUM));
  }

  /**
   * Gets the previous record lock set on a record.
   *
   * @param[in] in_lock   in: record lock
   * @param[in] heap_no   in: heap number of the record
   * @return              previous lock on the same record, nullptr if none exists
   */
  [[nodiscard]] const Lock *rec_get_prev(const Lock *in_lock, ulint heap_no) noexcept;

  /**
   * @brief Gets the type of a lock.
   *
   * This function retrieves the type of the specified lock. The type can be either
   * LOCK_TABLE or LOCK_REC.
   *
   * @param[in] lock The lock for which to get the type.
   *
   * @return The type of the lock, which can be LOCK_TABLE or LOCK_REC.
   */
  [[nodiscard]] inline ulint get_type_low(const Lock *lock) const noexcept { return lock->type(); }

#ifdef UNIV_DEBUG
  /**
   * @brief Checks if a lock exists for a given page and heap number.
   *
   * This function checks if a lock exists for a given page and heap number.
   *
   * @param[in] page_id The page id.
   * @param[in] heap_no The heap number.
   *
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  const Lock *rec_exists(const Rec_locks &rec_locks, ulint heap_no) const noexcept;
#endif /* UNIV_DEBUG */
  /**
   * @brief Sets the print InnoDB lock monitor flag.
   *
   * This function sets the print InnoDB lock monitor flag to true, indicating that
   * the InnoDB lock monitor should print information about lock requests and waits.
   */
  void set_print_lock_monitor() noexcept { m_print_lock_monitor = true; }

  void unset_print_lock_monitor() noexcept { m_print_lock_monitor = false; }

  /**
   * @brief Checks if the print InnoDB lock monitor flag is set.
   *
   * This function returns true if the print InnoDB lock monitor flag is set,
   * otherwise it returns false.
   *
   * @return true if the print InnoDB lock monitor flag is set, false otherwise.
   */
  [[nodiscard]] bool is_print_lock_monitor_set() const noexcept { return m_print_lock_monitor; }

  /**
   * @brief Creates a new instance of the Lock_sys class.
   *
   * This function allocates memory for a new Lock_sys object and initializes it
   * with the specified number of cells.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] n_cells The number of cells to initialize in the lock system.
   *
   * @return A pointer to the newly created Lock_sys object, or nullptr if the
   *         allocation fails.
   */
  [[nodiscard]] static Lock_sys *create(Trx_sys *trx_sys, ulint n_cells) noexcept;

  /**
   * @brief Destroys the Lock_sys instance and deallocates its memory.
   *
   * This function deallocates the memory associated with the given Lock_sys
   * instance and sets the pointer to nullptr to prevent dangling references.
   *
   * @param[in,out] lock_sys A reference to the pointer of the Lock_sys instance
   *                         to be destroyed. The pointer will be set to nullptr
   *                         after the instance is destroyed.
   */
  static void destroy(Lock_sys *&lock_sys) noexcept;

#ifndef UNIT_TESTING
 private:
#endif /* !UNIT_TESTING */

  /**
   * @brief Creates a new record lock and inserts it into the lock queue.
   *
   * This function creates a new record lock for the specified transaction and
   * inserts it into the lock queue. The lock mode and wait flag are specified
   * by the type_mode parameter, but the type is ignored and replaced by LOCK_REC.
   * It does not perform any checks for deadlocks or lock compatibility.
   *
   * @param[in] type_mode Lock mode and wait flag, type is ignored and replaced by LOCK_REC.
   * @param[in] space Tablespace ID.
   * @param[in] page_no Page number in the tablespace.
   * @param[in] heap_no Heap number in the page.
   * @param[in] n_bits Number of bits in the lock bitmap.
   * @param[in] index Index of the record.
   * @param[in,out] trx Transaction that wants to create the record lock.
   *
   * @return A pointer to the created lock.
   */
  [[nodiscard]] Lock *rec_create_low(
    Page_id page_id, Lock_mode type_mode, ulint heap_no, ulint n_bits, const Index *index, Trx *trx
  ) noexcept;

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
  [[nodiscard]] bool lock_validate() noexcept;

  /**
   * @brief Validates the record lock queues on a page.
   *
   * This function checks the validity of the record lock queues for a specific page
   * identified by its space ID and page number. It ensures that the locks on the page
   * are consistent and correctly maintained.
   *
   * @param[in] space The space ID of the page to validate.
   * @param[in] page_no The page number within the space to validate.
   * @return true if the record lock queues on the page are valid, false otherwise.
   */
  [[nodiscard]] bool rec_validate_page(Page_id page_id) noexcept;

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

#endif /* UNIV_DEBUG */

  /**
   * @brief Checks if a lock request results in a deadlock.
   *
   * This function determines whether a lock request by a transaction
   * results in a deadlock situation. If a deadlock is detected, it
   * decides whether the requesting transaction should be chosen as
   * the victim to resolve the deadlock.
   *
   * @param[in] lock Lock transaction is requesting.
   * @param[in] trx Transaction requesting the lock.
   *
   * @return true if a deadlock was detected and the requesting transaction
   *         was chosen as the victim; false if no deadlock was detected, or
   *         if a deadlock was detected but other transaction(s) were chosen
   *         as victim(s).
   */
  [[nodiscard]] bool deadlock_occurs(Lock *lock, Trx *trx) noexcept;

  /**
   * @brief Looks recursively for a deadlock.
   *
   * This function performs a recursive search to detect deadlocks in the lock system.
   * It checks if the transaction `trx` waiting for the lock `wait_lock` causes a deadlock
   * starting from the transaction `start`.
   *
   * @param[in] start The recursion starting point.
   * @param[in] trx A transaction waiting for a lock.
   * @param[in] wait_lock The lock that is waiting to be granted.
   * @param[in,out] cost The number of calculation steps thus far. If this exceeds
   *                     LOCK_MAX_N_STEPS_..., the function returns LOCK_EXCEED_MAX_DEPTH.
   * @param[in] depth The recursion depth. If this exceeds LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK,
   *                  the function returns LOCK_EXCEED_MAX_DEPTH.
   *
   * @return 0 if no deadlock is found.
   * @return LOCK_VICTIM_IS_START if a deadlock is found and 'start' is chosen as the victim.
   * @return LOCK_VICTIM_IS_OTHER if a deadlock is found and another transaction is chosen as the victim.
   *         In this case, the search must be repeated as there may be another deadlock.
   * @return LOCK_EXCEED_MAX_DEPTH if the lock search exceeds the maximum steps or depth.
   */
  [[nodiscard]] ulint deadlock_recursive(Trx *start, Trx *trx, Lock *wait_lock, ulint *cost, ulint depth) noexcept;

  /**
   * @brief Gets the wait flag of a lock.
   *
   * This function checks if the specified lock has the wait flag set.
   *
   * @param[in] lock The lock to check.
   *
   * @return true if the lock is waiting, false otherwise.
   */
  [[nodiscard]] inline bool get_wait(const Lock *lock) noexcept;

#ifdef UNIV_DEBUG
  /**
   * @brief Gets the first explicit lock request on a record.
   *
   * This function retrieves the first explicit lock request on a record
   * identified by the page id and the heap number.  It iterates through
   * the locks and returns the first lock that matches the specified heap number.
   *
   * @param[in] page_id The page id.
   * @param[in] heap_no The heap number of the record.
   *
   * @return The first lock on the specified record, or nullptr if none exists.
   */
  [[nodiscard]] inline Lock *rec_get_first(Page_id page_id, ulint heap_no) noexcept;
#endif /* UNIV_DEBUG */

  /**
   * @brief Checks if a transaction has the specified table lock, or stronger.
   *
   * This function searches for a lock held by the specified transaction (trx) on the given table.
   * It checks if the transaction has a lock on the table that is of the specified mode or stronger.
   *
   * @param[in] trx The transaction to check for the lock.
   * @param[in] table The table on which the lock is held.
   * @param[in] mode The lock mode to check for, or stronger.
   *
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] inline Lock *table_has(Trx *trx, Table *table, Lock_mode mode) noexcept;

  /**
   * @brief Checks if a transaction has a GRANTED explicit lock on a record that is stronger or equal to the specified mode.
   *
   * This function verifies whether the given transaction (trx) holds a granted explicit lock on the specified record
   * that is of the specified mode or stronger. The mode can be LOCK_S or LOCK_X, possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP.
   * For a supremum record, this is always regarded as a gap type request.
   *
   * @param[in] page_id The page id containing the record.
   * @param[in] precise_mode The precise lock mode to check for, which can be LOCK_S or LOCK_X, possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] heap_no The heap number of the record.
   * @param[in] trx The transaction to check for the lock.
   *
   * @return A pointer to the lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] inline const Lock *rec_has_expl(Page_id page_id, ulint precise_mode, ulint heap_no, const Trx *trx) const noexcept;

  /**
   * @brief Checks if some other transaction has a conflicting explicit lock request in the queue.
   *
   * This function determines whether any other transaction has a conflicting explicit lock request
   * on the specified record in the queue, such that the current transaction has to wait.
   *
   * @param[in] mode     The lock mode to check for, either LOCK_S or LOCK_X, possibly ORed with LOCK_GAP, LOCK_REC_NOT_GAP, or LOCK_INSERT_INTENTION.
   * @param[in] page_id  The page id containing the record.
   * @param[in] heap_no  The heap number of the record.
   * @param[in] trx      The transaction to check against.
   *
   * @return A pointer to the conflicting lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] Lock *rec_other_has_conflicting(Page_id page_id, Lock_mode mode, ulint heap_no, Trx *trx) noexcept;

  /**
   * @brief Finds a similar record lock on the same page for the same transaction.
   *
   * This function searches for a record lock of the same type and transaction on the same page.
   * It can be used to save space when a new record lock should be set on a page, as no new struct
   * is needed if a suitable existing one is found.
   *
   * @param[in] type_mode The type_mode field of the lock.
   * @param[in] heap_no The gap number of the record.
   * @param[in] lock The first lock on the page.
   * @param[in] trx The transaction to check for.
   *
   * @return A pointer to the similar lock if found, or nullptr if no such lock exists.
   */
  [[nodiscard]] inline Lock *rec_find_similar_on_page(Lock_mode type_mode, ulint heap_no, Lock *lock, const Trx *trx) noexcept;

  /**
   * @brief Checks if some transaction has an implicit x-lock on a record in a secondary index.
   *
   * This function determines whether any transaction holds an implicit exclusive lock (x-lock)
   * on a specified user record within a secondary index. It performs necessary validations and
   * checks the transaction ID to ensure the integrity of the page.
   *
   * @param[in] rec      The user record to check.
   * @param[in] index    The secondary index containing the record.
   * @param[in] offsets  Column offsets obtained from Phy_rec::get_col_offsets(rec, index).
   *
   * @return A pointer to the transaction that has the x-lock, or nullptr if no such transaction exists.
   */
  [[nodiscard]] Trx *sec_rec_some_has_impl_off_trx_sys_mutex(const rec_t *rec, const Index *index, const ulint *offsets) noexcept;

  /**
   * @brief Creates a new record lock and inserts it into the lock queue.
   *
   * This function creates a new record lock for the specified transaction and
   * inserts it into the lock queue. The lock mode and wait flag are specified
   * by the type_mode parameter, but the type is ignored and replaced by LOCK_REC.
   *
   * @param[in] type_mode Lock mode and wait flag, type is ignored and replaced by LOCK_REC.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] trx Transaction requesting the lock.
   *
   * @return The created lock.
   */
  [[nodiscard]] Lock *rec_create(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
  ) noexcept;

  /**
   * @brief Enqueues a waiting request for a lock which cannot be granted immediately and checks for deadlocks.
   *
   * This function enqueues a lock request that cannot be granted immediately and checks for deadlocks.
   * If a deadlock is detected, the lock request is removed and an error code is returned.
   *
   * @param[in] type_mode Lock mode this transaction is requesting: LOCK_S or LOCK_X,
   *                      possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP, ORed with LOCK_INSERT_INTENTION
   *                      if this waiting lock request is set when performing an insert of an index record.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] thr Query thread.
   *
   * @return DB_LOCK_WAIT if the lock request is enqueued and waiting.
   * @return DB_DEADLOCK if a deadlock is detected and the lock request is removed.
   * @return DB_QUE_THR_SUSPENDED if the query thread should be stopped.
   * @return DB_SUCCESS if there was a deadlock but another transaction was chosen as a victim, and the lock was granted immediately.
   */
  [[nodiscard]] db_err rec_enqueue_waiting(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Adds a record lock request in the record queue.
   *
   * The request is normally added as the last in the queue, but if there are no waiting lock requests
   * on the record, and the request to be added is not a waiting request, we can reuse a suitable
   * record lock object already existing on the same page, just setting the appropriate bit in its bitmap.
   * This is a low-level function which does NOT check for deadlocks or lock compatibility!
   *
   * @param[in] type_mode Lock mode, wait, gap etc. flags; type is ignored and replaced by LOCK_REC.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] trx Transaction.
   *
   * @return Lock where the bit was set.
   */
  [[nodiscard]] Lock *rec_add_to_queue(
    Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
  ) noexcept;

  /**
   * @brief Fast routine for locking a record in the most common cases.
   *
   * This function handles locking a record when there are no explicit locks on the page,
   * or there is just one lock, owned by this transaction, and of the right type_mode.
   * It is a low-level function that does NOT consider implicit locks and checks lock
   * compatibility within explicit locks. The function sets a normal next-key lock, or
   * in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl  If true, no lock is set if no wait is necessary. It is assumed that
   *                  the caller will set an implicit lock.
   * @param[in] mode  Lock mode: LOCK_X or LOCK_S possibly ORed with either LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] block Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index Index of the record.
   * @param[in] thr   Query thread.
   *
   * @return true if locking succeeded.
   */
  [[nodiscard]] inline bool rec_lock_fast(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief General, slower routine for locking a record.
   *
   * This is a low-level function which does NOT look at implicit locks. It checks
   * lock compatibility within explicit locks and sets a normal next-key lock, or
   * in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl   If true, no lock is set if no wait is necessary. It is assumed
   *                   that the caller will set an implicit lock.
   * @param[in] mode   Lock mode: LOCK_X or LOCK_S possibly ORed with either LOCK_GAP
   *                   or LOCK_REC_NOT_GAP.
   * @param[in] block  Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index  Index of the record.
   * @param[in] thr    Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, or an error code.
   */
  [[nodiscard]] db_err rec_lock_slow(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Tries to lock the specified record in the mode requested.
   *
   * If not immediately possible, enqueues a waiting lock request. This is a
   * low-level function which does NOT look at implicit locks! Checks lock
   * compatibility within explicit locks. This function sets a normal next-key
   * lock, or in the case of a page supremum record, a gap type lock.
   *
   * @param[in] impl   If true, no lock is set if no wait is necessary:
   *                   we assume that the caller will set an implicit lock.
   * @param[in] mode   Lock mode: LOCK_X or LOCK_S possibly ORed to either
   *                   LOCK_GAP or LOCK_REC_NOT_GAP.
   * @param[in] block  Buffer block containing the record.
   * @param[in] heap_no Heap number of the record.
   * @param[in] index  Index of the record.
   * @param[in] thr    Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, or an error code.
   */
  [[nodiscard]] db_err rec_lock(
    bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
  ) noexcept;

  /**
   * @brief Checks if a waiting record lock request still has to wait in a queue.
   *
   * This function determines whether a waiting record lock request still needs
   * to wait in the lock queue. It iterates through the locks on the same page
   * and checks if any of them require the waiting lock to wait.
   *
   * @param[in] rec_locks The record locks to check.
   * @param[in] wait_lock The lock request that is waiting.
   * @param[in] heap_no The heap number of the record.
   *
   * @return true if the lock request still has to wait, false otherwise.
   */
  [[nodiscard]] bool rec_has_to_wait_in_queue(const Rec_locks &rec_locks, Lock *wait_lock, ulint heap_no) const noexcept;

  /**
   * @brief Grants a lock to a waiting lock request and releases the waiting transaction.
   *
   * This function grants a lock to a waiting lock request and releases the
   * transaction that was waiting for the lock. It resets the wait flag and
   * the back pointer in the transaction to indicate that the lock wait has ended.
   *
   * @param[in] lock The lock request to be granted.
   */
  void grant(Lock *lock) noexcept;

  /**
   * @brief Cancels a waiting record lock request and releases the waiting transaction.
   *
   * This function cancels a waiting record lock request and releases the transaction
   * that requested it. It does not check if waiting lock requests behind this one
   * can now be granted.
   *
   * @param[in] lock The lock request to be canceled.
   */
  void rec_cancel(Lock *lock) noexcept;

  /**
   * @brief Removes a record lock request from the queue and grants locks to other transactions.
   *
   * This function removes a record lock request, whether waiting or granted, from the queue.
   * It also checks if other transactions waiting behind this lock request are now entitled
   * to a lock and grants it to them if they qualify. Note that all record locks contained
   * in the provided lock object are removed.
   *
   * @param[in] in_lock The record lock object. All record locks contained in this lock object
   *                    are removed. Transactions waiting behind will get their lock requests
   *                    granted if they are now qualified.
   */
  void rec_dequeue_from_page(Lock *in_lock) noexcept;

  /**
   * @brief Removes a record lock request, waiting or granted, from the queue.
   *
   * This function removes a record lock request from the queue, whether it is
   * currently waiting or has already been granted. All record locks contained
   * in the provided lock object are removed.
   *
   * @param[in,out] in_lock The record lock object. All record locks contained
   *                        in this lock object are removed.
   */
  void rec_discard(Lock *in_lock) noexcept;

  /**
   * @brief Removes record lock objects set on an index page which is discarded.
   *
   * This function removes all record lock objects associated with an index page
   * that is being discarded. It does not move locks or check for waiting locks,
   * so the lock bitmaps must already be reset when this function is called.
   *
   * @param[in] page_id The page id to be discarded.
   */
  void rec_free_all_from_discard_page(Page_id page_id) noexcept;

  /**
   * @brief Resets the lock bits for a single record and releases transactions waiting for lock requests.
   *
   * This function resets the lock bits for a specified record within a block and releases any transactions
   * that are waiting for lock requests on that record.
   *
   * @param[in] page_id  The page id containing the record.
   * @param[in] heap_no  The heap number of the record.
   */
  void rec_reset_and_release_wait(Page_id page_id, ulint heap_no) noexcept;

  /**
   * @brief Makes a record inherit the locks (except LOCK_INSERT_INTENTION type)
   * of another record as gap type locks, but does not reset the lock bits of
   * the other record.
   *
   * This function allows a record to inherit the locks of another record as gap type locks.
   * It does not reset the lock bits of the donating record. Additionally, any waiting lock
   * requests on the donating record are inherited as GRANTED gap locks.
   *
   * @param[in] heir_block   Block containing the record which inherits the locks.
   * @param[in] block        Block containing the record from which locks are inherited;
   *                         does NOT reset the locks on this record.
   * @param[in] heir_heap_no Heap number of the inheriting record.
   * @param[in] heap_no      Heap number of the donating record.
   */
  void rec_inherit_to_gap(const Buf_block *heir_block, const Buf_block *block, ulint heir_heap_no, ulint heap_no) noexcept;

  /**
   * @brief Makes a record inherit the gap locks (except LOCK_INSERT_INTENTION type)
   * of another record as gap type locks, but does not reset the lock bits of the
   * other record. Also, waiting lock requests are inherited as GRANTED gap locks.
   *
   * This function allows a record to inherit the gap locks of another record as gap type locks.
   * It does not reset the lock bits of the donating record. Additionally, any waiting lock
   * requests on the donating record are inherited as GRANTED gap locks.
   *
   * @param[in] block        Buffer block containing the record from which locks are inherited;
   *                         does NOT reset the locks on this record.
   * @param[in] heir_heap_no Heap number of the inheriting record.
   * @param[in] heap_no      Heap number of the donating record.
   */
  void rec_inherit_to_gap_if_gap_lock(const Buf_block *block, ulint heir_heap_no, ulint heap_no) noexcept;

  /**
   * @brief Moves the locks of a record to another record and resets the lock bits of
   * the donating record.
   *
   * This function transfers all locks from the donating record to the receiving record.
   * It also resets the lock bits of the donating record. The receiving record must not
   * have any existing lock requests.
   *
   * @param[in] receiver         Buffer block containing the receiving record.
   * @param[in] donator          Buffer block containing the donating record.
   * @param[in] receiver_heap_no Heap number of the record which gets the locks;
   *                             there must be no lock requests on it.
   * @param[in] donator_heap_no  Heap number of the record which gives the locks.
   */
  void rec_move(const Buf_block *receiver, const Buf_block *donator, ulint receiver_heap_no, ulint donator_heap_no) noexcept;

  /**
   * @brief Creates a table lock object and adds it as the last in the lock queue
   * of the table. Does NOT check for deadlocks or lock compatibility.
   *
   * @param[in] table Database table in dictionary cache.
   * @param[in] type_mode Lock mode possibly ORed with LOCK_WAIT.
   * @param[in] trx Transaction.
   * @return Lock* New lock object.
   */
  [[nodiscard]] inline Lock *table_create(Table *table, Lock_mode type_mode, Trx *trx) noexcept;

  /**
   * @brief Removes a table lock request from the queue and the trx list of locks.
   *
   * This is a low-level function which does NOT check if waiting requests
   * can now be granted.
   *
   * @param[in] lock The lock object to be removed.
   */
  inline void table_remove_low(Lock *lock) noexcept;

  /**
   * @brief Enqueues a waiting request for a table lock which cannot be granted immediately. Checks for deadlocks.
   *
   * This function enqueues a lock request for a table that cannot be granted immediately and checks for potential deadlocks.
   *
   * @param[in] mode   The lock mode this transaction is requesting.
   * @param[in] table  The table for which the lock is requested.
   * @param[in] thr    The query thread associated with the transaction.
   * @return DB_LOCK_WAIT if the lock request is enqueued and waiting,
   *         DB_DEADLOCK if a deadlock is detected,
   *         DB_QUE_THR_SUSPENDED if the query thread should be stopped,
   *         DB_SUCCESS if there was a deadlock but another transaction was chosen as a victim and the lock was granted immediately.
   */
  [[nodiscard]] db_err table_enqueue_waiting(Lock_mode mode, Table *table, que_thr_t *thr) noexcept;

  /**
   * Checks if other transactions have an incompatible mode lock request in the lock queue.
   *
   * @param[in] trx   Transaction, or nullptr if all transactions should be included.
   * @param[in] wait  LOCK_WAIT if also waiting locks are taken into account, or 0 if not.
   * @param[in] table Table.
   * @param[in] mode  Lock mode.
   * @return          Lock or nullptr.
   */
  [[nodiscard]] inline Lock *table_other_has_incompatible(Trx *trx, ulint wait, Table *table, Lock_mode mode) noexcept;

  /**
   * @brief Checks if a waiting table lock request still has to wait in a queue.
   *
   * This function determines whether a given table lock request, which is currently
   * waiting, still needs to wait in the queue. It iterates through the list of locks
   * on the table and checks for any conflicting locks that would require the wait_lock
   * to continue waiting.
   *
   * @param[in] wait_lock The lock request that is currently waiting.
   *
   * @return true if the lock request still has to wait, false otherwise.
   */
  [[nodiscard]] bool table_has_to_wait_in_queue(Lock *wait_lock) noexcept;

  /**
   * @brief Removes a table lock request from the queue and grants locks to other transactions.
   *
   * This function removes a table lock request, whether it is waiting or granted, from the queue.
   * It then checks the remaining transactions in the queue and grants their lock requests if they
   * are now qualified to receive the lock.
   *
   * @param[in] in_lock The table lock to be removed. Transactions waiting behind this lock will
   *                    get their lock requests granted if they are now qualified.
   */
  void table_dequeue(Lock *in_lock) noexcept;

  /**
   * @brief Removes locks of a transaction on a table to be dropped.
   *
   * If remove_sx_locks is true, then table-level S and X locks are
   * also removed in addition to other table-level and record-level locks.
   * No lock that is going to be removed is allowed to be a wait lock.
   *
   * @param[in] table           Table to be dropped.
   * @param[in] trx             Transaction.
   * @param[in] remove_sx_locks Also removes table S and X locks.
   */
  void remove_all_on_table_for_trx(Table *table, Trx *trx, bool remove_sx_locks) noexcept;

  /**
   * @brief Calculates the number of record lock structs in the record lock hash table.
   *
   * This function iterates through the record lock hash table and counts the number
   * of record lock structures present.
   *
   * @return ulint The number of record locks.
   */
  [[nodiscard]] ulint get_n_rec_locks() noexcept;

#ifdef UNIV_DEBUG
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
   * @brief Validates the lock system.
   *
   * This function iterates through all transactions and their associated locks,
   * validating the lock table queues and record lock queues to ensure the integrity
   * of the lock system.
   *
   * @return true if the lock system is valid, false otherwise.
   */
  [[nodiscard]] bool validate() noexcept;

#endif /* UNIV_DEBUG */

  /**
   * @brief Converts an implicit x-lock to an explicit x-lock on a record.
   *
   * If a transaction has an implicit x-lock on a record but no explicit x-lock
   * set on the record, this function sets one for it. Note that in the case of
   * a secondary index, the trx system mutex may get temporarily released.
   *
   * @param[in] block   The buffer block of the record.
   * @param[in] rec     The user record on the page.
   * @param[in] index   The index of the record.
   * @param[in] offsets Column offsets obtained from Phy_rec::get_col_offsets(rec, index).
   */
  void rec_convert_impl_to_expl(const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets) noexcept;

  /**
   * @brief Checks if a transaction has no waiters.
   *
   * This function determines whether the specified transaction has any other
   * transactions waiting on its locks. It iterates through the locks held by
   * the transaction and checks for any waiting locks.
   *
   * @param[in] trx The transaction to check for waiting transactions.
   *
   * @return true if no other transaction is waiting on its locks, false otherwise.
   */
  [[nodiscard]] bool trx_has_no_waiters(const Trx *trx) noexcept;

#ifndef UNIT_TESTING
 private:
#endif /* !UNIT_TESTING */

  /**
   * @brief The lock table.
   *
   * This unordered map holds the locks for each page in the buffer pool.
   * The key is the page ID, and the value is the list of locks on that page.
   */
  Page_id_hash<Rec_locks> m_rec_locks{};

  /**
   * @brief The buffer pool.
   *
   * This pointer holds the buffer pool, which manages the buffer pool. The pointer
   * is not owned by the lock system, but by the buffer pool. We extract the buffer pool
   * pointer from Trx_sys::Fil::m_buf_pool to simplify the calls.
   *
   * FIXME: This needs to be fixed, reduce these dependencies. We let it go for now
   * to simplify the changes.
   */
  Buf_pool *m_buf_pool{};

  /**
   * @brief The transaction system.
   *
   * This pointer holds the transaction system, which manages all transactions in the system.
   */
  Trx_sys *m_trx_sys{};

  /**
   * @brief Whether to print the InnoDB lock monitor.
   *
   * This flag indicates whether the InnoDB lock monitor should print information
   * about lock requests and waits.
   */
  bool m_print_lock_monitor{false};

  /**
   * @brief Whether a deadlock was found during the latest deadlock detection.
   *
   * This flag indicates whether a deadlock was detected during the most recent
   * deadlock detection cycle. It is used for logging and monitoring purposes.
   */
  bool m_deadlock_found{false};

  /**
   * @brief Sets the deadlock found flag.
   *
   * This function sets the deadlock found flag to true, indicating that a deadlock
   * was detected during the latest deadlock detection cycle.
   */
  void set_deadlock_found() noexcept { m_deadlock_found = true; }

  /**
   * @brief Checks if a deadlock was found.
   *
   * This function returns true if a deadlock was detected during the latest
   * deadlock detection cycle, false otherwise.
   *
   * @return true if a deadlock was found, false otherwise.
   */
  [[nodiscard]] bool is_deadlock_found() const noexcept { return m_deadlock_found; }
};

UT_LIST_NODE_GETTER_DEFINITION(Lock, m_trx_locks);
