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

LATCHING

There is no global mutex on the common paths of the lock system. Its state
is split, each piece owned by one latch:

  Latch                      Level (sync0sync.h)   Owns
  -------------------------  --------------------  --------------------------
  global rw-latch, sharded   SYNC_LOCK_SYS_GLOBAL  S: the right to take the
  (N_GLOBAL_SLOTS slots, a   330                   shard latches below; X
  thread S-latches one, X                          (all slots): all of the
  takes all)                                       lock system at once
  page shard latch           SYNC_LOCK_SYS_PAGE    the record lock queues of
  (N_PAGE_SHARDS, by page)   320                   the pages of the shard, the
                                                   bitmaps of their locks
  table shard latch          SYNC_LOCK_SYS_TABLE   Table::m_locks of the tables
  (N_TABLE_SHARDS, by table) 318                   of the shard
  trx sys mutex              SYNC_TRX_SYS 300      the trx lists, read views,
                                                   trx ids; not lock state
  active id shard mutex      SYNC_TRX_SYS_SHARD    TrxSys::m_ids (the active
  (TrxSys::m_ids)            299                   transactions by id)
  trx mutex (Trx::m_mutex)   SYNC_TRX 297          the lock list and lock heap
                                                   of the trx, its lock wait
                                                   state: m_wait_lock,
                                                   m_que_state,
                                                   m_was_chosen_as_deadlock_victim,
                                                   m_wait_thrs and the state of
                                                   the query threads in it

A thread acquires a latch only if its level is lower than that of every latch
it holds. So: the global latch first, then at most two page shard latches (in
the order of their shard numbers: the page operations that move locks between
two pages), or one table shard latch, possibly after a page shard latch; then
one trx mutex. The trx sys mutex may be taken with lock system latches held
(it is not, in practice); a lock system latch is never taken with the trx sys
mutex held: the callers that used to call the lock system with it held now
release it first (Trx::commit_off_kernel(), the lock monitor). A trx mutex is
taken last. The debug builds check this order (LockSys::Held, TrxMutex::n_held()
and mutex_own() of the trx sys mutex), and every internal function of the lock
system asserts the latch of the queue it reads (owns_page(), owns_table()).

The global latch in X replaces all the shard latches: the full deadlock
search, the cancel of a deadlock victim, validation, the lock monitor, DROP
and TRUNCATE (remove_all_on_table()) take it; they are rare.

LOCK WAIT STATE

A lock wait is entered by the requester with the latch of the queue and its
trx mutex held (enter_lock_wait()): its waiting lock is in the queue, its
edge of the waits-for graph published (Trx::m_blocking_trx), m_que_state is
TRX_QUE_LOCK_WAIT and its query thread in QUE_THR_LOCK_WAIT. It ends with the
latch of the queue and the trx mutex held: by a grant (grant()), by the cancel
of a deadlock victim (choose_victim()), or by a timeout or an interrupt
(cancel_wait()). Exactly one of them ends a wait: each first checks that the
waiting lock is still the trx's m_wait_lock, with the trx mutex, and a
cancelled request can't be granted. The query code (que0que.cc) and
Srv::suspend_user_thread() read and change the query thread state with the
trx mutex too.

The waiting thread waits for the event of its transaction (Trx::m_wait_event,
set in O(1) when the wait ends) until a steady clock deadline: the lock wait
timeout. There is no lock timeout thread.

GRANTS

A release removes a lock from its queue and then grants, in one pass over
the queue in queue order (rec_grant_pass(), table_grant_pass()), every
waiting lock that no lock ahead of it, of another transaction, blocks: O(W)
for W waiters, not O(W^2).

A commit releases its locks shard by shard (release()): the latch of one
shard at a time, for at most LOCK_RELEASE_BATCH locks of it in a row, with no
other latch held; others lock in other shards meanwhile.

DEADLOCKS

The fast path is latch free (the walk of the one edge graph, the edges are
atomics); the full search runs with the global latch in X, after the
requester released its shard latch (deadlock_resolve()). See lock0deadlock.h.

IMPLICIT LOCKS AND THE LIFE OF A TRANSACTION

A record carries an implicit X lock of the transaction whose id is in it (or,
for a secondary index record, that the clustered record says modified it), as
long as that transaction is active. Before a conflicting request is queued, the
implicit lock is made explicit, for its owner (impl_to_expl()). That needs:
the owner found by its id; known to be active; and the explicit lock created
in its lock list before its commit releases its locks, never after (a lock
left for a committed transaction), and never skipped while it is active (a
missed conflict). Without the trx sys mutex, as follows.

TrxSys::m_ids holds the active (and prepared) read-write transactions by id,
sharded, each shard with its own mutex. A transaction enters it when it gets
its id, and leaves it at C2 of its commit. Trx::m_n_ref counts pins.

The commit of T (Trx::commit_off_kernel()):
  C1  the commit in the file based world (undo log states, the mini-
      transaction): it can't be undone from here. [no latch held at its end
      but the trx sys mutex]
  C2  T leaves the active ids. [trx sys mutex, then the id shard mutex]
  C3  wait until T has no pins (TrxSys::drain_pins(): yield, then sleep).
      [no latch at all: a converter holding a pin may wait for a page latch
      and a lock system latch]
  C4  T's state becomes TRX_COMMITTED_IN_MEMORY. [trx sys mutex, then T's
      trx mutex]
  C6  T leaves the trx list (the read views). [trx sys mutex]
  C5  T's locks are released, shard by shard. [the shard latches, one at a
      time]
C6 comes before C5: a waiter granted by C5 that then opens a read view sees
T's changes.

A conversion for the record R of the page P with the trx id I in it:
  V1  pin the transaction with id I, if it is in the active ids
      (TrxSys::pin_active(), with the id shard mutex). [no lock latch]
  V2  latch the shard of P (global S, page shard).
  V3  T is still in the active ids (TrxSys::in_ids(), with the id shard
      mutex), and no other transaction holds a granted lock on R that
      conflicts with X: create T's explicit lock on R unless it has one; the
      request of the converter follows, under the same shard latch. Else
      create nothing: the lookup of V1 is stale.
  V4  unpin T, once the shard latch is released.

No lock is left for a committed transaction: a lock is created for T by a
conversion only between V1 and V4, while T is pinned. C2 makes new pins
impossible (the pin is taken with the id shard mutex, which C2 takes), C3
waits for the existing ones, and C4 comes after C3. So every lock a
conversion creates for T is in T's lock list before C4, hence before C5
walks the list and releases it. (T's own requests come from T's own thread,
before its commit; the locks a page operation creates for T, by inheritance
or a move, come from locks of T that C5 has not released yet, under the same
shard latches, and C5 runs until T's list is empty.)

No conflict is missed, and no conflict is made: a reader whose V3 comes
before C2 creates T's explicit lock under the page shard latch before its
own request, which therefore waits until C5 releases it. A reader whose V1
or V3 comes after C2 does not create one: C1 is done, T's changes are
committed (and a rollback undoes all its changes before its C2), so there is
no conflict to see; the reader's read view, opened after C6, sees T's
changes. The two kinds never hold conflicting granted locks on R: if A's V3
precedes C2 and B's V1 follows it, B latches the shard of P after A released
it (B's V1 < B's V2, and C2 < B's V1), so B's request sees T's lock and
waits. A V1 before C2 alone is not enough: B could find T gone and be
granted before A's V2, then A would create T's lock past B's (the reason V3
reads the active ids, not T's state, which stays active until C4). A Trx
object is never reused while pinned (the pool in TrxSys; destroy_trx()
asserts no pins).

A stale lookup by a rollback: T stays in the active ids through a rollback
(total, as a deadlock victim, or partial, to a savepoint or of a statement),
and a lookup made before an undo may convert after it. The undo reverts the
secondary index records before the clustered one (row0undo.cc), each with
its page X latched, and the converter holds the page latch of R from its
lookup to V3: so R itself does not change under the conversion. But a
lookup through RowVers reads other pages, and a converter A may still
convert for a change that a later reader B saw undone; B is then granted a
lock on R. V3 does not create T's lock past a granted conflicting lock of
another transaction: such a lock can't exist while T's implicit lock on R is
effective (T would have waited for it before changing R, and a converter
that found T would have created T's lock before its own request). If B's
lock is gone by V3, T gets an extra explicit X lock on R: harmless, A waits
for it, and C5 (or the end of the rollback) releases it.

The secondary index: the page max trx id against TrxSys::m_min_active_id, an
atomic lower bound of the smallest active id (stale means smaller: more
checks, never fewer), then the clustered record (RowVers), which ends in
TrxSys::pin_active(): the same V1..V4. */
/*******************************************************/

#pragma once

#include "srv0sys.h"

#include "btr0cur.h"
#include "lock0deadlock.h"
#include "lock0types.h"
#include "que0que.h"
#include "read0read.h"
#include "row0row.h"
#include "row0vers.h"
#include "srv0srv.h"

#include <array>
#include <atomic>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

struct TrxSys;
struct BufBlock;
struct Index;
struct ReadView;
struct QueThr;

#ifdef UNIV_DEBUG
extern bool lock_print_waits;
#endif /* UNIV_DEBUG */

/* Buffer for storing information about the most recent deadlock error */
extern FILE *lock_latest_err_stream;

struct LockSys {
  /** The decisions of a grant pass: each waiting lock, and whether the pass
  grants it. */
  using GrantPlan = std::vector<std::pair<const Lock *, bool>>;

  /** The instance this sub-system belongs to. */
  System &m_sys;

  /**
   * Creates the lock system at database start.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] n_cells Number of slots in the lock hash table.
   */
  explicit LockSys(System &sys) noexcept;

  /**
   * Closes the lock system at database shutdown.
   */
  ~LockSys() noexcept;

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
  void move_reorganize_page(const BufBlock *block, const BufBlock *oblock) noexcept;

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
  void move_rec_list_end(const BufBlock *new_block, const BufBlock *block, const Rec rec) noexcept;

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
  void move_rec_list_start(const BufBlock *new_block, const BufBlock *block, const Rec rec, const Rec old_end) noexcept;

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
  void update_split_right(const BufBlock *right_block, const BufBlock *left_block) noexcept;

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
  void update_merge_right(const BufBlock *right_block, const Rec orig_succ, const BufBlock *left_block) noexcept;

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
  void update_root_raise(const BufBlock *block, const BufBlock *root) noexcept;

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
  void update_copy_and_discard(const BufBlock *new_block, const BufBlock *block) noexcept;

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
  void update_split_left(const BufBlock *right_block, const BufBlock *left_block) noexcept;

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
  void update_merge_left(const BufBlock *left_block, const Rec orig_pred, const BufBlock *right_block) noexcept;

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
    const BufBlock *heir_block, const BufBlock *block, ulint heir_heap_no, ulint heap_no
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
  void update_discard(const BufBlock *heir_block, ulint heir_heap_no, const BufBlock *block) noexcept;

  /**
   * @brief Updates the lock table when a new user record is inserted.
   *
   * This function updates the lock table to reflect the insertion of a new user record.
   * It ensures that the appropriate locks are set for the newly inserted record.
   *
   * @param[in] block Buffer block containing the inserted record.
   * @param[in] rec The inserted record.
   */
  void update_insert(const BufBlock *block, const Rec rec) noexcept;

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
  void update_delete(const BufBlock *block, const Rec rec) noexcept;

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
  void rec_store_on_page_infimum(const BufBlock *block, const Rec rec) noexcept;

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
  void rec_restore_from_page_infimum(const BufBlock *block, const Rec rec, const BufBlock *donator) noexcept;

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
  [[nodiscard]] bool rec_expl_exist_on_page(PageId page_id) noexcept;

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
  [[nodiscard]] DbErr rec_insert_check_and_lock(
    ulint flags, const Rec rec, BufBlock *block, const Index *index, QueThr *thr, Mtr *mtr, bool *inherit
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
   * @param[in] offsets Column offsets for the record, obtained from PhyRec::get_col_offsets(rec, index).
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] DbErr clust_rec_modify_check_and_lock(
    ulint flags, const BufBlock *block, const Rec rec, const Index *index, const ulint *offsets, QueThr *thr
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
  [[nodiscard]] DbErr sec_rec_modify_check_and_lock(
    ulint flags, BufBlock *block, const Rec rec, const Index *index, QueThr *thr, Mtr *mtr
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
   * @param[in] offsets Column offsets for the record, obtained from PhyRec::get_col_offsets(rec, index).
   * @param[in] mode Mode of the lock which the read cursor should set on records: LOCK_S or LOCK_X; the latter is possible in SELECT FOR UPDATE.
   * @param[in] gap_mode LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] DbErr sec_rec_read_check_and_lock(
    ulint flags, const BufBlock *block, const Rec rec, const Index *index, const ulint *offsets, LockMode mode,
    ulint gap_mode, QueThr *thr
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
   * @param[in] offsets Column offsets for the record, obtained from PhyRec::get_col_offsets(rec, index).
   * @param[in] mode Mode of the lock which the read cursor should set on records: LOCK_S or LOCK_X; the latter is possible in SELECT FOR UPDATE.
   * @param[in] gap_mode LOCK_ORDINARY, LOCK_GAP, or LOCK_REC_NOT_GAP.
   * @param[in] thr Query thread.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED.
   */
  [[nodiscard]] DbErr clust_rec_read_check_and_lock(
    ulint flags, const BufBlock *block, const Rec rec, const Index *index, const ulint *offsets, LockMode mode,
    ulint gap_mode, QueThr *thr
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
  [[nodiscard]] DbErr clust_rec_read_check_and_lock_alt(
    ulint flags, const BufBlock *block, const Rec rec, const Index *index, LockMode mode, ulint gap_mode, QueThr *thr
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
   * @param[in] offsets Column offsets for the record, obtained from PhyRec::get_col_offsets(rec, index).
   * @param[in] view Consistent read view.
   *
   * @return true if the record is visible, or false if an earlier version of the record should be retrieved.
   */
  [[nodiscard]] bool clust_rec_cons_read_sees(const Rec rec, Index *index, const ulint *offsets, ReadView *view)
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
  [[nodiscard]] bool sec_rec_cons_read_sees(const Rec rec, ReadView *view) const noexcept;

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
  [[nodiscard]] DbErr lock_table(ulint flags, Table *table, LockMode mode, QueThr *thr) noexcept;

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
  void rec_unlock(Trx *trx, const BufBlock *block, const Rec rec, LockMode lock_mode) noexcept;

  /**
   * @brief Releases transaction locks and releases other transactions waiting because of these locks.
   *
   * This function releases all locks held by the specified transaction. Additionally, it releases
   * any other transactions that are waiting for these locks to be released.
   *
   * @param[in] trx The transaction whose locks are to be released.
   */
  void release(Trx *trx) noexcept;

  /**
   * @brief Ends the lock wait of trx with a cancel of its waiting request,
   * unless a grant (or another cancel) ended it first: for a lock wait
   * timeout or an interrupt. Takes the latch of the queue of the waiting
   * lock, then the trx mutex.
   *
   * @param[in] trx A transaction of the calling thread.
   *
   * @return true if this call cancelled the wait.
   */
  [[nodiscard]] bool cancel_wait(Trx *trx) noexcept;

  /**
   * @brief Locks a table for trx, waiting if needed: lock_table() with or
   * without a query thread (the tests). See enter_lock_wait().
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT or DB_DEADLOCK.
   */
  [[nodiscard]] DbErr table_lock_for(Trx *trx, Table *table, LockMode mode, QueThr *thr) noexcept;

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
  [[nodiscard]] Table *get_src_table(Trx *trx, Table *dest, LockMode *mode) noexcept;

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
   * @param[in] offsets The column offsets obtained from PhyRec::get_col_offsets(rec, index).
   * @param[in] has_trx_sys_mutex True if the caller owns the trx system mutex.
   *
   * @return true if the transaction id is sensible, false otherwise.
   */
  [[nodiscard]] bool check_trx_id_sanity(
    trx_id_t trx_id, const Rec rec, const Index *index, const ulint *offsets, bool has_trx_sys_mutex
  ) noexcept;

  /**
   * @brief Prints information about a table lock.
   *
   * This function outputs details of a table lock to the specified stream.
   *
   * @param[in] stderr The stream where the lock information will be printed.
   * @param[in] lock The table type lock whose information is to be printed.
   */
  void table_print(FILE *out, const Lock *lock) noexcept;

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
   * @param[in] stderr The stream where the lock information will be printed.
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
   * @param[in] offsets The column offsets for the record, obtained from PhyRec::get_col_offsets(rec, index)
   *
   * @return A pointer to the transaction that holds the implicit x-lock, or NULL if no such transaction exists
   *
   * @note No lock system latch is held: the transaction is returned pinned
   *  (TrxSys::pin_active()), the caller unpins it (TrxSys::unpin()).
   * @note The index must be a clustered index.
   * @note The record must be a user record (not a supremum/infinimum record).
   */
  [[nodiscard]] inline Trx *clust_rec_some_has_impl(const Rec rec, const Index *index, const ulint *offsets) noexcept {
    /* V1 of a conversion (see LATCHING): no lock system latch held. */
    ut_ad(index->is_clustered());
    ut_ad(page_rec_is_user_rec(rec));

    const auto trx_id = row_get_rec_trx_id(rec, index, offsets);

    /* The active transaction with that id, pinned; the caller unpins it. */
    return m_sys.m_trx_sys->pin_active(trx_id);
  }

  /**
   * Gets the heap_no of the smallest user record on a page.
   *
   * @param[in] block buffer block
   *
   * @return	heap_no of smallest user record, or PAGE_HEAP_NO_SUPREMUM
   */
  [[nodiscard]] inline ulint get_min_heap_no(const BufBlock *block) noexcept {
    const auto page = block->m_frame;

    /* The successor of the infimum: the supremum if the page is empty. */
    return page_rec_get_next(Rec(page + PAGE_INFIMUM)).get_heap_no();
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
  void set_print_lock_monitor() noexcept { m_print_lock_monitor.store(true, std::memory_order_relaxed); }

  void unset_print_lock_monitor() noexcept { m_print_lock_monitor.store(false, std::memory_order_relaxed); }

  /**
   * @brief Checks if the print InnoDB lock monitor flag is set.
   *
   * This function returns true if the print InnoDB lock monitor flag is set,
   * otherwise it returns false.
   *
   * @return true if the print InnoDB lock monitor flag is set, false otherwise.
   */
  [[nodiscard]] bool is_print_lock_monitor_set() const noexcept { return m_print_lock_monitor.load(std::memory_order_relaxed); }

  /**
   * @brief Creates a new instance of the LockSys class.
   *
   * This function allocates memory for a new LockSys object and initializes it
   * with the specified number of cells.
   *
   * @param[in] trx_sys The transaction system.
   * @param[in] n_cells The number of cells to initialize in the lock system.
   *
   * @return A pointer to the newly created LockSys object, or nullptr if the
   *         allocation fails.
   */
  /**
   * @brief Destroys the LockSys instance and deallocates its memory.
   *
   * This function deallocates the memory associated with the given LockSys
   * instance and sets the pointer to nullptr to prevent dangling references.
   *
   * @param[in,out] lock_sys A reference to the pointer of the LockSys instance
   *                         to be destroyed. The pointer will be set to nullptr
   *                         after the instance is destroyed.
   */
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
    PageId page_id, LockMode type_mode, ulint heap_no, ulint n_bits, const Index *index, Trx *trx
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
  [[nodiscard]] bool rec_validate_page(PageId page_id) noexcept;

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
    PageId page_id, LockMode mode, ulint gap, ulint wait, ulint heap_no, const Trx *trx
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
   * @brief Walks the waits-for graph from a transaction, one edge
   * (Trx::m_blocking_trx) per waiting transaction, see lock0deadlock.h.
   *
   * O(length of the path): no queue is read, no mark is set.
   *
   * @param[in] start The transaction to start from.
   * @param[in] depth The depth of start: 0, or more to test the limit.
   * @param[out] pred On a cycle, the transaction of the cycle that waits for start.
   *
   * @return CYCLE if the path comes back to start, TOO_DEEP if it is longer
   *  than LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK, NONE if it ends at a transaction
   *  that does not wait.
   */
  [[nodiscard]] DeadlockWalk deadlock_search(const Trx *start, ulint depth, Trx **pred, bool *single = nullptr) const noexcept;

  /**
   * @brief The hybrid deadlock check of a waiting transaction: the one edge
   * walk, then, if the walk found no cycle and some transaction on its path
   * has more than one blocker, the full search (deadlock_dfs()).
   *
   * @param[in] start A transaction with a waiting lock.
   * @param[out] pred On a cycle, the transaction of the cycle that waits for start.
   *
   * @return CYCLE, TOO_DEEP or NONE.
   */
  [[nodiscard]] DeadlockWalk deadlock_find(Trx *start, Trx **pred) noexcept;

  /**
   * @brief The full deadlock search: depth first over the blocker sets (the
   * owners of the locks ahead that a waiting lock has to wait for), visiting
   * only waiting transactions, each once per search (epoch marks).
   *
   * @param[in] start The transaction the search started from.
   * @param[in] trx A waiting transaction reached by the search.
   * @param[in] wait_lock Its waiting lock.
   * @param[in] depth Its depth.
   * @param[out] pred On a cycle, the transaction of the cycle that waits for start.
   *
   * @return CYCLE, TOO_DEEP or NONE.
   */
  [[nodiscard]] DeadlockWalk deadlock_dfs(const Trx *start, Trx *trx, const Lock *wait_lock, ulint depth, Trx **pred) noexcept;

  /**
   * @brief Chooses the victim of a cycle, and logs the cycle.
   *
   * @param[in] start The transaction whose edge closed the cycle.
   * @param[in] pred The transaction of the cycle that waits for start.
   *
   * @return start if pred does not weigh less than start, else pred.
   */
  [[nodiscard]] Trx *deadlock_victim(Trx *start, Trx *pred) noexcept;

  /**
   * @return the queue of a page, nullptr if it has no record locks.
   * @param[in] page_id The page.
   */
  [[nodiscard]] Rec_locks *rec_queue(PageId page_id) const noexcept {
    ut_ad(owns_page(page_id));

    auto &locks = const_cast<LockSys *>(this)->m_page_shards[page_shard_no(page_id)].m_locks;
    auto it = locks.find(page_id);

    return it == locks.end() ? nullptr : &it->second;
  }

  /** @return the queue of a page, created if it has none. */
  [[nodiscard]] Rec_locks &rec_queue_add(PageId page_id) noexcept {
    ut_ad(owns_page(page_id));

    return m_page_shards[page_shard_no(page_id)].m_locks[page_id];
  }

  /** Erase the (empty) queue of a page. */
  void rec_queue_erase(PageId page_id) noexcept {
    ut_ad(owns_page(page_id));

    (void)m_page_shards[page_shard_no(page_id)].m_locks.erase(page_id);
  }

  /**
   * @brief The deadlock check of the transactions whose waiting lock the
   * page operation of the calling thread moved (moved()): one victim per
   * cycle found. Called with no latch held, after the page operation.
   */
  void deadlock_check_moved() noexcept;

  /**
   * @brief The latch free part of the deadlock check of a waiting
   * transaction: the walk of the one edge graph.
   *
   * @return true if the walk found a cycle, or a path too deep, or it can't
   *  rule a cycle out (a transaction with several blockers on the path): the
   *  full check must run (deadlock_resolve()).
   */
  [[nodiscard]] bool deadlock_suspect(const Trx *trx) const noexcept;

  /**
   * @brief The full deadlock check of a request, with the global latch in X:
   * cancels the request if the requester is the victim.
   *
   * @param[in] trx The requester, in its lock wait.
   *
   * @return DB_DEADLOCK if trx is the victim, else DB_LOCK_WAIT.
   */
  [[nodiscard]] DbErr deadlock_resolve(Trx *trx) noexcept;

  /**
   * @brief After a request released its shard latch: if it waits and its
   * latch free deadlock walk was suspect, the full check.
   *
   * @param[in] err The result of the request.
   *
   * @return err, or DB_DEADLOCK.
   */
  [[nodiscard]] DbErr deadlock_after_wait(DbErr err) noexcept;

  /**
   * @brief Marks a transaction as a deadlock victim and cancels its waiting
   * request; with the global latch in X.
   */
  void choose_victim(Trx *victim) noexcept;

  /** @return the waiting lock of trx if it is in the lock wait state, else
  nullptr; with the global latch in X. */
  [[nodiscard]] Lock *waiting_lock(Trx *trx) noexcept;

  /**
   * @brief Puts trx in the lock wait state, its waiting lock created: with
   * the latch of its queue held. The latch free deadlock walk runs here;
   * if suspect, the caller runs deadlock_after_wait() once it released the
   * latch.
   *
   * @param[in] trx The requester.
   * @param[in] thr Its query thread, or nullptr (the tests).
   *
   * @return DB_LOCK_WAIT.
   */
  [[nodiscard]] DbErr enter_lock_wait(Trx *trx, QueThr *thr) noexcept;

  /** Reset the wait flag of a lock and the wait lock of its trx, with the trx mutex. */
  void reset_wait(Lock *lock) noexcept;

  /**
   * @brief Moves the edge of a waiting transaction to a new blocker.
   *
   * @param[in] trx A transaction that keeps waiting.
   * @param[in] blocker A transaction with a lock ahead that blocks its wait.
   */
  void wait_edge(Trx *trx, const Trx *blocker, bool multi) noexcept;

  /**
   * @brief Finds a transaction with a lock ahead of a waiting record lock
   * that it has to wait for.
   *
   * @param[in] rec_locks The queue of the page.
   * @param[in] wait_lock The waiting lock.
   * @param[in] heap_no Its record.
   *
   * @return The owner of the first such lock, or nullptr.
   */
  [[nodiscard]] const Trx *rec_find_blocker(const Rec_locks &rec_locks, const Lock *wait_lock, ulint heap_no, bool *multi) const noexcept;

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
  [[nodiscard]] inline Lock *rec_get_first(PageId page_id, ulint heap_no) noexcept;
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
  [[nodiscard]] Lock *table_has(Trx *trx, Table *table, LockMode mode) noexcept;

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
  [[nodiscard]] const Lock *rec_has_expl(PageId page_id, ulint precise_mode, ulint heap_no, const Trx *trx) const noexcept;

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
  [[nodiscard]] Lock *rec_other_has_conflicting(PageId page_id, LockMode mode, ulint heap_no, Trx *trx) noexcept;

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
  [[nodiscard]] Lock *rec_find_similar_on_page(LockMode type_mode, ulint heap_no, Lock *lock, const Trx *trx) noexcept;

  /**
   * @brief Checks if some transaction has an implicit x-lock on a record in a secondary index.
   *
   * This function determines whether any transaction holds an implicit exclusive lock (x-lock)
   * on a specified user record within a secondary index. It performs necessary validations and
   * checks the transaction ID to ensure the integrity of the page.
   *
   * @param[in] rec      The user record to check.
   * @param[in] index    The secondary index containing the record.
   * @param[in] offsets  Column offsets obtained from PhyRec::get_col_offsets(rec, index).
   *
   * @return A pointer to the transaction that has the x-lock, or nullptr if no such transaction exists.
   */
  [[nodiscard]] Trx *sec_rec_some_has_impl_off_trx_sys_mutex(const Rec rec, const Index *index, const ulint *offsets) noexcept;

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
    LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, const Trx *trx
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
  [[nodiscard]] DbErr rec_enqueue_waiting(
    LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
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
    LockMode type_mode, const BufBlock *block, ulint heap_no, const Index *index, const Trx *trx
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
  [[nodiscard]] bool rec_lock_fast(
    bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx
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
  [[nodiscard]] DbErr rec_lock_slow(
    bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
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
  [[nodiscard]] DbErr rec_lock(
    bool impl, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, Trx *trx, QueThr *thr
  ) noexcept;

  /**
   * @brief Locks a record for trx, waiting if needed: what the check and
   * lock functions do, with or without a query thread (the tests), no
   * implicit locks. An insert intention lock is set only if it waits, as
   * rec_insert_check_and_lock() does.
   *
   * @return DB_SUCCESS, DB_LOCK_WAIT or DB_DEADLOCK.
   */
  [[nodiscard]] DbErr rec_lock_for(
    Trx *trx, LockMode mode, const BufBlock *block, ulint heap_no, const Index *index, QueThr *thr = nullptr
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
   * @brief Grants the waiting locks of a page queue that no lock ahead blocks.
   *
   * One pass in queue order: O(locks + bits of the granted locks that
   * cover a record some lock waits for), not O(W^2) for W waiters.
   *
   * @param[in,out] rec_locks The queue of a page.
   */
  void rec_grant_pass(Rec_locks &rec_locks, GrantPlan *plan = nullptr) noexcept;

  /**
   * @brief rec_grant_pass() for the locks on one record.
   *
   * @param[in,out] rec_locks The queue of a page.
   * @param[in] heap_no The record.
   */
  void rec_grant_pass_on(Rec_locks &rec_locks, ulint heap_no, GrantPlan *plan = nullptr) noexcept;

  /**
   * @brief The decision of a grant pass for one waiting lock: grant it
   * (blocker == nullptr) or point its edge at the blocker. With a plan, only
   * notes the decision (the tests compare it with rec_has_to_wait_in_queue()).
   */
  void grant_or_edge(Lock *lock, const Trx *blocker, bool multi, GrantPlan *plan) noexcept;

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
  void rec_free_all_from_discard_page(PageId page_id) noexcept;

  /**
   * @brief Resets the lock bits for a single record and releases transactions waiting for lock requests.
   *
   * This function resets the lock bits for a specified record within a block and releases any transactions
   * that are waiting for lock requests on that record.
   *
   * @param[in] page_id  The page id containing the record.
   * @param[in] heap_no  The heap number of the record.
   */
  void rec_reset_and_release_wait(PageId page_id, ulint heap_no) noexcept;

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
  void rec_inherit_to_gap(const BufBlock *heir_block, const BufBlock *block, ulint heir_heap_no, ulint heap_no) noexcept;

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
  void rec_inherit_to_gap_if_gap_lock(const BufBlock *block, ulint heir_heap_no, ulint heap_no) noexcept;

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
  void rec_move(const BufBlock *receiver, const BufBlock *donator, ulint receiver_heap_no, ulint donator_heap_no) noexcept;

  /**
   * @brief Creates a table lock object and adds it as the last in the lock queue
   * of the table. Does NOT check for deadlocks or lock compatibility.
   *
   * @param[in] table Database table in dictionary cache.
   * @param[in] type_mode Lock mode possibly ORed with LOCK_WAIT.
   * @param[in] trx Transaction.
   * @return Lock* New lock object.
   */
  [[nodiscard]] Lock *table_create(Table *table, LockMode type_mode, Trx *trx) noexcept;

  /**
   * @brief Removes a table lock request from the queue and the trx list of locks.
   *
   * This is a low-level function which does NOT check if waiting requests
   * can now be granted.
   *
   * @param[in] lock The lock object to be removed.
   */
  void table_remove_low(Lock *lock) noexcept;

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
  [[nodiscard]] DbErr table_enqueue_waiting(LockMode mode, Table *table, Trx *trx, QueThr *thr) noexcept;

  /**
   * Checks if other transactions have an incompatible mode lock request in the lock queue.
   *
   * @param[in] trx   Transaction, or nullptr if all transactions should be included.
   * @param[in] wait  LOCK_WAIT if also waiting locks are taken into account, or 0 if not.
   * @param[in] table Table.
   * @param[in] mode  Lock mode.
   * @return          Lock or nullptr.
   */
  [[nodiscard]] Lock *table_other_has_incompatible(Trx *trx, ulint wait, Table *table, LockMode mode) noexcept;

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
   * @brief Grants the waiting locks of a table queue that no lock ahead
   * blocks, in one pass in queue order.
   *
   * @param[in,out] table The table.
   */
  void table_grant_pass(Table *table, GrantPlan *plan = nullptr) noexcept;

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
   * @param[in] offsets Column offsets obtained from PhyRec::get_col_offsets(rec, index).
   * @return true if the lock queue is valid, false otherwise.
   */
  [[nodiscard]] bool rec_queue_validate(
    const BufBlock *block, const Rec rec, const Index *index, const ulint *offsets
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
   * @brief Converts an implicit x-lock to an explicit x-lock on a record:
   * V3 of a conversion (see LATCHING). Sets an explicit X lock on the
   * record for impl_trx unless it has one.
   *
   * @param[in] impl_trx The owner of the implicit lock, pinned.
   * @param[in] block    The buffer block of the record, its shard latched.
   * @param[in] heap_no  The record.
   * @param[in] index    The index of the record.
   */
  void impl_to_expl(Trx *impl_trx, const BufBlock *block, ulint heap_no, const Index *index) noexcept;

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

  /** The number of slots of the global latch. ThreadSanitizer tracks at
  most 64 mutexes held by one thread (sanitizer_deadlock_detector.h): X holds
  every slot and may then take shard and trx latches, so fewer slots there. */
#if defined(__SANITIZE_THREAD__)
  static constexpr size_t N_GLOBAL_SLOTS = 16;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
  static constexpr size_t N_GLOBAL_SLOTS = 16;
#else
  static constexpr size_t N_GLOBAL_SLOTS = 64;
#endif
#else
  static constexpr size_t N_GLOBAL_SLOTS = 64;
#endif

  /** The number of page shards: the record lock queues, by page. */
  static constexpr size_t N_PAGE_SHARDS = 512;

  /** The number of table shards: the table lock queues, by table id. */
  static constexpr size_t N_TABLE_SHARDS = 512;

  /** A slot of the global latch, on a cache line of its own. */
  struct alignas(64) GlobalSlot {
    std::shared_mutex m_latch;
  };

  /** A page shard: its latch and the record lock queues of its pages. */
  struct alignas(64) PageShard {
    std::mutex m_mutex;
    Page_id_hash<Rec_locks> m_locks;
  };

  /** A table shard: its latch (the queues are in Table::m_locks). */
  struct alignas(64) TableShard {
    std::mutex m_mutex;
  };

  /** @return the page shard of a page. */
  [[nodiscard]] static uint32_t page_shard_no(PageId page_id) noexcept {
    const auto h = (uint64_t(page_id.m_space_id) << 32 | page_id.m_page_no) * 0x9E3779B97F4A7C15ULL;
    return uint32_t(h >> 55) % N_PAGE_SHARDS;
  }

  /** @return the table shard of a table. */
  [[nodiscard]] static uint32_t table_shard_no(const Table *table) noexcept;

  /** What the calling thread holds of the latches of a lock system: for the
  checks of the latching order and the ownership asserts, and for the
  reentrancy of X (a thread with X needs no other latch). Thread local. */
  struct Held {
    const LockSys *m_owner{};
    uint32_t m_x{};
    int32_t m_s{-1};
    std::array<int32_t, 2> m_pages{-1, -1};
    int32_t m_table{-1};
  };

  /** @return the latches of this lock system the calling thread holds. */
  [[nodiscard]] Held &held() const noexcept;

  /** @return true if the calling thread holds the global latch in X. */
  [[nodiscard]] bool x_own() const noexcept { return held().m_x > 0; }

  /** @return true if the calling thread holds the global latch, S or X. */
  [[nodiscard]] bool latched() const noexcept { return x_own() || held().m_s >= 0; }

  /** @return true if the calling thread holds no latch of the lock system. */
  [[nodiscard]] bool nothing_latched() const noexcept { return !latched(); }

  /** @return true if the calling thread may read and change the queue of page_id. */
  [[nodiscard]] bool owns_page(PageId page_id) const noexcept {
    const auto &h = held();
    const auto shard = int32_t(page_shard_no(page_id));
    return h.m_x > 0 || (h.m_s >= 0 && (h.m_pages[0] == shard || h.m_pages[1] == shard));
  }

  /** @return true if the calling thread may read and change the queue of table. */
  [[nodiscard]] bool owns_table(const Table *table) const noexcept {
    const auto &h = held();
    return h.m_x > 0 || (h.m_s >= 0 && h.m_table == int32_t(table_shard_no(table)));
  }

  /** The global latch in X (all its slots), reentrant. No other latch of
  the lock system, nor the trx sys mutex, nor a trx mutex may be held. */
  void x_lock() noexcept;
  void x_unlock() noexcept;

  /** The global latch in S, one slot. */
  void s_lock() noexcept;
  void s_unlock() noexcept;

  /** The global latch in X for a scope (a no-op if the thread has it). */
  class Exclusive {
   public:
    explicit Exclusive(LockSys &lock_sys) noexcept : m_lock_sys(lock_sys) { m_lock_sys.x_lock(); }
    ~Exclusive() noexcept { m_lock_sys.x_unlock(); }

    Exclusive(const Exclusive &) = delete;
    Exclusive &operator=(const Exclusive &) = delete;

   private:
    LockSys &m_lock_sys;
  };

  /** The global latch in S and one or two page shard latches for a scope (a
  no-op if the thread holds X). */
  class PageLatch {
   public:
    PageLatch(LockSys &lock_sys, PageId page_id) noexcept;
    PageLatch(LockSys &lock_sys, PageId a, PageId b) noexcept;
    ~PageLatch() noexcept;

    PageLatch(const PageLatch &) = delete;
    PageLatch &operator=(const PageLatch &) = delete;

   private:
    LockSys &m_lock_sys;
    bool m_noop{};
  };

  /** The global latch in S and a table shard latch for a scope (a no-op if
  the thread holds X). A page shard latch may be held. */
  class TableLatch {
   public:
    TableLatch(LockSys &lock_sys, const Table *table) noexcept;
    ~TableLatch() noexcept;

    TableLatch(const TableLatch &) = delete;
    TableLatch &operator=(const TableLatch &) = delete;

   private:
    LockSys &m_lock_sys;
    bool m_noop{};
    bool m_s_taken{};
  };

  /** The waiting transactions whose waiting lock a page operation of the
  calling thread moved, for deadlock_check_moved(). Thread local. */
  [[nodiscard]] static std::vector<Trx *> &moved() noexcept;

  /** The requester of the calling thread whose latch free deadlock walk was
  suspect, for deadlock_after_wait(). Thread local. */
  [[nodiscard]] static Trx *&suspect() noexcept;

  /** The slots of the global latch. */
  std::array<GlobalSlot, N_GLOBAL_SLOTS> m_global{};

  /** The page shards. */
  std::array<PageShard, N_PAGE_SHARDS> m_page_shards{};

  /** The table shards. */
  std::array<TableShard, N_TABLE_SHARDS> m_table_shards{};

  /**
   * @brief The buffer pool.
   *
   * This pointer holds the buffer pool, which manages the buffer pool. The pointer
   * is not owned by the lock system, but by the buffer pool. We extract the buffer pool
   * pointer from TrxSys::Fil::m_sys.m_buf_pool to simplify the calls.
   *
   * FIXME: This needs to be fixed, reduce these dependencies. We let it go for now
   * to simplify the changes.
   */

  /**
   * @brief The transaction system.
   *
   * This pointer holds the transaction system, which manages all transactions in the system.
   */

  /**
   * @brief Whether to print the InnoDB lock monitor.
   *
   * This flag indicates whether the InnoDB lock monitor should print information
   * about lock requests and waits.
   */
  /** Set by DDL (the lock monitor table) and tests, read by the monitor
  thread without a latch: a relaxed atomic (a flag, it orders nothing). */
  std::atomic<bool> m_print_lock_monitor{false};

  /**
   * @brief Whether a deadlock was found during the latest deadlock detection.
   *
   * This flag indicates whether a deadlock was detected during the most recent
   * deadlock detection cycle. It is used for logging and monitoring purposes.
   */
  bool m_deadlock_found{false};

  /** The most locks of one shard a release frees in a row while it holds
  the shard latch (release()): a bound on how long it makes others wait. */
  static constexpr ulint LOCK_RELEASE_BATCH = 64;

  /** The epoch of the latest full deadlock search: Trx::m_deadlock_mark
  equal to it marks a transaction whose subtree that search visited.
  Starts at 1, transactions at 0. With the global latch in X. */
  uint64_t m_deadlock_epoch{1};

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
