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
#include "buf0buf.h"
#include "buf0types.h"
#include "dict0dict.h"
#include "dict0types.h"
#include "hash0hash.h"
#include "innodb0types.h"
#include "lock0types.h"
#include "log0recv.h"
#include "mtr0types.h"
#include "page0cur.h"
#include "page0page.h"
#include "que0que.h"
#include "que0types.h"
#include "read0read.h"
#include "read0types.h"
#include "rem0types.h"
#include "row0row.h"
#include "row0vers.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0types.h"

#ifdef UNIV_DEBUG
extern bool lock_print_waits;
#endif /* UNIV_DEBUG */

/* Buffer for storing information about the most recent deadlock error */
extern ib_stream_t lock_latest_err_stream;

/** Gets the size of a lock struct.
@return	size in bytes */

ulint lock_get_size();

/** Creates the lock system at database start. */
void lock_sys_create(ulint n_cells); /*!< in: number of slots in lock hash table */

/** Closes the lock system at database shutdown. */
void lock_sys_close();

/** Updates the lock table when we have reorganized a page. NOTE: we copy
also the locks set on the infimum of the page; the infimum may carry
locks if an update of a record is occurring on the page, and its locks
were temporarily stored on the infimum. */
void lock_move_reorganize_page(
  const buf_block_t *block, /*!< in: old index page, now
                                reorganized */
  const buf_block_t *oblock
); /*!< in: copy of the old, not
                                reorganized page */

/** Moves the explicit locks on user records to another page if a record
list end is moved to another page. */
void lock_move_rec_list_end(
  const buf_block_t *new_block, /*!< in: index page to move to */
  const buf_block_t *block,     /*!< in: index page */
  const rec_t *rec
); /*!< in: record on page: this
                                  is the first record moved */

/** Moves the explicit locks on user records to another page if a record
list start is moved to another page. */
void lock_move_rec_list_start(
  const buf_block_t *new_block, /*!< in: index page to move to */
  const buf_block_t *block,     /*!< in: index page */
  const rec_t *rec,             /*!< in: record on page:
                                  this is the first
                                  record NOT copied */
  const rec_t *old_end
); /*!< in: old
                                  previous-to-last
                                  record on new_page
                                  before the records
                                  were copied */

/** Updates the lock table when a page is split to the right. */
void lock_update_split_right(
  const buf_block_t *right_block, /*!< in: right page */
  const buf_block_t *left_block
); /*!< in: left page */

/** Updates the lock table when a page is merged to the right. */
void lock_update_merge_right(
  const buf_block_t *right_block, /*!< in: right page to
                                    which merged */
  const rec_t *orig_succ,         /*!< in: original
                                    successor of infimum
                                    on the right page
                                    before merge */
  const buf_block_t *left_block
); /*!< in: merged index
                                    page which will be
                                    discarded */

/** Updates the lock table when the root page is copied to another in
btr_root_raise_and_insert. Note that we leave lock structs on the
root page, even though they do not make sense on other than leaf
pages: the reason is that in a pessimistic update the infimum record
of the root page will act as a dummy carrier of the locks of the record
to be updated. */
void lock_update_root_raise(
  const buf_block_t *block, /*!< in: index page to which copied */
  const buf_block_t *root
); /*!< in: root page */

/** Updates the lock table when a page is copied to another and the original
page is removed from the chain of leaf pages, except if page is the root! */
void lock_update_copy_and_discard(
  const buf_block_t *new_block, /*!< in: index page to
                                  which copied */
  const buf_block_t *block
); /*!< in: index page;
                                  NOT the root! */

/** Updates the lock table when a page is split to the left. */
void lock_update_split_left(
  const buf_block_t *right_block, /*!< in: right page */
  const buf_block_t *left_block
); /*!< in: left page */

/** Updates the lock table when a page is merged to the left. */
void lock_update_merge_left(
  const buf_block_t *left_block, /*!< in: left page to
                                     which merged */
  const rec_t *orig_pred,        /*!< in: original predecessor
                                     of supremum on the left page
                                     before merge */
  const buf_block_t *right_block
); /*!< in: merged index page
                                     which will be discarded */

/** Resets the original locks on heir and replaces them with gap type locks
inherited from rec. */
void lock_rec_reset_and_inherit_gap_locks(
  const buf_block_t *heir_block, /*!< in: block containing the
                                   record which inherits */
  const buf_block_t *block,      /*!< in: block containing the
                                   record from which inherited;
                                   does NOT reset the locks on
                                   this record */
  ulint heir_heap_no,            /*!< in: heap_no of the
                                   inheriting record */
  ulint heap_no
); /*!< in: heap_no of the
                                   donating record */

/** Updates the lock table when a page is discarded. */
void lock_update_discard(
  const buf_block_t *heir_block, /*!< in: index page
                                   which will inherit the locks */
  ulint heir_heap_no,            /*!< in: heap_no of the record
                                   which will inherit the locks */
  const buf_block_t *block
); /*!< in: index page
                                   which will be discarded */

/** Updates the lock table when a new user record is inserted. */
void lock_update_insert(
  const buf_block_t *block, /*!< in: buffer block containing rec */
  const rec_t *rec
); /*!< in: the inserted record */

/** Updates the lock table when a record is removed. */
void lock_update_delete(
  const buf_block_t *block, /*!< in: buffer block containing rec */
  const rec_t *rec
); /*!< in: the record to be removed */

/** Stores on the page infimum record the explicit locks of another record.
This function is used to store the lock state of a record when it is
updated and the size of the record changes in the update. The record
is in such an update moved, perhaps to another page. The infimum record
acts as a dummy carrier record, taking care of lock releases while the
actual record is being moved. */
void lock_rec_store_on_page_infimum(
  const buf_block_t *block, /*!< in: buffer block containing rec */
  const rec_t *rec
); /*!< in: record whose lock state
                              is stored on the infimum
                              record of the same page; lock
                              bits are reset on the
                              record */

/** Restores the state of explicit lock requests on a single record, where the
state was stored on the infimum of the page. */
void lock_rec_restore_from_page_infimum(
  const buf_block_t *block, /*!< in: buffer block containing rec */
  const rec_t *rec,         /*!< in: record whose lock state
                                 is restored */
  const buf_block_t *donator
); /*!< in: page (rec is not
                                necessarily on this page)
                                whose infimum stored the lock
                                state; lock bits are reset on
                                the infimum */

/** Returns true if there are explicit record locks on a page.
@return	true if there are explicit record locks on the page */
bool lock_rec_expl_exist_on_page(
  ulint space, /*!< in: space id */
  ulint page_no
); /*!< in: page number */

/** Checks if locks of other transactions prevent an immediate insert of
a record. If they do, first tests if the query thread should anyway
be suspended for some reason; if not, then puts the transaction and
the query thread to the lock wait state and inserts a waiting request
for a gap x-lock to the lock queue.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_rec_insert_check_and_lock(
  ulint flags,         /*!< in: if BTR_NO_LOCKING_FLAG bit is
                         set, does nothing */
  const rec_t *rec,    /*!< in: record after which to insert */
  buf_block_t *block,  /*!< in/out: buffer block of rec */
  dict_index_t *index, /*!< in: index */
  que_thr_t *thr,      /*!< in: query thread */
  mtr_t *mtr,          /*!< in/out: mini-transaction */
  bool *inherit
); /*!< out: set to true if the new
                         inserted record maybe should inherit
                         LOCK_GAP type locks from the successor
                         record */

/** Checks if locks of other transactions prevent an immediate modify (update,
delete mark, or delete unmark) of a clustered index record. If they do,
first tests if the query thread should anyway be suspended for some
reason; if not, then puts the transaction and the query thread to the
lock wait state and inserts a waiting request for a record x-lock to the
lock queue.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_clust_rec_modify_check_and_lock(
  ulint flags,              /*!< in: if BTR_NO_LOCKING_FLAG
                              bit is set, does nothing */
  const buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: record which should be
                              modified */
  dict_index_t *index,      /*!< in: clustered index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(rec, index) */
  que_thr_t *thr
); /*!< in: query thread */

/** Checks if locks of other transactions prevent an immediate modify
(delete mark or delete unmark) of a secondary index record.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_sec_rec_modify_check_and_lock(
  ulint flags,         /*!< in: if BTR_NO_LOCKING_FLAG
                         bit is set, does nothing */
  buf_block_t *block,  /*!< in/out: buffer block of rec */
  const rec_t *rec,    /*!< in: record which should be
                         modified; NOTE: as this is a secondary
                         index, we always have to modify the
                         clustered index record first: see the
                         comment below */
  dict_index_t *index, /*!< in: secondary index */
  que_thr_t *thr,      /*!< in: query thread */
  mtr_t *mtr
); /*!< in/out: mini-transaction */

/** Like the counterpart for a clustered index below, but now we read a
secondary index record.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_sec_rec_read_check_and_lock(
  ulint flags,              /*!< in: if BTR_NO_LOCKING_FLAG
                              bit is set, does nothing */
  const buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: user record or page
                              supremum record which should
                              be read or passed over by a
                              read cursor */
  dict_index_t *index,      /*!< in: secondary index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(rec, index) */
  enum Lock_mode mode,      /*!< in: mode of the lock which
                              the read cursor should set on
                              records: LOCK_S or LOCK_X; the
                              latter is possible in
                              SELECT FOR UPDATE */
  ulint gap_mode,           /*!< in: LOCK_ORDINARY, LOCK_GAP, or
                             LOCK_REC_NOT_GAP */
  que_thr_t *thr
); /*!< in: query thread */

/** Checks if locks of other transactions prevent an immediate read, or passing
over by a read cursor, of a clustered index record. If they do, first tests
if the query thread should anyway be suspended for some reason; if not, then
puts the transaction and the query thread to the lock wait state and inserts a
waiting request for a record lock to the lock queue. Sets the requested mode
lock on the record.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_clust_rec_read_check_and_lock(
  ulint flags,              /*!< in: if BTR_NO_LOCKING_FLAG
                              bit is set, does nothing */
  const buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: user record or page
                              supremum record which should
                              be read or passed over by a
                              read cursor */
  dict_index_t *index,      /*!< in: clustered index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(rec, index) */
  enum Lock_mode mode,      /*!< in: mode of the lock which
                              the read cursor should set on
                              records: LOCK_S or LOCK_X; the
                              latter is possible in
                              SELECT FOR UPDATE */
  ulint gap_mode,           /*!< in: LOCK_ORDINARY, LOCK_GAP, or
                             LOCK_REC_NOT_GAP */
  que_thr_t *thr
); /*!< in: query thread */

/** Checks if locks of other transactions prevent an immediate read, or passing
over by a read cursor, of a clustered index record. If they do, first tests
if the query thread should anyway be suspended for some reason; if not, then
puts the transaction and the query thread to the lock wait state and inserts a
waiting request for a record lock to the lock queue. Sets the requested mode
lock on the record. This is an alternative version of
lock_clust_rec_read_check_and_lock() that does not require the parameter
"offsets".
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */

db_err lock_clust_rec_read_check_and_lock_alt(
  ulint flags,              /*!< in: if BTR_NO_LOCKING_FLAG
                              bit is set, does nothing */
  const buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: user record or page
                              supremum record which should
                              be read or passed over by a
                              read cursor */
  dict_index_t *index,      /*!< in: clustered index */
  enum Lock_mode mode,      /*!< in: mode of the lock which
                              the read cursor should set on
                              records: LOCK_S or LOCK_X; the
                              latter is possible in
                              SELECT FOR UPDATE */
  ulint gap_mode,           /*!< in: LOCK_ORDINARY, LOCK_GAP, or
                             LOCK_REC_NOT_GAP */
  que_thr_t *thr
); /*!< in: query thread */

/** Checks that a record is seen in a consistent read.
@return true if sees, or false if an earlier version of the record
should be retrieved */
bool lock_clust_rec_cons_read_sees(
  const rec_t *rec,     /*!< in: user record which should be read or
                          passed over by a read cursor */
  dict_index_t *index,  /*!< in: clustered index */
  const ulint *offsets, /*!< in: Phy_rec::get_col_offsets(rec, index) */
  read_view_t *view
); /*!< in: consistent read view */

/** Checks that a non-clustered index record is seen in a consistent read.

NOTE that a non-clustered index page contains so little information on
its modifications that also in the case false, the present version of
rec may be the right, but we must check this from the clustered index
record.

@return true if certainly sees, or false if an earlier version of the
clustered index record might be needed */
ulint lock_sec_rec_cons_read_sees(
  const rec_t *rec, /*!< in: user record which
                              should be read or passed over
                              by a read cursor */
  const read_view_t *view
); /*!< in: consistent read view */

/** Locks the specified database table in the mode given. If the lock cannot
be granted immediately, the query thread is put to wait.
@return	DB_SUCCESS, DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED */
db_err lock_table(
  ulint flags,         /*!< in: if BTR_NO_LOCKING_FLAG bit is set,
                                does nothing */
  dict_table_t *table, /*!< in: database table in dictionary cache */
  enum Lock_mode mode, /*!< in: lock mode */
  que_thr_t *thr
); /*!< in: query thread */

/** Removes a granted record lock of a transaction from the queue and grants
locks to other transactions waiting in the queue if they now are entitled
to a lock. */
void lock_rec_unlock(
  trx_t *trx,               /*!< in: transaction that has
                               set a record lock */
  const buf_block_t *block, /*!< in: buffer block containing rec */
  const rec_t *rec,         /*!< in: record */
  enum Lock_mode Lock_mode
); /*!< in: LOCK_S or LOCK_X */

/** Releases transaction locks, and releases possible other transactions waiting
because of these locks. */
void lock_release_off_kernel(trx_t *trx); /*!< in: transaction */

/** Cancels a waiting lock request and releases possible other transactions
waiting behind it. */
void lock_cancel_waiting_and_release(Lock *lock); /*!< in: waiting lock request */

/** Removes locks on a table to be dropped or truncated.
If remove_also_table_sx_locks is true then table-level S and X locks are
also removed in addition to other table-level and record-level locks.
No lock, that is going to be removed, is allowed to be a wait lock. */
void lock_remove_all_on_table(
  dict_table_t *table, /*!< in: table to be dropped
                                      or truncated */
  bool remove_also_table_sx_locks
); /*!< in: also removes
                                   table S and X locks */

/** Calculates the fold value of a page file address: used in inserting or
searching for a lock in the hash table.
@return	folded value */
inline ulint lock_rec_fold(
  ulint space, /*!< in: space */
  ulint page_no
) /*!< in: page number */
  __attribute__((const));

/** Calculates the hash value of a page file address: used in inserting or
searching for a lock in the hash table.
@return	hashed value */
inline ulint lock_rec_hash(
  ulint space, /*!< in: space */
  ulint page_no
); /*!< in: page number */

/** Looks for a set bit in a record lock bitmap. Returns ULINT_UNDEFINED,
if none found.
@return bit index == heap number of the record, or ULINT_UNDEFINED if
none found */
ulint lock_rec_find_set_bit(const Lock *lock); /*!< in: record lock with at
                                                 least one bit set */

/** Gets the source table of an ALTER TABLE transaction.  The table must be
covered by an IX or IS table lock.
@return the source table of transaction, if it is covered by an IX or
IS table lock; dest if there is no source table, and NULL if the
transaction is locking more than two tables or an inconsistency is
found */
dict_table_t *lock_get_src_table(
  trx_t *trx,         /*!< in: transaction */
  dict_table_t *dest, /*!< in: destination of ALTER TABLE */
  Lock_mode *mode
); /*!< out: lock mode of the source table */

/** Determine if the given table is exclusively "owned" by the given
transaction, i.e., transaction holds LOCK_IX and possibly LOCK_AUTO_INC
on the table.
@return true if table is only locked by trx, with LOCK_IX, and
possibly LOCK_AUTO_INC */
bool lock_is_table_exclusive(
  dict_table_t *table, /*!< in: table */
  trx_t *trx
); /*!< in: transaction */

/** Checks if a lock request lock1 has to wait for request lock2.
@return	true if lock1 has to wait for lock2 to be removed */
bool lock_has_to_wait(
  const Lock *lock1, /*!< in: waiting lock */
  const Lock *lock2
); /*!< in: another lock; NOTE that it
                                            is assumed that this has a lock bit
                                            set on the same record as in lock1
                                            if the locks are record locks */

/** Checks that a transaction id is sensible, i.e., not in the future.
@return	true if ok */
bool lock_check_trx_id_sanity(
  trx_id_t trx_id,      /*!< in: trx id */
  const rec_t *rec,     /*!< in: user record */
  dict_index_t *index,  /*!< in: clustered index */
  const ulint *offsets, /*!< in: Phy_rec::get_col_offsets(rec, index) */
  bool has_kernel_mutex
); /*!< in: true if the caller owns the kernel mutex */

/** Prints info of a table lock. */
void lock_table_print(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const Lock *lock
); /*!< in: table type lock */

/** Prints info of a record lock. */
void lock_rec_print(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const Lock *lock
); /*!< in: record type lock */

/** Prints info of locks for all transactions.
@return false if not able to obtain kernel mutex
and exits without printing info */
bool lock_print_info_summary(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  bool nowait
); /*!< in: whether to wait for the
                            kernel mutex */

/** Prints info of locks for each transaction. */
void lock_print_info_all_transactions(ib_stream_t ib_stream); /*!< in: stream where to print */

/** Return approximate number or record locks (bits set in the bitmap) for
this transaction. Since delete-marked records may be removed, the
record count will not be precise. */
ulint lock_number_of_rows_locked(trx_t *trx); /*!< in: transaction */

/** Gets the type of a lock. Non-inline version for using outside of the
lock module.
@return	LOCK_TABLE or LOCK_REC */
ulint lock_get_type(const Lock *lock); /*!< in: lock */

/** Gets the id of the transaction owning a lock.
@return	transaction id */
uint64_t lock_get_trx_id(const Lock *lock); /*!< in: lock */

/** Gets the mode of a lock in a human readable string.
The string should not be free()'d or modified.
@return	lock mode */
const char *lock_get_mode_str(const Lock *lock); /*!< in: lock */

/** Gets the type of a lock in a human readable string.
The string should not be free()'d or modified.
@return	lock type */
const char *lock_get_type_str(const Lock *lock); /*!< in: lock */

/** Gets the id of the table on which the lock is.
@return	id of the table */
uint64_t lock_get_table_id(const Lock *lock); /*!< in: lock */

/** Gets the name of the table on which the lock is.
The string should not be free()'d or modified.
@return	name of the table */
const char *lock_get_table_name(const Lock *lock); /*!< in: lock */

/** For a record lock, gets the index on which the lock is.
@return	index */
const dict_index_t *lock_rec_get_index(const Lock *lock); /*!< in: lock */

/** For a record lock, gets the name of the index on which the lock is.
The string should not be free()'d or modified.
@return	name of the index */
const char *lock_rec_get_index_name(const Lock *lock); /*!< in: lock */

/** For a record lock, gets the tablespace number on which the lock is.
@return	tablespace number */
ulint lock_rec_get_space_id(const Lock *lock); /*!< in: lock */

/** For a record lock, gets the page number on which the lock is.
@return	page number */
ulint lock_rec_get_page_no(const Lock *lock); /*!< in: lock */

/** Reset the lock variables. */
void lock_var_init(void);

/** Closes the lock system at database shutdown. */
void lock_sys_close(void);

/** Lock modes and types */
/* @{ */

/** Mask used to extract mode from the type_mode field in a lock */
constexpr ulint LOCK_MODE_MASK = 0xFUL;

/** Lock types */
/* @{ */

/** Table lock */
constexpr ulint LOCK_TABLE = 16;

/** Record lock. */
constexpr ulint LOCK_REC = 32;

/** Mask used to extract lock type from the type_mode field in a lock */
constexpr ulint LOCK_TYPE_MASK = 0xF0UL;

static_assert(!(LOCK_MODE_MASK & LOCK_TYPE_MASK), "error LOCK_MODE_MASK & LOCK_TYPE_MASK");

/** Waiting lock flag; when set, it means that the lock has not
yet been granted, it is just waiting for its turn in the wait queue */
constexpr ulint LOCK_WAIT = 256;

/* Precise modes */

/** This flag denotes an ordinary next-key lock in contrast to
LOCK_GAP or LOCK_REC_NOT_GAP */
constexpr ulint LOCK_ORDINARY = 0;

/** When this bit is set, it means that the lock holds only on the
gap before the record; for instance, an x-lock on the gap does not
give permission to modify the record on which the bit is set; locks
of this type are created when records are removed from the index
chain of records */
constexpr ulint LOCK_GAP = 512;

/** This bit means that the lock is only on the index record and does
NOT block inserts to the gap before the index record; this is used in
the case when we retrieve a record with a unique key, and is also used
in locking plain SELECTs (not part of UPDATE or DELETE) when the user
has set the READ COMMITTED isolation level */
constexpr ulint LOCK_REC_NOT_GAP = 1024;

/** This bit is set when we place a waiting gap type record lock request
in order to let an insert of an index record to wait until there are no
conflicting locks by other transactions on the gap; note that this flag
remains set when the waiting lock is granted, or if the lock is inherited
to a neighboring record */
constexpr ulint LOCK_INSERT_INTENTION = 2048;

static_assert(
  !((LOCK_WAIT | LOCK_GAP | LOCK_REC_NOT_GAP | LOCK_INSERT_INTENTION) & LOCK_MODE_MASK),
  "Lock modes should be independent bits and be maskabe by LOCK_MODE_MASK"
);

static_assert(
  !((LOCK_WAIT | LOCK_GAP | LOCK_REC_NOT_GAP | LOCK_INSERT_INTENTION) & LOCK_TYPE_MASK),
  "Lock types should be independent bits and be maskable by LOCK_TYPE_MASK"
);

/* @} */

/** Lock operation struct */
typedef struct lock_op_struct lock_op_t;

/** Lock operation struct */
struct lock_op_struct {
  dict_table_t *table; /*!< table to be locked */
  enum Lock_mode mode; /*!< lock mode */
};

/** The lock system struct */
struct lock_sys_t {
  hash_table_t *rec_hash; /*!< hash table of the record locks */
};

/** The lock system */
extern lock_sys_t *lock_sys;

/** Calculates the fold value of a page file address: used in inserting or
searching for a lock in the hash table.
@return	folded value */
inline ulint lock_rec_fold(
  ulint space, /*!< in: space */
  ulint page_no
) /*!< in: page number */
{
  return (ut_fold_ulint_pair(space, page_no));
}

/** Calculates the hash value of a page file address: used in inserting or
searching for a lock in the hash table.
@return	hashed value */
inline ulint lock_rec_hash(
  ulint space, /*!< in: space */
  ulint page_no
) /*!< in: page number */
{
  return (hash_calc_hash(lock_rec_fold(space, page_no), lock_sys->rec_hash));
}

/**
 * @brief Checks if some transaction has an implicit x-lock on a record in a clustered index.
 *
 * This function checks if there is an active transaction that holds an implicit
 * exclusive lock on the given record in a clustered index.
 *
 * @param[in] rec The user record to check
 * @param[in] dict_index The clustered index containing the record
 * @param[in] offsets The column offsets for the record, obtained from Phy_rec::get_col_offsets(rec, index)
 *
 * @return A pointer to the transaction that holds the implicit x-lock, or NULL if no such transaction exists
 *
 * @note This function assumes that the kernel mutex is held by the caller.
 * @note The index must be a clustered index.
 * @note The record must be a user record (not a supremum/infinimum record).
 */
inline const trx_t *lock_clust_rec_some_has_impl(const rec_t *rec, dict_index_t *dict_index, const ulint *offsets) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(dict_index_is_clust(dict_index));
  ut_ad(page_rec_is_user_rec(rec));

  auto trx_id = row_get_rec_trx_id(rec, dict_index, offsets);

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
inline ulint lock_get_min_heap_no(const buf_block_t *block) {
  const page_t *page = block->m_frame;

  return rec_get_heap_no(page + rec_get_next_offs(page + PAGE_INFIMUM));
}

#ifdef UNIT_TESTING
/** Creates a new record lock and inserts it to the lock queue. Does NOT check
for deadlocks or lock compatibility!
@param[in] type_mode            Lock mode and wait flag, type is ignored and replaced by LOCK_REC
@param[in] space                Tablespace ID.
@param[in] page_no              Page number in tablespace
@param[in] heap_no              Heap number in page
@param[in] n_bits               No. of bits in lock bitmap
@param[in] index                Index of record
@param[in,out] trx              Transaction that wants to create the record lock.
@return	created lock */
Lock *lock_rec_create_low(
  ulint type_mode,
  space_id_t space,
  page_no_t page_no,
  ulint heap_no,
  ulint n_bits,
  dict_index_t *index,
  trx_t *trx);


/** Check if a transaction has any other transaction waiting on its locks.
@param[in] trx                  Transaction to check.
@return true if some other transaction is waiting on its locks. */
bool lock_trx_has_no_waiters(const trx_t *trx);

#endif /* UNIT_TESTING */

