/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/** @file lock/lock0lock.c
The transaction lock system

Created 5/7/1996 Heikki Tuuri
*******************************************************/

#include "lock0lock.h"
#include "lock0priv.h"

#include "api0ucode.h"
#include "dict0mem.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "usr0sess.h"

/* Restricts the length of search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK = 1000000;

/* Restricts the recursion depth of the search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK = 200;

/* When releasing transaction locks, this specifies how often we release
the kernel mutex for a moment to give also others access to it */
constexpr ulint LOCK_RELEASE_KERNEL_INTERVAL = 1000;

/* Safety margin when creating a new record lock: this many extra records
can be inserted to the page without need to create a lock with a bigger
bitmap */
constexpr ulint LOCK_PAGE_BITMAP_MARGIN = 64;

/* An explicit record lock affects both the record and the gap before it.
An implicit x-lock does not affect the gap, it only locks the index
record from read or update.

If a transaction has modified or inserted an index record, then
it owns an implicit x-lock on the record. On a secondary index record,
a transaction has an implicit x-lock also if it has modified the
clustered index record, the max trx id of the page where the secondary
index record resides is >= trx id of the transaction (or database recovery
is running), and there are no explicit non-gap lock requests on the
secondary index record.

This complicated definition for a secondary index comes from the
implementation: we want to be able to determine if a secondary index
record has an implicit x-lock, just by looking at the present clustered
index record, not at the historical versions of the record. The
complicated definition can be explained to the user so that there is
nondeterminism in the access path when a query is answered: we may,
or may not, access the clustered index record and thus may, or may not,
bump into an x-lock set there.

Different transaction can have conflicting locks set on the gap at the
same time. The locks on the gap are purely inhibitive: an insert cannot
be made, or a select cursor may have to wait if a different transaction
has a conflicting lock on the gap. An x-lock on the gap does not give
the right to insert into the gap.

An explicit lock can be placed on a user record or the supremum record of
a page. The locks on the supremum record are always thought to be of the gap
type, though the gap bit is not set. When we perform an update of a record
where the size of the record changes, we may temporarily store its explicit
locks on the infimum record of the page, though the infimum otherwise never
carries locks.

A waiting record lock can also be of the gap type. A waiting lock request
can be granted when there is no conflicting mode lock request by another
transaction ahead of it in the explicit lock queue.

In version 4.0.5 we added yet another explicit lock type: LOCK_REC_NOT_GAP.
It only locks the record it is placed on, not the gap before the record.
This lock type is necessary to emulate an Oracle-like READ COMMITTED isolation
level.

-------------------------------------------------------------------------
RULE 1: If there is an implicit x-lock on a record, and there are non-gap
-------
lock requests waiting in the queue, then the transaction holding the implicit
x-lock also has an explicit non-gap record x-lock. Therefore, as locks are
released, we can grant locks to waiting lock requests purely by looking at
the explicit lock requests in the queue.

RULE 3: Different transactions cannot have conflicting granted non-gap locks
-------
on a record at the same time. However, they can have conflicting granted gap
locks.
RULE 4: If a there is a waiting lock request in a queue, no lock request,
-------
gap or not, can be inserted ahead of it in the queue. In record deletes
and page splits new gap type locks can be created by the database manager
for a transaction, and without rule 4, the waits-for graph of transactions
might become cyclic without the database noticing it, as the deadlock check
is only performed when a transaction itself requests a lock!
-------------------------------------------------------------------------

An insert is allowed to a gap if there are no explicit lock requests by
other transactions on the next record. It does not matter if these lock
requests are granted or waiting, gap bit set or not, with the exception
that a gap type request set by another transaction to wait for
its turn to do an insert is ignored. On the other hand, an
implicit x-lock by another transaction does not prevent an insert, which
allows for more concurrency when using an Oracle-style sequence number
generator for the primary key with many transactions doing inserts
concurrently.

A modify of a record is allowed if the transaction has an x-lock on the
record, or if other transactions do not have any non-gap lock requests on the
record.

A read of a single user record with a cursor is allowed if the transaction
has a non-gap explicit, or an implicit lock on the record, or if the other
transactions have no x-lock requests on the record. At a page supremum a
read is always allowed.

In summary, an implicit lock is seen as a granted x-lock only on the
record, not on the gap. An explicit lock with no gap bit set is a lock
both on the record and the gap. If the gap bit is set, the lock is only
on the gap. Different transaction cannot own conflicting locks on the
record at the same time, but they may own conflicting locks on the gap.
Granted locks on a record give an access right to the record, but gap type
locks just inhibit operations.

NOTE: Finding out if some transaction has an implicit x-lock on a secondary
index record can be cumbersome. We may have to look at previous versions of
the corresponding clustered index record to find out if a delete marked
secondary index record was delete marked by an active transaction, not by
a committed one.

FACT A: If a transaction has inserted a row, it can delete it any time
without need to wait for locks.

PROOF: The transaction has an implicit x-lock on every index record inserted
for the row, and can thus modify each record without the need to wait. Q.E.D.

FACT B: If a transaction has read some result set with a cursor, it can read
it again, and retrieves the same result set, if it has not modified the
result set in the meantime. Hence, there is no phantom problem. If the
biggest record, in the alphabetical order, touched by the cursor is removed,
a lock wait may occur, otherwise not.

PROOF: When a read cursor proceeds, it sets an s-lock on each user record
it passes, and a gap type s-lock on each page supremum. The cursor must
wait until it has these locks granted. Then no other transaction can
have a granted x-lock on any of the user records, and therefore cannot
modify the user records. Neither can any other transaction insert into
the gaps which were passed over by the cursor. Page splits and merges,
and removal of obsolete versions of records do not affect this, because
when a user record or a page supremum is removed, the next record inherits
its locks as gap type locks, and therefore blocks inserts to the same gap.
Also, if a page supremum is inserted, it inherits its locks from the successor
record. When the cursor is positioned again at the start of the result set,
the records it will touch on its course are either records it touched
during the last pass or new inserted page supremums. It can immediately
access all these records, and when it arrives at the biggest record, it
notices that the result set is complete. If the biggest record was removed,
lock wait can occur because the next record only inherits a gap type lock,
and a wait may be needed. Q.E.D. */

/* If an index record should be changed or a new inserted, we must check
the lock on the record or the next. When a read cursor starts reading,
we will set a record level s-lock on each record it passes, except on the
initial record on which the cursor is positioned before we start to fetch
records. Our index tree search has the convention that the B-tree
cursor is positioned BEFORE the first possibly matching record in
the search. Optimizations are possible here: if the record is searched
on an equality condition to a unique key, we could actually set a special
lock on the record, a lock which would not prevent any insert before
this record. In the next key locking an x-lock set on a record also
prevents inserts just before that record.
        There are special infimum and supremum records on each page.
A supremum record can be locked by a read cursor. This records cannot be
updated but the lock prevents insert of a user record to the end of
the page.
        Next key locks will prevent the phantom problem where new rows
could appear to SELECT result sets after the select operation has been
performed. Prevention of phantoms ensures the serilizability of
transactions.
        What should we check if an insert of a new record is wanted?
Only the lock on the next record on the same page, because also the
supremum record can carry a lock. An s-lock prevents insertion, but
what about an x-lock? If it was set by a searched update, then there
is implicitly an s-lock, too, and the insert should be prevented.
What if our transaction owns an x-lock to the next record, but there is
a waiting s-lock request on the next record? If this s-lock was placed
by a read cursor moving in the ascending order in the index, we cannot
do the insert immediately, because when we finally commit our transaction,
the read cursor should see also the new inserted record. So we should
move the read cursor backward from the next record for it to pass over
the new inserted record. This move backward may be too cumbersome to
implement. If we in this situation just enqueue a second x-lock request
for our transaction on the next record, then the deadlock mechanism
notices a deadlock between our transaction and the s-lock request
transaction. This seems to be an ok solution.
        We could have the convention that granted explicit record locks,
lock the corresponding records from changing, and also lock the gaps
before them from inserting. A waiting explicit lock request locks the gap
before from inserting. Implicit record x-locks, which we derive from the
transaction id in the clustered index record, only lock the record itself
from modification, not the gap before it from inserting.
        How should we store update locks? If the search is done by a unique
key, we could just modify the record trx id. Otherwise, we could put a record
x-lock on the record. If the update changes ordering fields of the
clustered index record, the inserted new record needs no record lock in
lock table, the trx id is enough. The same holds for a secondary index
record. Searched delete is similar to update.

PROBLEM:
What about waiting lock requests? If a transaction is waiting to make an
update to a record which another modified, how does the other transaction
know to send the end-lock-wait signal to the waiting transaction? If we have
the convention that a transaction may wait for just one lock at a time, how
do we preserve it if lock wait ends?

PROBLEM:
Checking the trx id label of a secondary index record. In the case of a
modification, not an insert, is this necessary? A secondary index record
is modified only by setting or resetting its deleted flag. A secondary index
record contains fields to uniquely determine the corresponding clustered
index record. A secondary index record is therefore only modified if we
also modify the clustered index record, and the trx id checking is done
on the clustered index record, before we come to modify the secondary index
record. So, in the case of delete marking or unmarking a secondary index
record, we do not have to care about trx ids, only the locks in the lock
table must be checked. In the case of a select from a secondary index, the
trx id is relevant, and in this case we may have to search the clustered
index record.

PROBLEM: How to update record locks when page is split or merged, or
--------------------------------------------------------------------
a record is deleted or updated?
If the size of fields in a record changes, we perform the update by
a delete followed by an insert. How can we retain the locks set or
waiting on the record? Because a record lock is indexed in the bitmap
by the heap number of the record, when we remove the record from the
record list, it is possible still to keep the lock bits. If the page
is reorganized, we could make a table of old and new heap numbers,
and permute the bitmaps in the locks accordingly. We can add to the
table a row telling where the updated record ended. If the update does
not require a reorganization of the page, we can simply move the lock
bits for the updated record to the position determined by its new heap
number (we may have to allocate a new lock, if we run out of the bitmap
in the old one).
        A more complicated case is the one where the reinsertion of the
updated record is done pessimistically, because the structure of the
tree may change.

PROBLEM: If a supremum record is removed in a page merge, or a record
---------------------------------------------------------------------
removed in a purge, what to do to the waiting lock requests? In a split to
the right, we just move the lock requests to the new supremum. If a record
is removed, we could move the waiting lock request to its inheritor, the
next record in the index. But, the next record may already have lock
requests on its own queue. A new deadlock check should be made then. Maybe
it is easier just to release the waiting transactions. They can then enqueue
new lock requests on appropriate records.

PROBLEM: When a record is inserted, what locks should it inherit from the
-------------------------------------------------------------------------
upper neighbor? An insert of a new supremum record in a page split is
always possible, but an insert of a new user record requires that the upper
neighbor does not have any lock requests by other transactions, granted or
waiting, in its lock queue. Solution: We can copy the locks as gap type
locks, so that also the waiting locks are transformed to granted gap type
locks on the inserted record. */

/* LOCK COMPATIBILITY MATRIX
 *    IS IX S  X  AI
 * IS +	 +  +  -  +
 * IX +	 +  -  -  +
 * S  +	 -  +  -  -
 * X  -	 -  -  -  -
 * AI +	 +  -  -  -
 *
 * Note that for rows, InnoDB only acquires S or X locks.
 * For tables, InnoDB normally acquires IS or IX locks.
 * S or X table locks are only acquired for LOCK TABLES.
 * Auto-increment (AI) locks are needed because of
 * statement-level MySQL binlog.
 * See also lock_mode_compatible().
 */
#define LK(a, b) (1 << ((a)*LOCK_NUM + (b)))
#define LKS(a, b) LK(a, b) | LK(b, a)

/* Define the lock compatibility matrix in a ulint.  The first line below
defines the diagonal entries.  The following lines define the compatibility
for LOCK_IX, LOCK_S, and LOCK_AUTO_INC using LKS(), since the matrix
is symmetric. */
#define LOCK_MODE_COMPATIBILITY                                                                                                \
  0 | LK(LOCK_IS, LOCK_IS) | LK(LOCK_IX, LOCK_IX) | LK(LOCK_S, LOCK_S) | LKS(LOCK_IX, LOCK_IS) | LKS(LOCK_IS, LOCK_AUTO_INC) | \
    LKS(LOCK_S, LOCK_IS) | LKS(LOCK_AUTO_INC, LOCK_IS) | LKS(LOCK_AUTO_INC, LOCK_IX)

/* STRONGER-OR-EQUAL RELATION (mode1=row, mode2=column)
 *    IS IX S  X  AI
 * IS +  -  -  -  -
 * IX +  +  -  -  -
 * S  +  -  +  -  -
 * X  +  +  +  +  +
 * AI -  -  -  -  +
 * See lock_mode_stronger_or_eq().
 */

/* Define the stronger-or-equal lock relation in a ulint.  This relation
contains all pairs LK(mode1, mode2) where mode1 is stronger than or
equal to mode2. */
#define LOCK_MODE_STRONGER_OR_EQ                                                                                      \
  0 | LK(LOCK_IS, LOCK_IS) | LK(LOCK_IX, LOCK_IS) | LK(LOCK_IX, LOCK_IX) | LK(LOCK_S, LOCK_IS) | LK(LOCK_S, LOCK_S) | \
    LK(LOCK_AUTO_INC, LOCK_AUTO_INC) | LK(LOCK_X, LOCK_IS) | LK(LOCK_X, LOCK_IX) | LK(LOCK_X, LOCK_S) |               \
    LK(LOCK_X, LOCK_AUTO_INC) | LK(LOCK_X, LOCK_X)

#ifdef UNIV_DEBUG
bool lock_print_waits = false;

/** Validates the lock system.
@return	true if ok */
static bool lock_validate();

/** Validates the record lock queues on a page.
@return	true if ok */
static bool lock_rec_validate_page(space_id_t space, page_no_t page_no);
#endif /* UNIV_DEBUG */

/* The lock system */
lock_sys_t *lock_sys = nullptr;

/* We store info on the latest deadlock error to this buffer. InnoDB
Monitor will then fetch it and print */
bool lock_deadlock_found = false;

ib_stream_t lock_latest_err_stream;

struct Table_lock_get_node {
  /** Functor for accessing the embedded node within a table lock. */
  static const ut_list_node<Lock> &get_node(const Lock &lock) { return lock.un_member.tab_lock.locks; }
};

/* Flags for recursive deadlock search */
constexpr ulint LOCK_VICTIM_IS_START = 1;
constexpr ulint LOCK_VICTIM_IS_OTHER = 2;
constexpr ulint LOCK_EXCEED_MAX_DEPTH = 3;

/** Checks if a lock request results in a deadlock.
@param[in] lock                 Lock transaction is requesting.
@param[in] trx                  Transaction requesting the lock
@return true if a deadlock was detected and we chose trx as a victim;
false if no deadlock, or there was a deadlock, but we chose other
transaction(s) as victim(s) */
static bool lock_deadlock_occurs(Lock *lock, trx_t *trx);

/** Looks recursively for a deadlock.
@return 0 if no deadlock found, LOCK_VICTIM_IS_START if there was a
deadlock and we chose 'start' as the victim, LOCK_VICTIM_IS_OTHER if a
deadlock was found and we chose some other trx as a victim: we must do
the search again in this last case because there may be another
deadlock!
LOCK_EXCEED_MAX_DEPTH if the lock search exceeds max steps or max depth. */
static ulint lock_deadlock_recursive(
  trx_t *start,      /*!< in: recursion starting point */
  trx_t *trx,        /*!< in: a transaction waiting for a lock */
  Lock *wait_lock, /*!< in:  lock that is waiting to be granted */
  ulint *cost,       /*!< in/out: number of calculation steps thus
                       far: if this exceeds LOCK_MAX_N_STEPS_...
                       we return LOCK_EXCEED_MAX_DEPTH */
  ulint depth
); /*!< in: recursion depth: if this exceeds
                       LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK, we
                       return LOCK_EXCEED_MAX_DEPTH */

void lock_var_init() {
#ifdef UNIV_DEBUG
  lock_print_waits = false;
#endif /* UNIV_DEBUG */
  lock_sys = nullptr;
  lock_deadlock_found = false;
  lock_latest_err_stream = nullptr;
}

/** Gets the nth bit of a record lock.
@return	true if bit set also if i == ULINT_UNDEFINED return false*/
inline bool lock_rec_get_nth_bit(const Lock *lock, ulint i) {
  ulint byte_index;
  ulint bit_index;

  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  if (i >= lock->un_member.rec_lock.n_bits) {

    return false;
  }

  byte_index = i / 8;
  bit_index = i % 8;

  return 1 & ((const byte *)&lock[1])[byte_index] >> bit_index;
}

#define lock_mutex_enter_kernel() mutex_enter(&kernel_mutex)
#define lock_mutex_exit_kernel() mutex_exit(&kernel_mutex)

bool lock_check_trx_id_sanity(trx_id_t trx_id, const rec_t *rec, dict_index_t *index, const ulint *offsets, bool has_kernel_mutex) {
  bool is_ok = true;

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (!has_kernel_mutex) {
    mutex_enter(&kernel_mutex);
  }

  /* A sanity check: the trx_id in rec must be smaller than the global
  trx id counter */

  if (trx_id >= trx_sys->max_trx_id) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: transaction id associated"
      " with record\n"
    );
    rec_print_new(ib_stream, rec, offsets);
    ib_logger(ib_stream, "in ");
    dict_index_name_print(ib_stream, nullptr, index);
    ib_logger(
      ib_stream,
      "\n"
      "is %lu which is higher than the"
      " global trx id counter %lu\n"
      "The table is corrupt. You have to do"
      " dump + drop + reimport.\n",
      TRX_ID_PREP_PRINTF(trx_id),
      TRX_ID_PREP_PRINTF(trx_sys->max_trx_id)
    );

    is_ok = false;
  }

  if (!has_kernel_mutex) {
    mutex_exit(&kernel_mutex);
  }

  return is_ok;
}

bool lock_clust_rec_cons_read_sees(const rec_t *rec, dict_index_t *index, const ulint *offsets, read_view_t *view) {
  trx_id_t trx_id;

  ut_ad(dict_index_is_clust(index));
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  /* NOTE that we call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  kernel mutex here! */

  trx_id = row_get_rec_trx_id(rec, index, offsets);

  return read_view_sees_trx_id(view, trx_id);
}

ulint lock_sec_rec_cons_read_sees(const rec_t *rec, const read_view_t *view) {
  trx_id_t max_trx_id;

  ut_ad(page_rec_is_user_rec(rec));

  /* NOTE that we might call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  kernel mutex here! */

  if (recv_recovery_on) {
    return false;
  }

  max_trx_id = page_get_max_trx_id(page_align(rec));
  ut_ad(max_trx_id > 0);

  return max_trx_id < view->up_limit_id;
}

void lock_sys_create(ulint n_cells) {
  lock_sys = static_cast<lock_sys_t *>(mem_alloc(sizeof(lock_sys_t)));

  lock_sys->rec_hash = hash_create(n_cells);

  /* hash_create_mutexes(lock_sys->rec_hash, 2, SYNC_REC_LOCK); */

  lock_latest_err_stream = os_file_create_tmpfile();
  ut_a(lock_latest_err_stream);
}

void lock_sys_close() {
  /* This can happen if we decide to abort during the startup phase. */
  if (lock_sys == nullptr) {
    return;
  }

  /* hash_free_mutexes(lock_sys->rec_hash); */
  hash_table_free(lock_sys->rec_hash);
  lock_sys->rec_hash = nullptr;

  if (lock_latest_err_stream != nullptr) {
    fclose(lock_latest_err_stream);
    lock_latest_err_stream = nullptr;
  }

  mem_free(lock_sys);
  lock_sys = nullptr;
}

ulint lock_get_size() {
  return (ulint)sizeof(Lock);
}

/** Gets the mode of a lock.
@return	mode */
inline Lock_mode lock_get_mode(const Lock *lock) {
  return static_cast<Lock_mode>(lock->type_mode & LOCK_MODE_MASK);
}

/** Gets the wait flag of a lock.
@return	true if waiting */
inline bool lock_get_wait(const Lock *lock) {
  ut_ad(lock);

  if (unlikely(lock->type_mode & LOCK_WAIT)) {

    return true;
  }

  return false;
}

dict_table_t *lock_get_src_table(trx_t *trx, dict_table_t *dest, enum Lock_mode *mode) {
  Lock *lock;
  dict_table_t *src;

  src = nullptr;
  *mode = LOCK_NONE;

  for (lock = UT_LIST_GET_FIRST(trx->trx_locks); lock; lock = UT_LIST_GET_NEXT(trx_locks, lock)) {
    Table_lock *tab_lock;
    enum Lock_mode Lock_mode;
    if (!(lock_get_type_low(lock) & LOCK_TABLE)) {
      /* We are only interested in table locks. */
      continue;
    }
    tab_lock = &lock->un_member.tab_lock;
    if (dest == tab_lock->table) {
      /* We are not interested in the destination table. */
      continue;
    } else if (!src) {
      /* This presumably is the source table. */
      src = tab_lock->table;
      if (UT_LIST_GET_LEN(src->locks) != 1 || UT_LIST_GET_FIRST(src->locks) != lock) {
        /* We only support the case when
        there is only one lock on this table. */
        return nullptr;
      }
    } else if (src != tab_lock->table) {
      /* The transaction is locking more than
      two tables (src and dest): abort */
      return nullptr;
    }

    /* Check that the source table is locked by LOCK_IX or LOCK_IS. */
    Lock_mode = lock_get_mode(lock);
    if (Lock_mode == LOCK_IX || Lock_mode == LOCK_IS) {
      if (*mode != LOCK_NONE && *mode != Lock_mode) {
        /* There are multiple locks on src. */
        return nullptr;
      }
      *mode = Lock_mode;
    }
  }

  if (!src) {
    /* No source table lock found: flag the situation to caller */
    src = dest;
  }

  return src;
}

bool lock_is_table_exclusive(dict_table_t *table, trx_t *trx) {
  const Lock *lock;
  bool ok = false;

  ut_ad(table);
  ut_ad(trx);

  lock_mutex_enter_kernel();

  for (lock = UT_LIST_GET_FIRST(table->locks); lock; lock = UT_LIST_GET_NEXT(locks, &lock->un_member.tab_lock)) {
    if (lock->trx != trx) {
      /* A lock on the table is held
      by some other transaction. */
      goto not_ok;
    }

    if (!(lock_get_type_low(lock) & LOCK_TABLE)) {
      /* We are interested in table locks only. */
      continue;
    }

    switch (lock_get_mode(lock)) {
      case LOCK_IX:
        ok = true;
        break;
      case LOCK_AUTO_INC:
        /* It is allowed for trx to hold an
      auto_increment lock. */
        break;
      default:
      not_ok:
        /* Other table locks than LOCK_IX are not allowed. */
        ok = false;
        goto func_exit;
    }
  }

func_exit:
  lock_mutex_exit_kernel();

  return ok;
}

/** Sets the wait flag of a lock and the back pointer in trx to lock. */
inline void lock_set_lock_and_trx_wait(Lock *lock, trx_t *trx) {
  ut_ad(lock);
  ut_ad(trx->wait_lock == nullptr);

  trx->wait_lock = lock;
  lock->type_mode |= LOCK_WAIT;
}

/** The back pointer to a waiting lock request in the transaction is set to
nullptr and the wait bit in lock type_mode is reset. */
inline void lock_reset_lock_and_trx_wait(Lock *lock) {
  ut_ad((lock->trx)->wait_lock == lock);
  ut_ad(lock_get_wait(lock));

  /* Reset the back pointer in trx to this waiting lock request */

  lock->trx->wait_lock = nullptr;
  lock->type_mode &= ~LOCK_WAIT;
}

/** Gets the gap flag of a record lock.
@return	true if gap flag set */
inline bool lock_rec_get_gap(const Lock *lock) {
  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  return (lock->type_mode & LOCK_GAP) > 0;
}

/** Gets the LOCK_REC_NOT_GAP flag of a record lock.
@return	true if LOCK_REC_NOT_GAP flag set */
inline bool lock_rec_get_rec_not_gap(const Lock *lock) {
  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  return (lock->type_mode & LOCK_REC_NOT_GAP) != 0;
}

/** Gets the waiting insert flag of a record lock.
@return	true if gap flag set */
inline bool lock_rec_get_insert_intention(const Lock *lock) {
  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  return (lock->type_mode & LOCK_INSERT_INTENTION) != 0;
}

/** Calculates if lock mode 1 is stronger or equal to lock mode 2.
@return	nonzero if mode1 stronger or equal to mode2 */
inline ulint lock_mode_stronger_or_eq(Lock_mode mode1, Lock_mode mode2) {
  ut_ad(mode1 == LOCK_X || mode1 == LOCK_S || mode1 == LOCK_IX || mode1 == LOCK_IS || mode1 == LOCK_AUTO_INC);
  ut_ad(mode2 == LOCK_X || mode2 == LOCK_S || mode2 == LOCK_IX || mode2 == LOCK_IS || mode2 == LOCK_AUTO_INC);

  return (LOCK_MODE_STRONGER_OR_EQ)&LK(mode1, mode2);
}

/** Calculates if lock mode 1 is compatible with lock mode 2.
@return	nonzero if mode1 compatible with mode2 */
inline ulint lock_mode_compatible(Lock_mode mode1, Lock_mode mode2) {
  ut_ad(mode1 == LOCK_X || mode1 == LOCK_S || mode1 == LOCK_IX || mode1 == LOCK_IS || mode1 == LOCK_AUTO_INC);
  ut_ad(mode2 == LOCK_X || mode2 == LOCK_S || mode2 == LOCK_IX || mode2 == LOCK_IS || mode2 == LOCK_AUTO_INC);

  return (LOCK_MODE_COMPATIBILITY)&LK(mode1, mode2);
}

/** Checks if a lock request for a new lock has to wait for request lock2.
@param[in] trx                  trx of new lock
@param[in] type_mode            Precise mode of the new lock to set: LOCK_S or LOCK_X,
                                possibly ORed to LOCK_GAP or LOCK_REC_NOT_GAP, LOCK_INSERT_INTENTION
@param[in] lock2                Another record lock; NOTE that it is assumed that this has a lock bit
                                set on the same record as in the new lock we are setting.
@param[in] lock_is_on_supremum  true if we are setting the lock on the 'supremum' record of an index
                                page: we know then that the lock request is really for a 'gap' type lock
@return	true if new lock has to wait for lock2 to be removed */
inline bool lock_rec_has_to_wait(const trx_t *trx, ulint type_mode, const Lock *lock2, bool lock_is_on_supremum) {
  ut_ad(trx && lock2);
  ut_ad(lock_get_type_low(lock2) == LOCK_REC);

  if (trx != lock2->trx && !lock_mode_compatible(Lock_mode(LOCK_MODE_MASK & type_mode), lock_get_mode(lock2))) {

    /* We have somewhat complex rules when gap type record locks
    cause waits */

    if ((lock_is_on_supremum || (type_mode & LOCK_GAP)) && !(type_mode & LOCK_INSERT_INTENTION)) {

      /* Gap type locks without LOCK_INSERT_INTENTION flag
      do not need to wait for anything. This is because
      different users can have conflicting lock types
      on gaps. */

      return false;
    }

    if (!(type_mode & LOCK_INSERT_INTENTION) && lock_rec_get_gap(lock2)) {

      /* Record lock (LOCK_ORDINARY or LOCK_REC_NOT_GAP
      does not need to wait for a gap type lock */

      return false;
    }

    if ((type_mode & LOCK_GAP) && lock_rec_get_rec_not_gap(lock2)) {

      /* Lock on gap does not need to wait for
      a LOCK_REC_NOT_GAP type lock */

      return false;
    }

    if (lock_rec_get_insert_intention(lock2)) {

      /* No lock request needs to wait for an insert
      intention lock to be removed. This is ok since our
      rules allow conflicting locks on gaps. This eliminates
      a spurious deadlock caused by a next-key lock waiting
      for an insert intention lock; when the insert
      intention lock was granted, the insert deadlocked on
      the waiting next-key lock.

      Also, insert intention locks do not disturb each
      other. */

      return false;
    }

    return true;
  }

  return false;
}

bool lock_has_to_wait(const Lock *lock1, const Lock *lock2) {
  ut_ad(lock1 && lock2);

  if (lock1->trx != lock2->trx && !lock_mode_compatible(lock_get_mode(lock1), lock_get_mode(lock2))) {
    if (lock_get_type_low(lock1) == LOCK_REC) {
      ut_ad(lock_get_type_low(lock2) == LOCK_REC);

      /* If this lock request is for a supremum record
      then the second bit on the lock bitmap is set */

      return (lock_rec_has_to_wait(lock1->trx, lock1->type_mode, lock2, lock_rec_get_nth_bit(lock1, 1)));
    }

    return true;
  }

  return false;
}

/** Gets the number of bits in a record lock bitmap.
@return	number of bits */
inline ulint lock_rec_get_n_bits(const Lock *lock) {
  return lock->un_member.rec_lock.n_bits;
}

/** Sets the nth bit of a record lock to true. */
inline void lock_rec_set_nth_bit(Lock *lock, ulint i) {
  ulint byte_index;
  ulint bit_index;

  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);
  ut_ad(i < lock->un_member.rec_lock.n_bits);

  byte_index = i / 8;
  bit_index = i % 8;

  ((byte *)&lock[1])[byte_index] |= 1 << bit_index;
}

ulint lock_rec_find_set_bit(const Lock *lock) {
  for (ulint i = 0; i < lock_rec_get_n_bits(lock); i++) {

    if (lock_rec_get_nth_bit(lock, i)) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/** Resets the nth bit of a record lock. */
inline void lock_rec_reset_nth_bit(Lock *lock, ulint i) {
  ulint byte_index;
  ulint bit_index;

  ut_ad(lock);
  ut_ad(lock_get_type_low(lock) == LOCK_REC);
  ut_ad(i < lock->un_member.rec_lock.n_bits);

  byte_index = i / 8;
  bit_index = i % 8;

  ((byte *)&lock[1])[byte_index] &= ~(1 << bit_index);
}

/** Gets the first or next record lock on a page.
@return	next lock, nullptr if none exists */
inline Lock *lock_rec_get_next_on_page(Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  auto space = lock->un_member.rec_lock.space;
  auto page_no = lock->un_member.rec_lock.page_no;

  for (;;) {
    lock = static_cast<Lock *>(HASH_GET_NEXT(hash, lock));

    if (!lock) {

      break;
    }

    if ((lock->un_member.rec_lock.space == space) && (lock->un_member.rec_lock.page_no == page_no)) {

      break;
    }
  }

  return lock;
}

/** Gets the first record lock on a page, where the page is identified by its
file address.
@return	first lock, nullptr if none exists */
inline Lock *lock_rec_get_first_on_page_addr(space_id_t space, page_no_t page_no) {
  ut_ad(mutex_own(&kernel_mutex));

  for (auto lock = static_cast<Lock *>(HASH_GET_FIRST(lock_sys->rec_hash, lock_rec_hash(space, page_no)));
       lock != nullptr;
       lock = static_cast<Lock *>(HASH_GET_NEXT(hash, lock))) {

    if ((lock->un_member.rec_lock.space == space) && (lock->un_member.rec_lock.page_no == page_no)) {
      return lock;
    }
  }

  return nullptr;
}

bool lock_rec_expl_exist_on_page(space_id_t space, page_no_t page_no) {
  mutex_enter(&kernel_mutex);

  auto ret = lock_rec_get_first_on_page_addr(space, page_no) != 0;

  mutex_exit(&kernel_mutex);

  return ret;
}

/** Gets the first record lock on a page, where the page is identified by a
pointer to it.
@return	first lock, nullptr if none exists */
inline Lock *lock_rec_get_first_on_page(const buf_block_t *block) {
  ut_ad(mutex_own(&kernel_mutex));

  const auto space = block->get_space();
  const auto page_no = block->get_page_no();
  auto hash = buf_block_get_lock_hash_val(block);

  for (auto lock = static_cast<Lock *>(HASH_GET_FIRST(lock_sys->rec_hash, hash));
       lock != nullptr;
       lock = static_cast<Lock *>(HASH_GET_NEXT(hash, lock))) {

    if (lock->un_member.rec_lock.space == space && lock->un_member.rec_lock.page_no == page_no) {
      return lock;
    }
  }

  return nullptr;
}

/** Gets the next explicit lock request on a record.
@return	next lock, nullptr if none exists or if heap_no == ULINT_UNDEFINED */
inline Lock *lock_rec_get_next(ulint heap_no, Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));

  do {
    ut_ad(lock_get_type_low(lock) == LOCK_REC);
    lock = lock_rec_get_next_on_page(lock);
  } while (lock != nullptr && !lock_rec_get_nth_bit(lock, heap_no));

  return lock;
}

/** Gets the first explicit lock request on a record.
@return	first lock, nullptr if none exists */
inline Lock *lock_rec_get_first(const buf_block_t *block, ulint heap_no) {
  ut_ad(mutex_own(&kernel_mutex));

  for (auto lock = lock_rec_get_first_on_page(block);
       lock != nullptr;
       lock = lock_rec_get_next_on_page(lock)) {

    if (lock_rec_get_nth_bit(lock, heap_no)) {
      return lock;
    }
  }

  return nullptr;
}

/** Resets the record lock bitmap to zero. NOTE: does not touch the wait_lock
pointer in the transaction! This function is used in lock object creation
and resetting. */
static void lock_rec_bitmap_reset(Lock *lock) {
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  /* Reset to zero the bitmap which resides immediately after the lock
  struct */

  auto n_bytes = lock_rec_get_n_bits(lock) / 8;

  ut_ad((lock_rec_get_n_bits(lock) % 8) == 0);

  memset(&lock[1], 0, n_bytes);
}

/** Copies a record lock to heap.
@return	copy of lock */
static Lock *lock_rec_copy(
  const Lock *lock, /*!< in: record lock */
  mem_heap_t *heap
) /*!< in: memory heap */
{
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  auto size = sizeof(Lock) + lock_rec_get_n_bits(lock) / 8;

  return static_cast<Lock *>(mem_heap_dup(heap, lock, size));
}

const Lock *lock_rec_get_prev(const Lock *in_lock, ulint heap_no) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_type_low(in_lock) == LOCK_REC);

  Lock *found_lock{};
  auto space = in_lock->un_member.rec_lock.space;
  auto page_no = in_lock->un_member.rec_lock.page_no;
  auto lock = lock_rec_get_first_on_page_addr(space, page_no);

  for (;;) {
    ut_ad(lock);

    if (lock == in_lock) {

      return found_lock;
    }

    if (lock_rec_get_nth_bit(lock, heap_no)) {

      found_lock = lock;
    }

    lock = lock_rec_get_next_on_page(lock);
  }
}

/** Checks if a transaction has the specified table lock, or stronger.
@return	lock or nullptr */
inline Lock *lock_table_has(trx_t *trx, dict_table_t *table, Lock_mode mode) {
  ut_ad(mutex_own(&kernel_mutex));

  /* Look for stronger locks the same trx already has on the table */

  for (auto lock = UT_LIST_GET_LAST(table->locks);
       lock != nullptr;
       lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock)) {

    if (lock->trx == trx && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)) {

      /* The same trx already has locked the table in
      a mode stronger or equal to the mode given */

      ut_ad(!lock_get_wait(lock));

      return lock;
    }
  }

  return nullptr;
}

/** Checks if a transaction has a GRANTED explicit lock on rec stronger or equal
to precise_mode.
@param[in] precise_mode         LOCK_S or LOCK_X possibly ORed to LOCK_GAP or LOCK_REC_NOT_GAP,
                                for a supremum record we regard this always a gap type request
@param[in] block                Buffer block containing the record
@param[in] heap_no,             Heap number of the record
@param[in] trx                  Transaction
@return	lock or nullptr */
inline Lock *lock_rec_has_expl(ulint precise_mode, const buf_block_t *block, ulint heap_no, trx_t *trx) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((precise_mode & LOCK_MODE_MASK) == LOCK_S || (precise_mode & LOCK_MODE_MASK) == LOCK_X);
  ut_ad(!(precise_mode & LOCK_INSERT_INTENTION));

  for (auto lock = lock_rec_get_first(block, heap_no);
       lock != nullptr;
       lock = lock_rec_get_next(heap_no, lock)) {

    if (lock->trx == trx &&
        lock_mode_stronger_or_eq(lock_get_mode(lock), Lock_mode(precise_mode & LOCK_MODE_MASK)) &&
        !lock_get_wait(lock) &&
        (!lock_rec_get_rec_not_gap(lock) || (precise_mode & LOCK_REC_NOT_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
        (!lock_rec_get_gap(lock) || (precise_mode & LOCK_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
        (!lock_rec_get_insert_intention(lock))) {

      return lock;
    }
  }

  return nullptr;
}

#ifdef UNIV_DEBUG
/** Checks if some other transaction has a lock request in the queue.
@param[in] mode                 LOCK_S or LOCK_X
@param[in] gap                  LOCK_GAP if also gap locks are taken into account, or 0 if not
@param[in] wait                 LOCK_WAIT if also waiting locks are taken into account, or 0 if not
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of the record
@param[in] trx                  Transaction, or nullptr if requests by all transactions are taken into account
@return	lock or nullptr */
static Lock *lock_rec_other_has_expl_req(
  Lock_mode mode,
  ulint gap,
  ulint wait,
  const buf_block_t *block,
  ulint heap_no,
  const trx_t *trx) {

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(mode == LOCK_X || mode == LOCK_S);
  ut_ad(gap == 0 || gap == LOCK_GAP);
  ut_ad(wait == 0 || wait == LOCK_WAIT);

  auto lock = lock_rec_get_first(block, heap_no);

  while (lock != nullptr) {

    if (lock->trx != trx &&
        (gap || !(lock_rec_get_gap(lock) || heap_no == PAGE_HEAP_NO_SUPREMUM)) &&
	(wait || !lock_get_wait(lock)) &&
	lock_mode_stronger_or_eq(lock_get_mode(lock), mode)) {

      return lock;
    }

    lock = lock_rec_get_next(heap_no, lock);
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

/** Checks if some other transaction has a conflicting explicit lock request
in the queue, so that we have to wait.
@param[in] mode                 LOCK_S or LOCK_X, possibly ORed to LOCK_GAP or LOC_REC_NOT_GAP, LOCK_INSERT_INTENTION
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of the record
@param[in] trx                  Our transaction
@return	lock or nullptr */
static Lock *lock_rec_other_has_conflicting(Lock_mode mode, const buf_block_t *block, ulint heap_no, trx_t *trx) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = lock_rec_get_first(block, heap_no);

  if (lock != nullptr) {

    if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {

      do {

        if (lock_rec_has_to_wait(trx, mode, lock, true)) {
          return lock;
        }

        lock = lock_rec_get_next(heap_no, lock);

      } while (lock != nullptr);

    } else {

      do {

        if (lock_rec_has_to_wait(trx, mode, lock, false)) {
          return lock;
        }

        lock = lock_rec_get_next(heap_no, lock);

      } while (lock != nullptr);

    }
  }

  return nullptr;
}

/** Looks for a suitable type record lock struct by the same trx on the same
page. This can be used to save space when a new record lock should be set on a
page: no new struct is needed, if a suitable old is found.
@param[in] type_mode            Lock type_mode field
@param[in] heap_no              Gap number of the record
@param[in] lock                 lock_rec_get_first_on_page()
@param[in] trx                  Transaction
@return	lock or nullptr */
inline Lock *lock_rec_find_similar_on_page(ulint type_mode, ulint heap_no, Lock *lock, const trx_t *trx) {
  ut_ad(mutex_own(&kernel_mutex));

  while (lock != nullptr) {
    if (lock->trx == trx && lock->type_mode == type_mode && lock_rec_get_n_bits(lock) > heap_no) {

      return lock;
    }

    lock = lock_rec_get_next_on_page(lock);
  }

  return nullptr;
}

/** Checks if some transaction has an implicit x-lock on a record in a secondary index.
@param[in] rec                  User record
@param[in] index                Secondary index
@param[in] offsets              rec_get_offsets(rec, index)
@return	transaction which has the x-lock, or nullptr */
static trx_t *lock_sec_rec_some_has_impl_off_kernel(const rec_t *rec, dict_index_t *index, const ulint *offsets) {
  const page_t *page = page_align(rec);

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(!dict_index_is_clust(index));
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list, or
  database recovery is running. We do not write the changes of a page
  max trx id to the log, and therefore during recovery, this value
  for a page may be incorrect. */

  if (page_get_max_trx_id(page) < trx_list_get_min_trx_id() && !recv_recovery_on) {

    return nullptr;
  }

  /* Ok, in this case it is possible that some transaction has an
  implicit x-lock. We have to look in the clustered index. */

  if (!lock_check_trx_id_sanity(page_get_max_trx_id(page), rec, index, offsets, true)) {
    buf_page_print(page, 0);

    /* The page is corrupt: try to avoid a crash by returning
    nullptr */
    return nullptr;
  }

  return row_vers_impl_x_locked_off_kernel(rec, index, offsets);
}

ulint lock_number_of_rows_locked(trx_t *trx) {
  ulint n_records{};
  auto lock = UT_LIST_GET_FIRST(trx->trx_locks);

  while (lock != nullptr) {
    if (lock_get_type_low(lock) == LOCK_REC) {
      auto n_bits = lock_rec_get_n_bits(lock);

      for (ulint n_bit = 0; n_bit < n_bits; n_bit++) {
        if (lock_rec_get_nth_bit(lock, n_bit)) {
          n_records++;
        }
      }
    }

    lock = UT_LIST_GET_NEXT(trx_locks, lock);
  }

  return n_records;
}

#ifndef UNIT_TESTING
static
#endif /* !UNIT_TESTING */
  Lock *
  lock_rec_create_low(
    ulint type_mode, space_id_t space, page_no_t page_no, ulint heap_no, ulint n_bits, dict_index_t *index, trx_t *trx
  ) {
  ut_ad(mutex_own(&kernel_mutex));

  /* If rec is the supremum record, then we reset the gap and
  LOCK_REC_NOT_GAP bits, as all locks on the supremum are
  automatically of the gap type */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(type_mode & LOCK_REC_NOT_GAP));

    type_mode = type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP);
  }

  /* Make lock bitmap bigger by a safety margin */
  auto n_bytes = 1 + (n_bits + LOCK_PAGE_BITMAP_MARGIN) / 8;

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->lock_heap, sizeof(Lock) + n_bytes));

  UT_LIST_ADD_LAST(trx->trx_locks, lock);

  lock->trx = trx;

  lock->type_mode = (type_mode & ~LOCK_TYPE_MASK) | LOCK_REC;
  lock->index = index;

  lock->un_member.rec_lock.space = space;
  lock->un_member.rec_lock.page_no = page_no;
  lock->un_member.rec_lock.n_bits = n_bytes * 8;

  /* Reset to zero the bitmap which resides immediately after the
  lock struct */

  lock_rec_bitmap_reset(lock);

  /* Set the bit corresponding to rec */
  lock_rec_set_nth_bit(lock, heap_no);

  HASH_INSERT(Lock, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock_set_lock_and_trx_wait(lock, trx);
  }

  return lock;
}

/** Creates a new record lock and inserts it to the lock queue.
ulint type_mode                 Lock mode and wait flag, type is ignored and replaced by LOCK_REC
@param[in] block                Buffer block containing the record 
@param[in] heap_no              Heap number of the record
@param[in] index                Index of record
@param[in] trx                  Transaction
@return	created lock */
static Lock *lock_rec_create(
  ulint type_mode,
  const buf_block_t *block,
  ulint heap_no,
  dict_index_t *index,
  trx_t *trx) {
  const page_t *page;
  ulint space;
  ulint n_bits;
  ulint page_no;

  ut_ad(mutex_own(&kernel_mutex));

  space = block->get_space();
  page_no = block->get_page_no();
  page = block->m_frame;

  ut_ad(!!page_is_comp(page) == dict_table_is_comp(index->table));

  n_bits = page_dir_get_n_heap(page);

  return (lock_rec_create_low(type_mode, space, page_no, heap_no, n_bits, index, trx));
}

/** Enqueues a waiting request for a lock which cannot be granted immediately.
Checks for deadlocks.
@param[in] type_mode,           Lock mode this transaction is requesting: LOCK_S or LOCK_X,
                                possibly ORed with LOCK_GAP or LOCK_REC_NOT_GAP, ORed with LOCK_INSERT_INTENTION
                                if this waiting lock request is set when performing an insert of an index record
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of the record
@param[in] index                Index of record
@param[in] thr                  Query thread 
@return DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED, or
DB_SUCCESS; DB_SUCCESS means that there was a deadlock, but another
transaction was chosen as a victim, and we got the lock immediately:
no need to wait then */
static db_err lock_rec_enqueue_waiting(ulint type_mode, const buf_block_t *block, ulint heap_no, dict_index_t *index, que_thr_t *thr) {
  ut_ad(mutex_own(&kernel_mutex));

  /* Test if there already is some other reason to suspend thread:
  we do not enqueue a lock request if the query thread should be
  stopped anyway */

  if (unlikely(que_thr_stop(thr))) {

    ut_error;

    return DB_QUE_THR_SUSPENDED;
  }

  auto trx = thr_get_trx(thr);

  switch (trx_get_dict_operation(trx)) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: a record lock wait happens"
        " in a dictionary operation!\n"
        ""
      );
      dict_index_name_print(ib_stream, trx, index);
      ib_logger(
        ib_stream,
        ".\n"
        "Submit a detailed bug report "
        "check the InnoDB website for details"
      );
  }

  /* Enqueue the lock request that will wait to be granted */
  auto lock = lock_rec_create(type_mode | LOCK_WAIT, block, heap_no, index, trx);

  /* Check if a deadlock occurs: if yes, remove the lock request and
  return an error code */

  if (unlikely(lock_deadlock_occurs(lock, trx))) {

    lock_reset_lock_and_trx_wait(lock);
    lock_rec_reset_nth_bit(lock, heap_no);

    return DB_DEADLOCK;
  }

  /* If there was a deadlock but we chose another transaction as a
  victim, it is possible that we already have the lock now granted! */

  if (trx->wait_lock == nullptr) {

    return DB_SUCCESS;
  }

  trx->m_que_state = TRX_QUE_LOCK_WAIT;
  trx->was_chosen_as_deadlock_victim = false;
  trx->wait_started = time(nullptr);

  ut_a(que_thr_stop(thr));

#ifdef UNIV_DEBUG
  if (lock_print_waits) {
    ib_logger(ib_stream, "Lock wait for trx %lu in index ", (ulong)trx->m_id);
    ut_print_name(ib_stream, trx, false, index->name);
  }
#endif /* UNIV_DEBUG */

  return DB_LOCK_WAIT;
}

/** Adds a record lock request in the record queue. The request is normally
added as the last in the queue, but if there are no waiting lock requests
on the record, and the request to be added is not a waiting request, we
can reuse a suitable record lock object already existing on the same page,
just setting the appropriate bit in its bitmap. This is a low-level function
which does NOT check for deadlocks or lock compatibility!
@param[in] type_mode            Lock mode, wait, gap etc. flags; type is ignored and replaced by LOCK_REC 
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of the record 
@param[in] index                Index of record
@param[in] trx                  Ttransaction
@return	lock where the bit was set */
static Lock *lock_rec_add_to_queue(ulint type_mode, const buf_block_t *block, ulint heap_no, dict_index_t *index, trx_t *trx) {
  Lock *lock;

  ut_ad(mutex_own(&kernel_mutex));
#ifdef UNIV_DEBUG
  switch (type_mode & LOCK_MODE_MASK) {
    case LOCK_X:
    case LOCK_S:
      break;
    default:
      ut_error;
  }

  if (!(type_mode & (LOCK_WAIT | LOCK_GAP))) {
    enum Lock_mode mode = (type_mode & LOCK_MODE_MASK) == LOCK_S ? LOCK_X : LOCK_S;
    Lock *other_lock = lock_rec_other_has_expl_req(mode, 0, LOCK_WAIT, block, heap_no, trx);
    ut_a(!other_lock);
  }
#endif /* UNIV_DEBUG */

  type_mode |= LOCK_REC;

  /* If rec is the supremum record, then we can reset the gap bit, as
  all locks on the supremum are automatically of the gap type, and we
  try to avoid unnecessary memory consumption of a new record lock
  struct for a gap type lock */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(type_mode & LOCK_REC_NOT_GAP));

    /* There should never be LOCK_REC_NOT_GAP on a supremum
    record, but let us play safe */

    type_mode = type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP);
  }

  /* Look for a waiting lock request on the same record or on a gap */

  lock = lock_rec_get_first_on_page(block);

  while (lock != nullptr) {
    if (lock_get_wait(lock) && (lock_rec_get_nth_bit(lock, heap_no))) {

      goto somebody_waits;
    }

    lock = lock_rec_get_next_on_page(lock);
  }

  if (likely(!(type_mode & LOCK_WAIT))) {

    /* Look for a similar record lock on the same page:
    if one is found and there are no waiting lock requests,
    we can just set the bit */

    lock = lock_rec_find_similar_on_page(type_mode, heap_no, lock_rec_get_first_on_page(block), trx);

    if (lock) {

      lock_rec_set_nth_bit(lock, heap_no);

      return lock;
    }
  }

somebody_waits:
  return lock_rec_create(type_mode, block, heap_no, index, trx);
}

/** This is a fast routine for locking a record in the most common cases:
there are no explicit locks on the page, or there is just one lock, owned
by this transaction, and of the right type_mode. This is a low-level function
which does NOT look at implicit locks! Checks lock compatibility within
explicit locks. This function sets a normal next-key lock, or in the case of
a page supremum record, a gap type lock.
@param[in] impl                 if true, no lock is set if no wait is necessary:
                                we assume that the caller will set an implicit lock
@param[in] mode                 Lock mode: LOCK_X or LOCK_S possibly ORed to either
                                LOCK_GAP or LOCK_REC_NOT_GAP
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of record
@param[in] index                Index of record
@param[in] thr                  Query thread
@return	true if locking succeeded */
inline bool lock_rec_lock_fast(bool impl, ulint mode, const buf_block_t *block, ulint heap_no, dict_index_t *index, que_thr_t *thr) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  auto lock = lock_rec_get_first_on_page(block);
  auto trx = thr_get_trx(thr);

  if (lock == nullptr) {
    if (!impl) {
      lock_rec_create(mode, block, heap_no, index, trx);
    }

    return true;
  }

  if (lock_rec_get_next_on_page(lock)) {

    return false;
  }

  if (lock->trx != trx || lock->type_mode != (mode | LOCK_REC) || lock_rec_get_n_bits(lock) <= heap_no) {

    return false;
  }

  if (!impl) {
    /* If the nth bit of the record lock is already set then we
    do not set a new lock bit, otherwise we do set */

    if (!lock_rec_get_nth_bit(lock, heap_no)) {
      lock_rec_set_nth_bit(lock, heap_no);
    }
  }

  return true;
}

/** This is the general, and slower, routine for locking a record. This is a
low-level function which does NOT look at implicit locks! Checks lock
compatibility within explicit locks. This function sets a normal next-key
lock, or in the case of a page supremum record, a gap type lock.
@param[in] impl                 if true, no lock is set if no wait is necessary:
                                we assume that the caller will set an implicit lock
@param[in] mode                 Lock mode: LOCK_X or LOCK_S possibly ORed to either
                                LOCK_GAP or LOCK_REC_NOT_GAP
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of record
@param[in] index                Index of record
@param[in] thr                  Query thread
@return	DB_SUCCESS, DB_LOCK_WAIT, or error code */
static db_err lock_rec_lock_slow(bool impl, ulint mode, const buf_block_t *block, ulint heap_no, dict_index_t *index, que_thr_t *thr) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  db_err err;
  auto trx = thr_get_trx(thr);

  if (lock_rec_has_expl(mode, block, heap_no, trx)) {
    /* The trx already has a strong enough lock on rec: do
    nothing */

    err = DB_SUCCESS;
  } else if (lock_rec_other_has_conflicting(Lock_mode(mode), block, heap_no, trx)) {

    /* If another transaction has a non-gap conflicting request in
    the queue, as this transaction does not have a lock strong
    enough already granted on the record, we have to wait. */

    err = lock_rec_enqueue_waiting(mode, block, heap_no, index, thr);
  } else {
    if (!impl) {
      /* Set the requested lock on the record */

      lock_rec_add_to_queue(LOCK_REC | mode, block, heap_no, index, trx);
    }

    err = DB_SUCCESS;
  }

  return err;
}

/** Tries to lock the specified record in the mode requested. If not immediately
possible, enqueues a waiting lock request. This is a low-level function
which does NOT look at implicit locks! Checks lock compatibility within
explicit locks. This function sets a normal next-key lock, or in the case
of a page supremum record, a gap type lock.
@param[in] impl                 if true, no lock is set if no wait is necessary:
                                we assume that the caller will set an implicit lock
@param[in] mode                 Lock mode: LOCK_X or LOCK_S possibly ORed to either
                                LOCK_GAP or LOCK_REC_NOT_GAP
@param[in] block                Buffer block containing the record
@param[in] heap_no              Heap number of record
@param[in] index                Index of record
@param[in] thr                  Query thread
@return	DB_SUCCESS, DB_LOCK_WAIT, or error code */
static db_err lock_rec_lock(bool impl, ulint mode, const buf_block_t *block, ulint heap_no, dict_index_t *index, que_thr_t *thr) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);

  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP ||
    mode - (LOCK_MODE_MASK & mode) == 0
  );

  db_err err;
  if (lock_rec_lock_fast(impl, mode, block, heap_no, index, thr)) {

    /* We try a simplified and faster subroutine for the most
    common cases */

    err = DB_SUCCESS;
  } else {
    err = lock_rec_lock_slow(impl, mode, block, heap_no, index, thr);
  }

  return err;
}

/** Checks if a waiting record lock request still has to wait in a queue.
@return	true if still has to wait */
static bool lock_rec_has_to_wait_in_queue(Lock *wait_lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_wait(wait_lock));
  ut_ad(lock_get_type_low(wait_lock) == LOCK_REC);

  auto space = wait_lock->un_member.rec_lock.space;
  auto page_no = wait_lock->un_member.rec_lock.page_no;
  auto heap_no = lock_rec_find_set_bit(wait_lock);

  auto lock = lock_rec_get_first_on_page_addr(space, page_no);

  while (lock != wait_lock) {

    if (lock_rec_get_nth_bit(lock, heap_no) && lock_has_to_wait(wait_lock, lock)) {

      return true;
    }

    lock = lock_rec_get_next_on_page(lock);
  }

  return false;
}

/** Grants a lock to a waiting lock request and releases the waiting
transaction. */
static void lock_grant(Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));

  lock_reset_lock_and_trx_wait(lock);

#ifdef UNIV_DEBUG
  if (lock_print_waits) {
    ib_logger(ib_stream, "Lock wait for trx %lu ends\n", (ulong)lock->trx->m_id);
  }
#endif /* UNIV_DEBUG */

  /* If we are resolving a deadlock by choosing another transaction
  as a victim, then our original transaction may not be in the
  TRX_QUE_LOCK_WAIT state, and there is no need to end the lock wait
  for it */

  if (lock->trx->m_que_state == TRX_QUE_LOCK_WAIT) {
    trx_end_lock_wait(lock->trx);
  }
}

/** Cancels a waiting record lock request and releases the waiting transaction
that requested it. NOTE: does NOT check if waiting lock requests behind this
one can now be granted! */
static void lock_rec_cancel(Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_type_low(lock) == LOCK_REC);

  /* Reset the bit (there can be only one set bit) in the lock bitmap */
  lock_rec_reset_nth_bit(lock, lock_rec_find_set_bit(lock));

  /* Reset the wait flag and the back pointer to lock in trx */

  lock_reset_lock_and_trx_wait(lock);

  /* The following function releases the trx from lock wait */

  trx_end_lock_wait(lock->trx);
}

/** Removes a record lock request, waiting or granted, from the queue and
grants locks to other transactions in the queue if they now are entitled
to a lock. NOTE: all record locks contained in in_lock are removed.
@param[in] in_lock              record lock object: all record locks which are contained
                                in this lock object are removed; transactions waiting behind
                                will get their lock requests granted, if they are now qualified
                                to it */
static void lock_rec_dequeue_from_page(Lock *in_lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_type_low(in_lock) == LOCK_REC);

  auto trx = in_lock->trx;

  auto space = in_lock->un_member.rec_lock.space;
  auto page_no = in_lock->un_member.rec_lock.page_no;

  HASH_DELETE(Lock, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), in_lock);

  UT_LIST_REMOVE(trx->trx_locks, in_lock);

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  auto lock = lock_rec_get_first_on_page_addr(space, page_no);

  while (lock != nullptr) {
    if (lock_get_wait(lock) && !lock_rec_has_to_wait_in_queue(lock)) {

      /* Grant the lock */
      lock_grant(lock);
    }

    lock = lock_rec_get_next_on_page(lock);
  }
}

/** Removes a record lock request, waiting or granted, from the queue.
@param[in,out] in_lock          Record lock : all record locks which are contained in
                                this lock object are removed */
static void lock_rec_discard(Lock *in_lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock_get_type_low(in_lock) == LOCK_REC);

  auto trx = in_lock->trx;
  auto space = in_lock->un_member.rec_lock.space;
  auto page_no = in_lock->un_member.rec_lock.page_no;

  HASH_DELETE(Lock, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), in_lock);

  UT_LIST_REMOVE(trx->trx_locks, in_lock);
}

/** Removes record lock objects set on an index page which is discarded. This
function does not move locks, or check for waiting locks, therefore the
lock bitmaps must already be reset when this function is called.
@param[in] block                Page to be discarded. */
static void lock_rec_free_all_from_discard_page(const buf_block_t *block) {
  ut_ad(mutex_own(&kernel_mutex));

  auto space = block->get_space();
  auto page_no = block->get_page_no();
  auto lock = lock_rec_get_first_on_page_addr(space, page_no);

  while (lock != nullptr) {
    ut_ad(lock_rec_find_set_bit(lock) == ULINT_UNDEFINED);
    ut_ad(!lock_get_wait(lock));

    auto next_lock = lock_rec_get_next_on_page(lock);

    lock_rec_discard(lock);

    lock = next_lock;
  }
}

/** Resets the lock bits for a single record. Releases transactions waiting for
lock requests here.
@param[in] block                Block conaining the record.
@param[in] heap_no              Heap number of the record. */
static void lock_rec_reset_and_release_wait(const buf_block_t *block, ulint heap_no) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = lock_rec_get_first(block, heap_no);

  while (lock != nullptr) {
    if (lock_get_wait(lock)) {
      lock_rec_cancel(lock);
    } else {
      lock_rec_reset_nth_bit(lock, heap_no);
    }

    lock = lock_rec_get_next(heap_no, lock);
  }
}

/** Makes a record to inherit the locks (except LOCK_INSERT_INTENTION type)
of another record as gap type locks, but does not reset the lock bits of
the other record. Also waiting lock requests on rec are inherited as
GRANTED gap locks.
@param[in] heir_block           Block containing the record which inherits
@param[in] block                Block containing the record from which inherited;
                                does NOT reset the locks on this record
@param[in] heir_heap_no         Heap_no of the inheriting record
@param[in] heap_no              Heap_no of the donating record */
static void lock_rec_inherit_to_gap(const buf_block_t *heir_block, const buf_block_t *block, ulint heir_heap_no, ulint heap_no) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = lock_rec_get_first(block, heap_no);

  /* If session is using READ COMMITTED isolation level, we do not want
  locks set by an UPDATE or a DELETE to be inherited as gap type locks.
  But we DO want S-locks set by a consistency constraint to be inherited
  also then. */

  while (lock != nullptr) {
    if (!lock_rec_get_insert_intention(lock) && lock->trx->m_isolation_level != TRX_ISO_READ_COMMITTED && lock_get_mode(lock) == LOCK_X) {

      lock_rec_add_to_queue(LOCK_REC | LOCK_GAP | lock_get_mode(lock), heir_block, heir_heap_no, lock->index, lock->trx);
    }

    lock = lock_rec_get_next(heap_no, lock);
  }
}

/** Makes a record to inherit the gap locks (except LOCK_INSERT_INTENTION type)
of another record as gap type locks, but does not reset the lock bits of the
other record. Also waiting lock requests are inherited as GRANTED gap locks. 
@param[in] block                Buffer block 
@param[in] heir_heap_no         heap_no of record which inherits
@param[in] heap_no              heap_no of record from which inherited; does NOT
                                reset the locks on this record */
static void lock_rec_inherit_to_gap_if_gap_lock(const buf_block_t *block, ulint heir_heap_no, ulint heap_no) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = lock_rec_get_first(block, heap_no);

  while (lock != nullptr) {
    if (!lock_rec_get_insert_intention(lock) && (heap_no == PAGE_HEAP_NO_SUPREMUM || !lock_rec_get_rec_not_gap(lock))) {

      lock_rec_add_to_queue(LOCK_REC | LOCK_GAP | lock_get_mode(lock), block, heir_heap_no, lock->index, lock->trx);
    }

    lock = lock_rec_get_next(heap_no, lock);
  }
}

/** Moves the locks of a record to another record and resets the lock bits of
the donating record.
@param[in] receiver             Buffer block containing the receiving record
@param[in] donator              Buffer block containing the donating record
@param[in] receiver_heap_no     heap_no of the record which gets the locks;
                                there must be no lock requests on it!
@param[in] donator_heap_no      heap_no of the record which gives the locks */
static void lock_rec_move(const buf_block_t *receiver, const buf_block_t *donator, ulint receiver_heap_no, ulint donator_heap_no) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = lock_rec_get_first(donator, donator_heap_no);

  ut_ad(lock_rec_get_first(receiver, receiver_heap_no) == nullptr);

  while (lock != nullptr) {
    const ulint type_mode = lock->type_mode;

    lock_rec_reset_nth_bit(lock, donator_heap_no);

    if (unlikely(type_mode & LOCK_WAIT)) {
      lock_reset_lock_and_trx_wait(lock);
    }

    /* Note that we FIRST reset the bit, and then set the lock:
    the function works also if donator == receiver */

    lock_rec_add_to_queue(type_mode, receiver, receiver_heap_no, lock->index, lock->trx);
    lock = lock_rec_get_next(donator_heap_no, lock);
  }

  ut_ad(lock_rec_get_first(donator, donator_heap_no) == nullptr);
}

void lock_move_reorganize_page(const buf_block_t *block, const buf_block_t *oblock) {
  UT_LIST_BASE_NODE_T(Lock, trx_locks) old_locks;

  ulint comp;

  lock_mutex_enter_kernel();

  auto lock = lock_rec_get_first_on_page(block);

  if (lock == nullptr) {
    lock_mutex_exit_kernel();

    return;
  }

  auto heap = mem_heap_create(256);

  /* Copy first all the locks on the page to heap and reset the
  bitmaps in the original locks; chain the copies of the locks
  using the trx_locks field in them. */

  UT_LIST_INIT(old_locks);

  do {
    /* Make a copy of the lock */
    Lock *old_lock = lock_rec_copy(lock, heap);

    UT_LIST_ADD_LAST(old_locks, old_lock);

    /* Reset bitmap of lock */
    lock_rec_bitmap_reset(lock);

    if (lock_get_wait(lock)) {
      lock_reset_lock_and_trx_wait(lock);
    }

    lock = lock_rec_get_next_on_page(lock);
  } while (lock != nullptr);

  comp = page_is_comp(block->m_frame);
  ut_ad(comp == page_is_comp(oblock->m_frame));

  for (lock = UT_LIST_GET_FIRST(old_locks); lock; lock = UT_LIST_GET_NEXT(trx_locks, lock)) {
    /* NOTE: we copy also the locks set on the infimum and
    supremum of the page; the infimum may carry locks if an
    update of a record is occurring on the page, and its locks
    were temporarily stored on the infimum */
    page_cur_t cur1;
    page_cur_t cur2;

    page_cur_set_before_first(block, &cur1);
    page_cur_set_before_first(oblock, &cur2);

    /* Set locks according to old locks */
    for (;;) {
      ulint old_heap_no;
      ulint new_heap_no;

      ut_ad(comp || !memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size_old(page_cur_get_rec(&cur2))));
      if (likely(comp)) {
        old_heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur2));
        new_heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur1));
      } else {
        old_heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur2));
        new_heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur1));
      }

      if (lock_rec_get_nth_bit(lock, old_heap_no)) {

        /* Clear the bit in old_lock. */
        ut_d(lock_rec_reset_nth_bit(lock, old_heap_no));

        /* NOTE that the old lock bitmap could be too
        small for the new heap number! */

        lock_rec_add_to_queue(lock->type_mode, block, new_heap_no, lock->index, lock->trx);

        /* if (new_heap_no == PAGE_HEAP_NO_SUPREMUM
        && lock_get_wait(lock)) {
        ib_logger(ib_stream,
        "---\n--\n!!!Lock reorg: supr type %lu\n",
        lock->type_mode);
        } */
      }

      if (unlikely(new_heap_no == PAGE_HEAP_NO_SUPREMUM)) {

        ut_ad(old_heap_no == PAGE_HEAP_NO_SUPREMUM);
        break;
      }

      page_cur_move_to_next(&cur1);
      page_cur_move_to_next(&cur2);
    }

#ifdef UNIV_DEBUG
    {
      ulint i = lock_rec_find_set_bit(lock);

      /* Check that all locks were moved. */
      if (unlikely(i != ULINT_UNDEFINED)) {
        ib_logger(
          ib_stream,
          "lock_move_reorganize_page():"
          " %lu not moved in %p\n",
          (ulong)i,
          (void *)lock
        );
        ut_error;
      }
    }
#endif /* UNIV_DEBUG */
  }

  lock_mutex_exit_kernel();

  mem_heap_free(heap);

#ifdef UNIV_DEBUG_LOCK_VALIDATE
  ut_ad(lock_rec_validate_page(block->get_space(), block->get_page_no()));
#endif /* UNIV_DEBUG_LOCK_VALIDATE */
}

void lock_move_rec_list_end(const buf_block_t *new_block, const buf_block_t *block, const rec_t *rec) {
  Lock *lock;
  const ulint comp = page_rec_is_comp(rec);

  lock_mutex_enter_kernel();

  /* Note: when we move locks from record to record, waiting locks
  and possible granted gap type locks behind them are enqueued in
  the original order, because new elements are inserted to a hash
  table to the end of the hash chain, and lock_rec_add_to_queue
  does not reuse locks if there are waiters in the queue. */

  for (lock = lock_rec_get_first_on_page(block); lock; lock = lock_rec_get_next_on_page(lock)) {
    page_cur_t cur1;
    page_cur_t cur2;
    const ulint type_mode = lock->type_mode;

    page_cur_position(rec, block, &cur1);

    if (page_cur_is_before_first(&cur1)) {
      page_cur_move_to_next(&cur1);
    }

    page_cur_set_before_first(new_block, &cur2);
    page_cur_move_to_next(&cur2);

    /* Copy lock requests on user records to new page and
    reset the lock bits on the old */

    while (!page_cur_is_after_last(&cur1)) {
      ulint heap_no;

      if (comp) {
        heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur1));
      } else {
        heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur1));
        ut_ad(!memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size_old(page_cur_get_rec(&cur2))));
      }

      if (lock_rec_get_nth_bit(lock, heap_no)) {
        lock_rec_reset_nth_bit(lock, heap_no);

        if (unlikely(type_mode & LOCK_WAIT)) {
          lock_reset_lock_and_trx_wait(lock);
        }

        if (comp) {
          heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur2));
        } else {
          heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur2));
        }

        lock_rec_add_to_queue(type_mode, new_block, heap_no, lock->index, lock->trx);
      }

      page_cur_move_to_next(&cur1);
      page_cur_move_to_next(&cur2);
    }
  }

  lock_mutex_exit_kernel();

#ifdef UNIV_DEBUG_LOCK_VALIDATE
  ut_ad(lock_rec_validate_page(block->get_space(), block->get_page_no()));
  ut_ad(lock_rec_validate_page(new_block->get_space(), new_block->get_page_no()));
#endif /* UNIV_DEBUG_LOCK_VALIDATE */
}

void lock_move_rec_list_start(const buf_block_t *new_block, const buf_block_t *block, const rec_t *rec, const rec_t *old_end) {
  Lock *lock;
  const ulint comp = page_rec_is_comp(rec);

  ut_ad(block->m_frame == page_align(rec));
  ut_ad(new_block->m_frame == page_align(old_end));

  lock_mutex_enter_kernel();

  for (lock = lock_rec_get_first_on_page(block); lock; lock = lock_rec_get_next_on_page(lock)) {
    page_cur_t cur1;
    page_cur_t cur2;
    const ulint type_mode = lock->type_mode;

    page_cur_set_before_first(block, &cur1);
    page_cur_move_to_next(&cur1);

    page_cur_position(old_end, new_block, &cur2);
    page_cur_move_to_next(&cur2);

    /* Copy lock requests on user records to new page and
    reset the lock bits on the old */

    while (page_cur_get_rec(&cur1) != rec) {
      ulint heap_no;

      if (comp) {
        heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur1));
      } else {
        heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur1));
        ut_ad(!memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size_old(page_cur_get_rec(&cur2))));
      }

      if (lock_rec_get_nth_bit(lock, heap_no)) {
        lock_rec_reset_nth_bit(lock, heap_no);

        if (unlikely(type_mode & LOCK_WAIT)) {
          lock_reset_lock_and_trx_wait(lock);
        }

        if (comp) {
          heap_no = rec_get_heap_no_new(page_cur_get_rec(&cur2));
        } else {
          heap_no = rec_get_heap_no_old(page_cur_get_rec(&cur2));
        }

        lock_rec_add_to_queue(type_mode, new_block, heap_no, lock->index, lock->trx);
      }

      page_cur_move_to_next(&cur1);
      page_cur_move_to_next(&cur2);
    }

#ifdef UNIV_DEBUG
    if (page_rec_is_supremum(rec)) {
      ulint i;

      for (i = PAGE_HEAP_NO_USER_LOW; i < lock_rec_get_n_bits(lock); i++) {
        if (unlikely(lock_rec_get_nth_bit(lock, i))) {

          ib_logger(
            ib_stream,
            "lock_move_rec_list_start():"
            " %lu not moved in %p\n",
            (ulong)i,
            (void *)lock
          );
          ut_error;
        }
      }
    }
#endif /* UNIV_DEBUG */
  }

  lock_mutex_exit_kernel();

#ifdef UNIV_DEBUG_LOCK_VALIDATE
  ut_ad(lock_rec_validate_page(block->get_space(), block->get_page_no()));
#endif /* UNIV_DEBUG_LOCK_VALIDATE */
}

void lock_update_split_right(const buf_block_t *right_block, const buf_block_t *left_block) {
  ulint heap_no = lock_get_min_heap_no(right_block);

  lock_mutex_enter_kernel();

  /* Move the locks on the supremum of the left page to the supremum
  of the right page */

  lock_rec_move(right_block, left_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

  /* Inherit the locks to the supremum of left page from the successor
  of the infimum on right page */

  lock_rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);

  lock_mutex_exit_kernel();
}

void lock_update_merge_right(const buf_block_t *right_block, const rec_t *orig_succ, const buf_block_t *left_block) {
  lock_mutex_enter_kernel();

  /* Inherit the locks from the supremum of the left page to the
  original successor of infimum on the right page, to which the left
  page was merged */

  lock_rec_inherit_to_gap(right_block, left_block, page_rec_get_heap_no(orig_succ), PAGE_HEAP_NO_SUPREMUM);

  /* Reset the locks on the supremum of the left page, releasing
  waiting transactions */

  lock_rec_reset_and_release_wait(left_block, PAGE_HEAP_NO_SUPREMUM);

  lock_rec_free_all_from_discard_page(left_block);

  lock_mutex_exit_kernel();
}

void lock_update_root_raise(const buf_block_t *block, const buf_block_t *root) {
  lock_mutex_enter_kernel();

  /* Move the locks on the supremum of the root to the supremum of block */

  lock_rec_move(block, root, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);
  lock_mutex_exit_kernel();
}

void lock_update_copy_and_discard(const buf_block_t *new_block, const buf_block_t *block) {
  lock_mutex_enter_kernel();

  /* Move the locks on the supremum of the old page to the supremum
  of new_page */

  lock_rec_move(new_block, block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);
  lock_rec_free_all_from_discard_page(block);

  lock_mutex_exit_kernel();
}

void lock_update_split_left(const buf_block_t *right_block, const buf_block_t *left_block) {
  ulint heap_no = lock_get_min_heap_no(right_block);

  lock_mutex_enter_kernel();

  /* Inherit the locks to the supremum of the left page from the
  successor of the infimum on the right page */

  lock_rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);

  lock_mutex_exit_kernel();
}

void lock_update_merge_left(const buf_block_t *left_block, const rec_t *orig_pred, const buf_block_t *right_block) {
  const rec_t *left_next_rec;

  ut_ad(left_block->m_frame == page_align(orig_pred));

  lock_mutex_enter_kernel();

  left_next_rec = page_rec_get_next_const(orig_pred);

  if (!page_rec_is_supremum(left_next_rec)) {

    /* Inherit the locks on the supremum of the left page to the
    first record which was moved from the right page */

    lock_rec_inherit_to_gap(left_block, left_block, page_rec_get_heap_no(left_next_rec), PAGE_HEAP_NO_SUPREMUM);

    /* Reset the locks on the supremum of the left page,
    releasing waiting transactions */

    lock_rec_reset_and_release_wait(left_block, PAGE_HEAP_NO_SUPREMUM);
  }

  /* Move the locks from the supremum of right page to the supremum
  of the left page */

  lock_rec_move(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

  lock_rec_free_all_from_discard_page(right_block);

  lock_mutex_exit_kernel();
}

void lock_rec_reset_and_inherit_gap_locks(
  const buf_block_t *heir_block, const buf_block_t *block, ulint heir_heap_no, ulint heap_no
) {
  mutex_enter(&kernel_mutex);

  lock_rec_reset_and_release_wait(heir_block, heir_heap_no);

  lock_rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

  mutex_exit(&kernel_mutex);
}

void lock_update_discard(const buf_block_t *heir_block, ulint heir_heap_no, const buf_block_t *block) {
  const page_t *page = block->m_frame;
  const rec_t *rec;
  ulint heap_no;

  lock_mutex_enter_kernel();

  if (!lock_rec_get_first_on_page(block)) {
    /* No locks exist on page, nothing to do */

    lock_mutex_exit_kernel();

    return;
  }

  /* Inherit all the locks on the page to the record and reset all
  the locks on the page */

  if (page_is_comp(page)) {
    rec = page + PAGE_NEW_INFIMUM;

    do {
      heap_no = rec_get_heap_no_new(rec);

      lock_rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

      lock_rec_reset_and_release_wait(block, heap_no);

      rec = page + rec_get_next_offs(rec, true);
    } while (heap_no != PAGE_HEAP_NO_SUPREMUM);
  } else {
    rec = page + PAGE_OLD_INFIMUM;

    do {
      heap_no = rec_get_heap_no_old(rec);

      lock_rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

      lock_rec_reset_and_release_wait(block, heap_no);

      rec = page + rec_get_next_offs(rec, false);
    } while (heap_no != PAGE_HEAP_NO_SUPREMUM);
  }

  lock_rec_free_all_from_discard_page(block);

  lock_mutex_exit_kernel();
}

void lock_update_insert(const buf_block_t *block, const rec_t *rec) {
  ulint receiver_heap_no;
  ulint donator_heap_no;

  ut_ad(block->m_frame == page_align(rec));

  /* Inherit the gap-locking locks for rec, in gap mode, from the next
  record */

  if (page_rec_is_comp(rec)) {
    receiver_heap_no = rec_get_heap_no_new(rec);
    donator_heap_no = rec_get_heap_no_new(page_rec_get_next_low(rec, true));
  } else {
    receiver_heap_no = rec_get_heap_no_old(rec);
    donator_heap_no = rec_get_heap_no_old(page_rec_get_next_low(rec, false));
  }

  lock_mutex_enter_kernel();
  lock_rec_inherit_to_gap_if_gap_lock(block, receiver_heap_no, donator_heap_no);
  lock_mutex_exit_kernel();
}

void lock_update_delete(const buf_block_t *block, const rec_t *rec) {
  const page_t *page = block->m_frame;
  ulint heap_no;
  ulint next_heap_no;

  ut_ad(page == page_align(rec));

  if (page_is_comp(page)) {
    heap_no = rec_get_heap_no_new(rec);
    next_heap_no = rec_get_heap_no_new(page + rec_get_next_offs(rec, true));
  } else {
    heap_no = rec_get_heap_no_old(rec);
    next_heap_no = rec_get_heap_no_old(page + rec_get_next_offs(rec, false));
  }

  lock_mutex_enter_kernel();

  /* Let the next record inherit the locks from rec, in gap mode */

  lock_rec_inherit_to_gap(block, block, next_heap_no, heap_no);

  /* Reset the lock bits on rec and release waiting transactions */

  lock_rec_reset_and_release_wait(block, heap_no);

  lock_mutex_exit_kernel();
}

void lock_rec_store_on_page_infimum(const buf_block_t *block, const rec_t *rec) {
  ulint heap_no = page_rec_get_heap_no(rec);

  ut_ad(block->m_frame == page_align(rec));

  lock_mutex_enter_kernel();

  lock_rec_move(block, block, PAGE_HEAP_NO_INFIMUM, heap_no);

  lock_mutex_exit_kernel();
}

void lock_rec_restore_from_page_infimum(const buf_block_t *block, const rec_t *rec, const buf_block_t *donator) {
  ulint heap_no = page_rec_get_heap_no(rec);

  lock_mutex_enter_kernel();

  lock_rec_move(block, donator, heap_no, PAGE_HEAP_NO_INFIMUM);

  lock_mutex_exit_kernel();
}

static bool lock_deadlock_occurs(Lock *lock, trx_t *trx) {
  trx_t *mark_trx;
  ulint ret;
  ulint cost = 0;

  ut_ad(trx);
  ut_ad(lock);
  ut_ad(mutex_own(&kernel_mutex));
retry:
  /* We check that adding this trx to the waits-for graph
  does not produce a cycle. First mark all active transactions
  with 0: */

  mark_trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

  while (mark_trx) {
    mark_trx->m_deadlock_mark = 0;
    mark_trx = UT_LIST_GET_NEXT(trx_list, mark_trx);
  }

  ret = lock_deadlock_recursive(trx, trx, lock, &cost, 0);

  switch (ret) {
    case LOCK_VICTIM_IS_OTHER:
      /* We chose some other trx as a victim: retry if there still
    is a deadlock */
      goto retry;

    case LOCK_EXCEED_MAX_DEPTH:
      /* If the lock search exceeds the max step
    or the max depth, the current trx will be
    the victim. Print its information. */
      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "TOO DEEP OR LONG SEARCH IN THE LOCK TABLE"
        " WAITS-FOR GRAPH, WE WILL ROLL BACK"
        " FOLLOWING TRANSACTION \n"
      );

      ib_logger(ib_stream, "\n*** TRANSACTION:\n");
      trx_print(ib_stream, trx, 3000);

      ib_logger(ib_stream, "*** WAITING FOR THIS LOCK TO BE GRANTED:\n");

      if (lock_get_type(lock) == LOCK_REC) {
        lock_rec_print(ib_stream, lock);
      } else {
        lock_table_print(ib_stream, lock);
      }
      break;

    case LOCK_VICTIM_IS_START:
      ib_logger(ib_stream, "*** WE ROLL BACK TRANSACTION (2)\n");
      break;

    default:
      /* No deadlock detected*/
      return false;
  }

  lock_deadlock_found = true;

  return true;
}

static ulint lock_deadlock_recursive(trx_t *start, trx_t *trx, Lock *wait_lock, ulint *cost, ulint depth) {
  ulint ret;
  Lock *lock;
  trx_t *lock_trx;
  ulint heap_no = ULINT_UNDEFINED;

  ut_a(trx);
  ut_a(start);
  ut_a(wait_lock);
  ut_ad(mutex_own(&kernel_mutex));

  if (trx->m_deadlock_mark == 1) {
    /* We have already exhaustively searched the subtree starting
    from this trx */

    return 0;
  }

  *cost = *cost + 1;

  if (lock_get_type_low(wait_lock) == LOCK_REC) {
    ulint space;
    ulint page_no;

    heap_no = lock_rec_find_set_bit(wait_lock);
    ut_a(heap_no != ULINT_UNDEFINED);

    space = wait_lock->un_member.rec_lock.space;
    page_no = wait_lock->un_member.rec_lock.page_no;

    lock = lock_rec_get_first_on_page_addr(space, page_no);

    /* Position the iterator on the first matching record lock. */
    while (lock != nullptr && lock != wait_lock && !lock_rec_get_nth_bit(lock, heap_no)) {

      lock = lock_rec_get_next_on_page(lock);
    }

    if (lock == wait_lock) {
      lock = nullptr;
    }

    ut_ad(lock == nullptr || lock_rec_get_nth_bit(lock, heap_no));

  } else {
    lock = wait_lock;
  }

  /* Look at the locks ahead of wait_lock in the lock queue */

  for (;;) {
    /* Get previous table lock. */
    if (heap_no == ULINT_UNDEFINED) {

      lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
    }

    if (lock == nullptr) {
      /* We can mark this subtree as searched */
      trx->m_deadlock_mark = 1;

      return false;
    }

    if (lock_has_to_wait(wait_lock, lock)) {

      bool too_far = depth > LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK || *cost > LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK;

      lock_trx = lock->trx;

      if (lock_trx == start) {

        /* We came back to the recursion starting
        point: a deadlock detected; or we have
        searched the waits-for graph too long */

        ib_stream_t ib_stream;

        ib_stream = lock_latest_err_stream;

        ut_print_timestamp(ib_stream);

        ib_logger(ib_stream, "\n*** (1) TRANSACTION:\n");

        trx_print(ib_stream, wait_lock->trx, 3000);

        ib_logger(
          ib_stream,
          "*** (1) WAITING FOR THIS LOCK"
          " TO BE GRANTED:\n"
        );

        if (lock_get_type_low(wait_lock) == LOCK_REC) {
          lock_rec_print(ib_stream, wait_lock);
        } else {
          lock_table_print(ib_stream, wait_lock);
        }

        ib_logger(ib_stream, "*** (2) TRANSACTION:\n");

        trx_print(ib_stream, lock->trx, 3000);

        ib_logger(ib_stream, "*** (2) HOLDS THE LOCK(S):\n");

        if (lock_get_type_low(lock) == LOCK_REC) {
          lock_rec_print(ib_stream, lock);
        } else {
          lock_table_print(ib_stream, lock);
        }

        ib_logger(
          ib_stream,
          "*** (2) WAITING FOR THIS LOCK"
          " TO BE GRANTED:\n"
        );

        if (lock_get_type_low(start->wait_lock) == LOCK_REC) {
          lock_rec_print(ib_stream, start->wait_lock);
        } else {
          lock_table_print(ib_stream, start->wait_lock);
        }
#ifdef UNIV_DEBUG
        if (lock_print_waits) {
          ib_logger(ib_stream, "Deadlock detected\n");
        }
#endif /* UNIV_DEBUG */

        if (trx_weight_cmp(wait_lock->trx, start) >= 0) {
          /* Our recursion starting point
          transaction is 'smaller', let us
          choose 'start' as the victim and roll
          back it */

          return LOCK_VICTIM_IS_START;
        }

        lock_deadlock_found = true;

        /* Let us choose the transaction of wait_lock
        as a victim to try to avoid deadlocking our
        recursion starting point transaction */

        ib_logger(ib_stream, "*** WE ROLL BACK TRANSACTION (1)\n");

        wait_lock->trx->was_chosen_as_deadlock_victim = true;

        lock_cancel_waiting_and_release(wait_lock);

        /* Since trx and wait_lock are no longer
        in the waits-for graph, we can return false;
        note that our selective algorithm can choose
        several transactions as victims, but still
        we may end up rolling back also the recursion
        starting point transaction! */

        return LOCK_VICTIM_IS_OTHER;
      }

      if (too_far) {

#ifdef UNIV_DEBUG
        if (lock_print_waits) {
          ib_logger(
            ib_stream,
            "Deadlock search exceeds"
            " max steps or depth.\n"
          );
        }
#endif /* UNIV_DEBUG */
        /* The information about transaction/lock
        to be rolled back is available in the top
        level. Do not print anything here. */
        return LOCK_EXCEED_MAX_DEPTH;
      }

      if (lock_trx->m_que_state == TRX_QUE_LOCK_WAIT) {

        /* Another trx ahead has requested lock	in an
        incompatible mode, and is itself waiting for
        a lock */

        ret = lock_deadlock_recursive(start, lock_trx, lock_trx->wait_lock, cost, depth + 1);

        if (ret != 0) {

          return ret;
        }
      }
    }
    /* Get the next record lock to check. */
    if (heap_no != ULINT_UNDEFINED) {

      ut_a(lock != nullptr);

      do {
        lock = lock_rec_get_next_on_page(lock);
      } while (lock != nullptr && lock != wait_lock && !lock_rec_get_nth_bit(lock, heap_no));

      if (lock == wait_lock) {
        lock = nullptr;
      }
    }
  } /* end of the 'for (;;)'-loop */
}

/** Creates a table lock object and adds it as the last in the lock queue
of the table. Does NOT check for deadlocks or lock compatibility.
@param[in] table                Database table in dictionary cache 
@param[in] type_mode            Lock mode possibly ORed with LOCK_WAIT
@param[in] trx                  Transaction.
@return	own: new lock object */
inline Lock *lock_table_create(dict_table_t *table, ulint type_mode, trx_t *trx) {
  ut_ad(table && trx);
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->lock_heap, sizeof(Lock)));

  UT_LIST_ADD_LAST(trx->trx_locks, lock);

  lock->type_mode = type_mode | LOCK_TABLE;
  lock->trx = trx;

  lock->un_member.tab_lock.table = table;

  UT_LIST_ADD_LAST(table->locks, lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock_set_lock_and_trx_wait(lock, trx);
  }

  return lock;
}

/** Removes a table lock request from the queue and the trx list of locks;
this is a low-level function which does NOT check if waiting requests
can now be granted. */
inline void lock_table_remove_low(Lock *lock) {
  trx_t *trx;
  dict_table_t *table;

  ut_ad(mutex_own(&kernel_mutex));

  trx = lock->trx;
  table = lock->un_member.tab_lock.table;

  UT_LIST_REMOVE(trx->trx_locks, lock);
  UT_LIST_REMOVE(table->locks, lock);
}

/** Enqueues a waiting request for a table lock which cannot be granted
immediately. Checks for deadlocks.
@param[in] mode                 lock mode this transaction is requesting
@param[in] table                Table
@param[in] thr                  Query thread
@return DB_LOCK_WAIT, DB_DEADLOCK, or DB_QUE_THR_SUSPENDED, or
DB_SUCCESS; DB_SUCCESS means that there was a deadlock, but another
transaction was chosen as a victim, and we got the lock immediately:
no need to wait then */
static db_err lock_table_enqueue_waiting(ulint mode, dict_table_t *table, que_thr_t *thr) {
  ut_ad(mutex_own(&kernel_mutex));

  /* Test if there already is some other reason to suspend thread:
  we do not enqueue a lock request if the query thread should be
  stopped anyway */

  if (que_thr_stop(thr)) {
    ut_error;

    return DB_QUE_THR_SUSPENDED;
  }

  auto trx = thr_get_trx(thr);

  switch (trx_get_dict_operation(trx)) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: a table lock wait happens"
        " in a dictionary operation!\n"
        "Table name "
      );
      ut_print_name(ib_stream, trx, true, table->name);
      ib_logger(
        ib_stream,
        ".\n"
        "Submit a detailed bug report, "
        "check the InnoDB website for details"
      );
  }

  /* Enqueue the lock request that will wait to be granted */

  auto lock = lock_table_create(table, mode | LOCK_WAIT, trx);

  /* Check if a deadlock occurs: if yes, remove the lock request and
  return an error code */

  if (lock_deadlock_occurs(lock, trx)) {

    /* The order here is important, we don't want to
    lose the state of the lock before calling remove. */
    lock_table_remove_low(lock);
    lock_reset_lock_and_trx_wait(lock);

    return DB_DEADLOCK;
  }

  if (trx->wait_lock == nullptr) {
    /* Deadlock resolution chose another transaction as a victim,
    and we accidentally got our lock granted! */

    return DB_SUCCESS;
  }

  trx->m_que_state = TRX_QUE_LOCK_WAIT;
  trx->was_chosen_as_deadlock_victim = false;
  trx->wait_started = time(nullptr);

  ut_a(que_thr_stop(thr));

  return DB_LOCK_WAIT;
}

/** Checks if other transactions have an incompatible mode lock request in the lock queue.
@param[in] trx                  Transaction, or nullptr if all transactions should be included
@param[in] wait                 LOCK_WAIT if also waiting locks are taken into account, or 0 if not
@param[in] table                Table
@param[in] mode                 lock mode
@return	lock or nullptr */
inline Lock *lock_table_other_has_incompatible(trx_t *trx, ulint wait, dict_table_t *table, Lock_mode mode) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = UT_LIST_GET_LAST(table->locks);

  while (lock != nullptr) {

    if ((lock->trx != trx) && (!lock_mode_compatible(lock_get_mode(lock), mode)) && (wait || !(lock_get_wait(lock)))) {

      return lock;
    }

    lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
  }

  return nullptr;
}

db_err lock_table(ulint flags, dict_table_t *table, Lock_mode mode, que_thr_t *thr) {
  trx_t *trx;
  db_err err;

  ut_ad(table && thr);

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  ut_a(flags == 0);

  trx = thr_get_trx(thr);

  lock_mutex_enter_kernel();

  /* Look for stronger locks the same trx already has on the table */

  if (lock_table_has(trx, table, mode)) {

    lock_mutex_exit_kernel();

    return DB_SUCCESS;
  }

  /* We have to check if the new lock is compatible with any locks
  other transactions have in the table lock queue. */

  if (lock_table_other_has_incompatible(trx, LOCK_WAIT, table, mode)) {

    /* Another trx has a request on the table in an incompatible
    mode: this trx may have to wait */

    err = lock_table_enqueue_waiting(mode | flags, table, thr);

    lock_mutex_exit_kernel();

    return err;
  }

  lock_table_create(table, mode | flags, trx);

  ut_a(!flags || mode == LOCK_S || mode == LOCK_X);

  lock_mutex_exit_kernel();

  return DB_SUCCESS;
}

/** Checks if a waiting table lock request still has to wait in a queue.
@return	true if still has to wait */
static bool lock_table_has_to_wait_in_queue(Lock *wait_lock) {
  ut_ad(lock_get_wait(wait_lock));

  auto table = wait_lock->un_member.tab_lock.table;
  auto lock = UT_LIST_GET_FIRST(table->locks);

  while (lock != wait_lock) {

    if (lock_has_to_wait(wait_lock, lock)) {

      return true;
    }

    lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
  }

  return false;
}

/** Removes a table lock request, waiting or granted, from the queue and grants
locks to other transactions in the queue, if they now are entitled to a
lock.
@param[in] in_lock              Table lock ; transactions waiting behind will
                                get their lock requests granted, if they are now
                                qualified to it */
static void lock_table_dequeue(Lock *in_lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(lock_get_type_low(in_lock) == LOCK_TABLE);

  auto lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, in_lock);

  lock_table_remove_low(in_lock);

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  while (lock != nullptr) {

    if (lock_get_wait(lock) && !lock_table_has_to_wait_in_queue(lock)) {

      /* Grant the lock */
      lock_grant(lock);
    }

    lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
  }
}

void lock_rec_unlock(trx_t *trx, const buf_block_t *block, const rec_t *rec, Lock_mode Lock_mode) {
  ut_ad(trx && rec);
  ut_ad(block->m_frame == page_align(rec));

  auto heap_no = page_rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  auto lock = lock_rec_get_first(block, heap_no);

  /* Find the last lock with the same Lock_mode and transaction
  from the record. */

  Lock *release_lock = nullptr;

  while (lock != nullptr) {
    if (lock->trx == trx && lock_get_mode(lock) == Lock_mode) {
      release_lock = lock;
      ut_a(!lock_get_wait(lock));
    }

    lock = lock_rec_get_next(heap_no, lock);
  }

  /* If a record lock is found, release the record lock */

  if (likely(release_lock != nullptr)) {
    lock_rec_reset_nth_bit(release_lock, heap_no);
  } else {
    mutex_exit(&kernel_mutex);
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: unlock row could not"
      " find a %lu mode lock on the record\n",
      (ulong)Lock_mode
    );

    return;
  }

  /* Check if we can now grant waiting lock requests */

  lock = lock_rec_get_first(block, heap_no);

  while (lock != nullptr) {
    if (lock_get_wait(lock) && !lock_rec_has_to_wait_in_queue(lock)) {

      /* Grant the lock */
      lock_grant(lock);
    }

    lock = lock_rec_get_next(heap_no, lock);
  }

  mutex_exit(&kernel_mutex);
}

void lock_release_off_kernel(trx_t *trx) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = UT_LIST_GET_LAST(trx->trx_locks);
  ulint count{};

  while (lock != nullptr) {

    ++count;

    if (lock_get_type_low(lock) == LOCK_REC) {

      lock_rec_dequeue_from_page(lock);

    } else {

      ut_ad(lock_get_type_low(lock) & LOCK_TABLE);

      lock_table_dequeue(lock);
    }

    if (count == LOCK_RELEASE_KERNEL_INTERVAL) {
      /* Release the kernel mutex for a while, so that we
      do not monopolize it */

      lock_mutex_exit_kernel();

      lock_mutex_enter_kernel();

      count = 0;
    }

    lock = UT_LIST_GET_LAST(trx->trx_locks);
  }

  mem_heap_empty(trx->lock_heap);
}

void lock_cancel_waiting_and_release(Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));

  if (lock_get_type_low(lock) == LOCK_REC) {

    lock_rec_dequeue_from_page(lock);
  } else {
    ut_ad(lock_get_type_low(lock) & LOCK_TABLE);

    lock_table_dequeue(lock);
  }

  /* Reset the wait flag and the back pointer to lock in trx */

  lock_reset_lock_and_trx_wait(lock);

  /* The following function releases the trx from lock wait */

  trx_end_lock_wait(lock->trx);
}

/* True if a lock mode is S or X */
#define IS_LOCK_S_OR_X(lock) (lock_get_mode(lock) == LOCK_S || lock_get_mode(lock) == LOCK_X)

/** Removes locks of a transaction on a table to be dropped.
If remove_sx_locks is true then table-level S and X locks are
also removed in addition to other table-level and record-level locks.
No lock, that is going to be removed, is allowed to be a wait lock.
@param[in] table                Table to be dropped
@param[in] trx                  Transaction
@param[in] remove_sx_locks      Also removes table S and X locks */
static void lock_remove_all_on_table_for_trx(dict_table_t *table, trx_t *trx, bool remove_sx_locks) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = UT_LIST_GET_LAST(trx->trx_locks);

  while (lock != nullptr) {
    auto prev_lock = UT_LIST_GET_PREV(trx_locks, lock);

    if (lock_get_type_low(lock) == LOCK_REC && lock->index->table == table) {
      ut_a(!lock_get_wait(lock));

      lock_rec_discard(lock);
    } else if (lock_get_type_low(lock) & LOCK_TABLE && lock->un_member.tab_lock.table == table && (remove_sx_locks || !IS_LOCK_S_OR_X(lock))) {

      ut_a(!lock_get_wait(lock));

      lock_table_remove_low(lock);
    }

    lock = prev_lock;
  }
}

void lock_remove_all_on_table(dict_table_t *table, bool remove_sx_locks) {
  mutex_enter(&kernel_mutex);

  auto lock = UT_LIST_GET_FIRST(table->locks);

  while (lock != nullptr) {

    auto prev_lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);

    /* If we should remove all locks (remove_sx_locks
    is true), or if the lock is not table-level S or X lock,
    then check we are not going to remove a wait lock. */
    if (remove_sx_locks || !(lock_get_type(lock) == LOCK_TABLE && IS_LOCK_S_OR_X(lock))) {

      // HACK: For testing
      if (lock_get_wait(lock)) {
        if (remove_sx_locks) {
          ut_error;
        } else {
          goto next;
        }
      }
    }

    lock_remove_all_on_table_for_trx(table, lock->trx, remove_sx_locks);

    if (prev_lock == nullptr) {
      if (lock == UT_LIST_GET_FIRST(table->locks)) {
        /* lock was not removed, pick its successor */
        lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
      } else {
        /* lock was removed, pick the first one */
        lock = UT_LIST_GET_FIRST(table->locks);
      }
    } else if (UT_LIST_GET_NEXT(un_member.tab_lock.locks, prev_lock) != lock) {
      /* If lock was removed by
      lock_remove_all_on_table_for_trx() then pick the
      successor of prev_lock ... */
      lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, prev_lock);
    } else {
    next:
      /* ... otherwise pick the successor of lock. */
      lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
    }
  }

  mutex_exit(&kernel_mutex);
}

void lock_table_print(ib_stream_t ib_stream, const Lock *lock) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(lock_get_type_low(lock) == LOCK_TABLE);

  ib_logger(ib_stream, "TABLE LOCK table ");
  ut_print_name(ib_stream, lock->trx, true, lock->un_member.tab_lock.table->name);
  ib_logger(ib_stream, " trx id %lu", TRX_ID_PREP_PRINTF(lock->trx->m_id));

  if (lock_get_mode(lock) == LOCK_S) {
    ib_logger(ib_stream, " lock mode S");
  } else if (lock_get_mode(lock) == LOCK_X) {
    ib_logger(ib_stream, " lock mode X");
  } else if (lock_get_mode(lock) == LOCK_IS) {
    ib_logger(ib_stream, " lock mode IS");
  } else if (lock_get_mode(lock) == LOCK_IX) {
    ib_logger(ib_stream, " lock mode IX");
  } else if (lock_get_mode(lock) == LOCK_AUTO_INC) {
    ib_logger(ib_stream, " lock mode AUTO-INC");
  } else {
    ib_logger(ib_stream, " unknown lock mode %lu", (ulong)lock_get_mode(lock));
  }

  if (lock_get_wait(lock)) {
    ib_logger(ib_stream, " waiting");
  }

  ib_logger(ib_stream, "\n");
}

void lock_rec_print(ib_stream_t ib_stream, const Lock *lock) {
  const buf_block_t *block;
  ulint space;
  ulint page_no;
  ulint i;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(mutex_own(&kernel_mutex));
  ut_a(lock_get_type_low(lock) == LOCK_REC);

  space = lock->un_member.rec_lock.space;
  page_no = lock->un_member.rec_lock.page_no;

  ib_logger(
    ib_stream, "RECORD LOCKS space id %lu page no %lu n bits %lu ", (ulong)space, (ulong)page_no, (ulong)lock_rec_get_n_bits(lock)
  );
  dict_index_name_print(ib_stream, lock->trx, lock->index);
  ib_logger(ib_stream, " trx id %lu", TRX_ID_PREP_PRINTF(lock->trx->m_id));

  if (lock_get_mode(lock) == LOCK_S) {
    ib_logger(ib_stream, " lock mode S");
  } else if (lock_get_mode(lock) == LOCK_X) {
    ib_logger(ib_stream, " Lock_mode X");
  } else {
    ut_error;
  }

  if (lock_rec_get_gap(lock)) {
    ib_logger(ib_stream, " locks gap before rec");
  }

  if (lock_rec_get_rec_not_gap(lock)) {
    ib_logger(ib_stream, " locks rec but not gap");
  }

  if (lock_rec_get_insert_intention(lock)) {
    ib_logger(ib_stream, " insert intention");
  }

  if (lock_get_wait(lock)) {
    ib_logger(ib_stream, " waiting");
  }

  mtr_start(&mtr);

  ib_logger(ib_stream, "\n");

  Buf_pool::Request req {
    .m_page_id = { space, page_no },
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  block = srv_buf_pool->try_get_by_page_id(req);

  if (block != nullptr) {
    for (i = 0; i < lock_rec_get_n_bits(lock); i++) {

      if (lock_rec_get_nth_bit(lock, i)) {

        const rec_t *rec = page_find_rec_with_heap_no(block->get_frame(), i);
        offsets = rec_get_offsets(rec, lock->index, offsets, ULINT_UNDEFINED, &heap);

        ib_logger(ib_stream, "Record lock, heap no %lu ", (ulong)i);
        rec_print_new(ib_stream, rec, offsets);
        ib_logger(ib_stream, "\n");
      }
    }
  } else {
    for (i = 0; i < lock_rec_get_n_bits(lock); i++) {
      ib_logger(ib_stream, "Record lock, heap no %lu\n", (ulong)i);
    }
  }

  mtr_commit(&mtr);
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

#ifdef UNIV_DEBUG
/* Print the number of lock structs from lock_print_info_summary() only
in non-production builds for performance reasons. */
#define PRINT_NUM_OF_LOCK_STRUCTS
#endif /* UNIV_DEBUG */

#ifdef PRINT_NUM_OF_LOCK_STRUCTS
/** Calculates the number of record lock structs in the record lock hash table.
@return	number of record locks */
static ulint lock_get_n_rec_locks() {
  ulint n_locks{};

  ut_ad(mutex_own(&kernel_mutex));

  for (ulint i = 0; i < hash_get_n_cells(lock_sys->rec_hash); i++) {

    auto lock = (Lock *)HASH_GET_FIRST(lock_sys->rec_hash, i);

    while (lock) {
      n_locks++;

      lock = (Lock *)HASH_GET_NEXT(hash, lock);
    }
  }

  return n_locks;
}
#endif /* PRINT_NUM_OF_LOCK_STRUCTS */

bool lock_print_info_summary(ib_stream_t ib_stream, bool nowait) {
  /* if nowait is false, wait on the kernel mutex,
  otherwise return immediately if fail to obtain the
  mutex. */
  if (!nowait) {
    lock_mutex_enter_kernel();
  } else if (mutex_enter_nowait(&kernel_mutex)) {
    ib_logger(
      ib_stream,
      "FAIL TO OBTAIN KERNEL MUTEX, "
      "SKIP LOCK INFO PRINTING\n"
    );
    return false;
  }

  if (lock_deadlock_found) {
    ib_logger(
      ib_stream,
      "------------------------\n"
      "LATEST DETECTED DEADLOCK\n"
      "------------------------\n"
    );
  }

  ib_logger(
    ib_stream,
    "------------\n"
    "TRANSACTIONS\n"
    "------------\n"
  );

  ib_logger(ib_stream, "Trx id counter %lu\n", TRX_ID_PREP_PRINTF(trx_sys->max_trx_id));

  ib_logger(
    ib_stream,
    "Purge done for trx's n:o < %lu undo n:o < %lu\n",
    TRX_ID_PREP_PRINTF(purge_sys->purge_trx_no),
    TRX_ID_PREP_PRINTF(purge_sys->purge_undo_no)
  );

  ib_logger(ib_stream, "History list length %lu\n", (ulong)trx_sys->rseg_history_len);

#ifdef PRINT_NUM_OF_LOCK_STRUCTS
  ib_logger(ib_stream, "Total number of lock structs in row lock hash table %lu\n", (ulong)lock_get_n_rec_locks());
#endif /* PRINT_NUM_OF_LOCK_STRUCTS */
  return true;
}

void lock_print_info_all_transactions(ib_stream_t ib_stream) {
  Lock *lock;
  bool load_page_first = true;
  ulint nth_trx = 0;
  ulint nth_lock = 0;
  ulint i;
  mtr_t mtr;
  trx_t *trx;

  ib_logger(ib_stream, "LIST OF TRANSACTIONS FOR EACH SESSION:\n");

  /* First print info on non-active transactions */

  trx = UT_LIST_GET_FIRST(trx_sys->client_trx_list);

  while (trx) {
    if (trx->m_conc_state == TRX_NOT_STARTED) {
      ib_logger(ib_stream, "---");
      trx_print(ib_stream, trx, 600);
    }

    trx = UT_LIST_GET_NEXT(client_trx_list, trx);
  }

loop:
  trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

  i = 0;

  /* Since we temporarily release the kernel mutex when
  reading a database page in below, variable trx may be
  obsolete now and we must loop through the trx list to
  get probably the same trx, or some other trx. */

  while (trx && (i < nth_trx)) {
    trx = UT_LIST_GET_NEXT(trx_list, trx);
    i++;
  }

  if (trx == nullptr) {
    lock_mutex_exit_kernel();

    ut_ad(lock_validate());

    return;
  }

  if (nth_lock == 0) {
    ib_logger(ib_stream, "---");
    trx_print(ib_stream, trx, 600);

    if (trx->read_view) {
      ib_logger(
        ib_stream,
        "Trx read view will not see trx with"
        " id >= %lu, sees < %lu\n",
        TRX_ID_PREP_PRINTF(trx->read_view->low_limit_id),
        TRX_ID_PREP_PRINTF(trx->read_view->up_limit_id)
      );
    }

    if (trx->m_que_state == TRX_QUE_LOCK_WAIT) {
      ib_logger(
        ib_stream,
        "------- TRX HAS BEEN WAITING %lu SEC"
        " FOR THIS LOCK TO BE GRANTED:\n",
        (ulong)difftime(time(nullptr), trx->wait_started)
      );

      if (lock_get_type_low(trx->wait_lock) == LOCK_REC) {
        lock_rec_print(ib_stream, trx->wait_lock);
      } else {
        lock_table_print(ib_stream, trx->wait_lock);
      }

      ib_logger(ib_stream, "------------------\n");
    }
  }

  if (!srv_print_innodb_lock_monitor) {
    nth_trx++;
    goto loop;
  }

  i = 0;

  /* Look at the note about the trx loop above why we loop here:
  lock may be an obsolete pointer now. */

  lock = UT_LIST_GET_FIRST(trx->trx_locks);

  while (lock && (i < nth_lock)) {
    lock = UT_LIST_GET_NEXT(trx_locks, lock);
    i++;
  }

  if (lock == nullptr) {
    nth_trx++;
    nth_lock = 0;

    goto loop;
  }

  if (lock_get_type_low(lock) == LOCK_REC) {
    if (load_page_first) {
      auto space = lock->un_member.rec_lock.space;
      auto size = fil_space_get_flags(space);
      auto page_no = lock->un_member.rec_lock.page_no;

      if (unlikely(size == ULINT_UNDEFINED)) {

        /* It is a single table tablespace and the .ibd file is missing (TRUNCATE
        TABLE probably stole the locks): just print the lock without attempting to
        load the page in the buffer pool. */

        ib_logger(ib_stream, "RECORD LOCKS on non-existing space %lu\n", (ulong)space);
        goto print_rec;
      }

      lock_mutex_exit_kernel();

      mtr_start(&mtr);

      Buf_pool::Request req {
        .m_rw_latch = BUF_GET_NO_LATCH,
        .m_page_id = { space, page_no },
	.m_mode = BUF_GET_NO_LATCH,
	.m_file = __FILE__,
	.m_line = __LINE__,
	.m_mtr = &mtr
      };

      /* We are simply trying to force a read here. */
      (void) srv_buf_pool->get(req, nullptr);

      mtr_commit(&mtr);

      load_page_first = false;

      lock_mutex_enter_kernel();

      goto loop;
    }

  print_rec:
    lock_rec_print(ib_stream, lock);
  } else {
    ut_ad(lock_get_type_low(lock) & LOCK_TABLE);

    lock_table_print(ib_stream, lock);
  }

  load_page_first = true;

  nth_lock++;

  if (nth_lock >= 10) {
    ib_logger(
      ib_stream,
      "10 LOCKS PRINTED FOR THIS TRX:"
      " SUPPRESSING FURTHER PRINTS\n"
    );

    nth_trx++;
    nth_lock = 0;

    goto loop;
  }

  goto loop;
}

#ifdef UNIV_DEBUG
/** Validates the lock queue on a table.
@return	true if ok */
static bool lock_table_queue_validate(dict_table_t *table) {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = UT_LIST_GET_FIRST(table->locks);

  while (lock != nullptr) {
    ut_a(
      ((lock->trx)->m_conc_state == TRX_ACTIVE) || ((lock->trx)->m_conc_state == TRX_PREPARED) ||
      ((lock->trx)->m_conc_state == TRX_COMMITTED_IN_MEMORY)
    );

    if (!lock_get_wait(lock)) {

      ut_a(!lock_table_other_has_incompatible(lock->trx, 0, table, lock_get_mode(lock)));
    } else {

      ut_a(lock_table_has_to_wait_in_queue(lock));
    }

    lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
  }

  return true;
}

/** Validates the lock queue on a single record.
@param[in] block                Buffer block containing rec
@param[in] rec                  Record to look at
@param[in] index                Index, or nullptr if not known 
@param[in] offsets              rec_get_offsets(rec, index)
@return	true if ok */
static bool lock_rec_queue_validate(const buf_block_t *block, const rec_t *rec, dict_index_t *index, const ulint *offsets) {
  ut_a(rec);
  ut_a(block->get_frame() == page_align(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(!page_rec_is_comp(rec) == !rec_offs_comp(offsets));

  auto heap_no = page_rec_get_heap_no(rec);

  lock_mutex_enter_kernel();

  if (!page_rec_is_user_rec(rec)) {

    auto lock = lock_rec_get_first(block, heap_no);

    while (lock != nullptr) {
      switch (lock->trx->m_conc_state) {
        case TRX_ACTIVE:
        case TRX_PREPARED:
        case TRX_COMMITTED_IN_MEMORY:
          break;
        default:
          ut_error;
      }

      ut_a(trx_in_trx_list(lock->trx));

      if (lock_get_wait(lock)) {
        ut_a(lock_rec_has_to_wait_in_queue(lock));
      }

      if (index) {
        ut_a(lock->index == index);
      }

      lock = lock_rec_get_next(heap_no, lock);
    }

    lock_mutex_exit_kernel();

    return true;
  }

  if (index != nullptr && dict_index_is_clust(index)) {

    auto impl_trx = lock_clust_rec_some_has_impl(rec, index, offsets);

    if (impl_trx && lock_rec_other_has_expl_req(LOCK_S, 0, LOCK_WAIT, block, heap_no, impl_trx)) {

      ut_a(lock_rec_has_expl(LOCK_X | LOCK_REC_NOT_GAP, block, heap_no, impl_trx));
    }
  }

  auto lock = lock_rec_get_first(block, heap_no);

  while (lock != nullptr) {
    ut_a(
      lock->trx->m_conc_state == TRX_ACTIVE || lock->trx->m_conc_state == TRX_PREPARED ||
      lock->trx->m_conc_state == TRX_COMMITTED_IN_MEMORY
    );

    ut_a(trx_in_trx_list(lock->trx));

    if (index) {
      ut_a(lock->index == index);
    }

    if (!lock_rec_get_gap(lock) && !lock_get_wait(lock)) {

      enum Lock_mode mode;

      if (lock_get_mode(lock) == LOCK_S) {
        mode = LOCK_X;
      } else {
        mode = LOCK_S;
      }
      ut_a(!lock_rec_other_has_expl_req(mode, 0, 0, block, heap_no, lock->trx));

    } else if (lock_get_wait(lock) && !lock_rec_get_gap(lock)) {

      ut_a(lock_rec_has_to_wait_in_queue(lock));
    }

    lock = lock_rec_get_next(heap_no, lock);
  }

  lock_mutex_exit_kernel();

  return true;
}

static bool lock_rec_validate_page(space_id_t space, page_no_t page_no) {
  dict_index_t *index;
  Lock *lock;
  const rec_t *rec;
  ulint nth_lock = 0;
  ulint nth_bit = 0;
  ulint i;
  mtr_t mtr;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(!mutex_own(&kernel_mutex));

  mtr_start(&mtr);

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { space, page_no },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  const auto page = block->get_frame();

  lock_mutex_enter_kernel();
loop:
  lock = lock_rec_get_first_on_page_addr(space, page_no);

  if (!lock) {
    goto function_exit;
  }

  for (i = 0; i < nth_lock; i++) {

    lock = lock_rec_get_next_on_page(lock);

    if (!lock) {
      goto function_exit;
    }
  }

  ut_a(trx_in_trx_list(lock->trx));
  ut_a(
    lock->trx->m_conc_state == TRX_ACTIVE || lock->trx->m_conc_state == TRX_PREPARED || lock->trx->m_conc_state == TRX_COMMITTED_IN_MEMORY
  );

#ifdef UNIV_SYNC_DEBUG
  /* Only validate the record queues when this thread is not
  holding a space->latch.  Deadlocks are possible due to
  latching order violation when UNIV_DEBUG is defined while
  UNIV_SYNC_DEBUG is not. */
  if (!sync_thread_levels_contains(SYNC_FSP))
#endif /* UNIV_SYNC_DEBUG */
    for (i = nth_bit; i < lock_rec_get_n_bits(lock); i++) {

      if (i == 1 || lock_rec_get_nth_bit(lock, i)) {

        index = lock->index;
        rec = page_find_rec_with_heap_no(page, i);
        ut_a(rec);
        offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

        ib_logger(ib_stream, "Validating %lu %lu\n", (ulong)space, (ulong)page_no);

        lock_mutex_exit_kernel();

        /* If this thread is holding the file space
        latch (fil_space_t::latch), the following
        check WILL break the latching order and may
        cause a deadlock of threads. */

        lock_rec_queue_validate(block, rec, index, offsets);

        lock_mutex_enter_kernel();

        nth_bit = i + 1;

        goto loop;
      }
    }

  nth_bit = 0;
  nth_lock++;

  goto loop;

function_exit:
  lock_mutex_exit_kernel();

  mtr_commit(&mtr);

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return true;
}

/** Validates the lock system.
@return	true if ok */
static bool lock_validate() {
  Lock *lock;
  trx_t *trx;
  uint64_t limit;
  ulint space;
  ulint page_no;
  ulint i;

  lock_mutex_enter_kernel();

  trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

  while (trx) {
    lock = UT_LIST_GET_FIRST(trx->trx_locks);

    while (lock) {
      if (lock_get_type_low(lock) & LOCK_TABLE) {

        lock_table_queue_validate(lock->un_member.tab_lock.table);
      }

      lock = UT_LIST_GET_NEXT(trx_locks, lock);
    }

    trx = UT_LIST_GET_NEXT(trx_list, trx);
  }

  for (i = 0; i < hash_get_n_cells(lock_sys->rec_hash); i++) {

    limit = 0;

    for (;;) {
      lock = (Lock *)HASH_GET_FIRST(lock_sys->rec_hash, i);

      while (lock) {
        ut_a(trx_in_trx_list(lock->trx));

        space = lock->un_member.rec_lock.space;
        page_no = lock->un_member.rec_lock.page_no;

        if ((uint64_t((space) << 32) | page_no) >= limit) {
          break;
        }

        lock = (Lock *)HASH_GET_NEXT(hash, lock);
      }

      if (!lock) {

        break;
      }

      lock_mutex_exit_kernel();

      lock_rec_validate_page(space, page_no);

      lock_mutex_enter_kernel();

      limit = ((uint64_t(space) << 32) | page_no) + 1;
    }
  }

  lock_mutex_exit_kernel();

  return true;
}
#endif /* UNIV_DEBUG */

db_err lock_rec_insert_check_and_lock(
  ulint flags,
  const rec_t *rec,
  buf_block_t *block,
  dict_index_t *index,
  que_thr_t *thr,
  mtr_t *mtr,
  bool *inherit) {

  const rec_t *next_rec;
  trx_t *trx;
  Lock *lock;
  db_err err;
  ulint next_rec_heap_no;

  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  trx = thr_get_trx(thr);
  next_rec = page_rec_get_next_const(rec);
  next_rec_heap_no = page_rec_get_heap_no(next_rec);

  lock_mutex_enter_kernel();

  /* When inserting a record into an index, the table must be at
  least IX-locked or we must be building an index, in which case
  the table must be at least S-locked. */
  ut_ad(
    lock_table_has(trx, index->table, LOCK_IX) || (*index->name == TEMP_INDEX_PREFIX && lock_table_has(trx, index->table, LOCK_S))
  );

  lock = lock_rec_get_first(block, next_rec_heap_no);

  if (likely(lock == nullptr)) {
    /* We optimize CPU time usage in the simplest case */

    lock_mutex_exit_kernel();

    if (!dict_index_is_clust(index)) {
      /* Update the page max trx id field */
      page_update_max_trx_id(block, trx->m_id, mtr);
    }

    *inherit = false;

    return DB_SUCCESS;
  }

  *inherit = true;

  /* If another transaction has an explicit lock request which locks
  the gap, waiting or granted, on the successor, the insert has to wait.

  An exception is the case where the lock by the another transaction
  is a gap type lock which it placed to wait for its turn to insert. We
  do not consider that kind of a lock conflicting with our insert. This
  eliminates an unnecessary deadlock which resulted when 2 transactions
  had to wait for their insert. Both had waiting gap type lock requests
  on the successor, which produced an unnecessary deadlock. */

  if (lock_rec_other_has_conflicting(Lock_mode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION), block, next_rec_heap_no, trx)) {

    /* Note that we may get DB_SUCCESS also here! */
    err = lock_rec_enqueue_waiting(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION, block, next_rec_heap_no, index, thr);
  } else {
    err = DB_SUCCESS;
  }

  lock_mutex_exit_kernel();

  if ((err == DB_SUCCESS) && !dict_index_is_clust(index)) {
    /* Update the page max trx id field */
    page_update_max_trx_id(block, trx->m_id, mtr);
  }

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    const ulint *offsets;
    rec_offs_init(offsets_);

    offsets = rec_get_offsets(next_rec, index, offsets_, ULINT_UNDEFINED, &heap);
    ut_ad(lock_rec_queue_validate(block, next_rec, index, offsets));
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }
#endif /* UNIV_DEBUG */

  return err;
}

/** If a transaction has an implicit x-lock on a record, but no explicit x-lock
set on the record, sets one for it. NOTE that in the case of a secondary
index, the kernel mutex may get temporarily released. */
static void lock_rec_convert_impl_to_expl(
  const buf_block_t *block, /*!< in: buffer block of rec */
  const rec_t *rec,         /*!< in: user record on page */
  dict_index_t *index,      /*!< in: index of record */
  const ulint *offsets
) /*!< in: rec_get_offsets(rec, index) */
{
  trx_t *impl_trx;

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(!page_rec_is_comp(rec) == !rec_offs_comp(offsets));

  if (dict_index_is_clust(index)) {
    impl_trx = lock_clust_rec_some_has_impl(rec, index, offsets);
  } else {
    impl_trx = lock_sec_rec_some_has_impl_off_kernel(rec, index, offsets);
  }

  if (impl_trx) {
    ulint heap_no = page_rec_get_heap_no(rec);

    /* If the transaction has no explicit x-lock set on the
    record, set one for it */

    if (!lock_rec_has_expl(LOCK_X | LOCK_REC_NOT_GAP, block, heap_no, impl_trx)) {

      lock_rec_add_to_queue(LOCK_REC | LOCK_X | LOCK_REC_NOT_GAP, block, heap_no, index, impl_trx);
    }
  }
}

db_err lock_clust_rec_modify_check_and_lock(
  ulint flags, const buf_block_t *block, const rec_t *rec, dict_index_t *index, const ulint *offsets, que_thr_t *thr
) {

  db_err err;
  ulint heap_no;

  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(dict_index_is_clust(index));
  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  heap_no = rec_offs_comp(offsets) ? rec_get_heap_no_new(rec) : rec_get_heap_no_old(rec);

  lock_mutex_enter_kernel();

  ut_ad(lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));

  /* If a transaction has no explicit x-lock set on the record, set one
  for it */

  lock_rec_convert_impl_to_expl(block, rec, index, offsets);

  err = lock_rec_lock(true, LOCK_X | LOCK_REC_NOT_GAP, block, heap_no, index, thr);

  lock_mutex_exit_kernel();

  ut_ad(lock_rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err lock_sec_rec_modify_check_and_lock(
  ulint flags, buf_block_t *block, const rec_t *rec, dict_index_t *index, que_thr_t *thr, mtr_t *mtr
) {

  db_err err;
  ulint heap_no;

  ut_ad(!dict_index_is_clust(index));
  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  heap_no = page_rec_get_heap_no(rec);

  /* Another transaction cannot have an implicit lock on the record,
  because when we come here, we already have modified the clustered
  index record, and this would not have been possible if another active
  transaction had modified this secondary index record. */

  lock_mutex_enter_kernel();

  ut_ad(lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));

  err = lock_rec_lock(true, LOCK_X | LOCK_REC_NOT_GAP, block, heap_no, index, thr);

  lock_mutex_exit_kernel();

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    const ulint *offsets;
    rec_offs_init(offsets_);

    offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &heap);
    ut_ad(lock_rec_queue_validate(block, rec, index, offsets));
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }
#endif /* UNIV_DEBUG */

  if (err == DB_SUCCESS) {
    /* Update the page max trx id field */
    page_update_max_trx_id(block, thr_get_trx(thr)->m_id, mtr);
  }

  return err;
}

db_err lock_sec_rec_read_check_and_lock(
  ulint flags, const buf_block_t *block, const rec_t *rec, dict_index_t *index, const ulint *offsets, enum Lock_mode mode,
  ulint gap_mode, que_thr_t *thr
) {

  db_err err;
  ulint heap_no;

  ut_ad(!dict_index_is_clust(index));
  ut_ad(block->m_frame == page_align(rec));
  ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mode == LOCK_X || mode == LOCK_S);

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  heap_no = page_rec_get_heap_no(rec);

  lock_mutex_enter_kernel();

  ut_ad(mode != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
  ut_ad(mode != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list or a
  database recovery is running. */

  if ((page_get_max_trx_id(block->m_frame) >= trx_list_get_min_trx_id() || recv_recovery_on) && !page_rec_is_supremum(rec)) {

    lock_rec_convert_impl_to_expl(block, rec, index, offsets);
  }

  err = lock_rec_lock(false, mode | gap_mode, block, heap_no, index, thr);

  lock_mutex_exit_kernel();

  ut_ad(lock_rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err lock_clust_rec_read_check_and_lock(
  ulint flags, const buf_block_t *block, const rec_t *rec, dict_index_t *index, const ulint *offsets, enum Lock_mode mode,
  ulint gap_mode, que_thr_t *thr
) {

  db_err err;
  ulint heap_no;

  ut_ad(dict_index_is_clust(index));
  ut_ad(block->m_frame == page_align(rec));
  ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));
  ut_ad(gap_mode == LOCK_ORDINARY || gap_mode == LOCK_GAP || gap_mode == LOCK_REC_NOT_GAP);
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  heap_no = page_rec_get_heap_no(rec);

  lock_mutex_enter_kernel();

  ut_ad(mode != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
  ut_ad(mode != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));

  if (likely(heap_no != PAGE_HEAP_NO_SUPREMUM)) {

    lock_rec_convert_impl_to_expl(block, rec, index, offsets);
  }

  err = lock_rec_lock(false, mode | gap_mode, block, heap_no, index, thr);

  lock_mutex_exit_kernel();

  ut_ad(lock_rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err lock_clust_rec_read_check_and_lock_alt(
  ulint flags, const buf_block_t *block, const rec_t *rec, dict_index_t *index, enum Lock_mode mode, ulint gap_mode, que_thr_t *thr
) {

  mem_heap_t *tmp_heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  db_err err;
  rec_offs_init(offsets_);

  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &tmp_heap);
  err = lock_clust_rec_read_check_and_lock(flags, block, rec, index, offsets, mode, gap_mode, thr);
  if (tmp_heap) {
    mem_heap_free(tmp_heap);
  }
  return err;
}

ulint lock_get_type(const Lock *lock) {
  return lock_get_type_low(lock);
}

uint64_t lock_get_trx_id(const Lock *lock) {
  return trx_get_id(lock->trx);
}

const char *lock_get_mode_str(const Lock *lock) {
  bool is_gap_lock;

  is_gap_lock = lock_get_type_low(lock) == LOCK_REC && lock_rec_get_gap(lock);

  switch (lock_get_mode(lock)) {
    case LOCK_S:
      if (is_gap_lock) {
        return "S,GAP";
      } else {
        return "S";
      }
    case LOCK_X:
      if (is_gap_lock) {
        return "X,GAP";
      } else {
        return "X";
      }
    case LOCK_IS:
      if (is_gap_lock) {
        return "IS,GAP";
      } else {
        return "IS";
      }
    case LOCK_IX:
      if (is_gap_lock) {
        return "IX,GAP";
      } else {
        return "IX";
      }
    case LOCK_AUTO_INC:
      return "AUTO_INC";
    default:
      return "UNKNOWN";
  }
}

const char *lock_get_type_str(const Lock *lock) {
  switch (lock_get_type_low(lock)) {
    case LOCK_REC:
      return "RECORD";
    case LOCK_TABLE:
      return "TABLE";
    default:
      return "UNKNOWN";
  }
}

/** Gets the table on which the lock is.
@param[in] lock                 Get the table that owns this lock.
@return	table */
static dict_table_t *lock_get_table(const Lock *lock) {
  switch (lock_get_type_low(lock)) {
    case LOCK_REC:
      return lock->index->table;
    case LOCK_TABLE:
      return lock->un_member.tab_lock.table;
    default:
      ut_error;
      return nullptr;
  }
}

uint64_t lock_get_table_id(const Lock *lock) {
  auto table = lock_get_table(lock);

  return table->id;
}

const char *lock_get_table_name(const Lock *lock) {
  auto table = lock_get_table(lock);

  return table->name;
}

const dict_index_t *lock_rec_get_index(const Lock *lock) {
  ut_a(lock_get_type_low(lock) == LOCK_REC);

  return lock->index;
}

const char *lock_rec_get_index_name(const Lock *lock) {
  ut_a(lock_get_type_low(lock) == LOCK_REC);

  return lock->index->name;
}

ulint lock_rec_get_space_id(const Lock *lock) {
  ut_a(lock_get_type_low(lock) == LOCK_REC);

  return lock->un_member.rec_lock.space;
}

ulint lock_rec_get_page_no(const Lock *lock) {
  ut_a(lock_get_type_low(lock) == LOCK_REC);

  return lock->un_member.rec_lock.page_no;
}

#ifdef UNIT_TESTING
bool lock_trx_has_no_waiters(const trx_t *trx) {
  mutex_enter(&kernel_mutex);

  for (auto lock = UT_LIST_GET_LAST(trx->trx_locks);
       lock != nullptr;
       lock = UT_LIST_GET_PREV(trx_locks, lock)) {

    if (lock_get_type_low(lock) == LOCK_REC) {

      auto space = lock->un_member.rec_lock.space;
      auto page_no = lock->un_member.rec_lock.page_no;

      {
        /* Check for waiting locks. */
        for (auto rec_lock = lock_rec_get_first_on_page_addr(space, page_no);
             rec_lock != nullptr;
             rec_lock = lock_rec_get_next_on_page(rec_lock)) {

          if (lock_get_wait(rec_lock)) {
            mutex_exit(&kernel_mutex);
            return true;
          }
        }
      }

    } else {

      ut_ad(lock_get_type_low(lock) & LOCK_TABLE);

      for (auto table_lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
           table_lock != nullptr;
           table_lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, table_lock)) {

        if (lock_get_wait(table_lock)) {
          mutex_exit(&kernel_mutex);
          return true;
        }
      }
    }
  }

  mutex_exit(&kernel_mutex);

  return false;
}
#endif /* UNIT_TESTING */
