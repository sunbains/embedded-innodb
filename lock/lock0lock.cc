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

/** @file lock/lock0lock.c
The transaction lock system

Created 5/7/1996 Heikki Tuuri
*******************************************************/

#include "lock0lock.h"

#include "api0ucode.h"
#include "trx0purge.h"

/** Restricts the length of search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK = 1000000;

/** Restricts the recursion depth of the search we will do in the waits-for
graph of transactions */
constexpr ulint LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK = 200;

/** When releasing transaction locks, this specifies how often we release
the kernel mutex for a moment to give also others access to it */
constexpr ulint LOCK_RELEASE_KERNEL_INTERVAL = 1000;

/** Safety margin when creating a new record lock: this many extra records
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
 *    IS IX S  X
 * IS +	 +  +  -
 * IX +	 +  -  -
 * S  +	 -  +  -
 * X  -	 -  -  -
 *
 * Note that for rows, InnoDB only acquires S or X locks.
 * For tables, InnoDB normally acquires IS or IX locks.
 * S or X table locks are only acquired for LOCK TABLES.
 */

/**
 * Note that for rows, InnoDB only acquires S or X locks.
 * For tables, InnoDB normally acquires IS or IX locks.
 * S or X table locks are only acquired for LOCK TABLES.
 */
static const bool Lock_compatibility_matrix[4][4] = {
  /**         IS     IX       S     X   */
  /* IS */ {true, true, true, false},
  /* IX */ {true, true, false, false},
  /* S  */ {true, false, true, false},
  /* X  */ {false, false, false, false},
};

/**
 * Define the lock compatibility matrix in a bool.  The first line below
 * defines the diagonal entries. The following lines define the compatibility
 * for LOCK_IX, LOCK_S, since the matrix is symmetric. */

/* STRONGER-OR-EQUAL RELATION (mode1=row, mode2=column)
 *    IS IX S  X
 * IS +  -  -  -
 * IX +  +  -  -
 * S  +  -  +  -
 * X  +  +  +  +
 */

/** Define the stronger-or-equal lock relation in a ulint.  This relation
contains all pairs LK(mode1, mode2) where mode1 is stronger than or
equal to mode2. */
static const bool Lock_strength_matrix[4][4] = {
  /**         IS     IX       S     X    */
  /* IS */ {true, false, false, false},
  /* IX */ {true, true, false, false},
  /* S  */ {true, false, true, false},
  /* X  */ {true, true, true, true},
};

/* The lock system */
Lock_sys *srv_lock_sys{};

/**
 * @brief Validate the transaction state.
 *
 * @param[in] trx The transaction to validate.
 */
static inline void validate_trx_state(const Trx *trx) noexcept {
  switch (trx->m_conc_state) {
    case TRX_ACTIVE:
    case TRX_PREPARED:
    case TRX_COMMITTED_IN_MEMORY:
      break;
    default:
      ut_error;
  }
}

/** We store info on the latest deadlock error to this buffer. InnoDB
Monitor will then fetch it and print */
bool lock_deadlock_found{};

struct Table_lock_get_node {
  /**
   * @brief Functor for accessing the embedded node within a table lock.
   *
   * @param[in] lock The lock to get the node from.
   *
   * @return The embedded node within the lock.
   */
  static const ut_list_node<Lock> &get_node(const Lock &lock) { return lock.m_table.m_locks; }
};

struct Rec_lock_get_node {
  /**
   * @brief Functor for accessing the embedded node within a table lock.
   *
   * @param[in] lock The lock to get the node from.
   *
   * @return The embedded node within the lock.
   */
  static const ut_list_node<Lock> &get_node(const Lock &lock) { return lock.m_rec.m_rec_locks; }
};

/* Flags for recursive deadlock search */
constexpr ulint LOCK_VICTIM_IS_START = 1;
constexpr ulint LOCK_VICTIM_IS_OTHER = 2;
constexpr ulint LOCK_EXCEED_MAX_DEPTH = 3;

trx_id_t Lock::trx_id() const noexcept {
  return m_trx->m_id;
}

void Lock::set_trx_wait(Trx *trx) noexcept {
  ut_ad(trx->m_wait_lock == nullptr);

  trx->m_wait_lock = this;
  m_type_mode = Lock_mode(Lock_mode_type(m_type_mode) | LOCK_WAIT);
}

void Lock::reset() noexcept {
  ut_ad(is_waiting());
  ut_ad(m_trx->m_wait_lock == this);

  /* Reset the back pointer in trx to this waiting lock request */

  m_type_mode = Lock_mode(Lock_mode_type(m_type_mode) & ~LOCK_WAIT);
  m_trx->m_wait_lock = nullptr;
}

bool Lock::mode_stronger_or_eq(Lock_mode lhs, Lock_mode rhs) noexcept {
  ut_ad(lhs == LOCK_X || lhs == LOCK_S || lhs == LOCK_IX || lhs == LOCK_IS);
  ut_ad(rhs == LOCK_X || rhs == LOCK_S || rhs == LOCK_IX || rhs == LOCK_IS);

  return Lock_strength_matrix[lhs][rhs];
}

bool Lock::mode_compatible(Lock_mode lhs, Lock_mode rhs) noexcept {
  ut_ad(lhs == LOCK_X || lhs == LOCK_S || lhs == LOCK_IX || lhs == LOCK_IS);
  ut_ad(rhs == LOCK_X || rhs == LOCK_S || rhs == LOCK_IX || rhs == LOCK_IS);

  return Lock_compatibility_matrix[lhs][rhs];
}

bool Lock::rec_blocks(const Trx *trx, Lock_mode_type type_mode, bool lock_is_on_supremum) const noexcept {
  ut_ad(type() == LOCK_REC);

  if (trx != m_trx && !mode_compatible(Lock_mode(LOCK_MODE_MASK & type_mode), mode())) {

    /* We have somewhat complex rules when gap type record locks cause waits */

    if ((lock_is_on_supremum || (type_mode & LOCK_GAP)) && !(type_mode & LOCK_INSERT_INTENTION)) {

      /* Gap type locks without LOCK_INSERT_INTENTION flag do not need to wait for anything. This is because
      different users can have conflicting lock types on gaps. */

      return false;
    }

    if (!(type_mode & LOCK_INSERT_INTENTION) && rec_is_gap()) {

      /* Record lock (LOCK_ORDINARY or LOCK_REC_NOT_GAP does not need to wait for a gap type lock */

      return false;
    }

    if ((type_mode & LOCK_GAP) && rec_is_not_gap()) {

      /* Lock on gap does not need to wait for a LOCK_REC_NOT_GAP type lock */

      return false;
    }

    /* No lock request needs to wait for an insert intention lock to be removed. This is ok since our
    rules allow conflicting locks on gaps. This eliminates a spurious deadlock caused by a next-key lock waiting
    for an insert intention lock; when the insert intention lock was granted, the insert deadlocked on
    the waiting next-key lock.

    Also, insert intention locks do not disturb each other. */

    return !rec_is_insert_intention();

  } else {

    return false;
  }
}

bool Lock::has_to_wait_for(const Lock *lock, ulint heap_no) const noexcept {
  ut_ad(type() == LOCK_REC || heap_no == ULINT_UNDEFINED);

  if (m_trx != lock->m_trx && !mode_compatible(mode(), lock->mode())) {
    if (type() == LOCK_REC) {
      ut_ad(lock->type() == LOCK_REC);
      ut_ad(rec_is_nth_bit_set(heap_no));
      ut_ad(lock->rec_is_nth_bit_set(heap_no));

      return lock->rec_blocks(m_trx, precise_mode(), heap_no == PAGE_HEAP_NO_SUPREMUM);

    } else {
      return true;
    }
  } else {
    return false;
  }
}

Lock *Lock::rec_clone(mem_heap_t *heap) const noexcept {
  ut_ad(type() == LOCK_REC);

  const auto size = sizeof(Lock) + rec_get_n_bits() / 8;

  return static_cast<Lock *>(mem_heap_dup(heap, this, size));
}

uint64_t Lock::table_id() const noexcept {
  ut_ad(type() == LOCK_TABLE);
  return table()->m_id;
}

const char *Lock::table_name() const noexcept {
  ut_ad(type() == LOCK_TABLE);
  return table()->m_name;
}

const char *Lock::rec_index_name() const noexcept {
  ut_a(type() == LOCK_REC);
  return m_rec.m_index->m_name;
}

const char *Lock::get_mode_str() const noexcept {
  const auto is_gap_lock = type() == LOCK_REC && rec_is_gap();

  switch (mode()) {
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
    default:
      return "UNKNOWN";
  }
}

const char *Lock::get_type_str() const noexcept {
  switch (type()) {
    case LOCK_REC:
      return "RECORD";
    case LOCK_TABLE:
      return "TABLE";
    default:
      return "UNKNOWN";
  }
}

const Table *Lock::table() const noexcept {
  switch (type()) {
    case LOCK_REC:
      return m_rec.m_index->m_table;
    case LOCK_TABLE:
      return m_table.m_table;
    default:
      ut_error;
      return nullptr;
  }
}

std::string Lock::table_to_string() const noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(type() == LOCK_TABLE);

  auto str = std::format("TABLE LOCK table {} trx id {}", m_table.m_table->m_name, m_trx->m_id);

  if (mode() == LOCK_S) {
    str += " lock mode S";
  } else if (mode() == LOCK_X) {
    str += " lock mode X";
  } else if (mode() == LOCK_IS) {
    str += " lock mode IS";
  } else if (mode() == LOCK_IX) {
    str += " lock mode IX";
  } else {
    str += std::format(" unknown lock mode {}", Lock_mode_type(mode()));
  }

  if (is_waiting()) {
    str += " waiting";
  }

  return str;
}

std::string Lock::rec_to_string(Buf_pool *buf_pool) const noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(type() == LOCK_REC);

  mem_heap_t *heap{};

  auto str = std::format(
    "RECORD LOCKS page id {} n bits {} index {{ {}, {} }} of table {} trx id {}",
    page_id().to_string(),
    rec_get_n_bits(),
    rec_index()->m_id,
    rec_index()->m_name,
    rec_index()->m_table->m_name,
    m_trx->m_id
  );

  if (mode() == LOCK_S) {
    str += " lock mode S";
  } else if (mode() == LOCK_X) {
    str += " Lock_mode X";
  } else {
    ut_error;
  }

  if (rec_is_gap()) {
    str += " locks gap before rec";
  }

  if (rec_is_not_gap()) {
    str += " locks rec but not gap";
  }

  if (rec_is_insert_intention()) {
    str += " insert intention";
  }

  if (is_waiting()) {
    str += " waiting";
  }

  mtr_t mtr;

  mtr.start();

  Buf_pool::Request req{.m_page_id = page_id(), .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr};

  auto block = buf_pool->try_get_by_page_id(req);

  str += " heap no ";

  if (block != nullptr) {
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();
    rec_offs_init(offsets_);

    for (ulint i{}; i < rec_get_n_bits(); ++i) {

      if (rec_is_nth_bit_set(i)) {

        const rec_t *rec = page_find_rec_with_heap_no(block->get_frame(), i);

        {
          Phy_rec record{m_rec.m_index, rec};

          offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
        }

        str += std::format("{} ", i);
        str += ::rec_to_string(rec);
      }
    }
  } else {
    for (ulint i{}; i < rec_get_n_bits(); ++i) {
      str += std::format("{} ", i);
    }
  }
  str += "\n";

  mtr.commit();

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return str;
}

std::string Lock::to_string(Buf_pool *buf_pool) const noexcept {
  return std::format(
    "Lock {} type: {} mode: {}, - {}",
    (void *)this,
    get_type_str(),
    get_mode_str(),
    type() == LOCK_REC ? rec_to_string(buf_pool) : table_to_string()
  );
}

Lock_sys::Lock_sys(Trx_sys *trx_sys, ulint n_cells) noexcept : m_buf_pool{trx_sys->m_fsp->m_buf_pool}, m_trx_sys{trx_sys} {}

Lock_sys::~Lock_sys() noexcept {}

bool Lock_sys::check_trx_id_sanity(
  trx_id_t trx_id, const rec_t *rec, const Index *index, const ulint *offsets, bool has_kernel_mutex
) noexcept {
  bool is_ok{true};

  ut_ad(rec_offs_validate(rec, index, offsets));

  if (!has_kernel_mutex) {
    mutex_enter(&kernel_mutex);
  }

  /* A sanity check: the trx_id in rec must be smaller than the global trx id counter */

  if (trx_id >= m_trx_sys->m_max_trx_id) {
    log_err("Transaction id associated with record");
    log_err(rec_to_string(rec));
    log_err(std::format(
      "\nis {} which is higher than the global trx id counter {}"
      " The table is corrupt. You have to do dump + drop + reimport.",
      trx_id,
      m_trx_sys->m_max_trx_id
    ));

    is_ok = false;
  }

  if (!has_kernel_mutex) {
    mutex_exit(&kernel_mutex);
  }

  return is_ok;
}

bool Lock_sys::clust_rec_cons_read_sees(const rec_t *rec, Index *index, const ulint *offsets, read_view_t *view) const noexcept {
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  /* NOTE that we call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  kernel mutex here! */

  const auto trx_id = row_get_rec_trx_id(rec, index, offsets);

  return read_view_sees_trx_id(view, trx_id);
}

bool Lock_sys::sec_rec_cons_read_sees(const rec_t *rec, read_view_t *view) const noexcept {
  ut_ad(page_rec_is_user_rec(rec));

  /* NOTE that we might call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  kernel mutex here! */

  if (recv_recovery_on) {
    return false;
  }

  const auto max_trx_id = page_get_max_trx_id(page_align(rec));
  ut_ad(max_trx_id > 0);

  return max_trx_id < view->up_limit_id;
}

Table *Lock_sys::get_src_table(Trx *trx, Table *dest, Lock_mode *mode) noexcept {
  Table *src{};

  *mode = LOCK_NONE;

  for (auto lock : trx->m_trx_locks) {
    if (!(lock->type() & LOCK_TABLE)) {
      /* We are only interested in table locks. */
      continue;
    }

    auto &tab_lock = lock->m_table;

    if (dest == tab_lock.m_table) {
      /* We are not interested in the destination table. */
      continue;
    } else if (src == nullptr) {
      /* This presumably is the source table. */
      src = tab_lock.m_table;

      if (src->m_locks.size() != 1 || src->m_locks.front() != lock) {
        /* We only support the case when
        there is only one lock on this table. */
        return nullptr;
      }
    } else if (src != tab_lock.m_table) {
      /* The transaction is locking more than
      two tables (src and dest): abort */
      return nullptr;
    }

    /* Check that the source table is locked by LOCK_IX or LOCK_IS. */
    const auto Lock_mode = lock->mode();

    if (Lock_mode == LOCK_IX || Lock_mode == LOCK_IS) {
      if (*mode != LOCK_NONE && *mode != Lock_mode) {
        /* There are multiple locks on src. */
        return nullptr;
      }

      *mode = Lock_mode;
    }
  }

  if (src == nullptr) {
    /* No source table lock found: flag the situation to caller */
    src = dest;
  }

  return src;
}

bool Lock_sys::is_table_exclusive(Table *table, Trx *trx) noexcept {
  mutex_enter(&kernel_mutex);

  for (auto lock : table->m_locks) {
    if (lock->m_trx != trx) {
      /* A lock on the table is held by some other transaction.
      Other table locks than LOCK_IX are not allowed. */
      break;

    } else if (lock->type() != LOCK_TABLE) {
      /* We are interested in table locks only. */
    } else {
      switch (lock->mode()) {
        case LOCK_IX:
          mutex_exit(&kernel_mutex);
          return true;
        default:
          /* Other table locks than LOCK_IX are not allowed. */
          mutex_exit(&kernel_mutex);
          return false;
      }
    }
  }

  mutex_exit(&kernel_mutex);

  return false;
}

#ifdef UNIV_DEBUG
const Lock *Lock_sys::rec_exists(const Rec_locks &rec_locks, ulint heap_no) const noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  for (auto lock : rec_locks) {
    if (lock->rec_is_nth_bit_set(heap_no)) {
      return lock;
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

const Lock *Lock_sys::rec_get_prev(const Lock *in_lock, ulint heap_no) noexcept {
  ut_ad(in_lock != nullptr);
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(in_lock->type() == LOCK_REC);

  if (auto it = m_rec_locks.find(in_lock->page_id()); likely(it != m_rec_locks.end())) {

    Lock *prev_lock{};

    for (auto lock : it->second) {
      ut_ad(lock->type() == LOCK_REC);

      if (lock == in_lock) {
        return prev_lock;
      }

      if (lock->rec_is_nth_bit_set(heap_no)) {
        prev_lock = lock;
      }
    }
  }

  return nullptr;
}

Lock *Lock_sys::table_has(Trx *trx, Table *table, Lock_mode mode) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* Look for stronger locks the same trx already has on the table */

  for (auto lock = UT_LIST_GET_LAST(table->m_locks); lock != nullptr; lock = UT_LIST_GET_PREV(m_table.m_locks, lock)) {

    if (lock->m_trx == trx && Lock::mode_stronger_or_eq(lock->mode(), mode)) {

      /* The same trx already has locked the table in
      a mode stronger or equal to the mode given */

      ut_ad(!lock->is_waiting());

      return lock;
    }
  }

  return nullptr;
}

const Lock *Lock_sys::rec_has_expl(Page_id page_id, ulint precise_mode, ulint heap_no, const Trx *trx) const noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((precise_mode & LOCK_MODE_MASK) == LOCK_S || (precise_mode & LOCK_MODE_MASK) == LOCK_X);
  ut_ad(!(precise_mode & LOCK_INSERT_INTENTION));

  if (auto it = m_rec_locks.find(page_id); likely(it != m_rec_locks.end())) {
    for (auto lock : it->second) {

      if (lock->m_trx == trx && lock->rec_is_nth_bit_set(heap_no) &&
          Lock::mode_stronger_or_eq(lock->mode(), Lock_mode(precise_mode & LOCK_MODE_MASK)) && !lock->is_waiting() &&
          (!lock->rec_is_not_gap() || (precise_mode & LOCK_REC_NOT_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
          (!lock->rec_is_gap() || (precise_mode & LOCK_GAP) || heap_no == PAGE_HEAP_NO_SUPREMUM) &&
          (!lock->rec_is_insert_intention())) {

        return lock;
      }
    }
  }

  return nullptr;
}

#ifdef UNIV_DEBUG
const Lock *Lock_sys::rec_other_has_expl_req(Page_id page_id, Lock_mode mode, ulint gap, ulint wait, ulint heap_no, const Trx *trx)
  const noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(mode == LOCK_X || mode == LOCK_S);
  ut_ad(gap == 0 || gap == LOCK_GAP);
  ut_ad(wait == 0 || wait == LOCK_WAIT);

  if (auto it = m_rec_locks.find(page_id); likely(it != m_rec_locks.end())) {
    for (auto lock : it->second) {

      if (lock->m_trx != trx && lock->rec_is_nth_bit_set(heap_no) &&
          (gap || !(lock->rec_is_gap() || heap_no == PAGE_HEAP_NO_SUPREMUM)) && (wait || !lock->is_waiting()) &&
          Lock::mode_stronger_or_eq(lock->mode(), mode)) {

        return lock;
      }
    }
  }

  return nullptr;
}
#endif /* UNIV_DEBUG */

Lock *Lock_sys::rec_other_has_conflicting(Page_id page_id, Lock_mode mode, ulint heap_no, Trx *trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  if (auto it = m_rec_locks.find(page_id); likely(it != m_rec_locks.end())) {
    for (auto lock : it->second) {
      if (unlikely(lock->rec_is_nth_bit_set(heap_no) && lock->rec_blocks(trx, mode, heap_no == PAGE_HEAP_NO_SUPREMUM))) {
        return lock;
      }
    }
  }

  return nullptr;
}

Lock *Lock_sys::rec_find_similar_on_page(Lock_mode type_mode, ulint heap_no, Lock *lock, const Trx *trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  for (/* No op */; lock != nullptr; lock = lock->next()) {
    if (lock->m_trx == trx && lock->m_type_mode == type_mode && lock->rec_get_n_bits() > heap_no) {
      return lock;
    }
  }

  return nullptr;
}

Trx *Lock_sys::sec_rec_some_has_impl_off_kernel(const rec_t *rec, const Index *index, const ulint *offsets) noexcept {
  const page_t *page = page_align(rec);

  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(!index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list, or
  database recovery is running. We do not write the changes of a page
  max trx id to the log, and therefore during recovery, this value
  for a page may be incorrect. */

  if (page_get_max_trx_id(page) < m_trx_sys->get_min_trx_id() && !recv_recovery_on) {

    return nullptr;
  }

  /* Ok, in this case it is possible that some transaction has an
  implicit x-lock. We have to look in the clustered index. */

  if (!check_trx_id_sanity(page_get_max_trx_id(page), rec, index, offsets, true)) {
    buf_page_print(page, 0);

    /* The page is corrupt: try to avoid a crash by returning
    nullptr */
    return nullptr;
  }

  return srv_row_vers->impl_x_locked_off_kernel(rec, index, offsets);
}

Lock *Lock_sys::rec_create_low(
  Page_id page_id, Lock_mode type_mode, ulint heap_no, ulint n_bits, const Index *index, Trx *trx
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* If rec is the supremum record, then we reset the gap and
  LOCK_REC_NOT_GAP bits, as all locks on the supremum are
  automatically of the gap type */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(type_mode & LOCK_REC_NOT_GAP));

    type_mode = Lock_mode(type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP));
  }

  /* Make lock bitmap bigger by a safety margin */
  const auto n_bytes = 1 + (n_bits + LOCK_PAGE_BITMAP_MARGIN) / 8;

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->m_lock_heap, sizeof(Lock) + n_bytes));

  trx->m_trx_locks.push_back(lock);

  lock->m_trx = trx;

  lock->m_type_mode = Lock_mode((type_mode & ~LOCK_TYPE_MASK) | LOCK_REC);

  lock->m_rec.m_index = index;
  lock->m_rec.m_page_id = page_id;
  lock->m_n_bits = n_bytes * 8;

  /* Reset to zero the bitmap which resides immediately after the lock struct */

  lock->rec_bitmap_reset();

  /* Set the bit corresponding to rec */
  lock->rec_set_nth_bit(heap_no);

  auto it = m_rec_locks.find(lock->m_rec.m_page_id);

  if (it == m_rec_locks.end()) {
    Rec_locks rec_locks;

    rec_locks.push_back(lock);

    auto [it, inserted] = m_rec_locks.emplace(lock->m_rec.m_page_id, std::move(rec_locks));
    ut_a(inserted);

  } else {
    it->second.push_back(lock);
  }

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock->set_trx_wait(trx);
  }

  return lock;
}

Lock *Lock_sys::rec_create(
  Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto page = block->m_frame;
  const auto n_bits = page_dir_get_n_heap(page);

  return rec_create_low(block->get_page_id(), type_mode, heap_no, n_bits, index, const_cast<Trx *>(trx));
}

db_err Lock_sys::rec_enqueue_waiting(
  Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* Test if there already is some other reason to suspend thread:
  we do not enqueue a lock request if the query thread should be
  stopped anyway */

  if (unlikely(que_thr_stop(thr))) {

    ut_error;

    return DB_QUE_THR_SUSPENDED;
  }

  auto trx = thr_get_trx(thr);

  switch (trx->get_dict_operation()) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      log_err(std::format(
        "A record lock wait happens in a dictionary operation! trx_id: {} index: {} table: {} index_id: {} index_name: {}",
        trx->m_id,
        index->m_name,
        index->get_table_name(),
        index->m_id,
        index->m_name
      ));
  }

  /* Enqueue the lock request that will wait to be granted */
  auto lock = rec_create(Lock_mode(type_mode | LOCK_WAIT), block, heap_no, index, trx);

  /* Check if a deadlock occurs: if yes, remove the lock request and
  return an error code */

  if (unlikely(deadlock_occurs(lock, trx))) {

    lock->reset();
    lock->rec_reset_nth_bit(heap_no);

    return DB_DEADLOCK;

  } else if (unlikely(trx->m_wait_lock == nullptr)) {
    /* If there was a deadlock but we chose another transaction as a
    victim, it is possible that we already have the lock now granted! */

    return DB_SUCCESS;

  } else {

    trx->m_wait_started = time(nullptr);
    trx->m_que_state = TRX_QUE_LOCK_WAIT;
    trx->m_was_chosen_as_deadlock_victim = false;

    const auto success = que_thr_stop(thr);
    ut_a(success);

    return DB_LOCK_WAIT;
  }
}

Lock *Lock_sys::rec_add_to_queue(
  Lock_mode type_mode, const Buf_block *block, ulint heap_no, const Index *index, const Trx *trx
) noexcept {
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
    auto mode = (type_mode & LOCK_MODE_MASK) == LOCK_S ? LOCK_X : LOCK_S;
    auto other_lock = rec_other_has_expl_req(block->get_page_id(), mode, 0, LOCK_WAIT, heap_no, trx);
    ut_a(other_lock == nullptr);
  }
#endif /* UNIV_DEBUG */

  type_mode = Lock_mode(type_mode | LOCK_REC);

  /* If rec is the supremum record, then we can reset the gap bit, as
  all locks on the supremum are automatically of the gap type, and we
  try to avoid unnecessary memory consumption of a new record lock
  struct for a gap type lock */

  if (unlikely(heap_no == PAGE_HEAP_NO_SUPREMUM)) {
    ut_ad(!(Lock_mode_type(type_mode) & LOCK_REC_NOT_GAP));

    /* There should never be LOCK_REC_NOT_GAP on a supremum
    record, but let us play safe */

    type_mode = Lock_mode(type_mode & ~(LOCK_GAP | LOCK_REC_NOT_GAP));
  }

  /* Look for a waiting lock request on the same record or on a gap */

  auto it = m_rec_locks.find(block->get_page_id());

  if (it != m_rec_locks.end()) {
    for (auto lock : it->second) {
      if (lock->is_waiting() && lock->rec_is_nth_bit_set(heap_no)) {
        return rec_create(type_mode, block, heap_no, index, trx);
      }
    }
  }

  if (likely(!(type_mode & LOCK_WAIT))) {

    /* Look for a similar record lock on the same page:
    if one is found and there are no waiting lock requests,
    we can just set the bit */

    auto lock = it != m_rec_locks.end() ? it->second.front() : nullptr;

    lock = rec_find_similar_on_page(type_mode, heap_no, lock, trx);

    if (likely(lock != nullptr)) {

      lock->rec_set_nth_bit(heap_no);

      return lock;
    }
  }

  return rec_create(type_mode, block, heap_no, index, trx);
}

bool Lock_sys::rec_lock_fast(
  bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || table_has(thr_get_trx(thr), index->m_table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || table_has(thr_get_trx(thr), index->m_table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  auto trx = thr_get_trx(thr);
  auto it = m_rec_locks.find(block->get_page_id());

  if (it == m_rec_locks.end()) {

    if (!impl) {
      (void)rec_create(mode, block, heap_no, index, trx);
    }

    return true;
  }

  auto lock = it->second.front();

  if (lock->next() != nullptr) {

    return false;
  }

  if (lock->m_trx != trx || lock->m_type_mode != (mode | LOCK_REC) || lock->rec_get_n_bits() <= heap_no) {

    return false;
  }

  if (!impl) {
    /* If the nth bit of the record lock is already set then we
    do not set a new lock bit, otherwise we do set */

    if (!lock->rec_is_nth_bit_set(heap_no)) {
      lock->rec_set_nth_bit(heap_no);
    }
  }

  return true;
}

db_err Lock_sys::rec_lock_slow(
  bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || table_has(thr_get_trx(thr), index->m_table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || table_has(thr_get_trx(thr), index->m_table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);
  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == 0 ||
    mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP
  );

  db_err err;
  auto trx = thr_get_trx(thr);

  if (rec_has_expl(block->get_page_id(), mode, heap_no, trx)) {
    /* The trx already has a strong enough lock on rec: do
    nothing */

    err = DB_SUCCESS;
  } else if (rec_other_has_conflicting(block->get_page_id(), Lock_mode(mode), heap_no, trx)) {

    /* If another transaction has a non-gap conflicting request in
    the queue, as this transaction does not have a lock strong
    enough already granted on the record, we have to wait. */

    err = rec_enqueue_waiting(mode, block, heap_no, index, thr);

  } else {

    if (!impl) {
      /* Set the requested lock on the record */

      (void)rec_add_to_queue(Lock_mode(LOCK_REC | mode), block, heap_no, index, trx);
    }

    err = DB_SUCCESS;
  }

  return err;
}

db_err Lock_sys::rec_lock(
  bool impl, Lock_mode mode, const Buf_block *block, ulint heap_no, const Index *index, que_thr_t *thr
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_S || table_has(thr_get_trx(thr), index->m_table, LOCK_IS));
  ut_ad((LOCK_MODE_MASK & mode) != LOCK_X || table_has(thr_get_trx(thr), index->m_table, LOCK_IX));
  ut_ad((LOCK_MODE_MASK & mode) == LOCK_S || (LOCK_MODE_MASK & mode) == LOCK_X);

  ut_ad(
    mode - (LOCK_MODE_MASK & mode) == LOCK_GAP || mode - (LOCK_MODE_MASK & mode) == LOCK_REC_NOT_GAP ||
    mode - (LOCK_MODE_MASK & mode) == 0
  );

  db_err err;

  if (rec_lock_fast(impl, mode, block, heap_no, index, thr)) {

    /* We try a simplified and faster subroutine for the most common cases */

    err = DB_SUCCESS;

  } else {
    err = rec_lock_slow(impl, mode, block, heap_no, index, thr);
  }

  return err;
}

bool Lock_sys::rec_has_to_wait_in_queue(const Rec_locks &rec_locks, Lock *waiting_lock, ulint heap_no) const noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(waiting_lock->is_waiting());
  ut_ad(waiting_lock->type() == LOCK_REC);
  ut_ad(heap_no == waiting_lock->rec_find_set_bit());

  for (auto lock = rec_locks.front(); lock != waiting_lock; lock = lock->next()) {

    ut_ad(lock->type() == LOCK_REC);
    ut_ad(lock->page_id() == waiting_lock->page_id());

    if (heap_no < lock->rec_get_n_bits() && lock->rec_is_nth_bit_set(heap_no) && waiting_lock->has_to_wait_for(lock, heap_no)) {
      return true;
    }
  }

  return false;
}

void Lock_sys::grant(Lock *lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  lock->reset();

  /* If we are resolving a deadlock by choosing another transaction
  as a victim, then our original transaction may not be in the
  TRX_QUE_LOCK_WAIT state, and there is no need to end the lock wait
  for it */

  if (lock->m_trx->m_que_state == TRX_QUE_LOCK_WAIT) {
    lock->m_trx->end_lock_wait();
  }
}

void Lock_sys::rec_cancel(Lock *lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock->type() == LOCK_REC);

  /* Reset the bit (there can be only one set bit) in the lock bitmap */
  lock->rec_reset_nth_bit(lock->rec_find_set_bit());

  /* Reset the wait flag and the back pointer to lock in trx */
  lock->reset();

  /* The following function releases the trx from lock wait */
  lock->m_trx->end_lock_wait();
}

void Lock_sys::rec_dequeue_from_page(Lock *lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(lock->type() == LOCK_REC);

  auto it = m_rec_locks.find(lock->page_id());
  ut_a(it != m_rec_locks.end());

  auto &rec_locks = it->second;
  ut_a(!rec_locks.empty());

  rec_locks.remove(lock);

  bool rec_locks_empty{};

  if (rec_locks.empty()) {
    m_rec_locks.erase(it);
    rec_locks_empty = true;
  }

  auto trx = lock->m_trx;

  trx->m_trx_locks.remove(lock);

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  if (!rec_locks_empty) {
    for (auto lock : rec_locks) {
      if (lock->is_waiting() && !rec_has_to_wait_in_queue(rec_locks, lock, lock->rec_find_set_bit())) {
        /* Grant the lock */
        grant(lock);
      }
    }
  }
}

void Lock_sys::rec_discard(Lock *in_lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(in_lock->type() == LOCK_REC);

  auto trx = in_lock->m_trx;
  const auto n = m_rec_locks.erase(in_lock->page_id());
  ut_a(n == 1);

  trx->m_trx_locks.remove(in_lock);
}

void Lock_sys::rec_free_all_from_discard_page(Page_id page_id) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  if (auto it = m_rec_locks.find(page_id); it != m_rec_locks.end()) {
    auto lock = it->second.front();

    while (lock != nullptr) {
      ut_ad(lock->rec_find_set_bit() == ULINT_UNDEFINED);
      ut_ad(!lock->is_waiting());

      auto next_lock = lock->next();

      rec_discard(lock);

      lock = next_lock;
    }
  }
}

void Lock_sys::rec_reset_and_release_wait(Page_id page_id, ulint heap_no) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  if (auto it = m_rec_locks.find(page_id); it != m_rec_locks.end()) {
    for (auto lock : it->second) {
      if (lock->rec_is_nth_bit_set(heap_no)) {

        if (lock->is_waiting()) {
          rec_cancel(lock);
        } else {
          lock->rec_reset_nth_bit(heap_no);
        }
      }
    }
  }
}

void Lock_sys::rec_inherit_to_gap(const Buf_block *heir_block, const Buf_block *block, ulint heir_heap_no, ulint heap_no) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* If session is using READ COMMITTED isolation level, we do not want locks set by an UPDATE or a DELETE to be inherited as gap type locks.
  But we DO want S-locks set by a consistency constraint to be inherited also then. */
  if (auto it = m_rec_locks.find(block->get_page_id()); it != m_rec_locks.end()) {
    for (auto lock : it->second) {
      if (lock->rec_is_nth_bit_set(heap_no) && !lock->rec_is_insert_intention() &&
          lock->m_trx->m_isolation_level != TRX_ISO_READ_COMMITTED && lock->mode() == LOCK_X) {
        (void)rec_add_to_queue(
          Lock_mode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), heir_block, heir_heap_no, lock->rec_index(), lock->m_trx
        );
      }
    }
  }
}

void Lock_sys::rec_inherit_to_gap_if_gap_lock(const Buf_block *block, ulint heir_heap_no, ulint heap_no) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  if (auto it = m_rec_locks.find(block->get_page_id()); it != m_rec_locks.end()) {
    for (auto lock : it->second) {
      if (lock->rec_is_nth_bit_set(heap_no) && !lock->rec_is_insert_intention() &&
          (heap_no == PAGE_HEAP_NO_SUPREMUM || !lock->rec_is_not_gap())) {
        (void)rec_add_to_queue(
          Lock_mode(LOCK_REC | LOCK_GAP | Lock_mode_type(lock->mode())), block, heir_heap_no, lock->rec_index(), lock->m_trx
        );
      }
    }
  }
}

void Lock_sys::rec_move(
  const Buf_block *receiver, const Buf_block *donator, ulint receiver_heap_no, ulint donator_heap_no
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  IF_DEBUG({
    if (auto it = m_rec_locks.find(receiver->get_page_id()); it != m_rec_locks.end()) {
      ut_ad(rec_exists(it->second, receiver_heap_no) == nullptr);
    }
  })

  auto it = m_rec_locks.find(donator->get_page_id());

  if (it != m_rec_locks.end()) {

    for (auto lock : it->second) {
      if (lock->rec_is_nth_bit_set(donator_heap_no)) {
        const auto type_mode = lock->mode();

        lock->rec_reset_nth_bit(donator_heap_no);

        if (unlikely(type_mode & LOCK_WAIT)) {
          lock->reset();
        }

        /* Note that we FIRST reset the bit, and then set the lock:
        the function works also if donator == receiver */

        (void)rec_add_to_queue(type_mode, receiver, receiver_heap_no, lock->m_rec.m_index, lock->m_trx);
      }
    }

    ut_ad(rec_exists(it->second, donator_heap_no) == nullptr);
  }
}

void Lock_sys::move_reorganize_page(const Buf_block *block, const Buf_block *oblock) noexcept {
  UT_LIST_BASE_NODE_T(Lock, m_trx_locks) old_locks;

  mutex_enter(&kernel_mutex);

  auto it = m_rec_locks.find(block->get_page_id());

  if (it == m_rec_locks.end()) {
    mutex_exit(&kernel_mutex);
    return;
  }

  auto heap = mem_heap_create(256);

  /* Copy first all the locks on the page to heap and reset the
  bitmaps in the original locks; chain the copies of the locks
  using the trx_locks field in them. */

  for (auto lock : it->second) {
    /* Make a copy of the lock */
    auto old_lock = lock->rec_clone(heap);

    old_locks.push_back(old_lock);

    /* Reset bitmap of lock */
    lock->rec_bitmap_reset();

    if (lock->is_waiting()) {
      lock->reset();
    }
  }

  for (auto lock : old_locks) {
    /* NOTE: we copy also the locks set on the infimum and supremum of the page; the infimum may carry locks if an
    update of a record is occurring on the page, and its locks were temporarily stored on the infimum */
    Page_cursor cur1;
    Page_cursor cur2;

    cur1.set_before_first(block);
    cur2.set_before_first(oblock);

    /* Set locks according to old locks */
    for (;;) {
      ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), rec_get_data_size(cur2.get_rec())));

      const auto old_heap_no = rec_get_heap_no(cur2.get_rec());
      const auto new_heap_no = rec_get_heap_no(cur1.get_rec());

      if (lock->rec_is_nth_bit_set(old_heap_no)) {

        /* Clear the bit in old_lock. */
        ut_d(lock->rec_reset_nth_bit(old_heap_no));

        /* NOTE that the old lock bitmap could be too
        small for the new heap number! */

        (void)rec_add_to_queue(lock->m_type_mode, block, new_heap_no, lock->m_rec.m_index, lock->m_trx);
      }

      if (unlikely(new_heap_no == PAGE_HEAP_NO_SUPREMUM)) {
        ut_ad(old_heap_no == PAGE_HEAP_NO_SUPREMUM);
        break;
      }

      cur1.move_to_next();
      cur2.move_to_next();
    }

#ifdef UNIV_DEBUG
    {
      const auto i = lock->rec_find_set_bit();

      /* Check that all locks were moved. */
      if (unlikely(i != ULINT_UNDEFINED)) {
        log_fatal(std::format("move_reorganize_page(): {} not moved in {}", i, lock->to_string(m_buf_pool)));
      }
    }
#endif /* UNIV_DEBUG */
  }

  mutex_exit(&kernel_mutex);

  mem_heap_free(heap);

  ut_ad(rec_validate_page(block->get_page_id()));
}

void Lock_sys::move_rec_list_end(const Buf_block *new_block, const Buf_block *block, const rec_t *rec) noexcept {
  mutex_enter(&kernel_mutex);

  /* Note: when we move locks from record to record, waiting locks
  and possible granted gap type locks behind them are enqueued in
  the original order, because new elements are inserted to a hash
  table to the end of the hash chain, and lock_rec_add_to_queue
  does not reuse locks if there are waiters in the queue. */

  if (auto it = m_rec_locks.find(block->get_page_id()); it != m_rec_locks.end()) {

    for (auto lock : it->second) {
      Page_cursor cur1;
      Page_cursor cur2;
      const auto type_mode = lock->m_type_mode;

      cur1.position(rec, block);

      if (cur1.is_before_first()) {
        cur1.move_to_next();
      }

      cur2.set_before_first(new_block);
      cur2.move_to_next();

      /* Copy lock requests on user records to new page and
      reset the lock bits on the old */

      while (!cur1.is_after_last()) {
        auto heap_no = rec_get_heap_no(cur1.get_rec());

        ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), rec_get_data_size(cur2.get_rec())));

        if (lock->rec_is_nth_bit_set(heap_no)) {
          lock->rec_reset_nth_bit(heap_no);

          if (unlikely(type_mode & LOCK_WAIT)) {
            lock->reset();
          }

          heap_no = rec_get_heap_no(cur2.get_rec());

          (void)rec_add_to_queue(type_mode, new_block, heap_no, lock->m_rec.m_index, lock->m_trx);
        }

        cur1.move_to_next();
        cur2.move_to_next();
      }
    }
  }

  mutex_exit(&kernel_mutex);

  ut_ad(rec_validate_page(block->get_page_id()));
  ut_ad(rec_validate_page(new_block->get_page_id()));
}

void Lock_sys::move_rec_list_start(
  const Buf_block *new_block, const Buf_block *block, const rec_t *rec, const rec_t *old_end
) noexcept {
  ut_ad(block->m_frame == page_align(rec));
  ut_ad(new_block->m_frame == page_align(old_end));

  mutex_enter(&kernel_mutex);

  if (auto it = m_rec_locks.find(block->get_page_id()); it != m_rec_locks.end()) {

    for (auto lock : it->second) {
      Page_cursor cur1;
      Page_cursor cur2;
      const auto type_mode = lock->m_type_mode;

      cur1.set_before_first(block);
      cur1.move_to_next();

      cur2.position(old_end, new_block);
      cur2.move_to_next();

      /* Copy lock requests on user records to new page and
      reset the lock bits on the old */

      while (cur1.get_rec() != rec) {

        auto heap_no = rec_get_heap_no(cur1.get_rec());
        ut_ad(!memcmp(cur1.get_rec(), cur2.get_rec(), rec_get_data_size(cur2.get_rec())));

        if (lock->rec_is_nth_bit_set(heap_no)) {
          lock->rec_reset_nth_bit(heap_no);

          if (unlikely(type_mode & LOCK_WAIT)) {
            lock->reset();
          }

          heap_no = rec_get_heap_no(cur2.get_rec());

          (void)rec_add_to_queue(type_mode, new_block, heap_no, lock->m_rec.m_index, lock->m_trx);
        }

        cur1.move_to_next();
        cur2.move_to_next();
      }

      IF_DEBUG(if (page_rec_is_supremum(rec)) {
        for (auto i = PAGE_HEAP_NO_USER_LOW; i < lock->rec_get_n_bits(); ++i) {
          if (unlikely(lock->rec_is_nth_bit_set(i))) {
            log_fatal("move_rec_list_start(): {} not moved in {}", i, lock->to_string(m_buf_pool));
          }
        }
      })
    }
  }

  mutex_exit(&kernel_mutex);

  ut_ad(rec_validate_page(block->get_page_id()));
}

void Lock_sys::update_split_right(const Buf_block *right_block, const Buf_block *left_block) noexcept {
  const ulint heap_no = get_min_heap_no(right_block);

  mutex_enter(&kernel_mutex);

  /* Move the locks on the supremum of the left page to the supremum
  of the right page */

  rec_move(right_block, left_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

  /* Inherit the locks to the supremum of left page from the successor
  of the infimum on right page */

  rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_merge_right(const Buf_block *right_block, const rec_t *orig_succ, const Buf_block *left_block) noexcept {
  mutex_enter(&kernel_mutex);

  /* Inherit the locks from the supremum of the left page to the
  original successor of infimum on the right page, to which the left
  page was merged */

  rec_inherit_to_gap(right_block, left_block, page_rec_get_heap_no(orig_succ), PAGE_HEAP_NO_SUPREMUM);

  /* Reset the locks on the supremum of the left page, releasing
  waiting transactions */

  rec_reset_and_release_wait(left_block->get_page_id(), PAGE_HEAP_NO_SUPREMUM);

  rec_free_all_from_discard_page(left_block->get_page_id());

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_root_raise(const Buf_block *block, const Buf_block *root) noexcept {
  mutex_enter(&kernel_mutex);

  /* Move the locks on the supremum of the root to the supremum of block */
  rec_move(block, root, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_copy_and_discard(const Buf_block *new_block, const Buf_block *block) noexcept {
  mutex_enter(&kernel_mutex);

  /* Move the locks on the supremum of the old page to the supremum
  of new_page */

  rec_move(new_block, block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);
  rec_free_all_from_discard_page(block->get_page_id());

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_split_left(const Buf_block *right_block, const Buf_block *left_block) noexcept {
  const ulint heap_no = get_min_heap_no(right_block);

  mutex_enter(&kernel_mutex);

  /* Inherit the locks to the supremum of the left page from the
  successor of the infimum on the right page */

  rec_inherit_to_gap(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_merge_left(const Buf_block *left_block, const rec_t *orig_pred, const Buf_block *right_block) noexcept {
  const rec_t *left_next_rec;

  ut_ad(left_block->m_frame == page_align(orig_pred));

  mutex_enter(&kernel_mutex);

  left_next_rec = page_rec_get_next_const(orig_pred);

  if (!page_rec_is_supremum(left_next_rec)) {

    /* Inherit the locks on the supremum of the left page to the
    first record which was moved from the right page */

    rec_inherit_to_gap(left_block, left_block, page_rec_get_heap_no(left_next_rec), PAGE_HEAP_NO_SUPREMUM);

    /* Reset the locks on the supremum of the left page,
    releasing waiting transactions */

    rec_reset_and_release_wait(left_block->get_page_id(), PAGE_HEAP_NO_SUPREMUM);
  }

  /* Move the locks from the supremum of right page to the supremum
  of the left page */

  rec_move(left_block, right_block, PAGE_HEAP_NO_SUPREMUM, PAGE_HEAP_NO_SUPREMUM);

  rec_free_all_from_discard_page(right_block->get_page_id());

  mutex_exit(&kernel_mutex);
}

void Lock_sys::rec_reset_and_inherit_gap_locks(
  const Buf_block *heir_block, const Buf_block *block, ulint heir_heap_no, ulint heap_no
) noexcept {
  mutex_enter(&kernel_mutex);

  rec_reset_and_release_wait(heir_block->get_page_id(), heir_heap_no);

  rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_discard(const Buf_block *heir_block, ulint heir_heap_no, const Buf_block *block) noexcept {
  const auto page = block->m_frame;

  mutex_enter(&kernel_mutex);

  auto it = m_rec_locks.find(block->get_page_id());

  if (it == m_rec_locks.end()) {
    /* No locks exist on page, nothing to do */
    mutex_exit(&kernel_mutex);
    return;
  }

  /* Inherit all the locks on the page to the record and reset all the locks on the page */

  ulint heap_no;
  auto rec{page + PAGE_INFIMUM};

  do {
    heap_no = rec_get_heap_no(rec);

    rec_inherit_to_gap(heir_block, block, heir_heap_no, heap_no);

    rec_reset_and_release_wait(block->get_page_id(), heap_no);

    rec = page + rec_get_next_offs(rec);

  } while (heap_no != PAGE_HEAP_NO_SUPREMUM);

  rec_free_all_from_discard_page(block->get_page_id());

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_insert(const Buf_block *block, const rec_t *rec) noexcept {
  ut_ad(block->m_frame == page_align(rec));

  /* Inherit the gap-locking locks for rec, in gap mode, from the next
  record */

  const auto receiver_heap_no = rec_get_heap_no(rec);
  const auto donator_heap_no = rec_get_heap_no(page_rec_get_next_low(rec));

  mutex_enter(&kernel_mutex);

  rec_inherit_to_gap_if_gap_lock(block, receiver_heap_no, donator_heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::update_delete(const Buf_block *block, const rec_t *rec) noexcept {
  const page_t *page = block->m_frame;

  ut_ad(page == page_align(rec));

  const auto heap_no = rec_get_heap_no(rec);
  const auto next_heap_no = rec_get_heap_no(page + rec_get_next_offs(rec));

  mutex_enter(&kernel_mutex);

  /* Let the next record inherit the locks from rec, in gap mode */

  rec_inherit_to_gap(block, block, next_heap_no, heap_no);

  /* Reset the lock bits on rec and release waiting transactions */

  rec_reset_and_release_wait(block->get_page_id(), heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::rec_store_on_page_infimum(const Buf_block *block, const rec_t *rec) noexcept {
  const auto heap_no = page_rec_get_heap_no(rec);

  ut_ad(block->m_frame == page_align(rec));

  mutex_enter(&kernel_mutex);

  rec_move(block, block, PAGE_HEAP_NO_INFIMUM, heap_no);

  mutex_exit(&kernel_mutex);
}

void Lock_sys::rec_restore_from_page_infimum(const Buf_block *block, const rec_t *rec, const Buf_block *donator) noexcept {
  const auto heap_no = page_rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  rec_move(block, donator, heap_no, PAGE_HEAP_NO_INFIMUM);

  mutex_exit(&kernel_mutex);
}

bool Lock_sys::deadlock_occurs(Lock *lock, Trx *trx) noexcept {
  ulint ret;
  ulint cost{};

  ut_ad(mutex_own(&kernel_mutex));

retry:
  /* We check that adding this trx to the waits-for graph
  does not produce a cycle. First mark all active transactions
  with 0: */

  for (auto mark_trx : m_trx_sys->m_trx_list) {
    mark_trx->m_deadlock_mark = 0;
  }

  ret = deadlock_recursive(trx, trx, lock, &cost, 0);

  switch (ret) {
    case LOCK_VICTIM_IS_OTHER:
      /* We chose some other trx as a victim: retry if there still
    is a deadlock */
      goto retry;

    case LOCK_EXCEED_MAX_DEPTH:
      log_info("TOO DEEP OR LONG SEARCH IN THE LOCK TABLE WAITS-FOR GRAPH, WE WILL ROLL BACK FOLLOWING TRANSACTION");
      log_info("\n*** TRANSACTION:\n");

      log_info(trx->to_string(3000));

      log_info("*** WAITING FOR THIS LOCK TO BE GRANTED:n");

      log_info(lock->to_string(m_buf_pool));
      break;

    case LOCK_VICTIM_IS_START:
      log_info("*** WE ROLL BACK TRANSACTION (2)");
      break;

    default:
      /* No deadlock detected*/
      return false;
  }

  lock_deadlock_found = true;

  return true;
}

ulint Lock_sys::deadlock_recursive(Trx *start, Trx *trx, Lock *wait_lock, ulint *cost, ulint depth) noexcept {
  ulint ret;
  ut_ad(mutex_own(&kernel_mutex));

  if (trx->m_deadlock_mark == 1) {
    /* We have already exhaustively searched the subtree starting from this trx */

    return 0;
  }

  ++*cost;

  Lock *found_lock{};
  ulint heap_no{ULINT_UNDEFINED};

  if (wait_lock->type() == LOCK_REC) {
    heap_no = wait_lock->rec_find_set_bit();
    ut_a(heap_no != ULINT_UNDEFINED);

    if (auto it = m_rec_locks.find(wait_lock->page_id()); it != m_rec_locks.end()) {
      for (auto lock : it->second) {
        ut_ad(lock->type() == LOCK_REC);
        if (lock == wait_lock || lock->rec_is_nth_bit_set(heap_no)) {
          found_lock = lock;
          break;
        }
      }
    }

    if (found_lock == wait_lock) {
      found_lock = nullptr;
    }

    ut_ad(found_lock == nullptr || found_lock->rec_is_nth_bit_set(heap_no));

  } else {
    found_lock = wait_lock;
  }

  /* Look at the locks ahead of wait_lock in the lock queue */

  for (;;) {
    /* Get previous table lock. */
    if (heap_no == ULINT_UNDEFINED) {

      found_lock = UT_LIST_GET_PREV(m_table.m_locks, found_lock);
    }

    if (found_lock == nullptr) {
      /* We can mark this subtree as searched */
      trx->m_deadlock_mark = 1;

      return false;
    }

    if (wait_lock->has_to_wait_for(found_lock, heap_no)) {

      bool too_far = depth > LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK || *cost > LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK;

      auto lock_trx = found_lock->m_trx;

      if (lock_trx == start) {

        /* We came back to the recursion starting point: a deadlock detected; or we have searched the waits-for graph too long */

        log_info("\n*** (1) TRANSACTION:");

        log_info(wait_lock->m_trx->to_string(3000));

        log_info("*** (1) WAITING FOR THIS LOCK TO BE GRANTED:");

        log_info(wait_lock->to_string(m_buf_pool));

        log_info("*** (2) TRANSACTION:");

        log_info(found_lock->m_trx->to_string(3000));

        log_info("*** (2) HOLDS THE LOCK(S):");

        log_info(found_lock->to_string(m_buf_pool));

        log_info("*** (2) WAITING FOR THIS LOCK TO BE GRANTED:");

        log_info(wait_lock->to_string(m_buf_pool));

        log_info(start->m_wait_lock->to_string(m_buf_pool));

        if (Trx::weight_cmp(wait_lock->m_trx, start) >= 0) {
          /* Our recursion starting point transaction is 'smaller', let us
          choose 'start' as the victim and roll back it */

          return LOCK_VICTIM_IS_START;
        }

        lock_deadlock_found = true;

        /* Let us choose the transaction of wait_lock as a victim to try
        to avoid deadlocking our recursion starting point transaction */

        log_info("*** WE ROLL BACK TRANSACTION (1)");

        wait_lock->m_trx->m_was_chosen_as_deadlock_victim = true;

        cancel_waiting_and_release(wait_lock);

        /* Since trx and wait_lock are no longer in the waits-for graph, we can return false;
        note that our selective algorithm can choose several transactions as victims, but still
        we may end up rolling back also the recursion starting point transaction! */

        return LOCK_VICTIM_IS_OTHER;
      }

      if (too_far) {

        /* The information about transaction/lock to be rolled back is available in the top
        level. Do not print anything here. */
        return LOCK_EXCEED_MAX_DEPTH;
      }

      if (lock_trx->m_que_state == TRX_QUE_LOCK_WAIT) {

        /* Another trx ahead has requested lock	in an incompatible mode, and is itself waiting for a lock */

        ret = deadlock_recursive(start, lock_trx, lock_trx->m_wait_lock, cost, depth + 1);

        if (ret != 0) {

          return ret;
        }
      }
    }
    /* Get the next record lock to check. */
    if (heap_no != ULINT_UNDEFINED) {

      ut_a(found_lock != nullptr);

      do {
        found_lock = found_lock->next();
      } while (found_lock != nullptr && found_lock != wait_lock && !found_lock->rec_is_nth_bit_set(heap_no));

      if (found_lock == wait_lock) {
        found_lock = nullptr;
      }
    }
  } /* end of the 'for (;;)'-loop */
}

Lock *Lock_sys::table_create(Table *table, Lock_mode type_mode, Trx *trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = reinterpret_cast<Lock *>(mem_heap_alloc(trx->m_lock_heap, sizeof(Lock)));

  trx->m_trx_locks.push_back(lock);

  lock->m_trx = trx;
  lock->m_type_mode = Lock_mode(Lock_mode_type(type_mode) | LOCK_TABLE);

  lock->m_table.m_table = table;

  table->m_locks.push_back(lock);

  if (unlikely(type_mode & LOCK_WAIT)) {

    lock->set_trx_wait(trx);
  }

  return lock;
}

void Lock_sys::table_remove_low(Lock *lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto trx = lock->m_trx;
  auto table = lock->m_table.m_table;

  trx->m_trx_locks.remove(lock);
  table->m_locks.remove(lock);
}

[[nodiscard]] db_err Lock_sys::table_enqueue_waiting(Lock_mode mode, Table *table, que_thr_t *thr) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* Test if there already is some other reason to suspend thread:
  we do not enqueue a lock request if the query thread should be
  stopped anyway */

  if (que_thr_stop(thr)) {
    ut_error;

    return DB_QUE_THR_SUSPENDED;
  }

  auto trx = thr_get_trx(thr);

  switch (trx->get_dict_operation()) {
    case TRX_DICT_OP_NONE:
      break;
    case TRX_DICT_OP_TABLE:
    case TRX_DICT_OP_INDEX:
      log_err(std::format(
        "A table lock wait happens"
        " in a dictionary operation! Table name ",
        table->m_name
      ));
      log_err("Submit a detailed bug report, check the InnoDB website for details");
  }

  /* Enqueue the lock request that will wait to be granted */

  auto lock = table_create(table, Lock_mode(mode | LOCK_WAIT), trx);

  /* Check if a deadlock occurs: if yes, remove the lock request and
  return an error code */

  if (deadlock_occurs(lock, trx)) {

    /* The order here is important, we don't want to
    lose the state of the lock before calling remove. */
    table_remove_low(lock);
    lock->reset();

    return DB_DEADLOCK;
  }

  if (trx->m_wait_lock == nullptr) {
    /* Deadlock resolution chose another transaction as a victim,
    and we accidentally got our lock granted! */

    return DB_SUCCESS;
  }

  trx->m_que_state = TRX_QUE_LOCK_WAIT;
  trx->m_was_chosen_as_deadlock_victim = false;
  trx->m_wait_started = time(nullptr);

  const auto success = que_thr_stop(thr);
  ut_a(success);

  return DB_LOCK_WAIT;
}

Lock *Lock_sys::table_other_has_incompatible(Trx *trx, ulint wait, Table *table, Lock_mode mode) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = table->m_locks.back();

  while (lock != nullptr) {

    if (lock->m_trx != trx && !Lock::mode_compatible(lock->mode(), mode) && (wait || !lock->is_waiting())) {

      return lock;
    }

    lock = UT_LIST_GET_PREV(m_table.m_locks, lock);
  }

  return nullptr;
}

db_err Lock_sys::lock_table(ulint flags, Table *table, Lock_mode mode, que_thr_t *thr) noexcept {
  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  ut_a(flags == 0);

  auto trx = thr_get_trx(thr);

  mutex_enter(&kernel_mutex);

  /* Look for stronger locks the same trx already has on the table */

  if (table_has(trx, table, mode)) {

    mutex_exit(&kernel_mutex);

    return DB_SUCCESS;
  }

  /* We have to check if the new lock is compatible with any locks
  other transactions have in the table lock queue. */

  if (table_other_has_incompatible(trx, LOCK_WAIT, table, mode)) {

    /* Another trx has a request on the table in an incompatible
    mode: this trx may have to wait */

    auto err = table_enqueue_waiting(Lock_mode(mode | flags), table, thr);

    mutex_exit(&kernel_mutex);

    return err;
  }

  (void)table_create(table, Lock_mode(Lock_mode_type(mode) | flags), trx);

  ut_a(!flags || mode == LOCK_S || mode == LOCK_X);

  mutex_exit(&kernel_mutex);

  return DB_SUCCESS;
}

bool Lock_sys::table_has_to_wait_in_queue(Lock *wait_lock) noexcept {
  ut_ad(wait_lock->is_waiting());

  auto table = wait_lock->m_table.m_table;

  for (auto lock : table->m_locks) {
    if (lock == wait_lock) {
      return false;
    }

    if (wait_lock->has_to_wait_for(lock, ULINT_UNDEFINED)) {

      return true;
    }
  }

  return false;
}

void Lock_sys::table_dequeue(Lock *in_lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_a(in_lock->type() == LOCK_TABLE);

  auto lock = UT_LIST_GET_NEXT(m_table.m_locks, in_lock);

  table_remove_low(in_lock);

  /* Check if waiting locks in the queue can now be granted: grant
  locks if there are no conflicting locks ahead. */

  while (lock != nullptr) {

    if (lock->is_waiting() && !table_has_to_wait_in_queue(lock)) {

      /* Grant the lock */
      grant(lock);
    }

    lock = UT_LIST_GET_NEXT(m_table.m_locks, lock);
  }
}

void Lock_sys::rec_unlock(Trx *trx, const Buf_block *block, const rec_t *rec, Lock_mode Lock_mode) noexcept {
  ut_ad(block->m_frame == page_align(rec));

  const auto heap_no = page_rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  Lock *release_lock{};

  /* Find the last lock with the same Lock_mode and transaction from the record. */
  auto it = m_rec_locks.find(block->get_page_id());
  ut_a(it != m_rec_locks.end());

  for (auto lock : it->second) {
    if (lock->rec_is_nth_bit_set(heap_no)) {
      if (lock->m_trx == trx && lock->mode() == Lock_mode) {
        release_lock = lock;
        ut_a(!lock->is_waiting());
      }
    }
  }

  /* If a record lock is found, release the record lock */
  if (likely(release_lock != nullptr)) {
    release_lock->rec_reset_nth_bit(heap_no);
  } else {
    mutex_exit(&kernel_mutex);
    log_err(std::format("Unlock row could not find a {} mode lock on the record", to_int(Lock_mode)));
    return;
  }

  /* Check if we can now grant waiting lock requests */
  for (auto lock : it->second) {
    if (lock->rec_is_nth_bit_set(heap_no) && lock->is_waiting() && !rec_has_to_wait_in_queue(it->second, lock, heap_no)) {
      /* Grant the lock */
      grant(lock);
    }
  }

  mutex_exit(&kernel_mutex);
}

void Lock_sys::release_off_kernel(Trx *trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  ulint count{};
  auto lock = trx->m_trx_locks.back();

  while (lock != nullptr) {

    ++count;

    if (lock->type() == LOCK_REC) {

      rec_dequeue_from_page(lock);

    } else {

      ut_ad(lock->type() == LOCK_TABLE);

      table_dequeue(lock);
    }

    if (count == LOCK_RELEASE_KERNEL_INTERVAL) {
      /* Release the kernel mutex for a while, so that we
      do not monopolize it */

      mutex_exit(&kernel_mutex);

      mutex_enter(&kernel_mutex);

      count = 0;
    }

    lock = trx->m_trx_locks.back();
  }

  mem_heap_empty(trx->m_lock_heap);
}

void Lock_sys::cancel_waiting_and_release(Lock *lock) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  if (lock->type() == LOCK_REC) {

    rec_dequeue_from_page(lock);
  } else {
    ut_ad(lock->type() == LOCK_TABLE);

    table_dequeue(lock);
  }

  /* Reset the wait flag and the back pointer to lock in trx */

  lock->reset();

  /* The following function releases the trx from lock wait */

  lock->m_trx->end_lock_wait();
}

/* True if a lock mode is S or X */
#define IS_LOCK_S_OR_X(lock) (lock->mode() == LOCK_S || lock->mode() == LOCK_X)

void Lock_sys::remove_all_on_table_for_trx(Table *table, Trx *trx, bool remove_sx_locks) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto lock = trx->m_trx_locks.back();

  while (lock != nullptr) {
    auto prev_lock = UT_LIST_GET_PREV(m_trx_locks, lock);

    if (lock->type() == LOCK_REC && lock->m_table.m_table == table) {
      ut_a(!lock->is_waiting());

      rec_discard(lock);
    } else if (lock->type() == LOCK_TABLE && lock->m_table.m_table == table && (remove_sx_locks || !IS_LOCK_S_OR_X(lock))) {

      ut_a(!lock->is_waiting());

      table_remove_low(lock);
    }

    lock = prev_lock;
  }
}

void Lock_sys::remove_all_on_table(Table *table, bool remove_sx_locks) noexcept {
  mutex_enter(&kernel_mutex);

  auto lock = table->m_locks.front();

  while (lock != nullptr) {

    auto prev_lock = UT_LIST_GET_PREV(m_table.m_locks, lock);

    /* If we should remove all locks (remove_sx_locks
    is true), or if the lock is not table-level S or X lock,
    then check we are not going to remove a wait lock. */
    if (remove_sx_locks || !(lock->type() == LOCK_TABLE && IS_LOCK_S_OR_X(lock))) {

      // HACK: For testing
      if (lock->is_waiting()) {
        if (remove_sx_locks) {
          ut_error;
        } else {
          goto next;
        }
      }
    }

    remove_all_on_table_for_trx(table, lock->m_trx, remove_sx_locks);

    if (prev_lock == nullptr) {
      if (lock == table->m_locks.front()) {
        /* lock was not removed, pick its successor */
        lock = UT_LIST_GET_NEXT(m_table.m_locks, lock);
      } else {
        /* lock was removed, pick the first one */
        lock = table->m_locks.front();
      }
    } else if (UT_LIST_GET_NEXT(m_table.m_locks, prev_lock) != lock) {
      /* If lock was removed by lock_remove_all_on_table_for_trx() then pick the
      successor of prev_lock ... */
      lock = UT_LIST_GET_NEXT(m_table.m_locks, prev_lock);
    } else {
    next:
      /* ... otherwise pick the successor of lock. */
      lock = UT_LIST_GET_NEXT(m_table.m_locks, lock);
    }
  }

  mutex_exit(&kernel_mutex);
}

[[nodiscard]] ulint Lock_sys::get_n_rec_locks() noexcept {
  return m_rec_locks.size();
}

bool Lock_sys::print_info_summary(bool nowait) const noexcept {
  /* if nowait is false, wait on the kernel mutex,
  otherwise return immediately if fail to obtain the
  mutex. */
  if (!nowait) {
    mutex_enter(&kernel_mutex);
  } else if (mutex_enter_nowait(&kernel_mutex)) {
    log_info("FAIL TO OBTAIN KERNEL MUTEX, SKIP LOCK INFO PRINTING");
    return false;
  }

  if (lock_deadlock_found) {
    log_info(
      "------------------------\n"
      "LATEST DETECTED DEADLOCK\n"
      "------------------------\n"
    );
  }

  log_info(
    "------------\n"
    "TRANSACTIONS\n"
    "------------\n"
  );

  log_info("Trx id counter ", m_trx_sys->m_max_trx_id);

  log_info(std::format(
    "Purge done for trx's n:o < {} undo n:o < {}", m_trx_sys->m_purge->m_purge_trx_no, m_trx_sys->m_purge->m_purge_undo_no
  ));

  log_info("History list length ", m_trx_sys->m_rseg_history_len);

  return true;
}

void Lock_sys::print_info_all_transactions() noexcept {
  ulint nth_trx{};
  ulint nth_lock{};
  bool load_page_first{true};

  log_info("LIST OF TRANSACTIONS FOR EACH SESSION:");

  /* First print info on non-active transactions */

  for (auto trx : m_trx_sys->m_client_trx_list) {
    if (trx->m_conc_state == TRX_NOT_STARTED) {
      log_info("---");
      log_info(trx->to_string(600));
    }
  }

  const Lock *lock{};

  for (;;) {
    ulint i{};
    Trx *trx{};

    /* Since we temporarily release the kernel mutex when
    reading a database page in below, variable trx may be
    obsolete now and we must loop through the trx list to
    get probably the same trx, or some other trx. */

    for (auto tx : m_trx_sys->m_trx_list) {
      if (i == nth_trx) {
        trx = tx;
        break;
      }

      ++i;
    }

    if (trx == nullptr) {
      mutex_exit(&kernel_mutex);

      ut_ad(validate());

      return;
    }

    if (nth_lock == 0) {
      log_info("---");
      log_info(trx->to_string(600));

      if (trx->m_read_view) {
        log_info(std::format(
          "Trx read view will not see trx with id >= {}, sees < {}", trx->m_read_view->low_limit_id, trx->m_read_view->up_limit_id
        ));
      }

      if (trx->m_que_state == TRX_QUE_LOCK_WAIT) {
        log_info(std::format(
          "------- TRX HAS BEEN WAITING {} SEC FOR THIS LOCK TO BE GRANTED:", difftime(time(nullptr), trx->m_wait_started)
        ));

        log_info(lock->to_string(m_buf_pool));

        log_info("------------------");
      }
    }

    if (!is_print_lock_monitor_set()) {
      ++nth_trx;
      continue;
    }

    i = 0;

    /* Look at the note about the trx loop above why we loop here:
    lock may be an obsolete pointer now. */

    lock = trx->m_trx_locks.front();

    while (lock != nullptr && i < nth_lock) {
      lock = UT_LIST_GET_NEXT(m_trx_locks, lock);
      ++i;
    }

    if (lock == nullptr) {

      ++nth_trx;
      nth_lock = 0;

      continue;
    }

    if (lock->type() == LOCK_REC) {
      if (load_page_first) {
        const auto size = m_trx_sys->m_fsp->m_fil->space_get_flags(lock->rec_space_id());

        if (unlikely(size == ULINT_UNDEFINED)) {

          /* It is a single table tablespace and the .ibd file is missing (TRUNCATE
          TABLE probably stole the locks): just print the lock without attempting to
          load the page in the buffer pool. */

          log_info("RECORD LOCKS on non-existing space ", lock->rec_space_id());
          log_info(lock->rec_to_string(m_buf_pool));

        } else {

          mutex_exit(&kernel_mutex);

          mtr_t mtr;

          mtr.start();

          Buf_pool::Request req{
            .m_rw_latch = BUF_GET_NO_LATCH,
            .m_page_id = lock->page_id(),
            .m_mode = BUF_GET_NO_LATCH,
            .m_file = __FILE__,
            .m_line = __LINE__,
            .m_mtr = &mtr
          };

          /* We are simply trying to force a read here. */
          (void)m_buf_pool->get(req, nullptr);

          mtr.commit();

          load_page_first = false;

          mutex_enter(&kernel_mutex);

          continue;
        }
      } else {
        ut_ad(lock->type() == LOCK_TABLE);

        log_info(lock->table_to_string());
      }

      load_page_first = true;

      ++nth_lock;

      if (nth_lock >= 10) {
        ++nth_trx;
        log_info("10 LOCKS PRINTED FOR THIS TRX: SUPPRESSING FURTHER PRINTS");
        nth_lock = 0;
      } else {
        break;
      }
    }
  }
}

#ifdef UNIV_DEBUG
bool Lock_sys::table_queue_validate(Table *table) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  for (auto lock : table->m_locks) {
    validate_trx_state(lock->m_trx);

    if (!lock->is_waiting()) {

      ut_a(!table_other_has_incompatible(lock->m_trx, 0, table, lock->mode()));

    } else {

      ut_a(table_has_to_wait_in_queue(lock));
    }
  }

  return true;
}

bool Lock_sys::rec_queue_validate(const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets) noexcept {
  ut_ad(block->get_frame() == page_align(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  mutex_enter(&kernel_mutex);

  auto heap_no = page_rec_get_heap_no(rec);

  auto it = m_rec_locks.find(block->get_page_id());

  if (unlikely(!page_rec_is_user_rec(rec))) {

    if (it != m_rec_locks.end()) {

      for (auto lock : it->second) {
        if (likely(!lock->rec_is_nth_bit_set(heap_no))) {
          continue;
        }

        validate_trx_state(lock->m_trx);
        ut_ad(m_trx_sys->in_trx_list(lock->m_trx));

        if (lock->is_waiting()) {
          ut_ad(rec_has_to_wait_in_queue(it->second, lock, heap_no));
        }

        if (index != nullptr) {
          ut_ad(lock->m_rec.m_index == index);
        }
      }
    }

  } else {

    if (index != nullptr && index->is_clustered()) {

      auto impl_trx = clust_rec_some_has_impl(rec, index, offsets);

      if (impl_trx != nullptr && rec_other_has_expl_req(block->get_page_id(), LOCK_S, 0, LOCK_WAIT, heap_no, impl_trx) != nullptr) {

        ut_ad(rec_has_expl(block->get_page_id(), LOCK_X | LOCK_REC_NOT_GAP, heap_no, impl_trx));
      }
    }

    if (it != m_rec_locks.end()) {

      for (auto lock : it->second) {
        if (likely(!lock->rec_is_nth_bit_set(heap_no))) {
          continue;
        }

        validate_trx_state(lock->m_trx);
        ut_ad(m_trx_sys->in_trx_list(lock->m_trx));

        if (index != nullptr) {
          ut_a(lock->m_rec.m_index == index);
        }

        if (!(lock->rec_is_gap() || lock->is_waiting())) {

          const Lock_mode check_mode = lock->mode() == LOCK_S ? LOCK_X : LOCK_S;
          ut_ad(rec_other_has_expl_req(block->get_page_id(), check_mode, 0, 0, heap_no, lock->m_trx) == nullptr);

        } else if (lock->is_waiting() && !lock->rec_is_gap()) {

          ut_ad(rec_has_to_wait_in_queue(it->second, lock, heap_no));
        }
      }
    }
  }

  mutex_exit(&kernel_mutex);

  return true;
}

bool Lock_sys::rec_validate_page(Page_id page_id) noexcept {
  ut_ad(!mutex_own(&kernel_mutex));

  mtr_t mtr;

  mtr.start();

  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = page_id, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
  };

  auto block = m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  const auto page = block->get_frame();

  auto clean_up = [](mtr_t &mtr, mem_heap_t *heap) -> bool {
    mutex_exit(&kernel_mutex);

    mtr.commit();

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }

    return true;
  };

  ulint nth_lock{};

  auto advance_to_nth_lock = [&](Page_id page_id, ulint nth_lock) -> Lock * {
    Lock *lock{};

    if (auto it = m_rec_locks.find(page_id); it != m_rec_locks.end()) {
      lock = it->second.front();

      for (ulint i{}; i < nth_lock; ++i) {
        lock = lock->next();
      }
    }

    return lock;
  };

  mutex_enter(&kernel_mutex);

  ulint nth_bit{};
  mem_heap_t *heap{};

  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  rec_offs_init(offsets_);

  for (;;) {
    auto lock = advance_to_nth_lock(page_id, nth_lock);

    if (lock == nullptr) {
      return clean_up(mtr, heap);
    }

    ut_a(m_trx_sys->in_trx_list(lock->m_trx));

    validate_trx_state(lock->m_trx);

#ifdef UNIV_SYNC_DEBUG
    /* Only validate the record queues when this thread is not
    holding a space->latch.  Deadlocks are possible due to
    latching order violation when UNIV_DEBUG is defined while
    UNIV_SYNC_DEBUG is not. */
    if (!sync_thread_levels_contains(SYNC_FSP))
#endif /* UNIV_SYNC_DEBUG */

      ulint i{};
    const auto n_bits = lock->rec_get_n_bits();

    for (i = nth_bit; i < n_bits; ++i) {

      if (i == 1 || lock->rec_is_nth_bit_set(i)) {

        auto index = lock->rec_index();
        auto rec = page_find_rec_with_heap_no(page, i);

        ut_a(rec != nullptr);

        {
          Phy_rec record{index, rec};

          offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
        }

        mutex_exit(&kernel_mutex);

        /* If this thread is holding the file space latch (fil_space_t::latch), the following
          check WILL break the latching order and may cause a deadlock of threads. */

        (void)rec_queue_validate(block, rec, index, offsets);

        mutex_enter(&kernel_mutex);

        nth_bit = i + 1;

        break;
      }
    }

    if (i == n_bits) {
      nth_bit = 0;
      ++nth_lock;
    }
  }

  return clean_up(mtr, heap);
}

bool Lock_sys::validate() noexcept {
  mutex_enter(&kernel_mutex);

  for (auto trx : m_trx_sys->m_trx_list) {

    for (auto lock : trx->m_trx_locks) {

      if (lock->type() == LOCK_TABLE) {
        (void)table_queue_validate(lock->m_table.m_table);
      }
    }
  }

  for (auto &[page_id, rec_locks] : m_rec_locks) {

    ut_a(m_trx_sys->in_trx_list(rec_locks.front()->m_trx));

    mutex_exit(&kernel_mutex);

    (void)rec_validate_page(page_id);

    mutex_enter(&kernel_mutex);
  }

  mutex_exit(&kernel_mutex);

  return true;
}
#endif /* UNIV_DEBUG */

db_err Lock_sys::rec_insert_check_and_lock(
  ulint flags, const rec_t *rec, Buf_block *block, const Index *index, que_thr_t *thr, mtr_t *mtr, bool *inherit
) noexcept {
  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);
  auto next_rec = page_rec_get_next_const(rec);
  auto next_rec_heap_no = page_rec_get_heap_no(next_rec);

  mutex_enter(&kernel_mutex);

  /* When inserting a record into an index, the table must be at
  least IX-locked or we must be building an index, in which case
  the table must be at least S-locked. */
  ut_ad(table_has(trx, index->m_table, LOCK_IX) || (*index->m_name == TEMP_INDEX_PREFIX && table_has(trx, index->m_table, LOCK_S)));

  auto it = m_rec_locks.find(block->get_page_id());

  if (likely(it == m_rec_locks.end())) {
    mutex_exit(&kernel_mutex);

    if (likely(!index->is_clustered())) {
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

  db_err err;
  const auto type_mode = Lock_mode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION);
  auto wait_for = rec_other_has_conflicting(block->get_page_id(), type_mode, next_rec_heap_no, trx);

  if (wait_for != nullptr) {
    /* Note that we may get DB_SUCCESS also here! */
    err = rec_enqueue_waiting(type_mode, block, next_rec_heap_no, index, thr);

  } else {
    err = DB_SUCCESS;
  }

  mutex_exit(&kernel_mutex);

  if (err == DB_SUCCESS && !index->is_clustered()) {
    /* Update the page max trx id field */
    page_update_max_trx_id(block, trx->m_id, mtr);
  }

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap{};
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();
    rec_offs_init(offsets_);

    {
      Phy_rec record{index, next_rec};

      offsets = record.get_all_col_offsets(offsets_, &heap, Current_location());
    }

    ut_ad(rec_queue_validate(block, next_rec, index, offsets));

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }
#endif /* UNIV_DEBUG */

  return err;
}

void Lock_sys::rec_convert_impl_to_expl(
  const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets
) noexcept {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  const Trx *impl_trx;

  if (index->is_clustered()) {
    impl_trx = clust_rec_some_has_impl(rec, index, offsets);
  } else {
    impl_trx = sec_rec_some_has_impl_off_kernel(rec, index, offsets);
  }

  if (impl_trx != nullptr) {
    const auto heap_no = page_rec_get_heap_no(rec);

    /* If the transaction has no explicit x-lock set on the
    record, set one for it */

    const auto type_mode = Lock_mode(LOCK_X | LOCK_REC_NOT_GAP);
    if (!rec_has_expl(block->get_page_id(), type_mode, heap_no, impl_trx)) {
      (void)rec_add_to_queue(type_mode, block, heap_no, index, impl_trx);
    }
  }
}

db_err Lock_sys::clust_rec_modify_check_and_lock(
  ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, que_thr_t *thr
) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(index->is_clustered());
  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  const auto heap_no = rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  ut_ad(table_has(thr_get_trx(thr), index->m_table, LOCK_IX));

  /* If a transaction has no explicit x-lock set on the record, set one for it */

  rec_convert_impl_to_expl(block, rec, index, offsets);

  auto err = rec_lock(true, Lock_mode(LOCK_X | LOCK_REC_NOT_GAP), block, heap_no, index, thr);

  mutex_exit(&kernel_mutex);

  ut_ad(rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err Lock_sys::sec_rec_modify_check_and_lock(
  ulint flags, Buf_block *block, const rec_t *rec, const Index *index, que_thr_t *thr, mtr_t *mtr
) noexcept {
  ut_ad(!index->is_clustered());
  ut_ad(block->m_frame == page_align(rec));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  const auto heap_no = page_rec_get_heap_no(rec);

  /* Another transaction cannot have an implicit lock on the record, because when we come here, we already have modified the clustered
  index record, and this would not have been possible if another active transaction had modified this secondary index record. */

  mutex_enter(&kernel_mutex);

  ut_ad(table_has(thr_get_trx(thr), index->m_table, LOCK_IX));

  auto err = rec_lock(true, Lock_mode(LOCK_X | LOCK_REC_NOT_GAP), block, heap_no, index, thr);

  mutex_exit(&kernel_mutex);

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap{};
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();
    rec_offs_init(offsets_);

    {
      Phy_rec record{index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    ut_ad(rec_queue_validate(block, rec, index, offsets));

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

db_err Lock_sys::sec_rec_read_check_and_lock(
  ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, Lock_mode mode, ulint gap_mode,
  que_thr_t *thr
) noexcept {
  ut_ad(!index->is_clustered());
  ut_ad(block->m_frame == page_align(rec));
  ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mode == LOCK_X || mode == LOCK_S);

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  const auto heap_no = page_rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  ut_ad(mode != LOCK_X || table_has(thr_get_trx(thr), index->m_table, LOCK_IX));
  ut_ad(mode != LOCK_S || table_has(thr_get_trx(thr), index->m_table, LOCK_IS));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list or a
  database recovery is running. */

  if ((page_get_max_trx_id(block->m_frame) >= m_trx_sys->get_min_trx_id() || recv_recovery_on) && !page_rec_is_supremum(rec)) {

    rec_convert_impl_to_expl(block, rec, index, offsets);
  }

  auto err = rec_lock(false, Lock_mode(mode | gap_mode), block, heap_no, index, thr);

  mutex_exit(&kernel_mutex);

  ut_ad(rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err Lock_sys::clust_rec_read_check_and_lock(
  ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, const ulint *offsets, enum Lock_mode mode,
  ulint gap_mode, que_thr_t *thr
) noexcept {
  ut_ad(index->is_clustered());
  ut_ad(block->m_frame == page_align(rec));
  ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));
  ut_ad(gap_mode == LOCK_ORDINARY || gap_mode == LOCK_GAP || gap_mode == LOCK_REC_NOT_GAP);
  ut_ad(rec_offs_validate(rec, index, offsets));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  const auto heap_no = page_rec_get_heap_no(rec);

  mutex_enter(&kernel_mutex);

  ut_ad(mode != LOCK_X || table_has(thr_get_trx(thr), index->m_table, LOCK_IX));
  ut_ad(mode != LOCK_S || table_has(thr_get_trx(thr), index->m_table, LOCK_IS));

  if (likely(heap_no != PAGE_HEAP_NO_SUPREMUM)) {

    rec_convert_impl_to_expl(block, rec, index, offsets);
  }

  auto err = rec_lock(false, Lock_mode(mode | gap_mode), block, heap_no, index, thr);

  mutex_exit(&kernel_mutex);

  ut_ad(rec_queue_validate(block, rec, index, offsets));

  return err;
}

db_err Lock_sys::clust_rec_read_check_and_lock_alt(
  ulint flags, const Buf_block *block, const rec_t *rec, const Index *index, Lock_mode mode, ulint gap_mode, que_thr_t *thr
) noexcept {
  mem_heap_t *tmp_heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  rec_offs_init(offsets_);

  {
    Phy_rec record{index, rec};

    offsets = record.get_all_col_offsets(offsets, &tmp_heap, Current_location());
  }

  auto err = clust_rec_read_check_and_lock(flags, block, rec, index, offsets, mode, gap_mode, thr);

  if (tmp_heap != nullptr) {
    mem_heap_free(tmp_heap);
  }

  return err;
}

bool Lock_sys::trx_has_no_waiters(const Trx *trx) noexcept {
  mutex_enter(&kernel_mutex);

  for (auto lock = UT_LIST_GET_LAST(trx->m_trx_locks); lock != nullptr; lock = UT_LIST_GET_PREV(m_trx_locks, lock)) {

    if (lock->type() == LOCK_REC) {

      auto page_id = lock->page_id();

      if (auto it = m_rec_locks.find(page_id); it != m_rec_locks.end()) {
        for (auto lock : it->second) {
          if (lock->is_waiting()) {
            mutex_exit(&kernel_mutex);
            return true;
          }
        }
      }

    } else {

      ut_ad(lock->type() == LOCK_TABLE);

      for (auto table_lock = UT_LIST_GET_NEXT(m_table.m_locks, lock); table_lock != nullptr;
           table_lock = UT_LIST_GET_NEXT(m_table.m_locks, table_lock)) {

        if (table_lock->is_waiting()) {
          mutex_exit(&kernel_mutex);
          return true;
        }
      }
    }
  }

  mutex_exit(&kernel_mutex);

  return false;
}

Lock_sys *Lock_sys::create(Trx_sys *trx_sys, ulint n_cells) noexcept {
  auto ptr = ut_new(sizeof(Lock_sys));
  return ptr == nullptr ? nullptr : new (ptr) Lock_sys(trx_sys, n_cells);
}

void Lock_sys::destroy(Lock_sys *&lock_sys) noexcept {
  call_destructor(lock_sys);
  ut_delete(lock_sys);
  lock_sys = nullptr;
}
