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

#include "lock0priv.h"

#include "ut0ucode.h"
#include "trx0purge.h"

#include <algorithm>
#include <array>
#include <bit>
#include <functional>
#include <thread>
#include <vector>

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
bool Lock::mode_stronger_or_eq(LockMode lhs, LockMode rhs) noexcept {
  ut_ad(lhs == LOCK_X || lhs == LOCK_S || lhs == LOCK_IX || lhs == LOCK_IS);
  ut_ad(rhs == LOCK_X || rhs == LOCK_S || rhs == LOCK_IX || rhs == LOCK_IS);

  return Lock_strength_matrix[lhs][rhs];
}

bool Lock::has_to_wait_for(const Lock *lock) const noexcept {
  if (m_trx != lock->m_trx && !mode_compatible(mode(), lock->mode())) {
    if (type() == LOCK_REC) {
      ut_ad(lock->type() == LOCK_REC);
      ut_ad(rec_is_nth_bit_set(LOCK_BIT));
      ut_ad(lock->rec_is_nth_bit_set(LOCK_BIT));

      return lock->rec_blocks(m_trx, precise_mode());

    } else {
      return true;
    }
  } else {
    return false;
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

std::string Lock::table_to_string(System &sys) const noexcept {
  ut_ad(sys.m_lock_sys->latched());
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

std::string Lock::rec_to_string(System &sys) const noexcept {
  ut_ad(sys.m_lock_sys->latched());
  ut_a(type() == LOCK_REC);

  auto str = std::format(
    "RECORD LOCKS key {:#x} index {{ {}, {} }} of table {} trx id {}",
    key().m_fold,
    rec_index()->m_id,
    rec_index()->m_name,
    rec_index()->m_table->m_name,
    m_trx->m_id
  );

  if (mode() == LOCK_S) {
    str += " lock mode S";
  } else if (mode() == LOCK_X) {
    str += " LockMode X";
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

  if (!rec_is_nth_bit_set(LOCK_BIT)) {
    str += " released";
  }

  str += "\n";

  return str;
}

std::string Lock::to_string(System &sys) const noexcept {
  return std::format(
    "Lock {} type: {} mode: {}, - {}",
    (void *)this,
    get_type_str(),
    get_mode_str(),
    type() == LOCK_REC ? rec_to_string(sys) : table_to_string(sys)
  );
}

LockSys::LockSys(System &sys) noexcept : m_sys(sys) {}

LockSys::~LockSys() noexcept {}

/* The latches, see the LATCHING comment of lock0lock.h. */

namespace {

/** The deadlock check the thread owes (LockSys::suspect()). */
thread_local Trx *t_suspect{};

}  // namespace

uint32_t LockSys::table_shard_no(const Table *table) noexcept {
  return uint32_t((table->m_id * 0x9E3779B97F4A7C15ULL) >> 58) % N_TABLE_SHARDS;
}

LockSys::Held &LockSys::held() const noexcept {
  /* The latches of a lock system the thread holds. */
  thread_local Held t_held;

  auto &h = t_held;

  if (unlikely(h.m_owner != this)) {
    /* Another lock system (another System of the process): the thread must
    hold none of its latches. */
    ut_a(h.m_x == 0 && h.m_keys[0] < 0 && h.m_table < 0);

    h = Held{};
    h.m_owner = this;
  }

  return h;
}

Trx *&LockSys::suspect() noexcept {
  return t_suspect;
}

void LockSys::x_lock() noexcept {
  auto &h = held();

  if (h.m_x > 0) {
    ++h.m_x;
    return;
  }

  ut_ad(h.m_keys[0] < 0 && h.m_table < 0);
  ut_ad(!mutex_own(&m_sys.m_trx_sys->m_mutex));
  ut_ad(TrxMutex::n_held() == 0);

  /* The latching order: the key shards, then the table shards. */
  for (auto &shard : m_key_shards) {
    shard.m_mutex.lock();
  }

  for (auto &shard : m_table_shards) {
    shard.m_mutex.lock();
  }

  h.m_x = 1;
}

void LockSys::x_unlock() noexcept {
  auto &h = held();

  ut_ad(h.m_x > 0);

  if (--h.m_x > 0) {
    return;
  }

  for (auto &shard : m_table_shards) {
    shard.m_mutex.unlock();
  }

  for (auto &shard : m_key_shards) {
    shard.m_mutex.unlock();
  }
}

LockSys::KeyLatch::KeyLatch(LockSys &lock_sys, LockKey page_id) noexcept : KeyLatch(lock_sys, ShardNo{key_shard_no(page_id)}) {}

LockSys::KeyLatch::KeyLatch(LockSys &lock_sys, ShardNo shard_no) noexcept : m_lock_sys(lock_sys) {
  if (m_lock_sys.x_own()) {
    m_noop = true;
    return;
  }

  auto &h = m_lock_sys.held();

  /* The latching order. */
  ut_ad(h.m_keys[0] < 0 && h.m_table < 0);
  ut_ad(!mutex_own(&m_lock_sys.m_sys.m_trx_sys->m_mutex));
  ut_ad(TrxMutex::n_held() == 0);
  const auto shard = shard_no.m_no;

  m_lock_sys.m_key_shards[shard].m_mutex.lock();

  h.m_keys[0] = int32_t(shard);
}

LockSys::KeyLatch::KeyLatch(LockSys &lock_sys, LockKey a, LockKey b) noexcept : m_lock_sys(lock_sys) {
  if (m_lock_sys.x_own()) {
    m_noop = true;
    return;
  }

  auto &h = m_lock_sys.held();

  /* The latching order. */
  ut_ad(h.m_keys[0] < 0 && h.m_table < 0);
  ut_ad(!mutex_own(&m_lock_sys.m_sys.m_trx_sys->m_mutex));
  ut_ad(TrxMutex::n_held() == 0);
  auto sa = key_shard_no(a);
  auto sb = key_shard_no(b);

  /* Two shards: in the order of their numbers (the latching order). */
  if (sa > sb) {
    std::swap(sa, sb);
  }

  m_lock_sys.m_key_shards[sa].m_mutex.lock();
  h.m_keys[0] = int32_t(sa);

  if (sb != sa) {
    m_lock_sys.m_key_shards[sb].m_mutex.lock();
    h.m_keys[1] = int32_t(sb);
  }
}

LockSys::KeyLatch::~KeyLatch() noexcept {
  if (m_noop) {
    return;
  }

  auto &h = m_lock_sys.held();

  if (h.m_keys[1] >= 0) {
    m_lock_sys.m_key_shards[h.m_keys[1]].m_mutex.unlock();
    h.m_keys[1] = -1;
  }

  m_lock_sys.m_key_shards[h.m_keys[0]].m_mutex.unlock();
  h.m_keys[0] = -1;

}

LockSys::TableLatch::TableLatch(LockSys &lock_sys, const Table *table) noexcept : TableLatch(lock_sys, ShardNo{table_shard_no(table)}) {}

LockSys::TableLatch::TableLatch(LockSys &lock_sys, ShardNo shard_no) noexcept : m_lock_sys(lock_sys) {
  if (m_lock_sys.x_own()) {
    m_noop = true;
    return;
  }

  auto &h = m_lock_sys.held();

  /* After a key shard latch, if any: the latching order. */
  ut_ad(h.m_table < 0);
  ut_ad(!mutex_own(&m_lock_sys.m_sys.m_trx_sys->m_mutex));
  ut_ad(TrxMutex::n_held() == 0);

  const auto shard = shard_no.m_no;

  m_lock_sys.m_table_shards[shard].m_mutex.lock();

  h.m_table = int32_t(shard);
}

LockSys::TableLatch::~TableLatch() noexcept {
  if (m_noop) {
    return;
  }

  auto &h = m_lock_sys.held();

  m_lock_sys.m_table_shards[h.m_table].m_mutex.unlock();
  h.m_table = -1;
}

DbErr LockSys::rec_lock_for(Trx *trx, LockMode mode, LockKey key, const Index *index, QueThr *thr) noexcept {
  DbErr err;

  {
    KeyLatch latch(*this, key);

    if (mode & LOCK_INSERT_INTENTION) {
      /* As rec_insert_check_and_lock(): no lock unless it waits. */
      if (rec_other_has_conflicting(key, mode, trx) != nullptr) {
        err = rec_enqueue_waiting(mode, key, index, trx, thr);
      } else {
        err = DB_SUCCESS;
      }
    } else {
      err = rec_lock(false, mode, key, index, trx, thr);
    }
  }

  return deadlock_after_wait(err);
}

bool LockSys::check_trx_id_sanity(
  trx_id_t trx_id, const Rec rec, const Index *index, const ulint *offsets
) noexcept {
  bool is_ok{true};

  ut_ad(rec.offs_validate(index, offsets));

  /* A sanity check: the trx_id in rec must be smaller than the global trx id counter */

  const auto max_trx_id = m_sys.m_trx_sys->max_trx_id();

  /* An id of another node may be ahead of the local clock. */
  if (trx_id >= max_trx_id && m_sys.m_trx_sys->id_is_local(trx_id)) {
    log_err("Transaction id associated with record");
    log_err(rec.to_string());
    log_err(std::format(
      "\nis {} which is higher than the global trx id counter {}"
      " The table is corrupt. You have to do dump + drop + reimport.",
      trx_id,
      max_trx_id
    ));

    is_ok = false;
  }

  return is_ok;
}

bool LockSys::clust_rec_cons_read_sees(const Rec rec, Index *index, const ulint *offsets, ReadView *view) const noexcept {
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec.offs_validate(index, offsets));

  /* NOTE that we call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  trx system mutex here! */

  const auto trx_id = row_get_rec_trx_id(rec, index, offsets);

  return view->sees_trx_id(trx_id);
}

bool LockSys::sec_rec_cons_read_sees(const Rec rec, ReadView *view) const noexcept {
  ut_ad(page_rec_is_user_rec(rec));

  /* NOTE that we might call this function while holding the search
  system latch. To obey the latching order we must NOT reserve the
  trx system mutex here! */

  if (m_sys.m_recv->m_recovery_on) {
    return false;
  }

  return view->sees_trx_id(TrxSys::read_trx_id(rec.get() + row_get_sec_trx_id_offset(rec)));
}

Trx *LockSys::sec_rec_some_has_impl_off_trx_sys_mutex(const Rec rec, const Index *index, const ulint *offsets) noexcept {
  const page_t *page = rec.page_align();

  /* No lock system latch, nor the trx sys mutex (lock0lock.h). */
  ut_ad(nothing_latched());
  ut_ad(!mutex_own(&m_sys.m_trx_sys->m_mutex));
  ut_ad(!index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec.offs_validate(index, offsets));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list, or
  database recovery is running. We do not write the changes of a page
  max trx id to the log, and therefore during recovery, this value
  for a page may be incorrect. The smallest active id is read without
  the trx sys mutex: a stale value is smaller, it only makes us look. */

  if (page_get_max_trx_id(page) < m_sys.m_trx_sys->m_low_water.load(std::memory_order_acquire) && !m_sys.m_recv->m_recovery_on) {

    return nullptr;
  }

  /* Ok, in this case it is possible that some transaction has an
  implicit x-lock. We have to look in the clustered index. */

  if (!check_trx_id_sanity(page_get_max_trx_id(page), rec, index, offsets)) {
    buf_page_print(page, 0);

    /* The page is corrupt: try to avoid a crash by returning
    nullptr */
    return nullptr;
  }

  /* The owner, pinned. */
  return m_sys.m_row_vers->impl_x_locked_off_trx_sys_mutex(rec, index, offsets);
}

[[nodiscard]] ulint LockSys::get_n_rec_locks() noexcept {
  Exclusive x(*this);

  ulint n{};

  for (auto &shard : m_key_shards) {
    n += shard.m_locks.size();
  }

  return n;
}

bool LockSys::print_info_summary(bool nowait) const noexcept {
  /* The lock system latched X, then the trx sys mutex (the latching order,
  lock0lock.h): both are released by print_info_all_transactions(). If
  nowait is false, wait on the trx system mutex, otherwise return
  immediately if fail to obtain the mutex. */
  auto self = const_cast<LockSys *>(this);

  self->x_lock();

  if (!nowait) {
    mutex_enter(&m_sys.m_trx_sys->m_mutex);
  } else if (mutex_enter_nowait(&m_sys.m_trx_sys->m_mutex)) {
    self->x_unlock();
    log_info("FAIL TO OBTAIN TRX SYSTEM MUTEX, SKIP LOCK INFO PRINTING");
    return false;
  }

  if (is_deadlock_found()) {
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

  log_info("Trx id high water ", m_sys.m_trx_sys->max_trx_id());

  log_info(std::format(
    "Purge done for trx's n:o < {} undo n:o < {}", m_sys.m_trx_sys->m_purge->m_purge_trx_no, m_sys.m_trx_sys->m_purge->m_purge_undo_no
  ));

  log_info("History list length ", m_sys.m_trx_sys->m_rseg_history_len.load());

  return true;
}

void LockSys::print_info_all_transactions() noexcept {
  ulint nth_trx{};
  ulint nth_lock{};

  log_info("LIST OF TRANSACTIONS FOR EACH SESSION:");

  /* First print info on non-active transactions */

  for (auto &shard : m_sys.m_trx_sys->m_client_shards) {
    std::lock_guard<ut::Latch> guard(shard.m_latch);

    for (auto trx : shard.m_trxs) {
      if (trx->m_conc_state == TRX_NOT_STARTED) {
        log_info("---");
        log_info(trx->to_string(600));
      }
    }
  }

  const Lock *lock{};
  Trx *pinned{};

  /* The pin of the last round. */
  const auto unpin = [&pinned]() {
    if (pinned != nullptr) {
      TrxSys::unpin(pinned);
      pinned = nullptr;
    }
  };

  for (;;) {
    ulint i{};

    unpin();

    /* We temporarily release the latches when reading a database page in below:
    look the transaction up again, pinned: it can't reach C4 of its commit
    meanwhile. A list of ids, it is short lived. */
    const auto ids = m_sys.m_trx_sys->active_ids();

    if (nth_trx < ids.size()) {
      pinned = m_sys.m_trx_sys->pin_active(ids[nth_trx]);

      if (pinned == nullptr) {
        ++nth_trx;
        nth_lock = 0;
        continue;
      }
    }

    auto trx = pinned;

    if (trx == nullptr) {
      mutex_exit(&m_sys.m_trx_sys->m_mutex);
      x_unlock();

      ut_ad(validate());

      return;
    }

    if (nth_lock == 0) {
      log_info("---");
      log_info(trx->to_string(600));

      if (trx->m_read_view) {
        log_info(std::format(
          "Trx read view at {}, sees ids < {}",
          trx->m_read_view->m_snap_word,
          trx->m_read_view->m_watermark
        ));
      }

      if (trx->m_que_state == TRX_QUE_LOCK_WAIT) {
        log_info(std::format(
          "------- TRX HAS BEEN WAITING {} SEC FOR THIS LOCK TO BE GRANTED:", difftime(time(nullptr), trx->m_wait_started)
        ));

        log_info(trx->m_wait_lock->to_string(m_sys));

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
      /* A lock is on a key, there's no page to read for it. */
      log_info(lock->rec_to_string(m_sys));
    } else {
      ut_ad(lock->type() == LOCK_TABLE);

      log_info(lock->table_to_string(m_sys));
    }

    ++nth_lock;

    if (nth_lock >= 10) {
      ++nth_trx;
      log_info("10 LOCKS PRINTED FOR THIS TRX: SUPPRESSING FURTHER PRINTS");
      nth_lock = 0;
    }
  }
}

#ifdef UNIV_DEBUG
bool LockSys::table_queue_validate(Table *table) noexcept {
  ut_ad(owns_table(table));

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

bool LockSys::rec_queue_validate(LockKey key, const Index *index) noexcept {
  KeyLatch latch(*this, key);

  auto q = rec_queue(key);

  if (q == nullptr) {
    return true;
  }

  for (auto lock : *q) {
    validate_trx_state(lock->m_trx);
    ut_a(lock->key() == key);
    ut_a(index == nullptr || lock->rec_index() == index);

    if (!lock->rec_is_nth_bit_set(LOCK_BIT)) {
      continue;
    }

    if (lock->is_waiting()) {
      /* A waiting lock has a reason ahead of it. */
      ut_a(rec_has_to_wait_in_queue(*q, lock));

    } else if (!lock->rec_is_gap() && !lock->rec_is_insert_intention()) {
      /* No two granted locks on the record conflict. */
      const auto check_mode = lock->mode() == LOCK_S ? LOCK_X : LOCK_S;

      ut_a(rec_other_has_expl_req(key, check_mode, 0, 0, lock->m_trx) == nullptr);
    }
  }

  return true;
}

bool LockSys::validate() noexcept {
  ut_ad(nothing_latched());

  std::vector<LockKey> pages;

  {
    /* All of the lock system, then the trx sys mutex (lock0lock.h). */
    Exclusive x(*this);

    mutex_enter(&m_sys.m_trx_sys->m_mutex);

    for (const auto id : m_sys.m_trx_sys->active_ids()) {
      /* Pinned: it can't reach C4 of its commit meanwhile. */
      auto trx = m_sys.m_trx_sys->pin_active(id);

      if (trx == nullptr) {
        continue;
      }

      for (auto lock : trx->m_trx_locks) {

        if (lock->type() == LOCK_TABLE) {
          (void)table_queue_validate(lock->m_table.m_table);
        }
      }

      TrxSys::unpin(trx);
    }

    mutex_exit(&m_sys.m_trx_sys->m_mutex);

    for (auto &shard : m_key_shards) {
      for (auto &[page_id, rec_locks] : shard.m_locks) {
        ut_a(!rec_locks.empty());
        validate_trx_state(rec_locks.front()->m_trx);
        pages.push_back(page_id);
      }
    }
  }

  for (auto key : pages) {
    (void)rec_queue_validate(key, nullptr);
  }

  return true;
}
#endif /* UNIV_DEBUG */

DbErr LockSys::rec_insert_check_and_lock(ulint flags, LockKey next, const Index *index, QueThr *thr, bool *inherit) noexcept {
  if (flags & BTR_NO_LOCKING_FLAG) {
    *inherit = false;

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);

  /* When inserting a record into an index, the table must be at
  least IX-locked or we must be building an index, in which case
  the table must be at least S-locked. */
  ut_ad(table_has(trx, index->m_table, LOCK_IX) || (*index->m_name == TEMP_INDEX_PREFIX && table_has(trx, index->m_table, LOCK_S)));

  DbErr err{DB_SUCCESS};

  {
    KeyLatch latch(*this, next);

    if (likely(rec_queue(next) == nullptr)) {
      *inherit = false;

      return DB_SUCCESS;
    }

    /* If another transaction has an explicit lock request which locks
    the gap, waiting or granted, on the boundary after the key, the insert
    has to wait.

    An exception is the case where the lock by the another transaction
    is a gap type lock which it placed to wait for its turn to insert. We
    do not consider that kind of a lock conflicting with our insert. This
    eliminates an unnecessary deadlock which resulted when 2 transactions
    had to wait for their insert. Both had waiting gap type lock requests
    on the successor, which produced an unnecessary deadlock. */

    const auto type_mode = LockMode(LOCK_X | LOCK_GAP | LOCK_INSERT_INTENTION);

    if (rec_other_has_conflicting(next, type_mode, trx) != nullptr) {
      err = rec_enqueue_waiting(type_mode, next, index, trx, thr);
    }
  }

  *inherit = true;

  err = deadlock_after_wait(err);

  ut_ad(rec_queue_validate(next, index));

  return err;
}

void LockSys::impl_to_expl(Trx *impl_trx, LockKey key, const Index *index) noexcept {
  ut_ad(owns_key(key));

  /* V3 (lock0lock.h): impl_trx is pinned, so it can't reach C4 of its
  commit and a lock created here is released by its C5. But it may have
  passed C2 since V1: a request made since then did not find it and may
  hold a lock on the record already; its changes are committed, so there is
  no implicit lock left to convert. Checked under the key shard latch. */
  if (!m_sys.m_trx_sys->in_ids(impl_trx->m_id)) {
    return;
  }

  IF_DEBUG({
    trx_mutex_enter(impl_trx);
    ut_ad(impl_trx->m_conc_state == TRX_ACTIVE || impl_trx->m_conc_state == TRX_PREPARED);
    trx_mutex_exit(impl_trx);
  })

  /* If the transaction has no explicit x-lock set on the
  record, set one for it */

  const auto type_mode = LockMode(LOCK_X | LOCK_REC_NOT_GAP);

  if (rec_has_expl(key, type_mode, impl_trx)) {
    return;
  }

  /* Another transaction holds a granted lock on the record that conflicts
  with X: its lookup ran after the change of impl_trx was gone (committed
  past C2, or rolled back), else it would have converted first and waited.
  The lookup of this converter is stale: no implicit lock to convert. */
  if (auto q = rec_queue(key); q != nullptr) {
    for (auto lock : *q) {
      if (lock->m_trx != impl_trx && lock->rec_is_nth_bit_set(LOCK_BIT) && !lock->is_waiting() && !lock->rec_is_gap() &&
          !lock->rec_is_insert_intention()) {
        return;
      }
    }
  }

  (void)rec_add_to_queue(type_mode, key, index, impl_trx);
}

DbErr LockSys::clust_rec_modify_check_and_lock(
  ulint flags, LockKey key, const Rec rec, const Index *index, const ulint *offsets, QueThr *thr
) noexcept {
  ut_ad(rec.offs_validate(index, offsets));
  ut_ad(index->is_clustered());

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);

  ut_ad(table_has(trx, index->m_table, LOCK_IX));

  /* If a transaction has no explicit x-lock set on the record, set one for
  it: its owner is found and pinned before the latch (lock0lock.h). */
  auto impl_trx = clust_rec_some_has_impl(rec, index, offsets);

  DbErr err;

  {
    KeyLatch latch(*this, key);

    if (impl_trx != nullptr) {
      impl_to_expl(impl_trx, key, index);
    }

    err = rec_lock(true, LockMode(LOCK_X | LOCK_REC_NOT_GAP), key, index, trx, thr);
  }

  if (impl_trx != nullptr) {
    TrxSys::unpin(impl_trx);
  }

  err = deadlock_after_wait(err);

  ut_ad(rec_queue_validate(key, index));

  return err;
}

DbErr LockSys::sec_rec_modify_check_and_lock(ulint flags, LockKey key, const Index *index, QueThr *thr) noexcept {
  ut_ad(!index->is_clustered());

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  /* Another transaction cannot have an implicit lock on the record, because when we come here, we already have modified the clustered
  index record, and this would not have been possible if another active transaction had modified this secondary index record. */

  auto trx = thr_get_trx(thr);

  ut_ad(table_has(trx, index->m_table, LOCK_IX));

  DbErr err;

  {
    KeyLatch latch(*this, key);

    err = rec_lock(true, LockMode(LOCK_X | LOCK_REC_NOT_GAP), key, index, trx, thr);
  }

  err = deadlock_after_wait(err);

  ut_ad(rec_queue_validate(key, index));

  return err;
}

DbErr LockSys::sec_rec_read_check_and_lock(
  ulint flags, LockPos pos, const Rec rec, const Index *index, const ulint *offsets, LockMode mode, ulint gap_mode, QueThr *thr
) noexcept {
  ut_ad(!index->is_clustered());
  ut_ad(pos.m_fence || rec.offs_validate(index, offsets));
  ut_ad(mode == LOCK_X || mode == LOCK_S);

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);

  ut_ad(mode != LOCK_X || table_has(trx, index->m_table, LOCK_IX));
  ut_ad(mode != LOCK_S || table_has(trx, index->m_table, LOCK_IS));

  /* Some transaction may have an implicit x-lock on the record only
  if the max trx id for the page >= min trx id for the trx list or a
  database recovery is running (the smallest active id read without the
  trx sys mutex: a stale value is smaller, it only makes us look). Its
  owner is found and pinned before the latch (lock0lock.h). A fence has
  no record to have one on. */
  Trx *impl_trx{};

  if (!pos.m_fence &&
      (page_get_max_trx_id(rec.page_align()) >= m_sys.m_trx_sys->m_low_water.load(std::memory_order_acquire) || m_sys.m_recv->m_recovery_on)) {

    impl_trx = sec_rec_some_has_impl_off_trx_sys_mutex(rec, index, offsets);
  }

  DbErr err;

  {
    KeyLatch latch(*this, pos.m_key);

    if (impl_trx != nullptr) {
      impl_to_expl(impl_trx, pos.m_key, index);
    }

    err = rec_lock(false, fence_mode(pos, LockMode(mode | gap_mode)), pos.m_key, index, trx, thr);
  }

  if (impl_trx != nullptr) {
    TrxSys::unpin(impl_trx);
  }

  err = deadlock_after_wait(err);

  ut_ad(rec_queue_validate(pos.m_key, index));

  return err;
}

DbErr LockSys::clust_rec_read_check_and_lock(
  ulint flags, LockPos pos, const Rec rec, const Index *index, const ulint *offsets, enum LockMode mode, ulint gap_mode, QueThr *thr
) noexcept {
  ut_ad(index->is_clustered());
  ut_ad(gap_mode == LOCK_ORDINARY || gap_mode == LOCK_GAP || gap_mode == LOCK_REC_NOT_GAP);
  ut_ad(pos.m_fence || rec.offs_validate(index, offsets));

  if (flags & BTR_NO_LOCKING_FLAG) {

    return DB_SUCCESS;
  }

  auto trx = thr_get_trx(thr);

  ut_ad(mode != LOCK_X || table_has(trx, index->m_table, LOCK_IX));
  ut_ad(mode != LOCK_S || table_has(trx, index->m_table, LOCK_IS));

  /* The owner of an implicit lock, found and pinned before the latch. */
  Trx *impl_trx{};

  if (likely(!pos.m_fence)) {

    impl_trx = clust_rec_some_has_impl(rec, index, offsets);
  }

  DbErr err;

  {
    KeyLatch latch(*this, pos.m_key);

    if (impl_trx != nullptr) {
      impl_to_expl(impl_trx, pos.m_key, index);
    }

    err = rec_lock(false, fence_mode(pos, LockMode(mode | gap_mode)), pos.m_key, index, trx, thr);
  }

  if (impl_trx != nullptr) {
    TrxSys::unpin(impl_trx);
  }

  err = deadlock_after_wait(err);

  ut_ad(rec_queue_validate(pos.m_key, index));

  return err;
}

DbErr LockSys::clust_rec_read_check_and_lock_alt(
  ulint flags, LockPos pos, const Rec rec, const Index *index, LockMode mode, ulint gap_mode, QueThr *thr
) noexcept {
  MemHeap *tmp_heap{};
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();
  rec_offs_init(offsets_);

  if (!pos.m_fence) {
    PhyRec record{index, rec};

    offsets = record.get_all_col_offsets(offsets, &tmp_heap, Current_location());
  }

  auto err = clust_rec_read_check_and_lock(flags, pos, rec, index, offsets, mode, gap_mode, thr);

  if (tmp_heap != nullptr) {
    mem_heap_free(tmp_heap);
  }

  return err;
}

bool LockSys::trx_has_no_waiters(const Trx *trx) noexcept {
  /* The queues of the locks of trx, in any shard. */
  Exclusive x(*this);

  for (auto lock = UT_LIST_GET_LAST(trx->m_trx_locks); lock != nullptr; lock = UT_LIST_GET_PREV(m_trx_locks, lock)) {

    if (lock->type() == LOCK_REC) {

      auto page_id = lock->key();

      /* A lock of another transaction waits for a record that trx has locked. */
      if (auto q = rec_queue(page_id); q != nullptr) {
        for (auto waiting : (*q)) {
          if (waiting == lock || waiting->m_trx == trx || !waiting->is_waiting()) {
            continue;
          }

          if (lock->rec_is_nth_bit_set(LOCK_BIT) && waiting->rec_is_nth_bit_set(LOCK_BIT)) {
            return false;
          }
        }
      }

    } else {

      ut_ad(lock->type() == LOCK_TABLE);

      for (auto table_lock = UT_LIST_GET_NEXT(m_table.m_locks, lock); table_lock != nullptr;
           table_lock = UT_LIST_GET_NEXT(m_table.m_locks, table_lock)) {

        if (table_lock->is_waiting()) {
          return false;
        }
      }
    }
  }

  return true;
}

