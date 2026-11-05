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

/** @file include/trx0sys.h
Transaction system

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "srv0sys.h"

#include "innodb0types.h"

#include "buf0buf.h"
#include "data0type.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0types.h"
#include "read0types.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0types.h"
#include "ut0byte.h"
#include "ut0hlc.h"

#include <array>
#include <bit>
#include <tuple>
#include <utility>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "ut0lst.h"
#include "ut0lflist.h"

/* The typedef for rseg slot in the file copy */
using trx_sysf_rseg_t = byte;

/** The automatically created system rollback segment has this id */
constexpr ulint TRX_SYS_SYSTEM_RSEG_ID = 0;

/** The transaction system tablespace.
Space id and page no where the trx system file copy resides */
constexpr auto TRX_SYS_SPACE = SYS_TABLESPACE;

/** Page numnber of the transaction system meta data. */
constexpr auto TRX_SYS_PAGE_NO = FSP_TRX_SYS_PAGE_NO;

/** The offset of the transaction system header on the page */
constexpr auto TRX_SYS = FSEG_PAGE_DATA;

/** Transaction system header */
/*@{ */

/** The high water of the clock, in microseconds since the UNIX epoch, 8 bytes:
every value the clock issued is below it plus TRX_SYS_HLC_MARGIN_US. See
TrxSys::hlc_issued(). */
constexpr ulint TRX_SYS_HLC_STORE = 0;

/** Segment header for the tablespace segment the trx system is created into */
constexpr ulint TRX_SYS_FSEG_HEADER = 8;

/** The start of the array of rollback segment specification slots */
constexpr ulint TRX_SYS_RSEGS = 8 + FSEG_HEADER_SIZE;

/*@} */

/** Maximum number of rollback segments: the number of segment
specification slots in the transaction system array; rollback segment
id must fit in one byte, therefore 256; each slot is currently 8 bytes
in size */
constexpr ulint TRX_SYS_N_RSEGS = 256;

static_assert(UNIV_PAGE_SIZE >= 4096, "error UNIV_PAGE_SIZE < 4096");

/* Rollback segment specification slot offsets */
/*-------------------------------------------------------------*/

/** Tablespace where the segment header is placed. */
constexpr auto TRX_SYS_RSEG_SPACE = SYS_TABLESPACE;

/** Page number where the segment header is placed; this is FIL_NULL if the slot is unused */
constexpr page_no_t TRX_SYS_RSEG_PAGE_NO = 4;

/*-------------------------------------------------------------*/

/* Size of a rollback segment specification slot */
constexpr ulint TRX_SYS_RSEG_SLOT_SIZE = 8;

/** Past the rollback segment slots: the format of the data files, 8 bytes of
magic and a version of 4 bytes. There is no upgrade: a file of any other format
is refused at startup. */
constexpr ulint TRX_SYS_FORMAT_MAGIC = TRX_SYS_RSEGS + TRX_SYS_N_RSEGS * TRX_SYS_RSEG_SLOT_SIZE;
constexpr ulint TRX_SYS_FORMAT_VERSION = TRX_SYS_FORMAT_MAGIC + 8;

/** The epoch of the clock (8 bytes) and the node id of the instance (4 bytes). */
constexpr ulint TRX_SYS_EPOCH = TRX_SYS_FORMAT_VERSION + 4;
constexpr ulint TRX_SYS_NODE = TRX_SYS_EPOCH + 8;

/** The magic of the format, "IBHLCFMT". */
constexpr uint64_t TRX_SYS_FORMAT_MAGIC_N = 0x4942484C43464D54ULL;

/** The format: 1 for trx ids and numbers that are clock values (ut0hlc.h,
row0sysf.h). The files before it have no magic, and counters for ids. */
constexpr uint32_t TRX_SYS_FORMAT_VERSION_N = 2;

/** How far above the stored high water the clock may issue, in microseconds
(10 s). A thread that issues a value more than half of it above the store writes
the store, or the master thread does, and the others go on: at most one fsync per 5 s of the clock. A restart
continues above the store plus this, so it can be 10 s ahead of the wall clock
for at most 10 s; the clock keeps going by last + 1 until the wall clock passes it. */
constexpr uint64_t TRX_SYS_HLC_MARGIN_US = 10000000;

/** The clock of the list of transactions: TrxSys::m_clock, in words (ut0hlc.h). */
struct TrxClock {
  [[nodiscard]] uint64_t read() const noexcept { return m_clock->read_word(); }
  [[nodiscard]] uint64_t tick() noexcept { return m_clock->next_word(m_clock->wall()); }
  [[nodiscard]] uint64_t ticket() noexcept { return tick() | m_clock->node(); }
  [[nodiscard]] static uint64_t spread(uint64_t stamp) noexcept { return stamp >> ut::HLC_TIME_SHIFT; }

  ut::TimeService *m_clock;
};

/** The payload of a node of the list of transactions. */
struct TrxEntry {
  /** The transaction number: LSN_MAX, the id as an intent, the number. */
  std::atomic<trx_id_t> m_no{LSN_MAX};

  /** The transaction, until C2 of its commit. */
  std::atomic<Trx *> m_trx{};
};

using TrxList = ut::LfList<TrxEntry, TrxClock>;
using TrxNode = TrxList::Node;

/** READ VIEWS

A read view is a timestamp. Opening one takes a slot and two loads; there is no
walk, no list of ids, no mutex. What it sees is decided row by row, from the id on
the row and a table of the commits. This is the shape multi writer needs too: a read
compares against a time, not against a list of local active ids.

A word W(t) of the clock (ut0hlc.h) is an id of node 0: epoch, time. Words only grow,
and every operation on them is in one total order. The stamps are:

  id    of T, at its start: a word | node           (DB_TRX_ID)
  no    of T, at C1: a word | node, in the rseg mutex (m_no, purge's order, on disk)
  c     of T, at C6: a word                         (visibility, in memory)
  S     of a view: a read of the word

For one transaction, id < no < c, in clock order. S and c are compared with
themselves only, ids with W (below) and with the word of S by the time part: all
are words, so all comparisons are plain integer compares.

Two stamps because no is assigned before the commit is durable (under the rseg mutex,
for the order of the history lists) and c when it's visible: a view with S >= no
must not see a transaction that is yet to reach C6.

THE COMMIT TABLE

m_ids: id to node, sharded, each shard with a latch. A node has the stamp m_seq: 0
active (and prepared), PENDING committing, or c. The node is in the table from T's
start, before T writes a row with its id, until it is retired, and it outlives
the Trx, whose object may be reused at once.

C6: store PENDING; c = next word; CAS PENDING to c. A reader that meets PENDING takes
a word of its own and does the same CAS. The brief said wait here; helping needs no
wait: the stamp has one final value, whoever wins, and a helper's word is above its
S so the view doesn't see T. Only the winner's value counts.

VISIBILITY of id I for a view (S, W), in order, read under a page latch or from a
copy of a record, in any case with a fixed I:

  1. I is its creator's, for a normal view: visible.
  2. I < W: visible.
  3. the word of I > S: not visible.
  4. the table. Final stamp 0: not visible. c: visible iff c <= S. Not found: visible.

THE PROOF

W. The retirer (retire()) reads C0 = the word, then walks the list, then publishes W
as the minimum of the limit of C0 and the ids of the nodes whose final stamp is 0
or > C0. A node is pushed before it gets its id, so T with id <= C0 was pushed before
C0 was read and the walk meets it, or it is unlinked, and then c <= C0 (rule R1). So
every id below W has c <= C0. A view reads W, then S: it sees the release of W so
S >= C0 >= c. So rule 2 is right for any id. An id ahead of C0, another node's, isn't
below W, as the limit of C0 caps W.

Rule 3. A word of I above S means I isn't issued yet or isn't local; in both cases
its commit stamp, if any, is above I, so above S.

Rule 4. Reading 0: T stored PENDING after this read of the stamp, or T is alive.
The read comes after the load of S. T's word for c is issued after its store of
PENDING, so after the load of S: c > S. So not visible, and stays so for this
view whatever comes later: its final stamp will be above S.

(a) Two reads of a row under one view agree. Each of the four cases gives one answer
for a fixed I: W and S are fixed, the final stamp is one value, and the third case
is fixed. The table can only change for the view by the entry retired, which needs
c <= S (R1 below), and then rule 4 says visible by not found, as it said by c.

(b) B depends on A: B read what A made visible, or waited for a lock A released. A's
C6 happens before that, so c_A was final before B's id or B's stamp was taken, and
all stamps are in clock order: c_A < c_B. A view with c_B <= S has c_A <= S. A's
store of the stamp happened before the load of S that returned at least c_B, so the
view can't read A's stamp as 0 or PENDING with a value above S. So it sees A too:
no gaps in a chain.

(c) update(received) only raises the word: nothing above is about a difference of
stamps, only their order, and the word stays the total order of all of them. A jump
between a commit's tick and its CAS changes nothing: the value of the stamp was fixed
at the tick, a view whose S is above it sees it, once it reads the stamp. It helps or
finds the stamp, and the helper's value, taken after the jump, would be above S.

(d) Ids of another node aren't in the table. Rule 2 for those below W. Rule 3
if the word is above S (a row applied after observe_id() moved the clock past it
has a word below every later S). Else not found, visible. What that means once
there are writers on two nodes is an open question (not solved here: the visibility of
a foreign write is that of its execution time, not of this id).

NOT FOUND. It means committed for all live views: true for: an entry retired (R1); a
transaction committed before a restart (no entry for a committed recovered one, the
same as the list of old); an id of another node. And: an active or prepared
recovered transaction is in the table from TrxSys::start(), before any view; a rolled
back T keeps its entry while its rows exist, its rollback ends with its commit (C6).
No clustered record has its id then; a secondary record may, see below, and the stamp
is right for it. A T between its id and its entry in the table has no row with its
id yet.

SECONDARY RECORDS

A secondary record has no undo of its own, so a view can't get its earlier states from
it, only its last one. The leaf record ends with a DB_TRX_ID, M: the last transaction
that changed it. It is not part of the key: not in m_n_uniq nor in the node pointers,
(key, clustered key) is unique so a comparison never gets to it. A built entry has 0.

Rule M. Every change of a record's state sets M to its transaction, in the same mtr, X
latched: an insert (RowInsert::index_entry), the reuse of a delete marked record (the
same, the difference then includes M), a delete mark or unmark
(BtreeCursor::del_mark_set_sec_rec, M is in its redo record). Nothing else writes M.

For a view, if the rules above say it sees M: the state of the record is the state for
the view, and the delete mark decides. Else: go to the clustered index.

Proof. Let the view see M. A change after M would have set a new M, so the state on the
page is the state after M. A record belongs to one row, and its writers hold the X lock
of that row until they commit: so the writers of a record commit in the order they wrote
it (a rolled back one restores what it found). Everything before M is then also seen, and
nothing after M exists: the state after M is the latest the view can see. A later version
of the row that left this key alone doesn't touch the record and doesn't change this: the
record says the key is in the row, the clustered index gives the version when it is read.

Let the view not see M. It decides through the clustered index and its undo, which
has every version: right, as it always was, whatever the state of the record.

Unlike the test of PAGE_MAX_TRX_ID against W it needs no bound on all the ids below it:
it is about one id. So it isn't held back by a long transaction, or a watermark
behind the last commit. It costs a table lookup when M is at or above W.

Rollback. Undoing T restores the state and M stays T, which keeps the rule: T was the
last to change it. During the rollback T is active, nobody but T sees it: the clustered
index decides. After it: a view with S below c_T doesn't see T and goes to the clustered
index. A view with S at or above c_T sees T, and the restored state is the latest. If T
was retired, not found says visible, and it's such a view. After a crash, T is in the
table until its rollback ends, as an active one. A view of T itself (rule 1) after a
rollback to a savepoint: the state on the page is what T left. A cursor view isn't its
creator's for rule 1, it goes to the clustered index.

Purge removes delete marked records no view needs; it doesn't look at M.

An index being built: its records get the id of the transaction that builds it, by the
stamp of RowInsert::index_entry(). Until that one commits, views go to the clustered
index, whose row is the same under the table's lock.

THE HORIZON

An entry may go only if every view, now and later, sees it. Rule R1: unlink and erase
if c <= h, the min of C0 and the slots. The race: a view loads S, the retirer scans
the slots and misses it, retires c > S. The slot goes first:

  open:   slot.exchange(F)   F = the published floor, a lower bound of later S
          load W
          S = load word
          slot.store(S)

  retire: C0 = load word
          h = min(C0, slots)
          for c <= h, erase, unlink
          F = max(F, h), W

A slot seen set at v: v is F or S. F <= S: F was published with a value <= a C0, that
load of F comes before the load of S, so S >= C0 >= F. c <= h <= v <= S. A slot seen
0, and the view is opening: the exchange is after the read of the slot in the total
order, so its load of S is after the load of C0: S >= C0 >= h >= c. The hook
m_view_hook of the tests lets a retirer run between the two.

A long held view holds h and so the table: one entry per commit since, 64 byte node
and 16 byte table slot. That is the cost of this design (see below in the
list: the limit is 67 M nodes, and begin waits at it).

PURGE

Undo of T is needed by a view iff the view doesn't see T, c > S. Purge's view
(open_purge_view()) has S = h, its W from a walk, and:

  L = min(limit of h, the no of every T in the list with final stamp 0 or > h)

It has a slot during the walk only. A view of purge that stayed registered would hold
every commit since its open in the table, between batches. So its answer may change
during a batch: an entry above its h may be retired, and it then reads as seen. That
is only ever true of every live view, so it is safe for its two uses, the question if
an undo log must still exist and if a delete marked record must be kept; and L, what it
may remove, is fixed at the open.

Purge removes undo logs of a number below L. Let no_T < L. T's word for no is <= h
<= C0, read before the walk, so T was in the list at the walk, or it is retired.
If retired, every view sees it (R1, and rule U of the list for later ones). If met
with stamp <= h: every view has S >= h (seen slot) or S >= C0 >= h (as above): it
sees T. A T met with stamp 0 or > h has no <= L, a contradiction. A pending stamp is
helped, final above C0 >= h.

THE NUMBER: T's no allocated, not yet in the node, is missed by the walk with no <= h?
T stores its id, as an intent, in the node before it takes the number. If the walk
read LSN_MAX, the intent's store is after that read, so the number's word is after
the load of C0: no > h, it isn't below L. If it read the intent: L <= id < no. The
limit is lower than it could be, only for a moment, and that only delays purge.

m_no and c. no < c for one T: a view with no <= S may not see T (it is committing)
but the rule above is in terms of c, and uses no only as the name of the undo log:
L is below no of every unseen T. Below L purge has only seen transactions. A number is
per rseg in history order as it is taken under the rseg mutex; undo's reading of
it is as before.

WAITS. None for a view; a helper's CAS; the shard latch of the lookup, held
for a hash probe and a load. alloc fails only with 67 M entries.

THE MUTEX. Begin, commit, view open and close and the clock don't take the trx sys
mutex. A commit of a recovered transaction takes it once, for the list of those. */

/** The transaction system central memory data structure; protected by the
kernel mutex */
struct TrxSys {
  /** The instance this sub-system belongs to. */
  System &m_sys;


  /**
   * Constructor
   *
   * @param[in] fsp               File space management instance
   */
  explicit TrxSys(System &sys) noexcept;

  /**
   * Destructor
   */
  ~TrxSys() noexcept;

  /**
   * Create an instance of the transaction system.
   *
   * @param[in] fsp             File management instance to use
   *
   * @return new TrxSys instance.
   */
  /**
   * Destroy a transaction system instance.
   *
   * @param[in] recovery        Recovery flag
   */
  /** Start the transaction system.
   *
   * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] DbErr start(IbRecovery recovery) noexcept;

  /**
   * Create a new system tablespace.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] DbErr create_system_tablespace() noexcept;

  /**
   * Open an existing database instance.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] DbErr open_system_tablespace() noexcept;

  /**
   * Looks for a free slot for a rollback segment in the trx system file copy.
   *
   * @param[in] mtr               Mini-transaction handle
   *
   * @return	slot index or ULINT_UNDEFINED if not found
   */
  [[nodiscard]] ulint frseg_find_free(Mtr *mtr) noexcept;

  /**
   * Checks that trx is in the list of transactions, and active.
   *
   * @return	true if is in
   */
  [[nodiscard]] bool in_trx_list(Trx *in_trx) noexcept;

  /**
   * Looks for the trx handle with the given id, active or recovered. It may be
   * gone after the call, unless the caller knows that it can't be.
   *
   * @param[in]	trx_id	Trx id to search for
   *
   * @return	the trx handle or NULL if not found
   */
  [[nodiscard]] Trx *get_on_id(trx_id_t trx_id) noexcept;

  /**
   * @return the smallest active id, from a walk, or the high water mark.
   */
  [[nodiscard]] trx_id_t get_min_trx_id() noexcept;

  /**
   * Checks if a transaction with the given id is active.
   *
   * @param[in]	trx_id	Trx id of the transaction
   *
   * @return	true if active
   */
  [[nodiscard]] bool is_active(trx_id_t trx_id) noexcept {
    if (trx_id < m_low_water.load(std::memory_order_acquire)) {
      return false;
    }

    /* Of another node: no local transaction, no local lock. */
    if (!id_is_local(trx_id)) {
      return false;
    }

    if (trx_id >= max_trx_id()) {
      /* There must be corruption: we return true because this
      function is only called by lock_clust_rec_some_has_impl()
      and row_vers_impl_x_locked_off_kernel() and they have
      diagnostic prints in this case */

      return true;
    }

    return in_ids(trx_id);
  }

  /** @return the ids of the active transactions, biggest first. */
  [[nodiscard]] std::vector<trx_id_t> active_ids() noexcept;

  /** @return true if there is no active or recovered transaction. The mutex is held. */
  [[nodiscard]] bool trx_list_empty() noexcept;

  /** A transaction starts: in the list, its id, in the commit table. */
  void node_start(Trx *trx) noexcept;

  /** Publish the number of trx in its node (THE NUMBER, above). */
  void node_no_store(Trx *trx, trx_id_t no) noexcept {
    if (trx->m_node != nullptr) {
      static_cast<TrxNode *>(trx->m_node)->m_payload.m_no.store(no, std::memory_order_seq_cst);
    }
  }

  /**
   * C6 of a commit: T is visible to read views from here on.
   *
   * @param[in] trx   T.
   * @param[in] wall  The wall clock, read once per commit.
   *
   * @return its commit stamp.
   */
  [[nodiscard]] uint64_t commit_stamp(Trx *trx, uint64_t wall) noexcept;

  /** Out of the list of recovered transactions, if it is in. */
  void recv_list_remove(Trx *trx) noexcept;

  /**
   * Out of the active ids and the list of recovered transactions, without a
   * commit: recovery's clean up, tests.
   *
   * @param[in] trx A transaction.
   */
  void trx_list_remove(Trx *trx) noexcept;

  /** Retire what can go from the commit table, publish W and the floor: if no one
  else is at it. */
  void retire() noexcept;

  /** @return true if the view (S) sees id, from the commit table. */
  [[nodiscard]] bool visible_at(trx_id_t id, uint64_t snap) noexcept;

  /** @return the number of read views open. */
  [[nodiscard]] size_t n_read_views() const noexcept { return m_snaps.n_set(); }

  /** @return true if id has an entry in the commit table: for tests. */
  [[nodiscard]] bool id_in_table(trx_id_t id) noexcept {
    auto &shard = id_shard(id);
    std::lock_guard<ut::Latch> guard(shard.m_mutex);

    return shard.m_trxs.find(id) != nullptr;
  }

  /**
   * Allocates a new transaction id.
   *
   * @return	new, allocated trx id
   */
  [[nodiscard]] trx_id_t get_new_trx_id() noexcept { return next_word(m_clock.wall()) | m_clock.node(); }

  /**
   * Allocates a new transaction number.
   *
   * @param[in] wall  The wall clock, read once per commit.
   *
   * @return	new, allocated trx number
   */
  [[nodiscard]] trx_id_t get_new_trx_no(uint64_t wall) noexcept { return next_word(wall) | m_clock.node(); }

  /**
   * Issues a clock value, past the high water of the crash.
   *
   * @param[in] wall  The wall clock, read once per commit.
   *
   * @return the word of the new value.
   */
  [[nodiscard]] uint64_t next_word(uint64_t wall) noexcept {
    const auto word = m_clock.next_word(wall);

    hlc_issued(ut::id_to_hlc(word));

    return word;
  }

  /** Keep the guarantee of the stored high water for hlc: only a value at the edge
  of the margin stops here. The master thread stores it every second, before that,
  so no user thread pays for an fsync unless the clock outruns it: 5 s of the clock
  in 1 s, 5 million values a second, or a jump. */
  void hlc_issued(uint64_t hlc) noexcept {
    if (hlc >= m_hlc_stored.load(std::memory_order_acquire) + TRX_SYS_HLC_MARGIN_US) [[unlikely]] {
      hlc_store(hlc);
    }
  }

  /** The master thread: store the high water if half of the margin is gone. */
  void hlc_tick() noexcept {
    const auto hlc = m_clock.read();

    if (hlc >= m_hlc_stored.load(std::memory_order_acquire) + TRX_SYS_HLC_MARGIN_US / 2) {
      hlc_store(hlc);
    }
  }

  /**
   * Durably stores the high water of the clock. A crash can't reissue a value
   * issued before it: every value v returned has v < S + margin for the S that
   * is durable when it is returned, as a thread at or past that bound waits here for
   * the store (and its log flush). The restart starts the clock at S' + margin
   * or above, with S' >= S the store found on disk. S' + margin > v.
   *
   * @param[in] hlc  A value of the clock that was issued.
   */
  void hlc_store(uint64_t hlc) noexcept;

  /** @return the smallest id above all the ids issued so far: the high water
  mark. A local id read from a record is below it; an id of another node may
  be above it, until observe_id(). */
  [[nodiscard]] trx_id_t max_trx_id() const noexcept { return ut::word_id_limit(m_clock.read_word()); }

  /** Set the clock so that id is the high water mark, for tests. */
  void set_max_trx_id(trx_id_t id) noexcept {
    /* Wraps for a small id: the high water mark still comes out right. */
    m_clock.reset_word(id - ut::HLC_ID_STEP);
  }

  /** @return true if id was issued by this node. */
  [[nodiscard]] bool id_is_local(trx_id_t id) const noexcept { return ut::id_to_node(id) == m_clock.node(); }

  /** @return the full timestamp of a word of the clock on this node. */
  [[nodiscard]] ut::Timestamp timestamp_of_word(uint64_t word) const noexcept { return ut::id_to_timestamp(word | m_clock.node()); }

  /** @return a new timestamp. */
  [[nodiscard]] ut::Timestamp clock_next() noexcept { return timestamp_of_word(next_word(m_clock.wall())); }

  /**
   * Raises the clock to a timestamp received from outside, if it is ahead:
   * every later value is greater, durably. The epoch of the timestamp doesn't
   * count (as in Rust); observe_id() does that too.
   *
   * @param[in] received  The timestamp.
   *
   * @return false if refused: an hlc past the last id would stop all new
   * transactions. An hlc below the clock, or the base, changes nothing.
   */
  [[nodiscard]] bool clock_update(const ut::Timestamp &received) noexcept {
    const auto hlc = received.hlc();

    if (hlc > ut::HLC_BASE_US && !ut::hlc_fits(hlc)) {
      return false;
    }

    m_clock.update_hlc(hlc);

    if (ut::hlc_fits(hlc)) {
      hlc_issued(hlc);
    }

    return true;
  }

  /**
   * The entry point for applying a change that carries an id of another node:
   * call it before the change is visible. The clock moves past the id, in time
   * and epoch, and the stored high water covers it: local ids and the read views
   * opened after it are above it, also after a restart.
   *
   * @param[in] id  The id on the row.
   */
  void observe_id(trx_id_t id) noexcept {
    if (ut::id_to_epoch(id) > m_clock.epoch()) {
      clock_set_epoch(ut::id_to_epoch(id));
    }

    const bool ok = clock_update(ut::id_to_timestamp(id));
    ut_a(ok);
  }

  /**
   * Sets the epoch of the clock, and stores it.
   *
   * @param[in] epoch  The new epoch.
   */
  void clock_set_epoch(uint64_t epoch) noexcept;

  /**
   * Checks the format and the node id in the transaction system page of the
   * system tablespace, before the recovery reads the files.
   *
   * @param[in] fh    The system tablespace.
   * @param[in] node  The node id of the configuration.
   *
   * @return DB_SUCCESS, or DB_ERROR and a message is logged.
   */
  [[nodiscard]] static DbErr check_format(os_file_t fh, uint32_t node) noexcept;

  /**
   * Writes a trx id to an index page. In case that the id size changes in
   * some future version, this function should be used instead of
   * mach_write_...
   *
   * @param[in]	ptr	Pointer to memory where written
   * @param[in]	id	transaction id to write
  */
  static void write_trx_id(byte *ptr, trx_id_t id) noexcept {
    static_assert(DATA_TRX_ID_LEN == 8, "error DATA_TRX_ID_LEN != 8");

    mach_write_to_8(ptr, id);
  }

  /**
   * Reads a trx id from an index page. In case that the id size changes in
   * some future version, this function should be used instead of
   * mach_read_...
   *
   * @param[in]	ptr	Pointer to memory from where to read
   *
   * @return	id
   * */
  [[nodiscard]] static trx_id_t read_trx_id(const byte *ptr) noexcept {
    static_assert(DATA_TRX_ID_LEN == 8, "error DATA_TRX_ID_LEN != 8");

    return mach_read_from_8(ptr);
  }

  /**
     * This function is used to find number of prepared transactions and
     * their transaction objects for a recovery. This function is used to
     * recover any X/Open XA distributed transactions.
     *
     * @param[in,out] xid_list Prepared transactions.
     * @param[in] len Number of slots in xid_list.
     *
     * @return Number of prepared transactions.
     */
  [[nodiscard]] int recover(Xid *xid_list, ulint len) noexcept;

  /**
   * Gets a pointer to the transaction system header and x-latches its page.
   *
   * @param[in]	mtr	mtr
   *
   * @return	pointer to system header, page x-latched.
   */
  [[nodiscard]] trx_sysf_t *read_header(Mtr *mtr) noexcept {
    ut_ad(mtr != nullptr);

    BufPool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {TRX_SYS_SPACE, TRX_SYS_PAGE_NO},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto block = m_sys.m_buf_pool->get(req, nullptr);
    buf_block_dbg_add_level();

    return TRX_SYS + block->get_frame();
  }

  /** Gets the pointer in the nth slot of the rseg array.
   *
   * @param[in] sys	Trx system
   * @param[in] n	Index of slot
   *
   * @return	pointer to rseg object, NULL if slot not in use
   */
  [[nodiscard]] TrxRseg *get_nth_rseg(ulint n) noexcept {
    ut_ad(n < m_rsegs.size());

    return m_rsegs[n];
  }

  /**
   * Sets the pointer in the nth slot of the rseg array.
   *
   * @param[in] sys	Trx system
   * @param[in] n	Index of slot
   * @param[in] rseg	Pointer to rseg object, NULL if slot not in use
   */
  void set_nth_rseg(ulint n, TrxRseg *rseg) noexcept {
    ut_ad(n < m_rsegs.size());

    m_rsegs[n] = rseg;
  }

  /**
   * @brief Inserts the trx handle in the trx system trx list in the right position.
   *
   * The list is sorted on the trx id so that the biggest id is at the list start.
   * This function is used at the database startup to insert incomplete transactions to the list.
   *
   * @param[in]  trx The transaction handle to insert.
   */
  void trx_list_insert_ordered(Trx *trx) noexcept;

  /**
   * Creates trx objects for transactions and initializes the trx list of this at database start.
   * Rollback segment and undo log lists must already exist when this function is called,
   * because the lists of transactions to be rolled back or cleaned up are built based on the undo log lists.
   *
   * @param[in] recovery The recovery flag.
   */
  void trx_lists_init_at_db_start(IbRecovery recovery) noexcept;

  /**
   * Assigns a rollback segment to a transaction in a round-robin fashion.
   * Skips the SYSTEM rollback segment if another is available.
   *
   * @return	assigned rollback segment id
   */
  [[nodiscard]] ulint trx_assign_rseg() noexcept;

  /**
   * Creates a background transaction instance.
   *
   * @param[in] arg	Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_background_trx(void *arg) noexcept;

  /**
   * Frees a background transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_background_trx(Trx *&trx) noexcept;

  /**
   * Creates a user transaction instance.
   *
   * @param[in] arg	Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_user_trx(void *arg) noexcept;

  /**
   * Frees a client transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_user_trx(Trx *&trx) noexcept;

  /**
   * Checks if a page address is the trx sys header page.
   *
   * @param[in] space	Tablespace ID
   * @param[in] page_no	Page number
   *
   * @return	true if trx sys header page
   */
  [[nodiscard]] static bool is_hdr_page(space_id_t space_id, page_no_t page_no) noexcept {
    return space_id == TRX_SYS_SPACE && page_no == TRX_SYS_PAGE_NO;
  }

  /** Gets the space of the nth rollback segment slot in the trx system file copy.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	mtr	mtr
   *
   * @return	space id
   */
  [[nodiscard]] static space_id_t frseg_get_space(trx_sysf_t *sys_header, ulint i, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    return mtr->read_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, MLOG_4BYTES);
  }

  /**
   * Gets the page number of the nth rollback segment slot in the trx system header.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	mtr	mtr
   *
   * @return	page number, FIL_NULL if slot unused
   */
  [[nodiscard]] static page_no_t frseg_get_page_no(trx_sysf_t *sys_header, ulint i, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    return mtr->read_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_PAGE_NO, MLOG_4BYTES);
  }

  /**
   * Sets the space id of the nth rollback segment slot in the trx system
   * file copy.
   *
   * @param[in]	sys_header	Trx sys file copy
   * @param[in]	i	Slot index == rseg id
   * @param[in]	space	Space id
   * @param[in]	mtr	mtr
   */
  static void frseg_set_space(trx_sysf_t *sys_header, ulint i, space_id_t space, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, space, MLOG_4BYTES, mtr);
  }

  /**
   * Sets the page number of the nth rollback segment slot in the trx system header.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	page_no	Page number, FIL_NULL if the slot is reset to unused
   * @param[in]	mtr	mtr
   */

  static void frseg_set_page_no(trx_sysf_t *sys_header, ulint i, page_no_t page_no, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_PAGE_NO, page_no, MLOG_4BYTES, mtr);
  }

#ifdef WITH_XOPEN
  /**
   * This function is used to find one X/Open XA distributed transaction
   * which is in the prepared state
   *
   * @param[in] xid X/Open XA transaction identification
   *
   * @return trx or nullptr
   */
  [[nodiscard]] Trx *get_trx_by_xid(Xid *xid) noexcept;
#endif /* WITH_XOPEN */

  /** Closes a read view.
   * @param view read view
   */
  void close_read_view(ReadView *view);

  /** Closes a consistent read view for client. This function is called at an SQL
   * statement end if the trx isolation level is <= TRX_ISO_READ_COMMITTED.
   * @param trx trx which has a read view
   */
  void close_read_view_for_read_committed(Trx *trx);

  /**
   * This function sets a given consistent cursor view to a transaction
   * read view if given consistent cursor view is not nullptr. Otherwise, function
   * restores a global read view to a transaction read view.
   *
   * @param trx in: transaction where cursor is set
   * @param cursor_view in: consistent cursor view to be set
   */
  void set_cursor_view(Trx *trx, CursorView *cursor_view);

  /** Create a consistent cursor view to be used in cursors. In this
   * consistent read view modifications done by the creating transaction or future
   * transactions are not visible.
   * @param cr_trx trx where cursor view is created
   * @return cursor view
   */
  CursorView *create_cursor_view(Trx *cr_trx);

  /** Close a given consistent cursor view and restore global read view
   * back to a transaction read view.
   * @param trx trx
   * @param curview cursor view to be closed
   */
  void close_cursor_view(Trx *trx, CursorView *curview);

  /**
   * The read view of purge: see PURGE above. Close it with close_read_view().
   *
   * @param heap memory heap from which allocated
   * @return own: read view struct
   */
  ReadView *open_purge_view(MemHeap *heap);

  /**
   * Opens a read view: see READ VIEWS above. A normal one sees its creator.
   *
   * @param cr_trx_id trx_id of creating transaction, or 0 used in purge
   * @param heap memory heap from which allocated
   * @return own: read view struct
   */
  ReadView *open_read_view_now(trx_id_t cr_trx_id, MemHeap *heap);

#ifdef UNIT_TEST
 private:
#endif /* UNIT_TEST */

  /**
   * Creates the file page for the transaction system. This function is called
   * only at the database creation, before init().
   *
   * @param[in,out] mtr              Mini-transaction covering the operation.
   */
  void create_new_instance(Mtr *mtr) noexcept;

  /**
   * Creates trx objects for transactions and initializes the trx list of this at database start.
   * Rollback segment and undo log lists must already exist when this function is called,
   * because the lists of transactions to be rolled back or cleaned up are built based on the undo log lists.
   *
   * @param[in] recovery The recovery flag.
   */
  void init_at_db_start(IbRecovery recovery) noexcept;

  struct IdShard;

  /** @return the shard of the active ids of an id. */
  [[nodiscard]] IdShard &id_shard(trx_id_t id) noexcept { return m_ids[(id * 0x9E3779B97F4A7C15ULL) >> 56]; }

  /** Enter a node in the commit table. */
  void ids_insert(trx_id_t id, TrxNode *node) noexcept;

  /** Trx is not active in the commit table any more (C2 of its commit). */
  void ids_erase(Trx *trx) noexcept;

  /**
   * Finds an active transaction by its id and pins it: it can't reach step
   * C4 of its commit (committed in memory) until unpin(), nor can the Trx
   * object be reused.
   *
   * @param[in] id A transaction id.
   *
   * @return the pinned transaction, or nullptr if no active one has the id.
   */
  [[nodiscard]] Trx *pin_active(trx_id_t id) noexcept;

  /** Unpin a transaction pinned by pin_active(). */
  static void unpin(Trx *trx) noexcept { trx->m_n_ref.fetch_sub(1, std::memory_order_release); }

  /**
   * @return true if a transaction with the id is in the active ids (not past
   * C2 of its commit), read with the id shard mutex. V3 of a conversion.
   */
  [[nodiscard]] bool in_ids(trx_id_t id) noexcept;

  /**
   * Waits until no thread pins trx (step C3 of its commit). The calling
   * thread holds no latch: a pinning thread may wait for a page latch or a
   * lock system latch while it holds the pin.
   *
   * @param[in] trx A transaction erased from the active ids.
   */
  void drain_pins(Trx *trx) noexcept;

  /** Open the snapshot of a view: the slot, W, S. */
  void open_snapshot(ReadView *view) noexcept;

  /** Set W and the floor exactly, from the list, down too: startup and tests. */
  void refresh_low_water() noexcept;

  /** Raise an atomic to a value. */
  static void raise(std::atomic<uint64_t> &a, uint64_t v) noexcept {
    auto cur = a.load(std::memory_order_relaxed);

    while (cur < v && !a.compare_exchange_weak(cur, v, std::memory_order_release, std::memory_order_relaxed)) {
    }
  }

  /**
   * Creates a transaction instance.
   *
   * @param[in] arg Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_trx(void *arg) noexcept;

  /**
   * Destroys a transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_trx(Trx *&trx) noexcept;

 public:
  /** Currently serves the purpose of the old kernel mutex.
   * TODO: Split it over a few patches.
   */
  Mutex m_mutex{};

  /** The clock of the transaction ids and numbers, and of the commits. */
  ut::TimeService m_clock{0, 1};

  /** The high water of the clock that is durable (TRX_SYS_HLC_STORE). */
  std::atomic<uint64_t> m_hlc_stored{};

  /** Serializes the stores of the high water. */
  ut::Latch m_hlc_store_latch{};

  /** The list's clock. */
  TrxClock m_list_clock{&m_clock};

  /** The transactions with an id: the commit table's nodes. For the walks that are
  not on the read path, and for retirement. */
  TrxList m_nodes{m_list_clock};

  /** The slots of the snapshots of the read views. */
  ut::SlotPool m_snaps{};

  /** W: every id below it has a commit stamp no greater than a word read before
  the publish of this. Written by the retirer, read by the views and the page max
  trx id filters. A lower bound of the active ids (or the high water mark, none). */
  alignas(64) std::atomic<trx_id_t> m_low_water{};

  /** F: a lower bound of the snapshot of any view opened from now on. */
  std::atomic<uint64_t> m_snap_floor{1};

  /** Tests: the steps of C6 of a commit (0 before PENDING, 1 after it, 2 after the
  tick, 3 after the stamp); of opening a view: 0 before its slot, 1 between its slot
  and its S. */
  void (*m_commit_hook)(void *, Trx *, int){};
  void *m_commit_hook_arg{};
  void (*m_view_hook)(void *, int){};
  void *m_view_hook_arg{};

  /** The recovered transactions: active, prepared, committed in memory, sorted on
  trx id, biggest first. With the trx sys mutex. */
  UT_LIST_BASE_NODE_T_EXTERN(Trx, m_trx_list) m_recv_trx_list {};

  /** The transactions created for users, sharded: created and destroyed per API
  begin. The lock monitor walks them. */
  struct alignas(64) ClientShard {
    ut::Latch m_latch{};
    UT_LIST_BASE_NODE_T_EXTERN(Trx, m_client_trx_list) m_trxs {};
  };

  std::array<ClientShard, 64> m_client_shards{};

  /** List of rollback segment objects */
  UT_LIST_BASE_NODE_T_EXTERN(TrxRseg, m_rseg_list) m_rseg_list {};

  /* The commit table, by id (READ VIEWS above). And the active transactions, those
  with a Trx in the entry, for the implicit locks (the lock system finds the owner
  of a record's trx id without the trx sys mutex; see the LATCHING comment of
  lock0lock.h). A node is in it from the assignment of its id (start, or recovery
  for an active or prepared one) to its retirement; its Trx to step C2 of its commit.
  A shard latch is below the trx sys mutex (SYNC_TRX_SYS_SHARD) and above the trx
  mutex. */

  /** Number of shards of the active ids. */
  static constexpr size_t N_ID_SHARDS = 256;

  /** The ids of a shard: an open addressing hash table (id 0 marks a free slot: no
  transaction has id 0), linear probing, backward shift deletion; no allocation but
  to grow. The hash mixes the time bits of an id: the low ones are the node, constant
  on one node, the top ones the epoch, nearly constant. */
  template <typename V>
  class IdMapT {
   public:
    [[nodiscard]] V *find(trx_id_t id) const noexcept {
      if (m_n == 0) {
        return nullptr;
      }

      for (auto i = slot(id);; i = (i + 1) & mask()) {
        if (m_slots[i].first == id) {
          return m_slots[i].second;
        } else if (m_slots[i].first == 0) {
          return nullptr;
        }
      }
    }

    [[nodiscard]] bool contains(trx_id_t id) const noexcept { return find(id) != nullptr; }

    /** Map id to trx (insert, or replace). */
    void insert(trx_id_t id, V *trx) noexcept {
      ut_ad(id != 0);

      if ((m_n + 1) * 2 > m_slots.size()) {
        grow();
      }

      for (auto i = slot(id);; i = (i + 1) & mask()) {
        if (m_slots[i].first == id) {
          m_slots[i].second = trx;
          return;
        } else if (m_slots[i].first == 0) {
          m_slots[i] = {id, trx};
          ++m_n;
          return;
        }
      }
    }

    /** Erase id, if it maps to trx. */
    void erase(trx_id_t id, const V *trx) noexcept {
      if (m_n == 0) {
        return;
      }

      auto i = slot(id);

      for (;; i = (i + 1) & mask()) {
        if (m_slots[i].first == 0) {
          return;
        } else if (m_slots[i].first == id) {
          break;
        }
      }

      if (m_slots[i].second != trx) {
        return;
      }

      /* Shift back the entries after it that may take its slot: an entry
      at j whose home k is not in (i, j], cyclically. */
      for (auto j = i;;) {
        j = (j + 1) & mask();

        if (m_slots[j].first == 0) {
          break;
        }

        const auto k = slot(m_slots[j].first);
        const bool stays = i <= j ? (i < k && k <= j) : (i < k || k <= j);

        if (!stays) {
          m_slots[i] = m_slots[j];
          i = j;
        }
      }

      m_slots[i] = {0, nullptr};
      --m_n;
    }

   private:
    [[nodiscard]] size_t mask() const noexcept { return m_slots.size() - 1; }

    [[nodiscard]] size_t slot(trx_id_t id) const noexcept { return size_t((id * 0xD6E8FEB86659FD93ULL) >> m_shift); }

    void grow() noexcept {
      std::vector<std::pair<trx_id_t, V *>> old;

      old.swap(m_slots);

      const size_t n = old.empty() ? 16 : old.size() * 2;

      m_slots.assign(n, {0, nullptr});
      m_shift = 64 - uint32_t(std::countr_zero(n));
      m_n = 0;

      for (const auto &[id, trx] : old) {
        if (id != 0) {
          insert(id, trx);
        }
      }
    }

    std::vector<std::pair<trx_id_t, V *>> m_slots;
    uint32_t m_shift{64};
    size_t m_n{};
  };

  using IdMap = IdMapT<Trx>;

  /** A shard of the commit table. */
  struct alignas(64) IdShard {
    ut::Latch m_mutex;
    IdMapT<TrxNode> m_trxs;
  };

  /** The shards. */
  std::array<IdShard, N_ID_SHARDS> m_ids{};

  /** The memory of destroyed Trx objects, reused by create_trx() and freed
  with the TrxSys: a Trx is type stable while the System lives, the deadlock
  walk can read the edge of a transaction that committed under it (see
  lock0deadlock.h). */
  std::vector<std::pair<void *, CondVar *>> m_trx_pool{};

  /** Protects m_trx_pool. */
  ut::Latch m_trx_pool_mutex{};

  /** Latest rollback segment in the round-robin assignment
  of rollback segments to transactions */
  std::atomic<TrxRseg *> m_latest_rseg{};

  /** Pointer array to rollback segments; NULL if slot not in use */
  std::array<TrxRseg *, TRX_SYS_N_RSEGS> m_rsegs{};

  /** Length of the TRX_RSEG_HISTORY list (update undo logs for
  committed transactions) */
  std::atomic<ulint> m_rseg_history_len{};

  /** The following is true when we are using the database in the file per table
   * format, we have successfully upgraded, or have created a new database installation */
  bool m_multiple_tablespace_format{};

  /** Number of transactions currently allocated for the client */
  std::atomic<ulint> m_n_user_trx{};

  /** Number of background transactions currently allocated */
  std::atomic<ulint> m_n_background_trx{};


  /** Purge system. */
  PurgeSys *m_purge{};

  /** The recovered transaction being rolled back in crash recovery, for
  reporting the rollback progress. */
  Trx *m_recv_rollback_trx{};

  /** Undo number of m_recv_rollback_trx when its rollback started. */
  int64_t m_recv_rollback_max_undo_no{};

  /** Last rollback progress percentage reported. */
  ulint m_recv_rollback_printed_pct{};
};

