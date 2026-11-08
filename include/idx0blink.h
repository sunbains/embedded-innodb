/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file include/idx0blink.h
A Lehman & Yao B-link tree on disk, behind IndexAm. The pages, records and redo
are those of the B-tree (btr0btr.h, btr0cur.h); the record level code is shared.

The protocol:
 - A page holds keys < its high key, the first key of its right sibling at the
   time of the split, which never changes after. A rightmost page has none.
 - No index latch. A search holds no latch between pages and moves right when
   the key is past the high key: a stale parent or path is repaired by that.
 - A split happens with the leaf X latched, without a new search, and goes up
   from there. The latch order is bottom to top, then left to right. Nothing waits
   for a page on a lower level or to the left of one it holds.
 - The root keeps its page number: when it's full its records move to a new page,
   and it becomes the parent of that. Levels change only there, on the root.

A split is level by level, each level one mini-transaction:
 - On a level: the page, the new page and the right sibling. The new page is linked in, the record goes
   in, and the left page says PAGE_RIGHT_UNLINKED: its right sibling has no node pointer. That's a tree
   for everyone, a search gets to the new page through the left page as L&Y's do, and holds nothing
   through a commit. At most what the level above needs is held at a time, never the levels below that.
 - Then one that holds the left page and goes up for the parent, inserts the node pointer and clears
   the flag, in one. If that splits the parent, the parent is left in the same state, and so on up.
What the flag says is complete: the key is the page's high key, the child its right link. So anyone who
holds the page can do it, whoever split it, or after a crash: a writer that finds it does it after its own
change, and a crash has nothing to repair at restart. A page that's never written again costs a move
right. The flag goes with the bound: when a flagged page splits, the new page takes the flag for the
sibling after it, the left page has one for the new page. Nothing that takes a page off the tree starts
with a flag on it, see below: so a flag can't name a page that's gone, and no pointer goes in twice.

Why waiting for the tablespace latch with tree pages owned can't deadlock:

 Allocation is in a mini-transaction of its own, committed at once, so the latch isn't
 held while waiting for a tree page by that path. The other holders of it hold it to do
 something on file space, and the order is always tree page, then tablespace latch:
   - Btree::page_alloc() and Btree::page_free_low(), the only tree code under it, read the
     segment header from the root by a buffer fix, no latch: Btree::seg_root().
   - BLOB code (store, free) takes it holding the leaf X in the caller's mini-transaction or
     nothing. Under the latch it latches only BLOB pages, new pages and the record's page,
     which the same thread has (a recursion in btr0blob.cc, for its redesign to remove).
   - remove() and update() run the BLOB free in a short mini-transaction, not in the split's.
   - Btree::free_root() latches the root under it, but only after drop_data(): the index
     isn't in use. Undo pages and the file space pages under it are no tree pages.
   - A compensation, SysTrxSys::compensate(), latches the root under it: it would be the
     exception, but a B-link operation never calls SysTrx::rollback(), everything that can
     fail is checked before the first allocation (the reservation, the key size). It only
     runs at restart, single threaded.
 So nobody who holds the tablespace latch waits for a tree page: the wait graph has no edge
 from it to a tree latch, and a cycle through it can't exist. New pages are private until the
 mini-transaction of the split commits, no thread can be holding those.

How pages leave the tree, by a merge:

 A page with little on it (Merge::is_small()) takes in its right sibling when both fit and neither
 has PAGE_RIGHT_UNLINKED: a sibling that has no node pointer can't be found from the parent. In one
 mini-transaction that holds the two pages and the next one, left to right, then the parent: the
 latch order of a split. The records and the locks move, the left page gets the right page's links
 and high key, the parent loses its pointer to the right page, and that page is marked dead, by a
 PAGE_INDEX_ID that's no index's. A crash has all of that or none of it. It's best effort: when
 things aren't as they were when it looked, it leaves them.

 A page has records moved left, which no walk right can find. It doesn't have to, because of that
 one mini-transaction: when a page is dead there is no path to it on the tree, so whoever got there
 by a stale number goes back to the root (NodeStep::RESTART) and doesn't come back. A sibling's link
 always leads to a page on the tree: it's followed with the latch of the page that has the link, and
 the merge has to change that page. Records are found again by key: cursors that saved a position
 compare the modify clock of the page, which the merge bumps.

 A root with one child takes in the child's records and goes down a level, the reverse of a raise.

Freeing a dead page: it has to stay until nobody can hold its number. Those who can:
   - A descent, which holds the next page number with no latch. It's in a Drain::Guard until it
     has latched where it was going. A dead page goes when all the guards that were in when it
     died have left. Nothing waits for that: a thread frees what it can, and leaves the rest for
     the next one. A wait could deadlock, it can hold a latch a guard waits for.
   - A Path, which outlives a guard: a split can use it long after. It remembers Drain::unlinks(),
     changed before a tree changes: when it's different the path is thrown away.
   - move_left(), in a guard from the left link to its latch.
 In the mini-transaction of the merge a system transaction starts, with FREE_PAGE as its
 compensation, ended by the mini-transaction that frees it: a crash at any point has the page back,
 and a checkpoint carries it forward however long it waits. The free reads the segment header on
 a root with no latch: Btree::seg_root().

Who holds what, no RW latch anywhere on a page of this tree:
 - A page is owned by its bits, BufBlock::bits_lock(), to change it or to read a record on it and have
   it stay. Ownership has the same order as the RW latches had: everything said about latch order here
   is about this. A cursor that holds a page holds it until its mini-transaction commits.
 - A delete mark or a change of a record with no change of size, Intent::Mark, owns only the bit of the
   group of the record: BufBlock::m_bits. It's found with no ownership, and it never takes more, so it
   can't wait for a bit with one held. Their redo records are by offset and touch nothing else, so the
   order of them on the log doesn't matter between groups. Everything else has all the bits, until the
   commit, which puts its redo in the order of its changes to the page. A read of a record doesn't wait
   for a holder of a bit, only for the moment of the writes: BufBlock::RecWrite.
 - A reader that places no lock, Intent::Snapshot, holds nothing on a leaf either: a copy of the record
   that validates against the version. See BlinkCursor.
 - An insert with nothing unusual about it is queued on its page, BlinkCursor::insert_batched(), and
   whoever owns the page next does all that's queued in one mini-transaction. So a hot page costs one
   trip to the log per batch, and redo has one order. The owner decides nothing for anyone: a duplicate,
   a full page, a key that belongs further right go back to their thread, for the normal way. A lock wait
   is the transaction's, its thread acts on it as ever. A thread only sleeps with the page owned, the
   release wakes it.
 - A descent reads non-leaf pages with no ownership at all, so that readers don't write to the cache
   lines of the pages everyone goes through, and can't be part of a wait. See read_node(). Reading a
   page goes again if an owner got in the way, holding nothing. Merge picks what to try the same way.
 - No index latch: nobody takes Index::m_lock on this tree.
 - The flusher marks a page as being written, then waits for the bits to be free. */

#pragma once

#include "innodb0types.h"

#include "idx0am.h"
#include "rem0types.h"

struct DTuple;
struct Index;

namespace blink {

/** How read_node() chooses, and what seek_edge() and the sampling want. */
enum class Edge : uint8_t {
  /** By the key. */
  KEY,

  /** The first record: the leftmost child. */
  FIRST,

  /** The last record: the rightmost child, after going to the rightmost page. */
  LAST,

  /** A random child. */
  RANDOM
};

/** What the page tells the search to do next. */
struct NodeStep {
  enum Kind : uint8_t {
    /** m_page is the right sibling: the key belongs there or further. */
    RIGHT,

    /** m_page is the child. */
    CHILD,

    /** The page is at the level to stop at, nothing was read from it. */
    STOP,

    /** The page isn't on the tree any more: start from the root. */
    RESTART
  };

  Kind m_kind{};

  /** The next page. For STOP the page itself. */
  page_no_t m_page{};

  /** The level of the page that was read. */
  ulint m_level{};
};

/**
 * Read one page during a descent: the only code in a descent that reads a non-leaf page. It takes
 * no latch: buffer fix the page, read the block's version, read_node_raw(), validate the version,
 * go again if a writer got in the way. Nothing is held on return, and nothing while it waits. It's
 * safe to find a page number on an unlatched page because the descent is in a Drain::Guard: a merge
 * frees nothing a guard could have seen.
 *
 * @param[in,out] sys           The instance.
 * @param[in] index             The index.
 * @param[in] page_no           The page.
 * @param[in] key               The key, nullptr if edge isn't Edge::KEY.
 * @param[in] mode              The mode of the search on the leaf.
 * @param[in] edge              How to choose.
 * @param[in] stop_level        STOP on a page at this level or below.
 *
 * @return what to do next.
 */
[[nodiscard]] NodeStep read_node(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level
) noexcept;

#if defined(UNIV_DEBUG) || defined(UNIT_TESTING)
/** read_node() with the S latch and PageCursor: what it's checked against, and nothing else. */
[[nodiscard]] NodeStep read_node_latched(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level
) noexcept;
#endif /* UNIV_DEBUG || UNIT_TESTING */

/**
 * Search a page that nothing holds still, for read_node(). It never reads outside the page and
 * never asserts, whatever is on it, so the result means something only if the version of the block
 * validates after. The search is its own and not PageCursor's for that reason.
 *
 * @param[in] index             The index.
 * @param[in] page              The page, its bytes can be anything.
 * @param[in] page_no           The page's number.
 * @param[in] key               The key.
 * @param[in] mode              The mode of the search on the leaf.
 * @param[in] edge              How to choose.
 * @param[in] stop_level        STOP on a page at this level or below.
 * @param[out] step             What to do next.
 *
 * @return false if the page can't be what it should be: try again.
 */
[[nodiscard]] bool read_node_raw(
  const Index *index, const page_t *page, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level, NodeStep &step
) noexcept;

/** What a parent says about the children of a page, for a merge. */
struct RawFamily {
  enum Kind : uint8_t {
    /** Not a page of this tree any more. */
    DEAD,

    /** Not at the level asked for. */
    OTHER_LEVEL,

    /** The key is further right: see m_next. */
    RIGHT,

    /** The child for the key, and its neighbours under this page. */
    FOUND
  };

  Kind m_kind{};
  page_no_t m_next{FIL_NULL};
  page_no_t m_left{FIL_NULL};
  page_no_t m_page{FIL_NULL};
  page_no_t m_right{FIL_NULL};
  ulint m_n_children{};
};

/** As read_node_raw(), for the child of key on a page at level, and what's on either side of it.
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_family_raw(const Index *index, const page_t *page, const DTuple *key, ulint level, RawFamily &fam) noexcept;

/** As read_node_raw(), for IndexAm::read_scan_node().
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_scan_node_raw(
  const Index *index, const page_t *page, const DTuple *start, const DTuple *end, ulint stop_level, size_t max, IndexAm::ScanNode &node
) noexcept;

/** Where a search of a leaf ends. */
struct RawLeaf {
  enum Kind : uint8_t {
    /** Not a page of this tree any more. */
    DEAD,

    /** Not a leaf: a root that went up since. */
    NOT_LEAF,

    /** The key is further right: see m_next. */
    RIGHT,

    /** See m_offs. */
    FOUND
  };

  Kind m_kind{};
  page_no_t m_next{FIL_NULL};

  /** The place: a record, the infimum or the supremum. As a search on the page in that mode. */
  ulint m_offs{};
};

/** As read_node_raw(), the search of a leaf that nothing holds still.
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_leaf_raw(const Index *index, const page_t *page, const DTuple *key, ulint mode, Edge edge, RawLeaf &leaf) noexcept;

/** As read_node_raw(), the bit of the group of a record: BufBlock::group_bit().
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_group_raw(const page_t *page, ulint offs, ulint &bit) noexcept;

/** As read_node_raw(), what's before a place on a page.
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_prev_raw(const page_t *page, ulint offs, ulint &prev) noexcept;

/** As read_node_raw(), copy the start of a page and a record on it, to the same offsets in to.
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_rec_copy_raw(const page_t *page, ulint offs, byte *to) noexcept;

/** As read_node_raw(), for what page_get_rec_data_size() tells.
@param[out] size                ULINT_UNDEFINED if it's not a page of the tree at level.
@return false if the page can't be what it should be. */
[[nodiscard]] bool read_rec_size_raw(const Index *index, const page_t *page, ulint level, ulint &size) noexcept;

/**
 * @return the largest size of a high key, and with it the key of a record, for a page to always
 * take two node pointers and its high key. A record's key isn't truncated: a record has to
 * compare the same as the key of its node pointer, and the page number is its last field.
 */
[[nodiscard]] ulint max_key_size() noexcept;

/** The points a test can stop at, in a split. */
enum class TestPoint : uint8_t {
  /** A page was allocated, it isn't linked. */
  ALLOCATED,

  /** A level is split, nothing committed. */
  SPLIT,

  /** A level is split and committed, its node pointer isn't in, nothing is held. */
  UNLINKED,

  /** A merge has committed, its dead page isn't free yet. */
  MERGED,

  /** A read that holds nothing is about to copy a record, it has validated nothing yet. */
  SNAP_COPY,

  /** A batched insert found its page, it isn't queued on it yet. */
  BATCH_FOUND,
};

#ifdef UNIT_TESTING
/** Called at the points. */
using TestHook = void (*)(System &sys, TestPoint point);

/** Set the hook of an instance that uses this index, nullptr for none. */
void set_test_hook(System &sys, TestHook hook) noexcept;

/** Turn the merges on or off, to test what happens when a merge isn't possible. */
void set_merge(System &sys, bool on) noexcept;

/** Keep dead pages from being freed, as a guard that stays would. */
void hold_frees(System &sys, bool hold) noexcept;

/** @return how many inserts were done by a thread for another one. */
[[nodiscard]] uint64_t n_batched(System &sys) noexcept;

/** @return how many times a read of a non-leaf page went again, because of a writer. */
[[nodiscard]] uint64_t n_node_retries(System &sys) noexcept;

/** Allocate a leaf page in a system transaction and commit that step, with no link to it.
A crash after this has the page back in the segment.
@return the page, its system transaction stays open: the caller has to crash. */
page_no_t alloc_unlinked(System &sys, const Index *index) noexcept;
#endif /* UNIT_TESTING */

}  // namespace blink
