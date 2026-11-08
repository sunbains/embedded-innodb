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

/** @file include/idx0am.h
The index access method: what the engine needs from an index, whatever its
structure is. A B-tree and an LSM tree both have to be able to implement this,
so nothing here speaks of pages, tree levels, latches or where a record is kept.

The contract:
 - A record is in the physical record format of rem0rec.h.
 - A record from Cursor::rec() is valid until the next call on that cursor or
   the commit of its mini-transaction, which is also the redo context.
 - A change that can't finish with what the cursor holds returns DB_FAIL, and the
   caller repositions with Intent::Restructure. An implementation that doesn't
   need that, such as a B-link tree or an LSM tree, never returns it.
 - A position is a record or a point between records that an implementation has
   for gap locks to go on. seek() and next() only stop on records, position() and
   next_pos() also stop on the others: on_rec() is false there. An implementation
   without such points has nothing else to stop on, both behave the same. */

#pragma once

#include "innodb0types.h"

#include "api0types.h"
#include "db0err.h"
#include "lock0types.h"
#include "rem0rec.h"
#include "trx0types.h"

#include <memory>
#include <vector>

class System;

struct BigRec;
struct BtreePcursor;
struct BufBlock;
struct DTuple;
struct Index;
struct Mtr;
struct QueThr;
struct Trx;
struct Upd;

/** What the caller is going to do at the position. */
enum class Intent : uint8_t {
  /** Read the record, and have it stay: to place a lock on it. */
  Read,

  /** Read the record as it was at one time, holding nothing: what the cursor gives is a copy of it,
  which is all that is the same. An implementation can treat it as Read. */
  Snapshot,

  /** Delete mark the record or update it in place with no change of size, nothing else, on a clustered
  index. Nothing but those bytes of that record changes, so an implementation can hold less for it than
  for a Write, and that is all a cursor in this intent can be used for: no moving it, and no other kind
  of change. Not for a secondary index: its lock check writes PAGE_MAX_TRX_ID to the header. */
  Mark,

  /** Change the record, nothing else. */
  Write,

  /** Change the record, and the structure of the index if that's what it takes. */
  Restructure
};

/** Where a saved position is, relative to the key that was saved with it. */
enum class SavedAt : uint8_t {
  /** Nothing is saved. */
  None,

  /** On the record with that key. */
  On,

  /** Just before it, or before the first record of an empty index. */
  Before,

  /** Just after it, or after the last record of an empty index. */
  After
};

/** An index implementation. The engine has one of each kind. */
struct IndexAm {
  /** Where an index is, for the catalog to store. Opaque outside the implementation. */
  using Root = page_no_t;

  /** An iterator on an index. */
  struct Cursor {
    virtual ~Cursor() = default;

    /** Position on the first record that satisfies mode, or at the end if there is none. */
    virtual void seek(Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** Position where mode says, without going on to a record if the search ends between
    records: see the contract. To be used when what's between records matters to the caller. */
    virtual void position(const Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** Position on the last record <= entry, for insert() to put entry after it.
    low_match() and up_match() tell how much it shares with its neighbours.
    @param[in] intent           Write or Restructure.
    @param[in] thr              Query thread. */
    virtual void seek_insert(const Index *index, const DTuple *entry, Intent intent, QueThr *thr, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** Position on the record whose key is key, the fields key compares on.
    @return true if there is one. */
    [[nodiscard]] virtual bool find(const Index *index, const DTuple *key, Intent intent, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** Position before the first record, or after the last: then next() or prev() gets to it. */
    virtual void seek_edge(const Index *index, bool first, Intent intent, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** @return false if there is no next record: the cursor is at the end. */
    [[nodiscard]] virtual bool next(Mtr *mtr) noexcept = 0;

    /** @return false if there is no previous record: the cursor is at the start. */
    [[nodiscard]] virtual bool prev(Mtr *mtr) noexcept = 0;

    /** Like next(), but also stops on the positions between records: see the contract.
    @return false if there is no next position: the cursor is at the end. */
    [[nodiscard]] virtual bool next_pos(Mtr *mtr) noexcept = 0;

    /** Like prev(), but also stops on the positions between records: see the contract.
    @return false if there is no previous position: the cursor is at the start. */
    [[nodiscard]] virtual bool prev_pos(Mtr *mtr) noexcept = 0;

    /** Corruption: skip what the cursor can still reach in the damaged part of the index,
    the next next_pos() leaves it. Forced recovery only. */
    virtual void skip_rest(Mtr *mtr) noexcept = 0;

    /** @return true if positioned on a record, not before the first or after the last. */
    [[nodiscard]] virtual bool on_rec() const noexcept = 0;

    /** @return the record the cursor is on. */
    [[nodiscard]] virtual Rec rec() noexcept = 0;

    /** @return the index the cursor was last positioned on. */
    [[nodiscard]] virtual const Index *index() const noexcept = 0;

    /** @return the fields of the search key the position and what's before it match,
    after a search that ends at or before the key. */
    [[nodiscard]] virtual ulint low_match() noexcept = 0;

    /** @return the fields of the search key the position and what's after it match,
    after a search that ends at or after the key. */
    [[nodiscard]] virtual ulint up_match() noexcept = 0;

    /** @return the mode of the last search. */
    [[nodiscard]] virtual SearchMode search_mode() const noexcept = 0;

    /** ESCAPE HATCH, it isn't part of the abstraction: LockSys identifies a record by its
    block and record. Goes away when the lock API takes an opaque lock key.
    @return the block of the record. */
    [[nodiscard]] virtual BufBlock *block() noexcept = 0;

    /** @return what a lock at rec is on: rec is the record of the cursor, or one next to it that it
    read, or a position between records (on_rec() false), which is a fence. Valid while the cursor holds
    its position: a lock call with it and the change it protects are atomic with what the index holds. */
    [[nodiscard]] virtual LockPos lock_pos(Rec rec) noexcept = 0;

    /** Remember the position, to come back to it in another mini-transaction. */
    virtual void save(Mtr *mtr) noexcept = 0;

    /** Go back to the saved position.
    @return true if the record is still there. */
    [[nodiscard]] virtual bool restore(Intent intent, Mtr *mtr, SourceLocation loc) noexcept = 0;

    /** @return where the saved position is. */
    [[nodiscard]] virtual SavedAt saved_at() const noexcept = 0;

    /** @return true if there is a saved position to restore(). */
    [[nodiscard]] virtual bool has_saved() const noexcept = 0;

    /** Take over the saved position of src, a cursor of the same implementation. */
    virtual void copy_saved(Cursor &src) noexcept = 0;

    /** Commit mtr. The record is no longer available until restore(). */
    virtual void commit(Mtr *mtr) noexcept = 0;

    /** Remove the record for good.
    @param[in] intent           Write or Restructure, what the cursor was positioned with.
    @param[in] rb_ctx           The rollback that does it, if any.
    @param[in,out] mtr          Mini-transaction.
    @return DB_SUCCESS, DB_FAIL if it takes Restructure, DB_OUT_OF_FILE_SPACE. */
    [[nodiscard]] virtual DbErr remove(Intent intent, TrxRbCtx rb_ctx, Mtr *mtr) noexcept = 0;

    /** Update the record in place, as far as the implementation is concerned.
    @param[in] intent           Write or Restructure, what the cursor was positioned with.
    @param[in] flags            What to skip: BTR_NO_LOCKING_FLAG, ...
    @param[in,out] heap         For Restructure: where the fields that don't fit in the record go,
                                it's created if it's nullptr.
    @param[in,out] big_rec      For Restructure: the fields to store externally.
    @param[in] update           What to change.
    @param[in] cmpl_info        Compiler info on secondary index updates.
    @param[in,out] thr          Query thread.
    @param[in,out] mtr          Mini-transaction.
    @return DB_SUCCESS, DB_FAIL if it takes Restructure, an error. */
    [[nodiscard]] virtual DbErr update(
      Intent intent, ulint flags, MemHeap **heap, BigRec *big_rec, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr
    ) noexcept = 0;

    /** Update the record where the caller knows that no field changes size: it stays where
    it is and nothing has to be restructured. Other than update() this doesn't check.
    @param[in] flags            What to skip: BTR_NO_LOCKING_FLAG, ...
    @param[in] update           What to change.
    @param[in] cmpl_info        Compiler info on secondary index updates.
    @param[in,out] thr          Query thread.
    @param[in,out] mtr          Mini-transaction.
    @return DB_SUCCESS or an error. */
    [[nodiscard]] virtual DbErr update_in_place(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr) noexcept = 0;

    /** Insert entry, at the position of seek_insert().
    @param[in] intent           Write or Restructure, what the cursor was positioned with.
    @param[in] flags            What to skip: BTR_NO_LOCKING_FLAG, ...
    @param[in,out] entry        The record. On success fields that have to be stored
                                externally have been cut from it.
    @param[out] big_rec         The fields to store externally, see store_big_rec().
    @param[in] n_ext            Number of externally stored columns in entry.
    @param[in,out] thr          Query thread.
    @param[in,out] mtr          Mini-transaction.
    @return DB_SUCCESS, DB_FAIL if it takes Restructure, an error. */
    [[nodiscard]] virtual DbErr insert(
      Intent intent, ulint flags, DTuple *entry, BigRec *big_rec, ulint n_ext, QueThr *thr, Mtr *mtr
    ) noexcept = 0;

    /** Store the fields insert() or update() left in big_rec, for the record the cursor is on.
    @param[in] offsets          The offsets of the record.
    @param[in] big_rec          The fields.
    @param[in,out] mtr          Mini-transaction.
    @return DB_SUCCESS or an error. */
    [[nodiscard]] virtual DbErr store_big_rec(const RecOffs &offsets, const BigRec &big_rec, Mtr *mtr) noexcept = 0;

    /** Set or clear the delete mark of the record. */
    [[nodiscard]] virtual DbErr set_delete_mark(ulint flags, bool val, QueThr *thr, Mtr *mtr) noexcept = 0;

    /** Free what the cursor holds on to. */
    /**
     * Insert a new record if there is nothing unusual about it, in one call: what a page can do
     * right away. The work may be done by another thread, that owns the page, in a batch with others.
     * The entry and thr are used by that thread while this one waits.
     *
     * @return DB_FAIL if nothing was done: go the normal way, seek_insert() and on. Anything else is
     * the result of the insert, including that it has to wait for a lock.
     */
    [[nodiscard]] virtual DbErr insert_batched(const Index *, DTuple *, ulint, QueThr *) noexcept { return DB_FAIL; }

    /** The record that was read is in memory of the cursor's own, nothing more is wanted from the page
    in this mini-transaction: give it up.
    @return true if nothing is held for the cursor on any page now. */
    [[nodiscard]] virtual bool give_up_page(Mtr *) noexcept { return false; }

    virtual void close() noexcept = 0;
  };

  virtual ~IndexAm() = default;

  /** @return a new cursor. */
  [[nodiscard]] virtual std::unique_ptr<Cursor> cursor() = 0;

  /** Create an empty index. The caller holds the latch of the tablespace.
  @return where it is, FIL_NULL if there's no space left. */
  [[nodiscard]] virtual Root create(ulint type, space_id_t space, uint64_t index_id, Index *index, Mtr *mtr) noexcept = 0;

  /** Free the contents of an index, in as many mini-transactions as it takes.
  The index still exists until drop_root(), for the catalog to change atomically with that. */
  virtual void drop_data(space_id_t space, Root root) noexcept = 0;

  /** Free what is left of an index. The caller holds the latch of the tablespace. */
  virtual void drop_root(space_id_t space, Root root, Mtr *mtr) noexcept = 0;

  /** @return the pages in use, for the statistics. */
  [[nodiscard]] virtual ulint n_pages(const Index *index) noexcept = 0;

  /** @return the pages that hold records, for the statistics. */
  [[nodiscard]] virtual ulint n_data_pages(const Index *index) noexcept = 0;

  /** Update the statistics of the number of different key values of index. */
  virtual void estimate_distinct(const Index *index) noexcept = 0;

  /** Print the index for debugging. */
  virtual void print(Index *index) noexcept = 0;

  /** Do what can be done of the work put off, the server is going idle: what's left is for the
  next start, from the log. */
  virtual void quiesce() noexcept {}

  /** @return true if a change to the tree counts on Index::m_lock. If not nobody takes it in X
  mode, and there's nothing to take it in S mode for. */
  [[nodiscard]] virtual bool uses_index_latch() const noexcept { return true; }

  /** @return what to take on a page to read a record on it and have it stay. */
  [[nodiscard]] virtual ulint page_read_latch() const noexcept { return RW_S_LATCH; }

  /** @return what a nested mini-transaction takes on a page its caller holds, to log a change. */
  [[nodiscard]] virtual ulint page_held_latch() const noexcept { return RW_X_LATCH; }

  /** @return true if the structure of the index is what it should be. Nothing may be changing it. */
  [[nodiscard]] virtual bool validate(Index *index, Trx *trx) noexcept = 0;

  /**
   * Back on the position of the cursor the parallel reader keeps at a level of the tree, what
   * BtreePcursor::restore_position() is to a B-tree.
   *
   * It exposes a level of a tree, unlike the rest of this: it's what the parallel reader is
   * written on, until it goes through a Cursor and an implementation can split a scan.
   */
  [[nodiscard]] virtual bool restore_tree_cursor(BtreePcursor &pcur, ulint latch, Mtr *mtr, SourceLocation loc) noexcept = 0;

  /** The other side of restore_tree_cursor(). */
  virtual void save_tree_cursor(BtreePcursor &pcur, Mtr *mtr) noexcept = 0;

  /** What a page says to a scan that is being split. */
  struct ScanNode {
    /** It isn't a page of this index any more. */
    bool m_dead{};

    /** Its level. */
    ulint m_level{};

    /** The page numbers below it, in order. */
    std::vector<page_no_t> m_children;
  };

  /**
   * Read the children of a page for a scan: from the one that can have the first key >= start, up to
   * the first one with a key >= end. Holds nothing on return. As with restore_tree_cursor(), it's a
   * level of a tree, for the parallel reader.
   *
   * @param[in] index           The index.
   * @param[in] page_no         The page.
   * @param[in] start           Null for the first child.
   * @param[in] end             Null for no limit.
   * @param[in] stop_level      A page at this level or below: no children.
   * @param[in] max             The most children wanted.
   * @param[out] node           What it found.
   */
  virtual void read_scan_node(
    const Index *index, page_no_t page_no, const DTuple *start, const DTuple *end, ulint stop_level, size_t max, ScanNode &node
  ) noexcept = 0;

  /** While it lives a page number read from the tree stays a page of the tree. */
  struct PagePin {
    virtual ~PagePin() = default;
  };

  /** For the same user as restore_tree_cursor().
  @return null if the implementation doesn't need one. */
  [[nodiscard]] virtual std::unique_ptr<PagePin> pin_pages() noexcept { return nullptr; }
};

/** A cursor that gets its implementation from the index it's opened on. */
class IndexCursor {
 public:
  /** For a cursor that lives in a structure created where there's no instance: attach() it. */
  IndexCursor() noexcept = default;

  explicit IndexCursor(System &sys) noexcept : m_sys(&sys) {}

  void attach(System &sys) noexcept { m_sys = &sys; }

  [[nodiscard]] bool find(const Index *index, const DTuple *key, Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  void seek(Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  void position(const Index *index, const DTuple *key, SearchMode mode, Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  void seek_insert(const Index *index, const DTuple *entry, Intent intent, QueThr *thr, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  void seek_first(const Index *index, Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  void seek_last(const Index *index, Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept;

  [[nodiscard]] bool next(Mtr *mtr) noexcept { return m_cur->next(mtr); }

  [[nodiscard]] bool prev(Mtr *mtr) noexcept { return m_cur->prev(mtr); }

  [[nodiscard]] bool next_pos(Mtr *mtr) noexcept { return m_cur->next_pos(mtr); }

  [[nodiscard]] bool prev_pos(Mtr *mtr) noexcept { return m_cur->prev_pos(mtr); }

  void skip_rest(Mtr *mtr) noexcept { m_cur->skip_rest(mtr); }

  [[nodiscard]] bool on_rec() const noexcept { return m_cur->on_rec(); }

  [[nodiscard]] Rec rec() noexcept { return m_cur->rec(); }

  [[nodiscard]] const Index *index() const noexcept { return m_cur->index(); }

  [[nodiscard]] ulint low_match() noexcept { return m_cur->low_match(); }

  [[nodiscard]] ulint up_match() noexcept { return m_cur->up_match(); }

  [[nodiscard]] SearchMode search_mode() const noexcept { return m_cur->search_mode(); }

  /** ESCAPE HATCH for calls into the lock API only: see IndexAm::Cursor::block(). */
  [[nodiscard]] BufBlock *block() noexcept { return m_cur->block(); }

  [[nodiscard]] LockPos lock_pos(Rec rec) noexcept { return m_cur->lock_pos(rec); }

  void save(Mtr *mtr) noexcept { m_cur->save(mtr); }

  [[nodiscard]] bool restore(Intent intent, Mtr *mtr, SourceLocation loc = Current_location()) noexcept {
    return m_cur->restore(intent, mtr, loc);
  }

  [[nodiscard]] SavedAt saved_at() const noexcept { return m_cur->saved_at(); }

  /** A cursor that was never opened has nothing saved. */
  [[nodiscard]] bool has_saved() const noexcept { return m_cur != nullptr && m_cur->has_saved(); }

  /** Take over the saved position of src. */
  void copy_saved(IndexCursor &src) noexcept;

  void commit(Mtr *mtr) noexcept { m_cur->commit(mtr); }

  /** See IndexAm::Cursor::insert_batched(). */
  [[nodiscard]] DbErr insert_batched(const Index *index, DTuple *entry, ulint flags, QueThr *thr) noexcept;

  /** See IndexAm::Cursor::give_up_page(). */
  [[nodiscard]] bool give_up_page(Mtr *mtr) noexcept { return m_cur->give_up_page(mtr); }

  [[nodiscard]] DbErr remove(Intent intent, TrxRbCtx rb_ctx, Mtr *mtr) noexcept { return m_cur->remove(intent, rb_ctx, mtr); }

  [[nodiscard]] DbErr update(
    Intent intent, ulint flags, MemHeap **heap, BigRec *big_rec, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr
  ) noexcept {
    return m_cur->update(intent, flags, heap, big_rec, update, cmpl_info, thr, mtr);
  }

  [[nodiscard]] DbErr update_in_place(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *mtr) noexcept {
    return m_cur->update_in_place(flags, update, cmpl_info, thr, mtr);
  }

  [[nodiscard]] DbErr insert(Intent intent, ulint flags, DTuple *entry, BigRec *big_rec, ulint n_ext, QueThr *thr, Mtr *mtr) noexcept {
    return m_cur->insert(intent, flags, entry, big_rec, n_ext, thr, mtr);
  }

  [[nodiscard]] DbErr store_big_rec(const RecOffs &offsets, const BigRec &big_rec, Mtr *mtr) noexcept {
    return m_cur->store_big_rec(offsets, big_rec, mtr);
  }

  [[nodiscard]] DbErr set_delete_mark(ulint flags, bool val, QueThr *thr, Mtr *mtr) noexcept {
    return m_cur->set_delete_mark(flags, val, thr, mtr);
  }

  /** Nothing to do on a cursor that was never opened. */
  void close() noexcept {
    if (m_cur != nullptr) {
      m_cur->close();
    }
  }

  /** Close and give back the implementation's cursor, for a cursor that lives in memory
  that is freed without running destructors. It can still be used: it gets a new one. */
  void release() noexcept { m_cur.reset(); }

 private:
  /** Get a cursor of the implementation of index, once. */
  void bind(const Index *index);

  System *m_sys{};
  std::unique_ptr<IndexAm::Cursor> m_cur;

  /** What m_cur is a cursor of. */
  IndexAm *m_am{};
};

/** @return the canonical implementation, a B-tree. */
[[nodiscard]] std::unique_ptr<IndexAm> index_am_btree(System &sys);

/** @return the Lehman & Yao B-link tree, see idx0blink.h. */
[[nodiscard]] std::unique_ptr<IndexAm> index_am_blink(System &sys);
