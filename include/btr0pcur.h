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

/*** @file include/btr0pcur.h
The index tree persistent cursor

Created 2/23/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0cur.h"

struct mtr_t;
struct buf_block_t;

/* Relative positions for a stored cursor position */
enum class Btree_cursor_pos : uint8_t {
  UNSET = 0,
  ON = 1,
  BEFORE = 2,
  AFTER = 3,

  /* Note that if the tree is not empty, btr_pcur_store_position does not
  use the following, but only uses the above three alternatives, where the
  position is stored relative to a specific record: this makes implementation
  of a scroll cursor easier */

  /** In an empty tree */
  BEFORE_FIRST_IN_TREE = 4,

  /**In an empty tree */
  AFTER_LAST_IN_TREE = 5
};

enum class Btr_pcur_positioned {
  /** Persistent cursor is not positioned. */
  UNSET = 0,

  /** The persistent cursor is positioned by index search.
  Or optimistic get for rel_pos == BTR_PCUR_ON. */
  IS_POSITIONED,

  /** The persistent cursor is positioned by optimistic get to the same
  record as it was positioned at. Not used for rel_pos == Btree_cursor_pos::ON.
  It may need adjustment depending on previous/current search direction
  and rel_pos. */
  IS_POSITIONED_OPTIMISTIC,

  /* TODO: currently, the state can be IS_POSITIONED, though it
  really should be WAS_POSITIONED, because we have no obligation
  to commit the cursor with mtr; similarly latch_mode may be out of date.
  This can lead to problems if btr_pcur is not used the right way; all
  current code should be ok. */
  WAS_POSITIONED,
};

/* The persistent B-tree cursor structure. This is used mainly for SQL
selects, updates, and deletes. */
struct btr_pcur_t {
  struct Ctx {
    dict_index_t *m_dict_index{};
    const dtuple_t *m_tuple{};
    ib_srch_mode_t m_search_mode{};
    ulint m_latch_mode{};
    mtr_t* m_mtr{};
    Source_location m_loc{};
  };

  /** @brief Constructor. */
  btr_pcur_t();

  /**
   * @brief Destructor.
   */
  ~btr_pcur_t();

  /**
   * @brief Copies the stored position of a persistent cursor to this persistent cursor.
   *
   * This function copies the stored position information from one persistent cursor to another.
   *
   * @param src The persistent cursor from which the position information is copied.
   */
  void copy_stored_position(btr_pcur_t *src);

  /**
   * @brief Opens a persistent cursor on the first or last user record satisfying the search condition.
   *
   * If the mode is PAGE_CUR_G or PAGE_CUR_GE, the cursor is positioned on the first user record satisfying the search condition.
   * If the mode is PAGE_CUR_L or PAGE_CUR_LE, the cursor is positioned on the last user record satisfying the search condition.
   * If no such user record exists, the cursor is positioned after the last record in the tree for PAGE_CUR_G or PAGE_CUR_GE mode,
   * and before the first record in the tree for PAGE_CUR_L or PAGE_CUR_LE mode.
   *
   * @param index The index on which the search is performed.
   * @param tuple The tuple on which the search is done.
   * @param mode The search mode (PAGE_CUR_L, PAGE_CUR_LE, PAGE_CUR_G, or PAGE_CUR_GE).
   * @param latch_mode The latching mode (BTR_SEARCH_LEAF or BTR_MODIFY_LEAF).
   * @param file The file name where the function is called.
   * @param line The line number where the function is called.
   * @param mtr The mtr (mini-transaction) object.
   */
  void open_on_user_rec(dict_index_t *index, const dtuple_t *tuple, ib_srch_mode_t mode, ulint latch_mode, mtr_t *mtr, Source_location loc);
  
   /** Allows setting the persistent cursor manually.
    * @param[in] cursor      Page cursor where positioned.
    * @param[in] mode        PAGE_CUR_L, ...
    * @param[in] latch_mode  BTR_SEARCH_LEAF or BTR_MODIFY_LEAF */
  void open_on_user_rec(const page_cur_t &cursor, ib_srch_mode_t mode, ulint latch_mode);


  /**
   * @brief Stores the position of the cursor.
   *
   * The position of the cursor is stored by taking an initial segment of the
   * record the cursor is positioned on, * before, or after, and copying it
   * to the cursor data structure, or just setting a flag if the cursor is
   * before the first in an EMPTY tree, or after the last in an EMPTY tree.
   * 
   * NOTE that the page where the cursor is positioned must not be empty
   * if the index tree is not totally empty!
   *
   * @param cursor The persistent cursor.
   * @param mtr The mtr (mini-transaction) object.
   */
  void store_position(mtr_t *mtr);

  /**
   * @brief Restores the stored position of a persistent cursor.
   *
   * This function bufferfixes the page and obtains the specified latches to restore the stored position of the cursor.
   * If the cursor position was saved when:
   * (1) The cursor was positioned on a user record: this function restores the position to the last record LESS OR EQUAL
   *     to the stored record.
   * (2) The cursor was positioned on a page infimum record: restores the position to the last record LESS than the user
   *     record which was the successor of the page infimum.
   * (3) The cursor was positioned on the page supremum: restores to the first record GREATER than the user record which
   *     was the predecessor of the supremum.
   * (4) The cursor was positioned before the first or after the last in an empty tree: restores to before first or after
   *     the last in the tree.
   *
   * @param latch_mode The latching mode (BTR_SEARCH_LEAF, ...).
   * @param mtr The mtr (mini-transaction) object.
   * @param loc The callers location
   * @return True if the cursor position was stored when it was on a user record and it can be restored on a user record
   *         whose ordering fields are identical to the ones of the original user record.
   */
  bool restore_position(ulint latch_mode, mtr_t *mtr, Source_location loc);

  /**
   * @brief Releases the page latch and bufferfix reserved by the cursorif the latch mode is BTR_LEAF_SEARCH or BTR_LEAF_MODIFY.
   * 
   * Note: In the case of BTR_LEAF_MODIFY, there should not exist changes made by the current mini-transaction to the
   * data protected by the cursor latch, as then the latch must not be released until mtr_commit.
   * 
   * @param mtr The mtr (mini-transaction) object.
   */
  void release_leaf(mtr_t *mtr);

  /**
   * @brief Moves the persistent cursor to the first record on the next page.
   * Releases the latch on the current page and bufferunfixes it.
   * 
   * Note: There must not be modifications on the current page, as then the x-latch can be released only in mtr_commit.
   * 
   * @param mtr The mtr (mini-transaction) object.
   */
  void move_to_next_page(mtr_t *mtr);

  /**
   * @brief Moves the persistent cursor backward if it is on the first record of the page.
   * Releases the latch on the current page and bufferunfixes it.
   * 
   * Note: To prevent a possible deadlock, the operation first stores the position of the cursor,
   * releases the leaf latch, acquires necessary latches, and restores the cursor position again before returning.
   * The alphabetical position of the cursor is guaranteed to be sensible on return,
   * but it may happen that the cursor is not positioned on the last record of any page,
   * because the structure of the tree may have changed while the cursor had no latches.
   * 
   * @param mtr The mtr (mini-transaction) object.
   */
  void move_backward_from_page(mtr_t *mtr);

  /**
   * @brief Gets the rel_pos field for a cursor whose position has been stored.
   * 
   * @return The rel_pos field value.
   */
  Btree_cursor_pos get_rel_pos() const;

  /**
   * @brief Sets the mtr field for a persistent cursor.
   * 
   * @param mtr The mtr to set.
   */
  void set_mtr(mtr_t *mtr);

  /**
   * @brief Gets the mtr field for a persistent cursor.
   * 
   * @return The mtr field.
   */
  mtr_t *get_mtr();

  /**
   * @brief Returns the page of a persistent cursor.
   * 
   * @return Pointer to the page.
   */
  page_t *get_page();

  /**
   * @brief Returns the buffer block of a persistent cursor.
   * 
   * @return Pointer to the block.
   */
  buf_block_t *get_block();

  /**
   * @brief The index of the persistent cursor.
   * 
   * @return the index being traversed.
   */
  dict_index_t *get_index() {
    return get_btr_cur()->m_index;
  }
  /**
   * @brief Returns the record of a persistent cursor.
   * 
   * @return Pointer to the record.
   */
  rec_t *get_rec();

  /**
   * @brief Gets the up_match value for a pcur after a search.
   * 
   * @return Number of matched fields at the cursor or to the right if search mode was PAGE_CUR_GE, otherwise undefined.
   */
  ulint get_up_match();

  /**
   * @brief Gets the low_match value for a persistent cursor after a search.
   * 
   * @return Number of matched fields at the cursor or to the right if search mode was PAGE_CUR_LE, otherwise undefined.
   */
  ulint get_low_match();

  /**
   * @brief Checks if the persistent cursor is after the last user record on a page.
   * 
   * @return True if the cursor is after the last user record, false otherwise.
   */
  bool is_after_last_on_page() const;

  /**
   * @brief Checks if the persistent cursor is before the first user record on a page.
   * 
   * @return True if the cursor is before the first user record, false otherwise.
   */
  bool is_before_first_on_page() const;

  /**
   * @brief Checks if the persistent cursor is on a user record.
   * 
   * @return True if the cursor is on a user record, false otherwise.
   */
  bool is_on_user_rec() const;

  /**
   * @brief Checks if the persistent cursor is before the first user record in the index tree.
   * 
   * @param mtr The mtr.
   * @return True if the cursor is before the first user record in the index tree, false otherwise.
   */
  bool is_before_first_in_tree(mtr_t *mtr);

  /**
   * @brief Checks if the persistent cursor is after the last user record in the index tree.
   * 
   * @param mtr The mtr.
   * @return True if the cursor is after the last user record in the index tree, false otherwise.
   */
  bool is_after_last_in_tree(mtr_t *mtr);

  /**
   * @brief Moves the persistent cursor to the next record on the same page.
   */
  void move_to_next_on_page();

  /**
   * @brief Moves the persistent cursor to the previous record on the same page.
   * 
   */
  void move_to_prev_on_page();

  /**
   * @brief Moves the persistent cursor to the last record on the same page.
   * 
   * @param mtr The mtr.
   */
  void move_to_last_on_page(mtr_t *mtr);

  /**
   * @brief Moves the persistent cursor to the next user record in the tree.
   * If no user records are left, the cursor ends up 'after last in tree'.
   * 
   * @param mtr The mtr.
   * @return True if the cursor moved forward, ending on a user record.
   */
  bool move_to_next_user_rec(mtr_t *mtr);

  /**
   * @brief Moves the persistent cursor to the previous user record in the tree. If no
   * user records are left, the cursor ends up 'before first in tree.
   * NOTE: The function may release the page lock.
   * 
   * @param mtr The mtr.
   * @return True if the cursor moved backward, ending on a user record.
   */
  bool move_to_prev_user_rec(mtr_t *mtr);

  /**
   * Moves the persistent cursor to the next record in the tree. If no records
   * are left, the cursor stays 'after last in tree'.
   *
   * @param mtr The mtr
   * @return True if the cursor was not after last in tree
   */
  bool move_to_next(mtr_t *mtr);

  /**
   * Moves the persistent cursor to the previous record in the tree. If no
   * records are left, the cursor stays 'before first in tree'.
   *
   * @param mtr The mtr.
   * @return True if the cursor was not before first in tree.
   */
  bool move_to_prev(mtr_t *mtr);

  /**
   * Commits the mtr and sets the pcur latch mode to BTR_NO_LATCHES,
   * that is, the cursor becomes detached. If there have been modifications
   * to the page where pcur is positioned, this can be used instead of
   * release_leaf. Function store_position should be used before calling
   * this, if restoration of cursor is wanted later.
   *
   * @param mtr The mtr to commit.
   */
  void commit_specify_mtr(mtr_t *mtr);

  /**
   * Sets the persistent cursor latch mode to BTR_NO_LATCHES.
   */
  void detach(); 

  /**
   * Tests if a cursor is detached, that is the latch mode is BTR_NO_LATCHES.
   *
   * @return True if the cursor is detached.
   */
  bool is_detached();

  /** Free old_rec_buf. */
  void free_rec_buf() {
    mem_free(m_old_rec_buf);
    m_old_rec_buf = nullptr;
  }
  /**
   * @brief Initializes the persistent cursor.
   *
   * This function sets the old_rec_buf field to nullptr.
   * @param[in]  read_level  read level where the cursor would be positioned or
   * re-positioned.
   */
  void init(ulint read_level);

  /**
   * @brief Initializes and opens a persistent cursor to an index tree.
   *        It should be closed with close.
   *
   * @param dict_index The dict_index.
   * @param tuple The tuple on which search is done.
   * @param search_mode The search mode (PAGE_CUR_L, ...).
   *             NOTE that if the search is made using a unique prefix of a record,
   *             mode should be PAGE_CUR_LE, not PAGE_CUR_GE, as the latter may
   *             end up on the previous page from the record!
   * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
   * @param mtr The mtr.
   * @param file The file name.
   * @param line The line where called.
   */
  void open(
    dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t search_mode, ulint latch_mode, mtr_t* mtr, Source_location loc);

  /**
   * @brief Opens a persistent cursor to an index tree without initializing the cursor.
   *
   * @param dict_index The dict_index.
   * @param tuple The tuple on which the search is done.
   * @param search_mode The search mode (PAGE_CUR_L, ...).
   * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
   * @param has_search_latch The latch mode the caller currently has on btr_search_latch: RW_S_LATCH, or 0.
   * @param mtr The mtr.
   * @param file The file name.
   * @param line The line where called.
   */
  void open_with_no_init(
    dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t search_mode, ulint latch_mode,
    ulint has_search_latch, mtr_t *mtr, Source_location loc);

  /**
   * @brief Opens a persistent cursor at either end of an index.
   *
   * @param from_left True if open to the low end, false if open to the high end.
   * @param dict_index The dict_index.
   * @param latch_mode The latch mode.
   * @param do_init True if the cursor should be initialized.
   * @param[in] level read level where the cursor would be positioned or re-positioned.
   * @param mtr The mtr.
   */
  void open_at_index_side(bool from_left, dict_index_t *dict_index, ulint latch_mode, bool do_init, ulint level, mtr_t *mtr);

  /**
   * @brief Positions a cursor at a randomly chosen position within a B-tree.
   *
   * @param dict_index The dict_index.
   * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
   * @param mtr The mtr.
   * @param file The file name.
   * @param line The line where called.
   */
  void set_random_position(dict_index_t *dict_index, ulint latch_mode, mtr_t* mtr, Source_location loc);

  /**
   * @brief Frees the possible memory heap of a persistent cursor and sets the latch mode of the persistent cursor to BTR_NO_LATCHES.
   */
  void close();

  /**
   * @brief Returns the btr cursor component of a persistent cursor.
   * 
   * @return Pointer to btr cursor component.
   */
  btr_cur_t *get_btr_cur();
  
  /**
   * @brief Returns the btr cursor component of a persistent cursor.
   * 
   * @return Pointer to btr cursor component.
   */
  const btr_cur_t *get_btr_cur() const;

  /**
   * @brief Returns the page cursor component of a persistent cursor.
   * 
   * @return Pointer to page cursor component.
   */
  page_cur_t *get_page_cur();

  /**
   * @brief Returns the page cursor component of a persistent cursor.
   * 
   * @return Pointer to page cursor component.
   */
  const page_cur_t *get_page_cur() const;

  /** a B-tree cursor */
  btr_cur_t m_btr_cur;

  /** See the TODO note below. 
  BTR_SEARCH_LEAF, BTR_MODIFY_LEAF, BTR_MODIFY_TREE, or BTR_NO_LATCHES,
  depending on the latching state of the page and tree where the cursor is
  positioned; the last value means that the cursor is not currently positioned:
  we say then that the cursor is detached; it can be restored to
  attached if the old position was stored in old_rec */
  ulint m_latch_mode{};

  /** true if the old cursor position is stored */
  bool m_old_stored{};

  /** if cursor position is stored, contains an initial segment of the
  latest record cursor was positioned either on, before, or after */
  rec_t *m_old_rec{};

  /** number of fields in old_rec */
  ulint m_old_n_fields{};

  /** Btree_cursor_pos::ON, Btree_cursor_pos::BEFORE, or Btree_cursor_pos::AFTER, depending on whether
  cursor was on, before, or after the old_rec record */
  Btree_cursor_pos m_rel_pos{Btree_cursor_pos::UNSET};

  /** buffer block when the position was stored */
  buf_block_t *m_block_when_stored{};

  /** the modify clock value of the buffer block when the cursor position
  was stored */
  uint64_t m_modify_clock{};

  /** Cursor postiion state. */
  Btr_pcur_positioned m_pos_state{Btr_pcur_positioned::UNSET};

  /** PAGE_CUR_G, ... */
  ib_srch_mode_t m_search_mode{};

  /** the transaction, if we know it; otherwise this field is not defined;
  can ONLY BE USED in error prints in fatal assertion failures! */
  trx_t *m_trx_if_known{};

  /*-----------------------------*/
  /* NOTE that the following fields may possess dynamically allocated
  memory which should be freed if not needed anymore! */

  /** nullptr, or this field may contain a mini-transaction which holds the
  latch on the cursor page */
  mtr_t *m_mtr{};

  /** nullptr, or a dynamically allocated buffer for old_rec */
  byte *m_old_rec_buf{};

  /** old_rec_buf size if old_rec_buf is not nullptr */
  ulint m_buf_size{};

  /** Read level where the cursor would be positioned or re-positioned. */
  ulint m_read_level{};
};

inline Btree_cursor_pos btr_pcur_t::get_rel_pos() const {
  ut_ad(m_old_rec != nullptr);
  ut_ad(m_old_stored);

  ut_ad(m_pos_state == Btr_pcur_positioned::WAS_POSITIONED ||
        m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  return m_rel_pos;
}

inline void btr_pcur_t::set_mtr(mtr_t *mtr) {
  m_mtr = mtr;
}

inline mtr_t *btr_pcur_t::get_mtr() {
  return m_mtr;
}

inline const btr_cur_t *btr_pcur_t::get_btr_cur() const {
  return reinterpret_cast<const btr_cur_t *>(&m_btr_cur);
}

inline btr_cur_t *btr_pcur_t::get_btr_cur() {
  return reinterpret_cast<btr_cur_t *>(&m_btr_cur);
}

inline const page_cur_t *btr_pcur_t::get_page_cur() const {
  return btr_cur_get_page_cur(get_btr_cur());
}

inline page_cur_t *btr_pcur_t::get_page_cur() {
  return btr_cur_get_page_cur(get_btr_cur());
}

inline page_t *btr_pcur_t::get_page() {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  return btr_cur_get_page(get_btr_cur());
}

inline buf_block_t *btr_pcur_t::get_block() {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  return btr_cur_get_block(get_btr_cur());
}

inline rec_t *btr_pcur_t::get_rec() {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  return btr_cur_get_rec(get_btr_cur());
}

inline ulint btr_pcur_t::get_up_match() {
  ut_ad(m_pos_state == Btr_pcur_positioned::WAS_POSITIONED ||
        m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  auto btr_cur = get_btr_cur();

  ut_ad(btr_cur->up_match != ULINT_UNDEFINED);

  return btr_cur->up_match;
}

inline ulint btr_pcur_t::get_low_match() {
  /**
   * @note The cursor must be either in the WAS_POSITIONED or IS_POSITIONED state.
   */
  ut_ad(m_pos_state == Btr_pcur_positioned::WAS_POSITIONED ||
        m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  auto btr_cur = get_btr_cur();

  /**
   * @note The low_match value must not be undefined.
   */
  ut_ad(btr_cur->low_match != ULINT_UNDEFINED);

  return btr_cur->low_match;
}

inline bool btr_pcur_t::is_after_last_on_page() const {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  return page_cur_is_after_last(get_page_cur());
}

inline bool btr_pcur_t::is_before_first_on_page() const {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  return page_cur_is_before_first(get_page_cur());
}

inline bool btr_pcur_t::is_on_user_rec() const {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  return !is_before_first_on_page() && !is_after_last_on_page();
}

inline bool btr_pcur_t::is_before_first_in_tree(mtr_t *m) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_prev(get_page(), m) != FIL_NULL) {
    return false;
  } else {
    return page_cur_is_before_first(get_page_cur());
  }
}

inline bool btr_pcur_t::is_after_last_in_tree(mtr_t *m) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_next(get_page(), m) != FIL_NULL) {
    return false;
  } else {
    return page_cur_is_after_last(get_page_cur());
  }
}

inline void btr_pcur_t::move_to_next_on_page() {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_next(get_page_cur());

  m_old_stored = false;
}

inline void btr_pcur_t::move_to_prev_on_page() {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_prev(get_page_cur());

  m_old_stored = false;
}

inline void btr_pcur_t::move_to_last_on_page(mtr_t *) {
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  page_cur_set_after_last(get_block(), get_page_cur());

  m_old_stored = false;
}

inline bool btr_pcur_t::move_to_next_user_rec(mtr_t *mtr) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  m_old_stored = false;

  for (;;) {
    if (is_after_last_on_page()) {
      if (is_after_last_in_tree(mtr)) {
        return false;
      }
      move_to_next_page(mtr);
    } else {
      move_to_next_on_page();
    }
    if (is_on_user_rec()) {
      return true;
    }
  }
}

inline bool btr_pcur_t::move_to_prev_user_rec(mtr_t *mtr) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  m_old_stored = false;

  for (;;) {
    if (is_before_first_on_page()) {
      if (is_before_first_in_tree(mtr)) {
        return false;
      }
      move_backward_from_page(mtr);
    } else {
      move_to_prev_on_page();
    }

    if (is_on_user_rec()) {
      return true;
    }
  }
}

inline bool btr_pcur_t::move_to_next(mtr_t *mtr) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  m_old_stored = false;

  if (is_after_last_on_page()) {

    if (is_after_last_in_tree(mtr)) {

      return false;
    }

    move_to_next_page(mtr);

    return true;
  }

  move_to_next_on_page();

  return true;
}

inline bool btr_pcur_t::move_to_prev(mtr_t *mtr) {
  ut_ad(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);
  ut_ad(m_latch_mode != BTR_NO_LATCHES);

  m_old_stored = false;

  if (is_before_first_on_page()) {

    if (is_before_first_in_tree(mtr)) {

      return false;
    }

    move_backward_from_page(mtr);

    return true;
  }

  move_to_prev_on_page();

  return true;
}

inline void btr_pcur_t::commit_specify_mtr(mtr_t *mtr) {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  m_latch_mode = BTR_NO_LATCHES;

  mtr_commit(mtr);

  m_pos_state = Btr_pcur_positioned::WAS_POSITIONED;
}

inline void btr_pcur_t::detach() {
  ut_a(m_pos_state == Btr_pcur_positioned::IS_POSITIONED);

  m_latch_mode = BTR_NO_LATCHES;
  m_pos_state = Btr_pcur_positioned::WAS_POSITIONED;
}

inline bool btr_pcur_t::is_detached() {
  return m_latch_mode == BTR_NO_LATCHES;
}

inline void btr_pcur_t::init(ulint read_level) {
  m_old_stored = false;
  m_old_rec_buf = nullptr;
  m_old_rec = nullptr;
  m_read_level = read_level;
}

inline void btr_pcur_t::open(
  dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t search_mode, ulint latch_mode, mtr_t* mtr, Source_location loc) {

  init(0);

  m_latch_mode = latch_mode;
  m_search_mode = search_mode;

  /* Search with the tree cursor */
  auto btr_cur = get_btr_cur();

  btr_cur_search_to_nth_level(dict_index, m_read_level, tuple, search_mode, latch_mode, btr_cur, 0, loc.m_from.file_name(), loc.m_from.line(), mtr);

  m_pos_state = Btr_pcur_positioned::IS_POSITIONED;

  m_trx_if_known = nullptr;
}

inline void btr_pcur_t::open_with_no_init(
  dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t search_mode, ulint latch_mode, ulint has_search_latch,
  mtr_t* mtr, Source_location loc) {

  m_latch_mode = latch_mode;
  m_search_mode = search_mode;

  /* Search with the tree cursor */
  auto btr_cur = get_btr_cur();

  btr_cur_search_to_nth_level(dict_index, m_read_level, tuple, search_mode, latch_mode, btr_cur, has_search_latch, loc.m_from.file_name(), loc.m_from.line() , mtr);

  m_pos_state = Btr_pcur_positioned::IS_POSITIONED;
  m_old_stored = false;
  m_trx_if_known = nullptr;
}

inline void btr_pcur_t::open_at_index_side(bool from_left, dict_index_t *dict_index, ulint latch_mode, bool do_init, ulint level, mtr_t *mtr) {
  m_latch_mode = latch_mode;

  if (from_left) {
    m_search_mode = PAGE_CUR_G;
  } else {
    m_search_mode = PAGE_CUR_L;
  }

  if (do_init) {
    init(level);
  }

  btr_cur_open_at_index_side(from_left, dict_index, latch_mode, get_btr_cur(), m_read_level, mtr);

  m_pos_state = Btr_pcur_positioned::IS_POSITIONED;
  m_old_stored = false;
  m_trx_if_known = nullptr;
}

inline void btr_pcur_t::set_random_position(dict_index_t *dict_index, ulint latch_mode, mtr_t *mtr, Source_location loc) {

  m_latch_mode = latch_mode;
  m_search_mode = PAGE_CUR_G;

  init(0);

  btr_cur_open_at_rnd_pos(dict_index, latch_mode, get_btr_cur(), mtr);

  m_pos_state = Btr_pcur_positioned::IS_POSITIONED;
  m_old_stored = false;
  m_trx_if_known = nullptr;
}

inline void btr_pcur_t::close() {
  if (m_old_rec_buf != nullptr) {
    mem_free(m_old_rec_buf);
    m_old_rec = nullptr;
    m_old_rec_buf = nullptr;
  }

  m_btr_cur.m_page_cur.m_rec = nullptr;
  m_btr_cur.m_page_cur.m_block = nullptr;
  m_old_rec = nullptr;
  m_old_stored = false;
  m_latch_mode = BTR_NO_LATCHES;
  m_pos_state = Btr_pcur_positioned::UNSET;
  m_trx_if_known = nullptr;
}
