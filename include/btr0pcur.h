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

/*** @file include/btr0pcur.h
The index tree persistent cursor

Created 2/23/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0types.h"
#include "data0data.h"
#include "dict0dict.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0cur.h"

/* Relative positions for a stored cursor position */
constexpr ulint BTR_PCUR_ON = 1;
constexpr ulint BTR_PCUR_BEFORE = 2;
constexpr ulint BTR_PCUR_AFTER  = 3;

/* Note that if the tree is not empty, btr_pcur_store_position does not
use the following, but only uses the above three alternatives, where the
position is stored relative to a specific record: this makes implementation
of a scroll cursor easier */

/* In an empty tree */
constexpr ulint BTR_PCUR_BEFORE_FIRST_IN_TREE = 4;

/*! In an empty tree */
constexpr ulint BTR_PCUR_AFTER_LAST_IN_TREE = 5;

/*** Allocates memory for a persistent cursor object and initializes the cursor.
@return	own: persistent cursor */
btr_pcur_t *btr_pcur_create();

/**
 * @brief Frees the memory for a persistent cursor object.
 *
 * @param cursor The persistent cursor to be freed.
 */
void btr_pcur_free(btr_pcur_t *cursor);

/**
 * @brief Copies the stored position of a persistent cursor to another persistent cursor.
 *
 * This function copies the stored position information from one persistent cursor to another.
 *
 * @param pcur_receive The persistent cursor which will receive the position information.
 * @param pcur_donate The persistent cursor from which the position information is copied.
 */
void btr_pcur_copy_stored_position(
  btr_pcur_t *pcur_receive,
  btr_pcur_t *pcur_donate);

#define btr_pcur_open(i, t, md, l, c, m)  \
  btr_pcur_open_func(i, t, md, l, c, __FILE__, __LINE__, m)

#define btr_pcur_open_with_no_init(ix, t, md, l, cur, has, m) \
  btr_pcur_open_with_no_init_func(ix, t, md, l, cur, has, __FILE__, __LINE__, m)

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
 * @param cursor The memory buffer for the persistent cursor.
 * @param file The file name where the function is called.
 * @param line The line number where the function is called.
 * @param mtr The mtr (mini-transaction) object.
 */
void btr_pcur_open_on_user_rec_func(dict_index_t *index, const dtuple_t *tuple, ib_srch_mode_t mode, ulint latch_mode, btr_pcur_t *cursor, const char *file, ulint line, mtr_t *mtr);

#define btr_pcur_open_on_user_rec(i, t, md, l, c, m) \
  btr_pcur_open_on_user_rec_func(i, t, md, l, c, __FILE__, __LINE__, m)

#define btr_pcur_open_at_rnd_pos(i, l, c, m) \
  btr_pcur_open_at_rnd_pos_func(i, l, c, __FILE__, __LINE__, m)

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
void btr_pcur_store_position(btr_pcur_t *cursor, mtr_t *mtr);

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
 * @param cursor The detached persistent cursor.
 * @param file The file name where the function is called.
 * @param line The line number where the function is called.
 * @param mtr The mtr (mini-transaction) object.
 * @return True if the cursor position was stored when it was on a user record and it can be restored on a user record
 *         whose ordering fields are identical to the ones of the original user record.
 */
bool btr_pcur_restore_position_func(ulint latch_mode, btr_pcur_t *cursor, const char *file, ulint line, mtr_t *mtr);

#define btr_pcur_restore_position(l, cur, mtr) \
  btr_pcur_restore_position_func(l, cur, __FILE__, __LINE__, mtr)

/**
 * @brief Releases the page latch and bufferfix reserved by the cursorif the latch mode is BTR_LEAF_SEARCH or BTR_LEAF_MODIFY.
 * 
 * Note: In the case of BTR_LEAF_MODIFY, there should not exist changes made by the current mini-transaction to the data protected by the cursor latch,
 * as then the latch must not be released until mtr_commit.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr (mini-transaction) object.
 */
void btr_pcur_release_leaf(btr_pcur_t *cursor, mtr_t *mtr);

/**
 * @brief Moves the persistent cursor to the first record on the next page.
 * Releases the latch on the current page and bufferunfixes it.
 * 
 * Note: There must not be modifications on the current page, as then the x-latch can be released only in mtr_commit.
 * 
 * @param cursor The persistent cursor; must be on the last record of the current page.
 * @param mtr The mtr (mini-transaction) object.
 */
void btr_pcur_move_to_next_page(btr_pcur_t *cursor, mtr_t *mtr);

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
 * @param cursor The persistent cursor; must be on the first record of the current page.
 * @param mtr The mtr (mini-transaction) object.
 */
void btr_pcur_move_backward_from_page(btr_pcur_t *cursor, mtr_t *mtr);

#ifndef UNIV_DEBUG
#define btr_pcur_get_btr_cur(cursor) (&(cursor)->btr_cur) 
#define btr_pcur_get_page_cur(cursor) (&(cursor)->btr_cur.page_cur)
#endif /* !UNIV_DEBUG */

/* The persistent B-tree cursor structure. This is used mainly for SQL
selects, updates, and deletes. */
struct btr_pcur_t {
  /*!< a B-tree cursor */
  btr_cur_t btr_cur;

  /** See the TODO note below. 
  BTR_SEARCH_LEAF, BTR_MODIFY_LEAF, BTR_MODIFY_TREE, or BTR_NO_LATCHES,
  depending on the latching state of the page and tree where the cursor is
  positioned; the last value means that the cursor is not currently positioned:
  we say then that the cursor is detached; it can be restored to
  attached if the old position was stored in old_rec */
  ulint latch_mode;

  /** BTR_PCUR_OLD_STORED or BTR_PCUR_OLD_NOT_STORED */
  ulint old_stored;

  /** if cursor position is stored, contains an initial segment of the
  latest record cursor was positioned either on, before, or after */
  rec_t *old_rec;

  /** number of fields in old_rec */
  ulint old_n_fields;

  /** BTR_PCUR_ON, BTR_PCUR_BEFORE, or BTR_PCUR_AFTER, depending on whether
  cursor was on, before, or after the old_rec record */
  ulint rel_pos;

  /** buffer block when the position was stored */
  buf_block_t *block_when_stored;

  /** the modify clock value of the buffer block when the cursor position
  was stored */
  uint64_t modify_clock;

  /** see TODO note below!
    BTR_PCUR_IS_POSITIONED,
    BTR_PCUR_WAS_POSITIONED,
    BTR_PCUR_NOT_POSITIONED */
  ulint pos_state;

  /** PAGE_CUR_G, ... */
  ib_srch_mode_t search_mode;

  /** the transaction, if we know it; otherwise this field is not defined;
  can ONLY BE USED in error prints in fatal assertion failures! */
  trx_t *trx_if_known;

  /*-----------------------------*/
  /* NOTE that the following fields may possess dynamically allocated
  memory which should be freed if not needed anymore! */

  /** NULL, or this field may contain a mini-transaction which holds the
  latch on the cursor page */
  mtr_t *mtr;

  /** NULL, or a dynamically allocated buffer for old_rec */
  byte *old_rec_buf;

  /** old_rec_buf size if old_rec_buf is not NULL */
  ulint buf_size;
};

/* TODO: currently, the state can be BTR_PCUR_IS_POSITIONED, though it
really should be BTR_PCUR_WAS_POSITIONED, because we have no obligation
to commit the cursor with mtr; similarly latch_mode may be out of date.
This can lead to problems if btr_pcur is not used the right way; all
current code should be ok. */
constexpr ulint BTR_PCUR_IS_POSITIONED = 1997660512;

constexpr ulint BTR_PCUR_WAS_POSITIONED = 1187549791;

constexpr ulint BTR_PCUR_NOT_POSITIONED = 1328997689;

constexpr ulint BTR_PCUR_OLD_STORED = 908467085;

constexpr ulint BTR_PCUR_OLD_NOT_STORED = 122766467;

/**
 * @brief Gets the rel_pos field for a cursor whose position has been stored.
 * 
 * @param cursor The persistent cursor.
 * @return The rel_pos field value.
 */
inline ulint btr_pcur_get_rel_pos(const btr_pcur_t *cursor) {
  ut_ad(cursor);
  ut_ad(cursor->old_rec);
  ut_ad(cursor->old_stored == BTR_PCUR_OLD_STORED);
  ut_ad(cursor->pos_state == BTR_PCUR_WAS_POSITIONED ||
        cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return cursor->rel_pos;
}

/**
 * @brief Sets the mtr field for a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr to set.
 */
inline void btr_pcur_set_mtr(btr_pcur_t *cursor, mtr_t *mtr) {
  cursor->mtr = mtr;
}

/**
 * @brief Gets the mtr field for a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return The mtr field.
 */
inline mtr_t *btr_pcur_get_mtr(btr_pcur_t *cursor) {
  return cursor->mtr;
}

#ifdef UNIV_DEBUG
/**
 * @brief Returns the btr cursor component of a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return Pointer to btr cursor component.
 */
inline btr_cur_t *btr_pcur_get_btr_cur(const btr_pcur_t *cursor) {
  const btr_cur_t *btr_cur = &cursor->btr_cur;
  return (btr_cur_t *)btr_cur;
}

/**
 * @brief Returns the page cursor component of a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return Pointer to page cursor component.
 */
inline page_cur_t *btr_pcur_get_page_cur(const btr_pcur_t *cursor) {
  return btr_cur_get_page_cur(btr_pcur_get_btr_cur(cursor));
}

#endif /* UNIV_DEBUG */

/**
 * @brief Returns the page of a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return Pointer to the page.
 */
inline page_t* btr_pcur_get_page(btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return btr_cur_get_page(btr_pcur_get_btr_cur(cursor));
}

/**
 * @brief Returns the buffer block of a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return Pointer to the block.
 */
inline buf_block_t* btr_pcur_get_block(btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return btr_cur_get_block(btr_pcur_get_btr_cur(cursor));
}

/**
 * @brief Returns the record of a persistent cursor.
 * 
 * @param cursor The persistent cursor.
 * @return Pointer to the record.
 */
inline rec_t* btr_pcur_get_rec(btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return btr_cur_get_rec(btr_pcur_get_btr_cur(cursor));
}

/**
 * @brief Gets the up_match value for a pcur after a search.
 * 
 * @param cursor The memory buffer for persistent cursor.
 * @return Number of matched fields at the cursor or to the right if search mode was PAGE_CUR_GE, otherwise undefined.
 */
inline ulint btr_pcur_get_up_match(btr_pcur_t* cursor) {
  btr_cur_t* btr_cursor;

  ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) || (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

  btr_cursor = btr_pcur_get_btr_cur(cursor);

  ut_ad(btr_cursor->up_match != ULINT_UNDEFINED);

  return btr_cursor->up_match;
}

/**
 * @brief Gets the low_match value for a persistent cursor after a search.
 * 
 * @param cursor The memory buffer for persistent cursor.
 * @return Number of matched fields at the cursor or to the right if search mode was PAGE_CUR_LE, otherwise undefined.
 */
inline ulint btr_pcur_get_low_match(btr_pcur_t* cursor) {
  btr_cur_t* btr_cursor;

  /**
   * @note The cursor must be either in the BTR_PCUR_WAS_POSITIONED or BTR_PCUR_IS_POSITIONED state.
   */
  ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) || (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

  btr_cursor = btr_pcur_get_btr_cur(cursor);

  /**
   * @note The low_match value must not be undefined.
   */
  ut_ad(btr_cursor->low_match != ULINT_UNDEFINED);

  return btr_cursor->low_match;
}

/**
 * @brief Checks if the persistent cursor is after the last user record on a page.
 * 
 * @param cursor The persistent cursor.
 * @return True if the cursor is after the last user record, false otherwise.
 */
inline bool btr_pcur_is_after_last_on_page(const btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return (page_cur_is_after_last(btr_pcur_get_page_cur(cursor)));
}

/**
 * @brief Checks if the persistent cursor is before the first user record on a page.
 * 
 * @param cursor The persistent cursor.
 * @return True if the cursor is before the first user record, false otherwise.
 */
inline bool btr_pcur_is_before_first_on_page(const btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return (page_cur_is_before_first(btr_pcur_get_page_cur(cursor)));
}

/**
 * @brief Checks if the persistent cursor is on a user record.
 * 
 * @param cursor The persistent cursor.
 * @return True if the cursor is on a user record, false otherwise.
 */
inline bool btr_pcur_is_on_user_rec(const btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_pcur_is_before_first_on_page(cursor) || btr_pcur_is_after_last_on_page(cursor)) {
    return false;
  }

  return true;
}

/**
 * @brief Checks if the persistent cursor is before the first user record in the index tree.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr.
 * @return True if the cursor is before the first user record in the index tree, false otherwise.
 */
inline bool btr_pcur_is_before_first_in_tree(btr_pcur_t* cursor, mtr_t* mtr) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_prev(btr_pcur_get_page(cursor), mtr) != FIL_NULL) {
    return false;
  }

  return page_cur_is_before_first(btr_pcur_get_page_cur(cursor));
}

/**
 * @brief Checks if the persistent cursor is after the last user record in the index tree.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr.
 * @return True if the cursor is after the last user record in the index tree, false otherwise.
 */
inline bool btr_pcur_is_after_last_in_tree(btr_pcur_t* cursor, mtr_t* mtr) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_next(btr_pcur_get_page(cursor), mtr) != FIL_NULL) {
    return false;
  }

  return page_cur_is_after_last(btr_pcur_get_page_cur(cursor));
}

/**
 * @brief Moves the persistent cursor to the next record on the same page.
 * 
 * @param cursor The persistent cursor.
 */
inline void btr_pcur_move_to_next_on_page(btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_next(btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/**
 * @brief Moves the persistent cursor to the previous record on the same page.
 * 
 * @param cursor The persistent cursor.
 */
inline void btr_pcur_move_to_prev_on_page(btr_pcur_t* cursor) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_prev(btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/**
 * @brief Moves the persistent cursor to the last record on the same page.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr.
 */
inline void btr_pcur_move_to_last_on_page(btr_pcur_t* cursor, mtr_t* mtr) {
  UT_NOT_USED(mtr);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_set_after_last(btr_pcur_get_block(cursor), btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/**
 * @brief Moves the persistent cursor to the next user record in the tree. If no user records are left, the cursor ends up 'after last in tree'.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr.
 * @return True if the cursor moved forward, ending on a user record.
 */
inline bool btr_pcur_move_to_next_user_rec(btr_pcur_t* cursor, mtr_t* mtr) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  for (;;) {
    if (btr_pcur_is_after_last_on_page(cursor)) {
      if (btr_pcur_is_after_last_in_tree(cursor, mtr)) {
        return false;
      }
      btr_pcur_move_to_next_page(cursor, mtr);
    } else {
      btr_pcur_move_to_next_on_page(cursor);
    }
    if (btr_pcur_is_on_user_rec(cursor)) {
      return true;
    }
  }
}

/**
 * @brief Moves the persistent cursor to the previous user record in the tree. If no user records are left, the cursor ends up 'before first in tree'.
 * 
 * @param cursor The persistent cursor.
 * @param mtr The mtr.
 * @return True if the cursor moved backward, ending on a user record.
 */
inline bool btr_pcur_move_to_prev_user_rec(
    btr_pcur_t* cursor, /*!< in: persistent cursor; NOTE that the function may release the page latch */
    mtr_t* mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  for (;;) {
    if (btr_pcur_is_before_first_on_page(cursor)) {
      if (btr_pcur_is_before_first_in_tree(cursor, mtr)) {
        return false;
      }
      btr_pcur_move_backward_from_page(cursor, mtr);
    } else {
      btr_pcur_move_to_prev_on_page(cursor);
    }
  
    if (btr_pcur_is_on_user_rec(cursor)) {
      return true;
    }
  }
}

/**
 * Moves the persistent cursor to the next record in the tree. If no records
 * are left, the cursor stays 'after last in tree'.
 *
 * @param cursor The persistent cursor, the function may release the page latch.
 * @param mtr The mtr
 * @return True if the cursor was not after last in tree
 */
inline bool btr_pcur_move_to_next(btr_pcur_t *cursor, mtr_t *mtr) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  if (btr_pcur_is_after_last_on_page(cursor)) {

    if (btr_pcur_is_after_last_in_tree(cursor, mtr)) {

      return false;
    }

    btr_pcur_move_to_next_page(cursor, mtr);

    return true;
  }

  btr_pcur_move_to_next_on_page(cursor);

  return true;
}


/**
 * Moves the persistent cursor to the previous record in the tree. If no
 * records are left, the cursor stays 'before first in tree'.
 *
 * @param cursor The persistent cursor; NOTE that the function may release the page latch.
 * @param mtr The mtr.
 * @return True if the cursor was not before first in tree.
 */
inline bool btr_pcur_move_to_prev(btr_pcur_t *cursor, mtr_t *mtr) {
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  if (btr_pcur_is_before_first_on_page(cursor)) {

    if (btr_pcur_is_before_first_in_tree(cursor, mtr)) {

      return false;
    }

    btr_pcur_move_backward_from_page(cursor, mtr);

    return true;
  }

  btr_pcur_move_to_prev_on_page(cursor);

  return true;
}


/**
 * Commits the mtr and sets the pcur latch mode to BTR_NO_LATCHES,
 * that is, the cursor becomes detached. If there have been modifications
 * to the page where pcur is positioned, this can be used instead of
 * btr_pcur_release_leaf. Function btr_pcur_store_position should be used
 * before calling this, if restoration of cursor is wanted later.
 *
 * @param pcur The persistent cursor.
 * @param mtr The mtr to commit.
 */
inline void btr_pcur_commit_specify_mtr(btr_pcur_t *pcur, mtr_t *mtr) {
  ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

  pcur->latch_mode = BTR_NO_LATCHES;

  mtr_commit(mtr);

  pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}



/**
 * Sets the persistent cursor latch mode to BTR_NO_LATCHES.
 *
 * @param pcur The persistent cursor.
 */
inline void btr_pcur_detach(btr_pcur_t *pcur) {
  ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

  pcur->latch_mode = BTR_NO_LATCHES;

  pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}


/**
 * Tests if a cursor is detached, that is the latch mode is BTR_NO_LATCHES.
 *
 * @param pcur The persistent cursor.
 * @return True if the cursor is detached.
 */
inline bool btr_pcur_is_detached(btr_pcur_t *pcur) {
  return pcur->latch_mode == BTR_NO_LATCHES;
}


/**
 * @brief Initializes the persistent cursor.
 *
 * This function sets the old_rec_buf field to NULL.
 *
 * @param pcur The persistent cursor.
 */
inline void btr_pcur_init(btr_pcur_t *pcur) {
  pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;
  pcur->old_rec_buf = NULL;
  pcur->old_rec = NULL;
}

/**
 * @brief Initializes and opens a persistent cursor to an index tree.
 *        It should be closed with btr_pcur_close.
 *
 * @param dict_index The dict_index.
 * @param tuple The tuple on which search is done.
 * @param mode The search mode (PAGE_CUR_L, ...).
 *             NOTE that if the search is made using a unique prefix of a record,
 *             mode should be PAGE_CUR_LE, not PAGE_CUR_GE, as the latter may
 *             end up on the previous page from the record!
 * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
 * @param cursor The memory buffer for persistent cursor.
 * @param file The file name.
 * @param line The line where called.
 * @param mtr The mtr.
 */
inline void btr_pcur_open_func(dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t mode, ulint latch_mode, btr_pcur_t *cursor, const char *file, ulint line, mtr_t *mtr) {
  btr_cur_t *btr_cursor;

  /* Initialize the cursor */
  btr_pcur_init(cursor);

  cursor->latch_mode = latch_mode;
  cursor->search_mode = mode;

  /* Search with the tree cursor */
  btr_cursor = btr_pcur_get_btr_cur(cursor);
  btr_cur_search_to_nth_level(dict_index, 0, tuple, mode, latch_mode,
                              btr_cursor, 0, file, line, mtr);
  cursor->pos_state = BTR_PCUR_IS_POSITIONED;
  cursor->trx_if_known = NULL;
}

/**
 * @brief Opens a persistent cursor to an index tree without initializing the cursor.
 *
 * @param dict_index The dict_index.
 * @param tuple The tuple on which the search is done.
 * @param mode The search mode (PAGE_CUR_L, ...).
 * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
 * @param cursor The memory buffer for the persistent cursor.
 * @param has_search_latch The latch mode the caller currently has on btr_search_latch: RW_S_LATCH, or 0.
 * @param file The file name.
 * @param line The line where called.
 * @param mtr The mtr.
 */
inline void btr_pcur_open_with_no_init_func(dict_index_t *dict_index, const dtuple_t *tuple, ib_srch_mode_t mode, ulint latch_mode,
                                            btr_pcur_t *cursor, ulint has_search_latch, const char *file, ulint line, mtr_t *mtr) {
  btr_cur_t *btr_cursor;

  cursor->latch_mode = latch_mode;
  cursor->search_mode = mode;

  /* Search with the tree cursor */
  btr_cursor = btr_pcur_get_btr_cur(cursor);
  btr_cur_search_to_nth_level(dict_index, 0, tuple, mode, latch_mode, btr_cursor, has_search_latch, file, line, mtr);
  cursor->pos_state = BTR_PCUR_IS_POSITIONED;
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
  cursor->trx_if_known = NULL;
}

/**
 * @brief Opens a persistent cursor at either end of an index.
 *
 * @param from_left True if open to the low end, false if open to the high end.
 * @param dict_index The dict_index.
 * @param latch_mode The latch mode.
 * @param pcur The cursor.
 * @param do_init True if the cursor should be initialized.
 * @param mtr The mtr.
 */
inline void btr_pcur_open_at_index_side(bool from_left, dict_index_t *dict_index, ulint latch_mode, btr_pcur_t *pcur, bool do_init, mtr_t *mtr) {
  pcur->latch_mode = latch_mode;

  if (from_left) {
    pcur->search_mode = PAGE_CUR_G;
  } else {
    pcur->search_mode = PAGE_CUR_L;
  }

  if (do_init) {
    btr_pcur_init(pcur);
  }

  btr_cur_open_at_index_side(from_left, dict_index, latch_mode, btr_pcur_get_btr_cur(pcur), mtr);
  pcur->pos_state = BTR_PCUR_IS_POSITIONED;
  pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;
  pcur->trx_if_known = NULL;
}


/**
 * @brief Positions a cursor at a randomly chosen position within a B-tree.
 *
 * @param dict_index The dict_index.
 * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
 * @param cursor The B-tree pcur.
 * @param file The file name.
 * @param line The line where called.
 * @param mtr The mtr.
 */
inline void btr_pcur_open_at_rnd_pos_func(dict_index_t *dict_index, ulint latch_mode, btr_pcur_t *cursor, const char *file, ulint line, mtr_t *mtr) {
  cursor->latch_mode = latch_mode;
  cursor->search_mode = PAGE_CUR_G;

  btr_pcur_init(cursor);

  btr_cur_open_at_rnd_pos_func(dict_index, latch_mode, btr_pcur_get_btr_cur(cursor), file, line, mtr);
  cursor->pos_state = BTR_PCUR_IS_POSITIONED;
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
  cursor->trx_if_known = NULL;
}

/**
 * @brief Frees the possible memory heap of a persistent cursor and sets the latch mode of the persistent cursor to BTR_NO_LATCHES.
 *
 * @param cursor The persistent cursor.
 */
inline void btr_pcur_close(btr_pcur_t *cursor) {
  if (cursor->old_rec_buf != NULL) {
    mem_free(cursor->old_rec_buf);
    cursor->old_rec = NULL;
    cursor->old_rec_buf = NULL;
  }

  cursor->btr_cur.m_page_cur.rec = NULL;
  cursor->btr_cur.m_page_cur.block = NULL;
  cursor->old_rec = NULL;
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
  cursor->latch_mode = BTR_NO_LATCHES;
  cursor->pos_state = BTR_PCUR_NOT_POSITIONED;
  cursor->trx_if_known = NULL;
}

