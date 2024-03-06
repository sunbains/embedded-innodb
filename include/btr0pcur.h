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

/*** Frees the memory for a persistent cursor object. */

void btr_pcur_free(btr_pcur_t *cursor); /*!< in, own: persistent cursor */

/*** Copies the stored position of a pcur to another pcur. */
void btr_pcur_copy_stored_position(
    btr_pcur_t *pcur_receive, /*!< in: pcur which will receive the
                              position info */
    btr_pcur_t *pcur_donate); /*!< in: pcur from which the info is
                              copied */

/*** Sets the old_rec_buf field to NULL. */
inline void btr_pcur_init(btr_pcur_t *pcur); /*!< in: persistent cursor */

/*** Initializes and opens a persistent cursor to an index tree. It should be
closed with btr_pcur_close. */
inline void btr_pcur_open_func(
    dict_index_t *index,   /*!< in: index */
    const dtuple_t *tuple, /*!< in: tuple on which search done */
    ib_srch_mode_t mode,   /*!< in: PAGE_CUR_L, ...;
                  NOTE that if the search is made using a unique
                  prefix of a record, mode should be
                  PAGE_CUR_LE, not PAGE_CUR_GE, as the latter
                  may end up on the previous page from the
                  record! */
    ulint latch_mode,      /*!< in: BTR_SEARCH_LEAF, ... */
    btr_pcur_t *cursor,    /*!< in: memory buffer for persistent cursor */
    const char *file,      /*!< in: file name */
    ulint line,            /*!< in: line where called */
    mtr_t *mtr);           /*!< in: mtr */

#define btr_pcur_open(i, t, md, l, c, m)  \
  btr_pcur_open_func(i, t, md, l, c, __FILE__, __LINE__, m)

/*** Opens an persistent cursor to an index tree without initializing the
cursor. */
inline void btr_pcur_open_with_no_init_func(
    dict_index_t *index,    /*!< in: index */
    const dtuple_t *tuple,  /*!< in: tuple on which search done */
    ib_srch_mode_t mode,    /*!< in: PAGE_CUR_L, ...;
                   NOTE that if the search is made using a unique
                   prefix of a record, mode should be
                   PAGE_CUR_LE, not PAGE_CUR_GE, as the latter
                   may end up on the previous page of the
                   record! */
    ulint latch_mode,       /*!< in: BTR_SEARCH_LEAF, ...;
                         NOTE that if has_search_latch != 0 then
                         we maybe do not acquire a latch on the cursor
                         page, but assume that the caller uses his
                         btr search latch to protect the record! */
    btr_pcur_t *cursor,     /*!< in: memory buffer for persistent cursor */
    ulint has_search_latch, /*!< in: latch mode the caller
                   currently has on btr_search_latch:
                   RW_S_LATCH, or 0 */
    const char *file,       /*!< in: file name */
    ulint line,             /*!< in: line where called */
    mtr_t *mtr);            /*!< in: mtr */

#define btr_pcur_open_with_no_init(ix, t, md, l, cur, has, m) \
  btr_pcur_open_with_no_init_func(ix, t, md, l, cur, has, __FILE__, __LINE__, m)

/*** Opens a persistent cursor at either end of an index. */
inline void btr_pcur_open_at_index_side(
    bool from_left,      /*!< in: true if open to the low end,
                          false if to the high end */
    dict_index_t *index, /*!< in: index */
    ulint latch_mode,    /*!< in: latch mode */
    btr_pcur_t *pcur,    /*!< in: cursor */
    bool do_init,        /*!< in: true if should be initialized */
    mtr_t *mtr);         /*!< in: mtr */

/*** Gets the up_match value for a pcur after a search.
@return number of matched fields at the cursor or to the right if
search mode was PAGE_CUR_GE, otherwise undefined */
inline ulint btr_pcur_get_up_match(
    btr_pcur_t *cursor); /*!< in: memory buffer for persistent cursor */

/*** Gets the low_match value for a pcur after a search.
@return number of matched fields at the cursor or to the right if
search mode was PAGE_CUR_LE, otherwise undefined */
inline ulint btr_pcur_get_low_match(
    btr_pcur_t *cursor); /*!< in: memory buffer for persistent cursor */

/*** If mode is PAGE_CUR_G or PAGE_CUR_GE, opens a persistent cursor on the
first user record satisfying the search condition, in the case PAGE_CUR_L or
PAGE_CUR_LE, on the last user record. If no such user record exists, then
in the first case sets the cursor after last in tree, and in the latter case
before first in tree. The latching mode must be BTR_SEARCH_LEAF or
BTR_MODIFY_LEAF. */
void btr_pcur_open_on_user_rec_func(
    dict_index_t *index,   /*!< in: index */
    const dtuple_t *tuple, /*!< in: tuple on which search done */
    ib_srch_mode_t mode,   /*!< in: PAGE_CUR_L, ... */
    ulint latch_mode,      /*!< in: BTR_SEARCH_LEAF or
                           BTR_MODIFY_LEAF */
    btr_pcur_t *cursor,    /*!< in: memory buffer for persistent
                           cursor */
    const char *file,      /*!< in: file name */
    ulint line,            /*!< in: line where called */
    mtr_t *mtr);           /*!< in: mtr */

#define btr_pcur_open_on_user_rec(i, t, md, l, c, m) \
  btr_pcur_open_on_user_rec_func(i, t, md, l, c, __FILE__, __LINE__, m)

/*** Positions a cursor at a randomly chosen position within a B-tree. */
inline void
btr_pcur_open_at_rnd_pos_func(dict_index_t *index, /*!< in: index */
                              ulint latch_mode, /*!< in: BTR_SEARCH_LEAF, ... */
                              btr_pcur_t *cursor, /*!< in/out: B-tree pcur */
                              const char *file,   /*!< in: file name */
                              ulint line,         /*!< in: line where called */
                              mtr_t *mtr);        /*!< in: mtr */

#define btr_pcur_open_at_rnd_pos(i, l, c, m) \
  btr_pcur_open_at_rnd_pos_func(i, l, c, __FILE__, __LINE__, m)
/*** Frees the possible old_rec_buf buffer of a persistent cursor and sets the
latch mode of the persistent cursor to BTR_NO_LATCHES. */
inline void btr_pcur_close(btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** The position of the cursor is stored by taking an initial segment of the
record the cursor is positioned on, before, or after, and copying it to the
cursor data structure, or just setting a flag if the cursor id before the
first in an EMPTY tree, or after the last in an EMPTY tree. NOTE that the
page where the cursor is positioned must not be empty if the index tree is
not totally empty! */
void btr_pcur_store_position(btr_pcur_t *cursor, /*!< in: persistent cursor */
                             mtr_t *mtr);        /*!< in: mtr */

/*** Restores the stored position of a persistent cursor bufferfixing the page
and obtaining the specified latches. If the cursor position was saved when the
(1) cursor was positioned on a user record: this function restores the position
to the last record LESS OR EQUAL to the stored record;
(2) cursor was positioned on a page infimum record: restores the position to
the last record LESS than the user record which was the successor of the page
infimum;
(3) cursor was positioned on the page supremum: restores to the first record
GREATER than the user record which was the predecessor of the supremum.
(4) cursor was positioned before the first or after the last in an empty tree:
restores to before first or after the last in the tree.
@return true if the cursor position was stored when it was on a user
record and it can be restored on a user record whose ordering fields
are identical to the ones of the original user record */
bool btr_pcur_restore_position_func(
    ulint latch_mode,   /*!< in: BTR_SEARCH_LEAF, ... */
    btr_pcur_t *cursor, /*!< in: detached persistent cursor */
    const char *file,   /*!< in: file name */
    ulint line,         /*!< in: line where called */
    mtr_t *mtr);        /*!< in: mtr */

#define btr_pcur_restore_position(l, cur, mtr) \
  btr_pcur_restore_position_func(l, cur, __FILE__, __LINE__, mtr)

/*** If the latch mode of the cursor is BTR_LEAF_SEARCH or BTR_LEAF_MODIFY,
releases the page latch and bufferfix reserved by the cursor.
NOTE! In the case of BTR_LEAF_MODIFY, there should not exist changes
made by the current mini-transaction to the data protected by the
cursor latch, as then the latch must not be released until mtr_commit. */
void btr_pcur_release_leaf(btr_pcur_t *cursor, /*!< in: persistent cursor */
                           mtr_t *mtr);        /*!< in: mtr */

/*** Gets the rel_pos field for a cursor whose position has been stored.
@return	BTR_PCUR_ON, ... */
inline ulint
btr_pcur_get_rel_pos(const btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Sets the mtr field for a pcur. */
inline void btr_pcur_set_mtr(btr_pcur_t *cursor, /*!< in: persistent cursor */
                             mtr_t *mtr);        /*!< in, own: mtr */

/*** Gets the mtr field for a pcur.
@return	mtr */
inline mtr_t *
btr_pcur_get_mtr(btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Commits the mtr and sets the pcur latch mode to BTR_NO_LATCHES,
that is, the cursor becomes detached. If there have been modifications
to the page where pcur is positioned, this can be used instead of
btr_pcur_release_leaf. Function btr_pcur_store_position should be used
before calling this, if restoration of cursor is wanted later. */
inline void
btr_pcur_commit_specify_mtr(btr_pcur_t *pcur, /*!< in: persistent cursor */
                            mtr_t *mtr);      /*!< in: mtr to commit */

/*** Tests if a cursor is detached: that is the latch mode is BTR_NO_LATCHES.
@return	true if detached */
inline bool
btr_pcur_is_detached(btr_pcur_t *pcur); /*!< in: persistent cursor */

/*** Moves the persistent cursor to the next record in the tree. If no records
are left, the cursor stays 'after last in tree'.
@return	true if the cursor was not after last in tree */
inline bool btr_pcur_move_to_next(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the previous record in the tree. If no
records are left, the cursor stays 'before first in tree'.
@return	true if the cursor was not before first in tree */
inline bool btr_pcur_move_to_prev(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the last record on the same page. */
inline void
btr_pcur_move_to_last_on_page(btr_pcur_t *cursor, /*!< in: persistent cursor */
                              mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the next user record in the tree. If no user
records are left, the cursor ends up 'after last in tree'.
@return	true if the cursor moved forward, ending on a user record */
inline bool btr_pcur_move_to_next_user_rec(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the prev user record in the tree. If no user
records are left, the cursor ends up 'before first in tree'.
@return	true if the cursor moved backward, ending on a user record */
inline bool btr_pcur_move_to_prev_user_rec(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the first record on the next page.
Releases the latch on the current page, and bufferunfixes it.
Note that there must not be modifications on the current page,
as then the x-latch can be released only in mtr_commit. */
void btr_pcur_move_to_next_page(
    btr_pcur_t *cursor, /*!< in: persistent cursor; must be on the
                        last record of the current page */
    mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor backward if it is on the first record
of the page. Releases the latch on the current page, and bufferunfixes
it. Note that to prevent a possible deadlock, the operation first
stores the position of the cursor, releases the leaf latch, acquires
necessary latches and restores the cursor position again before returning.
The alphabetical position of the cursor is guaranteed to be sensible
on return, but it may happen that the cursor is not positioned on the
last record of any page, because the structure of the tree may have
changed while the cursor had no latches. */
void btr_pcur_move_backward_from_page(
    btr_pcur_t *cursor, /*!< in: persistent cursor, must be on the
                        first record of the current page */
    mtr_t *mtr);        /*!< in: mtr */

#ifdef UNIV_DEBUG
/*** Returns the btr cursor component of a persistent cursor.
@return	pointer to btr cursor component */
inline btr_cur_t *
btr_pcur_get_btr_cur(const btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Returns the page cursor component of a persistent cursor.
@return	pointer to page cursor component */
inline page_cur_t *
btr_pcur_get_page_cur(const btr_pcur_t *cursor); /*!< in: persistent cursor */
#else                                            /* UNIV_DEBUG */
#define btr_pcur_get_btr_cur(cursor) (&(cursor)->btr_cur) 
#define btr_pcur_get_page_cur(cursor) (&(cursor)->btr_cur.page_cur)
#endif /* UNIV_DEBUG */

/*** Returns the page of a persistent cursor.
@return	pointer to the page */
inline page_t *
btr_pcur_get_page(btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Returns the buffer block of a persistent cursor.
@return	pointer to the block */
inline buf_block_t *
btr_pcur_get_block(btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Returns the record of a persistent cursor.
@return	pointer to the record */
inline rec_t *
btr_pcur_get_rec(btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Checks if the persistent cursor is on a user record. */
inline bool
btr_pcur_is_on_user_rec(const btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Checks if the persistent cursor is after the last user record on
a page. */
inline bool btr_pcur_is_after_last_on_page(
    const btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Checks if the persistent cursor is before the first user record on
a page. */
inline bool btr_pcur_is_before_first_on_page(
    const btr_pcur_t *cursor); /*!< in: persistent cursor */

/*** Checks if the persistent cursor is before the first user record in
the index tree. */
inline bool btr_pcur_is_before_first_in_tree(
    btr_pcur_t *cursor, /*!< in: persistent cursor */
    mtr_t *mtr);        /*!< in: mtr */

/*** Checks if the persistent cursor is after the last user record in
the index tree. */
inline bool
btr_pcur_is_after_last_in_tree(btr_pcur_t *cursor, /*!< in: persistent cursor */
                               mtr_t *mtr);        /*!< in: mtr */

/*** Moves the persistent cursor to the next record on the same page. */
inline void btr_pcur_move_to_next_on_page(
    btr_pcur_t *cursor); /*!< in/out: persistent cursor */

/*** Moves the persistent cursor to the previous record on the same page. */
inline void btr_pcur_move_to_prev_on_page(
    btr_pcur_t *cursor); /*!< in/out: persistent cursor */

/* The persistent B-tree cursor structure. This is used mainly for SQL
selects, updates, and deletes. */
struct btr_pcur_struct {
  btr_cur_t btr_cur;              /*!< a B-tree cursor */
  ulint latch_mode;               /*!< see TODO note below!
                                  BTR_SEARCH_LEAF, BTR_MODIFY_LEAF,
                                  BTR_MODIFY_TREE, or BTR_NO_LATCHES,
                                  depending on the latching state of
                                  the page and tree where the cursor is
                                  positioned; the last value means that
                                  the cursor is not currently positioned:
                                  we say then that the cursor is
                                  detached; it can be restored to
                                  attached if the old position was
                                  stored in old_rec */
  ulint old_stored;               /*!< BTR_PCUR_OLD_STORED
                                  or BTR_PCUR_OLD_NOT_STORED */
  rec_t *old_rec;                 /*!< if cursor position is stored,
                                  contains an initial segment of the
                                  latest record cursor was positioned
                                  either on, before, or after */
  ulint old_n_fields;             /*!< number of fields in old_rec */
  ulint rel_pos;                  /*!< BTR_PCUR_ON, BTR_PCUR_BEFORE, or
                                  BTR_PCUR_AFTER, depending on whether
                                  cursor was on, before, or after the
                                  old_rec record */
  buf_block_t *block_when_stored; /* buffer block when the position was
                                stored */
  uint64_t modify_clock;          /*!< the modify clock value of the
                                     buffer block when the cursor position
                                     was stored */
  ulint pos_state;                /*!< see TODO note below!
                                  BTR_PCUR_IS_POSITIONED,
                                  BTR_PCUR_WAS_POSITIONED,
                                  BTR_PCUR_NOT_POSITIONED */
  ib_srch_mode_t search_mode;     /*!< PAGE_CUR_G, ... */
  trx_t *trx_if_known;            /*!< the transaction, if we know it;
                                  otherwise this field is not defined;
                                  can ONLY BE USED in error prints in
                                  fatal assertion failures! */
  /*-----------------------------*/
  /* NOTE that the following fields may possess dynamically allocated
  memory which should be freed if not needed anymore! */

  mtr_t *mtr;        /*!< NULL, or this field may contain
                     a mini-transaction which holds the
                     latch on the cursor page */
  byte *old_rec_buf; /*!< NULL, or a dynamically allocated
                     buffer for old_rec */
  ulint buf_size;    /*!< old_rec_buf size if old_rec_buf
                     is not NULL */
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

/** Gets the rel_pos field for a cursor whose position has been stored.
@return	BTR_PCUR_ON, ... */
inline ulint
btr_pcur_get_rel_pos(const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor);
  ut_ad(cursor->old_rec);
  ut_ad(cursor->old_stored == BTR_PCUR_OLD_STORED);
  ut_ad(cursor->pos_state == BTR_PCUR_WAS_POSITIONED ||
        cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return (cursor->rel_pos);
}

/** Sets the mtr field for a pcur. */
inline void btr_pcur_set_mtr(btr_pcur_t *cursor, /*!< in: persistent cursor */
                             mtr_t *mtr)         /*!< in, own: mtr */
{
  ut_ad(cursor);

  cursor->mtr = mtr;
}

/** Gets the mtr field for a pcur.
@return	mtr */
inline mtr_t *btr_pcur_get_mtr(btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor);

  return (cursor->mtr);
}

#ifdef UNIV_DEBUG
/** Returns the btr cursor component of a persistent cursor.
@return	pointer to btr cursor component */
inline btr_cur_t *
btr_pcur_get_btr_cur(const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  const btr_cur_t *btr_cur = &cursor->btr_cur;
  return ((btr_cur_t *)btr_cur);
}

/** Returns the page cursor component of a persistent cursor.
@return	pointer to page cursor component */
inline page_cur_t *
btr_pcur_get_page_cur(const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  return (btr_cur_get_page_cur(btr_pcur_get_btr_cur(cursor)));
}

#endif /* UNIV_DEBUG */

/** Returns the page of a persistent cursor.
@return	pointer to the page */
inline page_t *
btr_pcur_get_page(btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return (btr_cur_get_page(btr_pcur_get_btr_cur(cursor)));
}

/** Returns the buffer block of a persistent cursor.
@return	pointer to the block */
inline buf_block_t *
btr_pcur_get_block(btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);

  return (btr_cur_get_block(btr_pcur_get_btr_cur(cursor)));
}

/** Returns the record of a persistent cursor.
@return	pointer to the record */
inline rec_t *btr_pcur_get_rec(btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return (btr_cur_get_rec(btr_pcur_get_btr_cur(cursor)));
}

/** Gets the up_match value for a pcur after a search.
@return number of matched fields at the cursor or to the right if
search mode was PAGE_CUR_GE, otherwise undefined */
inline ulint btr_pcur_get_up_match(
    btr_pcur_t *cursor) /*!< in: memory buffer for persistent cursor */
{
  btr_cur_t *btr_cursor;

  ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) ||
        (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

  btr_cursor = btr_pcur_get_btr_cur(cursor);

  ut_ad(btr_cursor->up_match != ULINT_UNDEFINED);

  return (btr_cursor->up_match);
}

/** Gets the low_match value for a pcur after a search.
@return number of matched fields at the cursor or to the right if
search mode was PAGE_CUR_LE, otherwise undefined */
inline ulint btr_pcur_get_low_match(
    btr_pcur_t *cursor) /*!< in: memory buffer for persistent cursor */
{
  btr_cur_t *btr_cursor;

  ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) ||
        (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

  btr_cursor = btr_pcur_get_btr_cur(cursor);
  ut_ad(btr_cursor->low_match != ULINT_UNDEFINED);

  return (btr_cursor->low_match);
}

/** Checks if the persistent cursor is after the last user record on
a page. */
inline bool btr_pcur_is_after_last_on_page(
    const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return (page_cur_is_after_last(btr_pcur_get_page_cur(cursor)));
}

/** Checks if the persistent cursor is before the first user record on
a page. */
inline bool btr_pcur_is_before_first_on_page(
    const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  return (page_cur_is_before_first(btr_pcur_get_page_cur(cursor)));
}

/** Checks if the persistent cursor is on a user record. */
inline bool
btr_pcur_is_on_user_rec(const btr_pcur_t *cursor) /*!< in: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_pcur_is_before_first_on_page(cursor) ||
      btr_pcur_is_after_last_on_page(cursor)) {

    return (false);
  }

  return (true);
}

/** Checks if the persistent cursor is before the first user record in
the index tree. */
inline bool btr_pcur_is_before_first_in_tree(
    btr_pcur_t *cursor, /*!< in: persistent cursor */
    mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_prev(btr_pcur_get_page(cursor), mtr) != FIL_NULL) {

    return (false);
  }

  return (page_cur_is_before_first(btr_pcur_get_page_cur(cursor)));
}

/** Checks if the persistent cursor is after the last user record in
the index tree. */
inline bool
btr_pcur_is_after_last_in_tree(btr_pcur_t *cursor, /*!< in: persistent cursor */
                               mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  if (btr_page_get_next(btr_pcur_get_page(cursor), mtr) != FIL_NULL) {

    return (false);
  }

  return (page_cur_is_after_last(btr_pcur_get_page_cur(cursor)));
}

/** Moves the persistent cursor to the next record on the same page. */
inline void btr_pcur_move_to_next_on_page(
    btr_pcur_t *cursor) /*!< in/out: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_next(btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/** Moves the persistent cursor to the previous record on the same page. */
inline void btr_pcur_move_to_prev_on_page(
    btr_pcur_t *cursor) /*!< in/out: persistent cursor */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_move_to_prev(btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/** Moves the persistent cursor to the last record on the same page. */
inline void
btr_pcur_move_to_last_on_page(btr_pcur_t *cursor, /*!< in: persistent cursor */
                              mtr_t *mtr)         /*!< in: mtr */
{
  UT_NOT_USED(mtr);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  page_cur_set_after_last(btr_pcur_get_block(cursor),
                          btr_pcur_get_page_cur(cursor));

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/** Moves the persistent cursor to the next user record in the tree. If no user
records are left, the cursor ends up 'after last in tree'.
@return	true if the cursor moved forward, ending on a user record */
inline bool btr_pcur_move_to_next_user_rec(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
loop:
  if (btr_pcur_is_after_last_on_page(cursor)) {

    if (btr_pcur_is_after_last_in_tree(cursor, mtr)) {

      return (false);
    }

    btr_pcur_move_to_next_page(cursor, mtr);
  } else {
    btr_pcur_move_to_next_on_page(cursor);
  }

  if (btr_pcur_is_on_user_rec(cursor)) {

    return (true);
  }

  goto loop;
}

/** Moves the persistent cursor to the prev user record in the tree. If no user
records are left, the cursor ends up 'before first in tree'.
@return	true if the cursor moved backward, ending on a user record */
inline bool btr_pcur_move_to_prev_user_rec(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
loop:
  if (btr_pcur_is_before_first_on_page(cursor)) {

    if (btr_pcur_is_before_first_in_tree(cursor, mtr)) {

      return (false);
    }

    btr_pcur_move_backward_from_page(cursor, mtr);
  } else {
    btr_pcur_move_to_prev_on_page(cursor);
  }

  if (btr_pcur_is_on_user_rec(cursor)) {

    return (true);
  }

  goto loop;
}

/** Moves the persistent cursor to the next record in the tree. If no records
are left, the cursor stays 'after last in tree'.
@return	true if the cursor was not after last in tree */
inline bool btr_pcur_move_to_next(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  if (btr_pcur_is_after_last_on_page(cursor)) {

    if (btr_pcur_is_after_last_in_tree(cursor, mtr)) {

      return (false);
    }

    btr_pcur_move_to_next_page(cursor, mtr);

    return (true);
  }

  btr_pcur_move_to_next_on_page(cursor);

  return (true);
}

/** Moves the persistent cursor to the previous record in the tree. If no
records are left, the cursor stays 'before first in tree'.
@return	true if the cursor was not before first in tree */
inline bool btr_pcur_move_to_prev(
    btr_pcur_t *cursor, /*!< in: persistent cursor; NOTE that the
                        function may release the page latch */
    mtr_t *mtr)         /*!< in: mtr */
{
  ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  if (btr_pcur_is_before_first_on_page(cursor)) {

    if (btr_pcur_is_before_first_in_tree(cursor, mtr)) {

      return (false);
    }

    btr_pcur_move_backward_from_page(cursor, mtr);

    return (true);
  }

  btr_pcur_move_to_prev_on_page(cursor);

  return (true);
}

/** Commits the mtr and sets the pcur latch mode to BTR_NO_LATCHES,
that is, the cursor becomes detached. If there have been modifications
to the page where pcur is positioned, this can be used instead of
btr_pcur_release_leaf. Function btr_pcur_store_position should be used
before calling this, if restoration of cursor is wanted later. */
inline void
btr_pcur_commit_specify_mtr(btr_pcur_t *pcur, /*!< in: persistent cursor */
                            mtr_t *mtr)       /*!< in: mtr to commit */
{
  ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

  pcur->latch_mode = BTR_NO_LATCHES;

  mtr_commit(mtr);

  pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}

/** Sets the pcur latch mode to BTR_NO_LATCHES. */
inline void btr_pcur_detach(btr_pcur_t *pcur) /*!< in: persistent cursor */
{
  ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

  pcur->latch_mode = BTR_NO_LATCHES;

  pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}

/** Tests if a cursor is detached: that is the latch mode is BTR_NO_LATCHES.
@return	true if detached */
inline bool btr_pcur_is_detached(btr_pcur_t *pcur) /*!< in: persistent cursor */
{
  if (pcur->latch_mode == BTR_NO_LATCHES) {

    return (true);
  }

  return (false);
}

/** Sets the old_rec_buf field to NULL. */
inline void btr_pcur_init(btr_pcur_t *pcur) /*!< in: persistent cursor */
{
  pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;
  pcur->old_rec_buf = NULL;
  pcur->old_rec = NULL;
}

/** Initializes and opens a persistent cursor to an index tree. It should be
closed with btr_pcur_close. */
inline void btr_pcur_open_func(
    dict_index_t *dict_index, /*!< in: dict_index */
    const dtuple_t *tuple,    /*!< in: tuple on which search done */
    ib_srch_mode_t mode,      /*!< in: PAGE_CUR_L, ...;
                     NOTE that if the search is made using a unique
                     prefix of a record, mode should be
                     PAGE_CUR_LE, not PAGE_CUR_GE, as the latter
                     may end up on the previous page from the
                     record! */
    ulint latch_mode,         /*!< in: BTR_SEARCH_LEAF, ... */
    btr_pcur_t *cursor,       /*!< in: memory buffer for persistent cursor */
    const char *file,         /*!< in: file name */
    ulint line,               /*!< in: line where called */
    mtr_t *mtr)               /*!< in: mtr */
{
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

/** Opens an persistent cursor to an index tree without initializing the
cursor. */
inline void btr_pcur_open_with_no_init_func(
    dict_index_t *dict_index, /*!< in: dict_index */
    const dtuple_t *tuple,    /*!< in: tuple on which search done */
    ib_srch_mode_t mode,      /*!< in: PAGE_CUR_L, ...;
                     NOTE that if the search is made using a unique
                     prefix of a record, mode should be
                     PAGE_CUR_LE, not PAGE_CUR_GE, as the latter
                     may end up on the previous page of the
                     record! */
    ulint latch_mode,         /*!< in: BTR_SEARCH_LEAF, ...;
                           NOTE that if has_search_latch != 0 then
                           we maybe do not acquire a latch on the cursor
                           page, but assume that the caller uses his
                           btr search latch to protect the record! */
    btr_pcur_t *cursor,       /*!< in: memory buffer for persistent cursor */
    ulint has_search_latch,   /*!< in: latch mode the caller
                     currently has on btr_search_latch:
                     RW_S_LATCH, or 0 */
    const char *file,         /*!< in: file name */
    ulint line,               /*!< in: line where called */
    mtr_t *mtr)               /*!< in: mtr */
{
  btr_cur_t *btr_cursor;

  cursor->latch_mode = latch_mode;
  cursor->search_mode = mode;

  /* Search with the tree cursor */

  btr_cursor = btr_pcur_get_btr_cur(cursor);

  btr_cur_search_to_nth_level(dict_index, 0, tuple, mode, latch_mode,
                              btr_cursor, has_search_latch, file, line, mtr);
  cursor->pos_state = BTR_PCUR_IS_POSITIONED;

  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  cursor->trx_if_known = NULL;
}

/** Opens a persistent cursor at either end of an index. */
inline void btr_pcur_open_at_index_side(
    bool from_left,           /*!< in: true if open to the low end,
                               false if to the high end */
    dict_index_t *dict_index, /*!< in: dict_index */
    ulint latch_mode,         /*!< in: latch mode */
    btr_pcur_t *pcur,         /*!< in: cursor */
    bool do_init,             /*!< in: true if should be initialized */
    mtr_t *mtr)               /*!< in: mtr */
{
  pcur->latch_mode = latch_mode;

  if (from_left) {
    pcur->search_mode = PAGE_CUR_G;
  } else {
    pcur->search_mode = PAGE_CUR_L;
  }

  if (do_init) {
    btr_pcur_init(pcur);
  }

  btr_cur_open_at_index_side(from_left, dict_index, latch_mode,
                             btr_pcur_get_btr_cur(pcur), mtr);
  pcur->pos_state = BTR_PCUR_IS_POSITIONED;

  pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;

  pcur->trx_if_known = NULL;
}

/** Positions a cursor at a randomly chosen position within a B-tree. */
inline void
btr_pcur_open_at_rnd_pos_func(dict_index_t *dict_index, /*!< in: dict_index */
                              ulint latch_mode, /*!< in: BTR_SEARCH_LEAF, ... */
                              btr_pcur_t *cursor, /*!< in/out: B-tree pcur */
                              const char *file,   /*!< in: file name */
                              ulint line,         /*!< in: line where called */
                              mtr_t *mtr)         /*!< in: mtr */
{
  /* Initialize the cursor */

  cursor->latch_mode = latch_mode;
  cursor->search_mode = PAGE_CUR_G;

  btr_pcur_init(cursor);

  btr_cur_open_at_rnd_pos_func(dict_index, latch_mode,
                               btr_pcur_get_btr_cur(cursor), file, line, mtr);
  cursor->pos_state = BTR_PCUR_IS_POSITIONED;
  cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

  cursor->trx_if_known = NULL;
}

/** Frees the possible memory heap of a persistent cursor and sets the latch
mode of the persistent cursor to BTR_NO_LATCHES. */
inline void btr_pcur_close(btr_pcur_t *cursor) /*!< in: persistent cursor */
{
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
