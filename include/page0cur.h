/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/page0cur.h
The page cursor

Created 10/4/1994 Heikki Tuuri
*************************************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "data0data.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "rem0rec.h"

#define PAGE_CUR_ADAPT

/* Page cursor search modes; the values must be in this order! */

constexpr auto PAGE_CUR_UNSUPP = IB_CUR_UNSUPP;
constexpr auto PAGE_CUR_G = IB_CUR_G;
constexpr auto PAGE_CUR_GE = IB_CUR_GE;
constexpr auto PAGE_CUR_L = IB_CUR_L;
constexpr auto PAGE_CUR_LE = IB_CUR_LE;

/* This is a search mode used in:
 "column LIKE 'abc%' ORDER BY column DESC";
 we have to find strings which are <= 'abc' or which extend it */
/* constexpr auto PAGE_CUR_LE_OR_EXTENDS = 5; * */

#ifdef UNIV_SEARCH_DEBUG
/** As PAGE_CUR_LE, but skips search shortcut */
constexpr auto PAGE_CUR_DBG = 6;
#endif /* UNIV_SEARCH_DEBUG */

/**
 * @brief Inserts a record next to page cursor on an uncompressed page.
 * Returns pointer to inserted record if succeed, i.e., enough
 * space available, NULL otherwise. The cursor stays at the same position.
 * 
 * @param current_rec Pointer to current record after which the new record is inserted.
 * @param index Record descriptor.
 * @param rec Pointer to a physical record.
 * @param offsets In/out: Phy_rec::get_col_offsets(rec, index).
 * @param mtr Mini-transaction handle, or NULL.
 * @return Pointer to record if succeed, NULL otherwise.
 */
rec_t *page_cur_insert_rec_low(rec_t *current_rec, dict_index_t *index, const rec_t *rec, ulint *offsets, mtr_t *mtr);

/**
 * @brief Copies records from page to a newly created page, from a given record
 * onward, including that record. Infimum and supremum records are not copied.
 * 
 * @param new_page In/out: Index page to copy to.
 * @param rec First record to copy.
 * @param index Record descriptor.
 * @param mtr MTR.
 */
void page_copy_rec_list_end_to_created_page(page_t *new_page, rec_t *rec, dict_index_t *index, mtr_t *mtr);

/**
 * @brief Deletes a record at the page cursor. The cursor is moved to the
 * next record after the deleted one.
 * 
 * @param cursor In/out: A page cursor.
 * @param index Record descriptor.
 * @param offsets In: Phy_rec::get_col_offsets(cursor->rec, index).
 * @param mtr Mini-transaction handle.
 */
void page_cur_delete_rec(page_cur_t *cursor, dict_index_t *index, const ulint *offsets, mtr_t *mtr);

/**
 * @brief Searches the right position for a page cursor.
 * 
 * @param block In: Buffer block.
 * @param index In: Record descriptor.
 * @param tuple In: Data tuple.
 * @param mode In: PAGE_CUR_L, PAGE_CUR_LE, PAGE_CUR_G, or PAGE_CUR_GE.
 * @param iup_matched_fields In/out: Already matched fields in upper limit record.
 * @param iup_matched_bytes In/out: Already matched bytes in a field not yet completely matched.
 * @param ilow_matched_fields In/out: Already matched fields in lower limit record.
 * @param ilow_matched_bytes In/out: Already matched bytes in a field not yet completely matched.
 * @param cursor Out: Page cursor.
 */
void page_cur_search_with_match(
  const buf_block_t *block, const dict_index_t *index, const dtuple_t *tuple, ulint mode, ulint *iup_matched_fields,
  ulint *iup_matched_bytes, ulint *ilow_matched_fields, ulint *ilow_matched_bytes, page_cur_t *cursor
);

/**
 * @brief Positions a page cursor on a randomly chosen user record on a page. If there
 * are no user records, sets the cursor on the infimum record.
 * 
 * @param block In: Page.
 * @param cursor Out: Page cursor.
 */
void page_cur_open_on_rnd_user_rec(buf_block_t *block, page_cur_t *cursor);

/**
 * @brief Parses a log record of a record insert on a page.
 * 
 * @param is_short In: True if short inserts.
 * @param ptr In: Buffer.
 * @param end_ptr In: Buffer end.
 * @param block In: Page or NULL.
 * @param index In: Record descriptor.
 * @param mtr In: MTR or NULL.
 * @return End of log record or NULL.
 */
byte *page_cur_parse_insert_rec(bool is_short, byte *ptr, byte *end_ptr, buf_block_t *block, dict_index_t *index, mtr_t *mtr);

/**
 * @brief Parses a log record of copying a record list end to a new created page.
 * 
 * @param ptr In: Buffer.
 * @param end_ptr In: Buffer end.
 * @param block In: Page or NULL.
 * @param index In: Record descriptor.
 * @param mtr In: MTR or NULL.
 * @return End of log record or NULL.
 */
byte *page_parse_copy_rec_list_to_created_page(byte *ptr, byte *end_ptr, buf_block_t *block, dict_index_t *index, mtr_t *mtr);

/**
 * @brief Parses log record of a record delete on a page.
 * 
 * @param ptr In: Buffer.
 * @param end_ptr In: Buffer end.
 * @param block In: Page or NULL.
 * @param index In: Record descriptor.
 * @param mtr In: MTR or NULL.
 * @return Pointer to record end or NULL.
 */
byte *page_cur_parse_delete_rec(byte *ptr, byte *end_ptr, buf_block_t *block, dict_index_t *index, mtr_t *mtr);

/** Index page cursor */
struct page_cur_t {
  /** Index the cursor is on. */
  const dict_index_t *m_index{};

  /** pointer to a record on page */
  rec_t *m_rec{};

  /** Current offsets of the record. */
  ulint *m_offsets{};

  /** Pointer to the current block containing rec. */
  buf_block_t *m_block{};
};

/**
 * @brief Gets pointer to the page frame where the cursor is positioned.
 * 
 * @param cur In: Page cursor.
 * @return Page.
 */
inline page_t *page_cur_get_page(page_cur_t *cur) {
  ut_ad(page_align(cur->m_rec) == cur->m_block->get_frame());

  return page_align(cur->m_rec);
}

/**
 * @brief Gets pointer to the buffer block where the cursor is positioned.
 * 
 * @param cur In: Page cursor.
 * @return Buffer block.
 */
inline buf_block_t *page_cur_get_block(page_cur_t *cur) {
  ut_ad(page_align(cur->m_rec) == cur->m_block->get_frame());

  return cur->m_block;
}

/**
 * @brief Gets the record where the cursor is positioned.
 * 
 * @param cur In: Page cursor.
 * @return Record.
 */
inline rec_t *page_cur_get_rec(page_cur_t *cur) {
  ut_ad(page_align(cur->m_rec) == cur->m_block->get_frame());

  return cur->m_rec;
}

/**
 * @brief Sets the cursor object to point before the first user record on the page.
 * 
 * @param block In: Index page.
 * @param cur In: Cursor.
 */
inline void page_cur_set_before_first(buf_block_t *block, page_cur_t *cur) {
  cur->m_block = block;
  cur->m_rec = page_get_infimum_rec(cur->m_block->get_frame());
}

/**
 * @brief Sets the cursor object to point before the first user record on the page.
 * 
 * @param block In: Index page.
 * @param cur In: Cursor.
 */
inline void page_cur_set_before_first(const buf_block_t *block, page_cur_t *cur) {
  page_cur_set_before_first(const_cast<buf_block_t *>(block), cur);
}

/**
 * @brief Sets the cursor object to point after the last user record on the page.
 * 
 * @param block In: Index page.
 * @param cur In: Cursor.
 */
inline void page_cur_set_after_last(buf_block_t *block, page_cur_t *cur) {
  cur->m_block = block;
  cur->m_rec = page_get_supremum_rec(cur->m_block->get_frame());
}

/**
 * @brief Sets the cursor object to point after the last user record on the page.
 * 
 * @param block In: Index page.
 * @param cur In: Cursor.
 */
inline void page_cur_set_after_last(const buf_block_t *block, page_cur_t *cur) {
  cur->m_block = const_cast<buf_block_t *>(block);
  cur->m_rec = page_get_supremum_rec(cur->m_block->get_frame());
}

/**
 * @brief Returns true if the cursor is before the first user record on the page.
 * 
 * @param cur In: Cursor.
 * @return True if at start.
 */
inline bool page_cur_is_before_first(const page_cur_t *cur) {
  ut_ad(page_align(cur->m_rec) == cur->m_block->get_frame());
  return page_rec_is_infimum(cur->m_rec);
}

/**
 * @brief Returns true if the cursor is after the last user record.
 * 
 * @param cur In: Cursor.
 * @return True if at end.
 */
inline bool page_cur_is_after_last(const page_cur_t *cur) {
  ut_ad(page_align(cur->m_rec) == cur->m_block->get_frame());
  return page_rec_is_supremum(cur->m_rec);
}

/**
 * @brief Positions the cursor on the given record.
 * 
 * @param rec In: Record on a page.
 * @param block In: Buffer block containing the record.
 * @param cur Out: Page cursor.
 */
inline void page_cur_position(const rec_t *rec, const buf_block_t *block, page_cur_t *cur) {
  ut_ad(page_align(rec) == block->get_frame());

  cur->m_rec = const_cast<rec_t *>(rec);
  cur->m_block = const_cast<buf_block_t *>(block);
}

/**
 * @brief Invalidates a page cursor by setting the record pointer NULL.
 * 
 * @param cur Out: Page cursor.
 */
inline void page_cur_invalidate(page_cur_t *cur) {
  cur->m_rec = nullptr;
  cur->m_block = nullptr;
}

/**
 * @brief Moves the cursor to the next record on the page.
 * 
 * @param cur In/Out: Cursor; must not be after last.
 */
inline void page_cur_move_to_next(page_cur_t *cur) {
  ut_ad(!page_cur_is_after_last(cur));

  cur->m_rec = page_rec_get_next(cur->m_rec);
}

/**
 * @brief Moves the cursor to the previous record on the page.
 * 
 * @param cur In/Out: Cursor; must not be before the first.
 */
inline void page_cur_move_to_prev(page_cur_t *cur) {
  ut_ad(!page_cur_is_before_first(cur));

  cur->m_rec = page_rec_get_prev(cur->m_rec);
}

/**
 * @brief Searches the right position for a page cursor.
 * 
 * @param block In: Buffer block.
 * @param dict_index In: Record descriptor.
 * @param tuple In: Data tuple.
 * @param mode In: Search mode (PAGE_CUR_L, PAGE_CUR_LE, PAGE_CUR_G, or PAGE_CUR_GE).
 * @param cursor Out: Page cursor.
 * @return Number of matched fields on the left.
 */
inline ulint page_cur_search(
  const buf_block_t *block, const dict_index_t *dict_index, const dtuple_t *tuple, ulint mode, page_cur_t *cursor
) {
  ulint low_matched_fields = 0;
  ulint low_matched_bytes = 0;
  ulint up_matched_fields = 0;
  ulint up_matched_bytes = 0;

  ut_ad(dtuple_check_typed(tuple));

  page_cur_search_with_match(
    block, dict_index, tuple, mode, &up_matched_fields, &up_matched_bytes, &low_matched_fields, &low_matched_bytes, cursor
  );

  return low_matched_fields;
}

/**
 * @brief Inserts a record next to the page cursor.
 * Returns a pointer to the inserted record if successful (i.e., enough space available),
 * NULL otherwise. The cursor stays at the same logical position, but the physical position
 * may change if it is pointing to a compressed page that was reorganized.
 * 
 * @param cursor In/Out: Page cursor.
 * @param tuple In: Pointer to a data tuple.
 * @param dict_index In: Record descriptor.
 * @param n_ext In: Number of externally stored columns.
 * @param mtr In: Mini-transaction handle, or NULL.
 * @return Pointer to the inserted record if successful, NULL otherwise.
 */
inline rec_t *page_cur_tuple_insert(page_cur_t *cursor, const dtuple_t *tuple, dict_index_t *dict_index, ulint n_ext, mtr_t *mtr) {
  const auto size = rec_get_converted_size(dict_index, tuple, n_ext);
  auto heap = mem_heap_create(size + (4 + REC_OFFS_HEADER_SIZE + dtuple_get_n_fields(tuple)) * sizeof(ulint));
  auto rec = rec_convert_dtuple_to_rec((byte *)mem_heap_alloc(heap, size), dict_index, tuple, n_ext);

  {
    Phy_rec record(dict_index, rec);

    auto offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Source_location{});

    rec = page_cur_insert_rec_low(cursor->m_rec, dict_index, rec, offsets, mtr);
  }

  mem_heap_free(heap);

  return rec;
}

/**
 * @brief Inserts a record next to the page cursor.
 * Returns a pointer to the inserted record if successful (i.e., enough space available),
 * NULL otherwise. The cursor stays at the same logical position, but the physical position
 * may change if it is pointing to a compressed page that was reorganized.
 * 
 * @param cursor In/Out: Page cursor.
 * @param rec In: Record to insert.
 * @param dict_index In: Record descriptor.
 * @param offsets In/Out: Offsets of the record.
 * @param mtr In: Mini-transaction handle, or NULL.
 * @return Pointer to the inserted record if successful, NULL otherwise.
 */
inline rec_t *page_cur_rec_insert(page_cur_t *cursor, const rec_t *rec, dict_index_t *dict_index, ulint *offsets, mtr_t *mtr) {
  return page_cur_insert_rec_low(cursor->m_rec, dict_index, rec, offsets, mtr);
}
