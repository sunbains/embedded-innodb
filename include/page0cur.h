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

#include "buf0types.h"
#include "data0data.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "rem0rec.h"

// Forward declarations
struct dtuple_t;

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

/** Index page cursor */
struct Page_cursor {

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
  Rec insert_rec_low(Rec current_rec, const Index *index, const Rec rec, ulint *offsets, mtr_t *mtr);

  /**
   * @brief Copies records from page to a newly created page, from a given record
   * onward, including that record. Infimum and supremum records are not copied.
   *
   * @param new_page In/out: Index page to copy to.
   * @param rec First record to copy.
   * @param index Record descriptor.
   * @param mtr MTR.
   */
  static void copy_rec_list_end_to_created_page(page_t *new_page, Rec rec, const Index *index, mtr_t *mtr);

  /**
   * @brief Deletes a record at the page cursor. The cursor is moved to the
   * next record after the deleted one.
   *
   * @param index Record descriptor.
   * @param offsets In: Phy_rec::get_col_offsets(rec, index).
   * @param mtr Mini-transaction handle.
   */
  void delete_rec(const Index *index, const ulint *offsets, mtr_t *mtr);

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
   */
  void search_with_match(
    const Buf_block *block, const Index *index, const DTuple *tuple, ulint mode, ulint *iup_matched_fields,
    ulint *iup_matched_bytes, ulint *ilow_matched_fields, ulint *ilow_matched_bytes
  );

  /**
   * @brief Positions this page cursor on a randomly chosen user record on a page. If there
   * are no user records, sets the cursor on the infimum record.
   *
   * @param block In: Page.
   */
  void open_on_rnd_user_rec(Buf_block *block);

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
  static byte *parse_insert_rec(bool is_short, byte *ptr, byte *end_ptr, Buf_block *block, Index *index, mtr_t *mtr);

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
  static byte *parse_copy_rec_list_to_created_page(byte *ptr, byte *end_ptr, Buf_block *block, Index *index, mtr_t *mtr);

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
  static byte *parse_delete_rec(byte *ptr, byte *end_ptr, Buf_block *block, Index *index, mtr_t *mtr);

  /**
   * @brief Gets pointer to the page frame where the cursor is positioned.
   *
   * @return Page.
   */
  inline page_t *get_page() {
    ut_ad(m_rec.page_align() == m_block->get_frame());

    return m_rec.page_align();
  }

  /**
   * @brief Gets pointer to the buffer block where the cursor is positioned.
   *
   * @return Buffer block.
   */
  inline Buf_block *get_block() {
    ut_ad(m_rec.page_align() == m_block->get_frame());

    return m_block;
  }

  /**
   * @brief Gets the record where the cursor is positioned.
   *
   * @return Record.
   */
  inline Rec get_rec() {
    ut_ad(m_rec.page_align() == m_block->get_frame());

    return m_rec;
  }

  /**
   * @brief Sets the cursor object to point before the first user record on the page.
   *
   * @param block In: Index page.
   */
  inline void set_before_first(Buf_block *block) {
    m_block = block;
    m_rec = page_get_infimum_rec(m_block->get_frame());
  }

  /**
   * @brief Sets the cursor object to point before the first user record on the page.
   *
   * @param block In: Index page.
   */
  inline void set_before_first(const Buf_block *block) { set_before_first(const_cast<Buf_block *>(block)); }

  /**
   * @brief Sets the cursor object to point after the last user record on the page.
   *
   * @param block In: Index page.
   */
  inline void set_after_last(Buf_block *block) {
    m_block = block;
    m_rec = page_get_supremum_rec(m_block->get_frame());
  }

  /**
   * @brief Sets the cursor object to point after the last user record on the page.
   *
   * @param block In: Index page.
   */
  inline void set_after_last(const Buf_block *block) { set_after_last(const_cast<Buf_block *>(block)); }

  /**
   * @brief Returns true if the cursor is before the first user record on the page.
   *
   * @return True if at start.
   */
  inline bool is_before_first() const {
    ut_ad(m_rec.page_align() == m_block->get_frame());
    return page_rec_is_infimum(m_rec);
  }

  /**
   * @brief Returns true if the cursor is after the last user record.
   *
   * @return True if at end.
   */
  inline bool is_after_last() const {
    ut_ad(m_rec.page_align() == m_block->get_frame());
    return page_rec_is_supremum(m_rec);
  }

  /**
   * @brief Positions the cursor on the given record.
   *
   * @param rec In: Record on a page.
   * @param block In: Buffer block containing the record.
   */
  inline void position(const Rec rec, const Buf_block *block) {
    ut_ad(rec.page_align() == block->get_frame());

    m_rec = rec;
    m_block = const_cast<Buf_block *>(block);
  }

  /**
   * @brief Invalidates a page cursor by setting the record pointer NULL.
   *
   */
  inline void invalidate() {
    m_rec = nullptr;
    m_block = nullptr;
  }

  /**
   * @brief Moves the cursor to the next record on the page.
   *
   * @param cur In/Out: Cursor; must not be after last.
   */
  inline void move_to_next() {
    ut_ad(!is_after_last());

    m_rec = page_rec_get_next(m_rec);
  }

  /**
   * @brief Moves the cursor to the previous record on the page.
   *
   * @param cur In/Out: Cursor; must not be before the first.
   */
  inline void move_to_prev() {
    ut_ad(!is_before_first());

    m_rec = page_rec_get_prev(m_rec);
  }

  /**
   * @brief Searches the right position for a page cursor.
   *
   * @param block In: Buffer block.
   * @param index In: Record descriptor.
   * @param tuple In: Data tuple.
   * @param mode In: Search mode (PAGE_CUR_L, PAGE_CUR_LE, PAGE_CUR_G, or PAGE_CUR_GE).
   * @return Number of matched fields on the left.
   */
  inline ulint search(const Buf_block *block, const Index *index, const DTuple *tuple, ulint mode) {
    ulint low_matched_fields{};
    ulint low_matched_bytes{};
    ulint up_matched_fields{};
    ulint up_matched_bytes{};

    ut_ad(dtuple_check_typed(tuple));

    search_with_match(block, index, tuple, mode, &up_matched_fields, &up_matched_bytes, &low_matched_fields, &low_matched_bytes);

    return low_matched_fields;
  }

  /**
   * @brief Inserts a record next to the page cursor.
   * Returns a pointer to the inserted record if successful (i.e., enough space available),
   * NULL otherwise. The cursor stays at the same logical position, but the physical position
   * may change if it is pointing to a compressed page that was reorganized.
   *
   * @param tuple In: Pointer to a data tuple.
   * @param index In: Record descriptor.
   * @param n_ext In: Number of externally stored columns.
   * @param mtr In: Mini-transaction handle, or NULL.
   * @return Pointer to the inserted record if successful, NULL otherwise.
   */
  inline Rec tuple_insert(const DTuple *tuple, const Index *index, ulint n_ext, mtr_t *mtr) {
    const auto size = rec_get_converted_size(index, tuple, n_ext);
    auto heap = mem_heap_create(size + (4 + REC_OFFS_HEADER_SIZE + dtuple_get_n_fields(tuple)) * sizeof(ulint));
    auto rec = Rec::convert_dtuple_to_rec((byte *)mem_heap_alloc(heap, size), index, tuple, n_ext);

    {
      Phy_rec record(index, rec);

      auto offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &heap, Current_location());

      rec = insert_rec_low(m_rec, index, rec, offsets, mtr);
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
   * @param rec In: Record to insert.
   * @param index In: Record descriptor.
   * @param offsets In/Out: Offsets of the record.
   * @param mtr In: Mini-transaction handle, or NULL.
   * @return Pointer to the inserted record if successful, NULL otherwise.
   */
  inline Rec rec_insert(const Rec rec, Index *index, ulint *offsets, mtr_t *mtr) {
    return insert_rec_low(m_rec, index, rec, offsets, mtr);
  }

 private:
  /**
   * @brief Linear congruential generator PRNG.
   *
   * Returns a pseudo random number between 0 and 2^64-1 inclusive.
   * The formula and the constants being used are:
   * X[n+1] = (a * X[n] + c) mod m
   * where:
   * - X[0] = ut_time_us(nullptr)
   * - a = 1103515245 (3^5 * 5 * 7 * 129749)
   * - c = 12345 (3 * 5 * 823)
   * - m = 18446744073709551616 (2^64)
   *
   * @return Number between 0 and 2^64-1
   */
  static uint64_t lcg_prng();

  /**
   * @brief Tries a search shortcut based on the last insert.
   *
   * This function attempts to optimize the search by using information
   * about the last insert operation to quickly position the cursor.
   *
   * @param[in] block Index page
   * @param[in] index Record descriptor
   * @param[in] tuple Data tuple
   * @param[in,out] iup_matched_fields Already matched fields in upper limit record
   * @param[in,out] iup_matched_bytes Already matched bytes in a field not yet completely matched
   * @param[in,out] ilow_matched_fields Already matched fields in lower limit record
   * @param[in,out] ilow_matched_bytes Already matched bytes in a field not yet completely matched
   * @return true on success
   */
  bool try_search_shortcut(
    const Buf_block *block, const Index *index, const DTuple *tuple, ulint *iup_matched_fields, ulint *iup_matched_bytes,
    ulint *ilow_matched_fields, ulint *ilow_matched_bytes
  );

  /**
   * @brief Checks if the nth field in a record is a character type field which extends
   * the nth field in tuple.
   *
   * A field extends another if it is longer or equal in length and has
   * common first characters.
   *
   * @param[in] tuple Data tuple
   * @param[in] rec Record
   * @param[in] offsets Array returned by Phy_rec::get_col_offsets()
   * @param[in] n Compare nth field
   * @return true if rec field extends tuple field
   */
  bool rec_field_extends(const dtuple_t *tuple, const Rec rec, const ulint *offsets, ulint n);

  /**
   * @brief Writes the log record of a record insert on a page.
   *
   * @param[in] insert_rec Inserted physical record.
   * @param[in] rec_size Size of the inserted record.
   * @param[in] cursor_rec Record the cursor is pointing to.
   * @param[in] index Record descriptor.
   * @param[in] mtr Mini-transaction handle.
   */
  static void insert_rec_write_log(Rec insert_rec, ulint rec_size, Rec cursor_rec, const Index *index, mtr_t *mtr);

  /**
   * @brief Writes log record of a record delete on a page.
   *
   * @param[in] rec Record to be deleted.
   * @param[in] mtr Mini-transaction handle.
   */
  void delete_rec_write_log(Rec rec, mtr_t *mtr);

#ifdef PAGE_CUR_ADAPT
#ifdef UNIV_SEARCH_PERF_STAT
  ulint m_short_succ{};
#endif /* UNIV_SEARCH_PERF_STAT */
#endif /* PAGE_CUR_ADAPT */

 public:
  /** Index the cursor is on. */
  const Index *m_index{};

  /** pointer to a record on page */
  Rec m_rec{};

  /** Current offsets of the record. */
  ulint *m_offsets{};

  /** Pointer to the current block containing rec. */
  Buf_block *m_block{};
};

// Legacy typedef for backward compatibility
using page_cur_t = Page_cursor;
