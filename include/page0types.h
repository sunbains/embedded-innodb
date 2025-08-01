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

/** @file include/page0types.h
Index page routines

Created 2/2/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "ut0byte.h"

struct Trx;
struct Index;
struct DTuple;
struct upd_t;
struct mtr_t;
struct Rec;

/** Eliminates a name collision on HP-UX */
#define page_t ib_page_t

/** Type of the index page */
using page_t = byte;

/** Raw page wrapper for type safety and easy refactoring */
struct Raw_page {
  /** Default constructor */
  Raw_page() : m_page(nullptr) {}

  /** Implicit constructor from page_t* */
  Raw_page(page_t *page) : m_page(page) {}

  /** Implicit conversion to page_t* for compatibility */
  operator page_t *() const { return m_page; }

  /** Get raw page pointer */
  page_t *get() const { return m_page; }

  /** Check if valid */
  bool is_valid() const { return m_page != nullptr; }

  /** Pointer arithmetic support */
  byte *operator+(ptrdiff_t offset) const { return m_page + offset; }

  /** Array access support */
  byte &operator[](size_t index) const { return m_page[index]; }

  /**
   * Gets the start of a page.
   *
   * @param[in] ptr pointer to page frame
   *
   * @return	start of the page
   */
  static inline page_t *align(const void *ptr) { return reinterpret_cast<page_t *>(ut_align_down(ptr, UNIV_PAGE_SIZE)); }

  /**
   * Gets the offset within a page.
   *
   * @param[in] ptr pointer to page frame
   *
   * @return	offset from the start of the page
   */
  static inline ulint offset(const void *ptr) { return ut_align_offset(ptr, UNIV_PAGE_SIZE); }

 private:
  page_t *m_page;
};

/** Undo page wrapper class */
struct Undo_page {
  /** Default constructor */
  Undo_page() = default;

  /** Constructor from Raw_page */
  explicit Undo_page(Raw_page page) : m_raw_page(page) {}

  /** Constructor from page_t* */
  explicit Undo_page(page_t *page) : m_raw_page(page) {}

  /** Get the raw page */
  Raw_page get_raw_page() const { return m_raw_page; }

  /** Implicit conversion to Raw_page for compatibility */
  operator Raw_page() const { return m_raw_page; }

  /** Implicit conversion to page_t* for compatibility */
  operator page_t *() const { return m_raw_page.get(); }

  /** Get raw page pointer */
  page_t *get() const { return m_raw_page.get(); }

  /** Check if valid */
  bool is_valid() const { return m_raw_page.is_valid(); }

  void add_undo_rec_log(ulint old_free, ulint new_free, mtr_t *mtr) noexcept;
  ulint set_next_prev_and_add(byte *ptr, mtr_t *mtr) noexcept;
  ulint report_insert(Trx *trx, const Index *index, const DTuple *clust_entry, mtr_t *mtr);
  ulint report_modify(
    Trx *trx, const Index *index, const Rec &rec, const ulint *offsets, const upd_t *update, ulint cmpl_info, mtr_t *mtr
  );
  void erase_page_end(mtr_t *mtr);

 private:
  Raw_page m_raw_page;
};

/** Index page cursor */
struct Page_cursor;

/**
 * Gets the offset within a page.
 *
 * @param[in] ptr pointer to page frame
 *
 * @return	offset from the start of the page
 */
inline ulint page_offset(const void *p) { return Raw_page::offset(p); }

/**
 * Gets the start of a page.
 *
 * @param[in] ptr pointer to page frame
 *
 * @return	start of the page
 */
inline page_t *page_align(const void *p) { return Raw_page::align(p); }
