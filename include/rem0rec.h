/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
Copyrifht (c) 2024 Sunny Bains, All rights reserved.

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

/** @file include/rem0rec.h
Record manager

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#pragma once

#include "innodb0types.h"

#include <ostream>

#include "btr0types.h"
#include "data0data.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mtr0types.h"
#include "page0types.h"
#include "rem0types.h"
#include "ut0byte.h"



/**
 * Returns the extra size of a physical record if we know its
 * data size and number of fields.
 *
 * @param[in] data_size	data size
 * @param[in] n_fields	number of fields
 * @param[in] n_ext	number of externally stored columns
 *
 * @return	extra size
 */
inline ulint rec_get_converted_extra_size(ulint data_size, ulint n_fields, ulint n_ext) noexcept {
  if (n_ext == 0 && data_size <= REC_1BYTE_OFFS_LIMIT) {
    return REC_N_EXTRA_BYTES + n_fields;
  } else {
    return REC_N_EXTRA_BYTES + 2 * n_fields;
  }
}

/**
 * The following function returns the size of a data tuple when converted to
 * a physical record.
 *
 * @param[in,out] index	record descriptor
 * @param[in] dtuple	data tuple
 * @param[in] n_ext	number of externally stored columns
 *
 * @return	size
 */
inline ulint rec_get_converted_size(const Index *index, const DTuple *dtuple, ulint n_ext) noexcept {
  ut_ad(dtuple_check_typed(dtuple));

  const auto data_size = dtuple_get_data_size(dtuple);
  const auto extra_size = rec_get_converted_extra_size(data_size, dtuple_get_n_fields(dtuple), n_ext);

  return data_size + extra_size;
}

struct Immutable_rec {
  explicit Immutable_rec(const Rec &rec) noexcept : m_rec(rec) {}

  Immutable_rec next() const noexcept { return Immutable_rec(m_rec.get_next_ptr_const()); }

  ulint n_fields() const noexcept { return m_rec.get_n_fields(); }

  ulint heap_no() const noexcept { return m_rec.get_heap_no(); }

  ulint n_owned() const noexcept { return m_rec.get_n_owned(); }

  ulint info_bits() const noexcept { return m_rec.get_info_bits(); }

  ulint info_and_status_bits() const noexcept { return m_rec.get_1byte_offs_flag(); }

  bool is_delete_marked() const noexcept { return m_rec.get_deleted_flag(); }

  const Rec m_rec{};
};

struct Mutable_rec : public Immutable_rec {
  explicit Mutable_rec(const Rec &rec) noexcept : Immutable_rec(rec) {}

  Mutable_rec next() noexcept { return Mutable_rec(m_rec.get_next_ptr()); }

  void delete_mark() noexcept {
    Rec rec{const_cast<rec_t *>(m_rec.get())};

    rec.set_deleted_flag(true);
  }

  void delete_unmark() noexcept {
    Rec rec{const_cast<rec_t *>(m_rec.get())};

    rec.set_deleted_flag(false);
  }
};

/** Physical record manager */
struct Phy_rec {
  /** first = rec header size in bytes, second = data size in bytes. */
  using Size = std::pair<ulint, ulint>;

  /** Record fields.*/
  using DFields = std::pair<const dfield_t *, ulint>;

  /** Constructor.
   *
   * @param[in] index The index that contains the record.
   * @param[in] rec The record in the index.
   */
  explicit Phy_rec(const Index *index, const Rec rec) noexcept : m_index(index), m_rec(rec) {}

  /**
   * Determine the offset to each field in a leaf-page record.
   *
   * @param offsets The array of offsets. On input, it should contain
   *   the number of fields (n=rec_offs_n_fields(offsets)).
   */
  void get_col_offsets(ulint *offsets) const noexcept;

  /**
   * Retrieves the offsets of fields in a physical record. It can reuse
   * a previously allocated array.
   *
   * @param offsets An array consisting of offsets[0] allocated elements,
   *   or an array from Phy_rec::get_col_offsets(), or nullptr.
   * @param n_fields The maximum number of initialized fields (ULINT_UNDEFINED
   *   if all fields).
   * @param heap The memory heap, if offsets == nullptr. If it's nullptr, then
   *  the heap is allocated and returned in the output parameter.
   * @param sl Source location of caller.
   *
   * @return the new offsets representing the fields in the record.
   */
  ulint *get_col_offsets(ulint *offsets, ulint n_fields, mem_heap_t **heap, Source_location sl) const noexcept;

  /**
   * Retrieves the offsets of all fields in a physical record. It can reuse
   * a previously allocated array.
   *
   * @param offsets An array consisting of offsets[0] allocated elements,
   *   or an array from Phy_rec::get_col_offsets(), or nullptr.
   * @param heap The memory heap, if offsets == nullptr. If it's nullptr, then
   *  the heap is allocated and returned in the output parameter.
   * @param sl Source location of caller.
   *
   * @return the new offsets representing the fields in the record.
   */
  ulint *get_all_col_offsets(ulint *offsets, mem_heap_t **heap, Source_location sl) const noexcept {
    return get_col_offsets(offsets, ULINT_UNDEFINED, heap, sl);
  }

  template <size_t N>
  ulint *get_col_offsets(std::array<ulint, N> &offsets, ulint n_fields, mem_heap_t **heap, Source_location sl) const noexcept {
    ut_ad(n_fields == ULINT_UNDEFINED || n_fields < N);
    return get_col_offsets(offsets.data(), n_fields, heap, sl);
  }

  template <size_t N>
  ulint *get_all_col_offsets(std::array<ulint, N> &offsets, mem_heap_t **heap, Source_location sl) const noexcept {
    return get_col_offsets(offsets, ULINT_UNDEFINED, heap, sl);
  }

  /**
   * Get the physical size in bytes of the record minus the prefix extra  bytes (REC_N_EXTRA_BYTES).
   *
   * @param[in] index The index that contains the record.
   * @param[in] status The status of the record REC_STATUS_xxx.
   * @param[in] dfields The data fields of the record.
   *
   * @return The size of the record, pair.first = header size, pair.second = data size.
   */
  static Size get_encoded_size(Index *index, ulint status, const DFields &dfields) noexcept;

  /** Encode the data from dfields into m_rec.
   *
   * @param[in] index The index that contains the record.
   * @param[in,out] rec Encode into this buffer
   * @param[in] status The status of the record REC_STATUS_xxx.
   * @param[in] dfields The data fields and type information of the columns
   */
  static void encode(Index *index, Rec rec, ulint status, const DFields &dfields) noexcept;

 private:
  /** Record belongs to this index. */
  const Index *m_index{};

  /** Record byte array */
  const Rec m_rec{};
};
