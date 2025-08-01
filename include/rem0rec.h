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

// Function declarations for offset operations

// Moved to Rec::get_nth_field_offs

// Moved to Rec::copy_prefix_to_buf

// Moved to Rec::convert_dtuple_to_rec

// Moved to Rec::copy_prefix_to_dtuple

// Moved to Rec::validate

// Moved to Rec::log_info

// Moved to Rec::to_string (already exists)

// operator<< is defined in rem0types.h

// Moved to Rec::set_nth_field_null_bit

// Moved to Rec::set_nth_field_sql_null

// Moved to Rec::get_nth_field

// Moved to Rec::get_bit_field_1

// Moved to Rec::set_bit_field_1

// Moved to Rec::get_bit_field_2

// Moved to Rec::set_bit_field_2

// Moved to Rec::get_next_ptr_const

// Moved to Rec::get_next_ptr

// Moved to Rec::get_next_offs

// Moved to Rec::set_next_offs

// Moved to Rec::get_n_fields

// Moved to Rec::set_n_fields

// Moved to Rec::get_n_fields (overloaded version)

// Moved to Rec::get_n_owned

// Moved to Rec::set_n_owned

// Moved to Rec::get_info_bits

// Moved to Rec::set_info_bits

// Moved to Rec::get_info_and_status_bits

// Moved to Rec::get_deleted_flag

// Moved to Rec::set_deleted_flag

// Moved to Rec::get_heap_no

// Moved to Rec::set_heap_no

// Moved to Rec::get_1byte_offs_flag

// Moved to Rec::set_1byte_offs_flag

// Moved to Rec::get_1_field_end_info

// Moved to Rec::get_2_field_end_info

/**
 * The following function returns the number of allocated elements
 * for an array of offsets.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	number of elements
 */
inline ulint rec_offs_get_n_alloc(const ulint *offsets) noexcept {
  ut_ad(offsets != nullptr);

  auto n_alloc = offsets[0];

  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);
  UNIV_MEM_ASSERT_W(offsets, n_alloc * sizeof(*offsets));

  return n_alloc;
}

/**
 * The following function sets the number of allocated elements
 * for an array of offsets.
 *
 * @param[out] offsets	array for Phy_rec::get_col_offsets(), must be allocated
 * @param[in] n_alloc	number of elements
 */
inline void rec_offs_set_n_alloc(ulint *offsets, ulint n_alloc) noexcept {
  ut_ad(offsets != nullptr);
  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);

  UNIV_MEM_ASSERT_AND_ALLOC(offsets, n_alloc * sizeof *offsets);

  offsets[0] = n_alloc;
}

template <size_t N>
inline void rec_offs_init(std::array<ulint, N> &offsets) noexcept {
  rec_offs_set_n_alloc(offsets.data(), N);
}

/**
 * The following function returns the number of fields in a record.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	number of fields */
inline ulint rec_offs_n_fields(const ulint *offsets) noexcept {
  const auto n_fields = offsets[1];

  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields + REC_OFFS_HEADER_SIZE <= rec_offs_get_n_alloc(offsets));

  return n_fields;
}

/**
 * Get the base address of offsets.  The extra_size is stored at
 * this position, and following positions hold the end offsets of
 * the fields.
 *
 * @param[in] offsets	Column offsets within some record
 *
 * @return base address of offsets as a const pointer
 */
template <typename T>
inline T rec_offs_base(T offsets) noexcept {
  return offsets + REC_OFFS_HEADER_SIZE;
}

#ifdef UNIV_DEBUG
/**
 * Validates offsets returned by Phy_rec::get_col_offsets().
 *
 * @param[in] rec	record or nullptr
 * @param[in] index	record descriptor or nullptr
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	true if valid
 */
// Moved to Rec::offs_validate

/**
 * Updates debug data in offsets, in order to avoid bogus
 * rec_offs_validate() failures.
 *
 * @param[in] rec	record
 * @param[in] index	record descriptor
 * @param[out] offsets	array returned by Phy_rec::get_col_offsets()
 */
// Moved to Rec::offs_make_valid
#endif /* UNIV_DEBUG */

/**
 * The following function is used to get an offset to the nth
 * data field in a record.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n	field index
 * @param[out] len	length of the field; UNIV_SQL_NULL if SQL null
 *
 * @return	offset from the origin of rec
 */
inline ulint rec_get_nth_field_offs(const ulint *offsets, ulint n, ulint *len) noexcept {
  ulint offs;

  ut_ad(n < rec_offs_n_fields(offsets));

  if (unlikely(n == 0)) {
    offs = 0;
  } else {
    offs = rec_offs_base(offsets)[n] & REC_OFFS_MASK;
  }

  auto length = rec_offs_base(offsets)[n + 1];

  if (length & REC_OFFS_SQL_NULL) {
    length = UNIV_SQL_NULL;
  } else {
    length &= REC_OFFS_MASK;
    length -= offs;
  }

  *len = length;

  return offs;
}

// Moved to Rec::get_nth_field

/**
 * Determine if the offsets are for a record containing externally stored columns.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	nonzero if externally stored
 */
inline ulint rec_offs_any_extern(const ulint *offsets) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));

  return *rec_offs_base(offsets) & REC_OFFS_EXTERNAL;
}

/**
 * Returns nonzero if the extern bit is set in nth field of rec.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n	field index
 *
 * @return	nonzero if externally stored */
inline ulint rec_offs_nth_extern(const ulint *offsets, ulint n) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  return rec_offs_base(offsets)[n + 1] & REC_OFFS_EXTERNAL;
}

/**
 * Returns nonzero if the SQL nullptr bit is set in nth field of rec.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n	field index
 *
 * @return	nonzero if SQL nullptr
 */
inline ulint rec_offs_nth_sql_null(const ulint *offsets, ulint n) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  return rec_offs_base(offsets)[n + 1] & REC_OFFS_SQL_NULL;
}

/**
 * Gets the physical size of a field.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n	field index
 *
 * @return	length of field
 */
inline ulint rec_offs_nth_size(const ulint *offsets, ulint n) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  if (n == 0) {
    return rec_offs_base(offsets)[n + 1] & REC_OFFS_MASK;
  } else {
    return rec_offs_base(offsets)[n + 1] - (rec_offs_base(offsets)[n] & REC_OFFS_MASK);
  }
}

/**
 * Returns the number of extern bits set in a record.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	number of externally stored fields
 */
inline ulint rec_offs_n_extern(const ulint *offsets) noexcept {
  ulint n = 0;

  if (rec_offs_any_extern(offsets)) {
    for (ulint i = rec_offs_n_fields(offsets); i--;) {
      if (rec_offs_nth_extern(offsets, i)) {
        ++n;
      }
    }
  }

  return n;
}

/**
 * Returns the offset of n - 1th field end if the record is stored in the
 * 1-byte offsets form. If the field is SQL null, the flag is ORed in the returned
 * value. This function and the 2-byte counterpart are defined here because the
 * C-compiler was not able to sum negative and positive constant offsets, and
 * warned of constant arithmetic overflow within the compiler.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the PREVIOUS field, SQL null flag ORed
 */
// Moved to Rec::get_1_prev_field_end_info

/**
 * Returns the offset of n - 1th field end if the record is stored in the
 * 2-byte offsets form. If the field is SQL null, the flag is ORed in the returned
 * value.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the PREVIOUS field, SQL null flag ORed
 */
// Moved to Rec::get_2_prev_field_end_info

/**
 * Sets the field end info for the nth field if the record is stored in the 1-byte format.
 *
 * @param[in,out] rec	physical record
 * @param[in] n	field index
 * @param[in] info	value to set
 *
 */
// Moved to Rec::set_1_field_end_info

/**
 * Sets the field end info for the nth field if the record is stored in the
 * 2-byte format.
 *
 * @param[in,out] rec	physical record
 * @param[in] n	field index
 * @param[in] info	value to set
 *
 * */
// Moved to Rec::set_2_field_end_info

/**
 * Returns the offset of nth field start if the record is stored in the 1-byte offsets form.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field
 */
// Moved to Rec::get_1_field_start_offs

/**
 * Returns the offset of nth field start if the record is stored in the 2-byte offsets form.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field
 */
// Moved to Rec::get_2_field_start_offs

/**
 * The following function is used to read the offset of the start of a data
 * field in the record. The start of an SQL null field is the end offset of the
 * previous non-null field, or 0, if none exists. If n is the number of the last
 * field + 1, then the end offset of the last field is returned.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field
 */
// Moved to Rec::get_field_start_offs

/**
 * Gets the physical size of a field. Also an SQL null may have a
 * field of size > 0, if the data type is of a fixed size.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	field size in bytes
 */
// Moved to Rec::get_nth_field_size

/**
 * This is used to modify the value of an already existing field in a record.
 * The previous value must have exactly the same size as the new value. If len
 * is UNIV_SQL_NULL then the field is treated as an SQL null.
 *
 * @param[in,out] rec	physical record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n	field index
 * @param[in] data	pointer to the data
 * @param[in] len	length of the data or UNIV_SQL_NULL
 */
// Moved to Rec::set_nth_field

/**
 * The following function returns the data size of a physical
 * record, that is the sum of field lengths. SQL null fields
 * are counted as length 0 fields. The value returned by the function
 * is the distance from record origin to record end in bytes.
 *
 * @param[in] rec	physical record
 *
 * @return	size
 */
// Moved to Rec::get_data_size

/**
 * The following function sets the number of fields in offsets.
 *
 * @param[out] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n_fields	number of fields
 *
 */
inline void rec_offs_set_n_fields(ulint *offsets, ulint n_fields) noexcept {
  ut_ad(offsets);
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields + REC_OFFS_HEADER_SIZE <= rec_offs_get_n_alloc(offsets));

  offsets[1] = n_fields;
}

/**
 * The following function returns the data size of a physical
 * record, that is the sum of field lengths. SQL null fields
 * are counted as length 0 fields. The value returned by the function
 * is the distance from record origin to record end in bytes.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	size
 */
inline ulint rec_offs_data_size(const ulint *offsets) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));

  const auto size = rec_offs_base(offsets)[rec_offs_n_fields(offsets)] & REC_OFFS_MASK;

  ut_ad(size < UNIV_PAGE_SIZE);

  return size;
}

/**
 * Returns the total size of record minus data size of record. The value
 * returned by the function is the distance from record start to record origin
 * in bytes.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	size
 */
inline ulint rec_offs_extra_size(const ulint *offsets) noexcept {
  ut_ad(Rec{}.offs_validate(nullptr, offsets));

  const auto size = *rec_offs_base(offsets) & ~REC_OFFS_EXTERNAL;

  ut_ad(size < UNIV_PAGE_SIZE);

  return size;
}

/**
 * Returns the total size of a physical record.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	size
 */
inline ulint rec_offs_size(const ulint *offsets) noexcept {
  return rec_offs_data_size(offsets) + rec_offs_extra_size(offsets);
}

/**
 * Returns a pointer to the end of the record.
 *
 * @param[in] rec	pointer to record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	pointer to end */
// Moved to Rec::get_end

/**
 * Returns a pointer to the start of the record.
 *
 * @param[in] rec	pointer to record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	pointer to start
 */
// Moved to Rec::get_start

/**
 * Copies a physical record to a buffer.
 *
 * @param[out] buf	buffer
 * @param[in] rec	physical record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	pointer to the origin of the copy
 */
// Moved to Rec::copy

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

/**
 * Folds a prefix of a physical record to a ulint. Folds only existing fields,
 * that is, checks that we do not run out of the record.
 *
 * @param[in] rec	physical record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 * @param[in] n_fields	number of complete fields to fold
 * @param[in] n_bytes	number of bytes to fold in an incomplete last field
 * @param[in] tree_id	index tree id
 *
 * @return	the folded value
 */
// Moved to Rec::fold

/**
 * Determine if the offsets are for a record containing null BLOB pointers.
 * @param rec     in: record
 * @param offsets in: Phy_rec::get_col_offsets(rec)
 * @return        first field containing a null BLOB pointer, or NULL if none found
 */
// Moved to Rec::offs_any_null_extern

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
