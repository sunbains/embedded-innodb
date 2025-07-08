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

/** Info bit denoting the predefined minimum record: this bit is set
if and only if the record is the first user record on a non-leaf
B-tree page that is the leftmost page on its level
(PAGE_LEVEL is nonzero and FIL_PAGE_PREV is FIL_NULL). */
constexpr ulint REC_INFO_MIN_REC_FLAG = 0x10UL;

/* The deleted flag in info bits */

/** When bit is set to 1, it means the record has been delete marked */
constexpr ulint REC_INFO_DELETED_FLAG = 0x20UL;

/** Number of extra bytes in a record, in addition to the data and the offsets */
constexpr ulint REC_N_EXTRA_BYTES = 6;

/* Record status values */
constexpr ulint REC_STATUS_ORDINARY = 0;
constexpr ulint REC_STATUS_NODE_PTR = 1;
constexpr ulint REC_STATUS_INFIMUM = 2;
constexpr ulint REC_STATUS_SUPREMUM = 3;

/** Length of a B-tree node pointer, in bytes */
constexpr ulint REC_NODE_PTR_SIZE = 4;

#ifdef UNIV_DEBUG
/** Length of the Phy_rec::get_col_offsets() header */
constexpr ulint REC_OFFS_HEADER_SIZE = 4;
#else  /* UNIV_DEBUG */
/** Length of the Phy_rec::get_col_offsets() header */
constexpr ulint REC_OFFS_HEADER_SIZE = 2;
#endif /* UNIV_DEBUG */

/* Number of elements that should be initially allocated for the
offsets[] array, first passed to Phy_rec::get_col_offsets() */
constexpr ulint REC_OFFS_NORMAL_SIZE = 100;
constexpr ulint REC_OFFS_SMALL_SIZE = 10;

constexpr ulint REC_HEAP_NO_SHIFT = 3;

/** Maximum lengths for the data in a physical record if the offsets
are given in one byte (resp. two byte) format. */
constexpr ulint REC_1BYTE_OFFS_LIMIT = 0x7FUL;
constexpr ulint REC_2BYTE_OFFS_LIMIT = 0x7FFFUL;

/** The data size of record must be smaller than this because we reserve
two upmost bits in a two byte offset for special purposes */
constexpr ulint REC_MAX_DATA_SIZE = (16 * 1024);

/** SQL nullptr flag in offsets returned by Phy_rec::get_col_offsets() */
constexpr auto REC_OFFS_SQL_NULL = ((ulint)1 << 31);

/** External flag in offsets returned by Phy_rec::get_col_offsets() */
constexpr auto REC_OFFS_EXTERNAL = ((ulint)1 << 30);

/** Mask for offsets returned by Phy_rec::get_col_offsets() */
constexpr ulint REC_OFFS_MASK = (REC_OFFS_EXTERNAL - 1);

/** Offsets of the bit-fields in a record. NOTE! In the table the
most significant bytes and bits are written below less significant.

        row format = [...rec header | rec data ]

              Phy_rec header
  Byte offset		 Bit usage within byte
  -------------+-------------------------------------
  1	             [0..7] bits pointer to next record
  2	             [0..7] bits pointer to next record
  3	             [0]    bit short flag
                 [1..7] bits number of fields
  4	             [0..2] 3 bits number of fields
                 [3..7] 5 bits heap number
  5	             [0..7] 8 bits heap number
  6	             [0..3] 4 bits n_owned
                 [0..3] 4 bits info bits

  [msb.....................................................................................................lsb]
  [info bits (u4), n_owned (u4), heap number (u13), n_fields (u10), short flag (u1), next record pointer (u16)]
*/

/* We list the byte offsets from the origin of the record, the mask,
and the shift needed to obtain each bit-field of the record. */

constexpr ulint REC_NEXT = 2;
constexpr ulint REC_NEXT_MASK = 0xFFFFUL;
constexpr ulint REC_NEXT_SHIFT = 0;

constexpr ulint REC_SHORT = 3; /* This is single byte bit-field */
constexpr ulint REC_SHORT_MASK = 0x1UL;
constexpr ulint REC_SHORT_SHIFT = 0;

constexpr ulint REC_N_FIELDS = 4;
constexpr ulint REC_N_FIELDS_MASK = 0x7FEUL;
constexpr ulint REC_N_FIELDS_SHIFT = 1;

constexpr ulint REC_HEAP_NO = 5;
constexpr ulint REC_HEAP_NO_MASK = 0xFFF8UL;

constexpr ulint REC_N_OWNED = 6; /* This is single byte bit-field */
constexpr ulint REC_N_OWNED_MASK = 0xFUL;
constexpr ulint REC_N_OWNED_SHIFT = 0;

constexpr ulint REC_INFO_BITS = 6; /* This is single byte bit-field */
constexpr ulint REC_INFO_BITS_MASK = 0xF0UL;
constexpr ulint REC_INFO_BITS_SHIFT = 0;

/* The following masks are used to filter the SQL null bit from
one-byte and two-byte offsets */

constexpr ulint REC_1BYTE_SQL_NULL_MASK = 0x80UL;
constexpr ulint REC_2BYTE_SQL_NULL_MASK = 0x8000UL;

/* In a 2-byte offset the second most significant bit denotes
a field stored to another page: */

constexpr ulint REC_2BYTE_EXTERN_MASK = 0x4000UL;

static_assert(
  !(REC_SHORT_MASK << (8 * (REC_SHORT - 3)) ^ REC_N_FIELDS_MASK << (8 * (REC_N_FIELDS - 4)) ^
    REC_HEAP_NO_MASK << (8 * (REC_HEAP_NO - 4)) ^ REC_N_OWNED_MASK << (8 * (REC_N_OWNED - 3)) ^
    REC_INFO_BITS_MASK << (8 * (REC_INFO_BITS - 3)) ^ 0xFFFFFFFFUL),
  "#error sum of masks != 0xFFFFFFFFUL"
);

/**
 * The following function is used to get the offset to the nth data field in a record.
 *
 * @param rec Pointer to the record
 * @param n Index of the field
 * @param len Pointer to store the length of the field
 *            If the field is SQL null, it will be set to UNIV_SQL_NULL
 * @return The offset of the nth field in the record
 */
ulint rec_get_nth_field_offs(const rec_t *rec, ulint n, ulint *len) noexcept;

/**
 * Copies the first n fields of a physical record to a new physical record in a buffer.
 *
 * @param rec The physical record to copy the prefix from.
 * @param index The record descriptor.
 * @param n_fields The number of fields to copy.
 * @param buf A memory buffer for the copied prefix, or nullptr.
 * @param buf_size The size of the buffer.
 * @return own: Copied record.
 */
rec_t *rec_copy_prefix_to_buf(const rec_t *rec, const Index *index, ulint n_fields, byte *&buf, ulint &buf_size) noexcept;

/**
 * Builds a physical record out of a data tuple and stores it into the given buffer.
 *
 * @param buf The start address of the physical record.
 * @param index The record descriptor.
 * @param dtuple The data tuple to convert.
 * @param n_ext The number of externally stored columns.
 * @return A pointer to the converted record.
 */
rec_t *rec_convert_dtuple_to_rec(byte *buf, const Index *index, const DTuple *dtuple, ulint n_ext) noexcept;

/**
 * Copies the prefix of a physical record to a data tuple.
 *
 * @param tuple[out]     The output data tuple.
 * @param rec[in]        The input physical record.
 * @param index[in]      The record descriptor.
 * @param n_fields[in]   The number of fields to copy.
 * @param heap[in]       The memory heap.
 */
void rec_copy_prefix_to_dtuple(DTuple *tuple, const rec_t *rec, const Index *index, ulint n_fields, mem_heap_t *heap) noexcept;

/**
 * Validates a physical record.
 *
 * @param rec The physical record to validate.
 * @param offsets The array returned by Phy_rec::get_col_offsets().
 * @return True if the record is valid, false otherwise.
 */
bool rec_validate(const rec_t *rec, const ulint *offsets) noexcept;

/**
 * @brief Prints the contents of a physical record to log_info().
 *
 * @param rec The physical record to be printed.
 */
void rec_log_info(const rec_t *rec) noexcept;

/**
 * @brief Converts a physical record to a string.
 *
 * @param[in] rec The physical record to convert to a string.
 *
 * @return A string representation of the record.
 */
std::string rec_to_string(const rec_t *rec) noexcept;

/**
 * Print the Rec_offset to the output stream.
 *
 * @param[in,out] os Stream to write to
 * @param[in] r Record and its offsets
 *
 * @return the output stream
 */
inline std::ostream &operator<<(std::ostream &os, const rec_t *rec) noexcept {
  os << rec_to_string(rec);
  return os;
}

/**
 * Sets the null bit of the specified field in the given record.
 *
 * @param rec The record to modify.
 * @param i The index of the field.
 * @param val The value to set for the null bit.
 */
void rec_set_nth_field_null_bit(rec_t *rec, ulint i, bool val) noexcept;

/**
 * Sets the nth field of a record to SQL NULL.
 *
 * @param rec The record to modify.
 * @param n The index of the field to set to SQL NULL.
 */
void rec_set_nth_field_sql_null(rec_t *rec, ulint n) noexcept;

inline rec_t *rec_get_nth_field(rec_t *rec, ulint n, ulint *len) noexcept {
  return rec + rec_get_nth_field_offs(rec, n, len);
}

inline const rec_t *rec_get_nth_field(const rec_t *rec, ulint n, ulint *len) noexcept {
  return rec + rec_get_nth_field_offs(rec, n, len);
}

/**
 * Retrieves a bit field from a record.
 *
 * This function retrieves a bit field from a record based on the given offset, mask, and shift.
 *
 * @param rec   Pointer to the record origin.
 * @param offs  Offset from the origin down.
 * @param mask  Mask used to filter bits.
 * @param shift Shift right applied after masking.
 * @return      The retrieved bit field.
 */
inline ulint rec_get_bit_field_1(const rec_t *rec, ulint offs, ulint mask, ulint shift) noexcept {

  return (mach_read_from_1(rec - offs) & mask) >> shift;
}

/**
 * Sets a bit field within 1 byte.
 *
 * This function sets a bit field in a record by applying a mask and shifting the value.
 *
 * @param rec   Pointer to the record origin.
 * @param val   Value to set.
 * @param offs  Offset from the origin down.
 * @param mask  Mask used to filter bits.
 * @param shift Shift right applied after masking.
 */
inline void rec_set_bit_field_1(rec_t *rec, ulint val, ulint offs, ulint mask, ulint shift) noexcept {

  ut_ad(offs <= REC_N_EXTRA_BYTES);
  ut_ad(mask);
  ut_ad(mask <= 0xFFUL);
  ut_ad(((mask >> shift) << shift) == mask);
  ut_ad(((val << shift) & mask) == (val << shift));

  mach_write_to_1(rec - offs, (mach_read_from_1(rec - offs) & ~mask) | (val << shift));
}

/**
 * Gets a bit field from within 2 bytes.
 *
 * @param[in] rec	pointer to record origin
 * @param[in] offs	offset from the origin down
 * @param[in] mask	mask used to filter bits
 * @param[in] shift	shift right applied after masking
 */
inline ulint rec_get_bit_field_2(const rec_t *rec, ulint offs, ulint mask, ulint shift) noexcept {
  return (mach_read_from_2(rec - offs) & mask) >> shift;
}

/**
 * Sets a bit field within 2 bytes.
 *
 * @param rec   Pointer to the record origin.
 * @param val   Value to set in the bit field.
 * @param offs  Offset from the origin down.
 * @param mask  Mask used to filter bits.
 * @param shift Shift right applied after masking.
 */
inline void rec_set_bit_field_2(rec_t *rec, ulint val, ulint offs, ulint mask, ulint shift) noexcept {
  ut_ad(offs <= REC_N_EXTRA_BYTES);
  ut_ad(mask > 0xFFUL);
  ut_ad(mask <= 0xFFFFUL);
  ut_ad((mask >> shift) & 1);
  ut_ad(((mask >> shift) & ((mask >> shift) + 1)) == 0);
  ut_ad(((mask >> shift) << shift) == mask);
  ut_ad(((val << shift) & mask) == (val << shift));

  mach_write_to_2(rec - offs, (mach_read_from_2(rec - offs) & ~mask) | (val << shift));
}

/**
 * @brief The following function is used to get the pointer of the next chained record
 * on the same page.
 *
 * This function is used to get the pointer to the next record.
 *
 * @param rec The physical record.
 * @return	pointer to the next chained record, or nullptr if none
 */
inline const rec_t *rec_get_next_ptr_const(const rec_t *rec) noexcept {
  ut_ad(REC_NEXT_MASK == 0xFFFFUL);
  ut_ad(REC_NEXT_SHIFT == 0);

  const auto field_value = mach_read_from_2(rec - REC_NEXT);

  if (unlikely(field_value == 0)) {
    return nullptr;
  } else {
    ut_ad(field_value < UNIV_PAGE_SIZE);
    return reinterpret_cast<const rec_t *>(ut_align_down(rec, UNIV_PAGE_SIZE)) + field_value;
  }
}

/**
 * The following function is used to get the pointer of the next chained record on the same page.
 *
 * @param[in] rec The current physical record.
 * @param[in] comp A flag indicating whether the page format is compact or not.
 *                Nonzero value indicates compact format.
 * @return	pointer to the next chained record, or nullptr if none
 */
inline rec_t *rec_get_next_ptr(const rec_t *rec) noexcept {
  return reinterpret_cast<rec_t *>(const_cast<rec_t *>(rec_get_next_ptr_const(rec)));
}

/**
 * Calculates the offset of the next record in a physical record.
 *
 * @param rec The physical record.
 *
 * @return	the page offset of the next chained record, or 0 if none
 */
inline ulint rec_get_next_offs(const rec_t *rec) noexcept {
  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");
  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");

  const auto field_value = mach_read_from_2(rec - REC_NEXT);

  ut_ad(field_value < UNIV_PAGE_SIZE);

  return field_value;
}

/**
 * Sets the offset of the next record in a physical record.
 *
 * @param rec The physical record.
 * @param next The offset of the next record.
 */
inline void rec_set_next_offs(rec_t *rec, ulint next) noexcept {
  ut_ad(UNIV_PAGE_SIZE > next);

  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");
  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");

  mach_write_to_2(rec - REC_NEXT, next);
}

/**
 * The following function is used to get the number of fields
 * in a record.
 *
 * @param rec The physical record.
 * @return The number of fields in the record.
 */
inline ulint rec_get_n_fields(const rec_t *rec) noexcept {
  auto ret = rec_get_bit_field_2(rec, REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);

  ut_ad(ret <= REC_MAX_N_FIELDS);
  ut_ad(ret > 0);

  return ret;
}

/**
 * The following function is used to set the number of fields in a record.
 *
 * @param rec The physical record.
 * @param n_fields The number of fields.
 */
inline void rec_set_n_fields(rec_t *rec, ulint n_fields) noexcept {
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);

  rec_set_bit_field_2(rec, n_fields, REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);
}

/**
 * The following function is used to get the number of fields in a record.
 *
 * @param[in] rec	physical record
 *
 * @return	number of data fields
 */
inline ulint rec_get_n_fields(const rec_t *rec, const Index *) noexcept {
  return rec_get_n_fields(rec);
}

/**
 * The following function is used to get the number of records owned by the
 * previous directory record.
 *
 * @param[in] rec	physical record
 *
 * @return	number of owned records
 */
inline ulint rec_get_n_owned(const rec_t *rec) noexcept {
  return rec_get_bit_field_1(rec, REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/**
 * The following function is used to set the number of owned records.
 *
 * @param[in,out] rec physical record
 * @param[in] n_owned number of owned.
 */
inline void rec_set_n_owned(rec_t *rec, ulint n_owned) noexcept {
  rec_set_bit_field_1(rec, n_owned, REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/**
 * The following function is used to retrieve the info bits of a record.
 *
 * @param[in] rec	physical record
 *
 * @return	info bits
 */
inline ulint rec_get_info_bits(const rec_t *rec) noexcept {
  return rec_get_bit_field_1(rec, REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/**
 * The following function is used to set the info bits of a record.
 *
 * @param[in,out] rec	physical record
 * @param[in] bits	info bits
 */
inline void rec_set_info_bits(rec_t *rec, ulint bits) noexcept {
  rec_set_bit_field_1(rec, bits, REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/**
 * The following function is used to retrieve the info and status bits of a record.
 *
 * @param[in] rec	physical record
 *
 * @return	info bits
 */
inline ulint rec_get_info_and_status_bits(const rec_t *rec) noexcept {
  const auto bits = rec_get_info_bits(rec);

  ut_ad(!(bits & ~(REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)));

  return bits;
}

/**
 * The following function tells if record is delete marked.
 *
 * @param[in,out] rec	physical record
 *
 * @return	nonzero if delete marked */
inline ulint rec_get_deleted_flag(const rec_t *rec) noexcept {
  return unlikely(rec_get_bit_field_1(rec, REC_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT));
}

/**
 * The following function is used to set the deleted bit.
 *
 * @param[in,out] rec	physical record
 * @param[in] deleted	true to delete mark
 */
inline void rec_set_deleted_flag(rec_t *rec, bool deleted) noexcept {
  auto val = rec_get_info_bits(rec);

  if (deleted) {
    val |= REC_INFO_DELETED_FLAG;
  } else {
    val &= ~REC_INFO_DELETED_FLAG;
  }

  rec_set_info_bits(rec, val);
}

/**
 * The following function is used to get the heap number in the heap of the index page.
 *
 * @param[in] rec	physical record
 *
 * @return	heap order number
 */
inline ulint rec_get_heap_no(const rec_t *rec) noexcept {
  return rec_get_bit_field_2(rec, REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to set the heap number field.
 *
 * @param[in,out] rec	physical record
 * @param[in] heap_no	heap number
 */
inline void rec_set_heap_no(rec_t *rec, ulint heap_no) noexcept {
  rec_set_bit_field_2(rec, heap_no, REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to test whether the data offsets in the
 * record are stored in one-byte or two-byte format.
 *
 * @param[in] rec	physical record
 *
 * @return	true if 1-byte form
 */
inline bool rec_get_1byte_offs_flag(const rec_t *rec) noexcept {
  return rec_get_bit_field_1(rec, REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT);
}

/**
 * The following function is used to set the 1-byte offsets flag.
 *
 * @param[in,out] rec	physical record
 * @param[in] flag	true if 1-byte form
 */
inline void rec_set_1byte_offs_flag(rec_t *rec, bool flag) noexcept {
  rec_set_bit_field_1(rec, flag, REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT);
}

/**
 * Returns the offset of nth field end if the record is stored in the 1-byte
 * offsets form. If the field is SQL null, the flag is ORed in the returned
 * value.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field, SQL null flag ORed */
inline ulint rec_1_get_field_end_info(const rec_t *rec, ulint n) noexcept {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields(rec));

  return mach_read_from_1(rec - (REC_N_EXTRA_BYTES + n + 1));
}

/**
 * Returns the offset of nth field end if the record is stored in the 2-byte
 * offsets form. If the field is SQL null, the flag is ORed in the returned
 * value.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return offset of the start of the field, SQL null flag and extern storage flag ORed
 */
inline ulint rec_2_get_field_end_info(const rec_t *rec, ulint n) noexcept {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields(rec));

  return mach_read_from_2(rec - (REC_N_EXTRA_BYTES + 2 * n + 2));
}

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
inline bool rec_offs_validate(const rec_t *rec, const Index *index, const ulint *offsets) noexcept {
  ulint last = ULINT_MAX;
  ulint i = rec_offs_n_fields(offsets);

  if (rec != nullptr) {
    ut_ad((ulint)rec == offsets[2]);
    ut_a(rec_get_n_fields(rec) >= i);
  }
  if (index != nullptr) {
    ut_ad((ulint)index == offsets[3]);

    const auto max_n_fields = ut_max(index->get_n_fields(), index->get_n_unique_in_tree() + 1);

    ut_a(index->m_n_defined == 0 || i <= max_n_fields);
  }

  while (i--) {
    const auto curr = rec_offs_base(offsets)[i + 1] & REC_OFFS_MASK;
    ut_a(curr <= last);
    last = curr;
  }

  return true;
}

/**
 * Updates debug data in offsets, in order to avoid bogus
 * rec_offs_validate() failures.
 *
 * @param[in] rec	record
 * @param[in] index	record descriptor
 * @param[out] offsets	array returned by Phy_rec::get_col_offsets()
 */
inline void rec_offs_make_valid(const rec_t *rec, const Index *index, ulint *offsets) noexcept {
  ut_ad(rec_get_n_fields(rec, index) >= rec_offs_n_fields(offsets));

  offsets[2] = reinterpret_cast<uintptr_t>(rec);
  offsets[3] = reinterpret_cast<uintptr_t>(index);
}
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

inline const rec_t *rec_get_nth_field(const rec_t *rec, const ulint *offsets, ulint i, ulint *len) noexcept {
  return rec + rec_get_nth_field_offs(offsets, i, len);
}

/**
 * Determine if the offsets are for a record containing externally stored columns.
 *
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	nonzero if externally stored
 */
inline ulint rec_offs_any_extern(const ulint *offsets) noexcept {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

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
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
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
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
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
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
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
        n++;
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
inline ulint rec_1_get_prev_field_end_info(const rec_t *rec, ulint n) noexcept {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields(rec));

  return mach_read_from_1(rec - (REC_N_EXTRA_BYTES + n));
}

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
inline ulint rec_2_get_prev_field_end_info(const rec_t *rec, ulint n) noexcept {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields(rec));

  return mach_read_from_2(rec - (REC_N_EXTRA_BYTES + 2 * n));
}

/**
 * Sets the field end info for the nth field if the record is stored in the 1-byte format.
 *
 * @param[in,out] rec	physical record
 * @param[in] n	field index
 * @param[in] info	value to set
 *
 */
inline void rec_1_set_field_end_info(rec_t *rec, ulint n, ulint info) noexcept {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields(rec));

  mach_write_to_1(rec - (REC_N_EXTRA_BYTES + n + 1), info);
}

/**
 * Sets the field end info for the nth field if the record is stored in the
 * 2-byte format.
 *
 * @param[in,out] rec	physical record
 * @param[in] n	field index
 * @param[in] info	value to set
 *
 * */
inline void rec_2_set_field_end_info(rec_t *rec, ulint n, ulint info) noexcept {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields(rec));

  mach_write_to_2(rec - (REC_N_EXTRA_BYTES + 2 * n + 2), info);
}

/**
 * Returns the offset of nth field start if the record is stored in the 1-byte offsets form.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field
 */
inline ulint rec_1_get_field_start_offs(const rec_t *rec, ulint n) noexcept {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields(rec));

  if (n == 0) {
    return 0;
  } else {
    return rec_1_get_prev_field_end_info(rec, n) & ~REC_1BYTE_SQL_NULL_MASK;
  }
}

/**
 * Returns the offset of nth field start if the record is stored in the 2-byte offsets form.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	offset of the start of the field
 */
inline ulint rec_2_get_field_start_offs(const rec_t *rec, ulint n) noexcept {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields(rec));

  if (n == 0) {
    return 0;
  } else {
    return rec_2_get_prev_field_end_info(rec, n) & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
  }
}

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
inline ulint rec_get_field_start_offs(const rec_t *rec, ulint n) noexcept {
  ut_ad(n <= rec_get_n_fields(rec));

  if (n == 0) {
    return 0;
  } else if (rec_get_1byte_offs_flag(rec)) {
    return rec_1_get_field_start_offs(rec, n);
  } else {
    return rec_2_get_field_start_offs(rec, n);
  }
}

/**
 * Gets the physical size of a field. Also an SQL null may have a
 * field of size > 0, if the data type is of a fixed size.
 *
 * @param[in] rec	physical record
 * @param[in] n	field index
 *
 * @return	field size in bytes
 */
inline ulint rec_get_nth_field_size(const rec_t *rec, ulint n) noexcept {
  auto os = rec_get_field_start_offs(rec, n);
  auto next_os = rec_get_field_start_offs(rec, n + 1);

  ut_ad(next_os - os < UNIV_PAGE_SIZE);

  return next_os - os;
}

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
inline void rec_set_nth_field(rec_t *rec, const ulint *offsets, ulint n, const void *data, ulint len) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  if (unlikely(len == UNIV_SQL_NULL)) {
    if (!rec_offs_nth_sql_null(offsets, n)) {
      rec_set_nth_field_sql_null(rec, n);
    }

    return;
  }

  ulint dst_len;
  auto dst = rec + rec_get_nth_field_offs(offsets, n, &dst_len);

  if (dst_len == UNIV_SQL_NULL) {
    rec_set_nth_field_null_bit(rec, n, false);
    ut_ad(len == rec_get_nth_field_size(rec, n));
  } else {
    ut_ad(dst_len == len);
  }

  memcpy(dst, data, len);
}

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
inline ulint rec_get_data_size(const rec_t *rec) noexcept {
  return rec_get_field_start_offs(rec, rec_get_n_fields(rec));
}

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
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

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
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

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
inline byte *rec_get_end(rec_t *rec, const ulint *offsets) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  return rec + rec_offs_data_size(offsets);
}

/**
 * Returns a pointer to the start of the record.
 *
 * @param[in] rec	pointer to record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	pointer to start
 */
inline byte *rec_get_start(rec_t *rec, const ulint *offsets) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  return rec - rec_offs_extra_size(offsets);
}

/**
 * Copies a physical record to a buffer.
 *
 * @param[out] buf	buffer
 * @param[in] rec	physical record
 * @param[in] offsets	array returned by Phy_rec::get_col_offsets()
 *
 * @return	pointer to the origin of the copy
 */
inline rec_t *rec_copy(void *buf, const rec_t *rec, const ulint *offsets) noexcept {
  ut_ad(buf != nullptr);
  ut_ad(rec_offs_validate((rec_t *)rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));

  const auto extra_len = rec_offs_extra_size(offsets);
  const auto data_len = rec_offs_data_size(offsets);

  memcpy(buf, rec - extra_len, extra_len + data_len);

  return static_cast<rec_t *>(buf) + extra_len;
}

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
inline ulint rec_fold(const rec_t *rec, const ulint *offsets, ulint n_fields, ulint n_bytes, uint64_t tree_id) noexcept {

  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));
  ut_ad(n_fields + n_bytes > 0);

  const auto n_fields_rec = rec_offs_n_fields(offsets);

  ut_ad(n_fields <= n_fields_rec);
  ut_ad(n_fields < n_fields_rec || n_bytes == 0);

  if (n_fields > n_fields_rec) {
    n_fields = n_fields_rec;
  }

  if (n_fields == n_fields_rec) {
    n_bytes = 0;
  }

  auto fold = ut_uint64_fold(tree_id);

  for (ulint i = 0; i < n_fields; i++) {
    ulint len;
    auto data = rec_get_nth_field(rec, offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  if (n_bytes > 0) {
    ulint len;
    auto data = rec_get_nth_field(rec, offsets, n_fields, &len);

    if (len != UNIV_SQL_NULL) {
      if (len > n_bytes) {
        len = n_bytes;
      }

      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  return fold;
}

/**
 * Determine if the offsets are for a record containing null BLOB pointers.
 * @param rec     in: record
 * @param offsets in: Phy_rec::get_col_offsets(rec)
 * @return        first field containing a null BLOB pointer, or NULL if none found
 */
inline const byte *rec_offs_any_null_extern(const rec_t *rec, const ulint *offsets) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  if (!rec_offs_any_extern(offsets)) {
    return nullptr;
  }

  for (ulint i = 0; i < rec_offs_n_fields(offsets); i++) {
    if (rec_offs_nth_extern(offsets, i)) {
      ulint len;
      const byte *field = rec_get_nth_field(rec, offsets, i, &len);

      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);
      if (!memcmp(field + len - BTR_EXTERN_FIELD_REF_SIZE, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE)) {
        return field;
      }
    }
  }

  return nullptr;
}

struct Immutable_rec {
  explicit Immutable_rec(const rec_t *rec) noexcept : m_rec(rec) {}

  Immutable_rec next() const noexcept { return Immutable_rec(rec_get_next_ptr_const(m_rec)); }

  ulint n_fields() const noexcept { return rec_get_n_fields(m_rec, nullptr); }

  ulint heap_no() const noexcept { return rec_get_heap_no(m_rec); }

  ulint n_owned() const noexcept { return rec_get_n_owned(m_rec); }

  ulint info_bits() const noexcept { return rec_get_info_bits(m_rec); }

  ulint info_and_status_bits() const noexcept { return rec_get_1byte_offs_flag(m_rec); }

  bool is_delete_marked() const noexcept { return rec_get_deleted_flag(m_rec); }

  const rec_t *m_rec{};
};

struct Mutable_rec : public Immutable_rec {
  explicit Mutable_rec(rec_t *rec) noexcept : Immutable_rec(rec) {}

  Mutable_rec next() noexcept { return Mutable_rec(rec_get_next_ptr(const_cast<rec_t *>(m_rec))); }

  void delete_mark() noexcept { rec_set_deleted_flag(const_cast<rec_t *>(m_rec), true); }

  void delete_unmark() noexcept { rec_set_deleted_flag(const_cast<rec_t *>(m_rec), false); }
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
  explicit Phy_rec(const Index *index, const rec_t *rec) noexcept : m_index(index), m_rec(rec) {}

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
  static void encode(Index *index, rec_t *rec, ulint status, const DFields &dfields) noexcept;

 private:
  /** Record belongs to this index. */
  const Index *m_index{};

  /** Record byte array */
  const rec_t *m_rec{};
};
