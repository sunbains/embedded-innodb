/*****************************************************************************
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

/********************************************************************/ /**
 @file include/rem0types.h
 Record manager global types

 Created 5/30/1994 Heikki Tuuri
 *************************************************************************/

#pragma once

#include <string>

#include "mach0data.h"
#include "mem0mem.h"
#include "page0types.h"
#include "ut0dbg.h"
#include "ut0rnd.h"

// External field reference zero pattern
constexpr ulint BTR_EXTERN_FIELD_REF_SIZE_LOCAL = 20;
extern const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE_LOCAL];

// Forward declarations
class Index;
class DTuple;

// Function declarations for offset operations
ulint rec_offs_n_fields(const ulint *offsets) noexcept;
ulint rec_offs_data_size(const ulint *offsets) noexcept;
ulint rec_offs_extra_size(const ulint *offsets) noexcept;
ulint rec_offs_any_extern(const ulint *offsets) noexcept;
ulint rec_offs_nth_extern(const ulint *offsets, ulint n) noexcept;
ulint rec_offs_nth_sql_null(const ulint *offsets, ulint n) noexcept;
ulint rec_offs_get_nth_field(const ulint *offsets, ulint n, ulint *len) noexcept;

/* We define the physical record simply as an array of bytes */
/* Maximum values for various fields (for non-blob tuples) */
constexpr ulint REC_MAX_N_FIELDS = 1024 - 1;
constexpr ulint REC_MAX_HEAP_NO = 2 * 8192 - 1;
constexpr ulint REC_MAX_N_OWNED = 16 - 1;

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
  1                      [0..7] bits pointer to next record
  2                      [0..7] bits pointer to next record
  3                      [0]    bit short flag
                         [1..7] bits number of fields
  4                      [0..2] 3 bits number of fields
                         [3..7] 5 bits heap number
  5                      [0..7] 8 bits heap number
  6                      [0..3] 4 bits n_owned
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

using rec_t = byte;

/** Raw record wrapper for type safety and easy refactoring */
struct Rec {
  Rec() : m_rec(nullptr) {}

  Rec(std::nullptr_t) : m_rec(nullptr) {}

  Rec(rec_t *rec) : m_rec(rec) {}

  Rec(const rec_t *rec) : m_rec(const_cast<rec_t *>(rec)) {}

  Rec(Rec &&other) : m_rec(other.m_rec) { other.m_rec = nullptr; }

  Rec(const Rec &other) : m_rec(other.m_rec) {}

  Rec &operator=(const Rec &rhs) {
    if (this == &rhs) {
      return *this;
    } else {
      m_rec = rhs.m_rec;
      return *this;
    }
  }

  Rec &operator=(Rec &&rhs) {
    if (this == &rhs) {
      return *this;
    } else {
      m_rec = rhs.m_rec;
      rhs.m_rec = nullptr;
      return *this;
    }
  }

  rec_t *get() { return m_rec; }

  const rec_t *get() const { return m_rec; }

  operator rec_t *() { return m_rec; }

  operator const rec_t *() const { return m_rec; }

  bool is_valid() const { return !is_null(); }

  std::string to_string() const {
    if (is_null()) {
      return "Rec (nullptr)";
    }
    return "Rec (" + std::to_string(reinterpret_cast<uintptr_t>(m_rec)) + ")";
  }

  const rec_t *page_align() const noexcept { return Raw_page::align(m_rec); }

  rec_t *page_align() noexcept { return Raw_page::align(m_rec); }

  bool is_null() const noexcept { return m_rec == nullptr; }

  bool operator==(const Rec &rhs) const { return m_rec == rhs.m_rec; }

  bool operator!=(const Rec &rhs) const { return m_rec != rhs.m_rec; }

  // Record field operations
  /** The following function is used to get the offset to the nth data field in a record.
   * @param n Index of the field
   * @param len Pointer to store the length of the field
   *            If the field is SQL null, it will be set to UNIV_SQL_NULL
   * @return The offset of the nth field in the record
   */
  ulint get_nth_field_offs(ulint n, ulint *len) const noexcept;

  /** Copies the first n fields of a physical record to a new physical record in a buffer.
   * @param index The record descriptor.
   * @param n_fields The number of fields to copy.
   * @param buf A memory buffer for the copied prefix, or nullptr.
   * @param buf_size The size of the buffer.
   * @return own: Copied record.
   */
  Rec copy_prefix_to_buf(const Index *index, ulint n_fields, byte *&buf, ulint &buf_size) const noexcept;

  /** Copies the prefix of a physical record to a data tuple.
   * @param tuple[out]     The output data tuple.
   * @param index[in]      The record descriptor.
   * @param n_fields[in]   The number of fields to copy.
   * @param heap[in]       The memory heap.
   */
  void copy_prefix_to_dtuple(DTuple *tuple, const Index *index, ulint n_fields, mem_heap_t *heap) const noexcept;

  /** Validates a physical record.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return True if the record is valid, false otherwise.
   */
  bool validate(const ulint *offsets) const noexcept;

  /** Prints the contents of a physical record to log_info(). */
  void log_record_info() const noexcept;

  /** Sets the null bit of the specified field in the given record.
   * @param i The index of the field.
   * @param val The value to set for the null bit.
   */
  void set_nth_field_null_bit(ulint i, bool val) noexcept;

  /** Sets the nth field of a record to SQL NULL.
   * @param n The index of the field to set to SQL NULL.
   */
  void set_nth_field_sql_null(ulint n) noexcept;

  /** Gets the nth field of a record.
   * @param n The index of the field.
   * @param len Pointer to store the length of the field.
   * @return The nth field as a Rec.
   */
  Rec get_nth_field(ulint n, ulint *len) const noexcept { return m_rec + get_nth_field_offs(n, len); }

  /** Gets the nth field of a record with offsets.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @param i The field index.
   * @param len Pointer to store the length of the field.
   * @return The nth field as a Rec.
   */
  Rec get_nth_field(const ulint *offsets, ulint i, ulint *len) const noexcept {
    return m_rec + rec_offs_get_nth_field(offsets, i, len);
  }

  // Bit field operations
  /** Retrieves a bit field from a record.
   * This function retrieves a bit field from a record based on the given offset, mask, and shift.
   * @param offs  Offset from the origin down.
   * @param mask  Mask used to filter bits.
   * @param shift Shift right applied after masking.
   * @return      The retrieved bit field.
   */
  ulint get_bit_field_1(ulint offs, ulint mask, ulint shift) const noexcept {
    return (mach_read_from_1(m_rec - offs) & mask) >> shift;
  }

  /** Sets a bit field within 1 byte.
   * This function sets a bit field in a record by applying a mask and shifting the value.
   * @param val   Value to set.
   * @param offs  Offset from the origin down.
   * @param mask  Mask used to filter bits.
   * @param shift Shift right applied after masking.
   */
  void set_bit_field_1(ulint val, ulint offs, ulint mask, ulint shift) noexcept {
    ut_ad(offs <= REC_N_EXTRA_BYTES);
    ut_ad(mask);
    ut_ad(mask <= 0xFFUL);
    ut_ad(((mask >> shift) << shift) == mask);
    ut_ad(((val << shift) & mask) == (val << shift));

    mach_write_to_1(m_rec - offs, (mach_read_from_1(m_rec - offs) & ~mask) | (val << shift));
  }

  /** Gets a bit field from within 2 bytes.
   * @param offs	offset from the origin down
   * @param mask	mask used to filter bits
   * @param shift	shift right applied after masking
   */
  ulint get_bit_field_2(ulint offs, ulint mask, ulint shift) const noexcept {
    return (mach_read_from_2(m_rec - offs) & mask) >> shift;
  }

  /** Sets a bit field within 2 bytes.
   * @param val   Value to set in the bit field.
   * @param offs  Offset from the origin down.
   * @param mask  Mask used to filter bits.
   * @param shift Shift right applied after masking.
   */
  void set_bit_field_2(ulint val, ulint offs, ulint mask, ulint shift) noexcept {
    ut_ad(offs <= REC_N_EXTRA_BYTES);
    ut_ad(mask > 0xFFUL);
    ut_ad(mask <= 0xFFFFUL);
    ut_ad((mask >> shift) & 1);
    ut_ad(((mask >> shift) & ((mask >> shift) + 1)) == 0);
    ut_ad(((mask >> shift) << shift) == mask);
    ut_ad(((val << shift) & mask) == (val << shift));

    mach_write_to_2(m_rec - offs, (mach_read_from_2(m_rec - offs) & ~mask) | (val << shift));
  }

  // Record navigation
  /** Gets the pointer of the next chained record on the same page.
   * @return	pointer to the next chained record, or nullptr if none
   */
  Rec get_next_ptr_const() const noexcept {
    ut_ad(REC_NEXT_MASK == 0xFFFFUL);
    ut_ad(REC_NEXT_SHIFT == 0);

    const auto field_value = mach_read_from_2(m_rec - REC_NEXT);

    if (unlikely(field_value == 0)) {
      return Rec(nullptr);
    } else {
      ut_ad(field_value < UNIV_PAGE_SIZE);
      return Rec(static_cast<rec_t *>(ut_align_down(m_rec, UNIV_PAGE_SIZE)) + field_value);
    }
  }

  /** Gets the pointer of the next chained record on the same page.
   * @return	pointer to the next chained record, or nullptr if none
   */
  Rec get_next_ptr() const noexcept { return get_next_ptr_const(); }

  /** Calculates the offset of the next record in a physical record.
   * @return	the page offset of the next chained record, or 0 if none
   */
  ulint get_next_offs() const noexcept {
    static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");
    static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");

    const auto field_value = mach_read_from_2(m_rec - REC_NEXT);

    ut_ad(field_value < UNIV_PAGE_SIZE);

    return field_value;
  }

  /** Sets the offset of the next record in a physical record.
   * @param next The offset of the next record.
   */
  void set_next_offs(ulint next) noexcept {
    ut_ad(UNIV_PAGE_SIZE > next);

    static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");
    static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");

    mach_write_to_2(m_rec - REC_NEXT, next);
  }

  // Record properties
  /** Gets the number of fields in a record.
   * @return The number of fields in the record.
   */
  ulint get_n_fields() const noexcept {
    auto ret = get_bit_field_2(REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);

    ut_ad(ret <= REC_MAX_N_FIELDS);
    ut_ad(ret > 0);

    return ret;
  }

  /** Sets the number of fields in a record.
   * @param n_fields The number of fields.
   */
  void set_n_fields(ulint n_fields) noexcept {
    ut_ad(n_fields > 0);
    ut_ad(n_fields <= REC_MAX_N_FIELDS);

    set_bit_field_2(n_fields, REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);
  }

  /** Gets the number of records owned by the previous directory record.
   * @return	number of owned records
   */
  ulint get_n_owned() const noexcept { return get_bit_field_1(REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT); }

  /** Sets the number of owned records.
   * @param n_owned number of owned.
   */
  void set_n_owned(ulint n_owned) noexcept { set_bit_field_1(n_owned, REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT); }

  /** Retrieves the info bits of a record.
   * @return	info bits
   */
  ulint get_info_bits() const noexcept { return get_bit_field_1(REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT); }

  /** Sets the info bits of a record.
   * @param bits The info bits to set.
   */
  void set_info_bits(ulint bits) noexcept { set_bit_field_1(bits, REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT); }

  /** Gets the info and status bits of a record.
   * @return The info and status bits.
   */
  ulint get_info_and_status_bits() const noexcept {
    const auto bits = get_info_bits();

    ut_ad(!(bits & ~(REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)));

    return bits;
  }

  /** Gets the deleted flag of a record.
   * @return The deleted flag.
   */
  ulint get_deleted_flag() const noexcept {
    return unlikely(get_bit_field_1(REC_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT));
  }

  /** Sets the deleted flag of a record.
   * @param deleted The deleted flag value.
   */
  void set_deleted_flag(bool deleted) noexcept {
    auto val = get_info_bits();

    if (deleted) {
      val |= REC_INFO_DELETED_FLAG;
    } else {
      val &= ~REC_INFO_DELETED_FLAG;
    }

    set_info_bits(val);
  }

  /** Gets the heap number of a record.
   * @return The heap number.
   */
  ulint get_heap_no() const noexcept { return get_bit_field_2(REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT); }

  /** Sets the heap number of a record.
   * @param heap_no The heap number.
   */
  void set_heap_no(ulint heap_no) noexcept { set_bit_field_2(heap_no, REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT); }

  /** Gets the 1-byte offsets flag of a record.
   * @return The 1-byte offsets flag.
   */
  bool get_1byte_offs_flag() const noexcept { return get_bit_field_1(REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT); }

  /** Sets the 1-byte offsets flag of a record.
   * @param flag The 1-byte offsets flag.
   */
  void set_1byte_offs_flag(bool flag) noexcept { set_bit_field_1(flag, REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT); }

  // Field operations
  /** Gets the end info of the nth field in a 1-byte format record.
   * @param n The field index.
   * @return The end info of the nth field.
   */
  ulint get_1_field_end_info(ulint n) const noexcept {
    ut_ad(get_1byte_offs_flag());
    ut_ad(n < get_n_fields());

    return mach_read_from_1(m_rec - (REC_N_EXTRA_BYTES + n + 1));
  }

  /** Gets the end info of the nth field in a 2-byte format record.
   * @param n The field index.
   * @return The end info of the nth field.
   */
  ulint get_2_field_end_info(ulint n) const noexcept {
    ut_ad(!get_1byte_offs_flag());
    ut_ad(n < get_n_fields());

    return mach_read_from_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n + 2));
  }

  /** Gets the previous field end info in a 1-byte format record.
   * @param n The field index.
   * @return The previous field end info.
   */
  ulint get_1_prev_field_end_info(ulint n) const noexcept {
    ut_ad(get_1byte_offs_flag());
    ut_ad(n <= get_n_fields());

    return mach_read_from_1(m_rec - (REC_N_EXTRA_BYTES + n));
  }

  /** Gets the previous field end info in a 2-byte format record.
   * @param n The field index.
   * @return The previous field end info.
   */
  ulint get_2_prev_field_end_info(ulint n) const noexcept {
    ut_ad(!get_1byte_offs_flag());
    ut_ad(n <= get_n_fields());

    return mach_read_from_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n));
  }

  /** Sets the end info of the nth field in a 1-byte format record.
   * @param n The field index.
   * @param info The end info to set.
   */
  void set_1_field_end_info(ulint n, ulint info) noexcept {
    ut_ad(get_1byte_offs_flag());
    ut_ad(n < get_n_fields());

    mach_write_to_1(m_rec - (REC_N_EXTRA_BYTES + n + 1), info);
  }

  /** Sets the end info of the nth field in a 2-byte format record.
   * @param n The field index.
   * @param info The end info to set.
   */
  void set_2_field_end_info(ulint n, ulint info) noexcept {
    ut_ad(!get_1byte_offs_flag());
    ut_ad(n < get_n_fields());

    mach_write_to_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n + 2), info);
  }

  /** Gets the start offset of the nth field in a 1-byte format record.
   * @param n The field index.
   * @return The start offset of the nth field.
   */
  ulint get_1_field_start_offs(ulint n) const noexcept {
    ut_ad(get_1byte_offs_flag());
    ut_ad(n <= get_n_fields());

    if (n == 0) {
      return 0;
    } else {
      return get_1_prev_field_end_info(n) & ~REC_1BYTE_SQL_NULL_MASK;
    }
  }

  /** Gets the start offset of the nth field in a 2-byte format record.
   * @param n The field index.
   * @return The start offset of the nth field.
   */
  ulint get_2_field_start_offs(ulint n) const noexcept {
    ut_ad(!get_1byte_offs_flag());
    ut_ad(n <= get_n_fields());

    if (n == 0) {
      return 0;
    } else {
      return get_2_prev_field_end_info(n) & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
    }
  }

  /** Gets the start offset of the nth field in a record.
   * @param n The field index.
   * @return The start offset of the nth field.
   */
  ulint get_field_start_offs(ulint n) const noexcept {
    ut_ad(n <= get_n_fields());

    if (n == 0) {
      return 0;
    } else if (get_1byte_offs_flag()) {
      return get_1_field_start_offs(n);
    } else {
      return get_2_field_start_offs(n);
    }
  }

  /** Gets the size of the nth field in a record.
   * @param n The field index.
   * @return The size of the nth field.
   */
  ulint get_nth_field_size(ulint n) const noexcept {
    auto os = get_field_start_offs(n);
    auto next_os = get_field_start_offs(n + 1);

    ut_ad(next_os - os < UNIV_PAGE_SIZE);

    return next_os - os;
  }

  /** Sets the nth field of a record.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @param n The field index.
   * @param data The data to set.
   * @param len The length of the data.
   */
  void set_nth_field(const ulint *offsets, ulint n, const void *data, ulint len) noexcept {
    ut_ad(offs_validate(nullptr, offsets));

    if (unlikely(len == UNIV_SQL_NULL)) {
      if (!rec_offs_nth_sql_null(offsets, n)) {
        set_nth_field_sql_null(n);
      }

      return;
    }

    ulint dst_len;
    auto dst = m_rec + rec_offs_get_nth_field(offsets, n, &dst_len);

    if (dst_len == UNIV_SQL_NULL) {
      set_nth_field_null_bit(n, false);
      ut_ad(len == get_nth_field_size(n));
    } else {
      ut_ad(dst_len == len);
    }

    memcpy(dst, data, len);
  }

  /** Gets the data size of a record.
   * @return The data size of the record.
   */
  ulint get_data_size() const noexcept { return get_field_start_offs(get_n_fields()); }

  // Record operations
  /** Gets the end of a record.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return The end of the record.
   */
  Rec get_end(const ulint *offsets) const noexcept {
    ut_ad(offs_validate(nullptr, offsets));

    return Rec(m_rec + rec_offs_data_size(offsets));
  }

  /** Gets the start of a record.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return The start of the record.
   */
  Rec get_start(const ulint *offsets) const noexcept {
    ut_ad(offs_validate(nullptr, offsets));

    return Rec(m_rec - rec_offs_extra_size(offsets));
  }

  /** Copies a record to a buffer.
   * @param buf The buffer to copy to.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return The copied record.
   */
  Rec copy(void *buf, const ulint *offsets) const noexcept {
    ut_ad(buf != nullptr);
    ut_ad(offs_validate(nullptr, offsets));
    ut_ad(validate(offsets));

    const auto extra_len = rec_offs_extra_size(offsets);
    const auto data_len = rec_offs_data_size(offsets);

    memcpy(buf, m_rec - extra_len, extra_len + data_len);

    return Rec(static_cast<rec_t *>(buf) + extra_len);
  }

  /** Folds a record for hashing.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @param n_fields The number of fields to fold.
   * @param n_bytes The number of bytes to fold.
   * @param tree_id The tree ID.
   * @return The folded value.
   */
  ulint fold(const ulint *offsets, ulint n_fields, ulint n_bytes, uint64_t tree_id) const noexcept {
    ut_ad(offs_validate(nullptr, offsets));
    ut_ad(validate(offsets));
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
      auto data = get_nth_field(offsets, i, &len);

      if (len != UNIV_SQL_NULL) {
        fold = ut_fold_ulint_pair(fold, ut_fold_binary(data.get(), len));
      }
    }

    if (n_bytes > 0) {
      ulint len;
      auto data = get_nth_field(offsets, n_fields, &len);

      if (len != UNIV_SQL_NULL) {
        if (len > n_bytes) {
          len = n_bytes;
        }

        fold = ut_fold_ulint_pair(fold, ut_fold_binary(data.get(), len));
      }
    }

    return fold;
  }

  /** Checks if any field is null or extern.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return Pointer to the first null or extern field, or nullptr if none.
   */
  const byte *offs_any_null_extern(const ulint *offsets) const noexcept {
    ut_ad(offs_validate(nullptr, offsets));

    if (!rec_offs_any_extern(offsets)) {
      return nullptr;
    }

    for (ulint i = 0; i < rec_offs_n_fields(offsets); i++) {
      if (rec_offs_nth_extern(offsets, i)) {
        ulint len;
        const byte *field = get_nth_field(offsets, i, &len);

        ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE_LOCAL);
        if (!memcmp(field + len - BTR_EXTERN_FIELD_REF_SIZE_LOCAL, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE_LOCAL)) {
          return field;
        }
      }
    }

    return nullptr;
  }

  // Offset operations
  /** Validates the offsets of a record.
   * @param index The index descriptor.
   * @param offsets The array returned by Phy_rec::get_col_offsets().
   * @return True if the offsets are valid, false otherwise.
   */
  bool offs_validate(const Index *index, const ulint *offsets) const noexcept;

  /** Makes the offsets of a record valid.
   * @param index The index descriptor.
   * @param offsets The array to make valid.
   */
  void offs_make_valid(const Index *index, ulint *offsets) const noexcept {
    ut_ad(get_n_fields() >= rec_offs_n_fields(offsets));

    offsets[2] = reinterpret_cast<uintptr_t>(m_rec);
    offsets[3] = reinterpret_cast<uintptr_t>(index);
  }

  // Static factory method
  /** Converts a data tuple to a physical record.
   * @param buf The buffer to store the record.
   * @param index The index descriptor.
   * @param dtuple The data tuple to convert.
   * @param n_ext The number of externally stored columns.
   * @return The converted physical record.
   */
  static Rec convert_dtuple_to_rec(byte *buf, const Index *index, const DTuple *dtuple, ulint n_ext) noexcept;

 private:
  rec_t *m_rec;
};

// Original free functions that were inline - now defined as inline functions
inline std::ostream &operator<<(std::ostream &os, const Rec &rec) noexcept {
  return os << rec.to_string();
}

/** REC_MAX_INDEX_COL_LEN is measured in bytes and is the maximum
indexed column length (or indexed prefix length). It is set to 3*256,
so that one can create a column prefix index on 256 characters of a
TEXT or VARCHAR column also in the UTF-8 charset. In that charset,
a character may take at most 3 bytes.

This constant MUST NOT BE CHANGED, or the compatibility of InnoDB data
files will be at risk! */
constexpr ulint REC_MAX_INDEX_COL_LEN = 768;

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
inline ulint rec_offs_get_nth_field(const ulint *offsets, ulint n, ulint *len) noexcept {
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
