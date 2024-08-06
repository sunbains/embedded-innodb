/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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
#include "mtr0types.h"
#include "page0types.h"
#include "rem0types.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "ut0byte.h"

/** Info bit denoting the predefined minimum record: this bit is set
if and only if the record is the first user record on a non-leaf
B-tree page that is the leftmost page on its level
(PAGE_LEVEL is nonzero and FIL_PAGE_PREV is FIL_NULL). */
constexpr ulint REC_INFO_MIN_REC_FLAG = 0x10UL;

/* The deleted flag in info bits */

/** When bit is set to 1, it means the record has been delete marked */
constexpr ulint REC_INFO_DELETED_FLAG = 0x20UL;

/* Number of extra bytes in an old-style record, in addition to the data and the offsets */
constexpr ulint REC_N_OLD_EXTRA_BYTES = 6;

/* Number of extra bytes in a new-style record, in addition to the data and the offsets */
constexpr ulint REC_N_NEW_EXTRA_BYTES = 5;

/* Record status values */
constexpr ulint REC_STATUS_ORDINARY = 0;
constexpr ulint REC_STATUS_NODE_PTR = 1;
constexpr ulint REC_STATUS_INFIMUM = 2;
constexpr ulint REC_STATUS_SUPREMUM = 3;

/** The offset of heap_no in a compact record */
constexpr ulint REC_NEW_HEAP_NO = 4;

/** The shift of heap_no in a compact record. The status is stored in the low-order bits. */
constexpr ulint REC_HEAP_NO_SHIFT = 3;

/* Length of a B-tree node pointer, in bytes */
constexpr ulint REC_NODE_PTR_SIZE = 4;

#ifdef UNIV_DEBUG
/* Length of the rec_get_offsets() header */
constexpr ulint REC_OFFS_HEADER_SIZE = 4;
#else /* UNIV_DEBUG */
/* Length of the rec_get_offsets() header */
constexpr ulint REC_OFFS_HEADER_SIZE = 2;
#endif /* UNIV_DEBUG */

/* Number of elements that should be initially allocated for the
offsets[] array, first passed to rec_get_offsets() */
constexpr ulint REC_OFFS_NORMAL_SIZE = 100;
constexpr ulint REC_OFFS_SMALL_SIZE = 10;

/** Determine how many of the first n columns in a compact physical record are stored externally.
 *
 * @param rec The compact physical record.
 * @param index The record descriptor.
 * @param n The number of columns to scan.
 * @return	number of externally stored columns */
ulint rec_get_n_extern_new(const rec_t *rec, dict_index_t *index, ulint n);

/**
 * Retrieves the offsets of fields in a physical record. It can reuse a previously allocated array.
 *
 * @param rec The physical record.
 * @param index The record descriptor.
 * @param offsets An array consisting of offsets[0] allocated elements, or an array from rec_get_offsets(), or nullptr.
 * @param n_fields The maximum number of initialized fields (ULINT_UNDEFINED if all fields).
 * @param heap The memory heap.
 * @param file The file name where the function is called.
 * @param line The line number where the function is called.
 * @return the new offsets representing the fields in the record.
 */
ulint *rec_get_offsets_func(const rec_t *rec, const dict_index_t *index, ulint *offsets, ulint n_fields, mem_heap_t **heap, const char *file, ulint line);

#define rec_get_offsets(rec, index, offsets, n, heap) rec_get_offsets_func(rec, index, offsets, n, heap, __FILE__, __LINE__)

/**
 * Determine the offset to each field in a leaf-page record in ROW_FORMAT=COMPACT.
 * This is a special case of rec_init_offsets() and rec_get_offsets_func().
 *
 * @param rec The physical record in ROW_FORMAT=COMPACT.
 * @param extra The number of bytes to reserve between the record header and the data payload (usually REC_N_NEW_EXTRA_BYTES).
 * @param index The record descriptor.
 * @param offsets The array of offsets. On input, it should contain the number of fields (n=rec_offs_n_fields(offsets)).
 */
void rec_init_offsets_comp_ordinary(const rec_t *rec, ulint extra, const dict_index_t *index, ulint *offsets);

/**
 * Calculates the offsets of a compact record in reverse order.
 *
 * Calculate the offsets to each field in the record.  It can reuse a previously allocated array.
 * 
 * Take the extra bytes of a compact record in reverse order, excluding
 * the fixed-size REC_N_NEW_EXTRA_BYTES, and calculates the offsets of the record. The
 * record descriptor is provided through the 'index' parameter. The 'node_ptr' parameter
 * specifies the node pointer, where a non-zero value indicates a node pointer and 0
 * indicates a leaf node. The calculated offsets are stored in the 'offsets' array, which
 * should have enough allocated elements to accommodate the offsets.
 *
 * @param extra The extra bytes of a compact record in reverse order, excluding the fixed-size REC_N_NEW_EXTRA_BYTES.
 * @param index The index with the record.
 * @param node_ptr The node pointer, where a non-zero value indicates a node pointer and 0 indicates a leaf node.
 * @param offsets The array to store the calculated offsets.
 */
void rec_get_offsets_reverse(const byte *extra, const dict_index_t *index, ulint node_ptr, ulint *offsets);

#ifndef UNIV_DEBUG
#define rec_offs_make_valid(rec, index, offsets) ((void)0)
#endif /* UNIV_DEBUG */

/**
 * The following function is used to get the offset to the nth data field in an old-style record.
 *
 * @param rec Pointer to the record
 * @param n Index of the field
 * @param len Pointer to store the length of the field
 *            If the field is SQL null, it will be set to UNIV_SQL_NULL
 * @return The offset of the nth field in the record
 */
ulint rec_get_nth_field_offs_old(const rec_t *rec, ulint n, ulint *len);

#define rec_get_nth_field_old(rec, n, len) ((rec) + rec_get_nth_field_offs_old(rec, n, len))
#define rec_get_nth_field(rec, offsets, n, len) ((rec) + rec_get_nth_field_offs(offsets, n, len))
#define rec_offs_init(offsets) rec_offs_set_n_alloc(offsets, (sizeof offsets) / sizeof *offsets)

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
rec_t *rec_copy_prefix_to_buf(const rec_t *rec, const dict_index_t *index, ulint n_fields, byte *&buf, ulint &buf_size);

/**
 * Builds a ROW_FORMAT=COMPACT record out of a data tuple (normally REC_N_NEW_EXTRA_BYTES).
 *
 * This function converts a tuple to a compressed record using the given record descriptor.
 *
 * @param rec The origin of the record.
 * @param extra The number of bytes to reserve between the record header and the data payload.
 * @param index The record descriptor.
 * @param status The status bits of the record.
 * @param fields An array of data fields.
 * @param n_fields The number of data fields.
 */
void rec_convert_dtuple_to_rec_comp(rec_t *rec, ulint extra, const dict_index_t *index, ulint status, const dfield_t *fields, ulint n_fields);

/**
 * Builds a physical record out of a data tuple and stores it into the given buffer.
 *
 * @param buf The start address of the physical record.
 * @param index The record descriptor.
 * @param dtuple The data tuple to convert.
 * @param n_ext The number of externally stored columns.
 * @return A pointer to the converted record.
 */
rec_t *rec_convert_dtuple_to_rec(byte *buf, const dict_index_t *index, const dtuple_t *dtuple, ulint n_ext);

/**
 * Determines the size of a data tuple prefix in ROW_FORMAT=COMPACT.
 *
 * This function calculates the size of a record with compressed prefix, given the record
 * descriptor, array of data fields, and the number of data fields.
 * The result is stored in the `extra` parameter.
 *
 * @param index The record descriptor. The function assumes that `dict_table_is_comp()` holds, even if it does not.
 * @param fields The array of data fields.
 * @param n_fields The number of data fields.
 * @param extra A pointer to store the extra size.
 *
 * @return	total size
 */
ulint rec_get_converted_size_comp_prefix(const dict_index_t *index, const dfield_t *fields, ulint n_fields, ulint *extra);

/**
 * Determines the size of a data tuple in ROW_FORMAT=COMPACT.
 *
 * @param index Pointer to the record descriptor.
 * @param status Status bits of the record.
 * @param fields Array of data fields.
 * @param n_fields Number of data fields.
 * @param extra Pointer to store the extra size.
 * @return	total size
 */
ulint rec_get_converted_size_comp(const dict_index_t *index, ulint status, const dfield_t *fields, ulint n_fields, ulint *extra);

/**
 * Copies the prefix of a physical record to a data tuple.
 *
 * @param tuple[out]     The output data tuple.
 * @param rec[in]        The input physical record.
 * @param index[in]      The record descriptor.
 * @param n_fields[in]   The number of fields to copy.
 * @param heap[in]       The memory heap.
 */
void rec_copy_prefix_to_dtuple(dtuple_t *tuple, const rec_t *rec, const dict_index_t *index, ulint n_fields, mem_heap_t *heap);

/**
 * Validates a physical record.
 *
 * @param rec The physical record to validate.
 * @param offsets The array returned by rec_get_offsets().
 * @return True if the record is valid, false otherwise.
 */
bool rec_validate(const rec_t *rec, const ulint *offsets);

/**
 * @brief Prints the contents of a physical record to the specified stream.
 *
 * This function prints the contents of a physical record to the specified stream.
 *
 * @param ib_stream The stream where the record should be printed.
 * @param rec The physical record to be printed.
 */
void rec_print_old(ib_stream_t ib_stream, const rec_t *rec);

/** Prints an old-style physical record. */
std::ostream &rec_print_old(std::ostream &os, const rec_t *rec);

/**
 * Prints the given physical record to the specified stream.
 *
 * @param ib_stream The stream where the record should be printed.
 * @param rec The physical record to be printed.
 * @param offsets An array returned by rec_get_offsets().
 */
void rec_print(ib_stream_t ib_stream, const rec_t *rec, dict_index_t *index);

/**
 * @brief Prints the contents of a physical record to the specified output stream.
 *
 * This function prints the contents of a physical record to the specified output stream.
 *
 * @param os The output stream where the record will be printed.
 * @param rec The physical record to be printed.
 * @param index The record descriptor.
 * @return The output stream after printing the record.
 */
std::ostream &rec_print(std::ostream &os, const rec_t *rec, dict_index_t *index);

/** This is single byte bit-field */
constexpr ulint REC_INFO_BITS = 6;

/** Maximum lengths for the data in a physical record if the offsets
are given in one byte (resp. two byte) format. */
constexpr ulint REC_1BYTE_OFFS_LIMIT = 0x7FUL;
constexpr ulint REC_2BYTE_OFFS_LIMIT = 0x7FFFUL;

/** The data size of record must be smaller than this because we reserve
two upmost bits in a two byte offset for special purposes */
constexpr ulint REC_MAX_DATA_SIZE = (16 * 1024);

/** Compact flag ORed to the extra size returned by rec_get_offsets() */
constexpr auto REC_OFFS_COMPACT = ((ulint)1 << 31);

/** SQL nullptr flag in offsets returned by rec_get_offsets() */
constexpr auto REC_OFFS_SQL_NULL = ((ulint)1 << 31);

/** External flag in offsets returned by rec_get_offsets() */
constexpr auto REC_OFFS_EXTERNAL = ((ulint)1 << 30);

/** Mask for offsets returned by rec_get_offsets() */
constexpr ulint REC_OFFS_MASK = (REC_OFFS_EXTERNAL - 1);

/** Offsets of the bit-fields in an old-style record. NOTE! In the table the
most significant bytes and bits are written below less significant.

        (1) byte offset		(2) bit usage within byte
        downward from
        origin ->	1	8 bits pointer to next record
                        2	8 bits pointer to next record
                        3	1 bit short flag
                                7 bits number of fields
                        4	3 bits number of fields
                                5 bits heap number
                        5	8 bits heap number
                        6	4 bits n_owned
                                4 bits info bits
*/

/** Offsets of the bit-fields in a new-style record. NOTE! In the table the
most significant bytes and bits are written below less significant.

        (1) byte offset		(2) bit usage within byte
        downward from
        origin ->	1	8 bits relative offset of next record
                        2	8 bits relative offset of next record
                                  the relative offset is an unsigned 16-bit
                                  integer:
                                  (offset_of_next_record
                                   - offset_of_this_record) mod 64Ki,
                                  where mod is the modulo as a non-negative
                                  number;
                                  we can calculate the offset of the next
                                  record with the formula:
                                  relative_offset + offset_of_this_record
                                  mod UNIV_PAGE_SIZE
                        3	3 bits status:
                                        000=conventional record
                                        001=node pointer record (inside B-tree)
                                        010=infimum record
                                        011=supremum record
                                        1xx=reserved
                                5 bits heap number
                        4	8 bits heap number
                        5	4 bits n_owned
                                4 bits info bits
*/

/* We list the byte offsets from the origin of the record, the mask,
and the shift needed to obtain each bit-field of the record. */

constexpr ulint REC_NEXT = 2;
constexpr ulint REC_NEXT_MASK = 0xFFFFUL;
constexpr ulint REC_NEXT_SHIFT = 0;

constexpr ulint REC_OLD_SHORT = 3; /* This is single byte bit-field */
constexpr ulint REC_OLD_SHORT_MASK = 0x1UL;
constexpr ulint REC_OLD_SHORT_SHIFT = 0;

constexpr ulint REC_OLD_N_FIELDS = 4;
constexpr ulint REC_OLD_N_FIELDS_MASK = 0x7FEUL;
constexpr ulint REC_OLD_N_FIELDS_SHIFT = 1;

constexpr ulint REC_NEW_STATUS = 3; /* This is single byte bit-field */
constexpr ulint REC_NEW_STATUS_MASK = 0x7UL;
constexpr ulint REC_NEW_STATUS_SHIFT = 0;

constexpr ulint REC_OLD_HEAP_NO = 5;
constexpr ulint REC_HEAP_NO_MASK = 0xFFF8UL;

constexpr ulint REC_OLD_N_OWNED = 6; /* This is single byte bit-field */
constexpr ulint REC_NEW_N_OWNED = 5; /* This is single byte bit-field */
constexpr ulint REC_N_OWNED_MASK = 0xFUL;
constexpr ulint REC_N_OWNED_SHIFT = 0;

constexpr ulint REC_OLD_INFO_BITS = 6; /* This is single byte bit-field */
constexpr ulint REC_NEW_INFO_BITS = 5; /* This is single byte bit-field */
constexpr ulint REC_INFO_BITS_MASK = 0xF0UL;
constexpr ulint REC_INFO_BITS_SHIFT = 0;

/* The following masks are used to filter the SQL null bit from
one-byte and two-byte offsets */

constexpr ulint REC_1BYTE_SQL_NULL_MASK = 0x80UL;
constexpr ulint REC_2BYTE_SQL_NULL_MASK = 0x8000UL;

/* In a 2-byte offset the second most significant bit denotes
a field stored to another page: */

constexpr ulint REC_2BYTE_EXTERN_MASK = 0x4000UL;

static_assert(!(REC_OLD_SHORT_MASK << (8 * (REC_OLD_SHORT - 3)) ^ REC_OLD_N_FIELDS_MASK << (8 * (REC_OLD_N_FIELDS - 4)) ^
  REC_HEAP_NO_MASK << (8 * (REC_OLD_HEAP_NO - 4)) ^ REC_N_OWNED_MASK << (8 * (REC_OLD_N_OWNED - 3)) ^ 
  REC_INFO_BITS_MASK << (8 * (REC_OLD_INFO_BITS - 3)) ^ 0xFFFFFFFFUL),
  "#error sum of old-style masks != 0xFFFFFFFFUL");

static_assert(!(REC_NEW_STATUS_MASK << (8 * (REC_NEW_STATUS - 3)) ^ REC_HEAP_NO_MASK << (8 * (REC_NEW_HEAP_NO - 4)) ^ \
  REC_N_OWNED_MASK << (8 * (REC_NEW_N_OWNED - 3)) ^ REC_INFO_BITS_MASK << (8 * (REC_NEW_INFO_BITS - 3)) ^ 0xFFFFFFUL),
  "error sum of new-style masks != 0xFFFFFFUL");

/**
 * Prints a physical record.
 * 
 * @param[in,out] ib_stream	stream where to print
 * @param[in] rec		physical record
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return  stream after printing
 */
std::ostream &rec_print_new(std::ostream &os, const rec_t *rec, const ulint *offsets);

/**
 * Prints a physical record.
 * 
 * @param[in,out] ib_stream	stream where to print
 * @param[in] rec		physical record
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return  stream after printing
 */
void rec_print_new(ib_stream_t stream, const rec_t *rec, const ulint *offsets);

/**
 * Sets the null bit of the specified field in the given record.
 *
 * @param rec The record to modify.
 * @param i The index of the field.
 * @param val The value to set for the null bit.
 */
void rec_set_nth_field_null_bit(rec_t *rec, ulint i, bool val);

/**
 * Sets the nth field of a record to SQL NULL.
 *
 * @param rec The record to modify.
 * @param n The index of the field to set to SQL NULL.
 */
void rec_set_nth_field_sql_null(rec_t *rec, ulint n);

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
inline ulint rec_get_bit_field_1(const rec_t *rec, ulint offs, ulint mask, ulint shift) {
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
inline void rec_set_bit_field_1(rec_t *rec, ulint val, ulint offs, ulint mask, ulint shift) {
  ut_ad(offs <= REC_N_OLD_EXTRA_BYTES);
  ut_ad(mask);
  ut_ad(mask <= 0xFFUL);
  ut_ad(((mask >> shift) << shift) == mask);
  ut_ad(((val << shift) & mask) == (val << shift));

  mach_write_to_1(rec - offs, (mach_read_from_1(rec - offs) & ~mask) | (val << shift));
}

/** Gets a bit field from within 2 bytes. */
inline ulint rec_get_bit_field_2(
  const rec_t *rec, /*!< in: pointer to record origin */
  ulint offs,       /*!< in: offset from the origin down */
  ulint mask,       /*!< in: mask used to filter bits */
  ulint shift
) /*!< in: shift right applied after masking */
{
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
inline void rec_set_bit_field_2(rec_t *rec, ulint val, ulint offs, ulint mask, ulint shift) {
  ut_ad(offs <= REC_N_OLD_EXTRA_BYTES);
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
 * This function is used to get the pointer to the next record in a compact page format.
 * It takes a physical record and a flag indicating whether the page format is compact or not.
 *
 * @param rec The physical record.
 * @param comp Flag indicating whether the page format is compact or not.
 * @return	pointer to the next chained record, or nullptr if none
 */
inline const rec_t *rec_get_next_ptr_const(const rec_t *rec, ulint comp) {
  ut_ad(REC_NEXT_MASK == 0xFFFFUL);
  ut_ad(REC_NEXT_SHIFT == 0);

  auto field_value = mach_read_from_2(rec - REC_NEXT);

  if (unlikely(field_value == 0)) {
    return nullptr;
  }

  if (expect(comp, REC_OFFS_COMPACT)) {
#if UNIV_PAGE_SIZE <= 32768
    /* Note that for 64 KiB pages, field_value can 'wrap around'
    and the debug assertion is not valid */

    /* In the following assertion, field_value is interpreted
    as signed 16-bit integer in 2's complement arithmetics.
    If all platforms defined int16_t in the standard headers,
    the expression could be written simpler as
    (int16_t) field_value + ut_align_offset(...) < UNIV_PAGE_SIZE
    */
    ut_ad((field_value >= 32768 ? field_value - 65536 : field_value) + ut_align_offset(rec, UNIV_PAGE_SIZE) < UNIV_PAGE_SIZE);
#endif /* UNIV_PAGE_SIZE <= 32768 */

    /* There must be at least REC_N_NEW_EXTRA_BYTES + 1
    between each record. */
    ut_ad((field_value > REC_N_NEW_EXTRA_BYTES && field_value < 32768) || field_value < (uint16_t)-REC_N_NEW_EXTRA_BYTES);

    return (byte *)ut_align_down(rec, UNIV_PAGE_SIZE) + ut_align_offset(rec + field_value, UNIV_PAGE_SIZE);
  } else {
    ut_ad(field_value < UNIV_PAGE_SIZE);

    return (byte *)ut_align_down(rec, UNIV_PAGE_SIZE) + field_value;
  }
}

/**
 * The following function is used to get the pointer of the next chained record on the same page.
 *
 * @param rec The current physical record.
 * @param comp A flag indicating whether the page format is compact or not.
 *             Nonzero value indicates compact format.
 * @return	pointer to the next chained record, or nullptr if none
 */
inline rec_t *rec_get_next_ptr(rec_t *rec, ulint comp) {
  return ((rec_t *)rec_get_next_ptr_const(rec, comp));
}

/**
 * Calculates the offset of the next record in a physical record.
 *
 * @param rec The physical record.
 * @param comp Nonzero value indicates compact page format.
 * @return	the page offset of the next chained record, or 0 if none
 */
inline ulint rec_get_next_offs(const rec_t *rec, ulint comp) {
  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");
  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");

  auto field_value = mach_read_from_2(rec - REC_NEXT);

  if (expect(comp, REC_OFFS_COMPACT)) {
#if UNIV_PAGE_SIZE <= 32768
    /* Note that for 64 KiB pages, field_value can 'wrap around'
    and the debug assertion is not valid */

    /* In the following assertion, field_value is interpreted
    as signed 16-bit integer in 2's complement arithmetics.
    If all platforms defined int16_t in the standard headers,
    the expression could be written simpler as
    (int16_t) field_value + ut_align_offset(...) < UNIV_PAGE_SIZE
    */
    ut_ad((field_value >= 32768 ? field_value - 65536 : field_value) + ut_align_offset(rec, UNIV_PAGE_SIZE) < UNIV_PAGE_SIZE);
#endif
    if (unlikely(field_value == 0)) {

      return 0;
    }

    /* There must be at least REC_N_NEW_EXTRA_BYTES + 1
    between each record. */
    ut_ad((field_value > REC_N_NEW_EXTRA_BYTES && field_value < 32768) || field_value < (uint16_t)-REC_N_NEW_EXTRA_BYTES);

    return ut_align_offset(rec + field_value, UNIV_PAGE_SIZE);
  } else {
    ut_ad(field_value < UNIV_PAGE_SIZE);

    return field_value;
  }
}

/**
 * Sets the offset of the next record in an old-style physical record.
 *
 * @param rec The old-style physical record.
 * @param next The offset of the next record.
 */
inline void rec_set_next_offs_old(rec_t *rec, ulint next) {
  ut_ad(UNIV_PAGE_SIZE > next);

  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");
  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");

  mach_write_to_2(rec - REC_NEXT, next);
}

/**
 * The following function is used to set the next record offset field
 * of a new-style record.
 *
 * @param rec Pointer to the new-style physical record.
 * @param next Offset of the next record.
 */
inline void rec_set_next_offs_new(rec_t *rec, ulint next) {
  ulint field_value;

  ut_ad(UNIV_PAGE_SIZE > next);

  if (unlikely(!next)) {
    field_value = 0;
  } else {
    /* The following two statements calculate
    next - offset_of_rec mod 64Ki, where mod is the modulo
    as a non-negative number */

    field_value = (ulint)((lint)next - (lint)ut_align_offset(rec, UNIV_PAGE_SIZE));
    field_value &= REC_NEXT_MASK;
  }

  mach_write_to_2(rec - REC_NEXT, field_value);
}

/**
 * The following function is used to get the number of fields
 * in an old-style record.
 *
 * @param rec The physical record.
 * @return The number of fields in the record.
 */
inline ulint rec_get_n_fields_old(const rec_t *rec) {
  auto ret = rec_get_bit_field_2(rec, REC_OLD_N_FIELDS, REC_OLD_N_FIELDS_MASK, REC_OLD_N_FIELDS_SHIFT);

  ut_ad(ret <= REC_MAX_N_FIELDS);
  ut_ad(ret > 0);

  return ret;
}

/**
 * The following function is used to set the number of fields in an old-style record.
 *
 * @param rec The physical record.
 * @param n_fields The number of fields.
 */
inline void rec_set_n_fields_old(rec_t *rec, ulint n_fields) {
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);

  rec_set_bit_field_2(rec, n_fields, REC_OLD_N_FIELDS, REC_OLD_N_FIELDS_MASK, REC_OLD_N_FIELDS_SHIFT);
}

/** The following function retrieves the status bits of a new-style record.
 * @param[in]  rec	physical record
 * @return	status bits */
inline ulint rec_get_status(const rec_t *rec) {
  auto ret = rec_get_bit_field_1(rec, REC_NEW_STATUS, REC_NEW_STATUS_MASK, REC_NEW_STATUS_SHIFT);
  ut_ad((ret & ~REC_NEW_STATUS_MASK) == 0);

  return ret;
}

/**
 * The following function is used to get the number of fields in a record.
 * 
 * @param[in] rec	physical record
 * @param[in] dict_index	record descriptor
 * 
 * @return	number of data fields
 */
inline ulint rec_get_n_fields(const rec_t *rec, const dict_index_t *dict_index) {
  if (!dict_table_is_comp(dict_index->table)) {
    return rec_get_n_fields_old(rec);
  }

  switch (rec_get_status(rec)) {
    case REC_STATUS_ORDINARY:
      return dict_index_get_n_fields(dict_index);
    case REC_STATUS_NODE_PTR:
      return dict_index_get_n_unique_in_tree(dict_index) + 1;
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      return 1;
    default:
      ut_error;
      return ULINT_UNDEFINED;
  }
}

/**
 * The following function is used to get the number of records owned by the
 * previous directory record.
 * 
 * @param[in] rec	old-style physical record
 * 
 * @return	number of owned records
 */
inline ulint rec_get_n_owned_old(const rec_t *rec) {
  return rec_get_bit_field_1(rec, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/**
 * The following function is used to set the number of owned records.
 * 
 * @param[in,out] rec old-style physical record 
 * @param[in] n_owned number of owned. */
inline void rec_set_n_owned_old(rec_t *rec, ulint n_owned) {
  rec_set_bit_field_1(rec, n_owned, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/**
 * The following function is used to get the number of records owned by the
 * previous directory record.
 * 
 * @param[in] rec	new-style physical record
 * 
 * @return	number of owned records
 */
inline ulint rec_get_n_owned_new(const rec_t *rec) {
  return rec_get_bit_field_1(rec, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/**
 * The following function is used to set the number of owned records.
 * 
 * @param[in,out] rec new-style physical record
 * @param[in] n_owned number of owned
 */
inline void rec_set_n_owned_new(rec_t *rec, ulint n_owned) {
  rec_set_bit_field_1(rec, n_owned, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/** The following function is used to retrieve the info bits of a record.
 * 
 * @param[in] rec	physical record
 * @param[in] comp	nonzero=compact page format
 * 
 * @return	info bits
 */
inline ulint rec_get_info_bits(const rec_t *rec, ulint comp) {
  return rec_get_bit_field_1(rec, comp ? REC_NEW_INFO_BITS : REC_OLD_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/**
 * The following function is used to set the info bits of a record.
 * 
 * @param[in,out] rec	old-style physical record
 * @param[in] bits	info bits
 */
inline void rec_set_info_bits_old(rec_t *rec, ulint bits) {
  rec_set_bit_field_1(rec, bits, REC_OLD_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/**
 * The following function is used to set the info bits of a record.
 * 
 * @param[in,out] rec	new-style physical record
 * @param[in] bits	info bits
 */
inline void rec_set_info_bits_new(rec_t *rec, ulint bits) {
  rec_set_bit_field_1(rec, bits, REC_NEW_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/**
 * The following function is used to set the status bits of a new-style record.
 * 
 * @param[in,out] rec	physical record
 * @param[in] bits	info bits
 */
inline void rec_set_status(rec_t *rec, ulint bits) {
  rec_set_bit_field_1(rec, bits, REC_NEW_STATUS, REC_NEW_STATUS_MASK, REC_NEW_STATUS_SHIFT);
}

/**
 * The following function is used to retrieve the info and status
 * bits of a record.  (Only compact records have status bits.)
 * 
 * @param[in] rec	physical record
 * @param[in] comp	nonzero=compact page format
 * 
 * @return	info bits
 */
inline ulint rec_get_info_and_status_bits(const rec_t *rec, ulint comp) {
  static_assert(!((REC_NEW_STATUS_MASK >> REC_NEW_STATUS_SHIFT) & (REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)),
                "error REC_NEW_STATUS_MASK and REC_INFO_BITS_MASK overlap");

  ulint bits;

  if (expect(comp, REC_OFFS_COMPACT)) {
    bits = rec_get_info_bits(rec, true) | rec_get_status(rec);
  } else {
    bits = rec_get_info_bits(rec, false);
    ut_ad(!(bits & ~(REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)));
  }

  return bits;
}

/** The following function is used to set the info and status
 * bits of a record.  (Only compact records have status bits.)
 * 
 * @param[in,out] rec	physical record
 * @param[in] bits	info bits
 */
inline void rec_set_info_and_status_bits(rec_t *rec, ulint bits) {

  static_assert(!((REC_NEW_STATUS_MASK >> REC_NEW_STATUS_SHIFT) & (REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)),
                "error REC_NEW_STATUS_MASK and REC_INFO_BITS_MASK overlap");

  rec_set_status(rec, bits & REC_NEW_STATUS_MASK);
  rec_set_info_bits_new(rec, bits & ~REC_NEW_STATUS_MASK);
}

/**
 * The following function tells if record is delete marked.
 * 
 * @param[in,out] rec	physical record
 * @param[in] comp	nonzero=compact page format
 * 
 * @return	nonzero if delete marked */
inline ulint rec_get_deleted_flag(const rec_t *rec, ulint comp) {
  if (expect(comp, REC_OFFS_COMPACT)) {
    return unlikely(rec_get_bit_field_1(rec, REC_NEW_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT));
  } else {
    return unlikely(rec_get_bit_field_1(rec, REC_OLD_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT));
  }
}

/**
 * The following function is used to set the deleted bit.
 * 
 * @param[in,out] rec	old-style physical record
 * @param[in] flag	nonzero if delete marked
 */
inline void rec_set_deleted_flag_old(rec_t *rec, ulint flag) {
  auto val = rec_get_info_bits(rec, false);

  if (flag) {
    val |= REC_INFO_DELETED_FLAG;
  } else {
    val &= ~REC_INFO_DELETED_FLAG;
  }

  rec_set_info_bits_old(rec, val);
}

/**
 * The following function is used to set the deleted bit.
 * 
 * @param[in,out] rec	new-style physical record
 * @param[in] flag	nonzero if delete marked
 */
inline void rec_set_deleted_flag_new(rec_t *rec, ulint flag) {
  auto val = rec_get_info_bits(rec, true);

  if (flag != 0) {
    val |= REC_INFO_DELETED_FLAG;
  } else {
    val &= ~REC_INFO_DELETED_FLAG;
  }

  rec_set_info_bits_new(rec, val);
}

/**
 * The following function tells if a new-style record is a node pointer.
 * 
 * @param[in] rec	physical record
 * @return	true if node pointer
 */
inline bool rec_get_node_ptr_flag(const rec_t *rec) {
  return REC_STATUS_NODE_PTR == rec_get_status(rec);
}

/**
 * The following function is used to get the order number
 * of an old-style record in the heap of the index page.
 * 
 * @param[in] rec	physical record
 * 
 * @return	heap order number
 */
inline ulint rec_get_heap_no_old(const rec_t *rec) {
  return rec_get_bit_field_2(rec, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to set the heap number field in an old-style record.
 * 
 * @param[in,out] rec	physical record
 * @param[in] heap_no	heap number
 */
inline void rec_set_heap_no_old(rec_t *rec, ulint heap_no) {
  rec_set_bit_field_2(rec, heap_no, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to get the order number
 * of a new-style record in the heap of the index page.
 * 
 * @param[in] rec	physical record
 * 
 * @return	heap order number
 */
inline ulint rec_get_heap_no_new(const rec_t *rec) {
  return rec_get_bit_field_2(rec, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to set the heap number
 * field in a new-style record.
 * 
 * @param[in,out] rec	physical record
 * @param[in] heap_no	heap number
 */
inline void rec_set_heap_no_new(rec_t *rec, ulint heap_no) {
  rec_set_bit_field_2(rec, heap_no, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/**
 * The following function is used to test whether the data offsets in the
 * record are stored in one-byte or two-byte format.
 * 
 * @param[in] rec	physical record
 * 
 * @return	true if 1-byte form
 */
inline bool rec_get_1byte_offs_flag(const rec_t *rec) {
  return rec_get_bit_field_1(rec, REC_OLD_SHORT, REC_OLD_SHORT_MASK, REC_OLD_SHORT_SHIFT);
}

/**
 * The following function is used to set the 1-byte offsets flag.
 * 
 * @param[in,out] rec	physical record
 * @param[in] flag	true if 1-byte form
 */
inline void rec_set_1byte_offs_flag(rec_t *rec, bool flag) {
  rec_set_bit_field_1(rec, flag, REC_OLD_SHORT, REC_OLD_SHORT_MASK, REC_OLD_SHORT_SHIFT);
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
inline ulint rec_1_get_field_end_info(const rec_t *rec, ulint n) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  return mach_read_from_1(rec - (REC_N_OLD_EXTRA_BYTES + n + 1));
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
inline ulint rec_2_get_field_end_info(const rec_t *rec, ulint n) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  return mach_read_from_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n + 2));
}

/**
 * Get the base address of offsets.  The extra_size is stored at
 * this position, and following positions hold the end offsets of
 * the fields. */
#define rec_offs_base(offsets) (offsets + REC_OFFS_HEADER_SIZE)

/**
 * The following function returns the number of allocated elements
 * for an array of offsets.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	number of elements
 */
inline ulint rec_offs_get_n_alloc(const ulint *offsets) {
  ut_ad(offsets != nullptr);

  auto n_alloc = offsets[0];

  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);
  UNIV_MEM_ASSERT_W(offsets, n_alloc * sizeof *offsets);

  return n_alloc;
}

/**
 * The following function sets the number of allocated elements
 * for an array of offsets.
 * 
 * @param[out] offsets	array for rec_get_offsets(), must be allocated
 * @param[in] n_alloc	number of elements
 */
inline void rec_offs_set_n_alloc(ulint *offsets, ulint n_alloc) {
  ut_ad(offsets != nullptr );
  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);

  UNIV_MEM_ASSERT_AND_ALLOC(offsets, n_alloc * sizeof *offsets);

  offsets[0] = n_alloc;
}

/**
 * The following function returns the number of fields in a record.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	number of fields */
inline ulint rec_offs_n_fields(const ulint *offsets) {
  ut_ad(offsets != nullptr);

  auto n_fields = offsets[1];

  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields + REC_OFFS_HEADER_SIZE <= rec_offs_get_n_alloc(offsets));

  return n_fields;
}

/**
 * Validates offsets returned by rec_get_offsets().
 * 
 * @param[in] rec	record or nullptr
 * @param[in] dict_index	record descriptor or nullptr
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	true if valid
 */
inline bool rec_offs_validate(const rec_t *rec, const dict_index_t *dict_index, const ulint *offsets) {
  ulint i = rec_offs_n_fields(offsets);
  ulint last = ULINT_MAX;
  ulint comp = *rec_offs_base(offsets) & REC_OFFS_COMPACT;

  if (rec != nullptr) {
    ut_ad((ulint)rec == offsets[2]);

    if (comp == 0) {
      ut_a(rec_get_n_fields_old(rec) >= i);
    }
  }
  if (dict_index != nullptr) {
    ulint max_n_fields;

    ut_ad((ulint)dict_index == offsets[3]);

    max_n_fields = ut_max(dict_index_get_n_fields(dict_index), dict_index_get_n_unique_in_tree(dict_index) + 1);

    if (comp > 0 && rec != nullptr) {
      switch (rec_get_status(rec)) {
        case REC_STATUS_ORDINARY:
          break;
        case REC_STATUS_NODE_PTR:
          max_n_fields = dict_index_get_n_unique_in_tree(dict_index) + 1;
          break;
        case REC_STATUS_INFIMUM:
        case REC_STATUS_SUPREMUM:
          max_n_fields = 1;
          break;
        default:
          ut_error;
      }
    }
    /* dict_index->n_def == 0 for dummy indexes if !comp */
    ut_a(!comp || dict_index->n_def);
    ut_a(!dict_index->n_def || i <= max_n_fields);
  }

  while (i--) {
    const ulint curr = rec_offs_base(offsets)[1 + i] & REC_OFFS_MASK;
    ut_a(curr <= last);
    last = curr;
  }

  return true;
}

#ifdef UNIV_DEBUG
/**
 * Updates debug data in offsets, in order to avoid bogus
 * rec_offs_validate() failures.
 * 
 * @param[in] rec	record
 * @param[in] index	record descriptor
 * @param[out] offsets	array returned by rec_get_offsets()
 */
inline void rec_offs_make_valid(const rec_t *rec, const dict_index_t *index, ulint *offsets) {
  ut_ad(index);
  ut_ad(offsets);
  ut_ad(rec_get_n_fields(rec, index) >= rec_offs_n_fields(offsets));

  offsets[2] = (ulint)rec;
  offsets[3] = (ulint)index;
}
#endif /* UNIV_DEBUG */

/**
 * The following function is used to get an offset to the nth
 * data field in a record.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n	field index
 * @param[out] len	length of the field; UNIV_SQL_NULL if SQL null
 * 
 * @return	offset from the origin of rec
 */
inline ulint rec_get_nth_field_offs(const ulint *offsets, ulint n, ulint *len) {
  ulint offs;

  ut_ad(len > 0);
  ut_ad(n < rec_offs_n_fields(offsets));

  if (unlikely(n == 0)) {
    offs = 0;
  } else {
    offs = rec_offs_base(offsets)[n] & REC_OFFS_MASK;
  }

  auto length = rec_offs_base(offsets)[1 + n];

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
 * Determine if the offsets are for a record in the new compact format.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * @return	nonzero if compact format
 */
inline ulint rec_offs_comp(const ulint *offsets) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

  return *rec_offs_base(offsets) & REC_OFFS_COMPACT;
}

/**
 * Determine if the offsets are for a record containing externally stored columns.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	nonzero if externally stored
 */
inline ulint rec_offs_any_extern(const ulint *offsets) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

  return unlikely(*rec_offs_base(offsets) & REC_OFFS_EXTERNAL);
}

/**
 * Returns nonzero if the extern bit is set in nth field of rec.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n	field index
 * 
 * @return	nonzero if externally stored */
inline ulint rec_offs_nth_extern(const ulint *offsets, ulint n) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  return unlikely(rec_offs_base(offsets)[1 + n] & REC_OFFS_EXTERNAL);
}

/**
 * Returns nonzero if the SQL nullptr bit is set in nth field of rec.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n	field index
 * 
 * @return	nonzero if SQL nullptr
 */
inline ulint rec_offs_nth_sql_null(const ulint *offsets, ulint n) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  return unlikely(rec_offs_base(offsets)[1 + n] & REC_OFFS_SQL_NULL);
}

/**
 * Gets the physical size of a field.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n	field index
 * 
 * @return	length of field
 */
inline ulint rec_offs_nth_size(const ulint *offsets, ulint n) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));

  if (n == 0) {
    return rec_offs_base(offsets)[1 + n] & REC_OFFS_MASK;
  } else {
    return (rec_offs_base(offsets)[1 + n] - rec_offs_base(offsets)[n]) & REC_OFFS_MASK;
  }
}

/**
 * Returns the number of extern bits set in a record.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	number of externally stored fields
 */
inline ulint rec_offs_n_extern(const ulint *offsets) {
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
inline ulint rec_1_get_prev_field_end_info(const rec_t *rec, ulint n) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  return mach_read_from_1(rec - (REC_N_OLD_EXTRA_BYTES + n));
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
inline ulint rec_2_get_prev_field_end_info(const rec_t *rec, ulint n) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  return mach_read_from_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n));
}

/**
 * Sets the field end info for the nth field if the record is stored in the 1-byte format.
 * 
 * @param[in,out] rec	physical record
 * @param[in] n	field index
 * @param[in] info	value to set
 * 
 */
inline void rec_1_set_field_end_info(rec_t *rec, ulint n, ulint info) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  mach_write_to_1(rec - (REC_N_OLD_EXTRA_BYTES + n + 1), info);
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
inline void rec_2_set_field_end_info(rec_t *rec, ulint n, ulint info) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  mach_write_to_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n + 2), info);
}

/**
 * Returns the offset of nth field start if the record is stored in the 1-byte offsets form.
 * 
 * @param[in] rec	physical record
 * @param[in] n	field index
 * 
 * @return	offset of the start of the field
 */
inline ulint rec_1_get_field_start_offs(const rec_t *rec, ulint n) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

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
inline ulint rec_2_get_field_start_offs(const rec_t *rec, ulint n) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

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
inline ulint rec_get_field_start_offs(const rec_t *rec, ulint n) {
  ut_ad(n <= rec_get_n_fields_old(rec));

  if (n == 0) {
    return 0;
  } else if (rec_get_1byte_offs_flag(rec)) {
    return rec_1_get_field_start_offs(rec, n);
  } else {
    return rec_2_get_field_start_offs(rec, n);
  }
}

/**
 * Gets the physical size of an old-style field. Also an SQL null may have a
 * field of size > 0, if the data type is of a fixed size.
 * 
 * @param[in] rec	physical record
 * @param[in] n	field index
 * 
 * @return	field size in bytes
 */
inline ulint rec_get_nth_field_size(const rec_t *rec, ulint n) {
  auto os = rec_get_field_start_offs(rec, n);
  auto next_os = rec_get_field_start_offs(rec, n + 1);

  ut_ad(next_os - os < UNIV_PAGE_SIZE);

  return next_os - os;
}

/**
 * This is used to modify the value of an already existing field in a record.
 * The previous value must have exactly the same size as the new value. If len
 * is UNIV_SQL_NULL then the field is treated as an SQL null.
 * For records in ROW_FORMAT=COMPACT (new-style records), len must not be
 * UNIV_SQL_NULL unless the field already is SQL null.
 * 
 * @param[in,out] rec	physical record
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n	field index
 * @param[in] data	pointer to the data
 * @param[in] len	length of the data or UNIV_SQL_NULL
 */
inline void rec_set_nth_field(rec_t *rec, const ulint *offsets, ulint n, const void *data, ulint len) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  if (unlikely(len == UNIV_SQL_NULL)) {
    if (!rec_offs_nth_sql_null(offsets, n)) {
      ut_a(!rec_offs_comp(offsets));
      rec_set_nth_field_sql_null(rec, n);
    }

    return;
  }

  ulint len2;
  auto data2 = rec_get_nth_field(rec, offsets, n, &len2);

  if (len2 == UNIV_SQL_NULL) {
    ut_ad(!rec_offs_comp(offsets));
    rec_set_nth_field_null_bit(rec, n, false);
    ut_ad(len == rec_get_nth_field_size(rec, n));
  } else {
    ut_ad(len2 == len);
  }

  memcpy(data2, data, len);
}

/**
 * The following function returns the data size of an old-style physical
 * record, that is the sum of field lengths. SQL null fields
 * are counted as length 0 fields. The value returned by the function
 * is the distance from record origin to record end in bytes.
 * 
 * @param[in] rec	physical record
 * 
 * @return	size
 */
inline ulint rec_get_data_size_old(const rec_t *rec) {
  return rec_get_field_start_offs(rec, rec_get_n_fields_old(rec));
}

/**
 * The following function sets the number of fields in offsets.
 * 
 * @param[out] offsets	array returned by rec_get_offsets()
 * @param[in] n_fields	number of fields
 * 
 */
inline void rec_offs_set_n_fields(ulint *offsets, ulint n_fields) {
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
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	size
 */
inline ulint rec_offs_data_size(const ulint *offsets) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

  auto size = rec_offs_base(offsets)[rec_offs_n_fields(offsets)] & REC_OFFS_MASK;

  ut_ad(size < UNIV_PAGE_SIZE);

  return size;
}

/**
 * Returns the total size of record minus data size of record. The value
 * returned by the function is the distance from record start to record origin
 * in bytes.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	size
 */
inline ulint rec_offs_extra_size(const ulint *offsets) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));

  auto size = *rec_offs_base(offsets) & ~(REC_OFFS_COMPACT | REC_OFFS_EXTERNAL);

  ut_ad(size < UNIV_PAGE_SIZE);

  return size;
}

/**
 * Returns the total size of a physical record.
 * 
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	size
 */
inline ulint rec_offs_size(const ulint *offsets) {
  return rec_offs_data_size(offsets) + rec_offs_extra_size(offsets);
}

/**
 * Returns a pointer to the end of the record.
 * 
 * @param[in] rec	pointer to record
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	pointer to end */
inline byte *rec_get_end(rec_t *rec, const ulint *offsets) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  return rec + rec_offs_data_size(offsets);
}

/**
 * Returns a pointer to the start of the record.
 * 
 * @param[in] rec	pointer to record
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	pointer to start
 */
inline byte *rec_get_start(rec_t *rec, const ulint *offsets) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  return rec - rec_offs_extra_size(offsets);
}

/**
 * Copies a physical record to a buffer.
 * 
 * @param[out] buf	buffer
 * @param[in] rec	physical record
 * @param[in] offsets	array returned by rec_get_offsets()
 * 
 * @return	pointer to the origin of the copy
 */
inline rec_t *rec_copy(void *buf, const rec_t *rec, const ulint *offsets) {
  ut_ad(buf != nullptr);
  ut_ad(rec_offs_validate((rec_t *)rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));

  auto extra_len = rec_offs_extra_size(offsets);
  auto data_len = rec_offs_data_size(offsets);

  memcpy(buf, rec - extra_len, extra_len + data_len);

  return static_cast<byte *>(buf) + extra_len;
}

/**
 * Returns the extra size of an old-style physical record if we know its
 * data size and number of fields.
 * 
 * @param[in] data_size	data size
 * @param[in] n_fields	number of fields
 * @param[in] n_ext	number of externally stored columns
 * 
 * @return	extra size
 */
inline ulint rec_get_converted_extra_size(ulint data_size, ulint n_fields, ulint n_ext) {
  if (!n_ext && data_size <= REC_1BYTE_OFFS_LIMIT) {
    return REC_N_OLD_EXTRA_BYTES + n_fields;
  } else {
    return REC_N_OLD_EXTRA_BYTES + 2 * n_fields;
  }
}

/**
 * The following function returns the size of a data tuple when converted to
 * a physical record.
 * 
 * @param[in,out] dict_index	record descriptor
 * @param[in] dtuple	data tuple
 * @param[in] n_ext	number of externally stored columns
 * 
 * @return	size
 */
inline ulint rec_get_converted_size(dict_index_t *dict_index, const dtuple_t *dtuple, ulint n_ext) {
  ut_ad(dtuple != nullptr);
  ut_ad(dict_index != nullptr);
  ut_ad(dtuple_check_typed(dtuple));

  ut_ad(
    dtuple_get_n_fields(dtuple) == (((dtuple_get_info_bits(dtuple) & REC_NEW_STATUS_MASK) == REC_STATUS_NODE_PTR)
                                      ? dict_index_get_n_unique_in_tree(dict_index) + 1
                                      : dict_index_get_n_fields(dict_index))
  );

  if (dict_table_is_comp(dict_index->table)) {
    return rec_get_converted_size_comp(
      dict_index, dtuple_get_info_bits(dtuple) & REC_NEW_STATUS_MASK, dtuple->fields, dtuple->n_fields, nullptr
    );
  }

  auto data_size = dtuple_get_data_size(dtuple, 0);

  auto extra_size = rec_get_converted_extra_size(data_size, dtuple_get_n_fields(dtuple), n_ext);

  return data_size + extra_size;
}

/**
 * Folds a prefix of a physical record to a ulint. Folds only existing fields,
 * that is, checks that we do not run out of the record.
 * 
 * @param[in] rec	physical record
 * @param[in] offsets	array returned by rec_get_offsets()
 * @param[in] n_fields	number of complete fields to fold
 * @param[in] n_bytes	number of bytes to fold in an incomplete last field
 * @param[in] tree_id	index tree id
 * 
 * @return	the folded value
 */
inline ulint rec_fold(const rec_t *rec, const ulint *offsets, ulint n_fields, ulint n_bytes, uint64_t tree_id) {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));
  ut_ad(n_fields + n_bytes > 0);

  auto n_fields_rec = rec_offs_n_fields(offsets);

  ut_ad(n_fields <= n_fields_rec);
  ut_ad(n_fields < n_fields_rec || n_bytes == 0);

  if (n_fields > n_fields_rec) {
    n_fields = n_fields_rec;
  }

  if (n_fields == n_fields_rec) {
    n_bytes = 0;
  }

  ulint i;
  auto fold = ut_uint64_fold(tree_id);

  for (i = 0; i < n_fields; i++) {
    ulint len;
    auto data = rec_get_nth_field(rec, offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  if (n_bytes > 0) {
    ulint len;
    auto data = rec_get_nth_field(rec, offsets, i, &len);

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
 * @param offsets in: rec_get_offsets(rec)
 * @return        first field containing a null BLOB pointer, or NULL if none found
 */
inline const byte *rec_offs_any_null_extern(const rec_t *rec, const ulint *offsets) {
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

/** Wrapper for pretty-printing a record */
struct Rec_offsets {
  /** Record */
  const rec_t *m_rec{};

  /** Index the records belong to. */
  dict_index_t *m_index{};
};

/**
 * Print the Rec_offset to the output stream.
 * 
 * @param[in,out] os Stream to write to
 * @param[in] r Record and its offsets
 * 
 * @return the output stream
 */
inline std::ostream &operator<<(std::ostream &os, Rec_offsets &&r) {
  rec_print(os, r.m_rec, r.m_index);
  return os;
}

/**
 * Returns nonzero if the default bit is set in nth field of rec.
 * 
 * @return nonzero if default bit is set
 */
static inline ulint rec_offs_nth_default(const ulint *offsets, ulint n) {
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));
  return rec_offs_base(offsets)[1 + n] & REC_OFFS_MASK;
}
