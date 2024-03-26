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

/** Determine how many of the first n columns in a compact
physical record are stored externally.
@return	number of externally stored columns */
ulint rec_get_n_extern_new(
  const rec_t *rec,    /*!< in: compact physical record */
  dict_index_t *index, /*!< in: record descriptor */
  ulint n
); /*!< in: number of columns to scan */

/** The following function determines the offsets to each field
in the record.	It can reuse a previously allocated array.
@return	the new offsets */
ulint *rec_get_offsets_func(
  const rec_t *rec,          /*!< in: physical record */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint *offsets,            /*!< in/out: array consisting of
                                                offsets[0] allocated elements,
                                                or an array from rec_get_offsets(),
                                                or nullptr */
  ulint n_fields,            /*!< in: maximum number of
                                               initialized fields
                                                (ULINT_UNDEFINED if all fields) */
  mem_heap_t **heap,         /*!< in/out: memory heap */
  const char *file,          /*!< in: file name where called */
  ulint line
); /*!< in: line number where called */

#define rec_get_offsets(rec, index, offsets, n, heap) rec_get_offsets_func(rec, index, offsets, n, heap, __FILE__, __LINE__)

/** Determine the offset to each field in a leaf-page record
in ROW_FORMAT=COMPACT.  This is a special case of
rec_init_offsets() and rec_get_offsets_func(). */
void rec_init_offsets_comp_ordinary(
  const rec_t *rec,          /*!< in: physical record in
                               ROW_FORMAT=COMPACT */
  ulint extra,               /*!< in: number of bytes to reserve
                               between the record header and
                               the data payload
                               (usually REC_N_NEW_EXTRA_BYTES) */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint *offsets
); /*!< in/out: array of offsets;
                              in: n=rec_offs_n_fields(offsets) */

/** The following function determines the offsets to each field
in the record.  It can reuse a previously allocated array. */
void rec_get_offsets_reverse(
  const byte *extra,         /*!< in: the extra bytes of a
                               compact record in reverse order,
                               excluding the fixed-size
                               REC_N_NEW_EXTRA_BYTES */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint node_ptr,            /*!< in: nonzero=node pointer,
                              0=leaf node */
  ulint *offsets
); /*!< in/out: array consisting of
                              offsets[0] allocated elements */

#ifndef UNIV_DEBUG
#define rec_offs_make_valid(rec, index, offsets) ((void)0)
#endif /* UNIV_DEBUG */

/** The following function is used to get the offset to the nth
data field in an old-style record.
@return	offset to the field */
ulint rec_get_nth_field_offs_old(
  const rec_t *rec, /*!< in: record */
  ulint n,          /*!< in: index of the field */
  ulint *len
); /*!< out: length of the field; UNIV_SQL_NULL if SQL null */

#define rec_get_nth_field_old(rec, n, len) ((rec) + rec_get_nth_field_offs_old(rec, n, len))
#define rec_get_nth_field(rec, offsets, n, len) ((rec) + rec_get_nth_field_offs(offsets, n, len))
#define rec_offs_init(offsets) rec_offs_set_n_alloc(offsets, (sizeof offsets) / sizeof *offsets)

/** Copies the first n fields of a physical record to a new physical record in a buffer.
@return	own: copied record */
rec_t *rec_copy_prefix_to_buf(
  const rec_t *rec,          /*!< in: physical record */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint n_fields,            /*!< in: number of fields to copy */
  byte *&buf,                /*!< in/out: memory buffer for the copied prefix, or nullptr. */
  ulint &buf_size            /*!< in/out: buffer size */
);

/** Builds a ROW_FORMAT=COMPACT record out of a data tuple. */
void rec_convert_dtuple_to_rec_comp(
  rec_t *rec,                /*!< in: origin of record */
  ulint extra,               /*!< in: number of bytes to
                               reserve between the record
                               header and the data payload
                               (normally REC_N_NEW_EXTRA_BYTES) */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint status,              /*!< in: status bits of the record */
  const dfield_t *fields,    /*!< in: array of data fields */
  ulint n_fields
); /*!< in: number of data fields */

/** Builds a physical record out of a data tuple and
stores it into the given buffer.
@return	pointer to the origin of physical record */
rec_t *rec_convert_dtuple_to_rec(
  byte *buf,                 /*!< in: start address of the
                               physical record */
  const dict_index_t *index, /*!< in: record descriptor */
  const dtuple_t *dtuple,    /*!< in: data tuple */
  ulint n_ext
); /*!< in: number of
                               externally stored columns */
/** Determines the size of a data tuple prefix in ROW_FORMAT=COMPACT.
@return	total size */
ulint rec_get_converted_size_comp_prefix(
  const dict_index_t *index, /*!< in: record descriptor;
                               dict_table_is_comp() is
                               assumed to hold, even if
                               it does not */
  const dfield_t *fields,    /*!< in: array of data fields */
  ulint n_fields,            /*!< in: number of data fields */
  ulint *extra
); /*!< out: extra size */

/** Determines the size of a data tuple in ROW_FORMAT=COMPACT.
@return	total size */
ulint rec_get_converted_size_comp(
  const dict_index_t *index, /*!< in: record descriptor;
                               dict_table_is_comp() is
                               assumed to hold, even if
                               it does not */
  ulint status,              /*!< in: status bits of the record */
  const dfield_t *fields,    /*!< in: array of data fields */
  ulint n_fields,            /*!< in: number of data fields */
  ulint *extra
); /*!< out: extra size */

/** Copies the first n fields of a physical record to a data tuple.
The fields are copied to the memory heap. */
void rec_copy_prefix_to_dtuple(
  dtuple_t *tuple,           /*!< out: data tuple */
  const rec_t *rec,          /*!< in: physical record */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint n_fields,            /*!< in: number of fields
                               to copy */
  mem_heap_t *heap
); /*!< in: memory heap */

/** Validates the consistency of a physical record.
@return	true if ok */
bool rec_validate(
  const rec_t *rec, /*!< in: physical record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */

/** Prints an old-style physical record. */
void rec_print_old(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const rec_t *rec
); /*!< in: physical record */

/** Prints a physical record in ROW_FORMAT=COMPACT.  Ignores the
record header. */
void rec_print_comp(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const rec_t *rec,      /*!< in: physical record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */

/** Prints a physical record. */
void rec_print_new(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const rec_t *rec,      /*!< in: physical record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */

/** Prints a physical record. */
void rec_print(
  ib_stream_t ib_stream, /*!< in: stream where to print */
  const rec_t *rec,      /*!< in: physical record */
  dict_index_t *index
); /*!< in: record descriptor */

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

/** Sets the value of the ith field SQL null bit of an old-style record. */
void rec_set_nth_field_null_bit(
  rec_t *rec, /*!< in: record */
  ulint i,    /*!< in: ith field */
  bool val /*!< in: value to set */
);

/** Sets an old-style record field to SQL null.
The physical size of the field is not changed. */
void rec_set_nth_field_sql_null(
  rec_t *rec, /*!< in: record */
  ulint n /*!< in: index of the field */
);

/** Gets a bit field from within 1 byte. */
inline ulint rec_get_bit_field_1(
  const rec_t *rec, /*!< in: pointer to record origin */
  ulint offs,       /*!< in: offset from the origin down */
  ulint mask,       /*!< in: mask used to filter bits */
  ulint shift       /*!< in: shift right applied after masking */
)
{
  ut_ad(rec);

  return ((mach_read_from_1(rec - offs) & mask) >> shift);
}

/** Sets a bit field within 1 byte. */
inline void rec_set_bit_field_1(
  rec_t *rec, /*!< in: pointer to record origin */
  ulint val,  /*!< in: value to set */
  ulint offs, /*!< in: offset from the origin down */
  ulint mask, /*!< in: mask used to filter bits */
  ulint shift
) /*!< in: shift right applied after masking */
{
  ut_ad(rec);
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
  ut_ad(rec);

  return ((mach_read_from_2(rec - offs) & mask) >> shift);
}

/** Sets a bit field within 2 bytes. */
inline void rec_set_bit_field_2(
  rec_t *rec, /*!< in: pointer to record origin */
  ulint val,  /*!< in: value to set */
  ulint offs, /*!< in: offset from the origin down */
  ulint mask, /*!< in: mask used to filter bits */
  ulint shift
) /*!< in: shift right applied after masking */
{
  ut_ad(rec);
  ut_ad(offs <= REC_N_OLD_EXTRA_BYTES);
  ut_ad(mask > 0xFFUL);
  ut_ad(mask <= 0xFFFFUL);
  ut_ad((mask >> shift) & 1);
  ut_ad(0 == ((mask >> shift) & ((mask >> shift) + 1)));
  ut_ad(((mask >> shift) << shift) == mask);
  ut_ad(((val << shift) & mask) == (val << shift));

  mach_write_to_2(rec - offs, (mach_read_from_2(rec - offs) & ~mask) | (val << shift));
}

/** The following function is used to get the pointer of the next chained record
on the same page.
@return	pointer to the next chained record, or nullptr if none */
inline const rec_t *rec_get_next_ptr_const(
  const rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  ulint field_value;

  ut_ad(REC_NEXT_MASK == 0xFFFFUL);
  ut_ad(REC_NEXT_SHIFT == 0);

  field_value = mach_read_from_2(rec - REC_NEXT);

  if (unlikely(field_value == 0)) {

    return (nullptr);
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
#endif
    /* There must be at least REC_N_NEW_EXTRA_BYTES + 1
    between each record. */
    ut_ad((field_value > REC_N_NEW_EXTRA_BYTES && field_value < 32768) || field_value < (uint16_t)-REC_N_NEW_EXTRA_BYTES);

    return ((byte *)ut_align_down(rec, UNIV_PAGE_SIZE) + ut_align_offset(rec + field_value, UNIV_PAGE_SIZE));
  } else {
    ut_ad(field_value < UNIV_PAGE_SIZE);

    return ((byte *)ut_align_down(rec, UNIV_PAGE_SIZE) + field_value);
  }
}

/** The following function is used to get the pointer of the next chained record
on the same page.
@return	pointer to the next chained record, or nullptr if none */
inline rec_t *rec_get_next_ptr(
  rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  return ((rec_t *)rec_get_next_ptr_const(rec, comp));
}

/** The following function is used to get the offset of the next chained record
on the same page.
@return	the page offset of the next chained record, or 0 if none */
inline ulint rec_get_next_offs(
  const rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  ulint field_value;

  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");
  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");

  field_value = mach_read_from_2(rec - REC_NEXT);

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

      return (0);
    }

    /* There must be at least REC_N_NEW_EXTRA_BYTES + 1
    between each record. */
    ut_ad((field_value > REC_N_NEW_EXTRA_BYTES && field_value < 32768) || field_value < (uint16_t)-REC_N_NEW_EXTRA_BYTES);

    return (ut_align_offset(rec + field_value, UNIV_PAGE_SIZE));
  } else {
    ut_ad(field_value < UNIV_PAGE_SIZE);

    return (field_value);
  }
}

/** The following function is used to set the next record offset field
of an old-style record. */
inline void rec_set_next_offs_old(
  rec_t *rec, /*!< in: old-style physical record */
  ulint next
) /*!< in: offset of the next record */
{
  ut_ad(rec);
  ut_ad(UNIV_PAGE_SIZE > next);

  static_assert(REC_NEXT_MASK == 0xFFFFUL, "error REC_NEXT_MASK != 0xFFFFUL");
  static_assert(REC_NEXT_SHIFT == 0, "error REC_NEXT_SHIFT != 0");

  mach_write_to_2(rec - REC_NEXT, next);
}

/** The following function is used to set the next record offset field
of a new-style record. */
inline void rec_set_next_offs_new(
  rec_t *rec, /*!< in/out: new-style physical record */
  ulint next
) /*!< in: offset of the next record */
{
  ulint field_value;

  ut_ad(rec);
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

/** The following function is used to get the number of fields
in an old-style record.
@return	number of data fields */
inline ulint rec_get_n_fields_old(const rec_t *rec) /*!< in: physical record */
{
  ulint ret;

  ut_ad(rec);

  ret = rec_get_bit_field_2(rec, REC_OLD_N_FIELDS, REC_OLD_N_FIELDS_MASK, REC_OLD_N_FIELDS_SHIFT);
  ut_ad(ret <= REC_MAX_N_FIELDS);
  ut_ad(ret > 0);

  return (ret);
}

/** The following function is used to set the number of fields
in an old-style record. */
inline void rec_set_n_fields_old(
  rec_t *rec, /*!< in: physical record */
  ulint n_fields
) /*!< in: the number of fields */
{
  ut_ad(rec);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields > 0);

  rec_set_bit_field_2(rec, n_fields, REC_OLD_N_FIELDS, REC_OLD_N_FIELDS_MASK, REC_OLD_N_FIELDS_SHIFT);
}

/** The following function retrieves the status bits of a new-style record.
@return	status bits */
inline ulint rec_get_status(const rec_t *rec) /*!< in: physical record */
{
  ulint ret;

  ut_ad(rec);

  ret = rec_get_bit_field_1(rec, REC_NEW_STATUS, REC_NEW_STATUS_MASK, REC_NEW_STATUS_SHIFT);
  ut_ad((ret & ~REC_NEW_STATUS_MASK) == 0);

  return (ret);
}

/** The following function is used to get the number of fields
in a record.
@return	number of data fields */
inline ulint rec_get_n_fields(
  const rec_t *rec, /*!< in: physical record */
  const dict_index_t *dict_index
) /*!< in: record descriptor */
{
  ut_ad(rec);
  ut_ad(dict_index);

  if (!dict_table_is_comp(dict_index->table)) {
    return (rec_get_n_fields_old(rec));
  }

  switch (rec_get_status(rec)) {
    case REC_STATUS_ORDINARY:
      return (dict_index_get_n_fields(dict_index));
    case REC_STATUS_NODE_PTR:
      return (dict_index_get_n_unique_in_tree(dict_index) + 1);
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      return (1);
    default:
      ut_error;
      return (ULINT_UNDEFINED);
  }
}

/** The following function is used to get the number of records owned by the
previous directory record.
@return	number of owned records */
inline ulint rec_get_n_owned_old(const rec_t *rec) /*!< in: old-style physical record */
{
  return (rec_get_bit_field_1(rec, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT));
}

/** The following function is used to set the number of owned records. */
inline void rec_set_n_owned_old(
  rec_t *rec, /*!< in: old-style physical record */
  ulint n_owned
) /*!< in: the number of owned */
{
  rec_set_bit_field_1(rec, n_owned, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/** The following function is used to get the number of records owned by the
previous directory record.
@return	number of owned records */
inline ulint rec_get_n_owned_new(const rec_t *rec) /*!< in: new-style physical record */
{
  return (rec_get_bit_field_1(rec, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT));
}

/** The following function is used to set the number of owned records. */
inline void rec_set_n_owned_new(
  rec_t *rec, /*!< in/out: new-style physical record */
  ulint n_owned
) /*!< in: the number of owned */
{
  rec_set_bit_field_1(rec, n_owned, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

/** The following function is used to retrieve the info bits of a record.
@return	info bits */
inline ulint rec_get_info_bits(
  const rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  return (rec_get_bit_field_1(rec, comp ? REC_NEW_INFO_BITS : REC_OLD_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT));
}

/** The following function is used to set the info bits of a record. */
inline void rec_set_info_bits_old(
  rec_t *rec, /*!< in: old-style physical record */
  ulint bits
) /*!< in: info bits */
{
  rec_set_bit_field_1(rec, bits, REC_OLD_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/** The following function is used to set the info bits of a record. */
inline void rec_set_info_bits_new(
  rec_t *rec, /*!< in/out: new-style physical record */
  ulint bits
) /*!< in: info bits */
{
  rec_set_bit_field_1(rec, bits, REC_NEW_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

/** The following function is used to set the status bits of a new-style record.
 */
inline void rec_set_status(
  rec_t *rec, /*!< in/out: physical record */
  ulint bits
) /*!< in: info bits */
{
  rec_set_bit_field_1(rec, bits, REC_NEW_STATUS, REC_NEW_STATUS_MASK, REC_NEW_STATUS_SHIFT);
}

/** The following function is used to retrieve the info and status
bits of a record.  (Only compact records have status bits.)
@return	info bits */
inline ulint rec_get_info_and_status_bits(
  const rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  ulint bits;

  static_assert(!((REC_NEW_STATUS_MASK >> REC_NEW_STATUS_SHIFT) & (REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)),
                "error REC_NEW_STATUS_MASK and REC_INFO_BITS_MASK overlap");

  if (expect(comp, REC_OFFS_COMPACT)) {
    bits = rec_get_info_bits(rec, true) | rec_get_status(rec);
  } else {
    bits = rec_get_info_bits(rec, false);
    ut_ad(!(bits & ~(REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)));
  }
  return (bits);
}

/** The following function is used to set the info and status
bits of a record.  (Only compact records have status bits.) */
inline void rec_set_info_and_status_bits(
  rec_t *rec, /*!< in/out: physical record */
  ulint bits
) /*!< in: info bits */
{

  static_assert(!((REC_NEW_STATUS_MASK >> REC_NEW_STATUS_SHIFT) & (REC_INFO_BITS_MASK >> REC_INFO_BITS_SHIFT)),
                "error REC_NEW_STATUS_MASK and REC_INFO_BITS_MASK overlap");
  rec_set_status(rec, bits & REC_NEW_STATUS_MASK);
  rec_set_info_bits_new(rec, bits & ~REC_NEW_STATUS_MASK);
}

/** The following function tells if record is delete marked.
@return	nonzero if delete marked */
inline ulint rec_get_deleted_flag(
  const rec_t *rec, /*!< in: physical record */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  if (expect(comp, REC_OFFS_COMPACT)) {
    return (unlikely(rec_get_bit_field_1(rec, REC_NEW_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT)));
  } else {
    return (unlikely(rec_get_bit_field_1(rec, REC_OLD_INFO_BITS, REC_INFO_DELETED_FLAG, REC_INFO_BITS_SHIFT)));
  }
}

/** The following function is used to set the deleted bit. */
inline void rec_set_deleted_flag_old(
  rec_t *rec, /*!< in: old-style physical record */
  ulint flag
) /*!< in: nonzero if delete marked */
{
  ulint val;

  val = rec_get_info_bits(rec, false);

  if (flag) {
    val |= REC_INFO_DELETED_FLAG;
  } else {
    val &= ~REC_INFO_DELETED_FLAG;
  }

  rec_set_info_bits_old(rec, val);
}

/** The following function is used to set the deleted bit. */
inline void rec_set_deleted_flag_new(
  rec_t *rec, /*!< in/out: new-style physical record */
  ulint flag
) /*!< in: nonzero if delete marked */
{
  ulint val;

  val = rec_get_info_bits(rec, true);

  if (flag) {
    val |= REC_INFO_DELETED_FLAG;
  } else {
    val &= ~REC_INFO_DELETED_FLAG;
  }

  rec_set_info_bits_new(rec, val);
}

/** The following function tells if a new-style record is a node pointer.
@return	true if node pointer */
inline bool rec_get_node_ptr_flag(const rec_t *rec) /*!< in: physical record */
{
  return (REC_STATUS_NODE_PTR == rec_get_status(rec));
}

/** The following function is used to get the order number
of an old-style record in the heap of the index page.
@return	heap order number */
inline ulint rec_get_heap_no_old(const rec_t *rec) /*!< in: physical record */
{
  return (rec_get_bit_field_2(rec, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT));
}

/** The following function is used to set the heap number
field in an old-style record. */
inline void rec_set_heap_no_old(
  rec_t *rec, /*!< in: physical record */
  ulint heap_no
) /*!< in: the heap number */
{
  rec_set_bit_field_2(rec, heap_no, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/** The following function is used to get the order number
of a new-style record in the heap of the index page.
@return	heap order number */
inline ulint rec_get_heap_no_new(const rec_t *rec) /*!< in: physical record */
{
  return (rec_get_bit_field_2(rec, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT));
}

/** The following function is used to set the heap number
field in a new-style record. */
inline void rec_set_heap_no_new(
  rec_t *rec, /*!< in/out: physical record */
  ulint heap_no
) /*!< in: the heap number */
{
  rec_set_bit_field_2(rec, heap_no, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

/** The following function is used to test whether the data offsets in the
record are stored in one-byte or two-byte format.
@return	true if 1-byte form */
inline bool rec_get_1byte_offs_flag(const rec_t *rec) /*!< in: physical record */
{
  return (rec_get_bit_field_1(rec, REC_OLD_SHORT, REC_OLD_SHORT_MASK, REC_OLD_SHORT_SHIFT));
}

/** The following function is used to set the 1-byte offsets flag. */
inline void rec_set_1byte_offs_flag(
  rec_t *rec, /*!< in: physical record */
  bool flag
) /*!< in: true if 1byte form */
{
  rec_set_bit_field_1(rec, flag, REC_OLD_SHORT, REC_OLD_SHORT_MASK, REC_OLD_SHORT_SHIFT);
}

/** Returns the offset of nth field end if the record is stored in the 1-byte
offsets form. If the field is SQL null, the flag is ORed in the returned
value.
@return	offset of the start of the field, SQL null flag ORed */
inline ulint rec_1_get_field_end_info(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  return (mach_read_from_1(rec - (REC_N_OLD_EXTRA_BYTES + n + 1)));
}

/** Returns the offset of nth field end if the record is stored in the 2-byte
offsets form. If the field is SQL null, the flag is ORed in the returned
value.
@return offset of the start of the field, SQL null flag and extern
storage flag ORed */
inline ulint rec_2_get_field_end_info(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  return (mach_read_from_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n + 2)));
}

/* Get the base address of offsets.  The extra_size is stored at
this position, and following positions hold the end offsets of
the fields. */
#define rec_offs_base(offsets) (offsets + REC_OFFS_HEADER_SIZE)

/** The following function returns the number of allocated elements
for an array of offsets.
@return	number of elements */
inline ulint rec_offs_get_n_alloc(const ulint *offsets) /*!< in: array for rec_get_offsets() */
{
  ulint n_alloc;
  ut_ad(offsets);
  n_alloc = offsets[0];
  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);
  UNIV_MEM_ASSERT_W(offsets, n_alloc * sizeof *offsets);
  return (n_alloc);
}

/** The following function sets the number of allocated elements
for an array of offsets. */
inline void rec_offs_set_n_alloc(
  ulint *offsets, /*!< out: array for rec_get_offsets(),
                                     must be allocated */
  ulint n_alloc
) /*!< in: number of elements */
{
  ut_ad(offsets);
  ut_ad(n_alloc > REC_OFFS_HEADER_SIZE);
  UNIV_MEM_ASSERT_AND_ALLOC(offsets, n_alloc * sizeof *offsets);
  offsets[0] = n_alloc;
}

/** The following function returns the number of fields in a record.
@return	number of fields */
inline ulint rec_offs_n_fields(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ulint n_fields;
  ut_ad(offsets);
  n_fields = offsets[1];
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields + REC_OFFS_HEADER_SIZE <= rec_offs_get_n_alloc(offsets));
  return (n_fields);
}

/** Validates offsets returned by rec_get_offsets().
@return	true if valid */
inline bool rec_offs_validate(
  const rec_t *rec,               /*!< in: record or nullptr */
  const dict_index_t *dict_index, /*!< in: record descriptor or nullptr */
  const ulint *offsets
) /*!< in: array returned by
                                    rec_get_offsets() */
{
  ulint i = rec_offs_n_fields(offsets);
  ulint last = ULINT_MAX;
  ulint comp = *rec_offs_base(offsets) & REC_OFFS_COMPACT;

  if (rec) {
    ut_ad((ulint)rec == offsets[2]);
    if (!comp) {
      ut_a(rec_get_n_fields_old(rec) >= i);
    }
  }
  if (dict_index) {
    ulint max_n_fields;
    ut_ad((ulint)dict_index == offsets[3]);
    max_n_fields = ut_max(dict_index_get_n_fields(dict_index), dict_index_get_n_unique_in_tree(dict_index) + 1);
    if (comp && rec) {
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
    ulint curr = rec_offs_base(offsets)[1 + i] & REC_OFFS_MASK;
    ut_a(curr <= last);
    last = curr;
  }
  return (true);
}
#ifdef UNIV_DEBUG
/** Updates debug data in offsets, in order to avoid bogus
rec_offs_validate() failures. */
inline void rec_offs_make_valid(
  const rec_t *rec,          /*!< in: record */
  const dict_index_t *index, /*!< in: record descriptor */
  ulint *offsets
) /*!< in: array returned by
                                               rec_get_offsets() */
{
  ut_ad(rec);
  ut_ad(index);
  ut_ad(offsets);
  ut_ad(rec_get_n_fields(rec, index) >= rec_offs_n_fields(offsets));
  offsets[2] = (ulint)rec;
  offsets[3] = (ulint)index;
}
#endif /* UNIV_DEBUG */

/** The following function is used to get an offset to the nth
data field in a record.
@return	offset from the origin of rec */
inline ulint rec_get_nth_field_offs(
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint n,              /*!< in: index of the field */
  ulint *len
) /*!< out: length of the field; UNIV_SQL_NULL
                          if SQL null */
{
  ulint offs;
  ulint length;
  ut_ad(n < rec_offs_n_fields(offsets));
  ut_ad(len);

  if (unlikely(n == 0)) {
    offs = 0;
  } else {
    offs = rec_offs_base(offsets)[n] & REC_OFFS_MASK;
  }

  length = rec_offs_base(offsets)[1 + n];

  if (length & REC_OFFS_SQL_NULL) {
    length = UNIV_SQL_NULL;
  } else {
    length &= REC_OFFS_MASK;
    length -= offs;
  }

  *len = length;
  return (offs);
}

/** Determine if the offsets are for a record in the new
compact format.
@return	nonzero if compact format */
inline ulint rec_offs_comp(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  return (*rec_offs_base(offsets) & REC_OFFS_COMPACT);
}

/** Determine if the offsets are for a record containing
externally stored columns.
@return	nonzero if externally stored */
inline ulint rec_offs_any_extern(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  return (unlikely(*rec_offs_base(offsets) & REC_OFFS_EXTERNAL));
}

/** Returns nonzero if the extern bit is set in nth field of rec.
@return	nonzero if externally stored */
inline ulint rec_offs_nth_extern(
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint n
) /*!< in: nth field */
{
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));
  return (unlikely(rec_offs_base(offsets)[1 + n] & REC_OFFS_EXTERNAL));
}

/** Returns nonzero if the SQL nullptr bit is set in nth field of rec.
@return	nonzero if SQL nullptr */
inline ulint rec_offs_nth_sql_null(
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint n
) /*!< in: nth field */
{
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));
  return (unlikely(rec_offs_base(offsets)[1 + n] & REC_OFFS_SQL_NULL));
}

/** Gets the physical size of a field.
@return	length of field */
inline ulint rec_offs_nth_size(
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint n
) /*!< in: nth field */
{
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  ut_ad(n < rec_offs_n_fields(offsets));
  if (!n) {
    return (rec_offs_base(offsets)[1 + n] & REC_OFFS_MASK);
  }
  return ((rec_offs_base(offsets)[1 + n] - rec_offs_base(offsets)[n]) & REC_OFFS_MASK);
}

/** Returns the number of extern bits set in a record.
@return	number of externally stored fields */
inline ulint rec_offs_n_extern(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ulint n = 0;

  if (rec_offs_any_extern(offsets)) {
    ulint i;

    for (i = rec_offs_n_fields(offsets); i--;) {
      if (rec_offs_nth_extern(offsets, i)) {
        n++;
      }
    }
  }

  return (n);
}

/** Returns the offset of n - 1th field end if the record is stored in the
1-byte offsets form. If the field is SQL null, the flag is ORed in the returned
value. This function and the 2-byte counterpart are defined here because the
C-compiler was not able to sum negative and positive constant offsets, and
warned of constant arithmetic overflow within the compiler.
@return	offset of the start of the PREVIOUS field, SQL null flag ORed */
inline ulint rec_1_get_prev_field_end_info(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  return (mach_read_from_1(rec - (REC_N_OLD_EXTRA_BYTES + n)));
}

/** Returns the offset of n - 1th field end if the record is stored in the
2-byte offsets form. If the field is SQL null, the flag is ORed in the returned
value.
@return	offset of the start of the PREVIOUS field, SQL null flag ORed */
inline ulint rec_2_get_prev_field_end_info(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  return (mach_read_from_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n)));
}

/** Sets the field end info for the nth field if the record is stored in the
1-byte format. */
inline void rec_1_set_field_end_info(
  rec_t *rec, /*!< in: record */
  ulint n,    /*!< in: field index */
  ulint info
) /*!< in: value to set */
{
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  mach_write_to_1(rec - (REC_N_OLD_EXTRA_BYTES + n + 1), info);
}

/** Sets the field end info for the nth field if the record is stored in the
2-byte format. */
inline void rec_2_set_field_end_info(
  rec_t *rec, /*!< in: record */
  ulint n,    /*!< in: field index */
  ulint info
) /*!< in: value to set */
{
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old(rec));

  mach_write_to_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n + 2), info);
}

/** Returns the offset of nth field start if the record is stored in the 1-byte
offsets form.
@return	offset of the start of the field */
inline ulint rec_1_get_field_start_offs(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  if (n == 0) {

    return (0);
  }

  return (rec_1_get_prev_field_end_info(rec, n) & ~REC_1BYTE_SQL_NULL_MASK);
}

/** Returns the offset of nth field start if the record is stored in the 2-byte
offsets form.
@return	offset of the start of the field */
inline ulint rec_2_get_field_start_offs(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old(rec));

  if (n == 0) {

    return (0);
  }

  return (rec_2_get_prev_field_end_info(rec, n) & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK));
}

/** The following function is used to read the offset of the start of a data
field in the record. The start of an SQL null field is the end offset of the
previous non-null field, or 0, if none exists. If n is the number of the last
field + 1, then the end offset of the last field is returned.
@return	offset of the start of the field */
inline ulint rec_get_field_start_offs(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: field index */
{
  ut_ad(rec);
  ut_ad(n <= rec_get_n_fields_old(rec));

  if (n == 0) {

    return (0);
  }

  if (rec_get_1byte_offs_flag(rec)) {

    return (rec_1_get_field_start_offs(rec, n));
  }

  return (rec_2_get_field_start_offs(rec, n));
}

/** Gets the physical size of an old-style field.
Also an SQL null may have a field of size > 0,
if the data type is of a fixed size.
@return	field size in bytes */
inline ulint rec_get_nth_field_size(
  const rec_t *rec, /*!< in: record */
  ulint n
) /*!< in: index of the field */
{
  ulint os;
  ulint next_os;

  os = rec_get_field_start_offs(rec, n);
  next_os = rec_get_field_start_offs(rec, n + 1);

  ut_ad(next_os - os < UNIV_PAGE_SIZE);

  return (next_os - os);
}

/** This is used to modify the value of an already existing field in a record.
The previous value must have exactly the same size as the new value. If len
is UNIV_SQL_NULL then the field is treated as an SQL null.
For records in ROW_FORMAT=COMPACT (new-style records), len must not be
UNIV_SQL_NULL unless the field already is SQL null. */
inline void rec_set_nth_field(
  rec_t *rec,           /*!< in: record */
  const ulint *offsets, /*!< in: array returned by rec_get_offsets() */
  ulint n,              /*!< in: index number of the field */
  const void *data,     /*!< in: pointer to the data
                          if not SQL null */
  ulint len
) /*!< in: length of the data or UNIV_SQL_NULL */
{
  byte *data2;
  ulint len2;

  ut_ad(rec);
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  if (unlikely(len == UNIV_SQL_NULL)) {
    if (!rec_offs_nth_sql_null(offsets, n)) {
      ut_a(!rec_offs_comp(offsets));
      rec_set_nth_field_sql_null(rec, n);
    }

    return;
  }

  data2 = rec_get_nth_field(rec, offsets, n, &len2);
  if (len2 == UNIV_SQL_NULL) {
    ut_ad(!rec_offs_comp(offsets));
    rec_set_nth_field_null_bit(rec, n, false);
    ut_ad(len == rec_get_nth_field_size(rec, n));
  } else {
    ut_ad(len2 == len);
  }

  memcpy(data2, data, len);
}

/** The following function returns the data size of an old-style physical
record, that is the sum of field lengths. SQL null fields
are counted as length 0 fields. The value returned by the function
is the distance from record origin to record end in bytes.
@return	size */
inline ulint rec_get_data_size_old(const rec_t *rec) /*!< in: physical record */
{
  ut_ad(rec);

  return (rec_get_field_start_offs(rec, rec_get_n_fields_old(rec)));
}

/** The following function sets the number of fields in offsets. */
inline void rec_offs_set_n_fields(
  ulint *offsets, /*!< in/out: array returned by
                                                  rec_get_offsets() */
  ulint n_fields
) /*!< in: number of fields */
{
  ut_ad(offsets);
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= REC_MAX_N_FIELDS);
  ut_ad(n_fields + REC_OFFS_HEADER_SIZE <= rec_offs_get_n_alloc(offsets));
  offsets[1] = n_fields;
}

/** The following function returns the data size of a physical
record, that is the sum of field lengths. SQL null fields
are counted as length 0 fields. The value returned by the function
is the distance from record origin to record end in bytes.
@return	size */
inline ulint rec_offs_data_size(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ulint size;

  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  size = rec_offs_base(offsets)[rec_offs_n_fields(offsets)] & REC_OFFS_MASK;
  ut_ad(size < UNIV_PAGE_SIZE);
  return (size);
}

/** Returns the total size of record minus data size of record. The value
returned by the function is the distance from record start to record origin
in bytes.
@return	size */
inline ulint rec_offs_extra_size(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  ulint size;
  ut_ad(rec_offs_validate(nullptr, nullptr, offsets));
  size = *rec_offs_base(offsets) & ~(REC_OFFS_COMPACT | REC_OFFS_EXTERNAL);
  ut_ad(size < UNIV_PAGE_SIZE);
  return (size);
}

/** Returns the total size of a physical record.
@return	size */
inline ulint rec_offs_size(const ulint *offsets) /*!< in: array returned by rec_get_offsets() */
{
  return (rec_offs_data_size(offsets) + rec_offs_extra_size(offsets));
}

/** Returns a pointer to the end of the record.
@return	pointer to end */
inline byte *rec_get_end(
  rec_t *rec, /*!< in: pointer to record */
  const ulint *offsets
) /*!< in: array returned by rec_get_offsets() */
{
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  return (rec + rec_offs_data_size(offsets));
}

/** Returns a pointer to the start of the record.
@return	pointer to start */
inline byte *rec_get_start(
  rec_t *rec, /*!< in: pointer to record */
  const ulint *offsets
) /*!< in: array returned by rec_get_offsets() */
{
  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  return (rec - rec_offs_extra_size(offsets));
}

/** Copies a physical record to a buffer.
@return	pointer to the origin of the copy */
inline rec_t *rec_copy(
  void *buf,        /*!< in: buffer */
  const rec_t *rec, /*!< in: physical record */
  const ulint *offsets
) /*!< in: array returned by rec_get_offsets() */
{
  ulint extra_len;
  ulint data_len;

  ut_ad(rec && buf);
  ut_ad(rec_offs_validate((rec_t *)rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));

  extra_len = rec_offs_extra_size(offsets);
  data_len = rec_offs_data_size(offsets);

  memcpy(buf, rec - extra_len, extra_len + data_len);

  return ((byte *)buf + extra_len);
}

/** Returns the extra size of an old-style physical record if we know its
data size and number of fields.
@return	extra size */
inline ulint rec_get_converted_extra_size(
  ulint data_size, /*!< in: data size */
  ulint n_fields,  /*!< in: number of fields */
  ulint n_ext
) /*!< in: number of externally stored columns */
{
  if (!n_ext && data_size <= REC_1BYTE_OFFS_LIMIT) {

    return (REC_N_OLD_EXTRA_BYTES + n_fields);
  }

  return (REC_N_OLD_EXTRA_BYTES + 2 * n_fields);
}

/** The following function returns the size of a data tuple when converted to
a physical record.
@return	size */
inline ulint rec_get_converted_size(
  dict_index_t *dict_index, /*!< in: record descriptor */
  const dtuple_t *dtuple,   /*!< in: data tuple */
  ulint n_ext
) /*!< in: number of externally stored columns */
{
  ulint data_size;
  ulint extra_size;

  ut_ad(dict_index);
  ut_ad(dtuple);
  ut_ad(dtuple_check_typed(dtuple));

  ut_ad(
    dtuple_get_n_fields(dtuple) == (((dtuple_get_info_bits(dtuple) & REC_NEW_STATUS_MASK) == REC_STATUS_NODE_PTR)
                                      ? dict_index_get_n_unique_in_tree(dict_index) + 1
                                      : dict_index_get_n_fields(dict_index))
  );

  if (dict_table_is_comp(dict_index->table)) {
    return (rec_get_converted_size_comp(
      dict_index, dtuple_get_info_bits(dtuple) & REC_NEW_STATUS_MASK, dtuple->fields, dtuple->n_fields, nullptr
    ));
  }

  data_size = dtuple_get_data_size(dtuple, 0);

  extra_size = rec_get_converted_extra_size(data_size, dtuple_get_n_fields(dtuple), n_ext);

  return (data_size + extra_size);
}

/** Folds a prefix of a physical record to a ulint. Folds only existing fields,
that is, checks that we do not run out of the record.
@return	the folded value */
inline ulint rec_fold(
  const rec_t *rec,     /*!< in: the physical record */
  const ulint *offsets, /*!< in: array returned by
                                            rec_get_offsets() */
  ulint n_fields,       /*!< in: number of complete
                                            fields to fold */
  ulint n_bytes,        /*!< in: number of bytes to fold
                                            in an incomplete last field */
  uint64_t tree_id
) /*!< in: index tree id */
{
  ulint i;
  const byte *data;
  ulint len;
  ulint fold;
  ulint n_fields_rec;

  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  ut_ad(rec_validate(rec, offsets));
  ut_ad(n_fields + n_bytes > 0);

  n_fields_rec = rec_offs_n_fields(offsets);
  ut_ad(n_fields <= n_fields_rec);
  ut_ad(n_fields < n_fields_rec || n_bytes == 0);

  if (n_fields > n_fields_rec) {
    n_fields = n_fields_rec;
  }

  if (n_fields == n_fields_rec) {
    n_bytes = 0;
  }

  fold = ut_uint64_fold(tree_id);

  for (i = 0; i < n_fields; i++) {
    data = rec_get_nth_field(rec, offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  if (n_bytes > 0) {
    data = rec_get_nth_field(rec, offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      if (len > n_bytes) {
        len = n_bytes;
      }

      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  return (fold);
}
