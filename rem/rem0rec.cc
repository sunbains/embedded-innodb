/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/** @file rem/rem0rec.c
Record manager

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#include <sstream>
#include <format>

#include "btr0types.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "rem0rec.h"

/*			PHYSICAL RECORD 

The physical record, which is the data type of all the records
found in index pages of the database, has the following format
(lower addresses and more significant bits inside a byte are below
represented on a higher text line):

| offset of the end of the last field of data, the most significant
  bit is set to 1 if and only if the field is SQL-null,
  if the offset is 2-byte, then the second most significant
  bit is set to 1 if the field is stored on another page:
  mostly this will occur in the case of big BLOB fields |
...
| offset of the end of the first field of data + the SQL-null bit |
| 4 bits used to delete mark a record, and mark a predefined
  minimum record in alphabetical order |
| 4 bits giving the number of records owned by this record
  (this term is explained in page0page.h) |
| 13 bits giving the order number of this record in the
  heap of the index page |
| 10 bits giving the number of fields in this record |
| 1 bit which is set to 1 if the offsets above are given in
  one byte format, 0 if in two byte format |
| two bytes giving an absolute pointer to the next record in the page |
ORIGIN of the record
| first field of data |
...
| last field of data |

The origin of the record is the start address of the first field
of data. The offsets are given relative to the origin.
The offsets of the data fields are stored in an inverted
order because then the offset of the first fields are near the
origin, giving maybe a better processor cache hit rate in searches.

The offsets of the data fields are given as one-byte
(if there are less than 127 bytes of data in the record)
or two-byte unsigned integers. The most significant bit
is not part of the offset, instead it indicates the SQL-null
if the bit is set to 1. */

/* CANONICAL COORDINATES. A record can be seen as a single
string of 'characters' in the following way: catenate the bytes
in each field, in the order of fields. An SQL-null field
is taken to be an empty sequence of bytes. Then after
the position of each field insert in the string
the 'character' <FIELD-END>, except that after an SQL-null field
insert <NULL-FIELD-END>. Now the ordinal position of each
byte in this canonical string is its canonical coordinate.
So, for the record ("AA", SQL-NULL, "BB", ""), the canonical
string is "AA<FIELD_END><NULL-FIELD-END>BB<FIELD-END><FIELD-END>".
We identify prefixes (= initial segments) of a record
with prefixes of the canonical string. The canonical
length of the prefix is the length of the corresponding
prefix of the canonical string. The canonical length of
a record is the length of its canonical string.

For example, the maximal common prefix of records
("AA", SQL-NULL, "BB", "C") and ("AA", SQL-NULL, "B", "C")
is "AA<FIELD-END><NULL-FIELD-END>B", and its canonical
length is 5.

A complete-field prefix of a record is a prefix which ends at the
end of some field (containing also <FIELD-END>).
A record is a complete-field prefix of another record, if
the corresponding canonical strings have the same property. */

/**
 * Validates a record.
 *
 * @param rec The physical record to validate.
 * 
 * @return true if the record is valid, false otherwise.
 */
static bool rec_validate(const rec_t *rec) noexcept;

void Phy_rec::get_col_offsets(ulint *offsets) const noexcept {
  ulint offs{};
  ulint any_ext{};
  ulint null_mask{1};
  auto nulls = m_rec - 1;
  auto lens = nulls - UT_BITS_IN_BYTES(m_index->n_nullable);

#ifdef UNIV_DEBUG
  offsets[2] = reinterpret_cast<uintptr_t>(m_rec);
  offsets[3] = reinterpret_cast<uintptr_t>(m_index);
#endif /* UNIV_DEBUG */

  auto set_field_len = [&](ulint i, ulint len) {
    rec_offs_base(offsets)[i + 1] = len;
  };

  /* Read the lengths of fields 0..n */
  for (ulint i{}; i < rec_offs_n_fields(offsets); ++i) {
    const auto field = dict_index_get_nth_field(m_index, i);

    if (!(dict_field_get_col(field)->prtype & DATA_NOT_NULL)) {
      /* Nullable field => read the null flag */

      if (unlikely(!(byte)null_mask)) {
        --nulls;
        null_mask = 1;
      }

      if (*nulls & null_mask) {
        null_mask <<= 1;
        /* No length is stored for NULL fields.  We do not advance offs, and we set
        the length to zero and enable the SQL NULL flag in offsets[]. */
        set_field_len(i, offs | REC_OFFS_SQL_NULL);
        continue;
      }

      null_mask <<= 1;
    }

    if (unlikely(field->fixed_len == 0)) {
      ulint len = *lens;

      --lens;

      /* Variable-length field: read the length */
      const auto col = dict_field_get_col(field);

      /* If the maximum length of the field is up to 255 bytes, the actual
      length is always stored in one byte. If the maximum length is more than
      255 bytes, the actual length is stored in one byte for 0..127. The
      length will be encoded in two bytes when it is 128 or more, or when
      the field is stored externally. */
      if (unlikely(col->len > 255) || unlikely(col->mtype == DATA_BLOB)) {
        if (len & 0x80) {
          /* 1exxxxxxx xxxxxxxx */
          len <<= 8;
          len |= *lens;

          --lens;

          offs += len & 0x3fff;

          if (unlikely(len & 0x4000)) {
            ut_ad(dict_index_is_clust(m_index));
            any_ext = REC_OFFS_EXTERNAL;
            set_field_len(i, offs | REC_OFFS_EXTERNAL);
          } else {
            set_field_len(i, offs);
          }
          continue;
        }
      }

      offs += len;
      set_field_len(i, offs);
    } else {
      offs += field->fixed_len;
      set_field_len(i, offs);
    }
  }

  *rec_offs_base(offsets) = (m_rec - (lens + 1)) | any_ext;
}

ulint *Phy_rec::get_col_offsets(ulint *offsets, ulint n_fields, mem_heap_t **heap, Source_location sl) const noexcept {
  auto n = rec_get_n_fields(m_rec);

  if (unlikely(n_fields < n)) {
    n = n_fields;
  }

  auto size = n + 1 + REC_OFFS_HEADER_SIZE;

  if (unlikely(offsets == nullptr) || unlikely(rec_offs_get_n_alloc(offsets) < size)) {
    if (unlikely(*heap == nullptr)) {
      *heap = mem_heap_create_func(size * sizeof(ulint), MEM_HEAP_DYNAMIC, sl.m_from.file_name(), sl.m_from.line());
    }
    offsets = reinterpret_cast<ulint *>(mem_heap_alloc(*heap, size * sizeof(ulint)));
    rec_offs_set_n_alloc(offsets, size);
  }

  rec_offs_set_n_fields(offsets, n);

  /* The following sets the offsets to each field in the record in the offsets
  array. The offsets are written to a previously allocated array of ulint,
  where rec_offs_n_fields(offsets) has been initialized to the number of fields
  in the record:

     offsets[1] == n_fields.
  
  The rest of the array will be initialized by this function.

  For externally stored columns:
  
     offsets[0] == extra size, if REC_OFFS_EXTERNAL is set
    
  offsets[1..n_fields] will be set to offsets past the end of fields 0..n_fields,
  or to the beginning of fields 1..n_fields+1. When the high-order bit of the offset
  at [i+1] is set (REC_OFFS_SQL_NULL), the field i is NULL. When the second
  high-order bit of the offset at [i+1] is set (REC_OFFS_EXTERNAL), the
  field i is being stored externally.  */

  ut_d(rec_offs_make_valid(m_rec, m_index, offsets));

  ut_a(n == rec_offs_n_fields(offsets));

  auto col_offsets{rec_offs_base(offsets)};

  /* Determine extra size and end offsets */

  if (rec_get_1byte_offs_flag(m_rec)) {

    *rec_offs_base(offsets) = REC_N_EXTRA_BYTES + n;

    for (ulint i{}; i < n; ++i) {
      /* Determine offsets to fields */
      auto offs = rec_1_get_field_end_info(m_rec, i);

      if (offs & REC_1BYTE_SQL_NULL_MASK) {
        offs &= ~REC_1BYTE_SQL_NULL_MASK;
        offs |= REC_OFFS_SQL_NULL;
      }

      col_offsets[i + 1] = offs;
    }

  } else {

    *rec_offs_base(offsets) = REC_N_EXTRA_BYTES + 2 * n;

    for (ulint i{}; i < n; ++i) {
      /* Determine offsets to fields */
      auto offs = rec_2_get_field_end_info(m_rec, i);

      if (offs & REC_2BYTE_SQL_NULL_MASK) {
        offs &= ~REC_2BYTE_SQL_NULL_MASK;
        offs |= REC_OFFS_SQL_NULL;
      }

      if (offs & REC_2BYTE_EXTERN_MASK) {
        offs &= ~REC_2BYTE_EXTERN_MASK;
        offs |= REC_OFFS_EXTERNAL;
        *rec_offs_base(offsets) |= REC_OFFS_EXTERNAL;
      }

      col_offsets[i + 1] = offs;
    }
  }

  return offsets;
}

void Phy_rec::encode(dict_index_t *index, rec_t *rec, ulint status, const DFields& dfields) noexcept {
  ulint null_mask = 1;
  ulint n_node_ptr_field;

  ut_ad(dfields.second > 0);
  ut_ad(dfields.second <= dict_index_get_n_fields(index));

  switch (expect(status, REC_STATUS_ORDINARY)) {
    case REC_STATUS_ORDINARY:
      n_node_ptr_field = ULINT_UNDEFINED;
      break;
    case REC_STATUS_NODE_PTR:
      ut_ad(dfields.second == dict_index_get_n_unique_in_tree(index) + 1);
      n_node_ptr_field = dfields.second - 1;
      break;
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      ut_ad(dfields.second == 1);
      n_node_ptr_field = ULINT_UNDEFINED;
      break;
    default:
      ut_error;
      return;
  }

  auto end = rec;
  auto nulls = rec - 1;
  auto lens = nulls - UT_BITS_IN_BYTES(index->n_nullable);

  /* Clear the SQL-null flags */
  memset(lens + 1, 0, nulls - lens);

  /* Store the data and the offsets */

  ulint i{};

  for (auto field = dfields.first; i < dfields.second; ++i, ++field) {
    const auto len = dfield_get_len(field);
    const auto type = dfield_get_type(field);

    if (unlikely(i == n_node_ptr_field)) {
      ut_ad(dtype_get_prtype(type) & DATA_NOT_NULL);
      ut_ad(len == 4);
      memcpy(end, dfield_get_data(field), len);
      end += len;
      break;
    }

    if (!(dtype_get_prtype(type) & DATA_NOT_NULL)) {
      /* Nullable field */
      ut_ad(index->n_nullable > 0);

      if (unlikely(!(byte)null_mask)) {
        nulls--;
        null_mask = 1;
      }

      ut_ad(*nulls < null_mask);

      /* Set the null flag if necessary */
      if (dfield_is_null(field)) {
        *nulls |= null_mask;
        null_mask <<= 1;
        continue;
      }

      null_mask <<= 1;
    }
    /* only nullable fields can be null */
    ut_ad(!dfield_is_null(field));

    const auto ifield = dict_index_get_nth_field(index, i);
    const auto fixed_len = ifield->fixed_len;

    /* If the maximum length of a variable-length field
    is up to 255 bytes, the actual length is always stored
    in one byte. If the maximum length is more than 255
    bytes, the actual length is stored in one byte for
    0..127.  The length will be encoded in two bytes when
    it is 128 or more, or when the field is stored externally. */
    if (fixed_len > 0) {

      ut_ad(len == fixed_len);
      ut_ad(!dfield_is_ext(field));

    } else if (dfield_is_ext(field)) {

      ut_ad(ifield->col->len >= 256 || ifield->col->mtype == DATA_BLOB);
      ut_ad(len <= REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE);

      *lens-- = (byte)(len >> 8) | 0xc0;
      *lens-- = (byte)len;

    } else {

      ut_ad(len <= dtype_get_len(type) || dtype_get_mtype(type) == DATA_BLOB || dtype_get_mtype(type) == DATA_DECIMAL);

      if (len < 128 || (dtype_get_len(type) < 256 && dtype_get_mtype(type) != DATA_BLOB)) {

        *lens-- = (byte)len;

      } else {

        ut_ad(len < 16384);

        *lens-- = (byte)(len >> 8) | 0x80;
        *lens-- = (byte)len;
      }
    }

    memcpy(end, dfield_get_data(field), len);
    end += len;
  }
}

Phy_rec::Size Phy_rec::get_encoded_size(dict_index_t* index, ulint, const DFields& dfields) noexcept {
  Size size{UT_BITS_IN_BYTES(index->n_nullable), 0};

  ut_ad(dfields.second > 0);

  /* Read the lengths of fields 0..n */
  for (ulint i{}; i < dfields.second; ++i) {
    const auto dfield{&dfields.first[i]};
    const auto len = dfield_get_len(dfield);
    const auto dict_field{dict_index_get_nth_field(index, i)};
    const auto col = dict_field_get_col(dict_field);

    ut_ad(dict_col_type_assert_equal(col, dfield_get_type(dfield)));

    if (dfield_is_null(dfield)) {
      /* No length is stored for NULL fields. */
      ut_ad(!(col->prtype & DATA_NOT_NULL));
      continue;
    }

    ut_ad(len <= col->len || col->mtype == DATA_BLOB || col->mtype == DATA_DECIMAL);

    /* If the maximum length of a variable-length field is up to 255 bytes, the actual
    length is always stored in one byte. If the maximum length is more than 255 bytes,
    the actual length is stored in one byte for 0..127.  The length will be encoded in
    two bytes when it is 128 or more, or when the field is stored externally. */

    if (dict_field->fixed_len) {
      ut_ad(len == dict_field->fixed_len);
      /* dict_index_add_col() should guarantee this */
      ut_ad(!dict_field->prefix_len || dict_field->fixed_len == dict_field->prefix_len);
    } else if (dfield_is_ext(dfield)) {
      ut_ad(col->len >= 256 || col->mtype == DATA_BLOB);
      size.first += 2;
    } else if (len < 128 || (col->len < 256 && col->mtype != DATA_BLOB)) {
      ++size.first;
    } else {
      /* For variable-length columns, we look up the maximum length from the column itself.
      If this is a prefix index column shorter than 256 bytes, this will waste one byte. */
      size.first += 2;
    }

    /* Data size. */
    size.second += len;
  }

  return size;
}

ulint rec_get_nth_field_offs(const rec_t *rec, ulint n, ulint *len) noexcept {
  ulint os;
  ulint next_os;

  ut_a(n < rec_get_n_fields(rec));

  if (rec_get_1byte_offs_flag(rec)) {
    os = rec_1_get_field_start_offs(rec, n);

    next_os = rec_1_get_field_end_info(rec, n);

    if (next_os & REC_1BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;

      return os;
    }

    next_os = next_os & ~REC_1BYTE_SQL_NULL_MASK;
  } else {
    os = rec_2_get_field_start_offs(rec, n);

    next_os = rec_2_get_field_end_info(rec, n);

    if (next_os & REC_2BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;

      return os;
    }

    next_os = next_os & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
  }

  *len = next_os - os;

  ut_ad(*len < UNIV_PAGE_SIZE);

  return os;
}

void rec_set_nth_field_null_bit(rec_t *rec, ulint i, bool val) noexcept {
  if (rec_get_1byte_offs_flag(rec)) {

    auto info = rec_1_get_field_end_info(rec, i);

    if (val) {
      info = info | REC_1BYTE_SQL_NULL_MASK;
    } else {
      info = info & ~REC_1BYTE_SQL_NULL_MASK;
    }

    rec_1_set_field_end_info(rec, i, info);

  } else {

    auto info = rec_2_get_field_end_info(rec, i);

    if (val) {
      info = info | REC_2BYTE_SQL_NULL_MASK;
    } else {
      info = info & ~REC_2BYTE_SQL_NULL_MASK;
    }

    rec_2_set_field_end_info(rec, i, info);
  }
}

void rec_set_nth_field_sql_null(rec_t *rec, ulint n) noexcept {
  auto offset = rec_get_field_start_offs(rec, n);

  data_write_sql_null(rec + offset, rec_get_nth_field_size(rec, n));

  rec_set_nth_field_null_bit(rec, n, true);
}

rec_t *rec_convert_dtuple_to_rec(byte *buf, const dict_index_t *index, const dtuple_t *dtuple, ulint n_ext) noexcept {

  ut_ad(dtuple_validate(dtuple));
  ut_ad(dtuple_check_typed(dtuple));

  ulint len;

  const auto n_fields = dtuple_get_n_fields(dtuple);
  const auto data_size = dtuple_get_data_size(dtuple);

  ut_ad(n_fields > 0);

  /* Calculate the offset of the origin in the physical record */
  auto rec = buf + rec_get_converted_extra_size(data_size, n_fields, n_ext);

  /* Suppress Valgrind warnings of ut_ad()
  in mach_write_to_1(), mach_write_to_2() et al. */
  ut_d(memset(buf, 0xff, rec - buf + data_size));

  /* Store the number of fields */
  rec_set_n_fields(rec, n_fields);

  /* Set the info bits of the record */
  rec_set_info_bits(rec, dtuple_get_info_bits(dtuple) & REC_INFO_BITS_MASK);

  /* Store the data and the offsets */

  ulint end_offset = 0;

  if (n_ext == 0 && data_size <= REC_1BYTE_OFFS_LIMIT) {

    rec_set_1byte_offs_flag(rec, true);

    for (ulint i = 0; i < n_fields; i++) {
      ulint ored_offset;
      auto field = dtuple_get_nth_field(dtuple, i);

      if (dfield_is_null(field)) {
        len = dtype_get_sql_null_size(dfield_get_type(field));
        data_write_sql_null(rec + end_offset, len);

        end_offset += len;
        ored_offset = end_offset | REC_1BYTE_SQL_NULL_MASK;
      } else {
        /* If the data is not SQL null, store it */
        len = dfield_get_len(field);

        memcpy(rec + end_offset, dfield_get_data(field), len);

        end_offset += len;
        ored_offset = end_offset;
      }

      rec_1_set_field_end_info(rec, i, ored_offset);
    }
  } else {
    rec_set_1byte_offs_flag(rec, false);

    for (ulint i = 0; i < n_fields; i++) {
      ulint ored_offset;
      auto field = dtuple_get_nth_field(dtuple, i);

      if (dfield_is_null(field)) {
        len = dtype_get_sql_null_size(dfield_get_type(field));
        data_write_sql_null(rec + end_offset, len);

        end_offset += len;
        ored_offset = end_offset | REC_2BYTE_SQL_NULL_MASK;
      } else {
        /* If the data is not SQL null, store it */
        len = dfield_get_len(field);

        memcpy(rec + end_offset, dfield_get_data(field), len);

        end_offset += len;

        ored_offset = end_offset;

        if (dfield_is_ext(field)) {
          ored_offset |= REC_2BYTE_EXTERN_MASK;
        }
      }

      rec_2_set_field_end_info(rec, i, ored_offset);
    }
  }

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap{};
    const ulint *offsets;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];

    rec_offs_init(offsets_);

    {
      Phy_rec record{index, rec};

      offsets = record.get_col_offsets(offsets_, ULINT_UNDEFINED, &heap, Source_location{});  
    }

    ut_ad(rec_validate(rec, offsets));
    ut_ad(dtuple_get_n_fields(dtuple) == rec_offs_n_fields(offsets));

    for (ulint i{}; i < rec_offs_n_fields(offsets); ++i) {
      ut_ad(!dfield_is_ext(dtuple_get_nth_field(dtuple, i)) == !rec_offs_nth_extern(offsets, i));
    }

    if (unlikely(heap != nullptr)) {
      mem_heap_free(heap);
    }
  }
#endif /* UNIV_DEBUG */

  return rec;
}

void rec_copy_prefix_to_dtuple(dtuple_t *tuple, const rec_t *rec, const dict_index_t *index, ulint n_fields, mem_heap_t *heap) noexcept {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, n_fields, &heap, Source_location{});
  }

  ut_ad(rec_validate(rec, offsets));
  ut_ad(dtuple_check_typed(tuple));

  dtuple_set_info_bits(tuple, rec_get_info_bits(rec));

  for (ulint i = 0; i < n_fields; i++) {
    ulint len;

    auto field = dtuple_get_nth_field(tuple, i);
    auto data = rec_get_nth_field(rec, offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      dfield_set_data(field, mem_heap_dup(heap, data, len), len);
      ut_ad(!rec_offs_nth_extern(offsets, i));
    } else {
      dfield_set_null(field);
    }
  }
}

rec_t *rec_copy_prefix_to_buf(const rec_t *rec, const dict_index_t *index, ulint n_fields, byte *&buf, ulint &buf_size) noexcept {
  prefetch_rw(buf);

  ut_ad(rec_validate(rec));


  ulint area_start;
  const auto area_end = rec_get_field_start_offs(rec, n_fields);

  if (rec_get_1byte_offs_flag(rec)) {
    area_start = REC_N_EXTRA_BYTES + n_fields;
  } else {
    area_start = REC_N_EXTRA_BYTES + 2 * n_fields;
  }

  const auto prefix_len = area_start + area_end;

  if (buf == nullptr || buf_size < prefix_len) {
    if (buf != nullptr) {
      mem_free(buf);
    }

    buf = static_cast<byte *>(mem_alloc2(prefix_len, &buf_size));
  }

  memcpy(buf, rec - area_start, prefix_len);

  auto copy_rec = buf + area_start;

  rec_set_n_fields(copy_rec, n_fields);

  return copy_rec;
}

/**
 * Validates the consistency of a physical record.
 * 
 * @param[in] rec  Record to validate.
 * 
 * @return	true if ok
 */
static bool rec_validate(const rec_t *rec) noexcept {
  ulint len_sum = 0;

  auto n_fields = rec_get_n_fields(rec);

  if (n_fields == 0 || n_fields > REC_MAX_N_FIELDS) {
    log_err(std::format("Record has {} fields", n_fields));
    return false;
  }

  for (ulint i{}; i < n_fields; i++) {
    ulint len;
    auto data = rec_get_nth_field(rec, i, &len);

    if (!(len < UNIV_PAGE_SIZE || len == UNIV_SQL_NULL)) {
      log_err(std::format("Record field {} len {}", i, len));
      return false;
    }

    if (len != UNIV_SQL_NULL) {
      ulint sum{};

      len_sum += len;

      /* Dereference the end of the field to cause a memory trap if possible */
      sum += *(data + len - 1);
      (void)sum;

    } else {
      len_sum += rec_get_nth_field_size(rec, i);
    }
  }

  if (len_sum != rec_get_data_size(rec)) {
    log_err(std::format("Record len should be {}, len {}", len_sum, rec_get_data_size(rec)));
    return false;
  } else {
    return true;
  }
}

bool rec_validate(const rec_t *rec, const ulint *offsets) noexcept {
  ulint len_sum{};
  const auto n_fields = rec_offs_n_fields(offsets);

  if (n_fields == 0 || n_fields > REC_MAX_N_FIELDS) {
    log_err(std::format("Record has {} fields", n_fields));
    return false;
  }

  ut_a(n_fields <= rec_get_n_fields(rec));

  for (ulint i = 0; i < n_fields; i++) {
    ulint len;
    auto data = rec_get_nth_field(rec, offsets, i, &len);

    if (!((len < UNIV_PAGE_SIZE) || (len == UNIV_SQL_NULL))) {
      log_err(std::format("Record field {} len {}", i, len));
      return false;
    }

    if (len != UNIV_SQL_NULL) {
      ulint sum{};

      len_sum += len;

      /* Dereference the end of the field to cause a memory trap if possible */
      sum += *(data + len - 1);
      (void)sum;

    } else {
      len_sum += rec_get_nth_field_size(rec, i);
    }
  }

  if (len_sum != rec_offs_data_size(offsets)) {
    log_err("Record len should be %lu, len %lu\n", len_sum, rec_offs_data_size(offsets));
    return false;
  }

  ut_a(rec_validate(rec));

  return true;
}

std::ostream &rec_print(std::ostream &os, const rec_t *rec) noexcept {
  auto n = rec_get_n_fields(rec);

  os << std::format("PHYSICAL RECORD: n_fields {}; {}-byte offsets; info bits {}\n",
		    n,
		    (rec_get_1byte_offs_flag(rec) ? 1 : 2),
		    rec_get_info_bits(rec));

  for (ulint i = 0; i < n; i++) {
    ulint len;
    auto data = rec_get_nth_field(rec, i, &len);

    os << std::format(" {}:", i);

    if (len != UNIV_SQL_NULL) {
      ut_print_buf(os, data, std::max(len, 30UL));
      os << std::format("; len: {} ", len);
    } else {
      os << std::format(" SQL NULL, size {} ", rec_get_nth_field_size(rec, i));
    }

    os << ";\n";
  }

  rec_validate(rec);

  return os;
}

void rec_print(const rec_t *rec) noexcept {
  std::ostringstream os{};

  rec_print(os, rec);

  log_info("{}", os.str().c_str());
}
