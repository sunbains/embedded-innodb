/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
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

/** @file rem/rem0rec.c
Record manager

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#include <format>
#include <sstream>

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

void Phy_rec::get_col_offsets(ulint *offsets) const noexcept {
  ulint offs{};
  ulint any_ext{};
  ulint null_mask{1};
  auto nulls = m_rec - 1;
  auto lens = nulls - UT_BITS_IN_BYTES(m_index->m_n_nullable);

#ifdef UNIV_DEBUG
  offsets[2] = reinterpret_cast<uintptr_t>(m_rec.get());
  offsets[3] = reinterpret_cast<uintptr_t>(m_index);
#endif /* UNIV_DEBUG */

  auto set_field_len = [&](ulint i, ulint len) {
    rec_offs_base(offsets)[i + 1] = len;
  };

  /* Read the lengths of fields 0..n */
  for (ulint i{}; i < rec_offs_n_fields(offsets); ++i) {
    const auto field = m_index->get_nth_field(i);

    if (!(field->get_col()->prtype & DATA_NOT_NULL)) {
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

    if (unlikely(field->m_fixed_len == 0)) {
      ulint len = *lens;

      --lens;

      /* Variable-length field: read the length */
      const auto col = field->get_col();

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
            ut_ad(m_index->is_clustered());
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
      offs += field->m_fixed_len;
      set_field_len(i, offs);
    }
  }

  *rec_offs_base(offsets) = (m_rec - (lens + 1)) | any_ext;
}

ulint *Phy_rec::get_col_offsets(ulint *offsets, ulint n_fields, mem_heap_t **heap, Source_location sl) const noexcept {
  auto n = m_rec.get_n_fields();

  if (unlikely(n_fields < n)) {
    n = n_fields;
  }

  auto size = n + 1 + REC_OFFS_HEADER_SIZE;

  if (unlikely(offsets == nullptr) || unlikely(rec_offs_get_n_alloc(offsets) < size)) {
    if (unlikely(*heap == nullptr)) {
      *heap = mem_heap_create_func(size * sizeof(ulint), MEM_HEAP_DYNAMIC, std::move(sl));
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

  ut_d(m_rec.offs_make_valid(m_index, offsets));

  ut_a(n == rec_offs_n_fields(offsets));

  auto col_offsets{rec_offs_base(offsets)};

  /* Determine extra size and end offsets */

  if (m_rec.get_1byte_offs_flag()) {

    *rec_offs_base(offsets) = REC_N_EXTRA_BYTES + n;

    for (ulint i{}; i < n; ++i) {
      /* Determine offsets to fields */
      auto offs = m_rec.get_1_field_end_info(i);

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
      auto offs = m_rec.get_2_field_end_info(i);

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

void Phy_rec::encode(Index *index, Rec rec, ulint status, const DFields &dfields) noexcept {
  ulint null_mask = 1;
  ulint n_node_ptr_field;

  auto [dfield_array, n_fields] = dfields;
  ut_ad(n_fields > 0);
  ut_ad(n_fields <= index->get_n_fields());

  switch (expect(status, REC_STATUS_ORDINARY)) {
    case REC_STATUS_ORDINARY:
      n_node_ptr_field = ULINT_UNDEFINED;
      break;
    case REC_STATUS_NODE_PTR:
      ut_ad(n_fields == index->get_n_unique() + 1);
      n_node_ptr_field = n_fields - 1;
      break;
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      ut_ad(n_fields == 1);
      n_node_ptr_field = ULINT_UNDEFINED;
      break;
    default:
      ut_error;
      return;
  }

  auto end = rec.get();
  auto nulls = rec.get() - 1;
  auto lens = nulls - UT_BITS_IN_BYTES(index->m_n_nullable);

  /* Clear the SQL-null flags */
  memset(lens + 1, 0, nulls - lens);

  /* Store the data and the offsets */

  ulint i{};

  for (auto field = dfield_array; i < n_fields; ++i, ++field) {
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
      ut_ad(index->m_n_nullable > 0);

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

    const auto ifield = index->get_nth_field(i);
    const auto fixed_len = ifield->m_fixed_len;

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

      ut_ad(ifield->m_col->len >= 256 || ifield->m_col->mtype == DATA_BLOB);
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

Phy_rec::Size Phy_rec::get_encoded_size(Index *index, ulint, const DFields &dfields) noexcept {
  Size size{UT_BITS_IN_BYTES(index->m_n_nullable), 0};

  ut_ad(dfields.second > 0);

  /* Read the lengths of fields 0..n */
  for (ulint i{}; i < dfields.second; ++i) {
    const auto dfield{&dfields.first[i]};
    const auto len = dfield_get_len(dfield);
    const auto dict_field{index->get_nth_field(i)};
    const auto col = dict_field->get_col();

    ut_ad(col->assert_equal(dfield_get_type(dfield)));

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

    if (dict_field->m_fixed_len) {
      ut_ad(len == dict_field->m_fixed_len);
      /* index_add_col() should guarantee this */
      ut_ad(!dict_field->m_prefix_len || dict_field->m_fixed_len == dict_field->m_prefix_len);
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

ulint rec_offs_get_nth_field(const Rec &rec, ulint n, ulint *len) noexcept {
  ulint os;
  ulint next_os;

  ut_a(n < rec.get_n_fields());

  if (rec.get_1byte_offs_flag()) {
    os = rec.get_1_field_start_offs(n);

    next_os = rec.get_1_field_end_info(n);

    if (next_os & REC_1BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;

      return os;
    }

    next_os = next_os & ~REC_1BYTE_SQL_NULL_MASK;
  } else {
    os = rec.get_2_field_start_offs(n);

    next_os = rec.get_2_field_end_info(n);

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

void rec_set_nth_field_null_bit(Rec &rec, ulint i, bool val) noexcept {
  if (rec.get_1byte_offs_flag()) {

    auto info = rec.get_1_field_end_info(i);

    if (val) {
      info = info | REC_1BYTE_SQL_NULL_MASK;
    } else {
      info = info & ~REC_1BYTE_SQL_NULL_MASK;
    }

    rec.set_1_field_end_info(i, info);

  } else {

    auto info = rec.get_2_field_end_info(i);

    if (val) {
      info = info | REC_2BYTE_SQL_NULL_MASK;
    } else {
      info = info & ~REC_2BYTE_SQL_NULL_MASK;
    }

    rec.set_2_field_end_info(i, info);
  }
}

void rec_set_nth_field_sql_null(Rec &rec, ulint n) noexcept {
  auto offset = rec.get_field_start_offs(n);

  data_write_sql_null(rec.get() + offset, rec.get_nth_field_size(n));

  rec_set_nth_field_null_bit(rec, n, true);
}

Rec rec_convert_dtuple_to_rec(rec_t *buf, const Index *index, const DTuple *dtuple, ulint n_ext) noexcept {

  ut_ad(dtuple_validate(dtuple));
  ut_ad(dtuple_check_typed(dtuple));

  ulint len;

  const auto n_fields = dtuple_get_n_fields(dtuple);
  const auto data_size = dtuple_get_data_size(dtuple);

  ut_ad(n_fields > 0);

  /* Calculate the offset of the origin in the physical record */
  auto rec_ptr = buf + rec_get_converted_extra_size(data_size, n_fields, n_ext);
  Rec rec(rec_ptr);

  /* Suppress Valgrind warnings of ut_ad()
  in mach_write_to_1(), mach_write_to_2() et al. */
  ut_d(memset(buf, 0xff, rec_ptr - buf + data_size));

  /* Store the number of fields */
  rec.set_n_fields(n_fields);

  /* Set the info bits of the record */
  rec.set_info_bits(dtuple_get_info_bits(dtuple) & REC_INFO_BITS_MASK);

  /* Store the data and the offsets */

  ulint end_offset = 0;

  if (n_ext == 0 && data_size <= REC_1BYTE_OFFS_LIMIT) {

    rec.set_1byte_offs_flag(true);

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

      rec.set_1_field_end_info(i, ored_offset);
    }
  } else {
    rec.set_1byte_offs_flag(false);

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

      rec.set_2_field_end_info(i, ored_offset);
    }
  }

#ifdef UNIV_DEBUG
  {
    mem_heap_t *heap{};
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();

    rec_offs_init(offsets_);

    {
      Phy_rec record{index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    ut_ad(rec.validate(offsets));
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

void rec_copy_prefix_to_dtuple(DTuple *tuple, const Rec &rec, const Index *index, ulint n_fields, mem_heap_t *heap) noexcept {
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();

  rec_offs_init(offsets_);

  {
    Phy_rec record{index, rec};

    offsets = record.get_col_offsets(offsets, n_fields, &heap, Current_location());
  }

  ut_ad(rec.validate(offsets));
  ut_ad(dtuple_check_typed(tuple));

  dtuple_set_info_bits(tuple, rec.get_info_bits());

  for (ulint i = 0; i < n_fields; i++) {
    ulint len;

    auto field = dtuple_get_nth_field(tuple, i);
    auto data = rec.get_nth_field(offsets, i, &len);

    if (len != UNIV_SQL_NULL) {
      dfield_set_data(field, mem_heap_dup(heap, data, len), len);
      ut_ad(!rec_offs_nth_extern(offsets, i));
    } else {
      dfield_set_null(field);
    }
  }
}

Rec rec_copy_prefix_to_buf(const Rec &rec, const Index *index, ulint n_fields, byte *&buf, ulint &buf_size) noexcept {
  prefetch_rw(buf);

  ut_ad(rec.validate(nullptr));

  ulint area_start;
  const auto area_end = rec.get_field_start_offs(n_fields);

  if (rec.get_1byte_offs_flag()) {
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

  memcpy(buf, rec.get() - area_start, prefix_len);

  auto copy_rec_ptr = buf + area_start;
  Rec copy_rec(copy_rec_ptr);

  copy_rec.set_n_fields(n_fields);

  return copy_rec;
}

bool rec_validate(const Rec &rec, const ulint *offsets) noexcept {
  ulint len_sum{};
  const auto n_fields = rec_offs_n_fields(offsets);

  if (n_fields == 0 || n_fields > REC_MAX_N_FIELDS) {
    log_err(std::format("Record has {} fields", n_fields));
    return false;
  }

  ut_a(n_fields <= rec.get_n_fields());

  for (ulint i = 0; i < n_fields; ++i) {
    ulint len;
    auto data = rec.get_nth_field(offsets, i, &len);

    if (!((len < UNIV_PAGE_SIZE) || (len == UNIV_SQL_NULL))) {
      log_err(std::format("Record field {} len {}", i, len));
      return false;
    }

    if (len != UNIV_SQL_NULL) {
      ulint sum{};

      len_sum += len;

      /* Dereference the end of the field to cause a memory trap if possible */
      sum += *(data.get() + len - 1);
      (void)sum;

    } else {
      len_sum += rec.get_nth_field_size(i);
    }
  }

  if (len_sum != rec_offs_data_size(offsets)) {
    log_err("Record len should be %lu, len %lu\n", len_sum, rec_offs_data_size(offsets));
    return false;
  }

  ut_a(rec.validate(offsets));

  return true;
}

std::string rec_to_string(const Rec &rec) noexcept {
  auto n = rec.get_n_fields();

  auto str = std::format(
    "PHYSICAL RECORD: n_fields {}; {}-byte offsets; info bits {}", n, (rec.get_1byte_offs_flag() ? 1 : 2), rec.get_info_bits()
  );

  for (ulint i{}; i < n; ++i) {
    str += std::format(" {}:", i);

    ulint len;
    auto data = rec.get_nth_field(i, &len);

    if (len != UNIV_SQL_NULL) {
      std::ostringstream os{};

      buf_to_hex_string(os, data.get(), std::max(len, 30UL));

      str += os.str();
      str += std::format("; len: {} ", len);
    } else {
      str += std::format(" SQL NULL, size {} ", rec.get_nth_field_size(i));
    }
  }

  IF_DEBUG(rec.validate(nullptr));

  return str;
}

void rec_info_info(const Rec &rec) noexcept {
  log_info(rec.to_string());
}

// Rec method implementations
ulint Rec::get_nth_field_offs(ulint n, ulint *len) const noexcept {
  ulint os;
  ulint next_os;

  ut_a(n < get_n_fields());

  if (get_1byte_offs_flag()) {
    os = get_1_field_start_offs(n);
    next_os = get_1_field_end_info(n);

    if (next_os & REC_1BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;
      return os;
    }

    next_os = next_os & ~REC_1BYTE_SQL_NULL_MASK;
  } else {
    os = get_2_field_start_offs(n);
    next_os = get_2_field_end_info(n);

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

void Rec::set_nth_field_null_bit(ulint i, bool val) noexcept {
  if (get_1byte_offs_flag()) {
    auto info = get_1_field_end_info(i);
    if (val) {
      info |= REC_1BYTE_SQL_NULL_MASK;
    } else {
      info &= ~REC_1BYTE_SQL_NULL_MASK;
    }
    set_1_field_end_info(i, info);
  } else {
    auto info = get_2_field_end_info(i);
    if (val) {
      info |= REC_2BYTE_SQL_NULL_MASK;
    } else {
      info &= ~REC_2BYTE_SQL_NULL_MASK;
    }
    set_2_field_end_info(i, info);
  }
}

void Rec::set_nth_field_sql_null(ulint n) noexcept {
  set_nth_field_null_bit(n, true);
}

void Rec::log_record_info() const noexcept {
  rec_info_info(*this);
}

bool Rec::validate(const ulint *offsets) const noexcept {
  return rec_validate(*this, offsets);
}

bool Rec::offs_validate(const Index *index, const ulint *offsets) const noexcept {
  ulint last = ULINT_MAX;
  ulint i = rec_offs_n_fields(offsets);

  if (!is_null()) {
    ut_ad((ulint)m_rec == offsets[2]);
    ut_a(get_n_fields() >= i);
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

Rec Rec::copy_prefix_to_buf(const Index *index, ulint n_fields, byte *&buf, ulint &buf_size) const noexcept {
  return rec_copy_prefix_to_buf(*this, index, n_fields, buf, buf_size);
}

void Rec::copy_prefix_to_dtuple(DTuple *tuple, const Index *index, ulint n_fields, mem_heap_t *heap) const noexcept {
  rec_copy_prefix_to_dtuple(tuple, *this, index, n_fields, heap);
}

Rec Rec::convert_dtuple_to_rec(byte *buf, const Index *index, const DTuple *dtuple, ulint n_ext) noexcept {
  return rec_convert_dtuple_to_rec(buf, index, dtuple, n_ext);
}
