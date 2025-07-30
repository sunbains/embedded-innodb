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

/** @file rem/rem0cmp.c
Comparison services for records

Created 7/1/1994 Heikki Tuuri
************************************************************************/

#include "rem0cmp.h"

#include "api0api.h"
#include "api0ucode.h"
#include "srv0srv.h"

/*		ALPHABETICAL ORDER

The records are put into alphabetical order in the following
way: let F be the first field where two records disagree.
If there is a character in some position n where the
records disagree, the order is determined by comparison of
the characters at position n, possibly after
collating transformation. If there is no such character,
but the corresponding fields have different lengths, then
if the data type of the fields is paddable,
shorter field is padded with a padding character. If the
data type is not paddable, longer field is considered greater.
Finally, the SQL null is bigger than any other value.

At the present, the comparison functions return 0 in the case,
where two records disagree only in the way that one
has more fields than the other. */

#ifdef UNIV_DEBUG
/**
 * Used in debug checking of cmp_dtuple_... .
 * This function is used to compare a data tuple to a physical record. If
 * dtuple has n fields then rec must have either m >= n fields, or it must
 * differ from dtuple in some of the m fields rec has.
 * 
 * @param[in] cmp_ctx client compare context
 * @param[in] dtuple data tuple
 * @param[in] rec physical record which differs from dtuple in some of the common fields,
 *                or which has an equal number or more fields than dtuple
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 * @param[in/out] matched_fields number of already completely matched fields; when function
 *                returns, contains the value for current comparison
 * 
 * @return 1, 0, -1, if dtuple is greater, equal, less than rec,
 *  respectively, when only the common first fields are compared
 */
static int cmp_debug_dtuple_rec_with_match(
  void *cmp_ctx, const DTuple *dtuple, const rec_t *rec, const ulint *offsets, ulint *matched_fields
) noexcept;
#endif /* UNIV_DEBUG */

/**
 * Transforms the character code so that it is ordered appropriately for the
 * language. This is only used for the latin1 char set. The client does the
 * comparisons for other char sets.
 * 
 * @param[in] code code of a character stored in database record
 * 
 * @return	collation order position
 */
inline ulint cmp_collate(ulint code) noexcept {
  // return((ulint) srv_latin1_ordering[code]);
  /* FIXME: Default to ASCII */
  return code;
}

bool cmp_cols_are_equal(const Column *col1, const Column *col2, bool check_charsets) noexcept {
  if (dtype_is_non_binary_string_type(col1->mtype, col1->prtype) && dtype_is_non_binary_string_type(col2->mtype, col2->prtype)) {

    /* Both are non-binary string types: they can be compared if
    and only if the charset-collation is the same */

    if (check_charsets) {
      return dtype_get_charset_coll(col1->prtype == dtype_get_charset_coll(col2->prtype));
    } else {
      return true;
    }
  }

  if (dtype_is_binary_string_type(col1->mtype, col1->prtype) && dtype_is_binary_string_type(col2->mtype, col2->prtype)) {

    /* Both are binary string types: they can be compared */

    return true;
  }

  if (col1->mtype != col2->mtype) {

    return false;
  }

  if (col1->mtype == DATA_INT && (col1->prtype & DATA_UNSIGNED) != (col2->prtype & DATA_UNSIGNED)) {

    /* The storage format of an unsigned integer is different
    from a signed integer: in a signed integer we OR
    0x8000... to the value of positive integers. */

    return false;
  }

  return col1->mtype != DATA_INT || col1->len == col2->len;
}

/**
 * InnoDB uses this function to compare two data fields for which the
 * data type is such that we must compare whole fields or call the client
 * to do the comparison
 * 
 * @param[in] cmp_ctx client compare context
 * @param[in] mtype main type
 * @param[in] prtype precise type
 * @param[in] a data field
 * @param[in] a_length data field length, not UNIV_SQL_NULL
 * @param[in] b data field
 * @param[in] b_length data field length, not UNIV_SQL_NULL
 * 
 * @return	1, 0, -1, if a is greater, equal, less than b, respectively
 */
static int cmp_whole_field(
  void *cmp_ctx, ulint mtype, uint16_t prtype, const byte *a, unsigned int a_length, const byte *b, unsigned int b_length
) noexcept {
  int swap_flag = 1;

  switch (mtype) {

    case DATA_DECIMAL:
      /* Remove preceding spaces */
      for (; a_length && *a == ' '; a++, a_length--)
        ;
      for (; b_length && *b == ' '; b++, b_length--)
        ;

      if (*a == '-') {
        if (*b != '-') {
          return -1;
        }

        a++;
        b++;
        a_length--;
        b_length--;

        swap_flag = -1;

      } else if (*b == '-') {

        return 1;
      }

      while (a_length > 0 && (*a == '+' || *a == '0')) {
        a++;
        a_length--;
      }

      while (b_length > 0 && (*b == '+' || *b == '0')) {
        b++;
        b_length--;
      }

      if (a_length != b_length) {
        if (a_length < b_length) {
          return -swap_flag;
        }

        return swap_flag;
      }

      while (a_length > 0 && *a == *b) {

        a++;
        b++;
        a_length--;
      }

      if (a_length == 0) {

        return 0;
      }

      if (*a > *b) {
        return swap_flag;
      }

      return -swap_flag;
    case DATA_DOUBLE: {
      auto d_1 = mach_double_read(a);
      auto d_2 = mach_double_read(b);

      if (d_1 > d_2) {
        return 1;
      } else if (d_2 > d_1) {
        return -1;
      }

      return 0;
    }

    case DATA_FLOAT: {
      auto f_1 = mach_float_read(a);
      auto f_2 = mach_float_read(b);

      if (f_1 > f_2) {
        return 1;
      } else if (f_2 > f_1) {
        return -1;
      }

      return 0;
    }
    case DATA_BLOB:
      if (prtype & DATA_BINARY_TYPE) {
        log_err("Comparing a binary BLOB with a character set sensitive comparison!");
      }
      /* fall through */
    case DATA_VARCLIENT:
    case DATA_CLIENT: {
      ib_col_meta_t ib_col_meta;

      /* FIXME: We should do this once at a higher level. Current
      impact is on perfromance. */
      ib_col_meta.type = ib_col_type_t(mtype);

      /* FIXME: Set the length where it's known. */
      ib_col_meta.type_len = 0;
      ib_col_meta.attr = IB_COL_NONE;
      ib_col_meta.client_type = prtype & DATA_CLIENT_TYPE_MASK;

      const auto attr{to_int(ib_col_meta.attr)};

      if (prtype & DATA_NOT_NULL) {
        ib_col_meta.attr = static_cast<ib_col_attr_t>(attr | IB_COL_NOT_NULL);
      }

      if (prtype & DATA_UNSIGNED) {
        ib_col_meta.attr = static_cast<ib_col_attr_t>(attr | IB_COL_UNSIGNED);
      }

      if (prtype & DATA_CUSTOM_TYPE) {
        ib_col_meta.attr = static_cast<ib_col_attr_t>(attr | IB_COL_CUSTOM1);
      }

      if (prtype & (DATA_CUSTOM_TYPE << 1)) {
        ib_col_meta.attr = static_cast<ib_col_attr_t>(attr | IB_COL_CUSTOM2);
      }

      if (prtype & (DATA_CUSTOM_TYPE << 2)) {
        ib_col_meta.attr = static_cast<ib_col_attr_t>(attr | IB_COL_CUSTOM3);
      }
      return ib_client_compare(&ib_col_meta, a, a_length, b, b_length);
    }
    default:
      log_fatal("Unknown type number ", mtype);
  }

  return 0;
}

int cmp_data_data_slow(
  void *cmp_ctx, ulint mtype, ulint prtype, const byte *data1, ulint len1, const byte *data2, ulint len2
) noexcept {

  ulint data1_byte;
  ulint data2_byte;
  ulint cur_bytes;

  if (len1 == UNIV_SQL_NULL || len2 == UNIV_SQL_NULL) {

    if (len1 == len2) {

      return 0;
    }

    if (len1 == UNIV_SQL_NULL) {
      /* We define the SQL null to be the smallest possible
      value of a field in the alphabetical order */

      return -1;
    }

    return 1;
  }

  if (mtype >= DATA_FLOAT || (mtype == DATA_BLOB && 0 == (prtype & DATA_BINARY_TYPE) &&
                              dtype_get_charset_coll(prtype) != DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL)) {

    /* prtype is really a 16 unsigned type. */
    return cmp_whole_field(cmp_ctx, mtype, (uint16_t)prtype, data1, (unsigned)len1, data2, (unsigned)len2);
  }

  /* Compare then the fields */

  cur_bytes = 0;

  for (;;) {
    if (len1 <= cur_bytes) {
      if (len2 <= cur_bytes) {

        return 0;
      }

      data1_byte = dtype_get_pad_char(mtype, prtype);

      if (data1_byte == ULINT_UNDEFINED) {

        return -1;
      }
    } else {
      data1_byte = *data1;
    }

    if (len2 <= cur_bytes) {
      data2_byte = dtype_get_pad_char(mtype, prtype);

      if (data2_byte == ULINT_UNDEFINED) {

        return 1;
      }
    } else {
      data2_byte = *data2;
    }

    if (data1_byte == data2_byte) {
      /* If the bytes are equal, they will remain such even
      after the collation transformation below */

      goto next_byte;
    }

    if (mtype <= DATA_CHAR || (mtype == DATA_BLOB && 0 == (prtype & DATA_BINARY_TYPE))) {

      data1_byte = cmp_collate(data1_byte);
      data2_byte = cmp_collate(data2_byte);
    }

    if (data1_byte > data2_byte) {

      return 1;
    } else if (data1_byte < data2_byte) {

      return -1;
    }
  next_byte:
    /* Next byte */
    cur_bytes++;
    data1++;
    data2++;
  }

  return 0; /* Not reached */
}

int cmp_dtuple_rec_with_match(
  void *cmp_ctx, const DTuple *dtuple, const rec_t *rec, const ulint *offsets, ulint *matched_fields, ulint *matched_bytes
) noexcept {
  const dfield_t *dtuple_field;
  ulint dtuple_f_len;
  const byte *dtuple_b_ptr;
  ulint dtuple_byte;
  ulint rec_f_len;
  const byte *rec_b_ptr;
  ulint rec_byte;
  ulint cur_field;
  ulint cur_bytes;
  int ret = 3333;

  ut_ad(dtuple && rec && matched_fields && matched_bytes);
  ut_ad(dtuple_check_typed(dtuple));
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  cur_field = *matched_fields;
  cur_bytes = *matched_bytes;

  ut_ad(cur_field <= dtuple_get_n_fields_cmp(dtuple));
  ut_ad(cur_field <= rec_offs_n_fields(offsets));

  if (cur_bytes == 0 && cur_field == 0) {
    const auto rec_info = rec_get_info_bits(rec);
    const auto tup_info = dtuple_get_info_bits(dtuple);

    if (unlikely(rec_info & REC_INFO_MIN_REC_FLAG)) {
      ret = !(tup_info & REC_INFO_MIN_REC_FLAG);
      goto order_resolved;
    } else if (unlikely(tup_info & REC_INFO_MIN_REC_FLAG)) {
      ret = -1;
      goto order_resolved;
    }
  }

  /* Match fields in a loop; stop if we run out of fields in dtuple
  or find an externally stored field */

  while (cur_field < dtuple_get_n_fields_cmp(dtuple)) {

    ulint mtype;
    uint16_t prtype;

    dtuple_field = dtuple_get_nth_field(dtuple, cur_field);

    {
      const auto type = dfield_get_type(dtuple_field);

      mtype = type->mtype;
      prtype = type->prtype;
    }

    dtuple_f_len = dfield_get_len(dtuple_field);

    rec_b_ptr = rec_get_nth_field(rec, offsets, cur_field, &rec_f_len);

    /* If we have matched yet 0 bytes, it may be that one or
    both the fields are SQL null, or the record or dtuple may be
    the predefined minimum record, or the field is externally
    stored */

    if (likely(cur_bytes == 0)) {
      if (rec_offs_nth_extern(offsets, cur_field)) {
        /* We do not compare to an externally
        stored field */

        ret = 0;
        goto order_resolved;
      }

      if (dtuple_f_len == UNIV_SQL_NULL) {
        if (rec_f_len == UNIV_SQL_NULL) {

          goto next_field;
        }

        ret = -1;
        goto order_resolved;
      } else if (rec_f_len == UNIV_SQL_NULL) {
        /* We define the SQL null to be the
        smallest possible value of a field
        in the alphabetical order */

        ret = 1;
        goto order_resolved;
      }
    }

    if (mtype >= DATA_FLOAT || (mtype == DATA_BLOB && 0 == (prtype & DATA_BINARY_TYPE) &&
                                dtype_get_charset_coll(prtype) != DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL)) {

      ret = cmp_whole_field(
        cmp_ctx, mtype, prtype, (byte *)dfield_get_data(dtuple_field), (unsigned)dtuple_f_len, rec_b_ptr, (unsigned)rec_f_len
      );

      if (ret != 0) {
        cur_bytes = 0;

        goto order_resolved;
      } else {
        goto next_field;
      }
    }

    /* Set the pointers at the current byte */

    rec_b_ptr = rec_b_ptr + cur_bytes;
    dtuple_b_ptr = (byte *)dfield_get_data(dtuple_field) + cur_bytes;

    /* Compare then the fields */
    for (;;) {
      if (unlikely(rec_f_len <= cur_bytes)) {
        if (dtuple_f_len <= cur_bytes) {

          goto next_field;
        }

        rec_byte = dtype_get_pad_char(mtype, prtype);

        if (rec_byte == ULINT_UNDEFINED) {
          ret = 1;

          goto order_resolved;
        }
      } else {
        rec_byte = *rec_b_ptr;
      }

      if (unlikely(dtuple_f_len <= cur_bytes)) {
        dtuple_byte = dtype_get_pad_char(mtype, prtype);

        if (dtuple_byte == ULINT_UNDEFINED) {
          ret = -1;

          goto order_resolved;
        }
      } else {
        dtuple_byte = *dtuple_b_ptr;
      }

      if (dtuple_byte == rec_byte) {
        /* If the bytes are equal, they will remain such even after the collation
        transformation below */

        goto next_byte;
      }

      if (mtype <= DATA_CHAR || (mtype == DATA_BLOB && !(prtype & DATA_BINARY_TYPE))) {

        rec_byte = cmp_collate(rec_byte);
        dtuple_byte = cmp_collate(dtuple_byte);
      }

      ret = (int)(dtuple_byte - rec_byte);

      if (likely(ret != 0)) {
        if (ret < 0) {
          ret = -1;
          goto order_resolved;
        } else {
          ret = 1;
          goto order_resolved;
        }
      }
    next_byte:
      /* Next byte */
      cur_bytes++;
      rec_b_ptr++;
      dtuple_b_ptr++;
    }

  next_field:
    cur_field++;
    cur_bytes = 0;
  }

  ut_ad(cur_bytes == 0);

  /* If we ran out of fields, dtuple was equal to rec up to the common fields */
  ret = 0;

order_resolved:

  ut_ad(ret >= -1 && ret <= 1);
  ut_ad(ret == cmp_debug_dtuple_rec_with_match(cmp_ctx, dtuple, rec, offsets, matched_fields));

  /* In the debug version, the above cmp_debug_... sets *matched_fields to a value */
  ut_ad(*matched_fields == cur_field);

  *matched_fields = cur_field;
  *matched_bytes = cur_bytes;

  return ret;
}

int cmp_dtuple_rec(void *cmp_ctx, const DTuple *dtuple, const rec_t *rec, const ulint *offsets) noexcept {
  ulint matched_fields = 0;
  ulint matched_bytes = 0;

  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  return cmp_dtuple_rec_with_match(cmp_ctx, dtuple, rec, offsets, &matched_fields, &matched_bytes);
}

bool cmp_dtuple_is_prefix_of_rec(void *cmp_ctx, const DTuple *dtuple, const rec_t *rec, const ulint *offsets) noexcept {
  ulint n_fields;
  ulint matched_fields = 0;
  ulint matched_bytes = 0;

  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  n_fields = dtuple_get_n_fields(dtuple);

  if (n_fields > rec_offs_n_fields(offsets)) {

    return false;
  }

  cmp_dtuple_rec_with_match(cmp_ctx, dtuple, rec, offsets, &matched_fields, &matched_bytes);

  if (matched_fields == n_fields) {

    return true;
  }

  if (matched_fields == n_fields - 1 && matched_bytes == dfield_get_len(dtuple_get_nth_field(dtuple, n_fields - 1))) {
    return true;
  }

  return false;
}

int cmp_rec_rec_simple(
  const rec_t *rec1, const rec_t *rec2, const ulint *offsets1, const ulint *offsets2, const Index *index
) noexcept {
  ulint rec1_f_len;
  const byte *rec1_b_ptr;
  ulint rec1_byte;
  ulint rec2_f_len;
  const byte *rec2_b_ptr;
  ulint rec2_byte;
  ulint cur_field;

  auto n_uniq = index->get_n_unique();

  ut_ad(rec_offs_n_fields(offsets1) >= n_uniq);
  ut_ad(rec_offs_n_fields(offsets2) >= n_uniq);

  ut_ad(rec_offs_base(offsets1) == rec_offs_base(offsets2));

  for (cur_field = 0; cur_field < n_uniq; cur_field++) {

    ulint mtype;
    uint16_t prtype;
    ulint cur_bytes;

    {
      const auto col = index->get_nth_col(cur_field);

      mtype = col->mtype;
      prtype = col->prtype;
    }

    ut_ad(!rec_offs_nth_extern(offsets1, cur_field));
    ut_ad(!rec_offs_nth_extern(offsets2, cur_field));

    rec1_b_ptr = rec_get_nth_field(rec1, offsets1, cur_field, &rec1_f_len);
    rec2_b_ptr = rec_get_nth_field(rec2, offsets2, cur_field, &rec2_f_len);

    if (rec1_f_len == UNIV_SQL_NULL || rec2_f_len == UNIV_SQL_NULL) {

      if (rec1_f_len == rec2_f_len) {

        goto next_field;

      } else if (rec2_f_len == UNIV_SQL_NULL) {

        /* We define the SQL null to be the
        smallest possible value of a field
        in the alphabetical order */

        return 1;
      } else {
        return -1;
      }
    }

    if (mtype >= DATA_FLOAT || (mtype == DATA_BLOB && 0 == (prtype & DATA_BINARY_TYPE) &&
                                dtype_get_charset_coll(prtype) != DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL)) {

      int ret;

      ret = cmp_whole_field(index->m_cmp_ctx, mtype, prtype, rec1_b_ptr, (unsigned)rec1_f_len, rec2_b_ptr, (unsigned)rec2_f_len);

      if (ret) {
        return ret;
      }

      goto next_field;
    }

    /* Compare the fields */
    for (cur_bytes = 0;; cur_bytes++, rec1_b_ptr++, rec2_b_ptr++) {
      if (rec2_f_len <= cur_bytes) {

        if (rec1_f_len <= cur_bytes) {

          goto next_field;
        }

        rec2_byte = dtype_get_pad_char(mtype, prtype);

        if (rec2_byte == ULINT_UNDEFINED) {
          return 1;
        }
      } else {
        rec2_byte = *rec2_b_ptr;
      }

      if (rec1_f_len <= cur_bytes) {
        rec1_byte = dtype_get_pad_char(mtype, prtype);

        if (rec1_byte == ULINT_UNDEFINED) {
          return -1;
        }
      } else {
        rec1_byte = *rec1_b_ptr;
      }

      if (rec1_byte == rec2_byte) {
        /* If the bytes are equal, they will remain
        such even after the collation transformation
        below */

        continue;
      }

      if (mtype <= DATA_CHAR || (mtype == DATA_BLOB && !(prtype & DATA_BINARY_TYPE))) {

        rec1_byte = cmp_collate(rec1_byte);
        rec2_byte = cmp_collate(rec2_byte);
      }

      if (rec1_byte < rec2_byte) {
        return -1;
      } else if (rec1_byte > rec2_byte) {
        return 1;
      }
    }
  next_field:
    continue;
  }

  /* If we ran out of fields, rec1 was equal to rec2. */
  return 0;
}

int cmp_rec_rec_with_match(
  const rec_t *rec1, const rec_t *rec2, const ulint *offsets1, const ulint *offsets2, const Index *index, ulint *matched_fields,
  ulint *matched_bytes
) noexcept {
  ulint rec1_f_len;
  const byte *rec1_b_ptr;
  ulint rec1_byte;
  ulint rec2_f_len;
  const byte *rec2_b_ptr;
  ulint rec2_byte;
  int ret = 0;

  ut_ad(rec_offs_validate(rec1, index, offsets1));
  ut_ad(rec_offs_validate(rec2, index, offsets2));

  auto rec1_n_fields = rec_offs_n_fields(offsets1);
  auto rec2_n_fields = rec_offs_n_fields(offsets2);

  auto cur_field = *matched_fields;
  auto cur_bytes = *matched_bytes;

  /* Match fields in a loop */

  while ((cur_field < rec1_n_fields) && (cur_field < rec2_n_fields)) {

    const auto col = index->get_nth_col(cur_field);

    auto mtype = col->mtype;
    uint16_t prtype = col->prtype;

    rec1_b_ptr = rec_get_nth_field(rec1, offsets1, cur_field, &rec1_f_len);
    rec2_b_ptr = rec_get_nth_field(rec2, offsets2, cur_field, &rec2_f_len);

    if (cur_bytes == 0) {
      if (cur_field == 0) {
        /* Test if rec is the predefined minimum
        record */
        if (unlikely(rec_get_info_bits(rec1) & REC_INFO_MIN_REC_FLAG)) {

          if (!(rec_get_info_bits(rec2) & REC_INFO_MIN_REC_FLAG)) {
            ret = -1;
          }

          goto order_resolved;

        } else if (unlikely(rec_get_info_bits(rec2) & REC_INFO_MIN_REC_FLAG)) {

          ret = 1;

          goto order_resolved;
        }
      }

      if (rec_offs_nth_extern(offsets1, cur_field) || rec_offs_nth_extern(offsets2, cur_field)) {
        /* We do not compare to an externally stored field */

        goto order_resolved;
      }

      if (rec1_f_len == UNIV_SQL_NULL || rec2_f_len == UNIV_SQL_NULL) {

        if (rec1_f_len == rec2_f_len) {

          goto next_field;

        } else if (rec2_f_len == UNIV_SQL_NULL) {

          /* We define the SQL null to be the
          smallest possible value of a field
          in the alphabetical order */

          ret = 1;
        } else {
          ret = -1;
        }

        goto order_resolved;
      }
    }

    if (mtype >= DATA_FLOAT || (mtype == DATA_BLOB && 0 == (prtype & DATA_BINARY_TYPE) &&
                                dtype_get_charset_coll(prtype) != DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL)) {

      ret = cmp_whole_field(index->m_cmp_ctx, mtype, prtype, rec1_b_ptr, (unsigned)rec1_f_len, rec2_b_ptr, (unsigned)rec2_f_len);

      if (ret != 0) {
        cur_bytes = 0;

        goto order_resolved;
      } else {
        goto next_field;
      }
    }

    /* Set the pointers at the current byte */
    rec1_b_ptr = rec1_b_ptr + cur_bytes;
    rec2_b_ptr = rec2_b_ptr + cur_bytes;

    /* Compare then the fields */
    for (;;) {
      if (rec2_f_len <= cur_bytes) {

        if (rec1_f_len <= cur_bytes) {

          goto next_field;
        }

        rec2_byte = dtype_get_pad_char(mtype, prtype);

        if (rec2_byte == ULINT_UNDEFINED) {
          ret = 1;

          goto order_resolved;
        }
      } else {
        rec2_byte = *rec2_b_ptr;
      }

      if (rec1_f_len <= cur_bytes) {
        rec1_byte = dtype_get_pad_char(mtype, prtype);

        if (rec1_byte == ULINT_UNDEFINED) {
          ret = -1;

          goto order_resolved;
        }
      } else {
        rec1_byte = *rec1_b_ptr;
      }

      if (rec1_byte == rec2_byte) {
        /* If the bytes are equal, they will remain
        such even after the collation transformation
        below */

        goto next_byte;
      }

      if (mtype <= DATA_CHAR || (mtype == DATA_BLOB && !(prtype & DATA_BINARY_TYPE))) {

        rec1_byte = cmp_collate(rec1_byte);
        rec2_byte = cmp_collate(rec2_byte);
      }

      if (rec1_byte < rec2_byte) {
        ret = -1;
        goto order_resolved;
      } else if (rec1_byte > rec2_byte) {
        ret = 1;
        goto order_resolved;
      }
    next_byte:
      /* Next byte */

      cur_bytes++;
      rec1_b_ptr++;
      rec2_b_ptr++;
    }

  next_field:
    cur_field++;
    cur_bytes = 0;
  }

  ut_ad(cur_bytes == 0);

  /* If we ran out of fields, rec1 was equal to rec2 up
  to the common fields */
  ut_ad(ret == 0);
order_resolved:

  ut_ad((ret >= -1) && (ret <= 1));

  *matched_fields = cur_field;
  *matched_bytes = cur_bytes;

  return ret;
}

#ifdef UNIV_DEBUG
/**
 * Used in debug checking of cmp_dtuple_... .
 * This function is used to compare a data tuple to a physical record. If
 * dtuple has n fields then rec must have either m >= n fields, or it must
 * differ from dtuple in some of the m fields rec has. If encounters an
 * externally stored field, returns 0.
 * 
 * @param[in] cmp_ctx client compare context
 * @param[in] dtuple data tuple
 * @param[in] rec physical record which differs from dtuple in some of the common fields,
 *                or which has an equal number or more fields than dtuple
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 * @param[in/out] matched_fields number of already completely matched fields; when function
 *                returns, contains the value for current comparison
 * 
 * @return 1, 0, -1, if dtuple is greater, equal, less than rec,
 *  respectively, when only the common first fields are compared */
static int cmp_debug_dtuple_rec_with_match(
  void *cmp_ctx, const DTuple *dtuple, const rec_t *rec, const ulint *offsets, ulint *matched_fields
) noexcept {
  const dfield_t *dtuple_field;
  ulint dtuple_f_len;
  const byte *dtuple_f_data;
  ulint rec_f_len;
  const byte *rec_f_data;
  int ret = 3333;

  ut_ad(dtuple && rec && matched_fields);
  ut_ad(dtuple_check_typed(dtuple));
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  ut_ad(*matched_fields <= dtuple_get_n_fields_cmp(dtuple));
  ut_ad(*matched_fields <= rec_offs_n_fields(offsets));

  auto cur_field = *matched_fields;

  if (cur_field == 0) {
    if (unlikely(rec_get_info_bits(rec) & REC_INFO_MIN_REC_FLAG)) {

      ret = !(dtuple_get_info_bits(dtuple) & REC_INFO_MIN_REC_FLAG);

      goto order_resolved;
    }

    if (unlikely(dtuple_get_info_bits(dtuple) & REC_INFO_MIN_REC_FLAG)) {
      ret = -1;

      goto order_resolved;
    }
  }

  /* Match fields in a loop; stop if we run out of fields in dtuple */

  while (cur_field < dtuple_get_n_fields_cmp(dtuple)) {

    ulint mtype;
    ulint prtype;

    dtuple_field = dtuple_get_nth_field(dtuple, cur_field);
    {
      const dtype_t *type = dfield_get_type(dtuple_field);

      mtype = type->mtype;
      prtype = type->prtype;
    }

    dtuple_f_data = (byte *)dfield_get_data(dtuple_field);
    dtuple_f_len = dfield_get_len(dtuple_field);

    rec_f_data = rec_get_nth_field(rec, offsets, cur_field, &rec_f_len);

    if (rec_offs_nth_extern(offsets, cur_field)) {
      /* We do not compare to an externally stored field */

      ret = 0;

      goto order_resolved;
    }

    ret = cmp_data_data(cmp_ctx, mtype, prtype, dtuple_f_data, dtuple_f_len, rec_f_data, rec_f_len);

    if (ret != 0) {
      goto order_resolved;
    }

    cur_field++;
  }

  ret = 0; /* If we ran out of fields, dtuple was equal to rec
           up to the common fields */
order_resolved:
  ut_ad(ret >= -1 && ret <= 1);

  *matched_fields = cur_field;

  return ret;
}
#endif /* UNIV_DEBUG */
