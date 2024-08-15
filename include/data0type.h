/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/data0type.h
Data types

Created 1/16/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "api0ucode.h"
#include "mach0data.h"
#include "data0types.h"

struct dtype_t;
extern ulint data_client_default_charset_coll;

/**
 * Determine how many bytes the first n characters of the given string occupy.
 * If the string is shorter than n characters, returns the number of bytes
 * the characters in the string occupy.
 *
 * @param prtype     precise type
 * @param mbminlen   minimum length of a multi-byte character
 * @param mbmaxlen   maximum length of a multi-byte character
 * @param prefix_len length of the requested prefix, in characters, multiplied by dtype_get_mbmaxlen(dtype)
 * @param data_len   length of str (in bytes)
 * @param str        the string whose prefix length is being determined
 *
 * @return length of the prefix, in bytes
 */
ulint dtype_get_at_most_n_mbchars(
  ulint prtype,
  ulint mbminlen,
  ulint mbmaxlen,
  ulint prefix_len,
  ulint data_len,
  const char *str
);

/**
 * Checks if a data main type is a string type. Also a BLOB is considered a string type.
 *
 * @param mtype InnoDB main data type code: DATA_CHAR, ...
 * @return true if string type
 */
bool dtype_is_string_type(ulint mtype);

/**
 * Checks if a type is a binary string type. Note that for tables created with < 4.0.14,
 * we do not know if a DATA_BLOB column is a BLOB or a TEXT column. For those DATA_BLOB
 * columns this function currently returns false.
 *
 * @param mtype InnoDB main data type
 * @param prtype precise type
 * @return true if binary string type
 */
bool dtype_is_binary_string_type(ulint mtype, ulint prtype);

/**
 * Checks if a type is a non-binary string type. That is, dtype_is_string_type
 * is true and dtype_is_binary_string_type is false. Note that for tables created
 * with < 4.0.14, we do not know if a DATA_BLOB column is a BLOB or a TEXT column.
 * For those DATA_BLOB columns this function currently returns true.
 *
 * @param mtype  InnoDB main data type
 * @param prtype precise type
 * @return true if non-binary string type
 */
bool dtype_is_non_binary_string_type(ulint mtype, ulint prtype);

/**
 * Forms a precise type from the < 4.1.2 format precise type plus the charset-collation code.
 *
 * @param old_prtype     the user type code and the flags DATA_BINARY_TYPE etc.
 * @param charset_coll   user charset-collation code
 * @return               precise type, including the charset-collation code
 */
ulint dtype_form_prtype(ulint old_prtype, ulint charset_coll);

/**
 * Validates a data type structure.
 *
 * @param type   type struct to validate
 * @return       true if ok
 */
bool dtype_validate(const dtype_t *type);

/**
 * Prints a data type structure.
 *
 * @param type   type
 */
void dtype_print(const dtype_t *type);

/**
 * Gets the user type code from a dtype.
 *
 * @param type   type struct
 * @return       type code; this is NOT an InnoDB type code!
 */
inline ulint dtype_get_attrib(const dtype_t *type);

/**
 * Reset dtype variables.
 */
void dtype_var_init();


/**
 * Gets the client charset-collation code for user string types.
 *
 * @param prtype precise data type
 * @return client charset-collation code
 */
inline ulint dtype_get_charset_coll(ulint prtype) {
  return (prtype >> 16) & 0xFFUL;
}

/**
 * Gets the user type code from a dtype.
 *
 * @return       user type code; this is NOT an InnoDB type code!
 */
inline ulint dtype_get_attrib(const dtype_t *type) {
  return type->prtype & 0xFFUL;
}

/**
 * Compute the mbminlen and mbmaxlen members of a data type structure.
 *
 * @param mtype     main type
 * @param prtype    precise type (and collation)
 * @param mbminlen  out: minimum length of a multi-byte character
 * @param mbmaxlen  out: maximum length of a multi-byte character
 */
inline void dtype_get_mblen(ulint mtype, ulint prtype, ulint *mbminlen, ulint *mbmaxlen) {
  if (dtype_is_string_type(mtype)) {
    const auto cs = ib_ucode_get_charset(dtype_get_charset_coll(prtype));

    ib_ucode_get_charset_width(cs, mbminlen, mbmaxlen);

    ut_ad(*mbminlen <= *mbmaxlen);
    ut_ad(*mbminlen <= 2);     /* mbminlen in dtype_t is 0..3 */
    ut_ad(*mbmaxlen < 1 << 3); /* mbmaxlen in dtype_t is 0..7 */
  } else {
    *mbminlen = *mbmaxlen = 0;
  }
}

/**
 * Compute the mbminlen and mbmaxlen members of a data type structure.
 *
 * @param type   type struct to compute mbminlen and mbmaxlen for
 */
inline void dtype_set_mblen(dtype_t *type) {
  ulint mbminlen;
  ulint mbmaxlen;

  dtype_get_mblen(type->mtype, type->prtype, &mbminlen, &mbmaxlen);
  type->mbminlen = mbminlen;
  type->mbmaxlen = mbmaxlen;

  ut_ad(dtype_validate(type));
}

/**
 * Sets a data type structure.
 *
 * @param type    type struct to init
 * @param mtype   main data type
 * @param prtype  precise type
 * @param len     precision of type
 */
inline void dtype_set(dtype_t *type, ulint mtype, ulint prtype, ulint len) {
  ut_ad(mtype <= DATA_MTYPE_MAX);

  type->mtype = mtype;
  type->prtype = prtype;
  type->len = len;

  dtype_set_mblen(type);
}

/**
 * Copies a data type structure.
 *
 * @param type1   type struct to copy to
 * @param type2   type struct to copy from
 */
inline void dtype_copy(dtype_t *type1, const dtype_t *type2) {
  *type1 = *type2;

  ut_ad(dtype_validate(type1));
}

/** Gets the SQL main data type.
@return	SQL main data type */
inline ulint dtype_get_mtype(const dtype_t *type) {
  return type->mtype;
}

/** Gets the precise data type.
@return	precise data type */
inline ulint dtype_get_prtype(const dtype_t *type) {
  return type->prtype;
}

/** Gets the type length.
@return	fixed length of the type, in bytes, or 0 if variable-length */
inline ulint dtype_get_len(const dtype_t *type) {
  return type->len;
}

/** Gets the minimum length of a character, in bytes.
@return minimum length of a char, in bytes, or 0 if this is not a
character type */
inline ulint dtype_get_mbminlen(const dtype_t *type) {
  return type->mbminlen;
}

/** Gets the maximum length of a character, in bytes.
@return maximum length of a char, in bytes, or 0 if this is not a
character type */
inline ulint dtype_get_mbmaxlen(const dtype_t *type) {
  return type->mbmaxlen;
}

/**
 * Gets the padding character code for a type.
 *
 * @param mtype   main type
 * @param prtype  precise type
 * @return        padding character code, or ULINT_UNDEFINED if no padding specified
 */
inline ulint dtype_get_pad_char(ulint mtype, ulint prtype) {
  switch (mtype) {
    case DATA_FIXBINARY:
    case DATA_BINARY:
      if (unlikely(dtype_get_charset_coll(prtype) == DATA_CLIENT_BINARY_CHARSET_COLL)) {
        /* Starting from 5.0.18, do not pad
      VARBINARY or BINARY columns. */
        return (ULINT_UNDEFINED);
      }
      /* Fall through */
    case DATA_CHAR:
    case DATA_VARCHAR:
    case DATA_CLIENT:
    case DATA_VARCLIENT:
      /* Space is the padding character for all char and binary
    strings, and starting from 5.0.3, also for TEXT strings. */

      return (0x20);
    case DATA_BLOB:
      if (!(prtype & DATA_BINARY_TYPE)) {
        return (0x20);
      }
      /* Fall through */
    default:
      /* No padding specified */
      return (ULINT_UNDEFINED);
  }
}

/**
 * Stores for a type the information which determines its alphabetical ordering
 * and the storage size of an SQL nullptr value. This is the >= 4.1.x storage
 * format.
 *
 * @param buf         buffer for DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE bytes where we store the info
 * @param type        type struct
 * @param prefix_len  prefix length to replace type->len, or 0
 */
inline void dtype_new_store_for_order_and_null_size(byte *buf, const dtype_t *type, ulint prefix_len) {
  static_assert(6 == DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE, "error 6 != DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE");

  ut_ad(type->mtype >= DATA_VARCHAR);
  ut_ad(type->mtype <= DATA_CLIENT);

  buf[0] = (byte)(type->mtype & 0xFFUL);

  if (type->prtype & DATA_BINARY_TYPE) {
    buf[0] = buf[0] | 128;
  }

  buf[1] = (byte)(type->prtype & 0xFFUL);

  auto len = prefix_len ? prefix_len : type->len;

  mach_write_to_2(buf + 2, len & 0xFFFFUL);

  ut_ad(dtype_get_charset_coll(type->prtype) < 256);

  mach_write_to_2(buf + 4, dtype_get_charset_coll(type->prtype));

  if (type->prtype & DATA_NOT_NULL) {
    buf[4] |= 128;
  }
}

/**
 * Reads to a type the stored information which determines its alphabetical
 * ordering and the storage size of an SQL nullptr value. This is the < 4.1.x
 * storage format.
 *
 * @param type   type struct
 * @param buf    buffer for stored type order info
 */
inline void dtype_read_for_order_and_null_size(dtype_t *type, const byte *buf) {
  static_assert(4 == DATA_ORDER_NULL_TYPE_BUF_SIZE, "error 4 != DATA_ORDER_NULL_TYPE_BUF_SIZE");

  type->mtype = buf[0] & 63;
  type->prtype = buf[1];

  if (buf[0] & 128) {
    type->prtype = type->prtype | DATA_BINARY_TYPE;
  }

  type->len = mach_read_from_2(buf + 2);

  type->prtype = dtype_form_prtype(type->prtype, data_client_default_charset_coll);
  dtype_set_mblen(type);
}

/** Reads to a type the stored information which determines its alphabetical
 * ordering and the storage size of an SQL nullptr value. This is the >= 4.1.x
 * storage format.
 * @param type   type struct
 * @param buf    buffer for stored type order info */
inline void dtype_new_read_for_order_and_null_size(dtype_t *type, const byte *buf) {
  static_assert(6 == DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE, "error 6 != DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE");

  type->mtype = buf[0] & 63;
  type->prtype = buf[1];

  if (buf[0] & 128) {
    type->prtype |= DATA_BINARY_TYPE;
  }

  if (buf[4] & 128) {
    type->prtype |= DATA_NOT_NULL;
  }

  type->len = mach_read_from_2(buf + 2);

#if MULTIBYTE_CHARSET
  // FIXME: 
  ulint charset_coll = mach_read_from_2(buf + 4) & 0x7fff;
#endif /* MULTIBYTE_CHARSET */

  if (dtype_is_string_type(type->mtype)) {
#if MULTIBYTE_CHARSET
/* FIXME: This is probably MySQL specific too. */
    ut_a(charset_coll > 0);
    ut_a(charset_coll < 256);
    type->prtype = dtype_form_prtype(type->prtype, charset_coll);
#else
    type->prtype = 1; /* FIXME: Hack for testing. */
#endif /* MULTIBYTE_CHARSET */
  }
  dtype_set_mblen(type);
}

/**
 * Returns the size of a fixed size data type, 0 if not a fixed size type.
 *
 * @param mtype     main type
 * @param prtype    precise type
 * @param len       length
 * @param mbminlen  minimum length of a multibyte char
 * @param mbmaxlen  maximum length of a multibyte char
 * @return          fixed size, or 0
 */
inline ulint dtype_get_fixed_size_low(ulint mtype, ulint prtype, ulint len, ulint mbminlen, ulint mbmaxlen) {
  switch (mtype) {
    case DATA_SYS:
      IF_DEBUG(
      switch (prtype & DATA_CLIENT_TYPE_MASK) {
        case DATA_ROW_ID:
          ut_ad(len == DATA_ROW_ID_LEN);
          break;
        case DATA_TRX_ID:
          ut_ad(len == DATA_TRX_ID_LEN);
          break;
        case DATA_ROLL_PTR:
          ut_ad(len == DATA_ROLL_PTR_LEN);
          break;
        default:
          ut_ad(0);
          return 0;
      })

    case DATA_CHAR:
    case DATA_FIXBINARY:
    case DATA_INT:
    case DATA_FLOAT:
    case DATA_DOUBLE:
      return len;
    case DATA_CLIENT:
      if ((prtype & DATA_BINARY_TYPE) || mbminlen == mbmaxlen) {
        return len;
      }
      /* fall through for variable-length charsets */
    case DATA_VARCHAR:
    case DATA_BINARY:
    case DATA_DECIMAL:
    case DATA_VARCLIENT:
    case DATA_BLOB:
      return 0;
    default:
      ut_error;
  }

  return 0;
}

/**
 * Returns the minimum size of a data type.
 *
 * @param mtype     main type
 * @param prtype    precise type
 * @param len       length
 * @param mbminlen  minimum length of a multibyte char
 * @param mbmaxlen  maximum length of a multibyte char
 * @return          minimum size
 */
inline ulint dtype_get_min_size_low(
  ulint mtype,
  ulint prtype,
  ulint len,
  ulint mbminlen,
  ulint mbmaxlen) {
  switch (mtype) {
    case DATA_SYS:
      IF_DEBUG(
      switch (prtype & DATA_CLIENT_TYPE_MASK) {
        case DATA_ROW_ID:
          ut_ad(len == DATA_ROW_ID_LEN);
          break;
        case DATA_TRX_ID:
          ut_ad(len == DATA_TRX_ID_LEN);
          break;
        case DATA_ROLL_PTR:
          ut_ad(len == DATA_ROLL_PTR_LEN);
          break;
        default:
          ut_ad(0);
          return 0;
      })
    case DATA_CHAR:
    case DATA_FIXBINARY:
    case DATA_INT:
    case DATA_FLOAT:
    case DATA_DOUBLE:
      return (len);
    case DATA_CLIENT:
      if ((prtype & DATA_BINARY_TYPE) || mbminlen == mbmaxlen) {
        return (len);
      }
      /* this is a variable-length character set */
      ut_a(mbminlen > 0);
      ut_a(mbmaxlen > mbminlen);
      ut_a(len % mbmaxlen == 0);
      return len * mbminlen / mbmaxlen;
    case DATA_VARCHAR:
    case DATA_BINARY:
    case DATA_DECIMAL:
    case DATA_VARCLIENT:
    case DATA_BLOB:
      return (0);
    default:
      ut_error;
  }

  return 0;
}

/**
 * Returns the maximum size of a data type. Note: types in system tables may be
 * incomplete and return incorrect information.
 *
 * @param mtype     main type
 * @param len       length
 * @return          maximum size
 */
inline ulint dtype_get_max_size_low(ulint mtype, ulint len) {
  switch (mtype) {
    case DATA_SYS:
    case DATA_CHAR:
    case DATA_FIXBINARY:
    case DATA_INT:
    case DATA_FLOAT:
    case DATA_DOUBLE:
    case DATA_CLIENT:
    case DATA_VARCHAR:
    case DATA_BINARY:
    case DATA_DECIMAL:
    case DATA_VARCLIENT:
      return (len);
    case DATA_BLOB:
      break;
    default:
      ut_error;
  }

  return ULINT_MAX;
}

/**
 * Returns the ROW_FORMAT=REDUNDANT stored SQL nullptr size of a type.
 * For fixed length types it is the fixed length of the type, otherwise 0.
 *
 * @param type   in: type
 * @return       SQL null storage size in ROW_FORMAT=REDUNDANT
 */
inline ulint dtype_get_sql_null_size(const dtype_t *type) {
  return dtype_get_fixed_size_low(type->mtype, type->prtype, type->len, type->mbminlen, type->mbmaxlen);
}
