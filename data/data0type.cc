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

/** @file data/data0type.c
Data types

Created 1/16/1996 Heikki Tuuri
*******************************************************/

#include "data0type.h"

#ifdef UNIV_NONINL
#include "data0type.ic"
#endif

/* At the database startup we store the default-charset collation number of
this installation to this global variable. If we have < 4.1.2 format
column definitions, or records in the insert buffer, we use this
charset-collation code for them. */

ulint data_client_default_charset_coll;

/** Reset dtype variables. */

void dtype_var_init(void) {
  data_client_default_charset_coll = 0;
}

/** Determine how many bytes the first n characters of the given string occupy.
If the string is shorter than n characters, returns the number of bytes
the characters in the string occupy.
@return	length of the prefix, in bytes */

ulint dtype_get_at_most_n_mbchars(
  ulint prtype,     /*!< in: precise type */
  ulint mbminlen,   /*!< in: minimum length of a
                      multi-byte character */
  ulint mbmaxlen,   /*!< in: maximum length of a
                      multi-byte character */
  ulint prefix_len, /*!< in: length of the requested
                      prefix, in characters, multiplied by
                      dtype_get_mbmaxlen(dtype) */
  ulint data_len,   /*!< in: length of str (in bytes) */
  const char *str
) /*!< in: the string whose prefix
                      length is being determined */
{
  ut_a(data_len != UNIV_SQL_NULL);
  ut_ad(!mbmaxlen || !(prefix_len % mbmaxlen));

  if (mbminlen != mbmaxlen) {
    const charset_t *cs;

    ut_a(!(prefix_len % mbmaxlen));

    cs = ib_ucode_get_charset(dtype_get_charset_coll(prtype));

    return (ib_ucode_get_storage_size(cs, prefix_len, data_len, str));
  }

  if (prefix_len < data_len) {

    return (prefix_len);
  }

  return (data_len);
}

/** Checks if a data main type is a string type. Also a BLOB is considered a
string type.
@return	true if string type */

bool dtype_is_string_type(ulint mtype) /*!< in: InnoDB main data type code: DATA_CHAR, ... */
{
  if (mtype <= DATA_BLOB || mtype == DATA_CLIENT || mtype == DATA_VARCLIENT) {

    return (true);
  }

  return (false);
}

/** Checks if a type is a binary string type. Note that for tables created with
< 4.0.14, we do not know if a DATA_BLOB column is a BLOB or a TEXT column. For
those DATA_BLOB columns this function currently returns false.
@return	true if binary string type */

bool dtype_is_binary_string_type(
  ulint mtype, /*!< in: main data type */
  ulint prtype
) /*!< in: precise type */
{
  if ((mtype == DATA_FIXBINARY) || (mtype == DATA_BINARY) || (mtype == DATA_BLOB && (prtype & DATA_BINARY_TYPE))) {

    return (true);
  }

  return (false);
}

/** Checks if a type is a non-binary string type. That is, dtype_is_string_type
is true and dtype_is_binary_string_type is false. Note that for tables created
with < 4.0.14, we do not know if a DATA_BLOB column is a BLOB or a TEXT column.
For those DATA_BLOB columns this function currently returns true.
@return	true if non-binary string type */

bool dtype_is_non_binary_string_type(
  ulint mtype, /*!< in: main data type */
  ulint prtype
) /*!< in: precise type */
{
  if (dtype_is_string_type(mtype) == true && dtype_is_binary_string_type(mtype, prtype) == false) {

    return (true);
  }

  return (false);
}

/** Forms a precise type from the < 4.1.2 format precise type plus the
charset-collation code.
@return precise type, including the charset-collation code */

ulint dtype_form_prtype(
  ulint old_prtype, /*!< in: the user type code and the flags
                        DATA_BINARY_TYPE etc. */
  ulint charset_coll
) /*!< in: user charset-collation code */
{
  ut_a(old_prtype < 256 * 256);
  ut_a(charset_coll < 256);

  return (old_prtype + (charset_coll << 16));
}

/** Validates a data type structure.
@return	true if ok */

bool dtype_validate(const dtype_t *type) /*!< in: type struct to validate */
{
  ut_a(type);
  ut_a(type->mtype >= DATA_VARCHAR);
  ut_a(type->mtype <= DATA_CLIENT);

  if (type->mtype == DATA_SYS) {
    ut_a((type->prtype & DATA_CLIENT_TYPE_MASK) < DATA_N_SYS_COLS);
  }

  ut_a(type->mbminlen <= type->mbmaxlen);

  return (true);
}

/** Prints a data type structure. */

void dtype_print(const dtype_t *type) /*!< in: type */
{
  ulint mtype;
  ulint prtype;
  ulint len;

  ut_a(type);

  mtype = type->mtype;
  prtype = type->prtype;

  switch (mtype) {
    case DATA_VARCHAR:
      ib_logger(ib_stream, "DATA_VARCHAR");
      break;

    case DATA_CHAR:
      ib_logger(ib_stream, "DATA_CHAR");
      break;

    case DATA_BINARY:
      ib_logger(ib_stream, "DATA_BINARY");
      break;

    case DATA_FIXBINARY:
      ib_logger(ib_stream, "DATA_FIXBINARY");
      break;

    case DATA_BLOB:
      ib_logger(ib_stream, "DATA_BLOB");
      break;

    case DATA_INT:
      ib_logger(ib_stream, "DATA_INT");
      break;

    case DATA_CLIENT:
      ib_logger(ib_stream, "DATA_CLIENT");
      break;

    case DATA_SYS:
      ib_logger(ib_stream, "DATA_SYS");
      break;

    case DATA_FLOAT:
      ib_logger(ib_stream, "DATA_FLOAT");
      break;

    case DATA_DOUBLE:
      ib_logger(ib_stream, "DATA_DOUBLE");
      break;

    case DATA_DECIMAL:
      ib_logger(ib_stream, "DATA_DECIMAL");
      break;

    default:
      ib_logger(ib_stream, "type %lu", (ulong)mtype);
      break;
  }

  len = type->len;

  if ((type->mtype == DATA_SYS) || (type->mtype == DATA_VARCHAR) || (type->mtype == DATA_CHAR)) {
    ib_logger(ib_stream, " ");
    if (prtype == DATA_ROW_ID) {
      ib_logger(ib_stream, "DATA_ROW_ID");
      len = DATA_ROW_ID_LEN;
    } else if (prtype == DATA_ROLL_PTR) {
      ib_logger(ib_stream, "DATA_ROLL_PTR");
      len = DATA_ROLL_PTR_LEN;
    } else if (prtype == DATA_TRX_ID) {
      ib_logger(ib_stream, "DATA_TRX_ID");
      len = DATA_TRX_ID_LEN;
    } else if (prtype == DATA_ENGLISH) {
      ib_logger(ib_stream, "DATA_ENGLISH");
    } else {
      ib_logger(ib_stream, "prtype %lu", (ulong)prtype);
    }
  } else {
    if (prtype & DATA_UNSIGNED) {
      ib_logger(ib_stream, " DATA_UNSIGNED");
    }

    if (prtype & DATA_BINARY_TYPE) {
      ib_logger(ib_stream, " DATA_BINARY_TYPE");
    }

    if (prtype & DATA_NOT_NULL) {
      ib_logger(ib_stream, " DATA_NOT_NULL");
    }
  }

  ib_logger(ib_stream, " len %lu", (ulong)len);
}
