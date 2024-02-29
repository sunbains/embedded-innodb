/****************************************************************************

Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.

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

extern ulint data_client_default_charset_coll;

constexpr ulint DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL = 8;
constexpr ulint DATA_CLIENT_BINARY_CHARSET_COLL = 63;

/* SQL data type struct */
typedef struct dtype_struct dtype_t;

/* The 'MAIN TYPE' of a column */

/** Character varying of the latin1_swedish_ci charset-collation */
constexpr ulint DATA_VARCHAR = 1;

/* Fixed length character of the latin1_swedish_ci charset-collation */
constexpr ulint DATA_CHAR = 2;

/** Binary string of fixed length */
constexpr ulint DATA_FIXBINARY = 3;

/* Binary string */
constexpr ulint DATA_BINARY = 4;

/* binary large object, or a TEXT type. */
constexpr ulint DATA_BLOB = 5;

/** Integer: can be any size 1 - 8 bytes */
constexpr ulint DATA_INT = 6;

/** Address of the child page in node pointer */
constexpr ulint DATA_SYS_CHILD = 7;

/** System column */
constexpr ulint DATA_SYS = 8;

/** Data types >= DATA_FLOAT must be compared using the whole
field, not as binary strings */
constexpr ulint DATA_FLOAT = 9;
constexpr ulint DATA_DOUBLE = 10;

/** Decimal number stored as an ASCII string */
constexpr ulint DATA_DECIMAL = 11;

constexpr ulint DATA_VARCLIENT = 12;

/** Any charset varying length char */
constexpr ulint DATA_CLIENT = 13;

/** dtype_store_for_order_and_null_size() requires the values are <= 63 */
constexpr ulint DATA_MTYPE_MAX = 63;

/** The 'PRECISE TYPE' of a column

User tables have the following convention:

- In the least significant byte in the precise type we store the user type
code (not applicable for system columns).

- In the second least significant byte we OR flags DATA_NOT_NULL,
DATA_UNSIGNED, DATA_BINARY_TYPE.

- In the third least significant byte of the precise type of string types we
store the user charset-collation code.

If the stored charset code is 0 in the system table SYS_COLUMNS
of InnoDB, that means that the default charset of this installation
should be used.

When loading a table definition from the system tables to the InnoDB data
dictionary cache in main memory, if the stored charset-collation is 0, and
the type is a non-binary string, replace that 0 by the default
charset-collation code of the installation. In short, in old tables, the
charset-collation code in the system tables on disk can be 0, but in
in-memory data structures (dtype_t), the charset-collation code is
always != 0 for non-binary string types.

In new tables, in binary string types, the charset-collation code is the
user code for the 'binary charset', that is, != 0.

For binary string types and for DATA_CHAR, DATA_VARCHAR, and for those
DATA_BLOB which are binary or have the charset-collation latin1_swedish_ci,
InnoDB performs all comparisons internally, without resorting to the user
comparison functions. This is to save CPU time.

InnoDB's own internal system tables have different precise types for their
columns, and for them the precise type is usually not used at all.
*/

/** English language character string: only used for InnoDB's own system tables
 */
const ulint DATA_ENGLISH = 4;

/** Used for error checking and debugging */
const ulint DATA_ERROR = 111;

/* AND with this mask to extract the user type from the precise type */
const ulint DATA_CLIENT_TYPE_MASK = 255;

/* Precise data types for system columns and the length of those columns;
NOTE: the values must run from 0 up in the order given! All codes must
be less than 256 */

/* Row id: a dulint */
const ulint DATA_ROW_ID = 0;

/** Stored length for row id */
const ulint DATA_ROW_ID_LEN = 6;

/** Transaction id: 6 bytes */
const ulint DATA_TRX_ID = 1;
const ulint DATA_TRX_ID_LEN = 6;

/** Rollback data pointer: 7 bytes */
const ulint DATA_ROLL_PTR = 2;
const ulint DATA_ROLL_PTR_LEN = 7;

/** Number of system columns defined above */
const ulint DATA_N_SYS_COLS = 3;

/** Mask to extract the above from prtype */
const ulint DATA_SYS_PRTYPE_MASK = 0xF;

/** Flags ORed to the precise data type */

/** This is ORed to the precise type when the column is declared as NOT NULL */
const ulint DATA_NOT_NULL = 256;

/** This id ORed to the precise type when we have an unsigned integer
type. */
const ulint DATA_UNSIGNED = 512;

/** If the data type is a binary character string, this is ORed to the
precise type. */
const ulint DATA_BINARY_TYPE = 1024;

/** first custom type starts here */
const ulint DATA_CUSTOM_TYPE = 2048;

/* This many bytes we need to store the type information affecting the
alphabetical order for a single field and decide the storage size of an
SQL null*/
const ulint DATA_ORDER_NULL_TYPE_BUF_SIZE = 4;

/** Store the charset-collation number; one byte is left unused */
const ulint DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE = 6;

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
    const char *str); /*!< in: the string whose prefix
                      length is being determined */

/** Checks if a data main type is a string type. Also a BLOB is considered a
string type.
@return	true if string type */
bool dtype_is_string_type(
    ulint mtype); /*!< in: InnoDB main data type code: DATA_CHAR, ... */

/** Checks if a type is a binary string type. Note that for tables created with
< 4.0.14, we do not know if a DATA_BLOB column is a BLOB or a TEXT column. For
those DATA_BLOB columns this function currently returns false.
@return	true if binary string type */
bool dtype_is_binary_string_type(ulint mtype,   /*!< in: main data type */
                                 ulint prtype); /*!< in: precise type */

/** Checks if a type is a non-binary string type. That is, dtype_is_string_type
is true and dtype_is_binary_string_type is false. Note that for tables created
with < 4.0.14, we do not know if a DATA_BLOB column is a BLOB or a TEXT column.
For those DATA_BLOB columns this function currently returns true.
@return	true if non-binary string type */
bool dtype_is_non_binary_string_type(ulint mtype,   /*!< in: main data type */
                                     ulint prtype); /*!< in: precise type */

/** Sets a data type structure. */
inline void dtype_set(dtype_t *type, /*!< in: type struct to init */
                      ulint mtype,   /*!< in: main data type */
                      ulint prtype,  /*!< in: precise type */
                      ulint len);    /*!< in: precision of type */

/** Copies a data type structure. */
inline void
dtype_copy(dtype_t *type1,        /*!< in: type struct to copy to */
           const dtype_t *type2); /*!< in: type struct to copy from */

/** Gets the SQL main data type.
@return	SQL main data type */
inline ulint dtype_get_mtype(const dtype_t *type); /*!< in: data type */

/** Gets the precise data type.
@return	precise data type */
inline ulint dtype_get_prtype(const dtype_t *type); /*!< in: data type */

/** Compute the mbminlen and mbmaxlen members of a data type structure. */
inline void
dtype_get_mblen(ulint mtype,      /*!< in: main type */
                ulint prtype,     /*!< in: precise type (and collation) */
                ulint *mbminlen,  /*!< out: minimum length of a
                                  multi-byte character */
                ulint *mbmaxlen); /*!< out: maximum length of a
                                  multi-byte character */

/** Gets the user charset-collation code for user string types. */
inline ulint dtype_get_charset_coll(ulint prtype); /*!< in: precise data type */

/** Forms a precise type from the < 4.1.2 format precise type plus the
charset-collation code.
@return precise type, including the charset-collation code */
ulint dtype_form_prtype(
    ulint old_prtype,    /*!< in: the user type code and the flags
                         DATA_BINARY_TYPE etc. */
    ulint charset_coll); /*!< in: user charset-collation code */

/** Gets the type length.
@return	fixed length of the type, in bytes, or 0 if variable-length */
inline ulint dtype_get_len(const dtype_t *type); /*!< in: data type */

/** Gets the minimum length of a character, in bytes.
@return minimum length of a char, in bytes, or 0 if this is not a
character type */
inline ulint dtype_get_mbminlen(const dtype_t *type); /*!< in: type */

/** Gets the maximum length of a character, in bytes.
@return maximum length of a char, in bytes, or 0 if this is not a
character type */
inline ulint dtype_get_mbmaxlen(const dtype_t *type); /*!< in: type */

/** Gets the padding character code for the type.
@return	padding character code, or ULINT_UNDEFINED if no padding specified */
inline ulint dtype_get_pad_char(ulint mtype,   /*!< in: main type */
                                ulint prtype); /*!< in: precise type */

/** Returns the size of a fixed size data type, 0 if not a fixed size type.
@return	fixed size, or 0 */
inline ulint dtype_get_fixed_size_low(
    ulint mtype,    /*!< in: main type */
    ulint prtype,   /*!< in: precise type */
    ulint len,      /*!< in: length */
    ulint mbminlen, /*!< in: minimum length of a multibyte char */
    ulint mbmaxlen, /*!< in: maximum length of a multibyte char */
    ulint comp);    /*!< in: nonzero=ROW_FORMAT=COMPACT  */

/** Returns the minimum size of a data type.
@return	minimum size */
inline ulint dtype_get_min_size_low(
    ulint mtype,     /*!< in: main type */
    ulint prtype,    /*!< in: precise type */
    ulint len,       /*!< in: length */
    ulint mbminlen,  /*!< in: minimum length of a multibyte char */
    ulint mbmaxlen); /*!< in: maximum length of a multibyte char */

/** Returns the maximum size of a data type. Note: types in system tables may be
incomplete and return incorrect information.
@return	maximum size */
inline ulint dtype_get_max_size_low(ulint mtype, /*!< in: main type */
                                    ulint len);  /*!< in: length */

/** Returns the ROW_FORMAT=REDUNDANT stored SQL NULL size of a type.
For fixed length types it is the fixed length of the type, otherwise 0.
@return	SQL null storage size in ROW_FORMAT=REDUNDANT */
inline ulint
dtype_get_sql_null_size(const dtype_t *type, /*!< in: type */
                        ulint comp); /*!< in: nonzero=ROW_FORMAT=COMPACT  */

/** Reads to a type the stored information which determines its alphabetical
ordering and the storage size of an SQL NULL value. */
inline void dtype_read_for_order_and_null_size(
    dtype_t *type,    /*!< in: type struct */
    const byte *buf); /*!< in: buffer for the stored order info */

/** Stores for a type the information which determines its alphabetical ordering
and the storage size of an SQL NULL value. */
inline void dtype_new_store_for_order_and_null_size(
    byte *buf,           /*!< in: buffer for
                         DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE
                         bytes where we store the info */
    const dtype_t *type, /*!< in: type struct */
    ulint prefix_len);   /*!< in: prefix length to
                     replace type->len, or 0 */

/** Reads to a type the stored information which determines its alphabetical
ordering and the storage size of an SQL NULL value. This is the 4.1.x storage
format. */
inline void dtype_new_read_for_order_and_null_size(
    dtype_t *type,    /*!< in: type struct */
    const byte *buf); /*!< in: buffer for stored type order info */

/** Validates a data type structure.
@return	true if ok */

bool dtype_validate(const dtype_t *type); /*!< in: type struct to validate */

/** Prints a data type structure. */

void dtype_print(const dtype_t *type); /*!< in: type */

/** Gets the user type code from a dtype.
@return	type code; this is NOT an InnoDB type code! */
inline ulint dtype_get_attrib(const dtype_t *type); /*!< in: type struct */

/** Reset dtype variables. */

void dtype_var_init(void);

/* Structure for an SQL data type.
If you add fields to this structure, be sure to initialize them everywhere.
This structure is initialized in the following functions:
dtype_set()
dtype_read_for_order_and_null_size()
dtype_new_read_for_order_and_null_size()
sym_tab_add_null_lit() */

/* The following are used in two places, dtype_t and  dict_field_t, we
want to ensure that they are identical and also want to ensure that
all bit-fields can be packed tightly in both structs. */
#define DTYPE_FIELDS                                                           \
  unsigned mtype : 8;   /*!< main data type */                                 \
  unsigned prtype : 24; /*!< precise type; user data                           \
                        type, charset code, flags to                           \
                        indicate nullability,                                  \
                        signedness, whether this is a                          \
                        binary string */                                       \
  /* the remaining fields do not affect alphabetical ordering: */              \
  unsigned len : 16;     /*!< length */                                        \
  unsigned mbminlen : 2; /*!< minimum length of a                              \
                         character, in bytes */                                \
  unsigned mbmaxlen : 3; /*!< maximum length of bytes                          \
                         to store the string length) */
struct dtype_struct {
  DTYPE_FIELDS
};

#ifndef UNIV_NONINL
#include "data0type.ic"
#endif
