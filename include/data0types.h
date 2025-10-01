/*****************************************************************************
Copyright (c) 2000, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/data0types.h
 Some type definitions

 Created 9/21/2000 Heikki Tuuri
 *************************************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0mem.h"
#include "ut0lst.h"
#include "ut0ut.h"

struct Index;
struct Rec;

constexpr ulint DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL = 8;

constexpr ulint DATA_CLIENT_BINARY_CHARSET_COLL = 63;

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

/* Row id: a uint64_t */
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

/** This is ORed to the precise type when the column is declared as NOT nullptr */
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

constexpr ulint DATA_TUPLE_MAGIC_N = 65478679;

/* Structure for an SQL data type.
If you add fields to this structure, be sure to initialize them everywhere.
This structure is initialized in the following functions:
dtype_set()
dtype_read_for_order_and_null_size()
dtype_new_read_for_order_and_null_size()
sym_tab_add_null_lit() */

/* The following are used in two places, dtype_t and  Field, we
want to ensure that they are identical and also want to ensure that
all bit-fields can be packed tightly in both structs. */
#define DTYPE_FIELDS                                                  \
  /** Main data type */                                               \
  uint8_t mtype;                                                      \
  /* The remaining fields do not affect alphabetical ordering: */     \
  /** Length of the column in bytes. */                               \
  uint16_t len;                                                       \
  /** Precise type; user data type, charset code, flags to indicate \
   * nullability, signedness, whether this is a binary string */ \
  unsigned prtype : 24;                                               \
  /** Minimum length of a character, in bytes */                      \
  unsigned mbminlen : 2;                                              \
  /** Maximum length of bytes to store the string length) */          \
  unsigned mbmaxlen : 3;

struct dtype_t {
  DTYPE_FIELDS
};

/** Structure for an SQL data field */
struct dfield_t {
  /** Print the dfield_t object into the given output stream.
  @param[in]    out     the output stream.
  @return       the ouput stream. */
  std::ostream &print(std::ostream &out) const;

  /** Pointer to data */
  void *data;

  /** Data length; UNIV_SQL_NULL if SQL null */
  uint32_t len;

  /** true=externally stored, false=local */
  uint8_t ext;

  /** Type of data */
  dtype_t type;
};

/** Structure for an SQL data tuple of fields (logical record) */
struct DTuple {
  /** Print the tuple to the output stream.
  @param[in,out] out            Stream to output to.
  @return stream */
  std::ostream &print(std::ostream &o) const;

  /** Read the trx id from the tuple (DB_TRX_ID)
  @return transaction id of the tuple. */
  trx_id_t get_trx_id() const;

  /** Ignore at most n trailing default fields if this is a tuple
  from instant index
  @param[in]    index   clustered index object for this tuple */
  void ignore_trailing_default(const Index *index);

  /** Compare a data tuple to a physical record.
  @param[in]    rec             record
  @param[in]    index           index
  @param[in]    offsets         Phy_rec::get_col_offsets(rec)
  @param[in,out]        matched_fields  number of completely matched fields
  @return the comparison result of dtuple and rec
  @retval 0 if dtuple is equal to rec
  @retval negative if dtuple is less than rec
  @retval positive if dtuple is greater than rec */
  int compare(const Rec &rec, const Index *index, const ulint *offsets, ulint *matched_fields) const;

  /** Compare a data tuple to a physical record.
  @param[in]    rec             record
  @param[in]    index           index
  @param[in]    offsets         Phy_rec::get_col_offsets(rec)
  @return the comparison result of dtuple and rec
  @retval 0 if dtuple is equal to rec
  @retval negative if dtuple is less than rec
  @retval positive if dtuple is greater than rec */
  inline int compare(const Rec &rec, const Index *index, const ulint *offsets) const {
    ulint matched_fields{};

    return compare(rec, index, offsets, &matched_fields);
  }

  /** Get number of externally stored fields.
  @retval number of externally stored fields. */
  size_t get_n_ext() const;

  /** Does tuple has externally stored fields.
  @retval true if there is externally stored fields. */
  bool has_ext() const;

  /** Info bits of an index record: the default is 0; this field
   * is used if an index record is built from a data tuple */
  ulint info_bits;

  /** Number of fields in dtuple */
  ulint n_fields;

  /** Number of fields which should be used in comparison services
   * of rem0cmp.*; the index search is performed by comparing only
   * these fields, others are ignored; the default value in dtuple
   * creation is the same value as n_fields */
  ulint n_fields_cmp;

  /** Fields */
  dfield_t *fields;

  /** data tuples can be linked into a list using this field */
  UT_LIST_NODE_T(DTuple) tuple_list;

  /** Magic number, used in debug assertions */
  IF_DEBUG(ulint magic_n;)
};

/** A slot for a field in a big rec vector */
struct big_rec_field_t {
  /** Field number in record */
  ulint field_no;

  /** Stored data length, in bytes */
  ulint len;

  /** Stored data */
  const void *data;
};

/** Storage format for overflow data in a big record, that is, a
clustered index record which needs external storage of data fields */
struct big_rec_t {
  /** Memory heap from which allocated */
  mem_heap_t *heap;

  /** Number of stored fields */
  ulint n_fields;

  /** Stored fields */
  big_rec_field_t *fields;
};

