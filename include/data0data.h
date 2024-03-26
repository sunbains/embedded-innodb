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

/*** @file include/data0data.h
SQL data field and tuple

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#pragma once

#include "innodb0types.h"

#include "data0type.h"
#include "data0types.h"
#include "dict0types.h"
#include "mem0mem.h"

struct dict_index_t;

/** Storage for overflow data in a big record, that is, a clustered
index record which needs external storage of data fields */
typedef struct big_rec_struct big_rec_t;

#ifdef UNIV_DEBUG
/*** Gets pointer to the type struct of SQL data field.
@return	pointer to the type struct */
inline dtype_t *dfield_get_type(const dfield_t *field); /*!< in: SQL data field */
/*** Gets pointer to the data in a field.
@return	pointer to data */
inline void *dfield_get_data(const dfield_t *field); /*!< in: field */
#else                                                /* UNIV_DEBUG */
#define dfield_get_type(field) (&(field)->type)
#define dfield_get_data(field) (reinterpret_cast<byte *>((field)->data))
#endif /* UNIV_DEBUG */
/*** Sets the type struct of SQL data field. */
inline void dfield_set_type(
  dfield_t *field, /*!< in: SQL data field */
  dtype_t *type
); /*!< in: pointer to data type struct */
/*** Gets length of field data.
@return	length of data; UNIV_SQL_NULL if SQL null data */
inline ulint dfield_get_len(const dfield_t *field); /*!< in: field */
/*** Sets length in a field. */
inline void dfield_set_len(
  dfield_t *field, /*!< in: field */
  ulint len
); /*!< in: length or UNIV_SQL_NULL */
/*** Determines if a field is SQL nullptr
@return	nonzero if SQL null data */
inline ulint dfield_is_null(const dfield_t *field); /*!< in: field */
/*** Determines if a field is externally stored
@return	nonzero if externally stored */
inline ulint dfield_is_ext(const dfield_t *field); /*!< in: field */
/*** Sets the "external storage" flag */
inline void dfield_set_ext(dfield_t *field); /*!< in/out: field */
/*** Sets pointer to the data and length in a field. */
inline void dfield_set_data(
  dfield_t *field,  /*!< in: field */
  const void *data, /*!< in: data */
  ulint len
); /*!< in: length or UNIV_SQL_NULL */
/*** Sets a data field to SQL nullptr. */
inline void dfield_set_null(dfield_t *field); /*!< in/out: field */
/*** Writes an SQL null field full of zeros. */
inline void data_write_sql_null(
  byte *data, /*!< in: pointer to a buffer of size len */
  ulint len
); /*!< in: SQL null size in bytes */
/*** Copies the data and len fields. */
inline void dfield_copy_data(
  dfield_t *field1, /*!< out: field to copy to */
  const dfield_t *field2
); /*!< in: field to copy from */
/*** Copies a data field to another. */
inline void dfield_copy(
  dfield_t *field1, /*!< out: field to copy to */
  const dfield_t *field2
); /*!< in: field to copy from */
/*** Copies the data pointed to by a data field. */
inline void dfield_dup(
  dfield_t *field, /*!< in/out: data field */
  mem_heap_t *heap
); /*!< in: memory heap where allocated */
/*** Tests if data length and content is equal for two dfields.
@return	true if equal */
inline bool dfield_datas_are_binary_equal(
  const dfield_t *field1, /*!< in: field */
  const dfield_t *field2
); /*!< in: field */
/*** Tests if dfield data length and content is equal to the given.
@return	true if equal */

bool dfield_data_is_binary_equal(
  const dfield_t *field, /*!< in: field */
  ulint len,             /*!< in: data length or UNIV_SQL_NULL */
  const byte *data
); /*!< in: data */
/*** Gets number of fields in a data tuple.
@return	number of fields */
inline ulint dtuple_get_n_fields(const dtuple_t *tuple); /*!< in: tuple */
#ifdef UNIV_DEBUG
/*** Gets nth field of a tuple.
@return	nth field */
inline dfield_t *dtuple_get_nth_field(
  const dtuple_t *tuple, /*!< in: tuple */
  ulint n
);    /*!< in: index of field */
#else /* UNIV_DEBUG */
#define dtuple_get_nth_field(tuple, n) ((tuple)->fields + (n))
#endif /* UNIV_DEBUG */
/*** Gets info bits in a data tuple.
@return	info bits */
inline ulint dtuple_get_info_bits(const dtuple_t *tuple); /*!< in: tuple */
/*** Sets info bits in a data tuple. */
inline void dtuple_set_info_bits(
  dtuple_t *tuple, /*!< in: tuple */
  ulint info_bits
); /*!< in: info bits */
/*** Gets number of fields used in record comparisons.
@return	number of fields used in comparisons in rem0cmp.* */
inline ulint dtuple_get_n_fields_cmp(const dtuple_t *tuple); /*!< in: tuple */
/*** Gets number of fields used in record comparisons. */
inline void dtuple_set_n_fields_cmp(
  dtuple_t *tuple, /*!< in: tuple */
  ulint n_fields_cmp
); /*!< in: number of fields used
                                             in comparisons in rem0cmp.* */
/*** Creates a data tuple to a memory heap. The default value for number
of fields used in record comparisons for this tuple is n_fields.
@return	own: created tuple */
inline dtuple_t *dtuple_create(
  mem_heap_t *heap, /*!< in: memory heap where the
                                                 tuple is created */
  ulint n_fields
); /*!< in: number of fields */

/*** Wrap data fields in a tuple. The default value for number
of fields used in record comparisons for this tuple is n_fields.
@return	data tuple */
inline const dtuple_t *dtuple_from_fields(
  dtuple_t *tuple,        /*!< in: storage for data tuple */
  const dfield_t *fields, /*!< in: fields */
  ulint n_fields
); /*!< in: number of fields */

/*** Sets number of fields used in a tuple. Normally this is set in
dtuple_create, but if you want later to set it smaller, you can use this. */

void dtuple_set_n_fields(
  dtuple_t *tuple, /*!< in: tuple */
  ulint n_fields
); /*!< in: number of fields */
/*** Copies a data tuple to another.  This is a shallow copy; if a deep copy
is desired, dfield_dup() will have to be invoked on each field.
@return	own: copy of tuple */
inline dtuple_t *dtuple_copy(
  const dtuple_t *tuple, /*!< in: tuple to copy from */
  mem_heap_t *heap
); /*!< in: memory heap
                                   where the tuple is created */
/*** The following function returns the sum of data lengths of a tuple. The
space occupied by the field structs or the tuple struct is not counted.
@return	sum of data lens */
inline ulint dtuple_get_data_size(
  const dtuple_t *tuple, /*!< in: typed data tuple */
  ulint comp
); /*!< in: nonzero=ROW_FORMAT=COMPACT  */
/*** Computes the number of externally stored fields in a data tuple.
@return	number of fields */
inline ulint dtuple_get_n_ext(const dtuple_t *tuple); /*!< in: tuple */
/*** Compare two data tuples, respecting the collation of character fields.
@return	1, 0 , -1 if tuple1 is greater, equal, less, respectively,
than tuple2 */

int dtuple_coll_cmp(
  void *cmp_ctx,          /*!< in: client compare context */
  const dtuple_t *tuple1, /*!< in: tuple 1 */
  const dtuple_t *tuple2
); /*!< in: tuple 2 */
/*** Folds a prefix given as the number of fields of a tuple.
@return	the folded value */
inline ulint dtuple_fold(
  const dtuple_t *tuple, /*!< in: the tuple */
  ulint n_fields,        /*!< in: number of complete fields to fold */
  ulint n_bytes,         /*!< in: number of bytes to fold in an
                                   incomplete last field */
  uint64_t tree_id
) /*!< in: index tree id */
  __attribute__((pure));
/*** Sets types of fields binary in a tuple. */
inline void dtuple_set_types_binary(
  dtuple_t *tuple, /*!< in: data tuple */
  ulint n
); /*!< in: number of fields to set */
/*** Checks if a dtuple contains an SQL null value.
@return	true if some field is SQL null */
inline bool dtuple_contains_null(const dtuple_t *tuple); /*!< in: dtuple */
/*** Checks that a data field is typed. Asserts an error if not.
@return	true if ok */

bool dfield_check_typed(const dfield_t *field); /*!< in: data field */
/*** Checks that a data tuple is typed. Asserts an error if not.
@return	true if ok */

bool dtuple_check_typed(const dtuple_t *tuple); /*!< in: tuple */
/*** Checks that a data tuple is typed.
@return	true if ok */

bool dtuple_check_typed_no_assert(const dtuple_t *tuple); /*!< in: tuple */
#ifdef UNIV_DEBUG
/*** Validates the consistency of a tuple which must be complete, i.e,
all fields must have been set.
@return	true if ok */

bool dtuple_validate(const dtuple_t *tuple); /*!< in: tuple */
#endif                                       /* UNIV_DEBUG */

/** Pretty prints a dfield value according to its data type. */

void dfield_print(const dfield_t *dfield); /*!< in: dfield */
/** Pretty prints a dfield value according to its data type. Also the hex string
is printed if a string contains non-printable characters. */

void dfield_print_also_hex(const dfield_t *dfield); /*!< in: dfield */
/** The following function prints the contents of a tuple. */

void dtuple_print(
  ib_stream_t ib_stream, /*!< in: output stream */
  const dtuple_t *tuple
); /*!< in: tuple */
/** Moves parts of long fields in entry to the big record vector so that
the size of tuple drops below the maximum record size allowed in the
database. Moves data only from those fields which are not necessary
to determine uniquely the insertion place of the tuple in the index.
@return own: created big record vector, nullptr if we are not able to
shorten the entry enough, i.e., if there are too many fixed-length or
short fields in entry or the index is clustered */

big_rec_t *dtuple_convert_big_rec(
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in/out: index entry */
  ulint *n_ext
); /*!< in/out: number of
                                                   externally stored columns */
/*** Puts back to entry the data stored in vector. Note that to ensure the
fields in entry can accommodate the data, vector must have been created
from entry with dtuple_convert_big_rec. */

void dtuple_convert_back_big_rec(
  dict_index_t *index, /*!< in: index */
  dtuple_t *entry,     /*!< in: entry whose data was put to vector */
  big_rec_t *vector
); /*!< in, own: big rec vector; it is
                         freed in this function */
/*** Frees the memory in a big rec vector. */
inline void dtuple_big_rec_free(big_rec_t *vector); /*!< in, own: big rec vector; it is
                                        freed in this function */
/** Reset dfield variables. */

void dfield_var_init(void);

/*######################################################################*/

/** Structure for an SQL data field */
struct dfield_t {
  /** Print the dfield_t object into the given output stream.
  @param[in]    out     the output stream.
  @return       the ouput stream. */
  std::ostream &print(std::ostream &out) const;

  void *data;        /*!< pointer to data */
  uint32_t len;      /*!< data length; UNIV_SQL_NULL if SQL null */
  uint8_t ext;       /*!< true=externally stored, false=local */
  dtype_t type;      /*!< type of data */
};

/** Structure for an SQL data tuple of fields (logical record) */
struct dtuple_t {
  ulint info_bits;    /*!< info bits of an index record:
                      the default is 0; this field is used
                      if an index record is built from
                      a data tuple */
  ulint n_fields;     /*!< number of fields in dtuple */
  ulint n_fields_cmp; /*!< number of fields which should
                      be used in comparison services
                      of rem0cmp.*; the index search
                      is performed by comparing only these
                      fields, others are ignored; the
                      default value in dtuple creation is
                      the same value as n_fields */
  dfield_t *fields;   /*!< fields */
  UT_LIST_NODE_T(dtuple_t) tuple_list;
  /*!< data tuples can be linked into a
  list using this field */
#ifdef UNIV_DEBUG
  ulint magic_n; /*!< magic number, used in
                 debug assertions */
/** Value of dtuple_struct::magic_n */
#define DATA_TUPLE_MAGIC_N 65478679
#endif /* UNIV_DEBUG */
};

/** A slot for a field in a big rec vector */
typedef struct big_rec_field_struct big_rec_field_t;

/** A slot for a field in a big rec vector */
struct big_rec_field_struct {
  ulint field_no;   /*!< field number in record */
  ulint len;        /*!< stored data length, in bytes */
  const void *data; /*!< stored data */
};

/** Storage format for overflow data in a big record, that is, a
clustered index record which needs external storage of data fields */
struct big_rec_struct {
  mem_heap_t *heap;        /*!< memory heap from which
                           allocated */
  ulint n_fields;          /*!< number of stored fields */
  big_rec_field_t *fields; /*!< stored fields */
};

#include "mem0mem.h"
#include "ut0rnd.h"

#ifdef UNIV_DEBUG
/** Dummy variable to catch access to uninitialized fields.  In the
debug version, dtuple_create() will make all fields of dtuple_t point
to data_error. */
extern byte data_error;

/** Gets pointer to the type struct of SQL data field.
@return	pointer to the type struct */
inline dtype_t *dfield_get_type(const dfield_t *field) /*!< in: SQL data field */
{
  ut_ad(field);

  return ((dtype_t *)&(field->type));
}
#endif /* UNIV_DEBUG */

/** Sets the type struct of SQL data field. */
inline void dfield_set_type(
  dfield_t *field, /*!< in: SQL data field */
  dtype_t *type
) /*!< in: pointer to data type struct */
{
  ut_ad(field && type);

  field->type = *type;
}

#ifdef UNIV_DEBUG
/** Gets pointer to the data in a field.
@return	pointer to data */
inline void *dfield_get_data(const dfield_t *field) /*!< in: field */
{
  ut_ad(field);
  ut_ad((field->len == UNIV_SQL_NULL) || (field->data != &data_error));

  return ((void *)field->data);
}
#endif /* UNIV_DEBUG */

/** Gets length of field data.
@return	length of data; UNIV_SQL_NULL if SQL null data */
inline ulint dfield_get_len(const dfield_t *field) /*!< in: field */
{
  ut_ad(field);
  ut_ad((field->len == UNIV_SQL_NULL) || (field->data != &data_error));

  return (field->len);
}

/** Sets length in a field. */
inline void dfield_set_len(
  dfield_t *field, /*!< in: field */
  ulint len
) /*!< in: length or UNIV_SQL_NULL */
{
  ut_ad(field);
#ifdef UNIV_VALGRIND_DEBUG
  if (len != UNIV_SQL_NULL)
    UNIV_MEM_ASSERT_RW(field->data, len);
#endif /* UNIV_VALGRIND_DEBUG */

  field->ext = 0;
  field->len = len;
}

/** Determines if a field is SQL nullptr
@return	nonzero if SQL null data */
inline ulint dfield_is_null(const dfield_t *field) /*!< in: field */
{
  ut_ad(field);

  return (field->len == UNIV_SQL_NULL);
}

/** Determines if a field is externally stored
@return	nonzero if externally stored */
inline ulint dfield_is_ext(const dfield_t *field) /*!< in: field */
{
  ut_ad(field);

  return (unlikely(field->ext));
}

/** Sets the "external storage" flag */
inline void dfield_set_ext(dfield_t *field) /*!< in/out: field */
{
  ut_ad(field);

  field->ext = 1;
}

/** Sets pointer to the data and length in a field. */
inline void dfield_set_data(
  dfield_t *field,  /*!< in: field */
  const void *data, /*!< in: data */
  ulint len
) /*!< in: length or UNIV_SQL_NULL */
{
  ut_ad(field);

#ifdef UNIV_VALGRIND_DEBUG
  if (len != UNIV_SQL_NULL)
    UNIV_MEM_ASSERT_RW(data, len);
#endif /* UNIV_VALGRIND_DEBUG */
  field->data = (void *)data;
  field->ext = 0;
  field->len = len;
}

/** Sets a data field to SQL nullptr. */
inline void dfield_set_null(dfield_t *field) /*!< in/out: field */
{
  dfield_set_data(field, nullptr, UNIV_SQL_NULL);
}

/** Copies the data and len fields. */
inline void dfield_copy_data(
  dfield_t *field1, /*!< out: field to copy to */
  const dfield_t *field2
) /*!< in: field to copy from */
{
  ut_ad(field1 && field2);

  field1->data = field2->data;
  field1->len = field2->len;
  field1->ext = field2->ext;
}

/** Copies a data field to another. */
inline void dfield_copy(
  dfield_t *field1, /*!< out: field to copy to */
  const dfield_t *field2
) /*!< in: field to copy from */
{
  *field1 = *field2;
}

/** Copies the data pointed to by a data field. */
inline void dfield_dup(
  dfield_t *field, /*!< in/out: data field */
  mem_heap_t *heap
) /*!< in: memory heap where allocated */
{
  if (!dfield_is_null(field)) {
    UNIV_MEM_ASSERT_RW(field->data, field->len);
    field->data = mem_heap_dup(heap, field->data, field->len);
  }
}

/** Tests if data length and content is equal for two dfields.
@return	true if equal */
inline bool dfield_datas_are_binary_equal(
  const dfield_t *field1, /*!< in: field */
  const dfield_t *field2
) /*!< in: field */
{
  ulint len;

  len = field1->len;

  return (len == field2->len && (len == UNIV_SQL_NULL || !memcmp(field1->data, field2->data, len)));
}

/** Gets info bits in a data tuple.
@return	info bits */
inline ulint dtuple_get_info_bits(const dtuple_t *tuple) /*!< in: tuple */
{
  ut_ad(tuple);

  return (tuple->info_bits);
}

/** Sets info bits in a data tuple. */
inline void dtuple_set_info_bits(
  dtuple_t *tuple, /*!< in: tuple */
  ulint info_bits
) /*!< in: info bits */
{
  ut_ad(tuple);

  tuple->info_bits = info_bits;
}

/** Gets number of fields used in record comparisons.
@return	number of fields used in comparisons in rem0cmp.* */
inline ulint dtuple_get_n_fields_cmp(const dtuple_t *tuple) /*!< in: tuple */
{
  ut_ad(tuple);

  return (tuple->n_fields_cmp);
}

/** Sets number of fields used in record comparisons. */
inline void dtuple_set_n_fields_cmp(
  dtuple_t *tuple, /*!< in: tuple */
  ulint n_fields_cmp
) /*!< in: number of fields used
                                            in comparisons in rem0cmp.* */
{
  ut_ad(tuple);
  ut_ad(n_fields_cmp <= tuple->n_fields);

  tuple->n_fields_cmp = n_fields_cmp;
}

/** Gets number of fields in a data tuple.
@return	number of fields */
inline ulint dtuple_get_n_fields(const dtuple_t *tuple) /*!< in: tuple */
{
  ut_ad(tuple);

  return (tuple->n_fields);
}

#ifdef UNIV_DEBUG
/** Gets nth field of a tuple.
@return	nth field */
inline dfield_t *dtuple_get_nth_field(
  const dtuple_t *tuple, /*!< in: tuple */
  ulint n
) /*!< in: index of field */
{
  ut_ad(tuple);
  ut_ad(n < tuple->n_fields);

  return ((dfield_t *)tuple->fields + n);
}
#endif /* UNIV_DEBUG */

/** Creates a data tuple to a memory heap. The default value for number
of fields used in record comparisons for this tuple is n_fields.
@return	own: created tuple */
inline dtuple_t *dtuple_create(
  mem_heap_t *heap, /*!< in: memory heap where the
                                                 tuple is created */
  ulint n_fields
) /*!< in: number of fields */
{
  dtuple_t *tuple;

  ut_ad(heap);

  tuple = (dtuple_t *)mem_heap_alloc(heap, sizeof(dtuple_t) + n_fields * sizeof(dfield_t));
  tuple->info_bits = 0;
  tuple->n_fields = n_fields;
  tuple->n_fields_cmp = n_fields;
  tuple->fields = (dfield_t *)&tuple[1];

#ifdef UNIV_DEBUG
  tuple->magic_n = DATA_TUPLE_MAGIC_N;

  { /* In the debug version, initialize fields to an error value */
    ulint i;

    for (i = 0; i < n_fields; i++) {
      dfield_t *field;

      field = dtuple_get_nth_field(tuple, i);

      dfield_set_len(field, UNIV_SQL_NULL);
      field->data = &data_error;
      dfield_get_type(field)->mtype = DATA_ERROR;
    }
  }

  UNIV_MEM_INVALID(tuple->fields, n_fields * sizeof *tuple->fields);
#endif
  return (tuple);
}

/** Wrap data fields in a tuple. The default value for number
of fields used in record comparisons for this tuple is n_fields.
@return	data tuple */
inline const dtuple_t *dtuple_from_fields(
  dtuple_t *tuple,        /*!< in: storage for data tuple */
  const dfield_t *fields, /*!< in: fields */
  ulint n_fields
) /*!< in: number of fields */
{
  tuple->info_bits = 0;
  tuple->n_fields = tuple->n_fields_cmp = n_fields;
  tuple->fields = (dfield_t *)fields;
  ut_d(tuple->magic_n = DATA_TUPLE_MAGIC_N);

  return (tuple);
}

/** Copies a data tuple to another.  This is a shallow copy; if a deep copy
is desired, dfield_dup() will have to be invoked on each field.
@return	own: copy of tuple */
inline dtuple_t *dtuple_copy(
  const dtuple_t *tuple, /*!< in: tuple to copy from */
  mem_heap_t *heap
) /*!< in: memory heap
                                   where the tuple is created */
{
  ulint n_fields = dtuple_get_n_fields(tuple);
  dtuple_t *new_tuple = dtuple_create(heap, n_fields);
  ulint i;

  for (i = 0; i < n_fields; i++) {
    dfield_copy(dtuple_get_nth_field(new_tuple, i), dtuple_get_nth_field(tuple, i));
  }

  return (new_tuple);
}

/** The following function returns the sum of data lengths of a tuple. The space
occupied by the field structs or the tuple struct is not counted. Neither
is possible space in externally stored parts of the field.
@return	sum of data lengths */
inline ulint dtuple_get_data_size(
  const dtuple_t *tuple, /*!< in: typed data tuple */
  ulint comp
) /*!< in: nonzero=ROW_FORMAT=COMPACT  */
{
  const dfield_t *field;
  ulint n_fields;
  ulint len;
  ulint i;
  ulint sum = 0;

  ut_ad(tuple);
  ut_ad(dtuple_check_typed(tuple));
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);

  n_fields = tuple->n_fields;

  for (i = 0; i < n_fields; i++) {
    field = dtuple_get_nth_field(tuple, i);
    len = dfield_get_len(field);

    if (len == UNIV_SQL_NULL) {
      len = dtype_get_sql_null_size(dfield_get_type(field), comp);
    }

    sum += len;
  }

  return (sum);
}

/** Computes the number of externally stored fields in a data tuple.
@return	number of externally stored fields */
inline ulint dtuple_get_n_ext(const dtuple_t *tuple) /*!< in: tuple */
{
  ulint n_ext = 0;
  ulint n_fields = tuple->n_fields;
  ulint i;

  ut_ad(tuple);
  ut_ad(dtuple_check_typed(tuple));
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);

  for (i = 0; i < n_fields; i++) {
    n_ext += dtuple_get_nth_field(tuple, i)->ext;
  }

  return (n_ext);
}

/** Sets types of fields binary in a tuple. */
inline void dtuple_set_types_binary(
  dtuple_t *tuple, /*!< in: data tuple */
  ulint n
) /*!< in: number of fields to set */
{
  dtype_t *dfield_type;
  ulint i;

  for (i = 0; i < n; i++) {
    dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
    dtype_set(dfield_type, DATA_BINARY, 0, 0);
  }
}

/** Folds a prefix given as the number of fields of a tuple.
@return	the folded value */
inline ulint dtuple_fold(
  const dtuple_t *tuple, /*!< in: the tuple */
  ulint n_fields,        /*!< in: number of complete fields to fold */
  ulint n_bytes,         /*!< in: number of bytes to fold in an
                                   incomplete last field */
  uint64_t tree_id
) /*!< in: index tree id */
{
  const dfield_t *field;
  ulint i;
  const byte *data;
  ulint len;
  ulint fold;

  ut_ad(tuple);
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);
  ut_ad(dtuple_check_typed(tuple));

  fold = ut_uint64_fold(tree_id);

  for (i = 0; i < n_fields; i++) {
    field = dtuple_get_nth_field(tuple, i);

    data = (const byte *)dfield_get_data(field);
    len = dfield_get_len(field);

    if (len != UNIV_SQL_NULL) {
      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  if (n_bytes > 0) {
    field = dtuple_get_nth_field(tuple, i);

    data = (const byte *)dfield_get_data(field);
    len = dfield_get_len(field);

    if (len != UNIV_SQL_NULL) {
      if (len > n_bytes) {
        len = n_bytes;
      }

      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  return (fold);
}

/** Writes an SQL null field full of zeros. */
inline void data_write_sql_null(
  byte *data, /*!< in: pointer to a buffer of size len */
  ulint len
) /*!< in: SQL null size in bytes */
{
  memset(data, 0, len);
}

/** Checks if a dtuple contains an SQL null value.
@return	true if some field is SQL null */
inline bool dtuple_contains_null(const dtuple_t *tuple) /*!< in: dtuple */
{
  ulint n;
  ulint i;

  n = dtuple_get_n_fields(tuple);

  for (i = 0; i < n; i++) {
    if (dfield_is_null(dtuple_get_nth_field(tuple, i))) {

      return (true);
    }
  }

  return (false);
}

/** Frees the memory in a big rec vector. */
inline void dtuple_big_rec_free(big_rec_t *vector) /*!< in, own: big rec vector; it is
                                       freed in this function */
{
  mem_heap_free(vector->heap);
}
