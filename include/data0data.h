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

#include "data0type.h"
#include "data0types.h"
#include "dict0types.h"
#include "mem0mem.h"
#include "ut0rnd.h"

struct dict_index_t;

/** Storage for overflow data in a big record, that is, a clustered
index record which needs external storage of data fields */
struct big_rec_t;

#ifdef UNIV_DEBUG
/**
 * @brief Gets pointer to the type struct of SQL data field.
 * @param field - SQL data field
 * @return pointer to the type struct
 */
inline dtype_t *dfield_get_type(const dfield_t *field);

/**
 * @brief Gets pointer to the data in a field.
 * @param field - field
 * @return pointer to data
 */
inline void *dfield_get_data(const dfield_t *field);
#else /* UNIV_DEBUG */
#define dfield_get_type(field) (&field->type)
#define dfield_get_data(field) (reinterpret_cast<byte *>(field->data))
#endif /* UNIV_DEBUG */

/**
 * @brief Tests if dfield data length and content is equal to the given.
 * @param field - field
 * @param len - data length or UNIV_SQL_NULL
 * @param data - data
 * @return true if equal
 */
bool dfield_data_is_binary_equal(const dfield_t *field, ulint len, const byte *data);

/**
 * @brief Sets number of fields used in a tuple.
 * Normally this is set in dtuple_create, but if you want later to set it smaller, you can use this.
 * @param tuple - tuple
 * @param n_fields - number of fields
 */
void dtuple_set_n_fields(dtuple_t *tuple, ulint n_fields);

/**
 * @brief Compare two data tuples, respecting the collation of character fields.
 * @param cmp_ctx - client compare context
 * @param tuple1 - tuple 1
 * @param tuple2 - tuple 2
 * @return 1, 0, -1 if tuple1 is greater, equal, less, respectively, than tuple2
 */
int dtuple_coll_cmp(void *cmp_ctx, const dtuple_t *tuple1, const dtuple_t *tuple2);

/**
 * @brief Checks that a data field is typed. Asserts an error if not.
 * @param field - data field
 * @return true if ok
 */
bool dfield_check_typed(const dfield_t *field);

/**
 * @brief Checks that a data tuple is typed. Asserts an error if not.
 * @param tuple - tuple
 * @return true if ok
 */
bool dtuple_check_typed(const dtuple_t *tuple);

/**
 * @brief Checks that a data tuple is typed.
 * @param tuple - tuple
 * @return true if ok
 */
bool dtuple_check_typed_no_assert(const dtuple_t *tuple);

#ifdef UNIV_DEBUG
/**
 * @brief Validates the consistency of a tuple which must be complete, i.e,
 * all fields must have been set.
 * @param tuple - tuple
 * @return true if ok
 */
bool dtuple_validate(const dtuple_t *tuple);
#endif /* UNIV_DEBUG */

/** Pretty prints a dfield value according to its data type.
 * @param dfield - dfield
 */
void dfield_print(const dfield_t *dfield);

/** Pretty prints a dfield value according to its data type. Also the hex string
 * is printed if a string contains non-printable characters.
 * @param dfield - dfield
 */
void dfield_print_also_hex(const dfield_t *dfield);

/** The following function prints the contents of a tuple.
 * @param ib_stream - output stream
 * @param tuple - tuple
 */
void dtuple_print(ib_stream_t ib_stream, const dtuple_t *tuple);

/** Moves parts of long fields in entry to the big record vector so that
 * the size of tuple drops below the maximum record size allowed in the
 * database. Moves data only from those fields which are not necessary
 * to determine uniquely the insertion place of the tuple in the index.
 * @param index - index
 * @param entry - index entry
 * @param n_ext - number of externally stored columns
 * @return created big record vector, nullptr if we are not able to
 * shorten the entry enough, i.e., if there are too many fixed-length or
 * short fields in entry or the index is clustered
 */
big_rec_t *dtuple_convert_big_rec(dict_index_t *index, dtuple_t *entry, ulint *n_ext);

/** Puts back to entry the data stored in vector. Note that to ensure the
 * fields in entry can accommodate the data, vector must have been created
 * from entry with dtuple_convert_big_rec.
 * @param index - index
 * @param entry - entry whose data was put to vector
 * @param vector - big rec vector; it is freed in this function
 */
void dtuple_convert_back_big_rec(dict_index_t *index, dtuple_t *entry, big_rec_t *vector);

/** Reset dfield variables. */
void dfield_var_init();

#ifdef UNIV_DEBUG
/** Dummy variable to catch access to uninitialized fields.  In the
debug version, dtuple_create() will make all fields of dtuple_t point
to data_error. */
extern byte data_error;

/** Gets pointer to the type struct of SQL data field.
 * @param[in] field SQL data field
 * @return pointer to the type struct */
inline dtype_t *dfield_get_type(const dfield_t *field) {
  return const_cast<dtype_t *>(&field->type);
}
#endif /* UNIV_DEBUG */

/**
 * Sets the type struct of SQL data field.
 *
 * @param field - SQL data field
 * @param type - pointer to data type struct
 */
inline void dfield_set_type(dfield_t *field, dtype_t *type) {
  field->type = *type;
}

#ifdef UNIV_DEBUG
/** Gets pointer to the data in a field.
@return	pointer to data */
inline void *dfield_get_data(const dfield_t *field) /*!< in: field */
{
  ut_ad(field);
  ut_ad((field->len == UNIV_SQL_NULL) || (field->data != &data_error));

  return const_cast<void*>(field->data);
}
#endif /* UNIV_DEBUG */

/**
 * Gets length of field data.
 *
 * @param field - field
 * @return length of data; UNIV_SQL_NULL if SQL null data
 */
inline ulint dfield_get_len(const dfield_t *field)
{
  ut_ad(field->len == UNIV_SQL_NULL || field->data != &data_error);
return field->len;
}

/**
 * Sets length in a field.
 *
 * @param field - field
 * @param len - length or UNIV_SQL_NULL
 */
inline void dfield_set_len(dfield_t *field, ulint len) {
#ifdef UNIV_VALGRIND_DEBUG
  if (len != UNIV_SQL_NULL)
    UNIV_MEM_ASSERT_RW(field->data, len);
#endif /* UNIV_VALGRIND_DEBUG */

  field->ext = 0;
  field->len = len;
}

/**
 * Determines if a field is SQL nullptr
 *
 * @param field - field
 * @return nonzero if SQL null data
 */
inline ulint dfield_is_null(const dfield_t *field) {
  return field->len == UNIV_SQL_NULL;
}

/**
 * Determines if a field is externally stored
 *
 * @param field - field
 * @return nonzero if externally stored
 */
inline ulint dfield_is_ext(const dfield_t *field) {
  return field->ext;
}

/**
 * Sets the "external storage" flag
 *
 * @param field - field
 */
inline void dfield_set_ext(dfield_t *field) {
  field->ext = 1;
}

/**
 * Sets pointer to the data and length in a field.
 *
 * @param field - field
 * @param data - data
 * @param len - length or UNIV_SQL_NULL
 */
inline void dfield_set_data(dfield_t *field, const void *data, ulint len) {
#ifdef UNIV_VALGRIND_DEBUG
  if (len != UNIV_SQL_NULL)
    UNIV_MEM_ASSERT_RW(data, len);
#endif /* UNIV_VALGRIND_DEBUG */
  field->data = const_cast<void *>(data);
  field->ext = 0;
  field->len = len;
}

/**
 * Sets a data field to SQL nullptr.
 *
 * @param field - field
 */
inline void dfield_set_null(dfield_t *field) {
  dfield_set_data(field, nullptr, UNIV_SQL_NULL);
}

/**
 * Copies the data and len fields.
 *
 * @param field1 - out: field to copy to
 * @param field2 - in: field to copy from
 */
inline void dfield_copy_data(dfield_t *field1, const dfield_t *field2) {
  field1->data = field2->data;
  field1->len = field2->len;
  field1->ext = field2->ext;
}

/**
 * Copies a data field to another.
 *
 * @param field1 - out: field to copy to
 * @param field2 - in: field to copy from
 */
inline void dfield_copy(dfield_t *field1, const dfield_t *field2) {
  *field1 = *field2;
}

/**
 * Copies the data pointed to by a data field.
 *
 * @param field - in/out: data field
 * @param heap - in: memory heap where allocated
 */
inline void dfield_dup(dfield_t *field, mem_heap_t *heap) {
  if (!dfield_is_null(field)) {
    UNIV_MEM_ASSERT_RW(field->data, field->len);
    field->data = mem_heap_dup(heap, field->data, field->len);
  }
}

/**
 * Tests if data length and content is equal for two dfields.
 *
 * @param field1 - in: field
 * @param field2 - in: field
 * @return true if equal
 */
inline bool dfield_datas_are_binary_equal(const dfield_t *field1, const dfield_t *field2) {
  auto len = field1->len;

  return len == field2->len && (len == UNIV_SQL_NULL || !memcmp(field1->data, field2->data, len));
}

/** Gets info bits in a data tuple.
@return	info bits */
/**
 * Retrieves the information bits of a tuple.
 *
 * @param tuple The tuple to retrieve the information bits from.
 * @return The information bits of the tuple.
 */
inline ulint dtuple_get_info_bits(const dtuple_t *tuple) {
  return tuple->info_bits;
}

/**
 * Sets the info bits of a given tuple.
 *
 * @param tuple The tuple to set the info bits for.
 * @param info_bits The info bits to set.
 */
inline void dtuple_set_info_bits(dtuple_t *tuple, ulint info_bits) {
  tuple->info_bits = info_bits;
}

/**
 * Retrieves the number of fields for comparison in a tuple.
 *
 * @param tuple The tuple to retrieve the number of fields from.
 * @return The number of fields for comparison in the tuple.
 */
inline ulint dtuple_get_n_fields_cmp(const dtuple_t *tuple) {
  return tuple->n_fields_cmp;
}

/**
 * Sets the number of fields used in comparisons in rem0cmp.
 *
 * @param tuple The tuple to modify.
 * @param n_fields_cmp The number of fields used in comparisons.
 */
inline void dtuple_set_n_fields_cmp(dtuple_t *tuple, ulint n_fields_cmp) {
  ut_ad(n_fields_cmp <= tuple->n_fields);

  tuple->n_fields_cmp = n_fields_cmp;
}

/**
 * Retrieves the number of fields in a tuple.
 *
 * @param tuple The tuple to retrieve the number of fields from.
 * @return The number of fields in the tuple.
 */
inline ulint dtuple_get_n_fields(const dtuple_t *tuple) {
  return tuple->n_fields;
}

/**
 * Retrieves the nth field from a tuple.
 *
 * @param tuple The tuple from which to retrieve the field.
 * @param n The index of the field to retrieve.
 * @return A pointer to the nth field.
 */
inline dfield_t *dtuple_get_nth_field(const dtuple_t *tuple, ulint n) {
  ut_ad(n < tuple->n_fields);

  return static_cast<dfield_t *>(&tuple->fields[n]);
}

/**
 * Creates a data tuple to a memory heap.
 *
 * @param heap The memory heap where the tuple is created.
 * @param n_fields The number of fields.
 * @return The created tuple.
 */
inline dtuple_t *dtuple_create(mem_heap_t *heap, ulint n_fields) {
  ut_ad(heap);

  const ulint sz = sizeof(dtuple_t) + n_fields * sizeof(dfield_t);
  auto tuple = reinterpret_cast<dtuple_t *>(mem_heap_alloc(heap, sz));

  tuple->info_bits = 0;
  tuple->n_fields = n_fields;
  tuple->n_fields_cmp = n_fields;
  tuple->fields = (dfield_t *)&tuple[1];

#ifdef UNIV_DEBUG
  tuple->magic_n = DATA_TUPLE_MAGIC_N;

  { /* In the debug version, initialize fields to an error value */
    for (ulint i = 0; i < n_fields; i++) {
      auto field = dtuple_get_nth_field(tuple, i);

      dfield_set_len(field, UNIV_SQL_NULL);
      field->data = &data_error;
      dfield_get_type(field)->mtype = DATA_ERROR;
    }
  }

  UNIV_MEM_INVALID(tuple->fields, n_fields * sizeof *tuple->fields);
#endif /* UNIV_DEBUG */

  return tuple;
}

/**
 * Wrap data fields in a tuple. The default value for number
 * of fields used in record comparisons for this tuple is n_fields.
 *
 * @param tuple Storage for data tuple.
 * @param fields Fields.
 * @param n_fields Number of fields.
 * @return Data tuple.
 */
inline const dtuple_t *dtuple_from_fields(dtuple_t *tuple, const dfield_t *fields, ulint n_fields) {
  tuple->info_bits = 0;
  tuple->n_fields = tuple->n_fields_cmp = n_fields;
  tuple->fields = (dfield_t *)fields;
  ut_d(tuple->magic_n = DATA_TUPLE_MAGIC_N);

  return tuple;
}

/**
 * Copies a data tuple to another. This is a shallow copy; if a deep copy
 * is desired, dfield_dup() will have to be invoked on each field.
 *
 * @param tuple The tuple to copy from.
 * @param heap The memory heap where the tuple is created.
 * @return A copy of the tuple.
 */
inline dtuple_t *dtuple_copy(const dtuple_t *tuple, mem_heap_t *heap) {
  ulint n_fields = dtuple_get_n_fields(tuple);
  dtuple_t *new_tuple = dtuple_create(heap, n_fields);

  for (ulint i = 0; i < n_fields; i++) {
    dfield_copy(dtuple_get_nth_field(new_tuple, i), dtuple_get_nth_field(tuple, i));
  }

  return new_tuple;
}

/**
 * The following function returns the sum of data lengths of a tuple. The space
 * occupied by the field structs or the tuple struct is not counted. Neither
 * is possible space in externally stored parts of the field.
 *
 * @param tuple The typed data tuple.
 * @param comp Nonzero if ROW_FORMAT=COMPACT.
 * @return The sum of data lengths.
 */
inline ulint dtuple_get_data_size(const dtuple_t *tuple, ulint comp) {
  ut_ad(dtuple_check_typed(tuple));
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);

  ulint sum = 0;
  auto n_fields = tuple->n_fields;

  for (ulint i = 0; i < n_fields; i++) {
    auto field = dtuple_get_nth_field(tuple, i);
    auto len = dfield_get_len(field);

    if (len == UNIV_SQL_NULL) {
      len = dtype_get_sql_null_size(dfield_get_type(field), comp);
    }

    sum += len;
  }

  return sum;
}

/** Computes the number of externally stored fields in a data tuple.
@return	number of externally stored fields */
inline ulint dtuple_get_n_ext(const dtuple_t *tuple) /*!< in: tuple */
{
  ulint n_ext = 0;
  ulint n_fields = tuple->n_fields;

  ut_ad(tuple);
  ut_ad(dtuple_check_typed(tuple));
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);

  for (ulint i = 0; i < n_fields; i++) {
    n_ext += dtuple_get_nth_field(tuple, i)->ext;
  }

  return n_ext;
}

/**
 * Sets types of fields binary in a tuple.
 *
 * @param tuple The data tuple.
 * @param n The number of fields to set.
 */
inline void dtuple_set_types_binary(dtuple_t *tuple, ulint n) {
  for (ulint i = 0; i < n; i++) {
    auto dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
    dtype_set(dfield_type, DATA_BINARY, 0, 0);
  }
}

/**
 * Folds a prefix given as the number of fields of a tuple.
 *
 * @param tuple The tuple.
 * @param n_fields Number of complete fields to fold.
 * @param n_bytes Number of bytes to fold in an incomplete last field.
 * @param tree_id Index tree id.
 * @return The folded value.
 */
inline ulint dtuple_fold(const dtuple_t *tuple, ulint n_fields, ulint n_bytes, uint64_t tree_id) {
  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);
  ut_ad(dtuple_check_typed(tuple));

  ulint i;
  auto fold = ut_uint64_fold(tree_id);

  for (i = 0; i < n_fields; i++) {
    auto field = dtuple_get_nth_field(tuple, i);
    auto data = reinterpret_cast<const byte *>(dfield_get_data(field));
    auto len = dfield_get_len(field);

    if (len != UNIV_SQL_NULL) {
      fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
    }
  }

  if (n_bytes > 0) {
    auto field = dtuple_get_nth_field(tuple, i);
    auto data = reinterpret_cast<const byte *>(dfield_get_data(field));
    auto len = dfield_get_len(field);

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
 * Writes an SQL null field full of zeros.
 *
 * @param data Pointer to a buffer of size len.
 * @param len SQL null size in bytes.
 */
inline void data_write_sql_null(byte *data, ulint len) {
  memset(data, 0, len);
}

/**
 * Checks if a dtuple contains an SQL null value.
 *
 * @param tuple The dtuple.
 * @return True if some field is SQL null.
 */
inline bool dtuple_contains_null(const dtuple_t *tuple) {
  auto n = dtuple_get_n_fields(tuple);

  for (ulint i = 0; i < n; i++) {
    if (dfield_is_null(dtuple_get_nth_field(tuple, i))) {

      return true;
    }
  }

  return false;
}

/**
 * Frees the memory in a big rec vector.
 *
 * @param vector The big rec vector to free. (in, own)
 */
inline void dtuple_big_rec_free(big_rec_t *vector) {
  mem_heap_free(vector->heap);
}

inline size_t dtuple_t::get_n_ext() const {
  size_t n_ext = 0;
  for (uint32_t i = 0; i < n_fields; ++i) {
    if (dfield_is_ext(&fields[i])) {
      ++n_ext;
    }
  }
  return n_ext;
}

inline bool dtuple_t::has_ext() const {
  for (uint32_t i = 0; i < n_fields; ++i) {
    if (dfield_is_ext(&fields[i])) {
      return true;
    }
  }
  return false;
}

/** Print dtuple to the output stream.
 * @param o Output stream
 * @param tuple Tuple to print
*/
std::ostream &dtuple_print(std::ostream &o, const dtuple_t *tuple);