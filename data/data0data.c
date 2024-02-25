/**
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

/** @file data/data0data.c
SQL data field and tuple

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#include "data0data.h"

#ifdef UNIV_NONINL
#include "data0data.ic"
#endif

#ifndef UNIV_HOTBACKUP
#include "btr0cur.h"
#include "dict0dict.h"
#include "page0page.h"
#include "page0zip.h"
#include "rem0cmp.h"
#include "rem0rec.h"

#include <ctype.h>
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_DEBUG
/** Dummy variable to catch access to uninitialized fields.  In the
debug version, dtuple_create() will make all fields of dtuple_t point
to data_error. */
byte data_error;

#ifndef UNIV_DEBUG_VALGRIND
/** this is used to fool the compiler in dtuple_validate */
ulint data_dummy;
#endif /* !UNIV_DEBUG_VALGRIND */
#endif /* UNIV_DEBUG */

#ifndef UNIV_HOTBACKUP
/** Reset dfield variables. */

void dfield_var_init(void) {
#ifdef UNIV_DEBUG
  data_error = 0;
#ifndef UNIV_DEBUG_VALGRIND
  data_dummy = 0;
#endif /* !UNIV_DEBUG_VALGRIND */
#endif /* UNIV_DEBUG */
}

/** Tests if dfield data length and content is equal to the given.
@return	TRUE if equal */

ibool dfield_data_is_binary_equal(
    const dfield_t *field, /*!< in: field */
    ulint len,             /*!< in: data length or UNIV_SQL_NULL */
    const byte *data)      /*!< in: data */
{
  if (len != dfield_get_len(field)) {

    return (FALSE);
  }

  if (len == UNIV_SQL_NULL) {

    return (TRUE);
  }

  if (0 != memcmp(dfield_get_data(field), data, len)) {

    return (FALSE);
  }

  return (TRUE);
}

/** Compare two data tuples, respecting the collation of character fields.
@return 1, 0 , -1 if tuple1 is greater, equal, less, respectively,
than tuple2 */

int dtuple_coll_cmp(void *cmp_ctx,          /*!< in: client compare context */
                    const dtuple_t *tuple1, /*!< in: tuple 1 */
                    const dtuple_t *tuple2) /*!< in: tuple 2 */
{
  ulint n_fields;
  ulint i;

  ut_ad(tuple1 && tuple2);
  ut_ad(tuple1->magic_n == DATA_TUPLE_MAGIC_N);
  ut_ad(tuple2->magic_n == DATA_TUPLE_MAGIC_N);
  ut_ad(dtuple_check_typed(tuple1));
  ut_ad(dtuple_check_typed(tuple2));

  n_fields = dtuple_get_n_fields(tuple1);

  if (n_fields != dtuple_get_n_fields(tuple2)) {

    return (n_fields < dtuple_get_n_fields(tuple2) ? -1 : 1);
  }

  for (i = 0; i < n_fields; i++) {
    int cmp;
    const dfield_t *field1 = dtuple_get_nth_field(tuple1, i);
    const dfield_t *field2 = dtuple_get_nth_field(tuple2, i);

    cmp = cmp_dfield_dfield(cmp_ctx, field1, field2);

    if (cmp) {
      return (cmp);
    }
  }

  return (0);
}

/** Sets number of fields used in a tuple. Normally this is set in
dtuple_create, but if you want later to set it smaller, you can use this. */

void dtuple_set_n_fields(dtuple_t *tuple, /*!< in: tuple */
                         ulint n_fields)  /*!< in: number of fields */
{
  ut_ad(tuple);

  tuple->n_fields = n_fields;
  tuple->n_fields_cmp = n_fields;
}

/** Checks that a data field is typed.
@return	TRUE if ok */
static ibool
dfield_check_typed_no_assert(const dfield_t *field) /*!< in: data field */
{
  if (dfield_get_type(field)->mtype > DATA_CLIENT ||
      dfield_get_type(field)->mtype < DATA_VARCHAR) {

    ib_logger(ib_stream, "InnoDB: Error: data field type %lu, len %lu\n",
              (ulong)dfield_get_type(field)->mtype,
              (ulong)dfield_get_len(field));
    return (FALSE);
  }

  return (TRUE);
}

/** Checks that a data tuple is typed.
@return	TRUE if ok */

ibool dtuple_check_typed_no_assert(const dtuple_t *tuple) /*!< in: tuple */
{
  const dfield_t *field;
  ulint i;

  if (dtuple_get_n_fields(tuple) > REC_MAX_N_FIELDS) {
    ib_logger(ib_stream, "InnoDB: Error: index entry has %lu fields\n",
              (ulong)dtuple_get_n_fields(tuple));
  dump:
    ib_logger(ib_stream, "InnoDB: Tuple contents: ");
    dtuple_print(ib_stream, tuple);
    ib_logger(ib_stream, "\n");

    return (FALSE);
  }

  for (i = 0; i < dtuple_get_n_fields(tuple); i++) {

    field = dtuple_get_nth_field(tuple, i);

    if (!dfield_check_typed_no_assert(field)) {
      goto dump;
    }
  }

  return (TRUE);
}
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_DEBUG
/** Checks that a data field is typed. Asserts an error if not.
@return	TRUE if ok */

ibool dfield_check_typed(const dfield_t *field) /*!< in: data field */
{
  if (dfield_get_type(field)->mtype > DATA_CLIENT ||
      dfield_get_type(field)->mtype < DATA_VARCHAR) {

    ib_logger(ib_stream, "InnoDB: Error: data field type %lu, len %lu\n",
              (ulong)dfield_get_type(field)->mtype,
              (ulong)dfield_get_len(field));

    ut_error;
  }

  return (TRUE);
}

/** Checks that a data tuple is typed. Asserts an error if not.
@return	TRUE if ok */

ibool dtuple_check_typed(const dtuple_t *tuple) /*!< in: tuple */
{
  const dfield_t *field;
  ulint i;

  for (i = 0; i < dtuple_get_n_fields(tuple); i++) {

    field = dtuple_get_nth_field(tuple, i);

    ut_a(dfield_check_typed(field));
  }

  return (TRUE);
}

/** Validates the consistency of a tuple which must be complete, i.e,
all fields must have been set.
@return	TRUE if ok */

ibool dtuple_validate(const dtuple_t *tuple) /*!< in: tuple */
{
  const dfield_t *field;
  ulint n_fields;
  ulint len;
  ulint i;

  ut_ad(tuple->magic_n == DATA_TUPLE_MAGIC_N);

  n_fields = dtuple_get_n_fields(tuple);

  /* We dereference all the data of each field to test
  for memory traps */

  for (i = 0; i < n_fields; i++) {

    field = dtuple_get_nth_field(tuple, i);
    len = dfield_get_len(field);

    if (!dfield_is_null(field)) {

      const byte *data = dfield_get_data(field);
#ifndef UNIV_DEBUG_VALGRIND
      ulint j;

      for (j = 0; j < len; j++) {

        data_dummy += *data; /* fool the compiler not
                             to optimize out this
                             code */
        data++;
      }
#endif /* !UNIV_DEBUG_VALGRIND */

      UNIV_MEM_ASSERT_RW(data, len);
    }
  }

  ut_a(dtuple_check_typed(tuple));

  return (TRUE);
}
#endif /* UNIV_DEBUG */

#ifndef UNIV_HOTBACKUP
/** Pretty prints a dfield value according to its data type. */

void dfield_print(const dfield_t *dfield) /*!< in: dfield */
{
  const byte *data;
  ulint len;
  ulint i;

  len = dfield_get_len(dfield);
  data = dfield_get_data(dfield);

  if (dfield_is_null(dfield)) {
    ib_logger(ib_stream, "NULL");

    return;
  }

  switch (dtype_get_mtype(dfield_get_type(dfield))) {
  case DATA_CHAR:
  case DATA_VARCHAR:
    for (i = 0; i < len; i++) {
      int c = *data++;
      ib_logger(ib_stream, "%c", isprint(c) ? c : ' ');
    }

    if (dfield_is_ext(dfield)) {
      ib_logger(ib_stream, "(external)");
    }
    break;
  case DATA_INT:
    ut_a(len == 4); /* only works for 32-bit integers */
    ib_logger(ib_stream, "%d", (int)mach_read_from_4(data));
    break;
  default:
    ut_error;
  }
}

/** Pretty prints a dfield value according to its data type. Also the hex string
is printed if a string contains non-printable characters. */

void dfield_print_also_hex(const dfield_t *dfield) /*!< in: dfield */
{
  const byte *data;
  ulint len;
  ulint prtype;
  ulint i;
  ibool print_also_hex;

  len = dfield_get_len(dfield);
  data = dfield_get_data(dfield);

  if (dfield_is_null(dfield)) {
    ib_logger(ib_stream, "NULL");

    return;
  }

  prtype = dtype_get_prtype(dfield_get_type(dfield));

  switch (dtype_get_mtype(dfield_get_type(dfield))) {
    dulint id;
  case DATA_INT:
    switch (len) {
      ulint val;
    case 1:
      val = mach_read_from_1(data);

      if (!(prtype & DATA_UNSIGNED)) {
        val &= ~0x80;
        ib_logger(ib_stream, "%ld", (long)val);
      } else {
        ib_logger(ib_stream, "%lu", (ulong)val);
      }
      break;

    case 2:
      val = mach_read_from_2(data);

      if (!(prtype & DATA_UNSIGNED)) {
        val &= ~0x8000;
        ib_logger(ib_stream, "%ld", (long)val);
      } else {
        ib_logger(ib_stream, "%lu", (ulong)val);
      }
      break;

    case 3:
      val = mach_read_from_3(data);

      if (!(prtype & DATA_UNSIGNED)) {
        val &= ~0x800000;
        ib_logger(ib_stream, "%ld", (long)val);
      } else {
        ib_logger(ib_stream, "%lu", (ulong)val);
      }
      break;

    case 4:
      val = mach_read_from_4(data);

      if (!(prtype & DATA_UNSIGNED)) {
        val &= ~0x80000000;
        ib_logger(ib_stream, "%ld", (long)val);
      } else {
        ib_logger(ib_stream, "%lu", (ulong)val);
      }
      break;

    case 6:
      id = mach_read_from_6(data);
      ib_logger(ib_stream, "{%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
      break;

    case 7:
      id = mach_read_from_7(data);
      ib_logger(ib_stream, "{%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
      break;
    case 8:
      id = mach_read_from_8(data);
      ib_logger(ib_stream, "{%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
      break;
    default:
      goto print_hex;
    }
    break;

  case DATA_SYS:
    switch (prtype & DATA_SYS_PRTYPE_MASK) {
    case DATA_TRX_ID:
      id = mach_read_from_6(data);

      ib_logger(ib_stream, "trx_id " TRX_ID_FMT, TRX_ID_PREP_PRINTF(id));
      break;

    case DATA_ROLL_PTR:
      id = mach_read_from_7(data);

      ib_logger(ib_stream, "roll_ptr {%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
      break;

    case DATA_ROW_ID:
      id = mach_read_from_6(data);

      ib_logger(ib_stream, "row_id {%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
      break;

    default:
      id = mach_dulint_read_compressed(data);

      ib_logger(ib_stream, "mix_id {%lu %lu}", ut_dulint_get_high(id),
                ut_dulint_get_low(id));
    }
    break;

  case DATA_CHAR:
  case DATA_VARCHAR:
    print_also_hex = FALSE;

    for (i = 0; i < len; i++) {
      int c = *data++;

      if (!isprint(c)) {
        print_also_hex = TRUE;

        ib_logger(ib_stream, "\\x%02x", (unsigned char)c);
      } else {
        ib_logger(ib_stream, "%c", c);
      }
    }

    if (dfield_is_ext(dfield)) {
      ib_logger(ib_stream, "(external)");
    }

    if (!print_also_hex) {
      break;
    }

    data = dfield_get_data(dfield);
    /* fall through */

  case DATA_BINARY:
  default:
  print_hex:
    ib_logger(ib_stream, " Hex: ");

    for (i = 0; i < len; i++) {
      ib_logger(ib_stream, "%02lx", (ulint)*data++);
    }

    if (dfield_is_ext(dfield)) {
      ib_logger(ib_stream, "(external)");
    }
  }
}

/** Print a dfield value using ut_print_buf. */
static void dfield_print_raw(ib_stream_t ib_stream,  /*!< in: output stream */
                             const dfield_t *dfield) /*!< in: dfield */
{
  ulint len = dfield_get_len(dfield);
  if (!dfield_is_null(dfield)) {
    ulint print_len = ut_min(len, 1000);
    ut_print_buf(ib_stream, dfield_get_data(dfield), print_len);
    if (len != print_len) {
      ib_logger(ib_stream, "(total %lu bytes%s)", (ulong)len,
                dfield_is_ext(dfield) ? ", external" : "");
    }
  } else {
    ib_logger(ib_stream, " SQL NULL");
  }
}

/** The following function prints the contents of a tuple. */

void dtuple_print(ib_stream_t ib_stream, /*!< in: output stream */
                  const dtuple_t *tuple) /*!< in: tuple */
{
  ulint n_fields;
  ulint i;

  n_fields = dtuple_get_n_fields(tuple);

  ib_logger(ib_stream, "DATA TUPLE: %lu fields;\n", (ulong)n_fields);

  for (i = 0; i < n_fields; i++) {
    ib_logger(ib_stream, " %lu:", (ulong)i);

    dfield_print_raw(ib_stream, dtuple_get_nth_field(tuple, i));

    ib_logger(ib_stream, ";\n");
  }

  ut_ad(dtuple_validate(tuple));
}

/** Moves parts of long fields in entry to the big record vector so that
the size of tuple drops below the maximum record size allowed in the
database. Moves data only from those fields which are not necessary
to determine uniquely the insertion place of the tuple in the index.
@return own: created big record vector, NULL if we are not able to
shorten the entry enough, i.e., if there are too many fixed-length or
short fields in entry or the index is clustered */

big_rec_t *dtuple_convert_big_rec(dict_index_t *index, /*!< in: index */
                                  dtuple_t *entry, /*!< in/out: index entry */
                                  ulint *n_ext)    /*!< in/out: number of
                                                   externally stored columns */
{
  mem_heap_t *heap;
  big_rec_t *vector;
  dfield_t *dfield;
  dict_field_t *ifield;
  ulint size;
  ulint n_fields;
  ulint local_len;
  ulint local_prefix_len;

  if (UNIV_UNLIKELY(!dict_index_is_clust(index))) {
    return (NULL);
  }

  if (dict_table_get_format(index->table) < DICT_TF_FORMAT_ZIP) {
    /* up to v5.1: store a 768-byte prefix locally */
    local_len = BTR_EXTERN_FIELD_REF_SIZE + DICT_MAX_INDEX_COL_LEN;
  } else {
    /* new-format table: do not store any BLOB prefix locally */
    local_len = BTR_EXTERN_FIELD_REF_SIZE;
  }

  ut_a(dtuple_check_typed_no_assert(entry));

  size = rec_get_converted_size(index, entry, *n_ext);

  if (UNIV_UNLIKELY(size > 1000000000)) {
    ib_logger(ib_stream, "InnoDB: Warning: tuple size very big: %lu\n",
              (ulong)size);
    ib_logger(ib_stream, "InnoDB: Tuple contents: ");
    dtuple_print(ib_stream, entry);
    ib_logger(ib_stream, "\n");
  }

  heap = mem_heap_create(
      size + dtuple_get_n_fields(entry) * sizeof(big_rec_field_t) + 1000);

  vector = mem_heap_alloc(heap, sizeof(big_rec_t));

  vector->heap = heap;
  vector->fields = mem_heap_alloc(heap, dtuple_get_n_fields(entry) *
                                            sizeof(big_rec_field_t));

  /* Decide which fields to shorten: the algorithm is to look for
  a variable-length field that yields the biggest savings when
  stored externally */

  n_fields = 0;

  while (page_rec_needs_ext(rec_get_converted_size(index, entry, *n_ext),
                            dict_table_is_comp(index->table),
                            dict_index_get_n_fields(index),
                            dict_table_zip_size(index->table))) {
    ulint i;
    ulint longest = 0;
    ulint longest_i = ULINT_MAX;
    byte *data;
    big_rec_field_t *b;

    for (i = dict_index_get_n_unique_in_tree(index);
         i < dtuple_get_n_fields(entry); i++) {
      ulint savings;

      dfield = dtuple_get_nth_field(entry, i);
      ifield = dict_index_get_nth_field(index, i);

      /* Skip fixed-length, NULL, externally stored,
      or short columns */

      if (ifield->fixed_len || dfield_is_null(dfield) ||
          dfield_is_ext(dfield) || dfield_get_len(dfield) <= local_len ||
          dfield_get_len(dfield) <= BTR_EXTERN_FIELD_REF_SIZE * 2) {
        goto skip_field;
      }

      savings = dfield_get_len(dfield) - local_len;

      /* Check that there would be savings */
      if (longest >= savings) {
        goto skip_field;
      }

      /* In DYNAMIC and COMPRESSED format, store
      locally any non-BLOB columns whose maximum
      length does not exceed 256 bytes.  This is
      because there is no room for the "external
      storage" flag when the maximum length is 255
      bytes or less. This restriction trivially
      holds in REDUNDANT and COMPACT format, because
      there we always store locally columns whose
      length is up to local_len == 788 bytes.
      @see rec_init_offsets_comp_ordinary */
      if (ifield->col->mtype != DATA_BLOB && ifield->col->len < 256) {
        goto skip_field;
      }

      longest_i = i;
      longest = savings;

    skip_field:
      continue;
    }

    if (!longest) {
      /* Cannot shorten more */

      mem_heap_free(heap);

      return (NULL);
    }

    /* Move data from field longest_i to big rec vector.

    We store the first bytes locally to the record. Then
    we can calculate all ordering fields in all indexes
    from locally stored data. */

    dfield = dtuple_get_nth_field(entry, longest_i);
    ifield = dict_index_get_nth_field(index, longest_i);
    local_prefix_len = local_len - BTR_EXTERN_FIELD_REF_SIZE;

    b = &vector->fields[n_fields];
    b->field_no = longest_i;
    b->len = dfield_get_len(dfield) - local_prefix_len;
    b->data = (char *)dfield_get_data(dfield) + local_prefix_len;

    /* Allocate the locally stored part of the column. */
    data = mem_heap_alloc(heap, local_len);

    /* Copy the local prefix. */
    memcpy(data, dfield_get_data(dfield), local_prefix_len);
    /* Clear the extern field reference (BLOB pointer). */
    memset(data + local_prefix_len, 0, BTR_EXTERN_FIELD_REF_SIZE);
#if 0
		/* The following would fail the Valgrind checks in
		page_cur_insert_rec_low() and page_cur_insert_rec_zip().
		The BLOB pointers in the record will be initialized after
		the record and the BLOBs have been written. */
		UNIV_MEM_ALLOC(data + local_prefix_len,
			       BTR_EXTERN_FIELD_REF_SIZE);
#endif

    dfield_set_data(dfield, data, local_len);
    dfield_set_ext(dfield);

    n_fields++;
    (*n_ext)++;
    ut_ad(n_fields < dtuple_get_n_fields(entry));
  }

  vector->n_fields = n_fields;
  return (vector);
}

/** Puts back to entry the data stored in vector. Note that to ensure the
fields in entry can accommodate the data, vector must have been created
from entry with dtuple_convert_big_rec. */

void dtuple_convert_back_big_rec(
    dict_index_t *index __attribute__((unused)), /*!< in: index */
    dtuple_t *entry,   /*!< in: entry whose data was put to vector */
    big_rec_t *vector) /*!< in, own: big rec vector; it is
                       freed in this function */
{
  big_rec_field_t *b = vector->fields;
  const big_rec_field_t *const end = b + vector->n_fields;

  for (; b < end; b++) {
    dfield_t *dfield;
    ulint local_len;

    dfield = dtuple_get_nth_field(entry, b->field_no);
    local_len = dfield_get_len(dfield);

    ut_ad(dfield_is_ext(dfield));
    ut_ad(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

    local_len -= BTR_EXTERN_FIELD_REF_SIZE;

    ut_ad(local_len <= DICT_MAX_INDEX_COL_LEN);

    dfield_set_data(dfield, (char *)b->data - local_len, b->len + local_len);
  }

  mem_heap_free(vector->heap);
}
#endif /* !UNIV_HOTBACKUP */
