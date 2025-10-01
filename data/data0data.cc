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

/** @file data/data0data.c
SQL data field and tuple

Created 5/30/1994 Heikki Tuuri
*************************************************************************/

#include "data0data.h"

#include "btr0cur.h"
#include "dict0dict.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "rem0rec.h"

#include <ctype.h>
#include <algorithm>

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

/** Reset dfield variables. */

void dfield_var_init(void) {
#ifdef UNIV_DEBUG
  data_error = 0;
#ifndef UNIV_DEBUG_VALGRIND
  data_dummy = 0;
#endif /* !UNIV_DEBUG_VALGRIND */
#endif /* UNIV_DEBUG */
}

bool dfield_data_is_binary_equal(const dfield_t *field, ulint len, const byte *data) {
  if (len != dfield_get_len(field)) {

    return (false);
  }

  if (len == UNIV_SQL_NULL) {

    return (true);
  }

  if (0 != memcmp(dfield_get_data(field), data, len)) {

    return (false);
  }

  return (true);
}

int dtuple_coll_cmp(void *cmp_ctx, const DTuple *tuple1, const DTuple *tuple2) {
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

void dtuple_set_n_fields(DTuple *tuple, ulint n_fields) {
  ut_ad(tuple);

  tuple->n_fields = n_fields;
  tuple->n_fields_cmp = n_fields;
}

/** Checks that a data field is typed.
@return	true if ok */
static bool dfield_check_typed_no_assert(const dfield_t *field) /*!< in: data field */
{
  if (dfield_get_type(field)->mtype > DATA_CLIENT || dfield_get_type(field)->mtype < DATA_VARCHAR) {

    ib_logger(
      ib_stream, "Error: data field type %lu, len %lu\n", (ulong)dfield_get_type(field)->mtype, (ulong)dfield_get_len(field)
    );
    return (false);
  }

  return (true);
}

bool dtuple_check_typed_no_assert(const DTuple *tuple) {
  const dfield_t *field;
  ulint i;

  if (dtuple_get_n_fields(tuple) > REC_MAX_N_FIELDS) {
    ib_logger(ib_stream, "Error: index entry has %lu fields\n", (ulong)dtuple_get_n_fields(tuple));
  dump:
    ib_logger(ib_stream, "Tuple contents: ");
    dtuple_print(ib_stream, tuple);
    ib_logger(ib_stream, "\n");

    return (false);
  }

  for (i = 0; i < dtuple_get_n_fields(tuple); i++) {

    field = dtuple_get_nth_field(tuple, i);

    if (!dfield_check_typed_no_assert(field)) {
      goto dump;
    }
  }

  return (true);
}

#ifdef UNIV_DEBUG
bool dfield_check_typed(const dfield_t *field) {
  if (dfield_get_type(field)->mtype > DATA_CLIENT || dfield_get_type(field)->mtype < DATA_VARCHAR) {

    ib_logger(
      ib_stream, "Error: data field type %lu, len %lu\n", (ulong)dfield_get_type(field)->mtype, (ulong)dfield_get_len(field)
    );

    ut_error;
  }

  return (true);
}

bool dtuple_check_typed(const DTuple *tuple) {
  const dfield_t *field;
  ulint i;

  for (i = 0; i < dtuple_get_n_fields(tuple); i++) {

    field = dtuple_get_nth_field(tuple, i);

    ut_a(dfield_check_typed(field));
  }

  return (true);
}

bool dtuple_validate(const DTuple *tuple) {
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

      auto data = static_cast<const byte *>(dfield_get_data(field));
#ifndef UNIV_DEBUG_VALGRIND
      for (ulint j = 0; j < len; j++, ++data) {
        /* fool the compiler not to optimize out this code */
        data_dummy += *data;
      }
#endif /* !UNIV_DEBUG_VALGRIND */

      UNIV_MEM_ASSERT_RW(data, len);
    }
  }

  ut_a(dtuple_check_typed(tuple));

  return (true);
}
#endif /* UNIV_DEBUG */

void dfield_print(const dfield_t *dfield) {
  ulint i;

  auto len = dfield_get_len(dfield);
  auto data = (const byte *)dfield_get_data(dfield);

  if (dfield_is_null(dfield)) {
    ib_logger(ib_stream, "nullptr");

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

void dfield_print_also_hex(const dfield_t *dfield) {
  ulint i;
  bool print_also_hex;

  auto len = dfield_get_len(dfield);
  auto data = (const byte *)dfield_get_data(dfield);

  if (dfield_is_null(dfield)) {
    ib_logger(ib_stream, "nullptr");

    return;
  }

  auto prtype = dtype_get_prtype(dfield_get_type(dfield));

  uint64_t id;
  switch (dtype_get_mtype(dfield_get_type(dfield))) {
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
          ib_logger(ib_stream, "{%lu %lu}", id, id);
          break;

        case 7:
          id = mach_read_from_7(data);
          ib_logger(ib_stream, "{%lu %lu}", id, id);
          break;
        case 8:
          id = mach_read_from_8(data);
          ib_logger(ib_stream, "{%lu %lu}", id, id);
          break;
        default:
          goto print_hex;
      }
      break;

    case DATA_SYS:
      switch (prtype & DATA_SYS_PRTYPE_MASK) {
        case DATA_TRX_ID:
          id = mach_read_from_6(data);

          ib_logger(ib_stream, "trx_id %lu", TRX_ID_PREP_PRINTF(id));
          break;

        case DATA_ROLL_PTR:
          id = mach_read_from_7(data);

          ib_logger(ib_stream, "roll_ptr {%lu %lu}", id, id);
          break;

        case DATA_ROW_ID:
          id = mach_read_from_6(data);

          ib_logger(ib_stream, "row_id {%lu %lu}", id, id);
          break;

        default:
          id = mach_uint64_read_compressed(data);

          ib_logger(ib_stream, "mix_id {%lu %lu}", id, id);
      }
      break;

    case DATA_CHAR:
    case DATA_VARCHAR:
      print_also_hex = false;

      for (i = 0; i < len; i++) {
        int c = *data++;

        if (!isprint(c)) {
          print_also_hex = true;

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

      data = (byte *)dfield_get_data(dfield);
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

/**
 * @brief Print a dfield value using buf_to_hex_string.
 *
 * @param ib_stream Output stream.
 * @param dfield    Dfield to be printed.
 */
static void dfield_print_raw(std::ostream &o, const dfield_t *dfield) {
  ulint len = dfield_get_len(dfield);

  if (!dfield_is_null(dfield)) {
    ulint print_len = std::min<ulint>(len, 1000);

    buf_to_hex_string(o, dfield_get_data(dfield), print_len);

    if (len != print_len) {
      o << "(total " << len << (dfield_is_ext(dfield) ? ", external" : "");
    }
  } else {
    o << " SQL nullptr";
  }
}

std::ostream &dtuple_print(std::ostream &o, const DTuple *tuple) {
  const auto n_fields = dtuple_get_n_fields(tuple);

  o << "DATA TUPLE: " << n_fields << " fields;\n";

  for (ulint i = 0; i < n_fields; i++) {
    o << std::format("{}", i);

    dfield_print_raw(o, dtuple_get_nth_field(tuple, i));

    o << ";\n";
  }
  ut_ad(dtuple_validate(tuple));
  return o;
}

void dtuple_print(ib_stream_t ib_stream, const DTuple *tuple) {
  std::ostringstream os{};

  dtuple_print(os, tuple);
  ib_logger(ib_stream, "%s", os.str().c_str());
}

std::ostream &DTuple::print(std::ostream &o) const {
  dtuple_print(o, this);
  return o;
}

big_rec_t* dtuple_convert_big_rec(const Index *index, DTuple *entry, ulint *n_ext) {
  mem_heap_t *heap;
  big_rec_t *vector;
  ulint n_fields;
  ulint local_prefix_len;

  if (unlikely(!index->is_clustered())) {
    return (nullptr);
  }

  auto local_len = BTR_EXTERN_FIELD_REF_SIZE + DICT_MAX_INDEX_COL_LEN;

  ut_a(dtuple_check_typed_no_assert(entry));

  auto size = rec_get_converted_size(index, entry, *n_ext);

  if (unlikely(size > 1000000000)) {
    ib_logger(ib_stream, "Warning: tuple size very big: %lu\n", (ulong)size);
    ib_logger(ib_stream, "Tuple contents: ");
    dtuple_print(ib_stream, entry);
    ib_logger(ib_stream, "\n");
  }

  heap = mem_heap_create(size + dtuple_get_n_fields(entry) * sizeof(big_rec_field_t) + 1000);

  vector = (big_rec_t *)mem_heap_alloc(heap, sizeof(big_rec_t));

  vector->heap = heap;
  vector->fields = (big_rec_field_t *)mem_heap_alloc(heap, dtuple_get_n_fields(entry) * sizeof(big_rec_field_t));

  /* Decide which fields to shorten: the algorithm is to look for
  a variable-length field that yields the biggest savings when
  stored externally */

  n_fields = 0;

  while (page_rec_needs_ext(rec_get_converted_size(index, entry, *n_ext))) {
    ulint i;
    ulint longest = 0;
    ulint longest_i = ULINT_MAX;
    byte *data;
    big_rec_field_t *b;

    for (i = index->get_n_unique_in_tree(); i < dtuple_get_n_fields(entry); i++) {
      ulint savings;

      auto dfield = dtuple_get_nth_field(entry, i);
      auto ifield = index->get_nth_field(i);

      /* Skip fixed-length, nullptr, externally stored,
      or short columns */

      if (ifield->m_fixed_len || dfield_is_null(dfield) || dfield_is_ext(dfield) || dfield_get_len(dfield) <= local_len ||
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
      if (ifield->m_col->mtype != DATA_BLOB && ifield->m_col->len < 256) {
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

      return (nullptr);
    }

    /* Move data from field longest_i to big rec vector.

    We store the first bytes locally to the record. Then
    we can calculate all ordering fields in all indexes
    from locally stored data. */

    auto dfield = dtuple_get_nth_field(entry, longest_i);
    // auto ifield = index->get_nth_field(longest_i);
    local_prefix_len = local_len - BTR_EXTERN_FIELD_REF_SIZE;

    b = &vector->fields[n_fields];
    b->field_no = longest_i;
    b->len = dfield_get_len(dfield) - local_prefix_len;
    b->data = (char *)dfield_get_data(dfield) + local_prefix_len;

    /* Allocate the locally stored part of the column. */
    data = (byte *)mem_heap_alloc(heap, local_len);

    /* Copy the local prefix. */
    memcpy(data, dfield_get_data(dfield), local_prefix_len);
    /* Clear the extern field reference (BLOB pointer). */
    memset(data + local_prefix_len, 0, BTR_EXTERN_FIELD_REF_SIZE);

    dfield_set_data(dfield, data, local_len);
    dfield_set_ext(dfield);

    n_fields++;
    (*n_ext)++;
    ut_ad(n_fields < dtuple_get_n_fields(entry));
  }

  vector->n_fields = n_fields;
  return vector;
}

void dtuple_convert_back_big_rec(DTuple *entry, big_rec_t *vector) {
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

std::ostream &dfield_t::print(std::ostream &out) const {
  out << "dfield_t { data:" << (void *)data << ", ext=" << (ulint)ext << " ";

  if (dfield_is_ext(this)) {
    out << (static_cast<byte *>(data) + len - BTR_EXTERN_FIELD_REF_SIZE);
  }

  out << ", len=" << len << ", type=TBD" << "}";

  return out;
}

int DTuple::compare(const Rec &rec, const Index *, const ulint *offsets, ulint *matched_fields) const {
  ulint matched_bytes{};

  return cmp_dtuple_rec_with_match(nullptr, this, rec, offsets, matched_fields, &matched_bytes);
}
