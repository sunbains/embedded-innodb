/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file unit-tests/test_data.cc
Data types and tuples, row building, the query graph and the evaluator, and
sessions, driven directly:

  TypeTest     data0type: the main and precise types, lengths, fixed, min and
               max sizes, pad characters, the charset helpers, the stored
               order/null-size format.
  TupleTest    data0data: Dfield and DTuple create, copy, dup, sizes, fold,
               compare, NULLs, externally stored fields, the big record
               conversion and back.
  RowTest      row0row/row0ext on real records of tables of every column type
               in a shared instance: row_build (copy and pointers, NULLs,
               externally stored columns and their prefixes), index entries
               for clustered, secondary and prefix indexes, row references
               from secondary records, the trx id and roll pointer, fold and
               compare consistency of tuples with records, signed integer
               storage order.
  PrebuiltTest Prebuilt create/destroy/clear/update_trx and its row cache.
  SessTest     Session create/destroy and its link to the Trx.
  MiscTest     row0misc: the temporary file, the error strings, the handling
               of errors (statement and transaction rollback), table locks
               with waits, the statistics counter.
  QueTest      query graph forks and threads made by hand and parsed: states,
               lists, loops, que_eval_sql().
  EvalTest     the evaluator: arithmetic, comparisons (negative numbers),
               logic, functions, NULL, assignments and the control flow of a
               procedure, through que_eval_sql() and on parsed nodes.
  DataPerf     fold, compare, row_build, index entries, the evaluator. */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "eval0proc.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "pars0pars.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0ext.h"
#include "row0merge.h"
#include "row0misc.h"
#include "row0prebuilt.h"
#include "row0row.h"
#include "row0sel.h"
#include "trx0roll.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "usr0sess.h"

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstring>
#include <ctime>
#include <format>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {

/** A heap for the duration of a test. */
struct Heap {
  Heap(ulint n = 1024) : m_heap(mem_heap_create(n)) {}
  ~Heap() { mem_heap_free(m_heap); }

  Heap(const Heap &) = delete;
  Heap &operator=(const Heap &) = delete;

  operator MemHeap *() const { return m_heap; }

  MemHeap *m_heap;
};

/** @return the bytes of a field, nullopt for SQL NULL. */
std::optional<std::string> bytes(const Dfield *f) {
  if (dfield_is_null(f)) {
    return std::nullopt;
  }

  return std::string(static_cast<const char *>(dfield_get_data(f)), dfield_get_len(f));
}

/** @return field n of rec, nullopt for SQL NULL. */
std::optional<std::string> bytes(const Rec rec, const RecOffs &offs, ulint n) {
  ulint len;
  auto f = rec.get_nth_field(offs, n, &len);

  if (len == UNIV_SQL_NULL) {
    return std::nullopt;
  }

  return std::string(reinterpret_cast<const char *>(f.get()), len);
}

/** @return a deep copy of t in heap. */
DTuple *dup(const DTuple *t, MemHeap *heap) {
  auto c = dtuple_copy(t, heap);

  for (ulint i{}; i < dtuple_get_n_fields(c); ++i) {
    dfield_dup(dtuple_get_nth_field(c, i), heap);
  }

  dtuple_set_n_fields_cmp(c, dtuple_get_n_fields_cmp(t));
  dtuple_set_info_bits(c, dtuple_get_info_bits(t));

  return c;
}

/** Set field i of t to (mtype, prtype, len) and to the value v (nullopt: NULL). */
void set(MemHeap *heap, DTuple *t, ulint i, ulint mtype, ulint prtype, ulint len, std::optional<std::string> v) {
  auto f = dtuple_get_nth_field(t, i);

  dtype_set(dfield_get_type(f), mtype, prtype, len);

  if (!v.has_value()) {
    dfield_set_null(f);
  } else {
    auto p = mem_heap_alloc(heap, std::max<size_t>(v->size(), 1));

    memcpy(p, v->data(), v->size());
    dfield_set_data(f, p, v->size());
  }
}

/** @return v as a big endian 4 byte unsigned. */
std::string be4(uint32_t v) {
  std::string s(4, '\0');

  mach_write_to_4(reinterpret_cast<byte *>(s.data()), v);

  return s;
}

/** @return v as stored in a signed 4 byte INT column: the sign bit flipped. */
std::string s4(int32_t v) {
  return be4(uint32_t(v) ^ 0x80000000u);
}

/* --- data0type ------------------------------------------------------------ */

using TypeTest = ::testing::Test;

/** Every main type is or is not a string, binary or not. A BLOB is a string;
it is binary only with DATA_BINARY_TYPE. */
TEST_F(TypeTest, StringKinds) {
  struct K {
    ulint m_mtype;
    bool m_string;
    bool m_binary;
  };

  for (const auto &k : std::vector<K>{
         {DATA_VARCHAR, true, false},
         {DATA_CHAR, true, false},
         {DATA_FIXBINARY, true, true},
         {DATA_BINARY, true, true},
         {DATA_BLOB, true, false},
         {DATA_INT, false, false},
         {DATA_SYS, false, false},
         {DATA_FLOAT, false, false},
         {DATA_DOUBLE, false, false},
         {DATA_DECIMAL, false, false},
         {DATA_VARCLIENT, true, false},
         {DATA_CLIENT, true, false},
       }) {
    EXPECT_EQ(dtype_is_string_type(k.m_mtype), k.m_string) << k.m_mtype;
    EXPECT_EQ(dtype_is_binary_string_type(k.m_mtype, 0), k.m_binary) << k.m_mtype;
    EXPECT_EQ(dtype_is_non_binary_string_type(k.m_mtype, 0), k.m_string && !k.m_binary) << k.m_mtype;
  }

  EXPECT_TRUE(dtype_is_binary_string_type(DATA_BLOB, DATA_BINARY_TYPE));
  EXPECT_FALSE(dtype_is_non_binary_string_type(DATA_BLOB, DATA_BINARY_TYPE));
  /* DATA_BINARY_TYPE makes only a BLOB binary. */
  EXPECT_FALSE(dtype_is_binary_string_type(DATA_VARCHAR, DATA_BINARY_TYPE));
}

/** dtype_set() records the type; the multi-byte lengths are 0 (no charsets);
dtype_copy() copies all of it; the attrib and charset are bit fields of prtype. */
TEST_F(TypeTest, SetCopyFields) {
  Dtype t{};

  dtype_set(&t, DATA_INT, DATA_UNSIGNED | DATA_NOT_NULL, 8);

  EXPECT_EQ(dtype_get_mtype(&t), DATA_INT);
  EXPECT_EQ(dtype_get_prtype(&t), DATA_UNSIGNED | DATA_NOT_NULL);
  EXPECT_EQ(dtype_get_len(&t), 8UL);
  EXPECT_EQ(dtype_get_mbminlen(&t), 0UL);
  EXPECT_EQ(dtype_get_mbmaxlen(&t), 0UL);
  EXPECT_TRUE(dtype_validate(&t));

  Dtype u{};

  dtype_copy(&u, &t);
  EXPECT_EQ(u.mtype, t.mtype);
  EXPECT_EQ(u.prtype, t.prtype);
  EXPECT_EQ(u.len, t.len);

  /* The user type in the low byte, the charset in the third. */
  const auto prtype = dtype_form_prtype(0x1234, 0x56);

  EXPECT_EQ(prtype, 0x561234UL);
  EXPECT_EQ(dtype_get_charset_coll(prtype), 0x56UL);

  dtype_set(&t, DATA_VARCHAR, prtype, 100);
  EXPECT_EQ(dtype_get_attrib(&t), 0x34UL);

  /* The largest length that fits the 16 bit field. */
  dtype_set(&t, DATA_BLOB, 0, 65535);
  EXPECT_EQ(dtype_get_len(&t), 65535UL);
}

/** The pad character: space for the character types, for a binary that is
not of the binary charset, and for a non-binary BLOB (TEXT); none for INT,
a binary BLOB, a binary of the binary charset, FLOAT, ... */
TEST_F(TypeTest, PadChar) {
  constexpr auto NONE = ULINT_UNDEFINED;
  const auto BIN_CS = dtype_form_prtype(0, DATA_CLIENT_BINARY_CHARSET_COLL);

  EXPECT_EQ(dtype_get_pad_char(DATA_VARCHAR, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_CHAR, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_CLIENT, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_VARCLIENT, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_FIXBINARY, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_BINARY, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_FIXBINARY, BIN_CS), NONE);
  EXPECT_EQ(dtype_get_pad_char(DATA_BINARY, BIN_CS), NONE);
  EXPECT_EQ(dtype_get_pad_char(DATA_BLOB, 0), 0x20UL);
  EXPECT_EQ(dtype_get_pad_char(DATA_BLOB, DATA_BINARY_TYPE), NONE);

  for (auto mtype : {DATA_INT, DATA_SYS, DATA_FLOAT, DATA_DOUBLE, DATA_DECIMAL}) {
    EXPECT_EQ(dtype_get_pad_char(mtype, 0), NONE) << mtype;
  }
}

/** The fixed, minimum and maximum sizes of each main type. */
TEST_F(TypeTest, Sizes) {
  struct S {
    ulint m_mtype;
    ulint m_prtype;
    ulint m_len;
    ulint m_fixed;
    ulint m_min;
    ulint m_max;
  };

  for (const auto &s : std::vector<S>{
         {DATA_INT, 0, 1, 1, 1, 1},
         {DATA_INT, DATA_UNSIGNED, 8, 8, 8, 8},
         {DATA_CHAR, 0, 10, 10, 10, 10},
         {DATA_FIXBINARY, 0, 16, 16, 16, 16},
         {DATA_FLOAT, 0, 4, 4, 4, 4},
         {DATA_DOUBLE, 0, 8, 8, 8, 8},
         {DATA_SYS, DATA_ROW_ID, DATA_ROW_ID_LEN, 6, 6, 6},
         {DATA_SYS, DATA_TRX_ID, DATA_TRX_ID_LEN, 8, 8, 8},
         {DATA_SYS, DATA_ROLL_PTR, DATA_ROLL_PTR_LEN, 7, 7, 7},
         {DATA_VARCHAR, 0, 100, 0, 0, 100},
         {DATA_BINARY, 0, 100, 0, 0, 100},
         {DATA_DECIMAL, 0, 20, 0, 0, 20},
         {DATA_VARCLIENT, 0, 30, 0, 0, 30},
         {DATA_BLOB, 0, 0, 0, 0, ULINT_MAX},
         /* A client type of a fixed width charset (here no charset). */
         {DATA_CLIENT, 0, 12, 12, 12, 12},
         {DATA_CLIENT, DATA_BINARY_TYPE, 12, 12, 12, 12},
       }) {
    EXPECT_EQ(dtype_get_fixed_size_low(s.m_mtype, s.m_prtype, s.m_len, 0, 0), s.m_fixed) << s.m_mtype << " " << s.m_len;
    EXPECT_EQ(dtype_get_min_size_low(s.m_mtype, s.m_prtype, s.m_len, 0, 0), s.m_min) << s.m_mtype << " " << s.m_len;
    EXPECT_EQ(dtype_get_max_size_low(s.m_mtype, s.m_len), s.m_max) << s.m_mtype << " " << s.m_len;

    Dtype t{};

    dtype_set(&t, s.m_mtype, s.m_prtype, s.m_len);
    EXPECT_EQ(dtype_get_sql_null_size(&t), s.m_fixed) << s.m_mtype;
  }

  /* A variable width charset: the minimum is len * mbminlen / mbmaxlen. */
  EXPECT_EQ(dtype_get_fixed_size_low(DATA_CLIENT, 0, 30, 1, 3), 0UL);
  EXPECT_EQ(dtype_get_min_size_low(DATA_CLIENT, 0, 30, 1, 3), 10UL);
}

/** The prefix of at most n characters of a single byte charset: n bytes, or
the whole string when shorter, including 0 and n = 0. */
TEST_F(TypeTest, AtMostNChars) {
  const char *s = "abcdefgh";

  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 0, 0, 3, 8, s), 3UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 0, 0, 8, 8, s), 8UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 0, 0, 100, 8, s), 8UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 0, 0, 0, 8, s), 0UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 0, 0, 5, 0, s), 0UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 1, 1, 767, 20000, s), 767UL);
  EXPECT_EQ(dtype_get_at_most_n_mbchars(0, 1, 1, 768, 768, s), 768UL);
}

/** The >= 4.1 stored order/null-size format: 6 bytes, written then read
back. The read keeps mtype, len, DATA_BINARY_TYPE and DATA_NOT_NULL of a
non-string type; for a string type it sets prtype to 1 (an upstream hack,
dtype_new_read_for_order_and_null_size()), dropping the flags. */
TEST_F(TypeTest, StoreReadOrderNullSize) {
  std::array<byte, DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE> buf{};
  Dtype t{};
  Dtype r{};

  dtype_set(&t, DATA_INT, DATA_NOT_NULL | DATA_UNSIGNED | 3, 4);
  dtype_new_store_for_order_and_null_size(buf.data(), &t, 0);

  EXPECT_EQ(buf[0], DATA_INT);
  EXPECT_EQ(buf[1], 3);
  EXPECT_EQ(mach_read_from_2(buf.data() + 2), 4UL);
  EXPECT_EQ(buf[4] & 0x80, 0x80);

  dtype_new_read_for_order_and_null_size(&r, buf.data());
  EXPECT_EQ(r.mtype, DATA_INT);
  EXPECT_EQ(r.len, 4U);
  EXPECT_EQ(r.prtype & DATA_NOT_NULL, DATA_NOT_NULL);
  /* DATA_UNSIGNED is not part of the stored format. */
  EXPECT_EQ(r.prtype & DATA_UNSIGNED, 0UL);

  /* A prefix length replaces the length. */
  dtype_set(&t, DATA_BLOB, DATA_BINARY_TYPE, 0);
  dtype_new_store_for_order_and_null_size(buf.data(), &t, 767);
  EXPECT_EQ(buf[0], DATA_BLOB | 128);
  EXPECT_EQ(mach_read_from_2(buf.data() + 2), 767UL);
  EXPECT_EQ(buf[4] & 0x80, 0);

  dtype_new_read_for_order_and_null_size(&r, buf.data());
  EXPECT_EQ(r.mtype, DATA_BLOB);
  EXPECT_EQ(r.len, 767U);
  EXPECT_EQ(r.prtype, 1U);

  /* The < 4.1 format: 4 bytes, the flags in the first. */
  std::array<byte, DATA_ORDER_NULL_TYPE_BUF_SIZE> old{DATA_VARCHAR | 128, 5, 0, 77};

  dtype_read_for_order_and_null_size(&r, old.data());
  EXPECT_EQ(r.mtype, DATA_VARCHAR);
  EXPECT_EQ(r.len, 77U);
  EXPECT_EQ(r.prtype, 5 | DATA_BINARY_TYPE);
}

/* --- data0data ------------------------------------------------------------ */

using TupleTest = ::testing::Test;

/** A new tuple: n fields, all compared, no info bits; dtuple_set_n_fields()
sets both counts; dtuple_set_n_fields_cmp() only the compared. A tuple of
0 fields is valid. */
TEST_F(TupleTest, Create) {
  Heap heap;

  for (ulint n : {0UL, 1UL, 7UL, 300UL}) {
    auto t = dtuple_create(heap, n);

    EXPECT_EQ(dtuple_get_n_fields(t), n);
    EXPECT_EQ(dtuple_get_n_fields_cmp(t), n);
    EXPECT_EQ(dtuple_get_info_bits(t), 0UL);
    EXPECT_EQ(t->get_n_ext(), 0UL);
  }

  auto t = dtuple_create(heap, 5);

  dtuple_set_n_fields_cmp(t, 2);
  EXPECT_EQ(dtuple_get_n_fields(t), 5UL);
  EXPECT_EQ(dtuple_get_n_fields_cmp(t), 2UL);

  dtuple_set_n_fields(t, 3);
  EXPECT_EQ(dtuple_get_n_fields(t), 3UL);
  EXPECT_EQ(dtuple_get_n_fields_cmp(t), 3UL);

  dtuple_set_info_bits(t, REC_INFO_DELETED_FLAG);
  EXPECT_EQ(dtuple_get_info_bits(t), REC_INFO_DELETED_FLAG);
}

/** dfield_set_data/len/null/ext: setting data or a length clears the
external flag; NULL has len UNIV_SQL_NULL; a zero length value is not NULL. */
TEST_F(TupleTest, FieldSetters) {
  Dfield f{};
  const char *s = "abc";

  dfield_set_data(&f, s, 3);
  EXPECT_EQ(dfield_get_data(&f), s);
  EXPECT_EQ(dfield_get_len(&f), 3UL);
  EXPECT_FALSE(dfield_is_null(&f));
  EXPECT_FALSE(dfield_is_ext(&f));

  dfield_set_ext(&f);
  EXPECT_TRUE(dfield_is_ext(&f));

  dfield_set_len(&f, 2);
  EXPECT_FALSE(dfield_is_ext(&f));
  EXPECT_EQ(dfield_get_len(&f), 2UL);

  dfield_set_ext(&f);
  dfield_set_data(&f, s, 1);
  EXPECT_FALSE(dfield_is_ext(&f));

  dfield_set_data(&f, s, 0);
  EXPECT_FALSE(dfield_is_null(&f));
  EXPECT_EQ(dfield_get_len(&f), 0UL);

  dfield_set_null(&f);
  EXPECT_TRUE(dfield_is_null(&f));
  EXPECT_EQ(dfield_get_len(&f), UNIV_SQL_NULL);
  EXPECT_FALSE(dfield_is_ext(&f));

  Dtype t{};

  dtype_set(&t, DATA_BINARY, 0, 9);
  dfield_set_type(&f, &t);
  EXPECT_EQ(dfield_get_type(&f)->mtype, DATA_BINARY);
  EXPECT_EQ(dfield_get_type(&f)->len, 9U);
}

/** dfield_copy() copies type and value (a shallow copy), dfield_copy_data()
only the value and the external flag; dfield_dup() copies the data into the
heap and leaves NULL alone. */
TEST_F(TupleTest, FieldCopyDup) {
  Heap heap;
  std::string s = "value";
  Dfield a{};
  Dfield b{};

  dtype_set(dfield_get_type(&a), DATA_VARCHAR, 0, 10);
  dfield_set_data(&a, s.data(), s.size());
  dfield_set_ext(&a);

  dfield_copy(&b, &a);
  EXPECT_EQ(dfield_get_data(&b), s.data());
  EXPECT_EQ(dfield_get_len(&b), 5UL);
  EXPECT_TRUE(dfield_is_ext(&b));
  EXPECT_EQ(dfield_get_type(&b)->mtype, DATA_VARCHAR);

  Dfield c{};

  dtype_set(dfield_get_type(&c), DATA_INT, 0, 4);
  dfield_copy_data(&c, &a);
  EXPECT_EQ(dfield_get_data(&c), s.data());
  EXPECT_TRUE(dfield_is_ext(&c));
  EXPECT_EQ(dfield_get_type(&c)->mtype, DATA_INT);

  dfield_dup(&b, heap);
  EXPECT_NE(dfield_get_data(&b), s.data());
  s = "XXXXX";
  EXPECT_EQ(bytes(&b), "value");
  EXPECT_TRUE(dfield_is_ext(&b));

  dfield_set_null(&c);
  dfield_dup(&c, heap);
  EXPECT_TRUE(dfield_is_null(&c));

  /* A zero length value. */
  dfield_set_data(&c, s.data(), 0);
  dfield_dup(&c, heap);
  EXPECT_EQ(dfield_get_len(&c), 0UL);
}

/** Binary equality: lengths and bytes; NULL equals NULL only; a zero length
value is not NULL. */
TEST_F(TupleTest, BinaryEqual) {
  Dfield a{};
  Dfield b{};

  dfield_set_data(&a, "abc", 3);
  dfield_set_data(&b, "abc", 3);
  EXPECT_TRUE(dfield_datas_are_binary_equal(&a, &b));
  EXPECT_TRUE(dfield_data_is_binary_equal(&a, 3, reinterpret_cast<const byte *>("abc")));
  EXPECT_FALSE(dfield_data_is_binary_equal(&a, 3, reinterpret_cast<const byte *>("abd")));
  EXPECT_FALSE(dfield_data_is_binary_equal(&a, 2, reinterpret_cast<const byte *>("ab")));

  dfield_set_data(&b, "abc ", 4);
  EXPECT_FALSE(dfield_datas_are_binary_equal(&a, &b));

  dfield_set_null(&a);
  EXPECT_FALSE(dfield_datas_are_binary_equal(&a, &b));
  EXPECT_TRUE(dfield_data_is_binary_equal(&a, UNIV_SQL_NULL, nullptr));

  dfield_set_null(&b);
  EXPECT_TRUE(dfield_datas_are_binary_equal(&a, &b));

  dfield_set_data(&b, "", 0);
  EXPECT_FALSE(dfield_datas_are_binary_equal(&a, &b));
  EXPECT_FALSE(dfield_data_is_binary_equal(&b, UNIV_SQL_NULL, nullptr));
  EXPECT_TRUE(dfield_data_is_binary_equal(&b, 0, nullptr));
}

/** dtuple_copy() is a shallow copy of every field with the counts; the info
bits are not copied. */
TEST_F(TupleTest, Copy) {
  Heap heap;
  auto t = dtuple_create(heap, 3);

  set(heap, t, 0, DATA_INT, DATA_UNSIGNED, 4, be4(7));
  set(heap, t, 1, DATA_VARCHAR, 0, 20, std::nullopt);
  set(heap, t, 2, DATA_BLOB, 0, 0, std::string(""));
  dfield_set_ext(dtuple_get_nth_field(t, 2));
  dtuple_set_info_bits(t, 3);

  auto c = dtuple_copy(t, heap);

  ASSERT_EQ(dtuple_get_n_fields(c), 3UL);
  EXPECT_EQ(dtuple_get_info_bits(c), 0UL);

  for (ulint i{}; i < 3; ++i) {
    auto a = dtuple_get_nth_field(t, i);
    auto b = dtuple_get_nth_field(c, i);

    EXPECT_EQ(dfield_get_data(a), dfield_get_data(b)) << i;
    EXPECT_EQ(dfield_get_len(a), dfield_get_len(b)) << i;
    EXPECT_EQ(dfield_is_ext(a), dfield_is_ext(b)) << i;
    EXPECT_EQ(dfield_get_type(a)->mtype, dfield_get_type(b)->mtype) << i;
    EXPECT_EQ(dfield_get_type(a)->prtype, dfield_get_type(b)->prtype) << i;
  }

  EXPECT_EQ(c->get_n_ext(), 1UL);
  EXPECT_TRUE(c->has_ext());
  EXPECT_EQ(dtuple_get_n_ext(c), 1UL);
}

/** The data size: the sum of the lengths, a NULL counts its SQL NULL size
(the fixed size of the type, 0 if variable). */
TEST_F(TupleTest, DataSize) {
  Heap heap;
  auto t = dtuple_create(heap, 5);

  set(heap, t, 0, DATA_INT, 0, 4, s4(1));
  set(heap, t, 1, DATA_INT, 0, 8, std::nullopt);
  set(heap, t, 2, DATA_VARCHAR, 0, 30, std::nullopt);
  set(heap, t, 3, DATA_CHAR, 0, 10, std::nullopt);
  set(heap, t, 4, DATA_VARCHAR, 0, 30, std::string(17, 'x'));

  EXPECT_EQ(dtuple_get_data_size(t), 4UL + 8 + 0 + 10 + 17);

  auto e = dtuple_create(heap, 0);

  EXPECT_EQ(dtuple_get_data_size(e), 0UL);
}

/** dtuple_contains_null(); dtuple_set_types_binary() makes the first n
fields untyped binary strings. */
TEST_F(TupleTest, NullsAndBinaryTypes) {
  Heap heap;
  auto t = dtuple_create(heap, 3);

  set(heap, t, 0, DATA_INT, 0, 4, s4(1));
  set(heap, t, 1, DATA_VARCHAR, 0, 30, std::string(""));
  set(heap, t, 2, DATA_CHAR, 0, 10, std::string("x"));

  EXPECT_FALSE(dtuple_contains_null(t));

  dfield_set_null(dtuple_get_nth_field(t, 2));
  EXPECT_TRUE(dtuple_contains_null(t));

  dtuple_set_types_binary(t, 2);

  for (ulint i{}; i < 2; ++i) {
    auto type = dfield_get_type(dtuple_get_nth_field(t, i));

    EXPECT_EQ(type->mtype, DATA_BINARY) << i;
    EXPECT_EQ(type->prtype, 0U) << i;
    EXPECT_EQ(type->len, 0U) << i;
  }

  EXPECT_EQ(dfield_get_type(dtuple_get_nth_field(t, 2))->mtype, DATA_CHAR);
}

/** The fold of a tuple: the tree id, then each of the first n fields, NULLs
skipped, then n_bytes of the next field. It depends on the tree id, the
values and the number of fields; a prefix of more bytes than the field folds
the field; equal values fold equal. */
TEST_F(TupleTest, Fold) {
  Heap heap;
  auto t = dtuple_create(heap, 3);
  auto u = dtuple_create(heap, 3);

  for (auto x : {t, u}) {
    set(heap, x, 0, DATA_INT, DATA_UNSIGNED, 4, be4(42));
    set(heap, x, 1, DATA_VARCHAR, 0, 30, std::string("hello"));
    set(heap, x, 2, DATA_BINARY, 0, 30, std::nullopt);
  }

  for (ulint n = 0; n <= 3; ++n) {
    EXPECT_EQ(dtuple_fold(t, n, 0, 1), dtuple_fold(u, n, 0, 1)) << n;
  }

  EXPECT_NE(dtuple_fold(t, 1, 0, 1), dtuple_fold(t, 1, 0, 2));
  EXPECT_NE(dtuple_fold(t, 1, 0, 1), dtuple_fold(t, 2, 0, 1));
  /* A NULL field is skipped. */
  EXPECT_EQ(dtuple_fold(t, 2, 0, 1), dtuple_fold(t, 3, 0, 1));
  EXPECT_EQ(dtuple_fold(t, 2, 0, 1), dtuple_fold(t, 2, 7, 1));
  /* A byte prefix of field 1. */
  EXPECT_NE(dtuple_fold(t, 1, 2, 1), dtuple_fold(t, 1, 0, 1));
  EXPECT_EQ(dtuple_fold(t, 1, 5, 1), dtuple_fold(t, 2, 0, 1));
  EXPECT_EQ(dtuple_fold(t, 1, 500, 1), dtuple_fold(t, 2, 0, 1));
  EXPECT_EQ(dtuple_fold(t, 0, 0, 9), ut_uint64_fold(9));

  set(heap, u, 1, DATA_VARCHAR, 0, 30, std::string("hellp"));
  EXPECT_NE(dtuple_fold(t, 2, 0, 1), dtuple_fold(u, 2, 0, 1));
  /* The common prefix folds equal. */
  EXPECT_EQ(dtuple_fold(t, 1, 4, 1), dtuple_fold(u, 1, 4, 1));
}

/** dtuple_coll_cmp(): fewer fields is smaller; then field by field, with the
order of the type: NULL is the smallest, VARCHAR pads with spaces, a binary
BLOB does not, unsigned INT is big endian. */
TEST_F(TupleTest, CollCmp) {
  Heap heap;

  auto mk = [&](std::vector<std::tuple<ulint, ulint, std::optional<std::string>>> fs) {
    auto t = dtuple_create(heap, fs.size());

    for (ulint i{}; i < fs.size(); ++i) {
      set(heap, t, i, std::get<0>(fs[i]), std::get<1>(fs[i]), 30, std::get<2>(fs[i]));
    }

    return t;
  };

  auto a = mk({{DATA_INT, DATA_UNSIGNED, be4(1)}});
  auto b = mk({{DATA_INT, DATA_UNSIGNED, be4(2)}});
  auto ab = mk({{DATA_INT, DATA_UNSIGNED, be4(1)}, {DATA_VARCHAR, 0, std::string("x")}});

  EXPECT_LT(dtuple_coll_cmp(nullptr, a, b), 0);
  EXPECT_GT(dtuple_coll_cmp(nullptr, b, a), 0);
  EXPECT_EQ(dtuple_coll_cmp(nullptr, a, a), 0);
  /* The field count decides first. */
  EXPECT_LT(dtuple_coll_cmp(nullptr, b, ab), 0);
  EXPECT_GT(dtuple_coll_cmp(nullptr, ab, b), 0);

  auto n = mk({{DATA_VARCHAR, 0, std::nullopt}});
  auto e = mk({{DATA_VARCHAR, 0, std::string("")}});
  auto sp = mk({{DATA_VARCHAR, 0, std::string("  ")}});
  auto x = mk({{DATA_VARCHAR, 0, std::string("x")}});
  auto xs = mk({{DATA_VARCHAR, 0, std::string("x   ")}});

  EXPECT_LT(dtuple_coll_cmp(nullptr, n, e), 0);
  EXPECT_EQ(dtuple_coll_cmp(nullptr, n, n), 0);
  EXPECT_EQ(dtuple_coll_cmp(nullptr, e, sp), 0);
  EXPECT_EQ(dtuple_coll_cmp(nullptr, x, xs), 0);
  EXPECT_LT(dtuple_coll_cmp(nullptr, e, x), 0);

  auto be = mk({{DATA_BLOB, DATA_BINARY_TYPE, std::string("x")}});
  auto bes = mk({{DATA_BLOB, DATA_BINARY_TYPE, std::string("x ")}});

  EXPECT_LT(dtuple_coll_cmp(nullptr, be, bes), 0);

  /* Signed INT order needs the stored form: the sign bit flipped. */
  auto m1 = mk({{DATA_INT, 0, s4(-1)}});
  auto z = mk({{DATA_INT, 0, s4(0)}});
  auto mn = mk({{DATA_INT, 0, s4(INT32_MIN)}});
  auto mx = mk({{DATA_INT, 0, s4(INT32_MAX)}});

  EXPECT_LT(dtuple_coll_cmp(nullptr, m1, z), 0);
  EXPECT_LT(dtuple_coll_cmp(nullptr, mn, m1), 0);
  EXPECT_LT(dtuple_coll_cmp(nullptr, z, mx), 0);
}

/** The typed checks accept every main type from VARCHAR to CLIENT and reject
the others; dtuple_validate() reads each byte of the data. */
TEST_F(TupleTest, CheckTyped) {
  Heap heap;
  auto t = dtuple_create(heap, 2);

  set(heap, t, 0, DATA_VARCHAR, 0, 10, std::string("ab"));
  set(heap, t, 1, DATA_CLIENT, 0, 10, std::nullopt);

  IF_DEBUG(EXPECT_TRUE(dtuple_check_typed(t));)
  EXPECT_TRUE(dtuple_check_typed_no_assert(t));
  IF_DEBUG(EXPECT_TRUE(dtuple_validate(t));)

  auto too_many = dtuple_create(heap, REC_MAX_N_FIELDS + 1);

  for (ulint i{}; i < dtuple_get_n_fields(too_many); ++i) {
    set(heap, too_many, i, DATA_BINARY, 0, 1, std::nullopt);
  }

  EXPECT_FALSE(dtuple_check_typed_no_assert(too_many));
}

/** dtuple_check_typed_no_assert() reports a field of an invalid type.
BUG: it asserts instead (debug build): on the error it prints the tuple with
dtuple_print() (data/data0data.cc:436) which ends in ut_ad(dtuple_validate())
-> dtuple_check_typed() -> ut_error in dfield_check_typed() (data0data.cc:158).
The same in upstream 7c21eac0. Fix: no validation in dtuple_print(). */
TEST_F(TupleTest, CheckTypedNoAssert) {
  Heap heap;
  auto t = dtuple_create(heap, 2);

  set(heap, t, 0, DATA_VARCHAR, 0, 10, std::string("ab"));
  set(heap, t, 1, DATA_CLIENT, 0, 10, std::nullopt);

  dfield_get_type(dtuple_get_nth_field(t, 1))->mtype = DATA_ERROR;
  EXPECT_FALSE(dtuple_check_typed_no_assert(t));
}

/** A tuple prints its fields, NULLs and the length of long values. */
TEST_F(TupleTest, Print) {
  Heap heap;
  auto t = dtuple_create(heap, 3);

  set(heap, t, 0, DATA_BINARY, 0, 0, std::string("AB"));
  set(heap, t, 1, DATA_BINARY, 0, 0, std::nullopt);
  set(heap, t, 2, DATA_BLOB, 0, 0, std::string(2000, 'z'));

  std::ostringstream os;

  t->print(os);

  const auto s = os.str();

  EXPECT_NE(s.find("3 fields"), std::string::npos) << s;
  EXPECT_NE(s.find("SQL nullptr"), std::string::npos) << s;
  EXPECT_NE(s.find("total 2000"), std::string::npos) << s;
}

/** Dfield::print() of an external field shows its 20 byte reference.
BUG: data/data0data.cc:604 Dfield::print() writes the reference with
operator<<(const byte *): as a NUL terminated string, read past the 20
bytes and the field up to a NUL somewhere (here the field has none). Fix:
print the 20 bytes in hex (buf_to_hex_string()). */
TEST_F(TupleTest, PrintExternal) {
  std::vector<byte> buf(REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE, 'a');
  Dfield f{};

  dfield_set_data(&f, buf.data(), buf.size());
  dfield_set_ext(&f);

  std::ostringstream os;

  f.print(os);
  EXPECT_EQ(os.str().find(std::string(BTR_EXTERN_FIELD_REF_SIZE, 'a')), std::string::npos) << os.str();
}

}  // namespace

/* --- Shared instance: tables of every column type ------------------------ */

namespace {

/** A column of a table to create. */
struct ColDef {
  const char *m_name;
  ColType m_type;
  ColAttr m_attr;
  ulint m_len;
};

/** An index of a table to create: columns with their prefix lengths. */
struct IdxDef {
  const char *m_name;
  std::vector<std::pair<const char *, ulint>> m_cols;
  bool m_clustered{};
  bool m_unique{};
};

/** Create table name in sys. */
[[nodiscard]] DbErr create_table(System &sys, const std::string &name, const std::vector<ColDef> &cols, const std::vector<IdxDef> &idxs) {
  (void)api::database_create(sys, "test");

  api::TableSchema *tbl_sch{};

  if (auto err = api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1, 0); err != DB_SUCCESS) {
    return err;
  }

  auto err = DB_SUCCESS;

  for (const auto &c : cols) {
    if (err == DB_SUCCESS) {
      err = tbl_sch->add_col(c.m_name, c.m_type, c.m_attr, 0, c.m_len);
    }
  }

  for (const auto &i : idxs) {
    api::IndexSchema *idx_sch{};

    if (err == DB_SUCCESS) {
      err = tbl_sch->add_index(i.m_name, &idx_sch);
    }

    for (const auto &[col, prefix] : i.m_cols) {
      if (err == DB_SUCCESS) {
        err = idx_sch->add_col(col, prefix);
      }
    }

    if (err == DB_SUCCESS && i.m_clustered) {
      err = idx_sch->set_clustered();
    }

    if (err == DB_SUCCESS && i.m_unique) {
      err = idx_sch->set_unique();
    }
  }

  if (err == DB_SUCCESS) {
    auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
    uint64_t id{};

    err = api::schema_lock_exclusive(sys, trx);

    if (err == DB_SUCCESS) {
      err = api::table_create(sys, trx, tbl_sch, &id);
    }

    (void)api::trx_commit(sys, trx);
  }

  api::TableSchema::destroy(sys, tbl_sch);

  return err;
}

/** @return the table from the dictionary cache. */
Table *get_table(System &sys, const std::string &name) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(name.c_str());
  sys.m_dict->mutex_release();

  return table;
}

/** Fills a row tuple for an insert. */
using Fill = std::function<DbErr(api::Tuple *)>;

/** Insert a row into table name in trx. */
DbErr insert(System &sys, const std::string &name, Trx *trx, const Fill &fill) {
  api::Cursor *crsr{};

  if (auto err = api::cursor_open_table(sys, name.c_str(), trx, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->lock(sys, IB_LOCK_IX);
  auto tpl = crsr->clust_read_tuple_create();

  if (err == DB_SUCCESS) {
    err = fill(tpl);
  }

  if (err == DB_SUCCESS) {
    err = crsr->insert_row(sys, tpl);
  }

  api::Tuple::destroy(tpl);
  (void)api::Cursor::close(sys, crsr);

  return err;
}

/** Delete the row with the unsigned 4 byte key k (column 0, the PK). */
DbErr remove_u32(System &sys, const std::string &name, Trx *trx, uint32_t k) {
  api::Cursor *crsr{};

  if (auto err = api::cursor_open_table(sys, name.c_str(), trx, &crsr); err != DB_SUCCESS) {
    return err;
  }

  auto err = crsr->lock(sys, IB_LOCK_IX);

  if (err == DB_SUCCESS) {
    err = crsr->set_lock_mode(sys, IB_LOCK_X);
  }

  auto key = crsr->clust_search_tuple_create();
  int res{-1};

  if (err == DB_SUCCESS) {
    err = key->write_u32(0, k);
  }

  if (err == DB_SUCCESS) {
    err = crsr->moveto(sys, key, IB_CUR_GE, &res);
  }

  if (err == DB_SUCCESS && res != 0) {
    err = DB_RECORD_NOT_FOUND;
  }

  if (err == DB_SUCCESS) {
    err = crsr->delete_row(sys);
  }

  api::Tuple::destroy(key);
  (void)api::Cursor::close(sys, crsr);

  return err;
}

/** Call fn(rec, offsets) for each user record of index in order, the leaf
page S-latched by one mini-transaction for the duration of fn. */
void for_each_rec(System &sys, const Index *index, const std::function<void(const Rec, const RecOffs &)> &fn) {
  Mtr mtr;
  BtreePcursor pcur;
  MemHeap *heap{};

  mtr.start();
  pcur.open_at_index_side(sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

  while (pcur.move_to_next_user_rec(sys, &mtr)) {
    auto rec = pcur.get_rec();
    RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());

    fn(rec, offs);
  }

  pcur.close();
  mtr.commit(sys);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }
}

/** @return the number of user records in index. */
size_t n_recs(System &sys, const Index *index) {
  size_t n{};

  for_each_rec(sys, index, [&](const Rec, const RecOffs &) { ++n; });

  return n;
}

/** One started System for all the suites of the program that need one,
started by the first test that needs it, shut down at the end of the
program. Each test makes tables of its own, named after it. */
class SharedDb : public ::testing::Test {
 public:
  /** Shuts the shared instance down at the end of the program. */
  struct Env : ::testing::Environment {
    void TearDown() override {
      if (s_sys != nullptr) {
        EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
        s_sys.reset();
      }

      s_dir.reset();
    }
  };

 protected:
  static void SetUpTestSuite() {
    if (s_sys != nullptr) {
      return;
    }

    s_dir = std::make_unique<ut::TestDir>();
    s_config = ut::make_config(s_dir->m_path);
    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(s_config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
    ASSERT_TRUE(api::database_create(*s_sys, "test"));
  }

  void SetUp() override {
    ASSERT_NE(s_sys, nullptr);
    m_sys = s_sys.get();
    m_heap = mem_heap_create(1024);
  }

  void TearDown() override { mem_heap_free(m_heap); }

  /** @return the name of a table of the test. */
  static std::string name(const std::string &suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  [[nodiscard]] Trx *begin(TrxLevel level = IB_TRX_REPEATABLE_READ) {
    auto trx = api::trx_begin(*m_sys, level);

    EXPECT_NE(trx, nullptr);

    return trx;
  }

  void commit(Trx *trx) { EXPECT_EQ(api::trx_commit(*m_sys, trx), DB_SUCCESS); }
  void rollback(Trx *trx) { EXPECT_EQ(api::trx_rollback(*m_sys, trx), DB_SUCCESS); }

  /** Insert rows into table t in one committed transaction. */
  void load(const std::string &t, const std::vector<Fill> &rows) {
    auto trx = begin();

    for (size_t i{}; i < rows.size(); ++i) {
      ASSERT_EQ(insert(*m_sys, t, trx, rows[i]), DB_SUCCESS) << "row " << i;
    }

    commit(trx);
  }

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline Config s_config;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
  MemHeap *m_heap{};
};

[[maybe_unused]] const auto *s_env = ::testing::AddGlobalTestEnvironment(new SharedDb::Env);

/* --- row0row: rows and index entries ------------------------------------- */

/** The columns of the table of every type the API creates. */
const std::vector<ColDef> ALL_TYPES = {
  {"k", IB_INT, IB_COL_UNSIGNED, 4},
  {"i1", IB_INT, IB_COL_NONE, 1},
  {"u1", IB_INT, IB_COL_UNSIGNED, 1},
  {"i2", IB_INT, IB_COL_NONE, 2},
  {"u2", IB_INT, IB_COL_UNSIGNED, 2},
  {"i4", IB_INT, IB_COL_NONE, 4},
  {"i8", IB_INT, IB_COL_NONE, 8},
  {"u8", IB_INT, IB_COL_UNSIGNED, 8},
  {"vc", IB_VARCHAR, IB_COL_NONE, 10},
  {"ch", IB_CHAR, IB_COL_NONE, 10},
  {"fb", IB_BINARY, IB_COL_NONE, 4},
  {"vb", IB_VARBINARY, IB_COL_NONE, 10},
  {"bl", IB_BLOB, IB_COL_NONE, 0},
  {"fl", IB_FLOAT, IB_COL_NONE, 4},
  {"db", IB_DOUBLE, IB_COL_NONE, 8},
  {"dc", IB_DECIMAL, IB_COL_NONE, 12},
  {"nn", IB_INT, ColAttr(IB_COL_NOT_NULL | IB_COL_UNSIGNED), 4},
  {"vca", IB_VARCHAR_ANYCHARSET, IB_COL_NONE, 10},
  {"cha", IB_CHAR_ANYCHARSET, IB_COL_NONE, 10},
  {"cu", IB_INT, ColAttr(IB_COL_CUSTOM1 | IB_COL_CUSTOM3 | IB_COL_UNSIGNED), 4},
};

class RowTest : public SharedDb {
 protected:
  /** Create test/<test><suffix> with ALL_TYPES and secondary indexes on
  i4, (vc, i2), ch(3), bl(10). */
  std::string create_all(const std::string &suffix = "") {
    auto t = name(suffix);

    EXPECT_EQ(
      create_table(
        *m_sys,
        t,
        ALL_TYPES,
        {{"PRIMARY", {{"k", 0}}, true, true},
         {"S_I4", {{"i4", 0}}},
         {"S_VC_I2", {{"vc", 0}, {"i2", 0}}},
         {"S_CH3", {{"ch", 3}}},
         {"S_BL10", {{"bl", 10}}}}
      ),
      DB_SUCCESS
    );

    return t;
  }

  /** @return a fill of ALL_TYPES for key k: values derived from k, NULLs in the
  nullable columns if nulls. */
  static Fill all_row(uint32_t k, bool nulls, const std::string &blob = "") {
    return [=](api::Tuple *tpl) {
      auto err = tpl->write_u32(0, k);

      if (!nulls) {
        const int32_t s = int32_t(k) - 5;

        if (err == DB_SUCCESS) err = tpl->write_i8(1, int8_t(s));
        if (err == DB_SUCCESS) err = tpl->write_u8(2, uint8_t(k));
        if (err == DB_SUCCESS) err = tpl->write_i16(3, int16_t(s * 100));
        if (err == DB_SUCCESS) err = tpl->write_u16(4, uint16_t(k * 100));
        if (err == DB_SUCCESS) err = tpl->write_i32(5, s * 100000);
        if (err == DB_SUCCESS) err = tpl->write_i64(6, int64_t(s) * 10000000000LL);
        if (err == DB_SUCCESS) err = tpl->write_u64(7, uint64_t(k) * 10000000000ULL);

        const auto vc = std::format("v{}", k);

        if (err == DB_SUCCESS) err = tpl->set_value(8, vc.data(), vc.size());
        if (err == DB_SUCCESS) err = tpl->set_value(9, "chr", 3);
        if (err == DB_SUCCESS) err = tpl->set_value(10, "\x01\x02\x03\x04", 4);
        if (err == DB_SUCCESS) err = tpl->set_value(11, "vb\0x", 4);
        if (err == DB_SUCCESS) err = tpl->set_value(12, blob.data(), blob.size());
        if (err == DB_SUCCESS) err = tpl->write_float(13, 1.5f * float(s));
        if (err == DB_SUCCESS) err = tpl->write_double(14, 2.25 * s);
        if (err == DB_SUCCESS) err = tpl->set_value(15, "-123.45", 7);
        if (err == DB_SUCCESS) err = tpl->set_value(17, "any", 3);
        if (err == DB_SUCCESS) err = tpl->set_value(18, "anyc", 4);
        if (err == DB_SUCCESS) err = tpl->write_u32(19, k + 1);
      }

      if (err == DB_SUCCESS) {
        err = tpl->write_u32(16, k * 3);
      }

      return err;
    };
  }

  [[nodiscard]] Index *index(const std::string &t, const char *name = nullptr) {
    auto table = get_table(*m_sys, t);

    return name == nullptr ? table->get_first_index() : table->get_index_on_name(name);
  }
};

/** row_build() of every record, copying and by pointers: a tuple of all the
columns of the table (with the system columns) in column order, typed as the
columns, each value the bytes of its field in the record, the NULLs NULL, the
info bits of the record; nothing stored externally. The copy survives the
end of the mini-transaction. */
TEST_F(RowTest, BuildAllTypes) {
  const auto t = create_all();

  load(t, {all_row(1, false, "b"), all_row(2, true), all_row(3, false, "")});

  auto clust = index(t);
  auto table = clust->m_table;
  std::vector<std::vector<std::optional<std::string>>> copies;
  std::vector<DTuple *> rows;

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    for (auto type : {ROW_COPY_POINTERS, ROW_COPY_DATA}) {
      RowExt *ext{reinterpret_cast<RowExt *>(1)};
      auto row = row_build(*m_sys, type, clust, rec, offs, nullptr, &ext, m_heap);

      ASSERT_NE(row, nullptr);
      EXPECT_EQ(ext, nullptr);
      ASSERT_EQ(dtuple_get_n_fields(row), table->get_n_cols());
      EXPECT_EQ(dtuple_get_info_bits(row), rec.get_info_bits());

      for (ulint i{}; i < clust->get_n_fields(); ++i) {
        auto col = clust->get_nth_field(i)->m_col;
        auto f = dtuple_get_nth_field(row, col->get_no());

        EXPECT_EQ(dfield_get_type(f)->mtype, col->mtype) << i;
        EXPECT_EQ(dfield_get_type(f)->prtype, col->prtype) << i;
        EXPECT_EQ(dfield_get_type(f)->len, col->len) << i;
        EXPECT_EQ(bytes(f), bytes(rec, offs, i)) << "field " << i << " type " << type;
        EXPECT_FALSE(dfield_is_ext(f));

        /* By pointers: into the record itself. */
        if (type == ROW_COPY_POINTERS && !dfield_is_null(f)) {
          ulint len;

          EXPECT_EQ(dfield_get_data(f), rec.get_nth_field(offs, i, &len).get()) << i;
        }
      }

      if (type == ROW_COPY_DATA) {
        std::vector<std::optional<std::string>> v;

        for (ulint i{}; i < dtuple_get_n_fields(row); ++i) {
          v.push_back(bytes(dtuple_get_nth_field(row, i)));
        }

        copies.push_back(std::move(v));
        rows.push_back(row);
      }
    }
  });

  ASSERT_EQ(rows.size(), 3UL);

  /* The page is no longer latched: the copies hold. */
  for (size_t r{}; r < rows.size(); ++r) {
    for (ulint i{}; i < dtuple_get_n_fields(rows[r]); ++i) {
      EXPECT_EQ(bytes(dtuple_get_nth_field(rows[r], i)), copies[r][i]) << r << " " << i;
    }
  }

  /* The row of k = 2 has NULL in each nullable column, the NOT NULL is set. */
  for (ulint c{}; c < ALL_TYPES.size(); ++c) {
    const auto f = dtuple_get_nth_field(rows[1], c);

    if (c == 0 || c == 16) {
      EXPECT_FALSE(dfield_is_null(f)) << c;
    } else {
      EXPECT_TRUE(dfield_is_null(f)) << ALL_TYPES[c].m_name;
    }
  }

  /* The column types as the API created them. */
  for (ulint c{}; c < ALL_TYPES.size(); ++c) {
    const auto col = table->get_nth_col(c);
    const auto &d = ALL_TYPES[c];

    EXPECT_EQ(col->mtype, ulint(d.m_type)) << d.m_name;
    EXPECT_EQ((col->prtype & DATA_UNSIGNED) != 0, (d.m_attr & IB_COL_UNSIGNED) != 0) << d.m_name;
    EXPECT_EQ((col->prtype & DATA_NOT_NULL) != 0, (d.m_attr & IB_COL_NOT_NULL) != 0) << d.m_name;
    EXPECT_EQ((col->prtype & DATA_CUSTOM_TYPE) != 0, (d.m_attr & IB_COL_CUSTOM1) != 0) << d.m_name;
    EXPECT_EQ((col->prtype & (DATA_CUSTOM_TYPE << 2)) != 0, (d.m_attr & IB_COL_CUSTOM3) != 0) << d.m_name;
    EXPECT_EQ(col->len, d.m_len) << d.m_name;
  }
}

/** The values as stored: unsigned big endian, signed with the sign bit
flipped, a CHAR padded with spaces to its length, a VARCHAR as is. */
TEST_F(RowTest, StoredForms) {
  const auto t = create_all();

  load(t, {all_row(7, false, "blob")});

  for_each_rec(*m_sys, index(t), [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    auto row = row_build(*m_sys, ROW_COPY_POINTERS, index(t), rec, offs, nullptr, &ext, m_heap);
    auto v = [&](ulint c) { return bytes(dtuple_get_nth_field(row, c)).value_or("<NULL>"); };

    EXPECT_EQ(v(0), be4(7));
    EXPECT_EQ(v(1), std::string(1, char(uint8_t(2) ^ 0x80)));
    EXPECT_EQ(v(2), std::string(1, char(7)));
    EXPECT_EQ(v(3), std::string("\x80\xc8", 2));
    EXPECT_EQ(v(5), s4(200000));
    EXPECT_EQ(v(8), "v7");
    EXPECT_EQ(v(9).substr(0, 3), "chr");
    EXPECT_EQ(v(12), "blob");
    EXPECT_EQ(v(16), be4(21));
    EXPECT_EQ(v(19), be4(8));
  });
}

/** row_build_index_entry() from a built row gives, for the clustered index,
the record itself (equal fields, compares equal, the same converted size),
and for each secondary index an entry that is in the index: the prefix
columns cut to the prefix, NULLs kept NULL. The compared fields are the
unique ones of the tree. */
TEST_F(RowTest, IndexEntries) {
  const auto t = create_all();

  load(t, {all_row(1, false, "blobvalue-longer-than-ten"), all_row(2, true), all_row(3, false, "")});

  auto clust = index(t);
  std::vector<DTuple *> rows;

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    /* By pointers first: in a debug build ROW_COPY_DATA makes the (const)
    offsets valid for the copy (rec_offs_make_valid()), not for rec. */
    auto ptr = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, m_heap);
    auto entry = row_build_index_entry(ptr, ext, clust, m_heap);

    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(dtuple_get_n_fields(entry), clust->get_n_fields());
    EXPECT_EQ(dtuple_get_n_fields_cmp(entry), clust->get_n_unique_in_tree());

    for (ulint i{}; i < clust->get_n_fields(); ++i) {
      EXPECT_EQ(bytes(dtuple_get_nth_field(entry, i)), bytes(rec, offs, i)) << i;
    }

    EXPECT_EQ(cmp_dtuple_rec(nullptr, entry, rec, offs.get()), 0);
    EXPECT_EQ(rec_get_converted_size(clust, entry, 0), offs.size());
    rows.push_back(row_build(*m_sys, ROW_COPY_DATA, clust, rec, offs, nullptr, &ext, m_heap));
  });

  ASSERT_EQ(rows.size(), 3UL);

  for (auto idx = clust->get_next(); idx != nullptr; idx = idx->get_next()) {
    EXPECT_EQ(n_recs(*m_sys, idx), 3UL) << idx->m_name;

    for (size_t r{}; r < rows.size(); ++r) {
      auto entry = row_build_index_entry(rows[r], nullptr, idx, m_heap);

      ASSERT_NE(entry, nullptr);
      ASSERT_EQ(dtuple_get_n_fields(entry), idx->get_n_fields());
      EXPECT_EQ(dtuple_get_n_fields_cmp(entry), idx->get_n_unique_in_tree());

      for (ulint i{}; i < idx->get_n_fields(); ++i) {
        const auto field = idx->get_nth_field(i);
        const auto f = dtuple_get_nth_field(entry, i);

        if (field->m_prefix_len > 0 && !dfield_is_null(f)) {
          EXPECT_LE(dfield_get_len(f), field->m_prefix_len) << idx->m_name << " " << i;
        }
      }

      Mtr mtr;
      BtreePcursor pcur;

      mtr.start();

      const auto found = row_search_index_entry(*m_sys, idx, entry, BTR_SEARCH_LEAF, &pcur, &mtr);

      EXPECT_TRUE(found) << idx->m_name << " row " << r;

      if (found) {
        auto rec = pcur.get_rec();
        MemHeap *heap{};
        RecOffs offs = PhyRec(idx, rec).get_all_col_offsets(nullptr, &heap, Current_location());

        EXPECT_EQ(cmp_dtuple_rec(nullptr, entry, rec, offs.get()), 0);

        /* Not the DB_TRX_ID of a secondary record: a built entry has none yet. */
        const auto n = idx->is_clustered() ? idx->get_n_fields() : idx->get_n_fields() - 1;

        for (ulint i{}; i < n; ++i) {
          EXPECT_EQ(bytes(dtuple_get_nth_field(entry, i)), bytes(rec, offs, i)) << idx->m_name << " " << i;
        }

        mem_heap_free(heap);
      }

      pcur.close();
      mtr.commit(*m_sys);
    }
  }

  /* The prefix columns of row 1: ch(3) and bl(10). */
  auto ch3 = row_build_index_entry(rows[0], nullptr, index(t, "S_CH3"), m_heap);
  auto bl10 = row_build_index_entry(rows[0], nullptr, index(t, "S_BL10"), m_heap);

  EXPECT_EQ(bytes(dtuple_get_nth_field(ch3, 0)), "chr");
  EXPECT_EQ(bytes(dtuple_get_nth_field(bl10, 0)), "blobvalue-");

  /* Row 2: NULL stays NULL through a prefix; row 3: an empty blob. */
  auto bl10n = row_build_index_entry(rows[1], nullptr, index(t, "S_BL10"), m_heap);
  auto bl10e = row_build_index_entry(rows[2], nullptr, index(t, "S_BL10"), m_heap);

  EXPECT_TRUE(dfield_is_null(dtuple_get_nth_field(bl10n, 0)));
  EXPECT_EQ(bytes(dtuple_get_nth_field(bl10e, 0)), "");
}

/** row_rec_to_index_entry() of each record of each index, copying and by
pointers: the fields of the record, typed as the index, n_ext 0, the info
bits of the record (a delete marked record has the delete flag). */
TEST_F(RowTest, RecToIndexEntry) {
  const auto t = create_all();

  load(t, {all_row(1, false, "x"), all_row(2, true), all_row(3, false, "yy")});

  auto trx = begin();

  ASSERT_EQ(remove_u32(*m_sys, t, trx, 2), DB_SUCCESS);

  for (auto idx = index(t); idx != nullptr; idx = idx->get_next()) {
    size_t n{};
    size_t deleted{};

    for_each_rec(*m_sys, idx, [&](const Rec rec, const RecOffs &offs) {
      for (auto type : {ROW_COPY_POINTERS, ROW_COPY_DATA}) {
        ulint n_ext{99};
        auto entry = row_rec_to_index_entry(type, rec, idx, offs, &n_ext, m_heap);

        ASSERT_EQ(dtuple_get_n_fields(entry), idx->get_n_fields());
        EXPECT_EQ(n_ext, 0UL);
        EXPECT_EQ(dtuple_get_n_fields_cmp(entry), idx->get_n_unique_in_tree());
        EXPECT_EQ(dtuple_get_info_bits(entry), rec.get_info_bits());

        for (ulint i{}; i < idx->get_n_fields(); ++i) {
          const auto f = dtuple_get_nth_field(entry, i);

          EXPECT_EQ(bytes(f), bytes(rec, offs, i)) << idx->m_name << " " << i;
          EXPECT_EQ(dfield_get_type(f)->mtype, idx->get_nth_field(i)->m_col->mtype);

          ulint len;

          if (type == ROW_COPY_DATA && !dfield_is_null(f) && dfield_get_len(f) > 0) {
            EXPECT_NE(dfield_get_data(f), rec.get_nth_field(offs, i, &len).get());
          } else if (type == ROW_COPY_POINTERS && !dfield_is_null(f)) {
            EXPECT_EQ(dfield_get_data(f), rec.get_nth_field(offs, i, &len).get());
          }
        }

        if (type == ROW_COPY_POINTERS) {
          EXPECT_EQ(cmp_dtuple_rec(nullptr, entry, rec, offs.get()), 0);
        }
      }

      ++n;
      deleted += rec.get_deleted_flag() ? 1 : 0;
    });

    EXPECT_EQ(n, 3UL) << idx->m_name;
    EXPECT_EQ(deleted, 1UL) << idx->m_name;
  }

  rollback(trx);
}

/** A table of signed and unsigned integer keys at their extremes. */
class RowIntTest : public RowTest {
 protected:
  void SetUp() override {
    RowTest::SetUp();

    m_t = name();

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_t,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4},
         {"i", IB_INT, IB_COL_NONE, 4},
         {"u", IB_INT, IB_COL_UNSIGNED, 4},
         {"j", IB_INT, IB_COL_NONE, 8},
         {"h", IB_INT, IB_COL_NONE, 2}},
        {{"PRIMARY", {{"k", 0}}, true, true}, {"SI", {{"i", 0}}}, {"SU", {{"u", 0}}}, {"SJ", {{"j", 0}}}, {"SH", {{"h", 0}}}}
      ),
      DB_SUCCESS
    );

    std::vector<Fill> rows;

    for (uint32_t k{}; k < m_i.size(); ++k) {
      rows.push_back([this, k](api::Tuple *tpl) {
        auto err = tpl->write_u32(0, k);

        if (err == DB_SUCCESS) err = tpl->write_i32(1, m_i[k]);
        if (err == DB_SUCCESS) err = tpl->write_u32(2, m_u[k]);
        if (err == DB_SUCCESS) err = tpl->write_i64(3, m_j[k]);
        if (err == DB_SUCCESS) err = tpl->write_i16(4, m_h[k]);

        return err;
      });
    }

    load(m_t, rows);
  }

  /** The values, in no particular order; the same index in each is a row. */
  const std::vector<int32_t> m_i{0, -1, INT32_MIN, INT32_MAX, 1, -2, INT32_MIN + 1, INT32_MAX - 1};
  const std::vector<uint32_t> m_u{0, UINT32_MAX, 1, 0x80000000u, 0x7fffffffu, 2, UINT32_MAX - 1, 0x80000001u};
  const std::vector<int64_t> m_j{0, -1, INT64_MIN, INT64_MAX, 1, -2, INT64_MIN + 1, INT64_MAX - 1};
  const std::vector<int16_t> m_h{0, -1, INT16_MIN, INT16_MAX, 1, -2, INT16_MIN + 1, INT16_MAX - 1};
  std::string m_t;
};

/** Each secondary index holds its values in numeric order: signed values
stored big endian with the sign bit flipped, unsigned as they are. Entries
built from the records compare with the records and with each other in the
same order, and dtuple_coll_cmp() agrees. */
TEST_F(RowIntTest, StorageOrder) {
  struct Case {
    const char *m_idx;
    std::vector<std::string> m_expected;
  };

  auto sorted = [](auto v) {
    std::sort(v.begin(), v.end());
    return v;
  };

  std::vector<std::string> si;
  std::vector<std::string> su;
  std::vector<std::string> sj;
  std::vector<std::string> sh;

  for (auto v : sorted(m_i)) {
    si.push_back(s4(v));
  }

  for (auto v : sorted(m_u)) {
    su.push_back(be4(v));
  }

  for (auto v : sorted(m_j)) {
    std::string s(8, '\0');

    mach_write_to_8(reinterpret_cast<byte *>(s.data()), uint64_t(v) ^ (1ULL << 63));
    sj.push_back(s);
  }

  for (auto v : sorted(m_h)) {
    std::string s(2, '\0');

    mach_write_to_2(reinterpret_cast<byte *>(s.data()), uint16_t(v) ^ 0x8000u);
    sh.push_back(s);
  }

  for (const auto &c : std::vector<Case>{{"SI", si}, {"SU", su}, {"SJ", sj}, {"SH", sh}}) {
    auto idx = index(m_t, c.m_idx);
    std::vector<std::string> got;
    std::vector<DTuple *> entries;

    for_each_rec(*m_sys, idx, [&](const Rec rec, const RecOffs &offs) {
      ulint n_ext;

      got.push_back(*bytes(rec, offs, 0));
      entries.push_back(dup(row_rec_to_index_entry(ROW_COPY_POINTERS, rec, idx, offs, &n_ext, m_heap), m_heap));

      /* Each entry against each record seen so far: later is greater. */
      for (size_t i{}; i < entries.size(); ++i) {
        const auto r = cmp_dtuple_rec(nullptr, entries[i], rec, offs.get());

        if (i + 1 == entries.size()) {
          EXPECT_EQ(r, 0) << c.m_idx;
        } else {
          EXPECT_LT(r, 0) << c.m_idx << " entry " << i << " vs record " << entries.size() - 1;
        }
      }
    });

    EXPECT_EQ(got, c.m_expected) << c.m_idx;

    for (size_t i = 1; i < entries.size(); ++i) {
      EXPECT_LT(dtuple_coll_cmp(nullptr, entries[i - 1], entries[i]), 0) << c.m_idx << " " << i;
      EXPECT_GT(dtuple_coll_cmp(nullptr, entries[i], entries[i - 1]), 0) << c.m_idx << " " << i;
    }
  }
}

/** The fold of an entry built from a record equals the fold of the record,
for every number of fields and byte prefixes of the next field, with NULLs;
equal keys fold equal. */
TEST_F(RowTest, FoldMatchesRecord) {
  const auto t = create_all();
  std::vector<Fill> rows;

  for (uint32_t k = 1; k <= 20; ++k) {
    rows.push_back(all_row(k, k % 4 == 0, std::string(k * 3, char('a' + k))));
  }

  load(t, rows);

  for (auto idx = index(t); idx != nullptr; idx = idx->get_next()) {
    for_each_rec(*m_sys, idx, [&](const Rec rec, const RecOffs &offs) {
      ulint n_ext;
      auto entry = row_rec_to_index_entry(ROW_COPY_POINTERS, rec, idx, offs, &n_ext, m_heap);
      const auto n = idx->get_n_fields();

      for (ulint f{}; f <= n; ++f) {
        for (ulint nb : {0UL, 1UL, 3UL, 100UL}) {
          if (f + nb == 0 || (f == n && nb > 0)) {
            continue;
          }

          EXPECT_EQ(dtuple_fold(entry, f, nb, idx->m_id), rec.fold(offs, f, nb, idx->m_id))
            << idx->m_name << " fields " << f << " bytes " << nb;
        }
      }
    });
  }
}

/** The trx id and roll pointer of a clustered record: the id of the trx
that inserted it with an insert undo pointer, then of the trx that updated
it with an update undo pointer. The offset of DB_TRX_ID is precomputed
when the key is of fixed length (INT) and computed when it is not
(VARCHAR); both agree with the record. */
TEST_F(RowTest, TrxIdRollPtr) {
  const auto ti = name("i");
  const auto tv = name("v");

  ASSERT_EQ(
    create_table(
      *m_sys, ti, {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"a", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {{"k", 0}}, true, true}}
    ),
    DB_SUCCESS
  );
  ASSERT_EQ(
    create_table(
      *m_sys, tv, {{"k", IB_VARCHAR, IB_COL_NONE, 20}, {"a", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {{"k", 0}}, true, true}}
    ),
    DB_SUCCESS
  );

  auto clust_i = index(ti);
  auto clust_v = index(tv);

  EXPECT_NE(clust_i->m_trx_id_offset, 0U);
  EXPECT_EQ(clust_v->m_trx_id_offset, 0U);

  auto check = [&](Index *clust, trx_id_t id, bool is_insert) {
    size_t n{};

    for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
      const auto pos = clust->get_sys_col_field_pos(DATA_TRX_ID);
      ulint len;

      EXPECT_EQ(row_get_trx_id_offset(rec, clust, offs.get()), offs.get_nth_field(pos, &len));
      EXPECT_EQ(len, DATA_TRX_ID_LEN);
      EXPECT_EQ(row_get_rec_trx_id(rec, clust, offs.get()), id);

      const auto roll_ptr = row_get_rec_roll_ptr(rec, clust, offs.get());

      EXPECT_NE(roll_ptr, 0UL);
      EXPECT_EQ(trx_undo_roll_ptr_is_insert(roll_ptr), is_insert);
      EXPECT_EQ(roll_ptr, trx_read_roll_ptr(rec.get_nth_field(offs, pos + 1, &len).get()));
      ++n;
    });

    EXPECT_EQ(n, 1UL);
  };

  auto trx = begin();

  ASSERT_EQ(insert(*m_sys, ti, trx, [](api::Tuple *tpl) {
    auto err = tpl->write_u32(0, 1);
    return err == DB_SUCCESS ? tpl->write_u32(1, 10) : err;
  }), DB_SUCCESS);
  ASSERT_EQ(insert(*m_sys, tv, trx, [](api::Tuple *tpl) {
    auto err = tpl->set_value(0, "key", 3);
    return err == DB_SUCCESS ? tpl->write_u32(1, 10) : err;
  }), DB_SUCCESS);

  const auto id1 = trx->m_id;

  check(clust_i, id1, true);
  check(clust_v, id1, true);
  commit(trx);

  /* An update by another transaction: delete and insert again in place. */
  trx = begin();
  ASSERT_EQ(remove_u32(*m_sys, ti, trx, 1), DB_SUCCESS);

  const auto id2 = trx->m_id;

  EXPECT_NE(id2, id1);
  check(clust_i, id2, false);
  commit(trx);
}

/** A table (k INT UNSIGNED PK, s VARCHAR(40), INDEX SP(s(3))): the secondary
records are (s(3), k). */
class RowRefTest : public RowTest {
 protected:
  void SetUp() override {
    RowTest::SetUp();

    m_t = name();

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_t,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"s", IB_VARCHAR, IB_COL_NONE, 40}, {"i", IB_INT, IB_COL_NONE, 4}},
        {{"PRIMARY", {{"k", 0}}, true, true}, {"SP", {{"s", 3}}}, {"SI", {{"i", 0}}}}
      ),
      DB_SUCCESS
    );

    std::vector<Fill> rows;

    for (uint32_t k = 1; k <= 50; ++k) {
      rows.push_back([k](api::Tuple *tpl) {
        auto err = tpl->write_u32(0, k * 7);
        const auto s = std::format("s{:05}-tail", k);

        if (err == DB_SUCCESS) err = tpl->set_value(1, s.data(), s.size());
        if (err == DB_SUCCESS) err = tpl->write_i32(2, int32_t(k) - 25);

        return err;
      });
    }

    load(m_t, rows);
  }

  /** Check ref is the clustered key of the row whose secondary record is
  sec_rec, and leads to that clustered record. */
  void check_ref(const DTuple *ref, uint32_t k, const char *what) {
    ASSERT_EQ(dtuple_get_n_fields(ref), 1UL) << what;
    EXPECT_EQ(bytes(dtuple_get_nth_field(ref, 0)), be4(k)) << what;
    EXPECT_EQ(dfield_get_type(dtuple_get_nth_field(ref, 0))->mtype, DATA_INT) << what;
  }

  std::string m_t;
};

/** row_build_row_ref() of a record of an index without prefixes (SI): the
clustered key, copied or pointing into the record; it finds the clustered
record with row_search_on_row_ref() and row_get_clust_rec(). */
TEST_F(RowRefTest, FromSecondary) {
  auto si = index(m_t, "SI");
  auto clust = index(m_t);
  std::vector<std::pair<DTuple *, uint32_t>> refs;

  for_each_rec(*m_sys, si, [&](const Rec rec, const RecOffs &offs) {
    const auto k = mach_read_from_4(reinterpret_cast<const byte *>(bytes(rec, offs, 1)->data()));
    auto ref = row_build_row_ref(ROW_COPY_DATA, si, rec, m_heap);
    auto ptr = row_build_row_ref(ROW_COPY_POINTERS, si, rec, m_heap);
    ulint len;

    check_ref(ref, k, "copy");
    check_ref(ptr, k, "pointers");
    EXPECT_EQ(dfield_get_data(dtuple_get_nth_field(ptr, 0)), rec.get_nth_field(offs, 1, &len).get());
    EXPECT_NE(dfield_get_data(dtuple_get_nth_field(ref, 0)), rec.get_nth_field(offs, 1, &len).get());

    /* In a tuple of the caller, with and without the offsets. */
    auto in = dtuple_create(m_heap, clust->get_n_unique());

    row_build_row_ref_in_tuple(in, rec, si, &offs, nullptr);
    check_ref(in, k, "in_tuple");

    auto in2 = dtuple_create(m_heap, clust->get_n_unique());

    row_build_row_ref_in_tuple(in2, rec, si, nullptr, nullptr);
    check_ref(in2, k, "in_tuple no offsets");

    /* The fast way, with a map of the clustered key fields in the record. */
    std::vector<ulint> map;

    for (ulint i{}; i < clust->get_n_unique(); ++i) {
      map.push_back(si->get_nth_field_pos(clust, i));
    }

    auto fast = dtuple_create(m_heap, clust->get_n_unique());

    clust->copy_types(fast, clust->get_n_unique());
    row_build_row_ref_fast(fast, map.data(), rec, offs);
    check_ref(fast, k, "fast");

    refs.emplace_back(ref, k);
  });

  ASSERT_EQ(refs.size(), 50UL);

  for (const auto &[ref, k] : refs) {
    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    ASSERT_TRUE(row_search_on_row_ref(*m_sys, &pcur, BTR_SEARCH_LEAF, clust->m_table, ref, &mtr)) << k;

    ulint len;

    EXPECT_EQ(mach_read_from_4(pcur.get_rec().get_nth_field(0, &len).get()), k);
    pcur.close();
    mtr.commit(*m_sys);
  }

  /* A key that is not there: positioned before it, not found. */
  auto missing = dtuple_create(m_heap, 1);

  clust->copy_types(missing, 1);
  set(m_heap, missing, 0, DATA_INT, DATA_UNSIGNED, 4, be4(8));

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  EXPECT_FALSE(row_search_on_row_ref(*m_sys, &pcur, BTR_SEARCH_LEAF, clust->m_table, missing, &mtr));
  pcur.close();
  mtr.commit(*m_sys);

  /* Smaller than all: on the infimum. */
  set(m_heap, missing, 0, DATA_INT, DATA_UNSIGNED, 4, be4(0));
  mtr.start();
  EXPECT_FALSE(row_search_on_row_ref(*m_sys, &pcur, BTR_SEARCH_LEAF, clust->m_table, missing, &mtr));
  pcur.close();
  mtr.commit(*m_sys);

  /* row_get_clust_rec() from each secondary record. */
  for_each_rec(*m_sys, si, [&](const Rec rec, const RecOffs &offs) {
    Mtr mtr2;
    Index *ci{};

    mtr2.start();

    auto crec = row_get_clust_rec(*m_sys, BTR_SEARCH_LEAF, rec, si, &ci, &mtr2);

    EXPECT_EQ(ci, clust);
    ASSERT_FALSE(crec.is_null());

    ulint len;

    EXPECT_EQ(std::string(reinterpret_cast<const char *>(crec.get_nth_field(0, &len).get()), 4), *bytes(rec, offs, 1));
    mtr2.commit(*m_sys);
  });
}

/** The row reference from a record of a column prefix index SP(s(3)): the
records are (s(3), k), the reference is k, all 4 bytes.
BUG: row/row0row.cc:323 and :408 (row_build_row_ref, row_build_row_ref_in_tuple)
take the prefix length from field i of the secondary index instead of field
i of the clustered index (upstream 7c21eac0 row0row.c:465 uses clust_index).
SP's field 0 is s(3): the 4 byte key k is cut to 3 bytes and the reference
finds no clustered record. */
TEST_F(RowRefTest, FromPrefixIndex) {
  auto sp = index(m_t, "SP");
  auto clust = index(m_t);
  std::vector<std::pair<DTuple *, uint32_t>> refs;

  for_each_rec(*m_sys, sp, [&](const Rec rec, const RecOffs &offs) {
    EXPECT_EQ(bytes(rec, offs, 0)->size(), 3UL);

    const auto k = mach_read_from_4(reinterpret_cast<const byte *>(bytes(rec, offs, 1)->data()));

    check_ref(row_build_row_ref(ROW_COPY_DATA, sp, rec, m_heap), k, "copy");

    auto in = dtuple_create(m_heap, clust->get_n_unique());

    row_build_row_ref_in_tuple(in, rec, sp, &offs, nullptr);
    check_ref(in, k, "in_tuple");

    refs.emplace_back(row_build_row_ref(ROW_COPY_DATA, sp, rec, m_heap), k);
  });

  for (const auto &[ref, k] : refs) {
    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    EXPECT_TRUE(row_search_on_row_ref(*m_sys, &pcur, BTR_SEARCH_LEAF, clust->m_table, ref, &mtr)) << k;
    pcur.close();
    mtr.commit(*m_sys);
  }
}

/** Through the API: a search on SP with access to the clustered record finds
the row (the clustered lookup builds the reference with
row_build_row_ref_in_tuple(), see FromPrefixIndex). */
TEST_F(RowRefTest, PrefixIndexClusterAccess) {
  auto trx = begin();
  api::Cursor *crsr{};
  api::Cursor *sec{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, m_t.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IS), DB_SUCCESS);
  ASSERT_EQ(crsr->open_index(*m_sys, "SP", &sec), DB_SUCCESS);
  sec->set_cluster_access();

  auto key = sec->sec_search_tuple_create();
  int res{-1};

  ASSERT_EQ(key->set_value(0, "s00", 3), DB_SUCCESS);
  /* Each secondary record is skipped: "Clustered record for sec rec not
  found", then DB_END_OF_INDEX. */
  ASSERT_EQ(sec->moveto(*m_sys, key, IB_CUR_GE, &res), DB_SUCCESS);

  auto tpl = sec->clust_read_tuple_create();

  EXPECT_EQ(sec->read_row(*m_sys, tpl), DB_SUCCESS);

  uint32_t k{};

  EXPECT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
  EXPECT_EQ(k, 7U);

  api::Tuple::destroy(tpl);
  api::Tuple::destroy(key);
  (void)api::Cursor::close(*m_sys, sec);
  (void)api::Cursor::close(*m_sys, crsr);
  commit(trx);
}

/** A clustered key that is a column prefix, PRIMARY(s(4)), and a secondary
index on the whole column, SS(s): the secondary record holds s once, whole;
the reference must cut it to the 4 bytes of the clustered key (the same
divergence as FromPrefixIndex, the other way around). */
TEST_F(RowTest, RefOfPrefixPrimaryKey) {
  const auto t = name();
  const auto err = create_table(
    *m_sys,
    t,
    {{"s", IB_VARCHAR, IB_COL_NONE, 20}, {"v", IB_INT, IB_COL_UNSIGNED, 4}},
    {{"PRIMARY", {{"s", 4}}, true, true}, {"SS", {{"s", 0}}}, {"SV", {{"v", 0}}}}
  );

  if (err != DB_SUCCESS) {
    GTEST_SKIP() << "a primary key on a column prefix is refused: " << err;
  }

  load(t, {[](api::Tuple *tpl) {
    auto e = tpl->set_value(0, "abcdefgh", 8);
    return e == DB_SUCCESS ? tpl->write_u32(1, 5) : e;
  }});

  for (const char *idx_name : {"SS", "SV"}) {
    auto idx = index(t, idx_name);
    DTuple *ref{};

    for_each_rec(*m_sys, idx, [&](const Rec rec, const RecOffs &) { ref = row_build_row_ref(ROW_COPY_DATA, idx, rec, m_heap); });

    ASSERT_NE(ref, nullptr) << idx_name;
    EXPECT_EQ(bytes(dtuple_get_nth_field(ref, 0)), "abcd") << idx_name;

    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    EXPECT_TRUE(row_search_on_row_ref(*m_sys, &pcur, BTR_SEARCH_LEAF, index(t)->m_table, ref, &mtr)) << idx_name;
    pcur.close();
    mtr.commit(*m_sys);
  }
}

/** row_search_index_entry(): an entry that is not in the index, and an
empty index (positioned on the infimum). */
TEST_F(RowRefTest, SearchIndexEntryMissing) {
  auto si = index(m_t, "SI");
  auto entry = dtuple_create(m_heap, 2);

  si->copy_types(entry, 2);
  set(m_heap, entry, 0, DATA_INT, 0, 4, s4(1000));
  set(m_heap, entry, 1, DATA_INT, DATA_UNSIGNED, 4, be4(7));

  for (int i = 0; i < 2; ++i) {
    Mtr mtr;
    BtreePcursor pcur;

    mtr.start();
    EXPECT_FALSE(row_search_index_entry(*m_sys, si, entry, BTR_SEARCH_LEAF, &pcur, &mtr));
    pcur.close();
    mtr.commit(*m_sys);

    /* Smaller than all. */
    set(m_heap, entry, 0, DATA_INT, 0, 4, s4(INT32_MIN));
  }

  const auto t = name("empty");

  ASSERT_EQ(
    create_table(*m_sys, t, {{"k", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {{"k", 0}}, true, true}}), DB_SUCCESS
  );

  auto e = dtuple_create(m_heap, 1);

  index(t)->copy_types(e, 1);
  set(m_heap, e, 0, DATA_INT, DATA_UNSIGNED, 4, be4(1));

  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  EXPECT_FALSE(row_search_index_entry(*m_sys, index(t), e, BTR_SEARCH_LEAF, &pcur, &mtr));
  pcur.close();
  mtr.commit(*m_sys);
}

/* --- row0ext: prefixes of externally stored columns ---------------------- */

/** A table (k INT UNSIGNED PK, b BLOB, c BLOB, d VARCHAR(20)) with the column
prefix indexes PB(b(10)) and PB768(b(768)); c is in no index. Rows with b
and c of the sizes around the in-page and the external storage limits. */
class RowExtTest : public RowTest {
 protected:
  /* The table is made once for the suite: the tests only read it. */
  void SetUp() override {
    RowTest::SetUp();

    if (!s_t.empty()) {
      m_t = s_t;
      return;
    }

    m_t = s_t = name();

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_t,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"b", IB_BLOB, IB_COL_NONE, 0}, {"c", IB_BLOB, IB_COL_NONE, 0}, {"d", IB_VARCHAR, IB_COL_NONE, 20}},
        {{"PRIMARY", {{"k", 0}}, true, true}, {"PB", {{"b", 10}}}, {"PB768", {{"b", 768}}}}
      ),
      DB_SUCCESS
    );

    std::vector<Fill> rows;

    for (uint32_t k{}; k < SIZES.size(); ++k) {
      m_b.push_back(ut::blob(k, SIZES[k]));
      /* c has the sizes in reverse, the last row both long. */
      m_c.push_back(ut::blob(k + 1000, k + 1 == SIZES.size() ? 20000 : SIZES[SIZES.size() - 2 - k]));

      rows.push_back([k](api::Tuple *tpl) {
        auto err = tpl->write_u32(0, k);

        if (err == DB_SUCCESS) err = tpl->set_value(1, m_b[k].data(), m_b[k].size());
        if (err == DB_SUCCESS) err = tpl->set_value(2, m_c[k].data(), m_c[k].size());
        if (err == DB_SUCCESS) err = tpl->set_value(3, "d", 1);

        return err;
      });
    }

    load(m_t, rows);
  }

  /** The column numbers of b and c. */
  static constexpr ulint B = 1;
  static constexpr ulint C = 2;

  /** The sizes of b, in the order of k. */
  static inline const std::vector<size_t> SIZES{0, 1, 767, 768, 769, 4000, 8000, 8200, 16384, 100000, 20000};

  static inline std::string s_t;
  static inline std::vector<std::string> m_b;
  static inline std::vector<std::string> m_c;

  std::string m_t;
};

/** Each column of a built row is local or external as in the record. An
external one is the 768 byte prefix and a 20 byte reference that is set;
its prefix entries in PB and PB768 are the prefixes of the whole value,
from the local prefix (no cache) and from the cache made by
row_ext_create() alike. */
TEST_F(RowExtTest, Boundaries) {
  auto clust = index(m_t);
  auto pb = index(m_t, "PB");
  auto pb768 = index(m_t, "PB768");
  size_t n{};
  size_t n_ext{};

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    auto row = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, m_heap);
    const auto k = mach_read_from_4(reinterpret_cast<const byte *>(bytes(rec, offs, 0)->data()));

    for (auto [col, value] : {std::pair(B, &m_b[k]), std::pair(C, &m_c[k])}) {
      const auto pos = clust->get_clustered_field_pos(clust->m_table->get_nth_col(col));
      const auto f = dtuple_get_nth_field(row, col);

      EXPECT_EQ(dfield_is_ext(f) != 0, offs.nth_extern(pos) != 0) << k << " col " << col;

      if (value->size() <= REC_MAX_INDEX_COL_LEN) {
        EXPECT_FALSE(dfield_is_ext(f)) << k << " col " << col;
      }

      if (value->size() >= 100000) {
        EXPECT_TRUE(dfield_is_ext(f)) << k << " col " << col;
      }

      if (dfield_is_ext(f)) {
        ++n_ext;

        const auto data = static_cast<const byte *>(dfield_get_data(f));

        ASSERT_EQ(dfield_get_len(f), REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE) << k;
        EXPECT_EQ(std::string(reinterpret_cast<const char *>(data), REC_MAX_INDEX_COL_LEN), value->substr(0, REC_MAX_INDEX_COL_LEN));
        EXPECT_NE(memcmp(data + REC_MAX_INDEX_COL_LEN, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE), 0) << k;
      } else {
        EXPECT_EQ(bytes(f), *value) << k << " col " << col;
      }
    }

    /* The prefix entries, without and with a cache of b and c. */
    /* row_ext_create() is only for external columns. */
    auto cols = reinterpret_cast<ulint *>(mem_heap_alloc(m_heap, 2 * sizeof(ulint)));
    ulint n_cols{};

    for (auto col : {B, C}) {
      if (dfield_is_ext(dtuple_get_nth_field(row, col))) {
        cols[n_cols++] = col;
      }
    }

    RowExt *cache = n_cols == 0 ? nullptr : row_ext_create(*m_sys, n_cols, cols, row, m_heap);

    for (auto e : {static_cast<RowExt *>(nullptr), cache}) {
      for (auto [idx, len] : {std::pair(pb, 10UL), std::pair(pb768, 768UL)}) {
        auto entry = row_build_index_entry(row, e, idx, m_heap);

        ASSERT_NE(entry, nullptr) << k;
        EXPECT_EQ(bytes(dtuple_get_nth_field(entry, 0)), m_b[k].substr(0, len)) << k << " " << idx->m_name;
        EXPECT_FALSE(dfield_is_ext(dtuple_get_nth_field(entry, 0)));
      }
    }

    ++n;
  });

  EXPECT_EQ(n, SIZES.size());
  EXPECT_GE(n_ext, 6UL);

  /* The whole values through the API. */
  auto trx = begin();
  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(*m_sys, m_t.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IS), DB_SUCCESS);

  auto tpl = crsr->clust_read_tuple_create();
  auto err = crsr->first(*m_sys);
  uint32_t k{};

  while (err == DB_SUCCESS) {
    ASSERT_EQ(crsr->read_row(*m_sys, tpl), DB_SUCCESS);
    EXPECT_EQ(tpl->get_len(1), m_b[k].size()) << k;
    EXPECT_EQ(tpl->get_len(2), m_c[k].size()) << k;
    EXPECT_EQ(memcmp(tpl->get_value(1), m_b[k].data(), m_b[k].size()), 0) << k;
    tpl = tpl->clear();
    err = crsr->next(*m_sys);
    ++k;
  }

  EXPECT_EQ(k, SIZES.size());
  api::Tuple::destroy(tpl);
  (void)api::Cursor::close(*m_sys, crsr);
  commit(trx);
}

/** row_ext_create() on a built row: for each external column the prefix of
REC_MAX_INDEX_COL_LEN bytes of the whole value, looked up by column number;
a column not in the cache is not found. */
TEST_F(RowExtTest, Cache) {
  auto clust = index(m_t);
  size_t n{};

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    auto row = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, m_heap);
    const auto k = mach_read_from_4(reinterpret_cast<const byte *>(bytes(rec, offs, 0)->data()));

    if (!dfield_is_ext(dtuple_get_nth_field(row, B)) || !dfield_is_ext(dtuple_get_nth_field(row, C))) {
      return;
    }

    std::array<ulint, 2> cols{C, B};
    auto cache = row_ext_create(*m_sys, cols.size(), cols.data(), row, m_heap);

    ASSERT_EQ(cache->n_ext, 2UL);
    EXPECT_EQ(cache->ext, cols.data());

    for (auto [col, value] : {std::pair(B, &m_b[k]), std::pair(C, &m_c[k])}) {
      ulint len{};
      auto p = row_ext_lookup(cache, col, &len);

      ASSERT_NE(p, nullptr);
      ASSERT_NE(p, field_ref_zero);
      EXPECT_EQ(len, REC_MAX_INDEX_COL_LEN);
      EXPECT_EQ(std::string(reinterpret_cast<const char *>(p), len), value->substr(0, REC_MAX_INDEX_COL_LEN)) << k;
    }

    ulint len{};

    EXPECT_EQ(row_ext_lookup_ith(cache, 0, &len), row_ext_lookup(cache, C, &len));
    EXPECT_EQ(row_ext_lookup(cache, 0, &len), nullptr);
    EXPECT_EQ(row_ext_lookup(cache, 3, &len), nullptr);
    ++n;
  });

  EXPECT_GE(n, 1UL);
}

/** An external column whose reference is not yet set (all zero, as during
an insert before the BLOB is written): the cache can't fetch it, the lookup
gives field_ref_zero and the entry of a prefix index can't be built
(nullptr); the clustered index entry ignores the cache. */
TEST_F(RowExtTest, ZeroRef) {
  auto clust = index(m_t);
  auto table = clust->m_table;
  auto row = dtuple_create(m_heap, table->get_n_cols());

  table->copy_types(row);

  for (ulint i{}; i < table->get_n_cols(); ++i) {
    dfield_set_data(dtuple_get_nth_field(row, i), "\0\0\0\0\0\0\0\0", table->get_nth_col(i)->get_fixed_size());
  }

  dfield_set_data(dtuple_get_nth_field(row, C), "c", 1);
  dfield_set_data(dtuple_get_nth_field(row, 3), "d", 1);

  std::string local(REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE, 'x');

  memset(local.data() + REC_MAX_INDEX_COL_LEN, 0, BTR_EXTERN_FIELD_REF_SIZE);

  auto fb = dtuple_get_nth_field(row, B);

  dfield_set_data(fb, local.data(), local.size());
  dfield_set_ext(fb);

  ulint col = B;
  auto cache = row_ext_create(*m_sys, 1, &col, row, m_heap);
  ulint len{99};

  EXPECT_EQ(cache->len[0], 0UL);
  EXPECT_EQ(row_ext_lookup(cache, B, &len), field_ref_zero);
  EXPECT_EQ(len, 0UL);

  EXPECT_EQ(row_build_index_entry(row, cache, index(m_t, "PB"), m_heap), nullptr);

  auto entry = row_build_index_entry(row, cache, clust, m_heap);

  ASSERT_NE(entry, nullptr);
  EXPECT_TRUE(dfield_is_ext(dtuple_get_nth_field(entry, clust->get_clustered_field_pos(table->get_nth_col(B)))));

  /* Without a cache the local prefix serves. */
  auto pb = row_build_index_entry(row, nullptr, index(m_t, "PB"), m_heap);

  ASSERT_NE(pb, nullptr);
  EXPECT_EQ(bytes(dtuple_get_nth_field(pb, 0)), std::string(10, 'x'));
}

/** row_build() hands back a cache of the prefixes of the external columns
that are in an index (b, not c).
BUG: row/row0row.cc:173 `for (ulint i{}, j = 0; ...)` declares a second j
that shadows the j counting ext_cols: the outer j stays 0, *ext is always
nullptr and row_ext_create() is never called (upstream 7c21eac0 row0row.c
`for (i = j = 0; ...)`). The prefixes then come from the local 768 bytes
(enough in this format), but a reference that is not yet set is not seen
(row_build_index_entry() should return nullptr for it, see ZeroRef). */
TEST_F(RowExtTest, BuildMakesCache) {
  auto clust = index(m_t);
  size_t n{};

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    auto row = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, m_heap);
    const auto k = mach_read_from_4(reinterpret_cast<const byte *>(bytes(rec, offs, 0)->data()));

    if (!dfield_is_ext(dtuple_get_nth_field(row, B))) {
      EXPECT_EQ(ext, nullptr) << k;
      return;
    }

    ASSERT_NE(ext, nullptr) << k;
    ASSERT_EQ(ext->n_ext, 1UL);
    EXPECT_EQ(ext->ext[0], B);

    ulint len{};
    auto p = row_ext_lookup(ext, B, &len);

    ASSERT_NE(p, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(p), len), m_b[k].substr(0, REC_MAX_INDEX_COL_LEN));
    ++n;
  });

  EXPECT_GE(n, 2UL);
}

/** row_build() with no offsets computes them (the documented contract, as
upstream).
BUG: row/row0row.cc:181,186 read the fields with the parameter offsets,
not local_offsets: with offsets == nullptr it dereferences null. */
TEST_F(RowExtTest, BuildWithoutOffsets) {
  auto clust = index(m_t);

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    RowExt *ext{};
    auto a = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, m_heap);
    auto b = row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, RecOffs(nullptr), nullptr, &ext, m_heap);

    for (ulint i{}; i < dtuple_get_n_fields(a); ++i) {
      EXPECT_EQ(bytes(dtuple_get_nth_field(a, i)), bytes(dtuple_get_nth_field(b, i)));
    }
  });
}

/** An index on a column prefix longer than the 768 bytes that are kept in
the record of an external column must be refused: its entries can't be
built from the clustered record.
BUG: api/api0api.cc:919 IndexSchema::add_col() and the dictionary accept
any prefix length (the same upstream). With PB(b(1000)) and a 20000 byte b,
the delete of the row asserts in row_build_index_entry() at
row/row0row.cc:100 ut_a(ind_field->m_prefix_len <= len || ...). */
TEST_F(RowTest, PrefixLongerThanLocal) {
  const auto t = name();
  const auto err = create_table(
    *m_sys,
    t,
    {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"b", IB_BLOB, IB_COL_NONE, 0}},
    {{"PRIMARY", {{"k", 0}}, true, true}, {"PB", {{"b", 1000}}}}
  );

  EXPECT_NE(err, DB_SUCCESS);

  if (err == DB_SUCCESS) {
    const auto b = ut::blob(1, 20000);

    load(t, {[&](api::Tuple *tpl) {
      auto e = tpl->write_u32(0, 1);
      return e == DB_SUCCESS ? tpl->set_value(1, b.data(), b.size()) : e;
    }});

    auto trx = begin();

    EXPECT_EQ(remove_u32(*m_sys, t, trx, 1), DB_SUCCESS);
    rollback(trx);
  }
}

/* --- data0data: the big record conversion -------------------------------- */

/** A table (k INT UNSIGNED PK, a INT UNSIGNED, b BLOB, c BLOB, v VARCHAR(100))
for clustered index entries made by hand: (k, trx id, roll ptr, a, b, c, v). */
class BigRecTest : public RowTest {
 protected:
  void SetUp() override {
    RowTest::SetUp();

    m_t = name();

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_t,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4},
         {"a", IB_INT, IB_COL_UNSIGNED, 4},
         {"b", IB_BLOB, IB_COL_NONE, 0},
         {"c", IB_BLOB, IB_COL_NONE, 0},
         {"v", IB_VARCHAR, IB_COL_NONE, 100}},
        {{"PRIMARY", {{"k", 0}}, true, true}, {"SA", {{"a", 0}}}}
      ),
      DB_SUCCESS
    );

    m_clust = index(m_t);
    ASSERT_EQ(m_clust->get_n_fields(), 7UL);
  }

  /** @return a clustered index entry with b and c of the given values. */
  DTuple *entry(const std::string &b, const std::string &c) {
    auto e = dtuple_create(m_heap, 7);

    m_clust->copy_types(e, 7);

    auto put = [&](ulint i, const std::string &v) {
      auto p = mem_heap_alloc(m_heap, std::max<size_t>(v.size(), 1));

      memcpy(p, v.data(), v.size());
      dfield_set_data(dtuple_get_nth_field(e, i), p, v.size());
    };

    put(0, be4(1));
    put(1, std::string(DATA_TRX_ID_LEN, '\0'));
    put(2, std::string(DATA_ROLL_PTR_LEN, '\0'));
    put(3, be4(2));
    put(4, b);
    put(5, c);
    put(6, "short");

    return e;
  }

  /** Check field i of e was moved out: the 768 byte prefix of v, a zero
  reference, the external flag. */
  static void check_moved(const DTuple *e, ulint i, const std::string &v) {
    const auto f = dtuple_get_nth_field(e, i);

    ASSERT_TRUE(dfield_is_ext(f)) << i;
    ASSERT_EQ(dfield_get_len(f), REC_MAX_INDEX_COL_LEN + BTR_EXTERN_FIELD_REF_SIZE) << i;

    const auto data = static_cast<const byte *>(dfield_get_data(f));

    EXPECT_EQ(std::string(reinterpret_cast<const char *>(data), REC_MAX_INDEX_COL_LEN), v.substr(0, REC_MAX_INDEX_COL_LEN));
    EXPECT_EQ(memcmp(data + REC_MAX_INDEX_COL_LEN, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE), 0);
  }

  std::string m_t;
  Index *m_clust{};
};

/** A record that fits a page: nothing to move, an empty vector. */
TEST_F(BigRecTest, Fits) {
  auto e = entry("bb", std::string(3000, 'c'));
  ulint n_ext{};
  auto v = dtuple_convert_big_rec(m_clust, e, &n_ext);

  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->n_fields, 0UL);
  EXPECT_EQ(n_ext, 0UL);
  EXPECT_FALSE(e->has_ext());

  dtuple_convert_back_big_rec(e, v);
  EXPECT_EQ(dfield_get_len(dtuple_get_nth_field(e, 5)), 3000UL);
}

/** One long column: it alone moves, its first 768 bytes stay with a zero
reference; the record then fits; converted back, the field is the whole
value again (the same memory). */
TEST_F(BigRecTest, OneColumn) {
  const auto b = ut::blob(1, 20000);
  auto e = entry(b, "c");
  const auto orig = dfield_get_data(dtuple_get_nth_field(e, 4));
  ulint n_ext{};

  EXPECT_TRUE(page_rec_needs_ext(rec_get_converted_size(m_clust, e, 0)));

  auto v = dtuple_convert_big_rec(m_clust, e, &n_ext);

  ASSERT_NE(v, nullptr);
  ASSERT_EQ(v->n_fields, 1UL);
  EXPECT_EQ(n_ext, 1UL);
  EXPECT_EQ(v->fields[0].field_no, 4UL);
  EXPECT_EQ(v->fields[0].len, b.size() - REC_MAX_INDEX_COL_LEN);
  EXPECT_EQ(v->fields[0].data, static_cast<const byte *>(orig) + REC_MAX_INDEX_COL_LEN);
  check_moved(e, 4, b);
  EXPECT_EQ(e->get_n_ext(), 1UL);
  EXPECT_FALSE(page_rec_needs_ext(rec_get_converted_size(m_clust, e, n_ext)));

  dtuple_convert_back_big_rec(e, v);

  const auto f = dtuple_get_nth_field(e, 4);

  EXPECT_EQ(dfield_get_data(f), orig);
  EXPECT_EQ(dfield_get_len(f), b.size());
  EXPECT_FALSE(dfield_is_ext(f));
}

/** Two long columns: the longest moves first, and only as many as needed;
two that are both too long both move, the first of equal length first. */
TEST_F(BigRecTest, LongestFirst) {
  {
    const auto b = ut::blob(1, 6000);
    const auto c = ut::blob(2, 7000);
    auto e = entry(b, c);
    ulint n_ext{};
    auto v = dtuple_convert_big_rec(m_clust, e, &n_ext);

    ASSERT_NE(v, nullptr);
    ASSERT_EQ(v->n_fields, 1UL);
    EXPECT_EQ(v->fields[0].field_no, 5UL);
    check_moved(e, 5, c);
    EXPECT_FALSE(dfield_is_ext(dtuple_get_nth_field(e, 4)));
    dtuple_convert_back_big_rec(e, v);
  }
  {
    const auto b = ut::blob(1, 9000);
    const auto c = ut::blob(2, 9000);
    auto e = entry(b, c);
    ulint n_ext{};
    auto v = dtuple_convert_big_rec(m_clust, e, &n_ext);

    ASSERT_NE(v, nullptr);
    ASSERT_EQ(v->n_fields, 2UL);
    EXPECT_EQ(n_ext, 2UL);
    EXPECT_EQ(v->fields[0].field_no, 4UL);
    EXPECT_EQ(v->fields[1].field_no, 5UL);
    check_moved(e, 4, b);
    check_moved(e, 5, c);
    dtuple_convert_back_big_rec(e, v);
    EXPECT_EQ(bytes(dtuple_get_nth_field(e, 4)), b);
    EXPECT_EQ(bytes(dtuple_get_nth_field(e, 5)), c);
  }
}

/** A column already external is not moved again: n_ext counts it in. A
secondary index has nothing to move: nullptr. */
TEST_F(BigRecTest, AlreadyExternalAndSecondary) {
  const auto b = ut::blob(1, 9000);
  const auto c = ut::blob(2, 9000);
  auto e = entry(b, c);
  ulint n_ext{};
  auto v1 = dtuple_convert_big_rec(m_clust, e, &n_ext);

  ASSERT_NE(v1, nullptr);
  ASSERT_EQ(n_ext, 2UL);

  auto v2 = dtuple_convert_big_rec(m_clust, e, &n_ext);

  ASSERT_NE(v2, nullptr);
  EXPECT_EQ(v2->n_fields, 0UL);
  EXPECT_EQ(n_ext, 2UL);
  dtuple_convert_back_big_rec(e, v2);
  dtuple_convert_back_big_rec(e, v1);
  EXPECT_EQ(bytes(dtuple_get_nth_field(e, 4)), b);

  auto sa = index(m_t, "SA");
  auto se = dtuple_create(m_heap, sa->get_n_fields());

  sa->copy_types(se, sa->get_n_fields());
  set(m_heap, se, 0, DATA_INT, DATA_UNSIGNED, 4, be4(1));
  set(m_heap, se, 1, DATA_INT, DATA_UNSIGNED, 4, be4(1));
  n_ext = 0;
  EXPECT_EQ(dtuple_convert_big_rec(sa, se, &n_ext), nullptr);
}

/* --- row0prebuilt -------------------------------------------------------- */

class PrebuiltTest : public RowTest {
 protected:
  void SetUp() override {
    RowTest::SetUp();

    m_t = name();

    ASSERT_EQ(
      create_table(
        *m_sys,
        m_t,
        {{"k", IB_INT, IB_COL_UNSIGNED, 4}, {"v", IB_VARCHAR, IB_COL_NONE, 2000}, {"w", IB_INT, IB_COL_NONE, 4}},
        {{"PRIMARY", {{"k", 0}}, true, true}, {"SW", {{"w", 0}}}}
      ),
      DB_SUCCESS
    );

    std::vector<Fill> rows;

    /* Values of growing length: 1, 2, 4, ... 1024 bytes. */
    for (uint32_t k{}; k < 11; ++k) {
      rows.push_back([k](api::Tuple *tpl) {
        auto err = tpl->write_u32(0, k);
        const std::string v(size_t(1) << k, char('a' + k));

        if (err == DB_SUCCESS) err = tpl->set_value(1, v.data(), v.size());
        if (err == DB_SUCCESS) err = tpl->write_i32(2, -int32_t(k));

        return err;
      });
    }

    load(m_t, rows);

    m_table = get_table(*m_sys, m_t);
    m_sys->m_dict->table_increment_handle_count(m_table, false);
    m_prebuilt = Prebuilt::create(m_table);
  }

  void TearDown() override {
    if (m_prebuilt != nullptr) {
      Prebuilt::destroy(*m_sys, m_prebuilt, false);
    }

    RowTest::TearDown();
  }

  /** Add rec to the row cache the way RowCache::add_row() does (it is
  inline in row0sel.cc). */
  void cache_add(const Rec rec, const RecOffs &offs) {
    auto &rc = m_prebuilt->m_row_cache;

    ASSERT_EQ(rc.m_first, 0);
    ASSERT_LT(rc.m_n_cached, rc.m_n_size - 1);
    rc.m_cached_rows[rc.m_n_cached++].add_row(rec, offs);
  }

  std::string m_t;
  Table *m_table{};
  Prebuilt *m_prebuilt{};
};

/** A new prebuilt: magic numbers, the table, a search tuple of twice the
columns, a clustered reference typed as the unique fields, an empty row
cache of FETCH_CACHE_SIZE, statement start, no lock, no trx; it holds a
handle of the table until destroyed. */
TEST_F(PrebuiltTest, Create) {
  auto p = m_prebuilt;
  auto clust = m_table->get_clustered_index();

  EXPECT_EQ(p->m_magic_n, ROW_PREBUILT_ALLOCATED);
  EXPECT_EQ(p->m_magic_n2, ROW_PREBUILT_ALLOCATED);
  EXPECT_EQ(p->m_table, m_table);
  EXPECT_NE(p->m_heap, nullptr);
  EXPECT_NE(p->m_pcur, nullptr);
  EXPECT_NE(p->m_clust_pcur, nullptr);
  EXPECT_EQ(dtuple_get_n_fields(p->m_search_tuple), 2 * m_table->get_n_cols());
  ASSERT_EQ(dtuple_get_n_fields(p->m_clust_ref), clust->get_n_unique());
  EXPECT_EQ(dfield_get_type(dtuple_get_nth_field(p->m_clust_ref, 0))->mtype, DATA_INT);
  EXPECT_EQ(dfield_get_type(dtuple_get_nth_field(p->m_clust_ref, 0))->prtype & DATA_UNSIGNED, DATA_UNSIGNED);
  EXPECT_TRUE(p->m_sql_stat_start);
  EXPECT_EQ(p->m_select_lock_type, LOCK_NONE);
  EXPECT_EQ(p->m_trx, nullptr);
  EXPECT_EQ(p->m_index, nullptr);
  EXPECT_EQ(p->m_sel_graph, nullptr);
  EXPECT_EQ(p->m_old_vers_heap, nullptr);

  const auto &rc = p->m_row_cache;

  EXPECT_EQ(rc.m_n_max, FETCH_CACHE_SIZE);
  EXPECT_EQ(rc.m_n_size, FETCH_CACHE_SIZE);
  EXPECT_TRUE(rc.is_cache_empty());
  EXPECT_FALSE(rc.is_cache_full());
  EXPECT_FALSE(rc.is_cache_fetch_in_progress());
  EXPECT_EQ(rc.m_direction, ROW_SEL_UNDEFINED);

  for (ulint i{}; i < rc.m_n_max; ++i) {
    EXPECT_TRUE(rc.m_cached_rows[i].m_ptr.is_null());
    EXPECT_EQ(rc.m_cached_rows[i].m_max_len, 0U);
  }

  /* Two handles: ours and the one destroy() gives back. */
  EXPECT_GE(m_table->m_n_handles_opened, 1U);

  const auto n = m_table->m_n_handles_opened;

  m_sys->m_dict->table_increment_handle_count(m_table, false);

  auto q = Prebuilt::create(m_table);

  Prebuilt::destroy(*m_sys, q, false);
  EXPECT_EQ(m_table->m_n_handles_opened, n);
}

/** clear() resets the statement state, frees the old version heap and
drops the trx; it leaves the row cache. update_trx() sets the trx and
whether the index is usable by it. */
TEST_F(PrebuiltTest, ClearAndUpdateTrx) {
  auto p = m_prebuilt;
  auto trx = begin();

  p->m_index = m_table->get_clustered_index();
  p->update_trx(trx);
  EXPECT_EQ(p->m_trx, trx);
  EXPECT_TRUE(p->m_index_usable);

  p->m_sql_stat_start = false;
  p->m_client_has_locked = true;
  p->m_need_to_access_clustered = true;
  p->m_simple_select = true;
  p->m_select_lock_type = LOCK_X;
  p->m_old_vers_heap = mem_heap_create(100);
  p->m_row_cache.m_n_cached = 2;

  p->clear();

  EXPECT_TRUE(p->m_sql_stat_start);
  EXPECT_FALSE(p->m_client_has_locked);
  EXPECT_FALSE(p->m_need_to_access_clustered);
  EXPECT_FALSE(p->m_simple_select);
  EXPECT_FALSE(p->m_index_usable);
  EXPECT_EQ(p->m_select_lock_type, LOCK_NONE);
  EXPECT_EQ(p->m_old_vers_heap, nullptr);
  EXPECT_EQ(p->m_trx, nullptr);
  EXPECT_EQ(p->m_row_cache.m_n_cached, 2);

  p->m_row_cache.clear();
  commit(trx);
}

/** The row cache: rows come out in the order they were put in, copies of
the records; it is full at m_n_size - 1 rows; it is fetching while some
were taken and some are left; taking the last empties it and starts over
at the first slot; clear() empties it. */
TEST_F(PrebuiltTest, RowCacheOrder) {
  auto &rc = m_prebuilt->m_row_cache;
  auto clust = m_table->get_clustered_index();
  std::vector<std::string> recs;

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    if (rc.is_cache_full()) {
      return;
    }

    cache_add(rec, offs);

    const auto start = rec.get_start(offs);

    recs.emplace_back(reinterpret_cast<const char *>(start.get()), offs.size());
  });

  ASSERT_EQ(recs.size(), FETCH_CACHE_SIZE - 1);
  EXPECT_TRUE(rc.is_cache_full());
  EXPECT_FALSE(rc.is_cache_empty());
  EXPECT_FALSE(rc.is_cache_fetch_in_progress());

  for (size_t i{}; i < recs.size(); ++i) {
    auto rec = rc.cache_get_row();
    MemHeap *heap{};
    RecOffs offs = PhyRec(clust, rec).get_all_col_offsets(nullptr, &heap, Current_location());

    EXPECT_EQ(std::string(reinterpret_cast<const char *>(rec.get_start(offs).get()), offs.size()), recs[i]) << i;
    /* Prebuilt::cache_get_row() is declared (row0prebuilt.h) but defined nowhere. */
    mem_heap_free(heap);

    m_prebuilt->cache_next();

    if (i + 1 < recs.size()) {
      EXPECT_TRUE(rc.is_cache_fetch_in_progress()) << i;
      EXPECT_EQ(rc.m_first, i + 1);
    }
  }

  EXPECT_TRUE(rc.is_cache_empty());
  EXPECT_EQ(rc.m_first, 0);
  EXPECT_FALSE(rc.is_cache_fetch_in_progress());

  /* cache_next() on an empty cache does nothing. */
  rc.cache_next();
  EXPECT_EQ(rc.m_first, 0);
  EXPECT_EQ(rc.m_n_cached, 0);

  /* Fill again, take one, clear. */
  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    if (rc.m_n_cached < 2) {
      cache_add(rec, offs);
    }
  });

  rc.cache_next();
  EXPECT_EQ(rc.m_first, 1);
  rc.clear();
  EXPECT_TRUE(rc.is_cache_empty());
  EXPECT_EQ(rc.m_first, 0);
}

/** A cached row's buffer: twice the first record; a record that fits is
copied in place, a longer one gets a buffer of twice its size; the copy is
the record (the same bytes, extra and data). */
TEST_F(PrebuiltTest, CachedRowGrows) {
  auto clust = m_table->get_clustered_index();
  Prebuilt::CachedRow row{};
  std::vector<std::pair<std::string, size_t>> seen;

  for_each_rec(*m_sys, clust, [&](const Rec rec, const RecOffs &offs) {
    const auto old_ptr = row.m_ptr.get();
    const auto old_max = row.m_max_len;
    const auto size = offs.size();

    row.add_row(rec, offs);

    EXPECT_EQ(row.m_rec_len, size);
    EXPECT_GE(row.m_max_len, size);

    if (old_max >= size) {
      EXPECT_EQ(row.m_ptr.get(), old_ptr);
      EXPECT_EQ(row.m_max_len, old_max);
    } else {
      EXPECT_EQ(row.m_max_len, 2 * size);
    }

    /* The offsets are of rec (debug builds check it): compare by hand. */
    EXPECT_EQ(memcmp(row.m_rec.get() - offs.extra_size(), rec.get() - offs.extra_size(), size), 0);
    EXPECT_EQ(row.m_rec.get() - row.m_ptr.get(), ptrdiff_t(offs.extra_size()));
    seen.emplace_back("", size);
  });

  EXPECT_EQ(seen.size(), 11UL);
  mem_free(row.m_ptr.get());
}

/* --- usr0sess ------------------------------------------------------------- */

using SessTest = RowTest;

/** A session made alone: active, no trx, no graphs; destroy() frees it and
nulls the pointer. */
TEST_F(SessTest, CreateDestroy) {
  auto sess = Session::create(nullptr);

  ASSERT_NE(sess, nullptr);
  EXPECT_EQ(sess->m_state, Session::State::ACTIVE);
  EXPECT_EQ(sess->m_trx, nullptr);
  EXPECT_TRUE(sess->m_graphs.empty());

  Session::destroy(sess);
  EXPECT_EQ(sess, nullptr);
}

/** Every transaction has a session of its own that points back at it. A
graph published to a session is in its list. */
TEST_F(SessTest, TrxLinkAndGraphs) {
  auto trx = begin();
  auto trx2 = begin();

  ASSERT_NE(trx->m_sess, nullptr);
  EXPECT_EQ(trx->m_sess->m_trx, trx);
  EXPECT_EQ(trx->m_sess->m_state, Session::State::ACTIVE);
  EXPECT_NE(trx->m_sess, trx2->m_sess);

  auto sess = Session::create(trx);

  EXPECT_EQ(sess->m_trx, trx);

  auto heap = mem_heap_create(256);
  auto g1 = que_fork_create(nullptr, nullptr, QUE_FORK_USER_INTERFACE, heap);
  auto g2 = que_fork_create(nullptr, nullptr, QUE_FORK_USER_INTERFACE, heap);

  mutex_enter(&m_sys->m_trx_sys->m_mutex);
  que_graph_publish(*m_sys, g1, sess);
  que_graph_publish(*m_sys, g2, sess);
  mutex_exit(&m_sys->m_trx_sys->m_mutex);

  ASSERT_EQ(UT_LIST_GET_LEN(sess->m_graphs), 2UL);
  EXPECT_EQ(UT_LIST_GET_FIRST(sess->m_graphs), g1);
  EXPECT_EQ(UT_LIST_GET_LAST(sess->m_graphs), g2);

  UT_LIST_REMOVE(sess->m_graphs, g1);
  UT_LIST_REMOVE(sess->m_graphs, g2);
  mem_heap_free(heap);

  sess->m_trx = nullptr;
  Session::destroy(sess);

  commit(trx);
  commit(trx2);
}

/* --- row0misc ------------------------------------------------------------- */

using MiscTest = RowTest;

/** row_create_tempfile(): an unnamed file in $TMPDIR (no link), read and
written; none in a directory that does not exist. */
TEST_F(MiscTest, TempFile) {
  for (const char *prefix : {"ibtest", static_cast<const char *>(nullptr)}) {
    const auto fd = row_create_tempfile(prefix);

    ASSERT_GE(fd, 0);

    struct stat st {};

    ASSERT_EQ(::fstat(fd, &st), 0);
    EXPECT_EQ(st.st_nlink, 0U);
    EXPECT_EQ(::pwrite(fd, "hello", 5, 4096), 5);

    std::array<char, 5> buf{};

    EXPECT_EQ(::pread(fd, buf.data(), 5, 4096), 5);
    EXPECT_EQ(std::string(buf.data(), 5), "hello");
    ::close(fd);
  }

  const std::string old = ::getenv("TMPDIR") != nullptr ? ::getenv("TMPDIR") : "";

  ::setenv("TMPDIR", "/nonexistent-dir-of-test_data", 1);
  EXPECT_EQ(row_create_tempfile("x"), -1);
  ::setenv("TMPDIR", old.c_str(), 1);
}

/** Each error has its own string; an unknown code says so. */
TEST_F(MiscTest, Strerror) {
  std::set<std::string> seen;

  for (auto err : {DB_SUCCESS, DB_PANIC, DB_ERROR, DB_INTERRUPTED, DB_OUT_OF_MEMORY, DB_OUT_OF_FILE_SPACE, DB_LOCK_WAIT,
                   DB_DEADLOCK, DB_ROLLBACK, DB_DUPLICATE_KEY, DB_QUE_THR_SUSPENDED, DB_MISSING_HISTORY, DB_CLUSTER_NOT_FOUND,
                   DB_TABLE_NOT_FOUND, DB_MUST_GET_MORE_FILE_SPACE, DB_TABLE_EXISTS, DB_TOO_BIG_RECORD, DB_LOCK_WAIT_TIMEOUT,
                   DB_NO_REFERENCED_ROW, DB_ROW_IS_REFERENCED, DB_CANNOT_ADD_CONSTRAINT, DB_CORRUPTION,
                   DB_COL_APPEARS_TWICE_IN_INDEX, DB_CANNOT_DROP_CONSTRAINT, DB_NO_SAVEPOINT, DB_TABLESPACE_ALREADY_EXISTS,
                   DB_TABLESPACE_DELETED, DB_LOCK_TABLE_FULL, DB_FOREIGN_DUPLICATE_KEY, DB_TOO_MANY_CONCURRENT_TRXS,
                   DB_UNSUPPORTED, DB_PRIMARY_KEY_IS_NULL, DB_FATAL, DB_FAIL, DB_OVERFLOW, DB_UNDERFLOW, DB_STRONG_FAIL,
                   DB_RECORD_NOT_FOUND, DB_END_OF_INDEX, DB_SCHEMA_ERROR, DB_DATA_MISMATCH, DB_SCHEMA_NOT_LOCKED, DB_NOT_FOUND,
                   DB_READONLY, DB_INVALID_INPUT, DB_OUT_OF_RESOURCES, DB_INDEX_CORRUPT, DB_DDL_IN_PROGRESS}) {
    const std::string s = db_strerror(err);

    EXPECT_NE(s, "Unknown error") << int(err);
    EXPECT_TRUE(seen.insert(s).second) << int(err) << " " << s;
  }

  EXPECT_STREQ(db_strerror(DbErr(12345)), "Unknown error");
}

/** A table (k INT UNSIGNED PK) for the error handling and lock tests. */
class MiscTableTest : public MiscTest {
 protected:
  void SetUp() override {
    MiscTest::SetUp();
    m_t = name();
    ASSERT_EQ(create_table(*m_sys, m_t, {{"k", IB_INT, IB_COL_UNSIGNED, 4}}, {{"PRIMARY", {{"k", 0}}, true, true}}), DB_SUCCESS);
  }

  DbErr put(Trx *trx, uint32_t k) {
    return insert(*m_sys, m_t, trx, [k](api::Tuple *tpl) { return tpl->write_u32(0, k); });
  }

  size_t rows() { return n_recs_visible(); }

  /** @return the number of rows a new transaction sees. */
  size_t n_recs_visible() {
    auto trx = begin();
    api::Cursor *crsr{};
    size_t n{};

    EXPECT_EQ(api::cursor_open_table(*m_sys, m_t.c_str(), trx, &crsr), DB_SUCCESS);

    for (auto err = crsr->first(*m_sys); err == DB_SUCCESS; err = crsr->next(*m_sys)) {
      ++n;
    }

    (void)api::Cursor::close(*m_sys, crsr);
    commit(trx);

    return n;
  }

  std::string m_t;
};

/** An error that needs no rollback without a savepoint: reported, the error
state cleared, no retry. DB_CORRUPTION likewise (logged). */
TEST_F(MiscTableTest, HandleErrorsReport) {
  for (auto e : {DB_DUPLICATE_KEY, DB_TOO_BIG_RECORD, DB_CORRUPTION, DB_INTERRUPTED}) {
    auto trx = begin();
    DbErr err{DB_SUCCESS};

    ASSERT_EQ(put(trx, 1), DB_SUCCESS);
    trx->m_error_state = e;
    EXPECT_FALSE(row_handle_errors(*m_sys, &err, trx, nullptr, nullptr));
    EXPECT_EQ(err, e);
    EXPECT_EQ(trx->m_error_state, DB_SUCCESS);
    rollback(trx);
  }

  EXPECT_EQ(rows(), 0UL);
}

/** A duplicate key with a savepoint rolls back to it: the work before
stays. */
TEST_F(MiscTableTest, HandleErrorsStatementRollback) {
  auto trx = begin();

  ASSERT_EQ(put(trx, 1), DB_SUCCESS);

  auto savept = trx_savept_take(trx);

  ASSERT_EQ(put(trx, 2), DB_SUCCESS);
  ASSERT_EQ(put(trx, 3), DB_SUCCESS);

  DbErr err{};

  trx->m_error_state = DB_DUPLICATE_KEY;
  EXPECT_FALSE(row_handle_errors(*m_sys, &err, trx, nullptr, &savept));
  EXPECT_EQ(err, DB_DUPLICATE_KEY);
  commit(trx);

  EXPECT_EQ(rows(), 1UL);
}

/** A deadlock rolls back the whole transaction. */
TEST_F(MiscTableTest, HandleErrorsDeadlock) {
  auto trx = begin();

  ASSERT_EQ(put(trx, 1), DB_SUCCESS);
  ASSERT_EQ(put(trx, 2), DB_SUCCESS);

  DbErr err{};

  trx->m_error_state = DB_DEADLOCK;
  EXPECT_FALSE(row_handle_errors(*m_sys, &err, trx, nullptr, nullptr));
  EXPECT_EQ(err, DB_DEADLOCK);
  EXPECT_EQ(rows(), 0UL);
  rollback(trx);
}

/** row_lock_table_with_retry(): a lock granted at once; a conflicting one
waits until the holder commits, then is granted; a weaker lock of the same
trx is granted. */
TEST_F(MiscTableTest, LockTableWithRetry) {
  auto table = get_table(*m_sys, m_t);
  auto t1 = begin();
  auto t2 = begin();

  ASSERT_EQ(row_lock_table_with_retry(*m_sys, t1, table, LOCK_X), DB_SUCCESS);
  EXPECT_EQ(row_lock_table_with_retry(*m_sys, t1, table, LOCK_IS), DB_SUCCESS);

  const auto start = std::chrono::steady_clock::now();
  auto waiter = std::async(std::launch::async, [&] { return row_lock_table_with_retry(*m_sys, t2, table, LOCK_S); });

  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(300)), std::future_status::timeout);
  commit(t1);

  ASSERT_EQ(waiter.wait_for(std::chrono::seconds(20)), std::future_status::ready);
  EXPECT_EQ(waiter.get(), DB_SUCCESS);
  EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(300));
  commit(t2);
}

/** The statistics are computed again once more than 16 + n_rows / 16 rows
were changed: the counter starts over. */
TEST_F(MiscTableTest, UpdateStatistics) {
  auto table = get_table(*m_sys, m_t);

  table->m_stats.m_n_rows = 160;
  table->m_stats.m_modified_counter = 0;

  for (int i = 0; i < 27; ++i) {
    row_update_statistics_if_needed(*m_sys, table);
  }

  EXPECT_EQ(table->m_stats.m_modified_counter, 27UL);

  row_update_statistics_if_needed(*m_sys, table);
  EXPECT_EQ(table->m_stats.m_modified_counter, 0UL);
}

/* --- que0que -------------------------------------------------------------- */

using QueTest = RowTest;

/** A fork made by hand is its own graph unless it has one; a thread joins
its fork's list in the command wait state. */
TEST_F(QueTest, ForkAndThr) {
  auto heap = mem_heap_create(512);
  auto root = que_fork_create(nullptr, nullptr, QUE_FORK_PROCEDURE, heap);

  EXPECT_EQ(que_node_get_type(root), QUE_NODE_FORK);
  EXPECT_EQ(root->graph, root);
  EXPECT_EQ(root->state, QUE_FORK_COMMAND_WAIT);
  EXPECT_EQ(root->fork_type, QUE_FORK_PROCEDURE);
  EXPECT_EQ(root->n_active_thrs, 0UL);
  EXPECT_EQ(root->heap, heap);
  EXPECT_TRUE(root->thrs.empty());
  EXPECT_EQ(que_node_get_parent(root), nullptr);

  auto t1 = que_thr_create(root, heap);
  auto t2 = que_thr_create(root, heap);

  EXPECT_EQ(que_node_get_type(t1), QUE_NODE_THR);
  EXPECT_EQ(t1->magic_n, QUE_THR_MAGIC_N);
  EXPECT_EQ(t1->graph, root);
  EXPECT_EQ(t1->state, QUE_THR_COMMAND_WAIT);
  EXPECT_FALSE(t1->is_active);
  EXPECT_EQ(t1->run_node, nullptr);
  EXPECT_EQ(que_node_get_parent(t1), root);
  EXPECT_EQ(que_fork_get_first_thr(root), t1);
  EXPECT_EQ(UT_LIST_GET_NEXT(thrs, t1), t2);
  EXPECT_EQ(UT_LIST_GET_LEN(root->thrs), 2UL);

  auto child = que_fork_create(root, t1, QUE_FORK_SELECT_NON_SCROLL, heap);

  EXPECT_EQ(child->graph, root);
  EXPECT_EQ(que_node_get_parent(child), t1);
  EXPECT_TRUE(que_graph_is_select(child));
  EXPECT_FALSE(que_graph_is_select(root));

  t1->child = nullptr;
  t2->child = nullptr;
  que_graph_free_recursive(root);
  EXPECT_EQ(t1->magic_n, QUE_THR_MAGIC_FREED);
  EXPECT_EQ(t2->magic_n, QUE_THR_MAGIC_FREED);
  mem_heap_free(heap);
}

/** que_fork_start_command(): the first thread waiting for a command starts
(running, active, counted in the graph and the trx); else a suspended
one, else a completed one; stopping it for the client completes it and
gives the counts back. */
TEST_F(QueTest, StartCommand) {
  auto trx = begin();
  auto heap = mem_heap_create(512);
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_USER_INTERFACE, heap);
  auto t1 = que_thr_create(fork, heap);
  auto t2 = que_thr_create(fork, heap);

  t1->child = t2->child = nullptr;
  fork->trx = trx;

  const auto n_active = trx->m_n_active_thrs;

  auto thr = que_fork_start_command(fork);

  ASSERT_EQ(thr, t1);
  EXPECT_EQ(fork->state, QUE_FORK_ACTIVE);
  EXPECT_EQ(thr->state, QUE_THR_RUNNING);
  EXPECT_TRUE(thr->is_active);
  EXPECT_EQ(thr->run_node, thr);
  EXPECT_EQ(thr->prev_node, fork);
  EXPECT_EQ(fork->n_active_thrs, 1UL);
  EXPECT_EQ(trx->m_n_active_thrs, n_active + 1);

  que_thr_stop_for_client_no_error(thr, trx);
  EXPECT_EQ(thr->state, QUE_THR_COMPLETED);
  EXPECT_FALSE(thr->is_active);
  EXPECT_EQ(fork->n_active_thrs, 0UL);
  EXPECT_EQ(trx->m_n_active_thrs, n_active);

  /* t2 still waits for a command: it goes first. */
  thr = que_fork_start_command(fork);
  ASSERT_EQ(thr, t2);
  que_thr_stop_for_client_no_error(thr, trx);

  /* Both completed: the first completed one starts again. */
  thr = que_fork_start_command(fork);
  ASSERT_EQ(thr, t1);
  que_thr_stop_for_client_no_error(thr, trx);

  /* A suspended one is preferred to a completed one. */
  t2->state = QUE_THR_SUSPENDED;
  thr = que_fork_start_command(fork);
  ASSERT_EQ(thr, t2);
  EXPECT_EQ(thr->state, QUE_THR_RUNNING);

  /* que_thr_stop_client() of a running thread without an error leaves it. */
  que_thr_stop_client(*m_sys, thr);
  EXPECT_EQ(thr->state, QUE_THR_RUNNING);
  EXPECT_TRUE(thr->is_active);
  que_thr_stop_for_client_no_error(thr, trx);

  /* Moved to the run state for the client, and stopped. */
  que_thr_move_to_run_state_for_client(t1, trx);
  EXPECT_EQ(t1->state, QUE_THR_RUNNING);
  EXPECT_EQ(trx->m_n_active_thrs, n_active + 1);
  que_thr_stop_for_client_no_error(t1, trx);
  EXPECT_EQ(trx->m_n_active_thrs, n_active);

  /* A graph waiting for a command stops its thread: suspended. */
  fork->state = QUE_FORK_COMMAND_WAIT;
  mutex_enter(&m_sys->m_trx_sys->m_mutex);
  /* The state of the query thread: the trx mutex's too (lock0lock.h). */
  trx_mutex_enter(trx);
  EXPECT_TRUE(que_thr_stop(*m_sys, t1));
  trx_mutex_exit(trx);
  mutex_exit(&m_sys->m_trx_sys->m_mutex);
  EXPECT_EQ(t1->state, QUE_THR_SUSPENDED);

  que_graph_free_recursive(fork);
  mem_heap_free(heap);
  commit(trx);
}

/** The node lists: add at the end, length, next, parent. */
TEST_F(QueTest, NodeList) {
  auto heap = mem_heap_create(512);
  std::vector<QueCommon *> nodes;

  for (int i = 0; i < 4; ++i) {
    auto n = reinterpret_cast<QueCommon *>(mem_heap_zalloc(heap, sizeof(QueCommon)));

    n->type = QUE_NODE_EXIT;
    nodes.push_back(n);
  }

  EXPECT_EQ(que_node_list_get_len(nullptr), 0UL);

  que_node_t *list = que_node_list_add_last(nullptr, nodes[0]);

  EXPECT_EQ(list, nodes[0]);
  EXPECT_EQ(que_node_list_get_len(list), 1UL);

  for (int i = 1; i < 4; ++i) {
    EXPECT_EQ(que_node_list_add_last(list, nodes[i]), list);
  }

  EXPECT_EQ(que_node_list_get_len(list), 4UL);

  que_node_t *n = list;

  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(n, nodes[i]);
    n = que_node_get_next(n);
  }

  EXPECT_EQ(n, nullptr);

  que_node_set_parent(nodes[2], nodes[0]);
  EXPECT_EQ(que_node_get_parent(nodes[2]), nodes[0]);
  que_node_set_val_buf_size(nodes[1], 17);
  EXPECT_EQ(que_node_get_val_buf_size(nodes[1]), 17UL);
  EXPECT_EQ(que_node_get_val(nodes[1]), &nodes[1]->val);
  EXPECT_EQ(que_node_get_data_type(nodes[1]), &nodes[1]->val.type);

  mem_heap_free(heap);
}

/** A parsed procedure. */
class Parsed {
 public:
  Parsed(System &sys, const std::string &sql, ParsInfo *info = nullptr) {
    sys.m_dict->mutex_acquire();
    m_graph = pars_sql(sys, info != nullptr ? info : pars_info_create(), sql.c_str());
    sys.m_dict->mutex_release();
  }

  ~Parsed() { que_graph_free(m_graph); }

  /** @return the procedure node. */
  ProcNode *proc() const { return static_cast<ProcNode *>(que_fork_get_child(m_graph)); }

  /** @return the n-th statement of the procedure. */
  que_node_t *stat(ulint n) const {
    auto s = proc()->stat_list;

    while (n-- > 0) {
      s = que_node_get_next(s);
    }

    return s;
  }

  QueFork *m_graph{};
};

/** The graph of a procedure: a fork, its thread, the procedure node, its
statements in a list with the procedure as parent. The loop that contains
an EXIT is the innermost FOR or WHILE above it; outside a loop, none. */
TEST_F(QueTest, ParsedShapeAndLoops) {
  Parsed p(
    *m_sys,
    "PROCEDURE P () IS\n"
    "i INT;\n"
    "j INT;\n"
    "BEGIN\n"
    "  i := 0;\n"
    "  WHILE i < 3 LOOP\n"
    "    FOR j IN 1 .. 2 LOOP\n"
    "      IF (j = 2) THEN\n"
    "        EXIT;\n"
    "      END IF;\n"
    "    END LOOP;\n"
    "    i := i + 1;\n"
    "  END LOOP;\n"
    "  RETURN;\n"
    "END;\n"
  );

  ASSERT_NE(p.m_graph, nullptr);
  EXPECT_EQ(que_node_get_type(p.m_graph), QUE_NODE_FORK);

  auto thr = que_fork_get_first_thr(p.m_graph);
  auto proc = p.proc();

  ASSERT_EQ(que_node_get_type(proc), QUE_NODE_PROC);
  EXPECT_EQ(que_node_get_parent(proc), thr);
  EXPECT_EQ(que_node_list_get_len(proc->stat_list), 3UL);

  auto assign = p.stat(0);
  auto loop = static_cast<WhileNode *>(p.stat(1));
  auto ret = p.stat(2);

  EXPECT_EQ(que_node_get_type(assign), QUE_NODE_ASSIGNMENT);
  ASSERT_EQ(que_node_get_type(loop), QUE_NODE_WHILE);
  EXPECT_EQ(que_node_get_type(ret), QUE_NODE_RETURN);
  EXPECT_EQ(que_node_get_parent(assign), proc);

  auto for_node = static_cast<ForNode *>(loop->stat_list);

  ASSERT_EQ(que_node_get_type(for_node), QUE_NODE_FOR);
  EXPECT_EQ(que_node_get_parent(for_node), loop);

  auto if_node = static_cast<IfNode *>(for_node->stat_list);

  ASSERT_EQ(que_node_get_type(if_node), QUE_NODE_IF);

  auto exit_node = if_node->stat_list;

  ASSERT_EQ(que_node_get_type(exit_node), QUE_NODE_EXIT);
  EXPECT_EQ(que_node_get_containing_loop_node(exit_node), for_node);
  EXPECT_EQ(que_node_get_containing_loop_node(if_node), for_node);
  EXPECT_EQ(que_node_get_containing_loop_node(for_node), loop);
  EXPECT_EQ(que_node_get_containing_loop_node(ret), nullptr);
  EXPECT_EQ(que_node_get_containing_loop_node(assign), nullptr);
}

/** A procedure run by que_eval_sql() many times: each graph is freed, the
error state stays DB_SUCCESS. */
TEST_F(QueTest, EvalSqlRepeated) {
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

  for (int i = 0; i < 200; ++i) {
    auto info = pars_info_create();

    pars_info_add_int4_literal(info, "n", i);
    ASSERT_EQ(
      que_eval_sql(
        *m_sys, info, "PROCEDURE P () IS\ns CHAR;\nk INT;\nBEGIN\nk := :n * 2;\ns := CONCAT('a', TO_CHAR(k));\nEND;\n", false, trx
      ),
      DB_SUCCESS
    );
    EXPECT_EQ(trx->m_error_state, DB_SUCCESS);
  }

  commit(trx);
}

/* --- eval0eval / eval0proc ------------------------------------------------ */

/** A value handed back by a procedure: nullopt is SQL NULL. */
using Val = std::optional<std::string>;

/** The rows handed back by a procedure. */
using ValRows = std::vector<std::vector<Val>>;

/** @return 1 if v, else 0. */
constexpr int32_t c_val(bool v) {
  return v ? 1 : 0;
}

/** The bound function "out": appends the select list values of the cursor row. */
void *out(void *arg, void *user_arg) {
  auto sel = static_cast<SelNode *>(arg);
  auto rows = static_cast<ValRows *>(user_arg);
  std::vector<Val> row;

  for (auto exp = sel->m_select_list; exp != nullptr; exp = que_node_get_next(exp)) {
    row.emplace_back(bytes(que_node_get_val(exp)));
  }

  rows->push_back(std::move(row));

  return arg;
}

/** @return the 4 byte integer v as the evaluator holds it: big endian, two's
complement (not the sign flipped storage of a signed column). */
Val i4(int32_t v) {
  return be4(uint32_t(v));
}

class EvalTest : public RowTest {
 protected:
  /** Run the body of a procedure with the INT variables ints and the CHAR
  variables chars; "EMIT;" in body hands back the values of the variables
  emit (a comma separated list) as a row.
  @return the error */
  DbErr run(const std::string &vars, const std::string &emit, const std::string &body, ParsInfo *info = nullptr) {
    auto trx = begin();

    EXPECT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

    if (info == nullptr) {
      info = pars_info_create();
    }

    pars_info_add_function(info, "out", out, &m_rows);

    std::string b = body;

    for (size_t pos; (pos = b.find("EMIT;")) != std::string::npos;) {
      b.replace(pos, 5, "OPEN emit; FETCH emit INTO out(); CLOSE emit;");
    }

    const auto sql = std::format(
      "PROCEDURE P () IS\n{}\n"
      "DECLARE FUNCTION out;\n"
      "DECLARE CURSOR emit IS SELECT {} FROM SYS_TABLES WHERE NAME >= 'SYS_FOREIGN' AND NAME <= 'SYS_FOREIGN';\n"
      "BEGIN\n{}\nEND;\n",
      vars,
      emit,
      b
    );

    const auto err = que_eval_sql(*m_sys, info, sql.c_str(), false, trx);

    if (err == DB_SUCCESS) {
      commit(trx);
    } else {
      rollback(trx);
    }

    return err;
  }

  ValRows m_rows;
};

/** The arithmetic of 4 byte integers: wraps around at the limits (computed
in 64 bits, stored in 32), division truncates toward zero, unary minus. */
TEST_F(EvalTest, ArithmeticEdges) {
  ASSERT_EQ(
    run("a INT; b INT; c INT; d INT; e INT; f INT; g INT; h INT; i INT; j INT; k INT; l INT;",
        "a, b, c, d, e, f, g, h, i, j, k, l",
        "a := 2147483647 + 1;\n"
        "b := -2147483647 - 2;\n"
        "c := 65536 * 65536;\n"
        "d := -7 / 2;\n"
        "e := 7 / -2;\n"
        "f := -7 / -2;\n"
        "g := (-2147483647 - 1) / -1;\n"
        "h := -(-5);\n"
        "i := 0 - (-2147483647 - 1);\n"
        "j := 46341 * 46341;\n"
        "k := 0 / 5;\n"
        "l := -1 * -1;\n"
        "EMIT;"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(
    m_rows[0],
    (std::vector<Val>{
      i4(INT32_MIN), i4(INT32_MAX), i4(0), i4(-3), i4(-3), i4(3), i4(INT32_MIN), i4(5), i4(INT32_MIN),
      i4(int32_t(uint32_t(46341) * 46341u)), i4(0), i4(1)})
  );
}

/** Comparisons of integers of the same sign, and of strings. */
TEST_F(EvalTest, CmpSameSign) {
  struct C {
    const char *m_exp;
    bool m_val;
  };

  const std::vector<C> cases{
    {"1 < 2", true},       {"2 < 1", false},        {"2 > 1", true},        {"1 >= 1", true},       {"1 <= 0", false},
    {"0 = 0", true},       {"0 <> 0", false},       {"-5 < -3", true},      {"-3 < -5", false},     {"-1 = -1", true},
    {"-2 >= -2", true},    {"-2147483647 < -1", true}, {"2147483647 > 0", true}, {"'a' < 'b'", true}, {"'ab' > 'a'", true},
    {"'' < 'a'", true},    {"'a' = 'a'", true},
    /* Strings are VARCHAR: compared padded with spaces. */
    {"'a ' = 'a'", true},  {"'a' < 'a '", false},  {"'' = ' '", true},
  };

  std::string body;

  for (const auto &c : cases) {
    body += std::format("r := 0; IF ({}) THEN r := 1; END IF; EMIT;\n", c.m_exp);
  }

  ASSERT_EQ(run("r INT;", "r", body), DB_SUCCESS);
  ASSERT_EQ(m_rows.size(), cases.size());

  for (size_t i{}; i < cases.size(); ++i) {
    EXPECT_EQ(m_rows[i][0], i4(c_val(cases[i].m_val))) << cases[i].m_exp;
  }
}

/** Comparisons of a negative and a non-negative integer: by value, not by bytes. */
TEST_F(EvalTest, CmpAcrossZero) {
  struct C {
    const char *m_exp;
    bool m_val;
  };

  const std::vector<C> cases{
    {"-1 < 0", true},
    {"-1 > 0", false},
    {"0 > -1", true},
    {"-2147483647 - 1 < 2147483647", true},
    {"-1 <= 1", true},
    {"1 >= -1", true},
    {"-5 < 3", true},
    {":neg < :pos", true},
    {":neg - :pos < 0", true},
    {"3000000000 > 2147483647", true},
    {"-3000000000 < 0", true},
    {"-2147483648 < 0", true},
  };

  std::string body;

  for (const auto &c : cases) {
    body += std::format("r := 0; IF ({}) THEN r := 1; END IF; EMIT;\n", c.m_exp);
  }

  auto info = pars_info_create();

  pars_info_add_int4_literal(info, "neg", -7);
  pars_info_add_int4_literal(info, "pos", 7);

  ASSERT_EQ(run("r INT;", "r", body, info), DB_SUCCESS);
  ASSERT_EQ(m_rows.size(), cases.size());

  for (size_t i{}; i < cases.size(); ++i) {
    EXPECT_EQ(m_rows[i][0], i4(c_val(cases[i].m_val))) << cases[i].m_exp;
  }
}

/** AND, OR, NOT over every combination, and nested. */
TEST_F(EvalTest, Logic) {
  std::string body;

  for (int a = 0; a < 2; ++a) {
    for (int b = 0; b < 2; ++b) {
      body += std::format(
        "x := 0; y := 0; z := 0;\n"
        "IF ({} = 1) AND ({} = 1) THEN x := 1; END IF;\n"
        "IF ({} = 1) OR ({} = 1) THEN y := 1; END IF;\n"
        "IF NOT ({} = 1) THEN z := 1; END IF;\n"
        "EMIT;\n",
        a, b, a, b, a
      );
    }
  }

  body += "x := 0; IF NOT (NOT (1 = 1)) AND (NOT (1 = 2) OR (1 = 2)) THEN x := 1; END IF; EMIT;\n";

  ASSERT_EQ(run("x INT; y INT; z INT;", "x, y, z", body), DB_SUCCESS);
  ASSERT_EQ(m_rows.size(), 5UL);

  int r{};

  for (int a = 0; a < 2; ++a) {
    for (int b = 0; b < 2; ++b, ++r) {
      EXPECT_EQ(m_rows[r], (std::vector<Val>{i4(a & b), i4(a | b), i4(!a)})) << a << b;
    }
  }

  EXPECT_EQ(m_rows[4][0], i4(1));
}

/** TO_CHAR and TO_NUMBER at the limits and with signs. */
TEST_F(EvalTest, ToCharToNumber) {
  auto info = pars_info_create();

  pars_info_add_int4_literal(info, "mn", INT32_MIN);
  pars_info_add_int4_literal(info, "mx", INT32_MAX);

  ASSERT_EQ(
    run("a CHAR; b CHAR; c CHAR; d CHAR; e CHAR; n INT; m INT; s INT;",
        "a, b, c, d, e, n, m, s",
        "a := TO_CHAR(:mn);\n"
        "b := TO_CHAR(:mx);\n"
        "c := TO_CHAR(0);\n"
        "d := TO_CHAR(-1);\n"
        "e := TO_CHAR(10);\n"
        "n := TO_NUMBER('-42');\n"
        "m := TO_NUMBER('2147483647');\n"
        "s := :mn + :mx;\n"
        "EMIT;",
        info),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(
    m_rows[0],
    (std::vector<Val>{Val("-2147483648"), Val("2147483647"), Val("0"), Val("-1"), Val("10"), i4(-42), i4(INT32_MAX), i4(-1)})
  );
}

/** TO_NUMBER of a value that is part of a longer string.
BUG: eval/eval0eval.cc:640 TO_NUMBER is atoi() of the data: it ignores the
length of the value and reads on to a NUL. TO_NUMBER(SUBSTR('12345', 0, 2))
is 12345, not 12 (and a value not followed by a NUL is read past its end).
The same in upstream. Fix: convert at most dfield_get_len() bytes (copy to a
NUL terminated buffer, or std::from_chars). */
TEST_F(EvalTest, ToNumberOfSubstr) {
  ASSERT_EQ(run("n INT;", "n", "n := TO_NUMBER(SUBSTR('12345', 0, 2));\nEMIT;"), DB_SUCCESS);
  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(12));
}

/** The string functions at their edges: empty strings, the ends, a match at
the start, the end, overlapping, none, longer than the string. */
TEST_F(EvalTest, StringEdges) {
  ASSERT_EQ(
    run("s CHAR; e CHAR; l0 INT; c1 CHAR; s0 CHAR; s1 CHAR; s2 CHAR; i1 INT; i2 INT; i3 INT; i4 INT; i5 INT; i6 INT;",
        "l0, c1, s0, s1, s2, i1, i2, i3, i4, i5, i6",
        "s := 'abcabd';\n"
        "e := '';\n"
        "l0 := LENGTH(e);\n"
        "c1 := CONCAT(e, 'x', e, s);\n"
        "s0 := SUBSTR(s, 0, 0);\n"
        "s1 := SUBSTR(s, 5, 1);\n"
        "s2 := SUBSTR(s, 0, 6);\n"
        "i1 := INSTR(s, 'abc');\n"
        "i2 := INSTR(s, 'abd');\n"
        "i3 := INSTR(s, 'd');\n"
        "i4 := INSTR(s, 'abcabdx');\n"
        "i5 := INSTR('aab', 'ab');\n"
        "i6 := INSTR(s, 'x');\n"
        "EMIT;"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(
    m_rows[0],
    (std::vector<Val>{i4(0), Val("xabcabd"), Val(""), Val("d"), Val("abcabd"), i4(1), i4(4), i4(6), i4(0), i4(2), i4(0)})
  );
}

/** TO_BINARY of an integer takes its last n bytes (a CHAR); BINARY_TO_NUMBER
takes 1 to 4 bytes big endian; of a string TO_BINARY is the string (a
BINARY: a variable of that type needs a length, so it is looked into). */
TEST_F(EvalTest, Binary) {
  ASSERT_EQ(
    run("b1 CHAR; b4 CHAR; bm CHAR; n1 INT; n4 INT; nm INT; bs INT;",
        "b1, b4, bm, n1, n4, nm, bs",
        "b1 := TO_BINARY(258, 1);\n"
        "b4 := TO_BINARY(16909060, 4);\n"
        "bm := TO_BINARY(-1, 4);\n"
        "n1 := BINARY_TO_NUMBER(b1);\n"
        "n4 := BINARY_TO_NUMBER(b4);\n"
        "nm := BINARY_TO_NUMBER(bm);\n"
        "bs := INSTR(TO_BINARY('xyz'), 'z');\n"
        "EMIT;"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(
    m_rows[0],
    (std::vector<Val>{
      Val(std::string("\x02", 1)), Val(std::string("\x01\x02\x03\x04", 4)), Val(std::string("\xff\xff\xff\xff", 4)), i4(2),
      i4(16909060), i4(-1), i4(3)})
  );
}

/** A CHAR variable assigned longer, shorter and empty values, and an INT
variable assigned from an expression of itself. */
TEST_F(EvalTest, Assignments) {
  ASSERT_EQ(
    run("s CHAR; n INT;",
        "s, n",
        "s := 'a'; n := 1; EMIT;\n"
        "s := 'abcdefghijklmnopqrstuvwxyz'; n := n * 10 + n; EMIT;\n"
        "s := 'xy'; n := -n; EMIT;\n"
        "s := ''; n := n - n; EMIT;\n"
        "s := CONCAT(s, s, 'q'); EMIT;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 5UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{Val("a"), i4(1)}));
  EXPECT_EQ(m_rows[1], (std::vector<Val>{Val("abcdefghijklmnopqrstuvwxyz"), i4(11)}));
  EXPECT_EQ(m_rows[2], (std::vector<Val>{Val("xy"), i4(-11)}));
  EXPECT_EQ(m_rows[3], (std::vector<Val>{Val(""), i4(0)}));
  EXPECT_EQ(m_rows[4], (std::vector<Val>{Val("q"), i4(0)}));
}

/** The control flow: a FOR whose range is empty runs no time, a FOR over
negative bounds, a FOR of one value; the loop variable after the loop; a
WHILE whose condition is false; an EXIT leaves only the innermost loop; an
IF with ELSIFs none of which holds and no ELSE does nothing; RETURN from
inside nested loops ends the procedure. */
TEST_F(EvalTest, ControlFlow) {
  ASSERT_EQ(
    run("i INT; n INT; m INT; w INT; x INT; y INT; z INT;",
        "n, m, w, x, y, z",
        "n := 0; m := 0; w := 0; x := 0; y := 0; z := 0;\n"
        "FOR i IN 5 .. 4 LOOP n := n + 1; END LOOP;\n"
        "FOR i IN -3 .. -1 LOOP m := m + i; END LOOP;\n"
        "FOR i IN 7 .. 7 LOOP w := w + i; END LOOP;\n"
        "WHILE 1 = 2 LOOP x := 99; END LOOP;\n"
        "WHILE x < 3 LOOP\n"
        "  x := x + 1;\n"
        "  WHILE 1 = 1 LOOP y := y + 1; EXIT; END LOOP;\n"
        "END LOOP;\n"
        "IF (x = 0) THEN z := 1; ELSIF (x = 1) THEN z := 2; ELSIF (x = 2) THEN z := 3; END IF;\n"
        "EMIT;\n"
        "WHILE 1 = 1 LOOP\n"
        "  FOR i IN 1 .. 10 LOOP\n"
        "    IF (i = 3) THEN RETURN; END IF;\n"
        "    z := z + 1;\n"
        "  END LOOP;\n"
        "END LOOP;\n"
        "EMIT;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{i4(0), i4(-6), i4(7), i4(3), i4(3), i4(0)}));
}

/** RND(a, a) is a; RND(a, b) is in [a, b]; RND_STR(n) is n of 'a'..'c';
SYSDATE() is the time now. */
TEST_F(EvalTest, RndAndSysdate) {
  std::string body = "r := RND(3, 3); s := RND_STR(5); t := SYSDATE(); EMIT;\n";

  for (int i = 0; i < 20; ++i) {
    body += "r := RND(1, 6); s := RND_STR(0); EMIT;\n";
  }

  const auto now = int32_t(::time(nullptr));

  ASSERT_EQ(run("r INT; s CHAR; t INT;", "r, s, t", body), DB_SUCCESS);
  ASSERT_EQ(m_rows.size(), 21UL);
  EXPECT_EQ(m_rows[0][0], i4(3));
  ASSERT_EQ(m_rows[0][1]->size(), 5UL);

  for (char c : *m_rows[0][1]) {
    EXPECT_TRUE(c >= 'a' && c <= 'c') << c;
  }

  const auto t = int32_t(mach_read_from_4(reinterpret_cast<const byte *>(m_rows[0][2]->data())));

  EXPECT_LE(std::abs(t - now), 5);

  for (size_t i = 1; i < m_rows.size(); ++i) {
    const auto r = int32_t(mach_read_from_4(reinterpret_cast<const byte *>(m_rows[i][0]->data())));

    EXPECT_GE(r, 1);
    EXPECT_LE(r, 6);
    EXPECT_EQ(m_rows[i][1], Val(""));
  }
}

/** NULL (a NULL column of SYS_TABLES): the comparisons treat it as the
smallest value, equal to itself, as cmp_dfield_dfield() orders columns. */
TEST_F(EvalTest, NullOrder) {
  ASSERT_EQ(
    run("s CHAR; t CHAR; a INT; b INT; c INT; d INT;",
        "s, a, b, c, d",
        "s := 'x'; t := 'y';\n"
        "SELECT CLUSTER_NAME INTO s FROM SYS_TABLES WHERE NAME = 'SYS_FOREIGN';\n"
        "SELECT CLUSTER_NAME INTO t FROM SYS_TABLES WHERE NAME = 'SYS_FOREIGN';\n"
        "a := 0; b := 0; c := 0; d := 0;\n"
        "IF (s = t) THEN a := 1; END IF;\n"
        "IF (s < 'a') THEN b := 1; END IF;\n"
        "IF (s <> 'a') THEN c := 1; END IF;\n"
        "IF (s >= '') THEN d := 1; END IF;\n"
        "EMIT;"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{std::nullopt, i4(1), i4(1), i4(1), i4(0)}));
}

/** The evaluator on parsed nodes: eval_exp() of the IF conditions of a
procedure and of the values of its assignments, without running it;
eval_node_set_int_val()/eval_node_get_int_val() keep the sign. */
TEST_F(EvalTest, ParsedNodes) {
  Parsed p(
    *m_sys,
    "PROCEDURE P () IS\n"
    "r INT;\n"
    "BEGIN\n"
    "  IF (2 < 3) THEN r := 1; END IF;\n"
    "  IF (3 < 2) THEN r := 1; END IF;\n"
    "  IF ('b' >= 'a') AND NOT ('b' = 'c') THEN r := 1; END IF;\n"
    "  r := 6 * 7 - 50;\n"
    "  r := LENGTH(CONCAT('ab', 'cde'));\n"
    "END;\n"
  );

  ASSERT_NE(p.m_graph, nullptr);

  const std::vector<bool> conds{true, false, true};

  for (ulint i{}; i < conds.size(); ++i) {
    auto node = static_cast<IfNode *>(p.stat(i));

    ASSERT_EQ(que_node_get_type(node), QUE_NODE_IF);
    eval_exp(node->cond);
    EXPECT_EQ(eval_node_get_bool_val(node->cond), conds[i]) << i;

    /* Evaluated twice: the same, the value buffer reused. */
    eval_exp(node->cond);
    EXPECT_EQ(eval_node_get_bool_val(node->cond), conds[i]) << i;
  }

  auto a1 = static_cast<AssignNode *>(p.stat(3));
  auto a2 = static_cast<AssignNode *>(p.stat(4));

  ASSERT_EQ(que_node_get_type(a1), QUE_NODE_ASSIGNMENT);
  eval_exp(a1->val);
  EXPECT_EQ(eval_node_get_int_val(a1->val), -8);
  EXPECT_EQ(bytes(que_node_get_val(a1->val)), i4(-8));
  eval_exp(a2->val);
  EXPECT_EQ(eval_node_get_int_val(a2->val), 5);

  /* The value of a function node holds any 4 byte integer. */
  for (lint v : {lint(INT32_MIN), lint(-1), lint(0), lint(1), lint(INT32_MAX)}) {
    eval_node_set_int_val(a1->val, v);
    EXPECT_EQ(eval_node_get_int_val(a1->val), v);
  }

  /* 64 bit values are cut to 32. */
  eval_node_set_int_val(a1->val, lint(1) << 32 | 5);
  EXPECT_EQ(eval_node_get_int_val(a1->val), 5);
}

/* --- Performance ---------------------------------------------------------- */

using DataPerf = RowTest;

/** The hot paths of this group: tuple fold and compare, row_build and the
index entries of a row, row references, the evaluator. */
TEST_F(DataPerf, Rows) {
  const auto t = create_all();
  std::vector<Fill> rows;

  for (uint32_t k = 1; k <= 200; ++k) {
    rows.push_back(all_row(k, false, std::string(50, 'b')));
  }

  load(t, rows);

  auto clust = index(t);
  auto sec = index(t, "S_VC_I2");
  Rec rec;
  RecOffs offs;
  Mtr mtr;
  BtreePcursor pcur;
  MemHeap *oheap{};

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, clust, BTR_SEARCH_LEAF, true, 0, &mtr);
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  rec = pcur.get_rec();
  offs = PhyRec(clust, rec).get_all_col_offsets(nullptr, &oheap, Current_location());

  auto heap = mem_heap_create(16384);
  ulint n_ext;
  auto entry = dup(row_rec_to_index_entry(ROW_COPY_POINTERS, rec, clust, offs, &n_ext, heap), heap);

  auto r1 = ut::bench("dtuple_fold_clust", 1000000, [&](uint64_t i) { ut::keep(dtuple_fold(entry, 4, 0, i)); });

  EXPECT_LT(r1.ns_per_op(), 20000.0);

  auto r2 = ut::bench("cmp_dtuple_rec_clust", 1000000, [&](uint64_t) { ut::keep(cmp_dtuple_rec(nullptr, entry, rec, offs.get())); });

  EXPECT_LT(r2.ns_per_op(), 20000.0);

  auto r3 = ut::bench("row_build_pointers", 200000, [&](uint64_t i) {
    RowExt *ext;

    if (i % 64 == 0) {
      mem_heap_empty(heap);
    }

    ut::keep(row_build(*m_sys, ROW_COPY_POINTERS, clust, rec, offs, nullptr, &ext, heap));
  });

  EXPECT_LT(r3.ns_per_op(), 50000.0);

  mem_heap_empty(heap);

  RowExt *ext;
  auto row = row_build(*m_sys, ROW_COPY_DATA, clust, rec, offs, nullptr, &ext, m_heap);

  auto r4 = ut::bench("row_build_index_entry_sec", 500000, [&](uint64_t i) {
    if (i % 256 == 0) {
      mem_heap_empty(heap);
    }

    ut::keep(row_build_index_entry(row, nullptr, sec, heap));
  });

  EXPECT_LT(r4.ns_per_op(), 50000.0);

  pcur.close();
  mtr.commit(*m_sys);

  /* Row references from the secondary records. */
  mtr.start();
  pcur.open_at_index_side(*m_sys, true, sec, BTR_SEARCH_LEAF, true, 0, &mtr);
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  auto srec = pcur.get_rec();
  RecOffs soffs = PhyRec(sec, srec).get_all_col_offsets(nullptr, &oheap, Current_location());
  auto ref = dtuple_create(m_heap, clust->get_n_unique());

  auto r5 = ut::bench("row_build_row_ref_in_tuple", 500000, [&](uint64_t) {
    row_build_row_ref_in_tuple(ref, srec, sec, &soffs, nullptr);
    ut::keep(ref);
  });

  EXPECT_LT(r5.ns_per_op(), 50000.0);

  pcur.close();
  mtr.commit(*m_sys);
  mem_heap_free(heap);
  mem_heap_free(oheap);
}

/** que_eval_sql() of a procedure with a loop of arithmetic, comparisons
and a string function; and eval_exp() of a parsed condition. */
TEST_F(DataPerf, Eval) {
  Parsed p(*m_sys, "PROCEDURE P () IS\nr INT;\nBEGIN\n  IF ((2 * 3 + 4) / 2 >= 5) AND NOT ('ab' = 'ac') THEN r := 1; END IF;\nEND;\n");
  auto cond = static_cast<IfNode *>(p.stat(0))->cond;

  auto r1 = ut::bench("eval_exp_cond", 1000000, [&](uint64_t) {
    eval_exp(cond);
    ut::keep(eval_node_get_bool_val(cond));
  });

  EXPECT_LT(r1.ns_per_op(), 20000.0);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

  auto r2 = ut::bench("que_eval_sql_loop_1000", 50, [&](uint64_t) {
    EXPECT_EQ(
      que_eval_sql(
        *m_sys,
        pars_info_create(),
        "PROCEDURE P () IS\ni INT;\ns INT;\nc CHAR;\nBEGIN\n"
        "s := 0;\n"
        "FOR i IN 1 .. 1000 LOOP\n"
        "  IF (i > 500) THEN s := s + i * 2; ELSE s := s - 1; END IF;\n"
        "  c := CONCAT('x', 'y');\n"
        "END LOOP;\nEND;\n",
        false,
        trx
      ),
      DB_SUCCESS
    );
  });

  EXPECT_LT(r2.ns_per_op(), 1e9);
  commit(trx);
}

}  // namespace
