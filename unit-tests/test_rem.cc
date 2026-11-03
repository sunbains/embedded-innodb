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

/** @file unit-tests/test_rem.cc
Rec, RecOffs, PhyRec and the comparisons of rem0cmp: the physical record
format (header bits, 1 and 2 byte field offsets, SQL NULL and externally
stored fields), the conversion of a DTuple to a record and back, and the
ordering of every data type, in memory (SysTest). The ordering of integer
keys of every width and signedness in a real index (DbTest). */

#include "ut0env.h"
#include "ut0perf.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0upd.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace {

/** A field of a test tuple: its type and value (nullopt: SQL NULL). */
struct F {
  ulint m_mtype{DATA_BINARY};
  ulint m_prtype{0};
  ulint m_len{0};
  std::optional<std::string> m_data{};
  bool m_ext{false};
};

/** A heap for the duration of a test. */
struct Heap {
  Heap() : m_heap(mem_heap_create(1024)) {}
  ~Heap() { mem_heap_free(m_heap); }

  Heap(const Heap &) = delete;
  Heap &operator=(const Heap &) = delete;

  MemHeap *m_heap;
};

/** @return a typed tuple of the fields fs. */
DTuple *mk_tuple(MemHeap *heap, const std::vector<F> &fs) {
  auto tuple = dtuple_create(heap, fs.size());

  for (ulint i{}; i < fs.size(); ++i) {
    auto field = dtuple_get_nth_field(tuple, i);
    const auto &f = fs[i];

    dtype_set(dfield_get_type(field), f.m_mtype, f.m_prtype, f.m_len);

    if (!f.m_data.has_value()) {
      dfield_set_null(field);
    } else {
      auto data = mem_heap_alloc(heap, std::max<ulint>(f.m_data->size(), 1));

      memcpy(data, f.m_data->data(), f.m_data->size());
      dfield_set_data(field, data, f.m_data->size());

      if (f.m_ext) {
        dfield_set_ext(field);
      }
    }
  }

  return tuple;
}

/** A binary string field. */
F bin(const std::string &s) {
  return F{DATA_BINARY, 0, 255, s};
}

/** A VARCHAR field (padded with spaces in comparisons). */
F vc(const std::string &s) {
  return F{DATA_VARCHAR, DATA_ENGLISH, 255, s};
}

/** A nullable fixed length 4 byte INT field. */
F int4(std::optional<uint32_t> v) {
  if (!v.has_value()) {
    return F{DATA_INT, DATA_UNSIGNED, 4, std::nullopt};
  }

  byte buf[4];

  mach_write_to_4(buf, *v);

  return F{DATA_INT, DATA_UNSIGNED, 4, std::string(reinterpret_cast<char *>(buf), 4)};
}

/** A tuple converted to a record, with its offsets. */
struct PRec {
  PRec(const DTuple *tuple, ulint n_ext) {
    m_buf.resize(rec_get_converted_size(nullptr, tuple, n_ext) + 64);
    m_rec = Rec::convert_dtuple_to_rec(m_buf.data(), nullptr, tuple, n_ext);
    rec_offs_init(m_offs_);

    MemHeap *heap{};

    m_offs = PhyRec(nullptr, m_rec).get_all_col_offsets(m_offs_.data(), &heap, Current_location());
    EXPECT_EQ(heap, nullptr);
  }

  /** @return field n as a string, nullopt if SQL NULL. */
  std::optional<std::string> field(ulint n) const {
    ulint len;
    auto data = m_rec.get_nth_field(m_offs, n, &len);

    if (len == UNIV_SQL_NULL) {
      return std::nullopt;
    }

    return std::string(reinterpret_cast<const char *>(data.get()), len);
  }

  std::vector<byte> m_buf;
  Rec m_rec;
  std::array<ulint, REC_OFFS_NORMAL_SIZE> m_offs_{};
  RecOffs m_offs;
};

/** @return the 20 byte reference of an externally stored field: space,
page, offset, length, all zero for a reference not yet written. */
std::string ext_ref(uint32_t page_no, uint32_t len, bool zero = false) {
  std::string ref(BTR_EXTERN_FIELD_REF_SIZE_LOCAL, '\0');

  if (!zero) {
    auto p = reinterpret_cast<byte *>(ref.data());

    mach_write_to_4(p + 0, 0);
    mach_write_to_4(p + 4, page_no);
    mach_write_to_4(p + 8, 38);
    mach_write_to_4(p + 16, len);
  }

  return ref;
}

/** Compare the values a and b of one type with cmp_data_data. */
int cmp(ulint mtype, ulint prtype, const std::optional<std::string> &a, const std::optional<std::string> &b) {
  return cmp_data_data(
    nullptr,
    mtype,
    prtype,
    a ? reinterpret_cast<const byte *>(a->data()) : nullptr,
    a ? a->size() : UNIV_SQL_NULL,
    b ? reinterpret_cast<const byte *>(b->data()) : nullptr,
    b ? b->size() : UNIV_SQL_NULL
  );
}

/** @return v in the storage format of a len byte integer column. */
template <typename T>
std::string int_bytes(T v, bool usign) {
  std::string s(sizeof(T), '\0');

  mach_write_int_type(reinterpret_cast<byte *>(s.data()), reinterpret_cast<const byte *>(&v), sizeof(T), usign);

  return s;
}

using RecTest = ut::SysTest;

/** A record of small fields uses 1 byte offsets: the extra size is the
6 byte header and a byte per field, the data size the sum of the lengths. */
TEST_F(RecTest, OneByteOffsets) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("ab"), bin(""), bin("cdef")});
  PRec r(t, 0);

  EXPECT_TRUE(r.m_rec.get_1byte_offs_flag());
  EXPECT_EQ(r.m_rec.get_n_fields(), 3UL);
  EXPECT_EQ(r.m_offs.n_fields(), 3UL);
  EXPECT_EQ(r.m_offs.extra_size(), REC_N_EXTRA_BYTES + 3);
  EXPECT_EQ(r.m_offs.data_size(), 6UL);
  EXPECT_EQ(r.m_offs.size(), REC_N_EXTRA_BYTES + 3 + 6);
  EXPECT_EQ(r.m_offs.size(), rec_get_converted_size(nullptr, t, 0));
  EXPECT_EQ(r.m_rec.get_data_size(), 6UL);
  EXPECT_EQ(r.field(0), "ab");
  EXPECT_EQ(r.field(1), "");
  EXPECT_EQ(r.field(2), "cdef");
  EXPECT_FALSE(r.m_offs.any_extern());
  EXPECT_TRUE(r.m_rec.validate(r.m_offs));
  EXPECT_TRUE(r.m_rec.offs_validate(nullptr, r.m_offs));
}

/** Data longer than REC_1BYTE_OFFS_LIMIT (127) needs 2 byte offsets. */
TEST_F(RecTest, TwoByteOffsets) {
  Heap h;

  {
    auto t = mk_tuple(h.m_heap, {bin(std::string(REC_1BYTE_OFFS_LIMIT, 'x'))});
    PRec r(t, 0);

    EXPECT_TRUE(r.m_rec.get_1byte_offs_flag()) << "127 bytes still fit 1 byte offsets";
  }

  auto t = mk_tuple(h.m_heap, {bin(std::string(REC_1BYTE_OFFS_LIMIT + 1, 'x')), bin("y")});
  PRec r(t, 0);

  EXPECT_FALSE(r.m_rec.get_1byte_offs_flag());
  EXPECT_EQ(r.m_offs.extra_size(), REC_N_EXTRA_BYTES + 2 * 2);
  EXPECT_EQ(r.m_offs.data_size(), REC_1BYTE_OFFS_LIMIT + 2);
  EXPECT_EQ(r.field(0)->size(), REC_1BYTE_OFFS_LIMIT + 1);
  EXPECT_EQ(r.field(1), "y");
  EXPECT_EQ(r.m_offs.size(), rec_get_converted_size(nullptr, t, 0));
  EXPECT_TRUE(r.m_rec.validate(r.m_offs));
}

/** The extra size formula agrees with the records the conversion makes. */
TEST_F(RecTest, ConvertedExtraSize) {
  EXPECT_EQ(rec_get_converted_extra_size(0, 1, 0), REC_N_EXTRA_BYTES + 1);
  EXPECT_EQ(rec_get_converted_extra_size(REC_1BYTE_OFFS_LIMIT, 5, 0), REC_N_EXTRA_BYTES + 5);
  EXPECT_EQ(rec_get_converted_extra_size(REC_1BYTE_OFFS_LIMIT + 1, 5, 0), REC_N_EXTRA_BYTES + 10);
  /* An externally stored field forces 2 byte offsets, the extern bit is there. */
  EXPECT_EQ(rec_get_converted_extra_size(10, 5, 1), REC_N_EXTRA_BYTES + 10);
}

/** A NULL fixed length field occupies its fixed size in the data, filled
with zeros; a NULL variable length field none. Both are flagged in the
offsets and read back with length UNIV_SQL_NULL. */
TEST_F(RecTest, NullFields) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(7), int4(std::nullopt), F{DATA_BINARY, 0, 255, std::nullopt}, bin("z")});
  PRec r(t, 0);

  EXPECT_FALSE(r.m_offs.nth_sql_null(0));
  EXPECT_TRUE(r.m_offs.nth_sql_null(1));
  EXPECT_TRUE(r.m_offs.nth_sql_null(2));
  EXPECT_FALSE(r.m_offs.nth_sql_null(3));
  EXPECT_EQ(r.field(1), std::nullopt);
  EXPECT_EQ(r.field(2), std::nullopt);
  EXPECT_EQ(r.field(3), "z");

  /* 4 (INT) + 4 (NULL INT, reserved) + 0 (NULL VARBINARY) + 1. */
  EXPECT_EQ(r.m_offs.data_size(), 9UL);
  EXPECT_EQ(r.m_rec.get_nth_field_size(1), 4UL);
  EXPECT_EQ(r.m_rec.get_nth_field_size(2), 0UL);

  ulint len;
  const auto offs = r.m_rec.get_nth_field_offs(1, &len);

  EXPECT_EQ(len, UNIV_SQL_NULL);
  EXPECT_EQ(offs, 4UL);

  for (ulint i = 4; i < 8; ++i) {
    EXPECT_EQ(r.m_rec.get()[i], 0) << "byte " << i << " of the NULL field";
  }

  EXPECT_TRUE(r.m_rec.validate(r.m_offs));
}

/** RecOffs::nth_size() is the size the field occupies in the record. For
an SQL NULL field of fixed size, that is its fixed size: an update of the
field to a value of that size fits in place. */
TEST_F(RecTest, NthSizeOfNull) {
  /* Regression: RecOffs::nth_size() (include/rem0types.h:1149) returns
  base[n + 1] - (base[n] & REC_OFFS_MASK) without masking the flags of
  base[n + 1]: for a NULL (or externally stored) field n > 0 the result
  has REC_OFFS_SQL_NULL (or REC_OFFS_EXTERNAL) added. Upstream masks the
  difference: (base[1 + n] - base[n]) & REC_OFFS_MASK. */
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(7), int4(std::nullopt)});
  PRec r(t, 0);

  EXPECT_EQ(r.m_offs.nth_size(0), 4UL);
  EXPECT_EQ(r.m_offs.nth_size(1), 4UL);
}

/** nth_size() of fields that are neither NULL nor external. */
TEST_F(RecTest, NthSize) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("abc"), bin(""), int4(1), bin(std::string(200, 'q'))});
  PRec r(t, 0);

  EXPECT_EQ(r.m_offs.nth_size(0), 3UL);
  EXPECT_EQ(r.m_offs.nth_size(1), 0UL);
  EXPECT_EQ(r.m_offs.nth_size(2), 4UL);
  EXPECT_EQ(r.m_offs.nth_size(3), 200UL);
}

/** Setting and clearing the null bit of a field changes only that flag. */
TEST_F(RecTest, NullBit) {
  Heap h;

  for (auto len : {ulint(10), ulint(300)}) {
    auto t = mk_tuple(h.m_heap, {int4(1), bin(std::string(len, 'a'))});
    PRec r(t, 0);

    r.m_rec.set_nth_field_null_bit(0, true);

    ulint l;

    (void)r.m_rec.get_nth_field_offs(0, &l);
    EXPECT_EQ(l, UNIV_SQL_NULL) << len;
    EXPECT_EQ(r.m_rec.get_nth_field_size(0), 4UL);

    (void)r.m_rec.get_nth_field_offs(1, &l);
    EXPECT_EQ(l, len);

    r.m_rec.set_nth_field_null_bit(0, false);
    (void)r.m_rec.get_nth_field_offs(0, &l);
    EXPECT_EQ(l, 4UL);
  }
}

/** Setting a field to SQL NULL also clears its bytes, as the conversion of
a NULL field does: records with the same values are byte equal. */
TEST_F(RecTest, SetSqlNullClearsData) {
  /* Regression: Rec::set_nth_field_sql_null() (rem/rem0rec.cc:833) only sets the
  null bit; the free function rec_set_nth_field_sql_null() above it, and
  upstream, first write zeros over the field with data_write_sql_null(). */
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(0x01020304), bin("x")});
  PRec r(t, 0);

  r.m_rec.set_nth_field_sql_null(0);

  ulint l;

  (void)r.m_rec.get_nth_field_offs(0, &l);
  EXPECT_EQ(l, UNIV_SQL_NULL);

  for (ulint i = 0; i < 4; ++i) {
    EXPECT_EQ(r.m_rec.get()[i], 0) << "byte " << i;
  }
}

/** set_nth_field() overwrites a field of the same length in place and
can set a NULL fixed field to a value. */
TEST_F(RecTest, SetNthField) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(std::nullopt), bin("abcd")});
  PRec r(t, 0);

  byte v[4];

  mach_write_to_4(v, 0xdeadbeef);
  r.m_rec.set_nth_field(r.m_offs, 0, v, 4);
  r.m_rec.set_nth_field(r.m_offs, 1, "wxyz", 4);

  /* The offsets are stale after a NULL changes: recompute them. */
  MemHeap *heap{};
  auto offs = PhyRec(nullptr, r.m_rec).get_all_col_offsets(r.m_offs_.data(), &heap, Current_location());

  ulint len;
  auto f0 = r.m_rec.get_nth_field(offs, 0, &len);

  ASSERT_EQ(len, 4UL);
  EXPECT_EQ(mach_read_from_4(f0), 0xdeadbeefUL);
  EXPECT_EQ(r.field(1), "wxyz");
}

/** The header bit fields are independent of each other. */
TEST_F(RecTest, HeaderBits) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("k"), bin("v")});
  PRec r(t, 0);
  auto &rec = r.m_rec;

  rec.set_heap_no(REC_HEAP_NO_MASK >> REC_HEAP_NO_SHIFT);
  rec.set_n_owned(REC_MAX_N_OWNED);
  rec.set_info_bits(0);
  rec.set_next_offs(0x1234);

  EXPECT_EQ(rec.get_heap_no(), 8191UL);
  EXPECT_EQ(rec.get_n_owned(), REC_MAX_N_OWNED);
  EXPECT_EQ(rec.get_info_bits(), 0UL);
  EXPECT_EQ(rec.get_next_offs(), 0x1234UL);
  EXPECT_EQ(rec.get_n_fields(), 2UL);
  EXPECT_TRUE(rec.get_1byte_offs_flag());

  rec.set_deleted_flag(true);
  EXPECT_TRUE(rec.get_deleted_flag());
  EXPECT_EQ(rec.get_info_bits(), REC_INFO_DELETED_FLAG);
  EXPECT_EQ(rec.get_n_owned(), REC_MAX_N_OWNED);

  rec.set_info_bits(REC_INFO_DELETED_FLAG | REC_INFO_MIN_REC_FLAG);
  rec.set_n_owned(3);
  EXPECT_EQ(rec.get_info_bits(), REC_INFO_DELETED_FLAG | REC_INFO_MIN_REC_FLAG);
  EXPECT_EQ(rec.get_n_owned(), 3UL);

  rec.set_deleted_flag(false);
  EXPECT_FALSE(rec.get_deleted_flag());
  EXPECT_EQ(rec.get_info_bits(), REC_INFO_MIN_REC_FLAG);

  rec.set_heap_no(PAGE_HEAP_NO_USER_LOW);
  EXPECT_EQ(rec.get_heap_no(), 2UL);
  EXPECT_EQ(rec.get_n_owned(), 3UL);
  EXPECT_EQ(rec.get_next_offs(), 0x1234UL);
  EXPECT_EQ(rec.get_n_fields(), 2UL);

  /* The fields are untouched. */
  EXPECT_EQ(r.field(0), "k");
  EXPECT_EQ(r.field(1), "v");
}

/** The info bits of the tuple are stored in the record. */
TEST_F(RecTest, TupleInfoBits) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("k")});

  dtuple_set_info_bits(t, REC_INFO_DELETED_FLAG);

  PRec r(t, 0);

  EXPECT_TRUE(r.m_rec.get_deleted_flag());
}

/** Externally stored fields: 2 byte offsets with the extern bit, flagged
in the offsets, counted by n_extern(); offs_any_null_extern() finds a
reference that is still all zero. */
TEST_F(RecTest, ExternFields) {
  Heap h;
  const auto prefix = std::string(768, 'p');
  auto t = mk_tuple(
    h.m_heap,
    {bin("key"), F{DATA_BLOB, DATA_BINARY_TYPE, 0, prefix + ext_ref(5, 10000), true}, bin("x"),
     F{DATA_BLOB, DATA_BINARY_TYPE, 0, ext_ref(0, 0, true), true}}
  );

  EXPECT_EQ(dtuple_get_n_ext(t), 2UL);

  PRec r(t, 2);

  EXPECT_FALSE(r.m_rec.get_1byte_offs_flag());
  EXPECT_TRUE(r.m_offs.any_extern());
  EXPECT_EQ(r.m_offs.n_extern(), 2UL);
  EXPECT_FALSE(r.m_offs.nth_extern(0));
  EXPECT_TRUE(r.m_offs.nth_extern(1));
  EXPECT_FALSE(r.m_offs.nth_extern(2));
  EXPECT_TRUE(r.m_offs.nth_extern(3));
  EXPECT_EQ(r.field(1)->size(), 768 + BTR_EXTERN_FIELD_REF_SIZE_LOCAL);
  EXPECT_EQ(r.field(2), "x");

  /* The length, as get_nth_field_offs() reads it, strips the extern bit. */
  ulint len;

  (void)r.m_rec.get_nth_field_offs(1, &len);
  EXPECT_EQ(len, 768 + BTR_EXTERN_FIELD_REF_SIZE_LOCAL);

  const byte *zero_ref = r.m_rec.offs_any_null_extern(r.m_offs);
  ulint len3;

  EXPECT_EQ(zero_ref, r.m_rec.get_nth_field(r.m_offs, 3, &len3).get());

  EXPECT_TRUE(r.m_rec.validate(r.m_offs));
}

/** No reference is all zero: offs_any_null_extern() returns nullptr. */
TEST_F(RecTest, ExternNoNullRef) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("key"), F{DATA_BLOB, DATA_BINARY_TYPE, 0, ext_ref(9, 100000), true}});
  PRec r(t, 1);

  EXPECT_EQ(r.m_rec.offs_any_null_extern(r.m_offs), nullptr);
}

/** A record and its copy have the same fields; the copy of a prefix of the
fields has only those, and the same bytes for them. */
TEST_F(RecTest, Copy) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("one"), int4(std::nullopt), bin(std::string(150, 'w')), bin("four")});
  PRec r(t, 0);

  std::vector<byte> buf(r.m_offs.size());
  auto copy = r.m_rec.copy(buf.data(), r.m_offs);

  EXPECT_EQ(copy.get(), buf.data() + r.m_offs.extra_size());
  EXPECT_EQ(memcmp(buf.data(), r.m_rec.get() - r.m_offs.extra_size(), buf.size()), 0);
  EXPECT_EQ(copy.get_n_fields(), 4UL);

  byte *pbuf{};
  ulint pbuf_size{};
  auto prefix = r.m_rec.copy_prefix_to_buf(nullptr, 2, pbuf, pbuf_size);

  EXPECT_EQ(prefix.get_n_fields(), 2UL);

  ulint len;

  EXPECT_EQ(memcmp(prefix.get_nth_field(0, &len).get(), "one", 3), 0);
  EXPECT_EQ(len, 3UL);
  (void)prefix.get_nth_field(1, &len);
  EXPECT_EQ(len, UNIV_SQL_NULL);

  /* The buffer is reused when it is big enough. */
  auto old = pbuf;

  prefix = r.m_rec.copy_prefix_to_buf(nullptr, 1, pbuf, pbuf_size);
  EXPECT_EQ(pbuf, old);
  EXPECT_EQ(prefix.get_n_fields(), 1UL);

  mem_free(pbuf);
}

/** A tuple converted to a record and copied back to a tuple is the same. */
TEST_F(RecTest, RoundTrip) {
  Heap h;
  std::mt19937 rng(7);

  for (int round = 0; round < 200; ++round) {
    std::vector<F> fs;
    const auto n = 1 + rng() % 12;

    for (ulint i{}; i < n; ++i) {
      switch (rng() % 4) {
        case 0:
          fs.push_back(int4(rng()));
          break;
        case 1:
          fs.push_back(int4(std::nullopt));
          break;
        case 2:
          fs.push_back(bin(std::string(rng() % (round % 2 == 0 ? 10 : 400), char('a' + i))));
          break;
        default:
          fs.push_back(F{DATA_BINARY, 0, 255, std::nullopt});
      }
    }

    auto t = mk_tuple(h.m_heap, fs);
    PRec r(t, 0);

    ASSERT_EQ(r.m_offs.size(), rec_get_converted_size(nullptr, t, 0));

    auto back = dtuple_create(h.m_heap, n);

    dtuple_set_types_binary(back, n);
    r.m_rec.copy_prefix_to_dtuple(back, nullptr, n, h.m_heap);

    for (ulint i{}; i < n; ++i) {
      auto f = dtuple_get_nth_field(back, i);

      ASSERT_EQ(dfield_is_null(f), !fs[i].m_data.has_value()) << round << " " << i;

      if (!dfield_is_null(f)) {
        ASSERT_EQ(std::string(static_cast<const char *>(dfield_get_data(f)), dfield_get_len(f)), *fs[i].m_data)
          << round << " " << i;
      }
    }

    /* The tuple compares equal to its record. */
    ASSERT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), 0) << round;
    ASSERT_TRUE(cmp_dtuple_is_prefix_of_rec(nullptr, t, r.m_rec, r.m_offs));
  }
}

/** The fold of a record is the fold of its tuple, for any prefix. */
TEST_F(RecTest, Fold) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {bin("alpha"), int4(3), bin("gamma-delta")});
  PRec r(t, 0);

  for (ulint n_fields = 0; n_fields <= 2; ++n_fields) {
    for (ulint n_bytes : {0, 1, 3}) {
      if (n_fields + n_bytes == 0) {
        continue;
      }

      EXPECT_EQ(r.m_rec.fold(r.m_offs, n_fields, n_bytes, 42), dtuple_fold(t, n_fields, n_bytes, 42))
        << n_fields << " " << n_bytes;
    }
  }

  EXPECT_NE(r.m_rec.fold(r.m_offs, 1, 0, 42), r.m_rec.fold(r.m_offs, 1, 0, 43)) << "the tree id is folded in";
}

/** SQL NULL is smaller than any value of every type, equal to NULL. */
TEST_F(RecTest, CmpNull) {
  for (auto mtype : {DATA_VARCHAR, DATA_CHAR, DATA_BINARY, DATA_FIXBINARY, DATA_BLOB, DATA_INT, DATA_FLOAT, DATA_DOUBLE,
                     DATA_DECIMAL, DATA_CLIENT}) {
    const std::string v(mtype == DATA_DOUBLE ? 8 : 4, '\0');

    EXPECT_EQ(cmp(mtype, 0, std::nullopt, std::nullopt), 0) << mtype;
    EXPECT_EQ(cmp(mtype, 0, std::nullopt, v), -1) << mtype;
    EXPECT_EQ(cmp(mtype, 0, v, std::nullopt), 1) << mtype;
    EXPECT_EQ(cmp(mtype, 0, std::nullopt, std::string()), -1) << mtype << ": NULL < empty";
  }
}

/** Character strings are padded with spaces: trailing spaces don't count. */
TEST_F(RecTest, CmpVarcharPad) {
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "abc", "abc"), 0);
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "abc", "abc   "), 0);
  EXPECT_EQ(cmp(DATA_CHAR, DATA_ENGLISH, "abc   ", "abc"), 0);
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "", "  "), 0);
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "abc", "abd"), -1);
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "abc", "abc!"), -1) << "the pad ' ' (0x20) sorts before '!' (0x21)";
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "abc", std::string("abc\x1f")), 1) << "0x1f sorts before the pad";
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "b", "abcdef"), 1);
  /* Bytes compare unsigned. */
  EXPECT_EQ(cmp(DATA_VARCHAR, DATA_ENGLISH, "\xff", "a"), 1);
}

/** Binary strings with the binary collation are not padded: the shorter
is the smaller; other binary strings are padded with spaces. */
TEST_F(RecTest, CmpBinary) {
  const auto bin_coll = dtype_form_prtype(0, DATA_CLIENT_BINARY_CHARSET_COLL);

  EXPECT_EQ(cmp(DATA_BINARY, bin_coll, "ab", "ab"), 0);
  EXPECT_EQ(cmp(DATA_BINARY, bin_coll, "ab", "ab "), -1);
  EXPECT_EQ(cmp(DATA_FIXBINARY, bin_coll, std::string("ab\0", 3), "ab"), 1);
  EXPECT_EQ(cmp(DATA_BINARY, bin_coll, "", "a"), -1);
  EXPECT_EQ(cmp(DATA_BINARY, 0, "ab", "ab "), 0);
  EXPECT_EQ(cmp(DATA_BINARY, bin_coll, "\x80", "\x7f"), 1) << "unsigned bytes";
}

/** Unsigned integers of each width at the extremes. */
TEST_F(RecTest, CmpUnsigned) {
  const auto p = DATA_UNSIGNED;

  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint8_t>(0, true), int_bytes<uint8_t>(255, true)), -1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint8_t>(128, true), int_bytes<uint8_t>(127, true)), 1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint16_t>(256, true), int_bytes<uint16_t>(255, true)), 1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint16_t>(65535, true), int_bytes<uint16_t>(65535, true)), 0);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint32_t>(0x80000000, true), int_bytes<uint32_t>(0x7fffffff, true)), 1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint32_t>(1, true), int_bytes<uint32_t>(0x100, true)), -1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint64_t>(UINT64_MAX, true), int_bytes<uint64_t>(0, true)), 1);
  EXPECT_EQ(cmp(DATA_INT, p, int_bytes<uint64_t>(1ULL << 63, true), int_bytes<uint64_t>((1ULL << 63) - 1, true)), 1);
}

/** Signed integers, stored with the sign bit flipped, order as numbers. */
template <typename T>
void check_signed_order() {
  const std::vector<T> vals{
    std::numeric_limits<T>::min(), T(std::numeric_limits<T>::min() + 1), T(-2), T(-1), T(0), T(1),
    T(std::numeric_limits<T>::max() - 1), std::numeric_limits<T>::max()};

  for (size_t i{}; i < vals.size(); ++i) {
    for (size_t j{}; j < vals.size(); ++j) {
      const auto expected = vals[i] < vals[j] ? -1 : (vals[i] > vals[j] ? 1 : 0);

      EXPECT_EQ(cmp(DATA_INT, 0, int_bytes<T>(vals[i], false), int_bytes<T>(vals[j], false)), expected)
        << sizeof(T) << " bytes: " << int64_t(vals[i]) << " vs " << int64_t(vals[j]);
    }
  }
}

TEST_F(RecTest, CmpSigned) {
  check_signed_order<int8_t>();
  check_signed_order<int16_t>();
  check_signed_order<int32_t>();
  check_signed_order<int64_t>();
}

/** The storage format of an integer reads back to the value. */
TEST_F(RecTest, IntStorageRoundTrip) {
  for (int64_t v : {INT64_MIN, int64_t(-1), int64_t(0), int64_t(1), INT64_MAX}) {
    const auto s = int_bytes<int64_t>(v, false);
    int64_t back{};

    mach_read_int_type(&back, reinterpret_cast<const byte *>(s.data()), 8, false);
    EXPECT_EQ(back, v);
  }

  for (int16_t v : {int16_t(INT16_MIN), int16_t(-300), int16_t(0), int16_t(INT16_MAX)}) {
    const auto s = int_bytes<int16_t>(v, false);
    int16_t back{};

    mach_read_int_type(&back, reinterpret_cast<const byte *>(s.data()), 2, false);
    EXPECT_EQ(back, v);
  }
}

/** FLOAT and DOUBLE compare as numbers, negative values included. */
TEST_F(RecTest, CmpFloatDouble) {
  auto f = [](float v) {
    std::string s(4, '\0');
    mach_float_write(reinterpret_cast<byte *>(s.data()), v);
    return s;
  };
  auto d = [](double v) {
    std::string s(8, '\0');
    mach_double_write(reinterpret_cast<byte *>(s.data()), v);
    return s;
  };

  EXPECT_EQ(cmp(DATA_FLOAT, 0, f(-1.5f), f(1.0f)), -1);
  EXPECT_EQ(cmp(DATA_FLOAT, 0, f(-1.5f), f(-2.5f)), 1);
  EXPECT_EQ(cmp(DATA_FLOAT, 0, f(0.25f), f(0.25f)), 0);
  EXPECT_EQ(cmp(DATA_DOUBLE, 0, d(-1e300), d(1e-300)), -1);
  EXPECT_EQ(cmp(DATA_DOUBLE, 0, d(3.0), d(2.999999)), 1);
  EXPECT_EQ(cmp(DATA_DOUBLE, 0, d(-0.0), d(0.0)), 0);
}

/** DECIMAL strings compare as numbers: sign, leading zeros and spaces. */
TEST_F(RecTest, CmpDecimal) {
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "-5", "3"), -1);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "3", "-5"), 1);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "007", "7"), 0);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "  12", "12"), 0);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "-10", "-9"), -1);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "100", "99"), 1);
  EXPECT_EQ(cmp(DATA_DECIMAL, 0, "+5", "5"), 0);
}

/** Client types and binary BLOBs compare as binary strings, not padded. */
TEST_F(RecTest, CmpClientBlob) {
  EXPECT_EQ(cmp(DATA_CLIENT, 0, "ab", "ab"), 0);
  EXPECT_EQ(cmp(DATA_CLIENT, 0, "ab", "abc"), -1);
  EXPECT_EQ(cmp(DATA_CLIENT, 0, "b", "abc"), 1);
  EXPECT_EQ(cmp(DATA_VARCLIENT, 0, "", "a"), -1);
  EXPECT_EQ(cmp(DATA_BLOB, DATA_BINARY_TYPE, "ab", "ab "), -1);
  EXPECT_EQ(cmp(DATA_BLOB, DATA_BINARY_TYPE, "ab", "ab"), 0);
  /* A latin1 text BLOB is padded like a VARCHAR. */
  const auto latin1 = dtype_form_prtype(0, DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL);

  EXPECT_EQ(cmp(DATA_BLOB, latin1, "ab", "ab  "), 0);
}

/** cmp_dfield_dfield is cmp_data_data with the type of the first field. */
TEST_F(RecTest, CmpDfield) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {vc("abc"), vc("abc  "), bin("abd")});

  EXPECT_EQ(cmp_dfield_dfield(nullptr, dtuple_get_nth_field(t, 0), dtuple_get_nth_field(t, 1)), 0);
  EXPECT_EQ(cmp_dfield_dfield(nullptr, dtuple_get_nth_field(t, 0), dtuple_get_nth_field(t, 2)), -1);
  EXPECT_EQ(cmp_dfield_dfield(nullptr, dtuple_get_nth_field(t, 2), dtuple_get_nth_field(t, 0)), 1);
}

/** A tuple against a record: the result, and how many fields and bytes
of the first differing field matched. */
TEST_F(RecTest, CmpDtupleRecMatch) {
  Heap h;
  auto rt = mk_tuple(h.m_heap, {bin("abc"), bin("xyw"), int4(5)});
  PRec r(rt, 0);

  struct Case {
    std::vector<F> m_fs;
    int m_ret;
    ulint m_fields;
    ulint m_bytes;
  };

  const std::vector<Case> cases{
    {{bin("abc"), bin("xyz")}, 1, 1, 2},
    {{bin("abc"), bin("xya")}, -1, 1, 2},
    {{bin("abc"), bin("xyw"), int4(5)}, 0, 3, 0},
    {{bin("abc"), bin("xyw"), int4(6)}, 1, 2, 3},
    {{bin("abc"), bin("xyw"), int4(4)}, -1, 2, 3},
    {{bin("abb")}, -1, 0, 2},
    {{bin("abc")}, 0, 1, 0},
    {{bin("b")}, 1, 0, 0},
    {{F{DATA_BINARY, 0, 255, std::nullopt}}, -1, 0, 0},
  };

  for (size_t i{}; i < cases.size(); ++i) {
    auto t = mk_tuple(h.m_heap, cases[i].m_fs);
    ulint fields{};
    ulint bytes{};

    EXPECT_EQ(cmp_dtuple_rec_with_match(nullptr, t, r.m_rec, r.m_offs, &fields, &bytes), cases[i].m_ret) << i;
    EXPECT_EQ(fields, cases[i].m_fields) << i;
    EXPECT_EQ(bytes, cases[i].m_bytes) << i;
    EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), cases[i].m_ret) << i;
  }
}

/** Only n_fields_cmp fields of the tuple are compared. */
TEST_F(RecTest, CmpNFieldsCmp) {
  Heap h;
  auto rt = mk_tuple(h.m_heap, {bin("a"), bin("b")});
  PRec r(rt, 0);
  auto t = mk_tuple(h.m_heap, {bin("a"), bin("z")});

  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), 1);
  dtuple_set_n_fields_cmp(t, 1);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), 0);
}

/** The predefined minimum record is smaller than any tuple, unless the
tuple has the flag too; a tuple with the flag is smaller than any record. */
TEST_F(RecTest, CmpMinRec) {
  Heap h;
  auto rt = mk_tuple(h.m_heap, {bin("m")});
  PRec r(rt, 0);
  auto t = mk_tuple(h.m_heap, {bin("a")});

  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), -1);

  r.m_rec.set_info_bits(REC_INFO_MIN_REC_FLAG);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), 1);

  dtuple_set_info_bits(t, REC_INFO_MIN_REC_FLAG);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), 0);

  r.m_rec.set_info_bits(0);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs), -1);
}

/** An externally stored field of the record is not compared: equal so far. */
TEST_F(RecTest, CmpExternStops) {
  Heap h;
  auto rt = mk_tuple(h.m_heap, {bin("k"), F{DATA_BLOB, DATA_BINARY_TYPE, 0, std::string(30, 'b') + ext_ref(4, 9000), true}});
  PRec r(rt, 1);
  auto t = mk_tuple(h.m_heap, {bin("k"), F{DATA_BLOB, DATA_BINARY_TYPE, 0, std::string("zzz")}});
  ulint fields{};
  ulint bytes{};

  EXPECT_EQ(cmp_dtuple_rec_with_match(nullptr, t, r.m_rec, r.m_offs, &fields, &bytes), 0);
  EXPECT_EQ(fields, 1UL);
}

/** The tuple is a prefix of the record if its fields match and its last
field is a byte prefix of the record's field. */
TEST_F(RecTest, IsPrefix) {
  Heap h;
  auto rt = mk_tuple(h.m_heap, {bin("abc"), bin("defgh")});
  PRec r(rt, 0);

  EXPECT_TRUE(cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("abc")}), r.m_rec, r.m_offs));
  EXPECT_TRUE(cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("ab")}), r.m_rec, r.m_offs));
  EXPECT_TRUE(cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("abc"), bin("de")}), r.m_rec, r.m_offs));
  EXPECT_FALSE(cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("abd")}), r.m_rec, r.m_offs));
  EXPECT_FALSE(cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("abc"), bin("dx")}), r.m_rec, r.m_offs));
  EXPECT_FALSE(
    cmp_dtuple_is_prefix_of_rec(nullptr, mk_tuple(h.m_heap, {bin("abc"), bin("defgh"), bin("x")}), r.m_rec, r.m_offs)
  ) << "more fields than the record";
}

/** Columns of the same type are comparable; non-binary strings only with
the same charset-collation when it is checked. */
TEST_F(RecTest, ColsAreEqualTypes) {
  Column a{};
  Column b{};

  a.mtype = DATA_INT;
  a.prtype = DATA_UNSIGNED;
  a.len = 4;
  b = a;
  EXPECT_TRUE(cmp_cols_are_equal(&a, &b, false));

  b.prtype = 0;
  EXPECT_FALSE(cmp_cols_are_equal(&a, &b, false)) << "signed vs unsigned";

  b.prtype = DATA_UNSIGNED;
  b.len = 8;
  EXPECT_FALSE(cmp_cols_are_equal(&a, &b, false)) << "different widths";

  b.mtype = DATA_FLOAT;
  b.len = 4;
  EXPECT_FALSE(cmp_cols_are_equal(&a, &b, false));

  a.mtype = DATA_BINARY;
  a.prtype = 0;
  b.mtype = DATA_FIXBINARY;
  b.prtype = 0;
  EXPECT_TRUE(cmp_cols_are_equal(&a, &b, true)) << "binary strings";

  a.mtype = DATA_VARCHAR;
  a.prtype = dtype_form_prtype(DATA_ENGLISH, 8);
  b.mtype = DATA_CHAR;
  b.prtype = dtype_form_prtype(DATA_ENGLISH, 33);
  EXPECT_TRUE(cmp_cols_are_equal(&a, &b, false));
  EXPECT_FALSE(cmp_cols_are_equal(&a, &b, true)) << "different collations";
}

/** Two non-binary string columns with the same charset-collation are
comparable when the charsets are checked (foreign keys). */
TEST_F(RecTest, ColsAreEqualSameCharset) {
  /* Regression: cmp_cols_are_equal() (rem/rem0cmp.cc, check_charsets branch)
  returns dtype_get_charset_coll(col1->prtype == dtype_get_charset_coll(col2->prtype)):
  the parenthesis is misplaced, the charset of a bool is always 0, so two
  string columns are never equal when check_charsets is true. Upstream:
  dtype_get_charset_coll(col1->prtype) == dtype_get_charset_coll(col2->prtype). */
  Column a{};
  Column b{};

  a.mtype = DATA_VARCHAR;
  a.prtype = dtype_form_prtype(DATA_ENGLISH, 8);
  a.len = 10;
  b = a;
  b.mtype = DATA_CHAR;

  EXPECT_TRUE(cmp_cols_are_equal(&a, &b, true));
}

/** An update of a NULL fixed size field to a value of the same size does
not change the size of the record: RowUpdate can do it in place. */
TEST_F(RecTest, NullUpdateFitsInPlace) {
  /* Regression: RowUpdate::changes_field_size_or_external() (row/row0upd.cc:264)
  compares the new length with rec_offs_nth_size(), which for a NULL field
  n > 0 includes the REC_OFFS_SQL_NULL flag (see NthSizeOfNull):
  such updates are never done in place, upstream they are. */
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(1), int4(std::nullopt)});
  PRec r(t, 0);
  auto update = RowUpdate::upd_create(1, h.m_heap);
  auto uf = &update->m_fields[0];

  new (uf) UpdField();
  uf->m_field_no = 1;

  byte v[4];

  mach_write_to_4(v, 9);
  dtype_set(dfield_get_type(&uf->m_new_val), DATA_INT, DATA_UNSIGNED, 4);
  dfield_set_data(&uf->m_new_val, v, 4);

  EXPECT_FALSE(m_sys->m_row_upd->changes_field_size_or_external(nullptr, r.m_offs, update));
}

/** The same update of a field that is not NULL is in place; a different
length or an externally stored new value is not. */
TEST_F(RecTest, ChangesFieldSize) {
  Heap h;
  auto t = mk_tuple(h.m_heap, {int4(1), int4(2), bin("abc")});
  PRec r(t, 0);
  auto update = RowUpdate::upd_create(1, h.m_heap);
  auto uf = &update->m_fields[0];

  new (uf) UpdField();

  byte v[4];

  mach_write_to_4(v, 9);
  uf->m_field_no = 1;
  dtype_set(dfield_get_type(&uf->m_new_val), DATA_INT, DATA_UNSIGNED, 4);
  dfield_set_data(&uf->m_new_val, v, 4);
  EXPECT_FALSE(m_sys->m_row_upd->changes_field_size_or_external(nullptr, r.m_offs, update));

  uf->m_field_no = 2;
  dtype_set(dfield_get_type(&uf->m_new_val), DATA_BINARY, 0, 255);
  dfield_set_data(&uf->m_new_val, "xyz", 3);
  EXPECT_FALSE(m_sys->m_row_upd->changes_field_size_or_external(nullptr, r.m_offs, update));

  dfield_set_data(&uf->m_new_val, "xyzw", 4);
  EXPECT_TRUE(m_sys->m_row_upd->changes_field_size_or_external(nullptr, r.m_offs, update));

  dfield_set_data(&uf->m_new_val, "xyz", 3);
  dfield_set_ext(&uf->m_new_val);
  EXPECT_TRUE(m_sys->m_row_upd->changes_field_size_or_external(nullptr, r.m_offs, update));
}

/* --- Records in a real index ------------------------------------------ */

/** An integer type of a column. */
struct IntType {
  const char *m_name;
  ulint m_len;
  bool m_unsigned;
};

constexpr IntType INT_TYPES[] = {
  {"i1", 1, false}, {"u1", 1, true}, {"i2", 2, false}, {"u2", 2, true},
  {"i4", 4, false}, {"u4", 4, true}, {"i8", 8, false}, {"u8", 8, true},
};

/** Create table name(k <type> PRIMARY KEY, v VARCHAR(20) NULL). */
void create_int_table(System &sys, const std::string &name, const IntType &type) {
  (void)api::database_create(sys, "test");

  api::TableSchema *tbl_sch{};
  api::IndexSchema *idx_sch{};

  ASSERT_EQ(api::TableSchema::create(name.c_str(), &tbl_sch, IB_TBL_V1, 0), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_col("k", IB_INT, type.m_unsigned ? IB_COL_UNSIGNED : IB_COL_NONE, 0, type.m_len), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_col("v", IB_VARCHAR, IB_COL_NONE, 0, 20), DB_SUCCESS);
  ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);
  ASSERT_EQ(idx_sch->add_col("k", 0), DB_SUCCESS);
  ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

  auto trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  uint64_t id{};

  ASSERT_EQ(api::schema_lock_exclusive(sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_create(sys, trx, tbl_sch, &id), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(sys, trx), DB_SUCCESS);

  api::TableSchema::destroy(sys, tbl_sch);
}

/** @return the table from the dictionary cache. */
Table *get_table(System &sys, const std::string &name) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(name.c_str());
  sys.m_dict->mutex_release();

  return table;
}

/** The values of a type, from min to max, as int64 for signed and uint64 for unsigned. */
std::vector<std::pair<int64_t, uint64_t>> extremes(const IntType &type) {
  std::vector<std::pair<int64_t, uint64_t>> v;
  const auto bits = type.m_len * 8;

  if (type.m_unsigned) {
    const uint64_t max = bits == 64 ? UINT64_MAX : (1ULL << bits) - 1;

    for (uint64_t u : {uint64_t(0), uint64_t(1), uint64_t(2), max / 2, max / 2 + 1, max - 1, max}) {
      v.emplace_back(0, u);
    }
  } else {
    const int64_t max = bits == 64 ? INT64_MAX : int64_t((1ULL << (bits - 1)) - 1);
    const int64_t min = -max - 1;

    for (int64_t s : {min, min + 1, int64_t(-2), int64_t(-1), int64_t(0), int64_t(1), max - 1, max}) {
      v.emplace_back(s, 0);
    }
  }

  return v;
}

using RecDbTest = ut::DbTest;

/** Integer primary keys of every width and signedness, inserted in a
scrambled order, are stored in numeric order in the clustered index; the
records compare in that order with cmp_rec_rec and with the tuples of their
own values (signed integers ordered as unsigned was a bug). */
TEST_F(RecDbTest, IntKeyOrder) {
  for (const auto &type : INT_TYPES) {
    const auto name = std::string("test/") + type.m_name;

    create_int_table(*m_sys, name, type);

    auto vals = extremes(type);
    auto order = vals;

    std::reverse(order.begin(), order.end());
    std::rotate(order.begin(), order.begin() + 3, order.end());

    auto trx = begin();
    api::Cursor *crsr{};

    ASSERT_EQ(api::cursor_open_table(*m_sys, name.c_str(), trx, &crsr), DB_SUCCESS);
    ASSERT_EQ(crsr->lock(*m_sys, IB_LOCK_IX), DB_SUCCESS);

    for (const auto &[s, u] : order) {
      auto tpl = crsr->clust_read_tuple_create();

      switch (type.m_len * 2 + type.m_unsigned) {
        case 2: ASSERT_EQ(tpl->write_i8(0, int8_t(s)), DB_SUCCESS); break;
        case 3: ASSERT_EQ(tpl->write_u8(0, uint8_t(u)), DB_SUCCESS); break;
        case 4: ASSERT_EQ(tpl->write_i16(0, int16_t(s)), DB_SUCCESS); break;
        case 5: ASSERT_EQ(tpl->write_u16(0, uint16_t(u)), DB_SUCCESS); break;
        case 8: ASSERT_EQ(tpl->write_i32(0, int32_t(s)), DB_SUCCESS); break;
        case 9: ASSERT_EQ(tpl->write_u32(0, uint32_t(u)), DB_SUCCESS); break;
        case 16: ASSERT_EQ(tpl->write_i64(0, s), DB_SUCCESS); break;
        case 17: ASSERT_EQ(tpl->write_u64(0, u), DB_SUCCESS); break;
      }

      ASSERT_EQ(tpl->set_value(1, nullptr, api::IB_SQL_NULL), DB_SUCCESS);
      ASSERT_EQ(crsr->insert_row(*m_sys, tpl), DB_SUCCESS) << type.m_name << " " << s << " " << u;
      api::Tuple::destroy(tpl);
    }

    ASSERT_EQ(api::Cursor::close(*m_sys, crsr), DB_SUCCESS);
    commit(trx);

    /* Read the leaf records in index order. */
    auto index = get_table(*m_sys, name)->get_first_index();
    Mtr mtr;
    BtreePcursor pcur;
    std::vector<std::pair<int64_t, uint64_t>> got;
    std::vector<std::vector<byte>> recs;

    mtr.start();
    pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);

    while (pcur.move_to_next_user_rec(*m_sys, &mtr)) {
      auto rec = pcur.get_rec();
      MemHeap *heap{};
      std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};

      rec_offs_init(offs_);

      RecOffs offs = PhyRec(index, rec).get_all_col_offsets(offs_.data(), &heap, Current_location());
      ulint len;
      auto data = rec.get_nth_field(offs, 0, &len);

      ASSERT_EQ(len, type.m_len);

      if (type.m_unsigned) {
        uint64_t u{};

        mach_read_int_type(&u, data, len, true);
        got.emplace_back(0, u);
      } else {
        int64_t s{};

        mach_read_int_type(&s, data, len, false);

        /* Sign extend from the column width. */
        const auto shift = 64 - 8 * len;

        got.emplace_back(int64_t(uint64_t(s) << shift) >> shift, 0);
      }

      /* v is NULL in every row. */
      (void)rec.get_nth_field(offs, offs.n_fields() - 1, &len);
      EXPECT_EQ(len, UNIV_SQL_NULL);

      recs.emplace_back(rec.get() - offs.extra_size(), rec.get() + offs.data_size());
      recs.back().push_back(byte(offs.extra_size()));
    }

    pcur.close();
    mtr.commit(*m_sys);

    EXPECT_EQ(got, vals) << type.m_name;

    /* cmp_rec_rec on the copies, consecutive pairs. */
    for (size_t i = 1; i < recs.size(); ++i) {
      Rec r1(recs[i - 1].data() + recs[i - 1].back());
      Rec r2(recs[i].data() + recs[i].back());
      std::array<ulint, REC_OFFS_NORMAL_SIZE> o1_{};
      std::array<ulint, REC_OFFS_NORMAL_SIZE> o2_{};
      MemHeap *heap{};

      rec_offs_init(o1_);
      rec_offs_init(o2_);

      auto o1 = PhyRec(index, r1).get_all_col_offsets(o1_.data(), &heap, Current_location());
      auto o2 = PhyRec(index, r2).get_all_col_offsets(o2_.data(), &heap, Current_location());

      EXPECT_EQ(cmp_rec_rec(r1, r2, o1, o2, index), -1) << type.m_name << " " << i;
      EXPECT_EQ(cmp_rec_rec(r2, r1, o2, o1, index), 1) << type.m_name << " " << i;
      EXPECT_EQ(cmp_rec_rec(r1, r1, o1, o1, index), 0) << type.m_name << " " << i;
    }
  }
}

/** The record of a row converted back to a tuple with the index's types
compares equal to it; changing a key byte changes the order. */
TEST_F(RecDbTest, IndexRoundTrip) {
  load({{5, ut::Row{5, 50, "five"}}, {9, ut::Row{9, 90, "nine"}}});

  auto index = get_table(*m_sys, ut::TABLE)->get_first_index();
  Heap h;
  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  auto rec = pcur.get_rec();
  MemHeap *heap{};
  RecOffs offs = PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
  const auto n = offs.n_fields();

  EXPECT_EQ(n, index->get_n_fields());
  EXPECT_TRUE(rec.offs_validate(index, offs));
  EXPECT_TRUE(m_sys->m_btree->index_rec_validate(rec, index, true));

  auto tuple = dtuple_create(h.m_heap, n);

  index->copy_types(tuple, n);
  rec.copy_prefix_to_dtuple(tuple, index, n, h.m_heap);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, tuple, rec, offs), 0);

  /* The key 5 + 1: greater than the record, smaller than the next. */
  byte key[4];

  mach_write_to_4(key, 6);
  dfield_set_data(dtuple_get_nth_field(tuple, 0), key, 4);
  dtuple_set_n_fields_cmp(tuple, 1);
  EXPECT_EQ(cmp_dtuple_rec(nullptr, tuple, rec, offs), 1);

  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));
  rec = pcur.get_rec();
  offs = PhyRec(index, rec).get_all_col_offsets(offs, &heap, Current_location());
  EXPECT_EQ(cmp_dtuple_rec(nullptr, tuple, rec, offs), -1);

  pcur.close();
  mtr.commit(*m_sys);
  mem_heap_free(heap);
}


/** cmp_rec_rec_simple() compares two records with their own offsets, as
the merge sort of an index build does. */
TEST_F(RecDbTest, RecRecSimple) {
  /* Regression: cmp_rec_rec_simple() (rem/rem0cmp.cc:596) asserts
  rec_offs_base(offsets1) == rec_offs_base(offsets2): it compares the
  addresses of the two offsets arrays, which differ whenever two records
  are compared (row_merge_cmp() passes two arrays), so a debug build
  aborts. Upstream asserts rec_offs_comp(offsets1) == rec_offs_comp(offsets2),
  the format flag, which this engine no longer has. */
  load({{5, ut::Row{5, 50, "five"}}, {9, ut::Row{9, 90, "nine"}}});

  auto index = get_table(*m_sys, ut::TABLE)->get_first_index();
  Mtr mtr;
  BtreePcursor pcur;

  mtr.start();
  pcur.open_at_index_side(*m_sys, true, index, BTR_SEARCH_LEAF, true, 0, &mtr);
  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  auto r1 = pcur.get_rec();

  ASSERT_TRUE(pcur.move_to_next_user_rec(*m_sys, &mtr));

  auto r2 = pcur.get_rec();
  MemHeap *heap{};
  RecOffs o1 = PhyRec(index, r1).get_all_col_offsets(nullptr, &heap, Current_location());
  RecOffs o2 = PhyRec(index, r2).get_all_col_offsets(nullptr, &heap, Current_location());

  EXPECT_EQ(cmp_rec_rec_simple(r1, r2, o1, o2, index), -1);
  EXPECT_EQ(cmp_rec_rec_simple(r2, r1, o2, o1, index), 1);

  pcur.close();
  mtr.commit(*m_sys);
  mem_heap_free(heap);
}

}  // namespace

/* --- Performance --------------------------------------------------------- */

namespace {

using RecPerf = ut::SysTest;

/** Offsets of a record of 8 fields, compare of a tuple with it. */
TEST_F(RecPerf, OffsetsCompare) {
  Heap h;
  std::vector<F> fs;

  for (int i = 0; i < 8; ++i) {
    fs.push_back(i % 2 == 0 ? int4(uint32_t(i)) : bin(std::string(20, char('a' + i))));
  }

  auto t = mk_tuple(h.m_heap, fs);
  PRec r(t, 0);
  std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};

  rec_offs_init(offs_);

  auto ro = ut::bench("get_col_offsets", 2000000, [&](uint64_t) {
    MemHeap *heap{};
    auto offs = PhyRec(nullptr, r.m_rec).get_all_col_offsets(offs_.data(), &heap, Current_location());
    ut::keep(offs);
  });

  EXPECT_LT(ro.ns_per_op(), 5000.0);

  auto rc = ut::bench("cmp_dtuple_rec", 2000000, [&](uint64_t) {
    auto ret = cmp_dtuple_rec(nullptr, t, r.m_rec, r.m_offs);
    ut::keep(ret);
  });

  EXPECT_LT(rc.ns_per_op(), 50000.0);

  auto rv = ut::bench("convert_dtuple_to_rec", 500000, [&](uint64_t) {
    auto rec = Rec::convert_dtuple_to_rec(r.m_buf.data(), nullptr, t, 0);
    ut::keep(rec);
  });

  EXPECT_LT(rv.ns_per_op(), 50000.0);
}

/** Compare of integer and string fields. */
TEST_F(RecPerf, CmpData) {
  const auto a = int_bytes<int64_t>(-12345, false);
  const auto b = int_bytes<int64_t>(12345, false);
  const std::string s1(64, 'x');
  const std::string s2 = std::string(63, 'x') + "y";

  auto ri = ut::bench("cmp_int8", 5000000, [&](uint64_t) {
    auto ret = cmp(DATA_INT, 0, a, b);
    ut::keep(ret);
  });

  EXPECT_LT(ri.ns_per_op(), 10000.0);

  auto rs = ut::bench("cmp_varchar64", 2000000, [&](uint64_t) {
    auto ret = cmp(DATA_VARCHAR, DATA_ENGLISH, s1, s2);
    ut::keep(ret);
  });

  EXPECT_LT(rs.ns_per_op(), 20000.0);
}

}  // namespace
