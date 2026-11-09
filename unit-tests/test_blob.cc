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

/** @file unit-tests/test_blob.cc
The stores of the fields a record is too big for, through BlobStore and the generic layer of
blob0blob.h only: every store, on every tree. Edges of the lengths, prefixes, frees, who owns a
field; and the local cost of a write, a read and a free. */

#include "ut0env.h"
#include "ut0perf.h"

#include "blob0blob.h"
#include "blob0inline.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string>
#include <tuple>
#include <vector>

namespace {

using Kinds = std::tuple<BlobStoreKind, IndexAmKind>;

class BlobTest : public ut::DbTest, public ::testing::WithParamInterface<Kinds> {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_dict.m_blob_store = std::get<0>(GetParam());
    m_config.m_dict.m_index_am = std::get<1>(GetParam());

    start();

    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(ut::TABLE)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  [[nodiscard]] BlobStore &store() { return m_sys->blob_store(); }

  using Id = std::array<byte, BlobStore::ID_SIZE>;

  /** Store data, in a mini-transaction of its own. */
  [[nodiscard]] Id write(const std::string &data) {
    Id id{};
    Mtr mtr;

    mtr.start();
    store().prepare(m_index, &mtr, false);
    EXPECT_EQ(store().write(m_index, reinterpret_cast<const byte *>(data.data()), data.size(), 0, id.data(), &mtr), DB_SUCCESS);
    mtr.commit(*m_sys);

    return id;
  }

  /** @return the first len bytes of the field id finds. */
  [[nodiscard]] std::string read(const Id &id, ulint len) {
    std::string s(len, '\0');

    s.resize(store().read(id.data(), reinterpret_cast<byte *>(s.data()), len));

    return s;
  }

  void free(const Id &id) {
    Mtr mtr;

    mtr.start();
    store().prepare(m_index, &mtr, false);
    store().free(m_index, id.data(), &mtr);
    mtr.commit(*m_sys);
  }

  /** @return a reference: a prefix of local bytes, id, the flags and len. */
  [[nodiscard]] std::vector<byte> field(const std::string &local, const Id &id, ulint flags, ulint len) {
    std::vector<byte> f(local.begin(), local.end());
    const auto ref = f.size();

    f.resize(ref + BTR_EXTERN_FIELD_REF_SIZE);
    memcpy(f.data() + ref, id.data(), id.size());
    f[ref + BTR_EXTERN_LEN] = byte(flags);
    mach_write_to_4(f.data() + ref + BTR_EXTERN_LEN + 4, len);

    return f;
  }

  Index *m_index{};
};

/** The lengths at the edges of a page of the inline store, and well past them. */
const std::vector<ulint> LENGTHS{1, 2, BTR_BLOB_PART_MAX - 1, BTR_BLOB_PART_MAX, BTR_BLOB_PART_MAX + 1, 2 * BTR_BLOB_PART_MAX, 3 * BTR_BLOB_PART_MAX + 7, 1 << 20};

/** What's written is read back, whole and by its prefixes; more than there is is what there is. */
TEST_P(BlobTest, RoundTrip) {
  for (const auto len : LENGTHS) {
    const auto data = ut::blob(uint32_t(len), len);
    const auto id = write(data);

    EXPECT_EQ(read(id, len), data) << len;
    EXPECT_EQ(read(id, 0), "") << len;
    EXPECT_EQ(read(id, 1), data.substr(0, 1)) << len;
    EXPECT_EQ(read(id, len - 1), data.substr(0, len - 1)) << len;
    EXPECT_EQ(read(id, len + 100), data) << "no more than there is, " << len;

    free(id);
  }
}

/** Fields stay apart: each finds its own, freeing one leaves the others. */
TEST_P(BlobTest, Many) {
  std::vector<std::pair<Id, std::string>> fields;

  for (uint32_t i{}; i < 20; ++i) {
    const auto data = ut::blob(i, 1 + i * 3000);

    fields.emplace_back(write(data), data);
  }

  for (size_t i{}; i < fields.size(); i += 2) {
    free(fields[i].first);
  }

  for (size_t i{1}; i < fields.size(); i += 2) {
    EXPECT_EQ(read(fields[i].first, fields[i].second.size()), fields[i].second) << i;
    free(fields[i].first);
  }
}

/** Several fields and their frees in one mini-transaction: one change. */
TEST_P(BlobTest, OneMtr) {
  const auto a = ut::blob(1, 3 * BTR_BLOB_PART_MAX);
  const auto b = ut::blob(2, 100);
  Id ida{};
  Id idb{};
  Mtr mtr;

  mtr.start();
  store().prepare(m_index, &mtr, false);
  ASSERT_EQ(store().write(m_index, reinterpret_cast<const byte *>(a.data()), a.size(), 0, ida.data(), &mtr), DB_SUCCESS);
  ASSERT_EQ(store().write(m_index, reinterpret_cast<const byte *>(b.data()), b.size(), 0, idb.data(), &mtr), DB_SUCCESS);
  mtr.commit(*m_sys);

  EXPECT_EQ(read(ida, a.size()), a);
  EXPECT_EQ(read(idb, b.size()), b);

  mtr.start();
  store().prepare(m_index, &mtr, false);
  store().free(m_index, ida.data(), &mtr);
  store().free(m_index, idb.data(), &mtr);
  mtr.commit(*m_sys);
}

/** The pages for the statistics: none for nothing, then one more a page of the store at a time. */
TEST_P(BlobTest, Pages) {
  EXPECT_EQ(store().n_pages(0), 0UL);
  EXPECT_EQ(store().n_pages(1), 1UL);

  ulint last{};

  for (const auto len : LENGTHS) {
    const auto n = store().n_pages(len);

    EXPECT_GE(n, last) << len;
    EXPECT_LE(n, len) << len;
    last = n;
  }

  if (std::get<0>(GetParam()) == BlobStoreKind::INLINE) {
    EXPECT_EQ(store().n_pages(BTR_BLOB_PART_MAX), 1UL);
    EXPECT_EQ(store().n_pages(BTR_BLOB_PART_MAX + 1), 2UL);
  }
}

/** A reference of length 0 refers to nothing: no prefix past the record's, nothing to free. */
TEST_P(BlobTest, RefToNothing) {
  Blob blob{};
  const auto f = field("local", Id{}, 0, 0);
  std::array<byte, 100> buf{};
  auto heap = mem_heap_create(256);
  ulint len;

  EXPECT_EQ(blob.copy_externally_stored_field_prefix(*m_sys, buf.data(), buf.size(), f.data(), f.size()), 0UL);

  const auto copy = blob.copy_externally_stored_field(*m_sys, &len, f.data(), f.size(), heap);

  EXPECT_EQ(std::string(reinterpret_cast<const char *>(copy), len), "local");

  Mtr mtr;

  mtr.start();
  blob.prepare(*m_sys, m_index, &mtr, false);
  EXPECT_FALSE(blob.free_ref(*m_sys, m_index, f.data() + 5, RB_NONE, &mtr));
  mtr.commit(*m_sys);

  mem_heap_free(heap);
}

/** The prefix of a field: the record's bytes, then the store's, up to the length asked. */
TEST_P(BlobTest, Prefix) {
  Blob blob{};
  const auto data = ut::blob(7, 2 * BTR_BLOB_PART_MAX + 5);
  const auto id = write(data);
  const auto f = field("abc", id, 0, data.size());
  const auto whole = "abc" + data;

  for (const ulint len : {ulint{1}, ulint{3}, ulint{4}, ulint{BTR_BLOB_PART_MAX}, ulint{whole.size()}, ulint{whole.size() + 9}}) {
    std::vector<byte> buf(len);
    const auto n = blob.copy_externally_stored_field_prefix(*m_sys, buf.data(), len, f.data(), f.size());

    ASSERT_EQ(n, std::min<ulint>(len, whole.size())) << len;
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(buf.data()), n), whole.substr(0, n)) << len;
  }

  auto heap = mem_heap_create(256);
  ulint len;
  const auto copy = blob.copy_externally_stored_field(*m_sys, &len, f.data(), f.size(), heap);

  EXPECT_EQ(std::string(reinterpret_cast<const char *>(copy), len), whole);

  mem_heap_free(heap);
  free(id);
}

/** Only the owner frees, and a rollback doesn't free what's inherited; what's freed refers to nothing. */
TEST_P(BlobTest, Owner) {
  Blob blob{};
  const auto data = ut::blob(3, BTR_BLOB_PART_MAX + 1);
  const auto id = write(data);

  const auto free_ref = [&](ulint flags, TrxRbCtx rb_ctx) {
    auto f = field("", id, flags, data.size());
    Mtr mtr;

    mtr.start();
    blob.prepare(*m_sys, m_index, &mtr, false);

    const auto freed = blob.free_ref(*m_sys, m_index, f.data(), rb_ctx, &mtr);

    mtr.commit(*m_sys);

    return freed;
  };

  EXPECT_FALSE(free_ref(BTR_EXTERN_OWNER_FLAG, RB_NONE)) << "not the owner";
  EXPECT_FALSE(free_ref(BTR_EXTERN_INHERITED_FLAG, RB_NORMAL)) << "a rollback, inherited";
  EXPECT_FALSE(free_ref(BTR_EXTERN_INHERITED_FLAG, RB_RECOVERY)) << "a rollback in recovery, inherited";
  EXPECT_EQ(read(id, data.size()), data) << "still there";

  /* The owner frees it, an inherited field included outside a rollback. */
  EXPECT_TRUE(free_ref(BTR_EXTERN_INHERITED_FLAG, RB_NONE));

  /* A reference made to refer to nothing has nothing to free. */
  auto f = field("", id, 0, 0);
  Mtr mtr;

  mtr.start();
  blob.prepare(*m_sys, m_index, &mtr, false);
  EXPECT_FALSE(blob.free_ref(*m_sys, m_index, f.data(), RB_NONE, &mtr));
  mtr.commit(*m_sys);
}

/** Rows with fields of every size through the row layer, to the store and back, on this store. */
TEST_P(BlobTest, Rows) {
  ut::Rows rows;
  uint32_t k{};

  for (size_t len : {0, 1, 767, 768, 8000, 8200, 16384, 100000}) {
    rows[k] = ut::Row{k, k, ut::blob(k, len)};
    ++k;
  }

  load(rows);
  expect_rows(rows);

  auto trx = begin();

  for (auto &[key, row] : rows) {
    row.m_b = ut::blob(key + 50, row.m_b.size() == 0 ? 30000 : row.m_b.size() / 2);
    ASSERT_EQ(update(trx, key, row), DB_SUCCESS) << key;
  }

  commit(trx);
  expect_rows(rows);
}

const auto ALL = ::testing::Combine(::testing::Values(BlobStoreKind::INLINE, BlobStoreKind::MEM), ::testing::Values(IndexAmKind::BTREE, IndexAmKind::BLINK));

[[nodiscard]] std::string kinds_name(const ::testing::TestParamInfo<Kinds> &info) {
  return std::string(std::get<0>(info.param) == BlobStoreKind::INLINE ? "Inline" : "Mem") + (std::get<1>(info.param) == IndexAmKind::BTREE ? "Btree" : "Blink");
}

INSTANTIATE_TEST_SUITE_P(Stores, BlobTest, ALL, kinds_name);

/** Performance, a suite of its own: run only in a Release build (ut0perf.h). */
class BlobPerf : public BlobTest {};

/** The local cost of a field of 64K: written, read, freed, each in one mini-transaction. */
TEST_P(BlobPerf, WriteReadFree) {
  const auto data = ut::blob(1, 64 * 1024);

  const auto r = ut::bench("blob_64k_write_read_free", 200, [&](uint64_t) {
    const auto id = write(data);

    ut::keep(read(id, data.size()).size());
    free(id);
  });

  /* Per byte, a generous bound. */
  EXPECT_LT(r.ns_per_op() / double(data.size()), 50.0);
}

INSTANTIATE_TEST_SUITE_P(Stores, BlobPerf, ALL, kinds_name);

}  // namespace
