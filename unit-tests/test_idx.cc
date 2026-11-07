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

/** @file unit-tests/test_idx.cc
The index access method: what any implementation has to do. These tests go
through the interface only, so that they can run on every implementation. */

#include "ut0env.h"

#include "btr0blob.h"
#include "btr0cur.h"
#include "dict0dict.h"
#include "idx0am.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "pars0pars.h"
#include "row0sel.h"
#include "row0upd.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

namespace {

constexpr uint32_t N_ROWS = 2000;

/** No undo log and locks: the transaction here is only the redo context of the changes. */
constexpr ulint DML_FLAGS = BTR_NO_UNDO_LOG_FLAG | BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG;

/** No locks, the insert is undo logged as it is for row0ins: that is what lets an insert get
the space for restructuring by itself. */
constexpr ulint INS_FLAGS = BTR_NO_LOCKING_FLAG;

/** The fields of a record of the clustered index: k, DB_TRX_ID, DB_ROLL_PTR, a, b. */
constexpr ulint A_FIELD = 3;
constexpr ulint B_FIELD = 4;

class IdxTest : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();

    ut::Rows rows;

    for (uint32_t k{1}; k <= N_ROWS; ++k) {
      rows.emplace(k, ut::Row{k, k, std::string(100, 'x')});
    }

    load(rows);

    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(ut::TABLE)->get_first_index();
    m_sys->m_dict->mutex_release();

    m_heap = mem_heap_create(16384);
  }

  void TearDown() override {
    mem_heap_free(m_heap);
    ut::DbTest::TearDown();
  }

  /** @return a query thread of trx, for the changes. */
  [[nodiscard]] QueThr *thr_of(Trx *trx) { return pars_complete_graph_for_exec(SelNode::create(m_heap), trx, m_heap); }

  [[nodiscard]] byte *u32(uint32_t v) {
    auto p = static_cast<byte *>(mem_heap_alloc(m_heap, 4));

    mach_write_to_4(p, v);

    return p;
  }

  /** @return a search key for k. */
  [[nodiscard]] DTuple *key(uint32_t k) {
    auto t = dtuple_create(m_heap, 1);

    m_index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(k), 4);
    dtuple_set_n_fields_cmp(t, 1);

    return t;
  }

  /** @return a record for k, with a b of blen bytes. */
  [[nodiscard]] DTuple *entry(uint32_t k, ulint blen = 100) {
    const auto n = m_index->get_n_fields();
    auto t = dtuple_create(m_heap, n);

    m_index->copy_types(t, n);
    dfield_set_data(dtuple_get_nth_field(t, 0), u32(k), 4);

    auto sys = mem_heap_zalloc(m_heap, DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, DATA_TRX_ID_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + DATA_TRX_ID_LEN, DATA_ROLL_PTR_LEN);
    dfield_set_data(dtuple_get_nth_field(t, A_FIELD), u32(k), 4);

    auto b = static_cast<byte *>(mem_heap_alloc(m_heap, blen));

    memset(b, 'y', blen);
    dfield_set_data(dtuple_get_nth_field(t, B_FIELD), b, blen);
    dtuple_set_n_fields_cmp(t, m_index->get_n_unique_in_tree());

    return t;
  }

  /** @return an update of one field. */
  [[nodiscard]] Upd *upd_of(ulint field_no, const byte *data, ulint len) {
    auto upd = RowUpdate::upd_create(1, m_heap);
    auto field = upd->get_nth_field(0);

    field->m_field_no = field_no;
    field->m_orig_len = 0;
    field->m_exp = nullptr;
    dfield_set_data(&field->m_new_val, data, len);

    return upd;
  }

  /** @return the bytes of a field of the record for k. */
  [[nodiscard]] std::string field_of(uint32_t k, ulint field_no) {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    std::string val;

    if (cur.find(m_index, key(k), Intent::Read, &mtr)) {
      auto rec = cur.rec();
      RecOffs offsets = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &m_heap, Current_location());
      ulint len;
      const auto offset = offsets.get_nth_field(field_no, &len);

      val.assign(reinterpret_cast<const char *>(rec.get() + offset), len);
    }

    cur.close();
    mtr.commit(*m_sys);

    return val;
  }

  /** Insert e the way a caller has to: Write, and Restructure after DB_FAIL.
  @param[out] n_fail  Incremented if it took Restructure. */
  [[nodiscard]] DbErr put(DTuple *e, QueThr *thr, BigRec *big, ulint *n_fail) {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_insert(m_index, e, Intent::Write, thr, &mtr);

    auto err = cur.insert(Intent::Write, INS_FLAGS, e, big, 0, thr, &mtr);

    mtr.commit(*m_sys);

    if (err == DB_FAIL) {
      ++*n_fail;
      mtr.start();
      cur.seek_insert(m_index, e, Intent::Restructure, thr, &mtr);
      err = cur.insert(Intent::Restructure, INS_FLAGS, e, big, 0, thr, &mtr);
      mtr.commit(*m_sys);
    }

    return err;
  }

  /** @return the key of rec, it's the first column. */
  [[nodiscard]] uint32_t key_of(Rec rec) {
    auto heap = mem_heap_create(256);
    RecOffs offsets = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;
    const auto offset = offsets.get_nth_field(0, &len);
    const auto k = mach_read_from_4(rec.get() + offset);

    mem_heap_free(heap);

    return uint32_t(k);
  }

  /** Position cur on the first record with a key >= k. */
  void seek(IndexCursor &cur, uint32_t k, Mtr &mtr) {
    auto heap = mem_heap_create(256);
    auto key = dtuple_create(heap, 1);

    m_index->copy_types(key, 1);

    auto buf = static_cast<byte *>(mem_heap_alloc(heap, 4));

    mach_write_to_4(buf, k);
    dfield_set_data(dtuple_get_nth_field(key, 0), buf, 4);
    dtuple_set_n_fields_cmp(key, 1);

    cur.seek(m_index, key, PAGE_CUR_GE, Intent::Read, &mtr);

    mem_heap_free(heap);
  }

  Index *m_index{};
  MemHeap *m_heap{};
};

/** Every record, in order. */
TEST_F(IdxTest, Scan) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);
  EXPECT_FALSE(cur.on_rec()) << "before the first record";

  uint32_t n{};

  while (cur.next(&mtr)) {
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), ++n);
  }

  EXPECT_EQ(n, N_ROWS);
  EXPECT_FALSE(cur.on_rec()) << "after the last record";

  cur.close();
  mtr.commit(*m_sys);
}

/** Every record, backwards. */
TEST_F(IdxTest, ScanBack) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);
  EXPECT_FALSE(cur.on_rec()) << "after the last record";

  auto n = N_ROWS;

  while (cur.prev(&mtr)) {
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), n--);
  }

  EXPECT_EQ(n, 0U);

  cur.close();
  mtr.commit(*m_sys);
}

/** A seek gets the first record at or after the key, or nothing past the last one. */
TEST_F(IdxTest, Seek) {
  for (uint32_t k : {1U, 2U, N_ROWS / 2, N_ROWS}) {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    seek(cur, k, mtr);
    ASSERT_TRUE(cur.on_rec()) << k;
    EXPECT_EQ(key_of(cur.rec()), k);
    cur.close();
    mtr.commit(*m_sys);
  }

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  seek(cur, N_ROWS + 1, mtr);
  EXPECT_FALSE(cur.on_rec());
  cur.close();
  mtr.commit(*m_sys);
}

/** find() says if the record is there: the whole key has to match. */
TEST_F(IdxTest, Find) {
  auto find = [this](uint32_t k) {
    auto heap = mem_heap_create(256);
    auto key = dtuple_create(heap, 1);

    m_index->copy_types(key, 1);

    auto buf = static_cast<byte *>(mem_heap_alloc(heap, 4));

    mach_write_to_4(buf, k);
    dfield_set_data(dtuple_get_nth_field(key, 0), buf, 4);
    dtuple_set_n_fields_cmp(key, 1);

    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, key, Intent::Read, &mtr);

    if (found) {
      EXPECT_EQ(key_of(cur.rec()), k);
    }

    cur.close();
    mtr.commit(*m_sys);
    mem_heap_free(heap);

    return found;
  };

  EXPECT_TRUE(find(1));
  EXPECT_TRUE(find(N_ROWS / 2));
  EXPECT_TRUE(find(N_ROWS));
  EXPECT_FALSE(find(0));
  EXPECT_FALSE(find(N_ROWS + 1));
}

/** commit() lets go of the record, restore() gets it back. */
TEST_F(IdxTest, CommitRestore) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  seek(cur, 100, mtr);
  cur.save(&mtr);
  cur.commit(&mtr);

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Write, &mtr));
  EXPECT_EQ(key_of(cur.rec()), 100U);
  cur.commit(&mtr);

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Restructure, &mtr));
  EXPECT_EQ(key_of(cur.rec()), 100U);
  cur.close();
  mtr.commit(*m_sys);
}

/** A position outlives its mini-transaction. */
TEST_F(IdxTest, SaveRestore) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  seek(cur, 100, mtr);
  cur.save(&mtr);
  mtr.commit(*m_sys);

  mtr.start();
  EXPECT_TRUE(cur.restore(Intent::Read, &mtr));
  EXPECT_EQ(key_of(cur.rec()), 100U);
  EXPECT_TRUE(cur.next(&mtr));
  EXPECT_EQ(key_of(cur.rec()), 101U);
  cur.close();
  mtr.commit(*m_sys);
}

/** position() stops where the search ends, record or not, and says how much matched. */
TEST_F(IdxTest, Position) {
  auto at = [this](uint32_t k, SearchMode mode, auto check) {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.position(m_index, key(k), mode, Intent::Read, &mtr);
    EXPECT_EQ(cur.search_mode(), mode);
    EXPECT_EQ(cur.index(), m_index);
    check(cur, mtr);
    cur.close();
    mtr.commit(*m_sys);
  };

  at(100, PAGE_CUR_GE, [this](IndexCursor &cur, Mtr &) {
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), 100U);
    EXPECT_EQ(cur.up_match(), 1UL);
    /* For the lock API. */
    EXPECT_NE(cur.block(), nullptr);
  });

  at(100, PAGE_CUR_LE, [this](IndexCursor &cur, Mtr &) {
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), 100U);
    EXPECT_EQ(cur.low_match(), 1UL);
  });

  /* Past the last record: seek() would say the same, but position() doesn't look further. */
  at(N_ROWS + 1, PAGE_CUR_GE, [](IndexCursor &cur, Mtr &mtr) {
    EXPECT_FALSE(cur.on_rec());
    EXPECT_FALSE(cur.next_pos(&mtr));
    EXPECT_EQ(cur.up_match(), 0UL);
  });

  /* Before the first record. */
  at(0, PAGE_CUR_LE, [](IndexCursor &cur, Mtr &mtr) {
    EXPECT_FALSE(cur.on_rec());
    EXPECT_FALSE(cur.prev_pos(&mtr));
    EXPECT_EQ(cur.low_match(), 0UL);
  });
}

/** next_pos() gets to every record, in order, whatever else it stops on. */
TEST_F(IdxTest, NextPos) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_first(m_index, Intent::Read, &mtr);

  uint32_t n{};

  while (cur.next_pos(&mtr)) {
    if (cur.on_rec()) {
      EXPECT_EQ(key_of(cur.rec()), ++n);
    }
  }

  EXPECT_EQ(n, N_ROWS);
  EXPECT_FALSE(cur.on_rec());
  cur.close();
  mtr.commit(*m_sys);
}

/** prev_pos() the same, backwards. */
TEST_F(IdxTest, PrevPos) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.seek_last(m_index, Intent::Read, &mtr);

  auto n = N_ROWS;

  while (cur.prev_pos(&mtr)) {
    if (cur.on_rec()) {
      EXPECT_EQ(key_of(cur.rec()), n--);
    }
  }

  EXPECT_EQ(n, 0U);
  cur.close();
  mtr.commit(*m_sys);
}

/** skip_rest() goes off the records, and the scan goes on after what was skipped. */
TEST_F(IdxTest, SkipRest) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  cur.position(m_index, key(1), PAGE_CUR_GE, Intent::Read, &mtr);
  cur.skip_rest(&mtr);
  EXPECT_FALSE(cur.on_rec());

  bool found{};

  while (!found && cur.next_pos(&mtr)) {
    found = cur.on_rec();
  }

  ASSERT_TRUE(found);
  EXPECT_GT(key_of(cur.rec()), 1U);
  cur.close();
  mtr.commit(*m_sys);
}

/** saved_at() says where the saved position is. */
TEST_F(IdxTest, SavedAt) {
  IndexCursor cur{*m_sys};

  EXPECT_FALSE(cur.has_saved()) << "never opened";

  {
    Mtr mtr;

    mtr.start();
    seek(cur, 100, mtr);
    EXPECT_FALSE(cur.has_saved());
    cur.save(&mtr);
    EXPECT_TRUE(cur.has_saved());
    EXPECT_EQ(cur.saved_at(), SavedAt::On);
    cur.commit(&mtr);
    EXPECT_TRUE(cur.has_saved()) << "it outlives the mini-transaction";
  }

  {
    Mtr mtr;

    mtr.start();
    cur.position(m_index, key(N_ROWS + 1), PAGE_CUR_GE, Intent::Read, &mtr);
    cur.save(&mtr);
    EXPECT_EQ(cur.saved_at(), SavedAt::After);
    mtr.commit(*m_sys);
  }

  {
    Mtr mtr;

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);
    cur.save(&mtr);
    EXPECT_EQ(cur.saved_at(), SavedAt::Before);
    cur.close();
    mtr.commit(*m_sys);
  }

  EXPECT_FALSE(cur.has_saved()) << "closed";
}

/** copy_saved() onto a cursor that was never opened: it is a copy, the source can go. */
TEST_F(IdxTest, CopySaved) {
  Mtr mtr;
  IndexCursor src{*m_sys};
  IndexCursor dst;

  dst.attach(*m_sys);

  mtr.start();
  seek(src, 100, mtr);
  src.save(&mtr);
  src.commit(&mtr);

  dst.copy_saved(src);
  src.close();
  src.release();

  EXPECT_TRUE(dst.has_saved());
  EXPECT_EQ(dst.saved_at(), SavedAt::On);

  mtr.start();
  EXPECT_TRUE(dst.restore(Intent::Read, &mtr));
  EXPECT_EQ(key_of(dst.rec()), 100U);
  dst.close();
  mtr.commit(*m_sys);
}

/** release() gives up everything, the cursor can be opened again. */
TEST_F(IdxTest, Release) {
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  seek(cur, 100, mtr);
  cur.save(&mtr);
  mtr.commit(*m_sys);

  cur.release();
  EXPECT_FALSE(cur.has_saved());

  mtr.start();
  seek(cur, 200, mtr);
  EXPECT_EQ(key_of(cur.rec()), 200U);
  cur.close();
  mtr.commit(*m_sys);
}

/** seek_insert() ends on the last record <= the entry, and says how much that matches. */
TEST_F(IdxTest, SeekInsert) {
  auto trx = begin();
  auto thr = thr_of(trx);

  {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_insert(m_index, entry(100), Intent::Write, thr, &mtr);
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), 100U);
    EXPECT_GE(cur.low_match(), 1UL);
    mtr.commit(*m_sys);
  }

  {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_insert(m_index, entry(N_ROWS + 1), Intent::Write, thr, &mtr);
    ASSERT_TRUE(cur.on_rec());
    EXPECT_EQ(key_of(cur.rec()), N_ROWS);
    EXPECT_EQ(cur.low_match(), 0UL);
    EXPECT_EQ(cur.up_match(), 0UL);
    mtr.commit(*m_sys);
  }

  commit(trx);
}

/** insert(): enough records that some of them take Restructure, all of them are there. */
TEST_F(IdxTest, Insert) {
  auto trx = begin();
  auto thr = thr_of(trx);
  ulint n_fail{};

  for (uint32_t k{N_ROWS + 1}; k <= N_ROWS + 500; ++k) {
    BigRec big{};

    ASSERT_EQ(put(entry(k), thr, &big, &n_fail), DB_SUCCESS) << k;
    EXPECT_EQ(big.n_fields, 0UL);
  }

  commit(trx);

  for (uint32_t k{N_ROWS + 1}; k <= N_ROWS + 500; ++k) {
    EXPECT_EQ(field_of(k, A_FIELD), std::string(reinterpret_cast<const char *>(u32(k)), 4)) << k;
    EXPECT_EQ(field_of(k, B_FIELD), std::string(100, 'y')) << k;
  }
}

/** insert() of a record that doesn't fit leaves fields to store_big_rec(). */
TEST_F(IdxTest, StoreBigRec) {
  auto trx = begin();
  auto thr = thr_of(trx);
  const auto k = N_ROWS + 1;
  auto e = entry(k, 200000);
  BigRec big{};
  ulint n_fail{};

  ASSERT_EQ(put(e, thr, &big, &n_fail), DB_SUCCESS);
  ASSERT_GT(big.n_fields, 0UL);

  {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.position(m_index, e, PAGE_CUR_LE, Intent::Restructure, &mtr);
    ASSERT_TRUE(cur.on_rec());

    RecOffs offsets = PhyRec(m_index, cur.rec()).get_all_col_offsets(nullptr, &m_heap, Current_location());

    EXPECT_TRUE(offsets.any_extern());
    EXPECT_EQ(cur.store_big_rec(offsets, big, &mtr), DB_SUCCESS);
    mtr.commit(*m_sys);
  }

  dtuple_convert_back_big_rec(e, &big);
  commit(trx);

  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(k), Intent::Read, &mtr));

  auto rec = cur.rec();
  RecOffs offsets = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &m_heap, Current_location());
  Blob blob{};
  ulint len;
  auto b = blob.copy_externally_stored_field(*m_sys, rec, offsets, B_FIELD, &len, m_heap);

  EXPECT_EQ(std::string(reinterpret_cast<const char *>(b), len), std::string(200000, 'y'));
  cur.close();
  mtr.commit(*m_sys);
}

/** update_in_place() of a field of the same size. */
TEST_F(IdxTest, UpdateInPlace) {
  auto trx = begin();
  auto thr = thr_of(trx);
  const auto val = u32(4242);
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(100), Intent::Write, &mtr));
  EXPECT_EQ(cur.update_in_place(DML_FLAGS, upd_of(A_FIELD, val, 4), 0, thr, &mtr), DB_SUCCESS);
  cur.close();
  mtr.commit(*m_sys);
  commit(trx);

  EXPECT_EQ(field_of(100, A_FIELD), std::string(reinterpret_cast<const char *>(val), 4));
}

/** update() of a record to more than fits where it is: Write says so or does it, Restructure
does it. */
TEST_F(IdxTest, Update) {
  auto trx = begin();
  auto thr = thr_of(trx);
  const std::string val(5000, 'z');
  auto data = reinterpret_cast<const byte *>(val.data());
  Mtr mtr;
  IndexCursor cur{*m_sys};

  mtr.start();
  ASSERT_TRUE(cur.find(m_index, key(100), Intent::Write, &mtr));
  cur.save(&mtr);

  auto err = cur.update(Intent::Write, DML_FLAGS, nullptr, nullptr, upd_of(B_FIELD, data, val.size()), 0, thr, &mtr);

  mtr.commit(*m_sys);

  if (err == DB_FAIL) {
    MemHeap *heap{};
    BigRec big{};

    mtr.start();
    ASSERT_TRUE(cur.restore(Intent::Restructure, &mtr));
    err = cur.update(Intent::Restructure, DML_FLAGS, &heap, &big, upd_of(B_FIELD, data, val.size()), 0, thr, &mtr);
    mtr.commit(*m_sys);

    EXPECT_EQ(big.n_fields, 0UL);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }
  }

  EXPECT_EQ(err, DB_SUCCESS);
  commit(trx);

  EXPECT_EQ(field_of(100, B_FIELD), val);
  EXPECT_EQ(field_of(101, B_FIELD), std::string(100, 'x'));
}

/** The statistics come from the implementation. */
TEST_F(IdxTest, Sizes) {
  auto &am = m_sys->am(m_index);

  EXPECT_GT(am.n_pages(m_index), 1UL);
  EXPECT_GT(am.n_data_pages(m_index), 1UL);
  EXPECT_LE(am.n_data_pages(m_index), am.n_pages(m_index));
}

}  // namespace
