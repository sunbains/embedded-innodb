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

/** @file unit-tests/test_mtr.cc
Mtr: the mini-transaction handle, its log and its memo. */

#include "ut0env.h"

#include "buf0buf.h"
#include "fil0fil.h"
#include "log0recv.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0rw.h"
#include "ut0perf.h"

#include <random>
#include <vector>

namespace {

using MtrTest = ut::SysTest;

/** Put n bytes of log in mtr, as one record. */
void add_log(Mtr &mtr, ulint n) {
  auto ptr = static_cast<byte *>(mtr.m_log.push(n));

  memset(ptr, 0x5a, n);
  ++mtr.m_n_log_recs;
}

/** A started mtr has an empty log and memo. */
TEST_F(MtrTest, StartsEmpty) {
  Mtr mtr;

  mtr.start();

  EXPECT_EQ(mtr.m_log.get_data_size(), 0UL);
  EXPECT_EQ(mtr.m_memo.get_data_size(), 0UL);
  EXPECT_EQ(mtr.m_n_log_recs, 0UL);
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_ALL);

  mtr.commit(*m_sys);
}

/** An Mtr started again after a commit starts with an empty log: start()
did not reset the log, so the previous mini-transaction's records were
written to the redo log again in front of the next one's, with the wrong
single-record/multi-record framing. Recovery then failed to parse it. */
TEST_F(MtrTest, RestartClearsLog) {
  Mtr mtr;

  mtr.start();
  add_log(mtr, 10);
  EXPECT_EQ(mtr.m_log.get_data_size(), 10UL);

  /* No page was modified: commit writes nothing, the log stays in the handle. */
  mtr.commit(*m_sys);

  mtr.start();

  EXPECT_EQ(mtr.m_log.get_data_size(), 0UL);
  EXPECT_EQ(mtr.m_n_log_recs, 0UL);
  EXPECT_TRUE(mtr.m_log.single_block());

  mtr.commit(*m_sys);
}

/** The same with a log spanning several blocks of the dynamic array. */
TEST_F(MtrTest, RestartClearsBigLog) {
  Mtr mtr;

  mtr.start();

  for (int i = 0; i < 20; ++i) {
    add_log(mtr, 300);
  }

  EXPECT_EQ(mtr.m_log.get_data_size(), 6000UL);
  EXPECT_FALSE(mtr.m_log.single_block());

  mtr.commit(*m_sys);

  for (int round = 0; round < 3; ++round) {
    mtr.start();

    EXPECT_EQ(mtr.m_log.get_data_size(), 0UL);
    EXPECT_TRUE(mtr.m_log.single_block());

    add_log(mtr, 100);
    EXPECT_EQ(mtr.m_log.get_data_size(), 100UL);

    mtr.commit(*m_sys);
  }
}

/** set_log_mode() returns the previous mode, start() resets it. */
TEST_F(MtrTest, LogMode) {
  Mtr mtr;

  mtr.start();
  EXPECT_EQ(mtr.set_log_mode(MTR_LOG_NONE), MTR_LOG_ALL);
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_NONE);
  mtr.commit(*m_sys);

  mtr.start();
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_ALL);
  mtr.commit(*m_sys);
}


/** @return the log of mtr, block by block. */
std::vector<byte> log_of(const Mtr &mtr) {
  std::vector<byte> v;

  for (auto block = mtr.m_log.get_first_block(); block != nullptr; block = mtr.m_log.get_next_block(block)) {
    auto data = mtr.m_log.get_data(block);

    v.insert(v.end(), data, data + mtr.m_log.get_used(block));
  }

  return v;
}

/** A record of a mini-transaction log as recovery parses it. */
struct Rec {
  MlogType m_type{};
  space_id_t m_space{};
  page_no_t m_page_no{};
  std::vector<byte> m_body;
  bool m_single{};
};

/** Parse log, records back to back, as recovery does: every record must
parse and end exactly where the next begins. */
std::vector<Rec> parse_log(System &sys, std::vector<byte> log) {
  std::vector<Rec> recs;
  auto ptr = log.data();
  auto end = log.data() + log.size();

  while (ptr < end) {
    Rec rec;

    rec.m_single = (*ptr & MLOG_SINGLE_REC_FLAG) != 0;

    if ((*ptr & ~MLOG_SINGLE_REC_FLAG) == MLOG_MULTI_REC_END) {
      rec.m_type = MLOG_MULTI_REC_END;
      recs.push_back(rec);
      ++ptr;
      continue;
    }

    auto body = mlog_parse_initial_log_record(ptr, end, &rec.m_type, &rec.m_space, &rec.m_page_no);

    EXPECT_NE(body, nullptr) << "at " << ptr - log.data();

    if (body == nullptr) {
      break;
    }

    auto next = recv_parse_or_apply_log_rec_body(sys, rec.m_type, body, end, nullptr, nullptr);

    EXPECT_NE(next, nullptr) << "type " << int(rec.m_type) << " at " << ptr - log.data();

    if (next == nullptr) {
      break;
    }

    rec.m_body.assign(body, next);
    recs.push_back(rec);
    ptr = next;
  }

  EXPECT_EQ(ptr, end);
  EXPECT_FALSE(sys.m_recv->m_found_corrupt_log);

  return recs;
}

/** Mini-transactions on pages of the buffer pool that belong to no file:
blocks in the BUF_BLOCK_MEMORY state, x-fixed in the memo by hand. The
writers see real frames, and nothing is read from or written to disk. */
class MtrPageTest : public ut::SysTest {
 protected:
  void TearDown() override {
    for (auto block : m_blocks) {
      m_sys->m_buf_pool->block_free(block);
    }

    m_blocks.clear();

    ut::SysTest::TearDown();
  }

  /** @return a zeroed frame stamped as page (space, page_no) of type SYS. */
  BufBlock *page(space_id_t space, page_no_t page_no) {
    auto block = m_sys->m_buf_pool->block_alloc();
    auto frame = block->get_frame();

    memset(frame, 0, UNIV_PAGE_SIZE);
    mach_write_to_4(frame + FIL_PAGE_SPACE_ID, space);
    mach_write_to_4(frame + FIL_PAGE_OFFSET, page_no);
    mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS);

    m_blocks.push_back(block);

    return block;
  }

  /** Start mtr and x-fix block in its memo, as BufPool::get() would. */
  void start(Mtr &mtr, BufBlock *block) {
    mtr.start();
    mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);
  }

  /** Commit mtr without releasing the hand-made memo entries: the log is
  framed (single record flag or MLOG_MULTI_REC_END) as for the redo log. */
  void commit(Mtr &mtr) {
    mtr.m_memo.clear();
    mtr.commit(*m_sys);
  }

  /** @return a page image of type SYS to apply records to. */
  static std::vector<byte> image() {
    std::vector<byte> v(UNIV_PAGE_SIZE);

    mach_write_to_2(v.data() + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS);

    return v;
  }

  std::vector<BufBlock *> m_blocks;
};

/** The values at the boundaries of the compressed encoding, for a width. */
std::vector<ulint> values_of(MlogType type) {
  switch (type) {
    case MLOG_1BYTE:
      return {0, 1, 0x7F, 0x80, 0xFF};
    case MLOG_2BYTES:
      return {0, 0x7F, 0x80, 0x3FFF, 0x4000, 0xFFFF};
    default:
      return {0, 0x7F, 0x80, 0x3FFF, 0x4000, 0x1FFFFF, 0x200000, 0xFFFFFFF, 0x10000000, 0xFEFFFFFF, 0xFF000000, 0xFFFFFFFF};
  }
}

/** mlog_write_ulint() writes the page and logs type, space, page number,
offset and the compressed value; recovery parses exactly that record and
applies the same value. */
TEST_F(MtrPageTest, WriteUlint) {
  auto block = page(7, 300);
  auto frame = block->get_frame();

  for (auto type : {MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES}) {
    for (auto v : values_of(type)) {
      Mtr mtr;
      const ulint off = 100 + type;

      start(mtr, block);
      mlog_write_ulint(frame + off, v, type, &mtr);

      EXPECT_EQ(mtr.read_ulint(frame + off, type), v);
      EXPECT_EQ(mtr.m_n_log_recs, 1U);
      EXPECT_TRUE(mtr.m_modifications);

      const auto log = log_of(mtr);

      /* type, space 7, page 300 (2 bytes), offset, value */
      ASSERT_EQ(log.size(), 1 + 1 + 2 + 2 + mach_get_compressed_size(v)) << int(type) << " " << v;
      EXPECT_EQ(log[0], type);
      EXPECT_EQ(log[1], 7);
      EXPECT_EQ(mach_read_from_2(log.data() + 4), off);

      const auto recs = parse_log(*m_sys, log);

      ASSERT_EQ(recs.size(), 1UL);
      EXPECT_EQ(recs[0].m_type, type);
      EXPECT_EQ(recs[0].m_space, 7U);
      EXPECT_EQ(recs[0].m_page_no, 300U);

      auto img = image();
      auto body = recs[0].m_body;

      EXPECT_EQ(mlog_parse_nbytes(*m_sys, type, body.data(), body.data() + body.size(), img.data()), body.data() + body.size());
      EXPECT_EQ(memcmp(img.data() + off, frame + off, type), 0) << int(type) << " " << v;

      commit(mtr);
    }
  }
}

/** mlog_write_uint64() logs the 8 bytes compressed; the parser restores them. */
TEST_F(MtrPageTest, WriteUint64) {
  auto block = page(3, 9);
  auto frame = block->get_frame();

  for (uint64_t v : {0ULL, 1ULL, 0xFFFFFFFFULL, 0x100000000ULL, 0x7F12345678ULL, 0x8000000000000000ULL, ~0ULL}) {
    Mtr mtr;

    start(mtr, block);
    mlog_write_uint64(frame + 200, v, &mtr);

    EXPECT_EQ(mach_read_from_8(frame + 200), v);
    EXPECT_EQ(mtr.read_uint64(frame + 200), v);

    const auto log = log_of(mtr);

    EXPECT_EQ(log.size(), 3 + 2 + mach_uint64_get_compressed_size(v));

    const auto recs = parse_log(*m_sys, log);

    ASSERT_EQ(recs.size(), 1UL);
    EXPECT_EQ(recs[0].m_type, MLOG_8BYTES);

    auto img = image();
    auto body = recs[0].m_body;

    (void)mlog_parse_nbytes(*m_sys, MLOG_8BYTES, body.data(), body.data() + body.size(), img.data());
    EXPECT_EQ(mach_read_from_8(img.data() + 200), v);

    commit(mtr);
  }
}

/** Space ids and page numbers at the boundaries of their compressed
encoding come back from the record header. */
TEST_F(MtrPageTest, HeaderSpacePage) {
  for (space_id_t space : {0U, 0x7FU, 0x80U, 0x3FFFU, 0x4000U, 0xFFFFFFF0U}) {
    for (page_no_t page_no : {0U, 0x7FU, 0x80U, 0x3FFFU, 0x4000U, 0x10000000U}) {
      if (space == 0 && page_no >= FSP_EXTENT_SIZE && page_no < 3 * FSP_EXTENT_SIZE) {
        continue;
      }

      auto block = page(space, page_no);
      Mtr mtr;

      start(mtr, block);
      mlog_write_ulint(block->get_frame() + 64, 1, MLOG_1BYTE, &mtr);

      const auto recs = parse_log(*m_sys, log_of(mtr));

      ASSERT_EQ(recs.size(), 1UL);
      EXPECT_EQ(recs[0].m_space, space);
      EXPECT_EQ(recs[0].m_page_no, page_no);

      commit(mtr);
    }
  }
}

/** mlog_write_string() logs offset, length and the bytes; strings longer
than a block of the mtr log are split over several blocks. */
TEST_F(MtrPageTest, WriteString) {
  auto block = page(4, 44);
  auto frame = block->get_frame();

  for (ulint len : {1UL, 100UL, 500UL, 1000UL, 8000UL, ulint(UNIV_PAGE_SIZE) - 200}) {
    std::vector<byte> s(len);

    for (ulint i = 0; i < len; ++i) {
      s[i] = byte(i * 31 + len);
    }

    Mtr mtr;

    start(mtr, block);
    mlog_write_string(frame + 100, s.data(), len, &mtr);

    EXPECT_EQ(memcmp(frame + 100, s.data(), len), 0);

    const auto log = log_of(mtr);

    EXPECT_EQ(log.size(), 3 + 2 + 2 + len);

    const auto recs = parse_log(*m_sys, log);

    ASSERT_EQ(recs.size(), 1UL);
    EXPECT_EQ(recs[0].m_type, MLOG_WRITE_STRING);

    auto img = image();
    auto body = recs[0].m_body;

    EXPECT_EQ(mlog_parse_string(*m_sys, body.data(), body.data() + body.size(), img.data()), body.data() + body.size());
    EXPECT_EQ(memcmp(img.data() + 100, s.data(), len), 0) << len;

    commit(mtr);
  }
}

/** mlog_log_string() logs what is already on the page. */
TEST_F(MtrPageTest, LogString) {
  auto block = page(4, 45);
  auto frame = block->get_frame();

  memcpy(frame + 500, "already there", 13);

  Mtr mtr;

  start(mtr, block);
  mlog_log_string(frame + 500, 13, &mtr);

  const auto recs = parse_log(*m_sys, log_of(mtr));

  ASSERT_EQ(recs.size(), 1UL);

  auto img = image();
  auto body = recs[0].m_body;

  (void)mlog_parse_string(*m_sys, body.data(), body.data() + body.size(), img.data());
  EXPECT_EQ(memcmp(img.data() + 500, "already there", 13), 0);

  commit(mtr);
}

/** mlog_write_initial_log_record() writes only the header of a record whose
body the caller adds. */
TEST_F(MtrPageTest, InitialRecord) {
  auto block = page(9, 1000);
  Mtr mtr;

  start(mtr, block);
  mlog_write_initial_log_record(block->get_frame() + 120, MLOG_REC_MIN_MARK, &mtr);

  const auto log = log_of(mtr);

  ASSERT_EQ(log.size(), 4UL);
  EXPECT_EQ(log[0], MLOG_REC_MIN_MARK);
  EXPECT_EQ(log[1], 9);
  EXPECT_EQ(mach_read_from_2(log.data() + 2), 0x8000UL | 1000);
  EXPECT_EQ(mtr.m_n_log_recs, 1U);

  commit(mtr);
}

/** The catenate helpers append raw big endian or compressed values. */
TEST_F(MtrPageTest, Catenate) {
  Mtr mtr;

  mtr.start();

  mlog_catenate_ulint(&mtr, 0xAB, MLOG_1BYTE);
  mlog_catenate_ulint(&mtr, 0xABCD, MLOG_2BYTES);
  mlog_catenate_ulint(&mtr, 0x01234567, MLOG_4BYTES);
  mlog_catenate_ulint_compressed(&mtr, 0x4000);
  mlog_catenate_uint64_compressed(&mtr, 0x0000008000000001ULL);
  mlog_catenate_string(&mtr, reinterpret_cast<const byte *>("xyz"), 3);

  const std::vector<byte> expected{
    0xAB, 0xAB, 0xCD, 0x01, 0x23, 0x45, 0x67, 0xC0, 0x40, 0x00, 0x80, 0x80, 0x00, 0x00, 0x00, 0x01, 'x', 'y', 'z'
  };

  EXPECT_EQ(log_of(mtr), expected);
  EXPECT_EQ(mtr.m_n_log_recs, 0U);

  mtr.commit(*m_sys);
}

/** mlog_open() reserves room, mlog_close() keeps only what was used. */
TEST_F(MtrPageTest, OpenClose) {
  Mtr mtr;

  mtr.start();

  auto p = mlog_open(&mtr, 30);

  ASSERT_NE(p, nullptr);
  EXPECT_TRUE(mtr.m_modifications);

  p[0] = 1;
  p[1] = 2;
  mlog_close(&mtr, p + 2);

  EXPECT_EQ(log_of(mtr), (std::vector<byte>{1, 2}));

  mtr.commit(*m_sys);
}

/** With MTR_LOG_NONE the writers change the page and log nothing. */
TEST_F(MtrPageTest, LogNone) {
  auto block = page(5, 5);
  auto frame = block->get_frame();
  Mtr mtr;

  start(mtr, block);
  (void)mtr.set_log_mode(MTR_LOG_NONE);

  mlog_write_ulint(frame + 100, 0x1234, MLOG_2BYTES, &mtr);
  mlog_write_uint64(frame + 200, 42, &mtr);
  mlog_write_string(frame + 300, reinterpret_cast<const byte *>("abc"), 3, &mtr);
  mlog_catenate_ulint(&mtr, 5, MLOG_4BYTES);
  mlog_catenate_ulint_compressed(&mtr, 5);
  mlog_catenate_string(&mtr, reinterpret_cast<const byte *>("abc"), 3);

  EXPECT_EQ(mlog_open(&mtr, 10), nullptr);

  EXPECT_EQ(mach_read_from_2(frame + 100), 0x1234UL);
  EXPECT_EQ(mach_read_from_8(frame + 200), 42ULL);
  EXPECT_EQ(memcmp(frame + 300, "abc", 3), 0);

  EXPECT_EQ(mtr.m_log.get_data_size(), 0UL);
  EXPECT_EQ(mtr.m_n_log_recs, 0U);
  EXPECT_TRUE(mtr.m_modifications);

  commit(mtr);
}

/** MTR_LOG_SHORT_INSERTS does not turn logging back on after MTR_LOG_NONE. */
TEST_F(MtrTest, ShortInsertsAfterNone) {
  Mtr mtr;

  mtr.start();
  EXPECT_EQ(mtr.set_log_mode(MTR_LOG_NONE), MTR_LOG_ALL);
  EXPECT_EQ(mtr.set_log_mode(MTR_LOG_SHORT_INSERTS), MTR_LOG_NONE);
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_NONE);
  mtr.commit(*m_sys);

  mtr.start();
  EXPECT_EQ(mtr.set_log_mode(MTR_LOG_SHORT_INSERTS), MTR_LOG_ALL);
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_SHORT_INSERTS);
  mtr.commit(*m_sys);

  mtr.start();
  mtr.disable_redo_logging();
  EXPECT_EQ(mtr.get_log_mode(), MTR_LOG_NONE);
  mtr.commit(*m_sys);
}

/** The states of the handle. */
TEST_F(MtrTest, States) {
  Mtr mtr;

  EXPECT_FALSE(mtr.is_active());
  mtr.start();
  EXPECT_TRUE(mtr.is_active());
  EXPECT_EQ(mtr.m_state, MTR_ACTIVE);
  mtr.commit(*m_sys);
  EXPECT_FALSE(mtr.is_active());
  EXPECT_EQ(mtr.m_state, MTR_COMMITTED);
}

/** The log of a mini-transaction with one record is marked with
MLOG_SINGLE_REC_FLAG at commit; nothing is added. */
TEST_F(MtrPageTest, SingleRecFlag) {
  auto block = page(6, 60);
  Mtr mtr;

  start(mtr, block);
  mlog_write_ulint(block->get_frame() + 80, 7, MLOG_4BYTES, &mtr);

  const auto before = log_of(mtr);

  commit(mtr);

  const auto after = log_of(mtr);

  ASSERT_EQ(after.size(), before.size());
  EXPECT_EQ(after[0], before[0] | MLOG_SINGLE_REC_FLAG);
  EXPECT_TRUE(std::equal(after.begin() + 1, after.end(), before.begin() + 1));

  const auto recs = parse_log(*m_sys, after);

  ASSERT_EQ(recs.size(), 1UL);
  EXPECT_TRUE(recs[0].m_single);
  EXPECT_EQ(recs[0].m_type, MLOG_4BYTES);
}

/** The log of a mini-transaction with several records ends with
MLOG_MULTI_REC_END after the commit; no record has the single flag. */
TEST_F(MtrPageTest, MultiRecEnd) {
  auto block = page(6, 61);
  auto frame = block->get_frame();
  Mtr mtr;

  start(mtr, block);
  mlog_write_ulint(frame + 80, 7, MLOG_4BYTES, &mtr);
  mlog_write_ulint(frame + 90, 8, MLOG_1BYTE, &mtr);
  mlog_write_string(frame + 100, reinterpret_cast<const byte *>("abc"), 3, &mtr);

  EXPECT_EQ(mtr.m_n_log_recs, 3U);

  const auto size = mtr.m_log.get_data_size();

  commit(mtr);

  const auto log = log_of(mtr);

  ASSERT_EQ(log.size(), size + 1);
  EXPECT_EQ(log.back(), MLOG_MULTI_REC_END);

  const auto recs = parse_log(*m_sys, log);

  ASSERT_EQ(recs.size(), 4UL);

  for (const auto &rec : recs) {
    EXPECT_FALSE(rec.m_single);
  }

  EXPECT_EQ(recs[3].m_type, MLOG_MULTI_REC_END);
}

/** A file operation record counts as a record. */
TEST_F(MtrPageTest, FileOpRecord) {
  Mtr mtr;

  mtr.start();

  auto p = mlog_open(&mtr, 11);

  p = mlog_write_initial_log_record_for_file_op(MLOG_FILE_DELETE, 0x1234, 0, p, &mtr);
  mlog_close(&mtr, p);

  EXPECT_EQ(mtr.m_n_log_recs, 1U);

  const auto log = log_of(mtr);

  ASSERT_EQ(log.size(), 1 + 2 + 1UL);
  EXPECT_EQ(log[0], MLOG_FILE_DELETE);

  mtr.m_modifications = false;
  mtr.commit(*m_sys);
}

/** Hundreds of records over two pages make a log of many blocks; parsed
and applied to page images they reproduce both pages. */
TEST_F(MtrPageTest, ManyRecordsReplay) {
  auto b1 = page(11, 1);
  auto b2 = page(11, 2);
  std::mt19937 rng(9);
  Mtr mtr;

  start(mtr, b1);
  mtr.memo_push(b2, MTR_MEMO_PAGE_X_FIX);

  for (int i = 0; i < 600; ++i) {
    auto frame = (i % 2 == 0 ? b1 : b2)->get_frame();
    const ulint off = FIL_PAGE_DATA + (rng() % 2000) * 4;

    switch (rng() % 4) {
      case 0:
        mlog_write_ulint(frame + off, rng() & 0xFF, MLOG_1BYTE, &mtr);
        break;
      case 1:
        mlog_write_ulint(frame + off, rng() & 0xFFFF, MLOG_2BYTES, &mtr);
        break;
      case 2:
        mlog_write_ulint(frame + off, uint32_t(rng()), MLOG_4BYTES, &mtr);
        break;
      default:
        mlog_write_uint64(frame + off, (uint64_t(rng()) << 32) | rng(), &mtr);
    }
  }

  EXPECT_FALSE(mtr.m_log.single_block());

  commit(mtr);

  auto img1 = image();
  auto img2 = image();

  for (auto &rec : parse_log(*m_sys, log_of(mtr))) {
    if (rec.m_type == MLOG_MULTI_REC_END) {
      continue;
    }

    auto &img = rec.m_page_no == 1 ? img1 : img2;

    (void)mlog_parse_nbytes(*m_sys, rec.m_type, rec.m_body.data(), rec.m_body.data() + rec.m_body.size(), img.data());
  }

  /* The header (type, space, page no) was not logged: compare the data. */
  EXPECT_EQ(memcmp(img1.data() + FIL_PAGE_DATA, b1->get_frame() + FIL_PAGE_DATA, 8100), 0);
  EXPECT_EQ(memcmp(img2.data() + FIL_PAGE_DATA, b2->get_frame() + FIL_PAGE_DATA, 8100), 0);
}

/** An mtr with rw-locks in its memo. */
class MtrMemoTest : public ut::SysTest {
 protected:
  void SetUp() override {
    ut::SysTest::SetUp();

    for (auto &lock : m_locks) {
      rw_lock_create(&lock, SYNC_NO_ORDER_CHECK);
    }
  }

  void TearDown() override {
    for (auto &lock : m_locks) {
      EXPECT_EQ(lock.m_lock_word, X_LOCK_DECR);
      rw_lock_free(&lock);
    }

    ut::SysTest::TearDown();
  }

  std::array<RwLock, 40> m_locks;
};

/** commit() releases every latch in the memo, s and x. */
TEST_F(MtrMemoTest, CommitReleases) {
  Mtr mtr;

  mtr.start();

  for (size_t i = 0; i < m_locks.size(); ++i) {
    if (i % 2 == 0) {
      mtr_s_lock(&m_locks[i], &mtr);
      EXPECT_EQ(rw_lock_get_reader_count(&m_locks[i]), 1UL);
    } else {
      mtr_x_lock(&m_locks[i], &mtr);
      EXPECT_EQ(rw_lock_get_writer(&m_locks[i]), RW_LOCK_EX);
    }
  }

  /* 40 slots span several blocks of the memo. */
  EXPECT_EQ(mtr.m_memo.get_data_size(), m_locks.size() * sizeof(MtrMemoSlot));
  EXPECT_FALSE(mtr.m_memo.single_block());

#ifdef UNIV_DEBUG
  EXPECT_TRUE(mtr.memo_contains(&m_locks[0], MTR_MEMO_S_LOCK));
  EXPECT_FALSE(mtr.memo_contains(&m_locks[0], MTR_MEMO_X_LOCK));
  EXPECT_TRUE(mtr.memo_contains(&m_locks[39], MTR_MEMO_X_LOCK));
#endif /* UNIV_DEBUG */

  mtr.commit(*m_sys);
}

/** The same latch x-locked twice in one mtr is released twice. */
TEST_F(MtrMemoTest, RecursiveX) {
  Mtr mtr;

  mtr.start();
  mtr_x_lock(&m_locks[0], &mtr);
  mtr_x_lock(&m_locks[0], &mtr);

  EXPECT_EQ(rw_lock_get_x_lock_count(&m_locks[0]), 2UL);

  mtr.commit(*m_sys);
}

/** A savepoint is the memo size; rolling back to it releases only the
latches taken after it, the rest go at commit. */
TEST_F(MtrMemoTest, Savepoint) {
  Mtr mtr;

  mtr.start();

  for (int i = 0; i < 5; ++i) {
    mtr_x_lock(&m_locks[i], &mtr);
  }

  const auto sp = mtr.set_savepoint();

  EXPECT_EQ(sp, 5 * sizeof(MtrMemoSlot));
  EXPECT_EQ(mtr.get_savepoint(), sp);

  for (int i = 5; i < 30; ++i) {
    mtr_s_lock(&m_locks[i], &mtr);
  }

  mtr.rollback_to_savepoint(*m_sys, sp);

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(rw_lock_get_writer(&m_locks[i]), RW_LOCK_EX) << i;
  }

  for (int i = 5; i < 30; ++i) {
    EXPECT_EQ(m_locks[i].m_lock_word, X_LOCK_DECR) << i;
  }

  mtr.commit(*m_sys);
}

/** release_s_latch_at_savepoint() releases the s-latch taken at the savepoint. */
TEST_F(MtrMemoTest, ReleaseSAtSavepoint) {
  Mtr mtr;

  mtr.start();
  mtr_x_lock(&m_locks[0], &mtr);

  const auto sp = mtr.set_savepoint();

  mtr_s_lock(&m_locks[1], &mtr);
  mtr_s_lock(&m_locks[2], &mtr);

  mtr.release_s_latch_at_savepoint(sp, &m_locks[1]);

  EXPECT_EQ(m_locks[1].m_lock_word, X_LOCK_DECR);
  EXPECT_EQ(rw_lock_get_reader_count(&m_locks[2]), 1UL);

  mtr.commit(*m_sys);
}

/** memo_release() releases the latest matching slot only. */
TEST_F(MtrMemoTest, MemoRelease) {
  Mtr mtr;

  mtr.start();
  mtr_s_lock(&m_locks[3], &mtr);
  mtr_x_lock(&m_locks[4], &mtr);

  mtr.memo_release(*m_sys, &m_locks[4], MTR_MEMO_X_LOCK);
  EXPECT_EQ(m_locks[4].m_lock_word, X_LOCK_DECR);

  /* Wrong type: nothing released. */
  mtr.memo_release(*m_sys, &m_locks[3], MTR_MEMO_X_LOCK);
  EXPECT_EQ(rw_lock_get_reader_count(&m_locks[3]), 1UL);

  mtr.memo_release(*m_sys, &m_locks[3], MTR_MEMO_S_LOCK);
  EXPECT_EQ(m_locks[3].m_lock_word, X_LOCK_DECR);

#ifdef UNIV_DEBUG
  EXPECT_FALSE(mtr.memo_contains(&m_locks[3], MTR_MEMO_S_LOCK));
#endif /* UNIV_DEBUG */

  mtr.commit(*m_sys);
}

/** A restarted mtr has an empty memo. */
TEST_F(MtrMemoTest, RestartClearsMemo) {
  Mtr mtr;

  mtr.start();

  for (int i = 0; i < 40; ++i) {
    mtr_s_lock(&m_locks[i], &mtr);
  }

  mtr.commit(*m_sys);
  mtr.start();

  EXPECT_EQ(mtr.m_memo.get_data_size(), 0UL);
  EXPECT_TRUE(mtr.m_memo.single_block());
  EXPECT_EQ(mtr.set_savepoint(), 0UL);

  mtr.commit(*m_sys);
}

}  // namespace

namespace {

using MtrPerf = ut::SysTest;

/** Start and commit of an empty mini-transaction, the floor of every change. */
TEST_F(MtrPerf, StartCommit) {
  auto r = ut::bench("start_commit", 1000000, [&](uint64_t) {
    Mtr mtr;

    mtr.start();
    mtr.commit(*m_sys);
  });

  EXPECT_LT(r.ns_per_op(), 20000.0);
}


/** Start and commit of a mini-transaction with n 4-byte records on a page
(no WAL in a SysTest: the log is framed and dropped). */
class MtrPagePerf : public MtrPageTest {};

TEST_F(MtrPagePerf, CommitRecords) {
  auto block = page(12, 12);
  auto frame = block->get_frame();

  for (int n : {1, 10, 100}) {
    auto r = ut::bench(std::format("start_write_{}_commit", n), 200000 / n, [&](uint64_t i) {
      Mtr mtr;

      start(mtr, block);

      for (int j = 0; j < n; ++j) {
        mlog_write_ulint(frame + FIL_PAGE_DATA + 4 * j, uint32_t(i + j), MLOG_4BYTES, &mtr);
      }

      commit(mtr);
    });

    EXPECT_LT(r.ns_per_op(), 2000.0 * n + 20000.0);
  }
}

/** Memo push and release of 8 latches. */
TEST_F(MtrPerf, MemoLatches) {
  std::array<RwLock, 8> locks;

  for (auto &lock : locks) {
    rw_lock_create(&lock, SYNC_NO_ORDER_CHECK);
  }

  auto r = ut::bench("start_8_x_latches_commit", 500000, [&](uint64_t) {
    Mtr mtr;

    mtr.start();

    for (auto &lock : locks) {
      mtr_x_lock(&lock, &mtr);
    }

    mtr.commit(*m_sys);
  });

  for (auto &lock : locks) {
    rw_lock_free(&lock);
  }

  EXPECT_LT(r.ns_per_op(), 50000.0);
}

}  // namespace
