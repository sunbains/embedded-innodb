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

/** @file unit-tests/test_recv.cc
RecvSys: parsing redo records, and crash recovery of a killed instance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "dict0dict.h"
#include "fil0fil.h"
#include "log0log.h"
#include "log0recv.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0types.h"
#include "trx0types.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <map>
#include <thread>
#include <vector>

namespace {

using RecvParseTest = ut::SysTest;

/** Parse the body of a record of type from body, followed by trailing
bytes that are not part of it. @return the bytes the parser consumed. */
ulint parse(System &sys, MlogType type, std::vector<byte> body) {
  const auto len = body.size();

  /* Something that is not part of the record follows it. */
  body.insert(body.end(), {0xEE, 0xEE, 0xEE, 0xEE, 0xEE, 0xEE, 0xEE, 0xEE});

  auto ptr = body.data();
  auto end = recv_parse_or_apply_log_rec_body(sys, type, ptr, ptr + body.size(), nullptr, nullptr);

  EXPECT_NE(end, nullptr);

  const auto consumed = end == nullptr ? 0 : ulint(end - ptr);

  EXPECT_EQ(consumed, len) << "type " << int(type);

  return consumed;
}

/** The body of a clustered index delete mark: flags, value, the system
columns (position, roll pointer, trx id) and the record offset. */
std::vector<byte> clust_del_mark_body(trx_id_t trx_id) {
  std::vector<byte> body(64);
  auto ptr = body.data();

  *ptr++ = 0;  /* flags */
  *ptr++ = 1;  /* delete mark */
  ptr += varint::encode(ptr, 1);  /* position of DATA_TRX_ID */
  memset(ptr, 0, DATA_ROLL_PTR_LEN);
  ptr += DATA_ROLL_PTR_LEN;
  ptr += varint::encode64(ptr, trx_id);
  mach_write_to_2(ptr, 0x0d47);  /* record offset */
  ptr += 2;

  body.resize(ptr - body.data());

  return body;
}

/** MLOG_REC_CLUST_DELETE_MARK ends where it was written. The case fell
through into MLOG_REC_SEC_DELETE_MARK, which parsed 3 more bytes: the
parsing of the rest of the redo log went off the rails. */
TEST_F(RecvParseTest, ClustDelMark) {
  for (trx_id_t trx_id : {trx_id_t{0}, trx_id_t{2806}, trx_id_t{1} << 40}) {
    (void)parse(*m_sys, MLOG_REC_CLUST_DELETE_MARK, clust_del_mark_body(trx_id));
  }
}

/** MLOG_REC_SEC_DELETE_MARK: value, record offset, and the transaction. */
TEST_F(RecvParseTest, SecDelMark) {
  (void)parse(*m_sys, MLOG_REC_SEC_DELETE_MARK, {1, 0x01, 0x23, 0, 0, 0, 0, 0, 0, 0, 0});
}

/** MLOG_nBYTES: offset and the value, a varint for 4 and 8 bytes. */
TEST_F(RecvParseTest, NBytes) {
  std::vector<byte> body(16);
  auto ptr = body.data();

  mach_write_to_2(ptr, 38);
  ptr += 2;
  ptr += varint::encode(ptr, 0x12345);
  body.resize(ptr - body.data());

  (void)parse(*m_sys, MLOG_4BYTES, body);

  body.assign(16, 0);
  ptr = body.data();
  mach_write_to_2(ptr, 56);
  ptr += 2;
  ptr += varint::encode64(ptr, 0xaf6);
  body.resize(ptr - body.data());

  (void)parse(*m_sys, MLOG_8BYTES, body);
}


/** A buffer that ends at an inaccessible page: a parser that reads past
the end of the bytes it is given faults at once. */
class Guarded {
 public:
  explicit Guarded(size_t max) {
    const auto page = size_t(::sysconf(_SC_PAGESIZE));

    m_size = (max + page - 1) / page * page + page;
    m_mem = static_cast<byte *>(::mmap(nullptr, m_size + page, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

    EXPECT_NE(m_mem, MAP_FAILED);
    EXPECT_EQ(::mprotect(m_mem + m_size, page, PROT_NONE), 0);
  }

  ~Guarded() { ::munmap(m_mem, m_size + size_t(::sysconf(_SC_PAGESIZE))); }

  Guarded(const Guarded &) = delete;
  Guarded &operator=(const Guarded &) = delete;

  /** Copy n bytes so that they end at the guard page. @return the copy */
  byte *place(const byte *data, size_t n) {
    auto p = m_mem + m_size - n;

    if (n > 0) {
      memcpy(p, data, n);
    }

    return p;
  }

  /** @return the guard page: one past the end of a placed copy. */
  byte *end() const { return m_mem + m_size; }

 private:
  byte *m_mem{};
  size_t m_size{};
};

/** The lengths to try of a record of len bytes: all of them for a short
record, a spread for a long one. */
std::vector<size_t> prefixes(size_t len) {
  std::vector<size_t> v;

  for (size_t n = 0; n < len; n += (n < 64 || len - n < 64) ? 1 : std::max<size_t>(1, len / 64)) {
    v.push_back(n);
  }

  return v;
}

/** The body of a record of type parses to exactly its end, and every
truncation of it asks for more data (nullptr) without reading past the
end and without calling it corrupt. */
void expect_body(System &sys, MlogType type, const std::vector<byte> &body) {
  Guarded g(body.size() + 1);
  auto p = g.place(body.data(), body.size());

  EXPECT_EQ(recv_parse_or_apply_log_rec_body(sys, type, p, g.end(), nullptr, nullptr), g.end()) << "type " << int(type);
  EXPECT_FALSE(sys.m_recv->m_found_corrupt_log) << "type " << int(type);

  for (auto n : prefixes(body.size())) {
    p = g.place(body.data(), n);

    EXPECT_EQ(recv_parse_or_apply_log_rec_body(sys, type, p, g.end(), nullptr, nullptr), nullptr)
      << "type " << int(type) << " len " << n;
    EXPECT_FALSE(sys.m_recv->m_found_corrupt_log) << "type " << int(type) << " len " << n;
  }
}

/** The body of type is corrupt: nullptr and the corrupt flag, which is cleared again. */
void expect_corrupt(System &sys, MlogType type, std::vector<byte> body) {
  body.resize(body.size() + 16, 0);

  auto end = recv_parse_or_apply_log_rec_body(sys, type, body.data(), body.data() + body.size(), nullptr, nullptr);

  EXPECT_EQ(end, nullptr) << "type " << int(type);
  EXPECT_TRUE(sys.m_recv->m_found_corrupt_log) << "type " << int(type);

  sys.m_recv->m_found_corrupt_log = false;
}

/** @return offset (2 bytes) followed by the varint. */
std::vector<byte> nbytes_body(ulint offset, ulint v) {
  std::vector<byte> b(2 + 5);

  mach_write_to_2(b.data(), offset);
  b.resize(2 + varint::encode(b.data() + 2, v));

  return b;
}

/** @return offset (2 bytes) followed by the 64-bit varint. */
std::vector<byte> nbytes8_body(ulint offset, uint64_t v) {
  std::vector<byte> b(2 + 9);

  mach_write_to_2(b.data(), offset);
  b.resize(2 + varint::encode64(b.data() + 2, v));

  return b;
}

/** MLOG_nBYTES of every width and size class, complete and truncated. */
TEST_F(RecvParseTest, NBytesTruncated) {
  for (ulint v : {0UL, 0x7FUL, 0x80UL, 0xFFUL}) {
    expect_body(*m_sys, MLOG_1BYTE, nbytes_body(100, v));
  }

  for (ulint v : {0UL, 0x3FFFUL, 0x4000UL, 0xFFFFUL}) {
    expect_body(*m_sys, MLOG_2BYTES, nbytes_body(UNIV_PAGE_SIZE - 2, v));
  }

  for (ulint v : {0UL, 0x1FFFFFUL, 0x200000UL, 0xFFFFFFFUL, 0x10000000UL, 0xFF000000UL, 0xFFFFFFFFUL}) {
    expect_body(*m_sys, MLOG_4BYTES, nbytes_body(38, v));
  }

  for (uint64_t v : {0ULL, 0xFFFFFFFFULL, 0x100000000ULL, ~0ULL}) {
    expect_body(*m_sys, MLOG_8BYTES, nbytes8_body(38, v));
  }
}

/** Values too wide for the record type, and offsets off the page, are
corruption, not records. */
TEST_F(RecvParseTest, NBytesCorrupt) {
  expect_corrupt(*m_sys, MLOG_1BYTE, nbytes_body(100, 0x100));
  expect_corrupt(*m_sys, MLOG_2BYTES, nbytes_body(100, 0x10000));
  expect_corrupt(*m_sys, MLOG_4BYTES, nbytes_body(UNIV_PAGE_SIZE, 1));
  expect_corrupt(*m_sys, MLOG_8BYTES, nbytes8_body(UNIV_PAGE_SIZE + 5, 1));
}

/** @return a MLOG_WRITE_STRING body: offset, length, the bytes. */
std::vector<byte> string_body(ulint offset, ulint len) {
  std::vector<byte> b(4 + len);

  mach_write_to_2(b.data(), offset);
  mach_write_to_2(b.data() + 2, len);

  for (ulint i = 0; i < len; ++i) {
    b[4 + i] = byte(i);
  }

  return b;
}

/** MLOG_WRITE_STRING up to the end of the page, complete and truncated. */
TEST_F(RecvParseTest, StringTruncated) {
  expect_body(*m_sys, MLOG_WRITE_STRING, string_body(100, 0));
  expect_body(*m_sys, MLOG_WRITE_STRING, string_body(100, 1));
  expect_body(*m_sys, MLOG_WRITE_STRING, string_body(38, 1000));
  expect_body(*m_sys, MLOG_WRITE_STRING, string_body(0, UNIV_PAGE_SIZE));
}

/** A string that would go past the end of the page is corrupt. */
TEST_F(RecvParseTest, StringCorrupt) {
  auto b = string_body(UNIV_PAGE_SIZE - 10, 11);

  expect_corrupt(*m_sys, MLOG_WRITE_STRING, b);
  expect_corrupt(*m_sys, MLOG_WRITE_STRING, string_body(UNIV_PAGE_SIZE, 0));
}

/** The record types without a body, or with a fixed one. */
TEST_F(RecvParseTest, FixedBodies) {
  /* Nothing after the header. */
  for (auto type : {MLOG_PAGE_CREATE, MLOG_PAGE_REORGANIZE, MLOG_INIT_FILE_PAGE, MLOG_UNDO_ERASE_END, MLOG_UNDO_HDR_DISCARD}) {
    expect_body(*m_sys, type, {});
  }

  /* A record offset. */
  for (auto type : {MLOG_REC_MIN_MARK, MLOG_REC_DELETE, MLOG_LIST_END_DELETE, MLOG_LIST_START_DELETE}) {
    expect_body(*m_sys, type, {0x00, 0x63});
  }

  /* Delete mark value, record offset, and the transaction. */
  expect_body(*m_sys, MLOG_REC_SEC_DELETE_MARK, {1, 0x01, 0x23, 0, 0, 0, 0, 0, 0, 0, 0});
}

/** The clustered index delete mark of trx ids of every size, truncated. */
TEST_F(RecvParseTest, ClustDelMarkTruncated) {
  for (trx_id_t trx_id : {trx_id_t{0}, trx_id_t{0x7F} << 32, trx_id_t{1} << 40, ~trx_id_t{0}}) {
    expect_body(*m_sys, MLOG_REC_CLUST_DELETE_MARK, clust_del_mark_body(trx_id));
  }
}

/** The undo log records: the page init (varint type), the header create
and reuse (varint trx id) and an undo record (length and bytes). */
TEST_F(RecvParseTest, UndoTruncated) {
  for (ulint type : {1UL, 2UL, 0x80UL}) {
    std::vector<byte> b(5);

    b.resize(varint::encode(b.data(), type));
    expect_body(*m_sys, MLOG_UNDO_INIT, b);
  }

  for (trx_id_t id : {trx_id_t{5}, trx_id_t{1} << 33, ~trx_id_t{0}}) {
    std::vector<byte> b(9);

    b.resize(varint::encode64(b.data(), id));
    expect_body(*m_sys, MLOG_UNDO_HDR_CREATE, b);
    expect_body(*m_sys, MLOG_UNDO_HDR_REUSE, b);
  }

  for (ulint len : {0UL, 1UL, 300UL}) {
    std::vector<byte> b(2 + len, 0x33);

    mach_write_to_2(b.data(), len);
    expect_body(*m_sys, MLOG_UNDO_INSERT, b);
  }
}

/** The file operations: flags, a NUL terminated name, and for a rename the
new name. */
TEST_F(RecvParseTest, FileOpsTruncated) {
  auto body = [](std::vector<std::string> names) {
    std::vector<byte> b(4, 0);

    for (const auto &name : names) {
      byte len[2];

      mach_write_to_2(len, name.size() + 1);
      b.insert(b.end(), len, len + 2);
      b.insert(b.end(), name.begin(), name.end());
      b.push_back(0);
    }

    return b;
  };

  expect_body(*m_sys, MLOG_FILE_CREATE, body({"test/t1.ibd"}));
  expect_body(*m_sys, MLOG_FILE_DELETE, body({"test/t1.ibd"}));
  expect_body(*m_sys, MLOG_FILE_RENAME, body({"test/t1.ibd", "test/t2.ibd"}));
}

/** The types that are not redo records are corrupt log: they would stop
the parsing of the log at once. */
TEST_F(RecvParseTest, UnknownType) {
  /* 36 is MLOG_FILE_EXTEND. */
  for (int type : {0, 3, 5, 6, 7, 12, 27, 37, 40, 47}) {
    expect_corrupt(*m_sys, MlogType(type), {});
  }
}

/** MLOG_FILE_EXTEND: a 4 byte body (the new size), truncated nullptr and
not corrupt. Parsing does not extend anything: recovery replays it when it
stores the parsed records. */
TEST_F(RecvParseTest, FileExtend) {
  std::vector<byte> b(4);

  mach_write_to_4(b.data(), 1234);

  EXPECT_EQ(recv_parse_or_apply_log_rec_body(*m_sys, MLOG_FILE_EXTEND, b.data(), b.data() + 4, nullptr, nullptr), b.data() + 4);

  for (size_t n = 0; n < 4; ++n) {
    EXPECT_EQ(recv_parse_or_apply_log_rec_body(*m_sys, MLOG_FILE_EXTEND, b.data(), b.data() + n, nullptr, nullptr), nullptr) << n;
    EXPECT_FALSE(m_sys->m_recv->m_found_corrupt_log) << n;
  }
}

/** The header of a record: type (the single record flag stripped), the
varint space id and page number; truncated anywhere, nullptr. */
TEST_F(RecvParseTest, InitialRecord) {
  for (space_id_t space : {0U, 0x80U, 0x12345678U, 0xFFFFFFF0U}) {
    for (page_no_t page_no : {0U, 0x3FFFU, 0x4000U, 0xFFFFFFFEU}) {
      std::vector<byte> b(11);

      b[0] = MLOG_4BYTES | MLOG_SINGLE_REC_FLAG;

      auto n = 1 + varint::encode(b.data() + 1, space);

      n += varint::encode(b.data() + n, page_no);
      b.resize(n);

      Guarded g(b.size());
      MlogType type{};
      space_id_t s{};
      page_no_t p{};
      auto ptr = g.place(b.data(), b.size());

      EXPECT_EQ(mlog_parse_initial_log_record(ptr, g.end(), &type, &s, &p), g.end());
      EXPECT_EQ(type, MLOG_4BYTES);
      EXPECT_EQ(s, space);
      EXPECT_EQ(p, page_no);

      for (size_t len = 0; len < b.size(); ++len) {
        ptr = g.place(b.data(), len);
        EXPECT_EQ(mlog_parse_initial_log_record(ptr, g.end(), &type, &s, &p), nullptr) << space << " " << page_no << " " << len;
      }
    }
  }
}

/* Regression: the diagnostic of a corrupt log record reads before the parsing
buffer. recv_report_corrupt_log() (log/log0recv.cc:838-848) dumps from
  m_buf + m_previous_parsed_rec_offset - 100
but nothing sets m_previous_parsed_rec_offset (nor the previous type and
is-multi fields) any more: upstream recv_parse_log_recs() set them for every
record (7c21eac0 log0recv.c:2021, 2083). The offset is always 0, the dump
always starts 100 bytes before the 2 MB buffer: a heap buffer underflow
(and the message names record type 0 as the previous one). Here the buffer
is placed just after an inaccessible page, the read faults. */
TEST_F(RecvParseTest, CorruptReportInBounds) {
  auto &recv = *m_sys->m_recv;

  m_sys->m_config.m_recv.m_force_recovery = IB_RECOVERY_IGNORE_CORRUPT;
  recv.open(m_sys->m_buf_pool->get_curr_size());

  /* The parsing buffer right after a guard page. */
  const auto page = size_t(::sysconf(_SC_PAGESIZE));
  const auto prot = PROT_READ | PROT_WRITE;
  auto mem = static_cast<byte *>(::mmap(nullptr, page + RECV_PARSING_BUF_SIZE, prot, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

  ASSERT_NE(mem, MAP_FAILED);
  ASSERT_EQ(::mprotect(mem, page, PROT_NONE), 0);

  auto saved = recv.m_buf;

  recv.m_buf = mem + page;

  /* A record of an unknown type. */
  const std::vector<byte> log{byte(40 | MLOG_SINGLE_REC_FLAG), 0, 3, 0, 0, 0, 0};

  memcpy(recv.m_buf, log.data(), log.size());
  recv.m_len = log.size();
  recv.m_parse_start_lsn = 1000;
  recv.m_recovered_lsn = 1000;
  recv.m_scanned_lsn = 1000 + log.size();
  recv.m_recovered_offset = 0;

  m_sys->m_log->acquire();
  (void)recv_parse_log_recs(*m_sys, false);
  m_sys->m_log->release();

  EXPECT_TRUE(recv.m_found_corrupt_log);

  recv.m_buf = saved;
  recv.close();
  ::munmap(mem, page + RECV_PARSING_BUF_SIZE);
}

/** recv_parse_log_recs() on a parsing buffer filled by hand, as the scan of
the WAL fills it: the record groups are parsed (not stored) and consumed. */
class RecvGroupTest : public ut::SysTest {
 protected:
  void SetUp() override {
    ut::SysTest::SetUp();

    m_sys->m_recv->open(m_sys->m_buf_pool->get_curr_size());
    reset();
  }

  void TearDown() override {
    m_sys->m_recv->close();
    ut::SysTest::TearDown();
  }

  /** An empty parsing buffer at LSN START. */
  void reset() {
    auto &recv = *m_sys->m_recv;

    recv.m_len = 0;
    recv.m_recovered_offset = 0;
    recv.m_parse_start_lsn = START;
    recv.m_recovered_lsn = START;
    recv.m_scanned_lsn = START;
    recv.m_found_corrupt_log = false;
  }

  /** Add log to the buffer as scanned. */
  void add(const std::vector<byte> &log) {
    auto &recv = *m_sys->m_recv;

    memcpy(recv.m_buf + recv.m_len, log.data(), log.size());
    recv.m_len += log.size();
    recv.m_scanned_lsn += log.size();
  }

  /** Parse the buffer. */
  void parse() {
    m_sys->m_log->acquire();
    (void)recv_parse_log_recs(*m_sys, false);
    m_sys->m_log->release();

    EXPECT_FALSE(m_sys->m_recv->m_found_corrupt_log);
  }

  /** @return the parsed position in the buffer. */
  ulint parsed() const { return m_sys->m_recv->m_recovered_offset; }

  /** @return the LSN parsed up to. */
  lsn_t parsed_lsn() const { return m_sys->m_recv->m_recovered_lsn; }

  /** @return a MLOG_4BYTES record for (space, page_no), flagged single or not. */
  static std::vector<byte> rec4(space_id_t space, page_no_t page_no, ulint v, bool single) {
    std::vector<byte> b(32);

    b[0] = byte(MLOG_4BYTES | (single ? MLOG_SINGLE_REC_FLAG : 0));

    auto n = 1 + varint::encode(b.data() + 1, space);

    n += varint::encode(b.data() + n, page_no);
    mach_write_to_2(b.data() + n, 38);
    n += 2;
    n += varint::encode(b.data() + n, v);
    b.resize(n);

    return b;
  }

  /** @return a group of n records ending with MLOG_MULTI_REC_END, or one
  single record when n == 1. */
  static std::vector<byte> group(int n, ulint seed) {
    if (n == 1) {
      return rec4(0, page_no_t(seed % 1000), seed, true);
    }

    std::vector<byte> g;

    for (int i = 0; i < n; ++i) {
      const auto r = rec4(0, page_no_t((seed + i) % 1000), seed * 1000 + i * 77777, false);

      g.insert(g.end(), r.begin(), r.end());
    }

    g.push_back(MLOG_MULTI_REC_END);

    return g;
  }

  static constexpr lsn_t START = 1000000;
};

/** A single record group is consumed whole. */
TEST_F(RecvGroupTest, Single) {
  const auto g = group(1, 5);

  add(g);
  parse();

  EXPECT_EQ(parsed(), g.size());
  EXPECT_EQ(parsed_lsn(), START + g.size());
}

/** A multi-record group is consumed with its end mark. */
TEST_F(RecvGroupTest, Multi) {
  const auto g = group(5, 6);

  add(g);
  parse();

  EXPECT_EQ(parsed(), g.size());
  EXPECT_EQ(parsed_lsn(), START + g.size());
}

/** A multi-record group without its end mark is not parsed at all: its
records are applied together or not at all. */
TEST_F(RecvGroupTest, MultiWithoutEnd) {
  auto g = group(4, 7);

  g.pop_back();
  add(g);
  parse();

  EXPECT_EQ(parsed(), 0UL);
  EXPECT_EQ(parsed_lsn(), START);

  add({MLOG_MULTI_REC_END});
  parse();

  EXPECT_EQ(parsed(), g.size() + 1);
}

/** A single record cut anywhere is not parsed; when the rest arrives, it is. */
TEST_F(RecvGroupTest, SingleCut) {
  const auto g = group(1, 8);

  for (size_t cut = 0; cut < g.size(); ++cut) {
    reset();
    add(std::vector<byte>(g.begin(), g.begin() + ptrdiff_t(cut)));
    parse();

    EXPECT_EQ(parsed(), 0UL) << cut;

    add(std::vector<byte>(g.begin() + ptrdiff_t(cut), g.end()));
    parse();

    EXPECT_EQ(parsed(), g.size()) << cut;
  }
}

/** A dummy record is one byte of padding. */
TEST_F(RecvGroupTest, Dummy) {
  const auto g = group(1, 9);

  add({MLOG_DUMMY_RECORD});
  add(g);
  add({MLOG_DUMMY_RECORD});
  parse();

  EXPECT_EQ(parsed(), g.size() + 2);
  EXPECT_EQ(parsed_lsn(), START + g.size() + 2);
}

/** Many groups fed a byte at a time, as the WAL may end a read anywhere:
the parsing always stops at a group boundary and ends at the end. */
TEST_F(RecvGroupTest, ByteByByte) {
  std::vector<byte> log;
  std::vector<size_t> bounds{0};

  for (int i = 0; i < 200; ++i) {
    const auto g = group(1 + i % 6, ulint(i));

    log.insert(log.end(), g.begin(), g.end());
    bounds.push_back(log.size());
  }

  for (size_t i = 0; i < log.size(); ++i) {
    add({log[i]});
    parse();

    /* The last group boundary at or before i + 1. */
    const auto b = *std::prev(std::upper_bound(bounds.begin(), bounds.end(), i + 1));

    ASSERT_EQ(parsed(), b) << i;
    ASSERT_EQ(parsed_lsn(), START + b) << i;
  }

  EXPECT_EQ(parsed(), log.size());
}

/** Log in the buffer past the scanned LSN is not parsed: a group must be
completely scanned. */
TEST_F(RecvGroupTest, ScannedLimit) {
  const auto g = group(3, 10);

  add(g);
  m_sys->m_recv->m_scanned_lsn -= 1;
  parse();

  EXPECT_EQ(parsed(), 0UL);

  m_sys->m_recv->m_scanned_lsn += 1;
  parse();

  EXPECT_EQ(parsed(), g.size());
}


/** A record of the log of a running instance, as the walk found it. */
struct LogRec {
  MlogType m_type{};
  size_t m_off{};
  size_t m_len{};
};

/** Parse a whole record (header and body) at ptr. @return its end or nullptr */
byte *parse_rec(System &sys, byte *ptr, byte *end) {
  MlogType type{};
  space_id_t space{};
  page_no_t page_no{};

  auto body = mlog_parse_initial_log_record(ptr, end, &type, &space, &page_no);

  return body == nullptr ? nullptr : recv_parse_or_apply_log_rec_body(sys, type, body, end, nullptr, nullptr);
}

/** Walk log record by record as recovery parses it, checking the framing:
a record with the single flag is a group of its own, other records go on
until MLOG_MULTI_REC_END. @return the records */
std::vector<LogRec> walk(System &sys, std::vector<byte> &log, size_t &n_groups) {
  std::vector<LogRec> recs;
  bool in_group{};
  size_t pos{};
  const auto end = log.data() + log.size();

  while (pos < log.size()) {
    auto ptr = log.data() + pos;
    LogRec rec{MLOG_UNKNOWN, pos, 1};

    if (*ptr == MLOG_MULTI_REC_END) {
      EXPECT_TRUE(in_group) << "end mark outside a group at " << pos;
      in_group = false;
      ++n_groups;
      rec.m_type = MLOG_MULTI_REC_END;
    } else if (*ptr == MLOG_DUMMY_RECORD) {
      rec.m_type = MLOG_DUMMY_RECORD;
    } else {
      const bool single = (*ptr & MLOG_SINGLE_REC_FLAG) != 0;

      if (single) {
        EXPECT_FALSE(in_group) << "single record inside a group at " << pos;
        ++n_groups;
      } else {
        in_group = true;
      }

      rec.m_type = MlogType(*ptr & ~MLOG_SINGLE_REC_FLAG);

      auto next = parse_rec(sys, ptr, end);

      if (next == nullptr || sys.m_recv->m_found_corrupt_log) {
        ADD_FAILURE() << "record of type " << int(rec.m_type) << " at " << pos << " does not parse";
        break;
      }

      rec.m_len = size_t(next - ptr);
    }

    recs.push_back(rec);
    pos += rec.m_len;
  }

  EXPECT_EQ(pos, log.size());
  EXPECT_FALSE(in_group) << "the log ends inside a group";

  return recs;
}

/** The parsing buffer of a running instance, filled by hand: records are
parsed and stored per page, but never applied. */
class RecvStoreTest : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();

    auto &recv = *m_sys->m_recv;

    /* The i/o handler must not apply these records to pages it reads. */
    m_apply = recv.m_apply_log_recs;
    recv.m_apply_log_recs = false;

    recv.open(m_sys->m_buf_pool->get_curr_size());
    recv.m_len = 0;
    recv.m_recovered_offset = 0;
    recv.m_parse_start_lsn = START;
    recv.m_recovered_lsn = START;
    recv.m_scanned_lsn = START;
  }

  void TearDown() override {
    auto &recv = *m_sys->m_recv;

    recv.m_n_log_records = 0;
    recv.close();
    recv.m_apply_log_recs = m_apply;

    ut::DbTest::TearDown();
  }

  /** Add log to the buffer and parse it into the per page lists. */
  void parse(const std::vector<byte> &log) {
    auto &recv = *m_sys->m_recv;

    memcpy(recv.m_buf + recv.m_len, log.data(), log.size());
    recv.m_len += log.size();
    recv.m_scanned_lsn += log.size();

    m_sys->m_log->acquire();
    (void)recv_parse_log_recs(*m_sys, true);
    m_sys->m_log->release();

    EXPECT_FALSE(recv.m_found_corrupt_log);
  }

  /** @return the records stored for (space, page_no). */
  std::vector<LogRecord *> stored(space_id_t space, page_no_t page_no) {
    std::vector<LogRecord *> v;
    auto &recs = m_sys->m_recv->m_log_records;

    if (recs.contains(space) && recs[space].contains(page_no)) {
      for (auto r = recs[space][page_no]->m_rec_head; r != nullptr; r = r->m_next) {
        v.push_back(r);
      }
    }

    return v;
  }

  /** @return the body of rec, from its chunks. */
  static std::vector<byte> body_of(const LogRecord *rec, size_t *n_chunks) {
    std::vector<byte> v;

    *n_chunks = 0;

    for (auto d = rec->m_data; d != nullptr && v.size() < rec->m_len; d = d->m_next) {
      const auto n = std::min<size_t>(rec->m_len - v.size(), MEM_MAX_ALLOC_IN_BUF - sizeof(LogRecordData));
      auto p = reinterpret_cast<const byte *>(d + 1);

      v.insert(v.end(), p, p + n);
      ++*n_chunks;
    }

    return v;
  }

  static constexpr lsn_t START = 5000000;

  bool m_apply{};
};

/** A record bigger than a block of the record heap is stored in chunks
that put together give back the body, with the LSNs of its group. */
TEST_F(RecvStoreTest, BigRecordChunks) {
  std::vector<byte> rec{MLOG_WRITE_STRING | MLOG_SINGLE_REC_FLAG, 0, 5};
  std::vector<byte> body(4 + UNIV_PAGE_SIZE - 4);

  mach_write_to_2(body.data(), 4);
  mach_write_to_2(body.data() + 2, UNIV_PAGE_SIZE - 4);

  for (size_t i = 4; i < body.size(); ++i) {
    body[i] = byte(i * 13);
  }

  rec.insert(rec.end(), body.begin(), body.end());
  parse(rec);

  const auto recs = stored(0, 5);

  ASSERT_EQ(recs.size(), 1UL);
  EXPECT_EQ(recs[0]->m_type, MLOG_WRITE_STRING);
  EXPECT_EQ(recs[0]->m_len, body.size());
  EXPECT_EQ(recs[0]->m_start_lsn, START);
  EXPECT_EQ(recs[0]->m_end_lsn, START + rec.size());

  size_t chunks{};

  EXPECT_EQ(body_of(recs[0], &chunks), body);
  EXPECT_EQ(chunks, 2UL);
  EXPECT_EQ(m_sys->m_recv->m_n_log_records, 1UL);
}

/** The records of a group go to the lists of their pages in log order;
each starts at its own LSN, all end at the end of the group. */
TEST_F(RecvStoreTest, GroupPerPage) {
  auto rec4 = [](page_no_t page_no, ulint v) {
    std::vector<byte> b{MLOG_4BYTES, 0, byte(page_no), 0, 38};
    std::vector<byte> c(5);

    c.resize(varint::encode(c.data(), v));
    b.insert(b.end(), c.begin(), c.end());

    return b;
  };

  std::vector<byte> g;
  std::vector<lsn_t> starts;

  for (auto [page_no, v] : {std::pair{7U, 1UL}, {8U, 2UL}, {7U, 3UL}}) {
    starts.push_back(START + g.size());

    const auto r = rec4(page_no, v);

    g.insert(g.end(), r.begin(), r.end());
  }

  g.push_back(MLOG_MULTI_REC_END);
  parse(g);

  const auto p7 = stored(0, 7);
  const auto p8 = stored(0, 8);

  ASSERT_EQ(p7.size(), 2UL);
  ASSERT_EQ(p8.size(), 1UL);

  EXPECT_EQ(p7[0]->m_start_lsn, starts[0]);
  EXPECT_EQ(p8[0]->m_start_lsn, starts[1]);
  EXPECT_EQ(p7[1]->m_start_lsn, starts[2]);

  for (auto r : {p7[0], p7[1], p8[0]}) {
    EXPECT_EQ(r->m_end_lsn, START + g.size());
    EXPECT_EQ(r->m_type, MLOG_4BYTES);
  }

  size_t chunks{};

  EXPECT_EQ(varint::decode(body_of(p7[1], &chunks).data() + 2), 3UL);
  EXPECT_EQ(m_sys->m_recv->m_n_log_records, 2UL);
}

/** Records of a tablespace that does not exist are not stored. */
TEST_F(RecvStoreTest, UnknownSpace) {
  parse({MLOG_4BYTES | MLOG_SINGLE_REC_FLAG, 0x7F, 3, 0, 38, 1});

  EXPECT_TRUE(stored(0x7F, 3).empty());
  EXPECT_EQ(m_sys->m_recv->m_n_log_records, 0UL);
  EXPECT_EQ(m_sys->m_recv->m_recovered_offset, 6UL);
}

using RecvLogTest = ut::DbTest;

/** The log a running instance writes, read back from its checkpoint: every
record parses to where the next one begins, the groups are framed, and every
record cut anywhere asks for more data without reading past the cut or
calling the log corrupt. A writer and its parser that disagree on a record
format fail here (the clustered delete mark one did). */
TEST_F(RecvLogTest, RealLog) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 400; ++k) {
    rows[k] = ut::Row{k, k % 23, ut::blob(k, k % 7 == 0 ? 9000 : 60 + k % 300)};
  }

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 400; k += 3) {
    /* In place (same size), then a size change, then the key of SEC_A. */
    auto row = rows[k];

    if (k % 9 == 0) {
      row.m_b[0] = 'Z';
    } else if (k % 9 == 3) {
      row.m_b = ut::blob(k + 1, 20000);
    } else {
      row.m_a += 100;
    }

    ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    rows[k] = row;
  }

  for (uint32_t k = 1; k < 400; k += 10) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
    rows.erase(k);
  }

  commit(trx);

  /* A rolled back transaction: the undo is applied. */
  trx = begin();

  for (uint32_t k = 2; k < 400; k += 10) {
    ASSERT_EQ(remove(trx, k), DB_SUCCESS);
  }

  for (uint32_t k = 1000; k < 1100; ++k) {
    ASSERT_EQ(insert(trx, ut::Row{k, 1, "rolled back"}), DB_SUCCESS);
  }

  rollback(trx);

  expect_rows(rows);

  /* Stop the log at a group boundary and read it all. */
  auto &log = *m_sys->m_log;
  const auto config = log.wal_config();
  std::vector<byte> bytes;
  lsn_t ckpt{};
  uint64_t no{};

  log.acquire();

  ASSERT_TRUE(log.m_wal_backend->flush(IB_UINT64_T_MAX, true));
  ASSERT_TRUE(LogWalBackend::read_checkpoint(config, no, ckpt));

  const auto end = LogWalBackend::read(config, ckpt, [&](const byte *data, ulint len) {
    bytes.insert(bytes.end(), data, data + len);
    return true;
  });

  const auto lsn = log.get_lsn();

  log.release();

  ASSERT_EQ(end, lsn);
  ASSERT_GT(bytes.size(), 100000UL);

  size_t n_groups{};
  const auto recs = walk(*m_sys, bytes, n_groups);

  std::map<int, size_t> types;

  for (const auto &rec : recs) {
    ++types[rec.m_type];
  }

  std::string hist;

  for (auto [type, n] : types) {
    hist += std::format(" {}:{}", mlog_type_str(MlogType(type)), n);
  }

  std::printf("RealLog: %zu bytes, %zu groups, %zu records:%s\n", bytes.size(), n_groups, recs.size(), hist.c_str());

  for (auto type : {MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_REC_INSERT, MLOG_REC_CLUST_DELETE_MARK, MLOG_REC_SEC_DELETE_MARK,
                    MLOG_REC_UPDATE_IN_PLACE, MLOG_UNDO_INSERT, MLOG_UNDO_INIT, MLOG_PAGE_CREATE, MLOG_INIT_FILE_PAGE,
                    MLOG_WRITE_STRING, MLOG_MULTI_REC_END}) {
    EXPECT_GT(types[type], 0UL) << mlog_type_str(type);
  }

  /* Every record cut anywhere: at most 40 records of each type. */
  std::map<int, size_t> tried;
  Guarded g(UNIV_PAGE_SIZE * 8);

  for (const auto &rec : recs) {
    if (rec.m_type == MLOG_MULTI_REC_END || rec.m_type == MLOG_DUMMY_RECORD || tried[rec.m_type]++ >= 40) {
      continue;
    }

    ASSERT_LE(rec.m_len, UNIV_PAGE_SIZE * 8);

    for (auto n : prefixes(rec.m_len)) {
      auto p = g.place(bytes.data() + rec.m_off, n);

      ASSERT_EQ(parse_rec(*m_sys, p, g.end()), nullptr) << mlog_type_str(rec.m_type) << " cut at " << n << " of " << rec.m_len;
      ASSERT_FALSE(m_sys->m_recv->m_found_corrupt_log) << mlog_type_str(rec.m_type) << " cut at " << n << " of " << rec.m_len;
    }

    auto p = g.place(bytes.data() + rec.m_off, rec.m_len);

    ASSERT_EQ(parse_rec(*m_sys, p, g.end()), g.end()) << mlog_type_str(rec.m_type);
  }
}

/** Crash recovery: a killed instance comes back with exactly the committed
changes; the uncommitted ones are rolled back.

The workload runs twice: in the child that is killed, against the database,
and in the parent with m_apply false, as a model that computes the rows the
database must have after recovery. */
class RecvTest : public ut::DbTest {
 protected:
  /* READ COMMITTED: the uncommitted transactions don't block each other on gaps. */
  Trx *b() { return m_apply ? begin(IB_TRX_READ_COMMITTED) : nullptr; }

  void c(Trx *trx) {
    if (m_apply) {
      commit(trx);
    }
  }

  DbErr ins(Trx *trx, const ut::Row &row) { return m_apply ? insert(trx, row) : DB_SUCCESS; }

  DbErr upd(Trx *trx, uint32_t k, const ut::Row &row) { return m_apply ? update(trx, k, row) : DB_SUCCESS; }

  DbErr del(Trx *trx, uint32_t k) { return m_apply ? remove(trx, k) : DB_SUCCESS; }

  /** As crash(), with hook run between the kill of the child and the
  start of the recovery: it may damage or inspect the files. */
  void crash_hook(const std::function<void()> &fn, const std::function<void()> &hook) {
    stop();

    const auto pid = ::fork();

    ASSERT_NE(pid, -1);

    if (pid == 0) {
      m_sys = std::make_unique<System>();

      if (m_sys->init(m_config) != DB_SUCCESS || m_sys->startup() != DB_SUCCESS) {
        ::_exit(2);
      }

      fn();

      if (::testing::Test::HasFailure()) {
        ::_exit(3);
      }

      ::raise(SIGKILL);
      ::_exit(4);
    }

    int status{};
    const auto waited = ::waitpid(pid, &status, 0);

    hook();

    const auto t0 = std::chrono::steady_clock::now();

    start(false);

    m_recovery_secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    ASSERT_EQ(waited, pid);
    ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) << "the child failed before the kill, status " << status;
  }

  /** @return the path of the WAL file. */
  ut::fs::path wal_path() const { return m_dir.m_path / "redo.wal"; }

  /** @return the checkpoint LSN and the end of the log on disk. */
  std::pair<lsn_t, lsn_t> log_extent() const {
    LogWalBackend::Config config;

    config.m_log_dir = m_dir.m_path.string();
    config.m_n_blocks = std::max<size_t>(16, m_config.m_log.m_buffer_size / config.m_block_size);

    lsn_t ckpt{};
    uint64_t no{};

    EXPECT_TRUE(LogWalBackend::read_checkpoint(config, no, ckpt));

    const auto end = LogWalBackend::read(config, ckpt, [](const byte *, ulint) { return true; });

    return {ckpt, end};
  }

  /** Seconds the last crash_hook() recovery took. */
  double m_recovery_secs{};

  /** Run fn in a killed child, then run it here as the model. */
  void crash_model(const std::function<void()> &fn) {
    crash([&] {
      m_apply = true;
      fn();
    });

    m_apply = false;
    fn();
  }

  /** Committed changes. @return the rows they leave */
  ut::Rows committed(uint32_t n) {
    ut::Rows rows;

    /* Inserts in several transactions, some rows with big blobs. */
    for (uint32_t base = 0; base < n; base += 50) {
      auto trx = b();

      for (uint32_t k = base; k < std::min(n, base + 50); ++k) {
        ut::Row row{k, k % 17, ut::blob(k, k % 4 == 0 ? 20000 : 100)};

        EXPECT_EQ(ins(trx, row), DB_SUCCESS) << k;
        rows[k] = row;
      }

      c(trx);
    }

    auto trx = b();

    for (uint32_t k = 0; k < n; ++k) {
      auto row = rows[k];

      if (k % 3 == 0) {
        /* The secondary key changes. */
        row.m_a += 1000;
      } else if (k % 5 == 1) {
        /* The blob grows and goes off page. */
        row.m_b = ut::blob(k + 1, 30000);
      } else if (k % 5 == 2) {
        /* The blob shrinks back into the page. */
        row.m_b = ut::blob(k + 2, 10);
      } else {
        continue;
      }

      EXPECT_EQ(upd(trx, k, row), DB_SUCCESS) << k;
      rows[k] = row;
    }

    c(trx);

    trx = b();

    for (uint32_t k = 0; k < n; k += 7) {
      EXPECT_EQ(del(trx, k), DB_SUCCESS) << k;
      rows.erase(k);
    }

    /* The primary key moves: delete mark and insert. */
    for (uint32_t k = 2; k < n; k += 11) {
      if (rows.contains(k)) {
        auto row = rows[k];

        row.m_k = k + 100000;
        EXPECT_EQ(upd(trx, k, row), DB_SUCCESS) << k;
        rows.erase(k);
        rows[row.m_k] = row;
      }
    }

    /* A failed statement in a transaction that commits. */
    if (m_apply) {
      EXPECT_EQ(insert(trx, rows.begin()->second), DB_DUPLICATE_KEY);
    }

    c(trx);

    return rows;
  }

  /** Changes not committed at the kill: an update-only, an insert-only and
  a delete-only transaction, and one of everything. A commit that follows
  makes their redo durable. @return the rows after the commit */
  ut::Rows uncommitted(ut::Rows rows) {
    auto upd_trx = b();
    auto ins_trx = b();
    auto del_trx = b();
    auto mix_trx = b();

    int i{};

    for (const auto &[k, row] : rows) {
      auto r = row;

      switch (i++ % 4) {
        case 0:
          r.m_a += 7;
          r.m_b = ut::blob(k + 3, 25000);
          EXPECT_EQ(upd(upd_trx, k, r), DB_SUCCESS) << k;
          break;
        case 1:
          EXPECT_EQ(del(del_trx, k), DB_SUCCESS) << k;
          break;
        case 2:
          r.m_k = k + 200000;
          EXPECT_EQ(upd(mix_trx, k, r), DB_SUCCESS) << k;
          break;
        default:
          break;
      }
    }

    /* Disjoint from the keys the other transactions move rows to. */
    for (uint32_t k = 700000; k < 700100; ++k) {
      EXPECT_EQ(ins(ins_trx, ut::Row{k, k % 5, ut::blob(k, k % 10 == 0 ? 15000 : 50)}), DB_SUCCESS);
    }

    EXPECT_EQ(ins(mix_trx, ut::Row{400000, 1, "x"}), DB_SUCCESS);

    auto flush = b();
    const ut::Row row{500000, 0, "flush"};

    EXPECT_EQ(ins(flush, row), DB_SUCCESS);
    c(flush);

    rows[row.m_k] = row;

    return rows;
  }

  bool m_apply{true};
};

/** Committed changes only. */
TEST_F(RecvTest, Committed) {
  ut::Rows expected;

  crash_model([&] { expected = committed(400); });

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** Committed and uncommitted changes: the latter are rolled back. */
TEST_F(RecvTest, Uncommitted) {
  ut::Rows expected;

  crash_model([&] { expected = uncommitted(committed(400)); });

  /* The rollback of the recovered transactions runs in the background, a
  consistent read sees the committed rows meanwhile. */
  expect_rows(expected);

  /* After a normal shutdown all of it is undone. */
  restart();
  expect_rows(expected);
}

/** Killed again right after recovery, before the rollback could finish. */
TEST_F(RecvTest, Twice) {
  ut::Rows expected;

  crash_model([&] { expected = uncommitted(committed(200)); });

  crash([] {});

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** Many killed runs on the same database, each adding committed rows and
leaving an uncommitted transaction behind. */
TEST_F(RecvTest, Repeated) {
  ut::Rows expected;

  for (uint32_t round = 0; round < 4; ++round) {
    crash_model([&] {
      auto trx = b();

      for (uint32_t k = round * 100; k < round * 100 + 100; ++k) {
        ut::Row row{k, k % 13, ut::blob(k, k % 9 == 0 ? 12000 : 40)};

        EXPECT_EQ(ins(trx, row), DB_SUCCESS);
        expected[k] = row;
      }

      c(trx);

      auto open = b();

      EXPECT_EQ(ins(open, ut::Row{900000 + round, 1, "lost"}), DB_SUCCESS);
      EXPECT_EQ(del(open, expected.begin()->first), DB_SUCCESS);

      /* Flush the above with a commit. */
      auto flush = b();
      const ut::Row row{800000 + round, 0, "flush"};

      EXPECT_EQ(ins(flush, row), DB_SUCCESS);
      expected[row.m_k] = row;
      c(flush);
    });

    expect_rows(expected);
  }

  restart();
  expect_rows(expected);
}


/** Killed with a big transaction open: thousands of rows inserted and many
updated, its redo partly written when the log buffer filled. All of it is
rolled back. */
TEST_F(RecvTest, BigUncommitted) {
  ut::Rows expected;

  crash_model([&] {
    expected = committed(150);

    auto big = b();

    for (uint32_t k = 600000; k < 603000; ++k) {
      EXPECT_EQ(ins(big, ut::Row{k, k % 50, ut::blob(k, 200)}), DB_SUCCESS);
    }

    for (const auto &[k, row] : expected) {
      if (k % 2 == 0) {
        auto r = row;

        r.m_b = ut::blob(k + 9, 3000);
        EXPECT_EQ(upd(big, k, r), DB_SUCCESS);
      }
    }

    auto flush = b();
    const ut::Row row{500000, 0, "flush"};

    EXPECT_EQ(ins(flush, row), DB_SUCCESS);
    c(flush);
    expected[row.m_k] = row;
  });

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** Many small transactions, each a single change committed on its own. */
TEST_F(RecvTest, ManySmall) {
  ut::Rows expected;

  crash_model([&] {
    for (uint32_t k = 0; k < 300; ++k) {
      auto trx = b();
      const ut::Row row{k, k % 7, ut::blob(k, 50)};

      EXPECT_EQ(ins(trx, row), DB_SUCCESS);
      c(trx);
      expected[k] = row;

      if (k % 5 == 4) {
        trx = b();
        EXPECT_EQ(del(trx, k - 2), DB_SUCCESS);
        c(trx);
        expected.erase(k - 2);
      }

      if (k % 11 == 10) {
        trx = b();

        auto r = expected.at(k);

        r.m_a = 999;
        EXPECT_EQ(upd(trx, k, r), DB_SUCCESS);
        c(trx);
        expected[k] = r;
      }
    }
  });

  expect_rows(expected);
}

/** A committed row with a blob of megabytes, and an uncommitted update to
another blob of megabytes. */
TEST_F(RecvTest, BigBlob) {
  ut::Rows expected;

  crash_model([&] {
    auto trx = b();

    for (uint32_t k = 1; k <= 3; ++k) {
      const ut::Row row{k, k, ut::blob(k, 1000000 * k)};

      EXPECT_EQ(ins(trx, row), DB_SUCCESS);
      expected[k] = row;
    }

    c(trx);

    auto open = b();
    auto r = expected.at(2);

    r.m_b = ut::blob(77, 2500000);
    EXPECT_EQ(upd(open, 2, r), DB_SUCCESS);
    EXPECT_EQ(del(open, 3), DB_SUCCESS);

    auto flush = b();
    const ut::Row row{10, 0, "flush"};

    EXPECT_EQ(ins(flush, row), DB_SUCCESS);
    c(flush);
    expected[row.m_k] = row;
  });

  expect_rows(expected);
}

/** A checkpoint in the middle of the work: recovery starts there, the log
before it is a hole in the file. */
TEST_F(RecvTest, AfterCheckpoint) {
  ut::Rows expected;

  crash_model([&] {
    expected = committed(150);

    if (m_apply) {
      m_sys->m_log->make_checkpoint_at(IB_UINT64_T_MAX, true);
    }

    expected = uncommitted(expected);
  });

  const auto [ckpt, end] = log_extent();

  EXPECT_GT(end, ckpt);

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** Killed while a thread makes checkpoints back to back: every flush of the
pages and every write of the checkpoint file may be cut short. */
TEST_F(RecvTest, DuringCheckpoint) {
  ut::Rows expected;

  crash_model([&] {
    if (m_apply) {
      /* Runs until the kill; it must outlive this function. */
      std::thread([sys = m_sys.get()] {
        for (;;) {
          sys->m_log->make_checkpoint_at(IB_UINT64_T_MAX, true);
        }
      }).detach();
    }

    for (uint32_t base = 0; base < 600; base += 100) {
      auto trx = b();

      for (uint32_t k = base; k < base + 100; ++k) {
        const ut::Row row{k, k % 9, ut::blob(k, k % 10 == 0 ? 8000 : 300)};

        EXPECT_EQ(ins(trx, row), DB_SUCCESS);
        expected[k] = row;
      }

      c(trx);
    }

    expected = uncommitted(expected);
  });

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** The stamps of a secondary record are redo logged: after recovery the new records of
a committed key update have the id of the clustered row. */
TEST_F(RecvTest, SecIds) {
  ut::Rows rows;

  crash_model([&] {
    rows = committed(50);

    auto t = b();

    for (const auto &[k, row] : rows) {
      /* Values no row of committed() has: the key of each record changes. */
      EXPECT_EQ(upd(t, k, ut::Row{k, k + 200000, row.m_b}), DB_SUCCESS) << k;
    }

    c(t);
  });

  ASSERT_FALSE(rows.empty());

  for (const auto &[k, row] : rows) {
    const auto id = clust_id(k);
    const auto moved = sec_rec(k + 200000, k);
    const auto old = sec_rec(row.m_a, k);

    ASSERT_TRUE(moved.has_value()) << k;
    EXPECT_FALSE(moved->m_deleted) << k;

    /* The delete mark is its own redo record, with the id in it. Both records were
    last changed by the update. */
    ASSERT_TRUE(old.has_value()) << k;
    EXPECT_TRUE(old->m_deleted) << k;
    EXPECT_EQ(old->m_id, id) << k;
    EXPECT_EQ(moved->m_id, id) << k;
  }
}

/** Killed again and again while recovery rolls back a big transaction. */
TEST_F(RecvTest, KilledDuringRollback) {
  ut::Rows expected;

  crash_model([&] {
    expected = committed(100);

    auto big = b();

    for (uint32_t k = 600000; k < 604000; ++k) {
      EXPECT_EQ(ins(big, ut::Row{k, 3, ut::blob(k, 100)}), DB_SUCCESS);
    }

    auto flush = b();
    const ut::Row row{500000, 0, "flush"};

    EXPECT_EQ(ins(flush, row), DB_SUCCESS);
    c(flush);
    expected[row.m_k] = row;
  });

  for (int ms : {200, 50, 0}) {
    crash([ms] { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); });
  }

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** Blocks after the end of the log, stale (a valid block of another
place) or garbage, are not log: recovery ends where the log ends. */
TEST_F(RecvTest, StaleTail) {
  ut::Rows expected;

  crash_hook(
    [&] {
      expected = committed(100);
    },
    [&] {
      constexpr off_t BLOCK = 4096;
      constexpr lsn_t DATA = 4080;

      const auto [ckpt, end] = log_extent();

      std::ifstream in(wal_path(), std::ios::binary);
      std::vector<char> first(BLOCK);

      in.seekg(off_t(ckpt / DATA) * BLOCK);
      in.read(first.data(), BLOCK);
      in.close();

      std::fstream out(wal_path(), std::ios::in | std::ios::out | std::ios::binary);
      const auto after = off_t(end / DATA + 1) * BLOCK;

      /* The block the log starts in, again after its end. */
      out.seekp(after);
      out.write(first.data(), BLOCK);

      /* Garbage after that. */
      std::vector<char> junk(4 * BLOCK);

      for (size_t i = 0; i < junk.size(); ++i) {
        junk[i] = char(i * 7 + 3);
      }

      out.write(junk.data(), std::streamsize(junk.size()));
    }
  );

  /* The model: the same changes, not applied. */
  m_apply = false;
  expected = committed(100);

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** An extension is not synced when done, it is redo logged: recovery
extends the file when it parses MLOG_FILE_EXTEND. The child logs an
extension of the table's file by 200 pages without doing it, as if the
extension had not reached the disk before the crash; the file must be that
big after recovery, and usable. */
TEST_F(RecvTest, LostExtension) {
  ut::Rows expected;
  off_t before{};

  const auto ibd = m_dir.m_path / (std::string(ut::TABLE) + ".ibd");

  crash_hook(
    [&] {
      expected = committed(100);

      auto &sys = *m_sys;

      sys.m_dict->mutex_acquire();

      const auto space = sys.m_dict->table_get(ut::TABLE)->m_space_id;

      sys.m_dict->mutex_release();

      const auto size = page_no_t(sys.m_fil->space_get_size(space));

      ASSERT_GT(size, 0U);

      Mtr mtr;

      mtr.start();
      sys.m_fil->extend_write_log(space, size + 200, &mtr);
      mtr.commit(sys);

      sys.m_log->buffer_flush_to_disk();
    },
    [&] { before = off_t(ut::fs::file_size(ibd)); }
  );

  /* Recovery made the file 200 pages bigger. */
  EXPECT_GE(off_t(ut::fs::file_size(ibd)), before + off_t(200 * UNIV_PAGE_SIZE));

  m_apply = false;
  expected = committed(100);

  expect_rows(expected);

  /* The new part is usable: fill it (applied, not only the model). */
  m_apply = true;

  auto trx = b();

  for (uint32_t k = 1000; k < 1400; ++k) {
    ut::Row row{k, k % 17, ut::blob(k, 3000)};

    ASSERT_EQ(ins(trx, row), DB_SUCCESS) << k;
    expected[k] = row;
  }

  c(trx);

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

/** A crash right after extensions that were never synced: the rows in the
new pages are recovered (the file size may or may not have reached the
disk, recovery extends it from the log either way). */
TEST_F(RecvTest, UnsyncedExtensions) {
  ut::Rows expected;

  crash_model([&] {
    /* Big rows: the table's file is extended many times. */
    auto trx = b();

    for (uint32_t k = 0; k < 400; ++k) {
      ut::Row row{k, k % 13, ut::blob(k, 8000)};

      EXPECT_EQ(ins(trx, row), DB_SUCCESS) << k;
      expected[k] = row;
    }

    c(trx);
  });

  expect_rows(expected);
  restart();
  expect_rows(expected);
}

}  // namespace

namespace {

using RecvPerf = RecvGroupTest;

/** Parse of a MLOG_4BYTES body. */
TEST_F(RecvPerf, ParseBody) {
  const auto body = nbytes_body(100, 0x12345);
  std::vector<byte> b(body);

  auto r = ut::bench("parse_4bytes_body", 5000000, [&](uint64_t) {
    ut::keep(recv_parse_or_apply_log_rec_body(*m_sys, MLOG_4BYTES, b.data(), b.data() + b.size(), nullptr, nullptr));
  });

  EXPECT_LT(r.ns_per_op(), 5000.0);
}

/** recv_parse_log_recs() over 1 MB of groups of 1 to 6 records: the parse
rate of recovery in MB/s. */
TEST_F(RecvPerf, ParseGroups) {
  std::vector<byte> log;

  for (int i = 0; log.size() < 1024 * 1024; ++i) {
    const auto g = group(1 + i % 6, ulint(i));

    log.insert(log.end(), g.begin(), g.end());
  }

  auto r = ut::bench("parse_groups_1mb", 20, [&](uint64_t) {
    reset();
    add(log);
    parse();
    EXPECT_EQ(parsed(), log.size());
  });

  std::printf("PERF RecvPerf.ParseGroups MB/s=%.1f\n", double(log.size()) / r.ns_per_op() * 1e9 / 1e6);
  EXPECT_LT(r.ns_per_op(), 10e9);
}

class RecvCrashPerf : public RecvTest {};

/** Recovery of a killed instance: seconds per MB of redo from the checkpoint. */
TEST_F(RecvCrashPerf, RecoveryTime) {
  lsn_t redo{};

  crash_hook(
    [&] {
      ut::Rows rows;

      for (uint32_t k = 0; k < 3000; ++k) {
        rows[k] = ut::Row{k, k % 31, ut::blob(k, 2000)};
      }

      load(rows);
    },
    [&] {
      const auto [ckpt, end] = log_extent();

      redo = end - ckpt;
    }
  );

  const double mb = double(redo) / (1024 * 1024);

  std::printf(
    "PERF RecvCrashPerf.RecoveryTime redo_mb=%.1f secs=%.3f secs_per_mb=%.3f\n", mb, m_recovery_secs, m_recovery_secs / mb
  );

  ut::report(ut::BenchResult{"recovery_per_mb_redo", 1, uint64_t(redo), m_recovery_secs});

  EXPECT_GT(mb, 1.0);
  EXPECT_LT(m_recovery_secs / mb, 10.0);
}

}  // namespace
