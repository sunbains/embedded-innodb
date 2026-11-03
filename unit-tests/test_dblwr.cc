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

/** @file unit-tests/test_dblwr.cc
Dblwr: the doublewrite buffer, created at database creation, used for every
page write.

The doublewrite buffer is two blocks of FSP_EXTENT_SIZE pages in the system
tablespace. A flush batch first writes the pages to it, flushes it, then
writes them to their places; at start up, a page whose copy is in the buffer
and which is corrupt in its file (torn by a crash in the middle of a write)
is restored from the copy. The torn page tests simulate the torn write by
damaging a data file page after a clean shutdown. */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "buf0dblwr.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "srv0srv.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <set>
#include <vector>

namespace {

/** The pages of a file, read around the engine. */
std::vector<byte> read_pages(const ut::fs::path &path, page_no_t first, page_no_t n) {
  std::vector<byte> buf(size_t(n) * UNIV_PAGE_SIZE);
  const auto fd = ::open(path.c_str(), O_RDONLY);

  EXPECT_GE(fd, 0) << path;
  EXPECT_EQ(::pread(fd, buf.data(), buf.size(), off_t(first) * UNIV_PAGE_SIZE), ssize_t(buf.size()));
  ::close(fd);

  return buf;
}

/** Write a page of a file, around the engine. */
void write_page(const ut::fs::path &path, page_no_t page_no, const byte *page) {
  const auto fd = ::open(path.c_str(), O_WRONLY);

  ASSERT_GE(fd, 0) << path;
  ASSERT_EQ(::pwrite(fd, page, UNIV_PAGE_SIZE, off_t(page_no) * UNIV_PAGE_SIZE), ssize_t(UNIV_PAGE_SIZE));
  ASSERT_EQ(::fsync(fd), 0);
  ::close(fd);
}

space_id_t table_space(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table->m_space_id;
}

using DblwrTest = ut::DbTest;

/** It exists after creation and after a restart, in pages 64..191 of the
system tablespace. */
TEST_F(DblwrTest, Exists) {
  std::pair<page_no_t, page_no_t> blocks{};

  ASSERT_NE(m_sys->m_dblwr, nullptr);
  ASSERT_TRUE(m_sys->m_dblwr->check_if_exists(blocks));
  EXPECT_EQ(blocks.first, FSP_EXTENT_SIZE);
  EXPECT_EQ(blocks.second, 2 * FSP_EXTENT_SIZE);

  EXPECT_FALSE(m_sys->m_dblwr->is_page_inside(0));
  EXPECT_TRUE(m_sys->m_dblwr->is_page_inside(FSP_EXTENT_SIZE));
  EXPECT_TRUE(m_sys->m_dblwr->is_page_inside(3 * FSP_EXTENT_SIZE - 1));
  EXPECT_FALSE(m_sys->m_dblwr->is_page_inside(3 * FSP_EXTENT_SIZE));

  restart();

  ASSERT_TRUE(m_sys->m_dblwr->check_if_exists(blocks));
  EXPECT_EQ(blocks.first, FSP_EXTENT_SIZE);
}

/** Many pages written through it survive a restart and a crash. */
TEST_F(DblwrTest, Writes) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 1000; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 3000)};
  }

  load(rows);
  restart();
  expect_rows(rows);

  crash([this] {
    auto trx = begin();

    for (uint32_t k = 0; k < 1000; k += 2) {
      EXPECT_EQ(update(trx, k, ut::Row{k, k + 1, ut::blob(k + 1, 3000)}), DB_SUCCESS);
    }

    commit(trx);
  });

  for (uint32_t k = 0; k < 1000; k += 2) {
    rows[k] = ut::Row{k, k + 1, ut::blob(k + 1, 3000)};
  }

  expect_rows(rows);
}

/** A copy in the doublewrite buffer of a page of the test table that is
byte for byte the page in the data file. */
struct Copy {
  page_no_t m_page_no{};
  std::vector<byte> m_page;
};

/** Torn page tests: load the table, shut down, find pages of the table with
a current copy in the doublewrite buffer. */
class DblwrTornTest : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();

    for (uint32_t k = 0; k < 400; ++k) {
      m_rows[k] = ut::Row{k, k, ut::blob(k, 2000)};
    }

    load(m_rows);
    m_space = table_space(*m_sys);
    stop();

    m_copies = copies();
  }

  /** @return the copies in the doublewrite buffer of the pages of the table
  that are identical to the pages in the data file. */
  std::vector<Copy> copies() {
    const auto dblwr = read_pages(m_dir.m_path / "system.ibd", FSP_EXTENT_SIZE, 2 * FSP_EXTENT_SIZE);
    const auto n_pages = page_no_t(ut::fs::file_size(ibd()) / UNIV_PAGE_SIZE);
    const auto data = read_pages(ibd(), 0, n_pages);
    std::vector<Copy> found;
    std::set<page_no_t> seen;

    for (ulint i{}; i < 2 * FSP_EXTENT_SIZE; ++i) {
      const auto copy = dblwr.data() + i * UNIV_PAGE_SIZE;
      const auto space = mach_read_from_4(copy + FIL_PAGE_SPACE_ID);
      const auto page_no = page_no_t(mach_read_from_4(copy + FIL_PAGE_OFFSET));

      if (space != m_space || page_no == 0 || page_no >= n_pages || !seen.insert(page_no).second) {
        continue;
      }

      const auto page = data.data() + size_t(page_no) * UNIV_PAGE_SIZE;

      if (memcmp(copy, page, UNIV_PAGE_SIZE) == 0) {
        found.push_back(Copy{page_no, std::vector<byte>(copy, copy + UNIV_PAGE_SIZE)});
      } else {
        /* An older copy: a later write of the page was elsewhere. */
        found.erase(std::remove_if(found.begin(), found.end(), [&](const Copy &c) { return c.m_page_no == page_no; }), found.end());
      }
    }

    return found;
  }

  [[nodiscard]] ut::fs::path ibd() const { return m_dir.m_path / "test" / "t.ibd"; }

  /** @return the page in the data file now. */
  std::vector<byte> data_page(page_no_t page_no) { return read_pages(ibd(), page_no, 1); }

  ut::Rows m_rows;
  space_id_t m_space{};
  std::vector<Copy> m_copies;
};

/** The table's pages are in the doublewrite buffer after the shutdown
flush, and a clean restart changes no data page. */
TEST_F(DblwrTornTest, RestartKeepsPages) {
  ASSERT_FALSE(m_copies.empty());

  const auto before = read_pages(ibd(), 0, page_no_t(ut::fs::file_size(ibd()) / UNIV_PAGE_SIZE));

  start(false);

  const auto after = read_pages(ibd(), 0, page_no_t(before.size() / UNIV_PAGE_SIZE));

  EXPECT_TRUE(before == after) << "a data page changed";
  expect_rows(m_rows);
}

/** A page torn by a crash in the middle of its write (only the first 4K made
it to disk: the lsn in its trailer is not the one in its header) is restored
from its copy in the doublewrite buffer at start up. */
TEST_F(DblwrTornTest, TornPageRestored) {
  ASSERT_FALSE(m_copies.empty());

  for (const auto &copy : m_copies) {
    auto page = copy.m_page;

    std::fill(page.begin() + 4096, page.end(), byte(0));
    write_page(ibd(), copy.m_page_no, page.data());
  }

  start(false);

  for (const auto &copy : m_copies) {
    EXPECT_TRUE(data_page(copy.m_page_no) == copy.m_page) << "page " << copy.m_page_no << " restored";
  }

  expect_rows(m_rows);
}

/** Only the last 512 bytes torn. */
TEST_F(DblwrTornTest, TornTrailerRestored) {
  ASSERT_FALSE(m_copies.empty());

  const auto &copy = m_copies.front();
  auto page = copy.m_page;

  std::fill(page.end() - 512, page.end(), byte(0xff));
  write_page(ibd(), copy.m_page_no, page.data());

  start(false);

  EXPECT_TRUE(data_page(copy.m_page_no) == copy.m_page);
  expect_rows(m_rows);
}

/* Regression: a page damaged between its header and its trailer (a torn write of a
middle sector, a bit flip) is not restored from the doublewrite buffer: the
page checksum is the constant crc32::NO_CHECKSUM in this build, so
BufPool::is_corrupted() sees only a torn lsn, and Dblwr::recover_pages()
leaves the page as it is; it is then read as if it were good. Same root cause
as BufPoolTest.Checksum (include/ut0crc32.h). */
TEST_F(DblwrTornTest, TornMiddleRestored) {
  ASSERT_FALSE(m_copies.empty());

  const auto &copy = m_copies.front();
  auto page = copy.m_page;

  /* A middle sector is damaged: the lsn fields are intact. */
  for (size_t i = 8192; i < 8192 + 512; ++i) {
    page[i] ^= byte(0xff);
  }

  ASSERT_TRUE(page != copy.m_page);
  write_page(ibd(), copy.m_page_no, page.data());

  start(false);

  ASSERT_TRUE(data_page(copy.m_page_no) == copy.m_page) << "page " << copy.m_page_no << " restored";
  expect_rows(m_rows);
}

/** A page of a tablespace dropped since its copy was made: the copy is
ignored (and doesn't stop the start up). */
TEST_F(DblwrTornTest, DroppedSpaceIgnored) {
  start(false);

  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
  commit(trx);

  restart();

  EXPECT_FALSE(m_sys->m_fil->tablespace_exists_in_mem(m_space));
}

/** A database with the doublewrite buffer disabled works too. */
class NoDblwrTest : public ut::DbTest {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_dblwr.m_enabled = false;
    start();
  }
};

TEST_F(NoDblwrTest, Works) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 300; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 2000)};
  }

  const auto pages = m_sys->m_srv->m_stats.m_dblwr_pages_written;

  load(rows);
  restart();
  expect_rows(rows);

  EXPECT_EQ(pages, 0UL);
  EXPECT_EQ(m_sys->m_srv->m_stats.m_dblwr_pages_written, 0UL) << "nothing goes through the doublewrite buffer";
}

/** A System started once for all the tests of a suite (starting and
stopping one takes seconds), with scratch tablespaces. */
class DblwrSysTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    ut::fs::create_directories(s_dir->m_path / "test");

    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(ut::make_config(s_dir->m_path)), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
  }

  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      s_sys.reset();
    }

    s_dir.reset();
  }

 protected:
  void SetUp() override {
    ASSERT_NE(s_sys, nullptr);
    m_sys = s_sys.get();
  }

  /** Create a tablespace of n_pages pages, named after the test. */
  space_id_t new_space(page_no_t n_pages) {
    space_id_t id{SYS_TABLESPACE};
    const auto name = std::string("test/") + ::testing::UnitTest::GetInstance()->current_test_info()->name();

    EXPECT_EQ(m_sys->m_fil->create_new_single_table_tablespace(&id, name.c_str(), false, 0, FIL_IBD_FILE_INITIAL_SIZE), DB_SUCCESS);

    page_no_t actual{};

    EXPECT_TRUE(m_sys->m_fil->extend_space_to_desired_size(&actual, id, n_pages));

    m_path = s_dir->m_path / (name + ".ibd");

    return id;
  }

  /** Create page_id in the pool with value at FIL_PAGE_DATA, dirty. */
  void put(const PageId &page_id, uint32_t value) {
    Mtr mtr;

    mtr.start();

    auto block = m_sys->m_buf_pool->create(page_id, &mtr);

    BufPool::Request req{
      .m_rw_latch = RW_X_LATCH, .m_page_id = page_id, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
    };

    ASSERT_EQ(m_sys->m_buf_pool->get(req, nullptr), block);

    auto frame = block->get_frame();

    mach_write_to_4(frame + FIL_PAGE_OFFSET, page_id.page_no());
    mach_write_to_4(frame + FIL_PAGE_SPACE_ID, page_id.space_id());
    mlog_write_ulint(frame + FIL_PAGE_DATA, value, MLOG_4BYTES, &mtr);

    mtr.commit(*m_sys);
  }

  /** Flush all the dirty pages of the pool. */
  void flush_all() {
    auto &flusher = *m_sys->m_buf_pool->m_flusher;

    while (flusher.batch(BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX) == ULINT_UNDEFINED) {
      flusher.wait_batch_end(BUF_FLUSH_LIST);
    }

    flusher.wait_batch_end(BUF_FLUSH_LIST);
  }

  /** @return the doublewrite buffer area of system.ibd. */
  std::vector<byte> dblwr_area() { return read_pages(s_dir->m_path / "system.ibd", FSP_EXTENT_SIZE, 2 * FSP_EXTENT_SIZE); }

  Dblwr &dblwr() { return *m_sys->m_dblwr; }

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
  ut::fs::path m_path;
};

/** Ready, with its two blocks of FSP_EXTENT_SIZE pages at 64 and 128. */
TEST_F(DblwrSysTest, Ready) {
  EXPECT_TRUE(dblwr().is_ready());
  EXPECT_EQ(dblwr().m_block1, FSP_EXTENT_SIZE);
  EXPECT_EQ(dblwr().m_block2, 2 * FSP_EXTENT_SIZE);
  EXPECT_EQ(dblwr().m_bpages.size(), 2 * SYS_DOUBLEWRITE_BLOCK_SIZE);

  std::pair<page_no_t, page_no_t> blocks{};

  ASSERT_TRUE(dblwr().check_if_exists(blocks));
  EXPECT_EQ(blocks, std::make_pair(dblwr().m_block1, dblwr().m_block2));
}

/** is_page_inside() at the edges of both blocks. */
TEST_F(DblwrSysTest, PageInside) {
  const auto b1 = dblwr().m_block1;
  const auto b2 = dblwr().m_block2;

  EXPECT_FALSE(dblwr().is_page_inside(0));
  EXPECT_FALSE(dblwr().is_page_inside(b1 - 1));
  EXPECT_TRUE(dblwr().is_page_inside(b1));
  EXPECT_TRUE(dblwr().is_page_inside(b1 + SYS_DOUBLEWRITE_BLOCK_SIZE - 1));
  EXPECT_TRUE(dblwr().is_page_inside(b2));
  EXPECT_TRUE(dblwr().is_page_inside(b2 + SYS_DOUBLEWRITE_BLOCK_SIZE - 1));
  EXPECT_FALSE(dblwr().is_page_inside(b2 + SYS_DOUBLEWRITE_BLOCK_SIZE));
  EXPECT_FALSE(dblwr().is_page_inside(FIL_NULL - 1));
}

/** A Dblwr that has not been created or found: not ready, and no page is
inside it. */
TEST_F(DblwrSysTest, NotReady) {
  Dblwr fresh(*m_sys);

  EXPECT_FALSE(fresh.is_ready());
  EXPECT_EQ(fresh.m_first_free, 0UL);

  for (page_no_t p : {0U, 64U, 128U, 191U, 1000U}) {
    EXPECT_FALSE(fresh.is_page_inside(p)) << p;
  }
}

/** A read of a doublewrite buffer page into the buffer pool is refused.
(They are in the pool of a new database: Dblwr::initialize() allocates them
with fseg_alloc_free_page(), which creates them there.) */
TEST_F(DblwrSysTest, NotReadIntoPool) {
  for (page_no_t p : {dblwr().m_block1, page_no_t(dblwr().m_block2 + 5), page_no_t(dblwr().m_block2 + SYS_DOUBLEWRITE_BLOCK_SIZE - 1)}) {
    EXPECT_FALSE(buf_read_page(*m_sys, PageId(SYS_TABLESPACE, p))) << p;
  }
}

/** A flush writes the pages through the doublewrite buffer: the counters
go up, the copies are in its area of system.ibd, identical to the pages
written in place, and the buffer is empty afterwards. */
TEST_F(DblwrSysTest, FlushThrough) {
  const auto space = new_space(32);

  flush_all();

  const auto pages = m_sys->m_srv->m_stats.m_dblwr_pages_written;
  const auto writes = m_sys->m_srv->m_stats.m_dblwr_writes;

  for (page_no_t p = 0; p < 20; ++p) {
    put(PageId(space, p), p + 1000);
  }

  flush_all();

  EXPECT_GE(m_sys->m_srv->m_stats.m_dblwr_pages_written, pages + 20);
  EXPECT_GT(m_sys->m_srv->m_stats.m_dblwr_writes, writes);
  EXPECT_EQ(dblwr().m_first_free, 0UL);

  const auto area = dblwr_area();
  const auto data = read_pages(m_path, 0, 20);
  std::set<page_no_t> found;

  for (ulint i{}; i < 2 * FSP_EXTENT_SIZE; ++i) {
    const auto copy = area.data() + i * UNIV_PAGE_SIZE;

    if (mach_read_from_4(copy + FIL_PAGE_SPACE_ID) != space) {
      continue;
    }

    const auto p = page_no_t(mach_read_from_4(copy + FIL_PAGE_OFFSET));

    ASSERT_LT(p, 20U);
    EXPECT_EQ(mach_read_from_4(copy + FIL_PAGE_DATA), p + 1000UL);
    EXPECT_EQ(memcmp(copy, data.data() + size_t(p) * UNIV_PAGE_SIZE, UNIV_PAGE_SIZE), 0) << p;
    found.insert(p);
  }

  EXPECT_EQ(found.size(), 20UL);
}

/** A batch of more than one block of pages fills the second block too. */
TEST_F(DblwrSysTest, BothBlocks) {
  const auto space = new_space(200);

  flush_all();

  for (page_no_t p = 0; p < 2 * SYS_DOUBLEWRITE_BLOCK_SIZE; ++p) {
    put(PageId(space, p), p);
  }

  flush_all();

  const auto area = dblwr_area();
  ulint in_block2{};

  for (ulint i = SYS_DOUBLEWRITE_BLOCK_SIZE; i < 2 * SYS_DOUBLEWRITE_BLOCK_SIZE; ++i) {
    in_block2 += mach_read_from_4(area.data() + i * UNIV_PAGE_SIZE + FIL_PAGE_SPACE_ID) == space;
  }

  EXPECT_GT(in_block2, 0UL);
  EXPECT_EQ(dblwr().m_first_free, 0UL);

  const auto data = read_pages(m_path, 0, 2 * SYS_DOUBLEWRITE_BLOCK_SIZE);

  for (page_no_t p = 0; p < 2 * SYS_DOUBLEWRITE_BLOCK_SIZE; ++p) {
    EXPECT_EQ(mach_read_from_4(data.data() + size_t(p) * UNIV_PAGE_SIZE + FIL_PAGE_DATA), ulint(p)) << p;
  }
}

/** Many more dirty pages than the buffer holds: several rounds of it. */
TEST_F(DblwrSysTest, ManyBatches) {
  const page_no_t N = 1000;
  const auto space = new_space(N);

  flush_all();

  const auto writes = m_sys->m_srv->m_stats.m_dblwr_writes;

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), p * 2);
  }

  flush_all();

  EXPECT_GE(m_sys->m_srv->m_stats.m_dblwr_writes - writes, N / (2 * SYS_DOUBLEWRITE_BLOCK_SIZE));

  const auto data = read_pages(m_path, 0, N);

  for (page_no_t p = 0; p < N; ++p) {
    ASSERT_EQ(mach_read_from_4(data.data() + size_t(p) * UNIV_PAGE_SIZE + FIL_PAGE_DATA), ulint(p) * 2) << p;
  }
}

}  // namespace

namespace {

using DblwrPerf = DblwrSysTest;

/** Flush of dirty pages through the doublewrite buffer, per page. */
TEST_F(DblwrPerf, Flush) {
  const page_no_t N = 1000;
  const auto space = new_space(N);
  double secs{};
  uint64_t n{};

  flush_all();

  for (int round = 0; round < 5; ++round) {
    for (page_no_t p = 0; p < N; ++p) {
      put(PageId(space, p), p + round);
    }

    const auto start = std::chrono::steady_clock::now();

    flush_all();

    const std::chrono::duration<double> d = std::chrono::steady_clock::now() - start;

    secs += d.count();
    n += N;
  }

  ut::report(ut::BenchResult{"dblwr_flush_page", 1, n, secs});
  EXPECT_LT(secs * 1e9 / double(n), 10e6);
}

}  // namespace
