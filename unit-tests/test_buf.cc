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

/** @file unit-tests/test_buf.cc
BufPool: pages read, evicted, flushed and read again, with a buffer pool
much smaller than the data.

BufTest uses the test table through the API. BufPoolTest drives the buffer
pool of a shared started System directly, on scratch tablespaces created
with Fil (no Fsp: the pages are made with BufPool::create() and modified
with logged writes in mini-transactions, each page holding its page number,
space id and a value at FIL_PAGE_DATA). */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "buf0rea.h"
#include "fil0fil.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "srv0srv.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <format>
#include <thread>
#include <vector>

namespace {

/** A database with a small buffer pool. */
class BufTest : public ut::DbTest {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_buf_pool.m_size = 8 * 1024 * 1024;
    start();
  }
};

TEST_F(BufTest, Size) {
  /* In bytes. */
  EXPECT_EQ(m_sys->m_buf_pool->get_curr_size(), 8UL * 1024 * 1024);
}

/** Ten times more data than buffer pool: pages are evicted and flushed,
then read back for the checks. */
TEST_F(BufTest, DataBiggerThanPool) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 4000; ++k) {
    rows[k] = ut::Row{k, k % 31, ut::blob(k, k % 10 == 0 ? 60000 : 1500)};
  }

  for (uint32_t base = 0; base < 4000; base += 500) {
    ut::Rows part(rows.lower_bound(base), rows.lower_bound(base + 500));

    load(part);
  }

  expect_rows(rows);

  /* Updates of pages that have been evicted. */
  auto trx = begin();

  for (uint32_t k = 0; k < 4000; k += 13) {
    rows[k].m_a += 1;
    ASSERT_EQ(update(trx, k, rows[k]), DB_SUCCESS);
  }

  commit(trx);
  expect_rows(rows);

  restart();
  expect_rows(rows);
}

/** Several readers scanning a table larger than the pool at once. */
TEST_F(BufTest, ConcurrentScans) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 2000; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 4000)};
  }

  load(rows);

  std::vector<std::thread> threads;

  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 2; ++i) {
        auto trx = begin();

        EXPECT_EQ(scan(trx).size(), rows.size());
        commit(trx);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }
}

/** Size of the pool of the shared System: 1024 pages. */
constexpr ulint POOL_SIZE = 16 * 1024 * 1024;

/** A System started once for all the tests of a suite (starting and
stopping one takes seconds), with a small buffer pool. */
class SharedSys : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    s_dir = std::make_unique<ut::TestDir>();
    ut::fs::create_directories(s_dir->m_path / "test");

    auto config = ut::make_config(s_dir->m_path);

    config.m_buf_pool.m_size = POOL_SIZE;

    s_sys = std::make_unique<System>();

    ASSERT_EQ(s_sys->init(config), DB_SUCCESS);
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

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
};

/** Scratch tablespaces and pages on the shared System. */
class BufPoolTest : public SharedSys {
 protected:
  /** Create a tablespace of n_pages pages, named after the test. */
  space_id_t new_space(page_no_t n_pages, const std::string &suffix = "") {
    space_id_t id{SYS_TABLESPACE};
    const auto name = std::string("test/") + ::testing::UnitTest::GetInstance()->current_test_info()->name() + suffix;

    EXPECT_EQ(m_sys->m_fil->create_new_single_table_tablespace(&id, name.c_str(), false, 0, FIL_IBD_FILE_INITIAL_SIZE), DB_SUCCESS);

    page_no_t actual{};

    EXPECT_TRUE(m_sys->m_fil->extend_space_to_desired_size(&actual, id, n_pages));

    m_path = s_dir->m_path / (name + ".ibd");

    return id;
  }

  /** @return a request for page_id. */
  BufPool::Request req(const PageId &page_id, ulint latch, Mtr &mtr, ulint mode = BUF_GET) {
    return BufPool::Request{
      .m_rw_latch = latch, .m_page_id = page_id, .m_mode = mode, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
    };
  }

  /** Get a page in mtr. */
  BufBlock *get(const PageId &page_id, ulint latch, Mtr &mtr, ulint mode = BUF_GET) {
    auto r = req(page_id, latch, mtr, mode);

    return pool().get(r, nullptr);
  }

  /** Create page_id in the pool, initialized and holding value, logged. */
  void put(const PageId &page_id, uint32_t value) {
    Mtr mtr;

    mtr.start();

    auto block = pool().create(page_id, &mtr);

    ASSERT_EQ(get(page_id, RW_X_LATCH, mtr), block);

    auto frame = block->get_frame();

    /* The frame holds whatever page it held before: the page id goes first,
    unlogged, as in fsp_init_file_page(), the log records are addressed with
    it. */
    mach_write_to_4(frame + FIL_PAGE_OFFSET, page_id.page_no());
    mach_write_to_4(frame + FIL_PAGE_SPACE_ID, page_id.space_id());
    mlog_write_ulint(frame + FIL_PAGE_DATA, value, MLOG_4BYTES, &mtr);

    mtr.commit(*m_sys);
  }

  /** Change the value of a page in the pool or on disk. */
  void set(const PageId &page_id, uint32_t value) {
    Mtr mtr;

    mtr.start();
    mlog_write_ulint(get(page_id, RW_X_LATCH, mtr)->get_frame() + FIL_PAGE_DATA, value, MLOG_4BYTES, &mtr);
    mtr.commit(*m_sys);
  }

  /** @return the value of a page, read through the pool. */
  uint32_t value(const PageId &page_id, ulint latch = RW_S_LATCH) {
    Mtr mtr;

    mtr.start();

    auto block = get(page_id, latch, mtr);
    auto frame = block->get_frame();

    EXPECT_EQ(mach_read_from_4(frame + FIL_PAGE_OFFSET), page_id.page_no());
    EXPECT_EQ(mach_read_from_4(frame + FIL_PAGE_SPACE_ID), page_id.space_id());

    const auto v = uint32_t(mach_read_from_4(frame + FIL_PAGE_DATA));

    mtr.commit(*m_sys);

    return v;
  }

  /** @return the value of page_no in the file, read around the pool. */
  uint32_t disk_value(page_no_t page_no) {
    auto buf = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));
    const auto fd = ::open(m_path.c_str(), O_RDONLY);

    EXPECT_GE(fd, 0);
    EXPECT_EQ(::pread(fd, buf, UNIV_PAGE_SIZE, off_t(page_no) * UNIV_PAGE_SIZE), ssize_t(UNIV_PAGE_SIZE));
    ::close(fd);

    const auto v = uint32_t(mach_read_from_4(buf + FIL_PAGE_DATA));

    std::free(buf);

    return v;
  }

  /** Flush all the dirty pages of the pool. */
  void flush_all() {
    auto &flusher = *pool().m_flusher;

    while (flusher.batch(BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX) == ULINT_UNDEFINED) {
      flusher.wait_batch_end(BUF_FLUSH_LIST);
    }

    flusher.wait_batch_end(BUF_FLUSH_LIST);
  }

  /** @return the buf-fix count of a page in the pool, 0 if not there. */
  ulint fix_count(const PageId &page_id) {
    pool().mutex_acquire();

    auto bpage = pool().hash_get_page(page_id);
    const ulint n = bpage == nullptr ? 0 : bpage->m_buf_fix_count;

    pool().mutex_release();

    return n;
  }

  /** @return the control block of a cached page, or nullptr. */
  BufPage *cached(const PageId &page_id) {
    pool().mutex_acquire();

    auto bpage = pool().hash_get_page(page_id);

    pool().mutex_release();

    return bpage;
  }

  /** @return the block of a cached page, or nullptr. */
  BufBlock *cached_block(const PageId &page_id) {
    auto bpage = cached(page_id);

    return bpage == nullptr ? nullptr : bpage->get_block();
  }

  /** @return true if the pool's lists are consistent. */
  bool valid() {
#ifdef UNIV_DEBUG
    return pool().validate() && pool().m_LRU->validate() && pool().m_flusher->validate();
#else
    return true;
#endif /* UNIV_DEBUG */
  }

  BufPool &pool() { return *m_sys->m_buf_pool; }

  ut::fs::path m_path;
};

/** The pool of the shared System has the configured size; the read-ahead
area follows from it. */
TEST_F(BufPoolTest, Size) {
  EXPECT_EQ(pool().get_curr_size(), POOL_SIZE);
  EXPECT_EQ(pool().m_curr_size, POOL_SIZE / UNIV_PAGE_SIZE);
  EXPECT_EQ(pool().get_read_ahead_area(), std::min<ulint>(64, ut_2_power_up(pool().m_curr_size / 32)));
  EXPECT_LE(UT_LIST_GET_LEN(pool().m_LRU_list) + UT_LIST_GET_LEN(pool().m_free_list), pool().m_curr_size);
  EXPECT_TRUE(valid());
}

/** A created page is in the page hash, buffer-fixed by its mtr, and can be
got with each latch mode, the same block each time. */
TEST_F(BufPoolTest, CreateGet) {
  const auto space = new_space(16);
  const PageId id(space, 3);

  EXPECT_FALSE(pool().peek(id));
  EXPECT_EQ(cached(id), nullptr);

  {
    Mtr mtr;

    mtr.start();

    auto block = pool().create(id, &mtr);

    ASSERT_NE(block, nullptr);
    EXPECT_EQ(block->get_state(), BUF_BLOCK_FILE_PAGE);
    EXPECT_EQ(block->get_page_id(), id);
    EXPECT_EQ(fix_count(id), 1UL);
    EXPECT_TRUE(pool().peek(id));
    EXPECT_EQ(cached_block(id), block);
    EXPECT_EQ(Fil::page_get_type(block->get_frame()), FIL_PAGE_TYPE_ALLOCATED);
    EXPECT_EQ(mach_read_from_4(block->get_frame() + FIL_PAGE_PREV), FIL_NULL);
    EXPECT_EQ(mach_read_from_4(block->get_frame() + FIL_PAGE_NEXT), FIL_NULL);

    for (auto latch : {RW_S_LATCH, RW_NO_LATCH}) {
      EXPECT_EQ(get(id, latch, mtr, latch == RW_NO_LATCH ? BUF_GET_NO_LATCH : BUF_GET), block);
    }

    EXPECT_EQ(fix_count(id), 3UL);

    mtr.commit(*m_sys);
  }

  EXPECT_EQ(fix_count(id), 0UL);

  put(id, 42);
  EXPECT_EQ(value(id, RW_S_LATCH), 42U);
  EXPECT_EQ(value(id, RW_X_LATCH), 42U);
  EXPECT_TRUE(valid());
}

/** Creating a page already in the pool returns the cached block, buffer
fixed. */
TEST_F(BufPoolTest, CreateExisting) {
  const auto space = new_space(16);
  const PageId id(space, 1);

  put(id, 7);

  Mtr mtr;

  mtr.start();

  auto a = cached_block(id);
  auto b = pool().create(id, &mtr);

  EXPECT_EQ(a, b);
  EXPECT_EQ(fix_count(id), 1UL);

  mtr.commit(*m_sys);
}

/** BUF_GET_IF_IN_POOL doesn't read: nullptr for a page that isn't cached. */
TEST_F(BufPoolTest, GetIfInPool) {
  const auto space = new_space(16);
  const PageId id(space, 5);

  Mtr mtr;

  mtr.start();
  EXPECT_EQ(get(id, RW_S_LATCH, mtr, BUF_GET_IF_IN_POOL), nullptr);
  EXPECT_FALSE(pool().peek(id));
  mtr.commit(*m_sys);

  put(id, 1);

  mtr.start();
  EXPECT_NE(get(id, RW_S_LATCH, mtr, BUF_GET_IF_IN_POOL), nullptr);
  mtr.commit(*m_sys);
}

/** BUF_GET of a page not in the pool reads it from the file. */
TEST_F(BufPoolTest, GetReads) {
  const auto space = new_space(16);
  const PageId id(space, 9);

  /* Write the page around the pool. */
  auto buf = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  memset(buf, 0, UNIV_PAGE_SIZE);
  mach_write_to_4(buf + FIL_PAGE_OFFSET, 9);
  mach_write_to_4(buf + FIL_PAGE_SPACE_ID, space);
  mach_write_to_4(buf + FIL_PAGE_DATA, 4242);
  pool().m_flusher->init_for_writing(buf, 1);

  ASSERT_EQ(m_sys->m_fil->data_io(IoRequest::Sync_write, false, id, 0, UNIV_PAGE_SIZE, buf, nullptr), DB_SUCCESS);
  std::free(buf);

  const auto reads = pool().m_stat.n_pages_read;

  EXPECT_FALSE(pool().peek(id));
  EXPECT_EQ(value(id), 4242U);
  EXPECT_TRUE(pool().peek(id));
  EXPECT_EQ(pool().m_stat.n_pages_read, reads + 1);

  /* A hit doesn't read. */
  EXPECT_EQ(value(id), 4242U);
  EXPECT_EQ(pool().m_stat.n_pages_read, reads + 1);
}

/** The latch modes: S latches are shared, X excludes; RW_NO_LATCH only
fixes. Checked with the no-wait gets from another thread. */
TEST_F(BufPoolTest, LatchModes) {
  const auto space = new_space(16);
  const PageId id(space, 2);

  put(id, 1);

  /* Clean: the flusher would hold an S latch on it while it writes, in the counts below. */
  flush_all();

  auto block = cached_block(id);

  /* Try a no-wait S then X get from another thread. @return {s ok, x ok} */
  auto probe = [&]() {
    std::pair<bool, bool> r;

    std::thread([&] {
      Mtr mtr;

      mtr.start();

      BufPool::Request s{.m_rw_latch = RW_S_LATCH, .m_guess = block, .m_mode = BUF_KEEP_OLD, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr};

      r.first = pool().try_get_known_nowait(s);
      mtr.commit(*m_sys);

      mtr.start();

      BufPool::Request x{.m_rw_latch = RW_X_LATCH, .m_guess = block, .m_mode = BUF_KEEP_OLD, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr};

      r.second = pool().try_get_known_nowait(x);
      mtr.commit(*m_sys);
    }).join();

    return r;
  };

  EXPECT_EQ(probe(), std::make_pair(true, true));

  Mtr mtr;

  mtr.start();
  get(id, RW_S_LATCH, mtr);
  EXPECT_EQ(rw_lock_get_reader_count(&block->m_rw_lock), 1UL);
  EXPECT_EQ(probe(), std::make_pair(true, false)) << "S held";
  mtr.commit(*m_sys);

  mtr.start();
  get(id, RW_X_LATCH, mtr);
  EXPECT_EQ(rw_lock_get_writer(&block->m_rw_lock), RW_LOCK_EX);
  EXPECT_EQ(probe(), std::make_pair(false, false)) << "X held";
  mtr.commit(*m_sys);

  /* Owning it by its bits: exclusive, no RW latch, and the version says so. */
  {
    const auto before = block->read_begin();

    mtr.start();
    get(id, RW_B_LATCH, mtr);
    EXPECT_EQ(rw_lock_get_writer(&block->m_rw_lock), RW_LOCK_NOT_LOCKED);
    EXPECT_EQ(block->read_begin(), before + 1);

    std::thread([&] { EXPECT_FALSE(block->bits_try_lock()); }).join();

    mtr.commit(*m_sys);
    EXPECT_EQ(block->read_begin(), before + 2);
    EXPECT_EQ(block->m_bits.load(), 0UL);
  }

  /* The version is odd for exactly as long as a thread can be writing. */
  const auto version = block->read_begin();

  EXPECT_EQ(version % 2, 0UL);

  mtr.start();
  get(id, RW_S_LATCH, mtr);
  EXPECT_EQ(block->read_begin(), version) << "S held";
  EXPECT_TRUE(block->read_is_valid(version));
  mtr.commit(*m_sys);

  mtr.start();
  get(id, RW_X_LATCH, mtr);
  EXPECT_EQ(block->read_begin(), version + 1) << "X held";
  EXPECT_FALSE(block->read_is_valid(version));
  EXPECT_FALSE(block->read_is_valid(version + 1));
  get(id, RW_X_LATCH, mtr);
  EXPECT_EQ(block->read_begin(), version + 1) << "X in X";
  mtr.commit(*m_sys);
  EXPECT_EQ(block->read_begin(), version + 2);
  EXPECT_FALSE(block->read_is_valid(version));

  mtr.start();
  get(id, RW_NO_LATCH, mtr, BUF_GET_NO_LATCH);
  EXPECT_EQ(rw_lock_get_writer(&block->m_rw_lock), RW_LOCK_NOT_LOCKED);
  EXPECT_EQ(rw_lock_get_reader_count(&block->m_rw_lock), 0UL);
  EXPECT_EQ(fix_count(id), 1UL);
  EXPECT_EQ(probe(), std::make_pair(true, true)) << "fixed only";
  mtr.commit(*m_sys);

  EXPECT_EQ(fix_count(id), 0UL);
}

/** Each get buffer-fixes the page once more, the mtr commit releases all. */
TEST_F(BufPoolTest, FixCount) {
  const auto space = new_space(16);
  const PageId id(space, 4);

  put(id, 1);

  Mtr mtr;

  mtr.start();

  for (ulint i = 1; i <= 5; ++i) {
    get(id, RW_S_LATCH, mtr);
    EXPECT_EQ(fix_count(id), i);
  }

  mtr.commit(*m_sys);
  EXPECT_EQ(fix_count(id), 0UL);
}

/** The optimistic get succeeds while the modify clock is unchanged. */
TEST_F(BufPoolTest, TryGet) {
  const auto space = new_space(16);
  const PageId id(space, 6);

  put(id, 1);

  auto block = cached_block(id);
  const auto clock = block->m_modify_clock;

  Mtr mtr;

  mtr.start();

  BufPool::Request r{.m_rw_latch = RW_S_LATCH, .m_guess = block, .m_modify_clock = clock, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr};

  EXPECT_TRUE(pool().try_get(r));
  EXPECT_EQ(fix_count(id), 1UL);
  mtr.commit(*m_sys);

  /* Someone changed the page's records: the clock moved on. */
  mtr.start();
  get(id, RW_X_LATCH, mtr);
  ++block->m_modify_clock;
  mtr.commit(*m_sys);

  mtr.start();

  BufPool::Request stale{
    .m_rw_latch = RW_X_LATCH, .m_guess = block, .m_modify_clock = clock, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
  };

  EXPECT_FALSE(pool().try_get(stale));
  EXPECT_EQ(fix_count(id), 0UL) << "the failed try is unfixed";
  mtr.commit(*m_sys);
}

/** try_get_by_page_id(): the block if cached, never a read. */
TEST_F(BufPoolTest, TryGetByPageId) {
  const auto space = new_space(16);
  const PageId id(space, 7);

  Mtr mtr;

  mtr.start();

  auto r = req(id, RW_S_LATCH, mtr);

  EXPECT_EQ(pool().try_get_by_page_id(r), nullptr);
  mtr.commit(*m_sys);

  put(id, 1);

  mtr.start();
  r = req(id, RW_S_LATCH, mtr);
  EXPECT_EQ(pool().try_get_by_page_id(r), cached_block(id));
  mtr.commit(*m_sys);
}

/** A modified page is dirty: in the flush list with its oldest and newest
lsn, until it is flushed; then the file has the change. */
TEST_F(BufPoolTest, Flush) {
  const auto space = new_space(16);
  const PageId id(space, 8);

  put(id, 100);

  auto bpage = cached(id);

  EXPECT_NE(bpage->m_oldest_modification, 0UL);
  EXPECT_GE(bpage->m_newest_modification, bpage->m_oldest_modification);
  EXPECT_NE(pool().get_oldest_modification(), 0UL);
  EXPECT_LE(pool().get_oldest_modification(), bpage->m_oldest_modification);
  /* Not get_modified_ratio_pct() > 0: an integer percentage, one dirty page
  in a pool of more than 100 rounds to 0 (it passed only when background
  work had dirtied other pages). */

  const auto oldest = bpage->m_oldest_modification;

  set(id, 101);
  EXPECT_EQ(bpage->m_oldest_modification, oldest) << "the first change counts";
  EXPECT_GT(bpage->m_newest_modification, oldest);

  flush_all();

  EXPECT_EQ(bpage->m_oldest_modification, 0UL);
  EXPECT_EQ(disk_value(8), 101U);
  EXPECT_EQ(value(id), 101U);
  EXPECT_TRUE(valid());
}

/** The flush list is ordered by oldest modification, newest first. */
TEST_F(BufPoolTest, FlushListOrder) {
  const auto space = new_space(64);

  for (page_no_t p = 0; p < 50; ++p) {
    put(PageId(space, p), p);
  }

  pool().mutex_acquire();

  lsn_t prev{IB_UINT64_T_MAX};
  ulint n{};

  for (auto bpage : pool().m_flush_list) {
    EXPECT_LE(bpage->m_oldest_modification, prev);
    EXPECT_NE(bpage->m_oldest_modification, 0UL);
    prev = bpage->m_oldest_modification;
    ++n;
  }

  pool().mutex_release();

  EXPECT_GE(n, 50UL);
  EXPECT_TRUE(valid());

  flush_all();

  for (page_no_t p = 0; p < 50; ++p) {
    EXPECT_EQ(disk_value(p), p);
  }
}

/** Many more pages than the pool holds: the least recently used are
evicted (after being flushed), and read back with their last value. */
TEST_F(BufPoolTest, Eviction) {
  const page_no_t N = page_no_t(3 * pool().m_curr_size);
  const auto space = new_space(N);

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), p * 3 + 1);
  }

  ulint cached{};

  for (page_no_t p = 0; p < N; ++p) {
    cached += pool().peek(PageId(space, p));
  }

  /* All of the pool may be this table's: nothing else has to stay in it. */
  EXPECT_LE(cached, pool().m_curr_size);
  EXPECT_FALSE(pool().peek(PageId(space, 0))) << "the first pages are the least recently used";
  EXPECT_TRUE(pool().peek(PageId(space, N - 1)));
  EXPECT_LE(UT_LIST_GET_LEN(pool().m_LRU_list), pool().m_curr_size);
  EXPECT_TRUE(valid());

  for (page_no_t p = 0; p < N; ++p) {
    ASSERT_EQ(value(PageId(space, p)), p * 3 + 1) << p;
  }

  EXPECT_TRUE(valid());
}

/** make_young() puts a page at the head of the LRU list. */
TEST_F(BufPoolTest, MakeYoung) {
  const auto space = new_space(16);

  for (page_no_t p = 0; p < 10; ++p) {
    put(PageId(space, p), p);
  }

  auto bpage = cached(PageId(space, 0));

  pool().make_young(bpage);

  pool().mutex_acquire();
  EXPECT_EQ(UT_LIST_GET_FIRST(pool().m_LRU_list), bpage);
  pool().mutex_release();
}

/** The LRU list has an old sublist of about the configured share once it is
long enough. */
TEST_F(BufPoolTest, OldSublist) {
  const auto space = new_space(page_no_t(pool().m_curr_size));

  for (page_no_t p = 0; p < page_no_t(pool().m_curr_size); ++p) {
    put(PageId(space, p), p);
  }

  pool().mutex_acquire();

  const auto len = UT_LIST_GET_LEN(pool().m_LRU_list);
  const auto old_len = pool().m_LRU_old_len;
  ulint n_old{};

  for (auto bpage : pool().m_LRU_list) {
    n_old += bpage->m_old;
  }

  pool().mutex_release();

  if (len >= BufLru::OLD_MIN_LEN) {
    EXPECT_NE(pool().m_LRU_old, nullptr);
    EXPECT_EQ(n_old, old_len);

    const auto expected = len * pool().m_LRU->get_old_ratio() / BufLru::OLD_RATIO_DIV;

    EXPECT_NEAR(double(old_len), double(expected), double(len) / 10) << len;
  }

  EXPECT_TRUE(valid());
}

/** Blocks for other uses than file pages come from the free list (or the
LRU tail) and go back to the free list. */
TEST_F(BufPoolTest, BlockAlloc) {
  const auto free_len = [&] {
    pool().mutex_acquire();
    const auto n = UT_LIST_GET_LEN(pool().m_free_list);
    pool().mutex_release();
    return n;
  };

  const auto free_before = free_len();

  auto block = pool().block_alloc();

  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->get_state(), BUF_BLOCK_MEMORY);
  EXPECT_FALSE(pool().pointer_is_block_field(block->get_frame()));
  EXPECT_TRUE(pool().pointer_is_block_field(&block->m_page));
  EXPECT_EQ(pool().block_align(block->get_frame() + 100), block);
  EXPECT_EQ(pool().block_align(block->get_frame() + UNIV_PAGE_SIZE - 1), block);

  pool().block_free(block);

  EXPECT_EQ(block->get_state(), BUF_BLOCK_NOT_USED);
  EXPECT_GE(free_len(), free_before > 0 ? free_before : 1);
  EXPECT_TRUE(valid());
}

/** is_corrupted(): a page stamped by init_for_writing() is good, so is a
new (zero) page; a torn page (the lsn in the header and in the trailer
differ) is not. */
TEST_F(BufPoolTest, Corruption) {
  auto page = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  for (ulint i{}; i < UNIV_PAGE_SIZE; ++i) {
    page[i] = byte(i * 7);
  }

  pool().m_flusher->init_for_writing(page, 1000);
  EXPECT_FALSE(pool().is_corrupted(page));
  EXPECT_EQ(mach_read_from_4(page + FIL_PAGE_LSN + 4), 1000UL);
  EXPECT_EQ(mach_read_from_4(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + 4), 1000UL);

  std::vector<byte> zero(UNIV_PAGE_SIZE + UNIV_PAGE_SIZE);
  auto z = static_cast<byte *>(ut_align(zero.data(), UNIV_PAGE_SIZE));

  EXPECT_FALSE(pool().is_corrupted(z));

  mach_write_to_8(page + FIL_PAGE_LSN, 1001);
  EXPECT_TRUE(pool().is_corrupted(page)) << "torn: lsn at the start and the end differ";

  std::free(page);
}

/* Regression: page checksums are not computed: buf_page_data_calc_checksum() returns
the constant crc32::NO_CHECKSUM (0x1EDC6F41) for every page, so is_corrupted()
never detects a changed byte between the header and the trailer; only a torn
lsn is caught. include/ut0crc32.h guards the whole CRC-32C implementation
and its selection in crc32::init() with #if defined(__SSE4_2__), and the
engine is compiled without -msse4.2 (CMakeLists.txt adds it to
log/log0wal.cc only): every other translation unit's init() returns the
"no checksum" lambda. `inline const Checksum checksum{init()}` is then
initialized by whichever TU's dynamic initializer runs first (an ODR
violation): here the constant one. See include/ut0crc32.h:30-33,68,533-553. */
TEST_F(BufPoolTest, Checksum) {
  auto a = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));
  auto b = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  memset(a, 0x11, UNIV_PAGE_SIZE);
  memset(b, 0x22, UNIV_PAGE_SIZE);

  EXPECT_NE(buf_page_data_calc_checksum(a), buf_page_data_calc_checksum(b));

  pool().m_flusher->init_for_writing(a, 1000);
  EXPECT_FALSE(pool().is_corrupted(a));

  /* A bit flip in the middle of the page. */
  a[5000] ^= 1;
  EXPECT_TRUE(pool().is_corrupted(a)) << "checksum mismatch";

  std::free(a);
  std::free(b);
}

/** Bytes outside the checksummed area of a page don't make it corrupt:
FIL_PAGE_FILE_FLUSH_LSN is rewritten on page 0 of the system tablespace at
every shutdown, without a new checksum. */
TEST_F(BufPoolTest, FlushLsnNotChecksummed) {
  auto page = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  memset(page, 0x11, UNIV_PAGE_SIZE);
  pool().m_flusher->init_for_writing(page, 1000);
  mach_write_to_8(page + FIL_PAGE_FILE_FLUSH_LSN, 123456789);

  EXPECT_FALSE(pool().is_corrupted(page));

  std::free(page);
}

/** A fresh System for the read-ahead tests: no page has been evicted yet.
Once eviction has started (BufPool::m_freed_page_clock > 0), a page read into
the old part of the LRU list is "too old" at its first access and is moved
to the head without its access time being set (set_accessed_make_young(),
as in the upstream plugin), and linear read-ahead never sees an accessed area. */
class BufReadAheadTest : public BufPoolTest {};

/** Linear read-ahead: reading an area of pages in ascending order, linked
by their next and prev pointers, reads the next area in the background. */
TEST_F(BufReadAheadTest, Linear) {
  const auto area = page_no_t(pool().get_read_ahead_area());
  const auto space = new_space(4 * area);
  auto buf = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  for (page_no_t p = 0; p < 4 * area; ++p) {
    memset(buf, 0, UNIV_PAGE_SIZE);
    mach_write_to_4(buf + FIL_PAGE_OFFSET, p);
    mach_write_to_4(buf + FIL_PAGE_SPACE_ID, space);
    mach_write_to_4(buf + FIL_PAGE_PREV, p == 0 ? FIL_NULL : p - 1);
    mach_write_to_4(buf + FIL_PAGE_NEXT, p + 1);
    mach_write_to_4(buf + FIL_PAGE_DATA, p);
    pool().m_flusher->init_for_writing(buf, 1);
    ASSERT_EQ(m_sys->m_fil->data_io(IoRequest::Sync_write, false, PageId(space, p), 0, UNIV_PAGE_SIZE, buf, nullptr), DB_SUCCESS);
  }

  std::free(buf);

  const auto ra = pool().m_stat.n_ra_pages_read;

  /* The first area, page by page, ascending. */
  for (page_no_t p = area; p < 2 * area; ++p) {
    ASSERT_EQ(value(PageId(space, p)), p);
  }

  EXPECT_EQ(pool().m_stat.n_ra_pages_read, ra + area) << "the next area was read ahead";

  for (page_no_t p = 2 * area; p < 3 * area; ++p) {
    EXPECT_TRUE(pool().peek(PageId(space, p))) << p;
  }

  /* The pages read ahead are good. */
  for (page_no_t p = 2 * area; p < 3 * area; ++p) {
    EXPECT_EQ(value(PageId(space, p)), p);
  }
}

/** Random access, no read-ahead. */
TEST_F(BufReadAheadTest, Random) {
  const auto area = page_no_t(pool().get_read_ahead_area());
  const auto space = new_space(4 * area);

  for (page_no_t p = 0; p < 4 * area; ++p) {
    put(PageId(space, p), p);
  }

  flush_all();

  const auto ra = pool().m_stat.n_ra_pages_read;

  for (page_no_t i = 0; i < area; ++i) {
    const page_no_t p = (i * 37) % (4 * area);

    EXPECT_EQ(value(PageId(space, p)), p);
  }

  EXPECT_EQ(pool().m_stat.n_ra_pages_read, ra);
}

/** Threads read pages that are all in the pool and increment a counter on
one of them under the X latch: no increment is lost. */
TEST_F(BufPoolTest, ConcurrentHits) {
  const auto space = new_space(64);

  for (page_no_t p = 0; p < 64; ++p) {
    put(PageId(space, p), p);
  }

  const PageId counter(space, 63);

  set(counter, 0);

  constexpr int N_THREADS = 16;
  constexpr uint32_t N = 2000;
  std::atomic<ulint> bad{};
  std::vector<std::thread> threads;

  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (uint32_t i = 0; i < N; ++i) {
        const page_no_t p = (i * 7 + t) % 63;

        if (value(PageId(space, p)) != p) {
          ++bad;
        }

        if (i % 4 == 0) {
          Mtr mtr;

          mtr.start();

          auto frame = get(counter, RW_X_LATCH, mtr)->get_frame();

          mlog_write_ulint(frame + FIL_PAGE_DATA, mach_read_from_4(frame + FIL_PAGE_DATA) + 1, MLOG_4BYTES, &mtr);
          mtr.commit(*m_sys);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(bad.load(), 0UL);
  EXPECT_EQ(value(counter), N_THREADS * N / 4);
  EXPECT_EQ(fix_count(counter), 0UL);
  EXPECT_TRUE(valid());
}

/** Threads read random pages of a set three times bigger than the pool:
misses, reads and evictions at once, every value right. */
TEST_F(BufPoolTest, ConcurrentMisses) {
  const page_no_t N = page_no_t(3 * pool().m_curr_size);
  const auto space = new_space(N);

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), p + 7);
  }

  std::atomic<ulint> bad{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (uint32_t i = 0; i < 1500; ++i) {
        const page_no_t p = page_no_t((uint64_t(i) * 2654435761u + t * 977) % N);

        if (value(PageId(space, p)) != p + 7) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(bad.load(), 0UL);
  EXPECT_TRUE(valid());
}

/** Threads modify pages of a set bigger than the pool while others read:
the pages are flushed to make room and every page has its last value. */
TEST_F(BufPoolTest, ConcurrentWrites) {
  const page_no_t N = page_no_t(2 * pool().m_curr_size);
  const auto space = new_space(N);

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), 0);
  }

  constexpr int N_THREADS = 8;
  std::vector<std::thread> threads;

  /* Thread t owns the pages p with p % N_THREADS == t and counts its writes. */
  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (uint32_t round = 1; round <= 3; ++round) {
        for (page_no_t p = page_no_t(t); p < N; p += N_THREADS) {
          set(PageId(space, p), round);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  for (page_no_t p = 0; p < N; ++p) {
    ASSERT_EQ(value(PageId(space, p)), 3U) << p;
  }

  flush_all();

  for (page_no_t p = 0; p < N; p += 97) {
    EXPECT_EQ(disk_value(p), 3U) << p;
  }

  EXPECT_TRUE(valid());
}

/** An LRU flush batch writes dirty pages from the tail of the LRU list. */
TEST_F(BufPoolTest, LruFlush) {
  const page_no_t N = page_no_t(pool().m_curr_size / 2);
  const auto space = new_space(N);

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), p);
  }

  const auto written = pool().m_stat.n_pages_written;
  ulint n{ULINT_UNDEFINED};

  while ((n = pool().m_flusher->batch(BUF_FLUSH_LRU, 10, 0)) == ULINT_UNDEFINED) {
    pool().m_flusher->wait_batch_end(BUF_FLUSH_LRU);
  }

  pool().m_flusher->wait_batch_end(BUF_FLUSH_LRU);

  EXPECT_GE(pool().m_stat.n_pages_written, written + std::min<ulint>(n, 1));
  EXPECT_TRUE(valid());
}

/** The pages of a deleted space are evicted lazily, when the flush or the LRU meets them
(buf0flu.cc free_page_if_truncated()), not by Fil::delete_tablespace(): filling the pool
with another space leaves none of them, and the pool is consistent. */
TEST_F(BufPoolTest, DeleteSpaceEvictsLazily) {
  const auto space = new_space(16);

  for (page_no_t p = 0; p < 16; ++p) {
    put(PageId(space, p), p);
  }

  flush_all();

  ASSERT_TRUE(m_sys->m_fil->delete_tablespace(space));

  const page_no_t n = page_no_t(2 * pool().m_curr_size);
  const auto other = new_space(n, "other");

  for (page_no_t p = 0; p < n; ++p) {
    put(PageId(other, p), p);
  }

  ulint cached{};

  for (page_no_t p = 0; p < 16; ++p) {
    cached += pool().peek(PageId(space, p));
  }

  EXPECT_EQ(cached, 0UL);
  EXPECT_TRUE(valid());
}

}  // namespace

namespace {

using BufPerf = BufPoolTest;

/** Get of a page in the pool, S latch, one thread. */
TEST_F(BufPerf, GetHit) {
  const auto space = new_space(64);

  for (page_no_t p = 0; p < 64; ++p) {
    put(PageId(space, p), p);
  }

  auto r = ut::bench("buf_get_hit_s", 500000, [&](uint64_t i) {
    Mtr mtr;

    mtr.start();
    ut::keep(get(PageId(space, page_no_t(i % 64)), RW_S_LATCH, mtr));
    mtr.commit(*m_sys);
  });

  EXPECT_LT(r.ns_per_op(), 50000.0);
}

/** Get of a page in the pool, S latch, from 1, 4, 16 threads. */
TEST_F(BufPerf, GetHitMt) {
  const auto space = new_space(256);

  for (page_no_t p = 0; p < 256; ++p) {
    put(PageId(space, p), p);
  }

  for (int n : {1, 4, 16}) {
    auto r = ut::bench_mt(std::format("buf_get_hit_s_{}t", n), n, 100000, [&](int t, uint64_t i) {
      Mtr mtr;

      mtr.start();
      ut::keep(get(PageId(space, page_no_t((i * 7 + t * 31) % 256)), RW_S_LATCH, mtr));
      mtr.commit(*m_sys);
    });

    EXPECT_LT(r.ns_per_op(), 200000.0);
  }
}

/** Create of a new page (with its logged initialization). */
TEST_F(BufPerf, Create) {
  const page_no_t N = 20000;
  const auto space = new_space(N);

  auto r = ut::bench("buf_create_page", N, [&](uint64_t i) { put(PageId(space, page_no_t(i)), uint32_t(i)); });

  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** Misses: each get evicts a clean page and reads one. */
TEST_F(BufPerf, Miss) {
  const page_no_t N = page_no_t(4 * pool().m_curr_size);
  const auto space = new_space(N);

  for (page_no_t p = 0; p < N; ++p) {
    put(PageId(space, p), p);
  }

  flush_all();

  auto r = ut::bench("buf_get_miss_evict", 20000, [&](uint64_t i) {
    ut::keep(value(PageId(space, page_no_t((i * 2654435761u) % N))));
  });

  EXPECT_LT(r.ns_per_op(), 5e6);
}

/** Flush of dirty pages, as a whole flush-list batch. */
TEST_F(BufPerf, FlushList) {
  const page_no_t N = page_no_t(pool().m_curr_size / 2);
  const auto space = new_space(N);
  double secs{};
  uint64_t n{};

  for (int round = 0; round < 10; ++round) {
    for (page_no_t p = 0; p < N; ++p) {
      put(PageId(space, p), p + round);
    }

    const auto start = std::chrono::steady_clock::now();

    flush_all();

    const std::chrono::duration<double> d = std::chrono::steady_clock::now() - start;

    secs += d.count();
    n += N;
  }

  ut::report(ut::BenchResult{"buf_flush_list_page", 1, n, secs});
  EXPECT_LT(secs * 1e9 / double(n), 10e6);
}

}  // namespace
