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

/** @file unit-tests/test_fil.cc
Fil: the tablespace memory cache and the tablespace files.

FilMemTest drives the memory cache of a System that is only init()ed: spaces
and file nodes that exist in memory only (no file is opened). FilTest works
on a started System: scratch tablespaces are created in the test database
directory with Fil::create_new_single_table_tablespace() and used with
synchronous Fil::data_io(), bypassing the buffer pool. */

#include "ut0env.h"
#include "ut0perf.h"

#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "log0log.h"
#include "mach0data.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <format>
#include <set>
#include <thread>
#include <vector>

namespace {

space_id_t table_space(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table == nullptr ? NULL_SPACE_ID : table->m_space_id;
}

/** A page sized, page aligned buffer. */
struct Page {
  Page() : m_ptr(static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE))) { memset(m_ptr, 0, UNIV_PAGE_SIZE); }

  ~Page() { std::free(m_ptr); }

  Page(const Page &) = delete;
  Page &operator=(const Page &) = delete;

  void fill(uint32_t seed) {
    for (ulint i{}; i < UNIV_PAGE_SIZE; ++i) {
      m_ptr[i] = byte((seed * 31 + i) & 0xff);
    }
  }

  [[nodiscard]] bool is(uint32_t seed) const {
    for (ulint i{}; i < UNIV_PAGE_SIZE; ++i) {
      if (m_ptr[i] != byte((seed * 31 + i) & 0xff)) {
        return false;
      }
    }
    return true;
  }

  byte *m_ptr;
};

/** The memory cache of a System that is not started: nothing on disk. */
class FilMemTest : public ut::SysTest {
 protected:
  void TearDown() override {
    /* Every space must have exactly one node for close_all_files(). */
    m_sys->m_fil->close_all_files();
    ut::SysTest::TearDown();
  }

  /** Create a space with one file node of size pages, in memory only. */
  void add(const std::string &name, space_id_t id, ulint size) {
    ASSERT_TRUE(m_sys->m_fil->space_create(name.c_str(), id, 0));
    m_sys->m_fil->node_create(name.c_str(), size, id, false);
  }

  /** @return the path Fil uses for the table name. */
  [[nodiscard]] std::string ibd(const std::string &name) const { return m_sys->m_config.m_fil.m_data_home + name + ".ibd"; }

  Fil &fil() { return *m_sys->m_fil; }
};

/** A created space is in the cache with the size of its node and no flags. */
TEST_F(FilMemTest, SpaceCreate) {
  add("a.ibd", 100, 10);

  EXPECT_TRUE(fil().tablespace_exists_in_mem(100));
  EXPECT_EQ(fil().space_get_size(100), 10UL);
  EXPECT_EQ(fil().space_get_flags(100), 0UL);
  EXPECT_NE(fil().space_get_latch(100), nullptr);
  EXPECT_TRUE(fil().validate());
}

/** The size of a space is the size of its node, page n is in it for
n < size. */
TEST_F(FilMemTest, AddressBounds) {
  add("a.ibd", 100, 10);

  EXPECT_TRUE(fil().check_adress_in_tablespace(100, 0));
  EXPECT_TRUE(fil().check_adress_in_tablespace(100, 9));
  EXPECT_FALSE(fil().check_adress_in_tablespace(100, 10));
  EXPECT_FALSE(fil().check_adress_in_tablespace(100, FIL_NULL));
  EXPECT_FALSE(fil().check_adress_in_tablespace(101, 0));
}

/** A space that doesn't exist: every lookup says so. */
TEST_F(FilMemTest, Missing) {
  EXPECT_FALSE(fil().tablespace_exists_in_mem(77));
  EXPECT_EQ(fil().space_get_size(77), 0UL);
  EXPECT_EQ(fil().space_get_flags(77), ULINT_UNDEFINED);
  EXPECT_EQ(fil().space_get_version(77), -1);
  EXPECT_TRUE(fil().tablespace_deleted_or_being_deleted_in_mem(77, -1));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(77, "test/none", false, false, false));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(77, "test/none", false, false, true));
}

/** The system tablespace has no flags, even when it is not in the cache. */
TEST_F(FilMemTest, SysFlags) {
  EXPECT_EQ(fil().space_get_flags(SYS_TABLESPACE), 0UL);
}

/** Every space created gets a newer version than the ones before it. */
TEST_F(FilMemTest, Versions) {
  add("a.ibd", 100, 4);
  add("b.ibd", 101, 4);
  add("c.ibd", 7, 4);

  const auto a = fil().space_get_version(100);
  const auto b = fil().space_get_version(101);
  const auto c = fil().space_get_version(7);

  EXPECT_GT(a, 0);
  EXPECT_GT(b, a);
  EXPECT_GT(c, b);

  EXPECT_FALSE(fil().tablespace_deleted_or_being_deleted_in_mem(100, -1));
  EXPECT_FALSE(fil().tablespace_deleted_or_being_deleted_in_mem(100, a));
  EXPECT_TRUE(fil().tablespace_deleted_or_being_deleted_in_mem(100, b)) << "a different version is a different space";
}

/** A second space with the id of an existing one is refused, the first one
is untouched. */
TEST_F(FilMemTest, DuplicateId) {
  add("a.ibd", 100, 10);

  EXPECT_FALSE(fil().space_create("b.ibd", 100, 0));
  EXPECT_EQ(fil().space_get_size(100), 10UL);
  EXPECT_TRUE(fil().validate());
}

/** A space with the name of an existing one replaces it (the old one is
assumed to be an orphan left by a crash). */
TEST_F(FilMemTest, DuplicateName) {
  add("a.ibd", 100, 10);

  ASSERT_TRUE(fil().space_create("a.ibd", 101, 0));
  fil().node_create("a.ibd", 20, 101, false);

  EXPECT_FALSE(fil().tablespace_exists_in_mem(100));
  EXPECT_TRUE(fil().tablespace_exists_in_mem(101));
  EXPECT_EQ(fil().space_get_size(101), 20UL);
}

/** The system tablespace is never replaced by name. */
TEST_F(FilMemTest, SysDuplicateName) {
  add("a.ibd", 5, 10);

  EXPECT_FALSE(fil().space_create("a.ibd", SYS_TABLESPACE, 0));
  EXPECT_TRUE(fil().tablespace_exists_in_mem(5));
  EXPECT_FALSE(fil().tablespace_exists_in_mem(SYS_TABLESPACE));
}

/** A node for a space that doesn't exist is dropped with an error. */
TEST_F(FilMemTest, NodeForMissingSpace) {
  fil().node_create("x.ibd", 10, 999, false);

  EXPECT_FALSE(fil().tablespace_exists_in_mem(999));
  EXPECT_TRUE(fil().validate());
}

/** space_for_table_exists_in_mem() matches both the id and the name, the
name of a table being the path of its .ibd file. */
TEST_F(FilMemTest, ForTable) {
  add(ibd("test/a"), 100, 4);
  add(ibd("test/b"), 101, 4);

  EXPECT_TRUE(fil().space_for_table_exists_in_mem(100, "test/a", false, false, false));
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(101, "test/b", false, true, false));
  /* The id of one, the name of the other. */
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(100, "test/b", false, false, false));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(100, "test/b", false, false, true));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(102, "test/a", false, false, true));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(100, "test/c", false, false, true));
}

/** Reservations add up to the number of free extents, not beyond, and are
given back. */
TEST_F(FilMemTest, ReserveExtents) {
  add("a.ibd", 100, 64);

  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 0UL);

  EXPECT_TRUE(fil().space_reserve_free_extents(100, 10, 4));
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 4UL);

  EXPECT_FALSE(fil().space_reserve_free_extents(100, 10, 7)) << "4 + 7 > 10";
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 4UL);

  EXPECT_TRUE(fil().space_reserve_free_extents(100, 10, 6)) << "4 + 6 == 10";
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 10UL);

  EXPECT_TRUE(fil().space_reserve_free_extents(100, 10, 0));
  EXPECT_FALSE(fil().space_reserve_free_extents(100, 10, 1));

  fil().space_release_free_extents(100, 6);
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 4UL);

  fil().space_release_free_extents(100, 4);
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 0UL);

  fil().space_release_free_extents(100, 0);
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 0UL);
}

/** No free extent: nothing can be reserved, except nothing. */
TEST_F(FilMemTest, ReserveNone) {
  add("a.ibd", 100, 64);

  EXPECT_FALSE(fil().space_reserve_free_extents(100, 0, 1));
  EXPECT_TRUE(fil().space_reserve_free_extents(100, 0, 0));
}

/** Reservations of different spaces are independent. */
TEST_F(FilMemTest, ReservePerSpace) {
  add("a.ibd", 100, 64);
  add("b.ibd", 101, 64);

  EXPECT_TRUE(fil().space_reserve_free_extents(100, 5, 5));
  EXPECT_TRUE(fil().space_reserve_free_extents(101, 5, 3));
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 5UL);
  EXPECT_EQ(fil().space_get_n_reserved_extents(101), 3UL);

  fil().space_release_free_extents(100, 5);
  fil().space_release_free_extents(101, 3);
}

/** Many threads reserve and release one extent at a time: the count never
exceeds the free extents and ends at zero. */
TEST_F(FilMemTest, ReserveConcurrent) {
  add("a.ibd", 100, 64);

  constexpr ulint N_FREE = 5;
  std::atomic<ulint> held{};
  std::atomic<ulint> max_held{};
  std::atomic<ulint> refused{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 2000; ++i) {
        if (fil().space_reserve_free_extents(100, N_FREE, 1)) {
          const auto n = ++held;

          for (auto m = max_held.load(); n > m && !max_held.compare_exchange_weak(m, n);) {
          }

          --held;
          fil().space_release_free_extents(100, 1);
        } else {
          ++refused;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_LE(max_held.load(), N_FREE);
  EXPECT_EQ(fil().space_get_n_reserved_extents(100), 0UL);
}

/** File address and page header helpers. */
TEST_F(FilMemTest, PageHelpers) {
  EXPECT_TRUE(fil().addr_is_null(FilAddr{FIL_NULL, 0}));
  EXPECT_TRUE(fil().addr_is_null(FilAddr{FIL_NULL, 100}));
  EXPECT_FALSE(fil().addr_is_null(FilAddr{0, 0}));
  EXPECT_FALSE(fil().addr_is_null(FilAddr{FIL_NULL - 1, 0}));

  Page page;

  mach_write_to_4(page.m_ptr + FIL_PAGE_PREV, 41);
  mach_write_to_4(page.m_ptr + FIL_PAGE_NEXT, 43);
  EXPECT_EQ(fil().page_get_prev(page.m_ptr), 41UL);
  EXPECT_EQ(fil().page_get_next(page.m_ptr), 43UL);

  for (auto type : {FIL_PAGE_TYPE_ALLOCATED, FIL_PAGE_TYPE_INDEX, FIL_PAGE_TYPE_UNDO_LOG, FIL_PAGE_TYPE_INODE, FIL_PAGE_TYPE_SYS,
                    FIL_PAGE_TYPE_TRX_SYS, FIL_PAGE_TYPE_FSP_HDR, FIL_PAGE_TYPE_XDES, FIL_PAGE_TYPE_BLOB}) {
    Fil::page_set_type(page.m_ptr, type);
    EXPECT_EQ(Fil::page_get_type(page.m_ptr), type);
  }

  /* The type is two bytes at FIL_PAGE_TYPE, nothing else changes. */
  EXPECT_EQ(mach_read_from_2(page.m_ptr + FIL_PAGE_TYPE), ulint(FIL_PAGE_TYPE_BLOB));
  EXPECT_EQ(fil().page_get_prev(page.m_ptr), 41UL);
  EXPECT_EQ(fil().page_get_next(page.m_ptr), 43UL);
}

/** A System started once for all the tests of a suite (starting and
stopping one takes seconds), with a directory "test" for scratch tables. */
class SharedSys : public ::testing::Test {
 public:
  static void SetUpTestSuite() { start(nullptr); }

  /** Start the shared System, with the configuration changed by tweak. */
  static void start(void (*tweak)(Config &)) {
    s_dir = std::make_unique<ut::TestDir>();
    ut::fs::create_directories(s_dir->m_path / "test");

    auto config = ut::make_config(s_dir->m_path);

    if (tweak != nullptr) {
      tweak(config);
    }

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

  /** @return a table name unique to this test: "test/<test name><suffix>". */
  [[nodiscard]] static std::string name(const std::string &suffix = "") {
    return std::string("test/") + ::testing::UnitTest::GetInstance()->current_test_info()->name() + suffix;
  }

  /** @return the path of the .ibd file of the table name. */
  [[nodiscard]] static ut::fs::path ibd(const std::string &table) { return s_dir->m_path / (table + ".ibd"); }

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
};

/** Scratch tablespaces on a shared started System. */
class FilIoTest : public SharedSys {
 protected:
  /** Create a single-table tablespace for the table name, of size pages.
  @return its id */
  space_id_t create(const std::string &table, ulint size = FIL_IBD_FILE_INITIAL_SIZE, space_id_t id = SYS_TABLESPACE) {
    EXPECT_EQ(fil().create_new_single_table_tablespace(&id, table.c_str(), false, 0, size), DB_SUCCESS) << table;
    return id;
  }

  /** Write a page with the pattern seed with Sync_write. */
  DbErr write(space_id_t space, page_no_t page_no, uint32_t seed) {
    Page page;

    page.fill(seed);
    return fil().data_io(IoRequest::Sync_write, false, PageId(space, page_no), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr);
  }

  /** @return true if the page has the pattern seed, read with Sync_read. */
  bool check(space_id_t space, page_no_t page_no, uint32_t seed) {
    Page page;

    EXPECT_EQ(fil().data_io(IoRequest::Sync_read, false, PageId(space, page_no), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr), DB_SUCCESS);
    return page.is(seed);
  }

  /** Read page page_no of a file directly. */
  static void read_file(const ut::fs::path &path, page_no_t page_no, byte *buf) {
    const auto fd = ::open(path.c_str(), O_RDONLY);

    ASSERT_GE(fd, 0) << path;
    EXPECT_EQ(::pread(fd, buf, UNIV_PAGE_SIZE, off_t(page_no) * UNIV_PAGE_SIZE), ssize_t(UNIV_PAGE_SIZE));
    ::close(fd);
  }

  Fil &fil() { return *m_sys->m_fil; }
};

using FilTest = ut::DbTest;

/** The table has a tablespace of its own, in memory and on disk. */
TEST_F(FilTest, TableSpace) {
  const auto space = table_space(*m_sys);

  ASSERT_NE(space, NULL_SPACE_ID);
  EXPECT_NE(space, SYS_TABLESPACE);
  EXPECT_TRUE(m_sys->m_fil->tablespace_exists_in_mem(space));
  EXPECT_TRUE(m_sys->m_fil->tablespace_exists_in_mem(SYS_TABLESPACE));
  EXPECT_FALSE(m_sys->m_fil->tablespace_exists_in_mem(space + 1000));
  EXPECT_GT(m_sys->m_fil->space_get_size(space), 0UL);
  EXPECT_NE(m_sys->m_fil->space_get_flags(space), ULINT_UNDEFINED);
  EXPECT_TRUE(ut::fs::exists(m_dir.m_path / "test" / "t.ibd"));
  EXPECT_TRUE(m_sys->m_fil->space_for_table_exists_in_mem(space, ut::TABLE, false, false, false));
}

/** Dropping the table removes its space and its file. */
TEST_F(FilTest, Drop) {
  const auto space = table_space(*m_sys);
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
  commit(trx);

  EXPECT_FALSE(m_sys->m_fil->tablespace_exists_in_mem(space));
  EXPECT_FALSE(ut::fs::exists(m_dir.m_path / "test" / "t.ibd"));

  restart();
  EXPECT_FALSE(m_sys->m_fil->tablespace_exists_in_mem(space));
}

/** TRUNCATE gives the table a new space and deletes the old file. */
TEST_F(FilTest, Truncate) {
  load({{1, ut::Row{1, 1, ut::blob(1, 100000)}}});

  const auto before = table_space(*m_sys);
  uint64_t table_id{};

  ASSERT_EQ(api::table_truncate(*m_sys, ut::TABLE, &table_id), DB_SUCCESS);

  const auto after = table_space(*m_sys);

  EXPECT_NE(after, before);
  EXPECT_FALSE(m_sys->m_fil->tablespace_exists_in_mem(before));
  EXPECT_TRUE(m_sys->m_fil->tablespace_exists_in_mem(after));
  EXPECT_TRUE(ut::fs::exists(m_dir.m_path / "test" / "t.ibd"));

  expect_rows({});

  restart();
  EXPECT_EQ(table_space(*m_sys), after);
  load({{2, ut::Row{2, 2, "x"}}});
  expect_rows({{2, ut::Row{2, 2, "x"}}});
}

/** Space ids are not reused across restarts. */
TEST_F(FilTest, SpaceIds) {
  std::vector<space_id_t> ids{table_space(*m_sys)};

  for (int i = 0; i < 3; ++i) {
    uint64_t table_id{};

    ASSERT_EQ(api::table_truncate(*m_sys, ut::TABLE, &table_id), DB_SUCCESS);
    restart();

    const auto id = table_space(*m_sys);

    for (auto old : ids) {
      EXPECT_NE(id, old);
    }

    ids.push_back(id);
  }
}

/** A new single-table tablespace: a new id, a file of the requested size
whose first page has the space id, in the cache under the table name. */
TEST_F(FilIoTest, CreateSpace) {
  const auto id = create(name("x"));

  ASSERT_NE(id, SYS_TABLESPACE);
  EXPECT_TRUE(fil().tablespace_exists_in_mem(id));
  EXPECT_EQ(fil().space_get_size(id), ulint(FIL_IBD_FILE_INITIAL_SIZE));
  EXPECT_EQ(fil().space_get_flags(id), 0UL);
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(id, name("x").c_str(), false, false, false));
  EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE);

  Page page;

  read_file(ibd(name("x")), 0, page.m_ptr);
  EXPECT_EQ(m_sys->m_fsp->get_space_id(page.m_ptr), id);
  EXPECT_EQ(m_sys->m_fsp->get_flags(page.m_ptr), 0UL);
  EXPECT_FALSE(m_sys->m_buf_pool->is_corrupted(page.m_ptr));

  EXPECT_TRUE(fil().validate());
}

/** Consecutive creations get increasing ids. */
TEST_F(FilIoTest, CreateIdsIncrease) {
  std::vector<space_id_t> ids;

  for (int i = 0; i < 10; ++i) {
    ids.push_back(create(name(std::format("s{}", i))));
  }

  for (size_t i{1}; i < ids.size(); ++i) {
    EXPECT_GT(ids[i], ids[i - 1]);
  }

  for (auto id : ids) {
    EXPECT_TRUE(fil().tablespace_exists_in_mem(id));
  }
}

/** A tablespace with a given id gets it, and the next assigned id is above it. */
TEST_F(FilIoTest, CreateWithId) {
  const auto id = create(name("x"), FIL_IBD_FILE_INITIAL_SIZE, 5000);

  EXPECT_EQ(id, 5000U);
  EXPECT_TRUE(fil().tablespace_exists_in_mem(5000));
  EXPECT_GT(create(name("y")), 5000U);
}

/** A bigger initial size. */
TEST_F(FilIoTest, CreateBigger) {
  const auto id = create(name("x"), 100);

  EXPECT_EQ(fil().space_get_size(id), 100UL);
  EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), 100 * UNIV_PAGE_SIZE);
}

/** The file exists already: refused, the existing space is untouched. */
TEST_F(FilIoTest, CreateExists) {
  const auto id = create(name("x"));
  space_id_t id2{SYS_TABLESPACE};

  EXPECT_EQ(fil().create_new_single_table_tablespace(&id2, name("x").c_str(), false, 0, FIL_IBD_FILE_INITIAL_SIZE), DB_TABLESPACE_ALREADY_EXISTS);
  EXPECT_TRUE(fil().tablespace_exists_in_mem(id));
  EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE);
}

/** set_max_space_id_if_bigger() raises the counter, never lowers it. */
TEST_F(FilIoTest, MaxSpaceId) {
  fil().set_max_space_id_if_bigger(10000);
  EXPECT_EQ(create(name("x")), 10001U);

  fil().set_max_space_id_if_bigger(50);
  EXPECT_EQ(create(name("y")), 10002U);
}

/** Synchronous page i/o round trips, at the first and last page. */
TEST_F(FilIoTest, DataIo) {
  const auto id = create(name("x"), 10);

  for (page_no_t p : {0U, 1U, 5U, 9U}) {
    ASSERT_EQ(write(id, p, p + 1), DB_SUCCESS);
  }

  for (page_no_t p : {0U, 1U, 5U, 9U}) {
    EXPECT_TRUE(check(id, p, p + 1)) << p;
  }

  Page page;

  read_file(ibd(name("x")), 9, page.m_ptr);
  EXPECT_TRUE(page.is(10)) << "on disk";
}

/** i/o of part of a page, at an offset in the page, and of several pages. */
TEST_F(FilIoTest, DataIoPartial) {
  const auto id = create(name("x"), 10);

  ASSERT_EQ(write(id, 3, 7), DB_SUCCESS);

  auto buf = static_cast<byte *>(std::aligned_alloc(UNIV_PAGE_SIZE, 3 * UNIV_PAGE_SIZE));

  memset(buf, 0xaa, 1024);
  ASSERT_EQ(fil().data_io(IoRequest::Sync_write, false, PageId(id, 3), 512, 1024, buf, nullptr), DB_SUCCESS);

  Page page;

  ASSERT_EQ(fil().data_io(IoRequest::Sync_read, false, PageId(id, 3), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr), DB_SUCCESS);

  Page expected;

  expected.fill(7);
  memset(expected.m_ptr + 512, 0xaa, 1024);
  EXPECT_EQ(memcmp(page.m_ptr, expected.m_ptr, UNIV_PAGE_SIZE), 0);

  /* Three pages in one i/o. */
  ASSERT_EQ(write(id, 4, 8), DB_SUCCESS);
  ASSERT_EQ(write(id, 5, 9), DB_SUCCESS);
  ASSERT_EQ(fil().data_io(IoRequest::Sync_read, false, PageId(id, 3), 0, 3 * UNIV_PAGE_SIZE, buf, nullptr), DB_SUCCESS);
  EXPECT_EQ(memcmp(buf, expected.m_ptr, UNIV_PAGE_SIZE), 0);

  expected.fill(8);
  EXPECT_EQ(memcmp(buf + UNIV_PAGE_SIZE, expected.m_ptr, UNIV_PAGE_SIZE), 0);
  expected.fill(9);
  EXPECT_EQ(memcmp(buf + 2 * UNIV_PAGE_SIZE, expected.m_ptr, UNIV_PAGE_SIZE), 0);

  std::free(buf);
}

/** i/o on a space that doesn't exist is refused. */
TEST_F(FilIoTest, DataIoMissing) {
  Page page;

  EXPECT_EQ(fil().data_io(IoRequest::Sync_read, false, PageId(123456, 0), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr), DB_TABLESPACE_DELETED);
  EXPECT_EQ(fil().data_io(IoRequest::Sync_write, false, PageId(123456, 0), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr), DB_TABLESPACE_DELETED);
  EXPECT_TRUE(fil().validate());
}

/** The server counts the bytes read and written. */
TEST_F(FilIoTest, DataIoStats) {
  const auto id = create(name("x"), 10);
  auto &stats = m_sys->m_srv->m_stats;

  const auto read = stats.m_data_read;
  const auto written = stats.m_data_written;

  ASSERT_EQ(write(id, 1, 1), DB_SUCCESS);
  EXPECT_TRUE(check(id, 1, 1));

  EXPECT_GE(stats.m_data_written - written, UNIV_PAGE_SIZE);
  EXPECT_GE(stats.m_data_read - read, UNIV_PAGE_SIZE);
}

/** Extending: by one page, to an extent boundary, by many chunks of 64
pages; a smaller or equal size does nothing. The new pages are zero. */
TEST_F(FilIoTest, ExtendSpace) {
  const auto id = create(name("x"));
  page_no_t actual{};

  for (page_no_t size : {5U, 64U, 65U, 200U, 1000U}) {
    ASSERT_TRUE(fil().extend_space_to_desired_size(&actual, id, size)) << size;
    EXPECT_EQ(actual, size);
    EXPECT_EQ(fil().space_get_size(id), ulint(size));
    EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), size * UNIV_PAGE_SIZE);
  }

  for (page_no_t size : {1000U, 999U, 4U, 0U}) {
    ASSERT_TRUE(fil().extend_space_to_desired_size(&actual, id, size)) << size;
    EXPECT_EQ(actual, 1000U) << size;
  }

  EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), 1000 * UNIV_PAGE_SIZE);

  Page page;
  Page zero;

  for (page_no_t p : {4U, 64U, 999U}) {
    read_file(ibd(name("x")), p, page.m_ptr);
    EXPECT_EQ(memcmp(page.m_ptr, zero.m_ptr, UNIV_PAGE_SIZE), 0) << p;
  }

  /* The pages after the old end can be written. */
  ASSERT_EQ(write(id, 999, 3), DB_SUCCESS);
  EXPECT_TRUE(check(id, 999, 3));
  EXPECT_TRUE(fil().validate());
}

/** Extending does not touch the existing pages. */
TEST_F(FilIoTest, ExtendKeepsData) {
  const auto id = create(name("x"));
  page_no_t actual{};

  for (page_no_t p = 0; p < FIL_IBD_FILE_INITIAL_SIZE; ++p) {
    ASSERT_EQ(write(id, p, p + 100), DB_SUCCESS);
  }

  ASSERT_TRUE(fil().extend_space_to_desired_size(&actual, id, 300));

  for (page_no_t p = 0; p < FIL_IBD_FILE_INITIAL_SIZE; ++p) {
    EXPECT_TRUE(check(id, p, p + 100)) << p;
  }
}

/** An extension is flushed to disk. */
TEST_F(FilIoTest, ExtendFlushes) {
  const auto id = create(name("x"));
  const auto flushes = fil().get_flushes();
  page_no_t actual{};

  ASSERT_TRUE(fil().extend_space_to_desired_size(&actual, id, 128));
  EXPECT_GT(fil().get_flushes(), flushes);
  EXPECT_EQ(fil().get_pending_flushes(), 0UL);
}

/** Threads extend one space to different sizes at once: it ends at the
largest, and the file matches. */
TEST_F(FilIoTest, ExtendConcurrent) {
  const auto id = create(name("x"));
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (page_no_t i = 1; i <= 20; ++i) {
        page_no_t actual{};
        const page_no_t size = 4 + ((i * 37 + t * 11) % 20) * 50;

        EXPECT_TRUE(fil().extend_space_to_desired_size(&actual, id, size));
        EXPECT_GE(actual, size);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  page_no_t max{};

  for (int t = 0; t < 8; ++t) {
    for (page_no_t i = 1; i <= 20; ++i) {
      max = std::max<page_no_t>(max, 4 + ((i * 37 + t * 11) % 20) * 50);
    }
  }

  EXPECT_EQ(fil().space_get_size(id), ulint(max));
  EXPECT_EQ(ut::fs::file_size(ibd(name("x"))), max * UNIV_PAGE_SIZE);
  EXPECT_TRUE(fil().validate());
}

/** Threads extend and write their own spaces at once. */
TEST_F(FilIoTest, ExtendManySpaces) {
  std::vector<space_id_t> ids;

  for (int t = 0; t < 8; ++t) {
    ids.push_back(create(name(std::format("x{}", t))));
  }

  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (page_no_t size = 64; size <= 640; size += 64) {
        page_no_t actual{};

        EXPECT_TRUE(fil().extend_space_to_desired_size(&actual, ids[t], size));
        EXPECT_EQ(write(ids[t], size - 1, size + t), DB_SUCCESS);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  for (int t = 0; t < 8; ++t) {
    EXPECT_EQ(fil().space_get_size(ids[t]), 640UL);

    for (page_no_t size = 64; size <= 640; size += 64) {
      EXPECT_TRUE(check(ids[t], size - 1, size + t));
    }
  }
}

/** A write makes a space unflushed: flush() syncs it; a missing space is
ignored. (The fsync counter is global and the background threads flush
too: only lower bounds can be checked.) */
TEST_F(FilIoTest, Flush) {
  const auto id = create(name("x"));

  fil().flush(id);

  const auto before = fil().get_flushes();

  ASSERT_EQ(write(id, 1, 1), DB_SUCCESS);
  fil().flush(id);
  EXPECT_GE(fil().get_flushes(), before + 1);

  fil().flush(id);
  fil().flush(987654);
  EXPECT_TRUE(check(id, 1, 1));
}

/** flush_file_spaces() flushes every space written to. */
TEST_F(FilIoTest, FlushAll) {
  const auto a = create(name("a"));
  const auto b = create(name("b"));

  fil().flush_file_spaces();

  const auto before = fil().get_flushes();

  ASSERT_EQ(write(a, 1, 1), DB_SUCCESS);
  ASSERT_EQ(write(b, 1, 1), DB_SUCCESS);

  fil().flush_file_spaces();
  EXPECT_GE(fil().get_flushes(), before + 2);
}

/** Rename: the file moves, the cache follows, the data is intact. */
TEST_F(FilIoTest, Rename) {
  const auto id = create(name("x"));

  ASSERT_EQ(write(id, 2, 5), DB_SUCCESS);
  ASSERT_TRUE(fil().rename_tablespace(name("x").c_str(), id, name("y").c_str()));

  EXPECT_FALSE(ut::fs::exists(ibd(name("x"))));
  EXPECT_TRUE(ut::fs::exists(ibd(name("y"))));
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(id, name("y").c_str(), false, false, false));
  EXPECT_FALSE(fil().space_for_table_exists_in_mem(id, name("x").c_str(), false, false, false));
  EXPECT_TRUE(check(id, 2, 5));

  /* And back, without giving the old name. */
  ASSERT_TRUE(fil().rename_tablespace(nullptr, id, name("x").c_str()));
  EXPECT_TRUE(ut::fs::exists(ibd(name("x"))));
  EXPECT_TRUE(check(id, 2, 5));
  EXPECT_TRUE(fil().validate());
}

/** Rename to the name of another space: refused, nothing changes. */
TEST_F(FilIoTest, RenameToExisting) {
  const auto x = create(name("x"));
  const auto y = create(name("y"));

  EXPECT_FALSE(fil().rename_tablespace(name("x").c_str(), x, name("y").c_str()));
  EXPECT_TRUE(ut::fs::exists(ibd(name("x"))));
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(x, name("x").c_str(), false, false, false));
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(y, name("y").c_str(), false, false, false));

  ASSERT_EQ(write(x, 1, 9), DB_SUCCESS);
  EXPECT_TRUE(check(x, 1, 9));
}

/** Rename of a space that doesn't exist. */
TEST_F(FilIoTest, RenameMissing) {
  EXPECT_FALSE(fil().rename_tablespace(name("nope").c_str(), 424242, name("other").c_str()));
}

/** Delete: gone from the cache and the disk; a second delete fails. */
TEST_F(FilIoTest, Delete) {
  const auto id = create(name("x"));

  ASSERT_EQ(write(id, 1, 1), DB_SUCCESS);
  ASSERT_TRUE(fil().delete_tablespace(id));

  EXPECT_FALSE(fil().tablespace_exists_in_mem(id));
  EXPECT_FALSE(ut::fs::exists(ibd(name("x"))));
  EXPECT_EQ(fil().space_get_version(id), -1);
  EXPECT_TRUE(fil().tablespace_deleted_or_being_deleted_in_mem(id, -1));

  EXPECT_FALSE(fil().delete_tablespace(id));
  EXPECT_FALSE(fil().discard_tablespace(id));
  EXPECT_TRUE(fil().validate());
}

/** The name of a deleted tablespace can be used again, with a new id. */
TEST_F(FilIoTest, DeleteAndCreate) {
  const auto a = create(name("x"));

  ASSERT_TRUE(fil().delete_tablespace(a));

  const auto b = create(name("x"));

  EXPECT_GT(b, a);
  EXPECT_TRUE(fil().space_for_table_exists_in_mem(b, name("x").c_str(), false, false, false));
}

/** Discarding is deleting. */
TEST_F(FilIoTest, Discard) {
  const auto id = create(name("x"));

  ASSERT_TRUE(fil().discard_tablespace(id));
  EXPECT_FALSE(fil().tablespace_exists_in_mem(id));
  EXPECT_FALSE(ut::fs::exists(ibd(name("x"))));
}

/** Many spaces created and deleted. */
TEST_F(FilIoTest, ManySpaces) {
  std::set<space_id_t> ids;

  for (int i = 0; i < 30; ++i) {
    ids.insert(create(name(std::format("m{}", i))));
  }

  EXPECT_EQ(ids.size(), 30UL);

  for (auto id : ids) {
    ASSERT_EQ(write(id, 3, id), DB_SUCCESS);
  }

  for (auto id : ids) {
    EXPECT_TRUE(check(id, 3, id));
    EXPECT_TRUE(fil().delete_tablespace(id));
  }

  for (int i = 0; i < 30; ++i) {
    EXPECT_FALSE(ut::fs::exists(ibd(name(std::format("m{}", i)))));
  }

  EXPECT_TRUE(fil().validate());
}

/** The flushed lsn written to the system tablespace is read back. */
TEST_F(FilIoTest, FlushedLsn) {
  const auto lsn = m_sys->m_log->get_lsn();

  ASSERT_EQ(fil().write_flushed_lsn_to_data_files(lsn), DB_SUCCESS);

  const auto fd = ::open((s_dir->m_path / "system.ibd").c_str(), O_RDONLY);

  ASSERT_GE(fd, 0);

  lsn_t read{};

  fil().read_flushed_lsn(fd, read);
  ::close(fd);

  EXPECT_EQ(read, lsn);
}

/** More spaces with i/o than the limit of open files: the i/o still works. */
class FilOpenLimitTest : public FilIoTest {
 public:
  static void SetUpTestSuite() {
    start([](Config &config) { config.m_fil.m_max_n_open_files = 4; });
  }
};

TEST_F(FilOpenLimitTest, Io) {
  std::vector<space_id_t> ids;

  for (int i = 0; i < 8; ++i) {
    ids.push_back(create(name(std::format("o{}", i))));
  }

  for (auto id : ids) {
    ASSERT_EQ(write(id, 1, id), DB_SUCCESS);
  }

  for (auto id : ids) {
    EXPECT_TRUE(check(id, 1, id));
  }

  EXPECT_TRUE(fil().validate());
}

}  // namespace

namespace {

using FilPerf = FilIoTest;

/** Synchronous 16K page writes and reads through Fil::data_io(). */
TEST_F(FilPerf, DataIo) {
  const auto id = create(name("p"), 1024);
  Page page;

  page.fill(1);

  auto w = ut::bench("fil_sync_write_16k", 20000, [&](uint64_t i) {
    (void)fil().data_io(IoRequest::Sync_write, false, PageId(id, page_no_t(i * 7919 % 1024)), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr);
  });

  auto r = ut::bench("fil_sync_read_16k", 50000, [&](uint64_t i) {
    (void)fil().data_io(IoRequest::Sync_read, false, PageId(id, page_no_t(i * 7919 % 1024)), 0, UNIV_PAGE_SIZE, page.m_ptr, nullptr);
  });

  EXPECT_LT(w.ns_per_op(), 1e6);
  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** Synchronous page reads from 8 threads at once. */
TEST_F(FilPerf, DataIoMt) {
  const auto id = create(name("p"), 1024);

  for (page_no_t p = 0; p < 1024; ++p) {
    ASSERT_EQ(write(id, p, p), DB_SUCCESS);
  }

  std::vector<std::unique_ptr<Page>> pages;

  for (int t = 0; t < 8; ++t) {
    pages.push_back(std::make_unique<Page>());
  }

  auto r = ut::bench_mt("fil_sync_read_16k_mt", 8, 20000, [&](int t, uint64_t i) {
    (void)fil().data_io(IoRequest::Sync_read, false, PageId(id, page_no_t((i * 7919 + t) % 1024)), 0, UNIV_PAGE_SIZE, pages[t]->m_ptr, nullptr);
  });

  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** Extending a space by one extent (64 pages, written and flushed). */
TEST_F(FilPerf, Extend) {
  const auto id = create(name("p"));

  auto r = ut::bench("fil_extend_1m", 500, [&](uint64_t i) {
    page_no_t actual{};

    (void)fil().extend_space_to_desired_size(&actual, id, page_no_t(4 + (i + 1) * 64));
  });

  EXPECT_LT(r.ns_per_op(), 100e6);
}

/** Lookups in the memory cache. */
TEST_F(FilPerf, Lookup) {
  const auto id = create(name("p"));

  auto r = ut::bench("fil_space_get_size", 1000000, [&](uint64_t) { ut::keep(fil().space_get_size(id)); });

  EXPECT_LT(r.ns_per_op(), 20000.0);

  auto mt = ut::bench_mt("fil_exists_in_mem_mt", 8, 200000, [&](int, uint64_t) { ut::keep(fil().tablespace_exists_in_mem(id)); });

  EXPECT_LT(mt.ns_per_op(), 50000.0);
}

/** Reserve and release of an extent. */
TEST_F(FilPerf, Reserve) {
  const auto id = create(name("p"));

  auto r = ut::bench("fil_reserve_release", 1000000, [&](uint64_t) {
    (void)fil().space_reserve_free_extents(id, 10, 1);
    fil().space_release_free_extents(id, 1);
  });

  EXPECT_LT(r.ns_per_op(), 20000.0);
}

/** Create and delete of a single-table tablespace. */
TEST_F(FilPerf, CreateDelete) {
  auto r = ut::bench("fil_create_delete", 200, [&](uint64_t i) {
    const auto id = create(name(std::format("c{}", i)));

    (void)fil().delete_tablespace(id);
  });

  EXPECT_LT(r.ns_per_op(), 200e6);
}

}  // namespace
