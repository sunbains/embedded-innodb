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

/** @file unit-tests/test_fsp.cc
Fsp: space management in the system and the single-table tablespaces.

Most tests work on a scratch single-table tablespace, created the way the
dictionary creates one (Fil::create_new_single_table_tablespace() then
Fsp::header_init()), and drive the public file segment API on it: a segment
is created with fseg_create(), pages are allocated and freed with
fseg_alloc_free_page() and fseg_free_page(), the segment is dropped with
fseg_free_step(). The space header (page 0) is read directly to check the
extent lists: FSP_FREE (free extents), FSP_FREE_FRAG (fragment extents with
free pages), FSP_FULL_FRAG (full fragment extents). Fsp::validate() checks the
whole space after each test. */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "mach0data.h"
#include "mtr0mtr.h"

#include <cstdlib>
#include <format>
#include <set>
#include <thread>
#include <vector>

namespace {

/* The layout of the space header and of a segment inode, from fsp0fsp.cc. */
constexpr ulint HDR = FIL_PAGE_DATA;
constexpr ulint HDR_SIZE = 8;
constexpr ulint HDR_FREE_LIMIT = 12;
constexpr ulint HDR_FRAG_N_USED = 20;
constexpr ulint HDR_FREE = 24;
constexpr ulint HDR_FREE_FRAG = 24 + FLST_BASE_NODE_SIZE;
constexpr ulint HDR_FULL_FRAG = 24 + 2 * FLST_BASE_NODE_SIZE;
constexpr ulint HDR_SEG_ID = 24 + 3 * FLST_BASE_NODE_SIZE;
constexpr ulint HDR_INODES_FULL = 32 + 3 * FLST_BASE_NODE_SIZE;
constexpr ulint HDR_INODES_FREE = 32 + 4 * FLST_BASE_NODE_SIZE;

/** Segment inodes on an inode page. */
constexpr ulint INODE_SIZE = 16 + 3 * FLST_BASE_NODE_SIZE + (FSP_EXTENT_SIZE / 2) * 4;
constexpr ulint INODES_PER_PAGE = (UNIV_PAGE_SIZE - (FSEG_PAGE_DATA + FLST_NODE_SIZE) - 10) / INODE_SIZE;

/** Number of fragment pages a segment holds before it gets extents. */
constexpr ulint FRAG_LIMIT = FSP_EXTENT_SIZE / 2;

space_id_t table_space(System &sys) {
  sys.m_dict->mutex_acquire();
  auto table = sys.m_dict->table_get(ut::TABLE);
  sys.m_dict->mutex_release();

  return table->m_space_id;
}

/** The fields of a space header. */
struct Hdr {
  ulint m_size{};
  ulint m_free_limit{};
  ulint m_frag_n_used{};
  ulint m_n_free{};
  ulint m_n_free_frag{};
  ulint m_n_full_frag{};
  uint64_t m_seg_id{};
  ulint m_n_inodes_full{};
  ulint m_n_inodes_free{};
};

/** Where a segment header is: page and offset. */
struct Seg {
  space_id_t m_space{};
  page_no_t m_page_no{FIL_NULL};
  ulint m_offset{FSEG_PAGE_DATA};
};

/** Helpers on scratch tablespaces and segments of a started System m_s. */
class FspOps {
 protected:
  /** Create a single-table tablespace with its space header, like the
  dictionary does for a table. */
  space_id_t new_space(std::string name = "") {
    if (name.empty()) {
      name = std::string("test/") + ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    space_id_t id{SYS_TABLESPACE};

    EXPECT_EQ(m_s->m_fil->create_new_single_table_tablespace(&id, name.c_str(), false, 0, FIL_IBD_FILE_INITIAL_SIZE), DB_SUCCESS);

    Mtr mtr;

    mtr.start();
    m_s->m_fsp->header_init(id, FIL_IBD_FILE_INITIAL_SIZE, &mtr);
    mtr.commit(*m_s);

    return id;
  }

  /** Get a page X-latched in mtr. */
  BufBlock *get(space_id_t space, page_no_t page_no, Mtr &mtr, ulint latch = RW_X_LATCH) {
    BufPool::Request req{
      .m_rw_latch = latch, .m_page_id = {space, page_no}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
    };

    return m_s->m_buf_pool->get(req, nullptr);
  }

  /** @return the space header fields. */
  Hdr hdr(space_id_t space) {
    Mtr mtr;

    mtr.start();
    mtr_x_lock(m_s->m_fil->space_get_latch(space), &mtr);

    const auto h = get(space, 0, mtr)->get_frame() + HDR;

    Hdr r{
      .m_size = mach_read_from_4(h + HDR_SIZE),
      .m_free_limit = mach_read_from_4(h + HDR_FREE_LIMIT),
      .m_frag_n_used = mach_read_from_4(h + HDR_FRAG_N_USED),
      .m_n_free = flst_get_len(h + HDR_FREE, &mtr),
      .m_n_free_frag = flst_get_len(h + HDR_FREE_FRAG, &mtr),
      .m_n_full_frag = flst_get_len(h + HDR_FULL_FRAG, &mtr),
      .m_seg_id = mach_read_from_8(h + HDR_SEG_ID),
      .m_n_inodes_full = flst_get_len(h + HDR_INODES_FULL, &mtr),
      .m_n_inodes_free = flst_get_len(h + HDR_INODES_FREE, &mtr),
    };

    mtr.commit(*m_s);

    return r;
  }

  /** Create a segment whose header is on a page of its own.
  @return the segment, m_page_no is FIL_NULL if there was no space */
  Seg create_seg(space_id_t space) {
    Mtr mtr;
    Seg seg{.m_space = space};

    mtr.start();

    auto block = m_s->m_fsp->fseg_create(space, 0, seg.m_offset, &mtr);

    if (block != nullptr) {
      seg.m_page_no = block->get_page_no();
    }

    mtr.commit(*m_s);

    return seg;
  }

  /** @return the segment header, its page X-latched in mtr. */
  fseg_header_t *header(const Seg &seg, Mtr &mtr) { return get(seg.m_space, seg.m_page_no, mtr)->get_frame() + seg.m_offset; }

  /** Allocate a page in seg. @return the page number or FIL_NULL */
  page_no_t alloc(const Seg &seg, page_no_t hint = 0, byte dir = FSP_NO_DIR) {
    Mtr mtr;

    mtr.start();

    const auto page_no = m_s->m_fsp->fseg_alloc_free_page(header(seg, mtr), hint, dir, &mtr);

    mtr.commit(*m_s);

    return page_no;
  }

  /** Free page_no of seg. */
  void free(const Seg &seg, page_no_t page_no) {
    Mtr mtr;

    mtr.start();
    m_s->m_fsp->fseg_free_page(header(seg, mtr), seg.m_space, page_no, &mtr);
    mtr.commit(*m_s);
  }

  /** @return {reserved, used} pages of seg. */
  std::pair<ulint, ulint> reserved(const Seg &seg) {
    Mtr mtr;
    ulint used{};

    mtr.start();

    const auto n = m_s->m_fsp->fseg_n_reserved_pages(header(seg, mtr), &used, &mtr);

    mtr.commit(*m_s);

    return {n, used};
  }

  /** Free seg completely, step by step. @return the number of steps */
  ulint drop(const Seg &seg) {
    for (ulint steps{1};; ++steps) {
      Mtr mtr;

      mtr.start();

      const auto done = m_s->m_fsp->fseg_free_step(header(seg, mtr), &mtr);

      mtr.commit(*m_s);

      if (done) {
        return steps;
      }

      EXPECT_LT(steps, 10000UL);
    }
  }

  /** @return true if the segment is valid (asserts otherwise). */
  bool seg_valid(const Seg &seg) {
#ifdef UNIV_DEBUG
    Mtr mtr;

    mtr.start();

    const auto ok = m_s->m_fsp->fseg_validate(header(seg, mtr), &mtr);

    mtr.commit(*m_s);

    return ok;
#else
    /* fseg_validate() is debug only. */
    (void)seg;
    return true;
#endif /* UNIV_DEBUG */
  }

  Fsp &fsp() { return *m_s->m_fsp; }

  System *m_s{};
};

/** A System started once for all the tests of a suite (starting and
stopping one takes seconds), with a directory "test" for scratch tables. */
class SharedSys : public ::testing::Test {
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

  static inline std::unique_ptr<ut::TestDir> s_dir;
  static inline std::unique_ptr<System> s_sys;

  System *m_sys{};
};

/** Segments on scratch tablespaces of a shared System. */
class FspSegTest : public SharedSys, public FspOps {
 protected:
  void SetUp() override {
    SharedSys::SetUp();
    m_s = m_sys;
  }
};

/** A System of its own, for the tests that restart it. */
class FspTest : public ut::DbTest, public FspOps {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();
    m_s = m_sys.get();
  }

  void restart() {
    ut::DbTest::restart();
    m_s = m_sys.get();
  }
};

/** The free lists of the spaces are consistent at start. */
TEST_F(FspTest, ValidateAtStart) {
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
  EXPECT_TRUE(m_sys->m_fsp->validate(table_space(*m_sys)));
}

/** A space grows as rows come in, its free lists stay consistent as the
rows and their blob pages come and go. */
TEST_F(FspTest, GrowAndFree) {
  const auto space = table_space(*m_sys);
  const auto initial = m_sys->m_fil->space_get_size(space);

  ut::Rows rows;

  for (uint32_t k = 0; k < 300; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 40000)};
  }

  load(rows);

  EXPECT_GT(m_sys->m_fil->space_get_size(space), initial);
  EXPECT_TRUE(m_sys->m_fsp->validate(space));

  auto trx = begin();

  for (uint32_t k = 0; k < 300; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, k, "small"}), DB_SUCCESS);
    rows[k] = ut::Row{k, k, "small"};
  }

  commit(trx);

  EXPECT_TRUE(m_sys->m_fsp->validate(space));
  expect_rows(rows);

  restart();
  EXPECT_TRUE(m_sys->m_fsp->validate(space));
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
}

/** The undo logs of big transactions come from the system tablespace,
which stays consistent after their rollback. */
TEST_F(FspTest, UndoSpace) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 2000; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 500)};
  }

  load(rows);

  auto trx = begin();

  for (uint32_t k = 0; k < 2000; ++k) {
    ASSERT_EQ(update(trx, k, ut::Row{k, k + 1, ut::blob(k + 1, 500)}), DB_SUCCESS);
  }

  rollback(trx);

  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
  expect_rows(rows);
}

/** The header of a new single-table tablespace: its size, the first extent
initialized as a fragment extent with the descriptor page used, no segment. */
TEST_F(FspSegTest, HeaderInit) {
  const auto space = new_space();
  const auto h = hdr(space);

  EXPECT_EQ(h.m_size, ulint(FIL_IBD_FILE_INITIAL_SIZE));
  EXPECT_EQ(h.m_free_limit, FSP_EXTENT_SIZE);
  EXPECT_EQ(h.m_frag_n_used, 1UL) << "the descriptor page";
  EXPECT_EQ(h.m_n_free, 0UL);
  EXPECT_EQ(h.m_n_free_frag, 1UL);
  EXPECT_EQ(h.m_n_full_frag, 0UL);
  EXPECT_EQ(h.m_seg_id, 1UL);
  EXPECT_EQ(h.m_n_inodes_full, 0UL);
  EXPECT_EQ(h.m_n_inodes_free, 0UL);

  EXPECT_TRUE(fsp().validate(space));

  Mtr mtr;

  mtr.start();

  auto page = get(space, 0, mtr, RW_S_LATCH)->get_frame();

  EXPECT_EQ(fsp().get_space_id(page), space);
  EXPECT_EQ(fsp().get_flags(page), 0UL);
  EXPECT_EQ(fsp().get_size_low(page), ulint(FIL_IBD_FILE_INITIAL_SIZE));
  EXPECT_EQ(Fil::page_get_type(page), FIL_PAGE_TYPE_FSP_HDR);

  mtr.commit(*m_sys);
}

/** The header of the system tablespace after creation. */
TEST_F(FspSegTest, SysHeader) {
  const auto h = hdr(SYS_TABLESPACE);

  EXPECT_EQ(h.m_size, fsp().get_system_space_size());
  EXPECT_LE(h.m_size, m_sys->m_fil->space_get_size(SYS_TABLESPACE));
  EXPECT_LE(h.m_free_limit, h.m_size);
  EXPECT_EQ(h.m_free_limit % FSP_EXTENT_SIZE, 0UL);
  EXPECT_EQ(fsp().init_system_space_free_limit(), h.m_free_limit / FSP_EXTENT_SIZE);
  EXPECT_GT(h.m_seg_id, 1UL) << "the system segments";
  EXPECT_GT(h.m_n_inodes_full + h.m_n_inodes_free, 0UL);
}

/** init_fields() writes the id and flags read back by get_space_id() and
get_flags(); an id that differs from the page header's is undefined. */
TEST_F(FspSegTest, Fields) {
  auto page = static_cast<page_t *>(std::aligned_alloc(UNIV_PAGE_SIZE, UNIV_PAGE_SIZE));

  memset(page, 0, UNIV_PAGE_SIZE);
  fsp().init_fields(page, 77, 0);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, 77);

  EXPECT_EQ(fsp().get_space_id(page), 77U);
  EXPECT_EQ(fsp().get_flags(page), 0UL);

  mach_write_to_4(page + FIL_PAGE_SPACE_ID, 78);
  EXPECT_EQ(fsp().get_space_id(page), ULINT32_UNDEFINED);

  mach_write_to_4(page + HDR + HDR_SIZE, 1234);
  EXPECT_EQ(fsp().get_size_low(page), 1234UL);

  std::free(page);
}

/** Descriptor pages are the first page of every UNIV_PAGE_SIZE pages. */
TEST_F(FspSegTest, DescrPage) {
  EXPECT_TRUE(fsp().descr_page(0));
  EXPECT_FALSE(fsp().descr_page(1));
  EXPECT_FALSE(fsp().descr_page(FSP_EXTENT_SIZE));
  EXPECT_FALSE(fsp().descr_page(UNIV_PAGE_SIZE - 1));
  EXPECT_TRUE(fsp().descr_page(UNIV_PAGE_SIZE));
  EXPECT_FALSE(fsp().descr_page(UNIV_PAGE_SIZE + 1));
  EXPECT_TRUE(fsp().descr_page(5 * UNIV_PAGE_SIZE));
}

/** A segment: an inode page and its header page are allocated as fragment
pages, it owns its header page, the next segment id is used. */
TEST_F(FspSegTest, SegCreate) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  ASSERT_NE(seg.m_page_no, FIL_NULL);
  EXPECT_GT(seg.m_page_no, 0U);
  EXPECT_LT(seg.m_page_no, FSP_EXTENT_SIZE);

  const auto h = hdr(space);

  EXPECT_EQ(h.m_seg_id, 2UL);
  EXPECT_EQ(h.m_frag_n_used, 3UL) << "descriptor, inode and header pages";
  EXPECT_EQ(h.m_n_inodes_free, 1UL);
  EXPECT_EQ(h.m_n_inodes_full, 0UL);

  EXPECT_EQ(reserved(seg), std::make_pair(1UL, 1UL));
  EXPECT_TRUE(seg_valid(seg));
  EXPECT_TRUE(fsp().validate(space));

  /* The header points to an inode in the same space. */
  Mtr mtr;

  mtr.start();

  auto header_ptr = header(seg, mtr);

  EXPECT_EQ(mach_read_from_4(header_ptr + FSEG_HDR_SPACE), space);
  EXPECT_EQ(mach_read_from_4(header_ptr + FSEG_HDR_PAGE_NO), ulint(FSP_FIRST_INODE_PAGE_NO));
  EXPECT_EQ(Fil::page_get_type(Rec(header_ptr).page_align()), FIL_PAGE_TYPE_SYS);

  mtr.commit(*m_sys);
}

/** A segment header on an existing page of another segment. */
TEST_F(FspSegTest, SegCreateOnPage) {
  const auto space = new_space();
  const auto a = create_seg(space);

  Mtr mtr;

  mtr.start();

  auto block = fsp().fseg_create(space, a.m_page_no, FSEG_PAGE_DATA + FSEG_HEADER_SIZE, &mtr);

  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->get_page_no(), a.m_page_no);

  mtr.commit(*m_sys);

  const Seg b{.m_space = space, .m_page_no = a.m_page_no, .m_offset = FSEG_PAGE_DATA + FSEG_HEADER_SIZE};

  EXPECT_EQ(reserved(b), std::make_pair(0UL, 0UL)) << "owns no page yet";

  const auto p = alloc(b);

  ASSERT_NE(p, FIL_NULL);
  EXPECT_EQ(reserved(b), std::make_pair(1UL, 1UL));
  EXPECT_EQ(reserved(a), std::make_pair(1UL, 1UL));
  EXPECT_TRUE(fsp().validate(space));

  drop(b);
  EXPECT_TRUE(fsp().validate(space));
  EXPECT_EQ(reserved(a), std::make_pair(1UL, 1UL));
}

/** The first FRAG_LIMIT pages of a segment are fragment pages from the
space's fragment extents; the next one brings a whole extent. */
TEST_F(FspSegTest, FragThenExtent) {
  const auto space = new_space();
  const auto seg = create_seg(space);
  std::set<page_no_t> pages{seg.m_page_no};

  for (ulint i = 1; i < FRAG_LIMIT; ++i) {
    const auto p = alloc(seg);

    ASSERT_NE(p, FIL_NULL) << i;
    EXPECT_TRUE(pages.insert(p).second) << "allocated twice: " << p;
  }

  EXPECT_EQ(reserved(seg), std::make_pair(FRAG_LIMIT, FRAG_LIMIT));

  /* The fragment pages are in the first extent, the space grew page by
  page for them. */
  for (auto p : pages) {
    EXPECT_LT(p, FSP_EXTENT_SIZE);
  }

  EXPECT_TRUE(fsp().validate(space));

  const auto p = alloc(seg);

  ASSERT_NE(p, FIL_NULL);
  EXPECT_GE(p, FSP_EXTENT_SIZE) << "from an extent of the segment";
  EXPECT_EQ(p % FSP_EXTENT_SIZE, 0U) << "its first page";
  EXPECT_EQ(reserved(seg), std::make_pair(FRAG_LIMIT + FSP_EXTENT_SIZE, FRAG_LIMIT + 1));
  EXPECT_TRUE(seg_valid(seg));
  EXPECT_TRUE(fsp().validate(space));
}

/** A segment extent goes from the segment free list to the not-full list on
its first page, to the full list on its last. */
TEST_F(FspSegTest, ExtentFull) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i = 1; i < FRAG_LIMIT; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  std::set<page_no_t> pages;

  for (ulint i{}; i < FSP_EXTENT_SIZE; ++i) {
    const auto p = alloc(seg);

    ASSERT_NE(p, FIL_NULL);
    EXPECT_TRUE(pages.insert(p).second);
  }

  /* The pages of one whole extent. */
  EXPECT_EQ(*pages.rbegin() - *pages.begin(), FSP_EXTENT_SIZE - 1);

  const auto [n, used] = reserved(seg);

  EXPECT_EQ(used, FRAG_LIMIT + FSP_EXTENT_SIZE);
  EXPECT_GE(n, used);
  EXPECT_TRUE(seg_valid(seg));
  EXPECT_TRUE(fsp().validate(space));
}

/** Fragment extents of the space: FREE -> FREE_FRAG when a fragment page is
taken from a free extent, FREE_FRAG -> FULL_FRAG when its last page is
taken, and back as the pages are freed. */
TEST_F(FspSegTest, FragListTransitions) {
  const auto space = new_space();
  std::vector<Seg> segs;
  std::vector<std::vector<page_no_t>> pages;

  /* Fill the first extent (64 pages, one is the descriptor) with fragment
  pages of several segments, and go into the next one. */
  for (int s = 0; s < 3; ++s) {
    segs.push_back(create_seg(space));
    pages.emplace_back();

    for (ulint i = 1; i < FRAG_LIMIT; ++i) {
      const auto p = alloc(segs.back());

      ASSERT_NE(p, FIL_NULL);
      pages.back().push_back(p);
    }
  }

  /* 1 descriptor + 1 inode page + 3 * 32 segment pages = 98 fragment pages. */
  auto h = hdr(space);

  EXPECT_EQ(h.m_n_full_frag, 1UL) << "the first extent is full";
  EXPECT_EQ(h.m_n_free_frag, 1UL) << "the second has free pages";
  EXPECT_EQ(h.m_frag_n_used, 98UL - FSP_EXTENT_SIZE) << "counts the pages of FREE_FRAG extents only";
  EXPECT_TRUE(fsp().validate(space));

  /* Free a page of the full extent: it is not full anymore. */
  page_no_t in_first{FIL_NULL};

  for (auto &v : pages) {
    for (auto it = v.begin(); it != v.end(); ++it) {
      if (*it < FSP_EXTENT_SIZE) {
        in_first = *it;
        free(segs[&v - &pages[0]], in_first);
        v.erase(it);
        break;
      }
    }

    if (in_first != FIL_NULL) {
      break;
    }
  }

  ASSERT_NE(in_first, FIL_NULL);

  h = hdr(space);
  EXPECT_EQ(h.m_n_full_frag, 0UL);
  EXPECT_EQ(h.m_n_free_frag, 2UL);
  EXPECT_EQ(h.m_frag_n_used, 97UL);
  EXPECT_TRUE(fsp().validate(space));

  /* Free all the pages of the second extent, with the last segment (its
  header page is there): the extent becomes a free extent. */
  ASSERT_GE(segs[2].m_page_no, FSP_EXTENT_SIZE);
  drop(segs[2]);
  pages[2].clear();

  for (size_t s{}; s < segs.size(); ++s) {
    auto &v = pages[s];

    for (auto it = v.begin(); it != v.end();) {
      if (*it >= FSP_EXTENT_SIZE && *it < 2 * FSP_EXTENT_SIZE) {
        free(segs[s], *it);
        it = v.erase(it);
      } else {
        ++it;
      }
    }
  }

  h = hdr(space);
  EXPECT_EQ(h.m_n_free_frag, 1UL);
  EXPECT_GE(h.m_n_free, 1UL) << "the emptied extent";
  EXPECT_TRUE(fsp().validate(space));
}

/** Freeing pages of a segment, fragment and extent pages: the counts follow,
a page freed can be allocated again. */
TEST_F(FspSegTest, FreePage) {
  const auto space = new_space();
  const auto seg = create_seg(space);
  std::vector<page_no_t> pages;

  for (ulint i = 1; i < FRAG_LIMIT + 10; ++i) {
    pages.push_back(alloc(seg));
    ASSERT_NE(pages.back(), FIL_NULL);
  }

  const auto [n0, used0] = reserved(seg);

  /* A fragment page. */
  free(seg, pages[0]);
  EXPECT_EQ(reserved(seg), std::make_pair(n0 - 1, used0 - 1));

  /* A page of an extent: the extent stays reserved. */
  free(seg, pages.back());
  EXPECT_EQ(reserved(seg), std::make_pair(n0 - 1, used0 - 2));
  EXPECT_TRUE(seg_valid(seg));
  EXPECT_TRUE(fsp().validate(space));

  /* The extent page comes back first: it is free in an extent of the segment. */
  EXPECT_EQ(alloc(seg, pages.back()), pages.back());
  EXPECT_TRUE(fsp().validate(space));
}

/** All the pages of an extent of a segment freed: the extent goes back to
the space. */
TEST_F(FspSegTest, FreeWholeExtent) {
  const auto space = new_space();
  const auto seg = create_seg(space);
  std::vector<page_no_t> ext;

  for (ulint i = 1; i < FRAG_LIMIT; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  for (ulint i{}; i < 5; ++i) {
    ext.push_back(alloc(seg));
  }

  const auto before = hdr(space);

  for (auto p : ext) {
    free(seg, p);
  }

  const auto after = hdr(space);

  EXPECT_EQ(reserved(seg), std::make_pair(FRAG_LIMIT, FRAG_LIMIT));
  EXPECT_EQ(after.m_n_free, before.m_n_free + 1);
  EXPECT_TRUE(fsp().validate(space));
}

/** The hint: a free page of an extent of the segment is taken as hinted. */
TEST_F(FspSegTest, Hint) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i = 1; i < FRAG_LIMIT; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  const auto first = alloc(seg);

  ASSERT_NE(first, FIL_NULL);

  for (page_no_t want : {first + 10, first + 63, first + 1, first + 30}) {
    EXPECT_EQ(alloc(seg, want), want);
  }

  /* No hint: the lowest free page of the not-full extent. */
  EXPECT_EQ(alloc(seg, 0), first + 2);
  EXPECT_TRUE(fsp().validate(space));
}

/** Pages of a hinted extent the segment owns are taken around the hint. */
TEST_F(FspSegTest, HintTaken) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i = 1; i < FRAG_LIMIT; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  const auto first = alloc(seg);

  EXPECT_EQ(alloc(seg, first), first + 1) << "the hint is taken: the next free page from it";
  EXPECT_TRUE(fsp().validate(space));
}

/** Dropping a segment step by step frees all its pages, then its inode;
the inode page goes when its last inode does. */
TEST_F(FspSegTest, FreeStep) {
  const auto space = new_space();
  const auto before = hdr(space);
  const auto seg = create_seg(space);

  for (ulint i{}; i < 200; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  EXPECT_TRUE(fsp().validate(space));

  const auto steps = drop(seg);

  EXPECT_GT(steps, 1UL);

  const auto after = hdr(space);

  EXPECT_EQ(after.m_n_inodes_free, 0UL);
  EXPECT_EQ(after.m_n_inodes_full, 0UL);
  EXPECT_EQ(after.m_frag_n_used, before.m_frag_n_used) << "back to the descriptor page only";
  EXPECT_EQ(after.m_n_full_frag, 0UL);
  EXPECT_TRUE(fsp().validate(space));
}

/** A segment with its header page only. */
TEST_F(FspSegTest, FreeStepSmall) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  EXPECT_EQ(drop(seg), 1UL);
  EXPECT_TRUE(fsp().validate(space));
  EXPECT_EQ(hdr(space).m_frag_n_used, 1UL);
}

/** fseg_free_step_not_header() frees everything but the header page. */
TEST_F(FspSegTest, FreeStepNotHeader) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 100; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  for (ulint steps{};; ++steps) {
    ASSERT_LT(steps, 1000UL);

    Mtr mtr;

    mtr.start();

    const auto done = fsp().fseg_free_step_not_header(header(seg, mtr), &mtr);

    mtr.commit(*m_sys);

    if (done) {
      break;
    }
  }

  EXPECT_EQ(reserved(seg), std::make_pair(1UL, 1UL));
  EXPECT_TRUE(fsp().validate(space));

  drop(seg);
  EXPECT_TRUE(fsp().validate(space));
}

/** Segments use one inode each: an inode page holds INODES_PER_PAGE of them,
goes to the FULL list when it has no free one, a new page is allocated; all
of them come back when the segments are dropped. */
TEST_F(FspSegTest, ManySegments) {
  const auto space = new_space();
  std::vector<Seg> segs;

  for (ulint i{}; i < INODES_PER_PAGE + 5; ++i) {
    segs.push_back(create_seg(space));
    ASSERT_NE(segs.back().m_page_no, FIL_NULL) << i;
  }

  auto h = hdr(space);

  EXPECT_EQ(h.m_n_inodes_full, 1UL);
  EXPECT_EQ(h.m_n_inodes_free, 1UL);
  EXPECT_EQ(h.m_seg_id, INODES_PER_PAGE + 6);
  EXPECT_TRUE(fsp().validate(space));

  for (auto &seg : segs) {
    drop(seg);
  }

  h = hdr(space);

  EXPECT_EQ(h.m_n_inodes_full, 0UL);
  EXPECT_EQ(h.m_n_inodes_free, 0UL);
  EXPECT_EQ(h.m_seg_id, INODES_PER_PAGE + 6) << "ids are not reused";
  EXPECT_TRUE(fsp().validate(space));
}

/** Freeing one segment of a full inode page moves the page back to the
free inode list. */
TEST_F(FspSegTest, InodePageFullToFree) {
  const auto space = new_space();
  std::vector<Seg> segs;

  for (ulint i{}; i < INODES_PER_PAGE; ++i) {
    segs.push_back(create_seg(space));
  }

  EXPECT_EQ(hdr(space).m_n_inodes_full, 1UL);
  EXPECT_EQ(hdr(space).m_n_inodes_free, 0UL);

  drop(segs[5]);

  EXPECT_EQ(hdr(space).m_n_inodes_full, 0UL);
  EXPECT_EQ(hdr(space).m_n_inodes_free, 1UL);
  EXPECT_TRUE(fsp().validate(space));
}

/** The space grows when the segments need more: the size stays a whole
number of extents past the first, the free limit follows. */
TEST_F(FspSegTest, SpaceGrows) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 2000; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL) << i;
  }

  const auto h = hdr(space);

  EXPECT_GE(h.m_size, 2000UL);
  EXPECT_EQ(h.m_size % FSP_EXTENT_SIZE, 0UL);
  EXPECT_LE(h.m_free_limit, h.m_size);
  EXPECT_EQ(m_sys->m_fil->space_get_size(space), h.m_size);
  EXPECT_TRUE(seg_valid(seg));
  EXPECT_TRUE(fsp().validate(space));
}

/** A space past UNIV_PAGE_SIZE pages has a second descriptor page: it is
used, never handed out to a segment. */
TEST_F(FspSegTest, SecondDescriptorPage) {
  const auto space = new_space();
  const auto seg = create_seg(space);
  std::set<page_no_t> pages;

  for (ulint i{}; i < UNIV_PAGE_SIZE + 200; ++i) {
    const auto p = alloc(seg);

    ASSERT_NE(p, FIL_NULL) << i;
    ASSERT_FALSE(fsp().descr_page(p)) << p;
    pages.insert(p);
  }

  EXPECT_EQ(pages.size(), UNIV_PAGE_SIZE + 200);
  EXPECT_GT(hdr(space).m_size, UNIV_PAGE_SIZE);
  EXPECT_TRUE(fsp().validate(space));

  drop(seg);
  EXPECT_TRUE(fsp().validate(space));
}

/** Reservations: a small space reserves pages, not extents; a big space
reserves the extents and they are given back. */
TEST_F(FspSegTest, ReserveFreeExtents) {
  const auto space = new_space();
  ulint n{};

  {
    Mtr mtr;

    mtr.start();
    EXPECT_TRUE(fsp().reserve_free_extents(&n, space, 2, FSP_NORMAL, &mtr));
    mtr.commit(*m_sys);

    EXPECT_EQ(n, 0UL) << "small space";
    EXPECT_EQ(m_sys->m_fil->space_get_n_reserved_extents(space), 0UL);
  }

  /* Make the space big. */
  const auto seg = create_seg(space);

  for (ulint i{}; i < 600; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  for (auto type : {FSP_NORMAL, FSP_UNDO, FSP_CLEANING}) {
    Mtr mtr;

    mtr.start();
    EXPECT_TRUE(fsp().reserve_free_extents(&n, space, 3, type, &mtr)) << type;
    mtr.commit(*m_sys);

    EXPECT_EQ(n, 3UL);
    EXPECT_EQ(m_sys->m_fil->space_get_n_reserved_extents(space), 3UL);

    m_sys->m_fil->space_release_free_extents(space, n);
    EXPECT_EQ(m_sys->m_fil->space_get_n_reserved_extents(space), 0UL);
  }

  EXPECT_TRUE(fsp().validate(space));
}

/** A reservation bigger than the free extents extends the space. */
TEST_F(FspSegTest, ReserveExtends) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 200; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  const auto size = hdr(space).m_size;
  ulint n{};
  Mtr mtr;

  mtr.start();
  EXPECT_TRUE(fsp().reserve_free_extents(&n, space, 20, FSP_NORMAL, &mtr));
  mtr.commit(*m_sys);

  EXPECT_EQ(n, 20UL);
  EXPECT_GT(hdr(space).m_size, size);

  m_sys->m_fil->space_release_free_extents(space, n);
  EXPECT_TRUE(fsp().validate(space));
}

/** Available space: zero for a space smaller than an extent, the free
extents minus the safety margin otherwise. */
TEST_F(FspSegTest, AvailableSpace) {
  const auto space = new_space();

  EXPECT_EQ(fsp().get_available_space_in_free_extents(space), 0UL);

  page_no_t actual{};

  /* Grow the file and the header in step, through allocations. */
  const auto seg = create_seg(space);

  for (ulint i{}; i < 1000; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  (void)actual;

  const auto h = hdr(space);
  const auto kb = fsp().get_available_space_in_free_extents(space);
  const auto reserve = 2 + ((h.m_size / FSP_EXTENT_SIZE) * 2) / 200;
  auto n_free_up = (h.m_size - h.m_free_limit) / FSP_EXTENT_SIZE;

  if (n_free_up > 0) {
    --n_free_up;
  }

  const auto n_free = h.m_n_free + n_free_up;
  const uint64_t expected = n_free > reserve ? (n_free - reserve) * FSP_EXTENT_SIZE * (UNIV_PAGE_SIZE / 1024) : 0;

  EXPECT_EQ(kb, expected);
  EXPECT_GT(fsp().get_available_space_in_free_extents(SYS_TABLESPACE), 0UL);
}

/** Segments in the system tablespace. */
TEST_F(FspSegTest, SysSegment) {
  const auto before = hdr(SYS_TABLESPACE);
  const auto seg = create_seg(SYS_TABLESPACE);

  ASSERT_NE(seg.m_page_no, FIL_NULL);

  for (ulint i{}; i < 300; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  EXPECT_EQ(reserved(seg).second, 301UL);
  EXPECT_TRUE(fsp().validate(SYS_TABLESPACE));

  drop(seg);

  EXPECT_TRUE(fsp().validate(SYS_TABLESPACE));
  EXPECT_EQ(hdr(SYS_TABLESPACE).m_frag_n_used, before.m_frag_n_used);
}

/** Threads with segments of their own in one space, allocating and freeing
at once: no page is given to two segments, the space stays valid. */
TEST_F(FspSegTest, Concurrent) {
  const auto space = new_space();
  constexpr int N_THREADS = 8;
  std::vector<Seg> segs;

  for (int t = 0; t < N_THREADS; ++t) {
    segs.push_back(create_seg(space));
  }

  std::vector<std::vector<page_no_t>> pages(N_THREADS);
  std::vector<std::thread> threads;

  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 150; ++i) {
        const auto p = alloc(segs[t]);

        EXPECT_NE(p, FIL_NULL);
        pages[t].push_back(p);

        if (i % 3 == 2) {
          free(segs[t], pages[t][pages[t].size() / 2]);
          pages[t].erase(pages[t].begin() + pages[t].size() / 2);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::set<page_no_t> all;
  size_t n{};

  for (int t = 0; t < N_THREADS; ++t) {
    all.insert(segs[t].m_page_no);
    all.insert(pages[t].begin(), pages[t].end());
    n += pages[t].size() + 1;
    EXPECT_EQ(reserved(segs[t]).second, pages[t].size() + 1);
    EXPECT_TRUE(seg_valid(segs[t]));
  }

  EXPECT_EQ(all.size(), n) << "a page is in two segments";
  EXPECT_TRUE(fsp().validate(space));
}

/** Segments, their pages and the free lists survive a restart. */
TEST_F(FspTest, Restart) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 150; ++i) {
    ASSERT_NE(alloc(seg), FIL_NULL);
  }

  const auto h = hdr(space);
  const auto r = reserved(seg);

  restart();

  /* The scratch space is found by the scan of the data directory. */
  ASSERT_TRUE(m_sys->m_fil->tablespace_exists_in_mem(space));

  const auto h2 = hdr(space);

  EXPECT_EQ(h2.m_size, h.m_size);
  EXPECT_EQ(h2.m_free_limit, h.m_free_limit);
  EXPECT_EQ(h2.m_frag_n_used, h.m_frag_n_used);
  EXPECT_EQ(h2.m_n_free, h.m_n_free);
  EXPECT_EQ(h2.m_seg_id, h.m_seg_id);
  EXPECT_EQ(reserved(seg), r);
  EXPECT_TRUE(fsp().validate(space));

  drop(seg);
  EXPECT_TRUE(fsp().validate(space));
}

}  // namespace

namespace {

using FspPerf = FspSegTest;

/** Allocate and free a page of a segment that owns extents. */
TEST_F(FspPerf, AllocFree) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 200; ++i) {
    (void)alloc(seg);
  }

  auto r = ut::bench("fseg_alloc_free_page", 20000, [&](uint64_t) {
    const auto p = alloc(seg);

    free(seg, p);
  });

  EXPECT_LT(r.ns_per_op(), 2e6);
}

/** Allocate pages to grow a segment (the space extends as it goes). */
TEST_F(FspPerf, Grow) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  auto r = ut::bench("fseg_alloc_grow", 20000, [&](uint64_t) { ut::keep(alloc(seg)); });

  EXPECT_LT(r.ns_per_op(), 2e6);

  auto d = ut::bench("fseg_free_step", 1, [&](uint64_t) { ut::keep(drop(seg)); });

  EXPECT_LT(d.ns_per_op(), 10e9);
}

/** Create and drop of small segments. */
TEST_F(FspPerf, SegCreateDrop) {
  const auto space = new_space();

  auto r = ut::bench("fseg_create_drop", 5000, [&](uint64_t) {
    const auto seg = create_seg(space);

    (void)drop(seg);
  });

  EXPECT_LT(r.ns_per_op(), 5e6);
}

/** Validation of a space with a few thousand pages. */
TEST_F(FspPerf, Validate) {
  const auto space = new_space();
  const auto seg = create_seg(space);

  for (ulint i{}; i < 5000; ++i) {
    (void)alloc(seg);
  }

  auto r = ut::bench("fsp_validate", 50, [&](uint64_t) { ut::keep(fsp().validate(space)); });

  EXPECT_LT(r.ns_per_op(), 1e9);
}

/** Alloc and free from 8 threads, each with a segment of its own. */
TEST_F(FspPerf, AllocFreeMt) {
  const auto space = new_space();
  std::vector<Seg> segs;

  for (int t = 0; t < 8; ++t) {
    segs.push_back(create_seg(space));

    for (int i = 0; i < 100; ++i) {
      (void)alloc(segs.back());
    }
  }

  auto r = ut::bench_mt("fseg_alloc_free_page_mt", 8, 5000, [&](int t, uint64_t) {
    const auto p = alloc(segs[t]);

    free(segs[t], p);
  });

  EXPECT_LT(r.ns_per_op(), 10e6);
}

}  // namespace
