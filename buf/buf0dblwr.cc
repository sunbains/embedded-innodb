/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file buf/buf0dblwr.cc
Doublewrite buffer

Created 2024-09-25 by Sunny Bains. */

#include "buf0dblwr.h"
#include "trx0sys.h"

/** The doublewrite buffer instance */
DBLWR *srv_dblwr{};

DBLWR::DBLWR(FSP *fsp)
  : m_fsp(fsp),
    m_block1(ULINT32_UNDEFINED),
    m_block2(ULINT32_UNDEFINED) {


  mutex_create(&m_mutex, IF_DEBUG("DBLWR::m_mutex",) IF_SYNC_DEBUG(SYNC_DOUBLEWRITE,) Source_location{});

  m_ptr = static_cast<byte *>(ut_new((1 + 2 * SYS_DOUBLEWRITE_BLOCK_SIZE) * UNIV_PAGE_SIZE));

  m_write_buf = static_cast<byte *>(ut_align(m_ptr, UNIV_PAGE_SIZE));

  m_bpages.resize(2 * SYS_DOUBLEWRITE_BLOCK_SIZE);
}

DBLWR::~DBLWR() {
  if (m_ptr != nullptr) {
    ut_delete(m_ptr);
  }

  mutex_free(&m_mutex);
}

bool DBLWR::is_page_inside(page_no_t page_no) const noexcept {
  if (page_no >= m_block1 && page_no < m_block1 + SYS_DOUBLEWRITE_BLOCK_SIZE) {
    return true;
  } else {
    return page_no >= m_block2 && page_no < m_block2 + SYS_DOUBLEWRITE_BLOCK_SIZE;
  }
}

db_err DBLWR::initialize() noexcept {
  ut_a(m_block1 == ULINT32_UNDEFINED);
  ut_a(m_block2 == ULINT32_UNDEFINED);

  mtr_t mtr;

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { SYS_TABLESPACE, TRX_SYS_PAGE_NO },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  mtr.start();

  mtr.dblwr_create_in_progresss();

  auto block = m_fsp->m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  auto dblwr = block->get_frame() + SYS_DOUBLEWRITE;

  /* Check if the doublewrite buffer already exists */
  ut_a(mach_read_from_4(dblwr + SYS_DOUBLEWRITE_MAGIC) != SYS_DOUBLEWRITE_MAGIC_N);

  log_info("Doublewrite buffer not found: creating new");

  if (m_fsp->m_buf_pool->get_curr_size() < ((2 * SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2 + 100) * UNIV_PAGE_SIZE)) {
    log_err(
      "Cannot create doublewrite buffer: you must increase your buffer pool size."
      " Cannot continue operation."
    );

    mtr.commit();
    return DB_FATAL;
  }

  auto block2 = m_fsp->fseg_create(SYS_TABLESPACE, TRX_SYS_PAGE_NO, SYS_DOUBLEWRITE + SYS_DOUBLEWRITE_FSEG, &mtr);

  /* fseg_create acquires a second latch on the page, therefore we must declare it: */

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block2, SYNC_NO_ORDER_CHECK));

  if (block2 == nullptr) {
    log_err(
      "Cannot create doublewrite buffer: you must increase your tablespace size."
      " Cannot continue operation."
    );

    /* We need to exit without committing the mtr to prevent its modifications to
    the database getting to disk */

    mtr.commit();
    return DB_FATAL;
  }

  page_no_t prev_page_no{};
  auto fseg_header = block->get_frame() + SYS_DOUBLEWRITE + SYS_DOUBLEWRITE_FSEG;

  for (ulint i{}; i < 2 * SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2; ++i) {
    auto page_no = m_fsp->fseg_alloc_free_page(fseg_header, prev_page_no + 1, FSP_UP, &mtr);

    if (page_no == FIL_NULL) {
      log_err(
        "Cannot create doublewrite buffer: you must increase your tablespace size."
        " Cannot continue operation."
      );

      mtr.commit();
      return DB_FATAL;
    }

    /* We read the allocated pages to the buffer pool; when they are written to disk in a flush,
    the space id and page number fields are also written to the pages. When we at database startup
    read pages from the doublewrite buffer, we know that if the space id and page number in them
    are the same as the page position in the tablespace, then the page has not been written to in
    doublewrite. */

    IF_SYNC_DEBUG({
      Buf_pool::Request req {
        .m_rw_lock = RW_X_LATCH,
        .m_page_id = { SYS_TABLESPACE, page_no },
        .m_mode = BUF_GET,
        .m_file = __FILE__,
        .m_line = __LINE__,
        .m_mtr = &mtr
      };
      auto new_block = buf_pool->get(req, nullptr);
      buf_block_dbg_add_level(IF_SYNC_DEBUG(new_block, SYNC_NO_ORDER_CHECK));
    })

    if (i == FSP_EXTENT_SIZE / 2) {
      ut_a(page_no == FSP_EXTENT_SIZE);
      mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
      mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_REPEAT + SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
      m_block1 = page_no;
    } else if (i == FSP_EXTENT_SIZE / 2 + SYS_DOUBLEWRITE_BLOCK_SIZE) {
      ut_a(page_no == 2 * FSP_EXTENT_SIZE);
      mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
      mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_REPEAT + SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
      m_block2 = page_no;
    } else if (i > FSP_EXTENT_SIZE / 2) {
      ut_a(page_no == prev_page_no + 1);
    }

    prev_page_no = page_no;
  }

  mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_MAGIC, SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);

  mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_MAGIC + SYS_DOUBLEWRITE_REPEAT, SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);

  mlog_write_ulint(dblwr + SYS_DOUBLEWRITE_SPACE_ID_STORED, SYS_DOUBLEWRITE_SPACE_ID_STORED_N, MLOG_4BYTES, &mtr);

  mtr.commit();

  log_info("Doublewrite buffer created");

  return DB_SUCCESS;
}

bool DBLWR::check_if_exists(Fil *fil, std::pair<page_no_t, page_no_t> &offsets) noexcept {

  offsets.first = ULINT32_UNDEFINED;
  offsets.second = ULINT32_UNDEFINED;

  /* We bypass the buffer pool here. */
  auto ptr = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  auto page = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  /* Read the trx sys header to check if we are using the doublewrite buffer */

  fil->io(IO_request::Sync_read, false, SYS_TABLESPACE, TRX_SYS_PAGE_NO, 0, UNIV_PAGE_SIZE, page, nullptr);

  auto dblwr = page + SYS_DOUBLEWRITE;
  auto exists = mach_read_from_4(dblwr + SYS_DOUBLEWRITE_MAGIC) == SYS_DOUBLEWRITE_MAGIC_N;

  if (exists) {
    offsets.first = mach_read_from_4(dblwr + SYS_DOUBLEWRITE_BLOCK1);
    offsets.second = mach_read_from_4(dblwr + SYS_DOUBLEWRITE_BLOCK2);
  }

  ut_delete(ptr);

  return exists;
}

void DBLWR::recover_pages() noexcept {
  ut_a(m_block1 != ULINT32_UNDEFINED);
  ut_a(m_block2 != ULINT32_UNDEFINED);

  auto ptr = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  auto read_buf = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  /* Read the trx sys header to check if we are using the doublewrite buffer.
   * Note: We bypass the buffer pool here.*/

  m_fsp->m_fil->io(IO_request::Sync_read, false, SYS_TABLESPACE, TRX_SYS_PAGE_NO, 0, UNIV_PAGE_SIZE, read_buf, nullptr);

  {
    const auto dblwr = read_buf + SYS_DOUBLEWRITE;

    /* Check that the doublewrite buffer has been created */
    ut_a(mach_read_from_4(dblwr + SYS_DOUBLEWRITE_MAGIC) == SYS_DOUBLEWRITE_MAGIC_N);
  }

  auto buf = m_write_buf;

  /* Read the pages from both the doublewrite buffers into memory */

  m_fsp->m_fil->io(
    IO_request::Sync_read,
    false,
    SYS_TABLESPACE,
    m_block1,
    0,
    SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    buf,
    nullptr);

  m_fsp->m_fil->io(
    IO_request::Sync_read,
    false,
    SYS_TABLESPACE,
    m_block2,
    0,
    SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    buf + SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    nullptr
  );

  /* Check if any of these pages is half-written in data files, in the intended
   * position */

  auto page = buf;

  for (ulint i{}; i < SYS_DOUBLEWRITE_BLOCK_SIZE * 2; ++i) {
    const auto page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);
    const auto space_id = mach_read_from_4(page + FIL_PAGE_SPACE_ID);

    if (!m_fsp->m_fil->tablespace_exists_in_mem(space_id)) {
      /* Maybe we have dropped the single-table tablespace
      and this page once belonged to it: do nothing */
    } else if (!m_fsp->m_fil->check_adress_in_tablespace(space_id, page_no)) {
      log_warn(std::format(
        "A page in the doublewrite buffer is not within space bounds; space id {}"
        " page number {}, page {} in doublewrite buf.",
        space_id,
        page_no,
        i
      ));

    } else if (space_id == SYS_TABLESPACE && is_page_inside(page_no)) {
      /* It is an unwritten doublewrite buffer page: do nothing */
    } else {
      /* Read in the actual page from the file */
      m_fsp->m_fil->io(IO_request::Sync_read, false, space_id, page_no, 0, UNIV_PAGE_SIZE, read_buf, nullptr);

      /* Check if the page is corrupt */

      if (unlikely(m_fsp->m_buf_pool->is_corrupted(read_buf))) {

        log_warn(std::format(
          "Database page corruption or a failed file read of space {} page {}."
          " Trying to recover it from the doublewrite buffer.",
          space_id,
          page_no
        ));

        if (m_fsp->m_buf_pool->is_corrupted(page)) {
          log_info("Dump of the page:");
          buf_page_print(read_buf, 0);
          log_warn("Dump of corresponding page in doublewrite buffer:");
          buf_page_print(page, 0);

          log_fatal(
            "The page in the doublewrite buffer is corrupt too."
            " Cannot continue operation. You can try to recover"
            " the database with the option: force_recovery=6"
          );
        }

        /* Write the good page from the doublewrite buffer to the intended
         * position */

        m_fsp->m_fil->io(IO_request::Sync_write, false, space_id, page_no, 0, UNIV_PAGE_SIZE, page, nullptr);

        log_info("Recovered the page from the doublewrite buffer.");
      }
    }

    page += UNIV_PAGE_SIZE;
  }

  m_fsp->m_fil->flush_file_spaces(FIL_TABLESPACE);

  ut_delete(ptr);
}

DBLWR *DBLWR::create(FSP *fsp) noexcept {
  auto ptr = ut_new(sizeof(DBLWR));

  return ptr != nullptr ? new (ptr) DBLWR(fsp) : nullptr;
}

void DBLWR::destroy(DBLWR *&dblwr) noexcept {
  call_destructor(dblwr);
  ut_delete(dblwr);
  dblwr = nullptr;
}

