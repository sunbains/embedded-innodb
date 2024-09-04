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

/** @file include/buf0dblwr.h
Doublewrite buffer header file

Created 2024-09-25 by Sunny Bains. */

#pragma once

#include "innodb0types.h"

#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "sync0sync.h"

/** Doublewrite buffer */
/* @{ */
/** The offset of the doublewrite buffer header on the trx system header page */
constexpr ulint SYS_DOUBLEWRITE = UNIV_PAGE_SIZE - 200;

/** fseg header of the fseg containing the doublewrite buffer */
constexpr ulint SYS_DOUBLEWRITE_FSEG = 0;

/** 4-byte magic number which shows if we already have created the
doublewrite buffer */
constexpr auto SYS_DOUBLEWRITE_MAGIC = FSEG_HEADER_SIZE;

/** Page number of the first page in the first sequence of 64
(= FSP_EXTENT_SIZE) consecutive pages in the doublewrite buffer */
constexpr auto SYS_DOUBLEWRITE_BLOCK1 = 4 + FSEG_HEADER_SIZE;

/** Page number of the first page in the second sequence of 64
consecutive pages in the doublewrite buffer */
constexpr auto SYS_DOUBLEWRITE_BLOCK2 = 8 + FSEG_HEADER_SIZE;

/** We repeat TRX_SYS_DOUBLEWRITE_MAGIC, TRX_SYS_DOUBLEWRITE_BLOCK1,
TRX_SYS_DOUBLEWRITE_BLOCK2 so that if the trx sys header is half-written
to disk, we still may be able to recover the information */
constexpr auto SYS_DOUBLEWRITE_REPEAT = 12;

/** If this is not yet set to TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N,
we must reset the doublewrite buffer, because starting from 4.1.x the
space id of a data page is stored into FIL_PAGE_ARCH_LOG_NO_OR_SPACE_NO. */
constexpr auto SYS_DOUBLEWRITE_SPACE_ID_STORED = 24 + FSEG_HEADER_SIZE;

/** Contents of SYS_DOUBLEWRITE_SPACE_ID_STORED */
constexpr ulint SYS_DOUBLEWRITE_SPACE_ID_STORED_N = 1783657386;

/** Contents of SYS_DOUBLEWRITE_MAGIC */
constexpr ulint SYS_DOUBLEWRITE_MAGIC_N = 536853855;

/** Size of the doublewrite block in pages */
constexpr auto SYS_DOUBLEWRITE_BLOCK_SIZE = FSP_EXTENT_SIZE;

/* @} */

/** Doublewrite control struct */
struct DBLWR {

  /** 
   * Constructor.
   * 
   * @param[in,out] fsp Filespace manager to use for IO.
   */
  explicit DBLWR(FSP *fsp);

  /** 
   * Destructor.
   */
  ~DBLWR() noexcept;

  /** 
   * Create an instance of DBLWR.
   * 
   * @param[in,out] fsp Filespace manager to use for IO.
   * 
   * @return and instance or nullptr if there is an error.
   */
  static DBLWR* create(FSP *fsp) noexcept;

  /**
   * Destroy an instance of DBLWR.
   */
  static void destroy(DBLWR *&dblwr) noexcept;

  /**
   * Creates the doublewrite buffer to a new InnoDB installation. The header of
   * the doublewrite buffer is placed on the trx system header page.
   * 
   * @return DB_SUCCESS if successful, otherwise an error code.
   */
  db_err initialize() noexcept;

  /**
   * At a database startup initializes the doublewrite buffer memory structure if
   * we already have a doublewrite buffer created in the data files. If we are
   * upgrading to an InnoDB version which supports multiple tablespaces, then this
   * function performs the necessary update operations. If we are in a crash
   * recovery, this function uses a possible doublewrite buffer to restore
   * half-written pages in the data files.
   * 
   */
  void recover_pages() noexcept;

  /**
   * Determines if a page number is located inside the doublewrite buffer.
   * 
   * @param[in] page_no           Page number ot check.
   * 
   * @return true if the location is inside the two blocks of the
   *  doublewrite buffer */
  bool is_page_inside(page_no_t page_no) const noexcept;

  /**
   * Check if the dblwr buffer has been created in the tablespace.
   * 
   * @param[in,out] fil File manager instance
   * @param[out] offsets The double write segment start offsets.
   * 
   * @return true if it exists and is initialized.
   * 
   * @note offsets.first and offsets.second will be set to ULINT32_UNDEFINED
   * if the doublewrite buffer has not been created yet.
   */
  static bool check_if_exists(Fil* fil, std::pair<page_no_t, page_no_t> &offsets) noexcept;

  /** Filespace manager for IO. */
  FSP *m_fsp{};

  /** mutex protecting the first_free field and write_buf */
  mutex_t m_mutex{};

  /** The page number of the first doublewrite block (64 pages) */
  page_no_t m_block1{};

  /** Page number of the second block */
  page_no_t m_block2{};

  /** First free position in write_buf measured in units of UNIV_PAGE_SIZE */
  ulint m_first_free{};

  /** Write buffer used in writing to the doublewrite buffer, aligned to an
  address divisible by UNIV_PAGE_SIZE (which is required by Windows aio) */
  byte *m_write_buf{};

  /** pointer to write_buf, but unaligned */
  byte *m_ptr{};

   /** Array to store pointers to the buffer blocks which have been
  cached to write_buf */
  std::vector<buf_page_t*> m_bpages{};
};

/** Doublewrite system */
extern DBLWR *srv_dblwr;