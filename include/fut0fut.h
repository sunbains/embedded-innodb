/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/fut0fut.h
File-based utilities

Created 12/13/1995 Heikki Tuuri
***********************************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0buf.h"
#include "fil0fil.h"
#include "mtr0mtr.h"
#include "sync0rw.h"

/** Gets a pointer to a file address and latches the page.
@param[in] space_id             Tablespace ID
@param[in] addr                 File address
@param[in] rw_latch             RW_S_LATCH , RW_X_LATCH
@param[in,out] mtr              Mini-transaction to track block fetch.
@return pointer to a byte in a frame; the file page in the frame is
bufferfixed and latched */
inline byte *fut_get_ptr(space_id_t space_id, fil_addr_t addr, ulint rw_latch, mtr_t *mtr) {
  ut_ad(addr.m_boffset < UNIV_PAGE_SIZE);
  ut_ad(rw_latch == RW_S_LATCH || rw_latch == RW_X_LATCH);

  Buf_pool::Request req {
    .m_rw_latch = rw_latch,
    .m_page_id = { space_id, addr.m_page_no },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);
  auto ptr = block->get_frame() + addr.m_boffset;

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  return ptr;
}
