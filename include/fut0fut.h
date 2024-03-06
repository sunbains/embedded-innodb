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

#include "fil0fil.h"
#include "mtr0mtr.h"
#include "buf0buf.h"
#include "sync0rw.h"

/** Gets a pointer to a file address and latches the page.
@return pointer to a byte in a frame; the file page in the frame is
bufferfixed and latched.
@param[in] space_id             Tablespace ID.
@param[in] add                  File address
@param[in,out] rw_latch         RW_S_LATCH, RW_X_LATCH
@param[in,out] mtr              Mini-transaction. */
inline byte *fut_get_ptr(space_id_t space, fil_addr_t addr, ulint rw_latch, mtr_t *mtr) {
  ut_ad(addr.boffset < UNIV_PAGE_SIZE);
  ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

  auto block = buf_page_get(space, addr.page, rw_latch, mtr);
  auto ptr = buf_block_get_frame(block) + addr.boffset;

  buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

  return ptr;
}
