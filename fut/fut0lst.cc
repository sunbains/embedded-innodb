/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file fut/fut0lst.c
File-based list utilities

Created 11/28/1995 Heikki Tuuri
***********************************************************************/

#include "fut0lst.h"

#include "buf0buf.h"
#include "page0page.h"

/**
 * @brief Adds a node to an empty list.
 *
 * This function adds a node to a list that is currently empty. It updates
 * the first and last pointers of the base node to point to the new node,
 * and sets the previous and next pointers of the new node to null.
 * Additionally, it increments the length of the list.
 *
 * @param[in] base Pointer to the base node of the empty list.
 * @param[in] node Pointer to the node to add.
 * @param[in] mtr Pointer to the mini-transaction handle.
 */
static void flst_add_to_empty(flst_base_node_t *base, flst_node_t *node, mtr_t *mtr) {
  ulint len;
  space_id_t space;
  Fil_addr node_addr;

  ut_ad(mtr && base && node);
  ut_ad(base != node);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node, MTR_MEMO_PAGE_X_FIX));
  len = flst_get_len(base, mtr);
  ut_a(len == 0);

  buf_ptr_get_fsp_addr(node, &space, &node_addr);

  /* Update first and last fields of base node */
  flst_write_addr(base + FLST_FIRST, node_addr, mtr);
  flst_write_addr(base + FLST_LAST, node_addr, mtr);

  /* Set prev and next fields of node to add */
  flst_write_addr(node + FLST_PREV, fil_addr_null, mtr);
  flst_write_addr(node + FLST_NEXT, fil_addr_null, mtr);

  /* Update len of base node */
  mlog_write_ulint(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_add_last(flst_base_node_t *base, flst_node_t *node, mtr_t *mtr) {
  ulint len;
  space_id_t space;
  Fil_addr node_addr;
  Fil_addr last_addr;
  flst_node_t *last_node;

  ut_ad(mtr && base && node);
  ut_ad(base != node);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node, MTR_MEMO_PAGE_X_FIX));
  len = flst_get_len(base, mtr);
  last_addr = flst_get_last(base, mtr);

  buf_ptr_get_fsp_addr(node, &space, &node_addr);

  /* If the list is not empty, call flst_insert_after */
  if (len != 0) {
    if (last_addr.m_page_no == node_addr.m_page_no) {
      last_node = Rec(node).page_align() + last_addr.m_boffset;
    } else {
      last_node = fut_get_ptr(space, last_addr, RW_X_LATCH, mtr);
    }

    flst_insert_after(base, last_node, node, mtr);
  } else {
    /* else call flst_add_to_empty */
    flst_add_to_empty(base, node, mtr);
  }
}

void flst_add_first(flst_base_node_t *base, flst_node_t *node, mtr_t *mtr) {
  space_id_t space;
  Fil_addr node_addr;
  ulint len;
  Fil_addr first_addr;
  flst_node_t *first_node;

  ut_ad(mtr && base && node);
  ut_ad(base != node);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node, MTR_MEMO_PAGE_X_FIX));
  len = flst_get_len(base, mtr);
  first_addr = flst_get_first(base, mtr);

  buf_ptr_get_fsp_addr(node, &space, &node_addr);

  /* If the list is not empty, call flst_insert_before */
  if (len != 0) {
    if (first_addr.m_page_no == node_addr.m_page_no) {
      first_node = Rec(node).page_align() + first_addr.m_boffset;
    } else {
      first_node = fut_get_ptr(space, first_addr, RW_X_LATCH, mtr);
    }

    flst_insert_before(base, node, first_node, mtr);
  } else {
    /* else call flst_add_to_empty */
    flst_add_to_empty(base, node, mtr);
  }
}

void flst_insert_after(flst_base_node_t *base, flst_node_t *node1, flst_node_t *node2, mtr_t *mtr) {
  ulint len;
  space_id_t space;
  Fil_addr node1_addr;
  Fil_addr node2_addr;
  flst_node_t *node3;
  Fil_addr node3_addr;

  ut_ad(mtr && node1 && node2 && base);
  ut_ad(base != node1);
  ut_ad(base != node2);
  ut_ad(node2 != node1);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node1, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node2, MTR_MEMO_PAGE_X_FIX));

  buf_ptr_get_fsp_addr(node1, &space, &node1_addr);
  buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

  node3_addr = flst_get_next_addr(node1, mtr);

  /* Set prev and next fields of node2 */
  flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
  flst_write_addr(node2 + FLST_NEXT, node3_addr, mtr);

  if (!srv_fil->addr_is_null(node3_addr)) {
    /* Update prev field of node3 */
    node3 = fut_get_ptr(space, node3_addr, RW_X_LATCH, mtr);
    flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);
  } else {
    /* node1 was last in list: update last field in base */
    flst_write_addr(base + FLST_LAST, node2_addr, mtr);
  }

  /* Set next field of node1 */
  flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);

  /* Update len of base node */
  len = flst_get_len(base, mtr);
  mlog_write_ulint(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_insert_before(flst_base_node_t *base, flst_node_t *node2, flst_node_t *node3, mtr_t *mtr) {
  ulint len;
  space_id_t space;
  flst_node_t *node1;
  Fil_addr node1_addr;
  Fil_addr node2_addr;
  Fil_addr node3_addr;

  ut_ad(mtr && node2 && node3 && base);
  ut_ad(base != node2);
  ut_ad(base != node3);
  ut_ad(node2 != node3);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node2, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node3, MTR_MEMO_PAGE_X_FIX));

  buf_ptr_get_fsp_addr(node2, &space, &node2_addr);
  buf_ptr_get_fsp_addr(node3, &space, &node3_addr);

  node1_addr = flst_get_prev_addr(node3, mtr);

  /* Set prev and next fields of node2 */
  flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
  flst_write_addr(node2 + FLST_NEXT, node3_addr, mtr);

  if (!srv_fil->addr_is_null(node1_addr)) {
    /* Update next field of node1 */
    node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);
    flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);
  } else {
    /* node3 was first in list: update first field in base */
    flst_write_addr(base + FLST_FIRST, node2_addr, mtr);
  }

  /* Set prev field of node3 */
  flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);

  /* Update len of base node */
  len = flst_get_len(base, mtr);
  mlog_write_ulint(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_remove(flst_base_node_t *base, flst_node_t *node2, mtr_t *mtr) {
  ulint len;
  space_id_t space;
  flst_node_t *node1;
  flst_node_t *node3;
  Fil_addr node2_addr;

  ut_ad(mtr && node2 && base);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node2, MTR_MEMO_PAGE_X_FIX));

  buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

  auto node1_addr = flst_get_prev_addr(node2, mtr);
  auto node3_addr = flst_get_next_addr(node2, mtr);

  if (!srv_fil->addr_is_null(node1_addr)) {

    /* Update next field of node1 */

    if (node1_addr.m_page_no == node2_addr.m_page_no) {

      node1 = Rec(node2).page_align() + node1_addr.m_boffset;
    } else {
      node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);
    }

    ut_ad(node1 != node2);

    flst_write_addr(node1 + FLST_NEXT, node3_addr, mtr);
  } else {
    /* node2 was first in list: update first field in base */
    flst_write_addr(base + FLST_FIRST, node3_addr, mtr);
  }

  if (!srv_fil->addr_is_null(node3_addr)) {
    /* Update prev field of node3 */

    if (node3_addr.m_page_no == node2_addr.m_page_no) {

      node3 = Rec(node2).page_align() + node3_addr.m_boffset;
    } else {
      node3 = fut_get_ptr(space, node3_addr, RW_X_LATCH, mtr);
    }

    ut_ad(node2 != node3);

    flst_write_addr(node3 + FLST_PREV, node1_addr, mtr);
  } else {
    /* node2 was last in list: update last field in base */
    flst_write_addr(base + FLST_LAST, node1_addr, mtr);
  }

  /* Update len of base node */
  len = flst_get_len(base, mtr);
  ut_ad(len > 0);

  mlog_write_ulint(base + FLST_LEN, len - 1, MLOG_4BYTES, mtr);
}

void flst_cut_end(flst_base_node_t *base, flst_node_t *node2, ulint n_nodes, mtr_t *mtr) {
  space_id_t space;
  flst_node_t *node1;
  Fil_addr node2_addr;
  ulint len;

  ut_ad(mtr && node2 && base);
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node2, MTR_MEMO_PAGE_X_FIX));
  ut_ad(n_nodes > 0);

  buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

  auto node1_addr = flst_get_prev_addr(node2, mtr);

  if (!srv_fil->addr_is_null(node1_addr)) {

    /* Update next field of node1 */

    if (node1_addr.m_page_no == node2_addr.m_page_no) {

      node1 = Rec(node2).page_align() + node1_addr.m_boffset;
    } else {
      node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);
    }

    flst_write_addr(node1 + FLST_NEXT, fil_addr_null, mtr);
  } else {
    /* node2 was first in list: update the field in base */
    flst_write_addr(base + FLST_FIRST, fil_addr_null, mtr);
  }

  flst_write_addr(base + FLST_LAST, node1_addr, mtr);

  /* Update len of base node */
  len = flst_get_len(base, mtr);
  ut_ad(len >= n_nodes);

  mlog_write_ulint(base + FLST_LEN, len - n_nodes, MLOG_4BYTES, mtr);
}

void flst_truncate_end(flst_base_node_t *base, flst_node_t *node2, ulint n_nodes, mtr_t *mtr) {
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mtr->memo_contains_page(node2, MTR_MEMO_PAGE_X_FIX));

  if (n_nodes == 0) {

    ut_ad(srv_fil->addr_is_null(flst_get_next_addr(node2, mtr)));

    return;
  }

  space_id_t space;
  Fil_addr node2_addr;

  buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

  /* Update next field of node2 */
  flst_write_addr(node2 + FLST_NEXT, fil_addr_null, mtr);

  flst_write_addr(base + FLST_LAST, node2_addr, mtr);

  /* Update len of base node */
  const auto len = flst_get_len(base, mtr);
  ut_ad(len >= n_nodes);

  mlog_write_ulint(base + FLST_LEN, len - n_nodes, MLOG_4BYTES, mtr);
}

bool flst_validate(const flst_base_node_t *base, mtr_t *mtr1) {
  ut_ad(mtr1->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));

  /* We use two mini-transaction handles: the first is used to
  lock the base node, and prevent other threads from modifying the
  list. The second is used to traverse the list. We cannot run the
  second mtr without committing it at times, because if the list
  is long, then the x-locked pages could fill the buffer resulting
  in a deadlock. */

  /* Find out the space id */
  space_id_t space;
  Fil_addr base_addr;

  buf_ptr_get_fsp_addr(base, &space, &base_addr);

  auto len = flst_get_len(base, mtr1);
  auto node_addr = flst_get_first(base, mtr1);

  for (ulint i = 0; i < len; ++i) {
    mtr_t mtr2;

    mtr2.start();

    auto node = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr2);
    node_addr = flst_get_next_addr(node, &mtr2);

    /* Commit mtr2 each round to prevent buffer becoming full */
    mtr2.commit();
  }

  ut_a(srv_fil->addr_is_null(node_addr));

  node_addr = flst_get_last(base, mtr1);

  for (ulint i = 0; i < len; ++i) {
    mtr_t mtr2;

    mtr2.start();

    auto node = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr2);
    node_addr = flst_get_prev_addr(node, &mtr2);

    /* Commit mtr2 each round to prevent buffer becoming full */
    mtr2.commit();
  }

  ut_a(srv_fil->addr_is_null(node_addr));

  return true;
}

void flst_print(const flst_base_node_t *base, mtr_t *mtr) {
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));

  auto frame = Rec((byte *)base).page_align();
  auto len = flst_get_len(base, mtr);

  log_info(std::format(
    "FILE-BASED LIST:\n"
    "Base node in space {} page {} byte offset {}; len {}",
    page_get_space_id(frame),
    page_get_page_no(frame),
    page_offset(base),
    len
  ));
}
