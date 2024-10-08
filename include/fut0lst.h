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

/** @file include/fut0lst.h
File-based list utilities

Created 11/28/1995 Heikki Tuuri
***********************************************************************/

#pragma once

#include "innodb0types.h"

// #include "fil0fil.h"
#include "buf0buf.h"
#include "fut0fut.h"
#include "mtr0log.h"
#include "mtr0mtr.h"

struct Fil_addr;

/* The C 'types' of base node and list node: these should be used to
write self-documenting code. Of course, the sizeof macro cannot be
applied to these types! */

using flst_node_t = byte;
using flst_base_node_t = byte;

/** The physical size of a list base node in bytes */
constexpr ulint FLST_BASE_NODE_SIZE = 4 + 2 * FIL_ADDR_SIZE;

/* We define the field offsets of a node for the list */
/** 6-byte address of the previous list element; the page part of address is FIL_NULL, if no previous element */
constexpr ulint FLST_PREV = 0;

/* 6-byte address of the next list element; the page part of address is FIL_NULL, if no next element */
constexpr auto FLST_NEXT = FIL_ADDR_SIZE;

/* We define the field offsets of a base node for the list */

/** 32-bit list length field */
constexpr ulint FLST_LEN = 0;

/** 6-byte address of the first element of the list; undefined if empty list */
constexpr ulint FLST_FIRST = 4;

/** 6-byte address of the last element of the list; undefined if empty list */
constexpr ulint FLST_LAST = 4 + FIL_ADDR_SIZE;

/** The physical size of a list node in bytes */
constexpr ulint FLST_NODE_SIZE = 2 * FIL_ADDR_SIZE;

/** Adds a node as the last node in a list. */
void flst_add_last(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node,      /** in: node to add */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Adds a node as the first node in a list. */
void flst_add_first(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node,      /** in: node to add */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Inserts a node after another in a list. */
void flst_insert_after(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node1,     /** in: node to insert after */
  flst_node_t *node2,     /** in: node to add */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Inserts a node before another in a list. */
void flst_insert_before(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node2,     /** in: node to insert */
  flst_node_t *node3,     /** in: node to insert before */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Removes a node. */
void flst_remove(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node2,     /** in: node to remove */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Cuts off the tail of the list, including the node given. The number of
nodes which will be removed must be provided by the caller, as this function
does not measure the length of the tail. */
void flst_cut_end(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node2,     /** in: first node to remove */
  ulint n_nodes,          /** in: number of nodes to remove,
                            must be >= 1 */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Cuts off the tail of the list, not including the given node. The number of
nodes which will be removed must be provided by the caller, as this function
does not measure the length of the tail. */
void flst_truncate_end(
  flst_base_node_t *base, /** in: pointer to base node of list */
  flst_node_t *node2,     /** in: first node not to remove */
  ulint n_nodes,          /** in: number of nodes to remove */
  mtr_t *mtr
); /** in: mini-transaction handle */

/** Validates a file-based list.
@return	true if ok */
bool flst_validate(
  const flst_base_node_t *base, /** in: pointer to base node of list */
  mtr_t *mtr1
); /** in: mtr */

/** Prints info of a file-based list. */
void flst_print(
  const flst_base_node_t *base, /** in: pointer to base node of list */
  mtr_t *mtr
); /** in: mtr */

/** Writes a file address. */
inline void flst_write_addr(
  fil_faddr_t *faddr, /** in: pointer to file faddress */
  Fil_addr addr,    /** in: file address */
  mtr_t *mtr
) /** in: mini-transaction handle */
{
  ut_ad(faddr && mtr);
  ut_ad(mtr->memo_contains_page(faddr, MTR_MEMO_PAGE_X_FIX));
  ut_a(addr.m_page_no == FIL_NULL || addr.m_boffset >= FIL_PAGE_DATA);
  ut_a(ut_align_offset(faddr, UNIV_PAGE_SIZE) >= FIL_PAGE_DATA);

  mlog_write_ulint(faddr + FIL_ADDR_PAGE, addr.m_page_no, MLOG_4BYTES, mtr);
  mlog_write_ulint(faddr + FIL_ADDR_BYTE, addr.m_boffset, MLOG_2BYTES, mtr);
}

/** Reads a file address.
@param[in] faddr                Pointer to file faddress
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	file address */
inline Fil_addr flst_read_addr(const fil_faddr_t *faddr, mtr_t *mtr) {
  Fil_addr addr;

  ut_ad(faddr && mtr);

  addr.m_page_no = mtr->read_ulint(faddr + FIL_ADDR_PAGE, MLOG_4BYTES);
  addr.m_boffset = mtr->read_ulint(faddr + FIL_ADDR_BYTE, MLOG_2BYTES);
  ut_a(addr.m_page_no == FIL_NULL || addr.m_boffset >= FIL_PAGE_DATA);
  ut_a(ut_align_offset(faddr, UNIV_PAGE_SIZE) >= FIL_PAGE_DATA);

  return addr;
}

/** Initializes a list base node.
@param[in] base                 Pointer to the base node
@param[in,out] mtr              Mini-transaction that covers the operation. */
inline void flst_init(flst_base_node_t *base, mtr_t *mtr) {
  ut_ad(mtr->memo_contains_page(base, MTR_MEMO_PAGE_X_FIX));

  mlog_write_ulint(base + FLST_LEN, 0, MLOG_4BYTES, mtr);
  flst_write_addr(base + FLST_FIRST, fil_addr_null, mtr);
  flst_write_addr(base + FLST_LAST, fil_addr_null, mtr);
}

/** Gets list length.
@param[in] base                 Pointer to base node
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	length */
inline ulint flst_get_len(const flst_base_node_t *base, mtr_t *mtr) {
  return mtr->read_ulint(base + FLST_LEN, MLOG_4BYTES);
}

/** Gets list first node address.
@param[in] base                 Pointer to the base node
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	file address */
inline Fil_addr flst_get_first(const flst_base_node_t *base, mtr_t *mtr) {
  return flst_read_addr(base + FLST_FIRST, mtr);
}

/** Gets list last node address.
@param[in] base                 Pointer to the base node
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	file address */
inline Fil_addr flst_get_last(const flst_base_node_t *base, mtr_t *mtr) {
  return flst_read_addr(base + FLST_LAST, mtr);
}

/** Gets list next node address.
@param[in] node                 Pointer to the node
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	file address */
inline Fil_addr flst_get_next_addr(const flst_node_t *node, mtr_t *mtr) {
  return flst_read_addr(node + FLST_NEXT, mtr);
}

/** Gets list prev node address.
@param[in] node                 Pointer to the node node
@param[in,out] mtr              Mini-transaction that covers the operation.
@return	file address */
inline Fil_addr flst_get_prev_addr(const flst_node_t *node, mtr_t *mtr) {
  return flst_read_addr(node + FLST_PREV, mtr);
}
