/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/blob0inline.h
The format of the inline store, blob0inline.cc: a field is a chain of pages of the index's leaf
segment, each with a header and a part of the field. For the store and for tests that look at it. */

#pragma once

#include "blob0store.h"
#include "fil0types.h"
#include "innodb0types.h"

/** The store's bytes of a reference. @{ */

/** The space of the first page. */
constexpr ulint BTR_EXTERN_SPACE_ID = 0;

/** The first page of the chain. */
constexpr ulint BTR_EXTERN_PAGE_NO = 4;

/** Where the part header is on the first page. */
constexpr ulint BTR_EXTERN_OFFSET = 8;

/* @} */

static_assert(BTR_EXTERN_OFFSET + 4 == BlobStore::ID_SIZE);

/** The header of a part, at FIL_PAGE_DATA. @{ */

/** The length of the part on this page. */
constexpr ulint BTR_BLOB_HDR_PART_LEN = 0;

/** The next page of the chain, FIL_NULL on the last. */
constexpr ulint BTR_BLOB_HDR_NEXT_PAGE_NO = 4;

constexpr ulint BTR_BLOB_HDR_SIZE = 8;

/* @} */

/** The most of a field a page holds. */
constexpr ulint BTR_BLOB_PART_MAX = UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END;
