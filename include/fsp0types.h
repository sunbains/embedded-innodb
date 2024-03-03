/*****************************************************************************

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

/******************************************************
@file include/fsp0types.h
File space management types

Created May 26, 2009 Vasil Dimov
*******************************************************/

#pragma once
#include "innodb0types.h"

#include "fil0fil.h"

constexpr space_id_t SYS_TABLESPACE = 0;

/** @name Flags for inserting records in order If records are inserted in
order, there are the following flags to tell this (their type is made byte
for the compiler to warn if direction and hint parameters are switched in
fseg_alloc_free_page) */
/* @{ */

/** Alphabetically upwards */
constexpr byte FSP_UP = 111;

/** Alphabetically downwards */
constexpr byte FSP_DOWN = 112;

/** No order */
constexpr byte FSP_NO_DIR = 113;

/* @} */

/** File space extent size (one megabyte) in pages */
constexpr ulint FSP_EXTENT_SIZE = 1 << (20 - UNIV_PAGE_SIZE_SHIFT);

/** On a page of any file segment, data may be put starting from this offset */
constexpr auto FSEG_PAGE_DATA = FIL_PAGE_DATA;

/** @name File segment header
The file segment header points to the inode describing the file segment. */
/* @{ */
/** Data type for file segment header */
using fseg_header_t = byte;;

/** Space id of the inode */
constexpr ulint FSEG_HDR_SPACE =  0;

/** Page number of the inode */
constexpr ulint FSEG_HDR_PAGE_NO =  4;

/** Byte offset of the inode */
constexpr ulint FSEG_HDR_OFFSET =  8;

/** Length of the file system header, in bytes */
constexpr ulint FSEG_HEADER_SIZE =  10;

/* @} */

/** Flags for fsp_reserve_free_extents @{ */
constexpr ulint FSP_NORMAL = 1000000;
constexpr ulint FSP_UNDO = 2000000;
constexpr ulint FSP_CLEANING = 3000000;
/* @} */

/** Number of pages described in a single descriptor page: currently each page
description takes less than 1 byte; a descriptor page is repeated every
this many file pages */
// constexpr auto XDES_DESCRIBED_PER_PAGE = UNIV_PAGE_SIZE;

/** @name The space low address page map
The pages at FSP_XDES_OFFSET is repeated every XDES_DESCRIBED_PER_PAGE pages
in every tablespace. */

/* @{ */
/** extent descriptor */
constexpr ulint FSP_XDES_OFFSET =  0;

/** The number of reserved pages in a fragment extent. */
constexpr ulint FSP_XDES_RESERVED = 0;

/** In every tablespace */
constexpr ulint FSP_FIRST_INODE_PAGE_NO =  1;

/* The following pages exist in the system tablespace SYS_TABLESPACE. */

/** Transaction system header, in tablespace SYS_TABLESPACE */
constexpr page_no_t FSP_TRX_SYS_PAGE_NO =  2;

/** First rollback segment page, in tablespace SYS_TABLESPACWE */
constexpr page_no_t FSP_FIRST_RSEG_PAGE_NO =  3;

/** data dictionary header  page, in tablespace SYS_TABLESPACE */
constexpr page_no_t FSP_DICT_HDR_PAGE_NO =  4;

/* @} */
