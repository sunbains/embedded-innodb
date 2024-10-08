/*****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
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

/********************************************************************/ /**
 @file include/rem0types.h
 Record manager global types

 Created 5/30/1994 Heikki Tuuri
 *************************************************************************/

#pragma once

using rec_t = byte;

/* We define the physical record simply as an array of bytes */
/* Maximum values for various fields (for non-blob tuples) */
constexpr ulint REC_MAX_N_FIELDS = 1024 - 1;
constexpr ulint REC_MAX_HEAP_NO = 2 * 8192 - 1;
constexpr ulint REC_MAX_N_OWNED = 16 - 1;

/** REC_MAX_INDEX_COL_LEN is measured in bytes and is the maximum
indexed column length (or indexed prefix length). It is set to 3*256,
so that one can create a column prefix index on 256 characters of a
TEXT or VARCHAR column also in the UTF-8 charset. In that charset,
a character may take at most 3 bytes.

This constant MUST NOT BE CHANGED, or the compatibility of InnoDB data
files will be at risk! */
constexpr ulint REC_MAX_INDEX_COL_LEN = 768;
