/*****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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
 @file include/btr0types.h
 The index tree general types

 Created 2/17/1996 Heikki Tuuri
 *************************************************************************/

#pragma once

#include "page0types.h"
#include "rem0types.h"
#include "sync0rw.h"

/** Persistent cursor */
struct Btree_pcursor;

/** B-tree cursor */
struct Btree_cursor;

/** The size of a reference to data stored on a different page.
The reference is stored at the end of the prefix of the field
in the index record. */
constexpr ulint BTR_EXTERN_FIELD_REF_SIZE = 20;

/** A BLOB field reference full of zero, for use in assertions and tests.
Initially, BLOB field references are set to zero, in
dtuple_convert_big_rec(). */
extern const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE];

/** Maximum record size which can be stored on a page, without using the
special big record storage structure */
constexpr ulint BTR_PAGE_MAX_REC_SIZE = UNIV_PAGE_SIZE / 2 - 200;

constexpr ulint BTR_TOTAL_SIZE = 2;

constexpr ulint BTR_N_LEAF_PAGES = 1;

/* BTR_INSERT, BTR_DELETE and BTR_DELETE_MARK are mutually exclusive. */

/** If this is ORed to btr_latch_mode, it means that the search tuple
will be inserted to the index, at the searched position. */
constexpr size_t BTR_INSERT = 512;

/** This flag ORed to btr_latch_mode says that we do the search in query
optimization */
constexpr size_t BTR_ESTIMATE = 1024;

/** Try to delete mark the record at the searched position using the
insert/delete buffer when the record is not in the buffer pool. */
constexpr size_t BTR_DELETE_MARK = 2048;

/** Try to purge the record at the searched position using the insert/delete
buffer when the record is not in the buffer pool. */
constexpr size_t BTR_DELETE = 4096;

/** In the case of BTR_SEARCH_LEAF or BTR_MODIFY_LEAF, the caller is
already holding an S latch on the index tree */
constexpr size_t BTR_ALREADY_S_LATCHED = 8192;

/** In the case of BTR_MODIFY_TREE, the caller specifies the intention
to insert record only. It is used to optimize block->lock range.*/
constexpr size_t BTR_LATCH_FOR_INSERT = 16384;

/** In the case of BTR_MODIFY_TREE, the caller specifies the intention
to delete record only. It is used to optimize block->lock range.*/
constexpr size_t BTR_LATCH_FOR_DELETE = 32768;

/** In the case of BTR_MODIFY_LEAF, the caller intends to allocate or
free the pages of externally stored fields. */
constexpr size_t BTR_MODIFY_EXTERNAL = 65536;

#define BTR_LATCH_MODE_WITHOUT_FLAGS(latch_mode)                                                                              \
  ((latch_mode) & ~(BTR_INSERT | BTR_DELETE_MARK | BTR_DELETE | BTR_ESTIMATE | BTR_ALREADY_S_LATCHED | BTR_LATCH_FOR_INSERT | \
                    BTR_LATCH_FOR_DELETE | BTR_MODIFY_EXTERNAL))

/** @brief Maximum depth of a B-tree in InnoDB.

Note that this isn't a maximum as such; none of the tree operations
avoid producing trees bigger than this. It is instead a "max depth
that other code must work with", useful for e.g.  fixed-size arrays
that must store some information about each level in a tree. In other
words: if a B-tree with bigger depth than this is encountered, it is
not acceptable for it to lead to mysterious memory corruption, but it
is acceptable for the program to die with a clear assert failure. */
constexpr ulint BTR_MAX_DEPTH = 100;

/** Latching modes for btr_cur_search_to_nth_level(). */
enum btr_latch_mode {
  /** Search a record on a leaf page and S-latch it. */
  BTR_SEARCH_LEAF = RW_S_LATCH,

  /** (Prepare to) modify a record on a leaf page and X-latch it. */
  BTR_MODIFY_LEAF = RW_X_LATCH,

  /** Obtain no latches. */
  BTR_NO_LATCHES = RW_NO_LATCH,

  /** Start modifying the entire B-tree. */
  BTR_MODIFY_TREE = 33,

  /** Continue modifying the entire B-tree. */
  BTR_CONT_MODIFY_TREE = 34,

  /** Search the previous record. */
  BTR_SEARCH_PREV = 35,

  /** Modify the previous record. */
  BTR_MODIFY_PREV = 36
};
