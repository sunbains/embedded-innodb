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

/**************************************************/ /**
 @file include/row0types.h
 Row operation global types

 Created 12/27/1996 Heikki Tuuri
 *******************************************************/

#pragma once

struct Plan;
struct Prebuilt;
struct Undo_node;
struct Insert_node;
struct Row_purge;

struct upd_t;
struct upd_field_t;
struct upd_node_t;

struct sel_buf_t;

using table_handle_t = void *;
using ins_node_t = Insert_node;
using row_ext_t = struct row_ext_struct;
using del_node_t = struct del_node_struct;
using open_node_t = struct open_node_struct;
using fetch_node_t = struct fetch_node_struct;
using row_printf_node_t = struct row_printf_node_struct;

/* Insert node types */
enum ib_ins_mode_t {
  /* The first two modes are only used by the internal SQL parser */

  /** INSERT INTO ... SELECT ... */
  INS_SEARCHED,

  /** INSERT INTO ... VALUES ... */
  INS_VALUES,

  /** Insert the row directly */
  INS_DIRECT
};

/* Node execution states */
enum ib_ins_state_t {

  /** We should set an IX lock on table */
  INS_NODE_SET_IX_LOCK = 1,

  /** Row id should be allocated */
  INS_NODE_ALLOC_ROW_ID,

  /** Index entries should be built and inserted */
  INS_NODE_INSERT_ENTRIES
};

/* Flags for positioning the cursor. */
enum ib_cur_op_t {
  /** Undefined cursor operation */
  ROW_SEL_UNDEFINED,

  /** Required when openeing a cursor */
  ROW_SEL_MOVETO,

  /** Move to next record */
  ROW_SEL_NEXT,

  /** Move to previous record */
  ROW_SEL_PREV
};

/* Various match modes when fetching a record from a table. */
enum ib_match_t {
  /** Closest match possible */
  ROW_SEL_DEFAULT,

  /** Search using a complete key value */
  ROW_SEL_EXACT,

  /** Search using a key prefix which must match to rows:
  the prefix may contain an incomplete field (the last
  field in prefix may be just a prefix of a fixed length column) */
  ROW_SEL_EXACT_PREFIX
};
