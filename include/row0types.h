/*****************************************************************************

Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.

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

/**************************************************//**
@file include/row0types.h
Row operation global types

Created 12/27/1996 Heikki Tuuri
*******************************************************/

#ifndef row0types_h
#define row0types_h

typedef struct plan_struct plan_t;

typedef	struct upd_struct upd_t;

typedef struct upd_field_struct upd_field_t;

typedef	struct upd_node_struct upd_node_t;

typedef	struct del_node_struct del_node_t;

typedef	struct ins_node_struct ins_node_t;

typedef struct sel_node_struct	sel_node_t;

typedef struct open_node_struct	open_node_t;

typedef struct fetch_node_struct fetch_node_t;

typedef struct row_printf_node_struct	row_printf_node_t;
typedef struct sel_buf_struct	sel_buf_t;

typedef	struct undo_node_struct undo_node_t;

typedef	struct purge_node_struct purge_node_t;

typedef struct row_ext_struct row_ext_t;

typedef void* table_handle_t;

typedef struct row_prebuilt_struct row_prebuilt_t;

/* Insert node types */
typedef enum ib_ins_mode_enum {
					/* The first two modes are only
					used by the internal SQL parser */
	INS_SEARCHED,			/* INSERT INTO ... SELECT ... */
	INS_VALUES,			/* INSERT INTO ... VALUES ... */
	INS_DIRECT			/* Insert the row directly */
} ib_ins_mode_t;

/* Node execution states */
typedef enum ib_ins_state_enum {
	INS_NODE_SET_IX_LOCK = 1,	/* we should set an IX lock on table */
	INS_NODE_ALLOC_ROW_ID,		/* row id should be allocated */
	INS_NODE_INSERT_ENTRIES		/* index entries should be built and
					inserted */
} ib_ins_state_t;

/* Flags for positioning the cursor. */
typedef enum ib_cur_op_enum {
	ROW_SEL_MOVETO,			/* required when openeing a cursor */
	ROW_SEL_NEXT,			/* move to next record */
	ROW_SEL_PREV			/* move to previous record */
} ib_cur_op_t;

/* Various match modes when fetching a record from a table. */
typedef enum ib_match_enum {
	ROW_SEL_DEFAULT,		/* closest match possible */
	ROW_SEL_EXACT,			/* search using a complete key value */
	ROW_SEL_EXACT_PREFIX		/* search using a key prefix which
					must match to rows: the prefix may
					contain an incomplete field (the
					last field in prefix may be just
					a prefix of a fixed length column) */
} ib_match_t;

#endif
