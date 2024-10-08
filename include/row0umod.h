/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/row0umod.h
Undo modify of a row

Created 2/27/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "data0data.h"
#include "dict0types.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "row0types.h"
#include "trx0types.h"

/** Undoes a modify operation on a row of a table.
@param[in,out] node             Row undo node.
@param[in,out] thr              Query thread.
@return	DB_SUCCESS or error code */
db_err row_undo_mod(Undo_node *node, que_thr_t *thr);
