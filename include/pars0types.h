/*****************************************************************************
Copyright (c) 1998, 2009, Innobase Oy. All Rights Reserved.
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
 @file include/pars0types.h
 SQL parser global types

 Created 1/11/1998 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"
#include "ut0lst.h"

struct ParsUserFunc;
struct ParsBoundLit;
struct ParsBoundId;
struct SymNode;
struct ParsResWord;
struct FuncNode;
struct OrderNode;
struct ProcNode;
struct ElsifNode;
struct IfNode;
struct WhileNode;
struct ForNode;
struct ExitNode;
struct ReturnNode;
struct AssignNode;
struct ColAssignNode;

using sym_node_list_t = UT_LIST_BASE_NODE_T_EXTERN(SymNode, col_var_list);
using func_node_list_t = UT_LIST_BASE_NODE_T_EXTERN(FuncNode, func_node_list);
