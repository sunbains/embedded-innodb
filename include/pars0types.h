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

using pars_user_func_t = struct pars_user_func_struct;
using pars_bound_lit_t = struct pars_bound_lit_struct;
using pars_bound_id_t = struct pars_bound_id_struct;
using sym_node_t = struct sym_node_struct;
using pars_res_word_t = struct pars_res_word_struct;
using func_node_t = struct func_node_struct;
using order_node_t = struct order_node_struct;
using proc_node_t = struct proc_node_struct;
using elsif_node_t = struct elsif_node_struct;
using if_node_t = struct if_node_struct;
using while_node_t = struct while_node_struct;
using for_node_t = struct for_node_struct;
using exit_node_t = struct exit_node_struct;
using return_node_t = struct return_node_struct;
using assign_node_t = struct assign_node_struct;
using col_assign_node_t = struct col_assign_node_struct;

using sym_node_list_t = UT_LIST_BASE_NODE_T_EXTERN(sym_node_t, col_var_list);
using func_node_list_t = UT_LIST_BASE_NODE_T_EXTERN(func_node_t, func_node_list);
