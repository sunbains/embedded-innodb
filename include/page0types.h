/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/page0types.h
Index page routines

Created 2/2/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0types.h"

/** Eliminates a name collision on HP-UX */
#define page_t ib_page_t
/** Type of the index page */
using page_t = byte;

/** Index page cursor */
typedef struct page_cur_struct page_cur_t;
