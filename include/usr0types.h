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

/**************************************************/ /**
 @file include/usr0types.h
 Users and sessions global types

 Created 6/25/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"

#include "que0types.h"
#include "ut0lst.h"

struct trx_t;

/** Session states */
constexpr ulint SESS_ACTIVE = 1;

/** Session contains an error message which has not yet been
communicated to the client */
constexpr ulint SESS_ERROR = 2;

/** The session handle. All fields are protected by the kernel mutex */
struct sess_t {
  /** State of the session */
  ulint state;

  /** Transaction object permanently assigned for the session:
  the transaction instance designated by the trx id changes, but
  the memory structure is preserved */
  trx_t *trx;

  /** Query graphs belonging to this session */
  UT_LIST_BASE_NODE_T_EXTERN(que_t, graphs) graphs;
};

