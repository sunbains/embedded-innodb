/**
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

/** @file include/usr0sess.h
Sessions

Created 6/25/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "data0data.h"
#include "que0types.h"
#include "rem0rec.h"
#include "srv0srv.h"
#include "trx0types.h"
#include "usr0types.h"
#include "ut0byte.h"

/** Opens a session.
@return	own: session object */

sess_t *sess_open(void);
/** Closes a session, freeing the memory occupied by it. */

void sess_close(sess_t *sess); /*!< in, own: session object */

/** The session handle. All fields are protected by the kernel mutex */
struct sess_struct {
  ulint state; /*!< state of the session */
  trx_t *trx;  /*!< transaction object permanently
               assigned for the session: the
               transaction instance designated by the
               trx id changes, but the memory
               structure is preserved */
  UT_LIST_BASE_NODE_T_EXTERN(que_t, graphs)
  graphs; /*!< query graphs belonging to this
          session */
};

/* Session states */
#define SESS_ACTIVE 1
#define SESS_ERROR \
  2 /* session contains an error message                                       \
    which has not yet been communicated                                        \
    to the client */
