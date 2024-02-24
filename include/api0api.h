/**
Copyright (c) 2008, 2009 Innobase Oy. All rights reserved.
Copyright (c) 2008, 2009 Oracle. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

************************************************************************/
#ifndef INNOBASE_API_H
#define INNOBASE_API_H

#include "db0err.h"

#include "innodb.h"
#include <stdarg.h>

/**
Declare private functions that should not be visible in the public API
below, outside of API_BEGIN_INCLUDE/API_END_INCLUDE.
*************************************************************************/

/**
Execute arbitrary SQL using InnoDB's internal parser. The statement
is executed in a new transaction. Table name parameters must be prefixed
with a '$' symbol and variables with ':'
@return	DB_SUCCESS or error code */

ib_err_t
ib_exec_sql(
/*========*/
	const char*     sql,            /*!< in: sql to execute */
	ib_ulint_t	n_args,         /*!< in: no. of args */
	...);

/**
Execute arbitrary SQL using InnoDB's internal parser. The statement
is executed in a background transaction. It will lock the data
dictionary lock for the duration of the query.
@return	DB_SUCCESS or error code */

ib_err_t
ib_exec_ddl_sql(
/*============*/
	const char*	sql,		/*!< in: sql to execute */
	ib_ulint_t	n_args,		/*!< in: no. of args */
	...);

/**
Initialize the config system.
@return	DB_SUCCESS or error code */

ib_err_t
ib_cfg_init(void);
/*==============*/

/**
Shutdown the config system.
@return	DB_SUCCESS or error code */

ib_err_t
ib_cfg_shutdown(void);
/*==================*/

extern int srv_panic_status;
#define IB_CHECK_PANIC() do { if (srv_panic_status) return srv_panic_status; } while(0)

#endif /* INNOBASE_API_H */
