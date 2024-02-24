/***********************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.

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
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>


#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "dict0dict.h"
#include "pars0pars.h"
#include "que0que.h"
#include "trx0roll.h"
#include "api0api.h"

static int				api_sql_enter_func_enabled = 0;
#define UT_DBG_ENTER_FUNC_ENABLED	api_sql_enter_func_enabled

/*********************************************************************//**
Function to parse ib_exec_sql() and ib_exec_ddl_sql() args.
@return	own: info struct */
static
pars_info_t*
ib_exec_vsql(
/*=========*/
	int		n_args,		/*!< in: no. of args */
	va_list		ap)		/*!< in: arg list */
{
	int		i;
	pars_info_t*    info;

	UT_DBG_ENTER_FUNC;

	info = pars_info_create();

	for (i = 0; i < n_args; ++i) {
		ib_col_type_t   type;

		type = va_arg(ap, ib_col_type_t);

		switch (type) {
		case IB_CHAR:
		case IB_VARCHAR: {
			const char*     n;
			const char*     v;
			char		prefix;

			n = va_arg(ap, const char *);
			v = va_arg(ap, const char *);

			prefix = *n;
			ut_a(prefix == ':' || prefix == '$');
			++n;

			if (prefix == '$') {
				pars_info_add_id(info, n, v);
			} else {
				pars_info_add_str_literal(info, n, v);
			}
			break;
		}
		case IB_INT: {
			byte*		p;	/* dest buffer */
			ulint		l;	/* length */
			ulint		s;	/* TRUE if signed integer */
			const char*     n;	/* literal name */
			ulint		prtype;

			l = va_arg(ap, ib_ulint_t);
			s = va_arg(ap, ib_ulint_t);
			n = va_arg(ap, const char *);

			prtype = s ? 0 : DATA_UNSIGNED;
			p = mem_heap_alloc(info->heap, l);

			switch (l) {
			case 1: {
				byte	v;

				v = va_arg(ap, int);
				mach_write_int_type(p, (byte*)&v, l, s);
				break;
			}
			case 2: {
				ib_uint16_t     v;

				v = va_arg(ap, int);
				mach_write_int_type(p, (byte*)&v, l, s);
				break;
			}
			case 4: {
				ib_uint32_t     v;

				v = va_arg(ap, ib_uint32_t);
				mach_write_int_type(p, (byte*)&v, l, s);
				break;
			}
			case 8: {
				ib_uint64_t     v;

				v = va_arg(ap, ib_uint64_t);
				mach_write_int_type(p, (byte*)&v, l, s);
				break;
			}
			default:
				ut_error;
			}
			pars_info_add_literal(info, n, p, l, DATA_INT, prtype);
			break;
		}
		case IB_SYS: {
			const char*             n;
			pars_user_func_cb_t     f;
			void*                   a;

			n = va_arg(ap, const char *);
			f = va_arg(ap, pars_user_func_cb_t);
			a = va_arg(ap, void*);
			pars_info_add_function(info, n, f, a);
			break;
		}
		default:
			/* FIXME: Do the other types too */
			ut_error;
		}
	}

	return(info);
}

/*********************************************************************//**
Execute arbitrary SQL using InnoDB's internal parser. The statement
is executed in a new transaction. Table name parameters must be prefixed
with a '$' symbol and variables with ':'
@return	DB_SUCCESS or error code */

ib_err_t
ib_exec_sql(
/*========*/
	const char*     sql,            /*!< in: sql to execute */
	ib_ulint_t	n_args,         /*!< in: no. of args */
	...)
{
	va_list         ap;
	trx_t*          trx;
	ib_err_t	err;
	pars_info_t*    info;

	UT_DBG_ENTER_FUNC;

	va_start(ap, n_args);

	info = ib_exec_vsql(n_args, ap);

	va_end(ap);

	/* We use the private SQL parser of Innobase to generate
	the query graphs needed to execute the SQL statement. */

	trx = trx_allocate_for_client(NULL);
	err = trx_start(trx, ULINT_UNDEFINED);
	ut_a(err == DB_SUCCESS);
	trx->op_info = "exec client sql";

	dict_mutex_enter();
	/* Note that we've already acquired the dictionary mutex. */
	err = que_eval_sql(info, sql, FALSE, trx);
	ut_a(err == DB_SUCCESS);
	dict_mutex_exit();

	if (err != DB_SUCCESS) {
		trx_rollback(trx, FALSE, NULL);
	} else {
		trx_commit(trx);
	}

	trx->op_info = "";
	trx_free_for_client(trx);

	return(err);
}

/*********************************************************************//**
Execute arbitrary SQL using InnoDB's internal parser. The statement
is executed in a background transaction. It will lock the data
dictionary lock for the duration of the query.
@return	DB_SUCCESS or error code */

ib_err_t
ib_exec_ddl_sql(
/*============*/
	const char*	sql,		/*!< in: sql to execute */
	ib_ulint_t	n_args,		/*!< in: no. of args */
	...)
{
	va_list         ap;
	trx_t*          trx;
	ib_err_t	err;
	pars_info_t*    info;
	int		started;

	UT_DBG_ENTER_FUNC;

	va_start(ap, n_args);

	info = ib_exec_vsql(n_args, ap);

	va_end(ap);

	/* We use the private SQL parser of Innobase to generate
	the query graphs needed to execute the SQL statement. */

	trx = trx_allocate_for_background();
	started = trx_start(trx, ULINT_UNDEFINED);
	ut_a(started);
	trx->op_info = "exec client ddl sql";

	err = ib_schema_lock_exclusive((ib_trx_t) trx);
	ut_a(err == DB_SUCCESS);

	/* Note that we've already acquired the dictionary mutex by
	setting reserve_dict_mutex to FALSE. */
	err = que_eval_sql(info, sql, FALSE, trx);
	ut_a(err == DB_SUCCESS);

	ib_schema_unlock((ib_trx_t) trx);

	if (err != DB_SUCCESS) {
		trx_rollback(trx, FALSE, NULL);
	} else {
		trx_commit(trx);
	}

	trx->op_info = "";
	trx_free_for_background(trx);

	return(err);
}

