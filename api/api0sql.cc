/***********************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include "ib0config.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "api0api.h"
#include "dict0dict.h"
#include "pars0pars.h"
#include "que0que.h"
#include "trx0roll.h"

/**
 * Function to parse ib_exec_sql() and ib_exec_ddl_sql() args.
 *
 * @param n_args  in: no. of args
 * @param ap      in: arg list
 *
 * @return        own: info struct
 */
static pars_info_t *ib_exec_vsql(int n_args, va_list ap) {
  auto info = pars_info_create();

  for (int i = 0; i < n_args; ++i) {
    auto type = static_cast<ib_col_type_t>(va_arg(ap, int));

    switch (type) {
      case IB_CHAR:
      case IB_VARCHAR: {
        auto n = va_arg(ap, const char *);
        auto v = va_arg(ap, const char *);
        auto prefix = *n;

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
        auto l = va_arg(ap, ulint);
        auto s = va_arg(ap, ulint);
        auto n = va_arg(ap, const char *);

        auto prtype = s ? 0 : DATA_UNSIGNED;
        auto p = mem_heap_alloc(info->m_heap, l);

        switch (l) {
          case 1: {
            byte v = va_arg(ap, int);
            mach_write_int_type(p, (byte *)&v, l, s);
            break;
          }
          case 2: {
            uint16_t v = va_arg(ap, int);
            mach_write_int_type(p, (byte *)&v, l, s);
            break;
          }
          case 4: {
            uint32_t v = va_arg(ap, uint32_t);
            mach_write_int_type(p, (byte *)&v, l, s);
            break;
          }
          case 8: {
            uint64_t v = va_arg(ap, uint64_t);
            mach_write_int_type(p, (byte *)&v, l, s);
            break;
          }
          default:
            ut_error;
        }
        pars_info_add_literal(info, n, p, l, DATA_INT, prtype);
        break;
      }
      case IB_SYS: {
        auto n = va_arg(ap, const char *);
        auto f = va_arg(ap, pars_user_func_cb_t);
        auto a = va_arg(ap, void *);
        pars_info_add_function(info, n, f, a);
        break;
      }
      default:
        /* FIXME: Do the other types too */
        ut_error;
    }
  }

 return info;
}

ib_err_t ib_exec_sql( const char *sql, ulint n_args, ...) {
  va_list ap;

  va_start(ap, n_args);

  auto info = ib_exec_vsql(n_args, ap);

  va_end(ap);

  /* We use the private SQL parser of Innobase to generate
  the query graphs needed to execute the SQL statement. */

  auto trx = trx_allocate_for_client(nullptr);
  auto success = trx_start(trx, ULINT_UNDEFINED);
  ut_a(success);
  trx->m_op_info = "exec client sql";

  /* Note that we've already acquired the dictionary mutex. */
  auto err = que_eval_sql(info, sql, true, trx);
  ut_a(err == DB_SUCCESS);

  if (err != DB_SUCCESS) {
    trx_general_rollback(trx, false, nullptr);
  } else {
    auto commit_err = trx_commit(trx);
    ut_a(commit_err == DB_SUCCESS);
  }

  trx->m_op_info = "";
  trx_free_for_client(trx);

  return err;
}

ib_err_t ib_exec_ddl_sql(const char *sql, ulint n_args, ...) {
  va_list ap;

  va_start(ap, n_args);

  auto info = ib_exec_vsql(n_args, ap);

  va_end(ap);

  /* We use the private SQL parser of Innobase to generate
  the query graphs needed to execute the SQL statement. */

  auto trx = trx_allocate_for_background();
  auto started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  trx->m_op_info = "exec client ddl sql";

  auto err = ib_schema_lock_exclusive((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS);

  /* Note that we've already acquired the dictionary mutex by
  setting reserve_dict_mutex to false. */
  err = que_eval_sql(info, sql, false, trx);
  ut_a(err == DB_SUCCESS);

  err = ib_schema_unlock((ib_trx_t)trx);
  ut_a(err == DB_SUCCESS || err == DB_SCHEMA_NOT_LOCKED);

  if (err != DB_SUCCESS) {
    trx_general_rollback(trx, false, nullptr);
  } else {
    auto commit_err = trx_commit(trx);
    ut_a(commit_err == DB_SUCCESS);
  }

  trx->m_op_info = "";
  trx_free_for_background(trx);

  return err;
}
