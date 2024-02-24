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

/**********************************************************************
This is the base table class. In C++ terminology you can consider this
an abstract class that implements the generic functionality. The DDL
and DML functions implemented in this class are:
1) SELECT

2) DROP
3) TRUNCATE

This module also provides some helper functions for the derived class.

If you want to add another table to the test suite mt_drv just copy
one of the derived table class i.e.: mt_t1.c, mt_t2.c and make
appropriate changes to it. You'll need to then register that table
with the test suite by calling, say, register_t3_table() from
register_test_table() in mt_drv.c
***********************************************************************/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "test0aux.h"
#include "ib_mt_drv.h"
#include "ib_mt_base.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif


/*********************************************************************
Open a table and return a cursor for the table. */
ib_err_t
open_table(
/*=======*/
	const char*	dbname,		/*!< in: database name */
	const char*	name,		/*!< in: table name */
	ib_trx_t	ib_trx,		/*!< in: transaction */
	ib_crsr_t*	crsr)		/*!< out: innodb cursor */
{
	ib_err_t	err = DB_SUCCESS;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
	err = ib_cursor_open_table(table_name, ib_trx, crsr);

	return(err);
}

#if 0
/**********************************************************************
SELECT * from t */
static
ib_err_t
select_base(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	ib_err_t	err;
	ib_err_t	err2;
	ib_tpl_t	tpl = NULL;
	ib_crsr_t	crsr = NULL;
	cb_args_t*	cb_arg = (cb_args_t *)arg;
	tbl_class_t*	tbl = cb_arg->tbl;

	//fprintf(stderr, "base: SELECT\n");
	err = open_table(tbl->db_name, tbl->name, cb_arg->trx, &crsr);
	if (err != DB_SUCCESS) {
		goto err_exit;
	}

	err = ib_cursor_lock(crsr, IB_LOCK_IS);
	if (err != DB_SUCCESS) {
		goto err_exit;
	}

	tpl = ib_clust_read_tuple_create(crsr);
	if (tpl == NULL) {
		err = DB_OUT_OF_MEMORY;
		goto err_exit;
	}

	err = ib_cursor_first(crsr);
	if (err != DB_SUCCESS) {
		goto err_exit;
	}

	while (1) {
		err = ib_cursor_read_row(crsr, tpl);
		if (err != DB_SUCCESS) {
			goto err_exit;
		}
		update_err_stats(cb_arg->err_st, err);

		if (cb_arg->print_res) {
			print_tuple(stderr, tpl);
		}

		err = ib_cursor_next(crsr);
		if (err != DB_SUCCESS) {
			goto err_exit;
		}
		tpl = ib_tuple_clear(tpl);
		if (tpl == NULL) {
			err = DB_OUT_OF_MEMORY;
			goto err_exit;
		}
	}

err_exit:
	update_err_stats(cb_arg->err_st, err);

	if (tpl != NULL) {
		ib_tuple_delete(tpl);
	}

	if (crsr != NULL) {
		err2 = ib_cursor_close(crsr);
		assert(err2 == DB_SUCCESS);
		crsr = NULL;
	}

	return(err);
}
#endif

/**********************************************************************
stub for SELECT * from t */
ib_err_t
select_stub(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//usleep(100000);
	return(DB_SUCCESS);

}
/**********************************************************************
DROP TABLE t */
static
ib_err_t
drop_base(
/*======*/
	void*	arg)	/*!< in: arguments for callback */
{
	ib_err_t	err;
	ib_trx_t	ib_trx;
	char		table_name[IB_MAX_TABLE_NAME_LEN];
	cb_args_t*	cb_arg = (cb_args_t *)arg;
	tbl_class_t*	tbl = cb_arg->tbl;

	//fprintf(stderr, "base: DROP\n");
	snprintf(table_name, sizeof(table_name), "%s/%s",
		 tbl->db_name, tbl->name);

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_drop(ib_trx, table_name);
	/* We can get the DB_TABLESPACE_DELETED if the table is already
	on the background delete queue. */
	assert(err == DB_SUCCESS
	       || err == DB_TABLE_NOT_FOUND
	       || err == DB_TABLESPACE_DELETED);

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	update_err_stats(cb_arg->err_st, err);
	return(err);
}

/**********************************************************************
TRUNCATE TABLE t */
static
ib_err_t
truncate_base(
/*==========*/
	void*	arg)	/*!< in: arguments for callback */
{
	ib_err_t	err;
	ib_id_t		table_id;
	char		table_name[IB_MAX_TABLE_NAME_LEN];
	cb_args_t*	cb_arg = (cb_args_t *)arg;
	tbl_class_t*	tbl = cb_arg->tbl;

	//fprintf(stderr, "base: TRUNCATE\n");
	snprintf(table_name, sizeof(table_name), "%s/%s",
		 tbl->db_name, tbl->name);

	err = ib_table_truncate(table_name, &table_id);
	update_err_stats(cb_arg->err_st, err);

	return(err);
}

/**********************************************************************
Function stub */
static
ib_err_t
update_base(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//fprintf(stderr, "base: UPDATE\n");
	usleep(100000);
	return(DB_SUCCESS);
}

/**********************************************************************
Function stub */
static
ib_err_t
insert_base(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//fprintf(stderr, "base: INSERT\n");
	usleep(100000);
	return(DB_SUCCESS);
}

/**********************************************************************
Function stub */
static
ib_err_t
delete_base(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//fprintf(stderr, "base: DELETE\n");
	usleep(100000);
	return(DB_SUCCESS);
}

/**********************************************************************
Function stub */
static
ib_err_t
create_base(
/*========*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//fprintf(stderr, "base: CREATE\n");
	usleep(100000);
	return(DB_SUCCESS);
}

/**********************************************************************
Function stub */
static
ib_err_t
alter_base(
/*=======*/
	void*	arg)	/*!< in: arguments for callback */
{
	(void)arg;
	//fprintf(stderr, "base: ALTER\n");
	usleep(100000);
	return(DB_SUCCESS);
}

/**********************************************************************
Function to register this table class with mt_drv test suite */
void
register_base_table(
/*================*/
	tbl_class_t*	tbl)	/*!< in/out: table class to register */
{
	assert(tbl != NULL);

	strcpy((char *)tbl->name, "dummy");

	//tbl->dml_fn[DML_OP_TYPE_SELECT] = select_base;
	tbl->dml_fn[DML_OP_TYPE_SELECT] = select_stub;
	tbl->dml_fn[DML_OP_TYPE_UPDATE] = update_base;
	tbl->dml_fn[DML_OP_TYPE_INSERT] = insert_base;
	tbl->dml_fn[DML_OP_TYPE_DELETE] = delete_base;

	tbl->ddl_fn[DDL_OP_TYPE_CREATE] = create_base;
	tbl->ddl_fn[DDL_OP_TYPE_DROP] = drop_base;
	tbl->ddl_fn[DDL_OP_TYPE_ALTER] = alter_base;
	tbl->ddl_fn[DDL_OP_TYPE_TRUNCATE] = truncate_base;
}
