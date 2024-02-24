/** Copyright (c) 2009 Innobase Oy. All rights reserved.
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

/* Simple single threaded test that does the equivalent of:
 Create a database
 CREATE TABLE t(c1 INT, c2 VARCHAR(10), PK(c1)); 
 FOR I IN 1 ... 10 STEP 2
   INSERT INTO t VALUES(I, CHAR('a' + I)); ...
 END FOR
 SELECT * FROM t;
 UPDATE t SET c1 = c1 / 2;
 SELECT * FROM t;
 DROP TABLE t;
 
 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#include "test0aux.h"

#define DATABASE	"test"
#define TABLE		"t"

/** Create an InnoDB database (sub-directory). */
static
ib_err_t
create_database(
	const char*	name)
{
	ib_bool_t	err;

	err = ib_database_create(name);
	assert(err == IB_TRUE);

	return(DB_SUCCESS);
}

/** CREATE TABLE t(c1 INT, c2 VARCHAR(10), PRIMARY KEY(c1); */
static
ib_err_t
create_table(
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table name */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	/* Pass a table page size of 0, ie., use default page size. */
	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0, 4);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c2", IB_VARCHAR, IB_COL_NONE, 0, 10);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "c1", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c1", 0);
	assert(err == DB_SUCCESS);

	err = ib_index_schema_set_clustered(ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Create the table */
	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	if (ib_tbl_sch != NULL) {
		ib_table_schema_delete(ib_tbl_sch);
	}

	return(err);
}

/** Open a table and return a cursor for the table. */
static
ib_err_t
open_table(
	const char*	dbname,		/*!< in: database name */
	const char*	name,		/*!< in: table name */
	ib_trx_t	ib_trx,		/*!< in: transaction */
	ib_crsr_t*	crsr)		/*!< out: innodb cursor */
{
	ib_err_t	err = DB_SUCCESS;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif
	err = ib_cursor_open_table(table_name, ib_trx, crsr);
	assert(err == DB_SUCCESS);

	return(err);
}

/** INSERT INTO T VALUE(I, CHAR('a' + I)); */
static
ib_err_t
insert_rows(
	ib_crsr_t	crsr)		/*!< in, out: cursor to use for write */
{
	int		i;
	char		ptr[] = "a";
	ib_tpl_t	tpl = NULL;
	ib_err_t	err = DB_ERROR;

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	for (i = 0; i < 10; i += 2, ++*ptr) {

		err = ib_tuple_write_i32(tpl, 0, i);
		assert(err == DB_SUCCESS);

		err = ib_col_set_value(tpl, 1, ptr, 1);
		assert(err == DB_SUCCESS);

		err = ib_cursor_insert_row(crsr, tpl);
		assert(err == DB_SUCCESS);
	}

	if (tpl != NULL) {
		ib_tuple_delete(tpl);
	}

	return(err);
}

/** UPDATE t SET c1 = c1 / 2; */
static
ib_err_t
update_rows(
	ib_crsr_t	crsr)
{
	ib_err_t	err;
	ib_tpl_t	old_tpl = NULL;
	ib_tpl_t	new_tpl = NULL;

	/* Set the record lock mode, since we are doing a SELECT FOR UPDATE */
	err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
	assert(err == DB_SUCCESS);

	/* Prepare for a table scan. */
	err = ib_cursor_first(crsr);
	assert(err == DB_SUCCESS);

	/* Create the tuple instance that we will use to update the
	table. old_tpl is used for reading the existing row and
	new_tpl will contain the update row data. */

	old_tpl = ib_clust_read_tuple_create(crsr);
	assert(old_tpl != NULL);

	new_tpl = ib_clust_read_tuple_create(crsr);
	assert(new_tpl != NULL);

	/* Iterate over the records and update all records read. */
	while (err == DB_SUCCESS) {
		int		c1;

		err = ib_cursor_read_row(crsr, old_tpl);
		assert(err == DB_SUCCESS);

		/* Copy the old contents to the new tuple. */
		err = ib_tuple_copy(new_tpl, old_tpl);

		/* Get the first column value. */
		err = ib_tuple_read_i32(new_tpl, 0, &c1);
		assert(err == DB_SUCCESS);

		c1 /= 2;

		/* Set the updated value in the new tuple. */
		err = ib_tuple_write_i32(new_tpl, 0, c1);
		assert(err == DB_SUCCESS);

		err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
		assert(err == DB_SUCCESS);

		/* Move to the next record to update. */
		err = ib_cursor_next(crsr);
	}

	assert(err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND);

	if (old_tpl != NULL) {
		ib_tuple_delete(old_tpl);
	}
	if (new_tpl != NULL) {
		ib_tuple_delete(new_tpl);
	}

	return(DB_SUCCESS);
}

/** SELECT * FROM T; */
static
ib_err_t
do_query(
	ib_crsr_t	crsr)
{
	ib_err_t	err;
	ib_tpl_t	tpl;

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	err = ib_cursor_first(crsr);
	assert(err == DB_SUCCESS);

	while (err == DB_SUCCESS) {
		err = ib_cursor_read_row(crsr, tpl);

		assert(err == DB_SUCCESS
		       || err == DB_END_OF_INDEX
		       || err == DB_RECORD_NOT_FOUND);

		if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
			break;
		}

		print_tuple(stdout, tpl);

		err = ib_cursor_next(crsr);

		assert(err == DB_SUCCESS
		       || err == DB_END_OF_INDEX
		       || err == DB_RECORD_NOT_FOUND);
	}

	if (tpl != NULL) {
		ib_tuple_delete(tpl);
	}

	if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
		err = DB_SUCCESS;
	}
	return(err);
}

int main(int argc, char* argv[])
{
	ib_err_t	err;
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;
	ib_u64_t	version;

	(void)argc;
	(void)argv;

	version = ib_api_version();
	printf("API: %d.%d.%d\n",
		(int) (version >> 32),			/* Current version */
		(int) ((version >> 16)) & 0xffff,	/* Revisiion */
	       	(int) (version & 0xffff));		/* Age */

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	err = create_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = open_table(DATABASE, TABLE, ib_trx, &crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_lock(crsr, IB_LOCK_IX);
	assert(err == DB_SUCCESS);

	err = insert_rows(crsr);
	assert(err == DB_SUCCESS);

	err = do_query(crsr);
	assert(err == DB_SUCCESS);

	printf("Update all the rows\n");
	err = update_rows(crsr);
	assert(err == DB_SUCCESS);

	err = do_query(crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);
	crsr = NULL;

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
	VALGRIND_DO_LEAK_CHECK;
#endif

	return(EXIT_SUCCESS);
}
