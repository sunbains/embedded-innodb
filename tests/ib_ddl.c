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
 CREATE TABLE T(c1 INT, c2 VARCHAR(10), c3 BLOB); 
 INSERT INTO T VALUES(1, '1'); ...
 CREATE INDEX T_C1 ON T(c1);
 CREATE INDEX T_C2 ON T(c2);
 CREATE INDEX T_C2 ON T(c3(10));
 DROP TABLE T;
 
 The test will create all the relevant sub-directories in the current
 working directory. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE	"test"
#define TABLE		"ib_ddl"

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

/** CREATE TABLE T(C1 INT, C2 VARCHAR(10), C3 BLOB); */
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
		ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0, sizeof(ib_i32_t));

	assert(err == DB_SUCCESS);

	err = ib_tbl_sch_add_varchar_col(ib_tbl_sch, "c2", 10);
	assert(err == DB_SUCCESS);

	err = ib_tbl_sch_add_blob_col(ib_tbl_sch, "c3");
	assert(err == DB_SUCCESS);

	/* create table */
	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);

	if (err == DB_SUCCESS) {
		err = ib_trx_commit(ib_trx);
	} else {
		fprintf(stderr, "Table: %s create failed: %s\n",
				table_name, ib_strerror(err));

		err = ib_trx_rollback(ib_trx);
	}
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

/** INSERT INTO T VALUE(0, RANDOM(TEXT), RANDOM(TEXT)); ... 100 */
static
ib_err_t
insert_random_rows(
	ib_crsr_t	crsr)		/*!< in, out: cursor to use for write */
{
	ib_i32_t	i;
	ib_err_t	err;
	ib_tpl_t	tpl;
	char*		ptr = malloc(8192);

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	for (i = 0;  i < 100; ++i) {
		int		l;

		err = ib_tuple_write_i32(tpl, 0, i % 10);
		assert(err == DB_SUCCESS);

		l = gen_rand_text(ptr, 10);
		err = ib_col_set_value(tpl, 1, ptr, l);
		assert(err == DB_SUCCESS);

		l = gen_rand_text(ptr, 8192);
		err = ib_col_set_value(tpl, 2, ptr, l);
		assert(err == DB_SUCCESS);

		err = ib_cursor_insert_row(crsr, tpl);
		assert(err == DB_SUCCESS);

		tpl = ib_tuple_clear(tpl);
		assert(tpl != NULL);
	}

	if (tpl != NULL) {
		ib_tuple_delete(tpl);
	}

	free(ptr);

	return(err);
}

/** Create a secondary indexes on a table.
@return	DB_SUCCESS or error code */
static
ib_err_t
create_sec_index(
	const char*	table_name,	/*!< in: table name */
	const char*	col_name,	/*!< in: column name */
	int		prefix_len)	/*!< in: prefix index length */

{
	ib_err_t	err;
	ib_trx_t	ib_trx;
	ib_id_t		index_id = 0;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		index_name[IB_MAX_TABLE_NAME_LEN];

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

#ifdef __WIN__
	sprintf(index_name, "%s_%s", table_name, col_name);
#else
	snprintf(index_name, sizeof(index_name), "%s_%s", table_name, col_name);
#endif
	err = ib_index_schema_create(
		ib_trx, index_name, table_name, &ib_idx_sch);

	assert(err == DB_SUCCESS);

	err = ib_index_schema_add_col(ib_idx_sch, col_name, prefix_len);
	assert(err == DB_SUCCESS);

	err = ib_index_create(ib_idx_sch, &index_id);

	if (ib_idx_sch != NULL) {
		ib_index_schema_delete(ib_idx_sch);
		ib_idx_sch = NULL;
	}

	if (err == DB_SUCCESS) {
		err = ib_trx_commit(ib_trx);
	} else {
		err = ib_trx_rollback(ib_trx);
	}
	assert(err == DB_SUCCESS);

	return(err);
}

/** Create secondary indexes on T(C1), T(C2), T(C3). */
static
ib_err_t
create_sec_index_1(
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table to drop */
{
	ib_err_t	err;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	err = create_sec_index(table_name, "c1", 0);

	if (err == DB_SUCCESS) {
		err = create_sec_index(table_name, "c2", 0);
	}

	if (err == DB_SUCCESS) {
		err = create_sec_index(table_name, "c3", 10);
	}

	return(err);
}

/** Open the secondary index. */
static
ib_err_t
open_sec_index(
	ib_crsr_t	crsr,		/*!< in: table cusor */
	const char* 	index_name)	/*!< in: sec. index to open */
{
	ib_err_t	err;
	ib_crsr_t	idx_crsr;

	err = ib_cursor_open_index_using_name(crsr, index_name, &idx_crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(idx_crsr);
	assert(err == DB_SUCCESS);

	return(err);
}
/** Open the secondary indexes on T(C1), T(C2), T(C3). */
static
ib_err_t
open_sec_index_1(
	const char*	dbname,		/*!< in: database name */
	const char* 	name)		/*!< in: table name */
{
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;
	ib_err_t	err = DB_SUCCESS;
	char		index_name[IB_MAX_TABLE_NAME_LEN];
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

	err = ib_cursor_open_table(table_name, ib_trx, &crsr);
	assert(err == DB_SUCCESS);

#ifdef __WIN__
	sprintf(index_name, "%s_%s", table_name, "c1");
#else
	snprintf(index_name, sizeof(index_name), "%s_%s", table_name, "c1");
#endif
	err = open_sec_index(crsr, index_name);
	assert(err == DB_SUCCESS);

#ifdef __WIN__
	sprintf(index_name, "%s_%s", table_name, "c2");
#else
	snprintf(index_name, sizeof(index_name), "%s_%s", table_name, "c2");
#endif
	err = open_sec_index(crsr, index_name);
	assert(err == DB_SUCCESS);

#ifdef __WIN__
	sprintf(index_name, "%s_%s", table_name, "c3");
#else
	snprintf(index_name, sizeof(index_name), "%s_%s", table_name, "c3");
#endif
	err = open_sec_index(crsr, index_name);
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	return(err);
}

#define	TEMP_INDEX_PREFIX	'\377' /* from ut0ut.h */

/** Tests creating a index with a name that InnoDB uses for temporary indexs
@return	DB_SUCCESS or error code */
static
ib_err_t
test_create_temp_index(
	const char*     dbname,
	const char*	name,	/*!< in: table name */
	const char*	col_name)	/*!< in: column name */
{
	ib_err_t	err;
	ib_trx_t	ib_trx;
	ib_id_t		index_id = 0;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		index_name[IB_MAX_TABLE_NAME_LEN];
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

#ifdef __WIN__
	sprintf(index_name, "%c%s_%s", TEMP_INDEX_PREFIX, table_name, col_name);
#else
	snprintf(index_name, sizeof(index_name), "%c%s_%s", TEMP_INDEX_PREFIX,
		table_name, col_name);
#endif
	err = ib_index_schema_create(
		ib_trx, index_name, table_name, &ib_idx_sch);

	assert(err == DB_INVALID_INPUT);

	if (ib_idx_sch != NULL) {
		ib_index_schema_delete(ib_idx_sch);
		ib_idx_sch = NULL;
	}

	if (err == DB_SUCCESS) {
		err = ib_trx_commit(ib_trx);
	} else {
		err = ib_trx_rollback(ib_trx);
	}
	assert(err == DB_SUCCESS);

	return(err);
}

int main(int argc, char* argv[])
{
	ib_err_t	err;
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;

	(void) argc;
	(void) argv;

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

	err = insert_random_rows(crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);
	crsr = NULL;

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	err = create_sec_index_1(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = test_create_temp_index(DATABASE, TABLE, "c2");


	err = open_sec_index_1(DATABASE, TABLE);
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
