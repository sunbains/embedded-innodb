/*
  Copyright (C) 2010 Stewart Smith

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License
  as published by the Free Software Foundation; either version 2
  of the License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/
/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.

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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#define DATABASE	"test"
#define TABLE		"t"
/* A row from our test table. */
typedef struct row_t {
	char		c1[32];
	char		c2[32];
	ib_u32_t	c3;
} row_t;

#define COL_LEN(n)	(sizeof(((row_t*)0)->n))
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

/** CREATE TABLE T(
	c1	VARCHAR(n),
	c2	VARCHAR(n),
	c3	INT,
	PRIMARY KEY(c1, c2); */
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
		ib_tbl_sch, "c1",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(c1)-1);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c2",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(c2)-1);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c3",
		IB_INT, IB_COL_UNSIGNED, 0, COL_LEN(c3));

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "c1_c2", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c1", 0);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c2", 0);
	assert(err == DB_SUCCESS);

	err = ib_index_schema_set_clustered(ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* create table */
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

	printf("Create table\n");
	err = create_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	printf("Begin transaction\n");
	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	printf("Open cursor\n");
	err = open_table(DATABASE, TABLE, ib_trx, &crsr);
	assert(err == DB_SUCCESS);

	printf("Test ib_cursor_open_index_using_name doesn't segv");
	ib_crsr_t index_crsr;
	err = ib_cursor_open_index_using_name(crsr, "foobar", &index_crsr);

	assert(err == DB_TABLE_NOT_FOUND);

	printf("Close cursors\n");
	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);
	crsr = NULL;

	printf("Commit transaction\n");
	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);


	return(EXIT_SUCCESS);
}
