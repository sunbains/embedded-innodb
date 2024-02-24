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

/* Test to check whether invalid table names are rejected. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

/** All attempts to create the table should fail */
static
void
create_table(void)
{
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;

	err = ib_table_schema_create("", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("a", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("ab", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create(".", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("./", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("../", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("/", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("/aaaaa", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("/a/a", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("abcdef/", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_DATA_MISMATCH);

	err = ib_table_schema_create("a/b", &ib_tbl_sch, IB_TBL_COMPACT, 0);
	assert(err == DB_SUCCESS);

	ib_table_schema_delete(ib_tbl_sch);
}

int main(int argc, char* argv[])
{
	ib_err_t	err;

	(void)argc;
	(void)argv;

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	create_table();

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return(EXIT_SUCCESS);
}
