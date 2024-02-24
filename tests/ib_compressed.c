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

/* Simple test that creates a few compressed tables and checks
for valid page_size */

#include <stdio.h>
#include <stdlib.h>

#include "haildb.h"

#include "test0aux.h"

/* use a common database name "test" so it can be cleaned up by
"make test-clean" if we exit abnormally */
#ifdef DBNAME
#undef DBNAME
#endif /* DBNAME */
#define DBNAME	"test"

/* use a name that is not going to collide with names from other tests */
#define TABLENAME	DBNAME "/t_compressed"

int
main(int argc, char* argv[])
{
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_id_t		table_id;
	int		valid_page_sizes[] = {0, 1, 2, 4, 8, 16};
	int		invalid_page_sizes[] = {3, 5, 6, 14, 17, 32, 128, 301};
	size_t		i;

	(void)argc;
	(void)argv;

	OK(ib_init());

	test_configure();

	OK(ib_startup("barracuda"));

	OK(ib_cfg_set("file_per_table", IB_TRUE));

	OK(ib_database_create(DBNAME));

	for (i = 0;
	     i < sizeof(valid_page_sizes) / sizeof(valid_page_sizes[0]);
	     i++) {

		ib_trx_t	ib_trx;

		OK(ib_table_schema_create(TABLENAME, &ib_tbl_sch,
					  IB_TBL_COMPRESSED,
					  valid_page_sizes[i]));

		OK(ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT,
					   IB_COL_UNSIGNED, 0 /* ignored */,
					   sizeof(int)));

		ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

		OK(ib_schema_lock_exclusive(ib_trx));

		OK(ib_table_create(ib_trx, ib_tbl_sch, &table_id));

		OK(ib_trx_commit(ib_trx));

		ib_table_schema_delete(ib_tbl_sch);

		ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);

		OK(ib_schema_lock_exclusive(ib_trx));

		OK(ib_table_drop(ib_trx, TABLENAME));

		OK(ib_trx_commit(ib_trx));
	}

	for (i = 0;
	     i < sizeof(invalid_page_sizes) / sizeof(invalid_page_sizes[0]);
	     i++) {

		ib_err_t	ib_err;

		ib_err = ib_table_schema_create(TABLENAME, &ib_tbl_sch,
						IB_TBL_COMPRESSED,
						invalid_page_sizes[i]);

		if (ib_err == DB_SUCCESS) {
			fprintf(stderr, "Creating a compressed table with "
				"page size %d succeeded but should have "
				"failed", invalid_page_sizes[i]);
			exit(EXIT_FAILURE);
		}

	}

	/* ignore errors as there may be tables left over from other tests */
	OK(ib_database_drop(DBNAME));

	OK(ib_shutdown(IB_SHUTDOWN_NORMAL));

	return(EXIT_SUCCESS);
}
