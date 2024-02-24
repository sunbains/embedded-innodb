/***********************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith

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

/* Test table statistics API*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "test0aux.h"

#include <inttypes.h>

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE	"test"
#define TABLE		"ib_table_statistics"

/*********************************************************************
Create an InnoDB database (sub-directory). */
static
ib_err_t
create_database(
/*============*/
	const char*	name)
{
	ib_bool_t	err;

	err = ib_database_create(name);
	assert(err == IB_TRUE);

	return(DB_SUCCESS);
}

/*********************************************************************
CREATE TABLE T(
	pk	INT,
        a       INT,
        b       INT,
	PRIMARY KEY(pk),
        INDEX (a),
        INDEX (b),
; */
static
ib_err_t
create_table_t2(
/*=========*/
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table name */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	ib_idx_sch_t	ib_idx_sch_a = NULL;
	ib_idx_sch_t	ib_idx_sch_b = NULL;
	ib_tbl_fmt_t	tbl_fmt = IB_TBL_COMPACT;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, tbl_fmt, 0);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "pk",
		IB_INT, IB_COL_NONE, 0, 4);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "a",
		IB_INT, IB_COL_NONE, 0, 4);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "b",
		IB_INT, IB_COL_NONE, 0, 4);
	assert(err == DB_SUCCESS);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "pk", 0);
	assert(err == DB_SUCCESS);

	err = ib_index_schema_set_clustered(ib_idx_sch);
	assert(err == DB_SUCCESS);

	err = ib_index_schema_set_unique(ib_idx_sch);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "a", &ib_idx_sch_a);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch_a, "a", 0);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "b", &ib_idx_sch_b);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch_b, "b", 0);
	assert(err == DB_SUCCESS);

	/* create table */
	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
	if (err != DB_SUCCESS) {
		fprintf(stderr, "Warning: table create failed: %s\n",
				ib_strerror(err));
		err = ib_trx_rollback(ib_trx);
	} else {
		err = ib_trx_commit(ib_trx);
	}
	assert(err == DB_SUCCESS);

	if (ib_tbl_sch != NULL) {
		ib_table_schema_delete(ib_tbl_sch);
	}

	return(err);
}

/*********************************************************************
Open a table and return a cursor for the table. */
static
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

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif
	err = ib_cursor_open_table(table_name, ib_trx, crsr);
	assert(err == DB_SUCCESS);

	return(err);
}

/*********************************************************************
INSERT INTO T VALUE(int); */
static
ib_err_t
insert_rows(
/*===============*/
	ib_crsr_t	crsr,
	int start,
	int count)
{
	int		i;
	ib_err_t	err;
	ib_tpl_t	tpl = NULL;
	char*		ptr = malloc(8192);

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	for (i = start; i < start+count; ++i) {

		err = ib_tuple_write_u32(tpl, 0, i);
		assert(err == DB_SUCCESS);

		err = ib_tuple_write_u32(tpl, 1, i%10);
		assert(err == DB_SUCCESS);

		err = ib_tuple_write_u32(tpl, 1, rand());
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

static void display_table_stats(ib_crsr_t crsr)
{
	ib_err_t err;
	ib_table_stats_t tstats;
	err = ib_get_table_statistics(crsr, &tstats, sizeof(tstats));
	assert(err == DB_SUCCESS);

	printf("Table Statistics\n");
	printf("----------------\n");
	printf("Rows:\t%" PRIi64 "\n", tstats.stat_n_rows);
	printf("Clustered Index Size:\t%" PRIu64 "\n", tstats.stat_clustered_index_size);
	printf("Other Index Size:\t%" PRIu64 "\n", tstats.stat_sum_of_other_index_sizes);
	printf("Modified Counter:\t%" PRIu64 "\n", tstats.stat_modified_counter);
	printf("\n");

	printf("\n\nINSERT 10,000 ROWS\n\n");

	printf("\n\n");
	printf("INDEX Statistics: n_diff_key_vals\n");
	printf("---------------------------------\n");

	uint64_t ncols;
	int64_t *n_diff;
	unsigned int col;
	printf("PRIMARY:\t");
	err= ib_get_index_stat_n_diff_key_vals(crsr, "PRIMARY",&ncols, &n_diff);
	for (col=0; col<ncols; col++)
		printf("%"PRIi64"\t", n_diff[col]);
	printf("\n\n");
	free(n_diff);

	printf("a:\t");
	err= ib_get_index_stat_n_diff_key_vals(crsr, "a",&ncols, &n_diff);
	for (col=0; col<ncols; col++)
		printf("%"PRIi64"\t", n_diff[col]);
	printf("\n\n");
	free(n_diff);

	printf("b:\t");
	err= ib_get_index_stat_n_diff_key_vals(crsr, "b",&ncols, &n_diff);
	for (col=0; col<ncols; col++)
		printf("%"PRIi64"\t", n_diff[col]);
	printf("\n\n");
	free(n_diff);
}

int main(int argc, char* argv[])
{
	int		i;
	ib_err_t	err;
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	printf("Create table\n");
	err = create_table_t2(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = open_table(DATABASE, TABLE, ib_trx, &crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_lock(crsr, IB_LOCK_IX);
	assert(err == DB_SUCCESS);

	insert_rows(crsr, 1, 100);

	ib_table_stats_t tstats;
	err = ib_get_table_statistics(crsr, &tstats, sizeof(tstats));
	assert(err == DB_SUCCESS);

	display_table_stats(crsr);

	for (int i=0; i< 5; i++)
	{
		insert_rows(crsr, i*10000+(i+1+101), 10000);
		err = ib_cursor_close(crsr);
		assert(err == DB_SUCCESS);
		crsr = NULL;
		err = ib_trx_commit(ib_trx);
		assert(err == DB_SUCCESS);
		ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
		assert(ib_trx != NULL);

		err = open_table(DATABASE, TABLE, ib_trx, &crsr);
		assert(err == DB_SUCCESS);


		display_table_stats(crsr);
	}

	ib_table_stats_t tstats2;
	err = ib_get_table_statistics(crsr, &tstats2, sizeof(tstats2));
	assert(err == DB_SUCCESS);

	assert(tstats.stat_n_rows < tstats2.stat_n_rows);
	assert(tstats.stat_clustered_index_size < tstats2.stat_clustered_index_size);
	assert(tstats.stat_modified_counter < tstats2.stat_modified_counter);

	ib_update_table_statistics(crsr);

	display_table_stats(crsr);

	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);
	crsr = NULL;

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
	VALGRIND_DO_LEAK_CHECK;
#endif

	return(EXIT_SUCCESS);
}
