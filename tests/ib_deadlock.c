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

/* Create the conditions for a deadlock.
 
 Create a database
 CREATE TABLE T1(c1 INT, c2 INT, PK(c1));
 CREATE TABLE T2(c1 INT, c2 INT, PK(c1));

 In multiple threads:

 BEGIN;
 INSERT INTO Tx VALUES(1, 1);
 INSERT INTO Ty VALUES(N, N);
 -- sleep 60 seconds
 COMMIT;
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>

#include <getopt.h>	/* For getopt_long() */

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE	"test"

static ib_u32_t n_rows = 100;
static ib_u32_t n_threads = 2;

/* The page size for compressed tables, if this value is > 0 then
we create compressed tables. It's set via the command line parameter
--page-size INT */
static int page_size = 0;

/* Barrier to synchronize all threads */
static  pthread_barrier_t barrier;

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
CREATE TABLE T (c1 INT, c2 INT, PRIMARY KEY(c1)); */
static
ib_err_t
create_table(
/*=========*/
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table name */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	ib_tbl_fmt_t	tbl_fmt = IB_TBL_COMPACT;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	if (page_size > 0) {
		tbl_fmt = IB_TBL_COMPRESSED;

		printf("Creating compressed table with page size %d\n",
			page_size);
	}

	/* Pass a table page size of 0, ie., use default page size. */
	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, tbl_fmt, page_size);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c1", IB_INT, IB_COL_UNSIGNED, 0, sizeof(ib_u32_t));
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c2", IB_INT, IB_COL_UNSIGNED, 0, sizeof(ib_u32_t));
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c1", 0);
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
INSERT INTO T VALUE(i, i); */
static
ib_err_t
insert_rows(
/*========*/
	ib_crsr_t	crsr,		/*!< in, out: cursor to use for write */
	ib_u32_t	start,		/*!< in: start of column value */
	ib_u32_t	n_values,	/*!< in: no. of values to insert */
	int		thread_id)	/*!< in: id of thread doing insert */
{
	ib_u32_t	i;
	ib_tpl_t	tpl;
	ib_err_t	err = DB_SUCCESS;

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	for (i = start; i < start + n_values; ++i) {
		err = ib_tuple_write_u32(tpl, 0, i);
		assert(err == DB_SUCCESS);

		err = ib_tuple_write_u32(tpl, 1, (ib_u32_t) thread_id);
		assert(err == DB_SUCCESS);

		err = ib_cursor_insert_row(crsr, tpl);

		if (err != DB_SUCCESS) {
			break;
		}

		/* Since we are writing fixed length columns (all INTs),
		there is no need to reset the tuple. */
	}

	ib_tuple_delete(tpl);

	return(err);
}

/*********************************************************************
Run the test. */
static
void*
worker_thread(
/*==========*/
	void*		arg)
{
	int		ret;
	ib_err_t	err;
	ib_trx_t	ib_trx;
	ib_crsr_t	crsr1 = NULL;
	ib_crsr_t	crsr2 = NULL;
	ib_bool_t	deadlock = IB_FALSE;
	int		thread_id = *(int*) arg;

	free(arg);

	err = open_table(DATABASE, "T1", NULL, &crsr1);
	assert(err == DB_SUCCESS);

	err = open_table(DATABASE, "T2", NULL, &crsr2);
	assert(err == DB_SUCCESS);

	ret = pthread_barrier_wait(&barrier);
	assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
	if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
		printf("Start insert...\n");
	}

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	ib_cursor_attach_trx(crsr1, ib_trx);
	ib_cursor_attach_trx(crsr2, ib_trx);

	if (!(thread_id % 2)) {
		err = ib_cursor_lock(crsr1, IB_LOCK_IX);
		assert(err == DB_SUCCESS);

		err = ib_cursor_lock(crsr2, IB_LOCK_IX);
		assert(err == DB_SUCCESS);

		err = insert_rows(crsr1, 0, n_rows, thread_id);
		assert(err == DB_SUCCESS
		       || err == DB_DEADLOCK
		       || err == DB_LOCK_WAIT_TIMEOUT);

		if (err == DB_SUCCESS) {
			sleep(3);

			err = insert_rows(crsr2, 0, n_rows, thread_id);
			assert(err == DB_SUCCESS
			       || err == DB_DEADLOCK
			       || err == DB_LOCK_WAIT_TIMEOUT);

			if (err == DB_SUCCESS) {
				sleep(3);
			}
		}
	} else {
		err = ib_cursor_lock(crsr2, IB_LOCK_IX);
		assert(err == DB_SUCCESS);

		err = ib_cursor_lock(crsr1, IB_LOCK_IX);
		assert(err == DB_SUCCESS);

		err = insert_rows(crsr2, 0, n_rows, thread_id);
		assert(err == DB_SUCCESS
		       || err == DB_DEADLOCK
		       || err == DB_LOCK_WAIT_TIMEOUT);

		if (err == DB_SUCCESS) {
			sleep(3);

			err = insert_rows(crsr1, 0, n_rows, thread_id);
			assert(err == DB_SUCCESS
			       || err == DB_DEADLOCK
	       		       || err == DB_LOCK_WAIT_TIMEOUT);

			if (err == DB_SUCCESS) {
				sleep(3);
			}
		}
	}

	if (err != DB_SUCCESS) {
		deadlock = IB_TRUE;
	}

	err = ib_cursor_reset(crsr1);
	assert(err == DB_SUCCESS);

	err = ib_cursor_reset(crsr2);
	assert(err == DB_SUCCESS);

	if (!deadlock) {
		/* If all went well then the transaction should still
 		be active, we need to commit it. */
		assert(ib_trx_state(ib_trx) == IB_TRX_ACTIVE);
		err = ib_trx_commit(ib_trx);
		assert(err == DB_SUCCESS);
		printf("Thread#%d - trx committed.\n", thread_id);
	} else {
		/* The transaction should have been rolled back
 		by InnoDB, we can only release the handle now. */
		assert(ib_trx_state(ib_trx) != IB_TRX_ACTIVE);
		err = ib_trx_release(ib_trx);
		assert(err == DB_SUCCESS);

		printf("Thread#%d - deadlock, trx rolled back.\n", thread_id);
	}

	if (crsr1) {
		err = ib_cursor_close(crsr1);
		assert(err == DB_SUCCESS);
		crsr1 = NULL;
	}

	if (crsr2) {
		err = ib_cursor_close(crsr2);
		assert(err == DB_SUCCESS);
		crsr2 = NULL;
	}

	pthread_exit(0);
}

#ifndef __WIN__
/*********************************************************************
Set the runtime global options. */
static
void
set_options(
/*========*/
	int		argc,
	char*		argv[])
{
	int		opt;
	int		optind;
	int		size = 0;
	struct option*	longopts;
	int		count = 0;

	/* Count the number of InnoDB system options. */
	while (ib_longopts[count].name) {
		++count;
	}

	/* Add two of our options and a spot for the sentinel. */
	size = sizeof(struct option) * (count + 4);
	longopts = (struct option*) malloc(size);
	memset(longopts, 0x0, size);
	memcpy(longopts, ib_longopts, sizeof(struct option) * count);

	/* Add the local parameters (threads, rows and page_size). */
	longopts[count].name = "threads";
	longopts[count].has_arg = required_argument;
	longopts[count].flag = NULL;
	longopts[count].val = USER_OPT + 1;
	++count;

	longopts[count].name = "rows";
	longopts[count].has_arg = required_argument;
	longopts[count].flag = NULL;
	longopts[count].val = USER_OPT + 2;
	++count;

	longopts[count].name = "page_size";
	longopts[count].has_arg = required_argument;
	longopts[count].flag = NULL;
	longopts[count].val = USER_OPT + 3;

	while ((opt = getopt_long(argc, argv, "", longopts, &optind)) != -1) {
		switch(opt) {

		case USER_OPT + 1:
			n_threads = strtoul(optarg, NULL, 10);
			break;

		case USER_OPT + 2:
			n_rows = strtoul(optarg, NULL, 10);
			break;

		case USER_OPT + 3:
			page_size = strtoul(optarg, NULL, 10);
			break;

		default:
			/* If it's an InnoDB parameter, then we let the
			auxillary function handle it. */
			if (set_global_option(opt, optarg) != DB_SUCCESS) {
				print_usage(argv[0]);
				exit(EXIT_FAILURE);
			}

		} /* switch */
	}

	free(longopts);
}
#endif /* __WIN__ */

/*********************************************************************
Create the tables required for the test
@return DB_SUCCESS if all went well.*/
static
ib_err_t
create_tables(void)
/*===============*/
{
	ib_err_t	err;

	err = create_table(DATABASE, "T1");
	assert(err == DB_SUCCESS);

	err = create_table(DATABASE, "T2");
	assert(err == DB_SUCCESS);

	return(err);
}

/*********************************************************************
Drop the tables required for the test
@return DB_SUCCESS if all went well.*/
static
ib_err_t
drop_tables(void)
/*=============*/
{
	ib_err_t	err;

	err = drop_table(DATABASE, "T1");
	assert(err == DB_SUCCESS);

	err = drop_table(DATABASE, "T2");
	assert(err == DB_SUCCESS);

	return(err);
}

int main(int argc, char* argv[])
{
	int		i;
	int		ret;
	ib_err_t	err;
	pthread_t*      pthreads;

	ib_init();

	test_configure();

#ifndef __WIN__
	set_options(argc, argv);
#endif /* __WIN__ */

	err = ib_cfg_set_int("open_files", 8192);
	assert(err == DB_SUCCESS);

	/* Reduce the timeout to trigger lock timeout quickly. */
	err = ib_cfg_set_int("lock_wait_timeout", 3);
	assert(err == DB_SUCCESS);

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	err = create_tables();
	assert(err == DB_SUCCESS);

	ret = pthread_barrier_init(&barrier, NULL, n_threads);
	assert(ret == 0);

	pthreads = (pthread_t*) malloc(sizeof(*pthreads) * n_threads);
	memset(pthreads, 0, sizeof(*pthreads) * n_threads);

	printf("About to spawn %d threads ", n_threads);

	for (i = 0; i < n_threads; ++i) {
		int	retval;
		int*	ptr = malloc(sizeof(int));

		assert(ptr != NULL);
		*ptr = i;

		/* worker_thread owns the argument and is responsible for
		freeing it. */
		retval = pthread_create(&pthreads[i], NULL, worker_thread, ptr);

		if (retval != 0) {
			fprintf(stderr, "Error spawning thread %d, "
					"pthread_create() returned %d\n",
					i, retval);
			exit(EXIT_FAILURE);
		}
		printf(".");
	}

	printf("\nWaiting for threads to finish ...\n");

	for (i = 0; i < n_threads; ++i) {
		pthread_join(pthreads[i], NULL);
	}

	free(pthreads);
	pthreads = NULL;

	ret = pthread_barrier_destroy(&barrier);
	assert(ret == 0);

	err = drop_tables();
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
	VALGRIND_DO_LEAK_CHECK;
#endif

	return(EXIT_SUCCESS);
}
