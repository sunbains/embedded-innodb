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
NOTE: This test is currently Unix centric and will not compile on Windows.

This module is a driver framework for running arbitrary DML and DDL
statements on arbitrary number of tables using configurable number
of threads.
The DML statements that can be triggered are:
SELECT
INSERT
UPDATE
DELETE

The DDL statements that can be triggered are:
CREATE TABLE
ALTER TABLE
DROP TABLE
TRUNCATE TABLE

How it works:
=============
This is just a driver. You have to implement the DML and DDL
functionality for each table (see mt_base.c and mt_t1.c for examples)

Once a derived table class is implemented it can be plugged into this
driver by calling the register function in register_test_tables().

This driver goes through following sequence:
1) Create database test
2) Register all tables
3) Create each registered table
4) Seed each table with n_rows
5) Create n_ddl_threads and n_dml_threads
6) Each DDL thread does following:
	a) Choose one of the table randomly
	b) Choose a DDL operation randomly
	c) Execute the DDL operation and update the stats struct
	d) Wait for ddl_interval before going back to (a)
7) Each DML thread does following:
	a) Choose one of the table randomly
	b) Choose a DML operation randomly
	c) Execute the DDL operation and update the stats struct
	d) Return to (a)
8) The test continues for test_time
9) Waits for all worker threads to finish
10) Check all tables (for now checking mean only doing a select *)
11) Drop all tables and the database
12) Print the statistics
13) Clean up
**********************************************************************/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#include "test0aux.h"
#include "ib_mt_drv.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif


#define DATABASE "test"

/* Number of worker threads*/
static int		n_ddl_thr = 4;
static int		n_dml_thr = 16;

static pthread_t*	ddl_tid;
static pthread_t*	dml_tid;

/* Initial number of rows. */
static const int	n_rows = 1000;

/* batch size for DML. Commit after that many rows are worked upon */
static const int	batch_size = 1;

/* %age transactions to be rolled back */
static const int	rollback_percent = 10;

/* isolation level for transactions */
static const int	isolation_level = IB_TRX_REPEATABLE_READ;

/* Test duration in seconds */
static int		test_time = 100;

/* Interval between DDL statements in seconds */
static int		ddl_interval = 1;

/* To be incremented every time we do a DROP or TRUNCATE.
This is for debug purposes so that we know that the old
data actually vanished from the table after a DROP or
TRUNCATE */
static int		run_number = 1;
#define get_cur_run_number() (run_number)
#define incr_cur_run_number() (++run_number)

/* Flag used by worker threads to finish the run */
static ib_bool_t	test_running = IB_FALSE;

/* These functions are defined in the corresponding modules where
table classes are defined. Base class is defined in mt_base.c */
extern void register_base_table(tbl_class_t*);

/* Register new tables by providing initialaztion function def
here and calling it in register_test_tables() */
extern void register_t1_table(tbl_class_t*);
extern void register_t2_table(tbl_class_t*);

/* Maximum number of tables that this test suite can work with */
#define NUM_TBLS	64

static tbl_class_t	tbl_array[NUM_TBLS];

/* How many tables we have registered currently */
static int		num_tables = 0;

/* These are going to be configurable parameters for table
creation */
static ib_tbl_fmt_t	tbl_format = IB_TBL_COMPACT;
static ib_ulint_t	page_size = 0;

/* Datastructure to hold error statistics for DML and DDL operations */
static op_err_t dml_op_errs[DML_OP_TYPE_MAX];
static op_err_t ddl_op_errs[DDL_OP_TYPE_MAX];

/**********************************************************************
Update the error stats */
void
update_err_stats(
/*=============*/
	op_err_t*	e,	/*!< in: error stat struct */
	ib_err_t	err)	/*!< in: error code */
{
	pthread_mutex_lock(&e->mutex);
	e->n_ops++;
	if (err != DB_SUCCESS ) {
		e->n_errs++;
		e->errs[err]++;
	}
	pthread_mutex_unlock(&e->mutex);
}

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
Drop the database. */
static
ib_err_t
drop_database(
/*==========*/
	const char*	dbname)			/*!< in: db to drop */
{
	ib_err_t	err;
	err = ib_database_drop(dbname);

	return(err);
}

/**********************************************************************
Create test tables at startup */
static
void
create_test_table(
/*==============*/
	tbl_class_t*	tbl)	/*!< in: table to create */
{
	ib_err_t	err;
	cb_args_t	args;

	args.trx= NULL;
	args.tbl = tbl;
	args.isolation_level = isolation_level;
	args.run_number = get_cur_run_number();
	args.err_st = &ddl_op_errs[DDL_OP_TYPE_CREATE];

	err = tbl->ddl_fn[DDL_OP_TYPE_CREATE](&args);
	assert(err == DB_SUCCESS);
}

/**********************************************************************
Seed test table with initial data */
static
void
seed_test_table(
/*===========*/
	tbl_class_t*	tbl)	/*!< in: table to populate */
{
	ib_err_t	err;
	ib_trx_t	trx;
	cb_args_t	args;
	int		i;

	args.trx = NULL;
	args.isolation_level = isolation_level;
	args.batch_size = 1;
	args.tbl = tbl;

	for (i = 0; i < n_rows; ++i) {
		trx = ib_trx_begin(isolation_level);
		assert(trx != NULL);

		args.trx = trx;
		args.run_number = get_cur_run_number();
		args.err_st = &dml_op_errs[DML_OP_TYPE_INSERT];

		err = tbl->dml_fn[DML_OP_TYPE_INSERT](&args);

		err = ib_trx_commit(trx);
		assert(err == DB_SUCCESS);

	}
}

/**********************************************************************
Create test tables and populate them with n_rows of data */
static
void
init_test_tables(void)
/*==================*/
{
	int	i;

	for (i = 0; i < num_tables; ++i) {
		create_test_table(&tbl_array[i]);
	}

	for (i = 0; i < num_tables; ++i) {
		seed_test_table(&tbl_array[i]);
	}
}

/**********************************************************************
Drop test tables */
static
void
drop_test_tables(void)
/*==================*/
{
	ib_err_t	err;
	int		i;

	for (i = 0; i < num_tables; ++i) {
		cb_args_t	args;
		tbl_class_t*	tbl = &tbl_array[i];

		args.trx = NULL;
		args.tbl = tbl;
		args.isolation_level = isolation_level;
		args.err_st = &ddl_op_errs[DDL_OP_TYPE_DROP];

		err = tbl->ddl_fn[DDL_OP_TYPE_DROP](&args);

	}
}

/**********************************************************************
Depending on rollback_percent decides whether to commit or rollback a
given transaction */
static
void
commit_or_rollback(
/*===============*/
	ib_trx_t	trx,	/*!< in: trx to commit or rollback */
	int		cnt,	/*!< in: trx counter */
	ib_err_t	err)	/*!< in: err from last operation */
{

	/* We have set rollback_on_timeout therefore we need not to
	commit a trx that returns with this error. */
	if (err == DB_LOCK_WAIT_TIMEOUT) {
		;
	} else if ((!rollback_percent || cnt % (100 / rollback_percent))
	    && err == DB_SUCCESS) {
		//printf("Commit transaction\n");
		err = ib_trx_commit(trx);
		assert(err == DB_SUCCESS);
	} else {
		//printf("Rollback transaction\n");
		err = ib_trx_rollback(trx);
		assert(err == DB_SUCCESS);
	}
}

/**********************************************************************
A DML worker thread that performs a randomly chosen DML operation on a
randomly chosen table */
static
void*
dml_worker(
/*=======*/
	void*	dummy)	/*!< in: unused */
{
	ib_err_t	err;
	ib_trx_t	trx;
	tbl_class_t*	tbl;
	cb_args_t	args;
	int		j;
	int		cnt = 0;

	(void)dummy;

	fprintf(stderr, "dml_worker up!\n");

	args.isolation_level = isolation_level;
	args.batch_size = batch_size;
	do {
		trx = ib_trx_begin(isolation_level);
		assert(trx != NULL);

		/* choose a table randomly */
		tbl = &tbl_array[random() % num_tables];
		args.tbl = tbl;
		args.trx = trx;
		args.run_number = get_cur_run_number();

		/* choose a DML operation randomly */
		j = random() % DML_OP_TYPE_MAX;

		++cnt;
		args.err_st = &dml_op_errs[j];
		err = tbl->dml_fn[j](&args);

		/* If it was an insert or update and the
		run_number has changed we unconditionally
		rollback in order to maintain consistency
		in the values that are inserted in the
		columns */
		if (args.run_number != get_cur_run_number()
		    && (j == DML_OP_TYPE_UPDATE
		    || j == DML_OP_TYPE_INSERT)) {
			err = ib_trx_rollback(trx);
			assert(err == DB_SUCCESS);
		} else {
			commit_or_rollback(trx, cnt, err);
		}

	} while (test_running);

	fprintf(stderr, "dml_worker done!\n");
	return(NULL);
}

/**********************************************************************
A DDL worker thread that performs a randomly chosen DDL operation on a
randomly chosen table */
static
void*
ddl_worker(
/*=======*/
	void*	dummy)	/*!< in: unused */
{
	cb_args_t	args;

	(void)dummy;

	fprintf(stderr, "ddl_worker up!\n");

	args.isolation_level = isolation_level;
	do {
		int		j;
		ib_err_t	err;
		tbl_class_t*	tbl;

		/* wait for DDL interval */
		sleep(ddl_interval);

		/* choose a table randomly */
		tbl = &tbl_array[random() % num_tables];
		args.tbl = tbl;

		/* choose a DDL operation randomly */
		j = random() % DDL_OP_TYPE_MAX;

		/* When doing DROP or TRUNCATE we increment the
		run_number */
		if (j == DDL_OP_TYPE_DROP || j == DDL_OP_TYPE_TRUNCATE) {
			incr_cur_run_number();
		}

		args.err_st = &ddl_op_errs[j];
		err = tbl->ddl_fn[j](&args);

		/* If it was a DROP TABLE or if it returned
		DB_TABLE_NOT_FOUND then we should make an attempt to
		create the table right away */
		if ((err == DB_SUCCESS && j == DDL_OP_TYPE_DROP)
		    || (err == DB_TABLE_NOT_FOUND)) {
			args.err_st = &ddl_op_errs[DDL_OP_TYPE_CREATE];
			err = tbl->ddl_fn[DDL_OP_TYPE_CREATE](&args);
		}

	} while (test_running);

	fprintf(stderr, "ddl_worker done!\n");
	return(NULL);
}

/**********************************************************************
Create worker threads */
static
void
create_worker_threads(void)
/*=======================*/
{
	int rc;
	int i;

	dml_tid = (pthread_t*)malloc(sizeof(pthread_t) * n_dml_thr);
	assert(dml_tid != NULL);

	for (i = 0; i < n_dml_thr; ++i) {
		rc = pthread_create(&dml_tid[i], NULL, dml_worker, NULL);
		assert(!rc);
	}

	ddl_tid = (pthread_t*)malloc(sizeof(pthread_t) * n_ddl_thr);
	assert(ddl_tid != NULL);
	for (i = 0; i < n_ddl_thr; ++i) {
		rc = pthread_create(&ddl_tid[i], NULL, ddl_worker, NULL);
		assert(!rc);
	}
}

/**********************************************************************
Initialize the structure to hold error statistics */
static
void
init_err_op_struct(
/*===============*/
	op_err_t*	st)	/*!< in/out: struct to initialize */
{
	int rc;
	memset(st, 0x00, sizeof(*st));
	rc = pthread_mutex_init(&st->mutex, NULL);
	assert(!rc);
}

/**********************************************************************
Initialize statistic structures. */
static
void
init_stat_structs(void)
/*===================*/
{
	int i;

	for (i = 0; i < DML_OP_TYPE_MAX; ++i) {
		init_err_op_struct(&dml_op_errs[i]);
	}

	for (i = 0; i < DDL_OP_TYPE_MAX; ++i) {
		init_err_op_struct(&ddl_op_errs[i]);
	}
}

/**********************************************************************
Free up the resources used in the test */
static
void
clean_up(void)
/*==========*/
{
	int	i;

	for (i = 0; i < DML_OP_TYPE_MAX; ++i) {
		pthread_mutex_destroy(&dml_op_errs[i].mutex);
	}

	for (i = 0; i < DDL_OP_TYPE_MAX; ++i) {
		pthread_mutex_destroy(&ddl_op_errs[i].mutex);
	}

	free(dml_tid);
	free(ddl_tid);
}

/**********************************************************************
Print statistics at the end of the test */
static
void
print_one_struct(
/*=============*/
	op_err_t*	st)
{
	int	i;

	fprintf(stderr, "n_ops = %d n_err = %d\n",
			st->n_ops, st->n_errs);

	fprintf(stderr, "err  freq\n");
	fprintf(stderr, "=========\n");
	for (i = 0; i < DB_DATA_MISMATCH; ++i) {
		if (st->errs[i] != 0) {
			fprintf(stderr, "%d   %d\n", i, st->errs[i]);
		}
	}
	fprintf(stderr, "=========\n");
}

/**********************************************************************
Print statistics at the end of the test */
static
void
print_results(void)
/*===============*/
{
	fprintf(stderr, "SELECT: ");
	print_one_struct(&dml_op_errs[DML_OP_TYPE_SELECT]);
	fprintf(stderr, "DELETE: ");
	print_one_struct(&dml_op_errs[DML_OP_TYPE_DELETE]);
	fprintf(stderr, "UPDATE: ");
	print_one_struct(&dml_op_errs[DML_OP_TYPE_UPDATE]);
	fprintf(stderr, "INSERT: ");
	print_one_struct(&dml_op_errs[DML_OP_TYPE_INSERT]);

	fprintf(stderr, "CREATE: ");
	print_one_struct(&ddl_op_errs[DDL_OP_TYPE_CREATE]);
	fprintf(stderr, "DROP: ");
	print_one_struct(&ddl_op_errs[DDL_OP_TYPE_DROP]);
	fprintf(stderr, "ALTER: ");
	print_one_struct(&ddl_op_errs[DDL_OP_TYPE_ALTER]);
	fprintf(stderr, "TRUNCATE: ");
	print_one_struct(&ddl_op_errs[DDL_OP_TYPE_TRUNCATE]);
}

/**********************************************************************
Register a table to be part of this driver. This is a bit of
polymorphism using function pointers. Each table is first initialized
to the base table which provides the generic functionality for some
operations. The derived classes can decide to override this if they
want to. See mt_base.c and mt_t1.c on how to add new tables to this
driver. */
static
void
register_test_tables(void)
/*======================*/
{
	int	i;

	/* Initialize all table classes as base table */
	for (i = 0; i < NUM_TBLS; ++i) {
		register_base_table(&tbl_array[i]);
		strcpy((char *)&tbl_array[i].db_name, DATABASE);
		tbl_array[i].format = tbl_format;
		tbl_array[i].page_size = page_size;

	}

	/* Individually register different table types here */
	register_t1_table(&tbl_array[num_tables++]);
	register_t2_table(&tbl_array[num_tables++]);

	assert(num_tables < NUM_TBLS);
}

/**********************************************************************
Whatever checks we want to run after the test after all the worker
threads have finished. For now I just call the select function to
print the contents of the table */
static
void
check_test_tables(void)
/*======================*/
{
	int	i;
	cb_args_t	args;

	/* Initialize all table classes as dummy table */
	for (i = 0; i < num_tables; ++i) {
		tbl_class_t*	tbl;
		ib_trx_t	trx;
		ib_err_t	err;

		trx = ib_trx_begin(isolation_level);
		assert(trx != NULL);

		tbl = &tbl_array[i];
		args.tbl = tbl;
		args.trx = trx;
		args.isolation_level = isolation_level;
		args.print_res = IB_TRUE;
		args.run_number = get_cur_run_number();
		args.err_st = &dml_op_errs[DML_OP_TYPE_SELECT];

		tbl->dml_fn[DML_OP_TYPE_SELECT](&args);

		err = ib_trx_commit(trx);
		assert(err == DB_SUCCESS);
	}
}

int main(int argc, char* argv[])
{
	int		i;
	void*		res;
	ib_err_t	err;

	(void)argc;
	(void)argv;

	srandom(time(NULL));

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	init_stat_structs();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	register_test_tables();
	init_test_tables();

	/* start the test. */
	test_running = IB_TRUE;
	create_worker_threads();

	/* sleep for test duration */
	sleep(test_time);

	/* stop test and let workers exit */
	test_running = IB_FALSE;
	for (i = 0; i < n_ddl_thr; ++i) {
		pthread_join(ddl_tid[i], &res);
	}

	for (i = 0; i < n_dml_thr; ++i) {
		pthread_join(dml_tid[i], &res);
	}

	/* Let innodb finish off any background drop table requests */
	sleep(1);

	check_test_tables();

	drop_test_tables();

	err = drop_database(DATABASE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	print_results();

	clean_up();

#ifdef UNIV_DEBUG_VALGRIND
	VALGRIND_DO_LEAK_CHECK;
#endif

	return(EXIT_SUCCESS);
}
