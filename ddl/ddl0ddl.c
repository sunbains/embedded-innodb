/******************************************************
Implemention of Innobase DDL operations.

(c) 2008 Oracle Corpn/Innobase Oy

Created 12 Oct 2008.
*******************************************************/

#include "srv0srv.h"
#include "api0misc.h"
#include "trx0roll.h"
#include "dict0crea.h"
#include "dict0boot.h"
#include "dict0load.h"
#include "log0log.h"
#include "lock0lock.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "pars0pars.h"
#include "ut0lst.h"

/* List of tables we should drop in background. ALTER TABLE requires
that the table handler can drop the table in background when there are no
queries to it any more. Protected by the kernel mutex. */
typedef struct ddl_drop_struct	ddl_drop_t;
struct ddl_drop_struct{
	char*				table_name;
	UT_LIST_NODE_T(ddl_drop_t)	ddl_drop_list;
};

static UT_LIST_BASE_NODE_T(ddl_drop_t)	ddl_drop_list;
static ibool	ddl_drop_list_inited	= FALSE;

/* Magic table names for invoking various monitor threads */
static const char S_innodb_monitor[] = "innodb_monitor";
static const char S_innodb_lock_monitor[] = "innodb_lock_monitor";
static const char S_innodb_tablespace_monitor[] = "innodb_tablespace_monitor";
static const char S_innodb_table_monitor[] = "innodb_table_monitor";
static const char S_innodb_mem_validate[] = "innodb_mem_validate";

/*************************************************************************
Drops a table as a background operation.  On Unix in ALTER TABLE the table
handler does not remove the table before all handles to it has been removed.
Furhermore, the call to the drop table must be non-blocking. Therefore
we do the drop table as a background operation, which is taken care of by
the master thread in srv0srv.c.
@return	error code or DB_SUCCESS */
static
ulint
ddl_drop_table_in_background(
/*=========================*/
	const char*	name)	/*!< in: table name */
{
	trx_t*	trx;
	ulint	error;
	ibool	started;

	trx = trx_allocate_for_background();

	started = trx_start(trx, ULINT_UNDEFINED);
	ut_a(started);

	/* If the original transaction was dropping a table referenced by
	foreign keys, we must set the following to be able to drop the
	table: */

	trx->check_foreigns = FALSE;

#if 0
	ib_logger(ib_stream, "InnoDB: Info: Dropping table ");
	ut_print_name(ib_stream, trx, TRUE, name);
	ib_logger(ib_stream, " from background drop list\n"); 
#endif

	/* Try to drop the table in InnoDB */

	dict_lock_data_dictionary(trx);

	error = ddl_drop_table(name, trx, FALSE);

	trx_commit(trx);

	dict_unlock_data_dictionary(trx);

	log_buffer_flush_to_disk();

	trx_free_for_background(trx);

	return(error);
}

/*************************************************************************
The master thread in srv0srv.c calls this regularly to drop tables which
we must drop in background after queries to them have ended. Such lazy
dropping of tables is needed in ALTER TABLE on Unix.
@return	how many tables dropped + remaining tables in list */
UNIV_INTERN
ulint
ddl_drop_tables_in_background(void)
/*===============================*/
{
	ddl_drop_t*		drop;
	dict_table_t*		table;
	ulint			n_tables;
	ulint			n_tables_dropped = 0;
loop:
	mutex_enter(&kernel_mutex);

	if (!ddl_drop_list_inited) {

		UT_LIST_INIT(ddl_drop_list);
		ddl_drop_list_inited = TRUE;
	}

	drop = UT_LIST_GET_FIRST(ddl_drop_list);

	n_tables = UT_LIST_GET_LEN(ddl_drop_list);

	mutex_exit(&kernel_mutex);

	if (drop == NULL) {
		/* All tables dropped */

		return(n_tables + n_tables_dropped);
	}

	mutex_enter(&(dict_sys->mutex));
	table = dict_table_get_low(drop->table_name);
	mutex_exit(&(dict_sys->mutex));

	if (table == NULL) {
		/* If for some reason the table has already been dropped
		through some other mechanism, do not try to drop it */

		goto already_dropped;
	}

	if (DB_SUCCESS != ddl_drop_table_in_background(drop->table_name)) {
		/* If the DROP fails for some table, we return, and let the
		main thread retry later */

		return(n_tables + n_tables_dropped);
	}

	n_tables_dropped++;

already_dropped:
	mutex_enter(&kernel_mutex);

	UT_LIST_REMOVE(ddl_drop_list, ddl_drop_list, drop);

	ut_print_timestamp(ib_stream);
	ib_logger(ib_stream, "  InnoDB: Dropped table ");
	ut_print_name(ib_stream, NULL, TRUE, drop->table_name);
	ib_logger(ib_stream, " in background drop queue.\n");

	mem_free(drop->table_name);

	mem_free(drop);

	mutex_exit(&kernel_mutex);

	goto loop;
}

/*************************************************************************
Get the background drop list length. NOTE: the caller must own the kernel
mutex!
@return	how many tables in list */
UNIV_INTERN
ulint
ddl_get_background_drop_list_len_low(void)
/*======================================*/
{
	ut_ad(mutex_own(&kernel_mutex));

	if (!ddl_drop_list_inited) {

		UT_LIST_INIT(ddl_drop_list);
		ddl_drop_list_inited = TRUE;
	}

	return(UT_LIST_GET_LEN(ddl_drop_list));
}

/*************************************************************************
If a table is not yet in the drop list, adds the table to the list of tables
which the master thread drops in background. We need this on Unix because in
ALTER TABLE may call drop table even if the table has running queries on
it. Also, if there are running foreign key checks on the table, we drop the
table lazily.
@return	TRUE if the table was not yet in the drop list, and was added there */
static
ibool
ddl_add_table_to_background_drop_list(
/*==================================*/
	const char*	name)	/*!< in: table name */
{
	ddl_drop_t*	drop;

	mutex_enter(&kernel_mutex);

	if (!ddl_drop_list_inited) {

		UT_LIST_INIT(ddl_drop_list);
		ddl_drop_list_inited = TRUE;
	}

	/* Look if the table already is in the drop list */
	drop = UT_LIST_GET_FIRST(ddl_drop_list);

	while (drop != NULL) {
		if (strcmp(drop->table_name, name) == 0) {
			/* Already in the list */

			mutex_exit(&kernel_mutex);

			return(FALSE);
		}

		drop = UT_LIST_GET_NEXT(ddl_drop_list, drop);
	}

	drop = mem_alloc(sizeof(ddl_drop_t));

	drop->table_name = mem_strdup(name);

	UT_LIST_ADD_LAST(ddl_drop_list, ddl_drop_list, drop);

	/*	ib_logger(ib_stream, "InnoDB: Adding table ");
	ut_print_name(ib_stream, trx, TRUE, drop->table_name);
	ib_logger(ib_stream, " to background drop list\n"); */

	mutex_exit(&kernel_mutex);

	return(TRUE);
}

/*************************************************************************
Drops a table but does not commit the transaction.  If the
name of the dropped table ends in one of "innodb_monitor",
"innodb_lock_monitor", "innodb_tablespace_monitor",
"innodb_table_monitor", then this will also stop the printing of
monitor output by the master thread.
@return	error code or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_drop_table(
/*===========*/
	const char*	name,	/*!< in: table name */
	trx_t*		trx,	/*!< in: transaction handle */
	ibool		drop_db)/*!< in: TRUE=dropping whole database */
{
	dict_foreign_t*	foreign;
	dict_table_t*	table;
	ulint		space_id;
	enum db_err	err;
	const char*	table_name;
	ulint		namelen;
	pars_info_t*    info			= NULL;

	ut_a(name != NULL);

	if (srv_created_new_raw) {
		ib_logger(ib_stream,
		      "InnoDB: A new raw disk partition was initialized:\n"
		      "InnoDB: we do not allow database modifications"
		      " by the user.\n"
		      "InnoDB: Shut down the server and edit your config file "
		      "so that newraw is replaced with raw.\n");

		return(DB_ERROR);
	}

	trx->op_info = "dropping table";

	/* The table name is prefixed with the database name and a '/'.
	Certain table names starting with 'innodb_' have their special
	meaning regardless of the database name.  Thus, we need to
	ignore the database name prefix in the comparisons. */
	table_name = strchr(name, '/');
	ut_a(table_name);
	table_name++;
	namelen = strlen(table_name) + 1;

	if (namelen == sizeof S_innodb_monitor
	    && !memcmp(table_name, S_innodb_monitor,
		       sizeof S_innodb_monitor)) {

		/* Table name equals "innodb_monitor":
		stop monitor prints */

		srv_print_innodb_monitor = FALSE;
		srv_print_innodb_lock_monitor = FALSE;
	} else if (namelen == sizeof S_innodb_lock_monitor
		   && !memcmp(table_name, S_innodb_lock_monitor,
			      sizeof S_innodb_lock_monitor)) {
		srv_print_innodb_monitor = FALSE;
		srv_print_innodb_lock_monitor = FALSE;
	} else if (namelen == sizeof S_innodb_tablespace_monitor
		   && !memcmp(table_name, S_innodb_tablespace_monitor,
			      sizeof S_innodb_tablespace_monitor)) {

		srv_print_innodb_tablespace_monitor = FALSE;
	} else if (namelen == sizeof S_innodb_table_monitor
		   && !memcmp(table_name, S_innodb_table_monitor,
			      sizeof S_innodb_table_monitor)) {

		srv_print_innodb_table_monitor = FALSE;
	}

	/* Serialize data dictionary operations with dictionary mutex:
	no deadlocks can occur then in these operations */

	if (trx->dict_operation_lock_mode != RW_X_LATCH) {
		return(DB_SCHEMA_NOT_LOCKED);
	}

	ut_ad(mutex_own(&(dict_sys->mutex)));
#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

	table = dict_table_get_low(name);

	if (!table) {
		err = DB_TABLE_NOT_FOUND;
		ut_print_timestamp(ib_stream);

		ib_logger(ib_stream,
		      "  InnoDB: Error: table ");
		ut_print_name(ib_stream, trx, TRUE, name);
		ib_logger(ib_stream,
		      " does not exist in the InnoDB internal\n"
		      "InnoDB: data dictionary though the client is"
		      " trying to drop it.\n"
		      "InnoDB: You can look for further help on the\n"
		      "InnoDB: InnoDB website. Check the site for details\n");
		goto func_exit;
	}

	/* Check if the table is referenced by foreign key constraints from
	some other table (not the table itself) */

	foreign = UT_LIST_GET_FIRST(table->referenced_list);

	while (foreign && foreign->foreign_table == table) {
check_next_foreign:
		foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
	}

	if (foreign && trx->check_foreigns
	    && !(drop_db && dict_tables_have_same_db(
			 name, foreign->foreign_table_name))) {
		/* We only allow dropping a referenced table if
		FOREIGN_KEY_CHECKS is set to 0 */

		err = DB_CANNOT_DROP_CONSTRAINT;

		mutex_enter(&dict_foreign_err_mutex);
		ut_print_timestamp(ib_stream);

		ib_logger(ib_stream, "  Cannot drop table ");
		ut_print_name(ib_stream, trx, TRUE, name);
		ib_logger(ib_stream, "\nbecause it is referenced by ");
		ut_print_name(ib_stream,
			trx, TRUE, foreign->foreign_table_name);
		ib_logger(ib_stream, "\n");
		mutex_exit(&dict_foreign_err_mutex);

		goto func_exit;
	}

	if (foreign && trx->check_foreigns) {
		goto check_next_foreign;
	}

	if (table->n_handles_opened > 0) {
		ibool	added;

		added = ddl_add_table_to_background_drop_list(table->name);

		if (added) {
			ut_print_timestamp(ib_stream);
			ib_logger(ib_stream,
				"  InnoDB: Warning: Client is"
				" trying to drop table (%lu) ",
				(ulint) table->id.low);
			ut_print_name(ib_stream, trx, TRUE, table->name);
			ib_logger(ib_stream, "\n"
			      "InnoDB: though there are still"
			      " open handles to it.\n"
			      "InnoDB: Adding the table to the"
			      " background drop queue.\n");

			/* We return DB_SUCCESS though the drop will
			happen lazily later */
			err = DB_SUCCESS;
		} else {
			/* The table is already in the background drop list */
			err = DB_TABLESPACE_DELETED;
		}

		goto func_exit;
	}

	/* TODO: could we replace the counter n_foreign_key_checks_running
	with lock checks on the table? Acquire here an exclusive lock on the
	table, and rewrite lock0lock.c and the lock wait in srv0srv.c so that
	they can cope with the table having been dropped here? Foreign key
	checks take an IS or IX lock on the table. */

	if (table->n_foreign_key_checks_running > 0) {

		const char*	table_name = table->name;
		ibool		added;

		added = ddl_add_table_to_background_drop_list(table_name);

		if (added) {
			ut_print_timestamp(ib_stream);
			ib_logger(ib_stream,
				"  InnoDB: You are trying to drop table ");
			ut_print_name(ib_stream, trx, TRUE, table_name);
			ib_logger(ib_stream, "\n"
			      "InnoDB: though there is a"
			      " foreign key check running on it.\n"
			      "InnoDB: Adding the table to"
			      " the background drop queue.\n");

			/* We return DB_SUCCESS though the drop will
			happen lazily later */

			err = DB_SUCCESS;
		} else {
			/* The table is already in the background drop list */
			err = DB_TABLESPACE_DELETED;
		}

		goto func_exit;
	}

	/* Remove any locks there are on the table or its records */

	lock_remove_all_on_table(table, TRUE);

	trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);
	trx->table_id = table->id;

#if 0
	ib_logger(ib_stream, "Dropping: %ld\n", (long) table->id.low);
#endif

	/* We use the private SQL parser of Innobase to generate the
	query graphs needed in deleting the dictionary data from system
	tables in Innobase. Deleting a row from SYS_INDEXES table also
	frees the file segments of the B-tree associated with the index. */

	info = pars_info_create();

	pars_info_add_str_literal(info, "table_name", name);

	err = que_eval_sql(info,
			   "PROCEDURE DROP_TABLE_PROC () IS\n"
			   "sys_foreign_id CHAR;\n"
			   "table_id CHAR;\n"
			   "index_id CHAR;\n"
			   "foreign_id CHAR;\n"
			   "found INT;\n"
			   "BEGIN\n"
			   "SELECT ID INTO table_id\n"
			   "FROM SYS_TABLES\n"
			   "WHERE NAME = :table_name\n"
			   "LOCK IN SHARE MODE;\n"
			   "IF (SQL % NOTFOUND) THEN\n"
			   "       RETURN;\n"
			   "END IF;\n"
			   "found := 1;\n"
			   "SELECT ID INTO sys_foreign_id\n"
			   "FROM SYS_TABLES\n"
			   "WHERE NAME = 'SYS_FOREIGN'\n"
			   "LOCK IN SHARE MODE;\n"
			   "IF (SQL % NOTFOUND) THEN\n"
			   "       found := 0;\n"
			   "END IF;\n"
			   "IF (:table_name = 'SYS_FOREIGN') THEN\n"
			   "       found := 0;\n"
			   "END IF;\n"
			   "IF (:table_name = 'SYS_FOREIGN_COLS') THEN\n"
			   "       found := 0;\n"
			   "END IF;\n"
			   "WHILE found = 1 LOOP\n"
			   "       SELECT ID INTO foreign_id\n"
			   "       FROM SYS_FOREIGN\n"
			   "       WHERE FOR_NAME = :table_name\n"
			   "               AND TO_BINARY(FOR_NAME)\n"
			   "                 = TO_BINARY(:table_name)\n"
			   "               LOCK IN SHARE MODE;\n"
			   "       IF (SQL % NOTFOUND) THEN\n"
			   "               found := 0;\n"
			   "       ELSE\n"
			   "               DELETE FROM SYS_FOREIGN_COLS\n"
			   "               WHERE ID = foreign_id;\n"
			   "               DELETE FROM SYS_FOREIGN\n"
			   "               WHERE ID = foreign_id;\n"
			   "       END IF;\n"
			   "END LOOP;\n"
			   "found := 1;\n"
			   "WHILE found = 1 LOOP\n"
			   "       SELECT ID INTO index_id\n"
			   "       FROM SYS_INDEXES\n"
			   "       WHERE TABLE_ID = table_id\n"
			   "       LOCK IN SHARE MODE;\n"
			   "       IF (SQL % NOTFOUND) THEN\n"
			   "               found := 0;\n"
			   "       ELSE\n"
			   "               DELETE FROM SYS_FIELDS\n"
			   "               WHERE INDEX_ID = index_id;\n"
			   "               DELETE FROM SYS_INDEXES\n"
			   "               WHERE ID = index_id\n"
			   "               AND TABLE_ID = table_id;\n"
			   "       END IF;\n"
			   "END LOOP;\n"
			   "DELETE FROM SYS_COLUMNS\n"
			   "WHERE TABLE_ID = table_id;\n"
			   "DELETE FROM SYS_TABLES\n"
			   "WHERE ID = table_id;\n"
			   "END;\n"
			   , FALSE, trx);

	if (err != DB_SUCCESS) {

		if (err != DB_OUT_OF_FILE_SPACE) {
			ib_logger(ib_stream,
				"InnoDB: Error: unexpected err: %d", err);
			ut_error;
		}

		err = DB_MUST_GET_MORE_FILE_SPACE;

		ib_handle_errors(&err, trx, NULL, NULL);

		ut_error;
	} else {
		ibool		is_path;
		const char*	name_or_path;
		mem_heap_t*	heap;

		heap = mem_heap_create(200);

		/* Clone the name, in case it has been allocated
		from table->heap, which will be freed by
		dict_table_remove_from_cache(table) below. */
		name = mem_heap_strdup(heap, name);
		space_id = table->space;

		if (table->dir_path_of_temp_table != NULL) {
			is_path = TRUE;
			name_or_path = mem_heap_strdup(
				heap, table->dir_path_of_temp_table);
		} else {
			is_path = FALSE;
			name_or_path = name;
		}

		dict_table_remove_from_cache(table);

		// FIXME: srv_force_recovery should be passed in as an arg
		if (dict_load_table(srv_force_recovery, name) != NULL) {
			ut_print_timestamp(ib_stream);
			ib_logger(ib_stream,
				"  InnoDB: Error: not able to remove table ");
			ut_print_name(ib_stream, trx, TRUE, name);
			ib_logger(ib_stream,
				" from the dictionary cache!\n");
			err = DB_ERROR;
		}

		/* Do not drop possible .ibd tablespace if something went
		wrong: we do not want to delete valuable data of the user */

		if (err == DB_SUCCESS && space_id > 0) {
			if (!fil_space_for_table_exists_in_mem(space_id,
							       name_or_path,
							       is_path,
							       FALSE, TRUE)) {
				err = DB_SUCCESS;

				ib_logger(ib_stream,
					"InnoDB: We removed now the InnoDB"
					" internal data dictionary entry\n"
					"InnoDB: of table ");
				ut_print_name(ib_stream, trx, TRUE, name);
				ib_logger(ib_stream, ".\n");
			} else if (!fil_delete_tablespace(space_id)) {
				ib_logger(ib_stream,
					"InnoDB: We removed now the InnoDB"
					" internal data dictionary entry\n"
					"InnoDB: of table ");
				ut_print_name(ib_stream, trx, TRUE, name);
				ib_logger(ib_stream, ".\n");

				ut_print_timestamp(ib_stream);
				ib_logger(ib_stream,
					"  InnoDB: Error: not able to"
					" delete tablespace %lu of table ",
					(ulong) space_id);
				ut_print_name(ib_stream, trx, TRUE, name);
				ib_logger(ib_stream, "!\n");
				err = DB_ERROR;
			}
		}

		mem_heap_free(heap);
	}

func_exit:

	trx->op_info = "";

#ifndef UNIV_HOTBACKUP
	srv_wake_master_thread();
#endif /* !UNIV_HOTBACKUP */

	return((int) err);
}

/* Evaluates to true if str1 equals str2_onstack, used for comparing
the above strings. */
#define STR_EQ(str1, str1_len, str2_onstack) \
	((str1_len) == sizeof(str2_onstack) \
	 && memcmp(str1, str2_onstack, sizeof(str2_onstack)) == 0)

/*************************************************************************
Creates a table, if the name of the table ends in one of "innodb_monitor",
"innodb_lock_monitor", "innodb_tablespace_monitor", "innodb_table_monitor",
then this will also start the printing of monitor output by the master
thread. If the table name ends in "innodb_mem_validate", InnoDB will
try to invoke mem_validate().
@return	error code or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_create_table(
/*=============*/
	dict_table_t*	table,	/*!< in: table definition */
	trx_t*		trx)	/*!< in: transaction handle */
{
	tab_node_t*	node;
	mem_heap_t*	heap;
	que_thr_t*	thr;
	const char*	table_name;
	ulint		table_name_len;
	ulint		err;
	ulint		i;

	ut_ad(trx->client_thread_id == os_thread_get_curr_id());
#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(trx->dict_operation_lock_mode == RW_X_LATCH);

	if (srv_created_new_raw) {
		ib_logger(ib_stream,
		      "InnoDB: A new raw disk partition was initialized:\n"
		      "InnoDB: we do not allow database modifications"
		      " by the user.\n"
		      "InnoDB: Shut down the database and edit your config "
		      "file so that newraw is replaced with raw.\n");
err_exit:
		dict_mem_table_free(table);

		return(DB_ERROR);

	/* The table name is prefixed with the database name and a '/'.
	Certain table names starting with 'innodb_' have their special
	meaning regardless of the database name.  Thus, we need to
	ignore the database name prefix in the comparisons. */
	} else if (strchr(table->name, '/') == NULL) {
		ib_logger(ib_stream, "  InnoDB: Error: table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream,
			"not prefixed with a database name and '/'\n");
		goto err_exit;
	}

	trx->op_info = "creating table";

	/* Check that no reserved column names are used. */
	for (i = 0; i < dict_table_get_n_user_cols(table); i++) {
		if (dict_col_name_is_reserved(
			    dict_table_get_col_name(table, i))) {

			goto err_exit;
		}
	}

	table_name = strchr(table->name, '/');
	table_name++;
	table_name_len = strlen(table_name) + 1;

	if (STR_EQ(table_name, table_name_len, S_innodb_monitor)) {

		/* Table equals "innodb_monitor":
		start monitor prints */

		srv_print_innodb_monitor = TRUE;

		/* The lock timeout monitor thread also takes care
		of InnoDB monitor prints */

		os_event_set(srv_lock_timeout_thread_event);
	} else if (STR_EQ(table_name, table_name_len,
			  S_innodb_lock_monitor)) {

		srv_print_innodb_monitor = TRUE;
		srv_print_innodb_lock_monitor = TRUE;
		os_event_set(srv_lock_timeout_thread_event);
	} else if (STR_EQ(table_name, table_name_len,
			  S_innodb_tablespace_monitor)) {

		srv_print_innodb_tablespace_monitor = TRUE;
		os_event_set(srv_lock_timeout_thread_event);
	} else if (STR_EQ(table_name, table_name_len,
			  S_innodb_table_monitor)) {

		srv_print_innodb_table_monitor = TRUE;
		os_event_set(srv_lock_timeout_thread_event);
	} else if (STR_EQ(table_name, table_name_len,
			  S_innodb_mem_validate)) {
		/* We define here a debugging feature intended for
		developers */

		ib_logger(ib_stream,
		      "Validating InnoDB memory:\n"
		      "to use this feature you must compile InnoDB with\n"
		      "UNIV_MEM_DEBUG defined in univ.i and"
		      " the server must be\n"
		      "quiet because allocation from a mem heap"
		      " is not protected\n"
		      "by any semaphore.\n");
#ifdef UNIV_MEM_DEBUG
		ut_a(mem_validate());
		ib_logger(ib_stream, "Memory validated\n");
#else /* UNIV_MEM_DEBUG */
		ib_logger(ib_stream,
			"Memory NOT validated (recompile with "
			"UNIV_MEM_DEBUG)\n");
#endif /* UNIV_MEM_DEBUG */
	}

	heap = mem_heap_create(512);

	trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

	node = tab_create_graph_create(table, heap, FALSE);

	thr = pars_complete_graph_for_exec(node, trx, heap);

	ut_a(thr == que_fork_start_command(que_node_get_parent(thr)));
	que_run_threads(thr);

	err = trx->error_state;

	if (UNIV_UNLIKELY(err != DB_SUCCESS)) {
		trx->error_state = DB_SUCCESS;
	}

	switch (err) {
	case DB_OUT_OF_FILE_SPACE:
		ut_print_timestamp(ib_stream);
		ib_logger(ib_stream, "  InnoDB: Warning: cannot create table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream, " because tablespace full\n");

		if (dict_table_get_low(table->name)) {

			ddl_drop_table(table->name, trx, FALSE);
		}
		break;

	case DB_DUPLICATE_KEY:
		ut_print_timestamp(ib_stream);
		ib_logger(ib_stream, "  InnoDB: Error: table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream,
		      " already exists in InnoDB internal\n"
		      "InnoDB: data dictionary.\n"
		      "InnoDB: You can look for further help on\n"
		      "InnoDB: the InnoDB website\n");

		/* We may also get err == DB_ERROR if the .ibd file for the
		table already exists */

		break;
	}

	que_graph_free((que_t*) que_node_get_parent(thr));

	trx->op_info = "";

	return((int) err);
}

/*************************************************************************
Does an index creation operation.
@return	error number or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_create_index(
/*=============*/
	dict_index_t*	index,		/*!< in: index definition */
	trx_t*		trx)		/*!< in: transaction handle */
{
	ulint		err;
	que_thr_t*	thr;		/* Query thread */
	ind_node_t*	node;		/* Index creation node */
	mem_heap_t*	heap;		/* Memory heap */

#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */
	ut_ad(mutex_own(&(dict_sys->mutex)));

	/* This heap is destroyed when the query graph is freed. */
	heap = mem_heap_create(512);

	node = ind_create_graph_create(index, heap, FALSE);
	thr = pars_complete_graph_for_exec(node, trx, heap);

	ut_a(thr == que_fork_start_command(que_node_get_parent(thr)));

	que_run_threads(thr);

	err = trx->error_state;

	que_graph_free((que_t*) que_node_get_parent(thr));

	return(err);
}

/*************************************************************************
Truncates a table
@return	error code or DB_SUCCESS */
UNIV_INTERN
enum db_err
ddl_truncate_table(
/*===============*/
	dict_table_t*	table,	/*!< in: table handle */
	trx_t*		trx)	/*!< in: transaction handle */
{
	dict_foreign_t*	foreign;
	enum db_err	err;
	mem_heap_t*	heap;
	byte*		buf;
	dtuple_t*	tuple;
	dfield_t*	dfield;
	dict_index_t*	sys_index;
	btr_pcur_t	pcur;
	mtr_t		mtr;
	dulint		new_id;
	ulint		recreate_space = 0;
	pars_info_t*	info = NULL;

	/* How do we prevent crashes caused by ongoing operations on
	the table? Old operations could try to access non-existent
	pages.

	1) SQL queries, INSERT, SELECT, ...: we must get an exclusive
	table lock on the table before we can do TRUNCATE TABLE. Ensure
	there are no running queries on the table. This guarantee has
	to be provided by the SQL layer.

	2) Purge and rollback: we assign a new table id for the
	table. Since purge and rollback look for the table based on
	the table id, they see the table as 'dropped' and discard
	their operations.

	3) Insert buffer: TRUNCATE TABLE is analogous to DROP TABLE,
	so we do not have to remove insert buffer records, as the
	insert buffer works at a low level. If a freed page is later
	reallocated, the allocator will remove the ibuf entries for
	it.

	When we truncate *.ibd files by recreating them (analogous to
	DISCARD TABLESPACE), we remove all entries for the table in the
	insert buffer tree.  This is not strictly necessary, because
	in 6) we will assign a new tablespace identifier, but we can
	free up some space in the system tablespace.

	4) Linear readahead and random readahead: we use the same
	method as in 3) to discard ongoing operations. (This is only
	relevant for TRUNCATE TABLE by DISCARD TABLESPACE.)

	5) FOREIGN KEY operations: if
	table->n_foreign_key_checks_running > 0, we do not allow the
	TRUNCATE. We also reserve the data dictionary latch.

	6) Crash recovery: To prevent the application of pre-truncation
	redo log records on the truncated tablespace, we will assign
	a new tablespace identifier to the truncated tablespace. */

	if (srv_created_new_raw) {
		ib_logger(ib_stream,
		      "InnoDB: A new raw disk partition was initialized:\n"
		      "InnoDB: we do not allow database modifications"
		      " by the user.\n"
		      "InnoDB: Shut down server and edit config file so "
		      "that newraw is replaced with raw.\n");

		return(DB_ERROR);
	}

	trx->op_info = "truncating table";

	/* Serialize data dictionary operations with dictionary mutex:
	no deadlocks can occur then in these operations */
	ut_a(trx->dict_operation_lock_mode != 0);

	/* Prevent foreign key checks etc. while we are truncating the
	table */
	ut_ad(mutex_own(&(dict_sys->mutex)));

#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

	/* Check if the table is referenced by foreign key constraints from
	some other table (not the table itself) */

	foreign = UT_LIST_GET_FIRST(table->referenced_list);

	while (foreign && foreign->foreign_table == table) {
		foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
	}

	if (foreign && trx->check_foreigns) {
		/* We only allow truncating a referenced table if
		FOREIGN_KEY_CHECKS is set to 0 */

		mutex_enter(&dict_foreign_err_mutex);
		ut_print_timestamp(ib_stream);

		ib_logger(ib_stream, "  Cannot truncate table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream, " by DROP+CREATE\n"
		      "InnoDB: because it is referenced by ");
		ut_print_name(ib_stream,
			trx, TRUE, foreign->foreign_table_name);
		ib_logger(ib_stream, "\n");
		mutex_exit(&dict_foreign_err_mutex);

		err = DB_ERROR;
		goto func_exit;
	}

	/* TODO: could we replace the counter n_foreign_key_checks_running
	with lock checks on the table? Acquire here an exclusive lock on the
	table, and rewrite lock0lock.c and the lock wait in srv0srv.c so that
	they can cope with the table having been truncated here? Foreign key
	checks take an IS or IX lock on the table. */

	if (table->n_foreign_key_checks_running > 0) {
		ut_print_timestamp(ib_stream);
		ib_logger(ib_stream, "  InnoDB: Cannot truncate table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream, " by DROP+CREATE\n"
		      "InnoDB: because there is a foreign key check"
		      " running on it.\n");
		err = DB_ERROR;

		goto func_exit;
	}

	/* Remove all locks except the table-level S and X locks. */
	lock_remove_all_on_table(table, FALSE);

	trx->table_id = table->id;

	if (table->space && !table->dir_path_of_temp_table) {
		/* Discard and create the single-table tablespace. */
		ulint	space	= table->space;
		ulint	flags	= fil_space_get_flags(space);

		if (flags != ULINT_UNDEFINED
		    && fil_discard_tablespace(space)) {

			dict_index_t*	index;

			space = 0;

			if (fil_create_new_single_table_tablespace(
				    &space, table->name, FALSE, flags,
				    FIL_IBD_FILE_INITIAL_SIZE) != DB_SUCCESS) {
				ut_print_timestamp(ib_stream);
				ib_logger(ib_stream,
					"  InnoDB: TRUNCATE TABLE %s failed to"
					" create a new tablespace\n",
					table->name);
				table->ibd_file_missing = 1;
				err = DB_ERROR;
				goto func_exit;
			}

			recreate_space = space;

			/* Replace the space_id in the data dictionary cache.
			The persisent data dictionary (SYS_TABLES.SPACE
			and SYS_INDEXES.SPACE) are updated later in this
			function. */
			table->space = space;
			index = dict_table_get_first_index(table);
			do {
				index->space = space;
				index = dict_table_get_next_index(index);
			} while (index);

			mtr_start(&mtr);
			fsp_header_init(space,
					FIL_IBD_FILE_INITIAL_SIZE, &mtr);
			mtr_commit(&mtr);
		}
	}

	/* scan SYS_INDEXES for all indexes of the table */
	heap = mem_heap_create(800);

	tuple = dtuple_create(heap, 1);
	dfield = dtuple_get_nth_field(tuple, 0);

	buf = mem_heap_alloc(heap, 8);
	mach_write_to_8(buf, table->id);

	dfield_set_data(dfield, buf, 8);
	sys_index = dict_table_get_first_index(dict_sys->sys_indexes);
	dict_index_copy_types(tuple, sys_index, 1);

	mtr_start(&mtr);
	btr_pcur_open_on_user_rec(sys_index, tuple, PAGE_CUR_GE,
				  BTR_MODIFY_LEAF, &pcur, &mtr);
	for (;;) {
		rec_t*		rec;
		const byte*	field;
		ulint		len;
		ulint		root_page_no;

		if (!btr_pcur_is_on_user_rec(&pcur)) {
			/* The end of SYS_INDEXES has been reached. */
			break;
		}

		rec = btr_pcur_get_rec(&pcur);

		field = rec_get_nth_field_old(rec, 0, &len);
		ut_ad(len == 8);

		if (memcmp(buf, field, len) != 0) {
			/* End of indexes for the table (TABLE_ID mismatch). */
			break;
		}

		if (rec_get_deleted_flag(rec, FALSE)) {
			/* The index has been dropped. */
			goto next_rec;
		}

		/* This call may commit and restart mtr
		and reposition pcur. */
		root_page_no = dict_truncate_index_tree(table, recreate_space,
							&pcur, &mtr);

		rec = btr_pcur_get_rec(&pcur);

		if (root_page_no != FIL_NULL) {
			page_rec_write_index_page_no(
				rec, DICT_SYS_INDEXES_PAGE_NO_FIELD,
				root_page_no, &mtr);
			/* We will need to commit and restart the
			mini-transaction in order to avoid deadlocks.
			The dict_truncate_index_tree() call has allocated
			a page in this mini-transaction, and the rest of
			this loop could latch another index page. */
			mtr_commit(&mtr);
			mtr_start(&mtr);
			btr_pcur_restore_position(BTR_MODIFY_LEAF,
						  &pcur, &mtr);
		}

next_rec:
		btr_pcur_move_to_next_user_rec(&pcur, &mtr);
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);

	mem_heap_free(heap);

	new_id = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);

	info = pars_info_create();

	pars_info_add_int4_literal(info, "space", (lint) table->space);
	pars_info_add_dulint_literal(info, "old_id", table->id);
	pars_info_add_dulint_literal(info, "new_id", new_id);

	err = que_eval_sql(info,
			   "PROCEDURE RENUMBER_TABLESPACE_PROC () IS\n"
			   "BEGIN\n"
			   "UPDATE SYS_TABLES"
			   " SET ID = :new_id, SPACE = :space\n"
			   " WHERE ID = :old_id;\n"
			   "UPDATE SYS_COLUMNS SET TABLE_ID = :new_id\n"
			   " WHERE TABLE_ID = :old_id;\n"
			   "UPDATE SYS_INDEXES"
			   " SET TABLE_ID = :new_id, SPACE = :space\n"
			   " WHERE TABLE_ID = :old_id;\n"
			   "COMMIT WORK;\n"
			   "END;\n"
			   , FALSE, trx);

	if (err != DB_SUCCESS) {
		trx->error_state = DB_SUCCESS;
		trx_rollback(trx, FALSE, NULL);
		trx->error_state = DB_SUCCESS;
		ut_print_timestamp(ib_stream);
		ib_logger(ib_stream,
		      "  InnoDB: Unable to assign a new identifier to table ");
		ut_print_name(ib_stream, trx, TRUE, table->name);
		ib_logger(ib_stream, "\n"
		      "InnoDB: after truncating it.  Background processes"
		      " may corrupt the table!\n");
		err = DB_ERROR;
	} else {
		dict_table_change_id_in_cache(table, new_id);
	}

	dict_update_statistics(table);

func_exit:

	trx->op_info = "";

	srv_wake_master_thread();

	return(err);
}

/*************************************************************************
Drops an index.
@return	error code or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_drop_index(
/*===========*/
	dict_table_t*	table,		/*!< in: table instance */
	dict_index_t*	index,		/*!< in: index to drop */
	trx_t*		trx)		/*!< in: transaction handle */
{
	ulint		err = DB_SUCCESS;
	pars_info_t*	info = pars_info_create();

	/* We use the private SQL parser of Innobase to generate the
	query graphs needed in deleting the dictionary data from system
	tables in Innobase. Deleting a row from SYS_INDEXES table also
	frees the file segments of the B-tree associated with the index. */

	static const char str1[] =
		"PROCEDURE DROP_INDEX_PROC () IS\n"
		"BEGIN\n"
		/* Rename the index, so that it will be dropped by
		row_merge_drop_temp_indexes() at crash recovery
		if the server crashes before this trx is committed. */
		"UPDATE SYS_INDEXES SET NAME=CONCAT('"
		TEMP_INDEX_PREFIX_STR "', NAME) WHERE ID = :indexid;\n"
		"COMMIT WORK;\n"
		/* Drop the field definitions of the index. */
		"DELETE FROM SYS_FIELDS WHERE INDEX_ID = :indexid;\n"
		/* Drop the index definition and the B-tree. */
		"DELETE FROM SYS_INDEXES WHERE ID = :indexid;\n"
		"END;\n";

	ut_ad(index && table && trx);

	pars_info_add_dulint_literal(info, "indexid", index->id);

	trx_start_if_not_started(trx);
	trx->op_info = "dropping index";

	ut_a(trx->dict_operation_lock_mode == RW_X_LATCH);

	err = que_eval_sql(info, str1, FALSE, trx);

	ut_a(err == DB_SUCCESS);

	/* Replace this index with another equivalent index for all
	foreign key constraints on this table where this index is used */

	dict_table_replace_index_in_foreign_list(table, index);
	dict_index_remove_from_cache(table, index);

	trx->op_info = "";
	return(err);
}

/********************************************************************
Delete a single constraint.
@return	error code or DB_SUCCESS */
static
int
ddl_delete_constraint_low(
/*======================*/
	const char*	id,		/*!< in: constraint id */
	trx_t*		trx)		/*!< in: transaction handle */
{
	pars_info_t*	info = pars_info_create();

	pars_info_add_str_literal(info, "id", id);

	return((int) que_eval_sql(
		info,
		"PROCEDURE DELETE_CONSTRAINT () IS\n"
		"BEGIN\n"
		"DELETE FROM SYS_FOREIGN_COLS WHERE ID = :id;\n"
		"DELETE FROM SYS_FOREIGN WHERE ID = :id;\n"
		"END;\n"
		, FALSE, trx));
}

/********************************************************************
Delete a single constraint.
@return	error code or DB_SUCCESS */
static
int
ddl_delete_constraint(
/*==================*/
	const char*	id,		/*!< in: constraint id */
	const char*	database_name,	/*!< in: database name, with the
					trailing '/' */
	mem_heap_t*	heap,		/*!< in: memory heap */
	trx_t*		trx)		/*!< in: transaction handle */
{
	ulint		err;

	/* New format constraints have ids <databasename>/<constraintname>. */
	err = ddl_delete_constraint_low(
		mem_heap_strcat(heap, database_name, id), trx);

	if (err == DB_SUCCESS && !strchr(id, '/')) {
		/* Old format < 4.0.18 constraints have constraint ids
		<number>_<number>. We only try deleting them if the
		constraint name does not contain a '/' character, otherwise
		deleting a new format constraint named 'foo/bar' from
		database 'baz' would remove constraint 'bar' from database
		'foo', if it existed. */

		err = ddl_delete_constraint_low(id, trx);
	}

	return((int) err);
}

/*************************************************************************
Renames a table.
@return	error code or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_rename_table(
/*=============*/
	const char*	old_name,	/*!< in: old table name */
	const char*	new_name,	/*!< in: new table name */
	trx_t*		trx)		/*!< in: transaction handle */
{
	dict_table_t*	table;
	ulint		err			= DB_ERROR;
	mem_heap_t*	heap			= NULL;
	const char**	constraints_to_drop	= NULL;
	ulint		n_constraints_to_drop	= 0;
	pars_info_t*	info			= NULL;

	ut_a(old_name != NULL);
	ut_a(new_name != NULL);
	ut_ad(trx->client_thread_id == os_thread_get_curr_id());

	if (srv_created_new_raw || srv_force_recovery != IB_RECOVERY_DEFAULT) {
		ib_logger(ib_stream,
		      "InnoDB: A new raw disk partition was initialized or\n"
		      "InnoDB: innodb_force_recovery is on: we do not allow\n"
		      "InnoDB: database modifications by the user. Shut down\n"
		      "InnoDB: the server and ensure that newraw is replaced\n"
		      "InnoDB: with raw, and innodb_force_... is removed.\n");

		goto func_exit;
	}

	trx->op_info = "renaming table";

	table = dict_table_get_low(old_name);

	if (!table) {
		err = DB_TABLE_NOT_FOUND;
		goto func_exit;
	} else if (table->ibd_file_missing) {
		err = DB_TABLE_NOT_FOUND;
		goto func_exit;
	}

	/* We use the private SQL parser of Innobase to generate the query
	graphs needed in updating the dictionary data from system tables. */

	info = pars_info_create();

	pars_info_add_str_literal(info, "new_table_name", new_name);
	pars_info_add_str_literal(info, "old_table_name", old_name);

	err = que_eval_sql(
		info,
		"PROCEDURE RENAME_TABLE () IS\n"
		"BEGIN\n"
		"UPDATE SYS_TABLES SET NAME = :new_table_name\n"
		" WHERE NAME = :old_table_name;\n"
		"END;\n"
		, FALSE, trx);

	if (err == DB_SUCCESS) {
		/* Rename all constraints. */

		info = pars_info_create();

		pars_info_add_str_literal(info, "new_table_name", new_name);
		pars_info_add_str_literal(info, "old_table_name", old_name);

		err = que_eval_sql(
			info,
			"PROCEDURE RENAME_CONSTRAINT_IDS () IS\n"
			"gen_constr_prefix CHAR;\n"
			"new_db_name CHAR;\n"
			"foreign_id CHAR;\n"
			"new_foreign_id CHAR;\n"
			"old_db_name_len INT;\n"
			"old_t_name_len INT;\n"
			"new_db_name_len INT;\n"
			"id_len INT;\n"
			"found INT;\n"
			"BEGIN\n"
			"found := 1;\n"
			"old_db_name_len := INSTR(:old_table_name, '/')-1;\n"
			"new_db_name_len := INSTR(:new_table_name, '/')-1;\n"
			"new_db_name := SUBSTR(:new_table_name, 0,\n"
			"                      new_db_name_len);\n"
			"old_t_name_len := LENGTH(:old_table_name);\n"
			"gen_constr_prefix := CONCAT(:old_table_name,\n"
			"                            '_ibfk_');\n"
			"WHILE found = 1 LOOP\n"
			"       SELECT ID INTO foreign_id\n"
			"        FROM SYS_FOREIGN\n"
			"        WHERE FOR_NAME = :old_table_name\n"
			"         AND TO_BINARY(FOR_NAME)\n"
			"           = TO_BINARY(:old_table_name)\n"
			"         LOCK IN SHARE MODE;\n"
			"       IF (SQL % NOTFOUND) THEN\n"
			"        found := 0;\n"
			"       ELSE\n"
			"        UPDATE SYS_FOREIGN\n"
			"        SET FOR_NAME = :new_table_name\n"
			"         WHERE ID = foreign_id;\n"
			"        id_len := LENGTH(foreign_id);\n"
			"        IF (INSTR(foreign_id, '/') > 0) THEN\n"
			"               IF (INSTR(foreign_id,\n"
			"                         gen_constr_prefix) > 0)\n"
			"               THEN\n"
			"                new_foreign_id :=\n"
			"                CONCAT(:new_table_name,\n"
			"                SUBSTR(foreign_id, old_t_name_len,\n"
			"                       id_len - old_t_name_len));\n"
			"               ELSE\n"
			"                new_foreign_id :=\n"
			"                CONCAT(new_db_name,\n"
			"                SUBSTR(foreign_id,\n"
			"                       old_db_name_len,\n"
			"                       id_len - old_db_name_len));\n"
			"               END IF;\n"
			"               UPDATE SYS_FOREIGN\n"
			"                SET ID = new_foreign_id\n"
			"                WHERE ID = foreign_id;\n"
			"               UPDATE SYS_FOREIGN_COLS\n"
			"                SET ID = new_foreign_id\n"
			"                WHERE ID = foreign_id;\n"
			"        END IF;\n"
			"       END IF;\n"
			"END LOOP;\n"
			"UPDATE SYS_FOREIGN SET REF_NAME = :new_table_name\n"
			"WHERE REF_NAME = :old_table_name\n"
			"  AND TO_BINARY(REF_NAME)\n"
			"    = TO_BINARY(:old_table_name);\n"
			"END;\n"
			, FALSE, trx);

	} else if (n_constraints_to_drop > 0) {
		/* Drop some constraints of tmp tables. */

		ulint	i;
		char*	db_name;
		ulint	db_name_len;

		db_name_len = dict_get_db_name_len(old_name) + 1;
		db_name = mem_heap_strdupl(heap, old_name, db_name_len);

		for (i = 0; i < n_constraints_to_drop; i++) {
			err = ddl_delete_constraint(
				constraints_to_drop[i], db_name, heap, trx);

			if (err != DB_SUCCESS) {
				break;
			}
		}
	}

	if (err != DB_SUCCESS) {
		if (err == DB_DUPLICATE_KEY) {
			ut_print_timestamp(ib_stream);
			ib_logger(ib_stream,
			      "  InnoDB: Error; possible reasons:\n"
			      "InnoDB: 1) Table rename would cause"
			      " two FOREIGN KEY constraints\n"
			      "InnoDB: to have the same internal name"
			      " in case-insensitive comparison.\n"
			      " trying to rename table.\n" 
			      "InnoDB: If table ");
			ut_print_name(ib_stream, trx, TRUE, new_name);
			ib_logger(ib_stream,
			      " is a temporary table, then it can be that\n"
			      "InnoDB: there are still queries running"
			      " on the table, and it will be\n"
			      "InnoDB: dropped automatically when"
			      " the queries end.\n");
		}
		trx->error_state = DB_SUCCESS;
		trx_rollback(trx, FALSE, NULL);
		trx->error_state = DB_SUCCESS;
	} else {
		/* The following call will also rename the .ibd data file if
		the table is stored in a single-table tablespace */

		if (!dict_table_rename_in_cache(table, new_name, TRUE)) {
			trx->error_state = DB_SUCCESS;
			trx_rollback(trx, FALSE, NULL);
			trx->error_state = DB_SUCCESS;
			goto func_exit;
		}

		/* We only want to switch off some of the type checking in
		an ALTER, not in a RENAME. */

		err = dict_load_foreigns(new_name, trx->check_foreigns);

		if (err != DB_SUCCESS) {
			ibool	ret;

			ut_print_timestamp(ib_stream);

			ib_logger(ib_stream,
				"  InnoDB: Error: in RENAME TABLE"
				" table ");
			ut_print_name(ib_stream, trx, TRUE, new_name);
			ib_logger(ib_stream, "\n"
				"InnoDB: is referenced in"
				" foreign key constraints\n"
				"InnoDB: which are not compatible"
				" with the new table definition.\n");

			ret = dict_table_rename_in_cache(table,old_name, FALSE);
			ut_a(ret);

			trx->error_state = DB_SUCCESS;
			trx_rollback(trx, FALSE, NULL);
			trx->error_state = DB_SUCCESS;
		}
	}

func_exit:

	if (UNIV_LIKELY_NULL(heap)) {
		mem_heap_free(heap);
	}

	trx->op_info = "";

	return(err);
}

/*************************************************************************
Renames an index.
@return	error code or DB_SUCCESS */
UNIV_INTERN
ulint
ddl_rename_index(
/*=============*/
	const char*	table_name,	/*!< in: table that owns the index */
	const char*	old_name,	/*!< in: old table name */
	const char*	new_name,	/*!< in: new table name */
	trx_t*		trx)		/*!< in: transaction handle */
{
	dict_table_t*	table;
	pars_info_t*	info = NULL;
	ulint		err = DB_ERROR;

	ut_a(old_name != NULL);
	ut_a(old_name != NULL);
	ut_a(table_name != NULL);
	ut_ad(trx->client_thread_id == os_thread_get_curr_id());

	if (srv_created_new_raw || srv_force_recovery != IB_RECOVERY_DEFAULT) {
		ib_logger(ib_stream,
		      "InnoDB: A new raw disk partition was initialized or\n"
		      "InnoDB: innodb_force_recovery is on: we do not allow\n"
		      "InnoDB: database modifications by the user. Shut down\n"
		      "InnoDB: the server and ensure that newraw is replaced\n"
		      "InnoDB: with raw, and innodb_force_... is removed.\n");

		goto func_exit;
	}

	trx->op_info = "renaming index";

	table = dict_table_get_low(table_name);

	if (!table || table->ibd_file_missing) {
		err = DB_TABLE_NOT_FOUND;
		goto func_exit;
	}

	/* We use the private SQL parser of Innobase to generate the query
	graphs needed in updating the dictionary data from system tables. */

	info = pars_info_create();

	pars_info_add_str_literal(info, "table_name", table_name);
	pars_info_add_str_literal(info, "new_index_name", new_name);
	pars_info_add_str_literal(info, "old_index_name", old_name);

	err = que_eval_sql(
		info,
		"PROCEDURE RENAME_TABLE () IS\n"
		"table_id CHAR;\n"
		"BEGIN\n"
		"SELECT ID INTO table_id\n"
		" FROM SYS_TABLES\n"
		" WHERE NAME = :table_name\n"
		"LOCK IN SHARE MODE;\n"
		"IF (SQL % NOTFOUND) THEN\n"
		" RETURN;\n"
		"END IF;\n"
		"UPDATE SYS_INDEXES SET NAME = :new_index_name\n"
		" WHERE NAME = :old_index_name\n"
		"  AND table_id = table_id;\n"
		"END;\n"
		, FALSE, trx);

	if (err == DB_SUCCESS) {
		dict_index_t*	index;

		index = dict_table_get_first_index(table);

		do {
			/* FIXME: We are leaking memory here, well sort
			of, since the previous name allocation will not
			be freed till the index instance is destroyed. */
			if (strcasecmp(index->name, old_name) == 0) {
				index->name = mem_heap_strdup(
					index->heap, new_name);

				break;
			}

			index = dict_table_get_next_index(index);
		} while (index);

	} else {
		trx->error_state = DB_SUCCESS;
		trx_rollback(trx, FALSE, NULL);
	}

func_exit:

	trx->op_info = "";

	return(err);
}

/***********************************************************************
Drop all foreign keys in a database, see Bug#18942.
@return	error code or DB_SUCCESS */
static
enum db_err
ddl_drop_all_foreign_keys_in_db(
/*============================*/
	const char*	name,	/*!< in: database name which ends to '/' */
	trx_t*		trx)	/*!< in: transaction handle */
{
	enum db_err	err;
	pars_info_t*	pinfo;

	ut_a(name[strlen(name) - 1] == '/');

	pinfo = pars_info_create();

	pars_info_add_str_literal(pinfo, "dbname", name);

/* true if for_name is not prefixed with dbname */
#define TABLE_NOT_IN_THIS_DB \
"SUBSTR(for_name, 0, LENGTH(:dbname)) <> :dbname"

	err = que_eval_sql(pinfo,
			   "PROCEDURE DROP_ALL_FOREIGN_KEYS_PROC () IS\n"
			   "foreign_id CHAR;\n"
			   "for_name CHAR;\n"
			   "found INT;\n"
			   "DECLARE CURSOR cur IS\n"
			   "SELECT ID, FOR_NAME FROM SYS_FOREIGN\n"
			   "WHERE FOR_NAME >= :dbname\n"
			   "LOCK IN SHARE MODE\n"
			   "ORDER BY FOR_NAME;\n"
			   "BEGIN\n"
			   "found := 1;\n"
			   "OPEN cur;\n"
			   "WHILE found = 1 LOOP\n"
			   "        FETCH cur INTO foreign_id, for_name;\n"
			   "        IF (SQL % NOTFOUND) THEN\n"
			   "                found := 0;\n"
			   "        ELSIF (" TABLE_NOT_IN_THIS_DB ") THEN\n"
			   "                found := 0;\n"
			   "        ELSIF (1=1) THEN\n"
			   "                DELETE FROM SYS_FOREIGN_COLS\n"
			   "                WHERE ID = foreign_id;\n"
			   "                DELETE FROM SYS_FOREIGN\n"
			   "                WHERE ID = foreign_id;\n"
			   "        END IF;\n"
			   "END LOOP;\n"
			   "CLOSE cur;\n"
			   "END;\n",
			   FALSE, /* do not reserve dict mutex,
				  we are already holding it */
			   trx);

	return(err);
}

/*************************************************************************
Drops a database.
@return	error code or DB_SUCCESS */
UNIV_INTERN
enum db_err
ddl_drop_database(
/*==============*/
	const char*	name,	/*!< in: database name which ends in '/' */
	trx_t*		trx)	/*!< in: transaction handle */
{
	char*		table_name;
	enum db_err	err = DB_SUCCESS;
	ulint		namelen	= ut_strlen(name);

	ut_a(name[namelen - 1] == '/');
	ut_ad(trx->client_thread_id == os_thread_get_curr_id());

	trx->op_info = "dropping database";

loop:
	dict_lock_data_dictionary(trx);

	while ((table_name = dict_get_first_table_name_in_db(name))) {
		dict_table_t*	table;

		ut_a(memcmp(table_name, name, namelen) == 0);

		table = dict_table_get_low(table_name);

		ut_a(table);

		/* Wait until the user does not have any queries running on
		the table */

		if (table->n_handles_opened > 0) {
			dict_unlock_data_dictionary(trx);

			ut_print_timestamp(ib_stream);
			ib_logger(ib_stream,
				"  InnoDB: Warning: The client is trying to"
			      	" drop database ");
			ut_print_name(ib_stream, trx, TRUE, name);
			ib_logger(ib_stream, "\n"
			      "InnoDB: though there are still"
			      " open handles to table ");
			ut_print_name(ib_stream, trx, TRUE, table_name);
			ib_logger(ib_stream, ".\n");

			os_thread_sleep(1000000);

			mem_free(table_name);

			goto loop;
		}

		err = ddl_drop_table(table_name, trx, TRUE);

		if (err != DB_SUCCESS) {
			ib_logger(ib_stream, "InnoDB: DROP DATABASE ");
			ut_print_name(ib_stream, trx, TRUE, name);
			ib_logger(ib_stream,
				" failed with error %lu for table ",
				(ulint) err);
			ut_print_name(ib_stream, trx, TRUE, table_name);
			ib_logger(ib_stream, "\n");
			mem_free(table_name);
			break;
		}

		mem_free(table_name);
	}

	if (err == DB_SUCCESS) {
		/* After dropping all tables try to drop all leftover
		foreign keys in case orphaned ones exist */
		err = ddl_drop_all_foreign_keys_in_db(name, trx);

		if (err != DB_SUCCESS) {
			ib_logger(ib_stream, "InnoDB: DROP DATABASE ");
			ut_print_name(ib_stream, trx, TRUE, name);
			ib_logger(ib_stream, " failed with error %d while "
				"dropping all foreign keys", err);
		}
	}

	dict_unlock_data_dictionary(trx);

	trx->op_info = "";

	return(err);
}

/*********************************************************************//**
Drop all partially created indexes. */
UNIV_INTERN
void
ddl_drop_all_temp_indexes(
/*======================*/
	ib_recovery_t	recovery)	/*!< in: recovery level setting */
{
	trx_t*		trx;
	btr_pcur_t	pcur;
	mtr_t		mtr;
	ibool		started;

	/* Load the table definitions that contain partially defined
	indexes, so that the data dictionary information can be checked
	when accessing the tablename.ibd files. */
	trx = trx_allocate_for_background();
	started = trx_start(trx, ULINT_UNDEFINED);
	ut_a(started);
	trx->op_info = "dropping partially created indexes";
	dict_lock_data_dictionary(trx);

	mtr_start(&mtr);

	btr_pcur_open_at_index_side(
		TRUE,
		dict_table_get_first_index(dict_sys->sys_indexes),
		BTR_SEARCH_LEAF, &pcur, TRUE, &mtr);

	for (;;) {
		const rec_t*	rec;
		ulint		len;
		const byte*	field;
		dict_table_t*	table;
		dulint		table_id;

		btr_pcur_move_to_next_user_rec(&pcur, &mtr);

		if (!btr_pcur_is_on_user_rec(&pcur)) {
			break;
		}

		rec = btr_pcur_get_rec(&pcur);
		field = rec_get_nth_field_old(rec, DICT_SYS_INDEXES_NAME_FIELD,
					      &len);
		if (len == UNIV_SQL_NULL || len == 0
		    || mach_read_from_1(field) != (ulint) TEMP_INDEX_PREFIX) {
			continue;
		}

		/* This is a temporary index. */

		field = rec_get_nth_field_old(rec, 0/*TABLE_ID*/, &len);
		if (len != 8) {
			/* Corrupted TABLE_ID */
			continue;
		}

		table_id = mach_read_from_8(field);

		btr_pcur_store_position(&pcur, &mtr);
		btr_pcur_commit_specify_mtr(&pcur, &mtr);

		table = dict_load_table_on_id(recovery, table_id);

		if (table) {
			dict_index_t*	index;

			for (index = dict_table_get_first_index(table);
			     index; index = dict_table_get_next_index(index)) {

				if (*index->name == TEMP_INDEX_PREFIX) {
					ddl_drop_index(table, index, trx);
					trx_commit(trx);
				}
			}
		}

		mtr_start(&mtr);
		btr_pcur_restore_position(BTR_SEARCH_LEAF, &pcur, &mtr);
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);

	dict_unlock_data_dictionary(trx);

	trx_commit(trx);
	trx_free_for_background(trx);
}

/*********************************************************************//**
Drop all temporary tables. */
UNIV_INTERN
void
ddl_drop_all_temp_tables(
/*=====================*/
	ib_recovery_t	recovery)	/*!< in: recovery level setting*/
{
	trx_t*		trx;
	btr_pcur_t	pcur;
	mtr_t		mtr;
	mem_heap_t*	heap;
	ibool		started;

	trx = trx_allocate_for_background();
	started = trx_start(trx, ULINT_UNDEFINED);
	trx->op_info = "dropping temporary tables";
	dict_lock_data_dictionary(trx);

	heap = mem_heap_create(200);

	mtr_start(&mtr);

	btr_pcur_open_at_index_side(
		TRUE,
		dict_table_get_first_index(dict_sys->sys_tables),
		BTR_SEARCH_LEAF, &pcur, TRUE, &mtr);

	for (;;) {
		const rec_t*	rec;
		ulint		len;
		const byte*	field;
		dict_table_t*	table;
		const char*	table_name;

		btr_pcur_move_to_next_user_rec(&pcur, &mtr);

		if (!btr_pcur_is_on_user_rec(&pcur)) {
			break;
		}

		rec = btr_pcur_get_rec(&pcur);
		field = rec_get_nth_field_old(rec, 4/*N_COLS*/, &len);
		if (len != 4 || !(mach_read_from_4(field) & 0x80000000UL)) {
			continue;
		}

		/* Because this is not a ROW_FORMAT=REDUNDANT table,
		the is_temp flag is valid.  Examine it. */

		field = rec_get_nth_field_old(rec, 7/*MIX_LEN*/, &len);
		if (len != 4
		    || !(mach_read_from_4(field) & DICT_TF2_TEMPORARY)) {
			continue;
		}

		/* This is a temporary table. */
		field = rec_get_nth_field_old(rec, 0/*NAME*/, &len);
		if (len == UNIV_SQL_NULL || len == 0) {
			/* Corrupted SYS_TABLES.NAME */
			continue;
		}

		table_name = mem_heap_strdupl(heap, (const char*) field, len);

		btr_pcur_store_position(&pcur, &mtr);
		btr_pcur_commit_specify_mtr(&pcur, &mtr);

		table = dict_load_table(recovery, table_name);

		if (table) {
			ddl_drop_table(table_name, trx, FALSE);
			trx_commit(trx);
		}

		mtr_start(&mtr);
		btr_pcur_restore_position(BTR_SEARCH_LEAF, &pcur, &mtr);
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);
	mem_heap_free(heap);

	dict_unlock_data_dictionary(trx);

	trx_commit(trx);
	trx_free_for_background(trx);
}

