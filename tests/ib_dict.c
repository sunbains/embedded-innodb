/** Copyright (c) 2008, 2009 Innobase Oy. All rights reserved.
Copyright (c) 2008, 2009 Oracle. All rights reserved.

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

/* Test to check if we can list all tables in the InnoDB data dictionary
including the system tables.
 Create a database
 CREATE TABLE t1(C1 INT, C2 UNSIGNED INT, c3 VARCHAR(10) NOT NULL, PK(C1, C2)); 
 ...
 CREATE TABLE tn(C1 INT, C2 UNSIGNED INT, c3 VARCHAR(10) NOT NULL, PK(C1, C2)); 
 Print the schema using the API */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE	"dict_test"
#define TABLE		"t"

typedef struct visitor_arg {
	FILE*		fp;
	ib_trx_t	ib_trx;
	const char*	table_name;
	int		cur_col;
	int		n_indexes;
	int		n_table_cols;
	int		n_index_cols;
} visitor_arg_t;

/** Read a table's schema and print it. */
static
int
visit_tables(
	void*		arg,		/*!< in: callback argument */
	const char*	name,		/*!< in: table name */
	int		len);		/*!< in: table name length */

/** Callback functions to list table nams. */
static
int
visit_table_list(
	void*		arg,		/*!< User callback arg */
	const char*	name,		/*!< Table name */
	ib_tbl_fmt_t	tbl_fmt,	/*!< Table type */
	ib_ulint_t	page_size,	/*!< Table page size */
	int		n_cols,		/*!< No. of cols defined */
	int		n_indexes)	/*!< No. of indexes defined */
{
	(void) arg;
	(void) name;
	(void) page_size;
	(void) tbl_fmt;
	(void) n_cols;
	(void) n_indexes;

	return(0);
}

/** Callback functions to traverse a table's schema. */
static
int
visit_table(
	void*		arg,		/*!< User callback arg */
	const char*	name,		/*!< Table name */
	ib_tbl_fmt_t	tbl_fmt,	/*!< Table type */
	ib_ulint_t	page_size,	/*!< Table page size */
	int		n_cols,		/*!< No. of cols defined */
	int		n_indexes)	/*!< No. of indexes defined */
{
	const char*	p;
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;
	FILE*	fp = (FILE*) visitor_arg->fp;

	(void) page_size;
	(void) tbl_fmt;

	p = strchr(name, '/');
	if (p == NULL) {
		p = name;
	} else {
		++p;
	}

	fprintf(fp, "CREATE TABLE %s(\n", p);

	visitor_arg->cur_col = 0;
	visitor_arg->table_name = name;
	visitor_arg->n_table_cols = n_cols;
	visitor_arg->n_indexes = n_indexes;

	return(0);
}

/** Callback functions to traverse a table's schema. */
static
int
visit_table_col(
	void*		arg,		/*!< User callback arg */
	const char*	name,		/*!< Column name */
	ib_col_type_t	col_type,	/*!< Column type */
	ib_ulint_t	len,		/*!< Column len */
	ib_col_attr_t	attr)		/*!< Column attributes */
{
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;
	FILE*	fp = (FILE*) visitor_arg->fp;

	if (visitor_arg->cur_col > 0 && col_type != IB_SYS) {
		fprintf(fp, ",\n");
	}

	++visitor_arg->cur_col;

	/* Ignore system columns. */
	if (col_type != IB_SYS) {
		fprintf(fp, "\t%-16s\t", name);
	} else {
		if (visitor_arg->cur_col == visitor_arg->n_table_cols) {
			fprintf(fp, ");\n");
		}
		return(0);
	}

	switch (col_type) {
	case IB_VARCHAR:
	case IB_VARCHAR_ANYCHARSET:
		if (len > 0) {
			fprintf(fp, "VARCHAR(%d)", (int) len);
		} else {
			fprintf(fp, "TEXT");
		}
		break;
	case IB_CHAR:
	case IB_CHAR_ANYCHARSET:
		fprintf(fp, "CHAR(%d)", (int) len);
		break;
	case IB_BINARY:
		fprintf(fp, "BINARY(%d)", (int) len);
		break;
	case IB_VARBINARY:
		if (len > 0) {
			fprintf(fp, "VARBINARY(%d)", (int) len);
		} else {
			fprintf(fp, "VARBINARY");
		}
		break;

	case IB_BLOB:
		fprintf(fp, "BLOB");
		break;
	case IB_INT:
		if (attr & IB_COL_UNSIGNED) {
			fprintf(fp, "UNSIGNED ");
		}

		switch (len) {
		case 1:
			fprintf(fp, "TINYINT");
			break;
		case 2:
			fprintf(fp, "SMALLINT");
			break;
		case 4:
			fprintf(fp, "INT");
			break;
		case 8:
			fprintf(fp, "BIGINT");
			break;
		default:
			abort();
		}
		break;
	case IB_FLOAT:
		fprintf(fp, "FLOAT");
		break;
	case IB_DOUBLE:
		fprintf(fp, "DOUBLE");
		break;

	case IB_DECIMAL:
		fprintf(fp, "DECIMAL");
		break;
	default:
		fprintf(fp, "UNKNOWN");
		break;
	}

	if (attr & IB_COL_NOT_NULL) {
		fprintf(fp, " NOT NULL");
	}

	return(0);
}

/** Callback functions to traverse a table's schema. */
static
int
visit_index(
	void*		arg,		/*!< User callback arg */
	const char*	name,		/*!< Index name */
	ib_bool_t	clustered,	/*!< True if clustered */
	ib_bool_t	unique,		/*!< True if unique */
	int		n_cols)		/*!< No. of cols defined */
{
	const char*	p;
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;
	FILE*	fp = (FILE*) visitor_arg->fp;

	(void) clustered;

	visitor_arg->cur_col = 0;
	visitor_arg->n_index_cols = n_cols;

	p = strchr(visitor_arg->table_name, '/');
	if (p == NULL) {
		p = visitor_arg->table_name;
	} else {
		++p;
	}

	fprintf(fp,
		"CREATE %s INDEX %s ON %s(",
		(unique ? "UNIQUE" : ""),
		name,
		p);

	return(0);
}

/** Callback functions to traverse a table's schema. */
int
visit_index_col(
	void*		arg,		/*!< User callback arg */
	const char*	name,		/*!< Column name */
	ib_ulint_t	prefix_len)	/*!< Prefix length */
{
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;
	FILE*	fp = (FILE*) visitor_arg->fp;

	++visitor_arg->cur_col;
	fprintf(fp, "%s", name);

	if (visitor_arg->cur_col < visitor_arg->n_index_cols) {
		fprintf(fp, ",");
	} else {
		fprintf(fp, ");\n");
	}

	return(0);
}

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

/** Create a table using InnoDB's internal SQL parser. */
static
ib_err_t
create_table(
	const char*	dbname,			/*!< in: database name */
	const char*	name,			/*!< in: table name */
	int		n)			/*!< in: table suffix */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s%d", dbname, name, n);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);
#endif

	/* Pass a table page size of 0, ie., use default page size. */
	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "C1",
		IB_VARCHAR, IB_COL_NONE, 0, 10);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "C2",
		IB_VARCHAR, IB_COL_NONE, 0, 10);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "C3",
		IB_INT, IB_COL_UNSIGNED, 0, sizeof(ib_u32_t));

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "C1", 0);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "C2", 0);
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

/* Check whether NULL callbacks work. */
static const ib_schema_visitor_t	table_visitor = {
	IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL,
	visit_table_list,
	NULL,
	NULL,
	NULL	
};

/** Read table names only. */
static
int
visit_tables_list(
	void*		arg,
	const char*	name,
	int		len)
				/*!< out: 0 on success, nonzero on error */
{
	ib_err_t	err;
	char*		ptr = malloc(len + 1);
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;

	memset(ptr, 0x0, len + 1);

	strncpy(ptr, name, len);

	err = ib_table_schema_visit(
		visitor_arg->ib_trx, ptr, &table_visitor, arg);

	free(ptr);

	return(err == DB_SUCCESS ? 0 : -1);
}

static const ib_schema_visitor_t	visitor = {
	IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL,
	visit_table,
	visit_table_col,
	visit_index,
	visit_index_col
};

/** Read a table's schema and print it. */
static
int
visit_tables(
	void*		arg,
	const char*	name,
	int		len)
				/*!< out: 0 on success, nonzero on error */
{
	ib_err_t	err;
	char*		ptr = malloc(len + 1);
	visitor_arg_t*	visitor_arg = (visitor_arg_t*) arg;

	memset(ptr, 0x0, len + 1);

	strncpy(ptr, name, len);

	err = ib_table_schema_visit(visitor_arg->ib_trx, ptr, &visitor, arg);

	free(ptr);

	return(err == DB_SUCCESS ? 0 : -1);
}

/** Read the system tables  schema and print it.
@return	0 on success, nonzero on error */
static
int
visit_sys_tables(void)
{
	ib_err_t	err;
	visitor_arg_t	arg;
	ib_trx_t	ib_trx;

	ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

	memset(&arg, 0x0, sizeof(arg));

	arg.fp = stdout;
	arg.ib_trx = ib_trx;

	err = ib_table_schema_visit(ib_trx, "SYS_TABLES", &visitor, &arg);
	assert(err == DB_SCHEMA_NOT_LOCKED);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_visit(ib_trx, "SYS_TABLES", &visitor, &arg);
	assert(err == DB_SUCCESS);
	err = ib_table_schema_visit(ib_trx, "SYS_COLUMNS", &visitor, &arg);
	assert(err == DB_SUCCESS);
	err = ib_table_schema_visit(ib_trx, "SYS_TABLES", &visitor, &arg);
	assert(err == DB_SUCCESS);
	err = ib_table_schema_visit(ib_trx, "SYS_INDEXES", &visitor, &arg);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(ib_trx);

	return(err == DB_SUCCESS ? 0 : -1);
}

/** Iterate over all the tables in the InnoDB data dictionary and print
their schemas.
@return	DB_SUCCESS or error code */
static
ib_err_t
print_entire_schema(void)
{
	ib_err_t	err;
	visitor_arg_t	arg;

	memset(&arg, 0x0, sizeof(arg));
	arg.fp = stdout;
	arg.ib_trx = ib_trx_begin(IB_TRX_SERIALIZABLE);

	err = ib_schema_lock_exclusive(arg.ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_schema_tables_iterate(arg.ib_trx, visit_tables_list, &arg);
	assert(err == DB_SUCCESS);

	err = ib_schema_tables_iterate(arg.ib_trx, visit_tables, &arg);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(arg.ib_trx);
	assert(err == DB_SUCCESS);

	return(err);
}

/** Drop the table.
@return	DB_SUCCESS or error code */
static
ib_err_t
drop_table_n(
	const char*	dbname,	/*!< in: database name */
	const char*	name,	/*!< in: table to drop */
	int		n)	/*!< in: count */
{
	ib_trx_t	ib_trx;
	ib_err_t	err = DB_SUCCESS;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s%d", dbname, name, n);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s%d", dbname, name, n);
#endif

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_drop(ib_trx, table_name);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	return(err);
}

int
main(int argc, char* argv[])
{
	int		i;
	ib_err_t	err;

	(void) argc;
	(void) argv;

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	visit_sys_tables();

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	for (i = 0; i < 10; i++) {
		err = create_table(DATABASE, TABLE, i);
		assert(err == DB_SUCCESS);
	}

	err = print_entire_schema();
	assert(err == DB_SUCCESS);

	for (i = 0; i < 10; ++i) {
		err = drop_table_n(DATABASE, TABLE, i);
		assert(err == DB_SUCCESS);
	}

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

#ifdef UNIV_DEBUG_VALGRIND
	VALGRIND_DO_LEAK_CHECK;
#endif

	return(EXIT_SUCCESS);
}
