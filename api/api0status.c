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

#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#include "univ.i"
#include "srv0srv.h"
#include "api0api.h"
#include "api0ucode.h"

/** InnoDB status variables types. */
typedef enum {
	IB_STATUS_IBOOL,		/*!< Boolean status variable, ibool */
	IB_STATUS_I64,			/*!< ib_int64_t status variable */
	IB_STATUS_ULINT			/*!< uling status variable */
} ib_status_type_t;

/** InnoDB status variables */
typedef struct {
	const char*		name;	/*!< Status variable name */

	ib_status_type_t	type;	/*!< Status varable type */

	const void*		val;	/*!< Pointer to status value */
} ib_status_t;

/* All status variables that a user can query. */
static const ib_status_t status_vars[] = {
	/* IO system related */
	{"read_req_pending",		IB_STATUS_ULINT,
		&export_vars.innodb_data_pending_reads},

	{"write_req_pending",		IB_STATUS_ULINT,
		&export_vars.innodb_data_pending_writes},

	{"fsync_req_pending",		IB_STATUS_ULINT,
		&export_vars.innodb_data_pending_fsyncs},

	{"write_req_done",		IB_STATUS_ULINT,
		&export_vars.innodb_data_writes},

	{"read_req_done",		IB_STATUS_ULINT,
		&export_vars.innodb_data_reads},

	{"fsync_req_done",		IB_STATUS_ULINT,
		&export_vars.innodb_data_fsyncs},

	{"bytes_total_written",		IB_STATUS_ULINT,
		&export_vars.innodb_data_written},

	{"bytes_total_read",		IB_STATUS_ULINT,
		&export_vars.innodb_data_read},


	/* Buffer pool related */
	{"buffer_pool_current_size",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_total},

	{"buffer_pool_data_pages",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_data},

	{"buffer_pool_dirty_pages",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_dirty},

	{"buffer_pool_misc_pages",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_misc},

	{"buffer_pool_free_pages",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_free},

	{"buffer_pool_read_reqs",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_read_requests},

	{"buffer_pool_reads",		IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_reads},

	{"buffer_pool_waited_for_free",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_wait_free},

	{"buffer_pool_pages_flushed",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_pages_flushed},

	{"buffer_pool_write_reqs",	IB_STATUS_ULINT,
		&export_vars.innodb_buffer_pool_write_requests},

	{"buffer_pool_total_pages",	IB_STATUS_ULINT,
		&export_vars.innodb_pages_created},

	{"buffer_pool_pages_read",	IB_STATUS_ULINT,
		&export_vars.innodb_pages_read},

	{"buffer_pool_pages_written",	IB_STATUS_ULINT,
		&export_vars.innodb_pages_written},


	/* Double write buffer related */
	{"double_write_pages_written",	IB_STATUS_ULINT,
		&export_vars.innodb_dblwr_pages_written},

	{"double_write_invoked",	IB_STATUS_ULINT,
		&export_vars.innodb_dblwr_writes},


	/* Log related */
	{"log_buffer_slot_waits",	IB_STATUS_ULINT,
		&export_vars.innodb_log_waits},

	{"log_write_reqs",		IB_STATUS_ULINT,
		&export_vars.innodb_log_write_requests},

	{"log_write_flush_count",	IB_STATUS_ULINT,
		&export_vars.innodb_log_writes},

	{"log_bytes_written",		IB_STATUS_ULINT,
		&export_vars.innodb_os_log_written},

	{"log_fsync_req_done",		IB_STATUS_ULINT,
		&export_vars.innodb_os_log_fsyncs},

	{"log_write_req_pending",	IB_STATUS_ULINT,
		&export_vars.innodb_os_log_pending_writes},

	{"log_fsync_req_pending",	IB_STATUS_ULINT,
		&export_vars.innodb_os_log_pending_fsyncs},


	/* Lock related */
	{"lock_row_waits",		IB_STATUS_ULINT,
		&export_vars.innodb_row_lock_waits},

	{"lock_row_waiting",		IB_STATUS_ULINT,
		&export_vars.innodb_row_lock_current_waits},

	{"lock_total_wait_time_in_secs",IB_STATUS_ULINT,
		&export_vars.innodb_row_lock_time},

	{"lock_wait_time_avg_in_secs",	IB_STATUS_ULINT,
		&export_vars.innodb_row_lock_time_avg},

	{"lock_max_wait_time_in_secs",IB_STATUS_ULINT,
		&export_vars.innodb_row_lock_time_max},


	/* Row operations */
	{"row_total_read",		IB_STATUS_ULINT,
		&export_vars.innodb_rows_read},
	{"row_total_inserted",		IB_STATUS_ULINT,
		&export_vars.innodb_rows_inserted},
	{"row_total_updated",		IB_STATUS_ULINT,
		&export_vars.innodb_rows_updated},
	{"row_total_deleted",		IB_STATUS_ULINT,
		&export_vars.innodb_rows_deleted},

	/* Miscellaneous */
	{"page_size",			IB_STATUS_ULINT,
		&export_vars.innodb_page_size},

	{"have_atomic_builtins",	IB_STATUS_IBOOL,
		&export_vars.innodb_have_atomic_builtins},

	{ NULL, 0, 0}};

/*********************************************************************//**
Get a list of the names of all status variables.
The caller is responsible for free(3)ing the returned array of strings
when it is not needed anymore and for not modifying the individual strings.
ib_status_get_all() @{
@return	DB_SUCCESS or error code */

ib_err_t
ib_status_get_all(
/*===========*/
	const char***	names,		/*!< out: pointer to array of strings */
	ib_u32_t*	names_num)	/*!< out: number of strings returned */
{
	ib_u32_t	i;

	*names_num = UT_ARR_SIZE(status_vars);

	*names = (const char**) malloc(*names_num * sizeof(const char*));
	if (*names == NULL) {
		return(DB_OUT_OF_MEMORY);
	}

	for (i = 0; i < *names_num; i++) {
		(*names)[i] = status_vars[i].name;
	}

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//*
Get the status variable that matches name.

@return DB_SUCCESS if found else DB_NOT_FOUND */
static
ib_err_t
ib_status_lookup(
/*=============*/
	const char*	name,		/*!< in: Variable to lookup */
	const ib_status_t** var)	/*!< out: pointer to entry */
{
	const ib_status_t*	ptr;

	*var = NULL;

	for (ptr = status_vars; ptr && ptr->name != NULL; ++ptr) {
		if (ib_utf8_strcasecmp(name, ptr->name) == 0) {
			*var = ptr;
			return(DB_SUCCESS);
		}
	}

	return(DB_NOT_FOUND);
}

/*******************************************************************//**
Get the value of an INT status variable.
@file api/api0status.c

@return	DB_SUCCESS if found and type is INT,
	DB_DATA_MISMATCH if found but type is not INT,
	DB_NOT_FOUND otherwise. */

ib_err_t
ib_status_get_i64(
/*==============*/
	const char*	name,		/*!< in: Status variable name */
	ib_i64_t*	dst)		/*!< out: Variable value */
{
	ib_err_t	err;
	const ib_status_t*	var;

	err = ib_status_lookup(name, &var);

	if (err == DB_SUCCESS) {

		/* Read the latest values into export_vars. */
		srv_export_innodb_status();

		switch (var->type) {
		case IB_STATUS_ULINT:
			*dst = *(ulint*) var->val;
			break;

		case IB_STATUS_IBOOL:
			*dst = *(ibool*) var->val;
			break;

		case IB_STATUS_I64:
			*dst = *(ib_int64_t*) var->val;
			break;

		default:
			/* Currently the status variables are all INTs. If
			we add other types then this will signal to the user
			that the variable type is different. */	
			err = DB_DATA_MISMATCH;
			break;
		}
	}

	return(err);
}

