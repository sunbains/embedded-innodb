/***********************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
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
#include <stdarg.h>

#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#include "univ.i"
#include "api0api.h"
#include "btr0sea.h"
#include "buf0buf.h" /* for buf_pool */
#include "buf0lru.h" /* for buf_LRU_* */
#include "db0err.h"
#include "log0recv.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "trx0sys.h"	/* for trx_sys_file_format_name_to_id() */
#include "os0sync.h"

static	char*	srv_file_flush_method_str = NULL;

/* A point in the LRU list (expressed as a percent), all blocks from this
point onwards (inclusive) are considered "old" blocks. */
static ulint	lru_old_blocks_pct;

/** We copy the argument passed to ib_cfg_set_text("data_file_path")
because srv_parse_data_file_paths_and_sizes() parses it's input argument
destructively. The copy is done using ut_malloc(). */
static	char*	srv_data_file_paths_and_sizes = NULL;

/* enum ib_cfg_flag_t @{ */
typedef enum ib_cfg_flag {
	IB_CFG_FLAG_NONE = 0x1,
	IB_CFG_FLAG_READONLY_AFTER_STARTUP = 0x2,/* can be modified only
						 before innodb is started */
	IB_CFG_FLAG_READONLY = 0x4		/* cannot be modified */
} ib_cfg_flag_t;
/* @} */

/* struct ib_cfg_var_t @{ */
typedef struct ib_cfg_var {
	const char*	name;	/* config var name */
	ib_cfg_type_t	type;	/* config var type */
	ib_cfg_flag_t	flag;	/* config var flag */
	ib_uint64_t	min_val;/* minimum allowed value for numeric types,
				ignored for other types */
	ib_uint64_t	max_val;/* maximum allowed value for numeric types,
				ignored for other types */
	ib_err_t	(*validate)(const struct ib_cfg_var*, const void*); /*
				function used to validate a new variable's
				value when setting it */
	ib_err_t	(*set)(struct ib_cfg_var*, const void*); /* function
				used to set the variable's value */
	ib_err_t	(*get)(const struct ib_cfg_var*, void*); /* function
				used to get the variable's value */
	void*		tank;	/* opaque storage that may be used by the
				set() and get() functions */
} ib_cfg_var_t;
/* @} */

/*******************************************************************//**
@file api/api0cfg.c
Assign src to dst according to type. If this is a string variable (char*)
the string itself is not copied.
ib_cfg_assign() @{
@return	DB_SUCCESS if assigned (type is known) */
static
ib_err_t
ib_cfg_assign(
/*==========*/
	ib_cfg_type_t	type,	/*!< in: type of src and dst */
	void*		dst,	/*!< out: destination */
	const void*	src)	/*!< in: source */
{
	switch (type) {
	case IB_CFG_IBOOL: {

		*(ib_bool_t*) dst = *(const ib_bool_t*) src;
		return(DB_SUCCESS);
	}

	case IB_CFG_ULINT: {

		*(ulint*) dst = *(const ulint*) src;
		return(DB_SUCCESS);
	}

	case IB_CFG_ULONG: {

		*(ulong*) dst = *(const ulong*) src;
		return(DB_SUCCESS);
	}

	case IB_CFG_TEXT: {

		*(char**) dst = *(char**) src;
		return(DB_SUCCESS);
	}

	case IB_CFG_CB: {

		*(ib_cb_t*) dst = *(ib_cb_t*) src;
		return(DB_SUCCESS);
	}
	/* do not add default: in order to produce a compilation
	warning if new type is added which is not handled here */
	}

	/* NOT REACHED */
	return(DB_ERROR);
}
/* @} */

/*******************************************************************//**
A generic function used for ib_cfg_var_t::validate() to check a numeric
type for min/max allowed value overflow.
ib_cfg_var_validate_numeric() @{
@return	DB_SUCCESS if value is in range */
static
ib_err_t
ib_cfg_var_validate_numeric(
/*========================*/
	const struct ib_cfg_var* cfg_var,/*!< in/out: configuration variable to
					check */
	const void*		value)	/*!< in: value to check */
{
	switch (cfg_var->type) {

	case IB_CFG_ULINT: {
		ulint	v;

		v = *(ulint*) value;

		if ((ulint) cfg_var->min_val <= v
		    && v <= (ulint) cfg_var->max_val) {

			return(DB_SUCCESS);
		} else {

			return(DB_INVALID_INPUT);
		}
	}

	case IB_CFG_ULONG: {
		ulong	v;

		v = *(ulong*) value;

		if ((ulong) cfg_var->min_val <= v
		    && v <= (ulong) cfg_var->max_val) {

			return(DB_SUCCESS);
		} else {

			return(DB_INVALID_INPUT);
		}
	}

	default:
		ut_error;
	}

	/* NOT REACHED */
	return(DB_ERROR);
}
/* @} */

/* generic and specific ib_cfg_var_(set|get)_* functions @{ */

/*******************************************************************//**
A generic function used for ib_cfg_var_t::set() that stores the value
of the configuration parameter in the location pointed by
ib_cfg_var_t::tank. If this is a string variable (char*) then the string
is not copied and a reference to "value" is made. It should not be freed
or modified until InnoDB is running or a new value is set.
ib_cfg_var_set_generic() @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_generic(
/*===================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate */
	const void*		value)	/*!< in: value to set */
{
	ib_err_t	ret;

	if (cfg_var->validate != NULL) {
		ret = cfg_var->validate(cfg_var, value);
		if (ret != DB_SUCCESS) {
			return(ret);
		}
	}

	ret = ib_cfg_assign(cfg_var->type, cfg_var->tank, value);

	return(ret);
}
/* @} */

/*******************************************************************//**
A generic function used for ib_cfg_var_t::get() that retrieves the value
of the configuration parameter from the location pointed by
ib_cfg_var_t::tank and stores it in "value". The variable is not copied
to "value" even if it is a string variable (char*). Only a pointer to the
storage is written in "value". It should not be freed unless it was
allocated by the user and set with ib_cfg_set().
ib_cfg_var_get_generic() @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_generic(
/*===================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve */
	void*				value)	/*!< out: place to store
						the retrieved value */
{
	return(ib_cfg_assign(cfg_var->type, value, cfg_var->tank));
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "adaptive_hash_index".
ib_cfg_var_set_adaptive_hash_index() @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_adaptive_hash_index(
/*===============================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be
					"adaptive_hash_index" */
	const void*		value)	/*!< in: value to set, must point to
					ib_bool_t variable */
{
	ut_a(strcasecmp(cfg_var->name, "adaptive_hash_index") == 0);
	ut_a(cfg_var->type == IB_CFG_IBOOL);

	btr_search_enabled = !(*(const ib_bool_t*) value);

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Retrieve the value of the config variable "adaptive_hash_index".
ib_cfg_var_get_adaptive_hash_index() @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_adaptive_hash_index(
/*===============================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve, must be
						"adaptive_hash_index" */
	void*				value)	/*!< out: place to store
						the retrieved value, must
						point to ib_bool_t variable */
{
	ut_a(strcasecmp(cfg_var->name, "adaptive_hash_index") == 0);
	ut_a(cfg_var->type == IB_CFG_IBOOL);

	*(ib_bool_t*) value = !btr_search_enabled;

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "data_file_path".
ib_cfg_var_set_data_file_path() @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_data_file_path(
/*==========================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be
					"data_file_path" */
	const void*		value)	/*!< in: value to set, must point to
					char* variable */
{
	const char*	value_str;

	ut_a(strcasecmp(cfg_var->name, "data_file_path") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	value_str = *(char**) value;

	if (srv_parse_data_file_paths_and_sizes(value_str)) {
		return(DB_SUCCESS);
	} else {
		return(DB_INVALID_INPUT);
	}
}
/* @} */

/*******************************************************************//**
Retrieve the value of the config variable "data_file_path".
ib_cfg_var_get_data_file_path() @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_data_file_path(
/*==========================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve, must be
						"data_file_path" */
	void*				value)	/*!< out: place to store
						the retrieved value, must
						point to char* variable */
{
	ut_a(strcasecmp(cfg_var->name, "data_file_path") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	*(char**) value = srv_data_file_paths_and_sizes;

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "file_format".
ib_cfg_var_set_file_format() @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_file_format(
/*=======================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be "file_format" */
	const void*		value)	/*!< in: value to set, must point to
					char* variable */
{
	ulint		format_id;

	ut_a(strcasecmp(cfg_var->name, "file_format") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	format_id = trx_sys_file_format_name_to_id(*(char**) value);

	if (format_id > DICT_TF_FORMAT_MAX) {
		return(DB_INVALID_INPUT);
	}

	srv_file_format = format_id;

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Retrieve the value of the config variable "file_format".
ib_cfg_var_get_file_format @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_file_format(
/*=======================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve, must be
						"file_format" */
	void*				value)	/*!< out: place to store
						the retrieved value, must
						point to char* variable */
{
	ut_a(strcasecmp(cfg_var->name, "file_format") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	*(const char**) value = trx_sys_file_format_id_to_name(
		srv_file_format);

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Check the value of the config variable "data_home_dir". We need to ensure
that the value ends with a path separator.
ib_cfg_var_validate_data_home_dir() @{
@return	DB_SUCCESS if value is valid */
static
ib_err_t
ib_cfg_var_validate_data_home_dir(
/*==============================*/
	const struct ib_cfg_var*cfg_var,/*!< in/out: configuration variable to
					check, must be "data_home_dir" */
	const void*		value)	/*!< in: value to check, must point to
					char* variable */
{
	ulint		len;
	char*		value_str;

	ut_a(strcasecmp(cfg_var->name, "data_home_dir") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	value_str = *(char**) value;

	len = ut_strlen(value_str);

	/* We simply require that this variable end in a path separator.
	We will normalize it before use internally. */
	if (len == 0
	    || (value_str[len - 1] != '/' && value_str[len - 1] != '\\')) {

		return(DB_INVALID_INPUT);
	}

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "log_group_home_dir".
ib_cfg_var_set_log_group_home_dir @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_log_group_home_dir(
/*==============================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be
					"log_group_home_dir" */
	const void*		value)	/*!< in: value to set, must point to
					char* variable */
{
	const char*	value_str;

	ut_a(strcasecmp(cfg_var->name, "log_group_home_dir") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	ut_a(srv_log_group_home_dir == NULL);

	value_str = *(char**) value;

	if (srv_parse_log_group_home_dirs(value_str)) {
		return(DB_SUCCESS);
	} else {
		return(DB_INVALID_INPUT);
	}
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "flush_method".
ib_cfg_var_set_flush_method@{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_flush_method(
/*========================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be
					"log_group_home_dir" */
	const void*		value)	/*!< in: value to set, must point to
					char* variable */
{
	const char*	value_str;
	ib_err_t	err = DB_SUCCESS;

	ut_a(strcasecmp(cfg_var->name, "flush_method") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	value_str = *(const char**) value;

	os_aio_use_native_aio = FALSE;

#ifndef __WIN__
	if (0 == ut_strcmp(value_str, "fsync")) {
		srv_unix_file_flush_method = SRV_UNIX_FSYNC;
	} else if (0 == ut_strcmp(value_str, "O_DSYNC")) {
		srv_unix_file_flush_method = SRV_UNIX_O_DSYNC;
	} else if (0 == ut_strcmp(value_str, "O_DIRECT")) {
		srv_unix_file_flush_method = SRV_UNIX_O_DIRECT;
	} else if (0 == ut_strcmp(value_str, "littlesync")) {
		srv_unix_file_flush_method = SRV_UNIX_LITTLESYNC;
	} else if (0 == ut_strcmp(value_str, "nosync")) {
		srv_unix_file_flush_method = SRV_UNIX_NOSYNC;
	} else {
		err = DB_INVALID_INPUT;
	}
#else
	if (0 == ut_strcmp(value_str, "normal")) {
		srv_win_file_flush_method = SRV_WIN_IO_NORMAL;
	} else if (0 == ut_strcmp(value_str, "unbuffered")) {
		srv_win_file_flush_method = SRV_WIN_IO_UNBUFFERED;
	} else if (0 == ut_strcmp(value_str, "async_unbuffered")) {
		srv_win_file_flush_method = SRV_WIN_IO_UNBUFFERED;
	} else {
		err = DB_INVALID_INPUT;
	}

	if (err == DB_SUCCESS) {
		switch (os_get_os_version()) {
		case OS_WIN95:
		case OS_WIN31:
		case OS_WINNT:
			/* On Win 95, 98, ME, Win32 subsystem for Windows
			3.1, and NT use simulated aio. In NT Windows provides
		       	async i/o, but when run in conjunction with InnoDB
		       	Hot Backup, it seemed to corrupt the data files. */
	
			os_aio_use_native_aio = FALSE;
			break;
		default:
			/* On Win 2000 and XP use async i/o */
			os_aio_use_native_aio = TRUE;
			break;
		}
	}
#endif

	if (err == DB_SUCCESS) {
		*(const char**) cfg_var->tank = value_str;
	} else {
		*(const char**) cfg_var->tank = NULL;
	}

	return(err);
}
/* @} */
/*******************************************************************//**
Retrieve the value of the config variable "log_group_home_dir".
ib_cfg_var_get_log_group_home_dir() @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_log_group_home_dir(
/*==============================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve, must be
						"log_group_home_dir" */
	void*				value)	/*!< out: place to store
						the retrieved value, must
						point to char* variable */
{
	ut_a(strcasecmp(cfg_var->name, "log_group_home_dir") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	*(const char**) value = NULL;

	return(DB_SUCCESS);
}
/* @} */

/*******************************************************************//**
Set the value of the config variable "lru_old_blocks_pct".
ib_cfg_var_set_lru_old_blocks_pct() @{
@return	DB_SUCCESS if set successfully */
static
ib_err_t
ib_cfg_var_set_lru_old_blocks_pct(
/*==============================*/
	struct ib_cfg_var*	cfg_var,/*!< in/out: configuration variable to
					manipulate, must be
					"lru_old_blocks_pct" */
	const void*		value)	/*!< in: value to set, must point to
					ulint variable */
{
	ibool	adjust_buf_pool;

	ut_a(strcasecmp(cfg_var->name, "lru_old_blocks_pct") == 0);
	ut_a(cfg_var->type == IB_CFG_ULINT);

	if (cfg_var->validate != NULL) {
		ib_err_t	ret;

		ret = cfg_var->validate(cfg_var, value);

		if (ret != DB_SUCCESS) {
			return(ret);
		}
	}

	if (buf_pool != NULL) {
		/* buffer pool has been created */
		adjust_buf_pool = TRUE;
	} else {
		/* buffer pool not yet created, do not attempt to modify it */
		adjust_buf_pool = FALSE;
	}

	lru_old_blocks_pct = buf_LRU_old_ratio_update(
		*(ulint*) value, adjust_buf_pool);

	return(DB_SUCCESS);
}
/* @} */

/* ib_cfg_var_get_generic() is used to get the value of lru_old_blocks_pct */

/* There is no ib_cfg_var_set_version() */

/*******************************************************************//**
Retrieve the value of the config variable "version".
ib_cfg_var_get_version() @{
@return	DB_SUCCESS if retrieved successfully */
static
ib_err_t
ib_cfg_var_get_version(
/*===================*/
	const struct ib_cfg_var*	cfg_var,/*!< in: configuration
						variable whose value to
						retrieve, must be
						"version" */
	void*				value)	/*!< out: place to store
						the retrieved value, must
						point to char* variable */
{
	ut_a(strcasecmp(cfg_var->name, "version") == 0);
	ut_a(cfg_var->type == IB_CFG_TEXT);

	*(const char**) value = VERSION;

	return(DB_SUCCESS);
}
/* @} */

/* @} */

/* cfg_vars_defaults[] @{ */
static const ib_cfg_var_t cfg_vars_defaults[] = {
	{STRUCT_FLD(name,	"adaptive_hash_index"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_adaptive_hash_index),
	 STRUCT_FLD(get,	ib_cfg_var_get_adaptive_hash_index),
	 STRUCT_FLD(tank,	NULL)},

	{STRUCT_FLD(name,	"adaptive_flushing"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_adaptive_flushing)},

	{STRUCT_FLD(name,	"additional_mem_pool_size"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	512 * 1024),
	 STRUCT_FLD(max_val,	IB_UINT64_T_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_mem_pool_size)},

	{STRUCT_FLD(name,	"autoextend_increment"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	1),
	 STRUCT_FLD(max_val,	1000),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_auto_extend_increment)},

	{STRUCT_FLD(name,	"buffer_pool_size"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	5 * 1024 * 1024),
	 STRUCT_FLD(max_val,	ULINT_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_buf_pool_size)},

	{STRUCT_FLD(name,	"checksums"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_use_checksums)},

	{STRUCT_FLD(name,	"data_file_path"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_data_file_path),
	 STRUCT_FLD(get,	ib_cfg_var_get_data_file_path),
	 STRUCT_FLD(tank,	NULL)},

	{STRUCT_FLD(name,	"data_home_dir"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_data_home_dir),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_data_home)},

	{STRUCT_FLD(name,	"doublewrite"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_use_doublewrite_buf)},

	{STRUCT_FLD(name,	"file_format"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL /* validation is done inside
				     ib_cfg_var_set_file_format */),
	 STRUCT_FLD(set,	ib_cfg_var_set_file_format),
	 STRUCT_FLD(get,	ib_cfg_var_get_file_format),
	 STRUCT_FLD(tank,	NULL)},

	{STRUCT_FLD(name,	"file_io_threads"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	NULL /* The ::set() function should never
				     be called because this variable is
				     flagged as IB_CFG_FLAG_READONLY */),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_n_file_io_threads)},
	{STRUCT_FLD(name,	"file_per_table"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_file_per_table)},

	{STRUCT_FLD(name,	"flush_log_at_trx_commit"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	2),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_flush_log_at_trx_commit)},

	{STRUCT_FLD(name,	"flush_method"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_flush_method),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_file_flush_method_str)},

	{STRUCT_FLD(name,	"force_recovery"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	IB_RECOVERY_DEFAULT),
	 STRUCT_FLD(max_val,	IB_RECOVERY_NO_LOG_REDO),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_force_recovery)},

	{STRUCT_FLD(name,	"io_capacity"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	100),
	 STRUCT_FLD(max_val,	1000000),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_io_capacity)},

	{STRUCT_FLD(name,	"lock_wait_timeout"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	1),
	 STRUCT_FLD(max_val,	1024 * 1024 * 1024),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&ses_lock_wait_timeout)},

	{STRUCT_FLD(name,	"log_buffer_size"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	256 * 1024),
	 STRUCT_FLD(max_val,	IB_UINT64_T_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_log_buffer_curr_size)},

	{STRUCT_FLD(name,	"log_file_size"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	1024 * 1024),
	 STRUCT_FLD(max_val,	ULINT_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_log_file_curr_size)},

	{STRUCT_FLD(name,	"log_files_in_group"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	2),
	 STRUCT_FLD(max_val,	100),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_n_log_files)},

	{STRUCT_FLD(name,	"log_group_home_dir"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_log_group_home_dir),
	 STRUCT_FLD(get,	ib_cfg_var_get_log_group_home_dir),
	 STRUCT_FLD(tank,	NULL)},

	{STRUCT_FLD(name,	"max_dirty_pages_pct"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	100),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_max_buf_pool_modified_pct)},

	{STRUCT_FLD(name,	"max_purge_lag"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	IB_UINT64_T_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_max_purge_lag)},

	{STRUCT_FLD(name,	"lru_old_blocks_pct"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	5),
	 STRUCT_FLD(max_val,	95),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_lru_old_blocks_pct),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&lru_old_blocks_pct)},

	{STRUCT_FLD(name,	"lru_block_access_recency"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0xFFFFFFFFUL),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&buf_LRU_old_threshold_ms)},

	{STRUCT_FLD(name,	"open_files"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	10),
	 STRUCT_FLD(max_val,	IB_UINT64_T_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_max_n_open_files)},

	{STRUCT_FLD(name,	"read_io_threads"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	1),
	 STRUCT_FLD(max_val,	64),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_n_read_io_threads)},

	{STRUCT_FLD(name,	"write_io_threads"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY_AFTER_STARTUP),
	 STRUCT_FLD(min_val,	1),
	 STRUCT_FLD(max_val,	64),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_n_write_io_threads)},

	/* New, not present in InnoDB/MySQL */
	{STRUCT_FLD(name,	"pre_rollback_hook"),
	 STRUCT_FLD(type,	IB_CFG_CB),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&recv_pre_rollback_hook)},

	/* New, not present in InnoDB/MySQL */
	{STRUCT_FLD(name,	"print_verbose_log"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_print_verbose_log)},

	/* New, not present in InnoDB/MySQL */
	{STRUCT_FLD(name,	"rollback_on_timeout"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&ses_rollback_on_timeout)},

	{STRUCT_FLD(name,	"stats_sample_pages"),
	 STRUCT_FLD(type,	IB_CFG_ULINT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	1),
	 STRUCT_FLD(max_val,	ULINT_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_stats_sample_pages)},

	{STRUCT_FLD(name,	"status_file"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_innodb_status)},

	{STRUCT_FLD(name,	"sync_spin_loops"),
	 STRUCT_FLD(type,	IB_CFG_ULONG),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	IB_UINT64_T_MAX),
	 STRUCT_FLD(validate,	ib_cfg_var_validate_numeric),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_n_spin_wait_rounds)},

	{STRUCT_FLD(name,	"use_sys_malloc"),
	 STRUCT_FLD(type,	IB_CFG_IBOOL),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_NONE),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	ib_cfg_var_set_generic),
	 STRUCT_FLD(get,	ib_cfg_var_get_generic),
	 STRUCT_FLD(tank,	&srv_use_sys_malloc)},

	{STRUCT_FLD(name,	"version"),
	 STRUCT_FLD(type,	IB_CFG_TEXT),
	 STRUCT_FLD(flag,	IB_CFG_FLAG_READONLY),
	 STRUCT_FLD(min_val,	0),
	 STRUCT_FLD(max_val,	0),
	 STRUCT_FLD(validate,	NULL),
	 STRUCT_FLD(set,	NULL /* The ::set() function should never
				     be called because this variable is
				     flagged as IB_CFG_FLAG_READONLY */),
	 STRUCT_FLD(get,	ib_cfg_var_get_version),
	 STRUCT_FLD(tank,	NULL)},
};
/* @} */

/** This mutex has to work even if the InnoDB latching infrastructure
hasn't been initialized. */
static os_fast_mutex_t	cfg_vars_mutex;
static ib_cfg_var_t	cfg_vars[UT_ARR_SIZE(cfg_vars_defaults)];

/* public API functions and some auxiliary ones @{ */

/*********************************************************************//**
Lookup a variable name.
ib_cfg_lookup_var() @{
@return	config variable instance if found else NULL */
static
ib_cfg_var_t*
ib_cfg_lookup_var(
/*==============*/
	const char*	var)		/*!< in: variable name */
{
	ulint		i;

	for (i = 0; i < UT_ARR_SIZE(cfg_vars); ++i) {
		ib_cfg_var_t*	cfg_var;

		cfg_var = &cfg_vars[i];

		if (strcasecmp(var, cfg_var->name) == 0) {
			return(cfg_var);
		}
	}

	return(NULL);
}
/* @} */

/*********************************************************************//**
Get the type of a configuration variable. Returns DB_SUCCESS if the
variable with name "name" was found and "type" was set.
ib_cfg_var_get_type() @{
@return	DB_SUCCESS if successful */

ib_err_t
ib_cfg_var_get_type(
/*================*/
	const char*	name,		/*!< in: variable name */
	ib_cfg_type_t*	type)		/*!< out: variable type */
{
	ib_cfg_var_t*	cfg_var;
	ib_err_t	ret;

	os_fast_mutex_lock(&cfg_vars_mutex);

	cfg_var = ib_cfg_lookup_var(name);

	if (cfg_var != NULL) {
		*type = cfg_var->type;
		ret = DB_SUCCESS;
	} else {
		ret = DB_NOT_FOUND;
	}

	os_fast_mutex_unlock(&cfg_vars_mutex);

	return(ret);
}
/* @} */

/*********************************************************************//**
Set a configuration variable. "ap" must contain one argument whose type
depends on the type of the variable with the given "name". Returns
DB_SUCCESS if the variable with name "name" was found and if its value
was set.
ib_cfg_set_ap() @{
@return	DB_SUCCESS if set */
static
ib_err_t
ib_cfg_set_ap(
/*==========*/
	const char*	name,		/*!< in: variable name */
	va_list		ap)		/*!< in: variable value */
{
	ib_cfg_var_t*	cfg_var;
	ib_err_t	ret = DB_NOT_FOUND;

	os_fast_mutex_lock(&cfg_vars_mutex);

	cfg_var = ib_cfg_lookup_var(name);

	if (cfg_var != NULL) {

		/* check whether setting the variable is appropriate,
		according to its flag */
		if (cfg_var->flag & IB_CFG_FLAG_READONLY
		    || (cfg_var->flag & IB_CFG_FLAG_READONLY_AFTER_STARTUP
			&& srv_was_started)) {

			ret = DB_READONLY;
		} else {

			/* Get the parameter according to its type and
			call ::set() */
			switch (cfg_var->type) {
			case IB_CFG_IBOOL: {
				ib_bool_t	value;

				value = va_arg(ap, ib_bool_t);

				ret = cfg_var->set(cfg_var, &value);

				break;
			}

			case IB_CFG_ULINT: {
				ulint	value;

				value = va_arg(ap, ulint);

				ret = cfg_var->set(cfg_var, &value);

				break;
			}

			case IB_CFG_ULONG: {
				ulong	value;

				value = va_arg(ap, ulong);

				ret = cfg_var->set(cfg_var, &value);

				break;
			}

			case IB_CFG_TEXT: {
				const char*	value;

				value = va_arg(ap, const char*);

				ret = cfg_var->set(cfg_var, &value);

				break;
			}

			case IB_CFG_CB: {
				ib_cb_t	value;

				value = va_arg(ap, ib_cb_t);

				ret = cfg_var->set(cfg_var, &value);

				break;
			}
			/* do not add default: in order to produce a
			compilation warning if new type is added which is
			not handled here */
			}
		}
	}
	
	os_fast_mutex_unlock(&cfg_vars_mutex);

	return(ret);
}
/* @} */

/*********************************************************************//**
Set a configuration variable. The second argument's type depends on the
type of the variable with the given "name". Returns DB_SUCCESS if the
variable with name "name" was found and if its value was set. Strings
are not copied, be sure not to destroy or modify your string after it
has been given to this function. Memory management is left to the caller.
E.g. if you malloc() a string and give it to this function and then
malloc() another string and give it again to this function for the same
config variable, then you are responsible to free the first string.
ib_cfg_set() @{
@return	DB_SUCCESS if set */

ib_err_t
ib_cfg_set(
/*=======*/
	const char*	name,		/*!< in: variable name */
	...)				/*!< in: variable value */
{
	va_list		ap;
	ib_bool_t	ret;

	va_start(ap, name);

	ret = ib_cfg_set_ap(name, ap);

	va_end(ap);

	return(ret);
}
/* @} */

/*********************************************************************//**
Get the value of a configuration variable. The type of the returned value
depends on the type of the configuration variable. DB_SUCCESS is returned
if the variable with name "name" was found and "value" was set. Strings
are not copied and a reference to the internal storage is returned. Be
sure not to modify the returned value or you may confuse InnoDB or cause
a crash.
ib_cfg_get() @{
@return	DB_SUCCESS if retrieved successfully */

ib_err_t
ib_cfg_get(
/*=======*/
	const char*	name,		/*!< in: variable name */
	void*		value)		/*!< out: pointer to the place to
					store the retrieved value */
{
	ib_cfg_var_t*	cfg_var;
	ib_err_t	ret;

	os_fast_mutex_lock(&cfg_vars_mutex);

	cfg_var = ib_cfg_lookup_var(name);

	if (cfg_var != NULL) {
		ret = cfg_var->get(cfg_var, value);
	} else {
		ret = DB_NOT_FOUND;
	}

	os_fast_mutex_unlock(&cfg_vars_mutex);

	return(ret);
}
/* @} */

/*********************************************************************//**
Get a list of the names of all configuration variables.
The caller is responsible for free(3)ing the returned array of strings
when it is not needed anymore and for not modifying the individual strings.
ib_cfg_get_all() @{
@return	DB_SUCCESS or error code */

ib_err_t
ib_cfg_get_all(
/*===========*/
	const char***	names,		/*!< out: pointer to array of strings */
	ib_u32_t*	names_num)	/*!< out: number of strings returned */
{
	ib_u32_t	i;

	*names_num = UT_ARR_SIZE(cfg_vars_defaults);

	*names = (const char**) malloc(*names_num * sizeof(const char*));
	if (*names == NULL) {
		return(DB_OUT_OF_MEMORY);
	}

	for (i = 0; i < *names_num; i++) {
		(*names)[i] = cfg_vars_defaults[i].name;
	}

	return(DB_SUCCESS);
}
/* @} */

/*********************************************************************//**
Initialize the config system.
ib_cfg_init() @{
@return	DB_SUCCESS or error code */

ib_err_t
ib_cfg_init(void)
/*=============*/
{
	/* Initialize the mutex that protects cfg_vars[]. */
	os_fast_mutex_init(&cfg_vars_mutex);

	ut_memcpy(cfg_vars, cfg_vars_defaults, sizeof(cfg_vars));

	/* Set the default options. */
	srv_file_flush_method_str = NULL;
	srv_unix_file_flush_method = SRV_UNIX_FSYNC;
	srv_win_file_flush_method = SRV_WIN_IO_UNBUFFERED;

	os_aio_print_debug = FALSE;
	os_aio_use_native_aio = FALSE;

#define IB_CFG_SET(name, var)	\
	if (ib_cfg_set(name, var) != DB_SUCCESS) ut_error

	IB_CFG_SET("additional_mem_pool_size", 4 * 1024 * 1024);
	IB_CFG_SET("buffer_pool_size", 8 * 1024 * 1024);
	IB_CFG_SET("data_file_path", "ibdata1:32M:autoextend");
	IB_CFG_SET("data_home_dir", "./");
	IB_CFG_SET("file_per_table", IB_TRUE);
#ifndef __WIN__
	IB_CFG_SET("flush_method", "fsync");
#endif
	IB_CFG_SET("lock_wait_timeout", 60);
	IB_CFG_SET("log_buffer_size", 384 * 1024);
	IB_CFG_SET("log_file_size", 16 * 1024 * 1024);
	IB_CFG_SET("log_files_in_group", 2);
	IB_CFG_SET("log_group_home_dir", ".");
	IB_CFG_SET("lru_old_blocks_pct", 3 * 100 / 8);
	IB_CFG_SET("lru_block_access_recency", 0);
	IB_CFG_SET("rollback_on_timeout", IB_TRUE);
	IB_CFG_SET("read_io_threads", 4);
	IB_CFG_SET("write_io_threads", 4);
#undef IB_CFG_SET

	return(DB_SUCCESS);
}
/* @} */

/*********************************************************************//**
Shutdown the config system.
ib_cfg_shutdown() @{
@return	DB_SUCCESS or error code */

ib_err_t
ib_cfg_shutdown(void)
/*=================*/
{
	os_fast_mutex_lock(&cfg_vars_mutex);

	/* TODO: Check for NULL values of allocated config variables. */
	memset(cfg_vars, 0x0, sizeof(cfg_vars));

	os_fast_mutex_unlock(&cfg_vars_mutex);

	os_fast_mutex_free(&cfg_vars_mutex);

	return(DB_SUCCESS);
}
/* @} */

/* @} */

/* vim: set foldmethod=marker foldmarker=@{,@}: */
