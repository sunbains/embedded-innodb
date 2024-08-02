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
#endif /** HAVE_STRINGS_H */

#include "buf0lru.h"
#include "db0err.h"
#include "dict0mem.h"
#include "innodb0types.h"
#include "log0recv.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "trx0sys.h"

static char *srv_file_flush_method_str = nullptr;

/* A point in the LRU list (expressed as a percent), all blocks from this
point onwards (inclusive) are considered "old" blocks. */
static ulint lru_old_blocks_pct;

/** We copy the argument passed to ib_cfg_set_text("data_file_path")
because srv_parse_data_file_paths_and_sizes() parses it's input argument
destructively. The copy is done using ut_new(). */
static char *srv_data_file_paths_and_sizes = nullptr;

/* enum ib_cfg_flag_t @{ */
enum ib_cfg_flag_t {
  IB_CFG_FLAG_NONE = 0x1,

  /** Can be modified only before innodb is started */
  IB_CFG_FLAG_READONLY_AFTER_STARTUP = 0x2,

  /** cannot be modified */
  IB_CFG_FLAG_READONLY = 0x4

};

/* @} */

struct ib_cfg_var {
  /** Config var name */
  const char *name;

  /** Config var type */
  ib_cfg_type_t type;

  /** Config var flag */
  ib_cfg_flag_t flag;

  /** Minimum allowed value for numeric types, ignored for other types */
  uint64_t min_val;

  /** Maximum allowed value for numeric types, ignored for other types */
  uint64_t max_val;

  /** Function used to validate a new variable's value when setting it */
  ib_err_t (*validate)(const struct ib_cfg_var *, const void *);

  /** Function used to set the variable's value */
  ib_err_t (*set)(struct ib_cfg_var *, const void *);

  /** Function used to get the variable's value */
  ib_err_t (*get)(const struct ib_cfg_var *, void *);

  /** Opaque storage that may be used by the set() and get() functions */
  void *tank;
};

/**
 * Assign src to dst according to type. If this is a string variable (char*)
 * the string itself is not copied.
 * 
 * @param type - in: type of src and dst
 * @param dst - out: destination
 * @param src - in: source
 * 
 * @return DB_SUCCESS if assigned (type is known)
 */
static ib_err_t ib_cfg_assign(ib_cfg_type_t type, void *dst, const void *src) {
  switch (type) {
    case IB_CFG_IBOOL: {

      *(bool *)dst = *(const bool *)src;
      return (DB_SUCCESS);
    }

    case IB_CFG_ULINT: {

      *(ulint *)dst = *(const ulint *)src;
      return (DB_SUCCESS);
    }

    case IB_CFG_ULONG: {

      *(ulong *)dst = *(const ulong *)src;
      return (DB_SUCCESS);
    }

    case IB_CFG_TEXT: {

      *(char **)dst = *(char **)src;
      return (DB_SUCCESS);
    }

    case IB_CFG_CB: {

      *(ib_cb_t *)dst = *(ib_cb_t *)src;
      return (DB_SUCCESS);
    }
      /* do not add default: in order to produce a compilation
    warning if new type is added which is not handled here */
  }

  /* NOT REACHED */
  return (DB_ERROR);
}

/** A generic function used for ib_cfg_var::validate() to check a numeric
type for min/max allowed value overflow.
@param cfg_var - in/out: configuration variable to check
@param value - in: value to check
@return DB_SUCCESS if value is in range */
static ib_err_t ib_cfg_var_validate_numeric(const struct ib_cfg_var *cfg_var, const void *value) {
  switch (cfg_var->type) {

    case IB_CFG_ULINT: {
      ulint v;

      v = *(ulint *)value;

      if ((ulint)cfg_var->min_val <= v && v <= (ulint)cfg_var->max_val) {

        return (DB_SUCCESS);
      } else {

        return (DB_INVALID_INPUT);
      }
    }

    case IB_CFG_ULONG: {
      ulong v;

      v = *(ulong *)value;

      if ((ulong)cfg_var->min_val <= v && v <= (ulong)cfg_var->max_val) {

        return (DB_SUCCESS);
      } else {

        return (DB_INVALID_INPUT);
      }
    }

    default:
      ut_error;
  }

  /* NOT REACHED */
  return (DB_ERROR);
}

/* generic and specific ib_cfg_var_(set|get)_* functions @{ */

/**
 * A generic function used for ib_cfg_var::set() that stores the value
 * of the configuration parameter in the location pointed by
 * ib_cfg_var::tank. If this is a string variable (char*) then the string
 * is not copied and a reference to "value" is made. It should not be freed
 * or modified until InnoDB is running or a new value is set.
 * 
 * @param cfg_var - in/out: configuration variable to manipulate
 * @param value - in: value to set
 * 
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_generic(struct ib_cfg_var *cfg_var, const void *value) {
  ib_err_t ret;

  if (cfg_var->validate != nullptr) {
    ret = cfg_var->validate(cfg_var, value);
    if (ret != DB_SUCCESS) {
      return (ret);
    }
  }

  ret = ib_cfg_assign(cfg_var->type, cfg_var->tank, value);

  return (ret);
}

/**
 * A generic function used for ib_cfg_var::get() that retrieves the value
 * of the configuration parameter from the location pointed by
 * ib_cfg_var::tank and stores it in "value". The variable is not copied
 * to "value" even if it is a string variable (char*). Only a pointer to the
 * storage is written in "value". It should not be freed unless it was
 * allocated by the user and set with ib_cfg_set().
 * 
 * @param cfg_var - in: configuration variable whose value to retrieve
 * @param value - out: place to store the retrieved value
 * 
 * @return DB_SUCCESS if retrieved successfully
 */
static ib_err_t ib_cfg_var_get_generic(const struct ib_cfg_var *cfg_var, void *value) {
  return (ib_cfg_assign(cfg_var->type, value, cfg_var->tank));
}

/**
 * Set the value of the config variable "data_file_path".
 * 
 * @param cfg_var - in/out: configuration variable to manipulate, must be "data_file_path"
 * @param value - in: value to set, must point to char* variable
 * 
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_data_file_path(struct ib_cfg_var *cfg_var, const void *value) {
  const char *value_str;

  ut_a(strcasecmp(cfg_var->name, "data_file_path") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  value_str = *(char **)value;

  if (srv_parse_data_file_paths_and_sizes(value_str)) {
    return (DB_SUCCESS);
  } else {
    return (DB_INVALID_INPUT);
  }
}

/** Retrieve the value of the config variable "data_file_path".
 * 
 * @param cfg_var - in: configuration variable whose value to retrieve, must be "data_file_path"
 * @param value - out: place to store the retrieved value, must point to char* variable
 * 
 * @return DB_SUCCESS if retrieved successfully
 */
static ib_err_t ib_cfg_var_get_data_file_path(const struct ib_cfg_var *cfg_var, void *value) {
  ut_a(strcasecmp(cfg_var->name, "data_file_path") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  *(char **)value = srv_data_file_paths_and_sizes;

  return (DB_SUCCESS);
}

/** Set the value of the config variable "file_format".
 * 
 * @param cfg_var - in/out: configuration variable to manipulate, must be "file_format"
 * @param value - in: value to set, must point to char* variable
 * 
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_file_format(struct ib_cfg_var *cfg_var, const void *value) {
  ulint format_id;

  ut_a(strcasecmp(cfg_var->name, "file_format") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  format_id = trx_sys_file_format_name_to_id(*(char **)value);

  if (format_id > DICT_TF_FORMAT_MAX) {
    return (DB_INVALID_INPUT);
  }

  srv_file_format = format_id;

  return (DB_SUCCESS);
}

/** Retrieve the value of the config variable "file_format".
 * 
 * @param cfg_var - in: configuration variable whose value to retrieve, must be "file_format"
 * @param value - out: place to store the retrieved value, must point to char* variable
 * 
 * @return DB_SUCCESS if retrieved successfully
 */
static ib_err_t ib_cfg_var_get_file_format(const struct ib_cfg_var *cfg_var, void *value) {
  ut_a(strcasecmp(cfg_var->name, "file_format") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  *(const char **)value = trx_sys_file_format_id_to_name(srv_file_format);

  return (DB_SUCCESS);
}

/* @} */

/**
 * Check the value of the config variable "data_home_dir". We need to ensure
 * that the value ends with a path separator.
 *
 * @param cfg_var - in/out: configuration variable to check, must be "data_home_dir"
 * @param value - in: value to check, must point to char* variable
 *
 * @return DB_SUCCESS if value is valid
 */
static ib_err_t ib_cfg_var_validate_data_home_dir(const struct ib_cfg_var *cfg_var, const void *value) {
  ut_a(strcasecmp(cfg_var->name, "data_home_dir") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  auto value_str = *(char **)value;
  auto len = strlen(value_str);

  /* We simply require that this variable end in a path separator.
  We will normalize it before use internally. */
  if (len == 0 || (value_str[len - 1] != '/' && value_str[len - 1] != '\\')) {

    return (DB_INVALID_INPUT);
  }

  return (DB_SUCCESS);
}

/* @} */

/**
 * Set the value of the config variable "log_group_home_dir".
 *
 * @param cfg_var - in/out: configuration variable to manipulate, must be "log_group_home_dir"
 * @param value - in: value to set, must point to char* variable
 *
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_log_group_home_dir(struct ib_cfg_var *cfg_var, const void *value) {
  const char *value_str;

  ut_a(strcasecmp(cfg_var->name, "log_group_home_dir") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  ut_a(srv_log_group_home_dir == nullptr);

  value_str = *(char **)value;

  if (srv_parse_log_group_home_dirs(value_str)) {
    return (DB_SUCCESS);
  } else {
    return (DB_INVALID_INPUT);
  }
}

/* @} */

/**
 * Set the value of the config variable "flush_method".
 *
 * @param cfg_var - in/out: configuration variable to manipulate, must be "log_group_home_dir"
 * @param value - in: value to set, must point to char* variable
 *
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_flush_method(struct ib_cfg_var *cfg_var, const void *value) {
  const char *value_str;
  ib_err_t err = DB_SUCCESS;

  ut_a(strcasecmp(cfg_var->name, "flush_method") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  value_str = *(const char **)value;

  if (0 == strcmp(value_str, "fsync")) {
    srv_unix_file_flush_method = SRV_UNIX_FSYNC;
  } else if (0 == strcmp(value_str, "O_DSYNC")) {
    srv_unix_file_flush_method = SRV_UNIX_O_DSYNC;
  } else if (0 == strcmp(value_str, "O_DIRECT")) {
    srv_unix_file_flush_method = SRV_UNIX_O_DIRECT;
  } else if (0 == strcmp(value_str, "littlesync")) {
    srv_unix_file_flush_method = SRV_UNIX_LITTLESYNC;
  } else if (0 == strcmp(value_str, "nosync")) {
    srv_unix_file_flush_method = SRV_UNIX_NOSYNC;
  } else {
    err = DB_INVALID_INPUT;
  }

  if (err == DB_SUCCESS) {
    *(const char **)cfg_var->tank = value_str;
  } else {
    *(const char **)cfg_var->tank = nullptr;
  }

  return (err);
}

/* @} */
/**
 * Retrieve the value of the config variable "log_group_home_dir".
 *
 * @param cfg_var - in: configuration variable whose value to retrieve, must be "log_group_home_dir"
 * @param value - out: place to store the retrieved value, must point to char* variable
 *
 * @return DB_SUCCESS if retrieved successfully
 */
static ib_err_t ib_cfg_var_get_log_group_home_dir(const struct ib_cfg_var *cfg_var, void *value) {
  ut_a(strcasecmp(cfg_var->name, "log_group_home_dir") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  *(const char **)value = nullptr;

  return (DB_SUCCESS);
}

/* @} */

/**
 * Set the value of the config variable "lru_old_blocks_pct".
 *
 * @param cfg_var - in/out: configuration variable to manipulate, must be "lru_old_blocks_pct"
 * @param value - in: value to set, must point to ulint variable
 *
 * @return DB_SUCCESS if set successfully
 */
static ib_err_t ib_cfg_var_set_LRU_old_blocks_pct(struct ib_cfg_var *cfg_var, const void *value) {
  ut_a(strcasecmp(cfg_var->name, "lru_old_blocks_pct") == 0);
  ut_a(cfg_var->type == IB_CFG_ULINT);

  if (cfg_var->validate != nullptr) {
    ib_err_t ret;

    ret = cfg_var->validate(cfg_var, value);

    if (ret != DB_SUCCESS) {
      return (ret);
    }
  }

  lru_old_blocks_pct = Buf_LRU::old_ratio_update(*(ulint *)value, srv_buf_pool != nullptr);

  return DB_SUCCESS;
}

/* @} */

/* ib_cfg_var_get_generic() is used to get the value of lru_old_blocks_pct */

/* There is no ib_cfg_var_set_version() */

/**
 * Retrieve the value of the config variable "version".
 * 
 * @param cfg_var - in: configuration variable whose value to retrieve, must be "version"
 * @param value - out: place to store the retrieved value, must point to char* variable
 * 
 * @return DB_SUCCESS if retrieved successfully
 */
static ib_err_t ib_cfg_var_get_version(const struct ib_cfg_var *cfg_var, void *value) {
  ut_a(strcasecmp(cfg_var->name, "version") == 0);
  ut_a(cfg_var->type == IB_CFG_TEXT);

  *(const char **)value = VERSION;

  return DB_SUCCESS;
}

/* cfg_vars_defaults[] @{ */
static const ib_cfg_var cfg_vars_defaults[] = {
  {STRUCT_FLD(name, "adaptive_flushing"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_adaptive_flushing)},

  {STRUCT_FLD(name, "additional_mem_pool_size"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 512 * 1024),
   STRUCT_FLD(max_val, IB_UINT64_T_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_mem_pool_size)},

  {STRUCT_FLD(name, "autoextend_increment"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 1),
   STRUCT_FLD(max_val, 1000),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_auto_extend_increment)},

  {STRUCT_FLD(name, "buffer_pool_size"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 5 * 1024 * 1024),
   STRUCT_FLD(max_val, ULINT_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_buf_pool_size)},

  {STRUCT_FLD(name, "checksums"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_use_checksums)},

  {STRUCT_FLD(name, "data_file_path"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_data_file_path),
   STRUCT_FLD(get, ib_cfg_var_get_data_file_path),
   STRUCT_FLD(tank, nullptr)},

  {STRUCT_FLD(name, "data_home_dir"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, ib_cfg_var_validate_data_home_dir),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_data_home)},

  {STRUCT_FLD(name, "doublewrite"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_use_doublewrite_buf)},

  {STRUCT_FLD(name, "file_format"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(
     validate, nullptr /* validation is done inside
				     ib_cfg_var_set_file_format */
   ),
   STRUCT_FLD(set, ib_cfg_var_set_file_format),
   STRUCT_FLD(get, ib_cfg_var_get_file_format),
   STRUCT_FLD(tank, nullptr)},

  {STRUCT_FLD(name, "file_io_threads"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(
     set, nullptr /* The ::set() function should never
				     be called because this variable is
				     flagged as IB_CFG_FLAG_READONLY */
   ),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_n_file_io_threads)},
  {STRUCT_FLD(name, "file_per_table"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_file_per_table)},

  {STRUCT_FLD(name, "flush_log_at_trx_commit"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 2),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_flush_log_at_trx_commit)},

  {STRUCT_FLD(name, "flush_method"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_flush_method),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_file_flush_method_str)},

  {STRUCT_FLD(name, "force_recovery"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, IB_RECOVERY_DEFAULT),
   STRUCT_FLD(max_val, IB_RECOVERY_NO_LOG_REDO),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_force_recovery)},

  {STRUCT_FLD(name, "io_capacity"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 100),
   STRUCT_FLD(max_val, 1000000),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_io_capacity)},

  {STRUCT_FLD(name, "lock_wait_timeout"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 1),
   STRUCT_FLD(max_val, 1024 * 1024 * 1024),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &ses_lock_wait_timeout)},

  {STRUCT_FLD(name, "log_buffer_size"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 256 * 1024),
   STRUCT_FLD(max_val, IB_UINT64_T_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_log_buffer_curr_size)},

  {STRUCT_FLD(name, "log_file_size"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 1024 * 1024),
   STRUCT_FLD(max_val, ULINT_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_log_file_curr_size)},

  {STRUCT_FLD(name, "log_files_in_group"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 2),
   STRUCT_FLD(max_val, 100),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_n_log_files)},

  {STRUCT_FLD(name, "log_group_home_dir"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_log_group_home_dir),
   STRUCT_FLD(get, ib_cfg_var_get_log_group_home_dir),
   STRUCT_FLD(tank, nullptr)},

  {STRUCT_FLD(name, "max_dirty_pages_pct"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 100),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_max_buf_pool_modified_pct)},

  {STRUCT_FLD(name, "max_purge_lag"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, IB_UINT64_T_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_max_purge_lag)},

  {STRUCT_FLD(name, "lru_old_blocks_pct"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 5),
   STRUCT_FLD(max_val, 95),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_LRU_old_blocks_pct),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &lru_old_blocks_pct)},

  {STRUCT_FLD(name, "lru_block_access_recency"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0xFFFFFFFFUL),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &Buf_LRU::s_old_threshold_ms)},

  {STRUCT_FLD(name, "open_files"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 10),
   STRUCT_FLD(max_val, IB_UINT64_T_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_max_n_open_files)},

  {STRUCT_FLD(name, "read_io_threads"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 1),
   STRUCT_FLD(max_val, 64),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_n_read_io_threads)},

  {STRUCT_FLD(name, "write_io_threads"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY_AFTER_STARTUP),
   STRUCT_FLD(min_val, 1),
   STRUCT_FLD(max_val, 64),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_n_write_io_threads)},

  /* New, not present in InnoDB/MySQL */
  {STRUCT_FLD(name, "pre_rollback_hook"),
   STRUCT_FLD(type, IB_CFG_CB),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &recv_pre_rollback_hook)},

  /* New, not present in InnoDB/MySQL */
  {STRUCT_FLD(name, "print_verbose_log"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_print_verbose_log)},

  /* New, not present in InnoDB/MySQL */
  {STRUCT_FLD(name, "rollback_on_timeout"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &ses_rollback_on_timeout)},

  {STRUCT_FLD(name, "stats_sample_pages"),
   STRUCT_FLD(type, IB_CFG_ULINT),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 1),
   STRUCT_FLD(max_val, ULINT_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_stats_sample_pages)},

  {STRUCT_FLD(name, "status_file"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_innodb_status)},

  {STRUCT_FLD(name, "sync_spin_loops"),
   STRUCT_FLD(type, IB_CFG_ULONG),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, IB_UINT64_T_MAX),
   STRUCT_FLD(validate, ib_cfg_var_validate_numeric),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_n_spin_wait_rounds)},

  {STRUCT_FLD(name, "use_sys_malloc"),
   STRUCT_FLD(type, IB_CFG_IBOOL),
   STRUCT_FLD(flag, IB_CFG_FLAG_NONE),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(set, ib_cfg_var_set_generic),
   STRUCT_FLD(get, ib_cfg_var_get_generic),
   STRUCT_FLD(tank, &srv_use_sys_malloc)},

  {STRUCT_FLD(name, "version"),
   STRUCT_FLD(type, IB_CFG_TEXT),
   STRUCT_FLD(flag, IB_CFG_FLAG_READONLY),
   STRUCT_FLD(min_val, 0),
   STRUCT_FLD(max_val, 0),
   STRUCT_FLD(validate, nullptr),
   STRUCT_FLD(
     set, nullptr /* The ::set() function should never
				     be called because this variable is
				     flagged as IB_CFG_FLAG_READONLY */
   ),
   STRUCT_FLD(get, ib_cfg_var_get_version),
   STRUCT_FLD(tank, nullptr)},
};
/* @} */

/** This mutex has to work even if the InnoDB latching infrastructure
hasn't been initialized. */
static std::mutex cfg_vars_mutex;
static ib_cfg_var cfg_vars[std::size(cfg_vars_defaults)];

/* public API functions and some auxiliary ones @{ */

/** Lookup a variable name.
ib_cfg_lookup_var() @{
@return	config variable instance if found else NULL */
static ib_cfg_var *ib_cfg_lookup_var(const char *var /*!< in: variable name */) {

  for (ulint i = 0; i < std::size(cfg_vars); ++i) {

    auto cfg_var = &cfg_vars[i];

    if (strcasecmp(var, cfg_var->name) == 0) {
      return (cfg_var);
    }
  }

  return (nullptr);
}

ib_err_t ib_cfg_var_get_type(const char *name, ib_cfg_type_t *type) {
  ib_err_t ret;

  std::lock_guard lock(cfg_vars_mutex);

  auto cfg_var = ib_cfg_lookup_var(name);

  if (cfg_var != nullptr) {
    *type = cfg_var->type;
    ret = DB_SUCCESS;
  } else {
    ret = DB_NOT_FOUND;
  }

  return ret;
}

/* @} */

/**
 * Set a configuration variable.
 * 
 * @param name variable name
 * @param ap variable value
 * 
 * @return DB_SUCCESS if set
 * 
 * ib_cfg_set_ap() @{
 */
static ib_err_t ib_cfg_set_ap(const char *name, va_list ap) {
  ib_err_t ret = DB_NOT_FOUND;

  std::lock_guard lock(cfg_vars_mutex);

  auto cfg_var = ib_cfg_lookup_var(name);

  if (cfg_var != nullptr) {

    /* check whether setting the variable is appropriate,
    according to its flag */
    if (cfg_var->flag & IB_CFG_FLAG_READONLY || (cfg_var->flag & IB_CFG_FLAG_READONLY_AFTER_STARTUP && srv_was_started)) {

      ret = DB_READONLY;
    } else {

      /* Get the parameter according to its type and
      call ::set() */
      switch (cfg_var->type) {
        case IB_CFG_IBOOL: {
          bool value;

          /* Should be passing bool here, but va_arg only accepts int. */
          value = va_arg(ap, int);

          ret = cfg_var->set(cfg_var, &value);

          break;
        }

        case IB_CFG_ULINT: {
          ulint value;

          value = va_arg(ap, ulint);

          ret = cfg_var->set(cfg_var, &value);

          break;
        }

        case IB_CFG_ULONG: {
          ulong value;

          value = va_arg(ap, ulong);

          ret = cfg_var->set(cfg_var, &value);

          break;
        }

        case IB_CFG_TEXT: {
          const char *value;

          value = va_arg(ap, const char *);

          ret = cfg_var->set(cfg_var, &value);

          break;
        }

        case IB_CFG_CB: {
          ib_cb_t value;

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

  return (ret);
}

ib_err_t ib_cfg_set(const char *name, ...) {
  va_list ap;

  va_start(ap, name);

  auto ret = ib_cfg_set_ap(name, ap);

  va_end(ap);

  return ret;
}

ib_err_t ib_cfg_get(const char *name, void *value) {
  ib_err_t ret;

  std::lock_guard lock(cfg_vars_mutex);

  auto cfg_var = ib_cfg_lookup_var(name);

  if (cfg_var != nullptr) {
    ret = cfg_var->get(cfg_var, value);
  } else {
    ret = DB_NOT_FOUND;
  }

  return ret;
}

ib_err_t ib_cfg_get_all(const char ***names, uint32_t *names_num) {
  uint32_t i;

  *names_num = std::size(cfg_vars_defaults);

  *names = (const char **)malloc(*names_num * sizeof(const char *));
  if (*names == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  for (i = 0; i < *names_num; i++) {
    (*names)[i] = cfg_vars_defaults[i].name;
  }

  return DB_SUCCESS;
}

ib_err_t ib_cfg_init() {
  /* Initialize the mutex that protects cfg_vars[]. */
  memcpy(cfg_vars, cfg_vars_defaults, sizeof(cfg_vars));

  /* Set the default options. */
  srv_file_flush_method_str = nullptr;
  srv_unix_file_flush_method = SRV_UNIX_FSYNC;

#define IB_CFG_SET(name, var)              \
  if (ib_cfg_set(name, var) != DB_SUCCESS) \
  ut_error

  IB_CFG_SET("additional_mem_pool_size", 4 * 1024 * 1024);
  IB_CFG_SET("buffer_pool_size", 8 * 1024 * 1024);
  IB_CFG_SET("data_file_path", "ibdata1:32M:autoextend");
  IB_CFG_SET("data_home_dir", "./");
  IB_CFG_SET("file_per_table", true);
  IB_CFG_SET("flush_method", "fsync");
  IB_CFG_SET("lock_wait_timeout", 60);
  IB_CFG_SET("log_buffer_size", 384 * 1024);
  IB_CFG_SET("log_file_size", 16 * 1024 * 1024);
  IB_CFG_SET("log_files_in_group", 2);
  IB_CFG_SET("log_group_home_dir", ".");
  IB_CFG_SET("lru_old_blocks_pct", 3 * 100 / 8);
  IB_CFG_SET("lru_block_access_recency", 0);
  IB_CFG_SET("rollback_on_timeout", true);
  IB_CFG_SET("read_io_threads", 4);
  IB_CFG_SET("write_io_threads", 4);
#undef IB_CFG_SET

  return (DB_SUCCESS);
}

ib_err_t ib_cfg_shutdown() {
  std::lock_guard<std::mutex> lock(cfg_vars_mutex);

  /* TODO: Check for nullptr values of allocated config variables. */
  memset(cfg_vars, 0x0, sizeof(cfg_vars));

  return DB_SUCCESS;
}

