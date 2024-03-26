/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
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

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <getopt.h>

#include "test0aux.h"

#define COMMENT '#'

namespace logger {
int level = (int)Level::Debug;
const char *Progname = "ib_test";
} // namespace logger

/* Runtime config */
static const char log_group_home_dir[] = "log";
static const char data_file_path[] = "ibdata1:32M:autoextend";

static void create_directory(const char *path) {
  /* Try and create the log sub-directory */
  auto ret = mkdir(path, S_IRWXU);

  /* Note: This doesn't catch all errors. EEXIST can also refer to
  dangling symlinks. */
  if (ret == -1 && errno != EEXIST) {
    perror(path);
    exit(EXIT_FAILURE);
  }
}

/** Read a value from an integer column in an InnoDB tuple.
@return	column value */

uint64_t read_int_from_tuple(ib_tpl_t tpl, const ib_col_meta_t *col_meta, int i) {
  uint64_t ival = 0;

  switch (col_meta->type_len) {
  case 1: {
    ib_u8_t v;

    ib_col_copy_value(tpl, i, &v, sizeof(v));

    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (ib_i8_t)v;
    break;
  }
  case 2: {
    ib_u16_t v;

    ib_col_copy_value(tpl, i, &v, sizeof(v));
    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (ib_i16_t)v;
    break;
  }
  case 4: {
    ib_u32_t v;

    ib_col_copy_value(tpl, i, &v, sizeof(v));
    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (ib_i32_t)v;
    break;
  }
  case 8: {
    ib_col_copy_value(tpl, i, &ival, sizeof(ival));
    break;
  }
  default:
    assert(false);
  }

  return (ival);
}

/** Print all columns in a tuple. */

void print_int_col(FILE *stream, const ib_tpl_t tpl, int i, ib_col_meta_t *col_meta) {
  ib_err_t err = DB_SUCCESS;

  switch (col_meta->type_len) {
  case 1: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u8_t u8;

      err = ib_tuple_read_u8(tpl, i, &u8);
      fprintf(stream, "%u", u8);
    } else {
      ib_i8_t i8;

      err = ib_tuple_read_i8(tpl, i, &i8);
      fprintf(stream, "%d", i8);
    }
    break;
  }
  case 2: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u16_t u16;

      err = ib_tuple_read_u16(tpl, i, &u16);
      fprintf(stream, "%u", u16);
    } else {
      ib_i16_t i16;

      err = ib_tuple_read_i16(tpl, i, &i16);
      fprintf(stream, "%d", i16);
    }
    break;
  }
  case 4: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      ib_u32_t u32;

      err = ib_tuple_read_u32(tpl, i, &u32);
      fprintf(stream, "%u", u32);
    } else {
      ib_i32_t i32;

      err = ib_tuple_read_i32(tpl, i, &i32);
      fprintf(stream, "%d", i32);
    }
    break;
  }
  case 8: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint64_t u64;

      err = ib_tuple_read_u64(tpl, i, &u64);
      fprintf(stream, "%llu", (unsigned long long)u64);
    } else {
      ib_i64_t i64;

      err = ib_tuple_read_i64(tpl, i, &i64);
      fprintf(stream, "%lld", (long long)i64);
    }
    break;
  }
  default:
    assert(0);
    break;
  }
  assert(err == DB_SUCCESS);
}

/** Print character array of give size or upto 256 chars */
void print_char_array(FILE *stream, const char *array, int len) {
  const char *ptr = array;

  for (int i = 0; i < len; ++i) {
    fprintf(stream, "%c", *(ptr + i));
  }
}

/** Print all columns in a tuple. */
void print_tuple(FILE *stream, const ib_tpl_t tpl) {
  int n_cols = ib_tuple_get_n_cols(tpl);

  for (int i = 0; i < n_cols; ++i) {
    ib_ulint_t data_len;
    ib_col_meta_t col_meta;

    data_len = ib_col_get_meta(tpl, i, &col_meta);

    /* Skip system columns. */
    if (col_meta.type == IB_SYS) {
      continue;
      /* Nothing to print. */
    } else if (data_len == IB_SQL_NULL) {
      fprintf(stream, "|");
      continue;
    } else {
      switch (col_meta.type) {
      case IB_INT: {
        print_int_col(stream, tpl, i, &col_meta);
        break;
      }
      case IB_FLOAT: {
        float v;
        ib_err_t err;

        err = ib_tuple_read_float(tpl, i, &v);
        assert(err == DB_SUCCESS);
        fprintf(stream, "%f", v);
        break;
      }
      case IB_DOUBLE: {
        double v;
        ib_err_t err;

        err = ib_tuple_read_double(tpl, i, &v);
        assert(err == DB_SUCCESS);
        fprintf(stream, "%lf", v);
        break;
      }
      case IB_CHAR:
      case IB_BLOB:
      case IB_DECIMAL:
      case IB_VARCHAR:
      case IB_VARCHAR_ANYCHARSET:
      case IB_CHAR_ANYCHARSET: {
        const char *ptr;

        ptr = static_cast<const char *>(ib_col_get_value(tpl, i));
        fprintf(stream, "%d:", (int)data_len);
        print_char_array(stream, ptr, (int)data_len);
        break;
      }
      default:
        assert(false);
        break;
      }
    }
    fprintf(stream, "|");
  }
  fprintf(stream, "\n");
}

/** Setup the InnoDB configuration parameters. */
void test_configure() {
  ib_err_t err;

  create_directory(log_group_home_dir);

  err = ib_cfg_set_text("flush_method", "O_DIRECT");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("log_files_in_group", 2);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("log_file_size", 32 * 1024 * 1024);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("log_buffer_size", 24 * 16384);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("buffer_pool_size", 5 * 1024 * 1024);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("additional_mem_pool_size", 4 * 1024 * 1024);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("flush_log_at_trx_commit", 1);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("file_io_threads", 4);
  assert(err == DB_READONLY);

  err = ib_cfg_set_int("lock_wait_timeout", 60);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_int("open_files", 300);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_bool_on("doublewrite");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_bool_on("checksums");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_bool_on("rollback_on_timeout");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_bool_on("print_verbose_log");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_bool_on("file_per_table");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_text("data_home_dir", "./");
  assert(err == DB_SUCCESS);

  err = ib_cfg_set_text("log_group_home_dir", log_group_home_dir);

  if (err != DB_SUCCESS) {
    fprintf(stderr, "syntax error in log_group_home_dir, or a "
                    "wrong number of mirrored log groups\n");
    exit(1);
  }

  err = ib_cfg_set_text("data_file_path", data_file_path);

  if (err != DB_SUCCESS) {
    fprintf(stderr, "syntax error in data_file_path\n");
    exit(1);
  }
}

/** Generate random text upto max size. */

int gen_rand_text(char *ptr,    /*!< in,out: text written here */
                  int max_size) /*!< in: max size of ptr */
{
  int i;
  int len = 0;
  static char txt[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                      "abcdefghijklmnopqrstuvwxyz"
                      "0123456789";

  do {
    len = random() % max_size;
  } while (len == 0);

  for (i = 0; i < len; ++i, ++ptr) {
    *ptr = txt[random() % (sizeof(txt) - 1)];
  }

  return (len);
}

struct option ib_longopts[] = {
    {"ib-buffer-pool-size", required_argument, nullptr, 1},
    {"ib-log-file-size", required_argument, nullptr, 2},
    {"ib-disable-ahi", no_argument, nullptr, 3},
    {"ib-io-capacity", required_argument, nullptr, 4},
    {"ib-use-sys-malloc", required_argument, nullptr, 5},
    {"ib-lru-old-ratio", required_argument, nullptr, 6},
    {"ib-lru-access-threshold", required_argument, nullptr, 7},
    {"ib-force-recovery", required_argument, nullptr, 8},
    {"ib-log-dir", required_argument, nullptr, 9},
    {"ib-data-dir", required_argument, nullptr, 10},
    {"ib-data-file-path", required_argument, nullptr, 11},
    {"ib-disble-dblwr", no_argument, nullptr, 12},
    {"ib-disble-checksum", no_argument, nullptr, 13},
    {"ib-disble-file-per-table", no_argument, nullptr, 14},
    {"ib-flush-log-at-trx-commit", required_argument, nullptr, 15},
    {"ib-flush-method", required_argument, nullptr, 16},
    {"ib-read-threads", required_argument, nullptr, 17},
    {"ib-write-threads", required_argument, nullptr, 18},
    {"ib-max-open-files", required_argument, nullptr, 19},
    {"ib-lock-wait-timeout", required_argument, nullptr, 20},
    {nullptr, 0, nullptr, 0}};

/** Print usage. */
void print_usage(const char *progname) {
  fprintf(stderr,
          "usage: %s "
          "[--ib-buffer-pool-size size in mb]\n"
          "[--ib-log-file-size size in mb]\n"
          "[--ib-io-capacity number of records]\n"
          "[--ib-use-sys-malloc]\n"
          "[--ib-lru-old-ratio as %% e.g. 38]\n"
          "[--ib-lru-access-threshold in ms]\n"
          "[--ib-force-recovery 1-6]\n"
          "[--ib-log-dir path]\n"
          "[--ib-data-dir path]\n"
          "[--ib-data-file-path string]\n"
          "[--ib-disble-dblwr]\n"
          "[--ib-disble-checksum]\n"
          "[--ib-disble-file-per-table]\n"
          "[--ib-flush-log-at-trx-commit 1-3]\n"
          "[--ib-flush-method method]\n"
          "[--ib-read-threads count]\n"
          "[--ib-write-threads count]\n"
          "[--ib-max-open-files count]\n"
          "[--ib-lock-wait-timeout seconds]\n",
          progname);
}

/** Set the runtime global options. */

ib_err_t set_global_option(int opt, const char *arg) {
  ib_err_t err = DB_ERROR;

  /* FIXME: Change numbers to enums or #defines */
  switch (opt) {
  case 1: {
    ib_ulint_t size;

    size = strtoul(arg, nullptr, 10);
    size *= 1024UL * 1024UL;
    err = ib_cfg_set_int("buffer_pool_size", size);
    assert(err == DB_SUCCESS);
    break;
  }
  case 2: {
    ib_ulint_t size;

    size = strtoul(arg, nullptr, 10);
    size *= 1024UL * 1024UL;
    err = ib_cfg_set_int("log_file_size", size);
    assert(err == DB_SUCCESS);
    break;
  }
  case 3: {
    ib_ulint_t size;

    size = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("io_capacity", size);
    assert(err == DB_SUCCESS);
    break;
  }
  case 4:
    err = ib_cfg_set_bool_on("use_sys_malloc");
    assert(err == DB_SUCCESS);
    break;

  case 5: {
    ib_ulint_t pct;

    pct = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("lru_old_blocks_pct", pct);
    assert(err == DB_SUCCESS);
    break;
  }

  case 6: {
    ib_ulint_t pct;

    pct = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("lru_block_access_recency", pct);
    assert(err == DB_SUCCESS);
    break;
  }

  case 7: {
    ib_ulint_t level;

    level = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("force_recovery", level);
    assert(err == DB_SUCCESS);
    break;
  }

  case 8: {
    err = ib_cfg_set_text("log_group_home_dir", arg);
    assert(err == DB_SUCCESS);
    break;
  }

  case 9: {
    err = ib_cfg_set_text("data_home_dir", arg);
    assert(err == DB_SUCCESS);
    break;
  }

  case 10: {
    err = ib_cfg_set_text("data_file_path", arg);
    assert(err == DB_SUCCESS);
    break;
  }

  case 11: {
    err = ib_cfg_set_bool_off("doublewrite");
    assert(err == DB_SUCCESS);
    break;
  }

  case 12: {
    err = ib_cfg_set_bool_off("checksum");
    assert(err == DB_SUCCESS);
    break;
  }

  case 13: {
    err = ib_cfg_set_bool_off("file_per_table");
    assert(err == DB_SUCCESS);
    break;
  }

  case 14: {
    ib_ulint_t level;

    level = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("flush_log_at_trx_commit", level);
    assert(err == DB_SUCCESS);
    break;
  }

  case 15: {
    err = ib_cfg_set_int("flush_method", arg);
    assert(err == DB_SUCCESS);
    break;
  }

  case 16: {
    ib_ulint_t threads;

    threads = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("read_io_threads", threads);
    assert(err == DB_SUCCESS);
    break;
  }

  case 17: {
    ib_ulint_t threads;

    threads = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("write_io_threads", threads);
    assert(err == DB_SUCCESS);
    break;
  }

  case 18: {
    ib_ulint_t n;

    n = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("open_files", n);
    assert(err == DB_SUCCESS);
    break;
  }

  case 19: {
    ib_ulint_t secs;

    secs = strtoul(arg, nullptr, 10);
    err = ib_cfg_set_int("lock_wait_timeout", secs);
    assert(err == DB_SUCCESS);
    break;
  }

  default:
    err = DB_ERROR;
    break;

  } /* switch */

  return (err);
}

/** Print API version to stdout. */

void print_version(void) {
  uint64_t version;

  version = ib_api_version();
  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */
}

/** Skip line. */
static int config_skip_line(FILE *fp) {
  int ch;

  while ((ch = getc(fp)) != -1 && ch != '\n')
    ;

  return (ch);
}

/** Add an element to the config. */
static void config_add_elem(ib_config_t *config, const ib_string_t *key,
                            const ib_string_t *val) {
  ib_var_t *var;

  if (config->n_count == 0) {
    int size;

    config->n_count = 4;

    size = config->n_count * sizeof(ib_var_t);
    config->elems = (ib_var_t *)malloc(size);
  } else if (config->n_elems == config->n_count - 1) {
    int size;

    config->n_count *= 2;
    size = config->n_count * sizeof(ib_var_t);
    config->elems = (ib_var_t *)realloc(config->elems, size);
  }

  assert(config->n_elems < config->n_count);

  var = &config->elems[config->n_elems];

  assert(key->len > 0);
  assert(key->ptr != nullptr);

  var->name.len = key->len;
  var->name.ptr = (ib_byte_t *)malloc(var->name.len);
  memcpy(var->name.ptr, key->ptr, var->name.len);

  /* Value can be NULL for boolean variables */
  if (val->len > 0) {
    var->value.len = val->len;
    var->value.ptr = (ib_byte_t *)malloc(var->value.len);
    memcpy(var->value.ptr, val->ptr, var->value.len);
  }

  ++config->n_elems;
}

/** Parse an InnoDB config file, the file has a very simply format:
Lines beginning with '#' are ignored. Characters after '#' (inclusive)
are also ignored.  Empty lines are also ignored. Variable syntax is:
  \s*var_name\s*=\s*value\s*\n */

int config_parse_file(const char *filename, ib_config_t *config) {
  int ch;
  FILE *fp;
  ib_string_t key;
  ib_string_t val;
  ib_string_t *str;
  ib_byte_t name[BUFSIZ];
  ib_byte_t value[BUFSIZ];

  fp = fopen(filename, "r");

  if (fp == nullptr) {
    return (-1);
  }

  memset(&key, 0x0, sizeof(key));
  memset(&val, 0x0, sizeof(val));

  key.ptr = name;
  val.ptr = value;
  str = &key;

  while ((ch = getc(fp)) != -1) {

    /* Skip comments. */
    if (ch == COMMENT) {
      if (config_skip_line(fp) == -1) {
        break;
      }
      continue;
    }

    /* Skip white space. */
    while (ch != '\n' && isspace(ch)) {
      if ((ch = getc(fp)) == -1) {
        break;
      }
    }

    /* Skip comments. */
    if (ch == COMMENT) {
      if (config_skip_line(fp) == -1) {
        break;
      }
      continue;
    }

    if (ch == -1) {
      break;
    }

    switch (ch) {
    case '\r':
      break;
    case '\n':
      key.ptr = name;
      val.ptr = value;

      if (key.len > 0) {
        config_add_elem(config, &key, &val);
      }

      memset(&key, 0x0, sizeof(key));
      memset(&val, 0x0, sizeof(val));

      /* Anything by default is a variable name */
      str = &key;
      str->ptr = name;
      break;
    case '=':
      /* Anything that follows an '=' is a value. */
      str = &val;
      str->ptr = value;
      break;
    default:
      ++str->len;
      *str->ptr++ = ch;
    }

    assert(str->len < (int)sizeof(name));
  }

  if (str->len > 0) {
    key.ptr = name;
    val.ptr = value;

    config_add_elem(config, &key, &val);
  }

  fclose(fp);

  return (0);
}

/** Print the elements. */

void config_print(const ib_config_t *config) {
  ib_ulint_t i;

  for (i = 0; i < config->n_elems; ++i) {
    if (config->elems[i].value.ptr != nullptr) {
      printf("%.*s=%.*s\n", config->elems[i].name.len,
             config->elems[i].name.ptr, config->elems[i].value.len,
             config->elems[i].value.ptr);
    } else {
      printf("%.*s\n", config->elems[i].name.len, config->elems[i].name.ptr);
    }
  }
}

/** Free the the elements. */

void config_free(ib_config_t *config) {
  ib_ulint_t i;

  for (i = 0; i < config->n_elems; ++i) {
    free(config->elems[i].name.ptr);
    free(config->elems[i].value.ptr);
  }
  free(config->elems);

  memset(config, 0x0, sizeof(*config));
}

/** Drop the table. */

ib_err_t drop_table(const char *dbname, /*!< in: database name */
                    const char *name)   /*!< in: table to drop */
{
  ib_err_t err;
  ib_trx_t ib_trx;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = ib_schema_lock_exclusive(ib_trx);
  assert(err == DB_SUCCESS);

  err = ib_table_drop(ib_trx, table_name);
  assert(err == DB_SUCCESS);

  err = ib_trx_commit(ib_trx);
  assert(err == DB_SUCCESS);

  return (err);
}
